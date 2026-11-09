use super::*;

const MAX_COMMANDS: usize = 64;
const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_RESULT_BYTES: usize = 16 * 1024 * 1024;

pub(crate) struct Pipeline<'a> {
    connection: &'a mut PgConnection,
    commands: usize,
    input_bytes: usize,
    finished: bool,
}

impl PgConnection {
    /// Only the connection owner can queue BEGIN before lending the data-only window.
    pub(crate) fn pipeline_begin(&mut self, io: &IoControl) -> Result<Pipeline<'_>> {
        self.require_mode(Mode::Sql)?;
        if self.transaction_status()? != TransactionStatus::Idle {
            return Err(error(
                "PostgreSQL pipeline BEGIN requires an idle transaction",
            ));
        }
        let mut pipeline = self.pipeline(io)?;
        pipeline.query_internal("BEGIN", io)?;
        Ok(pipeline)
    }

    /// Pipeline completion validates a query window; use commit_observed for transaction completion.
    pub(crate) fn pipeline(&mut self, io: &IoControl) -> Result<Pipeline<'_>> {
        self.require_mode(Mode::Sql)?;
        let raw = self.require(Phase::Idle)?;
        io::check(io)?;
        // SAFETY: the idle SQL connection is nonblocking and remains exclusively borrowed.
        if unsafe { pq::PQenterPipelineMode(raw) } != 1 {
            return Err(error("PostgreSQL pipeline mode could not be entered"));
        }
        self.raw
            .as_ref()
            .ok_or_else(|| error("PostgreSQL connection is closed"))?
            .notices
            .reset();
        self.phase = Phase::Pipeline;
        Ok(Pipeline {
            connection: self,
            commands: 0,
            input_bytes: 0,
            finished: false,
        })
    }
}

impl Pipeline<'_> {
    pub(crate) fn commands(&self) -> usize {
        self.commands
    }

    pub(crate) fn query(&mut self, sql: &str, io: &IoControl) -> Result<()> {
        if !is_data_command(sql) {
            return self.fail(error(
                "PostgreSQL pipeline requires an internal data command",
            ));
        }
        self.query_internal(sql, io)
    }

    fn query_internal(&mut self, sql: &str, io: &IoControl) -> Result<()> {
        self.check_capacity(sql.len().saturating_add(1))?;
        let sql = match prepared_cstring(sql.as_bytes()) {
            Ok(sql) => sql,
            Err(failure) => return self.fail(failure),
        };
        self.dispatch(sql.as_bytes_with_nul().len(), io, |raw| {
            // SAFETY: libpq copies the terminated SQL; zero parameters require no arrays.
            unsafe {
                pq::PQsendQueryParams(
                    raw,
                    sql.as_ptr(),
                    0,
                    ptr::null(),
                    ptr::null(),
                    ptr::null(),
                    ptr::null(),
                    0,
                )
            }
        })
    }

    pub(crate) fn execute_prepared(
        &mut self,
        name: &str,
        params: &[PgParam<'_>],
        io: &IoControl,
    ) -> Result<()> {
        let bytes = params
            .iter()
            .fold(name.len().saturating_add(1), |bytes, param| {
                bytes.saturating_add(match param {
                    PgParam::Null => 4,
                    PgParam::Text(value) => value.len().saturating_add(5),
                    #[cfg(test)]
                    PgParam::Binary(value) => value.len().saturating_add(4),
                })
            });
        self.check_capacity(bytes)?;
        let inputs = (|| {
            let count = parameter_count(params.len())?;
            let name = prepared_cstring(name.as_bytes())?;
            let mut text = Vec::new();
            let mut values = Vec::with_capacity(params.len());
            let mut lengths = Vec::with_capacity(params.len());
            let mut formats = Vec::with_capacity(params.len());
            let mut bytes = name.as_bytes_with_nul().len();
            for param in params {
                let (value, length, format, size) = match param {
                    PgParam::Null => (ptr::null(), 0, 0, 4),
                    PgParam::Text(value) => {
                        let string = prepared_cstring(value)?;
                        let pointer = string.as_ptr();
                        text.push(string);
                        (pointer, 0, 0, value.len().saturating_add(5))
                    }
                    #[cfg(test)]
                    PgParam::Binary(value) => (
                        value.as_ptr().cast(),
                        parameter_length(value.len())?,
                        1,
                        value.len().saturating_add(4),
                    ),
                };
                bytes = bytes.saturating_add(size);
                values.push(value);
                lengths.push(length);
                formats.push(format);
            }
            Ok((count, name, text, values, lengths, formats, bytes))
        })();
        let (count, name, _text, values, lengths, formats, bytes) = match inputs {
            Ok(inputs) => inputs,
            Err(failure) => return self.fail(failure),
        };
        self.dispatch(bytes, io, |raw| {
            // SAFETY: checked arrays contain count entries and all parameter storage is live.
            unsafe {
                pq::PQsendQueryPrepared(
                    raw,
                    name.as_ptr(),
                    count,
                    values.as_ptr(),
                    lengths.as_ptr(),
                    formats.as_ptr(),
                    0,
                )
            }
        })
    }

    fn dispatch(
        &mut self,
        bytes: usize,
        io: &IoControl,
        send: impl FnOnce(*mut pq::PGconn) -> i32,
    ) -> Result<()> {
        self.check_capacity(bytes)?;
        self.connection.operation(io, |connection, _| {
            let raw = connection.require(Phase::Pipeline)?;
            if send(raw) != 1 {
                return Err(error("PostgreSQL pipeline dispatch failed"));
            }
            // SAFETY: this immediately follows the dispatch and bounds each returned PGresult to a row.
            if unsafe { pq::PQsetSingleRowMode(raw) } != 1 {
                return Err(error(
                    "PostgreSQL pipeline row streaming could not be enabled",
                ));
            }
            Ok(())
        })?;
        self.commands += 1;
        self.input_bytes += bytes;
        Ok(())
    }

    pub(crate) fn sync(
        mut self,
        io: &IoControl,
        mut validate: impl FnMut(usize, PgResult) -> Result<()>,
    ) -> Result<()> {
        if self.commands == 0 {
            return Err(error("PostgreSQL pipeline contains no commands"));
        }
        let commands = self.commands;
        let outcome = self.connection.operation(io, |connection, io| {
            let raw = connection.require(Phase::Pipeline)?;
            // SAFETY: this queues the one synchronization point for the exclusively held pipeline.
            if unsafe { pq::PQpipelineSync(raw) } != 1 {
                return Err(error("PostgreSQL pipeline synchronization dispatch failed"));
            }
            let mut index = 0;
            let mut terminal = false;
            let mut committed = false;
            let mut failure = None;
            loop {
                io::check(io)?;
                // SAFETY: nonblocking flush and result parsing use the live exclusive connection.
                let writing = match unsafe { pq::PQflush(raw) } {
                    0 => false,
                    1 => true,
                    _ => return Err(error("PostgreSQL pipeline output failed")),
                };
                // SAFETY: PQisBusy parses available input without waiting for more data.
                if unsafe { pq::PQisBusy(raw) } == 0 {
                    // SAFETY: PQisBusy proved retrieval nonblocking; each result has one owner.
                    let result = NonNull::new(unsafe { pq::PQgetResult(raw) })
                        .map(|raw| connection.own_result(raw))
                        .transpose()?;
                    let Some(result) = result else {
                        if !terminal || index >= commands {
                            return Err(error("PostgreSQL pipeline is missing a command result"));
                        }
                        terminal = false;
                        index += 1;
                        // Each pipeline query resets row mode when its terminal result is consumed.
                        if index < commands {
                            // SAFETY: the separator advanced to the next query before its input is parsed.
                            let mode = unsafe { pq::PQsetSingleRowMode(raw) };
                            // SAFETY: this reads local pipeline state after advancing its queue.
                            let status = unsafe { pq::PQpipelineStatus(raw) };
                            if mode != 1 && status != pq::PGpipelineStatus::PQ_PIPELINE_ABORTED {
                                return Err(error(
                                    "PostgreSQL pipeline row streaming could not be enabled",
                                ));
                            }
                        }
                        continue;
                    };
                    let status = result.status();
                    if status == pq::ExecStatusType::PGRES_PIPELINE_SYNC {
                        if index != commands || terminal {
                            return Err(error("PostgreSQL pipeline did not confirm every command"));
                        }
                        // SAFETY: the synchronization result consumed the last queued operation.
                        if unsafe { pq::PQexitPipelineMode(raw) } != 1 {
                            return Err(error("PostgreSQL pipeline could not return to idle mode"));
                        }
                        connection.phase = Phase::Idle;
                        if committed {
                            connection.raw.take();
                        }
                        return failure.map_or(Ok(()), Err);
                    }
                    if index >= commands || terminal {
                        return Err(error(
                            "PostgreSQL pipeline returned an unexpected command result",
                        ));
                    }
                    match status {
                        pq::ExecStatusType::PGRES_SINGLE_TUPLE
                        | pq::ExecStatusType::PGRES_TUPLES_OK
                        | pq::ExecStatusType::PGRES_COMMAND_OK => {
                            terminal = status != pq::ExecStatusType::PGRES_SINGLE_TUPLE;
                            let commit = terminal && result.command_tag()? == "COMMIT";
                            committed |= commit;
                            if commit {
                                failure.get_or_insert_with(|| {
                                    error("PostgreSQL pipeline COMMIT requires the checked commit boundary")
                                });
                                continue;
                            }
                            // SAFETY: this reads the owned result's allocation size without row contents.
                            if unsafe { pq::PQresultMemorySize(result.raw.as_ptr()) }
                                > MAX_RESULT_BYTES
                            {
                                failure.get_or_insert_with(|| {
                                    error("PostgreSQL pipeline exceeds the admitted row limit")
                                });
                            } else if let Err(error) = validate(index, result) {
                                failure.get_or_insert(error);
                            }
                        }
                        pq::ExecStatusType::PGRES_FATAL_ERROR
                        | pq::ExecStatusType::PGRES_NONFATAL_ERROR
                        | pq::ExecStatusType::PGRES_BAD_RESPONSE => {
                            terminal = true;
                            failure.get_or_insert_with(|| result.failure());
                        }
                        pq::ExecStatusType::PGRES_PIPELINE_ABORTED => {
                            terminal = true;
                            failure.get_or_insert_with(|| {
                                error("PostgreSQL pipeline command was aborted")
                            });
                        }
                        _ => {
                            return Err(error(
                                "PostgreSQL pipeline returned an unsupported result",
                            ));
                        }
                    }
                    continue;
                }
                let events =
                    connection.wait(libc::POLLIN | if writing { libc::POLLOUT } else { 0 }, io)?;
                if events & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                    connection.consume()?;
                }
            }
        });
        self.finished = self.connection.phase == Phase::Idle && self.connection.raw.is_some();
        outcome
    }

    fn fail<T>(&mut self, failure: Error) -> Result<T> {
        self.connection.raw.take();
        self.connection.phase = Phase::Idle;
        Err(failure)
    }

    fn check_capacity(&mut self, bytes: usize) -> Result<()> {
        if self.commands == MAX_COMMANDS || bytes > MAX_INPUT_BYTES.saturating_sub(self.input_bytes)
        {
            return self.fail(error("PostgreSQL pipeline exceeds its bounded input limit"));
        }
        Ok(())
    }
}

impl Drop for Pipeline<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.connection.raw.take();
            self.connection.phase = Phase::Idle;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_control_and_utilities_fail_before_pipeline_dispatch() {
        let io = crate::contracts::IoControl {
            deadline: Instant::now() + std::time::Duration::from_secs(1),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        for sql in [
            "COMMIT",
            "END",
            "ROLLBACK",
            "ABORT",
            "BEGIN",
            "START TRANSACTION",
            "SAVEPOINT x",
            "RELEASE SAVEPOINT x",
            "PREPARE TRANSACTION 'x'",
            "COMMIT PREPARED 'x'",
            "EXECUTE x",
            "DO $$ BEGIN END $$",
            "SET client_min_messages=error",
            "CREATE TABLE x(id int)",
            "/* hidden */ SELECT 1",
            "-- hidden\nSELECT 1",
            "WITHIN x",
            "with x AS (SELECT 1) SELECT * FROM x",
            "",
        ] {
            let mut connection = super::super::tests::unconnected();
            connection.mode = Mode::Sql;
            connection.phase = Phase::Pipeline;
            let mut pipeline = Pipeline {
                connection: &mut connection,
                commands: 0,
                input_bytes: 0,
                finished: false,
            };
            let failure = pipeline.query(sql, &io).unwrap_err();
            assert!(
                failure.message.contains("internal data"),
                "{sql}: {}",
                failure.message
            );
            assert_eq!(pipeline.commands(), 0);
            drop(pipeline);
            assert!(connection.raw.is_none());
        }
    }
}
