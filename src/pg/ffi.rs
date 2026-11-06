use super::replication::{ReplicationState, start_command};
use super::{IdentifySystem, ReplicationFrame, ReplicationPoll, StandbyStatus, error, io};
use crate::contracts::{Conninfo, Error, IoControl, Result};
use pq_sys as pq;
use std::ffi::{CStr, CString, c_void};
use std::marker::PhantomData;
use std::ptr::{self, NonNull};
use std::rc::Rc;
use std::time::Instant;

pub(crate) fn validate_conninfo(conninfo: &Conninfo) -> Result<()> {
    let mut diagnostic = ptr::null_mut();
    // SAFETY: conninfo is terminated, diagnostic is writable, and both outputs are freed once.
    let valid = unsafe {
        let options = pq::PQconninfoParse(conninfo.as_c_str().as_ptr(), &mut diagnostic);
        let valid = !options.is_null();
        pq::PQconninfoFree(options);
        if !diagnostic.is_null() {
            pq::PQfreemem(diagnostic.cast());
        }
        valid
    };
    if valid {
        Ok(())
    } else {
        Err(error(
            "PostgreSQL connection information is invalid; details redacted",
        ))
    }
}

pub(crate) struct PgConnection {
    raw: Option<Connection>,
    phase: Phase,
    mode: Mode,
    replication: ReplicationState,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    Sql,
    Logical,
}

struct Connection {
    raw: NonNull<pq::PGconn>,
    _owner: PhantomData<Rc<()>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: this is the sole owner of this live libpq connection.
        unsafe { pq::PQfinish(self.raw.as_ptr()) };
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Idle,
    Query,
    CopyIn,
    CopyOut,
    CopyBoth,
}

unsafe extern "C" fn discard_notice(_arg: *mut c_void, _message: *const libc::c_char) {
    // Server notices can contain SQL and row data; the default processor prints them.
}
pub(crate) struct PgResult {
    raw: NonNull<pq::PGresult>,
}
pub(crate) struct CopyBuffer {
    raw: NonNull<u8>,
    len: usize,
}

pub(crate) enum PgParam<'a> {
    Null,
    Text(&'a [u8]),
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Binary parameters await serial-applier integration"
        )
    )]
    Binary(&'a [u8]),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransactionStatus {
    Idle,
    Active,
    InTransaction,
    InError,
    Unknown,
}

impl PgConnection {
    pub(crate) fn connect(conninfo: &Conninfo, io: &IoControl) -> Result<Self> {
        Self::connect_mode(conninfo, Mode::Sql, io)
    }

    fn connect_mode(conninfo: &Conninfo, mode: Mode, io: &IoControl) -> Result<Self> {
        io::check(io)?;
        super::native::check()?;
        let raw = match mode {
            Mode::Sql => {
                // SAFETY: Conninfo owns a valid NUL-terminated string for this call.
                unsafe { pq::PQconnectStart(conninfo.as_c_str().as_ptr()) }
            }
            Mode::Logical => {
                validate_conninfo(conninfo)?;
                let keys = [c"dbname".as_ptr(), c"replication".as_ptr(), ptr::null()];
                let values = [
                    conninfo.as_c_str().as_ptr(),
                    c"database".as_ptr(),
                    ptr::null(),
                ];
                // SAFETY: terminated arrays/strings stay live while libpq expands and copies them.
                unsafe { pq::PQconnectStartParams(keys.as_ptr(), values.as_ptr(), 1) }
            }
        };
        let raw =
            NonNull::new(raw).ok_or_else(|| error("PostgreSQL connection allocation failed"))?;
        let owner = Connection {
            raw,
            _owner: PhantomData,
        };
        // SAFETY: owner keeps the connection live; the callback captures no Rust state.
        unsafe { pq::PQsetNoticeProcessor(raw.as_ptr(), Some(discard_notice), ptr::null_mut()) };
        // SAFETY: owner holds a valid connection even when connection startup fails.
        if unsafe { pq::PQstatus(raw.as_ptr()) } == pq::ConnStatusType::CONNECTION_BAD {
            return Err(error(
                "PostgreSQL connection failed; connection details redacted",
            ));
        }
        let mut events = libc::POLLOUT;
        loop {
            // SAFETY: owner holds the connection; libpq may replace its socket between polls.
            let socket = unsafe { pq::PQsocket(raw.as_ptr()) };
            io::wait(socket, events, io)?;
            // SAFETY: the socket is ready for the direction requested by libpq.
            match unsafe { pq::PQconnectPoll(raw.as_ptr()) } {
                pq::PostgresPollingStatusType::PGRES_POLLING_READING => events = libc::POLLIN,
                pq::PostgresPollingStatusType::PGRES_POLLING_WRITING => events = libc::POLLOUT,
                pq::PostgresPollingStatusType::PGRES_POLLING_OK => break,
                _ => {
                    return Err(error(
                        "PostgreSQL connection failed; connection details redacted",
                    ));
                }
            }
        }
        io::check(io)?;
        // SAFETY: connection startup succeeded and owner retains exclusive ownership.
        if unsafe { pq::PQsetnonblocking(raw.as_ptr(), 1) } != 0 {
            return Err(error("PostgreSQL nonblocking mode could not be enabled"));
        }
        Ok(Self {
            raw: Some(owner),
            phase: Phase::Idle,
            mode,
            replication: ReplicationState::default(),
        })
    }
    pub(crate) fn query(&mut self, sql: &str, io: &IoControl) -> Result<PgResult> {
        self.require_mode(Mode::Sql)?;
        self.operation(io, |connection| {
            connection.send(sql, io)?;
            connection.drain(None, io)
        })
    }

    /// Reads libpq's last known state without consuming input or checking server liveness.
    /// A closed connection returns an error rather than a transaction status.
    pub(crate) fn transaction_status(&self) -> Result<TransactionStatus> {
        let raw = self.raw()?;
        // SAFETY: raw() checks that this owner still holds a live PGconn allocation.
        let status = unsafe { pq::PQtransactionStatus(raw) };
        Ok(transaction_status_from_libpq(status))
    }

    /// Statements belong to this connection and must be prepared again after reconnecting.
    /// A zero parameter OID asks PostgreSQL to infer that parameter's type.
    pub(crate) fn prepare(
        &mut self,
        name: &str,
        sql: &str,
        param_types: &[u32],
        io: &IoControl,
    ) -> Result<()> {
        self.require_mode(Mode::Sql)?;
        self.operation(io, |connection| {
            let raw = connection.require(Phase::Idle)?;
            let count = parameter_count(param_types.len())?;
            let name = prepared_cstring(name.as_bytes())?;
            let sql = prepared_cstring(sql.as_bytes())?;
            io::check(io)?;
            connection.phase = Phase::Query;
            // SAFETY: libpq copies the terminated strings and the checked count of OIDs.
            let sent = unsafe {
                pq::PQsendPrepare(
                    raw,
                    name.as_ptr(),
                    sql.as_ptr(),
                    count,
                    param_types.as_ptr(),
                )
            };
            if sent != 1 {
                return Err(error("PostgreSQL prepared statement dispatch failed"));
            }
            connection.flush(io)?;
            let result = connection.drain(None, io)?;
            if result.status() != pq::ExecStatusType::PGRES_COMMAND_OK {
                return Err(error(
                    "PostgreSQL statement preparation did not complete successfully",
                ));
            }
            Ok(())
        })
    }

    /// Libpq copies inputs during dispatch; returned cells borrow only their result.
    /// Drained SQL errors permit rollback; interrupted protocol closes the connection.
    pub(crate) fn execute_prepared(
        &mut self,
        name: &str,
        params: &[PgParam<'_>],
        io: &IoControl,
    ) -> Result<PgResult> {
        self.require_mode(Mode::Sql)?;
        self.operation(io, |connection| {
            let raw = connection.require(Phase::Idle)?;
            let count = parameter_count(params.len())?;
            let name = prepared_cstring(name.as_bytes())?;
            let mut text = Vec::new();
            let mut values = Vec::with_capacity(params.len());
            let mut lengths = Vec::with_capacity(params.len());
            let mut formats = Vec::with_capacity(params.len());
            for param in params {
                let (value, length, format) = match param {
                    PgParam::Null => (ptr::null(), 0, 0),
                    PgParam::Text(bytes) => {
                        let value = prepared_cstring(bytes)?;
                        let pointer = value.as_ptr();
                        // Moving a CString leaves its allocation stable until dispatch completes.
                        text.push(value);
                        (pointer, 0, 0)
                    }
                    PgParam::Binary(bytes) => {
                        (bytes.as_ptr().cast(), parameter_length(bytes.len())?, 1)
                    }
                };
                values.push(value);
                lengths.push(length);
                formats.push(format);
            }
            io::check(io)?;
            connection.phase = Phase::Query;
            // SAFETY: arrays have count entries; text and borrowed binary storage stay live for copying.
            let sent = unsafe {
                pq::PQsendQueryPrepared(
                    raw,
                    name.as_ptr(),
                    count,
                    values.as_ptr(),
                    lengths.as_ptr(),
                    formats.as_ptr(),
                    0,
                )
            };
            if sent != 1 {
                return Err(error("PostgreSQL prepared execution dispatch failed"));
            }
            connection.flush(io)?;
            connection.drain(None, io)
        })
    }

    pub(crate) fn start_copy_out(&mut self, sql: &str, io: &IoControl) -> Result<()> {
        self.start_copy(sql, Phase::CopyOut, pq::ExecStatusType::PGRES_COPY_OUT, io)
    }

    pub(crate) fn read_copy(&mut self, io: &IoControl) -> Result<Option<CopyBuffer>> {
        self.require_mode(Mode::Sql)?;
        self.operation(io, |connection| {
            let raw = connection.require(Phase::CopyOut)?;
            loop {
                io::check(io)?;
                let mut buffer = ptr::null_mut();
                // SAFETY: exclusive COPY OUT connection; libpq writes its allocated buffer pointer.
                let size = unsafe { pq::PQgetCopyData(raw, &mut buffer, 1) };
                match size {
                    1.. => {
                        let raw = NonNull::new(buffer.cast())
                            .ok_or_else(|| error("PostgreSQL returned a missing COPY buffer"))?;
                        return Ok(Some(CopyBuffer {
                            raw,
                            len: size as usize,
                        }));
                    }
                    0 => {
                        connection.wait(libc::POLLIN, io)?;
                        connection.consume()?;
                    }
                    -1 => {
                        connection.phase = Phase::Query;
                        let result = connection.drain(None, io)?;
                        result.copy_count()?;
                        return Ok(None);
                    }
                    _ => {
                        connection.phase = Phase::Query;
                        return match connection.drain(None, io) {
                            Err(failure) => Err(failure),
                            Ok(_) => Err(error("PostgreSQL COPY read failed")),
                        };
                    }
                }
            }
        })
    }

    pub(crate) fn start_copy_in(&mut self, sql: &str, io: &IoControl) -> Result<()> {
        self.start_copy(sql, Phase::CopyIn, pq::ExecStatusType::PGRES_COPY_IN, io)
    }

    pub(crate) fn write_copy(&mut self, data: &[u8], io: &IoControl) -> Result<()> {
        self.require_mode(Mode::Sql)?;
        self.operation(io, |connection| {
            let raw = connection.require(Phase::CopyIn)?;
            // Flush each chunk to bound libpq's queue even when the caller holds an oversized row.
            for chunk in data.chunks(64 * 1024) {
                loop {
                    io::check(io)?;
                    connection.consume()?;
                    // SAFETY: chunk is valid for its bounded signed-int length, and libpq copies it.
                    match unsafe {
                        pq::PQputCopyData(raw, chunk.as_ptr().cast(), chunk.len() as i32)
                    } {
                        1 => break,
                        0 => {
                            connection.flush(io)?;
                            connection.wait(libc::POLLIN | libc::POLLOUT, io)?;
                        }
                        _ => return Err(error("PostgreSQL COPY write failed")),
                    }
                }
                connection.flush(io)?;
            }
            Ok(())
        })
    }

    pub(crate) fn finish_copy_in(&mut self, io: &IoControl) -> Result<u64> {
        self.require_mode(Mode::Sql)?;
        self.operation(io, |connection| {
            connection.end_copy(None, io)?;
            connection.drain(None, io)?.copy_count()
        })
    }

    pub(crate) fn abort_copy_in(&mut self, _reason: &str, io: &IoControl) -> Result<()> {
        self.require_mode(Mode::Sql)?;
        self.operation(io, |connection| {
            // An arbitrary caller reason could contain row values or connection secrets.
            connection.end_copy(Some(c"rpgmigrate aborted COPY"), io)?;
            let result = connection.drain(None, io);
            if connection.phase == Phase::Idle {
                match result {
                    Err(_) => Ok(()),
                    Ok(_) => Err(error("PostgreSQL unexpectedly accepted an aborted COPY")),
                }
            } else {
                result.map(|_| ())
            }
        })
    }

    pub(crate) fn quote_identifier(&mut self, name: &str) -> Result<String> {
        let raw = self.raw()?;
        if name.as_bytes().contains(&0) {
            return Err(error("PostgreSQL identifier contains a NUL byte"));
        }
        // SAFETY: libpq reads name.len() valid bytes, and returns an owned NUL-terminated allocation.
        let quoted = unsafe { pq::PQescapeIdentifier(raw, name.as_ptr().cast(), name.len()) };
        let quoted =
            NonNull::new(quoted).ok_or_else(|| error("PostgreSQL identifier quoting failed"))?;
        // SAFETY: the non-null libpq allocation contains a terminated C string.
        let bytes = unsafe { CStr::from_ptr(quoted.as_ptr()) }.to_bytes();
        let owned = CopyBuffer {
            raw: quoted.cast(),
            len: bytes.len(),
        };
        std::str::from_utf8(owned.as_bytes())
            .map(str::to_owned)
            .map_err(|_| error("PostgreSQL returned an invalid quoted identifier"))
    }

    fn raw(&self) -> Result<*mut pq::PGconn> {
        self.raw
            .as_ref()
            .map(|owner| owner.raw.as_ptr())
            .ok_or_else(|| error("PostgreSQL connection is closed"))
    }

    fn require_mode(&self, mode: Mode) -> Result<()> {
        if self.mode == mode {
            Ok(())
        } else {
            Err(error(
                "PostgreSQL operation is invalid for this connection mode",
            ))
        }
    }

    /// Opens a dedicated logical session with replication=database overriding conninfo.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Logical source integration is pending")
    )]
    pub(crate) fn connect_replication(conninfo: &Conninfo, io: &IoControl) -> Result<Self> {
        Self::connect_mode(conninfo, Mode::Logical, io)
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Logical source integration is pending")
    )]
    pub(crate) fn identify_system(&mut self, io: &IoControl) -> Result<IdentifySystem> {
        self.require_mode(Mode::Logical)?;
        self.require(Phase::Idle)?;
        self.operation(io, |connection| {
            connection.send_replication(c"IDENTIFY_SYSTEM", io)?;
            let result = connection.drain(None, io)?;
            // SAFETY: the result owns a live PGresult; dimensions are signed nonnegative ints.
            let columns = unsafe { pq::PQnfields(result.raw.as_ptr()) };
            if result.status() != pq::ExecStatusType::PGRES_TUPLES_OK
                || result.rows() != 1
                || columns != 4
            {
                return Err(error(
                    "PostgreSQL replication identity has an invalid result shape",
                ));
            }
            IdentifySystem::parse(
                1,
                &[
                    result.cell(0, 0)?,
                    result.cell(0, 1)?,
                    result.cell(0, 2)?,
                    result.cell(0, 3)?,
                ],
            )
        })
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Logical source integration is pending")
    )]
    pub(crate) fn start_replication(
        &mut self,
        slot: &str,
        start: u64,
        options: &[(&str, Option<&str>)],
        io: &IoControl,
    ) -> Result<()> {
        self.require_mode(Mode::Logical)?;
        self.require(Phase::Idle)?;
        let command = start_command(slot, start, options)?;
        self.operation(io, |connection| {
            connection.send_replication(&command, io)?;
            let result = connection
                .next_result(io)?
                .ok_or_else(|| error("PostgreSQL replication startup returned no result"))?;
            if result.status() == pq::ExecStatusType::PGRES_COPY_BOTH {
                connection.phase = Phase::CopyBoth;
                connection.replication = ReplicationState::default();
                return Ok(());
            }
            connection.drain(Some(result), io)?;
            Err(error(
                "PostgreSQL replication startup did not enter CopyBoth",
            ))
        })
    }

    /// False means NOT accepted: the existing pending status remains unchanged.
    /// Positions belong to the caller's durability policy, never to received frames.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Logical source integration is pending")
    )]
    pub(crate) fn queue_feedback(&mut self, status: StandbyStatus) -> Result<bool> {
        self.require_mode(Mode::Logical)?;
        self.require(Phase::CopyBoth)?;
        if self.replication.ending {
            return Err(error("PostgreSQL replication is ending"));
        }
        Ok(self.replication.queue(status))
    }

    /// Begins graceful shutdown; continue polling until End or an error.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Logical source integration is pending")
    )]
    pub(crate) fn request_replication_end(&mut self) -> Result<()> {
        self.require_mode(Mode::Logical)?;
        self.require(Phase::CopyBoth)?;
        self.replication.ending = true;
        Ok(())
    }

    /// FeedbackDrained leaves all delivery and feedback-position policy with the caller.
    /// End requires every terminal result; caller wakes preserve the absolute session deadline.
    /// Streaming errors close the connection.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Logical source integration is pending")
    )]
    pub(crate) fn poll_replication(
        &mut self,
        wake_at: Instant,
        io: &IoControl,
    ) -> Result<ReplicationPoll> {
        self.require_mode(Mode::Logical)?;
        self.require(Phase::CopyBoth)?;
        self.operation(io, |connection| connection.pump_replication(wake_at, io))
    }

    fn send_replication(&mut self, command: &CStr, io: &IoControl) -> Result<()> {
        let raw = self.require(Phase::Idle)?;
        io::check(io)?;
        self.phase = Phase::Query;
        // SAFETY: libpq copies the internal terminated command; walsender requires simple query.
        if unsafe { pq::PQsendQuery(raw, command.as_ptr()) } != 1 {
            return Err(error("PostgreSQL replication command dispatch failed"));
        }
        self.flush(io)
    }

    fn drained_feedback(&mut self) -> Option<ReplicationPoll> {
        if self
            .replication
            .pending
            .as_ref()
            .is_some_and(|pending| pending.drained)
        {
            self.replication
                .pending
                .take()
                .map(|pending| ReplicationPoll::FeedbackDrained(pending.status))
        } else {
            None
        }
    }

    fn replication_output(&mut self) -> Result<bool> {
        let raw = self.raw()?;
        if let Some(pending) = self
            .replication
            .pending
            .as_mut()
            .filter(|pending| !pending.queued)
        {
            let bytes = pending.status.encode();
            // SAFETY: libpq copies exactly one 34-byte feedback; no second status can queue yet.
            match unsafe { pq::PQputCopyData(raw, bytes.as_ptr().cast(), 34) } {
                1 => pending.queued = true,
                0 => {}
                _ => return Err(error("PostgreSQL replication feedback output failed")),
            }
        }
        if self.replication.ending
            && self.replication.pending.is_none()
            && !self.replication.sent_end
        {
            // SAFETY: this exclusive stream is ending; NULL requests a successful CopyDone.
            match unsafe { pq::PQputCopyEnd(raw, ptr::null()) } {
                1 => self.replication.sent_end = true,
                0 => {}
                _ => return Err(error("PostgreSQL replication termination failed")),
            }
        }
        // SAFETY: the live connection is nonblocking and owned by this thread.
        let flushing = match unsafe { pq::PQflush(raw) } {
            0 => {
                // Libpq may discard write-failed output and still return zero.
                if let Some(pending) = self
                    .replication
                    .pending
                    .as_mut()
                    .filter(|pending| pending.queued)
                {
                    pending.drained = true;
                }
                false
            }
            1 => {
                #[cfg(test)]
                {
                    self.replication.flush_pending += 1;
                }
                true
            }
            _ => return Err(error("PostgreSQL replication output flush failed")),
        };
        Ok(flushing
            || self
                .replication
                .pending
                .as_ref()
                .is_some_and(|pending| !pending.queued)
            || (self.replication.ending
                && !self.replication.sent_end
                && self.replication.pending.is_none()))
    }

    fn pump_replication(&mut self, wake_at: Instant, io: &IoControl) -> Result<ReplicationPoll> {
        let raw = self.raw()?;
        if Instant::now() >= wake_at {
            return Ok(ReplicationPoll::Wake);
        }
        if let Some(event) = self.drained_feedback() {
            return Ok(event);
        }
        let mut consumed = false;
        loop {
            io::check(io)?;
            if Instant::now() >= wake_at {
                return Ok(ReplicationPoll::Wake);
            }
            let mut complete = false;
            if self.replication.received_end {
                // CopyDone leaves libpq in COPY_IN until our CopyDone; ErrorResponse leaves BUSY/READY.
                // Inspect terminal input before blocked output can hide a server error.
                // SAFETY: PQisBusy gates nonblocking PQgetResult on this owned connection.
                if unsafe { pq::PQisBusy(raw) } == 0 {
                    // SAFETY: a non-null result transfers exclusive ownership to PgResult.
                    let result =
                        NonNull::new(unsafe { pq::PQgetResult(raw) }).map(|raw| PgResult { raw });
                    if let Some(result) = result {
                        match result.status() {
                            pq::ExecStatusType::PGRES_COPY_IN if !self.replication.sent_end => {}
                            pq::ExecStatusType::PGRES_COMMAND_OK if self.replication.sent_end => {
                                self.replication
                                    .terminals
                                    .observe(result.command_tag().ok());
                                continue;
                            }
                            pq::ExecStatusType::PGRES_COPY_IN
                            | pq::ExecStatusType::PGRES_COPY_OUT
                            | pq::ExecStatusType::PGRES_COPY_BOTH => {
                                return Err(error(
                                    "PostgreSQL replication completion unexpectedly entered COPY",
                                ));
                            }
                            _ => return Err(result.failure()),
                        }
                    } else {
                        self.replication.terminals.finish()?;
                        complete = true;
                    }
                }
            }
            let sent_end = self.replication.sent_end;
            let writing = self.replication_output()?;
            if complete && !writing && self.replication.pending.is_none() {
                self.phase = Phase::Idle;
                return Ok(ReplicationPoll::End);
            }
            if !self.replication.received_end {
                let mut buffer = ptr::null_mut();
                // SAFETY: asynchronous COPY returns either one owned allocation or a status code.
                match unsafe { pq::PQgetCopyData(raw, &mut buffer, 1) } {
                    size @ 1.. => {
                        let raw = NonNull::new(buffer.cast()).ok_or_else(|| {
                            error("PostgreSQL returned a missing replication buffer")
                        })?;
                        return ReplicationFrame::new(CopyBuffer {
                            raw,
                            len: size as usize,
                        })
                        .map(ReplicationPoll::Frame);
                    }
                    0 if !consumed => {
                        self.consume()?;
                        consumed = true;
                        continue;
                    }
                    0 => {}
                    -1 => {
                        self.replication.received_end = true;
                        self.replication.ending = true;
                        continue;
                    }
                    _ => return Err(error("PostgreSQL replication input failed")),
                }
            }
            if let Some(event) = self.drained_feedback() {
                return Ok(event);
            }
            if !sent_end && self.replication.sent_end {
                continue;
            }
            // SAFETY: raw remains owned; poll still observes both directions while output is blocked.
            let socket = unsafe { pq::PQsocket(raw) };
            let events = libc::POLLIN | if writing { libc::POLLOUT } else { 0 };
            match io::wait_until(socket, events, wake_at, io)? {
                Some(ready) => {
                    if ready & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                        self.consume()?;
                        consumed = true;
                    }
                }
                None => return Ok(ReplicationPoll::Wake),
            }
        }
    }

    fn require(&self, phase: Phase) -> Result<*mut pq::PGconn> {
        let raw = self.raw()?;
        if self.phase != phase {
            return Err(error(
                "PostgreSQL operation is invalid in the current protocol state",
            ));
        }
        Ok(raw)
    }

    fn operation<T>(
        &mut self,
        io: &IoControl,
        work: impl FnOnce(&mut Self) -> Result<T>,
    ) -> Result<T> {
        let outcome = io::check(io).and_then(|()| work(self));
        if outcome.is_err() && self.phase != Phase::Idle {
            // An interrupted protocol cannot safely serve another command or claim a rollback.
            self.raw.take();
            self.phase = Phase::Idle;
        }
        outcome
    }

    fn wait(&self, events: libc::c_short, io: &IoControl) -> Result<libc::c_short> {
        // SAFETY: raw() checks this thread still owns a live connection.
        let socket = unsafe { pq::PQsocket(self.raw()?) };
        io::wait(socket, events, io)
    }

    fn consume(&mut self) -> Result<()> {
        // SAFETY: raw() checks exclusive ownership; nonblocking mode is set during connection.
        if unsafe { pq::PQconsumeInput(self.raw()?) } != 1 {
            return Err(error("PostgreSQL connection input failed"));
        }
        Ok(())
    }

    fn flush(&mut self, io: &IoControl) -> Result<()> {
        let raw = self.raw()?;
        loop {
            io::check(io)?;
            // SAFETY: raw is exclusively owned and the connection is nonblocking.
            match unsafe { pq::PQflush(raw) } {
                0 => return Ok(()),
                1 => {
                    let events = self.wait(libc::POLLIN | libc::POLLOUT, io)?;
                    if events & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) != 0 {
                        self.consume()?;
                        // SAFETY: parse buffered notices so a blocked writer cannot deadlock the peer.
                        unsafe { pq::PQisBusy(raw) };
                    }
                }
                _ => return Err(error("PostgreSQL connection output failed")),
            }
        }
    }

    fn send(&mut self, sql: &str, io: &IoControl) -> Result<()> {
        let raw = self.require(Phase::Idle)?;
        let sql = CString::new(sql).map_err(|_| error("PostgreSQL command contains a NUL byte"))?;
        self.phase = Phase::Query;
        // SAFETY: libpq copies this C string; zero parameters require no parameter arrays.
        let sent = unsafe {
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
        };
        if sent != 1 {
            return Err(error("PostgreSQL command dispatch failed"));
        }
        self.flush(io)
    }

    fn next_result(&mut self, io: &IoControl) -> Result<Option<PgResult>> {
        let raw = self.raw()?;
        loop {
            io::check(io)?;
            // SAFETY: libpq parses buffered input and reports whether PQgetResult would block.
            if unsafe { pq::PQisBusy(raw) } == 0 {
                // SAFETY: PQisBusy proved this nonblocking; the returned result transfers ownership.
                return Ok(
                    NonNull::new(unsafe { pq::PQgetResult(raw) }).map(|raw| PgResult { raw })
                );
            }
            self.wait(libc::POLLIN, io)?;
            self.consume()?;
        }
    }

    fn drain(&mut self, mut first: Option<PgResult>, io: &IoControl) -> Result<PgResult> {
        let mut value = None;
        let mut failure = None;
        loop {
            let result = match first.take() {
                Some(result) => Some(result),
                None => self.next_result(io)?,
            };
            let Some(result) = result else { break };
            match result.status() {
                pq::ExecStatusType::PGRES_COMMAND_OK | pq::ExecStatusType::PGRES_TUPLES_OK => {
                    if value.is_some() {
                        failure.get_or_insert_with(|| {
                            error("PostgreSQL returned more than one command result")
                        });
                    } else {
                        value = Some(result);
                    }
                }
                pq::ExecStatusType::PGRES_COPY_IN
                | pq::ExecStatusType::PGRES_COPY_OUT
                | pq::ExecStatusType::PGRES_COPY_BOTH => {
                    return Err(error(
                        "PostgreSQL COPY requires the matching COPY operation",
                    ));
                }
                _ => {
                    failure.get_or_insert_with(|| result.failure());
                }
            }
        }
        self.phase = Phase::Idle;
        if let Some(failure) = failure {
            return Err(failure);
        }
        value.ok_or_else(|| error("PostgreSQL command returned no result"))
    }

    fn start_copy(
        &mut self,
        sql: &str,
        phase: Phase,
        status: pq::ExecStatusType,
        io: &IoControl,
    ) -> Result<()> {
        self.require_mode(Mode::Sql)?;
        self.operation(io, |connection| {
            connection.send(sql, io)?;
            let result = connection
                .next_result(io)?
                .ok_or_else(|| error("PostgreSQL COPY returned no initial result"))?;
            if result.status() == status {
                connection.phase = phase;
                return Ok(());
            }
            connection.drain(Some(result), io)?;
            Err(error(
                "PostgreSQL command did not start the requested COPY operation",
            ))
        })
    }

    fn end_copy(&mut self, reason: Option<&CStr>, io: &IoControl) -> Result<()> {
        let raw = self.require(Phase::CopyIn)?;
        loop {
            io::check(io)?;
            // SAFETY: the connection is in COPY IN; the optional reason remains a valid C string.
            match unsafe { pq::PQputCopyEnd(raw, reason.map_or(ptr::null(), CStr::as_ptr)) } {
                1 => break,
                0 => {
                    self.flush(io)?;
                    self.wait(libc::POLLIN | libc::POLLOUT, io)?;
                    self.consume()?;
                }
                _ => return Err(error("PostgreSQL COPY termination failed")),
            }
        }
        self.phase = Phase::Query;
        self.flush(io)
    }
}

impl PgResult {
    fn status(&self) -> pq::ExecStatusType {
        // SAFETY: this result owns a live libpq allocation.
        unsafe { pq::PQresultStatus(self.raw.as_ptr()) }
    }

    fn failure(&self) -> Error {
        // SAFETY: the optional SQLSTATE C string belongs to this live result and is copied below.
        let state =
            unsafe { pq::PQresultErrorField(self.raw.as_ptr(), pq::PG_DIAG_SQLSTATE.into()) };
        if !state.is_null() {
            // SAFETY: libpq returned a non-null, terminated error field owned by this result.
            let bytes = unsafe { CStr::from_ptr(state) }.to_bytes();
            if bytes.len() == 5
                && bytes
                    .iter()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
            {
                return error(&format!(
                    "PostgreSQL command failed (SQLSTATE {})",
                    std::str::from_utf8(bytes).unwrap()
                ));
            }
        }
        error("PostgreSQL command failed; server details redacted")
    }

    fn copy_count(&self) -> Result<u64> {
        if self.status() != pq::ExecStatusType::PGRES_COMMAND_OK {
            return Err(error("PostgreSQL COPY did not complete successfully"));
        }
        copy_rows(self.command_tag()?)
    }
    pub(crate) fn rows(&self) -> usize {
        // SAFETY: this result owns a live libpq allocation.
        unsafe { pq::PQntuples(self.raw.as_ptr()) as usize }
    }

    pub(crate) fn cell(&self, row: usize, column: usize) -> Result<Option<&[u8]>> {
        // SAFETY: this result owns a live libpq allocation.
        let columns = unsafe { pq::PQnfields(self.raw.as_ptr()) } as usize;
        if row >= self.rows() || column >= columns {
            return Err(error("PostgreSQL result cell is out of bounds"));
        }
        // SAFETY: indices were bounded by libpq's signed-int dimensions; the borrow cannot outlive self.
        unsafe {
            if pq::PQgetisnull(self.raw.as_ptr(), row as i32, column as i32) != 0 {
                return Ok(None);
            }
            let len = pq::PQgetlength(self.raw.as_ptr(), row as i32, column as i32);
            let value = pq::PQgetvalue(self.raw.as_ptr(), row as i32, column as i32);
            Ok(Some(std::slice::from_raw_parts(value.cast(), len as usize)))
        }
    }

    pub(crate) fn command_tag(&self) -> Result<&str> {
        // SAFETY: PQcmdStatus returns a C string owned by this live result.
        unsafe { CStr::from_ptr(pq::PQcmdStatus(self.raw.as_ptr())) }
            .to_str()
            .map_err(|_| error("PostgreSQL returned an invalid command tag"))
    }
}

impl Drop for PgResult {
    fn drop(&mut self) {
        // SAFETY: this is the sole owner of this live PGresult.
        unsafe { pq::PQclear(self.raw.as_ptr()) };
    }
}

impl CopyBuffer {
    pub(crate) fn as_bytes(&self) -> &[u8] {
        // SAFETY: libpq allocated len initialized bytes, exclusively owned until this buffer drops.
        unsafe { std::slice::from_raw_parts(self.raw.as_ptr(), self.len) }
    }
}

impl Drop for CopyBuffer {
    fn drop(&mut self) {
        // SAFETY: this allocation came from libpq and has exactly one owner.
        unsafe { pq::PQfreemem(self.raw.as_ptr().cast()) };
    }
}

fn copy_rows(tag: &str) -> Result<u64> {
    let count = tag
        .strip_prefix("COPY ")
        .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| error("PostgreSQL COPY completion has no valid row count"))?;
    count
        .parse()
        .map_err(|_| error("PostgreSQL COPY row count exceeds u64"))
}

fn transaction_status_from_libpq(status: pq::PGTransactionStatusType) -> TransactionStatus {
    match status {
        pq::PGTransactionStatusType::PQTRANS_IDLE => TransactionStatus::Idle,
        pq::PGTransactionStatusType::PQTRANS_ACTIVE => TransactionStatus::Active,
        pq::PGTransactionStatusType::PQTRANS_INTRANS => TransactionStatus::InTransaction,
        pq::PGTransactionStatusType::PQTRANS_INERROR => TransactionStatus::InError,
        pq::PGTransactionStatusType::PQTRANS_UNKNOWN => TransactionStatus::Unknown,
    }
}

fn parameter_count(count: usize) -> Result<i32> {
    if count > pq::PQ_QUERY_PARAM_MAX_LIMIT as usize {
        return Err(error(
            "PostgreSQL parameter count exceeds the protocol limit",
        ));
    }
    i32::try_from(count).map_err(|_| error("PostgreSQL parameter count exceeds the protocol limit"))
}

fn parameter_length(length: usize) -> Result<i32> {
    i32::try_from(length)
        .map_err(|_| error("PostgreSQL parameter length exceeds the protocol limit"))
}

fn prepared_cstring_length(length: usize) -> Result<usize> {
    let terminated = length
        .checked_add(1)
        .ok_or_else(|| error("PostgreSQL prepared input length exceeds the protocol limit"))?;
    parameter_length(terminated)?;
    Ok(terminated)
}

fn prepared_cstring(bytes: &[u8]) -> Result<CString> {
    prepared_cstring_length(bytes.len())?;
    CString::new(bytes).map_err(|_| error("PostgreSQL prepared input contains a NUL byte"))
}

#[cfg(test)]
pub(super) mod replication_testing {
    use super::*;

    pub(in crate::pg) fn owned_frame(bytes: &[u8]) -> super::super::ReplicationFrame {
        let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        let text = CString::new(format!("\\x{hex}")).unwrap();
        let mut len = 0;
        // SAFETY: libpq owns the decoded allocation; CopyBuffer frees it exactly once.
        let raw = unsafe { pq::PQunescapeBytea(text.as_ptr().cast(), &mut len) };
        super::super::ReplicationFrame::new(CopyBuffer {
            raw: NonNull::new(raw).unwrap(),
            len,
        })
        .unwrap()
    }

    pub(in crate::pg) fn create_slot(
        connection: &mut PgConnection,
        slot: &str,
        io: &IoControl,
    ) -> Result<()> {
        start_command(slot, 0, &[])?;
        connection.require_mode(Mode::Logical)?;
        let raw = connection.require(Phase::Idle)?;
        // SAFETY: version lookup reads this owned live connection without dispatch.
        let modern = unsafe { pq::PQserverVersion(raw) } >= 100000;
        let command = if modern {
            format!(
                "CREATE_REPLICATION_SLOT \"{slot}\" TEMPORARY LOGICAL test_decoding NOEXPORT_SNAPSHOT"
            )
        } else {
            // PostgreSQL 9.6 lacks temporary slots; its entire test cluster is disposable.
            format!("CREATE_REPLICATION_SLOT \"{slot}\" LOGICAL test_decoding")
        };
        connection.operation(io, |connection| {
            connection.send_replication(&CString::new(command).unwrap(), io)?;
            let result = connection.drain(None, io)?;
            if result.status() != pq::ExecStatusType::PGRES_TUPLES_OK
                || result.rows() != 1
                || result.cell(0, 0)? != Some(slot.as_bytes())
            {
                return Err(error(
                    "owned replication slot setup returned an invalid result",
                ));
            }
            Ok(())
        })
    }

    pub(in crate::pg) fn small_send_buffer(connection: &PgConnection) -> Result<()> {
        let raw = connection.raw()?;
        let size: libc::c_int = 4096;
        // SAFETY: this socket belongs to the test connection; the integer has its exact native size.
        if unsafe {
            libc::setsockopt(
                pq::PQsocket(raw),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&size as *const libc::c_int).cast(),
                std::mem::size_of_val(&size) as libc::socklen_t,
            )
        } != 0
        {
            return Err(error("owned replication socket setup failed"));
        }
        Ok(())
    }

    pub(in crate::pg) fn flush_pending(connection: &PgConnection) -> usize {
        connection.replication.flush_pending
    }

    pub(in crate::pg) fn feedback_waiting_for_drain(connection: &PgConnection) -> bool {
        connection
            .replication
            .pending
            .as_ref()
            .is_some_and(|pending| pending.queued && !pending.drained)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    fn control() -> IoControl {
        IoControl {
            deadline: Instant::now() + Duration::from_secs(1),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn replication_mode_errors_dispatch_nothing() {
        let io = control();
        let mut connection = PgConnection {
            raw: None,
            phase: Phase::Idle,
            mode: Mode::Sql,
            replication: ReplicationState::default(),
        };
        assert!(
            connection
                .identify_system(&io)
                .unwrap_err()
                .message
                .contains("mode")
        );
        assert!(
            connection
                .start_replication("slot", 0, &[], &io)
                .unwrap_err()
                .message
                .contains("mode")
        );
        assert!(
            connection
                .request_replication_end()
                .unwrap_err()
                .message
                .contains("mode")
        );
        connection.mode = Mode::Logical;
        assert!(
            connection
                .query("SELECT 1", &io)
                .err()
                .unwrap()
                .message
                .contains("mode")
        );
        assert!(
            connection
                .prepare("p", "SELECT 1", &[], &io)
                .unwrap_err()
                .message
                .contains("mode")
        );
        assert!(
            connection
                .execute_prepared("p", &[], &io)
                .err()
                .unwrap()
                .message
                .contains("mode")
        );
        assert!(
            connection
                .start_copy_out("COPY t TO STDOUT", &io)
                .unwrap_err()
                .message
                .contains("mode")
        );
        assert!(
            connection
                .start_copy_in("COPY t FROM STDIN", &io)
                .unwrap_err()
                .message
                .contains("mode")
        );
    }

    #[test]
    fn native_libpq_is_linked() {
        assert!(super::super::libpq_version() >= 180000);
    }

    #[test]
    fn transaction_status_maps_every_native_state() {
        for (native, expected) in [
            (
                pq::PGTransactionStatusType::PQTRANS_IDLE,
                TransactionStatus::Idle,
            ),
            (
                pq::PGTransactionStatusType::PQTRANS_ACTIVE,
                TransactionStatus::Active,
            ),
            (
                pq::PGTransactionStatusType::PQTRANS_INTRANS,
                TransactionStatus::InTransaction,
            ),
            (
                pq::PGTransactionStatusType::PQTRANS_INERROR,
                TransactionStatus::InError,
            ),
            (
                pq::PGTransactionStatusType::PQTRANS_UNKNOWN,
                TransactionStatus::Unknown,
            ),
        ] {
            assert_eq!(transaction_status_from_libpq(native), expected);
        }
    }

    #[test]
    fn transaction_status_rejects_a_closed_connection() {
        let connection = PgConnection {
            raw: None,
            phase: Phase::Idle,
            mode: Mode::Sql,
            replication: ReplicationState::default(),
        };
        let failure = connection.transaction_status().unwrap_err();
        assert_eq!(failure.kind, crate::contracts::ErrorKind::Pgsql);
        assert!(failure.message.contains("closed"));
    }

    #[test]
    fn conninfo_validation_uses_libpq_without_resolving_external_settings() {
        for value in [
            "host=/rpgmigrate-no-such-private-socket port=1",
            "service=rpgmigrate-nonexistent-service passfile=/rpgmigrate-unread-passfile",
            "postgresql://private-user:private-password@example.invalid:12345/private-db?sslmode=require",
        ] {
            validate_conninfo(&Conninfo::new(value.into()).unwrap()).unwrap();
        }
        for value in [
            "not_a_libpq_setting=private-secret",
            "password='private-secret",
            "postgresql://private-user:private-secret@[broken",
        ] {
            let error = validate_conninfo(&Conninfo::new(value.into()).unwrap()).unwrap_err();
            assert!(!format!("{error:?}").contains("private"));
            assert_eq!(error.kind, crate::contracts::ErrorKind::Pgsql);
        }
    }

    #[test]
    fn binary_cells_preserve_nuls_and_distinguish_null_from_empty() {
        // SAFETY: a null connection creates a standalone, exclusively owned PGresult.
        let raw = unsafe {
            pq::PQmakeEmptyPGresult(ptr::null_mut(), pq::ExecStatusType::PGRES_TUPLES_OK)
        };
        let result = PgResult {
            raw: NonNull::new(raw).unwrap(),
        };
        let mut attribute = pq::PGresAttDesc {
            name: c"value".as_ptr().cast_mut(),
            tableid: 0,
            columnid: 0,
            format: 1,
            typid: 17,
            typlen: -1,
            atttypmod: -1,
        };
        let mut bytes = [0, 255, b'x'];
        // SAFETY: libpq copies each descriptor/value; all pointers and lengths are valid.
        unsafe {
            assert_eq!(pq::PQsetResultAttrs(raw, 1, &mut attribute), 1);
            assert_eq!(pq::PQsetvalue(raw, 0, 0, bytes.as_mut_ptr().cast(), 3), 1);
            assert_eq!(pq::PQsetvalue(raw, 1, 0, ptr::null_mut(), -1), 1);
            assert_eq!(pq::PQsetvalue(raw, 2, 0, bytes.as_mut_ptr().cast(), 0), 1);
        }
        assert_eq!(result.rows(), 3);
        assert_eq!(result.cell(0, 0).unwrap(), Some(&[0, 255, b'x'][..]));
        assert_eq!(result.cell(1, 0).unwrap(), None);
        assert_eq!(result.cell(2, 0).unwrap(), Some(&[][..]));
        assert!(result.cell(3, 0).is_err());
        assert!(result.cell(0, 1).is_err());
        assert!(result.cell(usize::MAX, 0).is_err());
        assert!(result.command_tag().unwrap().is_empty());
    }

    #[test]
    fn owned_copy_bytes_preserve_non_utf8_and_nuls() {
        let mut len = 0;
        // SAFETY: a valid bytea C string and writable length are passed to libpq.
        let raw = unsafe { pq::PQunescapeBytea(c"\\000\\377x".as_ptr().cast(), &mut len) };
        let buffer = CopyBuffer {
            raw: NonNull::new(raw).unwrap(),
            len,
        };
        assert_eq!(buffer.as_bytes(), &[0, 255, b'x']);
    }

    #[test]
    fn closed_connection_rejects_every_protocol_operation() {
        let mut connection = PgConnection {
            raw: None,
            phase: Phase::Idle,
            mode: Mode::Sql,
            replication: ReplicationState::default(),
        };
        let io = control();
        assert!(connection.query("SELECT 1", &io).is_err());
        assert!(connection.prepare("p", "SELECT 1", &[], &io).is_err());
        assert!(connection.execute_prepared("p", &[], &io).is_err());
        assert!(connection.start_copy_out("COPY t TO STDOUT", &io).is_err());
        assert!(connection.read_copy(&io).is_err());
        assert!(connection.start_copy_in("COPY t FROM STDIN", &io).is_err());
        assert!(connection.write_copy(b"1\n", &io).is_err());
        assert!(connection.finish_copy_in(&io).is_err());
        assert!(connection.abort_copy_in("secret reason", &io).is_err());
        assert!(connection.quote_identifier("t").is_err());
    }

    #[test]
    fn prepared_parameter_counts_respect_the_protocol_limit() {
        assert_eq!(parameter_count(0).unwrap(), 0);
        assert_eq!(parameter_count(65_535).unwrap(), 65_535);
        assert!(parameter_count(65_536).is_err());
        assert!(parameter_count(usize::MAX).is_err());
    }

    #[test]
    fn prepared_parameter_lengths_do_not_wrap() {
        assert_eq!(parameter_length(0).unwrap(), 0);
        assert_eq!(parameter_length(i32::MAX as usize).unwrap(), i32::MAX);
        assert!(parameter_length(i32::MAX as usize + 1).is_err());
        assert!(parameter_length(usize::MAX).is_err());
    }

    #[test]
    fn prepared_text_length_reserves_its_terminator_without_overflow() {
        assert_eq!(prepared_cstring_length(0).unwrap(), 1);
        assert_eq!(
            prepared_cstring_length(i32::MAX as usize - 1).unwrap(),
            i32::MAX as usize
        );
        assert!(prepared_cstring_length(i32::MAX as usize).is_err());
        assert!(prepared_cstring_length(usize::MAX).is_err());
    }

    #[test]
    fn copy_completion_requires_a_valid_nonoverflowing_copy_count() {
        assert_eq!(copy_rows("COPY 0").unwrap(), 0);
        assert_eq!(copy_rows("COPY 18446744073709551615").unwrap(), u64::MAX);
        for invalid in [
            "",
            "COMMIT",
            "COPY",
            "COPY ",
            "COPY -1",
            "COPY +1",
            "COPY 1 2",
            "COPY 18446744073709551616",
        ] {
            assert!(copy_rows(invalid).is_err(), "accepted invalid COPY count");
        }
    }

    #[test]
    fn invalid_private_socket_diagnostics_do_not_reveal_credentials() {
        let conninfo = Conninfo::new("host=/rpgmigrate-no-such-private-socket port=1 user=private-user password=private-password dbname=private-db".into()).unwrap();
        let error = match PgConnection::connect(&conninfo, &control()) {
            Ok(_) => panic!("nonexistent private socket unexpectedly connected"),
            Err(error) => error,
        };
        let diagnostic = format!("{error:?}");
        for secret in [
            "private-user",
            "private-password",
            "private-db",
            "no-such-private-socket",
        ] {
            assert!(!diagnostic.contains(secret));
        }
        assert_eq!(error.kind, crate::contracts::ErrorKind::Pgsql);
    }
}
