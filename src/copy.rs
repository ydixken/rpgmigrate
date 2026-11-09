use std::sync::atomic::Ordering;

use crate::catalog::{command, configure_durability, confirm_durability, qualified, rollback};
use crate::contracts::{
    CopyFormat, CopyPlan, CopyReport, CopyTable, Error, ErrorKind, IoControl, Result,
};
use crate::pg::PgConnection;
use crate::state::NativeState;

mod clone;
pub(crate) use clone::run_tables;
#[cfg(test)]
mod live_tests;

pub(crate) fn run_copy(
    source: &mut PgConnection,
    target: &mut PgConnection,
    state: &mut NativeState,
    plan: &CopyPlan,
    io: &IoControl,
) -> Result<CopyReport> {
    let mut target_copy_active = false;
    let outcome = (|| {
        if plan.buffer_bytes == 0 {
            return Err(failure(
                ErrorKind::BadConfig,
                "COPY forwarding budget must be positive",
            ));
        }
        let required_durability = configure_durability(target, io)?;
        command(
            target,
            "SET CONSTRAINTS ALL DEFERRED",
            "SET CONSTRAINTS",
            io,
        )?;
        let report = transfer_tables(source, target, plan, &mut target_copy_active, io)?;
        command(
            target,
            "SET CONSTRAINTS ALL IMMEDIATE",
            "SET CONSTRAINTS",
            io,
        )?;
        confirm_durability(target, required_durability, io)?;
        command(source, "COMMIT", "COMMIT", io)?;
        check_control(io)?;
        Ok(report)
    })();
    let report = match outcome {
        Ok(report) => report,
        Err(error) => {
            if target_copy_active {
                let _ = target.abort_copy_in("COPY failed", io);
            }
            rollback(source, target, io);
            return Err(error);
        }
    };
    // Once COMMIT is dispatched, a missing response cannot prove that the target rolled back.
    let result = target.query("COMMIT", io).map_err(|_| unknown_commit())?;
    check_commit_tag(result.command_tag().map_err(|_| unknown_commit())?)?;
    state.record_copy_complete(&report)?;
    Ok(report)
}

fn transfer_tables(
    source: &mut PgConnection,
    target: &mut PgConnection,
    plan: &CopyPlan,
    target_copy_active: &mut bool,
    io: &IoControl,
) -> Result<CopyReport> {
    let mut report = CopyReport {
        tables: 0,
        rows: 0,
        bytes: 0,
    };
    for table in &plan.tables {
        transfer_table(
            source,
            target,
            table,
            plan.buffer_bytes,
            &mut report,
            target_copy_active,
            io,
        )?;
    }
    Ok(report)
}

pub(crate) fn transfer_table(
    source: &mut PgConnection,
    target: &mut PgConnection,
    table: &CopyTable,
    buffer_bytes: usize,
    report: &mut CopyReport,
    target_copy_active: &mut bool,
    io: &IoControl,
) -> Result<()> {
    let source_sql = copy_sql(source, table, "TO STDOUT")?;
    let target_sql = copy_sql(target, table, "FROM STDIN")?;
    target.start_copy_in(&target_sql, io)?;
    *target_copy_active = true;
    source.start_copy_out(&source_sql, io)?;
    // libpq owns one complete row, including rows larger than the forwarding budget.
    while let Some(buffer) = source.read_copy(io)? {
        report.bytes = add(
            report.bytes,
            buffer.as_bytes().len().try_into().map_err(|_| overflow())?,
        )?;
        for chunk in buffer.as_bytes().chunks(buffer_bytes.min(64 * 1024)) {
            target.write_copy(chunk, io)?;
        }
    }
    // read_copy(None) proves the final source COPY result passed transport validation.
    *target_copy_active = false;
    report.rows = add(report.rows, target.finish_copy_in(io)?)?;
    report.tables = add(report.tables, 1)?;
    Ok(())
}

pub(crate) fn validate_copy_table(table: &CopyTable) -> Result<()> {
    if table.format != CopyFormat::Text || table.columns.is_empty() {
        return Err(failure(
            ErrorKind::BadConfig,
            "COPY requires text format and explicit columns",
        ));
    }
    Ok(())
}

fn copy_sql(connection: &mut PgConnection, table: &CopyTable, direction: &str) -> Result<String> {
    validate_copy_table(table)?;
    let name = qualified(connection, &table.name.schema, &table.name.table)?;
    let columns = table
        .columns
        .iter()
        .map(|column| connection.quote_identifier(column))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!(
        "COPY {name} ({}) {direction} WITH (FORMAT text)",
        columns.join(", ")
    ))
}

fn check_control(io: &IoControl) -> Result<()> {
    if io.cancelled.load(Ordering::Acquire) {
        return Err(failure(
            ErrorKind::Pgsql,
            "COPY cancelled before target COMMIT",
        ));
    }
    if io.is_expired() {
        return Err(failure(
            ErrorKind::Pgsql,
            "COPY deadline exceeded before target COMMIT",
        ));
    }
    Ok(())
}

fn check_commit_tag(tag: &str) -> Result<()> {
    match tag {
        "COMMIT" => Ok(()),
        "ROLLBACK" => Err(failure(
            ErrorKind::Target,
            "target rolled back instead of committing COPY",
        )),
        _ => Err(unknown_commit()),
    }
}

fn add(current: u64, amount: u64) -> Result<u64> {
    current.checked_add(amount).ok_or_else(overflow)
}

#[track_caller]
fn overflow() -> Error {
    failure(ErrorKind::Internal, "COPY report counter overflow")
}

#[track_caller]
fn unknown_commit() -> Error {
    failure(
        ErrorKind::BadState,
        "commit-outcome-unknown: target COMMIT was not confirmed; do not retry this run",
    )
}

#[track_caller]
fn failure(kind: ErrorKind, message: &str) -> Error {
    Error::new(kind, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{Conninfo, StateAction, StateRequest};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    #[test]
    fn text_copy_validation_preserves_ordered_columns() {
        let table = CopyTable {
            name: crate::contracts::QualifiedTable {
                schema: "Mixed Schema".into(),
                table: "Quoted Table".into(),
            },
            columns: vec!["second".into(), "first".into()],
            format: CopyFormat::Text,
        };
        validate_copy_table(&table).unwrap();
        assert_eq!(table.format, CopyFormat::Text);
        assert_eq!(table.columns, ["second", "first"]);
    }

    #[test]
    fn copy_validation_refuses_binary_and_empty_columns_without_rewriting_format() {
        for (format, columns) in [
            (CopyFormat::Binary, vec!["payload".to_owned()]),
            (CopyFormat::Text, Vec::new()),
            (CopyFormat::Binary, Vec::new()),
        ] {
            let table = CopyTable {
                name: crate::contracts::QualifiedTable {
                    schema: "public".into(),
                    table: "copy_validation".into(),
                },
                columns,
                format,
            };
            let error = validate_copy_table(&table).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadConfig);
            assert_eq!(
                error.message,
                "COPY requires text format and explicit columns"
            );
            assert_eq!(table.format, format);
        }
    }

    #[test]
    fn only_positive_commit_tag_proves_target_completion() {
        assert!(check_commit_tag("COMMIT").is_ok());
        for tag in ["ROLLBACK", "", "COPY 3", "COMMIT PREPARED"] {
            assert!(check_commit_tag(tag).is_err());
        }
    }

    #[test]
    #[ignore = "requires the disposable PostgreSQL harness"]
    fn live_copy_preserves_remote_apply() {
        let context =
            std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("disposable harness is required");
        let context: serde_json::Value =
            serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
        let path =
            std::path::Path::new(context["root"].as_str().unwrap()).join("state-copy-durability");
        let mut state = crate::state::open(&StateRequest {
            path,
            action: StateAction::Create,
        })
        .unwrap();
        let source_info =
            Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_SOURCE").unwrap()).unwrap();
        let target_info =
            Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_TARGET").unwrap()).unwrap();
        let io = IoControl {
            deadline: Instant::now() + Duration::from_secs(30),
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        let mut source = PgConnection::connect(&source_info, &io).unwrap();
        let mut target = PgConnection::connect(&target_info, &io).unwrap();
        target
            .query("SET synchronous_commit = remote_apply", &io)
            .unwrap();
        let plan =
            crate::catalog::plan_copy(&mut source, &mut target, &io, 8 * 1024 * 1024).unwrap();
        let report = run_copy(&mut source, &mut target, &mut state, &plan, &io).unwrap();
        assert_eq!(report.rows, 10003);
        let setting = target.query("SHOW synchronous_commit", &io).unwrap();
        assert_eq!(setting.cell(0, 0).unwrap(), Some(&b"remote_apply"[..]));
    }
}
