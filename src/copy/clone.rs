use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use crate::apply::{CloneAuthority, CopyCommitOutcome, CopyPermit, CopyTxn};
use crate::catalog::{command, configure_session, qualified};
use crate::contracts::{
    CloneOptions, ClonePlan, CopyReport, Error, ErrorKind, IoControl, OwnedCopyTable, Result,
    SnapshotTicket,
};
use crate::pg::PgConnection;
use crate::snapshot::{self, CloneSnapshot};
use crate::state::NativeState;

const MAX_COPY_TABLES: usize = 4096;
const MAX_COPY_ROW_BYTES: usize = 16 * 1024 * 1024;

struct TableReceipt {
    index: usize,
    report: CopyReport,
    outcome: CopyCommitOutcome,
}

pub(crate) fn run_tables(
    options: &CloneOptions,
    plan: &ClonePlan,
    snapshot: &mut CloneSnapshot,
    state: &mut NativeState,
    authority: &mut CloneAuthority,
    io: &IoControl,
) -> Result<CopyReport> {
    super::check_control(io)?;
    if options.table_jobs == 0 || options.table_jobs > 128 {
        return Err(refusal(
            "clone-copy: table jobs must be between one and 128",
        ));
    }
    snapshot.connection()?;
    let ticket = snapshot.ticket().clone();
    let saved = state
        .checked_clone()?
        .ok_or_else(|| refusal("clone-copy: recorded snapshot is absent"))?;
    if saved.plan != *plan
        || saved.ticket != ticket
        || saved.phase != crate::state::ClonePhase::Copying
    {
        return Err(refusal(
            "clone-copy: recorded snapshot and copy phase must match the live exporter",
        ));
    }
    let owned = saved.owned_tables;
    validate_workset(plan, &owned, &ticket)?;
    let tasks = owned
        .iter()
        .enumerate()
        .map(|(index, table)| Ok((index, authority.permit(table)?)))
        .collect::<Result<Vec<_>>>()?;
    let queue = Mutex::new(tasks.into_iter());
    let failure = Mutex::new(None);
    let workers = options.table_jobs.min(owned.len());
    let receipts = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let queue = &queue;
            let failure = &failure;
            let ticket = &ticket;
            let owned = &owned;
            handles.push(scope.spawn(move || {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_worker(options, ticket, owned, queue, io)
                })) {
                    Ok(Ok(receipts)) => receipts,
                    Ok(Err(error)) => {
                        latch_worker_failure(failure, error, io);
                        Vec::new()
                    }
                    Err(_) => {
                        latch_worker_failure(
                            failure,
                            refusal("clone-copy: worker terminated without a result"),
                            io,
                        );
                        Vec::new()
                    }
                }
            }));
        }
        let mut receipts = Vec::with_capacity(owned.len());
        for handle in handles {
            match handle.join() {
                Ok(worker) => receipts.extend(worker),
                Err(_) => latch_worker_failure(
                    &failure,
                    refusal("clone-copy: worker terminated without a result"),
                    io,
                ),
            }
        }
        receipts
    });
    if let Some(error) = failure
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        return Err(error);
    }
    super::check_control(io)?;
    snapshot.connection()?;
    if receipts.len() != owned.len() {
        return Err(refusal("clone-copy: a table completion result is missing"));
    }
    let mut seen = BTreeSet::new();
    let mut report = CopyReport {
        tables: 0,
        rows: 0,
        bytes: 0,
    };
    // Every worker session has closed before uncertain commits are read back under the fence.
    for receipt in receipts {
        if !seen.insert(receipt.index) {
            return Err(refusal("clone-copy: duplicate table completion result"));
        }
        let committed = match receipt.outcome {
            CopyCommitOutcome::Durable => receipt.report,
            CopyCommitOutcome::Unknown => {
                let durable = authority.resolve_unknown(&owned[receipt.index], io)?
                    .ok_or_else(|| refusal("commit-outcome-unknown: COPY marker is absent; restart the copy generation"))?;
                if durable != receipt.report {
                    return Err(refusal(
                        "clone-copy: durable marker counts disagree with completed COPY",
                    ));
                }
                durable
            }
        };
        report.tables = super::add(report.tables, committed.tables)?;
        report.rows = super::add(report.rows, committed.rows)?;
        report.bytes = super::add(report.bytes, committed.bytes)?;
    }
    Ok(report)
}

fn latch_worker_failure(failure: &Mutex<Option<Error>>, error: Error, io: &IoControl) {
    let mut first = failure
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if first.is_none() {
        *first = Some(error);
    }
    io.cancelled.store(true, Ordering::Release);
}

fn run_worker(
    options: &CloneOptions,
    ticket: &SnapshotTicket,
    owned: &[OwnedCopyTable],
    queue: &Mutex<std::vec::IntoIter<(usize, CopyPermit)>>,
    io: &IoControl,
) -> Result<Vec<TableReceipt>> {
    let mut source = PgConnection::connect(&options.source, io)?;
    snapshot::import(&mut source, ticket, io)?;
    let mut receipts = Vec::new();
    loop {
        super::check_control(io)?;
        let task = queue
            .lock()
            .map_err(|_| refusal("clone-copy: worker queue is unavailable"))?
            .next();
        let Some((index, permit)) = task else { break };
        let mut target = PgConnection::connect(&options.target, io)?;
        configure_session(&mut target, io)?;
        let mut transaction = CopyTxn::begin(target, permit, io)?;
        let report = transfer_clone_table(
            &mut source,
            transaction.connection_mut()?,
            &owned[index],
            io,
        )?;
        let outcome = transaction.mark_and_commit(report.rows, report.bytes, io)?;
        receipts.push(TableReceipt {
            index,
            report,
            outcome,
        });
    }
    command(&mut source, "COMMIT", "COMMIT", io)?;
    Ok(receipts)
}

fn transfer_clone_table(
    source: &mut PgConnection,
    target: &mut PgConnection,
    owned: &OwnedCopyTable,
    io: &IoControl,
) -> Result<CopyReport> {
    let table = &owned.table;
    let source_name = qualified(source, &table.name.schema, &table.name.table)?;
    let target_name = qualified(target, &table.name.schema, &table.name.table)?;
    let source_columns = table
        .writable_columns
        .iter()
        .map(|name| source.quote_identifier(name))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let target_columns = table
        .writable_columns
        .iter()
        .map(|name| target.quote_identifier(name))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    command(
        source,
        &format!("LOCK TABLE ONLY {source_name} IN ACCESS SHARE MODE"),
        "LOCK TABLE",
        io,
    )?;
    let resolved_name = source.quote_literal(&source_name)?;
    let resolved = source.query(
        &format!("SELECT {resolved_name}::pg_catalog.regclass::pg_catalog.oid"),
        io,
    )?;
    let expected_oid = table.source_oid.to_string();
    if resolved.rows() != 1 || resolved.cell(0, 0)? != Some(expected_oid.as_bytes()) {
        return Err(refusal("clone-copy: source table identity changed"));
    }
    target.start_copy_in(
        &format!(
            "COPY {target_name} ({target_columns}) FROM STDIN WITH (FORMAT text, FREEZE true)"
        ),
        io,
    )?;
    source.start_copy_out(
        &format!(
            "COPY (SELECT {source_columns} FROM ONLY {source_name}) TO STDOUT WITH (FORMAT text)"
        ),
        io,
    )?;
    let mut report = CopyReport {
        tables: 1,
        rows: 0,
        bytes: 0,
    };
    while let Some(buffer) = source.read_copy_bounded(MAX_COPY_ROW_BYTES, io)? {
        report.rows = super::add(report.rows, 1)?;
        report.bytes = super::add(
            report.bytes,
            buffer
                .as_bytes()
                .len()
                .try_into()
                .map_err(|_| super::overflow())?,
        )?;
        for chunk in buffer.as_bytes().chunks(64 * 1024) {
            target.write_copy(chunk, io)?;
        }
    }
    let actual_rows = target.finish_copy_in(io)?;
    if actual_rows != report.rows {
        return Err(refusal(
            "clone-copy: source and target COPY row counts disagree",
        ));
    }
    if table.all_columns != table.writable_columns {
        crate::compare::verify_generated_rows(source, target, table, io)?;
    }
    Ok(report)
}

fn validate_workset(
    plan: &ClonePlan,
    owned: &[OwnedCopyTable],
    ticket: &SnapshotTicket,
) -> Result<()> {
    if plan.source != ticket.source
        || plan.generation != ticket.generation
        || plan.generation.0 == 0
        || plan.tables.len() != owned.len()
        || owned.len() > MAX_COPY_TABLES
    {
        return Err(refusal(
            "clone-copy: snapshot, generation, and owned inventory must match",
        ));
    }
    let mut source_tables = BTreeMap::new();
    let mut source_names = BTreeSet::new();
    for table in &plan.tables {
        if table.source_oid == 0
            || table.writable_columns.is_empty()
            || source_tables.insert(table.source_oid, table).is_some()
            || !source_names.insert((&table.name.schema, &table.name.table))
        {
            return Err(refusal(
                "clone-copy: source physical table inventory is invalid",
            ));
        }
    }
    let mut target_oids = BTreeSet::new();
    let mut seen_sources = BTreeSet::new();
    for table in owned {
        if table.generation != plan.generation
            || table.target_oid == 0
            || table.target_namespace_oid == 0
            || table.target_owner_oid == 0
            || source_tables.get(&table.table.source_oid).copied() != Some(&table.table)
            || !target_oids.insert(table.target_oid)
            || !seen_sources.insert(table.table.source_oid)
        {
            return Err(refusal(
                "clone-copy: exact table ownership proof does not match the copy plan",
            ));
        }
    }
    Ok(())
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        CloneSource, CloneTable, CloneTarget, CopyGeneration, QualifiedTable, RestorePolicy,
    };
    use crate::identity::SourceId;

    fn workset() -> (ClonePlan, Vec<OwnedCopyTable>, SnapshotTicket) {
        let source = CloneSource {
            identity: SourceId {
                system_identifier: 7,
                database_oid: 9,
            },
            timeline: 1,
            major: 16,
        };
        let generation = CopyGeneration(1);
        let table = CloneTable {
            source_oid: 20_000,
            name: QualifiedTable {
                schema: "public".into(),
                table: "payload".into(),
            },
            writable_columns: vec!["key".into(), "value".into()],
            all_columns: vec!["key".into(), "value".into(), "generated".into()],
            identity_modes: vec![0, 0, 0],
            partition_identity_modes: None,
        };
        let owned = OwnedCopyTable {
            table: table.clone(),
            target_oid: 30_000,
            target_namespace_oid: 2200,
            target_owner_oid: 10,
            generation,
        };
        (
            ClonePlan {
                source: source.clone(),
                target: CloneTarget {
                    identity: SourceId {
                        system_identifier: 8,
                        database_oid: 9,
                    },
                    major: 18,
                },
                generation,
                tables: vec![table],
                sequences: vec![],
                policy: RestorePolicy::default(),
            },
            vec![owned],
            SnapshotTicket {
                token: "snapshot".into(),
                source,
                generation,
            },
        )
    }

    #[test]
    fn copied_workset_requires_exact_source_generation_columns_and_ownership() {
        let (plan, owned, ticket) = workset();
        validate_workset(&plan, &owned, &ticket).unwrap();
        for field in 0..11 {
            let (mut plan, mut owned, mut ticket) = workset();
            match field {
                0 => ticket.source.timeline += 1,
                1 => ticket.generation.0 += 1,
                2 => owned[0].generation.0 += 1,
                3 => owned[0].table.source_oid += 1,
                4 => owned[0].table.writable_columns.swap(0, 1),
                5 => owned[0].target_oid = 0,
                6 => owned[0].target_namespace_oid = 0,
                7 => owned[0].target_owner_oid = 0,
                8 => {
                    owned.clear();
                }
                9 => {
                    plan.tables.push(plan.tables[0].clone());
                    owned.push(owned[0].clone());
                }
                _ => {
                    plan.tables[0].writable_columns.clear();
                    owned[0].table.writable_columns.clear();
                }
            }
            assert!(
                validate_workset(&plan, &owned, &ticket).is_err(),
                "field {field}"
            );
        }
    }

    #[test]
    fn an_established_empty_catalog_is_valid_but_an_omitted_table_is_not() {
        let (mut plan, mut owned, ticket) = workset();
        owned.clear();
        assert!(validate_workset(&plan, &owned, &ticket).is_err());
        plan.tables.clear();
        validate_workset(&plan, &owned, &ticket).unwrap();
    }

    #[test]
    fn worker_failure_preserves_first_error_and_cancels_siblings() {
        use std::sync::{Arc, atomic::AtomicBool};
        let failure = Mutex::new(None);
        let io = IoControl::unbounded(Arc::new(AtomicBool::new(false)));
        latch_worker_failure(&failure, refusal("original failure"), &io);
        latch_worker_failure(&failure, refusal("cancelled sibling"), &io);
        assert!(io.cancelled.load(Ordering::Acquire));
        assert_eq!(
            failure.into_inner().unwrap().unwrap().message,
            "original failure"
        );
    }
}
