//! A snapshot requires dedicated physical sessions without reconnect or pooling.
//! Persisted token text cannot replace the imported transaction owned here.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::catalog::{self, CopyInventory, CopyRelation, read_source_identity};
use crate::cdc::contracts::Event;
use crate::contracts::{
    CopyFormat, CopyReport, CopyTable, Error, ErrorKind, IoControl, MAX_SNAPSHOT_TOKEN_BYTES,
    QualifiedTable, Result, SnapshotAnchor,
};
use crate::copy;
use crate::copy_plan::{self, PlanEntry, SealedCopyPlan};
use crate::identity::SourceId;
use crate::pg::{IdentifySystem, PgConnection, SlotCreateOutcome, TransactionStatus};
use crate::state::{
    CopySnapshotIntent, CreatedSlotData, NativeState, SourceSlotBinding, SourceSlotReceipt,
};

#[cfg(test)]
#[path = "snapshot_live_tests.rs"]
mod live_tests;

#[cfg(test)]
#[path = "snapshot_journal_live_tests.rs"]
mod journal_live_tests;

pub(crate) struct SourceSnapshot {
    source: PgConnection,
    exporter: PgConnection,
    receipt: SourceSlotReceipt,
    session: SessionFacts,
    copy: CopyState,
}

#[derive(Default)]
struct CopyState {
    failed: bool,
    attempted: bool,
}

impl CopyState {
    fn begin(&mut self) -> Result<()> {
        if self.failed || self.attempted {
            return Err(refusal(
                "source-snapshot-copy: source operation cannot be retried",
            ));
        }
        // An error or unwind must leave the importer unavailable for further operations.
        self.failed = true;
        Ok(())
    }

    fn complete(&mut self) {
        self.failed = false;
    }

    fn require_finish(&self) -> Result<()> {
        if self.failed {
            return Err(refusal("source-snapshot-copy: failed source cannot finish"));
        }
        Ok(())
    }
}

pub(crate) struct OwnedSourceSlot {
    exporter: PgConnection,
    receipt: SourceSlotReceipt,
}

struct SessionFacts {
    source: SourceId,
    pid: u32,
    database: Vec<u8>,
    role: u32,
    version: u32,
    recovery: bool,
    read_only: bool,
    isolation: String,
    status: TransactionStatus,
}

impl SourceSnapshot {
    pub(crate) fn create(
        mut source: PgConnection,
        mut exporter: PgConnection,
        binding: SourceSlotBinding,
        state: &mut NativeState,
        io: &IoControl,
    ) -> Result<Self> {
        (|| {
            check_control(io)?;
            let source_version = source.server_version()?;
            require_start(
                source.is_replication()?,
                exporter.is_replication()?,
                source.transaction_status()?,
                exporter.transaction_status()?,
                source_version,
                exporter.server_version()?,
            )?;
            let source_pid = source.backend_pid()?;
            let before = inspect_source(&mut source, io)?;
            require_session(&binding, source_pid, source_version, &before, None)?;
            require_exporter(&exporter.identify_system(io)?, &before)?;
            let slot = source.quote_literal(binding.slot_name())?;
            let existing = source.query(
                &format!(
                    "SELECT 1 FROM pg_catalog.pg_replication_slots s
                     WHERE s.slot_name OPERATOR(pg_catalog.=) {slot}"
                ),
                io,
            )?;
            if existing.rows() != 0 {
                return Err(refusal("source-snapshot: existing source slot refused"));
            }

            let intent = state.begin_source_slot(&binding)?;
            let outcome = exporter.create_logical_slot_export(
                binding.slot_name(),
                binding.plugin().as_str(),
                io,
            )?;
            let receipt = state.confirm_source_slot(&intent, created_data(&binding, outcome)?)?;
            let token = source.quote_literal(&receipt.snapshot_token)?;
            let import = format!("SET TRANSACTION SNAPSHOT {token}");
            // Snapshot import must precede the first query in the new transaction.
            for (sql, tag) in [
                ("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", "BEGIN"),
                (import.as_str(), "SET"),
            ] {
                let result = source.query(sql, io)?;
                if result.command_tag()? != tag
                    || source.transaction_status()? != TransactionStatus::InTransaction
                {
                    return Err(refusal(
                        "source-snapshot: snapshot import was not confirmed",
                    ));
                }
            }
            let imported = inspect_source(&mut source, io)?;
            require_session(
                &binding,
                source_pid,
                source_version,
                &imported,
                Some(&before),
            )?;
            state.checked_source_binding(&binding)?;
            check_control(io)?;
            Ok(Self {
                source,
                exporter,
                receipt,
                session: before,
                copy: CopyState::default(),
            })
        })()
        .map_err(source_failure)
    }

    pub(crate) fn receipt(&self) -> &SourceSlotReceipt {
        &self.receipt
    }

    pub(crate) fn publish_copy_plan(
        &mut self,
        native: &mut NativeState,
        plan_nonce: [u8; 16],
        format: CopyFormat,
        buffer_bytes: usize,
        io: &IoControl,
    ) -> Result<CopySnapshotIntent> {
        self.copy.begin()?;
        let result = (|| {
            self.check_live(io)?;
            let receipt = self.receipt.clone();
            let mut relations =
                CopyRelations::open(&mut self.source, receipt.intent.binding.catalog_epoch(), io)?;
            let published = {
                let mut entries = relations.by_ref().map(|relation| {
                    let (table, event) = relation?.into_parts(format)?;
                    require_format(format, table.format)?;
                    PlanEntry::checked(table, event)
                });
                copy_plan::publish_copy_plan(
                    native,
                    &receipt,
                    plan_nonce,
                    buffer_bytes,
                    &mut entries,
                    io,
                )
            };
            let complete = relations.require_complete();
            let cleanup = relations.close();
            let plan = published?;
            complete?;
            cleanup?;
            drop(relations);
            self.check_live(io)?;
            Ok(CopySnapshotIntent {
                anchor: SnapshotAnchor {
                    stream: receipt.intent.binding.stream(),
                    snapshot_token: receipt.snapshot_token,
                    consistent_point: receipt.consistent_point,
                    plan_nonce,
                },
                plan,
            })
        })();
        if result.is_ok() {
            self.copy.complete();
        }
        result
    }

    pub(crate) fn copy_into(
        &mut self,
        target: &mut PgConnection,
        owned_target_relations: &[u32; 2],
        plan: &mut SealedCopyPlan<'_>,
        io: &IoControl,
    ) -> Result<CopyReport> {
        self.copy.begin()?;
        let mut target_copy_active = false;
        let result = (|| {
            self.check_live(io)?;
            catalog::configure_session(&mut self.source, io)?;
            catalog::configure_session(target, io)?;
            catalog::require_setting(target, "session_replication_role", "origin", io)?;
            {
                let mut relations = CopyRelations::open(
                    &mut self.source,
                    self.receipt.intent.binding.catalog_epoch(),
                    io,
                )?;
                let admitted = (|| {
                    plan.scan_from(0, io, |_, format, expected| {
                        let actual = relations.next().ok_or_else(|| {
                            refusal("source-snapshot-copy: sealed plan has an extra relation")
                        })??;
                        catalog::preflight_copy_target(
                            target,
                            &actual,
                            owned_target_relations,
                            io,
                        )?;
                        let (table, event) = actual.into_parts(format)?;
                        require_format(format, table.format)?;
                        require_relation(expected, &event)?;
                        copy::validate_copy_table(&table)
                    })?;
                    match relations.next() {
                        Some(Err(error)) => return Err(error),
                        Some(Ok(_)) => {
                            return Err(refusal(
                                "source-snapshot-copy: sealed plan omits a source relation",
                            ));
                        }
                        None => relations.require_complete()?,
                    }
                    Ok(())
                })();
                let cleanup = relations.close();
                admitted?;
                cleanup?;
            }
            self.check_live(io)?;
            let buffer_bytes = plan.buffer_bytes();
            let mut report = CopyReport {
                tables: 0,
                rows: 0,
                bytes: 0,
            };
            self.copy.attempted = true;
            plan.scan_from(0, io, |_, format, event| {
                let table = transfer_metadata(event, format)?;
                copy::transfer_table(
                    &mut self.source,
                    target,
                    &table,
                    buffer_bytes,
                    &mut report,
                    &mut target_copy_active,
                    io,
                )
            })?;
            self.check_live(io)?;
            Ok(report)
        })();
        if result.is_err() && target_copy_active && matches!(target.is_replication(), Ok(false)) {
            let _ = target.abort_copy_in("COPY failed", io);
        }
        if result.is_ok() {
            self.copy.complete();
        }
        result
    }

    fn check_live(&mut self, io: &IoControl) -> Result<()> {
        check_control(io)?;
        if self.source.is_replication()?
            || !self.exporter.is_replication()?
            || self.exporter.transaction_status()? != TransactionStatus::Idle
            || self.source.transaction_status()? != TransactionStatus::InTransaction
        {
            return Err(refusal(
                "source-snapshot: source transaction is unavailable",
            ));
        }
        let current = inspect_source(&mut self.source, io)?;
        require_session(
            &self.receipt.intent.binding,
            self.source.backend_pid()?,
            self.source.server_version()?,
            &current,
            Some(&self.session),
        )
    }

    pub(crate) fn finish(mut self, io: &IoControl) -> Result<OwnedSourceSlot> {
        (|| {
            self.copy.require_finish()?;
            self.check_live(io)?;
            let result = self.source.query("COMMIT", io).map_err(|_| {
                refusal("source-snapshot-finish-unknown: source COMMIT was not confirmed")
            })?;
            let tag = result.command_tag().map_err(|_| {
                refusal("source-snapshot-finish-unknown: source COMMIT tag is unavailable")
            })?;
            let status = self.source.transaction_status().map_err(|_| {
                refusal("source-snapshot-finish-unknown: source COMMIT state is unavailable")
            })?;
            require_commit(tag, status)?;
            check_control(io)?;
            Ok(OwnedSourceSlot {
                exporter: self.exporter,
                receipt: self.receipt,
            })
        })()
        .map_err(source_failure)
    }
}

impl OwnedSourceSlot {
    pub(crate) fn receipt(&self) -> &SourceSlotReceipt {
        &self.receipt
    }
}

struct CopyRelations<'a> {
    source: &'a mut PgConnection,
    inventory: Option<CopyInventory>,
    page: std::vec::IntoIter<(QualifiedTable, u32)>,
    catalog_epoch: u64,
    io: &'a IoControl,
    finished: bool,
    failed: bool,
}

impl<'a> CopyRelations<'a> {
    fn open(source: &'a mut PgConnection, catalog_epoch: u64, io: &'a IoControl) -> Result<Self> {
        let inventory = catalog::open_copy_inventory(source, io).map_err(source_failure)?;
        Ok(Self {
            source,
            inventory: Some(inventory),
            page: Vec::new().into_iter(),
            catalog_epoch,
            io,
            finished: false,
            failed: false,
        })
    }

    fn close(&mut self) -> Result<()> {
        self.page = Vec::new().into_iter();
        if let Some(inventory) = self.inventory.take() {
            let result = catalog::close_copy_inventory(self.source, inventory, self.io)
                .map_err(source_failure);
            if result.is_err() {
                self.failed = true;
            }
            result
        } else if self.finished && !self.failed {
            Ok(())
        } else {
            Err(refusal(
                "source-snapshot-copy: inventory CLOSE was not confirmed",
            ))
        }
    }

    fn require_complete(&self) -> Result<()> {
        if !self.finished || self.failed {
            return Err(refusal(
                "source-snapshot-copy: source inventory did not finish",
            ));
        }
        Ok(())
    }

    fn next_relation(&mut self) -> Result<Option<CopyRelation>> {
        loop {
            if let Some((name, oid)) = self.page.next() {
                return catalog::inspect_copy_relation(
                    self.source,
                    &name,
                    oid,
                    self.catalog_epoch,
                    self.io,
                )
                .map(Some)
                .map_err(source_failure);
            }
            // Release the exhausted page before the next bounded FETCH allocates its replacement.
            self.page = Vec::new().into_iter();
            let inventory = self
                .inventory
                .as_mut()
                .ok_or_else(|| refusal("source-snapshot-copy: source inventory is closed"))?;
            match catalog::fetch_copy_inventory(self.source, inventory, self.io)
                .map_err(source_failure)?
            {
                Some(page) => self.page = page.into_iter(),
                None => {
                    self.close()?;
                    self.finished = true;
                    return Ok(None);
                }
            }
        }
    }
}

impl Iterator for CopyRelations<'_> {
    type Item = Result<CopyRelation>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return Some(Err(refusal(
                "source-snapshot-copy: source inventory has failed",
            )));
        }
        if self.finished {
            return None;
        }
        match self.next_relation() {
            Ok(Some(relation)) => Some(Ok(relation)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

fn require_format(expected: CopyFormat, actual: CopyFormat) -> Result<()> {
    if actual != expected {
        return Err(refusal(
            "source-snapshot-copy: requested COPY format changed",
        ));
    }
    Ok(())
}

fn require_relation(expected: &Event, actual: &Event) -> Result<()> {
    if !matches!(actual, Event::Relation { .. }) || actual != expected {
        return Err(refusal(
            "source-snapshot-copy: canonical source relation changed",
        ));
    }
    Ok(())
}

const COPY_TABLE_HEAP_BYTES: usize = 4 * 1024 * 1024;

fn transfer_metadata(event: &Event, format: CopyFormat) -> Result<CopyTable> {
    let Event::Relation {
        schema,
        name,
        columns,
        ..
    } = event
    else {
        return Err(refusal(
            "source-snapshot-copy: transfer requires a Relation",
        ));
    };
    let mut available = COPY_TABLE_HEAP_BYTES;
    let requested = columns
        .len()
        .checked_mul(size_of::<String>())
        .ok_or_else(copy_allocation)?;
    charge_copy_heap(&mut available, requested)?;
    let mut names = Vec::new();
    names
        .try_reserve_exact(columns.len())
        .map_err(|_| copy_allocation())?;
    let actual = names
        .capacity()
        .checked_mul(size_of::<String>())
        .ok_or_else(copy_allocation)?;
    charge_copy_heap(
        &mut available,
        actual.checked_sub(requested).ok_or_else(copy_allocation)?,
    )?;
    let name = QualifiedTable {
        schema: clone_copy_name(schema, &mut available)?,
        table: clone_copy_name(name, &mut available)?,
    };
    for column in columns {
        names.push(clone_copy_name(&column.name, &mut available)?);
    }
    Ok(CopyTable {
        name,
        columns: names,
        format,
    })
}

fn charge_copy_heap(available: &mut usize, bytes: usize) -> Result<()> {
    *available = available.checked_sub(bytes).ok_or_else(copy_allocation)?;
    Ok(())
}

fn clone_copy_name(value: &str, available: &mut usize) -> Result<String> {
    charge_copy_heap(available, value.len())?;
    let mut copy = String::new();
    copy.try_reserve_exact(value.len())
        .map_err(|_| copy_allocation())?;
    charge_copy_heap(
        available,
        copy.capacity()
            .checked_sub(value.len())
            .ok_or_else(copy_allocation)?,
    )?;
    copy.push_str(value);
    Ok(copy)
}

fn copy_allocation() -> Error {
    refusal("source-snapshot-copy: transfer metadata exceeds its allocation budget")
}

fn inspect_source(source: &mut PgConnection, io: &IoControl) -> Result<SessionFacts> {
    let (identity, pid) = read_source_identity(source, io)?;
    let result = source.query(
        "SELECT pg_catalog.current_database()::pg_catalog.text, r.oid::pg_catalog.text,
                pg_catalog.pg_is_in_recovery(),
                pg_catalog.current_setting('transaction_read_only'),
                pg_catalog.current_setting('transaction_isolation'),
                pg_catalog.current_setting('server_version_num')
         FROM pg_catalog.pg_roles r
         WHERE r.rolname OPERATOR(pg_catalog.=) CURRENT_USER",
        io,
    )?;
    if result.rows() != 1 {
        return Err(refusal(
            "source-snapshot: source session evidence is missing",
        ));
    }
    session_facts(
        identity,
        pid,
        [
            result.cell(0, 0)?,
            result.cell(0, 1)?,
            result.cell(0, 2)?,
            result.cell(0, 3)?,
            result.cell(0, 4)?,
            result.cell(0, 5)?,
        ],
        source.transaction_status()?,
    )
}

fn session_facts(
    source: SourceId,
    pid: u32,
    fields: [Option<&[u8]>; 6],
    status: TransactionStatus,
) -> Result<SessionFacts> {
    let [
        Some(database),
        Some(role),
        Some(recovery),
        Some(read_only),
        Some(isolation),
        Some(version),
    ] = fields
    else {
        return Err(refusal(
            "source-snapshot: source session evidence is missing",
        ));
    };
    let invalid = || refusal("source-snapshot: source session evidence is invalid");
    let number = |bytes: &[u8]| -> Result<u32> {
        let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
        text.parse::<u32>()
            .ok()
            .filter(|number| *number != 0 && number.to_string() == text)
            .ok_or_else(invalid)
    };
    let isolation = std::str::from_utf8(isolation).map_err(|_| invalid())?;
    if database.is_empty()
        || database.contains(&0)
        || !matches!(
            isolation,
            "read uncommitted" | "read committed" | "repeatable read" | "serializable"
        )
    {
        return Err(invalid());
    }
    Ok(SessionFacts {
        source,
        pid,
        database: database.to_vec(),
        role: number(role)?,
        version: number(version)?,
        recovery: match recovery {
            b"t" => true,
            b"f" => false,
            _ => return Err(invalid()),
        },
        read_only: match read_only {
            b"on" => true,
            b"off" => false,
            _ => return Err(invalid()),
        },
        isolation: isolation.to_owned(),
        status,
    })
}

fn require_start(
    source_replication: bool,
    exporter_replication: bool,
    source_status: TransactionStatus,
    exporter_status: TransactionStatus,
    source_version: u32,
    exporter_version: u32,
) -> Result<()> {
    if source_replication
        || !exporter_replication
        || source_status != TransactionStatus::Idle
        || exporter_status != TransactionStatus::Idle
    {
        return Err(refusal(
            "source-snapshot: idle SQL source and logical exporter required",
        ));
    }
    if !matches!(source_version / 10000, 14 | 17) || source_version != exporter_version {
        return Err(refusal(
            "source-snapshot: source version is unsupported or changed",
        ));
    }
    Ok(())
}

fn require_session(
    binding: &SourceSlotBinding,
    startup_pid: u32,
    startup_version: u32,
    current: &SessionFacts,
    before: Option<&SessionFacts>,
) -> Result<()> {
    if current.source != binding.stream().source {
        return Err(refusal(
            "source-snapshot: source identity differs from native binding",
        ));
    }
    if current.pid == 0
        || current.pid != startup_pid
        || current.version != startup_version
        || !matches!(current.version / 10000, 14 | 17)
        || current.recovery
        || current.role == 0
        || current.database.is_empty()
    {
        return Err(refusal("source-snapshot: source session changed"));
    }
    if let Some(before) = before {
        if current.source != before.source
            || current.pid != before.pid
            || current.database != before.database
            || current.role != before.role
            || current.version != before.version
        {
            return Err(refusal("source-snapshot: source session changed"));
        }
        if current.status != TransactionStatus::InTransaction
            || !current.read_only
            || current.isolation != "repeatable read"
        {
            return Err(refusal(
                "source-snapshot: imported source transaction changed",
            ));
        }
    } else if current.status != TransactionStatus::Idle {
        return Err(refusal("source-snapshot: source session is not idle"));
    }
    Ok(())
}

fn require_exporter(identity: &IdentifySystem, source: &SessionFacts) -> Result<()> {
    if identity.system_identifier != source.source.system_identifier
        || identity.database_name != source.database
    {
        return Err(refusal(
            "source-snapshot: exporter identity differs from source",
        ));
    }
    Ok(())
}

fn created_data(
    binding: &SourceSlotBinding,
    outcome: SlotCreateOutcome,
) -> Result<CreatedSlotData> {
    let SlotCreateOutcome::Created(created) = outcome else {
        return Err(refusal(
            "source-slot-create-unknown: slot may retain WAL; retain native intent for manual review",
        ));
    };
    if created.slot_name != binding.slot_name()
        || created.plugin != binding.plugin().as_str()
        || created.snapshot_token.is_empty()
        || created.snapshot_token.len() > MAX_SNAPSHOT_TOKEN_BYTES
        || created.snapshot_token.contains('\0')
    {
        return Err(refusal(
            "source-slot-create-unknown: invalid creation receipt; retain native intent for manual review",
        ));
    }
    Ok(CreatedSlotData {
        consistent_point: created.consistent_point,
        snapshot_name: created.snapshot_token,
    })
}

fn require_commit(tag: &str, status: TransactionStatus) -> Result<()> {
    if tag != "COMMIT" || status != TransactionStatus::Idle {
        return Err(refusal(
            "source-snapshot-finish-unknown: positive idle source COMMIT was not confirmed",
        ));
    }
    Ok(())
}

fn check_control(io: &IoControl) -> Result<()> {
    if io.cancelled.load(Ordering::Acquire) || Instant::now() >= io.deadline {
        return Err(refusal(
            "source-snapshot: operation cancelled or deadline exceeded",
        ));
    }
    Ok(())
}

fn source_failure(mut error: Error) -> Error {
    if error.kind == ErrorKind::Pgsql {
        error.kind = ErrorKind::Source;
    }
    error
}

fn refusal(message: &str) -> Error {
    Error {
        kind: ErrorKind::Source,
        message: message.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::contracts::Plugin;
    use crate::contracts::{ErrorKind, MAX_SNAPSHOT_TOKEN_BYTES};
    use crate::identity::{RunId, SourceEndLsn, SourceId, StreamId};
    use crate::pg::{CreatedLogicalSlot, IdentifySystem, SlotCreateOutcome, TransactionStatus};
    use crate::state::SourceSlotBinding;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    fn binding() -> SourceSlotBinding {
        SourceSlotBinding::new(
            StreamId {
                run: RunId([1; 16]),
                source: SourceId {
                    system_identifier: u64::MAX,
                    database_oid: 55,
                },
                generation: 9,
            },
            "pgcopydb".into(),
            Plugin::TestDecoding,
            7,
        )
        .unwrap()
    }

    fn session(imported: bool) -> SessionFacts {
        SessionFacts {
            source: binding().stream().source,
            pid: 101,
            database: b"source".to_vec(),
            role: 123,
            version: 140024,
            recovery: false,
            read_only: imported,
            isolation: if imported {
                "repeatable read".into()
            } else {
                "read committed".into()
            },
            status: if imported {
                TransactionStatus::InTransaction
            } else {
                TransactionStatus::Idle
            },
        }
    }

    fn wire(token: String) -> CreatedLogicalSlot {
        CreatedLogicalSlot {
            slot_name: "pgcopydb".into(),
            plugin: "test_decoding".into(),
            consistent_point: SourceEndLsn(u64::MAX),
            snapshot_token: token,
        }
    }

    #[test]
    fn only_idle_sql_and_logical_peers_on_one_supported_version_can_begin() {
        use TransactionStatus::{Active, Idle, InError, InTransaction, Unknown};
        for version in [140024, 170011] {
            require_start(false, true, Idle, Idle, version, version).unwrap();
        }
        for (sql, logical) in [(true, true), (false, false), (true, false)] {
            assert!(require_start(sql, logical, Idle, Idle, 140024, 140024).is_err());
        }
        for status in [Active, InError, InTransaction, Unknown] {
            assert!(require_start(false, true, status, Idle, 140024, 140024).is_err());
            assert!(require_start(false, true, Idle, status, 140024, 140024).is_err());
        }
        for version in [0, 90600, 110000, 120000, 130000, 150000, 160000, 180000] {
            assert!(require_start(false, true, Idle, Idle, version, version).is_err());
        }
        assert!(require_start(false, true, Idle, Idle, 140024, 170011).is_err());
    }

    #[test]
    fn actual_source_and_backend_must_match_binding_and_startup_metadata() {
        let binding = binding();
        let before = session(false);
        require_session(&binding, 101, 140024, &before, None).unwrap();
        assert_eq!(binding.stream().generation, 9);
        assert_eq!(binding.catalog_epoch(), 7);
        for field in 0..5 {
            let mut changed = session(false);
            match field {
                0 => changed.source.system_identifier = 1,
                1 => changed.source.database_oid = 56,
                2 => changed.pid = 102,
                3 => changed.version = 170011,
                _ => changed.recovery = true,
            }
            assert!(require_session(&binding, 101, 140024, &changed, None).is_err());
        }
    }

    #[test]
    fn missing_or_malformed_session_properties_cannot_supply_evidence() {
        let fields: [Option<&[u8]>; 6] = [
            Some(b"source"),
            Some(b"123"),
            Some(b"f"),
            Some(b"on"),
            Some(b"repeatable read"),
            Some(b"140024"),
        ];
        let actual = session_facts(
            binding().stream().source,
            101,
            fields,
            TransactionStatus::InTransaction,
        )
        .unwrap();
        assert_eq!(actual.database, b"source");
        assert_eq!(actual.role, 123);
        assert!(actual.read_only);
        assert!(!actual.recovery);
        for index in 0..fields.len() {
            let mut missing = fields;
            missing[index] = None;
            assert!(session_facts(actual.source, actual.pid, missing, actual.status).is_err());
        }
        for (index, invalid) in [
            (0, b"".as_slice()),
            (1, b"0".as_slice()),
            (1, b"+123".as_slice()),
            (2, b"false".as_slice()),
            (3, b"true".as_slice()),
            (4, b"".as_slice()),
            (5, b"+140024".as_slice()),
        ] {
            let mut changed = fields;
            changed[index] = Some(invalid);
            assert!(session_facts(actual.source, actual.pid, changed, actual.status).is_err());
        }
    }

    #[test]
    fn imported_transaction_cannot_change_session_or_snapshot_characteristics() {
        let binding = binding();
        let before = session(false);
        let after = session(true);
        require_session(&binding, 101, 140024, &after, Some(&before)).unwrap();
        for field in 0..7 {
            let mut changed = session(true);
            match field {
                0 => changed.role += 1,
                1 => changed.database = b"other".to_vec(),
                2 => changed.read_only = false,
                3 => changed.isolation = "read committed".into(),
                4 => changed.status = TransactionStatus::Idle,
                5 => changed.pid += 1,
                _ => changed.recovery = true,
            }
            assert!(require_session(&binding, 101, 140024, &changed, Some(&before)).is_err());
        }
    }

    #[test]
    fn exporter_identity_is_not_a_database_oid_or_a_slot_consistent_point() {
        let before = session(false);
        let mut exporter = IdentifySystem {
            system_identifier: u64::MAX,
            timeline: 1,
            wal_position: 999,
            database_name: b"source".to_vec(),
        };
        require_exporter(&exporter, &before).unwrap();
        exporter.system_identifier = 1;
        assert!(require_exporter(&exporter, &before).is_err());
        exporter.system_identifier = u64::MAX;
        exporter.database_name = b"55".to_vec();
        assert!(require_exporter(&exporter, &before).is_err());
    }

    #[test]
    fn state_data_only_comes_from_matching_wire_fields_and_bounded_actual_token() {
        let binding = binding();
        for token in [
            "token".to_owned(),
            "x".repeat(MAX_SNAPSHOT_TOKEN_BYTES),
            "\u{e9}".repeat(MAX_SNAPSHOT_TOKEN_BYTES / 2),
        ] {
            let data =
                created_data(&binding, SlotCreateOutcome::Created(wire(token.clone()))).unwrap();
            assert_eq!(data.consistent_point, SourceEndLsn(u64::MAX));
            assert_eq!(data.snapshot_name, token);
        }
        for token in [
            "".to_owned(),
            "x\0y".into(),
            "x".repeat(MAX_SNAPSHOT_TOKEN_BYTES + 1),
        ] {
            assert!(created_data(&binding, SlotCreateOutcome::Created(wire(token))).is_err());
        }
        for wrong_slot in [true, false] {
            let mut changed = wire("token".into());
            if wrong_slot {
                changed.slot_name = "other".into();
            } else {
                changed.plugin = "pgoutput".into();
            }
            assert!(created_data(&binding, SlotCreateOutcome::Created(changed)).is_err());
        }
    }

    #[test]
    fn unknown_create_never_returns_state_confirmation_data() {
        let failure = created_data(
            &binding(),
            SlotCreateOutcome::Unknown(Error {
                kind: ErrorKind::Pgsql,
                message: "transport failure".into(),
            }),
        )
        .err()
        .expect("unknown creation cannot produce data");
        assert_eq!(failure.kind, ErrorKind::Source);
        assert!(failure.message.starts_with("source-slot-create-unknown:"));
    }

    #[test]
    fn only_positive_idle_commit_releases_the_exporter_owner() {
        require_commit("COMMIT", TransactionStatus::Idle).unwrap();
        for tag in ["ROLLBACK", "", "COMMIT PREPARED", "COPY 1"] {
            assert!(require_commit(tag, TransactionStatus::Idle).is_err());
        }
        for status in [
            TransactionStatus::Active,
            TransactionStatus::InTransaction,
            TransactionStatus::InError,
            TransactionStatus::Unknown,
        ] {
            assert!(require_commit("COMMIT", status).is_err());
        }
    }

    #[test]
    fn cancellation_or_elapsed_absolute_deadline_cannot_publish_a_capability() {
        let mut io = IoControl {
            deadline: Instant::now() + Duration::from_secs(30),
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        check_control(&io).unwrap();
        io.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        assert!(check_control(&io).is_err());
        io.cancelled
            .store(false, std::sync::atomic::Ordering::Release);
        io.deadline = Instant::now() - Duration::from_secs(1);
        assert!(check_control(&io).is_err());
    }

    #[test]
    fn incomplete_copy_operations_latch_failure_and_block_finish() {
        let mut state = CopyState::default();
        state.require_finish().unwrap();
        state.begin().unwrap();
        assert!(state.failed);
        assert!(!state.attempted);
        assert!(state.begin().is_err());
        assert!(state.require_finish().is_err());
    }

    #[test]
    fn completed_transfer_forbids_recopy_but_retains_source_finish() {
        let mut state = CopyState::default();
        state.begin().unwrap();
        state.attempted = true;
        state.complete();
        assert!(!state.failed);
        assert!(state.attempted);
        assert!(state.begin().is_err());
        state.require_finish().unwrap();
        assert!(!state.failed);
    }

    #[test]
    fn successful_plan_publication_does_not_claim_a_transfer_attempt() {
        let mut state = CopyState::default();
        state.begin().unwrap();
        state.complete();
        assert!(!state.attempted);
        state.require_finish().unwrap();
        state.begin().unwrap();
        assert!(state.failed);
    }

    fn relation_event() -> Event {
        Event::Relation {
            key: crate::cdc::contracts::RelationKey {
                oid: 83,
                generation: 7,
            },
            schema: "SourceSchema".into(),
            name: "Table\"Name".into(),
            columns: vec![
                crate::cdc::contracts::Column {
                    name: "z".into(),
                    type_oid: 23,
                    type_mod: -1,
                    key: true,
                },
                crate::cdc::contracts::Column {
                    name: "A".into(),
                    type_oid: 25,
                    type_mod: -1,
                    key: false,
                },
            ],
            replica_identity: b'd',
        }
    }

    #[test]
    fn transfer_metadata_preserves_names_order_and_requested_format() {
        let relation = relation_event();
        for format in [CopyFormat::Text, CopyFormat::Binary] {
            let table = transfer_metadata(&relation, format).unwrap();
            assert_eq!(table.name.schema, "SourceSchema");
            assert_eq!(table.name.table, "Table\"Name");
            assert_eq!(table.columns, ["z", "A"]);
            assert_eq!(table.format, format);
            require_format(format, table.format).unwrap();
        }
        assert!(require_format(CopyFormat::Text, CopyFormat::Binary).is_err());
        assert!(require_format(CopyFormat::Binary, CopyFormat::Text).is_err());
        assert!(
            transfer_metadata(
                &Event::Truncate {
                    relations: vec![],
                    cascade: false,
                    restart_identity: false
                },
                CopyFormat::Text
            )
            .is_err()
        );
    }

    #[test]
    fn transfer_metadata_refuses_oversized_callback_clones() {
        let mut event = relation_event();
        let Event::Relation { columns, .. } = &mut event else {
            unreachable!()
        };
        columns[1].name = "x".repeat(COPY_TABLE_HEAP_BYTES + 1);
        assert!(transfer_metadata(&event, CopyFormat::Text).is_err());
    }

    #[test]
    fn source_admission_requires_exact_canonical_metadata() {
        let expected = relation_event();
        require_relation(&expected, &expected).unwrap();
        for field in 0..5 {
            let mut actual = relation_event();
            let Event::Relation {
                key,
                schema,
                name,
                columns,
                ..
            } = &mut actual
            else {
                unreachable!()
            };
            match field {
                0 => key.generation = 9,
                1 => key.oid += 1,
                2 => schema.make_ascii_lowercase(),
                3 => name.push('x'),
                _ => columns.swap(0, 1),
            }
            assert!(
                require_relation(&expected, &actual).is_err(),
                "field {field}"
            );
        }
    }
}
