//! A snapshot requires dedicated physical sessions without reconnect or pooling.
//! Persisted token text cannot replace the imported transaction owned here.

use std::sync::atomic::Ordering;

use crate::catalog::{self, read_source_identity};
use crate::contracts::{Error, ErrorKind, IoControl, MAX_SNAPSHOT_TOKEN_BYTES, Result};
use crate::identity::SourceId;
use crate::pg::{IdentifySystem, PgConnection, SlotCreateOutcome, TransactionStatus};
use crate::state::{CreatedSlotData, NativeState, SourceSlotBinding, SourceSlotReceipt};

fn validate_clone_ticket(ticket: &crate::contracts::SnapshotTicket) -> Result<()> {
    if ticket.token.is_empty()
        || ticket.token.len() > MAX_SNAPSHOT_TOKEN_BYTES
        || ticket.token.as_bytes().contains(&0)
        || ticket.source.identity.system_identifier == 0
        || ticket.source.identity.database_oid == 0
        || ticket.source.timeline == 0
        || !matches!(ticket.source.major, 16 | 18)
        || ticket.generation.0 == 0
    {
        return Err(refusal("clone-snapshot: invalid snapshot ticket"));
    }
    Ok(())
}

pub(crate) struct CloneSnapshot {
    source: PgConnection,
    ticket: crate::contracts::SnapshotTicket,
    pid: u32,
}

impl CloneSnapshot {
    pub(crate) fn export(
        mut source: PgConnection,
        generation: crate::contracts::CopyGeneration,
        io: &IoControl,
    ) -> Result<Self> {
        check_control(io)?;
        if generation.0 == 0 {
            return Err(refusal("clone-snapshot: copy generation must be positive"));
        }
        require_clone_idle(&source)?;
        catalog::configure_session(&mut source, io)?;
        let before = catalog::read_clone_source(&mut source, io)?;
        disable_clone_timeouts(&mut source, before.major, io)?;
        let pid = source.backend_pid()?;
        catalog::command(
            &mut source,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            io,
        )?;
        let actual = catalog::read_clone_source(&mut source, io)?;
        if actual != before {
            return Err(refusal("clone-snapshot: exporter source identity changed"));
        }
        let result = source.query("SELECT pg_catalog.pg_export_snapshot()", io)?;
        if result.rows() != 1 {
            return Err(refusal(
                "clone-snapshot: exported snapshot token is missing",
            ));
        }
        let token = result
            .cell(0, 0)?
            .and_then(|value| std::str::from_utf8(value).ok())
            .ok_or_else(|| refusal("clone-snapshot: exported snapshot token is invalid"))?
            .to_owned();
        let ticket = crate::contracts::SnapshotTicket {
            token,
            source: actual,
            generation,
        };
        validate_clone_ticket(&ticket)?;
        require_clone_transaction(&mut source, io)?;
        Ok(Self {
            source,
            ticket,
            pid,
        })
    }

    pub(crate) fn ticket(&self) -> &crate::contracts::SnapshotTicket {
        &self.ticket
    }

    pub(crate) fn connection(&mut self) -> Result<&mut PgConnection> {
        if self.source.is_replication()?
            || self.source.transaction_status()? != TransactionStatus::InTransaction
            || self.source.backend_pid()? != self.pid
        {
            return Err(refusal(
                "clone-snapshot: exporter transaction is unavailable",
            ));
        }
        Ok(&mut self.source)
    }

    pub(crate) fn finish(mut self, io: &IoControl) -> Result<()> {
        check_control(io)?;
        self.connection()?;
        let result = self.source.query("COMMIT", io).map_err(|_| {
            refusal("clone-snapshot-finish-unknown: exporter COMMIT was not confirmed")
        })?;
        require_commit(result.command_tag()?, self.source.transaction_status()?)
    }
}

pub(crate) fn import(
    source: &mut PgConnection,
    ticket: &crate::contracts::SnapshotTicket,
    io: &IoControl,
) -> Result<()> {
    check_control(io)?;
    validate_clone_ticket(ticket)?;
    require_clone_idle(source)?;
    catalog::configure_session(source, io)?;
    if catalog::read_clone_source(source, io)? != ticket.source {
        return Err(refusal(
            "clone-snapshot: importer source identity does not match",
        ));
    }
    disable_clone_timeouts(source, ticket.source.major, io)?;
    let token = source.quote_literal(&ticket.token)?;
    catalog::command(
        source,
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "BEGIN",
        io,
    )?;
    // Import precedes every snapshot-dependent query in this transaction.
    catalog::command(
        source,
        &format!("SET TRANSACTION SNAPSHOT {token}"),
        "SET",
        io,
    )?;
    if catalog::read_clone_source(source, io)? != ticket.source {
        return Err(refusal("clone-snapshot: imported source identity changed"));
    }
    require_clone_transaction(source, io)?;
    check_control(io)
}

fn require_clone_idle(source: &PgConnection) -> Result<()> {
    if source.is_replication()? || source.transaction_status()? != TransactionStatus::Idle {
        return Err(refusal(
            "clone-snapshot: dedicated idle SQL connection required",
        ));
    }
    Ok(())
}

fn disable_clone_timeouts(source: &mut PgConnection, major: u32, io: &IoControl) -> Result<()> {
    for name in ["statement_timeout", "idle_in_transaction_session_timeout"] {
        catalog::command(source, &format!("SET {name} TO 0"), "SET", io)?;
        catalog::require_setting(source, name, "0", io)?;
    }
    if major >= 17 {
        catalog::command(source, "SET transaction_timeout TO 0", "SET", io)?;
        catalog::require_setting(source, "transaction_timeout", "0", io)?;
    }
    Ok(())
}

fn require_clone_transaction(source: &mut PgConnection, io: &IoControl) -> Result<()> {
    if source.transaction_status()? != TransactionStatus::InTransaction {
        return Err(refusal(
            "clone-snapshot: imported transaction is unavailable",
        ));
    }
    catalog::require_setting(source, "transaction_read_only", "on", io)?;
    catalog::require_setting(source, "transaction_isolation", "repeatable read", io)
}

#[cfg(test)]
mod clone_tests {
    use super::*;
    use crate::contracts::{CloneSource, CopyGeneration, SnapshotTicket};

    #[test]
    #[ignore = "requires the disposable M2 PostgreSQL harness"]
    fn live_clone_snapshot_is_shared_and_exporter_lifetime_is_required() {
        use crate::contracts::Conninfo;
        use std::sync::{Arc, atomic::AtomicBool};
        let context = std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned harness required");
        let context: serde_json::Value =
            serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
        assert_eq!(
            std::fs::read_to_string(
                std::path::Path::new(context["root"].as_str().unwrap()).join(".e1-owner")
            )
            .unwrap(),
            context["token"].as_str().unwrap()
        );
        let info = Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN").expect("M2 source required"),
        )
        .unwrap();
        let io = IoControl::unbounded(Arc::new(AtomicBool::new(false)));
        let mut writer = PgConnection::connect(&info, &io).unwrap();
        let counts_sql = "SELECT (SELECT count(*) FROM pg_catalog.pg_replication_slots), (SELECT count(*) FROM pg_catalog.pg_publication)";
        let before = writer.query(counts_sql, &io).unwrap();
        let table = format!("m2_snapshot_{}", std::process::id());
        let table = writer.quote_identifier(&table).unwrap();
        writer
            .query(&format!("CREATE TABLE public.{table} (value integer)"), &io)
            .unwrap();
        writer
            .query(&format!("INSERT INTO public.{table} VALUES (1)"), &io)
            .unwrap();
        let exporter = CloneSnapshot::export(
            PgConnection::connect(&info, &io).unwrap(),
            CopyGeneration(1),
            &io,
        )
        .unwrap();
        let ticket = exporter.ticket().clone();
        writer
            .query(&format!("INSERT INTO public.{table} VALUES (2)"), &io)
            .unwrap();
        let mut importer = PgConnection::connect(&info, &io).unwrap();
        import(&mut importer, &ticket, &io).unwrap();
        let rows = importer
            .query(
                &format!("SELECT value FROM ONLY public.{table} ORDER BY value"),
                &io,
            )
            .unwrap();
        assert_eq!(rows.rows(), 1);
        assert_eq!(rows.cell(0, 0).unwrap(), Some(b"1".as_slice()));
        exporter.finish(&io).unwrap();
        let rows = importer
            .query(&format!("SELECT count(*) FROM ONLY public.{table}"), &io)
            .unwrap();
        assert_eq!(rows.cell(0, 0).unwrap(), Some(b"1".as_slice()));
        command_importer_commit(&mut importer, &io);
        let mut late = PgConnection::connect(&info, &io).unwrap();
        let error = import(&mut late, &ticket, &io).unwrap_err();
        assert!(
            error.message.contains(if ticket.source.major == 16 {
                "22023"
            } else {
                "42704"
            }),
            "expired exported snapshot was not observed"
        );
        assert_eq!(
            late.transaction_status().unwrap(),
            TransactionStatus::InError
        );
        drop(late);
        let after = writer.query(counts_sql, &io).unwrap();
        assert!(before.cell(0, 0).unwrap() == after.cell(0, 0).unwrap());
        assert!(before.cell(0, 1).unwrap() == after.cell(0, 1).unwrap());
    }

    fn command_importer_commit(connection: &mut PgConnection, io: &IoControl) {
        catalog::command(connection, "COMMIT", "COMMIT", io).unwrap();
        assert_eq!(
            connection.transaction_status().unwrap(),
            TransactionStatus::Idle
        );
    }

    fn ticket() -> SnapshotTicket {
        SnapshotTicket {
            token: "00000003-0000001B-1".into(),
            source: CloneSource {
                identity: SourceId {
                    system_identifier: 7,
                    database_oid: 23,
                },
                timeline: 1,
                major: 16,
            },
            generation: CopyGeneration(1),
        }
    }

    #[test]
    fn clone_ticket_refuses_missing_identity_generation_and_invalid_token() {
        validate_clone_ticket(&ticket()).unwrap();
        for field in 0..8 {
            let mut changed = ticket();
            match field {
                0 => changed.token.clear(),
                1 => changed.token.push('\0'),
                2 => changed.token = "x".repeat(MAX_SNAPSHOT_TOKEN_BYTES + 1),
                3 => changed.source.identity.system_identifier = 0,
                4 => changed.source.identity.database_oid = 0,
                5 => changed.source.timeline = 0,
                6 => changed.source.major = 17,
                _ => changed.generation = CopyGeneration(0),
            }
            assert!(validate_clone_ticket(&changed).is_err(), "field {field}");
        }
    }
}

#[cfg(test)]
#[path = "snapshot_live_tests.rs"]
mod live_tests;

pub(crate) struct SourceSnapshot {
    source: PgConnection,
    exporter: PgConnection,
    receipt: SourceSlotReceipt,
    session: SessionFacts,
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
            })
        })()
        .map_err(source_failure)
    }

    pub(crate) fn receipt(&self) -> &SourceSlotReceipt {
        &self.receipt
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
    if io.cancelled.load(Ordering::Acquire) || io.is_expired() {
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
}
