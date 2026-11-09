//! A snapshot requires dedicated physical sessions without reconnect or pooling.
//! Persisted token text cannot replace the imported transaction owned here.

use std::sync::atomic::Ordering;

use crate::catalog;
use crate::contracts::{Error, ErrorKind, IoControl, MAX_SNAPSHOT_TOKEN_BYTES, Result};
#[cfg(test)]
use crate::identity::SourceId;
use crate::pg::{PgConnection, SlotCreateOutcome, TransactionStatus};
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

mod follow;
pub(crate) use follow::{FollowSnapshot, recover_follow_binding};

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
#[path = "snapshot/follow_tests.rs"]
mod follow_tests;

#[cfg(test)]
#[path = "snapshot/follow_fault_tests.rs"]
mod follow_fault_tests;

fn created_data(
    binding: &SourceSlotBinding,
    outcome: SlotCreateOutcome,
) -> Result<CreatedSlotData> {
    let created = match outcome {
        SlotCreateOutcome::Created(created) => created,
        SlotCreateOutcome::Unknown(failure) => {
            return Err(refusal(&format!(
                "source-slot-create-unknown: slot may retain WAL; retain native intent for manual review (failure category {:?})",
                failure.kind
            )));
        }
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

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::Source, message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::contracts::Plugin;
    use crate::contracts::{ErrorKind, MAX_SNAPSHOT_TOKEN_BYTES};
    use crate::identity::{RunId, SourceEndLsn, SourceId, StreamId};
    use crate::pg::{CreatedLogicalSlot, SlotCreateOutcome, TransactionStatus};
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

    fn wire(token: String) -> CreatedLogicalSlot {
        CreatedLogicalSlot {
            slot_name: "pgcopydb".into(),
            plugin: "test_decoding".into(),
            consistent_point: SourceEndLsn(u64::MAX),
            snapshot_token: token,
        }
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
            SlotCreateOutcome::Unknown(Error::new(ErrorKind::Pgsql, "transport failure")),
        )
        .err()
        .expect("unknown creation cannot produce data");
        assert_eq!(failure.kind, ErrorKind::Source);
        assert!(failure.message.starts_with("source-slot-create-unknown:"));
        assert!(failure.message.contains("failure category Pgsql"));
        assert!(!failure.message.contains("transport failure"));
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
