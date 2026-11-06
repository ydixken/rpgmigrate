use super::{Checkpoint, CommitOutcome, ControlRow, Frontier, TargetCommit, metadata};
use crate::catalog::{command, configure_durability, confirm_durability};
use crate::contracts::{Error, ErrorKind, IoControl, Result};
use crate::identity::StreamId;
use crate::pg::{PgConnection, PgParam, PgResult, TransactionStatus};
use crate::state::{ConfirmedProvenance, NativeState, TargetBinding, TargetProvenance};
use std::collections::BTreeSet;

// Stable database-wide session fence: ASCII RPGM, serial writer slot 1, across all runs.
const FENCE_CLASS: i32 = 0x5250_474d;
const FENCE_SLOT: i32 = 1;
// Bound backend statement memory until the serial consumer owns eviction.
const MAX_PREPARED_DML: usize = 1024;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Idle,
    InTransaction,
    CommitPending,
    Poisoned,
}

struct Session {
    connection: Option<PgConnection>,
    phase: Phase,
    pid: u32,
    database: u32,
    role: u32,
    system: Option<u64>,
    synchronous_commit: &'static str,
}

pub(crate) struct TargetAuthority {
    session: Session,
    provenance: ConfirmedProvenance,
    row: ControlRow,
    prepared: BTreeSet<String>,
}

pub(crate) struct TargetTxn<'a> {
    authority: &'a mut TargetAuthority,
    next: Frontier,
    failed: bool,
    finished: bool,
}

#[derive(Debug)]
pub(crate) struct DurableProof {
    evidence: Box<ProofEvidence>,
}

#[derive(Debug)]
struct ProofEvidence {
    stream: StreamId,
    through: Option<Frontier>,
    barrier_epoch: u64,
    barrier_counter: u64,
    target: ConfirmedProvenance,
}

enum CommitReceipt {
    Acknowledged(Option<DurableProof>),
    Unknown,
}

impl DurableProof {
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn stream(&self) -> StreamId {
        self.evidence.stream
    }
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn frontier(&self) -> Option<&Frontier> {
        self.evidence.through.as_ref()
    }
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn barrier_epoch(&self) -> u64 {
        self.evidence.barrier_epoch
    }
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn barrier_counter(&self) -> u64 {
        self.evidence.barrier_counter
    }
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn target(&self) -> &ConfirmedProvenance {
        &self.evidence.target
    }
}

impl TargetAuthority {
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn acquire(
        connection: PgConnection,
        stream: StreamId,
        state: &mut NativeState,
        io: &IoControl,
    ) -> Result<Self> {
        if stream.run != state.identity().run {
            return Err(refusal("target-authority: native run and stream disagree"));
        }
        let previous = state.target_provenance(&stream).cloned();
        let mut session = Session::acquire(connection, "on", io)?;
        let fresh = previous.is_none();
        let intent = match &previous {
            Some(TargetProvenance::Provisioning(intent)) => intent.clone(),
            Some(TargetProvenance::Confirmed(confirmed)) => confirmed.intent.clone(),
            None => {
                let binding = TargetBinding {
                    stream,
                    target_system_identifier: session.system,
                    target_database_oid: session.database,
                    schema_name: schema_name(stream),
                    table_name: "checkpoint".into(),
                    owner_oid: session.role,
                    metadata_version: 1,
                };
                state.begin_target_provisioning(binding)?
            }
        };
        session.validate_binding(&intent.binding)?;
        session.begin(io)?;
        let pending = !matches!(previous, Some(TargetProvenance::Confirmed(_)));
        let provenance = match previous {
            Some(TargetProvenance::Confirmed(confirmed)) => {
                if metadata::probe(session.connection()?, &intent, io)?.as_ref() != Some(&confirmed)
                {
                    return Err(refusal(
                        "target-authority: confirmed metadata is absent or replaced",
                    ));
                }
                confirmed
            }
            _ => match metadata::probe(session.connection()?, &intent, io)? {
                Some(_) if fresh => {
                    return Err(refusal("target-authority: new metadata namespace collided"));
                }
                Some(confirmed) => confirmed,
                None if fresh || intent.binding.target_system_identifier.is_some() => {
                    session.verify(io)?;
                    metadata::create(session.connection()?, &intent, io)?
                }
                None => {
                    return Err(refusal(
                        "target-authority: pending receipt absent without strong target identity; start a fresh migration",
                    ));
                }
            },
        };
        metadata::prepare(session.connection()?, &provenance, io)?;
        let row = metadata::lock_validate(session.connection()?, &provenance, io)?;
        let mut authority = Self {
            session,
            provenance,
            row,
            prepared: BTreeSet::new(),
        };
        if pending {
            if row.applied.is_some() {
                return Err(refusal(
                    "target-authority: unconfirmed provisioning contains applied data",
                ));
            }
            let next = increment_barrier(row, row.barrier_epoch)?;
            authority.write_row(next, io)?;
            if matches!(
                authority.finish(next, TargetCommit::Synchronous, io)?,
                CommitReceipt::Unknown
            ) {
                return Err(refusal(
                    "target-authority: provisioning COMMIT outcome unknown; reconcile retained native intent",
                ));
            }
            state.confirm_target_provisioning(authority.provenance.clone())?;
        } else {
            authority.session.rollback(io)?;
        }
        Ok(authority)
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn checkpoint(&mut self, io: &IoControl) -> Result<Option<Checkpoint>> {
        let outcome = (|| {
            self.session.begin(io)?;
            self.row = self.read_row(io)?;
            self.session.rollback(io)?;
            Ok(self.current_checkpoint())
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn begin(&mut self, next: Frontier, io: &IoControl) -> Result<TargetTxn<'_>> {
        validate_next(
            self.provenance.intent.binding.stream,
            self.row.applied,
            next,
        )?;
        let outcome = (|| {
            self.session.begin(io)?;
            let row = self.read_row(io)?;
            validate_next(self.provenance.intent.binding.stream, row.applied, next)?;
            self.row = row;
            Ok(())
        })();
        if let Err(error) = outcome {
            self.session.close();
            return Err(error);
        }
        Ok(TargetTxn {
            authority: self,
            next,
            failed: false,
            finished: false,
        })
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn reacquire(
        &mut self,
        connection: PgConnection,
        io: &IoControl,
    ) -> Result<Option<Checkpoint>> {
        self.session.close();
        self.prepared.clear();
        let mut session = Session::acquire(connection, self.session.synchronous_commit, io)?;
        session.validate_binding(&self.provenance.intent.binding)?;
        session.begin(io)?;
        if metadata::probe(session.connection()?, &self.provenance.intent, io)?.as_ref()
            != Some(&self.provenance)
        {
            return Err(refusal(
                "target-authority: confirmed metadata is absent or replaced",
            ));
        }
        metadata::prepare(session.connection()?, &self.provenance, io)?;
        session.verify(io)?;
        let row = metadata::lock_validate(session.connection()?, &self.provenance, io)?;
        session.rollback(io)?;
        self.session = session;
        self.row = row;
        Ok(self.current_checkpoint())
    }

    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn barrier(
        &mut self,
        through: Option<&Frontier>,
        epoch: u64,
        io: &IoControl,
    ) -> Result<DurableProof> {
        let outcome = (|| {
            self.session.begin(io)?;
            self.row = self.read_row(io)?;
            // Exact equality preserves the last descriptor identity even when source LSNs tie.
            if self.row.applied.as_ref() != through {
                return Err(refusal(
                    "target-authority: barrier frontier differs from the fenced checkpoint",
                ));
            }
            let next = increment_barrier(self.row, epoch)?;
            self.write_row(next, io)?;
            match self.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(proof)) => Ok(proof),
                CommitReceipt::Acknowledged(None) => Err(refusal(
                    "target-authority: synchronous barrier produced no proof",
                )),
                CommitReceipt::Unknown => {
                    Err(refusal("target-authority: barrier COMMIT outcome unknown"))
                }
            }
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    fn read_row(&mut self, io: &IoControl) -> Result<ControlRow> {
        self.session.require_transaction()?;
        self.session.verify(io)?;
        metadata::lock_validate(self.session.connection()?, &self.provenance, io)
    }

    fn write_row(&mut self, next: ControlRow, io: &IoControl) -> Result<()> {
        if self.read_row(io)? != self.row {
            return Err(refusal(
                "target-authority: checkpoint changed outside its owner",
            ));
        }
        metadata::update(
            self.session.connection()?,
            &self.provenance,
            &self.row,
            &next,
            io,
        )
    }

    fn current_checkpoint(&self) -> Option<Checkpoint> {
        self.row.applied.map(|applied| Checkpoint {
            stream: self.provenance.intent.binding.stream,
            applied,
            barrier_epoch: self.row.barrier_epoch,
            barrier_counter: self.row.barrier_counter,
        })
    }

    fn finish(
        &mut self,
        next: ControlRow,
        mode: TargetCommit,
        io: &IoControl,
    ) -> Result<CommitReceipt> {
        self.session.require_transaction()?;
        command(
            self.session.connection()?,
            "SET CONSTRAINTS ALL IMMEDIATE",
            "SET CONSTRAINTS",
            io,
        )?;
        if self.read_row(io)? != next {
            return Err(refusal(
                "target-authority: deferred effects changed the checkpoint",
            ));
        }
        let required = self.session.synchronous_commit;
        confirm_durability(self.session.connection()?, required, io)?;
        #[cfg(test)]
        if mode == TargetCommit::Asynchronous {
            command(
                self.session.connection()?,
                "SET LOCAL synchronous_commit = off",
                "SET",
                io,
            )?;
        }
        self.session.verify(io)?;
        self.session.require_transaction()?;
        self.session.phase = Phase::CommitPending;
        let evidence = self
            .session
            .connection()?
            .query("COMMIT", io)
            .and_then(|result| {
                let status = self.session.connection()?.transaction_status()?;
                Ok(commit_evidence(result.command_tag()?, status))
            });
        match evidence {
            Ok(CommitEvidence::Committed) => {
                self.session.phase = Phase::Idle;
                self.row = next;
                let proof = (mode == TargetCommit::Synchronous).then(|| DurableProof {
                    evidence: Box::new(ProofEvidence {
                        stream: self.provenance.intent.binding.stream,
                        through: next.applied,
                        barrier_epoch: next.barrier_epoch,
                        barrier_counter: next.barrier_counter,
                        target: self.provenance.clone(),
                    }),
                });
                Ok(CommitReceipt::Acknowledged(proof))
            }
            Ok(CommitEvidence::RolledBack) => {
                self.session.phase = Phase::Idle;
                Err(refusal("target-authority: COMMIT returned ROLLBACK"))
            }
            _ => {
                self.session.close();
                Ok(CommitReceipt::Unknown)
            }
        }
    }
}

impl TargetTxn<'_> {
    /// Only the internal serial SQL generator supplies statements; transaction control is forbidden.
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn prepare(
        &mut self,
        name: &str,
        sql: &str,
        types: &[u32],
        io: &IoControl,
    ) -> Result<()> {
        let outcome = (|| {
            self.require_healthy()?;
            if name.is_empty()
                || name.starts_with("rpgmigrate_")
                || name.len() > 128
                || self.authority.prepared.len() >= MAX_PREPARED_DML
                || !matches!(
                    sql.split_ascii_whitespace().next(),
                    Some("INSERT" | "UPDATE" | "DELETE" | "TRUNCATE")
                )
            {
                return Err(refusal(
                    "target-authority: invalid or excessive internal DML statement",
                ));
            }
            self.authority.session.verify(io)?;
            self.authority
                .session
                .connection()?
                .prepare(name, sql, types, io)?;
            self.authority.prepared.insert(name.to_owned());
            Ok(())
        })();
        self.failed |= outcome.is_err();
        outcome
    }
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn execute_prepared(
        &mut self,
        name: &str,
        params: &[PgParam<'_>],
        io: &IoControl,
    ) -> Result<PgResult> {
        let outcome = (|| {
            self.require_healthy()?;
            if !self.authority.prepared.contains(name) {
                return Err(refusal(
                    "target-authority: DML statement is not prepared on this backend",
                ));
            }
            if self.authority.read_row(io)? != self.authority.row {
                return Err(refusal(
                    "target-authority: checkpoint changed before user DML",
                ));
            }
            self.authority
                .session
                .connection()?
                .execute_prepared(name, params, io)
        })();
        self.failed |= outcome.is_err();
        outcome
    }
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn commit(self, io: &IoControl) -> Result<CommitOutcome> {
        self.commit_with_mode(TargetCommit::Synchronous, io)
    }
    fn commit_with_mode(mut self, mode: TargetCommit, io: &IoControl) -> Result<CommitOutcome> {
        self.require_healthy()?;
        let mut next = ControlRow {
            applied: Some(self.next),
            ..self.authority.row
        };
        if mode == TargetCommit::Synchronous {
            next = increment_barrier(next, next.barrier_epoch)?;
        }
        self.authority.write_row(next, io)?;
        let result = self.authority.finish(next, mode, io)?;
        self.finished = true;
        match result {
            CommitReceipt::Acknowledged(durable) => Ok(CommitOutcome::Acknowledged {
                checkpoint: self
                    .authority
                    .current_checkpoint()
                    .ok_or_else(|| refusal("target-authority: committed frontier is empty"))?,
                durable,
            }),
            CommitReceipt::Unknown => Ok(CommitOutcome::Unknown),
        }
    }
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn rollback(mut self, io: &IoControl) -> Result<()> {
        self.authority.session.rollback(io)?;
        self.finished = true;
        Ok(())
    }

    fn require_healthy(&self) -> Result<()> {
        if self.failed {
            return Err(refusal(
                "target-authority: transaction poisoned by an earlier DML error",
            ));
        }
        Ok(())
    }
}

impl Drop for TargetTxn<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.authority.session.close();
        }
    }
}

impl Session {
    fn acquire(
        mut connection: PgConnection,
        minimum: &'static str,
        io: &IoControl,
    ) -> Result<Self> {
        if connection.transaction_status()? != TransactionStatus::Idle {
            return Err(refusal(
                "target-authority: connection must start idle on a dedicated backend",
            ));
        }
        let result = connection.query("SELECT pg_catalog.pg_backend_pid(), d.oid, r.oid,
            NOT pg_catalog.pg_is_in_recovery() AND pg_catalog.current_setting('transaction_read_only') = 'off'
            AND pg_catalog.current_setting('fsync') = 'on'
            FROM pg_catalog.pg_database d, pg_catalog.pg_roles r
            WHERE d.datname = pg_catalog.current_database() AND r.rolname = CURRENT_USER", io)?;
        if result.rows() != 1 || result.cell(0, 3)? != Some(b"t") {
            return Err(refusal(
                "target-authority: writable primary with fsync on is required",
            ));
        }
        let pid = number(&result, 0)?;
        let database = number(&result, 1)?;
        let role = number(&result, 2)?;
        let system = optional_system_identifier(&mut connection, io)?;
        let captured = configure_durability(&mut connection, io)?;
        let synchronous_commit = if minimum == "remote_apply" {
            minimum
        } else {
            captured
        };
        command(&mut connection, "SET search_path = pg_catalog", "SET", io)?;
        let locked = connection.query(
            &format!("SELECT pg_catalog.pg_advisory_lock({FENCE_CLASS}, {FENCE_SLOT})"),
            io,
        )?;
        if locked.rows() != 1 {
            return Err(refusal("target-authority: writer fence returned no result"));
        }
        connection.prepare("rpgmigrate_authority_guard", &format!(
            "SELECT pg_catalog.pg_backend_pid() = $1::integer
            AND (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()) = $2::oid
            AND (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER) = $3::oid
            AND NOT pg_catalog.pg_is_in_recovery()
            AND pg_catalog.current_setting('transaction_read_only') = 'off'
            AND pg_catalog.current_setting('fsync') = 'on'
            AND pg_catalog.current_setting('search_path') = 'pg_catalog'
            AND EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype = 'advisory'
                AND database = $2::oid AND pid = $1::integer AND classid = {FENCE_CLASS}::oid
                AND objid = {FENCE_SLOT}::oid AND objsubid = 2 AND mode = 'ExclusiveLock' AND granted)"
        ), &[23, 26, 26], io)?;
        let mut session = Self {
            connection: Some(connection),
            phase: Phase::Idle,
            pid,
            database,
            role,
            system,
            synchronous_commit,
        };
        session.verify(io)?;
        Ok(session)
    }

    fn validate_binding(&self, binding: &TargetBinding) -> Result<()> {
        if binding.target_database_oid != self.database
            || binding
                .target_system_identifier
                .is_some_and(|system| Some(system) != self.system)
        {
            return Err(refusal(
                "target-authority: target binding cannot be freshly verified",
            ));
        }
        Ok(())
    }

    fn connection(&mut self) -> Result<&mut PgConnection> {
        self.connection.as_mut().ok_or_else(|| {
            refusal("target-authority: connection poisoned; reacquire the primary writer fence")
        })
    }

    fn close(&mut self) {
        self.connection.take();
        self.phase = Phase::Poisoned;
    }

    fn verify(&mut self, io: &IoControl) -> Result<()> {
        let values = [
            self.pid.to_string(),
            self.database.to_string(),
            self.role.to_string(),
        ];
        let params = values
            .each_ref()
            .map(|value| PgParam::Text(value.as_bytes()));
        let result =
            self.connection()?
                .execute_prepared("rpgmigrate_authority_guard", &params, io)?;
        if result.rows() != 1 || result.cell(0, 0)? != Some(b"t") {
            return Err(refusal(
                "target-authority: physical backend, primary, or writer fence changed",
            ));
        }
        Ok(())
    }

    fn begin(&mut self, io: &IoControl) -> Result<()> {
        if self.phase != Phase::Idle
            || self.connection()?.transaction_status()? != TransactionStatus::Idle
        {
            return Err(refusal("target-authority: transaction is not idle"));
        }
        self.verify(io)?;
        command(
            self.connection()?,
            "BEGIN ISOLATION LEVEL READ COMMITTED",
            "BEGIN",
            io,
        )?;
        self.phase = Phase::InTransaction;
        self.require_transaction()?;
        self.verify(io)
    }

    fn require_transaction(&mut self) -> Result<()> {
        if self.phase != Phase::InTransaction
            || self.connection()?.transaction_status()? != TransactionStatus::InTransaction
        {
            return Err(refusal(
                "target-authority: a valid owned transaction is required",
            ));
        }
        Ok(())
    }

    fn rollback(&mut self, io: &IoControl) -> Result<()> {
        if self.phase != Phase::InTransaction {
            return Err(refusal(
                "target-authority: rollback requires an owned transaction",
            ));
        }
        command(self.connection()?, "ROLLBACK", "ROLLBACK", io)?;
        if self.connection()?.transaction_status()? != TransactionStatus::Idle {
            return Err(refusal(
                "target-authority: rollback did not leave an idle connection",
            ));
        }
        self.phase = Phase::Idle;
        self.verify(io)
    }
}

#[derive(Debug, Eq, PartialEq)]
enum CommitEvidence {
    Committed,
    RolledBack,
    Unknown,
}

fn commit_evidence(tag: &str, status: TransactionStatus) -> CommitEvidence {
    match (tag, status) {
        ("COMMIT", TransactionStatus::Idle) => CommitEvidence::Committed,
        ("ROLLBACK", TransactionStatus::Idle) => CommitEvidence::RolledBack,
        _ => CommitEvidence::Unknown,
    }
}

fn increment_barrier(row: ControlRow, epoch: u64) -> Result<ControlRow> {
    if epoch < row.barrier_epoch {
        return Err(refusal("target-authority: barrier epoch regressed"));
    }
    Ok(ControlRow {
        barrier_epoch: epoch,
        barrier_counter: row
            .barrier_counter
            .checked_add(1)
            .ok_or_else(|| refusal("target-authority: barrier counter exhausted"))?,
        ..row
    })
}

fn schema_name(stream: StreamId) -> String {
    use std::fmt::Write;
    let mut name = String::from("_rpgmigrate_");
    for byte in stream.run.0 {
        write!(name, "{byte:02x}").expect("writing a String cannot fail");
    }
    name
}

fn number(result: &PgResult, column: usize) -> Result<u32> {
    let value = result
        .cell(0, column)?
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .and_then(|text| text.parse::<u32>().ok())
        .filter(|value| *value != 0);
    value.ok_or_else(|| refusal("target-authority: invalid backend or object identity"))
}

fn optional_system_identifier(
    connection: &mut PgConnection,
    io: &IoControl,
) -> Result<Option<u64>> {
    match connection.query(
        "SELECT system_identifier::text FROM pg_catalog.pg_control_system()",
        io,
    ) {
        Ok(result) => {
            if result.rows() != 1 {
                return Err(refusal("target-authority: invalid target system identity"));
            }
            let value = result
                .cell(0, 0)?
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
                .and_then(|text| text.parse::<i64>().ok())
                .map(|value| value as u64)
                .filter(|value| *value != 0);
            value
                .map(Some)
                .ok_or_else(|| refusal("target-authority: invalid target system identity"))
        }
        Err(error)
            if (error.message.contains("42501") || error.message.contains("42883"))
                && connection.transaction_status()? == TransactionStatus::Idle =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn validate_next(stream: StreamId, previous: Option<Frontier>, next: Frontier) -> Result<()> {
    let expected = previous.map_or(Some(1), |value| value.order.0.checked_add(1));
    if next.tx.stream != stream
        || Some(next.order.0) != expected
        || next.end.0 < next.commit.0
        || previous.is_some_and(|value| {
            value.tx.stream != stream || next.commit < value.commit || next.end < value.end
        })
    {
        return Err(refusal(
            "target-authority: noncontiguous or inconsistent source frontier",
        ));
    }
    Ok(())
}

fn refusal(message: &str) -> Error {
    Error {
        kind: ErrorKind::Target,
        message: message.to_owned(),
    }
}

#[cfg(test)]
#[path = "checkpoint_live_tests.rs"]
mod live_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{CommitLsn, RunId, SourceEndLsn, SourceId, TxId, TxSeq};

    fn frontier(order: u64, commit: u64, end: u64) -> Frontier {
        Frontier {
            tx: TxId {
                stream: StreamId {
                    run: RunId([7; 16]),
                    source: SourceId {
                        system_identifier: 9,
                        database_oid: 11,
                    },
                    generation: 1,
                },
                xid: 42,
                begin_ordinal: 23,
            },
            order: TxSeq(order),
            commit: CommitLsn(commit),
            end: SourceEndLsn(end),
        }
    }

    #[test]
    fn exact_order_allows_equal_lsns_and_reused_xids_with_earlier_begin() {
        let first = frontier(1, 100, 120);
        assert!(validate_next(first.tx.stream, None, first).is_ok());
        let mut next = frontier(2, 100, 120);
        next.tx.begin_ordinal = 3;
        assert!(validate_next(first.tx.stream, Some(first), next).is_ok());
    }

    #[test]
    fn rejects_gaps_regressions_zero_and_cross_stream_identity() {
        let first = frontier(1, 100, 120);
        for next in [
            frontier(0, 100, 120),
            frontier(1, 100, 120),
            frontier(3, 100, 120),
            frontier(2, 99, 120),
            frontier(2, 100, 119),
            frontier(2, 121, 120),
        ] {
            assert!(validate_next(first.tx.stream, Some(first), next).is_err());
        }
        assert!(validate_next(first.tx.stream, None, frontier(2, 100, 120)).is_err());
        let mut next = frontier(2, 100, 120);
        next.tx.stream.generation += 1;
        assert!(validate_next(first.tx.stream, Some(first), next).is_err());
        assert!(validate_next(next.tx.stream, Some(first), next).is_err());
        assert!(validate_next(first.tx.stream, Some(frontier(u64::MAX, 100, 120)), first).is_err());
    }

    #[test]
    fn barriers_reject_counter_overflow_and_epoch_regression() {
        let row = ControlRow {
            applied: None,
            barrier_epoch: 7,
            barrier_counter: u64::MAX,
        };
        assert!(increment_barrier(row, 7).is_err());
        assert!(
            increment_barrier(
                ControlRow {
                    barrier_counter: 0,
                    ..row
                },
                6
            )
            .is_err()
        );
        assert_eq!(
            increment_barrier(
                ControlRow {
                    barrier_counter: 0,
                    ..row
                },
                7
            )
            .unwrap(),
            ControlRow {
                applied: None,
                barrier_epoch: 7,
                barrier_counter: 1
            }
        );
    }

    #[test]
    fn only_exact_commit_tag_with_idle_state_is_positive_evidence() {
        assert_eq!(
            commit_evidence("COMMIT", TransactionStatus::Idle),
            CommitEvidence::Committed
        );
        assert_eq!(
            commit_evidence("ROLLBACK", TransactionStatus::Idle),
            CommitEvidence::RolledBack
        );
        for status in [
            TransactionStatus::Active,
            TransactionStatus::InTransaction,
            TransactionStatus::InError,
            TransactionStatus::Unknown,
        ] {
            assert_eq!(commit_evidence("COMMIT", status), CommitEvidence::Unknown);
        }
        for tag in ["", "COMMIT PREPARED", "UPDATE 1"] {
            assert_eq!(
                commit_evidence(tag, TransactionStatus::Idle),
                CommitEvidence::Unknown
            );
        }
    }
}
