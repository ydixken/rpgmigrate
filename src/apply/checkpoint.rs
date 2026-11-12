use super::{Checkpoint, CommitOutcome, ControlRow, Frontier, TargetCommit, metadata};
use crate::catalog::{command, configure_durability, confirm_durability};
use crate::cdc::contracts::Event;
use crate::contracts::{
    CopyReport, Error, ErrorKind, FollowBinding, IoControl, OwnedCopyTable, Result, SnapshotAnchor,
};
use crate::identity::{SourceEndLsn, StreamId};
use crate::pg::{CommitObservation, PgConnection, PgParam, PgResult, TransactionStatus};
use crate::state::{
    ConfirmedProvenance, ControlHandle, NativeState, SealedEndpos, TARGET_METADATA_VERSION,
    TargetBinding, TargetProvenance,
};
use std::collections::BTreeSet;

// Stable database-wide session fence: ASCII RPGM, serial writer slot 1, across all runs.
const FENCE_CLASS: i32 = 0x5250_474d;
fn fence_slot(run: crate::identity::RunId) -> i32 {
    i32::from_ne_bytes(crc32fast::hash(&run.0).to_ne_bytes())
}
// Bound backend statement memory independently of the consumer cache.
const MAX_PREPARED_DML: usize = 1024;
const CHECKPOINT_TABLE: &str = "checkpoint";

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Idle,
    BeginPending,
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
    fence_slot: i32,
}

pub(crate) struct TargetAuthority {
    session: Session,
    provenance: ConfirmedProvenance,
    row: ControlRow,
    prepared: BTreeSet<String>,
    control: Option<ControlHandle>,
}

pub(crate) struct TargetTxn<'a> {
    authority: &'a mut TargetAuthority,
    next: Option<Frontier>,
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
    certified_lsn: Option<SourceEndLsn>,
    follow: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainKind {
    Keepalive,
    LaterBegin,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DrainWitness {
    Keepalive(SourceEndLsn),
    LaterBegin(crate::identity::CommitLsn),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct DrainRecord {
    pub(super) end: SourceEndLsn,
    pub(super) witness: SourceEndLsn,
    pub(super) kind: DrainKind,
}

#[derive(Debug)]
pub(crate) struct DurableFollowSnapshot {
    stream: StreamId,
    durable_replay_lsn: SourceEndLsn,
    certified_lsn: SourceEndLsn,
    checkpoint_id: String,
    follow_completed: bool,
    target: ConfirmedProvenance,
}

impl DurableFollowSnapshot {
    pub(crate) fn stream(&self) -> StreamId {
        self.stream
    }
    pub(crate) fn durable_replay_lsn(&self) -> SourceEndLsn {
        self.durable_replay_lsn
    }
    pub(crate) fn certified_lsn(&self) -> SourceEndLsn {
        self.certified_lsn
    }
    pub(crate) fn checkpoint_id(&self) -> String {
        self.checkpoint_id.clone()
    }
    pub(crate) fn follow_completed(&self) -> bool {
        self.follow_completed
    }
    pub(crate) fn target(&self) -> &ConfirmedProvenance {
        &self.target
    }
}

#[derive(Debug)]
pub(crate) struct CompletedFollow {
    snapshot: DurableFollowSnapshot,
    drain: DrainRecord,
}

impl CompletedFollow {
    pub(crate) fn end(&self) -> SourceEndLsn {
        self.drain.end
    }
    pub(crate) fn witness(&self) -> SourceEndLsn {
        self.drain.witness
    }
    pub(crate) fn kind(&self) -> DrainKind {
        self.drain.kind
    }
    pub(crate) fn durable_replay_lsn(&self) -> SourceEndLsn {
        self.snapshot.durable_replay_lsn()
    }
    pub(crate) fn certified_lsn(&self) -> SourceEndLsn {
        self.snapshot.certified_lsn()
    }
    pub(crate) fn checkpoint_id(&self) -> String {
        self.snapshot.checkpoint_id()
    }
    pub(crate) fn target(&self) -> &ConfirmedProvenance {
        self.snapshot.target()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SafeFeedback {
    stream: StreamId,
    end: SourceEndLsn,
}

impl SafeFeedback {
    pub(crate) fn stream(&self) -> StreamId {
        self.stream
    }
    pub(crate) fn end(&self) -> SourceEndLsn {
        self.end
    }
}

#[cfg(test)]
pub(crate) fn completed_for_test() -> CompletedFollow {
    let (authority, _) = tests::disconnected_authority();
    CompletedFollow {
        snapshot: DurableFollowSnapshot {
            stream: authority.provenance.intent.binding.stream,
            durable_replay_lsn: SourceEndLsn(0x10),
            certified_lsn: SourceEndLsn(0x10),
            checkpoint_id: "1:0:3".into(),
            follow_completed: true,
            target: authority.provenance,
        },
        drain: DrainRecord {
            end: SourceEndLsn(0x20),
            witness: SourceEndLsn(0x30),
            kind: DrainKind::LaterBegin,
        },
    }
}

#[cfg(test)]
pub(crate) fn feedback_for_test(stream: StreamId, end: SourceEndLsn) -> SafeFeedback {
    assert_ne!(end.0, 0);
    SafeFeedback { stream, end }
}

#[derive(Clone, Debug)]
pub(crate) struct RecoveryStart {
    stream: StreamId,
    applied: Option<Frontier>,
    certified_lsn: Option<SourceEndLsn>,
    anchor: Option<SnapshotAnchor>,
    start_lsn: SourceEndLsn,
    confirmed_flush_lsn: SourceEndLsn,
}

#[cfg(test)]
pub(crate) fn recovery_start_for_test(
    binding: &FollowBinding,
    start_lsn: SourceEndLsn,
    confirmed_flush_lsn: SourceEndLsn,
) -> RecoveryStart {
    assert!(start_lsn >= confirmed_flush_lsn);
    assert!(start_lsn >= binding.slot.consistent_point);
    RecoveryStart {
        stream: binding.stream(),
        applied: None,
        certified_lsn: (start_lsn > binding.slot.consistent_point).then_some(start_lsn),
        anchor: Some(SnapshotAnchor {
            stream: binding.stream(),
            snapshot_token: binding.slot.snapshot_token.clone(),
            consistent_point: binding.slot.consistent_point,
            plan_nonce: binding.slot.intent.nonce,
        }),
        start_lsn,
        confirmed_flush_lsn,
    }
}

impl RecoveryStart {
    pub(crate) fn stream(&self) -> StreamId {
        self.stream
    }
    pub(crate) fn applied(&self) -> Option<Frontier> {
        self.applied
    }
    pub(crate) fn certified_lsn(&self) -> Option<SourceEndLsn> {
        self.certified_lsn
    }
    pub(crate) fn anchor(&self) -> Option<&SnapshotAnchor> {
        self.anchor.as_ref()
    }
    pub(crate) fn start_lsn(&self) -> SourceEndLsn {
        self.start_lsn
    }
    pub(crate) fn confirmed_flush_lsn(&self) -> SourceEndLsn {
        self.confirmed_flush_lsn
    }
}

enum CommitReceipt {
    Acknowledged(Option<DurableProof>),
    Unknown,
}

impl DurableProof {
    pub(crate) fn feedback(&self) -> Option<SafeFeedback> {
        if !self.evidence.follow {
            return None;
        }
        self.evidence
            .through
            .map(|f| f.end)
            .into_iter()
            .chain(self.evidence.certified_lsn)
            .max()
            .map(|end| SafeFeedback {
                stream: self.evidence.stream,
                end,
            })
    }

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
        Self::acquire_inner(connection, stream, state, io, 1)
    }

    fn acquire_inner(
        connection: PgConnection,
        stream: StreamId,
        state: &mut NativeState,
        io: &IoControl,
        slot: i32,
    ) -> Result<Self> {
        if stream.run != state.identity().run {
            return Err(refusal("target-authority: native run and stream disagree"));
        }
        let previous = state.target_provenance(&stream).cloned();
        let mut session = Session::acquire(connection, "on", slot, io)?;
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
                    table_name: CHECKPOINT_TABLE.into(),
                    owner_oid: session.role,
                    metadata_version: TARGET_METADATA_VERSION,
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
            row: row.clone(),
            prepared: BTreeSet::new(),
            control: None,
        };
        if pending {
            validate_pending_row(&row)?;
            let epoch = row.barrier_epoch;
            let next = increment_barrier(row, epoch)?;
            authority.write_row(&next, io)?;
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
        self.require_mutable(io)?;
        if self.row.follow_binding.is_some() {
            validate_follow_next(
                self.provenance.intent.binding.stream,
                self.row.applied,
                next,
            )?;
            if self.row.anchor.is_none()
                || durable_start(&self.row).is_some_and(|floor| next.end <= floor)
            {
                return Err(refusal(
                    "target-follow: source transaction does not advance anchored authority",
                ));
            }
        }
        validate_next(
            self.provenance.intent.binding.stream,
            self.row.applied,
            next,
        )?;
        let outcome = (|| {
            self.session.begin(io)?;
            let row = self.read_row(io)?;
            validate_next(self.provenance.intent.binding.stream, row.applied, next)?;
            if row != self.row {
                return Err(refusal(
                    "target-authority: checkpoint changed before owned transaction",
                ));
            }
            self.session.require_transaction()?;
            self.row = row;
            Ok(())
        })();
        if let Err(error) = outcome {
            self.session.close();
            return Err(error);
        }
        Ok(TargetTxn {
            authority: self,
            next: Some(next),
            failed: false,
            finished: false,
        })
    }

    pub(crate) fn acquire_follow(
        connection: PgConnection,
        native: &mut NativeState,
        binding: &FollowBinding,
        io: &IoControl,
    ) -> Result<Self> {
        let expected = native
            .follow_control()?
            .ok_or_else(|| refusal("target-follow: native follow binding is absent"))?;
        if expected.binding != *binding {
            return Err(refusal(
                "target-follow: requested follow binding differs from native state",
            ));
        }
        let mut authority = Self::acquire_inner(
            connection,
            binding.stream(),
            native,
            io,
            fence_slot(binding.stream().run),
        )?;
        if authority.row.follow_binding.as_ref() != Some(&expected) {
            return Err(refusal(
                "target-follow: logged follow binding differs from native state",
            ));
        }
        authority.control = Some(ControlHandle::for_worker(native, io)?);
        Ok(authority)
    }

    pub(crate) fn try_acquire_follow(
        connection: PgConnection,
        control: &ControlHandle,
        io: &IoControl,
    ) -> Result<Self> {
        control.validate(io)?;
        let checked = control.try_clone(io)?;
        let provenance = control.target().clone();
        let mut session = Session::acquire_mode(
            connection,
            "on",
            fence_slot(control.follow().binding.stream().run),
            true,
            io,
        )?;
        control.validate(io)?;
        session.validate_binding(&provenance.intent.binding)?;
        session.begin(io)?;
        if metadata::probe(session.connection()?, &provenance.intent, io)?.as_ref()
            != Some(&provenance)
        {
            return Err(refusal(
                "target-follow: existing confirmed metadata is absent or replaced",
            ));
        }
        metadata::prepare(session.connection()?, &provenance, io)?;
        let row = metadata::lock_validate(session.connection()?, &provenance, io)?;
        admitted_follow(&row, control.follow(), control.plan())?;
        session.rollback(io)?;
        Ok(Self {
            session,
            provenance,
            row,
            prepared: BTreeSet::new(),
            control: Some(checked),
        })
    }

    pub(crate) fn observe_follow(&mut self, io: &IoControl) -> Result<DurableFollowSnapshot> {
        let outcome = (|| {
            self.control
                .as_ref()
                .ok_or_else(|| refusal("target-follow: bound native control is absent"))?
                .validate(io)?;
            self.session.begin(io)?;
            self.row = self.read_row(io)?;
            self.validate_admission()?;
            let next = self.row.clone();
            self.write_row(&next, io)?;
            match self.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(proof)) => self.follow_snapshot(proof),
                _ => Err(refusal("target-follow: observation COMMIT outcome unknown")),
            }
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    pub(crate) fn record_drain(
        &mut self,
        request: &SealedEndpos,
        witness: DrainWitness,
        io: &IoControl,
    ) -> Result<DurableProof> {
        let outcome = (|| {
            self.require_mutable(io)?;
            self.validate_sealed(request, io, true)?;
            self.session.begin(io)?;
            if self.read_row(io)? != self.row {
                return Err(refusal("target-follow: drain predecessor changed"));
            }
            self.validate_sealed(request, io, true)?;
            let next = drained_row(&self.row, request.end(), witness)?;
            self.write_row(&next, io)?;
            match self.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(proof)) => Ok(proof),
                _ => Err(refusal("target-follow: drain COMMIT outcome unknown")),
            }
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    pub(crate) fn complete_follow(
        &mut self,
        options: &crate::contracts::CloneOptions,
        source: &mut PgConnection,
        native: &NativeState,
        plan: &ClonePlan,
        request: &SealedEndpos,
        io: &IoControl,
    ) -> Result<CompletedFollow> {
        if self.row.follow_completed {
            return self.observe_completed(request, io);
        }
        let outcome = (|| {
            self.require_mutable(io)?;
            self.validate_sealed(request, io, true)?;
            let saved = native
                .checked_clone()?
                .ok_or_else(|| refusal("target-follow: final owned inventory is absent"))?;
            if native.checked_follow_cleanup()?.is_some()
                || native.checked_follow_restart()?.is_some()
                || saved.plan != *plan
                || saved.phase != ClonePhase::Analyzing
                || native.follow_control()?.as_ref() != self.row.follow_binding.as_ref()
                || native.target_provenance(&request.request().stream())
                    != Some(&TargetProvenance::Confirmed(self.provenance.clone()))
            {
                return Err(refusal(
                    "target-follow: final native admission or inventory changed",
                ));
            }
            if options.copy_policy() != plan.copy_policy {
                return Err(refusal(
                    "target-follow: final large-object policy differs from bound plan",
                ));
            }
            if !plan.copy_policy.skip_large_objects {
                crate::large_objects::run_final(options, plan, request, self, io)?;
            }
            self.session.begin(io)?;
            if self.read_row(io)? != self.row {
                return Err(refusal("target-follow: completion predecessor changed"));
            }
            self.validate_sealed(request, io, true)?;
            if self
                .row
                .drain
                .is_none_or(|drain| drain.end != request.end())
            {
                return Err(refusal(
                    "target-follow: completion lacks the exact durable drain",
                ));
            }
            let markers = metadata::read_markers(self.session.connection()?, &self.provenance, io)?;
            validate_markers(&markers, &saved)?;
            aggregate_markers(&markers, &saved)?;
            let clone = self
                .row
                .clone
                .as_ref()
                .ok_or_else(|| refusal("target-follow: final clone control is absent"))?;
            clone.require_initial_large_objects(plan)?;
            if !plan.copy_policy.skip_large_objects && clone.large_objects.final_sync.is_none() {
                return Err(refusal(
                    "target-follow: final large-object completion is absent",
                ));
            }
            crate::catalog::sync_clone_sequences(
                source,
                self.session.connection()?,
                plan,
                &saved.owned_objects,
                io,
            )?;
            self.validate_sealed(request, io, true)?;
            if native.checked_clone()?.as_ref() != Some(&saved)
                || native.checked_follow_cleanup()?.is_some()
            {
                return Err(refusal(
                    "target-follow: native final inventory changed during sequence synchronization",
                ));
            }
            let mut next = self.row.clone();
            let epoch = next.barrier_epoch;
            next = increment_barrier(next, epoch)?;
            next.follow_completed = true;
            self.write_row(&next, io)?;
            match self.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(proof)) => self.completed_snapshot(proof),
                _ => Err(refusal("target-follow: completion COMMIT outcome unknown")),
            }
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    pub(crate) fn observe_completed(
        &mut self,
        request: &SealedEndpos,
        io: &IoControl,
    ) -> Result<CompletedFollow> {
        let outcome = (|| {
            self.session.begin(io)?;
            self.row = self.read_row(io)?;
            self.validate_sealed(request, io, false)?;
            if !self.row.follow_completed
                || self
                    .row
                    .drain
                    .is_none_or(|drain| drain.end != request.end())
            {
                return Err(refusal(
                    "target-follow: completed row does not match sealed endpos",
                ));
            }
            let next = self.row.clone();
            self.write_row(&next, io)?;
            match self.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(proof)) => self.completed_snapshot(proof),
                _ => Err(refusal(
                    "target-follow: completed observation COMMIT outcome unknown",
                )),
            }
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    pub(crate) fn close(mut self, io: &IoControl) -> Result<()> {
        self.session.require_idle()?;
        self.session.verify(io)?;
        let slot = self.session.fence_slot;
        let result = self.session.connection()?.query(
            &format!("SELECT pg_catalog.pg_advisory_unlock({FENCE_CLASS}, {slot})"),
            io,
        )?;
        if result.rows() != 1 || result.columns()? != 1 || result.cell(0, 0)? != Some(b"t") {
            return Err(refusal(
                "target-follow: owned writer fence release was not confirmed",
            ));
        }
        self.session.require_idle()?;
        self.session.close();
        Ok(())
    }

    fn require_mutable(&self, io: &IoControl) -> Result<()> {
        if self.row.follow_completed {
            return Err(refusal("target-follow: completed authority is immutable"));
        }
        if let Some(control) = &self.control {
            control.validate_active(io)?;
        }
        Ok(())
    }

    fn validate_admission(&self) -> Result<SourceEndLsn> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| refusal("target-follow: bound native control is absent"))?;
        if control.target() != &self.provenance {
            return Err(refusal("target-follow: target provenance binding changed"));
        }
        admitted_follow(&self.row, control.follow(), control.plan())
    }

    fn validate_sealed(&self, request: &SealedEndpos, io: &IoControl, active: bool) -> Result<()> {
        let control = self
            .control
            .as_ref()
            .ok_or_else(|| refusal("target-follow: bound native control is absent"))?;
        if active {
            control.validate_active(io)?;
        } else {
            control.validate(io)?;
        }
        let expected = request.request();
        if !expected.is_sealed()
            || expected.stream() != control.follow().binding.stream()
            || expected.copy_generation() != control.follow().binding.copy_generation
            || expected.slot_nonce() != control.follow().binding.slot.intent.nonce
            || expected.target_nonce() != self.provenance.intent.nonce
            || control.read(io)?.as_ref() != Some(expected)
        {
            return Err(refusal(
                "target-follow: sealed request or admission changed",
            ));
        }
        self.validate_admission()?;
        Ok(())
    }

    fn follow_snapshot(&self, proof: DurableProof) -> Result<DurableFollowSnapshot> {
        self.validate_admission()?;
        if proof.evidence.stream != self.provenance.intent.binding.stream
            || proof.evidence.through != self.row.applied
            || proof.evidence.certified_lsn != self.row.certified_lsn
            || proof.evidence.barrier_epoch != self.row.barrier_epoch
            || proof.evidence.barrier_counter != self.row.barrier_counter
            || proof.evidence.target != self.provenance
        {
            return Err(refusal(
                "target-follow: positive proof differs from observed row",
            ));
        }
        Ok(DurableFollowSnapshot {
            stream: proof.evidence.stream,
            durable_replay_lsn: durable_start(&self.row)
                .ok_or_else(|| refusal("target-follow: anchored replay frontier is absent"))?,
            certified_lsn: self.row.certified_lsn.unwrap_or(SourceEndLsn(0)),
            checkpoint_id: format!(
                "{}:{}:{}",
                proof.evidence.stream.generation, self.row.barrier_epoch, self.row.barrier_counter
            ),
            follow_completed: self.row.follow_completed,
            target: self.provenance.clone(),
        })
    }

    fn completed_snapshot(&self, proof: DurableProof) -> Result<CompletedFollow> {
        if !self.row.follow_completed {
            return Err(refusal("target-follow: final completion is not recorded"));
        }
        let drain = self
            .row
            .drain
            .ok_or_else(|| refusal("target-follow: final drain is absent"))?;
        Ok(CompletedFollow {
            snapshot: self.follow_snapshot(proof)?,
            drain,
        })
    }

    pub(crate) fn has_follow_anchor(&self) -> bool {
        self.row.follow_binding.is_some() && self.row.anchor.is_some()
    }

    pub(crate) fn recover_follow(
        &mut self,
        binding: &FollowBinding,
        confirmed_flush_lsn: SourceEndLsn,
        io: &IoControl,
    ) -> Result<RecoveryStart> {
        self.require_mutable(io)?;
        let outcome = (|| {
            if self
                .row
                .follow_binding
                .as_ref()
                .is_none_or(|f| f.binding != *binding)
            {
                return Err(refusal(
                    "target-follow: recovery binding differs from logged authority",
                ));
            }
            let through = self.row.applied;
            self.barrier(through.as_ref(), self.row.barrier_epoch, io)?;
            let proven = self.row.clone();
            self.session.begin(io)?;
            let observed = self.read_row(io)?;
            self.session.rollback(io)?;
            if observed != proven {
                return Err(refusal(
                    "target-follow: recovery row changed after positive writing barrier",
                ));
            }
            recovery_start(binding.stream(), &proven, confirmed_flush_lsn)
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    pub(crate) fn certify(
        &mut self,
        candidate: SourceEndLsn,
        io: &IoControl,
    ) -> Result<DurableProof> {
        self.require_mutable(io)?;
        let outcome = (|| {
            if self.row.follow_binding.is_none()
                || self.row.anchor.is_none()
                || candidate.0 == 0
                || candidate < durable_start(&self.row).unwrap_or(SourceEndLsn(0))
            {
                return Err(refusal(
                    "target-follow: idle certification is below durable authority or lacks its binding",
                ));
            }
            self.session.begin(io)?;
            if self.read_row(io)? != self.row {
                return Err(refusal("target-follow: certification predecessor changed"));
            }
            let mut next = self.row.clone();
            next.certified_lsn = Some(candidate);
            let epoch = next.barrier_epoch;
            next = increment_barrier(next, epoch)?;
            self.write_row(&next, io)?;
            match self.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(proof)) => Ok(proof),
                _ => Err(refusal(
                    "target-follow: certification COMMIT outcome unknown",
                )),
            }
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    pub(crate) fn install_follow_anchor(
        &mut self,
        native: &NativeState,
        receipt: &crate::state::SourceSlotReceipt,
        io: &IoControl,
    ) -> Result<DurableProof> {
        let outcome = (|| {
            let expected = native
                .follow_control()?
                .ok_or_else(|| refusal("target-follow: native follow binding is absent"))?;
            let saved = native
                .checked_clone()?
                .ok_or_else(|| refusal("target-follow: native copy stages are absent"))?;
            if expected.binding.slot != *receipt
                || self.row.follow_binding.as_ref() != Some(&expected)
                || saved.phase != ClonePhase::Analyzing
                || self
                    .row
                    .clone
                    .as_ref()
                    .is_none_or(|control| !control.matches_state(&saved))
                || self.row.applied.is_some()
                || self.row.certified_lsn.is_some()
            {
                return Err(refusal(
                    "target-follow: copy anchor requires exact completed copy and post-data stages",
                ));
            }
            let anchor = SnapshotAnchor {
                stream: expected.binding.stream(),
                snapshot_token: receipt.snapshot_token.clone(),
                consistent_point: receipt.consistent_point,
                plan_nonce: receipt.intent.nonce,
            };
            if self
                .row
                .anchor
                .as_ref()
                .is_some_and(|existing| existing != &anchor)
            {
                return Err(refusal("target-follow: existing copy anchor differs"));
            }
            self.session.begin(io)?;
            if self.read_row(io)? != self.row {
                return Err(refusal("target-follow: anchor predecessor changed"));
            }
            let markers = metadata::read_markers(self.session.connection()?, &self.provenance, io)?;
            validate_markers(&markers, &saved)?;
            aggregate_markers(&markers, &saved)?;
            self.row
                .clone
                .as_ref()
                .ok_or_else(|| refusal("target-follow: clone control is absent"))?
                .require_initial_large_objects(&saved.plan)?;
            let mut next = self.row.clone();
            next.anchor = Some(anchor);
            let epoch = next.barrier_epoch;
            next = increment_barrier(next, epoch)?;
            self.write_row(&next, io)?;
            match self.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(proof)) => Ok(proof),
                _ => Err(refusal("target-follow: anchor COMMIT outcome unknown")),
            }
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }

    pub(crate) fn applied(&self) -> Option<Frontier> {
        self.row.applied
    }

    pub(crate) fn check_replay(&self, next: Frontier) -> Result<bool> {
        let duplicate = classify_replay(
            self.provenance.intent.binding.stream,
            self.row.applied,
            next,
        )?;
        if !duplicate && durable_start(&self.row).is_some_and(|start| next.end <= start) {
            return Err(refusal(
                "target-follow: source transaction does not advance recovery authority",
            ));
        }
        Ok(duplicate)
    }

    pub(crate) fn begin_group(&mut self, io: &IoControl) -> Result<TargetTxn<'_>> {
        self.require_mutable(io)?;
        if self.session.phase != Phase::Idle
            || self.session.connection()?.transaction_status()? != TransactionStatus::Idle
        {
            return Err(refusal(
                "target-authority: group requires an idle owned backend",
            ));
        }
        if self.row.follow_binding.is_none() || self.row.anchor.is_none() {
            return Err(refusal(
                "target-authority: follow group requires its checked binding and copy anchor",
            ));
        }
        self.session.phase = Phase::BeginPending;
        Ok(TargetTxn {
            authority: self,
            next: None,
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
        let mut session = Session::acquire(
            connection,
            self.session.synchronous_commit,
            self.session.fence_slot,
            io,
        )?;
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

    pub(crate) fn barrier(
        &mut self,
        through: Option<&Frontier>,
        epoch: u64,
        io: &IoControl,
    ) -> Result<DurableProof> {
        self.require_mutable(io)?;
        let outcome = (|| {
            self.session.begin(io)?;
            self.row = self.read_row(io)?;
            // Exact equality preserves the last descriptor identity even when source LSNs tie.
            if self.row.applied.as_ref() != through {
                return Err(refusal(
                    "target-authority: barrier frontier differs from the fenced checkpoint",
                ));
            }
            let next = increment_barrier(self.row.clone(), epoch)?;
            self.write_row(&next, io)?;
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

    fn write_row(&mut self, next: &ControlRow, io: &IoControl) -> Result<()> {
        if self.read_row(io)? != self.row {
            return Err(refusal(
                "target-authority: checkpoint changed outside its owner",
            ));
        }
        metadata::update(
            self.session.connection()?,
            &self.provenance,
            &self.row,
            next,
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
        self.finish_inner(next, mode, false, io)
    }

    fn finish_inner(
        &mut self,
        next: ControlRow,
        mode: TargetCommit,
        warm: bool,
        io: &IoControl,
    ) -> Result<CommitReceipt> {
        self.session.require_transaction()?;
        command(
            self.session.connection()?,
            "SET CONSTRAINTS ALL IMMEDIATE",
            "SET CONSTRAINTS",
            io,
        )?;
        let observed = if warm {
            metadata::read_prepared(self.session.connection()?, &self.provenance, io)?
        } else {
            self.read_row(io)?
        };
        if observed != next {
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
        let evidence = self.session.connection()?.commit_observed(io);
        match evidence {
            Ok(CommitObservation::Committed) => {
                self.session.phase = Phase::Idle;
                self.row = next;
                let proof = (mode == TargetCommit::Synchronous).then(|| DurableProof {
                    evidence: Box::new(ProofEvidence {
                        stream: self.provenance.intent.binding.stream,
                        through: self.row.applied,
                        barrier_epoch: self.row.barrier_epoch,
                        barrier_counter: self.row.barrier_counter,
                        target: self.provenance.clone(),
                        certified_lsn: self.row.certified_lsn,
                        follow: self.row.follow_binding.is_some(),
                    }),
                });
                Ok(CommitReceipt::Acknowledged(proof))
            }
            Ok(CommitObservation::RolledBack) => {
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
    fn ensure_transaction(&mut self, io: &IoControl) -> Result<()> {
        if self.authority.session.phase == Phase::BeginPending {
            self.authority.session.phase = Phase::Idle;
            self.authority.session.begin(io)?;
        }
        self.authority.session.require_transaction()
    }

    pub(crate) fn advance(&mut self, next: Frontier) -> Result<()> {
        self.require_healthy()?;
        self.failed = true;
        let previous = self.next.or(self.authority.row.applied);
        if self.next.is_none()
            && durable_start(&self.authority.row).is_some_and(|start| next.end <= start)
        {
            return Err(refusal(
                "target-follow: source transaction does not advance recovery authority",
            ));
        }
        validate_follow_next(
            self.authority.provenance.intent.binding.stream,
            previous,
            next,
        )?;
        self.next = Some(next);
        self.failed = false;
        Ok(())
    }

    pub(crate) fn pipeline(
        &mut self,
        io: &IoControl,
        send: impl FnOnce(&mut crate::pg::Pipeline<'_>) -> Result<()>,
        validate: impl FnMut(usize, PgResult) -> Result<()>,
    ) -> Result<()> {
        self.pipeline_window(io, send, validate, None)
    }

    fn pipeline_window(
        &mut self,
        io: &IoControl,
        send: impl FnOnce(&mut crate::pg::Pipeline<'_>) -> Result<()>,
        mut validate: impl FnMut(usize, PgResult) -> Result<()>,
        next: Option<&ControlRow>,
    ) -> Result<()> {
        self.require_healthy()?;
        self.failed = true;
        let begin = self.authority.session.phase == Phase::BeginPending;
        if !begin {
            self.authority.session.require_transaction()?;
        }
        let connection = self.authority.session.connection()?;
        let mut pipeline = if begin {
            connection.pipeline_begin(io)?
        } else {
            connection.pipeline(io)?
        };
        send(&mut pipeline)?;
        let checkpoint_index = next.map(|_| pipeline.commands());
        if let Some(next) = next {
            metadata::pipeline_update(
                &mut pipeline,
                &self.authority.provenance,
                &self.authority.row,
                next,
                io,
            )?;
        }
        pipeline.sync(io, |index, result| {
            if begin && index == 0 {
                if result.command_tag()? != "BEGIN" {
                    return Err(refusal(
                        "target-authority: pipeline did not begin its transaction",
                    ));
                }
                Ok(())
            } else if checkpoint_index == Some(index) {
                metadata::validate_update(&result)
            } else {
                validate(index - usize::from(begin), result)
            }
        })?;
        self.authority.session.phase = Phase::InTransaction;
        self.authority.session.require_transaction()?;
        self.failed = false;
        Ok(())
    }

    pub(crate) fn checkpoint_input_bytes(&self) -> Result<usize> {
        self.require_healthy()?;
        let frontier = self.next.ok_or_else(|| {
            refusal("target-authority: checkpoint budget requires a sealed transaction")
        })?;
        let mut next = self.authority.row.clone();
        next.applied = Some(frontier);
        let epoch = next.barrier_epoch;
        next = increment_barrier(next, epoch)?;
        metadata::checkpoint_input_bytes(&self.authority.provenance, &self.authority.row, &next)?
            .checked_add(if self.authority.session.phase == Phase::BeginPending {
                6
            } else {
                0
            })
            .ok_or_else(|| refusal("target-authority: checkpoint input size overflow"))
    }

    pub(crate) fn commit_pipeline(
        mut self,
        io: &IoControl,
        send: impl FnOnce(&mut crate::pg::Pipeline<'_>) -> Result<()>,
        validate: impl FnMut(usize, PgResult) -> Result<()>,
    ) -> Result<CommitOutcome> {
        self.require_healthy()?;
        let frontier = self
            .next
            .ok_or_else(|| refusal("target-authority: group has no sealed source transaction"))?;
        let mut next = self.authority.row.clone();
        next.applied = Some(frontier);
        let epoch = next.barrier_epoch;
        next = increment_barrier(next, epoch)?;
        self.pipeline_window(io, send, validate, Some(&next))?;
        self.failed = true;
        let result = self
            .authority
            .finish_inner(next, TargetCommit::Synchronous, true, io)?;
        self.finished = true;
        match result {
            CommitReceipt::Acknowledged(durable) => Ok(CommitOutcome::Acknowledged {
                checkpoint: self.authority.current_checkpoint().ok_or_else(|| {
                    refusal("target-authority: committed group frontier is absent")
                })?,
                durable,
            }),
            CommitReceipt::Unknown => Ok(CommitOutcome::Unknown),
        }
    }

    pub(crate) fn preflight_follow_relation(
        &mut self,
        expected: &Event,
        binding: &FollowBinding,
        owned: &OwnedCopyTable,
        io: &IoControl,
    ) -> Result<crate::catalog::FollowRelation> {
        self.require_healthy()?;
        self.failed = true;
        if self
            .authority
            .row
            .follow_binding
            .as_ref()
            .is_none_or(|follow| follow.binding != *binding)
        {
            return Err(refusal(
                "target-follow: relation preflight binding differs from logged authority",
            ));
        }
        self.ensure_transaction(io)?;
        self.authority.session.verify(io)?;
        let relation = crate::catalog::preflight_follow_relation(
            self.authority.session.connection()?,
            expected,
            binding,
            owned,
            io,
        )?;
        self.authority.session.require_transaction()?;
        self.failed = false;
        Ok(relation)
    }

    /// Run on the first touch of each relation in every owned transaction.
    #[cfg_attr(not(test), expect(dead_code, reason = "E3 serial consumer is pending"))]
    pub(crate) fn preflight_relation(&mut self, expected: &Event, io: &IoControl) -> Result<()> {
        self.require_healthy()?;
        // Keep a caught helper panic from leaving this transaction committable.
        self.failed = true;
        let outcome = (|| {
            self.ensure_transaction(io)?;
            self.authority.session.verify(io)?;
            crate::catalog::preflight_cdc_target(
                self.authority.session.connection()?,
                expected,
                io,
            )?;
            self.authority.session.require_transaction()
        })();
        self.failed = outcome.is_err();
        outcome
    }

    /// Only the internal serial SQL generator supplies statements; transaction control is forbidden.
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
                    Some("INSERT" | "UPDATE" | "DELETE" | "TRUNCATE" | "WITH")
                )
            {
                return Err(refusal(
                    "target-authority: invalid or excessive internal DML statement",
                ));
            }
            self.ensure_transaction(io)?;
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
    /// The 63-byte limit prevents SQL identifier truncation of protocol statement names.
    pub(crate) fn deallocate(&mut self, name: &str, io: &IoControl) -> Result<()> {
        let outcome = (|| {
            self.require_healthy()?;
            if name.is_empty()
                || name.contains('\0')
                || name.len() > 63
                || name.starts_with("rpgmigrate_")
                || !self.authority.prepared.contains(name)
            {
                return Err(refusal(
                    "target-authority: invalid or unowned DML statement",
                ));
            }
            self.ensure_transaction(io)?;
            self.authority.session.verify(io)?;
            let quoted = self
                .authority
                .session
                .connection()?
                .quote_identifier(name)?;
            command(
                self.authority.session.connection()?,
                &format!("DEALLOCATE {quoted}"),
                "DEALLOCATE",
                io,
            )?;
            self.authority.prepared.remove(name);
            Ok(())
        })();
        self.failed |= outcome.is_err();
        outcome
    }
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
            self.ensure_transaction(io)?;
            let result = self
                .authority
                .session
                .connection()?
                .execute_prepared(name, params, io)?;
            self.authority.session.require_transaction()?;
            Ok(result)
        })();
        self.failed |= outcome.is_err();
        outcome
    }
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "direct authority commits are retained for tests")
    )]
    pub(crate) fn commit(self, io: &IoControl) -> Result<CommitOutcome> {
        self.commit_with_mode(TargetCommit::Synchronous, io)
    }
    fn commit_with_mode(mut self, mode: TargetCommit, io: &IoControl) -> Result<CommitOutcome> {
        self.require_healthy()?;
        let Some(frontier) = self.next else {
            self.failed = true;
            return Err(refusal(
                "target-authority: CDC commit requires a transaction frontier",
            ));
        };
        let mut next = ControlRow {
            applied: Some(frontier),
            ..self.authority.row.clone()
        };
        if mode == TargetCommit::Synchronous {
            let epoch = next.barrier_epoch;
            next = increment_barrier(next, epoch)?;
        }
        self.ensure_transaction(io)?;
        self.authority.write_row(&next, io)?;
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
        connection: PgConnection,
        minimum: &'static str,
        slot: i32,
        io: &IoControl,
    ) -> Result<Self> {
        Self::acquire_mode(connection, minimum, slot, false, io)
    }

    fn acquire_mode(
        mut connection: PgConnection,
        minimum: &'static str,
        slot: i32,
        try_fence: bool,
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
            AND pg_catalog.current_setting('full_page_writes') = 'on'
            FROM pg_catalog.pg_database d, pg_catalog.pg_roles r
            WHERE d.datname = pg_catalog.current_database() AND r.rolname = CURRENT_USER", io)?;
        if result.rows() != 1 || result.cell(0, 3)? != Some(b"t") {
            return Err(refusal(
                "target-authority: writable primary with fsync and full_page_writes on is required",
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
        let function = if try_fence {
            "pg_try_advisory_lock"
        } else {
            "pg_advisory_lock"
        };
        let locked = connection.query(
            &format!("SELECT pg_catalog.{function}({FENCE_CLASS}, {slot})"),
            io,
        )?;
        if locked.rows() != 1 || locked.columns()? != 1 {
            return Err(refusal(
                "target-authority: writer fence returned an invalid result",
            ));
        }
        if try_fence {
            match locked.cell(0, 0)? {
                Some(b"t") => (),
                Some(b"f") => {
                    return Err(Error::new(
                        ErrorKind::BadState,
                        "target-follow: writer fence is held by an active owner",
                    ));
                }
                _ => {
                    return Err(refusal(
                        "target-follow: writer try-fence returned an invalid result",
                    ));
                }
            }
        }
        let slot_oid = slot as u32;
        connection.prepare("rpgmigrate_authority_guard", &format!(
            "SELECT pg_catalog.pg_backend_pid() = $1::integer
            AND (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()) = $2::oid
            AND (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER) = $3::oid
            AND NOT pg_catalog.pg_is_in_recovery()
            AND pg_catalog.current_setting('transaction_read_only') = 'off'
            AND pg_catalog.current_setting('fsync') = 'on'
            AND pg_catalog.current_setting('full_page_writes') = 'on'
            AND pg_catalog.current_setting('search_path') = 'pg_catalog'
            AND EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype = 'advisory'
                AND database = $2::oid AND pid = $1::integer AND classid = {FENCE_CLASS}::oid
                AND objid = {slot_oid}::oid AND objsubid = 2 AND mode = 'ExclusiveLock' AND granted)"
        ), &[23, 26, 26], io)?;
        let mut session = Self {
            connection: Some(connection),
            phase: Phase::Idle,
            pid,
            database,
            role,
            system,
            synchronous_commit,
            fence_slot: slot,
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
                "target-authority: backend, primary, durability settings, or writer fence changed",
            ));
        }
        Ok(())
    }

    fn require_idle(&mut self) -> Result<()> {
        if self.phase != Phase::Idle
            || self.connection()?.transaction_status()? != TransactionStatus::Idle
        {
            return Err(refusal("target-authority: transaction is not idle"));
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

#[cfg(test)]
#[derive(Debug, Eq, PartialEq)]
enum CommitEvidence {
    Committed,
    RolledBack,
    Unknown,
}

#[cfg(test)]
fn commit_evidence(tag: &str, status: TransactionStatus) -> CommitEvidence {
    match (tag, status) {
        ("COMMIT", TransactionStatus::Idle) => CommitEvidence::Committed,
        ("ROLLBACK", TransactionStatus::Idle) => CommitEvidence::RolledBack,
        _ => CommitEvidence::Unknown,
    }
}

fn validate_pending_row(row: &ControlRow) -> Result<()> {
    if row.applied.is_some() {
        return Err(refusal(
            "target-authority: unconfirmed provisioning contains applied data",
        ));
    }
    if row.anchor.is_some() {
        return Err(refusal(
            "target-authority: unconfirmed provisioning contains a snapshot anchor",
        ));
    }
    Ok(())
}

fn increment_barrier(row: ControlRow, epoch: u64) -> Result<ControlRow> {
    if row.follow_completed {
        return Err(refusal(
            "target-follow: completed authority token is immutable",
        ));
    }
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
        || next.commit.is_some_and(|commit| next.end.0 < commit.0)
        || previous.is_some_and(|value| {
            value.tx.stream != stream
                || next
                    .commit
                    .zip(value.commit)
                    .is_some_and(|(next, previous)| next < previous)
                || next.end < value.end
        })
    {
        return Err(refusal(
            "target-authority: noncontiguous or inconsistent source frontier",
        ));
    }
    Ok(())
}

pub(crate) fn read_endpos_floor(
    connection: &mut PgConnection,
    control: &ControlHandle,
    io: &IoControl,
) -> Result<SourceEndLsn> {
    control.validate(io)?;
    if connection.transaction_status()? != TransactionStatus::Idle {
        return Err(refusal(
            "target-follow: admission read requires an idle connection",
        ));
    }
    let identity = connection.query("SELECT oid, NOT pg_catalog.pg_is_in_recovery() AND pg_catalog.current_setting('transaction_read_only') = 'off' FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()", io)?;
    let binding = &control.target().intent.binding;
    let system = optional_system_identifier(connection, io)?;
    if identity.rows() != 1
        || number(&identity, 0)? != binding.target_database_oid
        || identity.cell(0, 1)? != Some(b"t")
        || binding
            .target_system_identifier
            .is_some_and(|expected| system != Some(expected))
    {
        return Err(refusal(
            "target-follow: admission endpoint identity changed",
        ));
    }
    let row = metadata::read_only(connection, control.target(), io)?;
    control.validate(io)?;
    admitted_follow(&row, control.follow(), control.plan())
}

fn admitted_follow(
    row: &ControlRow,
    follow: &crate::state::FollowControl,
    plan: &ClonePlan,
) -> Result<SourceEndLsn> {
    let binding = &follow.binding;
    let control = row
        .clone
        .as_ref()
        .ok_or_else(|| refusal("target-follow: cutover clone control is absent"))?;
    control.require_initial_large_objects(plan)?;
    let anchor = row
        .anchor
        .as_ref()
        .ok_or_else(|| refusal("target-follow: cutover requires a complete copy anchor"))?;
    if row.follow_binding.as_ref() != Some(follow)
        || control.generation != plan.generation
        || control.phase != ClonePhase::Analyzing
        || control.snapshot_token != binding.slot.snapshot_token
        || control.source_timeline != plan.source.timeline
        || control.table_count != plan.tables.len() as u64
        || row.follow_completed
            && !plan.copy_policy.skip_large_objects
            && control.large_objects.final_sync.is_none()
        || binding.copy_generation != plan.generation
        || binding.stream().source != plan.source.identity
        || binding.source_timeline != plan.source.timeline
        || anchor.stream != binding.stream()
        || anchor.snapshot_token != binding.slot.snapshot_token
        || anchor.consistent_point != binding.slot.consistent_point
        || anchor.plan_nonce != binding.slot.intent.nonce
        || anchor.consistent_point.0 == 0
    {
        return Err(refusal(
            "target-follow: cutover generation, binding, stage, or copy anchor differs",
        ));
    }
    Ok(row
        .applied
        .map(|f| f.end)
        .unwrap_or(SourceEndLsn(0))
        .max(anchor.consistent_point))
}

fn drained_row(row: &ControlRow, end: SourceEndLsn, witness: DrainWitness) -> Result<ControlRow> {
    if row.follow_completed {
        return Err(refusal("target-follow: completed authority is immutable"));
    }
    let record = drain_record(end, witness)?;
    let mut next = row.clone();
    // A sealed restart re-proves its original witness without minting new feedback.
    if let Some(existing) = next.drain {
        if existing.end != end {
            return Err(refusal("target-follow: retained sealed endpos differs"));
        }
        return Ok(next);
    }
    next.drain = Some(record);
    if record.kind == DrainKind::Keepalive {
        next.certified_lsn = Some(
            next.certified_lsn
                .unwrap_or(SourceEndLsn(0))
                .max(record.witness),
        );
    }
    increment_barrier(next, row.barrier_epoch)
}

fn drain_record(end: SourceEndLsn, witness: DrainWitness) -> Result<DrainRecord> {
    let (position, kind) = match witness {
        DrainWitness::Keepalive(position) => (position, DrainKind::Keepalive),
        DrainWitness::LaterBegin(position) => (SourceEndLsn(position.0), DrainKind::LaterBegin),
    };
    if end.0 == 0 || position < end || kind == DrainKind::LaterBegin && position <= end {
        return Err(refusal(
            "target-follow: drain witness does not cover the sealed endpos",
        ));
    }
    Ok(DrainRecord {
        end,
        witness: position,
        kind,
    })
}

fn durable_start(row: &ControlRow) -> Option<SourceEndLsn> {
    row.applied
        .map(|f| f.end)
        .into_iter()
        .chain(row.certified_lsn)
        .chain(row.anchor.as_ref().map(|a| a.consistent_point))
        .max()
}

fn recovery_start(
    stream: StreamId,
    row: &ControlRow,
    confirmed_flush_lsn: SourceEndLsn,
) -> Result<RecoveryStart> {
    if row
        .follow_binding
        .as_ref()
        .is_none_or(|f| f.binding.stream() != stream)
        || row.anchor.is_none()
    {
        return Err(refusal(
            "target-follow: recovery requires its exact follow binding and copy anchor",
        ));
    }
    let start_lsn = durable_start(row)
        .ok_or_else(|| refusal("target-follow: durable recovery position is absent"))?;
    if confirmed_flush_lsn > start_lsn {
        return Err(Error::new(
            ErrorKind::BadState,
            "follow-recovery-gap: source confirmed flush is ahead of durable target authority",
        ));
    }
    Ok(RecoveryStart {
        stream,
        applied: row.applied,
        certified_lsn: row.certified_lsn,
        anchor: row.anchor.clone(),
        start_lsn,
        confirmed_flush_lsn,
    })
}

fn classify_replay(stream: StreamId, previous: Option<Frontier>, next: Frontier) -> Result<bool> {
    if let Some(previous) = previous {
        if next.end == previous.end {
            if next == previous {
                return Ok(true);
            }
            return Err(refusal(
                "target-follow: equal end LSN has conflicting transaction identity",
            ));
        }
        if next.end < previous.end {
            return Err(refusal(
                "target-follow: source end precedes durable applied frontier",
            ));
        }
    }
    validate_follow_next(stream, previous, next)?;
    Ok(false)
}

fn validate_follow_next(
    stream: StreamId,
    previous: Option<Frontier>,
    next: Frontier,
) -> Result<()> {
    validate_next(stream, previous, next)?;
    if next
        .commit
        .is_none_or(|commit| commit.0 == 0 || next.tx.begin_ordinal != commit.0)
        || previous.is_some_and(|previous| next.end <= previous.end)
    {
        return Err(refusal(
            "target-authority: follow frontier lacks strict source commit identity",
        ));
    }
    Ok(())
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::Target, message.to_owned())
}

#[cfg(test)]
#[path = "checkpoint_live_tests.rs"]
mod live_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::contracts::{Column, Event, RelationKey};
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
            commit: Some(CommitLsn(commit)),
            end: SourceEndLsn(end),
        }
    }

    pub(super) fn disconnected_authority() -> (TargetAuthority, IoControl) {
        let authority = TargetAuthority {
            session: Session {
                connection: None,
                phase: Phase::InTransaction,
                pid: 1,
                database: 11,
                role: 1,
                system: Some(9),
                synchronous_commit: "on",
                fence_slot: 1,
            },
            provenance: ConfirmedProvenance {
                intent: crate::state::ProvisioningIntent {
                    binding: TargetBinding {
                        stream: frontier(1, 100, 120).tx.stream,
                        target_system_identifier: Some(9),
                        target_database_oid: 11,
                        schema_name: "_rpgmigrate_test".into(),
                        table_name: "checkpoint".into(),
                        owner_oid: 1,
                        metadata_version: TARGET_METADATA_VERSION,
                    },
                    nonce: [1; 16],
                },
                schema_oid: 16_384,
                table_oid: 16_385,
                marker_table_oid: 16_386,
                marker_index_oid: 16_387,
                large_object_table_oid: 16_388,
                large_object_index_oid: 16_389,
            },
            row: ControlRow {
                drain: None,
                follow_completed: false,
                applied: None,
                barrier_epoch: 0,
                barrier_counter: 0,
                anchor: None,
                clone: None,
                certified_lsn: None,
                follow_binding: None,
            },
            prepared: BTreeSet::from(["keep".into()]),
            control: None,
        };
        let io = IoControl {
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        (authority, io)
    }

    #[test]
    fn cdc_commit_rejects_copy_mode_before_sql() {
        let (mut authority, io) = disconnected_authority();
        let cached = authority.row.clone();
        let tx = TargetTxn {
            authority: &mut authority,
            next: None,
            failed: false,
            finished: false,
        };
        let error = tx
            .commit(&io)
            .expect_err("COPY mode reached CDC finalization");
        assert_eq!(
            error.message,
            "target-authority: CDC commit requires a transaction frontier"
        );
        assert!(matches!(authority.session.phase, Phase::Poisoned));
        assert_eq!(authority.row, cached);
    }

    #[test]
    fn preflight_local_failures_poison_and_prevent_commit() {
        let relation = Event::Relation {
            key: RelationKey {
                oid: 1,
                generation: 1,
            },
            schema: "public".into(),
            name: "preflight".into(),
            columns: vec![Column {
                name: "id".into(),
                type_oid: 23,
                type_mod: -1,
                key: true,
            }],
            replica_identity: b'd',
        };
        for (phase, expected) in [
            (Phase::Idle, "a valid owned transaction is required"),
            (Phase::InTransaction, "connection poisoned"),
        ] {
            let (mut authority, io) = disconnected_authority();
            authority.session.phase = phase;
            let cached = authority.row.clone();
            let mut tx = TargetTxn {
                authority: &mut authority,
                next: Some(frontier(1, 100, 120)),
                failed: false,
                finished: false,
            };
            let error = tx.preflight_relation(&relation, &io).unwrap_err();
            assert!(error.message.contains(expected), "{error:?}");
            assert!(tx.failed);
            let error = tx.commit(&io).unwrap_err();
            assert!(error.message.contains("poisoned by an earlier DML error"));
            assert_eq!(authority.row, cached);
            assert!(matches!(authority.session.phase, Phase::Poisoned));
        }
    }

    #[test]
    fn preflight_cannot_clear_an_existing_failure() {
        let (mut authority, io) = disconnected_authority();
        let mut tx = TargetTxn {
            authority: &mut authority,
            next: Some(frontier(1, 100, 120)),
            failed: true,
            finished: false,
        };
        let event = Event::Truncate {
            relations: Vec::new(),
            cascade: false,
            restart_identity: false,
        };
        let error = tx.preflight_relation(&event, &io).unwrap_err();
        assert!(error.message.contains("poisoned by an earlier DML error"));
        assert!(tx.failed);
        assert!(tx.commit(&io).is_err());
    }

    #[test]
    fn prepare_with_reaches_backend_checks_but_utility_tokens_do_not() {
        for (sql, expected) in [
            (
                "WITH old AS (DELETE FROM x RETURNING id) INSERT INTO x SELECT id FROM old",
                "connection poisoned",
            ),
            ("WITHIN x", "invalid or excessive internal DML statement"),
            (
                "with x AS (SELECT 1) SELECT * FROM x",
                "invalid or excessive internal DML statement",
            ),
            ("COMMIT", "invalid or excessive internal DML statement"),
        ] {
            let (mut authority, io) = disconnected_authority();
            let mut tx = TargetTxn {
                authority: &mut authority,
                next: Some(frontier(1, 100, 120)),
                failed: false,
                finished: false,
            };
            let error = tx.prepare("candidate", sql, &[], &io).unwrap_err();
            assert!(error.message.contains(expected), "{error:?}");
            assert!(tx.failed);
            assert!(!tx.authority.prepared.contains("candidate"));
            assert!(tx.commit(&io).is_err());
            assert!(authority.current_checkpoint().is_none());
        }
    }

    #[test]
    fn deallocate_rejects_invalid_or_unowned_names_and_prevents_commit() {
        for (name, owned) in [
            (String::new(), true),
            ("unknown".into(), false),
            ("ALL".into(), false),
            ("rpgmigrate_authority_guard".into(), true),
            ("rpgmigrate_metadata_read".into(), true),
            ("bad\0name".into(), true),
            ("x".repeat(64), true),
            ("é".repeat(32), true),
        ] {
            let (mut authority, io) = disconnected_authority();
            if owned {
                authority.prepared.insert(name.clone());
            }
            let prepared = authority.prepared.clone();
            let mut tx = TargetTxn {
                authority: &mut authority,
                next: Some(frontier(1, 100, 120)),
                failed: false,
                finished: false,
            };
            let error = tx.deallocate(&name, &io).unwrap_err();
            assert!(error.message.contains("invalid or unowned DML statement"));
            assert!(tx.failed);
            assert_eq!(tx.authority.prepared, prepared);
            let error = tx.deallocate("keep", &io).unwrap_err();
            assert!(error.message.contains("poisoned by an earlier DML error"));
            assert!(tx.commit(&io).is_err());
            assert!(authority.current_checkpoint().is_none());
            assert!(matches!(authority.session.phase, Phase::Poisoned));
        }
    }

    #[test]
    fn deallocate_backend_failure_retains_owned_names_and_prevents_commit() {
        for name in [
            "slot".to_owned(),
            "ALL".into(),
            "quoted\"; DEALLOCATE ALL; --".into(),
            "x".repeat(63),
            format!("{}a", "é".repeat(31)),
        ] {
            let (mut authority, io) = disconnected_authority();
            authority.prepared.insert(name.clone());
            let prepared = authority.prepared.clone();
            let mut tx = TargetTxn {
                authority: &mut authority,
                next: Some(frontier(1, 100, 120)),
                failed: false,
                finished: false,
            };
            let error = tx.deallocate(&name, &io).unwrap_err();
            assert!(error.message.contains("connection poisoned"));
            assert!(tx.failed);
            assert_eq!(tx.authority.prepared, prepared);
            assert!(tx.commit(&io).is_err());
            assert!(authority.current_checkpoint().is_none());
            assert!(matches!(authority.session.phase, Phase::Poisoned));
        }
    }

    #[test]
    fn exact_order_allows_equal_lsns_and_reused_xids_with_earlier_begin() {
        let first = frontier(1, 100, 120);
        assert!(validate_next(first.tx.stream, None, first).is_ok());
        let mut next = frontier(2, 100, 120);
        next.tx.begin_ordinal = 3;
        assert!(validate_next(first.tx.stream, Some(first), next).is_ok());
        for (previous, commit, accepted) in [
            (None, None, true),
            (Some(100), None, true),
            (None, Some(99), true),
            (Some(100), Some(99), false),
            (Some(100), Some(100), true),
            (Some(100), Some(101), true),
        ] {
            let first = Frontier {
                commit: previous.map(CommitLsn),
                ..first
            };
            let next = Frontier {
                commit: commit.map(CommitLsn),
                ..next
            };
            assert_eq!(
                validate_next(first.tx.stream, Some(first), next).is_ok(),
                accepted
            );
            assert!(
                validate_next(
                    first.tx.stream,
                    Some(first),
                    Frontier {
                        end: SourceEndLsn(119),
                        ..next
                    }
                )
                .is_err()
            );
        }
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
            drain: None,
            follow_completed: false,
            applied: None,
            barrier_epoch: 7,
            barrier_counter: u64::MAX,
            anchor: None,
            clone: None,
            certified_lsn: None,
            follow_binding: None,
        };
        assert!(increment_barrier(row.clone(), 7).is_err());
        assert!(
            increment_barrier(
                ControlRow {
                    barrier_counter: 0,
                    ..row.clone()
                },
                6
            )
            .is_err()
        );
        assert_eq!(
            increment_barrier(
                ControlRow {
                    barrier_counter: 0,
                    ..row.clone()
                },
                7
            )
            .unwrap(),
            ControlRow {
                drain: None,
                follow_completed: false,
                applied: None,
                barrier_epoch: 7,
                barrier_counter: 1,
                anchor: None,
                clone: None,
                certified_lsn: None,
                follow_binding: None,
            }
        );
    }

    #[test]
    fn pending_provisioning_refuses_a_snapshot_anchor() {
        let (authority, _) = disconnected_authority();
        let mut row = authority.row.clone();
        assert!(validate_pending_row(&row).is_ok());
        row.anchor = Some(crate::contracts::SnapshotAnchor {
            stream: authority.provenance.intent.binding.stream,
            snapshot_token: "owned snapshot token".into(),
            consistent_point: SourceEndLsn(1),
            plan_nonce: [1; 16],
        });
        assert_eq!(
            validate_pending_row(&row).unwrap_err().message,
            "target-authority: unconfirmed provisioning contains a snapshot anchor"
        );
    }

    #[test]
    fn barriers_preserve_the_snapshot_anchor() {
        let (authority, _) = disconnected_authority();
        let mut row = authority.row.clone();
        row.anchor = Some(crate::contracts::SnapshotAnchor {
            stream: authority.provenance.intent.binding.stream,
            snapshot_token: "owned snapshot token".into(),
            consistent_point: SourceEndLsn(1),
            plan_nonce: [1; 16],
        });
        let next = increment_barrier(row.clone(), row.barrier_epoch).unwrap();
        assert_eq!(next.anchor, row.anchor);
        assert_eq!(next.applied, row.applied);
        assert_eq!(next.barrier_counter, row.barrier_counter + 1);
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

use crate::contracts::{
    ClonePlan, CopyFormat, CopyGeneration, CopyPart, LargeObjectCompletion, LargeObjectControl,
    OwnedObject,
};
use crate::state::{CloneInspection, ClonePhase, CloneState};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloneControl {
    pub(crate) generation: CopyGeneration,
    pub(crate) phase: ClonePhase,
    pub(crate) snapshot_token: String,
    pub(crate) source_timeline: u32,
    pub(crate) table_count: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) prepared_generation: Option<CopyGeneration>,
    pub(crate) large_objects: LargeObjectControl,
}
impl CloneControl {
    pub(super) fn validate(&self) -> Result<()> {
        if self.generation.0 == 0
            || self.source_timeline == 0
            || self.table_count > 4096
            || self.snapshot_token.is_empty()
            || self.snapshot_token.len() > crate::contracts::MAX_SNAPSHOT_TOKEN_BYTES
            || self.snapshot_token.contains('\0')
            || self
                .prepared_generation
                .is_some_and(|generation| generation != self.generation)
            || self.large_objects.generation != self.generation
            || self.large_objects.final_sync.is_some() && self.large_objects.initial.is_none()
        {
            return Err(refusal("target-clone: invalid clone control identity"));
        }
        Ok(())
    }
    fn validate_for(&self, plan: &ClonePlan) -> Result<()> {
        self.validate()?;
        if self.generation != plan.generation
            || plan.copy_policy.skip_large_objects
                && (self.large_objects.initial.is_some() || self.large_objects.final_sync.is_some())
            || !plan.copy_policy.skip_large_objects
                && !matches!(
                    self.phase,
                    ClonePhase::Fresh | ClonePhase::PreDataPending | ClonePhase::Copying
                )
                && self.large_objects.initial.is_none()
        {
            return Err(refusal(
                "target-clone: large-object policy or generation differs",
            ));
        }
        Ok(())
    }
    fn validate_transition(&self, next: &Self) -> Result<()> {
        self.validate()?;
        next.validate()?;
        let same = self.generation == next.generation
            && self.snapshot_token == next.snapshot_token
            && self.source_timeline == next.source_timeline
            && self.table_count == next.table_count
            && self
                .prepared_generation
                .is_none_or(|prepared| next.prepared_generation == Some(prepared))
            && self
                .large_objects
                .initial
                .is_none_or(|initial| next.large_objects.initial == Some(initial))
            && self
                .large_objects
                .final_sync
                .is_none_or(|final_sync| next.large_objects.final_sync == Some(final_sync));
        let restart = self.generation.0.checked_add(1) == Some(next.generation.0)
            && self.phase == ClonePhase::Copying
            && next.phase == ClonePhase::Copying
            && self.source_timeline == next.source_timeline
            && self.table_count == next.table_count
            && next.prepared_generation.is_none()
            && next.large_objects.initial.is_none()
            && next.large_objects.final_sync.is_none();
        if !same && !restart {
            return Err(refusal(
                "target-clone: clone control transition loses durable authority",
            ));
        }
        Ok(())
    }
    fn matches_state(&self, state: &CloneState) -> bool {
        self.validate_for(&state.plan).is_ok()
            && self.phase == state.phase
            && self.snapshot_token == state.ticket.token
            && self.source_timeline == state.plan.source.timeline
            && self.table_count == state.plan.tables.len() as u64
    }
    fn require_initial_large_objects(&self, plan: &ClonePlan) -> Result<()> {
        self.validate_for(plan)?;
        if !plan.copy_policy.skip_large_objects && self.large_objects.initial.is_none() {
            return Err(refusal(
                "target-clone: initial large-object completion is absent",
            ));
        }
        Ok(())
    }
    fn from_state(state: &CloneState) -> Self {
        Self {
            generation: state.plan.generation,
            phase: state.phase,
            snapshot_token: state.ticket.token.clone(),
            source_timeline: state.plan.source.timeline,
            table_count: state.plan.tables.len() as u64,
            prepared_generation: None,
            large_objects: LargeObjectControl {
                generation: state.plan.generation,
                initial: None,
                final_sync: None,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CopyDone {
    pub(crate) source_oid: u32,
    pub(crate) target_oid: u32,
    pub(crate) generation: CopyGeneration,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
    pub(crate) target_namespace_oid: u32,
    pub(crate) target_owner_oid: u32,
    pub(crate) format: CopyFormat,
    pub(crate) part: CopyPart,
}
impl CopyDone {
    fn matches(&self, table: &OwnedCopyTable, format: CopyFormat, part: &CopyPart) -> bool {
        self.source_oid == table.table.source_oid
            && self.target_oid == table.target_oid
            && self.generation == table.generation
            && self.target_namespace_oid == table.target_namespace_oid
            && self.target_owner_oid == table.target_owner_oid
            && self.format == format
            && self.part == *part
    }
}

pub(crate) struct CloneProgress {
    pub(crate) generation: CopyGeneration,
    pub(crate) phase: ClonePhase,
    pub(crate) markers: Vec<CopyDone>,
    pub(crate) durable_complete: bool,
    pub(crate) streaming: bool,
}

pub(crate) struct CloneAuthority {
    authority: TargetAuthority,
    state: CloneState,
    issued: BTreeSet<(u32, u32)>,
    follow: Option<crate::state::FollowControl>,
}

pub(crate) struct CopyPermit {
    provenance: ConfirmedProvenance,
    table: OwnedCopyTable,
    format: CopyFormat,
    part: CopyPart,
    control: CloneControl,
    coordinator_pid: u32,
    fence_slot: i32,
}

pub(crate) enum CopyCommitOutcome {
    Durable,
    Unknown,
}
pub(crate) struct CopyTxn {
    connection: Option<PgConnection>,
    permit: CopyPermit,
    pid: u32,
    synchronous_commit: &'static str,
    finished: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LargeObjectPhase {
    Initial,
    Final,
}
impl LargeObjectPhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Final => "final",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LargeObjectDone {
    pub(crate) generation: CopyGeneration,
    pub(crate) object_oid: u32,
    pub(crate) target_owner: u32,
    pub(crate) target_acl: Option<String>,
    pub(crate) target_comment: Option<String>,
    pub(crate) source_metadata: String,
    pub(crate) logical_bytes: u64,
    pub(crate) content_hash: [u8; 32],
    pub(crate) sync_phase: LargeObjectPhase,
}
impl LargeObjectDone {
    pub(super) fn validate(&self) -> Result<()> {
        if self.generation.0 == 0
            || self.object_oid == 0
            || self.target_owner == 0
            || self.source_metadata.is_empty()
            || self.source_metadata.len() > 65536
            || self.source_metadata.contains('\0')
            || [&self.target_acl, &self.target_comment]
                .into_iter()
                .flatten()
                .any(|value| value.len() > 65536 || value.contains('\0'))
        {
            return Err(refusal(
                "target-large-object: invalid bounded receipt identity or metadata",
            ));
        }
        Ok(())
    }
}

pub(crate) struct LargeObjectPermit {
    provenance: ConfirmedProvenance,
    control: CloneControl,
    coordinator_pid: u32,
    fence_slot: i32,
    phase: LargeObjectPhase,
    drain: Option<DrainRecord>,
    anchor: Option<SnapshotAnchor>,
    follow: Option<crate::state::FollowControl>,
}

pub(crate) struct LargeObjectTxn {
    connection: Option<PgConnection>,
    permit: LargeObjectPermit,
    object_oid: u32,
    existing: Option<LargeObjectDone>,
    pid: u32,
    synchronous_commit: &'static str,
    finished: bool,
}

impl LargeObjectTxn {
    pub(crate) fn begin(
        mut connection: PgConnection,
        permit: &LargeObjectPermit,
        object_oid: u32,
        io: &IoControl,
    ) -> Result<Self> {
        if object_oid == 0 || connection.transaction_status()? != TransactionStatus::Idle {
            return Err(refusal(
                "target-large-object: nonzero OID and idle dedicated connection required",
            ));
        }
        let durability = configure_durability(&mut connection, io)?;
        command(&mut connection, "SET search_path=pg_catalog", "SET", io)?;
        let pid = number(
            &connection.query("SELECT pg_catalog.pg_backend_pid()", io)?,
            0,
        )?;
        let mut tx = Self {
            connection: Some(connection),
            permit: LargeObjectPermit {
                provenance: permit.provenance.clone(),
                control: permit.control.clone(),
                coordinator_pid: permit.coordinator_pid,
                fence_slot: permit.fence_slot,
                phase: permit.phase,
                drain: permit.drain,
                anchor: permit.anchor.clone(),
                follow: permit.follow.clone(),
            },
            object_oid,
            existing: None,
            pid,
            synchronous_commit: durability,
            finished: false,
        };
        tx.verify(io)?;
        command(tx.connection_mut()?, "BEGIN", "BEGIN", io)?;
        // The coordinator holds the run fence before any worker takes an object lock.
        let slot = object_oid as i32;
        let result = tx.connection_mut()?.query(
            &format!(
                "SELECT pg_catalog.pg_advisory_xact_lock({}, {slot})",
                FENCE_CLASS + 1
            ),
            io,
        )?;
        if result.rows() != 1 {
            return Err(refusal("target-large-object: object lock is absent"));
        }
        let provenance = tx.permit.provenance.clone();
        let row = metadata::read_only(tx.connection_mut()?, &provenance, io)?;
        tx.validate_control(&row)?;
        let existing =
            metadata::read_large_object(tx.connection_mut()?, &provenance, object_oid, true, io)?;
        if let Some(done) = existing.as_ref() {
            if done.generation != tx.permit.control.generation
                || tx.permit.phase == LargeObjectPhase::Initial
                    && done.sync_phase != LargeObjectPhase::Initial
            {
                return Err(refusal(
                    "target-large-object: existing receipt generation or phase differs",
                ));
            }
            crate::large_objects::validate_owned(tx.connection_mut()?, done, io)?;
        } else {
            let result = tx.connection_mut()?.query(&format!("SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_largeobject_metadata WHERE oid={object_oid}::pg_catalog.oid)"), io)?;
            if result.rows() != 1 || result.cell(0, 0)? != Some(b"t") {
                return Err(refusal(
                    "target-large-object: target OID already exists without an owned receipt",
                ));
            }
        }
        tx.existing = existing;
        Ok(tx)
    }
    pub(crate) fn object_oid(&self) -> u32 {
        self.object_oid
    }
    pub(crate) fn existing_receipt(&self) -> Option<&LargeObjectDone> {
        self.existing.as_ref()
    }
    pub(crate) fn connection_mut(&mut self) -> Result<&mut PgConnection> {
        self.connection
            .as_mut()
            .ok_or_else(|| refusal("target-large-object: transaction connection is closed"))
    }
    fn verify(&mut self, io: &IoControl) -> Result<()> {
        let p = &self.permit;
        let binding = &p.provenance.intent.binding;
        let sql = format!(
            "SELECT pg_catalog.pg_backend_pid()={} AND (SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())={}::oid AND NOT pg_catalog.pg_is_in_recovery() AND pg_catalog.current_setting('transaction_read_only')='off' AND pg_catalog.current_setting('fsync')='on' AND pg_catalog.current_setting('full_page_writes')='on' AND pg_catalog.current_setting('search_path')='pg_catalog' AND EXISTS(SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='advisory' AND database={}::oid AND pid={} AND classid={FENCE_CLASS}::oid AND objid={}::oid AND objsubid=2 AND mode='ExclusiveLock' AND granted)",
            self.pid,
            binding.target_database_oid,
            binding.target_database_oid,
            p.coordinator_pid,
            p.fence_slot as u32
        );
        let result = self.connection_mut()?.query(&sql, io)?;
        if result.rows() != 1 || result.cell(0, 0)? != Some(b"t") {
            return Err(refusal(
                "target-large-object: coordinator fence or physical backend changed",
            ));
        }
        Ok(())
    }
    fn validate_control(&self, row: &ControlRow) -> Result<()> {
        if row.clone.as_ref() != Some(&self.permit.control)
            || row.drain != self.permit.drain
            || row.anchor != self.permit.anchor
            || row.follow_binding != self.permit.follow
            || row.follow_completed
            || self.permit.phase == LargeObjectPhase::Initial && row.anchor.is_some()
            || self.permit.phase == LargeObjectPhase::Final
                && (row.anchor.is_none() || row.drain.is_none())
        {
            return Err(refusal(
                "target-large-object: generation, phase, or drain permit is stale",
            ));
        }
        Ok(())
    }
    pub(crate) fn mark_and_commit(
        mut self,
        measurement: crate::large_objects::LargeObjectMeasurement,
        io: &IoControl,
    ) -> Result<(LargeObjectDone, CopyCommitOutcome)> {
        self.verify(io)?;
        if self.connection_mut()?.transaction_status()? != TransactionStatus::InTransaction {
            return Err(refusal(
                "target-large-object: object write did not retain its transaction",
            ));
        }
        let done = LargeObjectDone {
            generation: self.permit.control.generation,
            object_oid: self.object_oid,
            target_owner: measurement.target_owner,
            target_acl: measurement.target_acl,
            target_comment: measurement.target_comment,
            source_metadata: measurement.source_metadata,
            logical_bytes: measurement.logical_bytes,
            content_hash: measurement.content_hash,
            sync_phase: self.permit.phase,
        };
        done.validate()?;
        crate::large_objects::validate_owned(self.connection_mut()?, &done, io)?;
        let provenance = self.permit.provenance.clone();
        metadata::prepare(self.connection_mut()?, &provenance, io)?;
        let row = metadata::lock_validate(self.connection_mut()?, &provenance, io)?;
        self.validate_control(&row)?;
        if let Some(previous) = self.existing.clone() {
            metadata::delete_large_object(self.connection_mut()?, &provenance, &previous, io)?;
        }
        metadata::insert_large_object(self.connection_mut()?, &provenance, &done, io)?;
        let minimum = self.synchronous_commit;
        confirm_durability(self.connection_mut()?, minimum, io)?;
        self.verify(io)?;
        let observed = self.connection_mut()?.commit_observed(io);
        self.finished = true;
        match observed {
            Ok(CommitObservation::Committed) => Ok((done, CopyCommitOutcome::Durable)),
            Ok(CommitObservation::RolledBack) => {
                Err(refusal("target-large-object: COMMIT returned ROLLBACK"))
            }
            _ => {
                self.connection.take();
                Ok((done, CopyCommitOutcome::Unknown))
            }
        }
    }
}
impl Drop for LargeObjectTxn {
    fn drop(&mut self) {
        if !self.finished {
            self.connection.take();
        }
    }
}

fn large_object_aggregate(
    authority: &mut TargetAuthority,
    generation: CopyGeneration,
    phase: LargeObjectPhase,
    io: &IoControl,
) -> Result<LargeObjectCompletion> {
    let mut total = LargeObjectCompletion {
        objects: 0,
        bytes: 0,
    };
    let mut after = 0;
    loop {
        let page = metadata::large_object_page(
            authority.session.connection()?,
            &authority.provenance,
            after,
            io,
        )?;
        if page.is_empty() {
            return Ok(total);
        }
        for done in page {
            if done.generation != generation || done.sync_phase != phase {
                return Err(refusal(
                    "target-large-object: aggregate has an incomplete phase or stale generation",
                ));
            }
            crate::large_objects::validate_owned(authority.session.connection()?, &done, io)?;
            total.objects = total
                .objects
                .checked_add(1)
                .ok_or_else(|| refusal("target-large-object: object counter exhausted"))?;
            total.bytes = total
                .bytes
                .checked_add(done.logical_bytes)
                .ok_or_else(|| refusal("target-large-object: byte counter exhausted"))?;
            after = done.object_oid;
        }
    }
}

fn unlink_owned_large_object(
    authority: &mut TargetAuthority,
    done: &LargeObjectDone,
    io: &IoControl,
) -> Result<()> {
    crate::large_objects::validate_owned(authority.session.connection()?, done, io)?;
    let result = authority.session.connection()?.query(
        &format!(
            "SELECT pg_catalog.lo_unlink({}::pg_catalog.oid)",
            done.object_oid
        ),
        io,
    )?;
    if result.rows() != 1 || result.cell(0, 0)? != Some(b"1") {
        return Err(refusal(
            "target-large-object: owned unlink was not confirmed",
        ));
    }
    metadata::delete_large_object(
        authority.session.connection()?,
        &authority.provenance,
        done,
        io,
    )
}

fn validate_worker_barrier(previous: &ControlRow, current: &ControlRow) -> Result<()> {
    let mut expected = previous.clone();
    expected.barrier_epoch = current.barrier_epoch;
    expected.barrier_counter = current.barrier_counter;
    if expected != *current {
        return Err(refusal(
            "target-clone: worker reconciliation authority changed while reacquiring fence",
        ));
    }
    Ok(())
}

impl TargetAuthority {
    fn worker_fence_barrier(&mut self, io: &IoControl) -> Result<()> {
        // Callers join every worker before releasing and reacquiring the coordinator fence.
        self.session.require_idle()?;
        let slot = self.session.fence_slot;
        let result = self.session.connection()?.query(
            &format!("SELECT pg_catalog.pg_advisory_unlock({FENCE_CLASS},{slot})"),
            io,
        )?;
        if result.rows() != 1 || result.cell(0, 0)? != Some(b"t") {
            self.session.close();
            return Err(refusal("target-clone: cannot release run fence"));
        }
        let result = self.session.connection()?.query(
            &format!("SELECT pg_catalog.pg_advisory_lock({FENCE_CLASS},{slot})"),
            io,
        )?;
        if result.rows() != 1 {
            self.session.close();
            return Err(refusal("target-clone: cannot reacquire run fence"));
        }
        self.session.verify(io)?;
        let previous = self.row.clone();
        self.barrier(previous.applied.as_ref(), previous.barrier_epoch, io)?;
        if let Err(error) = validate_worker_barrier(&previous, &self.row) {
            self.session.close();
            return Err(error);
        }
        Ok(())
    }
    fn object_permit(
        &mut self,
        phase: LargeObjectPhase,
        plan: &ClonePlan,
        io: &IoControl,
    ) -> Result<LargeObjectPermit> {
        self.session.require_idle()?;
        let control = self
            .row
            .clone
            .as_ref()
            .ok_or_else(|| refusal("target-large-object: clone control is absent"))?;
        control.validate_for(plan)?;
        if plan.copy_policy.skip_large_objects
            || self.row.follow_completed
            || phase == LargeObjectPhase::Initial
                && (control.phase != ClonePhase::Copying
                    || self.row.anchor.is_some()
                    || control.large_objects.initial.is_some())
            || phase == LargeObjectPhase::Final
                && (self.row.anchor.is_none()
                    || self.row.drain.is_none()
                    || control.large_objects.initial.is_none()
                    || control.large_objects.final_sync.is_some())
        {
            return Err(refusal(
                "target-large-object: phase does not permit object mutation",
            ));
        }
        if metadata::read_only(self.session.connection()?, &self.provenance, io)? != self.row {
            return Err(refusal(
                "target-large-object: permit admission predecessor changed",
            ));
        }
        metadata::admit_large_object_inventory(
            self.session.connection()?,
            &self.provenance,
            plan.generation,
            io,
        )?;
        Ok(LargeObjectPermit {
            provenance: self.provenance.clone(),
            control: control.clone(),
            coordinator_pid: self.session.pid,
            fence_slot: self.session.fence_slot,
            phase,
            drain: self.row.drain,
            anchor: self.row.anchor.clone(),
            follow: self.row.follow_binding.clone(),
        })
    }
    pub(crate) fn large_object_completion(
        &self,
        phase: LargeObjectPhase,
    ) -> Option<LargeObjectCompletion> {
        self.row.clone.as_ref().and_then(|control| match phase {
            LargeObjectPhase::Initial => control.large_objects.initial,
            LargeObjectPhase::Final => control.large_objects.final_sync,
        })
    }
    pub(crate) fn large_object_permit(
        &mut self,
        plan: &ClonePlan,
        request: &SealedEndpos,
        io: &IoControl,
    ) -> Result<LargeObjectPermit> {
        self.require_mutable(io)?;
        self.validate_sealed(request, io, true)?;
        if self
            .row
            .drain
            .is_none_or(|drain| drain.end != request.end())
        {
            return Err(refusal(
                "target-large-object: final synchronization lacks exact durable drain",
            ));
        }
        self.object_permit(LargeObjectPhase::Final, plan, io)
    }
    fn resolve_object_unknown(
        &mut self,
        generation: CopyGeneration,
        phase: LargeObjectPhase,
        oid: u32,
        io: &IoControl,
    ) -> Result<Option<LargeObjectDone>> {
        let outcome = (|| {
            if oid == 0 {
                return Err(refusal(
                    "target-large-object: unknown COMMIT has no object identity",
                ));
            }
            self.worker_fence_barrier(io)?;
            self.session.begin(io)?;
            if self.read_row(io)? != self.row {
                return Err(refusal(
                    "target-large-object: unknown COMMIT authority changed",
                ));
            }
            let done = metadata::read_large_object(
                self.session.connection()?,
                &self.provenance,
                oid,
                true,
                io,
            )?;
            if let Some(done) = done.as_ref() {
                if done.generation != generation || done.sync_phase != phase {
                    return Err(refusal(
                        "target-large-object: unknown COMMIT receipt differs",
                    ));
                }
                crate::large_objects::validate_owned(self.session.connection()?, done, io)?;
            }
            self.session.rollback(io)?;
            if done.is_none() {
                self.session.close();
            }
            Ok(done)
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }
    pub(crate) fn resolve_large_object_unknown(
        &mut self,
        plan: &ClonePlan,
        request: &SealedEndpos,
        oid: u32,
        io: &IoControl,
    ) -> Result<Option<LargeObjectDone>> {
        self.validate_sealed(request, io, true)?;
        self.resolve_object_unknown(plan.generation, LargeObjectPhase::Final, oid, io)
    }
    pub(crate) fn reconcile_large_objects(
        &mut self,
        source: &mut PgConnection,
        plan: &ClonePlan,
        request: &SealedEndpos,
        io: &IoControl,
    ) -> Result<()> {
        let outcome = (|| {
            self.require_mutable(io)?;
            self.validate_sealed(request, io, true)?;
            if plan.copy_policy.skip_large_objects
                || self
                    .row
                    .drain
                    .is_none_or(|drain| drain.end != request.end())
            {
                return Err(refusal(
                    "target-large-object: cleanup lacks selected final phase and exact drain",
                ));
            }
            if crate::catalog::read_clone_source(source, io)? != plan.source {
                return Err(refusal(
                    "target-large-object: final source identity differs",
                ));
            }
            let mut after = 0;
            loop {
                self.session.begin(io)?;
                if self.read_row(io)? != self.row {
                    return Err(refusal(
                        "target-large-object: final cleanup predecessor changed",
                    ));
                }
                let page = metadata::large_object_page(
                    self.session.connection()?,
                    &self.provenance,
                    after,
                    io,
                )?;
                if page.is_empty() {
                    self.session.rollback(io)?;
                    break;
                }
                for done in page {
                    if done.generation != plan.generation {
                        return Err(refusal(
                            "target-large-object: final cleanup receipt has stale generation",
                        ));
                    }
                    let result = source.query(&format!("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_largeobject_metadata WHERE oid={}::pg_catalog.oid)", done.object_oid), io)?;
                    if result.rows() != 1 {
                        return Err(refusal(
                            "target-large-object: source membership query is incomplete",
                        ));
                    }
                    match result.cell(0, 0)? {
                        Some(b"f") => unlink_owned_large_object(self, &done, io)?,
                        Some(b"t") => {}
                        _ => {
                            return Err(refusal(
                                "target-large-object: source membership query is invalid",
                            ));
                        }
                    }
                    after = done.object_oid;
                }
                self.validate_sealed(request, io, true)?;
                let epoch = self.row.barrier_epoch;
                let next = increment_barrier(self.row.clone(), epoch)?;
                self.write_row(&next, io)?;
                match self.finish(next, TargetCommit::Synchronous, io)? {
                    CommitReceipt::Acknowledged(Some(_)) => {}
                    _ => {
                        return Err(refusal(
                            "target-large-object: final cleanup COMMIT outcome unknown; existing-only reconciliation required",
                        ));
                    }
                }
            }
            Ok(())
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }
    pub(crate) fn finish_large_objects(
        &mut self,
        plan: &ClonePlan,
        request: &SealedEndpos,
        completion: LargeObjectCompletion,
        io: &IoControl,
    ) -> Result<()> {
        let outcome = (|| {
            self.require_mutable(io)?;
            self.validate_sealed(request, io, true)?;
            if self
                .row
                .drain
                .is_none_or(|drain| drain.end != request.end())
            {
                return Err(refusal(
                    "target-large-object: final receipt lacks exact drain",
                ));
            }
            self.finish_object_phase(plan, LargeObjectPhase::Final, completion, io)?;
            self.validate_sealed(request, io, true)
        })();
        if outcome.is_err() {
            self.session.close();
        }
        outcome
    }
    fn finish_object_phase(
        &mut self,
        plan: &ClonePlan,
        phase: LargeObjectPhase,
        completion: LargeObjectCompletion,
        io: &IoControl,
    ) -> Result<()> {
        if plan.copy_policy.skip_large_objects {
            return Err(refusal(
                "target-large-object: skipped policy cannot publish a copy receipt",
            ));
        }
        self.session.begin(io)?;
        if self.read_row(io)? != self.row {
            return Err(refusal(
                "target-large-object: phase completion predecessor changed",
            ));
        }
        let mut control = self
            .row
            .clone
            .clone()
            .ok_or_else(|| refusal("target-large-object: phase control is absent"))?;
        control.validate_for(plan)?;
        if phase == LargeObjectPhase::Initial
            && (control.phase != ClonePhase::Copying || self.row.anchor.is_some())
            || phase == LargeObjectPhase::Final
                && (control.phase != ClonePhase::Analyzing
                    || self.row.drain.is_none()
                    || control.large_objects.initial.is_none())
        {
            return Err(refusal(
                "target-large-object: aggregate completion is outside its admitted phase",
            ));
        }
        if large_object_aggregate(self, plan.generation, phase, io)? != completion {
            return Err(refusal(
                "target-large-object: phase totals differ from exact durable inventory",
            ));
        }
        let receipt = match phase {
            LargeObjectPhase::Initial => &mut control.large_objects.initial,
            LargeObjectPhase::Final => &mut control.large_objects.final_sync,
        };
        if receipt.is_some_and(|previous| previous != completion) {
            return Err(refusal(
                "target-large-object: durable phase completion cannot change",
            ));
        }
        *receipt = Some(completion);
        control.validate_for(plan)?;
        self.row
            .clone
            .as_ref()
            .ok_or_else(|| refusal("target-large-object: previous phase control is absent"))?
            .validate_transition(&control)?;
        let mut next = self.row.clone();
        next.clone = Some(control);
        let epoch = next.barrier_epoch;
        next = increment_barrier(next, epoch)?;
        self.write_row(&next, io)?;
        match self.finish(next, TargetCommit::Synchronous, io)? {
            CommitReceipt::Acknowledged(Some(_)) => Ok(()),
            _ => Err(refusal(
                "target-large-object: phase completion COMMIT outcome unknown",
            )),
        }
    }
    fn reset_large_objects(&mut self, generation: CopyGeneration, io: &IoControl) -> Result<()> {
        let mut after = 0;
        loop {
            let page = metadata::large_object_page(
                self.session.connection()?,
                &self.provenance,
                after,
                io,
            )?;
            if page.is_empty() {
                return Ok(());
            }
            for done in page {
                if done.generation != generation {
                    return Err(refusal(
                        "target-large-object: reset found foreign generation",
                    ));
                }
                unlink_owned_large_object(self, &done, io)?;
                after = done.object_oid;
            }
        }
    }
}

impl CloneAuthority {
    pub(crate) fn acquire(
        connection: PgConnection,
        state: &mut NativeState,
        plan: &ClonePlan,
        io: &IoControl,
    ) -> Result<Self> {
        if state.checked_follow_cleanup()?.is_some() {
            return Err(refusal(
                "target-clone: terminal cleanup forbids copy or restart",
            ));
        }
        let saved = state
            .checked_clone()?
            .ok_or_else(|| refusal("target-clone: native clone binding is absent"))?;
        if saved.plan != *plan {
            return Err(refusal(
                "target-clone: requested plan differs from native binding",
            ));
        }
        let stream = StreamId {
            run: state.identity().run,
            source: plan.source.identity,
            generation: 1,
        };
        let follow = state.follow_control()?;
        let authority =
            TargetAuthority::acquire_inner(connection, stream, state, io, fence_slot(stream.run))?;
        let restart = state.checked_follow_restart()?;
        let pending_binding = restart.as_ref().is_some_and(|r| {
            r.new_bound
                && authority
                    .row
                    .follow_binding
                    .as_ref()
                    .is_some_and(|f| f.binding == r.previous)
                && follow
                    .as_ref()
                    .is_some_and(|f| f.binding.copy_generation == r.next_generation)
        });
        if authority.row.follow_binding.is_some()
            && authority.row.follow_binding != follow
            && !pending_binding
        {
            return Err(refusal(
                "target-clone: follow binding differs from native copy state",
            ));
        }
        if authority.provenance.intent.binding.target_system_identifier
            != Some(plan.target.identity.system_identifier)
            || authority.provenance.intent.binding.target_database_oid
                != plan.target.identity.database_oid
        {
            return Err(refusal(
                "target-clone: target identity differs from clone plan",
            ));
        }
        if authority.row.applied.is_some()
            || authority.row.anchor.is_some()
            || authority.row.certified_lsn.is_some()
        {
            return Err(refusal(
                "target-clone: plain clone cannot adopt CDC checkpoint state",
            ));
        }
        let mut owner = Self {
            authority,
            state: saved,
            issued: BTreeSet::new(),
            follow,
        };
        if let Some(control) = owner.authority.row.clone.as_ref() {
            control.validate()?;
            let expected = CloneControl::from_state(&owner.state);
            if control.source_timeline != expected.source_timeline
                || control.table_count != expected.table_count
                || !(control.generation == expected.generation
                    && control.snapshot_token == expected.snapshot_token
                    || control.phase == ClonePhase::Copying
                        && expected.phase == ClonePhase::Copying
                        && control.generation.0.checked_add(1) == Some(expected.generation.0))
            {
                return Err(refusal(
                    "target-clone: generation or snapshot binding differs",
                ));
            }
        } else {
            if owner.state.phase != ClonePhase::Fresh {
                return Err(refusal(
                    "target-clone: missing target clone state cannot be adopted",
                ));
            }
            owner.publish(CloneControl::from_state(&owner.state), false, io)?;
        }
        if owner.authority.row.follow_binding != owner.follow && !pending_binding {
            owner.publish(CloneControl::from_state(&owner.state), false, io)?;
        }
        Ok(owner)
    }

    pub(crate) fn large_object_permit(&mut self, io: &IoControl) -> Result<LargeObjectPermit> {
        self.authority
            .object_permit(LargeObjectPhase::Initial, &self.state.plan, io)
    }
    pub(crate) fn large_object_completion(
        &self,
        phase: LargeObjectPhase,
    ) -> Option<LargeObjectCompletion> {
        self.authority.large_object_completion(phase)
    }
    pub(crate) fn resolve_large_object_unknown(
        &mut self,
        oid: u32,
        io: &IoControl,
    ) -> Result<Option<LargeObjectDone>> {
        self.authority.resolve_object_unknown(
            self.state.plan.generation,
            LargeObjectPhase::Initial,
            oid,
            io,
        )
    }
    pub(crate) fn finish_large_objects(
        &mut self,
        completion: LargeObjectCompletion,
        io: &IoControl,
    ) -> Result<()> {
        let outcome = self.authority.finish_object_phase(
            &self.state.plan,
            LargeObjectPhase::Initial,
            completion,
            io,
        );
        if outcome.is_err() {
            self.authority.session.close();
        }
        outcome
    }

    pub(crate) fn begin_follow_restart(
        &mut self,
        native: &mut NativeState,
        checked_plan: &ClonePlan,
        io: &IoControl,
    ) -> Result<crate::state::FollowRestart> {
        let outcome = (|| {
            let follow = self
                .follow
                .as_ref()
                .ok_or_else(|| {
                    refusal("target-follow: restart requires its native follow binding")
                })?
                .clone();
            if self.state.phase != ClonePhase::Copying
                || self.state.plan.source != checked_plan.source
                || self.state.plan.target != checked_plan.target
                || self.state.plan.tables != checked_plan.tables
                || self.state.plan.sequences != checked_plan.sequences
                || self.state.plan.policy != checked_plan.policy
                || self.state.plan.copy_policy != checked_plan.copy_policy
            {
                return Err(refusal(
                    "target-follow: restart source plan or owned phase differs",
                ));
            }
            self.authority.session.begin(io)?;
            self.authority.row = self.authority.read_row(io)?;
            let previous = native
                .checked_follow_restart()?
                .map_or(follow.binding.clone(), |r| r.previous);
            if self
                .authority
                .row
                .follow_binding
                .as_ref()
                .is_none_or(|f| f.binding != previous)
                || self.authority.row.anchor.is_some()
                || self.authority.row.applied.is_some()
                || self.authority.row.certified_lsn.is_some()
                || self
                    .authority
                    .row
                    .clone
                    .as_ref()
                    .is_none_or(|c| c.phase != ClonePhase::Copying)
            {
                return Err(refusal(
                    "target-follow: durable anchored or post-data work cannot restart COPY",
                ));
            }
            let rebound = crate::catalog::reconcile_clone_tables(
                self.authority.session.connection()?,
                &self.state.plan,
                &self.state.owned_objects,
                io,
            )?;
            if rebound != self.state.owned_tables {
                return Err(refusal(
                    "target-follow: exact target ownership changed before source replacement",
                ));
            }
            let epoch = self.authority.row.barrier_epoch;
            let next = increment_barrier(self.authority.row.clone(), epoch)?;
            self.authority.write_row(&next, io)?;
            match self.authority.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(_)) => {}
                _ => {
                    return Err(refusal(
                        "target-follow: restart writing barrier COMMIT outcome unknown",
                    ));
                }
            }
            native.begin_follow_restart(&previous, checked_plan)
        })();
        if outcome.is_err() {
            self.authority.session.close();
        }
        outcome
    }

    pub(crate) fn install_follow_anchor(
        &mut self,
        native: &NativeState,
        receipt: &crate::state::SourceSlotReceipt,
        io: &IoControl,
    ) -> Result<DurableProof> {
        self.authority.install_follow_anchor(native, receipt, io)
    }

    pub(crate) fn permit(&mut self, table: &OwnedCopyTable, part: &CopyPart) -> Result<CopyPermit> {
        let format = checked_copy_part(&self.state, table, part)?;
        let control = self
            .authority
            .row
            .clone
            .as_ref()
            .ok_or_else(|| refusal("target-clone: target clone control is absent"))?;
        if control.phase != ClonePhase::Copying
            || control.generation != table.generation
            || !self.state.owned_tables.contains(table)
            || part.count > 1 && control.prepared_generation != Some(table.generation)
            || self.authority.session.phase != Phase::Idle
            || !self.issued.insert((table.table.source_oid, part.number))
        {
            return Err(refusal(
                "target-clone: table permit is absent, stale, or already delegated",
            ));
        }
        Ok(CopyPermit {
            provenance: self.authority.provenance.clone(),
            table: table.clone(),
            format,
            part: *part,
            control: control.clone(),
            coordinator_pid: self.authority.session.pid,
            fence_slot: self.authority.session.fence_slot,
        })
    }

    pub(crate) fn mark_stage(&mut self, phase: ClonePhase, io: &IoControl) -> Result<()> {
        let mut control = self
            .authority
            .row
            .clone
            .clone()
            .ok_or_else(|| refusal("target-clone: target clone control is absent"))?;
        if !control.phase.permits(phase) || phase == ClonePhase::Complete {
            return Err(refusal("target-clone: unsupported clone stage transition"));
        }
        if phase == ClonePhase::PostDataPending {
            self.copy_report(io)?;
            control.require_initial_large_objects(&self.state.plan)?;
        }
        control.phase = phase;
        self.publish(control, false, io)
    }

    pub(crate) fn advance_generation(&mut self, state: &NativeState, io: &IoControl) -> Result<()> {
        let next = state
            .checked_clone()?
            .ok_or_else(|| refusal("target-clone: native clone state is absent"))?;
        let follow = state.follow_control()?;
        let restart = state.checked_follow_restart()?;
        let expected = CloneControl::from_state(&next);
        let old = self
            .authority
            .row
            .clone
            .as_ref()
            .ok_or_else(|| refusal("target-clone: target clone control is absent"))?;
        if self.issued.is_empty()
            && old.matches_state(&next)
            && self.authority.row.follow_binding == follow
        {
            self.authority
                .barrier(None, self.authority.row.barrier_epoch, io)?;
            if !metadata::read_markers(
                self.authority.session.connection()?,
                &self.authority.provenance,
                io,
            )?
            .is_empty()
                || !metadata::large_object_page(
                    self.authority.session.connection()?,
                    &self.authority.provenance,
                    0,
                    io,
                )?
                .is_empty()
            {
                return Err(refusal(
                    "target-clone: uncertain reset reconciliation found retained receipts",
                ));
            }
            self.state = next;
            self.follow = follow;
            self.issued.clear();
            return Ok(());
        }
        if next.phase != ClonePhase::Copying
            || old.phase != ClonePhase::Copying
            || old.generation.0.checked_add(1) != Some(next.plan.generation.0)
            || next.plan.source != self.state.plan.source
            || next.plan.target != self.state.plan.target
            || next.plan.tables != self.state.plan.tables
            || next.plan.sequences != self.state.plan.sequences
            || next.plan.policy != self.state.plan.policy
            || next.plan.copy_policy != self.state.plan.copy_policy
            || self.authority.row.anchor.is_some()
            || self.authority.row.applied.is_some()
            || self.authority.row.certified_lsn.is_some()
            || follow.is_some()
                && restart.as_ref().is_none_or(|r| {
                    !r.new_bound
                        || self
                            .authority
                            .row
                            .follow_binding
                            .as_ref()
                            .is_none_or(|f| f.binding != r.previous)
                        || follow
                            .as_ref()
                            .is_none_or(|f| f.binding.copy_generation != r.next_generation)
                })
        {
            return Err(refusal(
                "target-clone: recopy requires the next exact owned generation",
            ));
        }
        let rebound = crate::catalog::reconcile_clone_tables(
            self.authority.session.connection()?,
            &next.plan,
            &next.owned_objects,
            io,
        )?;
        if rebound != next.owned_tables {
            return Err(refusal(
                "target-follow: exact target ownership changed before generation advance",
            ));
        }
        self.follow = follow;
        self.publish(expected, true, io)?;
        self.state = next;
        self.issued.clear();
        Ok(())
    }

    pub(crate) fn prepare_split_tables(
        &mut self,
        native: &NativeState,
        io: &IoControl,
    ) -> Result<()> {
        let outcome = (|| {
            let saved = native
                .checked_clone()?
                .ok_or_else(|| refusal("target-clone: split preparation lacks native binding"))?;
            if saved != self.state || saved.phase != ClonePhase::Copying || !self.issued.is_empty()
            {
                return Err(refusal(
                    "target-clone: split preparation requires the exact workset before workers",
                ));
            }
            let tables = saved
                .owned_tables
                .iter()
                .filter(|table| {
                    saved.plan.copy_tables.iter().any(|profile| {
                        profile.source_oid == table.table.source_oid && profile.parts.len() > 1
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            if tables.is_empty() {
                return Ok(());
            }
            let control =
                self.authority.row.clone.as_ref().ok_or_else(|| {
                    refusal("target-clone: split preparation lacks target control")
                })?;
            if !control.matches_state(&saved) {
                return Err(refusal("target-clone: split preparation control differs"));
            }
            if control.prepared_generation == Some(saved.plan.generation) {
                self.authority
                    .barrier(None, self.authority.row.barrier_epoch, io)?;
                return Ok(());
            }
            self.authority.session.begin(io)?;
            let mut row = self.authority.read_row(io)?;
            if row != self.authority.row
                || row.anchor.is_some()
                || row.applied.is_some()
                || row.certified_lsn.is_some()
            {
                return Err(refusal(
                    "target-clone: split preparation authority changed or became anchored",
                ));
            }
            self.reset_copy_tables(&saved, &tables, io)?;
            metadata::clear_table_markers(
                self.authority.session.connection()?,
                &self.authority.provenance,
                &tables,
                io,
            )?;
            row.clone
                .as_mut()
                .ok_or_else(|| refusal("target-clone: split control disappeared"))?
                .prepared_generation = Some(saved.plan.generation);
            let epoch = row.barrier_epoch;
            let next = increment_barrier(row, epoch)?;
            self.authority.write_row(&next, io)?;
            match self.authority.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(_)) => Ok(()),
                _ => Err(refusal(
                    "target-clone: split preparation COMMIT outcome unknown; fresh fenced recovery required",
                )),
            }
        })();
        if outcome.is_err() {
            self.authority.session.close();
        }
        outcome
    }

    fn reset_copy_tables(
        &mut self,
        saved: &CloneState,
        tables: &[OwnedCopyTable],
        io: &IoControl,
    ) -> Result<()> {
        let mut tables = tables.to_vec();
        tables.sort_by(|a, b| {
            (&a.table.name.schema, &a.table.name.table)
                .cmp(&(&b.table.name.schema, &b.table.name.table))
        });
        let mut names = Vec::with_capacity(tables.len());
        for table in &tables {
            names.push(format!(
                "ONLY {}",
                crate::catalog::qualified(
                    self.authority.session.connection()?,
                    &table.table.name.schema,
                    &table.table.name.table
                )?
            ));
        }
        if !names.is_empty() {
            command(
                self.authority.session.connection()?,
                &format!("LOCK TABLE {} IN ACCESS EXCLUSIVE MODE", names.join(", ")),
                "LOCK TABLE",
                io,
            )?;
        }
        let rebound = crate::catalog::reconcile_clone_tables(
            self.authority.session.connection()?,
            &saved.plan,
            &saved.owned_objects,
            io,
        )?;
        if rebound != saved.owned_tables {
            return Err(refusal(
                "target-clone: exact owned workset changed before reset",
            ));
        }
        for table in &tables {
            validate_owned_copy_table(self.authority.session.connection()?, table, io)?;
        }
        if !names.is_empty() {
            command(
                self.authority.session.connection()?,
                &format!(
                    "TRUNCATE TABLE {} CONTINUE IDENTITY RESTRICT",
                    names.join(", ")
                ),
                "TRUNCATE TABLE",
                io,
            )?;
        }
        Ok(())
    }

    pub(crate) fn truncate_follow_workset(
        &mut self,
        native: &NativeState,
        io: &IoControl,
    ) -> Result<()> {
        let outcome = (|| {
            let saved = native
                .checked_clone()?
                .ok_or_else(|| refusal("target-follow: restart copy ownership is absent"))?;
            let follow = native
                .follow_control()?
                .ok_or_else(|| refusal("target-follow: restart binding is absent"))?;
            if native.checked_follow_restart()?.is_some()
                || saved != self.state
                || Some(&follow) != self.follow.as_ref()
                || saved.phase != ClonePhase::Copying
                || saved.plan.generation.0 <= 1
                || !self.issued.is_empty()
            {
                return Err(refusal(
                    "target-follow: complete workset reset requires finished restart before workers",
                ));
            }
            self.authority.session.begin(io)?;
            let row = self.authority.read_row(io)?;
            if row != self.authority.row
                || row.follow_binding.as_ref() != Some(&follow)
                || row
                    .clone
                    .as_ref()
                    .is_none_or(|control| !control.matches_state(&saved))
                || row.anchor.is_some()
                || row.applied.is_some()
                || row.certified_lsn.is_some()
            {
                return Err(refusal(
                    "target-follow: restart reset authority changed or became anchored",
                ));
            }
            if !metadata::read_markers(
                self.authority.session.connection()?,
                &self.authority.provenance,
                io,
            )?
            .is_empty()
            {
                return Err(refusal(
                    "target-follow: restart reset requires invalidated copy markers",
                ));
            }
            self.reset_copy_tables(&saved, &saved.owned_tables, io)?;
            let mut row = row;
            row.clone
                .as_mut()
                .ok_or_else(|| refusal("target-follow: restart clone control disappeared"))?
                .prepared_generation = Some(saved.plan.generation);
            let epoch = row.barrier_epoch;
            let next = increment_barrier(row, epoch)?;
            self.authority.write_row(&next, io)?;
            match self.authority.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(_)) => Ok(()),
                _ => Err(refusal(
                    "target-follow: complete workset reset COMMIT outcome unknown",
                )),
            }
        })();
        if outcome.is_err() {
            self.authority.session.close();
        }
        outcome
    }

    pub(crate) fn bind_tables(&mut self, state: &NativeState, io: &IoControl) -> Result<()> {
        let next = state
            .checked_clone()?
            .ok_or_else(|| refusal("target-clone: native clone state is absent"))?;
        let mut previous = self.state.plan.clone();
        if matches!(
            self.state.phase,
            ClonePhase::Fresh | ClonePhase::PreDataPending
        ) && self.issued.is_empty()
        {
            previous.copy_tables = next.plan.copy_tables.clone();
        }
        if next.plan != previous
            || next.ticket != self.state.ticket
            || next.phase != ClonePhase::Copying
        {
            return Err(refusal("target-clone: positive table handoff is absent"));
        }
        self.state = next;
        self.mark_stage(ClonePhase::Copying, io)
    }

    fn publish(&mut self, control: CloneControl, clear: bool, io: &IoControl) -> Result<()> {
        let outcome = (|| {
            self.authority.session.begin(io)?;
            self.authority.row = self.authority.read_row(io)?;
            if let Some(previous) = self.authority.row.clone.as_ref() {
                previous.validate_transition(&control)?;
            }
            control.validate()?;
            let mut next = self.authority.row.clone();
            next.clone = Some(control);
            next.follow_binding = self.follow.clone();
            let epoch = next.barrier_epoch;
            next = increment_barrier(next, epoch)?;
            if clear {
                let previous = self
                    .authority
                    .row
                    .clone
                    .as_ref()
                    .ok_or_else(|| {
                        refusal("target-clone: reset lacks previous generation control")
                    })?
                    .generation;
                validate_reset_markers(
                    &metadata::read_markers(
                        self.authority.session.connection()?,
                        &self.authority.provenance,
                        io,
                    )?,
                    &self.state,
                    previous,
                )?;
                self.authority.reset_large_objects(previous, io)?;
                metadata::clear_markers(
                    self.authority.session.connection()?,
                    &self.authority.provenance,
                    io,
                )?;
            }
            self.authority.write_row(&next, io)?;
            match self.authority.finish(next, TargetCommit::Synchronous, io)? {
                CommitReceipt::Acknowledged(Some(_)) => Ok(()),
                _ => Err(refusal(
                    "target-clone: stage COMMIT outcome unknown; fresh fenced recovery required",
                )),
            }
        })();
        if outcome.is_err() {
            self.authority.session.close();
        }
        outcome
    }

    fn checked_markers(&mut self, io: &IoControl) -> Result<Vec<CopyDone>> {
        let row = metadata::read_only(
            self.authority.session.connection()?,
            &self.authority.provenance,
            io,
        )?;
        if row.clone != self.authority.row.clone {
            return Err(refusal(
                "target-clone: clone control changed outside coordinator",
            ));
        }
        let markers = metadata::read_markers(
            self.authority.session.connection()?,
            &self.authority.provenance,
            io,
        )?;
        validate_markers(&markers, &self.state)?;
        Ok(markers)
    }

    pub(crate) fn copy_report(&mut self, io: &IoControl) -> Result<CopyReport> {
        aggregate_markers(&self.checked_markers(io)?, &self.state)
    }

    pub(crate) fn resolve_unknown(
        &mut self,
        table: &OwnedCopyTable,
        part: &CopyPart,
        io: &IoControl,
    ) -> Result<Option<CopyReport>> {
        let format = checked_copy_part(&self.state, table, part)?;
        if !self.state.owned_tables.contains(table) {
            return Err(refusal(
                "target-clone: unknown commit table lacks exact ownership",
            ));
        }
        self.authority.worker_fence_barrier(io)?;
        let marker = metadata::read_marker(
            self.authority.session.connection()?,
            &self.authority.provenance,
            table,
            part,
            io,
        )?;
        if let Some(marker) = marker.as_ref()
            && !marker.matches(table, format, part)
        {
            self.authority.session.close();
            return Err(refusal(
                "target-clone: exact part receipt differs from permit",
            ));
        }
        if marker.is_none() {
            self.authority.session.close();
        }
        Ok(marker.map(|m| CopyReport {
            tables: u64::from(part.count == 1),
            rows: m.rows,
            bytes: m.bytes,
        }))
    }

    pub(crate) fn record_complete(&mut self, report: &CopyReport, io: &IoControl) -> Result<()> {
        if self.copy_report(io)? != *report {
            return Err(refusal(
                "target-clone: completion counts differ from durable markers",
            ));
        }
        let mut control = self
            .authority
            .row
            .clone
            .clone()
            .ok_or_else(|| refusal("target-clone: target clone control is absent"))?;
        if control.phase != ClonePhase::Analyzing {
            return Err(refusal("target-clone: completion requires ANALYZE stage"));
        }
        control.require_initial_large_objects(&self.state.plan)?;
        control.phase = ClonePhase::Complete;
        self.publish(control, false, io)
    }

    pub(crate) fn durable_completed(&mut self, io: &IoControl) -> Result<bool> {
        if self
            .authority
            .row
            .clone
            .as_ref()
            .is_none_or(|c| c.phase != ClonePhase::Complete)
        {
            return Ok(false);
        }
        self.authority
            .row
            .clone
            .as_ref()
            .ok_or_else(|| refusal("target-clone: completed control disappeared"))?
            .require_initial_large_objects(&self.state.plan)?;
        self.copy_report(io)?;
        self.authority
            .barrier(None, self.authority.row.barrier_epoch, io)?;
        Ok(true)
    }
}

impl CopyTxn {
    pub(crate) fn begin(
        mut connection: PgConnection,
        permit: CopyPermit,
        io: &IoControl,
    ) -> Result<Self> {
        if connection.transaction_status()? != TransactionStatus::Idle {
            return Err(refusal("target-copy: idle dedicated connection required"));
        }
        let durability = configure_durability(&mut connection, io)?;
        command(&mut connection, "SET search_path=pg_catalog", "SET", io)?;
        let result = connection.query("SELECT pg_catalog.pg_backend_pid()", io)?;
        let pid = number(&result, 0)?;
        let mut tx = Self {
            connection: Some(connection),
            permit,
            pid,
            synchronous_commit: durability,
            finished: false,
        };
        tx.verify(io)?;
        command(tx.connection_mut()?, "BEGIN", "BEGIN", io)?;
        let provenance = tx.permit.provenance.clone();
        let saved = metadata::read_only(tx.connection_mut()?, &provenance, io)?;
        if saved.clone.as_ref() != Some(&tx.permit.control) {
            return Err(refusal(
                "target-copy: generation or snapshot permit is stale",
            ));
        }
        let name = tx.qualified()?;
        let split = tx.permit.part.count > 1;
        if split && tx.permit.control.prepared_generation != Some(tx.permit.table.generation) {
            return Err(refusal(
                "target-copy: split generation lacks durable preparation",
            ));
        }
        command(
            tx.connection_mut()?,
            &format!(
                "LOCK TABLE ONLY {name} IN {} MODE",
                if split {
                    "ROW EXCLUSIVE"
                } else {
                    "ACCESS EXCLUSIVE"
                }
            ),
            "LOCK TABLE",
            io,
        )?;
        tx.validate_table(io)?;
        if !split {
            command(
                tx.connection_mut()?,
                &format!("TRUNCATE ONLY {name}"),
                "TRUNCATE TABLE",
                io,
            )?;
        }
        Ok(tx)
    }
    pub(crate) fn connection_mut(&mut self) -> Result<&mut PgConnection> {
        self.connection
            .as_mut()
            .ok_or_else(|| refusal("target-copy: transaction connection is closed"))
    }
    fn qualified(&mut self) -> Result<String> {
        let name = self.permit.table.table.name.clone();
        Ok(format!(
            "{}.{}",
            self.connection_mut()?.quote_identifier(&name.schema)?,
            self.connection_mut()?.quote_identifier(&name.table)?
        ))
    }
    fn verify(&mut self, io: &IoControl) -> Result<()> {
        let p = &self.permit;
        let binding = &p.provenance.intent.binding;
        let sql = format!(
            "SELECT pg_catalog.pg_backend_pid()={} AND (SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())={}::oid AND NOT pg_catalog.pg_is_in_recovery() AND pg_catalog.current_setting('transaction_read_only')='off' AND pg_catalog.current_setting('fsync')='on' AND pg_catalog.current_setting('full_page_writes')='on' AND pg_catalog.current_setting('search_path')='pg_catalog' AND EXISTS(SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='advisory' AND database={}::oid AND pid={} AND classid={FENCE_CLASS}::oid AND objid={}::oid AND objsubid=2 AND mode='ExclusiveLock' AND granted)",
            self.pid,
            binding.target_database_oid,
            binding.target_database_oid,
            p.coordinator_pid,
            p.fence_slot as u32
        );
        let result = self.connection_mut()?.query(&sql, io)?;
        if result.rows() != 1 || result.cell(0, 0)? != Some(b"t") {
            return Err(refusal(
                "target-copy: coordinator fence or physical backend changed",
            ));
        }
        Ok(())
    }
    fn validate_table(&mut self, io: &IoControl) -> Result<()> {
        let table = self.permit.table.clone();
        validate_owned_copy_table(self.connection_mut()?, &table, io)
    }
    pub(crate) fn mark_and_commit(
        mut self,
        rows: u64,
        bytes: u64,
        io: &IoControl,
    ) -> Result<CopyCommitOutcome> {
        self.verify(io)?;
        self.validate_table(io)?;
        if self.connection_mut()?.transaction_status()? != TransactionStatus::InTransaction {
            return Err(refusal(
                "target-copy: COPY did not finish in an active transaction",
            ));
        }
        let provenance = self.permit.provenance.clone();
        metadata::prepare(self.connection_mut()?, &provenance, io)?;
        let row = metadata::lock_validate(self.connection_mut()?, &provenance, io)?;
        if row.clone.as_ref() != Some(&self.permit.control) {
            return Err(refusal(
                "target-copy: copy generation changed before marker",
            ));
        }
        let t = &self.permit.table;
        let marker = CopyDone {
            source_oid: t.table.source_oid,
            target_oid: t.target_oid,
            generation: t.generation,
            rows,
            bytes,
            target_namespace_oid: t.target_namespace_oid,
            target_owner_oid: t.target_owner_oid,
            format: self.permit.format,
            part: self.permit.part,
        };
        metadata::insert_marker(self.connection_mut()?, &provenance, &marker, io)?;
        let minimum = self.synchronous_commit;
        confirm_durability(self.connection_mut()?, minimum, io)?;
        self.verify(io)?;
        let result = self.connection_mut()?.commit_observed(io);
        self.finished = true;
        match result {
            Ok(CommitObservation::Committed) => Ok(CopyCommitOutcome::Durable),
            Ok(CommitObservation::RolledBack) => {
                Err(refusal("target-copy: COMMIT returned ROLLBACK"))
            }
            _ => {
                self.connection.take();
                Ok(CopyCommitOutcome::Unknown)
            }
        }
    }
}
impl Drop for CopyTxn {
    fn drop(&mut self) {
        if !self.finished {
            self.connection.take();
        }
    }
}

fn validate_owned_copy_table(
    connection: &mut PgConnection,
    table: &OwnedCopyTable,
    io: &IoControl,
) -> Result<()> {
    let qualified = crate::catalog::qualified(
        connection,
        &table.table.name.schema,
        &table.table.name.table,
    )?;
    let qualified = connection.quote_literal(&qualified)?;
    let schema = connection.quote_literal(&table.table.name.schema)?;
    let name = connection.quote_literal(&table.table.name.table)?;
    let sql = format!(
        "SELECT c.oid={}::oid AND c.relnamespace={}::oid AND c.relowner={}::oid AND n.nspname={schema}::pg_catalog.text AND c.relname={name}::pg_catalog.text AND c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger tr WHERE tr.tgrelid=c.oid AND NOT tr.tgisinternal) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=pg_catalog.to_regclass({qualified})::oid",
        table.target_oid, table.target_namespace_oid, table.target_owner_oid
    );
    let result = connection.query(&sql, io)?;
    if result.rows() != 1 || result.cell(0, 0)? != Some(b"t") {
        return Err(refusal(
            "target-copy: exact owned table identity or COPY profile changed",
        ));
    }
    Ok(())
}

fn checked_copy_part(
    state: &CloneState,
    table: &OwnedCopyTable,
    part: &CopyPart,
) -> Result<CopyFormat> {
    if table.generation != state.plan.generation || !state.owned_tables.contains(table) {
        return Err(refusal("target-clone: part lacks exact owned generation"));
    }
    state
        .plan
        .copy_tables
        .iter()
        .find(|profile| {
            profile.source_oid == table.table.source_oid && profile.parts.contains(part)
        })
        .map(|profile| profile.format)
        .ok_or_else(|| refusal("target-clone: part differs from bound execution profile"))
}

fn validate_reset_markers(
    markers: &[CopyDone],
    state: &CloneState,
    generation: CopyGeneration,
) -> Result<()> {
    for marker in markers {
        if !state.owned_tables.iter().any(|table| {
            let mut previous = table.clone();
            previous.generation = generation;
            marker.matches(&previous, marker.format, &marker.part)
        }) {
            return Err(refusal(
                "target-clone: reset marker lacks exact retained owned table identity",
            ));
        }
    }
    Ok(())
}

fn validate_markers(markers: &[CopyDone], state: &CloneState) -> Result<()> {
    let mut seen = BTreeSet::new();
    for marker in markers {
        let table = state
            .owned_tables
            .iter()
            .find(|table| table.table.source_oid == marker.source_oid)
            .ok_or_else(|| refusal("target-clone: marker has a foreign table identity"))?;
        let format = checked_copy_part(state, table, &marker.part)?;
        if !seen.insert((marker.source_oid, marker.part.number))
            || !marker.matches(table, format, &marker.part)
        {
            return Err(refusal(
                "target-clone: marker has a foreign part, format, generation, or object identity",
            ));
        }
    }
    Ok(())
}
pub(crate) fn completed_copy_tables(markers: &[CopyDone], state: &CloneState) -> Result<u64> {
    validate_markers(markers, state)?;
    if matches!(state.phase, ClonePhase::Fresh | ClonePhase::PreDataPending)
        && state.owned_tables.is_empty()
        && markers.is_empty()
    {
        return Ok(0);
    }
    if state.plan.copy_tables.len() != state.owned_tables.len()
        || state.plan.copy_tables.iter().any(|profile| {
            profile.parts.is_empty()
                || !state
                    .owned_tables
                    .iter()
                    .any(|table| table.table.source_oid == profile.source_oid)
        })
    {
        return Err(refusal(
            "target-clone: execution profile lacks exact owned table coverage",
        ));
    }
    let mut counts = std::collections::BTreeMap::<u32, usize>::new();
    for marker in markers {
        *counts.entry(marker.source_oid).or_default() += 1;
    }
    Ok(state
        .plan
        .copy_tables
        .iter()
        .filter(|profile| {
            counts.get(&profile.source_oid).copied().unwrap_or(0) == profile.parts.len()
        })
        .count() as u64)
}
fn aggregate_markers(markers: &[CopyDone], state: &CloneState) -> Result<CopyReport> {
    let tables = completed_copy_tables(markers, state)?;
    let expected = state
        .plan
        .copy_tables
        .iter()
        .try_fold(0usize, |count, profile| {
            count.checked_add(profile.parts.len())
        })
        .ok_or_else(|| refusal("target-clone: part counter exhausted"))?;
    if expected > 4096 || markers.len() != expected || tables != state.owned_tables.len() as u64 {
        return Err(refusal(
            "target-clone: completed part marker inventory is incomplete",
        ));
    }
    let mut report = CopyReport {
        tables,
        rows: 0,
        bytes: 0,
    };
    for marker in markers {
        report.rows = report
            .rows
            .checked_add(marker.rows)
            .ok_or_else(|| refusal("target-clone: row counter exhausted"))?;
        report.bytes = report
            .bytes
            .checked_add(marker.bytes)
            .ok_or_else(|| refusal("target-clone: byte counter exhausted"))?;
    }
    Ok(report)
}

pub(crate) fn read_clone_progress(
    connection: &mut PgConnection,
    inspection: &CloneInspection,
    io: &IoControl,
) -> Result<CloneProgress> {
    let row = metadata::read_only(connection, &inspection.provenance, io)?;
    if row.follow_binding != inspection.follow {
        return Err(refusal(
            "target-follow: inspected follow binding differs from native authority",
        ));
    }
    let streaming = row.anchor.is_some() && inspection.follow.is_some();
    let control = row
        .clone
        .ok_or_else(|| refusal("target-clone: target clone control is absent"))?;
    control.validate_for(&inspection.clone_state.plan)?;
    if !matches!(
        control.phase,
        ClonePhase::Fresh | ClonePhase::PreDataPending | ClonePhase::Copying
    ) {
        control.require_initial_large_objects(&inspection.clone_state.plan)?;
    }
    let expected = CloneControl::from_state(&inspection.clone_state);
    if control.generation != expected.generation
        || control.snapshot_token != expected.snapshot_token
        || control.source_timeline != expected.source_timeline
        || control.table_count != expected.table_count
    {
        return Err(refusal(
            "target-clone: inspected generation or snapshot differs",
        ));
    }
    let markers = metadata::read_markers(connection, &inspection.provenance, io)?;
    validate_markers(&markers, &inspection.clone_state)?;
    let complete = inspection.follow.is_none()
        && control.phase == ClonePhase::Complete
        && inspection.clone_state.phase == ClonePhase::Complete
        && inspection
            .clone_state
            .completion
            .as_ref()
            .is_some_and(|saved| {
                aggregate_markers(&markers, &inspection.clone_state)
                    .is_ok_and(|actual| actual == *saved)
            });
    Ok(CloneProgress {
        generation: control.generation,
        phase: control.phase,
        markers,
        durable_complete: complete,
        streaming,
    })
}

pub(crate) fn metadata_inventory(
    connection: &mut PgConnection,
    provenance: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<Vec<OwnedObject>> {
    metadata::read_only(connection, provenance, io)?;
    let owned = metadata::owned_relation_oids(connection, provenance, io)?;
    let all = crate::catalog::clone_object_inventory(connection, io)?;
    let inventory = all
        .into_iter()
        .filter(|o| {
            o.class_oid == 2615 && o.object_oid == provenance.schema_oid
                || o.namespace_oid == Some(provenance.schema_oid)
        })
        .collect::<Vec<_>>();
    if !owned.into_iter().all(|oid| {
        inventory
            .iter()
            .any(|o| o.class_oid == 1259 && o.object_oid == oid)
    }) {
        return Err(refusal("target-metadata: exact owned relations are absent"));
    }
    if !inventory
        .iter()
        .any(|o| o.class_oid == 2615 && o.object_oid == provenance.schema_oid)
    {
        return Err(refusal("target-metadata: exact owned namespace is absent"));
    }
    Ok(inventory)
}

#[cfg(test)]
fn single_part() -> CopyPart {
    CopyPart {
        number: 1,
        count: 1,
        start_block: 0,
        end_block: None,
    }
}
#[cfg(test)]
pub(super) fn test_copy_profile(
    tables: &[crate::contracts::CloneTable],
) -> Vec<crate::contracts::CloneTableCopy> {
    tables
        .iter()
        .map(|table| crate::contracts::CloneTableCopy {
            source_oid: table.source_oid,
            format: CopyFormat::Text,
            parts: vec![single_part()],
        })
        .collect()
}

#[cfg(test)]
mod clone_marker_tests {
    use super::*;
    use crate::contracts::{
        CloneSource, CloneTable, CloneTarget, QualifiedTable, RestorePolicy, SnapshotTicket,
    };
    use crate::identity::SourceId;
    fn saved() -> CloneState {
        let source = CloneSource {
            identity: SourceId {
                system_identifier: 1,
                database_oid: 2,
            },
            timeline: 1,
            major: 16,
        };
        let table = OwnedCopyTable {
            table: CloneTable {
                source_oid: 3,
                name: QualifiedTable {
                    schema: "public".into(),
                    table: "t".into(),
                },
                writable_columns: vec!["id".into()],
                all_columns: vec!["id".into()],
                identity_modes: vec![0],
                partition_identity_modes: None,
            },
            target_oid: 4,
            target_namespace_oid: 2200,
            target_owner_oid: 10,
            generation: CopyGeneration(1),
        };
        CloneState {
            plan: ClonePlan {
                source: source.clone(),
                target: CloneTarget {
                    identity: SourceId {
                        system_identifier: 5,
                        database_oid: 6,
                    },
                    major: 18,
                },
                generation: CopyGeneration(1),
                tables: vec![table.table.clone()],
                copy_policy: crate::contracts::CloneCopyPolicy::default(),
                copy_tables: test_copy_profile(std::slice::from_ref(&table.table)),
                sequences: vec![],
                policy: RestorePolicy::default(),
            },
            ticket: SnapshotTicket {
                token: "t".into(),
                source,
                generation: CopyGeneration(1),
            },
            phase: ClonePhase::Copying,
            owned_objects: vec![],
            owned_tables: vec![table],
            completion: None,
        }
    }
    fn done() -> CopyDone {
        CopyDone {
            source_oid: 3,
            target_oid: 4,
            generation: CopyGeneration(1),
            rows: 1,
            bytes: 2,
            target_namespace_oid: 2200,
            target_owner_oid: 10,
            format: CopyFormat::Text,
            part: single_part(),
        }
    }
    #[test]
    fn part_completion_requires_the_exact_full_profile_and_counts_physical_tables() {
        let mut state = saved();
        let parts = vec![
            CopyPart {
                number: 1,
                count: 2,
                start_block: 0,
                end_block: Some(8),
            },
            CopyPart {
                number: 2,
                count: 2,
                start_block: 8,
                end_block: None,
            },
        ];
        state.plan.copy_tables[0].parts = parts.clone();
        let mut first = done();
        first.part = parts[0];
        let mut last = done();
        last.part = parts[1];
        assert!(aggregate_markers(&[first.clone()], &state).is_err());
        assert_eq!(
            completed_copy_tables(std::slice::from_ref(&first), &state).unwrap(),
            0
        );
        assert_eq!(
            completed_copy_tables(&[first.clone(), last.clone()], &state).unwrap(),
            1
        );
        assert_eq!(
            aggregate_markers(&[first.clone(), last.clone()], &state).unwrap(),
            CopyReport {
                tables: 1,
                rows: 2,
                bytes: 4
            }
        );
        for field in 0..5 {
            let mut wrong = first.clone();
            match field {
                0 => wrong.format = CopyFormat::Binary,
                1 => wrong.part.number = 2,
                2 => wrong.part.count = 3,
                3 => wrong.part.start_block = 1,
                _ => wrong.part.end_block = Some(9),
            }
            assert!(validate_markers(&[wrong, last.clone()], &state).is_err());
        }
        assert!(aggregate_markers(&[first.clone(), first], &state).is_err());
    }

    #[test]
    fn clone_control_requires_current_preparation_and_monotonic_large_object_receipts() {
        let state = saved();
        let mut control = CloneControl::from_state(&state);
        control.prepared_generation = Some(CopyGeneration(2));
        assert!(control.validate().is_err());
        control = CloneControl::from_state(&state);
        control.large_objects.generation = CopyGeneration(2);
        assert!(control.validate().is_err());
        control = CloneControl::from_state(&state);
        control.large_objects.final_sync = Some(LargeObjectCompletion {
            objects: 0,
            bytes: 0,
        });
        assert!(control.validate().is_err());
        control.large_objects.initial = Some(LargeObjectCompletion {
            objects: 0,
            bytes: 0,
        });
        control.validate().unwrap();
        let mut skipped = state.plan.clone();
        skipped.copy_policy.skip_large_objects = true;
        assert!(control.validate_for(&skipped).is_err());
    }

    #[test]
    fn preparation_and_object_control_fields_are_required_and_empty_copy_is_explicit() {
        let state = saved();
        let mut control = CloneControl::from_state(&state);
        assert!(control.require_initial_large_objects(&state.plan).is_err());
        control.large_objects.initial = Some(LargeObjectCompletion {
            objects: 0,
            bytes: 0,
        });
        control.require_initial_large_objects(&state.plan).unwrap();
        for field in ["prepared_generation", "large_objects"] {
            let mut value = serde_json::to_value(&control).unwrap();
            value.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<CloneControl>(value).is_err());
        }
        let mut skipped = state.plan;
        skipped.copy_policy.skip_large_objects = true;
        assert!(control.require_initial_large_objects(&skipped).is_err());
        control.large_objects.initial = None;
        control.require_initial_large_objects(&skipped).unwrap();
    }

    #[test]
    fn every_post_copy_phase_requires_actual_initial_large_object_completion() {
        let state = saved();
        for phase in [
            ClonePhase::PostDataPending,
            ClonePhase::PostDataComplete,
            ClonePhase::SequencesPending,
            ClonePhase::SequencesComplete,
            ClonePhase::Analyzing,
            ClonePhase::Complete,
        ] {
            let mut control = CloneControl::from_state(&state);
            control.phase = phase;
            assert!(control.validate_for(&state.plan).is_err());
            control.large_objects.initial = Some(LargeObjectCompletion {
                objects: 0,
                bytes: 0,
            });
            control.validate_for(&state.plan).unwrap();
            let mut skipped = state.plan.clone();
            skipped.copy_policy.skip_large_objects = true;
            control.large_objects.initial = None;
            control.validate_for(&skipped).unwrap();
        }
    }

    #[test]
    fn large_object_receipts_refuse_invalid_ids_and_unbounded_metadata() {
        let done = LargeObjectDone {
            generation: CopyGeneration(1),
            object_oid: 2,
            target_owner: 3,
            target_acl: None,
            target_comment: None,
            source_metadata: "{}".into(),
            logical_bytes: 0,
            content_hash: [0; 32],
            sync_phase: LargeObjectPhase::Initial,
        };
        done.validate().unwrap();
        for field in 0..7 {
            let mut invalid = done.clone();
            match field {
                0 => invalid.generation.0 = 0,
                1 => invalid.object_oid = 0,
                2 => invalid.target_owner = 0,
                3 => invalid.source_metadata.clear(),
                4 => invalid.source_metadata = "x".repeat(65537),
                5 => invalid.target_acl = Some("x".repeat(65537)),
                _ => invalid.target_comment = Some("x\0y".into()),
            }
            assert!(invalid.validate().is_err());
        }
    }

    #[test]
    fn worker_reconciliation_preserves_every_binding_besides_the_sync_barrier() {
        let (authority, _) = tests::disconnected_authority();
        let mut previous = authority.row;
        previous.clone = Some(CloneControl::from_state(&saved()));
        let mut current = previous.clone();
        current.barrier_counter += 1;
        validate_worker_barrier(&previous, &current).unwrap();
        current.clone.as_mut().unwrap().generation.0 += 1;
        assert!(validate_worker_barrier(&previous, &current).is_err());
        current = previous.clone();
        current.follow_completed = true;
        assert!(validate_worker_barrier(&previous, &current).is_err());
        current = previous.clone();
        current.certified_lsn = Some(SourceEndLsn(1));
        assert!(validate_worker_barrier(&previous, &current).is_err());
    }

    #[test]
    fn same_generation_control_cannot_forget_preparation_or_completed_objects() {
        let mut old = CloneControl::from_state(&saved());
        old.prepared_generation = Some(old.generation);
        old.large_objects.initial = Some(LargeObjectCompletion {
            objects: 1,
            bytes: 9,
        });
        let mut next = old.clone();
        next.phase = ClonePhase::PostDataPending;
        old.validate_transition(&next).unwrap();
        next.prepared_generation = None;
        assert!(old.validate_transition(&next).is_err());
        next = old.clone();
        next.large_objects.initial = None;
        assert!(old.validate_transition(&next).is_err());
        next.large_objects.initial = Some(LargeObjectCompletion {
            objects: 2,
            bytes: 9,
        });
        assert!(old.validate_transition(&next).is_err());
        next = old.clone();
        next.generation = CopyGeneration(2);
        next.large_objects.generation = next.generation;
        assert!(old.validate_transition(&next).is_err());
        next.prepared_generation = None;
        next.large_objects.initial = None;
        old.validate_transition(&next).unwrap();
    }

    #[test]
    fn split_permits_require_preparation_and_deduplicate_each_exact_part() {
        let mut state = saved();
        let parts = vec![
            CopyPart {
                number: 1,
                count: 2,
                start_block: 0,
                end_block: Some(8),
            },
            CopyPart {
                number: 2,
                count: 2,
                start_block: 8,
                end_block: None,
            },
        ];
        state.plan.copy_tables[0].parts = parts.clone();
        let table = state.owned_tables[0].clone();
        let (mut authority, _) = tests::disconnected_authority();
        authority.session.phase = Phase::Idle;
        authority.row.clone = Some(CloneControl::from_state(&state));
        let mut owner = CloneAuthority {
            authority,
            state,
            issued: BTreeSet::new(),
            follow: None,
        };
        assert!(owner.permit(&table, &parts[0]).is_err());
        assert!(owner.issued.is_empty());
        owner
            .authority
            .row
            .clone
            .as_mut()
            .unwrap()
            .prepared_generation = Some(table.generation);
        let first = owner.permit(&table, &parts[0]).unwrap();
        assert_eq!(first.part, parts[0]);
        assert!(owner.permit(&table, &parts[0]).is_err());
        owner.permit(&table, &parts[1]).unwrap();
        let mut wrong = parts[1];
        wrong.start_block += 1;
        assert!(owner.permit(&table, &wrong).is_err());
        let mut stale = table;
        stale.generation.0 += 1;
        assert!(owner.permit(&stale, &parts[1]).is_err());
    }
    #[test]
    fn markers_reject_other_tables_oids_owners_generations_and_duplicate_completion() {
        let state = saved();
        validate_markers(&[done()], &state).unwrap();
        for field in 0..5 {
            let mut bad = done();
            match field {
                0 => bad.source_oid += 1,
                1 => bad.target_oid += 1,
                2 => bad.generation.0 += 1,
                3 => bad.target_namespace_oid += 1,
                _ => bad.target_owner_oid += 1,
            };
            assert!(validate_markers(&[bad], &state).is_err());
        }
        assert!(validate_markers(&[done(), done()], &state).is_err());
        assert!(aggregate_markers(&[], &state).is_err());
    }
    #[test]
    fn marker_totals_and_generation_bounds_never_wrap() {
        let mut state = saved();
        state.plan.copy_tables[0].parts = vec![
            CopyPart {
                number: 1,
                count: 2,
                start_block: 0,
                end_block: Some(8),
            },
            CopyPart {
                number: 2,
                count: 2,
                start_block: 8,
                end_block: None,
            },
        ];
        let mut first = done();
        first.part = state.plan.copy_tables[0].parts[0];
        let mut last = done();
        last.part = state.plan.copy_tables[0].parts[1];
        first.rows = u64::MAX;
        assert_eq!(
            aggregate_markers(&[first.clone(), last.clone()], &state)
                .unwrap_err()
                .message,
            "target-clone: row counter exhausted"
        );
        first.rows = 0;
        first.bytes = u64::MAX;
        assert_eq!(
            aggregate_markers(&[first, last], &state)
                .unwrap_err()
                .message,
            "target-clone: byte counter exhausted"
        );
        let mut control = CloneControl::from_state(&saved());
        control.generation = CopyGeneration(0);
        assert!(control.validate().is_err());
        control = CloneControl::from_state(&saved());
        control.table_count = 4097;
        assert!(control.validate().is_err());
        control = CloneControl::from_state(&saved());
        control.snapshot_token = "x".repeat(1025);
        assert!(control.validate().is_err());
    }

    #[test]
    fn absent_or_empty_execution_profiles_cannot_complete_a_physical_table() {
        let mut state = saved();
        state.plan.copy_tables.clear();
        assert!(aggregate_markers(&[], &state).is_err());
        state = saved();
        state.plan.copy_tables[0].parts.clear();
        assert!(completed_copy_tables(&[], &state).is_err());
        state = saved();
        state.phase = ClonePhase::PreDataPending;
        state.owned_tables.clear();
        assert_eq!(completed_copy_tables(&[], &state).unwrap(), 0);
    }

    #[test]
    fn reset_markers_require_old_generation_and_retained_exact_object_identity() {
        let mut next = saved();
        next.plan.generation = CopyGeneration(2);
        next.owned_tables[0].generation = CopyGeneration(2);
        validate_reset_markers(&[done()], &next, CopyGeneration(1)).unwrap();
        for field in 0..5 {
            let mut invalid = done();
            match field {
                0 => invalid.generation.0 = 2,
                1 => invalid.source_oid += 1,
                2 => invalid.target_oid += 1,
                3 => invalid.target_owner_oid += 1,
                _ => invalid.target_namespace_oid += 1,
            }
            assert!(validate_reset_markers(&[invalid], &next, CopyGeneration(1)).is_err());
        }
    }
    #[test]
    fn worker_permits_are_send_without_containing_a_live_connection() {
        fn assert_send<T: Send>() {}
        assert_send::<CopyPermit>();
    }

    pub(super) fn cancel_sync_replication_wait(
        target: &crate::contracts::Conninfo,
        pid: u32,
        io: &IoControl,
    ) -> std::thread::JoinHandle<()> {
        let target = target.clone();
        let io = io.clone();
        std::thread::spawn(move || {
            let mut observer = PgConnection::connect(&target, &io).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let result = observer.query(&format!("SELECT wait_event='SyncRep' AND query='COMMIT' FROM pg_catalog.pg_stat_activity WHERE pid={pid}"), &io).unwrap();
                if result.rows() == 1 && result.cell(0, 0).unwrap() == Some(b"t".as_slice()) {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "COMMIT did not enter the required synchronous replication wait"
                );
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            assert_eq!(
                observer
                    .query(&format!("SELECT pg_catalog.pg_cancel_backend({pid})"), &io)
                    .unwrap()
                    .cell(0, 0)
                    .unwrap(),
                Some(b"t".as_slice())
            );
        })
    }

    pub(super) fn set_missing_sync_standby(
        target: &mut PgConnection,
        missing: bool,
        io: &IoControl,
    ) {
        let value = if missing {
            "FIRST 1 (rpgmigrate_absent)"
        } else {
            ""
        };
        let quoted = target.quote_literal(value).unwrap();
        target
            .query(
                &format!("ALTER SYSTEM SET synchronous_standby_names={quoted}"),
                io,
            )
            .unwrap();
        assert_eq!(
            target
                .query("SELECT pg_catalog.pg_reload_conf()", io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"t".as_slice())
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if target
                .query("SHOW synchronous_standby_names", io)
                .unwrap()
                .cell(0, 0)
                .unwrap()
                == Some(value.as_bytes())
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "owned synchronous standby configuration was not reloaded"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn sync_replication_uncertainty(
        authority: &mut CloneAuthority,
        state: &mut NativeState,
        target: &mut PgConnection,
        target_info: &crate::contracts::Conninfo,
        io: &IoControl,
    ) -> (bool, bool) {
        assert_eq!(
            target
                .query("SHOW synchronous_standby_names", io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"".as_slice())
        );
        let clone = state.checked_clone().unwrap().unwrap();
        let next = crate::contracts::SnapshotTicket {
            token: "sync-replication-warning-snapshot".into(),
            source: clone.plan.source.clone(),
            generation: CopyGeneration(clone.plan.generation.0 + 1),
        };
        let mut next_plan = state.checked_clone().unwrap().unwrap().plan;
        next_plan.generation = next.generation;
        state.advance_copy_generation(&next_plan, &next).unwrap();
        authority.advance_generation(state, io).unwrap();
        authority.authority.session.close();
        let plan = state.checked_clone().unwrap().unwrap().plan;
        let mut connection = PgConnection::connect(target_info, io).unwrap();
        connection
            .query("SET synchronous_commit=remote_apply", io)
            .unwrap();
        connection
            .query("SET client_min_messages=error", io)
            .unwrap();
        *authority = CloneAuthority::acquire(connection, state, &plan, io).unwrap();
        assert_eq!(
            authority.authority.session.synchronous_commit,
            "remote_apply"
        );
        let table = state.owned_copy_tables().unwrap().remove(0);
        let mut connection = PgConnection::connect(target_info, io).unwrap();
        connection
            .query("SET synchronous_commit=remote_apply", io)
            .unwrap();
        connection
            .query("SET client_min_messages=error", io)
            .unwrap();
        let mut tx = CopyTxn::begin(
            connection,
            authority.permit(&table, &single_part()).unwrap(),
            io,
        )
        .unwrap();
        assert_eq!(tx.synchronous_commit, "remote_apply");
        let name = tx.qualified().unwrap();
        tx.connection_mut()
            .unwrap()
            .start_copy_in(
                &format!("COPY {name} FROM STDIN WITH(FORMAT text,FREEZE true)"),
                io,
            )
            .unwrap();
        tx.connection_mut().unwrap().write_copy(b"5\n", io).unwrap();
        assert_eq!(tx.connection_mut().unwrap().finish_copy_in(io).unwrap(), 1);
        set_missing_sync_standby(target, true, io);
        let canceller = cancel_sync_replication_wait(target_info, tx.pid, io);
        let worker_unknown = matches!(
            tx.mark_and_commit(1, 2, io).unwrap(),
            CopyCommitOutcome::Unknown
        );
        canceller.join().unwrap();
        assert!(state.checked_clone().unwrap().unwrap().completion.is_none());
        let canceller =
            cancel_sync_replication_wait(target_info, authority.authority.session.pid, io);
        let barrier_refused = authority
            .resolve_unknown(&table, &single_part(), io)
            .is_err();
        canceller.join().unwrap();
        set_missing_sync_standby(target, false, io);
        authority.authority.session.close();
        let mut connection = PgConnection::connect(target_info, io).unwrap();
        connection
            .query("SET synchronous_commit=remote_apply", io)
            .unwrap();
        connection
            .query("SET client_min_messages=error", io)
            .unwrap();
        *authority = CloneAuthority::acquire(connection, state, &plan, io).unwrap();
        assert_eq!(
            authority.authority.session.synchronous_commit,
            "remote_apply"
        );
        assert_eq!(
            authority
                .resolve_unknown(&table, &single_part(), io)
                .unwrap(),
            Some(CopyReport {
                tables: 1,
                rows: 1,
                bytes: 2
            })
        );
        assert!(state.checked_clone().unwrap().unwrap().completion.is_none());
        assert_eq!(
            target
                .query(&format!("SELECT id FROM {name}"), io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"5".as_slice())
        );
        (worker_unknown, barrier_refused)
    }

    fn delayed_marker_commit(
        authority: &mut CloneAuthority,
        table: &OwnedCopyTable,
        target: &crate::contracts::Conninfo,
        io: &IoControl,
    ) -> bool {
        let permit = authority.permit(table, &single_part()).unwrap();
        let target = target.clone();
        let io_worker = io.clone();
        let coordinator_pid = authority.authority.session.pid;
        let (ready, started) = std::sync::mpsc::sync_channel(0);
        let handle = std::thread::spawn(move || {
            let mut tx = CopyTxn::begin(
                PgConnection::connect(&target, &io_worker).unwrap(),
                permit,
                &io_worker,
            )
            .unwrap();
            let table = tx.permit.table.clone();
            let name = tx.qualified().unwrap();
            tx.connection_mut()
                .unwrap()
                .start_copy_in(
                    &format!("COPY {name} FROM STDIN WITH(FORMAT text,FREEZE true)"),
                    &io_worker,
                )
                .unwrap();
            tx.connection_mut()
                .unwrap()
                .write_copy(b"4\n", &io_worker)
                .unwrap();
            assert_eq!(
                tx.connection_mut()
                    .unwrap()
                    .finish_copy_in(&io_worker)
                    .unwrap(),
                1
            );
            let provenance = tx.permit.provenance.clone();
            metadata::prepare(tx.connection_mut().unwrap(), &provenance, &io_worker).unwrap();
            let row =
                metadata::lock_validate(tx.connection_mut().unwrap(), &provenance, &io_worker)
                    .unwrap();
            assert_eq!(row.clone.as_ref(), Some(&tx.permit.control));
            metadata::insert_marker(
                tx.connection_mut().unwrap(),
                &provenance,
                &CopyDone {
                    source_oid: table.table.source_oid,
                    target_oid: table.target_oid,
                    generation: table.generation,
                    rows: 1,
                    bytes: 2,
                    target_namespace_oid: table.target_namespace_oid,
                    target_owner_oid: table.target_owner_oid,
                    format: CopyFormat::Text,
                    part: single_part(),
                },
                &io_worker,
            )
            .unwrap();
            ready.send(()).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let sql = format!(
                    "SELECT {} = ANY(pg_catalog.pg_blocking_pids({coordinator_pid}))",
                    tx.pid
                );
                let waiting = tx
                    .connection_mut()
                    .unwrap()
                    .query(&sql, &io_worker)
                    .unwrap();
                if waiting.cell(0, 0).unwrap() == Some(b"t".as_slice()) {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "coordinator did not wait behind the pending marker transaction"
                );
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            let minimum = tx.synchronous_commit;
            confirm_durability(tx.connection_mut().unwrap(), minimum, &io_worker).unwrap();
            assert_eq!(
                tx.connection_mut()
                    .unwrap()
                    .query("COMMIT", &io_worker)
                    .unwrap()
                    .command_tag()
                    .unwrap(),
                "COMMIT"
            );
            tx.finished = true;
        });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let result = authority
            .resolve_unknown(table, &single_part(), io)
            .unwrap();
        handle.join().unwrap();
        result
            == Some(CopyReport {
                tables: 1,
                rows: 1,
                bytes: 2,
            })
    }

    fn refuse_replaced_owned_name(
        authority: &mut CloneAuthority,
        table: &OwnedCopyTable,
        target: &mut PgConnection,
        target_info: &crate::contracts::Conninfo,
        io: &IoControl,
    ) -> bool {
        let schema = target.quote_identifier(&table.table.name.schema).unwrap();
        let name = target.quote_identifier(&table.table.name.table).unwrap();
        let renamed = target
            .quote_identifier(&format!("{}_owned_renamed", table.table.name.table))
            .unwrap();
        let permit = authority.permit(table, &single_part()).unwrap();
        target
            .query(
                &format!("ALTER TABLE {schema}.{name} RENAME TO {renamed}"),
                io,
            )
            .unwrap();
        target
            .query(&format!("CREATE TABLE {schema}.{name}(id int)"), io)
            .unwrap();
        target
            .query(&format!("INSERT INTO {schema}.{name} VALUES(777)"), io)
            .unwrap();
        let attempt = CopyTxn::begin(PgConnection::connect(target_info, io).unwrap(), permit, io);
        let refused = attempt.is_err();
        drop(attempt);
        assert_eq!(
            target
                .query(&format!("SELECT id FROM {schema}.{renamed}"), io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"4".as_slice())
        );
        assert_eq!(
            target
                .query(&format!("SELECT id FROM {schema}.{name}"), io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"777".as_slice())
        );
        assert!(authority.checked_markers(io).unwrap().is_empty());
        refused
    }

    fn lost_marker_commit(
        authority: &mut CloneAuthority,
        table: &OwnedCopyTable,
        target: &crate::contracts::Conninfo,
        io: &IoControl,
    ) {
        use std::io::{BufRead, Write};
        use std::process::{Command, Stdio};
        let script = r#"
import json,os,pathlib,shlex,sys
sys.path.insert(0,str(pathlib.Path.cwd()/'tests'/'support'))
from protocol_fault import ProtocolFault
from db_harness import conninfo
context=json.loads(pathlib.Path(os.environ['RPGMIGRATE_TEST_CONTEXT']).read_text())
values=dict(part.split('=',1) for part in shlex.split(os.environ['RPGMIGRATE_TEST_CLONE_TARGET_ADMIN']))
with ProtocolFault(context,'commit_unknown') as relay:
 print(json.dumps({'conninfo':conninfo(context,values['dbname'],values['user'],str(relay.directory))}),flush=True)
 assert sys.stdin.readline().strip()=='done'
 assert relay.backend_closed.wait(timeout=5) and relay.hits==1 and relay.client_copy_bytes>0
 print(json.dumps({'fault_hits':relay.hits,'backend_closed':True}),flush=True)
"#;
        let mut child = Command::new("python3")
            .args(["-c", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        assert!(output.read_line(&mut line).unwrap() > 0);
        let ready: serde_json::Value = serde_json::from_str(&line).unwrap();
        let conninfo =
            crate::contracts::Conninfo::new(ready["conninfo"].as_str().unwrap().into()).unwrap();
        let mut tx = CopyTxn::begin(
            PgConnection::connect(&conninfo, io).unwrap(),
            authority.permit(table, &single_part()).unwrap(),
            io,
        )
        .unwrap();
        let name = format!("{}.{}", table.table.name.schema, table.table.name.table);
        tx.connection_mut()
            .unwrap()
            .start_copy_in(
                &format!("COPY {name} FROM STDIN WITH(FORMAT text,FREEZE true)"),
                io,
            )
            .unwrap();
        tx.connection_mut().unwrap().write_copy(b"3\n", io).unwrap();
        assert_eq!(tx.connection_mut().unwrap().finish_copy_in(io).unwrap(), 1);
        assert!(matches!(
            tx.mark_and_commit(1, 2, io).unwrap(),
            CopyCommitOutcome::Unknown
        ));
        writeln!(input, "done").unwrap();
        input.flush().unwrap();
        line.clear();
        assert!(output.read_line(&mut line).unwrap() > 0);
        let observed: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(observed["fault_hits"], 1);
        assert_eq!(observed["backend_closed"], true);
        assert!(child.wait().unwrap().success());
        assert_eq!(
            authority
                .resolve_unknown(table, &single_part(), io)
                .unwrap(),
            Some(CopyReport {
                tables: 1,
                rows: 1,
                bytes: 2
            })
        );
        let mut observer = PgConnection::connect(target, io).unwrap();
        assert_eq!(
            observer
                .query(&format!("SELECT id FROM {name}"), io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"3".as_slice())
        );
    }

    #[test]
    #[ignore = "requires owned clone PostgreSQL harness"]
    fn live_markers_commit_atomically_and_refuse_stale_generation_and_foreign_metadata() {
        use crate::contracts::{Conninfo, StateAction, StateRequest};
        let io = IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )));
        let context: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned harness required"),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(
                std::path::Path::new(context["root"].as_str().unwrap()).join(".e1-owner")
            )
            .unwrap(),
            context["token"].as_str().unwrap()
        );
        let source_info =
            Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN").unwrap()).unwrap();
        let target_info =
            Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_CLONE_TARGET_ADMIN").unwrap()).unwrap();
        let mut source = PgConnection::connect(&source_info, &io).unwrap();
        let mut target = PgConnection::connect(&target_info, &io).unwrap();
        let schema = format!("m2_marker_{}", std::process::id());
        for sql in [
            format!("CREATE SCHEMA {schema}"),
            format!("CREATE TABLE {schema}.a(id int)"),
            format!("CREATE TABLE {schema}.b(id int)"),
        ] {
            source.query(&sql, &io).unwrap();
        }
        source
            .query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", &io)
            .unwrap();
        let source_identity = crate::catalog::read_clone_source(&mut source, &io).unwrap();
        let tables: Vec<_> = crate::catalog::inspect_clone_tables(&mut source, &[], &io)
            .unwrap()
            .into_iter()
            .filter(|t| t.name.schema == schema)
            .collect();
        let plan = ClonePlan {
            source: source_identity.clone(),
            target: crate::catalog::read_clone_target(&mut target, &io).unwrap(),
            generation: CopyGeneration(1),
            copy_policy: crate::contracts::CloneCopyPolicy {
                skip_large_objects: true,
                ..Default::default()
            },
            copy_tables: test_copy_profile(&tables),
            tables,
            sequences: vec![],
            policy: RestorePolicy::default(),
        };
        let path =
            std::path::Path::new(context["root"].as_str().unwrap()).join(format!("{schema}-state"));
        let operator = crate::contracts::OperatorIdentity {
            migration_uid: "03030303-0303-0303-0303-030303030303".into(),
            run: crate::identity::RunId([3; 16]),
            workdir_id: format!("{schema}-state"),
            checkpoint_generation: 1,
        };
        assert!(fence_slot(operator.run) < 0);
        let mut state = crate::state::open_clone(
            &StateRequest {
                path,
                action: StateAction::Create,
            },
            Some(&operator),
            &io,
        )
        .unwrap();
        state
            .bind_clone(
                &plan,
                &SnapshotTicket {
                    token: "marker-test-snapshot".into(),
                    source: source_identity.clone(),
                    generation: CopyGeneration(1),
                },
            )
            .unwrap();
        let mut authority = CloneAuthority::acquire(
            PgConnection::connect(&target_info, &io).unwrap(),
            &mut state,
            &plan,
            &io,
        )
        .unwrap();
        let before = crate::catalog::clone_object_inventory(&mut target, &io).unwrap();
        state.begin_pre_data().unwrap();
        authority
            .mark_stage(ClonePhase::PreDataPending, &io)
            .unwrap();
        for sql in [
            format!("CREATE SCHEMA {schema}"),
            format!("CREATE TABLE {schema}.a(id int)"),
            format!("CREATE TABLE {schema}.b(id int)"),
            format!("INSERT INTO {schema}.a VALUES(99)"),
            format!("INSERT INTO {schema}.b VALUES(99)"),
        ] {
            target.query(&sql, &io).unwrap();
        }
        let objects = crate::catalog::clone_object_inventory(&mut target, &io)
            .unwrap()
            .into_iter()
            .filter(|o| !before.contains(o))
            .collect::<Vec<_>>();
        let owned =
            crate::catalog::reconcile_clone_tables(&mut target, &plan, &objects, &io).unwrap();
        state
            .confirm_pre_data(&plan, objects, owned.clone())
            .unwrap();
        authority.bind_tables(&state, &io).unwrap();
        let permits = owned
            .iter()
            .map(|t| authority.permit(t, &single_part()).unwrap())
            .collect::<Vec<_>>();
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for permit in permits {
                let info = target_info.clone();
                let io = io.clone();
                handles.push(scope.spawn(move || {
                    let name = permit.table.table.name.clone();
                    let mut tx =
                        CopyTxn::begin(PgConnection::connect(&info, &io).unwrap(), permit, &io)
                            .unwrap();
                    tx.connection_mut()
                        .unwrap()
                        .start_copy_in(
                            &format!(
                                "COPY {}.{} FROM STDIN WITH(FORMAT text,FREEZE true)",
                                name.schema, name.table
                            ),
                            &io,
                        )
                        .unwrap();
                    tx.connection_mut()
                        .unwrap()
                        .write_copy(b"1\n", &io)
                        .unwrap();
                    assert_eq!(tx.connection_mut().unwrap().finish_copy_in(&io).unwrap(), 1);
                    assert!(matches!(
                        tx.mark_and_commit(1, 2, &io).unwrap(),
                        CopyCommitOutcome::Durable
                    ));
                }));
            }
            for handle in handles {
                handle.join().unwrap();
            }
        });
        assert_eq!(
            authority.copy_report(&io).unwrap(),
            CopyReport {
                tables: 2,
                rows: 2,
                bytes: 4
            }
        );
        for table in &owned {
            assert_eq!(
                authority
                    .resolve_unknown(table, &single_part(), &io)
                    .unwrap(),
                Some(CopyReport {
                    tables: 1,
                    rows: 1,
                    bytes: 2
                })
            );
        }
        // Retain an old permit to prove it cannot truncate after the generation handoff.
        authority.issued.clear();
        let stale = authority.permit(&owned[0], &single_part()).unwrap();
        let fresh = SnapshotTicket {
            token: "fresh-marker-snapshot".into(),
            source: source_identity,
            generation: CopyGeneration(2),
        };
        let mut next_plan = state.checked_clone().unwrap().unwrap().plan;
        next_plan.generation = fresh.generation;
        state.advance_copy_generation(&next_plan, &fresh).unwrap();
        authority.advance_generation(&state, &io).unwrap();
        assert!(
            CopyTxn::begin(
                PgConnection::connect(&target_info, &io).unwrap(),
                stale,
                &io
            )
            .is_err()
        );
        assert!(authority.copy_report(&io).is_err());
        assert_eq!(
            target
                .query(
                    &format!(
                        "SELECT id FROM {}.{}",
                        owned[0].table.name.schema, owned[0].table.name.table
                    ),
                    &io
                )
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"1".as_slice())
        );
        let current = state.owned_copy_tables().unwrap();
        let permit = authority.permit(&current[0], &single_part()).unwrap();
        let mut dropped = CopyTxn::begin(
            PgConnection::connect(&target_info, &io).unwrap(),
            permit,
            &io,
        )
        .unwrap();
        let table_name = format!(
            "{}.{}",
            current[0].table.name.schema, current[0].table.name.table
        );
        dropped
            .connection_mut()
            .unwrap()
            .start_copy_in(
                &format!("COPY {table_name} FROM STDIN WITH(FORMAT text,FREEZE true)"),
                &io,
            )
            .unwrap();
        dropped
            .connection_mut()
            .unwrap()
            .write_copy(b"2\n", &io)
            .unwrap();
        assert_eq!(
            dropped
                .connection_mut()
                .unwrap()
                .finish_copy_in(&io)
                .unwrap(),
            1
        );
        drop(dropped);
        assert_eq!(
            authority
                .resolve_unknown(&current[0], &single_part(), &io)
                .unwrap(),
            None
        );
        assert_eq!(
            target
                .query(&format!("SELECT id FROM {table_name}"), &io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"1".as_slice())
        );
        assert!(authority.permit(&current[0], &single_part()).is_err());
        let mut restart_plan = state.checked_clone().unwrap().unwrap().plan;
        restart_plan.generation.0 += 1;
        let restart_ticket = SnapshotTicket {
            token: "absent-marker-restart".into(),
            source: restart_plan.source.clone(),
            generation: restart_plan.generation,
        };
        state
            .advance_copy_generation(&restart_plan, &restart_ticket)
            .unwrap();
        authority = CloneAuthority::acquire(
            PgConnection::connect(&target_info, &io).unwrap(),
            &mut state,
            &restart_plan,
            &io,
        )
        .unwrap();
        authority.advance_generation(&state, &io).unwrap();
        let current = state.owned_copy_tables().unwrap();
        lost_marker_commit(&mut authority, &current[0], &target_info, &io);
        let pending_commit_resolved =
            delayed_marker_commit(&mut authority, &current[1], &target_info, &io);
        let (sync_worker_unknown, sync_barrier_refused) = sync_replication_uncertainty(
            &mut authority,
            &mut state,
            &mut target,
            &target_info,
            &io,
        );
        let next = SnapshotTicket {
            token: "identity-replacement-snapshot".into(),
            source: plan.source.clone(),
            generation: CopyGeneration(
                state.checked_clone().unwrap().unwrap().plan.generation.0 + 1,
            ),
        };
        let mut next_plan = state.checked_clone().unwrap().unwrap().plan;
        next_plan.generation = next.generation;
        state.advance_copy_generation(&next_plan, &next).unwrap();
        authority.advance_generation(&state, &io).unwrap();
        let latest = state.owned_copy_tables().unwrap();
        let replaced_name_refused =
            refuse_replaced_owned_name(&mut authority, &latest[1], &mut target, &target_info, &io);
        assert!(
            pending_commit_resolved
                && replaced_name_refused
                && sync_worker_unknown
                && sync_barrier_refused,
            "pending_commit_resolved={pending_commit_resolved}, replaced_name_refused={replaced_name_refused}, sync_worker_unknown={sync_worker_unknown}, sync_barrier_refused={sync_barrier_refused}"
        );
        let metadata_schema = authority
            .authority
            .provenance
            .intent
            .binding
            .schema_name
            .clone();
        target
            .query(
                &format!("CREATE TABLE {metadata_schema}.foreign_object(id int)"),
                &io,
            )
            .unwrap();
        assert!(metadata_inventory(&mut target, &authority.authority.provenance, &io).is_err());
    }
}

#[cfg(test)]
mod follow_authority_tests {
    use super::*;
    use crate::contracts::{CopyGeneration, PublicationReceipt};
    use crate::identity::{CommitLsn, RunId, SourceId, TxId, TxSeq};
    use crate::state::{FollowControl, SourceSlotBinding, SourceSlotIntent, SourceSlotReceipt};

    fn frontier(order: u64, end: u64) -> Frontier {
        Frontier {
            tx: TxId {
                stream: StreamId {
                    run: RunId([1; 16]),
                    source: SourceId {
                        system_identifier: 2,
                        database_oid: 3,
                    },
                    generation: 1,
                },
                xid: order as u32,
                begin_ordinal: end - 1,
            },
            order: TxSeq(order),
            commit: Some(CommitLsn(end - 1)),
            end: SourceEndLsn(end),
        }
    }

    fn row() -> ControlRow {
        let stream = frontier(1, 100).tx.stream;
        let receipt = SourceSlotReceipt {
            intent: SourceSlotIntent {
                binding: SourceSlotBinding::new(
                    stream,
                    "rpgmigrate_test".into(),
                    crate::cdc::contracts::Plugin::Pgoutput,
                    1,
                )
                .unwrap(),
                nonce: [2; 16],
            },
            consistent_point: SourceEndLsn(10),
            snapshot_token: "snapshot".into(),
        };
        ControlRow {
            drain: None,
            follow_completed: false,
            applied: None,
            barrier_epoch: 0,
            barrier_counter: 1,
            anchor: Some(SnapshotAnchor {
                stream,
                snapshot_token: receipt.snapshot_token.clone(),
                consistent_point: receipt.consistent_point,
                plan_nonce: receipt.intent.nonce,
            }),
            clone: None,
            certified_lsn: None,
            follow_binding: Some(FollowControl {
                binding: FollowBinding {
                    slot: receipt,
                    publication: PublicationReceipt {
                        name: "rpgmigrate_test".into(),
                        oid: 4,
                        owner_oid: 5,
                    },
                    source_timeline: 1,
                    copy_generation: CopyGeneration(1),
                    source_types: Vec::new(),
                },
                operator: None,
                origin_name: None,
            }),
        }
    }

    fn admitted_fixture() -> (ControlRow, crate::state::FollowControl, ClonePlan) {
        let mut row = row();
        row.applied = Some(frontier(1, 0x10));
        row.certified_lsn = Some(SourceEndLsn(0x10));
        row.anchor.as_mut().unwrap().consistent_point = SourceEndLsn(8);
        let follow = row.follow_binding.as_mut().unwrap();
        follow.binding.slot.consistent_point = SourceEndLsn(8);
        let plan = ClonePlan {
            source: crate::contracts::CloneSource {
                identity: follow.binding.stream().source,
                timeline: 1,
                major: 16,
            },
            target: crate::contracts::CloneTarget {
                identity: crate::identity::SourceId {
                    system_identifier: 4,
                    database_oid: 5,
                },
                major: 18,
            },
            generation: CopyGeneration(1),
            tables: Vec::new(),
            copy_policy: crate::contracts::CloneCopyPolicy {
                skip_large_objects: true,
                ..Default::default()
            },
            copy_tables: Vec::new(),
            sequences: Vec::new(),
            policy: crate::contracts::RestorePolicy::default(),
        };
        let follow = follow.clone();
        row.clone = Some(CloneControl {
            generation: plan.generation,
            phase: ClonePhase::Analyzing,
            snapshot_token: follow.binding.slot.snapshot_token.clone(),
            source_timeline: 1,
            table_count: 0,
            prepared_generation: None,
            large_objects: LargeObjectControl {
                generation: plan.generation,
                initial: None,
                final_sync: None,
            },
        });
        (row, follow, plan)
    }

    #[test]
    fn follow_request_floor_excludes_certification() {
        let (mut row, follow, plan) = admitted_fixture();
        assert_eq!(
            admitted_follow(&row, &follow, &plan).unwrap(),
            SourceEndLsn(0x10)
        );
        row.certified_lsn = Some(SourceEndLsn(0x30));
        assert_eq!(
            admitted_follow(&row, &follow, &plan).unwrap(),
            SourceEndLsn(0x10)
        );
        row.applied = Some(frontier(2, 0x18));
        assert_eq!(
            admitted_follow(&row, &follow, &plan).unwrap(),
            SourceEndLsn(0x18)
        );
        row.applied = Some(frontier(1, 0x10));
        let mut follow = follow;
        follow.binding.slot.consistent_point = SourceEndLsn(0x28);
        row.anchor.as_mut().unwrap().consistent_point = SourceEndLsn(0x28);
        row.follow_binding = Some(follow.clone());
        assert_eq!(
            admitted_follow(&row, &follow, &plan).unwrap(),
            SourceEndLsn(0x28)
        );
    }

    #[test]
    fn follow_request_floor_requires_exact_anchor() {
        let (valid, follow, plan) = admitted_fixture();
        for field in 0..9 {
            let mut row = valid.clone();
            match field {
                0 => row.anchor = None,
                1 => row.anchor.as_mut().unwrap().plan_nonce[0] ^= 1,
                2 => row.anchor.as_mut().unwrap().snapshot_token.push('x'),
                3 => row.anchor.as_mut().unwrap().stream.generation += 1,
                4 => row.clone.as_mut().unwrap().generation.0 += 1,
                5 => row.clone.as_mut().unwrap().phase = ClonePhase::Copying,
                6 => row.clone.as_mut().unwrap().source_timeline += 1,
                7 => {
                    row.follow_binding
                        .as_mut()
                        .unwrap()
                        .binding
                        .copy_generation
                        .0 += 1
                }
                _ => row.follow_binding = None,
            }
            assert!(
                admitted_follow(&row, &follow, &plan).is_err(),
                "field {field}"
            );
        }
    }

    #[test]
    fn follow_later_begin_is_not_feedback() {
        let (row, _, _) = admitted_fixture();
        let next = drained_row(
            &row,
            SourceEndLsn(0x20),
            DrainWitness::LaterBegin(CommitLsn(0x30)),
        )
        .unwrap();
        assert_eq!(next.applied, row.applied);
        assert_eq!(next.certified_lsn, Some(SourceEndLsn(0x10)));
        assert_eq!(durable_start(&next), Some(SourceEndLsn(0x10)));
        assert_eq!(next.barrier_counter, row.barrier_counter + 1);
        let (authority, _) = tests::disconnected_authority();
        let proof = DurableProof {
            evidence: Box::new(ProofEvidence {
                stream: row.follow_binding.as_ref().unwrap().binding.stream(),
                through: next.applied,
                barrier_epoch: next.barrier_epoch,
                barrier_counter: next.barrier_counter,
                target: authority.provenance,
                certified_lsn: next.certified_lsn,
                follow: true,
            }),
        };
        assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(0x10));
    }

    #[test]
    fn follow_equal_certified_keepalive_records_drain() {
        let (mut row, _, _) = admitted_fixture();
        row.certified_lsn = Some(SourceEndLsn(0x30));
        let next = drained_row(
            &row,
            SourceEndLsn(0x20),
            DrainWitness::Keepalive(SourceEndLsn(0x30)),
        )
        .unwrap();
        assert_eq!(
            next.drain.unwrap(),
            DrainRecord {
                end: SourceEndLsn(0x20),
                witness: SourceEndLsn(0x30),
                kind: DrainKind::Keepalive
            }
        );
        assert_eq!(next.certified_lsn, Some(SourceEndLsn(0x30)));
        assert_eq!(next.applied, row.applied);
        assert_eq!(next.barrier_counter, row.barrier_counter + 1);
    }

    #[test]
    fn sealed_drain_recovery_preserves_witness_token_and_crossing_commit() {
        let (mut row, _, _) = admitted_fixture();
        row.applied = Some(frontier(2, 0x28));
        let original = drained_row(
            &row,
            SourceEndLsn(0x20),
            DrainWitness::LaterBegin(CommitLsn(0x30)),
        )
        .unwrap();
        let recovered = drained_row(
            &original,
            SourceEndLsn(0x20),
            DrainWitness::Keepalive(SourceEndLsn(0x40)),
        )
        .unwrap();
        assert_eq!(recovered, original);
        assert_eq!(recovered.drain.unwrap().witness, SourceEndLsn(0x30));
        assert_eq!(recovered.applied.unwrap().end, SourceEndLsn(0x28));
        assert_eq!(recovered.certified_lsn, Some(SourceEndLsn(0x10)));
        assert!(
            drained_row(
                &original,
                SourceEndLsn(0x21),
                DrainWitness::LaterBegin(CommitLsn(0x40))
            )
            .is_err()
        );
    }

    #[test]
    fn drain_boundaries_and_completed_tokens_fail_closed() {
        let (row, _, _) = admitted_fixture();
        assert!(
            drained_row(
                &row,
                SourceEndLsn(0),
                DrainWitness::Keepalive(SourceEndLsn(0x30))
            )
            .is_err()
        );
        assert!(
            drained_row(
                &row,
                SourceEndLsn(0x20),
                DrainWitness::Keepalive(SourceEndLsn(0x1f))
            )
            .is_err()
        );
        assert!(
            drained_row(
                &row,
                SourceEndLsn(0x20),
                DrainWitness::LaterBegin(CommitLsn(0x20))
            )
            .is_err()
        );
        let mut completed = drained_row(
            &row,
            SourceEndLsn(0x20),
            DrainWitness::Keepalive(SourceEndLsn(0x20)),
        )
        .unwrap();
        completed.follow_completed = true;
        assert!(increment_barrier(completed.clone(), completed.barrier_epoch).is_err());
        assert!(
            drained_row(
                &completed,
                SourceEndLsn(0x20),
                DrainWitness::Keepalive(SourceEndLsn(0x20))
            )
            .is_err()
        );
        let mut exhausted = row;
        exhausted.barrier_counter = u64::MAX;
        assert!(
            drained_row(
                &exhausted,
                SourceEndLsn(0x20),
                DrainWitness::Keepalive(SourceEndLsn(0x20))
            )
            .is_err()
        );
    }

    #[test]
    fn reconnect_uses_barrier_proven_maximum_and_refuses_gaps() {
        let mut row = row();
        let stream = row.anchor.as_ref().unwrap().stream;
        assert_eq!(
            recovery_start(stream, &row, SourceEndLsn(10))
                .unwrap()
                .start_lsn(),
            SourceEndLsn(10)
        );
        row.applied = Some(frontier(1, 100));
        row.certified_lsn = Some(SourceEndLsn(200));
        let start = recovery_start(stream, &row, SourceEndLsn(150)).unwrap();
        assert_eq!(start.start_lsn(), SourceEndLsn(200));
        assert_eq!(start.confirmed_flush_lsn(), SourceEndLsn(150));
        let error = recovery_start(stream, &row, SourceEndLsn(201)).unwrap_err();
        assert_eq!(error.exit_code(), 3);
        assert!(!error.message.contains("snapshot"));
    }

    #[test]
    fn duplicate_is_only_the_exact_committed_source_identity() {
        let first = frontier(1, 100);
        assert!(classify_replay(first.tx.stream, Some(first), first).unwrap());
        let mut conflict = first;
        conflict.tx.xid += 1;
        assert!(classify_replay(first.tx.stream, Some(first), conflict).is_err());
        conflict = first;
        conflict.order.0 += 1;
        assert!(classify_replay(first.tx.stream, Some(first), conflict).is_err());
        assert!(classify_replay(first.tx.stream, Some(first), frontier(2, 99)).is_err());
        assert!(!classify_replay(first.tx.stream, Some(first), frontier(2, 101)).unwrap());
    }

    #[test]
    fn follow_commit_requires_source_begin_identity_and_strict_end() {
        let first = frontier(1, 100);
        let mut missing = frontier(2, 101);
        missing.commit = None;
        let mut zero = frontier(2, 101);
        zero.commit = Some(CommitLsn(0));
        zero.tx.begin_ordinal = 0;
        for invalid in [missing, zero, frontier(2, 100)] {
            assert!(validate_follow_next(first.tx.stream, Some(first), invalid).is_err());
        }
        let mut invalid = frontier(2, 101);
        invalid.tx.begin_ordinal += 1;
        assert!(validate_follow_next(first.tx.stream, Some(first), invalid).is_err());
    }

    #[test]
    fn known_frontier_follow_cannot_bypass_source_identity_or_copy_floor() {
        let (mut authority, io) = super::tests::disconnected_authority();
        authority.row = row();
        let stream = authority.row.anchor.as_ref().unwrap().stream;
        authority.provenance.intent.binding.stream = stream;
        let mut invalid = frontier(1, 100);
        invalid.tx.begin_ordinal += 1;
        assert!(authority.begin(invalid, &io).is_err());
        assert!(authority.begin(frontier(1, 10), &io).is_err());
        assert!(authority.row.applied.is_none());
    }

    #[test]
    fn follow_relation_binding_mismatch_poison_is_local_and_precedes_sql() {
        let (mut authority, io) = super::tests::disconnected_authority();
        authority.row = row();
        let mut binding = authority
            .row
            .follow_binding
            .as_ref()
            .unwrap()
            .binding
            .clone();
        binding.publication.oid += 1;
        let owned = OwnedCopyTable {
            table: crate::contracts::CloneTable {
                source_oid: 10,
                name: crate::contracts::QualifiedTable {
                    schema: "public".into(),
                    table: "items".into(),
                },
                writable_columns: vec!["id".into()],
                all_columns: vec!["id".into()],
                identity_modes: vec![0],
                partition_identity_modes: None,
            },
            target_oid: 20,
            target_namespace_oid: 21,
            target_owner_oid: 22,
            generation: CopyGeneration(1),
        };
        let relation = Event::Relation {
            key: crate::cdc::contracts::RelationKey {
                oid: 10,
                generation: 1,
            },
            schema: "public".into(),
            name: "items".into(),
            columns: Vec::new(),
            replica_identity: b'd',
        };
        let mut tx = TargetTxn {
            authority: &mut authority,
            next: None,
            failed: false,
            finished: false,
        };
        let error = tx
            .preflight_follow_relation(&relation, &binding, &owned, &io)
            .unwrap_err();
        assert_eq!(
            error.message,
            "target-follow: relation preflight binding differs from logged authority"
        );
        assert!(tx.failed);
        assert!(tx.commit(&io).is_err());
    }

    #[test]
    fn copy_anchor_alone_never_constructs_feedback() {
        let (authority, _) = super::tests::disconnected_authority();
        let mut proof = DurableProof {
            evidence: Box::new(ProofEvidence {
                stream: frontier(1, 100).tx.stream,
                through: None,
                certified_lsn: None,
                barrier_epoch: 0,
                barrier_counter: 1,
                target: authority.provenance.clone(),
                follow: true,
            }),
        };
        assert!(proof.feedback().is_none());
        proof.evidence.through = Some(frontier(1, 100));
        assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(100));
        proof.evidence.certified_lsn = Some(SourceEndLsn(200));
        assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(200));
        proof.evidence.follow = false;
        assert!(proof.feedback().is_none());
    }
}
