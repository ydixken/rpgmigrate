use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SyncSender};
use std::time::{Duration, Instant};

use super::checkpoint::{
    CompletedFollow, DrainWitness, DurableProof, RecoveryStart, SafeFeedback, TargetAuthority,
};
use super::serial::{Caches, event_bytes, refusal};
use super::{CommitOutcome, Frontier};
use crate::catalog;
use crate::cdc::contracts::{Begin, Commit, Event, FollowMessage};
use crate::contracts::{FollowBinding, FollowOptions, IoControl, OwnedCopyTable, Result};
use crate::follow::channel::{FeedbackSender, FollowReceiver};
use crate::identity::{SourceEndLsn, TxId, TxSeq};
use crate::pg::{PgConnection, operation_control};
use crate::state::{ControlHandle, EndposRequest, NativeState};

const POLL: Duration = Duration::from_millis(100);
const CERTIFY_INTERVAL: Duration = Duration::from_secs(10);

#[allow(
    clippy::too_many_arguments,
    reason = "M4 freezes this thread boundary with separate opaque handles"
)]
pub(crate) fn run(
    options: &FollowOptions,
    binding: &FollowBinding,
    native: &mut NativeState,
    source_confirmed_flush: SourceEndLsn,
    ready: SyncSender<RecoveryStart>,
    input: &FollowReceiver,
    feedback: &FeedbackSender,
    control: &ControlHandle,
    request_keepalive: &AtomicBool,
    io: &IoControl,
) -> Result<CompletedFollow> {
    options.limits.validate()?;
    if options.apply_group_txns == 0 {
        return Err(refusal("follow apply: group transaction limit is zero"));
    }
    control.validate_active(io)?;
    if control.follow().binding != *binding {
        return Err(refusal(
            "follow apply: control differs from the active follow binding",
        ));
    }
    let operation = operation_control(io);
    let mut connection = PgConnection::connect(&options.clone.target, &operation)?;
    configure(&mut connection, &operation)?;
    let owned = native.owned_copy_tables()?;
    let mut authority = TargetAuthority::acquire_follow(connection, native, binding, &operation)?;
    let start = authority.recover_follow(binding, source_confirmed_flush, &operation)?;
    let mut durable_floor = start
        .applied()
        .map(|frontier| frontier.end)
        .into_iter()
        .chain(start.certified_lsn())
        .chain(start.anchor().map(|anchor| anchor.consistent_point))
        .max()
        .unwrap_or(SourceEndLsn(0));
    ready
        .send(start)
        .map_err(|_| refusal("follow apply: readiness receiver closed"))?;
    let mut caches = Caches::new();
    let mut replay_relations = Vec::new();
    let mut pending = None;
    let mut last_certification = None;
    let mut requested = None;
    loop {
        check(io)?;
        read_request(control, &mut requested, request_keepalive, io)?;
        let message = match pending.take() {
            Some(message) => message,
            None => match input.recv_timeout(POLL) {
                Ok(message) => message,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(refusal("follow apply: receiver closed"));
                }
            },
        };
        let request = read_request(control, &mut requested, request_keepalive, io)?;
        match message {
            FollowMessage::Begin(begin) => {
                let previous = authority.applied();
                if is_replay(begin, previous, binding)? {
                    validate_replay(
                        begin,
                        previous
                            .ok_or_else(|| refusal("follow apply: replay checkpoint is absent"))?,
                        &authority,
                        binding,
                        input,
                        &mut replay_relations,
                        control,
                        &mut requested,
                        request_keepalive,
                        io,
                    )?;
                    continue;
                }
                if let Some(witness) = ordered_drain_witness(
                    request.as_ref().map(EndposRequest::end),
                    &FollowMessage::Begin(begin),
                    false,
                ) {
                    if let Some(completed) = try_complete(
                        &mut authority,
                        request.as_ref().expect("witness requires request"),
                        witness,
                        native,
                        options,
                        control,
                        feedback,
                        io,
                    )? {
                        authority.close(&operation_control(io))?;
                        return Ok(completed);
                    }
                    pending = Some(FollowMessage::Begin(begin));
                    continue;
                }
                pending = apply_group(
                    &mut authority,
                    begin,
                    previous,
                    &mut caches,
                    &mut replay_relations,
                    options,
                    binding,
                    &owned,
                    input,
                    feedback,
                    control,
                    &mut requested,
                    request_keepalive,
                    io,
                )?;
            }
            FollowMessage::Keepalive {
                wal_end,
                decoder_idle: true,
            } => {
                match certify_keepalive(
                    wal_end,
                    request.as_ref().map(EndposRequest::end),
                    authority.applied(),
                    &mut durable_floor,
                    &mut last_certification,
                    |candidate| {
                        authority
                            .certify(candidate, &operation_control(io))?
                            .feedback()
                            .ok_or_else(|| {
                                refusal("follow apply: durability proof has no feedback authority")
                            })
                    },
                )? {
                    KeepaliveAction::Drain(witness) => {
                        if let Some(completed) = try_complete(
                            &mut authority,
                            request.as_ref().expect("witness requires request"),
                            witness,
                            native,
                            options,
                            control,
                            feedback,
                            io,
                        )? {
                            authority.close(&operation_control(io))?;
                            return Ok(completed);
                        }
                    }
                    KeepaliveAction::Feedback(safe) => feedback.publish(safe)?,
                    KeepaliveAction::Wait => {}
                }
            }
            FollowMessage::Keepalive {
                decoder_idle: false,
                ..
            } => {}
            _ => return Err(refusal("follow apply: message outside source transaction")),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_group(
    authority: &mut TargetAuthority,
    first: Begin,
    previous: Option<Frontier>,
    caches: &mut Caches,
    replay_relations: &mut Vec<Event>,
    options: &FollowOptions,
    binding: &FollowBinding,
    owned: &[OwnedCopyTable],
    input: &FollowReceiver,
    feedback: &FeedbackSender,
    control: &ControlHandle,
    requested: &mut Option<SourceEndLsn>,
    request_keepalive: &AtomicBool,
    io: &IoControl,
) -> Result<Option<FollowMessage>> {
    let request = read_request(control, requested, request_keepalive, io)?;
    if ordered_drain_witness(
        request.as_ref().map(EndposRequest::end),
        &FollowMessage::Begin(first),
        false,
    )
    .is_some()
    {
        return Ok(Some(FollowMessage::Begin(first)));
    }
    let mut target = authority.begin_group(&operation_control(io))?;
    for relation in replay_relations.drain(..) {
        caches.event(
            relation,
            &mut target,
            binding,
            owned,
            &operation_control(io),
        )?;
    }
    let mut active = Some(first);
    let mut last = previous;
    let mut count = 0u64;
    let mut rows = 0usize;
    let mut bytes = 0usize;
    let pending = loop {
        check(io)?;
        let message = match input.recv_timeout(POLL) {
            Ok(message) => message,
            Err(RecvTimeoutError::Timeout) => {
                read_request(control, requested, request_keepalive, io)?;
                if active.is_none() {
                    break None;
                }
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(refusal("follow apply: receiver closed with open group"));
            }
        };
        match message {
            FollowMessage::Begin(begin) => {
                let request = read_request(control, requested, request_keepalive, io)?;
                if active.is_some() || is_replay(begin, last, binding)? {
                    return Err(refusal("follow apply: unexpected source BEGIN"));
                }
                if ordered_drain_witness(
                    request.as_ref().map(EndposRequest::end),
                    &FollowMessage::Begin(begin),
                    false,
                )
                .is_some()
                {
                    break Some(FollowMessage::Begin(begin));
                }
                active = Some(begin);
            }
            FollowMessage::Event(event) => {
                if active.is_none() {
                    return Err(refusal("follow apply: row outside source transaction"));
                }
                if !matches!(event, Event::Relation { .. }) {
                    rows = rows.saturating_add(1);
                }
                bytes = bytes.saturating_add(caches.event(
                    event,
                    &mut target,
                    binding,
                    owned,
                    &operation_control(io),
                )?);
            }
            FollowMessage::Commit(commit) => {
                let request = read_request(control, requested, request_keepalive, io)?;
                let begin = active
                    .take()
                    .ok_or_else(|| refusal("follow apply: COMMIT without BEGIN"))?;
                let next_order = last.map_or(Ok(1), |last| {
                    last.order
                        .0
                        .checked_add(1)
                        .ok_or_else(|| refusal("follow apply: transaction order exhausted"))
                })?;
                let next = frontier(begin, commit, binding, TxSeq(next_order))?;
                target.advance(next)?;
                last = Some(next);
                count += 1;
                if request.is_some()
                    || count >= options.apply_group_txns
                    || rows >= options.limits.group_rows
                    || bytes >= options.limits.group_bytes
                {
                    break None;
                }
            }
            FollowMessage::Keepalive {
                wal_end,
                decoder_idle: true,
            } => {
                if active.is_some() {
                    return Err(refusal(
                        "follow apply: idle keepalive inside source transaction",
                    ));
                }
                break Some(FollowMessage::Keepalive {
                    wal_end,
                    decoder_idle: true,
                });
            }
            FollowMessage::Keepalive {
                decoder_idle: false,
                ..
            } => {}
        }
    };
    if count == 0 {
        return Err(refusal(
            "follow apply: cannot commit unsealed source transaction",
        ));
    }
    read_request(control, requested, request_keepalive, io)?;
    let proof = committed_group(caches.commit(target, &operation_control(io))?, last)?;
    publish(proof, feedback)?;
    Ok(pending)
}

fn read_request(
    control: &ControlHandle,
    requested: &mut Option<SourceEndLsn>,
    request_keepalive: &AtomicBool,
    io: &IoControl,
) -> Result<Option<EndposRequest>> {
    check(io)?;
    control.validate_active(io)?;
    let current = control.read(io)?;
    note_request(
        current.as_ref().map(EndposRequest::end),
        requested,
        request_keepalive,
    )?;
    Ok(current)
}

fn note_request(
    current: Option<SourceEndLsn>,
    previous: &mut Option<SourceEndLsn>,
    request_keepalive: &AtomicBool,
) -> Result<()> {
    if previous.is_some_and(|old| current.is_none_or(|end| end < old)) {
        return Err(refusal(
            "follow apply: persisted end position disappeared or moved backwards",
        ));
    }
    if current != *previous {
        *previous = current;
        request_keepalive.store(true, Ordering::Release);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn try_complete(
    authority: &mut TargetAuthority,
    request: &EndposRequest,
    witness: DrainWitness,
    native: &NativeState,
    options: &FollowOptions,
    control: &ControlHandle,
    feedback: &FeedbackSender,
    io: &IoControl,
) -> Result<Option<CompletedFollow>> {
    check(io)?;
    control.validate_active(io)?;
    if control.read(io)?.as_ref().map(EndposRequest::end) != Some(request.end()) {
        return Ok(None);
    }
    authority.observe_follow(&operation_control(io))?;
    if control.read(io)?.as_ref().map(EndposRequest::end) != Some(request.end()) {
        return Ok(None);
    }
    let sealed = match control.seal(request, io) {
        Ok(sealed) => sealed,
        Err(error) => {
            check(io)?;
            if control.read(io)?.as_ref().map(EndposRequest::end) != Some(request.end()) {
                return Ok(None);
            }
            return Err(error);
        }
    };
    check(io)?;
    let proof = authority.record_drain(&sealed, witness, &operation_control(io))?;
    if let Some(safe) = proof.feedback() {
        feedback.publish(safe)?;
    }
    let mut source = PgConnection::connect(&options.clone.source, &operation_control(io))
        .map_err(|error| crate::endpoint_error(error, crate::contracts::ErrorKind::Source))?;
    catalog::configure_session(&mut source, &operation_control(io))?;
    let completed = authority.complete_follow(
        &options.clone,
        &mut source,
        native,
        control.plan(),
        &sealed,
        &operation_control(io),
    )?;
    check(io)?;
    Ok(Some(completed))
}

fn committed_group(outcome: CommitOutcome, last: Option<Frontier>) -> Result<DurableProof> {
    match outcome {
        CommitOutcome::Acknowledged {
            checkpoint,
            durable: Some(proof),
        } => {
            check_group_checkpoint(&checkpoint, last)?;
            Ok(proof)
        }
        CommitOutcome::Acknowledged { durable: None, .. } => {
            Err(refusal("follow apply: group lacks durable proof"))
        }
        CommitOutcome::Unknown => Err(refusal(
            "follow apply: target COMMIT outcome unknown; recovery required",
        )),
    }
}

fn ordered_drain_witness(
    end: Option<SourceEndLsn>,
    message: &FollowMessage,
    transaction_open: bool,
) -> Option<DrainWitness> {
    if transaction_open {
        return None;
    }
    let end = end?;
    match message {
        FollowMessage::Begin(begin) if begin.final_lsn.0 > end.0 => {
            Some(DrainWitness::LaterBegin(begin.final_lsn))
        }
        FollowMessage::Keepalive {
            wal_end,
            decoder_idle: true,
        } if *wal_end >= end => Some(DrainWitness::Keepalive(*wal_end)),
        _ => None,
    }
}

fn is_replay(begin: Begin, previous: Option<Frontier>, binding: &FollowBinding) -> Result<bool> {
    if begin.xid == 0 || begin.final_lsn.0 == 0 {
        return Err(refusal("follow apply: invalid source BEGIN"));
    }
    let Some(previous) = previous else {
        return Ok(false);
    };
    let commit = previous
        .commit
        .ok_or_else(|| refusal("follow apply: checkpoint has no source commit identity"))?;
    if begin.final_lsn.0 < commit.0 {
        return Err(refusal(
            "follow apply: source transaction precedes checkpoint",
        ));
    }
    if begin.final_lsn == commit {
        if previous.tx.stream != binding.stream()
            || previous.tx.xid != begin.xid
            || previous.tx.begin_ordinal != begin.final_lsn.0
        {
            return Err(refusal(
                "follow apply: equal source position has different identity",
            ));
        }
        return Ok(true);
    }
    Ok(false)
}
fn frontier(
    begin: Begin,
    commit: Commit,
    binding: &FollowBinding,
    order: TxSeq,
) -> Result<Frontier> {
    if begin.xid == 0
        || begin.xid != commit.xid
        || begin.final_lsn.0 == 0
        || begin.final_lsn != commit.commit
        || begin.commit_time_micros != commit.commit_time_micros
        || commit.end.0 < commit.commit.0
        || order.0 == 0
    {
        return Err(refusal(
            "follow apply: source transaction identity differs at COMMIT",
        ));
    }
    Ok(Frontier {
        tx: TxId {
            stream: binding.stream(),
            xid: begin.xid,
            begin_ordinal: begin.final_lsn.0,
        },
        order,
        commit: Some(commit.commit),
        end: commit.end,
    })
}
#[allow(clippy::too_many_arguments)]
fn validate_replay(
    begin: Begin,
    previous: Frontier,
    authority: &TargetAuthority,
    binding: &FollowBinding,
    input: &FollowReceiver,
    relations: &mut Vec<Event>,
    control: &ControlHandle,
    requested: &mut Option<SourceEndLsn>,
    request_keepalive: &AtomicBool,
    io: &IoControl,
) -> Result<()> {
    loop {
        check(io)?;
        match input.recv_timeout(POLL) {
            Ok(FollowMessage::Commit(commit)) => {
                let next = frontier(begin, commit, binding, previous.order)?;
                if !authority.check_replay(next)? {
                    return Err(refusal("follow apply: replay is not exact checkpoint"));
                }
                return Ok(());
            }
            Ok(FollowMessage::Event(event @ Event::Relation { .. })) => {
                remember_relation(relations, event)?
            }
            Ok(FollowMessage::Event(_))
            | Ok(FollowMessage::Keepalive {
                decoder_idle: false,
                ..
            }) => {}
            Ok(_) => return Err(refusal("follow apply: invalid checkpoint replay ordering")),
            Err(RecvTimeoutError::Timeout) => {
                read_request(control, requested, request_keepalive, io)?;
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(refusal("follow apply: receiver closed during replay"));
            }
        }
    }
}
fn remember_relation(relations: &mut Vec<Event>, event: Event) -> Result<()> {
    let Event::Relation { key, .. } = &event else {
        unreachable!()
    };
    if let Some(old) = relations
        .iter()
        .find(|old| matches!(old, Event::Relation { key: old_key, .. } if old_key.oid == key.oid))
    {
        if old != &event {
            return Err(refusal("follow apply: replay relation changed"));
        }
        return Ok(());
    }
    let bytes = relations
        .iter()
        .try_fold(event_bytes(&event)?, |bytes, old| {
            bytes
                .checked_add(event_bytes(old)?)
                .ok_or_else(|| refusal("follow apply: replay relation accounting overflow"))
        })?;
    if relations.len() >= 4096 || bytes > 8 * 1024 * 1024 {
        return Err(refusal("follow apply: replay metadata budget exceeded"));
    }
    relations.push(event);
    Ok(())
}
fn check_group_checkpoint(checkpoint: &super::Checkpoint, last: Option<Frontier>) -> Result<()> {
    if Some(checkpoint.applied) != last || checkpoint.stream != checkpoint.applied.tx.stream {
        return Err(refusal(
            "follow apply: committed checkpoint differs from sealed source transaction",
        ));
    }
    Ok(())
}
#[derive(Debug, Eq, PartialEq)]
enum KeepaliveAction {
    Drain(DrainWitness),
    Feedback(SafeFeedback),
    Wait,
}

fn certify_keepalive(
    candidate: SourceEndLsn,
    requested: Option<SourceEndLsn>,
    applied: Option<Frontier>,
    floor: &mut SourceEndLsn,
    last: &mut Option<Instant>,
    certify: impl FnOnce(SourceEndLsn) -> Result<SafeFeedback>,
) -> Result<KeepaliveAction> {
    if let Some(witness) = ordered_drain_witness(
        requested,
        &FollowMessage::Keepalive {
            wal_end: candidate,
            decoder_idle: true,
        },
        false,
    ) {
        return Ok(KeepaliveAction::Drain(witness));
    }
    // A queued idle marker can trail the COMMIT that was just made durable.
    if candidate <= (*floor).max(applied.map_or(SourceEndLsn(0), |frontier| frontier.end))
        || last.is_some_and(|last| last.elapsed() < CERTIFY_INTERVAL)
    {
        return Ok(KeepaliveAction::Wait);
    }
    let safe = certify(candidate)?;
    *floor = (*floor).max(safe.end());
    *last = Some(Instant::now());
    Ok(KeepaliveAction::Feedback(safe))
}

fn publish(proof: DurableProof, feedback: &FeedbackSender) -> Result<()> {
    let safe = proof
        .feedback()
        .ok_or_else(|| refusal("follow apply: durability proof has no feedback authority"))?;
    feedback.publish(safe)
}
fn configure(connection: &mut PgConnection, io: &IoControl) -> Result<()> {
    catalog::configure_session(connection, io)?;
    for setting in [
        "session_replication_role = replica",
        "tcp_keepalives_idle = 30",
        "tcp_keepalives_interval = 10",
        "tcp_keepalives_count = 3",
    ] {
        catalog::command(connection, &format!("SET {setting}"), "SET", io)?;
    }
    let result = connection.query("SHOW server_version_num", io)?;
    if result.rows() != 1 {
        return Err(refusal("follow apply: missing target version"));
    }
    let version = result
        .cell(0, 0)?
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|v| v.parse::<u32>().ok())
        .ok_or_else(|| refusal("follow apply: invalid target version"))?;
    if version / 10000 >= 17 {
        catalog::command(connection, "SET transaction_timeout = 0", "SET", io)?;
    }
    Ok(())
}
fn check(io: &IoControl) -> Result<()> {
    if io.cancelled.load(Ordering::Acquire) || io.is_expired() {
        return Err(crate::contracts::Error::cancelled(
            crate::contracts::ErrorKind::Target,
            "follow apply: interrupted",
        ));
    }
    Ok(())
}
#[cfg(test)]
#[path = "follow/tests.rs"]
mod tests;
