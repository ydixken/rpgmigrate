use std::sync::atomic::Ordering;
use std::sync::mpsc::{RecvTimeoutError, SyncSender};
use std::time::{Duration, Instant};

use super::checkpoint::{DurableProof, RecoveryStart, SafeFeedback, TargetAuthority};
use super::serial::{Caches, event_bytes, refusal};
use super::{CommitOutcome, Frontier};
use crate::catalog;
use crate::cdc::contracts::{Begin, Commit, Event, FollowMessage};
use crate::contracts::{FollowBinding, FollowOptions, IoControl, OwnedCopyTable, Result};
use crate::follow::channel::{FeedbackSender, FollowReceiver};
use crate::identity::{SourceEndLsn, TxId, TxSeq};
use crate::pg::{PgConnection, operation_control};
use crate::state::NativeState;

const POLL: Duration = Duration::from_millis(100);
const CERTIFY_INTERVAL: Duration = Duration::from_secs(10);

#[allow(
    clippy::too_many_arguments,
    reason = "M3 freezes this thread boundary with separate opaque handles"
)]
pub(crate) fn run(
    options: &FollowOptions,
    binding: &FollowBinding,
    native: &mut NativeState,
    source_confirmed_flush: SourceEndLsn,
    ready: SyncSender<RecoveryStart>,
    input: &FollowReceiver,
    feedback: &FeedbackSender,
    io: &IoControl,
) -> Result<()> {
    options.limits.validate()?;
    if options.apply_group_txns == 0 {
        return Err(refusal("follow apply: group transaction limit is zero"));
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
    loop {
        check(io)?;
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
                        io,
                    )?;
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
                    io,
                )?;
            }
            FollowMessage::Keepalive {
                wal_end,
                decoder_idle: true,
            } => {
                if let Some(safe) = certify_keepalive(
                    wal_end,
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
                    feedback.publish(safe)?;
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
    io: &IoControl,
) -> Result<Option<FollowMessage>> {
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
            Err(RecvTimeoutError::Timeout) if active.is_none() => break None,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => {
                return Err(refusal("follow apply: receiver closed with open group"));
            }
        };
        match message {
            FollowMessage::Begin(begin) => {
                if active.is_some() || is_replay(begin, last, binding)? {
                    return Err(refusal("follow apply: unexpected source BEGIN"));
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
                if count >= options.apply_group_txns
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
    match caches.commit(target, &operation_control(io))? {
        CommitOutcome::Acknowledged {
            checkpoint,
            durable: Some(proof),
        } => {
            check_group_checkpoint(&checkpoint, last)?;
            publish(proof, feedback)?;
        }
        CommitOutcome::Acknowledged { durable: None, .. } => {
            return Err(refusal("follow apply: group lacks durable proof"));
        }
        CommitOutcome::Unknown => {
            return Err(refusal(
                "follow apply: target COMMIT outcome unknown; recovery required",
            ));
        }
    }
    Ok(pending)
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
fn validate_replay(
    begin: Begin,
    previous: Frontier,
    authority: &TargetAuthority,
    binding: &FollowBinding,
    input: &FollowReceiver,
    relations: &mut Vec<Event>,
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
            Err(RecvTimeoutError::Timeout) => {}
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
fn certify_keepalive(
    candidate: SourceEndLsn,
    applied: Option<Frontier>,
    floor: &mut SourceEndLsn,
    last: &mut Option<Instant>,
    certify: impl FnOnce(SourceEndLsn) -> Result<SafeFeedback>,
) -> Result<Option<SafeFeedback>> {
    // A queued idle marker can trail the COMMIT that was just made durable.
    if candidate <= (*floor).max(applied.map_or(SourceEndLsn(0), |frontier| frontier.end))
        || last.is_some_and(|last| last.elapsed() < CERTIFY_INTERVAL)
    {
        return Ok(None);
    }
    let safe = certify(candidate)?;
    *floor = (*floor).max(safe.end());
    *last = Some(Instant::now());
    Ok(Some(safe))
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
        return Err(refusal("follow apply: interrupted"));
    }
    Ok(())
}
#[cfg(test)]
#[path = "follow/tests.rs"]
mod tests;
