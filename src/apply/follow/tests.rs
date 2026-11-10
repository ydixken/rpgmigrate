use super::*;
use crate::cdc::contracts::{Column, RelationKey};
use crate::contracts::{CopyGeneration, PublicationReceipt};
use crate::identity::{CommitLsn, RunId, SourceId, StreamId};
use crate::state::{SourceSlotBinding, SourceSlotIntent, SourceSlotReceipt};

#[test]
fn replay_relation_cache_refuses_conflicts_and_retains_one_exact_generation() {
    let event = Event::Relation {
        key: RelationKey {
            oid: 20_000,
            generation: 1,
        },
        schema: "public".into(),
        name: "items".into(),
        columns: vec![Column {
            name: "id".into(),
            type_oid: 23,
            type_mod: -1,
            key: true,
        }],
        replica_identity: b'd',
    };
    let mut relations = Vec::new();
    remember_relation(&mut relations, event.clone()).unwrap();
    remember_relation(&mut relations, event.clone()).unwrap();
    assert_eq!(relations.len(), 1);
    let mut changed = event;
    if let Event::Relation { key, .. } = &mut changed {
        key.generation += 1;
    }
    assert!(remember_relation(&mut relations, changed).is_err());
}

fn binding() -> FollowBinding {
    let stream = StreamId {
        run: RunId([1; 16]),
        source: SourceId {
            system_identifier: 9,
            database_oid: 10,
        },
        generation: 1,
    };
    FollowBinding {
        slot: SourceSlotReceipt {
            intent: SourceSlotIntent {
                binding: SourceSlotBinding::new(
                    stream,
                    "slot".into(),
                    crate::cdc::contracts::Plugin::Pgoutput,
                    1,
                )
                .unwrap(),
                nonce: [2; 16],
            },
            consistent_point: SourceEndLsn(16),
            snapshot_token: "snapshot".into(),
        },
        publication: PublicationReceipt {
            name: "pub".into(),
            oid: 21_000,
            owner_oid: 10,
        },
        source_timeline: 1,
        copy_generation: CopyGeneration(1),
        source_types: vec![],
    }
}
fn begin() -> Begin {
    Begin {
        xid: 7,
        final_lsn: CommitLsn(32),
        commit_time_micros: 99,
    }
}
fn commit() -> Commit {
    Commit {
        xid: 7,
        commit: CommitLsn(32),
        end: SourceEndLsn(48),
        commit_time_micros: 99,
    }
}
#[test]
fn frontier_uses_source_commit_identity_and_distinct_end_position() {
    let next = frontier(begin(), commit(), &binding(), TxSeq(4)).unwrap();
    assert_eq!(next.tx.begin_ordinal, 32);
    assert_eq!(next.order, TxSeq(4));
    assert_eq!(next.end, SourceEndLsn(48));
    assert!(is_replay(begin(), Some(next), &binding()).unwrap());
    for field in 0..5 {
        let mut bad = commit();
        match field {
            0 => bad.xid += 1,
            1 => bad.commit.0 += 1,
            2 => bad.end.0 = 31,
            3 => bad.commit_time_micros += 1,
            _ => bad.xid = 0,
        }
        assert!(frontier(begin(), bad, &binding(), TxSeq(4)).is_err());
    }
    assert!(frontier(begin(), commit(), &binding(), TxSeq(0)).is_err());
}
#[test]
fn replay_begin_refuses_lower_or_changed_equal_identity() {
    let previous = frontier(begin(), commit(), &binding(), TxSeq(4)).unwrap();
    let mut lower = begin();
    lower.final_lsn.0 -= 1;
    assert!(is_replay(lower, Some(previous), &binding()).is_err());
    let mut changed = begin();
    changed.xid += 1;
    assert!(is_replay(changed, Some(previous), &binding()).is_err());
    let mut next = begin();
    next.final_lsn.0 += 16;
    assert!(!is_replay(next, Some(previous), &binding()).unwrap());
    assert!(!is_replay(begin(), None, &binding()).unwrap());
}

#[test]
fn idle_keepalives_ignore_stale_and_equal_positions_after_durable_commit() {
    let applied = frontier(begin(), commit(), &binding(), TxSeq(1)).unwrap();
    let mut floor = applied.end;
    let mut last = None;
    for candidate in [SourceEndLsn(16), SourceEndLsn(32), applied.end] {
        assert_eq!(
            certify_keepalive(candidate, None, &mut floor, &mut last, |_| panic!(
                "stale or equal keepalive must not request certification"
            ))
            .unwrap(),
            KeepaliveAction::Wait
        );
    }
    assert_eq!(floor, SourceEndLsn(48));
    assert!(last.is_none());
    let candidate = SourceEndLsn(64);
    let KeepaliveAction::Feedback(safe) =
        certify_keepalive(candidate, None, &mut floor, &mut last, |requested| {
            assert_eq!(requested, candidate);
            Ok(super::super::checkpoint::feedback_for_test(
                binding().stream(),
                candidate,
            ))
        })
        .unwrap()
    else {
        panic!("new position requires certification");
    };
    assert_eq!(safe.end(), candidate);
    assert_eq!(floor, candidate);
    assert!(last.is_some());
    last = None;
    for candidate in [applied.end, floor] {
        assert_eq!(
            certify_keepalive(candidate, None, &mut floor, &mut last, |_| panic!(
                "previously proven position must not request certification"
            ))
            .unwrap(),
            KeepaliveAction::Wait
        );
    }
}

#[test]
fn idle_keepalive_requires_positive_proof_before_advancing_floor_or_cadence() {
    let mut floor = SourceEndLsn(48);
    let mut last = None;
    assert!(
        certify_keepalive(SourceEndLsn(64), None, &mut floor, &mut last, |_| Err(
            refusal("certification failed")
        ))
        .is_err()
    );
    assert_eq!(floor, SourceEndLsn(48));
    assert!(last.is_none());
    let candidate = SourceEndLsn(64);
    assert!(matches!(
        certify_keepalive(candidate, None, &mut floor, &mut last, |_| Ok(
            super::super::checkpoint::feedback_for_test(binding().stream(), candidate)
        ))
        .unwrap(),
        KeepaliveAction::Feedback(_)
    ));
    assert_eq!(
        certify_keepalive(SourceEndLsn(80), None, &mut floor, &mut last, |_| panic!(
            "certification cadence must remain bounded"
        ))
        .unwrap(),
        KeepaliveAction::Wait
    );
    assert_eq!(floor, candidate);
    last = Some(Instant::now() - CERTIFY_INTERVAL);
    let next = SourceEndLsn(80);
    assert!(matches!(
        certify_keepalive(next, None, &mut floor, &mut last, |_| Ok(
            super::super::checkpoint::feedback_for_test(binding().stream(), next)
        ))
        .unwrap(),
        KeepaliveAction::Feedback(_)
    ));
    assert_eq!(floor, next);
}

#[test]
fn acknowledged_group_checkpoint_matches_the_exact_last_sealed_source_commit() {
    let expected = frontier(begin(), commit(), &binding(), TxSeq(4)).unwrap();
    let checkpoint = super::super::Checkpoint {
        stream: expected.tx.stream,
        applied: expected,
        barrier_epoch: 1,
        barrier_counter: 1,
    };
    check_group_checkpoint(&checkpoint, Some(expected)).unwrap();
    assert!(check_group_checkpoint(&checkpoint, None).is_err());
    for field in 0..7 {
        let mut changed = checkpoint;
        match field {
            0 => changed.stream.generation += 1,
            1 => changed.applied.tx.stream.generation += 1,
            2 => changed.applied.tx.xid += 1,
            3 => changed.applied.tx.begin_ordinal += 1,
            4 => changed.applied.order.0 += 1,
            5 => changed.applied.commit = None,
            _ => changed.applied.end.0 += 1,
        }
        assert!(check_group_checkpoint(&changed, Some(expected)).is_err());
    }
}

#[test]
fn endpos_sampled_before_certification_still_drains() {
    let mut floor = SourceEndLsn(0x30);
    let mut last = Some(Instant::now());
    let action = certify_keepalive(
        SourceEndLsn(0x30),
        Some(SourceEndLsn(0x20)),
        &mut floor,
        &mut last,
        |_| panic!("drain must bypass ordinary certification skips"),
    )
    .unwrap();
    assert_eq!(
        action,
        KeepaliveAction::Drain(DrainWitness::Keepalive(SourceEndLsn(0x30)))
    );
    assert_eq!(floor, SourceEndLsn(0x30));
}

#[test]
fn fresh_equal_certified_keepalive_drains() {
    let mut floor = SourceEndLsn(0x30);
    let mut last = None;
    assert_eq!(
        certify_keepalive(SourceEndLsn(0x30), None, &mut floor, &mut last, |_| panic!(
            "unchanged certificate must not advance"
        ))
        .unwrap(),
        KeepaliveAction::Wait
    );
    for end in [SourceEndLsn(0x20), SourceEndLsn(0x30)] {
        assert_eq!(
            certify_keepalive(
                SourceEndLsn(0x30),
                Some(end),
                &mut floor,
                &mut last,
                |_| panic!("fresh equal-certified marker must select drain")
            )
            .unwrap(),
            KeepaliveAction::Drain(DrainWitness::Keepalive(SourceEndLsn(0x30)))
        );
    }
    assert_eq!(
        certify_keepalive(
            SourceEndLsn(0x30),
            Some(SourceEndLsn(0x40)),
            &mut floor,
            &mut last,
            |_| panic!("marker below E cannot authorize drain")
        )
        .unwrap(),
        KeepaliveAction::Wait
    );
}

#[test]
fn crossing_transaction_closes_whole_before_later_begin_witness() {
    let mut crossing = begin();
    crossing.final_lsn = CommitLsn(0x18);
    let end = Some(SourceEndLsn(0x20));
    assert!(ordered_drain_witness(end, &FollowMessage::Begin(crossing), false).is_none());
    let mut crossing_commit = commit();
    crossing_commit.commit = crossing.final_lsn;
    for commit_end in [SourceEndLsn(0x20), SourceEndLsn(0x28)] {
        crossing_commit.end = commit_end;
        let applied = frontier(crossing, crossing_commit, &binding(), TxSeq(1)).unwrap();
        assert_eq!(applied.end, commit_end);
        assert!(
            ordered_drain_witness(end, &FollowMessage::Commit(crossing_commit), false).is_none()
        );
    }
    let mut later = begin();
    later.final_lsn = CommitLsn(0x30);
    assert_eq!(
        ordered_drain_witness(end, &FollowMessage::Begin(later), false),
        Some(DrainWitness::LaterBegin(later.final_lsn))
    );
    assert!(ordered_drain_witness(end, &FollowMessage::Begin(later), true).is_none());
    later.final_lsn = CommitLsn(0x20);
    assert!(ordered_drain_witness(end, &FollowMessage::Begin(later), false).is_none());
}

#[test]
fn active_transaction_keepalive_is_never_drain_coverage() {
    for decoder_idle in [false, true] {
        let message = FollowMessage::Keepalive {
            wal_end: SourceEndLsn(0x30),
            decoder_idle,
        };
        assert!(ordered_drain_witness(Some(SourceEndLsn(0x20)), &message, true).is_none());
        if !decoder_idle {
            assert!(ordered_drain_witness(Some(SourceEndLsn(0x20)), &message, false).is_none());
        }
    }
}

#[test]
fn unapplied_or_unknown_group_cannot_release_a_drain_witness() {
    let expected = frontier(begin(), commit(), &binding(), TxSeq(1)).unwrap();
    let checkpoint = super::super::Checkpoint {
        stream: expected.tx.stream,
        applied: expected,
        barrier_epoch: 1,
        barrier_counter: 1,
    };
    assert!(
        committed_group(
            CommitOutcome::Unknown,
            Some(expected),
            DurabilityMode::Durable
        )
        .is_err()
    );
    assert!(
        committed_group(
            CommitOutcome::Acknowledged {
                checkpoint,
                durable: None
            },
            Some(expected),
            DurabilityMode::Durable,
        )
        .is_err()
    );
}

#[test]
fn cancellation_and_deadline_are_not_follow_completion() {
    let io = IoControl {
        deadline: Instant::now() + Duration::from_secs(1),
        cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
    io.cancelled.store(true, Ordering::Release);
    assert!(check(&io).unwrap_err().is_cancelled());
    io.cancelled.store(false, Ordering::Release);
    let expired = IoControl {
        deadline: Instant::now() - Duration::from_secs(1),
        ..io
    };
    assert!(check(&expired).unwrap_err().is_cancelled());
}

#[test]
fn retained_later_begin_is_reevaluated_for_each_current_forward_boundary() {
    let message = FollowMessage::Begin(Begin {
        final_lsn: CommitLsn(0x30),
        ..begin()
    });
    for end in [0x20, 0x28] {
        assert_eq!(
            ordered_drain_witness(Some(SourceEndLsn(end)), &message, false),
            Some(DrainWitness::LaterBegin(CommitLsn(0x30)))
        );
    }
    for end in [0x30, 0x38] {
        assert!(ordered_drain_witness(Some(SourceEndLsn(end)), &message, false).is_none());
    }
}

#[test]
fn keepalive_request_is_coalesced_and_observed_boundary_never_regresses() {
    let signal = AtomicBool::new(false);
    let mut previous = None;
    note_request(None, &mut previous, &signal).unwrap();
    assert!(!signal.load(Ordering::Acquire));
    note_request(Some(SourceEndLsn(0x20)), &mut previous, &signal).unwrap();
    assert!(signal.swap(false, Ordering::AcqRel));
    note_request(Some(SourceEndLsn(0x20)), &mut previous, &signal).unwrap();
    assert!(!signal.load(Ordering::Acquire));
    note_request(Some(SourceEndLsn(0x28)), &mut previous, &signal).unwrap();
    assert!(signal.swap(false, Ordering::AcqRel));
    for changed in [None, Some(SourceEndLsn(0x20))] {
        assert!(note_request(changed, &mut previous, &signal).is_err());
        assert_eq!(previous, Some(SourceEndLsn(0x28)));
        assert!(!signal.load(Ordering::Acquire));
    }
}

fn m9_settings() -> EffectiveFollowSettings {
    EffectiveFollowSettings {
        durability: DurabilityMode::ReplaySafe,
        barrier_interval_ms: Some(100),
        apply_group_txns: 100,
        limits: crate::contracts::FollowLimits::default(),
    }
}

fn m9_checkpoint(end: u64) -> super::super::Checkpoint {
    let mut applied = frontier(begin(), commit(), &binding(), TxSeq(end / 100)).unwrap();
    applied.end = SourceEndLsn(end);
    super::super::Checkpoint {
        stream: applied.tx.stream,
        applied,
        barrier_epoch: 1,
        barrier_counter: end / 100,
    }
}

fn m9_proof(checkpoint: super::super::Checkpoint) -> DurableProof {
    super::super::checkpoint::proof_for_test(checkpoint)
}

#[test]
fn m9_committed_group_checks_both_modes() {
    let checkpoint = m9_checkpoint(100);
    for durability in [DurabilityMode::Durable, DurabilityMode::ReplaySafe] {
        for has_proof in [false, true] {
            let outcome = || CommitOutcome::Acknowledged {
                checkpoint,
                durable: has_proof.then(|| m9_proof(checkpoint)),
            };
            let result = committed_group(outcome(), Some(checkpoint.applied), durability);
            assert_eq!(
                result.is_ok(),
                has_proof || durability == DurabilityMode::ReplaySafe
            );
            let mut changed = checkpoint.applied;
            changed.tx.xid += 1;
            assert!(committed_group(outcome(), Some(changed), durability).is_err());
            assert!(committed_group(outcome(), None, durability).is_err());
        }
        assert!(
            committed_group(CommitOutcome::Unknown, Some(checkpoint.applied), durability).is_err()
        );
        assert!(
            committed_group(
                CommitOutcome::Acknowledged {
                    checkpoint,
                    durable: Some(m9_proof(m9_checkpoint(200))),
                },
                Some(checkpoint.applied),
                durability
            )
            .is_err()
        );
    }
}

#[test]
fn m9_idle_due_barrier() {
    let now = Instant::now();
    let due = now + Duration::from_millis(100);
    let mut pending = None;
    assert!(
        synchronize_pending(&mut pending, due, false, |_| panic!("no pending work"))
            .unwrap()
            .is_none()
    );
    note_async_commit(&mut pending, m9_checkpoint(200), due);
    assert!(
        synchronize_pending(&mut pending, now, false, |_| panic!("barrier too early"))
            .unwrap()
            .is_none()
    );
    let proof = synchronize_pending(&mut pending, due, false, |checkpoint| {
        assert_eq!(checkpoint.applied.end, SourceEndLsn(200));
        Ok(m9_proof(*checkpoint))
    })
    .unwrap()
    .unwrap();
    assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(200));
    assert!(pending.is_none());
}

#[test]
fn m9_continuous_input_due_at_commit() {
    let now = Instant::now();
    let due = group_deadline(&m9_settings(), None, now).unwrap().unwrap();
    let mut pending = None;
    note_async_commit(&mut pending, m9_checkpoint(100), due);
    note_async_commit(
        &mut pending,
        m9_checkpoint(200),
        due + Duration::from_millis(90),
    );
    assert_eq!(pending.as_ref().unwrap().due, due);
    assert_eq!(
        pending.as_ref().unwrap().checkpoint.applied.end,
        SourceEndLsn(200)
    );
    assert_eq!(
        group_deadline(
            &m9_settings(),
            pending.as_ref(),
            now + Duration::from_secs(1)
        )
        .unwrap(),
        Some(due)
    );
    assert!(!close_group_due(true, Some(due), due));
    assert!(close_group_due(false, Some(due), due));
    assert!(
        synchronize_pending(&mut pending, due, false, |checkpoint| Ok(m9_proof(
            *checkpoint
        )))
        .unwrap()
        .is_some()
    );
}

#[test]
fn m9_long_transaction_stays_whole() {
    let now = Instant::now();
    let due = now + Duration::from_millis(100);
    for elapsed in [100, 500, 1000] {
        assert!(!close_group_due(
            true,
            Some(due),
            now + Duration::from_millis(elapsed)
        ));
    }
    assert!(close_group_due(
        false,
        Some(due),
        now + Duration::from_secs(1)
    ));
    assert!(!close_group_due(false, None, now + Duration::from_secs(1)));
}

#[test]
fn m9_failed_barrier_retains_pending() {
    let due = Instant::now();
    let mut pending = None;
    note_async_commit(&mut pending, m9_checkpoint(200), due);
    for message in ["barrier failed", "barrier COMMIT outcome unknown"] {
        assert!(synchronize_pending(&mut pending, due, false, |_| Err(refusal(message))).is_err());
        assert_eq!(pending.as_ref().unwrap().checkpoint, m9_checkpoint(200));
        assert_eq!(pending.as_ref().unwrap().due, due);
    }
    assert!(
        synchronize_pending(&mut pending, due, false, |_| Ok(m9_proof(m9_checkpoint(
            100
        ))))
        .is_err()
    );
    assert!(pending.is_some());
    for field in 0..4 {
        let mut changed = m9_checkpoint(200);
        match field {
            0 => changed.stream.generation += 1,
            1 => changed.applied.tx.xid += 1,
            2 => changed.barrier_epoch += 1,
            _ => changed.barrier_counter = 1,
        }
        assert!(synchronize_pending(&mut pending, due, false, |_| Ok(m9_proof(changed))).is_err());
        assert_eq!(pending.as_ref().unwrap().checkpoint, m9_checkpoint(200));
        assert_eq!(pending.as_ref().unwrap().due, due);
    }
}

#[test]
fn m9_drain_requires_sync() {
    let now = Instant::now();
    let due = now + Duration::from_millis(100);
    let mut pending = None;
    let (feedback, proofs) = crate::follow::channel::feedback();
    let mut floor = SourceEndLsn(100);
    publish(m9_proof(m9_checkpoint(100)), &feedback, &mut floor).unwrap();
    assert_eq!(proofs.take().unwrap().end(), SourceEndLsn(100));
    note_async_commit(&mut pending, m9_checkpoint(200), due);
    assert!(proofs.take().is_none());
    assert_eq!(floor, SourceEndLsn(100));
    assert!(
        synchronize_pending(&mut pending, now, true, |_| Err(refusal(
            "drain COMMIT unknown"
        )))
        .is_err()
    );
    assert_eq!(floor, SourceEndLsn(100));
    assert!(proofs.take().is_none());
    assert!(clear_covered_pending(&mut pending, &m9_proof(m9_checkpoint(100))).is_err());
    assert!(pending.is_some());
    let proof = m9_proof(m9_checkpoint(200));
    clear_covered_pending(&mut pending, &proof).unwrap();
    publish(proof, &feedback, &mut floor).unwrap();
    assert_eq!(proofs.take().unwrap().end(), SourceEndLsn(200));
    assert_eq!(floor, SourceEndLsn(200));
    assert!(pending.is_none());
}
