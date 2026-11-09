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
fn idle_keepalives_ignore_stale_and_equal_positions_after_applied_commit() {
    let applied = frontier(begin(), commit(), &binding(), TxSeq(1)).unwrap();
    let mut floor = SourceEndLsn(16);
    let mut last = None;
    for candidate in [SourceEndLsn(16), SourceEndLsn(32), applied.end] {
        assert!(
            certify_keepalive(candidate, Some(applied), &mut floor, &mut last, |_| {
                panic!("stale or equal keepalive must not request certification")
            })
            .unwrap()
            .is_none()
        );
    }
    assert_eq!(floor, SourceEndLsn(16));
    assert!(last.is_none());
    let candidate = SourceEndLsn(64);
    let safe = certify_keepalive(
        candidate,
        Some(applied),
        &mut floor,
        &mut last,
        |requested| {
            assert_eq!(requested, candidate);
            Ok(super::super::checkpoint::feedback_for_test(
                binding().stream(),
                candidate,
            ))
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(safe.end(), candidate);
    assert_eq!(floor, candidate);
    assert!(last.is_some());
    last = None;
    for candidate in [applied.end, floor] {
        assert!(
            certify_keepalive(candidate, Some(applied), &mut floor, &mut last, |_| {
                panic!("previously proven position must not request certification")
            })
            .unwrap()
            .is_none()
        );
    }
}

#[test]
fn idle_keepalive_requires_positive_proof_before_advancing_floor_or_cadence() {
    let mut floor = SourceEndLsn(48);
    let mut last = None;
    assert!(
        certify_keepalive(SourceEndLsn(64), None, &mut floor, &mut last, |_| {
            Err(refusal("certification failed"))
        })
        .is_err()
    );
    assert_eq!(floor, SourceEndLsn(48));
    assert!(last.is_none());
    let candidate = SourceEndLsn(64);
    assert!(
        certify_keepalive(candidate, None, &mut floor, &mut last, |_| {
            Ok(super::super::checkpoint::feedback_for_test(
                binding().stream(),
                candidate,
            ))
        })
        .unwrap()
        .is_some()
    );
    assert!(
        certify_keepalive(SourceEndLsn(80), None, &mut floor, &mut last, |_| {
            panic!("certification cadence must remain bounded")
        })
        .unwrap()
        .is_none()
    );
    assert_eq!(floor, candidate);
    last = Some(Instant::now() - CERTIFY_INTERVAL);
    let next = SourceEndLsn(80);
    assert!(
        certify_keepalive(next, None, &mut floor, &mut last, |_| {
            Ok(super::super::checkpoint::feedback_for_test(
                binding().stream(),
                next,
            ))
        })
        .unwrap()
        .is_some()
    );
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
