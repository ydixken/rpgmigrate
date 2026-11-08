use super::*;
use crate::cdc::codec::{COMPLETED_BYTES, HEADER_BYTES};
use crate::cdc::contracts::{Column, Value};
use crate::cdc::event_codec::encode_event;
use crate::identity::{RunId, SourceEndLsn, SourceId, StreamId, TxId, TxSeq};
use std::panic::{AssertUnwindSafe, catch_unwind};

fn budgets() -> SerialBudgets {
    SerialBudgets {
        frame_heap_bytes: 4096,
        reassembly_heap_bytes: 4096,
        decoded_heap_bytes: 4096,
        relation_slots: 4,
        relation_bytes: 16_384,
        plan_slots: 4,
        plan_bytes: 16_384,
        bind_bytes: 4096,
    }
}

fn wire() -> codec::Limits {
    codec::Limits {
        max_frame_bytes: 4096,
        max_event_bytes: 4096,
        max_parts: 64,
    }
}

fn tx() -> CompletedTx {
    CompletedTx {
        tx: TxId {
            stream: StreamId {
                run: RunId([3; 16]),
                source: SourceId {
                    system_identifier: 13,
                    database_oid: 17,
                },
                generation: 9,
            },
            xid: 19,
            begin_ordinal: 23,
        },
        order: TxSeq(1),
        commit: None,
        end: SourceEndLsn(100),
        event_count: 0,
        encoded_bytes: COMPLETED_BYTES as u64,
    }
}

fn relation() -> Event {
    Event::Relation {
        key: RelationKey {
            oid: 31,
            generation: 7,
        },
        schema: "public".into(),
        name: "serial_data".into(),
        columns: vec![
            Column {
                name: "id".into(),
                type_oid: 23,
                type_mod: -1,
                key: true,
            },
            Column {
                name: "v".into(),
                type_oid: 25,
                type_mod: -1,
                key: false,
            },
        ],
        replica_identity: b'd',
    }
}

fn insert() -> Event {
    Event::Insert {
        relation: RelationKey {
            oid: 31,
            generation: 7,
        },
        values: vec![Value::Text(b"1".to_vec()), Value::Null],
    }
}

fn framed(event: &Event) -> (Frame, CompletedTx) {
    let payload = encode_event(event, wire()).unwrap();
    let completed = CompletedTx {
        event_count: 1,
        encoded_bytes: (COMPLETED_BYTES + HEADER_BYTES + payload.len()) as u64,
        ..tx()
    };
    (
        Frame {
            tx: completed.tx,
            event_ordinal: 0,
            part: 0,
            last_part: true,
            payload,
        },
        completed,
    )
}

#[test]
fn complete_single_part_event_is_borrowed_and_counted() {
    let (frame, completed) = framed(&insert());
    let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
    let mut seen = 0;
    replay
        .visit(&Record::Frame(frame), &mut |event, charged| {
            assert_eq!(event, &insert());
            assert!(charged > 2 * size_of::<Value>());
            seen += 1;
            Ok(())
        })
        .unwrap();
    replay
        .visit(&Record::Completed(completed), &mut |_, _| {
            panic!("descriptor decoded as event")
        })
        .unwrap();
    replay.finish().unwrap();
    assert_eq!(seen, 1);
}

#[test]
fn multipart_event_decodes_only_after_its_final_part() {
    let (frame, mut completed) = framed(&relation());
    completed.encoded_bytes += HEADER_BYTES as u64;
    let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
    let split = frame.payload.len() / 2;
    let mut seen = 0;
    let mut visit = |event: &Event, _: usize| {
        assert_eq!(event, &relation());
        seen += 1;
        Ok(())
    };
    replay
        .visit(
            &Record::Frame(Frame {
                payload: frame.payload[..split].to_vec(),
                last_part: false,
                ..frame.clone()
            }),
            &mut visit,
        )
        .unwrap();
    replay
        .visit(
            &Record::Frame(Frame {
                payload: frame.payload[split..].to_vec(),
                part: 1,
                ..frame
            }),
            &mut visit,
        )
        .unwrap();
    replay
        .visit(&Record::Completed(completed), &mut visit)
        .unwrap();
    replay.finish().unwrap();
    assert_eq!(seen, 1);
}

#[test]
fn missing_descriptor_and_incomplete_parts_refuse_completion() {
    let (mut frame, completed) = framed(&insert());
    let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
    replay
        .visit(&Record::Frame(frame.clone()), &mut |_, _| Ok(()))
        .unwrap();
    assert!(replay.finish().is_err());
    frame.last_part = false;
    let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
    replay
        .visit(&Record::Frame(frame), &mut |_, _| Ok(()))
        .unwrap();
    assert!(
        replay
            .visit(&Record::Completed(completed), &mut |_, _| Ok(()))
            .is_err()
    );
    assert!(replay.finish().is_err());
}

#[test]
fn wrong_frame_identity_ordinal_or_part_poison_the_input() {
    let (frame, completed) = framed(&insert());
    for mutation in 0..5 {
        let mut changed = frame.clone();
        match mutation {
            0 => changed.tx.stream.generation += 1,
            1 => changed.tx.xid += 1,
            2 => changed.tx.begin_ordinal += 1,
            3 => changed.event_ordinal += 1,
            _ => changed.part += 1,
        }
        let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
        assert!(
            replay
                .visit(&Record::Frame(changed), &mut |_, _| panic!(
                    "invalid frame decoded"
                ))
                .is_err()
        );
        assert!(
            replay
                .visit(&Record::Frame(frame.clone()), &mut |_, _| Ok(()))
                .is_err()
        );
        assert!(replay.finish().is_err());
    }
}

#[test]
fn full_terminal_descriptor_must_match_the_external_expectation() {
    let (frame, completed) = framed(&insert());
    for mutation in 0..7 {
        let mut changed = completed;
        match mutation {
            0 => changed.tx.begin_ordinal += 1,
            1 => changed.tx.stream.source.database_oid += 1,
            2 => changed.order.0 += 1,
            3 => changed.commit = Some(crate::identity::CommitLsn(0)),
            4 => changed.end.0 += 1,
            5 => changed.event_count += 1,
            _ => changed.encoded_bytes += 1,
        }
        let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
        replay
            .visit(&Record::Frame(frame.clone()), &mut |_, _| Ok(()))
            .unwrap();
        assert!(
            replay
                .visit(&Record::Completed(changed), &mut |_, _| Ok(()))
                .is_err()
        );
        assert!(replay.finish().is_err());
    }
}

#[test]
fn matching_but_false_descriptor_totals_are_independently_rejected() {
    let (frame, completed) = framed(&insert());
    for bad in [
        CompletedTx {
            event_count: 2,
            ..completed
        },
        CompletedTx {
            encoded_bytes: completed.encoded_bytes + 1,
            ..completed
        },
    ] {
        let mut replay = Replay::new(bad, wire(), budgets()).unwrap();
        replay
            .visit(&Record::Frame(frame.clone()), &mut |_, _| Ok(()))
            .unwrap();
        assert!(
            replay
                .visit(&Record::Completed(bad), &mut |_, _| Ok(()))
                .is_err()
        );
    }
}

#[test]
fn no_records_are_allowed_after_the_terminal_descriptor() {
    let (frame, completed) = framed(&insert());
    for extra in [Record::Frame(frame.clone()), Record::Completed(completed)] {
        let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
        replay
            .visit(&Record::Frame(frame.clone()), &mut |_, _| Ok(()))
            .unwrap();
        replay
            .visit(&Record::Completed(completed), &mut |_, _| Ok(()))
            .unwrap();
        assert!(
            replay
                .visit(&extra, &mut |_, _| panic!("trailing event decoded"))
                .is_err()
        );
        assert!(replay.finish().is_err());
    }
}

#[test]
fn caught_handler_error_or_panic_cannot_be_swallowed_into_completion() {
    let (frame, completed) = framed(&insert());
    for panics in [false, true] {
        let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
        let caught = catch_unwind(AssertUnwindSafe(|| {
            replay.visit(&Record::Frame(frame.clone()), &mut |_, _| {
                if panics {
                    panic!("injected handler panic");
                }
                Err(refusal("injected handler error"))
            })
        }));
        if panics {
            assert!(caught.is_err());
        } else {
            assert!(caught.unwrap().is_err());
        }
        assert!(
            replay
                .visit(&Record::Completed(completed), &mut |_, _| Ok(()))
                .is_err()
        );
        assert!(replay.finish().is_err());
    }
}

#[test]
fn decode_heap_limit_is_independent_of_wire_bytes() {
    let event = Event::Insert {
        relation: RelationKey {
            oid: 31,
            generation: 7,
        },
        values: vec![Value::Null; 100],
    };
    let (frame, completed) = framed(&event);
    let mut limits = budgets();
    limits.decoded_heap_bytes = 100;
    let mut replay = Replay::new(completed, wire(), limits).unwrap();
    assert!(
        replay
            .visit(&Record::Frame(frame), &mut |_, _| panic!(
                "unbudgeted event admitted"
            ))
            .is_err()
    );
    assert!(replay.finish().is_err());
}

#[test]
fn raw_frame_capacity_is_charged_even_when_payload_is_small() {
    let (mut frame, completed) = framed(&insert());
    frame.payload.reserve_exact(2048);
    let mut limits = budgets();
    limits.frame_heap_bytes = 1024;
    let mut replay = Replay::new(completed, wire(), limits).unwrap();
    assert!(
        replay
            .visit(&Record::Frame(frame), &mut |_, _| panic!(
                "oversize capacity decoded"
            ))
            .is_err()
    );
}

#[test]
fn multipart_reassembly_has_its_own_capacity_limit() {
    let (frame, mut completed) = framed(&relation());
    completed.encoded_bytes += HEADER_BYTES as u64;
    let mut limits = budgets();
    let split = frame.payload.len() / 2;
    limits.reassembly_heap_bytes = split;
    let mut replay = Replay::new(completed, wire(), limits).unwrap();
    replay
        .visit(
            &Record::Frame(Frame {
                payload: frame.payload[..split].to_vec(),
                last_part: false,
                ..frame.clone()
            }),
            &mut |_, _| panic!("partial event decoded"),
        )
        .unwrap();
    assert!(
        replay
            .visit(
                &Record::Frame(Frame {
                    payload: frame.payload[split..].to_vec(),
                    part: 1,
                    ..frame
                }),
                &mut |_, _| panic!("over-budget event decoded")
            )
            .is_err()
    );
}

#[test]
fn empty_transaction_requires_a_real_descriptor() {
    let completed = tx();
    let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
    replay
        .visit(&Record::Completed(completed), &mut |_, _| {
            panic!("empty transaction emitted an event")
        })
        .unwrap();
    replay.finish().unwrap();
}

#[test]
fn exact_dml_command_tags_require_one_row() {
    for (event, accepted) in [
        (insert(), "INSERT 0 1"),
        (
            Event::Update {
                relation: RelationKey {
                    oid: 31,
                    generation: 7,
                },
                old: None,
                new: vec![],
            },
            "UPDATE 1",
        ),
        (
            Event::Delete {
                relation: RelationKey {
                    oid: 31,
                    generation: 7,
                },
                old: vec![],
            },
            "DELETE 1",
        ),
    ] {
        require_one_row(&event, accepted).unwrap();
        for rejected in [
            "",
            "INSERT 0 0",
            "INSERT 0 2",
            "UPDATE 0",
            "UPDATE 2",
            "DELETE 0",
            "DELETE 2",
            "COPY 1",
            "UPDATE 1 ",
        ] {
            assert!(require_one_row(&event, rejected).is_err(), "{rejected}");
        }
    }
}

#[test]
fn malformed_event_payloads_fail_before_the_handler() {
    let (frame, completed) = framed(&insert());
    for length in [0, 1, frame.payload.len() - 1] {
        let payload = frame.payload[..length].to_vec();
        let completed = CompletedTx {
            encoded_bytes: (COMPLETED_BYTES + HEADER_BYTES + payload.len()) as u64,
            ..completed
        };
        let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
        assert!(
            replay
                .visit(
                    &Record::Frame(Frame {
                        payload,
                        ..frame.clone()
                    }),
                    &mut |_, _| panic!("malformed event admitted")
                )
                .is_err()
        );
        assert!(replay.finish().is_err());
    }
}

#[test]
fn zero_sequence_and_commit_beyond_end_cannot_finish() {
    for completed in [
        CompletedTx {
            order: TxSeq(0),
            ..tx()
        },
        CompletedTx {
            commit: Some(crate::identity::CommitLsn(101)),
            ..tx()
        },
    ] {
        let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
        assert!(
            replay
                .visit(&Record::Completed(completed), &mut |_, _| Ok(()))
                .is_err()
        );
    }
}

#[test]
fn resource_configuration_rejects_zeroes_and_backend_statement_overflow() {
    for field in 0..9 {
        let mut limits = budgets();
        match field {
            0 => limits.frame_heap_bytes = 0,
            1 => limits.reassembly_heap_bytes = 0,
            2 => limits.decoded_heap_bytes = 0,
            3 => limits.relation_slots = 0,
            4 => limits.relation_bytes = 0,
            5 => limits.plan_slots = 0,
            6 => limits.plan_bytes = 0,
            7 => limits.bind_bytes = 0,
            _ => limits.plan_slots = 1025,
        }
        assert!(limits.validate().is_err());
    }
}

#[test]
fn cache_inline_storage_counts_against_the_byte_caps() {
    let mut limits = budgets();
    limits.relation_bytes = 1;
    assert!(Caches::new(limits).is_err());
    limits = budgets();
    limits.plan_bytes = 1;
    assert!(Caches::new(limits).is_err());
}

#[test]
fn relation_storage_counts_actual_capacities_including_column_names() {
    let mut event = relation();
    let original = relation_heap_bytes(&event).unwrap();
    let Event::Relation {
        schema, columns, ..
    } = &mut event
    else {
        unreachable!()
    };
    schema.reserve_exact(1024);
    columns[0].name.reserve_exact(512);
    columns.reserve_exact(8);
    assert!(
        relation_heap_bytes(&event).unwrap() >= original + 1024 + 512 + 8 * size_of::<Column>()
    );
}

fn cached_plan(caches: &mut Caches, row: &Event) -> usize {
    let ordinal = caches.plans.len() as u64;
    let plan = DmlPlan::new(&relation(), row).unwrap();
    let name = format!("test_{ordinal}");
    caches.plan_heap += plan.owned_capacity_bytes().unwrap() + name.capacity();
    let index = caches.plans.len();
    caches.plans.push(CachedPlan {
        relation: row_relation(row).unwrap(),
        plan,
        name,
        ordinal,
    });
    index
}

#[test]
fn cache_hot_path_reuses_sql_and_fallback_distinguishes_toast_shapes() {
    let mut caches = Caches::new(budgets()).unwrap();
    let first = cached_plan(&mut caches, &insert());
    let key = row_relation(&insert()).unwrap();
    let partial = Event::Update {
        relation: key,
        old: None,
        new: vec![Value::Text(b"1".to_vec()), Value::UnchangedToast],
    };
    let full = Event::Update {
        relation: key,
        old: None,
        new: vec![Value::Text(b"1".to_vec()), Value::Null],
    };
    let partial_index = cached_plan(&mut caches, &partial);
    let full_index = cached_plan(&mut caches, &full);
    let sql = caches.plans[first].plan.sql().as_ptr();
    let retained = caches.plan_heap;
    for row in [insert(), insert(), full, partial, insert()] {
        let expected = match &row {
            Event::Insert { .. } => first,
            Event::Update { new, .. } if matches!(new[1], Value::UnchangedToast) => partial_index,
            _ => full_index,
        };
        assert_eq!(caches.find_plan(key, &row).unwrap(), Some(expected));
        assert_eq!(caches.hot, Some(expected));
    }
    assert_eq!(caches.plans.len(), 3);
    assert_eq!(caches.plan_heap, retained);
    assert_eq!(caches.plans[first].plan.sql().as_ptr(), sql);
    let wrong = RelationKey {
        generation: 8,
        ..key
    };
    assert_eq!(caches.find_plan(wrong, &insert()).unwrap(), None);
}

#[test]
fn cache_requires_new_transaction_relation_checks() {
    let mut caches = Caches::new(budgets()).unwrap();
    caches.relations.push(CachedRelation {
        event: relation(),
        checked: true,
    });
    cached_plan(&mut caches, &insert());
    caches.hot = Some(0);
    caches.begin();
    assert!(!caches.relations[0].checked);
    assert_eq!(caches.hot, Some(0));
    assert_eq!(caches.plans.len(), 1);
}

#[test]
fn transaction_wire_size_can_exceed_all_event_buffers_without_accumulation() {
    let (mut frame, completed) = framed(&insert());
    let count = 10_000;
    let completed = CompletedTx {
        event_count: count,
        encoded_bytes: COMPLETED_BYTES as u64 + count * (HEADER_BYTES + frame.payload.len()) as u64,
        ..completed
    };
    assert!(completed.encoded_bytes > budgets().reassembly_heap_bytes as u64);
    let mut replay = Replay::new(completed, wire(), budgets()).unwrap();
    let mut seen = 0;
    for ordinal in 0..count {
        frame.event_ordinal = ordinal;
        replay
            .visit(&Record::Frame(frame.clone()), &mut |_, _| {
                seen += 1;
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(replay.parts.capacity(), 0);
    replay
        .visit(&Record::Completed(completed), &mut |_, _| Ok(()))
        .unwrap();
    replay.finish().unwrap();
    assert_eq!(seen, count);
}
