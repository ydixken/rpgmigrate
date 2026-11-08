use super::*;
use crate::cdc::contracts::{Column, RelationKey};
use crate::cdc::event_codec::decode_event;
use crate::identity::{RunId, SourceEndLsn, SourceId};
use crate::pg::owned_frame;

fn stream() -> StreamId {
    StreamId {
        run: RunId([7; 16]),
        source: SourceId {
            system_identifier: 123,
            database_oid: 456,
        },
        generation: 9,
    }
}

fn decoder(limits: Limits) -> Decoder {
    Decoder::new(
        Plugin::TestDecoding,
        stream(),
        limits,
        DecoderPosition {
            next_begin_ordinal: 71,
            next_order: TxSeq(4),
        },
    )
    .unwrap()
}

fn input(start: u64, payload: &str) -> ReplicationFrame {
    let mut bytes = vec![b'w'];
    bytes.extend(start.to_be_bytes());
    bytes.extend(u64::MAX.to_be_bytes());
    bytes.extend(0_i64.to_be_bytes());
    bytes.extend(payload.as_bytes());
    owned_frame(&bytes)
}

fn keepalive() -> ReplicationFrame {
    let mut bytes = vec![b'k'];
    bytes.extend(u64::MAX.to_be_bytes());
    bytes.extend(0_i64.to_be_bytes());
    bytes.push(1);
    owned_frame(&bytes)
}

struct Relations(Vec<Event>);

impl Relations {
    fn new() -> Self {
        Self(
            [("data", 42), ("other", 43)]
                .map(|(name, oid)| Event::Relation {
                    key: RelationKey { oid, generation: 7 },
                    schema: "public".into(),
                    name: name.into(),
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
                })
                .into(),
        )
    }
}

impl RelationResolver for Relations {
    fn resolve(&mut self, table: &QualifiedTable) -> Result<&Event> {
        self.0.iter().find(|event| matches!(event, Event::Relation { schema, name, .. } if *schema == table.schema && *name == table.table)).ok_or_else(|| Error { kind: ErrorKind::BadState, message: "fixture relation is missing".into() })
    }
}

fn feed(
    decoder: &mut Decoder,
    relations: &mut impl RelationResolver,
    frames: &mut Vec<Frame>,
    start: u64,
    payload: &str,
) -> Result<()> {
    decoder.feed(&input(start, payload), relations, &mut |frame| {
        frames.push(frame.clone());
        Ok(())
    })
}

fn events(frames: &[Frame], limits: Limits) -> Vec<Event> {
    let mut bytes = Vec::new();
    let mut events = Vec::new();
    for frame in frames {
        bytes.extend_from_slice(&frame.payload);
        if frame.last_part {
            events.push(decode_event(&bytes, limits).unwrap());
            bytes.clear();
        }
    }
    assert!(bytes.is_empty());
    events
}

#[test]
fn committed_transaction_preserves_every_same_lsn_row_and_terminal_start() {
    let limits = Limits::default();
    let mut decoder = decoder(limits);
    let mut relations = Relations::new();
    let mut frames = Vec::new();
    feed(&mut decoder, &mut relations, &mut frames, 500, "BEGIN 9").unwrap();
    for _ in 0..3 {
        feed(
            &mut decoder,
            &mut relations,
            &mut frames,
            1,
            "table public.data: INSERT: id[integer]:1 v[text]:'COMMIT 9'",
        )
        .unwrap();
    }
    decoder
        .feed(&keepalive(), &mut relations, &mut |_| {
            panic!("keepalive emitted")
        })
        .unwrap();
    assert!(decoder.take_completed().is_none());
    feed(&mut decoder, &mut relations, &mut frames, 600, "COMMIT 9").unwrap();
    let completed = decoder.take_completed().unwrap();
    assert_eq!(completed.tx.xid, 9);
    assert_eq!(completed.tx.begin_ordinal, 71);
    assert_eq!(completed.tx.stream, stream());
    assert_eq!(completed.order, TxSeq(4));
    assert_eq!(completed.commit, None);
    assert_eq!(completed.end, SourceEndLsn(600));
    assert_eq!(completed.event_count, 4);
    assert_eq!(
        completed.encoded_bytes,
        128 + frames
            .iter()
            .map(|frame| 88 + frame.payload.len() as u64)
            .sum::<u64>()
    );
    let got = events(&frames, limits);
    assert_eq!(got.len(), 4);
    assert!(matches!(got[0], Event::Relation { .. }));
    assert!(
        got[1..]
            .iter()
            .all(|event| matches!(event, Event::Insert { .. }))
    );
    for (ordinal, frame) in frames.iter().enumerate() {
        assert_eq!(frame.event_ordinal, ordinal as u64);
    }
}

#[test]
fn empty_transactions_and_reused_xids_keep_explicit_seed_identity() {
    let mut decoder = decoder(Limits::default());
    let mut relations = Relations::new();
    let mut frames = Vec::new();
    for (ordinal, order) in [(71, 4), (72, 5)] {
        feed(&mut decoder, &mut relations, &mut frames, 10, "BEGIN 9").unwrap();
        feed(&mut decoder, &mut relations, &mut frames, 10, "COMMIT 9").unwrap();
        let tx = decoder.take_completed().unwrap();
        assert_eq!(
            (
                tx.tx.begin_ordinal,
                tx.order.0,
                tx.event_count,
                tx.encoded_bytes
            ),
            (ordinal, order, 0, 128)
        );
    }
    assert!(frames.is_empty());
}

#[test]
fn relation_context_is_bounded_to_one_last_key_and_resets_per_transaction() {
    let mut decoder = decoder(Limits::default());
    let mut relations = Relations::new();
    let mut frames = Vec::new();
    feed(&mut decoder, &mut relations, &mut frames, 1, "BEGIN 3").unwrap();
    for table in ["data", "data", "other", "other", "data"] {
        feed(
            &mut decoder,
            &mut relations,
            &mut frames,
            1,
            &format!("table public.{table}: INSERT: id[integer]:1 v[text]:'x'"),
        )
        .unwrap();
    }
    feed(&mut decoder, &mut relations, &mut frames, 2, "COMMIT 3").unwrap();
    let tx = decoder.take_completed().unwrap();
    assert_eq!(tx.event_count, 8);
    let got = events(&frames, Limits::default());
    let tags: Vec<_> = got
        .iter()
        .map(|event| match event {
            Event::Relation { key, .. } => (b'R', key.oid),
            Event::Insert { relation, .. } => (b'I', relation.oid),
            _ => panic!(),
        })
        .collect();
    assert_eq!(
        tags,
        [
            (b'R', 42),
            (b'I', 42),
            (b'I', 42),
            (b'R', 43),
            (b'I', 43),
            (b'I', 43),
            (b'R', 42),
            (b'I', 42)
        ]
    );
    frames.clear();
    feed(&mut decoder, &mut relations, &mut frames, 2, "BEGIN 4").unwrap();
    feed(
        &mut decoder,
        &mut relations,
        &mut frames,
        2,
        "table public.data: DELETE: id[integer]:1",
    )
    .unwrap();
    assert!(matches!(
        events(&frames, Limits::default())[0],
        Event::Relation { .. }
    ));
}

#[test]
fn completion_backpressure_keeps_one_descriptor_and_accepts_keepalive() {
    let mut decoder = decoder(Limits::default());
    let mut relations = Relations::new();
    let mut frames = Vec::new();
    feed(&mut decoder, &mut relations, &mut frames, 1, "BEGIN 3").unwrap();
    feed(&mut decoder, &mut relations, &mut frames, 2, "COMMIT 3").unwrap();
    decoder
        .feed(&keepalive(), &mut relations, &mut |_| Ok(()))
        .unwrap();
    assert!(feed(&mut decoder, &mut relations, &mut frames, 2, "BEGIN 4").is_err());
    assert_eq!(decoder.take_completed().unwrap().tx.xid, 3);
    assert!(decoder.take_completed().is_none());
    feed(&mut decoder, &mut relations, &mut frames, 2, "BEGIN 4").unwrap();
}

#[test]
fn split_relation_finishes_before_dependent_row_parts() {
    let limits = Limits {
        max_frame_bytes: 128,
        max_event_bytes: 4096,
        max_parts: 128,
    };
    let mut decoder = decoder(limits);
    let mut relations = Relations::new();
    let mut frames = Vec::new();
    feed(&mut decoder, &mut relations, &mut frames, 1, "BEGIN 3").unwrap();
    feed(
        &mut decoder,
        &mut relations,
        &mut frames,
        1,
        &format!(
            "table public.data: INSERT: id[integer]:1 v[text]:'{}'",
            "x".repeat(200)
        ),
    )
    .unwrap();
    assert!(frames.len() > 5);
    assert!(frames.iter().all(|frame| frame.payload.len() + 88 <= 128));
    let row_start = frames
        .iter()
        .position(|frame| frame.event_ordinal == 1)
        .unwrap();
    assert!(frames[row_start - 1].last_part);
    for parts in [&frames[..row_start], &frames[row_start..]] {
        for (part, frame) in parts.iter().enumerate() {
            assert_eq!(frame.part, part as u32);
        }
        assert!(parts.last().unwrap().last_part);
    }
    assert_eq!(events(&frames, limits).len(), 2);
}

fn assert_poisoned(decoder: &mut Decoder, relations: &mut impl RelationResolver) {
    assert!(decoder.take_completed().is_none());
    assert!(
        decoder
            .feed(&input(99, "COMMIT 3"), relations, &mut |_| panic!(
                "poisoned decoder emitted"
            ))
            .is_err()
    );
    assert!(decoder.take_completed().is_none());
    assert!(
        decoder
            .feed(&input(99, "BEGIN 4"), relations, &mut |_| panic!(
                "poisoned decoder restarted"
            ))
            .is_err()
    );
}

#[test]
fn malformed_or_unsupported_transaction_messages_poison_without_completion() {
    for message in [
        "COMMIT 4",
        "COMMIT 3 (at now)",
        "COMMIT 3\n",
        "COMMIT 3\nBEGIN 4",
        "COMMIT 0",
        "BEGIN 4",
        "ROLLBACK 3",
        "PREPARE TRANSACTION 'x', txid 3",
        "opening a streamed block for transaction TXN 3",
        "message: transactional: 1 prefix: x, sz: 1 content:y",
        "table public.data: INSERT: id[integer]:1 v[text]:'unterminated",
        "table public.data: DELETE: (no-tuple-data)",
    ] {
        let mut decoder = decoder(Limits::default());
        let mut relations = Relations::new();
        let mut frames = Vec::new();
        feed(&mut decoder, &mut relations, &mut frames, 1, "BEGIN 3").unwrap();
        feed(
            &mut decoder,
            &mut relations,
            &mut frames,
            1,
            "table public.data: INSERT: id[integer]:1 v[text]:'valid prefix'",
        )
        .unwrap();
        assert!(
            feed(&mut decoder, &mut relations, &mut frames, 2, message).is_err(),
            "{message}"
        );
        assert_poisoned(&mut decoder, &mut relations);
    }
}

#[test]
fn rows_and_commits_without_begin_are_refused() {
    for message in [
        "COMMIT 3",
        "table public.data: INSERT: id[integer]:1 v[text]:'x'",
        "table public.data: TRUNCATE: (no-flags)",
    ] {
        let mut decoder = decoder(Limits::default());
        let mut relations = Relations::new();
        assert!(
            decoder
                .feed(&input(1, message), &mut relations, &mut |_| panic!(
                    "idle decoder emitted"
                ))
                .is_err()
        );
        assert_poisoned(&mut decoder, &mut relations);
    }
}

#[test]
fn sink_failure_after_any_relation_or_row_part_cannot_publish_completion() {
    let limits = Limits {
        max_frame_bytes: 128,
        max_event_bytes: 4096,
        max_parts: 128,
    };
    for fail_at in 0..8 {
        let mut decoder = decoder(limits);
        let mut relations = Relations::new();
        decoder
            .feed(&input(1, "BEGIN 3"), &mut relations, &mut |_| Ok(()))
            .unwrap();
        let mut calls = 0;
        let error = decoder
            .feed(
                &input(
                    1,
                    &format!(
                        "table public.data: INSERT: id[integer]:1 v[text]:'{}'",
                        "x".repeat(200)
                    ),
                ),
                &mut relations,
                &mut |_| {
                    let fail = calls == fail_at;
                    calls += 1;
                    if fail {
                        Err(Error {
                            kind: ErrorKind::Target,
                            message: "injected disk failure".into(),
                        })
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap_err();
        assert_eq!(
            (error.kind, error.message.as_str()),
            (ErrorKind::Target, "injected disk failure")
        );
        assert_eq!(calls, fail_at + 1);
        assert_poisoned(&mut decoder, &mut relations);
    }
}

#[test]
fn resolver_errors_keep_their_category_and_wrong_context_is_rejected() {
    struct Failing;
    impl RelationResolver for Failing {
        fn resolve(&mut self, _: &QualifiedTable) -> Result<&Event> {
            Err(Error {
                kind: ErrorKind::Reload,
                message: "injected catalog gap".into(),
            })
        }
    }
    let mut decoder = decoder(Limits::default());
    let mut failing = Failing;
    decoder
        .feed(&input(1, "BEGIN 3"), &mut failing, &mut |_| Ok(()))
        .unwrap();
    let error = decoder
        .feed(
            &input(1, "table public.data: DELETE: id[integer]:1"),
            &mut failing,
            &mut |_| panic!(),
        )
        .unwrap_err();
    assert_eq!(
        (error.kind, error.message.as_str()),
        (ErrorKind::Reload, "injected catalog gap")
    );
    assert_poisoned(&mut decoder, &mut failing);
    struct Wrong(Event);
    impl RelationResolver for Wrong {
        fn resolve(&mut self, _: &QualifiedTable) -> Result<&Event> {
            Ok(&self.0)
        }
    }
    let mut decoder = super::tests::decoder(Limits::default());
    let mut wrong = Wrong(Event::Insert {
        relation: RelationKey {
            oid: 42,
            generation: 7,
        },
        values: vec![],
    });
    decoder
        .feed(&input(1, "BEGIN 3"), &mut wrong, &mut |_| Ok(()))
        .unwrap();
    assert!(
        decoder
            .feed(
                &input(1, "table public.data: DELETE: id[integer]:1"),
                &mut wrong,
                &mut |_| panic!()
            )
            .is_err()
    );
    assert_poisoned(&mut decoder, &mut wrong);
}

#[test]
fn checked_restart_seed_overflow_cannot_create_a_completion() {
    for (begin, order, terminal) in [(u64::MAX, 4, false), (71, u64::MAX, true)] {
        let mut decoder = Decoder::new(
            Plugin::TestDecoding,
            stream(),
            Limits::default(),
            DecoderPosition {
                next_begin_ordinal: begin,
                next_order: TxSeq(order),
            },
        )
        .unwrap();
        let mut relations = Relations::new();
        let result = decoder.feed(&input(1, "BEGIN 3"), &mut relations, &mut |_| Ok(()));
        if terminal {
            result.unwrap();
            assert!(
                decoder
                    .feed(&input(2, "COMMIT 3"), &mut relations, &mut |_| panic!())
                    .is_err()
            );
        } else {
            assert!(result.is_err());
        }
        assert_poisoned(&mut decoder, &mut relations);
    }
}

#[test]
fn event_count_and_wire_byte_overflow_fail_before_sink_emission() {
    for event_count in [true, false] {
        let mut decoder = decoder(Limits::default());
        let mut relations = Relations::new();
        decoder
            .feed(&input(1, "BEGIN 3"), &mut relations, &mut |_| Ok(()))
            .unwrap();
        if event_count {
            decoder.active.as_mut().unwrap().event_count = u64::MAX;
        } else {
            decoder.active.as_mut().unwrap().encoded_bytes = u64::MAX;
        }
        assert!(
            decoder
                .feed(
                    &input(1, "table public.data: DELETE: id[integer]:1"),
                    &mut relations,
                    &mut |_| panic!("overflow reached sink")
                )
                .is_err()
        );
        assert_poisoned(&mut decoder, &mut relations);
    }
}

#[test]
fn invalid_plugin_sequence_and_shared_limits_are_refused() {
    for plugin in [Plugin::Pgoutput, Plugin::Wal2Json] {
        assert!(
            Decoder::new(
                plugin,
                stream(),
                Limits::default(),
                DecoderPosition {
                    next_begin_ordinal: 1,
                    next_order: TxSeq(1)
                }
            )
            .is_err()
        );
    }
    assert!(
        Decoder::new(
            Plugin::TestDecoding,
            stream(),
            Limits::default(),
            DecoderPosition {
                next_begin_ordinal: 1,
                next_order: TxSeq(0)
            }
        )
        .is_err()
    );
    for limits in [
        Limits {
            max_frame_bytes: 127,
            ..Limits::default()
        },
        Limits {
            max_event_bytes: 0,
            ..Limits::default()
        },
        Limits {
            max_parts: 0,
            ..Limits::default()
        },
    ] {
        assert!(
            Decoder::new(
                Plugin::TestDecoding,
                stream(),
                limits,
                DecoderPosition {
                    next_begin_ordinal: 1,
                    next_order: TxSeq(1)
                }
            )
            .is_err()
        );
    }
}

#[test]
fn decreasing_terminal_end_fails_but_prior_row_positions_are_not_filtered() {
    let mut decoder = decoder(Limits::default());
    let mut relations = Relations::new();
    let mut frames = Vec::new();
    feed(&mut decoder, &mut relations, &mut frames, 10, "BEGIN 3").unwrap();
    feed(&mut decoder, &mut relations, &mut frames, 20, "COMMIT 3").unwrap();
    decoder.take_completed().unwrap();
    feed(&mut decoder, &mut relations, &mut frames, 1, "BEGIN 4").unwrap();
    feed(
        &mut decoder,
        &mut relations,
        &mut frames,
        1,
        "table public.data: DELETE: id[integer]:1",
    )
    .unwrap();
    assert_eq!(frames.len(), 2);
    assert!(feed(&mut decoder, &mut relations, &mut frames, 19, "COMMIT 4").is_err());
    assert_poisoned(&mut decoder, &mut relations);
}

#[test]
fn truncate_contexts_precede_all_dependencies_and_keep_last_key_correct() {
    let mut decoder = decoder(Limits::default());
    let mut relations = Relations::new();
    let mut frames = Vec::new();
    feed(&mut decoder, &mut relations, &mut frames, 1, "BEGIN 3").unwrap();
    feed(
        &mut decoder,
        &mut relations,
        &mut frames,
        1,
        "table public.data: DELETE: id[integer]:1",
    )
    .unwrap();
    feed(
        &mut decoder,
        &mut relations,
        &mut frames,
        1,
        "table public.data, public.other: TRUNCATE: restart_seqs cascade",
    )
    .unwrap();
    feed(
        &mut decoder,
        &mut relations,
        &mut frames,
        1,
        "table public.other: DELETE: id[integer]:1",
    )
    .unwrap();
    feed(
        &mut decoder,
        &mut relations,
        &mut frames,
        1,
        "table public.data: DELETE: id[integer]:2",
    )
    .unwrap();
    let got = events(&frames, Limits::default());
    assert_eq!(got.len(), 7);
    assert!(matches!(
        got[0],
        Event::Relation {
            key: RelationKey { oid: 42, .. },
            ..
        }
    ));
    assert!(matches!(
        got[2],
        Event::Relation {
            key: RelationKey { oid: 43, .. },
            ..
        }
    ));
    assert!(
        matches!(&got[3],Event::Truncate { relations,cascade:true,restart_identity:true } if relations == &[RelationKey { oid:42,generation:7 },RelationKey { oid:43,generation:7 }])
    );
    assert!(matches!(
        got[4],
        Event::Delete {
            relation: RelationKey { oid: 43, .. },
            ..
        }
    ));
    assert!(matches!(
        got[5],
        Event::Relation {
            key: RelationKey { oid: 42, .. },
            ..
        }
    ));
}

#[test]
fn restarted_idle_decoder_uses_receiver_seed_and_reemits_context() {
    let mut relations = Relations::new();
    let mut first = decoder(Limits::default());
    let mut frames = Vec::new();
    feed(&mut first, &mut relations, &mut frames, 1, "BEGIN 3").unwrap();
    feed(
        &mut first,
        &mut relations,
        &mut frames,
        1,
        "table public.data: DELETE: id[integer]:1",
    )
    .unwrap();
    assert!(first.take_completed().is_none());
    let mut restarted = Decoder::new(
        Plugin::TestDecoding,
        stream(),
        Limits::default(),
        DecoderPosition {
            next_begin_ordinal: 900,
            next_order: TxSeq(81),
        },
    )
    .unwrap();
    frames.clear();
    feed(&mut restarted, &mut relations, &mut frames, 1, "BEGIN 3").unwrap();
    feed(
        &mut restarted,
        &mut relations,
        &mut frames,
        1,
        "table public.data: DELETE: id[integer]:1",
    )
    .unwrap();
    feed(&mut restarted, &mut relations, &mut frames, 1, "COMMIT 3").unwrap();
    let tx = restarted.take_completed().unwrap();
    assert_eq!((tx.tx.begin_ordinal, tx.order.0), (900, 81));
    assert!(matches!(
        events(&frames, Limits::default())[0],
        Event::Relation { .. }
    ));
}

#[test]
fn oversized_events_or_part_counts_poison_before_any_dependent_frame() {
    for limits in [
        Limits {
            max_frame_bytes: 128,
            max_event_bytes: 4096,
            max_parts: 2,
        },
        Limits {
            max_frame_bytes: 1024,
            max_event_bytes: 128,
            max_parts: 128,
        },
    ] {
        let mut decoder = decoder(limits);
        let mut relations = Relations::new();
        decoder
            .feed(&input(1, "BEGIN 3"), &mut relations, &mut |_| Ok(()))
            .unwrap();
        assert!(
            decoder
                .feed(
                    &input(
                        1,
                        &format!(
                            "table public.data: INSERT: id[integer]:1 v[text]:'{}'",
                            "x".repeat(300)
                        )
                    ),
                    &mut relations,
                    &mut |_| panic!("oversized row emitted")
                )
                .is_err()
        );
        assert_poisoned(&mut decoder, &mut relations);
    }
}

#[test]
fn one_large_plugin_message_spills_through_bounded_frame_parts() {
    let limits = Limits {
        max_frame_bytes: 65536,
        max_event_bytes: 2 * 1024 * 1024,
        max_parts: 64,
    };
    let mut decoder = decoder(limits);
    let mut relations = Relations::new();
    decoder
        .feed(&input(1, "BEGIN 3"), &mut relations, &mut |_| Ok(()))
        .unwrap();
    let mut parts = 0;
    let mut bytes = 0;
    decoder
        .feed(
            &input(
                1,
                &format!(
                    "table public.data: INSERT: id[integer]:1 v[text]:'{}'",
                    "x".repeat(1024 * 1024)
                ),
            ),
            &mut relations,
            &mut |frame| {
                assert!(frame.payload.len() + 88 <= 65536);
                parts += 1;
                bytes += 88 + frame.payload.len() as u64;
                Ok(())
            },
        )
        .unwrap();
    assert!(parts > 16);
    decoder
        .feed(&input(2, "COMMIT 3"), &mut relations, &mut |_| panic!())
        .unwrap();
    let tx = decoder.take_completed().unwrap();
    assert_eq!(tx.event_count, 2);
    assert_eq!(tx.encoded_bytes, bytes + 128);
}

#[test]
fn long_same_table_transaction_uses_external_sink_and_one_context() {
    use crate::cdc::codec::{
        ReadRecord, Record, TailPolicy, TxValidator, encode_completed, read_record, write_frame,
    };
    use std::io::{Read, Write};
    let limits = Limits {
        max_frame_bytes: 128,
        max_event_bytes: 512,
        max_parts: 32,
    };
    let mut decoder = decoder(limits);
    let mut relations = Relations::new();
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path =
        std::env::temp_dir().join(format!("rpgmigrate-decoder-{}-{stamp}", std::process::id()));
    let mut file = std::fs::File::create_new(&path).unwrap();
    decoder
        .feed(&input(1, "BEGIN 3"), &mut relations, &mut |_| Ok(()))
        .unwrap();
    let message = input(
        1,
        "table public.data: INSERT: id[integer]:1 v[text]:'a repeated row'",
    );
    let mut contexts = 0;
    let mut frame_count = 0;
    for _ in 0..10000 {
        decoder
            .feed(&message, &mut relations, &mut |frame| {
                if frame.event_ordinal == 0 && frame.part == 0 {
                    contexts += 1;
                }
                frame_count += 1;
                write_frame(&mut file, frame, limits).unwrap();
                Ok(())
            })
            .unwrap();
        assert!(decoder.take_completed().is_none());
    }
    decoder
        .feed(&input(2, "COMMIT 3"), &mut relations, &mut |_| panic!())
        .unwrap();
    let tx = decoder.take_completed().unwrap();
    assert_eq!(contexts, 1);
    assert_eq!(tx.event_count, 10001);
    assert!(tx.encoded_bytes > 1000 * limits.max_event_bytes);
    file.write_all(&encode_completed(&tx, limits).unwrap())
        .unwrap();
    assert_eq!(file.metadata().unwrap().len(), tx.encoded_bytes);
    drop(file);
    let mut reader = std::io::BufReader::new(std::fs::File::open(&path).unwrap());
    let mut validator = TxValidator::new(tx.tx, limits).unwrap();
    let mut read_frames = 0;
    loop {
        match read_record(&mut reader, limits, TailPolicy::Required).unwrap() {
            ReadRecord::Record(Record::Frame(frame)) => {
                validator.push(&frame).unwrap();
                read_frames += 1;
            }
            ReadRecord::Record(Record::Completed(completed)) => {
                assert_eq!(completed, tx);
                validator.finish(&completed).unwrap();
                break;
            }
            _ => panic!("spilled transaction has no checked completion"),
        }
    }
    assert_eq!(read_frames, frame_count);
    assert_eq!(reader.read(&mut [0_u8; 1]).unwrap(), 0);
    drop(reader);
    std::fs::remove_file(path).unwrap();
}
