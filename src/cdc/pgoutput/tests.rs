use super::*;
use crate::cdc::contracts::{FollowMessage, OldTupleKind, Plugin, Value};
use crate::contracts::{
    CopyGeneration, FollowBinding, FollowLimits, PublicationReceipt, SourceType,
};
use crate::identity::{RunId, SourceId, StreamId};
use crate::state::{SourceSlotBinding, SourceSlotIntent, SourceSlotReceipt};

fn binding() -> FollowBinding {
    let stream = StreamId {
        run: RunId([1; 16]),
        source: SourceId {
            system_identifier: 42,
            database_oid: 7,
        },
        generation: 1,
    };
    FollowBinding {
        slot: SourceSlotReceipt {
            intent: SourceSlotIntent {
                binding: SourceSlotBinding::new(
                    stream,
                    "decoder_owned".into(),
                    Plugin::Pgoutput,
                    1,
                )
                .unwrap(),
                nonce: [2; 16],
            },
            consistent_point: crate::identity::SourceEndLsn(1),
            snapshot_token: "synthetic-snapshot".into(),
        },
        publication: PublicationReceipt {
            name: "decoder_owned".into(),
            oid: 10,
            owner_oid: 11,
        },
        source_timeline: 1,
        copy_generation: CopyGeneration(1),
        source_types: vec![SourceType {
            oid: 16384,
            schema: "public".into(),
            name: "status".into(),
        }],
    }
}

pub(super) fn new_decoder() -> Decoder {
    let binding = binding();
    Decoder::new(binding.stream(), &binding, FollowLimits::default()).unwrap()
}

pub(super) fn begin(final_lsn: u64) -> Vec<u8> {
    let mut bytes = vec![b'B'];
    bytes.extend_from_slice(&final_lsn.to_be_bytes());
    bytes.extend_from_slice(&123_i64.to_be_bytes());
    bytes.extend_from_slice(&9_u32.to_be_bytes());
    bytes
}

fn commit(commit: u64, end: u64) -> Vec<u8> {
    let mut bytes = vec![b'C', 0];
    bytes.extend_from_slice(&commit.to_be_bytes());
    bytes.extend_from_slice(&end.to_be_bytes());
    bytes.extend_from_slice(&123_i64.to_be_bytes());
    bytes
}

pub(super) fn feed(
    decoder: &mut Decoder,
    bytes: &[u8],
) -> crate::contracts::Result<Vec<FollowMessage>> {
    let mut output = Vec::new();
    decoder.feed(bytes, &mut |message| {
        output.push(message);
        Ok(())
    })?;
    Ok(output)
}

#[test]
fn begin_and_commit_preserve_source_identity_and_distinct_end() {
    let mut decoder = new_decoder();
    assert!(decoder.is_idle());
    assert!(matches!(feed(&mut decoder, &begin(32)).unwrap().as_slice(),
        [FollowMessage::Begin(begin)] if begin.xid == 9 && begin.final_lsn.0 == 32 && begin.commit_time_micros == 123));
    assert!(!decoder.is_idle());
    assert!(
        matches!(feed(&mut decoder, &commit(32, 48)).unwrap().as_slice(),
        [FollowMessage::Commit(commit)] if commit.xid == 9 && commit.commit.0 == 32 && commit.end.0 == 48)
    );
    assert!(decoder.is_idle());
}

pub(super) fn relation(oid: u32, identity: u8, key: usize, types: &[u32]) -> Vec<u8> {
    let mut bytes = vec![b'R'];
    bytes.extend_from_slice(&oid.to_be_bytes());
    bytes.extend_from_slice(b"public\0items\0");
    bytes.push(identity);
    bytes.extend_from_slice(&(types.len() as u16).to_be_bytes());
    for (index, type_oid) in types.iter().enumerate() {
        bytes.push(u8::from(identity == b'f' || index == key));
        bytes.extend_from_slice(format!("col{index}").as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&type_oid.to_be_bytes());
        bytes.extend_from_slice(&(-1_i32).to_be_bytes());
    }
    bytes
}

fn tuple(fields: &[Value]) -> Vec<u8> {
    let mut bytes = (fields.len() as u16).to_be_bytes().to_vec();
    for field in fields {
        match field {
            Value::Null => bytes.push(b'n'),
            Value::UnchangedToast => bytes.push(b'u'),
            Value::Text(value) => {
                bytes.push(b't');
                bytes.extend_from_slice(&(value.len() as i32).to_be_bytes());
                bytes.extend_from_slice(value);
            }
        }
    }
    bytes
}

fn row(tag: u8, kind: u8, fields: &[Value]) -> Vec<u8> {
    let mut bytes = vec![tag, 0, 0, 0, 42, kind];
    bytes.extend_from_slice(&tuple(fields));
    bytes
}

fn ready(identity: u8) -> Decoder {
    let mut decoder = new_decoder();
    feed(&mut decoder, &begin(32)).unwrap();
    feed(
        &mut decoder,
        &relation(
            42,
            identity,
            if identity == b'n' { usize::MAX } else { 1 },
            &[25, 23],
        ),
    )
    .unwrap();
    decoder
}

fn text(value: &str) -> Value {
    Value::Text(value.as_bytes().to_vec())
}

pub(super) fn assert_poisoned(decoder: &mut Decoder) {
    assert!(!decoder.is_idle());
    let mut emitted = false;
    let error = decoder
        .feed(&begin(64), &mut |_| {
            emitted = true;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::BadState);
    assert!(!emitted);
}

#[test]
fn key_old_tuples_keep_nonkey_placeholders_and_new_toast() {
    let mut decoder = ready(b'd');
    let mut update = row(b'U', b'K', &[Value::Null, text("1")]);
    update.push(b'N');
    update.extend_from_slice(&tuple(&[Value::UnchangedToast, text("2")]));
    let events = feed(&mut decoder, &update).unwrap();
    assert!(
        matches!(events.as_slice(), [FollowMessage::Event(Event::Update { old: Some(old), new, .. })]
        if old.kind == OldTupleKind::Key && old.values == [Value::Null, text("1")]
        && *new == [Value::UnchangedToast, text("2")])
    );
    let events = feed(&mut decoder, &row(b'D', b'K', &[Value::Null, text("2")])).unwrap();
    assert!(
        matches!(events.as_slice(), [FollowMessage::Event(Event::Delete { old, .. })]
        if old.kind == OldTupleKind::Key && old.values.len() == 2)
    );
}

#[test]
fn full_old_tuples_keep_null_empty_and_unchanged_toast() {
    let mut decoder = ready(b'f');
    let events = feed(&mut decoder, &row(b'D', b'O', &[Value::Null, text("")])).unwrap();
    assert!(
        matches!(events.as_slice(), [FollowMessage::Event(Event::Delete { old, .. })]
        if old.kind == OldTupleKind::Full && old.values == [Value::Null, text("")])
    );
    assert!(
        feed(
            &mut decoder,
            &row(b'D', b'O', &[Value::UnchangedToast, text("2")])
        )
        .is_ok()
    );
}

#[test]
fn repeated_relation_metadata_retains_generation_but_ddl_is_refused() {
    let mut decoder = ready(b'd');
    let events = feed(&mut decoder, &relation(42, b'd', 1, &[25, 23])).unwrap();
    assert!(
        matches!(events.as_slice(), [FollowMessage::Event(Event::Relation { key, .. })] if key.generation == 1)
    );
    assert!(feed(&mut decoder, &relation(42, b'f', 1, &[25, 23])).is_err());
    assert_poisoned(&mut decoder);
}

#[test]
fn bound_custom_type_requires_matching_y_before_relation() {
    let mut decoder = new_decoder();
    feed(&mut decoder, &begin(32)).unwrap();
    assert!(feed(&mut decoder, &relation(42, b'd', 0, &[16384])).is_err());
    let mut decoder = new_decoder();
    feed(&mut decoder, &begin(32)).unwrap();
    assert!(
        feed(&mut decoder, b"Y\0\0\x40\0public\0status\0")
            .unwrap()
            .is_empty()
    );
    assert!(feed(&mut decoder, &relation(42, b'd', 0, &[16384])).is_ok());
    assert!(feed(&mut decoder, b"Y\0\0\x40\0public\0wrong\0").is_err());
}

#[test]
fn callback_errors_and_panics_latch_failure_before_emission() {
    let mut decoder = new_decoder();
    assert!(
        decoder
            .feed(&begin(32), &mut |_| Err(Error::new(
                ErrorKind::Target,
                "callback failed"
            )))
            .is_err()
    );
    assert_poisoned(&mut decoder);
    let mut decoder = new_decoder();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        decoder.feed(&begin(32), &mut |_| panic!("intentional callback panic"))
    }));
    assert!(result.is_err());
    assert_poisoned(&mut decoder);
}

#[test]
fn malformed_packets_never_emit_and_always_poison() {
    let mut packets = vec![
        vec![],
        vec![b'?'],
        begin(0),
        commit(32, 48),
        relation(42, b'?', 1, &[25, 23]),
    ];
    let mut xid = begin(32);
    xid[17..21].copy_from_slice(&0_u32.to_be_bytes());
    packets.push(xid);
    for time in [i64::MIN, i64::MAX, -211813488000000001, 9223371331200000000] {
        let mut bytes = begin(32);
        bytes[9..17].copy_from_slice(&time.to_be_bytes());
        packets.push(bytes);
    }
    let mut extra = begin(32);
    extra.push(0);
    packets.push(extra);
    for tag in b"SEcAPKrpbM" {
        packets.push(vec![*tag]);
    }
    for packet in packets {
        let mut decoder = new_decoder();
        let mut emitted = false;
        assert!(
            decoder
                .feed(&packet, &mut |_| {
                    emitted = true;
                    Ok(())
                })
                .is_err()
        );
        assert!(!emitted);
        assert_poisoned(&mut decoder);
    }
    for packet in [
        begin(64),
        commit(31, 48),
        commit(32, 31),
        row(b'I', b'N', &[text("x")]),
        row(b'I', b'N', &[Value::UnchangedToast, text("2")]),
        row(b'D', b'K', &[text("not-placeholder"), text("2")]),
        row(b'D', b'O', &[text("x"), text("2")]),
    ] {
        let mut decoder = ready(b'd');
        assert!(feed(&mut decoder, &packet).is_err());
        assert_poisoned(&mut decoder);
    }
}

#[test]
fn every_fixed_field_tuple_and_string_truncation_is_refused() {
    let corpus = [
        begin(32),
        commit(32, 48),
        relation(42, b'd', 1, &[25, 23]),
        b"Y\0\0\x40\0public\0status\0".to_vec(),
        row(b'I', b'N', &[text("x"), text("")]),
        row(b'D', b'K', &[Value::Null, text("2")]),
        b"O\0\0\0\0\0\0\0\x10origin\0".to_vec(),
        vec![b'T', 0, 0, 0, 1, 0, 0, 0, 0, 42],
    ];
    for packet in corpus {
        for end in 0..packet.len() {
            let mut decoder = if packet[0] == b'B' {
                new_decoder()
            } else {
                ready(b'd')
            };
            let mut emitted = false;
            assert!(
                decoder
                    .feed(&packet[..end], &mut |_| {
                        emitted = true;
                        Ok(())
                    })
                    .is_err(),
                "tag {} end {end}",
                packet[0]
            );
            assert!(!emitted);
            assert_poisoned(&mut decoder);
        }
    }
}

#[test]
fn invalid_lengths_flags_utf8_nuls_and_positional_counts_are_refused() {
    let mut packets = Vec::new();
    for length in [-1_i32, i32::MAX] {
        let mut bytes = vec![b'I', 0, 0, 0, 42, b'N', 0, 2, b't'];
        bytes.extend_from_slice(&length.to_be_bytes());
        packets.push(bytes);
    }
    for value in [vec![0], vec![255]] {
        let mut bytes = vec![b'I', 0, 0, 0, 42, b'N', 0, 2, b't', 0, 0, 0, 1];
        bytes.extend_from_slice(&value);
        bytes.push(b'n');
        packets.push(bytes);
    }
    packets.extend([
        vec![b'I', 0, 0, 0, 42, b'N', 0, 2, b'b', 0, 0, 0, 0, b'n'],
        vec![b'I', 0, 0, 0, 42, b'N', 0, 2, b'?', b'n'],
        vec![b'T', 0, 0, 0, 1, 4, 0, 0, 0, 42],
        vec![b'T', 0, 0, 0, 2, 0, 0, 0, 0, 42, 0, 0, 0, 42],
    ]);
    let mut bad_relation = relation(43, b'd', 1, &[25, 23]);
    bad_relation[21] = 2;
    packets.push(bad_relation);
    for packet in packets {
        let mut decoder = ready(b'd');
        assert!(feed(&mut decoder, &packet).is_err());
        assert_poisoned(&mut decoder);
    }
}

pub(super) fn fixture(major: u32) -> serde_json::Value {
    serde_json::from_str(match major {
        16 => include_str!("../../../tests/fixtures/cdc/pgoutput/pg16.json"),
        18 => include_str!("../../../tests/fixtures/cdc/pgoutput/pg18.json"),
        _ => unreachable!(),
    })
    .unwrap()
}

pub(super) fn unhex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let digit = |byte: u8| match byte {
                b'0'..=b'9' => byte - b'0',
                b'a'..=b'f' => byte - b'a' + 10,
                _ => panic!("invalid fixture hex"),
            };
            digit(pair[0]) * 16 + digit(pair[1])
        })
        .collect()
}

pub(super) fn fixture_decoder(fixture: &serde_json::Value) -> Decoder {
    let mut binding = binding();
    binding.source_types = serde_json::from_value(fixture["source_types"].clone()).unwrap();
    Decoder::new(binding.stream(), &binding, FollowLimits::default()).unwrap()
}

#[test]
fn actual_pg16_pg18_corpus_preserves_key_positions_toast_domain_and_spill() {
    for major in [16, 18] {
        let fixture = fixture(major);
        assert_eq!(fixture["version"], 1);
        assert_eq!(
            fixture["server"]["server_version_num"]
                .as_str()
                .unwrap()
                .parse::<u32>()
                .unwrap()
                / 10000,
            major
        );
        assert!(fixture["source_spill"]["spill_txns"].as_u64().unwrap() > 0);
        assert!(fixture["source_spill"]["spill_bytes"].as_u64().unwrap() > 0);
        assert!(
            fixture["source_types"]
                .as_array()
                .unwrap()
                .iter()
                .any(|typ| typ["schema"] == "pg_catalog" && typ["name"] == "int4")
        );
        let mut decoder = fixture_decoder(&fixture);
        let mut counts = std::collections::BTreeMap::new();
        let mut saw_key = false;
        let mut saw_full = false;
        let mut saw_toast = false;
        let mut saw_generated = false;
        let mut inserts = 0;
        let mut emitted = 0;
        let mut commits = 0;
        for packet in fixture["packets"].as_array().unwrap() {
            let bytes = unhex(packet["hex"].as_str().unwrap());
            *counts
                .entry((bytes[0] as char).to_string())
                .or_insert(0_u64) += 1;
            let messages = feed(&mut decoder, &bytes).unwrap();
            assert!(messages.len() <= 1);
            emitted += messages.len();
            for message in messages {
                match message {
                    FollowMessage::Begin(begin) => assert!(begin.final_lsn.0 > 0),
                    FollowMessage::Commit(commit) => {
                        commits += 1;
                        assert!(commit.end.0 >= commit.commit.0);
                    }
                    FollowMessage::Event(Event::Relation { name, columns, .. })
                        if name == "decoder_generated" =>
                    {
                        assert_eq!(
                            columns
                                .iter()
                                .map(|column| column.name.as_str())
                                .collect::<Vec<_>>(),
                            ["id", "value"]
                        );
                        saw_generated = true;
                    }
                    FollowMessage::Event(Event::Insert { .. }) => inserts += 1,
                    FollowMessage::Event(Event::Update { old: Some(old), .. })
                        if old.kind == OldTupleKind::Key =>
                    {
                        assert_eq!(
                            old.values,
                            [Value::Null, text("1"), Value::Null, Value::Null]
                        );
                        saw_key = true;
                    }
                    FollowMessage::Event(Event::Update { old: Some(old), .. })
                        if old.kind == OldTupleKind::Full =>
                    {
                        assert_eq!(old.values, [text("same"), Value::Null]);
                        saw_full = true;
                    }
                    FollowMessage::Event(Event::Update { new, .. })
                        if new.contains(&Value::UnchangedToast) =>
                    {
                        saw_toast = true
                    }
                    FollowMessage::Event(Event::Truncate {
                        relations,
                        cascade,
                        restart_identity,
                    }) => {
                        assert_eq!(relations.len(), 2);
                        assert!(cascade);
                        assert!(restart_identity);
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(
            serde_json::to_value(&counts).unwrap(),
            fixture["expected_tags"]
        );
        assert_eq!(
            counts,
            std::collections::BTreeMap::from([
                ("B".into(), 10),
                ("C".into(), 10),
                ("R".into(), 7),
                ("Y".into(), 3),
                ("I".into(), 518),
                ("U".into(), 5),
                ("D".into(), 3),
                ("T".into(), 1),
                ("O".into(), 1)
            ])
        );
        assert_eq!((inserts, emitted, commits), (518, 554, 10));
        assert!(saw_key && saw_full && saw_toast && saw_generated);
        assert!(decoder.is_idle());
    }
}

#[test]
fn binding_type_and_relation_metadata_are_bounded_unique_and_redacted() {
    for invalid in [
        SourceType {
            oid: 0,
            schema: "public".into(),
            name: "x".into(),
        },
        SourceType {
            oid: 16384,
            schema: "".into(),
            name: "x".into(),
        },
        SourceType {
            oid: 16384,
            schema: "public".into(),
            name: "x\0private-row-value".into(),
        },
        SourceType {
            oid: 16384,
            schema: "public".into(),
            name: "x".repeat(64),
        },
    ] {
        let mut binding = binding();
        binding.source_types = vec![invalid];
        let error = Decoder::new(binding.stream(), &binding, FollowLimits::default())
            .err()
            .unwrap();
        assert!(!format!("{error:?}").contains("private-row-value"));
    }
    let original = binding();
    let mut wrong_stream = original.stream();
    wrong_stream.generation += 1;
    assert!(Decoder::new(wrong_stream, &original, FollowLimits::default()).is_err());
    let mut binding = binding();
    binding.source_types.push(binding.source_types[0].clone());
    assert!(Decoder::new(binding.stream(), &binding, FollowLimits::default()).is_err());
    let mut binding = super::tests::binding();
    binding.source_types = (0..16385)
        .map(|i| SourceType {
            oid: 16384 + i,
            schema: "public".into(),
            name: "x".into(),
        })
        .collect();
    assert!(Decoder::new(binding.stream(), &binding, FollowLimits::default()).is_err());
    let mut decoder = new_decoder();
    feed(&mut decoder, &begin(32)).unwrap();
    let mut refused = false;
    for oid in 1..=4097 {
        let mut packet = relation(oid, b'd', 0, &[23]);
        packet.splice(12..17, format!("items{oid}").bytes());
        if feed(&mut decoder, &packet).is_err() {
            refused = true;
            break;
        }
        assert!(decoder.metadata_bytes().unwrap() <= 1024 * 1024);
    }
    assert!(refused);
    assert_poisoned(&mut decoder);
}

#[test]
fn source_order_flags_and_timestamp_consistency_are_enforced() {
    for change in 0..3 {
        let mut decoder = ready(b'd');
        let mut packet = commit(32, 48);
        match change {
            0 => packet[1] = 1,
            1 => packet[18..26].copy_from_slice(&124_i64.to_be_bytes()),
            _ => packet.push(0),
        };
        assert!(feed(&mut decoder, &packet).is_err());
        assert_poisoned(&mut decoder);
    }
    let mut decoder = new_decoder();
    feed(&mut decoder, &begin(32)).unwrap();
    feed(&mut decoder, &commit(32, 48)).unwrap();
    assert!(feed(&mut decoder, &begin(47)).is_err());
    assert_poisoned(&mut decoder);
    for tag in b"IRYUTDO" {
        let mut decoder = new_decoder();
        assert!(feed(&mut decoder, &[*tag]).is_err());
        assert_poisoned(&mut decoder);
    }
}

#[test]
fn supported_size_refusal_happens_before_callbacks() {
    let binding = binding();
    let limits = FollowLimits {
        max_message_bytes: 256,
        ..FollowLimits::default()
    };
    let mut decoder = Decoder::new(binding.stream(), &binding, limits).unwrap();
    feed(&mut decoder, &begin(32)).unwrap();
    feed(&mut decoder, &relation(42, b'd', 1, &[25, 23])).unwrap();
    let packet = row(b'I', b'N', &[text(&"x".repeat(190)), text("2")]);
    assert!(packet.len() < 256);
    let mut emitted = false;
    assert!(
        decoder
            .feed(&packet, &mut |_| {
                emitted = true;
                Ok(())
            })
            .is_err()
    );
    assert!(!emitted);
    assert_poisoned(&mut decoder);
}

#[test]
fn compact_keys_bad_identity_flags_missing_context_and_duplicate_columns_refuse() {
    let mut packets = vec![
        row(b'D', b'K', &[text("2")]),
        vec![
            b'I', 0, 0, 0, 42, b'N', 0, 2, b'b', 0, 0, 0, 0, b't', 0, 0, 0, 1, b'2',
        ],
        vec![b'I', 0, 0, 0, 99, b'N', 0, 0],
        vec![b'T', 0, 0, 0, 1, 0, 0, 0, 0, 99],
        vec![b'T', 0, 0, 0, 0, 0],
        vec![b'T', 255, 255, 255, 255, 0],
        b"O\0\0\0\0\0\0\0\0origin\0".to_vec(),
    ];
    let mut false_width = row(b'I', b'N', &[text("x"), text("2")]);
    false_width[6..8].copy_from_slice(&1_u16.to_be_bytes());
    packets.push(false_width);
    let mut relation_flags = relation(43, b'd', 1, &[25, 23]);
    relation_flags[21] = 2;
    packets.push(relation_flags);
    let mut identity = relation(43, b'?', 1, &[25, 23]);
    packets.push(identity.clone());
    identity[18] = b'f';
    identity[21] = 0;
    packets.push(identity);
    let mut duplicate = relation(43, b'd', 1, &[25, 23]);
    duplicate[39] = b'0';
    packets.push(duplicate);
    let mut encoding = relation(43, b'd', 1, &[25, 23]);
    encoding[12] = 255;
    packets.push(encoding);
    for packet in packets {
        let mut decoder = ready(b'd');
        let mut emitted = false;
        assert!(
            decoder
                .feed(&packet, &mut |_| {
                    emitted = true;
                    Ok(())
                })
                .is_err(),
            "tag {}",
            packet[0]
        );
        assert!(!emitted);
        assert_poisoned(&mut decoder);
    }
    for identity in *b"fn" {
        let mut decoder = ready(identity);
        assert!(feed(&mut decoder, &row(b'U', b'N', &[text("x"), text("2")])).is_err());
        assert_poisoned(&mut decoder);
    }
}

#[test]
fn update_old_and_new_buffers_share_the_decoded_message_budget() {
    let binding = binding();
    let tuple_bytes = 2 * size_of::<Value>() + 101;
    let limits = FollowLimits {
        max_message_bytes: size_of::<FollowMessage>() + 2 * tuple_bytes - 1,
        ..FollowLimits::default()
    };
    assert!(size_of::<FollowMessage>() + tuple_bytes < limits.max_message_bytes);
    assert!(size_of::<FollowMessage>() + 2 * tuple_bytes > limits.max_message_bytes);
    let mut decoder = Decoder::new(binding.stream(), &binding, limits).unwrap();
    feed(&mut decoder, &begin(32)).unwrap();
    feed(&mut decoder, &relation(42, b'f', 1, &[25, 23])).unwrap();
    let mut update = row(b'U', b'O', &[text(&"x".repeat(100)), text("1")]);
    update.push(b'N');
    update.extend_from_slice(&tuple(&[text(&"y".repeat(100)), text("2")]));
    assert!(update.len() < limits.max_message_bytes);
    assert!(feed(&mut decoder, &update).is_err());
    assert_poisoned(&mut decoder);
}
