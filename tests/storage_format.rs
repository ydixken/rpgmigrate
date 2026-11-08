#[path = "../src/cdc/codec.rs"]
mod format;
#[path = "../src/identity.rs"]
mod identity;

use format::*;
use std::io::{self, Cursor, Read, Write};

fn frame() -> Frame {
    Frame {
        tx: TxId {
            stream: StreamId {
                run: RunId([0x11; 16]),
                source: SourceId {
                    system_identifier: 0x0102_0304_0506_0708,
                    database_oid: 0x1234_5678,
                },
                generation: 9,
            },
            xid: 42,
            begin_ordinal: 77,
        },
        event_ordinal: 0,
        part: 0,
        last_part: true,
        payload: vec![0, b'x', 255],
    }
}

#[test]
fn persisted_identity_json_has_one_exact_version_two_shape() {
    let stream: identity::StreamId = frame().tx.stream;
    let encoded = serde_json::to_value(stream).unwrap();
    assert_eq!(
        encoded,
        serde_json::json!({
            "run": [17,17,17,17,17,17,17,17,17,17,17,17,17,17,17,17],
            "source": {"system_identifier": 0x0102_0304_0506_0708u64, "database_oid": 0x1234_5678u32},
            "generation": 9
        })
    );
    assert_eq!(
        serde_json::from_value::<StreamId>(encoded.clone()).unwrap(),
        stream
    );
    let mut extra = encoded.clone();
    extra["unexpected"] = true.into();
    assert!(serde_json::from_value::<StreamId>(extra).is_err());
    let mut extra_source = encoded;
    extra_source["source"]["unexpected"] = true.into();
    assert!(serde_json::from_value::<StreamId>(extra_source).is_err());
    assert!(serde_json::from_value::<RunId>(serde_json::json!([1, 2])).is_err());
}

fn completed() -> CompletedTx {
    CompletedTx {
        tx: frame().tx,
        order: TxSeq(1),
        commit: Some(CommitLsn(700)),
        end: SourceEndLsn(800),
        event_count: 1,
        encoded_bytes: 219,
    }
}

// Python struct and zlib produced this fixture independently of the codec.
fn golden_frame() -> Vec<u8> {
    from_hex(concat!(
        "5250475302000101580000000300000011111111111111111111111111111111",
        "08070605040302017856341209000000000000002a0000004d00000000000000",
        "0000000000000000000000000000000061c5a5357f58f31f0078ff"
    ))
}

fn golden_completed(commit: Option<CommitLsn>) -> Vec<u8> {
    from_hex(match commit {
        Some(CommitLsn(700)) => concat!(
            "5250475302000202580000002800000011111111111111111111111111111111",
            "08070605040302017856341209000000000000002a0000004d00000000000000",
            "00000000000000000000000000000000c5a8f579b743d65e0100000000000000",
            "bc0200000000000020030000000000000100000000000000db00000000000000"
        ),
        None => concat!(
            "5250475302000200580000002800000011111111111111111111111111111111",
            "08070605040302017856341209000000000000002a0000004d00000000000000",
            "000000000000000000000000000000006a93843beb90b9950100000000000000",
            "000000000000000020030000000000000100000000000000db00000000000000"
        ),
        Some(CommitLsn(0)) => concat!(
            "5250475302000202580000002800000011111111111111111111111111111111",
            "08070605040302017856341209000000000000002a0000004d00000000000000",
            "000000000000000000000000000000006a93843b9e2816cd0100000000000000",
            "000000000000000020030000000000000100000000000000db00000000000000"
        ),
        _ => panic!("no independent fixture for this commit"),
    })
}

fn from_hex(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn fix_header_crc(bytes: &mut [u8]) {
    let crc = crc32fast::hash(&bytes[..84]);
    bytes[84..88].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn frame_wire_matches_independent_fixture() {
    assert_eq!(
        encode_frame(&frame(), Limits::default()).unwrap(),
        golden_frame()
    );
    assert_eq!(
        decode_frame(&golden_frame(), Limits::default()).unwrap(),
        frame()
    );
}

#[test]
fn descriptor_preserves_distinct_commit_and_end_and_wire_total() {
    let bytes = encode_completed(&completed(), Limits::default()).unwrap();
    assert_eq!(bytes.len(), 128);
    assert_eq!(&bytes[88..96], &1_u64.to_le_bytes());
    assert_eq!(&bytes[96..104], &700_u64.to_le_bytes());
    assert_eq!(&bytes[104..112], &800_u64.to_le_bytes());
    assert_eq!(&bytes[112..120], &1_u64.to_le_bytes());
    assert_eq!(&bytes[120..128], &219_u64.to_le_bytes());
    assert_eq!(
        decode_completed(&bytes, Limits::default()).unwrap(),
        completed()
    );
}

#[test]
fn every_header_byte_is_checksummed_including_length_and_identity() {
    for offset in 0..88 {
        let mut bytes = golden_frame();
        bytes[offset] ^= 1;
        assert_eq!(
            decode_frame(&bytes, Limits::default()),
            Err(Error::CorruptHeader),
            "offset {offset}"
        );
    }
}

#[test]
fn complete_payload_corruption_is_never_an_unpublished_tail() {
    let mut bytes = golden_frame();
    bytes[90] ^= 1;
    for policy in [TailPolicy::Required, TailPolicy::Unpublished] {
        assert_eq!(
            read_record(&mut Cursor::new(&bytes), Limits::default(), policy),
            Err(Error::CorruptPayload)
        );
    }
    assert_eq!(
        decode_frame(&bytes, Limits::default()),
        Err(Error::CorruptPayload)
    );
}

#[test]
fn incomplete_input_requires_an_external_unpublished_assertion() {
    let bytes = golden_frame();
    for end in 0..bytes.len() {
        assert_eq!(
            read_record(
                &mut Cursor::new(&bytes[..end]),
                Limits::default(),
                TailPolicy::Required
            ),
            Err(Error::Gap)
        );
        let expected = if end == 0 {
            ReadRecord::Eof
        } else {
            ReadRecord::IncompleteTail
        };
        assert_eq!(
            read_record(
                &mut Cursor::new(&bytes[..end]),
                Limits::default(),
                TailPolicy::Unpublished
            ),
            Ok(expected)
        );
        assert_eq!(
            decode_frame(&bytes[..end], Limits::default()),
            Err(Error::Gap)
        );
    }
}

#[test]
fn unknown_version_kind_flags_and_reserved_bytes_fail_closed() {
    for (offset, value, expected) in [
        (4, 3, Error::UnsupportedVersion(3)),
        (6, 3, Error::UnknownKind(3)),
        (7, 3, Error::InvalidRecord),
        (8, 87, Error::InvalidRecord),
        (76, 1, Error::InvalidRecord),
        (0, b'X', Error::InvalidRecord),
    ] {
        let mut bytes = golden_frame();
        bytes[offset] = value;
        fix_header_crc(&mut bytes);
        assert_eq!(decode_frame(&bytes, Limits::default()), Err(expected));
    }
}

#[test]
fn excessive_length_fails_before_payload_read_or_allocation() {
    let mut bytes = golden_frame();
    bytes[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
    fix_header_crc(&mut bytes);
    let mut input = Cursor::new(&bytes[..88]);
    assert_eq!(
        read_record(&mut input, Limits::default(), TailPolicy::Unpublished),
        Err(Error::LimitExceeded)
    );
    assert_eq!(input.position(), 88);
}

#[test]
fn exact_slice_decoders_reject_suffix_and_wrong_record_kind() {
    assert_eq!(
        decode_frame(&golden_frame(), Limits::default()).unwrap(),
        frame()
    );
    let mut bytes = golden_frame();
    bytes.push(0);
    assert_eq!(
        decode_frame(&bytes, Limits::default()),
        Err(Error::InvalidRecord)
    );
    assert_eq!(
        decode_completed(&golden_frame(), Limits::default()),
        Err(Error::InvalidRecord)
    );
    assert_eq!(
        decode_frame(
            &encode_completed(&completed(), Limits::default()).unwrap(),
            Limits::default()
        ),
        Err(Error::InvalidRecord)
    );
}

#[test]
fn frame_and_event_limits_are_separate_and_finite() {
    for limits in [
        Limits {
            max_frame_bytes: 127,
            ..Limits::default()
        },
        Limits {
            max_frame_bytes: 64 * 1024 * 1024 + 1,
            ..Limits::default()
        },
        Limits {
            max_event_bytes: 0,
            ..Limits::default()
        },
        Limits {
            max_event_bytes: 1024 * 1024 * 1024 + 1,
            ..Limits::default()
        },
        Limits {
            max_parts: 0,
            ..Limits::default()
        },
        Limits {
            max_parts: 1_048_577,
            ..Limits::default()
        },
    ] {
        assert_eq!(encode_frame(&frame(), limits), Err(Error::InvalidLimits));
    }
    let limits = Limits {
        max_event_bytes: 2,
        ..Limits::default()
    };
    assert_eq!(encode_frame(&frame(), limits), Err(Error::LimitExceeded));
    assert_eq!(
        decode_frame(&golden_frame(), limits),
        Err(Error::LimitExceeded)
    );
    let mut oversized = frame();
    oversized.payload.resize(41, 0);
    let limits = Limits {
        max_frame_bytes: 128,
        ..Limits::default()
    };
    assert_eq!(encode_frame(&oversized, limits), Err(Error::LimitExceeded));
}

struct ShortIo<T>(T);

impl<T: Read> Read for ShortIo<T> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let size = bytes.len().min(3);
        self.0.read(&mut bytes[..size])
    }
}

impl<T: Write> Write for ShortIo<T> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(&bytes[..bytes.len().min(3)])
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

#[test]
fn streaming_io_handles_short_reads_and_writes() {
    let mut out = ShortIo(Vec::new());
    assert_eq!(
        write_frame(&mut out, &frame(), Limits::default()).unwrap(),
        91
    );
    assert_eq!(out.0, golden_frame());
    out.0
        .extend_from_slice(&encode_completed(&completed(), Limits::default()).unwrap());
    let mut input = ShortIo(Cursor::new(out.0));
    assert_eq!(
        read_record(&mut input, Limits::default(), TailPolicy::Required).unwrap(),
        ReadRecord::Record(Record::Frame(frame()))
    );
    assert_eq!(
        read_record(&mut input, Limits::default(), TailPolicy::Required).unwrap(),
        ReadRecord::Record(Record::Completed(completed()))
    );
    assert_eq!(
        read_record(&mut input, Limits::default(), TailPolicy::Required),
        Err(Error::Gap)
    );
}

#[test]
fn split_event_checks_reconstructed_size_and_counts_each_wire_header() {
    let limits = Limits {
        max_event_bytes: 3,
        ..Limits::default()
    };
    let mut first = frame();
    first.payload = vec![0, b'x'];
    first.last_part = false;
    let mut last = frame();
    last.payload = vec![255];
    last.part = 1;
    let mut descriptor = completed();
    descriptor.encoded_bytes = 307;
    let mut validator = TxValidator::new(first.tx, limits).unwrap();
    validator.push(&first).unwrap();
    validator.push(&last).unwrap();
    validator.finish(&descriptor).unwrap();

    let mut validator = TxValidator::new(
        first.tx,
        Limits {
            max_event_bytes: 2,
            ..limits
        },
    )
    .unwrap();
    validator.push(&first).unwrap();
    assert_eq!(validator.push(&last), Err(Error::LimitExceeded));
}

#[test]
fn finish_rejects_unfinished_event_even_when_counts_match() {
    let mut chunk = frame();
    chunk.last_part = false;
    let mut validator = TxValidator::new(chunk.tx, Limits::default()).unwrap();
    validator.push(&chunk).unwrap();
    let mut descriptor = completed();
    descriptor.event_count = 0;
    assert_eq!(validator.finish(&descriptor), Err(Error::Gap));
}

#[test]
fn validator_checks_all_identity_components_and_xid_reuse() {
    let expected = frame();
    let mut identities = [expected.tx; 6];
    identities[0].stream.run.0[0] ^= 1;
    identities[1].stream.source.system_identifier += 1;
    identities[2].stream.source.database_oid += 1;
    identities[3].stream.generation += 1;
    identities[4].xid += 1;
    identities[5].begin_ordinal += 1;
    for tx in identities {
        let mut different = expected.clone();
        different.tx = tx;
        let mut validator = TxValidator::new(expected.tx, Limits::default()).unwrap();
        assert_eq!(validator.push(&different), Err(Error::IdentityMismatch));
    }
}

#[test]
fn missing_duplicate_or_reordered_parts_and_events_fail() {
    for (event_ordinal, part) in [(1, 0), (0, 1)] {
        let mut chunk = frame();
        chunk.event_ordinal = event_ordinal;
        chunk.part = part;
        let mut validator = TxValidator::new(chunk.tx, Limits::default()).unwrap();
        assert_eq!(validator.push(&chunk), Err(Error::OrderMismatch));
    }
    let mut validator = TxValidator::new(frame().tx, Limits::default()).unwrap();
    validator.push(&frame()).unwrap();
    assert_eq!(validator.push(&frame()), Err(Error::OrderMismatch));
    assert!(validator.finish(&completed()).is_err());
}

#[test]
fn descriptor_cannot_hide_missing_events_or_payload_only_accounting() {
    for (event_count, encoded_bytes) in [(0, 219), (2, 219), (1, 3), (1, 91), (1, 218), (1, 220)] {
        let mut validator = TxValidator::new(frame().tx, Limits::default()).unwrap();
        validator.push(&frame()).unwrap();
        let descriptor = CompletedTx {
            event_count,
            encoded_bytes,
            ..completed()
        };
        assert_eq!(
            validator.finish(&descriptor),
            Err(Error::DescriptorMismatch)
        );
    }
    let validator = TxValidator::new(frame().tx, Limits::default()).unwrap();
    assert_eq!(
        validator.finish(&completed()),
        Err(Error::DescriptorMismatch)
    );
}

#[test]
fn descriptor_checks_identity_and_source_positions() {
    let mut validator = TxValidator::new(frame().tx, Limits::default()).unwrap();
    validator.push(&frame()).unwrap();
    let mut descriptor = completed();
    descriptor.tx.begin_ordinal += 1;
    assert_eq!(validator.finish(&descriptor), Err(Error::IdentityMismatch));
    for descriptor in [
        CompletedTx {
            commit: Some(CommitLsn(801)),
            ..completed()
        },
        CompletedTx {
            order: TxSeq(0),
            ..completed()
        },
        CompletedTx {
            encoded_bytes: 127,
            ..completed()
        },
    ] {
        assert_eq!(
            encode_completed(&descriptor, Limits::default()),
            Err(Error::InvalidRecord)
        );
    }
}

#[test]
fn consecutive_sequences_allow_equal_lsns_but_require_same_stream() {
    let previous = completed();
    let mut next = CompletedTx {
        order: TxSeq(2),
        ..previous
    };
    next.tx.begin_ordinal += 1;
    validate_next(&previous, &next).unwrap();
    next.order = TxSeq(3);
    assert_eq!(validate_next(&previous, &next), Err(Error::OrderMismatch));
    next.order = TxSeq(2);
    next.tx.stream.generation += 1;
    assert_eq!(
        validate_next(&previous, &next),
        Err(Error::IdentityMismatch)
    );
    next.tx.stream = previous.tx.stream;
    next.commit = Some(CommitLsn(699));
    assert_eq!(validate_next(&previous, &next), Err(Error::OrderMismatch));
    next.commit = previous.commit;
    next.end = SourceEndLsn(799);
    assert_eq!(validate_next(&previous, &next), Err(Error::OrderMismatch));
}

#[test]
fn zero_completion_sequence_cannot_enter_wire_or_replay() {
    let zero = CompletedTx {
        order: TxSeq(0),
        ..completed()
    };
    assert_eq!(
        encode_completed(&zero, Limits::default()),
        Err(Error::InvalidRecord)
    );
    let mut bytes = encode_completed(&completed(), Limits::default()).unwrap();
    bytes[88..96].copy_from_slice(&0_u64.to_le_bytes());
    let crc = crc32fast::hash(&bytes[88..]);
    bytes[80..84].copy_from_slice(&crc.to_le_bytes());
    fix_header_crc(&mut bytes);
    assert_eq!(
        decode_completed(&bytes, Limits::default()),
        Err(Error::InvalidRecord)
    );
    let mut validator = TxValidator::new(zero.tx, Limits::default()).unwrap();
    validator.push(&frame()).unwrap();
    assert_eq!(validator.finish(&zero), Err(Error::InvalidRecord));
    assert_eq!(
        validate_next(&zero, &completed()),
        Err(Error::InvalidRecord)
    );

    let mut first = completed();
    first.tx.xid = 0;
    first.tx.stream.generation = 0;
    assert_eq!(
        decode_completed(
            &encode_completed(&first, Limits::default()).unwrap(),
            Limits::default()
        )
        .unwrap(),
        first
    );
}

#[test]
fn empty_transaction_requires_its_complete_wire_descriptor() {
    let descriptor = CompletedTx {
        event_count: 0,
        encoded_bytes: 128,
        ..completed()
    };
    TxValidator::new(descriptor.tx, Limits::default())
        .unwrap()
        .finish(&descriptor)
        .unwrap();
    let decoded = decode_completed(
        &encode_completed(&descriptor, Limits::default()).unwrap(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(decoded, descriptor);
}

#[test]
fn part_count_limit_includes_the_last_part() {
    let limits = Limits {
        max_parts: 1,
        ..Limits::default()
    };
    encode_frame(&frame(), limits).unwrap();
    let mut chunk = frame();
    chunk.last_part = false;
    assert_eq!(encode_frame(&chunk, limits), Err(Error::LimitExceeded));
    chunk.last_part = true;
    chunk.part = 1;
    assert_eq!(encode_frame(&chunk, limits), Err(Error::LimitExceeded));
}

#[test]
fn streaming_checksum_matches_ieee_vector_without_collecting_chunks() {
    let mut checksum = StreamingChecksum::new();
    for chunk in [b"123".as_slice(), b"", b"4567", b"89"] {
        checksum.update(chunk);
    }
    assert_eq!(checksum.finish(), 0xcbf4_3926);
}

#[test]
fn giant_transaction_streams_through_one_reused_frame_buffer() {
    let limits = Limits {
        max_frame_bytes: 32 * 1024,
        max_event_bytes: 16 * 1024,
        max_parts: 1,
    };
    let mut chunk = frame();
    chunk.payload.resize(16 * 1024, 0xa5);
    let mut validator = TxValidator::new(chunk.tx, limits).unwrap();
    let mut wrong_total = TxValidator::new(chunk.tx, limits).unwrap();
    for ordinal in 0..4096 {
        chunk.event_ordinal = ordinal;
        validator.push(&chunk).unwrap();
        wrong_total.push(&chunk).unwrap();
    }
    let descriptor = CompletedTx {
        event_count: 4096,
        encoded_bytes: 67_469_440,
        ..completed()
    };
    validator.finish(&descriptor).unwrap();
    assert_eq!(
        wrong_total.finish(&CompletedTx {
            encoded_bytes: 67_469_439,
            ..descriptor
        }),
        Err(Error::DescriptorMismatch)
    );
}

#[test]
fn optional_commit_roundtrips_without_replacing_unknown_with_end() {
    for commit in [Some(CommitLsn(700)), None, Some(CommitLsn(0))] {
        let descriptor = CompletedTx {
            commit,
            ..completed()
        };
        let bytes = encode_completed(&descriptor, Limits::default()).unwrap();
        assert_eq!(bytes.len(), 128);
        let decoded = decode_completed(&bytes, Limits::default()).unwrap();
        assert_eq!(decoded.commit, commit);
        assert_eq!(decoded.end, SourceEndLsn(800));
        assert_eq!(decoded.encoded_bytes, 219);
    }
    let unknown = encode_completed(
        &CompletedTx {
            commit: None,
            ..completed()
        },
        Limits::default(),
    )
    .unwrap();
    let known_zero = encode_completed(
        &CompletedTx {
            commit: Some(CommitLsn(0)),
            ..completed()
        },
        Limits::default(),
    )
    .unwrap();
    assert_ne!(unknown, known_zero);
}

#[test]
fn v2_descriptor_goldens_match_slice_and_stream_readers() {
    for commit in [Some(CommitLsn(700)), None, Some(CommitLsn(0))] {
        let descriptor = CompletedTx {
            commit,
            ..completed()
        };
        let expected = golden_completed(commit);
        assert_eq!(
            encode_completed(&descriptor, Limits::default()).unwrap(),
            expected
        );
        assert_eq!(
            decode_completed(&expected, Limits::default()).unwrap(),
            descriptor
        );
        for policy in [TailPolicy::Required, TailPolicy::Unpublished] {
            assert_eq!(
                read_record(&mut Cursor::new(&expected), Limits::default(), policy).unwrap(),
                ReadRecord::Record(Record::Completed(descriptor))
            );
        }
    }
}

#[test]
fn v2_flags_are_kind_specific_and_descriptor_reserved_fields_stay_zero() {
    for (mut bytes, offset, value) in [
        (golden_frame(), 7, 2),
        (golden_frame(), 7, 3),
        (golden_completed(None), 7, 1),
        (golden_completed(Some(CommitLsn(700))), 7, 3),
        (golden_completed(None), 7, 4),
        (golden_completed(None), 7, 128),
        (golden_completed(None), 64, 1),
        (golden_completed(None), 72, 1),
        (golden_completed(None), 76, 1),
    ] {
        bytes[offset] = value;
        fix_header_crc(&mut bytes);
        let decoded = if bytes[6] == 1 {
            decode_frame(&bytes, Limits::default()).map(Record::Frame)
        } else {
            decode_completed(&bytes, Limits::default()).map(Record::Completed)
        };
        assert_eq!(decoded, Err(Error::InvalidRecord));
        assert_eq!(
            read_record(
                &mut Cursor::new(&bytes),
                Limits::default(),
                TailPolicy::Required
            ),
            Err(Error::InvalidRecord)
        );
    }
}

#[test]
fn unknown_commit_requires_zero_payload_even_with_valid_checksums() {
    let mut bytes = golden_completed(None);
    bytes[96] = 1;
    let payload_crc = crc32fast::hash(&bytes[88..]);
    bytes[80..84].copy_from_slice(&payload_crc.to_le_bytes());
    fix_header_crc(&mut bytes);
    assert_eq!(
        decode_completed(&bytes, Limits::default()),
        Err(Error::InvalidRecord)
    );
    for policy in [TailPolicy::Required, TailPolicy::Unpublished] {
        assert_eq!(
            read_record(&mut Cursor::new(&bytes), Limits::default(), policy),
            Err(Error::InvalidRecord)
        );
    }
}

#[test]
fn v1_frame_and_descriptor_are_refused_with_valid_checksums() {
    for mut bytes in [golden_frame(), golden_completed(Some(CommitLsn(700)))] {
        bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
        if bytes[6] == 2 {
            bytes[7] = 0;
        }
        fix_header_crc(&mut bytes);
        let decoded = if bytes[6] == 1 {
            decode_frame(&bytes, Limits::default()).map(Record::Frame)
        } else {
            decode_completed(&bytes, Limits::default()).map(Record::Completed)
        };
        assert_eq!(decoded, Err(Error::UnsupportedVersion(1)));
        for policy in [TailPolicy::Required, TailPolicy::Unpublished] {
            assert_eq!(
                read_record(&mut Cursor::new(&bytes), Limits::default(), policy),
                Err(Error::UnsupportedVersion(1))
            );
        }
    }
}

#[test]
fn commit_presence_and_value_bits_cannot_bypass_crc() {
    for commit in [Some(CommitLsn(700)), None, Some(CommitLsn(0))] {
        for (offset, expected) in [(7, Error::CorruptHeader), (96, Error::CorruptPayload)] {
            let mut bytes = golden_completed(commit);
            bytes[offset] ^= 2;
            assert_eq!(
                decode_completed(&bytes, Limits::default()),
                Err(expected.clone())
            );
            for policy in [TailPolicy::Required, TailPolicy::Unpublished] {
                assert_eq!(
                    read_record(&mut Cursor::new(&bytes), Limits::default(), policy),
                    Err(expected.clone())
                );
            }
        }
    }
}

#[test]
fn optional_commit_comparisons_use_only_present_evidence() {
    for (previous_commit, next_commit) in [
        (None, None),
        (None, Some(CommitLsn(0))),
        (Some(CommitLsn(700)), None),
        (Some(CommitLsn(700)), Some(CommitLsn(700))),
        (Some(CommitLsn(700)), Some(CommitLsn(701))),
    ] {
        let previous = CompletedTx {
            commit: previous_commit,
            ..completed()
        };
        let next = CompletedTx {
            commit: next_commit,
            order: TxSeq(2),
            ..completed()
        };
        assert_eq!(validate_next(&previous, &next), Ok(()));
        assert_eq!(
            validate_next(
                &previous,
                &CompletedTx {
                    end: SourceEndLsn(799),
                    ..next
                }
            ),
            Err(Error::OrderMismatch)
        );
        assert_eq!(
            validate_next(
                &previous,
                &CompletedTx {
                    order: TxSeq(3),
                    ..next
                }
            ),
            Err(Error::OrderMismatch)
        );
        let mut foreign = next;
        foreign.tx.stream.generation += 1;
        assert_eq!(
            validate_next(&previous, &foreign),
            Err(Error::IdentityMismatch)
        );
    }
    let previous = completed();
    let next = CompletedTx {
        commit: Some(CommitLsn(699)),
        order: TxSeq(2),
        ..previous
    };
    assert_eq!(validate_next(&previous, &next), Err(Error::OrderMismatch));
}

#[test]
fn unknown_commit_still_requires_sequence_and_exact_wire_totals() {
    let unknown = CompletedTx {
        commit: None,
        ..completed()
    };
    for invalid in [
        CompletedTx {
            order: TxSeq(0),
            ..unknown
        },
        CompletedTx {
            encoded_bytes: 127,
            ..unknown
        },
    ] {
        assert_eq!(
            encode_completed(&invalid, Limits::default()),
            Err(Error::InvalidRecord)
        );
    }
    let mut validator = TxValidator::new(unknown.tx, Limits::default()).unwrap();
    validator.push(&frame()).unwrap();
    validator.finish(&unknown).unwrap();
    let mut validator = TxValidator::new(unknown.tx, Limits::default()).unwrap();
    validator.push(&frame()).unwrap();
    assert_eq!(
        validator.finish(&CompletedTx {
            encoded_bytes: 3,
            ..unknown
        }),
        Err(Error::DescriptorMismatch)
    );
}
