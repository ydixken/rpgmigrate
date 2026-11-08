use super::*;
use crate::cdc::contracts::{Column, Plugin};
use crate::contracts::{SnapshotAnchor, StateAction, StateRequest};
use crate::identity::{RunId, SourceId, StreamId};
use crate::state::{self, ConfirmedProvenance, CreatedSlotData, TargetBinding};
use std::cell::{Cell, RefCell};
use std::fs::{self, DirBuilder};
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Step {
    BeforeCommit,
    AfterCommit,
    BeforeClose,
    AfterClose,
}

type Hook = Box<dyn FnMut(Step, Option<&Connection>, &Path) -> Result<()>>;

thread_local! {
    static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    pub(super) static DECODES: Cell<usize> = const { Cell::new(0) };
}

pub(super) fn step(step: Step, connection: Option<&Connection>, path: &Path) -> Result<()> {
    HOOK.with_borrow_mut(|hook| match hook {
        Some(hook) => hook(step, connection, path),
        None => Ok(()),
    })
}

pub(super) fn failed_close(connection: &Connection) {
    // SAFETY: only this test deliberately leaks a statement; the connection remains live.
    unsafe {
        let handle = connection.handle();
        let statement = rusqlite::ffi::sqlite3_next_stmt(handle, std::ptr::null_mut());
        assert!(!statement.is_null());
        assert_eq!(rusqlite::ffi::sqlite3_finalize(statement), 0);
    }
}

struct HookReset;

impl Drop for HookReset {
    fn drop(&mut self) {
        HOOK.with_borrow_mut(|hook| *hook = None);
    }
}

fn hook(visit: impl FnMut(Step, Option<&Connection>, &Path) -> Result<()> + 'static) -> HookReset {
    HOOK.with_borrow_mut(|hook| {
        assert!(hook.is_none());
        *hook = Some(Box::new(visit));
    });
    HookReset
}

struct Temp(PathBuf);

impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "rpgmigrate-plan-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        DirBuilder::new().mode(0o700).create(&path).unwrap();
        Self(path)
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

struct Fixture {
    native: NativeState,
    source: SourceSlotReceipt,
    temp: Temp,
}

impl Fixture {
    fn new() -> Self {
        let temp = Temp::new();
        let mut native = state::open(&StateRequest {
            path: temp.0.join("native"),
            action: StateAction::Create,
        })
        .unwrap();
        let stream = StreamId {
            run: native.identity().run,
            source: SourceId {
                system_identifier: 12345,
                database_oid: 16384,
            },
            generation: 9,
        };
        let binding =
            SourceSlotBinding::new(stream, "pgcopydb".into(), Plugin::Pgoutput, 7).unwrap();
        let intent = native.begin_source_slot(&binding).unwrap();
        let source = native
            .confirm_source_slot(
                &intent,
                CreatedSlotData {
                    consistent_point: SourceEndLsn(0x100000002),
                    snapshot_name: "00000003-0000001B-1".into(),
                },
            )
            .unwrap();
        let target = native
            .begin_target_provisioning(TargetBinding {
                stream,
                target_system_identifier: Some(67890),
                target_database_oid: 16385,
                schema_name: "rpgmigrate_owned".into(),
                table_name: "checkpoint".into(),
                owner_oid: 10,
                metadata_version: state::TARGET_METADATA_VERSION,
            })
            .unwrap();
        native
            .confirm_target_provisioning(ConfirmedProvenance {
                intent: target,
                schema_oid: 20000,
                table_oid: 20001,
            })
            .unwrap();
        Self {
            native,
            source,
            temp,
        }
    }

    fn publish(&mut self, entries: impl Iterator<Item = Result<PlanEntry>>) -> CopySnapshotIntent {
        let plan = publish_copy_plan(
            &mut self.native,
            &self.source,
            [42; 16],
            65536,
            entries,
            &io(),
        )
        .unwrap();
        let intent = CopySnapshotIntent {
            anchor: SnapshotAnchor {
                stream: self.source.intent.binding.stream(),
                snapshot_token: self.source.snapshot_token.clone(),
                consistent_point: self.source.consistent_point,
                plan_nonce: [42; 16],
            },
            plan,
        };
        self.native.bind_copy_snapshot(&intent).unwrap();
        intent
    }

    fn path(&self) -> PathBuf {
        self.native
            .open_plan_file([42; 16])
            .unwrap()
            .checked_path()
            .unwrap()
            .to_owned()
    }

    fn edit(&self, sql: &str) {
        let connection = Connection::open_with_flags(self.path(), write_flags()).unwrap();
        connection.execute_batch(sql).unwrap();
        connection.close().unwrap();
    }

    fn assert_unpublished(&self) {
        let plans = self.temp.0.join("native/data/plans");
        let names: Vec<_> = fs::read_dir(plans)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(names.len(), 1);
        assert!(names[0].to_str().unwrap().ends_with(".pending"));
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(self.temp.0.join("native/manifest.json")).unwrap())
                .unwrap();
        assert!(manifest["copy_snapshot"].is_null());
    }
}

fn io() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(120),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn relation(oid: u32) -> Event {
    Event::Relation {
        key: RelationKey { oid, generation: 7 },
        schema: "s".into(),
        name: format!("Table{oid}"),
        columns: vec![
            Column {
                name: "id".into(),
                type_oid: 23,
                type_mod: -1,
                key: true,
            },
            Column {
                name: "Payload".into(),
                type_oid: 25,
                type_mod: -1,
                key: false,
            },
        ],
        replica_identity: b'd',
    }
}

fn table(event: &Event, format: CopyFormat) -> CopyTable {
    let Event::Relation {
        schema,
        name,
        columns,
        ..
    } = event
    else {
        panic!("not a relation")
    };
    CopyTable {
        name: QualifiedTable {
            schema: schema.clone(),
            table: name.clone(),
        },
        columns: columns.iter().map(|column| column.name.clone()).collect(),
        format,
    }
}

fn entry(oid: u32) -> Result<PlanEntry> {
    let event = relation(oid);
    PlanEntry::checked(table(&event, CopyFormat::Text), event)
}

#[test]
fn copy_table_projection_mismatch_is_refused() {
    for case in 0..6 {
        let event = relation(11);
        let mut input = table(&event, CopyFormat::Text);
        match case {
            0 => input.name.schema = "S".into(),
            1 => input.name.table = "table11".into(),
            2 => input.columns.swap(0, 1),
            3 => {
                input.columns.pop();
            }
            4 => input.columns[1] = "payload".into(),
            _ => input.columns.push("extra".into()),
        }
        assert!(PlanEntry::checked(input, event).is_err(), "case {case}");
    }
}

#[test]
fn ordered_scans_and_indexed_lookups_agree() {
    let mut fixture = Fixture::new();
    let mut binary = entry(12).unwrap();
    binary.format = CopyFormat::Binary;
    let intent = fixture.publish([entry(11), Ok(binary), entry(13)].into_iter());
    let mut plan = open_sealed_copy_plan(&fixture.native, &intent, &io()).unwrap();
    assert_eq!(plan.buffer_bytes(), 65536);
    for after in [0, 1, 3] {
        let mut seen = Vec::new();
        plan.scan_from(after, &io(), |ordinal, format, event| {
            seen.push((ordinal, format, event.clone()));
            Ok(())
        })
        .unwrap();
        assert_eq!(seen.len(), usize::try_from(3 - after).unwrap());
        for (offset, (ordinal, format, event)) in seen.into_iter().enumerate() {
            let want = after + u64::try_from(offset).unwrap() + 1;
            assert_eq!(ordinal, want);
            assert_eq!(event, relation(u32::try_from(want + 10).unwrap()));
            assert_eq!(
                format,
                if want == 2 {
                    CopyFormat::Binary
                } else {
                    CopyFormat::Text
                }
            );
        }
    }
    DECODES.with(|count| count.set(0));
    plan.lookup_name(
        &QualifiedTable {
            schema: "s".into(),
            table: "Table12".into(),
        },
        &io(),
        |ordinal, format, event| {
            assert_eq!(ordinal, 2);
            assert_eq!(format, CopyFormat::Binary);
            assert_eq!(event, &relation(12));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(DECODES.with(Cell::get), 1);
    plan.lookup_relation(
        RelationKey {
            oid: 13,
            generation: 7,
        },
        &io(),
        |ordinal, _, _| {
            assert_eq!(ordinal, 3);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(DECODES.with(Cell::get), 2);
    assert!(
        plan.scan_from(4, &io(), |_, _, _| panic!("invalid cursor reached visitor"))
            .is_err()
    );
    assert!(
        plan.lookup_relation(
            RelationKey {
                oid: 13,
                generation: 9
            },
            &io(),
            |_, _, _| panic!("wrong epoch reached visitor")
        )
        .is_err()
    );
    assert!(
        plan.lookup_relation(
            RelationKey {
                oid: 99,
                generation: 7
            },
            &io(),
            |_, _, _| panic!("missing row reached visitor")
        )
        .is_err()
    );
}

#[test]
fn empty_plan_has_zero_logical_bytes_and_reopens() {
    let mut fixture = Fixture::new();
    let intent = fixture.publish(std::iter::empty());
    assert_eq!(intent.plan.table_count, 0);
    assert_eq!(intent.plan.logical_bytes, 0);
    let mut plan = open_sealed_copy_plan(&fixture.native, &intent, &io()).unwrap();
    plan.scan_from(0, &io(), |_, _, _| panic!("empty plan visited a row"))
        .unwrap();
    assert!(
        plan.lookup_name(
            &QualifiedTable {
                schema: "s".into(),
                table: "missing".into()
            },
            &io(),
            |_, _, _| panic!("missing row reached visitor")
        )
        .is_err()
    );
}

#[test]
fn projection_changes_are_refused_even_when_canonical_crcs_are_unchanged() {
    for sql in [
        "UPDATE records SET relation_oid=99",
        "UPDATE records SET catalog_epoch=x'0000000000000009'",
        "UPDATE records SET schema_name=x'53'",
        "UPDATE records SET table_name=x'7461626c653131'",
    ] {
        let mut fixture = Fixture::new();
        let intent = fixture.publish([entry(11)].into_iter());
        fixture.edit(sql);
        assert!(
            open_sealed_copy_plan(&fixture.native, &intent, &io()).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn schema_and_index_substitution_are_refused() {
    for sql in [
        "CREATE TABLE plan_columns(x BLOB)",
        "CREATE VIEW extra AS SELECT ordinal FROM records",
        "CREATE TRIGGER extra AFTER INSERT ON records BEGIN SELECT 1; END",
        "DROP INDEX records_by_name; CREATE UNIQUE INDEX records_by_name ON records(schema_name,table_name) WHERE ordinal>0",
        "DROP INDEX records_by_name; CREATE UNIQUE INDEX records_by_name ON records(lower(schema_name),table_name)",
        "DROP INDEX records_by_name; CREATE UNIQUE INDEX records_by_name ON records(schema_name DESC,table_name)",
        "DROP INDEX records_by_name; CREATE UNIQUE INDEX records_by_name ON records(schema_name COLLATE NOCASE,table_name)",
        "DROP INDEX records_by_relation",
        "PRAGMA user_version=2",
    ] {
        let mut fixture = Fixture::new();
        let intent = fixture.publish([entry(11)].into_iter());
        fixture.edit(sql);
        assert!(
            open_sealed_copy_plan(&fixture.native, &intent, &io()).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn incomplete_or_corrupt_seals_are_refused() {
    for sql in [
        "DELETE FROM seal",
        "DELETE FROM records",
        "UPDATE records SET ordinal=2",
        "UPDATE records SET record_crc32=x'00000000'",
        "UPDATE records SET copy_format=1",
        "UPDATE records SET relation_payload=zeroblob(1048489)",
        "UPDATE seal SET logical_bytes=x'ffffffffffffffff'",
        "UPDATE seal SET table_count=x'ffffffffffffffff'",
        "UPDATE seal SET crc32=x'00000000'",
        "UPDATE seal SET header=cast(header||' ' AS BLOB)",
    ] {
        let mut fixture = Fixture::new();
        let intent = fixture.publish([entry(11)].into_iter());
        fixture.edit(sql);
        assert!(
            open_sealed_copy_plan(&fixture.native, &intent, &io()).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn iterator_error_and_panic_never_publish_a_reference() {
    for panic in [false, true] {
        let mut fixture = Fixture::new();
        let entries = (0..2).map(|index| {
            if index == 0 {
                return entry(11);
            }
            if panic {
                panic!("iterator control");
            }
            Err(bad("injected iterator error"))
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            publish_copy_plan(
                &mut fixture.native,
                &fixture.source,
                [42; 16],
                65536,
                entries,
                &io(),
            )
        }));
        if panic {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_err());
        }
        fixture.assert_unpublished();
    }
}

#[test]
fn observer_errors_and_panics_never_publish_a_reference() {
    for stage in [
        Step::BeforeCommit,
        Step::AfterCommit,
        Step::BeforeClose,
        Step::AfterClose,
    ] {
        for panic in [false, true] {
            let mut fixture = Fixture::new();
            let _reset = hook(move |at, _, _| {
                if at != stage {
                    return Ok(());
                }
                if panic {
                    panic!("observer control {at:?}");
                }
                Err(bad("injected observer error"))
            });
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                publish_copy_plan(
                    &mut fixture.native,
                    &fixture.source,
                    [42; 16],
                    65536,
                    [entry(11)].into_iter(),
                    &io(),
                )
            }));
            if panic {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            fixture.assert_unpublished();
        }
    }
}

#[test]
fn actual_busy_commit_and_busy_close_return_no_reference() {
    for close in [false, true] {
        let mut fixture = Fixture::new();
        let mut reader = None;
        let _reset = hook(move |stage, connection, path| {
            if !close && stage == Step::BeforeCommit {
                let read = Connection::open_with_flags(path, read_flags()).unwrap();
                read.execute_batch("BEGIN").unwrap();
                let _: i64 = read
                    .query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))
                    .unwrap();
                reader = Some(read);
            }
            if close && stage == Step::BeforeClose {
                std::mem::forget(connection.unwrap().prepare("SELECT 1").unwrap());
            }
            let _ = &reader;
            Ok(())
        });
        let result = publish_copy_plan(
            &mut fixture.native,
            &fixture.source,
            [42; 16],
            65536,
            [entry(11)].into_iter(),
            &io(),
        );
        let error = result.unwrap_err();
        assert!(
            error
                .message
                .contains(if close { "close" } else { "COMMIT" }),
            "{error:?}"
        );
        fixture.assert_unpublished();
    }
}

#[test]
fn cancellation_and_zero_point_refuse_before_reserving_a_plan() {
    for case in 0..3 {
        let mut fixture = Fixture::new();
        let mut control = io();
        match case {
            0 => control.cancelled.store(true, Ordering::Relaxed),
            1 => control.deadline = Instant::now() - Duration::from_secs(1),
            _ => fixture.source.consistent_point = SourceEndLsn(0),
        }
        assert!(
            publish_copy_plan(
                &mut fixture.native,
                &fixture.source,
                [42; 16],
                65536,
                [entry(11)].into_iter(),
                &control
            )
            .is_err()
        );
        assert_eq!(
            fs::read_dir(fixture.temp.0.join("native/data/plans"))
                .unwrap()
                .count(),
            0
        );
    }
}

fn fixed_header() -> PlanHeader {
    PlanHeader {
        reference_version: 1,
        binding: SourceSlotBinding::new(
            StreamId {
                run: RunId([1; 16]),
                source: SourceId {
                    system_identifier: 12345,
                    database_oid: 16384,
                },
                generation: 9,
            },
            "pgcopydb".into(),
            Plugin::Pgoutput,
            7,
        )
        .unwrap(),
        snapshot_token: "00000003-0000001B-1".into(),
        consistent_point: SourceEndLsn(4294967298),
        plan_nonce: [42; 16],
        buffer_bytes: 65536,
    }
}

#[test]
fn header_and_checksum_bytes_match_independent_golden_values() {
    let golden = br#"{"reference_version":1,"binding":{"stream":{"run":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1],"source":{"system_identifier":12345,"database_oid":16384},"generation":9},"slot_name":"pgcopydb","plugin":"pgoutput","catalog_epoch":7},"snapshot_token":"00000003-0000001B-1","consistent_point":4294967298,"plan_nonce":[42,42,42,42,42,42,42,42,42,42,42,42,42,42,42,42],"buffer_bytes":65536}"#;
    let header = fixed_header();
    assert_eq!(header.encode().unwrap(), golden);
    assert_eq!(
        PlanHeader::decode(golden).unwrap().encode().unwrap(),
        golden
    );
    let payload = event_codec::encode_event(&relation(11), LIMITS).unwrap();
    assert_eq!(payload.len(), 70);
    assert_eq!(
        record_checksum(1, CopyFormat::Text, &payload).unwrap(),
        2_413_531_071
    );
    let mut crc = header_checksum(golden).unwrap();
    feed_record(&mut crc, 1, CopyFormat::Text, &payload).unwrap();
    assert_eq!(finish_checksum(crc, &header, 1, 71).crc32, 124_749_555);
    assert_eq!(
        finish_checksum(header_checksum(golden).unwrap(), &header, 0, 0).crc32,
        3_865_668_336
    );
}

#[test]
fn noncanonical_unknown_duplicate_and_missing_header_fields_are_refused() {
    let raw = String::from_utf8(fixed_header().encode().unwrap()).unwrap();
    for bytes in [
        format!("{raw} "),
        raw.replacen(
            "\"reference_version\":1",
            "\"reference_version\":1,\"reference_version\":1",
            1,
        ),
        raw.replacen(
            "\"catalog_epoch\":7",
            "\"catalog_epoch\":7,\"unknown\":0",
            1,
        ),
        raw.replacen(",\"buffer_bytes\":65536", "", 1),
        raw.replacen(
            "\"consistent_point\":4294967298",
            "\"consistent_point\":0",
            1,
        ),
        raw.replacen("\"reference_version\":1", "\"reference_version\":2", 1),
        raw.replacen("\"buffer_bytes\":65536", "\"buffer_bytes\":0", 1),
    ] {
        assert!(
            PlanHeader::decode(bytes.as_bytes()).is_err(),
            "accepted {bytes}"
        );
    }
    assert!(PlanHeader::decode(&vec![b' '; 16385]).is_err());
}

#[test]
fn source_receipt_mismatch_cannot_become_a_reader_capability() {
    let mut fixture = Fixture::new();
    let mut supplied = fixture.source.clone();
    supplied.snapshot_token.push('x');
    let reference = publish_copy_plan(
        &mut fixture.native,
        &supplied,
        [42; 16],
        65536,
        [entry(11)].into_iter(),
        &io(),
    )
    .unwrap();
    let intent = CopySnapshotIntent {
        anchor: SnapshotAnchor {
            stream: supplied.intent.binding.stream(),
            snapshot_token: supplied.snapshot_token,
            consistent_point: supplied.consistent_point,
            plan_nonce: [42; 16],
        },
        plan: reference,
    };
    assert!(open_sealed_copy_plan(&fixture.native, &intent, &io()).is_err());
    let error = fixture.native.bind_copy_snapshot(&intent).unwrap_err();
    assert!(error.message.contains("exact confirmed source provenance"));
    assert_eq!(fixture.native.checked_copy_snapshot().unwrap(), None);
}

#[test]
fn stream_generation_is_not_the_catalog_epoch() {
    let mut fixture = Fixture::new();
    let mut event = relation(11);
    let Event::Relation { key, .. } = &mut event else {
        unreachable!()
    };
    key.generation = 9;
    let bad_entry = PlanEntry::checked(table(&event, CopyFormat::Text), event).unwrap();
    assert!(
        publish_copy_plan(
            &mut fixture.native,
            &fixture.source,
            [42; 16],
            65536,
            [Ok(bad_entry)].into_iter(),
            &io()
        )
        .is_err()
    );
    fixture.assert_unpublished();
}

#[test]
fn relation_payload_boundary_is_codec_overhead_plus_one_format_byte() {
    for delta in [-1_i64, 0, 1] {
        let mut fixture = Fixture::new();
        let mut event = relation(11);
        let Event::Relation { columns, .. } = &mut event else {
            unreachable!()
        };
        columns.clear();
        let requested = usize::try_from(1_048_488_i64 + delta - 35).unwrap();
        let full = requested / 1037;
        let remainder = requested % 1037;
        assert!(remainder > 13);
        for index in 0..full {
            let name = format!("c{index:04}") + &"x".repeat(1019);
            columns.push(Column {
                name,
                type_oid: 25,
                type_mod: -1,
                key: false,
            });
        }
        columns.push(Column {
            name: "z".repeat(remainder - 13),
            type_oid: 25,
            type_mod: -1,
            key: false,
        });
        let checked = PlanEntry::checked(table(&event, CopyFormat::Text), event).unwrap();
        if delta > 0 {
            assert!(
                publish_copy_plan(
                    &mut fixture.native,
                    &fixture.source,
                    [42; 16],
                    65536,
                    [Ok(checked)].into_iter(),
                    &io()
                )
                .is_err()
            );
            fixture.assert_unpublished();
        } else {
            let intent = fixture.publish([Ok(checked)].into_iter());
            assert_eq!(
                intent.plan.logical_bytes,
                u64::try_from(1_048_489_i64 + delta).unwrap()
            );
            let mut plan = open_sealed_copy_plan(&fixture.native, &intent, &io()).unwrap();
            plan.scan_from(0, &io(), |ordinal, _, _| {
                assert_eq!(ordinal, 1);
                Ok(())
            })
            .unwrap();
        }
    }
}

#[test]
fn aggregate_crc_detects_changes_with_recomputed_record_crc() {
    let mut fixture = Fixture::new();
    let intent = fixture.publish([entry(11)].into_iter());
    let mut replacement = relation(11);
    let Event::Relation { columns, .. } = &mut replacement else {
        unreachable!()
    };
    columns[1].type_oid = 20;
    let bytes = event_codec::encode_event(&replacement, LIMITS).unwrap();
    let crc = record_checksum(1, CopyFormat::Text, &bytes).unwrap();
    let connection = Connection::open_with_flags(fixture.path(), write_flags()).unwrap();
    connection
        .execute(
            "UPDATE records SET relation_payload=?1,record_crc32=?2",
            params![bytes, crc.to_be_bytes().as_slice()],
        )
        .unwrap();
    connection.close().unwrap();
    let error = match open_sealed_copy_plan(&fixture.native, &intent, &io()) {
        Ok(_) => panic!("changed aggregate qualified a reader"),
        Err(error) => error,
    };
    assert!(error.message.contains("aggregate seal"), "{error:?}");
}

#[test]
fn zero_aggregate_crc_is_a_valid_seal_value() {
    let mut fixture = Fixture::new();
    let header = PlanHeader {
        binding: fixture.source.intent.binding.clone(),
        ..fixed_header()
    };
    let header_bytes = header.encode().unwrap();
    let calculate = |event: &Event| {
        let payload = event_codec::encode_event(event, LIMITS).unwrap();
        let mut checksum = header_checksum(&header_bytes).unwrap();
        feed_record(&mut checksum, 1, CopyFormat::Text, &payload).unwrap();
        finish_checksum(
            checksum,
            &header,
            1,
            u64::try_from(payload.len()).unwrap() + 1,
        )
        .crc32
    };
    let mut event = relation(11);
    let Event::Relation { columns, .. } = &mut event else {
        unreachable!()
    };
    columns[1].name = "@@@@@@@".into();
    let base = calculate(&event);
    let mut basis = [(0_u32, 0_u64); 32];
    for bit in 0..35 {
        let mut changed = event.clone();
        let Event::Relation { columns, .. } = &mut changed else {
            unreachable!()
        };
        let mut name = columns[1].name.as_bytes().to_vec();
        name[bit / 5] ^= 1 << (bit % 5);
        columns[1].name = String::from_utf8(name).unwrap();
        let mut value = calculate(&changed) ^ base;
        let mut mask = 1_u64 << bit;
        while value != 0 {
            let pivot = usize::try_from(31 - value.leading_zeros()).unwrap();
            if basis[pivot].0 == 0 {
                basis[pivot] = (value, mask);
                break;
            }
            value ^= basis[pivot].0;
            mask ^= basis[pivot].1;
        }
    }
    let mut value = base;
    let mut mask = 0_u64;
    while value != 0 {
        let pivot = usize::try_from(31 - value.leading_zeros()).unwrap();
        assert_ne!(basis[pivot].0, 0);
        value ^= basis[pivot].0;
        mask ^= basis[pivot].1;
    }
    let Event::Relation { columns, .. } = &mut event else {
        unreachable!()
    };
    let mut name = columns[1].name.as_bytes().to_vec();
    for bit in 0..35 {
        if mask & (1 << bit) != 0 {
            name[bit / 5] ^= 1 << (bit % 5);
        }
    }
    columns[1].name = String::from_utf8(name).unwrap();
    assert_eq!(calculate(&event), 0);
    let checked = PlanEntry::checked(table(&event, CopyFormat::Text), event).unwrap();
    let intent = fixture.publish([Ok(checked)].into_iter());
    assert_eq!(intent.plan.crc32, 0);
    open_sealed_copy_plan(&fixture.native, &intent, &io()).unwrap();
}

#[test]
fn large_plan_is_streamed_and_lookup_decodes_only_one_entry() {
    let mut fixture = Fixture::new();
    let intent = fixture.publish((1..=12000).map(|oid| {
        let mut event = relation(oid);
        let Event::Relation { columns, .. } = &mut event else {
            unreachable!()
        };
        columns[1].name = "c".repeat(900);
        PlanEntry::checked(table(&event, CopyFormat::Text), event)
    }));
    assert_eq!(intent.plan.table_count, 12000);
    assert!(intent.plan.logical_bytes > 8 * 1024 * 1024);
    let mut plan = open_sealed_copy_plan(&fixture.native, &intent, &io()).unwrap();
    DECODES.with(|count| count.set(0));
    plan.lookup_relation(
        RelationKey {
            oid: 12000,
            generation: 7,
        },
        &io(),
        |ordinal, _, _| {
            assert_eq!(ordinal, 12000);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(DECODES.with(Cell::get), 1);
    let mut visited = 0;
    plan.scan_from(11998, &io(), |_, _, _| {
        visited += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(visited, 2);
    for sql in [NAME_LOOKUP, RELATION_LOOKUP] {
        let mut query = plan
            .connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap();
        let details: Vec<String> = query
            .query_map(params![b"s".as_slice(), b"t".as_slice()], |row| row.get(3))
            .unwrap()
            .map(|row| row.unwrap())
            .collect();
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("SEARCH") && detail.contains("records_by_")),
            "{details:?}"
        );
        assert!(
            !details.iter().any(|detail| detail.contains("SCAN")),
            "{details:?}"
        );
    }
}

#[test]
fn callback_errors_and_panics_leave_restartable_pinned_reads() {
    let mut fixture = Fixture::new();
    let intent = fixture.publish([entry(11), entry(12)].into_iter());
    let mut plan = open_sealed_copy_plan(&fixture.native, &intent, &io()).unwrap();
    assert!(
        plan.scan_from(0, &io(), |_, _, _| Err(bad("visitor failed")))
            .is_err()
    );
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        plan.scan_from(0, &io(), |_, _, _| panic!("visitor control"))
    }));
    assert!(result.is_err());
    let mut seen = 0;
    plan.scan_from(0, &io(), |_, _, _| {
        seen += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, 2);
}

#[test]
fn sqlite_reader_checks_preserve_process_locks() {
    use std::os::fd::AsRawFd;
    if let Some(path) = std::env::var_os("RPG_COPY_PLAN_LOCK_PATH") {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        // SAFETY: flock contains integer fields; zero initializes its padding and range.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET.try_into().unwrap();
        // SAFETY: file is live and lock describes a valid whole-file write-lock request.
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) };
        if std::env::var("RPG_COPY_PLAN_EXPECT_LOCKED").unwrap() == "1" {
            assert_eq!(result, -1, "expected live SQLite reader lock");
            assert!(matches!(
                std::io::Error::last_os_error().raw_os_error(),
                Some(libc::EACCES | libc::EAGAIN)
            ));
        } else {
            assert_eq!(result, 0, "closed SQLite reader retained a lock");
        }
        return;
    }
    let mut fixture = Fixture::new();
    let intent = fixture.publish([entry(11)].into_iter());
    let path = fixture.path();
    let assert_lock = |locked: bool| {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "copy_plan::tests::sqlite_reader_checks_preserve_process_locks",
                "--nocapture",
            ])
            .env("RPG_COPY_PLAN_LOCK_PATH", &path)
            .env(
                "RPG_COPY_PLAN_EXPECT_LOCKED",
                if locked { "1" } else { "0" },
            )
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{}{}",
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr)
        );
    };
    let mut plan = open_sealed_copy_plan(&fixture.native, &intent, &io()).unwrap();
    plan.scan_from(0, &io(), |_, _, _| Ok(())).unwrap();
    plan.lookup_relation(
        RelationKey {
            oid: 11,
            generation: 7,
        },
        &io(),
        |_, _, _| Ok(()),
    )
    .unwrap();
    assert_lock(true);
    drop(plan);
    assert_lock(false);
}

#[test]
fn publication_refusal_after_close_returns_no_reference() {
    use std::os::unix::fs::PermissionsExt;
    let mut fixture = Fixture::new();
    let _reset = hook(|stage, _, path| {
        if stage == Step::AfterClose {
            fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
        }
        Ok(())
    });
    let error = publish_copy_plan(
        &mut fixture.native,
        &fixture.source,
        [42; 16],
        65536,
        [entry(11)].into_iter(),
        &io(),
    )
    .unwrap_err();
    assert!(error.message.contains("plan file ownership"), "{error:?}");
    fixture.assert_unpublished();
}

#[test]
fn namespace_replacement_refuses_before_lookup_visitor() {
    let mut fixture = Fixture::new();
    let intent = fixture.publish([entry(11)].into_iter());
    let path = fixture.path();
    let mut plan = open_sealed_copy_plan(&fixture.native, &intent, &io()).unwrap();
    fs::rename(&path, path.with_extension("replaced")).unwrap();
    assert!(
        plan.lookup_relation(
            RelationKey {
                oid: 11,
                generation: 7
            },
            &io(),
            |_, _, _| panic!("replaced namespace reached a consumer"),
        )
        .is_err()
    );
}

#[test]
fn malformed_relation_names_columns_and_events_are_refused() {
    for case in 0..6 {
        let mut event = relation(11);
        let Event::Relation {
            schema,
            name,
            columns,
            key,
            ..
        } = &mut event
        else {
            unreachable!()
        };
        match case {
            0 => schema.clear(),
            1 => name.push('\0'),
            2 => columns[1].name = "c".repeat(1025),
            3 => columns.resize(
                4097,
                Column {
                    name: "id".into(),
                    type_oid: 23,
                    type_mod: -1,
                    key: false,
                },
            ),
            4 => key.oid = 0,
            _ => key.generation = 0,
        }
        assert!(
            PlanEntry::checked(table(&event, CopyFormat::Text), event).is_err(),
            "case {case}"
        );
    }
    let input = table(&relation(11), CopyFormat::Text);
    assert!(
        PlanEntry::checked(
            input,
            Event::Truncate {
                relations: vec![],
                cascade: false,
                restart_identity: false
            }
        )
        .is_err()
    );
}
