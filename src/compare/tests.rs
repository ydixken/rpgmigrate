use super::*;

#[test]
fn internal_file_maps_refuse_before_state_access() {
    const CHILD: &str = "RPGMIGRATE_TEST_COMPARE_FILES_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let path = std::env::temp_dir().join(format!(
            "rpgmigrate-uncreated-compare-files-{}",
            std::process::id()
        ));
        assert!(!path.exists());
        let options = CloneOptions {
            source: crate::contracts::Conninfo::new("host=source.invalid".into()).unwrap(),
            target: crate::contracts::Conninfo::new("host=target.invalid".into()).unwrap(),
            state: crate::contracts::StateRequest {
                path: path.clone(),
                action: crate::contracts::StateAction::Create,
            },
            operator: None,
            table_jobs: 1,
            post_data_jobs: 1,
            policy: crate::contracts::RestorePolicy::default(),
            not_consistent: false,
            use_copy_binary: false,
            split_tables_larger_than: 0,
            split_max_parts: 8,
            skip_large_objects: false,
            large_objects_jobs: 4,
        };
        let io = IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )));
        assert_eq!(
            schema(&options, &io).unwrap_err().kind,
            ErrorKind::BadConfig
        );
        assert_eq!(data(&options, &io).unwrap_err().kind, ErrorKind::BadConfig);
        assert!(!path.exists());
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "compare::tests::internal_file_maps_refuse_before_state_access",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("RPGMIGRATE_INTERNAL_COMPARE_SOURCE_FILES", "{}")
        .env(
            "RPGMIGRATE_INTERNAL_COMPARE_TARGET_FILES",
            "{\"password\":3}",
        )
        .output()
        .unwrap();
    assert!(output.status.success(), "{:?}", output);
}

fn table(name: &str, oid: u32) -> CloneTable {
    CloneTable {
        source_oid: oid,
        name: crate::contracts::QualifiedTable {
            schema: "public".into(),
            table: name.into(),
        },
        writable_columns: vec!["value".into()],
        all_columns: vec!["value".into()],
        identity_modes: vec![0],
        partition_identity_modes: None,
    }
}

#[test]
fn inventories_measure_missing_tables_and_refuse_absent_coverage() {
    assert!(
        union_tables(&[], &[])
            .unwrap_err()
            .message
            .contains("coverage")
    );
    let source = [table("a", 1), table("b", 2)];
    let target = [table("a", 20), table("c", 30)];
    let inventory = union_tables(&source, &target).unwrap();
    assert_eq!(inventory.len(), 3);
    assert_eq!(
        (
            inventory[0].0.unwrap().source_oid,
            inventory[0].1.unwrap().source_oid
        ),
        (1, 20)
    );
    assert!(inventory[1].1.is_none());
    assert!(inventory[2].0.is_none());
    assert!(union_tables(&[table("a", 1), table("a", 2)], &[]).is_err());
}

#[test]
fn proven_empty_tables_require_successful_selected_large_object_comparison() {
    let policy = crate::contracts::CloneCopyPolicy::default();
    for message in [
        "large-object mismatch",
        "large-object inventory query failed",
    ] {
        let error = verified_tables(&[], &[], &policy, || Err(refusal(message))).unwrap_err();
        assert_eq!(error.message, message);
    }
    let inventory = verified_tables(&[], &[], &policy, || Ok(())).unwrap();
    assert!(inventory.is_empty());
    assert!(union_tables(&[], &[]).is_err());
    assert_eq!(
        serde_json::to_value(Vec::<TableComparison>::new()).unwrap(),
        serde_json::json!([])
    );
}

#[test]
fn table_coverage_cannot_mask_large_object_failure_or_override_selected_skip() {
    let source = [table("a", 1)];
    let target = [table("a", 2)];
    let policy = crate::contracts::CloneCopyPolicy::default();
    assert!(verified_tables(&source, &target, &policy, || Err(refusal("LO mismatch"))).is_err());
    let skipped = crate::contracts::CloneCopyPolicy {
        skip_large_objects: true,
        ..policy
    };
    assert_eq!(
        verified_tables(&source, &target, &skipped, || panic!(
            "recorded skip must bypass LO comparison"
        ))
        .unwrap()
        .len(),
        1
    );
    assert!(
        verified_tables(&[], &[], &skipped, || panic!(
            "recorded skip must bypass LO comparison"
        ))
        .unwrap()
        .is_empty()
    );
}

#[test]
fn legacy_progress_keys_and_table_array_fields_remain_exact() {
    let progress = CompareProgress::new(1, &[8192], Instant::now()).unwrap();
    let table = table("a", 1);
    let value = progress
        .value(false, Some((&table, 8192, Instant::now())))
        .unwrap();
    let expected = [
        "plannedTables",
        "comparedTables",
        "plannedBytes",
        "comparedBytes",
        "comparedRows",
        "differingTables",
        "failedTables",
        "elapsedMs",
        "final",
        "inFlightCount",
        "inFlight",
    ];
    assert_eq!(value.as_object().unwrap().len(), expected.len());
    for key in expected {
        assert!(value.get(key).is_some(), "{key}");
    }
    assert_eq!(value["plannedBytes"], 8192);
    assert_eq!(value["inFlightCount"], 1);
    assert_eq!(value["inFlight"][0]["bytes"], 8192);
    let report = TableComparison {
        schema: "public".into(),
        name: "a".into(),
        source: Some(Measurement::default().into()),
        target: None,
        status: "different",
    };
    assert!(!report.matched());
    let json = serde_json::to_value([report]).unwrap();
    assert_eq!(
        json[0]["source"],
        serde_json::json!({"rowcount":0,"checksum":"00000000"})
    );
    assert!(json[0]["target"].is_null());
}

#[test]
fn canonical_stream_refuses_empty_buffers_and_progress_failure() {
    assert!(compare(&[b""], &[]).is_err());
    assert!(
        compare_streams(
            || Ok(None::<&[u8]>),
            || Ok(None),
            |bytes| *bytes,
            || Err(refusal("progress failed"))
        )
        .is_err()
    );
    assert!(checked_add(i64::MAX as u64, 1).is_err());
}

fn compare(left: &[&[u8]], right: &[&[u8]]) -> Result<(bool, Measurement, Measurement)> {
    let mut left = left.iter();
    let mut right = right.iter();
    compare_streams(
        || Ok(left.next().copied()),
        || Ok(right.next().copied()),
        |bytes| *bytes,
        || Ok(()),
    )
}

#[test]
fn canonical_stream_measures_empty_and_duplicate_null_rows() {
    let (matched, source, target) = compare(&[], &[]).unwrap();
    assert!(matched);
    assert_eq!(source, target);
    assert_eq!(source.rows, 0);
    let rows = b"\\N\t\n\\N\t\n";
    let (matched, source, target) = compare(&[rows], &[rows]).unwrap();
    assert!(matched);
    assert_eq!(source, target);
    assert_eq!(source.rows, 2);
    assert_eq!(source.bytes, rows.len() as u64);
}

#[test]
fn canonical_stream_compares_actual_bytes_across_buffer_boundaries() {
    assert!(compare(&[b"a\nb", b"\n"], &[b"a", b"\nb\n"]).unwrap().0);
    for (left, right) in [
        (b"a\n".as_slice(), b"b\n".as_slice()),
        (b"\\N\n".as_slice(), b"\n".as_slice()),
        (b"a\na\n".as_slice(), b"a\n".as_slice()),
    ] {
        assert!(!compare(&[left], &[right]).unwrap().0);
    }
}

#[test]
fn canonical_stream_never_uses_crc_as_equality_authority() {
    let left = b"plumless\n";
    let right = b"buckeroo\n";
    assert_eq!(crc32fast::hash(left), crc32fast::hash(right));
    let (matched, source, target) = compare(&[left], &[right]).unwrap();
    assert_eq!(source.rows, target.rows);
    assert_eq!(source.checksum, target.checksum);
    assert!(!matched);
}

#[test]
fn canonical_stream_propagates_late_errors_after_a_difference() {
    let mut calls = 0;
    let error = compare_streams(
        || {
            calls += 1;
            if calls == 1 {
                Ok(Some(b"a\n".as_slice()))
            } else {
                Err(refusal("terminal source failure"))
            }
        },
        || Ok(None),
        |bytes| *bytes,
        || Ok(()),
    )
    .unwrap_err();
    assert_eq!(error.message, "terminal source failure");
}

#[test]
#[ignore = "requires the owned PostgreSQL harness"]
fn live_canonical_rows_preserve_only_generated_identity_null_and_duplicates() {
    std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned harness context is required");
    let conninfo = crate::contracts::Conninfo::new(
        std::env::var_os("RPGMIGRATE_TEST_SOURCE").expect("owned harness connection is required"),
    )
    .unwrap();
    let io = IoControl {
        deadline: Instant::now() + Duration::from_secs(60),
        cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };
    let mut source = PgConnection::connect(&conninfo, &io).unwrap();
    let mut target = PgConnection::connect(&conninfo, &io).unwrap();
    for (connection, style) in [(&mut source, "SQL, DMY"), (&mut target, "German, DMY")] {
        connection
            .query(&format!("SET DateStyle TO '{style}'"), &io)
            .unwrap();
        connection.query("CREATE TEMP TABLE canonical_rows (id bigint GENERATED ALWAYS AS IDENTITY, value text, derived text GENERATED ALWAYS AS (value || '!') STORED, stamp timestamptz, span interval, raw bytea, nullable text)",&io).unwrap();
        connection.query("INSERT INTO canonical_rows (id,value,stamp,span,raw,nullable) OVERRIDING SYSTEM VALUE VALUES (41,E'line\\n\\t\\\\☃','2024-01-02 03:04:05+03','1 day 02:03:04',pg_catalog.decode('00ff','hex'),NULL),(42,'duplicate','2024-01-02 03:04:05+03','1 day 02:03:04',pg_catalog.decode('','hex'),''),(42,'duplicate','2024-01-02 03:04:05+03','1 day 02:03:04',pg_catalog.decode('','hex'),'')",&io).unwrap();
        connection
            .query("CREATE TEMP TABLE canonical_parent (value text)", &io)
            .unwrap();
        connection
            .query(
                "CREATE TEMP TABLE canonical_child () INHERITS (canonical_parent)",
                &io,
            )
            .unwrap();
        connection
            .query("INSERT INTO canonical_parent VALUES ('parent')", &io)
            .unwrap();
        connection
            .query("INSERT INTO canonical_child VALUES ('child')", &io)
            .unwrap();
        catalog::configure_session(connection, &io).unwrap();
    }
    let columns = ["id", "value", "derived", "stamp", "span", "raw", "nullable"];
    let left = live_table(&mut source, "canonical_rows", &columns, &io);
    let right = live_table(&mut target, "canonical_rows", &columns, &io);
    let (matched, measure, _) = live_measure(&mut source, &mut target, &left, &right, &io);
    assert!(matched);
    assert_eq!(measure.rows, 3);
    target
        .query("UPDATE canonical_rows SET nullable='' WHERE id=41", &io)
        .unwrap();
    assert!(!live_measure(&mut source, &mut target, &left, &right, &io).0);
    target
        .query("UPDATE canonical_rows SET nullable=NULL WHERE id=41", &io)
        .unwrap();
    target.query("DELETE FROM ONLY canonical_rows WHERE ctid=(SELECT ctid FROM ONLY canonical_rows WHERE id=42 LIMIT 1)",&io).unwrap();
    let (matched, left_measure, right_measure) =
        live_measure(&mut source, &mut target, &left, &right, &io);
    assert!(!matched);
    assert_eq!((left_measure.rows, right_measure.rows), (3, 2));
    target.query("INSERT INTO canonical_rows (id,value,stamp,span,raw,nullable) OVERRIDING SYSTEM VALUE VALUES (42,'duplicate','2024-01-02 03:04:05+03','1 day 02:03:04',pg_catalog.decode('','hex'),'')",&io).unwrap();
    target
        .query("ALTER TABLE canonical_rows DROP COLUMN derived", &io)
        .unwrap();
    target.query("ALTER TABLE canonical_rows ADD COLUMN derived text GENERATED ALWAYS AS (value || '?') STORED",&io).unwrap();
    assert!(!live_measure(&mut source, &mut target, &left, &right, &io).0);
    let parent_left = live_table(&mut source, "canonical_parent", &["value"], &io);
    let parent_right = live_table(&mut target, "canonical_parent", &["value"], &io);
    target
        .query(
            "UPDATE ONLY canonical_child SET value='different child'",
            &io,
        )
        .unwrap();
    let (matched, measure, _) =
        live_measure(&mut source, &mut target, &parent_left, &parent_right, &io);
    assert!(matched);
    assert_eq!(measure.rows, 1);
}

fn live_table(
    connection: &mut PgConnection,
    name: &str,
    columns: &[&str],
    io: &IoControl,
) -> CloneTable {
    let name_literal = connection.quote_literal(name).unwrap();
    let result = connection.query(&format!("SELECT n.nspname,c.oid,c.relispartition FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.oid=pg_catalog.pg_my_temp_schema() AND c.relname={name_literal}"),io).unwrap();
    assert_eq!(result.rows(), 1);
    assert_eq!(result.cell(0, 2).unwrap(), Some(b"f".as_slice()));
    let source_oid = std::str::from_utf8(result.cell(0, 1).unwrap().unwrap())
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let attributes = connection.query(&format!("SELECT attname,attidentity FROM pg_catalog.pg_attribute WHERE attrelid={source_oid} AND attnum>0 AND NOT attisdropped ORDER BY attnum LIMIT 1601"),io).unwrap();
    assert_eq!(attributes.rows(), columns.len());
    let identity_modes = columns
        .iter()
        .enumerate()
        .map(|(row, name)| {
            assert_eq!(attributes.cell(row, 0).unwrap(), Some(name.as_bytes()));
            match attributes.cell(row, 1).unwrap() {
                Some(b"") => 0,
                Some(b"a") => b'a',
                Some(b"d") => b'd',
                _ => panic!("comparison fixture has an invalid identity mode"),
            }
        })
        .collect();
    CloneTable {
        source_oid,
        name: crate::contracts::QualifiedTable {
            schema: std::str::from_utf8(result.cell(0, 0).unwrap().unwrap())
                .unwrap()
                .into(),
            table: name.into(),
        },
        writable_columns: columns.iter().map(|value| (*value).into()).collect(),
        all_columns: columns.iter().map(|value| (*value).into()).collect(),
        identity_modes,
        partition_identity_modes: None,
    }
}

fn live_measure(
    source: &mut PgConnection,
    target: &mut PgConnection,
    left: &CloneTable,
    right: &CloneTable,
    io: &IoControl,
) -> (bool, Measurement, Measurement) {
    start_table(source, left, io).unwrap();
    start_table(target, right, io).unwrap();
    compare_streams(
        || source.read_copy_bounded(MAX_ROW_BYTES, io),
        || target.read_copy_bounded(MAX_ROW_BYTES, io),
        CopyBuffer::as_bytes,
        || Ok(()),
    )
    .unwrap()
}
