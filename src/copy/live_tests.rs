use super::*;
use crate::apply::CloneAuthority;
use crate::catalog;
use crate::contracts::{
    CloneOptions, ClonePlan, Conninfo, CopyGeneration, RestorePolicy, StateAction, StateRequest,
};
use crate::snapshot::CloneSnapshot;
use crate::state::{self, ClonePhase};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static CASE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    options: CloneOptions,
    plan: ClonePlan,
    snapshot: CloneSnapshot,
    state: NativeState,
    authority: CloneAuthority,
    schema: String,
    io: IoControl,
}

fn control() -> IoControl {
    IoControl::unbounded(Arc::new(AtomicBool::new(false)))
}

fn fixture_copy_tables(
    tables: &[crate::contracts::CloneTable],
    format: CopyFormat,
    parts: &[crate::contracts::CopyPart],
) -> Vec<crate::contracts::CloneTableCopy> {
    let mut profiles = catalog::provisional_copy_tables(tables).unwrap();
    for profile in &mut profiles {
        profile.format = format;
        profile.parts = parts.to_vec();
    }
    profiles
}

#[test]
fn fixture_execution_profiles_follow_oid_order_without_reordering_catalog_tables() {
    let table = |source_oid, name: &str| crate::contracts::CloneTable {
        source_oid,
        name: crate::contracts::QualifiedTable {
            schema: "public".into(),
            table: name.into(),
        },
        writable_columns: vec!["id".into()],
        all_columns: vec!["id".into()],
        identity_modes: vec![0],
        partition_identity_modes: None,
    };
    let tables = vec![
        table(30_000, "a_created_second"),
        table(20_000, "z_created_first"),
    ];
    let original = tables.clone();
    let parts = vec![crate::contracts::CopyPart {
        number: 1,
        count: 1,
        start_block: 0,
        end_block: None,
    }];
    let profiles = fixture_copy_tables(&tables, CopyFormat::Binary, &parts);
    assert_eq!(
        profiles
            .iter()
            .map(|profile| profile.source_oid)
            .collect::<Vec<_>>(),
        [20_000, 30_000]
    );
    assert!(
        profiles
            .iter()
            .all(|profile| profile.format == CopyFormat::Binary && profile.parts == parts)
    );
    assert_eq!(tables, original);
}

impl Fixture {
    fn new(source_ddl: &[&str], target_ddl: &[&str], seed: &[&str]) -> Self {
        Self::new_profile(
            source_ddl,
            target_ddl,
            seed,
            CopyFormat::Text,
            vec![crate::contracts::CopyPart {
                number: 1,
                count: 1,
                start_block: 0,
                end_block: None,
            }],
        )
    }

    fn new_profile(
        source_ddl: &[&str],
        target_ddl: &[&str],
        seed: &[&str],
        format: CopyFormat,
        parts: Vec<crate::contracts::CopyPart>,
    ) -> Self {
        let context = std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned harness required");
        let context: serde_json::Value =
            serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
        let root = PathBuf::from(context["root"].as_str().unwrap());
        assert_eq!(
            std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
            context["token"].as_str().unwrap()
        );
        let source_info = Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN").expect("M2 source required"),
        )
        .unwrap();
        let target_info = Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_CLONE_TARGET_ADMIN").expect("M2 target required"),
        )
        .unwrap();
        let io = control();
        let mut source = PgConnection::connect(&source_info, &io).unwrap();
        let mut target = PgConnection::connect(&target_info, &io).unwrap();
        let schema = format!(
            "m2_copy_{}_{}",
            std::process::id(),
            CASE.fetch_add(1, Ordering::Relaxed)
        );
        source
            .query(&format!("CREATE SCHEMA {schema}"), &io)
            .unwrap();
        source
            .query(&format!("SET search_path={schema},pg_catalog"), &io)
            .unwrap();
        for sql in source_ddl {
            source.query(sql, &io).unwrap();
        }
        for sql in seed {
            source.query(sql, &io).unwrap();
        }
        let mut snapshot = CloneSnapshot::export(
            PgConnection::connect(&source_info, &io).unwrap(),
            CopyGeneration(1),
            &io,
        )
        .unwrap();
        let tables: Vec<crate::contracts::CloneTable> =
            catalog::inspect_clone_tables(snapshot.connection().unwrap(), &[], &io)
                .unwrap()
                .into_iter()
                .filter(|table| table.name.schema == schema)
                .collect();
        let plan = ClonePlan {
            source: snapshot.ticket().source.clone(),
            target: catalog::read_clone_target(&mut target, &io).unwrap(),
            generation: CopyGeneration(1),
            copy_tables: fixture_copy_tables(&tables, format, &parts),
            copy_policy: crate::contracts::CloneCopyPolicy {
                use_copy_binary: format == CopyFormat::Binary,
                split_tables_larger_than: if parts.len() > 1 { 1 } else { 0 },
                ..crate::contracts::CloneCopyPolicy::default()
            },
            tables,
            sequences: vec![],
            policy: RestorePolicy::default(),
        };
        let options = CloneOptions {
            source: source_info,
            target: target_info,
            state: StateRequest {
                path: root.join(format!("{schema}-state")),
                action: StateAction::Create,
            },
            operator: None,
            table_jobs: 3,
            post_data_jobs: 3,
            policy: RestorePolicy::default(),
            not_consistent: false,
            use_copy_binary: format == CopyFormat::Binary,
            split_tables_larger_than: if parts.len() > 1 { 1 } else { 0 },
            split_max_parts: 8,
            skip_large_objects: false,
            large_objects_jobs: 4,
        };
        let mut state = state::open_clone(&options.state, None, &io).unwrap();
        state.bind_clone(&plan, snapshot.ticket()).unwrap();
        let mut authority = CloneAuthority::acquire(
            PgConnection::connect(&options.target, &io).unwrap(),
            &mut state,
            &plan,
            &io,
        )
        .unwrap();
        let before = catalog::clone_object_inventory(&mut target, &io).unwrap();
        state.begin_pre_data().unwrap();
        authority
            .mark_stage(ClonePhase::PreDataPending, &io)
            .unwrap();
        target
            .query(&format!("CREATE SCHEMA {schema}"), &io)
            .unwrap();
        target
            .query(&format!("SET search_path={schema},pg_catalog"), &io)
            .unwrap();
        for sql in target_ddl {
            target.query(sql, &io).unwrap();
        }
        let objects = catalog::clone_object_inventory(&mut target, &io)
            .unwrap()
            .into_iter()
            .filter(|object| !before.contains(object))
            .collect::<Vec<_>>();
        let tables = catalog::reconcile_clone_tables(&mut target, &plan, &objects, &io).unwrap();
        state.confirm_pre_data(&plan, objects, tables).unwrap();
        authority.bind_tables(&state, &io).unwrap();
        Self {
            options,
            plan,
            snapshot,
            state,
            authority,
            schema,
            io,
        }
    }

    fn copy(&mut self) -> Result<CopyReport> {
        run_tables(
            &self.options,
            &self.plan,
            &mut self.snapshot,
            &mut self.state,
            &mut self.authority,
            &self.io,
        )
    }

    fn target(&self) -> PgConnection {
        PgConnection::connect(&self.options.target, &control()).unwrap()
    }
}

#[test]
#[ignore = "requires the disposable M2 PostgreSQL harness"]
fn live_parallel_clone_preserves_snapshot_physical_heaps_identity_generated_and_wide_rows() {
    let ddl = [
        "CREATE TABLE heap (id bigint GENERATED ALWAYS AS IDENTITY, payload text, generated bigint GENERATED ALWAYS AS (id*2) STORED)",
        "CREATE TABLE empty (payload text)",
        "CREATE TABLE parent (id integer, payload text)",
        "CREATE TABLE child (extra integer) INHERITS (parent)",
        "CREATE TABLE root (id integer, payload text) PARTITION BY RANGE (id)",
        "CREATE TABLE leaf_a PARTITION OF root FOR VALUES FROM (0) TO (10)",
        "CREATE TABLE leaf_b PARTITION OF root FOR VALUES FROM (10) TO (20)",
    ];
    let seed = [
        "INSERT INTO heap (id,payload) OVERRIDING SYSTEM VALUE VALUES (41,NULL),(42,''),(43,E'tab\\tnewline\\n\\\\'),(44,repeat('w',9*1024*1024))",
        "INSERT INTO parent VALUES (1,'parent')",
        "INSERT INTO child VALUES (2,'child',3),(2,'child',3)",
        "INSERT INTO root VALUES (4,'leaf a'),(14,'leaf b')",
    ];
    let mut fixture = Fixture::new(&ddl, &ddl, &seed);
    assert_eq!(fixture.plan.tables.len(), 6);
    let parent = fixture
        .state
        .owned_copy_tables()
        .unwrap()
        .into_iter()
        .find(|table| table.table.name.table == "parent")
        .unwrap();
    let source_info = fixture.options.source.clone();
    let target_info = fixture.options.target.clone();
    let schema = fixture.schema.clone();
    let (ready, waiting) = std::sync::mpsc::sync_channel(1);
    let writer = std::thread::spawn(move || {
        let io = control();
        let mut target = PgConnection::connect(&target_info, &io).unwrap();
        target.query("BEGIN", &io).unwrap();
        target
            .query(
                &format!("LOCK TABLE ONLY {schema}.parent IN ACCESS EXCLUSIVE MODE"),
                &io,
            )
            .unwrap();
        ready.send(()).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "COPY worker did not reach the held table lock"
            );
            let locks = target.query(&format!("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_locks WHERE relation={}::pg_catalog.oid AND NOT granted AND mode='AccessExclusiveLock')", parent.target_oid), &io).unwrap();
            if locks.cell(0, 0).unwrap() == Some(b"t".as_slice()) {
                break;
            }
            std::thread::yield_now();
        }
        let mut source = PgConnection::connect(&source_info, &io).unwrap();
        source
            .query(
                &format!("INSERT INTO {schema}.parent VALUES (99,'written during COPY')"),
                &io,
            )
            .unwrap();
        target.query("COMMIT", &io).unwrap();
    });
    waiting
        .recv_timeout(std::time::Duration::from_secs(10))
        .unwrap();
    let copied = fixture.copy();
    writer.join().unwrap();
    let report = copied.unwrap();
    assert_eq!(report.tables, 6);
    assert_eq!(report.rows, 9);
    assert!(report.bytes > 9 * 1024 * 1024);
    assert_eq!(fixture.authority.copy_report(&fixture.io).unwrap(), report);
    let mut target = fixture.target();
    for table in &fixture.plan.tables {
        let name = catalog::qualified(&mut target, &table.name.schema, &table.name.table).unwrap();
        let columns = table
            .all_columns
            .iter()
            .map(|name| target.quote_identifier(name))
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .join(",");
        let sql = format!(
            "SELECT pg_catalog.row_to_json(t)::pg_catalog.text FROM (SELECT {columns} FROM ONLY {name} ORDER BY {columns}) t"
        );
        let source = fixture
            .snapshot
            .connection()
            .unwrap()
            .query(&sql, &fixture.io)
            .unwrap();
        let actual = target.query(&sql, &fixture.io).unwrap();
        assert_eq!(actual.rows(), source.rows(), "table {}", table.name.table);
        for row in 0..actual.rows() {
            assert!(
                actual.cell(row, 0).unwrap() == source.cell(row, 0).unwrap(),
                "table {} row {row} differs",
                table.name.table
            );
        }
    }
}

#[test]
#[ignore = "requires the disposable M2 PostgreSQL harness"]
fn live_clone_late_target_error_rolls_back_truncate_copy_and_marker() {
    let mut fixture = Fixture::new(
        &["CREATE TABLE payload (id integer, value text)"],
        &["CREATE TABLE payload (id integer CHECK (id<2), value text)"],
        &["INSERT INTO payload VALUES (1,'first'),(2,'late error')"],
    );
    let mut target = fixture.target();
    target
        .query(
            &format!(
                "INSERT INTO {}.payload VALUES (0,'sentinel')",
                fixture.schema
            ),
            &fixture.io,
        )
        .unwrap();
    let error = fixture.copy().unwrap_err();
    assert!(
        error.message.contains("23514"),
        "late target CHECK violation was not observed"
    );
    let io = control();
    let rows = target
        .query(
            &format!("SELECT id,value FROM ONLY {}.payload", fixture.schema),
            &io,
        )
        .unwrap();
    assert_eq!(rows.rows(), 1);
    assert_eq!(rows.cell(0, 0).unwrap(), Some(b"0".as_slice()));
    assert_eq!(rows.cell(0, 1).unwrap(), Some(b"sentinel".as_slice()));
    let owned = fixture.state.owned_copy_tables().unwrap();
    assert!(
        fixture
            .authority
            .resolve_unknown(
                &owned[0],
                &crate::contracts::CopyPart {
                    number: 1,
                    count: 1,
                    start_block: 0,
                    end_block: None
                },
                &io
            )
            .unwrap()
            .is_none()
    );
    let saved = fixture.state.checked_clone().unwrap().unwrap();
    assert_eq!(saved.phase, ClonePhase::Copying);
    assert!(saved.completion.is_none());

    let ddl = [
        "CREATE TABLE payload (id integer, value text, physical oid GENERATED ALWAYS AS (tableoid) STORED)",
    ];
    let mut generated = Fixture::new(
        &ddl,
        &ddl,
        &["INSERT INTO payload (id,value) VALUES (1,'copied')"],
    );
    let owned = generated.state.owned_copy_tables().unwrap();
    assert_ne!(
        owned[0].table.source_oid, owned[0].target_oid,
        "tableoid negative control requires different heap identities"
    );
    let mut target = generated.target();
    target
        .query(
            &format!(
                "INSERT INTO {}.payload (id,value) VALUES (0,'sentinel')",
                generated.schema
            ),
            &generated.io,
        )
        .unwrap();
    let error = generated.copy().unwrap_err();
    assert!(
        error.message.contains("generated values differ"),
        "generated-value parity refusal was not observed"
    );
    let io = control();
    let rows = target
        .query(
            &format!(
                "SELECT id,value,physical FROM ONLY {}.payload",
                generated.schema
            ),
            &io,
        )
        .unwrap();
    assert_eq!(rows.rows(), 1);
    assert!(rows.cell(0, 0).unwrap() == Some(b"0".as_slice()));
    assert!(rows.cell(0, 1).unwrap() == Some(b"sentinel".as_slice()));
    assert!(rows.cell(0, 2).unwrap() == Some(owned[0].target_oid.to_string().as_bytes()));
    assert!(
        generated
            .authority
            .resolve_unknown(
                &owned[0],
                &crate::contracts::CopyPart {
                    number: 1,
                    count: 1,
                    start_block: 0,
                    end_block: None
                },
                &io
            )
            .unwrap()
            .is_none()
    );
    assert!(
        generated
            .state
            .checked_clone()
            .unwrap()
            .unwrap()
            .completion
            .is_none()
    );
}

#[test]
#[ignore = "requires the disposable M2 PostgreSQL harness"]
fn live_clone_oversize_row_refuses_before_table_commit() {
    let ddl = ["CREATE TABLE payload (value text)"];
    let mut fixture = Fixture::new(
        &ddl,
        &ddl,
        &["INSERT INTO payload VALUES (repeat('x',16*1024*1024+1))"],
    );
    let error = fixture.copy().unwrap_err();
    assert!(
        error.message.contains("COPY")
            && (error.message.contains("bound") || error.message.contains("limit")),
        "{}",
        error.message
    );
    let io = control();
    let mut target = fixture.target();
    let rows = target
        .query(
            &format!("SELECT count(*) FROM ONLY {}.payload", fixture.schema),
            &io,
        )
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(b"0".as_slice()));
    let owned = fixture.state.owned_copy_tables().unwrap();
    assert!(
        fixture
            .authority
            .resolve_unknown(
                &owned[0],
                &crate::contracts::CopyPart {
                    number: 1,
                    count: 1,
                    start_block: 0,
                    end_block: None
                },
                &io
            )
            .unwrap()
            .is_none()
    );

    let ddl = [
        "CREATE TABLE payload (id integer, generated text GENERATED ALWAYS AS (repeat('g',16*1024*1024+1)) STORED)",
    ];
    let mut generated = Fixture::new(&ddl, &ddl, &["INSERT INTO payload (id) VALUES (1)"]);
    let error = generated.copy().unwrap_err();
    assert!(
        error.message.contains("COPY") && error.message.contains("limit"),
        "generated full-row limit was not observed"
    );
    let io = control();
    let mut target = generated.target();
    let rows = target
        .query(
            &format!("SELECT count(*) FROM ONLY {}.payload", generated.schema),
            &io,
        )
        .unwrap();
    assert!(rows.cell(0, 0).unwrap() == Some(b"0".as_slice()));
    let owned = generated.state.owned_copy_tables().unwrap();
    assert!(
        generated
            .authority
            .resolve_unknown(
                &owned[0],
                &crate::contracts::CopyPart {
                    number: 1,
                    count: 1,
                    start_block: 0,
                    end_block: None
                },
                &io
            )
            .unwrap()
            .is_none()
    );
}

#[test]
#[ignore = "requires the disposable M2 PostgreSQL harness"]
fn live_clone_established_empty_inventory_returns_zero_receipt() {
    let mut fixture = Fixture::new(&[], &[], &[]);
    let report = fixture.copy().unwrap();
    assert_eq!(
        report,
        CopyReport {
            tables: 0,
            rows: 0,
            bytes: 0
        }
    );
    assert_eq!(fixture.authority.copy_report(&fixture.io).unwrap(), report);
}

#[test]
#[ignore = "requires the disposable M2 PostgreSQL harness"]
fn live_clone_rejects_replaced_snapshot_in_same_generation_before_copy() {
    let ddl = ["CREATE TABLE payload (value text)"];
    let mut fixture = Fixture::new(&ddl, &ddl, &["INSERT INTO payload VALUES ('original')"]);
    fixture.snapshot = CloneSnapshot::export(
        PgConnection::connect(&fixture.options.source, &fixture.io).unwrap(),
        CopyGeneration(1),
        &fixture.io,
    )
    .unwrap();
    let error = fixture.copy().unwrap_err();
    assert!(
        error.message.contains("recorded snapshot"),
        "{}",
        error.message
    );
    let io = control();
    let mut target = fixture.target();
    let rows = target
        .query(
            &format!("SELECT count(*) FROM ONLY {}.payload", fixture.schema),
            &io,
        )
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(b"0".as_slice()));
}

#[test]
#[ignore = "requires the disposable M8 PostgreSQL harness"]
fn live_binary_copy_counts_empty_single_and_multiple_rows_from_command_results() {
    let ddl = ["CREATE TABLE payload (id integer,value text)"];
    for rows in [0, 1, 13] {
        let seed =
            format!("INSERT INTO payload SELECT n,'value-'||n FROM generate_series(1,{rows}) AS n");
        let mut fixture = Fixture::new_profile(
            &ddl,
            &ddl,
            &[&seed],
            CopyFormat::Binary,
            vec![crate::contracts::CopyPart {
                number: 1,
                count: 1,
                start_block: 0,
                end_block: None,
            }],
        );
        let report = fixture.copy().unwrap();
        assert_eq!(report.tables, 1);
        assert_eq!(report.rows, rows);
        assert!(
            report.bytes >= 21,
            "binary header and trailer were not relayed"
        );
        assert_eq!(fixture.authority.copy_report(&fixture.io).unwrap(), report);
    }
}

#[test]
#[ignore = "requires the disposable M8 PostgreSQL harness"]
fn live_split_copy_retains_every_part_without_freeze_or_worker_truncate() {
    let ddl = ["CREATE TABLE payload (id integer,value text)"];
    let mut fixture = Fixture::new_profile(
        &ddl,
        &ddl,
        &["INSERT INTO payload SELECT n,repeat('x',1000) FROM generate_series(1,2000) AS n"],
        CopyFormat::Text,
        vec![
            crate::contracts::CopyPart {
                number: 1,
                count: 2,
                start_block: 0,
                end_block: Some(1),
            },
            crate::contracts::CopyPart {
                number: 2,
                count: 2,
                start_block: 1,
                end_block: None,
            },
        ],
    );
    let report = fixture.copy().unwrap();
    assert_eq!(report.tables, 1);
    assert_eq!(report.rows, 2000);
    assert_eq!(fixture.authority.copy_report(&fixture.io).unwrap(), report);
    let result = fixture
        .target()
        .query(
            &format!(
                "SELECT count(*),count(DISTINCT id) FROM {}.payload",
                fixture.schema
            ),
            &fixture.io,
        )
        .unwrap();
    assert_eq!(result.cell(0, 0).unwrap(), Some(b"2000".as_slice()));
    assert_eq!(result.cell(0, 1).unwrap(), Some(b"2000".as_slice()));
}

#[test]
#[ignore = "requires a fresh disposable M8 database pair with no existing large objects"]
fn live_large_object_initial_copy_preserves_empty_binary_multichunk_and_sparse_objects() {
    let ddl = ["CREATE TABLE payload (id integer,object_oid oid)"];
    let mut fixture = Fixture::new(
        &ddl,
        &ddl,
        &[
            "INSERT INTO payload VALUES(1,pg_catalog.lo_create(0))",
            "INSERT INTO payload VALUES(2,pg_catalog.lo_from_bytea(0,pg_catalog.decode('00ff275c','hex')))",
            "INSERT INTO payload VALUES(3,pg_catalog.lo_from_bytea(0,pg_catalog.decode(repeat('5a',2*1024*1024+7),'hex')))",
            "INSERT INTO payload VALUES(4,pg_catalog.lo_create(0))",
            "SELECT pg_catalog.lo_put(object_oid,1024*1024+7,pg_catalog.decode('00ff','hex')) FROM payload WHERE id=4",
        ],
    );
    fixture.copy().unwrap();
    let oid_json = fixture
        .snapshot
        .connection()
        .unwrap()
        .query(
        "SELECT pg_catalog.json_typeof(pg_catalog.json_build_object('owner',m.lomowner)->'owner'),pg_catalog.json_typeof(pg_catalog.json_build_object('owner',m.lomowner::pg_catalog.int8)->'owner') FROM pg_catalog.pg_largeobject_metadata m ORDER BY m.oid LIMIT 1",
        &fixture.io,
    )
        .unwrap();
    assert_eq!(oid_json.rows(), 1);
    assert_eq!(oid_json.cell(0, 0).unwrap(), Some(b"string".as_slice()));
    assert_eq!(oid_json.cell(0, 1).unwrap(), Some(b"number".as_slice()));
    let completion = crate::large_objects::run_initial(
        &fixture.options,
        &fixture.plan,
        &mut fixture.snapshot,
        &mut fixture.authority,
        &fixture.io,
    )
    .unwrap();
    assert_eq!(completion.objects, 4);
    assert_eq!(
        completion.bytes,
        4 + (2 * 1024 * 1024 + 7) + (1024 * 1024 + 9)
    );
    let mut target = fixture.target();
    let binary=target.query(&format!("SELECT pg_catalog.encode(pg_catalog.lo_get(object_oid,0,4),'hex') FROM {}.payload WHERE id=2",fixture.schema),&fixture.io).unwrap();
    assert_eq!(binary.cell(0, 0).unwrap(), Some(b"00ff275c".as_slice()));
    let hole=target.query(&format!("SELECT pg_catalog.encode(pg_catalog.lo_get(object_oid,1024*1024+5,4),'hex') FROM {}.payload WHERE id=4",fixture.schema),&fixture.io).unwrap();
    assert_eq!(hole.cell(0, 0).unwrap(), Some(b"000000ff".as_slice()));
    let retry = crate::large_objects::run_initial(
        &fixture.options,
        &fixture.plan,
        &mut fixture.snapshot,
        &mut fixture.authority,
        &fixture.io,
    )
    .unwrap();
    assert_eq!(retry, completion);
}
