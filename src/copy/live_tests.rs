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

fn harness_context() -> serde_json::Value {
    let context = std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned harness required");
    let context: serde_json::Value =
        serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
    let root = PathBuf::from(context["root"].as_str().unwrap());
    assert_eq!(
        std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
        context["token"].as_str().unwrap()
    );
    context
}

fn harness_root() -> PathBuf {
    PathBuf::from(harness_context()["root"].as_str().unwrap())
}

fn admin(variable: &str) -> Conninfo {
    Conninfo::new(std::env::var_os(variable).expect("clone harness endpoint required")).unwrap()
}

fn connect(info: &Conninfo) -> PgConnection {
    PgConnection::connect(info, crate::pg::Role::Copy, &control()).unwrap()
}

// libpq keeps the last value of a repeated keyword.
fn with_keyword(info: &Conninfo, keyword: &str, value: &str) -> Conninfo {
    use std::os::unix::ffi::OsStringExt;
    let mut raw = info.as_c_str().to_bytes().to_vec();
    raw.extend_from_slice(format!(" {keyword}='{value}'").as_bytes());
    Conninfo::new(std::ffi::OsString::from_vec(raw)).unwrap()
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
        Self::new_on(
            admin("RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN"),
            admin("RPGMIGRATE_TEST_CLONE_TARGET_ADMIN"),
            source_ddl,
            target_ddl,
            seed,
            format,
            parts,
        )
    }

    fn new_on(
        source_info: Conninfo,
        target_info: Conninfo,
        source_ddl: &[&str],
        target_ddl: &[&str],
        seed: &[&str],
        format: CopyFormat,
        parts: Vec<crate::contracts::CopyPart>,
    ) -> Self {
        let root = harness_root();
        let io = control();
        let mut source = PgConnection::connect(&source_info, crate::pg::Role::Copy, &io).unwrap();
        let mut target = PgConnection::connect(&target_info, crate::pg::Role::Copy, &io).unwrap();
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
            PgConnection::connect(&source_info, crate::pg::Role::Copy, &io).unwrap(),
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
            PgConnection::connect(&options.target, crate::pg::Role::Copy, &io).unwrap(),
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
        PgConnection::connect(&self.options.target, crate::pg::Role::Copy, &control()).unwrap()
    }

    fn copy_large_objects(&mut self) -> Result<crate::contracts::LargeObjectCompletion> {
        crate::large_objects::run_initial(
            &self.options,
            &self.plan,
            &mut self.snapshot,
            &mut self.authority,
            &self.io,
        )
    }

    fn restart(self) -> Self {
        self.try_restart().unwrap()
    }

    /// Drops the engine objects without cleanup, as a killed process does, then follows `clone --restart`.
    fn try_restart(self) -> Result<Self> {
        let Self {
            options,
            snapshot,
            state,
            authority,
            schema,
            ..
        } = self;
        drop((authority, snapshot, state));
        Self::reopen(options, schema)
    }

    fn reopen(mut options: CloneOptions, schema: String) -> Result<Self> {
        // A new process starts with fresh cancellation and deadline state.
        let io = control();
        options.state.action = StateAction::Restart;
        let mut state = state::open_clone(&options.state, None, &io)?;
        let previous = state.checked_clone()?.unwrap();
        assert_eq!(previous.phase, ClonePhase::Copying);
        let mut plan = previous.plan.clone();
        plan.generation = CopyGeneration(plan.generation.0 + 1);
        let snapshot = CloneSnapshot::export(connect(&options.source), plan.generation, &io)?;
        let mut authority =
            CloneAuthority::acquire(connect(&options.target), &mut state, &previous.plan, &io)?;
        crate::clone::advance_copy_generation(
            &mut state,
            &mut authority,
            &plan,
            snapshot.ticket(),
            &io,
        )?;
        Ok(Self {
            options,
            plan,
            snapshot,
            state,
            authority,
            schema,
            io,
        })
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
        let mut target = PgConnection::connect(&target_info, crate::pg::Role::Copy, &io).unwrap();
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
        let mut source = PgConnection::connect(&source_info, crate::pg::Role::Copy, &io).unwrap();
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
        PgConnection::connect(&fixture.options.source, crate::pg::Role::Copy, &fixture.io).unwrap(),
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

// M8 fault matrix: each kill drops the engine objects and terminates their sessions, then recovers.

const SPLIT_ROWS: u64 = 2000;
// 1000-byte rows pack seven to a block, so row 561 is the first row of block 80, inside part 2.
const SPLIT_HOLD_ID: u64 = 561;

fn split_parts() -> Vec<crate::contracts::CopyPart> {
    [(1, 0, Some(50)), (2, 50, Some(150)), (3, 150, None)]
        .into_iter()
        .map(
            |(number, start_block, end_block)| crate::contracts::CopyPart {
                number,
                count: 3,
                start_block,
                end_block,
            },
        )
        .collect()
}

// The target CHECK waits on a test-held advisory lock when it reaches row `id`, pausing that COPY mid-stream.
fn held_payload(key: i64, id: u64) -> String {
    format!(
        "CREATE TABLE payload (id integer CHECK (id <> {id} OR pg_catalog.pg_advisory_xact_lock_shared({key}::int8)::text IS NOT NULL), value text)"
    )
}

fn split_fixture(target_ddl: &str) -> Fixture {
    let mut fixture = Fixture::new_profile(
        &["CREATE TABLE payload (id integer, value text)"],
        &[target_ddl],
        &[&format!(
            "INSERT INTO payload SELECT n,repeat('x',1000) FROM generate_series(1,{SPLIT_ROWS}) AS n"
        )],
        CopyFormat::Binary,
        split_parts(),
    );
    fixture.options.table_jobs = 1;
    fixture
}

fn value(connection: &mut PgConnection, sql: &str) -> String {
    let result = connection.query(sql, &control()).unwrap();
    assert_eq!(result.rows(), 1, "{sql}");
    String::from_utf8(result.cell(0, 0).unwrap().unwrap().to_vec()).unwrap()
}

fn poll(mut ready: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !ready() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    true
}

fn wait_until(what: &str, ready: impl FnMut() -> bool) {
    assert!(poll(ready), "{what}");
}

fn wait_for<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    let mut found = None;
    wait_until(what, || {
        found = probe();
        found.is_some()
    });
    found.unwrap()
}

/// Runs `engine` here while `controller` injects faults from its own sessions; `ready` starts the engine.
fn controlled<S, T>(
    subject: S,
    engine: impl FnOnce(S) -> T,
    controller: impl FnOnce(&dyn Fn()) + Send,
) -> T {
    let (sender, started) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let control = scope.spawn(move || {
            controller(&|| {
                let _ = sender.send(());
            })
        });
        if started.recv().is_err() {
            std::panic::resume_unwind(control.join().unwrap_err());
        }
        let result = engine(subject);
        if let Err(panic) = control.join() {
            std::panic::resume_unwind(panic);
        }
        result
    })
}

fn advisory_waiter(connection: &mut PgConnection, class: Option<i32>, key: i64) -> Option<u32> {
    let filter = match class {
        Some(class) => format!("classid={class}::oid AND objid={key}::oid AND objsubid=2"),
        None => format!(
            "classid=({key}::int8>>32)::oid AND objid=({key}::int8&4294967295)::oid AND objsubid=1"
        ),
    };
    let result = connection
        .query(
            &format!("SELECT pid FROM pg_catalog.pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AND {filter} AND NOT granted"),
            &control(),
        )
        .unwrap();
    (result.rows() == 1).then(|| {
        std::str::from_utf8(result.cell(0, 0).unwrap().unwrap())
            .unwrap()
            .parse()
            .unwrap()
    })
}

/// Terminates every other client session on each observer's database, as a process kill closes its sockets.
fn kill_engine(observers: &mut [&mut PgConnection]) {
    let mut keep = Vec::new();
    for observer in observers.iter_mut() {
        keep.push(value(observer, "SELECT pg_catalog.pg_backend_pid()"));
    }
    let others = format!(
        "FROM pg_catalog.pg_stat_activity WHERE datname=pg_catalog.current_database() AND backend_type='client backend' AND pid <> ALL('{{{}}}'::int4[])",
        keep.join(",")
    );
    // An observer inside a transaction keeps its first activity snapshot unless it clears it.
    let fresh = |observer: &mut PgConnection| {
        value(observer, "SELECT pg_catalog.pg_stat_clear_snapshot()::text");
    };
    for observer in observers.iter_mut() {
        fresh(observer);
        observer
            .query(
                &format!("SELECT pg_catalog.pg_terminate_backend(pid) {others}"),
                &control(),
            )
            .unwrap();
    }
    for observer in observers.iter_mut() {
        wait_until("killed engine sessions did not end", || {
            fresh(observer);
            value(observer, &format!("SELECT count(*) {others}")) == "0"
        });
    }
}

fn rows(connection: &mut PgConnection, schema: &str, predicate: &str) -> (u64, u64) {
    let result = connection
        .query(
            &format!("SELECT count(*),count(DISTINCT id) FROM ONLY {schema}.payload{predicate}"),
            &control(),
        )
        .unwrap();
    let cell = |column| {
        std::str::from_utf8(result.cell(0, column).unwrap().unwrap())
            .unwrap()
            .parse()
            .unwrap()
    };
    (cell(0), cell(1))
}

fn assert_markers(fixture: &mut Fixture, parts: usize) {
    let markers = fixture.authority.durable_markers(&fixture.io).unwrap();
    assert_eq!(markers.len(), parts);
    assert!(
        markers
            .iter()
            .all(|marker| marker.generation == fixture.plan.generation)
    );
}

#[test]
#[ignore = "requires the disposable M8 PostgreSQL harness"]
fn live_split_kills_after_reset_mid_part_and_after_last_part_restart_with_every_row_once() {
    let key = 0x4d38_e201;
    let mut fixture = split_fixture(&held_payload(key, SPLIT_HOLD_ID));
    let schema = fixture.schema.clone();
    let (source_info, target_info) = (
        fixture.options.source.clone(),
        fixture.options.target.clone(),
    );
    let mut source = connect(&source_info);
    assert_eq!(
        value(
            &mut source,
            &format!(
                "SELECT (ctid::text::point)[0] FROM ONLY {schema}.payload WHERE id={SPLIT_HOLD_ID}"
            )
        ),
        "80"
    );
    let first_part = rows(
        &mut source,
        &schema,
        " WHERE ctid < '(50,0)'::pg_catalog.tid",
    );
    drop(source);

    // Kill mid-part: part 1 commits, part 2 streams into its open transaction and stops at the held row.
    let failure = controlled(&mut fixture, Fixture::copy, |ready| {
        let mut source = connect(&source_info);
        let mut target = connect(&target_info);
        target
            .query(
                &format!("SELECT pg_catalog.pg_advisory_lock({key})"),
                &control(),
            )
            .unwrap();
        ready();
        let worker = wait_for("part 2 did not reach its held row", || {
            advisory_waiter(&mut target, None, key)
        });
        // COPY FROM advances tuples_processed only per flushed batch; received bytes show the open stream.
        let streamed = value(
            &mut target,
            &format!(
                "SELECT bytes_processed FROM pg_catalog.pg_stat_progress_copy WHERE pid={worker}"
            ),
        );
        assert!(
            streamed.parse::<u64>().unwrap() > 0,
            "part 2 was not mid-stream"
        );
        kill_engine(&mut [&mut source, &mut target]);
    })
    .unwrap_err();
    assert!(!failure.message.is_empty());
    let mut target = connect(&target_info);
    assert_eq!(rows(&mut target, &schema, ""), first_part);

    // Kill after the split reset commit and before any permit.
    let mut fixture = fixture.restart();
    assert_markers(&mut fixture, 0);
    fixture
        .authority
        .prepare_split_tables(&fixture.state, &fixture.io)
        .unwrap();
    assert_eq!(rows(&mut target, &schema, ""), (0, 0));

    // Kill after the last part committed, before the copy is aggregated or the stage advances.
    let mut fixture = fixture.restart();
    let report = fixture.copy().unwrap();
    assert_eq!(report.rows, SPLIT_ROWS);
    assert_markers(&mut fixture, 3);

    let mut fixture = fixture.restart();
    assert_markers(&mut fixture, 0);
    let report = fixture.copy().unwrap();
    assert_eq!((report.tables, report.rows), (1, SPLIT_ROWS));
    assert_eq!(fixture.authority.copy_report(&fixture.io).unwrap(), report);
    assert_markers(&mut fixture, 3);
    assert_eq!(fixture.plan.generation, CopyGeneration(4));
    assert_eq!(rows(&mut target, &schema, ""), (SPLIT_ROWS, SPLIT_ROWS));
}

#[test]
#[ignore = "requires the disposable M8 PostgreSQL harness"]
fn live_split_lost_part_marker_commit_resolves_through_fence_barrier_or_restarts_generation() {
    for commit in [true, false] {
        let mut fixture = split_fixture("CREATE TABLE payload (id integer, value text)");
        let schema = fixture.schema.clone();
        let direct = fixture.options.target.clone();
        let last_part = rows(
            &mut connect(&fixture.options.source),
            &schema,
            " WHERE ctid >= '(150,0)'::pg_catalog.tid",
        );
        let hold = CommitHold::start(Withhold::Copy(3));
        fixture.options.target = hold.conninfo(&direct);
        let coordinator = fixture.authority.coordinator_pid();
        let observed = std::sync::Mutex::new((false, None));
        let result = controlled(&mut fixture, Fixture::copy, |ready| {
            ready();
            let held = hold.wait_held();
            let mut target = connect(&direct);
            // Without the fence barrier the coordinator reads the marker before the COMMIT lands.
            let blocked = poll(|| {
                value(
                    &mut target,
                    &format!("SELECT {held} = ANY(pg_catalog.pg_blocking_pids({coordinator}))"),
                ) == "t"
            });
            let tag = hold.release(commit);
            *observed.lock().unwrap() = (blocked, tag);
        });
        let (blocked, tag) = observed.into_inner().unwrap();
        assert!(
            blocked,
            "the coordinator did not wait behind the held part-marker COMMIT"
        );
        fixture.options.target = direct;
        let mut target = fixture.target();
        if commit {
            assert_eq!(tag.as_deref(), Some("COMMIT"));
            let report = result.unwrap();
            assert_eq!(report.rows, SPLIT_ROWS);
            assert_eq!(fixture.authority.copy_report(&fixture.io).unwrap(), report);
            assert_markers(&mut fixture, 3);
            assert_eq!(rows(&mut target, &schema, ""), (SPLIT_ROWS, SPLIT_ROWS));
            continue;
        }
        assert_eq!(tag, None);
        let error = result.unwrap_err();
        assert!(
            error.message.contains("commit-outcome-unknown"),
            "{}",
            error.message
        );
        assert_eq!(
            rows(&mut target, &schema, ""),
            (SPLIT_ROWS - last_part.0, SPLIT_ROWS - last_part.1)
        );
        let mut fixture = fixture.restart();
        assert_markers(&mut fixture, 0);
        let report = fixture.copy().unwrap();
        assert_eq!(report.rows, SPLIT_ROWS);
        assert_markers(&mut fixture, 3);
        assert_eq!(rows(&mut target, &schema, ""), (SPLIT_ROWS, SPLIT_ROWS));
    }
}

#[test]
#[ignore = "requires the disposable M8 PostgreSQL harness"]
fn live_binary_source_error_mid_relay_leaves_no_marker_and_counts_no_partial_rows() {
    let key = 0x4d38_e203;
    let total = 20_000;
    let mut fixture = Fixture::new_profile(
        &["CREATE TABLE payload (id integer, value text)"],
        &[&held_payload(key, 200)],
        &[&format!(
            "INSERT INTO payload SELECT n,repeat('b',1000) FROM generate_series(1,{total}) AS n"
        )],
        CopyFormat::Binary,
        vec![crate::contracts::CopyPart {
            number: 1,
            count: 1,
            start_block: 0,
            end_block: None,
        }],
    );
    fixture.options.table_jobs = 1;
    let schema = fixture.schema.clone();
    let (source_info, target_info) = (
        fixture.options.source.clone(),
        fixture.options.target.clone(),
    );
    let failure = controlled(&mut fixture, Fixture::copy, |ready| {
        let mut source = connect(&source_info);
        let mut target = connect(&target_info);
        target
            .query(&format!("SELECT pg_catalog.pg_advisory_lock({key})"), &control())
            .unwrap();
        ready();
        wait_for("the relay did not reach the held target row", || {
            advisory_waiter(&mut target, None, key)
        });
        let relay = value(
            &mut source,
            "SELECT pid FROM pg_catalog.pg_stat_activity WHERE datname=pg_catalog.current_database() AND state='active' AND query LIKE 'COPY (SELECT %FORMAT binary)'",
        );
        let sent: u64 = value(
            &mut source,
            &format!(
                "SELECT tuples_processed FROM pg_catalog.pg_stat_progress_copy WHERE pid={relay}"
            ),
        )
        .parse()
        .unwrap();
        assert!(sent > 0 && sent < total, "source COPY was not mid-relay: {sent}");
        source
            .query(
                &format!("SELECT pg_catalog.pg_terminate_backend({relay})"),
                &control(),
            )
            .unwrap();
        wait_until("the source relay backend did not end", || {
            value(
                &mut source,
                &format!("SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid={relay}"),
            ) == "0"
        });
    })
    .unwrap_err();
    assert!(!failure.message.is_empty());
    let mut target = connect(&target_info);
    assert_eq!(rows(&mut target, &schema, ""), (0, 0));
    // The failed worker cancelled this process; the read-back below runs as a fresh coordinator call.
    fixture.io = control();
    let owned = fixture.state.owned_copy_tables().unwrap();
    let part = fixture.plan.copy_tables[0].parts[0];
    assert_eq!(
        fixture
            .authority
            .resolve_unknown(&owned[0], &part, &fixture.io)
            .unwrap(),
        None
    );
    assert!(fixture.authority.copy_report(&fixture.io).is_err());
    let mut fixture = fixture.restart();
    let report = fixture.copy().unwrap();
    assert_eq!(report.rows, total);
    assert_eq!(rows(&mut target, &schema, ""), (total, total));
}

/// A fresh database pair: large objects are database-wide, so these cases cannot share the harness pair.
struct FreshDatabases {
    source: Conninfo,
    target: Conninfo,
    names: [String; 2],
}

impl FreshDatabases {
    fn create() -> Self {
        let source = admin("RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN");
        let target = admin("RPGMIGRATE_TEST_CLONE_TARGET_ADMIN");
        let case = CASE.fetch_add(1, Ordering::Relaxed);
        let names = ["s", "t"].map(|side| format!("m8_lo_{side}_{}_{case}", std::process::id()));
        let mut connection = connect(&source);
        for name in &names {
            connection
                .query(
                    &format!("CREATE DATABASE {name} TEMPLATE template0 ENCODING 'UTF8'"),
                    &control(),
                )
                .unwrap();
        }
        Self {
            source: with_keyword(&source, "dbname", &names[0]),
            target: with_keyword(&target, "dbname", &names[1]),
            names,
        }
    }
}

impl Drop for FreshDatabases {
    fn drop(&mut self) {
        let Ok(mut connection) = PgConnection::connect(
            &admin("RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN"),
            crate::pg::Role::Copy,
            &control(),
        ) else {
            return;
        };
        for name in &self.names {
            let _ = connection.query(
                &format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
                &control(),
            );
        }
    }
}

fn large_object_fixture(databases: &FreshDatabases, objects: &[(u32, &str)]) -> Fixture {
    let seed = objects
        .iter()
        .map(|(oid, hex)| {
            format!("SELECT pg_catalog.lo_from_bytea({oid},pg_catalog.decode({hex},'hex'))")
        })
        .collect::<Vec<_>>();
    let seed = seed.iter().map(String::as_str).collect::<Vec<_>>();
    let mut fixture = Fixture::new_on(
        databases.source.clone(),
        databases.target.clone(),
        &[],
        &[],
        &seed,
        CopyFormat::Text,
        vec![crate::contracts::CopyPart {
            number: 1,
            count: 1,
            start_block: 0,
            end_block: None,
        }],
    );
    fixture.options.large_objects_jobs = 1;
    assert_eq!(fixture.copy().unwrap().tables, 0);
    fixture
}

fn large_objects(connection: &mut PgConnection) -> Vec<String> {
    let result = connection
        .query(
            "SELECT m.oid::pg_catalog.text||':'||pg_catalog.length(pg_catalog.lo_get(m.oid))||':'||pg_catalog.md5(pg_catalog.lo_get(m.oid)) FROM pg_catalog.pg_largeobject_metadata m ORDER BY m.oid",
            &control(),
        )
        .unwrap();
    (0..result.rows())
        .map(|row| String::from_utf8(result.cell(row, 0).unwrap().unwrap().to_vec()).unwrap())
        .collect()
}

fn hold_object(connection: &mut PgConnection, oid: u32, lock: bool) {
    let class = crate::apply::checkpoint::LARGE_OBJECT_LOCK_CLASS;
    let function = if lock { "lock" } else { "unlock" };
    connection
        .query(
            &format!("SELECT pg_catalog.pg_advisory_{function}({class},{oid})"),
            &control(),
        )
        .unwrap();
}

#[test]
#[ignore = "requires the disposable M8 PostgreSQL harness"]
fn live_large_object_kills_mid_object_and_after_commit_restart_without_duplicate_or_missing_object()
{
    let databases = FreshDatabases::create();
    let mut fixture = large_object_fixture(
        &databases,
        &[
            (91001, "repeat('a1',4096)"),
            (91002, "repeat('5a',3*1024*1024+11)"),
            (91003, "'00ff275c'"),
        ],
    );
    let class = crate::apply::checkpoint::LARGE_OBJECT_LOCK_CLASS;
    let expected = large_objects(&mut connect(&databases.source));
    assert_eq!(expected.len(), 3);

    // Kill mid-object: 91001 commits, 91002 is created in its open transaction and waits to write.
    let failure = controlled(&mut fixture, Fixture::copy_large_objects, |ready| {
        let mut source = connect(&databases.source);
        let mut target = connect(&databases.target);
        let mut blocker = connect(&databases.target);
        hold_object(&mut target, 91002, true);
        ready();
        wait_for("the 91002 worker did not wait for its object lock", || {
            advisory_waiter(&mut target, Some(class), 91002)
        });
        blocker.query("BEGIN", &control()).unwrap();
        blocker
            .query(
                "LOCK TABLE pg_catalog.pg_largeobject IN SHARE MODE",
                &control(),
            )
            .unwrap();
        hold_object(&mut target, 91002, false);
        wait_until("the 91002 worker did not wait to write its first chunk", || {
            value(
                &mut target,
                "SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype='relation' AND relation='pg_catalog.pg_largeobject'::pg_catalog.regclass AND NOT granted",
            ) == "1"
        });
        kill_engine(&mut [&mut source, &mut target, &mut blocker]);
    })
    .unwrap_err();
    assert!(!failure.message.is_empty());
    let mut target = connect(&databases.target);
    assert_eq!(large_objects(&mut target), expected[..1]);

    let mut fixture = fixture.restart();
    assert!(large_objects(&mut target).is_empty());
    assert!(
        fixture
            .authority
            .durable_large_objects(&fixture.io)
            .unwrap()
            .is_empty()
    );

    // Kill after two object commits, before the third object and the phase completion.
    let failure = controlled(&mut fixture, Fixture::copy_large_objects, |ready| {
        let mut source = connect(&databases.source);
        let mut target = connect(&databases.target);
        hold_object(&mut target, 91003, true);
        ready();
        wait_for("the 91003 worker did not wait for its object lock", || {
            advisory_waiter(&mut target, Some(class), 91003)
        });
        assert_eq!(large_objects(&mut target), expected[..2]);
        kill_engine(&mut [&mut source, &mut target]);
    })
    .unwrap_err();
    assert!(!failure.message.is_empty());
    assert_eq!(
        fixture
            .authority
            .large_object_completion(crate::apply::LargeObjectPhase::Initial),
        None
    );

    let mut fixture = fixture.restart();
    let mut target = connect(&databases.target);
    assert!(large_objects(&mut target).is_empty());
    let completion = fixture.copy_large_objects().unwrap();
    assert_eq!(completion.objects, 3);
    assert_eq!(completion.bytes, 4096 + 3 * 1024 * 1024 + 11 + 4);
    assert_eq!(large_objects(&mut target), expected);
    let receipts = fixture
        .authority
        .durable_large_objects(&fixture.io)
        .unwrap();
    assert_eq!(
        receipts
            .iter()
            .map(|done| done.object_oid)
            .collect::<Vec<_>>(),
        [91001, 91002, 91003]
    );
    assert!(
        receipts
            .iter()
            .all(|done| done.generation == CopyGeneration(3)
                && done.sync_phase == crate::apply::LargeObjectPhase::Initial)
    );
    assert_eq!(
        fixture
            .authority
            .large_object_completion(crate::apply::LargeObjectPhase::Initial),
        Some(completion)
    );
}

/// Runs the restart plan admission that `clone --restart` runs before the owned reset.
fn restart_admission(
    fixture: &mut Fixture,
    target: &mut PgConnection,
    owned: &[u32],
    skip: bool,
) -> Result<ClonePlan> {
    let metadata = crate::clone::metadata(&fixture.state, &fixture.plan, target, &fixture.io)?;
    let previous = fixture.state.checked_clone()?.unwrap();
    let mut copy_policy = fixture.plan.copy_policy;
    copy_policy.skip_large_objects = skip;
    catalog::plan_clone(
        fixture.snapshot.connection()?,
        target,
        fixture.plan.generation,
        fixture.plan.policy,
        copy_policy,
        &metadata,
        &previous.owned_objects,
        owned,
        &fixture.io,
    )
}

#[test]
#[ignore = "requires the disposable M8 PostgreSQL harness"]
fn live_large_object_restart_unlinks_only_owned_objects_and_keeps_foreign_ones() {
    let objects = [
        (92001, "'6f776e6564206f6e65'"),
        (92002, "'6f776e65642074776f'"),
    ];

    // Restart planning admits exactly the receipted OIDs; skipping large objects skips the check.
    let databases = FreshDatabases::create();
    let mut fixture = large_object_fixture(&databases, &objects);
    assert_eq!(fixture.copy_large_objects().unwrap().objects, 2);
    let mut target = connect(&databases.target);
    let owned = fixture
        .authority
        .owned_large_object_oids(&fixture.io)
        .unwrap();
    assert_eq!(owned, [92001, 92002]);
    restart_admission(&mut fixture, &mut target, &owned, false).unwrap();
    restart_admission(&mut fixture, &mut target, &[], true).unwrap();
    let refusal = restart_admission(&mut fixture, &mut target, &owned[..1], false).unwrap_err();
    assert!(
        refusal
            .message
            .contains("refuses 1 target large objects this run does not own, first OID 92002"),
        "{}",
        refusal.message
    );
    target
        .query(
            "SELECT pg_catalog.lo_from_bytea(95001,pg_catalog.decode('f0f1','hex'))",
            &control(),
        )
        .unwrap();
    // clone --restart refuses the foreign object here, before the owned reset.
    let refusal = restart_admission(&mut fixture, &mut target, &owned, false).unwrap_err();
    assert!(
        refusal
            .message
            .contains("refuses 1 target large objects this run does not own, first OID 95001"),
        "{}",
        refusal.message
    );
    let refusal = restart_admission(&mut fixture, &mut target, &[], false).unwrap_err();
    assert!(
        refusal
            .message
            .contains("refuses 3 target large objects this run does not own, first OID 92001"),
        "{}",
        refusal.message
    );
    restart_admission(&mut fixture, &mut target, &[], true).unwrap();
    assert_eq!(large_objects(&mut target).len(), 3);

    // This fixture's restart skips planning: the permit refuses the survivor as the second line.
    let foreign = large_objects(&mut target)
        .into_iter()
        .filter(|row| row.starts_with("95001:"))
        .collect::<Vec<_>>();
    assert_eq!(foreign.len(), 1);
    let mut fixture = fixture.restart();
    assert_eq!(large_objects(&mut target), foreign);
    assert!(
        fixture
            .authority
            .durable_large_objects(&fixture.io)
            .unwrap()
            .is_empty()
    );
    let refusal = fixture.copy_large_objects().unwrap_err();
    assert!(
        refusal.message.contains("unrecorded target object"),
        "{}",
        refusal.message
    );
    assert_eq!(large_objects(&mut target), foreign);

    // A foreign object that replaced an owned OID refuses the reset and keeps every object.
    let databases = FreshDatabases::create();
    let mut fixture = large_object_fixture(&databases, &objects);
    assert_eq!(fixture.copy_large_objects().unwrap().objects, 2);
    let mut target = connect(&databases.target);
    target
        .query(
            "SELECT pg_catalog.lo_unlink(92001),pg_catalog.lo_from_bytea(92001,pg_catalog.decode('f0f2','hex'))",
            &control(),
        )
        .unwrap();
    let replaced = large_objects(&mut target);
    assert_eq!(replaced.len(), 2);
    let refusal = fixture.try_restart().err().unwrap();
    assert!(refusal.message.contains("drifted"), "{}", refusal.message);
    assert_eq!(large_objects(&mut target), replaced);
}

/// A kill point inside the target publish of a generation advance, after the native advance.
#[derive(Clone, Copy, Debug, PartialEq)]
enum AdvanceKill {
    BeforeReset,
    MidLargeObjectReset,
    BeforeCommit,
    AfterCommit,
}

const ADVANCE_ROWS: u64 = 2000;

fn metadata_schema(target: &mut PgConnection) -> String {
    value(
        target,
        r"SELECT pg_catalog.quote_ident(nspname) FROM pg_catalog.pg_namespace WHERE nspname LIKE '\_rpgmigrate\_%'",
    )
}

/// The target control generation and its `markers/of generation` and `receipts/of generation` counts.
fn target_generation(target: &mut PgConnection, generation: u64) -> String {
    let metadata = metadata_schema(target);
    value(
        target,
        &format!(
            "SELECT (SELECT clone_state::pg_catalog.jsonb->>'generation' FROM ONLY {metadata}.checkpoint)||' '||(SELECT count(*)||'/'||count(*) FILTER (WHERE copy_generation={generation}) FROM ONLY {metadata}.copy_done)||' '||(SELECT count(*)||'/'||count(*) FILTER (WHERE copy_generation={generation}) FROM ONLY {metadata}.large_object_done)"
        ),
    )
}

fn native_generation(options: &CloneOptions) -> (u64, Option<u64>) {
    let saved = state::inspect_clone(&options.state, None, &control())
        .unwrap()
        .clone_state;
    assert_eq!(saved.phase, ClonePhase::Copying);
    (
        saved.plan.generation.0,
        saved.recopy.map(|previous| previous.generation.0),
    )
}

/// Restarts from `options` and kills that engine at `point`; the target stays at its previous generation
/// unless the withheld COMMIT is forwarded.
fn kill_generation_advance(
    databases: &FreshDatabases,
    options: &CloneOptions,
    schema: &str,
    point: AdvanceKill,
) {
    let restart = |options: CloneOptions| Fixture::reopen(options, schema.into()).err();
    let failure = if matches!(point, AdvanceKill::BeforeCommit | AdvanceKill::AfterCommit) {
        let hold = CommitHold::start(Withhold::After(b"lo_unlink"));
        let mut held = options.clone();
        held.target = hold.conninfo(&options.target);
        controlled(held, restart, |ready| {
            ready();
            hold.wait_held();
            let commit = point == AdvanceKill::AfterCommit;
            assert_eq!(hold.release(commit).as_deref(), commit.then_some("COMMIT"));
        })
    } else {
        controlled(options.clone(), restart, |ready| {
            let mut source = connect(&databases.source);
            let mut target = connect(&databases.target);
            let mut blocker = connect(&databases.target);
            let metadata = metadata_schema(&mut target);
            let object =
                "locktype='object' AND classid='pg_catalog.pg_largeobject'::pg_catalog.regclass";
            let (lock, waiter) = if point == AdvanceKill::BeforeReset {
                (
                    format!("LOCK TABLE ONLY {metadata}.copy_done IN ACCESS EXCLUSIVE MODE"),
                    format!(
                        "locktype='relation' AND relation='{metadata}.copy_done'::pg_catalog.regclass"
                    ),
                )
            } else {
                // The comment lock blocks the deletion lock that lo_unlink takes on the second object.
                (
                    "COMMENT ON LARGE OBJECT 94002 IS NULL".into(),
                    format!("{object} AND objid=94002"),
                )
            };
            blocker.query("BEGIN", &control()).unwrap();
            blocker.query(&lock, &control()).unwrap();
            ready();
            wait_until(
                "the coordinator did not wait inside its generation publish",
                || {
                    value(
                        &mut target,
                        &format!(
                            "SELECT count(*) FROM pg_catalog.pg_locks WHERE NOT granted AND {waiter}"
                        ),
                    ) == "1"
                },
            );
            if point == AdvanceKill::MidLargeObjectReset {
                let unlinked = format!(
                    "SELECT count(*) FROM pg_catalog.pg_locks WHERE granted AND mode='AccessExclusiveLock' AND {object} AND objid=94001"
                );
                assert_eq!(
                    value(&mut target, &unlinked),
                    "1",
                    "the first owned object was not unlinked yet"
                );
            }
            kill_engine(&mut [&mut source, &mut target, &mut blocker]);
        })
    };
    assert!(
        !failure
            .expect("the killed restart completed")
            .message
            .is_empty()
    );
}

#[test]
#[ignore = "requires the disposable M8 PostgreSQL harness"]
fn live_generation_advance_kills_resume_the_recopy_intent_with_every_row_and_object_once() {
    let databases = FreshDatabases::create();
    let ddl = "CREATE TABLE payload (id integer, value text)";
    let seed = [
        format!(
            "INSERT INTO payload SELECT n,'row-'||n FROM generate_series(1,{ADVANCE_ROWS}) AS n"
        ),
        "SELECT pg_catalog.lo_from_bytea(94001,pg_catalog.decode(repeat('a1',4096),'hex'))".into(),
        "SELECT pg_catalog.lo_from_bytea(94002,pg_catalog.decode(repeat('5a',70000),'hex'))".into(),
        "SELECT pg_catalog.lo_from_bytea(94003,pg_catalog.decode('00ff275c','hex'))".into(),
    ];
    let seed = seed.iter().map(String::as_str).collect::<Vec<_>>();
    let mut fixture = Fixture::new_on(
        databases.source.clone(),
        databases.target.clone(),
        &[ddl],
        &[ddl],
        &seed,
        CopyFormat::Text,
        vec![crate::contracts::CopyPart {
            number: 1,
            count: 1,
            start_block: 0,
            end_block: None,
        }],
    );
    fixture.options.large_objects_jobs = 1;
    let expected = large_objects(&mut connect(&databases.source));
    assert_eq!(expected.len(), 3);
    // A kill terminates every other session on the target, so each check opens its own.
    let observe = || connect(&databases.target);
    let copy_once = |fixture: &mut Fixture| {
        assert_eq!(fixture.copy().unwrap().rows, ADVANCE_ROWS);
        assert_eq!(fixture.copy_large_objects().unwrap().objects, 3);
        let mut target = observe();
        assert_eq!(
            rows(&mut target, &fixture.schema, ""),
            (ADVANCE_ROWS, ADVANCE_ROWS)
        );
        assert_eq!(large_objects(&mut target), expected);
        let generation = fixture.plan.generation.0;
        assert_eq!(
            target_generation(&mut target, generation),
            format!("{generation} 1/1 3/3")
        );
    };
    copy_once(&mut fixture);

    for point in [
        AdvanceKill::BeforeReset,
        AdvanceKill::MidLargeObjectReset,
        AdvanceKill::BeforeCommit,
        AdvanceKill::AfterCommit,
    ] {
        let (options, schema) = (fixture.options.clone(), fixture.schema.clone());
        let generation = fixture.plan.generation.0;
        drop(fixture);
        // A restart that resumes an intent and is killed again leaves the same intent behind.
        let kills: &[AdvanceKill] = if point == AdvanceKill::BeforeReset {
            &[point, AdvanceKill::MidLargeObjectReset]
        } else {
            &[point]
        };
        for kill in kills {
            kill_generation_advance(&databases, &options, &schema, *kill);
            let mut target = observe();
            assert_eq!(
                native_generation(&options),
                (generation + 1, Some(generation)),
                "{kill:?}"
            );
            if point == AdvanceKill::AfterCommit {
                assert_eq!(
                    target_generation(&mut target, generation + 1),
                    format!("{} 0/0 0/0", generation + 1)
                );
                assert!(large_objects(&mut target).is_empty());
            } else {
                assert_eq!(
                    target_generation(&mut target, generation),
                    format!("{generation} 1/1 3/3"),
                    "{kill:?}"
                );
                assert_eq!(large_objects(&mut target), expected, "{kill:?}");
            }
            assert_eq!(rows(&mut target, &schema, ""), (ADVANCE_ROWS, ADVANCE_ROWS));
        }
        if point == AdvanceKill::BeforeReset {
            // An intent naming another snapshot of the previous generation is not this run's target.
            let manifest = options.state.path.join("manifest.json");
            let original = std::fs::read(&manifest).unwrap();
            let mut changed: serde_json::Value = serde_json::from_slice(&original).unwrap();
            changed["clone_state"]["recopy"]["token"] = "foreign-previous-snapshot".into();
            std::fs::write(&manifest, serde_json::to_vec(&changed).unwrap()).unwrap();
            let refusal = Fixture::reopen(options.clone(), schema.clone())
                .err()
                .unwrap();
            assert!(
                refusal
                    .message
                    .contains("generation or snapshot binding differs"),
                "{}",
                refusal.message
            );
            assert_eq!(
                native_generation(&options),
                (generation + 1, Some(generation))
            );
            assert_eq!(
                target_generation(&mut observe(), generation),
                format!("{generation} 1/1 3/3")
            );
            std::fs::write(&manifest, original).unwrap();
        }
        fixture = Fixture::reopen(options.clone(), schema).unwrap();
        assert_eq!(fixture.plan.generation.0, generation + 2);
        assert_eq!(native_generation(&options), (generation + 2, None));
        let mut target = observe();
        assert_eq!(
            target_generation(&mut target, generation + 2),
            format!("{} 0/0 0/0", generation + 2)
        );
        assert!(large_objects(&mut target).is_empty());
        copy_once(&mut fixture);
    }
}

#[derive(Default)]
struct HeldCommit {
    pid: Option<u32>,
    server: Option<std::os::unix::net::UnixStream>,
    frames: Vec<u8>,
    tag: Option<String>,
    ready: bool,
    closed: bool,
}

type SharedHold = Arc<(std::sync::Mutex<HeldCommit>, std::sync::Condvar)>;

/// The target session whose next COMMIT the relay withholds.
#[derive(Clone, Copy)]
enum Withhold {
    /// The n-th session to start a COPY FROM STDIN.
    Copy(u64),
    /// The first session to send a statement containing these bytes.
    After(&'static [u8]),
}

/// Relays target sessions and withholds the selected session's COMMIT from the server.
struct CommitHold {
    directory: PathBuf,
    shared: SharedHold,
    stop: Arc<AtomicBool>,
    accept: Option<std::thread::JoinHandle<()>>,
}

impl CommitHold {
    fn start(select: Withhold) -> Self {
        use std::os::unix::fs::PermissionsExt;
        let context = harness_context();
        let socket_dir = PathBuf::from(context["socket_dir"].as_str().unwrap());
        let port = context["port"].as_u64().unwrap();
        let directory = socket_dir.join(format!(
            "hold-{}-{}",
            std::process::id(),
            CASE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        let listener =
            std::os::unix::net::UnixListener::bind(directory.join(format!(".s.PGSQL.{port}")))
                .unwrap();
        listener.set_nonblocking(true).unwrap();
        let upstream = socket_dir.join(format!(".s.PGSQL.{port}"));
        let shared = SharedHold::default();
        let stop = Arc::new(AtomicBool::new(false));
        let accept = {
            let (shared, stop) = (shared.clone(), stop.clone());
            std::thread::spawn(move || {
                let copies = Arc::new(AtomicU64::new(0));
                while !stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((client, _)) => {
                            let (upstream, copies, shared) =
                                (upstream.clone(), copies.clone(), shared.clone());
                            std::thread::spawn(move || {
                                relay_session(client, &upstream, &copies, select, &shared)
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(std::time::Duration::from_millis(2))
                        }
                        Err(_) => return,
                    }
                }
            })
        };
        Self {
            directory,
            shared,
            stop,
            accept: Some(accept),
        }
    }

    fn conninfo(&self, direct: &Conninfo) -> Conninfo {
        with_keyword(direct, "host", self.directory.to_str().unwrap())
    }

    fn wait_held(&self) -> u32 {
        let (lock, signal) = &*self.shared;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut held = lock.lock().unwrap();
        while held.server.is_none() || held.pid.is_none() {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .expect("the selected part COMMIT was never withheld");
            held = signal.wait_timeout(held, remaining).unwrap().0;
        }
        held.pid.unwrap()
    }

    /// Forwards the withheld COMMIT and returns its tag, or closes the backend so the server aborts.
    fn release(&self, commit: bool) -> Option<String> {
        use std::io::Write;
        let (lock, signal) = &*self.shared;
        let mut held = lock.lock().unwrap();
        let mut server = held.server.take().expect("no withheld COMMIT");
        if commit {
            let frames = std::mem::take(&mut held.frames);
            server.write_all(&frames).unwrap();
        } else {
            server.shutdown(std::net::Shutdown::Both).unwrap();
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !(held.closed || commit && held.ready) {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .expect("the withheld backend did not answer or close");
            held = signal.wait_timeout(held, remaining).unwrap().0;
        }
        let _ = server.shutdown(std::net::Shutdown::Both);
        held.tag.clone()
    }
}

impl Drop for CommitHold {
    fn drop(&mut self) {
        if let Some(server) = self.shared.0.lock().unwrap().server.take() {
            let _ = server.shutdown(std::net::Shutdown::Both);
        }
        self.stop.store(true, Ordering::Release);
        if let Some(accept) = self.accept.take() {
            let _ = accept.join();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

fn read_frame(stream: &mut impl std::io::Read, typed: bool) -> Option<Vec<u8>> {
    let mut frame = vec![0; if typed { 5 } else { 4 }];
    stream.read_exact(&mut frame).ok()?;
    let start = frame.len() - 4;
    let length = u32::from_be_bytes(frame[start..].try_into().unwrap()) as usize;
    if !(4..=64 * 1024 * 1024).contains(&length) {
        return None;
    }
    frame.resize(start + length, 0);
    stream.read_exact(&mut frame[start + 4..]).ok()?;
    Some(frame)
}

fn relay_session(
    mut client: std::os::unix::net::UnixStream,
    upstream: &std::path::Path,
    copies: &AtomicU64,
    select: Withhold,
    shared: &SharedHold,
) {
    use std::io::Write;
    use std::net::Shutdown;
    client.set_nonblocking(false).unwrap();
    let Ok(mut server) = std::os::unix::net::UnixStream::connect(upstream) else {
        return;
    };
    let held = Arc::new(AtomicBool::new(false));
    let pid = Arc::new(std::sync::Mutex::new(None));
    {
        let (mut server, mut client) = (server.try_clone().unwrap(), client.try_clone().unwrap());
        let (held, pid, shared) = (held.clone(), pid.clone(), shared.clone());
        std::thread::spawn(move || {
            while let Some(frame) = read_frame(&mut server, true) {
                if frame[0] == b'K' && frame.len() == 13 {
                    *pid.lock().unwrap() =
                        Some(u32::from_be_bytes(frame[5..9].try_into().unwrap()));
                }
                if held.load(Ordering::Acquire) {
                    let mut state = shared.0.lock().unwrap();
                    if frame[0] == b'C' {
                        state.tag =
                            Some(String::from_utf8_lossy(&frame[5..frame.len() - 1]).into());
                    }
                    state.ready |= frame[0] == b'Z';
                    shared.1.notify_all();
                } else if client.write_all(&frame).is_err() {
                    break;
                }
            }
            if held.load(Ordering::Acquire) {
                shared.0.lock().unwrap().closed = true;
                shared.1.notify_all();
            }
            let _ = client.shutdown(Shutdown::Both);
        });
    }
    let startup = loop {
        let Some(frame) = read_frame(&mut client, false) else {
            return;
        };
        // libpq may ask to negotiate encryption first; this local relay always declines.
        if frame.len() == 8 && matches!(&frame[4..], [4, 210, 22, 47] | [4, 210, 22, 48]) {
            let _ = client.write_all(b"N");
            continue;
        }
        break frame;
    };
    if server.write_all(&startup).is_err() {
        return;
    }
    let (mut counted, mut selected_session, mut withheld) = (false, false, None::<Vec<u8>>);
    while let Some(frame) = read_frame(&mut client, true) {
        if frame[0] == b'P' {
            let body = &frame[5..];
            let name = body
                .iter()
                .position(|byte| *byte == 0)
                .unwrap_or(body.len());
            let query = body[name + 1..]
                .split(|byte| *byte == 0)
                .next()
                .unwrap_or(&[]);
            let (matched, rank) = match select {
                Withhold::Copy(rank) => (
                    query.starts_with(b"COPY ") && query.windows(11).any(|w| w == b" FROM STDIN"),
                    rank,
                ),
                Withhold::After(needle) => (query.windows(needle.len()).any(|w| w == needle), 1),
            };
            if !counted && matched {
                counted = true;
                selected_session = copies.fetch_add(1, Ordering::SeqCst) + 1 == rank;
            }
            if selected_session && query == b"COMMIT" {
                held.store(true, Ordering::Release);
                withheld = Some(Vec::new());
            }
        }
        if let Some(frames) = withheld.as_mut() {
            frames.extend_from_slice(&frame);
            if frame[0] == b'S' {
                let mut state = shared.0.lock().unwrap();
                state.frames = std::mem::take(frames);
                state.pid = *pid.lock().unwrap();
                state.server = Some(server.try_clone().unwrap());
                shared.1.notify_all();
                drop(state);
                let _ = client.shutdown(Shutdown::Both);
                return;
            }
            continue;
        }
        if server.write_all(&frame).is_err() {
            break;
        }
    }
    let _ = server.shutdown(Shutdown::Write);
}
