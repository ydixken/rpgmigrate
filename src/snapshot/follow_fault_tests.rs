use super::*;
use crate::contracts::{
    CloneOptions, ClonePlan, Conninfo, CopyGeneration, FollowLimits, FollowOptions, RestorePolicy,
    StateAction, StateRequest,
};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
};

static CASE: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    admin: PgConnection,
    source: PgConnection,
    options: FollowOptions,
    native: Option<NativeState>,
    plan: ClonePlan,
    database: String,
    role: String,
    context: Value,
    io: IoControl,
}

impl Fixture {
    fn new(empty: bool) -> Self {
        let context: Value = serde_json::from_slice(
            &std::fs::read(
                std::env::var_os("RPGMIGRATE_TEST_FOLLOW_SNAPSHOT_CONTEXT")
                    .expect("owned follow snapshot harness required"),
            )
            .unwrap(),
        )
        .unwrap();
        let root = PathBuf::from(context["root"].as_str().unwrap());
        assert_eq!(
            std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
            context["token"].as_str().unwrap()
        );
        let io = IoControl::unbounded(Arc::new(AtomicBool::new(false)));
        let admin_info = Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_FOLLOW_SOURCE_ADMIN")
                .expect("owned source admin required"),
        )
        .unwrap();
        let mut admin = PgConnection::connect(&admin_info, &io).unwrap();
        let database = format!(
            "m3_fault_{}_{}",
            std::process::id(),
            CASE.fetch_add(1, Ordering::Relaxed)
        );
        admin
            .query(&format!("CREATE DATABASE {database}"), &io)
            .unwrap();
        let token = admin
            .quote_literal(context["token"].as_str().unwrap())
            .unwrap();
        admin
            .query(&format!("COMMENT ON DATABASE {database} IS {token}"), &io)
            .unwrap();
        let source_info = Conninfo::new(
            format!(
                "{} dbname={database}",
                std::str::from_utf8(admin_info.as_c_str().to_bytes()).unwrap()
            )
            .into(),
        )
        .unwrap();
        let mut source = PgConnection::connect(&source_info, &io).unwrap();
        catalog::configure_session(&mut source, &io).unwrap();
        if !empty {
            source
                .query("CREATE TABLE public.items(id integer PRIMARY KEY)", &io)
                .unwrap();
        }
        let source_id = catalog::read_clone_source(&mut source, &io).unwrap();
        let target = catalog::read_clone_target(&mut source, &io).unwrap();
        assert_eq!(source_id.identity, target.identity);
        catalog::command(
            &mut source,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            &io,
        )
        .unwrap();
        let tables = catalog::inspect_clone_tables(&mut source, &[], &io).unwrap();
        catalog::command(&mut source, "COMMIT", "COMMIT", &io).unwrap();
        assert_eq!(tables.is_empty(), empty);
        let plan = ClonePlan {
            source: source_id,
            target,
            generation: CopyGeneration(1),
            copy_tables: catalog::provisional_copy_tables(&tables).unwrap(),
            tables,
            sequences: Vec::new(),
            policy: RestorePolicy::default(),
            copy_policy: crate::contracts::CloneCopyPolicy::default(),
        };
        let options = FollowOptions {
            clone: CloneOptions {
                source: source_info.clone(),
                target: source_info,
                state: StateRequest {
                    path: root.join(&database),
                    action: StateAction::Create,
                },
                operator: None,
                table_jobs: 4,
                post_data_jobs: 4,
                policy: RestorePolicy::default(),
                not_consistent: false,
                use_copy_binary: false,
                split_tables_larger_than: 0,
                split_max_parts: 8,
                skip_large_objects: false,
                large_objects_jobs: 4,
            },
            slot_name: format!("{database}_slot"),
            publication_name: format!("{database}_pub"),
            origin_name: None,
            apply_group_txns: 100,
            limits: FollowLimits::default(),
        };
        let native = crate::state::open_follow(&options.clone.state, None, None, &io).unwrap();
        Self {
            admin,
            source,
            options,
            native: Some(native),
            plan,
            database,
            role: "e1_admin".into(),
            context,
            io,
        }
    }
    fn write_receipt(&self, case: &str, value: Value) {
        let path = PathBuf::from(self.context["root"].as_str().unwrap())
            .join(format!("follow-snapshot-fault-{case}.json"));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        serde_json::to_writer(&mut file, &value).unwrap();
        file.sync_all().unwrap();
    }
    fn attempt(&mut self) -> Result<FollowSnapshot> {
        FollowSnapshot::create(
            &self.options,
            &self.plan,
            self.native.as_mut().unwrap(),
            &self.io,
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.admin.query(
            &format!("DROP DATABASE {} WITH (FORCE)", self.database),
            &self.io,
        );
    }
}

struct Helper {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    finished: bool,
}
impl Helper {
    fn start(fixture: &mut Fixture, case: &str, phase: &str) -> Self {
        let mut child = Command::new(
            std::env::var_os("RPGMIGRATE_TEST_PYTHON").unwrap_or_else(|| "python3".into()),
        )
        .arg(
            std::env::var_os("RPGMIGRATE_TEST_FOLLOW_SNAPSHOT_HELPER")
                .expect("snapshot fault helper required"),
        )
        .args(["--case", case, "--phase", phase])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut helper = Self {
            child,
            input,
            output,
            finished: false,
        };
        let ready = helper.read();
        assert_eq!(ready["ready"], true);
        assert_eq!(ready["helper_pid"], helper.child.id());
        let host = fixture
            .source
            .quote_literal(ready["proxy_socket_dir"].as_str().unwrap())
            .unwrap();
        fixture.options.clone.source = Conninfo::new(
            format!(
                "{} host={host}",
                std::str::from_utf8(fixture.options.clone.source.as_c_str().to_bytes()).unwrap()
            )
            .into(),
        )
        .unwrap();
        helper.request(json!({"op":"arm", "native_path":fixture.options.clone.state.path.join("manifest.json"), "database":fixture.database,"role":fixture.role,"slot_name":fixture.options.slot_name,"publication_name":fixture.options.publication_name}));
        helper
    }
    fn read(&mut self) -> Value {
        let mut line = String::new();
        assert!(
            self.output.read_line(&mut line).unwrap() > 0,
            "snapshot helper exited without a response"
        );
        serde_json::from_str(&line).unwrap()
    }
    fn request(&mut self, value: Value) -> Value {
        writeln!(self.input, "{value}").unwrap();
        self.input.flush().unwrap();
        self.read()
    }
    fn finish(mut self) -> Value {
        let mut value = self.request(json!({"op":"quit"}));
        let status = self.child.wait().unwrap();
        assert!(status.success(), "snapshot helper failed");
        if value.get("profile").is_some() {
            assert_eq!(value["helper_pid"], self.child.id());
            value["helper_reaped"] = json!(true);
            value["helper_returncode"] = json!(status.code().unwrap());
        }
        self.finished = true;
        value
    }
}
impl Drop for Helper {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_empty_snapshot() {
    let mut fixture = Fixture::new(true);
    let mut snapshot = fixture.attempt().unwrap();
    assert!(
        catalog::inspect_clone_tables(
            snapshot.snapshot_mut().connection().unwrap(),
            &[],
            &fixture.io
        )
        .unwrap()
        .is_empty()
    );
    assert_eq!(
        snapshot.receipt().intent.binding.plugin(),
        crate::cdc::contracts::Plugin::Pgoutput
    );
    snapshot.finish(&fixture.io).unwrap();
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_imported_identity_mismatch() {
    for phase in ["control", "imported_identity_mismatch"] {
        let mut fixture = Fixture::new(false);
        let mut helper = Helper::start(&mut fixture, "imported_identity_mismatch", phase);
        let outcome = fixture.attempt();
        let observed = helper.request(json!({"op":"stats"}));
        assert_eq!(observed["details"]["imported"], true);
        assert!(observed["details"]["identity_before"].as_u64().unwrap() > 0);
        if phase == "control" {
            outcome.unwrap().finish(&fixture.io).unwrap();
            assert_eq!(helper.finish()["fault_hits"], 0);
        } else {
            assert_eq!(observed["fault_hits"], 1);
            let error = match outcome {
                Ok(snapshot) => {
                    drop(snapshot);
                    panic!("post-import altered SQL PID was accepted")
                }
                Err(error) => error,
            };
            assert!(
                error.message.contains("clone backend identity"),
                "unexpected refusal: {}",
                error.message
            );
            helper.request(json!({"op":"failed"}));
            let receipt = helper.finish();
            assert_eq!(receipt["fault_hits"], 1);
            fixture.write_receipt("imported_identity_mismatch", receipt);
        }
    }
}

fn refused(outcome: Result<FollowSnapshot>) -> crate::contracts::Error {
    match outcome {
        Ok(snapshot) => {
            drop(snapshot);
            panic!("fault returned a usable snapshot")
        }
        Err(error) => error,
    }
}

fn manifest(fixture: &Fixture) -> Value {
    serde_json::from_slice(
        &std::fs::read(fixture.options.clone.state.path.join("manifest.json")).unwrap(),
    )
    .unwrap()
}

fn inventory(fixture: &mut Fixture) -> (Vec<u8>, Vec<u8>) {
    let result = fixture.source.query("SELECT COALESCE((SELECT json_agg(json_build_array(oid::bigint,pubname,pubowner::bigint) ORDER BY oid)::text FROM pg_catalog.pg_publication),'[]'),COALESCE((SELECT json_agg(json_build_array(slot_name,plugin,datoid::bigint,active_pid,confirmed_flush_lsn::text) ORDER BY slot_name)::text FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())),'[]')", &fixture.io).unwrap();
    assert_eq!(result.rows(), 1);
    (
        result.cell(0, 0).unwrap().unwrap().to_vec(),
        result.cell(0, 1).unwrap().unwrap().to_vec(),
    )
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_wrong_source_refused() {
    let mut fixture = Fixture::new(false);
    let resources = inventory(&mut fixture);
    let before = manifest(&fixture);
    fixture.plan.source.identity.system_identifier ^= 1;
    assert_ne!(fixture.plan.source.identity.system_identifier, 0);
    let error = refused(fixture.attempt());
    assert!(
        error
            .message
            .starts_with("follow-preflight: source identity or table bound differs from the plan"),
        "unexpected refusal: {}",
        error.message
    );
    assert_eq!(inventory(&mut fixture), resources);
    assert_eq!(manifest(&fixture), before);
    assert_eq!(before["source_slot"], Value::Null);
    assert_eq!(before["follow_state"]["publication"], Value::Null);
    assert_eq!(before["follow_state"]["preliminary"], Value::Null);
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_control_permission_denied() {
    for denied in [false, true] {
        let mut fixture = Fixture::new(false);
        let role = format!("{}_role", fixture.database);
        fixture.admin.query(&format!("CREATE ROLE {role} LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS"),&fixture.io).unwrap();
        fixture
            .admin
            .query(
                &format!("GRANT CREATE ON DATABASE {} TO {role}", fixture.database),
                &fixture.io,
            )
            .unwrap();
        for sql in [format!("GRANT USAGE ON SCHEMA public TO {role}"),format!("ALTER TABLE public.items OWNER TO {role}"),
            "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system(),pg_catalog.pg_control_checkpoint() FROM PUBLIC".into(),
            format!("REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system(),pg_catalog.pg_control_checkpoint() FROM {role}"),
            format!("GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_checkpoint() TO {role}")] {
            fixture.source.query(&sql,&fixture.io).unwrap();
        }
        if !denied {
            fixture
                .source
                .query(
                    &format!("GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {role}"),
                    &fixture.io,
                )
                .unwrap();
        }
        fixture.options.clone.source = Conninfo::new(
            format!(
                "{} user={role}",
                std::str::from_utf8(fixture.options.clone.source.as_c_str().to_bytes()).unwrap()
            )
            .into(),
        )
        .unwrap();
        let mut restricted =
            PgConnection::connect(&fixture.options.clone.source, &fixture.io).unwrap();
        let flags=restricted.query("SELECT rolreplication,rolsuper,rolcreatedb,rolcreaterole,rolbypassrls,pg_catalog.has_database_privilege(pg_catalog.current_database(),'CREATE'),pg_catalog.has_function_privilege('pg_catalog.pg_control_system()','EXECUTE'),pg_catalog.has_function_privilege('pg_catalog.pg_control_checkpoint()','EXECUTE') FROM pg_catalog.pg_roles WHERE rolname=CURRENT_USER",&fixture.io).unwrap();
        assert_eq!(flags.rows(), 1);
        for (column, expected) in [
            b"t",
            b"f",
            b"f",
            b"f",
            b"f",
            b"t",
            if denied { b"f" } else { b"t" },
            b"t",
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(flags.cell(0, column).unwrap(), Some(expected.as_slice()));
        }
        drop(restricted);
        let resources = inventory(&mut fixture);
        let before = manifest(&fixture);
        if denied {
            let error = refused(fixture.attempt());
            assert!(
                error.message.contains("SQLSTATE 42501"),
                "unexpected refusal: {}",
                error.message
            );
            assert_eq!(inventory(&mut fixture), resources);
            assert_eq!(manifest(&fixture), before);
        } else {
            fixture.attempt().unwrap().finish(&fixture.io).unwrap();
        }
    }
}

fn create_loss(case: &str, phase: &str) -> (Value, Value) {
    let mut fixture = Fixture::new(false);
    let mut helper = Helper::start(&mut fixture, case, phase);
    let error = refused(fixture.attempt());
    assert!(
        error.message.starts_with("source-slot-create-unknown:"),
        "unexpected refusal: {}",
        error.message
    );
    helper.request(json!({"op":"failed"}));
    let before = manifest(&fixture);
    assert_eq!(before["source_slot"]["phase"], "provisioning");
    drop(fixture.native.take());
    let resume = StateRequest {
        path: fixture.options.clone.state.path.clone(),
        action: StateAction::Resume,
    };
    fixture.native = Some(crate::state::open_follow(&resume, None, None, &fixture.io).unwrap());
    assert_eq!(manifest(&fixture), before);
    let error = refused(fixture.attempt());
    assert!(
        error.message.starts_with(
            "follow-source: publication retry lacks the exact persisted preliminary source plan"
        ),
        "unexpected retry refusal: {}",
        error.message
    );
    assert_eq!(manifest(&fixture), before);
    helper.request(json!({"op":"retry_refused"}));
    let value = helper.finish();
    assert_eq!(value["create_dispatches"], 1);
    if case != "create_tail_lost" {
        fixture.write_receipt(case, value.clone());
    }
    (value, fixture.context.clone())
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_create_reply_lost() {
    create_loss("create_reply_lost", "create_reply_lost");
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_create_tail_lost() {
    let mut phases = serde_json::Map::new();
    let mut context = Value::Null;
    for phase in ["tail_lost", "ready_transaction", "ready_failed"] {
        let (receipt, owner) = create_loss("create_tail_lost", phase);
        phases.insert(phase.into(), receipt);
        context = owner;
    }
    let value = json!({"owner":context["token"],"profile":"follow_snapshot","case":"create_tail_lost","status":"observed","server_version_num":context["server_version_num"],"fault_hits":3,"phases":phases});
    let path = PathBuf::from(context["root"].as_str().unwrap())
        .join("follow-snapshot-fault-create_tail_lost.json");
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    serde_json::to_writer(&mut file, &value).unwrap();
    file.sync_all().unwrap();
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_expired_export_refused() {
    let mut fixture = Fixture::new(false);
    let mut helper = Helper::start(
        &mut fixture,
        "expired_export_refused",
        "expired_export_refused",
    );
    let error = refused(fixture.attempt());
    let state = match fixture.plan.source.major {
        16 => "22023",
        18 => "42704",
        _ => panic!("unqualified fixture major"),
    };
    assert!(
        error.message.contains(&format!("SQLSTATE {state}")),
        "unexpected expiry refusal: {}",
        error.message
    );
    helper.request(json!({"op":"failed"}));
    let receipt = helper.finish();
    assert_eq!(receipt["source_slot_phase"], "confirmed");
    assert_eq!(receipt["details"]["sqlstate"], state);
    fixture.write_receipt("expired_export_refused", receipt);
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_source_finish_unknown() {
    let mut fixture = Fixture::new(false);
    let mut helper = Helper::start(
        &mut fixture,
        "source_finish_unknown",
        "source_finish_unknown",
    );
    let snapshot = fixture.attempt().unwrap();
    let before = manifest(&fixture);
    assert_eq!(before["source_slot"]["phase"], "confirmed");
    helper.request(json!({"op":"finish"}));
    let error = snapshot.finish(&fixture.io).unwrap_err();
    assert!(
        error.message.starts_with("clone-snapshot-finish-unknown:"),
        "unexpected finish refusal: {}",
        error.message
    );
    assert_eq!(manifest(&fixture), before);
    assert_eq!(before["copy_complete"], Value::Null);
    assert_eq!(before["clone_state"], Value::Null);
    helper.request(json!({"op":"failed"}));
    fixture.write_receipt("source_finish_unknown", helper.finish());
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_application_name_preserved() {
    let inherited = std::env::var("PGAPPNAME").expect("harness must supply PGAPPNAME to the child");
    for explicit in [false, true] {
        let mut fixture = Fixture::new(false);
        assert_eq!(
            inherited,
            format!(
                "rpgmigrate_follow_snapshot_{}",
                &fixture.context["token"].as_str().unwrap()[..12]
            )
        );
        let expected = if explicit {
            "rpgmigrate_explicit_snapshot"
        } else {
            &inherited
        };
        let host = fixture
            .source
            .quote_literal(fixture.context["socket_dir"].as_str().unwrap())
            .unwrap();
        let explicit_option = if explicit {
            format!(" application_name={expected}")
        } else {
            String::new()
        };
        fixture.options.clone.source=Conninfo::new(format!("host={host} port={} dbname={} user=e1_admin sslmode=disable connect_timeout=3{explicit_option}",fixture.context["port"],fixture.database).into()).unwrap();
        let mut snapshot = fixture.attempt().unwrap();
        let result = snapshot
            .snapshot_mut()
            .connection()
            .unwrap()
            .query("SHOW application_name", &fixture.io)
            .unwrap();
        assert_eq!(result.rows(), 1);
        assert_eq!(result.cell(0, 0).unwrap(), Some(expected.as_bytes()));
        let exporter = snapshot.exporter_pid();
        let name=fixture.source.query(&format!("SELECT application_name,usename,datname,backend_type FROM pg_catalog.pg_stat_activity WHERE pid={exporter}"),&fixture.io).unwrap();
        assert_eq!(name.rows(), 1);
        for (column, expected) in [expected, "e1_admin", fixture.database.as_str(), "walsender"]
            .iter()
            .enumerate()
        {
            assert_eq!(name.cell(0, column).unwrap(), Some(expected.as_bytes()));
        }
        snapshot.finish(&fixture.io).unwrap();
    }
}
