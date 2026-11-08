use super::{OwnedSourceSlot, SourceSnapshot};
use crate::catalog::{
    VerifiedCdcSource, close_copy_inventory, fetch_copy_inventory, inspect_copy_relation,
    open_copy_inventory,
};
use crate::cdc::contracts::{Column, Event, Plugin, RelationKey};
use crate::contracts::{Conninfo, CopyFormat, ErrorKind, IoControl, StateAction, StateRequest};
use crate::identity::{SourceId, StreamId};
use crate::pg::{PgConnection, TransactionStatus};
use crate::state::{NativeState, SourceSlotBinding};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(30),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn fixture(key: &str) -> std::ffi::OsString {
    std::env::var_os("RPGMIGRATE_TEST_SNAPSHOT_CONTEXT")
        .expect("owned snapshot fixture is required");
    std::env::var_os(key).expect("snapshot fixture variable is required")
}

fn context() -> Value {
    let context: Value =
        serde_json::from_reader(File::open(fixture("RPGMIGRATE_TEST_SNAPSHOT_CONTEXT")).unwrap())
            .unwrap();
    let marker = PathBuf::from(context["root"].as_str().unwrap()).join(".e1-owner");
    assert_eq!(
        std::fs::read_to_string(marker).unwrap(),
        context["token"].as_str().unwrap()
    );
    assert_eq!(context["logical_slots"], 16);
    context
}

fn connection(key: &str, io: &IoControl) -> PgConnection {
    PgConnection::connect(&Conninfo::new(fixture(key)).unwrap(), io).unwrap()
}

fn exporter(io: &IoControl) -> PgConnection {
    PgConnection::connect_replication(
        &Conninfo::new(fixture("RPGMIGRATE_TEST_SNAPSHOT_EXPORTER")).unwrap(),
        io,
    )
    .unwrap()
}

fn expiry_sqlstate() -> &'static str {
    let version = fixture("RPGMIGRATE_TEST_EXPECTED_PG_VERSION")
        .into_string()
        .unwrap();
    let number = fixture("RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM")
        .into_string()
        .unwrap();
    let (numeric, expected) = match (version.as_str(), number.as_str()) {
        ("14.24", "140024") => (140024, "22023"),
        ("17.11", "170011") => (170011, "42704"),
        _ => panic!("unqualified declared snapshot expiry profile"),
    };
    let owned = context();
    assert_eq!(owned["expected_server_version"], version);
    assert_eq!(owned["expected_server_version_num"], numeric);
    assert_eq!(owned["server_version_num"], numeric);
    expected
}

fn database_connection(key: &str, database: &str, logical: bool, io: &IoControl) -> PgConnection {
    let text = fixture(key).into_string().unwrap();
    assert_eq!(text.matches("dbname='snapshot_source'").count(), 1);
    assert!(matches!(database, "snapshot_empty" | "snapshot_denied"));
    let text = text.replacen(
        "dbname='snapshot_source'",
        &format!("dbname='{database}'"),
        1,
    );
    let conninfo = Conninfo::new(text.into()).unwrap();
    if logical {
        PgConnection::connect_replication(&conninfo, io).unwrap()
    } else {
        PgConnection::connect(&conninfo, io).unwrap()
    }
}

fn scalar(connection: &mut PgConnection, sql: &str, io: &IoControl) -> String {
    let result = connection.query(sql, io).unwrap();
    assert_eq!(result.rows(), 1);
    String::from_utf8(result.cell(0, 0).unwrap().unwrap().to_vec()).unwrap()
}

fn native(label: &str) -> NativeState {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = PathBuf::from(context()["root"].as_str().unwrap());
    crate::state::open(&StateRequest {
        path: root.join(format!(
            "snapshot-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )),
        action: StateAction::Create,
    })
    .unwrap()
}

fn manifest(state: &NativeState) -> Value {
    let value: Value = serde_json::from_reader(
        File::open(state.identity().canonical_path.join("manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(value["magic"], "RPGMIGRATE");
    assert_eq!(value["format_version"], 4);
    assert_eq!(value.get("copy_snapshot"), Some(&serde_json::Value::Null));
    assert_eq!(value["target_provenance"], json!([]));
    assert!(value["copy_complete"].is_null());
    assert!(value.as_object().unwrap().contains_key("source_slot"));
    value
}

fn slot(case: &str) -> String {
    context()["slots"][case].as_str().unwrap().to_owned()
}

fn binding(state: &NativeState, slot: String, source: SourceId) -> SourceSlotBinding {
    SourceSlotBinding::new(
        StreamId {
            run: state.identity().run,
            source,
            generation: 9,
        },
        slot,
        Plugin::TestDecoding,
        7,
    )
    .unwrap()
}

fn source_id(connection: &mut PgConnection, io: &IoControl) -> SourceId {
    crate::catalog::read_source_identity(connection, io)
        .unwrap()
        .0
}

fn slot_count(admin: &mut PgConnection, name: &str, io: &IoControl) -> String {
    let name = admin.quote_literal(name).unwrap();
    scalar(
        admin,
        &format!("SELECT count(*) FROM pg_replication_slots WHERE slot_name={name}"),
        io,
    )
}

fn assert_closed(admin: &mut PgConnection, pid: u32, io: &IoControl) {
    loop {
        if scalar(
            admin,
            &format!("SELECT count(*) FROM pg_stat_activity WHERE pid={pid}"),
            io,
        ) == "0"
        {
            return;
        }
        assert!(
            Instant::now() < io.deadline,
            "owned backend closure deadline"
        );
        std::thread::yield_now();
    }
}

fn assert_finished(owner: OwnedSourceSlot, state: &NativeState, binding: &SourceSlotBinding) {
    assert_eq!(&owner.receipt().intent.binding, binding);
    assert_eq!(
        manifest(state)["source_slot"]["record"],
        serde_json::to_value(owner.receipt()).unwrap()
    );
    state.checked_source_binding(binding).unwrap();
}

fn importer_facts(snapshot: &mut SourceSnapshot, io: &IoControl) -> Value {
    assert_eq!(
        snapshot.source.transaction_status().unwrap(),
        TransactionStatus::InTransaction
    );
    assert_eq!(
        source_id(&mut snapshot.source, io),
        snapshot.receipt().intent.binding.stream().source
    );
    let facts: Value = serde_json::from_str(&scalar(
        &mut snapshot.source,
        "SELECT json_build_array(pg_backend_pid(), current_setting('transaction_isolation'), \
         current_setting('transaction_read_only'), txid_current_snapshot()::text)",
        io,
    ))
    .unwrap();
    assert_eq!(facts[0], snapshot.source.backend_pid().unwrap());
    assert_eq!(facts[0], snapshot.session.pid);
    assert_eq!(facts[1], "repeatable read");
    assert_eq!(facts[2], "on");
    assert!(!facts[3].as_str().unwrap().is_empty());
    facts
}

fn copy_catalog_checks(snapshot: &mut SourceSnapshot, admin: &mut PgConnection, io: &IoControl) {
    let facts = importer_facts(snapshot, io);
    let mut names = vec![
        "snapshot_cap_relation".to_owned(),
        "snapshot_data".to_owned(),
    ];
    names.extend((0..257).map(|number| format!("snapshot_page_{number:03}")));
    let expected = admin
        .query(
            "SELECT c.relname, c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
             WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','f') \
             ORDER BY c.relname::text COLLATE \"C\"",
            io,
        )
        .unwrap();
    assert_eq!(expected.columns().unwrap(), 2);
    assert_eq!(expected.rows(), 259);
    let expected: Vec<_> = (0..expected.rows())
        .map(|row| {
            let name = std::str::from_utf8(expected.cell(row, 0).unwrap().unwrap()).unwrap();
            assert_eq!(name, names[row]);
            let oid = std::str::from_utf8(expected.cell(row, 1).unwrap().unwrap())
                .unwrap()
                .parse::<u32>()
                .unwrap();
            assert_ne!(oid, 0);
            (name.to_owned(), oid)
        })
        .collect();
    let binding = &snapshot.receipt().intent.binding;
    assert_eq!(binding.stream().generation, 9);
    let epoch = binding.catalog_epoch();
    assert_eq!(epoch, 7);
    let mut inventory = open_copy_inventory(&mut snapshot.source, io).unwrap();
    let mut position = 0;
    for length in [128, 128, 3] {
        let page = fetch_copy_inventory(&mut snapshot.source, &mut inventory, io)
            .unwrap()
            .expect("the owned catalog page must be present");
        assert_eq!(page.len(), length);
        for (name, oid) in page {
            let (expected_name, expected_oid) = &expected[position];
            assert_eq!(name.schema, "public");
            assert_eq!(&name.table, expected_name);
            assert_eq!(oid, *expected_oid);
            let (table, event) = inspect_copy_relation(&mut snapshot.source, &name, oid, epoch, io)
                .unwrap()
                .into_parts(CopyFormat::Text)
                .unwrap();
            let columns = if expected_name == "snapshot_cap_relation" {
                vec![
                    Column {
                        name: "first key".into(),
                        type_oid: 23,
                        type_mod: -1,
                        key: true,
                    },
                    Column {
                        name: "payload".into(),
                        type_oid: 1043,
                        type_mod: 16,
                        key: false,
                    },
                    Column {
                        name: "last key".into(),
                        type_oid: 20,
                        type_mod: -1,
                        key: true,
                    },
                ]
            } else {
                vec![Column {
                    name: "value".into(),
                    type_oid: 23,
                    type_mod: -1,
                    key: true,
                }]
            };
            assert_eq!(table.name.schema, "public");
            assert_eq!(&table.name.table, expected_name);
            assert_eq!(
                table.columns,
                columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>()
            );
            assert_eq!(table.format, CopyFormat::Text);
            assert_eq!(
                event,
                Event::Relation {
                    key: RelationKey {
                        oid: *expected_oid,
                        generation: 7
                    },
                    schema: "public".into(),
                    name: expected_name.clone(),
                    columns,
                    replica_identity: b'd',
                }
            );
            position += 1;
        }
        assert_eq!(importer_facts(snapshot, io), facts);
    }
    assert_eq!(position, 259);
    assert!(
        fetch_copy_inventory(&mut snapshot.source, &mut inventory, io)
            .unwrap()
            .is_none()
    );
    assert!(
        fetch_copy_inventory(&mut snapshot.source, &mut inventory, io)
            .unwrap()
            .is_none()
    );
    close_copy_inventory(&mut snapshot.source, inventory, io).unwrap();
    assert_eq!(
        scalar(
            &mut snapshot.source,
            "SELECT count(*) FROM pg_cursors WHERE name='rpgmigrate_copy_inventory'",
            io
        ),
        "0"
    );
    let oids = expected
        .iter()
        .map(|(_, oid)| oid.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let pid = snapshot.source.backend_pid().unwrap();
    assert_eq!(
        scalar(
            admin,
            &format!(
                "SELECT count(DISTINCT relation) FROM pg_locks WHERE pid={pid} AND database={} AND locktype='relation' AND mode='AccessShareLock' AND granted AND relation IN ({oids})",
                snapshot
                    .receipt()
                    .intent
                    .binding
                    .stream()
                    .source
                    .database_oid
            ),
            io
        ),
        "259"
    );
    assert_eq!(importer_facts(snapshot, io), facts);
}

fn empty_catalog_checks(snapshot: &mut SourceSnapshot, io: &IoControl) {
    let facts = importer_facts(snapshot, io);
    let fetch = "FETCH FORWARD 128 FROM rpgmigrate_copy_inventory";
    let mut inventory = open_copy_inventory(&mut snapshot.source, io).unwrap();
    let empty = snapshot.source.query(fetch, io).unwrap();
    assert_eq!(empty.columns().unwrap(), 3);
    assert_eq!(empty.rows(), 0);
    assert_eq!(empty.command_tag().unwrap(), "FETCH 0");
    assert!(
        fetch_copy_inventory(&mut snapshot.source, &mut inventory, io)
            .unwrap()
            .is_none()
    );
    assert!(
        fetch_copy_inventory(&mut snapshot.source, &mut inventory, io)
            .unwrap()
            .is_none()
    );
    close_copy_inventory(&mut snapshot.source, inventory, io).unwrap();

    let mut failed = open_copy_inventory(&mut snapshot.source, io).unwrap();
    snapshot
        .source
        .query("CLOSE rpgmigrate_copy_inventory", io)
        .unwrap();
    snapshot.source.query("DECLARE rpgmigrate_copy_inventory NO SCROLL CURSOR WITHOUT HOLD FOR SELECT 1, 2 WHERE false", io).unwrap();
    let wrong = snapshot.source.query(fetch, io).unwrap();
    assert_eq!(wrong.columns().unwrap(), 2);
    assert_eq!(wrong.rows(), 0);
    assert_eq!(wrong.command_tag().unwrap(), "FETCH 0");
    let width_error = fetch_copy_inventory(&mut snapshot.source, &mut failed, io).unwrap_err();
    assert_eq!(width_error.kind, ErrorKind::BadConfig);
    assert_eq!(
        width_error.message,
        "COPY inventory requires exactly three fields"
    );

    snapshot
        .source
        .query("CLOSE rpgmigrate_copy_inventory", io)
        .unwrap();
    snapshot.source.query("DECLARE rpgmigrate_copy_inventory NO SCROLL CURSOR WITHOUT HOLD FOR SELECT 1, 'public', 'unused' WHERE false", io).unwrap();
    let healthy = snapshot.source.query(fetch, io).unwrap();
    assert_eq!(healthy.columns().unwrap(), 3);
    assert_eq!(healthy.rows(), 0);
    assert_eq!(healthy.command_tag().unwrap(), "FETCH 0");
    let failure = fetch_copy_inventory(&mut snapshot.source, &mut failed, io).unwrap_err();
    assert_eq!(failure.kind, ErrorKind::BadConfig);
    assert_eq!(failure.message, "COPY inventory admission has failed");
    close_copy_inventory(&mut snapshot.source, failed, io).unwrap_or_else(|cleanup| {
        panic!(
            "{}; cursor cleanup failed: {}",
            width_error.message, cleanup.message
        )
    });
    assert_eq!(
        scalar(
            &mut snapshot.source,
            "SELECT count(*) FROM pg_cursors WHERE name='rpgmigrate_copy_inventory'",
            io
        ),
        "0"
    );
    assert_eq!(importer_facts(snapshot, io), facts);
}

fn capability_checks(
    snapshot: &SourceSnapshot,
    state: &NativeState,
    binding: &SourceSlotBinding,
    admin: &mut PgConnection,
    io: &IoControl,
) {
    assert_eq!(&snapshot.receipt().intent.binding, binding);
    let oid = scalar(
        admin,
        "SELECT 'public.snapshot_cap_relation'::regclass::oid::text",
        io,
    )
    .parse::<u32>()
    .unwrap();
    let expected = Event::Relation {
        key: RelationKey { oid, generation: 7 },
        schema: "public".into(),
        name: "snapshot_cap_relation".into(),
        columns: vec![
            Column {
                name: "first key".into(),
                type_oid: 23,
                type_mod: -1,
                key: true,
            },
            Column {
                name: "payload".into(),
                type_oid: 1043,
                type_mod: 16,
                key: false,
            },
            Column {
                name: "last key".into(),
                type_oid: 20,
                type_mod: -1,
                key: true,
            },
        ],
        replica_identity: b'd',
    };
    let source = connection("RPGMIGRATE_TEST_SNAPSHOT_SQL", io);
    let pid = source.backend_pid().unwrap();
    let mut capability = VerifiedCdcSource::acquire(source, state, binding, io).unwrap();
    assert_eq!(capability.stream(), binding.stream());
    assert_eq!(capability.stream().generation, 9);
    capability.check_relation(&expected, io).unwrap();
    capability.check_relation(&expected, io).unwrap();
    drop(capability);
    assert_closed(admin, pid, io);

    for field in 0..8 {
        let mut wrong = expected.clone();
        let Event::Relation {
            key,
            columns,
            replica_identity,
            ..
        } = &mut wrong
        else {
            unreachable!()
        };
        match field {
            0 => key.oid = oid.checked_add(1).unwrap(),
            1 => key.generation = 9,
            2 => columns[0].name = "wrong_id".into(),
            3 => columns[0].type_oid = 20,
            4 => columns[1].type_mod = 17,
            5 => columns[0].key = false,
            6 => *replica_identity = b'f',
            _ => columns.swap(0, 1),
        }
        let source = connection("RPGMIGRATE_TEST_SNAPSHOT_SQL", io);
        let pid = source.backend_pid().unwrap();
        let mut capability = VerifiedCdcSource::acquire(source, state, binding, io).unwrap();
        let error = capability
            .check_relation(&wrong, io)
            .expect_err("changed source relation must refuse");
        let reason = if field == 1 {
            "CDC source relation identity or catalog epoch does not match its native binding"
        } else {
            "CDC source relation metadata does not match its exact source event"
        };
        assert_eq!(error.kind, ErrorKind::BadConfig);
        assert_eq!(error.message, reason);
        assert_closed(admin, pid, io);
        let error = capability
            .check_relation(&expected, io)
            .expect_err("poisoned capability must stay closed");
        assert_eq!(error.kind, ErrorKind::BadConfig);
        assert_eq!(error.message, "CDC source capability is poisoned");
        assert_closed(admin, pid, io);
    }

    assert!(
        SourceSlotBinding::new(
            binding.stream(),
            binding.slot_name().into(),
            binding.plugin(),
            0
        )
        .is_err()
    );
    let mut other_observer = connection("RPGMIGRATE_TEST_SNAPSHOT_OTHER_SQL", io);
    let other_identity = source_id(&mut other_observer, io);
    assert_ne!(
        other_identity.system_identifier,
        binding.stream().source.system_identifier
    );
    for field in 0..3 {
        let mut stream = binding.stream();
        if field == 0 {
            stream.source = other_identity;
        } else if field == 1 {
            stream.source.database_oid = stream.source.database_oid.checked_add(1).unwrap();
        }
        let wrong = SourceSlotBinding::new(
            stream,
            binding.slot_name().into(),
            binding.plugin(),
            if field == 2 { 8 } else { 7 },
        )
        .unwrap();
        let source = connection("RPGMIGRATE_TEST_SNAPSHOT_SQL", io);
        let pid = source.backend_pid().unwrap();
        let error = VerifiedCdcSource::acquire(source, state, &wrong, io)
            .err()
            .expect("changed native binding must refuse");
        assert_eq!(error.kind, ErrorKind::BadState);
        assert_eq!(
            error.message,
            "state-source: no exact confirmed source slot binding"
        );
        assert_closed(admin, pid, io);
    }
    let source = connection("RPGMIGRATE_TEST_SNAPSHOT_OTHER_SQL", io);
    let pid = source.backend_pid().unwrap();
    let error = VerifiedCdcSource::acquire(source, state, binding, io)
        .err()
        .expect("actual other SQL source must refuse");
    assert_eq!(error.kind, ErrorKind::BadConfig);
    assert_eq!(
        error.message,
        "CDC source identity or physical backend changed"
    );
    assert_closed(&mut other_observer, pid, io);
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_owned_snapshot() {
    let io = control();
    let mut state = native("owned");
    let mut admin = connection("RPGMIGRATE_TEST_SNAPSHOT_ADMIN", &io);
    let binding = binding(
        &state,
        slot("live_owned_snapshot"),
        source_id(&mut admin, &io),
    );
    let mut snapshot = SourceSnapshot::create(
        connection("RPGMIGRATE_TEST_SNAPSHOT_SQL", &io),
        exporter(&io),
        binding.clone(),
        &mut state,
        &io,
    )
    .unwrap_or_else(|error| panic!("owned snapshot refused: {}", error.message));
    assert_eq!(snapshot.receipt().intent.binding.stream().generation, 9);
    assert_eq!(snapshot.receipt().intent.binding.catalog_epoch(), 7);
    assert_eq!(manifest(&state)["source_slot"]["phase"], "confirmed");
    state.checked_source_binding(&binding).unwrap();
    let name = admin.quote_literal(binding.slot_name()).unwrap();
    let point: u64 = scalar(&mut admin, &format!("SELECT pg_wal_lsn_diff(confirmed_flush_lsn,'0/0')::text FROM pg_replication_slots WHERE slot_name={name}"), &io).parse().unwrap();
    assert_eq!(snapshot.receipt().consistent_point.0, point);
    assert!(!snapshot.receipt().snapshot_token.is_empty());
    assert_eq!(
        snapshot.source.transaction_status().unwrap(),
        TransactionStatus::InTransaction
    );
    assert_eq!(
        scalar(
            &mut snapshot.source,
            "SELECT current_setting('transaction_isolation') || '|' || current_setting('transaction_read_only')",
            &io
        ),
        "repeatable read|on"
    );
    assert_eq!(
        scalar(
            &mut snapshot.source,
            "SELECT json_agg(value ORDER BY value)::text FROM public.snapshot_data",
            &io
        ),
        "[10, 20]"
    );
    admin
        .query("UPDATE public.snapshot_data SET value=value+1", &io)
        .unwrap();
    admin
        .query("INSERT INTO public.snapshot_data VALUES (30)", &io)
        .unwrap();
    assert_eq!(
        scalar(
            &mut admin,
            "SELECT json_agg(value ORDER BY value)::text FROM public.snapshot_data",
            &io
        ),
        "[11, 21, 30]"
    );
    assert_eq!(
        scalar(
            &mut snapshot.source,
            "SELECT json_agg(value ORDER BY value)::text FROM public.snapshot_data",
            &io
        ),
        "[10, 20]"
    );
    let source_pid = snapshot.source.backend_pid().unwrap();
    let exporter_pid = snapshot.exporter.backend_pid().unwrap();
    copy_catalog_checks(&mut snapshot, &mut admin, &io);
    capability_checks(&snapshot, &state, &binding, &mut admin, &io);
    let owner = snapshot
        .finish(&io)
        .unwrap_or_else(|error| panic!("finish refused: {}", error.message));
    assert_closed(&mut admin, source_pid, &io);
    assert_eq!(
        scalar(
            &mut admin,
            &format!("SELECT count(*) FROM pg_locks WHERE pid={source_pid}"),
            &io
        ),
        "0"
    );
    assert_eq!(
        scalar(
            &mut admin,
            &format!("SELECT count(*) FROM pg_stat_activity WHERE pid={exporter_pid}"),
            &io
        ),
        "1"
    );
    assert_eq!(owner.exporter.backend_pid().unwrap(), exporter_pid);
    assert_finished(owner, &state, &binding);
    assert_closed(&mut admin, exporter_pid, &io);
    assert_eq!(slot_count(&mut admin, binding.slot_name(), &io), "1");
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_empty_snapshot() {
    let io = control();
    let mut state = native("empty");
    let mut admin = database_connection(
        "RPGMIGRATE_TEST_SNAPSHOT_ADMIN",
        "snapshot_empty",
        false,
        &io,
    );
    let binding = binding(
        &state,
        slot("live_empty_snapshot"),
        source_id(&mut admin, &io),
    );
    let inventory = "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','f')";
    assert_eq!(scalar(&mut admin, inventory, &io), "0");
    let mut snapshot = SourceSnapshot::create(
        database_connection("RPGMIGRATE_TEST_SNAPSHOT_SQL", "snapshot_empty", false, &io),
        database_connection(
            "RPGMIGRATE_TEST_SNAPSHOT_EXPORTER",
            "snapshot_empty",
            true,
            &io,
        ),
        binding.clone(),
        &mut state,
        &io,
    )
    .unwrap_or_else(|error| panic!("empty snapshot refused: {}", error.message));
    assert_eq!(scalar(&mut snapshot.source, inventory, &io), "0");
    assert_eq!(
        snapshot.source.transaction_status().unwrap(),
        TransactionStatus::InTransaction
    );
    empty_catalog_checks(&mut snapshot, &io);
    assert_finished(
        snapshot
            .finish(&io)
            .unwrap_or_else(|error| panic!("finish refused: {}", error.message)),
        &state,
        &binding,
    );
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_existing_slot_refused() {
    let io = control();
    let mut state = native("existing");
    let mut admin = connection("RPGMIGRATE_TEST_SNAPSHOT_ADMIN", &io);
    let binding = binding(
        &state,
        slot("live_existing_slot_refused"),
        source_id(&mut admin, &io),
    );
    let name = admin.quote_literal(binding.slot_name()).unwrap();
    assert_eq!(
        scalar(
            &mut admin,
            &format!(
                "SELECT slot_name FROM pg_create_logical_replication_slot({name},'test_decoding')"
            ),
            &io
        ),
        binding.slot_name()
    );
    let evidence = format!(
        "SELECT json_build_array(slot_name,plugin,database,temporary,active,confirmed_flush_lsn::text)::text FROM pg_replication_slots WHERE slot_name={name}"
    );
    let before = scalar(&mut admin, &evidence, &io);
    let error = SourceSnapshot::create(
        connection("RPGMIGRATE_TEST_SNAPSHOT_SQL", &io),
        exporter(&io),
        binding,
        &mut state,
        &io,
    )
    .err()
    .expect("existing slot must refuse");
    assert_eq!(
        error.message,
        "source-snapshot: existing source slot refused"
    );
    assert!(manifest(&state)["source_slot"].is_null());
    assert_eq!(scalar(&mut admin, &evidence, &io), before);
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_control_permission_denied() {
    let io = control();
    let mut state = native("denied");
    let mut admin = database_connection(
        "RPGMIGRATE_TEST_SNAPSHOT_ADMIN",
        "snapshot_denied",
        false,
        &io,
    );
    let binding = binding(
        &state,
        slot("live_control_permission_denied"),
        source_id(&mut admin, &io),
    );
    let mut source = connection("RPGMIGRATE_TEST_SNAPSHOT_DENIED_SQL", &io);
    assert_eq!(
        scalar(
            &mut source,
            "SELECT rolreplication AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolbypassrls) FROM pg_roles WHERE rolname=current_user",
            &io
        ),
        "t"
    );
    assert_eq!(
        scalar(
            &mut source,
            "SELECT has_function_privilege('pg_catalog.pg_control_system()','EXECUTE')",
            &io
        ),
        "f"
    );
    let logical = PgConnection::connect_replication(
        &Conninfo::new(fixture("RPGMIGRATE_TEST_SNAPSHOT_DENIED_SQL")).unwrap(),
        &io,
    )
    .unwrap();
    let error = SourceSnapshot::create(source, logical, binding.clone(), &mut state, &io)
        .err()
        .expect("control permission must refuse");
    assert!(
        error.message.contains("42501"),
        "wrong refusal: {}",
        error.message
    );
    assert!(manifest(&state)["source_slot"].is_null());
    assert_eq!(slot_count(&mut admin, binding.slot_name(), &io), "0");
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_wrong_source_refused() {
    let io = control();
    let mut state = native("wrong-source");
    let mut admin = connection("RPGMIGRATE_TEST_SNAPSHOT_ADMIN", &io);
    let expected = source_id(&mut admin, &io);
    let mut other = connection("RPGMIGRATE_TEST_SNAPSHOT_OTHER_SQL", &io);
    assert_ne!(
        source_id(&mut other, &io).system_identifier,
        expected.system_identifier
    );
    assert_eq!(
        scalar(&mut other, "SELECT current_database()", &io),
        "snapshot_source"
    );
    let binding = binding(&state, slot("live_wrong_source_refused"), expected);
    let error = SourceSnapshot::create(other, exporter(&io), binding.clone(), &mut state, &io)
        .err()
        .expect("wrong cluster must refuse");
    assert_eq!(
        error.message,
        "source-snapshot: source identity differs from native binding"
    );
    assert!(manifest(&state)["source_slot"].is_null());
    assert_eq!(slot_count(&mut admin, binding.slot_name(), &io), "0");
    assert_eq!(
        slot_count(
            &mut connection("RPGMIGRATE_TEST_SNAPSHOT_OTHER_SQL", &io),
            binding.slot_name(),
            &io
        ),
        "0"
    );
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_reused_intent_refused() {
    let io = control();
    let mut state = native("reused");
    let mut admin = connection("RPGMIGRATE_TEST_SNAPSHOT_ADMIN", &io);
    let source = source_id(&mut admin, &io);
    let first = binding(&state, slot("live_reused_intent_refused"), source);
    state.begin_source_slot(&first).unwrap();
    let before = manifest(&state);
    let second = binding(&state, format!("{}_second", first.slot_name()), source);
    assert_eq!(slot_count(&mut admin, first.slot_name(), &io), "0");
    assert_eq!(slot_count(&mut admin, second.slot_name(), &io), "0");
    let error = SourceSnapshot::create(
        connection("RPGMIGRATE_TEST_SNAPSHOT_SQL", &io),
        exporter(&io),
        second.clone(),
        &mut state,
        &io,
    )
    .err()
    .expect("reused native intent must refuse");
    assert!(
        error
            .message
            .starts_with("state-source: source slot intent requires a fresh incomplete run"),
        "wrong refusal: {}",
        error.message
    );
    assert_eq!(manifest(&state), before);
    assert_eq!(slot_count(&mut admin, second.slot_name(), &io), "0");
}

struct Fault {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
    receipt: PathBuf,
    phase: String,
    grouped: bool,
}

impl Fault {
    fn start(case: &str, phase: &str, io: &IoControl) -> (Self, Conninfo) {
        let mut command = Command::new(fixture("RPGMIGRATE_TEST_PYTHON"));
        command
            .arg(fixture("RPGMIGRATE_TEST_SNAPSHOT_FAULT_HELPER"))
            .args(["--case", case]);
        if case == "create_tail_lost" {
            command.args(["--phase", phase]);
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("snapshot helper failed to start");
        let mut fault = Self {
            input: child.stdin.take().unwrap(),
            output: child.stdout.take().unwrap(),
            child,
            receipt: PathBuf::new(),
            phase: phase.into(),
            grouped: case == "create_tail_lost",
        };
        let ready = fault.read(io);
        assert_eq!(ready["ready"], true);
        fault.receipt = PathBuf::from(ready["receipt"].as_str().unwrap());
        let conninfo = Conninfo::new(ready["conninfo"].as_str().unwrap().into()).unwrap();
        (fault, conninfo)
    }

    fn read(&mut self, io: &IoControl) -> Value {
        let mut bytes = Vec::new();
        loop {
            let remaining = io
                .deadline
                .checked_duration_since(Instant::now())
                .expect("snapshot helper response deadline");
            let mut descriptor = libc::pollfd {
                fd: self.output.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let milliseconds = i32::try_from(remaining.as_millis().max(1)).unwrap_or(i32::MAX);
            // SAFETY: poll receives one live descriptor and cannot outlive the borrowed output handle.
            let result = unsafe { libc::poll(&mut descriptor, 1, milliseconds) };
            if result < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
                continue;
            }
            assert!(
                result > 0,
                "snapshot helper response deadline or poll failure"
            );
            let mut byte = [0];
            assert_eq!(
                self.output.read(&mut byte).unwrap(),
                1,
                "helper closed without a receipt"
            );
            if byte[0] == b'\n' {
                return serde_json::from_slice(&bytes).expect("invalid snapshot helper JSON");
            }
            bytes.push(byte[0]);
            assert!(
                bytes.len() <= 64 * 1024,
                "oversized snapshot helper receipt"
            );
        }
    }

    fn command(&mut self, value: Value, io: &IoControl) -> Value {
        serde_json::to_writer(&mut self.input, &value).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
        self.read(io)
    }

    fn arm(&mut self, state: &NativeState, peer_pid: u32, io: &IoControl) {
        let value = self.command(json!({"op":"arm", "native_path":state.identity().canonical_path.join("manifest.json"), "peer_pid":peer_pid}), io);
        assert_eq!(value["armed"], true);
        assert_eq!(value["fault_hits"], 0);
    }

    fn failed(&mut self, io: &IoControl) {
        let value = self.command(json!({"op":"failed"}), io);
        assert_eq!(value["client_failed"], true);
        assert_eq!(value["failure_before_deadline"], true);
    }

    fn complete(mut self, io: &IoControl) {
        let proof = self.command(json!({"op":"quit"}), io);
        assert_eq!(proof["status"], "observed");
        assert_eq!(proof["backend_closed"], true);
        assert_eq!(proof["peer_closed"], true);
        let exit = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < io.deadline,
                "snapshot helper reap deadline"
            );
            std::thread::yield_now();
        };
        assert!(exit.success(), "snapshot helper failed");
        let mut receipt: Value =
            serde_json::from_reader(File::open(&self.receipt).unwrap()).unwrap();
        let phase = if self.grouped {
            &mut receipt["phases"][&self.phase]
        } else {
            &mut receipt
        };
        assert_eq!(phase["status"], "observed");
        assert_eq!(phase["helper_reaped"], false);
        phase["helper_reaped"] = json!(true);
        phase["helper_returncode"] = json!(exit.code().unwrap());
        if self.grouped {
            let phases = receipt["phases"].as_object().unwrap();
            let complete = phases.len() == 3
                && phases
                    .values()
                    .all(|phase| phase["status"] == "observed" && phase["helper_reaped"] == true);
            receipt["status"] = json!(if complete { "observed" } else { "pending" });
        }
        std::fs::write(&self.receipt, serde_json::to_vec(&receipt).unwrap()).unwrap();
    }
}

impl Drop for Fault {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn create_fault(case: &str, phase: &str) {
    let expected_expiry = if case == "expired_export_refused" {
        Some(expiry_sqlstate())
    } else {
        None
    };
    let io = control();
    let mut state = native(phase);
    let mut admin = connection("RPGMIGRATE_TEST_SNAPSHOT_ADMIN", &io);
    let mut name = slot(&format!("live_{case}"));
    if phase == "ready_transaction" {
        name.push_str("_t");
    }
    if phase == "ready_failed" {
        name.push_str("_e");
    }
    let binding = binding(&state, name, source_id(&mut admin, &io));
    let (mut fault, relay) = Fault::start(case, phase, &io);
    let creating = matches!(case, "create_reply_lost" | "create_tail_lost");
    let (source, exporter, peer_pid) = if creating {
        let source = connection("RPGMIGRATE_TEST_SNAPSHOT_SQL", &io);
        let peer = source.backend_pid().unwrap();
        (
            source,
            PgConnection::connect_replication(&relay, &io).unwrap(),
            peer,
        )
    } else {
        let exporter = exporter(&io);
        let peer = exporter.backend_pid().unwrap();
        (PgConnection::connect(&relay, &io).unwrap(), exporter, peer)
    };
    fault.arm(&state, peer_pid, &io);
    let error = SourceSnapshot::create(source, exporter, binding.clone(), &mut state, &io)
        .err()
        .expect("fault must not return a snapshot");
    fault.failed(&io);
    if creating {
        assert!(
            error.message.starts_with("source-slot-create-unknown:"),
            "wrong refusal: {}",
            error.message
        );
        assert_eq!(manifest(&state)["source_slot"]["phase"], "provisioning");
        assert!(state.checked_source_binding(&binding).is_err());
    } else {
        if case == "expired_export_refused" {
            assert_eq!(error.kind, ErrorKind::Source);
            assert_eq!(
                error.message,
                format!(
                    "PostgreSQL command failed (SQLSTATE {})",
                    expected_expiry.unwrap()
                )
            );
        } else {
            assert_eq!(case, "imported_identity_mismatch");
            assert_eq!(error.message, "source-snapshot: source session changed");
        }
        assert_eq!(manifest(&state)["source_slot"]["phase"], "confirmed");
        state.checked_source_binding(&binding).unwrap();
    }
    assert_eq!(slot_count(&mut admin, binding.slot_name(), &io), "1");
    fault.complete(&io);
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_create_reply_lost() {
    create_fault("create_reply_lost", "create_reply_lost");
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_create_tail_lost() {
    for phase in ["tail_lost", "ready_transaction", "ready_failed"] {
        create_fault("create_tail_lost", phase);
    }
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_expired_export_refused() {
    create_fault("expired_export_refused", "expired_export_refused");
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_imported_identity_mismatch() {
    create_fault("imported_identity_mismatch", "imported_identity_mismatch");
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_source_finish_unknown() {
    let io = control();
    let mut state = native("source_finish_unknown");
    let mut admin = connection("RPGMIGRATE_TEST_SNAPSHOT_ADMIN", &io);
    let binding = binding(
        &state,
        slot("live_source_finish_unknown"),
        source_id(&mut admin, &io),
    );
    let (mut fault, relay) = Fault::start("source_finish_unknown", "source_finish_unknown", &io);
    let exporter = exporter(&io);
    fault.arm(&state, exporter.backend_pid().unwrap(), &io);
    let snapshot = SourceSnapshot::create(
        PgConnection::connect(&relay, &io).unwrap(),
        exporter,
        binding.clone(),
        &mut state,
        &io,
    )
    .unwrap_or_else(|error| panic!("snapshot refused before COMMIT fault: {}", error.message));
    let error = snapshot
        .finish(&io)
        .err()
        .expect("lost source COMMIT cannot return an owner");
    fault.failed(&io);
    assert!(
        error.message.starts_with("source-snapshot-finish-unknown:"),
        "wrong refusal: {}",
        error.message
    );
    assert_eq!(manifest(&state)["source_slot"]["phase"], "confirmed");
    assert_eq!(slot_count(&mut admin, binding.slot_name(), &io), "1");
    fault.complete(&io);
}

#[test]
#[ignore = "requires owned source snapshot harness"]
fn live_application_name_preserved() {
    let io = control();
    let mut admin = connection("RPGMIGRATE_TEST_SNAPSHOT_ADMIN", &io);
    let source = source_id(&mut admin, &io);
    let environment = context()["application_name_env"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(std::env::var("PGAPPNAME").unwrap(), environment);
    for explicit in [true, false] {
        let mut state = native(if explicit { "app-explicit" } else { "app-env" });
        let binding = binding(
            &state,
            format!(
                "{}_{}",
                slot("live_application_name_preserved"),
                if explicit { "explicit" } else { "environment" }
            ),
            source,
        );
        let mut peers = Vec::new();
        for (key, logical) in [
            ("RPGMIGRATE_TEST_SNAPSHOT_SQL", false),
            ("RPGMIGRATE_TEST_SNAPSHOT_EXPORTER", true),
        ] {
            let text = fixture(key).into_string().unwrap();
            assert_eq!(
                text.matches("application_name='rpgmigrate_e1_fixture'")
                    .count(),
                1
            );
            let text = text.replacen(
                "application_name='rpgmigrate_e1_fixture'",
                if explicit {
                    "application_name='snapshot_explicit'"
                } else {
                    ""
                },
                1,
            );
            let conninfo = Conninfo::new(text.into()).unwrap();
            peers.push(if logical {
                PgConnection::connect_replication(&conninfo, &io).unwrap()
            } else {
                PgConnection::connect(&conninfo, &io).unwrap()
            });
        }
        let exporter = peers.pop().unwrap();
        let source = peers.pop().unwrap();
        let mut snapshot =
            SourceSnapshot::create(source, exporter, binding.clone(), &mut state, &io)
                .unwrap_or_else(|error| {
                    panic!("application-name snapshot refused: {}", error.message)
                });
        let expected = if explicit {
            "snapshot_explicit"
        } else {
            &environment
        };
        assert_eq!(
            scalar(
                &mut snapshot.source,
                "SELECT current_setting('application_name')",
                &io
            ),
            expected
        );
        let pid = snapshot.exporter.backend_pid().unwrap();
        assert_eq!(
            scalar(
                &mut admin,
                &format!("SELECT application_name FROM pg_stat_activity WHERE pid={pid}"),
                &io
            ),
            expected
        );
        assert_finished(
            snapshot
                .finish(&io)
                .unwrap_or_else(|error| panic!("finish refused: {}", error.message)),
            &state,
            &binding,
        );
    }
}
