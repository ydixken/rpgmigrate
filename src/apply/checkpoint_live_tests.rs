use super::*;
use crate::contracts::{Conninfo, StateAction, StateRequest};
use crate::identity::{CommitLsn, SourceEndLsn, SourceId, TxId, TxSeq};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(20),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn connection(io: &IoControl) -> PgConnection {
    std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned PostgreSQL harness required");
    let conninfo = Conninfo::new(
        std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_TARGET").expect("owned target required"),
    )
    .unwrap();
    PgConnection::connect(&conninfo, io).unwrap()
}

fn state() -> NativeState {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let context = PathBuf::from(std::env::var_os("RPGMIGRATE_TEST_CONTEXT").unwrap());
    let path = context.parent().unwrap().join(format!(
        "authority-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    crate::state::open(&StateRequest {
        path,
        action: StateAction::Create,
    })
    .unwrap()
}

fn stream_for(state: &NativeState) -> StreamId {
    StreamId {
        run: state.identity().run,
        source: SourceId {
            system_identifier: 777,
            database_oid: 42,
        },
        generation: 1,
    }
}

fn frontier(stream: StreamId, order: u64) -> Frontier {
    Frontier {
        tx: TxId {
            stream,
            xid: 99,
            begin_ordinal: 100 - order,
        },
        order: TxSeq(order),
        commit: CommitLsn(100),
        end: SourceEndLsn(120),
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn atomic_checkpoint() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut setup = connection(&io);
    setup
        .query(
            "CREATE TABLE public.authority_atomic (value integer NOT NULL)",
            &io,
        )
        .unwrap();
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    assert_eq!(authority.checkpoint(&io).unwrap(), None);
    let next = frontier(stream, 1);
    let mut tx = authority.begin(next, &io).unwrap();
    tx.prepare(
        "insert_effect",
        "INSERT INTO public.authority_atomic VALUES ($1)",
        &[23],
        &io,
    )
    .unwrap();
    tx.execute_prepared("insert_effect", &[PgParam::Text(b"42")], &io)
        .unwrap();
    let CommitOutcome::Acknowledged {
        checkpoint,
        durable: Some(proof),
    } = tx.commit(&io).unwrap()
    else {
        panic!("synchronous data transaction produced no durable proof")
    };
    assert_eq!(checkpoint.applied, next);
    assert_eq!(checkpoint.barrier_counter, 2);
    assert_eq!(proof.frontier(), Some(&next));
    assert_eq!(proof.stream(), stream);
    assert_eq!(proof.barrier_epoch(), checkpoint.barrier_epoch);
    assert_eq!(proof.barrier_counter(), checkpoint.barrier_counter);
    assert_eq!(proof.target().intent.binding.stream, stream);
    assert_eq!(authority.checkpoint(&io).unwrap(), Some(checkpoint));
    let rows = setup
        .query("SELECT value FROM public.authority_atomic", &io)
        .unwrap();
    assert_eq!(rows.rows(), 1);
    assert_eq!(rows.cell(0, 0).unwrap(), Some(&b"42"[..]));
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn rollback_checkpoint() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut observer = connection(&io);
    observer.query("CREATE TABLE public.authority_rollback (value integer UNIQUE DEFERRABLE INITIALLY DEFERRED)", &io).unwrap();
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    let mut tx = authority.begin(frontier(stream, 1), &io).unwrap();
    tx.prepare(
        "insert_rollback",
        "INSERT INTO public.authority_rollback VALUES ($1)",
        &[23],
        &io,
    )
    .unwrap();
    tx.execute_prepared("insert_rollback", &[PgParam::Text(b"1")], &io)
        .unwrap();
    tx.rollback(&io).unwrap();
    assert_eq!(authority.checkpoint(&io).unwrap(), None);
    let mut tx = authority.begin(frontier(stream, 1), &io).unwrap();
    tx.execute_prepared("insert_rollback", &[PgParam::Text(b"2")], &io)
        .unwrap();
    tx.execute_prepared("insert_rollback", &[PgParam::Text(b"2")], &io)
        .unwrap();
    assert!(tx.commit(&io).is_err());
    assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
    let rows = observer
        .query("SELECT count(*) FROM public.authority_rollback", &io)
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(&b"0"[..]));
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn exact_identity_order() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    authority
        .begin(frontier(stream, 1), &io)
        .unwrap()
        .commit(&io)
        .unwrap();
    let next = frontier(stream, 2);
    for invalid in [
        frontier(stream, 1),
        frontier(stream, 3),
        Frontier {
            commit: CommitLsn(99),
            ..next
        },
        Frontier {
            end: SourceEndLsn(119),
            ..next
        },
        Frontier {
            tx: TxId {
                stream: StreamId {
                    generation: 2,
                    ..stream
                },
                ..next.tx
            },
            ..next
        },
    ] {
        assert!(authority.begin(invalid, &io).is_err());
    }
    authority.begin(next, &io).unwrap().commit(&io).unwrap();
    assert_eq!(authority.checkpoint(&io).unwrap().unwrap().applied, next);
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn two_owner_fence() {
    let io = control();
    let mut first = state();
    let first_stream = stream_for(&first);
    let mut owner =
        TargetAuthority::acquire(connection(&io), first_stream, &mut first, &io).unwrap();
    owner
        .begin(frontier(first_stream, 1), &io)
        .unwrap()
        .rollback(&io)
        .unwrap();
    let mut second = state();
    let second_stream = stream_for(&second);
    let contender = connection(&io);
    let mut deadline = control();
    deadline.deadline = Instant::now() + Duration::from_millis(200);
    assert!(TargetAuthority::acquire(contender, second_stream, &mut second, &deadline).is_err());
    assert!(second.target_provenance(&second_stream).is_none());
    drop(owner);
    let resumed =
        TargetAuthority::acquire(connection(&io), second_stream, &mut second, &io).unwrap();
    drop(resumed);
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn readonly_refusal() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let conninfo =
        Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_READONLY").unwrap()).unwrap();
    let target = PgConnection::connect(&conninfo, &io).unwrap();
    assert!(TargetAuthority::acquire(target, stream, &mut native, &io).is_err());
    assert!(native.target_provenance(&stream).is_none());
}

fn named_connection(variable: &str, io: &IoControl) -> PgConnection {
    let value = Conninfo::new(std::env::var_os(variable).expect("owned fixture connection absent"))
        .unwrap();
    PgConnection::connect(&value, io).unwrap()
}

fn owned_provenance(native: &NativeState, stream: StreamId) -> ConfirmedProvenance {
    match native.target_provenance(&stream).unwrap() {
        TargetProvenance::Confirmed(confirmed) => confirmed.clone(),
        _ => panic!("metadata was not confirmed before admitting work"),
    }
}

fn qualified(connection: &mut PgConnection, provenance: &ConfirmedProvenance) -> String {
    crate::catalog::qualified(
        connection,
        &provenance.intent.binding.schema_name,
        &provenance.intent.binding.table_name,
    )
    .unwrap()
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn async_barrier() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut target = connection(&io);
    target
        .query("SET synchronous_commit = remote_apply", &io)
        .unwrap();
    let mut authority = TargetAuthority::acquire(target, stream, &mut native, &io).unwrap();
    let empty = authority.barrier(None, 3, &io).unwrap();
    assert_eq!(empty.frontier(), None);
    assert_eq!(empty.barrier_epoch(), 3);
    assert_eq!(empty.barrier_counter(), 2);
    let first = frontier(stream, 1);
    let CommitOutcome::Acknowledged {
        checkpoint,
        durable,
    } = authority
        .begin(first, &io)
        .unwrap()
        .commit_with_mode(TargetCommit::Asynchronous, &io)
        .unwrap()
    else {
        panic!("async COMMIT not acknowledged")
    };
    assert!(durable.is_none());
    assert_eq!(checkpoint.barrier_counter, 2);
    assert_eq!(authority.checkpoint(&io).unwrap(), Some(checkpoint));
    let proof = authority.barrier(Some(&first), 4, &io).unwrap();
    assert_eq!(proof.frontier(), Some(&first));
    assert_eq!(proof.barrier_epoch(), 4);
    assert_eq!(proof.barrier_counter(), 3);
    let setting = authority
        .session
        .connection()
        .unwrap()
        .query("SHOW synchronous_commit", &io)
        .unwrap();
    assert_eq!(setting.cell(0, 0).unwrap(), Some(&b"remote_apply"[..]));
    assert!(authority.barrier(None, 4, &io).is_err());
    authority.reacquire(connection(&io), &io).unwrap();
    assert!(authority.barrier(Some(&first), 3, &io).is_err());
    drop(authority);
    for weak in ["off", "local", "remote_write"] {
        let mut native = state();
        let stream = stream_for(&native);
        let mut target = connection(&io);
        target
            .query(&format!("SET synchronous_commit = {weak}"), &io)
            .unwrap();
        let mut owner = TargetAuthority::acquire(target, stream, &mut native, &io).unwrap();
        owner.barrier(None, 0, &io).unwrap();
        let setting = owner
            .session
            .connection()
            .unwrap()
            .query("SHOW synchronous_commit", &io)
            .unwrap();
        assert_eq!(setting.cell(0, 0).unwrap(), Some(&b"on"[..]));
    }
    let mut unsafe_target = named_connection("RPGMIGRATE_TEST_AUTHORITY_FSYNC_OFF", &io);
    let settings = unsafe_target
        .query(
            "SELECT NOT pg_catalog.pg_is_in_recovery()
        AND pg_catalog.current_setting('transaction_read_only') = 'off'
        AND pg_catalog.current_setting('fsync') = 'off'",
            &io,
        )
        .unwrap();
    assert_eq!(settings.cell(0, 0).unwrap(), Some(&b"t"[..]));
    let mut native = state();
    let stream = stream_for(&native);
    let failure = TargetAuthority::acquire(unsafe_target, stream, &mut native, &io)
        .err()
        .unwrap();
    assert!(failure.message.contains("fsync on"));
    assert!(native.target_provenance(&stream).is_none());
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn metadata_collision() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut observer = connection(&io);
    let name = observer.quote_identifier(&schema_name(stream)).unwrap();
    observer
        .query(&format!("CREATE SCHEMA {name}"), &io)
        .unwrap();
    observer
        .query(
            &format!("CREATE TABLE {name}.foreign_data (value integer)"),
            &io,
        )
        .unwrap();
    observer
        .query(&format!("INSERT INTO {name}.foreign_data VALUES (7)"), &io)
        .unwrap();
    assert!(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).is_err());
    let value = observer
        .query(&format!("SELECT value FROM {name}.foreign_data"), &io)
        .unwrap();
    assert_eq!(value.rows(), 1);
    assert_eq!(value.cell(0, 0).unwrap(), Some(&b"7"[..]));
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn metadata_shape_acl() {
    let io = control();
    for alteration in [
        "ENABLE ROW LEVEL SECURITY",
        "SET UNLOGGED",
        "ADD COLUMN foreign_column text",
    ] {
        let mut native = state();
        let stream = stream_for(&native);
        drop(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap());
        let provenance = owned_provenance(&native, stream);
        let mut setup = connection(&io);
        let table = qualified(&mut setup, &provenance);
        setup
            .query(&format!("ALTER TABLE {table} {alteration}"), &io)
            .unwrap();
        assert!(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).is_err());
    }
    for grant in ["SELECT", "UPDATE"] {
        let mut native = state();
        let stream = stream_for(&native);
        drop(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap());
        let mut setup = connection(&io);
        let table = qualified(&mut setup, &owned_provenance(&native, stream));
        setup
            .query(&format!("GRANT {grant} ON {table} TO PUBLIC"), &io)
            .unwrap();
        assert!(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).is_err());
        let privileges = setup
            .query(
                &format!("SELECT pg_catalog.has_table_privilege('public', '{table}', '{grant}')"),
                &io,
            )
            .unwrap();
        assert_eq!(privileges.cell(0, 0).unwrap(), Some(&b"t"[..]));
    }
    let mut setup = connection(&io);
    setup
        .query(
            "CREATE FUNCTION public.authority_metadata_trigger() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$",
            &io,
        )
        .unwrap();
    for alteration in [
        "ALTER TABLE @ ADD CONSTRAINT foreign_check CHECK (true)",
        "ALTER TABLE @ ALTER COLUMN magic SET DEFAULT 'foreign'",
        "CREATE INDEX foreign_index ON @ ((length(magic)))",
        "CREATE RULE foreign_rule AS ON UPDATE TO @ DO INSTEAD NOTHING",
        "CREATE TRIGGER foreign_trigger BEFORE UPDATE ON @ FOR EACH ROW EXECUTE PROCEDURE public.authority_metadata_trigger()",
        "CREATE TABLE $.foreign_child () INHERITS (@)",
        "UPDATE @ SET version = 2",
        "UPDATE @ SET nonce = pg_catalog.decode('00000000000000000000000000000000', 'hex')",
        "DELETE FROM @",
    ] {
        let mut native = state();
        let stream = stream_for(&native);
        drop(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap());
        let provenance = owned_provenance(&native, stream);
        let table = qualified(&mut setup, &provenance);
        let schema = setup
            .quote_identifier(&provenance.intent.binding.schema_name)
            .unwrap();
        setup
            .query(&alteration.replace('@', &table).replace('$', &schema), &io)
            .unwrap();
        assert!(
            TargetAuthority::acquire(connection(&io), stream, &mut native, &io).is_err(),
            "unsafe metadata accepted for case {alteration}"
        );
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn missing_privileges() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    assert!(
        TargetAuthority::acquire(
            named_connection("RPGMIGRATE_TEST_AUTHORITY_RESTRICTED", &io),
            stream,
            &mut native,
            &io
        )
        .is_err()
    );
    drop(native);
    let mut native = state();
    let stream = stream_for(&native);
    drop(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap());
    let provenance = owned_provenance(&native, stream);
    let mut admin = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
    let schema = admin
        .quote_identifier(&provenance.intent.binding.schema_name)
        .unwrap();
    let table = qualified(&mut admin, &provenance);
    admin
        .query(
            &format!("GRANT USAGE ON SCHEMA {schema} TO e3_restricted"),
            &io,
        )
        .unwrap();
    admin
        .query(
            &format!("GRANT SELECT, INSERT, UPDATE ON {table} TO e3_restricted"),
            &io,
        )
        .unwrap();
    for privilege in ["SELECT", "INSERT", "UPDATE"] {
        admin
            .query(
                &format!("REVOKE {privilege} ON {table} FROM e3_restricted"),
                &io,
            )
            .unwrap();
        assert!(
            TargetAuthority::acquire(
                named_connection("RPGMIGRATE_TEST_AUTHORITY_RESTRICTED", &io),
                stream,
                &mut native,
                &io
            )
            .is_err()
        );
        admin
            .query(
                &format!("GRANT {privilege} ON {table} TO e3_restricted"),
                &io,
            )
            .unwrap();
    }
    admin
        .query(
            &format!("REVOKE USAGE ON SCHEMA {schema} FROM e3_restricted"),
            &io,
        )
        .unwrap();
    assert!(
        TargetAuthority::acquire(
            named_connection("RPGMIGRATE_TEST_AUTHORITY_RESTRICTED", &io),
            stream,
            &mut native,
            &io
        )
        .is_err()
    );
    admin
        .query(
            &format!("GRANT USAGE ON SCHEMA {schema} TO e3_restricted"),
            &io,
        )
        .unwrap();
    let mut runtime = named_connection("RPGMIGRATE_TEST_AUTHORITY_RESTRICTED", &io);
    let privileges = runtime
        .query(
            &format!(
                "SELECT oid <> {owner},
        NOT pg_catalog.pg_has_role(CURRENT_USER, {owner}::oid, 'MEMBER'),
        NOT pg_catalog.has_database_privilege(pg_catalog.current_database(), 'CREATE'),
        NOT pg_catalog.has_function_privilege('pg_catalog.pg_control_system()', 'EXECUTE')
        FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER",
                owner = provenance.intent.binding.owner_oid
            ),
            &io,
        )
        .unwrap();
    assert_eq!(privileges.rows(), 1);
    for column in 0..4 {
        assert_eq!(privileges.cell(0, column).unwrap(), Some(&b"t"[..]));
    }
    let mut restricted = TargetAuthority::acquire(runtime, stream, &mut native, &io).unwrap();
    assert_eq!(restricted.barrier(None, 1, &io).unwrap().frontier(), None);
    drop(restricted);

    let results = [
        "E3 \"Quoted Writer\"".to_owned(),
        provenance.intent.binding.owner_oid.to_string(),
    ]
    .map(|writer| {
        let io = control();
        let mut native = state();
        let stream = stream_for(&native);
        drop(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap());
        let provenance = owned_provenance(&native, stream);
        let schema = admin
            .quote_identifier(&provenance.intent.binding.schema_name)
            .unwrap();
        let table = qualified(&mut admin, &provenance);
        let role = admin.quote_identifier(&writer).unwrap();
        admin
            .query(
                &format!("CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT"),
                &io,
            )
            .unwrap();
        admin
            .query(&format!("GRANT USAGE ON SCHEMA {schema} TO {role}"), &io)
            .unwrap();
        admin
            .query(&format!("GRANT SELECT, INSERT, UPDATE ON {table} TO {role}"), &io)
            .unwrap();
        let base = std::env::var("RPGMIGRATE_TEST_AUTHORITY_RESTRICTED").unwrap();
        let escaped = writer.replace('\\', "\\\\").replace('\'', "\\'");
        let conninfo = Conninfo::new(format!("{base} user='{escaped}'").into()).unwrap();
        let mut runtime = PgConnection::connect(&conninfo, &io).unwrap();
        let privileges = runtime.query(&format!(
            "SELECT rolname, oid <> {owner},
                NOT pg_catalog.pg_has_role(CURRENT_USER, {owner}::oid, 'MEMBER'),
                NOT pg_catalog.has_database_privilege(pg_catalog.current_database(), 'CREATE'),
                NOT pg_catalog.has_function_privilege('pg_catalog.pg_control_system()', 'EXECUTE'),
                pg_catalog.has_schema_privilege({schema_oid}::oid, 'USAGE'),
                pg_catalog.has_table_privilege({table_oid}::oid, 'SELECT'),
                pg_catalog.has_table_privilege({table_oid}::oid, 'INSERT'),
                pg_catalog.has_table_privilege({table_oid}::oid, 'UPDATE')
             FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER",
            owner = provenance.intent.binding.owner_oid,
            schema_oid = provenance.schema_oid,
            table_oid = provenance.table_oid,
        ), &io).unwrap();
        assert_eq!(privileges.rows(), 1);
        assert_eq!(privileges.cell(0, 0).unwrap(), Some(writer.as_bytes()));
        for column in 1..9 {
            assert_eq!(privileges.cell(0, column).unwrap(), Some(&b"t"[..]));
        }
        TargetAuthority::acquire(runtime, stream, &mut native, &io).and_then(|mut authority| {
            let proof = authority.barrier(None, 1, &io)?;
            assert_eq!(proof.frontier(), None);
            assert_eq!(proof.target(), &provenance);
            Ok(())
        })
    });
    assert!(
        results.iter().all(Result::is_ok),
        "writer recovery failed: {results:?}"
    );
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn foreign_replacement() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    drop(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap());
    let provenance = owned_provenance(&native, stream);
    let mut setup = connection(&io);
    let table = qualified(&mut setup, &provenance);
    setup
        .query(
            &format!("ALTER TABLE {table} RENAME TO saved_checkpoint"),
            &io,
        )
        .unwrap();
    setup
        .query(&format!("CREATE TABLE {table} (value integer)"), &io)
        .unwrap();
    setup
        .query(&format!("INSERT INTO {table} VALUES (44)"), &io)
        .unwrap();
    assert!(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).is_err());
    let value = setup
        .query(&format!("SELECT value FROM {table}"), &io)
        .unwrap();
    assert_eq!(value.cell(0, 0).unwrap(), Some(&b"44"[..]));
    setup.query(&format!("DROP TABLE {table}"), &io).unwrap();
    assert!(TargetAuthority::acquire(connection(&io), stream, &mut native, &io).is_err());
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn commit_rollback_tag() {
    let io = control();
    let mut observer = connection(&io);
    observer
        .query("CREATE TABLE public.authority_poison(value text)", &io)
        .unwrap();
    for rejected in [
        "parameter_nul",
        "statement_nul",
        "reserved_name",
        "excess_parameters",
        "unknown_statement",
    ] {
        let mut native = state();
        let stream = stream_for(&native);
        let mut authority =
            TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
        let mut tx = authority.begin(frontier(stream, 1), &io).unwrap();
        tx.prepare(
            "poison_insert",
            "INSERT INTO public.authority_poison VALUES ($1)",
            &[25],
            &io,
        )
        .unwrap();
        tx.execute_prepared("poison_insert", &[PgParam::Text(b"must roll back")], &io)
            .unwrap();
        let rejected = match rejected {
            "parameter_nul" => tx
                .execute_prepared("poison_insert", &[PgParam::Text(b"bad\0value")], &io)
                .map(|_| ()),
            "statement_nul" => tx.prepare(
                "rejected",
                "INSERT INTO public.authority_poison VALUES ('never')\0",
                &[],
                &io,
            ),
            "reserved_name" => tx.prepare(
                "rpgmigrate_escape",
                "INSERT INTO public.authority_poison VALUES ('never')",
                &[],
                &io,
            ),
            "excess_parameters" => tx.prepare(
                "rejected",
                "INSERT INTO public.authority_poison VALUES ('never')",
                &vec![25; 65_536],
                &io,
            ),
            "unknown_statement" => tx.execute_prepared("unknown", &[], &io).map(|_| ()),
            _ => unreachable!(),
        };
        assert!(rejected.is_err());
        assert_eq!(
            tx.authority
                .session
                .connection()
                .unwrap()
                .transaction_status()
                .unwrap(),
            TransactionStatus::InTransaction
        );
        assert!(
            tx.prepare(
                "after_error",
                "INSERT INTO public.authority_poison VALUES ('later')",
                &[],
                &io
            )
            .is_err()
        );
        assert!(
            tx.execute_prepared("poison_insert", &[PgParam::Text(b"later")], &io)
                .is_err()
        );
        assert!(
            tx.commit(&io).is_err(),
            "a swallowed client error allowed COMMIT"
        );
        assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
        let rows = observer
            .query("SELECT count(*) FROM public.authority_poison", &io)
            .unwrap();
        assert_eq!(rows.cell(0, 0).unwrap(), Some(&b"0"[..]));
    }
    let mut native = state();
    let stream = stream_for(&native);
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    let tx = authority.begin(frontier(stream, 1), &io).unwrap();
    let backend = tx.authority.session.connection().unwrap();
    assert!(backend.query("SELECT 1 / 0", &io).is_err());
    let result = backend.query("COMMIT", &io).unwrap();
    assert_eq!(result.command_tag().unwrap(), "ROLLBACK");
    assert_eq!(
        commit_evidence(
            result.command_tag().unwrap(),
            backend.transaction_status().unwrap()
        ),
        CommitEvidence::RolledBack
    );
    assert!(tx.commit(&io).is_err());
    assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
    let mut tx = authority.begin(frontier(stream, 1), &io).unwrap();
    assert!(tx.prepare("escape", "COMMIT", &[], &io).is_err());
    assert!(
        tx.execute_prepared("rpgmigrate_authority_guard", &[], &io)
            .is_err()
    );
    drop(tx);
    assert!(authority.checkpoint(&io).is_err());
    assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn deferred_setting_restore() {
    let io = control();
    let mut setup = connection(&io);
    setup
        .query("CREATE TABLE public.authority_deferred(value integer)", &io)
        .unwrap();
    setup.query("CREATE FUNCTION public.authority_weaken() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN PERFORM pg_catalog.set_config('synchronous_commit', 'off', false); RETURN NEW; END $$", &io).unwrap();
    setup
        .query(
            "CREATE CONSTRAINT TRIGGER authority_weaken AFTER INSERT ON public.authority_deferred
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE PROCEDURE public.authority_weaken()",
            &io,
        )
        .unwrap();
    let mut native = state();
    let stream = stream_for(&native);
    let mut target = connection(&io);
    target
        .query("SET synchronous_commit = remote_apply", &io)
        .unwrap();
    let mut authority = TargetAuthority::acquire(target, stream, &mut native, &io).unwrap();
    let mut tx = authority.begin(frontier(stream, 1), &io).unwrap();
    tx.prepare(
        "deferred_insert",
        "INSERT INTO public.authority_deferred VALUES (1)",
        &[],
        &io,
    )
    .unwrap();
    tx.execute_prepared("deferred_insert", &[], &io).unwrap();
    assert!(matches!(
        tx.commit(&io).unwrap(),
        CommitOutcome::Acknowledged {
            durable: Some(_),
            ..
        }
    ));
    let setting = authority
        .session
        .connection()
        .unwrap()
        .query("SHOW synchronous_commit", &io)
        .unwrap();
    assert_eq!(setting.cell(0, 0).unwrap(), Some(&b"remote_apply"[..]));
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn standby_refusal() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut standby = named_connection("RPGMIGRATE_TEST_AUTHORITY_STANDBY", &io);
    let result = standby
        .query("SELECT pg_catalog.pg_is_in_recovery()", &io)
        .unwrap();
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"t"[..]));
    assert!(TargetAuthority::acquire(standby, stream, &mut native, &io).is_err());
    assert!(native.target_provenance(&stream).is_none());
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn backend_fence_loss() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    authority
        .session
        .connection()
        .unwrap()
        .query("SELECT pg_catalog.pg_advisory_unlock_all()", &io)
        .unwrap();
    assert!(authority.begin(frontier(stream, 1), &io).is_err());
    assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
    authority.session.pid += 1;
    assert!(authority.begin(frontier(stream, 1), &io).is_err());
    assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
}

struct Fault {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    conninfo: Conninfo,
}

impl Fault {
    fn start(case: &str) -> Self {
        let mut child = Command::new(std::env::var_os("RPGMIGRATE_TEST_PYTHON").unwrap())
            .arg(std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER").unwrap())
            .args(["--case", case])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("owned COMMIT fault helper failed to start");
        let input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let ready = read_reply(&mut output);
        assert_eq!(ready["ready"], true);
        let conninfo = Conninfo::new(
            ready["conninfo"]
                .as_str()
                .expect("missing fault connection")
                .into(),
        )
        .unwrap();
        Self {
            child,
            input,
            output,
            conninfo,
        }
    }

    fn connect(&self, io: &IoControl) -> PgConnection {
        PgConnection::connect(&self.conninfo, io).unwrap()
    }

    fn request(&mut self, operation: &str) -> serde_json::Value {
        writeln!(self.input, "{}", serde_json::json!({"op": operation})).unwrap();
        self.input.flush().unwrap();
        read_reply(&mut self.output)
    }

    fn require_held(&mut self) {
        let status = self.request("status");
        assert_eq!(status["fault_hits"], 1);
        assert_eq!(status["commit_tag"], "COMMIT");
        assert_eq!(status["holding_backend"], true);
        assert_eq!(status["held_fence_observed"], true);
        assert!(status["backend_pid"].as_u64().unwrap() > 0);
    }

    fn release(&mut self) {
        let result = self.request("release");
        assert_eq!(result["released_fence_observed"], true);
        assert_eq!(result["holding_backend"], false);
    }

    fn finish(mut self) {
        self.request("quit");
        assert!(self.child.wait().unwrap().success());
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

fn read_reply(output: &mut BufReader<ChildStdout>) -> serde_json::Value {
    let mut line = String::new();
    assert!(
        output.read_line(&mut line).unwrap() > 0,
        "owned fault helper stopped without a reply"
    );
    serde_json::from_str(&line).expect("owned fault helper returned invalid JSON")
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn lost_commit_reply() {
    let io = control();
    let mut setup = connection(&io);
    setup
        .query("CREATE TABLE public.authority_once(value integer)", &io)
        .unwrap();
    let mut native = state();
    let stream = stream_for(&native);
    let mut fault = Fault::start("lost_commit_reply");
    let mut authority =
        TargetAuthority::acquire(fault.connect(&io), stream, &mut native, &io).unwrap();
    let first = frontier(stream, 1);
    let mut tx = authority.begin(first, &io).unwrap();
    tx.prepare(
        "insert_once",
        "INSERT INTO public.authority_once VALUES ($1)",
        &[23],
        &io,
    )
    .unwrap();
    tx.execute_prepared("insert_once", &[PgParam::Text(b"7")], &io)
        .unwrap();
    fault.request("arm");
    assert!(matches!(tx.commit(&io).unwrap(), CommitOutcome::Unknown));
    fault.require_held();
    assert!(authority.checkpoint(&io).is_err());
    let contender = connection(&io);
    let mut limited = control();
    limited.deadline = Instant::now() + Duration::from_millis(200);
    assert!(authority.reacquire(contender, &limited).is_err());
    fault.require_held();
    fault.release();
    let committed = authority.reacquire(connection(&io), &io).unwrap().unwrap();
    assert_eq!(committed.applied, first);
    assert_eq!(committed.barrier_counter, 2);
    let rows = setup
        .query(
            "SELECT count(*), min(value), max(value) FROM public.authority_once",
            &io,
        )
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(&b"1"[..]));
    assert_eq!(rows.cell(0, 1).unwrap(), Some(&b"7"[..]));
    assert_eq!(rows.cell(0, 2).unwrap(), Some(&b"7"[..]));
    let mut tx = authority.begin(frontier(stream, 2), &io).unwrap();
    assert!(
        tx.execute_prepared("insert_once", &[PgParam::Text(b"8")], &io)
            .is_err()
    );
    tx.rollback(&io).unwrap();
    let mut tx = authority.begin(frontier(stream, 2), &io).unwrap();
    tx.prepare(
        "insert_once",
        "INSERT INTO public.authority_once VALUES ($1)",
        &[23],
        &io,
    )
    .unwrap();
    tx.rollback(&io).unwrap();
    fault.finish();
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn provisioning_recovery() {
    let io = control();
    let mut native = state();
    let path = native.identity().canonical_path.clone();
    let stream = stream_for(&native);
    let mut fault = Fault::start("provisioning_recovery");
    fault.request("arm");
    let failure = TargetAuthority::acquire(fault.connect(&io), stream, &mut native, &io)
        .err()
        .unwrap();
    assert!(failure.message.contains("COMMIT outcome unknown"));
    assert!(matches!(
        native.target_provenance(&stream),
        Some(TargetProvenance::Provisioning(_))
    ));
    fault.require_held();
    drop(native);
    let mut recovered = crate::state::open_for_provenance_recovery(&path).unwrap();
    let contender = connection(&io);
    let mut limited = control();
    limited.deadline = Instant::now() + Duration::from_millis(200);
    assert!(TargetAuthority::acquire(contender, stream, &mut recovered, &limited).is_err());
    fault.require_held();
    fault.release();
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut recovered, &io).unwrap();
    assert_eq!(authority.checkpoint(&io).unwrap(), None);
    let proof = authority.barrier(None, 0, &io).unwrap();
    assert_eq!(proof.frontier(), None);
    assert_eq!(proof.barrier_counter(), 3);
    let binding = owned_provenance(&recovered, stream).intent.binding;
    drop(authority);
    fault.finish();

    let mut weak = state();
    let weak_stream = stream_for(&weak);
    let weak_path = weak.identity().canonical_path.clone();
    weak.begin_target_provisioning(TargetBinding {
        stream: weak_stream,
        target_system_identifier: None,
        schema_name: schema_name(weak_stream),
        ..binding.clone()
    })
    .unwrap();
    drop(weak);
    let mut weak = crate::state::open_for_provenance_recovery(&weak_path).unwrap();
    let failure = TargetAuthority::acquire(connection(&io), weak_stream, &mut weak, &io)
        .err()
        .unwrap();
    assert!(failure.message.contains("without strong target identity"));

    let mut strong = state();
    let strong_stream = stream_for(&strong);
    let strong_path = strong.identity().canonical_path.clone();
    let mut admin = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
    let system = optional_system_identifier(&mut admin, &io)
        .unwrap()
        .unwrap();
    let role = admin
        .query(
            "SELECT oid FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER",
            &io,
        )
        .unwrap();
    let intent = strong
        .begin_target_provisioning(TargetBinding {
            stream: strong_stream,
            target_system_identifier: Some(system),
            owner_oid: number(&role, 0).unwrap(),
            schema_name: schema_name(strong_stream),
            ..binding
        })
        .unwrap();
    drop(strong);
    let mut strong = crate::state::open_for_provenance_recovery(&strong_path).unwrap();
    let owner = TargetAuthority::acquire(admin, strong_stream, &mut strong, &io).unwrap();
    assert_eq!(owned_provenance(&strong, strong_stream).intent, intent);
    drop(owner);
    assert!(TargetAuthority::acquire(connection(&io), strong_stream, &mut strong, &io).is_err());
}
