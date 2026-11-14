use super::*;
use crate::cdc::contracts::{Column, Event, RelationKey};
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
        commit: Some(CommitLsn(100)),
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
    let table = qualified(&mut setup, &owned_provenance(&native, stream));
    let next = Frontier {
        commit: None,
        ..frontier(stream, 1)
    };
    let mut tx = authority.begin(next, &io).unwrap();
    let names = ["ALL", "All\"MiXeD; --"];
    for name in names {
        tx.prepare(
            name,
            "INSERT INTO public.authority_atomic VALUES ($1)",
            &[23],
            &io,
        )
        .unwrap();
    }
    tx.deallocate(names[1], &io).unwrap();
    assert!(!tx.authority.prepared.contains(names[1]));
    assert!(tx.authority.prepared.contains(names[0]));
    tx.execute_prepared(names[0], &[PgParam::Text(b"42")], &io)
        .unwrap();
    tx.prepare(
        names[1],
        "INSERT INTO public.authority_atomic VALUES ($1 + 1)",
        &[23],
        &io,
    )
    .unwrap();
    tx.execute_prepared(names[1], &[PgParam::Text(b"42")], &io)
        .unwrap();
    tx.deallocate(names[0], &io).unwrap();
    assert!(!tx.authority.prepared.contains(names[0]));
    assert!(tx.authority.prepared.contains(names[1]));
    tx.execute_prepared(names[1], &[PgParam::Text(b"43")], &io)
        .unwrap();
    tx.prepare(
        names[0],
        "INSERT INTO public.authority_atomic VALUES ($1 + 2)",
        &[23],
        &io,
    )
    .unwrap();
    tx.execute_prepared(names[0], &[PgParam::Text(b"43")], &io)
        .unwrap();
    let before = setup
        .query(
            &format!(
                "SELECT applied_order, (SELECT count(*) FROM ONLY public.authority_atomic) FROM ONLY {table}"
            ),
            &io,
        )
        .unwrap();
    assert_eq!(before.rows(), 1);
    assert_eq!(before.cell(0, 0).unwrap(), Some(b"0".as_slice()));
    assert_eq!(before.cell(0, 1).unwrap(), Some(b"0".as_slice()));
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
    assert_eq!(
        proof.target().intent.binding.metadata_version,
        TARGET_METADATA_VERSION
    );
    assert_eq!(authority.checkpoint(&io).unwrap(), Some(checkpoint));
    let position = setup.query(&format!(
        "SELECT applied_order, last_xid, begin_ordinal, source_commit_lsn, source_end_lsn FROM {table}"
    ), &io).unwrap();
    assert_eq!(position.rows(), 1);
    assert_eq!(position.cell(0, 0).unwrap(), Some(b"1".as_slice()));
    assert_eq!(position.cell(0, 1).unwrap(), Some(b"99".as_slice()));
    assert_eq!(position.cell(0, 2).unwrap(), Some(b"99".as_slice()));
    assert_eq!(position.cell(0, 3).unwrap(), None);
    assert_eq!(position.cell(0, 4).unwrap(), Some(b"0/78".as_slice()));
    let rows = setup
        .query(
            "SELECT value FROM public.authority_atomic ORDER BY value",
            &io,
        )
        .unwrap();
    assert_eq!(rows.rows(), 4);
    for (row, value) in [b"42", b"43", b"44", b"45"].iter().enumerate() {
        assert_eq!(rows.cell(row, 0).unwrap(), Some(value.as_slice()));
    }

    command(
        &mut setup,
        "CREATE TABLE public.authority_preflight_ok(id integer PRIMARY KEY, payload text)",
        "CREATE TABLE",
        &io,
    )
    .unwrap();
    let relation = Event::Relation {
        key: RelationKey {
            oid: 1,
            generation: 17,
        },
        schema: "public".into(),
        name: "authority_preflight_ok".into(),
        columns: vec![
            Column {
                name: "id".into(),
                type_oid: 23,
                type_mod: -1,
                key: true,
            },
            Column {
                name: "payload".into(),
                type_oid: 25,
                type_mod: -1,
                key: false,
            },
        ],
        replica_identity: b'd',
    };
    let next = frontier(stream, 2);
    let mut tx = authority.begin(next, &io).unwrap();
    let before = tx
        .authority
        .session
        .connection()
        .unwrap()
        .query("SELECT pg_catalog.pg_backend_pid()", &io)
        .unwrap();
    assert_eq!(before.rows(), 1);
    let pid = number(&before, 0).unwrap();
    assert_eq!(pid, tx.authority.session.pid);
    tx.preflight_relation(&relation, &io).unwrap();
    let owner = tx.authority.session.connection().unwrap().query(
        "SELECT pg_catalog.pg_backend_pid(), 'public.authority_preflight_ok'::pg_catalog.regclass::pg_catalog.oid,
            EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype = 'relation'
                AND pid = pg_catalog.pg_backend_pid()
                AND relation = 'public.authority_preflight_ok'::pg_catalog.regclass
                AND mode = 'ShareRowExclusiveLock' AND granted)",
        &io,
    ).unwrap();
    assert_eq!(owner.rows(), 1);
    assert_eq!(number(&owner, 0).unwrap(), pid);
    assert_ne!(number(&owner, 1).unwrap(), 1);
    assert_eq!(owner.cell(0, 2).unwrap(), Some(b"t".as_slice()));
    command(&mut setup, "BEGIN", "BEGIN", &io).unwrap();
    let blocked = setup
        .query(
            "LOCK TABLE ONLY public.authority_preflight_ok IN ROW EXCLUSIVE MODE NOWAIT",
            &io,
        )
        .err()
        .expect("preflight did not retain its target relation lock");
    assert!(blocked.message.contains("SQLSTATE 55P03"), "{blocked:?}");
    command(&mut setup, "ROLLBACK", "ROLLBACK", &io).unwrap();
    tx.prepare(
        "preflight_insert",
        "INSERT INTO public.authority_preflight_ok VALUES ($1, $2)",
        &[23, 25],
        &io,
    )
    .unwrap();
    tx.execute_prepared(
        "preflight_insert",
        &[PgParam::Text(b"1"), PgParam::Text(b"checked")],
        &io,
    )
    .unwrap();
    let before = setup.query(
        &format!("SELECT applied_order, (SELECT count(*) FROM ONLY public.authority_preflight_ok) FROM ONLY {table}"),
        &io,
    ).unwrap();
    assert_eq!(before.rows(), 1);
    assert_eq!(before.cell(0, 0).unwrap(), Some(b"1".as_slice()));
    assert_eq!(before.cell(0, 1).unwrap(), Some(b"0".as_slice()));
    let CommitOutcome::Acknowledged {
        checkpoint,
        durable: Some(proof),
    } = tx.commit(&io).unwrap()
    else {
        panic!("preflight transaction produced no durable acknowledgment")
    };
    assert_eq!(checkpoint.applied, next);
    assert_eq!(proof.frontier(), Some(&next));
    let applied = setup.query(
        &format!("SELECT c.applied_order, t.id, t.payload FROM ONLY {table} c CROSS JOIN public.authority_preflight_ok t"),
        &io,
    ).unwrap();
    assert_eq!(applied.rows(), 1);
    assert_eq!(applied.cell(0, 0).unwrap(), Some(b"2".as_slice()));
    assert_eq!(applied.cell(0, 1).unwrap(), Some(b"1".as_slice()));
    assert_eq!(applied.cell(0, 2).unwrap(), Some(b"checked".as_slice()));
    command(&mut setup, "BEGIN", "BEGIN", &io).unwrap();
    command(
        &mut setup,
        "LOCK TABLE ONLY public.authority_preflight_ok IN ROW EXCLUSIVE MODE NOWAIT",
        "LOCK TABLE",
        &io,
    )
    .unwrap();
    command(&mut setup, "ROLLBACK", "ROLLBACK", &io).unwrap();
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn metadata_boundary_locks() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut observer = connection(&io);
    observer
        .query(
            "CREATE TABLE public.authority_boundary_locks(value integer)",
            &io,
        )
        .unwrap();
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    let table = qualified(&mut observer, &owned_provenance(&native, stream));
    let cached = authority.row.clone();
    let probes = [
        (
            format!("LOCK TABLE ONLY {table} IN SHARE MODE NOWAIT"),
            "LOCK TABLE",
        ),
        (
            format!("LOCK TABLE ONLY {table} IN ROW EXCLUSIVE MODE NOWAIT"),
            "LOCK TABLE",
        ),
        // ROW SHARE is compatible with SRE, so only the row lock can reject this probe.
        (
            format!("SELECT run_id FROM ONLY {table} FOR UPDATE NOWAIT"),
            "SELECT 1",
        ),
    ];
    let mut tx = authority.begin(frontier(stream, 1), &io).unwrap();
    tx.prepare(
        "boundary_lock_insert",
        "INSERT INTO public.authority_boundary_locks VALUES ($1)",
        &[23],
        &io,
    )
    .unwrap();
    for value in [b"1", b"2"] {
        for (sql, _) in &probes {
            command(&mut observer, "BEGIN", "BEGIN", &io).unwrap();
            let error = observer
                .query(sql, &io)
                .err()
                .expect("metadata lock did not block its independent contender");
            assert!(error.message.contains("SQLSTATE 55P03"), "{error:?}");
            command(&mut observer, "ROLLBACK", "ROLLBACK", &io).unwrap();
        }
        tx.execute_prepared("boundary_lock_insert", &[PgParam::Text(value)], &io)
            .unwrap();
    }
    tx.rollback(&io).unwrap();
    for (sql, tag) in &probes {
        command(&mut observer, "BEGIN", "BEGIN", &io).unwrap();
        command(&mut observer, sql, tag, &io).unwrap();
        command(&mut observer, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }
    command(
        &mut observer,
        &format!("UPDATE ONLY {table} SET barrier_counter = barrier_counter + 1"),
        "UPDATE 1",
        &io,
    )
    .unwrap();
    let error = authority
        .begin(frontier(stream, 1), &io)
        .err()
        .expect("begin adopted a checkpoint changed between owned transactions");
    assert_eq!(
        error.message,
        "target-authority: checkpoint changed before owned transaction"
    );
    assert_eq!(authority.row, cached);
    assert!(matches!(authority.session.phase, Phase::Poisoned));
    assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
    let rows = observer
        .query("SELECT count(*) FROM public.authority_boundary_locks", &io)
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(b"0".as_slice()));
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn transaction_boundary_faults() {
    let io = control();
    let mut observer = connection(&io);
    observer
        .query(
            "CREATE TABLE public.authority_boundary_faults(value integer)",
            &io,
        )
        .unwrap();
    for fault in [
        "metadata_read_denied",
        "fence_lost",
        "pid_mismatch",
        "metadata_row_changed",
        "metadata_shape_changed",
        "raw_rollback",
    ] {
        let mut native = state();
        let stream = stream_for(&native);
        let mut authority =
            TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
        let provenance = owned_provenance(&native, stream);
        let table = qualified(&mut observer, &provenance);
        let next = frontier(stream, 1);
        let mut warm = authority.begin(next, &io).unwrap();
        warm.prepare(
            "boundary_fault_insert",
            "INSERT INTO public.authority_boundary_faults VALUES ($1)",
            &[23],
            &io,
        )
        .unwrap();
        warm.execute_prepared("boundary_fault_insert", &[PgParam::Text(b"0")], &io)
            .unwrap();
        warm.rollback(&io).unwrap();
        let mut tx = authority.begin(next, &io).unwrap();
        let expected = match fault {
            "metadata_read_denied" => {
                command(
                    tx.authority.session.connection().unwrap(),
                    &format!("REVOKE SELECT ON TABLE {table} FROM CURRENT_USER"),
                    "REVOKE",
                    &io,
                )
                .unwrap();
                let denied = tx
                    .authority
                    .session
                    .connection()
                    .unwrap()
                    .query(
                        &format!(
                            "SELECT pg_catalog.has_table_privilege({}, 'SELECT')",
                            provenance.table_oid
                        ),
                        &io,
                    )
                    .unwrap();
                assert_eq!(denied.rows(), 1);
                assert_eq!(denied.cell(0, 0).unwrap(), Some(b"f".as_slice()));
                "target-metadata: object identity, schema, or privileges are unsafe"
            }
            "fence_lost" => {
                tx.authority
                    .session
                    .connection()
                    .unwrap()
                    .query("SELECT pg_catalog.pg_advisory_unlock_all()", &io)
                    .unwrap();
                "target-authority: backend, primary, durability settings, or writer fence changed"
            }
            "pid_mismatch" => {
                tx.authority.session.pid += 1;
                "target-authority: backend, primary, durability settings, or writer fence changed"
            }
            "metadata_row_changed" => {
                command(
                    tx.authority.session.connection().unwrap(),
                    &format!("UPDATE ONLY {table} SET barrier_counter = barrier_counter + 1"),
                    "UPDATE 1",
                    &io,
                )
                .unwrap();
                "target-authority: checkpoint changed outside its owner"
            }
            "metadata_shape_changed" => {
                command(
                    tx.authority.session.connection().unwrap(),
                    &format!("ALTER TABLE {table} ADD COLUMN unexpected integer"),
                    "ALTER TABLE",
                    &io,
                )
                .unwrap();
                "target-metadata: object identity, schema, or privileges are unsafe"
            }
            "raw_rollback" => {
                command(
                    tx.authority.session.connection().unwrap(),
                    "ROLLBACK",
                    "ROLLBACK",
                    &io,
                )
                .unwrap();
                "target-authority: transaction poisoned by an earlier DML error"
            }
            _ => unreachable!(),
        };
        let result = tx.execute_prepared("boundary_fault_insert", &[PgParam::Text(b"1")], &io);
        if fault == "raw_rollback" {
            let error = result
                .err()
                .expect("DML dispatched outside its owned transaction");
            assert_eq!(
                error.message,
                "target-authority: a valid owned transaction is required"
            );
            assert!(tx.failed);
        } else {
            result.unwrap();
            assert!(!tx.failed);
        }
        let before_commit = observer
            .query("SELECT count(*) FROM public.authority_boundary_faults", &io)
            .unwrap();
        assert_eq!(
            before_commit.cell(0, 0).unwrap(),
            Some(b"0".as_slice()),
            "{fault}"
        );
        let error = tx
            .commit(&io)
            .expect_err(&format!("COMMIT accepted {fault}"));
        assert_eq!(error.message, expected, "{fault}");
        assert!(matches!(authority.session.phase, Phase::Poisoned));
        assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
        let rows = observer
            .query("SELECT count(*) FROM public.authority_boundary_faults", &io)
            .unwrap();
        assert_eq!(rows.cell(0, 0).unwrap(), Some(b"0".as_slice()));
    }
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
    let next = Frontier {
        commit: None,
        ..frontier(stream, 1)
    };
    let mut tx = authority.begin(next, &io).unwrap();
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
    let mut tx = authority.begin(next, &io).unwrap();
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
            commit: Some(CommitLsn(99)),
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
    for (index, commit) in [None, None, Some(CommitLsn(99)), Some(CommitLsn(99))]
        .into_iter()
        .enumerate()
    {
        let next = Frontier {
            commit,
            ..frontier(stream, index as u64 + 3)
        };
        for invalid in [
            Frontier {
                end: SourceEndLsn(119),
                ..next
            },
            Frontier {
                order: TxSeq(next.order.0 + 1),
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
        let CommitOutcome::Acknowledged {
            checkpoint,
            durable: Some(proof),
        } = authority.begin(next, &io).unwrap().commit(&io).unwrap()
        else {
            panic!("optional position transaction produced no durable proof")
        };
        assert_eq!(checkpoint.applied, next);
        assert_eq!(proof.frontier(), Some(&next));
        assert_eq!(authority.checkpoint(&io).unwrap().unwrap().applied, next);
    }
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
    let first = Frontier {
        commit: None,
        ..frontier(stream, 1)
    };
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
    let fabricated = Frontier {
        commit: Some(CommitLsn(100)),
        ..first
    };
    assert!(authority.barrier(Some(&fabricated), 4, &io).is_err());
    assert_eq!(
        authority.reacquire(connection(&io), &io).unwrap().unwrap(),
        Checkpoint {
            barrier_epoch: 4,
            barrier_counter: 3,
            ..checkpoint
        }
    );
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
    assert_eq!(
        failure.message,
        "target-authority: writable primary with fsync and full_page_writes on is required"
    );
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
        "ALTER TABLE @ ALTER COLUMN magic SET (n_distinct = 5)",
        "ALTER TABLE $.copy_done ALTER COLUMN magic SET (n_distinct = 5)",
        "CREATE INDEX foreign_index ON @ ((length(magic)))",
        "CREATE RULE foreign_rule AS ON UPDATE TO @ DO INSTEAD NOTHING",
        "CREATE TRIGGER foreign_trigger BEFORE UPDATE ON @ FOR EACH ROW EXECUTE PROCEDURE public.authority_metadata_trigger()",
        "CREATE TABLE $.foreign_child () INHERITS (@)",
        "UPDATE @ SET version = 1",
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
        if alteration == "UPDATE @ SET version = 1" {
            let version = setup
                .query(&format!("SELECT version FROM {table}"), &io)
                .unwrap();
            assert_eq!(version.rows(), 1);
            assert_eq!(version.cell(0, 0).unwrap(), Some(b"1".as_slice()));
        }
    }
    for alteration in [
        "last_xid = NULL",
        "begin_ordinal = NULL",
        "source_end_lsn = NULL",
        "source_commit_lsn = '0/79'",
        "applied_order = 0",
    ] {
        let mut native = state();
        let stream = stream_for(&native);
        let mut authority =
            TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
        let next = Frontier {
            commit: None,
            ..frontier(stream, 1)
        };
        authority.begin(next, &io).unwrap().commit(&io).unwrap();
        drop(authority);
        let table = qualified(&mut setup, &owned_provenance(&native, stream));
        setup
            .query(&format!("UPDATE {table} SET {alteration}"), &io)
            .unwrap();
        assert!(
            TargetAuthority::acquire(connection(&io), stream, &mut native, &io).is_err(),
            "invalid nonempty frontier accepted: {alteration}"
        );
    }
    let mut native = state();
    let stream = stream_for(&native);
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    authority
        .begin(
            Frontier {
                commit: None,
                ..frontier(stream, 1)
            },
            &io,
        )
        .unwrap()
        .commit(&io)
        .unwrap();
    let checkpoint = authority.checkpoint(&io).unwrap();
    let provenance = owned_provenance(&native, stream);
    for commit in [None, Some(CommitLsn(0))] {
        authority.session.begin(&io).unwrap();
        let old = authority.read_row(&io).unwrap();
        let mut stored = old.clone();
        stored.applied.as_mut().unwrap().commit = commit;
        metadata::update(
            authority.session.connection().unwrap(),
            &provenance,
            &old,
            &stored,
            &io,
        )
        .unwrap();
        let mut stale = stored.clone();
        stale.applied.as_mut().unwrap().commit = if commit.is_some() {
            None
        } else {
            Some(CommitLsn(0))
        };
        let epoch = stored.barrier_epoch;
        let next = increment_barrier(stored.clone(), epoch).unwrap();
        assert!(
            metadata::update(
                authority.session.connection().unwrap(),
                &provenance,
                &stale,
                &next,
                &io
            )
            .is_err()
        );
        assert_eq!(authority.read_row(&io).unwrap(), stored);
        authority.session.rollback(&io).unwrap();
        assert_eq!(authority.checkpoint(&io).unwrap(), checkpoint);
    }
    drop(authority);
    installed_snapshot_anchor(&mut setup, &io);
}

fn assert_snapshot_anchor_storage(
    observer: &mut PgConnection,
    table: &str,
    expected: &[&[u8]; 3],
    order: u64,
    effects: u64,
    io: &IoControl,
) -> u64 {
    let stored = observer
        .query(
            &format!(
                "SELECT copy_snapshot_token, copy_consistent_point::text,
            pg_catalog.encode(copy_plan_nonce, 'hex'), applied_order, barrier_counter,
            (SELECT count(*) FROM ONLY public.authority_anchor_rows) FROM ONLY {table}"
            ),
            io,
        )
        .unwrap();
    assert_eq!(stored.rows(), 1);
    for (column, expected) in expected.iter().enumerate() {
        assert_eq!(stored.cell(0, column).unwrap(), Some(*expected));
    }
    assert_eq!(
        stored.cell(0, 3).unwrap(),
        Some(order.to_string().as_bytes())
    );
    assert_eq!(
        stored.cell(0, 5).unwrap(),
        Some(effects.to_string().as_bytes())
    );
    std::str::from_utf8(stored.cell(0, 4).unwrap().unwrap())
        .unwrap()
        .parse()
        .unwrap()
}

fn installed_snapshot_anchor(observer: &mut PgConnection, io: &IoControl) {
    command(
        observer,
        "CREATE TABLE public.authority_anchor_rows(id integer PRIMARY KEY)",
        "CREATE TABLE",
        io,
    )
    .unwrap();
    let mut native = state();
    let stream = stream_for(&native);
    let mut authority = TargetAuthority::acquire(connection(io), stream, &mut native, io).unwrap();
    let provenance = owned_provenance(&native, stream);
    let table = qualified(observer, &provenance);
    let anchor = crate::contracts::SnapshotAnchor {
        stream,
        snapshot_token: "snapshot 'quoted' \"token\" \\\\ été".into(),
        consistent_point: SourceEndLsn(0xF123_4567_89AB_CDE0),
        plan_nonce: [
            0, 0x7f, 0x80, 0xff, 0x27, 0x5c, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
            0xaa,
        ],
    };
    let expected_fields: [&[u8]; 3] = [
        anchor.snapshot_token.as_bytes(),
        b"F1234567/89ABCDE0",
        b"007f80ff275c112233445566778899aa",
    ];
    let columns = observer
        .query(
            &format!(
                "SELECT attname, atttypid, atttypmod, attnotnull, attcollation,
            'pg_catalog.\"C\"'::pg_catalog.regcollation::pg_catalog.oid
            FROM pg_catalog.pg_attribute WHERE attrelid = {} AND attnum BETWEEN 20 AND 25
            ORDER BY attnum",
                provenance.table_oid
            ),
            io,
        )
        .unwrap();
    assert_eq!(columns.rows(), 6);
    for (row, (name, oid)) in [
        ("copy_snapshot_token", "25"),
        ("copy_consistent_point", "3220"),
        ("copy_plan_nonce", "17"),
        ("clone_state", "25"),
        ("certified_lsn", "3220"),
        ("follow_binding", "25"),
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(columns.cell(row, 0).unwrap(), Some(name.as_bytes()));
        assert_eq!(columns.cell(row, 1).unwrap(), Some(oid.as_bytes()));
        assert_eq!(columns.cell(row, 2).unwrap(), Some(b"-1".as_slice()));
        assert_eq!(columns.cell(row, 3).unwrap(), Some(b"f".as_slice()));
    }
    assert_eq!(columns.cell(0, 4).unwrap(), columns.cell(0, 5).unwrap());
    assert_eq!(columns.cell(1, 4).unwrap(), Some(b"0".as_slice()));
    assert_eq!(columns.cell(2, 4).unwrap(), Some(b"0".as_slice()));
    assert_eq!(columns.cell(3, 4).unwrap(), columns.cell(3, 5).unwrap());
    assert_eq!(columns.cell(4, 4).unwrap(), Some(b"0".as_slice()));
    assert_eq!(columns.cell(5, 4).unwrap(), columns.cell(5, 5).unwrap());
    authority.session.begin(io).unwrap();
    let types = authority
        .session
        .connection()
        .unwrap()
        .query(
            "SELECT pg_catalog.cardinality(parameter_types),
            parameter_types[20]::pg_catalog.oid, parameter_types[21]::pg_catalog.oid,
            parameter_types[22]::pg_catalog.oid, parameter_types[23]::pg_catalog.oid,
            parameter_types[24]::pg_catalog.oid, parameter_types[25]::pg_catalog.oid,
            parameter_types[37]::pg_catalog.oid, parameter_types[38]::pg_catalog.oid,
            parameter_types[39]::pg_catalog.oid, parameter_types[40]::pg_catalog.oid,
            parameter_types[41]::pg_catalog.oid, parameter_types[42]::pg_catalog.oid
            FROM pg_catalog.pg_prepared_statements WHERE name = 'rpgmigrate_metadata_update'",
            io,
        )
        .unwrap();
    assert_eq!(types.rows(), 1);
    for (column, expected) in [
        "46", "25", "3220", "17", "25", "3220", "25", "25", "3220", "17", "25", "3220", "25",
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(types.cell(0, column).unwrap(), Some(expected.as_bytes()));
    }
    let old = authority.read_row(io).unwrap();
    assert_eq!(old.applied, None);
    assert_eq!(old.anchor, None);
    let installed = ControlRow {
        anchor: Some(anchor.clone()),
        ..old.clone()
    };
    let mut foreign = installed.clone();
    foreign.anchor.as_mut().unwrap().stream.generation += 1;
    let error = metadata::update(
        authority.session.connection().unwrap(),
        &provenance,
        &old,
        &foreign,
        io,
    )
    .unwrap_err();
    assert_eq!(error.message, "target-metadata: invalid snapshot anchor");
    assert_eq!(authority.read_row(io).unwrap(), old);
    metadata::update(
        authority.session.connection().unwrap(),
        &provenance,
        &old,
        &installed,
        io,
    )
    .unwrap();
    assert_eq!(authority.read_row(io).unwrap(), installed);
    let invisible = observer.query(
        &format!("SELECT copy_snapshot_token, copy_consistent_point, copy_plan_nonce FROM ONLY {table}"), io,
    ).unwrap();
    assert_eq!(invisible.rows(), 1);
    for column in 0..3 {
        assert_eq!(invisible.cell(0, column).unwrap(), None);
    }
    // Commit fixture metadata without constructing snapshot coverage evidence.
    command(
        authority.session.connection().unwrap(),
        "COMMIT",
        "COMMIT",
        io,
    )
    .unwrap();
    assert_eq!(
        authority
            .session
            .connection()
            .unwrap()
            .transaction_status()
            .unwrap(),
        TransactionStatus::Idle
    );
    drop(authority);
    let mut authority = TargetAuthority::acquire(connection(io), stream, &mut native, io).unwrap();
    assert_eq!(authority.row.anchor.as_ref(), Some(&anchor));
    assert_eq!(authority.checkpoint(io).unwrap(), None);
    let installed_counter =
        assert_snapshot_anchor_storage(observer, &table, &expected_fields, 0, 0, io);
    for field in 0..3 {
        authority.session.begin(io).unwrap();
        let stored = authority.read_row(io).unwrap();
        let mut stale = stored.clone();
        let changed = stale.anchor.as_mut().unwrap();
        match field {
            0 => changed.snapshot_token.push('x'),
            1 => changed.consistent_point.0 += 1,
            2 => changed.plan_nonce[0] ^= 1,
            _ => unreachable!(),
        }
        let next = increment_barrier(stale.clone(), stale.barrier_epoch).unwrap();
        let error = metadata::update(
            authority.session.connection().unwrap(),
            &provenance,
            &stale,
            &next,
            io,
        )
        .unwrap_err();
        assert_eq!(
            error.message, "target-metadata: unexpected command result or affected row count",
            "stale anchor field {field}"
        );
        assert_eq!(
            authority
                .session
                .connection()
                .unwrap()
                .transaction_status()
                .unwrap(),
            TransactionStatus::InTransaction
        );
        assert_eq!(authority.read_row(io).unwrap(), stored);
        authority.session.rollback(io).unwrap();
        assert_eq!(
            assert_snapshot_anchor_storage(observer, &table, &expected_fields, 0, 0, io),
            installed_counter
        );
    }
    let next = Frontier {
        commit: Some(CommitLsn(anchor.consistent_point.0 + 0x10)),
        end: SourceEndLsn(anchor.consistent_point.0 + 0x20),
        ..frontier(stream, 1)
    };
    let mut tx = authority.begin(next, io).unwrap();
    tx.prepare(
        "anchor_insert",
        "INSERT INTO public.authority_anchor_rows VALUES ($1)",
        &[23],
        io,
    )
    .unwrap();
    tx.execute_prepared("anchor_insert", &[PgParam::Text(b"7")], io)
        .unwrap();
    assert_eq!(
        assert_snapshot_anchor_storage(observer, &table, &expected_fields, 0, 0, io),
        installed_counter
    );
    assert!(matches!(
        tx.commit(io).unwrap(),
        CommitOutcome::Acknowledged {
            durable: Some(_),
            ..
        }
    ));
    assert_eq!(authority.row.anchor.as_ref(), Some(&anchor));
    let cdc_counter = assert_snapshot_anchor_storage(observer, &table, &expected_fields, 1, 1, io);
    assert_eq!(cdc_counter, installed_counter + 1);
    let epoch = authority.row.barrier_epoch;
    let barrier = authority.barrier(Some(&next), epoch, io).unwrap();
    assert_eq!(barrier.frontier(), Some(&next));
    assert_eq!(authority.row.anchor.as_ref(), Some(&anchor));
    let barrier_counter =
        assert_snapshot_anchor_storage(observer, &table, &expected_fields, 1, 1, io);
    assert_eq!(barrier_counter, cdc_counter + 1);
    for field in 0..4 {
        authority.session.begin(io).unwrap();
        let stored = authority.read_row(io).unwrap();
        let mut changed = stored.clone();
        match field {
            0 => changed.anchor.as_mut().unwrap().snapshot_token.push('x'),
            1 => changed.anchor.as_mut().unwrap().consistent_point.0 += 1,
            2 => changed.anchor.as_mut().unwrap().plan_nonce[0] ^= 1,
            3 => changed.anchor = None,
            _ => unreachable!(),
        }
        let error = metadata::update(
            authority.session.connection().unwrap(),
            &provenance,
            &stored,
            &changed,
            io,
        )
        .unwrap_err();
        assert_eq!(
            error.message,
            "target-metadata: snapshot anchor cannot be replaced or removed"
        );
        assert_eq!(authority.read_row(io).unwrap(), stored);
        authority.session.rollback(io).unwrap();
        assert_eq!(
            assert_snapshot_anchor_storage(observer, &table, &expected_fields, 1, 1, io),
            barrier_counter
        );
    }
    authority.session.begin(io).unwrap();
    command(
        authority.session.connection().unwrap(),
        &format!("UPDATE ONLY {table} SET copy_snapshot_token = pg_catalog.repeat('é', 512)"),
        "UPDATE 1",
        io,
    )
    .unwrap();
    assert_eq!(
        authority
            .read_row(io)
            .unwrap()
            .anchor
            .unwrap()
            .snapshot_token,
        "é".repeat(512)
    );
    authority.session.rollback(io).unwrap();
    for (rejected, alteration, column, projected, expected_error) in [
        (
            "oversized_token",
            "copy_snapshot_token = pg_catalog.repeat('x', 1025), copy_consistent_point = NULL, copy_plan_nonce = NULL",
            19,
            b"".as_slice(),
            "target-metadata: partial snapshot anchor",
        ),
        (
            "oversized_utf8_token",
            "copy_snapshot_token = pg_catalog.repeat('é', 513), copy_consistent_point = NULL, copy_plan_nonce = NULL",
            19,
            b"".as_slice(),
            "target-metadata: partial snapshot anchor",
        ),
        (
            "oversized_nonce",
            "copy_snapshot_token = NULL, copy_consistent_point = NULL, copy_plan_nonce = pg_catalog.decode(pg_catalog.repeat('ab', 4096), 'hex')",
            21,
            b"".as_slice(),
            "target-metadata: partial snapshot anchor",
        ),
        (
            "zero_point",
            "copy_consistent_point = '0/0'::pg_catalog.pg_lsn",
            20,
            b"0/0".as_slice(),
            "target-metadata: invalid snapshot anchor",
        ),
        (
            "zero_nonce",
            "copy_plan_nonce = pg_catalog.decode(pg_catalog.repeat('00', 16), 'hex')",
            21,
            b"00000000000000000000000000000000".as_slice(),
            "target-metadata: invalid snapshot anchor",
        ),
        (
            "empty_token",
            "copy_snapshot_token = ''",
            19,
            b"".as_slice(),
            "target-metadata: invalid snapshot anchor",
        ),
    ] {
        authority.session.begin(io).unwrap();
        command(
            authority.session.connection().unwrap(),
            &format!("UPDATE ONLY {table} SET {alteration}"),
            "UPDATE 1",
            io,
        )
        .unwrap();
        // Inspect the prepared projection so invalid presence cannot disappear as SQL NULL.
        let projection = authority
            .session
            .connection()
            .unwrap()
            .execute_prepared("rpgmigrate_metadata_read", &[], io)
            .unwrap();
        assert_eq!(projection.rows(), 1);
        assert_eq!(
            projection.cell(0, column).unwrap(),
            Some(projected),
            "{rejected}: invalid present field became NULL or escaped its bounded projection"
        );
        let error = authority.read_row(io).unwrap_err();
        assert_eq!(error.message, expected_error, "{rejected}");
        assert_eq!(
            authority
                .session
                .connection()
                .unwrap()
                .transaction_status()
                .unwrap(),
            TransactionStatus::InTransaction
        );
        authority.session.rollback(io).unwrap();
        assert_eq!(
            assert_snapshot_anchor_storage(observer, &table, &expected_fields, 1, 1, io),
            barrier_counter
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
    let marker = format!("{schema}.copy_done");
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
    admin
        .query(
            &format!("GRANT SELECT, INSERT, DELETE ON {marker} TO e3_restricted"),
            &io,
        )
        .unwrap();
    for (relation, privileges) in [
        (&table, ["SELECT", "INSERT", "UPDATE"]),
        (&marker, ["SELECT", "INSERT", "DELETE"]),
    ] {
        for privilege in privileges {
            admin
                .query(
                    &format!("REVOKE {privilege} ON {relation} FROM e3_restricted"),
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
                    &format!("GRANT {privilege} ON {relation} TO e3_restricted"),
                    &io,
                )
                .unwrap();
        }
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
        let marker = format!("{schema}.copy_done");
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
        admin
            .query(&format!("GRANT SELECT, INSERT, DELETE ON {marker} TO {role}"), &io)
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
                pg_catalog.has_table_privilege({table_oid}::oid, 'UPDATE'),
                pg_catalog.has_table_privilege({marker_oid}::oid, 'SELECT'),
                pg_catalog.has_table_privilege({marker_oid}::oid, 'INSERT'),
                pg_catalog.has_table_privilege({marker_oid}::oid, 'DELETE'),
                NOT pg_catalog.has_table_privilege({marker_oid}::oid, 'UPDATE'),
                NOT pg_catalog.has_table_privilege({marker_oid}::oid, 'TRUNCATE'),
                NOT pg_catalog.has_table_privilege({marker_oid}::oid, 'REFERENCES'),
                NOT pg_catalog.has_table_privilege({marker_oid}::oid, 'TRIGGER')
             FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER",
            owner = provenance.intent.binding.owner_oid,
            schema_oid = provenance.schema_oid,
            table_oid = provenance.table_oid,
            marker_oid = provenance.marker_table_oid,
        ), &io).unwrap();
        assert_eq!(privileges.rows(), 1);
        assert_eq!(privileges.cell(0, 0).unwrap(), Some(writer.as_bytes()));
        for column in 1..16 {
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
        "deallocate_empty",
        "deallocate_unknown",
        "deallocate_unowned_all",
        "deallocate_reserved",
        "deallocate_nul",
        "deallocate_long",
        "deallocate_missing_backend",
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
        let result = match rejected {
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
            "deallocate_empty" => tx.deallocate("", &io),
            "deallocate_unknown" => tx.deallocate("unknown", &io),
            "deallocate_unowned_all" => tx.deallocate("ALL", &io),
            "deallocate_reserved" => tx.deallocate("rpgmigrate_authority_guard", &io),
            "deallocate_nul" => tx.deallocate("bad\0name", &io),
            "deallocate_long" => tx.deallocate(&"x".repeat(64), &io),
            "deallocate_missing_backend" => {
                command(
                    tx.authority.session.connection().unwrap(),
                    "DEALLOCATE \"poison_insert\"",
                    "DEALLOCATE",
                    &io,
                )
                .unwrap();
                tx.deallocate("poison_insert", &io)
            }
            _ => unreachable!(),
        };
        assert!(result.is_err(), "{rejected}");
        assert!(tx.authority.prepared.contains("poison_insert"));
        assert_eq!(
            tx.authority
                .session
                .connection()
                .unwrap()
                .transaction_status()
                .unwrap(),
            if rejected == "deallocate_missing_backend" {
                TransactionStatus::InError
            } else {
                TransactionStatus::InTransaction
            }
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

    // Release the preceding case's session fence before acquiring fresh owners.
    drop(authority);
    command(
        &mut observer,
        "CREATE TABLE public.authority_preflight_bad(id integer PRIMARY KEY)",
        "CREATE TABLE",
        &io,
    )
    .unwrap();
    let mut admin = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
    command(
        &mut admin,
        "CREATE TABLE public.authority_preflight_denied(id integer PRIMARY KEY)",
        "CREATE TABLE",
        &io,
    )
    .unwrap();
    command(
        &mut admin,
        "GRANT SELECT ON public.authority_preflight_denied TO e1_target",
        "GRANT",
        &io,
    )
    .unwrap();
    for rejected in [
        "local_event",
        "layout_mismatch",
        "server_lock",
        "fence_lost",
    ] {
        let mut native = state();
        let stream = stream_for(&native);
        let mut authority =
            TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
        let mut tx = authority.begin(frontier(stream, 1), &io).unwrap();
        tx.prepare(
            "preflight_poison_insert",
            "INSERT INTO public.authority_poison VALUES ($1)",
            &[25],
            &io,
        )
        .unwrap();
        tx.execute_prepared(
            "preflight_poison_insert",
            &[PgParam::Text(b"must roll back")],
            &io,
        )
        .unwrap();
        let relation = RelationKey {
            oid: 1,
            generation: 17,
        };
        let expected = if rejected == "local_event" {
            Event::Insert {
                relation,
                values: Vec::new(),
            }
        } else {
            Event::Relation {
                key: relation,
                schema: "public".into(),
                name: if rejected == "server_lock" {
                    "authority_preflight_denied"
                } else {
                    "authority_preflight_bad"
                }
                .into(),
                columns: vec![Column {
                    name: "id".into(),
                    type_oid: if rejected == "layout_mismatch" {
                        20
                    } else {
                        23
                    },
                    type_mod: -1,
                    key: true,
                }],
                replica_identity: b'd',
            }
        };
        if rejected == "fence_lost" {
            tx.authority
                .session
                .connection()
                .unwrap()
                .query("SELECT pg_catalog.pg_advisory_unlock_all()", &io)
                .unwrap();
        }
        let error = tx.preflight_relation(&expected, &io).unwrap_err();
        let status = tx
            .authority
            .session
            .connection()
            .unwrap()
            .transaction_status()
            .unwrap();
        let commit_error = tx
            .commit(&io)
            .expect_err("a caught preflight error allowed COMMIT");
        assert_eq!(
            commit_error.message, "target-authority: transaction poisoned by an earlier DML error",
            "{rejected}"
        );
        match rejected {
            "local_event" => assert_eq!(
                error.message,
                "CDC target preflight requires relation metadata"
            ),
            "layout_mismatch" => assert_eq!(
                error.message,
                "CDC target relation metadata does not match the source relation"
            ),
            "server_lock" => assert!(error.message.contains("SQLSTATE 42501"), "{error:?}"),
            "fence_lost" => assert_eq!(
                error.message,
                "target-authority: backend, primary, durability settings, or writer fence changed"
            ),
            _ => unreachable!(),
        }
        assert_eq!(
            status,
            if rejected == "server_lock" {
                TransactionStatus::InError
            } else {
                TransactionStatus::InTransaction
            },
            "{rejected}"
        );
        assert_eq!(authority.reacquire(connection(&io), &io).unwrap(), None);
        let rows = observer
            .query("SELECT count(*) FROM ONLY public.authority_poison", &io)
            .unwrap();
        assert_eq!(rows.rows(), 1);
        assert_eq!(
            rows.cell(0, 0).unwrap(),
            Some(b"0".as_slice()),
            "{rejected}"
        );
    }
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
    let first = Frontier {
        commit: None,
        ..frontier(stream, 1)
    };
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

fn install_test_follow(authority: &mut TargetAuthority, io: &IoControl) -> FollowBinding {
    let stream = authority.provenance.intent.binding.stream;
    let slot = crate::state::SourceSlotReceipt {
        intent: crate::state::SourceSlotIntent {
            binding: crate::state::SourceSlotBinding::new(
                stream,
                "rpgmigrate_follow_authority".into(),
                crate::cdc::contracts::Plugin::Pgoutput,
                1,
            )
            .unwrap(),
            nonce: [2; 16],
        },
        consistent_point: SourceEndLsn(10),
        snapshot_token: "test-snapshot".into(),
    };
    let binding = FollowBinding {
        slot,
        publication: crate::contracts::PublicationReceipt {
            name: "rpgmigrate_follow_authority".into(),
            oid: 42,
            owner_oid: 43,
        },
        source_timeline: 1,
        copy_generation: crate::contracts::CopyGeneration(1),
        source_types: Vec::new(),
    };
    authority.session.begin(io).unwrap();
    let mut next = authority.row.clone();
    next.follow_binding = Some(crate::state::FollowControl {
        binding: binding.clone(),
        operator: None,
        origin_name: None,
    });
    next.anchor = Some(SnapshotAnchor {
        stream,
        snapshot_token: binding.slot.snapshot_token.clone(),
        consistent_point: binding.slot.consistent_point,
        plan_nonce: binding.slot.intent.nonce,
    });
    let epoch = next.barrier_epoch;
    next = increment_barrier(next, epoch).unwrap();
    authority.write_row(&next, io).unwrap();
    let CommitReceipt::Acknowledged(Some(proof)) = authority
        .finish(next, TargetCommit::Synchronous, io)
        .unwrap()
    else {
        panic!("follow test anchor has no positive commit")
    };
    assert!(proof.feedback().is_none());
    binding
}

fn follow_frontier(stream: StreamId, order: u64, end: u64) -> Frontier {
    Frontier {
        tx: TxId {
            stream,
            xid: order as u32,
            begin_ordinal: end - 1,
        },
        order: TxSeq(order),
        commit: Some(CommitLsn(end - 1)),
        end: SourceEndLsn(end),
    }
}

struct IdentityRewriteFixture {
    _native: NativeState,
    authority: TargetAuthority,
    observer: PgConnection,
    table: String,
    committed: ControlRow,
    stream: StreamId,
}

fn identity_rewrite_fixture(case: &str, replica: bool, io: &IoControl) -> IdentityRewriteFixture {
    let mut native = state();
    let stream = stream_for(&native);
    let mut observer = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io);
    let table = format!("public.authority_identity_{case}");
    command(
        &mut observer,
        &format!(
            "CREATE TABLE {table} (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, \
             immediate_key integer UNIQUE, deferred_key integer UNIQUE DEFERRABLE INITIALLY DEFERRED, \
             body text NOT NULL)"
        ),
        "CREATE TABLE",
        io,
    )
    .unwrap();
    command(
        &mut observer,
        &format!(
            "INSERT INTO {table} OVERRIDING SYSTEM VALUE VALUES \
             (1,10,100,'original'),(2,20,200,'other')"
        ),
        "INSERT 0 2",
        io,
    )
    .unwrap();
    let mut target = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io);
    if replica {
        command(
            &mut target,
            "SET session_replication_role=replica",
            "SET",
            io,
        )
        .unwrap();
    }
    let mut authority = TargetAuthority::acquire(target, stream, &mut native, io).unwrap();
    install_test_follow(&mut authority, io);
    let mut tx = authority.begin_group(io).unwrap();
    tx.prepare(
        "identity_sentinel",
        &format!("UPDATE ONLY {table} SET body='uncommitted' WHERE id=2"),
        &[],
        io,
    )
    .unwrap();
    tx.prepare(
        "identity_rewrite",
        &format!(
            "WITH rpgmigrate_old AS (DELETE FROM ONLY {table} \
             WHERE ctid=(SELECT ctid FROM ONLY {table} \
             WHERE id IS NOT DISTINCT FROM $5 LIMIT 1 FOR UPDATE) \
             RETURNING id,immediate_key,deferred_key,body) \
             INSERT INTO {table} (id,immediate_key,deferred_key,body) \
             OVERRIDING SYSTEM VALUE SELECT $1,$2,$3,$4 FROM rpgmigrate_old"
        ),
        &[20, 23, 23, 25, 20],
        io,
    )
    .unwrap();
    tx.advance(follow_frontier(stream, 1, 100)).unwrap();
    let CommitOutcome::Acknowledged {
        durable: Some(proof),
        ..
    } = tx.commit(io).unwrap()
    else {
        panic!("identity fixture has no positive durable COMMIT")
    };
    assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(100));
    let committed = authority.row.clone();
    IdentityRewriteFixture {
        _native: native,
        authority,
        observer,
        table,
        committed,
        stream,
    }
}

fn require_identity_rewrite_result(result: PgResult) -> Result<()> {
    if result.rows() != 0 || result.command_tag()? != "INSERT 0 1" {
        return Err(refusal("identity rewrite did not replace exactly one row"));
    }
    Ok(())
}

fn assert_identity_rewrite_rolled_back(fixture: &mut IdentityRewriteFixture, io: &IoControl) {
    assert!(matches!(fixture.authority.session.phase, Phase::Poisoned));
    assert_eq!(fixture.authority.row, fixture.committed);
    let checkpoint = fixture
        .authority
        .reacquire(named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io), io)
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.applied, follow_frontier(fixture.stream, 1, 100));
    assert_eq!(fixture.authority.row, fixture.committed);
    let rows = fixture
        .observer
        .query(
            &format!(
                "SELECT id,immediate_key,deferred_key,body FROM ONLY {} ORDER BY id",
                fixture.table
            ),
            io,
        )
        .unwrap();
    assert_eq!(rows.rows(), 2);
    for (index, expected) in [["1", "10", "100", "original"], ["2", "20", "200", "other"]]
        .iter()
        .enumerate()
    {
        for (column, value) in expected.iter().enumerate() {
            assert_eq!(rows.cell(index, column).unwrap(), Some(value.as_bytes()));
        }
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_identity_rewrite_commits_data_and_checkpoint() {
    let io = control();
    let mut fixture = identity_rewrite_fixture("commit", true, &io);
    let checkpoint_table = qualified(&mut fixture.observer, &fixture.authority.provenance);
    let next = follow_frontier(fixture.stream, 2, 200);
    let mut tx = fixture.authority.begin_group(&io).unwrap();
    tx.execute_prepared("identity_sentinel", &[], &io).unwrap();
    let result = tx
        .execute_prepared(
            "identity_rewrite",
            &[
                PgParam::Text(b"3"),
                PgParam::Text(b"30"),
                PgParam::Text(b"300"),
                PgParam::Text(b"replacement"),
                PgParam::Text(b"1"),
            ],
            &io,
        )
        .unwrap();
    require_identity_rewrite_result(result).unwrap();
    tx.advance(next).unwrap();
    let before = fixture
        .observer
        .query(
            &format!(
                "SELECT applied_order,(SELECT body FROM ONLY {} WHERE id=1) \
                 FROM ONLY {checkpoint_table}",
                fixture.table
            ),
            &io,
        )
        .unwrap();
    assert_eq!(before.cell(0, 0).unwrap(), Some(b"1".as_slice()));
    assert_eq!(before.cell(0, 1).unwrap(), Some(b"original".as_slice()));
    let CommitOutcome::Acknowledged {
        checkpoint,
        durable: Some(proof),
    } = tx.commit(&io).unwrap()
    else {
        panic!("identity rewrite has no positive durable COMMIT")
    };
    assert_eq!(checkpoint.applied, next);
    assert_eq!(
        checkpoint.barrier_counter,
        fixture.committed.barrier_counter + 1
    );
    assert_eq!(proof.frontier(), Some(&next));
    assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(200));
    let rows = fixture
        .observer
        .query(
            &format!(
                "SELECT applied_order,source_end_lsn, \
                 (SELECT count(*)=2 AND bool_and( \
                  (id=3 AND immediate_key=30 AND deferred_key=300 AND body='replacement') \
                  OR (id=2 AND immediate_key=20 AND deferred_key=200 AND body='uncommitted')) \
                  FROM ONLY {}) \
                 FROM ONLY {checkpoint_table}",
                fixture.table
            ),
            &io,
        )
        .unwrap();
    assert_eq!(rows.rows(), 1);
    assert_eq!(rows.cell(0, 0).unwrap(), Some(b"2".as_slice()));
    assert_eq!(rows.cell(0, 1).unwrap(), Some(b"0/C8".as_slice()));
    assert_eq!(rows.cell(0, 2).unwrap(), Some(b"t".as_slice()));
    assert_eq!(fixture.authority.checkpoint(&io).unwrap(), Some(checkpoint));
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_identity_rewrite_constraint_failure_rolls_back() {
    let io = control();
    for (case, immediate, body, expected) in [
        (
            "unique",
            b"20".as_slice(),
            PgParam::Text(b"replacement"),
            "23505",
        ),
        ("not_null", b"30".as_slice(), PgParam::Null, "23502"),
    ] {
        let mut fixture = identity_rewrite_fixture(case, true, &io);
        let mut tx = fixture.authority.begin_group(&io).unwrap();
        tx.execute_prepared("identity_sentinel", &[], &io).unwrap();
        tx.advance(follow_frontier(fixture.stream, 2, 200)).unwrap();
        let result = tx.execute_prepared(
            "identity_rewrite",
            &[
                PgParam::Text(b"3"),
                PgParam::Text(immediate),
                PgParam::Text(b"300"),
                body,
                PgParam::Text(b"1"),
            ],
            &io,
        );
        let Err(error) = result else {
            panic!("identity rewrite accepted {case}")
        };
        assert!(error.message.contains(expected), "{error:?}");
        assert!(tx.commit(&io).is_err());
        assert_identity_rewrite_rolled_back(&mut fixture, &io);
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn identity_rewrite_origin_deferred_constraint_failure_rolls_back() {
    let io = control();
    let mut fixture = identity_rewrite_fixture("deferred_origin", false, &io);
    let mut tx = fixture.authority.begin_group(&io).unwrap();
    tx.execute_prepared("identity_sentinel", &[], &io).unwrap();
    tx.advance(follow_frontier(fixture.stream, 2, 200)).unwrap();
    require_identity_rewrite_result(
        tx.execute_prepared(
            "identity_rewrite",
            &[
                PgParam::Text(b"3"),
                PgParam::Text(b"30"),
                PgParam::Text(b"200"),
                PgParam::Text(b"replacement"),
                PgParam::Text(b"1"),
            ],
            &io,
        )
        .unwrap(),
    )
    .unwrap();
    let error = tx.commit(&io).unwrap_err();
    assert!(error.message.contains("23505"), "{error:?}");
    assert_identity_rewrite_rolled_back(&mut fixture, &io);
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_identity_rewrite_missing_row_and_wrong_tag_roll_back() {
    let io = control();
    for (case, old, expected_tag) in [
        ("missing", b"99".as_slice(), "INSERT 0 0"),
        ("wrong_tag", b"1".as_slice(), "UPDATE 1"),
    ] {
        let mut fixture = identity_rewrite_fixture(case, true, &io);
        let mut tx = fixture.authority.begin_group(&io).unwrap();
        tx.execute_prepared("identity_sentinel", &[], &io).unwrap();
        tx.advance(follow_frontier(fixture.stream, 2, 200)).unwrap();
        let result = tx.pipeline(
            &io,
            |pipeline| {
                if case == "wrong_tag" {
                    pipeline.execute_prepared("identity_sentinel", &[], &io)
                } else {
                    pipeline.execute_prepared(
                        "identity_rewrite",
                        &[
                            PgParam::Text(b"3"),
                            PgParam::Text(b"30"),
                            PgParam::Text(b"300"),
                            PgParam::Text(b"replacement"),
                            PgParam::Text(old),
                        ],
                        &io,
                    )
                }
            },
            |index, result| {
                assert_eq!(index, 0);
                assert_eq!(result.command_tag()?, expected_tag);
                require_identity_rewrite_result(result)
            },
        );
        assert!(result.is_err());
        assert!(tx.commit(&io).is_err());
        assert_identity_rewrite_rolled_back(&mut fixture, &io);
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_identity_rewrite_cas_failure_rolls_back() {
    let io = control();
    let mut fixture = identity_rewrite_fixture("cas", true, &io);
    let checkpoint_table = qualified(&mut fixture.observer, &fixture.authority.provenance);
    let mut tx = fixture.authority.begin_group(&io).unwrap();
    tx.execute_prepared("identity_sentinel", &[], &io).unwrap();
    require_identity_rewrite_result(
        tx.execute_prepared(
            "identity_rewrite",
            &[
                PgParam::Text(b"3"),
                PgParam::Text(b"30"),
                PgParam::Text(b"300"),
                PgParam::Text(b"replacement"),
                PgParam::Text(b"1"),
            ],
            &io,
        )
        .unwrap(),
    )
    .unwrap();
    tx.advance(follow_frontier(fixture.stream, 2, 200)).unwrap();
    let result = tx.commit_pipeline(
        &io,
        |pipeline| {
            pipeline.query(
                &format!("UPDATE ONLY {checkpoint_table} SET barrier_counter=barrier_counter+1"),
                &io,
            )
        },
        |_, _| Ok(()),
    );
    assert!(result.is_err());
    assert_identity_rewrite_rolled_back(&mut fixture, &io);
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_identity_rewrite_transaction_escape_rolls_back() {
    let io = control();
    for (case, sql) in [
        ("cte_commit", "WITH x AS (COMMIT) SELECT 1"),
        ("parent_commit", "WITH x AS (SELECT 1) COMMIT"),
        ("second_commit", "WITH x AS (SELECT 1) SELECT 1; COMMIT"),
    ] {
        let mut fixture = identity_rewrite_fixture(case, true, &io);
        let mut tx = fixture.authority.begin_group(&io).unwrap();
        tx.execute_prepared("identity_sentinel", &[], &io).unwrap();
        tx.advance(follow_frontier(fixture.stream, 2, 200)).unwrap();
        let error = tx.prepare("identity_escape", sql, &[], &io).unwrap_err();
        assert!(error.message.contains("42601"), "{error:?}");
        assert!(tx.failed);
        assert!(!tx.authority.prepared.contains("identity_escape"));
        assert_eq!(
            tx.authority
                .session
                .connection()
                .unwrap()
                .transaction_status()
                .unwrap(),
            TransactionStatus::InError
        );
        assert!(tx.execute_prepared("identity_sentinel", &[], &io).is_err());
        assert!(tx.commit(&io).is_err());
        assert_identity_rewrite_rolled_back(&mut fixture, &io);
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_streamed_group_is_atomic_and_checkpointed_once() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut observer = connection(&io);
    command(
        &mut observer,
        "CREATE TABLE public.authority_follow_group(value integer)",
        "CREATE TABLE",
        &io,
    )
    .unwrap();
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    install_test_follow(&mut authority, &io);
    let previous_counter = authority.row.barrier_counter;
    let mut setup = authority.begin_group(&io).unwrap();
    setup
        .prepare(
            "follow_group_insert",
            "INSERT INTO public.authority_follow_group VALUES ($1)",
            &[23],
            &io,
        )
        .unwrap();
    setup.rollback(&io).unwrap();
    let mut tx = authority.begin_group(&io).unwrap();
    assert!(matches!(tx.authority.session.phase, Phase::BeginPending));
    tx.pipeline(
        &io,
        |pipeline| pipeline.execute_prepared("follow_group_insert", &[PgParam::Text(b"1")], &io),
        |index, result| {
            assert_eq!(index, 0);
            assert_eq!(result.command_tag()?, "INSERT 0 1");
            Ok(())
        },
    )
    .unwrap();
    tx.advance(follow_frontier(stream, 1, 100)).unwrap();
    assert_eq!(
        observer
            .query("SELECT count(*) FROM public.authority_follow_group", &io)
            .unwrap()
            .cell(0, 0)
            .unwrap(),
        Some(b"0".as_slice())
    );
    tx.advance(follow_frontier(stream, 2, 200)).unwrap();
    let CommitOutcome::Acknowledged {
        checkpoint,
        durable: Some(proof),
    } = tx
        .commit_pipeline(
            &io,
            |pipeline| {
                pipeline.execute_prepared("follow_group_insert", &[PgParam::Text(b"2")], &io)
            },
            |index, result| {
                assert_eq!(index, 0);
                assert_eq!(result.command_tag()?, "INSERT 0 1");
                Ok(())
            },
        )
        .unwrap()
    else {
        panic!("follow group has no positive durable COMMIT")
    };
    assert_eq!(checkpoint.applied, follow_frontier(stream, 2, 200));
    assert_eq!(checkpoint.barrier_counter, previous_counter + 1);
    assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(200));
    assert_eq!(
        observer
            .query("SELECT count(*) FROM public.authority_follow_group", &io)
            .unwrap()
            .cell(0, 0)
            .unwrap(),
        Some(b"2".as_slice())
    );

    for (order, end, pipelined) in [(3, 300, false), (4, 400, true)] {
        let frontier = follow_frontier(stream, order, end);
        let mut expected = authority.row.clone();
        expected.applied = Some(frontier);
        expected.barrier_counter += 1;
        let mut tx = authority.begin_group(&io).unwrap();
        assert!(matches!(tx.authority.session.phase, Phase::BeginPending));
        assert_eq!(
            tx.authority
                .session
                .connection()
                .unwrap()
                .transaction_status()
                .unwrap(),
            TransactionStatus::Idle
        );
        tx.advance(frontier).unwrap();
        assert!(matches!(tx.authority.session.phase, Phase::BeginPending));
        assert_eq!(
            tx.authority
                .session
                .connection()
                .unwrap()
                .transaction_status()
                .unwrap(),
            TransactionStatus::Idle
        );
        let outcome = if pipelined {
            tx.commit_pipeline(
                &io,
                |_| Ok(()),
                |_, _| panic!("checkpoint-only group returned a data result"),
            )
        } else {
            tx.commit(&io)
        };
        let CommitOutcome::Acknowledged {
            checkpoint,
            durable: Some(proof),
        } = outcome.unwrap()
        else {
            panic!("checkpoint-only group has no positive durable COMMIT")
        };
        assert_eq!(checkpoint.applied, frontier);
        assert_eq!(checkpoint.barrier_counter, expected.barrier_counter);
        assert_eq!(proof.frontier(), Some(&frontier));
        assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(end));
        assert_eq!(authority.row, expected);
        assert!(matches!(authority.session.phase, Phase::Idle));
        assert_eq!(
            authority
                .session
                .connection()
                .unwrap()
                .transaction_status()
                .unwrap(),
            TransactionStatus::Idle
        );
        assert_eq!(
            metadata::read_only(&mut observer, &authority.provenance, &io).unwrap(),
            expected
        );
        let rows = observer
            .query(
                "SELECT value FROM public.authority_follow_group ORDER BY value",
                &io,
            )
            .unwrap();
        assert_eq!(rows.rows(), 2);
        assert_eq!(rows.cell(0, 0).unwrap(), Some(b"1".as_slice()));
        assert_eq!(rows.cell(1, 0).unwrap(), Some(b"2".as_slice()));
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_cas_failure_rolls_back_dml_and_feedback() {
    let io = control();
    let mut observer = connection(&io);
    command(
        &mut observer,
        "CREATE TABLE public.authority_follow_cas(value integer)",
        "CREATE TABLE",
        &io,
    )
    .unwrap();
    for commit_escape in [false, true] {
        let mut native = state();
        let stream = stream_for(&native);
        let mut authority =
            TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
        install_test_follow(&mut authority, &io);
        let table = qualified(&mut observer, &authority.provenance);
        let cached = authority.row.clone();
        let mut tx = authority.begin_group(&io).unwrap();
        tx.advance(follow_frontier(stream, 1, 100)).unwrap();
        let sabotage = if commit_escape {
            "COMMIT".into()
        } else {
            format!("UPDATE ONLY {table} SET barrier_counter = barrier_counter + 1")
        };
        let result = tx.commit_pipeline(
            &io,
            |pipeline| {
                pipeline.query("INSERT INTO public.authority_follow_cas VALUES (1)", &io)?;
                pipeline.query(&sabotage, &io)
            },
            |_, _| Ok(()),
        );
        assert!(result.is_err());
        assert!(matches!(authority.session.phase, Phase::Poisoned));
        assert_eq!(authority.row, cached);
        assert_eq!(
            observer
                .query("SELECT count(*) FROM public.authority_follow_cas", &io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"0".as_slice())
        );
        let row = observer
            .query(
                &format!("SELECT applied_order,barrier_counter FROM {table}"),
                &io,
            )
            .unwrap();
        assert_eq!(row.cell(0, 0).unwrap(), Some(b"0".as_slice()));
        assert_eq!(
            row.cell(0, 1).unwrap(),
            Some(cached.barrier_counter.to_string().as_bytes())
        );
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_certification_and_recovery_refuse_a_source_gap() {
    let io = control();
    let mut native = state();
    let stream = stream_for(&native);
    let mut authority =
        TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
    let binding = install_test_follow(&mut authority, &io);
    let proof = authority.certify(SourceEndLsn(200), &io).unwrap();
    assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(200));
    assert!(authority.row.applied.is_none());
    let start = authority
        .recover_follow(&binding, SourceEndLsn(200), &io)
        .unwrap();
    assert_eq!(start.start_lsn(), SourceEndLsn(200));
    let error = authority
        .recover_follow(&binding, SourceEndLsn(201), &io)
        .unwrap_err();
    assert_eq!(error.exit_code(), 3);
    assert!(matches!(authority.session.phase, Phase::Poisoned));
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness with exclusive SyncRep settings"]
fn follow_cancelled_sync_wait_never_proves_group_or_certification() {
    let io = control();
    let context_path = PathBuf::from(std::env::var_os("RPGMIGRATE_TEST_CONTEXT").unwrap());
    let context: serde_json::Value =
        serde_json::from_slice(&std::fs::read(context_path).unwrap()).unwrap();
    let token = context["token"].as_str().unwrap();
    let root = PathBuf::from(context["root"].as_str().unwrap());
    assert_eq!(
        std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
        token
    );
    let admin_info = Conninfo::new(
        std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_ADMIN").expect("owned admin required"),
    )
    .unwrap();
    let mut admin = PgConnection::connect(&admin_info, &io).unwrap();
    let identity = admin
        .query("SELECT pg_catalog.current_setting('cluster_name')", &io)
        .unwrap();
    assert_eq!(identity.cell(0, 0).unwrap(), Some(token.as_bytes()));
    command(&mut admin, "SET synchronous_commit=off", "SET", &io).unwrap();
    let mut setup = connection(&io);
    command(
        &mut setup,
        "CREATE TABLE public.authority_follow_cancel(value integer)",
        "CREATE TABLE",
        &io,
    )
    .unwrap();
    let mut protected_native = state();
    let protected_stream = stream_for(&protected_native);
    let mut protected = TargetAuthority::acquire(
        connection(&io),
        protected_stream,
        &mut protected_native,
        &io,
    )
    .unwrap();
    command(
        &mut admin,
        "ALTER SYSTEM SET full_page_writes=off",
        "ALTER SYSTEM",
        &io,
    )
    .unwrap();
    admin
        .query("SELECT pg_catalog.pg_reload_conf()", &io)
        .unwrap();
    let drift_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if admin
            .query("SHOW full_page_writes", &io)
            .unwrap()
            .cell(0, 0)
            .unwrap()
            == Some(b"off".as_slice())
        {
            break;
        }
        assert!(
            Instant::now() < drift_deadline,
            "owned full_page_writes drift was not observed"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut rejected = state();
    let rejected_stream = stream_for(&rejected);
    assert!(
        TargetAuthority::acquire(connection(&io), rejected_stream, &mut rejected, &io).is_err()
    );
    assert!(rejected.target_provenance(&rejected_stream).is_none());
    assert!(protected.checkpoint(&io).is_err());
    assert!(matches!(protected.session.phase, Phase::Poisoned));
    assert!(protected.barrier(None, 0, &io).is_err());
    command(
        &mut admin,
        "ALTER SYSTEM RESET full_page_writes",
        "ALTER SYSTEM",
        &io,
    )
    .unwrap();
    admin
        .query("SELECT pg_catalog.pg_reload_conf()", &io)
        .unwrap();
    let restore_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if admin
            .query("SHOW full_page_writes", &io)
            .unwrap()
            .cell(0, 0)
            .unwrap()
            == Some(b"on".as_slice())
        {
            break;
        }
        assert!(
            Instant::now() < restore_deadline,
            "owned full_page_writes was not restored"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    for certification in [false, true] {
        let mut native = state();
        let stream = stream_for(&native);
        let mut authority =
            TargetAuthority::acquire(connection(&io), stream, &mut native, &io).unwrap();
        let binding = install_test_follow(&mut authority, &io);
        let pid = authority.session.pid;
        let cancellation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            command(
                &mut admin,
                "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (rpgmigrate_missing_standby)'",
                "ALTER SYSTEM",
                &io,
            )
            .unwrap();
            assert_eq!(
                admin
                    .query("SELECT pg_catalog.pg_reload_conf()", &io)
                    .unwrap()
                    .cell(0, 0)
                    .unwrap(),
                Some(b"t".as_slice())
            );
            let mut reload_io = io.clone();
            reload_io.deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if admin
                    .query("SHOW synchronous_standby_names", &reload_io)
                    .unwrap()
                    .cell(0, 0)
                    .unwrap()
                    == Some(b"FIRST 1 (rpgmigrate_missing_standby)".as_slice())
                {
                    break;
                }
                assert!(
                    Instant::now() < reload_io.deadline,
                    "owned synchronous standby policy was not reloaded"
                );
            }
            // The checkpointer publishes shared SyncRep policy before processing checkpoints.
            command(&mut admin, "CHECKPOINT", "CHECKPOINT", &io).unwrap();
            let cancel_info = admin_info.clone();
            let cancel_io = io.clone();
            let cancel = std::thread::spawn(move || {
                let mut observer = PgConnection::connect(&cancel_info, &cancel_io).unwrap();
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    assert!(
                        Instant::now() < deadline,
                        "sync-cancel diagnostic pid={pid} wait=unobserved query=unobserved cancel=false"
                    );
                    let result = observer
                        .query(
                            &format!("SELECT pid,wait_event='SyncRep',query='COMMIT' FROM pg_catalog.pg_stat_activity WHERE pid={pid}"),
                            &cancel_io,
                        )
                        .unwrap();
                    assert_eq!(result.rows(), 1);
                    assert_eq!(number(&result, 0).unwrap(), pid);
                    if result.cell(0, 1).unwrap() == Some(b"t".as_slice())
                        && result.cell(0, 2).unwrap() == Some(b"t".as_slice())
                    {
                        let result = observer
                            .query(
                                &format!("SELECT pg_catalog.pg_cancel_backend({pid})"),
                                &cancel_io,
                            )
                            .unwrap();
                        let submitted = result.cell(0, 0).unwrap() == Some(b"t".as_slice());
                        eprintln!(
                            "sync-cancel diagnostic pid={pid} wait=SyncRep query=COMMIT cancel={submitted}"
                        );
                        assert!(submitted);
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            });
            if certification {
                let outcome = authority.certify(SourceEndLsn(200), &io);
                let variant = if outcome.is_ok() {
                    "Acknowledged"
                } else {
                    "Refused"
                };
                eprintln!("sync-cancel diagnostic phase=certification pid={pid} outcome={variant}");
                cancel
                    .join()
                    .expect("SyncRep cancellation was not observed");
                assert!(outcome.is_err());
                assert!(authority.row.certified_lsn.is_none());
            } else {
                let mut tx = authority.begin_group(&io).unwrap();
                tx.advance(follow_frontier(stream, 1, 100)).unwrap();
                let outcome = tx
                    .commit_pipeline(
                        &io,
                        |pipeline| {
                            pipeline
                                .query("INSERT INTO public.authority_follow_cancel VALUES (1)", &io)
                        },
                        |_, result| {
                            assert_eq!(result.command_tag()?, "INSERT 0 1");
                            Ok(())
                        },
                    )
                    .unwrap();
                let variant = match &outcome {
                    CommitOutcome::Acknowledged { .. } => "Acknowledged",
                    CommitOutcome::Unknown => "Unknown",
                };
                eprintln!("sync-cancel diagnostic phase=group pid={pid} outcome={variant}");
                cancel
                    .join()
                    .expect("SyncRep cancellation was not observed");
                assert!(
                    matches!(outcome, CommitOutcome::Unknown),
                    "outcome={variant}"
                );
                assert!(authority.row.applied.is_none());
            }
            assert!(matches!(authority.session.phase, Phase::Poisoned));
        }));
        command(
            &mut admin,
            "ALTER SYSTEM RESET synchronous_standby_names",
            "ALTER SYSTEM",
            &io,
        )
        .unwrap();
        admin
            .query("SELECT pg_catalog.pg_reload_conf()", &io)
            .unwrap();
        let reset_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let reset = admin.query("SHOW synchronous_standby_names", &io).unwrap();
            if reset.cell(0, 0).unwrap() == Some(b"".as_slice()) {
                break;
            }
            assert!(
                Instant::now() < reset_deadline,
                "owned SyncRep setting did not reset"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        if let Err(panic) = cancellation {
            std::panic::resume_unwind(panic);
        }
        authority.reacquire(connection(&io), &io).unwrap();
        let recovered = authority
            .recover_follow(&binding, SourceEndLsn(10), &io)
            .unwrap();
        assert_eq!(
            recovered.start_lsn(),
            SourceEndLsn(if certification { 200 } else { 100 })
        );
    }
}

fn follow_reset_resources(
    native: &mut NativeState,
    plan: &crate::contracts::ClonePlan,
    point: u64,
) -> FollowBinding {
    native.prepare_follow_source(plan).unwrap();
    let name = format!("rpgmigrate_reset_{}", plan.generation.0);
    let intent = native.begin_publication(&name).unwrap();
    let publication = crate::contracts::PublicationReceipt {
        name: name.clone(),
        oid: point as u32,
        owner_oid: 43,
    };
    native
        .record_publication_created(&intent, &publication)
        .unwrap();
    native.confirm_publication(&intent, &publication).unwrap();
    let stream = StreamId {
        run: native.identity().run,
        source: plan.source.identity,
        generation: 1,
    };
    let slot = native
        .begin_source_slot(
            &crate::state::SourceSlotBinding::new(
                stream,
                name,
                crate::cdc::contracts::Plugin::Pgoutput,
                plan.generation.0,
            )
            .unwrap(),
        )
        .unwrap();
    let receipt = native
        .confirm_source_slot(
            &slot,
            crate::state::CreatedSlotData {
                consistent_point: SourceEndLsn(point),
                snapshot_name: format!("reset-{point}"),
            },
        )
        .unwrap();
    FollowBinding {
        slot: receipt,
        publication,
        source_timeline: 1,
        copy_generation: plan.generation,
        source_types: Vec::new(),
    }
}

// Source receipts are synthetic here; every target ownership receipt comes from actual fixture DDL.
fn follow_reset_fixture(io: &IoControl, empty: bool) -> (NativeState, CloneAuthority, String) {
    let initial = state();
    let path = initial.identity().canonical_path.clone();
    drop(initial);
    let mut native = crate::state::open_follow(
        &StateRequest {
            path,
            action: StateAction::Resume,
        },
        None,
        None,
        io,
    )
    .unwrap();
    let schema = format!(
        "m3_reset_{}",
        native
            .identity()
            .run
            .0
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
    let mut observer = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io);
    let target = crate::catalog::read_clone_target(&mut observer, io).unwrap();
    let source = crate::contracts::CloneSource {
        identity: SourceId {
            system_identifier: 777,
            database_oid: 42,
        },
        timeline: 1,
        major: 16,
    };
    let mut plan = crate::contracts::ClonePlan {
        source: source.clone(),
        target,
        generation: crate::contracts::CopyGeneration(1),
        tables: if empty {
            Vec::new()
        } else {
            ["a", "b"]
                .into_iter()
                .enumerate()
                .map(|(index, name)| crate::contracts::CloneTable {
                    source_oid: 1001 + index as u32,
                    name: crate::contracts::QualifiedTable {
                        schema: schema.clone(),
                        table: name.into(),
                    },
                    writable_columns: vec!["id".into()],
                    all_columns: vec!["id".into()],
                    identity_modes: vec![0],
                    partition_identity_modes: None,
                })
                .collect()
        },
        sequences: Vec::new(),
        policy: crate::contracts::RestorePolicy::default(),
        copy_policy: crate::contracts::CloneCopyPolicy {
            skip_large_objects: true,
            ..Default::default()
        },
        copy_tables: Vec::new(),
    };
    plan.copy_tables = test_copy_profile(&plan.tables);
    let binding = follow_reset_resources(&mut native, &plan, 10);
    native
        .bind_follow(
            &plan,
            &crate::contracts::SnapshotTicket {
                token: binding.slot.snapshot_token.clone(),
                source: source.clone(),
                generation: plan.generation,
            },
            &binding,
        )
        .unwrap();
    let mut authority = CloneAuthority::acquire(
        named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io),
        &mut native,
        &plan,
        io,
    )
    .unwrap();
    native.begin_pre_data().unwrap();
    authority
        .mark_stage(crate::state::ClonePhase::PreDataPending, io)
        .unwrap();
    let before = crate::catalog::clone_object_inventory(&mut observer, io).unwrap();
    if !empty {
        observer
            .query(&format!("CREATE SCHEMA {schema}"), io)
            .unwrap();
        observer
            .query(
                &format!("CREATE TABLE {schema}.a(id integer PRIMARY KEY)"),
                io,
            )
            .unwrap();
        observer
            .query(&format!("CREATE TABLE {schema}.b(id integer)"), io)
            .unwrap();
    }
    let objects = crate::catalog::clone_object_inventory(&mut observer, io)
        .unwrap()
        .into_iter()
        .filter(|object| !before.contains(object))
        .collect::<Vec<_>>();
    let tables =
        crate::catalog::reconcile_clone_tables(&mut observer, &plan, &objects, io).unwrap();
    native
        .confirm_pre_data(&plan, objects, tables.clone())
        .unwrap();
    authority.bind_tables(&native, io).unwrap();
    for table in &tables {
        let mut tx = CopyTxn::begin(
            named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io),
            authority.permit(table, &single_part()).unwrap(),
            io,
        )
        .unwrap();
        tx.connection_mut()
            .unwrap()
            .start_copy_in(
                &format!(
                    "COPY {schema}.{} FROM STDIN WITH(FORMAT text,FREEZE true)",
                    table.table.name.table
                ),
                io,
            )
            .unwrap();
        tx.connection_mut().unwrap().write_copy(b"1\n", io).unwrap();
        assert_eq!(tx.connection_mut().unwrap().finish_copy_in(io).unwrap(), 1);
        assert!(matches!(
            tx.mark_and_commit(1, 2, io).unwrap(),
            CopyCommitOutcome::Durable
        ));
    }
    assert_eq!(
        authority.copy_report(io).unwrap().tables,
        tables.len() as u64
    );
    let restart = authority
        .begin_follow_restart(&mut native, &plan, io)
        .unwrap();
    native.confirm_follow_slot_absent(&restart).unwrap();
    native.confirm_follow_publication_absent(&restart).unwrap();
    plan.generation = crate::contracts::CopyGeneration(2);
    let binding = follow_reset_resources(&mut native, &plan, 20);
    native
        .bind_follow(
            &plan,
            &crate::contracts::SnapshotTicket {
                token: binding.slot.snapshot_token.clone(),
                source,
                generation: plan.generation,
            },
            &binding,
        )
        .unwrap();
    authority.advance_generation(&native, io).unwrap();
    native.finish_follow_restart().unwrap();
    assert!(
        metadata::read_markers(
            authority.authority.session.connection().unwrap(),
            &authority.authority.provenance,
            io
        )
        .unwrap()
        .is_empty()
    );
    (native, authority, schema)
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_restart_resets_the_complete_workset_and_allows_safe_preworker_retry() {
    let io = control();
    let (native, mut authority, schema) = follow_reset_fixture(&io, false);
    let before = authority.authority.row.barrier_counter;
    let mut observer = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
    for step in 1..=2 {
        authority.truncate_follow_workset(&native, &io).unwrap();
        assert_eq!(authority.authority.row.barrier_counter, before + step);
        let result = observer
            .query(
                &format!(
                    "SELECT (SELECT count(*) FROM {schema}.a), (SELECT count(*) FROM {schema}.b)"
                ),
                &io,
            )
            .unwrap();
        assert_eq!(result.cell(0, 0).unwrap(), Some(b"0".as_slice()));
        assert_eq!(result.cell(0, 1).unwrap(), Some(b"0".as_slice()));
    }
    let (native, mut empty, _) = follow_reset_fixture(&io, true);
    let counter = empty.authority.row.barrier_counter;
    empty.truncate_follow_workset(&native, &io).unwrap();
    assert_eq!(empty.authority.row.barrier_counter, counter + 1);
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_restart_refuses_foreign_replacement_and_fk_without_mutating_rows() {
    let io = control();
    for replacement in [true, false] {
        let (native, mut authority, schema) = follow_reset_fixture(&io, false);
        let mut observer = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
        if replacement {
            observer
                .query(&format!("ALTER TABLE {schema}.a RENAME TO original_a"), &io)
                .unwrap();
            observer
                .query(
                    &format!("CREATE TABLE {schema}.a(id integer PRIMARY KEY)"),
                    &io,
                )
                .unwrap();
            observer
                .query(&format!("INSERT INTO {schema}.a VALUES (91)"), &io)
                .unwrap();
        } else {
            observer
                .query(
                    &format!(
                        "CREATE TABLE {schema}.foreign_refs(id integer REFERENCES {schema}.a(id))"
                    ),
                    &io,
                )
                .unwrap();
            observer
                .query(
                    &format!("INSERT INTO {schema}.foreign_refs VALUES (1)"),
                    &io,
                )
                .unwrap();
        }
        let cached = authority.authority.row.clone();
        assert!(authority.truncate_follow_workset(&native, &io).is_err());
        assert_eq!(authority.authority.row, cached);
        assert!(matches!(authority.authority.session.phase, Phase::Poisoned));
        let result = observer.query(&format!("SELECT (SELECT count(*) FROM {schema}.a), (SELECT count(*) FROM {schema}.b), (SELECT min(id) FROM {schema}.a)"), &io).unwrap();
        assert_eq!(result.cell(0, 0).unwrap(), Some(b"1".as_slice()));
        assert_eq!(result.cell(0, 1).unwrap(), Some(b"1".as_slice()));
        assert_eq!(
            result.cell(0, 2).unwrap(),
            Some(if replacement {
                b"91".as_slice()
            } else {
                b"1".as_slice()
            })
        );
        let foreign = if replacement {
            "original_a"
        } else {
            "foreign_refs"
        };
        assert_eq!(
            observer
                .query(&format!("SELECT id FROM {schema}.{foreign}"), &io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(b"1".as_slice())
        );
        let metadata_name = qualified(&mut observer, &authority.authority.provenance);
        assert_eq!(
            observer
                .query(&format!("SELECT barrier_counter FROM {metadata_name}"), &io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(cached.barrier_counter.to_string().as_bytes())
        );
    }
}

fn cutover_options() -> crate::contracts::CloneOptions {
    crate::contracts::CloneOptions {
        source: Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_SOURCE").unwrap())
            .unwrap(),
        target: Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_ADMIN").unwrap())
            .unwrap(),
        state: StateRequest {
            path: PathBuf::new(),
            action: StateAction::Resume,
        },
        operator: None,
        table_jobs: 1,
        post_data_jobs: 1,
        policy: crate::contracts::RestorePolicy::default(),
        not_consistent: false,
        use_copy_binary: false,
        split_tables_larger_than: 0,
        split_max_parts: 8,
        skip_large_objects: true,
        large_objects_jobs: 1,
    }
}

struct CutoverFixture {
    native: NativeState,
    authority: TargetAuthority,
    plan: ClonePlan,
    control: ControlHandle,
    source: PgConnection,
    sequence: String,
}

fn cutover_fixture(io: &IoControl) -> CutoverFixture {
    let initial = state();
    let path = initial.identity().canonical_path.clone();
    drop(initial);
    let mut native = crate::state::open_follow(
        &StateRequest {
            path,
            action: StateAction::Resume,
        },
        None,
        None,
        io,
    )
    .unwrap();
    let mut source = named_connection("RPGMIGRATE_TEST_AUTHORITY_SOURCE", io);
    let mut target = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io);
    let schema = format!(
        "m4_{}",
        native
            .identity()
            .run
            .0
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    let sequence = format!("{schema}.final_sequence");
    let plan = ClonePlan {
        source: crate::catalog::read_clone_source(&mut source, io).unwrap(),
        target: crate::catalog::read_clone_target(&mut target, io).unwrap(),
        generation: CopyGeneration(1),
        tables: Vec::new(),
        copy_policy: crate::contracts::CloneCopyPolicy {
            skip_large_objects: true,
            ..Default::default()
        },
        copy_tables: Vec::new(),
        sequences: vec![crate::contracts::QualifiedTable {
            schema: schema.clone(),
            table: "final_sequence".into(),
        }],
        policy: crate::contracts::RestorePolicy::default(),
    };
    let binding = follow_reset_resources(&mut native, &plan, 8);
    native
        .bind_follow(
            &plan,
            &crate::contracts::SnapshotTicket {
                token: binding.slot.snapshot_token.clone(),
                source: plan.source.clone(),
                generation: plan.generation,
            },
            &binding,
        )
        .unwrap();
    let mut copy = CloneAuthority::acquire(
        named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io),
        &mut native,
        &plan,
        io,
    )
    .unwrap();
    native.begin_pre_data().unwrap();
    copy.mark_stage(ClonePhase::PreDataPending, io).unwrap();
    let before = crate::catalog::clone_object_inventory(&mut target, io).unwrap();
    for connection in [&mut source, &mut target] {
        command(
            connection,
            &format!("CREATE SCHEMA {schema}"),
            "CREATE SCHEMA",
            io,
        )
        .unwrap();
        command(
            connection,
            &format!("CREATE SEQUENCE {sequence}"),
            "CREATE SEQUENCE",
            io,
        )
        .unwrap();
    }
    target
        .query(
            &format!("SELECT pg_catalog.setval('{sequence}',7,true)"),
            io,
        )
        .unwrap();
    source
        .query(
            &format!("SELECT pg_catalog.setval('{sequence}',42,false)"),
            io,
        )
        .unwrap();
    let objects = crate::catalog::clone_object_inventory(&mut target, io)
        .unwrap()
        .into_iter()
        .filter(|o| !before.contains(o))
        .collect();
    native.confirm_pre_data(&plan, objects, Vec::new()).unwrap();
    copy.bind_tables(&native, io).unwrap();
    for phase in [
        ClonePhase::PostDataPending,
        ClonePhase::PostDataComplete,
        ClonePhase::SequencesPending,
        ClonePhase::SequencesComplete,
        ClonePhase::Analyzing,
    ] {
        native.mark_stage(phase).unwrap();
        copy.mark_stage(phase, io).unwrap();
    }
    copy.install_follow_anchor(&native, &binding.slot, io)
        .unwrap();
    drop(copy);
    let mut authority = TargetAuthority::acquire_follow(
        named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io),
        &mut native,
        &binding,
        io,
    )
    .unwrap();
    let mut tx = authority.begin_group(io).unwrap();
    tx.advance(follow_frontier(binding.stream(), 1, 0x10))
        .unwrap();
    assert!(matches!(
        tx.commit(io).unwrap(),
        CommitOutcome::Acknowledged {
            durable: Some(_),
            ..
        }
    ));
    let control = ControlHandle::for_worker(&native, io).unwrap();
    CutoverFixture {
        native,
        authority,
        plan,
        control,
        source,
        sequence,
    }
}

fn sealed_cutover(fixture: &CutoverFixture, io: &IoControl) -> SealedEndpos {
    let request = fixture
        .control
        .set(SourceEndLsn(0x20), SourceEndLsn(0x10), io)
        .unwrap();
    fixture.control.seal(&request, io).unwrap()
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_endpos_floor_is_unfenced_and_certification_independent() {
    let io = control();
    let mut fixture = cutover_fixture(&io);
    let before = fixture.authority.row.clone();
    let manifest = std::fs::read(
        fixture
            .native
            .identity()
            .canonical_path
            .join("manifest.json"),
    )
    .unwrap();
    let mut observer = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
    assert_eq!(
        read_endpos_floor(&mut observer, &fixture.control, &io).unwrap(),
        SourceEndLsn(0x10)
    );
    assert_eq!(fixture.authority.row, before);
    assert_eq!(
        std::fs::read(
            fixture
                .native
                .identity()
                .canonical_path
                .join("manifest.json")
        )
        .unwrap(),
        manifest
    );
    let request = fixture
        .control
        .set(SourceEndLsn(0x20), SourceEndLsn(0x10), &io)
        .unwrap();
    fixture.authority.certify(SourceEndLsn(0x30), &io).unwrap();
    assert_eq!(
        read_endpos_floor(&mut observer, &fixture.control, &io).unwrap(),
        SourceEndLsn(0x10)
    );
    assert_eq!(
        fixture
            .control
            .set(SourceEndLsn(0x20), SourceEndLsn(0x30), &io)
            .unwrap(),
        request
    );
    let observed = metadata::read_only(&mut observer, &fixture.authority.provenance, &io).unwrap();
    assert_eq!(observed, fixture.authority.row);
    assert_eq!(
        TargetAuthority::try_acquire_follow(
            named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
            &fixture.control,
            &io
        )
        .err()
        .unwrap()
        .kind,
        ErrorKind::BadState
    );
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_command_observation_is_existing_only_and_same_value() {
    let io = control();
    let fixture = cutover_fixture(&io);
    let before = fixture.authority.row.clone();
    let provenance = fixture.authority.provenance.clone();
    let native_bytes = std::fs::read(
        fixture
            .native
            .identity()
            .canonical_path
            .join("manifest.json"),
    )
    .unwrap();
    fixture.authority.close(&io).unwrap();
    let mut command = TargetAuthority::try_acquire_follow(
        named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
        &fixture.control,
        &io,
    )
    .unwrap();
    assert_eq!(command.row, before);
    let snapshot = command.observe_follow(&io).unwrap();
    assert_eq!(snapshot.stream(), provenance.intent.binding.stream);
    assert_eq!(snapshot.durable_replay_lsn(), SourceEndLsn(0x10));
    assert_eq!(snapshot.certified_lsn(), SourceEndLsn(0));
    assert!(!snapshot.follow_completed());
    assert_eq!(snapshot.target(), &provenance);
    assert_eq!(command.row, before);
    command.close(&io).unwrap();
    assert_eq!(
        std::fs::read(
            fixture
                .native
                .identity()
                .canonical_path
                .join("manifest.json")
        )
        .unwrap(),
        native_bytes
    );
    let mut observer = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
    assert_eq!(
        metadata::read_only(&mut observer, &provenance, &io).unwrap(),
        before
    );
    observer
        .query(
            &format!(
                "ALTER TABLE {}.checkpoint SET UNLOGGED",
                provenance.intent.binding.schema_name
            ),
            &io,
        )
        .unwrap();
    assert!(
        TargetAuthority::try_acquire_follow(
            named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
            &fixture.control,
            &io
        )
        .is_err()
    );
    assert_eq!(
        std::fs::read(
            fixture
                .native
                .identity()
                .canonical_path
                .join("manifest.json")
        )
        .unwrap(),
        native_bytes
    );
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_drain_completion_syncs_final_sequences_and_is_immutable() {
    let io = control();
    let mut fixture = cutover_fixture(&io);
    let request = sealed_cutover(&fixture, &io);
    let proof = fixture
        .authority
        .record_drain(&request, DrainWitness::LaterBegin(CommitLsn(0x30)), &io)
        .unwrap();
    assert_eq!(proof.feedback().unwrap().end(), SourceEndLsn(0x10));
    let counter = fixture.authority.row.barrier_counter;
    let mut observer = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
    let before = observer
        .query(
            &format!("SELECT last_value,is_called FROM {}", fixture.sequence),
            &io,
        )
        .unwrap();
    assert_eq!(before.cell(0, 0).unwrap(), Some(b"7".as_slice()));
    assert_eq!(before.cell(0, 1).unwrap(), Some(b"t".as_slice()));
    let mut wrong_source = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
    assert!(
        fixture
            .authority
            .complete_follow(
                &cutover_options(),
                &mut wrong_source,
                &fixture.native,
                &fixture.plan,
                &request,
                &io
            )
            .is_err()
    );
    fixture
        .authority
        .reacquire(
            named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
            &io,
        )
        .unwrap();
    assert!(!fixture.authority.row.follow_completed);
    let completed = fixture
        .authority
        .complete_follow(
            &cutover_options(),
            &mut fixture.source,
            &fixture.native,
            &fixture.plan,
            &request,
            &io,
        )
        .unwrap();
    assert_eq!(completed.end(), SourceEndLsn(0x20));
    assert_eq!(completed.witness(), SourceEndLsn(0x30));
    assert_eq!(completed.kind(), DrainKind::LaterBegin);
    assert_eq!(completed.durable_replay_lsn(), SourceEndLsn(0x10));
    assert_eq!(completed.certified_lsn(), SourceEndLsn(0));
    assert_eq!(fixture.authority.row.barrier_counter, counter + 1);
    let after = observer
        .query(
            &format!("SELECT last_value,is_called FROM {}", fixture.sequence),
            &io,
        )
        .unwrap();
    assert_eq!(after.cell(0, 0).unwrap(), Some(b"42".as_slice()));
    assert_eq!(after.cell(0, 1).unwrap(), Some(b"f".as_slice()));
    let frozen = fixture.authority.row.clone();
    assert!(fixture.authority.certify(SourceEndLsn(0x40), &io).is_err());
    assert!(fixture.authority.begin_group(&io).is_err());
    fixture.native.begin_follow_cleanup().unwrap();
    fixture.native.confirm_follow_slot_removed().unwrap();
    fixture.native.confirm_follow_publication_removed().unwrap();
    let reproved = fixture.authority.observe_completed(&request, &io).unwrap();
    assert_eq!(reproved.checkpoint_id(), completed.checkpoint_id());
    assert_eq!(fixture.authority.row, frozen);
    fixture.authority.close(&io).unwrap();
    let mut command = TargetAuthority::try_acquire_follow(
        named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
        &fixture.control,
        &io,
    )
    .unwrap();
    assert_eq!(
        command
            .observe_completed(&request, &io)
            .unwrap()
            .checkpoint_id(),
        completed.checkpoint_id()
    );
    command.close(&io).unwrap();
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_drain_recovery_reproves_original_witness() {
    let io = control();
    let mut fixture = cutover_fixture(&io);
    let pending = fixture
        .control
        .set(SourceEndLsn(0x20), SourceEndLsn(0x10), &io)
        .unwrap();
    let stream = fixture.control.follow().binding.stream();
    let mut crossing = fixture.authority.begin_group(&io).unwrap();
    crossing.advance(follow_frontier(stream, 2, 0x28)).unwrap();
    assert!(matches!(
        crossing.commit(&io).unwrap(),
        CommitOutcome::Acknowledged {
            durable: Some(_),
            ..
        }
    ));
    let sealed = fixture.control.seal(&pending, &io).unwrap();
    fixture
        .authority
        .record_drain(&sealed, DrainWitness::LaterBegin(CommitLsn(0x30)), &io)
        .unwrap();
    let persisted = fixture.authority.row.clone();
    fixture
        .authority
        .reacquire(
            named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
            &io,
        )
        .unwrap();
    fixture
        .authority
        .record_drain(&sealed, DrainWitness::Keepalive(SourceEndLsn(0x40)), &io)
        .unwrap();
    assert_eq!(fixture.authority.row, persisted);
    let completed = fixture
        .authority
        .complete_follow(
            &cutover_options(),
            &mut fixture.source,
            &fixture.native,
            &fixture.plan,
            &sealed,
            &io,
        )
        .unwrap();
    assert_eq!(completed.witness(), SourceEndLsn(0x30));
    assert_eq!(completed.kind(), DrainKind::LaterBegin);
    assert_eq!(completed.durable_replay_lsn(), SourceEndLsn(0x28));
}

impl Fault {
    fn connect_cutover(&self, io: &IoControl) -> PgConnection {
        use std::os::unix::ffi::OsStringExt;
        let mut value = self.conninfo.as_c_str().to_bytes().to_vec();
        value.extend_from_slice(b" user=e1_admin");
        PgConnection::connect(
            &Conninfo::new(std::ffi::OsString::from_vec(value)).unwrap(),
            io,
        )
        .unwrap()
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness"]
fn follow_observation_unknown_commit_needs_new_barrier() {
    let io = control();
    for stage in ["follow_drain", "follow_completion", "follow_observation"] {
        let mut fixture = cutover_fixture(&io);
        let request = sealed_cutover(&fixture, &io);
        if stage == "follow_completion" {
            fixture
                .authority
                .record_drain(&request, DrainWitness::LaterBegin(CommitLsn(0x30)), &io)
                .unwrap();
        }
        let mut fault = Fault::start(stage);
        fixture
            .authority
            .reacquire(fault.connect_cutover(&io), &io)
            .unwrap();
        let old = fixture.authority.row.clone();
        fault.request("arm");
        let failure = match stage {
            "follow_drain" => fixture
                .authority
                .record_drain(&request, DrainWitness::LaterBegin(CommitLsn(0x30)), &io)
                .err()
                .unwrap(),
            "follow_completion" => fixture
                .authority
                .complete_follow(
                    &cutover_options(),
                    &mut fixture.source,
                    &fixture.native,
                    &fixture.plan,
                    &request,
                    &io,
                )
                .err()
                .unwrap(),
            _ => fixture.authority.observe_follow(&io).err().unwrap(),
        };
        assert_eq!(failure.kind, ErrorKind::Target);
        assert!(matches!(fixture.authority.session.phase, Phase::Poisoned));
        assert_eq!(fixture.authority.row, old);
        fault.require_held();
        let refused = TargetAuthority::try_acquire_follow(
            named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
            &fixture.control,
            &io,
        )
        .err()
        .unwrap();
        assert_eq!(refused.kind, ErrorKind::BadState);
        let mut reader = named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io);
        assert_eq!(
            read_endpos_floor(&mut reader, &fixture.control, &io).unwrap(),
            SourceEndLsn(0x10)
        );
        let visible = metadata::read_only(&mut reader, &fixture.authority.provenance, &io).unwrap();
        assert_eq!(visible.follow_completed, stage == "follow_completion");
        fault.require_held();
        fault.release();
        fixture
            .authority
            .reacquire(
                named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
                &io,
            )
            .unwrap();
        let before_barrier = fixture.authority.row.clone();
        let snapshot = fixture.authority.observe_follow(&io).unwrap();
        assert_eq!(fixture.authority.row, before_barrier);
        assert_eq!(snapshot.follow_completed(), stage == "follow_completion");
        if stage == "follow_drain" {
            fixture
                .authority
                .record_drain(&request, DrainWitness::Keepalive(SourceEndLsn(0x40)), &io)
                .unwrap();
            assert_eq!(fixture.authority.row, before_barrier);
            assert_eq!(
                fixture.authority.row.drain.unwrap().witness,
                SourceEndLsn(0x30)
            );
        }
        if stage == "follow_completion" {
            let completed = fixture.authority.observe_completed(&request, &io).unwrap();
            assert_eq!(completed.checkpoint_id(), snapshot.checkpoint_id());
        }
        fixture.authority.close(&io).unwrap();
        fault.finish();
    }
}

#[test]
#[ignore = "requires owned authority PostgreSQL harness with exclusive SyncRep settings"]
fn follow_cancelled_sync_wait_never_proves_drain_completion_or_observation() {
    let io = control();
    let context_path = PathBuf::from(std::env::var_os("RPGMIGRATE_TEST_CONTEXT").unwrap());
    let context: serde_json::Value =
        serde_json::from_slice(&std::fs::read(context_path).unwrap()).unwrap();
    let token = context["token"].as_str().unwrap();
    assert_eq!(
        std::fs::read_to_string(PathBuf::from(context["root"].as_str().unwrap()).join(".e1-owner"))
            .unwrap(),
        token
    );
    let admin_info =
        Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_ADMIN").unwrap()).unwrap();
    let mut admin = PgConnection::connect(&admin_info, &io).unwrap();
    assert_eq!(
        admin
            .query("SELECT pg_catalog.current_setting('cluster_name')", &io)
            .unwrap()
            .cell(0, 0)
            .unwrap(),
        Some(token.as_bytes())
    );
    command(&mut admin, "SET synchronous_commit=off", "SET", &io).unwrap();
    for stage in ["drain", "completion", "observation"] {
        let mut fixture = cutover_fixture(&io);
        let request = sealed_cutover(&fixture, &io);
        if stage == "completion" {
            fixture
                .authority
                .record_drain(&request, DrainWitness::LaterBegin(CommitLsn(0x30)), &io)
                .unwrap();
        }
        let previous = fixture.authority.row.clone();
        let check = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            clone_marker_tests::set_missing_sync_standby(&mut admin, true, &io);
            let cancel = clone_marker_tests::cancel_sync_replication_wait(
                &admin_info,
                fixture.authority.session.pid,
                &io,
            );
            let failure = match stage {
                "drain" => fixture
                    .authority
                    .record_drain(&request, DrainWitness::LaterBegin(CommitLsn(0x30)), &io)
                    .err()
                    .unwrap(),
                "completion" => fixture
                    .authority
                    .complete_follow(
                        &cutover_options(),
                        &mut fixture.source,
                        &fixture.native,
                        &fixture.plan,
                        &request,
                        &io,
                    )
                    .err()
                    .unwrap(),
                _ => fixture.authority.observe_follow(&io).err().unwrap(),
            };
            cancel
                .join()
                .expect("M4 COMMIT cancellation was not observed");
            assert_eq!(failure.kind, ErrorKind::Target);
            assert!(matches!(fixture.authority.session.phase, Phase::Poisoned));
            assert_eq!(fixture.authority.row, previous);
        }));
        clone_marker_tests::set_missing_sync_standby(&mut admin, false, &io);
        if let Err(panic) = check {
            std::panic::resume_unwind(panic);
        }
        fixture
            .authority
            .reacquire(
                named_connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", &io),
                &io,
            )
            .unwrap();
        let visible = fixture.authority.row.clone();
        let snapshot = fixture.authority.observe_follow(&io).unwrap();
        assert_eq!(fixture.authority.row, visible);
        assert_eq!(snapshot.follow_completed(), stage == "completion");
        if stage == "completion" {
            fixture.authority.observe_completed(&request, &io).unwrap();
        }
    }
}
