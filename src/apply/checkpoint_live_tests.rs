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
                "target-authority: physical backend, primary, or writer fence changed"
            }
            "pid_mismatch" => {
                tx.authority.session.pid += 1;
                "target-authority: physical backend, primary, or writer fence changed"
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
            FROM pg_catalog.pg_attribute WHERE attrelid = {} AND attnum BETWEEN 20 AND 22
            ORDER BY attnum",
                provenance.table_oid
            ),
            io,
        )
        .unwrap();
    assert_eq!(columns.rows(), 3);
    for (row, (name, oid)) in [
        ("copy_snapshot_token", "25"),
        ("copy_consistent_point", "3220"),
        ("copy_plan_nonce", "17"),
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
    authority.session.begin(io).unwrap();
    let types = authority
        .session
        .connection()
        .unwrap()
        .query(
            "SELECT pg_catalog.cardinality(parameter_types),
            parameter_types[20]::pg_catalog.oid, parameter_types[21]::pg_catalog.oid,
            parameter_types[22]::pg_catalog.oid, parameter_types[30]::pg_catalog.oid,
            parameter_types[31]::pg_catalog.oid, parameter_types[32]::pg_catalog.oid
            FROM pg_catalog.pg_prepared_statements WHERE name = 'rpgmigrate_metadata_update'",
            io,
        )
        .unwrap();
    assert_eq!(types.rows(), 1);
    for (column, expected) in ["32", "25", "3220", "17", "25", "3220", "17"]
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
                "target-authority: physical backend, primary, or writer fence changed"
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
