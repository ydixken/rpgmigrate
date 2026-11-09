use super::prepared_live_tests::{
    IDENTITY_REWRITE_SQL, assert_identity_rewrite_state, identity_rewrite_fixture,
};
use super::{
    CommitObservation, Conninfo, IoControl, PgConnection, PgParam, TransactionStatus, error,
};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(15),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn connect(io: &IoControl) -> PgConnection {
    std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("isolated harness context is required");
    PgConnection::connect(
        &Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_SOURCE")
                .expect("isolated harness connection is required"),
        )
        .unwrap(),
        io,
    )
    .unwrap()
}

#[test]
#[ignore = "requires isolated harness connection"]
fn pipeline_with_identity_rewrite_is_atomic() {
    let io = control();
    let mut connection = connect(&io);
    identity_rewrite_fixture(&mut connection, &io);
    let mut pipeline = connection.pipeline_begin(&io).unwrap();
    pipeline
        .query("UPDATE identity_sentinel SET value=1", &io)
        .unwrap();
    pipeline
        .query(
            &IDENTITY_REWRITE_SQL
                .replace("$1", "11")
                .replace("$2", "'changed'")
                .replace("$3", "1")
                .replace("$4", "'first'"),
            &io,
        )
        .unwrap();
    let mut tags = Vec::new();
    pipeline
        .sync(&io, |index, result| {
            assert_eq!(result.rows(), 0);
            tags.push((index, result.command_tag()?.to_owned()));
            Ok(())
        })
        .unwrap();
    assert_eq!(
        tags,
        [
            (0, "BEGIN".to_owned()),
            (1, "UPDATE 1".to_owned()),
            (2, "INSERT 0 1".to_owned()),
        ]
    );
    connection
        .prepare("rewrite", IDENTITY_REWRITE_SQL, &[23, 25, 23, 25], &io)
        .unwrap();
    let mut pipeline = connection.pipeline(&io).unwrap();
    pipeline
        .execute_prepared(
            "rewrite",
            &[
                PgParam::Text(b"12"),
                PgParam::Text(b"changed"),
                PgParam::Text(b"11"),
                PgParam::Text(b"changed"),
            ],
            &io,
        )
        .unwrap();
    let mut tags = Vec::new();
    pipeline
        .sync(&io, |index, result| {
            assert_eq!(result.rows(), 0);
            tags.push((index, result.command_tag()?.to_owned()));
            Ok(())
        })
        .unwrap();
    assert_eq!(tags, [(0, "INSERT 0 1".to_owned())]);
    assert_eq!(
        connection.transaction_status().unwrap(),
        TransactionStatus::InTransaction
    );
    assert_identity_rewrite_state(&mut connection, 12, 1, &io);
    assert_eq!(
        connection.commit_observed(&io).unwrap(),
        CommitObservation::Committed
    );
    for (id, other, sqlstate) in [
        (&b"2"[..], PgParam::Text(b"changed"), "23505"),
        (&b"13"[..], PgParam::Null, "23502"),
    ] {
        let mut pipeline = connection.pipeline_begin(&io).unwrap();
        pipeline
            .query("UPDATE identity_sentinel SET value=2", &io)
            .unwrap();
        pipeline
            .execute_prepared(
                "rewrite",
                &[
                    PgParam::Text(id),
                    other,
                    PgParam::Text(b"12"),
                    PgParam::Text(b"changed"),
                ],
                &io,
            )
            .unwrap();
        let failure = pipeline.sync(&io, |_, _| Ok(())).unwrap_err();
        assert!(failure.message.contains(sqlstate));
        assert_eq!(
            connection.transaction_status().unwrap(),
            TransactionStatus::InError
        );
        assert_eq!(
            connection.commit_observed(&io).unwrap(),
            CommitObservation::RolledBack
        );
        assert_eq!(
            connection.transaction_status().unwrap(),
            TransactionStatus::Idle
        );
        assert_identity_rewrite_state(&mut connection, 12, 1, &io);
    }
}

#[test]
#[ignore = "requires isolated harness connection"]
fn pipeline_with_transaction_escapes_never_commit() {
    let io = control();
    let mut observer = connect(&io);
    observer
        .query("CREATE TABLE pipeline_escape_sentinel(value integer)", &io)
        .unwrap();
    observer
        .query("INSERT INTO pipeline_escape_sentinel VALUES (0)", &io)
        .unwrap();
    for sql in [
        "COMMIT",
        "SELECT 1; COMMIT",
        "WITH x AS (COMMIT) SELECT 1",
        "WITH x AS (SELECT 1) COMMIT",
        "WITH x AS (SELECT 1) SELECT 1; COMMIT",
    ] {
        let mut connection = connect(&io);
        connection.query("BEGIN", &io).unwrap();
        connection
            .query("UPDATE pipeline_escape_sentinel SET value=1", &io)
            .unwrap();
        let mut pipeline = connection.pipeline(&io).unwrap();
        if sql == "COMMIT" {
            let failure = pipeline.query(sql, &io).unwrap_err();
            assert!(failure.message.contains("internal data"));
            assert_eq!(pipeline.commands(), 0);
            drop(pipeline);
            assert!(connection.query("SELECT 1", &io).is_err());
        } else {
            pipeline.query(sql, &io).unwrap();
            assert_eq!(pipeline.commands(), 1);
            let failure = pipeline.sync(&io, |_, _| Ok(())).unwrap_err();
            assert!(
                failure.message.contains("42601"),
                "{sql}: {}",
                failure.message
            );
            assert_eq!(
                connection.transaction_status().unwrap(),
                TransactionStatus::InError
            );
            assert_eq!(
                connection.commit_observed(&io).unwrap(),
                CommitObservation::RolledBack
            );
            assert_eq!(
                connection.transaction_status().unwrap(),
                TransactionStatus::Idle
            );
        }
        assert_eq!(
            observer
                .query("SELECT value FROM pipeline_escape_sentinel", &io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(&b"0"[..])
        );
    }
    observer
        .query("DROP TABLE pipeline_escape_sentinel", &io)
        .unwrap();
}

#[test]
#[ignore = "requires isolated harness connection"]
fn pipeline_validates_ordered_rows_and_every_terminal_result() {
    let io = control();
    let mut connection = connect(&io);
    connection
        .prepare("value", "SELECT $1::text", &[25], &io)
        .unwrap();
    let mut pipeline = connection.pipeline_begin(&io).unwrap();
    pipeline.query("SELECT generate_series(1, 3)", &io).unwrap();
    pipeline
        .execute_prepared("value", &[PgParam::Text(b"tail")], &io)
        .unwrap();
    let mut values = Vec::new();
    let mut terminals = Vec::new();
    pipeline
        .sync(&io, |index, result| {
            if result.rows() == 1 {
                values.push((index, result.cell(0, 0)?.unwrap().to_vec()));
            } else {
                terminals.push(index);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(
        values,
        [
            (1, b"1".to_vec()),
            (1, b"2".to_vec()),
            (1, b"3".to_vec()),
            (2, b"tail".to_vec())
        ]
    );
    assert_eq!(terminals, [0, 1, 2]);
    assert_eq!(
        connection.commit_observed(&io).unwrap(),
        CommitObservation::Committed
    );
    assert!(connection.commit_observed(&io).is_err());
    assert!(connection.query("SELECT 42", &io).is_ok());
}

#[test]
#[ignore = "requires isolated harness connection"]
fn pipeline_sql_and_callback_errors_drain_through_sync() {
    let io = control();
    let mut connection = connect(&io);
    connection
        .query("CREATE TEMP TABLE skipped_pipeline(value integer)", &io)
        .unwrap();
    let mut pipeline = connection.pipeline_begin(&io).unwrap();
    for sql in ["SELECT 1 / 0", "INSERT INTO skipped_pipeline VALUES (1)"] {
        pipeline.query(sql, &io).unwrap();
    }
    let failure = pipeline.sync(&io, |_, _| Ok(())).unwrap_err();
    assert!(failure.message.contains("22012"));
    connection.query("ROLLBACK", &io).unwrap();
    let result = connection
        .query("SELECT count(*)=0 FROM skipped_pipeline", &io)
        .unwrap();
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"t"[..]));
    let mut pipeline = connection.pipeline(&io).unwrap();
    pipeline.query("SELECT generate_series(1, 3)", &io).unwrap();
    pipeline.query("SELECT 4", &io).unwrap();
    let mut seen = 0;
    let failure = pipeline
        .sync(&io, |_, _| {
            seen += 1;
            Err(error("synthetic validator refusal"))
        })
        .unwrap_err();
    assert_eq!(failure.message, "synthetic validator refusal");
    assert_eq!(seen, 6);
    assert!(connection.query("SELECT 42", &io).is_ok());
    let mut pipeline = connection.pipeline(&io).unwrap();
    pipeline
        .query("SELECT 'private-row-secret'::integer", &io)
        .unwrap();
    let failure = pipeline.sync(&io, |_, _| Ok(())).unwrap_err();
    assert!(!failure.message.contains("private-row-secret"));
    assert!(connection.query("SELECT 42", &io).is_ok());

    let mut refused = connect(&io);
    let mut pipeline = refused.pipeline_begin(&io).unwrap();
    let failure = pipeline
        .query(
            "DO $$ BEGIN RAISE WARNING 'private-warning-row-secret'; END $$",
            &io,
        )
        .expect_err("pipeline utility command was accepted");
    assert!(failure.message.contains("internal data"));
    assert!(!failure.message.contains("private-warning-row-secret"));
    drop(pipeline);
    assert!(refused.query("SELECT 1", &io).is_err());
    connection
        .query(
            "DO $$ BEGIN RAISE WARNING 'private-prior-warning'; END $$",
            &io,
        )
        .unwrap();
    connection
        .query("SET client_min_messages = error", &io)
        .unwrap();
    connection.query("BEGIN", &io).unwrap();
    assert_eq!(
        connection.commit_observed(&io).unwrap(),
        CommitObservation::Committed
    );
    assert_eq!(
        connection
            .query("SHOW client_min_messages", &io)
            .unwrap()
            .cell(0, 0)
            .unwrap(),
        Some(&b"error"[..])
    );

    connection
        .query("CREATE TEMP TABLE deferred_warning(value integer)", &io)
        .unwrap();
    connection.query("CREATE FUNCTION pg_temp.warn_at_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE WARNING 'private-deferred-row-secret'; RETURN NEW; END $$", &io).unwrap();
    connection.query("CREATE CONSTRAINT TRIGGER deferred_warning AFTER INSERT ON deferred_warning DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pg_temp.warn_at_commit()", &io).unwrap();
    connection.query("BEGIN", &io).unwrap();
    connection
        .query("INSERT INTO deferred_warning VALUES (1)", &io)
        .unwrap();
    assert_eq!(
        connection.commit_observed(&io).unwrap(),
        CommitObservation::Unknown
    );
    let rows = connection
        .query("SELECT count(*) FROM deferred_warning", &io)
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(&b"1"[..]));
    connection.query("BEGIN", &io).unwrap();
    assert_eq!(
        connection.commit_observed(&io).unwrap(),
        CommitObservation::Committed
    );
}

#[test]
#[ignore = "requires isolated harness connection"]
fn pipeline_drop_deadline_and_limits_fail_closed() {
    let io = control();
    let mut committed = connect(&io);
    committed
        .query("SET client_min_messages = error", &io)
        .unwrap();
    let mut pipeline = committed.pipeline_begin(&io).unwrap();
    assert!(
        pipeline
            .query("COMMIT", &io)
            .unwrap_err()
            .message
            .contains("internal data")
    );
    drop(pipeline);
    assert!(committed.query("SELECT 1", &io).is_err());
    let mut dropped = connect(&io);
    let mut pipeline = dropped.pipeline(&io).unwrap();
    pipeline.query("SELECT 1", &io).unwrap();
    drop(pipeline);
    assert!(dropped.query("SELECT 1", &io).is_err());
    let mut delayed = connect(&io);
    let mut pipeline = delayed.pipeline(&io).unwrap();
    pipeline.query("SELECT pg_sleep(10)", &io).unwrap();
    let mut short = control();
    short.deadline = Instant::now() + Duration::from_millis(20);
    assert!(
        pipeline
            .sync(&short, |_, _| Ok(()))
            .unwrap_err()
            .message
            .contains("deadline")
    );
    assert!(delayed.query("SELECT 1", &io).is_err());
    let mut empty = connect(&io);
    assert!(
        empty
            .pipeline(&io)
            .unwrap()
            .sync(&io, |_, _| Ok(()))
            .is_err()
    );
    assert!(empty.query("SELECT 1", &io).is_err());
    let mut bounded = connect(&io);
    let mut pipeline = bounded.pipeline(&io).unwrap();
    for _ in 0..64 {
        pipeline.query("SELECT 1", &io).unwrap();
    }
    assert!(pipeline.query("SELECT 1", &io).is_err());
    drop(pipeline);
    assert!(bounded.query("SELECT 1", &io).is_err());
    let mut oversized = connect(&io);
    let mut pipeline = oversized.pipeline(&io).unwrap();
    pipeline.query("SELECT repeat('x', 16777217)", &io).unwrap();
    assert!(
        pipeline
            .sync(&io, |_, _| Ok(()))
            .unwrap_err()
            .message
            .contains("row limit")
    );
    assert!(oversized.query("SELECT 1", &io).is_ok());
}
