use super::{CommitObservation, Conninfo, IoControl, PgConnection, PgParam, TransactionStatus};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(15),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn connection(io: &IoControl) -> PgConnection {
    std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("isolated harness context is required");
    let conninfo = Conninfo::new(
        std::env::var_os("RPGMIGRATE_TEST_SOURCE")
            .expect("isolated harness connection is required"),
    )
    .unwrap();
    PgConnection::connect(&conninfo, io).unwrap()
}

pub(super) const IDENTITY_REWRITE_SQL: &str = "WITH rpgmigrate_old AS (DELETE FROM ONLY \"pg_temp\".\"identity_rewrite\" WHERE ctid = (SELECT ctid FROM ONLY \"pg_temp\".\"identity_rewrite\" WHERE \"id\" IS NOT DISTINCT FROM $3 AND \"other\" IS NOT DISTINCT FROM $4 LIMIT 1 FOR UPDATE) RETURNING \"id\", \"body\", \"other\") INSERT INTO \"pg_temp\".\"identity_rewrite\" (\"id\", \"body\", \"other\") OVERRIDING SYSTEM VALUE SELECT $1, rpgmigrate_old.\"body\", $2 FROM rpgmigrate_old";

pub(super) fn identity_rewrite_fixture(connection: &mut PgConnection, io: &IoControl) {
    connection
        .query(
            "CREATE TEMP TABLE identity_rewrite(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body text, other text NOT NULL)",
            io,
        )
        .unwrap();
    connection
        .query(
            "INSERT INTO identity_rewrite OVERRIDING SYSTEM VALUE VALUES (1, repeat('x', 65536), 'first'), (2, 'keep', 'second')",
            io,
        )
        .unwrap();
    connection
        .query("CREATE TEMP TABLE identity_sentinel(value integer)", io)
        .unwrap();
    connection
        .query("INSERT INTO identity_sentinel VALUES (0)", io)
        .unwrap();
}

pub(super) fn assert_identity_rewrite_state(
    connection: &mut PgConnection,
    id: u32,
    sentinel: u32,
    io: &IoControl,
) {
    let result = connection
        .query(
            &format!(
                "SELECT (SELECT count(*) FROM identity_rewrite)=2 AND EXISTS (SELECT FROM identity_rewrite WHERE id={id} AND body=repeat('x', 65536) AND other='changed') AND EXISTS (SELECT FROM identity_rewrite WHERE id=2 AND body='keep' AND other='second') AND (SELECT value FROM identity_sentinel)={sentinel} AND (SELECT last_value=1 AND NOT is_called FROM identity_rewrite_id_seq)"
            ),
            io,
        )
        .unwrap();
    assert_eq!(result.rows(), 1);
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"t"[..]));
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_with_identity_rewrite_is_atomic() {
    let io = control();
    let mut connection = connection(&io);
    identity_rewrite_fixture(&mut connection, &io);
    connection
        .prepare("rewrite", IDENTITY_REWRITE_SQL, &[23, 25, 23, 25], &io)
        .unwrap();
    connection.query("BEGIN", &io).unwrap();
    connection
        .query("UPDATE identity_sentinel SET value=1", &io)
        .unwrap();
    let result = connection
        .execute_prepared(
            "rewrite",
            &[
                PgParam::Text(b"11"),
                PgParam::Text(b"changed"),
                PgParam::Text(b"1"),
                PgParam::Text(b"first"),
            ],
            &io,
        )
        .unwrap();
    assert_eq!(result.rows(), 0);
    assert_eq!(result.command_tag().unwrap(), "INSERT 0 1");
    assert_eq!(
        connection.transaction_status().unwrap(),
        TransactionStatus::InTransaction
    );
    assert_identity_rewrite_state(&mut connection, 11, 1, &io);
    assert_eq!(
        connection.commit_observed(&io).unwrap(),
        CommitObservation::Committed
    );
    for (id, other, sqlstate) in [
        (&b"2"[..], PgParam::Text(b"changed"), "23505"),
        (&b"12"[..], PgParam::Null, "23502"),
    ] {
        connection.query("BEGIN", &io).unwrap();
        connection
            .query("UPDATE identity_sentinel SET value=2", &io)
            .unwrap();
        let failure = connection
            .execute_prepared(
                "rewrite",
                &[
                    PgParam::Text(id),
                    other,
                    PgParam::Text(b"11"),
                    PgParam::Text(b"changed"),
                ],
                &io,
            )
            .err()
            .expect("failed insertion after DELETE was accepted");
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
        assert_identity_rewrite_state(&mut connection, 11, 1, &io);
    }
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_with_transaction_escapes_never_commit() {
    let io = control();
    let mut connection = connection(&io);
    connection
        .query(
            "CREATE TEMP TABLE prepared_escape_sentinel(value integer)",
            &io,
        )
        .unwrap();
    connection
        .query("INSERT INTO prepared_escape_sentinel VALUES (0)", &io)
        .unwrap();
    for (sql, state, message) in [
        ("COMMIT", TransactionStatus::InTransaction, "internal data"),
        ("SELECT 1; COMMIT", TransactionStatus::InError, "42601"),
        (
            "WITH x AS (COMMIT) SELECT 1",
            TransactionStatus::InError,
            "42601",
        ),
        (
            "WITH x AS (SELECT 1) COMMIT",
            TransactionStatus::InError,
            "42601",
        ),
        (
            "WITH x AS (SELECT 1) SELECT 1; COMMIT",
            TransactionStatus::InError,
            "42601",
        ),
    ] {
        connection.query("BEGIN", &io).unwrap();
        connection
            .query("UPDATE prepared_escape_sentinel SET value=1", &io)
            .unwrap();
        let failure = connection.prepare("escape", sql, &[], &io).unwrap_err();
        assert!(
            failure.message.contains(message),
            "{sql}: {}",
            failure.message
        );
        assert_eq!(connection.transaction_status().unwrap(), state);
        connection.query("ROLLBACK", &io).unwrap();
        assert_eq!(
            connection.transaction_status().unwrap(),
            TransactionStatus::Idle
        );
        assert_eq!(
            connection
                .query("SELECT value FROM prepared_escape_sentinel", &io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(&b"0"[..])
        );
    }
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_parameters_preserve_null_empty_binary_and_changed_values() {
    let io = control();
    let mut connection = connection(&io);
    connection
        .prepare(
            "values",
            "SELECT $1::text, $2::text, encode($3::bytea, 'hex'), octet_length($4::bytea), $5::integer, encode($6::bytea, 'hex')",
            &[25, 25, 17, 17, 23, 17],
            &io,
        )
        .unwrap();
    let first = connection
        .execute_prepared(
            "values",
            &[
                PgParam::Null,
                PgParam::Text(b""),
                PgParam::Binary(b"\0\xff'x\\"),
                PgParam::Binary(b""),
                PgParam::Text(b"7"),
                PgParam::Null,
            ],
            &io,
        )
        .unwrap();
    assert_eq!(first.rows(), 1);
    assert_eq!(first.cell(0, 0).unwrap(), None);
    assert_eq!(first.cell(0, 1).unwrap(), Some(&b""[..]));
    assert_eq!(first.cell(0, 2).unwrap(), Some(&b"00ff27785c"[..]));
    assert_eq!(first.cell(0, 3).unwrap(), Some(&b"0"[..]));
    assert_eq!(first.cell(0, 4).unwrap(), Some(&b"7"[..]));
    assert_eq!(first.cell(0, 5).unwrap(), None);
    let number = 9_i32.to_be_bytes();
    let second = connection
        .execute_prepared(
            "values",
            &[
                PgParam::Text(b"changed"),
                PgParam::Null,
                PgParam::Binary(b""),
                PgParam::Binary(b"x"),
                PgParam::Binary(&number),
                PgParam::Binary(b""),
            ],
            &io,
        )
        .unwrap();
    assert_eq!(second.cell(0, 0).unwrap(), Some(&b"changed"[..]));
    assert_eq!(second.cell(0, 1).unwrap(), None);
    assert_eq!(second.cell(0, 2).unwrap(), Some(&b""[..]));
    assert_eq!(second.cell(0, 3).unwrap(), Some(&b"1"[..]));
    assert_eq!(second.cell(0, 4).unwrap(), Some(&b"9"[..]));
    assert_eq!(second.cell(0, 5).unwrap(), Some(&b""[..]));
    assert_eq!(first.cell(0, 4).unwrap(), Some(&b"7"[..]));
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_invalid_inputs_do_not_dispatch() {
    let io = control();
    let mut connection = connection(&io);
    connection
        .query("CREATE TEMP TABLE prepared_validation(value text)", &io)
        .unwrap();
    connection
        .prepare(
            "insert_value",
            "INSERT INTO prepared_validation VALUES ($1)",
            &[25],
            &io,
        )
        .unwrap();
    connection.query("BEGIN", &io).unwrap();
    for (name, sql) in [
        ("rejected\0synthetic-private", "SELECT 1"),
        ("rejected", "SELECT 1\0synthetic-private"),
    ] {
        let failure = connection.prepare(name, sql, &[], &io).unwrap_err();
        assert!(failure.message.contains("NUL"));
        assert!(!failure.message.contains("synthetic-private"));
    }
    for (name, value) in [
        ("insert_value\0synthetic-private", &b"must-not-insert"[..]),
        ("insert_value", &b"synthetic-private\0tail"[..]),
    ] {
        let failure = connection
            .execute_prepared(name, &[PgParam::Text(value)], &io)
            .err()
            .expect("invalid input was dispatched");
        assert!(failure.message.contains("NUL"));
        assert!(!failure.message.contains("synthetic-private"));
    }
    let types = vec![25; 65_536];
    assert!(
        connection
            .prepare("too_many", "SELECT 1", &types, &io)
            .unwrap_err()
            .message
            .contains("count")
    );
    let params = std::iter::repeat_with(|| PgParam::Null)
        .take(65_536)
        .collect::<Vec<_>>();
    let failure = connection
        .execute_prepared("insert_value", &params, &io)
        .err()
        .expect("excess parameters were accepted");
    assert!(failure.message.contains("count"));
    let rows = connection
        .query("SELECT count(*) FROM prepared_validation", &io)
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(&b"0"[..]));
    let statements = connection
        .query(
            "SELECT count(*) FROM pg_prepared_statements WHERE name = 'rejected'",
            &io,
        )
        .unwrap();
    assert_eq!(statements.cell(0, 0).unwrap(), Some(&b"0"[..]));
    connection.query("ROLLBACK", &io).unwrap();
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_errors_never_return_stale_results() {
    let io = control();
    let mut connection = connection(&io);
    connection
        .prepare("value", "SELECT $1::integer", &[23], &io)
        .unwrap();
    let previous = connection
        .execute_prepared("value", &[PgParam::Text(b"41")], &io)
        .unwrap();
    let duplicate = connection
        .prepare("value", "SELECT 99", &[], &io)
        .unwrap_err();
    assert!(duplicate.message.contains("42P05"));
    let syntax = connection
        .prepare("bad", "SELECT synthetic-private invalid syntax", &[], &io)
        .unwrap_err();
    assert!(!syntax.message.contains("synthetic-private"));
    for (name, value, state) in [
        ("value", &b"synthetic-private"[..], "22P02"),
        ("missing", &b"42"[..], "26000"),
    ] {
        let failure = connection
            .execute_prepared(name, &[PgParam::Text(value)], &io)
            .err()
            .expect("previous success masked a new error");
        assert!(failure.message.contains(state));
        assert!(!failure.message.contains("synthetic-private"));
        let fresh = connection
            .execute_prepared("value", &[PgParam::Text(b"42")], &io)
            .unwrap();
        assert_eq!(fresh.rows(), 1);
        assert_eq!(fresh.cell(0, 0).unwrap(), Some(&b"42"[..]));
        assert_eq!(previous.cell(0, 0).unwrap(), Some(&b"41"[..]));
    }
    connection.prepare("", "SELECT 43", &[], &io).unwrap();
    let unnamed = connection.execute_prepared("", &[], &io).unwrap();
    assert_eq!(unnamed.cell(0, 0).unwrap(), Some(&b"43"[..]));
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_execution_deadline_closes_connection() {
    let mut io = control();
    let mut connection = connection(&io);
    connection
        .prepare("wait", "SELECT pg_sleep($1::float8)", &[701], &io)
        .unwrap();
    io.deadline = Instant::now() + Duration::from_millis(20);
    let started = Instant::now();
    let failure = connection
        .execute_prepared("wait", &[PgParam::Text(b"10")], &io)
        .err()
        .expect("deadline did not interrupt execution");
    assert!(failure.message.contains("deadline"));
    assert!(started.elapsed() < Duration::from_secs(1));
    let closed = connection
        .query("SELECT 1", &control())
        .err()
        .expect("interrupted connection was reused");
    assert!(closed.message.contains("closed"));
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_execution_cancellation_closes_connection() {
    let io = control();
    let mut connection = connection(&io);
    connection
        .prepare("wait", "SELECT pg_sleep($1::float8)", &[701], &io)
        .unwrap();
    let cancelled = Arc::clone(&io.cancelled);
    let trigger = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        cancelled.store(true, Ordering::Release);
    });
    let started = Instant::now();
    let failure = connection
        .execute_prepared("wait", &[PgParam::Text(b"10")], &io)
        .err()
        .expect("cancellation did not interrupt execution");
    trigger.join().unwrap();
    assert!(failure.message.contains("cancel"));
    assert!(started.elapsed() < Duration::from_secs(1));
    let closed = connection
        .query("SELECT 1", &control())
        .err()
        .expect("interrupted connection was reused");
    assert!(closed.message.contains("closed"));
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_changes_roll_back_with_transaction() {
    let io = control();
    let mut connection = connection(&io);
    assert_eq!(
        connection.transaction_status().unwrap(),
        TransactionStatus::Idle
    );
    connection
        .query(
            "CREATE TEMP TABLE prepared_effects(value integer PRIMARY KEY)",
            &io,
        )
        .unwrap();
    connection
        .query("CREATE TEMP TABLE prepared_checkpoint(value integer)", &io)
        .unwrap();
    connection
        .query("INSERT INTO prepared_checkpoint VALUES (0)", &io)
        .unwrap();
    connection
        .prepare(
            "effect",
            "INSERT INTO prepared_effects VALUES ($1)",
            &[23],
            &io,
        )
        .unwrap();
    connection
        .prepare(
            "checkpoint",
            "UPDATE prepared_checkpoint SET value = $1",
            &[23],
            &io,
        )
        .unwrap();
    for commit in [false, true] {
        connection.query("BEGIN", &io).unwrap();
        assert_eq!(
            connection.transaction_status().unwrap(),
            TransactionStatus::InTransaction
        );
        let inserted = connection
            .execute_prepared("effect", &[PgParam::Text(b"42")], &io)
            .unwrap();
        assert_eq!(inserted.command_tag().unwrap(), "INSERT 0 1");
        let checkpoint = connection
            .execute_prepared("checkpoint", &[PgParam::Text(b"42")], &io)
            .unwrap();
        assert_eq!(checkpoint.command_tag().unwrap(), "UPDATE 1");
        assert_eq!(
            connection.transaction_status().unwrap(),
            TransactionStatus::InTransaction
        );
        if !commit {
            let duplicate = connection
                .execute_prepared("effect", &[PgParam::Text(b"42")], &io)
                .err()
                .expect("duplicate effect was accepted");
            assert!(duplicate.message.contains("23505"));
            assert_eq!(
                connection.transaction_status().unwrap(),
                TransactionStatus::InError
            );
        }
        let command = if commit { "COMMIT" } else { "ROLLBACK" };
        assert_eq!(
            connection
                .query(command, &io)
                .unwrap()
                .command_tag()
                .unwrap(),
            command
        );
        assert_eq!(
            connection.transaction_status().unwrap(),
            TransactionStatus::Idle
        );
        let result = connection
            .query(
                "SELECT (SELECT count(*) FROM prepared_effects), value FROM prepared_checkpoint",
                &io,
            )
            .unwrap();
        let (count, checkpoint) = if commit {
            (&b"1"[..], &b"42"[..])
        } else {
            (&b"0"[..], &b"0"[..])
        };
        assert_eq!(result.cell(0, 0).unwrap(), Some(count));
        assert_eq!(result.cell(0, 1).unwrap(), Some(checkpoint));
    }
}

#[test]
#[ignore = "requires isolated harness connection"]
fn prepared_execution_rejects_copy_results() {
    let io = control();
    let mut connection = connection(&io);
    connection
        .prepare("copy", "COPY (SELECT 1) TO STDOUT", &[], &io)
        .unwrap();
    let failure = connection
        .execute_prepared("copy", &[], &io)
        .err()
        .expect("prepared execution accepted COPY mode");
    assert!(failure.message.contains("COPY"));
    assert!(connection.query("SELECT 1", &control()).is_err());
}
