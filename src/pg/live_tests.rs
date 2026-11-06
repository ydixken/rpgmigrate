use super::*;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(15),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn fixture() -> Conninfo {
    let _context =
        std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("isolated harness context is required");
    Conninfo::new(
        std::env::var_os("RPGMIGRATE_TEST_SOURCE")
            .expect("isolated harness connection is required"),
    )
    .unwrap()
}

#[test]
#[ignore = "requires isolated harness connection"]
fn query_drains_errors_rejects_multiple_statements_and_remains_usable() {
    let io = control();
    let mut connection = PgConnection::connect(&fixture(), &io).unwrap();
    for sql in [
        "SELECT 1/0; SELECT 1",
        "SELECT 'private-row'; SELECT 1",
        "SELECT 1/0",
    ] {
        let failure = match connection.query(sql, &io) {
            Ok(_) => panic!("query unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(!failure.message.contains("private-row"));
        let result = connection.query("SELECT 42", &io).unwrap();
        assert_eq!(result.rows(), 1);
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"42"[..]));
    }
    let name = connection.quote_identifier("quoted\"☃").unwrap();
    assert_eq!(name, "\"quoted\"\"☃\"");
    assert!(connection.quote_identifier("hidden\0tail").is_err());
    assert!(connection.query("SELECT 1\0private-row", &io).is_err());
    ping(&fixture(), &io).unwrap();
}

#[test]
#[ignore = "requires isolated harness connection"]
fn raw_copy_handles_empty_null_escaped_bytes_and_one_oversize_frame() {
    let io = control();
    let mut source = PgConnection::connect(&fixture(), &io).unwrap();
    let mut target = PgConnection::connect(&fixture(), &io).unwrap();
    for connection in [&mut source, &mut target] {
        connection
            .query(
                "CREATE TEMP TABLE pg_transport(n integer, value text, raw bytea)",
                &io,
            )
            .unwrap();
    }
    source.query("INSERT INTO pg_transport VALUES (1, E'line\\n\\t\\\\☃', decode('00ff', 'hex')), (2, NULL, NULL), (3, repeat('z',262144), decode('', 'hex'))", &io).unwrap();
    target.query("BEGIN", &io).unwrap();
    source
        .start_copy_out("COPY pg_transport TO STDOUT", &io)
        .unwrap();
    target
        .start_copy_in("COPY pg_transport FROM STDIN", &io)
        .unwrap();
    let mut largest = 0;
    while let Some(buffer) = source.read_copy(&io).unwrap() {
        largest = largest.max(buffer.as_bytes().len());
        target.write_copy(buffer.as_bytes(), &io).unwrap();
    }
    assert!(largest > 262144);
    assert_eq!(target.finish_copy_in(&io).unwrap(), 3);
    assert_eq!(
        target.query("COMMIT", &io).unwrap().command_tag().unwrap(),
        "COMMIT"
    );
    let sql = "SELECT n, value, raw FROM pg_transport ORDER BY n";
    let expected = source.query(sql, &io).unwrap();
    let actual = target.query(sql, &io).unwrap();
    assert_eq!(actual.rows(), 3);
    for row in 0..3 {
        for column in 0..3 {
            assert_eq!(
                expected.cell(row, column).unwrap(),
                actual.cell(row, column).unwrap()
            );
        }
    }
    source
        .start_copy_out("COPY (SELECT 1 WHERE false) TO STDOUT", &io)
        .unwrap();
    assert!(source.read_copy(&io).unwrap().is_none());
    target
        .start_copy_in("COPY pg_transport FROM STDIN", &io)
        .unwrap();
    assert_eq!(target.finish_copy_in(&io).unwrap(), 0);
}

#[test]
#[ignore = "requires isolated harness connection"]
fn late_copy_error_and_copy_abort_leave_transactions_rollbackable() {
    let io = control();
    let mut source = PgConnection::connect(&fixture(), &io).unwrap();
    source.start_copy_out("COPY (SELECT CASE WHEN n=3 THEN 1/(n-3) ELSE n END FROM generate_series(1,3) n) TO STDOUT", &io).unwrap();
    let mut frames = 0;
    loop {
        match source.read_copy(&io) {
            Ok(Some(_)) => frames += 1,
            Ok(None) => panic!("late COPY error was swallowed"),
            Err(error) => {
                assert!(error.message.contains("22012"));
                break;
            }
        }
    }
    assert_eq!(frames, 2);
    assert!(source.query("SELECT 1", &io).is_ok());
    let mut target = PgConnection::connect(&fixture(), &io).unwrap();
    target
        .query("CREATE TEMP TABLE pg_transport(n integer)", &io)
        .unwrap();
    target.query("BEGIN", &io).unwrap();
    target
        .start_copy_in("COPY pg_transport FROM STDIN", &io)
        .unwrap();
    target.write_copy(b"1\n", &io).unwrap();
    target.abort_copy_in("private-row-secret", &io).unwrap();
    target.query("ROLLBACK", &io).unwrap();
    let result = target
        .query("SELECT count(*) FROM pg_transport", &io)
        .unwrap();
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"0"[..]));
    target.query("BEGIN", &io).unwrap();
    target
        .start_copy_in("COPY pg_transport FROM STDIN", &io)
        .unwrap();
    let written = target.write_copy(b"private-row-secret\n", &io);
    let failure = written.and_then(|()| target.finish_copy_in(&io).map(|_| ()));
    assert!(!failure.unwrap_err().message.contains("private-row-secret"));
    target.query("ROLLBACK", &io).unwrap();
}

#[test]
#[ignore = "requires isolated harness connection"]
fn query_deadline_and_cancellation_stop_waiting() {
    let mut connection = PgConnection::connect(&fixture(), &control()).unwrap();
    let mut io = control();
    io.deadline = Instant::now() + Duration::from_millis(20);
    let started = Instant::now();
    assert!(connection.query("SELECT pg_sleep(10)", &io).is_err());
    assert!(started.elapsed() < Duration::from_secs(1));
    let mut connection = PgConnection::connect(&fixture(), &control()).unwrap();
    let io = control();
    let cancellation = Arc::clone(&io.cancelled);
    let trigger = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        cancellation.store(true, std::sync::atomic::Ordering::Release);
    });
    let started = Instant::now();
    assert!(connection.query("SELECT pg_sleep(10)", &io).is_err());
    trigger.join().unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[test]
#[ignore = "requires isolated harness connection"]
fn passfile_and_service_authentication_preserve_security_checks() {
    use super::passfile_tests::{Files, assert_child_completed, quote};
    use std::fs::{self, DirBuilder};
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

    if let Some(expected) = std::env::var_os("RPGMIGRATE_AUTH_CHILD_EXPECT_SUCCESS") {
        let conninfo =
            Conninfo::new(std::env::var_os("RPGMIGRATE_AUTH_CHILD_CONNINFO").unwrap()).unwrap();
        let result = ping(&conninfo, &control());
        if expected == "yes" {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().kind, ErrorKind::Pgsql);
        }
        println!("passfile-child-complete");
        return;
    }
    let base =
        std::env::var("RPGMIGRATE_TEST_AUTH_BASE").expect("isolated SCRAM fixture is required");
    let password = std::env::var("RPGMIGRATE_TEST_AUTH_PASSWORD")
        .expect("isolated SCRAM credential is required");
    for location in ["explicit", "environment", "default", "service"] {
        for shape in ["valid", "insecure", "nonregular", "wrong-password"] {
            let files = Files::new();
            let passfile = files.root.join(".pgpass");
            if shape == "nonregular" {
                DirBuilder::new().mode(0o700).create(&passfile).unwrap();
            } else {
                let value = if shape == "wrong-password" {
                    "synthetic-wrong-value"
                } else {
                    &password
                };
                let escaped = value.replace('\\', "\\\\").replace(':', "\\:");
                let mode = if shape == "insecure" { 0o644 } else { 0o600 };
                files.write(".pgpass", format!("*:*:*:*:{escaped}\n").as_bytes(), mode);
                assert_eq!(
                    fs::metadata(&passfile).unwrap().permissions().mode() & 0o777,
                    mode
                );
            }
            let mut conninfo = base.clone();
            let mut child = files.child(
                "pg::live_tests::passfile_and_service_authentication_preserve_security_checks",
            );
            child.arg("--ignored");
            match location {
                "explicit" => {
                    conninfo.push_str(&format!(" passfile={}", quote(passfile.to_str().unwrap())))
                }
                "environment" => {
                    child.env("PGPASSFILE", &passfile);
                }
                "default" => {}
                "service" => {
                    let service = files.write(
                        "service.conf",
                        format!("[synthetic]\npassfile={}\n", passfile.to_str().unwrap())
                            .as_bytes(),
                        0o600,
                    );
                    child.env("PGSERVICEFILE", service);
                    conninfo.push_str(" service=synthetic");
                }
                _ => unreachable!(),
            }
            let output = child
                .env("RPGMIGRATE_AUTH_CHILD_CONNINFO", conninfo)
                .env(
                    "RPGMIGRATE_AUTH_CHILD_EXPECT_SUCCESS",
                    if shape == "valid" { "yes" } else { "no" },
                )
                .output()
                .unwrap();
            assert_child_completed(&output);
            assert!(
                output.stderr.is_empty(),
                "native stderr escaped the SCRAM fixture; output withheld"
            );
        }
    }
}
