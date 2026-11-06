use super::ffi::replication_testing::{
    create_slot, feedback_waiting_for_drain, flush_pending, small_send_buffer,
};
use super::{
    Conninfo, IoControl, PgConnection, ReplicationEnvelope, ReplicationPoll, StandbyStatus,
};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(30),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn fixture(key: &str) -> std::ffi::OsString {
    std::env::var_os("RPGMIGRATE_TEST_REPLICATION_CONTEXT")
        .expect("owned replication fixture is required");
    std::env::var_os(key).expect("owned replication fixture variable is required")
}

fn normal(key: &str, io: &IoControl) -> PgConnection {
    PgConnection::connect(&Conninfo::new(fixture(key)).unwrap(), io).unwrap()
}

fn logical(io: &IoControl) -> PgConnection {
    // A plain SQL DSN proves the safe parameter override selects the logical protocol.
    PgConnection::connect_replication(
        &Conninfo::new(fixture("RPGMIGRATE_TEST_REPLICATION_SQL")).unwrap(),
        io,
    )
    .unwrap()
}

fn start(connection: &mut PgConnection, slot: &str, io: &IoControl) {
    create_slot(connection, slot, io).unwrap();
    connection
        .start_replication(slot, 0, &[("include-xids", Some("0"))], io)
        .unwrap();
}

fn finish(connection: &mut PgConnection, io: &IoControl) {
    connection.request_replication_end().unwrap();
    loop {
        if matches!(
            connection.poll_replication(io.deadline, io).unwrap(),
            ReplicationPoll::End
        ) {
            break;
        }
    }
}

fn status() -> StandbyStatus {
    StandbyStatus {
        write: 0,
        flush: 0,
        apply: 0,
        client_time_micros: 123456,
        request_reply: true,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

struct Fault {
    child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    partial_receipt: bool,
}

impl Fault {
    fn start(case: &str, io: &IoControl) -> (Self, PgConnection) {
        Self::start_phase(case, false, io)
    }

    fn start_phase(case: &str, pending_error: bool, io: &IoControl) -> (Self, PgConnection) {
        let mut command = Command::new(fixture("RPGMIGRATE_TEST_PYTHON"));
        command
            .arg(fixture("RPGMIGRATE_TEST_REPLICATION_FAULT_HELPER"))
            .args(["--case", case]);
        if pending_error {
            command.arg("--pending-error");
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("owned fault controller failed to start");
        let input = child.stdin.take().unwrap();
        let output = BufReader::new(child.stdout.take().unwrap());
        let mut fault = Self {
            child,
            input,
            output,
            partial_receipt: case == "terminal_error" && !pending_error,
        };
        let ready = fault.read();
        assert_eq!(ready["ready"], true);
        let conninfo = Conninfo::new(
            ready["conninfo"]
                .as_str()
                .expect("fault connection missing")
                .into(),
        )
        .unwrap();
        let connection = PgConnection::connect_replication(&conninfo, io).unwrap();
        (fault, connection)
    }

    fn read(&mut self) -> Value {
        let mut line = String::new();
        assert!(
            self.output
                .read_line(&mut line)
                .expect("fault receipt read failed")
                > 0,
            "fault controller closed without receipt"
        );
        serde_json::from_str(&line).expect("fault receipt is not JSON")
    }

    fn command(&mut self, value: Value) -> Value {
        serde_json::to_writer(&mut self.input, &value).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
        self.read()
    }

    fn complete(mut self, connection: PgConnection) {
        drop(connection);
        let receipt = self.command(json!({"op":"quit"}));
        if self.partial_receipt {
            assert_eq!(receipt["phase_status"], "observed");
            assert_eq!(receipt["status"], "pending");
        } else {
            assert_eq!(receipt["status"], "observed");
        }
        assert_eq!(receipt["backend_closed"], true);
        assert!(self.child.wait().unwrap().success());
    }
}

fn block_feedback(connection: &mut PgConnection, io: &IoControl) -> StandbyStatus {
    let supplied = StandbyStatus {
        request_reply: false,
        ..status()
    };
    let mut pending = false;
    while flush_pending(connection) == 0 || !feedback_waiting_for_drain(connection) {
        if !pending {
            assert!(connection.queue_feedback(supplied).unwrap());
            pending = true;
        }
        if let ReplicationPoll::FeedbackDrained(observed) = connection
            .poll_replication(Instant::now() + Duration::from_millis(1), io)
            .unwrap()
        {
            assert_eq!(observed, supplied);
            pending = false;
        }
    }
    assert!(pending);
    assert!(
        !connection
            .queue_feedback(StandbyStatus {
                write: u64::MAX,
                ..supplied
            })
            .unwrap()
    );
    supplied
}

impl Drop for Fault {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
#[ignore = "requires owned replication harness"]
fn logical_connection_identifies_database_and_rejects_sql_mode() {
    let io = control();
    let mut sql = normal("RPGMIGRATE_TEST_REPLICATION_SQL", &io);
    assert!(
        sql.identify_system(&io)
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        sql.start_replication("unused", 0, &[], &io)
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        sql.queue_feedback(status())
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        sql.request_replication_end()
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        sql.poll_replication(io.deadline, &io)
            .err()
            .unwrap()
            .message
            .contains("mode")
    );
    assert_eq!(
        sql.query("SELECT 1", &io).unwrap().cell(0, 0).unwrap(),
        Some(&b"1"[..])
    );
    let mut connection = logical(&io);
    let identity = connection.identify_system(&io).unwrap();
    assert_ne!(identity.system_identifier, 0);
    assert_ne!(identity.timeline, 0);
    assert_eq!(
        identity.database_name,
        fixture("RPGMIGRATE_TEST_REPLICATION_DATABASE").as_encoded_bytes()
    );
    assert!(
        connection
            .query("SELECT 1", &io)
            .err()
            .unwrap()
            .message
            .contains("mode")
    );
    assert!(
        connection
            .prepare("p", "SELECT 1", &[], &io)
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        connection
            .execute_prepared("p", &[], &io)
            .err()
            .unwrap()
            .message
            .contains("mode")
    );
    assert!(
        connection
            .start_copy_out("COPY t TO STDOUT", &io)
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        connection
            .start_copy_in("COPY t FROM STDIN", &io)
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        connection
            .read_copy(&io)
            .err()
            .unwrap()
            .message
            .contains("mode")
    );
    assert!(
        connection
            .write_copy(b"1\n", &io)
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        connection
            .finish_copy_in(&io)
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert!(
        connection
            .abort_copy_in("synthetic", &io)
            .unwrap_err()
            .message
            .contains("mode")
    );
    assert_eq!(
        connection.identify_system(&io).unwrap().database_name,
        identity.database_name
    );
}

#[test]
#[ignore = "requires owned replication harness"]
fn start_failure_is_redacted_and_not_copyboth() {
    let io = control();
    let mut connection = logical(&io);
    let error = connection
        .start_replication("synthetic_private_missing", 0, &[], &io)
        .unwrap_err();
    assert!(error.message.contains("42704"));
    assert!(!error.message.contains("synthetic_private"));
    for (slot, options) in [
        ("bad;slot", vec![]),
        ("slot", vec![("key", Some("synthetic\0value"))]),
    ] {
        assert!(
            connection
                .start_replication(slot, 0, &options, &io)
                .is_err()
        );
    }
    assert!(connection.queue_feedback(status()).is_err());
    assert!(connection.request_replication_end().is_err());
    assert!(connection.poll_replication(io.deadline, &io).is_err());
    connection.identify_system(&io).unwrap();
}

#[test]
#[ignore = "requires owned replication harness"]
fn test_decoding_preserves_data_and_oversize_frame() {
    let io = control();
    let mut connection = logical(&io);
    let mut admin = normal("RPGMIGRATE_TEST_REPLICATION_ADMIN", &io);
    admin
        .query("CREATE TABLE replication_wide (value text)", &io)
        .unwrap();
    start(&mut connection, "replication_wide", &io);
    admin
        .query(
            "INSERT INTO replication_wide VALUES (repeat('z', 9 * 1024 * 1024))",
            &io,
        )
        .unwrap();
    loop {
        if let ReplicationPoll::Frame(frame) =
            connection.poll_replication(io.deadline, &io).unwrap()
        {
            assert_eq!(
                frame.as_bytes().first().copied(),
                Some(if frame.payload().is_some() {
                    b'w'
                } else {
                    b'k'
                })
            );
            if let Some(payload) = frame.payload()
                && payload.len() > 8 * 1024 * 1024
            {
                let prefix = b"table public.replication_wide: INSERT: value[text]:'";
                assert!(payload.starts_with(prefix));
                assert_eq!(payload.last(), Some(&b'\''));
                let value = &payload[prefix.len()..payload.len() - 1];
                assert_eq!(value.len(), 9 * 1024 * 1024);
                assert!(value.iter().all(|byte| *byte == b'z'));
                assert_eq!(frame.as_bytes().len(), payload.len() + 25);
                break;
            }
        }
    }
    finish(&mut connection, &io);
}

#[test]
#[ignore = "requires owned replication harness"]
fn keepalive_feedback_uses_only_supplied_positions() {
    let io = control();
    let (mut fault, mut connection) = Fault::start("keepalive", &io);
    start(&mut connection, "replication_keepalive", &io);
    let supplied = status();
    let encoded = hex(&supplied.encode());
    fault.command(json!({"op":"arm", "wal_end":u64::MAX, "feedback_hex":encoded}));
    loop {
        if let ReplicationPoll::Frame(frame) =
            connection.poll_replication(io.deadline, &io).unwrap()
            && matches!(
                frame.envelope(),
                ReplicationEnvelope::Keepalive {
                    wal_end: u64::MAX,
                    reply_requested: true,
                    ..
                }
            )
        {
            break;
        }
    }
    assert_eq!(fault.command(json!({"op":"status"}))["feedback_count"], 0);
    assert!(connection.queue_feedback(supplied).unwrap());
    assert!(
        !connection
            .queue_feedback(StandbyStatus {
                flush: u64::MAX,
                ..supplied
            })
            .unwrap()
    );
    loop {
        if let ReplicationPoll::FeedbackDrained(observed) =
            connection.poll_replication(io.deadline, &io).unwrap()
        {
            assert_eq!(observed, supplied);
            break;
        }
    }
    loop {
        let receipt = fault.command(json!({"op":"status"}));
        if receipt["feedback_count"].as_u64().unwrap() > 0 {
            assert_eq!(receipt["feedback_hex"], encoded);
            break;
        }
        connection
            .poll_replication(Instant::now() + Duration::from_millis(5), &io)
            .unwrap();
    }
    // The exact wire receipt proves the zero positions; no data or keepalive generates feedback.
    finish(&mut connection, &io);
    fault.complete(connection);
}

#[test]
#[ignore = "requires owned replication harness"]
fn duplex_backpressure_keeps_input_live() {
    let io = control();
    let (mut fault, mut connection) = Fault::start("duplex", &io);
    small_send_buffer(&connection).unwrap();
    start(&mut connection, "replication_duplex", &io);
    fault.command(json!({"op":"arm"}));
    let supplied = block_feedback(&mut connection, &io);
    let mut admin = normal("RPGMIGRATE_TEST_REPLICATION_ADMIN", &io);
    admin
        .query("CREATE TABLE replication_duplex (value integer)", &io)
        .unwrap();
    admin
        .query("INSERT INTO replication_duplex VALUES (42)", &io)
        .unwrap();
    loop {
        if let ReplicationPoll::Frame(frame) =
            connection.poll_replication(io.deadline, &io).unwrap()
            && frame.payload().is_some_and(|bytes| {
                bytes
                    .windows(b"replication_duplex".len())
                    .any(|part| part == b"replication_duplex")
            })
        {
            break;
        }
    }
    let held = fault.command(json!({"op":"status"}));
    assert_eq!(held["held"], true);
    assert!(held["server_frames_while_held"].as_u64().unwrap() > 0);
    let observed = flush_pending(&connection);
    assert!(observed > 0);
    fault.command(json!({"op":"release", "pqflush_pending":observed}));
    loop {
        if let ReplicationPoll::FeedbackDrained(observed) =
            connection.poll_replication(io.deadline, &io).unwrap()
        {
            assert_eq!(observed, supplied);
            break;
        }
    }
    finish(&mut connection, &io);
    fault.complete(connection);
}

#[test]
#[ignore = "requires owned replication harness"]
fn graceful_stop_drains_then_reidentifies() {
    let io = control();
    let mut connection = logical(&io);
    let identity = connection.identify_system(&io).unwrap();
    start(&mut connection, "replication_graceful", &io);
    assert!(connection.query("SELECT 1", &io).is_err());
    assert!(connection.identify_system(&io).is_err());
    assert!(
        connection
            .start_replication("replication_graceful", 0, &[], &io)
            .is_err()
    );
    assert!(connection.queue_feedback(status()).unwrap());
    connection.request_replication_end().unwrap();
    assert!(connection.queue_feedback(status()).is_err());
    let mut drained = false;
    loop {
        match connection.poll_replication(io.deadline, &io).unwrap() {
            ReplicationPoll::FeedbackDrained(value) => {
                assert_eq!(value, status());
                drained = true;
            }
            ReplicationPoll::End => break,
            _ => {}
        }
    }
    assert!(drained);
    assert_eq!(
        connection.identify_system(&io).unwrap().system_identifier,
        identity.system_identifier
    );
}

#[test]
#[ignore = "requires owned replication harness"]
fn terminal_error_or_missing_completion_never_returns_end() {
    for case in ["terminal_error", "missing_completion"] {
        let io = control();
        let (mut fault, mut connection) = Fault::start(case, &io);
        start(&mut connection, case, &io);
        fault.command(json!({"op":"arm"}));
        connection.request_replication_end().unwrap();
        let failure = loop {
            match connection.poll_replication(io.deadline, &io) {
                Ok(ReplicationPoll::End) => panic!("invalid terminal sequence returned End"),
                Ok(_) => {}
                Err(error) => break error,
            }
        };
        if case == "terminal_error" {
            assert!(failure.message.contains("57014"), "{failure:?}");
        } else {
            assert_eq!(failure.kind, crate::contracts::ErrorKind::Pgsql);
        }
        assert!(
            connection
                .identify_system(&control())
                .unwrap_err()
                .message
                .contains("closed")
        );
        fault.complete(connection);
    }
    let mut io = control();
    let (mut fault, mut connection) = Fault::start_phase("terminal_error", true, &io);
    small_send_buffer(&connection).unwrap();
    start(&mut connection, "replication_pending_error", &io);
    fault.command(json!({"op":"arm"}));
    block_feedback(&mut connection, &io);
    assert!(feedback_waiting_for_drain(&connection));
    io.deadline = Instant::now() + Duration::from_secs(5);
    let deadline = io.deadline;
    fault.command(json!({"op":"cancel", "pqflush_pending":flush_pending(&connection)}));
    let started = Instant::now();
    let mut feedback_drained = 0;
    let failure = loop {
        match connection.poll_replication(io.deadline, &io) {
            Ok(ReplicationPoll::FeedbackDrained(_)) => {
                feedback_drained += 1;
            }
            Ok(ReplicationPoll::End) => panic!("server error returned End"),
            Ok(_) => {}
            Err(error) => break error,
        }
    };
    assert!(failure.message.contains("57014"), "{failure:?}");
    let before_deadline = Instant::now() < deadline && started.elapsed() < Duration::from_secs(2);
    assert!(before_deadline);
    assert_eq!(feedback_drained, 0);
    assert_eq!(io.deadline, deadline);
    let closed = connection
        .identify_system(&control())
        .unwrap_err()
        .message
        .contains("closed");
    assert!(closed);
    fault.command(json!({"op":"client_error", "sqlstate":"57014", "closed":closed, "before_deadline":before_deadline, "feedback_drained":feedback_drained}));
    fault.complete(connection);
}

#[test]
#[ignore = "requires owned replication harness"]
fn replication_deadline_closes_connection() {
    let mut io = control();
    let mut connection = logical(&io);
    start(&mut connection, "replication_deadline", &io);
    io.deadline = Instant::now() + Duration::from_millis(30);
    let deadline = io.deadline;
    let failure = loop {
        match connection.poll_replication(Instant::now() + Duration::from_millis(5), &io) {
            Ok(ReplicationPoll::End) => panic!("idle stream unexpectedly ended"),
            Ok(_) => assert_eq!(io.deadline, deadline),
            Err(error) => break error,
        }
    };
    assert!(failure.message.contains("deadline"));
    assert!(
        connection
            .identify_system(&control())
            .unwrap_err()
            .message
            .contains("closed")
    );
}

#[test]
#[ignore = "requires owned replication harness"]
fn replication_cancellation_closes_connection() {
    let io = control();
    let mut connection = logical(&io);
    start(&mut connection, "replication_cancel", &io);
    let cancelled = Arc::clone(&io.cancelled);
    let trigger = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        cancelled.store(true, Ordering::Release);
    });
    let started = Instant::now();
    let failure = loop {
        match connection.poll_replication(io.deadline, &io) {
            Ok(ReplicationPoll::End) => panic!("idle stream unexpectedly ended"),
            Ok(_) => {}
            Err(error) => break error,
        }
    };
    trigger.join().unwrap();
    assert!(failure.message.contains("cancel"));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(
        connection
            .identify_system(&control())
            .unwrap_err()
            .message
            .contains("closed")
    );
}
