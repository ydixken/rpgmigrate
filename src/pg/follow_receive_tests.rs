use super::*;
use serde_json::Value;
use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub(crate) struct Peer {
    child: Child,
    output: BufReader<ChildStdout>,
}

impl Peer {
    pub(crate) fn start(case: &str, io: &IoControl) -> (Self, PgConnection) {
        let mut child = Command::new("python3")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/support/follow_receive_peer.py"
            ))
            .args(["--case", case])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        assert!(output.read_line(&mut line).unwrap() > 0);
        let ready: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(ready["ready"], true);
        let port = ready["port"].as_u64().unwrap();
        let conninfo = Conninfo::new(format!("host=127.0.0.1 port={port} dbname=postgres user=receiver sslmode=disable gssencmode=disable connect_timeout=3").into()).unwrap();
        let mut source = PgConnection::connect_replication(&conninfo, io).unwrap();
        source
            .start_replication(
                "receiver",
                0,
                &[
                    ("proto_version", Some("1")),
                    ("publication_names", Some("receiver")),
                ],
                io,
            )
            .unwrap();
        (Self { child, output }, source)
    }

    fn finish(self, source: PgConnection) -> Value {
        drop(source);
        self.finish_closed()
    }

    pub(crate) fn ending(&mut self) {
        let mut line = String::new();
        assert!(self.output.read_line(&mut line).unwrap() > 0);
        let receipt: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(receipt["copy_done_waiting"], true);
        assert_eq!(receipt["positions_unchanged"], true);
    }

    pub(crate) fn requested(&mut self) {
        let mut line = String::new();
        assert!(self.output.read_line(&mut line).unwrap() > 0);
        let receipt: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(receipt["reply_requested"], true);
        assert_eq!(receipt["positions_unchanged"], true);
    }

    pub(crate) fn blocked(&mut self, minimum: u64) {
        let mut line = String::new();
        assert!(self.output.read_line(&mut line).unwrap() > 0);
        let receipt: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(receipt["blocked_status_count"], minimum);
        assert_eq!(receipt["positions_unchanged"], true);
    }

    pub(crate) fn finish_closed(mut self) -> Value {
        let mut line = String::new();
        assert!(self.output.read_line(&mut line).unwrap() > 0);
        let receipt: Value = serde_json::from_str(&line).unwrap();
        assert!(self.child.wait().unwrap().success());
        assert_eq!(receipt["status"], "observed");
        assert_eq!(receipt["closed"], true);
        receipt
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

pub(crate) fn pending_feedback(source: &mut PgConnection, io: &IoControl) {
    super::ffi::replication_testing::small_send_buffer(source).unwrap();
    for _ in 0..200_000 {
        source.queue_feedback(status()).unwrap();
        assert!(matches!(
            source
                .poll_replication_paused(Instant::now() + Duration::from_millis(1), io)
                .unwrap(),
            ReplicationPoll::Wake | ReplicationPoll::FeedbackDrained(_)
        ));
        if super::ffi::replication_testing::flush_pending(source) > 0
            && super::ffi::replication_testing::feedback_waiting_for_drain(source)
        {
            return;
        }
    }
    panic!("owned peer did not block actual PQflush");
}

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(5),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn status() -> StandbyStatus {
    StandbyStatus {
        write: 17,
        flush: 17,
        apply: 17,
        client_time_micros: 0,
        request_reply: false,
    }
}

#[test]
fn paused_reads_send_status_and_preserve_one_split_input_frame() {
    let io = control();
    let (peer, mut source) = Peer::start("paused", &io);
    let end = Instant::now() + Duration::from_millis(150);
    let mut drained = 0;
    while Instant::now() < end {
        source.queue_feedback(status()).unwrap();
        match source
            .poll_replication_paused(Instant::now() + Duration::from_millis(10), &io)
            .unwrap()
        {
            ReplicationPoll::FeedbackDrained(observed) => {
                assert_eq!(observed, status());
                drained += 1;
            }
            ReplicationPoll::Wake => {}
            _ => panic!("paused transport consumed source input"),
        }
    }
    assert!(drained > 1);
    loop {
        if let ReplicationPoll::Frame(frame) = source.poll_replication(io.deadline, &io).unwrap() {
            assert_eq!(frame.payload().unwrap(), b"partial-frame-body");
            assert_eq!(
                frame.envelope(),
                ReplicationEnvelope::XLogData {
                    wal_start: 500,
                    wal_end: 900,
                    server_time_micros: 0
                }
            );
            break;
        }
    }
    let receipt = peer.finish(source);
    assert!(receipt["feedback_count"].as_u64().unwrap() > 1);
    assert_eq!(receipt["positions_unchanged"], true);
}

#[test]
fn replication_notice_is_fatal_and_redacted() {
    let io = control();
    let (peer, mut source) = Peer::start("notice", &io);
    source.queue_feedback(status()).unwrap();
    let failure = loop {
        match source.poll_replication(io.deadline, &io) {
            Ok(ReplicationPoll::FeedbackDrained(_)) => {}
            Err(failure) => break failure,
            _ => panic!("replication notice was accepted"),
        }
    };
    assert!(failure.message.contains("notice"));
    assert!(!failure.message.contains("private-peer-value"));
    assert!(source.backend_pid().is_err());
    peer.finish(source);
}

#[test]
fn paused_poll_honors_cancellation_and_absolute_deadline() {
    for cancelled in [false, true] {
        let mut io = control();
        let (peer, mut source) = Peer::start("paused", &io);
        if cancelled {
            io.cancelled.store(true, Ordering::Release);
        } else {
            io.deadline = Instant::now();
        }
        let before = Instant::now();
        let failure = source
            .poll_replication_paused(before + Duration::from_secs(1), &io)
            .err()
            .unwrap();
        assert!(
            failure
                .message
                .contains(if cancelled { "cancel" } else { "deadline" })
        );
        assert!(before.elapsed() < Duration::from_millis(200));
        assert!(source.backend_pid().is_err());
        peer.finish(source);
    }
}

#[test]
fn sender_eof_is_an_error() {
    let io = control();
    let (peer, mut source) = Peer::start("eof", &io);
    let failure = source.poll_replication(io.deadline, &io).err().unwrap();
    assert!(failure.message.contains("connection"));
    peer.finish(source);
}

#[test]
fn paused_status_write_backpressure_retains_the_pending_status() {
    let io = control();
    let (peer, mut source) = Peer::start("blocked", &io);
    pending_feedback(&mut source, &io);
    assert!(!source.queue_feedback(status()).unwrap());
    loop {
        match source.poll_replication_paused(io.deadline, &io).unwrap() {
            ReplicationPoll::FeedbackDrained(observed) => {
                assert_eq!(observed, status());
                break;
            }
            ReplicationPoll::Wake => {}
            _ => panic!("blocked output consumed input"),
        }
    }
    assert!(peer.finish(source)["feedback_count"].as_u64().unwrap() > 0);
}

#[test]
#[ignore = "requires an owned PostgreSQL 16 or 18 receiver fixture"]
fn live_pipeline_commands_counts_dispatched_commands() {
    std::env::var_os("RPGMIGRATE_TEST_FOLLOW_RECEIVE_CONTEXT")
        .expect("owned receiver context required");
    let conninfo =
        Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_FOLLOW_RECEIVE_SOURCE").unwrap()).unwrap();
    let io = control();
    let mut connection = PgConnection::connect(&conninfo, &io).unwrap();
    connection.query("BEGIN", &io).unwrap();
    let mut pipeline: Pipeline<'_> = connection.pipeline(&io).unwrap();
    assert_eq!(pipeline.commands(), 0);
    pipeline.query("SELECT 1", &io).unwrap();
    assert_eq!(pipeline.commands(), 1);
    pipeline.query("SELECT 2", &io).unwrap();
    assert_eq!(pipeline.commands(), 2);
    let mut rows = 0;
    pipeline
        .sync(&io, |index, result| {
            assert!(index < 2);
            if result.rows() > 0 {
                rows += 1;
                assert_eq!(result.rows(), 1);
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(rows, 2);
    assert_eq!(
        connection.commit_observed(&io).unwrap(),
        CommitObservation::Committed
    );
    for command in [
        "COMMIT",
        "END",
        "ROLLBACK",
        "ABORT",
        "BEGIN",
        "START TRANSACTION",
        "SAVEPOINT escaped",
        "/* leading */ COMMIT",
    ] {
        let failure = connection
            .prepare("blocked_transaction", command, &[], &io)
            .unwrap_err();
        assert!(failure.message.contains("internal data"));
        let prepared = connection.query("SELECT count(*) FROM pg_catalog.pg_prepared_statements WHERE name='blocked_transaction'", &io).unwrap();
        assert_eq!(prepared.cell(0, 0).unwrap(), Some(b"0".as_slice()));
    }
    let mut observer = PgConnection::connect(&conninfo, &io).unwrap();
    let table = format!("receive_pipeline_guard_{}", std::process::id());
    observer
        .query(&format!("CREATE TABLE public.{table}(id integer)"), &io)
        .unwrap();
    for command in [
        "COMMIT",
        "END",
        "ROLLBACK",
        "ABORT",
        "START TRANSACTION",
        "SAVEPOINT escaped",
        "/* leading */ COMMIT",
        "-- leading\nCOMMIT",
    ] {
        let mut target = PgConnection::connect(&conninfo, &io).unwrap();
        target.query("BEGIN", &io).unwrap();
        let mut pipeline = target.pipeline(&io).unwrap();
        pipeline
            .query(&format!("INSERT INTO public.{table} VALUES(1)"), &io)
            .unwrap();
        let accepted = pipeline.query(command, &io).is_ok();
        if accepted {
            pipeline
                .query(&format!("INSERT INTO public.{table} VALUES(2)"), &io)
                .unwrap();
            let _ = pipeline.sync(&io, |_, _| Ok(()));
        } else {
            drop(pipeline);
        }
        let rows = observer
            .query(&format!("SELECT count(*) FROM public.{table}"), &io)
            .unwrap();
        assert_eq!(
            rows.cell(0, 0).unwrap(),
            Some(b"0".as_slice()),
            "pipeline command escaped its transaction: {command}"
        );
        assert!(
            !accepted,
            "pipeline transaction control reached dispatch: {command}"
        );
    }
    let mut target = PgConnection::connect(&conninfo, &io).unwrap();
    target.query("BEGIN", &io).unwrap();
    let mut pipeline = target.pipeline(&io).unwrap();
    pipeline
        .query(&format!("INSERT INTO public.{table} VALUES(3)"), &io)
        .unwrap();
    pipeline.query("SELECT 1; COMMIT", &io).unwrap();
    assert!(
        pipeline
            .sync(&io, |_, _| Ok(()))
            .unwrap_err()
            .message
            .contains("42601")
    );
    target.query("ROLLBACK", &io).unwrap();
    let rows = observer
        .query(&format!("SELECT count(*) FROM public.{table}"), &io)
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(b"0".as_slice()));
    observer
        .query(&format!("DROP TABLE public.{table}"), &io)
        .unwrap();
}
