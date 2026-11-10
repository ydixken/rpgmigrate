use crate::apply::checkpoint::{RecoveryStart, SafeFeedback};
use crate::cdc::contracts::{FollowMessage, Plugin};
use crate::contracts::{
    EffectiveFollowSettings, Error, ErrorKind, FollowBinding, FollowOptions, IoControl, Result,
};
use crate::follow::channel::{FeedbackReceiver, FollowSender};
use crate::identity::{SourceEndLsn, StreamId};
use crate::pg::{PgConnection, ReplicationEnvelope, ReplicationPoll, StandbyStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const STATUS_INTERVAL: Duration = Duration::from_secs(10);
const SERVICE_INTERVAL: Duration = Duration::from_millis(50);

#[expect(
    clippy::too_many_arguments,
    reason = "The worker contract keeps signals explicit"
)]
pub(crate) fn run(
    options: &FollowOptions,
    settings: &EffectiveFollowSettings,
    binding: &FollowBinding,
    start: RecoveryStart,
    output: &FollowSender,
    feedback: &FeedbackReceiver,
    ready: SyncSender<i32>,
    request_keepalive: &AtomicBool,
    stop: &AtomicBool,
    io: &IoControl,
) -> Result<()> {
    settings.validate()?;
    let stream = binding.stream();
    if start.stream() != stream
        || binding.slot.intent.binding.plugin() != Plugin::Pgoutput
        || (!options.slot_name.is_empty()
            && options.slot_name != binding.slot.intent.binding.slot_name())
        || (!options.publication_name.is_empty()
            && options.publication_name != binding.publication.name)
        || start.start_lsn() < start.confirmed_flush_lsn()
    {
        return Err(gap("follow receiver binding or recovery position differs"));
    }
    let mut source = PgConnection::connect_replication(&options.clone.source, io)?;
    configure(&mut source, io)?;
    let identity = source.identify_system(io)?;
    let database = source.logical_query(
        "SELECT oid::text FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()",
        io,
    )?;
    let expected_oid = stream.source.database_oid.to_string();
    if identity.system_identifier != stream.source.system_identifier
        || identity.timeline != binding.source_timeline
        || identity.database_name.is_empty()
        || database.rows() != 1
        || database.columns()? != 1
        || database.cell(0, 0)? != Some(expected_oid.as_bytes())
    {
        return Err(gap("follow source identity or timeline differs"));
    }
    let publication = source.quote_identifier(&binding.publication.name)?;
    let mut decoder = super::pgoutput::Decoder::new(stream, binding, settings.limits)?;
    let mut health = PgConnection::connect(&options.clone.source, io)?;
    source.start_replication(
        binding.slot.intent.binding.slot_name(),
        start.start_lsn().0,
        &[
            ("proto_version", Some("1")),
            ("publication_names", Some(&publication)),
        ],
        io,
    )?;
    let pid = i32::try_from(source.backend_pid()?)
        .ok()
        .filter(|pid| *pid > 0)
        .ok_or_else(|| gap("follow source backend identity is invalid"))?;
    let floor = crate::catalog::follow_start_confirmed_flush(&mut health, binding, pid, io)?;
    if floor < start.confirmed_flush_lsn() || floor > start.start_lsn() {
        return Err(gap(
            "follow source slot advanced beyond recovery or its captured floor regressed",
        ));
    }
    drop(health);
    ready
        .send(pid)
        .map_err(|_| gap("follow receiver readiness was abandoned"))?;
    let mut lease = ReceiveLease {
        status: Status::new(stream, floor, Instant::now(), STATUS_INTERVAL),
        request_keepalive,
        stop,
    };
    receive(
        &mut source,
        &mut decoder,
        output,
        feedback,
        &mut lease,
        settings.limits.max_message_bytes,
        io,
    )
}

fn configure(source: &mut PgConnection, io: &IoControl) -> Result<()> {
    let version = source.server_version()? / 10_000;
    if !matches!(version, 16 | 18) {
        return Err(gap("follow source major is unsupported"));
    }
    require_setting(source, "server_encoding", "UTF8", io)?;
    let transport = source.logical_query("SELECT pg_catalog.inet_client_addr() IS NULL", io)?;
    if transport.rows() != 1 || transport.columns()? != 1 {
        return Err(gap("follow source transport setting is invalid"));
    }
    let unix_socket = match transport.cell(0, 0)? {
        Some(b"t") => true,
        Some(b"f") => false,
        _ => return Err(gap("follow source transport setting is invalid")),
    };
    for (name, value) in [
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, YMD"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("lc_monetary", "C"),
        ("extra_float_digits", "3"),
        ("bytea_output", "hex"),
        ("row_security", "off"),
        ("search_path", "pg_catalog"),
        ("statement_timeout", "0"),
        ("idle_in_transaction_session_timeout", "0"),
        ("wal_sender_timeout", "60s"),
        ("tcp_keepalives_idle", "60"),
        ("tcp_keepalives_interval", "10"),
        ("tcp_keepalives_count", "5"),
    ] {
        let result = source.logical_query(&format!("SET {name} TO '{value}'"), io)?;
        if result.command_tag()? != "SET" {
            return Err(gap("follow source setting was not confirmed"));
        }
        let expected = if name == "wal_sender_timeout" {
            "1min"
        } else if unix_socket && name.starts_with("tcp_keepalives_") {
            "0"
        } else {
            value
        };
        require_setting(source, name, expected, io)?;
    }
    if version >= 17 {
        let result = source.logical_query("SET transaction_timeout TO 0", io)?;
        if result.command_tag()? != "SET" {
            return Err(gap("follow source timeout was not confirmed"));
        }
        require_setting(source, "transaction_timeout", "0", io)?;
    }
    Ok(())
}

fn require_setting(
    source: &mut PgConnection,
    name: &str,
    value: &str,
    io: &IoControl,
) -> Result<()> {
    let result = source.logical_query(&format!("SHOW {name}"), io)?;
    if result.rows() != 1 || result.columns()? != 1 || result.cell(0, 0)? != Some(value.as_bytes())
    {
        return Err(gap("follow source session setting differs"));
    }
    Ok(())
}

struct ReceiveLease<'a> {
    status: Status,
    request_keepalive: &'a AtomicBool,
    stop: &'a AtomicBool,
}

fn receive(
    source: &mut PgConnection,
    decoder: &mut super::pgoutput::Decoder,
    output: &FollowSender,
    feedback: &FeedbackReceiver,
    lease: &mut ReceiveLease<'_>,
    max_frame_bytes: usize,
    io: &IoControl,
) -> Result<()> {
    loop {
        check(io)?;
        lease
            .status
            .service(source, feedback, lease.request_keepalive)?;
        if lease.stop.load(Ordering::Acquire) {
            return stop(source, feedback, lease, io);
        }
        match source.poll_replication(lease.status.wake(), io)? {
            ReplicationPoll::Frame(frame) => {
                if frame.as_bytes().len() > max_frame_bytes {
                    return Err(gap(
                        "follow replication frame exceeds the supported sixteen MiB budget",
                    ));
                }
                match frame.envelope() {
                    ReplicationEnvelope::XLogData { .. } => {
                        decoder.feed(
                            frame
                                .payload()
                                .ok_or_else(|| gap("follow data payload is missing"))?,
                            &mut |message| admit(message, source, output, feedback, lease, io),
                        )?;
                    }
                    ReplicationEnvelope::Keepalive {
                        wal_end,
                        reply_requested,
                        ..
                    } => {
                        if reply_requested {
                            lease.status.pending = true;
                            lease
                                .status
                                .service(source, feedback, lease.request_keepalive)?;
                        }
                        admit(
                            FollowMessage::Keepalive {
                                wal_end: SourceEndLsn(wal_end),
                                decoder_idle: decoder.is_idle(),
                            },
                            source,
                            output,
                            feedback,
                            lease,
                            io,
                        )?;
                    }
                }
            }
            ReplicationPoll::FeedbackDrained(observed) => lease.status.check_drained(observed)?,
            ReplicationPoll::Wake => {}
            ReplicationPoll::End => {
                return Err(gap(
                    "follow source replication ended before a drain boundary",
                ));
            }
        }
    }
}

fn admit(
    mut message: FollowMessage,
    source: &mut PgConnection,
    output: &FollowSender,
    feedback: &FeedbackReceiver,
    lease: &mut ReceiveLease<'_>,
    io: &IoControl,
) -> Result<()> {
    output.validate_message(&message)?;
    loop {
        check(io)?;
        if lease.stop.load(Ordering::Acquire) {
            return Ok(());
        }
        match output.try_send(message) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Disconnected(_)) => return Err(gap("follow apply channel closed")),
            Err(TrySendError::Full(pending)) => message = pending,
        }
        if !lease.status.backpressure_reported {
            let snapshot = output.full_snapshot()?;
            crate::protocol::follow_backpressure(
                snapshot.limits,
                snapshot.queued_messages,
                snapshot.queued_bytes,
            )?;
            lease.status.backpressure_reported = true;
        }
        lease
            .status
            .service(source, feedback, lease.request_keepalive)?;
        match source.poll_replication_paused(lease.status.wake(), io)? {
            ReplicationPoll::FeedbackDrained(observed) => lease.status.check_drained(observed)?,
            ReplicationPoll::Wake => {}
            ReplicationPoll::End => return Err(gap("follow source ended while apply was blocked")),
            ReplicationPoll::Frame(_) => return Err(gap("follow paused transport consumed input")),
        }
    }
}

fn stop(
    source: &mut PgConnection,
    feedback: &FeedbackReceiver,
    lease: &mut ReceiveLease<'_>,
    io: &IoControl,
) -> Result<()> {
    let io = &crate::pg::operation_control(io);
    let mut ending = false;
    loop {
        check(io)?;
        if !ending {
            lease
                .status
                .service(source, feedback, lease.request_keepalive)?;
            if !lease.status.pending && !lease.status.reply_pending {
                source.request_replication_end()?;
                ending = true;
            }
        }
        match source.poll_replication(lease.status.wake(), io)? {
            ReplicationPoll::FeedbackDrained(observed) => lease.status.check_drained(observed)?,
            ReplicationPoll::Frame(_) => {
                // The source slot retains unapplied input; shutdown grants no feedback authority.
            }
            ReplicationPoll::Wake => {}
            ReplicationPoll::End if ending => return check(io),
            ReplicationPoll::End => {
                return Err(gap("follow source ended before requested shutdown"));
            }
        }
    }
}

struct Status {
    stream: StreamId,
    proven: SourceEndLsn,
    floor: SourceEndLsn,
    next: Instant,
    interval: Duration,
    pending: bool,
    reply_pending: bool,
    backpressure_reported: bool,
}

impl Status {
    fn new(stream: StreamId, floor: SourceEndLsn, now: Instant, interval: Duration) -> Self {
        Self {
            stream,
            proven: floor,
            floor,
            next: now,
            interval,
            pending: true,
            reply_pending: false,
            backpressure_reported: false,
        }
    }

    fn request_keepalive(&mut self, request: &AtomicBool) {
        self.reply_pending |= request.swap(false, Ordering::AcqRel);
    }

    fn accept(&mut self, proof: SafeFeedback) -> Result<()> {
        if proof.stream() != self.stream || proof.end() < self.proven {
            return Err(gap(
                "follow feedback changed stream or moved below its proven floor",
            ));
        }
        if proof.end() > self.proven {
            self.pending = true;
        }
        self.proven = proof.end();
        Ok(())
    }

    fn check_drained(&self, observed: StandbyStatus) -> Result<()> {
        if observed.write != observed.flush
            || observed.flush != observed.apply
            || observed.apply < self.floor.0
            || observed.apply > self.proven.0
        {
            return Err(gap(
                "follow drained status differs from proven feedback bounds",
            ));
        }
        Ok(())
    }

    fn wake(&self) -> Instant {
        let now = Instant::now();
        if self.next > now {
            self.next.min(now + SERVICE_INTERVAL)
        } else {
            now + SERVICE_INTERVAL
        }
    }

    fn service(
        &mut self,
        source: &mut PgConnection,
        feedback: &FeedbackReceiver,
        request_keepalive: &AtomicBool,
    ) -> Result<()> {
        self.request_keepalive(request_keepalive);
        if let Some(proof) = feedback.take() {
            self.accept(proof)?;
        }
        let now = Instant::now();
        self.submit(now, timestamp()?, |status| source.queue_feedback(status))
    }

    fn submit(
        &mut self,
        now: Instant,
        client_time_micros: i64,
        mut queue: impl FnMut(StandbyStatus) -> Result<bool>,
    ) -> Result<()> {
        if (self.pending || self.reply_pending || now >= self.next)
            && queue(StandbyStatus {
                write: self.proven.0,
                flush: self.proven.0,
                apply: self.proven.0,
                client_time_micros,
                request_reply: self.reply_pending,
            })?
        {
            self.pending = false;
            self.reply_pending = false;
            self.next = now + self.interval;
        }
        Ok(())
    }
}

fn timestamp() -> Result<i64> {
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| gap("follow clock precedes the Unix epoch"))?
        .as_micros();
    i64::try_from(unix)
        .ok()
        .and_then(|time| time.checked_sub(946_684_800_000_000))
        .ok_or_else(|| gap("follow timestamp exceeds its supported range"))
}

fn check(io: &IoControl) -> Result<()> {
    if io.cancelled.load(Ordering::Acquire) {
        return Err(gap("follow receive cancelled"));
    }
    if io.is_expired() {
        return Err(gap("follow receive deadline exceeded"));
    }
    Ok(())
}

#[track_caller]
fn gap(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{RunId, SourceId};
    use std::sync::atomic::AtomicBool;

    fn stream() -> StreamId {
        StreamId {
            run: RunId([1; 16]),
            source: SourceId {
                system_identifier: 2,
                database_oid: 3,
            },
            generation: 1,
        }
    }

    #[test]
    fn status_retries_without_losing_newer_proof_or_raising_unproven_positions() {
        let now = Instant::now();
        let mut status = Status::new(stream(), SourceEndLsn(10), now, Duration::from_secs(10));
        status
            .submit(now, 1, |sent| {
                assert_eq!((sent.write, sent.flush, sent.apply), (10, 10, 10));
                Ok(false)
            })
            .unwrap();
        assert!(status.pending);
        status
            .accept(crate::apply::checkpoint::feedback_for_test(
                stream(),
                SourceEndLsn(20),
            ))
            .unwrap();
        status
            .submit(now, 2, |sent| {
                assert_eq!((sent.write, sent.flush, sent.apply), (20, 20, 20));
                Ok(true)
            })
            .unwrap();
        assert!(!status.pending);
        let drained = StandbyStatus {
            write: 10,
            flush: 10,
            apply: 10,
            client_time_micros: 1,
            request_reply: false,
        };
        status.check_drained(drained).unwrap();
        for position in [9, 21] {
            assert!(
                status
                    .check_drained(StandbyStatus {
                        write: position,
                        flush: position,
                        apply: position,
                        ..drained
                    })
                    .is_err()
            );
        }
        assert!(
            status
                .check_drained(StandbyStatus {
                    apply: 11,
                    ..drained
                })
                .is_err()
        );
        assert_eq!(status.proven, SourceEndLsn(20));
        assert_eq!(status.next, now + Duration::from_secs(10));
        status
            .submit(now + Duration::from_secs(10), 3, |sent| {
                assert_eq!((sent.write, sent.flush, sent.apply), (20, 20, 20));
                Ok(true)
            })
            .unwrap();
        assert!(
            status
                .accept(crate::apply::checkpoint::feedback_for_test(
                    stream(),
                    SourceEndLsn(19)
                ))
                .is_err()
        );
        let mut foreign = stream();
        foreign.generation += 1;
        assert!(
            status
                .accept(crate::apply::checkpoint::feedback_for_test(
                    foreign,
                    SourceEndLsn(30)
                ))
                .is_err()
        );
    }
    #[test]
    fn drain_request_retries_and_coalesces_without_advancing_feedback() {
        let now = Instant::now();
        let mut status = Status::new(stream(), SourceEndLsn(17), now, STATUS_INTERVAL);
        status.submit(now, 0, |_| Ok(true)).unwrap();
        let request = AtomicBool::new(true);
        status.request_keepalive(&request);
        assert!(!request.load(Ordering::Acquire));
        status
            .submit(now + Duration::from_millis(1), 1, |sent| {
                assert!(sent.request_reply);
                assert_eq!((sent.write, sent.flush, sent.apply), (17, 17, 17));
                Ok(false)
            })
            .unwrap();
        request.store(true, Ordering::Release);
        status.request_keepalive(&request);
        status
            .submit(now + Duration::from_millis(2), 2, |sent| {
                assert!(sent.request_reply);
                assert_eq!((sent.write, sent.flush, sent.apply), (17, 17, 17));
                Ok(true)
            })
            .unwrap();
        status
            .submit(now + Duration::from_millis(3), 3, |_| {
                panic!("accepted coalesced request was sent twice")
            })
            .unwrap();
        status
            .submit(
                now + STATUS_INTERVAL + Duration::from_millis(2),
                4,
                |sent| {
                    assert!(!sent.request_reply);
                    assert_eq!((sent.write, sent.flush, sent.apply), (17, 17, 17));
                    Ok(true)
                },
            )
            .unwrap();
    }

    #[test]
    fn completed_stop_requires_checked_terminal_results() {
        for case in ["stop_idle", "stop_error", "stop_extra"] {
            let io = IoControl {
                deadline: Instant::now() + Duration::from_secs(5),
                cancelled: std::sync::Arc::new(AtomicBool::new(false)),
            };
            let limits = crate::contracts::FollowLimits::default();
            let (sender, receiver) = crate::follow::channel::bounded(limits).unwrap();
            let (_proofs, feedback) = crate::follow::channel::feedback();
            let (peer, mut source) = crate::pg::follow_receive_tests::Peer::start(case, &io);
            let mut decoder =
                super::super::pgoutput::Decoder::new(stream(), &binding(), limits).unwrap();
            let request_keepalive = AtomicBool::new(false);
            let stop = AtomicBool::new(true);
            let mut lease = ReceiveLease {
                status: Status::new(stream(), SourceEndLsn(17), Instant::now(), STATUS_INTERVAL),
                request_keepalive: &request_keepalive,
                stop: &stop,
            };
            let result = receive(
                &mut source,
                &mut decoder,
                &sender,
                &feedback,
                &mut lease,
                limits.max_message_bytes,
                &io,
            );
            if case == "stop_idle" {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().kind, ErrorKind::Pgsql);
            }
            assert!(!io.cancelled.load(Ordering::Acquire));
            assert_eq!(lease.status.proven, SourceEndLsn(17));
            assert_eq!(
                receiver.recv_timeout(Duration::ZERO),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            );
            drop(source);
            let receipt = peer.finish_closed();
            assert_eq!(receipt["copy_done"], true);
            assert_eq!(receipt["reply_requested_count"], 0);
        }
    }

    #[test]
    fn idle_drain_request_wakes_before_the_status_timer_without_advancing_feedback() {
        let request = std::sync::Arc::new(AtomicBool::new(false));
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let worker_request = std::sync::Arc::clone(&request);
        let worker_stop = std::sync::Arc::clone(&stop);
        let (ready, readiness) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let io = IoControl {
                deadline: Instant::now() + Duration::from_secs(5),
                cancelled: std::sync::Arc::new(AtomicBool::new(false)),
            };
            let limits = crate::contracts::FollowLimits::default();
            let (sender, _receiver) = crate::follow::channel::bounded(limits).unwrap();
            let (_proofs, feedback) = crate::follow::channel::feedback();
            let (peer, mut source) = crate::pg::follow_receive_tests::Peer::start("stop_idle", &io);
            let mut decoder =
                super::super::pgoutput::Decoder::new(stream(), &binding(), limits).unwrap();
            let mut lease = ReceiveLease {
                status: Status::new(stream(), SourceEndLsn(17), Instant::now(), STATUS_INTERVAL),
                request_keepalive: &worker_request,
                stop: &worker_stop,
            };
            lease
                .status
                .service(&mut source, &feedback, &worker_request)
                .unwrap();
            loop {
                if let ReplicationPoll::FeedbackDrained(observed) =
                    source.poll_replication_paused(io.deadline, &io).unwrap()
                {
                    lease.status.check_drained(observed).unwrap();
                    break;
                }
            }
            ready.send(peer).unwrap();
            receive(
                &mut source,
                &mut decoder,
                &sender,
                &feedback,
                &mut lease,
                limits.max_message_bytes,
                &io,
            )
            .unwrap();
            assert!(!io.cancelled.load(Ordering::Acquire));
            assert_eq!(lease.status.proven, SourceEndLsn(17));
        });
        let mut peer = readiness.recv_timeout(Duration::from_secs(2)).unwrap();
        let before = Instant::now();
        request.store(true, Ordering::Release);
        peer.requested();
        assert!(before.elapsed() < Duration::from_secs(1));
        stop.store(true, Ordering::Release);
        worker.join().unwrap();
        let receipt = peer.finish_closed();
        assert_eq!(receipt["copy_done"], true);
        assert_eq!(receipt["reply_requested_count"], 1);
    }

    #[test]
    fn completed_stop_during_partial_later_input_checks_end_without_decoding_the_tail() {
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let worker_stop = std::sync::Arc::clone(&stop);
        let limits = crate::contracts::FollowLimits::default();
        let (sender, receiver) = crate::follow::channel::bounded(limits).unwrap();
        let (ready, readiness) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let io = IoControl {
                deadline: Instant::now() + Duration::from_secs(5),
                cancelled: std::sync::Arc::new(AtomicBool::new(false)),
            };
            let (_proofs, feedback) = crate::follow::channel::feedback();
            let (peer, mut source) =
                crate::pg::follow_receive_tests::Peer::start("stop_partial", &io);
            ready.send(peer).unwrap();
            let mut decoder =
                super::super::pgoutput::Decoder::new(stream(), &binding(), limits).unwrap();
            let request = AtomicBool::new(false);
            let mut lease = ReceiveLease {
                status: Status::new(stream(), SourceEndLsn(17), Instant::now(), STATUS_INTERVAL),
                request_keepalive: &request,
                stop: &worker_stop,
            };
            receive(
                &mut source,
                &mut decoder,
                &sender,
                &feedback,
                &mut lease,
                limits.max_message_bytes,
                &io,
            )
            .unwrap();
            assert!(!decoder.is_idle());
            assert!(!io.cancelled.load(Ordering::Acquire));
            assert_eq!(lease.status.proven, SourceEndLsn(17));
        });
        let peer = readiness.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(matches!(
            receiver.recv_timeout(Duration::from_secs(1)).unwrap(),
            FollowMessage::Begin(begin) if begin.xid == 9
        ));
        stop.store(true, Ordering::Release);
        worker.join().unwrap();
        assert_eq!(
            receiver.recv_timeout(Duration::ZERO),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
        );
        assert_eq!(peer.finish_closed()["copy_done"], true);
    }

    #[test]
    fn completed_stop_drains_pending_status_before_ending_and_keeps_reply_intent() {
        let io = IoControl {
            deadline: Instant::now() + Duration::from_secs(5),
            cancelled: std::sync::Arc::new(AtomicBool::new(false)),
        };
        let limits = crate::contracts::FollowLimits::default();
        let (sender, _receiver) = crate::follow::channel::bounded(limits).unwrap();
        let (_proofs, feedback) = crate::follow::channel::feedback();
        let (peer, mut source) = crate::pg::follow_receive_tests::Peer::start("blocked", &io);
        crate::pg::follow_receive_tests::pending_feedback(&mut source, &io);
        let mut decoder =
            super::super::pgoutput::Decoder::new(stream(), &binding(), limits).unwrap();
        let request = AtomicBool::new(true);
        let stop = AtomicBool::new(true);
        let mut lease = ReceiveLease {
            status: Status::new(stream(), SourceEndLsn(17), Instant::now(), STATUS_INTERVAL),
            request_keepalive: &request,
            stop: &stop,
        };
        receive(
            &mut source,
            &mut decoder,
            &sender,
            &feedback,
            &mut lease,
            limits.max_message_bytes,
            &io,
        )
        .unwrap();
        assert!(!lease.status.reply_pending);
        assert_eq!(lease.status.proven, SourceEndLsn(17));
        drop(source);
        let receipt = peer.finish_closed();
        assert_eq!(receipt["copy_done"], true);
        assert_eq!(receipt["reply_requested_count"], 1);
    }

    #[test]
    fn completed_stop_waiting_for_terminals_preserves_deadline_and_cancellation() {
        for cancel in [false, true] {
            let cancelled = std::sync::Arc::new(AtomicBool::new(false));
            let worker_cancelled = std::sync::Arc::clone(&cancelled);
            let (ready, readiness) = std::sync::mpsc::sync_channel(1);
            let worker = std::thread::spawn(move || {
                let mut io = IoControl {
                    deadline: Instant::now() + Duration::from_secs(5),
                    cancelled: worker_cancelled,
                };
                let (peer, mut source) =
                    crate::pg::follow_receive_tests::Peer::start("stop_never", &io);
                if !cancel {
                    io.deadline = Instant::now() + Duration::from_millis(200);
                }
                ready.send(peer).unwrap();
                let limits = crate::contracts::FollowLimits::default();
                let (sender, _receiver) = crate::follow::channel::bounded(limits).unwrap();
                let (_proofs, feedback) = crate::follow::channel::feedback();
                let mut decoder =
                    super::super::pgoutput::Decoder::new(stream(), &binding(), limits).unwrap();
                let request = AtomicBool::new(false);
                let stop = AtomicBool::new(true);
                let mut lease = ReceiveLease {
                    status: Status::new(
                        stream(),
                        SourceEndLsn(17),
                        Instant::now(),
                        STATUS_INTERVAL,
                    ),
                    request_keepalive: &request,
                    stop: &stop,
                };
                receive(
                    &mut source,
                    &mut decoder,
                    &sender,
                    &feedback,
                    &mut lease,
                    limits.max_message_bytes,
                    &io,
                )
                .unwrap_err()
            });
            let mut peer = readiness.recv_timeout(Duration::from_secs(2)).unwrap();
            peer.ending();
            let before = Instant::now();
            if cancel {
                cancelled.store(true, Ordering::Release);
            }
            let failure = worker.join().unwrap();
            assert!(
                failure
                    .message
                    .contains(if cancel { "cancel" } else { "deadline" })
            );
            assert!(before.elapsed() < Duration::from_secs(1));
            assert_eq!(cancelled.load(Ordering::Acquire), cancel);
            assert_eq!(peer.finish_closed()["copy_done"], true);
        }
    }

    fn binding() -> FollowBinding {
        FollowBinding {
            slot: crate::state::SourceSlotReceipt {
                intent: crate::state::SourceSlotIntent {
                    binding: crate::state::SourceSlotBinding::new(
                        stream(),
                        "receiver".into(),
                        Plugin::Pgoutput,
                        1,
                    )
                    .unwrap(),
                    nonce: [2; 16],
                },
                consistent_point: SourceEndLsn(1),
                snapshot_token: "synthetic-snapshot".into(),
            },
            publication: crate::contracts::PublicationReceipt {
                name: "receiver".into(),
                oid: 10,
                owner_oid: 11,
            },
            source_timeline: 1,
            copy_generation: crate::contracts::CopyGeneration(1),
            source_types: vec![],
        }
    }

    #[test]
    fn full_fifo_keeps_status_alive_and_resumes_each_decoder_emission_once() {
        backpressure(false, false, false);
    }

    #[test]
    fn full_fifo_cancellation_closes_without_acking_or_retrying_a_poisoned_decoder() {
        backpressure(true, false, false);
    }

    #[test]
    #[ignore = "requires seventy seconds of bounded status while apply is blocked"]
    fn full_fifo_status_survives_walsender_timeout() {
        backpressure(false, true, false);
    }

    #[test]
    fn completed_stop_while_fifo_full_preserves_order_and_unapplied_feedback_floor() {
        backpressure(false, false, true);
    }

    fn backpressure(cancel_while_full: bool, real_timeout: bool, completed_stop: bool) {
        let limits = crate::contracts::FollowLimits {
            channel_messages: 2,
            ..crate::contracts::FollowLimits::default()
        };
        let (sender, receiver) = crate::follow::channel::bounded(limits).unwrap();
        let (_feedback, feedback) = crate::follow::channel::feedback();
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let control = IoControl {
            deadline: Instant::now() + Duration::from_secs(if real_timeout { 90 } else { 5 }),
            cancelled: std::sync::Arc::clone(&cancelled),
        };
        let request = std::sync::Arc::new(AtomicBool::new(false));
        let stop = std::sync::Arc::new(AtomicBool::new(false));
        let worker_request = std::sync::Arc::clone(&request);
        let worker_stop = std::sync::Arc::clone(&stop);
        let (ready, ready_receiver) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let (peer, mut source) = crate::pg::follow_receive_tests::Peer::start(
                if real_timeout {
                    "follow_long"
                } else {
                    "follow"
                },
                &control,
            );
            ready.send(peer).unwrap();
            let binding = binding();
            let mut decoder =
                super::super::pgoutput::Decoder::new(stream(), &binding, limits).unwrap();
            let mut lease = ReceiveLease {
                status: Status::new(
                    stream(),
                    SourceEndLsn(17),
                    Instant::now(),
                    if real_timeout {
                        STATUS_INTERVAL
                    } else {
                        Duration::from_millis(10)
                    },
                ),
                request_keepalive: &worker_request,
                stop: &worker_stop,
            };
            let result = receive(
                &mut source,
                &mut decoder,
                &sender,
                &feedback,
                &mut lease,
                limits.max_message_bytes,
                &control,
            );
            (
                result,
                decoder.is_idle(),
                lease.status.backpressure_reported,
            )
        });
        let mut peer = ready_receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        peer.blocked(if real_timeout { 8 } else { 3 });
        if !cancel_while_full && !completed_stop {
            let messages: Vec<_> = (0..7)
                .map(|_| receiver.recv_timeout(Duration::from_secs(1)).unwrap())
                .collect();
            assert!(matches!(messages[0], FollowMessage::Begin(begin) if begin.xid == 9));
            assert!(matches!(
                messages[1],
                FollowMessage::Event(crate::cdc::contracts::Event::Relation { .. })
            ));
            for (index, value) in [(2, b"1".as_slice()), (4, b"2".as_slice())] {
                assert!(
                    matches!(&messages[index], FollowMessage::Event(crate::cdc::contracts::Event::Insert { values, .. })
                    if values == &vec![crate::cdc::contracts::Value::Text(value.to_vec())])
                );
            }
            assert!(matches!(
                messages[3],
                FollowMessage::Keepalive {
                    wal_end: SourceEndLsn(500),
                    decoder_idle: false
                }
            ));
            assert!(
                matches!(messages[5], FollowMessage::Commit(commit) if commit.end == SourceEndLsn(120))
            );
            assert!(matches!(
                messages[6],
                FollowMessage::Keepalive {
                    wal_end: SourceEndLsn(777),
                    decoder_idle: true
                }
            ));
        }
        let before = Instant::now();
        if completed_stop {
            request.store(true, Ordering::Release);
            stop.store(true, Ordering::Release);
        } else {
            cancelled.store(true, Ordering::Release);
        }
        let (result, idle, backpressure_reported) = worker.join().unwrap();
        assert!(backpressure_reported);
        if completed_stop {
            result.unwrap();
            assert!(!cancelled.load(Ordering::Acquire));
        } else {
            assert!(result.unwrap_err().message.contains("cancel"));
        }
        assert!(before.elapsed() < Duration::from_millis(500));
        if cancel_while_full || completed_stop {
            assert!(!idle);
            assert!(matches!(
                receiver.recv_timeout(Duration::ZERO).unwrap(),
                FollowMessage::Begin(_)
            ));
            assert!(matches!(
                receiver.recv_timeout(Duration::ZERO).unwrap(),
                FollowMessage::Event(_)
            ));
        } else {
            assert!(idle);
        }
        assert_eq!(
            receiver.recv_timeout(Duration::ZERO),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
        );
        let receipt = peer.finish_closed();
        assert!(receipt["feedback_count"].as_u64().unwrap() >= 3);
        assert_eq!(receipt["positions_unchanged"], true);
        if completed_stop {
            assert_eq!(receipt["copy_done"], true);
            assert_eq!(receipt["reply_requested_count"], 1);
        }
    }
    #[test]
    fn frame_budget_refuses_before_decode_and_keeps_feedback_at_the_proven_floor() {
        let control = IoControl {
            deadline: Instant::now() + Duration::from_secs(3),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let limits = crate::contracts::FollowLimits::default();
        let (sender, receiver) = crate::follow::channel::bounded(limits).unwrap();
        let (_feedback, feedback) = crate::follow::channel::feedback();
        let (peer, mut source) = crate::pg::follow_receive_tests::Peer::start("follow", &control);
        let mut decoder =
            super::super::pgoutput::Decoder::new(stream(), &binding(), limits).unwrap();
        let request_keepalive = AtomicBool::new(false);
        let stop = AtomicBool::new(false);
        let mut lease = ReceiveLease {
            status: Status::new(stream(), SourceEndLsn(17), Instant::now(), STATUS_INTERVAL),
            request_keepalive: &request_keepalive,
            stop: &stop,
        };
        let failure = receive(
            &mut source,
            &mut decoder,
            &sender,
            &feedback,
            &mut lease,
            32,
            &control,
        )
        .unwrap_err();
        assert!(failure.message.contains("frame exceeds"));
        assert!(decoder.is_idle());
        assert_eq!(lease.status.proven, SourceEndLsn(17));
        assert_eq!(
            receiver.recv_timeout(Duration::ZERO),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        );
        drop(source);
        assert!(peer.finish_closed()["feedback_count"].as_u64().unwrap() >= 1);
    }
    #[test]
    #[ignore = "requires an owned PostgreSQL 16 or 18 receiver fixture"]
    fn live_configuration_and_logical_query_preserve_export_lifetime() {
        std::env::var_os("RPGMIGRATE_TEST_FOLLOW_RECEIVE_CONTEXT")
            .expect("owned receiver context required");
        let conninfo = crate::contracts::Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_FOLLOW_RECEIVE_SOURCE").unwrap(),
        )
        .unwrap();
        let control = IoControl::unbounded(std::sync::Arc::new(
            std::sync::atomic::AtomicBool::new(false),
        ));
        let mut source = PgConnection::connect_replication(&conninfo, &control).unwrap();
        configure(&mut source, &control).unwrap();
        for sql in ["SELECT 1/0", "SELECT 1; SELECT 2"] {
            assert!(source.logical_query(sql, &control).is_err());
            assert_eq!(
                source
                    .logical_query("SELECT 42", &control)
                    .unwrap()
                    .cell(0, 0)
                    .unwrap(),
                Some(b"42".as_slice())
            );
        }
        let slot = format!("receive_live_{}", std::process::id());
        let created = match source
            .create_logical_slot_export(&slot, "pgoutput", &control)
            .unwrap()
        {
            crate::pg::SlotCreateOutcome::Created(created) => created,
            crate::pg::SlotCreateOutcome::Unknown(failure) => {
                panic!("owned slot creation failed: {failure:?}")
            }
        };
        assert_eq!(created.slot_name, slot);
        assert!(
            source
                .logical_query("SHOW statement_timeout", &control)
                .err()
                .unwrap()
                .message
                .contains("protocol state")
        );
        assert!(source.identify_system(&control).is_err());
        assert!(source.backend_pid().unwrap() > 0);
        assert_eq!(
            source.transaction_status().unwrap(),
            crate::pg::TransactionStatus::Idle
        );
        let mut importer = PgConnection::connect(&conninfo, &control).unwrap();
        importer
            .query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", &control)
            .unwrap();
        let token = importer.quote_literal(&created.snapshot_token).unwrap();
        importer
            .query(&format!("SET TRANSACTION SNAPSHOT {token}"), &control)
            .unwrap();
        importer.query("SELECT 42", &control).unwrap();
        importer.query("COMMIT", &control).unwrap();
        drop(source);
        importer
            .query(
                &format!("SELECT pg_catalog.pg_drop_replication_slot('{slot}')"),
                &control,
            )
            .unwrap();
    }
    #[test]
    #[ignore = "requires an owned PostgreSQL 16 or 18 receiver fixture"]
    fn live_startup_refuses_slot_advanced_beyond_recovery_before_readiness() {
        use crate::contracts::{
            CloneOptions, CopyGeneration, FollowLimits, PublicationReceipt, RestorePolicy,
            StateAction, StateRequest,
        };
        use crate::state::{SourceSlotBinding, SourceSlotIntent, SourceSlotReceipt};
        let context = std::env::var_os("RPGMIGRATE_TEST_FOLLOW_RECEIVE_CONTEXT")
            .expect("owned receiver context required");
        let context: serde_json::Value =
            serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
        let root = std::path::PathBuf::from(context["root"].as_str().unwrap());
        let conninfo = crate::contracts::Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_FOLLOW_RECEIVE_SOURCE").unwrap(),
        )
        .unwrap();
        let control = IoControl {
            deadline: Instant::now() + Duration::from_secs(5),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let mut admin = PgConnection::connect(&conninfo, &control).unwrap();
        let source = crate::catalog::read_clone_source(&mut admin, &control).unwrap();
        let stream = StreamId {
            run: RunId([3; 16]),
            source: source.identity,
            generation: 1,
        };
        let slot = format!("receive_gap_{}", std::process::id());
        let publication = format!("receive_gap_pub_{}", std::process::id());
        admin
            .query(
                &format!("CREATE PUBLICATION {publication} FOR ALL TABLES"),
                &control,
            )
            .unwrap();
        let result = admin.query(&format!("SELECT oid::text,pubowner::text FROM pg_catalog.pg_publication WHERE pubname='{publication}'"), &control).unwrap();
        assert_eq!(result.rows(), 1);
        let number = |column| {
            std::str::from_utf8(result.cell(0, column).unwrap().unwrap())
                .unwrap()
                .parse::<u32>()
                .unwrap()
        };
        let receipt = PublicationReceipt {
            name: publication.clone(),
            oid: number(0),
            owner_oid: number(1),
        };
        let mut exporter = PgConnection::connect_replication(&conninfo, &control).unwrap();
        let created = match exporter
            .create_logical_slot_export(&slot, "pgoutput", &control)
            .unwrap()
        {
            crate::pg::SlotCreateOutcome::Created(created) => created,
            crate::pg::SlotCreateOutcome::Unknown(failure) => {
                panic!("owned slot creation failed: {failure:?}")
            }
        };
        let captured = created.consistent_point;
        let binding = FollowBinding {
            slot: SourceSlotReceipt {
                intent: SourceSlotIntent {
                    binding: SourceSlotBinding::new(stream, slot.clone(), Plugin::Pgoutput, 1)
                        .unwrap(),
                    nonce: [4; 16],
                },
                consistent_point: captured,
                snapshot_token: created.snapshot_token,
            },
            publication: receipt,
            source_timeline: source.timeline,
            copy_generation: CopyGeneration(1),
            source_types: vec![],
        };
        drop(exporter);
        let start = crate::apply::checkpoint::recovery_start_for_test(&binding, captured, captured);
        admin
            .query(
                "SELECT pg_catalog.pg_logical_emit_message(false,'receiver_gap','controlled-test')",
                &control,
            )
            .unwrap();
        admin.query("CHECKPOINT", &control).unwrap();
        let advanced = admin.query(&format!("SELECT end_lsn::text FROM pg_catalog.pg_replication_slot_advance('{slot}',pg_catalog.pg_current_wal_flush_lsn())"), &control).unwrap();
        let advanced = advanced.cell(0, 0).unwrap().unwrap().to_vec();
        let captured_text = format!("{:X}/{:X}", captured.0 >> 32, captured.0 as u32);
        let position = admin.query(&format!("SELECT confirmed_flush_lsn > '{captured_text}'::pg_catalog.pg_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name='{slot}'"), &control).unwrap();
        assert_eq!(position.cell(0, 0).unwrap(), Some(b"t".as_slice()));
        let path = root.join("receiver-unused-state");
        let options = FollowOptions {
            clone: CloneOptions {
                source: conninfo.clone(),
                target: conninfo,
                state: StateRequest {
                    path: path.clone(),
                    action: StateAction::Create,
                },
                operator: None,
                table_jobs: 1,
                post_data_jobs: 1,
                policy: RestorePolicy::default(),
                not_consistent: false,
                use_copy_binary: false,
                split_tables_larger_than: 0,
                split_max_parts: 8,
                skip_large_objects: false,
                large_objects_jobs: 4,
            },
            slot_name: slot.clone(),
            publication_name: publication.clone(),
            origin_name: Some("unowned_compatibility".into()),
            durability: None,
            apply_group_txns: Some(1),
            limits: FollowLimits::default(),
        };
        let settings = EffectiveFollowSettings {
            durability: crate::contracts::DurabilityMode::Durable,
            barrier_interval_ms: None,
            apply_group_txns: 1,
            limits: options.limits,
        };
        let (output, input) = crate::follow::channel::bounded(settings.limits).unwrap();
        let (_feedback, feedback) = crate::follow::channel::feedback();
        let (ready, readiness) = std::sync::mpsc::sync_channel(1);
        let failure = run(
            &options,
            &settings,
            &binding,
            start,
            &output,
            &feedback,
            ready,
            &AtomicBool::new(false),
            &AtomicBool::new(false),
            &control,
        )
        .unwrap_err();
        assert_eq!(failure.kind, ErrorKind::BadState);
        assert!(failure.message.contains("slot advanced beyond recovery"));
        assert_eq!(
            readiness.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Disconnected)
        );
        assert_eq!(
            input.recv_timeout(Duration::ZERO),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        );
        assert!(!path.exists());
        let position = admin.query(&format!("SELECT confirmed_flush_lsn::text,active FROM pg_catalog.pg_replication_slots WHERE slot_name='{slot}'"), &control).unwrap();
        assert_eq!(position.cell(0, 0).unwrap(), Some(advanced.as_slice()));
        assert_eq!(position.cell(0, 1).unwrap(), Some(b"f".as_slice()));
        admin
            .query(
                &format!("SELECT pg_catalog.pg_drop_replication_slot('{slot}')"),
                &control,
            )
            .unwrap();
        admin
            .query(&format!("DROP PUBLICATION {publication}"), &control)
            .unwrap();
    }
}
