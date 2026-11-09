use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::time::{Duration, Instant};

use crate::apply::CloneAuthority;
use crate::apply::checkpoint::{CompletedFollow, TargetAuthority};
use crate::catalog;
use crate::contracts::{
    ClonePlan, CopyGeneration, Error, ErrorKind, FollowBinding, FollowOptions, IoControl,
    RestorePolicy, Result, SnapshotTicket, StateAction,
};
use crate::pg::{PgConnection, operation_control};
use crate::snapshot::{CloneSnapshot, FollowSnapshot};
use crate::state::ControlHandle;
use crate::state::{self, ClonePhase, CloneState, NativeState, SourceSlotProvenance};

pub(crate) mod channel;

const POLL: Duration = Duration::from_millis(100);
const HEALTH_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Eq, PartialEq)]
enum Entry {
    Fresh,
    Recopy,
    Streaming,
}

fn entry(phase: Option<ClonePhase>, anchored: bool, action: StateAction) -> Result<Entry> {
    match (phase, anchored, action) {
        (None, false, _) => Ok(Entry::Fresh),
        (Some(ClonePhase::Analyzing), true, StateAction::Resume) => Ok(Entry::Streaming),
        (Some(ClonePhase::Copying), false, StateAction::Resume | StateAction::Restart) => {
            Ok(Entry::Recopy)
        }
        _ => Err(refusal(
            "follow-recovery: requested stage lacks a safe copy restart or durable follow anchor; preserve native state and inspect uncertain utility outcomes",
        )),
    }
}

pub(crate) fn run(mut options: FollowOptions, io: &IoControl) -> Result<String> {
    options.limits.validate()?;
    if options.apply_group_txns == 0 {
        return Err(refusal(
            "follow: apply group transaction limit must be positive",
        ));
    }
    let mut native = state::open_follow(
        &options.clone.state,
        options.clone.operator.as_ref(),
        options.origin_name.as_deref(),
        io,
    )?;
    let saved = native.checked_clone()?;
    if let Some(saved) = &saved {
        if options.clone.policy != RestorePolicy::default()
            && options.clone.policy != saved.plan.policy
        {
            return Err(refusal(
                "follow: restore policy differs from native binding",
            ));
        }
        options.clone.policy = saved.plan.policy;
    }
    let binding = native.checked_follow()?;
    if let Some(binding) = &binding {
        require_resource_names(&options, binding)?;
    }
    let mut completed = None;
    let anchored = if saved
        .as_ref()
        .is_some_and(|s| s.phase == ClonePhase::Analyzing)
    {
        let binding = binding
            .as_ref()
            .ok_or_else(|| refusal("follow: native binding is absent"))?;
        let connection = target(&options, io)?;
        let mut authority = TargetAuthority::acquire_follow(connection, &mut native, binding, io)?;
        let anchored = authority.has_follow_anchor();
        if anchored {
            let control = ControlHandle::for_worker(&native, io)?;
            if authority.observe_follow(io)?.follow_completed() {
                let request = control
                    .read(io)?
                    .filter(|request| request.is_sealed())
                    .ok_or_else(|| {
                        refusal("follow: completed row lacks its sealed end position")
                    })?;
                completed = Some(authority.observe_completed(&control.seal(&request, io)?, io)?);
            }
        }
        authority.close(io)?;
        anchored
    } else {
        false
    };
    let mode = entry(
        saved.as_ref().map(|s| s.phase),
        anchored,
        options.clone.state.action,
    )?;
    if let Some(completed) = completed {
        return completed_report(&options, &completed, io);
    }
    let (plan, binding) = match mode {
        Entry::Fresh => fresh(&options, &mut native, io)?,
        Entry::Recopy => recopy(&options, &mut native, io)?,
        Entry::Streaming => (
            saved
                .ok_or_else(|| refusal("follow: recorded plan is absent"))?
                .plan,
            binding.ok_or_else(|| refusal("follow: native binding is absent"))?,
        ),
    };
    crate::protocol::follow_startup(&options)?;
    let completed = stream(&options, &plan, &binding, &mut native, io)?;
    completed_report(&options, &completed, io)
}

fn fresh(
    options: &FollowOptions,
    native: &mut NativeState,
    io: &IoControl,
) -> Result<(ClonePlan, FollowBinding)> {
    let mut source = source(options, io)?;
    let mut target_sql = target(options, io)?;
    // Catalog table locks require a transaction before the slot snapshot exists.
    catalog::command(
        &mut source,
        "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "BEGIN",
        io,
    )?;
    let plan = catalog::plan_clone(
        &mut source,
        &mut target_sql,
        CopyGeneration(1),
        options.clone.policy,
        &[],
        &[],
        io,
    )?;
    catalog::command(&mut source, "COMMIT", "COMMIT", io)?;
    if native
        .checked_follow_source()?
        .is_some_and(|saved| saved != plan)
    {
        return Err(refusal(
            "follow: preliminary plan differs from its native source intent",
        ));
    }
    catalog::follow::reject_generated(&plan.tables)?;
    crate::schema::preflight(&source, &target_sql, &plan, io)?;
    let mut snapshot = FollowSnapshot::create(options, &plan, native, io)?;
    let binding = snapshot.binding().clone();
    native.bind_follow(&plan, snapshot.snapshot_mut().ticket(), &binding)?;
    let coordinator = target(options, io)?;
    let mut authority = CloneAuthority::acquire(coordinator, native, &plan, io)?;
    copy_initial(
        options,
        &plan,
        snapshot,
        native,
        &mut authority,
        true,
        &mut target_sql,
        io,
    )?;
    Ok((plan, binding))
}

fn recopy(
    options: &FollowOptions,
    native: &mut NativeState,
    io: &IoControl,
) -> Result<(ClonePlan, FollowBinding)> {
    let saved = native
        .checked_clone()?
        .ok_or_else(|| refusal("follow: restart lacks owned copy state"))?;
    let coordinator = target(options, io)?;
    let mut authority = CloneAuthority::acquire(coordinator, native, &saved.plan, io)?;
    let mut target = target(options, io)?;
    if let Some(restart) = native.checked_follow_restart()? {
        if restart.new_bound {
            authority.advance_generation(native, io)?;
            native.finish_follow_restart()?;
        } else if restart.slot_absent
            && restart.publication_absent
            && matches!(native.checked_source_slot()?, Some(SourceSlotProvenance::Confirmed(ref receipt))
                if receipt.intent.binding.catalog_epoch() == restart.next_generation.0)
        {
            let mut next = saved.plan.clone();
            next.generation = restart.next_generation;
            let binding = crate::snapshot::recover_follow_binding(options, &next, native, io)?;
            // This token reconciles the lost generation; COPY gets a fresh capability below.
            let ticket = SnapshotTicket {
                token: binding.slot.snapshot_token.clone(),
                source: next.source.clone(),
                generation: next.generation,
            };
            native.bind_follow(&next, &ticket, &binding)?;
            authority.advance_generation(native, io)?;
            native.finish_follow_restart()?;
        }
    }
    let saved = native
        .checked_clone()?
        .ok_or_else(|| refusal("follow: restart lost owned copy state"))?;
    let checked = restart_plan(options, native, &saved, &mut target, io)?;
    let restart = authority.begin_follow_restart(native, &checked, io)?;
    let mut plan = checked;
    plan.generation = restart.next_generation;
    let mut snapshot = FollowSnapshot::restart(options, &plan, native, io)?;
    let binding = snapshot.binding().clone();
    native.bind_follow(&plan, snapshot.snapshot_mut().ticket(), &binding)?;
    authority.advance_generation(native, io)?;
    native.finish_follow_restart()?;
    authority.truncate_follow_workset(native, io)?;
    copy_initial(
        options,
        &plan,
        snapshot,
        native,
        &mut authority,
        false,
        &mut target,
        io,
    )?;
    Ok((plan, binding))
}

fn restart_plan(
    options: &FollowOptions,
    native: &NativeState,
    saved: &CloneState,
    target: &mut PgConnection,
    io: &IoControl,
) -> Result<ClonePlan> {
    let mut snapshot = CloneSnapshot::export(source(options, io)?, saved.plan.generation, io)?;
    let metadata = crate::clone::metadata(native, &saved.plan, target, io)?;
    let checked = catalog::plan_clone(
        snapshot.connection()?,
        target,
        saved.plan.generation,
        options.clone.policy,
        &metadata,
        &saved.owned_objects,
        io,
    )?;
    crate::clone::require_same_plan(&saved.plan, &checked)?;
    crate::clone::require_pre_data_match(crate::schema::compare_pre_data(
        &options.clone,
        snapshot.ticket(),
        &metadata,
        io,
    ))?;
    snapshot.finish(io)?;
    Ok(checked)
}

#[allow(clippy::too_many_arguments)]
fn copy_initial(
    options: &FollowOptions,
    plan: &ClonePlan,
    mut snapshot: FollowSnapshot,
    native: &mut NativeState,
    authority: &mut CloneAuthority,
    fresh: bool,
    target: &mut PgConnection,
    io: &IoControl,
) -> Result<()> {
    let binding = snapshot.binding().clone();
    let exporter_pid = snapshot.exporter_pid();
    with_source_health(options, plan, &binding, Some(exporter_pid), io, || {
        let ticket = snapshot.snapshot_mut().ticket().clone();
        let archive = crate::schema::dump(&options.clone, &ticket, native, io)?;
        if fresh {
            let owned = crate::schema::restore_pre(&options.clone, plan, &archive, native, io)?;
            authority.mark_stage(ClonePhase::PreDataPending, io)?;
            let tables = catalog::reconcile_clone_tables(target, plan, &owned, io)?;
            native.confirm_pre_data(owned, tables)?;
            authority.bind_tables(native, io)?;
        }
        crate::copy::run_tables(
            &options.clone,
            plan,
            snapshot.snapshot_mut(),
            native,
            authority,
            io,
        )?;
        crate::clone::pending(native, authority, ClonePhase::PostDataPending, io)?;
        crate::schema::restore_post(&options.clone, &archive, io)?;
        crate::clone::positive(native, authority, ClonePhase::PostDataComplete, io)?;
        crate::clone::pending(native, authority, ClonePhase::SequencesPending, io)?;
        let saved = native
            .checked_clone()?
            .ok_or_else(|| refusal("follow: owned copy state disappeared"))?;
        catalog::sync_clone_sequences(
            snapshot.snapshot_mut().connection()?,
            target,
            plan,
            &saved.owned_objects,
            io,
        )?;
        crate::clone::positive(native, authority, ClonePhase::SequencesComplete, io)?;
        crate::clone::pending(native, authority, ClonePhase::Analyzing, io)?;
        catalog::analyze_clone(target, plan, &native.owned_copy_tables()?, io)?;
        let receipt = snapshot.receipt().clone();
        snapshot.finish(io)?;
        authority.install_follow_anchor(native, &receipt, io)?;
        Ok(())
    })
}

fn require_resource_names(options: &FollowOptions, binding: &FollowBinding) -> Result<()> {
    if !options.slot_name.is_empty() && options.slot_name != binding.slot.intent.binding.slot_name()
        || !options.publication_name.is_empty()
            && options.publication_name != binding.publication.name
    {
        return Err(refusal(
            "follow: explicit source resource names differ from native binding",
        ));
    }
    Ok(())
}

fn source(options: &FollowOptions, io: &IoControl) -> Result<PgConnection> {
    PgConnection::connect(&options.clone.source, &operation_control(io))
        .map_err(|error| crate::endpoint_error(error, ErrorKind::Source))
}

fn target(options: &FollowOptions, io: &IoControl) -> Result<PgConnection> {
    PgConnection::connect(&options.clone.target, &operation_control(io))
        .map_err(|error| crate::endpoint_error(error, ErrorKind::Target))
}

fn with_source_health<T>(
    options: &FollowOptions,
    plan: &ClonePlan,
    binding: &FollowBinding,
    pid: Option<i32>,
    io: &IoControl,
    work: impl FnOnce() -> Result<T>,
) -> Result<T> {
    std::thread::scope(|scope| {
        let (stop, stopped) = mpsc::sync_channel(1);
        let monitor = scope.spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut source = source(options, io)?;
                loop {
                    catalog::check_follow_health(&mut source, plan, binding, pid, io)?;
                    match stopped.recv_timeout(HEALTH_INTERVAL) {
                        Ok(()) | Err(RecvTimeoutError::Disconnected) => return Ok(()),
                        Err(RecvTimeoutError::Timeout) => check(io)?,
                    }
                }
            }))
            .unwrap_or_else(|_| Err(refusal("follow: source health worker panicked")));
            if result.is_err() {
                io.cancelled.store(true, Ordering::Release);
            }
            result
        });
        let result = work();
        let _ = stop.send(());
        monitor
            .join()
            .map_err(|_| refusal("follow: source health worker panicked"))??;
        result
    })
}

#[derive(Debug)]
enum WorkerOutcome {
    ApplyCompleted(Box<CompletedFollow>),
    ReceiveStopped,
    Failed(Error),
}

#[derive(Debug)]
enum WaitReady<T> {
    Ready(T),
    Completed(Box<CompletedFollow>),
}

fn stream(
    options: &FollowOptions,
    plan: &ClonePlan,
    binding: &FollowBinding,
    native: &mut NativeState,
    io: &IoControl,
) -> Result<CompletedFollow> {
    let control = ControlHandle::for_worker(native, io)?;
    let mut source = source(options, io)?;
    let confirmed = catalog::follow_confirmed_flush(&mut source, plan, binding, None, io)?;
    let (send, input) = channel::bounded(options.limits)?;
    let (feedback, proofs) = channel::feedback();
    let request_keepalive = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        let (outcomes, outcome) = mpsc::sync_channel(2);
        let (ready, recovery) = mpsc::sync_channel(1);
        let apply_outcomes = outcomes.clone();
        let apply_input = &input;
        let apply_feedback = &feedback;
        let apply_request = &request_keepalive;
        let apply_control = &control;
        let apply = scope.spawn(move || {
            worker(
                apply_outcomes,
                io,
                || {
                    crate::apply::follow::run(
                        options,
                        binding,
                        native,
                        confirmed,
                        ready.clone(),
                        apply_input,
                        apply_feedback,
                        apply_control,
                        apply_request,
                        io,
                    )
                },
                |completed| WorkerOutcome::ApplyCompleted(Box::new(completed)),
            );
            drop(ready);
        });
        let mut receive = None;
        let result = (|| {
            let start = match wait_ready(&recovery, &outcome, io)? {
                WaitReady::Ready(start) => start,
                WaitReady::Completed(completed) => return Ok(*completed),
            };
            let (ready, receiver) = mpsc::sync_channel(1);
            let receive_send = &send;
            let receive_proofs = &proofs;
            let receive_request = &request_keepalive;
            let receive_stop = &stop;
            receive = Some(scope.spawn(move || {
                worker(
                    outcomes,
                    io,
                    || {
                        crate::cdc::receive::run(
                            options,
                            binding,
                            start,
                            receive_send,
                            receive_proofs,
                            ready.clone(),
                            receive_request,
                            receive_stop,
                            io,
                        )
                    },
                    |()| WorkerOutcome::ReceiveStopped,
                );
                drop(ready);
            }));
            let pid = match wait_ready(&receiver, &outcome, io)? {
                WaitReady::Ready(pid) => pid,
                WaitReady::Completed(completed) => return Ok(*completed),
            };
            if pid <= 0 {
                return Err(refusal("follow: receiver returned an invalid backend PID"));
            }
            emit_streaming(options, io).map_err(|error| coordinator_error(error, &outcome))?;
            let mut next_health = Instant::now();
            loop {
                if let Ok(result) = outcome.try_recv() {
                    return outcome_completion(result).map(|completed| *completed);
                }
                check(io).map_err(|error| coordinator_error(error, &outcome))?;
                if Instant::now() >= next_health {
                    catalog::check_follow_health(&mut source, plan, binding, Some(pid), io)
                        .map_err(|error| coordinator_error(error, &outcome))?;
                    next_health = Instant::now() + HEALTH_INTERVAL;
                }
                match outcome.recv_timeout(POLL) {
                    Ok(result) => return outcome_completion(result).map(|completed| *completed),
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(refusal("follow: all streaming workers stopped"));
                    }
                }
            }
        })();
        if result.is_ok() {
            stop.store(true, Ordering::Release);
        } else {
            io.cancelled.store(true, Ordering::Release);
        }
        let receiver_started = receive.is_some();
        let apply_joined = apply.join();
        let receive_joined = receive.map(|receive| receive.join());
        let joined =
            if apply_joined.is_err() || receive_joined.is_some_and(|result| result.is_err()) {
                Err(refusal("follow: streaming worker panicked while joining"))
            } else {
                Ok(())
            };
        completed_join(result, &outcome, receiver_started, joined, io)
    })
}

fn outcome_completion(outcome: WorkerOutcome) -> Result<Box<CompletedFollow>> {
    match outcome {
        WorkerOutcome::ApplyCompleted(completed) => Ok(completed),
        WorkerOutcome::ReceiveStopped => Err(refusal(
            "follow: receiver stopped before durable apply completion",
        )),
        WorkerOutcome::Failed(error) => Err(error),
    }
}

fn wait_ready<T>(
    ready: &Receiver<T>,
    outcomes: &Receiver<WorkerOutcome>,
    io: &IoControl,
) -> Result<WaitReady<T>> {
    let bounded = operation_control(io);
    loop {
        if let Ok(outcome) = outcomes.try_recv() {
            return outcome_completion(outcome).map(WaitReady::Completed);
        }
        check(&bounded).map_err(|error| coordinator_error(error, outcomes))?;
        match ready.recv_timeout(POLL) {
            Ok(value) => return Ok(WaitReady::Ready(value)),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                return outcomes
                    .try_recv()
                    .map_err(|_| {
                        refusal("follow: readiness channel closed before positive startup")
                    })
                    .and_then(outcome_completion)
                    .map(WaitReady::Completed);
            }
        }
    }
}

fn coordinator_error(mut error: Error, outcomes: &Receiver<WorkerOutcome>) -> Error {
    if error.is_cancelled() {
        // Workers publish their causal error before publishing cancellation.
        for outcome in outcomes.try_iter() {
            if let WorkerOutcome::Failed(worker_error) = outcome {
                if !worker_error.is_cancelled() {
                    return worker_error;
                }
                error = worker_error;
            }
        }
    }
    error
}

fn prefer_error<T>(result: Result<T>, error: Error) -> Result<T> {
    match result {
        Err(primary) if !primary.is_cancelled() || error.is_cancelled() => Err(primary),
        _ => Err(error),
    }
}

fn completed_join(
    mut result: Result<CompletedFollow>,
    outcomes: &Receiver<WorkerOutcome>,
    receiver_started: bool,
    joined: Result<()>,
    io: &IoControl,
) -> Result<CompletedFollow> {
    let mut receive_stops = 0;
    for outcome in outcomes.try_iter() {
        match outcome {
            WorkerOutcome::ApplyCompleted(_) => {
                result = prefer_error(result, refusal("follow: duplicate apply completion"));
            }
            WorkerOutcome::ReceiveStopped => receive_stops += 1,
            WorkerOutcome::Failed(error) => result = prefer_error(result, error),
        }
    }
    if let Err(error) = joined {
        result = prefer_error(result, error);
    }
    let completed = result?;
    check(io)?;
    if receive_stops != usize::from(receiver_started) {
        return Err(refusal(
            "follow: missing or duplicate checked receiver shutdown",
        ));
    }
    Ok(completed)
}

fn worker<T>(
    outcomes: SyncSender<WorkerOutcome>,
    io: &IoControl,
    run: impl FnOnce() -> Result<T>,
    success: impl FnOnce(T) -> WorkerOutcome,
) {
    let outcome =
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run().map(success))) {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => WorkerOutcome::Failed(error),
            Err(_) => WorkerOutcome::Failed(refusal("follow: streaming worker panicked")),
        };
    let failed = matches!(outcome, WorkerOutcome::Failed(_));
    let _ = outcomes.send(outcome);
    if failed {
        io.cancelled.store(true, Ordering::Release);
    }
}

fn completed_report(
    options: &FollowOptions,
    completed: &CompletedFollow,
    io: &IoControl,
) -> Result<String> {
    check(io)?;
    let progress = crate::progress::completed_follow(&options.clone, completed, io)?;
    check(io)?;
    crate::protocol::completed_follow_report(progress.work.as_ref(), &progress.copy, completed)
}

fn emit_streaming(options: &FollowOptions, io: &IoControl) -> Result<()> {
    let progress = crate::progress::read(&options.clone, io)?;
    let output = crate::protocol::copy_report(
        crate::protocol::CopyCommand::Clone,
        options.clone.operator.as_ref(),
        "streaming",
        &progress.copy,
    )?;
    std::io::stdout()
        .lock()
        .write_all(output.as_bytes())
        .map_err(|_| refusal("follow: cannot write streaming report"))
}

fn check(io: &IoControl) -> Result<()> {
    if io.cancelled.load(Ordering::Acquire) {
        return Err(Error::cancelled(
            ErrorKind::BadState,
            "follow suspended: command cancelled or operation deadline exceeded",
        ));
    }
    if io.is_expired() {
        return Err(refusal(
            "follow suspended: command cancelled or operation deadline exceeded",
        ));
    }
    Ok(())
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};

    fn read_peer_message(stream: &mut TcpStream) -> u8 {
        let mut header = [0; 5];
        stream.read_exact(&mut header).unwrap();
        let size = u32::from_be_bytes(header[1..].try_into().unwrap());
        assert!((4..=4096).contains(&size));
        let mut payload = vec![0; size as usize - 4];
        stream.read_exact(&mut payload).unwrap();
        header[0]
    }

    fn accept_health_query(listener: TcpListener) -> TcpStream {
        let (mut stream, _) = listener.accept().unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut size = [0; 4];
        stream.read_exact(&mut size).unwrap();
        let size = u32::from_be_bytes(size);
        assert!((8..=4096).contains(&size));
        let mut startup = vec![0; size as usize - 4];
        stream.read_exact(&mut startup).unwrap();
        stream
            .write_all(b"R\0\0\0\x08\0\0\0\0Z\0\0\0\x05I")
            .unwrap();
        for tag in b"PBDES" {
            assert_eq!(read_peer_message(&mut stream), *tag);
        }
        stream
    }

    fn health_conninfo(port: u16) -> crate::contracts::Conninfo {
        crate::contracts::Conninfo::new(
            format!("host=127.0.0.1 port={port} dbname=health user=health sslmode=disable gssencmode=disable").into(),
        ).unwrap()
    }

    #[test]
    fn worker_failure_survives_cancellation_of_inflight_health_sql() {
        let mut io = control();
        io.deadline = Instant::now() + Duration::from_secs(5);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (health, inflight) = mpsc::sync_channel(1);
        let (stop, stopped) = mpsc::sync_channel(1);
        let (failed, failure) = mpsc::sync_channel(2);
        let (queued, complete) = mpsc::sync_channel(1);
        let primary = Error::new(ErrorKind::Target, "target apply refused");
        let origin = primary.origin;
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let _stream = accept_health_query(listener);
                health.send(()).unwrap();
                stopped.recv_timeout(Duration::from_secs(5)).unwrap();
            });
            let worker_io = &io;
            scope.spawn(move || {
                inflight.recv_timeout(Duration::from_secs(5)).unwrap();
                let peer = failed.clone();
                worker(
                    failed,
                    worker_io,
                    || Err(primary),
                    |()| WorkerOutcome::ReceiveStopped,
                );
                worker(
                    peer,
                    worker_io,
                    || check(worker_io),
                    |()| WorkerOutcome::ReceiveStopped,
                );
                queued.send(()).unwrap();
            });
            let mut source = PgConnection::connect(&health_conninfo(port), &io).unwrap();
            let error = source.query("SELECT 1", &io).err().unwrap();
            complete.recv_timeout(Duration::from_secs(5)).unwrap();
            let error = coordinator_error(error, &failure);
            stop.send(()).unwrap();
            assert_eq!(error.kind, ErrorKind::Target);
            assert_eq!(error.message, "target apply refused");
            assert!(std::ptr::eq(error.origin, origin));
            assert!(
                failed_error(failure.try_recv().unwrap())
                    .message
                    .contains("cancelled")
            );
        });
    }

    #[test]
    fn coordinator_preserves_actual_health_sql_failure_despite_queued_worker_failure() {
        let mut io = control();
        io.deadline = Instant::now() + Duration::from_secs(5);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (failed, failure) = mpsc::sync_channel(2);
        let (stop, stopped) = mpsc::sync_channel(1);
        std::thread::scope(|scope| {
            scope.spawn(move || {
                let mut stream = accept_health_query(listener);
                failed
                    .send(WorkerOutcome::Failed(Error::new(
                        ErrorKind::Target,
                        "apply failed",
                    )))
                    .unwrap();
                let payload = b"SERROR\0VERROR\0CXX000\0Mhealth query refused\0\0";
                stream.write_all(b"E").unwrap();
                stream
                    .write_all(&(payload.len() as u32 + 4).to_be_bytes())
                    .unwrap();
                stream.write_all(payload).unwrap();
                stream.write_all(b"Z\0\0\0\x05I").unwrap();
                stopped.recv_timeout(Duration::from_secs(5)).unwrap();
            });
            let mut source = PgConnection::connect(&health_conninfo(port), &io).unwrap();
            let error = source.query("SELECT 1", &io).err().unwrap();
            let origin = error.origin;
            let error = coordinator_error(error, &failure);
            stop.send(()).unwrap();
            assert_eq!(error.kind, ErrorKind::Pgsql);
            assert_eq!(error.message, "PostgreSQL command failed (SQLSTATE XX000)");
            assert!(std::ptr::eq(error.origin, origin));
            assert!(!error.is_cancelled());
            assert_eq!(
                failed_error(failure.try_recv().unwrap()).message,
                "apply failed"
            );
        });
    }

    #[test]
    fn coordinator_preserves_health_errors_and_cancel_looking_messages() {
        for (kind, message) in [
            (ErrorKind::BadState, "follow-gap: publication was replaced"),
            (ErrorKind::Pgsql, "PostgreSQL operation cancelled"),
        ] {
            let (failed, failure) = mpsc::sync_channel(2);
            failed
                .send(WorkerOutcome::Failed(Error::new(
                    ErrorKind::Target,
                    "apply failed",
                )))
                .unwrap();
            let error = Error::new(kind, message);
            let origin = error.origin;
            let error = coordinator_error(error, &failure);
            assert_eq!(error.kind, kind);
            assert_eq!(error.message, message);
            assert!(std::ptr::eq(error.origin, origin));
            assert_eq!(
                failed_error(failure.try_recv().unwrap()).message,
                "apply failed"
            );
        }
    }

    #[test]
    fn coordinator_deadline_preserves_its_error_and_queued_worker_failure() {
        let mut io = control();
        io.deadline = Instant::now();
        let (failed, failure) = mpsc::sync_channel(2);
        failed
            .send(WorkerOutcome::Failed(Error::new(
                ErrorKind::Target,
                "apply failed",
            )))
            .unwrap();
        let error = check(&io).unwrap_err();
        let origin = error.origin;
        let error = coordinator_error(error, &failure);
        assert_eq!(error.kind, ErrorKind::BadState);
        assert_eq!(
            error.message,
            "follow suspended: command cancelled or operation deadline exceeded"
        );
        assert!(std::ptr::eq(error.origin, origin));
        assert!(!error.is_cancelled());
        assert_eq!(
            failed_error(failure.try_recv().unwrap()).message,
            "apply failed"
        );
    }

    #[test]
    fn external_cancellation_without_worker_failure_is_preserved() {
        let io = control();
        let (_failed, failure) = mpsc::sync_channel(2);
        io.cancelled.store(true, Ordering::Release);
        let error = check(&io).unwrap_err();
        let origin = error.origin;
        let error = coordinator_error(error, &failure);
        assert_eq!(error.kind, ErrorKind::BadState);
        assert_eq!(
            error.message,
            "follow suspended: command cancelled or operation deadline exceeded"
        );
        assert!(std::ptr::eq(error.origin, origin));
        assert!(error.is_cancelled());
    }

    fn control() -> IoControl {
        IoControl {
            deadline: Instant::now() + Duration::from_secs(1),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    #[test]
    fn closed_readiness_cannot_authorize_replication_start() {
        let (send, ready) = mpsc::sync_channel::<i32>(1);
        let (_failed, failure) = mpsc::sync_channel(2);
        drop(send);
        let error = wait_ready(&ready, &failure, &control()).unwrap_err();
        assert!(error.message.contains("before positive startup"));
    }

    #[test]
    fn worker_failure_wins_over_a_queued_ready_value() {
        let (send, ready) = mpsc::sync_channel(1);
        let (failed, failure) = mpsc::sync_channel(2);
        send.send(123).unwrap();
        let primary = Error::new(ErrorKind::Source, "source lease refused");
        let origin = primary.origin;
        failed.send(WorkerOutcome::Failed(primary)).unwrap();
        failed
            .send(WorkerOutcome::Failed(Error::new(
                ErrorKind::Target,
                "peer stopped",
            )))
            .unwrap();
        let error = wait_ready(&ready, &failure, &control()).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Source);
        assert_eq!(error.message, "source lease refused");
        assert!(std::ptr::eq(error.origin, origin));
        assert_eq!(
            failed_error(failure.try_recv().unwrap()).message,
            "peer stopped"
        );
    }

    #[test]
    fn cancellation_blocks_even_positive_queued_readiness() {
        let io = control();
        let (send, ready) = mpsc::sync_channel(1);
        let (_failed, failure) = mpsc::sync_channel(2);
        send.send(123).unwrap();
        io.cancelled.store(true, Ordering::Release);
        assert!(wait_ready(&ready, &failure, &io).is_err());
    }

    #[test]
    fn successful_receiver_shutdown_does_not_set_error_cancellation() {
        let io = control();
        let (outcomes, _outcome) = mpsc::sync_channel(2);
        worker(outcomes, &io, || Ok(()), |()| WorkerOutcome::ReceiveStopped);
        assert!(!io.cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn receiver_stop_without_apply_completion_is_not_success() {
        let io = control();
        let (outcomes, outcome) = mpsc::sync_channel(2);
        worker(outcomes, &io, || Ok(()), |()| WorkerOutcome::ReceiveStopped);
        assert!(outcome_completion(outcome.recv().unwrap()).is_err());
    }

    fn failed_error(outcome: WorkerOutcome) -> Error {
        match outcome {
            WorkerOutcome::Failed(error) => error,
            _ => panic!("expected worker failure"),
        }
    }

    fn completed() -> CompletedFollow {
        crate::apply::checkpoint::completed_for_test()
    }

    #[test]
    fn completion_is_published_before_receive_stop() {
        let io = control();
        let stop = AtomicBool::new(false);
        let (outcomes, outcome) = mpsc::sync_channel(2);
        worker(
            outcomes,
            &io,
            || Ok(completed()),
            |completed| WorkerOutcome::ApplyCompleted(Box::new(completed)),
        );
        assert!(!stop.load(Ordering::Acquire));
        let completed = *outcome_completion(outcome.recv().unwrap()).unwrap();
        stop.store(true, Ordering::Release);
        assert!(completed_join(Ok(completed), &outcome, false, Ok(()), &io).is_ok());
        assert!(!io.cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn completion_does_not_drop_fifo_before_stop() {
        use crate::cdc::contracts::FollowMessage;
        use crate::contracts::FollowLimits;
        use crate::identity::SourceEndLsn;
        use std::sync::mpsc::TrySendError;

        let io = control();
        let limits = FollowLimits {
            channel_messages: 1,
            ..FollowLimits::default()
        };
        let (send, input) = channel::bounded(limits).unwrap();
        let message = || FollowMessage::Keepalive {
            wal_end: SourceEndLsn(32),
            decoder_idle: true,
        };
        send.try_send(message()).unwrap();
        let (outcomes, outcome) = mpsc::sync_channel(2);
        std::thread::scope(|scope| {
            let input = &input;
            let worker_io = &io;
            let apply = scope.spawn(move || {
                worker(
                    outcomes,
                    worker_io,
                    || {
                        let _held_input = input;
                        Ok(completed())
                    },
                    |completed| WorkerOutcome::ApplyCompleted(Box::new(completed)),
                );
            });
            let completed = *outcome_completion(outcome.recv_timeout(POLL).unwrap()).unwrap();
            apply.join().unwrap();
            assert!(matches!(
                send.try_send(message()),
                Err(TrySendError::Full(_))
            ));
            assert!(completed_join(Ok(completed), &outcome, false, Ok(()), &io).is_ok());
        });
    }

    #[test]
    fn completion_during_readiness() {
        let io = control();
        let (ready, recovery) = mpsc::sync_channel::<i32>(1);
        let (outcomes, outcome) = mpsc::sync_channel(2);
        let (release, released) = mpsc::sync_channel(1);
        std::thread::scope(|scope| {
            let worker_io = &io;
            let apply = scope.spawn(move || {
                worker(
                    outcomes,
                    worker_io,
                    || Ok(completed()),
                    |completed| WorkerOutcome::ApplyCompleted(Box::new(completed)),
                );
                released.recv_timeout(Duration::from_secs(1)).unwrap();
                drop(ready);
            });
            assert!(matches!(
                wait_ready(&recovery, &outcome, &io).unwrap(),
                WaitReady::Completed(_)
            ));
            assert!(matches!(
                recovery.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            release.send(()).unwrap();
            apply.join().unwrap();
        });
    }

    #[test]
    fn completion_after_readiness_sender_teardown_is_retained() {
        let io = control();
        let (ready, recovery) = mpsc::sync_channel::<i32>(1);
        let (outcomes, outcome) = mpsc::sync_channel(2);
        worker(
            outcomes,
            &io,
            || Ok(completed()),
            |completed| WorkerOutcome::ApplyCompleted(Box::new(completed)),
        );
        drop(ready);
        assert!(matches!(
            wait_ready(&recovery, &outcome, &io).unwrap(),
            WaitReady::Completed(_)
        ));
    }

    #[test]
    fn completed_join_preserves_real_failure() {
        let io = control();
        let (outcomes, outcome) = mpsc::sync_channel(2);
        let primary = Error::new(ErrorKind::Source, "source terminal result refused");
        let origin = primary.origin;
        outcomes.send(WorkerOutcome::Failed(primary)).unwrap();
        io.cancelled.store(true, Ordering::Release);
        let error = completed_join(Ok(completed()), &outcome, true, Ok(()), &io).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Source);
        assert_eq!(error.message, "source terminal result refused");
        assert!(std::ptr::eq(error.origin, origin));
        assert!(!error.is_cancelled());
    }

    #[test]
    fn completed_join_preserves_external_cancellation() {
        let io = control();
        let (outcomes, outcome) = mpsc::sync_channel(2);
        outcomes.send(WorkerOutcome::ReceiveStopped).unwrap();
        io.cancelled.store(true, Ordering::Release);
        let error = completed_join(Ok(completed()), &outcome, true, Ok(()), &io).unwrap_err();
        assert!(error.is_cancelled());
    }

    #[test]
    fn readiness_completion_cannot_hide_later_real_failure() {
        let io = control();
        let (_ready, recovery) = mpsc::sync_channel::<i32>(1);
        let (outcomes, outcome) = mpsc::sync_channel(2);
        outcomes
            .send(WorkerOutcome::ApplyCompleted(Box::new(completed())))
            .unwrap();
        outcomes
            .send(WorkerOutcome::Failed(Error::new(
                ErrorKind::Target,
                "late apply failure",
            )))
            .unwrap();
        let WaitReady::Completed(completed) = wait_ready(&recovery, &outcome, &io).unwrap() else {
            panic!("expected early completion");
        };
        let error = completed_join(Ok(*completed), &outcome, true, Ok(()), &io).unwrap_err();
        assert_eq!(error.kind, ErrorKind::Target);
        assert_eq!(error.message, "late apply failure");
    }

    #[test]
    fn completed_join_requires_exactly_one_checked_receiver_shutdown() {
        for stops in [0, 2] {
            let io = control();
            let (outcomes, outcome) = mpsc::sync_channel(2);
            for _ in 0..stops {
                outcomes.send(WorkerOutcome::ReceiveStopped).unwrap();
            }
            assert!(completed_join(Ok(completed()), &outcome, true, Ok(()), &io).is_err());
        }
        let io = control();
        let (outcomes, outcome) = mpsc::sync_channel(2);
        outcomes.send(WorkerOutcome::ReceiveStopped).unwrap();
        assert!(completed_join(Ok(completed()), &outcome, true, Ok(()), &io).is_ok());
    }

    #[test]
    fn completed_join_rejects_duplicate_completion_and_join_panic() {
        let io = control();
        let (outcomes, outcome) = mpsc::sync_channel(2);
        outcomes
            .send(WorkerOutcome::ApplyCompleted(Box::new(completed())))
            .unwrap();
        assert!(completed_join(Ok(completed()), &outcome, false, Ok(()), &io).is_err());
        let (_outcomes, outcome) = mpsc::sync_channel(2);
        assert!(
            completed_join(
                Ok(completed()),
                &outcome,
                false,
                Err(refusal("worker join panic")),
                &io
            )
            .is_err()
        );
    }

    #[test]
    fn completed_join_rechecks_deadline() {
        let mut io = control();
        io.deadline = Instant::now();
        let (_outcomes, outcome) = mpsc::sync_channel(2);
        assert!(completed_join(Ok(completed()), &outcome, false, Ok(()), &io).is_err());
    }

    #[test]
    fn worker_panic_is_a_failure_and_cancels_peers() {
        let io = control();
        let (outcomes, outcome) = mpsc::sync_channel(2);
        worker(
            outcomes,
            &io,
            || -> Result<()> { panic!("worker panic") },
            |()| WorkerOutcome::ReceiveStopped,
        );
        assert!(
            failed_error(outcome.recv().unwrap())
                .message
                .contains("panicked")
        );
        assert!(io.cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn durable_anchor_allows_resume_without_recopy() {
        assert_eq!(
            entry(Some(ClonePhase::Analyzing), true, StateAction::Resume).unwrap(),
            Entry::Streaming
        );
        assert!(entry(Some(ClonePhase::Analyzing), true, StateAction::Restart).is_err());
    }

    #[test]
    fn only_recorded_copying_can_restart_before_anchor() {
        for action in [StateAction::Resume, StateAction::Restart] {
            assert_eq!(
                entry(Some(ClonePhase::Copying), false, action).unwrap(),
                Entry::Recopy
            );
            for phase in [
                ClonePhase::Fresh,
                ClonePhase::PreDataPending,
                ClonePhase::PostDataPending,
                ClonePhase::PostDataComplete,
                ClonePhase::SequencesPending,
                ClonePhase::SequencesComplete,
                ClonePhase::Analyzing,
                ClonePhase::Complete,
            ] {
                assert!(entry(Some(phase), false, action).is_err(), "{phase:?}");
            }
        }
        assert_eq!(
            entry(None, false, StateAction::Restart).unwrap(),
            Entry::Fresh
        );
    }
}
