//! Stops a native clone or follow at each persisted window of phase Fresh and cancels it there, as a
//! process kill closes its sessions. Each hold is the controller's own session on the harness
//! cluster, released only after the engine returned.

use crate::apply::checkpoint::{FENCE_CLASS, LARGE_OBJECT_LOCK_CLASS, fence_slot};
use crate::contracts::{Conninfo, Error, IoControl, Result};
use crate::identity::RunId;
use crate::pg::{PgConnection, Role};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The windows of phase Fresh, in the order the engine reaches them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum FreshKill {
    /// Bound, waiting on the target fence before any target intent.
    Fence,
    /// The target intent is recorded and its metadata CREATE waits on an uncommitted same-name schema.
    TargetIntent,
    /// The target metadata committed while its COMMIT waited for a synchronous standby.
    TargetCommitted,
    /// The target metadata is confirmed and the copy waits behind a queued source table lock.
    Confirmed,
}

pub(crate) struct Endpoints {
    pub(crate) source: Conninfo,
    pub(crate) target: Conninfo,
}

fn control() -> IoControl {
    IoControl::unbounded(Arc::new(AtomicBool::new(false)))
}

fn connect(info: &Conninfo) -> PgConnection {
    PgConnection::connect(info, Role::Copy, &control()).unwrap()
}

pub(crate) fn scalar(connection: &mut PgConnection, sql: &str) -> String {
    let result = connection.query(sql, &control()).unwrap();
    assert_eq!(result.rows(), 1, "{sql}");
    String::from_utf8(result.cell(0, 0).unwrap().unwrap_or_default().to_vec()).unwrap()
}

pub(crate) fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    while !ready() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

pub(crate) fn metadata_schema(run: RunId) -> String {
    let hex = run
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("_rpgmigrate_{hex}")
}

fn waiting(connection: &mut PgConnection, predicate: &str) -> bool {
    scalar(
        connection,
        "SELECT pg_catalog.pg_stat_clear_snapshot()::text",
    );
    scalar(
        connection,
        &format!(
            "SELECT count(*) FROM pg_catalog.pg_locks l JOIN pg_catalog.pg_stat_activity a USING (pid) WHERE NOT l.granted AND a.pid<>pg_catalog.pg_backend_pid() AND {predicate}"
        ),
    ) != "0"
}

/// Resets the cluster's standby requirement even when the case panics.
struct StandbyHold(Conninfo);

impl StandbyHold {
    fn set(info: &Conninfo, names: &str) {
        let mut admin = connect(info);
        let sql = if names.is_empty() {
            "ALTER SYSTEM RESET synchronous_standby_names".to_owned()
        } else {
            format!("ALTER SYSTEM SET synchronous_standby_names='{names}'")
        };
        admin.query(&sql, &control()).unwrap();
        scalar(&mut admin, "SELECT pg_catalog.pg_reload_conf()::text");
    }
}

impl Drop for StandbyHold {
    fn drop(&mut self) {
        Self::set(&self.0, "");
    }
}

/// Runs `engine` on this thread while a controller holds it at `kill`, then cancels it; returns its error.
pub(crate) fn kill_at<T>(
    kill: FreshKill,
    endpoints: &Endpoints,
    run: RunId,
    engine: impl FnOnce(&IoControl) -> Result<T>,
) -> Error {
    let cancelled = Arc::new(AtomicBool::new(false));
    let io = IoControl::unbounded(Arc::clone(&cancelled));
    let done = Arc::new(AtomicBool::new(false));
    let (ready, started) = std::sync::mpsc::channel();
    let schema = metadata_schema(run);
    let outcome = std::thread::scope(|scope| {
        let finished = Arc::clone(&done);
        let controller = scope.spawn(move || {
            let (mut source, mut target) = (connect(&endpoints.source), connect(&endpoints.target));
            let mut queued = None;
            let alive = || {
                assert!(
                    !finished.load(Ordering::Acquire),
                    "the engine returned before the {kill:?} kill point"
                );
            };
            let mut fence = connect(&endpoints.target);
            let slot = fence_slot(run);
            scalar(
                &mut fence,
                &format!("SELECT pg_catalog.pg_advisory_lock({FENCE_CLASS},{slot})::text"),
            );
            ready.send(()).unwrap();
            wait_until("the bound engine never waited on the target fence", || {
                alive();
                waiting(
                    &mut target,
                    &format!("l.locktype='advisory' AND l.classid={FENCE_CLASS}::oid"),
                )
            });
            let mut hold = None;
            let mut standby = None;
            match kill {
                FreshKill::Fence => {}
                FreshKill::TargetIntent => {
                    let mut schema_hold = connect(&endpoints.target);
                    schema_hold.query("BEGIN", &control()).unwrap();
                    schema_hold
                        .query(&format!("CREATE SCHEMA {schema}"), &control())
                        .unwrap();
                    hold = Some(schema_hold);
                }
                FreshKill::TargetCommitted => {
                    StandbyHold::set(&endpoints.target, "rpgmigrate_never_connects");
                    standby = Some(StandbyHold(endpoints.target.clone()));
                }
                FreshKill::Confirmed => {
                    // The snapshot transaction keeps its table lock, so this request queues, and every later
                    // lock on the table queues behind it; it ends only when the controller terminates it.
                    let (pid, waiter) = std::sync::mpsc::channel();
                    let info = endpoints.source.clone();
                    queued = Some((
                        std::thread::spawn(move || {
                            let mut table_hold = connect(&info);
                            pid.send(table_hold.backend_pid().unwrap()).unwrap();
                            table_hold.query("BEGIN", &control()).unwrap();
                            let _ = table_hold.query(
                                "LOCK TABLE public.items IN ACCESS EXCLUSIVE MODE",
                                &control(),
                            );
                        }),
                        waiter.recv().unwrap(),
                    ));
                }
            }
            drop(fence);
            match kill {
                FreshKill::Fence => {}
                FreshKill::TargetIntent => wait_until(
                    "the metadata CREATE never waited on the uncommitted schema",
                    || {
                        alive();
                        waiting(
                            &mut target,
                            "l.locktype='transactionid' AND a.datname=pg_catalog.current_database() AND a.backend_type='client backend'",
                        )
                    },
                ),
                FreshKill::TargetCommitted => {
                    wait_until("the metadata COMMIT never waited for a standby", || {
                alive();
                        scalar(&mut target, "SELECT pg_catalog.pg_stat_clear_snapshot()::text");
                        scalar(&mut target, "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE wait_event='SyncRep' AND datname=pg_catalog.current_database()") == "1"
                    })
                }
                FreshKill::Confirmed => {
                    let holder = queued.as_ref().unwrap().1;
                    wait_until("the copy never queued behind the source table lock", || {
                alive();
                        waiting(
                            &mut source,
                            &format!("l.relation='public.items'::pg_catalog.regclass AND a.pid<>{holder}"),
                        )
                    })
                }
            }
            cancelled.store(true, Ordering::Release);
            wait_until("the cancelled engine never returned", || {
                finished.load(Ordering::Acquire)
            });
            drop(standby);
            drop(hold);
            if let Some((thread, holder)) = queued {
                scalar(
                    &mut source,
                    &format!("SELECT pg_catalog.pg_terminate_backend({holder})::text"),
                );
                thread.join().unwrap();
            }
        });
        started.recv().unwrap();
        let outcome = engine(&io);
        done.store(true, Ordering::Release);
        if let Err(panic) = controller.join() {
            std::panic::resume_unwind(panic);
        }
        outcome
    });
    outcome.err().expect("the killed engine completed")
}

/// Runs a restart while a SHARE lock on the marker table holds its generation publish, then cancels it.
pub(crate) fn kill_in_publish<T>(
    endpoints: &Endpoints,
    run: RunId,
    engine: impl FnOnce(&IoControl) -> Result<T>,
) -> Error {
    let schema = metadata_schema(run);
    kill_behind(
        endpoints,
        &format!("LOCK TABLE ONLY {schema}.copy_done IN SHARE MODE"),
        &format!("l.relation='{schema}.copy_done'::pg_catalog.regclass"),
        engine,
    )
}

/// Runs a copy while this run's per-object lock on `oid` holds its large-object worker, then cancels it.
pub(crate) fn kill_in_large_object<T>(
    endpoints: &Endpoints,
    oid: u32,
    engine: impl FnOnce(&IoControl) -> Result<T>,
) -> Error {
    let class = LARGE_OBJECT_LOCK_CLASS;
    kill_behind(
        endpoints,
        &format!("SELECT pg_catalog.pg_advisory_xact_lock({class},{oid})"),
        &format!("l.locktype='advisory' AND l.classid={class}::oid AND l.objid={oid}::oid"),
        engine,
    )
}

/// Runs `engine` while a target transaction holds `lock`, cancels it once a lock matching `waiter`
/// waits, and releases the hold after the engine returned.
fn kill_behind<T>(
    endpoints: &Endpoints,
    lock: &str,
    waiter: &str,
    engine: impl FnOnce(&IoControl) -> Result<T>,
) -> Error {
    let cancelled = Arc::new(AtomicBool::new(false));
    let io = IoControl::unbounded(Arc::clone(&cancelled));
    let done = Arc::new(AtomicBool::new(false));
    let (ready, started) = std::sync::mpsc::channel();
    let outcome = std::thread::scope(|scope| {
        let finished = Arc::clone(&done);
        let controller = scope.spawn(move || {
            let mut target = connect(&endpoints.target);
            let mut hold = connect(&endpoints.target);
            hold.query("BEGIN", &control()).unwrap();
            hold.query(lock, &control()).unwrap();
            ready.send(()).unwrap();
            wait_until("the engine never waited behind the held lock", || {
                assert!(
                    !finished.load(Ordering::Acquire),
                    "the engine returned before it waited behind the held lock"
                );
                waiting(&mut target, waiter)
            });
            cancelled.store(true, Ordering::Release);
            wait_until("the cancelled engine never returned", || {
                finished.load(Ordering::Acquire)
            });
            drop(hold);
        });
        started.recv().unwrap();
        let outcome = engine(&io);
        done.store(true, Ordering::Release);
        if let Err(panic) = controller.join() {
            std::panic::resume_unwind(panic);
        }
        outcome
    });
    outcome.err().expect("the killed engine completed")
}

/// Runs a follow until the target records its copy as analyzing, then cancels the streaming run.
pub(crate) fn follow_until_analyzing<T>(
    endpoints: &Endpoints,
    run: RunId,
    engine: impl FnOnce(&IoControl) -> Result<T>,
) -> Result<T> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let io = IoControl::unbounded(Arc::clone(&cancelled));
    let done = Arc::new(AtomicBool::new(false));
    let schema = metadata_schema(run);
    std::thread::scope(|scope| {
        let finished = Arc::clone(&done);
        let controller = scope.spawn(move || {
            let mut target = connect(&endpoints.target);
            let exists = format!("SELECT pg_catalog.to_regclass('{schema}.checkpoint') IS NOT NULL");
            let phase = format!(
                "SELECT coalesce((SELECT clone_state::pg_catalog.jsonb->>'phase' FROM ONLY {schema}.checkpoint WHERE copy_consistent_point IS NOT NULL),'')"
            );
            wait_until("the restarted follow never anchored its copy", || {
                finished.load(Ordering::Acquire)
                    || scalar(&mut target, &exists) == "t"
                        && scalar(&mut target, &phase) == "analyzing"
            });
            cancelled.store(true, Ordering::Release);
        });
        let outcome = engine(&io);
        done.store(true, Ordering::Release);
        if let Err(panic) = controller.join() {
            std::panic::resume_unwind(panic);
        }
        outcome
    })
}
