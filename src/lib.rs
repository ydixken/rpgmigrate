mod apply;
mod catalog;
mod cdc;
mod cli;
mod clone;
mod compare;
mod config;
mod contracts;
mod copy;
mod identity;
mod pg;
mod progress;
mod protocol;
mod schema;
mod snapshot;
mod state;

#[cfg(test)]
#[path = "../build.rs"]
mod build_policy;

use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, OnceLock};

use contracts::{Error, ErrorKind, Invocation, IoControl, Result};

const COMMAND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const COPY_BUFFER_BYTES: usize = 8 * 1024 * 1024;

pub fn run() -> u8 {
    let args = std::env::args_os().collect::<Vec<_>>();
    let environment = cli::Environment(std::env::vars_os().collect());
    let outcome = cli::parse(&args, &environment).and_then(|invocation| {
        let flag_help = matches!(&invocation, Invocation::Help(path) if path.0 != ["help"]);
        let output = execute(invocation)?;
        let written = if flag_help {
            std::io::stderr().lock().write_all(output.as_bytes())
        } else {
            std::io::stdout().lock().write_all(output.as_bytes())
        };
        written.map_err(|_| Error {
            kind: ErrorKind::Internal,
            message: "cannot write command output".into(),
        })
    });
    match outcome {
        Ok(()) => 0,
        Err(error) => {
            let _ = writeln!(std::io::stderr().lock(), "{}", diagnostic(&error));
            error.exit_code()
        }
    }
}

fn execute(invocation: Invocation) -> Result<String> {
    match invocation {
        Invocation::Help(path) => return cli::render_help(&path),
        Invocation::Version { json } => return Ok(cli::render_version(json)),
        Invocation::Unsupported(path) => {
            return Err(Error {
                kind: ErrorKind::BadConfig,
                message: format!("unimplemented command: {}", path.0.join(" ")),
            });
        }
        _ => {}
    }
    let (mut handlers, cancelled) = SignalHandlers::install([libc::SIGINT, libc::SIGTERM])?;
    let io = if matches!(
        invocation,
        Invocation::Clone(_)
            | Invocation::Progress { .. }
            | Invocation::CompareSchema(_)
            | Invocation::CompareData { .. }
    ) {
        IoControl::unbounded(cancelled)
    } else {
        io_control(cancelled)
    };
    let outcome = match invocation {
        Invocation::Ping { source, target } => (|| {
            pg::ping(&source, &io).map_err(|error| endpoint_error(error, ErrorKind::Source))?;
            pg::ping(&target, &io).map_err(|error| endpoint_error(error, ErrorKind::Target))?;
            Ok("{\"command\":\"ping\",\"status\":\"complete\"}\n".into())
        })(),
        Invocation::CopyTableData {
            source,
            target,
            state,
        } => (|| {
            let mut native = state::open(&state)?;
            let mut source = pg::PgConnection::connect(&source, &io)
                .map_err(|error| endpoint_error(error, ErrorKind::Source))?;
            let mut target = pg::PgConnection::connect(&target, &io)
                .map_err(|error| endpoint_error(error, ErrorKind::Target))?;
            let plan = catalog::plan_copy(&mut source, &mut target, &io, COPY_BUFFER_BYTES)?;
            let report = copy::run_copy(&mut source, &mut target, &mut native, &plan, &io)?;
            Ok(format!(
                "{}\n",
                serde_json::json!({
                    "command": "copy table-data",
                    "status": "complete",
                    "run": native.identity().run.0,
                    "state_format_version": native.identity().format_version,
                    "tables": report.tables,
                    "rows": report.rows,
                    "bytes": report.bytes,
                })
            ))
        })(),
        Invocation::Clone(options) => clone::run(options, &io),
        Invocation::Progress { options, json } => {
            let progress = progress::read(&options, &io)?;
            if json {
                protocol::copy_report(
                    protocol::CopyCommand::Progress,
                    progress.work.as_ref(),
                    progress.phase,
                    &progress.copy,
                )
            } else {
                Ok(format!("phase: {}\n", progress.phase))
            }
        }
        Invocation::CompareSchema(options) => {
            let comparison = compare::schema(&options, &io)?;
            comparison_output(
                format!(
                    "{}\n",
                    serde_json::to_string(&comparison).map_err(|_| output_error())?
                ),
                comparison.matched(),
            )
        }
        Invocation::CompareData { options, json } => {
            let comparison = compare::data(&options, &io)?;
            let matched = comparison.iter().all(compare::TableComparison::matched);
            let output = if json {
                format!(
                    "{}\n",
                    serde_json::to_string(&comparison).map_err(|_| output_error())?
                )
            } else {
                format!(
                    "{} tables: {}\n",
                    comparison.len(),
                    if matched { "match" } else { "different" }
                )
            };
            comparison_output(output, matched)
        }
        Invocation::Help(_) | Invocation::Version { .. } | Invocation::Unsupported(_) => {
            Err(Error {
                kind: ErrorKind::Internal,
                message: "invalid runtime invocation".into(),
            })
        }
    };
    let restored = handlers.restore();
    let output = outcome?;
    restored?;
    if io.cancelled.load(Ordering::Relaxed) {
        return Err(Error {
            kind: ErrorKind::Pgsql,
            message: "command cancelled".into(),
        });
    }
    Ok(output)
}

fn output_error() -> Error {
    Error {
        kind: ErrorKind::Internal,
        message: "cannot encode command output".into(),
    }
}

fn comparison_output(output: String, matched: bool) -> Result<String> {
    if matched {
        return Ok(output);
    }
    std::io::stdout()
        .lock()
        .write_all(output.as_bytes())
        .map_err(|_| output_error())?;
    Err(Error {
        kind: ErrorKind::BadState,
        message: "comparison found differences".into(),
    })
}

fn io_control(cancelled: Arc<AtomicBool>) -> IoControl {
    IoControl {
        deadline: std::time::Instant::now() + COMMAND_TIMEOUT,
        cancelled,
    }
}

fn endpoint_error(mut error: Error, endpoint: ErrorKind) -> Error {
    if error.kind == ErrorKind::Pgsql {
        error.kind = endpoint;
    }
    error
}

fn diagnostic(error: &Error) -> String {
    serde_json::json!({
        "level": "error",
        "category": format!("{:?}", error.kind),
        "exit_code": error.exit_code(),
        "error_severity": "ERROR",
        "message": format!("native:{:?}: {}", error.kind, error.message),
    })
    .to_string()
}

static CANCELLATION: OnceLock<Arc<AtomicBool>> = OnceLock::new();
static SIGNAL_FLAG: AtomicPtr<AtomicBool> = AtomicPtr::new(std::ptr::null_mut());

struct SignalHandlers {
    previous: [(libc::c_int, libc::sigaction); 2],
    restored: bool,
}

extern "C" fn cancel(_signal: libc::c_int) {
    let flag = SIGNAL_FLAG.load(Ordering::Acquire);
    if !flag.is_null() {
        // SAFETY: CANCELLATION retains the allocation for the entire process lifetime.
        unsafe { (*flag).store(true, Ordering::Relaxed) };
    }
}

impl SignalHandlers {
    fn install(signals: [libc::c_int; 2]) -> Result<(Self, Arc<AtomicBool>)> {
        let cancelled = CANCELLATION.get_or_init(|| Arc::new(AtomicBool::new(false)));
        cancelled.store(false, Ordering::Relaxed);
        SIGNAL_FLAG.store(Arc::as_ptr(cancelled).cast_mut(), Ordering::Release);

        // SAFETY: zeroed flags/pointers are valid before assigning the handler and mask.
        let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
        action.sa_sigaction = cancel as *const () as libc::sighandler_t;
        // SAFETY: action.sa_mask is writable storage for a complete signal set.
        if unsafe { libc::sigemptyset(&mut action.sa_mask) } != 0 {
            return Err(signal_error());
        }

        let first = replace_handler(signals[0], &action)?;
        let second = match replace_handler(signals[1], &action) {
            Ok(previous) => previous,
            Err(error) => {
                replace_handler(signals[0], &first)?;
                return Err(error);
            }
        };
        Ok((
            Self {
                previous: [(signals[0], first), (signals[1], second)],
                restored: false,
            },
            Arc::clone(cancelled),
        ))
    }

    fn restore(&mut self) -> Result<()> {
        for (signal, previous) in self.previous.iter().rev() {
            replace_handler(*signal, previous)?;
        }
        self.restored = true;
        Ok(())
    }
}

impl Drop for SignalHandlers {
    fn drop(&mut self) {
        if !self.restored {
            let _ = self.restore();
        }
    }
}

fn replace_handler(signal: libc::c_int, action: &libc::sigaction) -> Result<libc::sigaction> {
    let mut previous = std::mem::MaybeUninit::uninit();
    // SAFETY: action is initialized, and previous has space for the output action.
    if unsafe { libc::sigaction(signal, action, previous.as_mut_ptr()) } != 0 {
        return Err(signal_error());
    }
    // SAFETY: sigaction initialized previous on success.
    Ok(unsafe { previous.assume_init() })
}

fn signal_error() -> Error {
    Error {
        kind: ErrorKind::Internal,
        message: "cannot install or restore cancellation signal handlers".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    #[test]
    fn endpoint_context_preserves_configuration_errors() {
        let error = Error {
            kind: ErrorKind::BadConfig,
            message: "transport configuration is invalid".into(),
        };
        let error = endpoint_error(error, ErrorKind::Source);
        assert_eq!(error.kind, ErrorKind::BadConfig);
        assert_eq!(error.exit_code(), 2);

        let error = endpoint_error(
            Error {
                kind: ErrorKind::Pgsql,
                message: "connection failed".into(),
            },
            ErrorKind::Target,
        );
        assert_eq!(error.exit_code(), 7);
    }

    #[test]
    fn command_control_keeps_a_finite_deadline_and_shared_cancellation() {
        let flag = Arc::new(AtomicBool::new(false));
        let before = Instant::now();
        let io = io_control(Arc::clone(&flag));
        let after = Instant::now();
        assert!(io.deadline >= before + COMMAND_TIMEOUT);
        assert!(io.deadline <= after + COMMAND_TIMEOUT);
        flag.store(true, Ordering::Relaxed);
        assert!(io.cancelled.load(Ordering::Relaxed));
    }

    #[test]
    fn structured_errors_preserve_category_and_escape_record_delimiters() {
        let error = Error {
            kind: ErrorKind::BadState,
            message: "commit-outcome-unknown\nsecond line".into(),
        };
        let record = diagnostic(&error);
        assert!(!record.contains('\n'));
        let value: serde_json::Value = serde_json::from_str(&record).unwrap();
        assert_eq!(value["level"], "error");
        assert_eq!(value["category"], "BadState");
        assert_eq!(value["exit_code"], 3);
        assert_eq!(value["error_severity"], "ERROR");
        assert_eq!(
            value["message"],
            format!("native:BadState: {}", error.message)
        );
    }

    #[test]
    fn signals_cancel_and_restore_previous_handlers() {
        if std::env::var_os("RPGMIGRATE_SIGNAL_UNIT_CHILD").is_some() {
            let before = [
                (libc::SIGINT, action(libc::SIGINT)),
                (libc::SIGTERM, action(libc::SIGTERM)),
            ];
            let (guard, cancelled) =
                SignalHandlers::install([libc::SIGINT, libc::SIGTERM]).unwrap();
            for signal in [libc::SIGINT, libc::SIGTERM] {
                cancelled.store(false, Ordering::Relaxed);
                // SAFETY: this subprocess installed a handler for both tested signals.
                assert_eq!(unsafe { libc::raise(signal) }, 0);
                assert!(cancelled.load(Ordering::Relaxed));
            }
            drop(guard);
            for (signal, previous) in &before {
                assert_eq!(action(*signal).sa_sigaction, previous.sa_sigaction);
            }

            assert!(SignalHandlers::install([libc::SIGINT, libc::SIGKILL]).is_err());
            assert_eq!(action(libc::SIGINT).sa_sigaction, before[0].1.sa_sigaction);
            return;
        }
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::signals_cancel_and_restore_previous_handlers",
            ])
            .env("RPGMIGRATE_SIGNAL_UNIT_CHILD", "1")
            .output()
            .unwrap();
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stdout)
        );
    }

    fn action(signal: libc::c_int) -> libc::sigaction {
        let mut action = std::mem::MaybeUninit::uninit();
        // SAFETY: the signal is valid and sigaction writes the complete output on success.
        let status = unsafe { libc::sigaction(signal, std::ptr::null(), action.as_mut_ptr()) };
        assert_eq!(status, 0);
        // SAFETY: the successful call initialized action.
        unsafe { action.assume_init() }
    }
}
