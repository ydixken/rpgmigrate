use super::error;
use crate::contracts::{Error, ErrorKind, IoControl, Result};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

static UNBOUNDED: OnceLock<Instant> = OnceLock::new();
const OPERATION_TIMEOUT: Duration = Duration::from_secs(300);

impl IoControl {
    pub(crate) fn unbounded(cancelled: Arc<AtomicBool>) -> Self {
        Self {
            deadline: *UNBOUNDED.get_or_init(Instant::now),
            cancelled,
        }
    }

    pub(crate) fn is_expired(&self) -> bool {
        !self.is_unbounded() && Instant::now() >= self.deadline
    }

    fn is_unbounded(&self) -> bool {
        UNBOUNDED
            .get()
            .is_some_and(|deadline| self.deadline == *deadline)
    }
}

pub(crate) fn operation_control(io: &IoControl) -> IoControl {
    let deadline = Instant::now() + OPERATION_TIMEOUT;
    IoControl {
        deadline: if io.is_unbounded() {
            deadline
        } else {
            deadline.min(io.deadline)
        },
        cancelled: Arc::clone(&io.cancelled),
    }
}

pub(super) fn check(io: &IoControl) -> Result<()> {
    if io.cancelled.load(Ordering::Acquire) {
        return Err(Error::cancelled(
            ErrorKind::Pgsql,
            "PostgreSQL operation cancelled",
        ));
    }
    if io.is_expired() {
        return Err(error("PostgreSQL operation deadline exceeded"));
    }
    Ok(())
}

pub(super) fn wait(
    fd: libc::c_int,
    events: libc::c_short,
    io: &IoControl,
) -> Result<libc::c_short> {
    wait_until(fd, events, io.deadline, io)?
        .ok_or_else(|| error("PostgreSQL operation deadline exceeded"))
}

pub(super) fn wait_until(
    fd: libc::c_int,
    events: libc::c_short,
    wake_at: Instant,
    io: &IoControl,
) -> Result<Option<libc::c_short>> {
    if fd < 0 {
        return Err(error("PostgreSQL connection has no socket"));
    }
    loop {
        check(io)?;
        if Instant::now() >= wake_at {
            return Ok(None);
        }
        let remaining = io
            .deadline
            .min(wake_at)
            .saturating_duration_since(Instant::now());
        // The atomic cancellation flag has no descriptor, so bound its observation latency.
        let timeout = remaining.as_millis().clamp(1, 50) as libc::c_int;
        let mut descriptor = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        // SAFETY: descriptor is initialized and valid for exactly one pollfd.
        let ready = unsafe { libc::poll(&mut descriptor, 1, timeout) };
        check(io)?;
        if ready < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error("PostgreSQL socket polling failed"));
        }
        if descriptor.revents & libc::POLLNVAL != 0 {
            return Err(error("PostgreSQL socket is invalid"));
        }
        if Instant::now() >= wake_at {
            return Ok(None);
        }
        if ready > 0 {
            return Ok(Some(descriptor.revents));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    fn control(duration: Duration) -> IoControl {
        IoControl {
            deadline: Instant::now() + duration,
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn replication_wakeup_preserves_absolute_deadline() {
        let (reader, _writer) = UnixStream::pair().unwrap();
        let io = control(Duration::from_millis(30));
        let deadline = io.deadline;
        assert!(
            wait_until(
                reader.as_raw_fd(),
                libc::POLLIN,
                Instant::now() + Duration::from_millis(5),
                &io
            )
            .unwrap()
            .is_none()
        );
        assert_eq!(io.deadline, deadline);
        let failure = wait_until(
            reader.as_raw_fd(),
            libc::POLLIN,
            deadline + Duration::from_secs(1),
            &io,
        )
        .unwrap_err();
        assert!(failure.message.contains("deadline"));
    }

    #[test]
    fn replication_wait_honors_cancellation_and_readiness() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        let io = control(Duration::from_secs(1));
        writer.write_all(b"ready").unwrap();
        assert_ne!(
            wait_until(reader.as_raw_fd(), libc::POLLIN, io.deadline, &io)
                .unwrap()
                .unwrap()
                & libc::POLLIN,
            0
        );
        io.cancelled.store(true, Ordering::Release);
        assert!(
            wait_until(reader.as_raw_fd(), libc::POLLIN, io.deadline, &io)
                .unwrap_err()
                .message
                .contains("cancel")
        );
        assert!(
            wait_until(
                -1,
                libc::POLLIN,
                io.deadline,
                &control(Duration::from_secs(1))
            )
            .is_err()
        );
    }

    #[test]
    fn cancellation_and_expired_deadline_fail_before_io() {
        let io = control(Duration::from_secs(1));
        assert!(check(&io).is_ok());
        io.cancelled.store(true, Ordering::Relaxed);
        let cancelled = check(&io).unwrap_err();
        assert!(cancelled.message.contains("cancel"));
        assert!(cancelled.is_cancelled());
        let expired = check(&control(Duration::ZERO)).unwrap_err();
        assert!(expired.message.contains("deadline"));
        assert!(!expired.is_cancelled());
    }

    #[test]
    fn ready_socket_does_not_override_expired_deadline() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        writer.write_all(b"ready").unwrap();
        assert!(wait(reader.as_raw_fd(), libc::POLLIN, &control(Duration::ZERO)).is_err());
    }

    #[test]
    fn readiness_and_hangup_allow_the_caller_to_drain_input() {
        let (reader, mut writer) = UnixStream::pair().unwrap();
        writer.write_all(b"ready").unwrap();
        let io = control(Duration::from_secs(1));
        assert_ne!(
            wait(reader.as_raw_fd(), libc::POLLIN, &io).unwrap() & libc::POLLIN,
            0
        );
        drop(writer);
        assert_ne!(
            wait(reader.as_raw_fd(), libc::POLLIN, &io).unwrap() & (libc::POLLIN | libc::POLLHUP),
            0
        );
    }

    #[test]
    fn idle_socket_stops_at_the_original_deadline() {
        let (reader, _writer) = UnixStream::pair().unwrap();
        let io = control(Duration::from_millis(20));
        let start = Instant::now();
        let error = wait(reader.as_raw_fd(), libc::POLLIN, &io).unwrap_err();
        assert!(error.message.contains("deadline"));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn cancellation_interrupts_a_waiting_socket() {
        let (reader, _writer) = UnixStream::pair().unwrap();
        let io = control(Duration::from_secs(5));
        let cancelled = Arc::clone(&io.cancelled);
        let trigger = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            cancelled.store(true, Ordering::Release);
        });
        let start = Instant::now();
        let error = wait(reader.as_raw_fd(), libc::POLLIN, &io).unwrap_err();
        trigger.join().unwrap();
        assert!(error.message.contains("cancel"));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn missing_socket_is_not_treated_as_success() {
        assert!(wait(-1, libc::POLLIN, &control(Duration::from_secs(1))).is_err());
    }

    #[test]
    fn unbounded_run_uses_finite_operations_and_shared_cancellation() {
        let io = IoControl::unbounded(Arc::new(AtomicBool::new(false)));
        assert!(!io.is_expired());
        assert!(check(&io).is_ok());
        let bounded = operation_control(&io);
        assert!(bounded.deadline > Instant::now());
        assert!(bounded.deadline <= Instant::now() + Duration::from_secs(300));
        assert!(Arc::ptr_eq(&bounded.cancelled, &io.cancelled));
        io.cancelled.store(true, Ordering::Release);
        assert!(check(&bounded).unwrap_err().message.contains("cancel"));
    }

    #[test]
    fn operation_budget_never_extends_an_explicit_deadline() {
        let io = control(Duration::from_millis(20));
        assert_eq!(operation_control(&io).deadline, io.deadline);
        let expired = control(Duration::ZERO);
        assert!(expired.is_expired());
        assert!(check(&operation_control(&expired)).is_err());
    }
}
