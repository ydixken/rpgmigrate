mod ffi;
mod io;
mod replication;

#[cfg(test)]
pub(crate) mod follow_receive_tests;
#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod pipeline_live_tests;
#[cfg(test)]
mod prepared_live_tests;
#[cfg(test)]
mod replication_live_tests;
#[cfg(test)]
mod test_support;

#[allow(
    unused_imports,
    reason = "The opaque handles are the crate's transport interface."
)]
pub(crate) use ffi::{CopyBuffer, PgConnection, PgResult, validate_conninfo};

pub(crate) use ffi::CommitObservation;
pub(crate) use ffi::PgParam;
pub(crate) use ffi::Pipeline;
pub(crate) use ffi::utility_environment;
pub(crate) use io::operation_control;

pub(crate) use ffi::TransactionStatus;

pub(crate) use replication::{CreatedLogicalSlot, SlotCreateOutcome};
pub(crate) use replication::{IdentifySystem, ReplicationFrame, ReplicationPoll, StandbyStatus};

pub(crate) use replication::ReplicationEnvelope;

use crate::contracts::{Conninfo, Error, ErrorKind, IoControl, Result};

pub(crate) fn ping(conninfo: &Conninfo, io: &IoControl) -> Result<()> {
    let result = PgConnection::connect(conninfo, io)?.query("SELECT 1", io)?;
    if result.rows() != 1 || result.cell(0, 0)? != Some(b"1") {
        return Err(error("PostgreSQL ping returned an unexpected result"));
    }
    Ok(())
}

pub(crate) fn libpq_version() -> i32 {
    // SAFETY: PQlibVersion has no arguments or mutable application state.
    unsafe { pq_sys::PQlibVersion() }
}

#[track_caller]
fn error(message: &str) -> Error {
    Error::new(ErrorKind::Pgsql, message.to_owned())
}

#[cfg(test)]
mod origin_tests {
    use super::*;

    #[test]
    fn transport_error_records_its_caller() {
        let expected_line = line!() + 1;
        let error = error("PostgreSQL command failed; server details redacted");
        assert_eq!(error.origin.file(), file!());
        assert_eq!(error.origin.line(), expected_line);
        assert!(error.origin.column() > 0);
    }
}
