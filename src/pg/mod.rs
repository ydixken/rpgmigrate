mod ffi;
mod io;
mod native;
mod replication;

#[cfg(test)]
pub(crate) use ffi::replication_testing::owned_frame;

#[cfg(test)]
mod live_tests;
#[cfg(test)]
mod passfile_tests;
#[cfg(test)]
mod prepared_live_tests;
#[cfg(test)]
mod replication_live_tests;

#[allow(
    unused_imports,
    reason = "The opaque handles are the crate's transport interface."
)]
pub(crate) use ffi::{CopyBuffer, PgConnection, PgResult, validate_conninfo};

pub(crate) use ffi::PgParam;

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

fn error(message: &str) -> Error {
    Error {
        kind: ErrorKind::Pgsql,
        message: message.to_owned(),
    }
}
