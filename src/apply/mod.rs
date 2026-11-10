//! Serial authority requires a dedicated backend, including through session pooling.
//! Transaction/statement pooling and concurrent namespace DDL are unsupported.
//! Follow feedback requires a logged synchronous target commit.
//! An interrupted initial .manifest.tmp remains fail-closed in native state.

pub(crate) mod checkpoint;
pub(crate) mod follow;
mod metadata;
mod plan;
pub(crate) mod serial;

use crate::contracts::SnapshotAnchor;
use crate::identity::{CommitLsn, SourceEndLsn, StreamId, TxId, TxSeq};
use checkpoint::DurableProof;
pub(crate) use checkpoint::{
    CloneAuthority, CopyCommitOutcome, CopyPermit, CopyTxn, LargeObjectDone, LargeObjectPermit,
    LargeObjectPhase, LargeObjectTxn, metadata_inventory,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Frontier {
    pub(crate) tx: TxId,
    pub(crate) order: TxSeq,
    pub(crate) commit: Option<CommitLsn>,
    pub(crate) end: SourceEndLsn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Checkpoint {
    pub(crate) stream: StreamId,
    pub(crate) applied: Frontier,
    pub(crate) barrier_epoch: u64,
    pub(crate) barrier_counter: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ControlRow {
    pub(self) drain: Option<checkpoint::DrainRecord>,
    pub(super) follow_completed: bool,
    pub(super) applied: Option<Frontier>,
    pub(super) barrier_epoch: u64,
    pub(super) barrier_counter: u64,
    pub(super) anchor: Option<SnapshotAnchor>,
    pub(super) clone: Option<checkpoint::CloneControl>,
    pub(super) certified_lsn: Option<SourceEndLsn>,
    pub(super) follow_binding: Option<crate::state::FollowControl>,
}

#[derive(Debug)]
pub(crate) enum CommitOutcome {
    Acknowledged {
        checkpoint: Checkpoint,
        durable: Option<DurableProof>,
    },
    Unknown,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum TargetCommit {
    Synchronous,
    #[cfg(test)]
    Asynchronous,
}
