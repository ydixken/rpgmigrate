//! Serial authority requires a dedicated backend, including through session pooling.
//! Transaction/statement pooling and concurrent namespace DDL are unsupported.
//! Durable commits are the only production path until E4 supplies release policy.
//! An interrupted initial .manifest.tmp remains fail-closed in native state.

pub(crate) mod checkpoint;
mod metadata;
mod plan;
pub(crate) mod serial;

use crate::contracts::SnapshotAnchor;
use crate::identity::{CommitLsn, SourceEndLsn, StreamId, TxId, TxSeq};
use checkpoint::DurableProof;
pub(crate) use checkpoint::{
    CloneAuthority, CopyCommitOutcome, CopyPermit, CopyTxn, metadata_inventory,
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
    pub(super) applied: Option<Frontier>,
    pub(super) barrier_epoch: u64,
    pub(super) barrier_counter: u64,
    pub(super) anchor: Option<SnapshotAnchor>,
    pub(super) clone: Option<checkpoint::CloneControl>,
    pub(super) certified_lsn: Option<SourceEndLsn>,
}

#[derive(Debug)]
pub(crate) enum CommitOutcome {
    Acknowledged {
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "E3 serial consumer reads commit receipts")
        )]
        checkpoint: Checkpoint,
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "E3 serial consumer reads commit receipts")
        )]
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
