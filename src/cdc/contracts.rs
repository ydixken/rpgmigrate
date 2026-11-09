use std::fmt;

use serde::{Deserialize, Serialize};

use crate::identity::{CommitLsn, SourceEndLsn};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum Plugin {
    #[serde(rename = "pgoutput")]
    Pgoutput,
    #[serde(rename = "test_decoding")]
    TestDecoding,
    #[serde(rename = "wal2json")]
    Wal2Json,
}

impl Plugin {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pgoutput => "pgoutput",
            Self::TestDecoding => "test_decoding",
            Self::Wal2Json => "wal2json",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RelationKey {
    pub(crate) oid: u32,
    pub(crate) generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) type_oid: u32,
    pub(crate) type_mod: i32,
    pub(crate) key: bool,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) enum Value {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::UnchangedToast => f.write_str("UnchangedToast"),
            Self::Text(bytes) => f.debug_struct("Text").field("bytes", &bytes.len()).finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OldTupleKind {
    Key,
    Full,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OldTuple {
    pub(crate) kind: OldTupleKind,
    pub(crate) values: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Begin {
    pub(crate) xid: u32,
    pub(crate) final_lsn: CommitLsn,
    pub(crate) commit_time_micros: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Commit {
    pub(crate) xid: u32,
    pub(crate) commit: CommitLsn,
    pub(crate) end: SourceEndLsn,
    pub(crate) commit_time_micros: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FollowMessage {
    Begin(Begin),
    Event(Event),
    Commit(Commit),
    Keepalive {
        wal_end: SourceEndLsn,
        decoder_idle: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Event {
    Relation {
        key: RelationKey,
        schema: String,
        name: String,
        columns: Vec<Column>,
        replica_identity: u8,
    },
    Insert {
        relation: RelationKey,
        values: Vec<Value>,
    },
    Update {
        relation: RelationKey,
        old: Option<OldTuple>,
        new: Vec<Value>,
    },
    Delete {
        relation: RelationKey,
        old: OldTuple,
    },
    Truncate {
        relations: Vec<RelationKey>,
        cascade: bool,
        restart_identity: bool,
    },
}

#[cfg(test)]
mod follow_tests {
    use super::*;

    #[test]
    fn old_tuple_preserves_full_width_and_redacts_values() {
        let old = OldTuple {
            kind: OldTupleKind::Key,
            values: vec![Value::Null, Value::Text(b"private-row-value".to_vec())],
        };
        assert_eq!(old.values.len(), 2);
        assert_eq!(old.kind, OldTupleKind::Key);
        assert!(!format!("{old:?}").contains("private-row-value"));
    }

    #[test]
    fn commit_message_keeps_source_commit_and_end_distinct() {
        let message = FollowMessage::Commit(Commit {
            xid: 9,
            commit: crate::identity::CommitLsn(32),
            end: crate::identity::SourceEndLsn(48),
            commit_time_micros: 1,
        });
        assert!(matches!(message, FollowMessage::Commit(commit)
            if commit.xid == 9 && commit.commit.0 == 32 && commit.end.0 == 48));
    }
}
