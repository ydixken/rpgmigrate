use std::fmt;

use serde::{Deserialize, Serialize};

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
    Binary(Vec<u8>),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => f.write_str("Null"),
            Self::UnchangedToast => f.write_str("UnchangedToast"),
            Self::Text(bytes) => f.debug_struct("Text").field("bytes", &bytes.len()).finish(),
            Self::Binary(bytes) => f
                .debug_struct("Binary")
                .field("bytes", &bytes.len())
                .finish(),
        }
    }
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
        old: Option<Vec<Value>>,
        new: Vec<Value>,
    },
    Delete {
        relation: RelationKey,
        old: Vec<Value>,
    },
    Truncate {
        relations: Vec<RelationKey>,
        cascade: bool,
        restart_identity: bool,
    },
}
