use std::ffi::{CStr, CString, OsString};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::cli::CommandPath;
pub(crate) use crate::identity::RunId;
use crate::identity::{SourceEndLsn, SourceId, StreamId};

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) const MAX_SNAPSHOT_TOKEN_BYTES: usize = 1024;

#[derive(Debug)]
pub(crate) struct Error {
    pub(crate) kind: ErrorKind,
    pub(crate) message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Some frozen exit categories belong to later command milestones."
    )
)]
pub(crate) enum ErrorKind {
    BadArgs,
    BadConfig,
    BadState,
    Pgsql,
    Pgctl,
    Source,
    Target,
    Reload,
    Internal,
    Fatal,
}

impl Error {
    pub(crate) fn exit_code(&self) -> u8 {
        match self.kind {
            ErrorKind::BadArgs => 1,
            ErrorKind::BadConfig => 2,
            ErrorKind::BadState => 3,
            ErrorKind::Pgsql => 4,
            ErrorKind::Pgctl => 5,
            ErrorKind::Source => 6,
            ErrorKind::Target => 7,
            ErrorKind::Reload => 9,
            ErrorKind::Internal => 12,
            ErrorKind::Fatal => 122,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Conninfo(CString);

impl Conninfo {
    pub(crate) fn new(value: OsString) -> Result<Self> {
        if value.is_empty() {
            return Err(Error {
                kind: ErrorKind::BadConfig,
                message: "connection information is empty".into(),
            });
        }
        CString::new(value.into_encoded_bytes())
            .map(Self)
            .map_err(|_| Error {
                kind: ErrorKind::BadConfig,
                message: "connection information contains a NUL byte".into(),
            })
    }

    pub(crate) fn as_c_str(&self) -> &CStr {
        &self.0
    }
}

impl fmt::Debug for Conninfo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Conninfo([REDACTED])")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IoControl {
    pub(crate) deadline: Instant,
    pub(crate) cancelled: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QualifiedTable {
    pub(crate) schema: String,
    pub(crate) table: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CopyFormat {
    Text,
    #[allow(
        dead_code,
        reason = "The shared contract retains binary COPY; E1 accepts only text."
    )]
    Binary,
}

#[derive(Debug)]
pub(crate) struct CopyTable {
    pub(crate) name: QualifiedTable,
    pub(crate) columns: Vec<String>,
    pub(crate) format: CopyFormat,
}

#[derive(Debug)]
pub(crate) struct CopyPlan {
    pub(crate) tables: Vec<CopyTable>,
    pub(crate) buffer_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotAnchor {
    pub(crate) stream: StreamId,
    pub(crate) snapshot_token: String,
    pub(crate) consistent_point: SourceEndLsn,
    pub(crate) plan_nonce: [u8; 16],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CopyReport {
    pub(crate) tables: u64,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateAction {
    Create,
    Resume,
    Restart,
}

#[derive(Clone, Debug)]
pub(crate) struct StateRequest {
    pub(crate) path: PathBuf,
    pub(crate) action: StateAction,
}

#[derive(Clone, Debug)]
pub(crate) struct StateIdentity {
    pub(crate) run: RunId,
    pub(crate) canonical_path: PathBuf,
    pub(crate) format_version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OperatorIdentity {
    pub(crate) migration_uid: String,
    pub(crate) run: RunId,
    pub(crate) workdir_id: String,
    pub(crate) checkpoint_generation: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct CopyGeneration(pub(crate) u64);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloneSource {
    pub(crate) identity: SourceId,
    pub(crate) timeline: u32,
    pub(crate) major: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloneTarget {
    pub(crate) identity: SourceId,
    pub(crate) major: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SnapshotTicket {
    pub(crate) token: String,
    pub(crate) source: CloneSource,
    pub(crate) generation: CopyGeneration,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloneTable {
    pub(crate) source_oid: u32,
    pub(crate) name: QualifiedTable,
    pub(crate) writable_columns: Vec<String>,
    pub(crate) all_columns: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnedCopyTable {
    pub(crate) table: CloneTable,
    pub(crate) target_oid: u32,
    pub(crate) target_namespace_oid: u32,
    pub(crate) target_owner_oid: u32,
    pub(crate) generation: CopyGeneration,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnedObject {
    pub(crate) class_oid: u32,
    pub(crate) object_oid: u32,
    pub(crate) object_sub_id: i32,
    pub(crate) namespace_oid: Option<u32>,
    pub(crate) namespace_name: Option<String>,
    pub(crate) name: String,
    pub(crate) kind: String,
    pub(crate) owner_oid: u32,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RestorePolicy {
    pub(crate) no_owner: bool,
    pub(crate) no_acl: bool,
    pub(crate) no_comments: bool,
    pub(crate) no_tablespaces: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ClonePlan {
    pub(crate) source: CloneSource,
    pub(crate) target: CloneTarget,
    pub(crate) generation: CopyGeneration,
    pub(crate) tables: Vec<CloneTable>,
    pub(crate) sequences: Vec<QualifiedTable>,
    pub(crate) policy: RestorePolicy,
}

#[derive(Clone, Debug)]
pub(crate) struct CloneOptions {
    pub(crate) source: Conninfo,
    pub(crate) target: Conninfo,
    pub(crate) state: StateRequest,
    pub(crate) operator: Option<OperatorIdentity>,
    pub(crate) table_jobs: usize,
    pub(crate) post_data_jobs: usize,
    pub(crate) policy: RestorePolicy,
    pub(crate) not_consistent: bool,
}

#[derive(Debug)]
pub(crate) enum Invocation {
    Clone(CloneOptions),
    Progress {
        options: CloneOptions,
        json: bool,
    },
    CompareSchema(CloneOptions),
    CompareData {
        options: CloneOptions,
        json: bool,
    },
    Help(CommandPath),
    Version {
        json: bool,
    },
    Ping {
        source: Conninfo,
        target: Conninfo,
    },
    CopyTableData {
        source: Conninfo,
        target: Conninfo,
        state: StateRequest,
    },
    Unsupported(CommandPath),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_anchor_json_preserves_identity_and_requires_exact_fields() {
        let value = serde_json::json!({
            "stream": {
                "run": vec![1; 16],
                "source": {"system_identifier": 42, "database_oid": 7},
                "generation": 9,
            },
            "snapshot_token": "owned-snapshot-token",
            "consistent_point": 16,
            "plan_nonce": vec![2; 16],
        });
        let anchor: SnapshotAnchor = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(anchor).unwrap(), value);
        let mut unknown = value.clone();
        unknown["proof"] = serde_json::json!(true);
        assert!(serde_json::from_value::<SnapshotAnchor>(unknown).is_err());
        for field in ["stream", "snapshot_token", "consistent_point", "plan_nonce"] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<SnapshotAnchor>(missing).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn failures_preserve_nonzero_exit_categories() {
        for (kind, expected) in [
            (ErrorKind::BadArgs, 1),
            (ErrorKind::BadConfig, 2),
            (ErrorKind::BadState, 3),
            (ErrorKind::Pgsql, 4),
            (ErrorKind::Pgctl, 5),
            (ErrorKind::Source, 6),
            (ErrorKind::Target, 7),
            (ErrorKind::Reload, 9),
            (ErrorKind::Internal, 12),
            (ErrorKind::Fatal, 122),
        ] {
            let error = Error {
                kind,
                message: "redacted failure".into(),
            };
            assert_eq!(error.exit_code(), expected, "{kind:?}");
        }
    }

    #[test]
    fn conninfo_debug_redacts_uri_and_keyword_credentials() {
        for value in [
            "postgresql://alice:secret%40value@example.invalid/data",
            "user=alice password='secret value' host=example.invalid",
        ] {
            let conninfo = Conninfo::new(value.into()).unwrap();
            assert_eq!(format!("{conninfo:?}"), "Conninfo([REDACTED])");
            assert_eq!(conninfo.as_c_str().to_bytes(), value.as_bytes());
        }
    }

    #[test]
    fn invalid_conninfo_diagnostics_do_not_include_input() {
        let error = Conninfo::new("password=secret\0host=example.invalid".into()).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadConfig);
        assert_eq!(error.message, "connection information contains a NUL byte");
        assert!(!format!("{error:?}").contains("secret"));
    }

    #[test]
    fn empty_conninfo_does_not_select_an_implicit_database() {
        assert_eq!(
            Conninfo::new(OsString::new()).unwrap_err().kind,
            ErrorKind::BadConfig
        );
    }

    #[cfg(unix)]
    #[test]
    fn conninfo_preserves_non_utf8_platform_bytes() {
        use std::os::unix::ffi::OsStringExt;

        let bytes = b"host=/tmp/non-utf8-\xff".to_vec();
        let conninfo = Conninfo::new(OsString::from_vec(bytes.clone())).unwrap();
        assert_eq!(conninfo.as_c_str().to_bytes(), bytes);
        assert_eq!(format!("{conninfo:?}"), "Conninfo([REDACTED])");
    }
}
