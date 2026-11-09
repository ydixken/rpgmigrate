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

pub(crate) mod connection_files;
use connection_files::ConnectionFiles;

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) const MAX_SNAPSHOT_TOKEN_BYTES: usize = 1024;

#[derive(Debug)]
pub(crate) struct Error {
    pub(crate) kind: ErrorKind,
    pub(crate) message: String,
    pub(crate) origin: &'static std::panic::Location<'static>,
    cancelled: bool,
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
    #[track_caller]
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            origin: std::panic::Location::caller(),
            cancelled: false,
        }
    }

    #[track_caller]
    pub(crate) fn cancelled(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            cancelled: true,
            ..Self::new(kind, message)
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled
    }

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
pub(crate) struct Conninfo {
    raw: CString,
    files: Option<Arc<ConnectionFiles>>,
}

impl Conninfo {
    pub(crate) fn new(value: OsString) -> Result<Self> {
        if value.is_empty() {
            return Err(Error::new(
                ErrorKind::BadConfig,
                "connection information is empty",
            ));
        }
        CString::new(value.into_encoded_bytes())
            .map(|raw| Self { raw, files: None })
            .map_err(|_| {
                Error::new(
                    ErrorKind::BadConfig,
                    "connection information contains a NUL byte",
                )
            })
    }

    pub(crate) fn as_c_str(&self) -> &CStr {
        &self.raw
    }

    pub(crate) fn files(&self) -> Option<&Arc<ConnectionFiles>> {
        self.files.as_ref()
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
    pub(crate) identity_modes: Vec<u8>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) partition_identity_modes: Option<Vec<u8>>,
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FollowLimits {
    pub(crate) channel_bytes: usize,
    pub(crate) channel_messages: usize,
    pub(crate) max_message_bytes: usize,
    pub(crate) group_rows: usize,
    pub(crate) group_bytes: usize,
}

impl Default for FollowLimits {
    fn default() -> Self {
        Self {
            channel_bytes: 8 * 1024 * 1024,
            channel_messages: 1024,
            max_message_bytes: 16 * 1024 * 1024,
            group_rows: 10_000,
            group_bytes: 8 * 1024 * 1024,
        }
    }
}

impl FollowLimits {
    pub(crate) fn validate(self) -> Result<()> {
        if [
            (self.channel_bytes, 8 * 1024 * 1024),
            (self.channel_messages, 1024),
            (self.max_message_bytes, 16 * 1024 * 1024),
            (self.group_rows, 10_000),
            (self.group_bytes, 8 * 1024 * 1024),
        ]
        .iter()
        .any(|(value, maximum)| *value == 0 || value > maximum)
        {
            return Err(Error::new(
                ErrorKind::BadConfig,
                "follow limits must be positive and within the compiled profile budgets",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FollowOptions {
    pub(crate) clone: CloneOptions,
    pub(crate) slot_name: String,
    pub(crate) publication_name: String,
    pub(crate) origin_name: Option<String>,
    pub(crate) apply_group_txns: u64,
    pub(crate) limits: FollowLimits,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicationReceipt {
    pub(crate) name: String,
    pub(crate) oid: u32,
    pub(crate) owner_oid: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceType {
    pub(crate) oid: u32,
    pub(crate) schema: String,
    pub(crate) name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FollowBinding {
    pub(crate) slot: crate::state::SourceSlotReceipt,
    pub(crate) publication: PublicationReceipt,
    pub(crate) source_timeline: u32,
    pub(crate) copy_generation: CopyGeneration,
    pub(crate) source_types: Vec<SourceType>,
}

impl FollowBinding {
    pub(crate) fn stream(&self) -> StreamId {
        self.slot.intent.binding.stream()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StreamOptions {
    pub(crate) state: StateRequest,
    pub(crate) operator: Option<OperatorIdentity>,
    pub(crate) source: Option<Conninfo>,
    pub(crate) target: Option<Conninfo>,
    pub(crate) command: StreamCommand,
    pub(crate) json: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamCommand {
    Get(SentinelField),
    SetEndpos(EndposSource),
    Cleanup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SentinelField {
    All,
    Endpos,
    ReplayLsn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EndposSource {
    Explicit(SourceEndLsn),
    Current,
}

#[derive(Debug)]
pub(crate) enum Invocation {
    Clone(CloneOptions),
    Follow(FollowOptions),
    Stream(StreamOptions),
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
    fn follow_limits_admit_one_bounded_oversize_message() {
        let limits = FollowLimits::default();
        assert_eq!(limits.channel_bytes, 8 * 1024 * 1024);
        assert_eq!(limits.channel_messages, 1024);
        assert_eq!(limits.max_message_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.group_rows, 10_000);
        assert_eq!(limits.group_bytes, 8 * 1024 * 1024);
        limits.validate().unwrap();
        for field in 0..5 {
            let mut invalid = limits;
            match field {
                0 => invalid.channel_bytes = 0,
                1 => invalid.channel_messages = 0,
                2 => invalid.max_message_bytes = 0,
                3 => invalid.group_rows = 0,
                _ => invalid.group_bytes = 0,
            }
            assert!(invalid.validate().is_err(), "field {field}");
        }
        let oversized = FollowLimits {
            max_message_bytes: 16 * 1024 * 1024 + 1,
            ..limits
        };
        assert!(oversized.validate().is_err());
        for field in 0..4 {
            let mut invalid = limits;
            match field {
                0 => invalid.channel_bytes += 1,
                1 => invalid.channel_messages += 1,
                2 => invalid.group_rows += 1,
                _ => invalid.group_bytes += 1,
            }
            assert!(invalid.validate().is_err(), "field {field}");
        }
    }

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
    fn error_origin_records_constructor_and_forwarded_callers() {
        #[track_caller]
        fn forwarded() -> Error {
            Error::new(ErrorKind::BadState, "bounded failure")
        }

        let direct_line = line!() + 1;
        let direct = Error::new(ErrorKind::BadState, "bounded failure");
        assert_eq!(direct.origin.file(), file!());
        assert_eq!(direct.origin.line(), direct_line);
        assert!(direct.origin.column() > 0);

        let forwarded_line = line!() + 1;
        let forwarded = forwarded();
        assert_eq!(forwarded.origin.file(), file!());
        assert_eq!(forwarded.origin.line(), forwarded_line);
        assert!(forwarded.origin.column() > 0);
    }

    #[test]
    fn cancellation_marker_survives_endpoint_context_with_its_origin() {
        let expected_line = line!() + 1;
        let error = Error::cancelled(ErrorKind::Pgsql, "PostgreSQL operation cancelled");
        assert_eq!(error.origin.file(), file!());
        assert_eq!(error.origin.line(), expected_line);
        let origin = error.origin;
        let error = crate::endpoint_error(error, ErrorKind::Source);
        assert!(error.is_cancelled());
        assert_eq!(error.kind, ErrorKind::Source);
        assert_eq!(error.exit_code(), 6);
        assert_eq!(error.message, "PostgreSQL operation cancelled");
        assert!(std::ptr::eq(error.origin, origin));
        assert!(!Error::new(ErrorKind::Pgsql, "PostgreSQL operation cancelled").is_cancelled());
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
            let error = Error::new(kind, "redacted failure");
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
