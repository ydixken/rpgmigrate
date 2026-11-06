use std::ffi::{CStr, CString, OsString};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use crate::cli::CommandPath;

pub(crate) type Result<T> = std::result::Result<T, Error>;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RunId(pub(crate) [u8; 16]);

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
pub(crate) struct StateRequest {
    pub(crate) path: PathBuf,
    pub(crate) action: StateAction,
}

#[derive(Debug)]
pub(crate) struct StateIdentity {
    pub(crate) run: RunId,
    pub(crate) canonical_path: PathBuf,
    pub(crate) format_version: u32,
}

#[derive(Debug)]
pub(crate) enum Invocation {
    Help(CommandPath),
    Version,
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
