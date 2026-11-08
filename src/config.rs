use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;

use clap::ArgMatches;
use clap::parser::ValueSource;

use crate::cli::{CommandPath, Environment, Parser};
use crate::contracts::{
    CloneOptions, Conninfo, Error, ErrorKind, Invocation, RestorePolicy, Result, StateAction,
    StateRequest,
};

const COMMON_ENV: &[(&str, &str, bool)] = &[
    ("PGCOPYDB_SOURCE_PGURI", "source", false),
    ("PGCOPYDB_TARGET_PGURI", "target", false),
    ("PGCOPYDB_TABLE_JOBS", "table-jobs", false),
    ("PGCOPYDB_INDEX_JOBS", "index-jobs", false),
    ("PGCOPYDB_RESTORE_JOBS", "restore-jobs", false),
    ("PGCOPYDB_LARGE_OBJECTS_JOBS", "large-objects-jobs", false),
    (
        "PGCOPYDB_SPLIT_TABLES_LARGER_THAN",
        "split-tables-larger-than",
        false,
    ),
    ("PGCOPYDB_SPLIT_MAX_PARTS", "split-max-parts", false),
    (
        "PGCOPYDB_ESTIMATE_TABLE_SIZES",
        "estimate-table-sizes",
        true,
    ),
    ("PGCOPYDB_SNAPSHOT", "snapshot", false),
    ("PGCOPYDB_OUTPUT_PLUGIN", "plugin", false),
    (
        "PGCOPYDB_WAL2JSON_NUMERIC_AS_STRING",
        "wal2json-numeric-as-string",
        true,
    ),
    ("PGCOPYDB_DROP_IF_EXISTS", "drop-if-exists", true),
    ("PGCOPYDB_FAIL_FAST", "fail-fast", true),
    ("PGCOPYDB_SKIP_VACUUM", "skip-vacuum", true),
    ("PGCOPYDB_SKIP_ANALYZE", "skip-analyze", true),
    ("PGCOPYDB_SKIP_DB_PROPERTIES", "skip-db-properties", true),
    ("PGCOPYDB_SKIP_CTID_SPLIT", "skip-split-by-ctid", true),
    ("PGCOPYDB_SKIP_TABLESPACES", "no-tablespaces", true),
    ("PGCOPYDB_USE_COPY_BINARY", "use-copy-binary", true),
    (
        "PGCOPYDB_REPLAY_NO_OP_UPDATES",
        "replay-no-op-updates",
        true,
    ),
];

const STARTUP_ENV: &[(&str, &str, bool)] = &[
    ("PGCOPYDB_LOG_JSON", "log-json", true),
    ("PGCOPYDB_LOG_JSON_FILE", "log-json-file", true),
    ("PGCOPYDB_LOG_TIME_FORMAT", "log-time-format", false),
    ("PGCOPYDB_LOG_FILENAME", "log-filename", false),
    ("PGCOPYDB_DEBUG_BIN_PATH", "debug-bin-path", false),
];

enum Value {
    Flag(u8),
    Text(OsString),
}

struct Setting {
    value: Value,
    environment: Option<String>,
}

impl Setting {
    fn error(&self, name: &str, reason: &str) -> Error {
        if let Some(environment) = &self.environment {
            Error {
                kind: ErrorKind::BadConfig,
                message: format!("invalid {environment}: {reason}"),
            }
        } else {
            Error {
                kind: ErrorKind::BadArgs,
                message: format!("invalid --{name}: {reason}"),
            }
        }
    }

    fn active(&self) -> bool {
        !matches!(self.value, Value::Flag(0))
    }
}

pub(crate) struct Settings {
    parser: Parser,
    options: BTreeMap<String, Setting>,
    operands: Vec<OsString>,
}

impl Settings {
    pub(crate) fn resolve(
        parser: Parser,
        leaf: &ArgMatches,
        root: &ArgMatches,
        env: &Environment,
    ) -> Result<Self> {
        let mut settings = Self {
            parser,
            options: BTreeMap::new(),
            operands: Vec::new(),
        };
        settings.read_environment(STARTUP_ENV, env)?;
        if !matches!(parser, Parser::Root | Parser::Version | Parser::Group) {
            for &(name, option, boolean) in COMMON_ENV {
                if parser != Parser::List
                    || (option == "target" && leaf.try_contains_id("target").is_ok())
                    || matches!(
                        option,
                        "source"
                            | "split-tables-larger-than"
                            | "split-max-parts"
                            | "estimate-table-sizes"
                    )
                {
                    settings.read_environment(&[(name, option, boolean)], env)?;
                }
            }
        }
        if matches!(parser, Parser::Copy | Parser::Stream) {
            settings.read_environment(
                &[
                    ("PGCOPYDB_HOST", "host", false),
                    ("PGCOPYDB_PORT", "port", false),
                ],
                env,
            )?;
        }
        settings.read_arguments(root)?;
        if !std::ptr::eq(root, leaf) {
            settings.read_arguments(leaf)?;
        }
        if let Ok(Some(operands)) = leaf.try_get_many::<OsString>("operands") {
            settings.operands = operands.cloned().collect();
        }
        Ok(settings)
    }

    fn read_environment(
        &mut self,
        definitions: &[(&str, &str, bool)],
        env: &Environment,
    ) -> Result<()> {
        for &(name, option, boolean) in definitions {
            if let Some(value) = env.0.get(OsStr::new(name)) {
                let mut setting = Setting {
                    value: Value::Text(value.clone()),
                    environment: Some(name.into()),
                };
                if boolean {
                    let parsed = value
                        .to_str()
                        .and_then(parse_bool)
                        .ok_or_else(|| setting.error(option, "expected a boolean"))?;
                    setting.value = Value::Flag(u8::from(parsed));
                }
                // The baseline validates conninfo only after command-line overrides.
                if !matches!(option, "source" | "target") {
                    validate_value(option, &setting, self.parser)?;
                }
                self.options.insert(option.into(), setting);
            }
        }
        Ok(())
    }

    fn read_arguments(&mut self, matches: &ArgMatches) -> Result<()> {
        for id in matches.ids() {
            let name = id.as_str();
            if name == "operands" || matches.value_source(name) != Some(ValueSource::CommandLine) {
                continue;
            }
            if let Ok(Some(values)) = matches.try_get_many::<OsString>(name) {
                for value in values {
                    let setting = Setting {
                        value: Value::Text(value.clone()),
                        environment: None,
                    };
                    validate_value(name, &setting, self.parser)?;
                    self.options.insert(name.into(), setting);
                }
            } else if let Ok(Some(count)) = matches.try_get_one::<u8>(name) {
                self.options.insert(
                    name.into(),
                    Setting {
                        value: Value::Flag(*count),
                        environment: None,
                    },
                );
            }
        }
        Ok(())
    }

    fn text(&self, name: &str) -> Option<&OsStr> {
        match &self.options.get(name)?.value {
            Value::Text(text) => Some(text),
            Value::Flag(_) => None,
        }
    }

    fn count(&self, name: &str) -> u8 {
        match self.options.get(name).map(|setting| &setting.value) {
            Some(Value::Flag(count)) => *count,
            _ => 0,
        }
    }

    fn enabled(&self, name: &str) -> bool {
        self.count(name) > 0
    }

    fn invalid_combination(&self, names: &[&str], reason: &str) -> Error {
        if let Some((name, setting)) = names.iter().find_map(|name| {
            self.options
                .get(*name)
                .filter(|setting| setting.environment.is_some())
                .map(|setting| (*name, setting))
        }) {
            setting.error(name, reason)
        } else {
            Error {
                kind: ErrorKind::BadArgs,
                message: reason.into(),
            }
        }
    }

    pub(crate) fn validate(&self, parser: Parser, path: &CommandPath) -> Result<()> {
        if parser == Parser::Version && !self.operands.is_empty() {
            return Err(self.invalid_combination(&[], "version does not accept operands"));
        }
        if self.enabled("resume") && self.enabled("restart") {
            return Err(self.invalid_combination(
                &["resume", "restart"],
                "--resume and --restart cannot be combined",
            ));
        }
        if self.enabled("wal2json-numeric-as-string")
            && self.text("plugin") != Some(OsStr::new("wal2json"))
        {
            return Err(self.invalid_combination(
                &["wal2json-numeric-as-string", "plugin"],
                "--wal2json-numeric-as-string requires --plugin=wal2json",
            ));
        }
        let relaxed = self.text("durability") == Some(OsStr::new("relaxed"));
        let transactions = self.text("loss-max-transactions").is_some();
        let bytes = self.text("loss-max-bytes").is_some();
        if relaxed && !(transactions && bytes) {
            return Err(self.invalid_combination(
                &[],
                "--durability=relaxed requires --loss-max-transactions and --loss-max-bytes",
            ));
        }
        if !relaxed && (transactions || bytes) {
            return Err(self.invalid_combination(
                &[],
                "--loss-max-transactions and --loss-max-bytes require --durability=relaxed",
            ));
        }
        if parser == Parser::Sentinel {
            let selectors = [
                "startpos",
                "endpos",
                "apply",
                "write-lsn",
                "transform-lsn",
                "flush-lsn",
                "replay-lsn",
            ];
            if selectors
                .iter()
                .map(|name| u64::from(self.count(name)))
                .sum::<u64>()
                > 1
            {
                return Err(
                    self.invalid_combination(&[], "select exactly one sentinel field at most once")
                );
            }
            if self.enabled("current") && self.text("source").is_none() {
                return Err(self.invalid_combination(&[], "--current requires --source"));
            }
            if !self.enabled("help") && !self.enabled("version") {
                self.validate_sentinel_operands(path)?;
            }
        } else if parser == Parser::Stream
            && !self.operands.is_empty()
            && !(path.0 == ["stream", "apply"] && self.operands == [OsString::from("-")])
        {
            return Err(
                self.invalid_combination(&[], "this stream command does not accept these operands")
            );
        }
        Ok(())
    }

    fn validate_sentinel_operands(&self, path: &CommandPath) -> Result<()> {
        let count = match path
            .0
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .as_slice()
        {
            ["stream", "sentinel", "setup"] => 2,
            ["stream", "sentinel", "set", "startpos"] => 1,
            ["stream", "sentinel", "set", "endpos"] => usize::from(!self.enabled("current")),
            _ => 0,
        };
        if self.operands.len() != count
            || self
                .operands
                .iter()
                .any(|value| !value.to_str().is_some_and(valid_lsn))
        {
            return Err(
                self.invalid_combination(&[], "invalid sentinel operands; use the command help")
            );
        }
        Ok(())
    }

    pub(crate) fn reject_output_options(&self) -> Result<()> {
        for (name, setting) in &self.options {
            if setting.active()
                && ((name.starts_with("log-") && name != "log-json") || name == "debug-bin-path")
            {
                return Err(unsupported_option(name));
            }
        }
        Ok(())
    }

    pub(crate) fn invocation(
        &self,
        parser: Parser,
        path: CommandPath,
        env: &Environment,
    ) -> Result<Invocation> {
        let operator = crate::protocol::operator_identity(env)?;
        let clone_route = path.0 == ["clone"];
        let progress = path.0 == ["list", "progress"];
        let compare_schema = path.0 == ["compare", "schema"];
        let compare_data = path.0 == ["compare", "data"];
        let native_route = clone_route || progress || compare_schema || compare_data;
        if operator.is_some() && !native_route {
            return Err(Error {
                kind: ErrorKind::BadConfig,
                message: "unimplemented operator identity state binding; no database or state action was started".into(),
            });
        }
        if validate_apply_group(env)? {
            return Err(Error {
                kind: ErrorKind::BadConfig,
                message: "unimplemented PGCOPYDB_APPLY_GROUP_TXNS; no database or state action was started".into(),
            });
        }
        self.validate_connections(parser)?;
        if native_route {
            let options = self.clone_options(env, operator, !clone_route)?;
            return Ok(if clone_route {
                Invocation::Clone(options)
            } else if progress {
                Invocation::Progress {
                    options,
                    json: self.enabled("json"),
                }
            } else if compare_schema {
                Invocation::CompareSchema(options)
            } else {
                Invocation::CompareData {
                    options,
                    json: self.enabled("json"),
                }
            });
        }
        self.runtime_invocation(path, env)
    }

    fn clone_options(
        &self,
        env: &Environment,
        operator: Option<crate::contracts::OperatorIdentity>,
        read_only: bool,
    ) -> Result<CloneOptions> {
        if !self.operands.is_empty() {
            return Err(
                self.invalid_combination(&[], "native clone commands do not accept operands")
            );
        }
        for (name, setting) in &self.options {
            if !setting.active() {
                continue;
            }
            let allowed = matches!(name.as_str(), "source" | "target" | "dir" | "log-json")
                || (read_only && name == "json")
                || (!read_only
                    && matches!(
                        name.as_str(),
                        "resume"
                            | "restart"
                            | "not-consistent"
                            | "table-jobs"
                            | "index-jobs"
                            | "restore-jobs"
                            | "no-owner"
                            | "no-acl"
                            | "no-comments"
                            | "no-tablespaces"
                            | "skip-large-objects"
                    ));
            if !allowed {
                return Err(unsupported_option(name));
            }
        }
        if self.enabled("not-consistent") && !self.enabled("resume") {
            return Err(
                self.invalid_combination(&["not-consistent"], "--not-consistent requires --resume")
            );
        }
        let jobs = |name: &str| -> Option<usize> {
            self.text(name)
                .and_then(OsStr::to_str)
                .and_then(parse_integer)
                .and_then(|value| usize::try_from(value).ok())
        };
        let index_jobs = jobs("index-jobs");
        let restore_jobs = jobs("restore-jobs");
        if matches!((index_jobs, restore_jobs), (Some(index), Some(restore)) if index != restore) {
            return Err(self.invalid_combination(
                &["index-jobs", "restore-jobs"],
                "--index-jobs and --restore-jobs must agree in the plain clone profile",
            ));
        }
        let connection = |name| -> Result<Conninfo> {
            self.text(name)
                .ok_or_else(|| {
                    self.invalid_combination(
                        &[],
                        &format!("--{name} is required for this native command"),
                    )
                })
                .and_then(|value| Conninfo::new(value.to_owned()))
        };
        let action = if read_only || self.enabled("resume") {
            StateAction::Resume
        } else if self.enabled("restart") {
            StateAction::Restart
        } else {
            StateAction::Create
        };
        Ok(CloneOptions {
            source: connection("source")?,
            target: connection("target")?,
            state: StateRequest {
                path: self.state_path(env)?,
                action,
            },
            operator,
            table_jobs: jobs("table-jobs").unwrap_or(4),
            post_data_jobs: restore_jobs.or(index_jobs).unwrap_or(4),
            policy: RestorePolicy {
                no_owner: self.enabled("no-owner"),
                no_acl: self.enabled("no-acl"),
                no_comments: self.enabled("no-comments"),
                no_tablespaces: self.enabled("no-tablespaces"),
            },
            not_consistent: self.enabled("not-consistent"),
        })
    }

    fn validate_connections(&self, parser: Parser) -> Result<()> {
        for name in ["source", "target"] {
            if let Some(setting) = self.options.get(name) {
                validate_value(name, setting, parser)?;
            }
        }
        let needs_source = matches!(
            parser,
            Parser::Copy
                | Parser::Compare
                | Parser::Dump
                | Parser::List
                | Parser::Snapshot
                | Parser::Ping
        ) || (parser == Parser::Stream
            && self.text("target").is_some_and(|target| target != "-"));
        let needs_target = matches!(
            parser,
            Parser::Copy | Parser::Compare | Parser::Restore | Parser::Ping
        );
        if needs_source && self.text("source").is_none() {
            return Err(self.invalid_combination(
                &[],
                "--source is required; use PGCOPYDB_SOURCE_PGURI or an explicit connection",
            ));
        }
        if needs_target && self.text("target").is_none() {
            return Err(self.invalid_combination(
                &[],
                "--target is required; use PGCOPYDB_TARGET_PGURI or an explicit connection",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn contract_values(
        &self,
        parser: Parser,
        env: &Environment,
    ) -> Result<serde_json::Value> {
        crate::protocol::operator_run(env)?;
        validate_apply_group(env)?;
        self.validate_connections(parser)?;
        Ok(serde_json::json!({
            "effective_source": self.text("source").and_then(OsStr::to_str),
            "effective_target": self.text("target").and_then(OsStr::to_str),
            "table_jobs": self.text("table-jobs").and_then(OsStr::to_str),
            "log_json": self.enabled("log-json"),
        }))
    }

    fn runtime_invocation(&self, path: CommandPath, env: &Environment) -> Result<Invocation> {
        let copy = path.0 == ["copy", "table-data"];
        let ping = path.0 == ["ping"];
        if !copy && !ping {
            return Ok(Invocation::Unsupported(path));
        }
        if !self.operands.is_empty() {
            return Err(Error { kind: ErrorKind::BadConfig, message: "unimplemented operands: this command does not select a table by positional argument".into() });
        }
        for (name, setting) in &self.options {
            if matches!(name.as_str(), "source" | "target") || !setting.active() {
                continue;
            }
            if copy && matches!(name.as_str(), "dir" | "resume" | "restart") {
                continue;
            }
            if copy
                && name == "table-jobs"
                && self
                    .text(name)
                    .and_then(OsStr::to_str)
                    .and_then(parse_integer)
                    == Some(1)
            {
                continue;
            }
            // Unrelated common environment settings have no effect on baseline ping.
            if ping
                && setting.environment.is_some()
                && COMMON_ENV.iter().any(|(_, option, _)| option == name)
            {
                continue;
            }
            return Err(unsupported_option(name));
        }
        let source = Conninfo::new(self.text("source").expect("required source").to_owned())?;
        let target = Conninfo::new(self.text("target").expect("required target").to_owned())?;
        if ping {
            Ok(Invocation::Ping { source, target })
        } else {
            let action = if self.enabled("restart") {
                StateAction::Restart
            } else if self.enabled("resume") {
                StateAction::Resume
            } else {
                StateAction::Create
            };
            Ok(Invocation::CopyTableData {
                source,
                target,
                state: StateRequest {
                    path: self.state_path(env)?,
                    action,
                },
            })
        }
    }

    fn state_path(&self, env: &Environment) -> Result<PathBuf> {
        if let Some(path) = self.text("dir") {
            return Ok(PathBuf::from(path));
        }
        let base = env
            .0
            .get(OsStr::new("TMPDIR"))
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        // Resolve only the platform alias, never an explicit state directory.
        #[cfg(target_os = "macos")]
        let base =
            if base == std::path::Path::new("/tmp") || base == std::path::Path::new("/var/tmp") {
                std::fs::canonicalize(&base).map_err(|_| Error {
                    kind: ErrorKind::BadConfig,
                    message: "default temporary directory is unavailable".into(),
                })?
            } else {
                base
            };
        Ok(base.join("rpgmigrate"))
    }
}

fn validate_apply_group(env: &Environment) -> Result<bool> {
    let Some(value) = env.0.get(OsStr::new("PGCOPYDB_APPLY_GROUP_TXNS")) else {
        return Ok(false);
    };
    if !value
        .to_str()
        .is_some_and(|value| bounded_integer(value, 1, u64::MAX))
    {
        return Err(Error {
            kind: ErrorKind::BadConfig,
            message: "invalid PGCOPYDB_APPLY_GROUP_TXNS: expected a positive integer".into(),
        });
    }
    Ok(true)
}

fn unsupported_option(name: &str) -> Error {
    Error {
        kind: ErrorKind::BadConfig,
        message: format!("unimplemented option --{name}; no database or state action was started"),
    }
}

fn validate_value(name: &str, setting: &Setting, parser: Parser) -> Result<()> {
    let Value::Text(value) = &setting.value else {
        return Ok(());
    };
    if value.as_encoded_bytes().contains(&0) {
        return Err(setting.error(name, "NUL bytes are not allowed"));
    }
    let text = value.to_str();
    let reason = match name {
        "table-jobs" | "index-jobs" | "restore-jobs" | "large-objects-jobs" => {
            (!text.is_some_and(|value| bounded_integer(value, 1, 128))).then_some("expected an integer from 1 to 128")
        }
        "split-max-parts" | "apply-jobs" => {
            (!text.is_some_and(|value| bounded_integer(value, 1, i32::MAX as u64))).then_some("expected a positive 32-bit integer")
        }
        "port" => (!text.is_some_and(|value| bounded_integer(value, 1, 65535))).then_some("expected an integer from 1 to 65535"),
        "loss-max-transactions" => (!text.is_some_and(|value| bounded_integer(value, 1, u64::MAX))).then_some("expected a finite positive transaction count"),
        "split-tables-larger-than" | "max-replaydb-size" | "loss-max-bytes" => {
            (!text.and_then(parse_bytes).is_some_and(|bytes| name != "loss-max-bytes" || bytes > 0)).then_some("expected an integer byte count with B, kB, MB, GB, TB, PB, or EB units, without overflow")
        }
        "plugin" => (!matches!(text, Some("pgoutput" | "test_decoding" | "wal2json"))).then_some("expected pgoutput, test_decoding, or wal2json"),
        "durability" => (!matches!(text, Some("durable" | "replay-safe" | "relaxed"))).then_some("expected durable, replay-safe, or relaxed"),
        "endpos" => (!text.is_some_and(valid_lsn)).then_some("expected an LSN with two hexadecimal components"),
        "filters" | "requirements" => (!PathBuf::from(value).is_file()).then_some("expected an existing file"),
        "source" | "target" | "dir" | "host" => value.is_empty().then_some("the value must not be empty"),
        _ => None,
    };
    if let Some(reason) = reason {
        return Err(setting.error(name, reason));
    }
    if matches!(name, "source" | "target")
        && !(parser == Parser::Stream && name == "target" && value == "-")
    {
        let conninfo = Conninfo::new(value.clone())
            .map_err(|_| setting.error(name, "invalid connection information"))?;
        crate::pg::validate_conninfo(&conninfo)
            .map_err(|_| setting.error(name, "invalid connection information; details redacted"))?;
    }
    Ok(())
}

fn bounded_integer(text: &str, minimum: u64, maximum: u64) -> bool {
    parse_integer(text).is_some_and(|number| (minimum..=maximum).contains(&number))
}

fn parse_integer(text: &str) -> Option<u64> {
    text.trim_start_matches(|character: char| character.is_ascii_whitespace())
        .parse::<u64>()
        .ok()
}

fn parse_bytes(text: &str) -> Option<u64> {
    let text = text.trim_matches(|character: char| character.is_ascii_whitespace());
    let end = text
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(text.len());
    let number = text[..end].parse::<u64>().ok()?;
    let suffix = text[end..].trim_start_matches(|character: char| character.is_ascii_whitespace());
    let exponent = match suffix {
        "" | "B" => 0,
        "kB" => 1,
        "MB" => 2,
        "GB" => 3,
        "TB" => 4,
        "PB" => 5,
        "EB" => 6,
        _ => return None,
    };
    number.checked_mul(1024_u64.pow(exponent))
}

fn valid_lsn(text: &str) -> bool {
    text.split_once('/').is_some_and(|(high, low)| {
        [high, low].iter().all(|part| {
            (1..=8).contains(&part.len()) && part.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
    })
}

fn parse_bool(text: &str) -> Option<bool> {
    let text = text.to_ascii_lowercase();
    if text.is_empty() || text == "o" {
        return None;
    }
    if text == "1"
        || ["true", "yes", "on"]
            .iter()
            .any(|value| value.starts_with(&text))
    {
        Some(true)
    } else if text == "0"
        || ["false", "no", "off"]
            .iter()
            .any(|value| value.starts_with(&text))
    {
        Some(false)
    } else {
        None
    }
}
