use std::collections::BTreeMap;
use std::ffi::OsString;

use clap::builder::OsStringValueParser;
use clap::{Arg, ArgAction, ArgMatches, Command};

use crate::config::Settings;
use crate::contracts::{Error, ErrorKind, Invocation, Result};

pub(crate) struct Environment(pub(crate) BTreeMap<OsString, OsString>);

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CommandPath(pub(crate) Vec<String>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Parser {
    Root,
    Version,
    Copy,
    Compare,
    Dump,
    Restore,
    List,
    Stream,
    Sentinel,
    Snapshot,
    Ping,
    Group,
}

fn flag(name: &'static str, short: Option<char>) -> Arg {
    let arg = Arg::new(name).long(name).action(ArgAction::Count);
    if let Some(short) = short {
        arg.short(short)
    } else {
        arg
    }
}

fn value(name: &'static str, short: Option<char>) -> Arg {
    let arg = Arg::new(name)
        .long(name)
        .num_args(1)
        .action(ArgAction::Append)
        .value_parser(OsStringValueParser::new());
    if let Some(short) = short {
        arg.short(short)
    } else {
        arg
    }
}

fn options(parser: Parser) -> Vec<Arg> {
    let mut args = Vec::new();
    if parser == Parser::Group {
        return args;
    }
    if parser == Parser::Version {
        return vec![flag("json", Some('J')), flag("help", Some('h'))];
    }
    args.extend([
        flag("version", Some('V')),
        flag("verbose", Some('v')).visible_alias("notice"),
        flag("debug", Some('d')),
        flag("trace", Some('z')),
        flag("quiet", Some('q')),
        flag("help", Some('h')),
    ]);
    if parser == Parser::Root {
        args.push(flag("json", Some('J')));
        return args;
    }
    args.push(value("source", Some('S')));
    if matches!(
        parser,
        Parser::Copy
            | Parser::Compare
            | Parser::Dump
            | Parser::Restore
            | Parser::Stream
            | Parser::Ping
    ) {
        args.push(value("target", Some('T')));
    }
    if parser != Parser::Ping {
        args.push(value("dir", Some('D')));
    }
    if matches!(parser, Parser::Copy | Parser::Compare) {
        args.push(
            value(
                "table-jobs",
                Some(if parser == Parser::Copy { 'J' } else { 'j' }),
            )
            .visible_alias("jobs"),
        );
    }
    if matches!(
        parser,
        Parser::Copy | Parser::Dump | Parser::Restore | Parser::List
    ) {
        args.push(value("filters", Some('F')).visible_alias("filter"));
    }
    if matches!(
        parser,
        Parser::Copy | Parser::Dump | Parser::Restore | Parser::Stream | Parser::List
    ) {
        args.extend([
            flag("resume", Some('R')),
            flag(
                "not-consistent",
                Some(if parser == Parser::List { 'I' } else { 'C' }),
            ),
            value("snapshot", Some('N')),
        ]);
        if parser != Parser::List {
            args.push(flag("restart", Some('r')));
        }
    }
    if matches!(parser, Parser::Copy | Parser::Stream | Parser::Snapshot) {
        args.extend([
            value("plugin", Some('p')),
            flag("wal2json-numeric-as-string", Some('w')),
            value("slot-name", Some('s')),
        ]);
    }
    if matches!(parser, Parser::Copy | Parser::Stream) {
        args.extend([
            value("origin", Some('o')),
            value("endpos", Some('E')),
            value("host", None),
            value("port", None),
            flag("replay-no-op-updates", None),
            value("durability", None),
            value("apply-jobs", None),
            value("loss-max-transactions", None),
            value("loss-max-bytes", None),
        ]);
    }
    if matches!(parser, Parser::Copy | Parser::Snapshot) {
        args.push(flag("follow", Some('f')));
    }
    if matches!(parser, Parser::Copy | Parser::List) {
        args.extend([
            value("split-tables-larger-than", Some('L')).visible_alias("split-at"),
            value("split-max-parts", Some('u')),
            flag("estimate-table-sizes", Some('m')),
        ]);
    }
    if matches!(parser, Parser::Copy | Parser::Restore) {
        args.extend([
            flag("drop-if-exists", Some('c')),
            flag("no-owner", Some('O')),
            flag("no-comments", Some('X')),
            flag("no-acl", Some('x')),
            value("restore-jobs", Some('j')),
            flag(
                "skip-ext-comments",
                Some(if parser == Parser::Copy { 'M' } else { 'E' }),
            )
            .visible_alias("skip-ext-comment"),
        ]);
    }
    if matches!(parser, Parser::Copy | Parser::Dump | Parser::Restore) {
        args.push(flag("skip-extensions", Some('e')));
    }
    if matches!(parser, Parser::Copy | Parser::Dump) {
        args.push(flag("no-role-passwords", Some('P')));
    }
    match parser {
        Parser::Copy => args.extend([
            value("index-jobs", Some('I')),
            value("large-objects-jobs", Some('b')),
            flag("roles", Some('A')),
            flag("no-tablespaces", Some('y')),
            flag("skip-large-objects", Some('B')).visible_alias("skip-blobs"),
            flag("skip-collations", Some('l')),
            flag("skip-vacuum", Some('U')),
            flag("skip-analyze", Some('a')),
            flag("skip-db-properties", Some('g')),
            flag("skip-split-by-ctid", Some('k')),
            flag("use-copy-binary", Some('n')),
            value("requirements", Some('Q')),
            flag("fail-fast", Some('i')),
            flag("create-slot", Some('t')),
            value("publication", None),
            flag("all-databases", None),
            flag("defer-indexes", None),
        ]),
        Parser::Compare => args.extend([flag("all-databases", Some('a')), flag("json", Some('J'))]),
        Parser::List => args.extend([
            value("schema-name", Some('s')),
            value("table-name", Some('t')),
            flag("list-skipped", Some('x')),
            flag("without-pkey", Some('P')),
            flag("skip-split-by-ctid", None),
            // The baseline's short -k consumes an operand, unlike its long form.
            Arg::new("list-short-k")
                .short('k')
                .num_args(1)
                .action(ArgAction::Append)
                .value_parser(OsStringValueParser::new()),
            flag("force", Some('f')),
            flag("summary", Some('y')),
            flag("available-versions", Some('a')),
            flag("requirements", Some('r')),
            flag("json", Some('J')),
        ]),
        Parser::Stream => args.extend([
            value("max-replaydb-size", None),
            flag("dry-run", None),
            flag("to-stdout", Some('O')),
            flag("from-stdin", Some('I')),
        ]),
        Parser::Sentinel => args.extend([
            value("host", Some('H')),
            value("port", Some('P')),
            flag("startpos", Some('s')),
            flag("endpos", Some('e')),
            flag("apply", Some('a')),
            flag("write-lsn", Some('w')),
            flag("transform-lsn", Some('t')),
            flag("flush-lsn", Some('f')),
            flag("replay-lsn", Some('r')),
            flag("current", Some('C')),
            flag("json", Some('J')),
        ]),
        _ => {}
    }
    args
}

fn base(name: &'static str, parser: Parser) -> Command {
    Command::new(name)
        .disable_help_flag(true)
        .disable_help_subcommand(true)
        .disable_version_flag(true)
        .args(options(parser))
}

fn leaf(name: &'static str, parser: Parser) -> Command {
    base(name, parser).arg(
        Arg::new("operands")
            .num_args(0..)
            .trailing_var_arg(parser != Parser::Version)
            .value_parser(OsStringValueParser::new()),
    )
}

fn group(name: &'static str, parser: Parser, leaves: &[&'static str]) -> Command {
    base(name, Parser::Group)
        .arg(flag("help", Some('h')))
        .subcommands(leaves.iter().map(|name| {
            let command = leaf(name, parser);
            if parser == Parser::List && *name == "progress" {
                command.arg(value("target", Some('T')))
            } else {
                command
            }
        }))
}

fn command() -> Command {
    base("rpgmigrate", Parser::Root)
        .bin_name("rpgmigrate")
        .about("PostgreSQL migration engine")
        .after_help("Runtime support: ping and restricted copy table-data. Other commands fail before database or state changes.")
        .subcommands(["clone", "fork", "follow"].map(|name| leaf(name, Parser::Copy)))
        .subcommand(leaf("snapshot", Parser::Snapshot))
        .subcommand(leaf("ping", Parser::Ping))
        .subcommand(leaf("help", Parser::Group))
        .subcommand(leaf("version", Parser::Version))
        .subcommand(group("compare", Parser::Compare, &["schema", "data"]))
        .subcommand(group("copy", Parser::Copy, &["db", "roles", "extensions", "schema", "data", "table-data", "blobs", "sequences", "indexes", "constraints"]))
        .subcommand(group("dump", Parser::Dump, &["schema", "roles"]))
        .subcommand(group("restore", Parser::Restore, &["schema", "pre-data", "post-data", "roles", "parse-list"]))
        .subcommand(group("list", Parser::List, &["databases", "extensions", "collations", "tables", "table-parts", "sequences", "indexes", "depends", "schema", "progress"]))
        .subcommand(group("stream", Parser::Stream, &["setup", "cleanup", "prune", "prefetch", "catchup", "replay", "receive", "apply"])
            .subcommand(group("sentinel", Parser::Sentinel, &["setup", "get"])
                .subcommand(group("set", Parser::Sentinel, &["startpos", "endpos", "apply", "prefetch"]))))
}

fn parser_for(path: &[String]) -> Parser {
    match path.first().map(String::as_str) {
        None => Parser::Root,
        Some("version") => Parser::Version,
        Some("clone" | "fork" | "follow" | "copy") => Parser::Copy,
        Some("compare") => Parser::Compare,
        Some("dump") => Parser::Dump,
        Some("restore") => Parser::Restore,
        Some("list") => Parser::List,
        Some("snapshot") => Parser::Snapshot,
        Some("ping") => Parser::Ping,
        Some("stream") if path.get(1).is_some_and(|name| name == "sentinel") => Parser::Sentinel,
        Some("stream") => Parser::Stream,
        _ => Parser::Group,
    }
}

fn used(matches: &ArgMatches, name: &str) -> bool {
    matches
        .try_get_one::<u8>(name)
        .ok()
        .flatten()
        .is_some_and(|count| *count > 0)
}

pub(crate) fn parse(args: &[OsString], env: &Environment) -> Result<Invocation> {
    if args.is_empty() {
        return Err(Error {
            kind: ErrorKind::BadArgs,
            message: "argv must include the executable".into(),
        });
    }
    let matches = command().try_get_matches_from(args).map_err(|_| Error {
        kind: ErrorKind::BadArgs,
        // Clap diagnostics can include the original arguments and their secrets.
        message: "invalid command or option syntax; use --help for accepted arguments".into(),
    })?;
    let mut path = CommandPath(Vec::new());
    let mut current = &matches;
    let mut help = used(current, "help");
    let mut version = used(current, "version");
    while let Some((name, submatches)) = current.subcommand() {
        path.0.push(name.to_owned());
        current = submatches;
        help |= used(current, "help");
        version |= used(current, "version");
    }
    let parser = parser_for(&path.0);
    let settings = Settings::resolve(parser, current, &matches, env)?;
    settings.validate(parser, &path)?;
    if help || path.0 == ["help"] {
        return Ok(Invocation::Help(path));
    }
    if version || path.0 == ["version"] {
        settings.reject_output_options()?;
        return Ok(Invocation::Version {
            json: used(current, "json") || used(&matches, "json"),
        });
    }
    let mut tree = command();
    for part in &path.0 {
        tree = tree
            .find_subcommand(part)
            .expect("matched command exists")
            .clone();
    }
    if tree.has_subcommands() {
        return Err(Error {
            kind: ErrorKind::BadArgs,
            message: "a command is required; use --help for available commands".into(),
        });
    }
    settings.invocation(parser, path, env)
}

pub(crate) fn render_help(path: &CommandPath) -> Result<String> {
    let mut tree = command();
    tree.build();
    let mut selected = &mut tree;
    let path = if path.0 == ["help"] {
        &[]
    } else {
        path.0.as_slice()
    };
    for part in path {
        selected = selected.find_subcommand_mut(part).ok_or_else(|| Error {
            kind: ErrorKind::Internal,
            message: "help command path is not registered".into(),
        })?;
    }
    Ok(format!("{}\n", selected.render_help()))
}

#[cfg(test)]
pub(crate) fn contract_oracle(args: &[OsString], env: &Environment) -> Result<serde_json::Value> {
    let matches = command().try_get_matches_from(args).map_err(|_| Error {
        kind: ErrorKind::BadArgs,
        message: "invalid command or option syntax".into(),
    })?;
    let mut current = &matches;
    let mut path = CommandPath(Vec::new());
    while let Some((name, submatches)) = current.subcommand() {
        path.0.push(name.to_owned());
        current = submatches;
    }
    let parser = parser_for(&path.0);
    let settings = Settings::resolve(parser, current, &matches, env)?;
    settings.validate(parser, &path)?;
    if path.0 == ["version"] {
        settings.reject_output_options()?;
        return Ok(serde_json::json!({}));
    }
    settings.contract_values(parser, env)
}

pub(crate) fn render_version(json: bool) -> String {
    if json {
        return format!(
            "{}\n",
            serde_json::to_string(&crate::protocol::Version::current())
                .expect("version fields are serializable")
        );
    }
    format!(
        "rpgmigrate {}\nlibpq {}\n",
        env!("CARGO_PKG_VERSION"),
        crate::pg::libpq_version()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::StateAction;

    fn invoke(args: &[&str], environment: &[(&str, &str)]) -> Result<Invocation> {
        let args = std::iter::once("rpgmigrate")
            .chain(args.iter().copied())
            .map(OsString::from)
            .collect::<Vec<_>>();
        let env = Environment(
            environment
                .iter()
                .map(|(name, value)| (OsString::from(name), OsString::from(value)))
                .collect(),
        );
        parse(&args, &env)
    }

    const CONNECTIONS: &[(&str, &str)] = &[
        ("PGCOPYDB_SOURCE_PGURI", "dbname=source"),
        ("PGCOPYDB_TARGET_PGURI", "dbname=target"),
    ];

    #[test]
    fn clone_uses_effective_four_job_defaults_and_admits_restore_policy() {
        let env = [
            ("PGCOPYDB_SOURCE_PGURI", "host=source.invalid"),
            ("PGCOPYDB_TARGET_PGURI", "host=target.invalid"),
        ];
        let invocation = invoke(&["clone", "--no-owner", "--no-acl"], &env).unwrap();
        assert!(matches!(invocation, Invocation::Clone(options)
            if options.table_jobs == 4 && options.post_data_jobs == 4
                && options.policy.no_owner && options.policy.no_acl));
    }

    #[test]
    fn clone_refuses_conflicting_post_data_jobs_before_io() {
        let env = [
            ("PGCOPYDB_SOURCE_PGURI", "host=source.invalid"),
            ("PGCOPYDB_TARGET_PGURI", "host=target.invalid"),
        ];
        let error =
            invoke(&["clone", "--index-jobs", "2", "--restore-jobs", "3"], &env).unwrap_err();
        assert!(error.message.contains("--index-jobs and --restore-jobs"));
    }

    #[test]
    fn clone_explicit_jobs_and_recovery_flags_are_preserved() {
        let Invocation::Clone(options) = invoke(
            &[
                "clone",
                "--table-jobs",
                "2",
                "--restore-jobs",
                "3",
                "--resume",
                "--not-consistent",
            ],
            CONNECTIONS,
        )
        .unwrap() else {
            panic!("expected clone")
        };
        assert_eq!(options.table_jobs, 2);
        assert_eq!(options.post_data_jobs, 3);
        assert_eq!(options.state.action, StateAction::Resume);
        assert!(options.not_consistent);
        assert!(invoke(&["clone", "--not-consistent"], CONNECTIONS).is_err());
    }

    #[test]
    fn clone_refuses_staged_features_before_database_or_state_actions() {
        for option in [
            "--follow",
            "--roles",
            "--all-databases",
            "--drop-if-exists",
            "--use-copy-binary",
            "--skip-analyze",
            "--skip-extensions",
            "--defer-indexes",
        ] {
            let error = invoke(&["clone", option], CONNECTIONS).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadConfig, "{option}");
            assert!(
                error
                    .message
                    .contains("no database or state action was started"),
                "{option}"
            );
        }
    }

    #[test]
    fn measured_read_only_routes_require_both_connections_and_resume_state() {
        assert!(
            matches!(invoke(&["list", "progress", "--json"], CONNECTIONS).unwrap(),
            Invocation::Progress { options, json: true } if options.state.action == StateAction::Resume)
        );
        assert!(matches!(
            invoke(&["compare", "schema"], CONNECTIONS).unwrap(),
            Invocation::CompareSchema(_)
        ));
        assert!(matches!(
            invoke(&["compare", "data", "--json"], CONNECTIONS).unwrap(),
            Invocation::CompareData { json: true, .. }
        ));
        assert!(invoke(&["list", "progress", "--json"], &CONNECTIONS[..1]).is_err());
    }

    #[test]
    fn json_version_is_identity_free_and_advertises_no_runtime_capabilities() {
        let env = [
            ("PGCOPYDB_SOURCE_PGURI", "invalid connection"),
            ("PGCOPYDB_TARGET_PGURI", "invalid connection"),
            ("RPGMIGRATE_MIGRATION_UID", "invalid"),
            ("RPGMIGRATE_RUN_ID", "invalid"),
            ("RPGMIGRATE_WORKDIR_ID", "invalid"),
            ("RPGMIGRATE_CHECKPOINT_GENERATION", "invalid"),
            ("PGCOPYDB_APPLY_GROUP_TXNS", "invalid"),
            ("PGCOPYDB_LOG_JSON", "on"),
        ];
        let invocation = invoke(&["version", "--json"], &env).unwrap();
        let output = crate::execute(invocation).unwrap();
        assert_eq!(output.lines().count(), 1);
        assert!(output.ends_with('\n'));
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "protocol_version": 1,
                "engine_version": env!("CARGO_PKG_VERSION"),
                "capabilities": [],
            })
        );
    }

    #[test]
    fn supplied_operator_identity_is_validated_and_never_mutates_standalone_state() {
        let identity = [
            (
                "RPGMIGRATE_MIGRATION_UID",
                "12345678-9abc-def0-1234-56789abcdef0",
            ),
            ("RPGMIGRATE_RUN_ID", "123456789abcdef0123456789abcdef0"),
            ("RPGMIGRATE_WORKDIR_ID", "pvc-owned"),
            ("RPGMIGRATE_CHECKPOINT_GENERATION", "1"),
        ];
        let mut env = CONNECTIONS.to_vec();
        env.extend(identity);
        let error = invoke(&["copy", "table-data"], &env).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadConfig);
        assert!(error.message.contains("operator identity"));
        assert!(error.message.contains("unimplemented"));
        for (name, value) in identity {
            let mut partial = CONNECTIONS.to_vec();
            partial.push((name, value));
            let error = invoke(&["copy", "table-data"], &partial).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadConfig);
            assert!(!error.message.contains(value));
        }
        assert!(matches!(
            invoke(&["--help"], &env).unwrap(),
            Invocation::Help(_)
        ));
    }

    #[test]
    fn supplied_apply_group_setting_is_never_silently_ignored() {
        for value in ["100", "0", "invalid", "18446744073709551616"] {
            let mut env = CONNECTIONS.to_vec();
            env.push(("PGCOPYDB_APPLY_GROUP_TXNS", value));
            let error = invoke(&["copy", "table-data"], &env).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadConfig);
            assert!(error.message.contains("PGCOPYDB_APPLY_GROUP_TXNS"));
        }
    }

    #[test]
    fn plain_version_retains_text_identity_and_libpq_version() {
        for args in [vec!["version"], vec!["--version"]] {
            let output = crate::execute(invoke(&args, &[]).unwrap()).unwrap();
            assert_eq!(
                output,
                format!(
                    "rpgmigrate {}\nlibpq {}\n",
                    env!("CARGO_PKG_VERSION"),
                    crate::pg::libpq_version()
                )
            );
        }
    }

    #[test]
    fn version_rejects_malformed_json_syntax_and_operands() {
        for args in [
            vec!["version", "--json=true"],
            vec!["version", "--json", "unexpected"],
            vec!["version", "unexpected"],
        ] {
            assert_eq!(
                invoke(&args, &[]).unwrap_err().kind,
                ErrorKind::BadArgs,
                "{args:?}"
            );
        }
    }

    #[test]
    fn valid_connections_and_cli_precedence_produce_ping_without_io() {
        let invocation = invoke(&["ping", "-S", "dbname=override"], CONNECTIONS).unwrap();
        let Invocation::Ping { source, target } = invocation else {
            panic!("expected ping")
        };
        assert_eq!(source.as_c_str().to_bytes(), b"dbname=override");
        assert_eq!(target.as_c_str().to_bytes(), b"dbname=target");
    }

    #[test]
    fn copy_routes_serial_jobs_and_native_state_actions() {
        for (flag, expected) in [
            (None, StateAction::Create),
            (Some("--resume"), StateAction::Resume),
            (Some("--restart"), StateAction::Restart),
        ] {
            let mut args = vec!["copy", "table-data", "--dir", "/explicit/native", "-J", "1"];
            args.extend(flag);
            let invocation = invoke(&args, CONNECTIONS).unwrap();
            let Invocation::CopyTableData { state, .. } = invocation else {
                panic!("expected table-data")
            };
            assert_eq!(state.path, std::path::Path::new("/explicit/native"));
            assert_eq!(state.action, expected);
        }
    }

    #[test]
    fn boolean_environment_changes_copy_behavior_and_false_keeps_text_mode() {
        for (value, enabled) in [
            ("on", true),
            ("YES", true),
            ("t", true),
            ("1", true),
            ("off", false),
            ("n", false),
            ("FALSE", false),
            ("0", false),
        ] {
            let mut env = CONNECTIONS.to_vec();
            env.push(("PGCOPYDB_USE_COPY_BINARY", value));
            let result = invoke(&["copy", "table-data", "--dir", "/explicit/native"], &env);
            if enabled {
                let error = result.unwrap_err();
                assert_eq!(error.kind, ErrorKind::BadConfig);
                assert!(error.message.contains("use-copy-binary"));
            } else {
                assert!(matches!(result.unwrap(), Invocation::CopyTableData { .. }));
            }
        }
    }

    #[test]
    fn valid_environment_can_be_overridden_but_invalid_numeric_environment_cannot() {
        let mut env = CONNECTIONS.to_vec();
        env.push(("PGCOPYDB_TABLE_JOBS", "2"));
        assert!(matches!(
            invoke(&["copy", "table-data", "--table-jobs", "1"], &env).unwrap(),
            Invocation::CopyTableData { .. }
        ));
        env.pop();
        env.push(("PGCOPYDB_TABLE_JOBS", "invalid"));
        assert_eq!(
            invoke(&["copy", "table-data", "--table-jobs", "1"], &env)
                .unwrap_err()
                .kind,
            ErrorKind::BadConfig
        );
    }

    #[test]
    fn serial_table_jobs_dispatch_uses_the_validated_integer_syntax() {
        for value in ["1", "+1", " 1", "\t\r\n\u{000c} +1"] {
            let mut env = CONNECTIONS.to_vec();
            env.push(("PGCOPYDB_TABLE_JOBS", "2"));
            assert!(
                matches!(
                    invoke(&["copy", "table-data", "--table-jobs", value], &env).unwrap(),
                    Invocation::CopyTableData { .. }
                ),
                "CLI integer {value:?}"
            );
            env.pop();
            env.push(("PGCOPYDB_TABLE_JOBS", value));
            assert!(
                matches!(
                    invoke(&["copy", "table-data"], &env).unwrap(),
                    Invocation::CopyTableData { .. }
                ),
                "environment integer {value:?}"
            );
        }
    }

    #[test]
    fn list_uses_only_its_environment_subset() {
        let mut env = CONNECTIONS.to_vec();
        env.extend([
            ("PGCOPYDB_TABLE_JOBS", "invalid"),
            ("PGCOPYDB_SNAPSHOT", ""),
            ("PGCOPYDB_HOST", ""),
            ("PGCOPYDB_PORT", "invalid"),
        ]);
        assert!(matches!(
            invoke(&["list", "tables"], &env).unwrap(),
            Invocation::Unsupported(_)
        ));
        env.push(("PGCOPYDB_SPLIT_MAX_PARTS", "invalid"));
        assert_eq!(
            invoke(&["list", "tables"], &env).unwrap_err().kind,
            ErrorKind::BadConfig
        );
    }

    #[test]
    fn sentinel_does_not_use_coordinator_environment() {
        let env = [("PGCOPYDB_HOST", ""), ("PGCOPYDB_PORT", "invalid")];
        assert!(matches!(
            invoke(&["stream", "sentinel", "get"], &env).unwrap(),
            Invocation::Unsupported(_)
        ));
        assert_eq!(
            invoke(&["stream", "prune"], &env).unwrap_err().kind,
            ErrorKind::BadConfig
        );
    }

    #[test]
    fn default_state_root_is_native_and_explicit_path_stays_explicit() {
        let mut env = CONNECTIONS.to_vec();
        env.push(("TMPDIR", "/native-temporary"));
        let Invocation::CopyTableData { state, .. } =
            invoke(&["copy", "table-data"], &env).unwrap()
        else {
            panic!("expected table-data")
        };
        assert_eq!(
            state.path,
            std::path::Path::new("/native-temporary/rpgmigrate")
        );
        let Invocation::CopyTableData { state, .. } =
            invoke(&["copy", "table-data", "--dir", "/tmp/explicit"], &env).unwrap()
        else {
            panic!("expected table-data")
        };
        assert_eq!(state.path, std::path::Path::new("/tmp/explicit"));
    }

    #[test]
    fn unsupported_operands_and_options_never_produce_an_operational_invocation() {
        for args in [
            vec!["copy", "table-data", "public.only_this_table"],
            vec!["copy", "table-data", "--filter", "missing-file"],
            vec!["copy", "table-data", "--table-jobs", "2"],
            vec!["copy", "table-data", "--durability", "durable"],
            vec!["copy", "table-data", "--apply-jobs", "1"],
            vec!["ping", "--quiet"],
        ] {
            assert!(invoke(&args, CONNECTIONS).is_err(), "{args:?}");
        }
    }

    #[test]
    fn unknown_help_path_returns_an_internal_error() {
        assert_eq!(
            render_help(&CommandPath(vec!["unknown".into()]))
                .unwrap_err()
                .kind,
            ErrorKind::Internal
        );
    }

    #[test]
    fn help_command_preserves_its_identity_for_output_routing() {
        let Invocation::Help(path) = invoke(&["help"], &[]).unwrap() else {
            panic!("expected help");
        };
        assert_eq!(path.0, ["help"]);
        assert_eq!(
            render_help(&path).unwrap(),
            render_help(&CommandPath(Vec::new())).unwrap()
        );
    }

    #[test]
    fn help_does_not_mask_unknown_options_or_bad_values() {
        for args in [
            vec!["ping", "--help", "--unknown"],
            vec!["clone", "--table-jobs", "invalid", "--help"],
        ] {
            assert_eq!(invoke(&args, &[]).unwrap_err().kind, ErrorKind::BadArgs);
        }
    }
}
