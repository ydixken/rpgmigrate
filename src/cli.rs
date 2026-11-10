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
            | Parser::Sentinel
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
            flag("json", Some('J')),
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
    let command = base(name, parser).arg(
        Arg::new("operands")
            .num_args(0..)
            .trailing_var_arg(
                parser != Parser::Version && !(parser == Parser::Sentinel && name == "endpos"),
            )
            .value_parser(OsStringValueParser::new()),
    );
    if matches!(name, "clone" | "follow") {
        let limits = crate::contracts::FollowLimits::default();
        let replay_safe = match crate::contracts::REPLAY_SAFE_BARRIER_INTERVAL_MS {
            Some(interval) => format!("replay-safe uses writing barriers every {interval} ms"),
            None => "replay-safe is unavailable until measured cadence selection".into(),
        };
        command.after_help(format!(
            "Native follow requires pgoutput v1. Durability defaults to durable; {replay_safe}; relaxed is unavailable. Limits: {} queued messages, {} queued bytes plus one message up to {} bytes when empty; apply groups default to {} transactions, {} logical events or {} canonical bytes, closing only at a source COMMIT. Cutover requires a persisted endpos and a caller write freeze through final sequence and selected large-object synchronization.",
            limits.channel_messages, limits.channel_bytes, limits.max_message_bytes,
            crate::contracts::DEFAULT_FOLLOW_APPLY_GROUP_TXNS, limits.group_rows, limits.group_bytes,
        ))
    } else {
        command
    }
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
        .after_help("Native profiles: single-database clone, read-only progress and schema/data comparison, ping, restricted copy table-data, bounded follow, stream sentinel get, stream sentinel set endpos, and stream cleanup of owned resources. Follow requires pgoutput v1 and defaults to durable; see follow --help for replay-safe availability. Relaxed is unavailable. PostgreSQL pairs: 16 to 16, 18 to 18, and 16 to 18. Unsupported profile options fail before database or state changes.")
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
        return Err(Error::new(
            ErrorKind::BadArgs,
            "argv must include the executable",
        ));
    }
    let matches = command().try_get_matches_from(args).map_err(|_| {
        // Clap diagnostics can include the original arguments and their secrets.
        Error::new(
            ErrorKind::BadArgs,
            "invalid command or option syntax; use --help for accepted arguments",
        )
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
        return Err(Error::new(
            ErrorKind::BadArgs,
            "a command is required; use --help for available commands",
        ));
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
        selected = selected.find_subcommand_mut(part).ok_or_else(|| {
            Error::new(ErrorKind::Internal, "help command path is not registered")
        })?;
    }
    Ok(format!("{}\n", selected.render_help()))
}

#[cfg(test)]
pub(crate) fn contract_oracle(args: &[OsString], env: &Environment) -> Result<serde_json::Value> {
    let matches = command()
        .try_get_matches_from(args)
        .map_err(|_| Error::new(ErrorKind::BadArgs, "invalid command or option syntax"))?;
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
    fn m8_clone_and_follow_admit_selected_copy_features_before_io() {
        for command in ["clone", "follow"] {
            for selectors in [
                vec!["--use-copy-binary"],
                vec!["--split-tables-larger-than", "0", "--split-max-parts", "8"],
                vec![
                    "--split-tables-larger-than",
                    "536870912",
                    "--split-max-parts",
                    "1",
                ],
                vec!["--large-objects-jobs", "1"],
                vec!["--large-objects-jobs", "128"],
                vec!["--split-max-parts", "2147483647"],
                vec![],
            ] {
                let mut args = vec![command];
                args.extend(selectors);
                assert!(invoke(&args, CONNECTIONS).is_ok(), "{args:?}");
            }
        }
    }

    #[test]
    fn m8_copy_defaults_and_environment_precedence_preserve_selected_policy() {
        let Invocation::Clone(defaults) = invoke(&["clone"], CONNECTIONS).unwrap() else {
            panic!("clone")
        };
        assert!(!defaults.use_copy_binary);
        assert_eq!(defaults.split_tables_larger_than, 0);
        assert_eq!(defaults.split_max_parts, 8);
        assert!(!defaults.skip_large_objects);
        assert_eq!(defaults.large_objects_jobs, 4);
        let mut env = CONNECTIONS.to_vec();
        env.extend([
            ("PGCOPYDB_USE_COPY_BINARY", "false"),
            ("PGCOPYDB_SPLIT_TABLES_LARGER_THAN", "1GB"),
            ("PGCOPYDB_SPLIT_MAX_PARTS", "1"),
            ("PGCOPYDB_LARGE_OBJECTS_JOBS", "128"),
        ]);
        let Invocation::Clone(selected) = invoke(
            &[
                "clone",
                "--use-copy-binary",
                "--split-tables-larger-than",
                "536870912",
                "--split-max-parts",
                "8",
                "--large-objects-jobs",
                "1",
                "--skip-large-objects",
            ],
            &env,
        )
        .unwrap() else {
            panic!("clone")
        };
        assert!(selected.use_copy_binary);
        assert_eq!(selected.split_tables_larger_than, 536870912);
        assert_eq!(selected.split_max_parts, 8);
        assert!(selected.skip_large_objects);
        assert_eq!(selected.large_objects_jobs, 1);
        let Invocation::Clone(environment) = invoke(&["clone"], &env).unwrap() else {
            panic!("clone")
        };
        assert!(!environment.use_copy_binary);
        assert_eq!(environment.split_tables_larger_than, 1_073_741_824);
        assert_eq!(environment.split_max_parts, 1);
        assert_eq!(environment.large_objects_jobs, 128);
        assert_eq!(selected.copy_policy().split_tables_larger_than, 536870912);
    }

    #[test]
    fn m8_copy_selector_bounds_and_read_only_admission_fail_closed() {
        for selectors in [
            vec!["--large-objects-jobs", "0"],
            vec!["--large-objects-jobs", "129"],
            vec!["--split-max-parts", "0"],
            vec!["--split-max-parts", "2147483648"],
        ] {
            let mut args = vec!["clone"];
            args.extend(selectors);
            assert!(invoke(&args, CONNECTIONS).is_err(), "{args:?}");
        }
        for route in [
            vec!["list", "progress"],
            vec!["compare", "data"],
            vec!["compare", "schema"],
        ] {
            for selectors in [
                vec!["--use-copy-binary"],
                vec!["--split-max-parts", "8"],
                vec!["--large-objects-jobs", "4"],
                vec!["--skip-large-objects"],
            ] {
                let mut args = route.clone();
                args.extend(selectors);
                assert!(invoke(&args, CONNECTIONS).is_err(), "{args:?}");
            }
        }
    }

    #[test]
    fn m4_routes_admit_only_cutover_and_cleanup_commands() {
        for args in [
            vec!["stream", "sentinel", "get", "--endpos"],
            vec![
                "stream", "sentinel", "get", "--target", "dbname=t", "--json",
            ],
            vec![
                "stream",
                "sentinel",
                "get",
                "--target",
                "dbname=t",
                "--replay-lsn",
                "--json",
            ],
            vec![
                "stream", "sentinel", "set", "endpos", "--target", "dbname=t", "0/20",
            ],
            vec![
                "stream",
                "sentinel",
                "set",
                "endpos",
                "--source",
                "dbname=s",
                "--target",
                "dbname=t",
                "--current",
            ],
            vec!["stream", "cleanup", "--source", "dbname=s"],
        ] {
            assert!(
                !matches!(invoke(&args, &[]).unwrap(), Invocation::Unsupported(_)),
                "{args:?}"
            );
        }
    }

    #[test]
    fn m4_measured_commands_require_target_but_local_endpos_does_not() {
        for args in [
            vec!["stream", "sentinel", "get"],
            vec!["stream", "sentinel", "get", "--json", "--endpos"],
            vec!["stream", "sentinel", "get", "--replay-lsn"],
            vec!["stream", "sentinel", "set", "endpos", "0/20"],
        ] {
            assert_eq!(
                invoke(&args, &[]).unwrap_err().kind,
                ErrorKind::BadArgs,
                "{args:?}"
            );
        }
        assert!(!matches!(
            invoke(&["stream", "sentinel", "get", "--endpos"], &[]).unwrap(),
            Invocation::Unsupported(_)
        ));
    }

    #[test]
    fn m4_explicit_endpos_and_selectors_keep_exact_resume_options() {
        use crate::contracts::{EndposSource, SentinelField, StreamCommand};
        for (args, expected) in [
            (
                vec!["stream", "sentinel", "get", "--endpos"],
                StreamCommand::Get(SentinelField::Endpos),
            ),
            (
                vec![
                    "stream",
                    "sentinel",
                    "get",
                    "--target",
                    "dbname=t",
                    "--json",
                    "--replay-lsn",
                ],
                StreamCommand::Get(SentinelField::ReplayLsn),
            ),
            (
                vec![
                    "stream", "sentinel", "set", "endpos", "--target", "dbname=t", "--json", "a/b",
                ],
                StreamCommand::SetEndpos(EndposSource::Explicit(crate::identity::SourceEndLsn(
                    0xa_0000000b,
                ))),
            ),
            (
                vec![
                    "stream", "sentinel", "set", "endpos", "a/b", "--json", "--target", "dbname=t",
                ],
                StreamCommand::SetEndpos(EndposSource::Explicit(crate::identity::SourceEndLsn(
                    0xa_0000000b,
                ))),
            ),
            (
                vec![
                    "stream",
                    "sentinel",
                    "set",
                    "endpos",
                    "--dir",
                    "/rpgmigrate-parser-absent",
                    "--target",
                    "dbname=t",
                    "a/b",
                    "--json",
                ],
                StreamCommand::SetEndpos(EndposSource::Explicit(crate::identity::SourceEndLsn(
                    0xa_0000000b,
                ))),
            ),
            (
                vec![
                    "stream",
                    "sentinel",
                    "set",
                    "endpos",
                    "--source",
                    "dbname=s",
                    "--target",
                    "dbname=t",
                    "--current",
                ],
                StreamCommand::SetEndpos(EndposSource::Current),
            ),
        ] {
            let Invocation::Stream(options) = invoke(&args, &[]).unwrap() else {
                panic!("expected stream")
            };
            assert_eq!(options.command, expected);
            assert_eq!(options.state.action, StateAction::Resume);
            assert_eq!(options.json, args.contains(&"--json"));
            if args.contains(&"--target") {
                assert_eq!(options.target.unwrap().as_c_str().to_bytes(), b"dbname=t");
            }
            if args.contains(&"--dir") {
                assert_eq!(
                    options.state.path,
                    std::path::Path::new("/rpgmigrate-parser-absent")
                );
            }
        }
    }

    #[test]
    fn m4_refuses_zero_bad_endpoints_and_other_sentinel_mutations() {
        for args in [
            vec![
                "stream", "sentinel", "set", "endpos", "--target", "dbname=t", "0/0",
            ],
            vec![
                "stream",
                "sentinel",
                "set",
                "endpos",
                "--target",
                "dbname=t",
                "100000000/1",
                "--json",
            ],
            vec![
                "stream", "sentinel", "set", "endpos", "--target", "dbname=t", "0/20", "0/30",
                "--json",
            ],
            vec![
                "stream",
                "sentinel",
                "set",
                "endpos",
                "--source",
                "dbname=s",
                "--target",
                "dbname=t",
                "--current",
                "0/20",
                "--json",
            ],
            vec!["stream", "sentinel", "set", "startpos", "0/1", "--json"],
            vec![
                "stream",
                "sentinel",
                "get",
                "--target",
                "not conninfo",
                "--json",
            ],
            vec![
                "stream",
                "sentinel",
                "get",
                "--target",
                "dbname=t",
                "--current",
            ],
            vec![
                "stream", "sentinel", "set", "endpos", "--target", "dbname=t", "--endpos", "0/20",
            ],
        ] {
            assert_eq!(
                invoke(&args, &[]).unwrap_err().kind,
                ErrorKind::BadArgs,
                "{args:?}"
            );
        }
        for args in [
            vec!["stream", "sentinel", "get", "--startpos"],
            vec!["stream", "sentinel", "get", "--flush-lsn"],
        ] {
            assert_eq!(invoke(&args, &[]).unwrap_err().kind, ErrorKind::BadConfig);
        }
        for args in [
            vec!["stream", "sentinel", "set", "startpos", "0/20"],
            vec!["stream", "sentinel", "set", "apply"],
            vec!["stream", "sentinel", "setup", "0/10", "0/20"],
        ] {
            assert!(matches!(
                invoke(&args, &[]).unwrap(),
                Invocation::Unsupported(_)
            ));
        }
    }

    #[test]
    fn m9_omitted_requests_remain_none() {
        for args in [
            vec!["follow", "--skip-large-objects"],
            vec!["clone", "--follow", "--skip-large-objects"],
        ] {
            let Invocation::Follow(options) = invoke(&args, CONNECTIONS).unwrap() else {
                panic!("expected follow")
            };
            assert_eq!(options.durability, None);
            assert_eq!(options.apply_group_txns, None);
            assert_eq!(options.limits, crate::contracts::FollowLimits::default());
            assert_eq!(options.clone.table_jobs, 4);
            assert_eq!(options.clone.post_data_jobs, 4);
            assert!(options.origin_name.is_none());
        }
    }

    #[test]
    fn m9_durable_and_replay_safe_request_modes() {
        for (mode, expected) in [
            ("durable", crate::contracts::DurabilityMode::Durable),
            ("replay-safe", crate::contracts::DurabilityMode::ReplaySafe),
        ] {
            for route in [vec!["follow"], vec!["clone", "--follow"]] {
                let mut args = route;
                args.extend(["--durability", mode]);
                let Invocation::Follow(options) = invoke(&args, CONNECTIONS).unwrap() else {
                    panic!("expected follow")
                };
                assert_eq!(options.durability, Some(expected));
                assert_eq!(options.apply_group_txns, None);
            }
            assert!(invoke(&["clone", "--durability", mode], CONNECTIONS).is_err());
        }
    }

    #[test]
    fn m9_help_reports_defaults_and_mode_availability() {
        for route in ["clone", "follow"] {
            let help = render_help(&CommandPath(vec![route.into()])).unwrap();
            for expected in [
                "Durability defaults to durable",
                "relaxed is unavailable",
                "1024 queued messages",
                "8388608 queued bytes",
                "16777216 bytes when empty",
                "100 transactions",
                "10000 logical events",
                "8388608 canonical bytes",
            ] {
                assert!(help.contains(expected), "{route}: {expected}");
            }
            match crate::contracts::REPLAY_SAFE_BARRIER_INTERVAL_MS {
                Some(interval) => assert!(help.contains(&format!("every {interval} ms"))),
                None => assert!(
                    help.contains("replay-safe is unavailable until measured cadence selection")
                ),
            }
        }
    }

    #[test]
    fn m9_relaxed_refuses_before_effects() {
        for caps in [
            vec![],
            vec!["--loss-max-transactions", "1"],
            vec!["--loss-max-bytes", "1"],
            vec!["--loss-max-transactions", "1", "--loss-max-bytes", "1"],
        ] {
            let mut args = vec!["follow", "--durability", "relaxed"];
            args.extend(caps);
            let error = invoke(&args, CONNECTIONS).unwrap_err();
            assert!(matches!(
                error.kind,
                ErrorKind::BadArgs | ErrorKind::BadConfig
            ));
        }
    }

    #[test]
    fn follow_records_explicit_unowned_origin_and_native_resource_names() {
        let Invocation::Follow(options) = invoke(
            &[
                "follow",
                "--skip-large-objects",
                "--plugin",
                "pgoutput",
                "--durability",
                "durable",
                "--slot-name",
                "native_slot",
                "--publication",
                "native publication",
                "--origin",
                "unowned-origin",
            ],
            CONNECTIONS,
        )
        .unwrap() else {
            panic!("expected follow")
        };
        assert_eq!(options.slot_name, "native_slot");
        assert_eq!(options.publication_name, "native publication");
        assert_eq!(options.origin_name.as_deref(), Some("unowned-origin"));
    }

    #[test]
    fn m9_group_override_preserves_explicitness() {
        for value in ["1", "100", "18446744073709551615"] {
            let mut env = CONNECTIONS.to_vec();
            env.push(("PGCOPYDB_APPLY_GROUP_TXNS", value));
            let Invocation::Follow(options) =
                invoke(&["follow", "--skip-large-objects"], &env).unwrap()
            else {
                panic!("expected follow")
            };
            assert_eq!(
                options.apply_group_txns,
                Some(value.parse::<u64>().unwrap())
            );
            assert_eq!(options.durability, None);
            assert!(invoke(&["clone"], &env).is_err());
        }
        for value in [
            "",
            "0",
            "-1",
            "18446744073709551616",
            "not-an-integer",
            "0x64",
            "1.0",
            "1e2",
            "true",
        ] {
            let mut env = CONNECTIONS.to_vec();
            env.push(("PGCOPYDB_APPLY_GROUP_TXNS", value));
            let error = invoke(&["follow", "--skip-large-objects"], &env).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadConfig);
            assert!(error.message.contains("PGCOPYDB_APPLY_GROUP_TXNS"));
        }
    }

    #[test]
    fn follow_preserves_operator_resume_not_consistent_retry() {
        let Invocation::Follow(options) = invoke(
            &[
                "clone",
                "--follow",
                "--skip-large-objects",
                "--resume",
                "--not-consistent",
            ],
            CONNECTIONS,
        )
        .unwrap() else {
            panic!("expected follow")
        };
        assert_eq!(options.clone.state.action, StateAction::Resume);
        assert!(options.clone.not_consistent);
        assert!(
            invoke(
                &["follow", "--skip-large-objects", "--not-consistent"],
                CONNECTIONS,
            )
            .is_err()
        );
    }

    #[test]
    fn follow_refuses_unsupported_effective_settings_before_io() {
        for args in [
            vec!["--plugin", "test_decoding"],
            vec!["--plugin", "wal2json"],
            vec!["--endpos", "0/10"],
        ] {
            let mut argv = vec!["follow", "--skip-large-objects"];
            argv.extend(args);
            let error = invoke(&argv, CONNECTIONS).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadConfig);
            assert!(
                error
                    .message
                    .contains("no database or state action was started")
            );
        }
    }

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
            "--roles",
            "--all-databases",
            "--drop-if-exists",
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
    fn json_version_is_identity_free_and_advertises_only_qualified_capabilities() {
        let env = [
            ("PGCOPYDB_SOURCE_PGURI", "invalid connection"),
            ("PGCOPYDB_TARGET_PGURI", "invalid connection"),
            ("RPGMIGRATE_MIGRATION_UID", "invalid"),
            ("RPGMIGRATE_RUN_ID", "invalid"),
            ("RPGMIGRATE_WORKDIR_ID", "invalid"),
            ("RPGMIGRATE_CHECKPOINT_GENERATION", "invalid"),
            ("PGCOPYDB_APPLY_GROUP_TXNS", "invalid"),
            ("PGCOPYDB_LOG_JSON", "on"),
            ("RPGMIGRATE_INTERNAL_COMPARE_SOURCE_FILES", "invalid"),
            ("RPGMIGRATE_INTERNAL_COMPARE_TARGET_FILES", "invalid"),
        ];
        let invocation = invoke(&["version", "--json"], &env).unwrap();
        let output = crate::execute(invocation).unwrap();
        assert_eq!(output.lines().count(), 1);
        assert!(output.ends_with('\n'));
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();
        let profile = if cfg!(target_os = "linux") {
            "version-qualified-linux-profile.json"
        } else {
            "version-qualified-profile.json"
        };
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/native/v1/payloads")
            .join(profile);
        let expected: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(value, expected);
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
            invoke(&["stream", "sentinel", "get", "--endpos"], &env).unwrap(),
            Invocation::Stream(_)
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
