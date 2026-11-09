use serde::Serialize;

use crate::cli::Environment;
use crate::contracts::{Error, ErrorKind, FollowLimits, FollowOptions, OperatorIdentity, Result};
use crate::identity::RunId;

#[derive(Serialize)]
pub(crate) struct Version {
    protocol_version: u32,
    engine_version: &'static str,
    capabilities: &'static [&'static str],
}

impl Version {
    pub(crate) fn current() -> Self {
        Self {
            protocol_version: 1,
            engine_version: env!("CARGO_PKG_VERSION"),
            capabilities: &[
                "clone.single_db",
                "cdc.pgoutput",
                "progress.v1",
                "cutover.v1",
                "verify.v1",
                "cleanup.v1",
                #[cfg(target_os = "linux")]
                "verify.sealed_files.v1",
            ],
        }
    }
}

pub(crate) fn operator_run(env: &Environment) -> Result<Option<RunId>> {
    let names = [
        "RPGMIGRATE_MIGRATION_UID",
        "RPGMIGRATE_RUN_ID",
        "RPGMIGRATE_WORKDIR_ID",
        "RPGMIGRATE_CHECKPOINT_GENERATION",
    ];
    let values = names.map(|name| env.0.get(std::ffi::OsStr::new(name)));
    if values.iter().all(Option::is_none) {
        return Ok(None);
    }
    let invalid = || {
        Error::new(
            ErrorKind::BadConfig,
            "invalid operator identity environment; supply all four canonical identity values",
        )
    };
    let [uid, run, workdir, generation] =
        values.map(|value| value.and_then(|value| value.to_str()));
    let (Some(uid), Some(run), Some(workdir), Some("1")) = (uid, run, workdir, generation) else {
        return Err(invalid());
    };
    if uid.len() != 36
        || !uid.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
        || run.len() != 32
        || uid.replace('-', "") != run
        || workdir.is_empty()
        || workdir.len() > 256
        || workdir.trim() != workdir
        || workdir
            .chars()
            .any(|character| character.is_control() || matches!(character, '/' | '\\'))
    {
        return Err(invalid());
    }
    let mut bytes = [0; 16];
    for (byte, pair) in bytes.iter_mut().zip(run.as_bytes().as_chunks::<2>().0) {
        let hex = |digit: u8| {
            if digit.is_ascii_digit() {
                digit - b'0'
            } else {
                digit - b'a' + 10
            }
        };
        *byte = hex(pair[0]) * 16 + hex(pair[1]);
    }
    Ok(Some(RunId(bytes)))
}

pub(crate) fn operator_identity(env: &Environment) -> Result<Option<OperatorIdentity>> {
    let Some(run) = operator_run(env)? else {
        return Ok(None);
    };
    let value = |name: &str| {
        env.0
            .get(std::ffi::OsStr::new(name))
            .and_then(|value| value.to_str())
            .expect("operator_run validated all identity fields")
            .to_owned()
    };
    Ok(Some(OperatorIdentity {
        migration_uid: value("RPGMIGRATE_MIGRATION_UID"),
        run,
        workdir_id: value("RPGMIGRATE_WORKDIR_ID"),
        checkpoint_generation: 1,
    }))
}

pub(crate) fn work(operator: Option<&OperatorIdentity>) -> Option<serde_json::Value> {
    operator.map(|operator| {
        serde_json::json!({
            "migration_uid": operator.migration_uid,
            "run_id": operator.run.0.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "workdir_id": operator.workdir_id,
            "checkpoint_generation": operator.checkpoint_generation,
        })
    })
}

#[derive(Clone, Copy)]
pub(crate) enum CopyCommand {
    Clone,
    Progress,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Planning,
    Copying,
    Finalizing,
    Streaming,
    Draining,
    Completed,
    Failed,
    Suspended,
}

impl Phase {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "planning" => Ok(Self::Planning),
            "copying" => Ok(Self::Copying),
            "finalizing" => Ok(Self::Finalizing),
            "streaming" => Ok(Self::Streaming),
            "draining" => Ok(Self::Draining),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "suspended" => Ok(Self::Suspended),
            _ => Err(Error::new(
                ErrorKind::BadState,
                "native report phase is unsupported",
            )),
        }
    }
}

#[derive(Serialize)]
struct ReportDocument<'a, P> {
    protocol_version: u32,
    engine_version: &'static str,
    command: &'static str,
    capabilities: [&'static str; 1],
    work: serde_json::Value,
    phase: Phase,
    #[serde(flatten)]
    payload: &'a P,
}

#[derive(Serialize)]
struct StandaloneDocument<'a, P> {
    command: &'static str,
    phase: Phase,
    #[serde(flatten)]
    payload: &'a P,
}

fn report(
    command: &'static str,
    capability: &'static str,
    operator: Option<&OperatorIdentity>,
    phase: Phase,
    payload: &impl Serialize,
) -> Result<String> {
    let encoded = match work(operator) {
        Some(work) => serde_json::to_string(&ReportDocument {
            protocol_version: 1,
            engine_version: env!("CARGO_PKG_VERSION"),
            command,
            capabilities: [capability],
            work,
            phase,
            payload,
        })
        .map(|document| format!("native-report: {document}\n")),
        None => serde_json::to_string(&StandaloneDocument {
            command,
            phase,
            payload,
        })
        .map(|document| format!("{document}\n")),
    };
    encoded.map_err(|_| Error::new(ErrorKind::Internal, "cannot encode native report"))
}

pub(crate) fn copy_report(
    command: CopyCommand,
    operator: Option<&OperatorIdentity>,
    phase: &str,
    copy: &impl Serialize,
) -> Result<String> {
    #[derive(Serialize)]
    struct Payload<'a, C> {
        copy: &'a C,
    }
    let (command, capability) = match command {
        CopyCommand::Clone => ("clone", "clone.single_db"),
        CopyCommand::Progress => ("list progress", "progress.v1"),
    };
    report(
        command,
        capability,
        operator,
        Phase::parse(phase)?,
        &Payload { copy },
    )
}

#[derive(Clone, Copy)]
pub(crate) enum StreamReportCommand {
    Get,
    SetEndpos,
}

#[derive(Serialize)]
struct Replication {
    certified_lsn: String,
    durable_replay_lsn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_lsn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    end_persisted: Option<bool>,
    drained: bool,
    checkpoint_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    drain_proof: Option<DrainProof>,
}

#[derive(Serialize)]
struct DrainProof {
    #[serde(skip_serializing_if = "Option::is_none")]
    work: Option<serde_json::Value>,
    checkpoint_id: String,
    committed: bool,
    end_lsn: String,
    replay_lsn: String,
    kind: &'static str,
}

pub(crate) fn lsn(value: crate::identity::SourceEndLsn) -> String {
    format!("{:X}/{:X}", value.0 >> 32, value.0 as u32)
}

pub(crate) fn replication_report(
    command: StreamReportCommand,
    operator: Option<&OperatorIdentity>,
    request: Option<&crate::state::EndposRequest>,
    snapshot: &crate::apply::checkpoint::DurableFollowSnapshot,
    completed: Option<&crate::apply::checkpoint::CompletedFollow>,
) -> Result<String> {
    use crate::apply::checkpoint::DrainKind;
    if operator.is_some_and(|operator| operator.run != snapshot.stream().run)
        || request.is_some_and(|request| request.stream() != snapshot.stream())
        || snapshot.follow_completed() != completed.is_some()
        || snapshot.certified_lsn() > snapshot.durable_replay_lsn()
        || matches!(command, StreamReportCommand::SetEndpos) && request.is_none()
    {
        return Err(report_refusal("replication report binding is inconsistent"));
    }
    let drain_proof = completed
        .map(|completed| {
            if request
                .is_none_or(|request| !request.is_sealed() || request.end() != completed.end())
                || completed.target() != snapshot.target()
                || completed.checkpoint_id() != snapshot.checkpoint_id()
                || completed.certified_lsn() != snapshot.certified_lsn()
                || completed.durable_replay_lsn() != snapshot.durable_replay_lsn()
            {
                return Err(report_refusal(
                    "completed report differs from its observed authority",
                ));
            }
            Ok(DrainProof {
                work: work(operator),
                checkpoint_id: completed.checkpoint_id(),
                committed: true,
                end_lsn: lsn(completed.end()),
                replay_lsn: lsn(completed.witness()),
                kind: match completed.kind() {
                    DrainKind::Keepalive => "keepalive",
                    DrainKind::LaterBegin => "later_begin",
                },
            })
        })
        .transpose()?;
    #[derive(Serialize)]
    struct Payload {
        replication: Replication,
    }
    report(
        match command {
            StreamReportCommand::Get => "stream sentinel get",
            StreamReportCommand::SetEndpos => "stream sentinel set endpos",
        },
        "cutover.v1",
        operator,
        if completed.is_some() {
            Phase::Completed
        } else if request.is_some() {
            Phase::Draining
        } else {
            Phase::Streaming
        },
        &Payload {
            replication: Replication {
                certified_lsn: lsn(snapshot.certified_lsn()),
                durable_replay_lsn: lsn(snapshot.durable_replay_lsn()),
                end_lsn: request.map(|request| lsn(request.end())),
                end_persisted: request.map(|_| true),
                drained: completed.is_some(),
                checkpoint_id: snapshot.checkpoint_id(),
                drain_proof,
            },
        },
    )
}

pub(crate) fn completed_follow_report(
    operator: Option<&OperatorIdentity>,
    copy: &impl Serialize,
    completed: &crate::apply::checkpoint::CompletedFollow,
) -> Result<String> {
    if operator.is_some_and(|operator| operator.run != completed.target().intent.binding.stream.run)
    {
        return Err(report_refusal(
            "completed clone report has a different operator binding",
        ));
    }
    copy_report(CopyCommand::Clone, operator, "completed", copy)
}

pub(crate) fn cleanup_report(
    control: &crate::state::FollowControl,
    canonical_workdir: &std::path::Path,
    progress: &crate::state::CleanupProgress,
) -> Result<String> {
    control.validate()?;
    let name = canonical_workdir
        .to_str()
        .filter(|name| !name.is_empty())
        .filter(|_| canonical_workdir.is_absolute())
        .ok_or_else(|| report_refusal("cleanup workdir is not a canonical absolute path"))?;
    #[derive(Serialize)]
    struct Resource<'a> {
        kind: &'static str,
        name: &'a str,
        owned: bool,
        removed: bool,
    }
    #[derive(Serialize)]
    struct Cleanup<'a> {
        complete: bool,
        resources: Vec<Resource<'a>>,
    }
    #[derive(Serialize)]
    struct Payload<'a> {
        cleanup: Cleanup<'a>,
    }
    let mut resources = vec![
        Resource {
            kind: "replication_slot",
            name: control.binding.slot.intent.binding.slot_name(),
            owned: true,
            removed: progress.slot_removed,
        },
        Resource {
            kind: "publication",
            name: &control.binding.publication.name,
            owned: true,
            removed: progress.publication_removed,
        },
        Resource {
            kind: "workdir",
            name,
            owned: true,
            removed: false,
        },
    ];
    if let Some(name) = control.origin_name.as_deref() {
        resources.push(Resource {
            kind: "replication_origin",
            name,
            owned: false,
            removed: false,
        });
    }
    let complete = progress.slot_removed && progress.publication_removed;
    report(
        "stream cleanup",
        "cleanup.v1",
        control.operator.as_ref(),
        if complete {
            Phase::Completed
        } else {
            Phase::Suspended
        },
        &Payload {
            cleanup: Cleanup {
                complete,
                resources,
            },
        },
    )
}

#[track_caller]
fn report_refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

enum FollowLog {
    Startup {
        limits: FollowLimits,
        apply_group_txns: u64,
    },
    Backpressure {
        limits: FollowLimits,
        queued_messages: usize,
        queued_bytes: usize,
    },
}

impl FollowLog {
    fn startup(options: &FollowOptions) -> Self {
        Self::Startup {
            limits: options.limits,
            apply_group_txns: options.apply_group_txns,
        }
    }

    fn document(&self) -> serde_json::Value {
        match self {
            Self::Startup {
                limits,
                apply_group_txns,
            } => serde_json::json!({
                "level":"info", "error_severity":"INFO", "event":"follow.startup",
                "message":"native follow startup", "limits":limits,
                "apply_group_txns":apply_group_txns, "wal_storage_free_bytes":"unknown",
            }),
            Self::Backpressure {
                limits,
                queued_messages,
                queued_bytes,
            } => serde_json::json!({
                "level":"info", "error_severity":"INFO", "event":"follow.backpressure",
                "message":"native follow backpressure", "queue":{
                    "messages":queued_messages, "owned_bytes":queued_bytes,
                    "limit_messages":limits.channel_messages, "limit_bytes":limits.channel_bytes,
                },
            }),
        }
    }

    fn write(self) -> Result<()> {
        use std::io::Write;
        writeln!(std::io::stderr().lock(), "{}", self.document())
            .map_err(|_| Error::new(ErrorKind::Internal, "cannot write native follow log"))
    }
}

pub(crate) fn follow_startup(options: &FollowOptions) -> Result<()> {
    FollowLog::startup(options).write()
}

pub(crate) fn follow_backpressure(
    limits: FollowLimits,
    queued_messages: usize,
    queued_bytes: usize,
) -> Result<()> {
    FollowLog::Backpressure {
        limits,
        queued_messages,
        queued_bytes,
    }
    .write()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn replication_serialization_retains_measured_zero_and_omits_unknowns() {
        let value = serde_json::to_value(Replication {
            certified_lsn: lsn(crate::identity::SourceEndLsn(0)),
            durable_replay_lsn: lsn(crate::identity::SourceEndLsn(0x1_ffffffff)),
            end_lsn: None,
            end_persisted: None,
            drained: false,
            checkpoint_id: "1:2:3".into(),
            drain_proof: None,
        })
        .unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "certified_lsn":"0/0", "durable_replay_lsn":"1/FFFFFFFF",
                "drained":false, "checkpoint_id":"1:2:3",
            })
        );
    }

    #[test]
    fn standalone_replication_framing_cannot_supply_operator_evidence() {
        #[derive(Serialize)]
        struct Payload {
            replication: Replication,
        }
        let payload = Payload {
            replication: Replication {
                certified_lsn: "0/10".into(),
                durable_replay_lsn: "0/10".into(),
                end_lsn: Some("0/20".into()),
                end_persisted: Some(true),
                drained: false,
                checkpoint_id: "1:2:3".into(),
                drain_proof: None,
            },
        };
        let output = report(
            "stream sentinel get",
            "cutover.v1",
            None,
            Phase::Draining,
            &payload,
        )
        .unwrap();
        assert!(!output.starts_with("native-report: "));
        let standalone: serde_json::Value = serde_json::from_str(&output).unwrap();
        for required in ["protocol_version", "engine_version", "work", "capabilities"] {
            assert!(standalone.get(required).is_none());
        }
        let operator = operator_identity(&environment()).unwrap().unwrap();
        let output = report(
            "stream sentinel get",
            "cutover.v1",
            Some(&operator),
            Phase::Draining,
            &payload,
        )
        .unwrap();
        let bound: serde_json::Value =
            serde_json::from_str(output.strip_prefix("native-report: ").unwrap()).unwrap();
        assert_eq!(bound["capabilities"], serde_json::json!(["cutover.v1"]));
        assert_eq!(bound["work"], work(Some(&operator)).unwrap());
        assert_eq!(bound["replication"], standalone["replication"]);
        let discovery = serde_json::to_value(Version::current()).unwrap();
        assert_eq!(
            discovery["capabilities"]
                .as_array()
                .unwrap()
                .iter()
                .any(|capability| capability == "verify.sealed_files.v1"),
            cfg!(target_os = "linux")
        );
    }

    #[test]
    fn shared_fixtures_bind_discovery_and_configuration_to_real_parser() {
        let root =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/native/v1");
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("cases.json")).unwrap()).unwrap();
        let mut argv_count = 0;
        let mut producer_count = 0;
        let producer_id = if cfg!(target_os = "linux") {
            "version-qualified-linux-profile"
        } else {
            "version-qualified-profile"
        };
        for case in catalog["cases"].as_array().unwrap() {
            let id = case["id"].as_str().unwrap();
            if case["kind"] == "version" && case["accept"] == true {
                let fixture: serde_json::Value = serde_json::from_slice(
                    &std::fs::read(root.join(case["payload"].as_str().unwrap())).unwrap(),
                )
                .unwrap();
                if id == producer_id {
                    assert_eq!(
                        serde_json::to_value(Version::current()).unwrap(),
                        fixture,
                        "{id}"
                    );
                    producer_count += 1;
                }
                continue;
            }
            if case["kind"] != "argv" {
                continue;
            }
            argv_count += 1;
            let args = std::iter::once(OsString::from("rpgmigrate"))
                .chain(case["argv"].as_array().unwrap().iter().map(|arg| {
                    let arg = arg.as_str().unwrap();
                    if arg == "payloads/filters.ini" {
                        root.join(arg).into_os_string()
                    } else {
                        arg.into()
                    }
                }))
                .collect::<Vec<_>>();
            let env = crate::cli::Environment(
                case["env"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(key, value)| (key.into(), value.as_str().unwrap().into()))
                    .collect(),
            );
            let result = crate::cli::contract_oracle(&args, &env);
            assert_eq!(
                result.is_ok(),
                case["accept"].as_bool().unwrap(),
                "{id}: {result:?}"
            );
            if let Err(error) = result {
                assert_eq!(
                    format!("{:?}", error.kind),
                    case["expect"]["error_kind"].as_str().unwrap(),
                    "{id}"
                );
                continue;
            }
            let values = result.unwrap();
            for key in [
                "effective_source",
                "effective_target",
                "table_jobs",
                "log_json",
            ] {
                if let Some(expected) = case["expect"].get(key) {
                    assert_eq!(&values[key], expected, "{id}: {key}");
                }
            }
        }
        assert!(argv_count >= 43);
        assert_eq!(producer_count, 1);
    }

    fn environment() -> crate::cli::Environment {
        crate::cli::Environment(
            [
                (
                    "RPGMIGRATE_MIGRATION_UID",
                    "12345678-9abc-def0-1234-56789abcdef0",
                ),
                ("RPGMIGRATE_RUN_ID", "123456789abcdef0123456789abcdef0"),
                ("RPGMIGRATE_WORKDIR_ID", "pvc-owned"),
                ("RPGMIGRATE_CHECKPOINT_GENERATION", "1"),
            ]
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect(),
        )
    }

    #[test]
    fn operator_identity_maps_exactly_to_native_run_bytes() {
        assert_eq!(
            operator_run(&environment()).unwrap(),
            Some(crate::identity::RunId([
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc,
                0xde, 0xf0,
            ]))
        );
        assert_eq!(
            operator_run(&crate::cli::Environment(Default::default())).unwrap(),
            None
        );
    }

    #[test]
    fn complete_operator_binding_and_report_work_preserve_canonical_values() {
        let operator = operator_identity(&environment()).unwrap().unwrap();
        assert_eq!(
            operator.migration_uid,
            "12345678-9abc-def0-1234-56789abcdef0"
        );
        assert_eq!(operator.workdir_id, "pvc-owned");
        assert_eq!(operator.checkpoint_generation, 1);
        assert_eq!(
            work(Some(&operator)).unwrap(),
            serde_json::json!({
                "migration_uid": "12345678-9abc-def0-1234-56789abcdef0",
                "run_id": "123456789abcdef0123456789abcdef0",
                "workdir_id": "pvc-owned",
                "checkpoint_generation": 1,
            })
        );
        assert_eq!(work(None), None);
    }

    #[test]
    fn bound_copy_reports_emit_only_m1_fields_and_required_command_capabilities() {
        let operator = operator_identity(&environment()).unwrap().unwrap();
        let copy = serde_json::json!({
            "tables":{"total":2,"done":2},"indexes":{"total":1,"done":1},"bytes":{"total":9,"done":9},
        });
        for (command, name, capability) in [
            (CopyCommand::Clone, "clone", "clone.single_db"),
            (CopyCommand::Progress, "list progress", "progress.v1"),
        ] {
            let output = copy_report(command, Some(&operator), "completed", &copy).unwrap();
            let value: serde_json::Value =
                serde_json::from_str(output.strip_prefix("native-report: ").unwrap()).unwrap();
            assert_eq!(
                value,
                serde_json::json!({
                    "protocol_version":1,"engine_version":env!("CARGO_PKG_VERSION"),"command":name,
                    "capabilities":[capability],"work":work(Some(&operator)),"phase":"completed","copy":copy,
                })
            );
        }
    }

    #[test]
    fn standalone_progress_is_unframed_and_never_fabricates_operator_work() {
        let output = copy_report(
            CopyCommand::Progress,
            None,
            "copying",
            &serde_json::json!({"tables":{"total":2}}),
        )
        .unwrap();
        assert!(!output.starts_with("native-report: "));
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"command":"list progress","phase":"copying","copy":{"tables":{"total":2}}})
        );
        assert!(copy_report(CopyCommand::Clone, None, "complete", &serde_json::json!({})).is_err());
    }

    #[test]
    fn streaming_report_never_claims_follow_completion_or_drain() {
        let operator = operator_identity(&environment()).unwrap().unwrap();
        let output = copy_report(
            CopyCommand::Clone,
            Some(&operator),
            "streaming",
            &serde_json::json!({
                "tables":{"total":1,"done":1},
                "indexes":{"total":0,"done":0},
                "bytes":{"total":4,"done":4},
            }),
        )
        .unwrap();
        let value: serde_json::Value =
            serde_json::from_str(output.strip_prefix("native-report: ").unwrap()).unwrap();
        assert_eq!(value["phase"], "streaming");
        assert_eq!(value["command"], "clone");
        assert_eq!(value["work"], work(Some(&operator)).unwrap());
        assert!(value.get("complete").is_none());
        assert!(value.get("drain_proof").is_none());
        assert_eq!(
            value["capabilities"],
            serde_json::json!(["clone.single_db"])
        );
    }

    #[test]
    fn follow_info_events_expose_only_fixed_shapes_and_safe_counts() {
        let args = [
            "rpgmigrate",
            "follow",
            "--skip-large-objects",
            "--source",
            "host=source.invalid password=private-canary",
            "--target",
            "host=target.invalid",
            "--origin",
            "private-origin",
        ]
        .into_iter()
        .map(OsString::from)
        .collect::<Vec<_>>();
        let crate::contracts::Invocation::Follow(options) =
            crate::cli::parse(&args, &crate::cli::Environment(Default::default())).unwrap()
        else {
            panic!("expected follow")
        };
        let startup = FollowLog::startup(&options).document();
        assert_eq!(startup["event"], "follow.startup");
        assert_eq!(startup["error_severity"], "INFO");
        assert_eq!(startup["apply_group_txns"], 100);
        let encoded = startup.to_string();
        for private in [
            "source.invalid",
            "target.invalid",
            "private-canary",
            "private-origin",
            "password",
        ] {
            assert!(!encoded.contains(private));
        }
        let full = FollowLog::Backpressure {
            limits: FollowLimits::default(),
            queued_messages: 2,
            queued_bytes: 300,
        }
        .document();
        assert_eq!(
            full,
            serde_json::json!({
                "level":"info", "error_severity":"INFO", "event":"follow.backpressure",
                "message":"native follow backpressure", "queue":{
                    "messages":2, "owned_bytes":300, "limit_messages":1024, "limit_bytes":8*1024*1024,
                },
            })
        );
    }

    #[test]
    fn operator_identity_refuses_partial_noncanonical_and_mismatched_binding() {
        for (name, value) in [
            (
                "RPGMIGRATE_MIGRATION_UID",
                "12345678-9ABC-def0-1234-56789abcdef0",
            ),
            (
                "RPGMIGRATE_MIGRATION_UID",
                "123456789abcdef0123456789abcdef0",
            ),
            ("RPGMIGRATE_RUN_ID", "00000000000000000000000000000000"),
            ("RPGMIGRATE_RUN_ID", "123456789ABCDEF0123456789ABCDEF0"),
            ("RPGMIGRATE_WORKDIR_ID", ""),
            ("RPGMIGRATE_WORKDIR_ID", " foreign"),
            ("RPGMIGRATE_WORKDIR_ID", "foreign/owned"),
            ("RPGMIGRATE_WORKDIR_ID", "foreign\\owned"),
            ("RPGMIGRATE_WORKDIR_ID", "foreign\nowned"),
            ("RPGMIGRATE_CHECKPOINT_GENERATION", "01"),
            ("RPGMIGRATE_CHECKPOINT_GENERATION", "2"),
        ] {
            let mut env = environment();
            env.0.insert(name.into(), value.into());
            let error = operator_run(&env).unwrap_err();
            assert_eq!(error.kind, crate::contracts::ErrorKind::BadConfig, "{name}");
            assert!(!error.message.contains(value) || value.is_empty(), "{name}");
        }
        let mut env = environment();
        env.0.remove(&OsString::from("RPGMIGRATE_WORKDIR_ID"));
        assert!(operator_run(&env).is_err());
        env.0
            .insert("RPGMIGRATE_WORKDIR_ID".into(), "x".repeat(257).into());
        assert!(operator_run(&env).is_err());
    }
}
