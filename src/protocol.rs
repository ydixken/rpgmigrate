use serde::Serialize;

use crate::cli::Environment;
use crate::contracts::{Error, ErrorKind, OperatorIdentity, Result};
use crate::identity::RunId;

#[derive(Serialize)]
pub(crate) struct Version {
    protocol_version: u32,
    engine_version: &'static str,
    capabilities: [&'static str; 0],
}

impl Version {
    pub(crate) fn current() -> Self {
        Self {
            protocol_version: 1,
            engine_version: env!("CARGO_PKG_VERSION"),
            capabilities: [],
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
    let invalid = || Error {
        kind: ErrorKind::BadConfig,
        message: "invalid operator identity environment; supply all four canonical identity values"
            .into(),
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
            _ => Err(Error {
                kind: ErrorKind::BadState,
                message: "native report phase is unsupported".into(),
            }),
        }
    }
}

#[derive(Serialize)]
struct CopyDocument<'a, C> {
    protocol_version: u32,
    engine_version: &'static str,
    command: &'static str,
    capabilities: [&'static str; 1],
    work: serde_json::Value,
    phase: Phase,
    copy: &'a C,
}

#[derive(Serialize)]
struct StandaloneCopyDocument<'a, C> {
    command: &'static str,
    phase: Phase,
    copy: &'a C,
}

pub(crate) fn copy_report(
    command: CopyCommand,
    operator: Option<&OperatorIdentity>,
    phase: &str,
    copy: &impl Serialize,
) -> Result<String> {
    let (command, capability) = match command {
        CopyCommand::Clone => ("clone", "clone.single_db"),
        CopyCommand::Progress => ("list progress", "progress.v1"),
    };
    let phase = Phase::parse(phase)?;
    let encoded = match work(operator) {
        Some(work) => serde_json::to_string(&CopyDocument {
            protocol_version: 1,
            engine_version: env!("CARGO_PKG_VERSION"),
            command,
            capabilities: [capability],
            work,
            phase,
            copy,
        })
        .map(|document| format!("native-report: {document}\n")),
        None => serde_json::to_string(&StandaloneCopyDocument {
            command,
            phase,
            copy,
        })
        .map(|document| format!("{document}\n")),
    };
    encoded.map_err(|_| Error {
        kind: ErrorKind::Internal,
        message: "cannot encode native copy report".into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn shared_fixtures_bind_discovery_and_configuration_to_real_parser() {
        let root =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/native/v1");
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("cases.json")).unwrap()).unwrap();
        let mut argv_count = 0;
        let mut version_count = 0;
        for case in catalog["cases"].as_array().unwrap() {
            let id = case["id"].as_str().unwrap();
            if case["kind"] == "version" && case["accept"] == true {
                let fixture: serde_json::Value = serde_json::from_slice(
                    &std::fs::read(root.join(case["payload"].as_str().unwrap())).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    serde_json::to_value(Version::current()).unwrap(),
                    fixture,
                    "{id}"
                );
                version_count += 1;
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
        assert!(version_count > 0);
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
