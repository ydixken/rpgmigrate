use super::SourceSnapshot;
use crate::apply::serial::{SerialApplier, SerialBudgets};
use crate::apply::{SnapshotProof, TargetAuthority, copy_metadata_names_for_test};
use crate::catalog::{VerifiedCdcSource, command, load_cdc_relation, read_source_identity};
use crate::cdc::codec::{self, CompletedTx, Frame, Record};
use crate::cdc::contracts::{Event, Plugin};
use crate::cdc::decode::{Decoder, DecoderPosition, RelationResolver};
use crate::contracts::{
    Conninfo, CopyFormat, Error, ErrorKind, IoControl, QualifiedTable, Result, SnapshotAnchor,
    StateAction, StateRequest,
};
use crate::copy_plan::{PlanEntry, open_sealed_copy_plan};
use crate::identity::{StreamId, TxSeq};
use crate::pg::{PgConnection, ReplicationPoll};
use crate::state::{
    ConfirmedProvenance, CopySnapshotIntent, NativeState, SourceSlotBinding, TargetProvenance,
};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, atomic::AtomicBool};
use std::time::{Duration, Instant};

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(30),
        cancelled: Arc::new(AtomicBool::new(false)),
    }
}

fn context() -> Value {
    let path = std::env::var_os("RPGMIGRATE_TEST_COPY_SNAPSHOT_CONTEXT")
        .expect("owned COPY snapshot fixture required");
    let value: Value = serde_json::from_reader(File::open(path).unwrap()).unwrap();
    for owner in [&value, &value["copy_snapshot_source"]] {
        assert_eq!(
            std::fs::read_to_string(
                PathBuf::from(owner["root"].as_str().unwrap()).join(".e1-owner")
            )
            .unwrap(),
            owner["token"].as_str().unwrap()
        );
    }
    assert_eq!(value["copy_snapshot_source"]["logical_slots"], 16);
    assert_eq!(value["copy_snapshot_slots"].as_object().unwrap().len(), 14);
    value
}

fn connect(variable: &str, io: &IoControl) -> PgConnection {
    PgConnection::connect(
        &Conninfo::new(std::env::var_os(variable).expect("COPY snapshot endpoint missing"))
            .unwrap(),
        io,
    )
    .unwrap()
}

fn source_info(case: &str, variable: &str) -> Conninfo {
    let owned = context();
    let database = owned["copy_snapshot_variants"][case]["source_database"]
        .as_str()
        .unwrap();
    assert_eq!(database, format!("copy_{case}"));
    let base = std::env::var(variable).expect("COPY source endpoint missing");
    assert_eq!(base.matches("dbname='copy_atomic'").count(), 1);
    Conninfo::new(
        base.replacen("dbname='copy_atomic'", &format!("dbname='{database}'"), 1)
            .into(),
    )
    .unwrap()
}

fn source_admin(case: &str, io: &IoControl) -> PgConnection {
    PgConnection::connect(
        &source_info(case, "RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE_ADMIN"),
        io,
    )
    .unwrap()
}

fn scalar(connection: &mut PgConnection, sql: &str, io: &IoControl) -> String {
    let result = connection.query(sql, io).unwrap();
    assert_eq!(result.rows(), 1);
    String::from_utf8(result.cell(0, 0).unwrap().unwrap().to_vec()).unwrap()
}

fn target_admin(io: &IoControl) -> PgConnection {
    connect("RPGMIGRATE_TEST_AUTHORITY_ADMIN", io)
}

fn manifest(native: &NativeState) -> Value {
    let value: Value = serde_json::from_reader(
        File::open(native.identity().canonical_path.join("manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(value["magic"], "RPGMIGRATE");
    assert_eq!(value["format_version"], 4);
    assert!(value.get("copy_snapshot").is_some());
    value
}

fn native(label: &str) -> NativeState {
    crate::state::open(&StateRequest {
        path: PathBuf::from(context()["root"].as_str().unwrap())
            .join(format!("owning-copy-{label}")),
        action: StateAction::Create,
    })
    .unwrap()
}

fn qualified(connection: &mut PgConnection, schema: &str, table: &str) -> String {
    format!(
        "{}.{}",
        connection.quote_identifier(schema).unwrap(),
        connection.quote_identifier(table).unwrap()
    )
}

fn dynamic_fixture(case: &str, stream: StreamId, io: &IoControl) {
    if !matches!(case, "atomic" | "owned_table" | "owned_index") {
        return;
    }
    let (canonical, table, index) = copy_metadata_names_for_test(stream);
    let schema = if case == "atomic" {
        format!("{canonical}_lookalike")
    } else {
        canonical
    };
    let name = if case == "owned_index" { index } else { table };
    let mut source = source_admin(case, io);
    let owner = context();
    assert_eq!(
        scalar(
            &mut source,
            "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database()",
            io
        ),
        owner["copy_snapshot_source"]["token"].as_str().unwrap()
    );
    let relation = qualified(&mut source, &schema, &name);
    let schema_sql = source.quote_identifier(&schema).unwrap();
    let role = source
        .quote_identifier(
            owner["copy_snapshot_source"]["replication_role"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    for (sql, tag) in [
        (format!("CREATE SCHEMA {schema_sql}"), "CREATE SCHEMA"),
        (
            format!("CREATE TABLE {relation} (id integer PRIMARY KEY,payload integer)"),
            "CREATE TABLE",
        ),
        (
            format!("GRANT USAGE ON SCHEMA {schema_sql} TO {role}"),
            "GRANT",
        ),
        (format!("GRANT SELECT ON {relation} TO {role}"), "GRANT"),
    ] {
        command(&mut source, &sql, tag, io).unwrap();
    }
    if case == "atomic" {
        let mut target = connect("RPGMIGRATE_TEST_AUTHORITY_TARGET", io);
        let target_relation = qualified(&mut target, &schema, &name);
        let target_schema = target.quote_identifier(&schema).unwrap();
        for (sql, tag) in [
            (format!("CREATE SCHEMA {target_schema}"), "CREATE SCHEMA"),
            (
                format!("CREATE TABLE {target_relation} (id integer PRIMARY KEY,payload integer)"),
                "CREATE TABLE",
            ),
        ] {
            command(&mut target, &sql, tag, io).unwrap();
        }
    }
}

fn source(
    case: &str,
    io: &IoControl,
    relay: Option<&Conninfo>,
) -> (NativeState, SourceSnapshot, SourceSlotBinding) {
    let native = native(case);
    source_with_native(case, native, io, relay, "")
}

fn source_with_native(
    case: &str,
    mut native: NativeState,
    io: &IoControl,
    relay: Option<&Conninfo>,
    suffix: &str,
) -> (NativeState, SourceSnapshot, SourceSlotBinding) {
    let mut observer = source_admin(case, io);
    let identity = read_source_identity(&mut observer, io).unwrap().0;
    assert_eq!(
        serde_json::to_value(identity).unwrap(),
        context()["copy_snapshot_variants"][case]["source_identity"]
    );
    let stream = StreamId {
        run: native.identity().run,
        source: identity,
        generation: 9,
    };
    dynamic_fixture(case, stream, io);
    let slot = format!(
        "{}{suffix}",
        context()["copy_snapshot_slots"][case].as_str().unwrap()
    );
    let binding = SourceSlotBinding::new(stream, slot, Plugin::TestDecoding, 7).unwrap();
    let sql = if let Some(relay) = relay {
        PgConnection::connect(relay, io).unwrap()
    } else {
        PgConnection::connect(
            &source_info(case, "RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE"),
            io,
        )
        .unwrap()
    };
    let exporter = PgConnection::connect_replication(
        &source_info(case, "RPGMIGRATE_TEST_COPY_SNAPSHOT_EXPORTER"),
        io,
    )
    .unwrap();
    let snapshot = SourceSnapshot::create(sql, exporter, binding.clone(), &mut native, io).unwrap();
    native.checked_source_binding(&binding).unwrap();
    assert_eq!(snapshot.receipt().intent.binding.catalog_epoch(), 7);
    assert_eq!(snapshot.receipt().intent.binding.stream().generation, 9);
    (native, snapshot, binding)
}

fn publish(
    source: &mut SourceSnapshot,
    native: &mut NativeState,
    format: CopyFormat,
    io: &IoControl,
) -> CopySnapshotIntent {
    let nonce = native.identity().run.0;
    let intent = source
        .publish_copy_plan(native, nonce, format, 64 * 1024, io)
        .unwrap();
    assert!(native.checked_copy_snapshot().unwrap().is_none());
    assert_eq!(manifest(native)["target_provenance"], json!([]));
    assert_eq!(
        intent.anchor.stream,
        source.receipt().intent.binding.stream()
    );
    assert_eq!(
        intent.anchor.snapshot_token,
        source.receipt().snapshot_token
    );
    assert_eq!(
        intent.anchor.consistent_point,
        source.receipt().consistent_point
    );
    assert!(intent.anchor.consistent_point.0 > 0);
    intent
}

fn altered_plan(
    case: &str,
    source: &mut SourceSnapshot,
    native: &mut NativeState,
    io: &IoControl,
) -> CopySnapshotIntent {
    source.check_live(io).unwrap();
    let receipt = source.receipt().clone();
    let nonce = native.identity().run.0;
    let count = context()["copy_snapshot_variants"][case]["tables"]
        .as_u64()
        .unwrap();
    let mut relations = super::CopyRelations::open(
        &mut source.source,
        receipt.intent.binding.catalog_epoch(),
        io,
    )
    .unwrap();
    let mut position = 0;
    let published = {
        let mut entries = relations.by_ref().map(|relation| {
            position += 1;
            let format = if case == "late_binary" && position == count {
                CopyFormat::Binary
            } else {
                CopyFormat::Text
            };
            let (mut table, mut event) = relation?.into_parts(format)?;
            if case == "format_mismatch" && position == count {
                let crate::cdc::contracts::Event::Relation { columns, .. } = &mut event else {
                    unreachable!()
                };
                columns[1].name = "payload_changed".into();
                table.columns[1] = "payload_changed".into();
            }
            PlanEntry::checked(table, event)
        });
        crate::copy_plan::publish_copy_plan(native, &receipt, nonce, 64 * 1024, &mut entries, io)
    };
    let complete = relations.require_complete();
    let cleanup = relations.close();
    let plan = published.unwrap();
    complete.unwrap();
    cleanup.unwrap();
    drop(relations);
    assert_eq!(position, count);
    source.check_live(io).unwrap();
    CopySnapshotIntent {
        anchor: SnapshotAnchor {
            stream: receipt.intent.binding.stream(),
            snapshot_token: receipt.snapshot_token,
            consistent_point: receipt.consistent_point,
            plan_nonce: nonce,
        },
        plan,
    }
}

fn confirmed(native: &NativeState, stream: &StreamId) -> ConfirmedProvenance {
    let Some(TargetProvenance::Confirmed(value)) = native.target_provenance(stream) else {
        panic!("actual target provenance required")
    };
    value.clone()
}

fn metadata_table(connection: &mut PgConnection, confirmed: &ConfirmedProvenance) -> String {
    qualified(
        connection,
        &confirmed.intent.binding.schema_name,
        &confirmed.intent.binding.table_name,
    )
}

fn counter(confirmed: &ConfirmedProvenance, io: &IoControl) -> u64 {
    let mut observer = target_admin(io);
    let table = metadata_table(&mut observer, confirmed);
    scalar(
        &mut observer,
        &format!("SELECT barrier_counter::text FROM {table}"),
        io,
    )
    .parse()
    .unwrap()
}

fn acquire(
    native: &mut NativeState,
    intent: &CopySnapshotIntent,
    io: &IoControl,
    relay: Option<&Conninfo>,
) -> (TargetAuthority, ConfirmedProvenance, u64) {
    let target = if let Some(relay) = relay {
        PgConnection::connect(relay, io).unwrap()
    } else {
        connect("RPGMIGRATE_TEST_AUTHORITY_TARGET", io)
    };
    let authority = TargetAuthority::acquire(target, intent.anchor.stream, native, io).unwrap();
    let provenance = confirmed(native, &intent.anchor.stream);
    let expected = copy_metadata_names_for_test(intent.anchor.stream);
    assert_eq!(provenance.intent.binding.schema_name, expected.0);
    assert_eq!(provenance.intent.binding.table_name, expected.1);
    let mut observer = target_admin(io);
    let index = observer.query(&format!("SELECT i.indexrelid,c.relname,n.nspname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE i.indrelid={} AND i.indisprimary", provenance.table_oid), io).unwrap();
    assert_eq!(index.rows(), 1);
    let index_oid: u32 = std::str::from_utf8(index.cell(0, 0).unwrap().unwrap())
        .unwrap()
        .parse()
        .unwrap();
    assert!(index_oid != 0 && index_oid != provenance.table_oid);
    assert_eq!(index.cell(0, 1).unwrap().unwrap(), expected.2.as_bytes());
    assert_eq!(index.cell(0, 2).unwrap().unwrap(), expected.0.as_bytes());
    native.bind_copy_snapshot(intent).unwrap();
    assert_eq!(
        native.checked_copy_snapshot().unwrap(),
        Some(intent.clone())
    );
    let baseline = counter(&provenance, io);
    (authority, provenance, baseline)
}

fn assert_proof(
    proof: &SnapshotProof,
    intent: &CopySnapshotIntent,
    provenance: &ConfirmedProvenance,
    baseline: u64,
) {
    assert_eq!(proof.anchor(), &intent.anchor);
    assert_eq!(proof.durable().stream(), intent.anchor.stream);
    assert_eq!(proof.durable().target(), provenance);
    assert!(proof.durable().frontier().is_none());
    assert_eq!(
        proof.durable().barrier_counter(),
        baseline.checked_add(1).unwrap()
    );
}

fn table_rows(connection: &mut PgConnection, schema: &str, table: &str, io: &IoControl) -> Value {
    let table = qualified(connection, schema, table);
    serde_json::from_str(&scalar(connection, &format!("SELECT coalesce(json_agg(json_build_array(id,payload) ORDER BY id),'[]'::json)::text FROM {table}"), io)).unwrap()
}

fn assert_data(case: &str, io: &IoControl, copied: bool) {
    let owned = context();
    let count = owned["copy_snapshot_variants"][case]["initial_tables"]
        .as_u64()
        .unwrap();
    let mut source = source_admin(case, io);
    let mut target = target_admin(io);
    for number in 0..count {
        let name = format!("cs_{case}_{number:03}");
        let name = if count >= 128 {
            format!("{name:_<63}")
        } else {
            name
        };
        let expected = table_rows(&mut source, "public", &name, io);
        assert_eq!(expected.as_array().unwrap().len(), 2);
        assert_eq!(
            table_rows(&mut target, "public", &name, io),
            if copied { expected } else { json!([]) }
        );
    }
}

struct Fault {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
    receipt: PathBuf,
}

impl Fault {
    fn start(side: &str, case: &str, phase: &str, io: &IoControl) -> (Self, Conninfo) {
        let target = PathBuf::from(
            std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER")
                .expect("owned fault helper required"),
        );
        let helper = if side == "target" {
            target
        } else {
            assert_eq!(side, "source");
            target.parent().unwrap().join("source_snapshot_fault.py")
        };
        let mut child = Command::new(
            std::env::var_os("RPGMIGRATE_TEST_PYTHON").expect("fixture Python required"),
        )
        .arg(helper)
        .args(["--copy-case", case, "--phase", phase])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
        let mut fault = Self {
            input: child.stdin.take().unwrap(),
            output: child.stdout.take().unwrap(),
            child,
            receipt: PathBuf::new(),
        };
        let ready = fault.read(io);
        assert_eq!(ready["ready"], true);
        fault.receipt = PathBuf::from(ready["receipt"].as_str().unwrap());
        assert_eq!(
            fault.receipt,
            PathBuf::from(context()["root"].as_str().unwrap())
                .join(format!("copy-snapshot-wire-{side}-{case}-{phase}.json"))
        );
        let info = Conninfo::new(ready["conninfo"].as_str().unwrap().into()).unwrap();
        (fault, info)
    }

    fn read(&mut self, io: &IoControl) -> Value {
        let mut bytes = Vec::new();
        loop {
            let remaining = io
                .deadline
                .checked_duration_since(Instant::now())
                .expect("COPY helper response deadline");
            let mut descriptor = libc::pollfd {
                fd: self.output.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: the output descriptor remains live for this bounded poll.
            let ready = unsafe {
                libc::poll(
                    &mut descriptor,
                    1,
                    i32::try_from(remaining.as_millis().max(1)).unwrap_or(i32::MAX),
                )
            };
            if ready < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
                continue;
            }
            assert!(ready > 0, "COPY helper response deadline or poll error");
            let mut byte = [0];
            assert_eq!(
                self.output.read(&mut byte).unwrap(),
                1,
                "COPY helper closed without receipt"
            );
            if byte[0] == b'\n' {
                return serde_json::from_slice(&bytes).unwrap();
            }
            bytes.push(byte[0]);
            assert!(bytes.len() <= 64 * 1024, "oversized COPY helper response");
        }
    }

    fn command(&mut self, request: Value, io: &IoControl) -> Value {
        serde_json::to_writer(&mut self.input, &request).unwrap();
        self.input.write_all(b"\n").unwrap();
        self.input.flush().unwrap();
        self.read(io)
    }

    fn arm(&mut self, native: &NativeState, peer: Option<u32>, io: &IoControl) {
        let mut request = json!({"op":"arm","native_path":native.identity().canonical_path.join("manifest.json")});
        if let Some(peer) = peer {
            request["peer_pid"] = json!(peer);
        }
        let result = self.command(request, io);
        assert_eq!(result["armed"], true);
        assert_eq!(result["fault_hits"], 0);
    }

    fn release(&mut self, io: &IoControl) {
        let held = self.command(json!({"op":"status"}), io);
        assert_eq!(held["held_fence_observed"], true);
        let released = self.command(json!({"op":"release"}), io);
        assert_eq!(released["released_fence_observed"], true);
        assert_eq!(released["backend_closed"], true);
    }

    fn finish(mut self, io: &IoControl) -> Value {
        let receipt = self.command(json!({"op":"quit"}), io);
        assert_eq!(receipt["status"], "observed");
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < io.deadline, "COPY helper reap deadline");
            std::thread::yield_now();
        };
        assert!(status.success(), "COPY relay helper failed");
        let mut value: Value = serde_json::from_reader(File::open(&self.receipt).unwrap()).unwrap();
        assert_eq!(value["helper_pid"], self.child.id());
        assert_eq!(value["helper_reaped"], false);
        assert!(value["helper_returncode"].is_null());
        value["helper_reaped"] = json!(true);
        value["helper_returncode"] = json!(status.code().unwrap());
        std::fs::write(&self.receipt, serde_json::to_vec(&value).unwrap()).unwrap();
        value
    }
}

fn owned_oids(provenance: &ConfirmedProvenance, io: &IoControl) -> [u32; 2] {
    let mut observer = target_admin(io);
    let index = scalar(
        &mut observer,
        &format!(
            "SELECT indexrelid::text FROM pg_index WHERE indrelid={} AND indisprimary",
            provenance.table_oid
        ),
        io,
    )
    .parse::<u32>()
    .unwrap();
    assert!(index != 0 && index != provenance.table_oid);
    [provenance.table_oid, index]
}

fn alias_oids(case: &str, stream: StreamId, provenance: &ConfirmedProvenance, io: &IoControl) {
    let (canonical, table, index) = copy_metadata_names_for_test(stream);
    let schema = if case == "atomic" {
        format!("{canonical}_lookalike")
    } else {
        canonical
    };
    let name = if case == "owned_index" { index } else { table };
    let mut target = target_admin(io);
    let target_name = qualified(&mut target, &schema, &name);
    let literal = target.quote_literal(&target_name).unwrap();
    let target_oid: u32 = scalar(
        &mut target,
        &format!("SELECT {literal}::regclass::oid::text"),
        io,
    )
    .parse()
    .unwrap();
    let pair = owned_oids(provenance, io);
    let mut source = source_admin(case, io);
    let source_name = qualified(&mut source, &schema, &name);
    let literal = source.quote_literal(&source_name).unwrap();
    let source_oid: u32 = scalar(
        &mut source,
        &format!("SELECT {literal}::regclass::oid::text"),
        io,
    )
    .parse()
    .unwrap();
    assert_ne!(source_oid, 0);
    if case == "atomic" {
        assert!(!pair.contains(&target_oid));
        let index_oid: u32 = scalar(
            &mut target,
            &format!(
                "SELECT indexrelid::text FROM pg_index WHERE indrelid={target_oid} AND indisprimary"
            ),
            io,
        )
        .parse()
        .unwrap();
        assert!(!pair.contains(&index_oid));
        assert_eq!(table_rows(&mut source, &schema, &name, io), json!([]));
        assert_eq!(table_rows(&mut target, &schema, &name, io), json!([]));
    } else {
        assert_eq!(target_oid, pair[usize::from(case == "owned_index")]);
        assert!(
            !pair.contains(&source_oid),
            "source OID must not substitute for actual target class membership"
        );
    }
}

fn binary_metadata(io: &IoControl) {
    let extra = native("binary-metadata");
    let (mut native, mut source, _) = source_with_native("late_binary", extra, io, None, "_binary");
    let intent = publish(&mut source, &mut native, CopyFormat::Binary, io);
    let (target, _, _) = acquire(&mut native, &intent, io, None);
    let mut reader = open_sealed_copy_plan(&native, &intent, io).unwrap();
    let mut count = 0;
    reader
        .scan_from(0, io, |_, format, _| {
            assert_eq!(format, CopyFormat::Binary);
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 129);
    drop(reader);
    assert_data("late_binary", io, false);
    assert!(!source.copy.failed && !source.copy.attempted);
    drop(target);
    drop(source);
}

#[test]
#[ignore = "requires owned COPY snapshot harness"]
fn live_copy_snapshot_admission() {
    binary_metadata(&control());
    for case in [
        "page128",
        "page129",
        "late_refusal",
        "late_binary",
        "format_mismatch",
        "owned_table",
        "owned_index",
    ] {
        let io = control();
        let (mut native, mut source, _) = source(case, &io, None);
        let intent = if matches!(case, "late_binary" | "format_mismatch") {
            altered_plan(case, &mut source, &mut native, &io)
        } else {
            publish(&mut source, &mut native, CopyFormat::Text, &io)
        };
        let expected = context()["copy_snapshot_variants"][case]["tables"]
            .as_u64()
            .unwrap();
        assert_eq!(intent.plan.table_count, expected);
        if expected >= 128 {
            assert!(
                intent.plan.logical_bytes > 16 * 1024,
                "{case}: canonical plan has only {} bytes",
                intent.plan.logical_bytes
            );
        }
        let positive = matches!(case, "page128" | "page129");
        let (mut fault, relay) = Fault::start(
            "target",
            case,
            if positive { "copy" } else { "admission" },
            &io,
        );
        let (mut target, provenance, baseline) = acquire(&mut native, &intent, &io, Some(&relay));
        let control_bytes = native
            .identity()
            .canonical_path
            .join("manifest.json")
            .metadata()
            .unwrap()
            .len();
        assert!(
            control_bytes < 16 * 1024,
            "{case}: bound native control has {control_bytes} bytes"
        );
        if matches!(case, "owned_table" | "owned_index") {
            alias_oids(case, intent.anchor.stream, &provenance, &io);
        }
        fault.arm(&native, None, &io);
        let result = target.copy_snapshot(&mut source, &native, &io);
        if positive {
            let (report, proof) = result.unwrap();
            assert_eq!(report.tables, expected);
            assert_eq!(report.rows, expected * 2);
            assert_proof(&proof, &intent, &provenance, baseline);
            assert_data(case, &io, true);
            drop(target);
            let receipt = fault.finish(&io);
            assert_eq!(receipt["copy_in_starts"], expected);
            let owner = source.finish(&io).unwrap();
            native.record_copy_complete(&report).unwrap();
            drop(owner);
        } else {
            let error = result.unwrap_err();
            let reason = match case {
                "owned_table" | "owned_index" => {
                    "COPY cannot target an owned target metadata relation"
                }
                "late_refusal" => "CDC target relation metadata does not match the source relation",
                "late_binary" => "COPY requires text format and explicit columns",
                "format_mismatch" => "source-snapshot-copy: canonical source relation changed",
                _ => unreachable!(),
            };
            assert_eq!(error.message, reason);
            assert!(source.copy.failed && !source.copy.attempted);
            drop(target);
            let receipt = fault.finish(&io);
            assert_eq!(receipt["copy_in_starts"], 0);
            assert_eq!(receipt["commit_tags"], json!([]));
            assert_eq!(receipt["update_tags"], json!([]));
            assert_eq!(counter(&provenance, &io), baseline);
            assert_data(case, &io, false);
            assert!(manifest(&native)["copy_complete"].is_null());
        }
    }
}

fn fresh_target(native: &mut NativeState, stream: StreamId, io: &IoControl) -> TargetAuthority {
    TargetAuthority::acquire(
        connect("RPGMIGRATE_TEST_AUTHORITY_TARGET", io),
        stream,
        native,
        io,
    )
    .unwrap()
}

fn assert_closed(provenance: &ConfirmedProvenance, pid: u32, io: &IoControl) {
    let mut observer = target_admin(io);
    loop {
        if scalar(
            &mut observer,
            &format!(
                "SELECT count(*) FROM pg_stat_activity WHERE pid={pid} AND datid={}",
                provenance.intent.binding.target_database_oid
            ),
            io,
        ) == "0"
        {
            return;
        }
        assert!(
            Instant::now() < io.deadline,
            "owned target backend closure deadline"
        );
        std::thread::yield_now();
    }
}

fn assert_poisoned(
    target: &mut TargetAuthority,
    provenance: &ConfirmedProvenance,
    pid: u32,
    io: &IoControl,
) {
    // Reuse can itself close the session, so observe the original failure first.
    assert_closed(provenance, pid, io);
    let error = target.barrier(None, 0, io).unwrap_err();
    assert_eq!(error.message, "target-authority: transaction is not idle");
}

#[test]
#[ignore = "requires owned COPY snapshot harness"]
fn live_copy_snapshot_unknown() {
    let io = control();
    let (mut native, mut source, _) = source("unknown", &io, None);
    let intent = publish(&mut source, &mut native, CopyFormat::Text, &io);
    let (mut fault, relay) = Fault::start("target", "unknown", "copy_unknown", &io);
    let (mut target, provenance, baseline) = acquire(&mut native, &intent, &io, Some(&relay));
    fault.arm(&native, None, &io);
    let error = target.copy_snapshot(&mut source, &native, &io).unwrap_err();
    assert_eq!(
        error.message,
        "target-authority: COPY COMMIT outcome unknown; recover on a fresh fenced connection"
    );
    assert!(!source.copy.failed && source.copy.attempted);
    assert_eq!(
        native.checked_copy_snapshot().unwrap(),
        Some(intent.clone())
    );
    let held = fault.command(json!({"op":"status"}), &io);
    assert_eq!(held["held_fence_observed"], true);
    let old_pid = held["backend_pid"].as_u64().unwrap() as u32;
    let blocked_io = IoControl {
        deadline: (Instant::now() + Duration::from_millis(200)).min(io.deadline),
        cancelled: io.cancelled.clone(),
    };
    let contender = connect("RPGMIGRATE_TEST_AUTHORITY_TARGET", &blocked_io);
    assert!(
        TargetAuthority::acquire(contender, intent.anchor.stream, &mut native, &blocked_io)
            .is_err()
    );
    fault.release(&io);
    drop(target);
    fault.finish(&io);
    assert_closed(&provenance, old_pid, &io);
    assert_data("unknown", &io, true);
    assert_eq!(counter(&provenance, &io), baseline + 1);
    let mut alternate = intent.anchor.plan_nonce;
    alternate[0] ^= 1;
    let refused = source
        .publish_copy_plan(&mut native, alternate, CopyFormat::Text, 64 * 1024, &io)
        .unwrap_err();
    assert_eq!(
        refused.message,
        "source-snapshot-copy: source operation cannot be retried"
    );
    assert!(!source.copy.failed && source.copy.attempted);

    let (mut fault, relay) = Fault::start("target", "unknown", "recovery", &io);
    let (mut target, recovered_provenance, recovered_baseline) =
        acquire(&mut native, &intent, &io, Some(&relay));
    assert_eq!(recovered_provenance, provenance);
    fault.arm(&native, None, &io);
    let recovered = target.recover_snapshot(&native, &io).unwrap();
    assert_proof(&recovered, &intent, &provenance, recovered_baseline);
    assert_eq!(counter(&provenance, &io), baseline + 2);
    let pid = u32::try_from(
        fault.command(json!({"op":"status"}), &io)["backend_pid"]
            .as_u64()
            .unwrap(),
    )
    .unwrap();
    let refused = target.copy_snapshot(&mut source, &native, &io).unwrap_err();
    assert_eq!(
        refused.message,
        "target-authority: COPY requires an empty checkpoint and no snapshot anchor"
    );
    assert_poisoned(&mut target, &provenance, pid, &io);
    drop(target);
    let record = fault.finish(&io);
    assert_ne!(record["backend_pid"], old_pid);
    assert_data("unknown", &io, true);
    let owner = source.finish(&io).unwrap();
    assert_eq!(
        owner.receipt().intent.binding.stream(),
        intent.anchor.stream
    );
    assert!(manifest(&native)["copy_complete"].is_null());
}

#[test]
#[ignore = "requires owned COPY snapshot harness"]
fn live_copy_snapshot_source_finish_unknown() {
    let io = control();
    let (mut finish_fault, source_relay) =
        Fault::start("source", "source_finish", "finish_unknown", &io);
    let (mut native, mut source, _) = source("source_finish", &io, Some(&source_relay));
    let peer_pid = source.exporter.backend_pid().unwrap();
    let intent = publish(&mut source, &mut native, CopyFormat::Text, &io);
    let (mut target_fault, target_relay) = Fault::start("target", "source_finish", "copy", &io);
    let (mut target, provenance, baseline) =
        acquire(&mut native, &intent, &io, Some(&target_relay));
    target_fault.arm(&native, None, &io);
    let (report, proof) = target.copy_snapshot(&mut source, &native, &io).unwrap();
    assert_eq!((report.tables, report.rows), (2, 4));
    assert_proof(&proof, &intent, &provenance, baseline);
    assert_data("source_finish", &io, true);
    drop(target);
    let target_receipt = target_fault.finish(&io);
    assert_eq!(target_receipt["helper_reaped"], true);
    finish_fault.arm(&native, Some(peer_pid), &io);
    let error = source
        .finish(&io)
        .err()
        .expect("lost source COMMIT must not return an exporter owner");
    assert_eq!(
        error.message,
        "source-snapshot-finish-unknown: source COMMIT was not confirmed"
    );
    let refused = finish_fault.command(json!({"op":"failed"}), &io);
    assert_eq!(refused["fault_hits"], 1);
    let receipt = finish_fault.finish(&io);
    assert_eq!(receipt["imported"], true);
    assert_eq!(receipt["peer_closed"], true);
    assert_eq!(receipt["native_path"], target_receipt["native_path"]);
    assert_eq!(receipt["stream"], target_receipt["stream"]);
    assert_data("source_finish", &io, true);
    assert!(manifest(&native)["copy_complete"].is_null());
    let mut recovered = fresh_target(&mut native, intent.anchor.stream, &io);
    let proof = recovered.recover_snapshot(&native, &io).unwrap();
    assert_proof(&proof, &intent, &provenance, baseline + 1);
    assert_eq!(counter(&provenance, &io), baseline + 2);
    assert_data("source_finish", &io, true);
}

#[test]
#[ignore = "requires owned COPY snapshot harness"]
fn live_copy_snapshot_advisory_counts() {
    let io = control();
    let (mut native, mut source, _) = source("counts", &io, None);
    let intent = publish(&mut source, &mut native, CopyFormat::Text, &io);
    let (mut fault, relay) = Fault::start("target", "counts", "copy", &io);
    let (mut target, provenance, baseline) = acquire(&mut native, &intent, &io, Some(&relay));
    fault.arm(&native, None, &io);
    let (report, proof) = target.copy_snapshot(&mut source, &native, &io).unwrap();
    assert_proof(&proof, &intent, &provenance, baseline);
    drop(target);
    fault.finish(&io);
    let owner = source.finish(&io).unwrap();
    assert!(manifest(&native)["copy_complete"].is_null());
    let failure = native
        .record_copy_complete_with_create_failure(&report)
        .unwrap_err();
    assert_eq!(failure.message, "state-test: injected publication failure");
    assert!(manifest(&native)["copy_complete"].is_null());
    assert!(
        !native
            .identity()
            .canonical_path
            .join(".manifest.tmp")
            .exists()
    );
    assert_eq!(
        native.checked_copy_snapshot().unwrap(),
        Some(intent.clone())
    );
    let (mut fault, relay) = Fault::start("target", "counts", "recovery", &io);
    let (mut target, _, recovery_baseline) = acquire(&mut native, &intent, &io, Some(&relay));
    fault.arm(&native, None, &io);
    let recovered = target.recover_snapshot(&native, &io).unwrap();
    assert_proof(&recovered, &intent, &provenance, recovery_baseline);
    assert_eq!(counter(&provenance, &io), baseline + 2);
    drop(target);
    fault.finish(&io);
    assert_data("counts", &io, true);
    assert!(manifest(&native)["copy_complete"].is_null());
    let connection = connect("RPGMIGRATE_TEST_AUTHORITY_TARGET", &io);
    let pid = connection.backend_pid().unwrap();
    let mut target =
        TargetAuthority::acquire(connection, intent.anchor.stream, &mut native, &io).unwrap();
    let path = native.identity().canonical_path.join(".manifest.tmp");
    let mut incomplete = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    incomplete.write_all(b"incomplete publication").unwrap();
    incomplete.sync_all().unwrap();
    assert!(native.checked_copy_snapshot().is_err());
    let error = target.recover_snapshot(&native, &io).unwrap_err();
    assert_eq!(error.kind, ErrorKind::BadState);
    assert!(error.message.starts_with("state-"));
    assert_poisoned(&mut target, &provenance, pid, &io);
    assert_eq!(counter(&provenance, &io), baseline + 2);
    assert_data("counts", &io, true);
    drop(owner);
}

impl Drop for Fault {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
#[ignore = "requires owned COPY snapshot harness"]
fn live_copy_snapshot_empty() {
    let io = control();
    let (mut native, mut source, _) = source("empty", &io, None);
    let intent = publish(&mut source, &mut native, CopyFormat::Text, &io);
    assert_eq!(intent.plan.table_count, 0);
    let (mut fault, relay) = Fault::start("target", "empty", "copy", &io);
    let (mut target, provenance, baseline) = acquire(&mut native, &intent, &io, Some(&relay));
    fault.arm(&native, None, &io);
    let (report, proof) = target.copy_snapshot(&mut source, &native, &io).unwrap();
    assert_eq!((report.tables, report.rows, report.bytes), (0, 0, 0));
    assert_proof(&proof, &intent, &provenance, baseline);
    assert!(target.checkpoint(&io).unwrap().is_none());
    assert_eq!(counter(&provenance, &io), baseline + 1);
    drop(target);
    fault.finish(&io);
    let owner = source.finish(&io).unwrap();
    native.record_copy_complete(&report).unwrap();
    assert_eq!(manifest(&native)["copy_complete"]["tables"], 0);
    assert_eq!(
        owner.receipt().intent.binding.stream(),
        intent.anchor.stream
    );
}

#[test]
#[ignore = "requires owned COPY snapshot harness"]
fn live_copy_snapshot_rollback() {
    let io = control();
    let (mut native, mut source, _) = source("rollback", &io, None);
    let intent = publish(&mut source, &mut native, CopyFormat::Text, &io);
    let (mut fault, relay) = Fault::start("target", "rollback", "copy", &io);
    let (mut target, provenance, baseline) = acquire(&mut native, &intent, &io, Some(&relay));
    fault.arm(&native, None, &io);
    let error = target.copy_snapshot(&mut source, &native, &io).unwrap_err();
    assert_eq!(error.message, "PostgreSQL command failed (SQLSTATE 23505)");
    assert!(source.copy.failed);
    assert!(source.copy.attempted);
    drop(target);
    let receipt = fault.finish(&io);
    assert_eq!(receipt["copy_in_starts"], 2);
    assert_eq!(receipt["copy_complete_tags"], json!(["COPY 2"]));
    assert_eq!(receipt["error_sqlstates"], json!(["23505"]));
    assert_data("rollback", &io, false);
    assert_eq!(counter(&provenance, &io), baseline);
    let mut observer = target_admin(&io);
    let table = metadata_table(&mut observer, &provenance);
    assert_eq!(
        scalar(
            &mut observer,
            &format!(
                "SELECT (copy_snapshot_token IS NULL AND copy_consistent_point IS NULL AND copy_plan_nonce IS NULL AND applied_order=0)::text FROM {table}"
            ),
            &io
        ),
        "true"
    );
    assert!(manifest(&native)["copy_complete"].is_null());
}

struct Resolver(Event);

impl RelationResolver for Resolver {
    fn resolve(&mut self, table: &QualifiedTable) -> Result<&Event> {
        if let Event::Relation { schema, name, .. } = &self.0
            && schema == &table.schema
            && name == &table.table
        {
            return Ok(&self.0);
        }
        Err(Error {
            kind: ErrorKind::Source,
            message: "COPY fixture received an unexpected WAL relation".into(),
        })
    }
}

fn receive_one(
    owner: &mut super::OwnedSourceSlot,
    binding: &SourceSlotBinding,
    relation: Event,
    io: &IoControl,
) -> (CompletedTx, Vec<Frame>) {
    let limits = codec::Limits {
        max_frame_bytes: 4096,
        max_event_bytes: 16 * 1024,
        max_parts: 16,
    };
    let mut decoder = Decoder::new(
        Plugin::TestDecoding,
        binding.stream(),
        limits,
        DecoderPosition {
            next_begin_ordinal: 1,
            next_order: TxSeq(1),
        },
    )
    .unwrap();
    let mut resolver = Resolver(relation);
    let mut frames = Vec::with_capacity(64);
    let mut bytes = 64 * std::mem::size_of::<Frame>();
    loop {
        match owner
            .exporter
            .poll_replication(Instant::now() + Duration::from_millis(25), io)
            .unwrap()
        {
            ReplicationPoll::Frame(input) => {
                decoder
                    .feed(&input, &mut resolver, &mut |frame| {
                        bytes = bytes
                            .checked_add(frame.payload.len())
                            .expect("fixture frame charge overflow");
                        if frames.len() == 64 || bytes > 64 * 1024 {
                            return Err(Error {
                                kind: ErrorKind::Source,
                                message: "COPY fixture WAL frame budget exceeded".into(),
                            });
                        }
                        frames.push(frame.clone());
                        Ok(())
                    })
                    .unwrap();
                if let Some(completed) = decoder.take_completed() {
                    return (completed, frames);
                }
            }
            ReplicationPoll::Wake => {}
            ReplicationPoll::FeedbackDrained(_) | ReplicationPoll::End => {
                panic!("unexpected source feedback or terminal frame")
            }
        }
    }
}

#[test]
#[ignore = "requires owned COPY snapshot harness"]
fn live_copy_snapshot_atomic() {
    let io = control();
    let (mut native, mut source, binding) = source("atomic", &io, None);
    let intent = publish(&mut source, &mut native, CopyFormat::Text, &io);
    assert_eq!(intent.plan.table_count, 2);
    let (mut fault, relay) = Fault::start("target", "atomic", "copy", &io);
    let (mut target, provenance, baseline) = acquire(&mut native, &intent, &io, Some(&relay));
    alias_oids("atomic", binding.stream(), &provenance, &io);
    fault.arm(&native, None, &io);
    let (report, proof) = target.copy_snapshot(&mut source, &native, &io).unwrap();
    assert_eq!((report.tables, report.rows), (2, 2));
    assert!(report.bytes > 0);
    assert_proof(&proof, &intent, &provenance, baseline);
    assert_eq!(counter(&provenance, &io), baseline + 1);
    assert!(target.checkpoint(&io).unwrap().is_none());
    assert_data("atomic", &io, true);
    let mut observer = target_admin(&io);
    let table = metadata_table(&mut observer, &provenance);
    assert_eq!(
        scalar(
            &mut observer,
            &format!(
                "SELECT (applied_order=0 AND last_xid IS NULL AND begin_ordinal IS NULL AND source_commit_lsn IS NULL AND source_end_lsn IS NULL)::text FROM {table}"
            ),
            &io
        ),
        "true"
    );
    let relation = load_cdc_relation(
        &mut source.source,
        &QualifiedTable {
            schema: "public".into(),
            table: "cs_atomic_000".into(),
        },
        binding.catalog_epoch(),
        &io,
    )
    .unwrap();
    drop(target);
    let wire = fault.finish(&io);
    assert_eq!(wire["copy_complete_tags"], json!(["COPY 0", "COPY 2"]));
    let mut owner = source.finish(&io).unwrap();
    native.record_copy_complete(&report).unwrap();
    let point = owner.receipt().consistent_point.0;
    owner
        .exporter
        .start_replication(
            binding.slot_name(),
            point,
            &[
                ("include-xids", Some("true")),
                ("include-timestamp", Some("false")),
                ("skip-empty-xacts", Some("false")),
                ("only-local", Some("false")),
                ("include-rewrites", Some("false")),
                ("stream-changes", Some("false")),
            ],
            &io,
        )
        .unwrap();
    let mut writer = source_admin("atomic", &io);
    command(&mut writer, "BEGIN", "BEGIN", &io).unwrap();
    let xid = scalar(&mut writer, "SELECT (txid_current()%4294967296)::text", &io)
        .parse::<u32>()
        .unwrap();
    writer
        .query("INSERT INTO public.cs_atomic_000 VALUES (3,9)", &io)
        .unwrap();
    command(&mut writer, "COMMIT", "COMMIT", &io).unwrap();
    let (completed, frames) = receive_one(&mut owner, &binding, relation, &io);
    assert_eq!(completed.tx.stream, binding.stream());
    assert_eq!(completed.tx.xid, xid);
    assert_eq!(completed.tx.begin_ordinal, 1);
    assert_eq!(completed.order, TxSeq(1));
    assert_eq!(completed.commit, None);
    assert!(completed.end.0 >= intent.anchor.consistent_point.0);
    let authority = fresh_target(&mut native, binding.stream(), &io);
    let source = PgConnection::connect(
        &source_info("atomic", "RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE"),
        &io,
    )
    .unwrap();
    let mut source = VerifiedCdcSource::acquire(source, &native, &binding, &io).unwrap();
    let mut applier = SerialApplier::new(
        authority,
        codec::Limits {
            max_frame_bytes: 4096,
            max_event_bytes: 16 * 1024,
            max_parts: 16,
        },
        SerialBudgets {
            frame_heap_bytes: 64 * 1024,
            reassembly_heap_bytes: 64 * 1024,
            decoded_heap_bytes: 64 * 1024,
            relation_slots: 2,
            relation_bytes: 64 * 1024,
            plan_slots: 2,
            plan_bytes: 1024 * 1024,
            bind_bytes: 64 * 1024,
        },
    )
    .unwrap();
    let applied = applier
        .apply_one(
            &completed,
            &mut source,
            |visit| {
                for frame in &frames {
                    visit(&Record::Frame(frame.clone()))?;
                }
                visit(&Record::Completed(completed))
            },
            &io,
        )
        .unwrap();
    let crate::apply::CommitOutcome::Acknowledged {
        checkpoint,
        durable: Some(durable),
    } = applied
    else {
        panic!("real first CDC transaction must have synchronous proof")
    };
    let through = *durable.frontier().unwrap();
    assert_eq!(checkpoint.applied, through);
    assert_eq!(through.tx, completed.tx);
    assert_eq!(through.order, TxSeq(1));
    assert_eq!(through.end, completed.end);
    assert_eq!(counter(&provenance, &io), baseline + 2);
    assert_eq!(
        table_rows(&mut observer, "public", "cs_atomic_000", &io),
        json!([[1, 7], [2, 8], [3, 9]])
    );
    drop(applier);
    drop(source);
    let mut recovered = fresh_target(&mut native, binding.stream(), &io);
    let proof = recovered.recover_snapshot(&native, &io).unwrap();
    assert_eq!(proof.anchor(), &intent.anchor);
    assert_eq!(proof.durable().frontier(), Some(&through));
    assert_eq!(proof.durable().barrier_counter(), baseline + 3);
    assert_eq!(counter(&provenance, &io), baseline + 3);
    assert_eq!(
        table_rows(&mut observer, "public", "cs_atomic_000", &io),
        json!([[1, 7], [2, 8], [3, 9]])
    );
}

fn restore_anchor(
    observer: &mut PgConnection,
    provenance: &ConfirmedProvenance,
    intent: &CopySnapshotIntent,
    io: &IoControl,
) {
    let table = metadata_table(observer, provenance);
    let token = observer
        .quote_literal(&intent.anchor.snapshot_token)
        .unwrap();
    let point = intent.anchor.consistent_point.0;
    let point = format!("{:X}/{:X}", point >> 32, point & 0xffff_ffff);
    let nonce: String = intent
        .anchor
        .plan_nonce
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    observer.query(&format!("UPDATE {table} SET copy_snapshot_token={token},copy_consistent_point='{point}'::pg_lsn,copy_plan_nonce=decode('{nonce}','hex')"), io).unwrap();
}

#[test]
#[ignore = "requires owned COPY snapshot harness"]
fn live_copy_snapshot_recovery_refusal() {
    let io = control();
    let (mut native, mut source, binding) = source("recovery", &io, None);
    let intent = publish(&mut source, &mut native, CopyFormat::Text, &io);
    {
        let (mut fault, relay) = Fault::start("target", "recovery", "prebegin_intent", &io);
        let connection = PgConnection::connect(&relay, &io).unwrap();
        let pid = connection.backend_pid().unwrap();
        let mut target =
            TargetAuthority::acquire(connection, binding.stream(), &mut native, &io).unwrap();
        let provenance = confirmed(&native, &binding.stream());
        let baseline = counter(&provenance, &io);
        assert!(native.checked_copy_snapshot().unwrap().is_none());
        native.checked_source_binding(&binding).unwrap();
        fault.arm(&native, None, &io);
        let error = target.copy_snapshot(&mut source, &native, &io).unwrap_err();
        assert_eq!(
            error.message,
            "target-authority: COPY snapshot intent is absent"
        );
        assert!(!source.copy.failed && !source.copy.attempted);
        assert_poisoned(&mut target, &provenance, pid, &io);
        drop(target);
        let receipt = fault.finish(&io);
        assert_eq!(receipt["copy_in_starts"], 0);
        assert_eq!(counter(&provenance, &io), baseline);
        assert_eq!(intent.anchor.stream, binding.stream());
        assert_data("recovery", &io, false);
    }

    let (mut fault, relay) = Fault::start("target", "recovery", "setup", &io);
    let (mut target, provenance, baseline) = acquire(&mut native, &intent, &io, Some(&relay));
    fault.arm(&native, None, &io);
    let (report, proof) = target.copy_snapshot(&mut source, &native, &io).unwrap();
    assert_proof(&proof, &intent, &provenance, baseline);
    drop(target);
    fault.finish(&io);
    assert_data("recovery", &io, true);
    let mut observer = target_admin(&io);
    let table = metadata_table(&mut observer, &provenance);
    for phase in ["missing_anchor", "foreign_anchor"] {
        if phase == "missing_anchor" {
            observer.query(&format!("UPDATE {table} SET copy_snapshot_token=NULL,copy_consistent_point=NULL,copy_plan_nonce=NULL"), &io).unwrap();
        } else {
            let mut nonce = intent.anchor.plan_nonce;
            nonce[0] ^= 1;
            if nonce == [0; 16] {
                nonce[1] = 1;
            }
            let hex: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
            observer
                .query(
                    &format!("UPDATE {table} SET copy_plan_nonce=decode('{hex}','hex')"),
                    &io,
                )
                .unwrap();
        }
        let (mut fault, relay) = Fault::start("target", "recovery", phase, &io);
        let connection = PgConnection::connect(&relay, &io).unwrap();
        let pid = connection.backend_pid().unwrap();
        let mut target =
            TargetAuthority::acquire(connection, binding.stream(), &mut native, &io).unwrap();
        fault.arm(&native, None, &io);
        let error = target.recover_snapshot(&native, &io).unwrap_err();
        assert_eq!(
            error.message,
            "target-authority: logged COPY anchor differs from native intent"
        );
        assert_poisoned(&mut target, &provenance, pid, &io);
        drop(target);
        let receipt = fault.finish(&io);
        assert_eq!(receipt["update_tags"], json!([]));
        assert_eq!(counter(&provenance, &io), baseline + 1);
        assert_data("recovery", &io, true);
        restore_anchor(&mut observer, &provenance, &intent, &io);
    }

    let path = {
        let pin = native.open_plan_file(intent.plan.plan_nonce).unwrap();
        pin.checked_path().unwrap().to_path_buf()
    };
    let original = std::fs::read(&path).unwrap();
    assert!(original.len() >= 16 && original.len() <= 1024 * 1024);
    let (mut fault, relay) = Fault::start("target", "recovery", "prebegin_reader", &io);
    let connection = PgConnection::connect(&relay, &io).unwrap();
    let pid = connection.backend_pid().unwrap();
    let mut target =
        TargetAuthority::acquire(connection, binding.stream(), &mut native, &io).unwrap();
    fault.arm(&native, None, &io);
    let mut corrupt = original.clone();
    corrupt[0] ^= 0xff;
    std::fs::write(&path, &corrupt).unwrap();
    assert_eq!(
        native.checked_copy_snapshot().unwrap(),
        Some(intent.clone())
    );
    let error = target.copy_snapshot(&mut source, &native, &io).unwrap_err();
    assert_eq!(error.kind, ErrorKind::BadState);
    assert_eq!(error.message, "copy-plan: SQLite plan operation failed");
    assert!(!source.copy.failed && source.copy.attempted);
    assert_poisoned(&mut target, &provenance, pid, &io);

    let connection = connect("RPGMIGRATE_TEST_AUTHORITY_TARGET", &io);
    let recovery_pid = connection.backend_pid().unwrap();
    assert_ne!(recovery_pid, pid);
    let mut recovery =
        TargetAuthority::acquire(connection, binding.stream(), &mut native, &io).unwrap();
    assert_eq!(
        native.checked_copy_snapshot().unwrap(),
        Some(intent.clone())
    );
    let error = recovery.recover_snapshot(&native, &io).unwrap_err();
    assert_eq!(error.kind, ErrorKind::BadState);
    assert_eq!(error.message, "copy-plan: SQLite plan operation failed");
    assert_poisoned(&mut recovery, &provenance, recovery_pid, &io);
    drop(recovery);
    drop(target);
    fault.finish(&io);
    std::fs::write(&path, original).unwrap();
    open_sealed_copy_plan(&native, &intent, &io).unwrap();
    assert_eq!(counter(&provenance, &io), baseline + 1);

    let (mut fault, relay) = Fault::start("target", "recovery", "barrier_unknown", &io);
    let mut target = TargetAuthority::acquire(
        PgConnection::connect(&relay, &io).unwrap(),
        binding.stream(),
        &mut native,
        &io,
    )
    .unwrap();
    fault.arm(&native, None, &io);
    let error = target.recover_snapshot(&native, &io).unwrap_err();
    assert_eq!(
        error.message,
        "target-authority: snapshot recovery COMMIT outcome unknown"
    );
    fault.release(&io);
    drop(target);
    fault.finish(&io);
    assert_eq!(counter(&provenance, &io), baseline + 2);
    assert_data("recovery", &io, true);
    let owner = source.finish(&io).unwrap();
    assert_eq!(
        owner.receipt().intent.binding.stream(),
        intent.anchor.stream
    );
    native.record_copy_complete(&report).unwrap();
}
