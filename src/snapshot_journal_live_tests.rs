use super::SourceSnapshot;
use crate::apply::serial::{SerialApplier, SerialBudgets};
use crate::apply::{CommitOutcome, Frontier, TargetAuthority};
use crate::catalog::{VerifiedCdcSource, load_cdc_relation};
use crate::cdc::codec as format;
use crate::cdc::contracts::Event;
use crate::cdc::decode::{Decoder, DecoderPosition, RelationResolver};
use crate::contracts::{
    Conninfo, Error, ErrorKind, IoControl, QualifiedTable, Result, StateAction, StateRequest,
};
use crate::identity::{StreamId, TxSeq};
use crate::pg::{PgConnection, ReplicationPoll};
use crate::state::SourceSlotBinding;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

#[path = "../bench/storage/config.rs"]
mod config;
#[path = "../bench/storage/journal.rs"]
mod journal;
#[path = "../bench/storage/redb.rs"]
mod redb;
#[path = "../bench/storage/sqlite.rs"]
mod sqlite;

fn control() -> IoControl {
    IoControl {
        deadline: Instant::now() + Duration::from_secs(30),
        cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }
}

fn connection(variable: &str, replication: bool, io: &IoControl) -> PgConnection {
    let conninfo =
        Conninfo::new(std::env::var_os(variable).expect("owned journal fixture missing")).unwrap();
    if replication {
        PgConnection::connect_replication(&conninfo, io).unwrap()
    } else {
        PgConnection::connect(&conninfo, io).unwrap()
    }
}

fn context(variable: &str) -> serde_json::Value {
    let value: serde_json::Value = serde_json::from_reader(
        File::open(std::env::var_os(variable).expect("owned journal context missing")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(PathBuf::from(value["root"].as_str().unwrap()).join(".e1-owner"))
            .unwrap(),
        value["token"].as_str().unwrap()
    );
    value
}

fn scalar(connection: &mut PgConnection, sql: &str, io: &IoControl) -> String {
    let result = connection.query(sql, io).unwrap();
    assert_eq!(result.rows(), 1);
    String::from_utf8(result.cell(0, 0).unwrap().unwrap().to_vec()).unwrap()
}

fn rows(connection: &mut PgConnection, table: &str, io: &IoControl) -> serde_json::Value {
    serde_json::from_str(&scalar(connection, &format!(
        "SELECT coalesce(json_agg(json_build_array(id,payload) ORDER BY id),'[]'::json)::text FROM public.{table}"
    ), io)).unwrap()
}

fn write_source(writer: &mut PgConnection, table: &str, order: u64, io: &IoControl) -> u32 {
    crate::catalog::command(writer, "BEGIN", "BEGIN", io).unwrap();
    let xid = scalar(writer, "SELECT (txid_current() % 4294967296)::text", io)
        .parse()
        .unwrap();
    let sql = match order {
        1 => format!("INSERT INTO public.{table} VALUES (1,NULL),(2,''),(3,repeat('x',4096))"),
        2 => format!("UPDATE public.{table} SET id=4,payload=repeat('y',4096) WHERE id=3"),
        3 => format!("UPDATE public.{table} SET id=6,payload=repeat('z',4096) WHERE id=4"),
        _ => panic!("unexpected fixture source order"),
    };
    writer.query(&sql, io).unwrap();
    if order == 2 {
        writer
            .query(&format!("DELETE FROM public.{table} WHERE id=1"), io)
            .unwrap();
        writer
            .query(&format!("INSERT INTO public.{table} VALUES (5,NULL)"), io)
            .unwrap();
    }
    if order == 3 {
        writer
            .query(&format!("INSERT INTO public.{table} VALUES (7,'tail')"), io)
            .unwrap();
    }
    crate::catalog::command(writer, "COMMIT", "COMMIT", io).unwrap();
    xid
}

fn adapter_error() -> Error {
    Error {
        kind: ErrorKind::BadState,
        message: "journal qualification input failed".into(),
    }
}

fn storage_error(_: Error) -> config::Error {
    config::Error::Workload("journal qualification source failed")
}

struct Resolver(Event);

impl RelationResolver for Resolver {
    fn resolve(&mut self, table: &QualifiedTable) -> Result<&Event> {
        match &self.0 {
            Event::Relation { schema, name, .. }
                if schema == &table.schema && name == &table.table =>
            {
                Ok(&self.0)
            }
            _ => Err(adapter_error()),
        }
    }
}

fn configuration(path: PathBuf, binding: &SourceSlotBinding) -> config::Config {
    config::Config {
        dir: path,
        stream: binding.stream(),
        limits: format::Limits {
            max_frame_bytes: 512,
            max_event_bytes: 64 * 1024,
            max_parts: 1024,
        },
        batch_bytes: 1024 * 1024,
        cache_bytes: 1024 * 1024,
        segment_bytes: 4096,
    }
}

fn relation(
    snapshot: &mut SourceSnapshot,
    table: &str,
    binding: &SourceSlotBinding,
    io: &IoControl,
) -> Event {
    load_cdc_relation(
        &mut snapshot.source,
        &QualifiedTable {
            schema: "public".into(),
            table: table.into(),
        },
        binding.catalog_epoch(),
        io,
    )
    .unwrap()
}

fn receive_one(
    store: &mut config::Store,
    owner: &mut super::OwnedSourceSlot,
    decoder: &mut Decoder,
    resolver: &mut Resolver,
    io: &IoControl,
) -> format::CompletedTx {
    let mut external = None;
    let mut multipart = false;
    let stats = store
        .with_batch(|batch| {
            loop {
                match owner
                    .exporter
                    .poll_replication(Instant::now() + Duration::from_millis(25), io)
                    .map_err(storage_error)?
                {
                    ReplicationPoll::Frame(input) => {
                        decoder
                            .feed(&input, resolver, &mut |frame| {
                                multipart |= frame.part > 0;
                                batch.append(frame).map_err(|_| adapter_error())
                            })
                            .map_err(storage_error)?;
                        if let Some(completed) = decoder.take_completed() {
                            batch.complete(&completed)?;
                            external = Some(completed);
                            break;
                        }
                    }
                    ReplicationPoll::Wake => {}
                    ReplicationPoll::FeedbackDrained(_) | ReplicationPoll::End => {
                        return Err(config::Error::Workload(
                            "unexpected journal source terminal or feedback",
                        ));
                    }
                }
            }
            Ok(())
        })
        .unwrap();
    let external = external.expect("real source completion was not emitted");
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.encoded_bytes, external.encoded_bytes);
    assert_eq!(external.commit, None);
    assert!(external.event_count > 1);
    assert!(multipart, "fixture must exercise split event frames");
    external
}

fn scan_one(
    view: &config::ReadView<'_>,
    expected: &format::CompletedTx,
    visit: &mut dyn FnMut(&format::Record) -> Result<()>,
) -> Result<()> {
    let mut found = false;
    let mut failure = None;
    let scanned = view.scan_complete(TxSeq(expected.order.0.checked_sub(1).unwrap()), |record| {
        if found {
            return Err(config::Error::Workload(
                "unexpected transaction after expected input",
            ));
        }
        match record {
            format::Record::Frame(frame) if frame.tx != expected.tx => {
                return Err(config::Error::IdentityMismatch);
            }
            format::Record::Completed(completed) if completed != expected => {
                return Err(config::Error::Gap);
            }
            _ => {}
        }
        if let Err(error) = visit(record) {
            failure = Some(error);
            return Err(config::Error::Workload(
                "serial consumer refused journal input",
            ));
        }
        if let format::Record::Completed(_) = record {
            found = true;
        }
        Ok(())
    });
    if let Some(error) = failure {
        return Err(error);
    }
    scanned.map_err(|_| adapter_error())?;
    if !found {
        return Err(adapter_error());
    }
    Ok(())
}

fn frontier(completed: &format::CompletedTx) -> Frontier {
    Frontier {
        tx: completed.tx,
        order: completed.order,
        commit: completed.commit,
        end: completed.end,
    }
}

fn applier(authority: TargetAuthority, cfg: &config::Config) -> SerialApplier {
    SerialApplier::new(
        authority,
        cfg.limits,
        SerialBudgets {
            frame_heap_bytes: 64 * 1024,
            reassembly_heap_bytes: 128 * 1024,
            decoded_heap_bytes: 128 * 1024,
            relation_slots: 2,
            relation_bytes: 64 * 1024,
            plan_slots: 1,
            plan_bytes: 1024 * 1024,
            bind_bytes: 64 * 1024,
        },
    )
    .unwrap()
}

fn apply_one(
    applier: &mut SerialApplier,
    store: &config::Store,
    external: &format::CompletedTx,
    source: &mut VerifiedCdcSource<'_>,
    io: &IoControl,
) -> Result<CommitOutcome> {
    let mut applied = None;
    store
        .with_read_view(|view| {
            applied = Some(applier.apply_one(
                external,
                source,
                |visit| scan_one(view, external, visit),
                io,
            ));
            Ok(())
        })
        .map_err(|_| adapter_error())?;
    applied.expect("journal snapshot callback omitted")
}

fn assert_committed(outcome: CommitOutcome, external: &format::CompletedTx) {
    let CommitOutcome::Acknowledged {
        checkpoint,
        durable: Some(proof),
    } = outcome
    else {
        panic!("serial transaction lacks positive synchronous commit proof");
    };
    assert_eq!(checkpoint.applied, frontier(external));
    assert_eq!(proof.frontier(), Some(&frontier(external)));
    assert_eq!(proof.stream(), external.tx.stream);
    assert_eq!(proof.barrier_counter(), checkpoint.barrier_counter);
}

#[expect(
    clippy::too_many_arguments,
    reason = "qualification keeps endpoint and external-proof inputs explicit"
)]
fn negative_controls(
    native: &mut crate::state::NativeState,
    binding: &SourceSlotBinding,
    cfg: &config::Config,
    store: &config::Store,
    external: &format::CompletedTx,
    observer: &mut PgConnection,
    table: &str,
    io: &IoControl,
) {
    for defect in [
        "wrong_identity",
        "missing_first_frame",
        "missing_completion",
    ] {
        let mut expected = *external;
        if defect == "wrong_identity" {
            expected.tx.xid = expected.tx.xid.checked_add(1).unwrap();
        }
        let authority = TargetAuthority::acquire(
            connection("RPGMIGRATE_TEST_AUTHORITY_TARGET", false, io),
            binding.stream(),
            native,
            io,
        )
        .unwrap();
        let mut source = VerifiedCdcSource::acquire(
            connection("RPGMIGRATE_TEST_REPLICATION_SQL", false, io),
            native,
            binding,
            io,
        )
        .unwrap();
        let mut serial = applier(authority, cfg);
        let mut refusal = None;
        store
            .with_read_view(|view| {
                refusal = Some(serial.apply_one(
                    &expected,
                    &mut source,
                    |visit| {
                        let mut dropped = false;
                        let mut consumer_failure = None;
                        let scanned = view.scan_complete(TxSeq(0), |record| {
                            if defect == "missing_first_frame"
                                && !dropped
                                && matches!(record, format::Record::Frame(_))
                            {
                                dropped = true;
                                return Ok(());
                            }
                            if defect == "missing_completion"
                                && matches!(record, format::Record::Completed(_))
                            {
                                return Ok(());
                            }
                            visit(record).map_err(|error| {
                                consumer_failure = Some(error);
                                config::Error::Workload("expected malformed serial input refusal")
                            })
                        });
                        if let Some(error) = consumer_failure {
                            return Err(error);
                        }
                        scanned.map_err(|_| adapter_error())
                    },
                    io,
                ));
                Ok(())
            })
            .unwrap();
        let error = match refusal.expect("malformed-input callback omitted") {
            Err(error) => error,
            Ok(_) => panic!("serial accepted {defect}"),
        };
        let (kind, reason) = match defect {
            "wrong_identity" => (
                ErrorKind::BadState,
                "serial apply: invalid required CDC input: storage record identity does not match",
            ),
            "missing_first_frame" => (
                ErrorKind::BadState,
                "serial apply: invalid required CDC input: storage record order does not match",
            ),
            _ => (
                ErrorKind::Target,
                "serial apply: poisoned or incomplete transaction input",
            ),
        };
        assert_eq!(error.kind, kind, "{defect}: {}", error.message);
        assert_eq!(error.message, reason, "{defect}");
        drop(source);
        drop(serial);
        assert_eq!(rows(observer, table, io), serde_json::json!([]));
        let mut authority = TargetAuthority::acquire(
            connection("RPGMIGRATE_TEST_AUTHORITY_TARGET", false, io),
            binding.stream(),
            native,
            io,
        )
        .unwrap();
        assert_eq!(recover_barrier(&mut authority, io), None, "{defect}");
    }
}

fn recover_barrier(authority: &mut TargetAuthority, io: &IoControl) -> Option<Frontier> {
    let tentative = authority.checkpoint(io).unwrap();
    let proof = authority
        .barrier(
            tentative.as_ref().map(|checkpoint| &checkpoint.applied),
            1,
            io,
        )
        .unwrap();
    assert_eq!(
        proof.frontier(),
        tentative.as_ref().map(|checkpoint| &checkpoint.applied)
    );
    assert!(proof.barrier_counter() > tentative.map_or(0, |checkpoint| checkpoint.barrier_counter));
    proof.frontier().copied()
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CrashRequest {
    native: PathBuf,
    journal: PathBuf,
    proof: PathBuf,
    witness: PathBuf,
    binding: SourceSlotBinding,
    candidate: String,
}

fn candidate(name: &str) -> config::Candidate {
    match name {
        "segmented" => config::Candidate::Segmented,
        "sqlite" => config::Candidate::Sqlite,
        _ => panic!("unadmitted journal candidate"),
    }
}

fn crash_before_commit(request: CrashRequest, io: &IoControl) {
    let cfg = configuration(request.journal, &request.binding);
    let bytes = std::fs::read(&request.proof).unwrap();
    assert_eq!(bytes.len(), format::COMPLETED_BYTES);
    let external = format::decode_completed(&bytes, cfg.limits).unwrap();
    assert_eq!(external.tx.stream, request.binding.stream());
    let mut native = crate::state::open_for_provenance_recovery(&request.native).unwrap();
    native.checked_source_binding(&request.binding).unwrap();
    let authority = TargetAuthority::acquire(
        connection("RPGMIGRATE_TEST_AUTHORITY_TARGET", false, io),
        request.binding.stream(),
        &mut native,
        io,
    )
    .unwrap();
    let mut source = VerifiedCdcSource::acquire(
        connection("RPGMIGRATE_TEST_REPLICATION_SQL", false, io),
        &native,
        &request.binding,
        io,
    )
    .unwrap();
    let store = config::Store::reopen(candidate(&request.candidate), &cfg, external.order).unwrap();
    let mut serial = applier(authority, &cfg);
    store
        .with_read_view(|view| {
            let result = serial.apply_one(
                &external,
                &mut source,
                |visit| {
                    scan_one(view, &external, visit)?;
                    let temporary = request.witness.with_extension("tmp");
                    let mut witness = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(&temporary)
                        .unwrap();
                    serde_json::to_writer(
                        &mut witness,
                        &serde_json::json!({
                            "phase": "before_commit",
                            "pid": std::process::id(),
                            "binding": request.binding,
                            "proof": request.proof,
                        }),
                    )
                    .unwrap();
                    witness.sync_all().unwrap();
                    std::fs::rename(temporary, &request.witness).unwrap();
                    File::open(request.witness.parent().unwrap())
                        .unwrap()
                        .sync_all()
                        .unwrap();
                    let mut input = std::io::stdin().lock();
                    let remaining = io.deadline.saturating_duration_since(Instant::now());
                    let mut fd = libc::pollfd {
                        fd: input.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    };
                    // SAFETY: the single descriptor remains open through this bounded poll.
                    let ready = unsafe {
                        libc::poll(
                            &mut fd,
                            1,
                            i32::try_from(remaining.as_millis()).unwrap_or(i32::MAX),
                        )
                    };
                    assert!(ready > 0, "pre-COMMIT child was not killed before deadline");
                    let mut byte = [0];
                    input.read_exact(&mut byte).unwrap();
                    panic!("pre-COMMIT child must be killed, never released");
                },
                io,
            );
            assert!(result.is_err(), "pre-COMMIT child unexpectedly committed");
            Err(config::Error::Workload("pre-COMMIT child was not killed"))
        })
        .unwrap();
}

struct Process {
    child: Child,
    input: ChildStdin,
    output: ChildStdout,
}

impl Process {
    fn spawn(command: &mut Command) -> Self {
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self {
            input: child.stdin.take().unwrap(),
            output: child.stdout.take().unwrap(),
            child,
        }
    }

    fn read(&mut self, io: &IoControl) -> serde_json::Value {
        let mut bytes = Vec::new();
        loop {
            let remaining = io
                .deadline
                .checked_duration_since(Instant::now())
                .expect("journal helper deadline");
            let mut fd = libc::pollfd {
                fd: self.output.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: the one borrowed child descriptor stays open throughout this bounded poll.
            let ready = unsafe {
                libc::poll(
                    &mut fd,
                    1,
                    i32::try_from(remaining.as_millis().max(1)).unwrap_or(i32::MAX),
                )
            };
            if ready < 0
                && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted
            {
                continue;
            }
            assert!(
                ready > 0,
                "journal helper response deadline or poll failure"
            );
            let mut byte = [0];
            self.output
                .read_exact(&mut byte)
                .expect("journal helper closed without receipt");
            if byte[0] == b'\n' {
                return serde_json::from_slice(&bytes).expect("invalid journal helper receipt");
            }
            bytes.push(byte[0]);
            assert!(bytes.len() <= 64 * 1024, "oversized journal helper receipt");
        }
    }

    fn request(&mut self, operation: &str, io: &IoControl) -> serde_json::Value {
        writeln!(self.input, "{}", serde_json::json!({"op":operation})).unwrap();
        self.input.flush().unwrap();
        let response = self.read(io);
        assert_eq!(response["op"], operation);
        response
    }

    fn wait(&mut self, io: &IoControl) -> std::process::ExitStatus {
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < io.deadline,
                "journal child did not exit before deadline"
            );
            std::thread::yield_now();
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn private_file(path: &std::path::Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn kill_before_commit(
    request: &CrashRequest,
    observer: &mut PgConnection,
    table: &str,
    checkpoint: &str,
    io: &IoControl,
) {
    let started = Instant::now();
    let allowed = io.deadline.checked_duration_since(started).unwrap();
    let request_path = request.witness.with_extension("request.json");
    private_file(&request_path, &serde_json::to_vec(request).unwrap());
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "snapshot::journal_live_tests::journal_serial_crash_child",
            "--ignored",
        ])
        .env("RPGMIGRATE_TEST_JOURNAL_CRASH_REQUEST", &request_path);
    let mut child = Process::spawn(&mut command);
    loop {
        assert!(
            child.child.try_wait().unwrap().is_none(),
            "pre-COMMIT child exited without witness"
        );
        if request.witness.exists() {
            break;
        }
        assert!(Instant::now() < io.deadline, "pre-COMMIT witness deadline");
        std::thread::yield_now();
    }
    let witness: serde_json::Value =
        serde_json::from_reader(File::open(&request.witness).unwrap()).unwrap();
    assert_eq!(witness["phase"], "before_commit");
    assert_eq!(witness["pid"], child.child.id());
    assert_eq!(
        witness["binding"],
        serde_json::to_value(&request.binding).unwrap()
    );
    assert_eq!(witness["proof"], request.proof.to_str().unwrap());
    assert_eq!(rows(observer, table, io), serde_json::json!([]));
    assert_eq!(
        scalar(
            observer,
            &format!("SELECT applied_order::text FROM {checkpoint}"),
            io
        ),
        "0"
    );
    child.child.kill().unwrap();
    let status = child.wait(io);
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(libc::SIGKILL));
    assert!(child.child.try_wait().unwrap().is_some());
    let elapsed = started.elapsed();
    assert!(
        elapsed < allowed,
        "crash child exceeded the parent deadline"
    );
    let deadline_ms = u64::try_from(allowed.as_millis()).unwrap();
    let elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap();
    assert!(elapsed_ms < deadline_ms);
    let owned = context("RPGMIGRATE_TEST_CONTEXT");
    let root = PathBuf::from(owned["root"].as_str().unwrap());
    let proof = std::fs::read(&request.proof).unwrap();
    assert_eq!(proof.len(), format::COMPLETED_BYTES);
    let descriptor = format::decode_completed(
        &proof,
        configuration(request.journal.clone(), &request.binding).limits,
    )
    .unwrap();
    assert_eq!(descriptor.tx.stream, request.binding.stream());
    let receipt = root.join(format!("journal-{}-crash-receipt.json", request.candidate));
    let temporary = receipt.with_extension("tmp");
    private_file(
        &temporary,
        &serde_json::to_vec(&serde_json::json!({
            "owner": owned["token"],
            "candidate": request.candidate,
            "child_test": "snapshot::journal_live_tests::journal_serial_crash_child",
            "parent_pid": std::process::id(),
            "child_pid": child.child.id(),
            "binding": request.binding,
            "descriptor_hex": proof.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
            "witness_path": request.witness,
            "before_commit": true,
            "signal": libc::SIGKILL,
            "reaped": true,
            "deadline_ms": deadline_ms,
            "elapsed_ms": elapsed_ms,
        }))
        .unwrap(),
    );
    assert!(!receipt.exists(), "crash receipt must be new");
    std::fs::rename(temporary, &receipt).unwrap();
    File::open(root).unwrap().sync_all().unwrap();
}

#[test]
#[ignore = "requires parent-owned fresh journal child request"]
fn journal_serial_crash_child() {
    let io = control();
    let target = context("RPGMIGRATE_TEST_CONTEXT");
    let root = PathBuf::from(target["root"].as_str().unwrap());
    let path = PathBuf::from(
        std::env::var_os("RPGMIGRATE_TEST_JOURNAL_CRASH_REQUEST")
            .expect("parent-owned crash request missing"),
    );
    assert_eq!(path.parent(), Some(root.as_path()));
    let request: CrashRequest = serde_json::from_reader(File::open(path).unwrap()).unwrap();
    for path in [
        &request.native,
        &request.journal,
        &request.proof,
        &request.witness,
    ] {
        assert_eq!(path.parent(), Some(root.as_path()));
    }
    crash_before_commit(request, &io);
}

fn unknown_commit(
    name: &str,
    native: &mut crate::state::NativeState,
    binding: &SourceSlotBinding,
    cfg: &config::Config,
    store: &config::Store,
    external: &format::CompletedTx,
    io: &IoControl,
) {
    let mut command = Command::new(std::env::var_os("RPGMIGRATE_TEST_PYTHON").unwrap());
    command
        .arg(std::env::var_os("RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER").unwrap())
        .args(["--case", &format!("journal_{name}")]);
    let mut fault = Process::spawn(&mut command);
    let ready = fault.read(io);
    assert_eq!(ready["ready"], true);
    let conninfo = Conninfo::new(ready["conninfo"].as_str().unwrap().into()).unwrap();
    let authority = TargetAuthority::acquire(
        PgConnection::connect(&conninfo, io).unwrap(),
        binding.stream(),
        native,
        io,
    )
    .unwrap();
    let mut source = VerifiedCdcSource::acquire(
        connection("RPGMIGRATE_TEST_REPLICATION_SQL", false, io),
        native,
        binding,
        io,
    )
    .unwrap();
    let mut serial = applier(authority, cfg);
    let mut outcome = None;
    store
        .with_read_view(|view| {
            outcome = Some(serial.apply_one(
                external,
                &mut source,
                |visit| {
                    scan_one(view, external, visit)?;
                    fault.request("arm", io);
                    Ok(())
                },
                io,
            ));
            Ok(())
        })
        .unwrap();
    assert!(matches!(
        outcome.expect("unknown COMMIT callback omitted").unwrap(),
        CommitOutcome::Unknown
    ));
    drop(source);
    drop(serial);
    let held = fault.request("status", io);
    assert_eq!(held["fault_hits"], 1);
    assert_eq!(held["commit_tag"], "COMMIT");
    assert_eq!(held["holding_backend"], true);
    assert_eq!(held["held_fence_observed"], true);
    let mut limited = io.clone();
    limited.deadline = Instant::now() + Duration::from_millis(200);
    let blocked = match TargetAuthority::acquire(
        connection("RPGMIGRATE_TEST_AUTHORITY_TARGET", false, io),
        binding.stream(),
        native,
        &limited,
    ) {
        Err(error) => error,
        Ok(_) => panic!("fresh authority bypassed held old backend fence"),
    };
    assert_eq!(blocked.kind, ErrorKind::Pgsql);
    assert_eq!(blocked.message, "PostgreSQL operation deadline exceeded");
    let held_again = fault.request("status", io);
    assert_eq!(held_again["holding_backend"], true);
    let released = fault.request("release", io);
    assert_eq!(released["released_fence_observed"], true);
    assert_eq!(released["holding_backend"], false);
    let mut authority = TargetAuthority::acquire(
        connection("RPGMIGRATE_TEST_AUTHORITY_TARGET", false, io),
        binding.stream(),
        native,
        io,
    )
    .unwrap();
    let durable = recover_barrier(&mut authority, io);
    assert_eq!(durable, Some(frontier(external)));
    let finished = fault.request("quit", io);
    assert_eq!(finished["status"], "observed");
    assert_eq!(finished["backend_connection_closed"], true);
    assert!(fault.wait(io).success());
}

fn run(candidate_name: &str) {
    let io = control();
    let target_context = context("RPGMIGRATE_TEST_CONTEXT");
    let source_context = context("RPGMIGRATE_TEST_REPLICATION_CONTEXT");
    let root = PathBuf::from(target_context["root"].as_str().unwrap());
    let table = match candidate_name {
        "segmented" => "journal_serial_segmented",
        "sqlite" => "journal_serial_sqlite",
        _ => panic!("unadmitted journal fixture"),
    };
    let mut writer = connection("RPGMIGRATE_TEST_REPLICATION_ADMIN", false, &io);
    let mut observer = connection("RPGMIGRATE_TEST_AUTHORITY_ADMIN", false, &io);
    assert_eq!(rows(&mut writer, table, &io), serde_json::json!([]));
    assert_eq!(rows(&mut observer, table, &io), serde_json::json!([]));
    assert_eq!(
        scalar(
            &mut writer,
            "SELECT pg_catalog.shobj_description(oid,'pg_database') FROM pg_catalog.pg_database WHERE datname=current_database()",
            &io
        ),
        source_context["token"].as_str().unwrap()
    );
    assert_eq!(
        scalar(
            &mut observer,
            "SELECT pg_catalog.shobj_description(oid,'pg_database') FROM pg_catalog.pg_database WHERE datname=current_database()",
            &io
        ),
        target_context["token"].as_str().unwrap()
    );
    let mut native = crate::state::open(&StateRequest {
        path: root.join(format!("journal-{candidate_name}-native")),
        action: StateAction::Create,
    })
    .unwrap();
    let binding = SourceSlotBinding::new(
        StreamId {
            run: native.identity().run,
            source: crate::catalog::read_source_identity(&mut writer, &io)
                .unwrap()
                .0,
            generation: 9,
        },
        format!(
            "js_{}_{}",
            &source_context["token"].as_str().unwrap()[..10],
            candidate_name
        ),
        crate::cdc::contracts::Plugin::TestDecoding,
        7,
    )
    .unwrap();
    let mut snapshot = SourceSnapshot::create(
        connection("RPGMIGRATE_TEST_REPLICATION_SQL", false, &io),
        connection("RPGMIGRATE_TEST_REPLICATION_SOURCE", true, &io),
        binding.clone(),
        &mut native,
        &io,
    )
    .unwrap();
    let mut resolver = Resolver(relation(&mut snapshot, table, &binding, &io));
    let mut owner = snapshot.finish(&io).unwrap();
    owner
        .exporter
        .start_replication(
            binding.slot_name(),
            owner.receipt().consistent_point.0,
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
    let cfg = configuration(
        root.join(format!("journal-{candidate_name}-records")),
        &binding,
    );
    let mut store = config::Store::create(candidate(candidate_name), &cfg).unwrap();
    let mut decoder = Decoder::new(
        binding.plugin(),
        binding.stream(),
        cfg.limits,
        DecoderPosition {
            next_begin_ordinal: 1,
            next_order: TxSeq(1),
        },
    )
    .unwrap();
    let xid = write_source(&mut writer, table, 1, &io);
    let first = receive_one(&mut store, &mut owner, &mut decoder, &mut resolver, &io);
    assert_eq!(first.tx.xid, xid);
    assert_eq!(first.order, TxSeq(1));
    store.checkpoint().unwrap();
    drop(store);
    let store = config::Store::reopen(candidate(candidate_name), &cfg, first.order).unwrap();
    negative_controls(
        &mut native,
        &binding,
        &cfg,
        &store,
        &first,
        &mut observer,
        table,
        &io,
    );
    let confirmed = match native.target_provenance(&binding.stream()).unwrap() {
        crate::state::TargetProvenance::Confirmed(confirmed) => confirmed,
        _ => panic!("target metadata lacks confirmed provenance"),
    };
    let checkpoint_table = crate::catalog::qualified(
        &mut observer,
        &confirmed.intent.binding.schema_name,
        &confirmed.intent.binding.table_name,
    )
    .unwrap();
    let request = CrashRequest {
        native: native.identity().canonical_path.clone(),
        journal: cfg.dir.clone(),
        proof: root.join(format!("journal-{candidate_name}-external.bin")),
        witness: root.join(format!("journal-{candidate_name}-kill.json")),
        binding: binding.clone(),
        candidate: candidate_name.into(),
    };
    private_file(
        &request.proof,
        &format::encode_completed(&first, cfg.limits).unwrap(),
    );
    drop(store);
    drop(native);
    kill_before_commit(&request, &mut observer, table, &checkpoint_table, &io);
    let mut native = crate::state::open_for_provenance_recovery(&request.native).unwrap();
    let mut authority = TargetAuthority::acquire(
        connection("RPGMIGRATE_TEST_AUTHORITY_TARGET", false, &io),
        binding.stream(),
        &mut native,
        &io,
    )
    .unwrap();
    assert_eq!(recover_barrier(&mut authority, &io), None);
    assert_eq!(rows(&mut observer, table, &io), serde_json::json!([]));
    let mut source = VerifiedCdcSource::acquire(
        connection("RPGMIGRATE_TEST_REPLICATION_SQL", false, &io),
        &native,
        &binding,
        &io,
    )
    .unwrap();
    let mut serial = applier(authority, &cfg);
    let mut store = config::Store::reopen(candidate(candidate_name), &cfg, first.order).unwrap();
    assert_committed(
        apply_one(&mut serial, &store, &first, &mut source, &io).unwrap(),
        &first,
    );
    assert_eq!(
        rows(&mut observer, table, &io),
        serde_json::json!([[1, null], [2, ""], [3, "x".repeat(4096)]])
    );
    drop(source);
    drop(serial);

    let xid = write_source(&mut writer, table, 2, &io);
    let second = receive_one(&mut store, &mut owner, &mut decoder, &mut resolver, &io);
    assert_eq!(second.tx.xid, xid);
    assert_eq!(second.order, TxSeq(2));
    store.checkpoint().unwrap();
    drop(store);
    let store = config::Store::reopen(candidate(candidate_name), &cfg, second.order).unwrap();
    unknown_commit(
        candidate_name,
        &mut native,
        &binding,
        &cfg,
        &store,
        &second,
        &io,
    );
    assert_eq!(
        rows(&mut observer, table, &io),
        serde_json::json!([[2, ""], [4, "y".repeat(4096)], [5, null]])
    );
    drop(store);

    let mut store = config::Store::reopen(candidate(candidate_name), &cfg, second.order).unwrap();
    let xid = write_source(&mut writer, table, 3, &io);
    let third = receive_one(&mut store, &mut owner, &mut decoder, &mut resolver, &io);
    assert_eq!(third.tx.xid, xid);
    assert_eq!(third.order, TxSeq(3));
    store.checkpoint().unwrap();
    drop(store);
    let store = config::Store::reopen(candidate(candidate_name), &cfg, third.order).unwrap();
    let authority = TargetAuthority::acquire(
        connection("RPGMIGRATE_TEST_AUTHORITY_TARGET", false, &io),
        binding.stream(),
        &mut native,
        &io,
    )
    .unwrap();
    let mut source = VerifiedCdcSource::acquire(
        connection("RPGMIGRATE_TEST_REPLICATION_SQL", false, &io),
        &native,
        &binding,
        &io,
    )
    .unwrap();
    let mut serial = applier(authority, &cfg);
    assert_committed(
        apply_one(&mut serial, &store, &third, &mut source, &io).unwrap(),
        &third,
    );
    let expected = serde_json::json!([[2, ""], [5, null], [6, "z".repeat(4096)], [7, "tail"]]);
    assert_eq!(rows(&mut observer, table, &io), expected);
    assert_eq!(rows(&mut writer, table, &io), expected);
    let external = [first, second, third];
    let mut completed = 0;
    store
        .with_read_view(|view| {
            view.scan_complete(TxSeq(0), |record| {
                match record {
                    format::Record::Frame(frame) => assert_eq!(frame.tx, external[completed].tx),
                    format::Record::Completed(tx) => {
                        assert_eq!(tx, &external[completed]);
                        completed += 1;
                    }
                }
                Ok(())
            })
        })
        .unwrap();
    assert_eq!(completed, external.len());
}

#[test]
#[ignore = "requires owned fresh Engine source and target journal fixture"]
fn segmented_real_wal_reopens_and_serial_replays() {
    run("segmented");
}

#[test]
#[ignore = "requires owned fresh Engine source and target journal fixture"]
fn sqlite_real_wal_reopens_and_serial_replays() {
    run("sqlite");
}
