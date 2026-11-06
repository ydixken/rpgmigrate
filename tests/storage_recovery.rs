#[path = "../bench/storage.rs"]
pub mod storage;

#[test]
fn driver_manifest_requires_all_candidates_and_nonempty_bounded_cases() {
    let valid = storage::parse_manifest(storage::CASES).unwrap();
    assert_eq!(valid.candidates, ["segmented", "redb", "sqlite"]);
    assert_eq!(valid.cases.len(), 3);
    for (field, value) in [
        ("candidates", serde_json::json!([])),
        ("candidates", serde_json::json!(["sqlite", "redb"])),
        (
            "candidates",
            serde_json::json!(["sqlite", "sqlite", "redb"]),
        ),
        ("cases", serde_json::json!([])),
        ("repetitions", serde_json::json!(0)),
        ("repetitions", serde_json::json!(11)),
        ("version", serde_json::json!(2)),
    ] {
        let mut broken: serde_json::Value = serde_json::from_str(storage::CASES).unwrap();
        broken[field] = value;
        assert!(
            storage::parse_manifest(&broken.to_string()).is_err(),
            "{field}"
        );
    }
    for field in ["transactions", "events_per_transaction", "event_bytes"] {
        for value in [0, u64::MAX] {
            let mut broken: serde_json::Value = serde_json::from_str(storage::CASES).unwrap();
            broken["cases"][0][field] = value.into();
            assert!(
                storage::parse_manifest(&broken.to_string()).is_err(),
                "{field}"
            );
        }
    }
    for index in 0..3 {
        let mut broken: serde_json::Value = serde_json::from_str(storage::CASES).unwrap();
        broken["cases"].as_array_mut().unwrap().remove(index);
        assert!(storage::parse_manifest(&broken.to_string()).is_err());
    }
}

#[test]
fn driver_streams_equal_batches_and_verifies_external_input_for_every_candidate() {
    let case = storage::Case {
        name: "fixture".into(),
        transactions: 2,
        events_per_transaction: 3,
        event_bytes: 90,
    };
    let root = Scratch::new();
    let mut evidence = Vec::new();
    for (index, candidate) in [Candidate::Segmented, Candidate::Redb, Candidate::Sqlite]
        .into_iter()
        .enumerate()
    {
        let mut cfg = root.config();
        cfg.dir = root.0.join(format!("candidate-{index}"));
        cfg.limits.max_frame_bytes = 128;
        cfg.batch_bytes = 256;
        cfg.segment_bytes = 512;
        let expected = storage::Counts {
            frames: 18,
            completed: 2,
            encoded_bytes: 2380,
        };
        assert_eq!(case.counts(cfg.limits).unwrap(), expected);
        let mut store = Store::create(candidate, &cfg).unwrap();
        let written = storage::write_case(&mut store, &cfg, &case, 42).unwrap();
        assert_eq!(written.counts, expected);
        assert_eq!(written.durable_frontier, 2);
        assert!(written.batches > 2);
        store.checkpoint().unwrap();
        drop(store);
        assert!(matches!(
            Store::reopen(candidate, &cfg, TxSeq(3)),
            Err(Error::Gap)
        ));
        let mut reopened = Store::reopen(candidate, &cfg, TxSeq(2)).unwrap();
        let checksum = storage::verify_case(&reopened, &cfg, &case, 42).unwrap();
        assert!(storage::verify_case(&reopened, &cfg, &case, 43).is_err());
        let mut missing = case.clone();
        missing.transactions = 3;
        assert!(storage::verify_case(&reopened, &cfg, &missing, 42).is_err());
        reopened.checkpoint().unwrap();
        evidence.push((written.batches, written.batch_crc32, checksum));
    }
    assert_eq!(evidence.len(), 3);
    assert!(evidence.windows(2).all(|pair| pair[0] == pair[1]));
}

#[test]
fn driver_child_exit_and_timeout_are_errors_and_success_is_reaped() {
    use std::{
        process::Command,
        time::{Duration, Instant},
    };
    let mut success = Command::new("/bin/sh")
        .args(["-c", "exit 0"])
        .spawn()
        .unwrap();
    storage::wait_child(&mut success, Duration::from_secs(5)).unwrap();
    assert!(success.try_wait().unwrap().unwrap().success());
    let mut failure = Command::new("/bin/sh")
        .args(["-c", "exit 7"])
        .spawn()
        .unwrap();
    assert!(storage::wait_child(&mut failure, Duration::from_secs(5)).is_err());
    assert_eq!(failure.try_wait().unwrap().unwrap().code(), Some(7));
    let mut stalled = Command::new("/bin/sleep").arg("30").spawn().unwrap();
    let start = Instant::now();
    assert!(storage::wait_child(&mut stalled, Duration::from_millis(20)).is_err());
    assert!(start.elapsed() < Duration::from_secs(5));
    assert!(!stalled.try_wait().unwrap().unwrap().success());
}

#[test]
fn driver_measurement_includes_checkpoint_reopen_scan_and_actual_settings() {
    let root = Scratch::new();
    let cfg = root.config();
    let case = storage::Case {
        name: "fixture".into(),
        transactions: 2,
        events_per_transaction: 2,
        event_bytes: 40,
    };
    let report = storage::measure_case(Candidate::Sqlite, &cfg, &case, 42, 0).unwrap();
    assert_eq!(report.required_prefix, 2);
    assert_eq!(report.writes.counts, case.counts(cfg.limits).unwrap());
    assert_eq!(report.writes.durable_frontier, 2);
    assert!(
        report.timings.write_ns > 0
            && report.timings.checkpoint_ns > 0
            && report.timings.reopen_ns > 0
            && report.timings.verified_scan_ns > 0
            && report.timings.finalize_ns > 0
    );
    let phase_sum = report.timings.create_ns
        + report.timings.write_ns
        + report.timings.checkpoint_ns
        + report.timings.close_ns
        + report.timings.reopen_ns
        + report.timings.verified_scan_ns
        + report.timings.finalize_ns;
    assert!(report.timings.total_ns >= phase_sum);
    assert_eq!(report.settings["sqlite_version"], "3.54.1");
    assert_eq!(report.settings["cache_budget_is_rss_limit"], false);
    assert_eq!(report.configured_cache_bytes, cfg.cache_bytes);
    assert!(report.peak_rss_bytes > 0);
}

#[test]
fn driver_schedule_pairs_every_case_candidate_and_repetition_deterministically() {
    let mut manifest = storage::parse_manifest(storage::CASES).unwrap();
    manifest.repetitions = 3;
    let jobs = storage::schedule(&manifest);
    assert_eq!(jobs.len(), 27);
    assert_eq!(jobs, storage::schedule(&manifest));
    let unique: std::collections::BTreeSet<_> = jobs
        .iter()
        .map(|job| (job.case, job.candidate, job.repetition))
        .collect();
    assert_eq!(unique.len(), 27);
    for group in jobs.as_chunks::<3>().0 {
        assert!(
            group
                .iter()
                .all(|job| job.case == group[0].case && job.repetition == group[0].repetition)
        );
        assert_eq!(
            group
                .iter()
                .map(|job| job.candidate)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            3
        );
    }
    manifest.seed += 1;
    assert_ne!(jobs, storage::schedule(&manifest));
}

#[test]
fn driver_rejects_wrong_receipts_and_sqlite_build_overrides() {
    let root = Scratch::new();
    let cfg = root.config();
    let case = storage::Case {
        name: "small-commits".into(),
        transactions: 2,
        events_per_transaction: 1,
        event_bytes: 1,
    };
    let report = storage::measure_case(Candidate::Sqlite, &cfg, &case, 42, 0).unwrap();
    let mut manifest = storage::parse_manifest(storage::CASES).unwrap();
    manifest.seed = 42;
    manifest.cases[0] = case;
    let job = storage::Job {
        case: 0,
        candidate: 2,
        repetition: 0,
    };
    storage::validate_report(&report, &manifest, job, std::process::id()).unwrap();
    for mutation in 0..8 {
        let mut wrong = report.clone();
        match mutation {
            0 => wrong.writes.counts.encoded_bytes -= 1,
            1 => wrong.required_prefix = 1,
            2 => wrong.pid += 1,
            3 => wrong.writes.durable_frontier = 1,
            4 => wrong.settings = serde_json::Value::Null,
            5 => wrong.timings.total_ns = 0,
            6 => wrong.case = "omitted".into(),
            7 => wrong.writes.batches = 0,
            _ => unreachable!(),
        }
        assert!(
            storage::validate_report(&wrong, &manifest, job, std::process::id()).is_err(),
            "{mutation}"
        );
    }
    assert!(storage::sqlite_build_check(&[("LIBSQLITE3_SYS_USE_PKG_CONFIG", true)]).is_err());
    assert!(storage::sqlite_build_check(&[("LIBSQLITE3_FLAGS", true)]).is_err());
    assert!(storage::sqlite_build_check(&[("LIBSQLITE3_FLAGS", false)]).is_ok());
}

#[test]
fn driver_rejects_missing_duplicate_and_mismatched_case_receipts() {
    let root = Scratch::new();
    let case = storage::Case {
        name: "fixture".into(),
        transactions: 1,
        events_per_transaction: 1,
        event_bytes: 1,
    };
    let prototype = storage::measure_case(Candidate::Sqlite, &root.config(), &case, 42, 0).unwrap();
    let manifest = storage::parse_manifest(storage::CASES).unwrap();
    let reports: Vec<_> = storage::schedule(&manifest)
        .into_iter()
        .map(|job| {
            let mut report = prototype.clone();
            report.case = manifest.cases[job.case].name.clone();
            report.candidate = manifest.candidates[job.candidate].clone();
            report.repetition = job.repetition;
            report.seed = manifest.seed;
            report.required_prefix = manifest.cases[job.case].transactions;
            report.writes.counts = manifest.cases[job.case].counts(Limits::default()).unwrap();
            report.writes.durable_frontier = report.required_prefix;
            report
        })
        .collect();
    storage::validate_reports(&manifest, &reports).unwrap();
    assert!(storage::validate_reports(&manifest, &[]).is_err());
    for index in 0..reports.len() {
        let mut missing = reports.clone();
        missing.remove(index);
        assert!(storage::validate_reports(&manifest, &missing).is_err());
    }
    let mut duplicate = reports.clone();
    duplicate[1] = duplicate[0].clone();
    assert!(storage::validate_reports(&manifest, &duplicate).is_err());
    let mut different = reports.clone();
    different[0].writes.batch_crc32 ^= 1;
    assert!(storage::validate_reports(&manifest, &different).is_err());
    different = reports;
    different[0].input_crc32 ^= 1;
    assert!(storage::validate_reports(&manifest, &different).is_err());
}

#[test]
fn driver_report_reader_rejects_missing_malformed_oversized_and_symlink_files() {
    use std::os::unix::fs::symlink;
    let root = Scratch::new();
    let path = root.0.join("receipt.json");
    assert!(storage::read_report(&path).is_err());
    std::fs::write(&path, b"{}").unwrap();
    assert!(storage::read_report(&path).is_err());
    std::fs::write(&path, vec![b' '; 256 * 1024 + 1]).unwrap();
    assert!(storage::read_report(&path).is_err());
    let link = root.0.join("link.json");
    symlink(&path, &link).unwrap();
    assert!(storage::read_report(&link).is_err());
}

#[test]
fn driver_invalid_routes_and_existing_root_fail_before_candidate_creation() {
    use std::ffi::OsString;
    let root = Scratch::new();
    assert!(storage::run(&[]).is_err());
    assert!(storage::run(&[OsString::from("--candidate"), OsString::from("sqlite")]).is_err());
    assert!(storage::run(&[OsString::from("--root"), root.0.as_os_str().to_owned()]).is_err());
    assert_eq!(std::fs::read_dir(&root.0).unwrap().count(), 0);
}

use std::{io, path::PathBuf};
use storage::config::*;
use storage::format::{
    self, CommitLsn, CompletedTx, Frame, Limits, Record, RunId, SourceEndLsn, SourceId, StreamId,
    TxId, TxSeq,
};

fn config() -> Config {
    Config {
        dir: PathBuf::from("/tmp/owned-storage-experiment"),
        stream: StreamId {
            run: RunId([0x11; 16]),
            source: SourceId {
                system_identifier: 0x0102,
                database_oid: 42,
            },
            generation: 7,
        },
        limits: Limits::default(),
        batch_bytes: 8 * 1024 * 1024,
        cache_bytes: 64 * 1024 * 1024,
        segment_bytes: 64 * 1024 * 1024,
    }
}

#[test]
fn finite_config_accepts_a_frame_and_descriptor_budget() {
    assert_eq!(config().validate(), Ok(()));
}

#[test]
fn invalid_config_fails_without_filesystem_side_effects() {
    let mut cfg = config();
    cfg.dir = PathBuf::new();
    assert_eq!(
        cfg.validate(),
        Err(Error::Config("storage directory must name a path"))
    );
    cfg = config();
    cfg.cache_bytes = 0;
    assert_eq!(
        cfg.validate(),
        Err(Error::Config("storage cache budget must be positive"))
    );
    cfg = config();
    cfg.batch_bytes = 127;
    assert_eq!(
        cfg.validate(),
        Err(Error::Config(
            "storage batch cannot hold the largest record"
        ))
    );
    cfg = config();
    cfg.segment_bytes = 127;
    assert_eq!(
        cfg.validate(),
        Err(Error::Config(
            "storage segment cannot hold the largest record"
        ))
    );
    cfg = config();
    cfg.limits.max_parts = 0;
    assert_eq!(
        cfg.validate(),
        Err(Error::Format(format::Error::InvalidLimits))
    );
}

#[test]
fn store_identity_matches_independent_fixed_width_layout() {
    let cfg = config();
    let identity = cfg.identity();
    assert_eq!(identity.len(), 48);
    assert_eq!(&identity[..12], b"RPGEXP01\x01\x00\x00\x00");
    assert_eq!(&identity[12..28], &[0x11; 16]);
    assert_eq!(&identity[28..36], &[2, 1, 0, 0, 0, 0, 0, 0]);
    assert_eq!(&identity[36..40], &[42, 0, 0, 0]);
    assert_eq!(&identity[40..], &[7, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn missing_store_is_noncreating_for_each_candidate() {
    let root = Scratch::new();
    for candidate in [Candidate::Segmented, Candidate::Redb, Candidate::Sqlite] {
        assert!(matches!(
            Store::reopen(candidate, &root.config(), TxSeq(0)),
            Err(Error::Io(io::ErrorKind::NotFound))
        ));
        assert!(!root.config().dir.exists());
    }
}

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "rpgmigrate-contract-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn config(&self) -> Config {
        Config {
            dir: self.0.join("store"),
            ..config()
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn native_identity_exists_before_candidate_database_creation() {
    use std::os::unix::fs::PermissionsExt;
    let root = Scratch::new();
    let cfg = root.config();
    cfg.create_directory().unwrap();
    assert_eq!(
        std::fs::read(cfg.dir.join("identity")).unwrap(),
        cfg.identity()
    );
    assert_eq!(
        std::fs::metadata(&cfg.dir).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(cfg.dir.join("identity"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    cfg.verify_directory().unwrap();
    assert_eq!(std::fs::read_dir(&cfg.dir).unwrap().count(), 1);
}

#[test]
fn missing_or_foreign_identity_is_read_only_refusal() {
    let root = Scratch::new();
    let cfg = root.config();
    assert_eq!(
        cfg.verify_directory(),
        Err(Error::Io(io::ErrorKind::NotFound))
    );
    assert!(!cfg.dir.exists());
    cfg.create_directory().unwrap();
    let mut foreign = cfg.clone();
    foreign.stream.generation += 1;
    assert_eq!(foreign.verify_directory(), Err(Error::IdentityMismatch));
    assert_eq!(
        std::fs::read(cfg.dir.join("identity")).unwrap(),
        cfg.identity()
    );
    assert_eq!(
        cfg.create_directory(),
        Err(Error::Io(io::ErrorKind::AlreadyExists))
    );
}

#[test]
fn malformed_identity_is_never_repaired_or_followed() {
    let root = Scratch::new();
    let cfg = root.config();
    cfg.create_directory().unwrap();
    let path = cfg.dir.join("identity");
    std::fs::write(&path, b"foreign").unwrap();
    assert_eq!(cfg.verify_directory(), Err(Error::IdentityMismatch));
    assert_eq!(std::fs::read(&path).unwrap(), b"foreign");
    std::fs::remove_file(&path).unwrap();
    let outside = root.0.join("foreign");
    std::fs::write(&outside, cfg.identity()).unwrap();
    std::os::unix::fs::symlink(&outside, &path).unwrap();
    assert_eq!(cfg.verify_directory(), Err(Error::IdentityMismatch));
    assert_eq!(std::fs::read(&outside).unwrap(), cfg.identity());
}

#[test]
fn directory_creation_reports_a_failed_parent_sync() {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let root = Scratch::new();
    assert_ne!(
        std::fs::metadata(&root.0).unwrap().uid(),
        0,
        "private fixture permission tests require an unprivileged user"
    );
    std::fs::set_permissions(&root.0, std::fs::Permissions::from_mode(0o300)).unwrap();
    let outcome = root.config().create_directory();
    std::fs::set_permissions(&root.0, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(outcome, Err(Error::Io(io::ErrorKind::PermissionDenied)));
}

fn frame(sequence: u64, ordinal: u64) -> Frame {
    Frame {
        tx: TxId {
            stream: config().stream,
            xid: sequence as u32,
            begin_ordinal: sequence,
        },
        event_ordinal: ordinal,
        part: 0,
        last_part: true,
        payload: vec![42],
    }
}

fn completed(sequence: u64, events: u64) -> CompletedTx {
    CompletedTx {
        tx: frame(sequence, 0).tx,
        order: TxSeq(sequence),
        commit: Some(CommitLsn(10)),
        end: SourceEndLsn(11),
        event_count: events,
        encoded_bytes: events * 89 + 128,
    }
}

#[test]
fn external_required_prefix_cannot_be_inferred_from_surviving_frames() {
    let mut state = Progress::new(&config()).unwrap();
    state.observe(&Record::Frame(frame(1, 0))).unwrap();
    assert_eq!(state.frontier(), TxSeq(0));
    assert_eq!(state.require_prefix(TxSeq(1)), Err(Error::Gap));
    state.observe(&Record::Completed(completed(1, 1))).unwrap();
    assert_eq!(state.require_prefix(TxSeq(1)), Ok(()));
    assert_eq!(state.require_prefix(TxSeq(2)), Err(Error::Gap));
}

#[test]
fn required_sequences_cannot_skip_and_scan_cannot_start_past_frontier() {
    let mut state = Progress::new(&config()).unwrap();
    assert_eq!(state.check_after(TxSeq(1)), Err(Error::Gap));
    state.observe(&Record::Frame(frame(1, 0))).unwrap();
    assert!(state.observe(&Record::Completed(completed(2, 1))).is_err());
    assert_eq!(state.frontier(), TxSeq(0));
    assert_eq!(
        state.observe(&Record::Completed(completed(1, 1))),
        Err(Error::Poisoned)
    );
}

#[test]
fn giant_transaction_retains_one_scalar_validator_until_completion() {
    let mut state = Progress::new(&config()).unwrap();
    let mut input = frame(1, 0);
    for ordinal in 0..100_000 {
        input.event_ordinal = ordinal;
        state.observe(&Record::Frame(input.clone())).unwrap();
        assert_eq!(state.active_transactions(), 1);
        assert_eq!(state.frontier(), TxSeq(0));
    }
    state
        .observe(&Record::Completed(completed(1, 100_000)))
        .unwrap();
    assert_eq!(state.active_transactions(), 0);
    assert_eq!(state.frontier(), TxSeq(1));
    assert!(std::mem::size_of::<Progress>() <= 512);
}

#[test]
fn first_slice_refuses_interleaved_source_transactions() {
    let mut state = Progress::new(&config()).unwrap();
    state.observe(&Record::Frame(frame(1, 0))).unwrap();
    assert!(state.observe(&Record::Frame(frame(2, 0))).is_err());
    assert_eq!(state.active_transactions(), 1);
}

#[test]
fn repeated_lsns_are_valid_but_transaction_identity_cannot_repeat() {
    let mut state = Progress::new(&config()).unwrap();
    for sequence in 1..=2 {
        state.observe(&Record::Frame(frame(sequence, 0))).unwrap();
        state
            .observe(&Record::Completed(completed(sequence, 1)))
            .unwrap();
    }
    assert_eq!(state.frontier(), TxSeq(2));
    assert_eq!(state.check_after(TxSeq(2)), Ok(()));
    assert!(state.observe(&Record::Frame(frame(1, 0))).is_err());
}

#[test]
fn real_candidates_publish_only_complete_source_transactions() {
    for candidate in [Candidate::Segmented, Candidate::Redb, Candidate::Sqlite] {
        let root = Scratch::new();
        let cfg = root.config();
        let mut store = Store::create(candidate, &cfg).unwrap();
        let first = store
            .with_batch(|batch| batch.append(&frame(1, 0)))
            .unwrap();
        assert_eq!(
            first,
            BatchStats {
                frames: 1,
                completed: 0,
                encoded_bytes: 89,
                durable_frontier: TxSeq(0)
            }
        );
        let mut seen = 0;
        store
            .scan_complete(TxSeq(0), |_| {
                seen += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 0);
        let last = store
            .with_batch(|batch| batch.complete(&completed(1, 1)))
            .unwrap();
        assert_eq!(
            last,
            BatchStats {
                frames: 0,
                completed: 1,
                encoded_bytes: 128,
                durable_frontier: TxSeq(1)
            }
        );
        store.checkpoint().unwrap();
        drop(store);
        assert!(matches!(
            Store::reopen(candidate, &cfg, TxSeq(2)),
            Err(Error::Gap)
        ));
        let store = Store::reopen(candidate, &cfg, TxSeq(1)).unwrap();
        assert_eq!(store.scan_complete(TxSeq(2), |_| Ok(())), Err(Error::Gap));
        let mut records = Vec::new();
        store
            .scan_complete(TxSeq(0), |record| {
                records.push(record.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(
            records,
            vec![
                Record::Frame(frame(1, 0)),
                Record::Completed(completed(1, 1))
            ]
        );
    }
}

#[test]
fn real_reopen_requires_external_descriptor_evidence() {
    for candidate in [Candidate::Segmented, Candidate::Redb, Candidate::Sqlite] {
        let root = Scratch::new();
        let cfg = root.config();
        let mut store = Store::create(candidate, &cfg).unwrap();
        store
            .with_batch(|batch| batch.append(&frame(1, 0)))
            .unwrap();
        store.checkpoint().unwrap();
        drop(store);
        assert!(matches!(
            Store::reopen(candidate, &cfg, TxSeq(1)),
            Err(Error::Gap)
        ));
        let store = Store::reopen(candidate, &cfg, TxSeq(0)).unwrap();
        let mut seen = 0;
        store
            .scan_complete(TxSeq(0), |_| {
                seen += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 0);
    }
}

#[test]
fn swallowed_common_error_aborts_every_candidates_batch() {
    for candidate in [Candidate::Segmented, Candidate::Redb, Candidate::Sqlite] {
        let root = Scratch::new();
        let cfg = root.config();
        let mut store = Store::create(candidate, &cfg).unwrap();
        let result = store.with_batch(|batch| {
            batch.append(&frame(1, 0))?;
            assert!(batch.append(&frame(1, 9)).is_err());
            Ok(())
        });
        assert_eq!(result, Err(Error::Poisoned));
        assert_eq!(store.with_batch(|_| Ok(())), Err(Error::Poisoned));
        drop(store);
        let reopened = Store::reopen(candidate, &cfg, TxSeq(0)).unwrap();
        let mut seen = 0;
        reopened
            .scan_complete(TxSeq(0), |_| {
                seen += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 0);
    }
}

#[test]
fn descriptor_bytes_count_toward_the_batch_cap() {
    for candidate in [Candidate::Segmented, Candidate::Redb, Candidate::Sqlite] {
        let root = Scratch::new();
        let mut cfg = root.config();
        cfg.limits.max_frame_bytes = 128;
        cfg.batch_bytes = 216;
        let mut store = Store::create(candidate, &cfg).unwrap();
        let result = store.with_batch(|batch| {
            batch.append(&frame(1, 0))?;
            assert!(batch.complete(&completed(1, 1)).is_err());
            Ok(())
        });
        assert_eq!(result, Err(Error::Poisoned));
    }
}

#[test]
fn scan_validates_later_records_before_any_transaction_callback() {
    for fault in [
        "UPDATE records SET record = zeroblob(1) WHERE ordinal = 2",
        "UPDATE records SET record = zeroblob(1) WHERE ordinal = 3",
        "DELETE FROM records WHERE ordinal = 3",
    ] {
        let root = Scratch::new();
        let cfg = root.config();
        let mut store = Store::create(Candidate::Sqlite, &cfg).unwrap();
        store
            .with_batch(|batch| {
                batch.append(&frame(1, 0))?;
                batch.append(&frame(1, 1))?;
                batch.complete(&completed(1, 2))
            })
            .unwrap();
        let fixture = rusqlite::Connection::open(cfg.dir.join("records.sqlite3")).unwrap();
        assert_eq!(fixture.execute(fault, []).unwrap(), 1);
        drop(fixture);
        let mut callbacks = 0;
        assert!(
            store
                .scan_complete(TxSeq(0), |_| {
                    callbacks += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(
            callbacks, 0,
            "invalid transaction reached the callback: {fault}"
        );
    }
}

#[test]
fn scan_filters_after_prefix_and_never_exposes_incomplete_tail() {
    for candidate in [Candidate::Segmented, Candidate::Redb, Candidate::Sqlite] {
        let root = Scratch::new();
        let mut store = Store::create(candidate, &root.config()).unwrap();
        store
            .with_batch(|batch| {
                for seq in 1..=2 {
                    batch.append(&frame(seq, 0))?;
                    batch.complete(&completed(seq, 1))?;
                }
                batch.append(&frame(3, 0))
            })
            .unwrap();
        let mut records = Vec::new();
        store
            .scan_complete(TxSeq(1), |record| {
                records.push(record.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(
            records,
            vec![
                Record::Frame(frame(2, 0)),
                Record::Completed(completed(2, 1))
            ]
        );
        let mut calls = 0;
        assert_eq!(
            store.scan_complete(TxSeq(3), |_| {
                calls += 1;
                Ok(())
            }),
            Err(Error::Gap)
        );
        assert_eq!(calls, 0);
    }
}

#[test]
fn optional_physical_commit_evidence_survives_each_candidate_reader_and_reopen() {
    for candidate in [Candidate::Segmented, Candidate::Redb, Candidate::Sqlite] {
        let root = Scratch::new();
        let cfg = root.config();
        let mut store = Store::create(candidate, &cfg).unwrap();
        let mut expected = Vec::new();
        for (index, commit) in [Some(CommitLsn(10)), None, Some(CommitLsn(0))]
            .into_iter()
            .enumerate()
        {
            let seq = index as u64 + 1;
            let descriptor = CompletedTx {
                commit,
                ..completed(seq, 1)
            };
            let item = frame(seq, 0);
            let stats = store
                .with_batch(|batch| {
                    batch.append(&item)?;
                    batch.complete(&descriptor)
                })
                .unwrap();
            assert_eq!(stats.encoded_bytes, 217);
            assert_eq!(stats.durable_frontier, TxSeq(seq));
            expected.extend([Record::Frame(item), Record::Completed(descriptor)]);
        }
        let mut actual = Vec::new();
        store
            .scan_complete(TxSeq(0), |record| {
                actual.push(record.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected);
        store.checkpoint().unwrap();
        drop(store);
        let reopened = Store::reopen(candidate, &cfg, TxSeq(3)).unwrap();
        actual.clear();
        reopened
            .scan_complete(TxSeq(0), |record| {
                actual.push(record.clone());
                Ok(())
            })
            .unwrap();
        assert_eq!(actual, expected);
    }
}
