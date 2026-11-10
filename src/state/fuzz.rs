use super::*;
use crate::contracts::{FollowBinding, PublicationReceipt};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::os::unix::fs::PermissionsExt;

const SEED: u64 = 0x9e37_79b9_7f4a_7c15;
const CANARY: &str = "m10_synthetic_canary";
const MAX_CASE_BYTES: usize = 64 * 1024;

fn next(seed: &mut u64) -> u64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    *seed
}

struct Corpus {
    native: NativeState,
    seeds: Vec<Value>,
    count: usize,
    _temp: tests::TempDir,
}

impl Corpus {
    fn new() -> Self {
        let temp = tests::TempDir::new();
        let native = temp.create();
        let mut seeds = vec![serde_json::to_value(&native.manifest).unwrap()];
        let follow_temp = tests::TempDir::new();
        let io = IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )));
        let mut follow = open_follow(
            &StateRequest {
                path: follow_temp.path(),
                action: StateAction::Create,
            },
            None,
            None,
            &io,
        )
        .unwrap();
        seeds.push(serde_json::to_value(&follow.manifest).unwrap());
        follow
            .resolve_follow_settings(None, None, FollowLimits::default())
            .unwrap();
        seeds.push(serde_json::to_value(&follow.manifest).unwrap());
        let plan: ClonePlan = serde_json::from_value(json!({
            "source": {"identity": {"system_identifier": 2, "database_oid": 3}, "timeline": 1, "major": 16},
            "target": {"identity": {"system_identifier": 4, "database_oid": 5}, "major": 18},
            "generation": 1,
            "tables": [{"source_oid": 6, "name": {"schema": "public", "table": "synthetic"},
                "writable_columns": ["id"], "all_columns": ["id"], "identity_modes": [0],
                "partition_identity_modes": null}],
            "sequences": [], "policy": serde_json::to_value(crate::contracts::RestorePolicy::default()).unwrap(),
            "copy_policy": {"use_copy_binary": false, "split_tables_larger_than": 1,
                "split_max_parts": 8, "skip_large_objects": false},
            "copy_tables": [{"source_oid": 6, "format": "text", "parts": [
                {"number": 1, "count": 2, "start_block": 0, "end_block": 10},
                {"number": 2, "count": 2, "start_block": 10, "end_block": null}]}]
        })).unwrap();
        follow.prepare_follow_source(&plan).unwrap();
        let intent = follow.begin_publication("m10_synthetic").unwrap();
        let publication = PublicationReceipt {
            name: intent.name.clone(),
            oid: 20,
            owner_oid: 7,
        };
        follow
            .record_publication_created(&intent, &publication)
            .unwrap();
        follow.confirm_publication(&intent, &publication).unwrap();
        let stream = StreamId {
            run: follow.identity.run,
            source: plan.source.identity,
            generation: 1,
        };
        let intent = follow
            .begin_source_slot(
                &SourceSlotBinding::new(stream, "m10_synthetic".into(), Plugin::Pgoutput, 1)
                    .unwrap(),
            )
            .unwrap();
        let slot = follow
            .confirm_source_slot(
                &intent,
                CreatedSlotData {
                    consistent_point: SourceEndLsn(16),
                    snapshot_name: CANARY.into(),
                },
            )
            .unwrap();
        let binding = FollowBinding {
            slot,
            publication,
            source_timeline: 1,
            copy_generation: CopyGeneration(1),
            source_types: Vec::new(),
        };
        follow
            .bind_follow(
                &plan,
                &SnapshotTicket {
                    token: CANARY.into(),
                    source: plan.source.clone(),
                    generation: CopyGeneration(1),
                },
                &binding,
            )
            .unwrap();
        follow
            .begin_target_provisioning(TargetBinding {
                stream,
                target_system_identifier: Some(4),
                target_database_oid: 5,
                schema_name: "m10_synthetic".into(),
                table_name: "checkpoint".into(),
                owner_oid: 7,
                metadata_version: 7,
            })
            .unwrap();
        seeds.push(serde_json::to_value(&follow.manifest).unwrap());
        fn normalize(value: &mut Value) {
            match value {
                Value::Object(fields) => {
                    for (key, value) in fields {
                        if key == "run" || key == "nonce" {
                            *value = json!(vec![1; 16]);
                        } else {
                            normalize(value);
                        }
                    }
                }
                Value::Array(values) => values.iter_mut().for_each(normalize),
                _ => {}
            }
        }
        for seed in &mut seeds {
            normalize(seed);
            seed["canonical_path"] = json!(native.identity.canonical_path);
            seed["directory_device"] = json!(native.manifest.directory_device);
            seed["directory_inode"] = json!(native.manifest.directory_inode);
        }
        drop(follow);
        Self {
            native,
            seeds,
            count: 0,
            _temp: temp,
        }
    }

    fn exercise(&mut self, bytes: &[u8], expected: Option<bool>, category: &str) {
        assert!(bytes.len() <= MAX_CASE_BYTES || category == "size-boundary");
        let path = self.native.identity.canonical_path.join("manifest.json");
        fs::write(&path, bytes).unwrap();
        let before = fs::metadata(&path).unwrap();
        let entries = fs::read_dir(&self.native.identity.canonical_path)
            .unwrap()
            .count();
        let read = || {
            read_manifest(&self.native.directory, &self.native.identity.canonical_path)
                .map(|(manifest, _)| manifest)
        };
        let first = read();
        let second = read();
        match (&first, &second) {
            (Ok(a), Ok(b)) => assert!(
                a == b,
                "seed={SEED:x} case={} category={category}",
                self.count
            ),
            (Err(a), Err(b)) => {
                assert!(
                    a.kind == ErrorKind::BadState && a.kind == b.kind && a.message == b.message,
                    "seed={SEED:x} case={} category={category} error class",
                    self.count
                );
                assert!(
                    a.message.len() <= 256
                        && a.message.starts_with("state-")
                        && !a.message.contains(CANARY),
                    "seed={SEED:x} case={} category={category} unsafe diagnostic",
                    self.count
                );
            }
            _ => panic!(
                "seed={SEED:x} case={} category={category} nondeterministic outcome",
                self.count
            ),
        }
        if category == "format-before-body" {
            assert!(
                first
                    .as_ref()
                    .is_err_and(|error| error.message.starts_with("state-version:")),
                "seed={SEED:x} case={} category={category}",
                self.count
            );
        }
        if let Some(expected) = expected {
            assert_eq!(
                first.is_ok(),
                expected,
                "seed={SEED:x} case={} category={category}",
                self.count
            );
        }
        let after = fs::metadata(&path).unwrap();
        assert!(
            before.ino() == after.ino()
                && before.mode() == after.mode()
                && (before.mtime(), before.mtime_nsec()) == (after.mtime(), after.mtime_nsec())
                && fs::read(&path).unwrap() == bytes
                && fs::read_dir(&self.native.identity.canonical_path)
                    .unwrap()
                    .count()
                    == entries,
            "seed={SEED:x} case={} category={category} reader mutated state",
            self.count
        );
        self.count += 1;
    }

    fn value(&mut self, value: &Value, accepted: bool, category: &str) {
        self.exercise(
            &serde_json::to_vec(value).unwrap(),
            Some(accepted),
            category,
        );
    }
}

#[test]
fn bounded_mutation_state_manifest_bytes() {
    let mut corpus = Corpus::new();
    let seeds = corpus.seeds.clone();
    let mut random = SEED;
    let mut hash = Sha256::new();
    let mut expected = 0;
    for seed in &seeds {
        assert_eq!(seed["format_version"], 10);
        let mut canonical = seed.clone();
        canonical["canonical_path"] = json!("/synthetic/state");
        canonical["directory_device"] = json!(1);
        canonical["directory_inode"] = json!(1);
        hash.update(serde_json::to_vec(&canonical).unwrap());
        let bytes = serde_json::to_vec(seed).unwrap();
        assert!(bytes.len() < 4096);
        corpus.exercise(&bytes, Some(true), "valid-seed");
        for end in 0..bytes.len() {
            corpus.exercise(&bytes[..end], Some(false), "truncation");
        }
        for _ in 0..32 {
            let offset = next(&mut random) as usize % bytes.len();
            for category in ["insert", "delete", "flip"] {
                let mut mutated = bytes.clone();
                match category {
                    "insert" => mutated.insert(offset, next(&mut random) as u8),
                    "delete" => {
                        mutated.remove(offset);
                    }
                    _ => mutated[offset] ^= 1 << (next(&mut random) % 8),
                }
                corpus.exercise(&mutated, None, category);
            }
        }
        expected += 1 + bytes.len() + 32 * 3;
    }
    assert_eq!(
        seeds[3]["target_provenance"][0]["record"]["binding"]["metadata_version"],
        7
    );
    assert_eq!(corpus.count, expected);
    assert!((1000..20_000).contains(&corpus.count));
    eprintln!(
        "bounded_mutation_state_manifest_bytes seed={SEED:x} seeds={} cases={} canonical_corpus_sha256={}",
        seeds.len(),
        corpus.count,
        hash.finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
}

#[test]
fn bounded_mutation_state_schema_and_settings() {
    let mut corpus = Corpus::new();
    let seeds = corpus.seeds.clone();
    for seed in &seeds {
        for key in seed.as_object().unwrap().keys() {
            let mut missing = seed.clone();
            missing.as_object_mut().unwrap().remove(key);
            corpus.value(&missing, false, "missing-root-field");
        }
        let text = serde_json::to_string(seed).unwrap();
        for field in [
            "\"format_version\":10",
            "\"format_version\":10,\"format_vers\\u0069on\":10",
            "\"foreign\":null",
            "\"foreign\":true",
            "\"foreign\":18446744073709551616",
        ] {
            let bytes = format!("{{{field},{}", &text[1..]);
            corpus.exercise(bytes.as_bytes(), Some(false), "duplicate-or-unknown-root");
        }
        for version in [8, 9, 11] {
            let mut old = seed.clone();
            old["format_version"] = json!(version);
            old.as_object_mut().unwrap().remove("follow_state");
            corpus.value(&old, false, "format-before-body");
        }
    }
    for field in [
        "magic",
        "run",
        "canonical_path",
        "directory_device",
        "directory_inode",
    ] {
        let mut invalid = seeds[0].clone();
        invalid[field] = match field {
            "magic" => json!(CANARY),
            "run" => json!(vec![0; 16]),
            "canonical_path" => json!("/synthetic/foreign"),
            _ => json!(0),
        };
        corpus.value(&invalid, false, "root-binding");
    }
    for field in ["format_version", "directory_device", "directory_inode"] {
        for value in [json!(true), json!(1.0), json!(-1), Value::Null] {
            let mut invalid = seeds[0].clone();
            invalid[field] = value;
            corpus.value(&invalid, false, "root-integer-boundary");
        }
    }
    for field in seeds[1]["follow_state"].as_object().unwrap().keys() {
        let mut missing = seeds[1].clone();
        missing["follow_state"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        corpus.value(&missing, false, "missing-required-nullable-follow-field");
    }
    let settings_path = "/follow_state/effective_settings";
    for field in [
        "durability",
        "barrier_interval_ms",
        "apply_group_txns",
        "limits",
    ] {
        let mut invalid = seeds[2].clone();
        invalid
            .pointer_mut(settings_path)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        corpus.value(&invalid, false, "missing-setting");
    }
    for (field, values) in [
        (
            "durability",
            vec![json!("relaxed"), json!("unknown"), json!(true), Value::Null],
        ),
        (
            "barrier_interval_ms",
            vec![json!(0), json!(100), json!(true), json!(1.0)],
        ),
        (
            "apply_group_txns",
            vec![json!(0), json!(-1), json!(true), json!(1.0), Value::Null],
        ),
        ("unknown", vec![json!(CANARY)]),
    ] {
        for value in values {
            let mut invalid = seeds[2].clone();
            invalid.pointer_mut(settings_path).unwrap()[field] = value;
            corpus.value(&invalid, false, "invalid-setting");
        }
    }
    for (field, maximum) in [
        ("channel_bytes", 8388608),
        ("channel_messages", 1024),
        ("max_message_bytes", 16777216),
        ("group_rows", 10000),
        ("group_bytes", 8388608),
    ] {
        for value in [
            json!(0),
            json!(maximum + 1),
            json!(-1),
            json!(true),
            json!(1.0),
            json!(u64::MAX),
            Value::Null,
        ] {
            let mut invalid = seeds[2].clone();
            invalid["follow_state"]["effective_settings"]["limits"][field] = value;
            corpus.value(&invalid, false, "limit-boundary");
        }
        let mut missing = seeds[2].clone();
        missing["follow_state"]["effective_settings"]["limits"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        corpus.value(&missing, false, "missing-limit");
    }
    let text = serde_json::to_string(&seeds[2]).unwrap();
    for replacement in [
        "\"apply_group_txns\":100,\"apply_group_txns\":1",
        "\"apply_group_txns\":100,\"apply_group_tx\\u006es\":1",
        "\"apply_group_txns\":18446744073709551616",
    ] {
        let bytes = text.replace("\"apply_group_txns\":100", replacement);
        assert_ne!(bytes.len(), text.len());
        corpus.exercise(
            bytes.as_bytes(),
            Some(false),
            "duplicate-or-overflow-setting",
        );
    }
    for depth in [128, 256] {
        let bytes = format!(
            "{{\"magic\":\"RPGMIGRATE\",\"format_version\":10,\"foreign\":{}0{}}}",
            "[".repeat(depth),
            "]".repeat(depth)
        );
        corpus.exercise(bytes.as_bytes(), Some(false), "recursion-boundary");
    }
    corpus.exercise(&[0xff, 0, b'{'], Some(false), "invalid-utf8");
    for size in [MAX_MANIFEST_BYTES as usize, MAX_MANIFEST_BYTES as usize + 1] {
        let mut bytes = serde_json::to_vec(&seeds[0]).unwrap();
        bytes.resize(size, b' ');
        corpus.exercise(
            &bytes,
            Some(size == MAX_MANIFEST_BYTES as usize),
            "size-boundary",
        );
    }
    let mut replay_safe = seeds[2].clone();
    replay_safe["follow_state"]["effective_settings"]["durability"] = json!("replay-safe");
    for cadence in [Value::Null, json!(0), json!(true), json!(1.0)] {
        replay_safe["follow_state"]["effective_settings"]["barrier_interval_ms"] = cadence;
        corpus.value(&replay_safe, false, "replay-safe-invalid-cadence");
    }
    replay_safe["follow_state"]["effective_settings"]["barrier_interval_ms"] = json!(100);
    corpus.value(&replay_safe, true, "stored-replay-safe-cadence");
    for (path, mode, restore) in [
        (
            corpus.native.identity.canonical_path.join("manifest.json"),
            0o644,
            0o600,
        ),
        (corpus.native.identity.canonical_path.clone(), 0o755, 0o700),
    ] {
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        corpus.value(&seeds[0], false, "unsafe-permissions");
        fs::set_permissions(&path, fs::Permissions::from_mode(restore)).unwrap();
    }
    assert_eq!(corpus.count, 177);
    eprintln!(
        "bounded_mutation_state_schema_and_settings seed={SEED:x} cases={}",
        corpus.count
    );
}

#[test]
fn bounded_mutation_state_recovery_obligations() {
    let mut corpus = Corpus::new();
    let bound = corpus.seeds[3].clone();
    for (pointer, value) in [
        ("/clone_state/ticket/token", json!("")),
        ("/clone_state/ticket/generation", json!(2)),
        ("/clone_state/plan/copy_tables", json!([])),
        ("/clone_state/plan/copy_tables/0/parts/1/number", json!(1)),
        (
            "/clone_state/plan/copy_tables/0/parts/1/start_block",
            json!(11),
        ),
        ("/clone_state/plan/copy_tables/0/parts/1/count", json!(3)),
        (
            "/clone_state/plan/copy_policy/skip_large_objects",
            Value::Null,
        ),
        ("/clone_state/phase", json!("complete")),
        (
            "/clone_state/completion",
            json!({"tables": 1, "rows": 0, "bytes": 0}),
        ),
        ("/source_slot", Value::Null),
        (
            "/source_slot/record/intent/binding/slot_name",
            json!("changed_slot"),
        ),
        (
            "/source_slot/record/intent/binding/stream/generation",
            json!(2),
        ),
        ("/follow_state/publication", Value::Null),
        ("/follow_state/publication/receipt/oid", json!(21)),
        (
            "/follow_state/cleanup",
            json!({"slot_removed": false, "publication_removed": false}),
        ),
        ("/follow_state/effective_settings", Value::Null),
        (
            "/target_provenance/0/record/binding/metadata_version",
            json!(6),
        ),
    ] {
        let mut invalid = bound.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        corpus.value(&invalid, false, "recovery-obligation");
    }
    let mut invalid = bound.clone();
    invalid["clone_state"]["plan"]["copy_tables"][0]["parts"][1]
        .as_object_mut()
        .unwrap()
        .remove("end_block");
    corpus.value(&invalid, false, "required-nullable-part-bound");
    // Pending target intent is not positive COMMIT evidence for cleanup, even with source receipts.
    let bytes = serde_json::to_vec(&bound).unwrap();
    fs::write(
        corpus.native.identity.canonical_path.join("manifest.json"),
        &bytes,
    )
    .unwrap();
    corpus.native.manifest = read_manifest(
        &corpus.native.directory,
        &corpus.native.identity.canonical_path,
    )
    .unwrap()
    .0;
    corpus.native.identity.run = corpus.native.manifest.run;
    assert!(corpus.native.checked_follow().unwrap().is_some());
    for phase in [
        ClonePhase::Complete,
        ClonePhase::Copying,
        ClonePhase::Analyzing,
    ] {
        assert!(
            corpus
                .native
                .mark_stage(phase)
                .unwrap_err()
                .message
                .starts_with("state-stage:")
        );
        assert_eq!(
            fs::read(corpus.native.identity.canonical_path.join("manifest.json")).unwrap(),
            bytes
        );
        corpus.count += 1;
    }
    assert!(
        corpus
            .native
            .begin_follow_cleanup()
            .unwrap_err()
            .message
            .starts_with("state-cleanup:")
    );
    assert!(
        corpus
            .native
            .finish_follow_restart()
            .unwrap_err()
            .message
            .starts_with("state-follow:")
    );
    assert!(
        corpus
            .native
            .record_clone_complete(&CopyReport {
                tables: 1,
                rows: 0,
                bytes: 0
            })
            .unwrap_err()
            .message
            .starts_with("state-complete:")
    );
    assert_eq!(
        fs::read(corpus.native.identity.canonical_path.join("manifest.json")).unwrap(),
        bytes
    );
    corpus.count += 3;
    let clone = corpus.native.checked_clone().unwrap().unwrap();
    let plan = clone.plan;
    let table = plan.tables[0].clone();
    corpus.native.begin_pre_data().unwrap();
    corpus
        .native
        .confirm_pre_data(
            &plan,
            vec![OwnedObject {
                class_oid: 1259,
                object_oid: 30,
                object_sub_id: 0,
                namespace_oid: Some(31),
                namespace_name: Some(table.name.schema.clone()),
                name: table.name.table.clone(),
                kind: "r".into(),
                owner_oid: 7,
            }],
            vec![OwnedCopyTable {
                table,
                target_oid: 30,
                target_namespace_oid: 31,
                target_owner_oid: 7,
                generation: CopyGeneration(1),
            }],
        )
        .unwrap();
    let binding = corpus.native.checked_follow().unwrap().unwrap();
    corpus.native.begin_follow_restart(&binding, &plan).unwrap();
    let restart = serde_json::to_value(&corpus.native.manifest).unwrap();
    corpus.value(&restart, true, "valid-retained-restart");
    for (pointer, value) in [
        ("/follow_state/restart/next_generation", json!(0)),
        ("/follow_state/restart/next_generation", json!(1)),
        ("/follow_state/restart/next_generation", json!(u64::MAX)),
        ("/follow_state/restart/new_bound", json!(true)),
        (
            "/follow_state/restart/previous/slot/consistent_point",
            json!(0),
        ),
        ("/source_slot", Value::Null),
        ("/follow_state/publication", Value::Null),
        ("/clone_state/owned_tables", json!([])),
    ] {
        let mut invalid = restart.clone();
        *invalid.pointer_mut(pointer).unwrap() = value;
        corpus.value(&invalid, false, "restart-obligation");
    }
    assert_eq!(corpus.count, 33);
    eprintln!(
        "bounded_mutation_state_recovery_obligations seed={SEED:x} cases={}",
        corpus.count
    );
}
