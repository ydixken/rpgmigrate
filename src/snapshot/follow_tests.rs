use super::*;
use crate::contracts::{
    CloneOptions, ClonePlan, CloneTarget, Conninfo, CopyGeneration, FollowLimits, FollowOptions,
    RestorePolicy, SnapshotTicket, StateAction, StateRequest,
};
use crate::pg::CommitObservation;
use crate::state::SourceSlotProvenance;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64},
};

static CASE: AtomicU64 = AtomicU64::new(0);

fn with_owned_full_page_writes_off(
    source: &mut PgConnection,
    io: &IoControl,
    check: impl FnOnce(&mut PgConnection),
) {
    let context =
        std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned follow harness required");
    let context: serde_json::Value =
        serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
    let root = PathBuf::from(context["root"].as_str().unwrap());
    let token = context["token"].as_str().unwrap();
    assert_eq!(
        std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
        token
    );
    let expected_data = root.join("data").canonicalize().unwrap();
    let identity=source.query("SELECT pg_catalog.current_setting('data_directory'),pg_catalog.current_setting('cluster_name')",io).unwrap();
    let actual_data =
        PathBuf::from(std::str::from_utf8(identity.cell(0, 0).unwrap().unwrap()).unwrap())
            .canonicalize()
            .unwrap();
    assert_eq!(
        actual_data, expected_data,
        "configuration changes require the exact owned cluster"
    );
    assert_eq!(identity.cell(0, 1).unwrap(), Some(token.as_bytes()));
    let config = expected_data.join("postgresql.conf");
    assert!(
        std::fs::symlink_metadata(&config)
            .unwrap()
            .file_type()
            .is_file()
    );
    let original = std::fs::read(&config).unwrap();
    let reload = |source: &mut PgConnection, expected: &[u8]| {
        let reloaded = source
            .query("SELECT pg_catalog.pg_reload_conf()", io)
            .unwrap();
        assert_eq!(reloaded.cell(0, 0).unwrap(), Some(b"t".as_slice()));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let setting = source.query("SHOW full_page_writes", io).unwrap();
            if setting.cell(0, 0).unwrap() == Some(expected) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "owned full_page_writes reload was not observed"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    };
    let mut changed = original.clone();
    changed.extend_from_slice(b"\nfull_page_writes = off\n");
    std::fs::write(&config, changed).unwrap();
    reload(source, b"off");
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(source)));
    std::fs::write(&config, original).unwrap();
    reload(source, b"on");
    if let Err(payload) = outcome {
        std::panic::resume_unwind(payload);
    }
}

#[test]
fn restart_requires_the_same_plan_and_exact_next_copy_generation() {
    let source = crate::contracts::CloneSource {
        identity: SourceId {
            system_identifier: 7,
            database_oid: 23,
        },
        timeline: 1,
        major: 16,
    };
    let previous = ClonePlan {
        source,
        target: CloneTarget {
            identity: SourceId {
                system_identifier: 7,
                database_oid: 24,
            },
            major: 18,
        },
        generation: CopyGeneration(1),
        copy_tables: Vec::new(),
        tables: Vec::new(),
        sequences: Vec::new(),
        policy: RestorePolicy::default(),
        copy_policy: crate::contracts::CloneCopyPolicy::default(),
    };
    let mut next = previous.clone();
    next.generation = CopyGeneration(2);
    super::follow::require_restart_plan(&previous, &next, CopyGeneration(2)).unwrap();
    for field in 0..7 {
        let mut wrong = next.clone();
        match field {
            0 => wrong.source.timeline += 1,
            1 => wrong.target.identity.database_oid += 1,
            2 => wrong.generation = CopyGeneration(1),
            3 => wrong.policy.no_owner = true,
            4 => wrong.sequences.push(crate::contracts::QualifiedTable {
                schema: "public".into(),
                table: "extra".into(),
            }),
            5 => wrong.source.identity.database_oid += 1,
            _ => wrong.copy_policy.skip_large_objects = true,
        }
        assert!(super::follow::require_restart_plan(&previous, &wrong, CopyGeneration(2)).is_err());
    }
}

struct PendingFixture {
    root: PathBuf,
    native: NativeState,
    plan: ClonePlan,
    restart: crate::state::FollowRestart,
    binding: crate::contracts::FollowBinding,
}

impl PendingFixture {
    fn new() -> Self {
        let (root, mut native) = Self::open();
        let mut plan = Self::plan();
        let binding = Self::resources(&mut native, &plan, 10);
        let ticket = SnapshotTicket {
            token: binding.slot.snapshot_token.clone(),
            source: plan.source.clone(),
            generation: plan.generation,
        };
        native.bind_follow(&plan, &ticket, &binding).unwrap();
        native.begin_pre_data().unwrap();
        native
            .confirm_pre_data(&plan, Vec::new(), Vec::new())
            .unwrap();
        let restart = native.begin_follow_restart(&binding, &plan).unwrap();
        native.confirm_follow_slot_absent(&restart).unwrap();
        native.confirm_follow_publication_absent(&restart).unwrap();
        let restart = native.checked_follow_restart().unwrap().unwrap();
        plan.generation = restart.next_generation;
        let binding = Self::resources(&mut native, &plan, 20);
        Self {
            root,
            native,
            plan,
            restart,
            binding,
        }
    }

    fn open() -> (PathBuf, NativeState) {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!(
            "rpgmigrate-pending-source-{}-{}",
            std::process::id(),
            CASE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let native = Self::reopen(&root, StateAction::Create);
        (root, native)
    }

    fn reopen(root: &std::path::Path, action: StateAction) -> NativeState {
        crate::state::open_follow(
            &StateRequest {
                path: root.join("state"),
                action,
            },
            None,
            None,
            &IoControl::unbounded(Arc::new(AtomicBool::new(false))),
        )
        .unwrap()
    }

    fn plan() -> ClonePlan {
        ClonePlan {
            source: crate::contracts::CloneSource {
                identity: SourceId {
                    system_identifier: 7,
                    database_oid: 23,
                },
                timeline: 1,
                major: 16,
            },
            target: CloneTarget {
                identity: SourceId {
                    system_identifier: 8,
                    database_oid: 24,
                },
                major: 18,
            },
            generation: CopyGeneration(1),
            tables: Vec::new(),
            sequences: Vec::new(),
            policy: RestorePolicy::default(),
            copy_policy: crate::contracts::CloneCopyPolicy::default(),
            copy_tables: Vec::new(),
        }
    }

    fn slot_binding(native: &NativeState, plan: &ClonePlan) -> SourceSlotBinding {
        SourceSlotBinding::new(
            crate::identity::StreamId {
                run: native.identity().run,
                source: plan.source.identity,
                generation: 1,
            },
            "pending_source".into(),
            crate::cdc::contracts::Plugin::Pgoutput,
            plan.generation.0,
        )
        .unwrap()
    }

    fn publication(native: &mut NativeState, oid: u32) -> crate::contracts::PublicationReceipt {
        let intent = native.begin_publication("pending_source").unwrap();
        let publication = crate::contracts::PublicationReceipt {
            name: intent.name.clone(),
            oid,
            owner_oid: 7,
        };
        native
            .record_publication_created(&intent, &publication)
            .unwrap();
        native.confirm_publication(&intent, &publication).unwrap();
        publication
    }

    fn resources(
        native: &mut NativeState,
        plan: &ClonePlan,
        point: u64,
    ) -> crate::contracts::FollowBinding {
        native.prepare_follow_source(plan).unwrap();
        let publication = Self::publication(native, point as u32);
        let slot_intent = native
            .begin_source_slot(&Self::slot_binding(native, plan))
            .unwrap();
        let slot = native
            .confirm_source_slot(
                &slot_intent,
                CreatedSlotData {
                    consistent_point: crate::identity::SourceEndLsn(point),
                    snapshot_name: format!("snapshot-{point}"),
                },
            )
            .unwrap();
        crate::contracts::FollowBinding {
            slot,
            publication,
            source_timeline: plan.source.timeline,
            copy_generation: plan.generation,
            source_types: Vec::new(),
        }
    }

    fn manifest(&self) -> Vec<u8> {
        std::fs::read(self.root.join("state/manifest.json")).unwrap()
    }
}

impl Drop for PendingFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn pending_source_retirement_rejects_foreign_stale_or_bound_proofs_without_mutation() {
    let mut fixture = PendingFixture::new();
    let before = fixture.manifest();
    for field in 0..4 {
        let mut receipt = fixture.binding.slot.clone();
        if field == 0 {
            receipt.intent.nonce[0] ^= 1;
        }
        let mut proof = super::follow::PendingFollowSlotAbsence {
            restart: fixture.restart.clone(),
            slot: SourceSlotProvenance::Confirmed(receipt),
        };
        match field {
            0 => {}
            1 => proof.restart.next_generation.0 += 1,
            2 => proof.restart.new_bound = true,
            _ => proof.restart.slot_absent = false,
        }
        assert!(
            fixture
                .native
                .confirm_pending_follow_slot_absent(&proof)
                .is_err(),
            "field {field}"
        );
        assert_eq!(fixture.manifest(), before);
    }
    let mut proof = super::follow::PendingFollowPublicationAbsence {
        restart: fixture.restart.clone(),
        receipt: fixture.binding.publication.clone(),
    };
    assert!(
        fixture
            .native
            .confirm_pending_follow_publication_absent(&proof)
            .is_err()
    );
    assert_eq!(fixture.manifest(), before);
    fixture
        .native
        .confirm_pending_follow_slot_absent(&super::follow::PendingFollowSlotAbsence {
            restart: fixture.restart.clone(),
            slot: SourceSlotProvenance::Confirmed(fixture.binding.slot.clone()),
        })
        .unwrap();
    let before = fixture.manifest();
    proof.receipt.oid += 1;
    assert!(
        fixture
            .native
            .confirm_pending_follow_publication_absent(&proof)
            .is_err()
    );
    assert_eq!(fixture.manifest(), before);
}

#[test]
fn lost_unbound_snapshot_retires_receipts_without_binding_an_old_execution_profile() {
    let mut fixture = PendingFixture::new();
    let saved = fixture.native.checked_clone().unwrap().unwrap();
    let slot = super::follow::PendingFollowSlotAbsence {
        restart: fixture.restart.clone(),
        slot: SourceSlotProvenance::Confirmed(fixture.binding.slot.clone()),
    };
    fixture
        .native
        .confirm_pending_follow_slot_absent(&slot)
        .unwrap();
    assert!(fixture.native.checked_source_slot().unwrap().is_none());
    assert_eq!(fixture.native.checked_clone().unwrap().unwrap(), saved);
    assert_eq!(
        fixture.native.checked_follow_restart().unwrap(),
        Some(fixture.restart.clone())
    );
    let publication = super::follow::PendingFollowPublicationAbsence {
        restart: fixture.restart.clone(),
        receipt: fixture.binding.publication.clone(),
    };
    fixture
        .native
        .confirm_pending_follow_publication_absent(&publication)
        .unwrap();
    assert!(fixture.native.checked_publication().unwrap().is_none());
    assert_eq!(fixture.native.checked_clone().unwrap().unwrap(), saved);
    assert!(
        fixture
            .native
            .confirm_pending_follow_slot_absent(&slot)
            .is_err()
    );
    assert!(
        fixture
            .native
            .confirm_pending_follow_publication_absent(&publication)
            .is_err()
    );
    let fresh = PendingFixture::resources(&mut fixture.native, &fixture.plan, 30);
    assert_ne!(fresh.slot.intent.nonce, fixture.binding.slot.intent.nonce);
    assert_eq!(fresh.copy_generation, fixture.restart.next_generation);
    assert_eq!(fixture.native.checked_clone().unwrap().unwrap(), saved);
    let ticket = SnapshotTicket {
        token: fresh.slot.snapshot_token.clone(),
        source: fixture.plan.source.clone(),
        generation: fixture.plan.generation,
    };
    fixture
        .native
        .bind_follow(&fixture.plan, &ticket, &fresh)
        .unwrap();
    assert!(
        fixture
            .native
            .confirm_pending_follow_slot_absent(&slot)
            .is_err()
    );
    assert_eq!(
        fixture.native.checked_clone().unwrap().unwrap().plan,
        fixture.plan
    );
}

#[test]
fn restart_retires_its_pending_slot_intent_without_a_receipt_but_never_a_changed_one() {
    let mut fixture = PendingFixture::new();
    let restart = fixture.restart.clone();
    fixture
        .native
        .confirm_pending_follow_slot_absent(&super::follow::PendingFollowSlotAbsence {
            restart: restart.clone(),
            slot: SourceSlotProvenance::Confirmed(fixture.binding.slot.clone()),
        })
        .unwrap();
    fixture
        .native
        .confirm_pending_follow_publication_absent(
            &super::follow::PendingFollowPublicationAbsence {
                restart: restart.clone(),
                receipt: fixture.binding.publication.clone(),
            },
        )
        .unwrap();
    // The next attempt dies inside CREATE_REPLICATION_SLOT, before its receipt.
    let publication = PendingFixture::publication(&mut fixture.native, 40);
    let pending = fixture
        .native
        .begin_source_slot(&PendingFixture::slot_binding(
            &fixture.native,
            &fixture.plan,
        ))
        .unwrap();
    let before = fixture.manifest();
    for field in 0..4 {
        let mut intent = pending.clone();
        let mut changed = restart.clone();
        let slot = match field {
            0 => {
                intent.nonce[0] ^= 1;
                SourceSlotProvenance::Provisioning(intent)
            }
            1 => SourceSlotProvenance::Confirmed(crate::state::SourceSlotReceipt {
                intent,
                consistent_point: crate::identity::SourceEndLsn(50),
                snapshot_token: "snapshot-50".into(),
            }),
            2 => {
                changed.next_generation.0 += 1;
                SourceSlotProvenance::Provisioning(intent)
            }
            _ => {
                changed.new_bound = true;
                SourceSlotProvenance::Provisioning(intent)
            }
        };
        let proof = super::follow::PendingFollowSlotAbsence {
            restart: changed,
            slot,
        };
        assert!(
            fixture
                .native
                .confirm_pending_follow_slot_absent(&proof)
                .is_err(),
            "field {field}"
        );
        assert_eq!(fixture.manifest(), before);
    }
    // The first-run rule never retires a restart's intent.
    let provisioning = SourceSlotProvenance::Provisioning(pending.clone());
    assert!(
        fixture
            .native
            .require_pending_source_slot_retirement(&provisioning)
            .is_err()
    );
    assert!(
        fixture
            .native
            .confirm_pending_source_slot_absent(&provisioning)
            .is_err()
    );
    assert_eq!(fixture.manifest(), before);
    let proof = super::follow::PendingFollowSlotAbsence {
        restart: restart.clone(),
        slot: SourceSlotProvenance::Provisioning(pending.clone()),
    };
    fixture
        .native
        .confirm_pending_follow_slot_absent(&proof)
        .unwrap();
    assert!(fixture.native.checked_source_slot().unwrap().is_none());
    assert!(
        fixture
            .native
            .confirm_pending_follow_slot_absent(&proof)
            .is_err()
    );
    fixture
        .native
        .confirm_pending_follow_publication_absent(
            &super::follow::PendingFollowPublicationAbsence {
                restart,
                receipt: publication,
            },
        )
        .unwrap();
    let fresh = PendingFixture::resources(&mut fixture.native, &fixture.plan, 60);
    assert_ne!(fresh.slot.intent.nonce, pending.nonce);
}

#[test]
fn first_run_retires_its_pending_slot_intent_once_and_authorizes_one_new_create() {
    let plan = PendingFixture::plan();
    // A slot intent without a confirmed publication was never authorized by create.
    let (unpublished, mut native) = PendingFixture::open();
    native.prepare_follow_source(&plan).unwrap();
    let orphan = SourceSlotProvenance::Provisioning(
        native
            .begin_source_slot(&PendingFixture::slot_binding(&native, &plan))
            .unwrap(),
    );
    let before = std::fs::read(unpublished.join("state/manifest.json")).unwrap();
    assert!(
        native
            .require_pending_source_slot_retirement(&orphan)
            .is_err()
    );
    assert!(native.confirm_pending_source_slot_absent(&orphan).is_err());
    assert_eq!(
        std::fs::read(unpublished.join("state/manifest.json")).unwrap(),
        before
    );
    drop(native);
    std::fs::remove_dir_all(&unpublished).unwrap();

    let (root, mut native) = PendingFixture::open();
    native.prepare_follow_source(&plan).unwrap();
    PendingFixture::publication(&mut native, 30);
    let pending = native
        .begin_source_slot(&PendingFixture::slot_binding(&native, &plan))
        .unwrap();
    drop(native);
    let mut native = PendingFixture::reopen(&root, StateAction::Restart);
    let manifest = || std::fs::read(root.join("state/manifest.json")).unwrap();
    let before = manifest();
    let mut changed = pending.clone();
    changed.nonce[0] ^= 1;
    let (changed, pending_intent) = (SourceSlotProvenance::Provisioning(changed), pending.clone());
    let pending = SourceSlotProvenance::Provisioning(pending);
    assert!(
        native
            .require_pending_source_slot_retirement(&changed)
            .is_err()
    );
    assert!(native.confirm_pending_source_slot_absent(&changed).is_err());
    assert_eq!(manifest(), before);
    native
        .require_pending_source_slot_retirement(&pending)
        .unwrap();
    assert_eq!(manifest(), before);
    native.confirm_pending_source_slot_absent(&pending).unwrap();
    assert!(native.checked_source_slot().unwrap().is_none());
    assert!(native.confirm_pending_source_slot_absent(&pending).is_err());
    let binding = PendingFixture::slot_binding(&native, &plan);
    let replacement = native.begin_source_slot(&binding).unwrap();
    assert_ne!(replacement.nonce, pending_intent.nonce);
    assert!(native.begin_source_slot(&binding).is_err());
    let receipt = native
        .confirm_source_slot(
            &replacement,
            CreatedSlotData {
                consistent_point: crate::identity::SourceEndLsn(70),
                snapshot_name: "snapshot-70".into(),
            },
        )
        .unwrap();
    // An unbound receipt is retired only in its exact confirmed form, never as its intent.
    let before = manifest();
    for wrong in [
        SourceSlotProvenance::Provisioning(receipt.intent.clone()),
        SourceSlotProvenance::Confirmed(crate::state::SourceSlotReceipt {
            consistent_point: crate::identity::SourceEndLsn(71),
            ..receipt.clone()
        }),
    ] {
        assert!(
            native
                .require_pending_source_slot_retirement(&wrong)
                .is_err()
        );
        assert!(native.confirm_pending_source_slot_absent(&wrong).is_err());
        assert_eq!(manifest(), before);
    }
    let confirmed = SourceSlotProvenance::Confirmed(receipt);
    native
        .confirm_pending_source_slot_absent(&confirmed)
        .unwrap();
    assert!(native.checked_source_slot().unwrap().is_none());
    let again = native.begin_source_slot(&binding).unwrap();
    assert_ne!(again.nonce, replacement.nonce);
    assert!(native.begin_source_slot(&binding).is_err());
    drop(native);
    std::fs::remove_dir_all(&root).unwrap();
}

#[test]
fn an_uncreated_slot_intent_is_retired_only_in_its_exact_pending_form() {
    let plan = PendingFixture::plan();
    let (root, mut native) = PendingFixture::open();
    let manifest = || std::fs::read(root.join("state/manifest.json")).unwrap();
    native.prepare_follow_source(&plan).unwrap();
    PendingFixture::publication(&mut native, 30);
    let binding = PendingFixture::slot_binding(&native, &plan);
    let pending = native.begin_source_slot(&binding).unwrap();
    let before = manifest();
    let mut changed = pending.clone();
    changed.nonce[0] ^= 1;
    assert!(native.retire_uncreated_source_slot(&changed).is_err());
    assert_eq!(manifest(), before);
    native.retire_uncreated_source_slot(&pending).unwrap();
    drop(native);
    let mut native = PendingFixture::reopen(&root, StateAction::Resume);
    assert!(native.checked_source_slot().unwrap().is_none());
    assert!(native.retire_uncreated_source_slot(&pending).is_err());
    let replacement = native.begin_source_slot(&binding).unwrap();
    let receipt = native
        .confirm_source_slot(
            &replacement,
            CreatedSlotData {
                consistent_point: crate::identity::SourceEndLsn(70),
                snapshot_name: "snapshot-70".into(),
            },
        )
        .unwrap();
    let before = manifest();
    assert!(
        native
            .retire_uncreated_source_slot(&receipt.intent)
            .is_err()
    );
    assert_eq!(manifest(), before);
    drop(native);
    std::fs::remove_dir_all(&root).unwrap();
}

struct Fixture {
    admin: PgConnection,
    source: PgConnection,
    options: FollowOptions,
    native: Option<NativeState>,
    plan: ClonePlan,
    database: String,
    io: IoControl,
}

impl Fixture {
    fn new(ddl: &str) -> Self {
        let context =
            std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned follow harness required");
        let context: serde_json::Value =
            serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
        let root = PathBuf::from(context["root"].as_str().unwrap());
        assert_eq!(
            std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
            context["token"].as_str().unwrap()
        );
        let admin_info = Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_FOLLOW_SOURCE_ADMIN")
                .expect("owned follow source required"),
        )
        .unwrap();
        let io = IoControl::unbounded(Arc::new(AtomicBool::new(false)));
        let mut admin = PgConnection::connect(&admin_info, crate::pg::Role::Copy, &io).unwrap();
        let database = format!(
            "m3_source_{}_{}",
            std::process::id(),
            CASE.fetch_add(1, Ordering::Relaxed)
        );
        admin
            .query(&format!("CREATE DATABASE {database}"), &io)
            .unwrap();
        let source_info = Conninfo::new(
            format!(
                "{} dbname={database}",
                std::str::from_utf8(admin_info.as_c_str().to_bytes()).unwrap()
            )
            .into(),
        )
        .unwrap();
        let mut source = PgConnection::connect(&source_info, crate::pg::Role::Copy, &io).unwrap();
        catalog::configure_session(&mut source, &io).unwrap();
        for statement in ddl.split(';').filter(|s| !s.trim().is_empty()) {
            source.query(statement, &io).unwrap();
        }
        let source_id = catalog::read_clone_source(&mut source, &io).unwrap();
        catalog::command(
            &mut source,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            &io,
        )
        .unwrap();
        let tables = catalog::inspect_clone_tables(&mut source, &[], &io).unwrap();
        catalog::command(&mut source, "COMMIT", "COMMIT", &io).unwrap();
        let target = CloneTarget {
            identity: SourceId {
                system_identifier: source_id.identity.system_identifier,
                database_oid: source_id.identity.database_oid + 1,
            },
            major: source_id.major,
        };
        let plan = ClonePlan {
            source: source_id,
            target,
            generation: CopyGeneration(1),
            copy_tables: catalog::provisional_copy_tables(&tables).unwrap(),
            tables,
            sequences: Vec::new(),
            policy: RestorePolicy::default(),
            copy_policy: crate::contracts::CloneCopyPolicy::default(),
        };
        let options = FollowOptions {
            clone: CloneOptions {
                source: source_info.clone(),
                target: source_info,
                state: StateRequest {
                    path: root.join(format!("{database}-state")),
                    action: StateAction::Create,
                },
                operator: None,
                table_jobs: 4,
                post_data_jobs: 4,
                policy: RestorePolicy::default(),
                not_consistent: false,
                use_copy_binary: false,
                split_tables_larger_than: 0,
                split_max_parts: 8,
                skip_large_objects: false,
                large_objects_jobs: 4,
            },
            slot_name: String::new(),
            publication_name: String::new(),
            origin_name: Some("legacy_unowned_origin".into()),
            apply_group_txns: 100,
            limits: FollowLimits::default(),
        };
        let native = crate::state::open_follow(
            &options.clone.state,
            None,
            options.origin_name.as_deref(),
            &io,
        )
        .unwrap();
        Self {
            admin,
            source,
            options,
            native: Some(native),
            plan,
            database,
            io,
        }
    }
    fn create(&mut self) -> FollowSnapshot {
        FollowSnapshot::create(
            &self.options,
            &self.plan,
            self.native.as_mut().unwrap(),
            &self.io,
        )
        .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // The fixture owns this fresh database and never selects resources by a foreign prefix.
        let _ = self.admin.query(
            &format!("DROP DATABASE {} WITH (FORCE)", self.database),
            &self.io,
        );
    }
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_slot_snapshot_preserves_initial_boundary_and_exporter_lifetime() {
    let mut fixture = Fixture::new(
        "CREATE TABLE public.items(id integer PRIMARY KEY,payload text); INSERT INTO public.items VALUES(1,'before')",
    );
    let mut snapshot = fixture.create();
    let ticket = snapshot.snapshot_mut().ticket().clone();
    assert_eq!(ticket.token, snapshot.receipt().snapshot_token);
    assert!(snapshot.receipt().consistent_point.0 > 0);
    fixture
        .native
        .as_mut()
        .unwrap()
        .bind_follow(&fixture.plan, &ticket, snapshot.binding())
        .unwrap();
    fixture
        .source
        .query("INSERT INTO public.items VALUES(2,'after')", &fixture.io)
        .unwrap();
    let rows = snapshot
        .snapshot_mut()
        .connection()
        .unwrap()
        .query("SELECT count(*) FROM public.items", &fixture.io)
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(b"1".as_slice()));
    let mut importer = PgConnection::connect(
        &fixture.options.clone.source,
        crate::pg::Role::Copy,
        &fixture.io,
    )
    .unwrap();
    import(&mut importer, &ticket, &fixture.io).unwrap();
    let binding = snapshot.binding().clone();
    catalog::check_follow_health(
        &mut fixture.source,
        &fixture.plan,
        &binding,
        Some(snapshot.exporter_pid()),
        &fixture.io,
    )
    .unwrap();
    assert!(
        catalog::follow_confirmed_flush(
            &mut fixture.source,
            &fixture.plan,
            &binding,
            Some(snapshot.exporter_pid()),
            &fixture.io
        )
        .unwrap()
            >= binding.slot.consistent_point
    );
    snapshot.finish(&fixture.io).unwrap();
    let rows = importer
        .query("SELECT count(*) FROM public.items", &fixture.io)
        .unwrap();
    assert_eq!(rows.cell(0, 0).unwrap(), Some(b"1".as_slice()));
    catalog::command(&mut importer, "COMMIT", "COMMIT", &fixture.io).unwrap();
    let mut late = PgConnection::connect(
        &fixture.options.clone.source,
        crate::pg::Role::Copy,
        &fixture.io,
    )
    .unwrap();
    assert!(import(&mut late, &ticket, &fixture.io).is_err());
    assert_eq!(
        catalog::follow_confirmed_flush(
            &mut fixture.source,
            &fixture.plan,
            &binding,
            None,
            &fixture.io
        )
        .unwrap(),
        binding.slot.consistent_point
    );
    let inactive_pid = i32::try_from(fixture.source.backend_pid().unwrap()).unwrap();
    let error = catalog::follow_start_confirmed_flush(
        &mut fixture.source,
        &binding,
        inactive_pid,
        &fixture.io,
    )
    .unwrap_err();
    assert!(
        error.message.contains("active"),
        "inactive slot must not establish a receiver lease"
    );
    let mut receiver =
        PgConnection::connect_replication(&fixture.options.clone.source, &fixture.io).unwrap();
    let receiver_pid = i32::try_from(receiver.backend_pid().unwrap()).unwrap();
    receiver
        .start_replication(
            binding.slot.intent.binding.slot_name(),
            binding.slot.consistent_point.0,
            &[
                ("proto_version", Some("1")),
                ("publication_names", Some(&binding.publication.name)),
            ],
            &crate::pg::operation_control(&fixture.io),
        )
        .unwrap();
    assert_eq!(
        catalog::follow_start_confirmed_flush(
            &mut fixture.source,
            &binding,
            receiver_pid,
            &fixture.io
        )
        .unwrap(),
        binding.slot.consistent_point
    );
    assert!(
        catalog::follow_start_confirmed_flush(
            &mut fixture.source,
            &binding,
            receiver_pid.checked_add(1).unwrap(),
            &fixture.io
        )
        .unwrap_err()
        .message
        .contains("owner")
    );
    assert!(
        catalog::follow_start_confirmed_flush(&mut fixture.source, &binding, 0, &fixture.io)
            .is_err()
    );
    let mut changed = binding.clone();
    let mut stream = binding.stream();
    stream.source.database_oid += 1;
    changed.slot.intent.binding = SourceSlotBinding::new(
        stream,
        binding.slot.intent.binding.slot_name().into(),
        crate::cdc::contracts::Plugin::Pgoutput,
        binding.slot.intent.binding.catalog_epoch(),
    )
    .unwrap();
    assert!(
        catalog::follow_start_confirmed_flush(
            &mut fixture.source,
            &changed,
            receiver_pid,
            &fixture.io
        )
        .unwrap_err()
        .message
        .contains("identity")
    );
    changed = binding.clone();
    changed.source_timeline += 1;
    assert!(
        catalog::follow_start_confirmed_flush(
            &mut fixture.source,
            &changed,
            receiver_pid,
            &fixture.io
        )
        .unwrap_err()
        .message
        .contains("timeline")
    );
    with_owned_full_page_writes_off(&mut fixture.source, &fixture.io, |source| {
        let error = catalog::confirm_durability(source, "on", &fixture.io).unwrap_err();
        assert!(error.message.contains("full_page_writes"));
        assert!(
            catalog::check_follow_health(
                source,
                &fixture.plan,
                &binding,
                Some(receiver_pid),
                &fixture.io
            )
            .unwrap_err()
            .message
            .contains("full_page_writes")
        );
        assert!(
            catalog::follow_start_confirmed_flush(source, &binding, receiver_pid, &fixture.io)
                .unwrap_err()
                .message
                .contains("full_page_writes")
        );
        let mut fresh = Fixture::new("CREATE TABLE public.items(id integer PRIMARY KEY)");
        let error = FollowSnapshot::create(
            &fresh.options,
            &fresh.plan,
            fresh.native.as_mut().unwrap(),
            &fresh.io,
        )
        .err()
        .expect("unsafe source storage must refuse before publication");
        assert!(error.message.contains("full_page_writes"));
        assert!(
            fresh
                .native
                .as_ref()
                .unwrap()
                .checked_publication()
                .unwrap()
                .is_none()
        );
        let resources=fresh.source.query("SELECT (SELECT count(*) FROM pg_catalog.pg_publication),(SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()))",&fresh.io).unwrap();
        assert_eq!(resources.cell(0, 0).unwrap(), Some(b"0".as_slice()));
        assert_eq!(resources.cell(0, 1).unwrap(), Some(b"0".as_slice()));
    });
    assert_eq!(
        catalog::follow_start_confirmed_flush(
            &mut fixture.source,
            &binding,
            receiver_pid,
            &fixture.io
        )
        .unwrap(),
        binding.slot.consistent_point
    );
    drop(receiver);
    let origin=fixture.source.query("SELECT count(*) FROM pg_catalog.pg_replication_origin WHERE roname='legacy_unowned_origin'",&fixture.io).unwrap();
    assert_eq!(origin.cell(0, 0).unwrap(), Some(b"0".as_slice()));
}

fn live_major() -> u32 {
    Fixture::new("").plan.source.major
}

fn source_resources(fixture: &mut Fixture) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    let resources=fixture.source.query("SELECT (SELECT count(*) FROM pg_catalog.pg_publication),(SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()))",&fixture.io).unwrap();
    (
        resources.cell(0, 0).unwrap().map(<[u8]>::to_vec),
        resources.cell(0, 1).unwrap().map(<[u8]>::to_vec),
    )
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_generated_columns_refuse_before_resource_creation() {
    const EXPRESSION: &str =
        "follow-preflight: generated columns require built-in immutable expressions";
    const KEY: &str = "follow-preflight: replica identity keys must not contain generated columns";
    const FULL: &str = "follow-preflight: PostgreSQL 18 refuses UPDATE and DELETE";
    let mut cases = vec![
        (
            "CREATE FUNCTION public.twice(integer) RETURNS integer IMMUTABLE LANGUAGE sql AS 'SELECT $1*2'; CREATE TABLE public.items(id integer PRIMARY KEY,value integer,twice integer GENERATED ALWAYS AS (public.twice(value)) STORED)",
            EXPRESSION,
        ),
        (
            "CREATE COLLATION public.casefold(provider=icu,locale='und-u-ks-level2',deterministic=false); CREATE TABLE public.items(id integer PRIMARY KEY,value text,folded boolean GENERATED ALWAYS AS (value COLLATE public.casefold='x') STORED)",
            EXPRESSION,
        ),
        (
            "CREATE TABLE public.items(value integer,id integer GENERATED ALWAYS AS (value+1) STORED PRIMARY KEY)",
            KEY,
        ),
        (
            "CREATE TABLE public.items(id integer PRIMARY KEY,value integer NOT NULL,twice integer GENERATED ALWAYS AS (value*2) STORED NOT NULL); CREATE UNIQUE INDEX items_twice ON public.items(twice); ALTER TABLE public.items REPLICA IDENTITY USING INDEX items_twice",
            KEY,
        ),
    ];
    // PostgreSQL 16 publishes FULL tables with generated columns; the admission test covers it.
    if live_major() == 18 {
        cases.extend([
            ("CREATE TABLE public.items(id integer,value integer,twice integer GENERATED ALWAYS AS (value*2) STORED); ALTER TABLE public.items REPLICA IDENTITY FULL", FULL),
            ("CREATE TABLE public.items(id integer,value integer,twice integer GENERATED ALWAYS AS (value*2) VIRTUAL); ALTER TABLE public.items REPLICA IDENTITY FULL", FULL),
        ]);
    }
    let mut wrong = Vec::new();
    for (ddl, refusal) in cases {
        let mut fixture = Fixture::new(ddl);
        match FollowSnapshot::create(
            &fixture.options,
            &fixture.plan,
            fixture.native.as_mut().unwrap(),
            &fixture.io,
        ) {
            Ok(_) => {
                wrong.push(format!("admitted: {ddl}"));
                continue;
            }
            Err(error) if !error.message.contains(refusal) => {
                wrong.push(format!("{ddl}: {}", error.message));
            }
            Err(_) => {}
        }
        if source_resources(&mut fixture) != (Some(b"0".to_vec()), Some(b"0".to_vec()))
            || fixture
                .native
                .as_ref()
                .unwrap()
                .checked_publication()
                .unwrap()
                .is_some()
        {
            wrong.push(format!("resources created: {ddl}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_generated_columns_publish_only_writable_columns() {
    use crate::cdc::contracts::{Column, Event, FollowMessage, OldTupleKind};
    use crate::contracts::OwnedCopyTable;
    let major = live_major();
    // Generated columns sit between writable ones, so a positional mapping would shift `b`.
    let mut ddl = String::from(if major == 18 {
        "CREATE TABLE public.items(id integer PRIMARY KEY,a integer,g integer GENERATED ALWAYS AS (a*2) STORED,v integer GENERATED ALWAYS AS (a*3) VIRTUAL,b text)"
    } else {
        "CREATE TABLE public.items(id integer PRIMARY KEY,a integer,g integer GENERATED ALWAYS AS (a*2) STORED,b text)"
    });
    if major == 16 {
        ddl.push_str("; CREATE TABLE public.full_items(id integer,a integer,g integer GENERATED ALWAYS AS (a*2) STORED,b text); ALTER TABLE public.full_items REPLICA IDENTITY FULL");
    }
    let mut fixture = Fixture::new(&ddl);
    let io = fixture.io.clone();
    assert_eq!(fixture.plan.tables.len(), if major == 16 { 2 } else { 1 });
    for table in &fixture.plan.tables {
        assert_eq!(table.writable_columns, ["id", "a", "b"]);
        assert_eq!(
            table.all_columns,
            if table.name.table == "items" && major == 18 {
                &["id", "a", "g", "v", "b"][..]
            } else {
                &["id", "a", "g", "b"][..]
            }
        );
    }
    let snapshot = fixture.create();
    let binding = snapshot.binding().clone();
    catalog::check_follow_health(
        &mut fixture.source,
        &fixture.plan,
        &binding,
        Some(snapshot.exporter_pid()),
        &io,
    )
    .unwrap();
    snapshot.finish(&io).unwrap();
    let mut dml = vec![
        "INSERT INTO public.items(id,a,b) VALUES (1,10,'x'),(2,20,'y')",
        "UPDATE public.items SET a=11 WHERE id=1",
        "DELETE FROM public.items WHERE id=2",
    ];
    if major == 16 {
        dml.extend([
            "INSERT INTO public.full_items(id,a,b) VALUES (1,10,'x'),(1,10,'x')",
            "UPDATE public.full_items SET b='y' WHERE ctid=(SELECT ctid FROM public.full_items LIMIT 1)",
            "DELETE FROM public.full_items WHERE b='y'",
        ]);
    }
    for statement in dml {
        fixture.source.query(statement, &io).unwrap();
    }
    let slot = fixture
        .source
        .quote_literal(binding.slot.intent.binding.slot_name())
        .unwrap();
    let publication = fixture
        .source
        .quote_literal(&binding.publication.name)
        .unwrap();
    let changes = fixture.source.query(&format!("SELECT pg_catalog.encode(data,'hex') FROM pg_catalog.pg_logical_slot_peek_binary_changes({slot},NULL,NULL,'proto_version','1','publication_names',{publication})"), &io).unwrap();
    let mut decoder = crate::cdc::pgoutput::Decoder::new(
        binding.stream(),
        &binding,
        crate::contracts::FollowLimits::default(),
    )
    .unwrap();
    let mut events = Vec::new();
    for row in 0..changes.rows() {
        let hex = changes.cell(row, 0).unwrap().unwrap();
        let bytes = (0..hex.len())
            .step_by(2)
            .map(|at| {
                u8::from_str_radix(std::str::from_utf8(&hex[at..at + 2]).unwrap(), 16).unwrap()
            })
            .collect::<Vec<_>>();
        decoder
            .feed(&bytes, &mut |message| {
                if let FollowMessage::Event(event) = message {
                    events.push(event);
                }
                Ok(())
            })
            .unwrap();
    }
    assert!(decoder.is_idle());
    let mut relations = Vec::new();
    let (mut rows, mut full_old) = (0, 0);
    for event in &events {
        match event {
            Event::Relation { columns, .. } => {
                let names = columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>();
                assert_eq!(names, ["id", "a", "b"]);
                relations.push(event.clone());
            }
            Event::Insert { values, .. } => {
                assert_eq!(values.len(), 3);
                rows += 1;
            }
            Event::Update { old, new, .. } => {
                assert_eq!(new.len(), 3);
                if let Some(old) = old {
                    assert_eq!((old.kind, old.values.len()), (OldTupleKind::Full, 3));
                    full_old += 1;
                }
                rows += 1;
            }
            Event::Delete { old, .. } => {
                assert_eq!(old.values.len(), 3);
                full_old += usize::from(old.kind == OldTupleKind::Full);
                rows += 1;
            }
            _ => {}
        }
    }
    assert_eq!(relations.len(), fixture.plan.tables.len());
    assert_eq!((rows, full_old), if major == 16 { (8, 2) } else { (4, 0) });
    catalog::command(
        &mut fixture.source,
        "SET session_replication_role=replica",
        "SET",
        &io,
    )
    .unwrap();
    for relation in &relations {
        let Event::Relation {
            key,
            replica_identity,
            ..
        } = relation
        else {
            unreachable!()
        };
        let table = fixture
            .plan
            .tables
            .iter()
            .find(|table| table.source_oid == key.oid)
            .unwrap()
            .clone();
        let owner = fixture
            .source
            .query(
                &format!(
                    "SELECT relnamespace,relowner FROM pg_catalog.pg_class WHERE oid={}",
                    table.source_oid
                ),
                &io,
            )
            .unwrap();
        let number = |column| {
            std::str::from_utf8(owner.cell(0, column).unwrap().unwrap())
                .unwrap()
                .parse::<u32>()
                .unwrap()
        };
        let owned = OwnedCopyTable {
            target_oid: table.source_oid,
            target_namespace_oid: number(0),
            target_owner_oid: number(1),
            generation: fixture.plan.generation,
            table,
        };
        catalog::command(&mut fixture.source, "BEGIN", "BEGIN", &io).unwrap();
        let checked = catalog::preflight_follow_relation(
            &mut fixture.source,
            relation,
            &binding,
            &owned,
            &io,
        )
        .unwrap();
        assert_eq!(checked.target_type_oids, [23, 23, 25]);
        assert_eq!(checked.identity_modes, [0, 0, 0]);
        // publish_generated_columns=stored would send `g` here; apply must never receive it.
        let mut sent = relation.clone();
        if let Event::Relation { columns: sent, .. } = &mut sent {
            sent.insert(
                2,
                Column {
                    name: "g".into(),
                    type_oid: 23,
                    type_mod: -1,
                    key: *replica_identity == b'f',
                },
            );
        }
        let error =
            catalog::preflight_follow_relation(&mut fixture.source, &sent, &binding, &owned, &io)
                .unwrap_err();
        assert!(error.message.contains("owned table plan"), "{error:?}");
        catalog::command(&mut fixture.source, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }
    let quoted = fixture
        .source
        .quote_identifier(&binding.publication.name)
        .unwrap();
    let health = |fixture: &mut Fixture| {
        catalog::check_follow_health(&mut fixture.source, &fixture.plan, &binding, None, &io)
    };
    health(&mut fixture).unwrap();
    if major == 18 {
        fixture
            .source
            .query(
                &format!("ALTER PUBLICATION {quoted} SET (publish_generated_columns=stored)"),
                &io,
            )
            .unwrap();
        let error = health(&mut fixture).unwrap_err();
        assert!(error.message.contains("options"), "{error:?}");
        fixture
            .source
            .query(
                &format!("ALTER PUBLICATION {quoted} SET (publish_generated_columns=none)"),
                &io,
            )
            .unwrap();
        health(&mut fixture).unwrap();
    }
    // A column list naming every writable column publishes the same attnames; it still refuses.
    fixture
        .source
        .query(
            &format!("ALTER PUBLICATION {quoted} DROP TABLE public.items"),
            &io,
        )
        .unwrap();
    fixture
        .source
        .query(
            &format!("ALTER PUBLICATION {quoted} ADD TABLE ONLY public.items (id,a,b)"),
            &io,
        )
        .unwrap();
    let error = health(&mut fixture).unwrap_err();
    assert!(error.message.contains("column lists"), "{error:?}");
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_publication_membership_and_type_map_are_exact() {
    let mut fixture = Fixture::new(
        "CREATE TYPE public.mood AS ENUM('calm','busy'); CREATE DOMAIN public.mood_domain AS public.mood; CREATE TABLE public.root(id integer PRIMARY KEY,mood public.mood_domain) PARTITION BY RANGE(id); CREATE TABLE public.leaf PARTITION OF public.root FOR VALUES FROM(0) TO(100)",
    );
    let snapshot = fixture.create();
    assert_eq!(fixture.plan.tables.len(), 1);
    assert_eq!(fixture.plan.tables[0].name.table, "leaf");
    let domain = fixture
        .source
        .query(
            "SELECT oid FROM pg_catalog.pg_type WHERE typname='mood_domain'",
            &fixture.io,
        )
        .unwrap();
    let oid = std::str::from_utf8(domain.cell(0, 0).unwrap().unwrap())
        .unwrap()
        .parse::<u32>()
        .unwrap();
    assert!(
        snapshot
            .binding()
            .source_types
            .iter()
            .any(|item| item.oid == oid && item.schema == "public" && item.name == "mood")
    );
    let publication = snapshot.binding().publication.name.clone();
    let binding = snapshot.binding().clone();
    snapshot.finish(&fixture.io).unwrap();
    let publication = fixture.source.quote_identifier(&publication).unwrap();
    fixture
        .source
        .query(
            &format!("ALTER PUBLICATION {} DROP TABLE public.leaf", publication),
            &fixture.io,
        )
        .unwrap();
    assert!(
        catalog::check_follow_health(
            &mut fixture.source,
            &fixture.plan,
            &binding,
            None,
            &fixture.io
        )
        .unwrap_err()
        .message
        .contains("membership")
    );
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_colliding_names_and_wrong_resource_oids_are_refused() {
    let mut fixture = Fixture::new("CREATE TABLE public.items(id integer PRIMARY KEY)");
    fixture.options.publication_name = "foreign_publication".into();
    fixture
        .source
        .query(
            "CREATE PUBLICATION foreign_publication FOR TABLE public.items",
            &fixture.io,
        )
        .unwrap();
    let error = FollowSnapshot::create(
        &fixture.options,
        &fixture.plan,
        fixture.native.as_mut().unwrap(),
        &fixture.io,
    )
    .err()
    .expect("collision must refuse");
    assert!(error.message.contains("never adopted"));
    assert!(
        fixture
            .native
            .as_ref()
            .unwrap()
            .checked_publication()
            .unwrap()
            .is_none()
    );
    fixture
        .source
        .query("DROP PUBLICATION foreign_publication", &fixture.io)
        .unwrap();
    fixture.options.publication_name.clear();
    let snapshot = fixture.create();
    let mut changed = snapshot.binding().clone();
    changed.publication.oid += 1;
    assert!(
        catalog::check_follow_health(
            &mut fixture.source,
            &fixture.plan,
            &changed,
            Some(snapshot.exporter_pid()),
            &fixture.io
        )
        .unwrap_err()
        .message
        .contains("replaced")
    );
    snapshot.finish(&fixture.io).unwrap();
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_relation_uses_target_type_oids_and_exact_owned_identity() {
    use crate::cdc::contracts::{Column, Event, RelationKey};
    use crate::contracts::OwnedCopyTable;
    let mut fixture = Fixture::new(
        "CREATE TYPE public.mood AS ENUM('calm','busy'); CREATE DOMAIN public.mood_domain AS public.mood; CREATE TABLE public.items(id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,mood public.mood_domain)",
    );
    let snapshot = fixture.create();
    let mut binding = snapshot.binding().clone();
    snapshot.finish(&fixture.io).unwrap();
    let table = fixture.plan.tables[0].clone();
    let attrs=fixture.source.query(&format!("SELECT a.attname,a.atttypid,a.atttypmod,c.relnamespace,c.relowner FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid WHERE a.attrelid={} AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum",table.source_oid),&fixture.io).unwrap();
    let number = |row, column| {
        std::str::from_utf8(attrs.cell(row, column).unwrap().unwrap())
            .unwrap()
            .parse::<u32>()
            .unwrap()
    };
    let target_types = vec![number(0, 1), number(1, 1)];
    let remote_domain = target_types[1].checked_add(100_000).unwrap();
    binding.source_types[0].oid = remote_domain;
    let owned = OwnedCopyTable {
        target_oid: table.source_oid,
        target_namespace_oid: number(0, 3),
        target_owner_oid: number(0, 4),
        generation: fixture.plan.generation,
        table: table.clone(),
    };
    let relation = Event::Relation {
        key: RelationKey {
            oid: table.source_oid,
            generation: 1,
        },
        schema: table.name.schema.clone(),
        name: table.name.table.clone(),
        columns: vec![
            Column {
                name: "id".into(),
                type_oid: 23,
                type_mod: -1,
                key: true,
            },
            Column {
                name: "mood".into(),
                type_oid: remote_domain,
                type_mod: -1,
                key: false,
            },
        ],
        replica_identity: b'd',
    };
    catalog::command(
        &mut fixture.source,
        "SET session_replication_role=replica",
        "SET",
        &fixture.io,
    )
    .unwrap();
    catalog::command(&mut fixture.source, "BEGIN", "BEGIN", &fixture.io).unwrap();
    let checked = catalog::preflight_follow_relation(
        &mut fixture.source,
        &relation,
        &binding,
        &owned,
        &fixture.io,
    )
    .unwrap();
    assert_eq!(checked.relation, relation);
    assert_eq!(checked.target_type_oids, target_types);
    assert_eq!(checked.identity_modes, vec![b'a', 0]);
    let mut wrong = owned.clone();
    wrong.target_namespace_oid += 1;
    assert!(
        catalog::preflight_follow_relation(
            &mut fixture.source,
            &relation,
            &binding,
            &wrong,
            &fixture.io
        )
        .unwrap_err()
        .message
        .contains("identity")
    );
    catalog::command(&mut fixture.source, "ROLLBACK", "ROLLBACK", &fixture.io).unwrap();
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_executed_expressions_refuse_before_resource_creation() {
    const CHECK: &str = "follow-execution: table/domain CHECK";
    const INDEX: &str = "follow-execution: require indexes";
    const PARTITION: &str = "follow-execution: partition keys";
    const POSITIVE: &str = "CREATE FUNCTION public.positive(integer) RETURNS boolean IMMUTABLE LANGUAGE sql AS 'SELECT $1>0'";
    let mut wrong = Vec::new();
    for (ddl, refusal) in [
        (format!("{POSITIVE}; CREATE TABLE public.items(id integer PRIMARY KEY CHECK(public.positive(id)))"), CHECK),
        ("CREATE DOMAIN public.positive AS integer; ALTER DOMAIN public.positive ADD CONSTRAINT checked CHECK(VALUE>0) NOT VALID; CREATE TABLE public.items(id integer PRIMARY KEY,value public.positive)".into(), CHECK),
        (format!("{POSITIVE}; CREATE DOMAIN public.checked AS integer CHECK(public.positive(VALUE)); CREATE TYPE public.pair AS(value public.checked); CREATE TABLE public.items(id integer PRIMARY KEY,value public.pair[])"), CHECK),
        ("CREATE TABLE public.items(id integer PRIMARY KEY,at timestamptz CHECK(at<=now()))".into(), CHECK),
        ("CREATE TABLE public.items(id integer PRIMARY KEY,at timestamptz CHECK(at::text<>''))".into(), CHECK),
        ("CREATE OPERATOR public.=== (FUNCTION=int4eq,LEFTARG=integer,RIGHTARG=integer); CREATE TABLE public.items(id integer PRIMARY KEY CHECK(id OPERATOR(public.===) id))".into(), CHECK),
        ("CREATE COLLATION public.casefold(provider=icu,locale='und-u-ks-level2',deterministic=false); CREATE TABLE public.items(id integer PRIMARY KEY,value text CHECK(value COLLATE public.casefold<>'x'))".into(), CHECK),
        ("CREATE TABLE public.items(id integer PRIMARY KEY,value integer,EXCLUDE USING btree(value WITH =))".into(), CHECK),
        ("CREATE FUNCTION public.twice(integer) RETURNS integer IMMUTABLE LANGUAGE sql AS 'SELECT $1*2'; CREATE TABLE public.items(id integer PRIMARY KEY,value integer); CREATE INDEX expression_idx ON public.items((public.twice(value)))".into(), INDEX),
        (format!("{POSITIVE}; CREATE TABLE public.items(id integer PRIMARY KEY,value integer); CREATE INDEX partial_idx ON public.items(value) WHERE public.positive(value)"), INDEX),
        ("CREATE OPERATOR CLASS public.int4_local FOR TYPE integer USING btree AS OPERATOR 1 <,OPERATOR 2 <=,OPERATOR 3 =,OPERATOR 4 >=,OPERATOR 5 >,FUNCTION 1 btint4cmp(integer,integer); CREATE TABLE public.items(id integer PRIMARY KEY,value integer); CREATE INDEX local_idx ON public.items(value public.int4_local)".into(), INDEX),
        ("CREATE COLLATION public.casefold(provider=icu,locale='und-u-ks-level2',deterministic=false); CREATE TABLE public.items(id text COLLATE public.casefold PRIMARY KEY)".into(), INDEX),
        ("CREATE TABLE public.root(id integer) PARTITION BY RANGE((id+1)); CREATE TABLE public.leaf PARTITION OF public.root FOR VALUES FROM(0) TO(100); ALTER TABLE public.leaf REPLICA IDENTITY FULL".into(), PARTITION),
        ("CREATE TABLE public.root(id text) PARTITION BY RANGE(id); CREATE TABLE public.leaf PARTITION OF public.root FOR VALUES FROM('a') TO('z'); ALTER TABLE public.leaf REPLICA IDENTITY FULL".into(), PARTITION),
    ] {
        let mut fixture = Fixture::new(&ddl);
        // Collect every case, so one run shows each refusal a mutation opens.
        match FollowSnapshot::create(
            &fixture.options,
            &fixture.plan,
            fixture.native.as_mut().unwrap(),
            &fixture.io,
        ) {
            Ok(_) => {
                wrong.push(format!("admitted: {ddl}"));
                continue;
            }
            Err(error) if !error.message.contains(refusal) => {
                wrong.push(format!("{}: {ddl}", error.message));
                continue;
            }
            Err(_) => {}
        }
        assert!(
            fixture
                .native
                .as_ref()
                .unwrap()
                .checked_publication()
                .unwrap()
                .is_none()
        );
        let resources=fixture.source.query("SELECT (SELECT count(*) FROM pg_catalog.pg_publication),(SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()))",&fixture.io).unwrap();
        assert_eq!(resources.cell(0, 0).unwrap(), Some(b"0".as_slice()));
        assert_eq!(resources.cell(0, 1).unwrap(), Some(b"0".as_slice()));
    }
    assert!(wrong.is_empty(), "unexpected follow admission: {wrong:#?}");
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_admits_validated_builtin_checks_expression_indexes_and_extension_opclasses() {
    // The objects of the operator's legacy.sql fixture, plus one CHECK over every admitted node shape.
    let mut fixture = Fixture::new(concat!(
        "CREATE EXTENSION citext; ",
        "CREATE DOMAIN public.e2e_percent AS numeric CHECK (VALUE >= 0 AND VALUE <= 100); ",
        "CREATE TABLE public.app_users(id integer PRIMARY KEY,email citext,display_name text,quota_percent public.e2e_percent,",
        "CHECK (CASE WHEN id IN (1,2) THEN coalesce(display_name,'x') COLLATE \"C\" <> '' ELSE id IS NOT NULL END),",
        "CHECK (ARRAY[id,1] IS NOT NULL AND display_name::varchar IN ('a','b') IS NOT TRUE AND (id > 0) IS NOT FALSE)); ",
        "ALTER TABLE public.app_users ADD CONSTRAINT app_users_email_key UNIQUE (email); ",
        "CREATE TABLE public.readings(id integer PRIMARY KEY,captured_at timestamptz,quantity integer); ",
        "CREATE TABLE public.documents(id integer PRIMARY KEY,title text); ",
        "CREATE INDEX app_users_name_lower_idx ON public.app_users (lower(display_name)); ",
        "CREATE INDEX readings_recent_partial_idx ON public.readings (captured_at) WHERE quantity > 5; ",
        "CREATE INDEX documents_title_lower_idx ON public.documents (lower(title))",
    ));
    assert_eq!(fixture.plan.tables.len(), 3);
    catalog::follow::preflight_follow(&mut fixture.source, &fixture.plan, &fixture.io).unwrap();
}

/// Pins citext's recorded version on the source and, when given, on the target, runs the follow
/// extension check, and rolls both pins back.
fn pinned_extension_check(
    source: &mut PgConnection,
    target: &mut PgConnection,
    tables: &[crate::contracts::CloneTable],
    io: &IoControl,
    versions: (&str, Option<&str>),
) -> Result<()> {
    let pin = |connection: &mut PgConnection, version: &str| {
        catalog::command(connection, "BEGIN", "BEGIN", io).unwrap();
        let updated = connection
            .query(&format!("UPDATE pg_catalog.pg_extension SET extversion='{version}' WHERE extname='citext' RETURNING 1"), io)
            .unwrap();
        assert_eq!(
            updated.rows(),
            1,
            "citext must be installed to pin {version}"
        );
    };
    pin(source, versions.0);
    if let Some(version) = versions.1 {
        pin(target, version);
    }
    let result = catalog::follow::require_follow_extensions(source, target, tables, io);
    catalog::command(source, "ROLLBACK", "ROLLBACK", io).unwrap();
    if versions.1.is_some() {
        catalog::command(target, "ROLLBACK", "ROLLBACK", io).unwrap();
    }
    result
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_extension_opclasses_require_the_source_version_or_an_update_path_on_the_target() {
    let mut fixture = Fixture::new(
        "CREATE EXTENSION citext; CREATE TABLE public.items(id integer PRIMARY KEY,email citext UNIQUE)",
    );
    let io = fixture.io.clone();
    let database = format!("{}_target", fixture.database);
    fixture
        .admin
        .query(&format!("CREATE DATABASE {database}"), &io)
        .unwrap();
    let admin = std::env::var("RPGMIGRATE_TEST_FOLLOW_SOURCE_ADMIN").unwrap();
    let info = Conninfo::new(format!("{admin} dbname={database}").into()).unwrap();
    let mut target = PgConnection::connect(&info, crate::pg::Role::Copy, &io).unwrap();
    let tables = fixture.plan.tables.clone();
    let default = target
        .query(
            "SELECT default_version FROM pg_catalog.pg_available_extensions WHERE name='citext'",
            &io,
        )
        .unwrap();
    let default = std::str::from_utf8(default.cell(0, 0).unwrap().unwrap())
        .unwrap()
        .to_owned();
    assert!(
        ["1.6", "1.8"].contains(&default.as_str()),
        "the 1.4 cases need a default updatable from 1.4, got {default}"
    );
    // Source 1.4 against the target default stands in for 16 to 18, where citext goes 1.6 to 1.8.
    // The 9.9 source and the 0.0 and 1.4 targets have no update path to them from the source.
    let cases = [
        (false, default.as_str(), None, true),
        (false, "1.4", None, true),
        (false, "9.9", None, false),
        (true, default.as_str(), None, true),
        (true, "1.4", None, true),
        (true, default.as_str(), Some("1.4"), false),
        (true, "1.4", Some("0.0"), false),
        (true, "9.9", None, false),
    ];
    let mut wrong = Vec::new();
    for (installed, source_version, target_version, admitted) in cases {
        if installed {
            target
                .query("CREATE EXTENSION IF NOT EXISTS citext", &io)
                .unwrap();
        }
        match pinned_extension_check(
            &mut fixture.source,
            &mut target,
            &tables,
            &io,
            (source_version, target_version),
        ) {
            Ok(()) if admitted => {}
            Err(error) if !admitted && error.message.contains("an update path from it") => {}
            other => wrong.push(format!(
                "installed={installed} source={source_version} target={target_version:?}: {other:?}"
            )),
        }
    }
    assert!(
        wrong.is_empty(),
        "unexpected extension admission: {wrong:#?}"
    );
    catalog::command(&mut fixture.source, "BEGIN", "BEGIN", &io).unwrap();
    fixture
        .source
        .query("UPDATE pg_catalog.pg_extension SET extname='rpgmigrate_absent_extension' WHERE extname='citext'", &io)
        .unwrap();
    let error =
        catalog::follow::require_follow_extensions(&mut fixture.source, &mut target, &tables, &io)
            .unwrap_err();
    assert!(
        error.message.contains("an update path from it"),
        "{error:?}"
    );
    catalog::command(&mut fixture.source, "ROLLBACK", "ROLLBACK", &io).unwrap();
    drop(target);
    fixture
        .admin
        .query(&format!("DROP DATABASE {database} WITH (FORCE)"), &io)
        .unwrap();
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_positive_publication_retry_requires_the_persisted_source_plan() {
    for confirmed in [false, true] {
        let mut fixture = Fixture::new("CREATE TABLE public.items(id integer PRIMARY KEY)");
        let state = fixture.native.as_mut().unwrap();
        state.prepare_follow_source(&fixture.plan).unwrap();
        let name = catalog::follow::resource_name(
            "publication",
            state.identity().run,
            fixture.plan.generation,
        )
        .unwrap();
        let intent = state.begin_publication(&name).unwrap();
        catalog::command(&mut fixture.source, "BEGIN", "BEGIN", &fixture.io).unwrap();
        catalog::command(&mut fixture.source,&format!("CREATE PUBLICATION {name} FOR TABLE ONLY public.items WITH(publish_via_partition_root=false)"),"CREATE PUBLICATION",&fixture.io).unwrap();
        let receipt =
            catalog::follow::publication_receipt(&mut fixture.source, &name, &fixture.io).unwrap();
        state.record_publication_created(&intent, &receipt).unwrap();
        assert!(matches!(
            fixture.source.commit_observed(&fixture.io).unwrap(),
            CommitObservation::Committed
        ));
        if confirmed {
            state.confirm_publication(&intent, &receipt).unwrap();
        }
        drop(fixture.native.take());
        fixture.options.clone.state.action = StateAction::Resume;
        fixture.native = Some(
            crate::state::open_follow(
                &fixture.options.clone.state,
                None,
                fixture.options.origin_name.as_deref(),
                &fixture.io,
            )
            .unwrap(),
        );
        let snapshot = FollowSnapshot::create(
            &fixture.options,
            &fixture.plan,
            fixture.native.as_mut().unwrap(),
            &fixture.io,
        );
        if confirmed {
            let snapshot = snapshot.unwrap();
            assert_eq!(snapshot.binding().publication, receipt);
            snapshot.finish(&fixture.io).unwrap();
        } else {
            // A committed but unconfirmed candidate is dropped by its recorded OID, never adopted.
            let snapshot = snapshot.unwrap();
            assert_eq!(snapshot.binding().publication.name, receipt.name);
            assert_ne!(snapshot.binding().publication.oid, receipt.oid);
            let slot = snapshot.receipt().intent.binding.slot_name().to_owned();
            snapshot.finish(&fixture.io).unwrap();
            fixture
                .source
                .query(
                    &format!("SELECT pg_catalog.pg_drop_replication_slot('{slot}')"),
                    &fixture.io,
                )
                .unwrap();
        }
    }
}

fn harness_context() -> serde_json::Value {
    let context =
        std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned follow harness required");
    serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap()
}

/// Relays the engine's sessions to the owned server. A held pattern pauses the first client chunk
/// that contains it until opened or another pattern is held. Frozen, nothing passes either way and every
/// server session stays open until release, which is how a stopped or killed engine loses a reply.
struct Relay {
    direct: Conninfo,
    directory: PathBuf,
    state: Arc<RelayState>,
    acceptor: Option<std::thread::JoinHandle<()>>,
}

#[derive(Default)]
struct RelayState {
    stop: AtomicBool,
    frozen: AtomicBool,
    hold: std::sync::Mutex<Option<&'static [u8]>>,
    hits: AtomicU64,
    creates: AtomicU64,
    pumps: std::sync::Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl Relay {
    fn start(fixture: &mut Fixture) -> Self {
        use std::os::unix::fs::DirBuilderExt;
        use std::os::unix::net::{UnixListener, UnixStream};
        let context = harness_context();
        let socket_dir = PathBuf::from(context["socket_dir"].as_str().unwrap());
        let port = context["port"].as_u64().unwrap();
        let directory = socket_dir.join(format!(
            "relay-{}-{}",
            std::process::id(),
            CASE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .unwrap();
        let listener = UnixListener::bind(directory.join(format!(".s.PGSQL.{port}"))).unwrap();
        listener.set_nonblocking(true).unwrap();
        let server = socket_dir.join(format!(".s.PGSQL.{port}"));
        let state = Arc::new(RelayState::default());
        let shared = Arc::clone(&state);
        let acceptor = std::thread::spawn(move || {
            while !shared.stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((client, _)) => {
                        client.set_nonblocking(false).unwrap();
                        let server = UnixStream::connect(&server).unwrap();
                        let mut pumps = shared.pumps.lock().unwrap();
                        for (from, to, upstream) in [
                            (
                                client.try_clone().unwrap(),
                                server.try_clone().unwrap(),
                                true,
                            ),
                            (server, client, false),
                        ] {
                            let shared = Arc::clone(&shared);
                            pumps.push(std::thread::spawn(move || {
                                relay_pump(from, to, upstream, &shared)
                            }));
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(5))
                    }
                    Err(error) => panic!("relay accept failed: {error}"),
                }
            }
        });
        let direct = fixture.options.clone.source.clone();
        let host = fixture
            .source
            .quote_literal(directory.to_str().unwrap())
            .unwrap();
        fixture.options.clone.source = Conninfo::new(
            format!(
                "{} host={host}",
                std::str::from_utf8(direct.as_c_str().to_bytes()).unwrap()
            )
            .into(),
        )
        .unwrap();
        Self {
            direct,
            directory,
            state,
            acceptor: Some(acceptor),
        }
    }

    fn hold(&self, pattern: &'static [u8]) {
        *self.state.hold.lock().unwrap() = Some(pattern);
    }

    fn open(&self) {
        *self.state.hold.lock().unwrap() = None;
    }

    fn freeze(&self) {
        self.state.frozen.store(true, Ordering::Release);
    }

    fn hits(&self) -> u64 {
        self.state.hits.load(Ordering::Acquire)
    }

    fn creates(&self) -> u64 {
        self.state.creates.load(Ordering::Acquire)
    }

    /// Closes every relayed session and points the fixture at the server again.
    fn release(self, fixture: &mut Fixture) {
        fixture.options.clone.source = self.direct.clone();
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Release);
        let acceptor = self.acceptor.take().unwrap().join();
        for pump in std::mem::take(&mut *self.state.pumps.lock().unwrap()) {
            let _ = pump.join();
        }
        let _ = std::fs::remove_dir_all(&self.directory);
        if let Err(panic) = acceptor {
            std::panic::resume_unwind(panic);
        }
    }
}

fn relay_pump(
    mut from: std::os::unix::net::UnixStream,
    mut to: std::os::unix::net::UnixStream,
    upstream: bool,
    state: &RelayState,
) {
    use std::io::{Read, Write};
    let contains = |seen: &[u8], pattern: &[u8]| seen.windows(pattern.len()).any(|w| w == pattern);
    from.set_read_timeout(Some(std::time::Duration::from_millis(20)))
        .unwrap();
    let (mut buffer, mut seen) = ([0; 16384], Vec::new());
    while !state.stop.load(Ordering::Acquire) {
        let size = match from.read(&mut buffer) {
            Ok(0) => {
                if !state.frozen.load(Ordering::Acquire) {
                    let _ = to.shutdown(std::net::Shutdown::Write);
                }
                return;
            }
            Ok(size) => size,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::Interrupted
                ) =>
            {
                continue;
            }
            Err(_) => return,
        };
        if upstream {
            seen.extend_from_slice(&buffer[..size]);
            let create = contains(&seen, b"CREATE_REPLICATION_SLOT");
            let held = (*state.hold.lock().unwrap()).filter(|pattern| contains(&seen, pattern));
            if create {
                state.creates.fetch_add(1, Ordering::AcqRel);
            }
            if held.is_some() {
                state.hits.fetch_add(1, Ordering::AcqRel);
                while *state.hold.lock().unwrap() == held && !state.stop.load(Ordering::Acquire) {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }
            // A tail shorter than either pattern still finds one split across two reads.
            let keep = if create || held.is_some() { 0 } else { 31 };
            seen.drain(..seen.len().saturating_sub(keep));
        }
        if !state.frozen.load(Ordering::Acquire) && to.write_all(&buffer[..size]).is_err() {
            return;
        }
    }
}

/// Binds a first generation and records its copy-phase restart; returns the next generation's
/// plan and the bound slot's name.
fn record_restart(fixture: &mut Fixture) -> (ClonePlan, String) {
    let mut plan = fixture.plan.clone();
    let mut snapshot = fixture.create();
    let binding = snapshot.binding().clone();
    let ticket = snapshot.snapshot_mut().ticket().clone();
    snapshot.finish(&fixture.io).unwrap();
    let native = fixture.native.as_mut().unwrap();
    native.bind_follow(&plan, &ticket, &binding).unwrap();
    native.begin_pre_data().unwrap();
    let (objects, tables) = synthetic_handoff(&plan);
    native.confirm_pre_data(&plan, objects, tables).unwrap();
    let recorded = native.begin_follow_restart(&binding, &plan).unwrap();
    plan.generation = recorded.next_generation;
    (plan, binding.slot.intent.binding.slot_name().to_owned())
}

#[derive(Clone, Copy, Debug)]
enum Window {
    /// A slot takes the name after the first collision check, before the publication exists.
    BeforeRecheck,
    /// A slot takes the name after the repeated check, before this run's CREATE reaches the server.
    BeforeCreate,
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_foreign_slot_inside_the_creation_window_is_refused_and_kept() {
    for restart in [false, true] {
        for window in [Window::BeforeRecheck, Window::BeforeCreate] {
            let case = format!("restart={restart} window={window:?}");
            let mut fixture = Fixture::new(
                "CREATE TABLE public.items(id integer PRIMARY KEY); INSERT INTO public.items SELECT generate_series(1,50)",
            );
            // The operator passes one stable --slot-name across runs and generations.
            fixture.options.slot_name = format!("{}_slot", fixture.database);
            let name = fixture.options.slot_name.clone();
            let literal = fixture.source.quote_literal(&name).unwrap();
            let identity = format!(
                "SELECT coalesce((SELECT plugin||' '||datoid||' '||restart_lsn||' '||confirmed_flush_lsn||' '||catalog_xmin FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}),'absent')"
            );
            let next = if restart {
                record_restart(&mut fixture).0
            } else {
                fixture.plan.clone()
            };
            let relay = Relay::start(&mut fixture);
            relay.hold(match window {
                Window::BeforeRecheck => b"CREATE PUBLICATION",
                Window::BeforeCreate => b"CREATE_REPLICATION_SLOT",
            });
            let options = fixture.options.clone();
            let io = fixture.io.clone();
            let native = fixture.native.as_mut().unwrap();
            let (error, foreign) = std::thread::scope(|scope| {
                let controller = scope.spawn(|| {
                    let mut session =
                        PgConnection::connect(&relay.direct, crate::pg::Role::Copy, &control())
                            .unwrap();
                    wait_until("the creation window was never reached", || {
                        relay.hits() == 1
                    });
                    scalar(
                        &mut session,
                        &format!(
                            "SELECT (pg_catalog.pg_create_logical_replication_slot({literal},'pgoutput')).slot_name::text"
                        ),
                    );
                    let foreign = scalar(&mut session, &identity);
                    relay.open();
                    foreign
                });
                let outcome = if restart {
                    FollowSnapshot::restart(&options, &next, native, &io)
                } else {
                    FollowSnapshot::create(&options, &next, native, &io)
                };
                let foreign = controller.join().unwrap();
                (outcome.err().expect(&case), foreign)
            });
            let (refusal, creates) = match window {
                Window::BeforeRecheck => (
                    "follow-source: existing slot or publication is never adopted".to_owned(),
                    0,
                ),
                Window::BeforeCreate => (
                    format!(
                        "follow-source: slot {name} already exists, so this run's CREATE created nothing; it is never adopted or dropped"
                    ),
                    1,
                ),
            };
            assert_eq!(error.message, refusal, "{case}");
            assert_eq!(relay.creates(), creates, "{case}");
            relay.release(&mut fixture);
            assert!(
                fixture
                    .native
                    .as_ref()
                    .unwrap()
                    .checked_source_slot()
                    .unwrap()
                    .is_none(),
                "{case}"
            );
            assert_eq!(scalar(&mut fixture.source, &identity), foreign, "{case}");
            let retry = |fixture: &mut Fixture| {
                let native = fixture.native.as_mut().unwrap();
                if restart {
                    FollowSnapshot::retire_unbound(&fixture.options, &next, native, &fixture.io)?;
                    FollowSnapshot::restart(&fixture.options, &next, native, &fixture.io)
                } else {
                    FollowSnapshot::create(&fixture.options, &next, native, &fixture.io)
                }
            };
            let error = retry(&mut fixture).err().expect(&case);
            assert_eq!(
                error.message,
                if restart {
                    "follow-restart: a slot reappeared after recorded absence"
                } else {
                    "follow-source: existing slot or publication is never adopted"
                },
                "{case}"
            );
            assert_eq!(scalar(&mut fixture.source, &identity), foreign, "{case}");
            // Once the other run's slot is gone, the next start creates this run's own.
            scalar(
                &mut fixture.source,
                &format!("SELECT pg_catalog.pg_drop_replication_slot({literal})::text"),
            );
            let snapshot =
                retry(&mut fixture).unwrap_or_else(|error| panic!("{case}: {}", error.message));
            assert_eq!(
                snapshot.receipt().intent.binding.slot_name(),
                name,
                "{case}"
            );
            assert_ne!(scalar(&mut fixture.source, &identity), foreign, "{case}");
            snapshot.finish(&fixture.io).unwrap();
            scalar(
                &mut fixture.source,
                &format!("SELECT pg_catalog.pg_drop_replication_slot({literal})::text"),
            );
        }
    }
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_restart_keeps_a_surviving_bound_slot_of_another_shape() {
    let major = live_major();
    // ALTER_REPLICATION_SLOT takes FAILOVER from PostgreSQL 17 on and TWO_PHASE from 18 on.
    for variant in ["plugin", "database", "two_phase", "failover"] {
        if (variant == "two_phase" && major < 18) || (variant == "failover" && major < 17) {
            continue;
        }
        let mut fixture = Fixture::new("CREATE TABLE public.items(id integer PRIMARY KEY)");
        let (next, name) = record_restart(&mut fixture);
        let literal = fixture.source.quote_literal(&name).unwrap();
        let copy = fixture
            .source
            .quote_literal(&format!("{name}_copy"))
            .unwrap();
        let shape = format!(
            "SELECT slot_type||' '||plugin||' '||datoid||' '||two_phase||' '||coalesce(pg_catalog.to_jsonb(s)->>'failover','-')||' '||coalesce(active_pid::text,'-') FROM pg_catalog.pg_replication_slots s WHERE slot_name={literal}"
        );
        let point = format!(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}"
        );
        let (recorded, at) = (
            scalar(&mut fixture.source, &shape),
            scalar(&mut fixture.source, &point),
        );
        // Each variant keeps the bound slot's confirmed_flush_lsn, so only its shape differs.
        if let Some(plugin) = match variant {
            "plugin" => Some("test_decoding"),
            "database" => Some("pgoutput"),
            _ => None,
        } {
            // A copy keeps confirmed_flush_lsn and lives in the copying session's database.
            let session = if variant == "database" {
                &mut fixture.admin
            } else {
                &mut fixture.source
            };
            for sql in [
                format!(
                    "SELECT pg_catalog.pg_copy_logical_replication_slot({literal},{copy},false,'{plugin}')::text"
                ),
                format!("SELECT pg_catalog.pg_drop_replication_slot({literal})::text"),
                format!(
                    "SELECT pg_catalog.pg_copy_logical_replication_slot({copy},{literal})::text"
                ),
                format!("SELECT pg_catalog.pg_drop_replication_slot({copy})::text"),
            ] {
                scalar(session, &sql);
            }
        } else {
            let mut walsender =
                PgConnection::connect_replication(&fixture.options.clone.source, &control())
                    .unwrap();
            walsender
                .logical_query(
                    &format!(
                        "ALTER_REPLICATION_SLOT \"{name}\" ({} true)",
                        variant.to_uppercase()
                    ),
                    &control(),
                )
                .unwrap();
        }
        let changed = scalar(&mut fixture.source, &shape);
        assert_ne!(changed, recorded, "{variant}");
        assert_eq!(scalar(&mut fixture.source, &point), at, "{variant}");
        let manifest_path = fixture.options.clone.state.path.join("manifest.json");
        let before = std::fs::read(&manifest_path).unwrap();
        let error = FollowSnapshot::restart(
            &fixture.options,
            &next,
            fixture.native.as_mut().unwrap(),
            &fixture.io,
        )
        .err()
        .expect(variant);
        assert_eq!(
            error.message,
            "follow-restart: surviving source slot does not match the inactive owned creation receipt",
            "{variant}"
        );
        assert_eq!(scalar(&mut fixture.source, &shape), changed, "{variant}");
        assert_eq!(scalar(&mut fixture.source, &point), at, "{variant}");
        assert_eq!(std::fs::read(&manifest_path).unwrap(), before, "{variant}");
        scalar(
            &mut fixture.source,
            &format!("SELECT pg_catalog.pg_drop_replication_slot({literal})::text"),
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum SlotKill {
    /// The creating walsender ended and dropped its unfinished slot.
    Absent,
    /// The creating walsender still waits, so the slot is active.
    Active,
    /// This run's own CREATE persisted the slot while a frozen relay lost its reply.
    Persisted,
    /// This run's own CREATE persisted the slot, then failed in SaveSlotToPath with SQLSTATE 58P02.
    PersistFailed,
    ForeignPlugin,
    Physical,
    /// Foreign pgoutput slots with an option, or in a database, that this run's CREATE never uses.
    TwoPhase,
    Failover,
    OtherDatabase,
}

fn scalar(connection: &mut PgConnection, sql: &str) -> String {
    let result = connection.query(sql, &control()).unwrap();
    assert_eq!(result.rows(), 1, "{sql}");
    String::from_utf8(result.cell(0, 0).unwrap().unwrap_or_default().to_vec()).unwrap()
}

fn control() -> IoControl {
    IoControl::unbounded(Arc::new(AtomicBool::new(false)))
}

fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while !ready() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

fn pending_slot(fixture: &Fixture) -> (String, serde_json::Value) {
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(fixture.options.clone.state.path.join("manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["source_slot"]["phase"], "provisioning");
    let intent = &manifest["source_slot"]["record"];
    (
        intent["binding"]["slot_name"].as_str().unwrap().to_owned(),
        intent["nonce"].clone(),
    )
}

/// Runs `engine` while a source transaction holds an xid, so CREATE_REPLICATION_SLOT waits, then
/// cancels it there as a process kill would; the hold is returned still open.
fn kill_inside_slot_creation(
    fixture: &mut Fixture,
    engine: impl FnOnce(&FollowOptions, &mut NativeState, &IoControl) -> Result<FollowSnapshot>,
) -> PgConnection {
    let mut hold = PgConnection::connect(
        &fixture.options.clone.source,
        crate::pg::Role::Copy,
        &control(),
    )
    .unwrap();
    hold.query("BEGIN", &control()).unwrap();
    hold.query("SELECT pg_catalog.pg_current_xact_id()", &control())
        .unwrap();
    let cancelled = Arc::new(AtomicBool::new(false));
    let io = IoControl::unbounded(Arc::clone(&cancelled));
    let options = fixture.options.clone();
    let info = fixture.options.clone.source.clone();
    let native = fixture.native.as_mut().unwrap();
    let outcome = std::thread::scope(|scope| {
        let controller = scope.spawn(move || {
            let mut observer = PgConnection::connect(&info, crate::pg::Role::Copy, &control()).unwrap();
            wait_until("slot creation never waited on the held xid", || {
                scalar(&mut observer, "SELECT count(*) FROM pg_catalog.pg_locks l JOIN pg_catalog.pg_stat_activity a USING (pid) WHERE l.locktype='transactionid' AND NOT l.granted AND a.backend_type='walsender' AND a.datname=pg_catalog.current_database()") == "1"
            });
            cancelled.store(true, Ordering::Release);
        });
        let outcome = engine(&options, native, &io);
        if let Err(panic) = controller.join() {
            std::panic::resume_unwind(panic);
        }
        outcome
    });
    let error = outcome
        .err()
        .expect("the killed slot creation returned a snapshot");
    assert!(
        error.message.starts_with("source-slot-create-unknown:"),
        "{}",
        error.message
    );
    hold
}

/// Runs `engine` while a held transaction ID makes CREATE_REPLICATION_SLOT wait, and runs `act` on
/// the controller's own session once that walsender waits; the hold ends when `act` returns.
fn during_slot_creation<T>(
    info: &Conninfo,
    act: impl FnOnce(&mut PgConnection) + Send,
    engine: impl FnOnce() -> T,
) -> T {
    let mut hold = PgConnection::connect(info, crate::pg::Role::Copy, &control()).unwrap();
    hold.query("BEGIN", &control()).unwrap();
    hold.query("SELECT pg_catalog.pg_current_xact_id()", &control())
        .unwrap();
    let pid = hold.backend_pid().unwrap();
    std::thread::scope(|scope| {
        let controller = scope.spawn(move || {
            let mut session = PgConnection::connect(info, crate::pg::Role::Copy, &control()).unwrap();
            wait_until("slot creation never waited on the held xid", || {
                scalar(&mut session, "SELECT count(*) FROM pg_catalog.pg_locks l JOIN pg_catalog.pg_stat_activity a USING (pid) WHERE l.locktype='transactionid' AND NOT l.granted AND a.backend_type='walsender' AND a.datname=pg_catalog.current_database()") == "1"
            });
            act(&mut session);
            scalar(
                &mut session,
                &format!("SELECT pg_catalog.pg_terminate_backend({pid})::text"),
            );
        });
        let outcome = engine();
        if let Err(panic) = controller.join() {
            std::panic::resume_unwind(panic);
        }
        outcome
    })
}

fn slot_present(session: &mut PgConnection, name: &str) -> bool {
    let literal = session.quote_literal(name).unwrap();
    scalar(
        session,
        &format!("SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}"),
    ) == "1"
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_this_runs_slot_is_retired_before_a_refusal_that_does_not_concern_it() {
    let extra = |session: &mut PgConnection| {
        session
            .query(
                "CREATE TABLE public.extra(id integer PRIMARY KEY)",
                &control(),
            )
            .unwrap();
    };
    let changed = "follow-source: slot snapshot table plan differs from preliminary admission";
    // A restart's receipt stays unbound when the snapshot refuses; its retry finds the pending publication replaced.
    let mut fixture = Fixture::new(
        "CREATE TABLE public.items(id integer PRIMARY KEY); INSERT INTO public.items SELECT generate_series(1,50)",
    );
    let (next, _) = record_restart(&mut fixture);
    let info = fixture.options.clone.source.clone();
    let (options, io) = (fixture.options.clone(), fixture.io.clone());
    let native = fixture.native.as_mut().unwrap();
    let error = during_slot_creation(&info, extra, || {
        FollowSnapshot::restart(&options, &next, native, &io)
    })
    .err()
    .expect("a changed snapshot inventory was admitted");
    assert_eq!(error.message, changed);
    let manifest = || -> serde_json::Value {
        serde_json::from_slice(
            &std::fs::read(fixture.options.clone.state.path.join("manifest.json")).unwrap(),
        )
        .unwrap()
    };
    let state = manifest();
    assert_eq!(state["source_slot"]["phase"], "confirmed");
    let name = state["source_slot"]["record"]["intent"]["binding"]["slot_name"]
        .as_str()
        .unwrap()
        .to_owned();
    let publication = fixture
        .source
        .quote_identifier(
            state["follow_state"]["publication"]["receipt"]["name"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    assert!(slot_present(&mut fixture.source, &name));
    for sql in [
        format!("DROP PUBLICATION {publication}"),
        format!("CREATE PUBLICATION {publication} FOR TABLE ONLY public.items"),
    ] {
        fixture.source.query(&sql, &fixture.io).unwrap();
    }
    let error = FollowSnapshot::retire_unbound(
        &fixture.options,
        &next,
        fixture.native.as_mut().unwrap(),
        &fixture.io,
    )
    .expect_err("a replaced pending publication was accepted");
    assert_eq!(
        error.message,
        "follow-restart: pending publication was replaced"
    );
    assert!(!slot_present(&mut fixture.source, &name));
    assert_eq!(manifest()["source_slot"], serde_json::Value::Null);

    // A first run's receipt stays unbound the same way; its retry plans the changed inventory in fresh().
    let pair = FreshPair::new(FRESH_SEED);
    let mut options = pair.options();
    options.slot_name = format!("{}_slot", pair.names[0]);
    let error = during_slot_creation(&pair.endpoints.source, extra, || {
        crate::follow::run(options.clone(), &control())
    })
    .expect_err("a changed snapshot inventory was admitted");
    assert_eq!(error.message, changed);
    assert_eq!(fresh_state(&options)["source_slot"]["phase"], "confirmed");
    let mut source =
        PgConnection::connect(&pair.endpoints.source, crate::pg::Role::Copy, &control()).unwrap();
    assert!(slot_present(&mut source, &options.slot_name));
    options.clone.state.action = StateAction::Resume;
    let error =
        crate::follow::run(options.clone(), &control()).expect_err("a changed plan was admitted");
    assert_eq!(
        error.message,
        "follow: preliminary plan differs from its native source intent"
    );
    assert!(!slot_present(&mut source, &options.slot_name));
    assert_eq!(
        fresh_state(&options)["source_slot"],
        serde_json::Value::Null
    );
}

/// The owned cluster's data directory, required to be the harness's own.
fn owned_data_directory(source: &mut PgConnection) -> PathBuf {
    let context = harness_context();
    let root = PathBuf::from(context["root"].as_str().unwrap());
    assert_eq!(
        std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
        context["token"].as_str().unwrap()
    );
    let expected = root.join("data").canonicalize().unwrap();
    let actual = PathBuf::from(scalar(
        source,
        "SELECT pg_catalog.current_setting('data_directory')",
    ))
    .canonicalize()
    .unwrap();
    assert_eq!(
        actual, expected,
        "the slot file belongs to the exact owned cluster"
    );
    expected
}

/// Lets this run's own CREATE_REPLICATION_SLOT persist its slot while the engine loses the outcome:
/// through a frozen relay, as a stopped process would, or by a server error after the persist.
/// Returns the persisted slot's confirmed_flush_lsn.
fn lose_own_create(
    fixture: &mut Fixture,
    kill: SlotKill,
    engine: impl FnOnce(&FollowOptions, &mut NativeState, &IoControl) -> Result<FollowSnapshot>,
) -> String {
    let relay = (kill == SlotKill::Persisted).then(|| Relay::start(fixture));
    let data = owned_data_directory(&mut fixture.source);
    let path = fixture.options.clone.state.path.join("manifest.json");
    let info = relay.as_ref().map_or_else(
        || fixture.options.clone.source.clone(),
        |r| r.direct.clone(),
    );
    let cancelled = Arc::new(AtomicBool::new(false));
    let io = IoControl::unbounded(Arc::clone(&cancelled));
    let options = fixture.options.clone();
    let native = fixture.native.as_mut().unwrap();
    let outcome = during_slot_creation(
        &info,
        |_| match &relay {
            Some(relay) => {
                relay.freeze();
                cancelled.store(true, Ordering::Release);
            }
            None => {
                let manifest: serde_json::Value =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                let name = manifest["source_slot"]["record"]["binding"]["slot_name"]
                    .as_str()
                    .unwrap();
                // SaveSlotToPath opens state.tmp with O_EXCL after the slot became persistent.
                std::fs::File::create(data.join("pg_replslot").join(name).join("state.tmp"))
                    .unwrap();
            }
        },
        || engine(&options, native, &io),
    );
    let error = outcome
        .err()
        .expect("a lost CREATE outcome returned a snapshot");
    assert!(
        error.message.starts_with("source-slot-create-unknown:"),
        "{}",
        error.message
    );
    let (name, _) = pending_slot(fixture);
    let literal = fixture.source.quote_literal(&name).unwrap();
    let state = format!(
        "SELECT coalesce((SELECT temporary||' '||active FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}),'absent')"
    );
    wait_until("this run's CREATE never persisted its slot", || {
        scalar(&mut fixture.source, &state) == "false false"
    });
    if let Some(relay) = relay {
        relay.release(fixture);
    }
    scalar(
        &mut fixture.source,
        &format!(
            "SELECT confirmed_flush_lsn::text FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}"
        ),
    )
}

/// Leaves the recorded slot in the state `kill` names and returns the hold, if still open.
fn shape_pending_slot(
    fixture: &mut Fixture,
    name: &str,
    kill: SlotKill,
    hold: PgConnection,
) -> Option<PgConnection> {
    let literal = fixture.source.quote_literal(name).unwrap();
    let state = format!(
        "SELECT coalesce((SELECT CASE WHEN active THEN 'active' ELSE 'inactive' END FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}),'absent')"
    );
    assert_eq!(scalar(&mut fixture.source, &state), "active");
    if kill == SlotKill::Active {
        return Some(hold);
    }
    // The terminated walsender drops its unfinished slot deterministically.
    scalar(
        &mut fixture.source,
        &format!(
            "SELECT pg_catalog.pg_terminate_backend(active_pid)::text FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}"
        ),
    );
    wait_until("the unfinished slot outlived its walsender", || {
        scalar(&mut fixture.source, &state) == "absent"
    });
    drop(hold);
    let create = match kill {
        SlotKill::Persisted | SlotKill::PersistFailed => {
            unreachable!("lose_own_create persists this run's own slot")
        }
        SlotKill::ForeignPlugin => {
            format!("pg_catalog.pg_create_logical_replication_slot({literal},'test_decoding')")
        }
        SlotKill::Physical => format!("pg_catalog.pg_create_physical_replication_slot({literal})"),
        SlotKill::TwoPhase => {
            format!(
                "pg_catalog.pg_create_logical_replication_slot({literal},'pgoutput',false,true)"
            )
        }
        SlotKill::Failover => format!(
            "pg_catalog.pg_create_logical_replication_slot({literal},'pgoutput',false,false,true)"
        ),
        SlotKill::OtherDatabase => {
            format!("pg_catalog.pg_create_logical_replication_slot({literal},'pgoutput')")
        }
        SlotKill::Absent | SlotKill::Active => return None,
    };
    let session = if kill == SlotKill::OtherDatabase {
        &mut fixture.admin
    } else {
        &mut fixture.source
    };
    scalar(session, &format!("SELECT ({create}).slot_name::text"));
    assert_eq!(scalar(&mut fixture.source, &state), "inactive");
    None
}

/// Internally consistent target ownership; the source snapshot under test never reads the target.
fn synthetic_handoff(
    plan: &ClonePlan,
) -> (
    Vec<crate::contracts::OwnedObject>,
    Vec<crate::contracts::OwnedCopyTable>,
) {
    plan.tables
        .iter()
        .zip(70_000..)
        .map(|(table, target_oid)| {
            (
                crate::contracts::OwnedObject {
                    class_oid: 1259,
                    object_oid: target_oid,
                    object_sub_id: 0,
                    namespace_oid: Some(2200),
                    namespace_name: Some(table.name.schema.clone()),
                    name: table.name.table.clone(),
                    kind: "r".into(),
                    owner_oid: 10,
                },
                crate::contracts::OwnedCopyTable {
                    target_oid,
                    target_namespace_oid: 2200,
                    target_owner_oid: 10,
                    generation: plan.generation,
                    table: table.clone(),
                },
            )
        })
        .unzip()
}

fn snapshot_rows_and_objects(snapshot: &mut FollowSnapshot) -> String {
    scalar(
        snapshot.snapshot_mut().connection().unwrap(),
        "SELECT (SELECT count(*)||'/'||count(DISTINCT id) FROM public.items)||' '||(SELECT count(*)||'/'||coalesce(md5(string_agg(oid::text||':'||md5(lo_get(oid)),',' ORDER BY oid)),'-') FROM pg_catalog.pg_largeobject_metadata)",
    )
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_pending_slot_intent_is_dropped_never_adopted_then_created_again() {
    let kills = [
        SlotKill::Absent,
        SlotKill::Active,
        SlotKill::Persisted,
        SlotKill::PersistFailed,
        SlotKill::ForeignPlugin,
        SlotKill::Physical,
        SlotKill::TwoPhase,
        SlotKill::Failover,
        SlotKill::OtherDatabase,
    ];
    // Slot failover exists from PostgreSQL 17 on.
    let failover = live_major() >= 17;
    for restart in [false, true] {
        for kill in kills {
            if kill == SlotKill::Failover && !failover {
                continue;
            }
            let case = format!("restart={restart} kill={kill:?}");
            let mut fixture = Fixture::new(
                "CREATE TABLE public.items(id integer PRIMARY KEY,payload text); INSERT INTO public.items SELECT g,'row-'||g FROM generate_series(1,300) g; SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode(repeat('a5',5000),'hex')); SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode('00ff','hex'))",
            );
            let expected = scalar(
                &mut fixture.source,
                "SELECT (SELECT count(*)||'/'||count(DISTINCT id) FROM public.items)||' '||(SELECT count(*)||'/'||coalesce(md5(string_agg(oid::text||':'||md5(lo_get(oid)),',' ORDER BY oid)),'-') FROM pg_catalog.pg_largeobject_metadata)",
            );
            assert!(expected.starts_with("300/300 2/"), "{expected}");
            let mut plan = fixture.plan.clone();
            let mut retired = Vec::new();
            if restart {
                let mut snapshot = fixture.create();
                let binding = snapshot.binding().clone();
                let ticket = snapshot.snapshot_mut().ticket().clone();
                snapshot.finish(&fixture.io).unwrap();
                retired.push(binding.slot.intent.binding.slot_name().to_owned());
                let native = fixture.native.as_mut().unwrap();
                native.bind_follow(&plan, &ticket, &binding).unwrap();
                native.begin_pre_data().unwrap();
                let (objects, tables) = synthetic_handoff(&plan);
                native.confirm_pre_data(&plan, objects, tables).unwrap();
                let recorded = native.begin_follow_restart(&binding, &plan).unwrap();
                plan.generation = recorded.next_generation;
            }
            let next = plan.clone();
            let engine = |options: &FollowOptions, native: &mut NativeState, io: &IoControl| {
                if restart {
                    FollowSnapshot::restart(options, &next, native, io)
                } else {
                    FollowSnapshot::create(options, &next, native, io)
                }
            };
            let own = matches!(kill, SlotKill::Persisted | SlotKill::PersistFailed);
            let (persisted, mut hold) = if own {
                (Some(lose_own_create(&mut fixture, kill, engine)), None)
            } else {
                let hold = kill_inside_slot_creation(&mut fixture, engine);
                let (name, _) = pending_slot(&fixture);
                (None, shape_pending_slot(&mut fixture, &name, kill, hold))
            };
            let (name, nonce) = pending_slot(&fixture);
            let manifest_path = fixture.options.clone.state.path.join("manifest.json");
            let manifest = || std::fs::read(&manifest_path).unwrap();
            let literal = fixture.source.quote_literal(&name).unwrap();
            let retry = |fixture: &mut Fixture| {
                let native = fixture.native.as_mut().unwrap();
                if restart {
                    FollowSnapshot::retire_unbound(&fixture.options, &next, native, &fixture.io)?;
                    FollowSnapshot::restart(&fixture.options, &next, native, &fixture.io)
                } else {
                    FollowSnapshot::create(&fixture.options, &next, native, &fixture.io)
                }
            };
            if kill != SlotKill::Absent && !own {
                let before = manifest();
                let slot = scalar(
                    &mut fixture.source,
                    &format!(
                        "SELECT slot_type||' '||coalesce(plugin,'-')||' '||coalesce(datoid::text,'-')||' '||two_phase||' '||coalesce(pg_catalog.to_jsonb(s)->>'failover','-')||' '||coalesce(active_pid::text,'-')||' '||coalesce(restart_lsn::text,'-')||' '||coalesce(catalog_xmin::text,'-') FROM pg_catalog.pg_replication_slots s WHERE slot_name={literal}"
                    ),
                );
                let error = retry(&mut fixture).err().expect(&case);
                let refusal = if kill == SlotKill::Active {
                    format!("follow-source: pending source slot {name} is active in backend ")
                } else {
                    format!(
                        "follow-source: slot {name} differs from this run's unbound pgoutput creation"
                    )
                };
                assert!(
                    error.message.starts_with(&refusal),
                    "{case}: {}",
                    error.message
                );
                assert_eq!(manifest(), before, "{case}");
                assert_eq!(
                    scalar(
                        &mut fixture.source,
                        &format!(
                            "SELECT slot_type||' '||coalesce(plugin,'-')||' '||coalesce(datoid::text,'-')||' '||two_phase||' '||coalesce(pg_catalog.to_jsonb(s)->>'failover','-')||' '||coalesce(active_pid::text,'-')||' '||coalesce(restart_lsn::text,'-')||' '||coalesce(catalog_xmin::text,'-') FROM pg_catalog.pg_replication_slots s WHERE slot_name={literal}"
                        )
                    ),
                    slot,
                    "{case}"
                );
                // Clear the hazard as an operator would, by exact name, outside the engine.
                if let Some(open) = hold.take() {
                    scalar(
                        &mut fixture.source,
                        &format!(
                            "SELECT pg_catalog.pg_terminate_backend(active_pid)::text FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}"
                        ),
                    );
                    drop(open);
                } else {
                    scalar(
                        &mut fixture.source,
                        &format!("SELECT pg_catalog.pg_drop_replication_slot({literal})::text"),
                    );
                }
                wait_until("the recorded slot remained", || {
                    scalar(
                        &mut fixture.source,
                        &format!(
                            "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}"
                        ),
                    ) == "0"
                });
            }
            let mut snapshot =
                retry(&mut fixture).unwrap_or_else(|error| panic!("{case}: {}", error.message));
            let receipt = snapshot.receipt().clone();
            assert_eq!(receipt.intent.binding.slot_name(), name, "{case}");
            assert_eq!(
                snapshot.binding().copy_generation,
                next.generation,
                "{case}"
            );
            for sql in [
                "INSERT INTO public.items VALUES(301,'after')",
                "SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode('01','hex'))",
            ] {
                fixture.source.query(sql, &fixture.io).unwrap();
            }
            assert_eq!(snapshot_rows_and_objects(&mut snapshot), expected, "{case}");
            let point = format!(
                "{:X}/{:X}",
                receipt.consistent_point.0 >> 32,
                receipt.consistent_point.0 as u32
            );
            assert_eq!(
                scalar(
                    &mut fixture.source,
                    "SELECT string_agg(slot_name||' '||slot_type||' '||plugin||' '||temporary||' '||confirmed_flush_lsn,',') FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())"
                ),
                format!("{name} logical pgoutput false {point}"),
                "{case}"
            );
            for old in &retired {
                assert_ne!(old, &name, "{case}");
            }
            // The slot this run's CREATE persisted is dropped, never adopted: the new one starts later.
            if let Some(persisted) = &persisted {
                let (high, low) = persisted.split_once('/').unwrap();
                let old = (u64::from_str_radix(high, 16).unwrap() << 32)
                    | u64::from_str_radix(low, 16).unwrap();
                assert!(receipt.consistent_point.0 > old, "{case}: {persisted}");
            }
            let manifest: serde_json::Value = serde_json::from_slice(&manifest()).unwrap();
            assert_eq!(manifest["source_slot"]["phase"], "confirmed", "{case}");
            assert_ne!(
                manifest["source_slot"]["record"]["intent"]["nonce"], nonce,
                "{case}"
            );
            snapshot.finish(&fixture.io).unwrap();
            fixture
                .source
                .query(
                    &format!("SELECT pg_catalog.pg_drop_replication_slot({literal})"),
                    &fixture.io,
                )
                .unwrap();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum PublicationKill {
    /// The intent was recorded and its CREATE never committed.
    IntentAbsent,
    /// Another session created a publication under the intent's name afterwards.
    IntentForeign,
    /// The receipt was recorded and COMMIT never reached the server.
    ReceiptRolledBack,
    /// The receipt was recorded and COMMIT succeeded, but its reply was lost.
    ReceiptCommitted,
    /// The receipt was recorded and a publication of another OID took its name.
    ReceiptForeign,
    /// As `ReceiptCommitted`, but the publication's owner changed afterwards.
    ReceiptOwner,
    /// As `ReceiptCommitted`, but another session replaces it while the retry's DROP waits.
    ReplacedDuringRemoval,
    /// The engine's own COMMIT reached the server only after the engine stopped waiting for its reply.
    OwnCommitLost,
}

/// Reopens native state as the next process does after a kill.
fn reopen_native(fixture: &mut Fixture) {
    drop(fixture.native.take());
    fixture.options.clone.state.action = StateAction::Resume;
    fixture.native = Some(
        crate::state::open_follow(
            &fixture.options.clone.state,
            None,
            fixture.options.origin_name.as_deref(),
            &fixture.io,
        )
        .unwrap(),
    );
}

/// Runs `engine` through a relay that holds its publication COMMIT, cancels the engine there as a kill
/// would, then lets that COMMIT complete with no one left to read its reply. Returns the receipt's OID.
fn lose_own_publication_commit(
    fixture: &mut Fixture,
    engine: impl FnOnce(&FollowOptions, &mut NativeState, &IoControl) -> Result<FollowSnapshot>,
) -> Vec<String> {
    let relay = Relay::start(fixture);
    relay.hold(b"CREATE PUBLICATION");
    let cancelled = Arc::new(AtomicBool::new(false));
    let io = IoControl::unbounded(Arc::clone(&cancelled));
    let options = fixture.options.clone();
    let native = fixture.native.as_mut().unwrap();
    let outcome = std::thread::scope(|scope| {
        let controller = scope.spawn(|| {
            wait_until("the publication CREATE was never held", || {
                relay.hits() == 1
            });
            relay.hold(b"COMMIT");
            wait_until("the publication COMMIT was never held", || {
                relay.hits() == 2
            });
            cancelled.store(true, Ordering::Release);
        });
        let outcome = engine(&options, native, &io);
        if let Err(panic) = controller.join() {
            std::panic::resume_unwind(panic);
        }
        outcome
    });
    let error = outcome
        .err()
        .expect("a lost publication COMMIT returned a snapshot");
    assert_eq!(
        error.message,
        "follow-source-create-unknown: publication COMMIT was not confirmed; the next start drops it by its recorded OID and owner, then creates it again"
    );
    relay.open();
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(fixture.options.clone.state.path.join("manifest.json")).unwrap(),
    )
    .unwrap();
    let publication = &manifest["follow_state"]["publication"];
    assert_eq!(publication["phase"], "created");
    let oid = publication["receipt"]["oid"].to_string();
    wait_until("the held publication COMMIT never completed", || {
        scalar(
            &mut fixture.source,
            &format!("SELECT count(*) FROM pg_catalog.pg_publication WHERE oid={oid}"),
        ) == "1"
    });
    relay.release(fixture);
    reopen_native(fixture);
    vec![oid]
}

/// Leaves this run's publication CREATE where `kill` names and returns every OID it saw under `name`.
fn interrupt_publication_create(
    fixture: &mut Fixture,
    name: &str,
    kill: PublicationKill,
) -> Vec<String> {
    let native = fixture.native.as_mut().unwrap();
    let intent = native.begin_publication(name).unwrap();
    let mut seen = Vec::new();
    let create = format!(
        "CREATE PUBLICATION {name} FOR TABLE ONLY public.items WITH(publish_via_partition_root=false)"
    );
    if kill != PublicationKill::IntentAbsent && kill != PublicationKill::IntentForeign {
        catalog::command(&mut fixture.source, "BEGIN", "BEGIN", &fixture.io).unwrap();
        catalog::command(
            &mut fixture.source,
            &create,
            "CREATE PUBLICATION",
            &fixture.io,
        )
        .unwrap();
        let receipt =
            catalog::follow::publication_receipt(&mut fixture.source, name, &fixture.io).unwrap();
        native
            .record_publication_created(&intent, &receipt)
            .unwrap();
        seen.push(receipt.oid.to_string());
        if matches!(
            kill,
            PublicationKill::ReceiptCommitted
                | PublicationKill::ReplacedDuringRemoval
                | PublicationKill::ReceiptOwner
        ) {
            assert!(matches!(
                fixture.source.commit_observed(&fixture.io).unwrap(),
                CommitObservation::Committed
            ));
        } else {
            catalog::command(&mut fixture.source, "ROLLBACK", "ROLLBACK", &fixture.io).unwrap();
        }
    }
    if kill == PublicationKill::ReceiptOwner {
        fixture
            .source
            .query(
                &format!("ALTER PUBLICATION {name} OWNER TO e1_source"),
                &fixture.io,
            )
            .unwrap();
    }
    if matches!(
        kill,
        PublicationKill::IntentForeign | PublicationKill::ReceiptForeign
    ) {
        fixture.source.query(&create, &fixture.io).unwrap();
        seen.push(scalar(
            &mut fixture.source,
            &format!("SELECT oid::text FROM pg_catalog.pg_publication WHERE pubname='{name}'"),
        ));
    }
    reopen_native(fixture);
    seen
}

/// Runs `engine` while another session holds the lock of publication `name`; once the engine's DROP
/// waits on it, that session replaces the publication under the same name and commits. Returns the
/// engine's outcome and the replacement's OID.
fn replace_publication_during_drop<T>(
    info: &Conninfo,
    name: &str,
    engine: impl FnOnce() -> T,
) -> (T, String) {
    let (ready, held) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let controller = scope.spawn(move || {
            let connect = || PgConnection::connect(info, crate::pg::Role::Copy, &control()).unwrap();
            let (mut holder, mut observer) = (connect(), connect());
            holder.query("BEGIN", &control()).unwrap();
            // A comment holds the publication's object lock and no table lock, so only the engine's DROP waits.
            holder.query(&format!("COMMENT ON PUBLICATION {name} IS 'held'"), &control()).unwrap();
            ready.send(()).unwrap();
            wait_until("the engine's DROP never waited on the publication lock", || {
                scalar(&mut observer, "SELECT count(*) FROM pg_catalog.pg_locks WHERE locktype='object' AND classid='pg_catalog.pg_publication'::pg_catalog.regclass AND NOT granted") == "1"
            });
            holder.query(&format!("DROP PUBLICATION {name}"), &control()).unwrap();
            holder.query(&format!("CREATE PUBLICATION {name} FOR TABLE ONLY public.items"), &control()).unwrap();
            let replaced = scalar(&mut holder, &format!("SELECT oid::text FROM pg_catalog.pg_publication WHERE pubname='{name}'"));
            holder.query("COMMIT", &control()).unwrap();
            replaced
        });
        if held.recv().is_err() {
            std::panic::resume_unwind(controller.join().unwrap_err());
        }
        let outcome = engine();
        let replaced = controller
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
        (outcome, replaced)
    })
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_unconfirmed_publication_is_retired_or_dropped_by_exact_oid_never_adopted() {
    let kills = [
        PublicationKill::IntentAbsent,
        PublicationKill::IntentForeign,
        PublicationKill::ReceiptRolledBack,
        PublicationKill::ReceiptCommitted,
        PublicationKill::ReceiptForeign,
        PublicationKill::ReceiptOwner,
        PublicationKill::ReplacedDuringRemoval,
        PublicationKill::OwnCommitLost,
    ];
    for restart in [false, true] {
        for kill in kills {
            let case = format!("restart={restart} kill={kill:?}");
            let mut fixture = Fixture::new(
                "CREATE TABLE public.items(id integer PRIMARY KEY,payload text); INSERT INTO public.items SELECT g,'row-'||g FROM generate_series(1,300) g; SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode(repeat('a5',5000),'hex')); SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode('00ff','hex'))",
            );
            let expected = scalar(
                &mut fixture.source,
                "SELECT (SELECT count(*)||'/'||count(DISTINCT id) FROM public.items)||' '||(SELECT count(*)||'/'||coalesce(md5(string_agg(oid::text||':'||md5(lo_get(oid)),',' ORDER BY oid)),'-') FROM pg_catalog.pg_largeobject_metadata)",
            );
            let mut next = fixture.plan.clone();
            if restart {
                let mut snapshot = fixture.create();
                let binding = snapshot.binding().clone();
                let ticket = snapshot.snapshot_mut().ticket().clone();
                snapshot.finish(&fixture.io).unwrap();
                let native = fixture.native.as_mut().unwrap();
                native.bind_follow(&next, &ticket, &binding).unwrap();
                native.begin_pre_data().unwrap();
                let (objects, tables) = synthetic_handoff(&next);
                native.confirm_pre_data(&next, objects, tables).unwrap();
                let recorded = native.begin_follow_restart(&binding, &next).unwrap();
                next.generation = recorded.next_generation;
                // The restart removes both old resources by their receipts before its own CREATE.
                let slot = fixture
                    .source
                    .quote_literal(binding.slot.intent.binding.slot_name())
                    .unwrap();
                scalar(
                    &mut fixture.source,
                    &format!("SELECT pg_catalog.pg_drop_replication_slot({slot})::text"),
                );
                native.confirm_follow_slot_absent(&recorded).unwrap();
                fixture
                    .source
                    .query(
                        &format!("DROP PUBLICATION {}", binding.publication.name),
                        &fixture.io,
                    )
                    .unwrap();
                native.confirm_follow_publication_absent(&recorded).unwrap();
            } else {
                fixture
                    .native
                    .as_mut()
                    .unwrap()
                    .prepare_follow_source(&next)
                    .unwrap();
            }
            let run = fixture.native.as_ref().unwrap().identity().run;
            let name = catalog::follow::resource_name("publication", run, next.generation).unwrap();
            let seen = if kill == PublicationKill::OwnCommitLost {
                lose_own_publication_commit(&mut fixture, |options, native, io| {
                    if restart {
                        FollowSnapshot::restart(options, &next, native, io)
                    } else {
                        FollowSnapshot::create(options, &next, native, io)
                    }
                })
            } else {
                interrupt_publication_create(&mut fixture, &name, kill)
            };
            let manifest_path = fixture.options.clone.state.path.join("manifest.json");
            let manifest = || std::fs::read(&manifest_path).unwrap();
            let before: serde_json::Value = serde_json::from_slice(&manifest()).unwrap();
            let pending = before["follow_state"]["publication"].clone();
            assert_ne!(pending["phase"], "confirmed", "{case}");
            let retry = |fixture: &mut Fixture| {
                let native = fixture.native.as_mut().unwrap();
                if restart {
                    FollowSnapshot::retire_unbound(&fixture.options, &next, native, &fixture.io)?;
                    FollowSnapshot::restart(&fixture.options, &next, native, &fixture.io)
                } else {
                    FollowSnapshot::create(&fixture.options, &next, native, &fixture.io)
                }
            };
            let publications = |fixture: &mut Fixture| {
                scalar(
                    &mut fixture.source,
                    "SELECT coalesce(string_agg(pubname||' '||oid,',' ORDER BY pubname),'none') FROM pg_catalog.pg_publication",
                )
            };
            if kill == PublicationKill::ReplacedDuringRemoval {
                let bytes = manifest();
                let info = fixture.options.clone.source.clone();
                let (outcome, replaced) =
                    replace_publication_during_drop(&info, &name, || retry(&mut fixture));
                let error = outcome.err().expect(&case);
                assert!(
                    error.message.starts_with(&format!(
                        "follow-source: publication {name} changed identity during its removal"
                    )),
                    "{case}: {}",
                    error.message
                );
                assert_ne!(replaced, seen[0], "{case}");
                assert_eq!(manifest(), bytes, "{case}");
                assert_eq!(
                    publications(&mut fixture),
                    format!("{name} {replaced}"),
                    "{case}"
                );
                fixture
                    .source
                    .query(&format!("DROP PUBLICATION {name}"), &fixture.io)
                    .unwrap();
                reopen_native(&mut fixture);
            }
            if matches!(
                kill,
                PublicationKill::IntentForeign
                    | PublicationKill::ReceiptForeign
                    | PublicationKill::ReceiptOwner
            ) {
                let foreign = publications(&mut fixture);
                let bytes = manifest();
                let error = retry(&mut fixture).err().expect(&case);
                let refusal = if kill == PublicationKill::IntentForeign {
                    format!(
                        "follow-source: publication {name} (OID {}) exists, but this run's CREATE under that name never committed; it is not proven this run's and is never adopted or dropped",
                        seen[0]
                    )
                } else {
                    let owner = scalar(
                        &mut fixture.source,
                        &format!(
                            "SELECT pubowner::text FROM pg_catalog.pg_publication WHERE pubname='{name}'"
                        ),
                    );
                    format!(
                        "follow-source: publication {name} (OID {}, owner {owner}) is not this run's unconfirmed creation (OID {}, owner {})",
                        seen.last().unwrap(),
                        seen[0],
                        pending["receipt"]["owner_oid"]
                    )
                };
                assert!(
                    error.message.starts_with(&refusal),
                    "{case}: {}",
                    error.message
                );
                assert_eq!(manifest(), bytes, "{case}");
                assert_eq!(publications(&mut fixture), foreign, "{case}");
                assert_eq!(
                    source_resources(&mut fixture).1.as_deref(),
                    Some(b"0".as_slice())
                );
                // Clear the hazard as an operator would, by exact name, outside the engine.
                fixture
                    .source
                    .query(&format!("DROP PUBLICATION {name}"), &fixture.io)
                    .unwrap();
                reopen_native(&mut fixture);
            }
            let snapshot =
                retry(&mut fixture).unwrap_or_else(|error| panic!("{case}: {}", error.message));
            let mut snapshot = snapshot;
            let publication = snapshot.binding().publication.clone();
            assert_eq!(publication.name, name, "{case}");
            for old in &seen {
                assert_ne!(&publication.oid.to_string(), old, "{case}: adopted");
            }
            assert_eq!(
                publications(&mut fixture),
                format!("{name} {}", publication.oid),
                "{case}"
            );
            for sql in [
                "INSERT INTO public.items VALUES(301,'after')",
                "SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode('01','hex'))",
            ] {
                fixture.source.query(sql, &fixture.io).unwrap();
            }
            assert_eq!(snapshot_rows_and_objects(&mut snapshot), expected, "{case}");
            let after: serde_json::Value = serde_json::from_slice(&manifest()).unwrap();
            assert_eq!(after["follow_state"]["publication"]["phase"], "confirmed");
            assert_ne!(
                after["follow_state"]["publication"]["intent"]["nonce"], pending["intent"]["nonce"],
                "{case}"
            );
            assert_eq!(
                source_resources(&mut fixture).1.as_deref(),
                Some(b"1".as_slice())
            );
            let slot = fixture
                .source
                .quote_literal(snapshot.receipt().intent.binding.slot_name())
                .unwrap();
            snapshot.finish(&fixture.io).unwrap();
            fixture
                .source
                .query(
                    &format!("SELECT pg_catalog.pg_drop_replication_slot({slot})"),
                    &fixture.io,
                )
                .unwrap();
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum RecordedPublication {
    /// A restart removes the bound generation's publication by its receipt.
    Restart,
    /// A restart retires the next generation's confirmed publication before its slot existed.
    Retire,
    /// Cleanup removes the bound publication.
    Cleanup,
    /// The shared removal itself, given a receipt.
    Removal,
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_recorded_publication_replaced_during_its_removal_is_refused_and_kept() {
    use RecordedPublication::*;
    for site in [Restart, Retire, Cleanup, Removal] {
        let case = format!("{site:?}");
        let mut fixture = Fixture::new(
            "CREATE TABLE public.items(id integer PRIMARY KEY); INSERT INTO public.items SELECT generate_series(1,50)",
        );
        let info = fixture.options.clone.source.clone();
        let (options, io) = (fixture.options.clone(), fixture.io.clone());
        let manifest_path = fixture.options.clone.state.path.join("manifest.json");
        let manifest = || std::fs::read(&manifest_path).unwrap();
        let publications = |fixture: &mut Fixture| {
            scalar(
                &mut fixture.source,
                "SELECT coalesce(string_agg(pubname||' '||oid,',' ORDER BY pubname),'none') FROM pg_catalog.pg_publication",
            )
        };
        let refusal = |context: &str, name: &str| {
            format!(
                "{context}: publication {name} changed identity during its removal; it is never adopted or dropped"
            )
        };
        let (error, expected, recorded, replaced) = match site {
            Restart => {
                let (next, _) = record_restart(&mut fixture);
                let native = fixture.native.as_mut().unwrap();
                let recorded = native
                    .checked_follow_restart()
                    .unwrap()
                    .unwrap()
                    .previous
                    .publication;
                let (outcome, replaced) =
                    replace_publication_during_drop(&info, &recorded.name, || {
                        FollowSnapshot::restart(&options, &next, native, &io)
                    });
                let restart = native.checked_follow_restart().unwrap().unwrap();
                assert!(restart.slot_absent && !restart.publication_absent, "{case}");
                let expected = refusal("follow-restart", &recorded.name);
                (outcome.err().expect(&case), expected, recorded, replaced)
            }
            Retire => {
                let (next, slot) = record_restart(&mut fixture);
                let native = fixture.native.as_mut().unwrap();
                let restart = native.checked_follow_restart().unwrap().unwrap();
                // The restart removed both old resources by their receipts, then confirmed its own publication.
                let slot = fixture.source.quote_literal(&slot).unwrap();
                scalar(
                    &mut fixture.source,
                    &format!("SELECT pg_catalog.pg_drop_replication_slot({slot})::text"),
                );
                native.confirm_follow_slot_absent(&restart).unwrap();
                fixture
                    .source
                    .query(
                        &format!("DROP PUBLICATION {}", restart.previous.publication.name),
                        &io,
                    )
                    .unwrap();
                native.confirm_follow_publication_absent(&restart).unwrap();
                let name = catalog::follow::resource_name(
                    "publication",
                    native.identity().run,
                    next.generation,
                )
                .unwrap();
                let intent = native.begin_publication(&name).unwrap();
                fixture
                    .source
                    .query(
                        &format!("CREATE PUBLICATION {name} FOR TABLE ONLY public.items"),
                        &io,
                    )
                    .unwrap();
                let recorded =
                    catalog::follow::publication_receipt(&mut fixture.source, &name, &io).unwrap();
                native
                    .record_publication_created(&intent, &recorded)
                    .unwrap();
                native.confirm_publication(&intent, &recorded).unwrap();
                let bytes = manifest();
                let (outcome, replaced) = replace_publication_during_drop(&info, &name, || {
                    FollowSnapshot::retire_unbound(&options, &next, native, &io)
                });
                assert_eq!(manifest(), bytes, "{case}");
                let expected = refusal("follow-restart", &name);
                (outcome.expect_err(&case), expected, recorded, replaced)
            }
            Cleanup => {
                let snapshot = fixture.create();
                let binding = snapshot.binding().clone();
                snapshot.finish(&fixture.io).unwrap();
                let plan = fixture.plan.clone();
                let source = &mut fixture.source;
                let (outcome, replaced) =
                    replace_publication_during_drop(&info, &binding.publication.name, || {
                        catalog::follow::remove_follow_publication(
                            source, &plan, &binding, false, &io,
                        )
                    });
                let expected =
                    "cleanup-source: publication was replaced; preserve it for manual inspection"
                        .to_owned();
                (
                    outcome.expect_err(&case),
                    expected,
                    binding.publication,
                    replaced,
                )
            }
            Removal => {
                let name = format!("{}_removal", fixture.database);
                fixture
                    .source
                    .query(
                        &format!("CREATE PUBLICATION {name} FOR TABLE ONLY public.items"),
                        &io,
                    )
                    .unwrap();
                let recorded =
                    catalog::follow::publication_receipt(&mut fixture.source, &name, &io).unwrap();
                let present = publications(&mut fixture);
                let (oid, owner) = (recorded.oid, recorded.owner_oid);
                for other in [
                    crate::contracts::PublicationReceipt {
                        owner_oid: owner + 1,
                        ..recorded.clone()
                    },
                    crate::contracts::PublicationReceipt {
                        oid: oid + 1,
                        ..recorded.clone()
                    },
                ] {
                    let error = catalog::follow::drop_recorded_publication(
                        &mut fixture.source,
                        &other,
                        "removal",
                        "unknown",
                        &io,
                    )
                    .expect_err("a publication of another identity was dropped");
                    assert_eq!(
                        error.message,
                        format!(
                            "removal: publication {name} (OID {oid}, owner {owner}) is not the recorded one (OID {}, owner {}); it is never adopted or dropped",
                            other.oid, other.owner_oid
                        )
                    );
                    assert_eq!(
                        fixture.source.transaction_status().unwrap(),
                        TransactionStatus::Idle
                    );
                    assert_eq!(publications(&mut fixture), present);
                }
                let source = &mut fixture.source;
                let (outcome, replaced) = replace_publication_during_drop(&info, &name, || {
                    catalog::follow::drop_recorded_publication(
                        source, &recorded, "removal", "unknown", &io,
                    )
                });
                assert_eq!(
                    fixture.source.transaction_status().unwrap(),
                    TransactionStatus::Idle
                );
                (
                    outcome.expect_err(&case),
                    refusal("removal", &name),
                    recorded,
                    replaced,
                )
            }
        };
        assert_eq!(error.message, expected, "{case}");
        assert_ne!(replaced, recorded.oid.to_string(), "{case}");
        assert_eq!(
            publications(&mut fixture),
            format!("{} {replaced}", recorded.name),
            "{case}"
        );
        if let Removal = site {
            let replacement =
                catalog::follow::publication_receipt(&mut fixture.source, &recorded.name, &io)
                    .unwrap();
            for dropped in [true, false] {
                assert_eq!(
                    catalog::follow::drop_recorded_publication(
                        &mut fixture.source,
                        &replacement,
                        "removal",
                        "unknown",
                        &io
                    )
                    .unwrap(),
                    dropped
                );
                assert_eq!(
                    fixture.source.transaction_status().unwrap(),
                    TransactionStatus::Idle
                );
                assert_eq!(publications(&mut fixture), "none");
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum ReceiptKill {
    /// The slot persisted at its recorded consistent point after the exporter ended.
    Persisted,
    /// Someone removed the slot after the receipt.
    Absent,
    /// A pgoutput slot under the name sits at another consistent point.
    Moved,
    /// A slot of another plugin took the name.
    ForeignPlugin,
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_unbound_first_run_slot_receipt_is_dropped_at_its_point_never_adopted() {
    for kill in [
        ReceiptKill::Persisted,
        ReceiptKill::Absent,
        ReceiptKill::Moved,
        ReceiptKill::ForeignPlugin,
    ] {
        let case = format!("kill={kill:?}");
        let mut fixture = Fixture::new(
            "CREATE TABLE public.items(id integer PRIMARY KEY,payload text); INSERT INTO public.items SELECT g,'row-'||g FROM generate_series(1,300) g; SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode(repeat('a5',5000),'hex')); SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode('00ff','hex'))",
        );
        let expected = scalar(
            &mut fixture.source,
            "SELECT (SELECT count(*)||'/'||count(DISTINCT id) FROM public.items)||' '||(SELECT count(*)||'/'||coalesce(md5(string_agg(oid::text||':'||md5(lo_get(oid)),',' ORDER BY oid)),'-') FROM pg_catalog.pg_largeobject_metadata)",
        );
        // The first run records its slot receipt and dies before it binds the snapshot.
        let snapshot = fixture.create();
        let old = snapshot.receipt().clone();
        drop(snapshot);
        let name = old.intent.binding.slot_name().to_owned();
        let literal = fixture.source.quote_literal(&name).unwrap();
        let shape = format!(
            "SELECT coalesce((SELECT slot_type||' '||coalesce(plugin,'-')||' '||coalesce(confirmed_flush_lsn::text,'-')||' '||active FROM pg_catalog.pg_replication_slots WHERE slot_name={literal}),'absent')"
        );
        let point = format!(
            "{:X}/{:X}",
            old.consistent_point.0 >> 32,
            old.consistent_point.0 as u32
        );
        wait_until("the exporter kept the recorded slot active", || {
            scalar(&mut fixture.source, &shape) == format!("logical pgoutput {point} false")
        });
        match kill {
            ReceiptKill::Persisted => {}
            ReceiptKill::Absent | ReceiptKill::Moved | ReceiptKill::ForeignPlugin => {
                scalar(
                    &mut fixture.source,
                    &format!("SELECT pg_catalog.pg_drop_replication_slot({literal})::text"),
                );
                if kill != ReceiptKill::Absent {
                    // WAL moves on, so a recreated slot cannot sit at the recorded point.
                    fixture
                        .source
                        .query("INSERT INTO public.items VALUES(0,'moved')", &fixture.io)
                        .unwrap();
                    fixture
                        .source
                        .query("DELETE FROM public.items WHERE id=0", &fixture.io)
                        .unwrap();
                    let plugin = if kill == ReceiptKill::Moved {
                        "pgoutput"
                    } else {
                        "test_decoding"
                    };
                    scalar(
                        &mut fixture.source,
                        &format!(
                            "SELECT (pg_catalog.pg_create_logical_replication_slot({literal},'{plugin}')).slot_name::text"
                        ),
                    );
                }
            }
        }
        reopen_native(&mut fixture);
        let manifest_path = fixture.options.clone.state.path.join("manifest.json");
        let manifest = || std::fs::read(&manifest_path).unwrap();
        let retry = |fixture: &mut Fixture| {
            FollowSnapshot::create(
                &fixture.options,
                &fixture.plan,
                fixture.native.as_mut().unwrap(),
                &fixture.io,
            )
        };
        if matches!(kill, ReceiptKill::Moved | ReceiptKill::ForeignPlugin) {
            let (bytes, slot) = (manifest(), scalar(&mut fixture.source, &shape));
            let error = retry(&mut fixture).err().expect(&case);
            assert!(
                error.message.starts_with(&format!(
                    "follow-source: slot {name} differs from this run's unbound pgoutput creation"
                )),
                "{case}: {}",
                error.message
            );
            assert_eq!(manifest(), bytes, "{case}");
            assert_eq!(scalar(&mut fixture.source, &shape), slot, "{case}");
            scalar(
                &mut fixture.source,
                &format!("SELECT pg_catalog.pg_drop_replication_slot({literal})::text"),
            );
            reopen_native(&mut fixture);
        }
        let mut snapshot =
            retry(&mut fixture).unwrap_or_else(|error| panic!("{case}: {}", error.message));
        let receipt = snapshot.receipt().clone();
        assert_eq!(receipt.intent.binding.slot_name(), name, "{case}");
        assert_ne!(receipt.intent.nonce, old.intent.nonce, "{case}");
        assert!(
            receipt.consistent_point.0 > old.consistent_point.0,
            "{case}: adopted"
        );
        assert_ne!(receipt.snapshot_token, old.snapshot_token, "{case}");
        for sql in [
            "INSERT INTO public.items VALUES(301,'after')",
            "SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode('01','hex'))",
        ] {
            fixture.source.query(sql, &fixture.io).unwrap();
        }
        assert_eq!(snapshot_rows_and_objects(&mut snapshot), expected, "{case}");
        let point = format!(
            "{:X}/{:X}",
            receipt.consistent_point.0 >> 32,
            receipt.consistent_point.0 as u32
        );
        assert_eq!(
            scalar(
                &mut fixture.source,
                "SELECT string_agg(slot_name||' '||plugin||' '||confirmed_flush_lsn,',') FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())"
            ),
            format!("{name} pgoutput {point}"),
            "{case}"
        );
        snapshot.finish(&fixture.io).unwrap();
        scalar(
            &mut fixture.source,
            &format!("SELECT pg_catalog.pg_drop_replication_slot({literal})::text"),
        );
    }
}

/// A source and a target database on the follow harness cluster, dropped with the case.
struct FreshPair {
    admin: PgConnection,
    names: [String; 2],
    endpoints: super::fresh_kill::Endpoints,
    root: PathBuf,
}

impl FreshPair {
    fn new(seed: &str) -> Self {
        let context: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned follow harness required"),
            )
            .unwrap(),
        )
        .unwrap();
        let root = PathBuf::from(context["root"].as_str().unwrap());
        assert_eq!(
            std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
            context["token"].as_str().unwrap()
        );
        let admin_info = std::env::var("RPGMIGRATE_TEST_FOLLOW_SOURCE_ADMIN")
            .expect("owned follow source required");
        let mut admin = PgConnection::connect(
            &Conninfo::new(admin_info.clone().into()).unwrap(),
            crate::pg::Role::Copy,
            &control(),
        )
        .unwrap();
        let case = CASE.fetch_add(1, Ordering::Relaxed);
        let names = ["s", "t"].map(|side| format!("m8_fresh_{side}_{}_{case}", std::process::id()));
        for name in &names {
            admin
                .query(&format!("CREATE DATABASE {name}"), &control())
                .unwrap();
        }
        let info =
            |name: &str| Conninfo::new(format!("{admin_info} dbname={name}").into()).unwrap();
        let endpoints = super::fresh_kill::Endpoints {
            source: info(&names[0]),
            target: info(&names[1]),
        };
        let mut source =
            PgConnection::connect(&endpoints.source, crate::pg::Role::Copy, &control()).unwrap();
        for statement in seed.split(';').filter(|s| !s.trim().is_empty()) {
            source.query(statement, &control()).unwrap();
        }
        Self {
            admin,
            names,
            endpoints,
            root,
        }
    }

    fn options(&self) -> FollowOptions {
        FollowOptions {
            clone: CloneOptions {
                source: self.endpoints.source.clone(),
                target: self.endpoints.target.clone(),
                state: StateRequest {
                    path: self.root.join(format!("{}-state", self.names[0])),
                    action: StateAction::Create,
                },
                operator: None,
                table_jobs: 2,
                post_data_jobs: 2,
                policy: RestorePolicy::default(),
                not_consistent: false,
                use_copy_binary: true,
                split_tables_larger_than: 0,
                split_max_parts: 8,
                skip_large_objects: false,
                large_objects_jobs: 2,
            },
            slot_name: String::new(),
            publication_name: String::new(),
            origin_name: None,
            apply_group_txns: 100,
            limits: FollowLimits::default(),
        }
    }

    fn data(&self, info: &Conninfo) -> String {
        scalar(
            &mut PgConnection::connect(info, crate::pg::Role::Copy, &control()).unwrap(),
            "SELECT (SELECT count(*)||'/'||count(DISTINCT id) FROM public.items)||' '||(SELECT count(*)||'/'||coalesce(md5(string_agg(oid::text||':'||md5(lo_get(oid)),',' ORDER BY oid)),'-') FROM pg_catalog.pg_largeobject_metadata)",
        )
    }
}

impl Drop for FreshPair {
    fn drop(&mut self) {
        // The case owns both fresh databases and every slot in them; nothing else is selected.
        let _ = self.admin.query(
            &format!("SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE database IN ('{}','{}') AND NOT active", self.names[0], self.names[1]),
            &control(),
        );
        for name in &self.names {
            let _ = self.admin.query(
                &format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
                &control(),
            );
        }
    }
}

const FRESH_SEED: &str = "CREATE TABLE public.items(id integer PRIMARY KEY,payload text); INSERT INTO public.items SELECT g,'row-'||g FROM generate_series(1,300) g; SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode(repeat('a5',5000),'hex')); SELECT pg_catalog.lo_from_bytea(0,pg_catalog.decode('00ff','hex'))";

/// The native manifest's copy phase, generation and target provenance phase.
fn fresh_state(options: &FollowOptions) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(options.clone.state.path.join("manifest.json")).unwrap())
        .unwrap()
}

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_fresh_bound_run_restarts_its_data_phase_with_every_row_once() {
    use super::fresh_kill::{FreshKill, follow_until_analyzing, kill_at, kill_in_publish};
    for (kill, publish) in [
        (FreshKill::Fence, false),
        (FreshKill::TargetIntent, false),
        (FreshKill::TargetCommitted, false),
        (FreshKill::Confirmed, false),
        (FreshKill::Confirmed, true),
    ] {
        let case = format!("kill={kill:?} publish={publish}");
        let pair = FreshPair::new(FRESH_SEED);
        let expected = pair.data(&pair.endpoints.source);
        assert!(expected.starts_with("300/300 2/"), "{expected}");
        let mut options = pair.options();
        let run = crate::state::open_follow(&options.clone.state, None, None, &control())
            .unwrap()
            .identity()
            .run;
        // The operator's first attempt restarts, and every later attempt resumes without consistency.
        options.clone.state.action = StateAction::Restart;
        kill_at(kill, &pair.endpoints, run, |io| {
            crate::follow::run(options.clone(), io)
        });
        let killed = fresh_state(&options);
        assert_eq!(killed["clone_state"]["phase"], "fresh", "{case}");
        assert_eq!(killed["clone_state"]["plan"]["generation"], 1, "{case}");
        let target = killed["target_provenance"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["phase"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>();
        let schema = super::fresh_kill::metadata_schema(run);
        let mut observer =
            PgConnection::connect(&pair.endpoints.target, crate::pg::Role::Copy, &control())
                .unwrap();
        let metadata =
            format!("SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname='{schema}'");
        match kill {
            FreshKill::Fence => assert!(target.is_empty(), "{case}"),
            FreshKill::TargetIntent => {
                assert_eq!(target, ["provisioning"], "{case}");
                assert_eq!(scalar(&mut observer, &metadata), "0", "{case}");
            }
            FreshKill::TargetCommitted => {
                assert_eq!(target, ["provisioning"], "{case}");
                wait_until("the held metadata COMMIT never landed", || {
                    scalar(&mut observer, &metadata) == "1"
                });
            }
            FreshKill::Confirmed => assert_eq!(target, ["confirmed"], "{case}"),
        }
        options.clone.state.action = StateAction::Resume;
        options.clone.not_consistent = true;
        let mut generation = 2;
        if publish {
            kill_in_publish(&pair.endpoints, run, |io| {
                crate::follow::run(options.clone(), io)
            });
            let interrupted = fresh_state(&options);
            assert_eq!(interrupted["clone_state"]["phase"], "fresh", "{case}");
            assert_eq!(
                interrupted["clone_state"]["plan"]["generation"], 2,
                "{case}"
            );
            assert_eq!(
                interrupted["follow_state"]["restart"]["new_bound"], true,
                "{case}"
            );
            generation = 3;
        }
        let _ = follow_until_analyzing(&pair.endpoints, run, |io| {
            crate::follow::run(options.clone(), io)
        });
        let finished = fresh_state(&options);
        assert_eq!(finished["clone_state"]["phase"], "analyzing", "{case}");
        assert_eq!(
            finished["clone_state"]["plan"]["generation"], generation,
            "{case}"
        );
        assert!(finished["follow_state"]["restart"].is_null(), "{case}");
        assert_eq!(pair.data(&pair.endpoints.target), expected, "{case}");
        let mut source =
            PgConnection::connect(&pair.endpoints.source, crate::pg::Role::Copy, &control())
                .unwrap();
        let resource =
            |kind| catalog::follow::resource_name(kind, run, CopyGeneration(generation)).unwrap();
        assert_eq!(
            scalar(
                &mut source,
                "SELECT coalesce(string_agg(slot_name,',' ORDER BY slot_name),'none') FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())"
            ),
            resource("slot"),
            "{case}"
        );
        assert_eq!(
            scalar(
                &mut source,
                "SELECT coalesce(string_agg(pubname,',' ORDER BY pubname),'none') FROM pg_catalog.pg_publication"
            ),
            resource("publication"),
            "{case}"
        );
    }
    // Anything the run did not create refuses and stays: a foreign target table in phase Fresh,
    // and source pre-data drift once tables were created and copying began.
    for drift in [false, true] {
        let case = format!("drift={drift}");
        let pair = FreshPair::new(FRESH_SEED);
        let expected = pair.data(&pair.endpoints.source);
        let mut options = pair.options();
        let run = crate::state::open_follow(&options.clone.state, None, None, &control())
            .unwrap()
            .identity()
            .run;
        options.clone.state.action = StateAction::Restart;
        let mut source =
            PgConnection::connect(&pair.endpoints.source, crate::pg::Role::Copy, &control())
                .unwrap();
        let mut target =
            PgConnection::connect(&pair.endpoints.target, crate::pg::Role::Copy, &control())
                .unwrap();
        let (change, undo, refusal) = if drift {
            let object = scalar(
                &mut source,
                "SELECT min(oid)::text FROM pg_catalog.pg_largeobject_metadata",
            );
            super::fresh_kill::kill_in_large_object(
                &pair.endpoints,
                object.parse().unwrap(),
                |io| crate::follow::run(options.clone(), io),
            );
            assert_eq!(fresh_state(&options)["clone_state"]["phase"], "copying");
            (
                "ALTER TABLE public.items ADD CONSTRAINT drift CHECK (id > 0)",
                "ALTER TABLE public.items DROP CONSTRAINT drift",
                "clone-recovery: pre-data schema drift prevents restart",
            )
        } else {
            super::fresh_kill::kill_at(
                super::fresh_kill::FreshKill::Fence,
                &pair.endpoints,
                run,
                |io| crate::follow::run(options.clone(), io),
            );
            (
                "CREATE TABLE public.items(id integer); INSERT INTO public.items VALUES(7)",
                "DROP TABLE public.items",
                "clone requires a collision-free target; existing objects are never adopted",
            )
        };
        let side = if drift { &mut source } else { &mut target };
        for statement in change.split("; ") {
            side.query(statement, &control()).unwrap();
        }
        let before = fresh_state(&options);
        options.clone.state.action = StateAction::Resume;
        options.clone.not_consistent = true;
        // A start that wrongly proceeds is cancelled once it streams, so it fails here instead of hanging.
        let error = super::fresh_kill::follow_until_analyzing(&pair.endpoints, run, |io| {
            crate::follow::run(options.clone(), io)
        })
        .expect_err(&case);
        assert!(error.message.contains(refusal), "{case}: {}", error.message);
        let after = fresh_state(&options);
        assert_eq!(after["clone_state"], before["clone_state"], "{case}");
        assert_eq!(after["follow_state"], before["follow_state"], "{case}");
        if !drift {
            assert_eq!(
                scalar(
                    &mut target,
                    "SELECT string_agg(id::text,',') FROM public.items"
                ),
                "7"
            );
        }
        let side = if drift { &mut source } else { &mut target };
        side.query(undo, &control()).unwrap();
        let _ = super::fresh_kill::follow_until_analyzing(&pair.endpoints, run, |io| {
            crate::follow::run(options.clone(), io)
        });
        let finished = fresh_state(&options);
        assert_eq!(finished["clone_state"]["phase"], "analyzing", "{case}");
        assert_eq!(finished["clone_state"]["plan"]["generation"], 2, "{case}");
        assert_eq!(pair.data(&pair.endpoints.target), expected, "{case}");
    }
}
