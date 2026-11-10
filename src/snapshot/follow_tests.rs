use super::*;
use crate::contracts::{
    CloneOptions, ClonePlan, CloneTarget, Conninfo, CopyGeneration, FollowLimits, FollowOptions,
    RestorePolicy, SnapshotTicket, StateAction, StateRequest,
};
use crate::pg::CommitObservation;
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
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!(
            "rpgmigrate-pending-source-{}-{}",
            std::process::id(),
            CASE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut native = crate::state::open_follow(
            &StateRequest {
                path: root.join("state"),
                action: StateAction::Create,
            },
            None,
            None,
            &IoControl::unbounded(Arc::new(AtomicBool::new(false))),
        )
        .unwrap();
        let mut plan = ClonePlan {
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
        };
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

    fn resources(
        native: &mut NativeState,
        plan: &ClonePlan,
        point: u64,
    ) -> crate::contracts::FollowBinding {
        native.prepare_follow_source(plan).unwrap();
        let intent = native.begin_publication("pending_source").unwrap();
        let publication = crate::contracts::PublicationReceipt {
            name: intent.name.clone(),
            oid: point as u32,
            owner_oid: 7,
        };
        native
            .record_publication_created(&intent, &publication)
            .unwrap();
        native.confirm_publication(&intent, &publication).unwrap();
        let slot_intent = native
            .begin_source_slot(
                &SourceSlotBinding::new(
                    crate::identity::StreamId {
                        run: native.identity().run,
                        source: plan.source.identity,
                        generation: 1,
                    },
                    "pending_source".into(),
                    crate::cdc::contracts::Plugin::Pgoutput,
                    plan.generation.0,
                )
                .unwrap(),
            )
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
        let mut proof = super::follow::PendingFollowSlotAbsence {
            restart: fixture.restart.clone(),
            receipt: fixture.binding.slot.clone(),
        };
        match field {
            0 => proof.receipt.intent.nonce[0] ^= 1,
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
            receipt: fixture.binding.slot.clone(),
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
        receipt: fixture.binding.slot.clone(),
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
        let mut admin = PgConnection::connect(&admin_info, &io).unwrap();
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
        let mut source = PgConnection::connect(&source_info, &io).unwrap();
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
    let mut importer = PgConnection::connect(&fixture.options.clone.source, &fixture.io).unwrap();
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
    let mut late = PgConnection::connect(&fixture.options.clone.source, &fixture.io).unwrap();
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

#[test]
#[ignore = "requires the disposable M3 PostgreSQL 16/18 harness"]
fn live_follow_generated_columns_refuse_before_resource_creation() {
    let mut fixture = Fixture::new(
        "CREATE TABLE public.items(id integer PRIMARY KEY,value integer GENERATED ALWAYS AS(id+1) STORED)",
    );
    let error = FollowSnapshot::create(
        &fixture.options,
        &fixture.plan,
        fixture.native.as_mut().unwrap(),
        &fixture.io,
    )
    .err()
    .expect("generated follow must refuse");
    assert!(error.message.contains("generated"));
    let resources=fixture.source.query("SELECT (SELECT count(*) FROM pg_catalog.pg_publication),(SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()))",&fixture.io).unwrap();
    assert_eq!(resources.cell(0, 0).unwrap(), Some(b"0".as_slice()));
    assert_eq!(resources.cell(0, 1).unwrap(), Some(b"0".as_slice()));
    assert!(
        fixture
            .native
            .as_ref()
            .unwrap()
            .checked_publication()
            .unwrap()
            .is_none()
    );
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
    for ddl in [
        "CREATE TABLE public.items(id integer PRIMARY KEY CHECK(id>0))",
        "CREATE DOMAIN public.positive AS integer CHECK(VALUE>0); CREATE TABLE public.items(id integer PRIMARY KEY,value public.positive)",
        "CREATE TABLE public.items(id integer PRIMARY KEY,value text); CREATE INDEX expression_idx ON public.items((lower(value)))",
        "CREATE TABLE public.items(id integer PRIMARY KEY,value text); CREATE INDEX partial_idx ON public.items(value) WHERE id>0",
        "CREATE TABLE public.root(id integer) PARTITION BY RANGE((id+1)); CREATE TABLE public.leaf PARTITION OF public.root FOR VALUES FROM(0) TO(100); ALTER TABLE public.leaf REPLICA IDENTITY FULL",
        "CREATE COLLATION public.casefold(provider=icu,locale='und-u-ks-level2',deterministic=false); CREATE TABLE public.items(id text COLLATE public.casefold PRIMARY KEY)",
        "CREATE TABLE public.root(id text) PARTITION BY RANGE(id); CREATE TABLE public.leaf PARTITION OF public.root FOR VALUES FROM('a') TO('z'); ALTER TABLE public.leaf REPLICA IDENTITY FULL",
    ] {
        let mut fixture = Fixture::new(ddl);
        let error = FollowSnapshot::create(
            &fixture.options,
            &fixture.plan,
            fixture.native.as_mut().unwrap(),
            &fixture.io,
        )
        .err()
        .expect("executed expression must refuse before resources");
        assert!(
            error.message.contains("follow-execution"),
            "wrong refusal class: {}",
            error.message
        );
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
            assert!(
                snapshot
                    .err()
                    .expect("unconfirmed candidate must refuse")
                    .message
                    .contains("unconfirmed")
            );
            let slots=fixture.source.query("SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE datoid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database())",&fixture.io).unwrap();
            assert_eq!(slots.cell(0, 0).unwrap(), Some(b"0".as_slice()));
        }
    }
}
