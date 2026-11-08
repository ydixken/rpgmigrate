use super::*;
use crate::contracts::{CloneSource, CloneTable, CloneTarget, QualifiedTable, RestorePolicy};
use crate::identity::SourceId;

fn control() -> IoControl {
    IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
        false,
    )))
}
fn plan() -> ClonePlan {
    ClonePlan {
        source: CloneSource {
            identity: SourceId {
                system_identifier: 1,
                database_oid: 2,
            },
            timeline: 1,
            major: 16,
        },
        target: CloneTarget {
            identity: SourceId {
                system_identifier: 3,
                database_oid: 4,
            },
            major: 18,
        },
        generation: CopyGeneration(1),
        tables: vec![CloneTable {
            source_oid: 5,
            name: QualifiedTable {
                schema: "public".into(),
                table: "items".into(),
            },
            writable_columns: vec!["id".into()],
            all_columns: vec!["id".into()],
        }],
        sequences: vec![],
        policy: RestorePolicy::default(),
    }
}
fn ticket(plan: &ClonePlan) -> SnapshotTicket {
    SnapshotTicket {
        token: "000001-00002-1".into(),
        source: plan.source.clone(),
        generation: plan.generation,
    }
}
fn inventory(plan: &ClonePlan) -> (Vec<OwnedObject>, Vec<OwnedCopyTable>) {
    (
        vec![OwnedObject {
            class_oid: 1259,
            object_oid: 6,
            object_sub_id: 0,
            namespace_oid: Some(2200),
            namespace_name: Some("public".into()),
            name: "items".into(),
            kind: "r".into(),
            owner_oid: 10,
        }],
        vec![OwnedCopyTable {
            table: plan.tables[0].clone(),
            target_oid: 6,
            target_namespace_oid: 2200,
            target_owner_oid: 10,
            generation: plan.generation,
        }],
    )
}
fn start() -> (super::super::tests::TempDir, NativeState, ClonePlan) {
    let tmp = super::super::tests::TempDir::new();
    let mut state = open_clone(
        &StateRequest {
            path: tmp.path(),
            action: StateAction::Create,
        },
        None,
        &control(),
    )
    .unwrap();
    let plan = plan();
    state.bind_clone(&plan, &ticket(&plan)).unwrap();
    (tmp, state, plan)
}
#[test]
fn marker_generations_cannot_reuse_snapshot_bound_owned_tables() {
    let (_tmp, mut state, plan) = start();
    state.begin_pre_data().unwrap();
    let (objects, tables) = inventory(&plan);
    state.confirm_pre_data(objects, tables).unwrap();
    let mut next = ticket(&plan);
    next.generation = CopyGeneration(2);
    next.token = "new".into();
    state.advance_copy_generation(&next).unwrap();
    assert_eq!(
        state.owned_copy_tables().unwrap()[0].generation,
        CopyGeneration(2)
    );
    assert!(state.advance_copy_generation(&next).is_err());
    state.mark_stage(ClonePhase::PostDataPending).unwrap();
    next.generation = CopyGeneration(3);
    assert!(state.advance_copy_generation(&next).is_err());
}
#[test]
fn unknown_predata_never_adopts_objects_or_skips_handoff() {
    let (tmp, mut state, plan) = start();
    state.begin_pre_data().unwrap();
    assert!(state.mark_stage(ClonePhase::Copying).is_err());
    assert!(state.mark_stage(ClonePhase::PostDataPending).is_err());
    let (objects, mut tables) = inventory(&plan);
    tables[0].target_oid = 99;
    assert!(state.confirm_pre_data(objects, tables).is_err());
    assert_eq!(
        state.checked_clone().unwrap().unwrap().phase,
        ClonePhase::PreDataPending
    );
    drop(state);
    let reopened = open_clone(
        &StateRequest {
            path: tmp.path(),
            action: StateAction::Resume,
        },
        None,
        &control(),
    )
    .unwrap();
    assert_eq!(
        reopened.checked_clone().unwrap().unwrap().phase,
        ClonePhase::PreDataPending
    );
}
#[test]
fn inspection_overlaps_owner_without_lock_or_write() {
    let (tmp, mut state, plan) = start();
    let binding = TargetBinding {
        stream: StreamId {
            run: state.identity.run,
            source: plan.source.identity,
            generation: 1,
        },
        target_system_identifier: Some(3),
        target_database_oid: 4,
        schema_name: "rpgmigrate_native".into(),
        table_name: "checkpoint".into(),
        owner_oid: 10,
        metadata_version: TARGET_METADATA_VERSION,
    };
    let intent = state.begin_target_provisioning(binding).unwrap();
    state
        .confirm_target_provisioning(ConfirmedProvenance {
            intent,
            schema_oid: 11,
            table_oid: 12,
            marker_table_oid: 13,
            marker_index_oid: 14,
        })
        .unwrap();
    let before = fs::read(tmp.path().join("manifest.json")).unwrap();
    let read = inspect_clone(
        &StateRequest {
            path: tmp.path(),
            action: StateAction::Resume,
        },
        None,
        &control(),
    )
    .unwrap();
    assert_eq!(read.clone_state.plan, plan);
    assert_eq!(
        read.provenance.intent.binding.stream.run,
        state.identity.run
    );
    assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
    assert!(
        open_clone(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Resume
            },
            None,
            &control()
        )
        .is_err()
    );
}
#[test]
fn clone_rejects_old_formats_unknown_fields_and_inventory_overflow() {
    let (tmp, state, _) = start();
    drop(state);
    let path = tmp.path().join("manifest.json");
    let bytes = fs::read(&path).unwrap();
    for version in [1, 2, 3, 4, 6] {
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["format_version"] = version.into();
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            open_clone(
                &StateRequest {
                    path: tmp.path(),
                    action: StateAction::Resume
                },
                None,
                &control()
            )
            .is_err()
        );
    }
    fs::write(path, bytes).unwrap();
    let mut plan = plan();
    plan.tables = vec![plan.tables[0].clone(); MAX_CLONE_TABLES + 1];
    assert!(
        CloneState {
            ticket: ticket(&plan),
            plan,
            phase: ClonePhase::Fresh,
            owned_objects: vec![],
            owned_tables: vec![],
            completion: None
        }
        .validate()
        .is_err()
    );
}

#[test]
fn operator_binding_preserves_all_four_values_before_resume() {
    let tmp = super::super::tests::TempDir::new();
    let operator = OperatorIdentity {
        migration_uid: "03030303-0303-0303-0303-030303030303".into(),
        run: RunId([3; 16]),
        workdir_id: "owned-workdir".into(),
        checkpoint_generation: 1,
    };
    let request = StateRequest {
        path: tmp.path(),
        action: StateAction::Create,
    };
    let mut state = open_clone(&request, Some(&operator), &control()).unwrap();
    let plan = plan();
    state.bind_clone(&plan, &ticket(&plan)).unwrap();
    drop(state);
    let before = fs::read(tmp.path().join("manifest.json")).unwrap();
    let request = StateRequest {
        path: tmp.path(),
        action: StateAction::Resume,
    };
    for field in 0..3 {
        let mut wrong = operator.clone();
        match field {
            0 => wrong.workdir_id.push('x'),
            1 => wrong.checkpoint_generation = 2,
            _ => {
                wrong.run = RunId([4; 16]);
                wrong.migration_uid = "04040404-0404-0404-0404-040404040404".into();
            }
        }
        assert!(open_clone(&request, Some(&wrong), &control()).is_err());
        assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
    }
    assert!(open_clone(&request, None, &control()).is_err());
    assert_eq!(
        open_clone(&request, Some(&operator), &control())
            .unwrap()
            .identity
            .run,
        operator.run
    );
}

#[test]
fn serialized_exact_inventory_has_a_one_mib_hard_bound() {
    let plan = plan();
    let (mut objects, tables) = inventory(&plan);
    for oid in 100..10_100 {
        objects.push(OwnedObject {
            class_oid: 1259,
            object_oid: oid,
            object_sub_id: 0,
            namespace_oid: Some(2200),
            namespace_name: Some("public".into()),
            name: format!("object_{oid:056}"),
            kind: "r".into(),
            owner_oid: 10,
        });
    }
    let value = CloneState {
        ticket: ticket(&plan),
        plan,
        phase: ClonePhase::Copying,
        owned_objects: objects,
        owned_tables: tables,
        completion: None,
    };
    assert!(value.validate().unwrap_err().message.contains("one MiB"));
}
