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
            identity_modes: vec![0],
            partition_identity_modes: None,
        }],
        sequences: vec![],
        policy: RestorePolicy::default(),
        copy_policy: crate::contracts::CloneCopyPolicy::default(),
        copy_tables: vec![crate::contracts::CloneTableCopy {
            source_oid: 5,
            format: crate::contracts::CopyFormat::Text,
            parts: vec![crate::contracts::CopyPart {
                number: 1,
                count: 1,
                start_block: 0,
                end_block: None,
            }],
        }],
    }
}
fn ticket(plan: &ClonePlan) -> SnapshotTicket {
    SnapshotTicket {
        token: "000001-00002-1".into(),
        source: plan.source.clone(),
        generation: plan.generation,
    }
}

#[test]
fn required_copy_profile_round_trips_separately_from_source_catalog() {
    let mut value = serde_json::to_value(plan()).unwrap();
    value["copy_policy"] = serde_json::json!({
        "use_copy_binary": true,
        "split_tables_larger_than": 536870912_u64,
        "split_max_parts": 8,
        "skip_large_objects": false,
    });
    value["copy_tables"] = serde_json::json!([{
        "source_oid": 5,
        "format": "binary",
        "parts": [{"number": 1, "count": 1, "start_block": 0, "end_block": null}],
    }]);
    let restored: ClonePlan = serde_json::from_value(value.clone()).unwrap();
    validate_plan(&restored).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), value);
    assert!(value["tables"][0].get("format").is_none());
}

fn split_plan() -> ClonePlan {
    let mut plan = plan();
    plan.copy_policy.split_tables_larger_than = 1;
    plan.copy_tables[0].parts = vec![
        crate::contracts::CopyPart {
            number: 1,
            count: 2,
            start_block: 0,
            end_block: Some(10),
        },
        crate::contracts::CopyPart {
            number: 2,
            count: 2,
            start_block: 10,
            end_block: None,
        },
    ];
    plan
}

#[test]
fn copy_profile_refuses_gaps_overlaps_incomplete_sets_and_policy_mismatch() {
    validate_plan(&split_plan()).unwrap();
    for variant in 0..13 {
        let mut plan = split_plan();
        match variant {
            0 => plan.copy_tables.clear(),
            1 => plan.copy_tables.push(plan.copy_tables[0].clone()),
            2 => plan.copy_tables[0].source_oid = 6,
            3 => plan.copy_tables[0].parts[0].start_block = 1,
            4 => plan.copy_tables[0].parts[0].end_block = Some(0),
            5 => plan.copy_tables[0].parts[0].end_block = None,
            6 => plan.copy_tables[0].parts[1].start_block = 9,
            7 => plan.copy_tables[0].parts[1].start_block = 11,
            8 => plan.copy_tables[0].parts[1].number = 1,
            9 => plan.copy_tables[0].parts[1].count = 3,
            10 => plan.copy_tables[0].parts[1].end_block = Some(11),
            11 => plan.copy_policy.split_tables_larger_than = 0,
            _ => plan.copy_tables[0].format = crate::contracts::CopyFormat::Binary,
        }
        assert!(validate_plan(&plan).is_err(), "variant {variant}");
    }
    for maximum in [0, 1, u32::MAX] {
        let mut plan = split_plan();
        plan.copy_policy.split_max_parts = maximum;
        assert!(validate_plan(&plan).is_err(), "maximum {maximum}");
    }
}

#[test]
fn copy_profile_requires_nullable_bounds_and_all_selected_policy_keys() {
    for key in ["copy_tables", "copy_policy"] {
        let mut value = serde_json::to_value(split_plan()).unwrap();
        value.as_object_mut().unwrap().remove(key);
        assert!(serde_json::from_value::<ClonePlan>(value).is_err(), "{key}");
    }
    for key in [
        "use_copy_binary",
        "split_tables_larger_than",
        "split_max_parts",
        "skip_large_objects",
    ] {
        let mut value = serde_json::to_value(split_plan()).unwrap();
        value["copy_policy"].as_object_mut().unwrap().remove(key);
        assert!(serde_json::from_value::<ClonePlan>(value).is_err(), "{key}");
    }
    let mut value = serde_json::to_value(split_plan()).unwrap();
    value["copy_tables"][0]["parts"][1]
        .as_object_mut()
        .unwrap()
        .remove("end_block");
    assert!(serde_json::from_value::<ClonePlan>(value).is_err());
}

#[test]
fn copy_profile_enforces_actual_part_bound_without_multiplying_selector_limits() {
    for count in [4096_u32, 4097] {
        let mut plan = split_plan();
        plan.copy_policy.split_max_parts = i32::MAX as u32;
        plan.copy_tables[0].parts = (0..count)
            .map(|index| crate::contracts::CopyPart {
                number: index + 1,
                count,
                start_block: index,
                end_block: (index + 1 < count).then_some(index + 1),
            })
            .collect();
        assert_eq!(validate_plan(&plan).is_ok(), count == 4096);
    }
}

#[test]
fn execution_entries_require_canonical_oid_order_even_when_catalog_order_differs() {
    let mut plan = plan();
    let mut second = plan.tables[0].clone();
    second.source_oid = 6;
    second.name.table = "other".into();
    plan.tables.insert(0, second);
    let mut second_copy = plan.copy_tables[0].clone();
    second_copy.source_oid = 6;
    plan.copy_tables.push(second_copy);
    validate_plan(&plan).unwrap();
    plan.copy_tables.reverse();
    assert!(validate_plan(&plan).is_err());
}

#[test]
fn same_generation_binding_cannot_change_copy_policy_or_resolved_profile() {
    let (_tmp, mut state, _) = start();
    let saved = state.checked_clone().unwrap().unwrap();
    for variant in 0..3 {
        let mut changed = saved.plan.clone();
        match variant {
            0 => changed.copy_policy.use_copy_binary = true,
            1 => changed.copy_policy.skip_large_objects = true,
            _ => changed.copy_policy.split_max_parts = 1,
        }
        assert!(state.bind_clone(&changed, &saved.ticket).is_err());
        assert_eq!(state.checked_clone().unwrap().unwrap(), saved);
    }
}

#[test]
fn identity_evidence_keys_are_required() {
    for key in ["identity_modes", "partition_identity_modes"] {
        let mut value = serde_json::to_value(&plan().tables[0]).unwrap();
        value.as_object_mut().unwrap().remove(key);
        assert!(
            serde_json::from_value::<CloneTable>(value).is_err(),
            "missing {key} must not mean no identity"
        );
    }
}

#[test]
fn identity_evidence_rejects_invalid_modes_and_widths() {
    for modes in [
        vec![],
        vec![0, 0],
        vec![1],
        vec![b'A'],
        vec![b'D'],
        vec![255],
    ] {
        for partition in [false, true] {
            let mut plan = plan();
            if partition {
                plan.tables[0].partition_identity_modes = Some(modes.clone());
            } else {
                plan.tables[0].identity_modes = modes.clone();
            }
            let error = validate_plan(&plan).unwrap_err();
            assert!(error.message.starts_with("state-clone:"), "{error:?}");
        }
    }
}

#[test]
fn identity_evidence_rejects_null_local_vectors_and_unknown_fields() {
    for (key, invalid) in [
        ("identity_modes", serde_json::Value::Null),
        ("identity_modes", serde_json::json!([-1])),
        ("identity_modes", serde_json::json!([256])),
        ("partition_identity_modes", serde_json::json!([256])),
        ("unknown_identity", serde_json::json!([0])),
    ] {
        let mut value = serde_json::to_value(&plan().tables[0]).unwrap();
        value[key] = invalid;
        assert!(
            serde_json::from_value::<CloneTable>(value).is_err(),
            "{key}"
        );
    }
    let table = plan().tables.remove(0);
    let value = serde_json::to_value(&table).unwrap();
    assert_eq!(
        value.get("partition_identity_modes"),
        Some(&serde_json::Value::Null)
    );
    assert_eq!(serde_json::from_value::<CloneTable>(value).unwrap(), table);
}

#[test]
fn identity_evidence_round_trips_exactly_through_owned_restart() {
    for (name, major, local, partition) in [
        ("ordinary", 16, vec![b'a', b'd', 0], None),
        ("inheritance_child", 16, vec![b'd', 0, b'a'], None),
        ("partition16", 16, vec![0, 0, 0], Some(vec![b'a', b'd', 0])),
        (
            "partition18",
            18,
            vec![b'a', b'd', 0],
            Some(vec![b'a', b'd', 0]),
        ),
        (
            "partition_no_identity",
            16,
            vec![0, 0, 0],
            Some(vec![0, 0, 0]),
        ),
        (
            "independent_leaf16",
            16,
            vec![b'd', 0, 0],
            Some(vec![b'a', b'd', 0]),
        ),
    ] {
        let tmp = super::super::tests::TempDir::new();
        let request = StateRequest {
            path: tmp.path(),
            action: StateAction::Create,
        };
        let mut state = open_clone(&request, None, &control()).unwrap();
        let mut plan = plan();
        plan.source.major = major;
        plan.target.major = major;
        plan.tables[0].all_columns = vec!["id".into(), "other_id".into(), "body".into()];
        plan.tables[0].writable_columns = plan.tables[0].all_columns.clone();
        plan.tables[0].identity_modes = local;
        plan.tables[0].partition_identity_modes = partition;
        state.bind_clone(&plan, &ticket(&plan)).unwrap();
        state.begin_pre_data().unwrap();
        let (objects, tables) = inventory(&plan);
        state
            .confirm_pre_data(&plan, objects, tables.clone())
            .unwrap();
        drop(state);
        let before = fs::read(tmp.path().join("manifest.json")).unwrap();
        for action in [StateAction::Resume, StateAction::Restart] {
            let state = open_clone(
                &StateRequest {
                    path: tmp.path(),
                    action,
                },
                None,
                &control(),
            )
            .unwrap();
            let saved = state.checked_clone().unwrap().unwrap();
            assert_eq!(saved.plan, plan, "{name}");
            assert_eq!(saved.owned_tables, tables, "{name}");
            assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
        }
    }
}

#[test]
fn ownership_and_existing_plan_require_both_identity_vectors_to_match() {
    let (tmp, mut state, plan) = start();
    let before = fs::read(tmp.path().join("manifest.json")).unwrap();
    for partition in [false, true] {
        let mut changed = plan.clone();
        if partition {
            changed.tables[0].partition_identity_modes = Some(vec![0]);
        } else {
            changed.tables[0].identity_modes = vec![b'a'];
        }
        let error = state.bind_clone(&changed, &ticket(&changed)).unwrap_err();
        assert!(error.message.starts_with("state-clone:"), "{error:?}");
        assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
    }
    state.begin_pre_data().unwrap();
    let before = fs::read(tmp.path().join("manifest.json")).unwrap();
    for partition in [false, true] {
        let (objects, mut tables) = inventory(&plan);
        if partition {
            tables[0].table.partition_identity_modes = Some(vec![0]);
        } else {
            tables[0].table.identity_modes = vec![b'd'];
        }
        let error = state.confirm_pre_data(&plan, objects, tables).unwrap_err();
        assert!(error.message.starts_with("state-ownership:"), "{error:?}");
        assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
    }
}

#[test]
fn malformed_persisted_identity_evidence_refuses_restart_without_mutation() {
    let (tmp, state, _) = start();
    drop(state);
    let path = tmp.path().join("manifest.json");
    let original: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    for (key, invalid) in [
        ("identity_modes", None),
        ("partition_identity_modes", None),
        ("identity_modes", Some(serde_json::json!([]))),
        ("identity_modes", Some(serde_json::json!([1]))),
        ("partition_identity_modes", Some(serde_json::json!([]))),
        ("partition_identity_modes", Some(serde_json::json!([1]))),
        ("unknown_identity", Some(serde_json::json!([0]))),
    ] {
        let mut value = original.clone();
        let table = value["clone_state"]["plan"]["tables"][0]
            .as_object_mut()
            .unwrap();
        match invalid {
            Some(invalid) => {
                table.insert(key.into(), invalid);
            }
            None => {
                table.remove(key);
            }
        }
        let before = serde_json::to_vec(&value).unwrap();
        fs::write(&path, &before).unwrap();
        for action in [StateAction::Resume, StateAction::Restart] {
            let error = open_clone(
                &StateRequest {
                    path: tmp.path(),
                    action,
                },
                None,
                &control(),
            )
            .unwrap_err();
            assert!(
                error.message.starts_with("state-invalid:")
                    || error.message.starts_with("state-clone:"),
                "{key}: {error:?}"
            );
            assert_eq!(fs::read(&path).unwrap(), before);
        }
    }
}

#[test]
fn native_six_is_refused_before_body_decoding_without_database_access() {
    let (tmp, state, _) = start();
    drop(state);
    let path = tmp.path().join("manifest.json");
    let before = br#"{"magic":"RPGMIGRATE","format_version":6,"clone_state":"invalid old body"}"#;
    fs::write(&path, before).unwrap();
    for action in [
        StateAction::Create,
        StateAction::Resume,
        StateAction::Restart,
    ] {
        let request = StateRequest {
            path: tmp.path(),
            action,
        };
        for error in [
            open_clone(&request, None, &control()).unwrap_err(),
            inspect_clone(&request, None, &control()).unwrap_err(),
        ] {
            assert!(error.message.starts_with("state-version:"), "{error:?}");
        }
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
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
    start_with_plan(plan())
}
fn start_with_plan(plan: ClonePlan) -> (super::super::tests::TempDir, NativeState, ClonePlan) {
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
    state.bind_clone(&plan, &ticket(&plan)).unwrap();
    (tmp, state, plan)
}

#[test]
fn pre_data_handoff_resolves_the_original_snapshot_profile_atomically_once() {
    let mut provisional = split_plan();
    provisional.copy_tables[0].parts = plan().copy_tables[0].parts.clone();
    let (tmp, mut state, _) = start_with_plan(provisional.clone());
    state.begin_pre_data().unwrap();
    let resolved = split_plan();
    let (objects, tables) = inventory(&resolved);
    state.confirm_pre_data(&resolved, objects, tables).unwrap();
    let saved = state.checked_clone().unwrap().unwrap();
    assert_eq!(saved.plan, resolved);
    assert_eq!(saved.ticket, ticket(&provisional));
    assert_eq!(saved.phase, ClonePhase::Copying);
    assert_eq!(saved.owned_tables.len(), 1);
    let before = fs::read(tmp.path().join("manifest.json")).unwrap();
    let (objects, tables) = inventory(&provisional);
    assert!(
        state
            .confirm_pre_data(&provisional, objects, tables)
            .is_err()
    );
    assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
}

#[test]
fn pre_data_profile_handoff_refuses_changed_bindings_without_mutation() {
    for field in 0..9 {
        let mut provisional = split_plan();
        provisional.copy_tables[0].parts = plan().copy_tables[0].parts.clone();
        let (tmp, mut state, _) = start_with_plan(provisional.clone());
        state.begin_pre_data().unwrap();
        let before = fs::read(tmp.path().join("manifest.json")).unwrap();
        let mut resolved = split_plan();
        match field {
            0 => resolved.source.timeline += 1,
            1 => resolved.target.identity.database_oid += 1,
            2 => resolved.generation = CopyGeneration(2),
            3 => resolved.policy.no_owner = true,
            4 => resolved.copy_policy.use_copy_binary = true,
            5 => resolved.copy_policy.skip_large_objects = true,
            6 => resolved.tables[0].source_oid += 1,
            7 => resolved.copy_tables[0].parts[1].start_block += 1,
            _ => resolved.sequences.push(QualifiedTable {
                schema: "public".into(),
                table: "new_sequence".into(),
            }),
        }
        let (objects, tables) = inventory(&provisional);
        assert!(
            state.confirm_pre_data(&resolved, objects, tables).is_err(),
            "field {field}"
        );
        assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
    }
}

#[test]
fn fresh_generation_replaces_bounds_but_preserves_selected_policy_and_owned_identity() {
    let (_tmp, mut state, previous) = start_with_plan(split_plan());
    state.begin_pre_data().unwrap();
    let (objects, tables) = inventory(&previous);
    state.confirm_pre_data(&previous, objects, tables).unwrap();
    let mut next = previous.clone();
    next.generation = CopyGeneration(2);
    next.copy_tables[0].parts[0].end_block = Some(20);
    next.copy_tables[0].parts[1].start_block = 20;
    let mut next_ticket = ticket(&next);
    next_ticket.token = "fresh-layout".into();
    let saved = state.checked_clone().unwrap().unwrap();
    let mut wrong_policy = next.clone();
    wrong_policy.copy_policy.use_copy_binary = true;
    assert!(
        state
            .advance_copy_generation(&wrong_policy, &next_ticket)
            .is_err()
    );
    assert_eq!(state.checked_clone().unwrap().unwrap(), saved);
    state.advance_copy_generation(&next, &next_ticket).unwrap();
    let current = state.checked_clone().unwrap().unwrap();
    assert_eq!(current.plan, next);
    assert_eq!(current.ticket, next_ticket);
    assert_eq!(current.owned_objects, saved.owned_objects);
    assert_eq!(
        current.owned_tables[0].target_oid,
        saved.owned_tables[0].target_oid
    );
    assert_eq!(current.owned_tables[0].generation, CopyGeneration(2));
    assert!(state.bind_clone(&previous, &ticket(&previous)).is_err());
}
#[test]
fn marker_generations_cannot_reuse_snapshot_bound_owned_tables() {
    let (_tmp, mut state, plan) = start();
    state.begin_pre_data().unwrap();
    let (objects, tables) = inventory(&plan);
    state.confirm_pre_data(&plan, objects, tables).unwrap();
    let mut next = ticket(&plan);
    next.generation = CopyGeneration(2);
    next.token = "new".into();
    state
        .advance_copy_generation(
            &ClonePlan {
                generation: next.generation,
                ..plan.clone()
            },
            &next,
        )
        .unwrap();
    assert_eq!(
        state.owned_copy_tables().unwrap()[0].generation,
        CopyGeneration(2)
    );
    assert!(
        state
            .advance_copy_generation(
                &ClonePlan {
                    generation: next.generation,
                    ..plan.clone()
                },
                &next
            )
            .is_err()
    );
    state.mark_stage(ClonePhase::PostDataPending).unwrap();
    next.generation = CopyGeneration(3);
    assert!(
        state
            .advance_copy_generation(
                &ClonePlan {
                    generation: next.generation,
                    ..plan.clone()
                },
                &next
            )
            .is_err()
    );
}
#[test]
fn unknown_predata_never_adopts_objects_or_skips_handoff() {
    let (tmp, mut state, plan) = start();
    state.begin_pre_data().unwrap();
    assert!(state.mark_stage(ClonePhase::Copying).is_err());
    assert!(state.mark_stage(ClonePhase::PostDataPending).is_err());
    let (objects, mut tables) = inventory(&plan);
    tables[0].target_oid = 99;
    assert!(state.confirm_pre_data(&plan, objects, tables).is_err());
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
            large_object_table_oid: 15,
            large_object_index_oid: 16,
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
    assert!(read.effective_follow_settings().is_none());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&before).unwrap()["format_version"],
        10
    );
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
    for version in [1, 2, 3, 4, 5, 6, 7, 8, 9, 11] {
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

#[test]
fn both_identity_vectors_count_toward_the_manifest_bound() {
    let mut plan = plan();
    plan.tables[0].all_columns = (0..1600).map(|i| format!("column_{i}")).collect();
    plan.tables[0].identity_modes = vec![0; 1600];
    plan.tables[0].writable_columns = plan.tables[0].all_columns.clone();
    let (objects, tables) = inventory(&plan);
    let mut value = CloneState {
        ticket: ticket(&plan),
        plan,
        phase: ClonePhase::Copying,
        owned_objects: objects,
        owned_tables: tables,
        completion: None,
    };
    let filler = OwnedObject {
        class_oid: 1259,
        object_oid: 10_000,
        object_sub_id: 0,
        namespace_oid: Some(2200),
        namespace_name: Some("public".into()),
        name: "x".repeat(63),
        kind: "i".into(),
        owner_oid: 10,
    };
    let remaining = MAX_MANIFEST_BYTES as usize - 4096 - serde_json::to_vec(&value).unwrap().len();
    let count = remaining / (serde_json::to_vec(&filler).unwrap().len() + 1);
    value
        .owned_objects
        .extend((0..count).map(|index| OwnedObject {
            object_oid: 10_000 + index as u32,
            ..filler.clone()
        }));
    value.validate().unwrap();
    for partition in [false, true] {
        let mut oversized = value.clone();
        if partition {
            oversized.plan.tables[0].partition_identity_modes = Some(vec![0; 1600]);
        } else {
            oversized.plan.tables[0].identity_modes = vec![b'a'; 1600];
        }
        oversized.owned_tables[0].table = oversized.plan.tables[0].clone();
        assert!(
            oversized
                .validate()
                .unwrap_err()
                .message
                .contains("one MiB")
        );
    }
}

fn restart_operator() -> OperatorIdentity {
    OperatorIdentity {
        migration_uid: "03030303-0303-0303-0303-030303030303".into(),
        run: RunId([3; 16]),
        workdir_id: "owned-workdir".into(),
        checkpoint_generation: 1,
    }
}

#[test]
fn fresh_restart_initializes_only_absent_or_private_empty_paths() {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    for empty in [false, true] {
        let tmp = super::super::tests::TempDir::new();
        let prior_inode = if empty {
            fs::DirBuilder::new()
                .mode(0o700)
                .create(tmp.path())
                .unwrap();
            Some(fs::metadata(tmp.path()).unwrap().ino())
        } else {
            None
        };
        let operator = restart_operator();
        let state = open_clone(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Restart,
            },
            Some(&operator),
            &control(),
        )
        .unwrap();
        assert_eq!(state.identity.run, operator.run);
        assert_eq!(state.manifest.operator.as_ref(), Some(&operator));
        assert!(state.checked_clone().unwrap().is_none());
        assert!(state.manifest.target_provenance.is_empty());
        assert!(state.manifest.source_slot.is_none());
        assert!(state.manifest.copy_complete.is_none());
        assert_eq!(fs::metadata(tmp.path()).unwrap().mode() & 0o777, 0o700);
        assert_eq!(
            fs::metadata(tmp.path().join("manifest.json"))
                .unwrap()
                .mode()
                & 0o777,
            0o600
        );
        if let Some(inode) = prior_inode {
            assert_eq!(fs::metadata(tmp.path()).unwrap().ino(), inode);
        }
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
    }
}

#[test]
fn matching_init_only_restart_and_resume_preserve_the_existing_binding() {
    let tmp = super::super::tests::TempDir::new();
    let operator = restart_operator();
    drop(
        open_clone(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Create,
            },
            Some(&operator),
            &control(),
        )
        .unwrap(),
    );
    let before = fs::read(tmp.path().join("manifest.json")).unwrap();
    for action in [StateAction::Restart, StateAction::Resume] {
        let state = open_clone(
            &StateRequest {
                path: tmp.path(),
                action,
            },
            Some(&operator),
            &control(),
        )
        .unwrap();
        assert!(state.checked_clone().unwrap().is_none());
        assert_eq!(state.identity.run, operator.run);
        assert!(!state.fresh_created);
        assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
    }
    let mut other = operator.clone();
    other.workdir_id.push('x');
    for work in [None, Some(&other)] {
        assert!(
            open_clone(
                &StateRequest {
                    path: tmp.path(),
                    action: StateAction::Restart
                },
                work,
                &control()
            )
            .is_err()
        );
        assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
    }
}

#[test]
fn init_only_retry_never_rebinds_an_existing_run_to_an_operator() {
    let tmp = super::super::tests::TempDir::new();
    drop(tmp.create());
    let before = fs::read(tmp.path().join("manifest.json")).unwrap();
    let operator = restart_operator();
    for action in [StateAction::Restart, StateAction::Resume] {
        assert!(
            open_clone(
                &StateRequest {
                    path: tmp.path(),
                    action
                },
                Some(&operator),
                &control()
            )
            .is_err()
        );
        assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
    }
}

#[test]
fn resume_absent_or_empty_never_creates_a_manifest_or_directory() {
    use std::os::unix::fs::DirBuilderExt;
    for empty in [false, true] {
        let tmp = super::super::tests::TempDir::new();
        if empty {
            fs::DirBuilder::new()
                .mode(0o700)
                .create(tmp.path())
                .unwrap();
        }
        assert!(
            open_clone(
                &StateRequest {
                    path: tmp.path(),
                    action: StateAction::Resume
                },
                Some(&restart_operator()),
                &control()
            )
            .is_err()
        );
        if empty {
            assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 0);
        } else {
            assert!(!tmp.path().exists());
        }
    }
}

#[test]
fn restart_refuses_foreign_legacy_symlink_and_old_format_without_mutation() {
    use std::os::unix::fs::{DirBuilderExt, symlink};
    for artifact in ["pgcopydb.pid", "data", ".manifest.tmp"] {
        let tmp = super::super::tests::TempDir::new();
        fs::DirBuilder::new()
            .mode(0o700)
            .create(tmp.path())
            .unwrap();
        let path = tmp.path().join(artifact);
        fs::write(&path, b"foreign sentinel").unwrap();
        assert!(
            open_clone(
                &StateRequest {
                    path: tmp.path(),
                    action: StateAction::Restart
                },
                Some(&restart_operator()),
                &control()
            )
            .is_err()
        );
        assert_eq!(fs::read(&path).unwrap(), b"foreign sentinel");
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
    }
    let tmp = super::super::tests::TempDir::new();
    let destination = tmp.0.join("symlink-destination");
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&destination)
        .unwrap();
    symlink(&destination, tmp.path()).unwrap();
    assert!(
        open_clone(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Restart
            },
            Some(&restart_operator()),
            &control()
        )
        .is_err()
    );
    assert!(
        fs::symlink_metadata(tmp.path())
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(fs::read_dir(&destination).unwrap().count(), 0);
    let tmp = super::super::tests::TempDir::new();
    drop(
        open_clone(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Create,
            },
            Some(&restart_operator()),
            &control(),
        )
        .unwrap(),
    );
    let path = tmp.path().join("manifest.json");
    let mut old: serde_json::Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    old["format_version"] = 4.into();
    let before = serde_json::to_vec(&old).unwrap();
    fs::write(&path, &before).unwrap();
    assert!(
        open_clone(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Restart
            },
            Some(&restart_operator()),
            &control()
        )
        .is_err()
    );
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn resource_obligations_cannot_be_admitted_as_init_only_retries() {
    for obligation in ["target", "source", "completion"] {
        let tmp = super::super::tests::TempDir::new();
        let operator = restart_operator();
        let mut state = open_clone(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Create,
            },
            Some(&operator),
            &control(),
        )
        .unwrap();
        let stream = StreamId {
            run: operator.run,
            source: plan().source.identity,
            generation: 1,
        };
        match obligation {
            "target" => {
                state
                    .begin_target_provisioning(TargetBinding {
                        stream,
                        target_system_identifier: Some(3),
                        target_database_oid: 4,
                        schema_name: "native_owned".into(),
                        table_name: "checkpoint".into(),
                        owner_oid: 10,
                        metadata_version: TARGET_METADATA_VERSION,
                    })
                    .unwrap();
            }
            "source" => {
                let binding = SourceSlotBinding::new(
                    stream,
                    "native_owned".into(),
                    crate::cdc::contracts::Plugin::Pgoutput,
                    1,
                )
                .unwrap();
                state.begin_source_slot(&binding).unwrap();
            }
            _ => state
                .record_copy_complete(&CopyReport {
                    tables: 0,
                    rows: 0,
                    bytes: 0,
                })
                .unwrap(),
        }
        drop(state);
        let before = fs::read(tmp.path().join("manifest.json")).unwrap();
        for action in [StateAction::Restart, StateAction::Resume] {
            assert!(
                open_clone(
                    &StateRequest {
                        path: tmp.path(),
                        action
                    },
                    Some(&operator),
                    &control()
                )
                .is_err(),
                "{obligation}"
            );
            assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
        }
    }
}

#[test]
fn existing_partial_and_completed_clone_state_is_loaded_without_reset() {
    for complete in [false, true] {
        let (tmp, mut state, plan) = start();
        state.begin_pre_data().unwrap();
        if complete {
            let (objects, tables) = inventory(&plan);
            state.confirm_pre_data(&plan, objects, tables).unwrap();
            for phase in [
                ClonePhase::PostDataPending,
                ClonePhase::PostDataComplete,
                ClonePhase::SequencesPending,
                ClonePhase::SequencesComplete,
                ClonePhase::Analyzing,
            ] {
                state.mark_stage(phase).unwrap();
            }
            state
                .record_clone_complete(&CopyReport {
                    tables: 1,
                    rows: 7,
                    bytes: 21,
                })
                .unwrap();
        }
        let saved = state.checked_clone().unwrap();
        drop(state);
        let before = fs::read(tmp.path().join("manifest.json")).unwrap();
        for action in [StateAction::Restart, StateAction::Resume] {
            let reopened = open_clone(
                &StateRequest {
                    path: tmp.path(),
                    action,
                },
                None,
                &control(),
            )
            .unwrap();
            assert_eq!(reopened.checked_clone().unwrap(), saved);
            assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
        }
    }
}
