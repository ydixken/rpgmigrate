use super::*;
use crate::cdc::contracts::{OldTuple, OldTupleKind};
fn key() -> RelationKey {
    RelationKey {
        oid: 20_000,
        generation: 1,
    }
}
#[test]
fn every_dml_command_tag_requires_its_exact_plan_branch() {
    let mut checked = FollowRelation {
        relation: Event::Relation {
            key: key(),
            schema: "public".into(),
            name: "items".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 23,
                    type_mod: -1,
                    key: true,
                },
                Column {
                    name: "body".into(),
                    type_oid: 25,
                    type_mod: -1,
                    key: false,
                },
            ],
            replica_identity: b'd',
        },
        target_type_oids: vec![23, 25],
        identity_modes: vec![b'a', 0],
    };
    let values = vec![Value::Text(b"1".to_vec()), Value::Null];
    let insert = Event::Insert {
        relation: key(),
        values: values.clone(),
    };
    let ordinary = Event::Update {
        relation: key(),
        old: None,
        new: values.clone(),
    };
    let rewrite = Event::Update {
        relation: key(),
        old: Some(OldTuple {
            kind: OldTupleKind::Key,
            values: vec![Value::Text(b"2".to_vec()), Value::Null],
        }),
        new: values.clone(),
    };
    let delete = Event::Delete {
        relation: key(),
        old: OldTuple {
            kind: OldTupleKind::Key,
            values,
        },
    };
    for (event, expected) in [
        (&insert, "INSERT 0 1"),
        (&ordinary, "UPDATE 1"),
        (&rewrite, "INSERT 0 1"),
        (&delete, "DELETE 1"),
    ] {
        let plan = DmlPlan::new(&checked, event).unwrap();
        require_one_row(&plan, expected).unwrap();
        for tag in [
            "",
            "INSERT 0 0",
            "INSERT 0 2",
            "UPDATE 0",
            "UPDATE 2",
            "DELETE 0",
            "DELETE 2",
            "COMMIT",
            "UPDATE 1 ",
            "INSERT 0 1",
            "UPDATE 1",
            "DELETE 1",
        ] {
            assert_eq!(
                require_one_row(&plan, tag).is_ok(),
                tag == expected,
                "{tag}"
            );
        }
    }
    checked.identity_modes[0] = b'd';
    require_one_row(&DmlPlan::new(&checked, &rewrite).unwrap(), "UPDATE 1").unwrap();
}
#[test]
fn pipeline_charge_matches_transport_null_text_and_binary_encoding() {
    assert_eq!(
        prepared_bytes(
            "fa",
            &[
                PgParam::Null,
                PgParam::Text(b"abc"),
                PgParam::Binary(&[0, 255])
            ]
        )
        .unwrap(),
        3 + 4 + 8 + 6
    );
    assert_eq!(WINDOW_COMMANDS + 2, 64);
    assert_eq!(WINDOW_BYTES + CHECKPOINT_RESERVE, PIPELINE_BYTES);
}
#[test]
fn event_charge_counts_full_width_old_images_and_payload_bytes() {
    let old = OldTuple {
        kind: OldTupleKind::Full,
        values: vec![Value::Text(vec![1; 40]), Value::Null],
    };
    let event = Event::Update {
        relation: key(),
        old: Some(old),
        new: vec![Value::Text(vec![2; 80]), Value::UnchangedToast],
    };
    assert_eq!(
        event_bytes(&event).unwrap(),
        size_of::<Event>() + 4 * size_of::<Value>() + 120
    );
    assert!(add(usize::MAX, 1).is_err());
}
#[test]
fn empty_cache_has_no_row_accumulation_or_unbounded_preallocation() {
    let cache = Caches::new();
    assert_eq!(cache.pending.capacity(), 0);
    assert_eq!(cache.pending_bytes, 0);
    assert_eq!(
        cache.plan_bytes().unwrap(),
        PLAN_SLOTS * size_of::<CachedPlan>()
    );
    assert!(!cache.serial);
}
