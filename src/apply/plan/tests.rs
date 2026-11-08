use super::*;
use crate::cdc::contracts::{Column, RelationKey, Value};
use crate::contracts::{Error, ErrorKind};

fn key() -> RelationKey {
    RelationKey {
        oid: 42_000,
        generation: 7,
    }
}

fn relation() -> Event {
    Event::Relation {
        key: key(),
        schema: "public".into(),
        name: "items".into(),
        columns: vec![
            Column {
                name: "tenant".into(),
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
            Column {
                name: "id".into(),
                type_oid: 20,
                type_mod: -1,
                key: true,
            },
            Column {
                name: "note".into(),
                type_oid: 25,
                type_mod: -1,
                key: false,
            },
        ],
        replica_identity: b'd',
    }
}

fn text(bytes: &[u8]) -> Value {
    Value::Text(bytes.to_vec())
}

fn insert() -> Event {
    Event::Insert {
        relation: key(),
        values: vec![text(b"12"), text(b"payload"), text(b"34"), Value::Null],
    }
}

fn update() -> Event {
    Event::Update {
        relation: key(),
        old: Some(vec![text(b"10"), text(b"30")]),
        new: vec![text(b"12"), text(b"new body"), text(b"34"), Value::Null],
    }
}

fn delete() -> Event {
    Event::Delete {
        relation: key(),
        old: vec![text(b"10"), text(b"30")],
    }
}

fn params<'a>(bound: &[PgParam<'a>]) -> Vec<Option<&'a [u8]>> {
    bound
        .iter()
        .map(|param| match param {
            PgParam::Null => None,
            PgParam::Text(bytes) => Some(*bytes),
            PgParam::Binary(_) => panic!("binary is outside the first apply profile"),
        })
        .collect()
}

fn refusal(result: Result<DmlPlan>) -> Error {
    result.err().expect("unsafe DML must be refused")
}

#[test]
fn insert_parameters_preserve_all_column_order_nulls_and_borrowed_bytes() {
    let row = insert();
    let plan = DmlPlan::new(&relation(), &row).unwrap();
    assert_eq!(
        plan.sql(),
        r#"INSERT INTO "public"."items" ("tenant", "body", "id", "note") VALUES ($1, $2, $3, $4)"#
    );
    assert_eq!(plan.param_types(), &[23, 25, 20, 25]);
    assert!(plan.matches(&row).unwrap());
    let bound = plan.bind(&row).unwrap();
    assert_eq!(
        params(&bound),
        [
            Some(&b"12"[..]),
            Some(&b"payload"[..]),
            Some(&b"34"[..]),
            None
        ]
    );
    let Event::Insert { values, .. } = &row else {
        unreachable!()
    };
    let Value::Text(original) = &values[1] else {
        unreachable!()
    };
    let PgParam::Text(borrowed) = &bound[1] else {
        unreachable!()
    };
    assert_eq!(original.as_ptr(), borrowed.as_ptr());
}

#[test]
fn changed_keys_use_old_subset_in_persisted_order_despite_reverse_index_order() {
    // The source PK is (id, tenant), while persisted columns place tenant first.
    let row = update();
    let plan = DmlPlan::new(&relation(), &row).unwrap();
    assert_eq!(
        plan.sql(),
        r#"UPDATE ONLY "public"."items" SET "tenant" = $1, "body" = $2, "id" = $3, "note" = $4 WHERE "tenant" = $5 AND "id" = $6"#
    );
    assert_eq!(plan.param_types(), &[23, 25, 20, 25, 23, 20]);
    assert_eq!(
        params(&plan.bind(&row).unwrap()),
        [
            Some(&b"12"[..]),
            Some(&b"new body"[..]),
            Some(&b"34"[..]),
            None,
            Some(&b"10"[..]),
            Some(&b"30"[..])
        ]
    );
}

#[test]
fn update_without_old_image_derives_predicate_keys_from_new_columns() {
    let mut row = update();
    let Event::Update { old, .. } = &mut row else {
        unreachable!()
    };
    *old = None;
    let plan = DmlPlan::new(&relation(), &row).unwrap();
    assert_eq!(
        params(&plan.bind(&row).unwrap()),
        [
            Some(&b"12"[..]),
            Some(&b"new body"[..]),
            Some(&b"34"[..]),
            None,
            Some(&b"12"[..]),
            Some(&b"34"[..])
        ]
    );
    assert!(plan.matches(&update()).unwrap());
    assert_eq!(
        params(&plan.bind(&update()).unwrap())[4..],
        [Some(&b"10"[..]), Some(&b"30"[..])]
    );
}

#[test]
fn delete_uses_only_the_verified_key_subset() {
    let row = delete();
    let plan = DmlPlan::new(&relation(), &row).unwrap();
    assert_eq!(
        plan.sql(),
        r#"DELETE FROM ONLY "public"."items" WHERE "tenant" = $1 AND "id" = $2"#
    );
    assert_eq!(plan.param_types(), &[23, 20]);
    assert_eq!(
        params(&plan.bind(&row).unwrap()),
        [Some(&b"10"[..]), Some(&b"30"[..])]
    );
}

#[test]
fn update_omits_unchanged_toast_but_binds_explicit_null() {
    let mut row = update();
    let Event::Update { new, .. } = &mut row else {
        unreachable!()
    };
    new[1] = Value::UnchangedToast;
    let plan = DmlPlan::new(&relation(), &row).unwrap();
    assert_eq!(
        plan.sql(),
        r#"UPDATE ONLY "public"."items" SET "tenant" = $1, "id" = $2, "note" = $3 WHERE "tenant" = $4 AND "id" = $5"#
    );
    assert_eq!(plan.param_types(), &[23, 20, 25, 23, 20]);
    assert_eq!(
        params(&plan.bind(&row).unwrap()),
        [
            Some(&b"12"[..]),
            Some(&b"34"[..]),
            None,
            Some(&b"10"[..]),
            Some(&b"30"[..])
        ]
    );
    assert!(!plan.matches(&update()).unwrap());
    assert!(plan.bind(&update()).is_err());
}

#[test]
fn cached_update_shape_distinguishes_which_columns_have_unchanged_toast() {
    let mut first = update();
    let Event::Update { new, .. } = &mut first else {
        unreachable!()
    };
    new[1] = Value::UnchangedToast;
    let plan = DmlPlan::new(&relation(), &first).unwrap();
    let mut other = update();
    let Event::Update { new, .. } = &mut other else {
        unreachable!()
    };
    new[3] = Value::UnchangedToast;
    assert!(!plan.matches(&other).unwrap());
    assert!(plan.bind(&other).is_err());
}

#[test]
fn reused_plan_survives_metadata_drop_and_keeps_row_values_out_of_sql() {
    let metadata = relation();
    let mut row = insert();
    let plan = DmlPlan::new(&metadata, &row).unwrap();
    drop(metadata);
    let sql = plan.sql().as_ptr();
    for bytes in [
        b"' ); DROP TABLE private; --".as_slice(),
        b"",
        b"\\x00ff\n\"opaque\"",
    ] {
        let Event::Insert { values, .. } = &mut row else {
            unreachable!()
        };
        values[1] = text(bytes);
        assert!(plan.matches(&row).unwrap());
        assert_eq!(params(&plan.bind(&row).unwrap())[1], Some(bytes));
        assert_eq!(plan.sql().as_ptr(), sql);
    }
    assert!(!plan.sql().contains("private"));
}

#[test]
fn identifiers_quote_schema_table_and_columns_without_changing_unicode() {
    let mut metadata = relation();
    let Event::Relation {
        schema,
        name,
        columns,
        ..
    } = &mut metadata
    else {
        unreachable!()
    };
    *schema = "sé\"lect".into();
    *name = "表.\"items".into();
    columns[0].name = "where\"鍵".into();
    columns[1].name = "line\nbreak".into();
    let plan = DmlPlan::new(&metadata, &insert()).unwrap();
    assert_eq!(
        plan.sql(),
        "INSERT INTO \"sé\"\"lect\".\"表.\"\"items\" (\"where\"\"鍵\", \"line\nbreak\", \"id\", \"note\") VALUES ($1, $2, $3, $4)"
    );
}

#[test]
fn identifier_nul_is_refused_in_every_position_without_echoing_input() {
    for position in 0..3 {
        let mut metadata = relation();
        let Event::Relation {
            schema,
            name,
            columns,
            ..
        } = &mut metadata
        else {
            unreachable!()
        };
        match position {
            0 => *schema = "private-schema\0name".into(),
            1 => *name = "private-table\0name".into(),
            _ => columns[1].name = "private-column\0name".into(),
        }
        let error = refusal(DmlPlan::new(&metadata, &insert()));
        assert!(!format!("{error:?}").contains("private"));
        assert_ne!(error.exit_code(), 0);
    }
}

#[test]
fn relation_oid_and_generation_must_match_during_build_match_and_bind() {
    let plan = DmlPlan::new(&relation(), &insert()).unwrap();
    for wrong in [
        RelationKey {
            oid: 42_001,
            ..key()
        },
        RelationKey {
            generation: 8,
            ..key()
        },
    ] {
        for mut row in [insert(), update(), delete()] {
            match &mut row {
                Event::Insert { relation, .. }
                | Event::Update { relation, .. }
                | Event::Delete { relation, .. } => *relation = wrong,
                _ => unreachable!(),
            }
            assert!(DmlPlan::new(&relation(), &row).is_err());
            assert!(plan.matches(&row).is_err());
            assert!(plan.bind(&row).is_err());
        }
    }
}

#[test]
fn valid_other_operations_do_not_match_and_cannot_bind() {
    for first in [insert(), update(), delete()] {
        let plan = DmlPlan::new(&relation(), &first).unwrap();
        for other in [insert(), update(), delete()] {
            let same = std::mem::discriminant(&first) == std::mem::discriminant(&other);
            assert_eq!(plan.matches(&other).unwrap(), same);
            assert_eq!(plan.bind(&other).is_ok(), same);
        }
    }
}

#[test]
fn missing_and_extra_new_columns_or_old_key_values_are_refused() {
    for difference in [-1, 1] {
        let mut cases = [insert(), update(), update(), delete()];
        for (index, row) in cases.iter_mut().enumerate() {
            let values = match row {
                Event::Insert { values, .. } => values,
                Event::Update { new, .. } if index == 1 => new,
                Event::Update { old: Some(old), .. } | Event::Delete { old, .. } => old,
                _ => unreachable!(),
            };
            if difference < 0 {
                values.pop();
            } else {
                values.push(text(b"unexpected"));
            }
            let plan = DmlPlan::new(
                &relation(),
                &[insert(), update(), update(), delete()][index],
            )
            .unwrap();
            assert!(DmlPlan::new(&relation(), row).is_err());
            assert!(plan.matches(row).is_err());
            assert!(plan.bind(row).is_err());
        }
    }
}

#[test]
fn null_and_unchanged_toast_keys_are_refused_in_new_and_old_images() {
    for invalid in [Value::Null, Value::UnchangedToast] {
        for key_index in [0, 2] {
            for mut row in [insert(), update()] {
                match &mut row {
                    Event::Insert { values, .. } => values[key_index] = invalid.clone(),
                    Event::Update { new, .. } => new[key_index] = invalid.clone(),
                    _ => unreachable!(),
                }
                assert!(DmlPlan::new(&relation(), &row).is_err());
            }
        }
        for key_index in 0..2 {
            for mut row in [update(), delete()] {
                match &mut row {
                    Event::Update { old: Some(old), .. } | Event::Delete { old, .. } => {
                        old[key_index] = invalid.clone();
                    }
                    _ => unreachable!(),
                }
                assert!(DmlPlan::new(&relation(), &row).is_err());
            }
        }
    }
}

#[test]
fn insert_never_accepts_unchanged_toast_in_nonkey_columns() {
    let mut row = insert();
    let Event::Insert { values, .. } = &mut row else {
        unreachable!()
    };
    values[1] = Value::UnchangedToast;
    assert!(DmlPlan::new(&relation(), &row).is_err());
}

#[test]
fn binary_is_explicitly_unsupported_in_new_and_old_images() {
    for (index, mut row) in [insert(), update(), update(), delete()]
        .into_iter()
        .enumerate()
    {
        let values = match &mut row {
            Event::Insert { values, .. } => values,
            Event::Update { new, .. } if index == 1 => new,
            Event::Update { old: Some(old), .. } | Event::Delete { old, .. } => old,
            _ => unreachable!(),
        };
        values[0] = Value::Binary(vec![0, 255]);
        let error = refusal(DmlPlan::new(&relation(), &row));
        assert!(error.message.contains("binary"));
    }
}

#[test]
fn relation_context_and_truncate_are_never_treated_as_row_plans() {
    let plan = DmlPlan::new(&relation(), &insert()).unwrap();
    for event in [
        relation(),
        Event::Truncate {
            relations: vec![key()],
            cascade: true,
            restart_identity: true,
        },
        Event::Truncate {
            relations: vec![key()],
            cascade: false,
            restart_identity: false,
        },
    ] {
        assert!(DmlPlan::new(&relation(), &event).is_err());
        assert!(plan.matches(&event).is_err());
        assert!(plan.bind(&event).is_err());
    }
}

#[test]
fn unverified_replica_identity_and_absent_keys_fail_before_sql() {
    for identity in [b'f', b'n', b'i', 0] {
        let mut metadata = relation();
        let Event::Relation {
            replica_identity, ..
        } = &mut metadata
        else {
            unreachable!()
        };
        *replica_identity = identity;
        assert!(DmlPlan::new(&metadata, &insert()).is_err());
    }
    let mut metadata = relation();
    let Event::Relation { columns, .. } = &mut metadata else {
        unreachable!()
    };
    for column in columns {
        column.key = false;
    }
    for row in [insert(), update(), delete()] {
        assert!(DmlPlan::new(&metadata, &row).is_err());
    }
}

#[test]
fn empty_duplicate_and_invalid_relation_metadata_are_refused() {
    for case in 0..7 {
        let mut metadata = relation();
        let Event::Relation {
            key,
            schema,
            name,
            columns,
            ..
        } = &mut metadata
        else {
            unreachable!()
        };
        match case {
            0 => schema.clear(),
            1 => name.clear(),
            2 => columns.clear(),
            3 => columns[1].name = "tenant".into(),
            4 => columns[1].type_oid = 0,
            5 => columns[1].type_oid = 16_384,
            _ => key.oid = 0,
        }
        assert!(DmlPlan::new(&metadata, &insert()).is_err());
    }
    assert!(DmlPlan::new(&insert(), &insert()).is_err());
}

#[test]
fn parameter_bounds_include_predicate_keys_and_checked_addition() {
    assert_eq!(parameter_count(65_534, 1).unwrap(), 65_535);
    assert!(parameter_count(65_535, 1).is_err());
    assert!(parameter_count(usize::MAX, 1).is_err());
}

#[test]
fn owned_capacity_counts_spare_space_in_all_four_buffers_but_not_inline_storage() {
    let mut plan = DmlPlan::new(&relation(), &update()).unwrap();
    let mut expected = plan.owned_capacity_bytes().unwrap();
    for buffer in 0..4 {
        let (before, after, element_bytes) = match buffer {
            0 => {
                let before = plan.sql.capacity();
                plan.sql.reserve_exact(before + 1);
                (before, plan.sql.capacity(), 1)
            }
            1 => {
                let before = plan.param_types.capacity();
                plan.param_types.reserve_exact(before + 1);
                (before, plan.param_types.capacity(), size_of::<u32>())
            }
            2 => {
                let before = plan.key_columns.capacity();
                plan.key_columns.reserve_exact(before + 1);
                (before, plan.key_columns.capacity(), size_of::<usize>())
            }
            _ => {
                let before = plan.write_columns.capacity();
                plan.write_columns.reserve_exact(before + 1);
                (before, plan.write_columns.capacity(), size_of::<usize>())
            }
        };
        assert!(after > before);
        expected += (after - before) * element_bytes;
        assert_eq!(plan.owned_capacity_bytes().unwrap(), expected);
    }
    plan.sql = String::new();
    plan.param_types = Vec::new();
    plan.key_columns = Vec::new();
    plan.write_columns = Vec::new();
    assert_eq!(plan.owned_capacity_bytes().unwrap(), 0);
}

#[test]
fn diagnostics_do_not_expose_relation_names_or_row_values() {
    let mut metadata = relation();
    let Event::Relation { schema, name, .. } = &mut metadata else {
        unreachable!()
    };
    *schema = "private-schema".into();
    *name = "private-table".into();
    let mut row = update();
    let Event::Update { old, new, .. } = &mut row else {
        unreachable!()
    };
    *old = Some(vec![text(b"private-key")]);
    new[1] = text(b"private-row-value");
    let error = refusal(DmlPlan::new(&metadata, &row));
    assert_eq!(error.kind, ErrorKind::Target);
    assert!(!format!("{error:?}").contains("private"));
    assert_ne!(error.exit_code(), 0);
}
