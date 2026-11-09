use super::*;
use crate::cdc::contracts::Column;

fn key() -> RelationKey {
    RelationKey {
        oid: 42_000,
        generation: 7,
    }
}
fn text(value: &[u8]) -> Value {
    Value::Text(value.to_vec())
}
fn relation(identity: u8) -> FollowRelation {
    FollowRelation {
        relation: Event::Relation {
            key: key(),
            schema: "odd\"schema".into(),
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
                    type_oid: 42_123,
                    type_mod: -1,
                    key: false,
                },
                Column {
                    name: "other".into(),
                    type_oid: 25,
                    type_mod: -1,
                    key: true,
                },
            ],
            replica_identity: identity,
        },
        target_type_oids: vec![23, 51_987, 25],
        identity_modes: vec![0, 0, 0],
    }
}
fn insert() -> Event {
    Event::Insert {
        relation: key(),
        values: vec![text(b"2"), text(b"new"), Value::Null],
    }
}
fn update(identity: u8) -> Event {
    Event::Update {
        relation: key(),
        old: Some(OldTuple {
            kind: if identity == b'f' {
                OldTupleKind::Full
            } else {
                OldTupleKind::Key
            },
            values: vec![
                text(b"1"),
                if identity == b'f' {
                    text(b"old")
                } else {
                    Value::Null
                },
                Value::Null,
            ],
        }),
        new: vec![text(b"2"), Value::UnchangedToast, Value::Null],
    }
}
fn delete(identity: u8) -> Event {
    let Event::Update { old: Some(old), .. } = update(identity) else {
        unreachable!()
    };
    Event::Delete {
        relation: key(),
        old,
    }
}
fn values<'a>(params: &[PgParam<'a>]) -> Vec<Option<&'a [u8]>> {
    params
        .iter()
        .map(|p| match p {
            PgParam::Null => None,
            PgParam::Text(b) | PgParam::Binary(b) => Some(*b),
        })
        .collect()
}
#[test]
fn insert_quotes_identifiers_and_uses_actual_target_type_oids() {
    let row = insert();
    let mut metadata = relation(b'd');
    metadata.identity_modes[0] = b'a';
    let plan = DmlPlan::new(&metadata, &row).unwrap();
    assert_eq!(
        plan.sql(),
        "INSERT INTO \"odd\"\"schema\".\"items\" (\"id\", \"body\", \"other\") OVERRIDING SYSTEM VALUE VALUES ($1, $2, $3)"
    );
    assert_eq!(plan.param_types(), [23, 51_987, 25]);
    assert_eq!(
        values(&plan.bind(&row).unwrap()),
        [Some(&b"2"[..]), Some(&b"new"[..]), None]
    );
    assert!(!plan.sql().contains("new"));
}
#[test]
fn default_and_index_old_keys_are_full_width_and_null_safe() {
    for identity in *b"di" {
        let row = update(identity);
        let plan = DmlPlan::new(&relation(identity), &row).unwrap();
        assert_eq!(plan.param_types(), [23, 25, 23, 25]);
        assert_eq!(
            values(&plan.bind(&row).unwrap()),
            [Some(&b"2"[..]), None, Some(&b"1"[..]), None]
        );
        assert!(
            plan.sql()
                .contains("\"id\" IS NOT DISTINCT FROM $3 AND \"other\" IS NOT DISTINCT FROM $4")
        );
        assert!(!plan.sql().contains("\"body\" ="));
    }
}
#[test]
fn update_without_old_keys_uses_new_positions_and_reuses_same_plan() {
    let row = update(b'd');
    let plan = DmlPlan::new(&relation(b'd'), &row).unwrap();
    let mut no_old = update(b'd');
    if let Event::Update { old, .. } = &mut no_old {
        *old = None;
    }
    assert!(plan.matches(&no_old).unwrap());
    assert_eq!(values(&plan.bind(&no_old).unwrap())[2], Some(&b"2"[..]));
}
#[test]
fn full_identity_mutates_only_one_duplicate_physical_row() {
    for row in [update(b'f'), delete(b'f')] {
        let mut metadata = relation(b'f');
        if let Event::Relation { columns, .. } = &mut metadata.relation {
            for column in columns {
                column.key = false;
            }
        }
        let plan = DmlPlan::new(&metadata, &row).unwrap();
        assert!(plan.sql().contains("WHERE ctid = (SELECT ctid FROM ONLY"));
        assert!(plan.sql().ends_with("LIMIT 1)"));
        assert!(
            plan.sql()
                .contains("\"body\"::pg_catalog.text COLLATE pg_catalog.\"C\"")
        );
        assert_eq!(values(&plan.bind(&row).unwrap()).last(), Some(&None));
    }
}
#[test]
fn toast_shape_is_cached_and_unavailable_identity_is_refused() {
    let row = update(b'd');
    let plan = DmlPlan::new(&relation(b'd'), &row).unwrap();
    let mut changed = row.clone();
    if let Event::Update { new, .. } = &mut changed {
        new[1] = Value::Null;
    }
    assert!(!plan.matches(&changed).unwrap());
    assert!(plan.bind(&changed).is_err());
    let mut unavailable = update(b'f');
    if let Event::Update { old: Some(old), .. } = &mut unavailable {
        old.values[1] = Value::UnchangedToast;
    }
    assert!(DmlPlan::new(&relation(b'f'), &unavailable).is_err());
}
#[test]
fn malformed_width_kind_generation_and_insert_toast_fail_closed() {
    for case in 0..5 {
        let mut row = update(b'd');
        if let Event::Update {
            relation,
            old: Some(old),
            new,
        } = &mut row
        {
            match case {
                0 => {
                    old.values.pop();
                }
                1 => old.kind = OldTupleKind::Full,
                2 => relation.generation += 1,
                3 => {
                    new.pop();
                }
                _ => old.values[0] = Value::UnchangedToast,
            }
        }
        assert!(DmlPlan::new(&relation(b'd'), &row).is_err());
    }
    let mut row = insert();
    if let Event::Insert { values, .. } = &mut row {
        values[1] = Value::UnchangedToast;
    }
    assert!(DmlPlan::new(&relation(b'd'), &row).is_err());
    let mut row = update(b'f');
    if let Event::Update { old, .. } = &mut row {
        *old = None;
    }
    assert!(DmlPlan::new(&relation(b'f'), &row).is_err());
}
#[test]
fn invalid_checked_metadata_and_unsupported_events_are_refused() {
    for case in 0..5 {
        let mut checked = relation(b'd');
        match case {
            0 => checked.target_type_oids.clear(),
            1 => checked.target_type_oids[0] = 0,
            _ => {
                if let Event::Relation {
                    columns,
                    replica_identity,
                    name,
                    ..
                } = &mut checked.relation
                {
                    match case {
                        2 => *replica_identity = b'n',
                        3 => columns[1].name = "id".into(),
                        _ => name.push('\0'),
                    }
                }
            }
        }
        assert!(DmlPlan::new(&checked, &insert()).is_err());
    }
    assert!(DmlPlan::new(&relation(b'd'), &relation(b'd').relation).is_err());
    assert!(
        DmlPlan::new(
            &relation(b'd'),
            &Event::Truncate {
                relations: vec![key()],
                cascade: false,
                restart_identity: false
            }
        )
        .is_err()
    );
    assert_eq!(parameter_count(65_534, 1).unwrap(), 65_535);
    assert!(parameter_count(65_535, 1).is_err());
    assert!(parameter_count(usize::MAX, 1).is_err());
}
#[test]
fn text_bind_borrows_values_without_casts_or_row_diagnostics() {
    let row = insert();
    let Event::Insert { values, .. } = &row else {
        unreachable!()
    };
    let Value::Text(expected) = &values[1] else {
        unreachable!()
    };
    let plan = DmlPlan::new(&relation(b'd'), &row).unwrap();
    let params = plan.bind(&row).unwrap();
    let PgParam::Text(value) = params[1] else {
        panic!("text value must bind in text format")
    };
    assert_eq!(value, expected);
    assert!(std::ptr::eq(value.as_ptr(), expected.as_ptr()));
    assert!(!plan.sql().contains("::"));
    assert!(plan.owned_capacity_bytes().unwrap() >= plan.sql().len());
}

fn live_connection(io: &crate::contracts::IoControl) -> crate::pg::PgConnection {
    std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned database harness required");
    let conninfo = crate::contracts::Conninfo::new(
        std::env::var_os("RPGMIGRATE_TEST_SOURCE").expect("owned database source required"),
    )
    .unwrap();
    crate::pg::PgConnection::connect(&conninfo, io).unwrap()
}
fn live_control() -> crate::contracts::IoControl {
    crate::contracts::IoControl {
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
        cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }
}
fn live_plan(
    connection: &mut crate::pg::PgConnection,
    metadata: &FollowRelation,
    event: &Event,
    ordinal: usize,
    io: &crate::contracts::IoControl,
) -> String {
    let plan = DmlPlan::new(metadata, event).unwrap();
    let name = format!("owned_dml_{ordinal}");
    connection
        .prepare(&name, plan.sql(), plan.param_types(), io)
        .unwrap();
    let result = connection
        .execute_prepared(&name, &plan.bind(event).unwrap(), io)
        .unwrap();
    result.command_tag().unwrap().to_owned()
}
#[test]
#[ignore = "requires owned PostgreSQL harness"]
fn live_full_duplicate_null_and_casefold_preserves_exact_multiset() {
    let io = live_control();
    let mut db = live_connection(&io);
    crate::catalog::configure_session(&mut db, &io).unwrap();
    db.query("CREATE COLLATION pg_temp.owned_dml_casefold (provider = icu, locale = 'und-u-ks-level2', deterministic = false)", &io).unwrap();
    db.query("CREATE TEMP TABLE owned_dml_full (id text COLLATE pg_temp.owned_dml_casefold, body text, other text)", &io).unwrap();
    db.query(
        "INSERT INTO owned_dml_full VALUES ('Foo',NULL,''),('Foo',NULL,''),('foo',NULL,'')",
        &io,
    )
    .unwrap();
    let mut metadata = relation(b'f');
    if let Event::Relation {
        schema,
        name,
        columns,
        ..
    } = &mut metadata.relation
    {
        *schema = "pg_temp".into();
        *name = "owned_dml_full".into();
        for column in columns {
            column.type_oid = 25;
            column.key = false;
        }
    }
    metadata.target_type_oids = vec![25, 25, 25];
    let old = OldTuple {
        kind: OldTupleKind::Full,
        values: vec![text(b"Foo"), Value::Null, text(b"")],
    };
    let change = Event::Update {
        relation: key(),
        old: Some(old.clone()),
        new: vec![text(b"Bar"), Value::UnchangedToast, text(b"")],
    };
    assert_eq!(live_plan(&mut db, &metadata, &change, 1, &io), "UPDATE 1");
    let remove = Event::Delete {
        relation: key(),
        old: old.clone(),
    };
    assert_eq!(live_plan(&mut db, &metadata, &remove, 2, &io), "DELETE 1");
    let result = db
        .query(
            "SELECT id, body, other FROM owned_dml_full ORDER BY id COLLATE \"C\"",
            &io,
        )
        .unwrap();
    assert_eq!(result.rows(), 2);
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"Bar"[..]));
    assert_eq!(result.cell(1, 0).unwrap(), Some(&b"foo"[..]));
    assert_eq!(result.cell(0, 1).unwrap(), None);
    assert_eq!(live_plan(&mut db, &metadata, &remove, 3, &io), "DELETE 0");
}
#[test]
#[ignore = "requires owned PostgreSQL harness"]
fn live_target_domain_oid_identity_override_and_oversize_text() {
    let io = live_control();
    let mut db = live_connection(&io);
    crate::catalog::configure_session(&mut db, &io).unwrap();
    db.query("CREATE DOMAIN pg_temp.owned_dml_domain AS text", &io)
        .unwrap();
    db.query("CREATE TEMP TABLE owned_dml_identity (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body pg_temp.owned_dml_domain, other text)", &io).unwrap();
    let result = db.query("SELECT atttypid FROM pg_catalog.pg_attribute WHERE attrelid = 'pg_temp.owned_dml_identity'::pg_catalog.regclass AND attname = 'body'", &io).unwrap();
    let oid = std::str::from_utf8(result.cell(0, 0).unwrap().unwrap())
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let mut metadata = relation(b'd');
    metadata.target_type_oids = vec![23, oid, 25];
    metadata.identity_modes[0] = b'a';
    if let Event::Relation {
        schema,
        name,
        columns,
        ..
    } = &mut metadata.relation
    {
        *schema = "pg_temp".into();
        *name = "owned_dml_identity".into();
        columns[2].key = false;
    }
    let payload = vec![b'x'; 1024 * 1024 + 17];
    let row = Event::Insert {
        relation: key(),
        values: vec![text(b"42"), Value::Text(payload), Value::Null],
    };
    assert_eq!(live_plan(&mut db, &metadata, &row, 1, &io), "INSERT 0 1");
    let result = db
        .query(
            "SELECT id, octet_length(body), other FROM owned_dml_identity",
            &io,
        )
        .unwrap();
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"42"[..]));
    assert_eq!(result.cell(0, 1).unwrap(), Some(&b"1048593"[..]));
    assert_eq!(result.cell(0, 2).unwrap(), None);
}

#[test]
#[ignore = "requires owned PostgreSQL harness"]
fn live_serial_and_pipeline_agree_on_repeated_keys_and_full_duplicates() {
    let io = live_control();
    let mut db = live_connection(&io);
    crate::catalog::configure_session(&mut db, &io).unwrap();
    let old = OldTuple {
        kind: OldTupleKind::Full,
        values: vec![text(b"1"), text(b"first"), Value::Null],
    };
    let events = vec![
        Event::Insert {
            relation: key(),
            values: old.values.clone(),
        },
        Event::Insert {
            relation: key(),
            values: old.values.clone(),
        },
        Event::Update {
            relation: key(),
            old: Some(old.clone()),
            new: vec![text(b"2"), Value::UnchangedToast, Value::Null],
        },
        Event::Delete {
            relation: key(),
            old,
        },
    ];
    for (mode, table) in [
        ("serial", "owned_dml_serial"),
        ("pipeline", "owned_dml_pipeline"),
    ] {
        db.query(
            &format!("CREATE TEMP TABLE {table} (id text, body text, other text)"),
            &io,
        )
        .unwrap();
        let mut metadata = relation(b'f');
        metadata.target_type_oids = vec![25, 25, 25];
        if let Event::Relation {
            schema,
            name,
            columns,
            ..
        } = &mut metadata.relation
        {
            *schema = "pg_temp".into();
            *name = table.into();
            for column in columns {
                column.type_oid = 25;
                column.key = false;
            }
        }
        let plans: Vec<_> = events
            .iter()
            .map(|event| DmlPlan::new(&metadata, event).unwrap())
            .collect();
        let names: Vec<_> = (0..events.len())
            .map(|index| format!("owned_{mode}_{index}"))
            .collect();
        for (plan, name) in plans.iter().zip(&names) {
            db.prepare(name, plan.sql(), plan.param_types(), &io)
                .unwrap();
        }
        db.query("BEGIN", &io).unwrap();
        if mode == "serial" {
            for ((plan, name), event) in plans.iter().zip(&names).zip(&events) {
                super::super::serial::require_result(
                    plan,
                    db.execute_prepared(name, &plan.bind(event).unwrap(), &io)
                        .unwrap(),
                )
                .unwrap();
            }
        } else {
            let mut pipeline = db.pipeline(&io).unwrap();
            for ((plan, name), event) in plans.iter().zip(&names).zip(&events) {
                pipeline
                    .execute_prepared(name, &plan.bind(event).unwrap(), &io)
                    .unwrap();
            }
            pipeline
                .sync(&io, |index, result| {
                    super::super::serial::require_result(&plans[index], result)
                })
                .unwrap();
        }
        assert_eq!(
            db.query("COMMIT", &io).unwrap().command_tag().unwrap(),
            "COMMIT"
        );
    }
    let result = db.query("SELECT * FROM (TABLE owned_dml_serial EXCEPT ALL TABLE owned_dml_pipeline) AS diff UNION ALL SELECT * FROM (TABLE owned_dml_pipeline EXCEPT ALL TABLE owned_dml_serial) AS diff", &io).unwrap();
    assert_eq!(result.rows(), 0);
    let result = db
        .query("SELECT id, body, other FROM owned_dml_pipeline", &io)
        .unwrap();
    assert_eq!(result.rows(), 1);
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"2"[..]));
    assert_eq!(result.cell(0, 1).unwrap(), Some(&b"first"[..]));
    assert_eq!(result.cell(0, 2).unwrap(), None);
}

#[test]
fn always_key_ordinary_update_omits_proven_unchanged_assignment() {
    let mut metadata = relation(b'd');
    metadata.identity_modes[0] = b'a';
    let mut row = update(b'd');
    if let Event::Update { old, .. } = &mut row {
        *old = None;
    }
    let plan = DmlPlan::new(&metadata, &row).unwrap();
    assert!(plan.sql().starts_with("UPDATE ONLY "));
    assert!(!plan.sql().contains("SET \"id\" ="));
    assert_eq!(plan.param_types(), [25, 23, 25]);
    assert_eq!(
        values(&plan.bind(&row).unwrap()),
        [None, Some(&b"2"[..]), None]
    );
}

#[test]
fn changed_always_key_uses_one_locked_physical_row_rewrite() {
    let mut metadata = relation(b'd');
    metadata.identity_modes[0] = b'a';
    let row = update(b'd');
    let plan = DmlPlan::new(&metadata, &row).unwrap();
    assert_eq!(
        plan.sql(),
        "WITH rpgmigrate_old AS (DELETE FROM ONLY \"odd\"\"schema\".\"items\" WHERE ctid = (SELECT ctid FROM ONLY \"odd\"\"schema\".\"items\" WHERE \"id\" IS NOT DISTINCT FROM $3 AND \"other\" IS NOT DISTINCT FROM $4 LIMIT 1 FOR UPDATE) RETURNING \"id\", \"body\", \"other\") INSERT INTO \"odd\"\"schema\".\"items\" (\"id\", \"body\", \"other\") OVERRIDING SYSTEM VALUE SELECT $1, rpgmigrate_old.\"body\", $2 FROM rpgmigrate_old"
    );
    assert_eq!(plan.param_types(), [23, 25, 23, 25]);
    assert_eq!(
        values(&plan.bind(&row).unwrap()),
        [Some(&b"2"[..]), None, Some(&b"1"[..]), None]
    );
}

#[test]
fn always_cache_classification_rejects_opposite_identity_branch() {
    let mut metadata = relation(b'd');
    metadata.identity_modes[0] = b'a';
    let changed = update(b'd');
    let mut unchanged = changed.clone();
    if let Event::Update { old, .. } = &mut unchanged {
        *old = None;
    }
    for (first, second) in [(&unchanged, &changed), (&changed, &unchanged)] {
        let plan = DmlPlan::new(&metadata, first).unwrap();
        assert!(plan.matches(first).unwrap());
        assert!(!plan.matches(second).unwrap());
        assert!(plan.bind(second).is_err());
    }
}

#[test]
fn nonkey_always_requires_full_old_proof_even_when_key_placeholders_match() {
    let mut metadata = relation(b'd');
    metadata.identity_modes[2] = b'a';
    if let Event::Relation { columns, .. } = &mut metadata.relation {
        columns[2].key = false;
    }
    for absent in [false, true] {
        let mut row = update(b'd');
        if let Event::Update { old, new, .. } = &mut row {
            new[2] = text(b"9");
            if absent {
                *old = None;
            } else {
                old.as_mut().unwrap().values[2] = text(b"9");
            }
        }
        let plan = DmlPlan::new(&metadata, &row).unwrap();
        assert_eq!(plan.expected_tag(), "INSERT 0 1");
        assert!(
            plan.sql()
                .starts_with("WITH rpgmigrate_old AS (DELETE FROM ONLY ")
        );
    }
    let mut metadata = relation(b'f');
    metadata.identity_modes[2] = b'a';
    let mut row = update(b'f');
    if let Event::Update {
        old: Some(old),
        new,
        ..
    } = &mut row
    {
        old.values[2] = text(b"9");
        new[2] = text(b"9");
    }
    let plan = DmlPlan::new(&metadata, &row).unwrap();
    assert_eq!(plan.expected_tag(), "UPDATE 1");
    assert!(!plan.sql().contains("\"other\" ="));
    assert_eq!(plan.param_types(), [23, 23, 51_987, 25]);
}

#[test]
fn multiple_always_values_require_available_exact_proof_and_writable_remainder() {
    let mut metadata = relation(b'd');
    metadata.identity_modes = vec![b'a', 0, b'a'];
    let mut row = update(b'd');
    if let Event::Update {
        old: Some(old),
        new,
        ..
    } = &mut row
    {
        old.values[0] = text(b"2");
        new[1] = text(b"changed body");
    }
    let plan = DmlPlan::new(&metadata, &row).unwrap();
    assert_eq!(plan.expected_tag(), "UPDATE 1");
    assert!(plan.sql().contains("SET \"body\" = $1 WHERE"));
    assert_eq!(plan.param_types(), [51_987, 23, 25]);
    if let Event::Update {
        old: Some(old),
        new,
        ..
    } = &mut row
    {
        old.values[0] = text(b"1");
        new[2] = Value::UnchangedToast;
    }
    assert!(DmlPlan::new(&metadata, &row).is_err());
    assert!(plan.matches(&row).is_err());
    assert!(plan.bind(&row).is_err());
}

#[test]
fn identity_only_update_rewrites_without_defaults_and_keeps_exact_key_types() {
    let mut metadata = relation(b'd');
    if let Event::Relation { columns, .. } = &mut metadata.relation {
        columns.truncate(1);
    }
    metadata.target_type_oids = vec![20];
    metadata.identity_modes = vec![b'a'];
    let row = Event::Update {
        relation: key(),
        old: None,
        new: vec![text(b"8")],
    };
    let plan = DmlPlan::new(&metadata, &row).unwrap();
    assert_eq!(plan.expected_tag(), "INSERT 0 1");
    assert_eq!(plan.param_types(), [20, 20]);
    assert_eq!(
        values(&plan.bind(&row).unwrap()),
        [Some(&b"8"[..]), Some(&b"8"[..])]
    );
    assert!(plan.sql().contains("LIMIT 1 FOR UPDATE) RETURNING \"id\")"));
    assert!(
        plan.sql()
            .ends_with("OVERRIDING SYSTEM VALUE SELECT $1 FROM rpgmigrate_old")
    );
    assert!(!plan.sql().contains("DEFAULT"));
}

#[test]
fn by_default_key_changes_stay_writable_and_full_rewrite_has_one_raw_selector() {
    let mut metadata = relation(b'd');
    metadata.identity_modes[0] = b'd';
    let plan = DmlPlan::new(&metadata, &update(b'd')).unwrap();
    assert_eq!(plan.expected_tag(), "UPDATE 1");
    assert!(plan.sql().contains("SET \"id\" = $1"));
    assert!(!plan.sql().contains("OVERRIDING"));
    metadata = relation(b'f');
    metadata.identity_modes[0] = b'a';
    let plan = DmlPlan::new(&metadata, &update(b'f')).unwrap();
    assert_eq!(plan.sql().matches("SELECT ctid").count(), 1);
    assert_eq!(plan.sql().matches("FROM ONLY").count(), 2);
    assert!(
        plan.sql()
            .contains("\"body\"::pg_catalog.text COLLATE pg_catalog.\"C\"")
    );
    assert!(plan.sql().contains("LIMIT 1 FOR UPDATE"));
    assert_eq!(plan.param_types(), [23, 25, 23, 51_987, 25]);
}

#[test]
fn checked_identity_modes_are_bounded_and_owned_column_capacity_is_charged() {
    for modes in [vec![], vec![0, 0], vec![0, b'x', 0], vec![0; 4]] {
        let mut metadata = relation(b'd');
        metadata.identity_modes = modes;
        assert!(DmlPlan::new(&metadata, &insert()).is_err());
    }
    let mut metadata = relation(b'd');
    metadata.identity_modes[0] = b'a';
    let mut plan = DmlPlan::new(&metadata, &update(b'd')).unwrap();
    let before = plan.owned_capacity_bytes().unwrap();
    let capacity = plan.always_columns.capacity();
    plan.always_columns.reserve(100);
    assert_eq!(
        plan.owned_capacity_bytes().unwrap() - before,
        (plan.always_columns.capacity() - capacity) * size_of::<usize>()
    );
}

fn live_cached_replay(
    db: &mut crate::pg::PgConnection,
    metadata: &FollowRelation,
    events: &[Event],
    prefix: &str,
    pipeline: bool,
    io: &crate::contracts::IoControl,
) -> Vec<&'static str> {
    let mut plans = Vec::<DmlPlan>::new();
    let mut names = Vec::<String>::new();
    let mut indices = Vec::new();
    for event in events {
        let index = plans
            .iter()
            .position(|plan| plan.matches(event).unwrap())
            .unwrap_or_else(|| {
                let plan = DmlPlan::new(metadata, event).unwrap();
                let name = format!("{prefix}_{}", plans.len());
                db.prepare(&name, plan.sql(), plan.param_types(), io)
                    .unwrap();
                plans.push(plan);
                names.push(name);
                plans.len() - 1
            });
        indices.push(index);
    }
    db.query("BEGIN", io).unwrap();
    db.query("SET LOCAL session_replication_role=replica", io)
        .unwrap();
    if pipeline {
        let mut pipeline = db.pipeline(io).unwrap();
        for (event, index) in events.iter().zip(&indices) {
            pipeline
                .execute_prepared(&names[*index], &plans[*index].bind(event).unwrap(), io)
                .unwrap();
        }
        pipeline
            .sync(io, |index, result| {
                super::super::serial::require_result(&plans[indices[index]], result)
            })
            .unwrap();
    } else {
        for (event, index) in events.iter().zip(&indices) {
            super::super::serial::require_result(
                &plans[*index],
                db.execute_prepared(&names[*index], &plans[*index].bind(event).unwrap(), io)
                    .unwrap(),
            )
            .unwrap();
        }
    }
    db.query("SET CONSTRAINTS ALL IMMEDIATE", io).unwrap();
    assert_eq!(
        db.query("COMMIT", io).unwrap().command_tag().unwrap(),
        "COMMIT"
    );
    indices
        .iter()
        .map(|index| plans[*index].expected_tag())
        .collect()
}

fn live_metadata(table: &str, identity: u8, modes: &[u8]) -> FollowRelation {
    let mut metadata = relation(identity);
    metadata.identity_modes = modes.to_vec();
    metadata.target_type_oids = vec![23, 25, 25];
    if let Event::Relation {
        schema,
        name,
        columns,
        ..
    } = &mut metadata.relation
    {
        *schema = "pg_temp".into();
        *name = table.into();
        columns[1].type_oid = 25;
        columns[2].key = false;
    }
    metadata
}

#[test]
#[ignore = "requires owned PostgreSQL harness"]
fn live_identity_updates_match_independent_oracle_and_preserve_sequence() {
    let io = live_control();
    let mut db = live_connection(&io);
    crate::catalog::configure_session(&mut db, &io).unwrap();
    db.query("CREATE TEMP TABLE owned_identity_source (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body text, other text)", &io).unwrap();
    db.query(
        "INSERT INTO owned_identity_source(body,other) VALUES (repeat('x',1280000),'seed')",
        &io,
    )
    .unwrap();
    let result = db
        .query(
            "UPDATE owned_identity_source SET id=DEFAULT,other='changed' RETURNING id",
            &io,
        )
        .unwrap();
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"2"[..]));
    let decoded_default = Value::Text(result.cell(0, 0).unwrap().unwrap().to_vec());
    let events = [
        Event::Insert {
            relation: key(),
            values: vec![text(b"1"), Value::Text(vec![b'x'; 1280000]), text(b"seed")],
        },
        Event::Update {
            relation: key(),
            old: None,
            new: vec![text(b"1"), Value::UnchangedToast, text(b"ordinary")],
        },
        Event::Update {
            relation: key(),
            old: Some(OldTuple {
                kind: OldTupleKind::Key,
                values: vec![text(b"1"), Value::Null, Value::Null],
            }),
            new: vec![decoded_default, Value::UnchangedToast, text(b"changed")],
        },
        Event::Update {
            relation: key(),
            old: None,
            new: vec![text(b"2"), Value::UnchangedToast, text(b"final")],
        },
    ];
    db.query("CREATE TEMP TABLE owned_identity_deferred_source (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body text, other text UNIQUE DEFERRABLE INITIALLY DEFERRED)",&io).unwrap();
    db.query("INSERT INTO owned_identity_deferred_source(body,other) VALUES ('first','left'),('second','right')",&io).unwrap();
    db.query("BEGIN", &io).unwrap();
    let result=db.query("UPDATE owned_identity_deferred_source SET id=DEFAULT,other='right' WHERE id=1 RETURNING id",&io).unwrap();
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"3"[..]));
    let first = Value::Text(result.cell(0, 0).unwrap().unwrap().to_vec());
    let result=db.query("UPDATE owned_identity_deferred_source SET id=DEFAULT,other='left' WHERE id=2 RETURNING id",&io).unwrap();
    assert_eq!(result.cell(0, 0).unwrap(), Some(&b"4"[..]));
    let second = Value::Text(result.cell(0, 0).unwrap().unwrap().to_vec());
    assert_eq!(
        db.query("COMMIT", &io).unwrap().command_tag().unwrap(),
        "COMMIT"
    );
    let permutation = [
        Event::Insert {
            relation: key(),
            values: vec![text(b"1"), text(b"first"), text(b"left")],
        },
        Event::Insert {
            relation: key(),
            values: vec![text(b"2"), text(b"second"), text(b"right")],
        },
        Event::Update {
            relation: key(),
            old: Some(OldTuple {
                kind: OldTupleKind::Key,
                values: vec![text(b"1"), Value::Null, Value::Null],
            }),
            new: vec![first, Value::UnchangedToast, text(b"right")],
        },
        Event::Update {
            relation: key(),
            old: Some(OldTuple {
                kind: OldTupleKind::Key,
                values: vec![text(b"2"), Value::Null, Value::Null],
            }),
            new: vec![second, Value::UnchangedToast, text(b"left")],
        },
    ];
    for (pipeline, table) in [
        (false, "owned_identity_serial"),
        (true, "owned_identity_pipeline"),
    ] {
        db.query(&format!("CREATE TEMP TABLE {table} (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body text, other text)"), &io).unwrap();
        let metadata = live_metadata(table, b'd', &[b'a', 0, 0]);
        live_cached_replay(
            &mut db,
            &metadata,
            &events[..1],
            &format!("{table}_oversize"),
            false,
            &io,
        );
        assert_eq!(
            live_cached_replay(&mut db, &metadata, &events[1..], table, pipeline, &io),
            ["UPDATE 1", "INSERT 0 1", "UPDATE 1"]
        );
        let result = db.query(&format!("SELECT id,body,other FROM {table} EXCEPT ALL SELECT 2,repeat('x',1280000),'final'"),&io).unwrap();
        assert_eq!(result.rows(), 0);
        let result = db.query(&format!("SELECT 2,repeat('x',1280000),'final' EXCEPT ALL SELECT id,body,other FROM {table}"),&io).unwrap();
        assert_eq!(result.rows(), 0);
        let result = db.query(&format!("SELECT a.attidentity, s.last_value, s.is_called FROM pg_catalog.pg_attribute a CROSS JOIN pg_temp.{table}_id_seq s WHERE a.attrelid='pg_temp.{table}'::pg_catalog.regclass AND a.attname='id'"), &io).unwrap();
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"a"[..]));
        assert_eq!(result.cell(0, 1).unwrap(), Some(&b"1"[..]));
        assert_eq!(result.cell(0, 2).unwrap(), Some(&b"f"[..]));
        let default_table = format!("{table}_default");
        db.query(&format!("CREATE TEMP TABLE {default_table} (id integer GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, body text, other text)"), &io).unwrap();
        let metadata = live_metadata(&default_table, b'd', &[b'd', 0, 0]);
        let default_events = [events[0].clone(), events[2].clone()];
        live_cached_replay(
            &mut db,
            &metadata,
            &default_events[..1],
            &format!("{default_table}_oversize"),
            false,
            &io,
        );
        assert_eq!(
            live_cached_replay(
                &mut db,
                &metadata,
                &default_events[1..],
                &default_table,
                pipeline,
                &io
            ),
            ["UPDATE 1"]
        );
        let result = db
            .query(
                &format!("SELECT id,octet_length(body),other FROM {default_table}"),
                &io,
            )
            .unwrap();
        assert_eq!(result.rows(), 1);
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"2"[..]));
        assert_eq!(result.cell(0, 1).unwrap(), Some(&b"1280000"[..]));
        assert_eq!(result.cell(0, 2).unwrap(), Some(&b"changed"[..]));
        let nonkey_table = format!("{table}_nonkey");
        db.query(&format!("CREATE TEMP TABLE {nonkey_table} (id integer PRIMARY KEY, body text, other integer GENERATED ALWAYS AS IDENTITY)"), &io).unwrap();
        db.query(
            &format!("INSERT INTO {nonkey_table} OVERRIDING SYSTEM VALUE VALUES (1,'kept',7)"),
            &io,
        )
        .unwrap();
        let mut metadata = live_metadata(&nonkey_table, b'd', &[0, 0, b'a']);
        metadata.target_type_oids[2] = 23;
        if let Event::Relation { columns, .. } = &mut metadata.relation {
            columns[2].type_oid = 23;
        }
        let event = Event::Update {
            relation: key(),
            old: None,
            new: vec![text(b"1"), Value::UnchangedToast, text(b"8")],
        };
        assert_eq!(
            live_cached_replay(&mut db, &metadata, &[event], &nonkey_table, pipeline, &io),
            ["INSERT 0 1"]
        );
        let result = db
            .query(&format!("SELECT id,body,other FROM {nonkey_table}"), &io)
            .unwrap();
        assert_eq!(result.rows(), 1);
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"1"[..]));
        assert_eq!(result.cell(0, 1).unwrap(), Some(&b"kept"[..]));
        assert_eq!(result.cell(0, 2).unwrap(), Some(&b"8"[..]));
        let only_table = format!("{table}_only");
        db.query(&format!("CREATE TEMP TABLE {only_table} (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"),&io).unwrap();
        db.query(
            &format!("INSERT INTO {only_table} OVERRIDING SYSTEM VALUE VALUES (5)"),
            &io,
        )
        .unwrap();
        let mut metadata = live_metadata(&only_table, b'd', b"a");
        metadata.target_type_oids.truncate(1);
        if let Event::Relation { columns, .. } = &mut metadata.relation {
            columns.truncate(1);
        }
        let event = Event::Update {
            relation: key(),
            old: None,
            new: vec![text(b"5")],
        };
        assert_eq!(
            live_cached_replay(&mut db, &metadata, &[event], &only_table, pipeline, &io),
            ["INSERT 0 1"]
        );
        let result = db
            .query(&format!("SELECT id FROM {only_table}"), &io)
            .unwrap();
        assert_eq!(result.rows(), 1);
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"5"[..]));
        let deferred_table = format!("{table}_deferred");
        db.query(&format!("CREATE TEMP TABLE {deferred_table} (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body text, other text UNIQUE DEFERRABLE INITIALLY DEFERRED)"),&io).unwrap();
        let metadata = live_metadata(&deferred_table, b'd', &[b'a', 0, 0]);
        assert_eq!(
            live_cached_replay(
                &mut db,
                &metadata,
                &permutation,
                &deferred_table,
                pipeline,
                &io
            ),
            ["INSERT 0 1"; 4]
        );
        let result=db.query(&format!("SELECT id,body,other FROM {deferred_table} EXCEPT ALL SELECT id,body,other FROM owned_identity_deferred_source"),&io).unwrap();
        assert_eq!(result.rows(), 0);
        let result=db.query(&format!("SELECT id,body,other FROM owned_identity_deferred_source EXCEPT ALL SELECT id,body,other FROM {deferred_table}"),&io).unwrap();
        assert_eq!(result.rows(), 0);
        let result = db
            .query(
                &format!("SELECT last_value,is_called FROM pg_temp.{deferred_table}_id_seq"),
                &io,
            )
            .unwrap();
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"1"[..]));
        assert_eq!(result.cell(0, 1).unwrap(), Some(&b"f"[..]));
    }
}

#[test]
#[ignore = "requires owned PostgreSQL harness"]
fn live_identity_full_and_physical_leaf_rewrites_preserve_exact_rows() {
    let io = live_control();
    let mut db = live_connection(&io);
    crate::catalog::configure_session(&mut db, &io).unwrap();
    for (pipeline, prefix) in [
        (false, "owned_identity_full_serial"),
        (true, "owned_identity_full_pipeline"),
    ] {
        db.query(&format!("CREATE TEMP TABLE {prefix} (id integer GENERATED ALWAYS AS IDENTITY, body text, other text)"),&io).unwrap();
        db.query(&format!("INSERT INTO {prefix} OVERRIDING SYSTEM VALUE VALUES (1,NULL,''),(1,NULL,''),(1,NULL,'untouched')"),&io).unwrap();
        let metadata = live_metadata(prefix, b'f', &[b'a', 0, 0]);
        let events = [Event::Update {
            relation: key(),
            old: Some(OldTuple {
                kind: OldTupleKind::Full,
                values: vec![text(b"1"), Value::Null, text(b"")],
            }),
            new: vec![text(b"2"), Value::UnchangedToast, text(b"")],
        }];
        assert_eq!(
            live_cached_replay(&mut db, &metadata, &events, prefix, pipeline, &io),
            ["INSERT 0 1"]
        );
        let result=db.query(&format!("SELECT id,body,other FROM {prefix} EXCEPT ALL SELECT * FROM (VALUES (1,NULL::text,''),(1,NULL::text,'untouched'),(2,NULL::text,'')) expected(id,body,other)"),&io).unwrap();
        assert_eq!(result.rows(), 0);
        let result=db.query(&format!("SELECT * FROM (VALUES (1,NULL::text,''),(1,NULL::text,'untouched'),(2,NULL::text,'')) expected(id,body,other) EXCEPT ALL SELECT id,body,other FROM {prefix}"),&io).unwrap();
        assert_eq!(result.rows(), 0);
        let parent = format!("{prefix}_parent");
        let leaf = format!("{prefix}_leaf");
        let sibling = format!("{prefix}_sibling");
        db.query(&format!("CREATE TEMP TABLE {parent} (id integer GENERATED ALWAYS AS IDENTITY, body text, other text) PARTITION BY LIST(other)"), &io).unwrap();
        db.query(
            &format!("CREATE TEMP TABLE {leaf} PARTITION OF {parent} FOR VALUES IN ('left')"),
            &io,
        )
        .unwrap();
        db.query(
            &format!("CREATE TEMP TABLE {sibling} PARTITION OF {parent} FOR VALUES IN ('right')"),
            &io,
        )
        .unwrap();
        let result=db.query(&format!("SELECT attidentity FROM pg_catalog.pg_attribute WHERE attrelid='pg_temp.{leaf}'::pg_catalog.regclass AND attname='id'"),&io).unwrap();
        if result.cell(0, 0).unwrap() == Some(&b""[..]) {
            db.query(
                &format!("ALTER TABLE {leaf} ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY"),
                &io,
            )
            .unwrap();
        } else {
            assert_eq!(result.cell(0, 0).unwrap(), Some(&b"a"[..]));
        }
        db.query(
            &format!("INSERT INTO {leaf} OVERRIDING SYSTEM VALUE VALUES (1,'left body','left')"),
            &io,
        )
        .unwrap();
        db.query(
            &format!(
                "INSERT INTO {sibling} OVERRIDING SYSTEM VALUE VALUES (1,'right body','right')"
            ),
            &io,
        )
        .unwrap();
        let result = db
            .query(
                &format!("SELECT p.ctid=c.ctid FROM ONLY {leaf} p CROSS JOIN ONLY {sibling} c"),
                &io,
            )
            .unwrap();
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"t"[..]));
        let metadata = live_metadata(&leaf, b'd', &[b'a', 0, 0]);
        let event = Event::Update {
            relation: key(),
            old: Some(OldTuple {
                kind: OldTupleKind::Key,
                values: vec![text(b"1"), Value::Null, Value::Null],
            }),
            new: vec![text(b"9"), Value::UnchangedToast, text(b"left")],
        };
        live_cached_replay(&mut db, &metadata, &[event], &leaf, pipeline, &io);
        let result = db
            .query(
                &format!("SELECT id,body,other FROM {parent} ORDER BY id"),
                &io,
            )
            .unwrap();
        assert_eq!(result.rows(), 2);
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"1"[..]));
        assert_eq!(result.cell(0, 1).unwrap(), Some(&b"right body"[..]));
        assert_eq!(result.cell(0, 2).unwrap(), Some(&b"right"[..]));
        assert_eq!(result.cell(1, 0).unwrap(), Some(&b"9"[..]));
        assert_eq!(result.cell(1, 1).unwrap(), Some(&b"left body"[..]));
        assert_eq!(result.cell(1, 2).unwrap(), Some(&b"left"[..]));
    }
}

#[test]
#[ignore = "requires owned PostgreSQL harness"]
fn live_identity_missing_row_refuses_result_and_failed_insert_rolls_back() {
    let io = live_control();
    let mut db = live_connection(&io);
    crate::catalog::configure_session(&mut db, &io).unwrap();
    db.query("CREATE TEMP TABLE owned_identity_rollback (id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY, body text NOT NULL, other text)",&io).unwrap();
    db.query("INSERT INTO owned_identity_rollback OVERRIDING SYSTEM VALUE VALUES (1,'original','keep'),(2,'sentinel','keep')",&io).unwrap();
    let metadata = live_metadata("owned_identity_rollback", b'd', &[b'a', 0, 0]);
    for (index, new_id, new_body) in [
        (0, b"3".as_slice(), Value::UnchangedToast),
        (1, b"2".as_slice(), Value::UnchangedToast),
        (2, b"3".as_slice(), Value::Null),
    ] {
        let old_id = if index == 0 {
            b"99".as_slice()
        } else {
            b"1".as_slice()
        };
        let event = Event::Update {
            relation: key(),
            old: Some(OldTuple {
                kind: OldTupleKind::Key,
                values: vec![text(old_id), Value::Null, Value::Null],
            }),
            new: vec![text(new_id), new_body, text(b"changed")],
        };
        let plan = DmlPlan::new(&metadata, &event).unwrap();
        let name = format!("owned_identity_rollback_{index}");
        db.prepare(&name, plan.sql(), plan.param_types(), &io)
            .unwrap();
        db.query("BEGIN", &io).unwrap();
        db.query(
            "UPDATE owned_identity_rollback SET other='earlier' WHERE id=2",
            &io,
        )
        .unwrap();
        let result = db.execute_prepared(&name, &plan.bind(&event).unwrap(), &io);
        if index == 0 {
            let result = result.unwrap();
            assert_eq!(result.rows(), 0);
            assert_eq!(result.command_tag().unwrap(), "INSERT 0 0");
            assert!(super::super::serial::require_result(&plan, result).is_err());
        } else {
            assert!(result.err().unwrap().message.contains(if index == 1 {
                "SQLSTATE 23505"
            } else {
                "SQLSTATE 23502"
            }));
        }
        db.query("ROLLBACK", &io).unwrap();
        let result = db
            .query(
                "SELECT id,body,other FROM owned_identity_rollback ORDER BY id",
                &io,
            )
            .unwrap();
        assert_eq!(result.rows(), 2);
        assert_eq!(result.cell(0, 0).unwrap(), Some(&b"1"[..]));
        assert_eq!(result.cell(0, 1).unwrap(), Some(&b"original"[..]));
        assert_eq!(result.cell(0, 2).unwrap(), Some(&b"keep"[..]));
        assert_eq!(result.cell(1, 2).unwrap(), Some(&b"keep"[..]));
    }
    let insert = Event::Insert {
        relation: key(),
        values: vec![text(b"3"), text(b"returned"), text(b"row")],
    };
    let plan = DmlPlan::new(&metadata, &insert).unwrap();
    db.query("BEGIN", &io).unwrap();
    let result=db.query("INSERT INTO owned_identity_rollback OVERRIDING SYSTEM VALUE VALUES (3,'returned','row') RETURNING id",&io).unwrap();
    assert_eq!(result.rows(), 1);
    assert_eq!(result.command_tag().unwrap(), "INSERT 0 1");
    assert!(super::super::serial::require_result(&plan, result).is_err());
    db.query("ROLLBACK", &io).unwrap();
}
