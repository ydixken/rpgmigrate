use super::*;
use crate::cdc::contracts::Value;

fn relation(columns: &[(&str, u32, bool)]) -> Event {
    Event::Relation {
        key: RelationKey {
            oid: 42,
            generation: 7,
        },
        schema: "public".into(),
        name: "data".into(),
        columns: columns
            .iter()
            .map(|(name, type_oid, key)| Column {
                name: (*name).into(),
                type_oid: *type_oid,
                type_mod: -1,
                key: *key,
            })
            .collect(),
        replica_identity: b'd',
    }
}

fn row(input: &str, relation: &Event) -> Result<Event> {
    match parse(input.as_bytes(), Limits::default())? {
        Message::Row(row) => row.decode(relation, Limits::default()),
        _ => panic!("expected a row fixture"),
    }
}

fn text(value: &str) -> Value {
    Value::Text(value.as_bytes().to_vec())
}

#[test]
fn terminal_messages_require_exact_xids_and_whole_boundaries() {
    assert!(matches!(
        parse(b"BEGIN 3", Limits::default()).unwrap(),
        Message::Begin(3)
    ));
    assert!(matches!(
        parse(b"COMMIT 4294967295", Limits::default()).unwrap(),
        Message::Commit(u32::MAX)
    ));
    for input in [
        "BEGIN",
        "COMMIT",
        "BEGIN 0",
        "BEGIN 2\nCOMMIT 2",
        "COMMIT 2 (at x)",
        "COMMIT +2",
        "COMMIT 4294967296",
        "COMMIT 2\n",
        "COMMIT 02",
    ] {
        assert!(
            parse(input.as_bytes(), Limits::default()).is_err(),
            "{input}"
        );
    }
}

#[test]
fn distinguishes_null_empty_marker_and_quoted_spellings() {
    let rel = relation(&[
        ("id", 23, true),
        ("a", 25, false),
        ("b", 25, false),
        ("c", 25, false),
        ("d", 25, false),
    ]);
    let event = row("table public.data: INSERT: id[integer]:1 a[text]:null b[text]:'null' c[text]:'' d[text]:'unchanged-toast-datum'", &rel).unwrap();
    assert_eq!(
        event,
        Event::Insert {
            relation: RelationKey {
                oid: 42,
                generation: 7
            },
            values: vec![
                text("1"),
                Value::Null,
                text("null"),
                text(""),
                text("unchanged-toast-datum")
            ]
        }
    );
}

#[test]
fn collapses_apostrophes_once_and_preserves_backslashes_and_newlines() {
    let rel = relation(&[("id", 23, true), ("v", 25, false)]);
    let event = row(
        "table public.data: INSERT: id[integer]:1 v[text]:'a''''b''\\n\\\\\nCOMMIT 99'",
        &rel,
    )
    .unwrap();
    let Event::Insert { values, .. } = event else {
        panic!()
    };
    assert_eq!(values[1], text("a''b'\\n\\\\\nCOMMIT 99"));
}

#[test]
fn quoted_identifiers_preserve_dots_brackets_colons_and_double_quotes() {
    let mut rel = relation(&[("time", 23, true), ("[column:name].\"", 25, false)]);
    if let Event::Relation { schema, name, .. } = &mut rel {
        *schema = "Foo\".Bar".into();
        *name = ":Identifier [As] \"Column\".$1:".into();
    }
    let input = "table \"Foo\"\".Bar\".\":Identifier [As] \"\"Column\"\".$1:\": INSERT: \"time\"[integer]:1 \"[column:name].\"\"\"[text]:'value'";
    assert!(
        matches!(row(input, &rel).unwrap(), Event::Insert { values, .. } if values == vec![text("1"), text("value")])
    );
}

#[test]
fn type_labels_are_delimited_outside_quotes_and_array_brackets() {
    for label in [
        "text[]",
        "text[][]",
        "\"S.[x]\".\"T]\"[]",
        "\"a\"\"[b]\"[][]",
        "timestamp with time zone",
    ] {
        let input = format!("[{label}]:tail");
        let mut cursor = Cursor::new(&input);
        assert_eq!(cursor.type_label().unwrap(), label);
        assert_eq!(cursor.remaining(), ":tail");
    }
    for input in ["[text[]:x", "[\"open]:x", "[]:x", "[text]]:x"] {
        let mut cursor = Cursor::new(input);
        assert!(
            cursor
                .type_label()
                .and_then(|_| cursor.expect(":"))
                .is_err()
        );
    }
}

#[test]
fn update_without_old_key_keeps_none_and_preserves_toast() {
    let rel = relation(&[("id", 23, true), ("v", 25, false)]);
    assert_eq!(
        row(
            "table public.data: UPDATE: id[integer]:7 v[text]:unchanged-toast-datum",
            &rel
        )
        .unwrap(),
        Event::Update {
            relation: RelationKey {
                oid: 42,
                generation: 7
            },
            old: None,
            new: vec![text("7"), Value::UnchangedToast]
        }
    );
}

#[test]
fn old_keys_follow_persisted_column_order() {
    let rel = relation(&[("z", 23, true), ("v", 25, false), ("a", 23, true)]);
    let input = "table public.data: UPDATE: old-key: z[integer]:2 a[integer]:9 new-tuple: z[integer]:3 v[text]:'changed' a[integer]:10";
    let Event::Update { old, new, .. } = row(input, &rel).unwrap() else {
        panic!()
    };
    assert_eq!(old, Some(vec![text("2"), text("9")]));
    assert_eq!(new, vec![text("3"), text("changed"), text("10")]);
    assert!(
        row(
            "table public.data: DELETE: z[integer]:3 a[integer]:10",
            &rel
        )
        .is_ok()
    );
    assert!(
        row(
            "table public.data: DELETE: a[integer]:10 z[integer]:3",
            &rel
        )
        .is_err()
    );
}

#[test]
fn builtin_output_keeps_numeric_precision_bool_and_bit_values() {
    let rel = relation(&[
        ("id", 23, true),
        ("f", 701, false),
        ("n", 1700, false),
        ("b", 16, false),
        ("bits", 1562, false),
        ("when", 1184, false),
    ]);
    let event = row("table public.data: INSERT: id[integer]:-1 f[double precision]:-216237.00000035969 n[numeric]:NaN b[boolean]:false bits[bit varying]:B'101' \"when\"[timestamp with time zone]:'2026-01-01 00:00:00+00'", &rel).unwrap();
    let Event::Insert { values, .. } = event else {
        panic!()
    };
    assert_eq!(
        values,
        vec![
            text("-1"),
            text("-216237.00000035969"),
            text("NaN"),
            text("false"),
            text("101"),
            text("2026-01-01 00:00:00+00")
        ]
    );
}

#[test]
fn truncate_preserves_quoted_relation_list_and_exact_flags() {
    for (flags, cascade, restart_identity) in [
        ("(no-flags)", false, false),
        ("restart_seqs", false, true),
        ("cascade", true, false),
        ("restart_seqs cascade", true, true),
    ] {
        let input = format!("table public.a, \"S p\".\"B, c:[].\": TRUNCATE: {flags}");
        let Message::Truncate {
            tables,
            cascade: got_cascade,
            restart_identity: got_restart,
        } = parse(input.as_bytes(), Limits::default()).unwrap()
        else {
            panic!()
        };
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[1].schema, "S p");
        assert_eq!(tables[1].table, "B, c:[].");
        assert_eq!((got_cascade, got_restart), (cascade, restart_identity));
    }
    for flags in [
        "",
        "cascade restart_seqs",
        "(no-flags) cascade",
        "cascade cascade",
        "restart_identity",
        "restart_seqs ",
    ] {
        assert!(
            parse(
                format!("table public.a: TRUNCATE: {flags}").as_bytes(),
                Limits::default()
            )
            .is_err()
        );
    }
}

#[test]
fn refuses_unsupported_missing_and_ambiguous_tuple_evidence() {
    let rel = relation(&[("id", 23, true), ("v", 25, false)]);
    for input in [
        "table public.data: INSERT: (no-tuple-data)",
        "table public.data: INSERT: id[integer]:1",
        "table public.data: INSERT: v[text]:'x' id[integer]:1",
        "table public.data: INSERT: id[bigint]:1 v[text]:'x'",
        "table public.data: INSERT: id[integer]:null v[text]:'x'",
        "table public.data: INSERT: id[integer]:1 v[text]:unchanged-toast-datum",
        "table public.data: INSERT: id[integer]:1 v[text]:bare",
        "table public.data: INSERT: id[integer]:1 v[text]:'unterminated",
        "table public.data: INSERT: id[integer]:1 v[text]:'x' v[text]:'again'",
        "table public.data: INSERT: oid[oid]:9 id[integer]:1 v[text]:'x'",
        "table public.data: UPDATE: id[integer]:unchanged-toast-datum v[text]:'x'",
        "table public.data: UPDATE: old-key: id[integer]:null new-tuple: id[integer]:2 v[text]:'x'",
        "table public.data: UPDATE: old-key: new-tuple: id[integer]:2 v[text]:'x'",
        "table public.data: DELETE: (no-tuple-data)",
        "table public.data: DELETE: id[integer]:1 v[text]:'x'",
        "table public.data: DELETE: id[integer]:1\nCOMMIT 9",
    ] {
        assert!(row(input, &rel).is_err(), "{input}");
    }
}

#[test]
fn refuses_full_keyless_wrong_name_and_unsupported_types() {
    for change in 0..5 {
        let mut rel = relation(&[("id", 23, true), ("v", 25, false)]);
        if let Event::Relation {
            replica_identity,
            name,
            columns,
            ..
        } = &mut rel
        {
            match change {
                0 => *replica_identity = b'f',
                1 => columns[0].key = false,
                2 => *name = "other".into(),
                3 => columns[1].type_oid = 1009,
                _ => columns[1].name = "id".into(),
            }
        }
        assert!(row("table public.data: INSERT: id[integer]:1 v[text]:'x'", &rel).is_err());
    }
}

#[test]
fn malformed_control_and_utf8_do_not_leak_row_contents() {
    for input in [
        b"message: transactional: 1 prefix: private, sz: 1 content:x".as_slice(),
        b"opening a streamed block for transaction TXN 3",
        b"PREPARE TRANSACTION 'private', txid 3",
        b"table public.data: INSERT: v[text]:'\xff'",
        b"table public.data: INSERT: v[text]:'a\0b'",
    ] {
        let error = match parse(input, Limits::default()) {
            Err(error) => error,
            Ok(_) => panic!("unsupported message was accepted"),
        };
        assert!(!error.message.contains("private"));
        assert_eq!(error.kind, ErrorKind::Source);
    }
}

#[test]
fn exact_relation_payload_limit_does_not_refuse_a_smaller_row() {
    let rel = relation(&[("id", 23, true), ("v", 25, false)]);
    let limits = Limits {
        max_event_bytes: 66,
        ..Limits::default()
    };
    let Message::Row(row) = parse(
        b"table public.data: INSERT: id[integer]:1 v[text]:'x'",
        limits,
    )
    .unwrap() else {
        panic!()
    };
    assert!(row.decode(&rel, limits).is_ok());
}

#[test]
fn unchanged_toast_is_refused_for_fixed_width_builtin_types() {
    for (oid, name) in [
        (23, "integer"),
        (16, "boolean"),
        (701, "double precision"),
        (1082, "date"),
        (2950, "uuid"),
        (1186, "interval"),
    ] {
        let rel = relation(&[("id", 23, true), ("v", oid, false)]);
        assert!(
            row(
                &format!(
                    "table public.data: UPDATE: id[integer]:1 v[{name}]:unchanged-toast-datum"
                ),
                &rel
            )
            .is_err(),
            "{name}"
        );
    }
}

#[test]
fn every_incomplete_row_is_rejected_and_no_value_suffix_is_ignored() {
    let rel = relation(&[("id", 23, true), ("v", 25, false)]);
    let input = "table public.data: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:2 v[text]:'normal value'";
    for end in 0..input.len() {
        assert!(row(&input[..end], &rel).is_err(), "{end}");
    }
    for suffix in [" ", "\n", "\t", "]", " COMMIT 3", " new-tuple:"] {
        assert!(row(&format!("{input}{suffix}"), &rel).is_err());
    }
}

#[test]
fn numeric_and_bit_tokens_refuse_malformed_or_quoted_raw_output() {
    for (oid, name, bad) in [
        (23, "integer", "'1'"),
        (23, "integer", "1x"),
        (23, "integer", "+1"),
        (701, "double precision", "1e"),
        (701, "double precision", "NaNx"),
        (16, "boolean", "t"),
        (16, "boolean", "'true'"),
        (1560, "bit", "B'102'"),
        (1562, "bit varying", "'101'"),
        (1562, "bit varying", "B'1''0'"),
    ] {
        let rel = relation(&[("id", 23, true), ("v", oid, false)]);
        assert!(
            row(
                &format!("table public.data: INSERT: id[integer]:1 v[{name}]:{bad}"),
                &rel
            )
            .is_err()
        );
    }
}

#[test]
fn pinned_postgresql_regression_messages_decode_without_line_splitting() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/cdc/test_decoding/postgresql-18.6.json"
    ))
    .unwrap();
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 6);
    let mut rel = relation(&[
        ("id", 23, true),
        ("somedata", 23, false),
        ("text", 1043, false),
    ]);
    if let Event::Relation { name, .. } = &mut rel {
        *name = "replication_example".into();
    }
    assert!(
        matches!(row(cases[0]["message"].as_str().unwrap(), &rel).unwrap(), Event::Insert { values, .. } if values == vec![text("1"),text("1"),text("1")])
    );
    let mut rel = relation(&[
        ("id", 23, true),
        ("toasted_col1", 25, false),
        ("rand1", 701, false),
        ("toasted_col2", 25, false),
        ("rand2", 701, false),
    ]);
    if let Event::Relation { name, .. } = &mut rel {
        *name = "xpto".into();
    }
    assert!(
        matches!(row(cases[1]["message"].as_str().unwrap(), &rel).unwrap(), Event::Update { old: None, new, .. } if new == vec![text("1"), Value::UnchangedToast,text("123.456"),Value::UnchangedToast,text("1578")])
    );
    assert!(
        matches!(row(cases[2]["message"].as_str().unwrap(), &rel).unwrap(), Event::Delete { old, .. } if old == vec![text("1")])
    );
    for case in &cases[3..] {
        assert!(matches!(
            parse(
                case["message"].as_str().unwrap().as_bytes(),
                Limits::default()
            )
            .unwrap(),
            Message::Truncate { .. }
        ));
    }
}
