//! The authority owns the transaction, writer fence, and durability proof.
//! Namespace DDL must remain paused while these owned relations are in use.

use super::checkpoint::{DrainKind, DrainRecord};
use super::{ControlRow, Frontier};
use crate::contracts::{
    Error, ErrorKind, IoControl, MAX_SNAPSHOT_TOKEN_BYTES, Result, SnapshotAnchor,
};
use crate::identity::{CommitLsn, SourceEndLsn, StreamId, TxId, TxSeq};
use crate::pg::{PgConnection, PgParam, PgResult, TransactionStatus};
use crate::state::{ConfirmedProvenance, ProvisioningIntent, TARGET_METADATA_VERSION};

const CATALOG: &str = "rpgmigrate_metadata_catalog";
const READ: &str = "rpgmigrate_metadata_read";
const UPDATE: &str = "rpgmigrate_metadata_update";
const NUMERIC_U64: i32 = (20 << 16) + 4;
// PGDG 16.16 and 18.7 stamp each new column with a server-owned pg_attver that SET and RESET leave unchanged.
// Any other column option, or a second entry, is still foreign.
const ATTOPTIONS: &str = "(a.attoptions IS NULL OR (pg_catalog.cardinality(a.attoptions) = 1 AND a.attoptions[1] OPERATOR(pg_catalog.~) '^pg_attver=[0-9]+$'))";
const COLUMNS: [(&str, &str, u32, i32, bool); 29] = [
    ("magic", "text", 25, -1, true),
    ("version", "int4", 23, -1, true),
    ("run_id", "bytea", 17, -1, true),
    ("nonce", "bytea", 17, -1, true),
    ("source_system", "numeric", 1700, NUMERIC_U64, true),
    ("source_database", "oid", 26, -1, true),
    ("generation", "numeric", 1700, NUMERIC_U64, true),
    ("target_system", "numeric", 1700, NUMERIC_U64, false),
    ("target_database", "oid", 26, -1, true),
    ("owner_oid", "oid", 26, -1, true),
    ("schema_oid", "oid", 26, -1, true),
    ("table_oid", "oid", 26, -1, true),
    ("applied_order", "numeric", 1700, NUMERIC_U64, true),
    ("last_xid", "oid", 26, -1, false),
    ("begin_ordinal", "numeric", 1700, NUMERIC_U64, false),
    ("source_commit_lsn", "pg_lsn", 3220, -1, false),
    ("source_end_lsn", "pg_lsn", 3220, -1, false),
    ("barrier_epoch", "numeric", 1700, NUMERIC_U64, true),
    ("barrier_counter", "numeric", 1700, NUMERIC_U64, true),
    ("copy_snapshot_token", "text", 25, -1, false),
    ("copy_consistent_point", "pg_lsn", 3220, -1, false),
    ("copy_plan_nonce", "bytea", 17, -1, false),
    ("clone_state", "text", 25, -1, false),
    ("certified_lsn", "pg_lsn", 3220, -1, false),
    ("follow_binding", "text", 25, -1, false),
    ("drain_end_lsn", "pg_lsn", 3220, -1, false),
    ("drain_witness_lsn", "pg_lsn", 3220, -1, false),
    ("drain_kind", "text", 25, -1, false),
    ("follow_completed", "bool", 16, -1, true),
];

pub(super) fn probe(
    connection: &mut PgConnection,
    intent: &ProvisioningIntent,
    io: &IoControl,
) -> Result<Option<ConfirmedProvenance>> {
    in_transaction(connection)?;
    validate_intent(intent)?;
    let Some(confirmed) = locate(connection, intent, io)? else {
        return Ok(None);
    };
    let table = lock_table(connection, &confirmed, io)?;
    let version = server_version(connection, io)?;
    let catalog = query_once(
        connection,
        &catalog_sql(version)?,
        &catalog_values(&confirmed),
        io,
    )?;
    require_catalog(&catalog)?;
    validate_receipt_catalogs(connection, &confirmed, io)?;
    read_row(&connection.query(&read_sql(&table), io)?, &confirmed)?;
    Ok(Some(confirmed))
}

pub(super) fn create(
    connection: &mut PgConnection,
    intent: &ProvisioningIntent,
    io: &IoControl,
) -> Result<ConfirmedProvenance> {
    in_transaction(connection)?;
    validate_intent(intent)?;
    let binding = &intent.binding;
    let context = query_once(
        connection,
        "SELECT r.rolname, d.oid, pg_catalog.has_database_privilege(d.oid, 'CREATE')
         FROM pg_catalog.pg_roles r CROSS JOIN pg_catalog.pg_database d
         WHERE r.oid = $1::pg_catalog.oid AND d.datname = pg_catalog.current_database()",
        &[Some(binding.owner_oid.to_string())],
        io,
    )?;
    if context.rows() != 1
        || decimal(required(context.cell(0, 1)?)?)? != u64::from(binding.target_database_oid)
        || context.cell(0, 2)? != Some(b"t")
    {
        return Err(refusal(
            "target-metadata: target identity or creation privilege is invalid",
        ));
    }
    let owner = std::str::from_utf8(required(context.cell(0, 0)?)?)
        .map_err(|_| refusal("target-metadata: invalid owner name"))?;
    let owner = connection.quote_identifier(owner)?;
    let schema = connection.quote_identifier(&binding.schema_name)?;
    let table = qualified(connection, intent)?;
    let version = server_version(connection, io)?;
    let sql = format!(
        "CREATE SCHEMA {schema} AUTHORIZATION {owner} {}",
        create_table_sql(&table, version)
    );
    command(connection, &sql, "CREATE SCHEMA", io)?;
    command(
        connection,
        &format!("REVOKE ALL ON SCHEMA {schema} FROM PUBLIC"),
        "REVOKE",
        io,
    )?;
    command(
        connection,
        &format!("REVOKE ALL ON TABLE {table} FROM PUBLIC"),
        "REVOKE",
        io,
    )?;
    let marker = marker_qualified(connection, intent)?;
    command(connection, &marker_create_sql(&marker), "CREATE TABLE", io)?;
    command(
        connection,
        &format!("REVOKE ALL ON TABLE {marker} FROM PUBLIC"),
        "REVOKE",
        io,
    )?;
    let objects = large_object_qualified(connection, intent)?;
    command(
        connection,
        &large_object_create_sql(&objects),
        "CREATE TABLE",
        io,
    )?;
    command(
        connection,
        &format!("REVOKE ALL ON TABLE {objects} FROM PUBLIC"),
        "REVOKE",
        io,
    )?;
    let confirmed = locate(connection, intent, io)?
        .ok_or_else(|| refusal("target-metadata: created namespace is absent"))?;
    validate_receipt_catalogs(connection, &confirmed, io)?;
    lock_table(connection, &confirmed, io)?;
    require_catalog(&query_once(
        connection,
        &catalog_sql(version)?,
        &catalog_values(&confirmed),
        io,
    )?)?;
    let values = row_values(&confirmed, &empty_row())?;
    let insert = format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        column_names(),
        placeholders()
    );
    let result = query_once(connection, &insert, &values, io)?;
    require_tag(&result, "INSERT 0 1")?;
    read_row(&connection.query(&read_sql(&table), io)?, &confirmed)?;
    Ok(confirmed)
}

pub(super) fn prepare(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<()> {
    in_transaction(connection)?;
    let table = lock_table(connection, confirmed, io)?;
    let version = server_version(connection, io)?;
    connection.prepare(CATALOG, &catalog_sql(version)?, &[], io)?;
    require_catalog(&execute(
        connection,
        CATALOG,
        &catalog_values(confirmed),
        io,
    )?)?;
    validate_receipt_catalogs(connection, confirmed, io)?;
    connection.prepare(READ, &read_sql(&table), &[], io)?;
    connection.prepare(UPDATE, &update_sql(&table), &[], io)?;
    Ok(())
}

pub(super) fn lock_validate(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<ControlRow> {
    in_transaction(connection)?;
    lock_table(connection, confirmed, io)?;
    require_catalog(&execute(
        connection,
        CATALOG,
        &catalog_values(confirmed),
        io,
    )?)?;
    validate_receipt_catalogs(connection, confirmed, io)?;
    read_row(&connection.execute_prepared(READ, &[], io)?, confirmed)
}

pub(super) fn owned_relation_oids(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<[u32; 6]> {
    read_only(connection, confirmed, io)?;
    let result = query_once(
        connection,
        "SELECT indexrelid FROM pg_catalog.pg_index
         WHERE indrelid = $1::pg_catalog.oid AND indisprimary LIMIT 2",
        &[Some(confirmed.table_oid.to_string())],
        io,
    )?;
    if result.columns()? != 1 {
        return Err(refusal(
            "target-metadata: invalid owned primary index identity",
        ));
    }
    let index = if result.rows() == 1 {
        result.cell(0, 0)?
    } else {
        None
    };
    let [table, index] = owned_relation_pair(confirmed.table_oid, result.rows(), index)?;
    let identities = [
        table,
        index,
        confirmed.marker_table_oid,
        confirmed.marker_index_oid,
        confirmed.large_object_table_oid,
        confirmed.large_object_index_oid,
    ];
    if identities.contains(&0)
        || identities
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != identities.len()
    {
        return Err(refusal(
            "target-metadata: owned relation identities overlap",
        ));
    }
    Ok(identities)
}

fn owned_relation_pair(table: u32, rows: usize, index: Option<&[u8]>) -> Result<[u32; 2]> {
    if table == 0 || rows != 1 {
        return Err(refusal(
            "target-metadata: invalid owned primary index identity",
        ));
    }
    let index = oid(required(index)?)?;
    if index == table {
        return Err(refusal(
            "target-metadata: invalid owned primary index identity",
        ));
    }
    Ok([table, index])
}

pub(super) fn update(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    old: &ControlRow,
    next: &ControlRow,
    io: &IoControl,
) -> Result<()> {
    in_transaction(connection)?;
    let values = update_values(confirmed, old, next)?;
    require_tag(&execute(connection, UPDATE, &values, io)?, "UPDATE 1")
}

pub(super) fn checkpoint_input_bytes(
    confirmed: &ConfirmedProvenance,
    old: &ControlRow,
    next: &ControlRow,
) -> Result<usize> {
    update_values(confirmed, old, next)?
        .iter()
        .try_fold(UPDATE.len() + 1, |bytes, value| {
            bytes
                .checked_add(value.as_ref().map_or(4, |value| value.len() + 5))
                .ok_or_else(|| refusal("target-metadata: checkpoint input size overflow"))
        })
}

pub(super) fn pipeline_update(
    pipeline: &mut crate::pg::Pipeline<'_>,
    confirmed: &ConfirmedProvenance,
    old: &ControlRow,
    next: &ControlRow,
    io: &IoControl,
) -> Result<()> {
    let values = update_values(confirmed, old, next)?;
    let params = values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map_or(PgParam::Null, |value| PgParam::Text(value.as_bytes()))
        })
        .collect::<Vec<_>>();
    pipeline.execute_prepared(UPDATE, &params, io)
}

pub(super) fn validate_update(result: &PgResult) -> Result<()> {
    require_tag(result, "UPDATE 1")
}

pub(super) fn read_prepared(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<ControlRow> {
    read_row(&connection.execute_prepared(READ, &[], io)?, confirmed)
}

fn update_values(
    confirmed: &ConfirmedProvenance,
    old: &ControlRow,
    next: &ControlRow,
) -> Result<Vec<Option<String>>> {
    if old.follow_completed && old != next {
        return Err(refusal("target-metadata: completed follow is immutable"));
    }
    if old.drain.is_some() && old.drain != next.drain {
        return Err(refusal("target-metadata: sealed drain cannot change"));
    }
    if old.anchor.is_some() && old.anchor != next.anchor {
        return Err(refusal(
            "target-metadata: snapshot anchor cannot be replaced or removed",
        ));
    }
    if old.anchor.is_none()
        && next.anchor.is_some()
        && (old.applied.is_some() || next.applied.is_some())
    {
        return Err(refusal("target-metadata: snapshot anchor must precede CDC"));
    }
    if old.follow_binding.is_some() && old.follow_binding != next.follow_binding {
        let recopy = old
            .follow_binding
            .as_ref()
            .zip(next.follow_binding.as_ref())
            .is_some_and(|(a, b)| {
                a.binding.stream() == b.binding.stream()
                    && a.binding.source_timeline == b.binding.source_timeline
                    && a.binding.source_types == b.binding.source_types
                    && a.operator == b.operator
                    && a.origin_name == b.origin_name
                    && a.binding.copy_generation.0.checked_add(1)
                        == Some(b.binding.copy_generation.0)
                    && a.binding.slot.intent.nonce != b.binding.slot.intent.nonce
            })
            && old.anchor.is_none()
            && next.anchor.is_none()
            && old.applied.is_none()
            && next.applied.is_none()
            && old.certified_lsn.is_none()
            && next.certified_lsn.is_none()
            && old
                .clone
                .as_ref()
                .zip(next.clone.as_ref())
                .is_some_and(|(a, b)| {
                    a.phase == crate::state::ClonePhase::Copying
                        && b.phase == crate::state::ClonePhase::Copying
                        && a.generation.0.checked_add(1) == Some(b.generation.0)
                        && a.source_timeline == b.source_timeline
                        && a.table_count == b.table_count
                });
        if !recopy {
            return Err(refusal(
                "target-metadata: follow binding cannot be replaced or removed",
            ));
        }
    }
    if old.certified_lsn > next.certified_lsn {
        return Err(refusal(
            "target-metadata: certified frontier cannot move backwards",
        ));
    }
    let mut values = row_values(confirmed, next)?;
    values.extend(row_values(confirmed, old)?.into_iter().skip(12));
    Ok(values)
}

fn in_transaction(connection: &PgConnection) -> Result<()> {
    if connection.transaction_status()? != TransactionStatus::InTransaction {
        return Err(refusal(
            "target-metadata: an active caller transaction is required",
        ));
    }
    Ok(())
}

fn locate(
    connection: &mut PgConnection,
    intent: &ProvisioningIntent,
    io: &IoControl,
) -> Result<Option<ConfirmedProvenance>> {
    let result = query_once(connection,
        "SELECT d.oid, n.oid, c.oid, c.relkind FROM pg_catalog.pg_database d
         LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = $1::pg_catalog.text
         LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = $2::pg_catalog.text
         WHERE d.datname = pg_catalog.current_database()",
        &[Some(intent.binding.schema_name.clone()), Some(intent.binding.table_name.clone())], io)?;
    if result.rows() != 1 {
        return Err(refusal(
            "target-metadata: target database identity is absent",
        ));
    }
    let found = decode_location(
        &[
            result.cell(0, 0)?,
            result.cell(0, 1)?,
            result.cell(0, 2)?,
            result.cell(0, 3)?,
        ],
        intent,
    )?;
    let Some(mut confirmed) = found else {
        return Ok(None);
    };
    let marker = query_once(
        connection,
        "SELECT c.oid, i.indexrelid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid=c.oid AND i.indisprimary WHERE c.relnamespace=$1::oid AND c.relname='copy_done' AND c.relkind='r'",
        &[Some(confirmed.schema_oid.to_string())],
        io,
    )?;
    if marker.rows() != 1 {
        return Err(refusal(
            "target-metadata: marker table identity is absent or ambiguous",
        ));
    }
    confirmed.marker_table_oid = oid(required(marker.cell(0, 0)?)?)?;
    confirmed.marker_index_oid = oid(required(marker.cell(0, 1)?)?)?;
    let objects = query_once(
        connection,
        "SELECT c.oid, i.indexrelid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid=c.oid AND i.indisprimary WHERE c.relnamespace=$1::oid AND c.relname='large_object_done' AND c.relkind='r'",
        &[Some(confirmed.schema_oid.to_string())],
        io,
    )?;
    if objects.rows() != 1 {
        return Err(refusal(
            "target-metadata: large-object ledger identity is absent or ambiguous",
        ));
    }
    confirmed.large_object_table_oid = oid(required(objects.cell(0, 0)?)?)?;
    confirmed.large_object_index_oid = oid(required(objects.cell(0, 1)?)?)?;
    Ok(Some(confirmed))
}

fn decode_location(
    cells: &[Option<&[u8]>; 4],
    intent: &ProvisioningIntent,
) -> Result<Option<ConfirmedProvenance>> {
    if oid(required(cells[0])?)? != intent.binding.target_database_oid {
        return Err(refusal(
            "target-metadata: target database identity does not match",
        ));
    }
    if cells[1..].iter().all(Option::is_none) {
        return Ok(None);
    }
    if cells[3] != Some(b"r") {
        return Err(refusal(
            "target-metadata: namespace is partial or contains a foreign object",
        ));
    }
    Ok(Some(ConfirmedProvenance {
        intent: intent.clone(),
        schema_oid: oid(required(cells[1])?)?,
        table_oid: oid(required(cells[2])?)?,
        marker_table_oid: 0,
        marker_index_oid: 0,
        large_object_table_oid: 0,
        large_object_index_oid: 0,
    }))
}

fn oid(bytes: &[u8]) -> Result<u32> {
    u32::try_from(decimal(bytes)?)
        .ok()
        .filter(|value| *value != 0)
        .ok_or_else(|| refusal("target-metadata: invalid object identity"))
}

fn server_version(connection: &mut PgConnection, io: &IoControl) -> Result<u64> {
    let result = connection.query("SHOW server_version_num", io)?;
    if result.rows() != 1 {
        return Err(refusal("target-metadata: server version is absent"));
    }
    let version = decimal(required(result.cell(0, 0)?)?)?;
    if version < 90600 {
        return Err(refusal("target-metadata: unsupported catalog version"));
    }
    Ok(version)
}

fn qualified(connection: &mut PgConnection, intent: &ProvisioningIntent) -> Result<String> {
    Ok(format!(
        "{}.{}",
        connection.quote_identifier(&intent.binding.schema_name)?,
        connection.quote_identifier(&intent.binding.table_name)?
    ))
}

fn lock_table(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<String> {
    row_values(confirmed, &empty_row())?;
    let table = qualified(connection, &confirmed.intent)?;
    command(
        connection,
        &format!("LOCK TABLE ONLY {table} IN SHARE ROW EXCLUSIVE MODE"),
        "LOCK TABLE",
        io,
    )?;
    Ok(table)
}

fn query_once(
    connection: &mut PgConnection,
    sql: &str,
    values: &[Option<String>],
    io: &IoControl,
) -> Result<PgResult> {
    // The unnamed slot keeps setup queries separate from the reusable authority statements.
    connection.prepare("", sql, &[], io)?;
    execute(connection, "", values, io)
}

fn execute(
    connection: &mut PgConnection,
    name: &str,
    values: &[Option<String>],
    io: &IoControl,
) -> Result<PgResult> {
    let params = values
        .iter()
        .map(|value| {
            value
                .as_ref()
                .map_or(PgParam::Null, |text| PgParam::Text(text.as_bytes()))
        })
        .collect::<Vec<_>>();
    connection.execute_prepared(name, &params, io)
}

fn command(connection: &mut PgConnection, sql: &str, tag: &str, io: &IoControl) -> Result<()> {
    require_tag(&connection.query(sql, io)?, tag)
}

fn require_tag(result: &PgResult, tag: &str) -> Result<()> {
    if result.command_tag()? != tag {
        return Err(refusal(
            "target-metadata: unexpected command result or affected row count",
        ));
    }
    Ok(())
}

fn require_catalog(result: &PgResult) -> Result<()> {
    if result.rows() != 1 || result.cell(0, 0)? != Some(b"t") {
        return Err(refusal(
            "target-metadata: object identity, schema, or privileges are unsafe",
        ));
    }
    Ok(())
}

fn catalog_values(confirmed: &ConfirmedProvenance) -> Vec<Option<String>> {
    let binding = &confirmed.intent.binding;
    vec![
        Some(confirmed.schema_oid.to_string()),
        Some(confirmed.table_oid.to_string()),
        Some(binding.target_database_oid.to_string()),
        Some(binding.owner_oid.to_string()),
        Some(binding.schema_name.clone()),
        Some(binding.table_name.clone()),
        Some(confirmed.marker_table_oid.to_string()),
        Some(confirmed.marker_index_oid.to_string()),
        Some(confirmed.large_object_table_oid.to_string()),
        Some(confirmed.large_object_index_oid.to_string()),
    ]
}

fn read_row(result: &PgResult, confirmed: &ConfirmedProvenance) -> Result<ControlRow> {
    if result.rows() != 1 || result.columns()? != COLUMNS.len() {
        return Err(refusal(
            "target-metadata: expected exactly one complete control row",
        ));
    }
    let cells = (0..COLUMNS.len())
        .map(|column| result.cell(0, column))
        .collect::<Result<Vec<_>>>()?;
    decode_row(&cells, confirmed)
}

fn read_sql(table: &str) -> String {
    let columns = COLUMNS.iter().map(|column| match column.0 {
        "magic" => "CASE WHEN pg_catalog.octet_length(magic) < 64 THEN magic END".into(),
        "run_id" | "nonce" => format!("CASE WHEN pg_catalog.octet_length({0}) = 16 THEN pg_catalog.encode({0}, 'hex') END", column.0),
        // Invalid present optional fields must not become an absent anchor.
        "follow_binding" => "CASE WHEN follow_binding IS NULL THEN NULL WHEN pg_catalog.octet_length(follow_binding) <= 262144 THEN follow_binding ELSE '' END".into(),
        "drain_kind" => "CASE WHEN drain_kind IS NULL THEN NULL WHEN pg_catalog.octet_length(drain_kind) <= 11 THEN drain_kind ELSE '' END".into(),
        "clone_state" => "CASE WHEN clone_state IS NULL THEN NULL WHEN pg_catalog.octet_length(clone_state) <= 4096 THEN clone_state ELSE '' END".into(),
        "copy_snapshot_token" => format!("CASE WHEN copy_snapshot_token IS NULL THEN NULL WHEN pg_catalog.octet_length(copy_snapshot_token) <= {MAX_SNAPSHOT_TOKEN_BYTES} THEN copy_snapshot_token ELSE '' END"),
        "copy_plan_nonce" => "CASE WHEN copy_plan_nonce IS NULL THEN NULL WHEN pg_catalog.octet_length(copy_plan_nonce) = 16 THEN pg_catalog.encode(copy_plan_nonce, 'hex') ELSE '' END".into(),
        name => name.to_owned(),
    }).collect::<Vec<_>>().join(", ");
    format!("SELECT {columns} FROM ONLY {table} LIMIT 2 FOR UPDATE")
}

fn create_table_sql(table: &str, version: u64) -> String {
    let columns = COLUMNS
        .iter()
        .map(|column| {
            let modifier = if column.3 == NUMERIC_U64 {
                "(20,0)"
            } else {
                ""
            };
            let collation = if column.2 == 25 {
                " COLLATE pg_catalog.\"C\""
            } else {
                ""
            };
            let required = if column.4 { " NOT NULL" } else { "" };
            format!(
                "{} pg_catalog.{}{modifier}{collation}{required}",
                column.0, column.1
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let heap = if version >= 120000 {
        " USING heap"
    } else {
        " WITHOUT OIDS"
    };
    format!(
        "CREATE TABLE {table} ({columns}, PRIMARY KEY (run_id, source_system, source_database, generation)){heap}"
    )
}

fn column_names() -> String {
    COLUMNS
        .iter()
        .map(|column| column.0)
        .collect::<Vec<_>>()
        .join(", ")
}

fn placeholders() -> String {
    COLUMNS
        .iter()
        .enumerate()
        .map(|(index, column)| format!("${}::pg_catalog.{}", index + 1, column.1))
        .collect::<Vec<_>>()
        .join(", ")
}

fn decode_row(cells: &[Option<&[u8]>], confirmed: &ConfirmedProvenance) -> Result<ControlRow> {
    if cells.len() != COLUMNS.len() {
        return Err(refusal("target-metadata: invalid control row width"));
    }
    let mut expected = row_values(confirmed, &empty_row())?;
    expected[2] = Some(hex(&confirmed.intent.binding.stream.run.0));
    expected[3] = Some(hex(&confirmed.intent.nonce));
    if cells[..12]
        .iter()
        .zip(&expected[..12])
        .any(|(actual, wanted)| *actual != wanted.as_ref().map(|value| value.as_bytes()))
    {
        return Err(refusal(
            "target-metadata: creation receipt does not match provenance",
        ));
    }
    let order = decimal(required(cells[12])?)?;
    let applied = if order == 0 {
        if cells[13..17].iter().any(Option::is_some) {
            return Err(refusal(
                "target-metadata: empty frontier has transaction fields",
            ));
        }
        None
    } else {
        let xid = u32::try_from(decimal(required(cells[13])?)?)
            .map_err(|_| refusal("target-metadata: invalid transaction identity"))?;
        let begin_ordinal = decimal(required(cells[14])?)?;
        let commit = cells[15].map(lsn).transpose()?.map(CommitLsn);
        let end = lsn(required(cells[16])?)?;
        if commit.is_some_and(|commit| end < commit.0) {
            return Err(refusal(
                "target-metadata: source positions are inconsistent",
            ));
        }
        Some(Frontier {
            tx: TxId {
                stream: confirmed.intent.binding.stream,
                xid,
                begin_ordinal,
            },
            order: TxSeq(order),
            commit,
            end: SourceEndLsn(end),
        })
    };
    let row = ControlRow {
        drain: decode_drain(&cells[25..28])?,
        follow_completed: match required(cells[28])? {
            b"t" => true,
            b"f" => false,
            _ => return Err(refusal("target-metadata: invalid follow completion flag")),
        },
        applied,
        barrier_epoch: decimal(required(cells[17])?)?,
        barrier_counter: decimal(required(cells[18])?)?,
        anchor: decode_anchor(&cells[19..22], confirmed.intent.binding.stream)?,
        clone: cells[22]
            .map(|bytes| {
                if bytes.len() > 4096 {
                    return Err(refusal("target-metadata: clone control exceeds bound"));
                }
                let value: super::checkpoint::CloneControl = serde_json::from_slice(bytes)
                    .map_err(|_| refusal("target-metadata: invalid clone control"))?;
                value.validate()?;
                Ok(value)
            })
            .transpose()?,
        certified_lsn: cells[23].map(lsn).transpose()?.map(SourceEndLsn),
        follow_binding: cells[24]
            .map(|bytes| {
                if bytes.len() > crate::state::MAX_FOLLOW_CONTROL_BYTES {
                    return Err(refusal("target-metadata: follow binding exceeds bound"));
                }
                let value: crate::state::FollowControl = serde_json::from_slice(bytes)
                    .map_err(|_| refusal("target-metadata: invalid follow binding"))?;
                value.validate()?;
                Ok(value)
            })
            .transpose()?,
    };
    row_values(confirmed, &row)?;
    Ok(row)
}

fn decode_drain(cells: &[Option<&[u8]>]) -> Result<Option<DrainRecord>> {
    match cells {
        [None, None, None] => Ok(None),
        [Some(end), Some(witness), Some(kind)] => {
            let kind = match *kind {
                b"keepalive" => DrainKind::Keepalive,
                b"later_begin" => DrainKind::LaterBegin,
                _ => return Err(refusal("target-metadata: invalid drain kind")),
            };
            Ok(Some(DrainRecord {
                end: SourceEndLsn(lsn(end)?),
                witness: SourceEndLsn(lsn(witness)?),
                kind,
            }))
        }
        _ => Err(refusal("target-metadata: partial drain record")),
    }
}

fn decode_anchor(cells: &[Option<&[u8]>], stream: StreamId) -> Result<Option<SnapshotAnchor>> {
    match cells {
        [None, None, None] => Ok(None),
        [Some(token), Some(point), Some(nonce)] => {
            if token.len() > MAX_SNAPSHOT_TOKEN_BYTES
                || nonce.len() != 32
                || !nonce.iter().all(u8::is_ascii_hexdigit)
            {
                return Err(refusal("target-metadata: invalid snapshot anchor"));
            }
            let snapshot_token = std::str::from_utf8(token)
                .map_err(|_| refusal("target-metadata: invalid snapshot anchor"))?
                .to_owned();
            let nonce = std::str::from_utf8(nonce)
                .map_err(|_| refusal("target-metadata: invalid snapshot anchor"))?;
            let mut plan_nonce = [0; 16];
            for (index, byte) in plan_nonce.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&nonce[index * 2..index * 2 + 2], 16)
                    .map_err(|_| refusal("target-metadata: invalid snapshot anchor"))?;
            }
            let anchor = SnapshotAnchor {
                stream,
                snapshot_token,
                consistent_point: SourceEndLsn(lsn(point)?),
                plan_nonce,
            };
            validate_anchor(&anchor, stream)?;
            Ok(Some(anchor))
        }
        _ => Err(refusal("target-metadata: partial snapshot anchor")),
    }
}

fn validate_anchor(anchor: &SnapshotAnchor, stream: StreamId) -> Result<()> {
    if anchor.stream != stream
        || anchor.snapshot_token.is_empty()
        || anchor.snapshot_token.len() > MAX_SNAPSHOT_TOKEN_BYTES
        || anchor.snapshot_token.contains('\0')
        || anchor.consistent_point.0 == 0
        || anchor.plan_nonce == [0; 16]
    {
        return Err(refusal("target-metadata: invalid snapshot anchor"));
    }
    Ok(())
}

fn decimal(bytes: &[u8]) -> Result<u64> {
    if bytes.is_empty() || bytes.len() > 20 || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(refusal("target-metadata: invalid unsigned integer"));
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| refusal("target-metadata: unsigned integer exceeds its bound"))
}

fn lsn(bytes: &[u8]) -> Result<u64> {
    let parts = bytes.split(|value| *value == b'/').collect::<Vec<_>>();
    if parts.len() != 2
        || parts.iter().any(|part| {
            part.is_empty() || part.len() > 8 || !part.iter().all(u8::is_ascii_hexdigit)
        })
    {
        return Err(refusal("target-metadata: invalid source position"));
    }
    let half = |part: &[u8]| {
        std::str::from_utf8(part)
            .ok()
            .and_then(|text| u32::from_str_radix(text, 16).ok())
            .ok_or_else(|| refusal("target-metadata: invalid source position"))
    };
    Ok((u64::from(half(parts[0])?) << 32) | u64::from(half(parts[1])?))
}

fn row_values(confirmed: &ConfirmedProvenance, row: &ControlRow) -> Result<Vec<Option<String>>> {
    validate_intent(&confirmed.intent)?;
    if confirmed.schema_oid == 0 || confirmed.table_oid == 0 {
        return Err(refusal(
            "target-metadata: invalid confirmed object identity",
        ));
    }
    let binding = &confirmed.intent.binding;
    if let Some(anchor) = &row.anchor {
        validate_anchor(anchor, binding.stream)?;
    }
    if row.applied.is_some_and(|frontier| {
        frontier.tx.stream != binding.stream
            || frontier.order.0 == 0
            || frontier
                .commit
                .is_some_and(|commit| frontier.end.0 < commit.0)
    }) {
        return Err(refusal("target-metadata: invalid source frontier"));
    }
    if let Some(follow) = &row.follow_binding {
        follow.validate()?;
        if (row.applied.is_some() || row.certified_lsn.is_some()) && row.anchor.is_none() {
            return Err(refusal(
                "target-metadata: follow progress requires its immutable copy anchor",
            ));
        }
        if follow.binding.stream() != binding.stream
            || row.anchor.as_ref().is_some_and(|anchor| {
                anchor.snapshot_token != follow.binding.slot.snapshot_token
                    || anchor.consistent_point != follow.binding.slot.consistent_point
                    || anchor.plan_nonce != follow.binding.slot.intent.nonce
            })
            || row.applied.is_some_and(|frontier| {
                frontier
                    .commit
                    .is_none_or(|commit| commit.0 == 0 || commit.0 != frontier.tx.begin_ordinal)
                    || frontier.end.0 == 0
            })
        {
            return Err(refusal("target-metadata: inconsistent follow identity"));
        }
    } else if row.certified_lsn.is_some() {
        return Err(refusal(
            "target-metadata: certified frontier requires follow binding",
        ));
    }
    if row.certified_lsn == Some(SourceEndLsn(0)) {
        return Err(refusal("target-metadata: zero certified frontier"));
    }
    if row.drain.is_some() || row.follow_completed {
        let Some(drain) = row.drain else {
            return Err(refusal(
                "target-metadata: completed follow lacks drain record",
            ));
        };
        let valid_phase = row.clone.as_ref().is_some_and(|clone| {
            clone.phase == crate::state::ClonePhase::Analyzing
                && row.follow_binding.as_ref().is_some_and(|follow| {
                    clone.generation == follow.binding.copy_generation
                        && clone.source_timeline == follow.binding.source_timeline
                })
        });
        if row.follow_binding.is_none()
            || row.anchor.is_none()
            || !valid_phase
            || drain.end.0 == 0
            || drain.witness.0 == 0
            || drain.witness < drain.end
            || drain.kind == DrainKind::LaterBegin && drain.witness <= drain.end
            || row
                .anchor
                .as_ref()
                .is_some_and(|anchor| anchor.consistent_point > drain.end)
            || drain.kind == DrainKind::Keepalive
                && row.certified_lsn.is_none_or(|lsn| lsn < drain.witness)
        {
            return Err(refusal("target-metadata: invalid anchored follow drain"));
        }
    }
    let mut values = vec![
        Some("rpgmigrate".into()),
        Some(binding.metadata_version.to_string()),
        Some(format!("\\x{}", hex(&binding.stream.run.0))),
        Some(format!("\\x{}", hex(&confirmed.intent.nonce))),
        Some(binding.stream.source.system_identifier.to_string()),
        Some(binding.stream.source.database_oid.to_string()),
        Some(binding.stream.generation.to_string()),
        binding
            .target_system_identifier
            .map(|value| value.to_string()),
        Some(binding.target_database_oid.to_string()),
        Some(binding.owner_oid.to_string()),
        Some(confirmed.schema_oid.to_string()),
        Some(confirmed.table_oid.to_string()),
        Some(row.applied.map_or(0, |value| value.order.0).to_string()),
        row.applied.map(|value| value.tx.xid.to_string()),
        row.applied.map(|value| value.tx.begin_ordinal.to_string()),
        row.applied
            .and_then(|value| value.commit)
            .map(|value| format!("{:X}/{:X}", value.0 >> 32, value.0 as u32)),
        row.applied
            .map(|value| format!("{:X}/{:X}", value.end.0 >> 32, value.end.0 as u32)),
        Some(row.barrier_epoch.to_string()),
        Some(row.barrier_counter.to_string()),
        row.anchor
            .as_ref()
            .map(|anchor| anchor.snapshot_token.clone()),
        row.anchor.as_ref().map(|anchor| {
            format!(
                "{:X}/{:X}",
                anchor.consistent_point.0 >> 32,
                anchor.consistent_point.0 as u32
            )
        }),
        row.anchor
            .as_ref()
            .map(|anchor| format!("\\x{}", hex(&anchor.plan_nonce))),
    ];
    values.push(
        row.clone
            .as_ref()
            .map(|value| {
                value.validate()?;
                serde_json::to_string(value)
                    .map_err(|_| refusal("target-metadata: invalid clone control"))
            })
            .transpose()?,
    );
    values.push(
        row.certified_lsn
            .map(|v| format!("{:X}/{:X}", v.0 >> 32, v.0 as u32)),
    );
    values.push(
        row.follow_binding
            .as_ref()
            .map(|value| {
                serde_json::to_string(value)
                    .map_err(|_| refusal("target-metadata: invalid follow binding"))
            })
            .transpose()?,
    );
    values.extend([
        row.drain
            .map(|drain| format!("{:X}/{:X}", drain.end.0 >> 32, drain.end.0 as u32)),
        row.drain
            .map(|drain| format!("{:X}/{:X}", drain.witness.0 >> 32, drain.witness.0 as u32)),
        row.drain.map(|drain| match drain.kind {
            DrainKind::Keepalive => "keepalive".into(),
            DrainKind::LaterBegin => "later_begin".into(),
        }),
        Some(if row.follow_completed { "t" } else { "f" }.into()),
    ]);
    Ok(values)
}

fn update_sql(table: &str) -> String {
    let assignments = COLUMNS[12..]
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{} = ${}::pg_catalog.{}", column.0, index + 13, column.1))
        .collect::<Vec<_>>()
        .join(", ");
    let identity = COLUMNS[..12].iter().enumerate().map(|(index, column)| {
        format!(
            "{} IS NOT DISTINCT FROM ${}::pg_catalog.{}",
            column.0,
            index + 1,
            column.1
        )
    });
    let predecessor = COLUMNS[12..].iter().enumerate().map(|(index, column)| {
        format!(
            "{} IS NOT DISTINCT FROM ${}::pg_catalog.{}",
            column.0,
            index + COLUMNS.len() + 1,
            column.1
        )
    });
    let predicate = identity
        .chain(predecessor)
        .collect::<Vec<_>>()
        .join(" AND ");
    format!("UPDATE ONLY {table} SET {assignments} WHERE {predicate}")
}

fn validate_intent(intent: &ProvisioningIntent) -> Result<()> {
    let binding = &intent.binding;
    if binding.stream.run.0 == [0; 16]
        || intent.nonce == [0; 16]
        || binding.stream.source.system_identifier == 0
        || binding.stream.source.database_oid == 0
        || binding.target_system_identifier == Some(0)
        || binding.target_database_oid == 0
        || binding.owner_oid == 0
        || binding.metadata_version != TARGET_METADATA_VERSION
        || [&binding.schema_name, &binding.table_name]
            .iter()
            .any(|name| name.is_empty() || name.len() > 63 || name.contains('\0'))
        || binding.schema_name.starts_with("pg_")
        || binding.schema_name == "information_schema"
    {
        return Err(refusal("target-metadata: invalid provisioning intent"));
    }
    Ok(())
}

fn empty_row() -> ControlRow {
    ControlRow {
        drain: None,
        follow_completed: false,
        applied: None,
        barrier_epoch: 0,
        barrier_counter: 0,
        anchor: None,
        clone: None,
        certified_lsn: None,
        follow_binding: None,
    }
}

fn required(value: Option<&[u8]>) -> Result<&[u8]> {
    value.ok_or_else(|| refusal("target-metadata: required control field is absent"))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn catalog_sql(version: u64) -> Result<String> {
    if version < 90600 {
        return Err(refusal("target-metadata: unsupported catalog version"));
    }
    let heap = if version >= 120000 {
        "EXISTS (SELECT 1 FROM pg_catalog.pg_am am WHERE am.oid = c.relam AND am.amname = 'heap' AND am.oid < 16384)"
    } else {
        "c.relam = 0 AND NOT c.relhasoids"
    };
    let partition = if version >= 100000 {
        "AND NOT c.relispartition"
    } else {
        ""
    };
    let identity = if version >= 100000 {
        "AND a.attidentity = ''"
    } else {
        ""
    };
    let generated = if version >= 120000 {
        "AND a.attgenerated = ''"
    } else {
        ""
    };
    let key_count = if version >= 110000 {
        "AND i.indnkeyatts = 4"
    } else {
        ""
    };
    let not_null_constraints = if version >= 180000 {
        "OR (con.contype = 'n' AND con.conenforced AND con.convalidated
            AND NOT con.condeferrable AND NOT con.condeferred
            AND con.conislocal AND con.coninhcount = 0 AND con.conindid = 0
            AND con.conbin IS NULL AND pg_catalog.cardinality(con.conkey) = 1
            AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = c.oid AND a.attnum = con.conkey[1] AND a.attnotnull))"
    } else {
        ""
    };
    let enforced_pk = if version >= 180000 {
        "AND p.conenforced AND NOT p.conperiod"
    } else {
        ""
    };
    let attributes = COLUMNS
        .iter()
        .enumerate()
        .map(|(index, column)| {
            format!(
                "({}, '{}', {}, {}, {})",
                index + 1,
                column.0,
                column.2,
                column.3,
                column.4
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let width = COLUMNS.len();
    Ok(format!(
        r#"SELECT
        n.nspowner = $4::pg_catalog.oid AND c.relowner = $4::pg_catalog.oid
        AND n.nspname = $5::pg_catalog.text AND c.relname = $6::pg_catalog.text
        AND EXISTS (SELECT 1 FROM pg_catalog.pg_database d
            WHERE d.datname = pg_catalog.current_database() AND d.oid = $3::pg_catalog.oid)
        AND c.relkind = 'r' AND c.relpersistence = 'p' AND {heap} {partition}
        AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
        AND NOT c.relisshared AND c.reloftype = 0 AND c.relnatts = {width} AND c.relchecks = 0
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_policy policy WHERE policy.polrelid = c.oid)
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid)
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class = c.oid)
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h WHERE h.inhrelid = c.oid OR h.inhparent = c.oid)
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_foreign_table f WHERE f.ftrelid = c.oid)
        AND pg_catalog.has_schema_privilege(n.oid, 'USAGE')
        AND pg_catalog.has_table_privilege(c.oid, 'SELECT')
        AND pg_catalog.has_table_privilege(c.oid, 'INSERT')
        AND pg_catalog.has_table_privilege(c.oid, 'UPDATE')
        AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.aclexplode(COALESCE(n.nspacl, pg_catalog.acldefault('n', n.nspowner))) acl
            WHERE acl.grantor <> n.nspowner OR (acl.grantee <> n.nspowner AND
                (acl.grantee <> writer.oid
                    OR acl.privilege_type <> 'USAGE' OR acl.is_grantable)))
        AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.aclexplode(COALESCE(c.relacl, pg_catalog.acldefault('r', c.relowner))) acl
            WHERE acl.grantor <> c.relowner OR (acl.grantee <> c.relowner AND
                (acl.grantee <> writer.oid
                    OR acl.privilege_type NOT IN ('SELECT', 'INSERT', 'UPDATE') OR acl.is_grantable)))
        AND (SELECT count(*) FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0) = {width}
        AND NOT EXISTS (
            SELECT 1 FROM (VALUES {attributes}) expected(num, name, type_oid, modifier, required)
            LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = expected.num
            WHERE NOT COALESCE(a.attname = expected.name AND a.atttypid = expected.type_oid::pg_catalog.oid
                AND a.atttypmod = expected.modifier AND a.attnotnull = expected.required
                AND NOT a.attisdropped AND NOT a.atthasdef AND a.attislocal AND a.attinhcount = 0
                AND a.attndims = 0 AND a.attacl IS NULL AND {ATTOPTIONS} AND a.attfdwoptions IS NULL
                AND a.attcollation = CASE WHEN a.atttypid = 25
                    THEN 'pg_catalog."C"'::pg_catalog.regcollation::pg_catalog.oid ELSE 0 END
                {identity} {generated}, false))
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attrdef a WHERE a.adrelid = c.oid)
        AND p.contype = 'p' AND p.connamespace = n.oid AND NOT p.condeferrable AND NOT p.condeferred
        AND p.convalidated AND p.conislocal AND p.coninhcount = 0 AND p.contypid = 0
        AND p.confrelid = 0 AND p.conbin IS NULL AND p.conkey = ARRAY[3,5,6,7]::pg_catalog.int2[] {enforced_pk}
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint con
            WHERE con.conrelid = c.oid AND NOT (con.oid = p.oid {not_null_constraints}))
        AND (SELECT count(*) FROM pg_catalog.pg_index ix WHERE ix.indrelid = c.oid) = 1
        AND idx.relkind = 'i' AND idx.relpersistence = 'p' AND idx.relnamespace = n.oid
        AND idx.relowner = c.relowner AND idx.relacl IS NULL
        AND idx.oid NOT IN ($7::oid,$8::oid,$9::oid,$10::oid)
        AND EXISTS (SELECT 1 FROM pg_catalog.pg_am am WHERE am.oid = idx.relam AND am.amname = 'btree' AND am.oid < 16384)
        AND i.indisunique AND i.indisprimary AND NOT i.indisexclusion AND i.indimmediate
        AND i.indisvalid AND i.indisready AND i.indislive AND i.indnatts = 4 {key_count}
        AND i.indexprs IS NULL AND i.indpred IS NULL
        AND i.indkey = '3 5 6 7'::pg_catalog.int2vector
        AND i.indcollation = '0 0 0 0'::pg_catalog.oidvector
        AND i.indoption = '0 0 0 0'::pg_catalog.int2vector
        AND pg_catalog.cardinality(i.indclass::pg_catalog.oid[]) = 4
        AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.unnest(i.indclass::pg_catalog.oid[]) WITH ORDINALITY key(opclass, position)
            LEFT JOIN pg_catalog.pg_opclass op ON op.oid = key.opclass
            WHERE NOT COALESCE(op.oid < 16384 AND op.opcnamespace = 'pg_catalog'::pg_catalog.regnamespace
                AND op.opcmethod = idx.relam AND op.opcdefault AND op.opckeytype = 0
                AND op.opcname = CASE key.position WHEN 1 THEN 'bytea_ops' WHEN 3 THEN 'oid_ops' ELSE 'numeric_ops' END
                AND op.opcintype = CASE key.position WHEN 1 THEN 17 WHEN 3 THEN 26 ELSE 1700 END::pg_catalog.oid, false))
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class other
            WHERE other.relnamespace = n.oid AND other.oid NOT IN (c.oid, idx.oid, $7::oid, $8::oid, $9::oid, $10::oid))
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep
            WHERE dep.refclassid = 'pg_catalog.pg_namespace'::pg_catalog.regclass AND dep.refobjid = n.oid
            AND NOT ((dep.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND dep.objid IN (c.oid, idx.oid, $7::oid, $8::oid, $9::oid, $10::oid))
                OR (dep.classid = 'pg_catalog.pg_type'::pg_catalog.regclass AND (dep.objid IN (c.reltype, rowtype.typarray) OR dep.objid IN (SELECT t.oid FROM pg_catalog.pg_type t WHERE t.typrelid IN ($7::oid,$9::oid) OR t.oid IN (SELECT typarray FROM pg_catalog.pg_type WHERE typrelid IN ($7::oid,$9::oid)))))
                OR (dep.classid = 'pg_catalog.pg_constraint'::pg_catalog.regclass
                    AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint con WHERE con.oid = dep.objid AND con.conrelid IN (c.oid,$7::oid,$9::oid)))))
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep
            WHERE dep.deptype = 'e' AND ((dep.classid = 'pg_catalog.pg_namespace'::pg_catalog.regclass AND dep.objid = n.oid)
                OR (dep.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND dep.objid IN (c.oid, idx.oid, $7::oid, $8::oid, $9::oid, $10::oid))
                OR (dep.classid = 'pg_catalog.pg_type'::pg_catalog.regclass AND (dep.objid IN (c.reltype, rowtype.typarray) OR dep.objid IN (SELECT t.oid FROM pg_catalog.pg_type t WHERE t.typrelid IN ($7::oid,$9::oid) OR t.oid IN (SELECT typarray FROM pg_catalog.pg_type WHERE typrelid IN ($7::oid,$9::oid)))))))
        FROM pg_catalog.pg_namespace n
        JOIN pg_catalog.pg_roles writer ON writer.rolname = CURRENT_USER
        JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
        JOIN pg_catalog.pg_type rowtype ON rowtype.oid = c.reltype
        JOIN pg_catalog.pg_constraint p ON p.conrelid = c.oid AND p.contype = 'p'
        JOIN pg_catalog.pg_index i ON i.indexrelid = p.conindid AND i.indrelid = c.oid
        JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid
        WHERE n.oid = $1::pg_catalog.oid AND c.oid = $2::pg_catalog.oid"#
    ))
}

#[track_caller]
fn refusal(message: &'static str) -> Error {
    Error::new(ErrorKind::Target, message)
}

const MARKER_COLUMNS: [(&str, &str, u32, i32, bool); 16] = [
    ("magic", "text", 25, -1, true),
    ("version", "int4", 23, -1, true),
    ("run_id", "bytea", 17, -1, true),
    ("nonce", "bytea", 17, -1, true),
    ("source_table", "oid", 26, -1, true),
    ("target_table", "oid", 26, -1, true),
    ("copy_generation", "numeric", 1700, NUMERIC_U64, true),
    ("rows", "numeric", 1700, NUMERIC_U64, true),
    ("bytes", "numeric", 1700, NUMERIC_U64, true),
    ("target_namespace", "oid", 26, -1, true),
    ("target_owner", "oid", 26, -1, true),
    ("copy_format", "text", 25, -1, true),
    ("part_number", "int4", 23, -1, true),
    ("part_count", "int4", 23, -1, true),
    ("start_block", "int8", 20, -1, true),
    ("end_block", "int8", 20, -1, false),
];

const LARGE_OBJECT_COLUMNS: [(&str, &str, u32, i32, bool); 13] = [
    ("magic", "text", 25, -1, true),
    ("version", "int4", 23, -1, true),
    ("run_id", "bytea", 17, -1, true),
    ("nonce", "bytea", 17, -1, true),
    ("copy_generation", "numeric", 1700, NUMERIC_U64, true),
    ("object_oid", "oid", 26, -1, true),
    ("target_owner", "oid", 26, -1, true),
    ("target_acl", "text", 25, -1, false),
    ("target_comment", "text", 25, -1, false),
    ("source_metadata", "text", 25, -1, true),
    ("logical_bytes", "numeric", 1700, NUMERIC_U64, true),
    ("content_hash", "bytea", 17, -1, true),
    ("sync_phase", "text", 25, -1, true),
];

fn marker_qualified(connection: &mut PgConnection, intent: &ProvisioningIntent) -> Result<String> {
    Ok(format!(
        "{}.{}",
        connection.quote_identifier(&intent.binding.schema_name)?,
        connection.quote_identifier("copy_done")?
    ))
}

fn marker_create_sql(table: &str) -> String {
    receipt_create_sql(
        table,
        &MARKER_COLUMNS,
        "run_id, nonce, copy_generation, source_table, target_table, part_number",
    )
}

fn large_object_qualified(
    connection: &mut PgConnection,
    intent: &ProvisioningIntent,
) -> Result<String> {
    Ok(format!(
        "{}.{}",
        connection.quote_identifier(&intent.binding.schema_name)?,
        connection.quote_identifier("large_object_done")?
    ))
}

fn large_object_create_sql(table: &str) -> String {
    receipt_create_sql(
        table,
        &LARGE_OBJECT_COLUMNS,
        "run_id, nonce, copy_generation, object_oid",
    )
}

fn receipt_create_sql(table: &str, columns: &[(&str, &str, u32, i32, bool)], key: &str) -> String {
    let fields = columns
        .iter()
        .map(|(name, ty, oid, modifier, required)| {
            format!(
                "{name} pg_catalog.{ty}{}{}{}",
                if *modifier == NUMERIC_U64 {
                    "(20,0)"
                } else {
                    ""
                },
                if *oid == 25 {
                    " COLLATE pg_catalog.\"C\""
                } else {
                    ""
                },
                if *required { " NOT NULL" } else { "" }
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("CREATE TABLE {table} ({fields}, PRIMARY KEY({key})) USING heap")
}

fn validate_receipt_catalogs(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<()> {
    let identities = [
        confirmed.table_oid,
        confirmed.marker_table_oid,
        confirmed.marker_index_oid,
        confirmed.large_object_table_oid,
        confirmed.large_object_index_oid,
    ];
    if identities.contains(&0)
        || identities
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != identities.len()
    {
        return Err(refusal(
            "target-metadata: invalid receipt object identities",
        ));
    }
    validate_receipt_catalog(connection, confirmed, false, io)?;
    validate_receipt_catalog(connection, confirmed, true, io)
}

fn validate_receipt_catalog(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    large_objects: bool,
    io: &IoControl,
) -> Result<()> {
    if confirmed.marker_table_oid == 0
        || confirmed.marker_index_oid == 0
        || confirmed.marker_table_oid == confirmed.table_oid
        || confirmed.marker_index_oid == confirmed.table_oid
        || confirmed.marker_table_oid == confirmed.marker_index_oid
    {
        return Err(refusal("target-metadata: invalid marker object identities"));
    }
    let (name, columns, keys, classes, table_oid, index_oid) = if large_objects {
        (
            "large_object_done",
            LARGE_OBJECT_COLUMNS.as_slice(),
            vec![3, 4, 5, 6],
            vec!["bytea_ops", "bytea_ops", "numeric_ops", "oid_ops"],
            confirmed.large_object_table_oid,
            confirmed.large_object_index_oid,
        )
    } else {
        (
            "copy_done",
            MARKER_COLUMNS.as_slice(),
            vec![3, 4, 7, 5, 6, 13],
            vec![
                "bytea_ops",
                "bytea_ops",
                "numeric_ops",
                "oid_ops",
                "oid_ops",
                "int4_ops",
            ],
            confirmed.marker_table_oid,
            confirmed.marker_index_oid,
        )
    };
    let width = columns.len();
    let key_count = keys.len();
    let key_vector = keys
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    let key_array = keys
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let zero_vector = vec!["0"; key_count].join(" ");
    let opclasses = classes.iter().map(|class| format!("(SELECT oid FROM pg_catalog.pg_opclass WHERE opcnamespace='pg_catalog'::pg_catalog.regnamespace AND opcname='{class}' AND opcmethod=idx.relam AND opcdefault)")).collect::<Vec<_>>().join(",");
    let attributes = columns
        .iter()
        .enumerate()
        .map(|(i, (name, _, ty, modifier, required))| {
            format!("({},'{name}',{ty},{modifier},{required})", i + 1)
        })
        .collect::<Vec<_>>()
        .join(",");
    let version = server_version(connection, io)?;
    let pg18 = if version >= 180000 {
        "OR (con.contype='n' AND con.conenforced AND con.convalidated AND NOT con.condeferrable AND NOT con.condeferred AND con.conislocal AND con.coninhcount=0 AND con.conindid=0 AND con.conbin IS NULL AND cardinality(con.conkey)=1 AND EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum=con.conkey[1] AND a.attnotnull))"
    } else {
        ""
    };
    let sql = format!(
        r#"SELECT c.relnamespace=$3::oid AND c.relowner=$4::oid AND c.relname='{name}'
        AND c.relkind='r' AND c.relpersistence='p' AND c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap')
        AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT c.relispartition AND NOT c.relisshared
        AND c.reloftype=0 AND c.relnatts={width} AND c.relchecks=0
        AND pg_catalog.has_table_privilege(c.oid,'SELECT') AND pg_catalog.has_table_privilege(c.oid,'INSERT') AND pg_catalog.has_table_privilege(c.oid,'DELETE')
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.aclexplode(COALESCE(c.relacl,pg_catalog.acldefault('r',c.relowner))) a WHERE a.grantor<>c.relowner OR (a.grantee<>c.relowner AND (a.grantee<>(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=CURRENT_USER) OR a.privilege_type NOT IN('SELECT','INSERT','DELETE') OR a.is_grantable)))
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT EXISTS(SELECT 1 FROM (VALUES {attributes}) e(num,name,ty,modifier,required) WHERE a.attnum=e.num AND a.attname=e.name AND a.atttypid=e.ty::oid AND a.atttypmod=e.modifier AND a.attnotnull=e.required AND NOT a.attisdropped AND NOT a.atthasdef AND a.attislocal AND a.attinhcount=0 AND a.attndims=0 AND a.attacl IS NULL AND {ATTOPTIONS} AND a.attfdwoptions IS NULL AND a.attidentity='' AND a.attgenerated='' AND a.attcollation=CASE WHEN a.atttypid=25 THEN 'pg_catalog."C"'::pg_catalog.regcollation::oid ELSE 0 END))
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attrdef a WHERE a.adrelid=c.oid)
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid=c.oid)
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_policy p WHERE p.polrelid=c.oid)
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid)
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits h WHERE h.inhrelid=c.oid OR h.inhparent=c.oid)
        AND (SELECT count(*) FROM pg_catalog.pg_index ix WHERE ix.indrelid=c.oid)=1
        AND i.indexrelid=$2::oid AND i.indisunique AND i.indisprimary AND NOT i.indisexclusion AND i.indimmediate AND i.indisvalid AND i.indisready AND i.indislive
        AND i.indnatts={key_count} AND i.indnkeyatts={key_count} AND i.indexprs IS NULL AND i.indpred IS NULL
        AND i.indkey='{key_vector}'::pg_catalog.int2vector AND i.indcollation='{zero_vector}'::pg_catalog.oidvector AND i.indoption='{zero_vector}'::pg_catalog.int2vector
        AND idx.relname='{name}_pkey' AND idx.relkind='i' AND idx.relnamespace=c.relnamespace AND idx.relowner=c.relowner AND idx.relpersistence='p' AND idx.relacl IS NULL
        AND idx.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='btree')
        AND i.indclass=ARRAY[{opclasses}]::pg_catalog.oidvector
        AND p.contype='p' AND p.connamespace=c.relnamespace AND NOT p.condeferrable AND NOT p.condeferred AND p.convalidated AND p.conislocal AND p.coninhcount=0 AND p.conkey=ARRAY[{key_array}]::pg_catalog.int2[] AND p.conbin IS NULL
        AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint con WHERE con.conrelid=c.oid AND NOT(con.oid=p.oid {pg18}))
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_index i ON i.indrelid=c.oid JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid JOIN pg_catalog.pg_constraint p ON p.conrelid=c.oid AND p.conindid=idx.oid WHERE c.oid=$1::oid"#
    );
    require_catalog(&query_once(
        connection,
        &sql,
        &[
            Some(table_oid.to_string()),
            Some(index_oid.to_string()),
            Some(confirmed.schema_oid.to_string()),
            Some(confirmed.intent.binding.owner_oid.to_string()),
        ],
        io,
    )?)
}

pub(super) fn read_only(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<ControlRow> {
    validate_intent(&confirmed.intent)?;
    if locate(connection, &confirmed.intent, io)?.as_ref() != Some(confirmed) {
        return Err(refusal("target-metadata: exact metadata identities differ"));
    }
    let version = server_version(connection, io)?;
    require_catalog(&query_once(
        connection,
        &catalog_sql(version)?,
        &catalog_values(confirmed),
        io,
    )?)?;
    validate_receipt_catalogs(connection, confirmed, io)?;
    let table = qualified(connection, &confirmed.intent)?;
    read_row(
        &connection.query(&read_sql(&table).replace(" FOR UPDATE", ""), io)?,
        confirmed,
    )
}

fn marker_projection(table: &str) -> String {
    format!(
        "SELECT CASE WHEN magic='rpgmigrate' THEN magic ELSE '' END,version,CASE WHEN pg_catalog.octet_length(run_id)=16 THEN pg_catalog.encode(run_id,'hex') ELSE '' END,CASE WHEN pg_catalog.octet_length(nonce)=16 THEN pg_catalog.encode(nonce,'hex') ELSE '' END,source_table,target_table,copy_generation,rows,bytes,target_namespace,target_owner,CASE WHEN copy_format IN ('text','binary') THEN copy_format ELSE '' END,part_number,part_count,start_block,end_block FROM ONLY {table}"
    )
}

fn decode_marker(
    cells: &[Option<&[u8]>],
    confirmed: &ConfirmedProvenance,
) -> Result<super::checkpoint::CopyDone> {
    if cells.len() != MARKER_COLUMNS.len() {
        return Err(refusal("target-metadata: invalid marker row width"));
    }
    if cells[0] != Some(b"rpgmigrate")
        || decimal(required(cells[1])?)? != u64::from(TARGET_METADATA_VERSION)
        || cells[2] != Some(hex(&confirmed.intent.binding.stream.run.0).as_bytes())
        || cells[3] != Some(hex(&confirmed.intent.nonce).as_bytes())
    {
        return Err(refusal("target-metadata: marker provenance differs"));
    }
    Ok(super::checkpoint::CopyDone {
        source_oid: oid(required(cells[4])?)?,
        target_oid: oid(required(cells[5])?)?,
        generation: crate::contracts::CopyGeneration(decimal(required(cells[6])?)?),
        rows: decimal(required(cells[7])?)?,
        bytes: decimal(required(cells[8])?)?,
        target_namespace_oid: oid(required(cells[9])?)?,
        target_owner_oid: oid(required(cells[10])?)?,
        format: match required(cells[11])? {
            b"text" => crate::contracts::CopyFormat::Text,
            b"binary" => crate::contracts::CopyFormat::Binary,
            _ => return Err(refusal("target-metadata: invalid COPY format")),
        },
        part: crate::contracts::CopyPart {
            number: positive_int4(required(cells[12])?)?,
            count: positive_int4(required(cells[13])?)?,
            start_block: block(required(cells[14])?)?,
            end_block: cells[15].map(block).transpose()?,
        },
    })
}

fn marker_result(
    result: &PgResult,
    row: usize,
    confirmed: &ConfirmedProvenance,
) -> Result<super::checkpoint::CopyDone> {
    if result.columns()? != MARKER_COLUMNS.len() {
        return Err(refusal("target-metadata: invalid marker result width"));
    }
    let mut cells = [None; MARKER_COLUMNS.len()];
    for (column, cell) in cells.iter_mut().enumerate() {
        *cell = result.cell(row, column)?;
    }
    decode_marker(&cells, confirmed)
}

pub(super) fn read_markers(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<Vec<super::checkpoint::CopyDone>> {
    let table = marker_qualified(connection, &confirmed.intent)?;
    let result = connection.query(
        &format!(
            "{} ORDER BY source_table,part_number LIMIT 4097",
            marker_projection(&table)
        ),
        io,
    )?;
    if result.columns()? != MARKER_COLUMNS.len() {
        return Err(refusal("target-metadata: invalid marker result width"));
    }
    if result.rows() > 4096 {
        return Err(refusal(
            "target-metadata: marker inventory exceeds part bound",
        ));
    }
    (0..result.rows())
        .map(|row| marker_result(&result, row, confirmed))
        .collect()
}

pub(super) fn read_marker(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    table: &crate::contracts::OwnedCopyTable,
    part: &crate::contracts::CopyPart,
    io: &IoControl,
) -> Result<Option<super::checkpoint::CopyDone>> {
    let marker = marker_qualified(connection, &confirmed.intent)?;
    let sql = format!(
        "{} WHERE run_id=$1::pg_catalog.bytea AND nonce=$2::pg_catalog.bytea AND copy_generation=$3::pg_catalog.numeric AND source_table=$4::pg_catalog.oid AND target_table=$5::pg_catalog.oid AND part_number=$6::pg_catalog.int4 LIMIT 2",
        marker_projection(&marker)
    );
    let result = query_once(
        connection,
        &sql,
        &[
            Some(format!(
                "\\x{}",
                hex(&confirmed.intent.binding.stream.run.0)
            )),
            Some(format!("\\x{}", hex(&confirmed.intent.nonce))),
            Some(table.generation.0.to_string()),
            Some(table.table.source_oid.to_string()),
            Some(table.target_oid.to_string()),
            Some(part.number.to_string()),
        ],
        io,
    )?;
    if result.columns()? != MARKER_COLUMNS.len() {
        return Err(refusal("target-metadata: invalid marker result width"));
    }
    match result.rows() {
        0 => Ok(None),
        1 => Ok(Some(marker_result(&result, 0, confirmed)?)),
        _ => Err(refusal("target-metadata: ambiguous exact COPY part marker")),
    }
}

pub(super) fn insert_marker(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    marker: &super::checkpoint::CopyDone,
    io: &IoControl,
) -> Result<()> {
    let table = marker_qualified(connection, &confirmed.intent)?;
    let values = vec![
        Some("rpgmigrate".into()),
        Some(TARGET_METADATA_VERSION.to_string()),
        Some(format!(
            "\\x{}",
            hex(&confirmed.intent.binding.stream.run.0)
        )),
        Some(format!("\\x{}", hex(&confirmed.intent.nonce))),
        Some(marker.source_oid.to_string()),
        Some(marker.target_oid.to_string()),
        Some(marker.generation.0.to_string()),
        Some(marker.rows.to_string()),
        Some(marker.bytes.to_string()),
        Some(marker.target_namespace_oid.to_string()),
        Some(marker.target_owner_oid.to_string()),
        Some(
            match marker.format {
                crate::contracts::CopyFormat::Text => "text",
                crate::contracts::CopyFormat::Binary => "binary",
            }
            .into(),
        ),
        Some(marker.part.number.to_string()),
        Some(marker.part.count.to_string()),
        Some(marker.part.start_block.to_string()),
        marker.part.end_block.map(|end| end.to_string()),
    ];
    let placeholders = MARKER_COLUMNS
        .iter()
        .enumerate()
        .map(|(i, (_, ty, _, _, _))| format!("${}::pg_catalog.{ty}", i + 1))
        .collect::<Vec<_>>()
        .join(",");
    require_tag(
        &query_once(
            connection,
            &format!("INSERT INTO {table} VALUES({placeholders})"),
            &values,
            io,
        )?,
        "INSERT 0 1",
    )
}

fn large_object_projection(table: &str) -> String {
    format!(
        "SELECT CASE WHEN magic='rpgmigrate' THEN magic ELSE '' END,version,CASE WHEN pg_catalog.octet_length(run_id)=16 THEN pg_catalog.encode(run_id,'hex') ELSE '' END,CASE WHEN pg_catalog.octet_length(nonce)=16 THEN pg_catalog.encode(nonce,'hex') ELSE '' END,copy_generation,object_oid,target_owner,CASE WHEN pg_catalog.octet_length(target_acl)<=65536 THEN target_acl WHEN target_acl IS NULL THEN NULL ELSE '' END,CASE WHEN pg_catalog.octet_length(target_comment)<=65536 THEN target_comment WHEN target_comment IS NULL THEN NULL ELSE '' END,CASE WHEN pg_catalog.octet_length(source_metadata)<=65536 THEN source_metadata ELSE '' END,logical_bytes,CASE WHEN pg_catalog.octet_length(content_hash)=32 THEN pg_catalog.encode(content_hash,'hex') ELSE '' END,CASE WHEN sync_phase IN ('initial','final') THEN sync_phase ELSE '' END,(target_acl IS NULL OR pg_catalog.octet_length(target_acl)<=65536) AND (target_comment IS NULL OR pg_catalog.octet_length(target_comment)<=65536) AND pg_catalog.octet_length(source_metadata)<=65536 FROM ONLY {table}"
    )
}

fn decode_large_object(
    result: &PgResult,
    row: usize,
    confirmed: &ConfirmedProvenance,
) -> Result<super::checkpoint::LargeObjectDone> {
    if result.cell(row, 0)? != Some(b"rpgmigrate")
        || decimal(required(result.cell(row, 1)?)?)? != u64::from(TARGET_METADATA_VERSION)
        || result.cell(row, 2)? != Some(hex(&confirmed.intent.binding.stream.run.0).as_bytes())
        || result.cell(row, 3)? != Some(hex(&confirmed.intent.nonce).as_bytes())
        || result.cell(row, 13)? != Some(b"t")
    {
        return Err(refusal(
            "target-metadata: large-object receipt provenance or metadata bound differs",
        ));
    }
    let text = |column| -> Result<Option<String>> {
        result
            .cell(row, column)?
            .map(|value| {
                std::str::from_utf8(value)
                    .map(str::to_owned)
                    .map_err(|_| refusal("target-metadata: invalid large-object metadata encoding"))
            })
            .transpose()
    };
    let encoded = required(result.cell(row, 11)?)?;
    if encoded.len() != 64 {
        return Err(refusal("target-metadata: invalid large-object digest"));
    }
    let mut hash = [0u8; 32];
    for (byte, pair) in hash.iter_mut().zip(encoded.as_chunks::<2>().0) {
        let digit = |value| match value {
            b'0'..=b'9' => Ok(value - b'0'),
            b'a'..=b'f' => Ok(value - b'a' + 10),
            _ => Err(refusal("target-metadata: invalid large-object digest")),
        };
        *byte = digit(pair[0])? * 16 + digit(pair[1])?;
    }
    let done = super::checkpoint::LargeObjectDone {
        generation: crate::contracts::CopyGeneration(decimal(required(result.cell(row, 4)?)?)?),
        object_oid: oid(required(result.cell(row, 5)?)?)?,
        target_owner: oid(required(result.cell(row, 6)?)?)?,
        target_acl: text(7)?,
        target_comment: text(8)?,
        source_metadata: text(9)?
            .ok_or_else(|| refusal("target-metadata: source large-object metadata is absent"))?,
        logical_bytes: decimal(required(result.cell(row, 10)?)?)?,
        content_hash: hash,
        sync_phase: match required(result.cell(row, 12)?)? {
            b"initial" => super::checkpoint::LargeObjectPhase::Initial,
            b"final" => super::checkpoint::LargeObjectPhase::Final,
            _ => return Err(refusal("target-metadata: invalid large-object phase")),
        },
    };
    done.validate()?;
    Ok(done)
}

pub(super) fn admit_large_object_inventory(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    generation: crate::contracts::CopyGeneration,
    io: &IoControl,
) -> Result<()> {
    let table = large_object_qualified(connection, &confirmed.intent)?;
    let result = query_once(
        connection,
        &format!(
            "SELECT NOT EXISTS(SELECT 1 FROM pg_catalog.pg_largeobject_metadata m WHERE NOT EXISTS(SELECT 1 FROM ONLY {table} d WHERE d.object_oid=m.oid AND d.magic='rpgmigrate' AND d.version=$1::pg_catalog.int4 AND d.run_id=$2::pg_catalog.bytea AND d.nonce=$3::pg_catalog.bytea AND d.copy_generation=$4::pg_catalog.numeric) LIMIT 1)"
        ),
        &[
            Some(TARGET_METADATA_VERSION.to_string()),
            Some(format!(
                "\\x{}",
                hex(&confirmed.intent.binding.stream.run.0)
            )),
            Some(format!("\\x{}", hex(&confirmed.intent.nonce))),
            Some(generation.0.to_string()),
        ],
        io,
    )?;
    if result.rows() != 1 || result.columns()? != 1 || result.cell(0, 0)? != Some(b"t") {
        return Err(refusal(
            "target-large-object: unrecorded target object identity refuses admission",
        ));
    }
    Ok(())
}

pub(super) fn large_object_page(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    after: u32,
    io: &IoControl,
) -> Result<Vec<super::checkpoint::LargeObjectDone>> {
    let table = large_object_qualified(connection, &confirmed.intent)?;
    let sql = format!(
        "{} WHERE ($1::pg_catalog.oid=0 OR object_oid>$1::pg_catalog.oid) ORDER BY object_oid LIMIT 128",
        large_object_projection(&table)
    );
    let result = query_once(connection, &sql, &[Some(after.to_string())], io)?;
    if result.columns()? != LARGE_OBJECT_COLUMNS.len() + 1 {
        return Err(refusal("target-metadata: invalid large-object page width"));
    }
    let mut page = Vec::with_capacity(result.rows());
    let mut previous = after;
    for row in 0..result.rows() {
        let done = decode_large_object(&result, row, confirmed)?;
        if done.object_oid <= previous {
            return Err(refusal(
                "target-metadata: large-object inventory is not strictly ordered",
            ));
        }
        previous = done.object_oid;
        page.push(done);
    }
    Ok(page)
}

pub(super) fn read_large_object(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    object_oid: u32,
    lock: bool,
    io: &IoControl,
) -> Result<Option<super::checkpoint::LargeObjectDone>> {
    let table = large_object_qualified(connection, &confirmed.intent)?;
    let sql = format!(
        "{} WHERE object_oid=$1::pg_catalog.oid LIMIT 2{}",
        large_object_projection(&table),
        if lock { " FOR UPDATE" } else { "" }
    );
    let result = query_once(connection, &sql, &[Some(object_oid.to_string())], io)?;
    if result.columns()? != LARGE_OBJECT_COLUMNS.len() + 1 {
        return Err(refusal(
            "target-metadata: invalid large-object result width",
        ));
    }
    match result.rows() {
        0 => Ok(None),
        1 => Ok(Some(decode_large_object(&result, 0, confirmed)?)),
        _ => Err(refusal("target-metadata: ambiguous large-object receipt")),
    }
}

pub(super) fn delete_large_object(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    done: &super::checkpoint::LargeObjectDone,
    io: &IoControl,
) -> Result<()> {
    let table = large_object_qualified(connection, &confirmed.intent)?;
    let result = query_once(
        connection,
        &format!(
            "DELETE FROM ONLY {table} WHERE run_id=$1::pg_catalog.bytea AND nonce=$2::pg_catalog.bytea AND copy_generation=$3::pg_catalog.numeric AND object_oid=$4::pg_catalog.oid"
        ),
        &[
            Some(format!(
                "\\x{}",
                hex(&confirmed.intent.binding.stream.run.0)
            )),
            Some(format!("\\x{}", hex(&confirmed.intent.nonce))),
            Some(done.generation.0.to_string()),
            Some(done.object_oid.to_string()),
        ],
        io,
    )?;
    require_tag(&result, "DELETE 1")
}

pub(super) fn insert_large_object(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    done: &super::checkpoint::LargeObjectDone,
    io: &IoControl,
) -> Result<()> {
    done.validate()?;
    let table = large_object_qualified(connection, &confirmed.intent)?;
    let values = [
        Some("rpgmigrate".into()),
        Some(TARGET_METADATA_VERSION.to_string()),
        Some(format!(
            "\\x{}",
            hex(&confirmed.intent.binding.stream.run.0)
        )),
        Some(format!("\\x{}", hex(&confirmed.intent.nonce))),
        Some(done.generation.0.to_string()),
        Some(done.object_oid.to_string()),
        Some(done.target_owner.to_string()),
        done.target_acl.clone(),
        done.target_comment.clone(),
        Some(done.source_metadata.clone()),
        Some(done.logical_bytes.to_string()),
        Some(format!("\\x{}", hex(&done.content_hash))),
        Some(done.sync_phase.as_str().into()),
    ];
    let placeholders = LARGE_OBJECT_COLUMNS
        .iter()
        .enumerate()
        .map(|(index, (_, ty, _, _, _))| format!("${}::pg_catalog.{ty}", index + 1))
        .collect::<Vec<_>>()
        .join(",");
    require_tag(
        &query_once(
            connection,
            &format!("INSERT INTO {table} VALUES({placeholders})"),
            &values,
            io,
        )?,
        "INSERT 0 1",
    )
}

fn block(bytes: &[u8]) -> Result<u32> {
    u32::try_from(decimal(bytes)?)
        .map_err(|_| refusal("target-metadata: block number out of range"))
}

fn positive_int4(bytes: &[u8]) -> Result<u32> {
    let value = block(bytes)?;
    if value == 0 || value > i32::MAX as u32 {
        return Err(refusal("target-metadata: part number out of range"));
    }
    Ok(value)
}

pub(super) fn clear_table_markers(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    tables: &[crate::contracts::OwnedCopyTable],
    io: &IoControl,
) -> Result<()> {
    let name = marker_qualified(connection, &confirmed.intent)?;
    for table in tables {
        let result = query_once(
            connection,
            &format!(
                "DELETE FROM ONLY {name} WHERE run_id=$1::pg_catalog.bytea AND nonce=$2::pg_catalog.bytea AND copy_generation=$3::pg_catalog.numeric AND source_table=$4::pg_catalog.oid AND target_table=$5::pg_catalog.oid"
            ),
            &[
                Some(format!(
                    "\\x{}",
                    hex(&confirmed.intent.binding.stream.run.0)
                )),
                Some(format!("\\x{}", hex(&confirmed.intent.nonce))),
                Some(table.generation.0.to_string()),
                Some(table.table.source_oid.to_string()),
                Some(table.target_oid.to_string()),
            ],
            io,
        )?;
        if !result.command_tag()?.starts_with("DELETE ") {
            return Err(refusal("target-metadata: split marker invalidation failed"));
        }
    }
    Ok(())
}

pub(super) fn clear_markers(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<()> {
    let table = marker_qualified(connection, &confirmed.intent)?;
    let result = connection.query(&format!("DELETE FROM ONLY {table}"), io)?;
    if !result.command_tag()?.starts_with("DELETE ") {
        return Err(refusal("target-metadata: marker invalidation failed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{RunId, SourceId, StreamId};
    use crate::state::TargetBinding;

    #[test]
    fn metadata7_part_marker_layout_and_primary_key_are_exact() {
        assert_eq!(MARKER_COLUMNS.len(), 16);
        let sql = marker_create_sql("\"s\".\"copy_done\"");
        assert!(sql.contains("copy_format pg_catalog.text COLLATE pg_catalog.\"C\" NOT NULL"));
        assert!(sql.contains("end_block pg_catalog.int8,"));
        assert!(sql.contains(
            "PRIMARY KEY(run_id, nonce, copy_generation, source_table, target_table, part_number)"
        ));
    }

    #[test]
    fn metadata7_marker_decode_requires_exact_width_format_and_nullable_part_end() {
        let confirmed = provenance();
        let run = hex(&confirmed.intent.binding.stream.run.0);
        let nonce = hex(&confirmed.intent.nonce);
        let mut cells = [
            Some(b"rpgmigrate".as_slice()),
            Some(b"7"),
            Some(run.as_bytes()),
            Some(nonce.as_bytes()),
            Some(b"3"),
            Some(b"4"),
            Some(b"1"),
            Some(b"5"),
            Some(b"6"),
            Some(b"7"),
            Some(b"8"),
            Some(b"binary"),
            Some(b"2"),
            Some(b"2"),
            Some(b"4294967295"),
            None,
        ];
        let marker = decode_marker(&cells, &confirmed).unwrap();
        assert_eq!(marker.format, crate::contracts::CopyFormat::Binary);
        assert_eq!(marker.part.end_block, None);
        assert_eq!(marker.part.start_block, u32::MAX);
        assert!(decode_marker(&cells[..15], &confirmed).is_err());
        for (column, value) in [
            (11, b"csv".as_slice()),
            (12, b"0"),
            (13, b"2147483648"),
            (14, b"4294967296"),
            (15, b"-1"),
        ] {
            let before = cells[column];
            cells[column] = Some(value);
            assert!(decode_marker(&cells, &confirmed).is_err());
            cells[column] = before;
        }
    }

    #[test]
    fn metadata7_large_object_layout_has_required_nullable_metadata() {
        assert_eq!(LARGE_OBJECT_COLUMNS.len(), 13);
        let sql = large_object_create_sql("\"s\".\"large_object_done\"");
        assert!(sql.contains("target_acl pg_catalog.text COLLATE pg_catalog.\"C\","));
        assert!(sql.contains("target_comment pg_catalog.text COLLATE pg_catalog.\"C\","));
        assert!(sql.contains("PRIMARY KEY(run_id, nonce, copy_generation, object_oid)"));
    }

    #[test]
    fn metadata7_accepts_new_empty_row_and_refuses_previous_width() {
        let mut cells = empty_cells()[..25].to_vec();
        cells[1] = Some(b"7");
        cells.extend([None, None, None, Some(b"f".as_slice())]);
        assert!(decode_row(&cells, &provenance()).is_ok());
        assert!(decode_row(&cells[..25], &provenance()).is_err());
    }

    fn provenance() -> ConfirmedProvenance {
        ConfirmedProvenance {
            intent: ProvisioningIntent {
                binding: TargetBinding {
                    stream: StreamId {
                        run: RunId([0x11; 16]),
                        source: SourceId {
                            system_identifier: 7,
                            database_oid: 11,
                        },
                        generation: 3,
                    },
                    target_system_identifier: Some(13),
                    target_database_oid: 17,
                    schema_name: "private_metadata".into(),
                    table_name: "checkpoint".into(),
                    owner_oid: 19,
                    metadata_version: TARGET_METADATA_VERSION,
                },
                nonce: [0x22; 16],
            },
            schema_oid: 23,
            table_oid: 29,
            marker_table_oid: 31,
            marker_index_oid: 37,
            large_object_table_oid: 41,
            large_object_index_oid: 43,
        }
    }

    fn empty_cells() -> [Option<&'static [u8]>; 29] {
        [
            Some(b"rpgmigrate"),
            Some(b"7"),
            Some(b"11111111111111111111111111111111"),
            Some(b"22222222222222222222222222222222"),
            Some(b"7"),
            Some(b"11"),
            Some(b"3"),
            Some(b"13"),
            Some(b"17"),
            Some(b"19"),
            Some(b"23"),
            Some(b"29"),
            Some(b"0"),
            None,
            None,
            None,
            None,
            Some(b"0"),
            Some(b"0"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(b"f"),
        ]
    }

    fn snapshot_cells() -> [Option<&'static [u8]>; 29] {
        let mut cells = [None; 29];
        cells[..19].copy_from_slice(&empty_cells()[..19]);
        cells[28] = Some(b"f");
        cells
    }

    #[test]
    fn metadata7_accepts_empty_and_complete_snapshot_rows() {
        let mut cells = snapshot_cells();
        assert_eq!(decode_row(&cells, &provenance()).unwrap().applied, None);
        cells[19] = Some(b"owned snapshot 'token'");
        cells[20] = Some(b"0/1");
        cells[21] = Some(b"01000000000000000000000000000000");
        assert_eq!(decode_row(&cells, &provenance()).unwrap().applied, None);
    }

    #[test]
    fn metadata7_partial_snapshot_fields_have_a_specific_refusal() {
        let values: [&[u8]; 3] = [
            b"owned snapshot token",
            b"0/1",
            b"01000000000000000000000000000000",
        ];
        for mask in 1..7 {
            let mut cells = snapshot_cells();
            for (index, value) in values.iter().enumerate() {
                if mask & (1 << index) != 0 {
                    cells[19 + index] = Some(value);
                }
            }
            assert_eq!(
                decode_row(&cells, &provenance()).unwrap_err().message,
                "target-metadata: partial snapshot anchor",
                "presence mask {mask}"
            );
        }
    }

    #[test]
    fn metadata7_bounded_projection_preserves_null_and_invalid_presence() {
        let sql = read_sql("\"s\".\"checkpoint\"");
        assert!(sql.contains("CASE WHEN copy_snapshot_token IS NULL THEN NULL WHEN pg_catalog.octet_length(copy_snapshot_token) <= 1024 THEN copy_snapshot_token ELSE '' END"));
        assert!(sql.contains("CASE WHEN copy_plan_nonce IS NULL THEN NULL WHEN pg_catalog.octet_length(copy_plan_nonce) = 16 THEN pg_catalog.encode(copy_plan_nonce, 'hex') ELSE '' END"));
        assert!(sql.ends_with(" LIMIT 2 FOR UPDATE"));
    }

    #[test]
    fn metadata7_fresh_catalog_requires_all_29_columns() {
        assert_eq!(COLUMNS.len(), 29);
        let ddl = create_table_sql("\"s\".\"checkpoint\"", 170000);
        assert!(ddl.contains("copy_snapshot_token pg_catalog.text COLLATE pg_catalog.\"C\""));
        assert!(ddl.contains("copy_consistent_point pg_catalog.pg_lsn"));
        assert!(ddl.contains("copy_plan_nonce pg_catalog.bytea"));
        for version in [90600, 110000, 170000, 180000] {
            let sql = catalog_sql(version).unwrap();
            assert!(sql.contains("c.relnatts = 29"));
            assert!(sql.contains("a.attnum > 0) = 29"));
            assert!(sql.contains("(20, 'copy_snapshot_token', 25, -1, false)"));
            assert!(sql.contains("(21, 'copy_consistent_point', 3220, -1, false)"));
            assert!(sql.contains("(22, 'copy_plan_nonce', 17, -1, false)"));
            assert!(sql.contains("c.relchecks = 0"));
            assert!(!sql.contains("ALTER TABLE"));
        }
    }

    #[test]
    fn metadata7_cas_compares_all_fields_through_parameter_46() {
        let sql = update_sql("\"s\".\"checkpoint\"");
        assert_eq!(sql.matches(" IS NOT DISTINCT FROM ").count(), 29);
        assert!(sql.contains("applied_order IS NOT DISTINCT FROM $30::pg_catalog.numeric"));
        assert!(sql.contains("copy_snapshot_token IS NOT DISTINCT FROM $37::pg_catalog.text"));
        assert!(sql.contains("copy_consistent_point IS NOT DISTINCT FROM $38::pg_catalog.pg_lsn"));
        assert!(sql.contains("copy_plan_nonce IS NOT DISTINCT FROM $39::pg_catalog.bytea"));
    }

    fn anchor() -> SnapshotAnchor {
        SnapshotAnchor {
            stream: provenance().intent.binding.stream,
            snapshot_token: "owned snapshot 'token'".into(),
            consistent_point: SourceEndLsn(u64::MAX),
            plan_nonce: [0x11; 16],
        }
    }

    #[test]
    fn snapshot_anchor_codec_round_trips_all_fields() {
        let mut cells = snapshot_cells();
        cells[19] = Some(b"owned snapshot 'token'");
        cells[20] = Some(b"FFFFFFFF/FFFFFFFF");
        cells[21] = Some(b"aAbBcCdDeEfF00112233445566778899");
        let row = decode_row(&cells, &provenance()).unwrap();
        assert_eq!(
            row.anchor,
            Some(SnapshotAnchor {
                plan_nonce: [
                    0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                    0x77, 0x88, 0x99
                ],
                ..anchor()
            })
        );
        let values = row_values(&provenance(), &row).unwrap();
        assert_eq!(values.len(), 29);
        assert_eq!(values[19].as_deref(), Some("owned snapshot 'token'"));
        assert_eq!(values[20].as_deref(), Some("FFFFFFFF/FFFFFFFF"));
        assert_eq!(
            values[21].as_deref(),
            Some("\\xaabbccddeeff00112233445566778899")
        );
        assert_eq!(
            &row_values(&provenance(), &empty_row()).unwrap()[19..22],
            &[None, None, None]
        );
    }

    #[test]
    fn snapshot_anchor_token_bounds_are_byte_bounds_without_extra_grammar() {
        for token in ["x ' \" ; --\n é".into(), "x".repeat(1024), "é".repeat(512)] {
            let row = ControlRow {
                anchor: Some(SnapshotAnchor {
                    snapshot_token: token.clone(),
                    ..anchor()
                }),
                ..empty_row()
            };
            assert_eq!(
                row_values(&provenance(), &row).unwrap()[19].as_deref(),
                Some(token.as_str())
            );
            let mut cells = snapshot_cells();
            cells[19] = Some(token.as_bytes());
            cells[20] = Some(b"0/1");
            cells[21] = Some(b"11111111111111111111111111111111");
            assert_eq!(
                decode_row(&cells, &provenance())
                    .unwrap()
                    .anchor
                    .unwrap()
                    .snapshot_token,
                token
            );
        }
        for token in [
            String::new(),
            "bad\0token".into(),
            "x".repeat(1025),
            "é".repeat(513),
        ] {
            let row = ControlRow {
                anchor: Some(SnapshotAnchor {
                    snapshot_token: token.clone(),
                    ..anchor()
                }),
                ..empty_row()
            };
            assert!(row_values(&provenance(), &row).is_err());
            let mut cells = snapshot_cells();
            cells[19] = Some(token.as_bytes());
            cells[20] = Some(b"0/1");
            cells[21] = Some(b"11111111111111111111111111111111");
            assert!(decode_row(&cells, &provenance()).is_err());
        }
        let mut cells = snapshot_cells();
        cells[19] = Some(&[0xff]);
        cells[20] = Some(b"0/1");
        cells[21] = Some(b"11111111111111111111111111111111");
        assert!(decode_row(&cells, &provenance()).is_err());
    }

    #[test]
    fn snapshot_anchor_refuses_zero_points_and_bad_nonces() {
        let mut cells = snapshot_cells();
        cells[19] = Some(b"token");
        cells[20] = Some(b"0/0");
        cells[21] = Some(b"11111111111111111111111111111111");
        assert!(decode_row(&cells, &provenance()).is_err());
        cells[20] = Some(b"0/1");
        for nonce in [
            b"".as_slice(),
            b"1111111111111111111111111111111",
            b"111111111111111111111111111111111",
            b"gg111111111111111111111111111111",
            b"00000000000000000000000000000000",
            b"\\x11111111111111111111111111111111",
        ] {
            cells[21] = Some(nonce);
            assert!(decode_row(&cells, &provenance()).is_err(), "{nonce:?}");
        }
        for invalid in [
            SnapshotAnchor {
                consistent_point: SourceEndLsn(0),
                ..anchor()
            },
            SnapshotAnchor {
                plan_nonce: [0; 16],
                ..anchor()
            },
        ] {
            assert!(
                row_values(
                    &provenance(),
                    &ControlRow {
                        anchor: Some(invalid),
                        ..empty_row()
                    }
                )
                .is_err()
            );
        }
    }

    #[test]
    fn snapshot_anchor_refuses_other_streams_during_encoding() {
        for field in 0..4 {
            let mut invalid = anchor();
            match field {
                0 => invalid.stream.run.0[0] ^= 1,
                1 => invalid.stream.source.system_identifier += 1,
                2 => invalid.stream.source.database_oid += 1,
                3 => invalid.stream.generation += 1,
                _ => unreachable!(),
            }
            assert!(
                row_values(
                    &provenance(),
                    &ControlRow {
                        anchor: Some(invalid),
                        ..empty_row()
                    }
                )
                .is_err()
            );
        }
    }

    #[test]
    fn metadata7_refuses_version2_and_legacy_row_width() {
        assert_eq!(TARGET_METADATA_VERSION, 7);
        let mut cells = snapshot_cells();
        cells[1] = Some(b"2");
        assert_eq!(
            decode_row(&cells, &provenance()).unwrap_err().message,
            "target-metadata: creation receipt does not match provenance"
        );
        assert_eq!(
            decode_row(&cells[..19], &provenance()).unwrap_err().message,
            "target-metadata: invalid control row width"
        );
        let mut old = provenance();
        old.intent.binding.metadata_version = 2;
        assert_eq!(
            row_values(&old, &empty_row()).unwrap_err().message,
            "target-metadata: invalid provisioning intent"
        );
    }

    #[test]
    fn snapshot_anchor_cas_encodes_both_old_and_new_values() {
        let confirmed = provenance();
        let installed = ControlRow {
            anchor: Some(anchor()),
            ..empty_row()
        };
        let values = update_values(&confirmed, &empty_row(), &installed).unwrap();
        assert_eq!(values.len(), 46);
        assert_eq!(
            &values[19..22],
            &row_values(&confirmed, &installed).unwrap()[19..22]
        );
        assert_eq!(&values[36..39], &[None, None, None]);
        let cdc = ControlRow {
            applied: Some(Frontier {
                tx: TxId {
                    stream: confirmed.intent.binding.stream,
                    xid: 31,
                    begin_ordinal: 1,
                },
                order: TxSeq(1),
                commit: None,
                end: SourceEndLsn(99),
            }),
            barrier_counter: 1,
            ..installed.clone()
        };
        let values = update_values(&confirmed, &installed, &cdc).unwrap();
        assert_eq!(&values[19..22], &values[36..39]);
        let barrier = ControlRow {
            barrier_counter: 2,
            ..cdc.clone()
        };
        assert!(update_values(&confirmed, &cdc, &barrier).is_ok());
    }

    #[test]
    fn snapshot_anchor_is_immutable_and_cannot_follow_cdc() {
        let confirmed = provenance();
        let installed = ControlRow {
            anchor: Some(anchor()),
            ..empty_row()
        };
        assert!(update_values(&confirmed, &installed, &empty_row()).is_err());
        for field in 0..4 {
            let mut replacement = anchor();
            match field {
                0 => replacement.snapshot_token.push('x'),
                1 => replacement.consistent_point.0 -= 1,
                2 => replacement.plan_nonce[0] ^= 1,
                3 => replacement.stream.generation += 1,
                _ => unreachable!(),
            }
            assert_eq!(
                update_values(
                    &confirmed,
                    &installed,
                    &ControlRow {
                        anchor: Some(replacement),
                        ..empty_row()
                    }
                )
                .unwrap_err()
                .message,
                "target-metadata: snapshot anchor cannot be replaced or removed"
            );
        }
        let frontier = Frontier {
            tx: TxId {
                stream: confirmed.intent.binding.stream,
                xid: 31,
                begin_ordinal: 1,
            },
            order: TxSeq(1),
            commit: None,
            end: SourceEndLsn(99),
        };
        let cdc = ControlRow {
            applied: Some(frontier),
            ..empty_row()
        };
        assert_eq!(
            update_values(&confirmed, &cdc, &installed)
                .unwrap_err()
                .message,
            "target-metadata: snapshot anchor must precede CDC"
        );
        assert_eq!(
            update_values(
                &confirmed,
                &empty_row(),
                &ControlRow {
                    applied: Some(frontier),
                    ..installed
                }
            )
            .unwrap_err()
            .message,
            "target-metadata: snapshot anchor must precede CDC"
        );
    }

    #[test]
    fn decimal_accepts_entire_u64_range_without_signs_or_rounding() {
        assert_eq!(decimal(b"0").unwrap(), 0);
        assert_eq!(decimal(b"18446744073709551615").unwrap(), u64::MAX);
        for value in [
            b"".as_slice(),
            b"-1",
            b"+1",
            b"1.0",
            b"1e2",
            b" 1",
            b"18446744073709551616",
        ] {
            assert!(decimal(value).is_err());
        }
    }

    #[test]
    fn source_positions_keep_both_unsigned_halves() {
        assert_eq!(lsn(b"0/0").unwrap(), 0);
        assert_eq!(lsn(b"FFFFFFFF/FFFFFFFF").unwrap(), u64::MAX);
        assert_eq!(lsn(b"1/10").unwrap(), 0x1_0000_0010);
        for value in [
            b"".as_slice(),
            b"1",
            b"/0",
            b"0/",
            b"100000000/0",
            b"0/100000000",
            b"0/+1",
            b"0/1/2",
            b"-1/0",
        ] {
            assert!(lsn(value).is_err());
        }
    }

    #[test]
    fn empty_control_row_does_not_invent_a_frontier() {
        assert_eq!(
            decode_row(&empty_cells(), &provenance()).unwrap(),
            ControlRow {
                drain: None,
                follow_completed: false,
                applied: None,
                barrier_epoch: 0,
                barrier_counter: 0,
                anchor: None,
                clone: None,
                certified_lsn: None,
                follow_binding: None,
            }
        );
        let mut cells = empty_cells();
        let mut confirmed = provenance();
        confirmed.intent.binding.target_system_identifier = None;
        cells[7] = None;
        assert!(decode_row(&cells, &confirmed).is_ok());
    }

    #[test]
    fn nonempty_control_row_keeps_full_identity_and_u64_bounds() {
        let mut cells = empty_cells();
        cells[12] = Some(b"18446744073709551615");
        cells[13] = Some(b"4294967295");
        cells[14] = Some(b"18446744073709551615");
        cells[15] = Some(b"FFFFFFFF/FFFFFFFF");
        cells[16] = Some(b"FFFFFFFF/FFFFFFFF");
        cells[17] = Some(b"18446744073709551615");
        cells[18] = Some(b"18446744073709551615");
        let confirmed = provenance();
        assert_eq!(
            decode_row(&cells, &confirmed).unwrap(),
            ControlRow {
                drain: None,
                follow_completed: false,
                applied: Some(Frontier {
                    tx: TxId {
                        stream: confirmed.intent.binding.stream,
                        xid: u32::MAX,
                        begin_ordinal: u64::MAX
                    },
                    order: TxSeq(u64::MAX),
                    commit: Some(CommitLsn(u64::MAX)),
                    end: SourceEndLsn(u64::MAX),
                }),
                barrier_epoch: u64::MAX,
                barrier_counter: u64::MAX,
                anchor: None,
                clone: None,
                certified_lsn: None,
                follow_binding: None,
            }
        );
        cells[15] = None;
        assert!(
            decode_row(&cells, &confirmed).is_ok(),
            "a complete source transaction can lack a physical commit LSN"
        );
        let unknown = decode_row(&cells, &confirmed).unwrap().applied.unwrap();
        assert_eq!(unknown.commit, None);
        assert_eq!(unknown.end, SourceEndLsn(u64::MAX));
        assert_eq!(unknown.order, TxSeq(u64::MAX));
        assert_eq!(unknown.tx.begin_ordinal, u64::MAX);
        cells[15] = Some(b"0/0");
        assert_eq!(
            decode_row(&cells, &confirmed)
                .unwrap()
                .applied
                .unwrap()
                .commit,
            Some(CommitLsn(0))
        );
        for column in [13, 14, 16] {
            let mut partial = cells;
            partial[15] = None;
            partial[column] = None;
            assert!(decode_row(&partial, &confirmed).is_err());
        }
        cells[15] = Some(b"invalid");
        assert!(decode_row(&cells, &confirmed).is_err());
    }

    #[test]
    fn partial_transaction_identity_and_invalid_receipts_fail_closed() {
        let mut old = provenance();
        old.intent.binding.metadata_version = 1;
        assert!(row_values(&old, &empty_row()).is_err());
        let mut old_cells = empty_cells();
        old_cells[1] = Some(b"1");
        assert!(decode_row(&old_cells, &provenance()).is_err());
        for column in 0..12 {
            let mut cells = empty_cells();
            cells[column] = Some(b"9");
            assert!(
                decode_row(&cells, &provenance()).is_err(),
                "column {column}"
            );
        }
        for column in 13..17 {
            let mut cells = empty_cells();
            cells[column] = Some(b"1");
            assert!(decode_row(&cells, &provenance()).is_err());
            cells[12] = Some(b"1");
            assert!(decode_row(&cells, &provenance()).is_err());
        }
        assert!(decode_row(&empty_cells()[..18], &provenance()).is_err());
        let mut cells = empty_cells();
        cells[12] = Some(b"1");
        cells[13] = Some(b"1");
        cells[14] = Some(b"1");
        cells[15] = Some(b"1/0");
        cells[16] = Some(b"0/FFFFFFFF");
        assert!(decode_row(&cells, &provenance()).is_err());
        cells[16] = cells[15];
        cells[13] = Some(b"4294967296");
        assert!(decode_row(&cells, &provenance()).is_err());
    }

    #[test]
    fn row_parameters_preserve_nulls_unsigned_limits_and_stream_identity() {
        let confirmed = provenance();
        let mut row = ControlRow {
            drain: None,
            follow_completed: false,
            applied: None,
            barrier_epoch: u64::MAX,
            barrier_counter: 2,
            anchor: None,
            clone: None,
            certified_lsn: None,
            follow_binding: None,
        };
        let values = row_values(&confirmed, &row).unwrap();
        assert_eq!(values.len(), 29);
        assert_eq!(
            values[2].as_deref(),
            Some("\\x11111111111111111111111111111111")
        );
        assert_eq!(&values[12..17], &[Some("0".into()), None, None, None, None]);
        assert_eq!(values[17].as_deref(), Some("18446744073709551615"));
        row.applied = Some(Frontier {
            tx: TxId {
                stream: confirmed.intent.binding.stream,
                xid: u32::MAX,
                begin_ordinal: 0,
            },
            order: TxSeq(1),
            commit: Some(CommitLsn(0x1_0000_0010)),
            end: SourceEndLsn(u64::MAX),
        });
        let values = row_values(&confirmed, &row).unwrap();
        assert_eq!(values[13].as_deref(), Some("4294967295"));
        assert_eq!(values[15].as_deref(), Some("1/10"));
        assert_eq!(values[16].as_deref(), Some("FFFFFFFF/FFFFFFFF"));
        row.applied.as_mut().unwrap().commit = None;
        let values = row_values(&confirmed, &row).unwrap();
        assert_eq!(
            &values[12..17],
            &[
                Some("1".into()),
                Some("4294967295".into()),
                Some("0".into()),
                None,
                Some("FFFFFFFF/FFFFFFFF".into())
            ]
        );
        row.applied.as_mut().unwrap().commit = Some(CommitLsn(0));
        assert_eq!(
            row_values(&confirmed, &row).unwrap()[15].as_deref(),
            Some("0/0")
        );
        row.applied.as_mut().unwrap().tx.stream.generation += 1;
        assert!(row_values(&confirmed, &row).is_err());
    }

    #[test]
    fn follow_progress_requires_anchor_and_canonical_source_commit() {
        let mut confirmed = provenance();
        confirmed.intent.binding.stream.generation = 1;
        let stream = confirmed.intent.binding.stream;
        let slot = crate::state::SourceSlotReceipt {
            intent: crate::state::SourceSlotIntent {
                binding: crate::state::SourceSlotBinding::new(
                    stream,
                    "rpgmigrate_metadata".into(),
                    crate::cdc::contracts::Plugin::Pgoutput,
                    1,
                )
                .unwrap(),
                nonce: [3; 16],
            },
            consistent_point: SourceEndLsn(10),
            snapshot_token: "snapshot".into(),
        };
        let mut row = empty_row();
        row.follow_binding = Some(crate::state::FollowControl {
            binding: crate::contracts::FollowBinding {
                slot: slot.clone(),
                publication: crate::contracts::PublicationReceipt {
                    name: "rpgmigrate_metadata".into(),
                    oid: 31,
                    owner_oid: 37,
                },
                source_timeline: 1,
                copy_generation: crate::contracts::CopyGeneration(1),
                source_types: Vec::new(),
            },
            operator: None,
            origin_name: None,
        });
        row.applied = Some(Frontier {
            tx: TxId {
                stream,
                xid: 9,
                begin_ordinal: 100,
            },
            order: TxSeq(1),
            commit: Some(CommitLsn(100)),
            end: SourceEndLsn(120),
        });
        assert!(row_values(&confirmed, &row).is_err());
        row.anchor = Some(SnapshotAnchor {
            stream,
            snapshot_token: slot.snapshot_token,
            consistent_point: slot.consistent_point,
            plan_nonce: slot.intent.nonce,
        });
        assert!(row_values(&confirmed, &row).is_ok());
        row.applied.as_mut().unwrap().commit = None;
        assert!(row_values(&confirmed, &row).is_err());
        row.applied.as_mut().unwrap().commit = Some(CommitLsn(99));
        assert!(row_values(&confirmed, &row).is_err());
        row.applied = None;
        row.certified_lsn = Some(SourceEndLsn(200));
        row.anchor = None;
        assert!(row_values(&confirmed, &row).is_err());
    }

    fn drain_fixture() -> (ConfirmedProvenance, ControlRow) {
        let mut confirmed = provenance();
        confirmed.intent.binding.stream.generation = 1;
        let stream = confirmed.intent.binding.stream;
        let slot = crate::state::SourceSlotReceipt {
            intent: crate::state::SourceSlotIntent {
                binding: crate::state::SourceSlotBinding::new(
                    stream,
                    "rpgmigrate_drain".into(),
                    crate::cdc::contracts::Plugin::Pgoutput,
                    1,
                )
                .unwrap(),
                nonce: [3; 16],
            },
            consistent_point: SourceEndLsn(8),
            snapshot_token: "snapshot".into(),
        };
        let mut row = empty_row();
        row.anchor = Some(SnapshotAnchor {
            stream,
            snapshot_token: slot.snapshot_token.clone(),
            consistent_point: slot.consistent_point,
            plan_nonce: slot.intent.nonce,
        });
        row.follow_binding = Some(crate::state::FollowControl {
            binding: crate::contracts::FollowBinding {
                slot,
                publication: crate::contracts::PublicationReceipt {
                    name: "rpgmigrate_drain".into(),
                    oid: 31,
                    owner_oid: 37,
                },
                source_timeline: 1,
                copy_generation: crate::contracts::CopyGeneration(1),
                source_types: Vec::new(),
            },
            operator: None,
            origin_name: None,
        });
        row.clone = Some(crate::apply::checkpoint::CloneControl {
            generation: crate::contracts::CopyGeneration(1),
            phase: crate::state::ClonePhase::Analyzing,
            snapshot_token: "snapshot".into(),
            source_timeline: 1,
            table_count: 0,
            prepared_generation: None,
            large_objects: crate::contracts::LargeObjectControl {
                generation: crate::contracts::CopyGeneration(1),
                initial: None,
                final_sync: None,
            },
        });
        row.certified_lsn = Some(SourceEndLsn(0x30));
        row.drain = Some(DrainRecord {
            end: SourceEndLsn(0x20),
            witness: SourceEndLsn(0x30),
            kind: DrainKind::Keepalive,
        });
        (confirmed, row)
    }

    #[test]
    fn drain_codec_rejects_partial_records_invalid_kinds_and_legacy_rows() {
        let (confirmed, row) = drain_fixture();
        let mut values = row_values(&confirmed, &row).unwrap();
        values[2] = Some(hex(&confirmed.intent.binding.stream.run.0));
        values[3] = Some(hex(&confirmed.intent.nonce));
        values[21] = Some(hex(&row.anchor.as_ref().unwrap().plan_nonce));
        let cells = values
            .iter()
            .map(|v| v.as_ref().map(|s| s.as_bytes()))
            .collect::<Vec<_>>();
        assert_eq!(decode_row(&cells, &confirmed).unwrap(), row);
        for mask in 1..7 {
            let mut partial = cells.clone();
            for field in 0..3 {
                if mask & (1 << field) == 0 {
                    partial[25 + field] = None;
                }
            }
            assert!(decode_row(&partial, &confirmed).is_err(), "mask {mask}");
        }
        for (index, invalid) in [
            (25, b"0/0".as_slice()),
            (26, b"0/1"),
            (27, b"foreign"),
            (28, b"true"),
            (1, b"5"),
        ] {
            let mut invalid_cells = cells.clone();
            invalid_cells[index] = Some(invalid);
            assert!(
                decode_row(&invalid_cells, &confirmed).is_err(),
                "field {index}"
            );
        }
        assert!(decode_row(&cells[..25], &confirmed).is_err());
        let mut old = confirmed;
        old.intent.binding.metadata_version = 5;
        assert!(row_values(&old, &row).is_err());
    }

    #[test]
    fn completed_row_allows_only_same_value_reproof_and_retains_crossing_commit() {
        let (confirmed, mut row) = drain_fixture();
        let stream = confirmed.intent.binding.stream;
        row.applied = Some(Frontier {
            tx: TxId {
                stream,
                xid: 9,
                begin_ordinal: 0x18,
            },
            order: TxSeq(1),
            commit: Some(CommitLsn(0x18)),
            end: SourceEndLsn(0x28),
        });
        row.follow_completed = true;
        assert!(update_values(&confirmed, &row, &row).is_ok());
        for field in 0..6 {
            let mut changed = row.clone();
            match field {
                0 => changed.follow_completed = false,
                1 => changed.barrier_counter += 1,
                2 => changed.barrier_epoch += 1,
                3 => changed.certified_lsn = Some(SourceEndLsn(0x40)),
                4 => changed.drain.as_mut().unwrap().witness = SourceEndLsn(0x40),
                _ => changed.applied.as_mut().unwrap().end = SourceEndLsn(0x29),
            }
            assert!(
                update_values(&confirmed, &row, &changed).is_err(),
                "field {field}"
            );
        }
        row.drain = None;
        assert!(row_values(&confirmed, &row).is_err());
    }

    #[test]
    fn compare_and_swap_sql_matches_entire_receipt_and_nullable_predecessor() {
        let sql = update_sql("\"s\".\"checkpoint\"");
        assert!(sql.starts_with("UPDATE ONLY \"s\".\"checkpoint\" SET "));
        assert!(sql.contains("nonce IS NOT DISTINCT FROM $4::pg_catalog.bytea"));
        assert!(sql.contains("table_oid IS NOT DISTINCT FROM $12::pg_catalog.oid"));
        assert!(sql.contains("applied_order IS NOT DISTINCT FROM $30::pg_catalog.numeric"));
        assert!(sql.contains("last_xid IS NOT DISTINCT FROM $31::pg_catalog.oid"));
        assert!(sql.contains("source_commit_lsn IS NOT DISTINCT FROM $33::pg_catalog.pg_lsn"));
        assert!(sql.contains("source_end_lsn IS NOT DISTINCT FROM $34::pg_catalog.pg_lsn"));
        assert!(sql.contains("barrier_counter IS NOT DISTINCT FROM $36::pg_catalog.numeric"));
        assert_eq!(sql.matches(" IS NOT DISTINCT FROM ").count(), 29);
    }

    #[test]
    fn catalog_sql_uses_versioned_fields_and_separate_privilege_checks() {
        let old = catalog_sql(90600).unwrap();
        assert!(!old.contains("attidentity"));
        assert!(!old.contains("attgenerated"));
        assert!(!old.contains("indnkeyatts"));
        assert!(!old.contains("relispartition"));
        assert!(old.contains("NOT c.relhasoids"));
        for privilege in ["SELECT", "INSERT", "UPDATE"] {
            assert!(old.contains(&format!(
                "pg_catalog.has_table_privilege(c.oid, '{privilege}')"
            )));
        }
        let modern = catalog_sql(170000).unwrap();
        assert!(modern.contains("a.attidentity = ''"));
        assert!(modern.contains("a.attgenerated = ''"));
        assert!(modern.contains("i.indnkeyatts = 4"));
        assert!(modern.contains("NOT c.relispartition"));
        assert!(!modern.contains("relhasoids"));
        assert!(catalog_sql(90500).is_err());
    }

    #[test]
    fn namespace_absence_requires_positive_matching_database_evidence() {
        let confirmed = provenance();
        assert_eq!(
            decode_location(&[Some(b"17"), None, None, None], &confirmed.intent).unwrap(),
            None
        );
        assert_eq!(
            decode_location(
                &[Some(b"17"), Some(b"23"), Some(b"29"), Some(b"r")],
                &confirmed.intent
            )
            .unwrap(),
            Some(ConfirmedProvenance {
                marker_table_oid: 0,
                marker_index_oid: 0,
                large_object_table_oid: 0,
                large_object_index_oid: 0,
                ..confirmed.clone()
            })
        );
        for cells in [
            [None, None, None, None],
            [Some(b"18".as_slice()), None, None, None],
            [Some(b"17".as_slice()), Some(b"23".as_slice()), None, None],
            [
                Some(b"17".as_slice()),
                Some(b"23".as_slice()),
                Some(b"29".as_slice()),
                Some(b"v".as_slice()),
            ],
        ] {
            assert!(decode_location(&cells, &confirmed.intent).is_err());
        }
    }

    #[test]
    fn owned_class_oid_pair_requires_one_nonzero_distinct_primary_index() {
        assert_eq!(owned_relation_pair(23, 1, Some(b"29")).unwrap(), [23, 29]);
        assert_eq!(
            owned_relation_pair(23, 1, Some(b"4294967295")).unwrap(),
            [23, u32::MAX]
        );
        for (table, rows, index) in [
            (0, 1, Some(b"29".as_slice())),
            (23, 0, None),
            (23, 2, Some(b"29".as_slice())),
            (23, 1, None),
            (23, 1, Some(b"0".as_slice())),
            (23, 1, Some(b"23".as_slice())),
            (23, 1, Some(b"4294967296".as_slice())),
            (23, 1, Some(b"-1".as_slice())),
            (23, 1, Some(b"29.0".as_slice())),
            (23, 1, Some(b"bad".as_slice())),
        ] {
            assert!(
                owned_relation_pair(table, rows, index).is_err(),
                "{table}, {rows}, {index:?}"
            );
        }
    }
}
