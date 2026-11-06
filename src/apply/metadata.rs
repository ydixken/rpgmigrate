//! The authority owns the transaction, writer fence, and durability proof.
//! Namespace DDL must remain paused while these owned relations are in use.

use super::{ControlRow, Frontier};
use crate::contracts::{Error, ErrorKind, IoControl, Result};
use crate::identity::{CommitLsn, SourceEndLsn, TxId, TxSeq};
use crate::pg::{PgConnection, PgParam, PgResult, TransactionStatus};
use crate::state::{ConfirmedProvenance, ProvisioningIntent, TARGET_METADATA_VERSION};

const CATALOG: &str = "rpgmigrate_metadata_catalog";
const READ: &str = "rpgmigrate_metadata_read";
const UPDATE: &str = "rpgmigrate_metadata_update";
const NUMERIC_U64: i32 = (20 << 16) + 4;
const COLUMNS: [(&str, &str, u32, i32, bool); 19] = [
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
    let confirmed = locate(connection, intent, io)?
        .ok_or_else(|| refusal("target-metadata: created namespace is absent"))?;
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
    read_row(&connection.execute_prepared(READ, &[], io)?, confirmed)
}

pub(super) fn update(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    old: &ControlRow,
    next: &ControlRow,
    io: &IoControl,
) -> Result<()> {
    in_transaction(connection)?;
    let mut values = row_values(confirmed, next)?;
    values.extend(row_values(confirmed, old)?.into_iter().skip(12));
    require_tag(&execute(connection, UPDATE, &values, io)?, "UPDATE 1")
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
    decode_location(
        &[
            result.cell(0, 0)?,
            result.cell(0, 1)?,
            result.cell(0, 2)?,
            result.cell(0, 3)?,
        ],
        intent,
    )
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
    ]
}

fn read_row(result: &PgResult, confirmed: &ConfirmedProvenance) -> Result<ControlRow> {
    if result.rows() != 1 {
        return Err(refusal("target-metadata: expected exactly one control row"));
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
            let collation = if column.0 == "magic" {
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
    Ok(ControlRow {
        applied,
        barrier_epoch: decimal(required(cells[17])?)?,
        barrier_counter: decimal(required(cells[18])?)?,
    })
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
    if row.applied.is_some_and(|frontier| {
        frontier.tx.stream != binding.stream
            || frontier.order.0 == 0
            || frontier
                .commit
                .is_some_and(|commit| frontier.end.0 < commit.0)
    }) {
        return Err(refusal("target-metadata: invalid source frontier"));
    }
    Ok(vec![
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
    ])
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
            index + 20,
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
        applied: None,
        barrier_epoch: 0,
        barrier_counter: 0,
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
    Ok(format!(
        r#"SELECT
        n.nspowner = $4::pg_catalog.oid AND c.relowner = $4::pg_catalog.oid
        AND n.nspname = $5::pg_catalog.text AND c.relname = $6::pg_catalog.text
        AND EXISTS (SELECT 1 FROM pg_catalog.pg_database d
            WHERE d.datname = pg_catalog.current_database() AND d.oid = $3::pg_catalog.oid)
        AND c.relkind = 'r' AND c.relpersistence = 'p' AND {heap} {partition}
        AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
        AND NOT c.relisshared AND c.reloftype = 0 AND c.relnatts = 19 AND c.relchecks = 0
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
        AND (SELECT count(*) FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0) = 19
        AND NOT EXISTS (
            SELECT 1 FROM (VALUES {attributes}) expected(num, name, type_oid, modifier, required)
            LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = expected.num
            WHERE NOT COALESCE(a.attname = expected.name AND a.atttypid = expected.type_oid::pg_catalog.oid
                AND a.atttypmod = expected.modifier AND a.attnotnull = expected.required
                AND NOT a.attisdropped AND NOT a.atthasdef AND a.attislocal AND a.attinhcount = 0
                AND a.attndims = 0 AND a.attacl IS NULL AND a.attoptions IS NULL AND a.attfdwoptions IS NULL
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
            WHERE other.relnamespace = n.oid AND other.oid NOT IN (c.oid, idx.oid))
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep
            WHERE dep.refclassid = 'pg_catalog.pg_namespace'::pg_catalog.regclass AND dep.refobjid = n.oid
            AND NOT ((dep.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND dep.objid IN (c.oid, idx.oid))
                OR (dep.classid = 'pg_catalog.pg_type'::pg_catalog.regclass AND dep.objid IN (c.reltype, rowtype.typarray))
                OR (dep.classid = 'pg_catalog.pg_constraint'::pg_catalog.regclass
                    AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint con WHERE con.oid = dep.objid AND con.conrelid = c.oid))))
        AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep
            WHERE dep.deptype = 'e' AND ((dep.classid = 'pg_catalog.pg_namespace'::pg_catalog.regclass AND dep.objid = n.oid)
                OR (dep.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND dep.objid IN (c.oid, idx.oid))
                OR (dep.classid = 'pg_catalog.pg_type'::pg_catalog.regclass AND dep.objid IN (c.reltype, rowtype.typarray))))
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

fn refusal(message: &'static str) -> Error {
    Error {
        kind: ErrorKind::Target,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{RunId, SourceId, StreamId};
    use crate::state::TargetBinding;

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
        }
    }

    fn empty_cells() -> [Option<&'static [u8]>; 19] {
        [
            Some(b"rpgmigrate"),
            Some(b"2"),
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
        ]
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
                applied: None,
                barrier_epoch: 0,
                barrier_counter: 0,
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
            applied: None,
            barrier_epoch: u64::MAX,
            barrier_counter: 2,
        };
        let values = row_values(&confirmed, &row).unwrap();
        assert_eq!(values.len(), 19);
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
    fn compare_and_swap_sql_matches_entire_receipt_and_nullable_predecessor() {
        let sql = update_sql("\"s\".\"checkpoint\"");
        assert!(sql.starts_with("UPDATE ONLY \"s\".\"checkpoint\" SET "));
        assert!(sql.contains("nonce IS NOT DISTINCT FROM $4::pg_catalog.bytea"));
        assert!(sql.contains("table_oid IS NOT DISTINCT FROM $12::pg_catalog.oid"));
        assert!(sql.contains("applied_order IS NOT DISTINCT FROM $20::pg_catalog.numeric"));
        assert!(sql.contains("last_xid IS NOT DISTINCT FROM $21::pg_catalog.oid"));
        assert!(sql.contains("source_commit_lsn IS NOT DISTINCT FROM $23::pg_catalog.pg_lsn"));
        assert!(sql.contains("source_end_lsn IS NOT DISTINCT FROM $24::pg_catalog.pg_lsn"));
        assert!(sql.contains("barrier_counter IS NOT DISTINCT FROM $26::pg_catalog.numeric"));
        assert_eq!(sql.matches(" IS NOT DISTINCT FROM ").count(), 19);
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
            Some(confirmed.clone())
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
}
