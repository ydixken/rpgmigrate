use std::collections::{BTreeMap, BTreeSet};

use crate::contracts::{
    CopyFormat, CopyPlan, CopyTable, Error, ErrorKind, IoControl, QualifiedTable, Result,
};
use crate::pg::{PgConnection, PgResult};

const CATALOG_PAGE: usize = 128;
const CATALOG_BYTES: usize = 8 * 1024 * 1024;
// PostgreSQL reserves lower OIDs for core catalog objects.
const FIRST_NORMAL_OBJECT_ID: u32 = 16_384;

#[derive(Debug, Eq, PartialEq)]
struct Relation {
    oid: u32,
    schema: String,
    table: String,
    supported: bool,
}

#[derive(Debug, Eq, PartialEq)]
struct Column {
    name: String,
    type_name: String,
    modifier: String,
    not_null: bool,
    collation_schema: String,
    collation: String,
}

// Success retains both transactions and their locks for run_copy on these same connections.
pub(crate) fn plan_copy(
    source: &mut PgConnection,
    target: &mut PgConnection,
    io: &IoControl,
    buffer_bytes: usize,
) -> Result<CopyPlan> {
    let outcome = (|| {
        if buffer_bytes == 0 {
            return Err(refusal("COPY forwarding budget must be positive"));
        }
        configure_session(source, io)?;
        configure_session(target, io)?;
        configure_durability(target, io)?;
        require_setting(target, "session_replication_role", "origin", io)?;

        let initial_source = inventory(source, io, false)?;
        let initial_target = inventory(target, io, true)?;
        for (name, relation) in &initial_source {
            require_supported(relation)?;
            require_supported(initial_target.get(name).ok_or_else(|| {
                refusal("COPY requires every source table to exist on the target")
            })?)?;
        }

        command(
            source,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            io,
        )?;
        command(
            target,
            "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE",
            "BEGIN",
            io,
        )?;
        // Lock before the first source snapshot so pre-lock DDL cannot leave a stale catalog.
        for relation in initial_source.values() {
            let name = qualified(source, &relation.schema, &relation.table)?;
            command(
                source,
                &format!("LOCK TABLE ONLY {name} IN ACCESS SHARE MODE"),
                "LOCK TABLE",
                io,
            )?;
        }
        for name in initial_source.keys() {
            let quoted = qualified(target, &name.0, &name.1)?;
            command(
                target,
                &format!("LOCK TABLE ONLY {quoted} IN SHARE ROW EXCLUSIVE MODE"),
                "LOCK TABLE",
                io,
            )?;
        }

        let locked_source = inventory(source, io, false)?;
        let locked_target = inventory(target, io, true)?;
        if initial_source != locked_source {
            return Err(refusal(
                "source catalog changed before COPY locks were acquired",
            ));
        }
        let target_workset = locked_source
            .keys()
            .map(|name| {
                locked_target
                    .get(name)
                    .map(|relation| relation.oid)
                    .ok_or_else(|| {
                        refusal("target catalog changed before COPY locks were acquired")
                    })
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let mut tables = Vec::new();
        let mut remaining = CATALOG_BYTES;
        for (name, relation) in &locked_source {
            let destination = locked_target
                .get(name)
                .ok_or_else(|| refusal("target catalog changed before COPY locks were acquired"))?;
            if initial_target.get(name) != Some(destination) {
                return Err(refusal(
                    "target catalog changed before COPY locks were acquired",
                ));
            }
            require_supported(relation)?;
            require_supported(destination)?;
            require_target_execution(target, destination.oid, &target_workset, io)?;
            let columns = columns(source, relation.oid, io)?;
            if columns != self::columns(target, destination.oid, io)? {
                return Err(refusal(
                    "source and target column layouts or types are incompatible",
                ));
            }
            let quoted = qualified(target, &name.0, &name.1)?;
            let rows = target.query(&format!("SELECT 1 FROM ONLY {quoted} LIMIT 1"), io)?;
            if rows.rows() != 0 {
                return Err(refusal("COPY requires empty target tables"));
            }
            let names: Vec<String> = columns.into_iter().map(|column| column.name).collect();
            charge(
                &mut remaining,
                std::mem::size_of::<CopyTable>() + name.0.len() + name.1.len(),
            )?;
            for column in &names {
                charge(&mut remaining, std::mem::size_of::<String>() + column.len())?;
            }
            tables.push(CopyTable {
                name: QualifiedTable {
                    schema: name.0.clone(),
                    table: name.1.clone(),
                },
                columns: names,
                format: CopyFormat::Text,
            });
        }
        Ok(CopyPlan {
            tables,
            buffer_bytes,
        })
    })();
    if outcome.is_err() {
        rollback(source, target, io);
    }
    outcome
}

fn configure_session(connection: &mut PgConnection, io: &IoControl) -> Result<()> {
    require_setting(connection, "server_encoding", "UTF8", io)?;
    for (name, value) in [
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, YMD"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("extra_float_digits", "3"),
        ("bytea_output", "hex"),
        ("row_security", "off"),
        ("search_path", "pg_catalog"),
    ] {
        set_setting(connection, name, value, io)?;
    }
    Ok(())
}

fn set_setting(
    connection: &mut PgConnection,
    name: &str,
    value: &str,
    io: &IoControl,
) -> Result<()> {
    // Only internal setting names and literal values reach this SQL construction.
    command(connection, &format!("SET {name} TO '{value}'"), "SET", io)?;
    require_setting(connection, name, value, io)
}

fn setting(connection: &mut PgConnection, name: &str, io: &IoControl) -> Result<String> {
    let result = connection.query(&format!("SHOW {name}"), io)?;
    if result.rows() != 1 {
        return Err(refusal("PostgreSQL returned an invalid session setting"));
    }
    Ok(text(&result, 0, 0)?.to_owned())
}

fn require_setting(
    connection: &mut PgConnection,
    name: &str,
    expected: &str,
    io: &IoControl,
) -> Result<()> {
    if setting(connection, name, io)? != expected {
        return Err(refusal(&format!("COPY requires {name} = {expected}")));
    }
    Ok(())
}

pub(crate) fn configure_durability(
    connection: &mut PgConnection,
    io: &IoControl,
) -> Result<&'static str> {
    let current = setting(connection, "synchronous_commit", io)?;
    let required = durable_setting(&current)?;
    confirm_durability(connection, required, io)?;
    Ok(required)
}

pub(crate) fn confirm_durability(
    connection: &mut PgConnection,
    required: &str,
    io: &IoControl,
) -> Result<()> {
    require_setting(connection, "fsync", "on", io)?;
    set_setting(connection, "synchronous_commit", required, io)
}

fn durable_setting(setting: &str) -> Result<&'static str> {
    match setting {
        "remote_apply" => Ok("remote_apply"),
        "off" | "local" | "remote_write" | "on" => Ok("on"),
        _ => Err(refusal("unsupported target synchronous_commit setting")),
    }
}

fn require_target_execution(
    connection: &mut PgConnection,
    table_oid: u32,
    workset: &BTreeSet<u32>,
    io: &IoControl,
) -> Result<()> {
    let mut after = 0u32;
    loop {
        let constraints = connection.query(&format!(
            "SELECT c.oid, c.contype, c.condeferrable, c.confrelid,
                pg_catalog.cardinality(c.conkey) > 0
                AND pg_catalog.cardinality(c.conkey) = pg_catalog.cardinality(c.confkey)
                AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.generate_subscripts(c.conkey, 1) AS k(position)
                    LEFT JOIN pg_catalog.pg_attribute child ON child.attrelid = c.conrelid AND child.attnum = c.conkey[k.position]
                    LEFT JOIN pg_catalog.pg_attribute parent ON parent.attrelid = c.confrelid AND parent.attnum = c.confkey[k.position]
                    WHERE child.atttypid IS NULL OR parent.atttypid IS NULL OR child.atttypid <> parent.atttypid),
                pg_catalog.cardinality(c.conpfeqop) > 0
                AND pg_catalog.cardinality(c.conppeqop) > 0
                AND pg_catalog.cardinality(c.conffeqop) > 0
                AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.unnest(c.conpfeqop || c.conppeqop || c.conffeqop) AS comparison(oid)
                    LEFT JOIN pg_catalog.pg_operator op ON op.oid = comparison.oid
                    LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = op.oprnamespace
                    WHERE op.oid IS NULL OR op.oid >= {FIRST_NORMAL_OBJECT_ID} OR ns.nspname IS DISTINCT FROM 'pg_catalog')
             FROM pg_catalog.pg_constraint c
             WHERE c.conrelid = {table_oid} AND c.contype IN ('c', 'f', 'x') AND c.oid > {after}
             ORDER BY c.oid LIMIT {CATALOG_PAGE}"
        ), io)?;
        for row in 0..constraints.rows() {
            let oid = object_id(&constraints, row, 0)?;
            if oid <= after {
                return Err(refusal("constraint catalog page did not advance"));
            }
            after = oid;
            match text(&constraints, row, 1)? {
                "c" => return Err(refusal("E1 COPY does not support target CHECK constraints")),
                "x" => {
                    return Err(refusal(
                        "E1 COPY does not support target exclusion constraints",
                    ));
                }
                "f" => {
                    if constraints.cell(row, 2)? != Some(b"t") {
                        return Err(refusal("E1 COPY requires deferrable target foreign keys"));
                    }
                    if !workset.contains(&object_id(&constraints, row, 3)?) {
                        return Err(refusal(
                            "E1 COPY requires foreign-key parents inside the validated copy workset",
                        ));
                    }
                    if constraints.cell(row, 4)? != Some(b"t") {
                        return Err(refusal(
                            "E1 COPY requires identical built-in types for foreign-key column pairs",
                        ));
                    }
                    if constraints.cell(row, 5)? != Some(b"t") {
                        return Err(refusal(
                            "E1 COPY requires built-in foreign-key comparison operators",
                        ));
                    }
                }
                _ => return Err(refusal("unsupported target constraint kind")),
            }
        }
        if constraints.rows() < CATALOG_PAGE {
            break;
        }
    }
    // Even an emptiness query can invoke an index access method during planning.
    let indexes = connection.query(
        &format!(
            "SELECT NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_index i
            LEFT JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid
            LEFT JOIN pg_catalog.pg_am am ON am.oid = idx.relam
            WHERE i.indrelid = {table_oid} AND (
                i.indexprs IS NOT NULL OR i.indpred IS NOT NULL
                OR am.oid IS NULL OR am.oid >= {FIRST_NORMAL_OBJECT_ID}
                OR am.amname NOT IN ('btree', 'hash', 'gist', 'gin', 'spgist', 'brin')
                OR pg_catalog.cardinality(i.indclass::oid[]) <> i.indnkeyatts
                OR EXISTS (
                    SELECT 1 FROM pg_catalog.unnest(i.indclass::oid[]) AS key(oid)
                    LEFT JOIN pg_catalog.pg_opclass op ON op.oid = key.oid
                    LEFT JOIN pg_catalog.pg_namespace ns ON ns.oid = op.opcnamespace
                    WHERE op.oid IS NULL OR op.oid >= {FIRST_NORMAL_OBJECT_ID}
                        OR ns.nspname IS DISTINCT FROM 'pg_catalog' OR op.opcmethod IS DISTINCT FROM am.oid)))"
        ),
        io,
    )?;
    if indexes.rows() != 1 || indexes.cell(0, 0)? != Some(b"t") {
        return Err(refusal(
            "E1 COPY requires plain indexes with built-in access methods and operator classes",
        ));
    }
    Ok(())
}

fn object_id(result: &PgResult, row: usize, column: usize) -> Result<u32> {
    text(result, row, column)?
        .parse()
        .map_err(|_| refusal("catalog returned an invalid object identity"))
}

fn inventory(
    connection: &mut PgConnection,
    io: &IoControl,
    is_target: bool,
) -> Result<BTreeMap<(String, String), Relation>> {
    let mut tables = BTreeMap::new();
    let mut after = 0u32;
    let mut remaining = CATALOG_BYTES;
    loop {
        let result = connection.query(&format!(
            "SELECT c.oid, n.nspname, c.relname,
                c.relkind = 'r' AND c.relpersistence = 'p'
                AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
                AND NOT c.relispartition AND c.reloftype = 0 AND am.amname = 'heap' AND am.oid < {FIRST_NORMAL_OBJECT_ID}
                AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid)
                AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid
                    AND (NOT t.tgisinternal OR ({is_target} AND t.tgisinternal
                        AND t.tgconstraint <> 0 AND t.tgenabled NOT IN ('O', 'A'))))
                AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class = c.oid)
             FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
             WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
               AND c.relkind IN ('r','p','v','m','f') AND c.oid > {after}
             ORDER BY c.oid LIMIT {CATALOG_PAGE}"
        ), io)?;
        for row in 0..result.rows() {
            let oid = object_id(&result, row, 0)?;
            if oid <= after {
                return Err(refusal("catalog page did not advance"));
            }
            let schema = text(&result, row, 1)?.to_owned();
            let table = text(&result, row, 2)?.to_owned();
            charge(
                &mut remaining,
                2 * (schema.len() + table.len()) + std::mem::size_of::<Relation>() + 128,
            )?;
            let relation = Relation {
                oid,
                schema: schema.clone(),
                table: table.clone(),
                supported: result.cell(row, 3)? == Some(b"t"),
            };
            if tables.insert((schema, table), relation).is_some() {
                return Err(refusal("catalog contains duplicate table identities"));
            }
            after = oid;
        }
        if result.rows() < CATALOG_PAGE {
            return Ok(tables);
        }
    }
}

fn columns(connection: &mut PgConnection, oid: u32, io: &IoControl) -> Result<Vec<Column>> {
    let result = connection.query(
        &format!(
            "SELECT a.attname, tn.nspname, t.typname, t.typtype, a.atttypmod, a.attnotnull,
                a.attisdropped, a.attgenerated, a.attidentity, a.attislocal, a.attinhcount,
                COALESCE(cn.nspname, ''), COALESCE(co.collname, '')
         FROM pg_catalog.pg_attribute a
         LEFT JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
         LEFT JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
         LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
         LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace
         WHERE a.attrelid = {oid} AND a.attnum > 0 ORDER BY a.attnum LIMIT 1601"
        ),
        io,
    )?;
    if result.rows() == 0 || result.rows() > 1600 {
        return Err(refusal("COPY requires a supported nonempty column layout"));
    }
    let mut columns = Vec::new();
    for row in 0..result.rows() {
        if result.cell(row, 6)? != Some(b"f")
            || !text(&result, row, 7)?.is_empty()
            || !text(&result, row, 8)?.is_empty()
            || result.cell(row, 9)? != Some(b"t")
            || text(&result, row, 10)? != "0"
        {
            return Err(refusal(
                "COPY does not support dropped, generated, identity, or inherited columns",
            ));
        }
        if !supported_type(
            text(&result, row, 1)?,
            text(&result, row, 3)?,
            text(&result, row, 2)?,
        ) {
            return Err(refusal("COPY does not support this column type"));
        }
        columns.push(Column {
            name: text(&result, row, 0)?.to_owned(),
            type_name: text(&result, row, 2)?.to_owned(),
            modifier: text(&result, row, 4)?.to_owned(),
            not_null: result.cell(row, 5)? == Some(b"t"),
            collation_schema: text(&result, row, 11)?.to_owned(),
            collation: text(&result, row, 12)?.to_owned(),
        });
    }
    Ok(columns)
}

fn supported_type(namespace: &str, kind: &str, name: &str) -> bool {
    namespace == "pg_catalog"
        && kind == "b"
        && matches!(
            name,
            "bool"
                | "int2"
                | "int4"
                | "int8"
                | "float4"
                | "float8"
                | "numeric"
                | "text"
                | "varchar"
                | "bpchar"
                | "bytea"
                | "date"
                | "time"
                | "timetz"
                | "timestamp"
                | "timestamptz"
                | "interval"
                | "uuid"
                | "json"
                | "jsonb"
                | "bit"
                | "varbit"
                | "inet"
                | "cidr"
                | "macaddr"
                | "macaddr8"
        )
}

fn require_supported(relation: &Relation) -> Result<()> {
    if !relation.supported {
        return Err(refusal(
            "COPY requires logged ordinary heap tables without RLS, unsupported trigger states, rules, inheritance, or partitions",
        ));
    }
    Ok(())
}

fn charge(remaining: &mut usize, bytes: usize) -> Result<()> {
    *remaining = remaining
        .checked_sub(bytes)
        .ok_or_else(|| refusal("COPY catalog exceeds the E1 8 MiB catalog budget"))?;
    Ok(())
}

fn text(result: &PgResult, row: usize, column: usize) -> Result<&str> {
    let bytes = result
        .cell(row, column)?
        .ok_or_else(|| refusal("catalog returned an unexpected NULL"))?;
    std::str::from_utf8(bytes).map_err(|_| refusal("catalog returned invalid UTF8"))
}

pub(crate) fn qualified(
    connection: &mut PgConnection,
    schema: &str,
    table: &str,
) -> Result<String> {
    Ok(format!(
        "{}.{}",
        connection.quote_identifier(schema)?,
        connection.quote_identifier(table)?
    ))
}

pub(crate) fn command(
    connection: &mut PgConnection,
    sql: &str,
    expected: &str,
    io: &IoControl,
) -> Result<()> {
    if connection.query(sql, io)?.command_tag()? != expected {
        return Err(refusal("PostgreSQL returned an unexpected command result"));
    }
    Ok(())
}

pub(crate) fn rollback(source: &mut PgConnection, target: &mut PgConnection, io: &IoControl) {
    let _ = target.query("ROLLBACK", io);
    let _ = source.query("ROLLBACK", io);
}

fn refusal(message: &str) -> Error {
    Error {
        kind: ErrorKind::BadConfig,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::Conninfo;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    #[test]
    fn copy_requires_durable_commit_without_weakening_remote_apply() {
        for (current, expected) in [
            ("off", "on"),
            ("local", "on"),
            ("remote_write", "on"),
            ("on", "on"),
            ("remote_apply", "remote_apply"),
        ] {
            assert_eq!(durable_setting(current).unwrap(), expected);
        }
        assert!(durable_setting("unknown").is_err());
    }

    #[test]
    fn text_copy_rejects_types_with_unverified_input_output_semantics() {
        for name in [
            "text",
            "bytea",
            "int4",
            "numeric",
            "date",
            "interval",
            "timestamptz",
            "float8",
        ] {
            assert!(supported_type("pg_catalog", "b", name));
        }
        for (namespace, kind, name) in [
            ("public", "b", "text"),
            ("pg_catalog", "d", "text"),
            ("pg_catalog", "b", "money"),
            ("pg_catalog", "b", "regclass"),
            ("pg_catalog", "b", "_int4"),
            ("public", "e", "color"),
        ] {
            assert!(!supported_type(namespace, kind, name));
        }
    }

    #[test]
    #[ignore = "requires the disposable PostgreSQL harness"]
    fn live_preflight_retains_target_locks_and_source_snapshot() {
        std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("disposable harness is required");
        let source_info =
            Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_SOURCE").unwrap()).unwrap();
        let target_info =
            Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_TARGET").unwrap()).unwrap();
        let io = IoControl {
            deadline: Instant::now() + Duration::from_secs(20),
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        let mut source = PgConnection::connect(&source_info, &io).unwrap();
        let mut target = PgConnection::connect(&target_info, &io).unwrap();
        let mut source_writer = PgConnection::connect(&source_info, &io).unwrap();
        let mut target_writer = PgConnection::connect(&target_info, &io).unwrap();
        source_writer
            .query("SET lock_timeout = '100ms'", &io)
            .unwrap();
        target_writer
            .query("SET lock_timeout = '100ms'", &io)
            .unwrap();
        let plan = plan_copy(&mut source, &mut target, &io, 8 * 1024 * 1024).unwrap();
        assert_eq!(plan.tables.len(), 2);
        assert_eq!(
            setting(&mut source, "transaction_read_only", &io).unwrap(),
            "on"
        );
        assert_eq!(
            setting(&mut source, "transaction_isolation", &io).unwrap(),
            "repeatable read"
        );

        source_writer
            .query(
                "INSERT INTO \"E1 space\".\"empty table\" VALUES ('snapshot-canary', NULL)",
                &io,
            )
            .unwrap();
        let count = source
            .query("SELECT count(*) FROM \"E1 space\".\"empty table\"", &io)
            .unwrap();
        assert_eq!(count.cell(0, 0).unwrap(), Some(&b"0"[..]));
        for table in &plan.tables {
            let source_name =
                qualified(&mut source_writer, &table.name.schema, &table.name.table).unwrap();
            let target_name =
                qualified(&mut target_writer, &table.name.schema, &table.name.table).unwrap();
            for (connection, sql) in [
                (
                    &mut source_writer,
                    format!("ALTER TABLE {source_name} ADD COLUMN unexpected integer"),
                ),
                (
                    &mut target_writer,
                    format!("INSERT INTO {target_name} DEFAULT VALUES"),
                ),
            ] {
                let error = connection
                    .query(&sql, &io)
                    .err()
                    .expect("concurrent change must wait for the retained lock");
                assert!(error.message.contains("55P03"), "{}", error.message);
            }
            let error = target_writer
                .query(
                    &format!("ALTER TABLE {target_name} ADD COLUMN unexpected integer"),
                    &io,
                )
                .err()
                .unwrap();
            assert!(error.message.contains("55P03"), "{}", error.message);
        }
        rollback(&mut source, &mut target, &io);
        source_writer.query("DELETE FROM \"E1 space\".\"empty table\" WHERE \"nullable value\" = 'snapshot-canary'", &io).unwrap();
        assert_eq!(
            target
                .query("SELECT count(*) FROM \"E1 space\".\"empty table\"", &io)
                .unwrap()
                .cell(0, 0)
                .unwrap(),
            Some(&b"0"[..])
        );
    }
}
