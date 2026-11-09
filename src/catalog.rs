use std::collections::{BTreeMap, BTreeSet};

mod clone;
pub(crate) mod follow;
pub(crate) use clone::{
    analyze_clone, clone_object_inventory, inspect_clone_tables, plan_clone, read_clone_source,
    read_clone_target, reconcile_clone_tables, sync_clone_sequences, verify_exact_exclusions,
};
pub(crate) use follow::{
    FollowRelation, check_follow_health, follow_confirmed_flush, follow_start_confirmed_flush,
    preflight_follow_relation,
};

use crate::cdc::contracts::{Column as CdcColumn, Event, RelationKey};
use crate::contracts::{
    CopyFormat, CopyPlan, CopyTable, Error, ErrorKind, IoControl, QualifiedTable, Result,
};
use crate::identity::SourceId;
#[cfg(test)]
use crate::identity::StreamId;
use crate::pg::{PgConnection, PgParam, PgResult, TransactionStatus};
#[cfg(test)]
use crate::state::SourceSlotBinding;

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

#[derive(Debug, Eq, PartialEq)]
#[cfg(test)]
enum CopyInventoryPhase {
    Open,
    Eof,
    Failed,
}

#[cfg(test)]
pub(crate) struct CopyInventory {
    previous: Option<QualifiedTable>,
    phase: CopyInventoryPhase,
    backend_pid: u32,
}

#[cfg(test)]
impl CopyInventory {
    fn require_owner(&self, pid: u32, status: TransactionStatus, logical: bool) -> Result<()> {
        if pid == 0
            || pid != self.backend_pid
            || status != TransactionStatus::InTransaction
            || logical
        {
            return Err(refusal(
                "COPY inventory requires its healthy original SQL transaction",
            ));
        }
        Ok(())
    }

    fn ready(&self, pid: u32, status: TransactionStatus, logical: bool) -> Result<bool> {
        self.require_owner(pid, status, logical)?;
        match self.phase {
            CopyInventoryPhase::Open => Ok(true),
            CopyInventoryPhase::Eof => Ok(false),
            CopyInventoryPhase::Failed => Err(refusal("COPY inventory admission has failed")),
        }
    }

    fn accept_page(
        &mut self,
        tag: &str,
        rows: Vec<(QualifiedTable, u32)>,
    ) -> Result<Option<Vec<(QualifiedTable, u32)>>> {
        let result = (|| {
            if self.phase != CopyInventoryPhase::Open
                || rows.len() > CATALOG_PAGE
                || tag != format!("FETCH {}", rows.len())
            {
                return Err(refusal("COPY inventory returned an invalid FETCH page"));
            }
            let mut previous = self.previous.as_ref();
            for (table, oid) in &rows {
                require_copy_name(table)?;
                if *oid == 0
                    || previous.is_some_and(|last| {
                        (&last.schema, &last.table) >= (&table.schema, &table.table)
                    })
                {
                    return Err(refusal("COPY inventory identities did not advance"));
                }
                previous = Some(table);
            }
            if let Some((last, _)) = rows.last() {
                self.previous = Some(QualifiedTable {
                    schema: last.schema.clone(),
                    table: last.table.clone(),
                });
            }
            if rows.len() < CATALOG_PAGE {
                self.phase = CopyInventoryPhase::Eof;
            }
            Ok(if rows.is_empty() { None } else { Some(rows) })
        })();
        if result.is_err() {
            self.phase = CopyInventoryPhase::Failed;
        }
        result
    }
}

fn require_copy_name(table: &QualifiedTable) -> Result<()> {
    if [&table.schema, &table.table]
        .iter()
        .any(|name| name.is_empty() || name.len() > 63 || name.contains('\0'))
    {
        return Err(refusal(
            "COPY inventory requires bounded PostgreSQL identifiers",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn require_copy_inventory_columns(columns: usize) -> Result<()> {
    if columns != 3 {
        return Err(refusal("COPY inventory requires exactly three fields"));
    }
    Ok(())
}

#[cfg(test)]
fn copy_inventory_row(fields: &[Option<&[u8]>]) -> Result<(QualifiedTable, u32)> {
    if fields.len() != 3 {
        return Err(refusal("COPY inventory requires exactly three fields"));
    }
    let fields = fields
        .iter()
        .map(|field| {
            std::str::from_utf8(field.ok_or_else(|| refusal("COPY inventory returned NULL"))?)
                .map_err(|_| refusal("COPY inventory returned invalid UTF8"))
        })
        .collect::<Result<Vec<_>>>()?;
    let oid = cdc_object_id(fields[0])?;
    let table = QualifiedTable {
        schema: fields[1].into(),
        table: fields[2].into(),
    };
    require_copy_name(&table)?;
    Ok((table, oid))
}

#[cfg(test)]
pub(crate) struct CopyRelation {
    event: Event,
    layout: Vec<Column>,
}

#[cfg(test)]
impl CopyRelation {
    pub(crate) fn into_parts(self, format: CopyFormat) -> Result<(CopyTable, Event)> {
        let (schema, name) = cdc_target_expected(&self.event)?;
        let Event::Relation { key, columns, .. } = &self.event else {
            return Err(refusal("COPY requires canonical relation metadata"));
        };
        if key.oid == 0
            || key.generation == 0
            || columns.len() != self.layout.len()
            || columns
                .iter()
                .zip(&self.layout)
                .any(|(column, layout)| column.name != layout.name)
        {
            return Err(refusal(
                "COPY relation metadata has an invalid identity or layout",
            ));
        }
        let table = CopyTable {
            name: QualifiedTable {
                schema: schema.into(),
                table: name.into(),
            },
            columns: columns.iter().map(|column| column.name.clone()).collect(),
            format,
        };
        Ok((table, self.event))
    }
}

#[cfg(test)]
fn require_copy_source_relation(
    table: &QualifiedTable,
    oid: u32,
    epoch: u64,
    actual: &Relation,
) -> Result<()> {
    require_copy_name(table)?;
    require_supported(actual)?;
    if oid == 0
        || epoch == 0
        || oid != actual.oid
        || table.schema != actual.schema
        || table.table != actual.table
    {
        return Err(refusal(
            "COPY source relation identity or catalog epoch does not match",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn require_copy_locked_oid(expected: u32, resolved: &str) -> Result<()> {
    if expected == 0 || cdc_object_id(resolved)? != expected {
        return Err(refusal("COPY locked source relation OID does not match"));
    }
    Ok(())
}

#[cfg(test)]
fn require_copy_layout(expected: &[Column], actual: &[Column]) -> Result<()> {
    if expected != actual {
        return Err(refusal(
            "source and target column layouts or types are incompatible",
        ));
    }
    Ok(())
}

fn require_copy_owned_relations(owned: &[u32; 2]) -> Result<()> {
    if owned[0] == 0 || owned[1] == 0 || owned[0] == owned[1] {
        return Err(refusal(
            "COPY requires two nonzero distinct owned target class OIDs",
        ));
    }
    Ok(())
}

fn require_copy_target_unowned(target_oid: u32, owned: &[u32; 2]) -> Result<()> {
    require_copy_owned_relations(owned)?;
    if target_oid == 0 {
        return Err(refusal("COPY target class OID is invalid"));
    }
    if owned.contains(&target_oid) {
        return Err(refusal(
            "COPY cannot target an owned target metadata relation",
        ));
    }
    Ok(())
}

fn copy_target_class_oid(
    target: &mut PgConnection,
    schema: &str,
    name: &str,
    owned: &[u32; 2],
    io: &IoControl,
) -> Result<u32> {
    // The ordinary-table lookup hides indexes, so check class identity before eligibility.
    target.prepare(
        "",
        "SELECT c.oid
         FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname::pg_catalog.text = $1 AND c.relname::pg_catalog.text = $2
           AND n.nspname = $1::pg_catalog.name AND c.relname = $2::pg_catalog.name LIMIT 2",
        &[25, 25],
        io,
    )?;
    let result = target.execute_prepared(
        "",
        &[
            PgParam::Text(schema.as_bytes()),
            PgParam::Text(name.as_bytes()),
        ],
        io,
    )?;
    if result.rows() != 1 || result.columns()? != 1 {
        return Err(refusal(
            "COPY target class identity is missing or ambiguous",
        ));
    }
    let oid = cdc_object_id(text(&result, 0, 0)?)?;
    require_copy_target_unowned(oid, owned)?;
    Ok(oid)
}

/// Probing source identity must preserve the caller's transaction and session state.
/// Missing control-read privilege is an error because source identity cannot be optional.
#[cfg(test)]
pub(crate) fn read_source_identity(
    connection: &mut PgConnection,
    io: &IoControl,
) -> Result<(SourceId, u32)> {
    let result = connection.query(
        "SELECT c.system_identifier::pg_catalog.text, d.oid, pg_catalog.pg_backend_pid(),
                pg_catalog.current_setting('server_version_num')
         FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_database d
         WHERE d.datname OPERATOR(pg_catalog.=) pg_catalog.current_database()",
        io,
    )?;
    if result.rows() != 1 {
        return Err(refusal("source SQL returned an invalid identity result"));
    }
    source_identity_fields([
        text(&result, 0, 0)?,
        text(&result, 0, 1)?,
        text(&result, 0, 2)?,
        text(&result, 0, 3)?,
    ])
}

#[cfg(test)]
fn source_identity_fields(fields: [&str; 4]) -> Result<(SourceId, u32)> {
    let system = fields[0]
        .parse::<i64>()
        .map_err(|_| refusal("source SQL returned an invalid system identity"))?;
    if system == 0 || system.to_string() != fields[0] {
        return Err(refusal("source SQL returned an invalid system identity"));
    }
    let database_oid = cdc_object_id(fields[1])?;
    let pid = fields[2]
        .parse::<i32>()
        .map_err(|_| refusal("source SQL returned an invalid backend identity"))?;
    if pid <= 0 || pid.to_string() != fields[2] {
        return Err(refusal("source SQL returned an invalid backend identity"));
    }
    let version = fields[3]
        .parse::<u32>()
        .map_err(|_| refusal("source SQL returned an invalid server version"))?;
    if version.to_string() != fields[3] || !matches!(version / 10000, 16 | 18) {
        return Err(refusal(
            "source identity requires the PostgreSQL 16 or 18 profile",
        ));
    }
    Ok((
        SourceId {
            // The SQL bigint exposes the unsigned control-file identifier's bit pattern.
            system_identifier: system as u64,
            database_oid,
        },
        pid as u32,
    ))
}

#[cfg(test)]
fn require_cdc_source_idle(status: TransactionStatus, logical: bool) -> Result<()> {
    if status != TransactionStatus::Idle || logical {
        return Err(refusal(
            "CDC source requires an idle ordinary SQL connection",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn require_cdc_source_identity(
    expected: SourceId,
    pid: u32,
    actual: (SourceId, u32),
) -> Result<()> {
    if pid == 0 || pid > i32::MAX as u32 || actual != (expected, pid) {
        return Err(refusal("CDC source identity or physical backend changed"));
    }
    Ok(())
}

#[cfg(test)]
fn require_cdc_source_settings(read_only: &str, isolation: &str, recovery: &str) -> Result<()> {
    if read_only != "on" || isolation != "read committed" || recovery != "f" {
        return Err(refusal(
            "CDC source requires READ COMMITTED READ ONLY on its primary",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn cdc_source_table(expected: &Event, catalog_epoch: u64) -> Result<QualifiedTable> {
    let Event::Relation {
        key, schema, name, ..
    } = expected
    else {
        return Err(refusal("CDC source check requires relation metadata"));
    };
    cdc_relation_budget(schema, name)?;
    if key.oid == 0 || catalog_epoch == 0 || key.generation != catalog_epoch {
        return Err(refusal(
            "CDC source relation identity or catalog epoch does not match its native binding",
        ));
    }
    Ok(QualifiedTable {
        schema: schema.clone(),
        table: name.clone(),
    })
}

#[cfg(test)]
fn require_cdc_source_match(expected: &Event, actual: &Event) -> Result<()> {
    if !matches!(
        (expected, actual),
        (Event::Relation { .. }, Event::Relation { .. })
    ) || expected != actual
    {
        return Err(refusal(
            "CDC source relation metadata does not match its exact source event",
        ));
    }
    Ok(())
}

fn require_cdc_target_transaction(status: TransactionStatus) -> Result<()> {
    if status != TransactionStatus::InTransaction {
        return Err(refusal(
            "CDC target preflight requires an active transaction",
        ));
    }
    Ok(())
}

fn require_cdc_target_settings(read_only: &str, isolation: &str, role: &str) -> Result<()> {
    if read_only != "off" || isolation != "read committed" || role != "origin" {
        return Err(refusal(
            "CDC target preflight requires READ COMMITTED READ WRITE and session_replication_role = origin",
        ));
    }
    Ok(())
}

fn cdc_target_expected(expected: &Event) -> Result<(&str, &str)> {
    let Event::Relation {
        schema,
        name,
        columns,
        replica_identity,
        ..
    } = expected
    else {
        return Err(refusal("CDC target preflight requires relation metadata"));
    };
    let mut remaining = cdc_relation_budget(schema, name)?;
    if *replica_identity != b'd'
        || columns.is_empty()
        || columns.len() > 1600
        || !columns.iter().any(|column| column.key)
    {
        return Err(refusal(
            "CDC target preflight requires DEFAULT primary-key metadata",
        ));
    }
    let mut names = BTreeSet::new();
    for column in columns {
        if column.name.is_empty()
            || column.name.contains('\0')
            || !names.insert(column.name.as_str())
            || column.type_oid == 0
            || column.type_oid >= FIRST_NORMAL_OBJECT_ID
        {
            return Err(refusal(
                "CDC target preflight received invalid column metadata",
            ));
        }
        charge(&mut remaining, std::mem::size_of::<CdcColumn>())?;
        charge(&mut remaining, column.name.len())?;
    }
    Ok((schema, name))
}

fn require_cdc_target_match(expected: &Event, actual: &Event) -> Result<()> {
    cdc_target_expected(expected)?;
    match (expected, actual) {
        (
            Event::Relation {
                schema,
                name,
                columns,
                replica_identity,
                ..
            },
            Event::Relation {
                schema: actual_schema,
                name: actual_name,
                columns: actual_columns,
                replica_identity: actual_identity,
                ..
            },
        ) if schema == actual_schema
            && name == actual_name
            && columns == actual_columns
            && replica_identity == actual_identity =>
        {
            Ok(())
        }
        _ => Err(refusal(
            "CDC target relation metadata does not match the source relation",
        )),
    }
}

fn require_cdc_target_identity(before: &Relation, after: &Relation) -> Result<()> {
    require_supported(after)?;
    if before != after {
        return Err(refusal(
            "CDC target relation changed before its lock was acquired",
        ));
    }
    Ok(())
}

fn cdc_relation_budget(schema: &str, table: &str) -> Result<usize> {
    let mut remaining = CATALOG_BYTES;
    for name in [schema, table] {
        if name.is_empty() || name.contains('\0') || name.len() > CATALOG_BYTES {
            return Err(refusal("CDC requires valid nonempty relation names"));
        }
        charge(&mut remaining, name.len())?;
    }
    Ok(remaining)
}

/// The caller establishes the source catalog snapshot, generation, and table/namespace DDL quiescence.
/// A fresh lookup cannot prove which metadata described an earlier change.
/// The caller retains ownership of its transaction and locks.
#[cfg(test)]
pub(crate) fn load_cdc_relation(
    source: &mut PgConnection,
    table: &QualifiedTable,
    generation: u64,
    io: &IoControl,
) -> Result<Event> {
    let remaining = cdc_relation_budget(&table.schema, &table.table)?;
    configure_session(source, io)?;
    let relation = inventory(source, io, false)?
        .remove(&(table.schema.clone(), table.table.clone()))
        .ok_or_else(|| refusal("CDC source relation does not exist in a supported schema"))?;
    let layout = cdc_relation_layout(source, &relation, io)?;
    require_plain_indexes(source, relation.oid, io)?;
    cdc_relation_event(source, relation, &layout, generation, remaining, io)
}

/// TargetTxn supplies its verified fenced connection and poisons itself on every error.
/// The caller keeps table/namespace DDL quiescent and invokes preflight on first touch in each transaction.
/// The caller retains these locks until COMMIT and reacquires them in each later transaction.
pub(crate) fn preflight_cdc_target(
    target: &mut PgConnection,
    expected: &Event,
    io: &IoControl,
) -> Result<()> {
    checked_cdc_target_layout(target, expected, None, io).map(|_| ())
}

fn checked_cdc_target_layout(
    target: &mut PgConnection,
    expected: &Event,
    owned_target_relations: Option<&[u32; 2]>,
    io: &IoControl,
) -> Result<Vec<Column>> {
    let (schema, name) = cdc_target_expected(expected)?;
    require_cdc_target_transaction(target.transaction_status()?)?;
    let settings = target.query(
        "SELECT pg_catalog.current_setting('transaction_read_only'),
                pg_catalog.current_setting('transaction_isolation'),
                pg_catalog.current_setting('session_replication_role')",
        io,
    )?;
    if settings.rows() != 1 {
        return Err(refusal("CDC target returned invalid transaction settings"));
    }
    require_cdc_target_settings(
        text(&settings, 0, 0)?,
        text(&settings, 0, 1)?,
        text(&settings, 0, 2)?,
    )?;
    configure_session(target, io)?;
    let copy_oid = owned_target_relations
        .map(|owned| copy_target_class_oid(target, schema, name, owned, io))
        .transpose()?;
    let before = cdc_target_lookup(target, schema, name, io)?;
    if let Some(owned) = owned_target_relations {
        require_copy_target_unowned(before.oid, owned)?;
        if copy_oid != Some(before.oid) {
            return Err(refusal(
                "COPY target class identity changed before eligibility",
            ));
        }
    }
    require_supported(&before)?;
    let quoted = qualified(target, schema, name)?;
    command(
        target,
        &format!("LOCK TABLE ONLY {quoted} IN SHARE ROW EXCLUSIVE MODE"),
        "LOCK TABLE",
        io,
    )?;
    if let Some(owned) = owned_target_relations {
        let oid = copy_target_class_oid(target, schema, name, owned, io)?;
        if oid != before.oid {
            return Err(refusal(
                "COPY target class identity changed before its lock was acquired",
            ));
        }
    }
    let locked = cdc_target_lookup(target, schema, name, io)?;
    if let Some(owned) = owned_target_relations {
        require_copy_target_unowned(locked.oid, owned)?;
    }
    require_cdc_target_identity(&before, &locked)?;
    if let Some(owned) = owned_target_relations {
        require_copy_target_unowned(locked.oid, owned)?;
    }
    let layout = cdc_relation_layout(target, &locked, io)?;
    let oid = locked.oid;
    let foreign_keys = target.query(
        &format!(
            "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
             WHERE contype = 'f' AND (conrelid = {oid} OR confrelid = {oid}))"
        ),
        io,
    )?;
    if foreign_keys.rows() != 1 || foreign_keys.cell(0, 0)? != Some(b"t") {
        return Err(refusal(
            "CDC target preflight does not support foreign keys",
        ));
    }
    require_target_execution_profile(target, oid, TargetForeignKeys::Refuse, io)?;
    let remaining = cdc_relation_budget(schema, name)?;
    let actual = cdc_relation_event(target, locked, &layout, 0, remaining, io)?;
    require_cdc_target_match(expected, &actual)?;
    let Event::Relation { columns, .. } = actual else {
        return Err(refusal("CDC target preflight requires relation metadata"));
    };
    require_cdc_target_dml_privileges(target, oid, &columns, io)?;
    Ok(layout)
}

fn require_cdc_target_dml_privileges(
    target: &mut PgConnection,
    oid: u32,
    columns: &[CdcColumn],
    io: &IoControl,
) -> Result<()> {
    let keys = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.key)
        .map(|(index, _)| (index + 1).to_string())
        .collect::<Vec<_>>()
        .join(", ");
    if columns.is_empty() || keys.is_empty() {
        return Err(refusal(
            "CDC target permissions require primary-key metadata",
        ));
    }
    let count = columns.len();
    let privileges = target.query(
        &format!(
            "SELECT pg_catalog.has_table_privilege({oid}::oid, 'DELETE')
                AND count(*) = {count}
                AND COALESCE(pg_catalog.bool_and(
                    pg_catalog.has_column_privilege({oid}::oid, a.attnum, 'INSERT')
                    AND pg_catalog.has_column_privilege({oid}::oid, a.attnum, 'UPDATE')
                    AND (a.attnum NOT IN ({keys})
                        OR pg_catalog.has_column_privilege({oid}::oid, a.attnum, 'SELECT'))), false)
             FROM pg_catalog.pg_attribute a
             WHERE a.attrelid = {oid} AND a.attnum > 0 AND NOT a.attisdropped"
        ),
        io,
    )?;
    if privileges.rows() != 1 || privileges.cell(0, 0)? != Some(b"t") {
        return Err(refusal(
            "CDC target lacks required table or column DML privileges",
        ));
    }
    Ok(())
}

fn cdc_target_lookup(
    connection: &mut PgConnection,
    schema: &str,
    name: &str,
    io: &IoControl,
) -> Result<Relation> {
    let eligibility = relation_eligibility(true);
    connection.prepare(
        "",
        &format!(
            "SELECT c.oid, n.nspname, c.relname, {eligibility}
             FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
             WHERE n.nspname::text = $1 AND c.relname::text = $2
               AND n.nspname = $1::pg_catalog.name AND c.relname = $2::pg_catalog.name
               AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
               AND c.relkind IN ('r','p','v','m','f') LIMIT 2"
        ),
        &[25, 25],
        io,
    )?;
    let result = connection.execute_prepared(
        "",
        &[
            PgParam::Text(schema.as_bytes()),
            PgParam::Text(name.as_bytes()),
        ],
        io,
    )?;
    if result.rows() != 1 {
        return Err(refusal("CDC target relation is missing or ambiguous"));
    }
    Ok(Relation {
        oid: cdc_object_id(text(&result, 0, 0)?)?,
        schema: text(&result, 0, 1)?.into(),
        table: text(&result, 0, 2)?.into(),
        supported: result.cell(0, 3)? == Some(b"t"),
    })
}

fn cdc_relation_layout(
    source: &mut PgConnection,
    relation: &Relation,
    io: &IoControl,
) -> Result<Vec<Column>> {
    require_supported(relation)?;
    let layout = columns(source, relation.oid, io)?;
    let oid = relation.oid;
    let identity = source.query(
        &format!("SELECT relreplident FROM pg_catalog.pg_class WHERE oid = {oid}"),
        io,
    )?;
    if identity.rows() != 1 {
        return Err(refusal("CDC source relation metadata is missing"));
    }
    if identity.cell(0, 0)? != Some(b"d") {
        return Err(refusal("CDC requires DEFAULT replica identity"));
    }
    Ok(layout)
}

fn cdc_relation_event(
    source: &mut PgConnection,
    relation: Relation,
    layout: &[Column],
    generation: u64,
    mut remaining: usize,
    io: &IoControl,
) -> Result<Event> {
    let oid = relation.oid;
    let primary = source.query(
        &format!(
            "SELECT i.indkey::text, i.indnkeyatts, i.indnatts,
                i.indisunique AND i.indisvalid AND i.indisready AND i.indislive
                AND i.indimmediate AND NOT i.indcheckxmin
                AND i.indexprs IS NULL AND i.indpred IS NULL
                AND idx.relkind = 'i' AND idx.relpersistence = 'p'
                AND am.amname = 'btree' AND am.oid < {FIRST_NORMAL_OBJECT_ID}
             FROM pg_catalog.pg_index i
             LEFT JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid
             LEFT JOIN pg_catalog.pg_am am ON am.oid = idx.relam
             WHERE i.indrelid = {oid} AND i.indisprimary LIMIT 2"
        ),
        io,
    )?;
    if primary.rows() != 1 || primary.cell(0, 3)? != Some(b"t") {
        return Err(refusal("CDC requires one valid immediate primary key"));
    }
    let keys = cdc_primary_key(
        text(&primary, 0, 0)?,
        text(&primary, 0, 1)?,
        text(&primary, 0, 2)?,
        layout,
    )?;
    let attributes = source.query(
        &format!(
            "SELECT a.attname, a.attnum, a.atttypid, a.atttypmod, t.typname
             FROM pg_catalog.pg_attribute a
             LEFT JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
             WHERE a.attrelid = {oid} AND a.attnum > 0 ORDER BY a.attnum LIMIT 1601"
        ),
        io,
    )?;
    if attributes.rows() != layout.len() {
        return Err(refusal("CDC catalog column layout changed"));
    }
    let mut columns = Vec::with_capacity(layout.len());
    for (row, column) in layout.iter().enumerate() {
        charge(&mut remaining, std::mem::size_of::<CdcColumn>())?;
        charge(&mut remaining, column.name.len())?;
        columns.push(cdc_column(
            column,
            row + 1,
            [
                text(&attributes, row, 0)?,
                text(&attributes, row, 1)?,
                text(&attributes, row, 2)?,
                text(&attributes, row, 3)?,
                text(&attributes, row, 4)?,
            ],
            keys.contains(&(row + 1)),
        )?);
    }
    Ok(Event::Relation {
        key: RelationKey { oid, generation },
        schema: relation.schema,
        name: relation.table,
        columns,
        replica_identity: b'd',
    })
}

fn cdc_object_id(oid: &str) -> Result<u32> {
    if oid.is_empty() || !oid.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(refusal("CDC catalog returned an invalid object identity"));
    }
    let oid = oid
        .parse::<u32>()
        .map_err(|_| refusal("CDC catalog returned an invalid object identity"))?;
    if oid == 0 {
        return Err(refusal("CDC catalog returned an invalid object identity"));
    }
    Ok(oid)
}

fn cdc_attribute_number(value: &str) -> Result<usize> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(refusal("CDC catalog returned invalid attribute metadata"));
    }
    let number = value
        .parse::<usize>()
        .map_err(|_| refusal("CDC catalog returned invalid attribute metadata"))?;
    if !(1..=1600).contains(&number) {
        return Err(refusal("CDC catalog returned invalid attribute metadata"));
    }
    Ok(number)
}

fn cdc_primary_key(
    attributes: &str,
    key_count: &str,
    attribute_count: &str,
    columns: &[Column],
) -> Result<BTreeSet<usize>> {
    let key_count = cdc_attribute_number(key_count)?;
    let attribute_count = cdc_attribute_number(attribute_count)?;
    if key_count > attribute_count || attribute_count > columns.len() {
        return Err(refusal(
            "CDC catalog returned an invalid primary-key layout",
        ));
    }
    let mut seen = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for (position, attribute) in attributes.split_ascii_whitespace().enumerate() {
        let attribute = cdc_attribute_number(attribute)?;
        if position >= attribute_count || attribute > columns.len() || !seen.insert(attribute) {
            return Err(refusal(
                "CDC catalog returned an invalid primary-key layout",
            ));
        }
        if position < key_count {
            if !columns[attribute - 1].not_null {
                return Err(refusal("CDC requires nonnullable primary-key columns"));
            }
            keys.insert(attribute);
        }
    }
    if seen.len() != attribute_count {
        return Err(refusal(
            "CDC catalog returned an invalid primary-key layout",
        ));
    }
    Ok(keys)
}

fn cdc_column(
    column: &Column,
    position: usize,
    metadata: [&str; 5],
    key: bool,
) -> Result<CdcColumn> {
    let [name, attribute, oid, modifier, type_name] = metadata;
    if name.is_empty()
        || name.contains('\0')
        || name != column.name
        || cdc_attribute_number(attribute)? != position
        || type_name != column.type_name
        || modifier != column.modifier
    {
        return Err(refusal(
            "CDC column metadata does not match the eligible layout",
        ));
    }
    let type_oid = cdc_object_id(oid)?;
    if type_oid >= FIRST_NORMAL_OBJECT_ID {
        return Err(refusal("CDC requires built-in column type identities"));
    }
    let type_mod = modifier
        .parse::<i32>()
        .map_err(|_| refusal("CDC catalog returned an invalid column type modifier"))?;
    Ok(CdcColumn {
        name: name.into(),
        type_oid,
        type_mod,
        key,
    })
}

struct CopyPreflight {
    initial_source: BTreeMap<(String, String), Relation>,
    initial_target: BTreeMap<(String, String), Relation>,
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
        let preflight = inspect_copy(source, target, io)?;
        lock_copy_plan(source, target, io, buffer_bytes, preflight)
    })();
    if outcome.is_err() {
        rollback(source, target, io);
    }
    outcome
}

fn inspect_copy(
    source: &mut PgConnection,
    target: &mut PgConnection,
    io: &IoControl,
) -> Result<CopyPreflight> {
    configure_session(source, io)?;
    configure_session(target, io)?;
    configure_durability(target, io)?;
    require_setting(target, "session_replication_role", "origin", io)?;

    let initial_source = inventory(source, io, false)?;
    let initial_target = inventory(target, io, true)?;
    for (name, relation) in &initial_source {
        require_supported(relation)?;
        require_supported(
            initial_target.get(name).ok_or_else(|| {
                refusal("COPY requires every source table to exist on the target")
            })?,
        )?;
    }

    Ok(CopyPreflight {
        initial_source,
        initial_target,
    })
}

fn lock_copy_plan(
    source: &mut PgConnection,
    target: &mut PgConnection,
    io: &IoControl,
    buffer_bytes: usize,
    preflight: CopyPreflight,
) -> Result<CopyPlan> {
    let CopyPreflight {
        initial_source,
        initial_target,
    } = preflight;
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
                .ok_or_else(|| refusal("target catalog changed before COPY locks were acquired"))
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
}

pub(crate) fn configure_session(connection: &mut PgConnection, io: &IoControl) -> Result<()> {
    require_setting(connection, "server_encoding", "UTF8", io)?;
    for (name, value) in [
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, YMD"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("lc_monetary", "C"),
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

pub(crate) fn require_setting(
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
    require_durable_storage(connection, io)?;
    set_setting(connection, "synchronous_commit", required, io)
}

fn require_durable_storage(connection: &mut PgConnection, io: &IoControl) -> Result<()> {
    require_setting(connection, "fsync", "on", io)?;
    require_setting(connection, "full_page_writes", "on", io)
}

fn durable_setting(setting: &str) -> Result<&'static str> {
    match setting {
        "remote_apply" => Ok("remote_apply"),
        "off" | "local" | "remote_write" | "on" => Ok("on"),
        _ => Err(refusal("unsupported target synchronous_commit setting")),
    }
}

enum TargetForeignKeys<'a> {
    CopyWorkset(&'a BTreeSet<u32>),
    Refuse,
}

impl TargetForeignKeys<'_> {
    fn workset(&self) -> Result<&BTreeSet<u32>> {
        match self {
            Self::CopyWorkset(workset) => Ok(workset),
            Self::Refuse => Err(refusal(
                "CDC target preflight does not support foreign keys",
            )),
        }
    }
}

fn require_target_execution(
    connection: &mut PgConnection,
    table_oid: u32,
    workset: &BTreeSet<u32>,
    io: &IoControl,
) -> Result<()> {
    require_target_execution_profile(
        connection,
        table_oid,
        TargetForeignKeys::CopyWorkset(workset),
        io,
    )
}

fn require_target_execution_profile(
    connection: &mut PgConnection,
    table_oid: u32,
    foreign_keys: TargetForeignKeys<'_>,
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
                    let workset = foreign_keys.workset()?;
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
    require_plain_indexes(connection, table_oid, io)
}

fn require_plain_indexes(
    connection: &mut PgConnection,
    table_oid: u32,
    io: &IoControl,
) -> Result<()> {
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

fn relation_eligibility(is_target: bool) -> String {
    format!(
        "c.relkind = 'r' AND c.relpersistence = 'p'
                AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity
                AND NOT c.relispartition AND c.reloftype = 0 AND am.amname = 'heap' AND am.oid < {FIRST_NORMAL_OBJECT_ID}
                AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid = c.oid OR i.inhparent = c.oid)
                AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgrelid = c.oid
                    AND (NOT t.tgisinternal OR ({is_target} AND t.tgisinternal
                        AND t.tgconstraint <> 0 AND t.tgenabled NOT IN ('O', 'A'))))
                AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class = c.oid)"
    )
}

fn inventory(
    connection: &mut PgConnection,
    io: &IoControl,
    is_target: bool,
) -> Result<BTreeMap<(String, String), Relation>> {
    let mut tables = BTreeMap::new();
    let mut after = 0u32;
    let mut remaining = CATALOG_BYTES;
    let eligibility = relation_eligibility(is_target);
    loop {
        let result = connection.query(
            &format!(
                "SELECT c.oid, n.nspname, c.relname,
                {eligibility}
             FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
             WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
               AND c.relkind IN ('r','p','v','m','f') AND c.oid > {after}
             ORDER BY c.oid LIMIT {CATALOG_PAGE}"
            ),
            io,
        )?;
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
        .ok_or_else(|| refusal("catalog exceeds the 8 MiB catalog budget"))?;
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

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadConfig, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::Conninfo;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    #[test]
    fn copy_owned_relations_require_nonzero_distinct_class_oids() {
        for owned in [[0, 23], [17, 0], [17, 17]] {
            assert!(require_copy_owned_relations(&owned).is_err());
        }
        require_copy_owned_relations(&[17, 23]).unwrap();
        require_copy_owned_relations(&[u32::MAX, 1]).unwrap();
    }

    #[test]
    fn copy_owned_target_refuses_actual_table_and_index_class_oids() {
        for oid in [17, 23] {
            let error = require_copy_target_unowned(oid, &[17, 23]).unwrap_err();
            assert!(error.message.contains("owned target metadata"));
        }
        assert!(require_copy_target_unowned(0, &[17, 23]).is_err());
        require_copy_target_unowned(83, &[17, 23]).unwrap();
    }

    #[test]
    fn copy_owned_target_ignores_source_endpoint_oid() {
        let target = cdc_target_relation();
        for source_oid in [17, 23, target.oid] {
            let mut source = cdc_target_event();
            if let Event::Relation { key, .. } = &mut source {
                key.oid = source_oid;
            }
            cdc_target_expected(&source).unwrap();
            require_copy_target_unowned(target.oid, &[17, 23]).unwrap();
        }
    }

    #[test]
    fn copy_owned_target_allows_ordinary_lookalike_names() {
        let mut target = cdc_target_relation();
        for (schema, name) in [
            ("rpgmigrate", "checkpoint"),
            ("metadata", "checkpoint_pkey"),
            ("ordinary", "owned_target_metadata"),
        ] {
            target.schema = schema.into();
            target.table = name.into();
            require_copy_target_unowned(target.oid, &[17, 23]).unwrap();
            require_supported(&target).unwrap();
        }
    }

    fn copy_inventory_state() -> CopyInventory {
        CopyInventory {
            previous: None,
            phase: CopyInventoryPhase::Open,
            backend_pid: 41,
        }
    }

    fn copy_inventory_rows(start: usize, count: usize) -> Vec<(QualifiedTable, u32)> {
        (start..start + count)
            .map(|index| {
                (
                    cdc_table("schema", &format!("table{index:08}")),
                    index as u32 + 1,
                )
            })
            .collect()
    }

    #[test]
    fn copy_inventory_validates_row_shape_names_and_numeric_oid() {
        require_copy_inventory_columns(3).unwrap();
        for columns in [0, 1, 2, 4, usize::MAX] {
            assert!(require_copy_inventory_columns(columns).is_err());
        }
        let good = [
            Some(b"4294967295".as_slice()),
            Some(b"schema".as_slice()),
            Some(b"table".as_slice()),
        ];
        assert_eq!(copy_inventory_row(&good).unwrap().1, u32::MAX);
        assert!(copy_inventory_row(&good[..2]).is_err());
        let mut extra = good.to_vec();
        extra.push(None);
        assert!(copy_inventory_row(&extra).is_err());
        extra[3] = Some(b"extra");
        assert!(copy_inventory_row(&extra).is_err());
        for value in [
            None,
            Some(b"".as_slice()),
            Some(b"a\0b".as_slice()),
            Some(b"\xff".as_slice()),
        ] {
            let mut fields = good;
            fields[1] = value;
            assert!(copy_inventory_row(&fields).is_err());
        }
        for oid in ["0", "-1", "+1", "4294967296", "1 "] {
            let mut fields = good;
            fields[0] = Some(oid.as_bytes());
            assert!(copy_inventory_row(&fields).is_err());
        }
        let long = "x".repeat(64);
        let mut fields = good;
        fields[2] = Some(long.as_bytes());
        assert!(copy_inventory_row(&fields).is_err());
    }

    #[test]
    fn copy_inventory_requires_exact_fetch_count_and_page_bound() {
        for tag in [
            "FETCH 127",
            "FETCH +128",
            "FETCH 0128",
            "FETCH 128 ",
            "SELECT 128",
        ] {
            let mut inventory = copy_inventory_state();
            assert!(
                inventory
                    .accept_page(tag, copy_inventory_rows(0, 128))
                    .is_err()
            );
            assert_eq!(inventory.phase, CopyInventoryPhase::Failed);
        }
        assert!(
            copy_inventory_state()
                .accept_page("FETCH 129", copy_inventory_rows(0, 129))
                .is_err()
        );
        let mut inventory = copy_inventory_state();
        assert_eq!(
            inventory
                .accept_page("FETCH 128", copy_inventory_rows(0, 128))
                .unwrap()
                .unwrap()
                .len(),
            128
        );
        assert_eq!(inventory.phase, CopyInventoryPhase::Open);
    }

    #[test]
    fn copy_inventory_checks_order_across_pages_without_partial_advance() {
        let mut inventory = copy_inventory_state();
        inventory
            .accept_page("FETCH 128", copy_inventory_rows(0, 128))
            .unwrap();
        let mut bad = copy_inventory_rows(128, 128);
        bad[127].0.table = "table00000127".into();
        assert!(inventory.accept_page("FETCH 128", bad).is_err());
        assert_eq!(inventory.previous.as_ref().unwrap().table, "table00000127");
        assert_eq!(inventory.phase, CopyInventoryPhase::Failed);
        let mut ordered = copy_inventory_state();
        copy_inventory_state()
            .accept_page(
                "FETCH 2",
                vec![(cdc_table("A", "z"), 1), (cdc_table("a", "A"), 2)],
            )
            .unwrap();
        assert!(
            copy_inventory_state()
                .accept_page(
                    "FETCH 2",
                    vec![(cdc_table("z", "A"), 1), (cdc_table("a", "z"), 2),]
                )
                .is_err()
        );
        let names = ["A", "Z", "a", "é", "Ω"];
        let rows = names
            .iter()
            .enumerate()
            .map(|(n, name)| (cdc_table("schema", name), n as u32 + 1))
            .collect();
        assert_eq!(
            ordered.accept_page("FETCH 5", rows).unwrap().unwrap().len(),
            5
        );
        let mut duplicate = copy_inventory_state();
        assert!(
            duplicate
                .accept_page(
                    "FETCH 2",
                    vec![(cdc_table("schema", "x"), 1), (cdc_table("schema", "x"), 2)]
                )
                .is_err()
        );
    }

    #[test]
    fn copy_inventory_only_validated_fetch_establishes_eof() {
        let mut empty = copy_inventory_state();
        assert!(empty.accept_page("FETCH 0", vec![]).unwrap().is_none());
        assert_eq!(empty.phase, CopyInventoryPhase::Eof);
        assert!(
            empty
                .ready(42, TransactionStatus::InTransaction, false)
                .is_err()
        );
        assert!(empty.ready(41, TransactionStatus::InError, false).is_err());
        assert!(
            !empty
                .ready(41, TransactionStatus::InTransaction, false)
                .unwrap()
        );
        let mut short = copy_inventory_state();
        assert_eq!(
            short
                .accept_page("FETCH 1", copy_inventory_rows(0, 1))
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(short.phase, CopyInventoryPhase::Eof);
        let mut failed = copy_inventory_state();
        assert!(failed.accept_page("SELECT 0", vec![]).is_err());
        assert!(
            failed
                .ready(41, TransactionStatus::InTransaction, false)
                .is_err()
        );
        assert!(failed.accept_page("FETCH 0", vec![]).is_err());
    }

    #[test]
    fn copy_inventory_requires_healthy_original_ordinary_backend() {
        let inventory = copy_inventory_state();
        assert!(
            inventory
                .ready(41, TransactionStatus::InTransaction, false)
                .unwrap()
        );
        for pid in [0, 42] {
            assert!(
                inventory
                    .ready(pid, TransactionStatus::InTransaction, false)
                    .is_err()
            );
        }
        for status in [
            TransactionStatus::Idle,
            TransactionStatus::Active,
            TransactionStatus::InError,
            TransactionStatus::Unknown,
        ] {
            assert!(inventory.ready(41, status, false).is_err());
        }
        assert!(
            inventory
                .ready(41, TransactionStatus::InTransaction, true)
                .is_err()
        );
    }

    #[test]
    fn copy_inventory_streams_beyond_old_catalog_budget() {
        let mut inventory = copy_inventory_state();
        let schema = "s".repeat(60);
        let mut total_bytes = 0;
        for page in 0..1024 {
            let rows = (page * 128..(page + 1) * 128)
                .map(|n| {
                    let name = format!("table{n:08}{}", "x".repeat(45));
                    total_bytes += schema.len() + name.len();
                    (cdc_table(&schema, &name), n as u32 + 1)
                })
                .collect();
            assert_eq!(
                inventory
                    .accept_page("FETCH 128", rows)
                    .unwrap()
                    .unwrap()
                    .len(),
                128
            );
        }
        assert!(total_bytes > CATALOG_BYTES);
        assert!(inventory.accept_page("FETCH 0", vec![]).unwrap().is_none());
    }

    #[test]
    fn copy_source_requires_exact_oid_name_and_positive_catalog_epoch() {
        let table = cdc_table("target schema", "target table");
        require_copy_source_relation(&table, 83, 7, &cdc_target_relation()).unwrap();
        for (oid, epoch) in [(0, 7), (84, 7), (83, 0)] {
            assert!(
                require_copy_source_relation(&table, oid, epoch, &cdc_target_relation()).is_err()
            );
        }
        let mut changed = cdc_target_relation();
        changed.schema = "different".into();
        assert!(require_copy_source_relation(&table, 83, 7, &changed).is_err());
        changed = cdc_target_relation();
        changed.supported = false;
        assert!(require_copy_source_relation(&table, 83, 7, &changed).is_err());
        require_copy_locked_oid(83, "83").unwrap();
        for value in ["0", "84", "-1", "4294967296"] {
            assert!(require_copy_locked_oid(83, value).is_err());
        }
    }

    #[test]
    fn copy_relation_consumes_ordered_copy_table_and_canonical_event() {
        let event = cdc_target_event();
        for format in [CopyFormat::Text, CopyFormat::Binary] {
            let (table, actual) = CopyRelation {
                event: event.clone(),
                layout: cdc_layout(),
            }
            .into_parts(format)
            .unwrap();
            assert_eq!(actual, event);
            assert_eq!(table.name.schema, "target schema");
            assert_eq!(table.name.table, "target table");
            assert_eq!(table.columns, ["first key", "payload", "last key"]);
            assert_eq!(table.format, format);
        }
        assert!(
            CopyRelation {
                event: Event::Insert {
                    relation: RelationKey {
                        oid: 71,
                        generation: 9
                    },
                    values: vec![]
                },
                layout: cdc_layout(),
            }
            .into_parts(CopyFormat::Text)
            .is_err()
        );
        for zero in 0..2 {
            let mut invalid = cdc_target_event();
            if let Event::Relation { key, .. } = &mut invalid {
                if zero == 0 {
                    key.oid = 0;
                } else {
                    key.generation = 0;
                }
            }
            assert!(
                CopyRelation {
                    event: invalid,
                    layout: cdc_layout()
                }
                .into_parts(CopyFormat::Text)
                .is_err()
            );
        }
        let mut malformed = cdc_target_event();
        if let Event::Relation { columns, .. } = &mut malformed {
            columns.reverse();
        }
        assert!(
            CopyRelation {
                event: malformed,
                layout: cdc_layout()
            }
            .into_parts(CopyFormat::Text)
            .is_err()
        );
    }

    #[test]
    fn copy_layout_preserves_nullability_collation_and_physical_order() {
        require_copy_layout(&cdc_layout(), &cdc_layout()).unwrap();
        for change in 0..4 {
            let mut actual = cdc_layout();
            match change {
                0 => actual[1].not_null = true,
                1 => actual[1].collation_schema = "pg_catalog".into(),
                2 => actual[1].collation = "C".into(),
                _ => actual.reverse(),
            }
            assert!(require_copy_layout(&cdc_layout(), &actual).is_err());
        }
    }

    #[test]
    fn copy_cdc_fk_policy_refuses_without_inventing_workset_membership() {
        assert!(TargetForeignKeys::Refuse.workset().is_err());
        let real = BTreeSet::from([17, 29]);
        assert!(std::ptr::eq(
            TargetForeignKeys::CopyWorkset(&real).workset().unwrap(),
            &real
        ));
    }

    #[test]
    fn source_identity_preserves_full_signed_system_identifier_bits() {
        for (sql, expected) in [
            ("1", 1),
            ("9223372036854775807", 9_223_372_036_854_775_807),
            ("-9223372036854775808", 9_223_372_036_854_775_808),
            ("-1", u64::MAX),
        ] {
            assert_eq!(
                source_identity_fields([sql, "23", "41", "160015"]).unwrap(),
                (
                    SourceId {
                        system_identifier: expected,
                        database_oid: 23
                    },
                    41
                )
            );
        }
    }

    #[test]
    fn source_identity_rejects_invalid_or_zero_system_identifier() {
        for value in [
            "",
            "0",
            "+1",
            "1 ",
            "9223372036854775808",
            "-9223372036854775809",
            "invalid",
        ] {
            assert_eq!(
                source_identity_fields([value, "23", "41", "160015"])
                    .unwrap_err()
                    .kind,
                ErrorKind::BadConfig
            );
        }
    }

    #[test]
    fn source_identity_preserves_full_oid_and_positive_pid_ranges() {
        assert_eq!(
            source_identity_fields(["7", "4294967295", "2147483647", "180006"]).unwrap(),
            (
                SourceId {
                    system_identifier: 7,
                    database_oid: u32::MAX
                },
                i32::MAX as u32
            )
        );
        assert_eq!(
            source_identity_fields(["7", "1", "1", "160000"]).unwrap().1,
            1
        );
    }

    #[test]
    fn source_identity_rejects_invalid_database_oids() {
        for oid in ["", "0", "-1", "+1", "1 ", "4294967296", "invalid"] {
            assert!(source_identity_fields(["7", oid, "41", "160015"]).is_err());
        }
    }

    #[test]
    fn source_identity_rejects_invalid_or_nonpositive_backend_pids() {
        for pid in [
            "",
            "0",
            "-1",
            "+1",
            "1 ",
            "2147483648",
            "4294967295",
            "invalid",
        ] {
            assert!(source_identity_fields(["7", "23", pid, "160015"]).is_err());
        }
    }

    #[test]
    fn source_identity_requires_the_qualified_server_profile() {
        for version in ["160000", "160015", "180000", "180006"] {
            assert!(source_identity_fields(["7", "23", "41", version]).is_ok());
        }
        for version in [
            "",
            "0",
            "90624",
            "130000",
            "150000",
            "140000",
            "170000",
            "14.24",
            "4294967296",
        ] {
            assert!(source_identity_fields(["7", "23", "41", version]).is_err());
        }
    }

    fn verified_source_event() -> Event {
        let mut event = cdc_target_event();
        let Event::Relation { key, .. } = &mut event else {
            unreachable!()
        };
        key.generation = 7;
        event
    }

    #[test]
    fn verified_source_requires_idle_ordinary_sql() {
        require_cdc_source_idle(TransactionStatus::Idle, false).unwrap();
        for status in [
            TransactionStatus::Active,
            TransactionStatus::InTransaction,
            TransactionStatus::InError,
            TransactionStatus::Unknown,
        ] {
            assert!(require_cdc_source_idle(status, false).is_err());
        }
        assert!(require_cdc_source_idle(TransactionStatus::Idle, true).is_err());
    }

    #[test]
    fn verified_source_requires_exact_identity_and_positive_pinned_backend() {
        let source = SourceId {
            system_identifier: u64::MAX,
            database_oid: u32::MAX,
        };
        require_cdc_source_identity(source, 41, (source, 41)).unwrap();
        for (system_identifier, database_oid, pid) in [
            (1, u32::MAX, 41),
            (u64::MAX, 1, 41),
            (u64::MAX, u32::MAX, 42),
        ] {
            assert!(
                require_cdc_source_identity(
                    source,
                    41,
                    (
                        SourceId {
                            system_identifier,
                            database_oid
                        },
                        pid
                    )
                )
                .is_err()
            );
        }
        for pid in [0, i32::MAX as u32 + 1, u32::MAX] {
            assert!(require_cdc_source_identity(source, pid, (source, pid)).is_err());
        }
        require_cdc_source_identity(source, i32::MAX as u32, (source, i32::MAX as u32)).unwrap();
    }

    #[test]
    fn verified_source_requires_read_committed_read_only_primary() {
        require_cdc_source_settings("on", "read committed", "f").unwrap();
        for (read_only, isolation, recovery) in [
            ("off", "read committed", "f"),
            ("on", "repeatable read", "f"),
            ("on", "serializable", "f"),
            ("on", "read committed", "t"),
            ("on", "read committed", ""),
        ] {
            assert!(require_cdc_source_settings(read_only, isolation, recovery).is_err());
        }
    }

    #[test]
    fn verified_source_uses_native_catalog_epoch_not_stream_generation() {
        let binding = SourceSlotBinding::new(
            StreamId {
                run: crate::identity::RunId([1; 16]),
                source: SourceId {
                    system_identifier: 2,
                    database_oid: 3,
                },
                generation: 9,
            },
            "catalog_test".into(),
            crate::cdc::contracts::Plugin::Pgoutput,
            7,
        )
        .unwrap();
        assert_ne!(binding.catalog_epoch(), binding.stream().generation);
        let event = verified_source_event();
        let table = cdc_source_table(&event, binding.catalog_epoch()).unwrap();
        assert_eq!(table.schema, "target schema");
        assert_eq!(table.table, "target table");
        assert!(cdc_source_table(&event, binding.stream().generation).is_err());
        assert!(cdc_source_table(&event, 0).is_err());
    }

    #[test]
    fn verified_source_rejects_nonrelation_or_invalid_relation_identity() {
        let mut event = verified_source_event();
        let Event::Relation { key, .. } = &mut event else {
            unreachable!()
        };
        key.oid = 0;
        assert!(cdc_source_table(&event, 7).is_err());
        for name in ["", "bad\0name"] {
            let mut event = verified_source_event();
            let Event::Relation { schema, .. } = &mut event else {
                unreachable!()
            };
            *schema = name.into();
            assert!(cdc_source_table(&event, 7).is_err());
        }
        let row = Event::Insert {
            relation: RelationKey {
                oid: 71,
                generation: 7,
            },
            values: vec![],
        };
        assert!(cdc_source_table(&row, 7).is_err());
    }

    #[test]
    fn verified_source_match_requires_source_oid_and_epoch() {
        let expected = verified_source_event();
        require_cdc_source_match(&expected, &expected).unwrap();
        for oid in [false, true] {
            let mut actual = expected.clone();
            let Event::Relation { key, .. } = &mut actual else {
                unreachable!()
            };
            if oid {
                key.oid += 1;
            } else {
                key.generation += 1;
            }
            assert!(
                require_cdc_source_match(&expected, &actual).is_err(),
                "accepted different source identity: {actual:?}"
            );
        }
    }

    #[test]
    fn verified_source_match_requires_all_ordered_metadata() {
        let expected = verified_source_event();
        for field in 0..10 {
            let mut actual = expected.clone();
            let Event::Relation {
                schema,
                name,
                columns,
                replica_identity,
                ..
            } = &mut actual
            else {
                unreachable!()
            };
            match field {
                0 => *schema = "other schema".into(),
                1 => *name = "other name".into(),
                2 => columns.swap(0, 2),
                3 => columns[0].name = "other column".into(),
                4 => columns[0].type_oid = 20,
                5 => columns[0].type_mod = 12,
                6 => columns[0].key = false,
                7 => *replica_identity = b'f',
                8 => {
                    columns.pop();
                }
                9 => columns.push(columns[0].clone()),
                _ => unreachable!(),
            }
            assert!(
                require_cdc_source_match(&expected, &actual).is_err(),
                "field {field}"
            );
        }
        let row = Event::Insert {
            relation: RelationKey {
                oid: 71,
                generation: 7,
            },
            values: vec![],
        };
        assert!(require_cdc_source_match(&expected, &row).is_err());
        assert!(require_cdc_source_match(&row, &expected).is_err());
        assert!(require_cdc_source_match(&row, &row).is_err());
    }

    fn source_identity_connection(admin: bool, io: &IoControl) -> PgConnection {
        std::env::var_os("RPGMIGRATE_TEST_CONTEXT")
            .expect("owned source catalog harness is required");
        let variable = if admin {
            "RPGMIGRATE_TEST_SOURCE_CATALOG_ADMIN"
        } else {
            "RPGMIGRATE_TEST_SOURCE_CATALOG_SOURCE"
        };
        let info = Conninfo::new(
            std::env::var_os(variable).expect("owned source catalog connection is required"),
        )
        .unwrap();
        PgConnection::connect(&info, io).unwrap()
    }

    #[test]
    #[ignore = "requires the owned source catalog PostgreSQL harness"]
    fn live_source_identity_exact_backend() {
        let io = cdc_control();
        let mut source = source_identity_connection(false, &io);
        source.query("SET DateStyle = 'SQL, DMY'", &io).unwrap();
        let expected = source
            .query(
                "SELECT (SELECT system_identifier FROM pg_catalog.pg_control_system()),
                        (SELECT oid FROM pg_catalog.pg_database
                         WHERE datname = pg_catalog.current_database()),
                        pg_catalog.pg_backend_pid(), NOT r.rolsuper
                 FROM pg_catalog.pg_roles r WHERE r.rolname = CURRENT_USER",
                &io,
            )
            .unwrap();
        assert_eq!(expected.rows(), 1);
        assert_eq!(text(&expected, 0, 3).unwrap(), "t");
        let identity = SourceId {
            system_identifier: text(&expected, 0, 0).unwrap().parse::<i64>().unwrap() as u64,
            database_oid: text(&expected, 0, 1).unwrap().parse().unwrap(),
        };
        let pid = text(&expected, 0, 2).unwrap().parse::<u32>().unwrap();
        let path = setting(&mut source, "search_path", &io).unwrap();
        let style = setting(&mut source, "DateStyle", &io).unwrap();
        assert_eq!(
            source.transaction_status().unwrap(),
            TransactionStatus::Idle
        );
        assert_eq!(
            read_source_identity(&mut source, &io).unwrap(),
            (identity, pid)
        );
        assert_eq!(
            source.transaction_status().unwrap(),
            TransactionStatus::Idle
        );
        assert_eq!(setting(&mut source, "search_path", &io).unwrap(), path);
        assert_eq!(setting(&mut source, "DateStyle", &io).unwrap(), style);
        let actual = source
            .query("SELECT pg_catalog.pg_backend_pid()", &io)
            .unwrap();
        assert_eq!(text(&actual, 0, 0).unwrap().parse::<u32>().unwrap(), pid);
    }

    #[test]
    #[ignore = "requires the owned source catalog PostgreSQL harness"]
    fn live_source_identity_preserves_imported_snapshot() {
        let io = cdc_control();
        let mut writer = source_identity_connection(false, &io);
        let (schema, quoted_schema) = cdc_schema(&mut writer, "source_identity_snapshot", &io);
        cdc_create(
            &mut writer,
            &schema,
            "canary",
            "id integer PRIMARY KEY",
            &io,
        );
        let table = qualified(&mut writer, &schema, "canary").unwrap();
        writer
            .query(&format!("INSERT INTO {table} VALUES (1)"), &io)
            .unwrap();
        command(&mut writer, "COMMIT", "COMMIT", &io).unwrap();

        let mut exporter = source_identity_connection(false, &io);
        command(
            &mut exporter,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            &io,
        )
        .unwrap();
        let exported = exporter
            .query("SELECT pg_catalog.pg_export_snapshot()", &io)
            .unwrap();
        let token = text(&exported, 0, 0).unwrap();
        assert!(!token.is_empty() && token.len() <= crate::contracts::MAX_SNAPSHOT_TOKEN_BYTES);

        let mut importer = source_identity_connection(false, &io);
        importer
            .query(
                &format!("SET search_path TO {quoted_schema}, pg_catalog"),
                &io,
            )
            .unwrap();
        importer.query("SET DateStyle = 'SQL, DMY'", &io).unwrap();
        command(
            &mut importer,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            &io,
        )
        .unwrap();
        let literal = importer.quote_literal(token).unwrap();
        importer
            .query(&format!("SET TRANSACTION SNAPSHOT {literal}"), &io)
            .unwrap();
        command(
            &mut importer,
            &format!("LOCK TABLE ONLY {table} IN ACCESS SHARE MODE"),
            "LOCK TABLE",
            &io,
        )
        .unwrap();
        let before = read_source_identity(&mut importer, &io).unwrap();
        let path = setting(&mut importer, "search_path", &io).unwrap();
        let style = setting(&mut importer, "DateStyle", &io).unwrap();
        writer
            .query(&format!("INSERT INTO {table} VALUES (2)"), &io)
            .unwrap();
        let count = importer
            .query(&format!("SELECT count(*) FROM {table}"), &io)
            .unwrap();
        assert_eq!(text(&count, 0, 0).unwrap(), "1");

        assert_eq!(read_source_identity(&mut importer, &io).unwrap(), before);
        assert_eq!(
            importer.transaction_status().unwrap(),
            TransactionStatus::InTransaction
        );
        assert_eq!(
            setting(&mut importer, "transaction_isolation", &io).unwrap(),
            "repeatable read"
        );
        assert_eq!(
            setting(&mut importer, "transaction_read_only", &io).unwrap(),
            "on"
        );
        assert_eq!(setting(&mut importer, "search_path", &io).unwrap(), path);
        assert_eq!(setting(&mut importer, "DateStyle", &io).unwrap(), style);
        let count = importer
            .query(&format!("SELECT count(*) FROM {table}"), &io)
            .unwrap();
        assert_eq!(text(&count, 0, 0).unwrap(), "1");
        writer.query("SET lock_timeout = '100ms'", &io).unwrap();
        let blocked = writer
            .query(
                &format!("ALTER TABLE {table} ADD COLUMN released integer"),
                &io,
            )
            .err()
            .unwrap();
        assert!(blocked.message.contains("55P03"), "{blocked:?}");

        command(&mut importer, "ROLLBACK", "ROLLBACK", &io).unwrap();
        command(&mut exporter, "ROLLBACK", "ROLLBACK", &io).unwrap();
        writer
            .query(
                &format!("ALTER TABLE {table} ADD COLUMN released integer"),
                &io,
            )
            .unwrap();
        let count = importer
            .query(&format!("SELECT count(*) FROM {table}"), &io)
            .unwrap();
        assert_eq!(text(&count, 0, 0).unwrap(), "2");
    }

    #[test]
    #[ignore = "requires the owned source catalog PostgreSQL harness"]
    fn live_source_identity_refuses_missing_control_privilege() {
        let io = cdc_control();
        let mut admin = source_identity_connection(true, &io);
        command(&mut admin, "BEGIN", "BEGIN", &io).unwrap();
        admin.query("SET LOCAL ROLE e1_auth", &io).unwrap();
        let role = admin
            .query(
                "SELECT CURRENT_USER = 'e1_auth' AND NOT r.rolsuper
                    AND NOT pg_catalog.has_function_privilege(
                        CURRENT_USER, 'pg_catalog.pg_control_system()', 'EXECUTE')
             FROM pg_catalog.pg_roles r WHERE r.rolname = CURRENT_USER",
                &io,
            )
            .unwrap();
        assert_eq!(role.rows(), 1);
        assert_eq!(text(&role, 0, 0).unwrap(), "t");
        let denied = read_source_identity(&mut admin, &io).unwrap_err();
        assert_eq!(denied.kind, ErrorKind::Pgsql);
        assert!(denied.message.contains("42501"), "{denied:?}");
        assert_eq!(
            admin.transaction_status().unwrap(),
            TransactionStatus::InError
        );
        command(&mut admin, "ROLLBACK", "ROLLBACK", &io).unwrap();
        assert_eq!(admin.transaction_status().unwrap(), TransactionStatus::Idle);
    }

    fn cdc_target_event() -> Event {
        Event::Relation {
            key: RelationKey {
                oid: 71,
                generation: 9,
            },
            schema: "target schema".into(),
            name: "target table".into(),
            columns: vec![
                CdcColumn {
                    name: "first key".into(),
                    type_oid: 23,
                    type_mod: -1,
                    key: true,
                },
                CdcColumn {
                    name: "payload".into(),
                    type_oid: 1043,
                    type_mod: 16,
                    key: false,
                },
                CdcColumn {
                    name: "last key".into(),
                    type_oid: 20,
                    type_mod: -1,
                    key: true,
                },
            ],
            replica_identity: b'd',
        }
    }

    fn cdc_target_relation() -> Relation {
        Relation {
            oid: 83,
            schema: "target schema".into(),
            table: "target table".into(),
            supported: true,
        }
    }

    #[test]
    fn cdc_target_requires_active_transaction() {
        require_cdc_target_transaction(TransactionStatus::InTransaction).unwrap();
        for state in [
            TransactionStatus::Idle,
            TransactionStatus::Active,
            TransactionStatus::InError,
            TransactionStatus::Unknown,
        ] {
            assert_eq!(
                require_cdc_target_transaction(state).unwrap_err().kind,
                ErrorKind::BadConfig
            );
        }
    }

    #[test]
    fn cdc_target_requires_writable_read_committed_origin() {
        require_cdc_target_settings("off", "read committed", "origin").unwrap();
        for (read_only, isolation, role) in [
            ("on", "read committed", "origin"),
            ("off", "repeatable read", "origin"),
            ("off", "serializable", "origin"),
            ("off", "read committed", "replica"),
            ("off", "read committed", "local"),
            ("", "read committed", "origin"),
        ] {
            assert_eq!(
                require_cdc_target_settings(read_only, isolation, role)
                    .unwrap_err()
                    .kind,
                ErrorKind::BadConfig
            );
        }
    }

    #[test]
    fn cdc_target_expected_accepts_bounded_default_pk_metadata() {
        let expected = cdc_target_event();
        assert_eq!(
            cdc_target_expected(&expected).unwrap(),
            ("target schema", "target table")
        );
    }

    #[test]
    fn cdc_target_expected_rejects_nonrelation_events() {
        let relation = RelationKey {
            oid: 71,
            generation: 9,
        };
        for event in [
            Event::Insert {
                relation,
                values: vec![],
            },
            Event::Update {
                relation,
                old: None,
                new: vec![],
            },
            Event::Delete {
                relation,
                old: crate::cdc::contracts::OldTuple {
                    kind: crate::cdc::contracts::OldTupleKind::Key,
                    values: vec![],
                },
            },
            Event::Truncate {
                relations: vec![relation],
                cascade: false,
                restart_identity: false,
            },
        ] {
            assert!(cdc_target_expected(&event).is_err());
        }
    }

    #[test]
    fn cdc_target_expected_rejects_invalid_relation_names() {
        for (schema, name) in [
            ("", "table"),
            ("schema", ""),
            ("bad\0schema", "table"),
            ("schema", "bad\0table"),
        ] {
            let mut expected = cdc_target_event();
            let Event::Relation {
                schema: actual_schema,
                name: actual_name,
                ..
            } = &mut expected
            else {
                unreachable!()
            };
            *actual_schema = schema.into();
            *actual_name = name.into();
            assert!(cdc_target_expected(&expected).is_err());
        }
    }

    #[test]
    fn cdc_target_expected_rejects_invalid_column_layouts() {
        for case in 0..8 {
            let mut expected = cdc_target_event();
            let Event::Relation { columns, .. } = &mut expected else {
                unreachable!()
            };
            match case {
                0 => columns.clear(),
                1 => columns.resize(1601, columns[0].clone()),
                2 => columns[1].name = columns[0].name.clone(),
                3 => columns[0].name.clear(),
                4 => columns[0].name = "bad\0column".into(),
                5 => columns[0].type_oid = 0,
                6 => columns[0].type_oid = FIRST_NORMAL_OBJECT_ID,
                7 => columns.iter_mut().for_each(|column| column.key = false),
                _ => unreachable!(),
            }
            assert!(cdc_target_expected(&expected).is_err(), "case {case}");
        }
    }

    #[test]
    fn cdc_target_expected_rejects_nondefault_identity() {
        for identity in [b'f', b'n', b'i', 0] {
            let mut expected = cdc_target_event();
            let Event::Relation {
                replica_identity, ..
            } = &mut expected
            else {
                unreachable!()
            };
            *replica_identity = identity;
            assert!(cdc_target_expected(&expected).is_err());
        }
    }

    #[test]
    fn cdc_target_expected_enforces_catalog_byte_budget() {
        let mut expected = cdc_target_event();
        let Event::Relation { columns, .. } = &mut expected else {
            unreachable!()
        };
        columns[0].name = "x".repeat(CATALOG_BYTES);
        assert!(cdc_target_expected(&expected).is_err());
    }

    #[test]
    fn cdc_target_match_ignores_endpoint_oid_and_generation() {
        let expected = cdc_target_event();
        let mut actual = expected.clone();
        let Event::Relation { key, .. } = &mut actual else {
            unreachable!()
        };
        *key = RelationKey {
            oid: 99_001,
            generation: u64::MAX,
        };
        require_cdc_target_match(&expected, &actual).unwrap();
    }

    #[test]
    fn cdc_target_match_requires_qualified_name() {
        let expected = cdc_target_event();
        for field in 0..2 {
            let mut actual = expected.clone();
            let Event::Relation { schema, name, .. } = &mut actual else {
                unreachable!()
            };
            if field == 0 {
                *schema = "other schema".into();
            } else {
                *name = "other table".into();
            }
            assert!(require_cdc_target_match(&expected, &actual).is_err());
        }
    }

    #[test]
    fn cdc_target_match_requires_physical_column_order() {
        let expected = cdc_target_event();
        let mut actual = expected.clone();
        let Event::Relation { columns, .. } = &mut actual else {
            unreachable!()
        };
        columns.swap(0, 2);
        assert!(require_cdc_target_match(&expected, &actual).is_err());
    }

    #[test]
    fn cdc_target_match_requires_every_column_field_and_identity() {
        let expected = cdc_target_event();
        for field in 0..6 {
            let mut actual = expected.clone();
            let Event::Relation {
                columns,
                replica_identity,
                ..
            } = &mut actual
            else {
                unreachable!()
            };
            match field {
                0 => columns[0].name = "other column".into(),
                1 => columns[0].type_oid = 20,
                2 => columns[0].type_mod = 12,
                3 => columns[0].key = false,
                4 => {
                    columns.pop();
                }
                5 => *replica_identity = b'f',
                _ => unreachable!(),
            }
            assert!(
                require_cdc_target_match(&expected, &actual).is_err(),
                "field {field}"
            );
        }
    }

    #[test]
    fn cdc_target_match_rejects_nonrelation_metadata() {
        let expected = cdc_target_event();
        let Event::Relation { key, .. } = &expected else {
            unreachable!()
        };
        let row = Event::Insert {
            relation: *key,
            values: vec![],
        };
        assert!(require_cdc_target_match(&expected, &row).is_err());
        assert!(require_cdc_target_match(&row, &expected).is_err());
    }

    #[test]
    fn cdc_target_locked_identity_accepts_same_relation() {
        require_cdc_target_identity(&cdc_target_relation(), &cdc_target_relation()).unwrap();
    }

    #[test]
    fn cdc_target_locked_identity_rejects_replacement_or_unsupported_relation() {
        let before = cdc_target_relation();
        for field in 0..4 {
            let mut after = cdc_target_relation();
            match field {
                0 => after.oid += 1,
                1 => after.schema = "renamed schema".into(),
                2 => after.table = "renamed table".into(),
                3 => after.supported = false,
                _ => unreachable!(),
            }
            assert!(require_cdc_target_identity(&before, &after).is_err());
        }
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_clone_admission_selects_each_heap_once_and_omits_generated_input() {
        let io = IoControl {
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let mut connection = cdc_target_connection(true, &io);
        command(&mut connection, "BEGIN", "BEGIN", &io).unwrap();
        let schema = format!("clone_admission_{}", std::process::id());
        let q = connection.quote_identifier(&schema).unwrap();
        for sql in [
            format!("CREATE SCHEMA {q}"),
            format!("CREATE TYPE {q}.mood AS ENUM ('quiet','busy')"),
            format!("CREATE DOMAIN {q}.positive AS integer CHECK (VALUE>0)"),
            format!(
                "CREATE TABLE {q}.parent (id integer GENERATED ALWAYS AS IDENTITY, number {q}.positive, moods {q}.mood[], computed integer GENERATED ALWAYS AS (id+1) STORED)"
            ),
            format!("CREATE TABLE {q}.child () INHERITS ({q}.parent)"),
            format!("CREATE TABLE {q}.root (id integer, items integer[]) PARTITION BY RANGE (id)"),
            format!("CREATE TABLE {q}.leaf PARTITION OF {q}.root FOR VALUES FROM (0) TO (100)"),
            format!("CREATE INDEX ON {q}.parent (number)"),
            format!(
                "CREATE FUNCTION {q}.trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$"
            ),
            format!(
                "CREATE TRIGGER normal_trigger BEFORE INSERT ON {q}.parent FOR EACH ROW EXECUTE FUNCTION {q}.trigger_fn()"
            ),
        ] {
            connection.query(&sql, &io).unwrap();
        }
        let tables = inspect_clone_tables(&mut connection, &[], &io)
            .unwrap()
            .into_iter()
            .filter(|t| t.name.schema == schema)
            .collect::<Vec<_>>();
        assert_eq!(
            tables
                .iter()
                .map(|t| t.name.table.as_str())
                .collect::<Vec<_>>(),
            ["child", "leaf", "parent"]
        );
        for table in tables.iter().filter(|t| t.name.table != "leaf") {
            assert_eq!(table.writable_columns, ["id", "number", "moods"]);
            assert_eq!(table.all_columns, ["id", "number", "moods", "computed"]);
        }
        command(&mut connection, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires owned PostgreSQL with private German and US monetary locales"]
    fn live_money_clone_preserves_values_and_defaults_across_role_locales() {
        use crate::contracts::{
            CloneOptions, CopyGeneration, RestorePolicy, StateAction, StateRequest,
        };
        let context = std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned harness required");
        let context: serde_json::Value =
            serde_json::from_slice(&std::fs::read(context).unwrap()).unwrap();
        let root = std::path::PathBuf::from(context["root"].as_str().unwrap());
        let token = context["token"].as_str().unwrap();
        assert_eq!(
            std::fs::read_to_string(root.join(".e1-owner")).unwrap(),
            token
        );
        let io = IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )));
        let source_admin = Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN").expect("owned source required"),
        )
        .unwrap();
        let target_admin = Conninfo::new(
            std::env::var_os("RPGMIGRATE_TEST_CLONE_TARGET_ADMIN").expect("owned target required"),
        )
        .unwrap();
        let role_info = |info: &Conninfo, role: &str| {
            Conninfo::new(
                format!(
                    "{} user={role}",
                    std::str::from_utf8(info.as_c_str().to_bytes()).unwrap()
                )
                .into(),
            )
            .unwrap()
        };
        let source_info = role_info(&source_admin, "e1_source");
        let target_info = role_info(&target_admin, "e1_target");
        for (info, role, locale) in [
            (&source_admin, "e1_source", "de_DE.UTF-8"),
            (&target_admin, "e1_target", "en_US.UTF-8"),
        ] {
            let mut admin = PgConnection::connect(info, &io).unwrap();
            let actual = admin.query("SHOW cluster_name", &io).unwrap();
            assert_eq!(actual.cell(0, 0).unwrap(), Some(token.as_bytes()));
            admin
                .query(
                    &format!("ALTER ROLE {role} SET lc_monetary TO '{locale}'"),
                    &io,
                )
                .unwrap();
            admin.query(&format!("GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system(),pg_catalog.pg_control_checkpoint() TO {role}"),&io).unwrap();
        }
        let mut source = PgConnection::connect(&source_info, &io).unwrap();
        let mut target = PgConnection::connect(&target_info, &io).unwrap();
        for (connection, locale) in [(&mut source, "de_DE.UTF-8"), (&mut target, "en_US.UTF-8")] {
            require_setting(connection, "lc_monetary", locale, &io).unwrap();
            let role=connection.query("SELECT NOT rolsuper AND NOT rolreplication AND NOT rolcreatedb AND NOT rolcreaterole FROM pg_catalog.pg_roles WHERE rolname=CURRENT_USER",&io).unwrap();
            assert_eq!(role.cell(0, 0).unwrap(), Some(b"t".as_slice()));
            command(connection, "SET lc_monetary TO 'C'", "SET", &io).unwrap();
            require_setting(connection, "lc_monetary", "C", &io).unwrap();
            command(connection, "RESET lc_monetary", "RESET", &io).unwrap();
            require_setting(connection, "lc_monetary", locale, &io).unwrap();
        }
        let defaults=source.query("SELECT (1234.56::pg_catalog.numeric)::pg_catalog.money::pg_catalog.text,ARRAY[(1234.56::pg_catalog.numeric)::pg_catalog.money,NULL]::pg_catalog.text",&io).unwrap();
        let english = target
            .query(
                "SELECT (1234.56::pg_catalog.numeric)::pg_catalog.money::pg_catalog.text",
                &io,
            )
            .unwrap();
        assert_ne!(
            defaults.cell(0, 0).unwrap(),
            english.cell(0, 0).unwrap(),
            "monetary locale contrast is required"
        );
        let money = source
            .quote_literal(text(&defaults, 0, 0).unwrap())
            .unwrap();
        let array = source
            .quote_literal(text(&defaults, 0, 1).unwrap())
            .unwrap();
        source.query(&format!("CREATE TABLE public.money_values(id integer PRIMARY KEY,amount pg_catalog.money DEFAULT {money}::pg_catalog.money,amounts pg_catalog.money[] DEFAULT {array}::pg_catalog.money[])"),&io).unwrap();
        source.query("INSERT INTO public.money_values VALUES(1,DEFAULT,DEFAULT),(2,(-42.01::pg_catalog.numeric)::pg_catalog.money,ARRAY[(-42.01::pg_catalog.numeric)::pg_catalog.money,NULL]),(3,NULL,NULL)",&io).unwrap();
        let mut snapshot =
            crate::snapshot::CloneSnapshot::export(source, CopyGeneration(1), &io).unwrap();
        let policy = RestorePolicy {
            no_owner: true,
            no_acl: true,
            ..RestorePolicy::default()
        };
        let plan = plan_clone(
            snapshot.connection().unwrap(),
            &mut target,
            CopyGeneration(1),
            policy,
            &[],
            &[],
            &io,
        )
        .unwrap();
        let options = CloneOptions {
            source: source_info,
            target: target_info,
            state: StateRequest {
                path: root.join("money-clone-state"),
                action: StateAction::Create,
            },
            operator: None,
            table_jobs: 2,
            post_data_jobs: 4,
            policy,
            not_consistent: false,
        };
        let mut state = crate::state::open_clone(&options.state, None, &io).unwrap();
        state.bind_clone(&plan, snapshot.ticket()).unwrap();
        crate::schema::preflight(&options, &plan, &io).unwrap();
        let archive = crate::schema::dump(&options, snapshot.ticket(), &state, &io).unwrap();
        let mut authority = crate::apply::CloneAuthority::acquire(
            PgConnection::connect(&options.target, &io).unwrap(),
            &mut state,
            &plan,
            &io,
        )
        .unwrap();
        let objects =
            crate::schema::restore_pre(&options, &plan, &archive, &mut state, &io).unwrap();
        authority
            .mark_stage(crate::state::ClonePhase::PreDataPending, &io)
            .unwrap();
        let tables = reconcile_clone_tables(&mut target, &plan, &objects, &io).unwrap();
        state.confirm_pre_data(objects, tables).unwrap();
        authority.bind_tables(&state, &io).unwrap();
        let copied = crate::copy::run_tables(
            &options,
            &plan,
            &mut snapshot,
            &mut state,
            &mut authority,
            &io,
        )
        .unwrap();
        assert_eq!((copied.tables, copied.rows), (1, 3));
        crate::schema::restore_post(&options, &archive, &io).unwrap();
        require_setting(snapshot.connection().unwrap(), "lc_monetary", "C", &io).unwrap();
        require_setting(&mut target, "lc_monetary", "C", &io).unwrap();
        let rows=target.query("SELECT id,amount::pg_catalog.numeric,amounts[1]::pg_catalog.numeric,amounts[2] IS NULL,amounts IS NULL FROM public.money_values ORDER BY id",&io).unwrap();
        assert_eq!(rows.rows(), 3);
        for (row, amount) in [
            (0, Some(b"1234.56".as_slice())),
            (1, Some(b"-42.01".as_slice())),
            (2, None),
        ] {
            assert_eq!(rows.cell(row, 1).unwrap(), amount);
            assert_eq!(rows.cell(row, 2).unwrap(), amount);
            assert_eq!(rows.cell(row, 3).unwrap(), Some(b"t".as_slice()));
        }
        assert_eq!(rows.cell(2, 4).unwrap(), Some(b"t".as_slice()));
        target
            .query("INSERT INTO public.money_values(id) VALUES(4)", &io)
            .unwrap();
        let restored_default=target.query("SELECT amount::pg_catalog.numeric,amounts[1]::pg_catalog.numeric,amounts[2] IS NULL FROM public.money_values WHERE id=4",&io).unwrap();
        assert_eq!(
            restored_default.cell(0, 0).unwrap(),
            Some(b"1234.56".as_slice())
        );
        assert_eq!(
            restored_default.cell(0, 1).unwrap(),
            Some(b"1234.56".as_slice())
        );
        assert_eq!(restored_default.cell(0, 2).unwrap(), Some(b"t".as_slice()));
        snapshot.finish(&io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_clone_admission_refuses_unsupported_relations_and_trigger_states() {
        let io = IoControl {
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(30),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let mut connection = cdc_target_connection(true, &io);
        let cases: &[(&[&str], &str)] = &[
            (
                &["CREATE UNLOGGED TABLE {s}.item(id integer)"],
                "clone refuses",
            ),
            (
                &["CREATE MATERIALIZED VIEW {s}.item AS SELECT 1 AS id"],
                "clone refuses",
            ),
            (
                &[
                    "CREATE FOREIGN DATA WRAPPER clone_fdw",
                    "CREATE SERVER clone_srv FOREIGN DATA WRAPPER clone_fdw",
                    "CREATE FOREIGN TABLE {s}.item(id integer) SERVER clone_srv",
                ],
                "clone refuses",
            ),
            (
                &[
                    "CREATE TABLE {s}.item(id integer)",
                    "CREATE FUNCTION {s}.trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$",
                    "CREATE TRIGGER test BEFORE INSERT ON {s}.item FOR EACH ROW EXECUTE FUNCTION {s}.trigger_fn()",
                    "ALTER TABLE {s}.item ENABLE ALWAYS TRIGGER test",
                ],
                "clone refuses",
            ),
            (
                &[
                    "CREATE TABLE {s}.item(id integer)",
                    "ALTER EXTENSION plpgsql ADD TABLE {s}.item",
                ],
                "clone refuses",
            ),
            (
                &["CREATE TABLE {s}.item(computed integer GENERATED ALWAYS AS (1) STORED)"],
                "at least one writable column",
            ),
        ];
        for (index, (definitions, expected)) in cases.iter().enumerate() {
            command(&mut connection, "BEGIN", "BEGIN", &io).unwrap();
            let q = connection
                .quote_identifier(&format!("clone_refusal_{}_{index}", std::process::id()))
                .unwrap();
            connection
                .query(&format!("CREATE SCHEMA {q}"), &io)
                .unwrap();
            for definition in *definitions {
                connection
                    .query(&definition.replace("{s}", &q), &io)
                    .unwrap();
            }
            let error = inspect_clone_tables(&mut connection, &[], &io).unwrap_err();
            assert!(error.message.contains(expected));
            command(&mut connection, "ROLLBACK", "ROLLBACK", &io).unwrap();
        }
    }

    fn cdc_target_connection(admin: bool, io: &IoControl) -> PgConnection {
        std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned catalog harness is required");
        let variable = if admin {
            "RPGMIGRATE_TEST_CATALOG_TARGET_ADMIN"
        } else {
            "RPGMIGRATE_TEST_CATALOG_TARGET"
        };
        let info = Conninfo::new(
            std::env::var_os(variable).expect("owned catalog target connection is required"),
        )
        .unwrap();
        PgConnection::connect(&info, io).unwrap()
    }

    fn cdc_target_fixture(
        admin: bool,
        label: &str,
        io: &IoControl,
    ) -> (PgConnection, String, String) {
        let mut target = cdc_target_connection(admin, io);
        let schema = format!("cdc_target_{label}_{}", std::process::id());
        let quoted = target.quote_identifier(&schema).unwrap();
        command(
            &mut target,
            "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE",
            "BEGIN",
            io,
        )
        .unwrap();
        target
            .query(&format!("CREATE SCHEMA {quoted}"), io)
            .unwrap();
        (target, schema, quoted)
    }

    fn cdc_target_create(
        target: &mut PgConnection,
        schema: &str,
        name: &str,
        io: &IoControl,
    ) -> Event {
        cdc_create(
            target,
            schema,
            name,
            "\"first key\" integer NOT NULL, payload varchar(12), \"last key\" bigint NOT NULL,
             PRIMARY KEY (\"last key\", \"first key\") INCLUDE (payload)",
            io,
        );
        let mut expected = cdc_target_event();
        let Event::Relation {
            schema: event_schema,
            name: event_name,
            ..
        } = &mut expected
        else {
            unreachable!()
        };
        *event_schema = schema.into();
        *event_name = name.into();
        expected
    }

    fn cdc_target_point_lookup_controls(target: &mut PgConnection, schema: &str, io: &IoControl) {
        let indexes = target.query(
            "SELECT pg_catalog.pg_get_indexdef('pg_catalog.pg_class_relname_nsp_index'::pg_catalog.regclass, 1, true),
                    pg_catalog.pg_get_indexdef('pg_catalog.pg_class_relname_nsp_index'::pg_catalog.regclass, 2, true),
                    pg_catalog.pg_get_indexdef('pg_catalog.pg_namespace_nspname_index'::pg_catalog.regclass, 1, true)", io).unwrap();
        for (column, expected) in ["relname", "relnamespace", "nspname"].iter().enumerate() {
            assert_eq!(text(&indexes, 0, column).unwrap(), *expected);
        }
        let mut cases = Vec::new();
        for name in ["a".repeat(63), "MiXeD".into(), "é".into()] {
            let expected = cdc_target_create(target, schema, &name, io);
            preflight_cdc_target(target, &expected, io).unwrap();
            cases.push((schema.to_owned(), name.clone(), true));
            cases.push((schema.to_owned(), format!("{name}x"), false));
            cases.push((schema.to_owned(), name.to_uppercase(), false));
        }
        cases.push((schema.to_owned(), "e\u{301}".into(), false));
        cases.push((schema.to_uppercase(), "MiXeD".into(), false));
        let long_schema = format!("{schema}{}", "s".repeat(63 - schema.len()));
        let quoted = target.quote_identifier(&long_schema).unwrap();
        target
            .query(&format!("CREATE SCHEMA {quoted}"), io)
            .unwrap();
        cdc_target_create(target, &long_schema, "rows", io);
        cases.push((long_schema.clone(), "rows".into(), true));
        cases.push((format!("{long_schema}x"), "rows".into(), false));
        for (schema, name, present) in cases {
            let eligibility = relation_eligibility(true);
            target
                .prepare(
                    "",
                    &format!(
                        "SELECT c.oid, n.nspname, c.relname, {eligibility}
                 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
                 WHERE n.nspname::text = $1 AND c.relname::text = $2
                   AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
                   AND c.relkind IN ('r','p','v','m','f') LIMIT 2"
                    ),
                    &[25, 25],
                    io,
                )
                .unwrap();
            let old = target
                .execute_prepared(
                    "",
                    &[
                        PgParam::Text(schema.as_bytes()),
                        PgParam::Text(name.as_bytes()),
                    ],
                    io,
                )
                .unwrap();
            assert_eq!(old.rows(), usize::from(present));
            let actual = cdc_target_lookup(target, &schema, &name, io);
            if present {
                let actual = actual.unwrap();
                assert_eq!(
                    actual.oid,
                    cdc_object_id(text(&old, 0, 0).unwrap()).unwrap()
                );
                assert_eq!(actual.schema, text(&old, 0, 1).unwrap());
                assert_eq!(actual.table, text(&old, 0, 2).unwrap());
                assert_eq!(actual.supported, old.cell(0, 3).unwrap() == Some(b"t"));
            } else {
                let error = actual.unwrap_err();
                assert_eq!(error.kind, ErrorKind::BadConfig);
                assert!(error.message.contains("missing"));
            }
        }
    }

    fn cdc_target_refuses(
        target: &mut PgConnection,
        expected: &Event,
        reason: &str,
        io: &IoControl,
    ) {
        let error = preflight_cdc_target(target, expected, io).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadConfig, "{error:?}");
        assert!(error.message.contains(reason), "{error:?}");
    }

    fn cdc_target_restricted_privilege_controls(io: &IoControl) {
        let (mut admin, schema, quoted_schema) = cdc_target_fixture(true, "privileges", io);
        admin
            .query(
                &format!("GRANT USAGE ON SCHEMA {quoted_schema} TO e1_auth, e1_source"),
                io,
            )
            .unwrap();
        let membership = admin
            .query(
                "SELECT NOT pg_catalog.pg_has_role('e1_auth', 'e1_source', 'MEMBER')",
                io,
            )
            .unwrap();
        assert_eq!(membership.cell(0, 0).unwrap(), Some(&b"t"[..]));
        let all = "\"first key\", payload, \"last key\"";
        let keys = "\"first key\", \"last key\"";
        for case in 0..7 {
            let name = format!("rights_{case}");
            let expected = cdc_target_create(&mut admin, &schema, &name, io);
            let quoted = qualified(&mut admin, &schema, &name).unwrap();
            let oid = cdc_target_lookup(&mut admin, &schema, &name, io)
                .unwrap()
                .oid;
            if case < 2 {
                let rights = if case == 0 {
                    "SELECT, INSERT, UPDATE"
                } else {
                    "SELECT, INSERT, UPDATE, DELETE"
                };
                admin
                    .query(&format!("GRANT {rights} ON {quoted} TO e1_auth"), io)
                    .unwrap();
            } else {
                let grantee = if case == 6 { "e1_source" } else { "e1_auth" };
                let insert = if case == 3 { keys } else { all };
                let update = if case == 4 { keys } else { all };
                let select = if case == 5 { "\"first key\"" } else { keys };
                admin
                    .query(&format!("GRANT DELETE ON {quoted} TO {grantee}"), io)
                    .unwrap();
                admin.query(&format!("GRANT INSERT ({insert}), UPDATE ({update}), SELECT ({select}) ON {quoted} TO {grantee}"), io).unwrap();
                if case == 6 {
                    admin.query("GRANT e1_source TO e1_auth", io).unwrap();
                }
            }
            admin.query("SET LOCAL ROLE e1_auth", io).unwrap();
            let identity = admin
                .query(
                    &format!(
                        "SELECT CURRENT_USER = 'e1_auth' AND NOT r.rolsuper
                 AND NOT pg_catalog.pg_has_role(c.relowner, 'USAGE')
                 FROM pg_catalog.pg_roles r, pg_catalog.pg_class c
                 WHERE r.rolname = CURRENT_USER AND c.oid = {oid}"
                    ),
                    io,
                )
                .unwrap();
            assert_eq!(identity.rows(), 1);
            assert_eq!(identity.cell(0, 0).unwrap(), Some(&b"t"[..]));
            let preflight = preflight_cdc_target(&mut admin, &expected, io);
            if case == 0 {
                command(&mut admin, "SAVEPOINT permission_probe", "SAVEPOINT", io).unwrap();
                let denied = admin
                    .query(
                        &format!(
                            "DELETE FROM {quoted} WHERE \"first key\" = 1 AND \"last key\" = 3"
                        ),
                        io,
                    )
                    .err()
                    .unwrap();
                assert!(denied.message.contains("42501"), "{denied:?}");
                command(
                    &mut admin,
                    "ROLLBACK TO SAVEPOINT permission_probe",
                    "ROLLBACK",
                    io,
                )
                .unwrap();
            }
            if matches!(case, 0 | 3 | 4 | 5) {
                let error = preflight
                    .expect_err("preflight accepted a role missing required DML privileges");
                assert_eq!(error.kind, ErrorKind::BadConfig);
                assert!(error.message.contains("DML privileges"), "{error:?}");
            } else {
                preflight.unwrap();
                if matches!(case, 2 | 6) {
                    let minimum = admin
                        .query(
                            &format!(
                        "SELECT NOT pg_catalog.has_table_privilege({oid}::oid, 'INSERT')
                         AND NOT pg_catalog.has_table_privilege({oid}::oid, 'UPDATE')
                         AND NOT pg_catalog.has_table_privilege({oid}::oid, 'SELECT')
                         AND NOT pg_catalog.has_column_privilege({oid}::oid, 2::smallint, 'SELECT')"
                    ),
                            io,
                        )
                        .unwrap();
                    assert_eq!(minimum.cell(0, 0).unwrap(), Some(&b"t"[..]));
                }
                command(&mut admin, &format!("INSERT INTO {quoted} (\"first key\", payload, \"last key\") VALUES (1, 'first', 3)"), "INSERT 0 1", io).unwrap();
                command(&mut admin, &format!("UPDATE ONLY {quoted} SET payload = 'changed' WHERE \"first key\" = 1 AND \"last key\" = 3"), "UPDATE 1", io).unwrap();
                command(
                    &mut admin,
                    &format!(
                        "DELETE FROM ONLY {quoted} WHERE \"first key\" = 1 AND \"last key\" = 3"
                    ),
                    "DELETE 1",
                    io,
                )
                .unwrap();
            }
            admin.query("RESET ROLE", io).unwrap();
        }
        command(&mut admin, "ROLLBACK", "ROLLBACK", io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_target_matches_and_retains_lock() {
        let io = cdc_control();
        let (mut target, schema, _) = cdc_target_fixture(false, "lock \"'\n\\", &io);
        let name = "row \"'\n\\";
        let expected = cdc_target_create(&mut target, &schema, name, &io);
        let quoted = qualified(&mut target, &schema, name).unwrap();
        let pid = text(
            &target
                .query("SELECT pg_catalog.pg_backend_pid()", &io)
                .unwrap(),
            0,
            0,
        )
        .unwrap()
        .to_owned();
        command(&mut target, "COMMIT", "COMMIT", &io).unwrap();
        command(
            &mut target,
            "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE",
            "BEGIN",
            &io,
        )
        .unwrap();
        preflight_cdc_target(&mut target, &expected, &io).unwrap();
        assert_eq!(
            target.transaction_status().unwrap(),
            TransactionStatus::InTransaction
        );
        assert_eq!(
            text(
                &target
                    .query("SELECT pg_catalog.pg_backend_pid()", &io)
                    .unwrap(),
                0,
                0
            )
            .unwrap(),
            pid
        );
        assert_eq!(setting(&mut target, "DateStyle", &io).unwrap(), "ISO, YMD");
        let mut writer = cdc_target_connection(false, &io);
        writer.query("SET lock_timeout = '100ms'", &io).unwrap();
        for sql in [
            format!("ALTER TABLE {quoted} ADD COLUMN added integer"),
            format!("INSERT INTO {quoted} VALUES (1, 'payload', 3)"),
        ] {
            let error = writer.query(&sql, &io).err().unwrap();
            assert!(error.message.contains("55P03"), "{error:?}");
        }
        target
            .query(
                &format!("INSERT INTO {quoted} VALUES (2, 'own write', 4)"),
                &io,
            )
            .unwrap();
        command(&mut target, "COMMIT", "COMMIT", &io).unwrap();
        writer
            .query(
                &format!("INSERT INTO {quoted} VALUES (1, 'payload', 3)"),
                &io,
            )
            .unwrap();
        writer
            .query(
                &format!("ALTER TABLE {quoted} ADD COLUMN added integer"),
                &io,
            )
            .unwrap();
        command(
            &mut target,
            "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE",
            "BEGIN",
            &io,
        )
        .unwrap();
        cdc_target_refuses(&mut target, &expected, "does not match", &io);
        command(&mut target, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_target_rejects_metadata_mismatch() {
        let io = cdc_control();
        let (mut target, schema, _) = cdc_target_fixture(false, "mismatch", &io);
        cdc_target_point_lookup_controls(&mut target, &schema, &io);
        let expected = cdc_target_create(&mut target, &schema, "rows", &io);
        preflight_cdc_target(&mut target, &expected, &io).unwrap();
        for field in 0..7 {
            let mut changed = expected.clone();
            let Event::Relation {
                schema,
                name,
                columns,
                ..
            } = &mut changed
            else {
                unreachable!()
            };
            match field {
                0 => *schema = "missing schema".into(),
                1 => *name = "missing table".into(),
                2 => columns[0].name = "other column".into(),
                3 => columns[0].type_oid = 20,
                4 => columns[1].type_mod = 24,
                5 => columns[0].key = false,
                6 => columns.swap(0, 2),
                _ => unreachable!(),
            }
            cdc_target_refuses(
                &mut target,
                &changed,
                if field < 2 {
                    "missing"
                } else {
                    "does not match"
                },
                &io,
            );
        }
        let quoted = qualified(&mut target, &schema, "rows").unwrap();
        target
            .query(&format!("ALTER TABLE {quoted} REPLICA IDENTITY FULL"), &io)
            .unwrap();
        cdc_target_refuses(&mut target, &expected, "DEFAULT replica identity", &io);
        assert_eq!(
            target.transaction_status().unwrap(),
            TransactionStatus::InTransaction
        );
        command(&mut target, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_target_rejects_execution_profile() {
        let io = cdc_control();
        cdc_target_restricted_privilege_controls(&io);
        let (mut target, schema, quoted_schema) = cdc_target_fixture(false, "execution", &io);
        let eligible = cdc_target_create(&mut target, &schema, "eligible", &io);
        preflight_cdc_target(&mut target, &eligible, &io).unwrap();
        cdc_create(
            &mut target,
            &schema,
            "parent",
            "id integer PRIMARY KEY",
            &io,
        );
        for (name, alteration, reason) in [
            ("check", "ADD CHECK (\"first key\" > 0)".into(), "CHECK"),
            (
                "exclusion",
                "ADD EXCLUDE USING btree (\"first key\" WITH =)".into(),
                "exclusion",
            ),
            (
                "outgoing",
                format!(
                    "ADD FOREIGN KEY (\"first key\") REFERENCES {quoted_schema}.parent(id) DEFERRABLE INITIALLY DEFERRED"
                ),
                "foreign keys",
            ),
            ("self_fk", String::new(), "foreign keys"),
            (
                "rls",
                "ENABLE ROW LEVEL SECURITY".into(),
                "logged ordinary heap",
            ),
        ] {
            let expected = cdc_target_create(&mut target, &schema, name, &io);
            let quoted = qualified(&mut target, &schema, name).unwrap();
            let alteration = if name == "self_fk" {
                format!(
                    "ADD FOREIGN KEY (\"first key\", \"last key\") REFERENCES {quoted} (\"first key\", \"last key\") DEFERRABLE INITIALLY DEFERRED"
                )
            } else {
                alteration
            };
            target
                .query(&format!("ALTER TABLE {quoted} {alteration}"), &io)
                .unwrap();
            cdc_target_refuses(&mut target, &expected, reason, &io);
        }
        let incoming = cdc_target_create(&mut target, &schema, "incoming", &io);
        let quoted = qualified(&mut target, &schema, "incoming").unwrap();
        target.query(&format!(
            "CREATE TABLE {quoted_schema}.child (id integer, last_id bigint,
             FOREIGN KEY (id, last_id) REFERENCES {quoted} (\"first key\", \"last key\") ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)"
        ), &io).unwrap();
        cdc_target_refuses(&mut target, &incoming, "foreign keys", &io);
        for (name, index) in [
            ("expression", "((\"first key\" + 1))"),
            ("partial", "(\"first key\") WHERE \"first key\" > 0"),
        ] {
            let expected = cdc_target_create(&mut target, &schema, name, &io);
            let quoted = qualified(&mut target, &schema, name).unwrap();
            target
                .query(
                    &format!("CREATE INDEX index_{name} ON {quoted} {index}"),
                    &io,
                )
                .unwrap();
            cdc_target_refuses(&mut target, &expected, "plain indexes", &io);
        }
        let trigger = cdc_target_create(&mut target, &schema, "trigger", &io);
        let quoted = qualified(&mut target, &schema, "trigger").unwrap();
        target.query(&format!("CREATE FUNCTION {quoted_schema}.noop() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$"), &io).unwrap();
        target.query(&format!("CREATE TRIGGER cdc_hook BEFORE INSERT ON {quoted} FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.noop()"), &io).unwrap();
        cdc_target_refuses(&mut target, &trigger, "unsupported trigger states", &io);
        command(&mut target, "ROLLBACK", "ROLLBACK", &io).unwrap();

        let (mut admin, schema, quoted_schema) = cdc_target_fixture(true, "negative", &io);
        admin.query(&format!(
            "CREATE OPERATOR CLASS {quoted_schema}.custom_int4_ops FOR TYPE integer USING btree AS
             OPERATOR 1 pg_catalog.< (integer,integer), OPERATOR 2 pg_catalog.<= (integer,integer),
             OPERATOR 3 pg_catalog.= (integer,integer), OPERATOR 4 pg_catalog.>= (integer,integer),
             OPERATOR 5 pg_catalog.> (integer,integer), FUNCTION 1 pg_catalog.btint4cmp(integer,integer)"
        ), &io).unwrap();
        let opclass = cdc_target_create(&mut admin, &schema, "opclass", &io);
        let quoted = qualified(&mut admin, &schema, "opclass").unwrap();
        admin.query(&format!("CREATE INDEX custom_index ON {quoted} (\"first key\" {quoted_schema}.custom_int4_ops)"), &io).unwrap();
        cdc_target_refuses(&mut admin, &opclass, "plain indexes", &io);
        let internal = cdc_target_create(&mut admin, &schema, "internal", &io);
        let quoted = qualified(&mut admin, &schema, "internal").unwrap();
        admin.query(&format!("ALTER TABLE {quoted} ADD FOREIGN KEY (\"first key\", \"last key\") REFERENCES {quoted} (\"first key\", \"last key\") DEFERRABLE INITIALLY DEFERRED"), &io).unwrap();
        cdc_target_refuses(&mut admin, &internal, "foreign keys", &io);
        let relation = cdc_target_lookup(&mut admin, &schema, "internal", &io).unwrap();
        let triggers = admin
            .query(
                &format!(
                    "SELECT tgname FROM pg_catalog.pg_trigger WHERE tgrelid = {} AND tgisinternal
             AND tgconstraint <> 0 ORDER BY oid LIMIT 17",
                    relation.oid
                ),
                &io,
            )
            .unwrap();
        assert_eq!(triggers.rows(), 4);
        for (alteration, reason) in [
            ("DISABLE TRIGGER", "unsupported trigger states"),
            ("ENABLE REPLICA TRIGGER", "unsupported trigger states"),
            ("ENABLE ALWAYS TRIGGER", "foreign keys"),
        ] {
            for row in 0..triggers.rows() {
                let name = admin
                    .quote_identifier(text(&triggers, row, 0).unwrap())
                    .unwrap();
                admin
                    .query(&format!("ALTER TABLE {quoted} {alteration} {name}"), &io)
                    .unwrap();
            }
            cdc_target_refuses(&mut admin, &internal, reason, &io);
        }
        command(&mut admin, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_target_requires_owned_transaction_settings() {
        let io = cdc_control();
        let (mut target, schema, _) = cdc_target_fixture(false, "settings", &io);
        let expected = cdc_target_create(&mut target, &schema, "rows", &io);
        preflight_cdc_target(&mut target, &expected, &io).unwrap();
        command(&mut target, "COMMIT", "COMMIT", &io).unwrap();
        cdc_target_refuses(&mut target, &expected, "active transaction", &io);
        assert_eq!(
            target.transaction_status().unwrap(),
            TransactionStatus::Idle
        );
        for begin in [
            "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY",
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE",
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
        ] {
            command(&mut target, begin, "BEGIN", &io).unwrap();
            cdc_target_refuses(&mut target, &expected, "READ COMMITTED READ WRITE", &io);
            assert_eq!(
                target.transaction_status().unwrap(),
                TransactionStatus::InTransaction
            );
            command(&mut target, "ROLLBACK", "ROLLBACK", &io).unwrap();
        }
        let mut admin = cdc_target_connection(true, &io);
        for role in ["replica", "local"] {
            command(
                &mut admin,
                "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE",
                "BEGIN",
                &io,
            )
            .unwrap();
            admin
                .query(&format!("SET LOCAL session_replication_role = {role}"), &io)
                .unwrap();
            cdc_target_refuses(
                &mut admin,
                &expected,
                "session_replication_role = origin",
                &io,
            );
            assert_eq!(
                setting(&mut admin, "session_replication_role", &io).unwrap(),
                role
            );
            command(&mut admin, "ROLLBACK", "ROLLBACK", &io).unwrap();
        }
    }

    fn cdc_control() -> IoControl {
        IoControl {
            deadline: Instant::now() + Duration::from_secs(30),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn cdc_source(admin: bool, io: &IoControl) -> PgConnection {
        std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned catalog harness is required");
        let variable = if admin {
            "RPGMIGRATE_TEST_CATALOG_ADMIN"
        } else {
            "RPGMIGRATE_TEST_CATALOG_SOURCE"
        };
        let info = Conninfo::new(
            std::env::var_os(variable).expect("owned catalog database connection is required"),
        )
        .unwrap();
        PgConnection::connect(&info, io).unwrap()
    }

    fn cdc_schema(source: &mut PgConnection, label: &str, io: &IoControl) -> (String, String) {
        let schema = format!("cdc_{label}_{}", std::process::id());
        let quoted = source.quote_identifier(&schema).unwrap();
        command(source, "BEGIN ISOLATION LEVEL REPEATABLE READ", "BEGIN", io).unwrap();
        source
            .query(&format!("CREATE SCHEMA {quoted}"), io)
            .unwrap();
        (schema, quoted)
    }

    fn cdc_table(schema: &str, table: &str) -> QualifiedTable {
        QualifiedTable {
            schema: schema.into(),
            table: table.into(),
        }
    }

    fn cdc_create(
        source: &mut PgConnection,
        schema: &str,
        name: &str,
        definition: &str,
        io: &IoControl,
    ) -> QualifiedTable {
        let table = cdc_table(schema, name);
        let quoted = qualified(source, schema, name).unwrap();
        source
            .query(&format!("CREATE TABLE {quoted} ({definition})"), io)
            .unwrap();
        table
    }

    fn cdc_refuses(
        source: &mut PgConnection,
        table: &QualifiedTable,
        expected: &str,
        io: &IoControl,
    ) {
        let error = load_cdc_relation(source, table, 9, io).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadConfig, "{error:?}");
        assert!(error.message.contains(expected), "{error:?}");
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_relation_metadata() {
        use crate::pg::PgParam;

        let io = cdc_control();
        let mut source = cdc_source(false, &io);
        let (schema, _) = cdc_schema(&mut source, "metadata \"'\n\\", &io);
        let name = "row \"'\n\\";
        let table = cdc_create(
            &mut source,
            &schema,
            name,
            "\"first key\" integer NOT NULL, payload varchar(12), \"last key\" bigint NOT NULL, raw bytea,
             PRIMARY KEY (\"last key\", \"first key\") INCLUDE (payload)",
            &io,
        );
        source
            .prepare(
                "",
                "SELECT c.oid FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname::text = $1 AND c.relname::text = $2",
                &[25, 25],
                &io,
            )
            .unwrap();
        let oid = source
            .execute_prepared(
                "",
                &[
                    PgParam::Text(schema.as_bytes()),
                    PgParam::Text(name.as_bytes()),
                ],
                &io,
            )
            .unwrap();
        assert_eq!(oid.rows(), 1);
        let oid = text(&oid, 0, 0).unwrap().parse::<u32>().unwrap();
        let expected = Event::Relation {
            key: RelationKey {
                oid,
                generation: u64::MAX,
            },
            schema: schema.clone(),
            name: name.into(),
            columns: vec![
                CdcColumn {
                    name: "first key".into(),
                    type_oid: 23,
                    type_mod: -1,
                    key: true,
                },
                CdcColumn {
                    name: "payload".into(),
                    type_oid: 1043,
                    type_mod: 16,
                    key: false,
                },
                CdcColumn {
                    name: "last key".into(),
                    type_oid: 20,
                    type_mod: -1,
                    key: true,
                },
                CdcColumn {
                    name: "raw".into(),
                    type_oid: 17,
                    type_mod: -1,
                    key: false,
                },
            ],
            replica_identity: b'd',
        };
        assert_eq!(
            load_cdc_relation(&mut source, &table, u64::MAX, &io).unwrap(),
            expected
        );
        let mut expected_zero = expected;
        if let Event::Relation { key, .. } = &mut expected_zero {
            key.generation = 0;
        }
        assert_eq!(
            load_cdc_relation(&mut source, &table, 0, &io).unwrap(),
            expected_zero
        );
        for missing in [
            cdc_table(&schema, "absent"),
            cdc_table("absent schema", name),
        ] {
            cdc_refuses(&mut source, &missing, "relation", &io);
        }
        for invalid in [
            cdc_table("", name),
            cdc_table(&schema, ""),
            cdc_table(&schema, "invalid\0name"),
        ] {
            cdc_refuses(&mut source, &invalid, "name", &io);
        }
        command(&mut source, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_identity_requires_default_primary_key() {
        let io = cdc_control();
        let mut source = cdc_source(false, &io);
        let (schema, _) = cdc_schema(&mut source, "identity", &io);
        let missing = cdc_create(&mut source, &schema, "missing", "id integer NOT NULL", &io);
        cdc_refuses(&mut source, &missing, "primary key", &io);
        let deferred = cdc_create(
            &mut source,
            &schema,
            "deferred",
            "id integer PRIMARY KEY DEFERRABLE INITIALLY DEFERRED",
            &io,
        );
        cdc_refuses(&mut source, &deferred, "primary key", &io);
        for identity in ["FULL", "NOTHING", "USING INDEX identity_index"] {
            let table = cdc_create(
                &mut source,
                &schema,
                identity,
                "id integer PRIMARY KEY, alternate integer NOT NULL",
                &io,
            );
            let quoted = qualified(&mut source, &schema, identity).unwrap();
            if identity.starts_with("USING") {
                source
                    .query(
                        &format!("CREATE UNIQUE INDEX identity_index ON {quoted} (alternate)"),
                        &io,
                    )
                    .unwrap();
            }
            source
                .query(
                    &format!("ALTER TABLE {quoted} REPLICA IDENTITY {identity}"),
                    &io,
                )
                .unwrap();
            cdc_refuses(&mut source, &table, "DEFAULT replica identity", &io);
        }
        command(&mut source, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_rejects_unsupported_columns() {
        let io = cdc_control();
        let mut source = cdc_source(false, &io);
        let (schema, quoted_schema) = cdc_schema(&mut source, "columns", &io);
        source
            .query(
                &format!("CREATE DOMAIN {quoted_schema}.domain_value AS integer"),
                &io,
            )
            .unwrap();
        source
            .query(
                &format!("CREATE TYPE {quoted_schema}.enum_value AS ENUM ('one')"),
                &io,
            )
            .unwrap();
        for (name, definition, expected) in [
            (
                "array",
                "id integer PRIMARY KEY, value integer[]".into(),
                "column type",
            ),
            (
                "money",
                "id integer PRIMARY KEY, value money".into(),
                "column type",
            ),
            (
                "domain",
                format!("id integer PRIMARY KEY, value {quoted_schema}.domain_value"),
                "column type",
            ),
            (
                "enum",
                format!("id integer PRIMARY KEY, value {quoted_schema}.enum_value"),
                "column type",
            ),
            (
                "generated",
                "id integer PRIMARY KEY, value integer GENERATED ALWAYS AS (id + 1) STORED".into(),
                "generated",
            ),
            (
                "identity",
                "id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY".into(),
                "identity",
            ),
            ("empty", String::new(), "nonempty column layout"),
        ] {
            let table = cdc_create(&mut source, &schema, name, &definition, &io);
            cdc_refuses(&mut source, &table, expected, &io);
        }
        let dropped = cdc_create(
            &mut source,
            &schema,
            "dropped",
            "id integer PRIMARY KEY, value text",
            &io,
        );
        let quoted = qualified(&mut source, &schema, "dropped").unwrap();
        source
            .query(&format!("ALTER TABLE {quoted} DROP COLUMN value"), &io)
            .unwrap();
        cdc_refuses(&mut source, &dropped, "dropped", &io);
        command(&mut source, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_rejects_unsupported_relations() {
        let io = cdc_control();
        let mut source = cdc_source(false, &io);
        let (schema, quoted_schema) = cdc_schema(&mut source, "relations", &io);
        for (name, statement) in [
            (
                "unlogged",
                "CREATE UNLOGGED TABLE @ (id integer PRIMARY KEY)",
            ),
            ("view", "CREATE VIEW @ AS SELECT 1 AS id"),
            (
                "materialized",
                "CREATE MATERIALIZED VIEW @ AS SELECT 1 AS id",
            ),
            (
                "partitioned",
                "CREATE TABLE @ (id integer PRIMARY KEY) PARTITION BY RANGE (id)",
            ),
        ] {
            let quoted = qualified(&mut source, &schema, name).unwrap();
            source.query(&statement.replace('@', &quoted), &io).unwrap();
            cdc_refuses(
                &mut source,
                &cdc_table(&schema, name),
                "logged ordinary heap",
                &io,
            );
        }
        let parent = qualified(&mut source, &schema, "parent").unwrap();
        source
            .query(
                &format!("CREATE TABLE {parent} (id integer PRIMARY KEY)"),
                &io,
            )
            .unwrap();
        let child = qualified(&mut source, &schema, "child").unwrap();
        source
            .query(&format!("CREATE TABLE {child} () INHERITS ({parent})"), &io)
            .unwrap();
        for name in ["parent", "child"] {
            cdc_refuses(
                &mut source,
                &cdc_table(&schema, name),
                "logged ordinary heap",
                &io,
            );
        }
        for name in ["rls", "forced_rls", "trigger", "rule"] {
            let table = cdc_create(&mut source, &schema, name, "id integer PRIMARY KEY", &io);
            let quoted = qualified(&mut source, &schema, name).unwrap();
            let sql = match name {
                "rls" => format!("ALTER TABLE {quoted} ENABLE ROW LEVEL SECURITY"),
                "forced_rls" => format!("ALTER TABLE {quoted} FORCE ROW LEVEL SECURITY"),
                "trigger" => {
                    source.query(&format!("CREATE FUNCTION {quoted_schema}.noop() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$"), &io).unwrap();
                    format!(
                        "CREATE TRIGGER cdc_hook BEFORE INSERT ON {quoted} FOR EACH ROW EXECUTE FUNCTION {quoted_schema}.noop()"
                    )
                }
                "rule" => {
                    format!("CREATE RULE cdc_rule AS ON INSERT TO {quoted} DO INSTEAD NOTHING")
                }
                _ => unreachable!(),
            };
            source.query(&sql, &io).unwrap();
            cdc_refuses(&mut source, &table, "logged ordinary heap", &io);
        }
        command(&mut source, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_rejects_unsupported_indexes() {
        let io = cdc_control();
        let mut source = cdc_source(true, &io);
        let (schema, quoted_schema) = cdc_schema(&mut source, "indexes", &io);
        source.query(&format!(
            "CREATE OPERATOR CLASS {quoted_schema}.custom_int4_ops FOR TYPE integer USING btree AS
             OPERATOR 1 pg_catalog.< (integer,integer), OPERATOR 2 pg_catalog.<= (integer,integer),
             OPERATOR 3 pg_catalog.= (integer,integer), OPERATOR 4 pg_catalog.>= (integer,integer),
             OPERATOR 5 pg_catalog.> (integer,integer), FUNCTION 1 pg_catalog.btint4cmp(integer,integer)"
        ), &io).unwrap();
        for (name, index) in [
            ("expression", "((value + 1))".into()),
            ("partial", "(value) WHERE value > 0".into()),
            (
                "opclass",
                format!("(value {quoted_schema}.custom_int4_ops)"),
            ),
        ] {
            let table = cdc_create(
                &mut source,
                &schema,
                name,
                "id integer PRIMARY KEY, value integer",
                &io,
            );
            let quoted = qualified(&mut source, &schema, name).unwrap();
            let index_name = source.quote_identifier(&format!("index_{name}")).unwrap();
            source
                .query(
                    &format!("CREATE INDEX {index_name} ON {quoted} {index}"),
                    &io,
                )
                .unwrap();
            cdc_refuses(&mut source, &table, "plain indexes", &io);
        }
        command(&mut source, "ROLLBACK", "ROLLBACK", &io).unwrap();
    }

    #[test]
    #[ignore = "requires the owned catalog PostgreSQL harness"]
    fn live_cdc_retains_caller_snapshot() {
        let io = cdc_control();
        let mut source = cdc_source(false, &io);
        let mut writer = cdc_source(false, &io);
        let (schema, _) = cdc_schema(&mut source, "snapshot", &io);
        let table = cdc_create(&mut source, &schema, "rows", "id integer PRIMARY KEY", &io);
        let quoted = qualified(&mut source, &schema, "rows").unwrap();
        source
            .query(&format!("INSERT INTO {quoted} VALUES (1)"), &io)
            .unwrap();
        command(&mut source, "COMMIT", "COMMIT", &io).unwrap();
        command(
            &mut source,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            &io,
        )
        .unwrap();
        command(
            &mut source,
            &format!("LOCK TABLE ONLY {quoted} IN ACCESS SHARE MODE"),
            "LOCK TABLE",
            &io,
        )
        .unwrap();
        let relation = load_cdc_relation(&mut source, &table, 7, &io).unwrap();
        assert_eq!(
            setting(&mut source, "transaction_isolation", &io).unwrap(),
            "repeatable read"
        );
        assert_eq!(
            setting(&mut source, "transaction_read_only", &io).unwrap(),
            "on"
        );
        writer
            .query(&format!("INSERT INTO {quoted} VALUES (2)"), &io)
            .unwrap();
        assert_eq!(
            text(
                &source
                    .query(&format!("SELECT count(*) FROM {quoted}"), &io)
                    .unwrap(),
                0,
                0
            )
            .unwrap(),
            "1"
        );
        assert_eq!(
            load_cdc_relation(&mut source, &table, 7, &io).unwrap(),
            relation
        );
        writer.query("SET lock_timeout = '100ms'", &io).unwrap();
        let error = writer
            .query(
                &format!("ALTER TABLE {quoted} ADD COLUMN blocked integer"),
                &io,
            )
            .err()
            .unwrap();
        assert!(error.message.contains("55P03"), "{error:?}");
        assert_eq!(
            source.transaction_status().unwrap(),
            crate::pg::TransactionStatus::InTransaction
        );
        command(&mut source, "COMMIT", "COMMIT", &io).unwrap();
        assert_eq!(
            text(
                &source
                    .query(&format!("SELECT count(*) FROM {quoted}"), &io)
                    .unwrap(),
                0,
                0
            )
            .unwrap(),
            "2"
        );
    }

    fn cdc_layout() -> Vec<Column> {
        [
            ("first key", "int4", "-1", true),
            ("payload", "varchar", "16", false),
            ("last key", "int8", "-1", true),
        ]
        .into_iter()
        .map(|(name, type_name, modifier, not_null)| Column {
            name: name.into(),
            type_name: type_name.into(),
            modifier: modifier.into(),
            not_null,
            collation_schema: String::new(),
            collation: String::new(),
        })
        .collect()
    }

    #[test]
    fn cdc_object_id_preserves_unsigned_range() {
        assert_eq!(cdc_object_id("4294967295").unwrap(), u32::MAX);
        assert_eq!(cdc_object_id("1").unwrap(), 1);
    }

    #[test]
    fn cdc_object_id_rejects_invalid_or_overflowing_oids() {
        for oid in ["", "0", "-1", "+1", " 1", "1 ", "4294967296", "1.0"] {
            let error = cdc_object_id(oid).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadConfig, "{oid}");
            assert!(!error.message.contains(oid) || oid.is_empty());
        }
    }

    #[test]
    fn cdc_primary_key_preserves_physical_order_and_excludes_included_columns() {
        let layout = cdc_layout();
        let keys = cdc_primary_key("3 1 2", "2", "3", &layout).unwrap();
        let metadata = [
            ["first key", "1", "23", "-1", "int4"],
            ["payload", "2", "1043", "16", "varchar"],
            ["last key", "3", "20", "-1", "int8"],
        ];
        let columns = layout
            .iter()
            .zip(metadata)
            .enumerate()
            .map(|(position, (column, row))| {
                cdc_column(column, position + 1, row, keys.contains(&(position + 1))).unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            columns,
            vec![
                CdcColumn {
                    name: "first key".into(),
                    type_oid: 23,
                    type_mod: -1,
                    key: true,
                },
                CdcColumn {
                    name: "payload".into(),
                    type_oid: 1043,
                    type_mod: 16,
                    key: false,
                },
                CdcColumn {
                    name: "last key".into(),
                    type_oid: 20,
                    type_mod: -1,
                    key: true,
                },
            ]
        );
        assert_eq!(
            columns
                .iter()
                .filter(|column| column.key)
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["first key", "last key"]
        );
    }

    #[test]
    fn cdc_primary_key_rejects_invalid_counts() {
        let layout = cdc_layout();
        for (keys, attributes) in [
            ("0", "1"),
            ("2", "1"),
            ("1", "0"),
            ("1", "4"),
            ("", "1"),
            ("1", ""),
            ("-1", "1"),
            ("+1", "1"),
            ("1", "18446744073709551616"),
        ] {
            assert!(cdc_primary_key("1", keys, attributes, &layout).is_err());
        }
        assert!(cdc_primary_key("1", "1", "1", &[]).is_err());
    }

    #[test]
    fn cdc_primary_key_rejects_missing_extra_and_duplicate_attributes() {
        let layout = cdc_layout();
        for (attributes, keys, total) in [
            ("", "1", "1"),
            ("1", "2", "2"),
            ("1 3", "1", "1"),
            ("1 1", "2", "2"),
            ("1 3 3", "2", "3"),
        ] {
            assert!(cdc_primary_key(attributes, keys, total, &layout).is_err());
        }
    }

    #[test]
    fn cdc_primary_key_rejects_nonpositive_missing_or_expression_attributes() {
        let layout = cdc_layout();
        for attributes in ["0", "-1", "4", "+1", "4294967296", "1.0", "NULL"] {
            assert!(cdc_primary_key(attributes, "1", "1", &layout).is_err());
        }
    }

    #[test]
    fn cdc_primary_key_requires_not_null_keys_but_allows_nullable_include() {
        let layout = cdc_layout();
        assert!(cdc_primary_key("2", "1", "1", &layout).is_err());
        assert_eq!(
            cdc_primary_key("1 2", "1", "2", &layout).unwrap(),
            BTreeSet::from([1])
        );
    }

    #[test]
    fn cdc_column_preserves_numeric_types_and_signed_typmods() {
        let mut layout = cdc_layout();
        for modifier in [i32::MIN, -1, 0, i32::MAX] {
            let modifier = modifier.to_string();
            layout[0].modifier = modifier.clone();
            let column = cdc_column(
                &layout[0],
                1,
                ["first key", "1", "23", &modifier, "int4"],
                true,
            )
            .unwrap();
            assert_eq!(column.type_oid, 23);
            assert_eq!(column.type_mod.to_string(), modifier);
            assert!(column.key);
        }
    }

    #[test]
    fn cdc_column_rejects_mismatched_name_position_type_and_typmod() {
        let layout = cdc_layout();
        for row in [
            ["payload", "1", "23", "-1", "int4"],
            ["first key", "2", "23", "-1", "int4"],
            ["first key", "0", "23", "-1", "int4"],
            ["first key", "1", "23", "-1", "int8"],
            ["first key", "1", "23", "0", "int4"],
            ["", "1", "23", "-1", "int4"],
        ] {
            assert!(cdc_column(&layout[0], 1, row, true).is_err());
        }
    }

    #[test]
    fn cdc_column_rejects_invalid_or_nonbuiltin_type_oids() {
        let layout = cdc_layout();
        for oid in ["", "0", "-1", "+23", "23 ", "16384", "4294967296"] {
            assert!(
                cdc_column(&layout[0], 1, ["first key", "1", oid, "-1", "int4"], true).is_err()
            );
        }
    }

    #[test]
    fn cdc_column_rejects_invalid_or_overflowing_typmods() {
        let mut layout = cdc_layout();
        for modifier in ["", "2147483648", "-2147483649", "-1 ", "1.0"] {
            layout[0].modifier = modifier.into();
            assert!(
                cdc_column(
                    &layout[0],
                    1,
                    ["first key", "1", "23", modifier, "int4"],
                    true,
                )
                .is_err()
            );
        }
    }

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
