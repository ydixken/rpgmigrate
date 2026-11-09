use std::collections::BTreeSet;

use crate::catalog::FollowRelation;
use crate::cdc::contracts::{Event, OldTuple, OldTupleKind, RelationKey, Value};
use crate::contracts::{Error, ErrorKind, Result};
use crate::pg::PgParam;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Operation {
    Insert,
    Update,
    Delete,
}

struct RowValues<'a> {
    operation: Operation,
    new: &'a [Value],
    old: Option<&'a OldTuple>,
}

pub(super) struct DmlPlan {
    relation: RelationKey,
    operation: Operation,
    column_count: usize,
    full_identity: bool,
    key_columns: Vec<usize>,
    always_columns: Vec<usize>,
    rewrite_identity: bool,
    write_columns: Vec<usize>,
    sql: String,
    param_types: Vec<u32>,
}

impl DmlPlan {
    pub(super) fn new(checked: &FollowRelation, row: &Event) -> Result<Self> {
        let Event::Relation {
            key,
            schema,
            name,
            columns,
            replica_identity,
        } = &checked.relation
        else {
            return Err(refusal("apply DML: checked relation context is required"));
        };
        if key.oid == 0
            || !matches!(replica_identity, b'd' | b'i' | b'f')
            || columns.is_empty()
            || columns.len() > 1600
            || checked.target_type_oids.len() != columns.len()
            || checked.target_type_oids.contains(&0)
            || checked.identity_modes.len() != columns.len()
            || checked
                .identity_modes
                .iter()
                .any(|mode| !matches!(mode, 0 | b'a' | b'd'))
        {
            return Err(refusal("apply DML: relation profile is unsupported"));
        }
        let table = format!("{}.{}", quote_identifier(schema)?, quote_identifier(name)?);
        let mut names = BTreeSet::new();
        let mut quoted = Vec::with_capacity(columns.len());
        for column in columns {
            if column.type_oid == 0 || !names.insert(column.name.as_str()) {
                return Err(refusal("apply DML: column metadata is invalid"));
            }
            quoted.push(quote_identifier(&column.name)?);
        }
        let full_identity = *replica_identity == b'f';
        let key_columns: Vec<_> = columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| (full_identity || column.key).then_some(index))
            .collect();
        if key_columns.is_empty() {
            return Err(refusal("apply DML: usable identity is required"));
        }
        let operation = operation(row)?;
        let mut plan = Self {
            relation: *key,
            operation,
            column_count: columns.len(),
            full_identity,
            key_columns,
            always_columns: checked
                .identity_modes
                .iter()
                .enumerate()
                .filter_map(|(index, mode)| (*mode == b'a').then_some(index))
                .collect(),
            rewrite_identity: false,
            write_columns: Vec::new(),
            sql: String::new(),
            param_types: Vec::new(),
        };
        let values = plan.values(row)?;
        plan.rewrite_identity = plan.needs_identity_rewrite(&values)?;
        let new = values.new;
        plan.write_columns = plan
            .writable_columns(operation, new, plan.rewrite_identity)
            .collect();
        let predicates = if operation == Operation::Insert {
            &[][..]
        } else {
            &plan.key_columns
        };
        let count = parameter_count(plan.write_columns.len(), predicates.len())?;
        plan.param_types = plan
            .write_columns
            .iter()
            .chain(predicates)
            .map(|index| checked.target_type_oids[*index])
            .collect();
        let predicate = predicates.iter().enumerate().map(|(index, column)|
            if full_identity {
                format!("({}::pg_catalog.text COLLATE pg_catalog.\"C\") IS NOT DISTINCT FROM (${}::pg_catalog.text COLLATE pg_catalog.\"C\")", quoted[*column], plan.write_columns.len() + index + 1)
            } else {
                format!("{} IS NOT DISTINCT FROM ${}", quoted[*column], plan.write_columns.len() + index + 1)
            })
            .collect::<Vec<_>>().join(" AND ");
        // FULL has no uniqueness guarantee; one source row changes exactly one physical target row.
        let ordinary_predicate = full_identity
            .then(|| format!("ctid = (SELECT ctid FROM ONLY {table} WHERE {predicate} LIMIT 1)"));
        let ordinary_predicate = ordinary_predicate.as_deref().unwrap_or(&predicate);
        plan.sql = match operation {
            Operation::Insert => {
                let placeholders = (1..=count)
                    .map(|index| format!("${index}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let overriding = if !plan.always_columns.is_empty() {
                    " OVERRIDING SYSTEM VALUE"
                } else {
                    ""
                };
                format!(
                    "INSERT INTO {table} ({}){overriding} VALUES ({placeholders})",
                    quoted.join(", ")
                )
            }
            Operation::Update if plan.rewrite_identity => {
                let mut parameter = 0;
                let selected = new
                    .iter()
                    .zip(&quoted)
                    .map(|(value, name)| {
                        if matches!(value, Value::UnchangedToast) {
                            format!("rpgmigrate_old.{name}")
                        } else {
                            parameter += 1;
                            format!("${parameter}")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let columns = quoted.join(", ");
                format!(
                    "WITH rpgmigrate_old AS (DELETE FROM ONLY {table} WHERE ctid = (SELECT ctid FROM ONLY {table} WHERE {predicate} LIMIT 1 FOR UPDATE) RETURNING {columns}) INSERT INTO {table} ({columns}) OVERRIDING SYSTEM VALUE SELECT {selected} FROM rpgmigrate_old"
                )
            }
            Operation::Update => {
                let assignments = plan
                    .write_columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| format!("{} = ${}", quoted[*column], index + 1))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("UPDATE ONLY {table} SET {assignments} WHERE {ordinary_predicate}")
            }
            Operation::Delete => format!("DELETE FROM ONLY {table} WHERE {ordinary_predicate}"),
        };
        Ok(plan)
    }
    pub(super) fn expected_tag(&self) -> &'static str {
        match self.operation {
            Operation::Insert => "INSERT 0 1",
            Operation::Update if self.rewrite_identity => "INSERT 0 1",
            Operation::Update => "UPDATE 1",
            Operation::Delete => "DELETE 1",
        }
    }
    pub(super) fn sql(&self) -> &str {
        &self.sql
    }
    pub(super) fn param_types(&self) -> &[u32] {
        &self.param_types
    }
    pub(super) fn owned_capacity_bytes(&self) -> Result<usize> {
        [
            (self.sql.capacity(), size_of::<u8>()),
            (self.param_types.capacity(), size_of::<u32>()),
            (self.key_columns.capacity(), size_of::<usize>()),
            (self.always_columns.capacity(), size_of::<usize>()),
            (self.write_columns.capacity(), size_of::<usize>()),
        ]
        .into_iter()
        .try_fold(0usize, |total, (capacity, bytes)| {
            capacity
                .checked_mul(bytes)
                .and_then(|bytes| total.checked_add(bytes))
                .ok_or_else(|| refusal("apply DML: capacity overflow"))
        })
    }
    pub(super) fn matches(&self, row: &Event) -> Result<bool> {
        self.shape_matches(&self.values(row)?)
    }
    pub(super) fn bind<'a>(&self, row: &'a Event) -> Result<Vec<PgParam<'a>>> {
        let values = self.values(row)?;
        if !self.shape_matches(&values)? {
            return Err(refusal("apply DML: prepared shape differs"));
        }
        let RowValues {
            operation,
            new,
            old,
        } = values;
        let mut params = Vec::with_capacity(self.param_types.len());
        for index in &self.write_columns {
            params.push(parameter(&new[*index])?);
        }
        if operation != Operation::Insert {
            for column in &self.key_columns {
                params.push(parameter(match old {
                    Some(old) => &old.values[*column],
                    None => &new[*column],
                })?);
            }
        }
        Ok(params)
    }
    fn shape_matches(&self, values: &RowValues<'_>) -> Result<bool> {
        if values.operation != self.operation {
            return Ok(false);
        }
        let rewrite = self.needs_identity_rewrite(values)?;
        Ok(rewrite == self.rewrite_identity
            && self.write_columns.iter().copied().eq(self.writable_columns(
                values.operation,
                values.new,
                rewrite,
            )))
    }
    fn writable_columns<'a>(
        &'a self,
        operation: Operation,
        new: &'a [Value],
        rewrite: bool,
    ) -> impl Iterator<Item = usize> + 'a {
        new.iter().enumerate().filter_map(move |(index, value)| {
            (!matches!(value, Value::UnchangedToast)
                && (operation != Operation::Update
                    || rewrite
                    || !self.always_columns.contains(&index)))
            .then_some(index)
        })
    }
    fn needs_identity_rewrite(&self, values: &RowValues<'_>) -> Result<bool> {
        if values.operation != Operation::Update {
            return Ok(false);
        }
        let mut rewrite = false;
        for column in &self.always_columns {
            let new = &values.new[*column];
            if matches!(new, Value::UnchangedToast) {
                return Err(refusal("apply DML: ALWAYS identity value unavailable"));
            }
            let unchanged = match values.old {
                Some(old) if old.kind == OldTupleKind::Full => old.values[*column] == *new,
                Some(old) => self.key_columns.contains(column) && old.values[*column] == *new,
                None => self.key_columns.contains(column),
            };
            rewrite |= !unchanged;
        }
        Ok(rewrite
            || self
                .writable_columns(values.operation, values.new, false)
                .next()
                .is_none())
    }
    fn values<'a>(&self, row: &'a Event) -> Result<RowValues<'a>> {
        let (relation, operation, new, old) = match row {
            Event::Insert { relation, values } => {
                (relation, Operation::Insert, values.as_slice(), None)
            }
            Event::Update { relation, old, new } => {
                (relation, Operation::Update, new.as_slice(), old.as_ref())
            }
            Event::Delete { relation, old } => (relation, Operation::Delete, &[][..], Some(old)),
            _ => return Err(refusal("apply DML: row event required")),
        };
        if *relation != self.relation {
            return Err(refusal("apply DML: relation generation differs"));
        }
        if operation != Operation::Delete {
            if new.len() != self.column_count {
                return Err(refusal("apply DML: new tuple width differs"));
            }
            if operation == Operation::Insert
                && new.iter().any(|v| matches!(v, Value::UnchangedToast))
            {
                return Err(refusal("apply DML: INSERT has unavailable value"));
            }
        }
        if let Some(old) = old {
            if old.values.len() != self.column_count
                || (old.kind == OldTupleKind::Full) != self.full_identity
            {
                return Err(refusal("apply DML: old tuple shape differs"));
            }
        } else if self.full_identity && operation != Operation::Insert {
            return Err(refusal("apply DML: FULL requires old tuple"));
        }
        if operation != Operation::Insert {
            for column in &self.key_columns {
                let value = old.map_or_else(|| &new[*column], |old| &old.values[*column]);
                if matches!(value, Value::UnchangedToast) {
                    return Err(refusal("apply DML: identity value unavailable"));
                }
            }
        }
        Ok(RowValues {
            operation,
            new,
            old,
        })
    }
}

fn operation(row: &Event) -> Result<Operation> {
    match row {
        Event::Insert { .. } => Ok(Operation::Insert),
        Event::Update { .. } => Ok(Operation::Update),
        Event::Delete { .. } => Ok(Operation::Delete),
        _ => Err(refusal("apply DML: row event required")),
    }
}
fn parameter_count(columns: usize, keys: usize) -> Result<usize> {
    columns
        .checked_add(keys)
        .filter(|count| *count <= pq_sys::PQ_QUERY_PARAM_MAX_LIMIT as usize)
        .ok_or_else(|| refusal("apply DML: too many parameters"))
}
pub(super) fn quote_identifier(name: &str) -> Result<String> {
    if name.is_empty() || name.contains('\0') {
        return Err(refusal("apply DML: invalid identifier"));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}
fn parameter(value: &Value) -> Result<PgParam<'_>> {
    match value {
        Value::Null => Ok(PgParam::Null),
        Value::Text(bytes) => Ok(PgParam::Text(bytes)),
        Value::UnchangedToast => Err(refusal("apply DML: unchanged TOAST cannot be bound")),
    }
}
#[track_caller]
fn refusal(message: &'static str) -> Error {
    Error::new(ErrorKind::Target, message)
}
#[cfg(test)]
mod tests;
