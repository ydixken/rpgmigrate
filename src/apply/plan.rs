use std::collections::BTreeSet;

use crate::cdc::contracts::{Event, RelationKey, Value};
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
    old: Option<&'a [Value]>,
}

pub(super) struct DmlPlan {
    relation: RelationKey,
    operation: Operation,
    column_count: usize,
    key_columns: Vec<usize>,
    write_columns: Vec<usize>,
    sql: String,
    param_types: Vec<u32>,
}

impl DmlPlan {
    /// The caller verifies both endpoint profiles, ordered columns, and generation under DDL quiescence.
    /// Catalog preflight must reject generated/identity columns and establish DEFAULT PK/builtin semantics.
    /// This pure planner cannot verify either database or establish historical relation identity.
    pub(super) fn new(relation: &Event, row: &Event) -> Result<Self> {
        let Event::Relation {
            key,
            schema,
            name,
            columns,
            replica_identity,
        } = relation
        else {
            return Err(refusal("serial DML: relation context is required"));
        };
        if key.oid == 0 || *replica_identity != b'd' || columns.is_empty() || columns.len() > 1600 {
            return Err(refusal("serial DML: relation profile is unsupported"));
        }
        let table = format!("{}.{}", quote_identifier(schema)?, quote_identifier(name)?);
        let mut names = BTreeSet::new();
        let mut quoted = Vec::with_capacity(columns.len());
        for column in columns {
            if column.type_oid == 0
                || column.type_oid >= 16_384
                || !names.insert(column.name.as_str())
            {
                return Err(refusal("serial DML: column metadata is invalid"));
            }
            quoted.push(quote_identifier(&column.name)?);
        }
        let key_columns: Vec<_> = columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| column.key.then_some(index))
            .collect();
        if key_columns.is_empty() {
            return Err(refusal("serial DML: a verified primary key is required"));
        }
        let operation = match row {
            Event::Insert { .. } => Operation::Insert,
            Event::Update { .. } => Operation::Update,
            Event::Delete { .. } => Operation::Delete,
            Event::Truncate { .. } => return Err(truncate_refusal()),
            Event::Relation { .. } => {
                return Err(refusal("serial DML: a row event is required"));
            }
        };
        let mut plan = Self {
            relation: *key,
            operation,
            column_count: columns.len(),
            key_columns,
            write_columns: Vec::new(),
            sql: String::new(),
            param_types: Vec::new(),
        };
        let RowValues { new, .. } = plan.values(row)?;
        plan.write_columns = new
            .iter()
            .enumerate()
            .filter_map(|(index, value)| (!matches!(value, Value::UnchangedToast)).then_some(index))
            .collect();
        let predicate_columns = if operation == Operation::Insert {
            &[][..]
        } else {
            plan.key_columns.as_slice()
        };
        let count = parameter_count(plan.write_columns.len(), predicate_columns.len())?;
        plan.param_types = plan
            .write_columns
            .iter()
            .chain(predicate_columns)
            .map(|index| columns[*index].type_oid)
            .collect();
        let predicate = predicate_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                format!(
                    "{} = ${}",
                    quoted[*column],
                    plan.write_columns.len() + index + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        plan.sql = match operation {
            Operation::Insert => {
                let placeholders = (1..=count)
                    .map(|index| format!("${index}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "INSERT INTO {table} ({}) VALUES ({placeholders})",
                    quoted.join(", ")
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
                format!("UPDATE ONLY {table} SET {assignments} WHERE {predicate}")
            }
            Operation::Delete => format!("DELETE FROM ONLY {table} WHERE {predicate}"),
        };
        Ok(plan)
    }

    pub(super) fn sql(&self) -> &str {
        &self.sql
    }

    pub(super) fn param_types(&self) -> &[u32] {
        &self.param_types
    }

    /// Heap buffer capacity excludes inline storage, allocator overhead, and borrowed row values.
    /// The cache counts its inline entry and map overhead separately.
    pub(super) fn owned_capacity_bytes(&self) -> Result<usize> {
        [
            (self.sql.capacity(), size_of::<u8>()),
            (self.param_types.capacity(), size_of::<u32>()),
            (self.key_columns.capacity(), size_of::<usize>()),
            (self.write_columns.capacity(), size_of::<usize>()),
        ]
        .into_iter()
        .try_fold(0usize, |total, (capacity, element_bytes)| {
            capacity
                .checked_mul(element_bytes)
                .and_then(|bytes| total.checked_add(bytes))
                .ok_or_else(|| {
                    refusal("serial DML: owned buffer capacity exceeds the accounting limit")
                })
        })
    }

    pub(super) fn matches(&self, row: &Event) -> Result<bool> {
        let RowValues { operation, new, .. } = self.values(row)?;
        Ok(self.shape_matches(operation, new))
    }

    pub(super) fn bind<'a>(&self, row: &'a Event) -> Result<Vec<PgParam<'a>>> {
        let RowValues {
            operation,
            new,
            old,
        } = self.values(row)?;
        if !self.shape_matches(operation, new) {
            return Err(refusal(
                "serial DML: row does not match the prepared plan shape",
            ));
        }
        let mut params = Vec::with_capacity(self.param_types.len());
        for index in &self.write_columns {
            params.push(parameter(&new[*index])?);
        }
        if operation != Operation::Insert {
            for (index, column) in self.key_columns.iter().enumerate() {
                let value = match old {
                    Some(old) => &old[index],
                    None => &new[*column],
                };
                params.push(parameter(value)?);
            }
        }
        Ok(params)
    }

    fn shape_matches(&self, operation: Operation, new: &[Value]) -> bool {
        operation == self.operation
            && (operation != Operation::Update
                || self
                    .write_columns
                    .iter()
                    .copied()
                    .eq(new.iter().enumerate().filter_map(|(index, value)| {
                        (!matches!(value, Value::UnchangedToast)).then_some(index)
                    })))
    }

    fn values<'a>(&self, row: &'a Event) -> Result<RowValues<'a>> {
        let (relation, operation, new, old) = match row {
            Event::Insert { relation, values } => {
                (relation, Operation::Insert, values.as_slice(), None)
            }
            Event::Update { relation, old, new } => {
                (relation, Operation::Update, new.as_slice(), old.as_deref())
            }
            Event::Delete { relation, old } => {
                (relation, Operation::Delete, &[][..], Some(old.as_slice()))
            }
            Event::Truncate { .. } => return Err(truncate_refusal()),
            Event::Relation { .. } => {
                return Err(refusal("serial DML: a row event is required"));
            }
        };
        if *relation != self.relation {
            return Err(refusal(
                "serial DML: relation identity or generation does not match",
            ));
        }
        if operation != Operation::Delete {
            if new.len() != self.column_count {
                return Err(refusal("serial DML: new column count does not match"));
            }
            for value in new {
                if matches!(value, Value::Binary(_)) {
                    return Err(binary_refusal());
                }
                if operation == Operation::Insert && matches!(value, Value::UnchangedToast) {
                    return Err(refusal("serial DML: INSERT cannot contain unchanged TOAST"));
                }
            }
            for index in &self.key_columns {
                require_key(&new[*index])?;
            }
        }
        if let Some(old) = old {
            if old.len() != self.key_columns.len() {
                return Err(refusal("serial DML: old key count does not match"));
            }
            for value in old {
                require_key(value)?;
            }
        }
        Ok(RowValues {
            operation,
            new,
            old,
        })
    }
}

fn parameter_count(columns: usize, keys: usize) -> Result<usize> {
    columns
        .checked_add(keys)
        .filter(|count| *count <= pq_sys::PQ_QUERY_PARAM_MAX_LIMIT as usize)
        .ok_or_else(|| refusal("serial DML: parameter count exceeds the protocol limit"))
}

fn quote_identifier(name: &str) -> Result<String> {
    if name.is_empty() || name.contains('\0') {
        return Err(refusal(
            "serial DML: identifier is empty or contains a NUL byte",
        ));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

fn require_key(value: &Value) -> Result<()> {
    match value {
        Value::Text(_) => Ok(()),
        Value::Binary(_) => Err(binary_refusal()),
        Value::Null | Value::UnchangedToast => Err(refusal(
            "serial DML: primary-key value is missing or unavailable",
        )),
    }
}

fn parameter(value: &Value) -> Result<PgParam<'_>> {
    match value {
        Value::Null => Ok(PgParam::Null),
        Value::Text(bytes) => Ok(PgParam::Text(bytes)),
        Value::Binary(_) => Err(binary_refusal()),
        Value::UnchangedToast => Err(refusal("serial DML: unchanged TOAST cannot be bound")),
    }
}

fn binary_refusal() -> Error {
    refusal("serial DML: binary values are unsupported by the first apply profile")
}

fn truncate_refusal() -> Error {
    refusal("serial DML: TRUNCATE requires verified target scope closure")
}

fn refusal(message: &'static str) -> Error {
    Error {
        kind: ErrorKind::Target,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests;
