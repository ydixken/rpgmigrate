use std::borrow::Cow;
use std::collections::HashSet;

use crate::cdc::codec::{HEADER_BYTES, Limits};
use crate::cdc::contracts::{Column, Event, RelationKey, Value};
use crate::contracts::{Error, ErrorKind, QualifiedTable, Result};

pub(crate) enum Message<'a> {
    Begin(u32),
    Commit(u32),
    Row(Row<'a>),
    Truncate {
        tables: Vec<QualifiedTable>,
        cascade: bool,
        restart_identity: bool,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum RowKind {
    Insert,
    Update,
    Delete,
}

pub(crate) struct Row<'a> {
    pub(crate) table: QualifiedTable,
    kind: RowKind,
    input: &'a str,
}

pub(crate) fn parse(payload: &[u8], limits: Limits) -> Result<Message<'_>> {
    let mut budget = Budget::new(limits)?;
    let input = std::str::from_utf8(payload).map_err(|_| invalid())?;
    if input.contains('\0') {
        return Err(invalid());
    }
    for (prefix, begin) in [("BEGIN ", true), ("COMMIT ", false)] {
        if let Some(xid) = input.strip_prefix(prefix) {
            if xid.is_empty()
                || xid.starts_with('0')
                || !xid.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(invalid());
            }
            let xid = xid.parse().map_err(|_| invalid())?;
            return Ok(if begin {
                Message::Begin(xid)
            } else {
                Message::Commit(xid)
            });
        }
    }
    let mut cursor = Cursor::new(input);
    cursor.expect("table ")?;
    let table = cursor.table()?;
    let mut tables = Vec::new();
    if cursor.remaining().starts_with(", ") || cursor.remaining().starts_with(": TRUNCATE:") {
        budget.take(8)?;
        budget.take(12)?;
        tables.try_reserve(1).map_err(|_| limit_error())?;
        tables.push(table);
        while cursor.consume(", ") {
            budget.take(12)?;
            let table = cursor.table()?;
            tables.try_reserve(1).map_err(|_| limit_error())?;
            tables.push(table);
        }
        cursor.expect(": TRUNCATE: ")?;
        let (cascade, restart_identity) = match cursor.remaining() {
            "(no-flags)" => (false, false),
            "restart_seqs" => (false, true),
            "cascade" => (true, false),
            "restart_seqs cascade" => (true, true),
            _ => return Err(invalid()),
        };
        return Ok(Message::Truncate {
            tables,
            cascade,
            restart_identity,
        });
    }
    let kind = if cursor.consume(": INSERT:") {
        RowKind::Insert
    } else if cursor.consume(": UPDATE:") {
        RowKind::Update
    } else if cursor.consume(": DELETE:") {
        RowKind::Delete
    } else {
        return Err(invalid());
    };
    Ok(Message::Row(Row {
        table,
        kind,
        input: cursor.remaining(),
    }))
}

impl Row<'_> {
    pub(crate) fn decode(&self, relation: &Event, limits: Limits) -> Result<Event> {
        let (key, columns) = validate_relation(&self.table, relation, limits)?;
        let mut budget = Budget::new(limits)?;
        budget.take(18)?;
        let mut cursor = Cursor::new(self.input);
        let event = match self.kind {
            RowKind::Insert => Event::Insert {
                relation: key,
                values: cursor.values(columns, false, false, &mut budget)?,
            },
            RowKind::Update => {
                budget.take(1)?;
                let old = if cursor.consume(" old-key:") {
                    budget.take(4)?;
                    let old = cursor.values(columns, true, false, &mut budget)?;
                    cursor.expect(" new-tuple:")?;
                    Some(old)
                } else {
                    None
                };
                Event::Update {
                    relation: key,
                    old,
                    new: cursor.values(columns, false, true, &mut budget)?,
                }
            }
            RowKind::Delete => Event::Delete {
                relation: key,
                old: cursor.values(columns, true, false, &mut budget)?,
            },
        };
        cursor.finish()?;
        Ok(event)
    }
}

pub(crate) fn validate_relation<'a>(
    table: &QualifiedTable,
    relation: &'a Event,
    limits: Limits,
) -> Result<(RelationKey, &'a [Column])> {
    let Event::Relation {
        key,
        schema,
        name,
        columns,
        replica_identity,
    } = relation
    else {
        return Err(invalid());
    };
    if *replica_identity != b'd'
        || key.oid == 0
        || *schema != table.schema
        || *name != table.table
        || columns.is_empty()
        || !columns.iter().any(|column| column.key)
    {
        return Err(invalid());
    }
    let mut budget = Budget::new(limits)?;
    budget.take(27)?;
    budget.take(schema.len())?;
    budget.take(name.len())?;
    for column in columns {
        budget.take(13)?;
        budget.take(column.name.len())?;
        if column.name.is_empty()
            || column.name.len() > 63
            || column.name.contains('\0')
            || type_name(column.type_oid).is_none()
        {
            return Err(invalid());
        }
    }
    let mut names = HashSet::new();
    names
        .try_reserve(columns.len())
        .map_err(|_| limit_error())?;
    if columns
        .iter()
        .any(|column| !names.insert(column.name.as_str()))
    {
        return Err(invalid());
    }
    Ok((*key, columns))
}

fn invalid() -> Error {
    Error {
        kind: ErrorKind::Source,
        message: "test_decoding message is invalid or unsupported".into(),
    }
}

fn limit_error() -> Error {
    Error {
        kind: ErrorKind::Source,
        message: "test_decoding event exceeds decoder limits".into(),
    }
}

struct Budget {
    remaining: u64,
}

impl Budget {
    fn new(limits: Limits) -> Result<Self> {
        limits.validate().map_err(|_| limit_error())?;
        let part_bytes = (limits.max_frame_bytes - HEADER_BYTES) as u64;
        Ok(Self {
            remaining: limits
                .max_event_bytes
                .min(part_bytes * u64::from(limits.max_parts)),
        })
    }

    fn take(&mut self, bytes: impl TryInto<u64>) -> Result<()> {
        let bytes = bytes.try_into().map_err(|_| limit_error())?;
        self.remaining = self.remaining.checked_sub(bytes).ok_or_else(limit_error)?;
        Ok(())
    }
}

fn type_name(oid: u32) -> Option<&'static str> {
    Some(match oid {
        16 => "boolean",
        17 => "bytea",
        20 => "bigint",
        21 => "smallint",
        23 => "integer",
        25 => "text",
        700 => "real",
        701 => "double precision",
        1700 => "numeric",
        1042 => "character",
        1043 => "character varying",
        1082 => "date",
        1083 => "time without time zone",
        1266 => "time with time zone",
        1114 => "timestamp without time zone",
        1184 => "timestamp with time zone",
        1186 => "interval",
        2950 => "uuid",
        114 => "json",
        3802 => "jsonb",
        1560 => "bit",
        1562 => "bit varying",
        869 => "inet",
        650 => "cidr",
        829 => "macaddr",
        774 => "macaddr8",
        _ => return None,
    })
}

fn number(input: &str, integer: bool) -> bool {
    if !integer && matches!(input, "NaN" | "Infinity" | "-Infinity") {
        return true;
    }
    let mut bytes = input.as_bytes();
    if bytes.starts_with(b"-") {
        bytes = &bytes[1..];
    }
    let whole = bytes
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if whole == 0 {
        return false;
    }
    bytes = &bytes[whole..];
    if !integer && bytes.starts_with(b".") {
        bytes = &bytes[1..];
        let fraction = bytes
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if fraction == 0 {
            return false;
        }
        bytes = &bytes[fraction..];
    }
    if !integer && matches!(bytes.first(), Some(b'e' | b'E')) {
        bytes = &bytes[1..];
        if matches!(bytes.first(), Some(b'+' | b'-')) {
            bytes = &bytes[1..];
        }
        let exponent = bytes
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if exponent == 0 {
            return false;
        }
        bytes = &bytes[exponent..];
    }
    bytes.is_empty()
}

struct Cursor<'a> {
    input: &'a str,
}

impl<'a> Cursor<'a> {
    fn new(input: &'a str) -> Self {
        Self { input }
    }
    fn remaining(&self) -> &'a str {
        self.input
    }
    fn consume(&mut self, value: &str) -> bool {
        if let Some(rest) = self.input.strip_prefix(value) {
            self.input = rest;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, value: &str) -> Result<()> {
        if self.consume(value) {
            Ok(())
        } else {
            Err(invalid())
        }
    }
    fn finish(&self) -> Result<()> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
    fn table(&mut self) -> Result<QualifiedTable> {
        let schema = self.identifier()?.into_owned();
        self.expect(".")?;
        let table = self.identifier()?.into_owned();
        Ok(QualifiedTable { schema, table })
    }
    fn identifier(&mut self) -> Result<Cow<'a, str>> {
        if self.input.starts_with('"') {
            let (inner, length) = self.quoted(b'"')?;
            if length == 0 || length > 63 {
                return Err(invalid());
            }
            if length == inner.len() {
                return Ok(Cow::Borrowed(inner));
            }
            let bytes = unquote(inner, b'"', length)?;
            return String::from_utf8(bytes)
                .map(Cow::Owned)
                .map_err(|_| invalid());
        }
        let bytes = self.input.as_bytes();
        if !bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || *byte == b'_')
        {
            return Err(invalid());
        }
        let length = bytes
            .iter()
            .take_while(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || **byte == b'_')
            .count();
        if length > 63 {
            return Err(invalid());
        }
        let (name, rest) = self.input.split_at(length);
        self.input = rest;
        Ok(Cow::Borrowed(name))
    }
    fn type_label(&mut self) -> Result<&'a str> {
        self.expect("[")?;
        let bytes = self.input.as_bytes();
        let mut depth = 1_usize;
        let mut quoted = false;
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'"' if quoted && bytes.get(index + 1) == Some(&b'"') => {
                    index += 2;
                    continue;
                }
                b'"' => quoted = !quoted,
                b'[' if !quoted => depth = depth.checked_add(1).ok_or_else(invalid)?,
                b']' if !quoted => {
                    depth -= 1;
                    if depth == 0 {
                        if index == 0 {
                            return Err(invalid());
                        }
                        let label = &self.input[..index];
                        self.input = &self.input[index + 1..];
                        return Ok(label);
                    }
                }
                _ => {}
            }
            index += 1;
        }
        Err(invalid())
    }
    fn quoted(&mut self, quote: u8) -> Result<(&'a str, usize)> {
        if self.input.as_bytes().first() != Some(&quote) {
            return Err(invalid());
        }
        let input = &self.input[1..];
        let bytes = input.as_bytes();
        let mut index = 0;
        let mut length = 0;
        while index < bytes.len() {
            if bytes[index] == quote {
                if bytes.get(index + 1) == Some(&quote) {
                    index += 2;
                    length += 1;
                    continue;
                }
                self.input = &input[index + 1..];
                return Ok((&input[..index], length));
            }
            index += 1;
            length += 1;
        }
        Err(invalid())
    }
    fn values(
        &mut self,
        columns: &[Column],
        keys_only: bool,
        update: bool,
        budget: &mut Budget,
    ) -> Result<Vec<Value>> {
        let count = columns
            .iter()
            .filter(|column| !keys_only || column.key)
            .count();
        let mut values = Vec::new();
        values.try_reserve_exact(count).map_err(|_| limit_error())?;
        for column in columns.iter().filter(|column| !keys_only || column.key) {
            self.expect(" ")?;
            if self.identifier()?.as_ref() != column.name {
                return Err(invalid());
            }
            if Some(self.type_label()?) != type_name(column.type_oid) {
                return Err(invalid());
            }
            self.expect(":")?;
            let value = self.value(column.type_oid, budget)?;
            if (column.key && matches!(value, Value::Null | Value::UnchangedToast))
                || (!update && matches!(value, Value::UnchangedToast))
            {
                return Err(invalid());
            }
            values.push(value);
        }
        Ok(values)
    }
    fn value(&mut self, oid: u32, budget: &mut Budget) -> Result<Value> {
        let token_end = self.input.find(' ').unwrap_or(self.input.len());
        let token = &self.input[..token_end];
        if matches!(token, "null" | "unchanged-toast-datum") {
            if token == "unchanged-toast-datum"
                && !matches!(
                    oid,
                    17 | 25 | 1042 | 1043 | 1700 | 114 | 3802 | 1560 | 1562 | 869 | 650
                )
            {
                return Err(invalid());
            }
            self.input = &self.input[token_end..];
            budget.take(1)?;
            return Ok(if token == "null" {
                Value::Null
            } else {
                Value::UnchangedToast
            });
        }
        let (bytes, length, quote) = match oid {
            20 | 21 | 23 | 700 | 701 | 1700 | 16 => {
                if if oid == 16 {
                    !matches!(token, "true" | "false")
                } else {
                    !number(token, matches!(oid, 20 | 21 | 23))
                } {
                    return Err(invalid());
                }
                self.input = &self.input[token_end..];
                (token, token.len(), None)
            }
            1560 | 1562 => {
                self.expect("B")?;
                let (inner, length) = self.quoted(b'\'')?;
                if !inner.bytes().all(|byte| matches!(byte, b'0' | b'1')) {
                    return Err(invalid());
                }
                (inner, length, None)
            }
            _ => {
                let (inner, length) = self.quoted(b'\'')?;
                (inner, length, Some(b'\''))
            }
        };
        budget.take(5)?;
        budget.take(length)?;
        Ok(Value::Text(unquote(bytes, quote.unwrap_or(0), length)?))
    }
}

fn unquote(input: &str, quote: u8, length: usize) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(length)
        .map_err(|_| limit_error())?;
    let mut bytes = input.bytes();
    while let Some(byte) = bytes.next() {
        output.push(byte);
        if byte == quote {
            bytes.next();
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests;
