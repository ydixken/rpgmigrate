use std::mem::size_of;

use crate::cdc::contracts::{
    Begin, Column, Commit, Event, FollowMessage, OldTuple, OldTupleKind, Plugin, RelationKey, Value,
};
use crate::contracts::{Error, ErrorKind, FollowBinding, FollowLimits, Result, SourceType};
use crate::identity::{CommitLsn, SourceEndLsn, StreamId};

#[path = "pgoutput/cursor.rs"]
mod cursor;
use cursor::Cursor;

const MAX_RELATIONS: usize = 4096;
const MAX_TYPES: usize = 16384;
const MAX_COLUMNS: usize = 1600;
const MAX_METADATA_BYTES: usize = 1024 * 1024;
const FIRST_NORMAL_OBJECT_ID: u32 = 16384;

#[derive(Clone)]
struct Relation {
    key: RelationKey,
    schema: String,
    name: String,
    columns: Vec<Column>,
    replica_identity: u8,
}

#[derive(Clone)]
struct Type {
    source: SourceType,
    seen: bool,
}

pub(crate) struct Decoder {
    limits: FollowLimits,
    active: Option<Begin>,
    last_end: Option<SourceEndLsn>,
    relations: Vec<Relation>,
    types: Vec<Type>,
    origin: Option<(u64, String)>,
    poisoned: bool,
}

impl Decoder {
    pub(crate) fn new(
        stream: StreamId,
        binding: &FollowBinding,
        limits: FollowLimits,
    ) -> Result<Self> {
        limits.validate()?;
        if stream != binding.stream()
            || stream.run.0 == [0; 16]
            || stream.source.system_identifier == 0
            || stream.source.database_oid == 0
            || stream.generation == 0
            || binding.slot.intent.binding.plugin() != Plugin::Pgoutput
            || binding.slot.consistent_point.0 == 0
            || binding.source_timeline == 0
            || binding.copy_generation.0 == 0
            || binding.source_types.len() > MAX_TYPES
        {
            return Err(refusal("invalid immutable follow binding"));
        }
        if add(
            size_of::<Self>(),
            binding.source_types.len() * size_of::<Type>(),
        )? > MAX_METADATA_BYTES
        {
            return Err(refusal("metadata budget exceeded"));
        }
        let mut types = Vec::new();
        reserve(&mut types, binding.source_types.len())?;
        let mut bytes = size_of::<Self>() + types.capacity() * size_of::<Type>();
        for source in &binding.source_types {
            if source.oid < FIRST_NORMAL_OBJECT_ID
                || !valid_name(&source.schema)
                || !valid_name(&source.name)
                || types
                    .iter()
                    .any(|known: &Type| known.source.oid == source.oid)
            {
                return Err(refusal("invalid or duplicate bound type metadata"));
            }
            bytes = add(bytes, add(source.schema.len(), source.name.len())?)?;
            if bytes > MAX_METADATA_BYTES {
                return Err(refusal("metadata budget exceeded"));
            }
            types.push(Type {
                source: source.clone(),
                seen: false,
            });
        }
        Ok(Self {
            limits,
            active: None,
            last_end: None,
            relations: Vec::new(),
            types,
            origin: None,
            poisoned: false,
        })
    }

    pub(crate) fn is_idle(&self) -> bool {
        !self.poisoned && self.active.is_none()
    }

    pub(crate) fn feed(
        &mut self,
        payload: &[u8],
        emit: &mut dyn FnMut(FollowMessage) -> Result<()>,
    ) -> Result<()> {
        if self.poisoned {
            return Err(Error::new(
                ErrorKind::BadState,
                "pgoutput: decoder is poisoned",
            ));
        }
        // A caught callback panic must leave the decoder unusable.
        self.poisoned = true;
        if payload.len() > self.limits.max_message_bytes {
            return Err(refusal("message budget exceeded"));
        }
        let mut cursor = Cursor::new(payload);
        let tag = cursor.byte()?;
        let message = match tag {
            b'B' => Some(self.begin(&mut cursor)?),
            b'C' => Some(self.commit(&mut cursor)?),
            b'R' | b'Y' | b'I' | b'U' | b'D' | b'T' | b'O' => {
                if self.active.is_none() {
                    return Err(refusal("message outside transaction"));
                }
                match tag {
                    b'R' => Some(FollowMessage::Event(self.relation(&mut cursor)?)),
                    b'Y' => {
                        self.source_type(&mut cursor)?;
                        None
                    }
                    b'O' => {
                        self.origin(&mut cursor)?;
                        None
                    }
                    b'T' => Some(FollowMessage::Event(self.truncate(&mut cursor)?)),
                    _ => Some(FollowMessage::Event(self.row(tag, &mut cursor)?)),
                }
            }
            _ => return Err(refusal("unsupported message tag or protocol feature")),
        };
        cursor.finish()?;
        if let Some(message) = message {
            if message_bytes(&message)? > self.limits.max_message_bytes {
                return Err(refusal("decoded message budget exceeded"));
            }
            emit(message)?;
        }
        self.poisoned = false;
        Ok(())
    }

    fn begin(&mut self, cursor: &mut Cursor<'_>) -> Result<FollowMessage> {
        if self.active.is_some() {
            return Err(refusal("duplicate transaction begin"));
        }
        let final_lsn = CommitLsn(cursor.u64()?);
        let commit_time_micros = timestamp(cursor.i64()?)?;
        let xid = cursor.u32()?;
        if final_lsn.0 == 0 || xid < 3 || self.last_end.is_some_and(|last| final_lsn.0 < last.0) {
            return Err(refusal("invalid transaction begin identity"));
        }
        let begin = Begin {
            xid,
            final_lsn,
            commit_time_micros,
        };
        self.active = Some(begin);
        self.origin = None;
        Ok(FollowMessage::Begin(begin))
    }

    fn commit(&mut self, cursor: &mut Cursor<'_>) -> Result<FollowMessage> {
        let begin = self.active.ok_or_else(|| refusal("commit without begin"))?;
        if cursor.byte()? != 0 {
            return Err(refusal("unknown commit flags"));
        }
        let commit = CommitLsn(cursor.u64()?);
        let end = SourceEndLsn(cursor.u64()?);
        let commit_time_micros = timestamp(cursor.i64()?)?;
        if commit != begin.final_lsn
            || end.0 < commit.0
            || commit_time_micros != begin.commit_time_micros
            || self.last_end.is_some_and(|last| end <= last)
        {
            return Err(refusal("inconsistent source commit identity"));
        }
        self.active = None;
        self.last_end = Some(end);
        self.origin = None;
        Ok(FollowMessage::Commit(Commit {
            xid: begin.xid,
            commit,
            end,
            commit_time_micros,
        }))
    }

    fn source_type(&mut self, cursor: &mut Cursor<'_>) -> Result<()> {
        let oid = cursor.u32()?;
        let schema = cursor.namespace()?;
        let name = cursor.string(63, false)?;
        let known = self
            .types
            .iter_mut()
            .find(|known| known.source.oid == oid)
            .ok_or_else(|| refusal("unbound source type metadata"))?;
        if known.source.schema != schema || known.source.name != name {
            return Err(refusal("source type metadata changed"));
        }
        known.seen = true;
        Ok(())
    }

    fn relation(&mut self, cursor: &mut Cursor<'_>) -> Result<Event> {
        let oid = cursor.u32()?;
        let schema = cursor.namespace()?;
        let name = cursor.string(63, false)?;
        let replica_identity = cursor.byte()?;
        if oid == 0 || !matches!(replica_identity, b'd' | b'n' | b'f' | b'i') {
            return Err(refusal("invalid relation metadata"));
        }
        let width = usize::from(cursor.u16()?);
        if width > MAX_COLUMNS {
            return Err(refusal("relation column budget exceeded"));
        }
        let mut columns = Vec::new();
        reserve(&mut columns, width)?;
        for _ in 0..width {
            let flags = cursor.byte()?;
            let name = cursor.string(63, false)?;
            let type_oid = cursor.u32()?;
            let type_mod = cursor.i32()?;
            if flags > 1
                || type_oid == 0
                || type_mod < -1
                || columns.iter().any(|column: &Column| column.name == name)
                || (type_oid >= FIRST_NORMAL_OBJECT_ID
                    && !self
                        .types
                        .iter()
                        .any(|known| known.seen && known.source.oid == type_oid))
            {
                return Err(refusal("invalid or unbound relation column metadata"));
            }
            columns.push(Column {
                name,
                type_oid,
                type_mod,
                key: flags == 1,
            });
        }
        if (replica_identity == b'f' && columns.iter().any(|column| !column.key))
            || (replica_identity == b'n' && columns.iter().any(|column| column.key))
            || (replica_identity == b'i' && !columns.iter().any(|column| column.key))
        {
            return Err(refusal("inconsistent replica identity flags"));
        }
        let relation = Relation {
            key: RelationKey { oid, generation: 1 },
            schema,
            name,
            columns,
            replica_identity,
        };
        if let Some(previous) = self.relations.iter().find(|known| known.key.oid == oid) {
            if previous.schema != relation.schema
                || previous.name != relation.name
                || previous.columns != relation.columns
                || previous.replica_identity != relation.replica_identity
            {
                return Err(refusal("relation changed during DDL-quiescent follow"));
            }
        } else {
            if self.relations.len() == MAX_RELATIONS
                || self
                    .relations
                    .iter()
                    .any(|known| known.schema == relation.schema && known.name == relation.name)
            {
                return Err(refusal("relation count or unique identity violated"));
            }
            reserve(&mut self.relations, 1)?;
            if add(self.metadata_bytes()?, relation.heap_bytes()?)? > MAX_METADATA_BYTES {
                return Err(refusal("metadata budget exceeded"));
            }
            self.relations.push(relation.clone());
        }
        Ok(Event::Relation {
            key: relation.key,
            schema: relation.schema,
            name: relation.name,
            columns: relation.columns,
            replica_identity,
        })
    }

    fn row(&self, tag: u8, cursor: &mut Cursor<'_>) -> Result<Event> {
        let oid = cursor.u32()?;
        let relation = self
            .relations
            .iter()
            .find(|known| known.key.oid == oid)
            .ok_or_else(|| refusal("row has no relation context"))?;
        let kind = cursor.byte()?;
        let mut charged = size_of::<FollowMessage>();
        match tag {
            b'I' if kind == b'N' => Ok(Event::Insert {
                relation: relation.key,
                values: self.tuple(cursor, relation, TupleKind::Insert, &mut charged)?,
            }),
            b'U' => {
                if relation.replica_identity == b'n'
                    || !relation.columns.iter().any(|column| column.key)
                {
                    return Err(refusal("update lacks replica identity"));
                }
                let old = if matches!(kind, b'K' | b'O') {
                    let old = self.old(cursor, relation, kind, &mut charged)?;
                    if cursor.byte()? != b'N' {
                        return Err(refusal("update lacks new tuple"));
                    }
                    Some(old)
                } else if kind == b'N' && relation.replica_identity != b'f' {
                    None
                } else {
                    return Err(refusal("invalid update tuple kind"));
                };
                let new = self.tuple(cursor, relation, TupleKind::Update, &mut charged)?;
                Ok(Event::Update {
                    relation: relation.key,
                    old,
                    new,
                })
            }
            b'D' => Ok(Event::Delete {
                relation: relation.key,
                old: self.old(cursor, relation, kind, &mut charged)?,
            }),
            _ => Err(refusal("invalid row tuple kind")),
        }
    }

    fn old(
        &self,
        cursor: &mut Cursor<'_>,
        relation: &Relation,
        tag: u8,
        charged: &mut usize,
    ) -> Result<OldTuple> {
        let kind = match (tag, relation.replica_identity) {
            (b'K', b'd' | b'i') if relation.columns.iter().any(|column| column.key) => {
                OldTupleKind::Key
            }
            (b'O', b'f') => OldTupleKind::Full,
            _ => return Err(refusal("old tuple conflicts with replica identity")),
        };
        let values = self.tuple(
            cursor,
            relation,
            if kind == OldTupleKind::Key {
                TupleKind::Key
            } else {
                TupleKind::Full
            },
            charged,
        )?;
        Ok(OldTuple { kind, values })
    }

    fn tuple(
        &self,
        cursor: &mut Cursor<'_>,
        relation: &Relation,
        kind: TupleKind,
        charged: &mut usize,
    ) -> Result<Vec<Value>> {
        let width = usize::from(cursor.u16()?);
        if width != relation.columns.len() {
            return Err(refusal("tuple width differs from relation"));
        }
        *charged = add(*charged, width * size_of::<Value>())?;
        if *charged > self.limits.max_message_bytes {
            return Err(refusal("tuple metadata budget exceeded"));
        }
        let mut values = Vec::new();
        reserve(&mut values, width)?;
        *charged = add(*charged, (values.capacity() - width) * size_of::<Value>())?;
        for column in &relation.columns {
            let value = match cursor.byte()? {
                b'n' => Value::Null,
                b'u' if matches!(kind, TupleKind::Update | TupleKind::Full) => {
                    Value::UnchangedToast
                }
                b't' => {
                    let length = usize::try_from(cursor.i32()?)
                        .map_err(|_| refusal("negative tuple field length"))?;
                    *charged = add(*charged, length)?;
                    if *charged > self.limits.max_message_bytes {
                        return Err(refusal("tuple field budget exceeded"));
                    }
                    let bytes = cursor.take(length)?;
                    if bytes.contains(&0) || std::str::from_utf8(bytes).is_err() {
                        return Err(refusal("invalid text tuple encoding"));
                    }
                    let mut value = Vec::new();
                    reserve(&mut value, length)?;
                    *charged = add(*charged, value.capacity() - length)?;
                    if *charged > self.limits.max_message_bytes {
                        return Err(refusal("tuple allocation budget exceeded"));
                    }
                    value.extend_from_slice(bytes);
                    Value::Text(value)
                }
                _ => return Err(refusal("unsupported tuple representation")),
            };
            if matches!(kind, TupleKind::Key) && !column.key && value != Value::Null {
                return Err(refusal("non-key old tuple placeholder is not null"));
            }
            values.push(value);
        }
        Ok(values)
    }

    fn truncate(&self, cursor: &mut Cursor<'_>) -> Result<Event> {
        let count =
            usize::try_from(cursor.u32()?).map_err(|_| refusal("truncate count overflow"))?;
        if count == 0 || count > MAX_RELATIONS {
            return Err(refusal("truncate relation budget exceeded"));
        }
        let flags = cursor.byte()?;
        if flags & !3 != 0 {
            return Err(refusal("unknown truncate flags"));
        }
        let mut relations = Vec::new();
        reserve(&mut relations, count)?;
        for _ in 0..count {
            let oid = cursor.u32()?;
            let key = self
                .relations
                .iter()
                .find(|relation| relation.key.oid == oid)
                .map(|relation| relation.key)
                .ok_or_else(|| refusal("truncate has no relation context"))?;
            if relations.contains(&key) {
                return Err(refusal("duplicate truncate relation"));
            }
            relations.push(key);
        }
        Ok(Event::Truncate {
            relations,
            cascade: flags & 1 != 0,
            restart_identity: flags & 2 != 0,
        })
    }

    fn origin(&mut self, cursor: &mut Cursor<'_>) -> Result<()> {
        let lsn = cursor.u64()?;
        let name = cursor.string(1024, false)?;
        if lsn == 0 {
            return Err(refusal("invalid origin LSN"));
        }
        self.origin = None;
        if add(self.metadata_bytes()?, name.capacity())? > MAX_METADATA_BYTES {
            return Err(refusal("metadata budget exceeded"));
        }
        self.origin = Some((lsn, name));
        Ok(())
    }

    fn metadata_bytes(&self) -> Result<usize> {
        let mut bytes = size_of::<Self>();
        bytes = add(bytes, self.relations.capacity() * size_of::<Relation>())?;
        bytes = add(bytes, self.types.capacity() * size_of::<Type>())?;
        for relation in &self.relations {
            bytes = add(bytes, relation.heap_bytes()?)?;
        }
        for known in &self.types {
            bytes = add(
                bytes,
                add(known.source.schema.capacity(), known.source.name.capacity())?,
            )?;
        }
        if let Some((_, name)) = &self.origin {
            bytes = add(bytes, name.capacity())?;
        }
        Ok(bytes)
    }
}

impl Relation {
    fn heap_bytes(&self) -> Result<usize> {
        let mut bytes = add(self.schema.capacity(), self.name.capacity())?;
        bytes = add(bytes, self.columns.capacity() * size_of::<Column>())?;
        for column in &self.columns {
            bytes = add(bytes, column.name.capacity())?;
        }
        Ok(bytes)
    }
}

enum TupleKind {
    Insert,
    Update,
    Key,
    Full,
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 63 && !name.as_bytes().contains(&0)
}

// PostgreSQL 16/18 datatype/timestamp.h excludes infinities and the upper endpoint.
fn timestamp(value: i64) -> Result<i64> {
    if !(-211_813_488_000_000_000..9_223_371_331_200_000_000).contains(&value) {
        return Err(refusal("invalid transaction timestamp"));
    }
    Ok(value)
}

fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| refusal("allocation accounting overflow"))
}

fn reserve<T>(values: &mut Vec<T>, additional: usize) -> Result<()> {
    values
        .try_reserve_exact(additional)
        .map_err(|_| refusal("allocation refused"))
}

fn message_bytes(message: &FollowMessage) -> Result<usize> {
    let mut bytes = size_of::<FollowMessage>();
    let mut tuple = |values: &Vec<Value>| -> Result<()> {
        bytes = add(bytes, values.capacity() * size_of::<Value>())?;
        for value in values {
            if let Value::Text(value) = value {
                bytes = add(bytes, value.capacity())?;
            }
        }
        Ok(())
    };
    match message {
        FollowMessage::Event(Event::Relation {
            schema,
            name,
            columns,
            ..
        }) => {
            bytes = add(bytes, add(schema.capacity(), name.capacity())?)?;
            bytes = add(bytes, columns.capacity() * size_of::<Column>())?;
            for column in columns {
                bytes = add(bytes, column.name.capacity())?;
            }
        }
        FollowMessage::Event(Event::Insert { values, .. }) => tuple(values)?,
        FollowMessage::Event(Event::Update { old, new, .. }) => {
            if let Some(old) = old {
                tuple(&old.values)?;
            }
            tuple(new)?;
        }
        FollowMessage::Event(Event::Delete { old, .. }) => tuple(&old.values)?,
        FollowMessage::Event(Event::Truncate { relations, .. }) => {
            bytes = add(bytes, relations.capacity() * size_of::<RelationKey>())?
        }
        _ => {}
    }
    Ok(bytes)
}

#[track_caller]
fn refusal(reason: &'static str) -> Error {
    Error::new(ErrorKind::Source, format!("pgoutput: {reason}"))
}

#[cfg(test)]
#[path = "pgoutput/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "pgoutput/fuzz.rs"]
mod fuzz;
