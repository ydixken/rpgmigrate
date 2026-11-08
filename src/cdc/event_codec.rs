//! Versioned event payloads have separate budgets for wire bytes and decoded heap capacity.

use super::codec::{Error, HEADER_BYTES, Limits, Result};
use super::contracts::{Column, Event, RelationKey, Value};

const VERSION: u8 = 1;

pub(crate) struct DecodeBudget {
    remaining: usize,
}

impl DecodeBudget {
    pub(crate) fn new(bytes: usize) -> Self {
        Self { remaining: bytes }
    }

    pub(crate) fn remaining_bytes(&self) -> usize {
        self.remaining
    }

    fn charge<T>(&mut self, count: usize) -> Result<usize> {
        let bytes = count.checked_mul(size_of::<T>()).ok_or(Error::Overflow)?;
        self.remaining = self.remaining.checked_sub(bytes).ok_or(Error::Allocation)?;
        Ok(bytes)
    }

    fn settle<T>(&mut self, requested_bytes: usize, capacity: usize) -> Result<()> {
        // Exact reserve can grant excess; settling bounds retained capacity, not transient allocator use.
        let bytes = capacity
            .checked_mul(size_of::<T>())
            .ok_or(Error::Overflow)?;
        self.charge::<u8>(bytes.checked_sub(requested_bytes).ok_or(Error::Overflow)?)?;
        Ok(())
    }
}

struct Reservation<'a> {
    budget: &'a mut DecodeBudget,
    available: usize,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        self.budget.remaining = self.available;
    }
}

/// Callback-owned clones require separate admission beyond this borrowed event.
pub(crate) fn with_decoded_event<R>(
    bytes: &[u8],
    limits: Limits,
    budget: &mut DecodeBudget,
    visit: impl FnOnce(&Event, usize) -> R,
) -> Result<R> {
    let available = budget.remaining;
    let reservation = Reservation { budget, available };
    let event = decode_with_budget(bytes, limits, reservation.budget)?;
    let retained = available
        .checked_sub(reservation.budget.remaining)
        .ok_or(Error::Overflow)?;
    let result = visit(&event, retained);
    drop(event);
    Ok(result)
}

pub(crate) fn encode_event(event: &Event, limits: Limits) -> Result<Vec<u8>> {
    let budget = payload_budget(limits)?;
    let mut size = 0_usize;
    // Validate and measure before reserving, using the same field order for both passes.
    put_event(event, &mut |part| {
        size = size.checked_add(part.len()).ok_or(Error::Overflow)?;
        if u64::try_from(size).map_err(|_| Error::Overflow)? > budget {
            return Err(Error::LimitExceeded);
        }
        Ok(())
    })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::Allocation)?;
    put_event(event, &mut |part| {
        bytes.extend_from_slice(part);
        Ok(())
    })?;
    Ok(bytes)
}

pub(crate) fn decode_event(bytes: &[u8], limits: Limits) -> Result<Event> {
    decode_with_budget(bytes, limits, &mut DecodeBudget::new(usize::MAX))
}

fn decode_with_budget(bytes: &[u8], limits: Limits, budget: &mut DecodeBudget) -> Result<Event> {
    let wire_budget = payload_budget(limits)?;
    if u64::try_from(bytes.len()).map_err(|_| Error::Overflow)? > wire_budget {
        return Err(Error::LimitExceeded);
    }
    let mut input = Reader { bytes, budget };
    let version = input.byte()?;
    if version != VERSION {
        return Err(Error::UnsupportedVersion(u16::from(version)));
    }
    let event = match input.byte()? {
        1 => Event::Relation {
            key: input.key()?,
            schema: input.name()?,
            name: input.name()?,
            columns: input.vector(13, |input| {
                Ok(Column {
                    name: input.name()?,
                    type_oid: input.u32()?,
                    type_mod: i32::from_le_bytes(input.fixed()?),
                    key: input.boolean()?,
                })
            })?,
            replica_identity: input.byte()?,
        },
        2 => Event::Insert {
            relation: input.key()?,
            values: input.values()?,
        },
        3 => Event::Update {
            relation: input.key()?,
            old: if input.boolean()? {
                Some(input.values()?)
            } else {
                None
            },
            new: input.values()?,
        },
        4 => Event::Delete {
            relation: input.key()?,
            old: input.values()?,
        },
        5 => Event::Truncate {
            relations: input.vector(12, Reader::key)?,
            cascade: input.boolean()?,
            restart_identity: input.boolean()?,
        },
        tag => return Err(Error::UnknownKind(tag)),
    };
    if !input.bytes.is_empty() {
        return Err(Error::InvalidRecord);
    }
    Ok(event)
}

fn payload_budget(limits: Limits) -> Result<u64> {
    limits.validate()?;
    let per_part =
        u64::try_from(limits.max_frame_bytes - HEADER_BYTES).map_err(|_| Error::Overflow)?;
    let parts = per_part
        .checked_mul(u64::from(limits.max_parts))
        .ok_or(Error::Overflow)?;
    Ok(limits.max_event_bytes.min(parts))
}

fn put_event(event: &Event, put: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    put(&[VERSION])?;
    match event {
        Event::Relation {
            key,
            schema,
            name,
            columns,
            replica_identity,
        } => {
            put(&[1])?;
            put_key(*key, put)?;
            put_bytes(schema.as_bytes(), put)?;
            put_bytes(name.as_bytes(), put)?;
            put_len(columns.len(), put)?;
            for column in columns {
                put_bytes(column.name.as_bytes(), put)?;
                put(&column.type_oid.to_le_bytes())?;
                put(&column.type_mod.to_le_bytes())?;
                put(&[u8::from(column.key)])?;
            }
            put(&[*replica_identity])
        }
        Event::Insert { relation, values } => {
            put(&[2])?;
            put_key(*relation, put)?;
            put_values(values, put)
        }
        Event::Update { relation, old, new } => {
            put(&[3])?;
            put_key(*relation, put)?;
            put(&[u8::from(old.is_some())])?;
            if let Some(old) = old {
                put_values(old, put)?;
            }
            put_values(new, put)
        }
        Event::Delete { relation, old } => {
            put(&[4])?;
            put_key(*relation, put)?;
            put_values(old, put)
        }
        Event::Truncate {
            relations,
            cascade,
            restart_identity,
        } => {
            put(&[5])?;
            put_len(relations.len(), put)?;
            for relation in relations {
                put_key(*relation, put)?;
            }
            put(&[u8::from(*cascade), u8::from(*restart_identity)])
        }
    }
}

fn put_key(key: RelationKey, put: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    put(&key.oid.to_le_bytes())?;
    put(&key.generation.to_le_bytes())
}

fn put_len(length: usize, put: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    put(&u32::try_from(length)
        .map_err(|_| Error::Overflow)?
        .to_le_bytes())
}

fn put_bytes(bytes: &[u8], put: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    put_len(bytes.len(), put)?;
    put(bytes)
}

fn put_values(values: &[Value], put: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
    put_len(values.len(), put)?;
    for value in values {
        match value {
            Value::Null => put(&[0])?,
            Value::UnchangedToast => put(&[1])?,
            Value::Text(bytes) => {
                put(&[2])?;
                put_bytes(bytes, put)?;
            }
            Value::Binary(bytes) => {
                put(&[3])?;
                put_bytes(bytes, put)?;
            }
        }
    }
    Ok(())
}

struct Reader<'a, 'b> {
    bytes: &'a [u8],
    budget: &'b mut DecodeBudget,
}

impl<'a> Reader<'a, '_> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let (field, rest) = self.bytes.split_at_checked(length).ok_or(Error::Gap)?;
        self.bytes = rest;
        Ok(field)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("checked field width"))
    }

    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }

    fn boolean(&mut self) -> Result<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::InvalidRecord),
        }
    }

    fn key(&mut self) -> Result<RelationKey> {
        Ok(RelationKey {
            oid: self.u32()?,
            generation: u64::from_le_bytes(self.fixed()?),
        })
    }

    fn length(&mut self) -> Result<usize> {
        usize::try_from(self.u32()?).map_err(|_| Error::Overflow)
    }

    fn name(&mut self) -> Result<String> {
        let length = self.length()?;
        let name = std::str::from_utf8(self.take(length)?).map_err(|_| Error::InvalidRecord)?;
        let requested = self.budget.charge::<u8>(name.len())?;
        let mut owned = String::new();
        owned
            .try_reserve_exact(name.len())
            .map_err(|_| Error::Allocation)?;
        self.budget.settle::<u8>(requested, owned.capacity())?;
        owned.push_str(name);
        Ok(owned)
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let length = self.length()?;
        let value = self.take(length)?;
        let requested = self.budget.charge::<u8>(value.len())?;
        let mut owned = Vec::new();
        owned
            .try_reserve_exact(value.len())
            .map_err(|_| Error::Allocation)?;
        self.budget.settle::<u8>(requested, owned.capacity())?;
        owned.extend_from_slice(value);
        Ok(owned)
    }

    fn vector<T>(
        &mut self,
        minimum_bytes: usize,
        mut read: impl FnMut(&mut Self) -> Result<T>,
    ) -> Result<Vec<T>> {
        let count = self.length()?;
        // A count cannot authorize allocation beyond the items present in this bounded payload.
        if count > self.bytes.len() / minimum_bytes {
            return Err(Error::InvalidRecord);
        }
        let requested = self.budget.charge::<T>(count)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| Error::Allocation)?;
        self.budget.settle::<T>(requested, values.capacity())?;
        for _ in 0..count {
            values.push(read(self)?);
        }
        Ok(values)
    }

    fn values(&mut self) -> Result<Vec<Value>> {
        self.vector(1, |input| match input.byte()? {
            0 => Ok(Value::Null),
            1 => Ok(Value::UnchangedToast),
            2 => Ok(Value::Text(input.bytes()?)),
            3 => Ok(Value::Binary(input.bytes()?)),
            tag => Err(Error::UnknownKind(tag)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::contracts::{Column, RelationKey, Value};

    fn retained_bytes(event: &Event) -> usize {
        fn values_bytes(values: &[Value], capacity: usize) -> usize {
            capacity * size_of::<Value>()
                + values
                    .iter()
                    .map(|value| match value {
                        Value::Text(bytes) | Value::Binary(bytes) => bytes.capacity(),
                        Value::Null | Value::UnchangedToast => 0,
                    })
                    .sum::<usize>()
        }
        match event {
            Event::Relation {
                schema,
                name,
                columns,
                ..
            } => {
                columns.capacity() * size_of::<Column>()
                    + schema.capacity()
                    + name.capacity()
                    + columns
                        .iter()
                        .map(|column| column.name.capacity())
                        .sum::<usize>()
            }
            Event::Insert { values, .. } => values_bytes(values, values.capacity()),
            Event::Update { old, new, .. } => {
                old.as_ref()
                    .map_or(0, |old| values_bytes(old, old.capacity()))
                    + values_bytes(new, new.capacity())
            }
            Event::Delete { old, .. } => values_bytes(old, old.capacity()),
            Event::Truncate { relations, .. } => relations.capacity() * size_of::<RelationKey>(),
        }
    }

    fn decode_both(bytes: &[u8], limits: Limits) -> Result<Event> {
        let decoded = decode_event(bytes, limits);
        let mut budget = DecodeBudget::new(usize::MAX);
        let bounded = with_decoded_event(bytes, limits, &mut budget, |event, charged| {
            assert_eq!(decoded.as_ref(), Ok(event));
            assert_eq!(charged, retained_bytes(event));
        });
        assert_eq!(bounded, decoded.as_ref().map(|_| ()).map_err(Clone::clone));
        assert_eq!(budget.remaining_bytes(), usize::MAX);
        decoded
    }

    fn key() -> RelationKey {
        RelationKey {
            oid: 0x1234_5678,
            generation: 0x0102_0304_0506_0708,
        }
    }

    fn fixtures() -> [(Event, &'static str); 6] {
        // Independent Python struct fixtures fix field order, widths, and signedness.
        [
            (
                Event::Relation {
                    key: key(),
                    schema: "s\"\\\nα".into(),
                    name: "T .".into(),
                    columns: vec![
                        Column {
                            name: "id".into(),
                            type_oid: 23,
                            type_mod: -1,
                            key: true,
                        },
                        Column {
                            name: "blob".into(),
                            type_oid: 17,
                            type_mod: -42,
                            key: false,
                        },
                    ],
                    replica_identity: b'd',
                },
                concat!(
                    "01017856341208070605040302010600000073225c0aceb10300000054202e",
                    "0200000002000000696417000000ffffffff0104000000626c6f6211000000d6ffffff0064"
                ),
            ),
            (
                Event::Insert {
                    relation: key(),
                    values: vec![
                        Value::Null,
                        Value::UnchangedToast,
                        Value::Text(vec![]),
                        Value::Text(vec![b'a', 0, 255, b'\n', b'\\']),
                        Value::Binary(vec![0, 1, 254, 255]),
                    ],
                },
                "0102785634120807060504030201050000000001020000000002050000006100ff0a5c03040000000001feff",
            ),
            (
                Event::Update {
                    relation: key(),
                    old: None,
                    new: vec![Value::Text(b"7".to_vec())],
                },
                "01037856341208070605040302010001000000020100000037",
            ),
            (
                Event::Update {
                    relation: key(),
                    old: Some(vec![Value::Null, Value::Binary(vec![])]),
                    new: vec![],
                },
                "0103785634120807060504030201010200000000030000000000000000",
            ),
            (
                Event::Delete {
                    relation: key(),
                    old: vec![Value::Null, Value::Text(vec![])],
                },
                "010478563412080706050403020102000000000200000000",
            ),
            (
                Event::Truncate {
                    relations: vec![
                        key(),
                        RelationKey {
                            oid: 9,
                            generation: 0,
                        },
                    ],
                    cascade: true,
                    restart_identity: false,
                },
                "0105020000007856341208070605040302010900000000000000000000000100",
            ),
        ]
    }

    fn from_hex(hex: &str) -> Vec<u8> {
        assert!(hex.len().is_multiple_of(2));
        hex.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn assert_golden(index: usize) {
        let (event, hex) = &fixtures()[index];
        let bytes = from_hex(hex);
        assert_eq!(encode_event(event, Limits::default()).unwrap(), bytes);
        assert_eq!(decode_both(&bytes, Limits::default()).unwrap(), *event);
    }

    #[test]
    fn relation_matches_independent_golden_with_signed_typmods_and_utf8_names() {
        assert_golden(0);
    }

    #[test]
    fn insert_matches_independent_golden_without_converting_value_bytes() {
        assert_golden(1);
    }

    #[test]
    fn update_without_old_image_matches_independent_golden() {
        assert_golden(2);
    }

    #[test]
    fn update_with_old_image_matches_independent_golden() {
        assert_golden(3);
    }

    #[test]
    fn delete_matches_independent_golden() {
        assert_golden(4);
    }

    #[test]
    fn truncate_matches_independent_golden() {
        assert_golden(5);
    }

    #[test]
    fn every_incomplete_field_is_rejected() {
        for (_, hex) in fixtures() {
            let bytes = from_hex(hex);
            for end in 0..bytes.len() {
                assert!(
                    decode_both(&bytes[..end], Limits::default()).is_err(),
                    "tag {} accepted prefix {end}",
                    bytes[1]
                );
            }
        }
    }

    #[test]
    fn trailing_bytes_are_rejected_for_every_variant() {
        for (_, hex) in fixtures() {
            let mut bytes = from_hex(hex);
            bytes.push(0);
            assert_eq!(
                decode_both(&bytes, Limits::default()),
                Err(Error::InvalidRecord)
            );
        }
    }

    #[test]
    fn unknown_payload_versions_are_rejected() {
        for version in [0, 2, 255] {
            let mut bytes = from_hex(fixtures()[0].1);
            bytes[0] = version;
            assert_eq!(
                decode_both(&bytes, Limits::default()),
                Err(Error::UnsupportedVersion(u16::from(version)))
            );
        }
    }

    #[test]
    fn reserved_event_tags_are_rejected() {
        for tag in [0, 6, 255] {
            assert_eq!(
                decode_both(&[1, tag], Limits::default()),
                Err(Error::UnknownKind(tag))
            );
        }
    }

    #[test]
    fn reserved_value_tags_are_rejected() {
        for tag in [4, 255] {
            let mut bytes = from_hex(fixtures()[1].1);
            bytes[18] = tag;
            assert_eq!(
                decode_both(&bytes, Limits::default()),
                Err(Error::UnknownKind(tag))
            );
        }
    }

    #[test]
    fn invalid_utf8_is_rejected_in_every_identifier_role() {
        for offset in [18, 28, 39] {
            let mut bytes = from_hex(fixtures()[0].1);
            bytes[offset] = 255;
            assert_eq!(
                decode_both(&bytes, Limits::default()),
                Err(Error::InvalidRecord)
            );
        }
    }

    #[test]
    fn boolean_fields_accept_only_zero_or_one() {
        for (index, offsets) in [(0, [49, 66]), (5, [30, 31])] {
            for offset in offsets {
                for invalid in [2, 255] {
                    let mut bytes = from_hex(fixtures()[index].1);
                    bytes[offset] = invalid;
                    assert_eq!(
                        decode_both(&bytes, Limits::default()),
                        Err(Error::InvalidRecord)
                    );
                }
            }
        }
    }

    #[test]
    fn old_image_presence_accepts_only_zero_or_one() {
        for index in [2, 3] {
            for invalid in [2, 255] {
                let mut bytes = from_hex(fixtures()[index].1);
                bytes[14] = invalid;
                assert_eq!(
                    decode_both(&bytes, Limits::default()),
                    Err(Error::InvalidRecord)
                );
            }
        }
    }

    #[test]
    fn impossible_counts_fail_before_reserving_vectors() {
        for (index, offset) in [(0, 31), (1, 14), (2, 15), (3, 15), (4, 14), (5, 2)] {
            let mut bytes = from_hex(fixtures()[index].1);
            bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert_eq!(
                decode_both(&bytes, Limits::default()),
                Err(Error::InvalidRecord),
                "fixture {index}"
            );
        }
    }

    #[test]
    fn impossible_byte_lengths_fail_before_reserving_bytes() {
        for (index, offset) in [(0, 14), (0, 24), (0, 35), (1, 21), (3, 21)] {
            let mut bytes = from_hex(fixtures()[index].1);
            bytes[offset..offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            assert_eq!(decode_both(&bytes, Limits::default()), Err(Error::Gap));
        }
    }

    #[test]
    fn absent_and_present_empty_old_images_remain_distinct() {
        let absent = Event::Update {
            relation: key(),
            old: None,
            new: vec![],
        };
        let present = Event::Update {
            relation: key(),
            old: Some(vec![]),
            new: vec![],
        };
        let absent_bytes = encode_event(&absent, Limits::default()).unwrap();
        let present_bytes = encode_event(&present, Limits::default()).unwrap();
        assert_ne!(absent_bytes, present_bytes);
        assert_eq!(
            decode_both(&absent_bytes, Limits::default()).unwrap(),
            absent
        );
        assert_eq!(
            decode_both(&present_bytes, Limits::default()).unwrap(),
            present
        );
        for bytes in [&absent_bytes, &present_bytes] {
            let mut budget = DecodeBudget::new(0);
            assert_eq!(
                with_decoded_event(bytes, Limits::default(), &mut budget, |_, charged| charged),
                Ok(0)
            );
        }
    }

    #[test]
    fn empty_vectors_keep_their_declared_wire_shape() {
        let relation = RelationKey {
            oid: 0,
            generation: 0,
        };
        for event in [
            Event::Relation {
                key: relation,
                schema: String::new(),
                name: String::new(),
                columns: vec![],
                replica_identity: 0,
            },
            Event::Insert {
                relation,
                values: vec![],
            },
            Event::Delete {
                relation,
                old: vec![],
            },
            Event::Truncate {
                relations: vec![],
                cascade: false,
                restart_identity: true,
            },
        ] {
            let bytes = encode_event(&event, Limits::default()).unwrap();
            assert_eq!(decode_both(&bytes, Limits::default()).unwrap(), event);
            let mut budget = DecodeBudget::new(0);
            assert_eq!(
                with_decoded_event(
                    &bytes,
                    Limits::default(),
                    &mut budget,
                    |decoded, charged| {
                        assert_eq!(decoded, &event);
                        charged
                    }
                ),
                Ok(0)
            );
        }
    }

    #[test]
    fn decoded_budget_rejects_typed_values_before_visiting() {
        let count = 65_536_u32;
        for tag in [0, 1] {
            let mut bytes = from_hex(fixtures()[1].1)[..18].to_vec();
            bytes[14..18].copy_from_slice(&count.to_le_bytes());
            bytes.resize(18 + count as usize, tag);
            let mut budget = DecodeBudget::new(bytes.len());
            let mut visited = false;
            assert_eq!(
                with_decoded_event(&bytes, Limits::default(), &mut budget, |_, _| visited =
                    true),
                Err(Error::Allocation)
            );
            assert!(!visited);
            assert_eq!(budget.remaining_bytes(), bytes.len());
        }
    }

    #[test]
    fn decoded_budget_charges_all_nested_bytes_and_both_update_images() {
        let minimums = [
            2 * size_of::<Column>() + 15,
            5 * size_of::<Value>() + 9,
            size_of::<Value>() + 1,
            2 * size_of::<Value>(),
            2 * size_of::<Value>(),
            2 * size_of::<RelationKey>(),
        ];
        for ((_, hex), minimum) in fixtures().into_iter().zip(minimums) {
            let bytes = from_hex(hex);
            let mut budget = DecodeBudget::new(minimum - 1);
            assert_eq!(
                with_decoded_event(&bytes, Limits::default(), &mut budget, |_, _| panic!(
                    "underbudget event reached sink"
                )),
                Err(Error::Allocation)
            );
            assert_eq!(budget.remaining_bytes(), minimum - 1);
        }
        let update = Event::Update {
            relation: key(),
            old: Some(vec![Value::Null]),
            new: vec![Value::Text(b"raw".to_vec())],
        };
        let bytes = encode_event(&update, Limits::default()).unwrap();
        let available = 2 * size_of::<Value>() + 2;
        let mut budget = DecodeBudget::new(available);
        assert_eq!(
            with_decoded_event(&bytes, Limits::default(), &mut budget, |_, _| ()),
            Err(Error::Allocation)
        );
        assert_eq!(budget.remaining_bytes(), available);
    }

    #[test]
    fn decoded_budget_admits_exact_capacity_and_refuses_excess_or_overflow() {
        let width = size_of::<Value>();
        for (available, outcome, remaining) in [
            (4 * width, Ok(()), 0),
            (4 * width - 1, Err(Error::Allocation), 2 * width - 1),
        ] {
            let mut budget = DecodeBudget::new(available);
            assert_eq!(budget.charge::<Value>(2), Ok(2 * width));
            assert_eq!(budget.settle::<Value>(2 * width, 4), outcome);
            assert_eq!(budget.remaining_bytes(), remaining);
        }
        let mut budget = DecodeBudget::new(usize::MAX);
        assert_eq!(budget.charge::<Value>(usize::MAX), Err(Error::Overflow));
        assert_eq!(budget.settle::<Value>(0, usize::MAX), Err(Error::Overflow));
        assert_eq!(budget.remaining_bytes(), usize::MAX);
        assert_eq!(budget.charge::<u8>(usize::MAX), Ok(usize::MAX));
        assert_eq!(budget.charge::<u8>(1), Err(Error::Allocation));
        assert_eq!(budget.remaining_bytes(), 0);
    }

    #[test]
    fn sink_error_and_unwind_release_the_decoded_budget() {
        let bytes = from_hex(fixtures()[1].1);
        let mut budget = DecodeBudget::new(usize::MAX);
        assert_eq!(
            with_decoded_event(&bytes, Limits::default(), &mut budget, |_, _| Err::<(), _>(
                "sink failed"
            )),
            Ok(Err("sink failed"))
        );
        assert_eq!(budget.remaining_bytes(), usize::MAX);
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_decoded_event::<()>(&bytes, Limits::default(), &mut budget, |_, _| {
                panic!("sink panicked")
            })
        }));
        assert!(panic.is_err());
        assert_eq!(budget.remaining_bytes(), usize::MAX);
        assert_eq!(
            with_decoded_event(&bytes, Limits::default(), &mut budget, |event, charged| {
                assert_eq!(charged, retained_bytes(event));
            }),
            Ok(())
        );
        assert_eq!(budget.remaining_bytes(), usize::MAX);
    }

    #[test]
    fn invalid_shared_limits_fail_before_parsing_or_encoding() {
        for limits in [
            Limits {
                max_frame_bytes: 127,
                ..Limits::default()
            },
            Limits {
                max_frame_bytes: 64 * 1024 * 1024 + 1,
                ..Limits::default()
            },
            Limits {
                max_event_bytes: 0,
                ..Limits::default()
            },
            Limits {
                max_event_bytes: 1024 * 1024 * 1024 + 1,
                ..Limits::default()
            },
            Limits {
                max_parts: 0,
                ..Limits::default()
            },
            Limits {
                max_parts: 1_048_577,
                ..Limits::default()
            },
        ] {
            assert_eq!(
                encode_event(&fixtures()[0].0, limits),
                Err(Error::InvalidLimits)
            );
            assert_eq!(decode_both(&[], limits), Err(Error::InvalidLimits));
        }
    }

    #[test]
    fn exact_event_limit_is_inclusive_and_smaller_limit_fails() {
        let (event, hex) = &fixtures()[1];
        let bytes = from_hex(hex);
        let limits = Limits {
            max_event_bytes: 44,
            max_frame_bytes: 128,
            max_parts: 2,
        };
        assert_eq!(encode_event(event, limits).unwrap(), bytes);
        assert_eq!(decode_both(&bytes, limits).unwrap(), *event);
        let smaller = Limits {
            max_event_bytes: 43,
            ..limits
        };
        assert_eq!(encode_event(event, smaller), Err(Error::LimitExceeded));
        assert_eq!(decode_both(&bytes, smaller), Err(Error::LimitExceeded));
    }

    #[test]
    fn part_budget_counts_frame_headers_and_includes_the_last_part() {
        let limits = Limits {
            max_frame_bytes: 128,
            max_parts: 1,
            ..Limits::default()
        };
        let exact = Event::Insert {
            relation: key(),
            values: vec![Value::Binary(vec![0; 17])],
        };
        let bytes = encode_event(&exact, limits).unwrap();
        assert_eq!(bytes.len(), 40);
        assert_eq!(decode_both(&bytes, limits).unwrap(), exact);
        let larger = Event::Insert {
            relation: key(),
            values: vec![Value::Binary(vec![0; 18])],
        };
        assert_eq!(encode_event(&larger, limits), Err(Error::LimitExceeded));
        let two_parts = Limits {
            max_parts: 2,
            ..limits
        };
        let bytes = encode_event(&larger, two_parts).unwrap();
        assert_eq!(decode_both(&bytes, limits), Err(Error::LimitExceeded));
        assert_eq!(decode_both(&bytes, two_parts).unwrap(), larger);
    }

    #[test]
    fn oversized_payload_is_rejected_before_its_version_is_parsed() {
        let limits = Limits {
            max_event_bytes: 16,
            ..Limits::default()
        };
        assert_eq!(decode_both(&[255; 17], limits), Err(Error::LimitExceeded));
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn oversized_lengths_cannot_wrap_a_u32_or_emit_a_prefix() {
        let mut bytes = Vec::new();
        let result = put_len(u32::MAX as usize + 1, &mut |part| {
            bytes.extend_from_slice(part);
            Ok(())
        });
        assert_eq!(result, Err(Error::Overflow));
        assert!(bytes.is_empty());
        put_len(u32::MAX as usize, &mut |part| {
            bytes.extend_from_slice(part);
            Ok(())
        })
        .unwrap();
        assert_eq!(bytes, [255; 4]);
    }

    #[test]
    fn mutated_payloads_are_rejected_or_have_one_canonical_encoding() {
        for (_, hex) in fixtures() {
            let mut bytes = from_hex(hex);
            for index in 0..bytes.len() {
                let original = bytes[index];
                for replacement in 0..=u8::MAX {
                    bytes[index] = replacement;
                    if let Ok(event) = decode_both(&bytes, Limits::default()) {
                        assert_eq!(encode_event(&event, Limits::default()).unwrap(), bytes);
                    }
                }
                bytes[index] = original;
            }
        }
    }

    #[test]
    fn value_debug_omits_row_bytes() {
        let secret = b"private-row".to_vec();
        assert_eq!(
            format!("{:?}", Value::Text(secret.clone())),
            "Text { bytes: 11 }"
        );
        assert_eq!(
            format!("{:?}", Value::Binary(secret)),
            "Binary { bytes: 11 }"
        );
    }
}
