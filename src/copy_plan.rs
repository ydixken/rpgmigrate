//! Plan references qualify immutable metadata, not source sessions or target progress.

use std::ffi::CStr;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crc32fast::Hasher;
use rusqlite::{Connection, OpenFlags, Params, Row, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::cdc::codec::{HEADER_BYTES, Limits};
use crate::cdc::contracts::{Event, RelationKey};
use crate::cdc::event_codec::{self, DecodeBudget};
use crate::contracts::{
    COPY_PLAN_REFERENCE_VERSION, CopyFormat, CopyPlanRef, CopyTable, Error, ErrorKind, IoControl,
    MAX_SNAPSHOT_TOKEN_BYTES, QualifiedTable, Result,
};
use crate::identity::SourceEndLsn;
use crate::state::{
    CopySnapshotIntent, NativePlanPin, NativeState, SourceSlotBinding, SourceSlotReceipt,
};

const MAX_HEADER_BYTES: usize = 16_384;
const MAX_NAME_BYTES: usize = 1024;
const MAX_COLUMNS: usize = 4096;
const DECODE_BYTES: usize = 4 * 1024 * 1024;
const CACHE_BYTES: i64 = 4 * 1024 * 1024;
const LIMITS: Limits = Limits {
    max_frame_bytes: 1_048_576,
    max_event_bytes: 1_048_576,
    max_parts: 1,
};
const MAX_RELATION_BYTES: usize = LIMITS.max_frame_bytes - HEADER_BYTES;
const SQLITE_SOURCE_ID: &CStr =
    c"2026-10-16 14:45:20 555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8e6ca76";

const SEAL_DDL: &str = "CREATE TABLE seal(singleton INTEGER PRIMARY KEY CHECK(singleton=1),header BLOB NOT NULL,table_count BLOB NOT NULL CHECK(length(table_count)=8),logical_bytes BLOB NOT NULL CHECK(length(logical_bytes)=8),crc32 BLOB NOT NULL CHECK(length(crc32)=4)) STRICT";
const RECORDS_DDL: &str = "CREATE TABLE records(ordinal INTEGER PRIMARY KEY CHECK(ordinal>0),relation_oid INTEGER NOT NULL CHECK(relation_oid BETWEEN 1 AND 4294967295),catalog_epoch BLOB NOT NULL CHECK(length(catalog_epoch)=8),schema_name BLOB NOT NULL,table_name BLOB NOT NULL,copy_format INTEGER NOT NULL CHECK(copy_format IN (0,1)),relation_payload BLOB NOT NULL,record_crc32 BLOB NOT NULL CHECK(length(record_crc32)=4)) STRICT";
const RELATION_INDEX_DDL: &str =
    "CREATE UNIQUE INDEX records_by_relation ON records(relation_oid,catalog_epoch)";
const NAME_INDEX_DDL: &str =
    "CREATE UNIQUE INDEX records_by_name ON records(schema_name,table_name)";
const NAME_LOOKUP: &str = "SELECT ordinal FROM main.records INDEXED BY records_by_name WHERE schema_name=?1 AND table_name=?2 LIMIT 2";
const RELATION_LOOKUP: &str = "SELECT ordinal FROM main.records INDEXED BY records_by_relation WHERE relation_oid=?1 AND catalog_epoch=?2 LIMIT 2";
const RECORD_SELECT: &str = "SELECT ordinal,relation_oid,CASE WHEN typeof(catalog_epoch)='blob' AND length(catalog_epoch)=8 THEN catalog_epoch END,CASE WHEN typeof(schema_name)='blob' AND length(schema_name) BETWEEN 1 AND 1024 THEN schema_name END,CASE WHEN typeof(table_name)='blob' AND length(table_name) BETWEEN 1 AND 1024 THEN table_name END,copy_format,CASE WHEN typeof(relation_payload)='blob' AND length(relation_payload) BETWEEN 1 AND 1048488 THEN relation_payload END,CASE WHEN typeof(record_crc32)='blob' AND length(record_crc32)=4 THEN record_crc32 END FROM main.records NOT INDEXED";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PlanHeader {
    reference_version: u32,
    binding: SourceSlotBinding,
    snapshot_token: String,
    consistent_point: SourceEndLsn,
    plan_nonce: [u8; 16],
    buffer_bytes: u64,
}

impl PlanHeader {
    fn validate(&self) -> Result<usize> {
        SourceSlotBinding::new(
            self.binding.stream(),
            self.binding.slot_name().into(),
            self.binding.plugin(),
            self.binding.catalog_epoch(),
        )?;
        if self.reference_version != COPY_PLAN_REFERENCE_VERSION
            || self.plan_nonce == [0; 16]
            || self.consistent_point.0 == 0
            || self.snapshot_token.is_empty()
            || self.snapshot_token.len() > MAX_SNAPSHOT_TOKEN_BYTES
            || self.snapshot_token.contains('\0')
            || self.buffer_bytes == 0
        {
            return Err(bad("invalid header identity, snapshot or buffer size"));
        }
        usize::try_from(self.buffer_bytes).map_err(|_| bad("buffer size exceeds this platform"))
    }

    fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| bad("cannot encode plan header"))?;
        if bytes.len() > MAX_HEADER_BYTES {
            return Err(bad("header exceeds its byte limit"));
        }
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > MAX_HEADER_BYTES {
            return Err(bad("header exceeds its byte limit"));
        }
        let header: Self = serde_json::from_slice(bytes).map_err(|_| bad("invalid plan header"))?;
        if header.encode()? != bytes {
            return Err(bad("header bytes are not canonical"));
        }
        Ok(header)
    }
}

pub(crate) struct PlanEntry {
    format: CopyFormat,
    relation: Event,
}

impl PlanEntry {
    pub(crate) fn checked(table: CopyTable, relation: Event) -> Result<Self> {
        validate_relation(&relation)?;
        let Event::Relation {
            schema,
            name,
            columns,
            ..
        } = &relation
        else {
            return Err(bad("plan entry is not a Relation"));
        };
        if table.name.schema != *schema
            || table.name.table != *name
            || table.columns.len() != columns.len()
            || table
                .columns
                .iter()
                .zip(columns)
                .any(|(name, column)| name != &column.name)
        {
            return Err(bad(
                "CopyTable differs from canonical Relation names or column order",
            ));
        }
        Ok(Self {
            format: table.format,
            relation,
        })
    }
}

pub(crate) fn publish_copy_plan(
    native: &mut NativeState,
    source: &SourceSlotReceipt,
    plan_nonce: [u8; 16],
    buffer_bytes: usize,
    entries: impl Iterator<Item = Result<PlanEntry>>,
    io: &IoControl,
) -> Result<CopyPlanRef> {
    check_io(io)?;
    native.checked_source_binding(&source.intent.binding)?;
    if source.intent.nonce == [0; 16] {
        return Err(bad("source receipt has no creation nonce"));
    }
    let header = PlanHeader {
        reference_version: COPY_PLAN_REFERENCE_VERSION,
        binding: source.intent.binding.clone(),
        snapshot_token: source.snapshot_token.clone(),
        consistent_point: source.consistent_point,
        plan_nonce,
        buffer_bytes: u64::try_from(buffer_bytes).map_err(|_| bad("buffer size overflow"))?,
    };
    let header_bytes = header.encode()?;
    check_sqlite()?;
    let draft = native.create_plan_file(plan_nonce)?;
    let mut connection =
        Connection::open_with_flags(draft.checked_path()?, write_flags()).map_err(sqlite_error)?;
    draft.checked_path()?;
    configure(&connection, true)?;
    let mut checksum = header_checksum(&header_bytes)?;
    let mut count = 0_u64;
    let mut logical_bytes = 0_u64;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(sqlite_error)?;
    for ddl in [SEAL_DDL, RECORDS_DDL, RELATION_INDEX_DDL, NAME_INDEX_DDL] {
        transaction.execute_batch(ddl).map_err(sqlite_error)?;
    }
    transaction
        .pragma_update(None, "user_version", COPY_PLAN_REFERENCE_VERSION)
        .map_err(sqlite_error)?;
    {
        let mut insert = transaction.prepare("INSERT INTO records(ordinal,relation_oid,catalog_epoch,schema_name,table_name,copy_format,relation_payload,record_crc32) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)")
            .map_err(sqlite_error)?;
        for entry in entries {
            check_io(io)?;
            let entry = entry?;
            validate_relation(&entry.relation)?;
            let Event::Relation {
                key, schema, name, ..
            } = &entry.relation
            else {
                return Err(bad("plan entry is not a Relation"));
            };
            if key.generation != header.binding.catalog_epoch() {
                return Err(bad("Relation epoch differs from confirmed catalog epoch"));
            }
            let payload =
                event_codec::encode_event(&entry.relation, LIMITS).map_err(codec_error)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| bad("table count overflow"))?;
            let ordinal = sqlite_ordinal(count)?;
            logical_bytes = logical_bytes
                .checked_add(payload_length(&payload)?)
                .ok_or_else(|| bad("logical byte count overflow"))?;
            let crc = record_checksum(count, entry.format, &payload)?;
            if insert
                .execute(params![
                    ordinal,
                    i64::from(key.oid),
                    key.generation.to_be_bytes().as_slice(),
                    schema.as_bytes(),
                    name.as_bytes(),
                    format_tag(entry.format),
                    &payload,
                    crc.to_be_bytes().as_slice()
                ])
                .map_err(sqlite_error)?
                != 1
            {
                return Err(bad("insert did not store exactly one record"));
            }
            feed_record(&mut checksum, count, entry.format, &payload)?;
        }
    }
    let reference = finish_checksum(checksum, &header, count, logical_bytes);
    if transaction.execute("INSERT INTO seal(singleton,header,table_count,logical_bytes,crc32) VALUES(1,?1,?2,?3,?4)", params![&header_bytes, count.to_be_bytes().as_slice(), logical_bytes.to_be_bytes().as_slice(), reference.crc32.to_be_bytes().as_slice()])
        .map_err(sqlite_error)? != 1
    {
        return Err(bad("insert did not store exactly one seal"));
    }
    draft.checked_path()?;
    check_io(io)?;
    #[cfg(test)]
    tests::step(
        tests::Step::BeforeCommit,
        Some(&transaction),
        draft.checked_path()?,
    )?;
    transaction
        .commit()
        .map_err(|_| bad("SQLite COMMIT failed; outcome may be uncertain"))?;
    #[cfg(test)]
    tests::step(
        tests::Step::AfterCommit,
        Some(&connection),
        draft.checked_path()?,
    )?;
    connection.execute_batch("BEGIN").map_err(sqlite_error)?;
    verify_schema(&connection)?;
    let (stored, stored_bytes, stored_ref) = load_seal(&connection)?;
    if stored_bytes != header_bytes || stored_ref != reference {
        return Err(bad("stored seal differs from the completed plan"));
    }
    verify_records(&connection, &stored, &stored_bytes, &reference, io)?;
    connection.execute_batch("ROLLBACK").map_err(sqlite_error)?;
    draft.checked_path()?;
    check_io(io)?;
    #[cfg(test)]
    tests::step(
        tests::Step::BeforeClose,
        Some(&connection),
        draft.checked_path()?,
    )?;
    connection.close().map_err(|(connection, _)| {
        #[cfg(test)]
        tests::failed_close(&connection);
        drop(connection);
        bad("SQLite close failed; plan remains unpublished")
    })?;
    #[cfg(test)]
    tests::step(tests::Step::AfterClose, None, draft.checked_path()?)?;
    check_io(io)?;
    draft.publish()?;
    Ok(reference)
}

pub(crate) struct SealedCopyPlan<'n> {
    // SQLite must close before NativePlanPin releases namespace authority.
    connection: Connection,
    pin: NativePlanPin<'n>,
    header: PlanHeader,
    reference: CopyPlanRef,
    buffer_bytes: usize,
}

pub(crate) fn open_sealed_copy_plan<'n>(
    native: &'n NativeState,
    expected: &CopySnapshotIntent,
    io: &IoControl,
) -> Result<SealedCopyPlan<'n>> {
    check_io(io)?;
    if native.checked_copy_snapshot()? != Some(expected.clone()) {
        return Err(bad("no exact persisted COPY snapshot intent"));
    }
    check_sqlite()?;
    let pin = native.open_plan_file(expected.plan.plan_nonce)?;
    let connection =
        Connection::open_with_flags(pin.checked_path()?, read_flags()).map_err(sqlite_error)?;
    pin.check()?;
    if !connection.is_readonly("main").map_err(sqlite_error)? {
        return Err(bad("sealed reader is not read-only"));
    }
    configure(&connection, false)?;
    connection.execute_batch("BEGIN").map_err(sqlite_error)?;
    verify_schema(&connection)?;
    let (header, header_bytes, reference) = load_seal(&connection)?;
    native.checked_source_binding(&header.binding)?;
    if header.binding.stream() != expected.anchor.stream
        || header.snapshot_token != expected.anchor.snapshot_token
        || header.consistent_point != expected.anchor.consistent_point
        || header.plan_nonce != expected.anchor.plan_nonce
        || reference != expected.plan
    {
        return Err(bad("seal differs from persisted snapshot intent"));
    }
    verify_records(&connection, &header, &header_bytes, &reference, io)?;
    pin.check()?;
    check_io(io)?;
    let buffer_bytes = header.validate()?;
    Ok(SealedCopyPlan {
        connection,
        pin,
        header,
        reference,
        buffer_bytes,
    })
}

impl SealedCopyPlan<'_> {
    pub(crate) fn buffer_bytes(&self) -> usize {
        self.buffer_bytes
    }

    fn check(&self, io: &IoControl) -> Result<()> {
        check_io(io)?;
        self.pin.check()?;
        if self.connection.is_autocommit() {
            return Err(bad("sealed read transaction ended"));
        }
        Ok(())
    }

    pub(crate) fn scan_from(
        &mut self,
        after: u64,
        io: &IoControl,
        mut visit: impl FnMut(u64, CopyFormat, &Event) -> Result<()>,
    ) -> Result<()> {
        self.check(io)?;
        if after > self.reference.table_count {
            return Err(bad("scan cursor exceeds the sealed table count"));
        }
        let mut statement = self
            .connection
            .prepare(&format!(
                "{RECORD_SELECT} WHERE ordinal>?1 ORDER BY ordinal"
            ))
            .map_err(sqlite_error)?;
        let mut rows = statement
            .query([sqlite_ordinal(after)?])
            .map_err(sqlite_error)?;
        let mut ordinal = after;
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            check_io(io)?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| bad("scan ordinal overflow"))?;
            let record = Record::read(row)?;
            if record.ordinal != ordinal || ordinal > self.reference.table_count {
                return Err(bad("ordered plan scan has a gap or extra record"));
            }
            checked_record(&record, &self.header, |event| {
                visit(ordinal, record.format, event)
            })?;
        }
        if ordinal != self.reference.table_count {
            return Err(bad("ordered plan scan ended before the seal"));
        }
        self.check(io)
    }

    pub(crate) fn lookup_name(
        &mut self,
        name: &QualifiedTable,
        io: &IoControl,
        visit: impl FnOnce(u64, CopyFormat, &Event) -> Result<()>,
    ) -> Result<()> {
        self.check(io)?;
        validate_name(&name.schema)?;
        validate_name(&name.table)?;
        let ordinal = find_ordinal(
            &self.connection,
            NAME_LOOKUP,
            params![name.schema.as_bytes(), name.table.as_bytes()],
        )?;
        self.read_one(ordinal, io, |ordinal, format, event| {
            let Event::Relation {
                schema,
                name: table,
                ..
            } = event
            else {
                return Err(bad("indexed plan entry is not a Relation"));
            };
            if schema != &name.schema || table != &name.table {
                return Err(bad("name index disagrees with canonical Relation"));
            }
            visit(ordinal, format, event)
        })
    }

    pub(crate) fn lookup_relation(
        &mut self,
        key: RelationKey,
        io: &IoControl,
        visit: impl FnOnce(u64, CopyFormat, &Event) -> Result<()>,
    ) -> Result<()> {
        self.check(io)?;
        if key.oid == 0 || key.generation != self.header.binding.catalog_epoch() {
            return Err(bad(
                "lookup relation identity or epoch differs from the plan",
            ));
        }
        let ordinal = find_ordinal(
            &self.connection,
            RELATION_LOOKUP,
            params![i64::from(key.oid), key.generation.to_be_bytes().as_slice()],
        )?;
        self.read_one(ordinal, io, |ordinal, format, event| {
            let Event::Relation { key: actual, .. } = event else {
                return Err(bad("indexed plan entry is not a Relation"));
            };
            if *actual != key {
                return Err(bad("relation index disagrees with canonical Relation"));
            }
            visit(ordinal, format, event)
        })
    }

    fn read_one(
        &self,
        ordinal: u64,
        io: &IoControl,
        visit: impl FnOnce(u64, CopyFormat, &Event) -> Result<()>,
    ) -> Result<()> {
        if ordinal == 0 || ordinal > self.reference.table_count {
            return Err(bad("index ordinal exceeds the sealed table count"));
        }
        let mut statement = self
            .connection
            .prepare(&format!("{RECORD_SELECT} WHERE ordinal=?1"))
            .map_err(sqlite_error)?;
        let mut rows = statement
            .query([sqlite_ordinal(ordinal)?])
            .map_err(sqlite_error)?;
        let row = rows
            .next()
            .map_err(sqlite_error)?
            .ok_or_else(|| bad("index points to a missing record"))?;
        let record = Record::read(row)?;
        if record.ordinal != ordinal {
            return Err(bad("index points to a different ordinal"));
        }
        checked_record(&record, &self.header, |event| {
            visit(ordinal, record.format, event)
        })?;
        self.check(io)
    }
}

struct Record<'a> {
    ordinal: u64,
    oid: u32,
    epoch: u64,
    schema: &'a [u8],
    table: &'a [u8],
    format: CopyFormat,
    payload: &'a [u8],
    crc32: u32,
}

impl<'a> Record<'a> {
    fn read(row: &'a Row<'_>) -> Result<Self> {
        let ordinal = unsigned(row.get(0).map_err(sqlite_error)?)?;
        let oid = u32::try_from(row.get::<_, i64>(1).map_err(sqlite_error)?)
            .map_err(|_| bad("invalid projected relation OID"))?;
        if ordinal == 0 || oid == 0 {
            return Err(bad("zero projected ordinal or relation OID"));
        }
        let format = match row.get::<_, i64>(5).map_err(sqlite_error)? {
            0 => CopyFormat::Text,
            1 => CopyFormat::Binary,
            _ => return Err(bad("invalid COPY format tag")),
        };
        let payload = blob(row, 6)?;
        if payload.is_empty() || payload.len() > MAX_RELATION_BYTES {
            return Err(bad("Relation payload exceeds its byte limit"));
        }
        Ok(Self {
            ordinal,
            oid,
            epoch: u64::from_be_bytes(fixed(row, 2)?),
            schema: blob(row, 3)?,
            table: blob(row, 4)?,
            format,
            payload,
            crc32: u32::from_be_bytes(fixed(row, 7)?),
        })
    }
}

fn checked_record(
    record: &Record<'_>,
    header: &PlanHeader,
    visit: impl FnOnce(&Event) -> Result<()>,
) -> Result<()> {
    if record.crc32 != record_checksum(record.ordinal, record.format, record.payload)? {
        return Err(bad("record CRC differs"));
    }
    #[cfg(test)]
    tests::DECODES.with(|count| count.set(count.get() + 1));
    event_codec::with_decoded_event(
        record.payload,
        LIMITS,
        &mut DecodeBudget::new(DECODE_BYTES),
        |event, _| {
            validate_relation(event)?;
            let Event::Relation {
                key, schema, name, ..
            } = event
            else {
                return Err(bad("stored event is not a Relation"));
            };
            if key.oid != record.oid
                || key.generation != record.epoch
                || record.epoch != header.binding.catalog_epoch()
                || schema.as_bytes() != record.schema
                || name.as_bytes() != record.table
            {
                return Err(bad("index projection differs from canonical Relation"));
            }
            if event_codec::encode_event(event, LIMITS).map_err(codec_error)? != record.payload {
                return Err(bad("Relation bytes are not canonical"));
            }
            visit(event)
        },
    )
    .map_err(codec_error)?
}

fn verify_records(
    connection: &Connection,
    header: &PlanHeader,
    header_bytes: &[u8],
    expected: &CopyPlanRef,
    io: &IoControl,
) -> Result<()> {
    let mut checksum = header_checksum(header_bytes)?;
    let mut count = 0_u64;
    let mut logical_bytes = 0_u64;
    let mut statement = connection
        .prepare(&format!("{RECORD_SELECT} ORDER BY ordinal"))
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        check_io(io)?;
        count = count
            .checked_add(1)
            .ok_or_else(|| bad("table count overflow"))?;
        let record = Record::read(row)?;
        if record.ordinal != count || count > expected.table_count {
            return Err(bad("plan ordinals have a gap or extra record"));
        }
        checked_record(&record, header, |_| Ok(()))?;
        if find_ordinal(
            connection,
            NAME_LOOKUP,
            params![record.schema, record.table],
        )? != count
            || find_ordinal(
                connection,
                RELATION_LOOKUP,
                params![i64::from(record.oid), record.epoch.to_be_bytes().as_slice()],
            )? != count
        {
            return Err(bad("lookup index differs from base records"));
        }
        logical_bytes = logical_bytes
            .checked_add(payload_length(record.payload)?)
            .ok_or_else(|| bad("logical byte count overflow"))?;
        feed_record(&mut checksum, count, record.format, record.payload)?;
    }
    if finish_checksum(checksum, header, count, logical_bytes) != *expected {
        return Err(bad("aggregate seal count, logical bytes or CRC differs"));
    }
    check_io(io)
}

fn find_ordinal(connection: &Connection, sql: &str, parameters: impl Params) -> Result<u64> {
    let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
    let mut rows = statement.query(parameters).map_err(sqlite_error)?;
    let ordinal = unsigned(
        rows.next()
            .map_err(sqlite_error)?
            .ok_or_else(|| bad("indexed plan record is missing"))?
            .get(0)
            .map_err(sqlite_error)?,
    )?;
    if ordinal == 0 || rows.next().map_err(sqlite_error)?.is_some() {
        return Err(bad("indexed plan record is zero or duplicated"));
    }
    Ok(ordinal)
}

fn load_seal(connection: &Connection) -> Result<(PlanHeader, Vec<u8>, CopyPlanRef)> {
    let mut statement = connection.prepare("SELECT singleton,CASE WHEN typeof(header)='blob' AND length(header) BETWEEN 1 AND 16384 THEN header END,CASE WHEN typeof(table_count)='blob' AND length(table_count)=8 THEN table_count END,CASE WHEN typeof(logical_bytes)='blob' AND length(logical_bytes)=8 THEN logical_bytes END,CASE WHEN typeof(crc32)='blob' AND length(crc32)=4 THEN crc32 END FROM main.seal LIMIT 2")
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    let row = rows
        .next()
        .map_err(sqlite_error)?
        .ok_or_else(|| bad("seal is missing"))?;
    if row.get::<_, i64>(0).map_err(sqlite_error)? != 1 {
        return Err(bad("seal singleton differs"));
    }
    let raw = blob(row, 1)?;
    let header = PlanHeader::decode(raw)?;
    let bytes = raw.to_vec();
    let reference = CopyPlanRef {
        reference_version: header.reference_version,
        plan_nonce: header.plan_nonce,
        table_count: u64::from_be_bytes(fixed(row, 2)?),
        logical_bytes: u64::from_be_bytes(fixed(row, 3)?),
        crc32: u32::from_be_bytes(fixed(row, 4)?),
    };
    sqlite_ordinal(reference.table_count)?;
    if rows.next().map_err(sqlite_error)?.is_some() {
        return Err(bad("seal is duplicated"));
    }
    Ok((header, bytes, reference))
}

fn verify_schema(connection: &Connection) -> Result<()> {
    let version: u32 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(sqlite_error)?;
    if version != COPY_PLAN_REFERENCE_VERSION {
        return Err(bad("plan schema version differs"));
    }
    let objects = [
        ("table", "seal", "seal", SEAL_DDL),
        ("table", "records", "records", RECORDS_DDL),
        (
            "index",
            "records_by_relation",
            "records",
            RELATION_INDEX_DDL,
        ),
        ("index", "records_by_name", "records", NAME_INDEX_DDL),
    ];
    let mut roots = [0_i64; 4];
    let mut statement = connection.prepare("SELECT type,name,tbl_name,rootpage,CASE WHEN length(sql)<=1024 THEN sql END FROM main.sqlite_schema LIMIT 5")
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let kind = text(row, 0)?;
        let name = text(row, 1)?;
        let table = text(row, 2)?;
        let sql = text(row, 4)?;
        let index = objects
            .iter()
            .position(|object| *object == (kind, name, table, sql))
            .ok_or_else(|| bad("unexpected plan schema object or DDL"))?;
        let root: i64 = row.get(3).map_err(sqlite_error)?;
        if root <= 0 || roots[index] != 0 || roots.contains(&root) {
            return Err(bad("schema root pages are missing or duplicated"));
        }
        roots[index] = root;
    }
    if roots.contains(&0) {
        return Err(bad("required plan schema object is missing"));
    }
    let mut temporary = connection
        .prepare("SELECT 1 FROM temp.sqlite_schema LIMIT 1")
        .map_err(sqlite_error)?;
    if temporary.exists([]).map_err(sqlite_error)? {
        return Err(bad("temporary schema is not empty"));
    }
    verify_table_list(connection)?;
    verify_columns(
        connection,
        "PRAGMA main.table_xinfo(seal)",
        &[
            ("singleton", "INTEGER", false),
            ("header", "BLOB", true),
            ("table_count", "BLOB", true),
            ("logical_bytes", "BLOB", true),
            ("crc32", "BLOB", true),
        ],
    )?;
    verify_columns(
        connection,
        "PRAGMA main.table_xinfo(records)",
        &[
            ("ordinal", "INTEGER", false),
            ("relation_oid", "INTEGER", true),
            ("catalog_epoch", "BLOB", true),
            ("schema_name", "BLOB", true),
            ("table_name", "BLOB", true),
            ("copy_format", "INTEGER", true),
            ("relation_payload", "BLOB", true),
            ("record_crc32", "BLOB", true),
        ],
    )?;
    let mut seal_indexes = connection
        .prepare("PRAGMA main.index_list(seal)")
        .map_err(sqlite_error)?;
    if seal_indexes.exists([]).map_err(sqlite_error)? {
        return Err(bad("seal has an unexpected index"));
    }
    let mut indexes = connection
        .prepare("PRAGMA main.index_list(records)")
        .map_err(sqlite_error)?;
    let mut rows = indexes.query([]).map_err(sqlite_error)?;
    let mut seen = 0_u8;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let bit = match text(row, 1)? {
            "records_by_relation" => 1,
            "records_by_name" => 2,
            _ => return Err(bad("unexpected records index")),
        };
        if seen & bit != 0
            || row.get::<_, i64>(2).map_err(sqlite_error)? != 1
            || text(row, 3)? != "c"
            || row.get::<_, i64>(4).map_err(sqlite_error)? != 0
        {
            return Err(bad("lookup index is duplicated, partial or not unique"));
        }
        seen |= bit;
    }
    if seen != 3 {
        return Err(bad("required lookup index is missing"));
    }
    verify_index(
        connection,
        "PRAGMA main.index_xinfo(records_by_relation)",
        [(1, "relation_oid"), (2, "catalog_epoch")],
    )?;
    verify_index(
        connection,
        "PRAGMA main.index_xinfo(records_by_name)",
        [(3, "schema_name"), (4, "table_name")],
    )
}

fn verify_table_list(connection: &Connection) -> Result<()> {
    let mut statement = connection
        .prepare("PRAGMA main.table_list")
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    let mut seen = 0_u8;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let (bit, columns, strict) = match text(row, 1)? {
            "sqlite_schema" => (1, 5, 0),
            "seal" => (2, 5, 1),
            "records" => (4, 8, 1),
            _ => return Err(bad("unexpected plan table")),
        };
        if seen & bit != 0
            || text(row, 0)? != "main"
            || text(row, 2)? != "table"
            || row.get::<_, i64>(3).map_err(sqlite_error)? != columns
            || row.get::<_, i64>(4).map_err(sqlite_error)? != 0
            || row.get::<_, i64>(5).map_err(sqlite_error)? != strict
        {
            return Err(bad("plan table properties differ"));
        }
        seen |= bit;
    }
    if seen != 7 {
        return Err(bad("plan table is missing"));
    }
    Ok(())
}

fn verify_columns(
    connection: &Connection,
    sql: &str,
    expected: &[(&str, &str, bool)],
) -> Result<()> {
    let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    for (index, (name, kind, not_null)) in expected.iter().enumerate() {
        let row = rows
            .next()
            .map_err(sqlite_error)?
            .ok_or_else(|| bad("plan column is missing"))?;
        let ordinal = i64::try_from(index).map_err(|_| bad("column ordinal overflow"))?;
        if row.get::<_, i64>(0).map_err(sqlite_error)? != ordinal
            || text(row, 1)? != *name
            || text(row, 2)? != *kind
            || row.get::<_, i64>(3).map_err(sqlite_error)? != i64::from(*not_null)
            || !matches!(
                row.get_ref(4).map_err(sqlite_error)?,
                rusqlite::types::ValueRef::Null
            )
            || row.get::<_, i64>(5).map_err(sqlite_error)? != i64::from(index == 0)
            || row.get::<_, i64>(6).map_err(sqlite_error)? != 0
        {
            return Err(bad("plan column properties differ"));
        }
    }
    if rows.next().map_err(sqlite_error)?.is_some() {
        return Err(bad("plan has an extra column"));
    }
    Ok(())
}

fn verify_index(connection: &Connection, sql: &str, keys: [(i64, &str); 2]) -> Result<()> {
    let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    let expected = [
        (keys[0].0, Some(keys[0].1), 1),
        (keys[1].0, Some(keys[1].1), 1),
        (-1, None, 0),
    ];
    for (index, (cid, name, key)) in expected.into_iter().enumerate() {
        let row = rows
            .next()
            .map_err(sqlite_error)?
            .ok_or_else(|| bad("lookup index column is missing"))?;
        let actual_name = row.get_ref(2).map_err(sqlite_error)?;
        let name_matches = match name {
            Some(expected) => actual_name.as_str().ok() == Some(expected),
            None => matches!(actual_name, rusqlite::types::ValueRef::Null),
        };
        if row.get::<_, i64>(0).map_err(sqlite_error)?
            != i64::try_from(index).map_err(|_| bad("index ordinal overflow"))?
            || row.get::<_, i64>(1).map_err(sqlite_error)? != cid
            || !name_matches
            || row.get::<_, i64>(3).map_err(sqlite_error)? != 0
            || text(row, 4)? != "BINARY"
            || row.get::<_, i64>(5).map_err(sqlite_error)? != key
        {
            return Err(bad(
                "lookup index column, expression, collation or order differs",
            ));
        }
    }
    if rows.next().map_err(sqlite_error)?.is_some() {
        return Err(bad("lookup index has an extra column"));
    }
    Ok(())
}

fn configure(connection: &Connection, writer: bool) -> Result<()> {
    let page_size: i64 = connection
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .map_err(sqlite_error)?;
    let cache_pages = CACHE_BYTES
        .checked_div(page_size)
        .filter(|pages| *pages > 0)
        .ok_or_else(|| bad("invalid SQLite page size"))?;
    for (name, expected) in [
        ("cache_size", cache_pages),
        ("mmap_size", 0),
        ("busy_timeout", 0),
        ("trusted_schema", 0),
    ] {
        set_pragma(connection, name, expected)?;
    }
    let mode: String = if writer {
        for (name, value) in [
            ("synchronous", 2),
            ("fullfsync", 1),
            ("checkpoint_fullfsync", 1),
        ] {
            set_pragma(connection, name, value)?;
        }
        connection
            .pragma_update_and_check(None, "journal_mode", "DELETE", |row| row.get(0))
            .map_err(sqlite_error)?
    } else {
        connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(sqlite_error)?
    };
    if mode != "delete" {
        return Err(bad("SQLite plan is not in DELETE journal mode"));
    }
    Ok(())
}

fn set_pragma(connection: &Connection, name: &str, expected: i64) -> Result<()> {
    connection
        .pragma_update(None, name, expected)
        .map_err(sqlite_error)?;
    let actual: i64 = connection
        .pragma_query_value(None, name, |row| row.get(0))
        .map_err(sqlite_error)?;
    if actual != expected {
        return Err(bad("SQLite connection setting differs"));
    }
    Ok(())
}

fn check_sqlite() -> Result<()> {
    // SAFETY: the linked SQLite API returns its static NUL-terminated source identifier.
    let source = unsafe { CStr::from_ptr(rusqlite::ffi::sqlite3_sourceid()) };
    if rusqlite::version_number() != 3_054_001 || source != SQLITE_SOURCE_ID {
        return Err(bad("SQLite runtime version or source identity differs"));
    }
    Ok(())
}

fn write_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
}

fn read_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_ONLY
        | OpenFlags::SQLITE_OPEN_NO_MUTEX
        | OpenFlags::SQLITE_OPEN_NOFOLLOW
}

fn validate_relation(event: &Event) -> Result<()> {
    let Event::Relation {
        key,
        schema,
        name,
        columns,
        ..
    } = event
    else {
        return Err(bad("plan entry is not a Relation"));
    };
    if key.oid == 0 || key.generation == 0 || columns.len() > MAX_COLUMNS {
        return Err(bad("invalid Relation identity or column count"));
    }
    validate_name(schema)?;
    validate_name(name)?;
    for column in columns {
        validate_name(&column.name)?;
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_BYTES || name.contains('\0') {
        return Err(bad("invalid or oversized Relation name"));
    }
    Ok(())
}

fn payload_length(payload: &[u8]) -> Result<u64> {
    u64::try_from(payload.len())
        .map_err(|_| bad("payload size overflow"))?
        .checked_add(1)
        .ok_or_else(|| bad("payload size overflow"))
}

fn header_checksum(header: &[u8]) -> Result<Hasher> {
    let mut checksum = Hasher::new();
    checksum.update(b"RPGPLAN1H");
    checksum.update(
        &u64::try_from(header.len())
            .map_err(|_| bad("header size overflow"))?
            .to_be_bytes(),
    );
    checksum.update(header);
    Ok(checksum)
}

fn feed_record(
    checksum: &mut Hasher,
    ordinal: u64,
    format: CopyFormat,
    payload: &[u8],
) -> Result<()> {
    checksum.update(b"R");
    checksum.update(&ordinal.to_be_bytes());
    checksum.update(&payload_length(payload)?.to_be_bytes());
    checksum.update(&[format_tag(format)]);
    checksum.update(payload);
    Ok(())
}

fn record_checksum(ordinal: u64, format: CopyFormat, payload: &[u8]) -> Result<u32> {
    let mut checksum = Hasher::new();
    feed_record(&mut checksum, ordinal, format, payload)?;
    Ok(checksum.finalize())
}

fn finish_checksum(
    mut checksum: Hasher,
    header: &PlanHeader,
    table_count: u64,
    logical_bytes: u64,
) -> CopyPlanRef {
    checksum.update(b"S");
    checksum.update(&table_count.to_be_bytes());
    checksum.update(&logical_bytes.to_be_bytes());
    CopyPlanRef {
        reference_version: header.reference_version,
        plan_nonce: header.plan_nonce,
        table_count,
        logical_bytes,
        crc32: checksum.finalize(),
    }
}

fn format_tag(format: CopyFormat) -> u8 {
    match format {
        CopyFormat::Text => 0,
        CopyFormat::Binary => 1,
    }
}

fn sqlite_ordinal(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| bad("ordinal exceeds SQLite signed integer width"))
}

fn unsigned(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| bad("negative plan integer"))
}

fn blob<'a>(row: &'a Row<'_>, column: usize) -> Result<&'a [u8]> {
    row.get_ref(column)
        .map_err(sqlite_error)?
        .as_blob()
        .map_err(|_| bad("bounded plan BLOB is absent or has the wrong type"))
}

fn text<'a>(row: &'a Row<'_>, column: usize) -> Result<&'a str> {
    row.get_ref(column)
        .map_err(sqlite_error)?
        .as_str()
        .map_err(|_| bad("plan schema text is absent or has the wrong type"))
}

fn fixed<const N: usize>(row: &Row<'_>, column: usize) -> Result<[u8; N]> {
    blob(row, column)?
        .try_into()
        .map_err(|_| bad("plan integer BLOB width differs"))
}

fn check_io(io: &IoControl) -> Result<()> {
    if io.cancelled.load(Ordering::Relaxed) || Instant::now() >= io.deadline {
        return Err(bad("operation cancelled or deadline expired"));
    }
    Ok(())
}

fn sqlite_error(_: rusqlite::Error) -> Error {
    bad("SQLite plan operation failed")
}
fn codec_error(_: crate::cdc::codec::Error) -> Error {
    bad("invalid or oversized canonical Relation encoding")
}
fn bad(message: &str) -> Error {
    Error {
        kind: ErrorKind::BadState,
        message: format!("copy-plan: {message}"),
    }
}

#[cfg(test)]
mod tests;
