//! Canonical packed CDC records shared by runtime and storage experiments.
//! `encoded_bytes` includes every frame header and the completion descriptor.
//! The caller must prove an unpublished suffix before selecting `Unpublished`.

use std::fmt;
use std::io::{self, Read, Write};

pub use super::identity::{CommitLsn, RunId, SourceEndLsn, SourceId, StreamId, TxId, TxSeq};

pub const HEADER_BYTES: usize = 88;
pub const COMPLETED_BYTES: usize = 128;
pub const VERSION: u16 = 2;
pub const MAGIC: [u8; 4] = *b"RPGS";
const COMMIT_PRESENT: u8 = 0x02;

#[derive(Clone, Eq, PartialEq)]
pub struct Frame {
    pub tx: TxId,
    pub event_ordinal: u64,
    pub part: u32,
    pub last_part: bool,
    pub payload: Vec<u8>,
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Frame")
            .field("tx", &self.tx)
            .field("event_ordinal", &self.event_ordinal)
            .field("part", &self.part)
            .field("last_part", &self.last_part)
            .field("payload_bytes", &self.payload.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletedTx {
    pub tx: TxId,
    pub order: TxSeq,
    // Some source plugins expose only the transaction-end position.
    pub commit: Option<CommitLsn>,
    pub end: SourceEndLsn,
    pub event_count: u64,
    pub encoded_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_event_bytes: u64,
    pub max_parts: u32,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_event_bytes: 64 * 1024 * 1024,
            max_parts: 65_536,
        }
    }
}

impl Limits {
    pub(crate) fn validate(self) -> Result<()> {
        if !(COMPLETED_BYTES..=64 * 1024 * 1024).contains(&self.max_frame_bytes)
            || !(1..=1024 * 1024 * 1024).contains(&self.max_event_bytes)
            || !(1..=1_048_576).contains(&self.max_parts)
        {
            return Err(Error::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TailPolicy {
    Required,
    /// The caller proved this suffix lies beyond the durable publication frontier.
    Unpublished,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Record {
    Frame(Frame),
    Completed(CompletedTx),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadRecord {
    Record(Record),
    Eof,
    IncompleteTail,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Io(io::ErrorKind),
    InvalidLimits,
    UnsupportedVersion(u16),
    UnknownKind(u8),
    CorruptHeader,
    CorruptPayload,
    InvalidRecord,
    LimitExceeded,
    Gap,
    IdentityMismatch,
    OrderMismatch,
    DescriptorMismatch,
    Overflow,
    Allocation,
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(kind) => write!(f, "storage record I/O failed: {kind}"),
            Self::InvalidLimits => f.write_str("storage format limits are invalid"),
            Self::UnsupportedVersion(_) => f.write_str("storage record version is unsupported"),
            Self::UnknownKind(_) => f.write_str("storage record kind is unknown"),
            Self::CorruptHeader => f.write_str("storage record header checksum failed"),
            Self::CorruptPayload => f.write_str("storage record payload checksum failed"),
            Self::InvalidRecord => f.write_str("storage record fields are invalid"),
            Self::LimitExceeded => f.write_str("storage record exceeds a format limit"),
            Self::Gap => f.write_str("required storage record or event part is missing"),
            Self::IdentityMismatch => f.write_str("storage record identity does not match"),
            Self::OrderMismatch => f.write_str("storage record order does not match"),
            Self::DescriptorMismatch => f.write_str("transaction descriptor totals do not match"),
            Self::Overflow => f.write_str("storage record arithmetic overflowed"),
            Self::Allocation => f.write_str("storage record allocation failed"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error.kind())
    }
}

pub fn encode_frame(frame: &Frame, limits: Limits) -> Result<Vec<u8>> {
    check_frame(frame.payload.len(), frame.part, frame.last_part, limits)?;
    let header = encode_header(
        frame.tx,
        1,
        u8::from(frame.last_part),
        frame.event_ordinal,
        frame.part,
        &frame.payload,
    );
    pack(&header, &frame.payload)
}

pub fn decode_frame(bytes: &[u8], limits: Limits) -> Result<Frame> {
    let (header, payload) = decode_slice(bytes, limits)?;
    if header.kind != 1 {
        return Err(Error::InvalidRecord);
    }
    let mut owned = allocate(payload.len())?;
    owned.extend_from_slice(payload);
    Ok(header.frame(owned))
}

pub fn encode_completed(tx: &CompletedTx, limits: Limits) -> Result<Vec<u8>> {
    limits.validate()?;
    check_completed(tx)?;
    let mut payload = [0; 40];
    for (slot, value) in payload.as_chunks_mut::<8>().0.iter_mut().zip([
        tx.order.0,
        tx.commit.map_or(0, |commit| commit.0),
        tx.end.0,
        tx.event_count,
        tx.encoded_bytes,
    ]) {
        slot.copy_from_slice(&value.to_le_bytes());
    }
    let flags = if tx.commit.is_some() {
        COMMIT_PRESENT
    } else {
        0
    };
    pack(&encode_header(tx.tx, 2, flags, 0, 0, &payload), &payload)
}

pub fn decode_completed(bytes: &[u8], limits: Limits) -> Result<CompletedTx> {
    let (header, payload) = decode_slice(bytes, limits)?;
    header.completed(payload)
}

pub fn write_frame<W: Write>(writer: &mut W, frame: &Frame, limits: Limits) -> Result<usize> {
    check_frame(frame.payload.len(), frame.part, frame.last_part, limits)?;
    let header = encode_header(
        frame.tx,
        1,
        u8::from(frame.last_part),
        frame.event_ordinal,
        frame.part,
        &frame.payload,
    );
    writer.write_all(&header)?;
    writer.write_all(&frame.payload)?;
    Ok(HEADER_BYTES + frame.payload.len())
}

pub fn read_record<R: Read>(
    reader: &mut R,
    limits: Limits,
    policy: TailPolicy,
) -> Result<ReadRecord> {
    limits.validate()?;
    let mut raw = [0; HEADER_BYTES];
    let count = read_until_eof(reader, &mut raw)?;
    if count != raw.len() {
        return short_record(policy, count == 0);
    }
    let header = decode_header(&raw, limits)?;
    let mut payload = allocate(header.payload_len)?;
    payload.resize(header.payload_len, 0);
    if read_until_eof(reader, &mut payload)? != payload.len() {
        return short_record(policy, false);
    }
    header.check_payload(&payload)?;
    let record = if header.kind == 1 {
        Record::Frame(header.frame(payload))
    } else {
        Record::Completed(header.completed(&payload)?)
    };
    Ok(ReadRecord::Record(record))
}

fn short_record(policy: TailPolicy, empty: bool) -> Result<ReadRecord> {
    match policy {
        TailPolicy::Required => Err(Error::Gap),
        TailPolicy::Unpublished if empty => Ok(ReadRecord::Eof),
        TailPolicy::Unpublished => Ok(ReadRecord::IncompleteTail),
    }
}

fn read_until_eof<R: Read>(reader: &mut R, bytes: &mut [u8]) -> Result<usize> {
    let mut count = 0;
    while count < bytes.len() {
        match reader.read(&mut bytes[count..]) {
            Ok(0) => break,
            Ok(size) => count += size,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(count)
}

fn allocate(size: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::Allocation)?;
    Ok(bytes)
}

fn pack(header: &[u8; HEADER_BYTES], payload: &[u8]) -> Result<Vec<u8>> {
    let size = HEADER_BYTES
        .checked_add(payload.len())
        .ok_or(Error::Overflow)?;
    let mut bytes = allocate(size)?;
    bytes.extend_from_slice(header);
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

fn check_frame(payload_len: usize, part: u32, last: bool, limits: Limits) -> Result<()> {
    limits.validate()?;
    let size = HEADER_BYTES
        .checked_add(payload_len)
        .ok_or(Error::Overflow)?;
    if size > limits.max_frame_bytes
        || u64::try_from(payload_len).map_err(|_| Error::Overflow)? > limits.max_event_bytes
        || part >= limits.max_parts
        || (!last && part + 1 >= limits.max_parts)
    {
        return Err(Error::LimitExceeded);
    }
    Ok(())
}

fn check_completed(tx: &CompletedTx) -> Result<()> {
    if tx.order.0 == 0
        || tx.commit.is_some_and(|commit| commit.0 > tx.end.0)
        || tx.encoded_bytes < COMPLETED_BYTES as u64
    {
        return Err(Error::InvalidRecord);
    }
    Ok(())
}

fn encode_header(
    tx: TxId,
    kind: u8,
    flags: u8,
    ordinal: u64,
    part: u32,
    payload: &[u8],
) -> [u8; HEADER_BYTES] {
    let mut raw = [0; HEADER_BYTES];
    raw[..4].copy_from_slice(&MAGIC);
    raw[4..6].copy_from_slice(&VERSION.to_le_bytes());
    raw[6] = kind;
    raw[7] = flags;
    raw[8..12].copy_from_slice(&(HEADER_BYTES as u32).to_le_bytes());
    raw[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    raw[16..32].copy_from_slice(&tx.stream.run.0);
    raw[32..40].copy_from_slice(&tx.stream.source.system_identifier.to_le_bytes());
    raw[40..44].copy_from_slice(&tx.stream.source.database_oid.to_le_bytes());
    raw[44..52].copy_from_slice(&tx.stream.generation.to_le_bytes());
    raw[52..56].copy_from_slice(&tx.xid.to_le_bytes());
    raw[56..64].copy_from_slice(&tx.begin_ordinal.to_le_bytes());
    raw[64..72].copy_from_slice(&ordinal.to_le_bytes());
    raw[72..76].copy_from_slice(&part.to_le_bytes());
    raw[80..84].copy_from_slice(&crc32fast::hash(payload).to_le_bytes());
    let checksum = crc32fast::hash(&raw[..84]);
    raw[84..88].copy_from_slice(&checksum.to_le_bytes());
    raw
}

struct Header {
    kind: u8,
    tx: TxId,
    ordinal: u64,
    part: u32,
    last: bool,
    commit_present: bool,
    payload_len: usize,
    payload_crc: u32,
}

impl Header {
    fn check_payload(&self, payload: &[u8]) -> Result<()> {
        if crc32fast::hash(payload) != self.payload_crc {
            return Err(Error::CorruptPayload);
        }
        Ok(())
    }

    fn frame(self, payload: Vec<u8>) -> Frame {
        Frame {
            tx: self.tx,
            event_ordinal: self.ordinal,
            part: self.part,
            last_part: self.last,
            payload,
        }
    }

    fn completed(self, payload: &[u8]) -> Result<CompletedTx> {
        if self.kind != 2 {
            return Err(Error::InvalidRecord);
        }
        let raw_commit = u64::from_le_bytes(field(payload, 8));
        let commit = if self.commit_present {
            Some(CommitLsn(raw_commit))
        } else if raw_commit == 0 {
            None
        } else {
            return Err(Error::InvalidRecord);
        };
        let tx = CompletedTx {
            tx: self.tx,
            order: TxSeq(u64::from_le_bytes(field(payload, 0))),
            commit,
            end: SourceEndLsn(u64::from_le_bytes(field(payload, 16))),
            event_count: u64::from_le_bytes(field(payload, 24)),
            encoded_bytes: u64::from_le_bytes(field(payload, 32)),
        };
        check_completed(&tx)?;
        Ok(tx)
    }
}

fn field<const N: usize>(bytes: &[u8], offset: usize) -> [u8; N] {
    bytes[offset..offset + N]
        .try_into()
        .expect("validated fixed record width")
}

fn decode_header(raw: &[u8; HEADER_BYTES], limits: Limits) -> Result<Header> {
    limits.validate()?;
    if crc32fast::hash(&raw[..84]) != u32::from_le_bytes(field(raw, 84)) {
        return Err(Error::CorruptHeader);
    }
    if raw[..4] != MAGIC {
        return Err(Error::InvalidRecord);
    }
    let version = u16::from_le_bytes(field(raw, 4));
    if version != VERSION {
        return Err(Error::UnsupportedVersion(version));
    }
    let kind = raw[6];
    if kind != 1 && kind != 2 {
        return Err(Error::UnknownKind(kind));
    }
    let allowed_flags = if kind == 1 { 1 } else { COMMIT_PRESENT };
    if u32::from_le_bytes(field(raw, 8)) != HEADER_BYTES as u32
        || u32::from_le_bytes(field(raw, 76)) != 0
        || raw[7] & !allowed_flags != 0
    {
        return Err(Error::InvalidRecord);
    }
    let header = Header {
        kind,
        tx: TxId {
            stream: StreamId {
                run: RunId(field(raw, 16)),
                source: SourceId {
                    system_identifier: u64::from_le_bytes(field(raw, 32)),
                    database_oid: u32::from_le_bytes(field(raw, 40)),
                },
                generation: u64::from_le_bytes(field(raw, 44)),
            },
            xid: u32::from_le_bytes(field(raw, 52)),
            begin_ordinal: u64::from_le_bytes(field(raw, 56)),
        },
        ordinal: u64::from_le_bytes(field(raw, 64)),
        part: u32::from_le_bytes(field(raw, 72)),
        last: raw[7] == 1,
        commit_present: raw[7] & COMMIT_PRESENT != 0,
        payload_len: usize::try_from(u32::from_le_bytes(field(raw, 12)))
            .map_err(|_| Error::Overflow)?,
        payload_crc: u32::from_le_bytes(field(raw, 80)),
    };
    if kind == 1 {
        check_frame(header.payload_len, header.part, header.last, limits)?;
    } else if header.payload_len != 40 || header.ordinal != 0 || header.part != 0 || header.last {
        return Err(Error::InvalidRecord);
    }
    Ok(header)
}

fn decode_slice(bytes: &[u8], limits: Limits) -> Result<(Header, &[u8])> {
    limits.validate()?;
    let raw = bytes.get(..HEADER_BYTES).ok_or(Error::Gap)?;
    let header = decode_header(raw.try_into().expect("checked header width"), limits)?;
    let length = HEADER_BYTES
        .checked_add(header.payload_len)
        .ok_or(Error::Overflow)?;
    if bytes.len() < length {
        return Err(Error::Gap);
    }
    if bytes.len() != length {
        return Err(Error::InvalidRecord);
    }
    let payload = &bytes[HEADER_BYTES..];
    header.check_payload(payload)?;
    Ok((header, payload))
}

pub struct TxValidator {
    tx: TxId,
    limits: Limits,
    events: u64,
    part: u32,
    event_bytes: u64,
    wire_bytes: u64,
    failed: bool,
}

impl TxValidator {
    pub fn new(tx: TxId, limits: Limits) -> Result<Self> {
        limits.validate()?;
        Ok(Self {
            tx,
            limits,
            events: 0,
            part: 0,
            event_bytes: 0,
            wire_bytes: COMPLETED_BYTES as u64,
            failed: false,
        })
    }

    pub fn push(&mut self, frame: &Frame) -> Result<()> {
        if self.failed {
            return Err(Error::InvalidRecord);
        }
        // A rejected frame cannot be omitted and followed by a successful COMMIT check.
        self.failed = true;
        if frame.tx != self.tx {
            return Err(Error::IdentityMismatch);
        }
        if frame.event_ordinal != self.events || frame.part != self.part {
            return Err(Error::OrderMismatch);
        }
        check_frame(
            frame.payload.len(),
            frame.part,
            frame.last_part,
            self.limits,
        )?;
        let payload_bytes = u64::try_from(frame.payload.len()).map_err(|_| Error::Overflow)?;
        self.event_bytes = self
            .event_bytes
            .checked_add(payload_bytes)
            .ok_or(Error::Overflow)?;
        if self.event_bytes > self.limits.max_event_bytes {
            return Err(Error::LimitExceeded);
        }
        self.wire_bytes = self
            .wire_bytes
            .checked_add(HEADER_BYTES as u64)
            .and_then(|size| size.checked_add(payload_bytes))
            .ok_or(Error::Overflow)?;
        if frame.last_part {
            self.events = self.events.checked_add(1).ok_or(Error::Overflow)?;
            self.part = 0;
            self.event_bytes = 0;
        } else {
            self.part = self.part.checked_add(1).ok_or(Error::Overflow)?;
        }
        self.failed = false;
        Ok(())
    }

    pub fn finish(self, completed: &CompletedTx) -> Result<()> {
        if self.failed {
            return Err(Error::InvalidRecord);
        }
        if completed.tx != self.tx {
            return Err(Error::IdentityMismatch);
        }
        if self.part != 0 {
            return Err(Error::Gap);
        }
        if completed.event_count != self.events || completed.encoded_bytes != self.wire_bytes {
            return Err(Error::DescriptorMismatch);
        }
        check_completed(completed)?;
        Ok(())
    }
}

pub fn validate_next(previous: &CompletedTx, next: &CompletedTx) -> Result<()> {
    check_completed(previous)?;
    check_completed(next)?;
    if previous.tx.stream != next.tx.stream {
        return Err(Error::IdentityMismatch);
    }
    let order = previous.order.0.checked_add(1).ok_or(Error::Overflow)?;
    let commit_regressed = matches!(
        (previous.commit, next.commit),
        (Some(previous), Some(next)) if next < previous
    );
    if next.order.0 != order || commit_regressed || next.end < previous.end {
        return Err(Error::OrderMismatch);
    }
    Ok(())
}

#[derive(Default)]
pub struct StreamingChecksum(crc32fast::Hasher);

impl StreamingChecksum {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    pub fn finish(self) -> u32 {
        self.0.finalize()
    }
}
