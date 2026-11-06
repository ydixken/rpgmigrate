use super::{CopyBuffer, error};
use crate::contracts::Result;
use std::ffi::CString;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct IdentifySystem {
    pub(crate) system_identifier: u64,
    pub(crate) timeline: u32,
    pub(crate) wal_position: u64,
    pub(crate) database_name: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StandbyStatus {
    pub(crate) write: u64,
    pub(crate) flush: u64,
    pub(crate) apply: u64,
    /// PostgreSQL epoch (2000-01-01), in microseconds.
    pub(crate) client_time_micros: i64,
    pub(crate) request_reply: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplicationEnvelope {
    XLogData {
        wal_start: u64,
        wal_end: u64,
        server_time_micros: i64,
    },
    Keepalive {
        wal_end: u64,
        server_time_micros: i64,
        reply_requested: bool,
    },
}

/// Owns one full libpq message, which can exceed a caller's nominal byte budget.
/// Libpq input buffers and frames retained by the caller are additional allocations.
pub(crate) struct ReplicationFrame {
    buffer: CopyBuffer,
    envelope: ReplicationEnvelope,
}

pub(crate) enum ReplicationPoll {
    Frame(
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "Logical source integration is pending")
        )]
        ReplicationFrame,
    ),
    /// Libpq's output buffer is no longer pending; the supplied status is returned unchanged.
    /// This says nothing about sending, peer receipt, source acknowledgment, or durability.
    FeedbackDrained(
        #[cfg_attr(
            not(test),
            expect(dead_code, reason = "Logical source integration is pending")
        )]
        StandbyStatus,
    ),
    Wake,
    End,
}

pub(super) struct PendingFeedback {
    pub(super) status: StandbyStatus,
    pub(super) queued: bool,
    pub(super) drained: bool,
}

#[derive(Default)]
pub(super) struct ReplicationState {
    pub(super) pending: Option<PendingFeedback>,
    pub(super) ending: bool,
    pub(super) sent_end: bool,
    pub(super) received_end: bool,
    pub(super) terminals: TerminalResults,
    #[cfg(test)]
    pub(super) flush_pending: usize,
}

#[derive(Default)]
pub(super) struct TerminalResults {
    count: usize,
    invalid: bool,
}

impl IdentifySystem {
    pub(super) fn parse(rows: usize, fields: &[Option<&[u8]>]) -> Result<Self> {
        let invalid = || error("PostgreSQL replication identity is invalid");
        if rows != 1 || fields.len() != 4 {
            return Err(invalid());
        }
        let field = |index: usize| {
            fields[index]
                .filter(|value| !value.is_empty())
                .ok_or_else(invalid)
        };
        let decimal = |bytes: &[u8]| -> Result<u64> {
            if !bytes.iter().all(u8::is_ascii_digit) {
                return Err(invalid());
            }
            std::str::from_utf8(bytes)
                .map_err(|_| invalid())?
                .parse()
                .map_err(|_| invalid())
        };
        let system_identifier = decimal(field(0)?)?;
        let timeline = u32::try_from(decimal(field(1)?)?).map_err(|_| invalid())?;
        if system_identifier == 0 || timeline == 0 {
            return Err(invalid());
        }
        let lsn = field(2)?;
        let mut parts = lsn.split(|byte| *byte == b'/');
        let mut hex = || -> Result<u32> {
            let part = parts.next().ok_or_else(invalid)?;
            if part.is_empty() || part.len() > 8 || !part.iter().all(u8::is_ascii_hexdigit) {
                return Err(invalid());
            }
            u32::from_str_radix(std::str::from_utf8(part).map_err(|_| invalid())?, 16)
                .map_err(|_| invalid())
        };
        let wal_position = (u64::from(hex()?) << 32) | u64::from(hex()?);
        if parts.next().is_some() {
            return Err(invalid());
        }
        Ok(Self {
            system_identifier,
            timeline,
            wal_position,
            database_name: field(3)?.to_vec(),
        })
    }
}

impl StandbyStatus {
    pub(super) fn encode(self) -> [u8; 34] {
        let mut bytes = [0; 34];
        bytes[0] = b'r';
        bytes[1..9].copy_from_slice(&self.write.to_be_bytes());
        bytes[9..17].copy_from_slice(&self.flush.to_be_bytes());
        bytes[17..25].copy_from_slice(&self.apply.to_be_bytes());
        bytes[25..33].copy_from_slice(&self.client_time_micros.to_be_bytes());
        bytes[33] = u8::from(self.request_reply);
        bytes
    }
}

impl ReplicationFrame {
    pub(super) fn new(buffer: CopyBuffer) -> Result<Self> {
        let envelope = parse_envelope(buffer.as_bytes())?;
        Ok(Self { buffer, envelope })
    }
    pub(crate) fn as_bytes(&self) -> &[u8] {
        self.buffer.as_bytes()
    }
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Logical source integration is pending")
    )]
    pub(crate) fn envelope(&self) -> ReplicationEnvelope {
        self.envelope
    }
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "Logical source integration is pending")
    )]
    pub(crate) fn payload(&self) -> Option<&[u8]> {
        match self.envelope {
            ReplicationEnvelope::XLogData { .. } => Some(&self.as_bytes()[25..]),
            ReplicationEnvelope::Keepalive { .. } => None,
        }
    }
}

impl ReplicationState {
    pub(super) fn queue(&mut self, status: StandbyStatus) -> bool {
        if self.pending.is_some() {
            return false;
        }
        self.pending = Some(PendingFeedback {
            status,
            queued: false,
            drained: false,
        });
        true
    }
}

impl TerminalResults {
    pub(super) fn observe(&mut self, tag: Option<&str>) {
        // Walsender emits COPY 0, then SELECT on older servers or START_REPLICATION on newer ones.
        self.invalid |= !matches!(
            (self.count, tag),
            (0, Some("COPY 0")) | (1, Some("SELECT" | "START_REPLICATION"))
        );
        self.count = self.count.saturating_add(1);
    }
    pub(super) fn finish(&self) -> Result<()> {
        if self.count == 2 && !self.invalid {
            Ok(())
        } else {
            Err(error(
                "PostgreSQL replication completion is invalid or incomplete",
            ))
        }
    }
}

pub(super) fn start_command(
    slot: &str,
    start: u64,
    options: &[(&str, Option<&str>)],
) -> Result<CString> {
    let invalid = || error("PostgreSQL replication start arguments are invalid");
    if slot.is_empty()
        || slot.len() > 63
        || !slot
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid());
    }
    let mut capacity = 128_usize;
    for (name, value) in options {
        // PostgreSQL 9.6's replication scanner cannot escape quotes in option names.
        if name.is_empty()
            || name.len() > 63
            || name.contains('\0')
            || name.contains('"')
            || value.is_some_and(|value| value.contains('\0'))
        {
            return Err(invalid());
        }
        capacity = name
            .len()
            .checked_add(value.map_or(0, str::len))
            .and_then(|len| len.checked_mul(2))
            .and_then(|len| len.checked_add(8))
            .and_then(|len| capacity.checked_add(len))
            .filter(|len| *len < i32::MAX as usize)
            .ok_or_else(invalid)?;
    }
    let mut command = String::with_capacity(capacity);
    use std::fmt::Write;
    write!(
        command,
        "START_REPLICATION SLOT \"{slot}\" LOGICAL {:X}/{:X}",
        start >> 32,
        start as u32
    )
    .unwrap();
    for (index, (name, value)) in options.iter().enumerate() {
        command.push_str(if index == 0 { " (" } else { ", " });
        command.push('"');
        command.push_str(name);
        command.push('"');
        if let Some(value) = value {
            // The replication scanner uses standard literals; backslashes are literal, not E-string escapes.
            command.push_str(" '");
            command.push_str(&value.replace('\'', "''"));
            command.push('\'');
        }
    }
    if !options.is_empty() {
        command.push(')');
    }
    CString::new(command).map_err(|_| invalid())
}

fn parse_envelope(bytes: &[u8]) -> Result<ReplicationEnvelope> {
    match bytes.first() {
        Some(b'w') if bytes.len() >= 25 => Ok(ReplicationEnvelope::XLogData {
            wal_start: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
            wal_end: u64::from_be_bytes(bytes[9..17].try_into().unwrap()),
            server_time_micros: i64::from_be_bytes(bytes[17..25].try_into().unwrap()),
        }),
        Some(b'k') if bytes.len() == 18 && bytes[17] <= 1 => Ok(ReplicationEnvelope::Keepalive {
            wal_end: u64::from_be_bytes(bytes[1..9].try_into().unwrap()),
            server_time_micros: i64::from_be_bytes(bytes[9..17].try_into().unwrap()),
            reply_requested: bytes[17] == 1,
        }),
        _ => Err(error("PostgreSQL replication frame is invalid")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identify_system_validates_shape_and_fields() {
        let valid = [
            Some(&b"18446744073709551615"[..]),
            Some(&b"7"[..]),
            Some(&b"1/ABC"[..]),
            Some(&b"db\xff"[..]),
        ];
        let parsed = IdentifySystem::parse(1, &valid).unwrap();
        assert_eq!(parsed.system_identifier, u64::MAX);
        assert_eq!(parsed.timeline, 7);
        assert_eq!(parsed.wal_position, 0x1_0000_0abc);
        assert_eq!(parsed.database_name, b"db\xff");
        for rows in [0, 2] {
            assert!(IdentifySystem::parse(rows, &valid).is_err());
        }
        for columns in 0..4 {
            assert!(IdentifySystem::parse(1, &valid[..columns]).is_err());
        }
        let mut extra = valid.to_vec();
        extra.push(Some(b"extra"));
        assert!(IdentifySystem::parse(1, &extra).is_err());
        for column in 0..4 {
            let mut fields = valid;
            fields[column] = None;
            assert!(IdentifySystem::parse(1, &fields).is_err());
            fields[column] = Some(b"");
            assert!(IdentifySystem::parse(1, &fields).is_err());
        }
        for (column, value) in [
            (0, "18446744073709551616"),
            (0, "0"),
            (0, "+1"),
            (1, "4294967296"),
            (1, "0"),
            (2, "100000000/0"),
            (2, "1/100000000"),
            (2, "1/G"),
            (2, "1/2/3"),
        ] {
            let mut fields = valid;
            fields[column] = Some(value.as_bytes());
            assert!(IdentifySystem::parse(1, &fields).is_err());
        }
    }

    #[test]
    fn start_replication_escapes_options_and_rejects_invalid_input() {
        let command = start_command(
            "slot_1",
            0x1_0000_002a,
            &[
                ("odd name", Some("a'\\b; SELECT 1")),
                ("include-xids", Some("0")),
                ("flag", None),
            ],
        )
        .unwrap();
        assert_eq!(command.to_bytes(), b"START_REPLICATION SLOT \"slot_1\" LOGICAL 1/2A (\"odd name\" 'a''\\b; SELECT 1', \"include-xids\" '0', \"flag\")");
        for slot in ["", "BAD", "a;b", "a\0b", &"a".repeat(64)] {
            assert!(start_command(slot, 0, &[]).is_err());
        }
        for options in [
            vec![("", None)],
            vec![("x\0y", None)],
            vec![("odd\"name", None)],
            vec![("x", Some("a\0b"))],
        ] {
            assert!(start_command("slot", 0, &options).is_err());
        }
        let failure =
            start_command("slot", 0, &[("private\"name", Some("private-value"))]).unwrap_err();
        assert_eq!(
            failure.message,
            "PostgreSQL replication start arguments are invalid"
        );
    }

    #[test]
    fn xlogdata_preserves_envelope_and_binary_payload() {
        let mut bytes = vec![b'w'];
        bytes.extend_from_slice(&1_u64.to_be_bytes());
        bytes.extend_from_slice(&u64::MAX.to_be_bytes());
        bytes.extend_from_slice(&(-2_i64).to_be_bytes());
        bytes.extend_from_slice(&[0, 255, 128]);
        assert_eq!(
            parse_envelope(&bytes).unwrap(),
            ReplicationEnvelope::XLogData {
                wal_start: 1,
                wal_end: u64::MAX,
                server_time_micros: -2
            }
        );
        let frame = super::super::ffi::replication_testing::owned_frame(&bytes);
        assert_eq!(frame.as_bytes(), bytes);
        assert_eq!(frame.payload(), Some(&[0, 255, 128][..]));
        assert!(parse_envelope(&bytes[..25]).is_ok());
    }

    #[test]
    fn keepalive_rejects_wrong_length_and_flags() {
        let mut bytes = vec![b'k'];
        bytes.extend_from_slice(&9_u64.to_be_bytes());
        bytes.extend_from_slice(&(-1_i64).to_be_bytes());
        bytes.push(1);
        assert_eq!(
            parse_envelope(&bytes).unwrap(),
            ReplicationEnvelope::Keepalive {
                wal_end: 9,
                server_time_micros: -1,
                reply_requested: true
            }
        );
        bytes[17] = 0;
        assert!(matches!(
            parse_envelope(&bytes),
            Ok(ReplicationEnvelope::Keepalive {
                reply_requested: false,
                ..
            })
        ));
        for flag in [2, 255] {
            bytes[17] = flag;
            assert!(parse_envelope(&bytes).is_err());
        }
        bytes[17] = 0;
        bytes.push(0);
        assert!(parse_envelope(&bytes).is_err());
    }

    #[test]
    fn replication_frames_reject_unknown_tags_and_truncation() {
        for (tag, length) in [(b'w', 25), (b'k', 18)] {
            let mut bytes = vec![0; length];
            bytes[0] = tag;
            for end in 0..length {
                assert!(parse_envelope(&bytes[..end]).is_err());
            }
        }
        for tag in [0, b'r', b'h', 255] {
            let mut bytes = [0; 25];
            bytes[0] = tag;
            assert!(parse_envelope(&bytes).is_err());
        }
    }

    fn status() -> StandbyStatus {
        StandbyStatus {
            write: 1,
            flush: 2,
            apply: 3,
            client_time_micros: -1,
            request_reply: true,
        }
    }

    #[test]
    fn standby_status_matches_34_byte_wire_fixture() {
        assert_eq!(
            status().encode(),
            [
                b'r', 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 255,
                255, 255, 255, 255, 255, 255, 255, 1
            ]
        );
    }

    #[test]
    fn feedback_queue_is_single_pending_and_never_advances_positions() {
        let mut state = ReplicationState::default();
        assert!(state.queue(status()));
        assert!(!state.queue(StandbyStatus {
            write: 99,
            ..status()
        }));
        let mut keepalive = [0; 18];
        keepalive[0] = b'k';
        keepalive[1..9].copy_from_slice(&u64::MAX.to_be_bytes());
        keepalive[17] = 1;
        parse_envelope(&keepalive).unwrap();
        let pending = state.pending.as_ref().unwrap();
        assert_eq!(pending.status, status());
        assert!(!pending.queued);
        state.pending.take();
        assert!(state.queue(StandbyStatus {
            write: 99,
            ..status()
        }));
    }

    #[test]
    fn replication_terminal_sequence_accepts_legacy_and_current() {
        for tail in ["SELECT", "START_REPLICATION"] {
            let mut results = TerminalResults::default();
            results.observe(Some("COPY 0"));
            results.observe(Some(tail));
            results.finish().unwrap();
        }
    }

    #[test]
    fn replication_terminal_sequence_rejects_missing_extra_and_errors() {
        for tags in [
            vec![],
            vec![Some("COPY 0")],
            vec![Some("SELECT"), Some("COPY 0")],
            vec![Some("COPY 1"), Some("SELECT")],
            vec![Some("COPY 0"), Some("SELECT"), Some("SELECT")],
            vec![Some("COPY 0"), Some("SELECT"), None],
            vec![None, Some("COPY 0"), Some("SELECT")],
        ] {
            let mut results = TerminalResults::default();
            for tag in tags {
                results.observe(tag);
            }
            assert!(results.finish().is_err());
        }
    }
}
