use super::codec::{self, CompletedTx, Frame, Limits};
use super::contracts::{Event, Plugin, RelationKey};
use super::event_codec::encode_event;
use super::plugins::test_decoding::{self, Message};
use crate::contracts::{Error, ErrorKind, QualifiedTable, Result};
use crate::identity::{SourceEndLsn, StreamId, TxId, TxSeq};
use crate::pg::{ReplicationEnvelope, ReplicationFrame};

pub(crate) struct DecoderPosition {
    pub(crate) next_begin_ordinal: u64,
    pub(crate) next_order: TxSeq,
}

pub(crate) trait RelationResolver {
    fn resolve(&mut self, table: &QualifiedTable) -> Result<&Event>;
}

pub(crate) struct Decoder {
    stream: StreamId,
    limits: Limits,
    position: DecoderPosition,
    active: Option<Active>,
    completed: Option<CompletedTx>,
    last_end: Option<SourceEndLsn>,
    poisoned: bool,
}

struct Active {
    tx: TxId,
    event_count: u64,
    encoded_bytes: u64,
    last_relation: Option<RelationKey>,
}

impl Decoder {
    pub(crate) fn new(
        plugin: Plugin,
        stream: StreamId,
        limits: Limits,
        position: DecoderPosition,
    ) -> Result<Self> {
        if plugin != Plugin::TestDecoding || position.next_order.0 == 0 {
            return Err(error(
                ErrorKind::BadConfig,
                "CDC decoder plugin or position is unsupported",
            ));
        }
        limits
            .validate()
            .map_err(|_| error(ErrorKind::BadConfig, "CDC decoder limits are invalid"))?;
        Ok(Self {
            stream,
            limits,
            position,
            active: None,
            completed: None,
            last_end: None,
            poisoned: false,
        })
    }
    pub(crate) fn feed(
        &mut self,
        input: &ReplicationFrame,
        resolver: &mut impl RelationResolver,
        emit: &mut impl FnMut(&Frame) -> Result<()>,
    ) -> Result<()> {
        if self.poisoned {
            return Err(error(ErrorKind::BadState, "CDC decoder is poisoned"));
        }
        let ReplicationEnvelope::XLogData { wal_start, .. } = input.envelope() else {
            return Ok(());
        };
        if self.completed.is_some() {
            return Err(error(
                ErrorKind::BadState,
                "CDC completion must be consumed before transaction input",
            ));
        }
        let result = input
            .payload()
            .ok_or_else(|| error(ErrorKind::Source, "CDC message payload is missing"))
            .and_then(|payload| self.feed_message(payload, wal_start, resolver, emit));
        if result.is_err() {
            self.poisoned = true;
            self.active = None;
        }
        result
    }
    pub(crate) fn take_completed(&mut self) -> Option<CompletedTx> {
        self.completed.take()
    }

    fn feed_message(
        &mut self,
        payload: &[u8],
        wal_start: u64,
        resolver: &mut impl RelationResolver,
        emit: &mut impl FnMut(&Frame) -> Result<()>,
    ) -> Result<()> {
        match test_decoding::parse(payload, self.limits)? {
            Message::Begin(xid) => {
                if self.active.is_some() {
                    return Err(error(
                        ErrorKind::Source,
                        "CDC transactions overlap in a nonstreaming profile",
                    ));
                }
                let begin_ordinal = self.position.next_begin_ordinal;
                self.position.next_begin_ordinal =
                    begin_ordinal.checked_add(1).ok_or_else(overflow)?;
                self.active = Some(Active {
                    tx: TxId {
                        stream: self.stream,
                        xid,
                        begin_ordinal,
                    },
                    event_count: 0,
                    encoded_bytes: codec::COMPLETED_BYTES as u64,
                    last_relation: None,
                });
            }
            Message::Commit(xid) => {
                let active = self.active.as_ref().ok_or_else(outside_transaction)?;
                if active.tx.xid != xid {
                    return Err(error(
                        ErrorKind::Source,
                        "CDC COMMIT does not match the active transaction",
                    ));
                }
                let end = SourceEndLsn(wal_start);
                if self.last_end.is_some_and(|previous| previous > end) {
                    return Err(error(
                        ErrorKind::Source,
                        "CDC transaction end positions decreased",
                    ));
                }
                let next_order = self
                    .position
                    .next_order
                    .0
                    .checked_add(1)
                    .ok_or_else(overflow)?;
                self.completed = Some(CompletedTx {
                    tx: active.tx,
                    order: self.position.next_order,
                    commit: None,
                    end,
                    event_count: active.event_count,
                    encoded_bytes: active.encoded_bytes,
                });
                self.position.next_order = TxSeq(next_order);
                self.last_end = Some(end);
                self.active = None;
            }
            Message::Row(row) => {
                if self.active.is_none() {
                    return Err(outside_transaction());
                }
                let relation = resolver.resolve(&row.table)?;
                let event = row.decode(relation, self.limits)?;
                let Event::Relation { key, .. } = relation else {
                    return Err(error(
                        ErrorKind::BadState,
                        "CDC resolver context is not a relation",
                    ));
                };
                self.emit_relation(*key, relation, emit)?;
                self.emit_event(&event, emit)?;
            }
            Message::Truncate {
                tables,
                cascade,
                restart_identity,
            } => {
                if self.active.is_none() {
                    return Err(outside_transaction());
                }
                let mut relations = Vec::new();
                relations
                    .try_reserve_exact(tables.len())
                    .map_err(|_| error(ErrorKind::Source, "CDC relation list allocation failed"))?;
                for table in tables {
                    let relation = resolver.resolve(&table)?;
                    let (key, _) = test_decoding::validate_relation(&table, relation, self.limits)?;
                    self.emit_relation(key, relation, emit)?;
                    relations.push(key);
                }
                self.emit_event(
                    &Event::Truncate {
                        relations,
                        cascade,
                        restart_identity,
                    },
                    emit,
                )?;
            }
        }
        Ok(())
    }

    fn emit_relation(
        &mut self,
        key: RelationKey,
        relation: &Event,
        emit: &mut impl FnMut(&Frame) -> Result<()>,
    ) -> Result<()> {
        if self
            .active
            .as_ref()
            .ok_or_else(outside_transaction)?
            .last_relation
            != Some(key)
        {
            self.emit_event(relation, emit)?;
            self.active
                .as_mut()
                .ok_or_else(outside_transaction)?
                .last_relation = Some(key);
        }
        Ok(())
    }

    fn emit_event(
        &mut self,
        event: &Event,
        emit: &mut impl FnMut(&Frame) -> Result<()>,
    ) -> Result<()> {
        let active = self.active.as_mut().ok_or_else(outside_transaction)?;
        let payload = encode_event(event, self.limits).map_err(|_| {
            error(
                ErrorKind::Source,
                "CDC event encoding failed or exceeded decoder limits",
            )
        })?;
        let part_bytes = self.limits.max_frame_bytes - codec::HEADER_BYTES;
        let part_count = payload.len().div_ceil(part_bytes);
        let event_count = active.event_count.checked_add(1).ok_or_else(overflow)?;
        let encoded_bytes = (part_count as u64)
            .checked_mul(codec::HEADER_BYTES as u64)
            .and_then(|bytes| bytes.checked_add(payload.len() as u64))
            .and_then(|bytes| bytes.checked_add(active.encoded_bytes))
            .ok_or_else(overflow)?;
        for (part, chunk) in payload.chunks(part_bytes).enumerate() {
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(chunk.len())
                .map_err(|_| error(ErrorKind::Source, "CDC frame allocation failed"))?;
            bytes.extend_from_slice(chunk);
            emit(&Frame {
                tx: active.tx,
                event_ordinal: active.event_count,
                part: u32::try_from(part).map_err(|_| overflow())?,
                last_part: part + 1 == part_count,
                payload: bytes,
            })?;
        }
        active.event_count = event_count;
        active.encoded_bytes = encoded_bytes;
        Ok(())
    }
}

fn error(kind: ErrorKind, message: &str) -> Error {
    Error {
        kind,
        message: message.into(),
    }
}

fn outside_transaction() -> Error {
    error(ErrorKind::Source, "CDC change has no active transaction")
}

fn overflow() -> Error {
    error(
        ErrorKind::BadState,
        "CDC decoder position or accounting overflowed",
    )
}

#[cfg(test)]
mod tests;
