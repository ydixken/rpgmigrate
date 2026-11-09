use crate::apply::checkpoint::SafeFeedback;
use crate::cdc::contracts::{Event, FollowMessage, Value};
use crate::contracts::{Error, ErrorKind, FollowLimits, Result};
use crate::identity::{SourceEndLsn, StreamId};
use std::collections::VecDeque;
use std::mem::size_of;
use std::sync::mpsc::{RecvTimeoutError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

struct Queue {
    state: Mutex<QueueState>,
    available: Condvar,
    limits: FollowLimits,
    storage_bytes: usize,
}

struct QueueState {
    messages: VecDeque<Box<FollowMessage>>,
    bytes: usize,
    sender_alive: bool,
    receiver_alive: bool,
    last_full: Option<(usize, usize)>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueueSnapshot {
    pub(crate) limits: FollowLimits,
    pub(crate) queued_messages: usize,
    pub(crate) queued_bytes: usize,
}

pub(crate) struct FollowSender(Arc<Queue>);
pub(crate) struct FollowReceiver(Arc<Queue>);

pub(crate) fn bounded(limits: FollowLimits) -> Result<(FollowSender, FollowReceiver)> {
    limits.validate()?;
    if limits.channel_bytes > 8 * 1024 * 1024 || limits.channel_messages > 1024 {
        return Err(error("follow FIFO limits exceed the supported budget"));
    }
    let mut messages = VecDeque::new();
    messages
        .try_reserve_exact(limits.channel_messages)
        .map_err(|_| error("follow FIFO allocation failed"))?;
    let storage_bytes = messages
        .capacity()
        .checked_mul(size_of::<Box<FollowMessage>>())
        .filter(|bytes| *bytes < limits.channel_bytes && *bytes < limits.max_message_bytes)
        .ok_or_else(|| error("follow FIFO metadata exceeds its byte budget"))?;
    let queue = Arc::new(Queue {
        state: Mutex::new(QueueState {
            messages,
            bytes: 0,
            sender_alive: true,
            receiver_alive: true,
            last_full: None,
        }),
        available: Condvar::new(),
        limits,
        storage_bytes,
    });
    Ok((FollowSender(Arc::clone(&queue)), FollowReceiver(queue)))
}

impl FollowSender {
    pub(crate) fn full_snapshot(&self) -> Result<QueueSnapshot> {
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| error("follow FIFO is poisoned"))?;
        let (queued_messages, queued_bytes) = state
            .last_full
            .ok_or_else(|| error("follow FIFO has no full observation"))?;
        Ok(QueueSnapshot {
            limits: self.0.limits,
            queued_messages,
            queued_bytes,
        })
    }

    pub(crate) fn validate_message(&self, message: &FollowMessage) -> Result<()> {
        message_charge(message)?
            .checked_add(self.0.storage_bytes)
            .filter(|bytes| *bytes <= self.0.limits.max_message_bytes)
            .ok_or_else(|| error("follow message exceeds the supported sixteen MiB budget"))?;
        Ok(())
    }

    pub(crate) fn try_send(
        &self,
        message: FollowMessage,
    ) -> std::result::Result<(), TrySendError<FollowMessage>> {
        let Ok(mut state) = self.0.state.lock() else {
            return Err(TrySendError::Disconnected(message));
        };
        if !state.sender_alive || !state.receiver_alive {
            return Err(TrySendError::Disconnected(message));
        }
        let charge = message_charge(&message).ok().and_then(|charge| {
            charge
                .checked_add(self.0.storage_bytes)
                .filter(|bytes| *bytes <= self.0.limits.max_message_bytes)
                .map(|_| charge)
        });
        let Some(charge) = charge else {
            state.sender_alive = false;
            self.0.available.notify_all();
            return Err(TrySendError::Disconnected(message));
        };
        let fits_bytes = state
            .bytes
            .checked_add(charge)
            .and_then(|bytes| bytes.checked_add(self.0.storage_bytes))
            .is_some_and(|bytes| bytes <= self.0.limits.channel_bytes);
        if state.messages.len() == self.0.limits.channel_messages
            || (!state.messages.is_empty() && !fits_bytes)
        {
            state.last_full = Some((state.messages.len(), state.bytes + self.0.storage_bytes));
            return Err(TrySendError::Full(message));
        }
        state.bytes += charge;
        state.messages.push_back(Box::new(message));
        self.0.available.notify_one();
        Ok(())
    }
}

impl Drop for FollowSender {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.sender_alive = false;
        }
        self.0.available.notify_all();
    }
}

impl FollowReceiver {
    pub(crate) fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> std::result::Result<FollowMessage, RecvTimeoutError> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| RecvTimeoutError::Disconnected)?;
        loop {
            if let Some(message) = state.messages.pop_front() {
                let charge =
                    message_charge(&message).map_err(|_| RecvTimeoutError::Disconnected)?;
                state.bytes -= charge;
                return Ok(*message);
            }
            if !state.sender_alive {
                return Err(RecvTimeoutError::Disconnected);
            }
            let remaining = deadline
                .ok_or(RecvTimeoutError::Timeout)?
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            state = self
                .0
                .available
                .wait_timeout(state, remaining)
                .map_err(|_| RecvTimeoutError::Disconnected)?
                .0;
        }
    }
}

impl Drop for FollowReceiver {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.receiver_alive = false;
        }
        self.0.available.notify_all();
    }
}

pub(crate) fn message_charge(message: &FollowMessage) -> Result<usize> {
    fn add(total: &mut usize, bytes: usize) -> Result<()> {
        *total = total
            .checked_add(bytes)
            .ok_or_else(|| error("follow message accounting overflowed"))?;
        Ok(())
    }
    fn values(total: &mut usize, values: &Vec<Value>) -> Result<()> {
        add(
            total,
            values
                .capacity()
                .checked_mul(size_of::<Value>())
                .ok_or_else(|| error("follow tuple accounting overflowed"))?,
        )?;
        for value in values {
            if let Value::Text(bytes) = value {
                add(total, bytes.capacity())?;
            }
        }
        Ok(())
    }
    let mut total = size_of::<FollowMessage>();
    if let FollowMessage::Event(event) = message {
        match event {
            Event::Relation {
                schema,
                name,
                columns,
                ..
            } => {
                add(&mut total, schema.capacity())?;
                add(&mut total, name.capacity())?;
                add(
                    &mut total,
                    columns
                        .capacity()
                        .checked_mul(size_of::<crate::cdc::contracts::Column>())
                        .ok_or_else(|| error("follow relation accounting overflowed"))?,
                )?;
                for column in columns {
                    add(&mut total, column.name.capacity())?;
                }
            }
            Event::Insert { values: tuple, .. } => values(&mut total, tuple)?,
            Event::Update { old, new, .. } => {
                if let Some(old) = old {
                    values(&mut total, &old.values)?;
                }
                values(&mut total, new)?;
            }
            Event::Delete { old, .. } => values(&mut total, &old.values)?,
            Event::Truncate { relations, .. } => add(
                &mut total,
                relations
                    .capacity()
                    .checked_mul(size_of::<crate::cdc::contracts::RelationKey>())
                    .ok_or_else(|| error("follow truncate accounting overflowed"))?,
            )?,
        }
    }
    Ok(total)
}

#[derive(Default)]
struct Mailbox {
    last: Option<(StreamId, SourceEndLsn)>,
    pending: Option<SafeFeedback>,
}

pub(crate) struct FeedbackSender(Arc<Mutex<Mailbox>>);
pub(crate) struct FeedbackReceiver(Arc<Mutex<Mailbox>>);

pub(crate) fn feedback() -> (FeedbackSender, FeedbackReceiver) {
    let mailbox = Arc::new(Mutex::new(Mailbox::default()));
    (
        FeedbackSender(Arc::clone(&mailbox)),
        FeedbackReceiver(mailbox),
    )
}

impl FeedbackSender {
    pub(crate) fn publish(&self, proof: SafeFeedback) -> Result<()> {
        let mut mailbox = self
            .0
            .lock()
            .map_err(|_| error("follow feedback mailbox is poisoned"))?;
        if mailbox
            .last
            .is_some_and(|(stream, end)| stream != proof.stream() || end > proof.end())
        {
            return Err(error("follow feedback changed stream or moved backward"));
        }
        mailbox.last = Some((proof.stream(), proof.end()));
        mailbox.pending = Some(proof);
        Ok(())
    }
}

impl FeedbackReceiver {
    pub(crate) fn take(&self) -> Option<SafeFeedback> {
        self.0
            .lock()
            .expect("follow feedback mailbox is poisoned")
            .pending
            .take()
    }
}

#[track_caller]
fn error(message: &str) -> Error {
    Error::new(ErrorKind::Source, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cdc::contracts::{Begin, RelationKey};
    use crate::identity::CommitLsn;

    fn message(bytes: usize) -> FollowMessage {
        FollowMessage::Event(Event::Insert {
            relation: RelationKey {
                oid: 1,
                generation: 1,
            },
            values: vec![Value::Text(vec![0; bytes])],
        })
    }

    fn limits(bytes: usize, messages: usize, max: usize) -> FollowLimits {
        FollowLimits {
            channel_bytes: bytes,
            channel_messages: messages,
            max_message_bytes: max,
            ..FollowLimits::default()
        }
    }

    #[test]
    fn fifo_charges_capacity_not_length_and_refunds_on_receive() {
        let mut values = Vec::with_capacity(3);
        let mut text = Vec::with_capacity(100);
        text.extend_from_slice(b"x");
        values.push(Value::Text(text));
        let message = FollowMessage::Event(Event::Insert {
            relation: RelationKey {
                oid: 1,
                generation: 1,
            },
            values,
        });
        let charge = message_charge(&message).unwrap();
        assert_eq!(
            charge,
            size_of::<FollowMessage>() + 3 * size_of::<Value>() + 100
        );
        let (sender, receiver) = bounded(limits(charge + 16, 2, charge + 16)).unwrap();
        assert!(sender.full_snapshot().is_err());
        let expected = message.clone();
        sender.try_send(message).unwrap();
        assert!(matches!(
            sender.try_send(expected.clone()),
            Err(TrySendError::Full(_))
        ));
        let snapshot = sender.full_snapshot().unwrap();
        assert_eq!(snapshot.queued_messages, 1);
        assert_eq!(snapshot.queued_bytes, charge + sender.0.storage_bytes);
        assert_eq!(snapshot.limits.channel_messages, 2);
        assert_eq!(snapshot.limits.channel_bytes, charge + 16);
        assert_eq!(receiver.recv_timeout(Duration::ZERO).unwrap(), expected);
        assert_eq!(
            sender.full_snapshot().unwrap().queued_bytes,
            snapshot.queued_bytes
        );
        sender.try_send(expected.clone()).unwrap();
        assert_eq!(receiver.recv_timeout(Duration::ZERO).unwrap(), expected);
    }

    #[test]
    fn oversize_message_is_admitted_only_alone_and_unchargable_input_is_refused() {
        let first = message(100);
        let oversize = message(1000);
        let oversized_charge = message_charge(&oversize).unwrap();
        let (sender, receiver) = bounded(limits(512, 2, 2048)).unwrap();
        sender.try_send(first.clone()).unwrap();
        assert!(matches!(
            sender.try_send(oversize.clone()),
            Err(TrySendError::Full(_))
        ));
        assert_eq!(receiver.recv_timeout(Duration::ZERO).unwrap(), first);
        sender.try_send(oversize.clone()).unwrap();
        assert!(matches!(sender.try_send(first), Err(TrySendError::Full(_))));
        assert_eq!(receiver.recv_timeout(Duration::ZERO).unwrap(), oversize);
        assert!(oversized_charge > 512);
        assert!(sender.validate_message(&message(2048)).is_err());
        assert!(matches!(
            sender.try_send(message(2048)),
            Err(TrySendError::Disconnected(_))
        ));
        assert_eq!(
            receiver.recv_timeout(Duration::ZERO),
            Err(RecvTimeoutError::Disconnected)
        );
    }

    #[test]
    fn count_limit_fifo_timeout_and_closure_are_explicit() {
        let (sender, receiver) = bounded(limits(4096, 2, 4096)).unwrap();
        for xid in [1, 2] {
            sender
                .try_send(FollowMessage::Begin(Begin {
                    xid,
                    final_lsn: CommitLsn(xid.into()),
                    commit_time_micros: 0,
                }))
                .unwrap();
        }
        assert!(matches!(
            sender.try_send(message(1)),
            Err(TrySendError::Full(_))
        ));
        for xid in [1, 2] {
            assert!(
                matches!(receiver.recv_timeout(Duration::ZERO).unwrap(), FollowMessage::Begin(begin) if begin.xid == xid)
            );
        }
        assert_eq!(
            receiver.recv_timeout(Duration::from_millis(1)),
            Err(RecvTimeoutError::Timeout)
        );
        drop(sender);
        assert_eq!(
            receiver.recv_timeout(Duration::ZERO),
            Err(RecvTimeoutError::Disconnected)
        );
        let (sender, receiver) = bounded(FollowLimits::default()).unwrap();
        drop(receiver);
        assert!(matches!(
            sender.try_send(message(1)),
            Err(TrySendError::Disconnected(_))
        ));
    }

    #[test]
    fn feedback_mailbox_preserves_monotonicity_after_take_and_replaces_pending_proof() {
        let stream = StreamId {
            run: crate::identity::RunId([1; 16]),
            source: crate::identity::SourceId {
                system_identifier: 2,
                database_oid: 3,
            },
            generation: 1,
        };
        let proof = |end| crate::apply::checkpoint::feedback_for_test(stream, SourceEndLsn(end));
        let (sender, receiver) = feedback();
        sender.publish(proof(10)).unwrap();
        sender.publish(proof(20)).unwrap();
        assert_eq!(receiver.take().unwrap().end(), SourceEndLsn(20));
        assert!(receiver.take().is_none());
        assert!(sender.publish(proof(19)).is_err());
        sender.publish(proof(20)).unwrap();
        let mut foreign = stream;
        foreign.generation += 1;
        assert!(
            sender
                .publish(crate::apply::checkpoint::feedback_for_test(
                    foreign,
                    SourceEndLsn(30)
                ))
                .is_err()
        );
        assert_eq!(receiver.take().unwrap().end(), SourceEndLsn(20));
    }

    #[test]
    fn zero_or_unbounded_limits_are_refused() {
        assert!(bounded(limits(0, 2, 4096)).is_err());
        assert!(bounded(limits(4096, 0, 4096)).is_err());
        assert!(bounded(limits(4096, 1025, 4096)).is_err());
        assert!(bounded(limits(9 * 1024 * 1024, 2, 16 * 1024 * 1024)).is_err());
    }
}
