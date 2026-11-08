use super::checkpoint::{TargetAuthority, TargetTxn};
use super::plan::DmlPlan;
use super::{CommitOutcome, Frontier};
use crate::catalog::VerifiedCdcSource;
use crate::cdc::codec::{self, CompletedTx, Frame, Record, TxValidator};
use crate::cdc::contracts::{Column, Event, RelationKey};
use crate::cdc::event_codec::{DecodeBudget, with_decoded_event};
use crate::contracts::{Error, ErrorKind, IoControl, Result};
use crate::pg::PgParam;

#[derive(Clone, Copy)]
pub(crate) struct SerialBudgets {
    pub(crate) frame_heap_bytes: usize,
    pub(crate) reassembly_heap_bytes: usize,
    pub(crate) decoded_heap_bytes: usize,
    pub(crate) relation_slots: usize,
    pub(crate) relation_bytes: usize,
    pub(crate) plan_slots: usize,
    pub(crate) plan_bytes: usize,
    pub(crate) bind_bytes: usize,
}

impl SerialBudgets {
    fn validate(self) -> Result<()> {
        if [
            self.frame_heap_bytes,
            self.reassembly_heap_bytes,
            self.decoded_heap_bytes,
            self.relation_slots,
            self.relation_bytes,
            self.plan_slots,
            self.plan_bytes,
            self.bind_bytes,
        ]
        .contains(&0)
            || self.plan_slots > 1024
        {
            return Err(refusal("serial apply: invalid resource budgets"));
        }
        Ok(())
    }
}

pub(crate) struct SerialApplier {
    authority: TargetAuthority,
    wire: codec::Limits,
    budgets: SerialBudgets,
    caches: Caches,
}

impl SerialApplier {
    /// The authority is freshly acquired; reconnect requires rebuilding this owner and its caches.
    pub(crate) fn new(
        authority: TargetAuthority,
        wire: codec::Limits,
        budgets: SerialBudgets,
    ) -> Result<Self> {
        wire.validate().map_err(input_error)?;
        Ok(Self {
            authority,
            wire,
            budgets,
            caches: Caches::new(budgets)?,
        })
    }

    /// The fresh online producer supplies one complete transaction and returns only at its input boundary.
    /// Input errors, including errors after the descriptor, must propagate from the producer.
    pub(crate) fn apply_one(
        &mut self,
        expected: &CompletedTx,
        source: &mut VerifiedCdcSource<'_>,
        input: impl FnOnce(&mut dyn FnMut(&Record) -> Result<()>) -> Result<()>,
        io: &IoControl,
    ) -> Result<CommitOutcome> {
        if source.stream() != expected.tx.stream {
            return Err(refusal(
                "serial apply: source capability stream does not match",
            ));
        }
        let mut target = self.authority.begin(
            Frontier {
                tx: expected.tx,
                order: expected.order,
                commit: expected.commit,
                end: expected.end,
            },
            io,
        )?;
        self.caches.begin();
        let mut replay = Replay::new(*expected, self.wire, self.budgets)?;
        input(&mut |record| {
            replay.visit(record, &mut |event, charged| {
                self.caches
                    .apply(event, charged, source, &mut target, self.budgets, io)
            })
        })?;
        replay.finish()?;
        target.commit(io)
    }
}

struct Replay {
    expected: CompletedTx,
    validator: Option<TxValidator>,
    wire: codec::Limits,
    budgets: SerialBudgets,
    decoded: DecodeBudget,
    parts: Vec<u8>,
    complete: bool,
    failed: bool,
}

impl Replay {
    fn new(expected: CompletedTx, wire: codec::Limits, budgets: SerialBudgets) -> Result<Self> {
        budgets.validate()?;
        Ok(Self {
            expected,
            validator: Some(TxValidator::new(expected.tx, wire).map_err(input_error)?),
            wire,
            budgets,
            decoded: DecodeBudget::new(budgets.decoded_heap_bytes),
            parts: Vec::new(),
            complete: false,
            failed: false,
        })
    }

    fn visit(
        &mut self,
        record: &Record,
        event: &mut impl FnMut(&Event, usize) -> Result<()>,
    ) -> Result<()> {
        if self.failed {
            return Err(refusal("serial apply: input is poisoned"));
        }
        // A producer can catch a callback error or unwind and still return Ok.
        self.failed = true;
        if self.complete {
            return Err(refusal(
                "serial apply: records follow the terminal descriptor",
            ));
        }
        let outcome = match record {
            Record::Frame(frame) => self.frame(frame, event),
            Record::Completed(completed) => {
                if *completed != self.expected {
                    return Err(refusal(
                        "serial apply: terminal descriptor differs from expectation",
                    ));
                }
                self.validator
                    .take()
                    .ok_or_else(|| refusal("serial apply: missing input validator"))?
                    .finish(completed)
                    .map_err(input_error)?;
                self.complete = true;
                Ok(())
            }
        };
        if outcome.is_ok() {
            self.failed = false;
        }
        outcome
    }

    fn frame(
        &mut self,
        frame: &Frame,
        event: &mut impl FnMut(&Event, usize) -> Result<()>,
    ) -> Result<()> {
        self.validator
            .as_mut()
            .ok_or_else(|| refusal("serial apply: missing input validator"))?
            .push(frame)
            .map_err(input_error)?;
        if frame.payload.capacity() > self.budgets.frame_heap_bytes {
            return Err(refusal("serial apply: frame heap capacity budget exceeded"));
        }
        if frame.part == 0 && frame.last_part {
            return with_decoded_event(&frame.payload, self.wire, &mut self.decoded, event)
                .map_err(input_error)?;
        }
        let length = add(self.parts.len(), frame.payload.len())?;
        if length > self.budgets.reassembly_heap_bytes {
            return Err(refusal(
                "serial apply: multipart reassembly budget exceeded",
            ));
        }
        self.parts
            .try_reserve_exact(frame.payload.len())
            .map_err(|_| refusal("serial apply: reassembly allocation failed"))?;
        if self.parts.capacity() > self.budgets.reassembly_heap_bytes {
            return Err(refusal(
                "serial apply: multipart reassembly capacity budget exceeded",
            ));
        }
        self.parts.extend_from_slice(&frame.payload);
        if frame.last_part {
            with_decoded_event(&self.parts, self.wire, &mut self.decoded, event)
                .map_err(input_error)??;
            self.parts.clear();
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        if self.failed || !self.complete {
            return Err(refusal(
                "serial apply: poisoned or incomplete transaction input",
            ));
        }
        Ok(())
    }
}

struct CachedRelation {
    event: Event,
    checked: bool,
}

struct CachedPlan {
    relation: RelationKey,
    plan: DmlPlan,
    name: String,
    ordinal: u64,
}

struct Caches {
    relations: Vec<CachedRelation>,
    plans: Vec<CachedPlan>,
    relation_heap: usize,
    plan_heap: usize,
    hot: Option<usize>,
    next_statement: u64,
}

impl Caches {
    fn new(budgets: SerialBudgets) -> Result<Self> {
        budgets.validate()?;
        Ok(Self {
            relations: reserve_slots(budgets.relation_slots, budgets.relation_bytes)?,
            plans: reserve_slots(budgets.plan_slots, budgets.plan_bytes)?,
            relation_heap: 0,
            plan_heap: 0,
            hot: None,
            next_statement: 0,
        })
    }

    fn begin(&mut self) {
        for relation in &mut self.relations {
            relation.checked = false;
        }
    }

    fn apply(
        &mut self,
        event: &Event,
        charged: usize,
        source: &mut VerifiedCdcSource<'_>,
        target: &mut TargetTxn<'_>,
        budgets: SerialBudgets,
        io: &IoControl,
    ) -> Result<()> {
        if matches!(event, Event::Relation { .. }) {
            return self.relation(event, charged, source, target, budgets, io);
        }
        let key = row_relation(event)?;
        let relation = self
            .relations
            .iter()
            .position(|entry| relation_key(&entry.event) == key && entry.checked)
            .ok_or_else(|| {
                refusal("serial apply: row precedes verified relation context in this transaction")
            })?;
        let index = match self.find_plan(key, event)? {
            Some(index) => index,
            None => {
                let plan = DmlPlan::new(&self.relations[relation].event, event)?;
                self.prepare_plan(key, plan, target, budgets, io)?
            }
        };
        let entry = &self.plans[index];
        if multiply(entry.plan.param_types().len(), size_of::<PgParam<'_>>())? > budgets.bind_bytes
        {
            return Err(refusal("serial apply: parameter binding budget exceeded"));
        }
        let params = entry.plan.bind(event)?;
        if multiply(params.capacity(), size_of::<PgParam<'_>>())? > budgets.bind_bytes {
            return Err(refusal(
                "serial apply: parameter binding capacity budget exceeded",
            ));
        }
        let result = target.execute_prepared(&entry.name, &params, io)?;
        require_one_row(event, result.command_tag()?)
    }

    fn relation(
        &mut self,
        event: &Event,
        charged: usize,
        source: &mut VerifiedCdcSource<'_>,
        target: &mut TargetTxn<'_>,
        budgets: SerialBudgets,
        io: &IoControl,
    ) -> Result<()> {
        let key = relation_key(event);
        if let Some(entry) = self
            .relations
            .iter_mut()
            .find(|entry| relation_key(&entry.event) == key)
        {
            if entry.event != *event {
                return Err(refusal("serial apply: conflicting relation metadata"));
            }
            if !entry.checked {
                source.check_relation(event, io)?;
                target.preflight_relation(event, io)?;
                entry.checked = true;
            }
            return Ok(());
        }
        if self
            .relations
            .iter()
            .any(|entry| relation_key(&entry.event).oid == key.oid)
        {
            return Err(refusal("serial apply: conflicting relation generation"));
        }
        let base = multiply(self.relations.capacity(), size_of::<CachedRelation>())?;
        if add(base, charged)? > budgets.relation_bytes {
            return Err(refusal(
                "serial apply: single relation exceeds cache byte budget",
            ));
        }
        while self.relations.len() >= budgets.relation_slots
            || add(add(base, self.relation_heap)?, charged)? > budgets.relation_bytes
        {
            let index = self
                .relations
                .iter()
                .position(|entry| !entry.checked)
                .ok_or_else(|| {
                    refusal("serial apply: transaction relation count or byte budget exhausted")
                })?;
            let old = relation_key(&self.relations[index].event);
            self.retire_relation(old, target, io)?;
            let removed = self.relations.remove(index);
            self.relation_heap -= relation_heap_bytes(&removed.event)?;
        }
        let owned = event.clone();
        let bytes = relation_heap_bytes(&owned)?;
        if add(add(base, self.relation_heap)?, bytes)? > budgets.relation_bytes {
            return Err(refusal(
                "serial apply: relation clone capacity exceeds cache byte budget",
            ));
        }
        source.check_relation(event, io)?;
        target.preflight_relation(event, io)?;
        self.relation_heap = add(self.relation_heap, bytes)?;
        self.relations.push(CachedRelation {
            event: owned,
            checked: true,
        });
        Ok(())
    }

    fn find_plan(&mut self, key: RelationKey, row: &Event) -> Result<Option<usize>> {
        if let Some(index) = self.hot {
            let entry = &self.plans[index];
            if entry.relation == key && entry.plan.matches(row)? {
                return Ok(Some(index));
            }
        }
        // The hot path avoids a scan; misses remain bounded by plan_slots and row width.
        for (index, entry) in self.plans.iter().enumerate() {
            if Some(index) != self.hot && entry.relation == key && entry.plan.matches(row)? {
                self.hot = Some(index);
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    fn prepare_plan(
        &mut self,
        key: RelationKey,
        plan: DmlPlan,
        target: &mut TargetTxn<'_>,
        budgets: SerialBudgets,
        io: &IoControl,
    ) -> Result<usize> {
        let ordinal = self.next_statement;
        self.next_statement = ordinal
            .checked_add(1)
            .ok_or_else(|| refusal("serial apply: statement identity exhausted"))?;
        let name = format!("sa_{ordinal:016x}");
        let bytes = add(plan.owned_capacity_bytes()?, name.capacity())?;
        let base = multiply(self.plans.capacity(), size_of::<CachedPlan>())?;
        if add(base, bytes)? > budgets.plan_bytes {
            return Err(refusal(
                "serial apply: single plan exceeds cache byte budget",
            ));
        }
        while self.plans.len() >= budgets.plan_slots
            || add(add(base, self.plan_heap)?, bytes)? > budgets.plan_bytes
        {
            let index = self
                .plans
                .iter()
                .enumerate()
                .min_by_key(|(_, entry)| entry.ordinal)
                .map(|(index, _)| index)
                .ok_or_else(|| refusal("serial apply: prepared plan budget exhausted"))?;
            self.retire_plan(index, target, io)?;
        }
        target.prepare(&name, plan.sql(), plan.param_types(), io)?;
        self.plan_heap = add(self.plan_heap, bytes)?;
        let index = self.plans.len();
        self.plans.push(CachedPlan {
            relation: key,
            plan,
            name,
            ordinal,
        });
        self.hot = Some(index);
        Ok(index)
    }

    fn retire_relation(
        &mut self,
        key: RelationKey,
        target: &mut TargetTxn<'_>,
        io: &IoControl,
    ) -> Result<()> {
        while let Some(index) = self.plans.iter().position(|entry| entry.relation == key) {
            self.retire_plan(index, target, io)?;
        }
        Ok(())
    }

    fn retire_plan(
        &mut self,
        index: usize,
        target: &mut TargetTxn<'_>,
        io: &IoControl,
    ) -> Result<()> {
        target.deallocate(&self.plans[index].name, io)?;
        let old = self.plans.swap_remove(index);
        self.plan_heap -= add(old.plan.owned_capacity_bytes()?, old.name.capacity())?;
        self.hot = None;
        Ok(())
    }
}

fn reserve_slots<T>(count: usize, budget: usize) -> Result<Vec<T>> {
    if multiply(count, size_of::<T>())? > budget {
        return Err(refusal(
            "serial apply: cache inline storage exceeds byte budget",
        ));
    }
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(count)
        .map_err(|_| refusal("serial apply: cache allocation failed"))?;
    if multiply(entries.capacity(), size_of::<T>())? > budget {
        return Err(refusal(
            "serial apply: cache container capacity exceeds byte budget",
        ));
    }
    Ok(entries)
}

fn relation_key(event: &Event) -> RelationKey {
    match event {
        Event::Relation { key, .. } => *key,
        _ => unreachable!("cache retains only relation events"),
    }
}

fn row_relation(event: &Event) -> Result<RelationKey> {
    match event {
        Event::Insert { relation, .. }
        | Event::Update { relation, .. }
        | Event::Delete { relation, .. } => Ok(*relation),
        Event::Truncate { .. } => Err(refusal(
            "serial apply: TRUNCATE requires verified target scope closure",
        )),
        Event::Relation { .. } => Err(refusal("serial apply: a row event is required")),
    }
}

fn relation_heap_bytes(event: &Event) -> Result<usize> {
    let Event::Relation {
        schema,
        name,
        columns,
        ..
    } = event
    else {
        return Err(refusal("serial apply: relation metadata is required"));
    };
    let mut bytes = add(
        add(schema.capacity(), name.capacity())?,
        multiply(columns.capacity(), size_of::<Column>())?,
    )?;
    for column in columns {
        bytes = add(bytes, column.name.capacity())?;
    }
    Ok(bytes)
}

fn require_one_row(event: &Event, tag: &str) -> Result<()> {
    let expected = match event {
        Event::Insert { .. } => "INSERT 0 1",
        Event::Update { .. } => "UPDATE 1",
        Event::Delete { .. } => "DELETE 1",
        _ => {
            return Err(refusal(
                "serial apply: exact row count requires a DML event",
            ));
        }
    };
    if tag != expected {
        return Err(refusal("serial apply: DML did not affect exactly one row"));
    }
    Ok(())
}

fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| refusal("serial apply: byte accounting overflow"))
}
fn multiply(left: usize, right: usize) -> Result<usize> {
    left.checked_mul(right)
        .ok_or_else(|| refusal("serial apply: byte accounting overflow"))
}
fn input_error(error: codec::Error) -> Error {
    Error {
        kind: ErrorKind::BadState,
        message: format!("serial apply: invalid required CDC input: {error}"),
    }
}
fn refusal(message: &str) -> Error {
    Error {
        kind: ErrorKind::Target,
        message: message.to_owned(),
    }
}

#[cfg(test)]
#[path = "serial/tests.rs"]
mod tests;
