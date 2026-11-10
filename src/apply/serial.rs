use super::CommitOutcome;
use super::checkpoint::TargetTxn;
use super::plan::{DmlPlan, quote_identifier};
use crate::catalog::FollowRelation;
use crate::cdc::contracts::{Column, Event, RelationKey, Value};
use crate::contracts::{
    DurabilityMode, Error, ErrorKind, FollowBinding, IoControl, OwnedCopyTable, Result,
};
use crate::pg::{PgParam, PgResult, Pipeline};

const PLAN_SLOTS: usize = 128;
const RELATION_SLOTS: usize = 4096;
const CACHE_BYTES: usize = 8 * 1024 * 1024;
const WINDOW_COMMANDS: usize = 62;
const PIPELINE_BYTES: usize = 1024 * 1024;
const CHECKPOINT_RESERVE: usize = 256 * 1024 + 6;
const WINDOW_BYTES: usize = PIPELINE_BYTES - CHECKPOINT_RESERVE;

struct CachedPlan {
    key: RelationKey,
    plan: DmlPlan,
    name: String,
}
enum Pending {
    Row { plan: usize, event: Event },
    Truncate(String),
}

pub(super) struct Caches {
    relations: Vec<FollowRelation>,
    plans: Vec<CachedPlan>,
    pending: Vec<Pending>,
    pending_bytes: usize,
    pending_heap: usize,
    next_statement: u64,
    hot: Option<usize>,
    #[cfg(test)]
    pub(super) serial: bool,
}
impl Caches {
    pub(super) fn new() -> Self {
        Self {
            relations: Vec::with_capacity(RELATION_SLOTS),
            plans: Vec::with_capacity(PLAN_SLOTS),
            pending: Vec::new(),
            pending_bytes: 0,
            pending_heap: 0,
            next_statement: 0,
            hot: None,
            #[cfg(test)]
            serial: false,
        }
    }
    pub(super) fn event(
        &mut self,
        event: Event,
        target: &mut TargetTxn<'_>,
        binding: &FollowBinding,
        owned: &[OwnedCopyTable],
        io: &IoControl,
    ) -> Result<usize> {
        let charged = event_bytes(&event)?;
        if matches!(event, Event::Relation { .. }) {
            self.relation(event, target, binding, owned, io)?;
            return Ok(charged);
        }
        if let Event::Truncate {
            relations,
            restart_identity,
            ..
        } = &event
        {
            let mut names = Vec::with_capacity(relations.len());
            if relations.is_empty() {
                return Err(refusal("follow apply: empty TRUNCATE"));
            }
            for (index, key) in relations.iter().enumerate() {
                if relations[..index].contains(key) {
                    return Err(refusal("follow apply: duplicate TRUNCATE relation"));
                }
                let checked = self
                    .relations
                    .iter()
                    .find(|r| relation_key(&r.relation) == *key)
                    .ok_or_else(|| refusal("follow apply: TRUNCATE relation is unchecked"))?;
                let Event::Relation { schema, name, .. } = &checked.relation else {
                    unreachable!()
                };
                names.push(format!(
                    "ONLY {}.{}",
                    quote_identifier(schema)?,
                    quote_identifier(name)?
                ));
            }
            // No CASCADE: PostgreSQL refuses an incomplete dependency closure atomically.
            let sql = format!(
                "TRUNCATE TABLE {} {}",
                names.join(", "),
                if *restart_identity {
                    "RESTART IDENTITY"
                } else {
                    "CONTINUE IDENTITY"
                }
            );
            let bytes = add(sql.len(), 1)?;
            if bytes > WINDOW_BYTES {
                return Err(refusal("follow apply: TRUNCATE exceeds pipeline budget"));
            }
            self.make_room(bytes, charged, target, io)?;
            self.pending.push(Pending::Truncate(sql));
            self.pending_bytes = add(self.pending_bytes, bytes)?;
            self.pending_heap = add(self.pending_heap, charged)?;
            return Ok(charged);
        }
        let key = row_relation(&event)?;
        let relation = self
            .relations
            .iter()
            .position(|r| relation_key(&r.relation) == key)
            .ok_or_else(|| refusal("follow apply: row precedes checked relation"))?;
        let mut found = match self.hot {
            Some(index)
                if self.plans[index].key == key && self.plans[index].plan.matches(&event)? =>
            {
                Some(index)
            }
            _ => None,
        };
        for (index, entry) in self.plans.iter().enumerate() {
            if found.is_some() {
                break;
            }
            if entry.key == key && entry.plan.matches(&event)? {
                found = Some(index);
                break;
            }
        }
        let index = match found {
            Some(index) => index,
            None => {
                // Preparing/evicting waits only on a cold shape and cannot overtake queued DML.
                self.flush(target, io)?;
                let plan = DmlPlan::new(&self.relations[relation], &event)?;
                if plan.owned_capacity_bytes()? > CACHE_BYTES {
                    return Err(refusal("follow apply: plan exceeds cache budget"));
                }
                while self.plans.len() >= PLAN_SLOTS
                    || add(add(self.plan_bytes()?, plan.owned_capacity_bytes()?)?, 20)?
                        > CACHE_BYTES
                {
                    if self.plans.is_empty() {
                        return Err(refusal("follow apply: plan cache byte budget exhausted"));
                    }
                    target.deallocate(&self.plans[0].name, io)?;
                    self.plans.remove(0);
                }
                let ordinal = self.next_statement;
                self.next_statement = ordinal
                    .checked_add(1)
                    .ok_or_else(|| refusal("follow apply: prepared identity exhausted"))?;
                let name = format!("fa_{ordinal:016x}");
                target.prepare(&name, plan.sql(), plan.param_types(), io)?;
                self.plans.push(CachedPlan { key, plan, name });
                self.plans.len() - 1
            }
        };
        self.hot = Some(index);
        let params = self.plans[index].plan.bind(&event)?;
        let bytes = prepared_bytes(&self.plans[index].name, &params)?;
        if bytes > WINDOW_BYTES || charged > CACHE_BYTES || self.serial_mode() {
            self.flush(target, io)?;
            let entry = &self.plans[index];
            let params = entry.plan.bind(&event)?;
            require_result(
                &entry.plan,
                target.execute_prepared(&entry.name, &params, io)?,
            )?;
        } else {
            self.make_room(bytes, charged, target, io)?;
            self.pending.push(Pending::Row { plan: index, event });
            self.pending_bytes = add(self.pending_bytes, bytes)?;
            self.pending_heap = add(self.pending_heap, charged)?;
        }
        Ok(charged)
    }
    fn serial_mode(&self) -> bool {
        #[cfg(test)]
        {
            self.serial
        }
        #[cfg(not(test))]
        {
            false
        }
    }
    fn relation(
        &mut self,
        event: Event,
        target: &mut TargetTxn<'_>,
        binding: &FollowBinding,
        owned: &[OwnedCopyTable],
        io: &IoControl,
    ) -> Result<()> {
        let key = relation_key(&event);
        if let Some(old) = self
            .relations
            .iter()
            .find(|r| relation_key(&r.relation).oid == key.oid)
        {
            if old.relation != event {
                return Err(refusal("follow apply: frozen relation changed"));
            }
            return Ok(());
        }
        let table = owned
            .iter()
            .find(|t| t.table.source_oid == key.oid)
            .ok_or_else(|| refusal("follow apply: relation outside owned source plan"))?;
        let bytes = self.relations.iter().try_fold(
            self.relations.capacity() * size_of::<FollowRelation>(),
            |total, r| {
                add(
                    add(
                        add(total, event_bytes(&r.relation)?)?,
                        r.target_type_oids.capacity() * size_of::<u32>(),
                    )?,
                    r.identity_modes.capacity() * size_of::<u8>(),
                )
            },
        )?;
        if self.relations.len() >= RELATION_SLOTS
            || add(
                add(bytes, event_bytes(&event)?)?,
                1600 * (size_of::<u32>() + size_of::<u8>()),
            )? > CACHE_BYTES
        {
            return Err(refusal("follow apply: checked relation cache exhausted"));
        }
        self.flush(target, io)?;
        self.relations
            .push(target.preflight_follow_relation(&event, binding, table, io)?);
        Ok(())
    }
    fn plan_bytes(&self) -> Result<usize> {
        self.plans.iter().try_fold(
            self.plans.capacity() * size_of::<CachedPlan>(),
            |bytes, p| {
                add(
                    add(bytes, p.name.capacity())?,
                    p.plan.owned_capacity_bytes()?,
                )
            },
        )
    }
    fn make_room(
        &mut self,
        bytes: usize,
        heap: usize,
        target: &mut TargetTxn<'_>,
        io: &IoControl,
    ) -> Result<()> {
        if self.pending.len() >= WINDOW_COMMANDS
            || add(self.pending_bytes, bytes)? > WINDOW_BYTES
            || add(self.pending_heap, heap)? > CACHE_BYTES
        {
            self.flush(target, io)?;
        }
        Ok(())
    }
    pub(super) fn flush(&mut self, target: &mut TargetTxn<'_>, io: &IoControl) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        target.pipeline(
            io,
            |pipeline| self.send(pipeline, io),
            |index, result| self.validate(index, result),
        )?;
        self.pending.clear();
        self.pending_bytes = 0;
        self.pending_heap = 0;
        Ok(())
    }
    pub(super) fn commit(
        &mut self,
        mut target: TargetTxn<'_>,
        durability: DurabilityMode,
        io: &IoControl,
    ) -> Result<CommitOutcome> {
        if add(self.pending_bytes, target.checkpoint_input_bytes()?)? > PIPELINE_BYTES {
            self.flush(&mut target, io)?;
        }
        let outcome = target.commit_pipeline(
            durability,
            io,
            |pipeline| self.send(pipeline, io),
            |index, result| self.validate(index, result),
        )?;
        self.pending.clear();
        self.pending_bytes = 0;
        self.pending_heap = 0;
        Ok(outcome)
    }
    fn send(&self, pipeline: &mut Pipeline<'_>, io: &IoControl) -> Result<()> {
        for pending in &self.pending {
            match pending {
                Pending::Row { plan, event } => {
                    let entry = &self.plans[*plan];
                    pipeline.execute_prepared(&entry.name, &entry.plan.bind(event)?, io)?;
                }
                Pending::Truncate(sql) => pipeline.query(sql, io)?,
            }
        }
        Ok(())
    }
    fn validate(&self, index: usize, result: PgResult) -> Result<()> {
        match self
            .pending
            .get(index)
            .ok_or_else(|| refusal("follow apply: unexpected pipeline result"))?
        {
            Pending::Row { plan, .. } => require_result(&self.plans[*plan].plan, result),
            Pending::Truncate(_)
                if result.rows() == 0 && result.command_tag()? == "TRUNCATE TABLE" =>
            {
                Ok(())
            }
            _ => Err(refusal("follow apply: unexpected TRUNCATE result")),
        }
    }
}
pub(super) fn require_result(plan: &DmlPlan, result: PgResult) -> Result<()> {
    if result.rows() != 0 {
        return Err(refusal("follow apply: unexpected returned rows"));
    }
    require_one_row(plan, result.command_tag()?)
}
fn require_one_row(plan: &DmlPlan, tag: &str) -> Result<()> {
    if tag != plan.expected_tag() {
        return Err(refusal("follow apply: DML did not affect exactly one row"));
    }
    Ok(())
}
fn prepared_bytes(name: &str, params: &[PgParam<'_>]) -> Result<usize> {
    params.iter().try_fold(add(name.len(), 1)?, |bytes, p| {
        add(
            bytes,
            match p {
                PgParam::Null => 4,
                PgParam::Text(value) => add(value.len(), 5)?,
                #[cfg(test)]
                PgParam::Binary(value) => add(value.len(), 4)?,
            },
        )
    })
}
pub(super) fn event_bytes(event: &Event) -> Result<usize> {
    let values = |values: &Vec<Value>| {
        values
            .iter()
            .try_fold(values.capacity() * size_of::<Value>(), |bytes, value| {
                add(
                    bytes,
                    match value {
                        Value::Text(b) => b.capacity(),
                        _ => 0,
                    },
                )
            })
    };
    let bytes = match event {
        Event::Relation {
            schema,
            name,
            columns,
            ..
        } => columns.iter().try_fold(
            add(
                add(schema.capacity(), name.capacity())?,
                columns.capacity() * size_of::<Column>(),
            )?,
            |bytes, c| add(bytes, c.name.capacity()),
        )?,
        Event::Insert { values: row, .. } => values(row)?,
        Event::Update { old, new, .. } => add(
            values(new)?,
            old.as_ref().map_or(Ok(0), |old| values(&old.values))?,
        )?,
        Event::Delete { old, .. } => values(&old.values)?,
        Event::Truncate { relations, .. } => relations.capacity() * size_of::<RelationKey>(),
    };
    add(size_of::<Event>(), bytes)
}
fn relation_key(event: &Event) -> RelationKey {
    match event {
        Event::Relation { key, .. } => *key,
        _ => unreachable!(),
    }
}
fn row_relation(event: &Event) -> Result<RelationKey> {
    match event {
        Event::Insert { relation, .. }
        | Event::Update { relation, .. }
        | Event::Delete { relation, .. } => Ok(*relation),
        _ => Err(refusal("follow apply: row required")),
    }
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| refusal("follow apply: byte accounting overflow"))
}
#[track_caller]
pub(super) fn refusal(message: &'static str) -> Error {
    Error::new(ErrorKind::Target, message)
}
#[cfg(test)]
#[path = "serial/tests.rs"]
mod tests;
