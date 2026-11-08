use super::*;
use std::collections::BTreeSet;

pub(crate) const MAX_CLONE_TABLES: usize = 4096;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClonePhase {
    Fresh,
    PreDataPending,
    Copying,
    PostDataPending,
    PostDataComplete,
    SequencesPending,
    SequencesComplete,
    Analyzing,
    Complete,
}

impl ClonePhase {
    pub(crate) fn permits(self, next: Self) -> bool {
        self == next
            || matches!(
                (self, next),
                (Self::Fresh, Self::PreDataPending)
                    | (Self::PreDataPending, Self::Copying)
                    | (Self::Copying, Self::PostDataPending)
                    | (Self::PostDataPending, Self::PostDataComplete)
                    | (Self::PostDataComplete, Self::SequencesPending)
                    | (Self::SequencesPending, Self::SequencesComplete)
                    | (Self::SequencesComplete, Self::Analyzing)
                    | (Self::Analyzing, Self::Complete)
            )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloneState {
    pub(crate) plan: ClonePlan,
    pub(crate) ticket: SnapshotTicket,
    pub(crate) phase: ClonePhase,
    pub(crate) owned_objects: Vec<OwnedObject>,
    pub(crate) owned_tables: Vec<OwnedCopyTable>,
    pub(crate) completion: Option<CopyReport>,
}

#[derive(Clone, Debug)]
pub(crate) struct CloneInspection {
    pub(crate) clone_state: CloneState,
    pub(crate) provenance: ConfirmedProvenance,
}

fn check_io(io: &IoControl) -> Result<()> {
    if io.cancelled.load(std::sync::atomic::Ordering::Acquire) {
        return Err(bad_state("state-cancelled: operation cancelled"));
    }
    if io.is_expired() {
        return Err(bad_state("state-timeout: operation deadline expired"));
    }
    Ok(())
}

fn validate_operator(operator: &OperatorIdentity) -> Result<()> {
    let run: String = operator
        .run
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if operator.run.0 == [0; 16]
        || operator.checkpoint_generation != 1
        || operator.migration_uid.len() != 36
        || operator.migration_uid.replace('-', "") != run
        || !operator.migration_uid.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
        || operator.workdir_id.is_empty()
        || operator.workdir_id.len() > 256
        || operator.workdir_id.trim() != operator.workdir_id
        || operator
            .workdir_id
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\'))
    {
        return Err(bad_state(
            "state-operator: canonical operator identity is required",
        ));
    }
    Ok(())
}

pub(crate) fn open_clone(
    request: &StateRequest,
    operator: Option<&OperatorIdentity>,
    io: &IoControl,
) -> Result<NativeState> {
    check_io(io)?;
    if let Some(operator) = operator {
        validate_operator(operator)?;
    }
    let mut state = open_native(request, true)?;
    if state.fresh_created {
        let mut next = state.checked_manifest()?;
        if let Some(operator) = operator {
            next.run = operator.run;
            next.operator = Some(operator.clone());
            state.identity.run = operator.run;
        }
        state.replace_manifest(next)?;
    } else if state.manifest.operator.as_ref() != operator {
        return Err(bad_state(
            "state-operator: requested identity differs from native binding",
        ));
    }
    if request.action != StateAction::Create && state.manifest.clone_state.is_none() {
        return Err(bad_state(
            "state-clone: native state has no plain clone binding",
        ));
    }
    Ok(state)
}

pub(crate) fn inspect_clone(
    request: &StateRequest,
    operator: Option<&OperatorIdentity>,
    io: &IoControl,
) -> Result<CloneInspection> {
    check_io(io)?;
    if let Some(operator) = operator {
        validate_operator(operator)?;
    }
    let leaf = request
        .path
        .file_name()
        .ok_or_else(|| bad_state("state-unsafe: state path is invalid"))?;
    let parent_path = fs::canonicalize(
        request
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
    .map_err(|_| bad_state("state-parent: state parent is unavailable"))?;
    let parent = open_parent(&parent_path)?;
    let name = CString::new(leaf.as_bytes())
        .map_err(|_| bad_state("state-unsafe: state path is invalid"))?;
    let handle = open_at(&parent, &name, DIRECTORY_FLAGS, 0)
        .map_err(|_| bad_state("state-missing: native state is unavailable"))?;
    check_private(&metadata(&handle)?, 0o700)?;
    let directory = Directory {
        parent_path,
        parent,
        name,
        handle,
    };
    directory.check()?;
    let path = directory.parent_path.join(leaf);
    if !manifest_present(&directory.handle, false)? {
        return Err(bad_state("state-missing: native manifest is unavailable"));
    }
    let (manifest, _) = read_manifest(&directory, &path)?;
    if manifest.operator.as_ref() != operator {
        return Err(bad_state(
            "state-operator: requested identity differs from native binding",
        ));
    }
    let clone_state = manifest
        .clone_state
        .ok_or_else(|| bad_state("state-clone: native clone binding is absent"))?;
    let provenance = match manifest.target_provenance.as_slice() {
        [TargetProvenance::Confirmed(p)] => p.clone(),
        _ => {
            return Err(bad_state(
                "state-clone: confirmed target metadata is absent",
            ));
        }
    };
    if provenance.intent.binding.stream.source != clone_state.plan.source.identity {
        return Err(bad_state(
            "state-clone: source and target provenance differ",
        ));
    }
    Ok(CloneInspection {
        clone_state,
        provenance,
    })
}

impl NativeState {
    pub(crate) fn bind_clone(&mut self, plan: &ClonePlan, ticket: &SnapshotTicket) -> Result<()> {
        let mut next = self.checked_manifest()?;
        let clone = CloneState {
            plan: plan.clone(),
            ticket: ticket.clone(),
            phase: ClonePhase::Fresh,
            owned_objects: Vec::new(),
            owned_tables: Vec::new(),
            completion: None,
        };
        clone.validate()?;
        if let Some(existing) = &next.clone_state {
            if existing.plan != *plan || existing.ticket != *ticket {
                return Err(bad_state("state-clone: clone binding cannot be replaced"));
            }
            return self.synchronize_manifest();
        }
        if plan.generation != CopyGeneration(1) || next.source_slot.is_some() {
            return Err(bad_state(
                "state-clone: plain clone starts at generation one without a source slot",
            ));
        }
        next.clone_state = Some(clone);
        self.replace_manifest(next)
    }

    pub(crate) fn checked_clone(&self) -> Result<Option<CloneState>> {
        Ok(self.checked_manifest()?.clone_state)
    }

    pub(crate) fn owned_copy_tables(&self) -> Result<Vec<OwnedCopyTable>> {
        Ok(self
            .checked_clone()?
            .ok_or_else(|| bad_state("state-clone: clone binding is absent"))?
            .owned_tables)
    }

    pub(crate) fn begin_pre_data(&mut self) -> Result<()> {
        self.mark_stage(ClonePhase::PreDataPending)
    }

    pub(crate) fn confirm_pre_data(
        &mut self,
        objects: Vec<OwnedObject>,
        tables: Vec<OwnedCopyTable>,
    ) -> Result<()> {
        let mut next = self.checked_manifest()?;
        let clone = next
            .clone_state
            .as_mut()
            .ok_or_else(|| bad_state("state-clone: clone binding is absent"))?;
        if clone.phase != ClonePhase::PreDataPending {
            return Err(bad_state(
                "state-ownership: positive pre-data handoff is not pending",
            ));
        }
        clone.owned_objects = objects;
        clone.owned_tables = tables;
        clone.phase = ClonePhase::Copying;
        clone.validate()?;
        self.replace_manifest(next)
    }

    pub(crate) fn advance_copy_generation(&mut self, ticket: &SnapshotTicket) -> Result<()> {
        let mut next = self.checked_manifest()?;
        let clone = next
            .clone_state
            .as_mut()
            .ok_or_else(|| bad_state("state-clone: clone binding is absent"))?;
        if clone.phase != ClonePhase::Copying {
            return Err(bad_state(
                "state-recovery: recopy requires positively owned pre-postdata state",
            ));
        }
        let generation = clone
            .plan
            .generation
            .0
            .checked_add(1)
            .ok_or_else(|| bad_state("state-generation: copy generation exhausted"))?;
        if ticket.source != clone.plan.source || ticket.generation != CopyGeneration(generation) {
            return Err(bad_state(
                "state-generation: fresh snapshot source or generation differs",
            ));
        }
        clone.ticket = ticket.clone();
        clone.plan.generation = ticket.generation;
        for table in &mut clone.owned_tables {
            table.generation = ticket.generation;
        }
        clone.completion = None;
        clone.validate()?;
        self.replace_manifest(next)
    }

    pub(crate) fn mark_stage(&mut self, phase: ClonePhase) -> Result<()> {
        let mut next = self.checked_manifest()?;
        let clone = next
            .clone_state
            .as_mut()
            .ok_or_else(|| bad_state("state-clone: clone binding is absent"))?;
        if !clone.phase.permits(phase)
            || phase == ClonePhase::Complete
            || phase == ClonePhase::Copying
        {
            return Err(bad_state(
                "state-stage: clone stage transition is unsupported",
            ));
        }
        clone.phase = phase;
        self.replace_manifest(next)
    }

    pub(crate) fn record_clone_complete(&mut self, report: &CopyReport) -> Result<()> {
        let mut next = self.checked_manifest()?;
        let clone = next
            .clone_state
            .as_mut()
            .ok_or_else(|| bad_state("state-clone: clone binding is absent"))?;
        if clone.phase != ClonePhase::Analyzing || report.tables != clone.owned_tables.len() as u64
        {
            return Err(bad_state(
                "state-complete: clone stage or table count differs",
            ));
        }
        clone.phase = ClonePhase::Complete;
        clone.completion = Some(report.clone());
        self.replace_manifest(next)
    }
}

impl CloneState {
    pub(crate) fn validate(&self) -> Result<()> {
        let plan = &self.plan;
        let valid_name = |s: &str| !s.is_empty() && s.len() <= 63 && !s.contains('\0');
        if plan.generation.0 == 0
            || self.ticket.generation != plan.generation
            || self.ticket.source != plan.source
            || self.ticket.token.is_empty()
            || self.ticket.token.len() > MAX_SNAPSHOT_TOKEN_BYTES
            || self.ticket.token.contains('\0')
            || plan.source.identity.system_identifier == 0
            || plan.source.identity.database_oid == 0
            || plan.source.timeline == 0
            || plan.target.identity.system_identifier == 0
            || plan.target.identity.database_oid == 0
            || ![16, 18].contains(&plan.source.major)
            || ![16, 18].contains(&plan.target.major)
            || plan.source.major > plan.target.major
            || plan.tables.len() > MAX_CLONE_TABLES
        {
            return Err(bad_state(
                "state-clone: invalid clone identity, snapshot, generation, or profile",
            ));
        }
        let mut sources = BTreeSet::new();
        let mut names = BTreeSet::new();
        for table in &plan.tables {
            if table.source_oid == 0
                || !sources.insert(table.source_oid)
                || !names.insert((&table.name.schema, &table.name.table))
                || !valid_name(&table.name.schema)
                || !valid_name(&table.name.table)
                || table.all_columns.len() > 1600
                || table
                    .writable_columns
                    .iter()
                    .any(|c| !table.all_columns.contains(c))
                || table.all_columns.iter().any(|c| !valid_name(c))
                || table.all_columns.iter().collect::<BTreeSet<_>>().len()
                    != table.all_columns.len()
                || table.writable_columns.iter().collect::<BTreeSet<_>>().len()
                    != table.writable_columns.len()
            {
                return Err(bad_state(
                    "state-clone: invalid or duplicate source table identity",
                ));
            }
        }
        let owned_phase = !matches!(self.phase, ClonePhase::Fresh | ClonePhase::PreDataPending);
        if owned_phase && self.owned_tables.len() != plan.tables.len() {
            return Err(bad_state(
                "state-ownership: complete exact table ownership is required",
            ));
        }
        if !owned_phase && (!self.owned_tables.is_empty() || !self.owned_objects.is_empty()) {
            return Err(bad_state(
                "state-ownership: objects cannot precede positive pre-data handoff",
            ));
        }
        let mut objects = BTreeSet::new();
        for object in &self.owned_objects {
            if object.class_oid == 0
                || object.object_oid == 0
                || object.object_sub_id < 0
                || object.owner_oid == 0
                || object.namespace_oid == Some(0)
                || object.name.is_empty()
                || !objects.insert((object.class_oid, object.object_oid, object.object_sub_id))
            {
                return Err(bad_state(
                    "state-ownership: invalid or duplicate owned object",
                ));
            }
        }
        let mut targets = BTreeSet::new();
        let mut copied_sources = BTreeSet::new();
        for owned in &self.owned_tables {
            if owned.generation != plan.generation
                || owned.target_oid == 0
                || owned.target_namespace_oid == 0
                || owned.target_owner_oid == 0
                || !targets.insert(owned.target_oid)
                || !copied_sources.insert(owned.table.source_oid)
                || !plan.tables.contains(&owned.table)
                || !self.owned_objects.iter().any(|o| {
                    o.class_oid == 1259
                        && o.object_oid == owned.target_oid
                        && o.object_sub_id == 0
                        && o.namespace_oid == Some(owned.target_namespace_oid)
                        && o.namespace_name.as_deref() == Some(&owned.table.name.schema)
                        && o.name == owned.table.name.table
                        && o.kind == "r"
                        && o.owner_oid == owned.target_owner_oid
                })
            {
                return Err(bad_state(
                    "state-ownership: table lacks exact created-object provenance",
                ));
            }
        }
        if (self.phase == ClonePhase::Complete) != self.completion.is_some()
            || self
                .completion
                .as_ref()
                .is_some_and(|r| r.tables != self.owned_tables.len() as u64)
        {
            return Err(bad_state("state-complete: incomplete clone receipt"));
        }
        if serde_json::to_vec(self)
            .map_err(|_| bad_state("state-clone: cannot encode clone binding"))?
            .len()
            > MAX_MANIFEST_BYTES as usize - 4096
        {
            return Err(bad_state(
                "state-clone: exact ownership inventory exceeds one MiB",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
