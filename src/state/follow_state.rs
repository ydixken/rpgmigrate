use super::*;
use crate::contracts::{FollowBinding, PublicationReceipt, SourceType};
use std::collections::BTreeSet;

pub(crate) const MAX_FOLLOW_CONTROL_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicationIntent {
    pub(crate) run: RunId,
    pub(crate) name: String,
    pub(crate) nonce: [u8; 16],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PublicationProvenance {
    Provisioning {
        intent: PublicationIntent,
    },
    Created {
        intent: PublicationIntent,
        receipt: PublicationReceipt,
    },
    Confirmed {
        intent: PublicationIntent,
        receipt: PublicationReceipt,
    },
}

impl PublicationProvenance {
    pub(crate) fn intent(&self) -> &PublicationIntent {
        match self {
            Self::Provisioning { intent }
            | Self::Created { intent, .. }
            | Self::Confirmed { intent, .. } => intent,
        }
    }

    pub(crate) fn confirmed(&self) -> Option<&PublicationReceipt> {
        match self {
            Self::Confirmed { receipt, .. } => Some(receipt),
            _ => None,
        }
    }

    fn validate(&self, run: RunId) -> Result<()> {
        let intent = self.intent();
        if intent.run != run || intent.nonce == [0; 16] || !valid_name(&intent.name) {
            return Err(bad_state("state-follow: invalid publication intent"));
        }
        match self {
            Self::Created { receipt, .. } | Self::Confirmed { receipt, .. }
                if receipt.name != intent.name || receipt.oid == 0 || receipt.owner_oid == 0 =>
            {
                Err(bad_state("state-follow: invalid publication receipt"))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FollowState {
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) cleanup: Option<CleanupProgress>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) origin_name: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) publication: Option<PublicationProvenance>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) binding: Option<FollowBinding>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) restart: Option<FollowRestart>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) preliminary: Option<ClonePlan>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CleanupProgress {
    pub(crate) slot_removed: bool,
    pub(crate) publication_removed: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FollowRestart {
    pub(crate) previous: FollowBinding,
    pub(crate) next_generation: CopyGeneration,
    pub(crate) slot_absent: bool,
    pub(crate) publication_absent: bool,
    pub(crate) new_bound: bool,
}

fn same_plan(a: &ClonePlan, b: &ClonePlan) -> bool {
    a.source == b.source
        && a.target == b.target
        && a.tables == b.tables
        && a.sequences == b.sequences
        && a.policy == b.policy
        && a.copy_policy == b.copy_policy
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FollowControl {
    pub(crate) binding: FollowBinding,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) operator: Option<OperatorIdentity>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) origin_name: Option<String>,
}

impl FollowControl {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_follow_binding(&self.binding)?;
        if let Some(operator) = &self.operator {
            clone_state::validate_operator(operator)?;
            if operator.run != self.binding.stream().run {
                return Err(bad_state("state-follow: operator and source run differ"));
            }
        }
        if self
            .origin_name
            .as_deref()
            .is_some_and(|name| !valid_name(name))
        {
            return Err(bad_state("state-follow: invalid unowned origin name"));
        }
        if serde_json::to_vec(self)
            .map_err(|_| bad_state("state-follow: cannot encode follow control"))?
            .len()
            > MAX_FOLLOW_CONTROL_BYTES
        {
            return Err(bad_state(
                "state-follow: follow control exceeds bounded inventory",
            ));
        }
        Ok(())
    }
}

fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 63 && !name.contains('\0')
}

pub(crate) fn validate_follow_source_types(
    types: &[SourceType],
    operator: Option<&OperatorIdentity>,
) -> Result<()> {
    if let Some(operator) = operator {
        clone_state::validate_operator(operator)?;
    }
    let mut seen = BTreeSet::new();
    for ty in types {
        if ty.oid == 0 || !valid_name(&ty.schema) || !valid_name(&ty.name) || !seen.insert(ty.oid) {
            return Err(bad_state("state-follow: invalid source type map"));
        }
    }
    // Reserve worst-case escaped snapshot/identifier strings and the full operator binding.
    if serde_json::to_vec(types)
        .map_err(|_| bad_state("state-follow: cannot encode source type map"))?
        .len()
        .checked_add(8192)
        .is_none_or(|bytes| bytes > MAX_FOLLOW_CONTROL_BYTES)
    {
        return Err(bad_state(
            "state-follow: source type map exceeds follow control budget",
        ));
    }
    Ok(())
}

pub(crate) fn validate_follow_binding(binding: &FollowBinding) -> Result<()> {
    let stream = binding.stream();
    SourceSlotProvenance::Confirmed(binding.slot.clone()).validate(stream.run)?;
    if stream.generation != 1
        || binding.slot.intent.binding.plugin() != Plugin::Pgoutput
        || binding.source_timeline == 0
        || binding.copy_generation.0 == 0
        || !valid_name(&binding.publication.name)
        || binding.publication.oid == 0
        || binding.publication.owner_oid == 0
        || binding.source_types.len() > 16_384
    {
        return Err(bad_state("state-follow: invalid follow binding"));
    }
    validate_follow_source_types(&binding.source_types, None)?;
    let mut types = BTreeSet::new();
    for ty in &binding.source_types {
        if ty.oid == 0 || !valid_name(&ty.schema) || !valid_name(&ty.name) || !types.insert(ty.oid)
        {
            return Err(bad_state("state-follow: invalid source type map"));
        }
    }
    if serde_json::to_vec(binding)
        .map_err(|_| bad_state("state-follow: cannot encode follow binding"))?
        .len()
        > MAX_FOLLOW_CONTROL_BYTES
    {
        return Err(bad_state(
            "state-follow: follow binding exceeds bounded inventory",
        ));
    }
    Ok(())
}

pub(crate) fn open_follow(
    request: &StateRequest,
    operator: Option<&OperatorIdentity>,
    origin_name: Option<&str>,
    io: &IoControl,
) -> Result<NativeState> {
    clone_state::check_io(io)?;
    if let Some(operator) = operator {
        clone_state::validate_operator(operator)?;
    }
    if origin_name.is_some_and(|name| !valid_name(name)) {
        return Err(bad_state("state-follow: invalid unowned origin name"));
    }
    let mut state = open_native(request, true, true)?;
    let mut next = state.checked_manifest()?;
    if state.fresh_created {
        if let Some(operator) = operator {
            next.run = operator.run;
            next.operator = Some(operator.clone());
            state.identity.run = operator.run;
        }
    } else if next.operator.as_ref() != operator {
        return Err(bad_state(
            "state-operator: requested identity differs from native binding",
        ));
    }
    if let Some(follow) = &next.follow_state {
        if follow.origin_name.as_deref() != origin_name {
            return Err(bad_state(
                "state-follow: unowned origin differs from native binding",
            ));
        }
    } else {
        if !next.is_init_only() {
            return Err(bad_state(
                "state-follow: plain clone or foreign state cannot become follow",
            ));
        }
        next.follow_state = Some(FollowState {
            cleanup: None,
            origin_name: origin_name.map(str::to_owned),
            publication: None,
            binding: None,
            restart: None,
            preliminary: None,
        });
    }
    // A mode-only INIT record has not authorized any source CREATE attempt.
    state.fresh_created = next.source_slot.is_none()
        && next
            .follow_state
            .as_ref()
            .is_some_and(|f| f.publication.is_none())
        && next.clone_state.is_none()
        && next.target_provenance.is_empty();
    state.replace_manifest(next)?;
    Ok(state)
}

impl NativeState {
    pub(crate) fn begin_follow_cleanup(&mut self) -> Result<CleanupProgress> {
        control::with_native_lock(self, |native| {
            let mut next = native.checked_manifest()?;
            let follow = next
                .follow_state
                .as_mut()
                .ok_or_else(|| bad_state("state-cleanup: native follow inventory is absent"))?;
            let binding = follow
                .binding
                .as_ref()
                .ok_or_else(|| bad_state("state-cleanup: confirmed follow binding is absent"))?;
            if follow.restart.is_some()
                || follow
                    .publication
                    .as_ref()
                    .and_then(PublicationProvenance::confirmed)
                    != Some(&binding.publication)
                || next.source_slot != Some(SourceSlotProvenance::Confirmed(binding.slot.clone()))
                || !matches!(
                    next.target_provenance.as_slice(),
                    [TargetProvenance::Confirmed(_)]
                )
            {
                return Err(bad_state(
                    "state-cleanup: complete confirmed native resource inventory is required",
                ));
            }
            if let Some(progress) = follow.cleanup {
                native.synchronize_manifest()?;
                return Ok(progress);
            }
            let progress = CleanupProgress {
                slot_removed: false,
                publication_removed: false,
            };
            follow.cleanup = Some(progress);
            native.replace_manifest(next)?;
            Ok(progress)
        })
    }

    pub(crate) fn confirm_follow_slot_removed(&mut self) -> Result<CleanupProgress> {
        self.confirm_follow_cleanup(true)
    }

    pub(crate) fn confirm_follow_publication_removed(&mut self) -> Result<CleanupProgress> {
        self.confirm_follow_cleanup(false)
    }

    fn confirm_follow_cleanup(&mut self, slot: bool) -> Result<CleanupProgress> {
        control::with_native_lock(self, |native| {
            let mut next = native.checked_manifest()?;
            let progress = next
                .follow_state
                .as_mut()
                .and_then(|follow| follow.cleanup.as_mut())
                .ok_or_else(|| bad_state("state-cleanup: durable cleanup intent is absent"))?;
            let already = if slot {
                progress.slot_removed
            } else {
                progress.publication_removed
            };
            if slot {
                progress.slot_removed = true;
            } else {
                progress.publication_removed = true;
            }
            let progress = *progress;
            if already {
                native.synchronize_manifest()?;
            } else {
                native.replace_manifest(next)?;
            }
            Ok(progress)
        })
    }

    pub(crate) fn follow_control(&self) -> Result<Option<FollowControl>> {
        let manifest = self.checked_manifest()?;
        let Some(follow) = manifest.follow_state else {
            return Ok(None);
        };
        follow
            .binding
            .map(|binding| {
                let control = FollowControl {
                    binding,
                    operator: manifest.operator,
                    origin_name: follow.origin_name,
                };
                control.validate()?;
                Ok(control)
            })
            .transpose()
    }

    pub(crate) fn checked_follow_source(&self) -> Result<Option<ClonePlan>> {
        let manifest = self.checked_manifest()?;
        let follow = manifest
            .follow_state
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        if let Some(plan) = follow.preliminary {
            return Ok(Some(plan));
        }
        Ok(manifest.clone_state.map(|saved| saved.plan))
    }

    pub(crate) fn prepare_follow_source(&mut self, plan: &ClonePlan) -> Result<()> {
        clone_state::validate_plan(plan)?;
        if let Some(existing) = self.checked_follow_source()? {
            let restarting = self.checked_follow_restart()?.is_some_and(|restart| {
                restart.slot_absent
                    && restart.publication_absent
                    && !restart.new_bound
                    && restart.next_generation == plan.generation
                    && same_plan(&existing, plan)
            });
            if existing != *plan && !restarting {
                return Err(bad_state(
                    "state-follow: preliminary source plan cannot be replaced",
                ));
            }
            return self.synchronize_manifest();
        }
        let mut next = self.checked_manifest()?;
        let follow = next
            .follow_state
            .as_mut()
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        if plan.generation != CopyGeneration(1)
            || follow.publication.is_some()
            || next.source_slot.is_some()
            || next.clone_state.is_some()
        {
            return Err(bad_state(
                "state-follow: source plan must precede every resource intent",
            ));
        }
        follow.preliminary = Some(plan.clone());
        let bytes = serde_json::to_vec(&next)
            .map_err(|_| bad_state("state-follow: cannot encode source plan"))?
            .len();
        if bytes
            .checked_add(MAX_FOLLOW_CONTROL_BYTES + 8192)
            .is_none_or(|bytes| bytes > MAX_MANIFEST_BYTES as usize)
        {
            return Err(bad_state(
                "state-follow: preliminary source plan leaves no room for exact follow receipts",
            ));
        }
        self.replace_manifest(next)
    }

    pub(crate) fn checked_source_slot(&self) -> Result<Option<SourceSlotProvenance>> {
        Ok(self.checked_manifest()?.source_slot)
    }

    pub(crate) fn checked_follow_cleanup(&self) -> Result<Option<CleanupProgress>> {
        Ok(self
            .checked_manifest()?
            .follow_state
            .and_then(|f| f.cleanup))
    }

    pub(crate) fn checked_follow_restart(&self) -> Result<Option<FollowRestart>> {
        Ok(self
            .checked_manifest()?
            .follow_state
            .and_then(|f| f.restart))
    }

    pub(crate) fn begin_follow_restart(
        &mut self,
        expected: &FollowBinding,
        plan: &ClonePlan,
    ) -> Result<FollowRestart> {
        control::with_native_lock(self, |native| {
            native.begin_follow_restart_locked(expected, plan)
        })
    }

    fn begin_follow_restart_locked(
        &mut self,
        expected: &FollowBinding,
        plan: &ClonePlan,
    ) -> Result<FollowRestart> {
        let mut next = self.checked_manifest()?;
        let saved = next
            .clone_state
            .as_ref()
            .ok_or_else(|| bad_state("state-follow: restart lacks owned copy state"))?;
        let follow = next
            .follow_state
            .as_mut()
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        if let Some(restart) = &follow.restart {
            if restart.previous != *expected || !same_plan(&saved.plan, plan) {
                return Err(bad_state(
                    "state-follow: restart retry differs from retained source plan",
                ));
            }
            self.synchronize_manifest()?;
            return Ok(restart.clone());
        }
        if saved.phase != ClonePhase::Copying
            || !same_plan(&saved.plan, plan)
            || follow.binding.as_ref() != Some(expected)
        {
            return Err(bad_state(
                "state-follow: restart requires the exact pre-postdata owned plan",
            ));
        }
        let next_generation = CopyGeneration(
            saved
                .plan
                .generation
                .0
                .checked_add(1)
                .ok_or_else(|| bad_state("state-follow: copy generation exhausted"))?,
        );
        let restart = FollowRestart {
            previous: expected.clone(),
            next_generation,
            slot_absent: false,
            publication_absent: false,
            new_bound: false,
        };
        follow.restart = Some(restart.clone());
        self.replace_manifest(next)?;
        Ok(restart)
    }

    pub(crate) fn confirm_follow_slot_absent(&mut self, expected: &FollowRestart) -> Result<()> {
        self.confirm_follow_resource_absent(expected, true)
    }

    pub(crate) fn confirm_follow_publication_absent(
        &mut self,
        expected: &FollowRestart,
    ) -> Result<()> {
        self.confirm_follow_resource_absent(expected, false)
    }

    fn confirm_follow_resource_absent(
        &mut self,
        expected: &FollowRestart,
        slot: bool,
    ) -> Result<()> {
        let mut next = self.checked_manifest()?;
        let follow = next
            .follow_state
            .as_mut()
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        let restart = follow
            .restart
            .as_mut()
            .ok_or_else(|| bad_state("state-follow: resource drop lacks durable restart intent"))?;
        if restart.previous != expected.previous
            || restart.next_generation != expected.next_generation
            || restart.new_bound
        {
            return Err(bad_state(
                "state-follow: resource absence does not match retained restart identity",
            ));
        }
        if slot {
            if restart.slot_absent {
                return self.synchronize_manifest();
            }
            if next.source_slot
                != Some(SourceSlotProvenance::Confirmed(
                    restart.previous.slot.clone(),
                ))
            {
                return Err(bad_state(
                    "state-follow: old slot receipt changed before positive absence",
                ));
            }
            restart.slot_absent = true;
            next.source_slot = None;
        } else {
            if restart.publication_absent {
                return self.synchronize_manifest();
            }
            if follow
                .publication
                .as_ref()
                .and_then(PublicationProvenance::confirmed)
                != Some(&restart.previous.publication)
            {
                return Err(bad_state(
                    "state-follow: old publication receipt changed before positive absence",
                ));
            }
            restart.publication_absent = true;
            follow.publication = None;
        }
        self.replace_manifest(next)
    }

    pub(crate) fn confirm_pending_follow_slot_absent(
        &mut self,
        proof: &crate::snapshot::PendingFollowSlotAbsence,
    ) -> Result<()> {
        let mut next = self.checked_manifest()?;
        require_pending_resource_retirement(&next, proof.restart())?;
        let receipt = proof.receipt();
        if receipt.intent.binding.stream() != proof.restart().previous.stream()
            || receipt.intent.binding.catalog_epoch() != proof.restart().next_generation.0
            || receipt.intent.binding.plugin() != Plugin::Pgoutput
            || next.source_slot != Some(SourceSlotProvenance::Confirmed(receipt.clone()))
        {
            return Err(bad_state(
                "state-follow: pending slot absence has a different exact receipt",
            ));
        }
        next.source_slot = None;
        self.replace_manifest(next)
    }

    pub(crate) fn confirm_pending_follow_publication_absent(
        &mut self,
        proof: &crate::snapshot::PendingFollowPublicationAbsence,
    ) -> Result<()> {
        let mut next = self.checked_manifest()?;
        require_pending_resource_retirement(&next, proof.restart())?;
        let follow = next.follow_state.as_mut().expect("checked follow state");
        if next.source_slot.is_some()
            || follow
                .publication
                .as_ref()
                .and_then(PublicationProvenance::confirmed)
                != Some(proof.receipt())
        {
            return Err(bad_state(
                "state-follow: pending publication absence has a different exact receipt or surviving slot",
            ));
        }
        follow.publication = None;
        self.replace_manifest(next)
    }

    pub(crate) fn finish_follow_restart(&mut self) -> Result<()> {
        let mut next = self.checked_manifest()?;
        let follow = next
            .follow_state
            .as_mut()
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        if follow.restart.as_ref().is_none_or(|r| !r.new_bound) {
            return Err(bad_state(
                "state-follow: restart finish lacks its exact new binding",
            ));
        }
        follow.restart = None;
        self.replace_manifest(next)
    }

    pub(crate) fn checked_follow(&self) -> Result<Option<FollowBinding>> {
        let manifest = self.checked_manifest()?;
        let follow = manifest
            .follow_state
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        Ok(follow.binding)
    }

    pub(crate) fn checked_publication(&self) -> Result<Option<PublicationProvenance>> {
        let manifest = self.checked_manifest()?;
        Ok(manifest
            .follow_state
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?
            .publication)
    }

    pub(crate) fn begin_publication(&mut self, name: &str) -> Result<PublicationIntent> {
        let mut next = self.checked_manifest()?;
        let follow = next
            .follow_state
            .as_mut()
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        let restart_create = follow
            .restart
            .as_ref()
            .is_some_and(|r| r.slot_absent && r.publication_absent && !r.new_bound);
        if (!self.fresh_created && !restart_create)
            || follow.preliminary.is_none() && follow.binding.is_none()
            || follow.publication.is_some()
            || !valid_name(name)
        {
            return Err(bad_state(
                "state-follow: publication CREATE requires a fresh intent",
            ));
        }
        let intent = PublicationIntent {
            run: self.identity.run,
            name: name.to_owned(),
            nonce: random_identity()?,
        };
        follow.publication = Some(PublicationProvenance::Provisioning {
            intent: intent.clone(),
        });
        self.replace_manifest(next)?;
        Ok(intent)
    }

    pub(crate) fn record_publication_created(
        &mut self,
        intent: &PublicationIntent,
        receipt: &PublicationReceipt,
    ) -> Result<()> {
        self.publish_publication(intent, receipt, false)
    }

    pub(crate) fn confirm_publication(
        &mut self,
        intent: &PublicationIntent,
        receipt: &PublicationReceipt,
    ) -> Result<()> {
        self.publish_publication(intent, receipt, true)
    }

    fn publish_publication(
        &mut self,
        intent: &PublicationIntent,
        receipt: &PublicationReceipt,
        confirmed: bool,
    ) -> Result<()> {
        let mut next = self.checked_manifest()?;
        let follow = next
            .follow_state
            .as_mut()
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        let replacement = if confirmed {
            PublicationProvenance::Confirmed {
                intent: intent.clone(),
                receipt: receipt.clone(),
            }
        } else {
            PublicationProvenance::Created {
                intent: intent.clone(),
                receipt: receipt.clone(),
            }
        };
        replacement.validate(self.identity.run)?;
        match follow.publication.as_ref() {
            Some(existing) if existing == &replacement => return self.synchronize_manifest(),
            Some(PublicationProvenance::Provisioning { intent: pending })
                if pending == intent && !confirmed => {}
            Some(PublicationProvenance::Created {
                intent: pending,
                receipt: candidate,
            }) if pending == intent && candidate == receipt && confirmed => {}
            _ => {
                return Err(bad_state(
                    "state-follow: publication confirmation lacks its exact preceding receipt",
                ));
            }
        }
        follow.publication = Some(replacement);
        self.replace_manifest(next)
    }

    pub(crate) fn bind_follow(
        &mut self,
        plan: &ClonePlan,
        ticket: &SnapshotTicket,
        binding: &FollowBinding,
    ) -> Result<()> {
        validate_follow_binding(binding)?;
        clone_state::validate_plan(plan)?;
        let preliminary = self.checked_follow_source()?;
        if preliminary
            .as_ref()
            .is_none_or(|saved| !same_plan(saved, plan))
        {
            return Err(bad_state(
                "state-follow: snapshot plan differs from preliminary source authority",
            ));
        }
        let mut next = self.checked_manifest()?;
        let follow = next
            .follow_state
            .as_mut()
            .ok_or_else(|| bad_state("state-follow: native state is not a follow run"))?;
        if follow.binding.as_ref() == Some(binding) {
            if next
                .clone_state
                .as_ref()
                .is_none_or(|s| s.plan != *plan || s.ticket != *ticket)
            {
                return Err(bad_state(
                    "state-follow: snapshot plan differs from native binding",
                ));
            }
            return self.synchronize_manifest();
        }
        let restarting = follow.restart.as_ref().is_some_and(|r| {
            r.slot_absent
                && r.publication_absent
                && !r.new_bound
                && r.next_generation == plan.generation
                && follow.binding.as_ref() == Some(&r.previous)
        });
        if (!restarting
            && preliminary
                .as_ref()
                .is_none_or(|saved| saved.generation != plan.generation))
            || follow.binding.is_some() && !restarting
            || follow
                .publication
                .as_ref()
                .and_then(PublicationProvenance::confirmed)
                != Some(&binding.publication)
            || next.source_slot != Some(SourceSlotProvenance::Confirmed(binding.slot.clone()))
            || binding.stream().run != self.identity.run
            || binding.stream().source != plan.source.identity
            || binding.source_timeline != plan.source.timeline
            || binding.copy_generation != plan.generation
            || ticket.token != binding.slot.snapshot_token
        {
            return Err(bad_state(
                "state-follow: follow resources differ from positive native receipts",
            ));
        }
        let clone = CloneState {
            plan: plan.clone(),
            ticket: ticket.clone(),
            phase: ClonePhase::Fresh,
            owned_objects: Vec::new(),
            owned_tables: Vec::new(),
            completion: None,
        };
        clone.validate()?;
        if restarting {
            let saved = next
                .clone_state
                .as_mut()
                .ok_or_else(|| bad_state("state-follow: restart lacks owned copy state"))?;
            if saved.phase != ClonePhase::Copying
                || !same_plan(&saved.plan, plan)
                || saved.plan.generation.0.checked_add(1) != Some(plan.generation.0)
            {
                return Err(bad_state(
                    "state-follow: restart source plan or owned generation changed",
                ));
            }
            saved.plan = plan.clone();
            saved.ticket = ticket.clone();
            for table in &mut saved.owned_tables {
                table.generation = plan.generation;
            }
            saved.completion = None;
            saved.validate()?;
            follow.restart.as_mut().unwrap().new_bound = true;
        } else {
            if next.clone_state.is_some() {
                return Err(bad_state(
                    "state-follow: existing snapshot cannot be replaced",
                ));
            }
            next.clone_state = Some(clone);
        }
        follow.binding = Some(binding.clone());
        follow.preliminary = None;
        self.replace_manifest(next)
    }
}

fn require_pending_resource_retirement(
    manifest: &Manifest,
    expected: &FollowRestart,
) -> Result<()> {
    let saved = manifest.clone_state.as_ref().ok_or_else(|| {
        bad_state("state-follow: pending resource retirement lacks owned copy state")
    })?;
    let follow = manifest
        .follow_state
        .as_ref()
        .ok_or_else(|| bad_state("state-follow: pending resource retirement lacks follow state"))?;
    if follow.restart.as_ref() != Some(expected)
        || expected.new_bound
        || !expected.slot_absent
        || !expected.publication_absent
        || follow.binding.as_ref() != Some(&expected.previous)
        || saved.phase != ClonePhase::Copying
        || saved.plan.generation != expected.previous.copy_generation
    {
        return Err(bad_state(
            "state-follow: pending resource retirement differs from its unbound restart intent",
        ));
    }
    Ok(())
}

impl FollowState {
    pub(super) fn validate(&self, manifest: &Manifest) -> Result<()> {
        if self
            .origin_name
            .as_deref()
            .is_some_and(|name| !valid_name(name))
        {
            return Err(bad_state("state-follow: invalid unowned origin name"));
        }
        if self.cleanup.is_some() {
            let binding = self.binding.as_ref().ok_or_else(|| {
                bad_state("state-cleanup: cleanup lacks confirmed follow inventory")
            })?;
            if self.restart.is_some()
                || self
                    .publication
                    .as_ref()
                    .and_then(PublicationProvenance::confirmed)
                    != Some(&binding.publication)
                || manifest.source_slot
                    != Some(SourceSlotProvenance::Confirmed(binding.slot.clone()))
                || !matches!(
                    manifest.target_provenance.as_slice(),
                    [TargetProvenance::Confirmed(_)]
                )
            {
                return Err(bad_state(
                    "state-cleanup: cleanup inventory is incomplete or uncertain",
                ));
            }
        }
        if let Some(plan) = &self.preliminary {
            clone_state::validate_plan(plan)?;
            if self.binding.is_some() || manifest.clone_state.is_some() {
                return Err(bad_state(
                    "state-follow: preliminary plan duplicates bound copy authority",
                ));
            }
            if manifest
                .source_slot
                .as_ref()
                .is_some_and(|slot| slot.intent().binding.stream().source != plan.source.identity)
            {
                return Err(bad_state(
                    "state-follow: source receipt differs from preliminary plan",
                ));
            }
        }
        if let Some(publication) = &self.publication {
            publication.validate(manifest.run)?;
        }
        if let Some(binding) = &self.binding {
            validate_follow_binding(binding)?;
            if binding.stream().run != manifest.run
                || self.restart.is_none()
                    && self
                        .publication
                        .as_ref()
                        .and_then(PublicationProvenance::confirmed)
                        != Some(&binding.publication)
                || self.restart.is_none()
                    && manifest.source_slot
                        != Some(SourceSlotProvenance::Confirmed(binding.slot.clone()))
                || manifest.clone_state.as_ref().is_none_or(|s| {
                    s.plan.source.identity != binding.stream().source
                        || s.plan.source.timeline != binding.source_timeline
                        || s.plan.generation != binding.copy_generation
                })
            {
                return Err(bad_state(
                    "state-follow: inconsistent persisted follow binding",
                ));
            }
        }
        if let Some(restart) = &self.restart {
            validate_follow_binding(&restart.previous)?;
            let saved = manifest
                .clone_state
                .as_ref()
                .ok_or_else(|| bad_state("state-follow: restart lacks owned copy state"))?;
            if restart.previous.stream().run != manifest.run
                || restart.previous.copy_generation.0.checked_add(1)
                    != Some(restart.next_generation.0)
                || saved.phase != ClonePhase::Copying
                || self.binding.as_ref().is_none_or(|binding| {
                    if restart.new_bound {
                        binding.copy_generation != restart.next_generation
                            || binding.stream() != restart.previous.stream()
                            || !restart.slot_absent
                            || !restart.publication_absent
                    } else {
                        binding != &restart.previous
                    }
                })
                || !restart.slot_absent
                    && manifest.source_slot
                        != Some(SourceSlotProvenance::Confirmed(
                            restart.previous.slot.clone(),
                        ))
                || !restart.publication_absent
                    && self
                        .publication
                        .as_ref()
                        .and_then(PublicationProvenance::confirmed)
                        != Some(&restart.previous.publication)
            {
                return Err(bad_state(
                    "state-follow: inconsistent retained restart intent",
                ));
            }
            if restart.new_bound
                && (manifest.source_slot
                    != self
                        .binding
                        .as_ref()
                        .map(|b| SourceSlotProvenance::Confirmed(b.slot.clone()))
                    || self
                        .publication
                        .as_ref()
                        .and_then(PublicationProvenance::confirmed)
                        != self.binding.as_ref().map(|b| &b.publication))
            {
                return Err(bad_state(
                    "state-follow: restarted binding lacks exact new source receipts",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn control() -> IoControl {
        IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )))
    }

    #[test]
    fn native_follow_mode_is_immutable_and_publication_candidate_is_not_adopted() {
        let tmp = super::super::tests::TempDir::new();
        let mut request = StateRequest {
            path: tmp.path(),
            action: StateAction::Create,
        };
        let mut native = open_follow(&request, None, Some("unowned_origin"), &control()).unwrap();
        native.prepare_follow_source(&empty_plan()).unwrap();
        let intent = native.begin_publication("rpgmigrate_owned").unwrap();
        let receipt = PublicationReceipt {
            name: intent.name.clone(),
            oid: 7,
            owner_oid: 8,
        };
        assert!(native.confirm_publication(&intent, &receipt).is_err());
        native
            .record_publication_created(&intent, &receipt)
            .unwrap();
        assert!(
            native
                .checked_publication()
                .unwrap()
                .unwrap()
                .confirmed()
                .is_none()
        );
        assert!(native.begin_publication("rpgmigrate_other").is_err());
        let run = native.identity().run;
        drop(native);
        request.action = StateAction::Resume;
        assert!(open_follow(&request, None, None, &control()).is_err());
        assert!(open_clone(&request, None, &control()).is_err());
        let mut resumed = open_follow(&request, None, Some("unowned_origin"), &control()).unwrap();
        assert_eq!(resumed.identity().run, run);
        assert!(
            resumed
                .checked_publication()
                .unwrap()
                .unwrap()
                .confirmed()
                .is_none()
        );
        resumed.confirm_publication(&intent, &receipt).unwrap();
        assert_eq!(
            resumed.checked_publication().unwrap().unwrap().confirmed(),
            Some(&receipt)
        );
    }

    #[test]
    fn mode_only_init_retry_preserves_run_and_allows_one_source_create() {
        let tmp = super::super::tests::TempDir::new();
        let mut request = StateRequest {
            path: tmp.path(),
            action: StateAction::Restart,
        };
        let native = open_follow(&request, None, None, &control()).unwrap();
        let run = native.identity().run;
        drop(native);
        request.action = StateAction::Resume;
        let mut native = open_follow(&request, None, None, &control()).unwrap();
        assert_eq!(native.identity().run, run);
        let stream = StreamId {
            run,
            source: crate::identity::SourceId {
                system_identifier: 2,
                database_oid: 3,
            },
            generation: 1,
        };
        let binding =
            SourceSlotBinding::new(stream, "rpgmigrate_init".into(), Plugin::Pgoutput, 1).unwrap();
        native.begin_source_slot(&binding).unwrap();
        assert!(native.begin_source_slot(&binding).is_err());
    }

    fn empty_plan() -> ClonePlan {
        ClonePlan {
            source: crate::contracts::CloneSource {
                identity: crate::identity::SourceId {
                    system_identifier: 2,
                    database_oid: 3,
                },
                timeline: 1,
                major: 16,
            },
            target: crate::contracts::CloneTarget {
                identity: crate::identity::SourceId {
                    system_identifier: 4,
                    database_oid: 5,
                },
                major: 18,
            },
            generation: CopyGeneration(1),
            tables: Vec::new(),
            sequences: Vec::new(),
            policy: crate::contracts::RestorePolicy::default(),
            copy_policy: crate::contracts::CloneCopyPolicy::default(),
            copy_tables: Vec::new(),
        }
    }

    #[test]
    fn preliminary_plan_is_durable_immutable_and_authorizes_only_one_slot_attempt() {
        let tmp = super::super::tests::TempDir::new();
        let request = StateRequest {
            path: tmp.path(),
            action: StateAction::Create,
        };
        let mut native = open_follow(&request, None, None, &control()).unwrap();
        assert!(native.begin_publication("rpgmigrate_owned").is_err());
        let plan = empty_plan();
        native.prepare_follow_source(&plan).unwrap();
        let mut changed = plan.clone();
        changed.source.timeline += 1;
        assert!(native.prepare_follow_source(&changed).is_err());
        let intent = native.begin_publication("rpgmigrate_owned").unwrap();
        let publication = PublicationReceipt {
            name: intent.name.clone(),
            oid: 7,
            owner_oid: 8,
        };
        native
            .record_publication_created(&intent, &publication)
            .unwrap();
        native.confirm_publication(&intent, &publication).unwrap();
        let run = native.identity().run;
        drop(native);
        let mut native = open_follow(
            &StateRequest {
                action: StateAction::Resume,
                ..request
            },
            None,
            None,
            &control(),
        )
        .unwrap();
        assert_eq!(native.checked_follow_source().unwrap(), Some(plan.clone()));
        native.prepare_follow_source(&plan).unwrap();
        assert!(native.prepare_follow_source(&changed).is_err());
        let stream = StreamId {
            run,
            source: plan.source.identity,
            generation: 1,
        };
        let binding =
            SourceSlotBinding::new(stream, "rpgmigrate_owned".into(), Plugin::Pgoutput, 1).unwrap();
        native.begin_source_slot(&binding).unwrap();
        assert!(native.begin_source_slot(&binding).is_err());
    }

    fn bound_follow() -> (
        super::super::tests::TempDir,
        NativeState,
        ClonePlan,
        FollowBinding,
    ) {
        let tmp = super::super::tests::TempDir::new();
        let mut native = open_follow(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Create,
            },
            None,
            None,
            &control(),
        )
        .unwrap();
        let plan = ClonePlan {
            source: crate::contracts::CloneSource {
                identity: crate::identity::SourceId {
                    system_identifier: 2,
                    database_oid: 3,
                },
                timeline: 1,
                major: 16,
            },
            target: crate::contracts::CloneTarget {
                identity: crate::identity::SourceId {
                    system_identifier: 4,
                    database_oid: 5,
                },
                major: 18,
            },
            generation: CopyGeneration(1),
            tables: Vec::new(),
            sequences: Vec::new(),
            policy: crate::contracts::RestorePolicy::default(),
            copy_policy: crate::contracts::CloneCopyPolicy::default(),
            copy_tables: Vec::new(),
        };
        let binding = create_resources(&mut native, &plan, 10);
        let ticket = SnapshotTicket {
            token: binding.slot.snapshot_token.clone(),
            source: plan.source.clone(),
            generation: plan.generation,
        };
        native.bind_follow(&plan, &ticket, &binding).unwrap();
        native.begin_pre_data().unwrap();
        native
            .confirm_pre_data(&plan, Vec::new(), Vec::new())
            .unwrap();
        (tmp, native, plan, binding)
    }

    fn create_resources(native: &mut NativeState, plan: &ClonePlan, point: u64) -> FollowBinding {
        native.prepare_follow_source(plan).unwrap();
        let intent = native.begin_publication("rpgmigrate_restart").unwrap();
        let publication = PublicationReceipt {
            name: intent.name.clone(),
            oid: point as u32,
            owner_oid: 7,
        };
        native
            .record_publication_created(&intent, &publication)
            .unwrap();
        native.confirm_publication(&intent, &publication).unwrap();
        let stream = StreamId {
            run: native.identity().run,
            source: plan.source.identity,
            generation: 1,
        };
        let slot_intent = native
            .begin_source_slot(
                &SourceSlotBinding::new(
                    stream,
                    "rpgmigrate_restart".into(),
                    Plugin::Pgoutput,
                    plan.generation.0,
                )
                .unwrap(),
            )
            .unwrap();
        let slot = native
            .confirm_source_slot(
                &slot_intent,
                CreatedSlotData {
                    consistent_point: SourceEndLsn(point),
                    snapshot_name: format!("snapshot-{point}"),
                },
            )
            .unwrap();
        FollowBinding {
            slot,
            publication,
            source_timeline: plan.source.timeline,
            copy_generation: plan.generation,
            source_types: Vec::new(),
        }
    }

    #[test]
    fn first_snapshot_replans_parts_but_repeated_binding_requires_exact_profile() {
        use crate::contracts::{CloneTable, CloneTableCopy, CopyFormat, CopyPart, QualifiedTable};
        let tmp = super::super::tests::TempDir::new();
        let mut native = open_follow(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Create,
            },
            None,
            None,
            &control(),
        )
        .unwrap();
        let mut preliminary = empty_plan();
        preliminary.copy_policy.split_tables_larger_than = 1;
        preliminary.tables.push(CloneTable {
            source_oid: 42,
            name: QualifiedTable {
                schema: "public".into(),
                table: "items".into(),
            },
            writable_columns: vec!["id".into()],
            all_columns: vec!["id".into()],
            identity_modes: vec![0],
            partition_identity_modes: None,
        });
        preliminary.copy_tables.push(CloneTableCopy {
            source_oid: 42,
            format: CopyFormat::Text,
            parts: vec![CopyPart {
                number: 1,
                count: 1,
                start_block: 0,
                end_block: None,
            }],
        });
        let binding = create_resources(&mut native, &preliminary, 10);
        let mut snapshot_plan = preliminary.clone();
        snapshot_plan.copy_tables[0].parts = vec![
            CopyPart {
                number: 1,
                count: 2,
                start_block: 0,
                end_block: Some(10),
            },
            CopyPart {
                number: 2,
                count: 2,
                start_block: 10,
                end_block: None,
            },
        ];
        let ticket = SnapshotTicket {
            token: binding.slot.snapshot_token.clone(),
            source: snapshot_plan.source.clone(),
            generation: snapshot_plan.generation,
        };
        native
            .bind_follow(&snapshot_plan, &ticket, &binding)
            .unwrap();
        assert_eq!(native.checked_clone().unwrap().unwrap().plan, snapshot_plan);
        assert!(native.bind_follow(&preliminary, &ticket, &binding).is_err());
        native
            .bind_follow(&snapshot_plan, &ticket, &binding)
            .unwrap();
    }

    fn cleanup_target_binding(plan: &ClonePlan, binding: &FollowBinding) -> TargetBinding {
        TargetBinding {
            stream: binding.stream(),
            target_system_identifier: Some(plan.target.identity.system_identifier),
            target_database_oid: plan.target.identity.database_oid,
            schema_name: "rpgmigrate_cleanup".into(),
            table_name: "checkpoint".into(),
            owner_oid: 7,
            metadata_version: TARGET_METADATA_VERSION,
        }
    }

    #[test]
    fn cleanup_requires_exact_confirmed_target_receipt_before_terminal_intent() {
        for pending in [false, true] {
            let (tmp, mut native, plan, binding) = bound_follow();
            if pending {
                native
                    .begin_target_provisioning(cleanup_target_binding(&plan, &binding))
                    .unwrap();
            }
            let before = std::fs::read(tmp.path().join("manifest.json")).unwrap();
            let error = native.begin_follow_cleanup().unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadState);
            assert!(native.checked_follow_cleanup().unwrap().is_none());
            assert_eq!(
                std::fs::read(tmp.path().join("manifest.json")).unwrap(),
                before
            );
        }
    }

    #[test]
    fn cleanup_persisted_intent_refuses_empty_target_inventory() {
        let (tmp, mut native, _, _) = bound_follow();
        let before = std::fs::read(tmp.path().join("manifest.json")).unwrap();
        let mut next = native.checked_manifest().unwrap();
        next.follow_state.as_mut().unwrap().cleanup = Some(CleanupProgress {
            slot_removed: false,
            publication_removed: false,
        });
        assert!(native.replace_manifest(next).is_err());
        assert_eq!(
            std::fs::read(tmp.path().join("manifest.json")).unwrap(),
            before
        );
    }

    #[test]
    fn cleanup_progress_is_terminal_monotone_and_reopenable() {
        let (tmp, mut native, plan, binding) = bound_follow();
        let intent = native
            .begin_target_provisioning(cleanup_target_binding(&plan, &binding))
            .unwrap();
        native
            .confirm_target_provisioning(ConfirmedProvenance {
                intent,
                schema_oid: 11,
                table_oid: 12,
                marker_table_oid: 13,
                marker_index_oid: 14,
                large_object_table_oid: 15,
                large_object_index_oid: 16,
            })
            .unwrap();
        let mut wrong_target = native.checked_manifest().unwrap();
        let TargetProvenance::Confirmed(receipt) = &mut wrong_target.target_provenance[0] else {
            unreachable!()
        };
        receipt.intent.binding.target_database_oid += 1;
        assert!(native.replace_manifest(wrong_target).is_err());
        assert!(native.checked_follow_cleanup().unwrap().is_none());
        assert!(native.confirm_follow_slot_removed().is_err());
        assert_eq!(
            native.begin_follow_cleanup().unwrap(),
            CleanupProgress {
                slot_removed: false,
                publication_removed: false
            }
        );
        assert!(native.begin_follow_restart(&binding, &plan).is_err());
        assert_eq!(
            native.confirm_follow_slot_removed().unwrap(),
            CleanupProgress {
                slot_removed: true,
                publication_removed: false
            }
        );
        assert_eq!(
            native.confirm_follow_slot_removed().unwrap(),
            CleanupProgress {
                slot_removed: true,
                publication_removed: false
            }
        );
        assert_eq!(
            native.confirm_follow_publication_removed().unwrap(),
            CleanupProgress {
                slot_removed: true,
                publication_removed: true
            }
        );
        let saved = native.checked_manifest().unwrap();
        let mut regress = saved.clone();
        regress
            .follow_state
            .as_mut()
            .unwrap()
            .cleanup
            .as_mut()
            .unwrap()
            .slot_removed = false;
        assert!(native.replace_manifest(regress).is_err());
        let mut restart = saved;
        restart.follow_state.as_mut().unwrap().cleanup = None;
        assert!(native.replace_manifest(restart).is_err());
        drop(native);
        let mut reopened = open_for_provenance_recovery(&tmp.path()).unwrap();
        assert_eq!(reopened.checked_follow().unwrap(), Some(binding));
        assert_eq!(
            reopened.begin_follow_cleanup().unwrap(),
            CleanupProgress {
                slot_removed: true,
                publication_removed: true
            }
        );
    }

    #[test]
    fn cleanup_refuses_unconfirmed_and_restart_inventory_before_intent() {
        let (tmp, mut native, plan, binding) = bound_follow();
        native.begin_follow_restart(&binding, &plan).unwrap();
        let before = std::fs::read(tmp.path().join("manifest.json")).unwrap();
        assert!(native.begin_follow_cleanup().is_err());
        assert_eq!(
            std::fs::read(tmp.path().join("manifest.json")).unwrap(),
            before
        );
        let tmp = super::super::tests::TempDir::new();
        let mut native = open_follow(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Create,
            },
            None,
            None,
            &control(),
        )
        .unwrap();
        native.prepare_follow_source(&empty_plan()).unwrap();
        native.begin_publication("rpgmigrate_uncertain").unwrap();
        let before = std::fs::read(tmp.path().join("manifest.json")).unwrap();
        assert!(native.begin_follow_cleanup().is_err());
        assert_eq!(
            std::fs::read(tmp.path().join("manifest.json")).unwrap(),
            before
        );
    }

    #[test]
    fn restart_retains_receipts_until_each_positive_absence_and_new_binding() {
        let (tmp, mut native, mut plan, old) = bound_follow();
        let restart = native.begin_follow_restart(&old, &plan).unwrap();
        assert_eq!(restart.next_generation, CopyGeneration(2));
        assert_eq!(
            native.checked_follow_source().unwrap().unwrap().generation,
            CopyGeneration(1)
        );
        assert!(native.begin_publication("replacement").is_err());
        native.confirm_follow_slot_absent(&restart).unwrap();
        assert!(native.begin_publication("replacement").is_err());
        assert_eq!(native.checked_follow().unwrap(), Some(old.clone()));
        drop(native);
        let mut native = open_follow(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Resume,
            },
            None,
            None,
            &control(),
        )
        .unwrap();
        let persisted = native.checked_follow_restart().unwrap().unwrap();
        assert!(persisted.slot_absent);
        assert!(!persisted.publication_absent);
        assert_eq!(persisted.previous, old);
        native.confirm_follow_publication_absent(&restart).unwrap();
        plan.generation = CopyGeneration(2);
        let binding = create_resources(&mut native, &plan, 20);
        let ticket = SnapshotTicket {
            token: binding.slot.snapshot_token.clone(),
            source: plan.source.clone(),
            generation: plan.generation,
        };
        native.bind_follow(&plan, &ticket, &binding).unwrap();
        let retained = native.checked_follow_restart().unwrap().unwrap();
        assert!(retained.new_bound);
        assert_eq!(retained.previous, old);
        assert_eq!(native.checked_follow().unwrap(), Some(binding));
        assert_eq!(
            native.checked_clone().unwrap().unwrap().phase,
            ClonePhase::Copying
        );
        native.finish_follow_restart().unwrap();
        assert!(native.checked_follow_restart().unwrap().is_none());
    }

    #[test]
    fn restart_never_resets_uncertain_post_data_or_unconfirmed_source_create() {
        let (_tmp, mut native, plan, binding) = bound_follow();
        native.mark_stage(ClonePhase::PostDataPending).unwrap();
        assert!(native.begin_follow_restart(&binding, &plan).is_err());
        assert!(native.checked_follow_restart().unwrap().is_none());
    }

    #[test]
    fn source_type_budget_refuses_duplicates_and_oversize_before_resource_intents() {
        let ty = SourceType {
            oid: 42,
            schema: "public".into(),
            name: "custom".into(),
        };
        validate_follow_source_types(std::slice::from_ref(&ty), None).unwrap();
        assert!(validate_follow_source_types(&[ty.clone(), ty], None).is_err());
        let types = (1..5000)
            .map(|oid| SourceType {
                oid,
                schema: "s".repeat(63),
                name: "n".repeat(63),
            })
            .collect::<Vec<_>>();
        assert!(validate_follow_source_types(&types, None).is_err());
    }

    #[test]
    fn publication_requires_positive_commit_confirmation() {
        let intent = PublicationIntent {
            run: RunId([1; 16]),
            name: "rpgmigrate_test".into(),
            nonce: [2; 16],
        };
        let candidate = PublicationProvenance::Created {
            intent: intent.clone(),
            receipt: PublicationReceipt {
                name: intent.name.clone(),
                oid: 42,
                owner_oid: 43,
            },
        };
        assert!(candidate.confirmed().is_none());
        assert_eq!(candidate.intent(), &intent);
    }

    #[test]
    fn follow_binding_refuses_invalid_resource_identity() {
        let stream = StreamId {
            run: RunId([1; 16]),
            source: crate::identity::SourceId {
                system_identifier: 2,
                database_oid: 3,
            },
            generation: 1,
        };
        let mut binding = FollowBinding {
            slot: SourceSlotReceipt {
                intent: SourceSlotIntent {
                    binding: SourceSlotBinding::new(
                        stream,
                        "rpgmigrate_test".into(),
                        Plugin::Pgoutput,
                        1,
                    )
                    .unwrap(),
                    nonce: [2; 16],
                },
                consistent_point: SourceEndLsn(10),
                snapshot_token: "snapshot".into(),
            },
            publication: PublicationReceipt {
                name: "rpgmigrate_test".into(),
                oid: 4,
                owner_oid: 5,
            },
            source_timeline: 1,
            copy_generation: CopyGeneration(1),
            source_types: Vec::new(),
        };
        validate_follow_binding(&binding).unwrap();
        binding.publication.oid = 0;
        assert!(validate_follow_binding(&binding).is_err());
        binding.publication.oid = 4;
        binding.source_timeline = 0;
        assert!(validate_follow_binding(&binding).is_err());
    }
}
