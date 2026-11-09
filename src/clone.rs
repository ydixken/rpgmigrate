use crate::apply::CloneAuthority;
use crate::catalog;
use crate::contracts::{
    CloneOptions, ClonePlan, CopyGeneration, CopyReport, Error, ErrorKind, IoControl, OwnedObject,
    RestorePolicy, Result, StateAction,
};
use crate::identity::StreamId;
use crate::pg::PgConnection;
use crate::snapshot::CloneSnapshot;
use crate::state::{self, ClonePhase, NativeState, TargetProvenance};

#[derive(Debug, Eq, PartialEq)]
enum Entry {
    Fresh,
    Recopy,
    AfterPostData,
    AfterSequences,
    Complete,
}

fn entry(phase: Option<ClonePhase>, action: StateAction, not_consistent: bool) -> Result<Entry> {
    // open_clone admits None only for a new or positively proven pristine INIT state.
    match (phase, action, not_consistent) {
        (None, StateAction::Create | StateAction::Restart, false)
        | (None, StateAction::Resume, _) => Ok(Entry::Fresh),
        (Some(ClonePhase::Complete), StateAction::Resume, _) => Ok(Entry::Complete),
        (Some(ClonePhase::Copying), StateAction::Restart, false)
        | (Some(ClonePhase::Copying), StateAction::Resume, true) => Ok(Entry::Recopy),
        (Some(ClonePhase::PostDataComplete), StateAction::Resume, false) => {
            Ok(Entry::AfterPostData)
        }
        (Some(ClonePhase::SequencesComplete), StateAction::Resume, false) => {
            Ok(Entry::AfterSequences)
        }
        (Some(ClonePhase::PreDataPending), _, _) => Err(refusal(
            "clone-recovery: pre-data ownership handoff is unproven; preserve target objects and use a fresh migration after manual inspection",
        )),
        (Some(ClonePhase::Copying), _, _) => Err(refusal(
            "clone-recovery: COPY snapshot was lost; use --restart or --resume --not-consistent before post-data",
        )),
        (Some(ClonePhase::Complete), _, _) => Err(refusal(
            "clone-recovery: completed clone permits idempotent --resume only; restart after post-data is unsupported",
        )),
        (
            Some(
                ClonePhase::PostDataPending | ClonePhase::SequencesPending | ClonePhase::Analyzing,
            ),
            _,
            _,
        ) => Err(refusal(
            "clone-recovery: post-data or final utility outcome is unproven; preserve target objects and inspect the interrupted stage manually",
        )),
        _ => Err(refusal(
            "clone-recovery: requested recovery boundary is unsupported; preserve native state and target objects",
        )),
    }
}

pub(crate) fn run(mut options: CloneOptions, io: &IoControl) -> Result<String> {
    let mut native = state::open_clone(&options.state, options.operator.as_ref(), io)?;
    let previous = native.checked_clone()?;
    let entry = entry(
        previous.as_ref().map(|state| state.phase),
        options.state.action,
        options.not_consistent,
    )?;
    if let Some(previous) = &previous {
        if options.policy != RestorePolicy::default() && options.policy != previous.plan.policy {
            return Err(refusal(
                "clone-recovery: restore policy differs from the native binding",
            ));
        }
        options.policy = previous.plan.policy;
    }
    let generation = if entry == Entry::Recopy {
        CopyGeneration(
            previous
                .as_ref()
                .expect("recopy has native state")
                .plan
                .generation
                .0
                .checked_add(1)
                .ok_or_else(|| refusal("clone-recovery: copy generation overflow"))?,
        )
    } else {
        previous
            .as_ref()
            .map_or(CopyGeneration(1), |state| state.plan.generation)
    };
    let source = PgConnection::connect(&options.source, io)
        .map_err(|error| crate::endpoint_error(error, ErrorKind::Source))?;
    let mut snapshot = CloneSnapshot::export(source, generation, io)?;
    if let Some(previous) = &previous
        && snapshot.ticket().source != previous.plan.source
    {
        return Err(refusal(
            "clone-recovery: source identity, timeline, or major differs from native state",
        ));
    }
    let mut target = PgConnection::connect(&options.target, io)
        .map_err(|error| crate::endpoint_error(error, ErrorKind::Target))?;
    let plan = if let Some(previous) = &previous {
        if catalog::read_clone_target(&mut target, io)? != previous.plan.target {
            return Err(refusal(
                "clone-recovery: target identity or major differs from native state",
            ));
        }
        previous.plan.clone()
    } else {
        let plan = catalog::plan_clone(
            snapshot.connection()?,
            &mut target,
            generation,
            options.policy,
            &[],
            &[],
            io,
        )?;
        native.bind_clone(&plan, snapshot.ticket())?;
        plan
    };
    crate::schema::preflight(&options, &plan, io)?;
    let coordinator = PgConnection::connect(&options.target, io)
        .map_err(|error| crate::endpoint_error(error, ErrorKind::Target))?;
    let mut authority = CloneAuthority::acquire(coordinator, &mut native, &plan, io)?;
    if entry == Entry::Complete {
        if !authority.durable_completed(io)? {
            return Err(refusal(
                "clone-recovery: native completion has no durable target receipt",
            ));
        }
        let report = previous
            .as_ref()
            .and_then(|state| state.completion.clone())
            .ok_or_else(|| refusal("clone-recovery: completed native receipt is missing"))?;
        if authority.copy_report(io)? != report {
            return Err(refusal(
                "clone-recovery: local completion counts differ from durable table markers",
            ));
        }
        snapshot.finish(io)?;
        return completion(&options, &native, &plan, &report, io);
    }
    let mut plan = plan;
    let report;
    if matches!(entry, Entry::Fresh | Entry::Recopy) {
        if entry == Entry::Recopy {
            let previous = previous.as_ref().expect("recopy has native state");
            let metadata = metadata(&native, &plan, &mut target, io)?;
            let candidate = catalog::plan_clone(
                snapshot.connection()?,
                &mut target,
                generation,
                options.policy,
                &metadata,
                &previous.owned_objects,
                io,
            )?;
            require_same_plan(&plan, &candidate)?;
            require_pre_data_match(crate::schema::compare_pre_data(
                &options,
                snapshot.ticket(),
                &metadata,
                io,
            ))?;
            let rebound = catalog::reconcile_clone_tables(
                &mut target,
                &candidate,
                &previous.owned_objects,
                io,
            )?;
            if rebound.len() != previous.owned_tables.len()
                || rebound
                    .iter()
                    .zip(&previous.owned_tables)
                    .any(|(new, old)| {
                        new.table != old.table
                            || new.target_oid != old.target_oid
                            || new.target_namespace_oid != old.target_namespace_oid
                            || new.target_owner_oid != old.target_owner_oid
                    })
            {
                return Err(refusal(
                    "clone-recovery: recorded table ownership changed before restart",
                ));
            }
            native.advance_copy_generation(snapshot.ticket())?;
            authority.advance_generation(&native, io)?;
            plan = candidate;
        }
        let archive = crate::schema::dump(&options, snapshot.ticket(), &native, io)?;
        if entry == Entry::Fresh {
            let owned = crate::schema::restore_pre(&options, &plan, &archive, &mut native, io)?;
            authority.mark_stage(ClonePhase::PreDataPending, io)?;
            let tables = catalog::reconcile_clone_tables(&mut target, &plan, &owned, io)?;
            native.confirm_pre_data(owned, tables)?;
            authority.bind_tables(&native, io)?;
        }
        report = crate::copy::run_tables(
            &options,
            &plan,
            &mut snapshot,
            &mut native,
            &mut authority,
            io,
        )?;
        pending(&mut native, &mut authority, ClonePhase::PostDataPending, io)?;
        crate::schema::restore_post(&options, &archive, io)?;
        positive(
            &mut native,
            &mut authority,
            ClonePhase::PostDataComplete,
            io,
        )?;
    } else {
        report = authority.copy_report(io)?;
    }
    if entry != Entry::AfterSequences {
        pending(
            &mut native,
            &mut authority,
            ClonePhase::SequencesPending,
            io,
        )?;
        let owned = native
            .checked_clone()?
            .ok_or_else(|| refusal("clone-state: native clone binding disappeared"))?;
        catalog::sync_clone_sequences(
            snapshot.connection()?,
            &mut target,
            &plan,
            &owned.owned_objects,
            io,
        )?;
        positive(
            &mut native,
            &mut authority,
            ClonePhase::SequencesComplete,
            io,
        )?;
    }
    pending(&mut native, &mut authority, ClonePhase::Analyzing, io)?;
    let tables = native.owned_copy_tables()?;
    catalog::analyze_clone(&mut target, &plan, &tables, io)?;
    snapshot.finish(io)?;
    authority.record_complete(&report, io)?;
    native.record_clone_complete(&report)?;
    completion(&options, &native, &plan, &report, io)
}

pub(crate) fn require_pre_data_match(result: Result<bool>) -> Result<()> {
    match result {
        Ok(true) => Ok(()),
        Ok(false) => Err(refusal(
            "clone-recovery: pre-data schema drift prevents restart; preserve target objects and native state",
        )),
        Err(_) => Err(refusal(
            "clone-recovery: pre-data schema verification did not positively complete; preserve target objects and native state",
        )),
    }
}

pub(crate) fn require_same_plan(previous: &ClonePlan, candidate: &ClonePlan) -> Result<()> {
    if previous.source != candidate.source
        || previous.target != candidate.target
        || previous.tables != candidate.tables
        || previous.sequences != candidate.sequences
        || previous.policy != candidate.policy
    {
        return Err(refusal(
            "clone-recovery: source plan or endpoint binding changed; fresh-snapshot restart cannot reuse target ownership",
        ));
    }
    Ok(())
}

pub(crate) fn metadata(
    native: &NativeState,
    plan: &ClonePlan,
    target: &mut PgConnection,
    io: &IoControl,
) -> Result<Vec<OwnedObject>> {
    let stream = StreamId {
        run: native.identity().run,
        source: plan.source.identity,
        generation: 1,
    };
    let Some(TargetProvenance::Confirmed(provenance)) = native.target_provenance(&stream) else {
        return Err(refusal(
            "clone-recovery: exact metadata provenance is absent",
        ));
    };
    crate::apply::metadata_inventory(target, provenance, io)
}

pub(crate) fn pending(
    native: &mut NativeState,
    authority: &mut CloneAuthority,
    phase: ClonePhase,
    io: &IoControl,
) -> Result<()> {
    native.mark_stage(phase)?;
    authority.mark_stage(phase, io)
}

pub(crate) fn positive(
    native: &mut NativeState,
    authority: &mut CloneAuthority,
    phase: ClonePhase,
    io: &IoControl,
) -> Result<()> {
    authority.mark_stage(phase, io)?;
    native.mark_stage(phase)
}

fn completion(
    options: &CloneOptions,
    native: &NativeState,
    plan: &ClonePlan,
    report: &CopyReport,
    io: &IoControl,
) -> Result<String> {
    let progress = crate::progress::read(options, io)?;
    if progress.phase != "completed" || !progress.copy.is_complete() {
        return Err(refusal(
            "clone-complete: measured table, index, and byte completion is unproven",
        ));
    }
    if options.operator.is_some() {
        return crate::protocol::copy_report(
            crate::protocol::CopyCommand::Clone,
            progress.work.as_ref(),
            progress.phase,
            &progress.copy,
        );
    }
    let value = serde_json::json!({
        "command": "clone",
        "phase": "completed",
        "status": "complete",
        "complete": true,
        "run": native.identity().run.0,
        "state_format_version": native.identity().format_version,
        "copy_generation": plan.generation.0,
        "tables": report.tables,
        "rows": report.rows,
        "bytes": report.bytes,
        "copy": progress.copy,
    });
    Ok(format!("{value}\n"))
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_init_state_starts_without_recovery_artifacts() {
        for (action, not_consistent) in [
            (StateAction::Create, false),
            (StateAction::Restart, false),
            (StateAction::Resume, false),
            (StateAction::Resume, true),
        ] {
            assert_eq!(entry(None, action, not_consistent).unwrap(), Entry::Fresh);
        }
        for action in [StateAction::Create, StateAction::Restart] {
            assert!(entry(None, action, true).is_err());
        }
    }

    #[test]
    fn lost_snapshot_requires_explicit_fresh_generation() {
        assert!(entry(Some(ClonePhase::Copying), StateAction::Resume, false).is_err());
        assert_eq!(
            entry(Some(ClonePhase::Copying), StateAction::Resume, true).unwrap(),
            Entry::Recopy
        );
        assert_eq!(
            entry(Some(ClonePhase::Copying), StateAction::Restart, false).unwrap(),
            Entry::Recopy
        );
    }

    #[test]
    fn restart_schema_guard_requires_positive_equality_before_publication() {
        let mut published = false;
        for result in [Ok(false), Err(refusal("utility outcome unknown"))] {
            let outcome = require_pre_data_match(result).map(|()| published = true);
            assert!(outcome.is_err());
            assert!(!published);
        }
        require_pre_data_match(Ok(true)).unwrap();
    }

    #[test]
    fn completed_resume_never_authorizes_restart() {
        assert_eq!(
            entry(Some(ClonePhase::Complete), StateAction::Resume, false).unwrap(),
            Entry::Complete
        );
        assert!(entry(Some(ClonePhase::Complete), StateAction::Restart, false).is_err());
        assert_eq!(
            entry(Some(ClonePhase::Complete), StateAction::Resume, true).unwrap(),
            Entry::Complete
        );
    }

    #[test]
    fn uncertain_utility_stages_refuse_recovery() {
        for phase in [
            ClonePhase::Fresh,
            ClonePhase::PreDataPending,
            ClonePhase::PostDataPending,
            ClonePhase::SequencesPending,
            ClonePhase::Analyzing,
        ] {
            for action in [StateAction::Resume, StateAction::Restart] {
                assert!(entry(Some(phase), action, true).is_err(), "{phase:?}");
                assert!(entry(Some(phase), action, false).is_err(), "{phase:?}");
            }
        }
    }
}
