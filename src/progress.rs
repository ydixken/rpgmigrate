use serde::Serialize;

use crate::apply::checkpoint::CloneProgress;
use crate::catalog;
use crate::contracts::{
    CloneOptions, Error, ErrorKind, IoControl, OperatorIdentity, OwnedObject, Result,
};
use crate::state::{ClonePhase, CloneState};

#[derive(Debug, Serialize)]
pub(crate) struct ProgressSnapshot {
    pub(crate) work: Option<OperatorIdentity>,
    pub(crate) phase: &'static str,
    pub(crate) copy: CopyProgress,
}

#[derive(Debug, Serialize)]
pub(crate) struct CopyProgress {
    tables: Counter,
    indexes: Counter,
    bytes: Counter,
}

impl CopyProgress {
    fn measured(
        state: &CloneState,
        progress: &CloneProgress,
        indexes_total: u64,
        indexes_done: Option<u64>,
    ) -> Result<Self> {
        let copied_bytes = progress.markers.iter().try_fold(0u64, |sum, marker| {
            sum.checked_add(marker.bytes)
                .filter(|value| *value <= i64::MAX as u64)
                .ok_or_else(|| refusal("progress byte counter overflow"))
        })?;
        Ok(Self {
            tables: Counter::new(
                Some(state.plan.tables.len() as u64),
                Some(crate::apply::checkpoint::completed_copy_tables(
                    &progress.markers,
                    state,
                )?),
            )?,
            indexes: Counter::new(Some(indexes_total), indexes_done)?,
            bytes: Counter::new(
                if progress.durable_complete || progress.streaming {
                    Some(copied_bytes)
                } else {
                    None
                },
                Some(copied_bytes),
            )?,
        })
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.tables.is_complete() && self.indexes.is_complete() && self.bytes.is_complete()
    }
}

pub(crate) fn read(options: &CloneOptions, io: &IoControl) -> Result<ProgressSnapshot> {
    read_measured(options, None, io)
}

pub(crate) fn completed_follow(
    options: &CloneOptions,
    completed: &crate::apply::checkpoint::CompletedFollow,
    io: &IoControl,
) -> Result<ProgressSnapshot> {
    read_measured(options, Some(completed), io)
}

fn read_measured(
    options: &CloneOptions,
    completed: Option<&crate::apply::checkpoint::CompletedFollow>,
    io: &IoControl,
) -> Result<ProgressSnapshot> {
    let (mut source, mut target, inspection, metadata) = crate::compare::peers(options, io)?;
    if completed.is_some_and(|proof| {
        proof.target() != &inspection.provenance || inspection.follow.is_none()
    }) {
        return Err(refusal(
            "completed progress differs from its native authority binding",
        ));
    }
    let progress = crate::apply::checkpoint::read_clone_progress(&mut target, &inspection, io)?;
    if progress.generation != inspection.clone_state.plan.generation {
        return Err(refusal(
            "progress generation differs from the recorded clone",
        ));
    }
    let source_objects = catalog::clone_object_inventory(&mut source, io)?;
    let target_objects = catalog::clone_object_inventory(&mut target, io)?;
    let indexes_total = index_count(&source_objects, &[])?;
    let indexes_done = if matches!(
        progress.phase,
        ClonePhase::PostDataComplete
            | ClonePhase::SequencesPending
            | ClonePhase::SequencesComplete
            | ClonePhase::Analyzing
            | ClonePhase::Complete
    ) {
        Some(index_count(&target_objects, &metadata)?)
    } else {
        None
    };
    let current_phase = phase(
        progress.phase,
        progress.durable_complete,
        progress.streaming,
    );
    let result = ProgressSnapshot {
        work: options.operator.clone(),
        phase: if completed.is_some() {
            "completed"
        } else {
            current_phase
        },
        copy: CopyProgress::measured(
            &inspection.clone_state,
            &progress,
            indexes_total,
            indexes_done,
        )?,
    };
    if completed.is_some() && !result.copy.is_complete() {
        return Err(refusal(
            "completed follow lacks complete measured COPY counters",
        ));
    }
    catalog::command(&mut source, "COMMIT", "COMMIT", io)?;
    catalog::command(&mut target, "COMMIT", "COMMIT", io)?;
    Ok(result)
}

fn index_count(objects: &[OwnedObject], metadata: &[OwnedObject]) -> Result<u64> {
    catalog::verify_exact_exclusions(objects, metadata)?;
    Ok(objects
        .iter()
        .filter(|object| {
            object.class_oid == 1259
                && matches!(object.kind.as_str(), "i" | "I")
                && !metadata.contains(object)
        })
        .count() as u64)
}

#[derive(Debug, Serialize)]
pub(crate) struct Counter {
    #[serde(skip_serializing_if = "Option::is_none")]
    total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    done: Option<u64>,
}

impl Counter {
    fn is_complete(&self) -> bool {
        matches!((self.total,self.done),(Some(total),Some(done)) if total==done && total<=i64::MAX as u64)
    }

    fn new(total: Option<u64>, done: Option<u64>) -> Result<Self> {
        if total
            .into_iter()
            .chain(done)
            .any(|value| value > i64::MAX as u64)
            || matches!((total,done),(Some(total),Some(done)) if done>total)
        {
            return Err(refusal(
                "progress counter exceeds its measured total or protocol range",
            ));
        }
        Ok(Self { total, done })
    }
}

fn phase(stage: ClonePhase, durable_complete: bool, streaming: bool) -> &'static str {
    if streaming {
        return "streaming";
    }
    match stage {
        ClonePhase::Fresh | ClonePhase::PreDataPending => "planning",
        ClonePhase::Copying => "copying",
        ClonePhase::Complete if durable_complete => "completed",
        _ => "finalizing",
    }
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(oid: u32, name: &str) -> OwnedObject {
        OwnedObject {
            class_oid: 1259,
            object_oid: oid,
            object_sub_id: 0,
            namespace_oid: Some(10),
            namespace_name: Some("_rpgmigrate_exact".into()),
            name: name.into(),
            kind: "i".into(),
            owner_oid: 20,
        }
    }

    fn split_progress() -> (CloneState, CloneProgress) {
        use crate::contracts::{
            CloneCopyPolicy, ClonePlan, CloneSource, CloneTable, CloneTableCopy, CloneTarget,
            CopyFormat, CopyGeneration, CopyPart, OwnedCopyTable, QualifiedTable, RestorePolicy,
            SnapshotTicket,
        };
        let generation = CopyGeneration(1);
        let source = CloneSource {
            identity: crate::identity::SourceId {
                system_identifier: 1,
                database_oid: 2,
            },
            timeline: 1,
            major: 16,
        };
        let table = CloneTable {
            source_oid: 3,
            name: QualifiedTable {
                schema: "public".into(),
                table: "t".into(),
            },
            writable_columns: vec!["id".into()],
            all_columns: vec!["id".into()],
            identity_modes: vec![0],
            partition_identity_modes: None,
        };
        let parts = vec![
            CopyPart {
                number: 1,
                count: 2,
                start_block: 0,
                end_block: Some(8),
            },
            CopyPart {
                number: 2,
                count: 2,
                start_block: 8,
                end_block: None,
            },
        ];
        let markers = parts
            .iter()
            .map(|part| crate::apply::checkpoint::CopyDone {
                source_oid: table.source_oid,
                target_oid: 4,
                generation,
                rows: 5,
                bytes: 10,
                target_namespace_oid: 2200,
                target_owner_oid: 10,
                format: CopyFormat::Binary,
                part: *part,
            })
            .collect();
        let state = CloneState {
            plan: ClonePlan {
                source: source.clone(),
                target: CloneTarget {
                    identity: crate::identity::SourceId {
                        system_identifier: 5,
                        database_oid: 6,
                    },
                    major: 18,
                },
                generation,
                tables: vec![table.clone()],
                sequences: vec![],
                policy: RestorePolicy::default(),
                copy_policy: CloneCopyPolicy {
                    use_copy_binary: true,
                    split_tables_larger_than: 1,
                    ..Default::default()
                },
                copy_tables: vec![CloneTableCopy {
                    source_oid: table.source_oid,
                    format: CopyFormat::Binary,
                    parts,
                }],
            },
            ticket: SnapshotTicket {
                token: "snapshot".into(),
                source,
                generation,
            },
            phase: ClonePhase::Copying,
            owned_objects: vec![],
            owned_tables: vec![OwnedCopyTable {
                table,
                target_oid: 4,
                target_namespace_oid: 2200,
                target_owner_oid: 10,
                generation,
            }],
            completion: None,
        };
        let progress = CloneProgress {
            generation,
            phase: ClonePhase::Copying,
            markers,
            durable_complete: false,
            streaming: false,
        };
        (state, progress)
    }

    #[test]
    fn split_progress_counts_only_complete_physical_tables_and_durable_bytes() {
        let (state, mut progress) = split_progress();
        let last = progress.markers.pop().unwrap();
        let partial = CopyProgress::measured(&state, &progress, 0, Some(0)).unwrap();
        assert_eq!(
            serde_json::to_value(partial).unwrap(),
            serde_json::json!({
                "tables": {"total": 1, "done": 0},
                "indexes": {"total": 0, "done": 0},
                "bytes": {"done": 10},
            })
        );
        progress.markers.push(last);
        let complete = CopyProgress::measured(&state, &progress, 0, Some(0)).unwrap();
        assert_eq!(complete.tables.done, Some(1));
        assert_eq!(complete.bytes.done, Some(20));
        assert!(!complete.is_complete());
        progress.streaming = true;
        assert!(
            CopyProgress::measured(&state, &progress, 0, Some(0))
                .unwrap()
                .is_complete()
        );
    }

    #[test]
    fn progress_rejects_duplicate_or_foreign_part_receipts() {
        let (state, progress) = split_progress();
        let mut duplicate = CloneProgress {
            markers: vec![progress.markers[0].clone(); 2],
            ..progress
        };
        assert!(CopyProgress::measured(&state, &duplicate, 0, None).is_err());
        let valid = duplicate.markers[0].clone();
        for field in 0..5 {
            let mut marker = valid.clone();
            match field {
                0 => marker.generation = crate::contracts::CopyGeneration(2),
                1 => marker.format = crate::contracts::CopyFormat::Text,
                2 => marker.part.start_block = 1,
                3 => marker.target_owner_oid += 1,
                _ => marker.target_oid += 1,
            }
            duplicate.markers = vec![marker];
            assert!(CopyProgress::measured(&state, &duplicate, 0, None).is_err());
        }
    }

    #[test]
    fn progress_checks_sum_overflow_and_keeps_empty_lo_work_incomplete() {
        let (mut state, mut progress) = split_progress();
        progress.markers[0].bytes = i64::MAX as u64;
        assert!(CopyProgress::measured(&state, &progress, 0, Some(0)).is_err());
        state.plan.tables.clear();
        state.plan.copy_tables.clear();
        state.owned_tables.clear();
        progress.markers.clear();
        assert!(!state.plan.copy_policy.skip_large_objects);
        let copy = CopyProgress::measured(&state, &progress, 0, Some(0)).unwrap();
        assert_eq!(copy.tables.done, Some(0));
        assert!(!copy.is_complete());
        assert_eq!(phase(progress.phase, false, false), "copying");
    }

    #[test]
    fn index_measurements_exclude_only_exact_current_object_identities() {
        let owned = object(1, "checkpoint_pk");
        let similar = object(2, "checkpoint_pk_extra");
        assert_eq!(
            index_count(&[owned.clone(), similar], std::slice::from_ref(&owned)).unwrap(),
            1
        );
        let changed = object(3, "checkpoint_pk");
        assert!(index_count(&[changed], &[owned]).is_err());
    }

    #[test]
    fn missing_progress_measurements_stay_unknown() {
        let value = serde_json::to_value(Counter::new(None, None).unwrap()).unwrap();
        assert_eq!(value, serde_json::json!({}));
        assert_eq!(
            serde_json::to_value(Counter::new(Some(3), None).unwrap()).unwrap(),
            serde_json::json!({"total":3})
        );
        assert_eq!(
            serde_json::to_value(Counter::new(Some(0), Some(0)).unwrap()).unwrap(),
            serde_json::json!({"total":0,"done":0})
        );
    }

    #[test]
    fn copy_completion_requires_every_measured_counter_pair() {
        let measured = || Counter::new(Some(3), Some(3)).unwrap();
        let mut copy = CopyProgress {
            tables: measured(),
            indexes: measured(),
            bytes: measured(),
        };
        assert!(copy.is_complete());
        for (total, done) in [
            (None, None),
            (Some(3), None),
            (None, Some(3)),
            (Some(3), Some(2)),
            (Some(2), Some(3)),
            (Some(u64::MAX), Some(u64::MAX)),
        ] {
            copy.indexes = Counter { total, done };
            assert!(!copy.is_complete(), "{total:?}/{done:?}");
        }
        copy.indexes = measured();
        copy.tables = Counter::new(None, None).unwrap();
        assert!(!copy.is_complete());
        copy.tables = measured();
        copy.bytes = Counter::new(Some(3), None).unwrap();
        assert!(!copy.is_complete());
    }

    #[test]
    fn progress_refuses_impossible_or_out_of_contract_counters() {
        assert!(Counter::new(Some(2), Some(3)).is_err());
        assert!(Counter::new(None, Some(u64::MAX)).is_err());
        assert!(Counter::new(Some(u64::MAX), None).is_err());
    }

    #[test]
    fn local_completion_never_proves_durable_target_completion() {
        assert_ne!(phase(ClonePhase::Complete, false, false), "completed");
        assert_eq!(phase(ClonePhase::Complete, true, false), "completed");
    }

    #[test]
    fn anchored_follow_reports_streaming_without_follow_completion() {
        assert_eq!(phase(ClonePhase::Analyzing, false, true), "streaming");
        assert_eq!(phase(ClonePhase::Complete, true, true), "streaming");
        assert_eq!(phase(ClonePhase::Analyzing, false, false), "finalizing");
    }
}
