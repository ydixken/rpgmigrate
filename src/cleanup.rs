use crate::catalog;
use crate::contracts::{
    Error, ErrorKind, IoControl, Result, StateAction, StreamCommand, StreamOptions,
};
use crate::pg::PgConnection;
use crate::state::{CleanupProgress, SourceSlotProvenance, TargetProvenance};

pub(crate) fn run(options: &StreamOptions, io: &IoControl) -> Result<String> {
    if options.state.action != StateAction::Resume || options.command != StreamCommand::Cleanup {
        return Err(refusal("cleanup requires existing native follow state"));
    }
    let source = options
        .source
        .as_ref()
        .ok_or_else(|| Error::new(ErrorKind::BadArgs, "stream cleanup requires --source"))?;
    let target = options
        .target
        .as_ref()
        .ok_or_else(|| Error::new(ErrorKind::BadArgs, "stream cleanup requires --target"))?;
    let mut native = crate::state::open_for_provenance_recovery(&options.state.path)?;
    let control = native
        .follow_control()?
        .ok_or_else(|| refusal("cleanup requires a complete confirmed native follow inventory"))?;
    if control.operator != options.operator || native.checked_follow_restart()?.is_some() {
        return Err(refusal(
            "cleanup operator or current generation differs from native state",
        ));
    }
    let binding = &control.binding;
    let plan = native
        .checked_clone()?
        .ok_or_else(|| refusal("cleanup source and target plan is absent"))?
        .plan;
    if native.checked_source_slot()? != Some(SourceSlotProvenance::Confirmed(binding.slot.clone()))
        || native
            .checked_publication()?
            .and_then(|publication| publication.confirmed().cloned())
            != Some(binding.publication.clone())
    {
        return Err(refusal("cleanup source resource creation is unconfirmed"));
    }
    let Some(TargetProvenance::Confirmed(target_receipt)) =
        native.target_provenance(&binding.stream())
    else {
        return Err(refusal("cleanup target resource creation is unconfirmed"));
    };
    let target_binding = &target_receipt.intent.binding;
    if target_binding.target_system_identifier != Some(plan.target.identity.system_identifier)
        || target_binding.target_database_oid != plan.target.identity.database_oid
    {
        return Err(refusal(
            "cleanup target receipt differs from its recorded plan",
        ));
    }
    let mut source = PgConnection::connect(source, io)
        .map_err(|error| crate::endpoint_error(error, ErrorKind::Source))?;
    let mut target = PgConnection::connect(target, io)
        .map_err(|error| crate::endpoint_error(error, ErrorKind::Target))?;
    catalog::configure_session(&mut source, io)?;
    catalog::configure_session(&mut target, io)?;
    if catalog::read_clone_target(&mut target, io)? != plan.target {
        return Err(refusal(
            "cleanup target identity differs from the confirmed binding",
        ));
    }
    let progress = native.checked_follow_cleanup()?.unwrap_or(CleanupProgress {
        slot_removed: false,
        publication_removed: false,
    });
    catalog::follow::inspect_follow_cleanup(&mut source, &plan, binding, &progress, io)?;
    let progress = native.begin_follow_cleanup()?;
    catalog::follow::remove_follow_slot(&mut source, &plan, binding, progress.slot_removed, io)?;
    let progress = native.confirm_follow_slot_removed()?;
    catalog::follow::remove_follow_publication(
        &mut source,
        &plan,
        binding,
        progress.publication_removed,
        io,
    )?;
    let progress = native.confirm_follow_publication_removed()?;
    catalog::follow::inspect_follow_cleanup(&mut source, &plan, binding, &progress, io)?;
    crate::protocol::cleanup_report(&control, &native.identity().canonical_path, &progress)
}

#[track_caller]
fn refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{Conninfo, StateAction, StateRequest, StreamCommand};
    use std::sync::{Arc, atomic::AtomicBool};
    use std::time::{Duration, Instant};

    struct Temp(std::path::PathBuf);

    impl Temp {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            Self(std::env::temp_dir().join(format!(
                "rpgmigrate-cleanup-owned-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            )))
        }
    }

    impl Drop for Temp {
        fn drop(&mut self) {
            if self.0.exists() {
                std::fs::remove_dir_all(&self.0).unwrap();
            }
        }
    }

    fn io() -> IoControl {
        IoControl {
            deadline: Instant::now() + Duration::from_secs(5),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn options() -> StreamOptions {
        StreamOptions {
            state: StateRequest {
                path: std::env::temp_dir()
                    .join(format!("rpgmigrate-cleanup-missing-{}", std::process::id())),
                action: StateAction::Resume,
            },
            operator: None,
            source: Some(Conninfo::new("host=source.invalid".into()).unwrap()),
            target: Some(Conninfo::new("host=target.invalid".into()).unwrap()),
            command: StreamCommand::Cleanup,
            json: true,
        }
    }

    #[test]
    fn cleanup_requires_both_bound_endpoints_before_opening_state() {
        for source in [true, false] {
            let mut options = options();
            if source {
                options.source = None;
            } else {
                options.target = None;
            }
            let error = run(&options, &io()).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadArgs);
            assert!(!options.state.path.exists());
        }
    }

    #[test]
    fn cleanup_refuses_an_active_native_owner_without_manifest_changes() {
        let temp = Temp::new();
        let mut options = options();
        options.state.path = temp.0.clone();
        let owner = crate::state::open(&StateRequest {
            path: temp.0.clone(),
            action: StateAction::Create,
        })
        .unwrap();
        let before = std::fs::read(temp.0.join("manifest.json")).unwrap();
        let error = run(&options, &io()).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadState);
        assert!(error.message.starts_with("state-busy:"));
        assert_eq!(std::fs::read(temp.0.join("manifest.json")).unwrap(), before);
        drop(owner);
    }

    #[test]
    fn cleanup_refuses_unconfirmed_inventory_without_terminal_intent_or_endpoint_contact() {
        let temp = Temp::new();
        let mut options = options();
        options.state.path = temp.0.clone();
        let owner = crate::state::open_follow(
            &StateRequest {
                path: temp.0.clone(),
                action: StateAction::Create,
            },
            None,
            None,
            &io(),
        )
        .unwrap();
        drop(owner);
        let before = std::fs::read(temp.0.join("manifest.json")).unwrap();
        let error = run(&options, &io()).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadState);
        assert!(
            error
                .message
                .contains("complete confirmed native follow inventory")
        );
        assert_eq!(std::fs::read(temp.0.join("manifest.json")).unwrap(), before);
        assert_eq!(std::fs::read_dir(&temp.0).unwrap().count(), 1);
    }
}
