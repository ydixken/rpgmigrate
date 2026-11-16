use super::*;
use crate::contracts::{ClonePlan, FollowBinding, FollowOptions, SnapshotTicket};
use crate::identity::StreamId;
use crate::pg::CommitObservation;
use crate::state::{PublicationProvenance, SourceSlotProvenance};

pub(crate) struct FollowSnapshot {
    snapshot: CloneSnapshot,
    exporter: PgConnection,
    binding: FollowBinding,
    exporter_pid: i32,
}

pub(crate) struct PendingFollowSlotAbsence {
    pub(super) restart: crate::state::FollowRestart,
    pub(super) slot: SourceSlotProvenance,
}

impl PendingFollowSlotAbsence {
    pub(crate) fn restart(&self) -> &crate::state::FollowRestart {
        &self.restart
    }

    pub(crate) fn slot(&self) -> &SourceSlotProvenance {
        &self.slot
    }
}

pub(crate) struct PendingFollowPublicationAbsence {
    pub(super) restart: crate::state::FollowRestart,
    pub(super) receipt: crate::contracts::PublicationReceipt,
}

impl PendingFollowPublicationAbsence {
    pub(crate) fn restart(&self) -> &crate::state::FollowRestart {
        &self.restart
    }

    pub(crate) fn receipt(&self) -> &crate::contracts::PublicationReceipt {
        &self.receipt
    }
}

impl FollowSnapshot {
    pub(crate) fn retire_unbound(
        options: &FollowOptions,
        plan: &ClonePlan,
        native: &mut NativeState,
        io: &IoControl,
    ) -> Result<()> {
        let restart = native.checked_follow_restart()?.ok_or_else(|| {
            follow_refusal("follow-restart: pending resource retirement lacks its durable intent")
        })?;
        let previous = native.checked_clone()?.ok_or_else(|| {
            follow_refusal("follow-restart: pending resource retirement lacks its owned plan")
        })?;
        require_restart_plan(&previous.plan, plan, restart.next_generation)?;
        if restart.new_bound || !restart.slot_absent || !restart.publication_absent {
            return Err(follow_refusal(
                "follow-restart: only an unbound replacement after old-resource absence can retire",
            ));
        }
        // An unconfirmed publication CREATE is resolved by `create`, which every restart reaches.
        let publication = native
            .checked_publication()?
            .and_then(|p| p.confirmed().cloned());
        let slot = match native.checked_source_slot()? {
            Some(slot)
                if slot.intent().binding.stream() == restart.previous.stream()
                    && slot.intent().binding.catalog_epoch() == restart.next_generation.0
                    && slot.intent().binding.plugin()
                        == crate::cdc::contracts::Plugin::Pgoutput
                    && publication.is_some() =>
            {
                Some(slot)
            }
            None => None,
            _ => {
                return Err(follow_refusal(
                    "follow-restart: pending source slot lacks its exact new-generation intent",
                ));
            }
        };
        let mut source =
            PgConnection::connect(&options.clone.source, crate::pg::Role::Receive, io)?;
        catalog::configure_session(&mut source, io)?;
        catalog::follow::preflight_follow_restart(&mut source, plan, io)?;
        if let Some(publication) = &publication {
            pending_publication_present(&mut source, publication, plan, io)?;
        }
        if let Some(slot) = slot {
            drop_unbound_slot(&mut source, &slot, plan, io)?;
            native.confirm_pending_follow_slot_absent(&PendingFollowSlotAbsence {
                restart: restart.clone(),
                slot,
            })?;
        }
        if let Some(receipt) = publication {
            if pending_publication_present(&mut source, &receipt, plan, io)? {
                let required = catalog::configure_durability(&mut source, io)?;
                catalog::command(&mut source, "BEGIN", "BEGIN", io)?;
                let name = source.quote_identifier(&receipt.name)?;
                catalog::command(
                    &mut source,
                    &format!("DROP PUBLICATION {name}"),
                    "DROP PUBLICATION",
                    io,
                )?;
                catalog::command(
                    &mut source,
                    "SET CONSTRAINTS ALL IMMEDIATE",
                    "SET CONSTRAINTS",
                    io,
                )?;
                catalog::confirm_durability(&mut source, required, io)?;
                if !matches!(source.commit_observed(io), Ok(CommitObservation::Committed)) {
                    return Err(follow_refusal(
                        "follow-restart: pending publication removal outcome is unknown",
                    ));
                }
            }
            let name = source.quote_literal(&receipt.name)?;
            let absent = source.query(
                &format!("SELECT 1 FROM pg_catalog.pg_publication WHERE pubname={name}"),
                io,
            )?;
            if absent.columns()? != 1 || absent.rows() != 0 {
                return Err(follow_refusal(
                    "follow-restart: pending publication remains after removal",
                ));
            }
            native.confirm_pending_follow_publication_absent(&PendingFollowPublicationAbsence {
                restart,
                receipt,
            })?;
        }
        Ok(())
    }

    pub(crate) fn restart(
        options: &FollowOptions,
        plan: &ClonePlan,
        native: &mut NativeState,
        io: &IoControl,
    ) -> Result<Self> {
        let restart = native.checked_follow_restart()?.ok_or_else(|| {
            follow_refusal("follow-restart: source replacement lacks a durable restart intent")
        })?;
        let previous = native
            .checked_clone()?
            .ok_or_else(|| follow_refusal("follow-restart: prior owned plan is missing"))?;
        require_restart_plan(&previous.plan, plan, restart.next_generation)?;
        if restart.new_bound {
            return Err(follow_refusal(
                "follow-restart: new source snapshot was lost after binding; fresh resource ownership requires manual inspection",
            ));
        }
        let mut source =
            PgConnection::connect(&options.clone.source, crate::pg::Role::Receive, io)?;
        catalog::configure_session(&mut source, io)?;
        catalog::follow::preflight_follow_restart(&mut source, plan, io)?;
        catalog::command(
            &mut source,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            io,
        )?;
        if catalog::inspect_clone_tables(&mut source, &[], io)? != plan.tables {
            return Err(follow_refusal(
                "follow-restart: current source table inventory differs from the recorded plan",
            ));
        }
        let types = catalog::follow::source_types(&mut source, &plan.tables, io)?;
        catalog::command(&mut source, "COMMIT", "COMMIT", io)?;
        crate::state::validate_follow_source_types(&types, options.clone.operator.as_ref())?;
        if types != restart.previous.source_types {
            return Err(follow_refusal(
                "follow-restart: source type identity changed",
            ));
        }
        if restart.slot_absent
            && restart.publication_absent
            && native.checked_publication()?.is_some()
        {
            return Self::create(options, plan, native, io);
        }
        let publication_literal = source.quote_literal(&restart.previous.publication.name)?;
        let publication_before = source.query(
            &format!("SELECT 1 FROM pg_catalog.pg_publication WHERE pubname={publication_literal}"),
            io,
        )?;
        if publication_before.rows() > 1
            || (restart.publication_absent && publication_before.rows() != 0)
        {
            return Err(follow_refusal(
                "follow-restart: publication reappeared after recorded absence",
            ));
        }
        if publication_before.rows() == 1 {
            if catalog::follow::publication_receipt(
                &mut source,
                &restart.previous.publication.name,
                io,
            )? != restart.previous.publication
            {
                return Err(follow_refusal(
                    "follow-restart: surviving publication is not this run's exact owned object",
                ));
            }
            catalog::follow::publication_membership(
                &mut source,
                &restart.previous.publication,
                &plan.tables,
                io,
            )?;
        }
        let slot = source.quote_literal(restart.previous.slot.intent.binding.slot_name())?;
        let present=source.query(&format!("SELECT plugin,datoid,temporary,active_pid,confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name={slot}"),io)?;
        if present.columns()? != 5 || present.rows() > 1 {
            return Err(follow_refusal(
                "follow-restart: source slot evidence is invalid",
            ));
        }
        if restart.slot_absent && present.rows() != 0 {
            return Err(follow_refusal(
                "follow-restart: a slot reappeared after recorded absence",
            ));
        }
        if !restart.slot_absent {
            if present.rows() == 1 {
                let point = format!(
                    "{:X}/{:X}",
                    restart.previous.slot.consistent_point.0 >> 32,
                    restart.previous.slot.consistent_point.0 as u32
                );
                if present.cell(0, 0)? != Some(b"pgoutput")
                    || present.cell(0, 1)?
                        != Some(plan.source.identity.database_oid.to_string().as_bytes())
                    || present.cell(0, 2)? != Some(b"f")
                    || present.cell(0, 3)?.is_some()
                    || present.cell(0, 4)? != Some(point.as_bytes())
                {
                    return Err(follow_refusal(
                        "follow-restart: surviving source slot does not match the inactive owned creation receipt",
                    ));
                }
                let dropped = source.query(
                    &format!("SELECT pg_catalog.pg_drop_replication_slot({slot})"),
                    io,
                )?;
                if dropped.rows() != 1
                    || dropped.columns()? != 1
                    || dropped.command_tag()? != "SELECT 1"
                {
                    return Err(follow_refusal(
                        "follow-restart: source slot removal was not confirmed",
                    ));
                }
                let absent = source.query(
                    &format!(
                        "SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name={slot}"
                    ),
                    io,
                )?;
                if absent.rows() != 0 {
                    return Err(follow_refusal(
                        "follow-restart: owned source slot remains after removal",
                    ));
                }
            }
            native.confirm_follow_slot_absent(&restart)?;
        }
        let name = source.quote_literal(&restart.previous.publication.name)?;
        let present = source.query(
            &format!("SELECT oid,pubowner FROM pg_catalog.pg_publication WHERE pubname={name}"),
            io,
        )?;
        if present.columns()? != 2 || present.rows() > 1 {
            return Err(follow_refusal(
                "follow-restart: publication evidence is invalid",
            ));
        }
        if restart.publication_absent && present.rows() != 0 {
            return Err(follow_refusal(
                "follow-restart: a publication reappeared after recorded absence",
            ));
        }
        if !restart.publication_absent {
            if present.rows() == 1 {
                if catalog::follow::publication_receipt(
                    &mut source,
                    &restart.previous.publication.name,
                    io,
                )? != restart.previous.publication
                {
                    return Err(follow_refusal(
                        "follow-restart: surviving publication is not this run's exact owned object",
                    ));
                }
                catalog::follow::publication_membership(
                    &mut source,
                    &restart.previous.publication,
                    &plan.tables,
                    io,
                )?;
                let required = catalog::configure_durability(&mut source, io)?;
                catalog::command(&mut source, "BEGIN", "BEGIN", io)?;
                let name = source.quote_identifier(&restart.previous.publication.name)?;
                catalog::command(
                    &mut source,
                    &format!("DROP PUBLICATION {name}"),
                    "DROP PUBLICATION",
                    io,
                )?;
                catalog::command(
                    &mut source,
                    "SET CONSTRAINTS ALL IMMEDIATE",
                    "SET CONSTRAINTS",
                    io,
                )?;
                catalog::confirm_durability(&mut source, required, io)?;
                if !matches!(source.commit_observed(io), Ok(CommitObservation::Committed)) {
                    return Err(follow_refusal(
                        "follow-restart: publication removal outcome is unknown; preserve durable restart intent",
                    ));
                }
                let absent = source.query(
                    &format!("SELECT 1 FROM pg_catalog.pg_publication WHERE pubname={publication_literal}"),
                    io,
                )?;
                if absent.rows() != 0 {
                    return Err(follow_refusal(
                        "follow-restart: owned publication remains after removal",
                    ));
                }
            }
            native.confirm_follow_publication_absent(&restart)?;
        }
        Self::create(options, plan, native, io)
    }

    pub(crate) fn create(
        options: &FollowOptions,
        plan: &ClonePlan,
        native: &mut NativeState,
        io: &IoControl,
    ) -> Result<Self> {
        check_control(io)?;
        let mut source =
            PgConnection::connect(&options.clone.source, crate::pg::Role::Receive, io)?;
        require_clone_idle(&source)?;
        catalog::configure_session(&mut source, io)?;
        catalog::follow::preflight_follow(&mut source, plan, io)?;
        catalog::command(
            &mut source,
            "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
            "BEGIN",
            io,
        )?;
        if catalog::inspect_clone_tables(&mut source, &[], io)? != plan.tables {
            return Err(follow_refusal(
                "follow-source: preliminary source table inventory differs from the admitted plan",
            ));
        }
        let preliminary_types = catalog::follow::source_types(&mut source, &plan.tables, io)?;
        catalog::command(&mut source, "COMMIT", "COMMIT", io)?;
        crate::state::validate_follow_source_types(
            &preliminary_types,
            options.clone.operator.as_ref(),
        )?;
        let slot_name = if options.slot_name.is_empty() {
            catalog::follow::resource_name("slot", native.identity().run, plan.generation)?
        } else {
            options.slot_name.clone()
        };
        let publication_name = if options.publication_name.is_empty() {
            catalog::follow::resource_name("publication", native.identity().run, plan.generation)?
        } else {
            options.publication_name.clone()
        };
        let slot_binding = SourceSlotBinding::new(
            StreamId {
                run: native.identity().run,
                source: plan.source.identity,
                generation: 1,
            },
            slot_name,
            crate::cdc::contracts::Plugin::Pgoutput,
            plan.generation.0,
        )?;
        native.prepare_follow_source(plan)?;
        // A restart retires its unbound slot in retire_unbound, so this is a first run's retry.
        if let Some(slot) = native.checked_source_slot()? {
            native.require_pending_source_slot_retirement(&slot)?;
            drop_unbound_slot(&mut source, &slot, plan, io)?;
            native.confirm_pending_source_slot_absent(&slot)?;
        }
        if let Some(pending) = native
            .checked_publication()?
            .filter(|p| p.confirmed().is_none())
        {
            native.require_unconfirmed_publication_retirement(&pending)?;
            drop_unconfirmed_publication(&mut source, &pending, io)?;
            native.retire_unconfirmed_publication(&pending)?;
        }
        let reusable_publication = match native.checked_publication()? {
            None => None,
            Some(provenance) => {
                let receipt=provenance.confirmed().cloned().ok_or_else(||follow_refusal("follow-source: unconfirmed publication creation requires manual inspection"))?;
                if native.checked_follow_source()?.as_ref() != Some(plan)
                    || native.checked_source_slot()?.is_some()
                    || receipt.name != publication_name
                {
                    return Err(follow_refusal(
                        "follow-source: publication retry lacks the exact persisted preliminary source plan",
                    ));
                }
                if catalog::follow::publication_receipt(&mut source, &receipt.name, io)? != receipt
                {
                    return Err(follow_refusal(
                        "follow-source: confirmed publication was replaced before slot creation",
                    ));
                }
                catalog::follow::publication_membership(&mut source, &receipt, &plan.tables, io)?;
                Some(receipt)
            }
        };
        let slot = source.quote_literal(slot_binding.slot_name())?;
        let publication = source.quote_literal(&publication_name)?;
        let collisions=source.query(&format!("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name={slot}),EXISTS(SELECT 1 FROM pg_catalog.pg_publication WHERE pubname={publication})"),io)?;
        if collisions.rows() != 1
            || collisions.columns()? != 2
            || collisions.cell(0, 0)? != Some(b"f")
            || collisions.cell(0, 1)?
                != Some(if reusable_publication.is_some() {
                    b"t"
                } else {
                    b"f"
                })
        {
            return Err(follow_refusal(
                "follow-source: existing slot or publication is never adopted",
            ));
        }
        let mut exporter = PgConnection::connect_replication(&options.clone.source, io)?;
        let exporter_pid = i32::try_from(exporter.backend_pid()?)
            .ok()
            .filter(|pid| *pid > 0)
            .ok_or_else(|| follow_refusal("follow-source: exporter PID is invalid"))?;
        let identity = exporter.identify_system(io)?;
        let database = source.query("SELECT pg_catalog.current_database()", io)?;
        if identity.system_identifier != plan.source.identity.system_identifier
            || identity.timeline != plan.source.timeline
            || database.rows() != 1
            || database.cell(0, 0)? != Some(identity.database_name.as_slice())
            || exporter.server_version()? / 10_000 != plan.source.major
        {
            return Err(follow_refusal(
                "follow-source: replication exporter identity differs from the plan",
            ));
        }
        configure_follow_exporter(&mut exporter, io)?;
        let publication = if let Some(publication) = reusable_publication {
            publication
        } else {
            let intent = native.begin_publication(&publication_name)?;
            let required = catalog::configure_durability(&mut source, io)?;
            catalog::command(&mut source, "BEGIN", "BEGIN", io)?;
            let name = source.quote_identifier(&publication_name)?;
            let tables = plan
                .tables
                .iter()
                .map(|table| {
                    catalog::qualified(&mut source, &table.name.schema, &table.name.table)
                        .map(|name| format!("ONLY {name}"))
                })
                .collect::<Result<Vec<_>>>()?;
            let members = if tables.is_empty() {
                String::new()
            } else {
                format!(" FOR TABLE {}", tables.join(", "))
            };
            catalog::command(
                &mut source,
                &format!(
                    "CREATE PUBLICATION {name}{members} WITH (publish='insert, update, delete, truncate',publish_via_partition_root=false)"
                ),
                "CREATE PUBLICATION",
                io,
            )?;
            catalog::command(
                &mut source,
                "SET CONSTRAINTS ALL IMMEDIATE",
                "SET CONSTRAINTS",
                io,
            )?;
            let publication =
                catalog::follow::publication_receipt(&mut source, &publication_name, io)?;
            catalog::follow::publication_membership(&mut source, &publication, &plan.tables, io)?;
            native.record_publication_created(&intent, &publication)?;
            catalog::confirm_durability(&mut source, required, io)?;
            match source.commit_observed(io) {
                Ok(CommitObservation::Committed) => {
                    native.confirm_publication(&intent, &publication)?
                }
                Ok(CommitObservation::RolledBack) => {
                    return Err(follow_refusal(
                        "follow-source: publication CREATE rolled back; retain the creation intent for inspection",
                    ));
                }
                _ => {
                    return Err(follow_refusal(
                        "follow-source-create-unknown: publication COMMIT was not confirmed; retain exact candidate ownership for manual inspection",
                    ));
                }
            }
            publication
        };
        let slot_intent = native.begin_source_slot(&slot_binding)?;
        // CREATE has not been sent, so a slot that took the name since the first check is not ours.
        let free = source
            .query(
                &format!("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name={slot})"),
                io,
            )
            .and_then(|taken| {
                Ok(taken.rows() == 1 && taken.columns()? == 1 && taken.cell(0, 0)? == Some(b"f"))
            });
        if !matches!(free, Ok(true)) {
            native.retire_uncreated_source_slot(&slot_intent)?;
            return Err(free.err().unwrap_or_else(|| {
                follow_refusal("follow-source: existing slot or publication is never adopted")
            }));
        }
        let outcome =
            exporter.create_logical_slot_export(slot_binding.slot_name(), "pgoutput", io)?;
        if matches!(outcome, SlotCreateOutcome::NameTaken) {
            native.retire_uncreated_source_slot(&slot_intent)?;
        }
        let receipt =
            native.confirm_source_slot(&slot_intent, created_data(&slot_binding, outcome)?)?;
        let ticket = SnapshotTicket {
            token: receipt.snapshot_token.clone(),
            source: plan.source.clone(),
            generation: plan.generation,
        };
        import(&mut source, &ticket, io)?;
        if catalog::inspect_clone_tables(&mut source, &[], io)? != plan.tables {
            return Err(follow_refusal(
                "follow-source: slot snapshot table plan differs from preliminary admission",
            ));
        }
        catalog::follow::publication_membership(&mut source, &publication, &plan.tables, io)?;
        let source_types = catalog::follow::source_types(&mut source, &plan.tables, io)?;
        if source_types != preliminary_types {
            return Err(follow_refusal(
                "follow-source: slot snapshot type map differs from preliminary admission",
            ));
        }
        let binding = FollowBinding {
            slot: receipt,
            publication,
            source_timeline: plan.source.timeline,
            copy_generation: plan.generation,
            source_types,
        };
        let pid = source.backend_pid()?;
        Ok(Self {
            snapshot: CloneSnapshot {
                source,
                ticket,
                pid,
            },
            exporter,
            binding,
            exporter_pid,
        })
    }

    pub(crate) fn snapshot_mut(&mut self) -> &mut CloneSnapshot {
        &mut self.snapshot
    }
    pub(crate) fn receipt(&self) -> &SourceSlotReceipt {
        &self.binding.slot
    }
    pub(crate) fn binding(&self) -> &FollowBinding {
        &self.binding
    }
    pub(crate) fn exporter_pid(&self) -> i32 {
        self.exporter_pid
    }

    pub(crate) fn finish(self, io: &IoControl) -> Result<()> {
        if !self.exporter.is_replication()?
            || self.exporter.transaction_status()? != TransactionStatus::Idle
            || self.exporter.backend_pid()? != self.exporter_pid as u32
        {
            return Err(follow_refusal(
                "follow-source: snapshot exporter is unavailable",
            ));
        }
        self.snapshot.finish(io)
    }
}

pub(super) fn require_restart_plan(
    previous: &ClonePlan,
    next: &ClonePlan,
    generation: crate::contracts::CopyGeneration,
) -> Result<()> {
    if next.generation != generation
        || previous.generation.0.checked_add(1) != Some(generation.0)
        || previous.source != next.source
        || previous.target != next.target
        || previous.tables != next.tables
        || previous.sequences != next.sequences
        || previous.policy != next.policy
        || previous.copy_policy != next.copy_policy
    {
        return Err(follow_refusal(
            "follow-restart: source/target plan, ownership, or copy generation changed",
        ));
    }
    Ok(())
}

/// Drops this run's unbound slot by its recorded name and creation shape, and by its receipt's
/// consistent point when it has one. It is never adopted: its export ended with its creating session.
fn drop_unbound_slot(
    source: &mut PgConnection,
    slot: &SourceSlotProvenance,
    plan: &ClonePlan,
    io: &IoControl,
) -> Result<()> {
    let slot_name = slot.intent().binding.slot_name();
    let name = source.quote_literal(slot_name)?;
    let present = source.query(&format!(
        "SELECT slot_type,plugin,datoid,temporary,active_pid,confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name={name}"
    ), io)?;
    if present.columns()? != 6 || present.rows() > 1 {
        return Err(follow_refusal(
            "follow-source: pending slot inventory is invalid",
        ));
    }
    if present.rows() == 1 {
        if let Some(pid) = present.cell(0, 4)? {
            return Err(follow_refusal(&format!(
                "follow-source: pending source slot {slot_name} is active in backend {}; it is never adopted, so retry once that session has ended",
                String::from_utf8_lossy(pid)
            )));
        }
        let point = match slot {
            SourceSlotProvenance::Confirmed(receipt) => Some(format!(
                "{:X}/{:X}",
                receipt.consistent_point.0 >> 32,
                receipt.consistent_point.0 as u32
            )),
            SourceSlotProvenance::Provisioning(_) => None,
        };
        if present.cell(0, 0)? != Some(b"logical")
            || present.cell(0, 1)? != Some(b"pgoutput")
            || present.cell(0, 2)? != Some(plan.source.identity.database_oid.to_string().as_bytes())
            || present.cell(0, 3)? != Some(b"f")
            || point.is_some() && present.cell(0, 5)? != point.as_deref().map(str::as_bytes)
        {
            return Err(follow_refusal(&format!(
                "follow-source: slot {slot_name} differs from this run's unbound pgoutput creation; it is never adopted or dropped"
            )));
        }
        let dropped = source.query(
            &format!("SELECT pg_catalog.pg_drop_replication_slot({name})"),
            io,
        )?;
        if dropped.rows() != 1 || dropped.columns()? != 1 || dropped.command_tag()? != "SELECT 1" {
            return Err(follow_refusal(
                "follow-source: pending slot removal is unconfirmed",
            ));
        }
    }
    let absent = source.query(
        &format!("SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name={name}"),
        io,
    )?;
    if absent.columns()? != 1 || absent.rows() != 0 {
        return Err(follow_refusal(
            "follow-source: pending slot remains after removal",
        ));
    }
    Ok(())
}

/// Resolves a publication CREATE whose COMMIT this run never confirmed. Its receipt is fsynced before
/// COMMIT is sent, so an intent alone never committed; a receipt is ours only at its recorded OID and owner,
/// and is then dropped, never adopted.
fn drop_unconfirmed_publication(
    source: &mut PgConnection,
    pending: &PublicationProvenance,
    io: &IoControl,
) -> Result<()> {
    let name = &pending.intent().name;
    let literal = source.quote_literal(name)?;
    let inventory =
        format!("SELECT oid,pubowner FROM pg_catalog.pg_publication WHERE pubname={literal}");
    let owned = |present: &crate::pg::PgResult| -> Result<bool> {
        if present.columns()? != 2 || present.rows() > 1 {
            return Err(follow_refusal(
                "follow-source: unconfirmed publication inventory is invalid",
            ));
        }
        if present.rows() == 0 {
            return Ok(false);
        }
        let oid = String::from_utf8_lossy(present.cell(0, 0)?.unwrap_or_default()).into_owned();
        match pending {
            PublicationProvenance::Created { receipt, .. }
                if oid == receipt.oid.to_string()
                    && present.cell(0, 1)? == Some(receipt.owner_oid.to_string().as_bytes()) =>
            {
                Ok(true)
            }
            PublicationProvenance::Created { receipt, .. } => Err(follow_refusal(&format!(
                "follow-source: publication {name} (OID {oid}) is not this run's unconfirmed creation (OID {}); it is never adopted or dropped",
                receipt.oid
            ))),
            _ => Err(follow_refusal(&format!(
                "follow-source: publication {name} (OID {oid}) exists, but this run's CREATE under that name never committed; it is not this run's and is never adopted or dropped"
            ))),
        }
    };
    if !owned(&source.query(&inventory, io)?)? {
        return Ok(());
    }
    let PublicationProvenance::Created { receipt, .. } = pending else {
        unreachable!("only a recorded receipt is owned")
    };
    let required = catalog::configure_durability(source, io)?;
    catalog::command(source, "BEGIN", "BEGIN", io)?;
    if !owned(&source.query(&inventory, io)?)? {
        return catalog::command(source, "ROLLBACK", "ROLLBACK", io);
    }
    let identifier = source.quote_identifier(name)?;
    catalog::command(
        source,
        &format!("DROP PUBLICATION {identifier}"),
        "DROP PUBLICATION",
        io,
    )?;
    // DROP resolves the name again under its own lock; that lock names the publication it removed.
    let dropped = source.query("SELECT objid FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid() AND locktype='object' AND classid='pg_catalog.pg_publication'::pg_catalog.regclass AND mode='AccessExclusiveLock' AND granted", io)?;
    if dropped.columns()? != 1
        || dropped.rows() != 1
        || dropped.cell(0, 0)? != Some(receipt.oid.to_string().as_bytes())
    {
        return Err(follow_refusal(&format!(
            "follow-source: publication {name} changed identity during its removal; it is never adopted or dropped"
        )));
    }
    catalog::command(
        source,
        "SET CONSTRAINTS ALL IMMEDIATE",
        "SET CONSTRAINTS",
        io,
    )?;
    catalog::confirm_durability(source, required, io)?;
    if !matches!(source.commit_observed(io), Ok(CommitObservation::Committed)) {
        return Err(follow_refusal(&format!(
            "follow-source: unconfirmed publication {name} removal outcome is unknown; retry to resolve it by its recorded OID"
        )));
    }
    if owned(&source.query(&inventory, io)?)? {
        return Err(follow_refusal(&format!(
            "follow-source: unconfirmed publication {name} remains after removal"
        )));
    }
    Ok(())
}

fn pending_publication_present(
    source: &mut PgConnection,
    receipt: &crate::contracts::PublicationReceipt,
    plan: &ClonePlan,
    io: &IoControl,
) -> Result<bool> {
    let name = source.quote_literal(&receipt.name)?;
    let present = source.query(
        &format!("SELECT oid,pubowner FROM pg_catalog.pg_publication WHERE pubname={name}"),
        io,
    )?;
    if present.columns()? != 2 || present.rows() > 1 {
        return Err(follow_refusal(
            "follow-restart: pending publication inventory is invalid",
        ));
    }
    if present.rows() == 0 {
        return Ok(false);
    }
    if catalog::follow::publication_receipt(source, &receipt.name, io)? != *receipt {
        return Err(follow_refusal(
            "follow-restart: pending publication was replaced",
        ));
    }
    catalog::follow::publication_membership(source, receipt, &plan.tables, io)?;
    Ok(true)
}

fn configure_follow_exporter(exporter: &mut PgConnection, io: &IoControl) -> Result<()> {
    exporter.disable_timeouts(io)?;
    for (name, value) in [
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, YMD"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("extra_float_digits", "3"),
        ("bytea_output", "hex"),
    ] {
        let result = exporter.logical_query(&format!("SET {name} TO '{value}'"), io)?;
        if result.command_tag()? != "SET" {
            return Err(follow_refusal(
                "follow-source: exporter setting was not confirmed",
            ));
        }
        let observed = exporter.logical_query(&format!("SHOW {name}"), io)?;
        if observed.rows() != 1 || observed.cell(0, 0)? != Some(value.as_bytes()) {
            return Err(follow_refusal("follow-source: exporter setting differs"));
        }
    }
    Ok(())
}

#[track_caller]
fn follow_refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}
