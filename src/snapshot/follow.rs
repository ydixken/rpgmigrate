use super::*;
use crate::contracts::{ClonePlan, FollowBinding, FollowOptions, SnapshotTicket};
use crate::identity::StreamId;
use crate::pg::CommitObservation;

pub(crate) struct FollowSnapshot {
    snapshot: CloneSnapshot,
    exporter: PgConnection,
    binding: FollowBinding,
    exporter_pid: i32,
}

pub(crate) struct PendingFollowSlotAbsence {
    pub(super) restart: crate::state::FollowRestart,
    pub(super) receipt: SourceSlotReceipt,
}

impl PendingFollowSlotAbsence {
    pub(crate) fn restart(&self) -> &crate::state::FollowRestart {
        &self.restart
    }

    pub(crate) fn receipt(&self) -> &SourceSlotReceipt {
        &self.receipt
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
        let publication = native
            .checked_publication()?
            .map(|p| {
                p.confirmed().cloned().ok_or_else(|| {
                    follow_refusal("follow-restart: pending publication creation is unproven")
                })
            })
            .transpose()?;
        let slot = match native.checked_source_slot()? {
            Some(crate::state::SourceSlotProvenance::Confirmed(slot))
                if slot.intent.binding.stream() == restart.previous.stream()
                    && slot.intent.binding.catalog_epoch() == restart.next_generation.0
                    && slot.intent.binding.plugin() == crate::cdc::contracts::Plugin::Pgoutput
                    && publication.is_some() =>
            {
                Some(slot)
            }
            None => None,
            _ => {
                return Err(follow_refusal(
                    "follow-restart: pending source slot lacks its exact new-generation receipt",
                ));
            }
        };
        let mut source = PgConnection::connect(&options.clone.source, io)?;
        catalog::configure_session(&mut source, io)?;
        catalog::follow::preflight_follow_restart(&mut source, plan, io)?;
        if let Some(publication) = &publication {
            pending_publication_present(&mut source, publication, plan, io)?;
        }
        if let Some(receipt) = slot {
            let name = source.quote_literal(receipt.intent.binding.slot_name())?;
            let present = source.query(&format!(
                "SELECT plugin,datoid,temporary,active_pid,confirmed_flush_lsn FROM pg_catalog.pg_replication_slots WHERE slot_name={name}"
            ), io)?;
            if present.columns()? != 5 || present.rows() > 1 {
                return Err(follow_refusal(
                    "follow-restart: pending slot inventory is invalid",
                ));
            }
            if present.rows() == 1 {
                let point = format!(
                    "{:X}/{:X}",
                    receipt.consistent_point.0 >> 32,
                    receipt.consistent_point.0 as u32
                );
                if present.cell(0, 0)? != Some(b"pgoutput")
                    || present.cell(0, 1)?
                        != Some(plan.source.identity.database_oid.to_string().as_bytes())
                    || present.cell(0, 2)? != Some(b"f")
                    || present.cell(0, 3)?.is_some()
                    || present.cell(0, 4)? != Some(point.as_bytes())
                {
                    return Err(follow_refusal(
                        "follow-restart: pending slot differs from its inactive creation receipt",
                    ));
                }
                let dropped = source.query(
                    &format!("SELECT pg_catalog.pg_drop_replication_slot({name})"),
                    io,
                )?;
                if dropped.rows() != 1
                    || dropped.columns()? != 1
                    || dropped.command_tag()? != "SELECT 1"
                {
                    return Err(follow_refusal(
                        "follow-restart: pending slot removal is unconfirmed",
                    ));
                }
            }
            let absent = source.query(
                &format!("SELECT 1 FROM pg_catalog.pg_replication_slots WHERE slot_name={name}"),
                io,
            )?;
            if absent.columns()? != 1 || absent.rows() != 0 {
                return Err(follow_refusal(
                    "follow-restart: pending slot remains after removal",
                ));
            }
            native.confirm_pending_follow_slot_absent(&PendingFollowSlotAbsence {
                restart: restart.clone(),
                receipt,
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
        let mut source = PgConnection::connect(&options.clone.source, io)?;
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
        let mut source = PgConnection::connect(&options.clone.source, io)?;
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
        configure_follow_exporter(&mut exporter, plan.source.major, io)?;
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
        let outcome =
            exporter.create_logical_slot_export(slot_binding.slot_name(), "pgoutput", io)?;
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

fn configure_follow_exporter(
    exporter: &mut PgConnection,
    major: u32,
    io: &IoControl,
) -> Result<()> {
    for (name, value) in [
        ("statement_timeout", "0"),
        ("idle_in_transaction_session_timeout", "0"),
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
    if major >= 17 {
        let set = exporter.logical_query("SET transaction_timeout TO 0", io)?;
        if set.command_tag()? != "SET" {
            return Err(follow_refusal(
                "follow-source: exporter transaction timeout was not disabled",
            ));
        }
        let value = exporter.logical_query("SHOW transaction_timeout", io)?;
        if value.rows() != 1 || value.cell(0, 0)? != Some(b"0") {
            return Err(follow_refusal(
                "follow-source: exporter transaction timeout differs",
            ));
        }
    }
    Ok(())
}

#[track_caller]
fn follow_refusal(message: &str) -> Error {
    Error::new(ErrorKind::BadState, message)
}
