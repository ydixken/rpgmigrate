//! Native state requires exclusive control of its private 0700 run directory.
//! Permissions and advisory locks protect against other UIDs and cooperating rpgmigrate processes.
//! Arbitrary noncooperating mutation by the same UID during an active run is outside this guarantee.
//! POSIX validation followed by rename does not provide compare-and-swap protection.

use std::ffi::{CStr, CString};
use std::fs::{self, File, Metadata, TryLockError};
use std::io::{self, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cdc::contracts::Plugin;
use crate::contracts::{
    COPY_PLAN_REFERENCE_VERSION, CopyPlanRef, CopyReport, Error, ErrorKind,
    MAX_SNAPSHOT_TOKEN_BYTES, Result, RunId, SnapshotAnchor, StateAction, StateIdentity,
    StateRequest,
};
use crate::identity::{SourceEndLsn, StreamId};

mod plan_files;
pub(crate) use plan_files::{NativePlanDraft, NativePlanPin};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CopySnapshotIntent {
    pub(crate) anchor: SnapshotAnchor,
    pub(crate) plan: CopyPlanRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceSlotBinding {
    stream: StreamId,
    slot_name: String,
    plugin: Plugin,
    catalog_epoch: u64,
}

impl SourceSlotBinding {
    pub(crate) fn new(
        stream: StreamId,
        slot_name: String,
        plugin: Plugin,
        catalog_epoch: u64,
    ) -> Result<Self> {
        let binding = Self {
            stream,
            slot_name,
            plugin,
            catalog_epoch,
        };
        binding.validate(stream.run)?;
        Ok(binding)
    }

    pub(crate) fn stream(&self) -> StreamId {
        self.stream
    }
    pub(crate) fn slot_name(&self) -> &str {
        &self.slot_name
    }
    pub(crate) fn plugin(&self) -> Plugin {
        self.plugin
    }
    pub(crate) fn catalog_epoch(&self) -> u64 {
        self.catalog_epoch
    }

    fn validate(&self, run: RunId) -> Result<()> {
        if self.stream.run != run
            || run.0 == [0; 16]
            || self.stream.source.system_identifier == 0
            || self.stream.source.database_oid == 0
            || self.catalog_epoch == 0
            || self.slot_name.is_empty()
            || self.slot_name.len() > 63
            || !self
                .slot_name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(bad_state(
                "state-source: source slot binding has an invalid identity, epoch, or name",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceSlotIntent {
    pub(crate) binding: SourceSlotBinding,
    pub(crate) nonce: [u8; 16],
}

pub(crate) struct CreatedSlotData {
    pub(crate) consistent_point: SourceEndLsn,
    pub(crate) snapshot_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceSlotReceipt {
    pub(crate) intent: SourceSlotIntent,
    pub(crate) consistent_point: SourceEndLsn,
    pub(crate) snapshot_token: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "phase",
    content = "record",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum SourceSlotProvenance {
    Provisioning(SourceSlotIntent),
    Confirmed(SourceSlotReceipt),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TargetBinding {
    pub(crate) stream: StreamId,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) target_system_identifier: Option<u64>,
    pub(crate) target_database_oid: u32,
    pub(crate) schema_name: String,
    pub(crate) table_name: String,
    pub(crate) owner_oid: u32,
    pub(crate) metadata_version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProvisioningIntent {
    pub(crate) binding: TargetBinding,
    pub(crate) nonce: [u8; 16],
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfirmedProvenance {
    pub(crate) intent: ProvisioningIntent,
    pub(crate) schema_oid: u32,
    pub(crate) table_oid: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "phase",
    content = "record",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub(crate) enum TargetProvenance {
    Provisioning(ProvisioningIntent),
    Confirmed(ConfirmedProvenance),
}

const MANIFEST: &CStr = c"manifest.json";
const TEMP: &CStr = c".manifest.tmp";
const MAX_MANIFEST_BYTES: u64 = 16 * 1024;
const FORMAT_VERSION: u32 = 4;
const MAX_TARGET_PROVENANCE: usize = 1;
pub(crate) const TARGET_METADATA_VERSION: u32 = 3;
const DIRECTORY_FLAGS: libc::c_int = libc::O_RDONLY | libc::O_DIRECTORY;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Manifest {
    magic: String,
    format_version: u32,
    run: RunId,
    canonical_path: PathBuf,
    directory_device: u64,
    directory_inode: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    copy_complete: Option<Completion>,
    target_provenance: Vec<TargetProvenance>,
    #[serde(deserialize_with = "Option::deserialize")]
    source_slot: Option<SourceSlotProvenance>,
    #[serde(deserialize_with = "Option::deserialize")]
    copy_snapshot: Option<CopySnapshotIntent>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Completion {
    tables: u64,
    rows: u64,
    bytes: u64,
}

#[derive(Debug)]
struct Directory {
    parent_path: PathBuf,
    parent: File,
    name: CString,
    handle: File,
}

#[derive(Debug)]
pub(crate) struct NativeState {
    identity: StateIdentity,
    directory: Directory,
    manifest: Manifest,
    manifest_file: File,
    fresh_created: bool,
    plans: plan_files::PlanNamespace,
}

pub(crate) fn open(request: &StateRequest) -> Result<NativeState> {
    open_native(request, false)
}

/// Reload local records only; these do not prove a live source snapshot or fresh fenced target authority.
/// An interrupted publication that leaves .manifest.tmp fails closed; this slice does not repair it.
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "native CDC resume consumer is pending")
)]
pub(crate) fn open_for_provenance_recovery(path: &Path) -> Result<NativeState> {
    open_native(
        &StateRequest {
            path: path.into(),
            action: StateAction::Resume,
        },
        true,
    )
}

fn open_native(request: &StateRequest, recover_provenance: bool) -> Result<NativeState> {
    let leaf = request.path.file_name().ok_or_else(|| {
        bad_state("state-unsafe: specify a state directory below an existing parent")
    })?;
    let name = CString::new(leaf.as_bytes())
        .map_err(|_| bad_state("state-unsafe: state directory name is invalid"))?;
    let parent_path = fs::canonicalize(
        request
            .path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
    .map_err(|_| bad_state("state-parent: state parent must exist and be accessible"))?;
    let parent = open_parent(&parent_path)?;
    let handle = match open_at(&parent, &name, DIRECTORY_FLAGS, 0) {
        Ok(handle) => handle,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if request.action != StateAction::Create {
                return Err(bad_state("state-missing: native state does not exist"));
            }
            check_same(&parent, &open_parent(&parent_path)?)?;
            // SAFETY: parent is live and name is a NUL-terminated single path component.
            if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
                return Err(bad_state(
                    "state-create: cannot exclusively create state directory",
                ));
            }
            parent
                .sync_all()
                .map_err(|_| bad_state("state-sync: cannot synchronize state parent"))?;
            open_at(&parent, &name, DIRECTORY_FLAGS, 0)
                .map_err(|_| bad_state("state-replaced: new state directory is unavailable"))?
        }
        Err(_) => {
            return Err(bad_state(
                "state-unsafe: state path is not a real directory",
            ));
        }
    };
    check_private(&metadata(&handle)?, 0o700)?;
    handle.try_lock().map_err(|error| match error {
        TryLockError::WouldBlock => {
            bad_state("state-busy: another owner holds the state directory lock")
        }
        TryLockError::Error(_) => bad_state("state-lock: cannot acquire the state directory lock"),
    })?;
    let directory = Directory {
        parent_path,
        parent,
        name,
        handle,
    };
    directory.check()?;
    let path = directory.parent_path.join(leaf);
    let entries = root_entries(&directory.handle, false)?;
    if entries.manifest {
        let (manifest, manifest_file) = read_manifest(&directory, &path)?;
        let plans = plan_files::PlanNamespace::open(&directory)?;
        if recover_provenance {
            if manifest.target_provenance.is_empty() && manifest.source_slot.is_none() {
                return Err(bad_state(
                    "state-provenance: native state has no provisioning intent",
                ));
            }
            return Ok(NativeState {
                identity: StateIdentity {
                    run: manifest.run,
                    canonical_path: path,
                    format_version: manifest.format_version,
                },
                directory,
                manifest,
                manifest_file,
                fresh_created: false,
                plans,
            });
        }
        if request.action == StateAction::Create {
            return Err(bad_state(
                "state-exists: native state already exists; use a fresh directory",
            ));
        }
        // E1 cannot bind prior target work to source/target identities or reconcile unknown COMMIT.
        return Err(bad_state(
            "state-recovery-unsupported: E1 cannot resume or restart; recovery requires source and target identity binding",
        ));
    }
    if request.action != StateAction::Create {
        return Err(bad_state(
            "state-missing: native state manifest does not exist",
        ));
    }
    if entries.data {
        return Err(bad_state(
            "state-foreign: data without native state cannot be adopted",
        ));
    }
    let plans = plan_files::PlanNamespace::create(&directory)?;
    let meta = metadata(&directory.handle)?;
    let run = RunId(random_identity()?);
    let manifest = Manifest {
        magic: "RPGMIGRATE".into(),
        format_version: FORMAT_VERSION,
        run,
        canonical_path: path.clone(),
        directory_device: meta.dev(),
        directory_inode: meta.ino(),
        copy_complete: None,
        target_provenance: Vec::new(),
        source_slot: None,
        copy_snapshot: None,
    };
    let manifest_file = publish(&directory, &plans, &manifest, None)?;
    Ok(NativeState {
        identity: StateIdentity {
            run,
            canonical_path: path,
            format_version: FORMAT_VERSION,
        },
        directory,
        manifest,
        manifest_file,
        fresh_created: true,
        plans,
    })
}

impl NativeState {
    pub(crate) fn identity(&self) -> &StateIdentity {
        &self.identity
    }

    pub(crate) fn bind_copy_snapshot(&mut self, intent: &CopySnapshotIntent) -> Result<()> {
        let mut next = self.checked_manifest()?;
        intent.validate(&next)?;
        if let Some(existing) = &next.copy_snapshot {
            if existing != intent {
                return Err(bad_state(
                    "state-copy: existing COPY snapshot intent cannot be replaced",
                ));
            }
            self.open_plan_file(intent.plan.plan_nonce)?;
            return self.synchronize_manifest();
        }
        self.open_plan_file(intent.plan.plan_nonce)?;
        next.copy_snapshot = Some(intent.clone());
        self.replace_manifest(next)
    }

    pub(crate) fn checked_copy_snapshot(&self) -> Result<Option<CopySnapshotIntent>> {
        let current = self.checked_manifest()?;
        if let Some(intent) = &current.copy_snapshot {
            self.open_plan_file(intent.plan.plan_nonce)?;
        }
        Ok(current.copy_snapshot)
    }

    /// Only a successful durable publication authorizes the caller's single CREATE attempt.
    pub(crate) fn begin_source_slot(
        &mut self,
        binding: &SourceSlotBinding,
    ) -> Result<SourceSlotIntent> {
        binding.validate(self.identity.run)?;
        let mut next = self.checked_manifest()?;
        if !self.fresh_created || next.source_slot.is_some() || next.copy_complete.is_some() {
            return Err(bad_state(
                "state-source: source slot intent requires a fresh incomplete run without prior source intent",
            ));
        }
        let intent = SourceSlotIntent {
            binding: binding.clone(),
            nonce: random_identity()?,
        };
        next.source_slot = Some(SourceSlotProvenance::Provisioning(intent.clone()));
        next.validate_provenance()?;
        // Even a failed publication must not authorize a second CREATE attempt through this handle.
        self.fresh_created = false;
        self.replace_manifest(next)?;
        Ok(intent)
    }

    pub(crate) fn confirm_source_slot(
        &mut self,
        intent: &SourceSlotIntent,
        created: CreatedSlotData,
    ) -> Result<SourceSlotReceipt> {
        let receipt = SourceSlotReceipt {
            intent: intent.clone(),
            consistent_point: created.consistent_point,
            snapshot_token: created.snapshot_name,
        };
        let replacement = SourceSlotProvenance::Confirmed(receipt.clone());
        replacement.validate(self.identity.run)?;
        let mut next = self.checked_manifest()?;
        match next.source_slot.as_ref() {
            Some(SourceSlotProvenance::Provisioning(pending)) if pending == intent => {
                next.source_slot = Some(replacement);
                self.replace_manifest(next)?;
            }
            Some(existing) if existing == &replacement => self.synchronize_manifest()?,
            _ => {
                return Err(bad_state(
                    "state-source: confirmation does not match the persisted source slot intent",
                ));
            }
        }
        Ok(receipt)
    }

    /// Confirmed local records cannot reconstruct an imported snapshot or prove source history.
    pub(crate) fn checked_source_binding(&self, expected: &SourceSlotBinding) -> Result<()> {
        expected.validate(self.identity.run)?;
        let current = self.checked_manifest()?;
        if current.run != self.identity.run
            || current.format_version != self.identity.format_version
        {
            return Err(bad_state(
                "state-source: held native identity does not match source state",
            ));
        }
        match current.source_slot {
            Some(SourceSlotProvenance::Confirmed(receipt))
                if receipt.intent.binding == *expected =>
            {
                Ok(())
            }
            _ => Err(bad_state(
                "state-source: no exact confirmed source slot binding",
            )),
        }
    }

    /// Local provenance is neither target authority nor proof of durable target progress.
    pub(crate) fn target_provenance(&self, stream: &StreamId) -> Option<&TargetProvenance> {
        self.manifest
            .target_provenance
            .iter()
            .find(|record| record.intent().binding.stream == *stream)
    }

    /// Return only after the intent is durable; an identical retry synchronizes the same intent again.
    /// TargetAuthority must separately authorize creation on the acquired physical backend.
    pub(crate) fn begin_target_provisioning(
        &mut self,
        binding: TargetBinding,
    ) -> Result<ProvisioningIntent> {
        binding.validate(self.identity.run)?;
        let mut next = self.checked_manifest()?;
        if let Some(record) = next.target_provenance.first() {
            if let TargetProvenance::Provisioning(intent) = record
                && intent.binding == binding
            {
                self.synchronize_manifest()?;
                return Ok(intent.clone());
            }
            return Err(bad_state(
                "state-provenance: existing target binding or phase cannot be replaced",
            ));
        }
        let intent = ProvisioningIntent {
            binding,
            nonce: random_identity()?,
        };
        next.target_provenance
            .push(TargetProvenance::Provisioning(intent.clone()));
        self.replace_manifest(next)?;
        Ok(intent)
    }

    /// The caller must verify the intent receipt, object identities, and synchronous target COMMIT under its fence.
    /// Stored Some(system ID) must match fresh evidence; None with absent objects cannot authorize recovery after reconnect.
    pub(crate) fn confirm_target_provisioning(
        &mut self,
        confirmed: ConfirmedProvenance,
    ) -> Result<()> {
        let replacement = TargetProvenance::Confirmed(confirmed);
        replacement.validate(self.identity.run)?;
        let mut next = self.checked_manifest()?;
        match next.target_provenance.first() {
            Some(TargetProvenance::Provisioning(intent)) if intent == replacement.intent() => {
                next.target_provenance[0] = replacement;
                self.replace_manifest(next)
            }
            Some(existing) if existing == &replacement => self.synchronize_manifest(),
            _ => Err(bad_state(
                "state-provenance: confirmation does not match the persisted provisioning intent",
            )),
        }
    }

    /// Call only after all terminal COPY results and synchronous target COMMIT responses succeed.
    /// This receipt cannot authorize recovery or reuse of target data.
    pub(crate) fn record_copy_complete(&mut self, report: &CopyReport) -> Result<()> {
        let mut completed = self.checked_manifest()?;
        if completed.copy_complete.is_some() {
            return Err(bad_state(
                "state-complete: COPY completion is already recorded",
            ));
        }
        completed.copy_complete = Some(Completion {
            tables: report.tables,
            rows: report.rows,
            bytes: report.bytes,
        });
        self.replace_manifest(completed)
    }

    #[cfg(test)]
    pub(crate) fn record_copy_complete_with_create_failure(
        &mut self,
        report: &CopyReport,
    ) -> Result<()> {
        let _failure = tests::PublicationFailure::at(tests::PublicationStep::Create);
        self.record_copy_complete(report)
    }

    fn checked_manifest(&self) -> Result<Manifest> {
        self.checked_manifest_with_draft(None)
    }

    fn checked_manifest_with_draft(&self, draft: Option<[u8; 16]>) -> Result<Manifest> {
        self.directory.check()?;
        self.plans.check(&self.directory, draft)?;
        if !manifest_present(&self.directory.handle, false)? {
            return Err(bad_state("state-replaced: native manifest disappeared"));
        }
        let (current, file) = read_manifest(&self.directory, &self.identity.canonical_path)?;
        check_same(&self.manifest_file, &file)?;
        if current != self.manifest {
            return Err(bad_state(
                "state-replaced: native manifest changed during this run",
            ));
        }
        Ok(current)
    }

    fn replace_manifest(&mut self, next: Manifest) -> Result<()> {
        next.validate_provenance()?;
        self.manifest_file = publish(
            &self.directory,
            &self.plans,
            &next,
            Some((&self.manifest, &self.manifest_file)),
        )?;
        self.manifest = next;
        Ok(())
    }

    fn synchronize_manifest(&self) -> Result<()> {
        #[cfg(test)]
        tests::publication_step(tests::PublicationStep::FileSync)?;
        self.manifest_file.sync_all().map_err(|_| {
            bad_state("state-sync: cannot synchronize native provisioning metadata")
        })?;
        #[cfg(test)]
        tests::publication_step(tests::PublicationStep::DirectorySync)?;
        self.directory.handle.sync_all().map_err(|_| {
            bad_state("state-sync: cannot synchronize native provisioning directory")
        })?;
        self.checked_manifest()?;
        Ok(())
    }
}

impl SourceSlotProvenance {
    fn intent(&self) -> &SourceSlotIntent {
        match self {
            Self::Provisioning(intent) => intent,
            Self::Confirmed(receipt) => &receipt.intent,
        }
    }

    fn validate(&self, run: RunId) -> Result<()> {
        let intent = self.intent();
        intent.binding.validate(run)?;
        if intent.nonce == [0; 16] {
            return Err(bad_state("state-source: source slot nonce must be nonzero"));
        }
        if let Self::Confirmed(receipt) = self
            && (receipt.consistent_point.0 == 0
                || receipt.snapshot_token.is_empty()
                || receipt.snapshot_token.len() > MAX_SNAPSHOT_TOKEN_BYTES
                || receipt.snapshot_token.contains('\0'))
        {
            return Err(bad_state(
                "state-source: source slot receipt has an invalid position or snapshot token",
            ));
        }
        Ok(())
    }
}

impl CopySnapshotIntent {
    fn validate(&self, manifest: &Manifest) -> Result<()> {
        if self.anchor.stream.run != manifest.run
            || self.anchor.consistent_point.0 == 0
            || self.anchor.snapshot_token.is_empty()
            || self.anchor.snapshot_token.len() > MAX_SNAPSHOT_TOKEN_BYTES
            || self.anchor.snapshot_token.contains('\0')
            || self.anchor.plan_nonce == [0; 16]
            || self.plan.plan_nonce != self.anchor.plan_nonce
            || self.plan.reference_version != COPY_PLAN_REFERENCE_VERSION
        {
            return Err(bad_state(
                "state-copy: COPY snapshot identity or reference is invalid",
            ));
        }
        match &manifest.source_slot {
            Some(SourceSlotProvenance::Confirmed(receipt))
                if receipt.intent.binding.stream == self.anchor.stream
                    && receipt.consistent_point == self.anchor.consistent_point
                    && receipt.snapshot_token == self.anchor.snapshot_token => {}
            _ => {
                return Err(bad_state(
                    "state-copy: COPY snapshot requires exact confirmed source provenance",
                ));
            }
        }
        match manifest.target_provenance.first() {
            Some(TargetProvenance::Confirmed(receipt))
                if receipt.intent.binding.stream == self.anchor.stream =>
            {
                Ok(())
            }
            _ => Err(bad_state(
                "state-copy: COPY snapshot requires exact confirmed target provenance",
            )),
        }
    }
}

impl TargetBinding {
    fn validate(&self, run: RunId) -> Result<()> {
        if self.stream.run != run
            || self.stream.source.system_identifier == 0
            || self.stream.source.database_oid == 0
            || self.target_system_identifier == Some(0)
            || self.target_database_oid == 0
            || self.owner_oid == 0
            || self.metadata_version != TARGET_METADATA_VERSION
            || self.schema_name.is_empty()
            || self.table_name.is_empty()
            || self.schema_name.contains('\0')
            || self.table_name.contains('\0')
        {
            return Err(bad_state(
                "state-provenance: target binding has an invalid identity, name, owner, or metadata version",
            ));
        }
        Ok(())
    }
}

impl TargetProvenance {
    fn intent(&self) -> &ProvisioningIntent {
        match self {
            Self::Provisioning(intent) => intent,
            Self::Confirmed(confirmed) => &confirmed.intent,
        }
    }

    fn validate(&self, run: RunId) -> Result<()> {
        let intent = self.intent();
        intent.binding.validate(run)?;
        if intent.nonce == [0; 16] {
            return Err(bad_state(
                "state-provenance: provisioning nonce must be nonzero",
            ));
        }
        if let Self::Confirmed(confirmed) = self
            && (confirmed.schema_oid == 0 || confirmed.table_oid == 0)
        {
            return Err(bad_state(
                "state-provenance: confirmed object OIDs must be nonzero",
            ));
        }
        Ok(())
    }
}

impl Manifest {
    fn validate_provenance(&self) -> Result<()> {
        if self.target_provenance.len() > MAX_TARGET_PROVENANCE {
            return Err(bad_state(
                "state-provenance: this slice permits only one unique target stream",
            ));
        }
        for record in &self.target_provenance {
            record.validate(self.run)?;
        }
        if let Some(source) = &self.source_slot {
            source.validate(self.run)?;
            if self.target_provenance.first().is_some_and(|target| {
                target.intent().binding.stream != source.intent().binding.stream
            }) {
                return Err(bad_state(
                    "state-source: source and target provenance streams differ",
                ));
            }
        }
        if let Some(intent) = &self.copy_snapshot {
            intent.validate(self)?;
        }
        Ok(())
    }
}

fn random_identity() -> Result<[u8; 16]> {
    let mut bytes = [0; 16];
    // SAFETY: bytes provides 16 writable bytes, below getentropy's 256-byte limit.
    if unsafe { libc::getentropy(bytes.as_mut_ptr().cast(), bytes.len()) } != 0 || bytes == [0; 16]
    {
        return Err(bad_state("state-random: cannot generate native identity"));
    }
    Ok(bytes)
}

impl Drop for Directory {
    fn drop(&mut self) {
        // A concurrent child spawn can retain the open description until exec closes its descriptors.
        let _ = self.handle.unlock();
    }
}

impl Directory {
    fn check(&self) -> Result<()> {
        let parent = open_parent(&self.parent_path)?;
        check_same(&self.parent, &parent)?;
        let current = open_at(&parent, &self.name, DIRECTORY_FLAGS, 0).map_err(|_| {
            bad_state("state-replaced: state directory disappeared or became unsafe")
        })?;
        check_same(&self.handle, &current)?;
        check_private(&metadata(&self.handle)?, 0o700)
    }
}

fn bad_state(message: &'static str) -> Error {
    Error {
        kind: ErrorKind::BadState,
        message: message.into(),
    }
}

fn metadata(file: &File) -> Result<Metadata> {
    file.metadata()
        .map_err(|_| bad_state("state-io: cannot inspect state ownership"))
}

fn check_same(expected: &File, current: &File) -> Result<()> {
    let expected = metadata(expected)?;
    let current = metadata(current)?;
    if (expected.dev(), expected.ino()) != (current.dev(), current.ino()) {
        return Err(bad_state(
            "state-replaced: state directory or file identity changed",
        ));
    }
    Ok(())
}

fn check_private(meta: &Metadata, mode: u32) -> Result<()> {
    // SAFETY: geteuid has no arguments or memory preconditions.
    if meta.uid() != unsafe { libc::geteuid() } || meta.mode() & 0o7777 != mode {
        return Err(bad_state(
            "state-permissions: native state must have private owner permissions",
        ));
    }
    Ok(())
}

fn open_at(parent: &File, name: &CStr, flags: libc::c_int, mode: libc::c_uint) -> io::Result<File> {
    // SAFETY: parent stays live, name is NUL-terminated, and mode is supplied for O_CREAT.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            mode,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openat returned a new owned descriptor, which File closes exactly once.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn open_parent(path: &Path) -> Result<File> {
    let mut directory =
        File::open("/").map_err(|_| bad_state("state-parent: cannot open state parent"))?;
    for component in path.components() {
        if let Component::Normal(name) = component {
            let name = CString::new(name.as_bytes())
                .map_err(|_| bad_state("state-parent: state parent name is invalid"))?;
            directory = open_at(&directory, &name, DIRECTORY_FLAGS, 0)
                .map_err(|_| bad_state("state-parent: state parent changed or is inaccessible"))?;
        }
        let meta = metadata(&directory)?;
        // SAFETY: geteuid has no arguments or memory preconditions.
        let trusted_owner = meta.uid() == 0 || meta.uid() == unsafe { libc::geteuid() };
        if !trusted_owner || (meta.mode() & 0o022 != 0 && meta.mode() & 0o1000 == 0) {
            return Err(bad_state(
                "state-parent: state parent permits unsafe directory replacement",
            ));
        }
    }
    Ok(directory)
}

struct RootEntries {
    manifest: bool,
    data: bool,
}

fn manifest_present(directory: &File, allow_temp: bool) -> Result<bool> {
    Ok(root_entries(directory, allow_temp)?.manifest)
}

fn root_entries(directory: &File, allow_temp: bool) -> Result<RootEntries> {
    let handle = open_at(directory, c".", DIRECTORY_FLAGS, 0)
        .map_err(|_| bad_state("state-io: cannot inspect state directory"))?;
    let fd = handle.into_raw_fd();
    // SAFETY: fd is an owned directory descriptor; fdopendir takes ownership only on success.
    let stream = unsafe { libc::fdopendir(fd) };
    if stream.is_null() {
        // SAFETY: fdopendir failed and left the owned fd open.
        drop(unsafe { File::from_raw_fd(fd) });
        return Err(bad_state("state-io: cannot inspect state directory"));
    }
    let mut found = RootEntries {
        manifest: false,
        data: false,
    };
    let result = loop {
        // SAFETY: errno is thread-local, and stream remains live until closed below.
        let entry = unsafe {
            *errno_pointer() = 0;
            libc::readdir(stream)
        };
        if entry.is_null() {
            let error = io::Error::last_os_error();
            break if error.raw_os_error() == Some(0) {
                Ok(found)
            } else {
                Err(bad_state("state-io: cannot read state directory entries"))
            };
        }
        // SAFETY: readdir returned a live entry with a NUL-terminated d_name until the next call.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        match name.to_bytes() {
            b"." | b".." => (),
            b"manifest.json" => found.manifest = true,
            b"data" => found.data = true,
            b".manifest.tmp" if allow_temp => (),
            _ => {
                break Err(bad_state(
                    "state-foreign: state directory contains unrecognized entries",
                ));
            }
        }
    };
    // SAFETY: this call releases stream and the descriptor transferred to it exactly once.
    if unsafe { libc::closedir(stream) } != 0 {
        return Err(bad_state("state-io: cannot close state directory scan"));
    }
    result
}

unsafe fn errno_pointer() -> *mut libc::c_int {
    #[cfg(target_os = "macos")]
    // SAFETY: __error returns this thread's errno address.
    unsafe {
        libc::__error()
    }
    #[cfg(target_os = "linux")]
    // SAFETY: __errno_location returns this thread's errno address.
    unsafe {
        libc::__errno_location()
    }
}

fn open_metadata(directory: &File, name: &CStr) -> Result<File> {
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: directory and name remain live; stat points to writable stat storage.
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(bad_state("state-invalid: native metadata is unavailable"));
    }
    // SAFETY: successful fstatat initialized stat.
    let stat = unsafe { stat.assume_init() };
    if stat.st_mode & libc::S_IFMT != libc::S_IFREG || stat.st_nlink != 1 {
        return Err(bad_state(
            "state-unsafe: native metadata must be a regular file with one link",
        ));
    }
    let file = open_at(directory, name, libc::O_RDONLY, 0)
        .map_err(|_| bad_state("state-unsafe: native metadata cannot be opened safely"))?;
    let meta = metadata(&file)?;
    if !meta.is_file()
        || meta.nlink() != 1
        || (meta.dev(), meta.ino()) != (stat.st_dev as _, stat.st_ino)
    {
        return Err(bad_state("state-unsafe: native metadata identity changed"));
    }
    check_private(&meta, 0o600)?;
    Ok(file)
}

fn read_manifest(directory: &Directory, path: &Path) -> Result<(Manifest, File)> {
    let file = open_metadata(&directory.handle, MANIFEST)?;
    let mut bytes = Vec::new();
    (&file)
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| bad_state("state-invalid: cannot read native manifest"))?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(bad_state(
            "state-invalid: native manifest exceeds the control metadata limit",
        ));
    }
    // Inspect the format before decoding its body so older native manifests cannot be converted implicitly.
    #[derive(Deserialize)]
    struct FormatHeader {
        magic: String,
        format_version: u32,
    }
    let header: FormatHeader = serde_json::from_slice(&bytes)
        .map_err(|_| bad_state("state-invalid: native manifest is corrupt or incomplete"))?;
    if header.magic != "RPGMIGRATE" {
        return Err(bad_state("state-identity: native state magic is invalid"));
    }
    if header.format_version != FORMAT_VERSION {
        return Err(bad_state(
            "state-version: native state version is unsupported",
        ));
    }
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|_| bad_state("state-invalid: native manifest is corrupt or incomplete"))?;
    let meta = metadata(&directory.handle)?;
    if manifest.run.0 == [0; 16]
        || manifest.canonical_path != path
        || manifest.directory_device != meta.dev()
        || manifest.directory_inode != meta.ino()
    {
        return Err(bad_state(
            "state-identity: native state identity does not match this directory",
        ));
    }
    manifest.validate_provenance()?;
    directory.check()?;
    Ok((manifest, file))
}

fn publish(
    directory: &Directory,
    plans: &plan_files::PlanNamespace,
    manifest: &Manifest,
    previous: Option<(&Manifest, &File)>,
) -> Result<File> {
    let bytes = serde_json::to_vec(manifest)
        .map_err(|_| bad_state("state-invalid: cannot encode native manifest"))?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(bad_state(
            "state-invalid: native manifest exceeds the control metadata limit",
        ));
    }
    check_publication(directory, plans, manifest, previous, false)?;
    #[cfg(test)]
    tests::publication_step(tests::PublicationStep::Create)?;
    let mut temporary = open_at(
        &directory.handle,
        TEMP,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o600,
    )
    .map_err(|_| bad_state("state-write: cannot exclusively create temporary native metadata"))?;
    #[cfg(test)]
    tests::publication_step(tests::PublicationStep::Write)?;
    temporary
        .write_all(&bytes)
        .map_err(|_| bad_state("state-write: cannot write temporary native metadata"))?;
    #[cfg(test)]
    tests::publication_step(tests::PublicationStep::FileSync)?;
    temporary
        .sync_all()
        .map_err(|_| bad_state("state-sync: cannot synchronize temporary native metadata"))?;
    check_publication(directory, plans, manifest, previous, true)?;
    check_same(&temporary, &open_metadata(&directory.handle, TEMP)?)?;
    #[cfg(test)]
    tests::publication_step(tests::PublicationStep::Rename)?;
    // SAFETY: both names are single components in the same live directory; its advisory lock remains held.
    if unsafe {
        libc::renameat(
            directory.handle.as_raw_fd(),
            TEMP.as_ptr(),
            directory.handle.as_raw_fd(),
            MANIFEST.as_ptr(),
        )
    } != 0
    {
        return Err(bad_state("state-write: cannot publish native manifest"));
    }
    #[cfg(test)]
    tests::publication_step(tests::PublicationStep::DirectorySync)?;
    directory
        .handle
        .sync_all()
        .map_err(|_| bad_state("state-sync: cannot synchronize native manifest directory"))?;
    #[cfg(test)]
    tests::publication_step(tests::PublicationStep::Published)?;
    directory.check()?;
    let published = open_metadata(&directory.handle, MANIFEST)?;
    check_same(&temporary, &published)?;
    Ok(published)
}

fn check_publication(
    directory: &Directory,
    plans: &plan_files::PlanNamespace,
    manifest: &Manifest,
    previous: Option<(&Manifest, &File)>,
    allow_temp: bool,
) -> Result<()> {
    directory.check()?;
    plans.check(directory, None)?;
    let present = manifest_present(&directory.handle, allow_temp)?;
    match previous {
        Some((expected, file)) if present => {
            let (current, current_file) = read_manifest(directory, &manifest.canonical_path)?;
            check_same(file, &current_file)?;
            if current != *expected {
                return Err(bad_state(
                    "state-replaced: native manifest changed before publication",
                ));
            }
        }
        None if !present => (),
        _ => {
            return Err(bad_state(
                "state-replaced: native manifest appeared or disappeared before publication",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::StateAction;
    use serde_json::{Value, json};
    use std::cell::Cell;
    use std::collections::BTreeMap;
    use std::fs::{self, DirBuilder, File, Permissions};
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt, symlink};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum PublicationStep {
        Create,
        Write,
        FileSync,
        Rename,
        DirectorySync,
        Published,
    }

    thread_local! {
        static FAIL_AT: Cell<Option<PublicationStep>> = const { Cell::new(None) };
    }

    pub(super) struct PublicationFailure;

    impl PublicationFailure {
        pub(super) fn at(step: PublicationStep) -> Self {
            FAIL_AT.with(|slot| assert!(slot.replace(Some(step)).is_none()));
            Self
        }
    }

    impl Drop for PublicationFailure {
        fn drop(&mut self) {
            FAIL_AT.with(|slot| slot.set(None));
        }
    }

    pub(super) fn publication_step(step: PublicationStep) -> Result<()> {
        FAIL_AT.with(|slot| {
            if slot.get() == Some(step) {
                slot.set(None);
                Err(bad_state("state-test: injected publication failure"))
            } else {
                Ok(())
            }
        })
    }

    pub(super) struct TempDir(pub(super) PathBuf);

    impl TempDir {
        pub(super) fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let parent = fs::canonicalize(std::env::temp_dir()).unwrap();
            loop {
                let path = parent.join(format!(
                    "rpgmigrate-state-test-{}-{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                match DirBuilder::new().mode(0o700).create(&path) {
                    Ok(()) => return Self(path),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => panic!("cannot create test directory: {error}"),
                }
            }
        }

        pub(super) fn path(&self) -> PathBuf {
            self.0.join("state")
        }

        pub(super) fn create(&self) -> NativeState {
            open(&request(self.path(), StateAction::Create)).unwrap()
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn request(path: PathBuf, action: StateAction) -> StateRequest {
        StateRequest { path, action }
    }

    fn manifest(path: &Path) -> Value {
        serde_json::from_slice(&fs::read(path.join("manifest.json")).unwrap()).unwrap()
    }

    fn change_manifest(path: &Path, edit: impl FnOnce(&mut Value)) {
        let mut value = manifest(path);
        edit(&mut value);
        fs::write(
            path.join("manifest.json"),
            serde_json::to_vec(&value).unwrap(),
        )
        .unwrap();
    }

    #[derive(Debug, PartialEq)]
    struct Entry {
        mode: u32,
        inode: u64,
        modified: (i64, i64),
        contents: Vec<u8>,
    }

    fn tree(path: &Path) -> BTreeMap<PathBuf, Entry> {
        fn collect(path: &Path, entries: &mut BTreeMap<PathBuf, Entry>) {
            let meta = fs::symlink_metadata(path).unwrap();
            let contents = if meta.is_file() {
                fs::read(path).unwrap()
            } else if meta.is_symlink() {
                fs::read_link(path)
                    .unwrap()
                    .as_os_str()
                    .as_encoded_bytes()
                    .to_vec()
            } else {
                Vec::new()
            };
            entries.insert(
                path.into(),
                Entry {
                    mode: meta.mode(),
                    inode: meta.ino(),
                    modified: (meta.mtime(), meta.mtime_nsec()),
                    contents,
                },
            );
            if meta.is_dir() {
                for entry in fs::read_dir(path).unwrap() {
                    collect(&entry.unwrap().path(), entries);
                }
            }
        }
        let mut entries = BTreeMap::new();
        collect(path, &mut entries);
        entries
    }

    fn refusal(path: &Path, action: StateAction, code: &str) {
        let before = tree(path);
        let error = open(&request(path.into(), action)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadState);
        assert!(error.message.starts_with(code), "{error:?}");
        assert_eq!(tree(path), before);
    }

    #[test]
    fn fresh_state_publishes_private_native_identity_without_completion() {
        let tmp = TempDir::new();
        let state = tmp.create();
        let value = manifest(&tmp.path());
        assert_eq!(value["magic"], "RPGMIGRATE");
        assert_eq!(value["format_version"], 4);
        assert_eq!(value["canonical_path"], tmp.path().to_str().unwrap());
        assert_eq!(value["run"].as_array().unwrap().len(), 16);
        assert_ne!(value["run"], json!(vec![0; 16]));
        assert_eq!(value["copy_complete"], Value::Null);
        assert_eq!(value["target_provenance"], json!([]));
        assert_eq!(value.get("source_slot"), Some(&Value::Null));
        assert_eq!(value.get("copy_snapshot"), Some(&Value::Null));
        assert_eq!(state.identity().canonical_path, tmp.path());
        assert_eq!(state.identity().format_version, 4);
        assert_eq!(value["run"], json!(state.identity().run.0));
        let directory = fs::metadata(tmp.path()).unwrap();
        assert_eq!(value["directory_inode"], directory.ino());
        assert_eq!(value["directory_device"], directory.dev());
        assert_eq!(directory.mode() & 0o777, 0o700);
        assert_eq!(
            fs::metadata(tmp.path().join("manifest.json"))
                .unwrap()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 2);
        for path in [tmp.path().join("data"), tmp.path().join("data/plans")] {
            assert_eq!(fs::metadata(path).unwrap().mode() & 0o7777, 0o700);
        }
    }

    #[test]
    fn source_slot_required_nullable_field_refuses_omission() {
        let tmp = TempDir::new();
        drop(tmp.create());
        change_manifest(&tmp.path(), |value| value["source_slot"] = Value::Null);
        recovery_refusal(&tmp.path(), "state-provenance:");
        change_manifest(&tmp.path(), |value| {
            value.as_object_mut().unwrap().remove("source_slot");
        });
        recovery_refusal(&tmp.path(), "state-invalid:");
        for action in [
            StateAction::Create,
            StateAction::Resume,
            StateAction::Restart,
        ] {
            refusal(&tmp.path(), action, "state-invalid:");
        }
    }

    #[test]
    fn existing_private_empty_directory_can_start_a_new_run() {
        let tmp = TempDir::new();
        DirBuilder::new().mode(0o700).create(tmp.path()).unwrap();
        let inode = fs::metadata(tmp.path()).unwrap().ino();
        let state = tmp.create();
        assert_eq!(state.identity().canonical_path, tmp.path());
        assert_eq!(fs::metadata(tmp.path()).unwrap().ino(), inode);
    }

    #[test]
    fn prior_native_run_cannot_be_opened_for_new_copy() {
        let tmp = TempDir::new();
        drop(tmp.create());
        refusal(&tmp.path(), StateAction::Create, "state-exists:");
    }

    #[test]
    fn native_resume_is_explicitly_unsupported_without_recovery_binding() {
        let tmp = TempDir::new();
        drop(tmp.create());
        refusal(
            &tmp.path(),
            StateAction::Resume,
            "state-recovery-unsupported:",
        );
    }

    #[test]
    fn native_restart_is_explicitly_unsupported_without_recovery_binding() {
        let tmp = TempDir::new();
        drop(tmp.create());
        refusal(
            &tmp.path(),
            StateAction::Restart,
            "state-recovery-unsupported:",
        );
    }

    #[test]
    fn missing_resume_and_restart_do_not_create_a_directory() {
        let tmp = TempDir::new();
        for action in [StateAction::Resume, StateAction::Restart] {
            let error = open(&request(tmp.path(), action)).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadState);
            assert!(!tmp.path().exists());
            assert!(error.message.starts_with("state-missing:"), "{error:?}");
        }
    }

    #[test]
    fn missing_parent_does_not_create_intermediate_directories() {
        let tmp = TempDir::new();
        let error = open(&request(tmp.path().join("nested"), StateAction::Create)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadState);
        assert!(!tmp.path().exists());
        assert!(error.message.starts_with("state-parent:"), "{error:?}");
    }

    #[test]
    fn legacy_and_unrelated_directories_remain_unchanged_for_all_actions() {
        for name in [
            "pgcopydb.db",
            "source.db",
            "target.db",
            "schema.json",
            "notes.txt",
        ] {
            let tmp = TempDir::new();
            DirBuilder::new().mode(0o700).create(tmp.path()).unwrap();
            fs::write(
                tmp.path().join(name),
                b"SQLite format 3\0legacy or unrelated bytes",
            )
            .unwrap();
            for action in [
                StateAction::Create,
                StateAction::Resume,
                StateAction::Restart,
            ] {
                refusal(&tmp.path(), action, "state-foreign:");
            }
        }
    }

    #[test]
    fn mixed_native_legacy_state_is_rejected_before_publication() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        fs::write(tmp.path().join("source.db"), b"SQLite format 3\0legacy").unwrap();
        let before = tree(&tmp.path());
        let error = state.record_copy_complete(&report()).unwrap_err();
        assert!(error.message.starts_with("state-foreign:"), "{error:?}");
        assert_eq!(tree(&tmp.path()), before);
        drop(state);
        refusal(&tmp.path(), StateAction::Restart, "state-foreign:");
    }

    #[test]
    fn unknown_version_is_rejected_before_restart() {
        let tmp = TempDir::new();
        drop(tmp.create());
        change_manifest(&tmp.path(), |value| value["format_version"] = json!(5));
        refusal(&tmp.path(), StateAction::Restart, "state-version:");
    }

    #[test]
    fn truncated_native_manifest_is_not_repaired() {
        let tmp = TempDir::new();
        drop(tmp.create());
        fs::write(
            tmp.path().join("manifest.json"),
            b"{\"magic\":\"RPGMIGRATE\"",
        )
        .unwrap();
        refusal(&tmp.path(), StateAction::Restart, "state-invalid:");
    }

    #[test]
    fn invalid_native_identity_is_rejected_without_mutation() {
        for (key, value) in [
            ("magic", json!("PGCOPYDB")),
            ("run", json!(vec![0; 16])),
            ("canonical_path", json!("/foreign/state")),
            ("directory_inode", json!(0)),
            ("directory_device", json!(u64::MAX)),
        ] {
            let tmp = TempDir::new();
            drop(tmp.create());
            change_manifest(&tmp.path(), |manifest| manifest[key] = value);
            refusal(&tmp.path(), StateAction::Restart, "state-identity:");
        }
    }

    #[test]
    fn unsafe_directory_permissions_are_not_repaired() {
        let tmp = TempDir::new();
        DirBuilder::new().mode(0o700).create(tmp.path()).unwrap();
        for mode in [0o500, 0o750, 0o777] {
            fs::set_permissions(tmp.path(), Permissions::from_mode(mode)).unwrap();
            refusal(&tmp.path(), StateAction::Create, "state-permissions:");
        }
        fs::set_permissions(tmp.path(), Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn unsafe_manifest_permissions_are_not_repaired() {
        let tmp = TempDir::new();
        drop(tmp.create());
        fs::set_permissions(
            tmp.path().join("manifest.json"),
            Permissions::from_mode(0o644),
        )
        .unwrap();
        refusal(&tmp.path(), StateAction::Restart, "state-permissions:");
    }

    #[test]
    fn directory_lock_conflict_never_creates_a_lock_marker() {
        let tmp = TempDir::new();
        DirBuilder::new().mode(0o700).create(tmp.path()).unwrap();
        let competing = File::open(tmp.path()).unwrap();
        competing.try_lock().unwrap();
        refusal(&tmp.path(), StateAction::Create, "state-busy:");
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 0);
        drop(competing);
        let state = tmp.create();
        refusal(&tmp.path(), StateAction::Create, "state-busy:");
        drop(state);
        let released = File::open(tmp.path()).unwrap();
        released.try_lock().unwrap();
    }

    #[test]
    fn final_directory_symlink_is_rejected_without_touching_its_target() {
        let tmp = TempDir::new();
        let foreign = tmp.0.join("foreign");
        DirBuilder::new().mode(0o700).create(&foreign).unwrap();
        fs::write(foreign.join("source.db"), b"legacy").unwrap();
        symlink(&foreign, tmp.path()).unwrap();
        let before = tree(&tmp.0);
        let error = open(&request(tmp.path(), StateAction::Restart)).unwrap_err();
        assert!(error.message.starts_with("state-unsafe:"), "{error:?}");
        assert_eq!(tree(&tmp.0), before);
    }

    #[test]
    fn existing_parent_alias_is_canonicalized_before_state_creation() {
        let tmp = TempDir::new();
        let alias = tmp.0.join("alias");
        symlink(&tmp.0, &alias).unwrap();
        let state = open(&request(alias.join("state"), StateAction::Create)).unwrap();
        assert_eq!(state.identity().canonical_path, tmp.path());
    }

    #[test]
    fn manifest_symlink_and_hardlink_are_rejected_without_target_changes() {
        for hard in [false, true] {
            let tmp = TempDir::new();
            drop(tmp.create());
            let external = tmp.0.join("external.json");
            fs::rename(tmp.path().join("manifest.json"), &external).unwrap();
            if hard {
                fs::hard_link(&external, tmp.path().join("manifest.json")).unwrap();
            } else {
                symlink(&external, tmp.path().join("manifest.json")).unwrap();
            }
            let before = tree(&tmp.0);
            let error = open(&request(tmp.path(), StateAction::Restart)).unwrap_err();
            assert!(error.message.starts_with("state-unsafe:"), "{error:?}");
            assert_eq!(tree(&tmp.0), before);
        }
    }

    fn report() -> CopyReport {
        CopyReport {
            tables: 2,
            rows: 10003,
            bytes: 10485760,
        }
    }

    #[test]
    fn completion_receipt_is_atomic_and_preserves_run_identity() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let run = state.identity().run;
        let old_manifest = fs::read(tmp.path().join("manifest.json")).unwrap();
        let old_file = File::open(tmp.path().join("manifest.json")).unwrap();
        state.record_copy_complete(&report()).unwrap();
        assert_eq!(state.identity().run, run);
        assert_eq!(
            manifest(&tmp.path())["copy_complete"],
            json!({"tables":2,"rows":10003,"bytes":10485760})
        );
        use std::io::Read;
        let mut old_bytes = Vec::new();
        (&old_file).read_to_end(&mut old_bytes).unwrap();
        assert_eq!(old_bytes, old_manifest);
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 2);
        let before = tree(&tmp.path());
        let error = state.record_copy_complete(&report()).unwrap_err();
        assert!(error.message.starts_with("state-complete:"), "{error:?}");
        assert_eq!(tree(&tmp.path()), before);
        drop(state);
        refusal(
            &tmp.path(),
            StateAction::Resume,
            "state-recovery-unsupported:",
        );
    }

    #[test]
    fn aborted_or_unknown_commit_has_no_implicit_completion_on_drop() {
        let tmp = TempDir::new();
        drop(tmp.create());
        assert_eq!(manifest(&tmp.path())["copy_complete"], Value::Null);
        refusal(&tmp.path(), StateAction::Create, "state-exists:");
    }

    #[test]
    fn replaced_directory_is_rejected_before_completion_without_cleanup() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let moved = tmp.0.join("moved");
        fs::rename(tmp.path(), &moved).unwrap();
        DirBuilder::new().mode(0o700).create(tmp.path()).unwrap();
        fs::write(tmp.path().join("source.db"), b"legacy").unwrap();
        let before = tree(&tmp.0);
        let error = state.record_copy_complete(&report()).unwrap_err();
        assert!(error.message.starts_with("state-replaced:"), "{error:?}");
        assert_eq!(tree(&tmp.0), before);
    }

    #[test]
    fn changed_run_identity_is_rejected_before_completion() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        change_manifest(&tmp.path(), |value| value["run"] = json!(vec![7; 16]));
        let before = tree(&tmp.path());
        let error = state.record_copy_complete(&report()).unwrap_err();
        assert!(error.message.starts_with("state-replaced:"), "{error:?}");
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn incomplete_or_extended_manifest_is_rejected_before_restart() {
        for field in [
            "copy_complete",
            "target_provenance",
            "source_slot",
            "copy_snapshot",
            "run",
            "canonical_path",
            "directory_device",
        ] {
            let tmp = TempDir::new();
            drop(tmp.create());
            change_manifest(&tmp.path(), |value| {
                value.as_object_mut().unwrap().remove(field);
            });
            refusal(&tmp.path(), StateAction::Restart, "state-invalid:");
        }
        let tmp = TempDir::new();
        drop(tmp.create());
        change_manifest(&tmp.path(), |value| {
            value["foreign_extension"] = json!(true)
        });
        refusal(&tmp.path(), StateAction::Restart, "state-invalid:");
    }

    #[test]
    fn oversized_native_metadata_is_rejected_without_mutation() {
        let tmp = TempDir::new();
        drop(tmp.create());
        fs::write(tmp.path().join("manifest.json"), vec![b' '; 16385]).unwrap();
        refusal(&tmp.path(), StateAction::Restart, "state-invalid:");
    }

    #[test]
    fn stale_temporary_metadata_is_not_removed_or_adopted() {
        let tmp = TempDir::new();
        drop(tmp.create());
        fs::write(tmp.path().join(".manifest.tmp"), b"incomplete publication").unwrap();
        for action in [
            StateAction::Create,
            StateAction::Resume,
            StateAction::Restart,
        ] {
            refusal(&tmp.path(), action, "state-foreign:");
        }
    }

    #[test]
    fn manifest_directory_is_rejected_without_traversal() {
        let tmp = TempDir::new();
        DirBuilder::new().mode(0o700).create(tmp.path()).unwrap();
        fs::create_dir(tmp.path().join("manifest.json")).unwrap();
        fs::write(tmp.path().join("manifest.json/source.db"), b"legacy").unwrap();
        refusal(&tmp.path(), StateAction::Restart, "state-unsafe:");
    }

    #[test]
    fn parent_replacement_cannot_redirect_completion() {
        let tmp = TempDir::new();
        let parent = tmp.0.join("parent");
        DirBuilder::new().mode(0o700).create(&parent).unwrap();
        let path = parent.join("state");
        let mut state = open(&request(path.clone(), StateAction::Create)).unwrap();
        fs::rename(&parent, tmp.0.join("moved-parent")).unwrap();
        DirBuilder::new().mode(0o700).create(&parent).unwrap();
        DirBuilder::new().mode(0o700).create(&path).unwrap();
        fs::write(path.join("source.db"), b"legacy").unwrap();
        let before = tree(&tmp.0);
        let error = state.record_copy_complete(&report()).unwrap_err();
        assert!(error.message.starts_with("state-replaced:"), "{error:?}");
        assert_eq!(tree(&tmp.0), before);
    }

    #[test]
    fn same_contents_in_replaced_manifest_do_not_establish_ownership() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let bytes = fs::read(tmp.path().join("manifest.json")).unwrap();
        fs::rename(
            tmp.path().join("manifest.json"),
            tmp.0.join("previous.json"),
        )
        .unwrap();
        fs::write(tmp.path().join("manifest.json"), &bytes).unwrap();
        fs::set_permissions(
            tmp.path().join("manifest.json"),
            Permissions::from_mode(0o600),
        )
        .unwrap();
        let before = tree(&tmp.0);
        let error = state.record_copy_complete(&report()).unwrap_err();
        assert!(error.message.starts_with("state-replaced:"), "{error:?}");
        assert_eq!(tree(&tmp.0), before);
    }

    #[test]
    fn verified_empty_copy_can_publish_a_zero_report() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        state
            .record_copy_complete(&CopyReport {
                tables: 0,
                rows: 0,
                bytes: 0,
            })
            .unwrap();
        assert_eq!(
            manifest(&tmp.path())["copy_complete"],
            json!({"tables":0,"rows":0,"bytes":0})
        );
    }

    #[test]
    fn native_lock_is_exclusive_across_processes() {
        const CHILD_PATH: &str = "RPGMIGRATE_TEST_STATE_LOCK_CHILD";
        if let Some(path) = std::env::var_os(CHILD_PATH) {
            let error = open(&request(path.into(), StateAction::Create)).unwrap_err();
            assert!(error.message.starts_with("state-busy:"), "{error:?}");
            return;
        }
        let tmp = TempDir::new();
        let _state = tmp.create();
        let before = tree(&tmp.path());
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "state::tests::native_lock_is_exclusive_across_processes",
                "--nocapture",
            ])
            .env(CHILD_PATH, tmp.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn dropping_owner_releases_lock_even_with_an_inherited_descriptor() {
        let tmp = TempDir::new();
        let state = tmp.create();
        let inherited = state.directory.handle.try_clone().unwrap();
        drop(state);
        let next_owner = File::open(tmp.path()).unwrap();
        assert!(next_owner.try_lock().is_ok());
        drop(inherited);
    }

    #[test]
    fn dropping_unpublished_owner_releases_inherited_lock() {
        let tmp = TempDir::new();
        DirBuilder::new().mode(0o700).create(tmp.path()).unwrap();
        let handle = File::open(tmp.path()).unwrap();
        handle.try_lock().unwrap();
        let inherited = handle.try_clone().unwrap();
        let directory = Directory {
            parent_path: tmp.0.clone(),
            parent: File::open(&tmp.0).unwrap(),
            name: CString::new("state").unwrap(),
            handle,
        };
        drop(directory);
        let next_owner = File::open(tmp.path()).unwrap();
        assert!(next_owner.try_lock().is_ok());
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 0);
        drop(inherited);
    }

    fn binding(state: &NativeState) -> TargetBinding {
        TargetBinding {
            stream: StreamId {
                run: state.identity().run,
                source: crate::identity::SourceId {
                    system_identifier: 12345,
                    database_oid: 16384,
                },
                generation: 0,
            },
            target_system_identifier: Some(67890),
            target_database_oid: 16385,
            schema_name: "rpgmigrate_owned".into(),
            table_name: "checkpoint".into(),
            owner_oid: 10,
            metadata_version: 3,
        }
    }

    fn confirmed(intent: &ProvisioningIntent) -> ConfirmedProvenance {
        ConfirmedProvenance {
            intent: intent.clone(),
            schema_oid: 20000,
            table_oid: 20001,
        }
    }

    fn recovery_refusal(path: &Path, code: &str) {
        let before = tree(path);
        let error = open_for_provenance_recovery(path).unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadState);
        assert!(error.message.starts_with(code), "{error:?}");
        assert_eq!(tree(path), before);
    }

    #[test]
    fn prior_metadata_versions_are_refused_without_rewrite() {
        for (version, confirming) in [(1, false), (1, true), (2, false), (2, true)] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let mut old_binding = binding(&state);
            old_binding.metadata_version = version;
            let before = tree(&tmp.path());
            let error = state.begin_target_provisioning(old_binding).unwrap_err();
            assert!(error.message.starts_with("state-provenance:"), "{error:?}");
            assert_eq!(tree(&tmp.path()), before);
            let intent = state.begin_target_provisioning(binding(&state)).unwrap();
            if confirming {
                state
                    .confirm_target_provisioning(confirmed(&intent))
                    .unwrap();
            }
            drop(state);
            change_manifest(&tmp.path(), |value| {
                let record = &mut value["target_provenance"][0]["record"];
                let intent = if confirming {
                    &mut record["intent"]
                } else {
                    record
                };
                intent["binding"]["metadata_version"] = json!(version);
            });
            assert_eq!(manifest(&tmp.path())["format_version"], 4);
            recovery_refusal(&tmp.path(), "state-provenance:");
            for action in [
                StateAction::Create,
                StateAction::Resume,
                StateAction::Restart,
            ] {
                refusal(&tmp.path(), action, "state-provenance:");
            }
        }
    }

    #[test]
    fn intent_is_published_before_return_and_can_be_reloaded_with_its_nonce() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let expected = binding(&state);
        let old = fs::read(tmp.path().join("manifest.json")).unwrap();
        let mut old_file = File::open(tmp.path().join("manifest.json")).unwrap();
        let intent = state.begin_target_provisioning(expected.clone()).unwrap();
        assert_eq!(intent.binding, expected);
        assert_ne!(intent.nonce, [0; 16]);
        assert_eq!(
            manifest(&tmp.path())["target_provenance"][0]["phase"],
            "provisioning"
        );
        let mut bytes = Vec::new();
        old_file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, old);
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 2);
        drop(state);
        let before = tree(&tmp.path());
        let recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
        assert_eq!(
            recovered.target_provenance(&expected.stream),
            Some(&TargetProvenance::Provisioning(intent))
        );
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn confirmation_preserves_the_exact_intent_and_reloads_object_oids() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let intent = state.begin_target_provisioning(binding(&state)).unwrap();
        let completed = confirmed(&intent);
        state
            .confirm_target_provisioning(completed.clone())
            .unwrap();
        assert_eq!(
            manifest(&tmp.path())["target_provenance"][0]["phase"],
            "confirmed"
        );
        drop(state);
        let recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
        assert_eq!(
            recovered.target_provenance(&intent.binding.stream),
            Some(&TargetProvenance::Confirmed(completed))
        );
    }

    #[test]
    fn repeated_intent_and_confirmation_are_idempotent_without_new_nonce_or_inode() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = binding(&state);
        let intent = state.begin_target_provisioning(desired.clone()).unwrap();
        let before = tree(&tmp.path());
        assert_eq!(state.begin_target_provisioning(desired).unwrap(), intent);
        assert_eq!(tree(&tmp.path()), before);
        let completed = confirmed(&intent);
        state
            .confirm_target_provisioning(completed.clone())
            .unwrap();
        let before = tree(&tmp.path());
        state.confirm_target_provisioning(completed).unwrap();
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn confirmed_provenance_cannot_return_to_provisioning() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = binding(&state);
        let intent = state.begin_target_provisioning(desired.clone()).unwrap();
        state
            .confirm_target_provisioning(confirmed(&intent))
            .unwrap();
        let before = tree(&tmp.path());
        assert!(state.begin_target_provisioning(desired).is_err());
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn confirmation_requires_a_persisted_matching_intent_and_nonzero_oids() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let synthetic = ProvisioningIntent {
            binding: binding(&state),
            nonce: [1; 16],
        };
        let before = tree(&tmp.path());
        assert!(
            state
                .confirm_target_provisioning(confirmed(&synthetic))
                .is_err()
        );
        assert_eq!(tree(&tmp.path()), before);
        let intent = state.begin_target_provisioning(binding(&state)).unwrap();
        for variant in 0..5 {
            let mut evidence = confirmed(&intent);
            match variant {
                0 => evidence.intent.nonce[0] ^= 1,
                1 => evidence.schema_oid = 0,
                2 => evidence.table_oid = 0,
                3 => evidence.intent.binding.owner_oid += 1,
                _ => evidence.intent.binding.target_system_identifier = None,
            }
            let before = tree(&tmp.path());
            assert!(state.confirm_target_provisioning(evidence).is_err());
            assert_eq!(tree(&tmp.path()), before);
        }
    }

    #[test]
    fn target_binding_cannot_be_rewritten_or_downgraded() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = binding(&state);
        state.begin_target_provisioning(desired.clone()).unwrap();
        for variant in 0..6 {
            let mut changed = desired.clone();
            match variant {
                0 => changed.target_system_identifier = None,
                1 => changed.target_system_identifier = Some(98765),
                2 => changed.target_database_oid += 1,
                3 => changed.owner_oid += 1,
                4 => changed.schema_name.push('x'),
                _ => changed.table_name.push('x'),
            }
            let before = tree(&tmp.path());
            assert!(state.begin_target_provisioning(changed).is_err());
            assert_eq!(tree(&tmp.path()), before);
        }
    }

    #[test]
    fn optional_target_system_id_round_trips_without_becoming_authority() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let mut desired = binding(&state);
        desired.target_system_identifier = None;
        let intent = state.begin_target_provisioning(desired.clone()).unwrap();
        drop(state);
        let mut state = open_for_provenance_recovery(&tmp.path()).unwrap();
        assert_eq!(
            state.target_provenance(&desired.stream),
            Some(&TargetProvenance::Provisioning(intent.clone()))
        );
        let mut upgraded = desired;
        upgraded.target_system_identifier = Some(67890);
        let before = tree(&tmp.path());
        assert!(state.begin_target_provisioning(upgraded).is_err());
        assert_eq!(tree(&tmp.path()), before);
        state
            .confirm_target_provisioning(confirmed(&intent))
            .unwrap();
        assert_eq!(
            manifest(&tmp.path())["target_provenance"][0]["record"]["intent"]["binding"]["target_system_identifier"],
            Value::Null
        );
    }

    #[test]
    fn initial_slice_refuses_a_second_stream_and_duplicate_persisted_streams() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let mut desired = binding(&state);
        state.begin_target_provisioning(desired.clone()).unwrap();
        desired.stream.generation += 1;
        let before = tree(&tmp.path());
        assert!(state.begin_target_provisioning(desired).is_err());
        assert_eq!(tree(&tmp.path()), before);
        drop(state);
        change_manifest(&tmp.path(), |value| {
            let first = value["target_provenance"][0].clone();
            value["target_provenance"]
                .as_array_mut()
                .unwrap()
                .push(first);
        });
        recovery_refusal(&tmp.path(), "state-provenance:");
    }

    #[test]
    fn invalid_local_binding_is_refused_before_any_publication() {
        for variant in 0..10 {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let mut desired = binding(&state);
            match variant {
                0 => desired.stream.run.0[0] ^= 1,
                1 => desired.stream.source.system_identifier = 0,
                2 => desired.stream.source.database_oid = 0,
                3 => desired.target_system_identifier = Some(0),
                4 => desired.target_database_oid = 0,
                5 => desired.owner_oid = 0,
                6 => desired.metadata_version = 1,
                7 => desired.schema_name.clear(),
                8 => desired.table_name.clear(),
                _ => desired.schema_name.push('\0'),
            }
            let before = tree(&tmp.path());
            let error = state.begin_target_provisioning(desired).unwrap_err();
            assert!(error.message.starts_with("state-provenance:"), "{error:?}");
            assert_eq!(tree(&tmp.path()), before);
        }
    }

    #[test]
    fn manifest_limit_is_checked_before_creating_publication_files() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let mut desired = binding(&state);
        desired.schema_name = "a".repeat(16384);
        let before = tree(&tmp.path());
        let error = state.begin_target_provisioning(desired).unwrap_err();
        assert!(error.message.starts_with("state-invalid:"), "{error:?}");
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn recovery_refuses_missing_empty_and_copy_only_roots_without_creation() {
        let tmp = TempDir::new();
        assert!(open_for_provenance_recovery(&tmp.path()).is_err());
        assert!(!tmp.path().exists());
        DirBuilder::new().mode(0o700).create(tmp.path()).unwrap();
        recovery_refusal(&tmp.path(), "state-missing:");
        drop(tmp.create());
        recovery_refusal(&tmp.path(), "state-provenance:");
    }

    #[test]
    fn recovery_refuses_v1_and_unknown_versions_without_conversion() {
        for version in [1, 2, 3, 5] {
            let tmp = TempDir::new();
            drop(tmp.create());
            change_manifest(&tmp.path(), |value| {
                value["format_version"] = json!(version);
                value.as_object_mut().unwrap().remove("target_provenance");
            });
            recovery_refusal(&tmp.path(), "state-version:");
        }
    }

    #[test]
    fn recovered_provenance_does_not_enable_external_copy_resume_or_restart() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        state.begin_target_provisioning(binding(&state)).unwrap();
        drop(state);
        for action in [StateAction::Resume, StateAction::Restart] {
            refusal(&tmp.path(), action, "state-recovery-unsupported:");
        }
        let recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
        assert_eq!(recovered.identity().format_version, 4);
    }

    #[test]
    fn recovery_keeps_path_permissions_and_exclusive_lock_checks() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        state.begin_target_provisioning(binding(&state)).unwrap();
        recovery_refusal(&tmp.path(), "state-busy:");
        drop(state);
        fs::set_permissions(tmp.path(), Permissions::from_mode(0o750)).unwrap();
        recovery_refusal(&tmp.path(), "state-permissions:");
        fs::set_permissions(tmp.path(), Permissions::from_mode(0o700)).unwrap();
        let alias = tmp.0.join("alias");
        symlink(tmp.path(), &alias).unwrap();
        recovery_refusal(&alias, "state-unsafe:");
    }

    #[test]
    fn unpublished_temporary_file_blocks_provenance_without_cleanup() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = binding(&state);
        let intent = state.begin_target_provisioning(desired.clone()).unwrap();
        fs::write(tmp.path().join(".manifest.tmp"), b"partial confirmation").unwrap();
        let before = tree(&tmp.path());
        assert!(
            state
                .confirm_target_provisioning(confirmed(&intent))
                .is_err()
        );
        assert_eq!(
            state.target_provenance(&desired.stream),
            Some(&TargetProvenance::Provisioning(intent))
        );
        assert_eq!(tree(&tmp.path()), before);
        drop(state);
        recovery_refusal(&tmp.path(), "state-foreign:");
    }

    #[test]
    fn replaced_manifest_blocks_intent_and_confirmation() {
        for confirming in [false, true] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let desired = binding(&state);
            let intent = if confirming {
                Some(state.begin_target_provisioning(desired.clone()).unwrap())
            } else {
                None
            };
            let original = tmp.0.join("original.json");
            fs::rename(tmp.path().join("manifest.json"), &original).unwrap();
            fs::copy(&original, tmp.path().join("manifest.json")).unwrap();
            let before = tree(&tmp.0);
            let error = if let Some(intent) = intent {
                state
                    .confirm_target_provisioning(confirmed(&intent))
                    .unwrap_err()
            } else {
                state.begin_target_provisioning(desired).unwrap_err()
            };
            assert!(error.message.starts_with("state-replaced:"), "{error:?}");
            assert_eq!(tree(&tmp.0), before);
        }
    }

    #[test]
    fn publication_failure_before_rename_preserves_the_previous_manifest() {
        for confirming in [false, true] {
            for step in [
                PublicationStep::Create,
                PublicationStep::Write,
                PublicationStep::FileSync,
                PublicationStep::Rename,
            ] {
                let tmp = TempDir::new();
                let mut state = tmp.create();
                let desired = binding(&state);
                let intent =
                    confirming.then(|| state.begin_target_provisioning(desired.clone()).unwrap());
                let previous = state.target_provenance(&desired.stream).cloned();
                let path = tmp.path().join("manifest.json");
                let before = tree(&tmp.path());
                let bytes = fs::read(&path).unwrap();
                let failure = PublicationFailure::at(step);
                let result = if let Some(intent) = &intent {
                    state.confirm_target_provisioning(confirmed(intent))
                } else {
                    state.begin_target_provisioning(desired.clone()).map(|_| ())
                };
                let error = result.expect_err(&format!("{confirming:?}: {step:?}"));
                assert!(error.message.starts_with("state-test:"));
                assert_eq!(state.target_provenance(&desired.stream), previous.as_ref());
                assert_eq!(fs::read(&path).unwrap(), bytes);
                assert_eq!(fs::metadata(&path).unwrap().ino(), before[&path].inode);
                drop(failure);
                drop(state);
                if step == PublicationStep::Create {
                    assert_eq!(tree(&tmp.path()), before);
                    if confirming {
                        let recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
                        assert_eq!(
                            recovered.target_provenance(&desired.stream),
                            previous.as_ref()
                        );
                    } else {
                        recovery_refusal(&tmp.path(), "state-provenance:");
                    }
                } else {
                    assert!(tmp.path().join(".manifest.tmp").is_file());
                    recovery_refusal(&tmp.path(), "state-foreign:");
                }
            }
        }
    }

    #[test]
    fn publication_failure_after_rename_returns_no_success_and_requires_reopen() {
        for confirming in [false, true] {
            for step in [PublicationStep::DirectorySync, PublicationStep::Published] {
                let tmp = TempDir::new();
                let mut state = tmp.create();
                let desired = binding(&state);
                let intent =
                    confirming.then(|| state.begin_target_provisioning(desired.clone()).unwrap());
                let previous = state.target_provenance(&desired.stream).cloned();
                let failure = PublicationFailure::at(step);
                let result = if let Some(intent) = &intent {
                    state.confirm_target_provisioning(confirmed(intent))
                } else {
                    state.begin_target_provisioning(desired.clone()).map(|_| ())
                };
                let error = result.expect_err(&format!("{confirming:?}: {step:?}"));
                assert!(error.message.starts_with("state-test:"));
                assert_eq!(state.target_provenance(&desired.stream), previous.as_ref());
                assert!(!tmp.path().join(".manifest.tmp").exists());
                assert!(manifest(&tmp.path())["copy_complete"].is_null());
                let persisted: TargetProvenance =
                    serde_json::from_value(manifest(&tmp.path())["target_provenance"][0].clone())
                        .unwrap();
                if let Some(intent) = &intent {
                    assert_eq!(persisted, TargetProvenance::Confirmed(confirmed(intent)));
                } else {
                    let TargetProvenance::Provisioning(pending) = &persisted else {
                        panic!("unexpected confirmation");
                    };
                    assert_eq!(pending.binding, desired);
                    assert_ne!(pending.nonce, [0; 16]);
                }
                drop(failure);
                let error = state
                    .begin_target_provisioning(desired.clone())
                    .unwrap_err();
                assert!(error.message.starts_with("state-replaced:"), "{error:?}");
                drop(state);
                let recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
                assert_eq!(
                    recovered.target_provenance(&desired.stream),
                    Some(&persisted)
                );
            }
        }
    }

    #[test]
    fn identical_transition_retry_must_synchronize_before_returning_success() {
        for confirming in [false, true] {
            for step in [PublicationStep::FileSync, PublicationStep::DirectorySync] {
                let tmp = TempDir::new();
                let mut state = tmp.create();
                let desired = binding(&state);
                let intent = state.begin_target_provisioning(desired.clone()).unwrap();
                let completed = confirmed(&intent);
                if confirming {
                    state
                        .confirm_target_provisioning(completed.clone())
                        .unwrap();
                }
                let before = tree(&tmp.path());
                let failure = PublicationFailure::at(step);
                let result = if confirming {
                    state.confirm_target_provisioning(completed.clone())
                } else {
                    state.begin_target_provisioning(desired.clone()).map(|_| ())
                };
                let error = result.expect_err(&format!("{confirming:?}: {step:?}"));
                assert!(error.message.starts_with("state-test:"));
                assert_eq!(tree(&tmp.path()), before);
                drop(failure);
                if confirming {
                    state.confirm_target_provisioning(completed).unwrap();
                } else {
                    assert_eq!(state.begin_target_provisioning(desired).unwrap(), intent);
                }
                assert_eq!(tree(&tmp.path()), before);
            }
        }
    }

    #[test]
    fn recovery_validates_every_persisted_binding_and_nonzero_nonce() {
        let invalid = [
            ("/nonce", json!(vec![0; 16])),
            ("/binding/stream/run", json!(vec![0; 16])),
            ("/binding/stream/source/system_identifier", json!(0)),
            ("/binding/stream/source/database_oid", json!(0)),
            ("/binding/target_system_identifier", json!(0)),
            ("/binding/target_database_oid", json!(0)),
            ("/binding/owner_oid", json!(0)),
            ("/binding/metadata_version", json!(1)),
            ("/binding/schema_name", json!("")),
            ("/binding/table_name", json!("")),
            ("/binding/schema_name", json!("a\0b")),
            ("/binding/table_name", json!("a\0b")),
        ];
        for (pointer, invalid) in invalid {
            for confirming in [false, true] {
                let tmp = TempDir::new();
                let mut state = tmp.create();
                let intent = state.begin_target_provisioning(binding(&state)).unwrap();
                if confirming {
                    state
                        .confirm_target_provisioning(confirmed(&intent))
                        .unwrap();
                }
                drop(state);
                change_manifest(&tmp.path(), |value| {
                    let record = &mut value["target_provenance"][0]["record"];
                    let record = if confirming {
                        &mut record["intent"]
                    } else {
                        record
                    };
                    *record.pointer_mut(pointer).unwrap() = invalid.clone();
                });
                recovery_refusal(&tmp.path(), "state-provenance:");
            }
        }
    }

    #[test]
    fn recovery_validates_confirmed_object_oids_and_rejects_unknown_fields() {
        for field in ["schema_oid", "table_oid"] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = state.begin_target_provisioning(binding(&state)).unwrap();
            state
                .confirm_target_provisioning(confirmed(&intent))
                .unwrap();
            drop(state);
            change_manifest(&tmp.path(), |value| {
                value["target_provenance"][0]["record"][field] = json!(0)
            });
            recovery_refusal(&tmp.path(), "state-provenance:");
        }
        for pointer in [
            "",
            "/record",
            "/record/intent",
            "/record/intent/binding",
            "/record/intent/binding/stream",
            "/record/intent/binding/stream/source",
        ] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = state.begin_target_provisioning(binding(&state)).unwrap();
            state
                .confirm_target_provisioning(confirmed(&intent))
                .unwrap();
            drop(state);
            change_manifest(&tmp.path(), |value| {
                value["target_provenance"][0]
                    .pointer_mut(pointer)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .insert("unknown".into(), json!(1));
            });
            recovery_refusal(&tmp.path(), "state-invalid:");
        }
    }

    #[test]
    fn recovery_refuses_missing_optional_identity_and_duplicate_json_fields() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        state.begin_target_provisioning(binding(&state)).unwrap();
        drop(state);
        let path = tmp.path().join("manifest.json");
        let original = fs::read_to_string(&path).unwrap();
        change_manifest(&tmp.path(), |value| {
            value["target_provenance"][0]["record"]["binding"]
                .as_object_mut()
                .unwrap()
                .remove("target_system_identifier");
        });
        recovery_refusal(&tmp.path(), "state-invalid:");
        fs::write(
            &path,
            original.replace(
                "\"metadata_version\":3",
                "\"metadata_version\":3,\"metadata_version\":3",
            ),
        )
        .unwrap();
        recovery_refusal(&tmp.path(), "state-invalid:");
    }

    fn source_binding(state: &NativeState) -> SourceSlotBinding {
        let mut stream = binding(state).stream;
        stream.generation = 9;
        SourceSlotBinding::new(stream, "pgcopydb".into(), Plugin::Pgoutput, 7).unwrap()
    }

    fn created_slot() -> CreatedSlotData {
        CreatedSlotData {
            consistent_point: SourceEndLsn(0x100000002),
            snapshot_name: "00000003-0000001B-1".into(),
        }
    }

    fn source_receipt(intent: &SourceSlotIntent) -> SourceSlotReceipt {
        SourceSlotReceipt {
            intent: intent.clone(),
            consistent_point: SourceEndLsn(0x100000002),
            snapshot_token: "00000003-0000001B-1".into(),
        }
    }

    fn source_refusal<T: std::fmt::Debug>(result: Result<T>) {
        let error = result.unwrap_err();
        assert_eq!(error.kind, ErrorKind::BadState);
        assert!(error.message.starts_with("state-source:"), "{error:?}");
    }

    #[test]
    fn source_slot_intent_is_published_once_before_return() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = source_binding(&state);
        source_refusal(state.checked_source_binding(&desired));
        let path = tmp.path().join("manifest.json");
        let old = fs::read(&path).unwrap();
        let mut old_file = File::open(&path).unwrap();
        let intent = state.begin_source_slot(&desired).unwrap();
        assert_eq!(intent.binding, desired);
        assert_ne!(intent.nonce, [0; 16]);
        assert_eq!(intent.binding.stream().generation, 9);
        assert_eq!(intent.binding.catalog_epoch(), 7);
        assert_eq!(intent.binding.slot_name(), "pgcopydb");
        assert_eq!(intent.binding.plugin(), Plugin::Pgoutput);
        assert_eq!(
            manifest(&tmp.path())["source_slot"],
            json!({"phase": "provisioning", "record": {
                "binding": {"stream": desired.stream(), "slot_name": "pgcopydb",
                    "plugin": "pgoutput", "catalog_epoch": 7}, "nonce": intent.nonce}})
        );
        let mut bytes = Vec::new();
        old_file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, old);
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 2);
        source_refusal(state.checked_source_binding(&desired));
        let before = tree(&tmp.path());
        source_refusal(state.begin_source_slot(&desired));
        assert_eq!(tree(&tmp.path()), before);
        drop(state);
        let mut recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
        source_refusal(recovered.checked_source_binding(&desired));
        let mut changed = desired.clone();
        changed.slot_name = "another_absent_slot".into();
        source_refusal(recovered.begin_source_slot(&changed));
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn source_slot_begin_requires_a_fresh_handle_without_copy_completion() {
        for reopened in [false, true] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let desired = source_binding(&state);
            if reopened {
                let mut target = binding(&state);
                target.stream = desired.stream();
                state.begin_target_provisioning(target).unwrap();
                drop(state);
                state = open_for_provenance_recovery(&tmp.path()).unwrap();
            } else {
                state.record_copy_complete(&report()).unwrap();
            }
            let before = tree(&tmp.path());
            source_refusal(state.begin_source_slot(&desired));
            assert_eq!(tree(&tmp.path()), before);
            assert_eq!(manifest(&tmp.path()).get("source_slot"), Some(&Value::Null));
        }
    }

    #[test]
    fn source_slot_confirmation_requires_exact_pending_intent() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = source_binding(&state);
        let synthetic = SourceSlotIntent {
            binding: desired.clone(),
            nonce: [1; 16],
        };
        let before = tree(&tmp.path());
        source_refusal(state.confirm_source_slot(&synthetic, created_slot()));
        assert_eq!(tree(&tmp.path()), before);
        let intent = state.begin_source_slot(&desired).unwrap();
        for variant in 0..8 {
            let mut changed = intent.clone();
            match variant {
                0 => changed.nonce[0] ^= 1,
                1 => changed.nonce = [0; 16],
                2 => changed.binding.stream.run.0[0] ^= 1,
                3 => changed.binding.stream.source.system_identifier += 1,
                4 => changed.binding.stream.generation += 1,
                5 => changed.binding.catalog_epoch += 1,
                6 => changed.binding.plugin = Plugin::Wal2Json,
                _ => changed.binding.slot_name.push('x'),
            }
            let before = tree(&tmp.path());
            source_refusal(state.confirm_source_slot(&changed, created_slot()));
            assert_eq!(tree(&tmp.path()), before);
        }
        source_refusal(state.checked_source_binding(&desired));
    }

    #[test]
    fn source_slot_confirmed_receipt_is_exact_immutable_and_reopenable() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = source_binding(&state);
        let intent = state.begin_source_slot(&desired).unwrap();
        let receipt = state.confirm_source_slot(&intent, created_slot()).unwrap();
        assert_eq!(receipt, source_receipt(&intent));
        assert_eq!(
            manifest(&tmp.path())["source_slot"],
            json!({"phase": "confirmed", "record": {
                "intent": intent, "consistent_point": 0x100000002_u64,
                "snapshot_token": "00000003-0000001B-1"}})
        );
        state.checked_source_binding(&desired).unwrap();
        let before = tree(&tmp.path());
        assert_eq!(
            state.confirm_source_slot(&intent, created_slot()).unwrap(),
            receipt
        );
        assert_eq!(tree(&tmp.path()), before);
        for changed in [
            CreatedSlotData {
                consistent_point: SourceEndLsn(0x100000003),
                ..created_slot()
            },
            CreatedSlotData {
                snapshot_name: "another-token".into(),
                ..created_slot()
            },
        ] {
            source_refusal(state.confirm_source_slot(&intent, changed));
            assert_eq!(tree(&tmp.path()), before);
        }
        source_refusal(state.begin_source_slot(&desired));
        drop(state);
        let mut recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
        recovered.checked_source_binding(&desired).unwrap();
        source_refusal(recovered.begin_source_slot(&desired));
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn source_slot_checked_binding_rejects_every_changed_identity() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = source_binding(&state);
        let intent = state.begin_source_slot(&desired).unwrap();
        state.confirm_source_slot(&intent, created_slot()).unwrap();
        state.checked_source_binding(&desired).unwrap();
        let before = tree(&tmp.path());
        for variant in 0..7 {
            let mut changed = desired.clone();
            match variant {
                0 => changed.stream.run.0[0] ^= 1,
                1 => changed.stream.source.system_identifier += 1,
                2 => changed.stream.source.database_oid += 1,
                3 => changed.stream.generation += 1,
                4 => changed.catalog_epoch += 1,
                5 => changed.plugin = Plugin::TestDecoding,
                _ => changed.slot_name.push('x'),
            }
            source_refusal(state.checked_source_binding(&changed));
            assert_eq!(tree(&tmp.path()), before);
        }
    }

    #[test]
    fn source_slot_binding_rejects_invalid_identities_epoch_and_names() {
        let tmp = TempDir::new();
        let state = tmp.create();
        let valid = source_binding(&state);
        for variant in 0..12 {
            let mut stream = valid.stream();
            let mut epoch = 7;
            let mut slot = "pgcopydb".to_owned();
            match variant {
                0 => stream.run = RunId([0; 16]),
                1 => stream.source.system_identifier = 0,
                2 => stream.source.database_oid = 0,
                3 => epoch = 0,
                4 => slot.clear(),
                5 => slot = "a".repeat(64),
                6 => slot = "Upper".into(),
                7 => slot = "a-b".into(),
                8 => slot = "a.b".into(),
                9 => slot = "a\0b".into(),
                10 => slot = "é".into(),
                _ => slot = "a b".into(),
            }
            source_refusal(SourceSlotBinding::new(
                stream,
                slot,
                Plugin::Pgoutput,
                epoch,
            ));
        }
        let mut zero_generation = valid.stream();
        zero_generation.generation = 0;
        SourceSlotBinding::new(zero_generation, "0_a".into(), Plugin::Pgoutput, 7).unwrap();
    }

    #[test]
    fn source_slot_wrong_run_is_refused_before_publication() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let mut desired = source_binding(&state);
        desired.stream.run.0[0] ^= 1;
        let before = tree(&tmp.path());
        source_refusal(state.begin_source_slot(&desired));
        assert_eq!(tree(&tmp.path()), before);
        state.begin_source_slot(&source_binding(&state)).unwrap();
    }

    #[test]
    fn source_slot_creation_data_rejects_zero_point_and_invalid_token() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = source_binding(&state);
        let intent = state.begin_source_slot(&desired).unwrap();
        for invalid in [
            CreatedSlotData {
                consistent_point: SourceEndLsn(0),
                ..created_slot()
            },
            CreatedSlotData {
                snapshot_name: String::new(),
                ..created_slot()
            },
            CreatedSlotData {
                snapshot_name: "a\0b".into(),
                ..created_slot()
            },
            CreatedSlotData {
                snapshot_name: "a".repeat(1025),
                ..created_slot()
            },
            CreatedSlotData {
                snapshot_name: "é".repeat(513),
                ..created_slot()
            },
        ] {
            let before = tree(&tmp.path());
            source_refusal(state.confirm_source_slot(&intent, invalid));
            assert_eq!(tree(&tmp.path()), before);
            source_refusal(state.checked_source_binding(&desired));
        }
    }

    #[test]
    fn source_slot_bounds_and_canonical_plugins_round_trip() {
        for (plugin, spelling, point, token) in [
            (Plugin::Pgoutput, "pgoutput", 1, "a".into()),
            (
                Plugin::TestDecoding,
                "test_decoding",
                u64::MAX,
                "é".repeat(512),
            ),
            (Plugin::Wal2Json, "wal2json", 17, "x".repeat(1024)),
        ] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let desired =
                SourceSlotBinding::new(source_binding(&state).stream(), "a".repeat(63), plugin, 7)
                    .unwrap();
            let intent = state.begin_source_slot(&desired).unwrap();
            let receipt = state
                .confirm_source_slot(
                    &intent,
                    CreatedSlotData {
                        consistent_point: SourceEndLsn(point),
                        snapshot_name: token.clone(),
                    },
                )
                .unwrap();
            assert_eq!(receipt.consistent_point, SourceEndLsn(point));
            assert_eq!(receipt.snapshot_token, token);
            assert_eq!(
                manifest(&tmp.path())["source_slot"]["record"]["intent"]["binding"]["plugin"],
                spelling
            );
            assert_eq!(manifest(&tmp.path())["copy_complete"], Value::Null);
            drop(state);
            open_for_provenance_recovery(&tmp.path())
                .unwrap()
                .checked_source_binding(&desired)
                .unwrap();
        }
    }

    #[test]
    fn source_slot_and_target_provenance_require_the_same_stream_in_either_order() {
        for source_first in [false, true] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let desired = source_binding(&state);
            let mut target = binding(&state);
            target.stream = desired.stream();
            target.stream.generation += 1;
            if source_first {
                state.begin_source_slot(&desired).unwrap();
                let before = tree(&tmp.path());
                source_refusal(state.begin_target_provisioning(target));
                assert_eq!(tree(&tmp.path()), before);
            } else {
                state.begin_target_provisioning(target).unwrap();
                let before = tree(&tmp.path());
                source_refusal(state.begin_source_slot(&desired));
                assert_eq!(tree(&tmp.path()), before);
            }
        }
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = source_binding(&state);
        let intent = state.begin_source_slot(&desired).unwrap();
        state.confirm_source_slot(&intent, created_slot()).unwrap();
        let mut target = binding(&state);
        target.stream = desired.stream();
        let target = state.begin_target_provisioning(target).unwrap();
        state
            .confirm_target_provisioning(confirmed(&target))
            .unwrap();
        state.checked_source_binding(&desired).unwrap();
        assert_eq!(
            manifest(&tmp.path())["target_provenance"][0]["record"]["intent"]["binding"]["metadata_version"],
            3
        );
    }

    #[test]
    fn source_slot_publication_failures_never_return_authorization_or_replace_local_record() {
        for confirming in [false, true] {
            for step in [
                PublicationStep::Create,
                PublicationStep::Write,
                PublicationStep::FileSync,
                PublicationStep::Rename,
                PublicationStep::DirectorySync,
                PublicationStep::Published,
            ] {
                let tmp = TempDir::new();
                let mut state = tmp.create();
                let desired = source_binding(&state);
                let intent = confirming.then(|| state.begin_source_slot(&desired).unwrap());
                let previous = state.manifest.source_slot.clone();
                let path = tmp.path().join("manifest.json");
                let before = fs::read(&path).unwrap();
                let inode = fs::metadata(&path).unwrap().ino();
                let failure = PublicationFailure::at(step);
                let result = if let Some(intent) = &intent {
                    state
                        .confirm_source_slot(intent, created_slot())
                        .map(|_| ())
                } else {
                    state.begin_source_slot(&desired).map(|_| ())
                };
                let error = result.expect_err(&format!("{confirming:?}: {step:?}"));
                assert!(error.message.starts_with("state-test:"), "{error:?}");
                assert_eq!(state.manifest.source_slot, previous);
                assert!(state.checked_source_binding(&desired).is_err());
                drop(failure);
                assert!(state.begin_source_slot(&desired).is_err());
                if matches!(
                    step,
                    PublicationStep::DirectorySync | PublicationStep::Published
                ) {
                    assert!(!tmp.path().join(".manifest.tmp").exists());
                    assert_ne!(fs::metadata(&path).unwrap().ino(), inode);
                    let expected: SourceSlotProvenance =
                        serde_json::from_value(manifest(&tmp.path())["source_slot"].clone())
                            .unwrap();
                    if let Some(intent) = intent {
                        assert_eq!(
                            expected,
                            SourceSlotProvenance::Confirmed(source_receipt(&intent))
                        );
                    } else {
                        let SourceSlotProvenance::Provisioning(intent) = expected else {
                            panic!("unexpected source confirmation")
                        };
                        assert_eq!(intent.binding, desired);
                        assert_ne!(intent.nonce, [0; 16]);
                    }
                    drop(state);
                    let mut recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
                    assert_eq!(
                        recovered.checked_source_binding(&desired).is_ok(),
                        confirming
                    );
                    source_refusal(recovered.begin_source_slot(&desired));
                } else {
                    assert_eq!(fs::read(&path).unwrap(), before);
                    assert_eq!(fs::metadata(&path).unwrap().ino(), inode);
                    assert_eq!(
                        tmp.path().join(".manifest.tmp").exists(),
                        step != PublicationStep::Create
                    );
                }
            }
        }
    }

    #[test]
    fn source_slot_identical_confirmation_retries_must_synchronize() {
        for step in [PublicationStep::FileSync, PublicationStep::DirectorySync] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let desired = source_binding(&state);
            let intent = state.begin_source_slot(&desired).unwrap();
            state.confirm_source_slot(&intent, created_slot()).unwrap();
            let before = tree(&tmp.path());
            let failure = PublicationFailure::at(step);
            let error = state
                .confirm_source_slot(&intent, created_slot())
                .unwrap_err();
            assert!(error.message.starts_with("state-test:"), "{error:?}");
            assert_eq!(tree(&tmp.path()), before);
            drop(failure);
            assert_eq!(
                state.confirm_source_slot(&intent, created_slot()).unwrap(),
                source_receipt(&intent)
            );
            assert_eq!(tree(&tmp.path()), before);
        }
    }

    #[test]
    fn source_slot_manifest_bound_is_checked_before_publication_files() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = source_binding(&state);
        let mut target = binding(&state);
        target.stream = desired.stream();
        target.schema_name = "a".repeat(13000);
        state.begin_target_provisioning(target).unwrap();
        let intent = state.begin_source_slot(&desired).unwrap();
        let before = tree(&tmp.path());
        let error = state
            .confirm_source_slot(
                &intent,
                CreatedSlotData {
                    snapshot_name: "\u{1}".repeat(1024),
                    ..created_slot()
                },
            )
            .unwrap_err();
        assert!(error.message.starts_with("state-invalid:"), "{error:?}");
        assert_eq!(tree(&tmp.path()), before);
        assert!(
            fs::metadata(tmp.path().join("manifest.json"))
                .unwrap()
                .len()
                <= 16384
        );
        source_refusal(state.checked_source_binding(&desired));
    }

    #[test]
    fn source_slot_checked_binding_rechecks_the_held_manifest() {
        for (variant, code) in [
            (0, "state-replaced:"),
            (1, "state-replaced:"),
            (2, "state-replaced:"),
            (3, "state-invalid:"),
            (4, "state-version:"),
            (5, "state-replaced:"),
            (6, "state-permissions:"),
            (7, "state-source:"),
        ] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let desired = source_binding(&state);
            let intent = state.begin_source_slot(&desired).unwrap();
            state.confirm_source_slot(&intent, created_slot()).unwrap();
            state.checked_source_binding(&desired).unwrap();
            let path = tmp.path().join("manifest.json");
            match variant {
                0 => {
                    let original = tmp.0.join("original.json");
                    fs::rename(&path, &original).unwrap();
                    fs::copy(&original, &path).unwrap();
                }
                1 => change_manifest(&tmp.path(), |value| value["source_slot"] = Value::Null),
                2 => change_manifest(&tmp.path(), |value| {
                    value["source_slot"]["record"]["intent"]["binding"]["catalog_epoch"] = json!(8);
                }),
                3 => change_manifest(&tmp.path(), |value| {
                    value.as_object_mut().unwrap().remove("source_slot");
                }),
                4 => change_manifest(&tmp.path(), |value| value["format_version"] = json!(2)),
                5 => fs::remove_file(&path).unwrap(),
                6 => fs::set_permissions(&path, Permissions::from_mode(0o644)).unwrap(),
                _ => state.identity.format_version = 2,
            }
            let before = tree(&tmp.0);
            let error = state.checked_source_binding(&desired).unwrap_err();
            assert_eq!(error.kind, ErrorKind::BadState);
            assert!(error.message.starts_with(code), "{variant}: {error:?}");
            assert_eq!(tree(&tmp.0), before);
        }
    }

    #[test]
    fn source_slot_recovery_validates_every_persisted_intent() {
        for confirming in [false, true] {
            for (pointer, invalid, code) in [
                ("/nonce", json!(vec![0; 16]), "state-source:"),
                ("/binding/stream/run", json!(vec![0; 16]), "state-source:"),
                ("/binding/stream/run", json!(vec![7; 16]), "state-source:"),
                (
                    "/binding/stream/source/system_identifier",
                    json!(0),
                    "state-source:",
                ),
                (
                    "/binding/stream/source/database_oid",
                    json!(0),
                    "state-source:",
                ),
                ("/binding/catalog_epoch", json!(0), "state-source:"),
                ("/binding/slot_name", json!(""), "state-source:"),
                ("/binding/slot_name", json!("a".repeat(64)), "state-source:"),
                ("/binding/slot_name", json!("a\0b"), "state-source:"),
                ("/binding/slot_name", json!("UPPER"), "state-source:"),
                ("/binding/plugin", json!("unknown_plugin"), "state-invalid:"),
            ] {
                let tmp = TempDir::new();
                let mut state = tmp.create();
                let intent = state.begin_source_slot(&source_binding(&state)).unwrap();
                if confirming {
                    state.confirm_source_slot(&intent, created_slot()).unwrap();
                }
                drop(state);
                change_manifest(&tmp.path(), |value| {
                    let record = &mut value["source_slot"]["record"];
                    let intent = if confirming {
                        &mut record["intent"]
                    } else {
                        record
                    };
                    *intent.pointer_mut(pointer).unwrap() = invalid.clone();
                });
                recovery_refusal(&tmp.path(), code);
            }
        }
    }

    #[test]
    fn source_slot_recovery_validates_receipt_and_required_fields() {
        for (pointer, value, code) in [
            ("/record/consistent_point", json!(0), "state-source:"),
            ("/record/snapshot_token", json!(""), "state-source:"),
            ("/record/snapshot_token", json!("a\0b"), "state-source:"),
            (
                "/record/snapshot_token",
                json!("x".repeat(1025)),
                "state-source:",
            ),
            ("/record/snapshot_token", Value::Null, "state-invalid:"),
            ("/phase", json!("copy_anchor"), "state-invalid:"),
        ] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = state.begin_source_slot(&source_binding(&state)).unwrap();
            state.confirm_source_slot(&intent, created_slot()).unwrap();
            drop(state);
            change_manifest(&tmp.path(), |manifest| {
                *manifest["source_slot"].pointer_mut(pointer).unwrap() = value.clone();
            });
            recovery_refusal(&tmp.path(), code);
        }
        for (pointer, field) in [
            ("", "phase"),
            ("", "record"),
            ("/record", "intent"),
            ("/record", "consistent_point"),
            ("/record", "snapshot_token"),
            ("/record/intent", "nonce"),
            ("/record/intent", "binding"),
            ("/record/intent/binding", "stream"),
            ("/record/intent/binding", "slot_name"),
            ("/record/intent/binding", "plugin"),
            ("/record/intent/binding", "catalog_epoch"),
        ] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = state.begin_source_slot(&source_binding(&state)).unwrap();
            state.confirm_source_slot(&intent, created_slot()).unwrap();
            drop(state);
            change_manifest(&tmp.path(), |value| {
                assert!(
                    value["source_slot"]
                        .pointer_mut(pointer)
                        .unwrap()
                        .as_object_mut()
                        .unwrap()
                        .remove(field)
                        .is_some()
                );
            });
            recovery_refusal(&tmp.path(), "state-invalid:");
        }
    }

    #[test]
    fn source_slot_recovery_refuses_unknown_duplicate_and_non_utf8_fields() {
        for pointer in [
            "",
            "/record",
            "/record/intent",
            "/record/intent/binding",
            "/record/intent/binding/stream",
            "/record/intent/binding/stream/source",
        ] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = state.begin_source_slot(&source_binding(&state)).unwrap();
            state.confirm_source_slot(&intent, created_slot()).unwrap();
            drop(state);
            change_manifest(&tmp.path(), |value| {
                value["source_slot"]
                    .pointer_mut(pointer)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .insert("unknown".into(), json!(1));
            });
            recovery_refusal(&tmp.path(), "state-invalid:");
        }
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let intent = state.begin_source_slot(&source_binding(&state)).unwrap();
        state.confirm_source_slot(&intent, created_slot()).unwrap();
        drop(state);
        let path = tmp.path().join("manifest.json");
        let original = fs::read_to_string(&path).unwrap();
        let needle = "\"catalog_epoch\":7";
        assert_eq!(original.matches(needle).count(), 1);
        fs::write(
            &path,
            original.replace(needle, "\"catalog_epoch\":7,\"catalog_epoch\":7"),
        )
        .unwrap();
        recovery_refusal(&tmp.path(), "state-invalid:");
        let mut bytes = original.into_bytes();
        let start = bytes
            .windows(b"00000003-0000001B-1".len())
            .position(|window| window == b"00000003-0000001B-1")
            .unwrap();
        bytes[start] = 0xff;
        fs::write(path, bytes).unwrap();
        recovery_refusal(&tmp.path(), "state-invalid:");
    }

    #[test]
    fn source_slot_deleted_intent_cannot_make_a_reopened_handle_fresh() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let desired = source_binding(&state);
        let intent = state.begin_source_slot(&desired).unwrap();
        let mut target = binding(&state);
        target.stream = desired.stream();
        state.begin_target_provisioning(target).unwrap();
        drop(state);
        change_manifest(&tmp.path(), |value| {
            assert!(
                value
                    .as_object_mut()
                    .unwrap()
                    .remove("source_slot")
                    .is_some()
            );
        });
        recovery_refusal(&tmp.path(), "state-invalid:");
        for action in [
            StateAction::Create,
            StateAction::Resume,
            StateAction::Restart,
        ] {
            refusal(&tmp.path(), action, "state-invalid:");
        }
        change_manifest(&tmp.path(), |value| value["source_slot"] = Value::Null);
        let mut recovered = open_for_provenance_recovery(&tmp.path()).unwrap();
        let before = tree(&tmp.path());
        source_refusal(recovered.checked_source_binding(&desired));
        source_refusal(recovered.begin_source_slot(&desired));
        source_refusal(recovered.confirm_source_slot(&intent, created_slot()));
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn source_slot_old_versions_and_e1_reuse_remain_refused_without_conversion() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let intent = state.begin_source_slot(&source_binding(&state)).unwrap();
        state.confirm_source_slot(&intent, created_slot()).unwrap();
        drop(state);
        refusal(&tmp.path(), StateAction::Create, "state-exists:");
        for action in [StateAction::Resume, StateAction::Restart] {
            refusal(&tmp.path(), action, "state-recovery-unsupported:");
        }
        for version in [1, 2, 3, 5, u32::MAX] {
            change_manifest(&tmp.path(), |value| {
                value["format_version"] = json!(version);
                value.as_object_mut().unwrap().remove("source_slot");
            });
            recovery_refusal(&tmp.path(), "state-version:");
            for action in [
                StateAction::Create,
                StateAction::Resume,
                StateAction::Restart,
            ] {
                refusal(&tmp.path(), action, "state-version:");
            }
        }
    }

    fn copy_snapshot_intent(state: &mut NativeState) -> CopySnapshotIntent {
        let source = source_binding(state);
        let pending = state.begin_source_slot(&source).unwrap();
        let receipt = state.confirm_source_slot(&pending, created_slot()).unwrap();
        let mut target = binding(state);
        target.stream = source.stream();
        let pending = state.begin_target_provisioning(target).unwrap();
        state
            .confirm_target_provisioning(confirmed(&pending))
            .unwrap();
        state.create_plan_file([42; 16]).unwrap().publish().unwrap();
        CopySnapshotIntent {
            anchor: SnapshotAnchor {
                stream: source.stream(),
                snapshot_token: receipt.snapshot_token,
                consistent_point: receipt.consistent_point,
                plan_nonce: [42; 16],
            },
            plan: CopyPlanRef {
                reference_version: 1,
                plan_nonce: [42; 16],
                table_count: 0,
                logical_bytes: 0,
                crc32: 0,
            },
        }
    }

    #[test]
    fn copy_count_create_failure_preserves_intent_and_clears_guard() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let intent = copy_snapshot_intent(&mut state);
        state.bind_copy_snapshot(&intent).unwrap();
        let report = CopyReport {
            tables: 0,
            rows: 0,
            bytes: 0,
        };
        let before = tree(&tmp.path());
        let error = state
            .record_copy_complete_with_create_failure(&report)
            .unwrap_err();
        assert_eq!(error.message, "state-test: injected publication failure");
        assert_eq!(tree(&tmp.path()), before);
        assert!(state.checked_manifest().unwrap().copy_complete.is_none());
        assert_eq!(state.checked_copy_snapshot().unwrap(), Some(intent.clone()));
        assert_eq!(FAIL_AT.with(Cell::get), None);
        drop(state);

        let mut state = open_for_provenance_recovery(&tmp.path()).unwrap();
        assert_eq!(state.checked_copy_snapshot().unwrap(), Some(intent));
        assert_eq!(tree(&tmp.path()), before);
        state.record_copy_complete(&report).unwrap();
        assert!(state.checked_manifest().unwrap().copy_complete.is_some());
        let error = state
            .record_copy_complete_with_create_failure(&report)
            .unwrap_err();
        assert!(error.message.starts_with("state-complete:"));
        assert_eq!(FAIL_AT.with(Cell::get), None);
    }

    #[test]
    fn copy_snapshot_intent_is_exact_immutable_and_reopens() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        assert_eq!(state.checked_copy_snapshot().unwrap(), None);
        let intent = copy_snapshot_intent(&mut state);
        state.bind_copy_snapshot(&intent).unwrap();
        assert_eq!(state.checked_copy_snapshot().unwrap(), Some(intent.clone()));
        assert_eq!(manifest(&tmp.path())["copy_snapshot"], json!(intent));
        let before = tree(&tmp.path());
        state.bind_copy_snapshot(&intent).unwrap();
        assert_eq!(tree(&tmp.path()), before);
        for variant in 0..5 {
            let mut changed = intent.clone();
            match variant {
                0 => changed.plan.crc32 = 1,
                1 => changed.plan.table_count = 1,
                2 => changed.plan.logical_bytes = 1,
                3 => {
                    changed.plan.plan_nonce = [43; 16];
                    changed.anchor.plan_nonce = [43; 16];
                }
                _ => changed.anchor.snapshot_token.push('x'),
            }
            assert!(state.bind_copy_snapshot(&changed).is_err());
            assert_eq!(tree(&tmp.path()), before);
        }
        drop(state);
        let state = open_for_provenance_recovery(&tmp.path()).unwrap();
        assert_eq!(state.checked_copy_snapshot().unwrap(), Some(intent));
        assert_eq!(tree(&tmp.path()), before);
    }

    #[test]
    fn copy_snapshot_rejects_invalid_identity_version_nonce_and_zero_point_before_publication() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let intent = copy_snapshot_intent(&mut state);
        for variant in 0..10 {
            let mut invalid = intent.clone();
            match variant {
                0 => invalid.anchor.consistent_point = SourceEndLsn(0),
                1 => invalid.anchor.stream.run.0[0] ^= 1,
                2 => invalid.anchor.stream.source.system_identifier += 1,
                3 => invalid.anchor.stream.generation += 1,
                4 => invalid.anchor.snapshot_token.push('x'),
                5 => invalid.anchor.plan_nonce = [0; 16],
                6 => invalid.plan.plan_nonce = [0; 16],
                7 => invalid.plan.plan_nonce = [41; 16],
                8 => invalid.plan.reference_version = 0,
                _ => invalid.plan.reference_version = 2,
            }
            let before = tree(&tmp.path());
            let error = state.bind_copy_snapshot(&invalid).unwrap_err();
            assert!(
                error.message.starts_with("state-copy:"),
                "{variant}: {error:?}"
            );
            assert_eq!(state.checked_copy_snapshot().unwrap(), None);
            assert_eq!(
                manifest(&tmp.path()).get("copy_snapshot"),
                Some(&Value::Null)
            );
            assert!(manifest(&tmp.path())["copy_complete"].is_null());
            assert_eq!(tree(&tmp.path()), before);
        }
        state.bind_copy_snapshot(&intent).unwrap();
    }

    #[test]
    fn copy_snapshot_requires_confirmed_source_and_target_and_existing_plan() {
        for variant in 0..5 {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = copy_snapshot_intent(&mut state);
            match variant {
                0 => state.manifest.source_slot = None,
                1 => {
                    let Some(SourceSlotProvenance::Confirmed(receipt)) =
                        &state.manifest.source_slot
                    else {
                        panic!("missing source")
                    };
                    state.manifest.source_slot =
                        Some(SourceSlotProvenance::Provisioning(receipt.intent.clone()));
                }
                2 => state.manifest.target_provenance.clear(),
                3 => {
                    let TargetProvenance::Confirmed(receipt) = &state.manifest.target_provenance[0]
                    else {
                        panic!("missing target")
                    };
                    state.manifest.target_provenance[0] =
                        TargetProvenance::Provisioning(receipt.intent.clone());
                }
                _ => {
                    fs::remove_dir_all(tmp.path().join("data/plans").join("2a".repeat(16))).unwrap()
                }
            }
            // Publish the controlled prerequisite change so held-file checks do not mask provenance refusal.
            let bytes = serde_json::to_vec(&state.manifest).unwrap();
            fs::write(tmp.path().join("manifest.json"), bytes).unwrap();
            let before = tree(&tmp.path());
            let error = state.bind_copy_snapshot(&intent).unwrap_err();
            let code = if variant == 4 {
                "state-plan:"
            } else {
                "state-copy:"
            };
            assert!(error.message.starts_with(code), "{variant}: {error:?}");
            assert_eq!(tree(&tmp.path()), before);
        }
    }

    #[test]
    fn copy_snapshot_preserves_u64_reference_fields_and_zero_crc_without_sqlite_narrowing() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let mut intent = copy_snapshot_intent(&mut state);
        intent.plan.table_count = u64::MAX;
        intent.plan.logical_bytes = u64::MAX;
        state.bind_copy_snapshot(&intent).unwrap();
        assert_eq!(state.checked_copy_snapshot().unwrap(), Some(intent));
    }

    #[test]
    fn copy_snapshot_required_nullable_field_and_unknown_fields_refuse_corruption() {
        let tmp = TempDir::new();
        let state = tmp.create();
        assert_eq!(state.checked_copy_snapshot().unwrap(), None);
        drop(state);
        change_manifest(&tmp.path(), |value| {
            assert!(
                value
                    .as_object_mut()
                    .unwrap()
                    .remove("copy_snapshot")
                    .is_some()
            );
        });
        recovery_refusal(&tmp.path(), "state-invalid:");
        for action in [
            StateAction::Create,
            StateAction::Resume,
            StateAction::Restart,
        ] {
            refusal(&tmp.path(), action, "state-invalid:");
        }
        for pointer in ["", "/anchor", "/plan"] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = copy_snapshot_intent(&mut state);
            state.bind_copy_snapshot(&intent).unwrap();
            drop(state);
            change_manifest(&tmp.path(), |value| {
                value["copy_snapshot"]
                    .pointer_mut(pointer)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .insert("unknown".into(), json!(1));
            });
            recovery_refusal(&tmp.path(), "state-invalid:");
        }
    }

    #[test]
    fn copy_snapshot_publication_and_identical_resync_failures_return_no_success() {
        for repeating in [false, true] {
            for step in [
                PublicationStep::Create,
                PublicationStep::Write,
                PublicationStep::FileSync,
                PublicationStep::Rename,
                PublicationStep::DirectorySync,
                PublicationStep::Published,
            ] {
                if repeating
                    && !matches!(
                        step,
                        PublicationStep::FileSync | PublicationStep::DirectorySync
                    )
                {
                    continue;
                }
                let tmp = TempDir::new();
                let mut state = tmp.create();
                let intent = copy_snapshot_intent(&mut state);
                if repeating {
                    state.bind_copy_snapshot(&intent).unwrap();
                }
                let previous = state.manifest.copy_snapshot.clone();
                let failure = PublicationFailure::at(step);
                let error = state.bind_copy_snapshot(&intent).unwrap_err();
                assert!(
                    error.message.starts_with("state-test:"),
                    "{repeating} {step:?}: {error:?}"
                );
                assert_eq!(state.manifest.copy_snapshot, previous);
                drop(failure);
            }
        }
    }

    #[test]
    fn copy_snapshot_checked_lookup_refuses_changed_control_or_missing_plan() {
        for (variant, code) in [
            (0, "state-replaced:"),
            (1, "state-invalid:"),
            (2, "state-version:"),
            (3, "state-plan:"),
        ] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = copy_snapshot_intent(&mut state);
            state.bind_copy_snapshot(&intent).unwrap();
            assert_eq!(state.checked_copy_snapshot().unwrap(), Some(intent));
            match variant {
                0 => change_manifest(&tmp.path(), |value| value["copy_snapshot"] = Value::Null),
                1 => change_manifest(&tmp.path(), |value| {
                    value.as_object_mut().unwrap().remove("copy_snapshot");
                }),
                2 => change_manifest(&tmp.path(), |value| value["format_version"] = json!(3)),
                _ => {
                    fs::remove_dir_all(tmp.path().join("data/plans").join("2a".repeat(16))).unwrap()
                }
            }
            let before = tree(&tmp.path());
            let error = state.checked_copy_snapshot().unwrap_err();
            assert!(error.message.starts_with(code), "{variant}: {error:?}");
            assert_eq!(tree(&tmp.path()), before);
        }
    }

    #[test]
    fn copy_snapshot_recovery_revalidates_nonzero_point_and_exact_reference() {
        for (pointer, invalid) in [
            ("/anchor/consistent_point", json!(0)),
            ("/anchor/snapshot_token", json!("different")),
            ("/plan/reference_version", json!(2)),
            ("/plan/plan_nonce", json!(vec![0; 16])),
            ("/anchor/plan_nonce", json!(vec![41; 16])),
        ] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let intent = copy_snapshot_intent(&mut state);
            state.bind_copy_snapshot(&intent).unwrap();
            drop(state);
            change_manifest(&tmp.path(), |value| {
                *value["copy_snapshot"].pointer_mut(pointer).unwrap() = invalid.clone();
            });
            recovery_refusal(&tmp.path(), "state-copy:");
        }
    }

    #[test]
    fn copy_snapshot_keeps_the_manifest_bound_before_creating_publication_files() {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let source = source_binding(&state);
        let pending = state.begin_source_slot(&source).unwrap();
        let receipt = state
            .confirm_source_slot(
                &pending,
                CreatedSlotData {
                    snapshot_name: "\u{1}".repeat(1024),
                    ..created_slot()
                },
            )
            .unwrap();
        let mut target = binding(&state);
        target.stream = source.stream();
        target.schema_name = "a".repeat(6000);
        let pending = state.begin_target_provisioning(target).unwrap();
        state
            .confirm_target_provisioning(confirmed(&pending))
            .unwrap();
        state.create_plan_file([42; 16]).unwrap().publish().unwrap();
        let intent = CopySnapshotIntent {
            anchor: SnapshotAnchor {
                stream: source.stream(),
                snapshot_token: receipt.snapshot_token,
                consistent_point: receipt.consistent_point,
                plan_nonce: [42; 16],
            },
            plan: CopyPlanRef {
                reference_version: 1,
                plan_nonce: [42; 16],
                table_count: 0,
                logical_bytes: 0,
                crc32: 0,
            },
        };
        let before = tree(&tmp.path());
        let error = state.bind_copy_snapshot(&intent).unwrap_err();
        assert!(error.message.starts_with("state-invalid:"), "{error:?}");
        assert_eq!(state.checked_copy_snapshot().unwrap(), None);
        assert_eq!(tree(&tmp.path()), before);
    }
}
