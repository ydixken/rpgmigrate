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

use crate::contracts::{
    CopyReport, Error, ErrorKind, Result, RunId, StateAction, StateIdentity, StateRequest,
};
use crate::identity::StreamId;

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
const FORMAT_VERSION: u32 = 2;
const MAX_TARGET_PROVENANCE: usize = 1;
pub(crate) const TARGET_METADATA_VERSION: u32 = 2;
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
}

pub(crate) fn open(request: &StateRequest) -> Result<NativeState> {
    open_native(request, false)
}

/// Reload local provenance only; the caller must verify fresh fenced target evidence before target use.
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
    if manifest_present(&directory.handle, false)? {
        let (manifest, manifest_file) = read_manifest(&directory, &path)?;
        if recover_provenance {
            if manifest.target_provenance.is_empty() {
                return Err(bad_state(
                    "state-provenance: native state has no target provisioning intent",
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
    };
    let manifest_file = publish(&directory, &manifest, None)?;
    Ok(NativeState {
        identity: StateIdentity {
            run,
            canonical_path: path,
            format_version: FORMAT_VERSION,
        },
        directory,
        manifest,
        manifest_file,
    })
}

impl NativeState {
    pub(crate) fn identity(&self) -> &StateIdentity {
        &self.identity
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

    fn checked_manifest(&self) -> Result<Manifest> {
        self.directory.check()?;
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

fn manifest_present(directory: &File, allow_temp: bool) -> Result<bool> {
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
    let mut found = false;
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
            b"manifest.json" => found = true,
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
    check_publication(directory, manifest, previous, false)?;
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
    check_publication(directory, manifest, previous, true)?;
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
    manifest: &Manifest,
    previous: Option<(&Manifest, &File)>,
    allow_temp: bool,
) -> Result<()> {
    directory.check()?;
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

    struct PublicationFailure;

    impl PublicationFailure {
        fn at(step: PublicationStep) -> Self {
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

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
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

        fn path(&self) -> PathBuf {
            self.0.join("state")
        }

        fn create(&self) -> NativeState {
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
        assert_eq!(value["format_version"], 2);
        assert_eq!(value["canonical_path"], tmp.path().to_str().unwrap());
        assert_eq!(value["run"].as_array().unwrap().len(), 16);
        assert_ne!(value["run"], json!(vec![0; 16]));
        assert_eq!(value["copy_complete"], Value::Null);
        assert_eq!(value["target_provenance"], json!([]));
        assert_eq!(state.identity().canonical_path, tmp.path());
        assert_eq!(state.identity().format_version, 2);
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
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
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
        change_manifest(&tmp.path(), |value| value["format_version"] = json!(3));
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
        // A forked child can retain this description until exec.
        let inherited = competing.try_clone().unwrap();
        refusal(&tmp.path(), StateAction::Create, "state-busy:");
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 0);
        competing.unlock().unwrap();
        drop(competing);
        let state = tmp.create();
        drop(inherited);
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
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
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
            metadata_version: 2,
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
    fn metadata_version_one_is_refused_without_rewrite() {
        for confirming in [false, true] {
            let tmp = TempDir::new();
            let mut state = tmp.create();
            let mut old_binding = binding(&state);
            old_binding.metadata_version = 1;
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
                intent["binding"]["metadata_version"] = json!(1);
            });
            assert_eq!(manifest(&tmp.path())["format_version"], 2);
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
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
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
        for version in [1, 3] {
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
        assert_eq!(recovered.identity().format_version, 2);
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
                "\"metadata_version\":2",
                "\"metadata_version\":2,\"metadata_version\":2",
            ),
        )
        .unwrap();
        recovery_refusal(&tmp.path(), "state-invalid:");
    }
}
