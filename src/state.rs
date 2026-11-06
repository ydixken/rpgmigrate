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

const MANIFEST: &CStr = c"manifest.json";
const TEMP: &CStr = c".manifest.tmp";
const MAX_MANIFEST_BYTES: u64 = 16 * 1024;
const DIRECTORY_FLAGS: libc::c_int = libc::O_RDONLY | libc::O_DIRECTORY;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct Manifest {
    magic: String,
    format_version: u32,
    run: [u8; 16],
    canonical_path: PathBuf,
    directory_device: u64,
    directory_inode: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    copy_complete: Option<Completion>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
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
        read_manifest(&directory, &path)?;
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
    let mut run = [0u8; 16];
    // SAFETY: run provides exactly 16 writable bytes, below getentropy's 256-byte limit.
    if unsafe { libc::getentropy(run.as_mut_ptr().cast(), run.len()) } != 0 || run == [0; 16] {
        return Err(bad_state(
            "state-random: cannot generate native run identity",
        ));
    }
    let manifest = Manifest {
        magic: "RPGMIGRATE".into(),
        format_version: 1,
        run,
        canonical_path: path.clone(),
        directory_device: meta.dev(),
        directory_inode: meta.ino(),
        copy_complete: None,
    };
    let manifest_file = publish(&directory, &manifest, None)?;
    Ok(NativeState {
        identity: StateIdentity {
            run: RunId(run),
            canonical_path: path,
            format_version: 1,
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

    /// Call only after all terminal COPY results and synchronous target COMMIT responses succeed.
    /// This receipt cannot authorize recovery or reuse of target data.
    pub(crate) fn record_copy_complete(&mut self, report: &CopyReport) -> Result<()> {
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
        if current.copy_complete.is_some() {
            return Err(bad_state(
                "state-complete: COPY completion is already recorded",
            ));
        }
        let mut completed = current;
        completed.copy_complete = Some(Completion {
            tables: report.tables,
            rows: report.rows,
            bytes: report.bytes,
        });
        self.manifest_file = publish(&self.directory, &completed, Some((&self.manifest, &file)))?;
        self.manifest = completed;
        Ok(())
    }
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
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|_| bad_state("state-invalid: native manifest is corrupt or incomplete"))?;
    if manifest.magic != "RPGMIGRATE" {
        return Err(bad_state("state-identity: native state magic is invalid"));
    }
    if manifest.format_version != 1 {
        return Err(bad_state(
            "state-version: native state version is unsupported",
        ));
    }
    let meta = metadata(&directory.handle)?;
    if manifest.run == [0; 16]
        || manifest.canonical_path != path
        || manifest.directory_device != meta.dev()
        || manifest.directory_inode != meta.ino()
    {
        return Err(bad_state(
            "state-identity: native state identity does not match this directory",
        ));
    }
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
    let mut temporary = open_at(
        &directory.handle,
        TEMP,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o600,
    )
    .map_err(|_| bad_state("state-write: cannot exclusively create temporary native metadata"))?;
    temporary
        .write_all(&bytes)
        .and_then(|_| temporary.sync_all())
        .map_err(|_| bad_state("state-sync: cannot synchronize temporary native metadata"))?;
    check_publication(directory, manifest, previous, true)?;
    check_same(&temporary, &open_metadata(&directory.handle, TEMP)?)?;
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
    directory
        .handle
        .sync_all()
        .map_err(|_| bad_state("state-sync: cannot synchronize native manifest directory"))?;
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
    use std::collections::BTreeMap;
    use std::fs::{self, DirBuilder, File, Permissions};
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt, symlink};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

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
        assert_eq!(value["format_version"], 1);
        assert_eq!(value["canonical_path"], tmp.path().to_str().unwrap());
        assert_eq!(value["run"].as_array().unwrap().len(), 16);
        assert_ne!(value["run"], json!(vec![0; 16]));
        assert_eq!(value["copy_complete"], Value::Null);
        assert_eq!(state.identity().canonical_path, tmp.path());
        assert_eq!(state.identity().format_version, 1);
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
        change_manifest(&tmp.path(), |value| value["format_version"] = json!(2));
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
        for field in ["copy_complete", "run", "canonical_path", "directory_device"] {
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
}
