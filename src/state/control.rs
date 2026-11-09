use super::*;
use std::time::{Duration, Instant};

const CONTROL: &CStr = c"control";
const LOCK: &CStr = c"lock";
const ENDPOS: &CStr = c"endpos.json";
const PENDING: &CStr = c".endpos.tmp";
const MAX_REQUEST_BYTES: u64 = 4096;
const LOCK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EndposRequest {
    version: u32,
    stream: StreamId,
    copy_generation: CopyGeneration,
    slot_nonce: [u8; 16],
    target_nonce: [u8; 16],
    end_lsn: SourceEndLsn,
    sealed: bool,
}

impl EndposRequest {
    pub(crate) fn stream(&self) -> StreamId {
        self.stream
    }
    pub(crate) fn copy_generation(&self) -> CopyGeneration {
        self.copy_generation
    }
    pub(crate) fn slot_nonce(&self) -> [u8; 16] {
        self.slot_nonce
    }
    pub(crate) fn target_nonce(&self) -> [u8; 16] {
        self.target_nonce
    }
    pub(crate) fn end(&self) -> SourceEndLsn {
        self.end_lsn
    }
    pub(crate) fn is_sealed(&self) -> bool {
        self.sealed
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SealedEndpos {
    request: EndposRequest,
}
impl SealedEndpos {
    pub(crate) fn request(&self) -> &EndposRequest {
        &self.request
    }
    pub(crate) fn end(&self) -> SourceEndLsn {
        self.request.end()
    }
}

#[derive(Debug)]
pub(crate) struct ControlHandle {
    directory: Directory,
    control: File,
    lock: File,
    follow: FollowControl,
    plan: ClonePlan,
    target: ConfirmedProvenance,
}

struct Record {
    request: EndposRequest,
    file: File,
}
struct ControlLock(File);
impl Drop for ControlLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

impl ControlHandle {
    pub(crate) fn inspect(
        path: &Path,
        operator: Option<&OperatorIdentity>,
        io: &IoControl,
    ) -> Result<Self> {
        clone_state::check_io(io)?;
        if let Some(operator) = operator {
            clone_state::validate_operator(operator)?;
        }
        let leaf = path
            .file_name()
            .ok_or_else(|| bad_state("state-control: state path is invalid"))?;
        let parent_path = fs::canonicalize(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )
        .map_err(|_| bad_state("state-control: state parent is unavailable"))?;
        let parent = open_parent(&parent_path)?;
        let name = CString::new(leaf.as_bytes())
            .map_err(|_| bad_state("state-control: state path is invalid"))?;
        let handle = open_at(&parent, &name, DIRECTORY_FLAGS, 0)
            .map_err(|_| bad_state("state-control: existing native state is unavailable"))?;
        let directory = Directory {
            parent_path,
            parent,
            name,
            handle,
        };
        directory.check()?;
        if !manifest_present(&directory.handle, false)? {
            return Err(bad_state(
                "state-control: existing native manifest is absent",
            ));
        }
        let (manifest, _) = read_manifest(&directory, &directory.parent_path.join(leaf))?;
        if manifest.operator.as_ref() != operator {
            return Err(bad_state(
                "state-control: operator differs from native binding",
            ));
        }
        let (follow, plan, target) = admitted(&manifest)?;
        let (control, lock) = namespace(&directory)?;
        let result = Self {
            directory,
            control,
            lock,
            follow,
            plan,
            target,
        };
        let _guard = result.acquire(Some(io))?;
        result.current()?;
        result.record(ENDPOS)?;
        result.record(PENDING)?;
        Ok(result)
    }

    pub(crate) fn for_worker(native: &NativeState, io: &IoControl) -> Result<Self> {
        native.checked_manifest()?;
        Self::inspect(
            &native.identity.canonical_path,
            native.manifest.operator.as_ref(),
            io,
        )
    }

    pub(crate) fn try_clone(&self, io: &IoControl) -> Result<Self> {
        self.validate(io)?;
        let path = self
            .directory
            .parent_path
            .join(std::ffi::OsStr::from_bytes(self.directory.name.to_bytes()));
        let result = Self::inspect(&path, self.follow.operator.as_ref(), io)?;
        check_same(&self.directory.handle, &result.directory.handle)?;
        check_same(&self.control, &result.control)?;
        check_same(&self.lock, &result.lock)?;
        if self.follow != result.follow || self.plan != result.plan || self.target != result.target
        {
            return Err(bad_state(
                "state-control: native binding changed while cloning handle",
            ));
        }
        Ok(result)
    }

    pub(crate) fn validate_active(&self, io: &IoControl) -> Result<()> {
        let _guard = self.acquire(Some(io))?;
        if self.current()? {
            return Err(bad_state(
                "state-control: terminal cleanup forbids unfinished follow",
            ));
        }
        self.record(ENDPOS)?;
        self.record(PENDING)?;
        Ok(())
    }

    pub(crate) fn validate(&self, io: &IoControl) -> Result<()> {
        let _guard = self.acquire(Some(io))?;
        self.current()?;
        self.record(ENDPOS)?;
        self.record(PENDING)?;
        Ok(())
    }

    pub(crate) fn read(&self, io: &IoControl) -> Result<Option<EndposRequest>> {
        let _guard = self.acquire(Some(io))?;
        self.current()?;
        let record = self.record(ENDPOS)?;
        if let Some(pending) = self.record(PENDING)? {
            let retrying_seal = record.as_ref().is_some_and(|final_record| {
                let mut sealed = final_record.request.clone();
                sealed.sealed = true;
                !final_record.request.is_sealed() && pending.request == sealed
            });
            if !retrying_seal {
                return Err(bad_state(
                    "state-control: pending publication requires an identical retry",
                ));
            }
        }
        if let Some(record) = &record {
            self.synchronize(record)?;
        }
        Ok(record.map(|record| record.request))
    }

    pub(crate) fn set(
        &self,
        end: SourceEndLsn,
        floor: SourceEndLsn,
        io: &IoControl,
    ) -> Result<EndposRequest> {
        let _guard = self.acquire(Some(io))?;
        let cleanup = self.current()?;
        let previous = self.record(ENDPOS)?;
        let pending = self.record(PENDING)?;
        if end.0 == 0 {
            return Err(bad_state("state-control: end position must be nonzero"));
        }
        let desired = match &previous {
            Some(record) if end == record.request.end() => {
                // Whole source transactions may advance progress beyond an already admitted boundary.
                if cleanup && !record.request.is_sealed() {
                    return Err(bad_state(
                        "state-control: terminal cleanup permits only the existing sealed boundary",
                    ));
                }
                record.request.clone()
            }
            Some(record) if record.request.is_sealed() || end < record.request.end() => {
                return Err(bad_state(
                    "state-control: boundary cannot decrease or change after sealing",
                ));
            }
            _ => {
                if cleanup || end < floor {
                    return Err(bad_state(
                        "state-control: boundary is below admission floor or cleanup is terminal",
                    ));
                }
                EndposRequest {
                    version: 1,
                    stream: self.follow.binding.stream(),
                    copy_generation: self.plan.generation,
                    slot_nonce: self.follow.binding.slot.intent.nonce,
                    target_nonce: self.target.intent.nonce,
                    end_lsn: end,
                    sealed: false,
                }
            }
        };
        self.publish(&desired, previous.as_ref(), pending.as_ref())?;
        Ok(desired)
    }

    pub(crate) fn seal(&self, expected: &EndposRequest, io: &IoControl) -> Result<SealedEndpos> {
        let _guard = self.acquire(Some(io))?;
        let cleanup = self.current()?;
        self.check_request(expected)?;
        let previous = self.record(ENDPOS)?;
        let previous = previous
            .as_ref()
            .ok_or_else(|| bad_state("state-control: boundary is absent"))?;
        if previous.request.end() != expected.end() {
            return Err(bad_state("state-control: boundary changed before sealing"));
        }
        if cleanup && !previous.request.is_sealed() {
            return Err(bad_state(
                "state-control: cleanup cannot seal an unfinished boundary",
            ));
        }
        let mut request = expected.clone();
        request.sealed = true;
        self.publish(&request, Some(previous), self.record(PENDING)?.as_ref())?;
        Ok(SealedEndpos { request })
    }

    pub(crate) fn follow(&self) -> &FollowControl {
        &self.follow
    }
    pub(crate) fn plan(&self) -> &ClonePlan {
        &self.plan
    }
    pub(crate) fn target(&self) -> &ConfirmedProvenance {
        &self.target
    }

    fn acquire(&self, io: Option<&IoControl>) -> Result<ControlLock> {
        check_namespace(&self.directory, &self.control, &self.lock)?;
        let guard = acquire(&self.control, &self.lock, io)?;
        check_namespace(&self.directory, &self.control, &self.lock)?;
        Ok(guard)
    }

    fn current(&self) -> Result<bool> {
        check_namespace(&self.directory, &self.control, &self.lock)?;
        if !manifest_present(&self.directory.handle, false)? {
            return Err(bad_state("state-control: native manifest disappeared"));
        }
        let path = self
            .directory
            .parent_path
            .join(std::ffi::OsStr::from_bytes(self.directory.name.to_bytes()));
        let (manifest, _) = read_manifest(&self.directory, &path)?;
        let (follow, plan, target) = admitted(&manifest)?;
        if follow != self.follow || plan != self.plan || target != self.target {
            return Err(bad_state("state-control: native follow binding changed"));
        }
        Ok(manifest
            .follow_state
            .as_ref()
            .is_some_and(|follow| follow.cleanup.is_some()))
    }

    fn check_request(&self, request: &EndposRequest) -> Result<()> {
        if request.version != 1
            || request.end().0 == 0
            || request.stream() != self.follow.binding.stream()
            || request.copy_generation() != self.plan.generation
            || request.slot_nonce() != self.follow.binding.slot.intent.nonce
            || request.target_nonce() != self.target.intent.nonce
        {
            return Err(bad_state(
                "state-control: request has an invalid version, boundary, or binding",
            ));
        }
        Ok(())
    }

    fn record(&self, name: &CStr) -> Result<Option<Record>> {
        let Some(file) = optional_file(&self.control, name)? else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        (&file)
            .take(MAX_REQUEST_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| bad_state("state-control: cannot read boundary record"))?;
        if bytes.len() as u64 > MAX_REQUEST_BYTES {
            return Err(bad_state(
                "state-control: boundary record exceeds its size limit",
            ));
        }
        let request: EndposRequest = serde_json::from_slice(&bytes)
            .map_err(|_| bad_state("state-control: boundary record is corrupt or incomplete"))?;
        self.check_request(&request)?;
        check_same(&file, &open_metadata(&self.control, name)?)?;
        Ok(Some(Record { request, file }))
    }

    fn check_previous(&self, previous: Option<&Record>) -> Result<()> {
        match (previous, self.record(ENDPOS)?) {
            (None, None) => Ok(()),
            (Some(expected), Some(current)) if expected.request == current.request => {
                check_same(&expected.file, &current.file)
            }
            _ => Err(bad_state(
                "state-control: boundary record changed during publication",
            )),
        }
    }

    fn synchronize(&self, record: &Record) -> Result<()> {
        #[cfg(test)]
        tests::publication_step(tests::PublicationStep::FileSync)?;
        record
            .file
            .sync_all()
            .map_err(|_| bad_state("state-control: cannot synchronize boundary record"))?;
        #[cfg(test)]
        tests::publication_step(tests::PublicationStep::DirectorySync)?;
        self.control
            .sync_all()
            .map_err(|_| bad_state("state-control: cannot synchronize control directory"))?;
        self.current()?;
        self.check_previous(Some(record))
    }

    fn publish(
        &self,
        request: &EndposRequest,
        previous: Option<&Record>,
        pending: Option<&Record>,
    ) -> Result<()> {
        if pending.is_some_and(|pending| pending.request != *request) {
            return Err(bad_state(
                "state-control: conflicting pending publication is preserved",
            ));
        }
        if pending.is_none() && previous.is_some_and(|previous| previous.request == *request) {
            return self.synchronize(previous.expect("checked existing record"));
        }
        let temporary = if let Some(pending) = pending {
            check_same(&pending.file, &open_metadata(&self.control, PENDING)?)?;
            open_metadata(&self.control, PENDING)?
        } else {
            #[cfg(test)]
            tests::publication_step(tests::PublicationStep::Create)?;
            let mut file = open_at(
                &self.control,
                PENDING,
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                0o600,
            )
            .map_err(|_| {
                bad_state("state-control: cannot exclusively create temporary boundary record")
            })?;
            #[cfg(test)]
            tests::publication_step(tests::PublicationStep::Write)?;
            let bytes = serde_json::to_vec(request)
                .map_err(|_| bad_state("state-control: cannot encode boundary record"))?;
            file.write_all(&bytes)
                .map_err(|_| bad_state("state-control: cannot write boundary record"))?;
            file
        };
        #[cfg(test)]
        tests::publication_step(tests::PublicationStep::FileSync)?;
        temporary.sync_all().map_err(|_| {
            bad_state("state-control: cannot synchronize temporary boundary record")
        })?;
        self.current()?;
        self.check_previous(previous)?;
        check_same(&temporary, &open_metadata(&self.control, PENDING)?)?;
        #[cfg(test)]
        tests::publication_step(tests::PublicationStep::Rename)?;
        // SAFETY: names are fixed components under the retained, checked directory and exclusive control lock.
        if unsafe {
            libc::renameat(
                self.control.as_raw_fd(),
                PENDING.as_ptr(),
                self.control.as_raw_fd(),
                ENDPOS.as_ptr(),
            )
        } != 0
        {
            return Err(bad_state("state-control: cannot publish boundary record"));
        }
        #[cfg(test)]
        tests::publication_step(tests::PublicationStep::DirectorySync)?;
        self.control
            .sync_all()
            .map_err(|_| bad_state("state-control: cannot synchronize published boundary"))?;
        #[cfg(test)]
        tests::publication_step(tests::PublicationStep::Published)?;
        self.current()?;
        check_same(&temporary, &open_metadata(&self.control, ENDPOS)?)?;
        Ok(())
    }
}

fn admitted(manifest: &Manifest) -> Result<(FollowControl, ClonePlan, ConfirmedProvenance)> {
    let follow = manifest
        .follow_state
        .as_ref()
        .ok_or_else(|| bad_state("state-control: native follow state is absent"))?;
    let saved = manifest
        .clone_state
        .as_ref()
        .ok_or_else(|| bad_state("state-control: native copy binding is absent"))?;
    let binding = follow
        .binding
        .as_ref()
        .ok_or_else(|| bad_state("state-control: confirmed follow binding is absent"))?;
    let target = match manifest.target_provenance.as_slice() {
        [TargetProvenance::Confirmed(target)] => target,
        _ => {
            return Err(bad_state(
                "state-control: confirmed target receipt is absent",
            ));
        }
    };
    if saved.phase != ClonePhase::Analyzing
        || follow.restart.is_some()
        || binding.copy_generation != saved.plan.generation
        || binding.stream() != target.intent.binding.stream
        || manifest.source_slot != Some(SourceSlotProvenance::Confirmed(binding.slot.clone()))
        || follow
            .publication
            .as_ref()
            .and_then(PublicationProvenance::confirmed)
            != Some(&binding.publication)
    {
        return Err(bad_state(
            "state-control: cutover requires the current established follow generation",
        ));
    }
    let control = FollowControl {
        binding: binding.clone(),
        operator: manifest.operator.clone(),
        origin_name: follow.origin_name.clone(),
    };
    control.validate()?;
    Ok((control, saved.plan.clone(), target.clone()))
}

fn optional_file(directory: &File, name: &CStr) -> Result<Option<File>> {
    let file = match open_at(directory, name, libc::O_RDONLY, 0) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(bad_state(
                "state-control: metadata path cannot be opened safely",
            ));
        }
    };
    let meta = metadata(&file)?;
    if !meta.is_file() || meta.nlink() != 1 {
        return Err(bad_state(
            "state-control: metadata must be a regular single-link file",
        ));
    }
    check_private(&meta, 0o600)?;
    check_same(&file, &open_metadata(directory, name)?)?;
    Ok(Some(file))
}

pub(super) fn validate_directory(root: &File) -> Result<()> {
    let control = open_at(root, CONTROL, DIRECTORY_FLAGS, 0)
        .map_err(|_| bad_state("state-control: control directory cannot be opened safely"))?;
    check_private(&metadata(&control)?, 0o700)?;
    let handle = open_at(&control, c".", DIRECTORY_FLAGS, 0)
        .map_err(|_| bad_state("state-control: cannot inspect control directory"))?;
    let fd = handle.into_raw_fd();
    // SAFETY: fd is an owned directory descriptor, transferred only if fdopendir succeeds.
    let stream = unsafe { libc::fdopendir(fd) };
    if stream.is_null() {
        // SAFETY: fdopendir failed and did not consume fd.
        drop(unsafe { File::from_raw_fd(fd) });
        return Err(bad_state("state-control: cannot inspect control directory"));
    }
    let result = loop {
        // SAFETY: errno is thread-local and stream remains live until closed below.
        let entry = unsafe {
            *errno_pointer() = 0;
            libc::readdir(stream)
        };
        if entry.is_null() {
            break if io::Error::last_os_error().raw_os_error() == Some(0) {
                Ok(())
            } else {
                Err(bad_state("state-control: cannot read control entries"))
            };
        }
        // SAFETY: readdir provides a NUL-terminated name until its next call.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        match name.to_bytes() {
            b"." | b".." => (),
            b"lock" | b"endpos.json" | b".endpos.tmp" => {
                if let Err(error) = open_metadata(&control, name) {
                    break Err(error);
                }
            }
            _ => {
                break Err(bad_state(
                    "state-control: unrecognized control entry is preserved",
                ));
            }
        }
    };
    // SAFETY: closedir releases stream and its transferred descriptor exactly once.
    if unsafe { libc::closedir(stream) } != 0 {
        return Err(bad_state("state-control: cannot close control scan"));
    }
    result
}

fn namespace(root: &Directory) -> Result<(File, File)> {
    root.check()?;
    // SAFETY: root is live, CONTROL is a fixed single-component name, and mode is private.
    if unsafe { libc::mkdirat(root.handle.as_raw_fd(), CONTROL.as_ptr(), 0o700) } != 0
        && io::Error::last_os_error().kind() != io::ErrorKind::AlreadyExists
    {
        return Err(bad_state("state-control: cannot create control directory"));
    }
    validate_directory(&root.handle)?;
    let control = open_at(&root.handle, CONTROL, DIRECTORY_FLAGS, 0)
        .map_err(|_| bad_state("state-control: control directory is unavailable"))?;
    #[cfg(test)]
    tests::namespace_step(tests::NamespaceStep::Parent)?;
    root.handle
        .sync_all()
        .map_err(|_| bad_state("state-control: cannot synchronize control directory creation"))?;
    let lock = match open_at(
        &control,
        LOCK,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o600,
    ) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            open_metadata(&control, LOCK)?
        }
        Err(_) => return Err(bad_state("state-control: cannot create control lock")),
    };
    #[cfg(test)]
    tests::namespace_step(tests::NamespaceStep::Lock)?;
    lock.sync_all()
        .map_err(|_| bad_state("state-control: cannot synchronize control lock"))?;
    #[cfg(test)]
    tests::namespace_step(tests::NamespaceStep::Directory)?;
    control
        .sync_all()
        .map_err(|_| bad_state("state-control: cannot synchronize control lock directory"))?;
    check_namespace(root, &control, &lock)?;
    Ok((control, lock))
}

fn check_namespace(root: &Directory, control: &File, lock: &File) -> Result<()> {
    root.check()?;
    validate_directory(&root.handle)?;
    let current = open_at(&root.handle, CONTROL, DIRECTORY_FLAGS, 0)
        .map_err(|_| bad_state("state-control: control directory identity changed"))?;
    check_same(control, &current)?;
    check_same(lock, &open_metadata(control, LOCK)?)
}

fn acquire(control: &File, identity: &File, io: Option<&IoControl>) -> Result<ControlLock> {
    let file = open_metadata(control, LOCK)?;
    check_same(identity, &file)?;
    let deadline = Instant::now() + LOCK_TIMEOUT;
    loop {
        if let Some(io) = io {
            clone_state::check_io(io)?;
        }
        match file.try_lock() {
            Ok(()) => return Ok(ControlLock(file)),
            Err(TryLockError::WouldBlock) if io.is_some() && Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(TryLockError::WouldBlock) => {
                return Err(bad_state("state-control: control lock is busy"));
            }
            Err(TryLockError::Error(_)) => {
                return Err(bad_state("state-control: cannot acquire control lock"));
            }
        }
    }
}

pub(super) fn with_native_lock<T>(
    native: &mut NativeState,
    action: impl FnOnce(&mut NativeState) -> Result<T>,
) -> Result<T> {
    native.checked_manifest()?;
    let (control, lock) = namespace(&native.directory)?;
    let _guard = acquire(&control, &lock, None)?;
    check_namespace(&native.directory, &control, &lock)?;
    native.checked_manifest()?;
    action(native)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{
        CloneSource, CloneTarget, FollowBinding, PublicationReceipt, RestorePolicy,
    };
    use crate::identity::SourceId;
    use serde_json::{Value, json};
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum PublicationStep {
        Create,
        Write,
        FileSync,
        Rename,
        DirectorySync,
        Published,
    }
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    pub(super) enum NamespaceStep {
        Parent,
        Lock,
        Directory,
    }
    thread_local! { static NAMESPACE_FAIL_AT: std::cell::Cell<Option<NamespaceStep>> = const { std::cell::Cell::new(None) }; }
    pub(super) fn namespace_step(step: NamespaceStep) -> Result<()> {
        NAMESPACE_FAIL_AT.with(|slot| {
            if slot.get() == Some(step) {
                slot.set(None);
                Err(bad_state(
                    "state-test: injected control namespace synchronization failure",
                ))
            } else {
                Ok(())
            }
        })
    }
    type PublicationMutation = (PublicationStep, Box<dyn FnOnce()>);
    thread_local! {
        static FAIL_AT: std::cell::Cell<Option<PublicationStep>> = const { std::cell::Cell::new(None) };
        static MUTATE_AT: std::cell::RefCell<Option<PublicationMutation>> = const { std::cell::RefCell::new(None) };
    }
    pub(super) fn publication_step(step: PublicationStep) -> Result<()> {
        let mutate = MUTATE_AT.with(|slot| {
            let mut value = slot.borrow_mut();
            if value.as_ref().is_some_and(|(at, _)| *at == step) {
                value.take()
            } else {
                None
            }
        });
        if let Some((_, mutate)) = mutate {
            mutate();
        }
        FAIL_AT.with(|slot| {
            if slot.get() == Some(step) {
                slot.set(None);
                Err(bad_state(
                    "state-test: injected control publication failure",
                ))
            } else {
                Ok(())
            }
        })
    }

    fn io() -> IoControl {
        IoControl::unbounded(Arc::new(AtomicBool::new(false)))
    }

    fn fixture() -> (super::super::tests::TempDir, NativeState) {
        let tmp = super::super::tests::TempDir::new();
        let mut native = open_follow(
            &StateRequest {
                path: tmp.path(),
                action: StateAction::Create,
            },
            None,
            None,
            &io(),
        )
        .unwrap();
        let plan = ClonePlan {
            source: CloneSource {
                identity: SourceId {
                    system_identifier: 2,
                    database_oid: 3,
                },
                timeline: 1,
                major: 16,
            },
            target: CloneTarget {
                identity: SourceId {
                    system_identifier: 4,
                    database_oid: 5,
                },
                major: 18,
            },
            generation: CopyGeneration(1),
            tables: Vec::new(),
            sequences: Vec::new(),
            policy: RestorePolicy::default(),
        };
        let stream = StreamId {
            run: native.identity.run,
            source: plan.source.identity,
            generation: 1,
        };
        let slot = SourceSlotReceipt {
            intent: SourceSlotIntent {
                binding: SourceSlotBinding::new(stream, "owned_slot".into(), Plugin::Pgoutput, 1)
                    .unwrap(),
                nonce: [1; 16],
            },
            consistent_point: SourceEndLsn(8),
            snapshot_token: "snapshot-8".into(),
        };
        let publication = PublicationReceipt {
            name: "owned_publication".into(),
            oid: 9,
            owner_oid: 10,
        };
        let binding = FollowBinding {
            slot: slot.clone(),
            publication: publication.clone(),
            source_timeline: 1,
            copy_generation: plan.generation,
            source_types: Vec::new(),
        };
        let mut next = native.checked_manifest().unwrap();
        next.source_slot = Some(SourceSlotProvenance::Confirmed(slot));
        next.target_provenance = vec![TargetProvenance::Confirmed(ConfirmedProvenance {
            intent: ProvisioningIntent {
                binding: TargetBinding {
                    stream,
                    target_system_identifier: Some(4),
                    target_database_oid: 5,
                    schema_name: "owned_meta".into(),
                    table_name: "checkpoint".into(),
                    owner_oid: 10,
                    metadata_version: TARGET_METADATA_VERSION,
                },
                nonce: [2; 16],
            },
            schema_oid: 11,
            table_oid: 12,
            marker_table_oid: 13,
            marker_index_oid: 14,
        })];
        next.clone_state = Some(CloneState {
            ticket: SnapshotTicket {
                token: "snapshot-8".into(),
                source: plan.source.clone(),
                generation: plan.generation,
            },
            plan,
            phase: ClonePhase::Analyzing,
            owned_objects: Vec::new(),
            owned_tables: Vec::new(),
            completion: None,
        });
        let follow = next.follow_state.as_mut().unwrap();
        follow.publication = Some(PublicationProvenance::Confirmed {
            intent: super::super::follow_state::PublicationIntent {
                run: stream.run,
                name: publication.name.clone(),
                nonce: [3; 16],
            },
            receipt: publication,
        });
        follow.binding = Some(binding);
        native.replace_manifest(next).unwrap();
        (tmp, native)
    }

    #[test]
    fn control_concurrent_forward_only() {
        let (tmp, native) = fixture();
        let control = ControlHandle::for_worker(&native, &io()).unwrap();
        assert!(control.read(&io()).unwrap().is_none());
        assert!(
            control
                .set(SourceEndLsn(0), SourceEndLsn(8), &io())
                .is_err()
        );
        assert!(
            control
                .set(SourceEndLsn(7), SourceEndLsn(8), &io())
                .is_err()
        );
        let first = control
            .set(SourceEndLsn(20), SourceEndLsn(8), &io())
            .unwrap();
        assert_eq!(first.end(), SourceEndLsn(20));
        assert_eq!(
            control
                .set(SourceEndLsn(20), SourceEndLsn(30), &io())
                .unwrap(),
            first
        );
        assert!(
            control
                .set(SourceEndLsn(19), SourceEndLsn(8), &io())
                .is_err()
        );
        assert!(
            control
                .set(SourceEndLsn(25), SourceEndLsn(30), &io())
                .is_err()
        );
        let path = tmp.path();
        let outcomes = std::thread::scope(|scope| {
            let a = scope.spawn(|| {
                ControlHandle::inspect(&path, None, &io()).unwrap().set(
                    SourceEndLsn(30),
                    SourceEndLsn(8),
                    &io(),
                )
            });
            let b = scope.spawn(|| {
                ControlHandle::inspect(&path, None, &io()).unwrap().set(
                    SourceEndLsn(40),
                    SourceEndLsn(8),
                    &io(),
                )
            });
            (a.join().unwrap(), b.join().unwrap())
        });
        assert!(outcomes.1.is_ok());
        assert_eq!(
            control.read(&io()).unwrap().unwrap().end(),
            SourceEndLsn(40)
        );
        assert_eq!(
            ControlHandle::inspect(&tmp.path(), None, &io())
                .unwrap()
                .read(&io())
                .unwrap(),
            control.read(&io()).unwrap()
        );
    }
    #[test]
    fn control_seal_races() {
        let (tmp, native) = fixture();
        let control = ControlHandle::for_worker(&native, &io()).unwrap();
        let first = control
            .set(SourceEndLsn(20), SourceEndLsn(8), &io())
            .unwrap();
        let sealed = control.seal(&first, &io()).unwrap();
        assert_eq!(sealed.end(), first.end());
        assert!(sealed.request().is_sealed());
        assert_eq!(control.seal(&first, &io()).unwrap().end(), first.end());
        assert_eq!(
            control.set(first.end(), SourceEndLsn(50), &io()).unwrap(),
            *sealed.request()
        );
        assert!(
            control
                .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                .is_err()
        );
        assert!(
            control
                .set(SourceEndLsn(10), SourceEndLsn(8), &io())
                .is_err()
        );
        assert!(
            ControlHandle::inspect(&tmp.path(), None, &io())
                .unwrap()
                .read(&io())
                .unwrap()
                .unwrap()
                .is_sealed()
        );
    }

    #[test]
    fn control_cleanup_serializes_new_boundary_and_retains_sealed_observation() {
        let (tmp, mut native) = fixture();
        let control = ControlHandle::for_worker(&native, &io()).unwrap();
        let copy = control.try_clone(&io()).unwrap();
        assert_eq!(copy.follow(), control.follow());
        assert_eq!(copy.plan(), control.plan());
        assert_eq!(copy.target(), control.target());
        control.validate_active(&io()).unwrap();
        let first = control
            .set(SourceEndLsn(20), SourceEndLsn(8), &io())
            .unwrap();
        let sealed = control.seal(&first, &io()).unwrap();
        with_native_lock(&mut native, |native| {
            let mut next = native.checked_manifest()?;
            next.follow_state.as_mut().unwrap().cleanup = Some(super::super::CleanupProgress {
                slot_removed: false,
                publication_removed: false,
            });
            native.replace_manifest(next)
        })
        .unwrap();
        assert!(control.validate_active(&io()).is_err());
        control.validate(&io()).unwrap();
        assert_eq!(
            control.read(&io()).unwrap().as_ref(),
            Some(sealed.request())
        );
        assert_eq!(
            control.set(first.end(), SourceEndLsn(99), &io()).unwrap(),
            *sealed.request()
        );
        assert_eq!(control.seal(&first, &io()).unwrap().end(), first.end());
        assert!(
            control
                .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                .is_err()
        );
        let observed = ControlHandle::inspect(&tmp.path(), None, &io()).unwrap();
        assert_eq!(
            observed.read(&io()).unwrap().as_ref(),
            Some(sealed.request())
        );
    }

    #[test]
    fn control_publication_faults() {
        for step in [
            PublicationStep::Create,
            PublicationStep::Write,
            PublicationStep::FileSync,
            PublicationStep::Rename,
            PublicationStep::DirectorySync,
            PublicationStep::Published,
        ] {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            FAIL_AT.with(|slot| slot.set(Some(step)));
            assert!(
                control
                    .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                    .is_err(),
                "{step:?}"
            );
            FAIL_AT.with(|slot| assert!(slot.get().is_none(), "missed injection {step:?}"));
            if step == PublicationStep::Write {
                let before = fs::read(tmp.path().join("control/.endpos.tmp")).unwrap();
                assert!(control.read(&io()).is_err());
                assert!(
                    control
                        .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                        .is_err()
                );
                assert!(ControlHandle::inspect(&tmp.path(), None, &io()).is_err());
                assert_eq!(
                    fs::read(tmp.path().join("control/.endpos.tmp")).unwrap(),
                    before
                );
            } else {
                if matches!(step, PublicationStep::FileSync | PublicationStep::Rename) {
                    assert!(control.read(&io()).is_err());
                    let before = fs::read(tmp.path().join("control/.endpos.tmp")).unwrap();
                    assert!(
                        control
                            .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                            .is_err()
                    );
                    assert_eq!(
                        fs::read(tmp.path().join("control/.endpos.tmp")).unwrap(),
                        before
                    );
                }
                let reopened = ControlHandle::inspect(&tmp.path(), None, &io()).unwrap();
                assert_eq!(
                    reopened
                        .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                        .unwrap()
                        .end(),
                    SourceEndLsn(20)
                );
                assert_eq!(
                    control.read(&io()).unwrap().unwrap().end(),
                    SourceEndLsn(20)
                );
                assert!(!tmp.path().join("control/.endpos.tmp").exists());
            }
        }
    }

    #[test]
    fn control_seal_publication_faults() {
        for step in [
            PublicationStep::Create,
            PublicationStep::Write,
            PublicationStep::FileSync,
            PublicationStep::Rename,
            PublicationStep::DirectorySync,
            PublicationStep::Published,
        ] {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            let first = control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .unwrap();
            FAIL_AT.with(|slot| slot.set(Some(step)));
            assert!(control.seal(&first, &io()).is_err(), "{step:?}");
            FAIL_AT.with(|slot| assert!(slot.get().is_none(), "missed injection {step:?}"));
            if step == PublicationStep::Write {
                assert!(control.seal(&first, &io()).is_err());
                assert!(control.read(&io()).is_err());
            } else {
                if matches!(step, PublicationStep::FileSync | PublicationStep::Rename) {
                    assert!(control.set(first.end(), SourceEndLsn(8), &io()).is_err());
                    assert!(
                        control
                            .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                            .is_err()
                    );
                }
                assert!(control.seal(&first, &io()).unwrap().request().is_sealed());
                assert!(
                    ControlHandle::inspect(&tmp.path(), None, &io())
                        .unwrap()
                        .read(&io())
                        .unwrap()
                        .unwrap()
                        .is_sealed()
                );
            }
        }
    }

    #[test]
    fn control_forward_update_and_seal_have_one_ordered_winner() {
        for _ in 0..12 {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            let first = control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .unwrap();
            let competing = ControlHandle::inspect(&tmp.path(), None, &io()).unwrap();
            let barrier = std::sync::Barrier::new(2);
            let (advanced, sealed) = std::thread::scope(|scope| {
                let advance = scope.spawn(|| {
                    barrier.wait();
                    competing.set(SourceEndLsn(30), SourceEndLsn(8), &io())
                });
                let seal = scope.spawn(|| {
                    barrier.wait();
                    control.seal(&first, &io())
                });
                (advance.join().unwrap(), seal.join().unwrap())
            });
            assert_ne!(advanced.is_ok(), sealed.is_ok());
            let current = control.read(&io()).unwrap().unwrap();
            if advanced.is_ok() {
                assert_eq!(current.end(), SourceEndLsn(30));
                assert!(!current.is_sealed());
            } else {
                assert_eq!(current.end(), first.end());
                assert!(current.is_sealed());
            }
        }
    }

    fn change_manifest(path: &Path, change: impl FnOnce(&mut Value)) {
        let path = path.join("manifest.json");
        let mut value: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        change(&mut value);
        fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }

    #[test]
    fn control_cutover_requires_anchored_streaming() {
        for phase in [
            ClonePhase::Fresh,
            ClonePhase::PreDataPending,
            ClonePhase::Copying,
            ClonePhase::PostDataPending,
            ClonePhase::PostDataComplete,
            ClonePhase::SequencesPending,
            ClonePhase::SequencesComplete,
        ] {
            let (tmp, mut native) = fixture();
            let mut next = native.checked_manifest().unwrap();
            next.clone_state.as_mut().unwrap().phase = phase;
            native.replace_manifest(next).unwrap();
            assert!(
                ControlHandle::for_worker(&native, &io()).is_err(),
                "{phase:?}"
            );
            assert!(!tmp.path().join("control/endpos.json").exists());
        }
        let (tmp, mut native) = fixture();
        let control = ControlHandle::for_worker(&native, &io()).unwrap();
        let mut next = native.checked_manifest().unwrap();
        next.clone_state.as_mut().unwrap().phase = ClonePhase::Copying;
        native.replace_manifest(next).unwrap();
        assert!(
            control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .is_err()
        );
        let binding = native.follow_control().unwrap().unwrap().binding;
        let plan = native.checked_clone().unwrap().unwrap().plan;
        native.begin_follow_restart(&binding, &plan).unwrap();
        assert!(
            control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .is_err()
        );
        assert!(!tmp.path().join("control/endpos.json").exists());
    }

    #[test]
    fn control_binding_changed() {
        for field in [
            "target_nonce",
            "slot_nonce",
            "generation",
            "unconfirmed_target",
            "unconfirmed_publication",
            "source_receipt",
            "operator",
            "format",
        ] {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .unwrap();
            let before = fs::read(tmp.path().join("control/endpos.json")).unwrap();
            change_manifest(&tmp.path(), |value| match field {
                "target_nonce" => {
                    value["target_provenance"][0]["record"]["intent"]["nonce"][0] = json!(99)
                }
                "slot_nonce" => {
                    value["source_slot"]["record"]["intent"]["nonce"][0] = json!(99);
                    value["follow_state"]["binding"]["slot"]["intent"]["nonce"][0] = json!(99);
                }
                "generation" => {
                    value["clone_state"]["plan"]["generation"] = json!(2);
                    value["clone_state"]["ticket"]["generation"] = json!(2);
                    value["follow_state"]["binding"]["copy_generation"] = json!(2);
                }
                "unconfirmed_target" => value["target_provenance"] = json!([]),
                "unconfirmed_publication" => value["follow_state"]["publication"] = Value::Null,
                "source_receipt" => value["source_slot"] = Value::Null,
                "operator" => value["operator"] = json!({}),
                "format" => value["format_version"] = json!(7),
                _ => unreachable!(),
            });
            assert!(control.read(&io()).is_err(), "{field}");
            assert!(control.validate(&io()).is_err(), "{field}");
            assert!(control.try_clone(&io()).is_err(), "{field}");
            assert!(
                control
                    .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                    .is_err(),
                "{field}"
            );
            assert_eq!(
                fs::read(tmp.path().join("control/endpos.json")).unwrap(),
                before,
                "{field}"
            );
        }
    }

    #[test]
    fn control_foreign_paths() {
        for kind in [
            "extra_root",
            "extra_control",
            "control_symlink",
            "control_mode",
            "lock_replaced",
            "lock_symlink",
            "lock_mode",
            "lock_hardlink",
            "end_symlink",
            "end_fifo",
            "end_hardlink",
            "end_mode",
            "end_directory",
            "temp_corrupt",
        ] {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            let first = control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .unwrap();
            let root = tmp.path();
            let directory = root.join("control");
            let lock = directory.join("lock");
            let end = directory.join("endpos.json");
            let manifest = fs::read(root.join("manifest.json")).unwrap();
            match kind {
                "extra_root" => {
                    fs::write(root.join("foreign"), b"preserve").unwrap();
                }
                "extra_control" => {
                    fs::write(directory.join("foreign"), b"preserve").unwrap();
                }
                "control_symlink" => {
                    let moved = tmp.0.join("moved_control");
                    fs::rename(&directory, &moved).unwrap();
                    symlink(&moved, &directory).unwrap();
                }
                "control_mode" => {
                    fs::set_permissions(&directory, fs::Permissions::from_mode(0o750)).unwrap()
                }
                "lock_replaced" => {
                    fs::rename(&lock, tmp.0.join("old_lock")).unwrap();
                    fs::write(&lock, b"").unwrap();
                    fs::set_permissions(&lock, fs::Permissions::from_mode(0o600)).unwrap();
                }
                "lock_symlink" => {
                    fs::rename(&lock, tmp.0.join("old_lock")).unwrap();
                    symlink(tmp.0.join("old_lock"), &lock).unwrap();
                }
                "lock_mode" => {
                    fs::set_permissions(&lock, fs::Permissions::from_mode(0o640)).unwrap()
                }
                "lock_hardlink" => fs::hard_link(&lock, tmp.0.join("duplicate_lock")).unwrap(),
                "end_symlink" => {
                    fs::rename(&end, tmp.0.join("old_end")).unwrap();
                    symlink(tmp.0.join("old_end"), &end).unwrap();
                }
                "end_fifo" => {
                    fs::remove_file(&end).unwrap();
                    let path = CString::new(end.as_os_str().as_bytes()).unwrap();
                    // SAFETY: path is NUL-terminated and identifies a private test-only FIFO.
                    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
                }
                "end_hardlink" => fs::hard_link(&end, tmp.0.join("duplicate_end")).unwrap(),
                "end_mode" => fs::set_permissions(&end, fs::Permissions::from_mode(0o640)).unwrap(),
                "end_directory" => {
                    fs::remove_file(&end).unwrap();
                    fs::create_dir(&end).unwrap();
                }
                "temp_corrupt" => {
                    let path = directory.join(".endpos.tmp");
                    fs::write(&path, b"{").unwrap();
                    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
                }
                _ => unreachable!(),
            }
            assert!(control.read(&io()).is_err(), "{kind}");
            assert!(
                control
                    .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                    .is_err(),
                "{kind}"
            );
            assert!(control.seal(&first, &io()).is_err(), "{kind}");
            assert!(control.try_clone(&io()).is_err(), "{kind}");
            if kind != "lock_replaced" {
                assert!(
                    ControlHandle::inspect(&root, None, &io()).is_err(),
                    "{kind}"
                );
            }
            assert_eq!(
                fs::read(root.join("manifest.json")).unwrap(),
                manifest,
                "{kind}"
            );
        }
    }

    #[test]
    fn control_root_and_control_directory_replacement_are_refused() {
        for replace_root in [false, true] {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            let first = control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .unwrap();
            let path = if replace_root {
                tmp.path()
            } else {
                tmp.path().join("control")
            };
            fs::rename(&path, tmp.0.join("retained_original")).unwrap();
            fs::create_dir(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            assert!(control.read(&io()).is_err());
            assert!(
                control
                    .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                    .is_err()
            );
            assert!(control.seal(&first, &io()).is_err());
            assert!(control.try_clone(&io()).is_err());
        }
    }

    #[test]
    fn control_replaced_final_record_during_publication_is_refused() {
        for step in [PublicationStep::FileSync, PublicationStep::Published] {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .unwrap();
            let path = tmp.path().join("control/endpos.json");
            let retained = tmp.0.join("replaced_record");
            MUTATE_AT.with(|slot| {
                *slot.borrow_mut() = Some((
                    step,
                    Box::new(move || {
                        let bytes = fs::read(&path).unwrap();
                        fs::rename(&path, retained).unwrap();
                        fs::write(&path, bytes).unwrap();
                        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
                    }),
                ))
            });
            assert!(
                control
                    .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                    .is_err()
            );
            MUTATE_AT.with(|slot| assert!(slot.borrow().is_none(), "missed replacement hook"));
        }
    }

    #[test]
    fn control_strict_json_binding_and_lsn_validation() {
        for kind in [
            "zero",
            "overflow",
            "negative",
            "malformed_lsn",
            "old_version",
            "unknown",
            "stream",
            "generation",
            "slot_nonce",
            "target_nonce",
            "missing",
            "truncated",
            "too_large",
        ] {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            let first = control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .unwrap();
            let path = tmp.path().join("control/endpos.json");
            let mut value = serde_json::to_value(&first).unwrap();
            match kind {
                "zero" => value["end_lsn"] = json!(0),
                "negative" => value["end_lsn"] = json!(-1),
                "malformed_lsn" => value["end_lsn"] = json!("0/BOGUS"),
                "old_version" => value["version"] = json!(0),
                "unknown" => value["extra"] = json!(true),
                "stream" => value["stream"]["source"]["database_oid"] = json!(99),
                "generation" => value["copy_generation"] = json!(2),
                "slot_nonce" => value["slot_nonce"][0] = json!(9),
                "target_nonce" => value["target_nonce"][0] = json!(9),
                "missing" => {
                    value.as_object_mut().unwrap().remove("sealed");
                }
                _ => (),
            }
            let bytes = match kind {
                "overflow" => serde_json::to_string(&value)
                    .unwrap()
                    .replace("\"end_lsn\":20", "\"end_lsn\":18446744073709551616")
                    .into_bytes(),
                "truncated" => b"{".to_vec(),
                "too_large" => vec![b' '; MAX_REQUEST_BYTES as usize + 1],
                _ => serde_json::to_vec(&value).unwrap(),
            };
            fs::write(&path, &bytes).unwrap();
            assert!(control.read(&io()).is_err(), "{kind}");
            assert!(
                control
                    .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                    .is_err(),
                "{kind}"
            );
            assert!(control.seal(&first, &io()).is_err(), "{kind}");
            assert!(
                ControlHandle::inspect(&tmp.path(), None, &io()).is_err(),
                "{kind}"
            );
            assert_eq!(fs::read(&path).unwrap(), bytes, "{kind}");
        }
    }

    #[test]
    fn control_lock_contention_is_bounded_and_native_action_is_not_run() {
        let (tmp, mut native) = fixture();
        let control = ControlHandle::for_worker(&native, &io()).unwrap();
        let competitor = File::open(tmp.path().join("control/lock")).unwrap();
        competitor.try_lock().unwrap();
        let mut bounded = io();
        bounded.deadline = Instant::now() + Duration::from_millis(20);
        let started = Instant::now();
        assert!(control.read(&bounded).is_err());
        assert!(started.elapsed() < Duration::from_secs(1));
        let mut called = false;
        assert!(
            with_native_lock(&mut native, |_| {
                called = true;
                Ok(())
            })
            .is_err()
        );
        assert!(!called);
        competitor.unlock().unwrap();
        assert!(control.read(&io()).unwrap().is_none());
        assert!(
            with_native_lock(&mut native, |_| Err::<(), _>(bad_state(
                "state-test: action failed"
            )))
            .is_err()
        );
        competitor.try_lock().unwrap();
        competitor.unlock().unwrap();
    }

    #[test]
    fn control_cleanup_intent_wins_against_a_stale_writer() {
        let (_tmp, mut native) = fixture();
        let control = ControlHandle::for_worker(&native, &io()).unwrap();
        control
            .set(SourceEndLsn(20), SourceEndLsn(8), &io())
            .unwrap();
        let (entered, acquired) = std::sync::mpsc::sync_channel(1);
        let (proceed, wait) = std::sync::mpsc::sync_channel(1);
        std::thread::scope(|scope| {
            let cleanup = scope.spawn(move || {
                with_native_lock(&mut native, |native| {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                    let mut next = native.checked_manifest()?;
                    next.follow_state.as_mut().unwrap().cleanup =
                        Some(super::super::CleanupProgress {
                            slot_removed: false,
                            publication_removed: false,
                        });
                    native.replace_manifest(next)
                })
            });
            acquired.recv().unwrap();
            assert!(control.acquire(None).is_err());
            let writer = scope.spawn(|| control.set(SourceEndLsn(30), SourceEndLsn(8), &io()));
            proceed.send(()).unwrap();
            cleanup.join().unwrap().unwrap();
            assert!(writer.join().unwrap().is_err());
        });
        assert_eq!(
            control.read(&io()).unwrap().unwrap().end(),
            SourceEndLsn(20)
        );
        assert!(
            control
                .seal(&control.read(&io()).unwrap().unwrap(), &io())
                .is_err()
        );
    }

    #[test]
    fn control_namespace_synchronization_faults() {
        for step in [
            NamespaceStep::Parent,
            NamespaceStep::Lock,
            NamespaceStep::Directory,
        ] {
            let (tmp, native) = fixture();
            let before = fs::read(tmp.path().join("manifest.json")).unwrap();
            NAMESPACE_FAIL_AT.with(|slot| slot.set(Some(step)));
            assert!(
                ControlHandle::for_worker(&native, &io()).is_err(),
                "{step:?}"
            );
            NAMESPACE_FAIL_AT
                .with(|slot| assert!(slot.get().is_none(), "missed injection {step:?}"));
            let reopened = ControlHandle::for_worker(&native, &io()).unwrap();
            assert!(reopened.read(&io()).unwrap().is_none());
            assert_eq!(fs::read(tmp.path().join("manifest.json")).unwrap(), before);
        }
    }
    #[test]
    fn control_interrupted_seal_reopens_only_the_durable_final_boundary() {
        for step in [
            PublicationStep::Create,
            PublicationStep::Write,
            PublicationStep::FileSync,
            PublicationStep::Rename,
            PublicationStep::DirectorySync,
            PublicationStep::Published,
        ] {
            let (tmp, native) = fixture();
            let control = ControlHandle::for_worker(&native, &io()).unwrap();
            let first = control
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .unwrap();
            FAIL_AT.with(|slot| slot.set(Some(step)));
            assert!(control.seal(&first, &io()).is_err(), "{step:?}");
            FAIL_AT.with(|slot| assert!(slot.get().is_none()));
            drop(control);
            if step == PublicationStep::Write {
                assert!(ControlHandle::inspect(&tmp.path(), None, &io()).is_err());
                continue;
            }
            let reopened = ControlHandle::inspect(&tmp.path(), None, &io()).unwrap();
            let current = reopened.read(&io()).unwrap().unwrap();
            assert_eq!(current.end(), first.end());
            if matches!(step, PublicationStep::FileSync | PublicationStep::Rename) {
                assert!(!current.is_sealed());
                assert!(tmp.path().join("control/.endpos.tmp").exists());
                FAIL_AT.with(|slot| slot.set(Some(PublicationStep::FileSync)));
                assert!(reopened.read(&io()).is_err());
                FAIL_AT.with(|slot| assert!(slot.get().is_none()));
                FAIL_AT.with(|slot| slot.set(Some(PublicationStep::DirectorySync)));
                assert!(reopened.read(&io()).is_err());
                FAIL_AT.with(|slot| assert!(slot.get().is_none()));
            }
            assert!(
                reopened
                    .seal(&current, &io())
                    .unwrap()
                    .request()
                    .is_sealed()
            );
            assert!(!tmp.path().join("control/.endpos.tmp").exists());
        }
    }

    #[test]
    fn control_pending_forward_update_cannot_select_a_boundary() {
        let (tmp, native) = fixture();
        let control = ControlHandle::for_worker(&native, &io()).unwrap();
        control
            .set(SourceEndLsn(20), SourceEndLsn(8), &io())
            .unwrap();
        FAIL_AT.with(|slot| slot.set(Some(PublicationStep::FileSync)));
        assert!(
            control
                .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                .is_err()
        );
        FAIL_AT.with(|slot| assert!(slot.get().is_none()));
        let reopened = ControlHandle::inspect(&tmp.path(), None, &io()).unwrap();
        assert!(reopened.read(&io()).is_err());
        assert!(
            reopened
                .set(SourceEndLsn(20), SourceEndLsn(8), &io())
                .is_err()
        );
        assert_eq!(
            reopened
                .set(SourceEndLsn(30), SourceEndLsn(8), &io())
                .unwrap()
                .end(),
            SourceEndLsn(30)
        );
        assert_eq!(
            control.read(&io()).unwrap().unwrap().end(),
            SourceEndLsn(30)
        );
    }

    #[test]
    fn control_inspection_and_drop_preserve_the_native_owner_lock() {
        let (tmp, native) = fixture();
        let competitor = File::open(tmp.path()).unwrap();
        assert!(matches!(
            competitor.try_lock(),
            Err(TryLockError::WouldBlock)
        ));
        let control = ControlHandle::for_worker(&native, &io()).unwrap();
        let inspected = ControlHandle::inspect(&tmp.path(), None, &io()).unwrap();
        inspected
            .set(SourceEndLsn(20), SourceEndLsn(8), &io())
            .unwrap();
        drop(inspected);
        drop(control);
        assert!(matches!(
            competitor.try_lock(),
            Err(TryLockError::WouldBlock)
        ));
        drop(native);
        competitor.try_lock().unwrap();
        competitor.unlock().unwrap();
    }
}
