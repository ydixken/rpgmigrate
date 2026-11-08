use super::*;

const DATA: &CStr = c"data";
const PLANS: &CStr = c"plans";
const LEAF: &CStr = c"plan.db";

#[derive(Debug)]
pub(super) struct PlanNamespace {
    data: File,
    plans: File,
}

pub(crate) struct NativePlanDraft<'state> {
    owner: &'state mut NativeState,
    directory: File,
    nonce: [u8; 16],
    identity: (u64, u64),
    path: PathBuf,
}

pub(crate) struct NativePlanPin<'state> {
    owner: &'state NativeState,
    directory: File,
    nonce: [u8; 16],
    identity: (u64, u64),
    path: PathBuf,
}

fn names(nonce: [u8; 16]) -> Result<(CString, CString)> {
    if nonce == [0; 16] {
        return Err(bad_state("state-plan: plan nonce must be nonzero"));
    }
    let hex: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok((
        CString::new(format!(".{hex}.pending")).unwrap(),
        CString::new(hex).unwrap(),
    ))
}

fn open_directory(parent: &File, name: &CStr) -> Result<File> {
    let file = open_at(parent, name, DIRECTORY_FLAGS, 0)
        .map_err(|_| bad_state("state-plan: native plan directory is missing or unsafe"))?;
    check_private(&metadata(&file)?, 0o700)?;
    Ok(file)
}

fn create_directory(parent: &File, name: &CStr) -> Result<File> {
    // SAFETY: parent is live and name is an internal single NUL-terminated component.
    if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
        return Err(bad_state(
            "state-plan: cannot exclusively create native plan directory",
        ));
    }
    open_directory(parent, name)
}

fn entries(directory: &File, mut visit: impl FnMut(&CStr) -> Result<()>) -> Result<()> {
    let file = open_at(directory, c".", DIRECTORY_FLAGS, 0)
        .map_err(|_| bad_state("state-plan: cannot inspect plan directory"))?;
    let fd = file.into_raw_fd();
    // SAFETY: fd is owned; fdopendir consumes it on success.
    let stream = unsafe { libc::fdopendir(fd) };
    if stream.is_null() {
        // SAFETY: fdopendir left this owned fd open on failure.
        drop(unsafe { File::from_raw_fd(fd) });
        return Err(bad_state("state-plan: cannot scan plan directory"));
    }
    let result = loop {
        // SAFETY: stream is live and errno is thread-local.
        let entry = unsafe {
            *errno_pointer() = 0;
            libc::readdir(stream)
        };
        if entry.is_null() {
            break if io::Error::last_os_error().raw_os_error() == Some(0) {
                Ok(())
            } else {
                Err(bad_state("state-plan: cannot read plan directory entries"))
            };
        }
        // SAFETY: this entry remains live until the next readdir call.
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name != c"."
            && name != c".."
            && let Err(error) = visit(name)
        {
            break Err(error);
        }
    };
    // SAFETY: closes stream and the fd it owns exactly once.
    if unsafe { libc::closedir(stream) } != 0 {
        return Err(bad_state("state-plan: cannot close plan directory scan"));
    }
    result
}

fn leaf_identity(directory: &File) -> Result<(u64, u64)> {
    let mut stat = MaybeUninit::<libc::stat>::uninit();
    // SAFETY: stat is writable and directory/LEAF remain live.
    if unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            LEAF.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(bad_state("state-plan: plan file is missing"));
    }
    // SAFETY: successful fstatat initialized stat.
    let stat = unsafe { stat.assume_init() };
    checked_leaf_stat(&stat)
}

fn checked_leaf_stat(stat: &libc::stat) -> Result<(u64, u64)> {
    // SAFETY: geteuid has no arguments or memory preconditions.
    let uid = unsafe { libc::geteuid() };
    if stat.st_mode & libc::S_IFMT != libc::S_IFREG
        || stat.st_nlink != 1
        || stat.st_mode & 0o7777 != 0o600
        || stat.st_uid != uid
    {
        return Err(bad_state(
            "state-plan: plan file ownership or type is unsafe",
        ));
    }
    Ok((stat.st_dev as _, stat.st_ino))
}

fn only_leaf(directory: &File) -> Result<()> {
    let mut found = false;
    entries(directory, |name| {
        if name != LEAF {
            return Err(bad_state(
                "state-foreign: sealed plan contains extra entries",
            ));
        }
        found = true;
        Ok(())
    })?;
    if !found {
        return Err(bad_state("state-plan: sealed plan file is missing"));
    }
    leaf_identity(directory)?;
    Ok(())
}

impl PlanNamespace {
    pub(super) fn create(root: &Directory) -> Result<Self> {
        root.check()?;
        let data = create_directory(&root.handle, DATA)?;
        let plans = create_directory(&data, PLANS)?;
        plans
            .sync_all()
            .map_err(|_| bad_state("state-sync: cannot synchronize plans directory"))?;
        data.sync_all()
            .map_err(|_| bad_state("state-sync: cannot synchronize data directory"))?;
        root.handle
            .sync_all()
            .map_err(|_| bad_state("state-sync: cannot synchronize native data entry"))?;
        let namespace = Self { data, plans };
        namespace.check(root, None)?;
        Ok(namespace)
    }

    pub(super) fn open(root: &Directory) -> Result<Self> {
        let data = open_directory(&root.handle, DATA)?;
        let plans = open_directory(&data, PLANS)?;
        let namespace = Self { data, plans };
        namespace.check(root, None)?;
        Ok(namespace)
    }

    pub(super) fn check(&self, root: &Directory, draft: Option<[u8; 16]>) -> Result<()> {
        root.check()?;
        check_same(&self.data, &open_directory(&root.handle, DATA)?)?;
        check_same(&self.plans, &open_directory(&self.data, PLANS)?)?;
        entries(&self.data, |name| {
            if name == PLANS {
                Ok(())
            } else {
                Err(bad_state(
                    "state-foreign: native data contains unrecognized entries",
                ))
            }
        })?;
        let pending = draft.map(names).transpose()?.map(|names| names.0);
        entries(&self.plans, |name| {
            if pending
                .as_ref()
                .is_some_and(|pending| pending.as_c_str() == name)
            {
                open_directory(&self.plans, name)?;
                return Ok(());
            }
            if name.to_bytes().len() != 32
                || !name
                    .to_bytes()
                    .iter()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
                || name.to_bytes().iter().all(|b| *b == b'0')
            {
                return Err(bad_state(
                    "state-foreign: native plans contain unrecognized entries",
                ));
            }
            let object = open_directory(&self.plans, name)?;
            only_leaf(&object)
        })
    }
}

impl NativeState {
    pub(crate) fn create_plan_file(
        &mut self,
        nonce: [u8; 16],
    ) -> Result<super::NativePlanDraft<'_>> {
        let (pending, sealed) = names(nonce)?;
        self.checked_manifest()?;
        match open_at(&self.plans.plans, &sealed, DIRECTORY_FLAGS, 0) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            _ => {
                return Err(bad_state(
                    "state-plan-exists: final plan entry already exists or is unsafe",
                ));
            }
        }
        let directory = create_directory(&self.plans.plans, &pending)?;
        let file = open_at(
            &directory,
            LEAF,
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )
        .map_err(|_| bad_state("state-plan: cannot reserve native plan file"))?;
        check_private(&metadata(&file)?, 0o600)?;
        let identity = leaf_identity(&directory)?;
        drop(file);
        let path = self
            .identity
            .canonical_path
            .join("data/plans")
            .join(pending.to_str().unwrap())
            .join("plan.db");
        Ok(NativePlanDraft {
            owner: self,
            directory,
            nonce,
            identity,
            path,
        })
    }

    pub(crate) fn open_plan_file(&self, nonce: [u8; 16]) -> Result<super::NativePlanPin<'_>> {
        let (_, sealed) = names(nonce)?;
        self.checked_manifest()?;
        let directory = open_directory(&self.plans.plans, &sealed)?;
        only_leaf(&directory)?;
        let identity = leaf_identity(&directory)?;
        let path = self
            .identity
            .canonical_path
            .join("data/plans")
            .join(sealed.to_str().unwrap())
            .join("plan.db");
        Ok(NativePlanPin {
            owner: self,
            directory,
            nonce,
            identity,
            path,
        })
    }
}

impl NativePlanDraft<'_> {
    pub(crate) fn checked_path(&self) -> Result<&Path> {
        self.owner.checked_manifest_with_draft(Some(self.nonce))?;
        let (pending, _) = names(self.nonce)?;
        check_same(
            &self.directory,
            &open_directory(&self.owner.plans.plans, &pending)?,
        )?;
        if leaf_identity(&self.directory)? != self.identity {
            return Err(bad_state("state-replaced: draft plan file changed"));
        }
        Ok(&self.path)
    }

    /// The adapter must close its database connection successfully before calling this method.
    pub(crate) fn publish(self) -> Result<()> {
        self.checked_path()?;
        only_leaf(&self.directory)?;
        let file = open_metadata(&self.directory, LEAF)?;
        #[cfg(test)]
        tests::step(tests::Step::FileSync)?;
        file.sync_all()
            .map_err(|_| bad_state("state-sync: cannot synchronize plan file"))?;
        #[cfg(test)]
        tests::step(tests::Step::ObjectSync)?;
        self.directory
            .sync_all()
            .map_err(|_| bad_state("state-sync: cannot synchronize plan object directory"))?;
        self.checked_path()?;
        let (pending, sealed) = names(self.nonce)?;
        #[cfg(test)]
        tests::before_publish(&self.owner.plans.plans, &sealed)?;
        publish_plan_directory(&self.owner.plans.plans, &pending, &sealed)?;
        #[cfg(test)]
        tests::step(tests::Step::ParentSync)?;
        self.owner
            .plans
            .plans
            .sync_all()
            .map_err(|_| bad_state("state-sync: cannot synchronize plan publication"))?;
        #[cfg(test)]
        tests::step(tests::Step::Published)?;
        self.owner.checked_manifest()?;
        check_same(
            &self.directory,
            &open_directory(&self.owner.plans.plans, &sealed)?,
        )?;
        if leaf_identity(&self.directory)? != self.identity {
            return Err(bad_state("state-replaced: published plan file changed"));
        }
        Ok(())
    }
}

impl NativePlanPin<'_> {
    pub(crate) fn check(&self) -> Result<()> {
        self.owner.checked_manifest()?;
        let (_, sealed) = names(self.nonce)?;
        check_same(
            &self.directory,
            &open_directory(&self.owner.plans.plans, &sealed)?,
        )?;
        only_leaf(&self.directory)?;
        if leaf_identity(&self.directory)? != self.identity {
            return Err(bad_state("state-replaced: sealed plan file changed"));
        }
        Ok(())
    }

    pub(crate) fn checked_path(&self) -> Result<&Path> {
        self.check()?;
        Ok(&self.path)
    }
}

fn rename_no_replace(plans: &File, pending: &CStr, sealed: &CStr) -> io::Result<()> {
    #[cfg(test)]
    if let Some(errno) = tests::take_errno() {
        return Err(io::Error::from_raw_os_error(errno));
    }
    #[cfg(target_os = "macos")]
    // SAFETY: plans is pinned; both internal names are single NUL-terminated components.
    let result = unsafe {
        libc::renameatx_np(
            plans.as_raw_fd(),
            pending.as_ptr(),
            plans.as_raw_fd(),
            sealed.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(target_os = "linux")]
    // SAFETY: plans is pinned; both internal names are single NUL-terminated components.
    let result = unsafe {
        libc::renameat2(
            plans.as_raw_fd(),
            pending.as_ptr(),
            plans.as_raw_fd(),
            sealed.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let result = {
        let _ = (plans, pending, sealed);
        return Err(io::Error::from(io::ErrorKind::Unsupported));
    };
    if result == 0 {
        return Ok(());
    }
    Err(io::Error::last_os_error())
}

fn publish_plan_directory(plans: &File, pending: &CStr, sealed: &CStr) -> Result<()> {
    let error = match rename_no_replace(plans, pending, sealed) {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    let errno = error.raw_os_error();
    if errno == Some(libc::EEXIST) {
        Err(bad_state(
            "state-plan-exists: atomic publication refused an existing final entry",
        ))
    } else if error.kind() == io::ErrorKind::Unsupported
        || errno.is_some_and(|value| {
            [libc::ENOSYS, libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP].contains(&value)
        })
    {
        Err(bad_state(
            "state-plan-unsupported: atomic non-replacing publication is unavailable",
        ))
    } else {
        Err(bad_state(
            "state-plan: atomic non-replacing publication failed",
        ))
    }
}

#[cfg(test)]
mod tests;
