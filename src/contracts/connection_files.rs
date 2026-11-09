#[cfg(target_os = "linux")]
use std::collections::BTreeMap;
use std::ffi::{CStr, CString, OsString};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, OwnedFd};
use std::process::Command;
use std::sync::Arc;

use serde::{Deserialize, Deserializer};

use super::{CloneOptions, Error, ErrorKind, Result};

pub(crate) const SOURCE_FILES_ENV: &str = "RPGMIGRATE_INTERNAL_COMPARE_SOURCE_FILES";
pub(crate) const TARGET_FILES_ENV: &str = "RPGMIGRATE_INTERNAL_COMPARE_TARGET_FILES";
#[cfg(target_os = "linux")]
const ABSENT_FILE: &str = "/usr/share/rpgmigrate/.native-file-absent";
const MAX_MAP_BYTES: usize = 1024;
#[cfg(target_os = "linux")]
const MAX_FILE_BYTES: u64 = 1024 * 1024;

#[derive(Default)]
enum Selection {
    #[default]
    Unselected,
    Absent,
    Descriptor(i32),
}

impl<'de> Deserialize<'de> for Selection {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        match Option::<i32>::deserialize(deserializer)? {
            Some(fd) => Ok(Self::Descriptor(fd)),
            None => Ok(Self::Absent),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileMap {
    #[serde(default)]
    passfile: Selection,
    #[serde(default)]
    sslrootcert: Selection,
    #[serde(default)]
    sslcert: Selection,
    #[serde(default)]
    sslkey: Selection,
    #[serde(default)]
    sslcrl: Selection,
}

impl FileMap {
    fn parse(value: OsString) -> Result<Self> {
        let bytes = value.into_encoded_bytes();
        if bytes.len() > MAX_MAP_BYTES
            || bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{')
        {
            return Err(invalid());
        }
        let map: Self = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if map
            .entries()
            .iter()
            .any(|(_, selection)| matches!(selection, Selection::Descriptor(fd) if *fd < 3))
        {
            return Err(invalid());
        }
        Ok(map)
    }

    fn entries(&self) -> [(&'static CStr, &Selection); 5] {
        [
            (c"passfile", &self.passfile),
            (c"sslrootcert", &self.sslrootcert),
            (c"sslcert", &self.sslcert),
            (c"sslkey", &self.sslkey),
            (c"sslcrl", &self.sslcrl),
        ]
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Eq, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    size: u64,
}

#[cfg(target_os = "linux")]
struct SealedDescriptor {
    fd: OwnedFd,
    identity: FileIdentity,
}

pub(crate) struct ConnectionFiles {
    options: Vec<(&'static CStr, CString)>,
    #[cfg(target_os = "linux")]
    descriptors: Vec<SealedDescriptor>,
    #[cfg(target_os = "linux")]
    has_absence: bool,
}

pub(crate) fn bind_compare(options: &CloneOptions) -> Result<CloneOptions> {
    bind_values(
        options,
        std::env::var_os(SOURCE_FILES_ENV),
        std::env::var_os(TARGET_FILES_ENV),
    )
}

fn bind_values(
    options: &CloneOptions,
    source: Option<OsString>,
    target: Option<OsString>,
) -> Result<CloneOptions> {
    let (source, target) = match (source, target) {
        (None, None) => return Ok(options.clone()),
        (Some(source), Some(target)) => (FileMap::parse(source)?, FileMap::parse(target)?),
        _ => return Err(invalid()),
    };
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (source, target);
        Err(unsupported())
    }
    #[cfg(target_os = "linux")]
    {
        check_kernel()?;
        let mut inputs = BTreeMap::new();
        for (_, selection) in source.entries().into_iter().chain(target.entries()) {
            match selection {
                Selection::Descriptor(fd) => {
                    if let std::collections::btree_map::Entry::Vacant(entry) = inputs.entry(*fd) {
                        entry.insert(inspect(*fd)?);
                    }
                }
                Selection::Absent => check_absence(std::path::Path::new(ABSENT_FILE))?,
                Selection::Unselected => {}
            }
        }
        let source = Arc::new(ConnectionFiles::duplicate(source, &inputs)?);
        let target = Arc::new(ConnectionFiles::duplicate(target, &inputs)?);
        for fd in inputs.keys() {
            // SAFETY: fcntl validates the borrowed descriptor without taking ownership.
            let flags = unsafe { libc::fcntl(*fd, libc::F_GETFD) };
            if flags < 0
                // SAFETY: only close-on-exec is changed on this validated borrowed descriptor.
                || unsafe { libc::fcntl(*fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0
            {
                return Err(invalid());
            }
        }
        let mut bound = options.clone();
        bound.source.files = Some(source);
        bound.target.files = Some(target);
        Ok(bound)
    }
}

impl ConnectionFiles {
    pub(crate) fn options(&self) -> impl Iterator<Item = (&CStr, &CStr)> {
        self.options
            .iter()
            .map(|(key, value)| (*key, value.as_c_str()))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        #[cfg(not(target_os = "linux"))]
        return Err(unsupported());
        #[cfg(target_os = "linux")]
        {
            for descriptor in &self.descriptors {
                if inspect(descriptor.fd.as_raw_fd())? != descriptor.identity {
                    return Err(invalid());
                }
            }
            if self.has_absence {
                check_absence(std::path::Path::new(ABSENT_FILE))?;
            }
            Ok(())
        }
    }

    pub(crate) fn inherit_only(command: &mut Command, files: Option<Arc<Self>>) -> Result<()> {
        command
            .env_remove(SOURCE_FILES_ENV)
            .env_remove(TARGET_FILES_ENV);
        if let Some(files) = &files {
            files.validate()?;
        }
        #[cfg(not(target_os = "linux"))]
        return Err(unsupported());
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::CommandExt;
            // SAFETY: after fork the closure only reads retained owners and calls Linux syscalls.
            unsafe {
                command.pre_exec(move || {
                    if libc::close_range(3, u32::MAX, libc::CLOSE_RANGE_CLOEXEC as libc::c_int) != 0
                    {
                        return Err(std::io::Error::last_os_error());
                    }
                    if let Some(files) = &files {
                        for descriptor in &files.descriptors {
                            let fd = descriptor.fd.as_raw_fd();
                            let flags = libc::fcntl(fd, libc::F_GETFD);
                            if flags < 0
                                || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0
                            {
                                return Err(std::io::Error::last_os_error());
                            }
                        }
                    }
                    Ok(())
                });
            }
            Ok(())
        }
    }

    #[cfg(target_os = "linux")]
    fn duplicate(map: FileMap, inputs: &BTreeMap<i32, FileIdentity>) -> Result<Self> {
        use std::os::fd::FromRawFd;
        let mut files = Self {
            options: Vec::new(),
            descriptors: Vec::new(),
            has_absence: false,
        };
        let mut duplicates = BTreeMap::new();
        for (key, selection) in map.entries() {
            let value = match selection {
                Selection::Unselected => continue,
                Selection::Absent => {
                    files.has_absence = true;
                    CString::new(ABSENT_FILE).expect("fixed absence path has no NUL")
                }
                Selection::Descriptor(input) => {
                    let fd = if let Some(fd) = duplicates.get(input) {
                        *fd
                    } else {
                        // SAFETY: fcntl returns a new descriptor owned exclusively by this result.
                        let fd = unsafe { libc::fcntl(*input, libc::F_DUPFD_CLOEXEC, 3) };
                        if fd < 0 {
                            return Err(invalid());
                        }
                        // SAFETY: the successful duplication transferred sole ownership of fd.
                        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
                        let identity = inspect(fd)?;
                        if inputs.get(input) != Some(&identity) {
                            return Err(invalid());
                        }
                        files.descriptors.push(SealedDescriptor {
                            fd: owned,
                            identity,
                        });
                        duplicates.insert(*input, fd);
                        fd
                    };
                    CString::new(format!("/proc/self/fd/{fd}")).expect("descriptor path has no NUL")
                }
            };
            files.options.push((key, value));
        }
        Ok(files)
    }
}

#[cfg(target_os = "linux")]
fn inspect(fd: i32) -> Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt;
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat initializes metadata only on success; invalid descriptors are kernel errors.
    if unsafe { libc::fstat(fd, metadata.as_mut_ptr()) } != 0 {
        return Err(invalid());
    }
    // SAFETY: successful fstat initialized every field.
    let metadata = unsafe { metadata.assume_init() };
    // SAFETY: geteuid has no preconditions and no ownership effect.
    let identity = validate_metadata(&metadata, unsafe { libc::geteuid() })?;
    let required = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    // SAFETY: fcntl checks this borrowed descriptor without modifying it.
    let seals = unsafe { libc::fcntl(fd, libc::F_GET_SEALS) };
    if seals < 0 || seals & required != required {
        return Err(invalid());
    }
    let path = std::fs::metadata(format!("/proc/self/fd/{fd}")).map_err(|_| invalid())?;
    if path.dev() != identity.device || path.ino() != identity.inode || path.len() != identity.size
    {
        return Err(invalid());
    }
    Ok(identity)
}

#[cfg(target_os = "linux")]
fn check_kernel() -> Result<()> {
    // UINT_MAX cannot name a RawFd, so this probes the flag without changing any descriptor.
    // SAFETY: Linux checks this empty descriptor range and rejects unsupported flags/syscalls.
    if unsafe { libc::close_range(u32::MAX, u32::MAX, libc::CLOSE_RANGE_CLOEXEC as libc::c_int) }
        != 0
    {
        return Err(Error::new(
            ErrorKind::BadConfig,
            "internal comparison descriptor isolation is unavailable",
        ));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_metadata(metadata: &libc::stat, owner: libc::uid_t) -> Result<FileIdentity> {
    if metadata.st_mode & libc::S_IFMT != libc::S_IFREG
        || metadata.st_mode & 0o7777 != 0o400
        || metadata.st_uid != owner
        || metadata.st_nlink != 0
        || metadata.st_size < 0
        || metadata.st_size as u64 > MAX_FILE_BYTES
    {
        return Err(invalid());
    }
    Ok(FileIdentity {
        device: metadata.st_dev,
        inode: metadata.st_ino,
        size: metadata.st_size as u64,
    })
}

#[cfg(target_os = "linux")]
fn check_absence(path: &std::path::Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => Err(invalid()),
    }
}

fn invalid() -> Error {
    Error::new(
        ErrorKind::BadConfig,
        "internal comparison connection files are invalid; details redacted",
    )
}

#[cfg(not(target_os = "linux"))]
fn unsupported() -> Error {
    Error::new(
        ErrorKind::BadConfig,
        "internal comparison connection files require Linux",
    )
}

#[cfg(test)]
pub(crate) mod tests;
