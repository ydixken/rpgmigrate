use super::*;

fn options() -> CloneOptions {
    CloneOptions {
        source: super::super::Conninfo::new("host=source.invalid".into()).unwrap(),
        target: super::super::Conninfo::new("host=target.invalid".into()).unwrap(),
        state: super::super::StateRequest {
            path: std::env::temp_dir().join("rpgmigrate-files-unused"),
            action: super::super::StateAction::Create,
        },
        operator: None,
        table_jobs: 1,
        post_data_jobs: 1,
        policy: super::super::RestorePolicy::default(),
        not_consistent: false,
        use_copy_binary: false,
        split_tables_larger_than: 0,
        split_max_parts: 8,
        skip_large_objects: false,
        large_objects_jobs: 4,
    }
}

#[test]
fn absent_maps_preserve_native_conninfo_and_attach_no_files() {
    use std::os::unix::ffi::OsStringExt;
    let mut options = options();
    options.source = super::super::Conninfo::new(OsString::from_vec(
        b"host=private-\xff passfile=''".to_vec(),
    ))
    .unwrap();
    let bound = bind_values(&options, None, None).unwrap();
    assert_eq!(
        bound.source.as_c_str().to_bytes(),
        b"host=private-\xff passfile=''"
    );
    assert_eq!(bound.target.as_c_str().to_bytes(), b"host=target.invalid");
    assert!(bound.source.files().is_none());
    assert!(bound.target.files().is_none());
}

#[test]
fn file_maps_reject_unknown_duplicate_and_invalid_values() {
    for value in [
        "",
        "[]",
        "null",
        "{}{}",
        "{\"passfile\":2}",
        "{\"passfile\":-1}",
        "{\"passfile\":2147483648}",
        "{\"passfile\":3.0}",
        "{\"passfile\":true}",
        "{\"passfile\":\"3\"}",
        "{\"passfile\":{\"fd\":3}}",
        "{\"service\":3}",
        "{\"sslcrldir\":3}",
        "{\"password\":3}",
        "{\"passfile\":3,\"passfile\":4}",
        "{\"passfile\":null,\"passfile\":null}",
    ] {
        assert!(FileMap::parse(value.into()).is_err(), "{value}");
    }
    assert!(FileMap::parse(OsString::from(" ".repeat(1025))).is_err());
    assert!(FileMap::parse("{}".into()).is_ok());
    assert!(FileMap::parse("{\"passfile\":null,\"sslkey\":3,\"sslcert\":3}".into()).is_ok());
}

#[test]
fn partial_map_presence_is_refused() {
    let options = options();
    assert!(bind_values(&options, Some("{}".into()), None).is_err());
    assert!(bind_values(&options, None, Some("{}".into())).is_err());
}

#[test]
#[cfg(target_os = "linux")]
fn stable_absence_requires_a_missing_path_and_refuses_dangling_symlinks() {
    let path = std::env::temp_dir().join(format!(
        "rpgmigrate-absence-fixture-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    assert!(check_absence(&path).is_ok());
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    assert!(check_absence(&path).is_err());
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink("rpgmigrate-deliberately-missing", &path).unwrap();
    assert!(check_absence(&path).is_err());
    std::fs::remove_file(&path).unwrap();
    assert!(check_absence(&path).is_ok());
}

#[test]
#[cfg(not(target_os = "linux"))]
fn present_maps_refuse_on_non_linux_even_when_empty() {
    let error = bind_values(&options(), Some("{}".into()), Some("{}".into())).unwrap_err();
    assert_eq!(error.kind, ErrorKind::BadConfig);
    assert!(error.message.contains("require Linux"));
}

#[cfg(target_os = "linux")]
pub(crate) fn sealed(bytes: &[u8]) -> OwnedFd {
    use std::os::fd::FromRawFd;
    // SAFETY: a terminated name and supported flags create one owned descriptor.
    let fd = unsafe {
        libc::memfd_create(
            c"rpgmigrate-file".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    assert!(fd >= 3);
    // SAFETY: successful memfd_create returned sole ownership of this descriptor.
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    let mut written = 0;
    while written < bytes.len() {
        // SAFETY: the slice remains readable throughout this bounded write.
        let count =
            unsafe { libc::write(fd, bytes[written..].as_ptr().cast(), bytes.len() - written) };
        assert!(count > 0);
        written += count as usize;
    }
    // SAFETY: these calls only change the owned descriptor's mode and immutable seals.
    unsafe {
        assert_eq!(libc::fchmod(fd, 0o400), 0);
        assert_eq!(
            libc::fcntl(
                fd,
                libc::F_ADD_SEALS,
                libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL
            ),
            0
        );
    }
    owned
}

#[cfg(target_os = "linux")]
pub(crate) fn bound_source(raw: &str, source: &str, target: &str) -> CloneOptions {
    let mut options = options();
    options.source = super::super::Conninfo::new(raw.into()).unwrap();
    bind_values(&options, Some(source.into()), Some(target.into())).unwrap()
}

#[test]
#[cfg(target_os = "linux")]
fn sealed_shared_descriptors_are_per_role_owned_and_clone_safe() {
    let input = sealed(b"private-fixture");
    let fd = input.as_raw_fd();
    let source = format!("{{\"sslcert\":{fd},\"sslkey\":{fd}}}");
    let target = format!("{{\"passfile\":{fd}}}");
    let bound = bound_source("host=source.invalid", &source, &target);
    let source_files = bound.source.files().unwrap();
    let target_files = bound.target.files().unwrap();
    assert_eq!(source_files.descriptors.len(), 1);
    assert_eq!(target_files.descriptors.len(), 1);
    let source_fd = source_files.descriptors[0].fd.as_raw_fd();
    let target_fd = target_files.descriptors[0].fd.as_raw_fd();
    let identity = source_files.descriptors[0].identity;
    assert_ne!(source_fd, target_fd);
    assert_eq!(source_files.options[0].1, source_files.options[1].1);
    // SAFETY: F_GETFD only inspects the three live descriptors.
    unsafe {
        for fd in [fd, source_fd, target_fd] {
            assert_ne!(libc::fcntl(fd, libc::F_GETFD) & libc::FD_CLOEXEC, 0);
        }
    }
    let clone = bound.source.clone();
    drop(input);
    drop(bound);
    clone.files().unwrap().validate().unwrap();
    assert!(inspect(target_fd).ok() != Some(identity));
    assert!(inspect(source_fd).ok() == Some(identity));
    drop(clone);
    assert!(inspect(source_fd).ok() != Some(identity));
}

#[test]
#[cfg(target_os = "linux")]
fn all_maps_parse_before_borrowed_descriptor_flags_change() {
    let input = sealed(b"fixture");
    let fd = input.as_raw_fd();
    // SAFETY: only the fixture's inheritance bit is changed.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_SETFD, 0) }, 0);
    let source = format!("{{\"passfile\":{fd}}}");
    assert!(
        bind_values(
            &options(),
            Some(source.into()),
            Some("{\"unknown\":3}".into())
        )
        .is_err()
    );
    // SAFETY: the failed binding must not have changed this live fixture descriptor.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, 0);
}

#[test]
#[cfg(target_os = "linux")]
fn descriptor_admission_refuses_missing_type_mode_owner_and_size() {
    assert!(inspect(i32::MAX).is_err());
    let wrong_type = std::fs::File::open("/dev/null").unwrap();
    assert!(inspect(wrong_type.as_raw_fd()).is_err());
    let wrong_mode = sealed(b"fixture");
    // SAFETY: fchmod changes only the owned fixture's metadata.
    assert_eq!(unsafe { libc::fchmod(wrong_mode.as_raw_fd(), 0o600) }, 0);
    assert!(inspect(wrong_mode.as_raw_fd()).is_err());
    let input = sealed(b"fixture");
    let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: successful fstat initializes the metadata below.
    assert_eq!(
        unsafe { libc::fstat(input.as_raw_fd(), metadata.as_mut_ptr()) },
        0
    );
    // SAFETY: fstat succeeded above.
    let metadata = unsafe { metadata.assume_init() };
    assert!(validate_metadata(&metadata, metadata.st_uid.wrapping_add(1)).is_err());
    let oversized = sealed(&vec![0; 1024 * 1024 + 1]);
    assert!(inspect(oversized.as_raw_fd()).is_err());
    for bytes in [Vec::new(), vec![0; 1024 * 1024]] {
        let valid = sealed(&bytes);
        assert!(inspect(valid.as_raw_fd()).is_ok());
    }
}

#[test]
#[cfg(target_os = "linux")]
fn descriptor_admission_requires_each_immutable_seal() {
    use std::os::fd::FromRawFd;
    let required = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    for missing in [
        libc::F_SEAL_WRITE,
        libc::F_SEAL_GROW,
        libc::F_SEAL_SHRINK,
        libc::F_SEAL_SEAL,
    ] {
        // SAFETY: the terminated name creates one owned sealable memfd.
        let fd = unsafe {
            libc::memfd_create(
                c"rpgmigrate-file".as_ptr(),
                libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
            )
        };
        assert!(fd >= 3);
        // SAFETY: successful creation transferred sole descriptor ownership.
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        // SAFETY: only the owned fixture is configured and sealed.
        unsafe {
            assert_eq!(libc::fchmod(fd, 0o400), 0);
            assert_eq!(libc::fcntl(fd, libc::F_ADD_SEALS, required & !missing), 0);
        }
        assert!(inspect(owned.as_raw_fd()).is_err());
    }
}

#[test]
#[cfg(target_os = "linux")]
fn kernel_probe_preserves_descriptor_inventory_and_flags() {
    const CHILD: &str = "RPGMIGRATE_TEST_COMPARE_KERNEL_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "contracts::connection_files::tests::kernel_probe_preserves_descriptor_inventory_and_flags", "--nocapture"])
            .env(CHILD, "1")
            .env_remove(SOURCE_FILES_ENV).env_remove(TARGET_FILES_ENV)
            .output().unwrap();
        assert!(output.status.success(), "{output:?}");
        return;
    }
    fn inventory() -> Vec<OsString> {
        let mut entries: Vec<_> = std::fs::read_dir("/proc/self/fd")
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        entries.sort();
        entries
    }
    let fixture = sealed(b"fixture");
    let fd = fixture.as_raw_fd();
    // SAFETY: the probe must preserve the owned fixture's explicit inheritable state.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_SETFD, 0) }, 0);
    let before = inventory();
    check_kernel().unwrap();
    assert_eq!(inventory(), before);
    // SAFETY: F_GETFD inspects the live fixture without changing it.
    assert_eq!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, 0);
}

#[test]
#[cfg(target_os = "linux")]
fn utility_descendants_inherit_only_their_unique_role_descriptors() {
    const CHILD: &str = "RPGMIGRATE_TEST_COMPARE_INHERIT_CHILD";
    if let Some(probe) = std::env::var_os(CHILD) {
        let probe: serde_json::Value = serde_json::from_slice(probe.as_encoded_bytes()).unwrap();
        assert!(std::env::var_os(SOURCE_FILES_ENV).is_none());
        assert!(std::env::var_os(TARGET_FILES_ENV).is_none());
        for expected in probe.as_array().unwrap() {
            let fd = expected[0].as_i64().unwrap() as i32;
            let device = expected[1].as_u64().unwrap();
            let inode = expected[2].as_u64().unwrap();
            let open =
                inspect(fd).is_ok_and(|actual| actual.device == device && actual.inode == inode);
            assert_eq!(open, expected[3].as_bool().unwrap(), "descriptor {fd}");
        }
        return;
    }
    let shared = sealed(b"shared-combined-pem-fixture");
    let other = sealed(b"target-only-fixture");
    let ambient = sealed(b"unrelated-fixture");
    // SAFETY: deliberately make the unrelated fixture inheritable to exercise isolation.
    assert_eq!(
        unsafe { libc::fcntl(ambient.as_raw_fd(), libc::F_SETFD, 0) },
        0
    );
    let bound = bound_source(
        "host=source.invalid",
        &format!(
            "{{\"sslcert\":{},\"sslkey\":{}}}",
            shared.as_raw_fd(),
            shared.as_raw_fd()
        ),
        &format!(
            "{{\"passfile\":{},\"sslcert\":{}}}",
            shared.as_raw_fd(),
            other.as_raw_fd()
        ),
    );
    let source = bound.source.files().unwrap();
    let target = bound.target.files().unwrap();
    for selected in [Some(source), Some(target), None] {
        let mut probe = Vec::new();
        for files in [source, target] {
            for descriptor in &files.descriptors {
                let expected = selected.is_some_and(|selected| Arc::ptr_eq(selected, files));
                probe.push(serde_json::json!([
                    descriptor.fd.as_raw_fd(),
                    descriptor.identity.device,
                    descriptor.identity.inode,
                    expected
                ]));
            }
        }
        for fd in [shared.as_raw_fd(), other.as_raw_fd(), ambient.as_raw_fd()] {
            let identity = inspect(fd).unwrap();
            probe.push(serde_json::json!([
                fd,
                identity.device,
                identity.inode,
                false
            ]));
        }
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", "contracts::connection_files::tests::utility_descendants_inherit_only_their_unique_role_descriptors", "--nocapture"])
            .env(CHILD, serde_json::to_string(&probe).unwrap())
            .env(SOURCE_FILES_ENV, "private-map-must-be-removed")
            .env(TARGET_FILES_ENV, "private-map-must-be-removed");
        ConnectionFiles::inherit_only(&mut command, selected.cloned()).unwrap();
        let output = command.output().unwrap();
        assert!(output.status.success(), "{output:?}");
    }
}
