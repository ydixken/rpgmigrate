use super::*;
use crate::state::tests::TempDir;
use std::cell::Cell;
use std::os::unix::fs::{PermissionsExt, symlink};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Step {
    FileSync,
    ObjectSync,
    Rename,
    ParentSync,
    Published,
}

thread_local! {
    static FAIL: Cell<Option<Step>> = const { Cell::new(None) };
    static COLLIDE: Cell<bool> = const { Cell::new(false) };
    static COLLISION_ID: Cell<Option<(u64, u64)>> = const { Cell::new(None) };
    static ERRNO: Cell<Option<i32>> = const { Cell::new(None) };
}

pub(super) fn take_errno() -> Option<i32> {
    ERRNO.with(|slot| slot.take())
}

pub(super) fn step(step: Step) -> Result<()> {
    FAIL.with(|slot| {
        if slot.get() == Some(step) {
            slot.set(None);
            Err(bad_state("state-plan-test: injected publication failure"))
        } else {
            Ok(())
        }
    })
}

pub(super) fn before_publish(plans: &File, sealed: &CStr) -> Result<()> {
    if COLLIDE.with(|slot| slot.replace(false)) {
        let existing = create_directory(plans, sealed)?;
        let meta = metadata(&existing)?;
        COLLISION_ID.with(|slot| slot.set(Some((meta.dev(), meta.ino()))));
    }
    step(Step::Rename)
}

#[test]
fn draft_publication_and_pinned_reopen_preserve_bytes() {
    let tmp = TempDir::new();
    let mut state = tmp.create();
    let draft = state.create_plan_file([7; 16]).unwrap();
    let path = draft.checked_path().unwrap().to_owned();
    fs::write(&path, b"opaque plan bytes").unwrap();
    let identity = draft.identity;
    draft.publish().unwrap();
    assert!(!path.exists());
    let pin = state.open_plan_file([7; 16]).unwrap();
    assert_eq!(pin.identity, identity);
    assert_eq!(
        fs::read(pin.checked_path().unwrap()).unwrap(),
        b"opaque plan bytes"
    );
    drop(pin);
    drop(state);
    // Local namespace reopening supplies no seal-content or source-snapshot proof.
    let reopened = open_for_provenance_recovery(&tmp.path()).unwrap_err();
    assert!(reopened.message.starts_with("state-provenance:"));
}

#[test]
fn atomic_publication_refuses_an_empty_final_directory_without_replacing_its_inode() {
    let tmp = TempDir::new();
    let mut state = tmp.create();
    let draft = state.create_plan_file([9; 16]).unwrap();
    let (_, sealed) = names([9; 16]).unwrap();
    let final_path = tmp.path().join("data/plans").join(sealed.to_str().unwrap());
    fs::write(draft.checked_path().unwrap(), b"retained draft").unwrap();
    COLLIDE.with(|slot| assert!(!slot.replace(true)));
    let error = draft.publish().unwrap_err();
    assert!(error.message.starts_with("state-plan-exists:"), "{error:?}");
    assert!(final_path.is_dir());
    assert_eq!(fs::read_dir(&final_path).unwrap().count(), 0);
    let expected = COLLISION_ID
        .with(|slot| slot.take())
        .expect("collision hook did not run");
    let actual = fs::metadata(&final_path).unwrap();
    assert_eq!((actual.dev(), actual.ino()), expected);
    let (pending, _) = names([9; 16]).unwrap();
    assert_eq!(
        fs::read(
            tmp.path()
                .join("data/plans")
                .join(pending.to_str().unwrap())
                .join("plan.db")
        )
        .unwrap(),
        b"retained draft"
    );
}

#[test]
fn publication_faults_return_no_success_and_never_clean_artifacts() {
    for point in [
        Step::FileSync,
        Step::ObjectSync,
        Step::Rename,
        Step::ParentSync,
        Step::Published,
    ] {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let draft = state.create_plan_file([3; 16]).unwrap();
        fs::write(draft.checked_path().unwrap(), b"retained").unwrap();
        FAIL.with(|slot| assert!(slot.replace(Some(point)).is_none()));
        let error = draft.publish().unwrap_err();
        assert!(
            error.message.starts_with("state-plan-test:"),
            "{point:?}: {error:?}"
        );
        assert_eq!(FAIL.with(|slot| slot.get()), None);
        let (pending, sealed) = names([3; 16]).unwrap();
        let published = matches!(point, Step::ParentSync | Step::Published);
        let object = if published { sealed } else { pending };
        assert_eq!(
            fs::read(
                tmp.path()
                    .join("data/plans")
                    .join(object.to_str().unwrap())
                    .join("plan.db")
            )
            .unwrap(),
            b"retained"
        );
        drop(state);
        if !published {
            let error = open_for_provenance_recovery(&tmp.path()).unwrap_err();
            assert!(error.message.starts_with("state-foreign:"), "{error:?}");
        }
    }
}

#[test]
fn draft_drop_is_not_publication_and_public_reopen_refuses_pending() {
    let tmp = TempDir::new();
    let mut state = tmp.create();
    let path = state
        .create_plan_file([4; 16])
        .unwrap()
        .checked_path()
        .unwrap()
        .to_owned();
    assert!(path.is_file());
    assert!(state.checked_manifest().is_err());
    assert!(state.open_plan_file([4; 16]).is_err());
    drop(state);
    let before = fs::read(&path).unwrap();
    assert!(open_for_provenance_recovery(&tmp.path()).is_err());
    assert_eq!(fs::read(path).unwrap(), before);
}

#[test]
fn foreign_data_before_first_manifest_is_not_adopted() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path()).unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();
    fs::create_dir(tmp.path().join("data")).unwrap();
    let inode = fs::metadata(tmp.path().join("data")).unwrap().ino();
    let error = open(&StateRequest {
        path: tmp.path(),
        action: StateAction::Create,
    })
    .unwrap_err();
    assert!(error.message.starts_with("state-foreign:"), "{error:?}");
    assert!(!tmp.path().join("manifest.json").exists());
    assert_eq!(fs::metadata(tmp.path().join("data")).unwrap().ino(), inode);
}

#[test]
fn missing_and_zero_nonce_open_never_creates_an_object() {
    let tmp = TempDir::new();
    let mut state = tmp.create();
    assert!(state.create_plan_file([0; 16]).is_err());
    assert!(state.open_plan_file([0; 16]).is_err());
    assert!(state.open_plan_file([7; 16]).is_err());
    assert_eq!(
        fs::read_dir(tmp.path().join("data/plans")).unwrap().count(),
        0
    );
}

#[test]
fn pinned_checks_refuse_replacement_symlinks_permissions_and_hardlinks() {
    for variant in 0..5 {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        state.create_plan_file([8; 16]).unwrap().publish().unwrap();
        let pin = state.open_plan_file([8; 16]).unwrap();
        let path = pin.checked_path().unwrap().to_owned();
        let inode = fs::metadata(&path).unwrap().ino();
        match variant {
            0 => {
                fs::rename(&path, tmp.0.join("original")).unwrap();
                fs::write(&path, b"replacement").unwrap();
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
            }
            1 => {
                fs::rename(&path, tmp.0.join("original")).unwrap();
                symlink(tmp.0.join("original"), &path).unwrap();
            }
            2 => fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap(),
            3 => fs::hard_link(&path, tmp.0.join("linked")).unwrap(),
            _ => {
                let object = path.parent().unwrap();
                fs::rename(object, tmp.0.join("object")).unwrap();
                fs::create_dir(object).unwrap();
                fs::set_permissions(object, fs::Permissions::from_mode(0o700)).unwrap();
            }
        }
        assert!(pin.check().is_err(), "{variant}, original inode {inode}");
    }
}

#[test]
fn sealed_publication_refuses_auxiliary_entries() {
    let tmp = TempDir::new();
    let mut state = tmp.create();
    let draft = state.create_plan_file([5; 16]).unwrap();
    let path = draft.checked_path().unwrap().to_owned();
    fs::write(
        path.parent().unwrap().join("unclosed-sidecar"),
        b"unfinished",
    )
    .unwrap();
    let error = draft.publish().unwrap_err();
    assert!(error.message.starts_with("state-foreign:"));
    assert!(path.exists());
}

#[test]
fn stat_only_checks_preserve_posix_file_locks() {
    fn lock(file: &File) -> io::Result<()> {
        // SAFETY: zero initializes the platform flock fields before setting a valid byte range.
        let mut request: libc::flock = unsafe { std::mem::zeroed() };
        request.l_type = libc::F_WRLCK as _;
        request.l_whence = libc::SEEK_SET as _;
        request.l_start = 0;
        request.l_len = 1;
        // SAFETY: file is live and request points to this platform's initialized flock.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &request) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
    if let Some(path) = std::env::var_os("RPGMIGRATE_PLAN_LOCK_PROBE") {
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let error = lock(&file).expect_err("the parent's POSIX lock was released");
        assert!(
            [Some(libc::EACCES), Some(libc::EAGAIN)].contains(&error.raw_os_error()),
            "{error:?}"
        );
        return;
    }
    let tmp = TempDir::new();
    let mut state = tmp.create();
    state.create_plan_file([6; 16]).unwrap().publish().unwrap();
    let pin = state.open_plan_file([6; 16]).unwrap();
    let path = pin.checked_path().unwrap().to_owned();
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    lock(&file).unwrap();
    pin.check().unwrap();
    pin.checked_path().unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "state::plan_files::tests::stat_only_checks_preserve_posix_file_locks",
            "--nocapture",
        ])
        .env("RPGMIGRATE_PLAN_LOCK_PROBE", path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn unsupported_publication_never_falls_back_to_replacing_rename() {
    for errno in [libc::ENOSYS, libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP] {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        let draft = state.create_plan_file([10; 16]).unwrap();
        let path = draft.checked_path().unwrap().to_owned();
        let (_, sealed) = names([10; 16]).unwrap();
        ERRNO.with(|slot| assert!(slot.replace(Some(errno)).is_none()));
        let error = draft.publish().unwrap_err();
        assert!(
            error.message.starts_with("state-plan-unsupported:"),
            "{errno}: {error:?}"
        );
        assert!(path.exists());
        assert!(
            !tmp.path()
                .join("data/plans")
                .join(sealed.to_str().unwrap())
                .exists()
        );
        assert_eq!(ERRNO.with(|slot| slot.get()), None);
    }
}

#[test]
fn leaf_stat_validation_refuses_wrong_owner_without_a_permission_mismatch() {
    // SAFETY: only initialized ownership/type/identity fields are used by this validator.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    stat.st_mode = libc::S_IFREG | 0o600;
    stat.st_nlink = 1;
    // SAFETY: geteuid has no arguments or memory preconditions.
    stat.st_uid = unsafe { libc::geteuid() };
    checked_leaf_stat(&stat).unwrap();
    stat.st_uid ^= 1;
    assert!(checked_leaf_stat(&stat).is_err());
}

#[test]
fn parent_directory_replacement_and_missing_reopen_are_not_repaired() {
    for relative in ["data", "data/plans"] {
        let tmp = TempDir::new();
        let mut state = tmp.create();
        state.create_plan_file([11; 16]).unwrap().publish().unwrap();
        let pin = state.open_plan_file([11; 16]).unwrap();
        let path = tmp.path().join(relative);
        fs::rename(&path, tmp.0.join("original-directory")).unwrap();
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        let replacement = fs::metadata(&path).unwrap().ino();
        assert!(pin.check().is_err());
        assert_eq!(fs::metadata(&path).unwrap().ino(), replacement);
        drop(pin);
        drop(state);
        fs::remove_dir(&path).unwrap();
        assert!(open_for_provenance_recovery(&tmp.path()).is_err());
        assert!(!path.exists());
    }
}
