use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) struct Files {
    pub(super) root: PathBuf,
}

impl Files {
    pub(super) fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "rpgmigrate-passfile-{}-{stamp}",
            std::process::id()
        ));
        DirBuilder::new().mode(0o700).create(&root).unwrap();
        Self { root }
    }

    pub(super) fn write(&self, name: &str, bytes: &[u8], mode: u32) -> PathBuf {
        let path = self.root.join(name);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        file.write_all(bytes).unwrap();
        file.set_permissions(fs::Permissions::from_mode(mode))
            .unwrap();
        path
    }

    pub(super) fn child(&self, test: &str) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args(["--exact", test, "--nocapture", "--test-threads=1"]);
        for (key, _) in std::env::vars_os() {
            if key.as_bytes().starts_with(b"PG") {
                command.env_remove(key);
            }
        }
        command.env("HOME", &self.root);
        command
    }
}

impl Drop for Files {
    fn drop(&mut self) {
        // Only this test's exclusively created private directory is eligible for cleanup.
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub(super) fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

pub(super) fn assert_child_completed(output: &Output) {
    assert!(
        output.status.success(),
        "passfile subprocess failed; output withheld"
    );
    assert!(
        output
            .stdout
            .windows(b"passfile-child-complete".len())
            .any(|part| part == b"passfile-child-complete"),
        "passfile subprocess did not execute the selected test"
    );
}
