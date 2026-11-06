use super::*;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

#[test]
fn passfile_warnings_do_not_escape_transport() {
    if let Some(value) = std::env::var_os("RPGMIGRATE_PASSFILE_CHILD_CONNINFO") {
        let io = IoControl {
            deadline: Instant::now() + Duration::from_secs(1),
            cancelled: Arc::new(AtomicBool::new(false)),
        };
        let outcome = PgConnection::connect(&Conninfo::new(value).unwrap(), &io);
        let failure = match outcome {
            Ok(_) => panic!("nonexistent private socket connected"),
            Err(failure) => failure,
        };
        // A missing native capability must not masquerade as a successful warning regression.
        assert_eq!(failure.kind, ErrorKind::Pgsql);
        println!("passfile-child-complete");
        return;
    }
    let mut leaked = Vec::new();
    for location in ["explicit", "environment", "default", "service"] {
        for shape in ["insecure", "nonregular"] {
            let files = Files::new();
            let passfile = files.root.join(".pgpass");
            if shape == "insecure" {
                files.write(
                    ".pgpass",
                    b"*:1:synthetic:synthetic:synthetic-test-only\n",
                    0o644,
                );
            } else {
                DirBuilder::new().mode(0o700).create(&passfile).unwrap();
            }
            let mut conninfo = format!(
                "host={} port=1 user=synthetic dbname=synthetic",
                quote(files.root.join("missing-socket").to_str().unwrap())
            );
            let mut child =
                files.child("pg::passfile_tests::passfile_warnings_do_not_escape_transport");
            match location {
                "explicit" => {
                    conninfo.push_str(&format!(" passfile={}", quote(passfile.to_str().unwrap())))
                }
                "environment" => {
                    child.env("PGPASSFILE", &passfile);
                }
                "default" => {}
                "service" => {
                    let service = files.write(
                        "service.conf",
                        format!("[synthetic]\npassfile={}\n", passfile.to_str().unwrap())
                            .as_bytes(),
                        0o600,
                    );
                    child.env("PGSERVICEFILE", service);
                    conninfo.push_str(" service=synthetic");
                }
                _ => unreachable!(),
            }
            let output = child
                .env("RPGMIGRATE_PASSFILE_CHILD_CONNINFO", conninfo)
                .output()
                .unwrap();
            assert_child_completed(&output);
            if !output.stderr.is_empty() {
                leaked.push(format!("{location}/{shape}"));
            }
        }
    }
    assert!(
        leaked.is_empty(),
        "native stderr escaped for synthetic scenarios: {leaked:?}"
    );
}
