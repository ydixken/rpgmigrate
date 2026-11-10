use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::catalog;
use crate::contracts::connection_files::{ConnectionFiles, SOURCE_FILES_ENV, TARGET_FILES_ENV};
use crate::contracts::{
    CloneOptions, ClonePlan, CloneTarget, Error, ErrorKind, IoControl, OwnedObject, RestorePolicy,
    Result, SnapshotTicket,
};
use crate::pg::PgConnection;
use crate::state::{ClonePhase, NativeState};

struct PrivateFiles {
    directory: PathBuf,
    names: Vec<String>,
}

impl PrivateFiles {
    fn new() -> Result<Self> {
        let mut random = [0u8; 16];
        File::open("/dev/urandom")
            .and_then(|mut f| f.read_exact(&mut random))
            .map_err(|_| failure("cannot create private utility storage"))?;
        let name = random
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect::<String>();
        let directory = std::env::temp_dir().join(format!("rpgmigrate-schema-{name}"));
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&directory)
            .map_err(|_| failure("cannot create private utility storage"))?;
        Ok(Self {
            directory,
            names: Vec::new(),
        })
    }
    fn file(&mut self, name: &str) -> Result<(PathBuf, File)> {
        let path = self.directory.join(name);
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(&path)
            .map_err(|_| failure("cannot create private utility file"))?;
        self.names.push(name.into());
        Ok((path, file))
    }
}
impl Drop for PrivateFiles {
    fn drop(&mut self) {
        for name in &self.names {
            let _ = fs::remove_file(self.directory.join(name));
        }
        let _ = fs::remove_dir(&self.directory);
    }
}

struct Tools {
    dump: PathBuf,
    restore: PathBuf,
    major: u32,
}

fn tool_major(output: &[u8], name: &str) -> Result<u32> {
    let output = std::str::from_utf8(output)
        .map_err(|_| failure("PostgreSQL utility returned an invalid version"))?;
    let prefix = format!("{name} (PostgreSQL) ");
    let version = output
        .strip_prefix(&prefix)
        .and_then(|v| v.split_whitespace().next())
        .ok_or_else(|| failure("PostgreSQL utility returned an invalid version"))?;
    if version.split('.').count() != 2
        || version
            .split('.')
            .any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(failure("PostgreSQL utility returned an invalid version"));
    }
    version
        .split('.')
        .next()
        .and_then(|v| v.parse().ok())
        .filter(|v| matches!(v, 16 | 18))
        .ok_or_else(|| failure("PostgreSQL utility requires major 16 or 18"))
}

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            // The private process group includes pg_restore parallel workers.
            // SAFETY: the owned child was started in its own process group.
            unsafe {
                libc::kill(-(self.0.id() as i32), libc::SIGKILL);
            }
            let _ = self.0.wait();
        }
    }
}

fn run(command: &mut Command, io: &IoControl, deadline: Option<Instant>) -> Result<ExitStatus> {
    if io.cancelled.load(Ordering::Acquire) {
        return Err(failure("PostgreSQL utility was cancelled"));
    }
    command
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .process_group(0);
    let child = command
        .spawn()
        .map_err(|_| failure("cannot start PostgreSQL utility"))?;
    let mut child = Running(child);
    loop {
        if io.cancelled.load(Ordering::Acquire) {
            return Err(failure("PostgreSQL utility was cancelled"));
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            return Err(failure("PostgreSQL utility discovery timed out"));
        }
        if let Some(status) = child
            .0
            .try_wait()
            .map_err(|_| failure("cannot observe PostgreSQL utility completion"))?
        {
            if !status.success() {
                // Failed restore leaders must not leave their parallel workers mutating the target.
                // SAFETY: the private process group belongs to the utility just observed exiting.
                unsafe {
                    libc::kill(-(child.0.id() as i32), libc::SIGKILL);
                }
            }
            return Ok(status);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn capture(program: &Path, args: &[&str], io: &IoControl, internal_files: bool) -> Result<Vec<u8>> {
    let mut files = PrivateFiles::new()?;
    let (path, file) = files.file("output")?;
    let mut command = Command::new(program);
    command.args(args).stdout(file);
    if internal_files {
        ConnectionFiles::inherit_only(&mut command, None)?;
    }
    let status = run(
        &mut command,
        io,
        Some(Instant::now() + Duration::from_secs(10)),
    )?;
    if !status.success() {
        return Err(failure("PostgreSQL utility discovery failed"));
    }
    let mut file =
        File::open(path).map_err(|_| failure("cannot read PostgreSQL utility version"))?;
    if file
        .metadata()
        .map_err(|_| failure("cannot inspect PostgreSQL utility version"))?
        .len()
        > 4096
    {
        return Err(failure("PostgreSQL utility version exceeds its bound"));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|_| failure("cannot read PostgreSQL utility version"))?;
    Ok(bytes)
}

fn tools(major: u32, io: &IoControl) -> Result<Tools> {
    tools_in(major, io, Path::new("/usr/lib/postgresql"), false)
}

fn tools_in(major: u32, io: &IoControl, stock_root: &Path, internal_files: bool) -> Result<Tools> {
    let explicit = if let Some(path) = std::env::var_os("PG_BINDIR") {
        if path.is_empty() {
            return Err(failure("PG_BINDIR must select an existing tool directory"));
        }
        Some(PathBuf::from(path))
    } else if let Some(config) = std::env::var_os("PG_CONFIG") {
        let bytes = capture(Path::new(&config), &["--bindir"], io, internal_files)?;
        let value = std::str::from_utf8(&bytes)
            .map_err(|_| failure("pg_config returned an invalid tool directory"))?
            .trim_end_matches(['\n', '\r']);
        if value.is_empty() || value.contains(['\n', '\r']) {
            return Err(failure("pg_config returned an invalid tool directory"));
        }
        Some(PathBuf::from(value))
    } else {
        None
    };
    let packaged = stock_root.join(major.to_string()).join("bin");
    let directories = if let Some(directory) = explicit {
        vec![directory]
    } else if packaged
        .try_exists()
        .map_err(|_| failure("cannot inspect packaged PostgreSQL tool directory"))?
    {
        vec![packaged]
    } else {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()).collect()
    };
    for directory in directories {
        let dump = directory.join("pg_dump");
        let restore = directory.join("pg_restore");
        if !dump.is_file() || !restore.is_file() {
            continue;
        }
        let dump_version = tool_major(
            &capture(&dump, &["--version"], io, internal_files)?,
            "pg_dump",
        )?;
        let restore_version = tool_major(
            &capture(&restore, &["--version"], io, internal_files)?,
            "pg_restore",
        )?;
        if dump_version != major || restore_version != major {
            return Err(failure(
                "pg_dump and pg_restore must match the target major",
            ));
        }
        return Ok(Tools {
            dump,
            restore,
            major,
        });
    }
    Err(failure(
        "matching target-major pg_dump and pg_restore were not found",
    ))
}

fn utility(program: &Path, connection: &PgConnection) -> Result<Command> {
    let environment = connection.utility_environment()?;
    utility_command(
        program,
        environment.remove,
        environment.values,
        environment.files,
    )
}

fn utility_command(
    program: &Path,
    remove: Vec<OsString>,
    values: Vec<(OsString, OsString)>,
    files: Option<Arc<ConnectionFiles>>,
) -> Result<Command> {
    let mut options = values
        .iter()
        .find(|(name, _)| name == "PGOPTIONS")
        .map(|(_, value)| value.clone())
        .unwrap_or_default();
    options.push(
        " -c synchronous_commit=on -c row_security=off -c client_encoding=UTF8 -c lc_monetary=C",
    );
    let mut command = Command::new(program);
    command
        .env_remove(SOURCE_FILES_ENV)
        .env_remove(TARGET_FILES_ENV);
    if let Some(files) = files {
        ConnectionFiles::inherit_only(&mut command, Some(files))?;
    }
    for name in remove.iter().map(OsStr::new).chain([
        OsStr::new("PGSERVICEFILE"),
        OsStr::new("PGSYSCONFDIR"),
        OsStr::new("PGSERVICE"),
    ]) {
        command.env_remove(name);
    }
    command
        .envs(values)
        .env("PGOPTIONS", options)
        .env("PGAPPNAME", "rpgmigrate-schema")
        .arg("--no-password")
        .stdout(Stdio::null());
    Ok(command)
}

fn policy(command: &mut Command, policy: RestorePolicy) {
    for (enabled, arg) in [
        (policy.no_owner, "--no-owner"),
        (policy.no_acl, "--no-acl"),
        (policy.no_comments, "--no-comments"),
        (policy.no_tablespaces, "--no-tablespaces"),
    ] {
        if enabled {
            command.arg(arg);
        }
    }
}

pub(crate) struct SchemaArchive {
    files: PrivateFiles,
    path: PathBuf,
    tools: Tools,
    ticket: SnapshotTicket,
    policy: RestorePolicy,
    target: CloneTarget,
}

pub(crate) fn preflight(
    source: &PgConnection,
    target: &PgConnection,
    plan: &ClonePlan,
    io: &IoControl,
) -> Result<()> {
    let _ = tools(plan.target.major, io)?;
    let _ = source.utility_environment()?;
    let _ = target.utility_environment()?;
    Ok(())
}

pub(crate) fn dump(
    options: &CloneOptions,
    ticket: &SnapshotTicket,
    state: &NativeState,
    io: &IoControl,
) -> Result<SchemaArchive> {
    let clone = state
        .checked_clone()?
        .ok_or_else(|| failure("schema dump requires a bound clone state"))?;
    if clone.ticket != *ticket || clone.plan.policy != options.policy {
        return Err(failure("schema archive snapshot or policy binding differs"));
    }
    let mut source = PgConnection::connect(&options.source, io)?;
    if catalog::read_clone_source(&mut source, io)? != ticket.source {
        return Err(failure(
            "schema dump source identity differs from the snapshot",
        ));
    }
    let mut target = PgConnection::connect(&options.target, io)?;
    let target_identity = catalog::read_clone_target(&mut target, io)?;
    if target_identity != clone.plan.target || ticket.source.major > target_identity.major {
        return Err(failure(
            "schema dump database version or identity binding differs",
        ));
    }
    let tools = tools(target_identity.major, io)?;
    let mut files = PrivateFiles::new()?;
    let (path, file) = files.file("schema.dump")?;
    let mut command = utility(&tools.dump, &source)?;
    command
        .args([
            "--format=custom",
            "--schema-only",
            "--quote-all-identifiers",
            "--no-publications",
            "--no-subscriptions",
        ])
        .arg(format!("--snapshot={}", ticket.token))
        .arg("--file")
        .arg(&path);
    policy(&mut command, options.policy);
    if !run(&mut command, io, None)?.success() {
        return Err(failure(
            "schema dump failed; utility diagnostics are suppressed",
        ));
    }
    file.sync_all()
        .map_err(|_| failure("cannot synchronize schema archive"))?;
    if file
        .metadata()
        .map_err(|_| failure("cannot inspect schema archive"))?
        .len()
        == 0
    {
        return Err(failure("schema dump produced an empty archive"));
    }
    Ok(SchemaArchive {
        files,
        path,
        tools,
        ticket: ticket.clone(),
        policy: options.policy,
        target: target_identity,
    })
}

fn created_objects(
    before: &[OwnedObject],
    after: Vec<OwnedObject>,
    policy: RestorePolicy,
) -> Result<Vec<OwnedObject>> {
    let address =
        |object: &OwnedObject| (object.class_oid, object.object_oid, object.object_sub_id);
    for previous in before {
        let matches = after
            .iter()
            .filter(|object| address(object) == address(previous))
            .collect::<Vec<_>>();
        if matches.len() != 1 {
            return Err(failure(
                "pre-data restore changed an existing target object identity",
            ));
        }
        let mut actual = matches[0].clone();
        if !policy.no_owner
            && previous.class_oid == 2615
            && previous.kind == "namespace"
            && previous.name == "public"
        {
            actual.owner_oid = previous.owner_oid;
        }
        if actual != *previous {
            return Err(failure(
                "pre-data restore changed an existing target object binding",
            ));
        }
    }
    Ok(after
        .into_iter()
        .filter(|object| {
            !before
                .iter()
                .any(|previous| address(object) == address(previous))
        })
        .collect())
}

/// Target DDL quiescence makes the positive before/after OID difference an ownership proof.
pub(crate) fn restore_pre(
    options: &CloneOptions,
    plan: &ClonePlan,
    archive: &SchemaArchive,
    state: &mut NativeState,
    io: &IoControl,
) -> Result<Vec<OwnedObject>> {
    let _ = &archive.files;
    let bound = state
        .checked_clone()?
        .ok_or_else(|| failure("pre-data restore requires bound clone state"))?;
    if bound.phase != ClonePhase::Fresh || bound.plan != *plan || bound.ticket != archive.ticket {
        return Err(failure(
            "pre-data ownership handoff is absent or uncertain; use a fresh target or manual recovery",
        ));
    }
    if archive.ticket.source != plan.source
        || archive.ticket.generation != plan.generation
        || archive.policy != plan.policy
        || archive.policy != options.policy
        || archive.tools.major != plan.target.major
    {
        return Err(failure("pre-data restore archive binding differs"));
    }
    let mut target = PgConnection::connect(&options.target, io)?;
    if catalog::read_clone_target(&mut target, io)? != plan.target {
        return Err(failure("pre-data restore target identity differs"));
    }
    catalog::configure_durability(&mut target, io)?;
    let before = catalog::clone_object_inventory(&mut target, io)?;
    let mut command = utility(&archive.tools.restore, &target)?;
    state.begin_pre_data()?;
    // An empty explicit dbname selects restore connection mode; libpq reads PGDATABASE.
    command
        .args([
            "--dbname=",
            "--section=pre-data",
            "--single-transaction",
            "--exit-on-error",
            "--no-publications",
            "--no-subscriptions",
        ])
        .arg(&archive.path);
    policy(&mut command, options.policy);
    if !run(&mut command, io, None)?.success() {
        return Err(failure(
            "pre-data utility outcome has no positive ownership handoff; use a fresh target or manual recovery",
        ));
    }
    let after = catalog::clone_object_inventory(&mut target, io)?;
    created_objects(&before, after, options.policy)
}

pub(crate) fn restore_post(
    options: &CloneOptions,
    archive: &SchemaArchive,
    io: &IoControl,
) -> Result<()> {
    if options.policy != archive.policy || options.post_data_jobs == 0 {
        return Err(failure("post-data restore policy or concurrency differs"));
    }
    let mut target = PgConnection::connect(&options.target, io)?;
    if catalog::read_clone_target(&mut target, io)? != archive.target {
        return Err(failure(
            "post-data target identity differs from the schema archive",
        ));
    }
    catalog::configure_durability(&mut target, io)?;
    let mut command = utility(&archive.tools.restore, &target)?;
    command
        .args([
            "--dbname=",
            "--section=post-data",
            "--exit-on-error",
            "--no-publications",
            "--no-subscriptions",
        ])
        .arg(format!("--jobs={}", options.post_data_jobs))
        .arg(&archive.path);
    policy(&mut command, options.policy);
    if !run(&mut command, io, None)?.success() {
        return Err(failure(
            "post-data utility did not positively complete; automatic recopy is refused",
        ));
    }
    Ok(())
}

fn exact_schema_pattern(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[derive(Clone, Copy, Default)]
struct ComparisonDump<'a> {
    section: Option<&'static str>,
    snapshot: Option<&'a str>,
}

fn comparison_dump(
    options: &CloneOptions,
    connection: &PgConnection,
    tools: &Tools,
    path: &Path,
    exclude: &BTreeSet<String>,
    selection: ComparisonDump<'_>,
    io: &IoControl,
) -> Result<()> {
    let mut command = utility(&tools.dump, connection)?;
    command
        .args([
            "--format=plain",
            "--schema-only",
            "--quote-all-identifiers",
            "--no-publications",
            "--no-subscriptions",
        ])
        .arg("--file")
        .arg(path);
    command.arg("--restrict-key=RpgmigrateSchemaComparison");
    if let Some(section) = selection.section {
        command.arg(format!("--section={section}"));
    }
    if let Some(snapshot) = selection.snapshot {
        command.arg(format!("--snapshot={snapshot}"));
    }

    for namespace in exclude {
        command.arg(format!(
            "--exclude-schema={}",
            exact_schema_pattern(namespace)
        ));
    }
    policy(&mut command, options.policy);
    if !run(&mut command, io, None)?.success() {
        return Err(failure(
            "schema comparison dump did not positively complete",
        ));
    }
    Ok(())
}

fn normalized_header(reader: &mut impl BufRead) -> Result<()> {
    let expected = ["--\n", "-- PostgreSQL database dump\n", "--\n", "\n"];
    let mut line = String::new();
    for expected in expected {
        line.clear();
        reader
            .read_line(&mut line)
            .map_err(|_| failure("cannot read schema comparison header"))?;
        if line != expected {
            return Err(failure("schema utility produced an unrecognized header"));
        }
    }
    line.clear();
    reader
        .read_line(&mut line)
        .map_err(|_| failure("cannot read schema comparison header"))?;
    if line.starts_with("\\restrict ") {
        if line != "\\restrict RpgmigrateSchemaComparison\n" {
            return Err(failure("schema comparison restrict key differs"));
        }
        line.clear();
        reader
            .read_line(&mut line)
            .map_err(|_| failure("cannot read schema comparison header"))?;
        if line != "\n" {
            return Err(failure("schema utility produced an unrecognized header"));
        }
        line.clear();
        reader
            .read_line(&mut line)
            .map_err(|_| failure("cannot read schema comparison header"))?;
    }
    if !line.starts_with("-- Dumped from database version ") {
        return Err(failure(
            "schema utility omitted the source version provenance",
        ));
    }
    line.clear();
    reader
        .read_line(&mut line)
        .map_err(|_| failure("cannot read schema comparison header"))?;
    if !line.starts_with("-- Dumped by pg_dump version ") {
        return Err(failure(
            "schema utility omitted the tool version provenance",
        ));
    }
    Ok(())
}

pub(crate) fn compare(
    options: &CloneOptions,
    exact_metadata: &[OwnedObject],
    io: &IoControl,
) -> Result<bool> {
    compare_schemas(options, exact_metadata, None, io)
}

pub(crate) fn compare_pre_data(
    options: &CloneOptions,
    ticket: &SnapshotTicket,
    exact_metadata: &[OwnedObject],
    io: &IoControl,
) -> Result<bool> {
    compare_schemas(options, exact_metadata, Some(ticket), io)
}

fn compare_schemas(
    options: &CloneOptions,
    exact_metadata: &[OwnedObject],
    ticket: Option<&SnapshotTicket>,
    io: &IoControl,
) -> Result<bool> {
    let mut source = PgConnection::connect(&options.source, io)?;
    let mut target = PgConnection::connect(&options.target, io)?;
    catalog::configure_session(&mut source, io)?;
    catalog::configure_session(&mut target, io)?;
    let source_identity = catalog::read_clone_source(&mut source, io)?;
    let target_identity = catalog::read_clone_target(&mut target, io)?;
    if ticket.is_some_and(|ticket| ticket.source != source_identity || ticket.generation.0 == 0) {
        return Err(failure(
            "pre-data comparison snapshot binding differs from the current source",
        ));
    }

    if source_identity.major > target_identity.major {
        return Err(failure(
            "schema comparison requires an admitted forward version pair",
        ));
    }
    let inventory = catalog::clone_object_inventory(&mut target, io)?;
    catalog::verify_exact_exclusions(&inventory, exact_metadata)?;
    let exclude = exact_metadata
        .iter()
        .filter_map(|o| o.namespace_name.clone())
        .collect::<BTreeSet<_>>();
    if inventory.iter().any(|o| {
        o.namespace_name
            .as_ref()
            .is_some_and(|n| exclude.contains(n))
            && !exact_metadata.contains(o)
    }) {
        return Err(failure(
            "schema comparison found an unowned object in the bookkeeping namespace",
        ));
    }
    let source_inventory = catalog::clone_object_inventory(&mut source, io)?;
    if source_inventory.iter().any(|o| {
        o.namespace_name
            .as_ref()
            .is_some_and(|n| exclude.contains(n))
    }) {
        return Err(failure(
            "source schema collides with target bookkeeping exclusion",
        ));
    }
    let tools = tools_in(
        target_identity.major,
        io,
        Path::new("/usr/lib/postgresql"),
        options.source.files().is_some(),
    )?;
    let mut files = PrivateFiles::new()?;
    let (source_path, _) = files.file("source.sql")?;
    let (target_path, _) = files.file("target.sql")?;
    comparison_dump(
        options,
        &source,
        &tools,
        &source_path,
        &BTreeSet::new(),
        ComparisonDump {
            section: ticket.map(|_| "pre-data"),
            snapshot: ticket.map(|ticket| ticket.token.as_str()),
        },
        io,
    )?;
    comparison_dump(
        options,
        &target,
        &tools,
        &target_path,
        &exclude,
        ComparisonDump {
            section: ticket.map(|_| "pre-data"),
            snapshot: None,
        },
        io,
    )?;
    let mut left = BufReader::new(
        File::open(source_path).map_err(|_| failure("cannot read schema comparison input"))?,
    );
    let mut right = BufReader::new(
        File::open(target_path).map_err(|_| failure("cannot read schema comparison input"))?,
    );
    normalized_header(&mut left)?;
    normalized_header(&mut right)?;
    let mut a = [0u8; 65536];
    let mut b = [0u8; 65536];
    loop {
        let al = left
            .read(&mut a)
            .map_err(|_| failure("cannot read schema comparison input"))?;
        let bl = right
            .read(&mut b)
            .map_err(|_| failure("cannot read schema comparison input"))?;
        if al != bl || a[..al] != b[..bl] {
            return Ok(false);
        }
        if al == 0 {
            return Ok(true);
        }
    }
}

#[track_caller]
fn failure(message: &str) -> Error {
    Error::new(ErrorKind::BadConfig, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::Conninfo;

    #[test]
    #[cfg(target_os = "linux")]
    fn internal_utility_builder_retains_only_selected_role_files() {
        use crate::contracts::connection_files::tests::{bound_source, sealed};
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStringExt;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        const MODE: &str = "RPGMIGRATE_TEST_UTILITY_FILES_MODE";
        const PROBE: &str = "RPGMIGRATE_TEST_UTILITY_FILES_PROBE";
        const TEST: &str =
            "schema::tests::internal_utility_builder_retains_only_selected_role_files";
        match std::env::var(MODE).as_deref() {
            Ok("probe") => {
                assert!(std::env::var_os(SOURCE_FILES_ENV).is_none());
                assert!(std::env::var_os(TARGET_FILES_ENV).is_none());
                let cert = std::env::var_os("PGSSLCERT").unwrap();
                assert_eq!(std::env::var_os("PGSSLKEY").unwrap(), cert);
                assert_eq!(std::fs::read(cert).unwrap(), b"source-fixture");
                let probe: serde_json::Value =
                    serde_json::from_str(&std::env::var(PROBE).unwrap()).unwrap();
                for entry in probe.as_array().unwrap() {
                    let path = format!("/proc/self/fd/{}", entry[0].as_i64().unwrap());
                    let leaked = std::fs::metadata(path).is_ok_and(|metadata| {
                        metadata.dev() == entry[1].as_u64().unwrap()
                            && metadata.ino() == entry[2].as_u64().unwrap()
                    });
                    assert!(!leaked);
                }
                return;
            }
            Ok("build") => {}
            _ => {
                let output = Command::new(std::env::current_exe().unwrap())
                    .args(["--exact", TEST, "--nocapture"])
                    .env(MODE, "build")
                    .env(SOURCE_FILES_ENV, "private-parent-map")
                    .env(TARGET_FILES_ENV, "private-parent-map")
                    .output()
                    .unwrap();
                assert!(output.status.success(), "{output:?}");
                return;
            }
        }
        let source = sealed(b"source-fixture");
        let target = sealed(b"target-fixture");
        let ambient = sealed(b"unrelated-fixture");
        // SAFETY: only this owned fixture is made inheritable for the negative control.
        assert_eq!(
            unsafe { libc::fcntl(ambient.as_raw_fd(), libc::F_SETFD, 0) },
            0
        );
        let bound = bound_source(
            "host=source.invalid",
            &format!(
                "{{\"sslcert\":{},\"sslkey\":{}}}",
                source.as_raw_fd(),
                source.as_raw_fd()
            ),
            &format!("{{\"passfile\":{}}}", target.as_raw_fd()),
        );
        let files = bound.source.files().unwrap().clone();
        let lifetime = Arc::downgrade(&files);
        let mut values: Vec<(OsString, OsString)> = files
            .options()
            .map(|(key, value)| {
                (
                    if key == c"sslcert" {
                        "PGSSLCERT".into()
                    } else {
                        "PGSSLKEY".into()
                    },
                    OsString::from_vec(value.to_bytes().to_vec()),
                )
            })
            .collect();
        values.push(("PGOPTIONS".into(), "-c search_path=pg_catalog".into()));
        let mut forbidden = vec![source.as_raw_fd(), target.as_raw_fd(), ambient.as_raw_fd()];
        forbidden.extend(bound.target.files().unwrap().options().map(|(_, path)| {
            path.to_str()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap()
                .parse::<i32>()
                .unwrap()
        }));
        let probe: Vec<_> = forbidden
            .into_iter()
            .map(|fd| {
                let metadata = std::fs::metadata(format!("/proc/self/fd/{fd}")).unwrap();
                serde_json::json!([fd, metadata.dev(), metadata.ino()])
            })
            .collect();
        let mut private = PrivateFiles::new().unwrap();
        let (program, mut script) = private.file("probe").unwrap();
        use std::io::Write;
        let executable = std::env::current_exe()
            .unwrap()
            .to_str()
            .unwrap()
            .replace('\'', "'\\''");
        writeln!(
            script,
            "#!/bin/sh\nexec '{executable}' --exact {TEST} --nocapture"
        )
        .unwrap();
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700)).unwrap();
        drop(script);
        let mut command = utility_command(&program, vec![], values, Some(files)).unwrap();
        command
            .env(MODE, "probe")
            .env(PROBE, serde_json::to_string(&probe).unwrap());
        drop(bound);
        assert!(lifetime.upgrade().is_some());
        let status = command.status().unwrap();
        assert!(status.success());
        drop(command);
        assert!(lifetime.upgrade().is_none());
    }
    use std::ffi::OsString;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    struct ToolFixtures(PrivateFiles);

    impl ToolFixtures {
        fn pair(&self, name: &str, dump: u32, restore: u32) -> PathBuf {
            use std::os::unix::fs::PermissionsExt;
            let directory = self.0.directory.join(name);
            fs::create_dir_all(&directory).unwrap();
            for (tool, major) in [("pg_dump", dump), ("pg_restore", restore)] {
                let path = directory.join(tool);
                fs::write(
                    &path,
                    format!("#!/bin/sh\nprintf '%s\\n' '{tool} (PostgreSQL) {major}.6'\n"),
                )
                .unwrap();
                fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
            }
            directory
        }

        fn config(&self, name: &str, directory: &str) -> PathBuf {
            use std::os::unix::fs::PermissionsExt;
            let path = self.0.directory.join(name);
            fs::write(&path, format!("#!/bin/sh\nprintf '%s\\n' '{directory}'\n")).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            path
        }
    }

    impl Drop for ToolFixtures {
        fn drop(&mut self) {
            // Only this test's exclusively created private tree is eligible for cleanup.
            fs::remove_dir_all(&self.0.directory).unwrap();
        }
    }

    fn utility_io() -> IoControl {
        IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )))
    }

    fn utility_context(io: &IoControl) -> (serde_json::Value, PgConnection) {
        let path =
            std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned schema context is required");
        let context: serde_json::Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
        let token = context["token"].as_str().unwrap();
        let root = Path::new(context["root"].as_str().unwrap());
        assert!(
            fs::read_to_string(root.join(".e1-owner")).unwrap() == token,
            "fixture ownership differs"
        );
        let mut admin = PgConnection::connect(
            &Conninfo::new(
                std::env::var_os("RPGMIGRATE_TEST_SCHEMA_SOURCE_ADMIN")
                    .expect("schema admin is required"),
            )
            .unwrap(),
            io,
        )
        .unwrap();
        let identity = admin
            .query("SELECT current_setting('cluster_name')", io)
            .unwrap();
        assert!(
            identity.cell(0, 0).unwrap() == Some(token.as_bytes()),
            "fixture cluster identity differs"
        );
        (context, admin)
    }

    fn utility_child(test: &str, case: &str, root: &Path) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            test,
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ]);
        for (key, _) in std::env::vars_os() {
            if key.as_bytes().starts_with(b"PG") && key != "PG_BINDIR" && key != "PG_CONFIG" {
                command.env_remove(key);
            }
        }
        command
            .env("RPGMIGRATE_TEST_UTILITY_CHILD", case)
            .env("RPGMIGRATE_TEST_UTILITY_ROOT", root);
        command
    }

    fn utility_child_completed(command: &mut Command) {
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "schema utility subprocess failed; output withheld"
        );
        assert!(
            output
                .stdout
                .windows(b"schema-utility-child-complete".len())
                .any(|part| part == b"schema-utility-child-complete"),
            "schema utility child did not execute"
        );
    }

    fn connection_value(connection: &PgConnection, name: &str) -> OsString {
        connection
            .utility_environment()
            .unwrap()
            .values
            .into_iter()
            .find(|(key, _)| key == name)
            .expect("required effective fixture setting is absent")
            .1
    }

    fn uri_value(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("%{byte:02X}")).collect()
    }

    fn utility_uri(connection: &PgConnection, database: &str) -> String {
        let mut uri = format!(
            "postgresql:///?dbname={}&sslmode=disable",
            uri_value(database.as_bytes())
        );
        for (parameter, name) in [
            ("host", "PGHOST"),
            ("port", "PGPORT"),
            ("user", "PGUSER"),
            ("passfile", "PGPASSFILE"),
        ] {
            uri.push_str(&format!(
                "&{parameter}={}",
                uri_value(connection_value(connection, name).as_bytes())
            ));
        }
        uri
    }

    fn utility_probe(
        connection: &PgConnection,
        context: &serde_json::Value,
        expected: &str,
    ) -> Command {
        let mut command = utility(
            &Path::new(context["bindir"].as_str().unwrap()).join("psql"),
            connection,
        )
        .unwrap();
        assert!(
            expected
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "fixture database name is invalid"
        );
        let literal = format!("'{expected}'");
        command
            .args(["-X", "-qAt", "--set=ON_ERROR_STOP=1", "--command"])
            .arg(format!("SELECT 1/(current_database()={literal})::integer"));
        command
    }

    fn utility_keyword(base: &OsStr, key: &str, value: &OsStr) -> OsString {
        let mut bytes = base.as_bytes().to_vec();
        bytes.extend_from_slice(format!(" {key}='").as_bytes());
        for byte in value.as_bytes() {
            if matches!(byte, b'\\' | b'\'') {
                bytes.push(b'\\');
            }
            bytes.push(*byte);
        }
        bytes.push(b'\'');
        OsString::from_vec(bytes)
    }

    fn utility_private_file(path: &Path, bytes: &[u8]) {
        use std::io::Write;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        file.write_all(bytes).unwrap();
    }

    struct UtilityRelay {
        port: u16,
        stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
        sockets: std::sync::Arc<std::sync::Mutex<Vec<std::net::TcpStream>>>,
        worker: Option<std::thread::JoinHandle<bool>>,
    }

    fn utility_relay_copy(
        mut input: std::net::TcpStream,
        mut output: std::net::TcpStream,
        stop: &std::sync::atomic::AtomicBool,
    ) {
        use std::io::Write;
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut buffer = [0u8; 8192];
        let (mut start, mut end) = (0, 0);
        while !stop.load(Ordering::Acquire) && Instant::now() < deadline {
            if start == end {
                match input.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(length) => {
                        start = 0;
                        end = length;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                }
            }
            match output.write(&buffer[start..end]) {
                Ok(0) => break,
                Ok(length) => start += length,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    }

    impl UtilityRelay {
        fn new(server_port: u16) -> Self {
            use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
            let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let sockets = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let worker_stop = stop.clone();
            let worker_sockets = sockets.clone();
            let worker = std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                while !worker_stop.load(Ordering::Acquire) && Instant::now() < deadline {
                    let client = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(5));
                            continue;
                        }
                        Err(_) => return false,
                    };
                    let address = SocketAddr::from((Ipv4Addr::LOCALHOST, server_port));
                    let Ok(server) = TcpStream::connect_timeout(&address, Duration::from_secs(2))
                    else {
                        return false;
                    };
                    if client.set_nonblocking(true).is_err()
                        || server.set_nonblocking(true).is_err()
                    {
                        return false;
                    }
                    let Ok(client_read) = client.try_clone() else {
                        return false;
                    };
                    let Ok(server_write) = server.try_clone() else {
                        return false;
                    };
                    {
                        let mut owned = worker_sockets.lock().unwrap();
                        if worker_stop.load(Ordering::Acquire) {
                            let _ = client.shutdown(std::net::Shutdown::Both);
                            let _ = server.shutdown(std::net::Shutdown::Both);
                            return false;
                        }
                        owned.push(client.try_clone().unwrap());
                        owned.push(server.try_clone().unwrap());
                    }
                    let outbound_stop = worker_stop.clone();
                    let outbound = std::thread::spawn(move || {
                        utility_relay_copy(client_read, server_write, &outbound_stop)
                    });
                    let inbound = std::thread::spawn(move || {
                        utility_relay_copy(server, client, &worker_stop);
                    });
                    let outbound = outbound.join();
                    let inbound = inbound.join();
                    return outbound.is_ok() && inbound.is_ok();
                }
                false
            });
            Self {
                port,
                stop,
                sockets,
                worker: Some(worker),
            }
        }

        fn close(&mut self) -> bool {
            self.stop.store(true, Ordering::Release);
            for stream in self.sockets.lock().unwrap().iter() {
                let _ = stream.shutdown(std::net::Shutdown::Both);
            }
            self.worker
                .take()
                .is_none_or(|worker| worker.join().unwrap_or(false))
        }
    }

    impl Drop for UtilityRelay {
        fn drop(&mut self) {
            let _ = self.close();
        }
    }

    fn utility_selected_peer_loss(context: &serde_json::Value, io: &IoControl) {
        let server_port = u16::try_from(context["port"].as_u64().unwrap()).unwrap();
        let mut relay = UtilityRelay::new(server_port);
        let base =
            std::env::var_os("RPGMIGRATE_TEST_SOURCE").expect("owned TCP role fixture is required");
        let password = std::env::var_os("RPGMIGRATE_TEST_AUTH_PASSWORD").unwrap();
        let mut info = utility_keyword(&base, "password", &password);
        for (name, value) in [
            (
                "host",
                "selected.fixture.invalid,fallback.fixture.invalid".to_owned(),
            ),
            ("hostaddr", "127.0.0.1,127.0.0.1".to_owned()),
            ("port", format!("{},{}", relay.port, server_port)),
            ("connect_timeout", "2".to_owned()),
        ] {
            info = utility_keyword(&info, name, OsStr::new(&value));
        }
        let connection = PgConnection::connect(&Conninfo::new(info).unwrap(), io).unwrap();
        let database = connection_value(&connection, "PGDATABASE");
        let mut captured = utility_probe(&connection, context, database.to_str().unwrap());
        for (name, expected) in [
            ("PGHOST", "selected.fixture.invalid".to_owned()),
            ("PGHOSTADDR", "127.0.0.1".to_owned()),
            ("PGPORT", relay.port.to_string()),
        ] {
            assert!(
                captured
                    .get_envs()
                    .any(|(key, value)| key == name && value == Some(OsStr::new(&expected))),
                "selected TCP peer or TLS hostname changed"
            );
        }
        let started = Instant::now();
        assert!(
            relay.close(),
            "owned relay did not join its forwarding threads"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "owned relay teardown exceeded its bound"
        );
        assert!(
            !run(&mut captured, io, None).unwrap().success(),
            "utility failed over after losing the selected peer"
        );
        let mut direct = utility_keyword(&base, "password", &password);
        for (name, value) in [
            ("host", "fallback.fixture.invalid".to_owned()),
            ("hostaddr", "127.0.0.1".to_owned()),
            ("port", server_port.to_string()),
        ] {
            direct = utility_keyword(&direct, name, OsStr::new(&value));
        }
        let direct = PgConnection::connect(&Conninfo::new(direct).unwrap(), io).unwrap();
        assert!(
            run(
                &mut utility_probe(&direct, context, database.to_str().unwrap()),
                io,
                None
            )
            .unwrap()
            .success(),
            "direct fallback positive control failed"
        );
    }

    #[test]
    #[ignore = "requires an owned PostgreSQL schema fixture"]
    fn live_utility_effective_auth_and_tls_settings_are_private() {
        const TEST: &str =
            "schema::tests::live_utility_effective_auth_and_tls_settings_are_private";
        let io = utility_io();
        let (context, mut admin) = utility_context(&io);
        let password = std::env::var_os("RPGMIGRATE_TEST_AUTH_PASSWORD")
            .expect("isolated SCRAM credential is required");
        if let Some(case) = std::env::var_os("RPGMIGRATE_TEST_UTILITY_CHILD") {
            let root = PathBuf::from(std::env::var_os("RPGMIGRATE_TEST_UTILITY_ROOT").unwrap());
            let info =
                Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_AUTH_BASE").unwrap()).unwrap();
            let connection = PgConnection::connect(&info, &io);
            if ["insecure", "nonregular", "wrong-password"]
                .iter()
                .any(|name| case == *name)
            {
                assert!(
                    connection.is_err(),
                    "unsafe or incorrect service passfile authenticated"
                );
            } else {
                let connection = connection.unwrap();
                let before = catalog::clone_object_inventory(&mut admin, &io).unwrap();
                if case == "unsupported-secret" || case == "unsupported-fallback" {
                    let failure = match utility(
                        &Path::new(context["bindir"].as_str().unwrap()).join("psql"),
                        &connection,
                    ) {
                        Err(error) => error,
                        Ok(_) => panic!("unmapped effective option must be refused before spawn"),
                    };
                    assert!(
                        failure.message
                            == "PostgreSQL utility connection settings cannot be represented safely"
                    );
                } else {
                    let environment = connection.utility_environment().unwrap();
                    assert!(
                        environment
                            .values
                            .iter()
                            .any(|(name, value)| name == "PGUSER" && value == "e1_auth"),
                        "effective role precedence changed"
                    );
                    if case != "service-passfile" {
                        assert!(
                            environment
                                .values
                                .iter()
                                .any(|(name, value)| name == "PGPASSWORD" && value == &password),
                            "effective credential precedence changed"
                        );
                    } else {
                        assert!(
                            environment
                                .values
                                .iter()
                                .any(|(name, value)| name == "PGPASSFILE"
                                    && value == root.join("passfile").as_os_str()),
                            "effective service passfile changed"
                        );
                    }
                    let mut command = utility_probe(
                        &connection,
                        &context,
                        connection_value(&connection, "PGDATABASE")
                            .to_str()
                            .unwrap(),
                    );
                    let expected_root = if case == "explicit" {
                        "root-explicit-"
                    } else {
                        "root-service-"
                    };
                    for (name, value) in [
                        (
                            "PGSSLROOTCERT",
                            root.join(OsString::from_vec(
                                [expected_root.as_bytes(), &[0xff]].concat(),
                            ))
                            .into_os_string(),
                        ),
                        (
                            "PGSSLKEY",
                            root.join(OsString::from_vec(b"key-service-\xfe".to_vec()))
                                .into_os_string(),
                        ),
                        (
                            "PGSSLCERT",
                            root.join(OsString::from_vec(b"cert-environment-\xfd".to_vec()))
                                .into_os_string(),
                        ),
                        ("PGSSLMODE", "disable".into()),
                        ("PGAPPNAME", "rpgmigrate-schema".into()),
                    ] {
                        assert!(
                            command
                                .get_envs()
                                .any(|(key, actual)| key == name
                                    && actual == Some(value.as_os_str())),
                            "effective TLS or application setting bytes changed"
                        );
                    }
                    let options = if case == "explicit" {
                        b"-c statement_timeout=12345".as_slice()
                    } else {
                        b"-c statement_timeout=23456"
                    };
                    let mut expected_options = options.to_vec();
                    expected_options.extend_from_slice(b" -c synchronous_commit=on -c row_security=off -c client_encoding=UTF8 -c lc_monetary=C");
                    assert!(
                        command.get_envs().any(|(key, value)| key == "PGOPTIONS"
                            && value.is_some_and(|value| value.as_bytes() == expected_options)),
                        "effective options were replaced by process defaults"
                    );
                    for name in ["PGSERVICE", "PGSERVICEFILE", "PGSYSCONFDIR"] {
                        assert!(
                            command
                                .get_envs()
                                .any(|(key, value)| key == name && value.is_none()),
                            "service lookup remains enabled in child"
                        );
                    }
                    assert!(!password.is_empty());
                    for argument in command.get_args() {
                        assert!(
                            !argument
                                .as_bytes()
                                .windows(password.len())
                                .any(|part| part == password.as_bytes()),
                            "credential reached utility argv"
                        );
                        assert!(
                            !argument
                                .as_bytes()
                                .windows(b"postgresql://".len())
                                .any(|part| part == b"postgresql://"),
                            "connection URI reached utility argv"
                        );
                    }
                    fs::remove_file(root.join("service.conf")).unwrap();
                    assert!(
                        run(&mut command, &io, None).unwrap().success(),
                        "captured authentication reread service or lost its credential"
                    );
                }
                assert!(
                    catalog::clone_object_inventory(&mut admin, &io).unwrap() == before,
                    "authentication check mutated database objects"
                );
            }
            println!("schema-utility-child-complete");
            return;
        }
        let base = std::env::var_os("RPGMIGRATE_TEST_AUTH_BASE")
            .expect("isolated SCRAM fixture is required");
        let authenticated = PgConnection::connect(
            &Conninfo::new(utility_keyword(&base, "password", &password)).unwrap(),
            &io,
        )
        .unwrap();
        let mut base_uri = "postgresql:///?sslmode=disable&service=auth".to_owned();
        for (parameter, name) in [
            ("host", "PGHOST"),
            ("port", "PGPORT"),
            ("dbname", "PGDATABASE"),
        ] {
            base_uri.push_str(&format!(
                "&{parameter}={}",
                uri_value(connection_value(&authenticated, name).as_bytes())
            ));
        }
        for case in [
            "explicit",
            "service-password",
            "service-passfile",
            "insecure",
            "nonregular",
            "wrong-password",
            "unsupported-secret",
            "unsupported-fallback",
        ] {
            let files = ToolFixtures(PrivateFiles::new().unwrap());
            let root = &files.0.directory;
            let passfile = root.join("passfile");
            if case == "nonregular" {
                fs::DirBuilder::new().mode(0o700).create(&passfile).unwrap();
            } else {
                let value = if case == "wrong-password" {
                    OsStr::new("synthetic-wrong-password")
                } else {
                    &password
                };
                let mut contents = b"*:*:*:*:".to_vec();
                for byte in value.as_bytes() {
                    if matches!(byte, b'\\' | b':') {
                        contents.push(b'\\');
                    }
                    contents.push(*byte);
                }
                contents.push(b'\n');
                utility_private_file(&passfile, &contents);
                if case == "insecure" {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&passfile, fs::Permissions::from_mode(0o644)).unwrap();
                }
            }
            let mut service =
                b"[auth]\nsslmode=disable\noptions=-c statement_timeout=23456\n".to_vec();
            service.extend_from_slice(if case == "explicit" {
                b"user=synthetic-wrong-role\n"
            } else {
                b"user=e1_auth\n"
            });
            for (key, value) in [
                (
                    "sslrootcert",
                    root.join(OsString::from_vec(b"root-service-\xff".to_vec())),
                ),
                (
                    "sslkey",
                    root.join(OsString::from_vec(b"key-service-\xfe".to_vec())),
                ),
            ] {
                service.extend_from_slice(format!("{key}=").as_bytes());
                service.extend_from_slice(value.as_os_str().as_bytes());
                service.push(b'\n');
            }
            if [
                "service-passfile",
                "insecure",
                "nonregular",
                "wrong-password",
            ]
            .contains(&case)
            {
                service.extend_from_slice(b"passfile=");
                service.extend_from_slice(passfile.as_os_str().as_bytes());
                service.push(b'\n');
            } else {
                service.extend_from_slice(b"password=");
                service.extend_from_slice(if case == "explicit" {
                    b"synthetic-wrong-password"
                } else {
                    password.as_bytes()
                });
                service.push(b'\n');
            }
            if case == "unsupported-secret" {
                service.extend_from_slice(b"sslpassword=synthetic-private-secret\n");
            }
            if case == "unsupported-fallback" {
                service
                    .extend_from_slice(b"fallback_application_name=synthetic-private-fallback\n");
            }
            let service_path = root.join("service.conf");
            utility_private_file(&service_path, &service);
            let mut info = base_uri.clone();
            if case == "explicit" {
                info.push_str(&format!(
                    "&user=e1_auth&password={}&sslrootcert={}&options={}",
                    uri_value(password.as_bytes()),
                    uri_value(
                        root.join(OsString::from_vec(b"root-explicit-\xff".to_vec()))
                            .as_os_str()
                            .as_bytes()
                    ),
                    uri_value(b"-c statement_timeout=12345")
                ));
            }
            let mut child = utility_child(TEST, case, root);
            child
                .env("RPGMIGRATE_TEST_AUTH_BASE", info)
                .env("PGUSER", "synthetic-wrong-inherited-role")
                .env("PGSERVICEFILE", &service_path)
                .env("PGPASSFILE", root.join("wrong-inherited-passfile"))
                .env("PGSSLROOTCERT", root.join("wrong-inherited-root"))
                .env("PGSSLKEY", root.join("wrong-inherited-key"))
                .env(
                    "PGSSLCERT",
                    root.join(OsString::from_vec(b"cert-environment-\xfd".to_vec())),
                )
                .env("PGOPTIONS", "-c statement_timeout=34567");
            if case == "explicit" || case == "service-password" {
                child.env("PGPASSWORD", "synthetic-wrong-password");
            }
            utility_child_completed(&mut child);
        }
    }

    #[test]
    #[ignore = "requires an owned PostgreSQL schema fixture"]
    fn live_utility_service_precedence_preserves_validated_database() {
        const TEST: &str =
            "schema::tests::live_utility_service_precedence_preserves_validated_database";
        let io = utility_io();
        let (context, mut admin) = utility_context(&io);
        if std::env::var_os("RPGMIGRATE_TEST_UTILITY_CHILD").is_some() {
            let root = PathBuf::from(std::env::var_os("RPGMIGRATE_TEST_UTILITY_ROOT").unwrap());
            let config: serde_json::Value =
                serde_json::from_slice(&fs::read(root.join("case.json")).unwrap()).unwrap();
            let source_info =
                Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_SCHEMA_SOURCE_ADMIN").unwrap())
                    .unwrap();
            let target_info =
                Conninfo::new(std::env::var_os("RPGMIGRATE_TEST_SCHEMA_TARGET_ADMIN").unwrap())
                    .unwrap();
            let mut source = PgConnection::connect(&source_info, &io).unwrap();
            let mut target = PgConnection::connect(&target_info, &io).unwrap();
            for (connection, expected) in [
                (&source, config["source"].as_str().unwrap()),
                (&target, config["target"].as_str().unwrap()),
            ] {
                assert!(
                    connection_value(connection, "PGDATABASE") == expected,
                    "native connection selected the decoy"
                );
                assert!(
                    connection_value(connection, "PGHOST").as_bytes()
                        == context["socket_dir"].as_str().unwrap().as_bytes(),
                    "selected socket was not pinned"
                );
                let settings = connection.utility_environment().unwrap();
                assert!(settings.remove.iter().any(|name| name == "PGHOSTADDR"));
                assert!(
                    !settings
                        .values
                        .iter()
                        .any(|(name, _)| name == "PGHOSTADDR" || name == "PGSERVICE")
                );
            }
            let beta_info = Conninfo::new(config["beta_uri"].as_str().unwrap().into()).unwrap();
            let mut beta = PgConnection::connect(&beta_info, &io).unwrap();
            let before_beta = catalog::clone_object_inventory(&mut beta, &io).unwrap();
            source.query("CREATE SCHEMA utility_route", &io).unwrap();
            source
                .query("CREATE TABLE utility_route.empty_item(id integer)", &io)
                .unwrap();
            source
                .query(
                    "CREATE INDEX utility_route_index ON utility_route.empty_item(id)",
                    &io,
                )
                .unwrap();
            let mut snapshot = crate::snapshot::CloneSnapshot::export(
                source,
                crate::contracts::CopyGeneration(1),
                &io,
            )
            .unwrap();
            let policy = RestorePolicy {
                no_owner: true,
                no_acl: true,
                ..RestorePolicy::default()
            };
            let plan = catalog::plan_clone(
                snapshot.connection().unwrap(),
                &mut target,
                crate::contracts::CopyGeneration(1),
                policy,
                crate::contracts::CloneCopyPolicy::default(),
                &[],
                &[],
                &io,
            )
            .unwrap();
            let options = CloneOptions {
                source: source_info,
                target: target_info,
                state: crate::contracts::StateRequest {
                    path: root.join("state"),
                    action: crate::contracts::StateAction::Create,
                },
                operator: None,
                table_jobs: 1,
                post_data_jobs: 1,
                use_copy_binary: false,
                split_tables_larger_than: 0,
                split_max_parts: 8,
                skip_large_objects: false,
                large_objects_jobs: 4,
                policy,
                not_consistent: false,
            };
            let mut state = crate::state::open_clone(&options.state, None, &io).unwrap();
            state.bind_clone(&plan, snapshot.ticket()).unwrap();
            preflight(snapshot.connection().unwrap(), &target, &plan, &io).unwrap();
            let archive = dump(&options, snapshot.ticket(), &state, &io).unwrap();
            let before_target = catalog::clone_object_inventory(&mut target, &io).unwrap();
            let service = root.join("service.conf");
            let original_service = fs::read(&service).unwrap();
            let mut unsupported = original_service.clone();
            unsupported.extend_from_slice(b"sslpassword=private-fixture-secret\n");
            fs::write(&service, unsupported).unwrap();
            let bad_target = PgConnection::connect(&options.target, &io).unwrap();
            let failure = match preflight(snapshot.connection().unwrap(), &bad_target, &plan, &io) {
                Err(error) => error,
                Ok(_) => panic!("unmapped service secret must be refused"),
            };
            assert!(
                failure.message
                    == "PostgreSQL utility connection settings cannot be represented safely"
            );
            assert!(restore_pre(&options, &plan, &archive, &mut state, &io).is_err());
            assert!(
                state.checked_clone().unwrap().unwrap().phase == ClonePhase::Fresh,
                "unsupported settings recorded a restore intent"
            );
            assert!(
                catalog::clone_object_inventory(&mut target, &io).unwrap() == before_target,
                "unsupported settings mutated target"
            );
            fs::write(&service, original_service).unwrap();
            let objects = restore_pre(&options, &plan, &archive, &mut state, &io).unwrap();
            assert!(
                objects.iter().any(|object| object.name == "empty_item"),
                "pre-data restore omitted its relation"
            );
            assert!(
                compare_pre_data(&options, snapshot.ticket(), &[], &io).unwrap(),
                "snapshot schema comparison used a different database"
            );
            restore_post(&options, &archive, &io).unwrap();
            assert!(
                compare(&options, &[], &io).unwrap(),
                "schema comparison used a different database"
            );
            let result = target
                .query("SELECT count(*) FROM utility_route.empty_item", &io)
                .unwrap();
            assert!(
                result.cell(0, 0).unwrap() == Some(b"0"),
                "synthetic relation is not empty"
            );
            let result = target
                .query(
                    "SELECT to_regclass('utility_route.utility_route_index') IS NOT NULL",
                    &io,
                )
                .unwrap();
            assert!(
                result.cell(0, 0).unwrap() == Some(b"t"),
                "post-data restore omitted its index"
            );
            let mut captured_dump =
                utility(&archive.tools.dump, snapshot.connection().unwrap()).unwrap();
            captured_dump
                .args([
                    "--format=custom",
                    "--schema-only",
                    "--no-publications",
                    "--no-subscriptions",
                ])
                .arg(format!("--snapshot={}", snapshot.ticket().token))
                .arg("--file")
                .arg(root.join("captured.dump"));
            let mut source_probe = utility_probe(
                snapshot.connection().unwrap(),
                &context,
                config["source"].as_str().unwrap(),
            );
            let mut target_probe =
                utility_probe(&target, &context, config["target"].as_str().unwrap());
            fs::remove_file(service).unwrap();
            assert!(
                run(&mut captured_dump, &io, None).unwrap().success(),
                "captured snapshot dump reread the service"
            );
            assert!(
                run(&mut source_probe, &io, None).unwrap().success(),
                "captured source changed database"
            );
            assert!(
                run(&mut target_probe, &io, None).unwrap().success(),
                "captured target changed database"
            );
            assert!(
                catalog::clone_object_inventory(&mut beta, &io).unwrap() == before_beta,
                "decoy inventory changed"
            );
            snapshot.finish(&io).unwrap();
            println!("schema-utility-child-complete");
            return;
        }
        for case in ["explicit", "inherited"] {
            let files = ToolFixtures(PrivateFiles::new().unwrap());
            let suffix = files
                .0
                .directory
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .strip_prefix("rpgmigrate-schema-")
                .unwrap();
            let names = [
                format!("e1_util_s_{suffix}"),
                format!("e1_util_t_{suffix}"),
                format!("e1_util_b_{suffix}"),
            ];
            for name in &names {
                let name = admin.quote_identifier(name).unwrap();
                admin
                    .query(&format!("CREATE DATABASE {name} TEMPLATE template0"), &io)
                    .unwrap();
            }
            let source_uri = utility_uri(&admin, &names[0]);
            let target_uri = utility_uri(&admin, &names[1]);
            let beta_uri = utility_uri(&admin, &names[2]);
            utility_private_file(&files.0.directory.join("case.json"), &serde_json::to_vec(&serde_json::json!({"source": names[0], "target": names[1], "beta_uri": beta_uri})).unwrap());
            let service = files.0.directory.join("service.conf");
            utility_private_file(&service, format!("[decoy]\nhost={}\nport={}\ndbname={}\nuser=e1_admin\nsslmode=disable\noptions=-c statement_timeout=12000\n", context["socket_dir"].as_str().unwrap(), context["port"].as_u64().unwrap(), names[2]).as_bytes());
            let mut child = utility_child(TEST, case, &files.0.directory);
            child
                .env("PGSERVICEFILE", &service)
                .env("PGDATABASE", &names[2])
                .env("PGOPTIONS", "-c statement_timeout=9000");
            let missing = files.0.directory.join("missing-socket");
            let selected = context["socket_dir"].as_str().unwrap();
            let source_uri = format!(
                "{source_uri}&host={}",
                uri_value(format!("{},{}", missing.display(), selected).as_bytes())
            );
            if case == "explicit" {
                child
                    .env(
                        "RPGMIGRATE_TEST_SCHEMA_SOURCE_ADMIN",
                        format!("{source_uri}&service=decoy"),
                    )
                    .env(
                        "RPGMIGRATE_TEST_SCHEMA_TARGET_ADMIN",
                        format!("{target_uri}&service=decoy"),
                    );
            } else {
                child
                    .env("PGSERVICE", "decoy")
                    .env("RPGMIGRATE_TEST_SCHEMA_SOURCE_ADMIN", source_uri)
                    .env("RPGMIGRATE_TEST_SCHEMA_TARGET_ADMIN", target_uri);
            }
            utility_child_completed(&mut child);
        }
        utility_selected_peer_loss(&context, &io);
    }

    #[test]
    fn utility_discovery_child() {
        let Some(root) = std::env::var_os("RPGMIGRATE_TEST_TOOL_ROOT") else {
            return;
        };
        let major = std::env::var("RPGMIGRATE_TEST_TOOL_MAJOR")
            .unwrap()
            .parse()
            .unwrap();
        let io = IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )));
        let result = tools_in(major, &io, Path::new(&root), false);
        if let Some(expected) = std::env::var_os("RPGMIGRATE_TEST_TOOL_DIRECTORY") {
            let selected = result.unwrap();
            assert_eq!(selected.dump, Path::new(&expected).join("pg_dump"));
            assert_eq!(selected.restore, Path::new(&expected).join("pg_restore"));
            assert_eq!(selected.major, major);
        } else {
            let error = result
                .err()
                .expect("invalid tool selection must be refused");
            assert_eq!(
                error.message,
                std::env::var("RPGMIGRATE_TEST_TOOL_ERROR").unwrap()
            );
        }
        println!("utility-discovery-child-complete");
    }

    #[test]
    fn utility_discovery_preserves_overrides_and_selects_packaged_target_major() {
        let fixtures = ToolFixtures(PrivateFiles::new().unwrap());
        let root = fixtures.0.directory.join("stock");
        let stock16 = fixtures.pair("stock/16/bin", 16, 16);
        let stock18 = fixtures.pair("stock/18/bin", 18, 18);
        let path16 = fixtures.pair("path16", 16, 16);
        let path18 = fixtures.pair("path18", 18, 18);
        let mixed = fixtures.pair("mixed/16/bin", 16, 18);
        let broken_root = fixtures.0.directory.join("broken");
        fs::create_dir_all(broken_root.join("16/bin")).unwrap();
        let absent = fixtures.0.directory.join("absent");
        let config16 = fixtures.config("config16", path16.to_str().unwrap());
        let config18 = fixtures.config("config18", path18.to_str().unwrap());
        let config_empty = fixtures.config("config-empty", "");
        let mismatch = "pg_dump and pg_restore must match the target major";
        let missing = "matching target-major pg_dump and pg_restore were not found";
        let empty = Path::new("");
        let cases = [
            (
                "implicit16-path18",
                16,
                &root,
                &path18,
                None,
                None,
                Ok(stock16.as_path()),
            ),
            (
                "implicit18-path16",
                18,
                &root,
                &path16,
                None,
                None,
                Ok(stock18.as_path()),
            ),
            (
                "absent-stock-path16",
                16,
                &absent,
                &path16,
                None,
                None,
                Ok(path16.as_path()),
            ),
            (
                "absent-stock-path18",
                18,
                &absent,
                &path18,
                None,
                None,
                Ok(path18.as_path()),
            ),
            (
                "absent-stock-wrong-path",
                16,
                &absent,
                &path18,
                None,
                None,
                Err(mismatch),
            ),
            (
                "absent-stock-empty-path",
                16,
                &absent,
                &absent,
                None,
                None,
                Err(missing),
            ),
            (
                "broken-stock-no-fallback",
                16,
                &broken_root,
                &path16,
                None,
                None,
                Err(missing),
            ),
            (
                "mixed-stock-no-fallback",
                16,
                &fixtures.0.directory.join("mixed"),
                &path16,
                None,
                None,
                Err(mismatch),
            ),
            (
                "bindir16-over-config18",
                16,
                &root,
                &path18,
                Some(path16.as_path()),
                Some(config18.as_path()),
                Ok(path16.as_path()),
            ),
            (
                "bindir18",
                18,
                &root,
                &path16,
                Some(path18.as_path()),
                None,
                Ok(path18.as_path()),
            ),
            (
                "bindir-mismatch",
                16,
                &root,
                &path16,
                Some(path18.as_path()),
                None,
                Err(mismatch),
            ),
            (
                "bindir-mixed-tools",
                16,
                &root,
                &path16,
                Some(mixed.as_path()),
                None,
                Err(mismatch),
            ),
            (
                "bindir-missing",
                16,
                &root,
                &path16,
                Some(absent.as_path()),
                None,
                Err(missing),
            ),
            (
                "bindir-empty",
                16,
                &root,
                &path16,
                Some(empty),
                Some(config16.as_path()),
                Err("PG_BINDIR must select an existing tool directory"),
            ),
            (
                "config16",
                16,
                &root,
                &path18,
                None,
                Some(config16.as_path()),
                Ok(path16.as_path()),
            ),
            (
                "config18",
                18,
                &root,
                &path16,
                None,
                Some(config18.as_path()),
                Ok(path18.as_path()),
            ),
            (
                "config-mismatch",
                16,
                &root,
                &path16,
                None,
                Some(config18.as_path()),
                Err(mismatch),
            ),
            (
                "config-empty-output",
                16,
                &root,
                &path16,
                None,
                Some(config_empty.as_path()),
                Err("pg_config returned an invalid tool directory"),
            ),
            (
                "config-empty",
                16,
                &root,
                &path16,
                None,
                Some(empty),
                Err("cannot start PostgreSQL utility"),
            ),
        ];
        let mut failures = Vec::new();
        for (name, major, stock, path, bindir, config, expected) in cases {
            let mut child = Command::new(std::env::current_exe().unwrap());
            child
                .args([
                    "--exact",
                    "schema::tests::utility_discovery_child",
                    "--nocapture",
                ])
                .env_remove("PG_BINDIR")
                .env_remove("PG_CONFIG")
                .env_remove("RPGMIGRATE_TEST_TOOL_DIRECTORY")
                .env_remove("RPGMIGRATE_TEST_TOOL_ERROR")
                .env("PATH", path)
                .env("RPGMIGRATE_TEST_TOOL_ROOT", stock)
                .env("RPGMIGRATE_TEST_TOOL_MAJOR", major.to_string());
            if let Some(path) = bindir {
                child.env("PG_BINDIR", path);
            }
            if let Some(path) = config {
                child.env("PG_CONFIG", path);
            }
            match expected {
                Ok(directory) => {
                    child.env("RPGMIGRATE_TEST_TOOL_DIRECTORY", directory);
                }
                Err(message) => {
                    child.env("RPGMIGRATE_TEST_TOOL_ERROR", message);
                }
            }
            let output = child.output().unwrap();
            if !output.status.success()
                || !String::from_utf8_lossy(&output.stdout)
                    .contains("utility-discovery-child-complete")
            {
                failures.push(format!(
                    "{name}: {}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn utility_version_requires_the_actual_named_qualified_major() {
        assert_eq!(
            tool_major(b"pg_dump (PostgreSQL) 18.6\n", "pg_dump").unwrap(),
            18
        );
        for bytes in [
            b"pg_restore (PostgreSQL) 18.6\n".as_slice(),
            b"pg_dump (PostgreSQL) 17.7\n",
            b"18.6\n",
        ] {
            assert!(tool_major(bytes, "pg_dump").is_err());
        }
    }
    #[test]
    fn schema_pattern_is_exact_even_for_wildcards_and_quotes() {
        assert_eq!(exact_schema_pattern("a*?\"b"), "\"a*?\"\"b\"");
    }
    #[test]
    fn normalization_preserves_header_looking_function_body_bytes() {
        let input=b"--\n-- PostgreSQL database dump\n--\n\n-- Dumped from database version 16.15\n-- Dumped by pg_dump version 18.6\n\nCREATE FUNCTION x() RETURNS text AS $$-- Dumped from database version evil$$ LANGUAGE sql;\n";
        let mut reader = BufReader::new(input.as_slice());
        normalized_header(&mut reader).unwrap();
        let mut tail = String::new();
        reader.read_to_string(&mut tail).unwrap();
        assert!(tail.contains("-- Dumped from database version evil"));
    }
    #[test]
    fn normalization_requires_the_complete_provenance_header() {
        for input in [
            b"".as_slice(),
            b"--\n-- PostgreSQL database dump\n--\n\nCREATE TABLE x();\n",
        ] {
            assert!(normalized_header(&mut BufReader::new(input)).is_err());
        }
    }
    #[test]
    fn private_utility_files_do_not_survive_owner_drop() {
        let directory;
        {
            let mut files = PrivateFiles::new().unwrap();
            directory = files.directory.clone();
            files.file("archive").unwrap();
            assert!(directory.join("archive").is_file());
        }
        assert!(!directory.exists());
    }
    #[test]
    fn silent_utility_does_not_inherit_expired_sql_io_budget() {
        let io = IoControl {
            deadline: Instant::now(),
            cancelled: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let status = run(
            Command::new("/bin/sh").args(["-c", "sleep 0.05"]),
            &io,
            None,
        )
        .unwrap();
        assert!(status.success());
    }
    #[test]
    fn cancellation_reaps_a_silent_utility_process_group() {
        let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let io = IoControl {
            deadline: Instant::now(),
            cancelled: cancelled.clone(),
        };
        let signal = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cancelled.store(true, Ordering::Release);
        });
        let started = Instant::now();
        let result = run(
            Command::new("/bin/sh").args(["-c", "sleep 30 & wait"]),
            &io,
            None,
        );
        signal.join().unwrap();
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(3));
    }
    #[test]
    fn public_baseline_owner_change_never_adopts_the_existing_namespace() {
        let baseline = OwnedObject {
            class_oid: 2615,
            object_oid: 2200,
            object_sub_id: 0,
            namespace_oid: Some(2200),
            namespace_name: Some("public".into()),
            name: "public".into(),
            kind: "namespace".into(),
            owner_oid: 6171,
        };
        let mut changed = baseline.clone();
        changed.owner_oid = 42;
        assert!(
            created_objects(
                std::slice::from_ref(&baseline),
                vec![changed.clone()],
                RestorePolicy::default()
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            created_objects(
                std::slice::from_ref(&baseline),
                vec![changed],
                RestorePolicy {
                    no_owner: true,
                    ..RestorePolicy::default()
                }
            )
            .is_err()
        );
        let mut replaced = baseline.clone();
        replaced.object_oid += 1;
        assert!(
            created_objects(
                std::slice::from_ref(&baseline),
                vec![replaced],
                RestorePolicy::default()
            )
            .is_err()
        );
        let mut metadata = baseline.clone();
        metadata.name = "metadata".into();
        metadata.namespace_name = Some("metadata".into());
        let mut changed = metadata.clone();
        changed.owner_oid = 42;
        assert!(created_objects(&[metadata], vec![changed], RestorePolicy::default()).is_err());
    }
    #[test]
    #[ignore = "requires an owned PostgreSQL schema fixture"]
    fn live_pre_data_guard_refuses_schema_drift_without_target_mutation() {
        use crate::contracts::{CopyGeneration, StateAction, StateRequest};
        std::env::var_os("RPGMIGRATE_TEST_CONTEXT").expect("owned schema harness is required");
        let io = IoControl::unbounded(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
            false,
        )));
        let mut files = PrivateFiles::new().unwrap();
        let options = CloneOptions {
            source: Conninfo::new(
                std::env::var_os("RPGMIGRATE_TEST_SCHEMA_SOURCE_ADMIN")
                    .expect("owned schema source is required"),
            )
            .unwrap(),
            target: Conninfo::new(
                std::env::var_os("RPGMIGRATE_TEST_SCHEMA_TARGET_ADMIN")
                    .expect("owned schema target is required"),
            )
            .unwrap(),
            state: StateRequest {
                path: files.directory.join("unused-state"),
                action: StateAction::Create,
            },
            operator: None,
            table_jobs: 4,
            post_data_jobs: 4,
            use_copy_binary: false,
            split_tables_larger_than: 0,
            split_max_parts: 8,
            skip_large_objects: false,
            large_objects_jobs: 4,
            policy: RestorePolicy::default(),
            not_consistent: false,
        };
        let mut source = PgConnection::connect(&options.source, &io).unwrap();
        let mut target = PgConnection::connect(&options.target, &io).unwrap();
        for sql in [
            "CREATE SCHEMA guard",
            "CREATE FUNCTION guard.constant() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE TABLE guard.item(id integer GENERATED ALWAYS AS IDENTITY, payload varchar(12) DEFAULT 'base', computed integer GENERATED ALWAYS AS (id+1) STORED)",
            "CREATE INDEX item_payload ON guard.item(payload)",
            "CREATE TABLE guard.root_a(id integer) PARTITION BY RANGE(id)",
            "CREATE TABLE guard.root_b(id integer) PARTITION BY RANGE(id)",
            "CREATE TABLE guard.leaf PARTITION OF guard.root_a FOR VALUES FROM (0) TO (100)",
            "CREATE TABLE guard.parent_a(id integer)",
            "CREATE TABLE guard.parent_b(id integer)",
            "CREATE TABLE guard.child(id integer) INHERITS(guard.parent_a)",
        ] {
            source.query(sql, &io).unwrap();
        }
        let tools = tools(
            catalog::read_clone_target(&mut target, &io).unwrap().major,
            &io,
        )
        .unwrap();
        let (archive, _) = files.file("initial.dump").unwrap();
        let mut command = utility(&tools.dump, &source).unwrap();
        command
            .args([
                "--format=custom",
                "--schema-only",
                "--quote-all-identifiers",
                "--no-publications",
                "--no-subscriptions",
            ])
            .arg("--file")
            .arg(&archive);
        assert!(run(&mut command, &io, None).unwrap().success());
        let mut command = utility(&tools.restore, &target).unwrap();
        command
            .args([
                "--dbname=",
                "--section=pre-data",
                "--single-transaction",
                "--exit-on-error",
            ])
            .arg(&archive);
        assert!(run(&mut command, &io, None).unwrap().success());
        target
            .query(
                "INSERT INTO guard.item(id,payload) OVERRIDING SYSTEM VALUE VALUES (7,'sentinel')",
                &io,
            )
            .unwrap();
        let verify = || {
            let mut exporter = PgConnection::connect(&options.source, &io).unwrap();
            exporter
                .query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY", &io)
                .unwrap();
            let source = catalog::read_clone_source(&mut exporter, &io).unwrap();
            let exported = exporter
                .query("SELECT pg_catalog.pg_export_snapshot()", &io)
                .unwrap();
            let token = std::str::from_utf8(exported.cell(0, 0).unwrap().unwrap())
                .unwrap()
                .to_owned();
            let ticket = SnapshotTicket {
                source,
                token,
                generation: CopyGeneration(2),
            };
            let equal = compare_pre_data(&options, &ticket, &[], &io).unwrap();
            exporter.query("ROLLBACK", &io).unwrap();
            equal
        };
        assert!(verify(), "unchanged pre-data witness differs");
        let cases: &[(&str, &[&str], &[&str])] = &[
            (
                "type",
                &["ALTER TABLE guard.item ALTER COLUMN payload TYPE varchar(24)"],
                &["ALTER TABLE guard.item ALTER COLUMN payload TYPE varchar(12)"],
            ),
            (
                "default",
                &["ALTER TABLE guard.item ALTER COLUMN payload SET DEFAULT 'changed'"],
                &["ALTER TABLE guard.item ALTER COLUMN payload SET DEFAULT 'base'"],
            ),
            (
                "generated",
                &[
                    "ALTER TABLE guard.item DROP COLUMN computed",
                    "ALTER TABLE guard.item ADD COLUMN computed integer GENERATED ALWAYS AS (id+2) STORED",
                ],
                &[
                    "ALTER TABLE guard.item DROP COLUMN computed",
                    "ALTER TABLE guard.item ADD COLUMN computed integer GENERATED ALWAYS AS (id+1) STORED",
                ],
            ),
            (
                "identity",
                &["ALTER TABLE guard.item ALTER COLUMN id SET GENERATED BY DEFAULT"],
                &["ALTER TABLE guard.item ALTER COLUMN id SET GENERATED ALWAYS"],
            ),
            (
                "function",
                &[
                    "CREATE OR REPLACE FUNCTION guard.constant() RETURNS integer LANGUAGE sql AS $$ SELECT 2 $$",
                ],
                &[
                    "CREATE OR REPLACE FUNCTION guard.constant() RETURNS integer LANGUAGE sql AS $$ SELECT 1 $$",
                ],
            ),
            (
                "partition",
                &[
                    "ALTER TABLE guard.root_a DETACH PARTITION guard.leaf",
                    "ALTER TABLE guard.root_b ATTACH PARTITION guard.leaf FOR VALUES FROM (0) TO (100)",
                ],
                &[
                    "ALTER TABLE guard.root_b DETACH PARTITION guard.leaf",
                    "ALTER TABLE guard.root_a ATTACH PARTITION guard.leaf FOR VALUES FROM (0) TO (100)",
                ],
            ),
            (
                "inheritance",
                &[
                    "ALTER TABLE guard.child NO INHERIT guard.parent_a",
                    "ALTER TABLE guard.child INHERIT guard.parent_b",
                ],
                &[
                    "ALTER TABLE guard.child NO INHERIT guard.parent_b",
                    "ALTER TABLE guard.child INHERIT guard.parent_a",
                ],
            ),
        ];
        for (label, changed, reset) in cases {
            for sql in *changed {
                source.query(sql, &io).unwrap();
            }
            assert!(!verify(), "{label} drift was not refused");
            let preserved = target
                .query(
                    "SELECT count(*)=1 AND min(id)=7 AND min(payload)='sentinel' FROM guard.item",
                    &io,
                )
                .unwrap();
            assert_eq!(
                preserved.cell(0, 0).unwrap(),
                Some(b"t".as_slice()),
                "guard mutated target rows"
            );
            for sql in *reset {
                source.query(sql, &io).unwrap();
            }
            assert!(
                verify(),
                "{label} reset did not restore the pre-data witness"
            );
        }
    }
}
