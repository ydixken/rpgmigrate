use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Read};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::catalog;
use crate::contracts::{
    CloneOptions, ClonePlan, CloneTarget, Conninfo, Error, ErrorKind, IoControl, OwnedObject,
    RestorePolicy, Result, SnapshotTicket,
};
use crate::pg::{self, PgConnection};
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

fn capture(program: &Path, args: &[&str], io: &IoControl) -> Result<Vec<u8>> {
    let mut files = PrivateFiles::new()?;
    let (path, file) = files.file("output")?;
    let status = run(
        Command::new(program).args(args).stdout(file),
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
    let explicit = if let Some(path) = std::env::var_os("PG_BINDIR") {
        if path.is_empty() {
            return Err(failure("PG_BINDIR must select an existing tool directory"));
        }
        Some(PathBuf::from(path))
    } else if let Some(config) = std::env::var_os("PG_CONFIG") {
        let bytes = capture(Path::new(&config), &["--bindir"], io)?;
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
    let directories = if let Some(directory) = explicit {
        vec![directory]
    } else {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()).collect()
    };
    for directory in directories {
        let dump = directory.join("pg_dump");
        let restore = directory.join("pg_restore");
        if !dump.is_file() || !restore.is_file() {
            continue;
        }
        let dump_version = tool_major(&capture(&dump, &["--version"], io)?, "pg_dump")?;
        let restore_version = tool_major(&capture(&restore, &["--version"], io)?, "pg_restore")?;
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

fn utility(program: &Path, conninfo: &Conninfo) -> Result<Command> {
    let environment = pg::utility_environment(conninfo)?;
    let mut options = environment
        .iter()
        .find(|(name, _)| name == "PGOPTIONS")
        .map(|(_, value)| value.clone())
        .or_else(|| std::env::var_os("PGOPTIONS"))
        .unwrap_or_default();
    options.push(
        " -c synchronous_commit=on -c row_security=off -c client_encoding=UTF8 -c lc_monetary=C",
    );
    let mut command = Command::new(program);
    command
        .envs(environment)
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

pub(crate) fn preflight(options: &CloneOptions, plan: &ClonePlan, io: &IoControl) -> Result<()> {
    let _ = tools(plan.target.major, io)?;
    let _ = pg::utility_environment(&options.source)?;
    let _ = pg::utility_environment(&options.target)?;
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
    let mut command = utility(&tools.dump, &options.source)?;
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
    state.begin_pre_data()?;
    let mut command = utility(&archive.tools.restore, &options.target)?;
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
    let mut command = utility(&archive.tools.restore, &options.target)?;
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
    connection: &Conninfo,
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
    let tools = tools(target_identity.major, io)?;
    let mut files = PrivateFiles::new()?;
    let (source_path, _) = files.file("source.sql")?;
    let (target_path, _) = files.file("target.sql")?;
    comparison_dump(
        options,
        &options.source,
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
        &options.target,
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
        let mut command = utility(&tools.dump, &options.source).unwrap();
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
        let mut command = utility(&tools.restore, &options.target).unwrap();
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
