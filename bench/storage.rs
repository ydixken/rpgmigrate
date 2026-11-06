#[path = "storage/config.rs"]
pub mod config;
#[path = "storage/format.rs"]
pub mod format;
#[path = "../src/identity.rs"]
mod identity;
#[path = "storage/journal.rs"]
pub mod journal;
#[path = "storage/redb.rs"]
pub mod redb;
#[path = "storage/sqlite.rs"]
pub mod sqlite;

use config::{Error, Result};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

pub const CASES: &str = include_str!("storage/cases.json");

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u32,
    pub seed: u64,
    pub repetitions: u32,
    pub candidates: Vec<String>,
    pub cases: Vec<Case>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Case {
    pub name: String,
    pub transactions: u64,
    pub events_per_transaction: u64,
    pub event_bytes: u64,
}

pub fn parse_manifest(input: &str) -> Result<Manifest> {
    let manifest: Manifest = serde_json::from_str(input)
        .map_err(|_| Error::Workload("invalid storage workload manifest"))?;
    let mut candidates = manifest
        .candidates
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    candidates.sort_unstable();
    let mut names = manifest
        .cases
        .iter()
        .map(|case| case.name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    if manifest.version != 1
        || !(1..=10).contains(&manifest.repetitions)
        || candidates != ["redb", "segmented", "sqlite"]
        || names != ["giant-transaction", "small-commits", "wide-events"]
    {
        return Err(Error::Workload(
            "storage manifest must include every required candidate and case",
        ));
    }
    for case in &manifest.cases {
        case.counts(format::Limits::default())?;
        if (case.name == "giant-transaction"
            && (case.transactions != 1
                || case.events_per_transaction * case.event_bytes <= 64 * 1024 * 1024))
            || (case.name == "wide-events" && case.event_bytes <= 1024 * 1024)
            || (case.name == "small-commits" && case.transactions < 2)
        {
            return Err(Error::Workload(
                "storage case does not exercise its required shape",
            ));
        }
    }
    Ok(manifest)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Counts {
    pub frames: u64,
    pub completed: u64,
    pub encoded_bytes: u64,
}

impl Case {
    pub fn counts(&self, limits: format::Limits) -> Result<Counts> {
        if !(1..=10_000).contains(&self.transactions)
            || !(1..=1_000_000).contains(&self.events_per_transaction)
            || !(1..=limits.max_event_bytes).contains(&self.event_bytes)
            || limits.max_frame_bytes < format::COMPLETED_BYTES
        {
            return Err(Error::Workload(
                "storage workload is empty or exceeds the pilot limit",
            ));
        }
        let parts = self
            .event_bytes
            .div_ceil((limits.max_frame_bytes - format::HEADER_BYTES) as u64);
        if parts > u64::from(limits.max_parts) {
            return Err(Error::Workload("storage event exceeds the part limit"));
        }
        let events = self
            .transactions
            .checked_mul(self.events_per_transaction)
            .ok_or(Error::Overflow)?;
        let frames = events.checked_mul(parts).ok_or(Error::Overflow)?;
        let encoded_bytes = events
            .checked_mul(self.event_bytes)
            .and_then(|bytes| {
                frames
                    .checked_mul(format::HEADER_BYTES as u64)
                    .and_then(|headers| bytes.checked_add(headers))
            })
            .and_then(|bytes| {
                self.transactions
                    .checked_mul(format::COMPLETED_BYTES as u64)
                    .and_then(|descriptors| bytes.checked_add(descriptors))
            })
            .ok_or(Error::Overflow)?;
        if encoded_bytes > 512 * 1024 * 1024 {
            return Err(Error::Workload(
                "storage workload exceeds 512 MiB canonical bytes",
            ));
        }
        Ok(Counts {
            frames,
            completed: self.transactions,
            encoded_bytes,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WriteReceipt {
    pub counts: Counts,
    pub batches: u64,
    pub batch_crc32: u32,
    pub durable_frontier: u64,
}

struct Input<'a> {
    cfg: &'a config::Config,
    case: &'a Case,
    seed: u64,
    transaction: u64,
    event: u64,
    offset: u64,
    tx_bytes: u64,
}

impl<'a> Input<'a> {
    fn new(cfg: &'a config::Config, case: &'a Case, seed: u64) -> Result<Self> {
        cfg.validate()?;
        let totals = case.counts(cfg.limits)?;
        Ok(Self {
            cfg,
            case,
            seed,
            transaction: 1,
            event: 0,
            offset: 0,
            tx_bytes: totals.encoded_bytes / case.transactions,
        })
    }

    fn next_record(&mut self) -> Result<Option<format::Record>> {
        use format::{CommitLsn, CompletedTx, Frame, Record, SourceEndLsn, TxId, TxSeq};
        if self.transaction > self.case.transactions {
            return Ok(None);
        }
        let tx = TxId {
            stream: self.cfg.stream,
            xid: self.transaction as u32,
            begin_ordinal: self.transaction,
        };
        if self.event == self.case.events_per_transaction {
            let completed = CompletedTx {
                tx,
                order: TxSeq(self.transaction),
                commit: Some(CommitLsn(self.transaction * 2)),
                end: SourceEndLsn(self.transaction * 2 + 1),
                event_count: self.event,
                encoded_bytes: self.tx_bytes,
            };
            self.transaction += 1;
            self.event = 0;
            return Ok(Some(Record::Completed(completed)));
        }
        let capacity = (self.cfg.limits.max_frame_bytes - format::HEADER_BYTES) as u64;
        let size = capacity.min(self.case.event_bytes - self.offset) as usize;
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(size)
            .map_err(|_| Error::Format(format::Error::Allocation))?;
        let base = self.seed ^ self.transaction.rotate_left(17) ^ self.event.rotate_left(37);
        for index in 0..size {
            let position = self.offset + index as u64;
            payload.push((mix(base.wrapping_add(position / 8)) >> (8 * (position % 8))) as u8);
        }
        let frame = Frame {
            tx,
            event_ordinal: self.event,
            part: (self.offset / capacity) as u32,
            last_part: self.offset + size as u64 == self.case.event_bytes,
            payload,
        };
        self.offset += size as u64;
        if frame.last_part {
            self.event += 1;
            self.offset = 0;
        }
        Ok(Some(Record::Frame(frame)))
    }
}

fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

fn record_size(record: &format::Record) -> u64 {
    match record {
        format::Record::Frame(frame) => (format::HEADER_BYTES + frame.payload.len()) as u64,
        format::Record::Completed(_) => format::COMPLETED_BYTES as u64,
    }
}

pub fn write_case(
    store: &mut config::Store,
    cfg: &config::Config,
    case: &Case,
    seed: u64,
) -> Result<WriteReceipt> {
    let expected = case.counts(cfg.limits)?;
    let mut input = Input::new(cfg, case, seed)?;
    let mut pending = input.next_record()?;
    let mut receipt = WriteReceipt {
        counts: Counts::default(),
        batches: 0,
        batch_crc32: 0,
        durable_frontier: 0,
    };
    let mut boundaries = format::StreamingChecksum::new();
    while pending.is_some() {
        let mut batch_counts = Counts::default();
        let stats = store.with_batch(|batch| {
            while let Some(record) = pending.as_ref() {
                let size = record_size(record);
                if batch_counts.encoded_bytes + size > cfg.batch_bytes {
                    break;
                }
                match record {
                    format::Record::Frame(frame) => {
                        batch.append(frame)?;
                        batch_counts.frames += 1;
                    }
                    format::Record::Completed(tx) => {
                        batch.complete(tx)?;
                        batch_counts.completed += 1;
                    }
                }
                batch_counts.encoded_bytes += size;
                pending = input.next_record()?;
            }
            Ok(())
        })?;
        receipt.counts.frames += batch_counts.frames;
        receipt.counts.completed += batch_counts.completed;
        receipt.counts.encoded_bytes += batch_counts.encoded_bytes;
        if stats.frames != batch_counts.frames
            || stats.completed != batch_counts.completed
            || stats.encoded_bytes != batch_counts.encoded_bytes
            || stats.durable_frontier.0 != receipt.counts.completed
        {
            return Err(Error::Workload(
                "storage batch receipt differs from the input oracle",
            ));
        }
        receipt.durable_frontier = stats.durable_frontier.0;
        receipt.batches += 1;
        boundaries.update(&stats.encoded_bytes.to_le_bytes());
        boundaries.update(&stats.durable_frontier.0.to_le_bytes());
    }
    if receipt.counts != expected || receipt.batches == 0 {
        return Err(Error::Workload("storage write omitted required input"));
    }
    receipt.batch_crc32 = boundaries.finish();
    Ok(receipt)
}

pub fn verify_case(
    store: &config::Store,
    cfg: &config::Config,
    case: &Case,
    seed: u64,
) -> Result<u32> {
    let mut input = Input::new(cfg, case, seed)?;
    let mut checksum = format::StreamingChecksum::new();
    store.scan_complete(format::TxSeq(0), |record| {
        if input.next_record()?.as_ref() != Some(record) {
            return Err(Error::Workload(
                "storage replay differs from the independent input stream",
            ));
        }
        let bytes = match record {
            format::Record::Frame(frame) => format::encode_frame(frame, cfg.limits)?,
            format::Record::Completed(tx) => format::encode_completed(tx, cfg.limits)?,
        };
        checksum.update(&bytes);
        Ok(())
    })?;
    if input.next_record()?.is_some() {
        return Err(Error::Workload("storage replay omitted required input"));
    }
    Ok(checksum.finish())
}

pub fn wait_child(child: &mut Child, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return if status.success() {
                Ok(())
            } else {
                Err(Error::Workload("storage child failed"))
            };
        }
        if Instant::now() >= deadline {
            child.kill()?;
            let reap_deadline = Instant::now() + Duration::from_secs(2);
            while child.try_wait()?.is_none() {
                if Instant::now() >= reap_deadline {
                    return Err(Error::Workload(
                        "storage child did not exit after termination",
                    ));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            return Err(Error::Workload("storage child exceeded its deadline"));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

pub fn sqlite_build_check(overrides: &[(&str, bool)]) -> Result<()> {
    if overrides.iter().any(|(_, present)| *present) {
        return Err(Error::Config(
            "SQLite build overrides are not allowed in this experiment",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Timings {
    pub create_ns: u64,
    pub write_ns: u64,
    pub checkpoint_ns: u64,
    pub close_ns: u64,
    pub reopen_ns: u64,
    pub verified_scan_ns: u64,
    pub finalize_ns: u64,
    pub total_ns: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub version: u32,
    pub candidate: String,
    pub case: String,
    pub repetition: u32,
    pub pid: u32,
    pub seed: u64,
    pub required_prefix: u64,
    pub writes: WriteReceipt,
    pub input_crc32: u32,
    pub timings: Timings,
    pub peak_rss_bytes: u64,
    pub configured_cache_bytes: usize,
    pub settings: serde_json::Value,
    pub build: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Job {
    pub case: usize,
    pub candidate: usize,
    pub repetition: u32,
}

pub fn schedule(manifest: &Manifest) -> Vec<Job> {
    let mut jobs = Vec::new();
    let mut random = manifest.seed;
    let mut shuffle = |items: &mut [usize]| {
        for index in (1..items.len()).rev() {
            random = random.wrapping_add(0x9e3779b97f4a7c15);
            items.swap(index, (mix(random) % (index as u64 + 1)) as usize);
        }
    };
    for repetition in 0..manifest.repetitions {
        let mut cases = [0, 1, 2];
        shuffle(&mut cases);
        for case in cases {
            let mut candidates = [0, 1, 2];
            shuffle(&mut candidates);
            for candidate in candidates {
                jobs.push(Job {
                    case,
                    candidate,
                    repetition,
                });
            }
        }
    }
    jobs
}

pub fn validate_report(report: &Report, manifest: &Manifest, job: Job, pid: u32) -> Result<()> {
    let case = manifest
        .cases
        .get(job.case)
        .ok_or(Error::Workload("unknown storage case"))?;
    let name = manifest
        .candidates
        .get(job.candidate)
        .ok_or(Error::Workload("unknown storage candidate"))?;
    let timings = &report.timings;
    let phase_sum = [
        timings.create_ns,
        timings.write_ns,
        timings.checkpoint_ns,
        timings.close_ns,
        timings.reopen_ns,
        timings.verified_scan_ns,
        timings.finalize_ns,
    ]
    .into_iter()
    .try_fold(0u64, |sum, value| {
        sum.checked_add(value).ok_or(Error::Overflow)
    })?;
    if report.version != 1
        || report.case != case.name
        || report.candidate != *name
        || report.pid != pid
        || pid == 0
        || report.repetition != job.repetition
        || job.repetition >= manifest.repetitions
        || report.seed != manifest.seed
        || report.required_prefix != case.transactions
        || report.writes.counts != case.counts(format::Limits::default())?
        || report.writes.durable_frontier != case.transactions
        || report.writes.batches == 0
        || report.peak_rss_bytes == 0
        || report.configured_cache_bytes != 64 * 1024 * 1024
        || !report
            .settings
            .as_object()
            .is_some_and(|object| !object.is_empty())
        || report.build["sqlite_source_id"] != SQLITE_SOURCE_ID
        || report.build["sqlite_build_overrides_rejected"] != true
        || timings.total_ns == 0
        || timings.total_ns < phase_sum
        || timings.write_ns == 0
        || timings.reopen_ns == 0
        || timings.verified_scan_ns == 0
    {
        return Err(Error::Workload(
            "storage child receipt does not match the required job",
        ));
    }
    Ok(())
}

pub fn validate_reports(manifest: &Manifest, reports: &[Report]) -> Result<()> {
    let jobs = schedule(manifest);
    if reports.is_empty() || reports.len() != jobs.len() {
        return Err(Error::Workload(
            "storage experiment omitted required receipts",
        ));
    }
    for (job, report) in jobs.into_iter().zip(reports) {
        validate_report(report, manifest, job, report.pid)?;
        if reports.iter().any(|other| {
            other.case == report.case
                && (other.input_crc32 != report.input_crc32
                    || other.writes.batches != report.writes.batches
                    || other.writes.batch_crc32 != report.writes.batch_crc32)
        }) {
            return Err(Error::Workload(
                "storage candidates used different input or batch boundaries",
            ));
        }
    }
    Ok(())
}

const SQLITE_SOURCE_ID: &str =
    "2026-10-16 14:45:20 555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8e6ca76";

fn build_info() -> Result<serde_json::Value> {
    macro_rules! checked_overrides {
        ($($name:literal),+ $(,)?) => { &[$(($name, option_env!($name).is_some() || std::env::var_os($name).is_some())),+] };
    }
    sqlite_build_check(checked_overrides!(
        "LIBSQLITE3_SYS_USE_PKG_CONFIG",
        "LIBSQLITE3_FLAGS",
        "SQLITE_MAX_VARIABLE_NUMBER",
        "SQLITE_MAX_EXPR_DEPTH",
        "SQLITE_MAX_COLUMN",
        "SQLITE3_LIB_DIR",
        "SQLITE3_INCLUDE_DIR",
        "SQLITE3_STATIC",
        "CFLAGS",
        "HOST_CFLAGS",
        "TARGET_CFLAGS",
        "CFLAGS_aarch64-apple-darwin",
        "CFLAGS_aarch64_apple_darwin",
        "CFLAGS_aarch64-unknown-linux-gnu",
        "CFLAGS_aarch64_unknown_linux_gnu",
        "CFLAGS_x86_64-unknown-linux-gnu",
        "CFLAGS_x86_64_unknown_linux_gnu",
        "CFLAGS_x86_64-apple-darwin",
        "CFLAGS_x86_64_apple_darwin"
    ))?;
    let sqlite = rusqlite::Connection::open_in_memory()
        .map_err(|_| Error::Backend("SQLite build probe failed"))?;
    let source: String = sqlite
        .query_row("SELECT sqlite_source_id()", [], |row| row.get(0))
        .map_err(|_| Error::Backend("SQLite source probe failed"))?;
    if rusqlite::version() != "3.54.1" || source != SQLITE_SOURCE_ID {
        return Err(Error::Config(
            "SQLite does not match the reviewed bundled source",
        ));
    }
    let mut options = sqlite
        .prepare("PRAGMA compile_options")
        .map_err(|_| Error::Backend("SQLite options probe failed"))?;
    let compile_options: Vec<String> = options
        .query_map([], |row| row.get(0))
        .map_err(|_| Error::Backend("SQLite options probe failed"))?
        .collect::<std::result::Result<_, _>>()
        .map_err(|_| Error::Backend("SQLite options probe failed"))?;
    Ok(
        serde_json::json!({"os": std::env::consts::OS, "architecture": std::env::consts::ARCH,
        "debug_assertions": cfg!(debug_assertions), "bundled_sqlite_feature": true,
        "sqlite_build_overrides_rejected": true, "sqlite_version": rusqlite::version(),
        "sqlite_source_id": source, "sqlite_compile_options": compile_options, "redb_version": "4.5.0"}),
    )
}

fn elapsed(start: Instant) -> Result<u64> {
    u64::try_from(start.elapsed().as_nanos()).map_err(|_| Error::Overflow)
}

pub fn peak_rss_bytes() -> Result<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage writes this valid output buffer; fields are read only after success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: successful getrusage initialized the rusage structure above.
    let raw =
        u64::try_from(unsafe { usage.assume_init() }.ru_maxrss).map_err(|_| Error::Overflow)?;
    if cfg!(target_os = "linux") {
        raw.checked_mul(1024).ok_or(Error::Overflow)
    } else if cfg!(target_os = "macos") {
        Ok(raw)
    } else {
        Err(Error::Config(
            "storage RSS measurement requires Linux or macOS",
        ))
    }
}

fn candidate_name(candidate: config::Candidate) -> &'static str {
    match candidate {
        config::Candidate::Segmented => "segmented",
        config::Candidate::Redb => "redb",
        config::Candidate::Sqlite => "sqlite",
    }
}

fn candidate(name: &str) -> Result<config::Candidate> {
    match name {
        "segmented" => Ok(config::Candidate::Segmented),
        "redb" => Ok(config::Candidate::Redb),
        "sqlite" => Ok(config::Candidate::Sqlite),
        _ => Err(Error::Workload("unknown storage candidate")),
    }
}

pub fn measure_case(
    candidate: config::Candidate,
    cfg: &config::Config,
    case: &Case,
    seed: u64,
    repetition: u32,
) -> Result<Report> {
    cfg.validate()?;
    case.counts(cfg.limits)?;
    let build = build_info()?;
    let total = Instant::now();
    let mut timings = Timings::default();
    let phase = Instant::now();
    let mut store = config::Store::create(candidate, cfg)?;
    timings.create_ns = elapsed(phase)?;
    let phase = Instant::now();
    let writes = write_case(&mut store, cfg, case, seed)?;
    timings.write_ns = elapsed(phase)?;
    let phase = Instant::now();
    store.checkpoint()?;
    timings.checkpoint_ns = elapsed(phase)?;
    let phase = Instant::now();
    drop(store);
    timings.close_ns = elapsed(phase)?;
    let phase = Instant::now();
    let mut reopened = config::Store::reopen(candidate, cfg, format::TxSeq(case.transactions))?;
    timings.reopen_ns = elapsed(phase)?;
    let phase = Instant::now();
    let input_crc32 = verify_case(&reopened, cfg, case, seed)?;
    timings.verified_scan_ns = elapsed(phase)?;
    let settings = reopened.settings()?;
    let phase = Instant::now();
    reopened.checkpoint()?;
    drop(reopened);
    timings.finalize_ns = elapsed(phase)?;
    timings.total_ns = elapsed(total)?;
    Ok(Report {
        version: 1,
        candidate: candidate_name(candidate).into(),
        case: case.name.clone(),
        repetition,
        pid: std::process::id(),
        seed,
        required_prefix: case.transactions,
        writes,
        input_crc32,
        timings,
        peak_rss_bytes: peak_rss_bytes()?,
        configured_cache_bytes: cfg.cache_bytes,
        settings,
        build,
    })
}

fn file_stem(manifest: &Manifest, job: Job) -> String {
    format!(
        "{}-{}-{}",
        manifest.cases[job.case].name, manifest.candidates[job.candidate], job.repetition
    )
}

fn config_for(root: &Path, manifest: &Manifest, job: Job) -> config::Config {
    let mut run = [0; 16];
    run[..8].copy_from_slice(&manifest.seed.to_le_bytes());
    run[8..].copy_from_slice(&(!manifest.seed).to_le_bytes());
    config::Config {
        dir: root.join(file_stem(manifest, job)),
        stream: format::StreamId {
            run: format::RunId(run),
            source: format::SourceId {
                system_identifier: 1,
                database_oid: 1,
            },
            generation: 1,
        },
        limits: format::Limits::default(),
        batch_bytes: 8 * 1024 * 1024,
        cache_bytes: 64 * 1024 * 1024,
        segment_bytes: 64 * 1024 * 1024,
    }
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)
        .map_err(|_| Error::Workload("storage report could not be written"))?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

pub fn read_report(path: &Path) -> Result<Report> {
    read_json(path)
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err(Error::Workload("storage report is not a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(256 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 256 * 1024 {
        return Err(Error::Workload("storage report exceeds its size limit"));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::Workload("storage report is missing required fields"))
}

fn run_parent(root: &Path, manifest: &Manifest) -> Result<()> {
    build_info()?;
    fs::DirBuilder::new().mode(0o700).create(root)?;
    let root = root.canonicalize()?;
    write_json(&root.join("cases.json"), manifest)?;
    let exe = std::env::current_exe()?;
    let mut reports = Vec::new();
    for job in schedule(manifest) {
        let mut child = Command::new(&exe)
            .arg("--child")
            .arg(&root)
            .arg(job.case.to_string())
            .arg(job.candidate.to_string())
            .arg(job.repetition.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?;
        let pid = child.id();
        wait_child(&mut child, Duration::from_secs(300))?;
        let report = read_report(&root.join(format!("{}.json", file_stem(manifest, job))))?;
        validate_report(&report, manifest, job, pid)?;
        reports.push(report);
    }
    validate_reports(manifest, &reports)?;
    let rates: Vec<_> = reports.iter().map(|report| {
        let seconds = report.timings.total_ns as f64 / 1_000_000_000.0;
        serde_json::json!({"candidate": report.candidate, "case": report.case, "repetition": report.repetition,
            "storage_completed_transactions_per_second_total": report.writes.counts.completed as f64 / seconds,
            "storage_encoded_bytes_per_second_total": report.writes.counts.encoded_bytes as f64 / seconds})
    }).collect();
    let result = serde_json::json!({"version": 1, "qualification": "single-stream single-writer synchronous pilot; no backend selection",
        "cache_state": "warm-after-write; fresh directory is not cold-cache proof", "rss_scope": "fresh child RUSAGE_SELF per candidate/case/repetition; cache budget is not RSS",
        "timing_scope": "create, write, checkpoint, close, reopen integrity, two-pass verified scan, final checkpoint and close",
        "ordering": "seeded paired candidate shuffle within seeded case order", "manifest": manifest,
        "reports": reports, "storage_throughput": rates});
    write_json(&root.join("report.json"), &result)?;
    File::open(&root)?.sync_all()?;
    println!(
        "{}",
        serde_json::to_string(&result)
            .map_err(|_| Error::Workload("storage summary serialization failed"))?
    );
    Ok(())
}

pub mod c1 {
    use super::*;
    use std::os::fd::RawFd;

    pub const CANDIDATES: [&str; 3] = ["segmented", "redb", "sqlite"];
    pub const ROLES: [&str; 3] = ["receive", "apply", "status"];
    pub const RECEIVE_CHECKS: &[&str] = &[
        "duplicate-role",
        "partial-hidden",
        "reverse-begin",
        "reused-xid",
        "peer-preserved",
        "stale-cas",
        "restart-ordinal",
        "duplicate-identity-rollback",
    ];
    pub const APPLY_CHECKS: &[&str] = &[
        "duplicate-role",
        "commits-visible",
        "peer-preserved",
        "stale-cas",
        "cursor-persisted",
    ];
    pub const STATUS_CHECKS: &[&str] = &[
        "multiple-status",
        "cross-process-role-refusal",
        "partial-hidden",
        "stable-two-pass",
        "new-view-sees-publish",
        "after-exclusive",
        "readonly",
        "visitor-abort-reusable",
        "required-prefix-gap",
    ];

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Head {
        pub receive_revision: u64,
        pub apply_revision: u64,
        pub next_record: u64,
        pub published: u64,
        pub consumer_cursor: u64,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Receipt {
        pub version: u32,
        pub candidate: String,
        pub role: String,
        pub pid: u32,
        pub required_prefix: u64,
        pub head: Head,
        pub counts: Counts,
        pub input_crc32: u32,
        pub checks: Vec<String>,
    }

    const FINAL_HEAD: Head = Head {
        receive_revision: 5,
        apply_revision: 3,
        next_record: 9,
        published: 4,
        consumer_cursor: 4,
    };
    const COUNTS: Counts = Counts {
        frames: 4,
        completed: 4,
        encoded_bytes: 1024,
    };

    fn checks(role: &str) -> Result<&'static [&'static str]> {
        match role {
            "receive" => Ok(RECEIVE_CHECKS),
            "apply" => Ok(APPLY_CHECKS),
            "status" => Ok(STATUS_CHECKS),
            _ => Err(Error::Workload("unknown C1 role")),
        }
    }

    pub fn validate_receipts(receipts: &[Receipt], pids: &[u32], input_crc32: u32) -> Result<()> {
        if receipts.len() != 9 || pids.len() != 9 {
            return Err(Error::Workload("C1 omitted required role receipts"));
        }
        for (index, receipt) in receipts.iter().enumerate() {
            let role = ROLES[index % 3];
            if receipt.version != 1
                || receipt.candidate != CANDIDATES[index / 3]
                || receipt.role != role
                || receipt.pid == 0
                || receipt.pid != pids[index]
                || pids[..index].contains(&receipt.pid)
                || receipt.required_prefix != 4
                || receipt.head != FINAL_HEAD
                || receipt.counts != COUNTS
                || receipt.input_crc32 != input_crc32
                || receipt.checks != checks(role)?
            {
                return Err(Error::Workload(
                    "C1 role receipt differs from the scheduled proof",
                ));
            }
        }
        Ok(())
    }

    pub struct Channel {
        input: RawFd,
        output: RawFd,
        deadline: Instant,
    }

    impl Channel {
        pub fn new(input: RawFd, output: RawFd, deadline: Instant) -> Result<Self> {
            for fd in [input, output] {
                // SAFETY: these borrowed descriptors remain open for the channel's lifetime.
                let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
                if flags < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                // SAFETY: F_SETFL takes an integer flag value for the same open descriptor.
                let changed = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
                if changed < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            Ok(Self {
                input,
                output,
                deadline,
            })
        }
        fn wait_ready(&self, fd: RawFd, events: libc::c_short) -> Result<()> {
            loop {
                let remaining = self
                    .deadline
                    .checked_duration_since(Instant::now())
                    .ok_or(Error::Workload("C1 child exceeded its deadline"))?;
                let mut poll = libc::pollfd {
                    fd,
                    events,
                    revents: 0,
                };
                let timeout = remaining
                    .as_millis()
                    .saturating_add(1)
                    .min(i32::MAX as u128) as i32;
                // SAFETY: poll points to one initialized pollfd for the duration of the call.
                let ready = unsafe { libc::poll(&mut poll, 1, timeout) };
                if ready == 0 {
                    continue;
                }
                if ready < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(error.into());
                }
                return Ok(());
            }
        }
        fn transfer(&self, fd: RawFd, bytes: &mut [u8], writing: bool) -> Result<()> {
            let mut offset = 0;
            while offset < bytes.len() {
                self.wait_ready(fd, if writing { libc::POLLOUT } else { libc::POLLIN })?;
                let tail = &mut bytes[offset..];
                // SAFETY: tail is a live, exclusive buffer of the supplied length; fd remains open.
                let transferred = unsafe {
                    if writing {
                        libc::write(fd, tail.as_ptr().cast(), tail.len())
                    } else {
                        libc::read(fd, tail.as_mut_ptr().cast(), tail.len())
                    }
                };
                if transferred == 0 {
                    return Err(Error::Workload("C1 pipe closed before its receipt"));
                }
                if transferred < 0 {
                    let error = std::io::Error::last_os_error();
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
                    ) {
                        continue;
                    }
                    return Err(error.into());
                }
                offset += transferred as usize;
            }
            Ok(())
        }
        pub fn send(&self, value: &impl Serialize) -> Result<()> {
            let mut bytes = [0; 4096];
            let mut output = std::io::Cursor::new(bytes.as_mut_slice());
            serde_json::to_writer(&mut output, value)
                .map_err(|_| Error::Workload("C1 pipe message exceeds its bound"))?;
            let length = output.position() as usize;
            self.transfer(self.output, &mut (length as u32).to_le_bytes(), true)?;
            self.transfer(self.output, &mut bytes[..length], true)
        }
        pub fn receive<T: serde::de::DeserializeOwned>(&self) -> Result<T> {
            let mut length = [0; 4];
            self.transfer(self.input, &mut length, false)?;
            let length = u32::from_le_bytes(length) as usize;
            if length == 0 || length > 4096 {
                return Err(Error::Workload("C1 pipe message exceeds its bound"));
            }
            let mut bytes = [0; 4096];
            self.transfer(self.input, &mut bytes[..length], false)?;
            serde_json::from_slice(&bytes[..length])
                .map_err(|_| Error::Workload("invalid C1 pipe message"))
        }
        pub fn require_eof(&self) -> Result<()> {
            // A successful exit does not prove that finished was the last output.
            loop {
                self.wait_ready(self.input, libc::POLLIN)?;
                let mut byte = [0u8; 1];
                // SAFETY: byte is a live, exclusive one-byte buffer; input remains open.
                let read = unsafe { libc::read(self.input, byte.as_mut_ptr().cast(), byte.len()) };
                if read == 0 {
                    return Ok(());
                }
                if read > 0 {
                    return Err(Error::Workload("C1 child wrote after finished"));
                }
                let error = std::io::Error::last_os_error();
                if !matches!(
                    error.kind(),
                    std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
                ) {
                    return Err(error.into());
                }
            }
        }
    }

    fn require(ok: bool, message: &'static str) -> Result<()> {
        if ok {
            Ok(())
        } else {
            Err(Error::Workload(message))
        }
    }

    fn role(name: &str) -> Result<config::Role> {
        match name {
            "receive" => Ok(config::Role::Receive),
            "apply" => Ok(config::Role::Apply),
            "status" => Ok(config::Role::Status),
            _ => Err(Error::Workload("unknown C1 role")),
        }
    }

    fn configuration(root: &Path, name: &str) -> Result<config::Config> {
        candidate(name)?;
        let manifest = parse_manifest(CASES)?;
        let mut cfg = config_for(
            root,
            &manifest,
            Job {
                case: 0,
                candidate: 0,
                repetition: 0,
            },
        );
        cfg.dir = root.join(name);
        Ok(cfg)
    }

    fn records(cfg: &config::Config) -> Result<Vec<format::Record>> {
        let case = Case {
            name: "C1".into(),
            transactions: 4,
            events_per_transaction: 1,
            event_bytes: 40,
        };
        let mut input = Input::new(cfg, &case, parse_manifest(CASES)?.seed)?;
        let mut records = Vec::with_capacity(8);
        while let Some(mut record) = input.next_record()? {
            let tx = match &mut record {
                format::Record::Frame(frame) => &mut frame.tx,
                format::Record::Completed(tx) => {
                    tx.commit = Some(format::CommitLsn(10));
                    tx.end = format::SourceEndLsn(11);
                    &mut tx.tx
                }
            };
            let (begin, xid) = [(20, 7), (10, 8), (30, 7), (40, 9)][tx.begin_ordinal as usize - 1];
            tx.begin_ordinal = begin;
            tx.xid = xid;
            records.push(record);
        }
        require(records.len() == 8, "C1 canonical fixture is incomplete")?;
        Ok(records)
    }

    fn encoded(record: &format::Record, cfg: &config::Config) -> Result<Vec<u8>> {
        Ok(match record {
            format::Record::Frame(frame) => format::encode_frame(frame, cfg.limits)?,
            format::Record::Completed(tx) => format::encode_completed(tx, cfg.limits)?.to_vec(),
        })
    }

    fn input_crc32(cfg: &config::Config) -> Result<u32> {
        let mut checksum = format::StreamingChecksum::new();
        for record in records(cfg)? {
            checksum.update(&encoded(&record, cfg)?);
        }
        Ok(checksum.finish())
    }

    fn head(view: &config::ReadView<'_>) -> Result<Head> {
        let head = view.head();
        require(
            head.catalog_version == config::CATALOG_VERSION,
            "C1 catalog version differs",
        )?;
        Ok(Head {
            receive_revision: head.receive_revision,
            apply_revision: head.apply_revision,
            next_record: head.next_record.get(),
            published: head.published.map_or(0, |tx| tx.order.0),
            consumer_cursor: head.consumer_cursor.0,
        })
    }

    fn snapshot(store: &config::Store) -> Result<Head> {
        let mut saved = None;
        store.with_read_view(|view| {
            saved = Some(head(view)?);
            Ok(())
        })?;
        saved.ok_or(Error::Workload("C1 read-view callback was omitted"))
    }

    fn verify(
        view: &config::ReadView<'_>,
        cfg: &config::Config,
        after: u64,
        through: u64,
        mut first: impl FnMut() -> Result<()>,
    ) -> Result<u32> {
        require(after <= through && through <= 4, "invalid C1 scan oracle")?;
        let records = records(cfg)?;
        let mut expected = records[after as usize * 2..through as usize * 2].iter();
        let mut checksum = format::StreamingChecksum::new();
        let mut seen = 0;
        view.scan_complete(format::TxSeq(after), |record| {
            require(
                expected.next() == Some(record),
                "C1 scan differs from canonical input",
            )?;
            if seen == 0 {
                first()?;
            }
            seen += 1;
            checksum.update(&encoded(record, cfg)?);
            Ok(())
        })?;
        require(
            expected.next().is_none() && seen == (through - after) * 2,
            "C1 scan omitted records",
        )?;
        Ok(checksum.finish())
    }

    fn transaction(
        store: &mut config::Store,
        cfg: &config::Config,
        sequence: usize,
        revision: u64,
    ) -> Result<()> {
        let records = records(cfg)?;
        let format::Record::Frame(frame) = &records[(sequence - 1) * 2] else {
            unreachable!()
        };
        let format::Record::Completed(tx) = &records[sequence * 2 - 1] else {
            unreachable!()
        };
        let stats = store.with_receive(revision, |batch| {
            batch.append(frame)?;
            batch.complete(tx)
        })?;
        require(
            stats.frames == 1
                && stats.completed == 1
                && stats.encoded_bytes == 256
                && stats.receive_revision == revision + 1
                && stats.durable_frontier.0 == sequence as u64,
            "C1 receive receipt differs from the input oracle",
        )
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Message {
        candidate: String,
        role: String,
        pid: u32,
        phase: String,
        head: Head,
    }

    fn command(channel: &Channel, expected: &str) -> Result<()> {
        require(
            channel.receive::<String>()? == expected,
            "C1 command is out of order",
        )
    }

    fn acknowledge(
        channel: &Channel,
        name: &str,
        role: &str,
        phase: &str,
        head: Head,
    ) -> Result<()> {
        channel.send(&Message {
            candidate: name.into(),
            role: role.into(),
            pid: std::process::id(),
            phase: phase.into(),
            head,
        })
    }

    fn receive_steps(
        store: config::Store,
        cfg: &config::Config,
        name: &str,
        channel: &Channel,
    ) -> Result<config::Store> {
        let mut store = store;
        command(channel, "partial")?;
        transaction(&mut store, cfg, 1, 0)?;
        let records = records(cfg)?;
        let format::Record::Frame(frame) = &records[2] else {
            unreachable!()
        };
        let stats = store.with_receive(1, |batch| batch.append(frame))?;
        require(
            stats.frames == 1
                && stats.completed == 0
                && stats.encoded_bytes == 128
                && stats.receive_revision == 2
                && stats.durable_frontier.0 == 1,
            "C1 partial batch receipt differs",
        )?;
        store.with_read_view(|view| verify(view, cfg, 0, 1, || Ok(())).map(|_| ()))?;
        acknowledge(channel, name, "receive", "partial", snapshot(&store)?)?;

        command(channel, "complete")?;
        let before = snapshot(&store)?;
        let mut called = false;
        let outcome = store.with_receive(1, |_| {
            called = true;
            Ok(())
        });
        require(
            outcome == Err(Error::Conflict) && !called && snapshot(&store)? == before,
            "C1 stale receive CAS changed state",
        )?;
        let format::Record::Completed(tx) = &records[3] else {
            unreachable!()
        };
        let stats = store.with_receive(2, |batch| batch.complete(tx))?;
        require(
            stats.frames == 0
                && stats.completed == 1
                && stats.encoded_bytes == 128
                && stats.receive_revision == 3
                && stats.durable_frontier.0 == 2,
            "C1 descriptor batch receipt differs",
        )?;
        acknowledge(channel, name, "receive", "complete", snapshot(&store)?)?;

        command(channel, "third")?;
        transaction(&mut store, cfg, 3, 3)?;
        acknowledge(channel, name, "receive", "third", snapshot(&store)?)?;

        command(channel, "restart")?;
        let before = snapshot(&store)?;
        drop(store);
        let mut store = config::Store::reopen(candidate(name)?, cfg, format::TxSeq(3))?;
        require(
            snapshot(&store)? == before,
            "C1 reopen changed catalog or append ordinal",
        )?;
        transaction(&mut store, cfg, 4, 4)?;
        let before = snapshot(&store)?;
        let format::Record::Frame(duplicate) = &records[0] else {
            unreachable!()
        };
        let format::Record::Completed(mut descriptor) = records[1] else {
            unreachable!()
        };
        descriptor.order = format::TxSeq(5);
        let result = store.with_receive(5, |batch| {
            batch.append(duplicate)?;
            batch.complete(&descriptor)
        });
        require(
            result == Err(Error::IdentityMismatch),
            "C1 completion index accepted repeated transaction identity",
        )?;
        require(
            store.with_receive(5, |_| Ok(())) == Err(Error::Poisoned),
            "C1 failed mutator remained usable",
        )?;
        drop(store);
        let store = config::Store::reopen(candidate(name)?, cfg, format::TxSeq(4))?;
        require(
            snapshot(&store)? == before,
            "C1 failed completion changed catalog",
        )?;
        store.with_read_view(|view| verify(view, cfg, 0, 4, || Ok(())).map(|_| ()))?;
        acknowledge(channel, name, "receive", "restart", snapshot(&store)?)?;
        Ok(store)
    }

    fn apply_steps(
        mut store: config::Store,
        cfg: &config::Config,
        name: &str,
        channel: &Channel,
    ) -> Result<config::Store> {
        for (phase, revision, cursor) in [("one", 0, 1), ("two", 1, 2), ("final", 2, 4)] {
            command(channel, phase)?;
            if phase == "one" {
                let reader = config::Store::open_role(
                    candidate(name)?,
                    cfg,
                    config::Role::Status,
                    format::TxSeq(1),
                )?;
                reader.with_read_view(|view| verify(view, cfg, 0, 1, || Ok(())).map(|_| ()))?;
            }
            store.with_read_view(|view| {
                verify(view, cfg, 0, head(view)?.published, || Ok(())).map(|_| ())
            })?;
            let before = snapshot(&store)?;
            store.advance_consumer(revision, format::TxSeq(cursor))?;
            let after = snapshot(&store)?;
            require(
                after.receive_revision == before.receive_revision
                    && after.published == before.published
                    && after.next_record == before.next_record
                    && after.apply_revision == revision + 1
                    && after.consumer_cursor == cursor,
                "C1 Apply overwrote peer fields",
            )?;
            require(
                store.advance_consumer(revision, format::TxSeq(cursor)) == Err(Error::Conflict)
                    && snapshot(&store)? == after,
                "C1 stale Apply CAS changed state",
            )?;
            acknowledge(channel, name, "apply", phase, after)?;
        }
        drop(store);
        config::Store::open_role(candidate(name)?, cfg, config::Role::Apply, format::TxSeq(4))
    }

    fn status_steps(
        mut store: config::Store,
        cfg: &config::Config,
        name: &str,
        channel: &Channel,
    ) -> Result<config::Store> {
        command(channel, "partial")?;
        store.with_read_view(|view| verify(view, cfg, 0, 1, || Ok(())).map(|_| ()))?;
        acknowledge(channel, name, "status", "partial", snapshot(&store)?)?;
        command(channel, "pin")?;
        store.with_read_view(|view| {
            let captured = head(view)?;
            verify(view, cfg, 0, 2, || {
                acknowledge(channel, name, "status", "pinned", captured)?;
                command(channel, "resume")
            })?;
            verify(view, cfg, 0, 2, || Ok(()))?;
            require(
                head(view)? == captured,
                "C1 read view changed while publishers committed",
            )?;
            acknowledge(channel, name, "status", "stable", captured)
        })?;
        command(channel, "latest")?;
        store.with_read_view(|view| {
            verify(view, cfg, 0, 3, || Ok(()))?;
            verify(view, cfg, 1, 3, || Ok(()))?;
            let mut called = false;
            let result = view.scan_complete(format::TxSeq(4), |_| {
                called = true;
                Ok(())
            });
            require(
                result == Err(Error::Gap) && !called,
                "C1 after-prefix gap emitted records",
            )?;
            let stop = Error::Workload("C1 visitor stop");
            require(
                view.scan_complete(format::TxSeq(0), |_| Err(stop.clone())) == Err(stop),
                "C1 visitor abort was ignored",
            )?;
            verify(view, cfg, 2, 3, || Ok(())).map(|_| ())
        })?;
        let before = snapshot(&store)?;
        let mut called = false;
        let result = store.with_receive(before.receive_revision, |_| {
            called = true;
            Ok(())
        });
        require(
            result == Err(Error::RoleViolation)
                && !called
                && store.advance_consumer(before.apply_revision, format::TxSeq(3))
                    == Err(Error::RoleViolation)
                && store.checkpoint() == Err(Error::RoleViolation)
                && snapshot(&store)? == before,
            "C1 Status accepted a mutation",
        )?;
        acknowledge(channel, name, "status", "latest", before)?;
        command(channel, "gap")?;
        require(
            matches!(
                config::Store::open_role(
                    candidate(name)?,
                    cfg,
                    config::Role::Status,
                    format::TxSeq(5)
                ),
                Err(Error::Gap)
            ),
            "C1 external required prefix was inferred from surviving storage",
        )?;
        acknowledge(channel, name, "status", "gap", snapshot(&store)?)?;
        Ok(store)
    }

    pub fn child(root: &Path, name: &str, role_name: &str) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let cfg = configuration(root, name)?;
        let role = role(role_name)?;
        let metadata = fs::symlink_metadata(root)?;
        require(
            metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && metadata.permissions().mode() & 0o777 == 0o700,
            "C1 child root is not a private directory",
        )?;
        let channel = Channel::new(
            libc::STDIN_FILENO,
            libc::STDOUT_FILENO,
            Instant::now() + Duration::from_secs(300),
        )?;
        let result = (|| {
            let store = if role == config::Role::Receive {
                config::Store::create(candidate(name)?, &cfg)?
            } else {
                config::Store::open_role(candidate(name)?, &cfg, role, format::TxSeq(0))?
            };
            if role == config::Role::Status {
                drop(config::Store::open_role(
                    candidate(name)?,
                    &cfg,
                    role,
                    format::TxSeq(0),
                )?);
                for writer in [config::Role::Receive, config::Role::Apply] {
                    require(
                        matches!(
                            config::Store::open_role(
                                candidate(name)?,
                                &cfg,
                                writer,
                                format::TxSeq(0)
                            ),
                            Err(Error::Busy)
                        ),
                        "C1 writer role was acquired by a second OS process",
                    )?;
                }
            } else {
                require(
                    matches!(
                        config::Store::open_role(candidate(name)?, &cfg, role, format::TxSeq(0)),
                        Err(Error::Busy)
                    ),
                    "C1 duplicate role was not refused",
                )?;
            }
            acknowledge(&channel, name, role_name, "ready", snapshot(&store)?)?;
            let store = match role {
                config::Role::Receive => receive_steps(store, &cfg, name, &channel)?,
                config::Role::Apply => apply_steps(store, &cfg, name, &channel)?,
                config::Role::Status => status_steps(store, &cfg, name, &channel)?,
            };
            command(&channel, "finish")?;
            let final_head = snapshot(&store)?;
            require(
                final_head == FINAL_HEAD,
                "C1 final catalog differs from the external oracle",
            )?;
            let mut checksum = None;
            store.with_read_view(|view| {
                checksum = Some(verify(view, &cfg, 0, 4, || Ok(()))?);
                Ok(())
            })?;
            let receipt = Receipt {
                version: 1,
                candidate: name.into(),
                role: role_name.into(),
                pid: std::process::id(),
                required_prefix: 4,
                head: final_head,
                counts: COUNTS,
                input_crc32: checksum.ok_or(Error::Workload("C1 final scan was omitted"))?,
                checks: checks(role_name)?
                    .iter()
                    .map(|check| (*check).into())
                    .collect(),
            };
            drop(store);
            write_json(&root.join(format!("{name}-{role_name}.json")), &receipt)?;
            acknowledge(&channel, name, role_name, "finished", final_head)
        })();
        if let Err(error) = &result {
            write_json(
                &root.join(format!("{name}-{role_name}-error.json")),
                &serde_json::json!({"version":1,"candidate":name,"role":role_name,"pid":std::process::id(),"error":error.to_string()}),
            )?;
        }
        result
    }

    struct Worker {
        child: Child,
        channel: Channel,
        _input: std::process::ChildStdin,
        _output: std::process::ChildStdout,
        name: String,
        role: String,
    }

    impl Worker {
        fn spawn(root: &Path, name: &str, role: &str, deadline: Instant) -> Result<Self> {
            use std::os::fd::AsRawFd;
            let mut child = Command::new(std::env::current_exe()?)
                .arg("--c1-child")
                .arg(root)
                .arg(name)
                .arg(role)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?;
            let input = child.stdin.take().expect("piped child stdin");
            let output = child.stdout.take().expect("piped child stdout");
            let channel = match Channel::new(output.as_raw_fd(), input.as_raw_fd(), deadline) {
                Ok(channel) => channel,
                Err(error) => {
                    let _ = child.kill();
                    let _ = wait_child(&mut child, Duration::from_secs(2));
                    return Err(error);
                }
            };
            Ok(Self {
                child,
                channel,
                _input: input,
                _output: output,
                name: name.into(),
                role: role.into(),
            })
        }
        fn expect(&self, phase: &str, expected: Head) -> Result<()> {
            let message: Message = self.channel.receive().inspect_err(|_| {
                eprintln!("{}", serde_json::json!({"candidate":self.name,"role":self.role,"phase":phase,"error":"C1 receipt missing or invalid"}));
            })?;
            require(
                message.candidate == self.name
                    && message.role == self.role
                    && message.pid == self.child.id()
                    && message.phase == phase
                    && message.head == expected,
                "C1 phase receipt differs from the external oracle",
            )
        }
        fn step(&self, phase: &str, expected: Head) -> Result<()> {
            self.channel.send(&phase)?;
            self.expect(phase, expected)
        }
    }

    impl Drop for Worker {
        fn drop(&mut self) {
            if !matches!(self.child.try_wait(), Ok(Some(_))) {
                let _ = self.child.kill();
                let _ = wait_child(&mut self.child, Duration::from_secs(2));
            }
        }
    }

    pub fn parent(root: &Path) -> Result<()> {
        build_info()?;
        fs::DirBuilder::new().mode(0o700).create(root)?;
        let root = root.canonicalize()?;
        let oracle = input_crc32(&configuration(&root, CANDIDATES[0])?)?;
        let mut receipts = Vec::with_capacity(9);
        let mut pids = Vec::with_capacity(9);
        for name in CANDIDATES {
            let deadline = Instant::now() + Duration::from_secs(300);
            let empty = Head {
                receive_revision: 0,
                apply_revision: 0,
                next_record: 1,
                published: 0,
                consumer_cursor: 0,
            };
            let receive = Worker::spawn(&root, name, "receive", deadline)?;
            receive.expect("ready", empty)?;
            let apply = Worker::spawn(&root, name, "apply", deadline)?;
            apply.expect("ready", empty)?;
            let status = Worker::spawn(&root, name, "status", deadline)?;
            status.expect("ready", empty)?;
            let partial = Head {
                receive_revision: 2,
                apply_revision: 0,
                next_record: 4,
                published: 1,
                consumer_cursor: 0,
            };
            receive.step("partial", partial)?;
            status.step("partial", partial)?;
            let applied_one = Head {
                apply_revision: 1,
                consumer_cursor: 1,
                ..partial
            };
            apply.step("one", applied_one)?;
            let complete = Head {
                receive_revision: 3,
                next_record: 5,
                published: 2,
                ..applied_one
            };
            receive.step("complete", complete)?;
            status.channel.send(&"pin")?;
            status.expect("pinned", complete)?;
            let third = Head {
                receive_revision: 4,
                next_record: 7,
                published: 3,
                ..complete
            };
            receive.step("third", third)?;
            let applied_two = Head {
                apply_revision: 2,
                consumer_cursor: 2,
                ..third
            };
            apply.step("two", applied_two)?;
            status.channel.send(&"resume")?;
            status.expect("stable", complete)?;
            status.step("latest", applied_two)?;
            receive.step(
                "restart",
                Head {
                    receive_revision: 5,
                    next_record: 9,
                    published: 4,
                    ..applied_two
                },
            )?;
            apply.step("final", FINAL_HEAD)?;
            status.step("gap", FINAL_HEAD)?;
            let mut workers = [receive, apply, status];
            for worker in &workers {
                worker.channel.send(&"finish")?;
            }
            for worker in &mut workers {
                worker.expect("finished", FINAL_HEAD)?;
                wait_child(
                    &mut worker.child,
                    deadline.saturating_duration_since(Instant::now()),
                )?;
                worker.channel.require_eof()?;
                let receipt: Receipt =
                    read_json(&root.join(format!("{name}-{}.json", worker.role)))?;
                pids.push(worker.child.id());
                receipts.push(receipt);
            }
        }
        validate_receipts(&receipts, &pids, oracle)?;
        write_json(
            &root.join("c1-report.json"),
            &serde_json::json!({"version":1,
            "qualification":"development storage role coordination; consumer cursor is read progress only",
            "scope":"all history retained; one active source transaction; no target proof, ACK, power-loss or performance qualification",
            "required_prefix":4,"input_crc32":oracle,"receipts":receipts}),
        )?;
        File::open(&root)?.sync_all()?;
        println!(
            "{}",
            serde_json::json!({"c1":"pass","candidates":3,"children":9,"required_prefix":4})
        );
        Ok(())
    }
}

pub fn run(args: &[OsString]) -> Result<()> {
    let manifest = parse_manifest(CASES)?;
    if args.len() == 1 && (args[0] == "--help" || args[0] == "-h") {
        println!(
            "Usage: storage --root NEW_DIRECTORY | --c1-root NEW_DIRECTORY\nRun the storage pilot or C1 role checks in fresh children; existing directories are refused.\nC1 consumer cursors measure read progress only."
        );
        return Ok(());
    }
    if args.len() == 2 && args[0] == "--root" {
        return run_parent(Path::new(&args[1]), &manifest);
    }
    if args.len() == 2 && args[0] == "--c1-root" {
        return c1::parent(Path::new(&args[1]));
    }
    if args.len() == 4 && args[0] == "--c1-child" {
        let name = args[2]
            .to_str()
            .ok_or(Error::Workload("invalid C1 child job"))?;
        let role = args[3]
            .to_str()
            .ok_or(Error::Workload("invalid C1 child job"))?;
        return c1::child(Path::new(&args[1]), name, role);
    }
    if args.len() == 5 && args[0] == "--child" {
        let parse = |value: &OsString| {
            value
                .to_str()
                .and_then(|text| text.parse::<usize>().ok())
                .ok_or(Error::Workload("invalid storage child job"))
        };
        let job = Job {
            case: parse(&args[2])?,
            candidate: parse(&args[3])?,
            repetition: u32::try_from(parse(&args[4])?).map_err(|_| Error::Overflow)?,
        };
        if !schedule(&manifest).contains(&job) {
            return Err(Error::Workload("unknown storage child job"));
        }
        let root = PathBuf::from(&args[1]);
        let metadata = fs::symlink_metadata(&root)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(Error::Config("storage report directory is invalid"));
        }
        let report = measure_case(
            candidate(&manifest.candidates[job.candidate])?,
            &config_for(&root, &manifest, job),
            &manifest.cases[job.case],
            manifest.seed,
            job.repetition,
        )?;
        validate_report(&report, &manifest, job, std::process::id())?;
        return write_json(
            &root.join(format!("{}.json", file_stem(&manifest, job))),
            &report,
        );
    }
    Err(Error::Workload(
        "expected --root, --c1-root NEW_DIRECTORY or --help",
    ))
}

pub fn main() -> ExitCode {
    match run(&std::env::args_os().skip(1).collect::<Vec<_>>()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({"category": "storage_experiment", "error": error.to_string(), "exit": 1})
            );
            ExitCode::FAILURE
        }
    }
}
