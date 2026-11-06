//! Experiment contracts; runtime migration code does not import this module.
//! Backends export `Store::create/reopen(&Config) -> Result<Self>`; verify native identity before database open.
//! `with_batch(&mut self, f: impl FnOnce(&mut RecordSink<'_>) -> Result<()>) -> Result<()>` commits synchronously.
//! `scan_records(&self, visit: &mut RecordSink<'_>) -> Result<()>` streams persisted records in append order.
//! `checkpoint(&mut self) -> Result<()>` checks final synchronization/checkpoint status before timing ends.
//! Open transaction/table/statement handles once per batch, then drop them before commit.

use super::format::{self, CompletedTx, Frame, Limits, Record, StreamId, TxSeq};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

pub type Result<T> = std::result::Result<T, Error>;
pub type RecordSink<'a> = dyn FnMut(&[u8]) -> Result<()> + 'a;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Candidate {
    Segmented,
    Redb,
    Sqlite,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub dir: PathBuf,
    pub stream: StreamId,
    pub limits: Limits,
    pub batch_bytes: u64,
    pub cache_bytes: usize,
    pub segment_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchStats {
    pub frames: u64,
    pub completed: u64,
    pub encoded_bytes: u64,
    pub durable_frontier: TxSeq,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    Config(&'static str),
    Workload(&'static str),
    Backend(&'static str),
    Io(io::ErrorKind),
    Format(format::Error),
    IdentityMismatch,
    Gap,
    Poisoned,
    Overflow,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) | Self::Workload(message) | Self::Backend(message) => {
                f.write_str(message)
            }
            Self::Io(kind) => write!(f, "storage experiment I/O failed: {kind}"),
            Self::Format(error) => error.fmt(f),
            Self::IdentityMismatch => f.write_str("storage experiment identity differs"),
            Self::Gap => f.write_str("required storage transaction prefix is missing"),
            Self::Poisoned => f.write_str("storage batch failed; reopen and validate before reuse"),
            Self::Overflow => f.write_str("storage experiment arithmetic overflowed"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error.kind())
    }
}

impl From<format::Error> for Error {
    fn from(error: format::Error) -> Self {
        Self::Format(error)
    }
}

impl Config {
    pub fn create_directory(&self) -> Result<()> {
        self.validate()?;
        fs::DirBuilder::new().mode(0o700).create(&self.dir)?;
        let mut identity = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.dir.join("identity"))?;
        identity.write_all(&self.identity())?;
        identity.sync_all()?;
        File::open(&self.dir)?.sync_all()?;
        let parent = self
            .dir
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        File::open(parent)?.sync_all()?;
        Ok(())
    }

    pub fn verify_directory(&self) -> Result<()> {
        self.validate()?;
        let directory = fs::symlink_metadata(&self.dir)?;
        if !directory.is_dir() || directory.permissions().mode() & 0o777 != 0o700 {
            return Err(Error::IdentityMismatch);
        }
        let path = self.dir.join("identity");
        let before = fs::symlink_metadata(&path)?;
        if !before.is_file()
            || before.nlink() != 1
            || before.len() != 48
            || before.permissions().mode() & 0o777 != 0o600
        {
            return Err(Error::IdentityMismatch);
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let opened = file.metadata()?;
        if opened.dev() != before.dev() || opened.ino() != before.ino() {
            return Err(Error::IdentityMismatch);
        }
        let mut identity = [0; 48];
        file.read_exact(&mut identity)?;
        if identity != self.identity() {
            return Err(Error::IdentityMismatch);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.dir.as_os_str().is_empty() || self.dir.file_name().is_none() {
            return Err(Error::Config("storage directory must name a path"));
        }
        let tx = format::TxId {
            stream: self.stream,
            xid: 1,
            begin_ordinal: 1,
        };
        format::TxValidator::new(tx, self.limits)?;
        if self.cache_bytes == 0 {
            return Err(Error::Config("storage cache budget must be positive"));
        }
        let largest = u64::try_from(self.limits.max_frame_bytes).map_err(|_| Error::Overflow)?;
        if self.batch_bytes < largest {
            return Err(Error::Config(
                "storage batch cannot hold the largest record",
            ));
        }
        if self.segment_bytes < largest {
            return Err(Error::Config(
                "storage segment cannot hold the largest record",
            ));
        }
        Ok(())
    }

    // Empty stores still need identity evidence before any records exist.
    pub fn identity(&self) -> [u8; 48] {
        let mut identity = [0; 48];
        identity[..8].copy_from_slice(b"RPGEXP01");
        identity[8..12].copy_from_slice(&1u32.to_le_bytes());
        identity[12..28].copy_from_slice(&self.stream.run.0);
        identity[28..36].copy_from_slice(&self.stream.source.system_identifier.to_le_bytes());
        identity[36..40].copy_from_slice(&self.stream.source.database_oid.to_le_bytes());
        identity[40..48].copy_from_slice(&self.stream.generation.to_le_bytes());
        identity
    }
}

pub struct BatchWriter<'a> {
    sink: &'a mut RecordSink<'a>,
    progress: &'a mut Progress,
    limit: u64,
    frames: u64,
    completed: u64,
    bytes: u64,
    failed: bool,
}

pub struct Progress {
    stream: StreamId,
    limits: Limits,
    active: Option<(format::TxId, format::TxValidator)>,
    last: Option<CompletedTx>,
    poisoned: bool,
}

impl Progress {
    pub fn new(cfg: &Config) -> Result<Self> {
        cfg.validate()?;
        Ok(Self {
            stream: cfg.stream,
            limits: cfg.limits,
            active: None,
            last: None,
            poisoned: false,
        })
    }

    pub fn observe(&mut self, record: &Record) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.poisoned = true;
        match record {
            Record::Frame(frame) => self.frame(frame)?,
            Record::Completed(completed) => self.complete(completed)?,
        }
        self.poisoned = false;
        Ok(())
    }

    fn validator(&mut self, tx: format::TxId) -> Result<&mut format::TxValidator> {
        if tx.stream != self.stream {
            return Err(Error::IdentityMismatch);
        }
        if self.active.is_none() {
            if self
                .last
                .is_some_and(|last| tx.begin_ordinal <= last.tx.begin_ordinal)
            {
                return Err(Error::IdentityMismatch);
            }
            self.active = Some((tx, format::TxValidator::new(tx, self.limits)?));
        }
        let (active, validator) = self.active.as_mut().expect("active validator was created");
        if *active != tx {
            return Err(Error::Workload(
                "first storage experiment supports one active source transaction",
            ));
        }
        Ok(validator)
    }

    fn frame(&mut self, frame: &Frame) -> Result<()> {
        self.validator(frame.tx)?.push(frame)?;
        Ok(())
    }

    fn complete(&mut self, completed: &CompletedTx) -> Result<()> {
        if let Some(last) = self.last {
            format::validate_next(&last, completed)?;
        } else if completed.order != TxSeq(1) {
            return Err(Error::Gap);
        }
        self.validator(completed.tx)?;
        self.active
            .take()
            .expect("active validator was created")
            .1
            .finish(completed)?;
        self.last = Some(*completed);
        Ok(())
    }

    pub fn frontier(&self) -> TxSeq {
        self.last.map_or(TxSeq(0), |last| last.order)
    }

    pub fn require_prefix(&self, required: TxSeq) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.frontier() < required {
            return Err(Error::Gap);
        }
        Ok(())
    }

    pub fn active_transactions(&self) -> usize {
        usize::from(self.active.is_some())
    }

    pub fn check_after(&self, after: TxSeq) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if after > self.frontier() {
            return Err(Error::Gap);
        }
        Ok(())
    }
}

impl BatchWriter<'_> {
    pub fn append(&mut self, frame: &Frame) -> Result<()> {
        if self.failed {
            return Err(Error::Poisoned);
        }
        self.failed = true;
        let bytes = format::encode_frame(frame, self.progress.limits)?;
        self.charge(bytes.len())?;
        self.progress.frame(frame)?;
        (self.sink)(&bytes)?;
        self.frames = self.frames.checked_add(1).ok_or(Error::Overflow)?;
        self.failed = false;
        Ok(())
    }

    pub fn complete(&mut self, tx: &CompletedTx) -> Result<()> {
        if self.failed {
            return Err(Error::Poisoned);
        }
        self.failed = true;
        let bytes = format::encode_completed(tx, self.progress.limits)?;
        self.charge(bytes.len())?;
        self.progress.complete(tx)?;
        (self.sink)(&bytes)?;
        self.completed = self.completed.checked_add(1).ok_or(Error::Overflow)?;
        self.failed = false;
        Ok(())
    }

    fn charge(&mut self, size: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(size).map_err(|_| Error::Overflow)?)
            .ok_or(Error::Overflow)?;
        if self.bytes > self.limit {
            return Err(Error::Workload("storage batch byte budget exceeded"));
        }
        Ok(())
    }
}

pub struct Store {
    backend: Backend,
    cfg: Config,
    progress: Progress,
}

enum Backend {
    Segmented(super::journal::Store),
    Redb(super::redb::Store),
    Sqlite(super::sqlite::Store),
}

impl Backend {
    fn with_batch(&mut self, write: impl FnOnce(&mut RecordSink<'_>) -> Result<()>) -> Result<()> {
        match self {
            Self::Segmented(store) => store.with_batch(write),
            Self::Redb(store) => store.with_batch(write),
            Self::Sqlite(store) => store.with_batch(write),
        }
    }
    fn scan_records(&self, visit: &mut RecordSink<'_>) -> Result<()> {
        match self {
            Self::Segmented(store) => store.scan_records(visit),
            Self::Redb(store) => store.scan_records(visit),
            Self::Sqlite(store) => store.scan_records(visit),
        }
    }
    fn checkpoint(&mut self) -> Result<()> {
        match self {
            Self::Segmented(store) => store.checkpoint(),
            Self::Redb(store) => store.checkpoint(),
            Self::Sqlite(store) => store.checkpoint(),
        }
    }
}

impl Store {
    pub fn settings(&self) -> Result<serde_json::Value> {
        match &self.backend {
            Backend::Segmented(store) => store.settings(),
            Backend::Redb(store) => store.settings(),
            Backend::Sqlite(store) => store.settings(),
        }
    }

    pub fn create(candidate: Candidate, cfg: &Config) -> Result<Self> {
        cfg.validate()?;
        let backend = match candidate {
            Candidate::Segmented => Backend::Segmented(super::journal::Store::create(cfg)?),
            Candidate::Redb => Backend::Redb(super::redb::Store::create(cfg)?),
            Candidate::Sqlite => Backend::Sqlite(super::sqlite::Store::create(cfg)?),
        };
        Ok(Self {
            backend,
            cfg: cfg.clone(),
            progress: Progress::new(cfg)?,
        })
    }

    pub fn reopen(candidate: Candidate, cfg: &Config, required_prefix: TxSeq) -> Result<Self> {
        cfg.verify_directory()?;
        let backend = match candidate {
            Candidate::Segmented => Backend::Segmented(super::journal::Store::reopen(cfg)?),
            Candidate::Redb => Backend::Redb(super::redb::Store::reopen(cfg)?),
            Candidate::Sqlite => Backend::Sqlite(super::sqlite::Store::reopen(cfg)?),
        };
        let mut progress = Progress::new(cfg)?;
        backend.scan_records(&mut |bytes| progress.observe(&decode(bytes, cfg.limits)?))?;
        progress.require_prefix(required_prefix)?;
        Ok(Self {
            backend,
            cfg: cfg.clone(),
            progress,
        })
    }

    pub fn with_batch(
        &mut self,
        write: impl FnOnce(&mut BatchWriter<'_>) -> Result<()>,
    ) -> Result<BatchStats> {
        if self.progress.poisoned {
            return Err(Error::Poisoned);
        }
        let mut counts = None;
        let result = self.backend.with_batch(|sink| {
            let mut emit = |bytes: &[u8]| sink(bytes);
            let mut batch = BatchWriter {
                sink: &mut emit,
                progress: &mut self.progress,
                limit: self.cfg.batch_bytes,
                frames: 0,
                completed: 0,
                bytes: 0,
                failed: false,
            };
            write(&mut batch)?;
            if batch.failed {
                return Err(Error::Poisoned);
            }
            if batch.frames == 0 && batch.completed == 0 {
                return Err(Error::Workload("empty storage batch"));
            }
            counts = Some((batch.frames, batch.completed, batch.bytes));
            Ok(())
        });
        if let Err(error) = result {
            self.progress.poisoned = true;
            return Err(error);
        }
        let (frames, completed, encoded_bytes) =
            counts.ok_or(Error::Backend("storage batch callback was not executed"))?;
        Ok(BatchStats {
            frames,
            completed,
            encoded_bytes,
            durable_frontier: self.progress.frontier(),
        })
    }

    pub fn scan_complete(
        &self,
        after: TxSeq,
        mut visit: impl FnMut(&Record) -> Result<()>,
    ) -> Result<()> {
        self.progress.check_after(after)?;
        let frontier = self.progress.frontier();
        // A later bad descriptor must fail before any transaction reaches the caller.
        let mut validated = Progress::new(&self.cfg)?;
        self.backend
            .scan_records(&mut |bytes| validated.observe(&decode(bytes, self.cfg.limits)?))?;
        if validated.frontier() != frontier {
            return Err(Error::Gap);
        }
        let mut checked = Progress::new(&self.cfg)?;
        self.backend.scan_records(&mut |bytes| {
            let record = decode(bytes, self.cfg.limits)?;
            let sequence = match &record {
                Record::Frame(_) => {
                    TxSeq(checked.frontier().0.checked_add(1).ok_or(Error::Overflow)?)
                }
                Record::Completed(tx) => tx.order,
            };
            checked.observe(&record)?;
            if sequence > after && sequence <= frontier {
                visit(&record)?;
            }
            Ok(())
        })?;
        if checked.frontier() != frontier {
            return Err(Error::Gap);
        }
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        if self.progress.poisoned {
            return Err(Error::Poisoned);
        }
        let result = self.backend.checkpoint();
        if result.is_err() {
            self.progress.poisoned = true;
        }
        result
    }
}

fn decode(bytes: &[u8], limits: Limits) -> Result<Record> {
    let mut input = io::Cursor::new(bytes);
    let format::ReadRecord::Record(record) =
        format::read_record(&mut input, limits, format::TailPolicy::Required)?
    else {
        return Err(Error::Gap);
    };
    if input.position() != bytes.len() as u64 {
        return Err(Error::Format(format::Error::InvalidRecord));
    }
    Ok(record)
}
