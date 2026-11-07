//! Development storage experiments; runtime migration code does not import this module.
//! The closed backend set shares catalog transactions and bounded snapshot reads.
//! Role locks protect cooperating experiment processes; consumer progress is not target proof.

use super::format::{self, CompletedTx, Frame, Limits, Record, StreamId, TxSeq};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[path = "catalog.rs"]
mod catalog;
pub use catalog::{
    CATALOG_VERSION, CatalogHead, CatalogReadView, CompletionEntry, ReceiveSink, RecordNo, Role,
};

pub type Result<T> = std::result::Result<T, Error>;
pub type RecordSink<'a> = dyn FnMut(&[u8]) -> Result<()> + 'a;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Candidate {
    Segmented,
    Redb,
    Sqlite,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FaultPoint {
    BeforeAppend,
    AfterAppend,
    BeforeRotationSync,
    AfterRotationSync,
    BeforeFileSync,
    AfterFileSync,
    BeforeDirectorySync,
    AfterDirectorySync,
    BeforeCatalogCommit,
    AfterCatalogCommit,
    BeforeCatalogDirectorySync,
    AfterCatalogDirectorySync,
    BeforeReceiveReturn,
}

pub type FaultObserver<'a> = dyn Fn(FaultPoint) -> Result<()> + 'a;

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
    pub receive_revision: u64,
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
    Busy,
    Conflict,
    RoleViolation,
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
            Self::Busy => f.write_str("storage experiment role or transaction is busy"),
            Self::Conflict => f.write_str("storage experiment catalog revision differs"),
            Self::RoleViolation => f.write_str("storage experiment role forbids this operation"),
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
        for name in catalog::ROLE_FILES {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(self.dir.join(name))?
                .sync_all()?;
        }
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
        for name in catalog::ROLE_FILES {
            catalog::verify_role_file(&self.dir.join(name))?;
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
        identity[..8].copy_from_slice(b"RPGEXP02");
        identity[8..12].copy_from_slice(&2u32.to_le_bytes());
        identity[12..28].copy_from_slice(&self.stream.run.0);
        identity[28..36].copy_from_slice(&self.stream.source.system_identifier.to_le_bytes());
        identity[36..40].copy_from_slice(&self.stream.source.database_oid.to_le_bytes());
        identity[40..48].copy_from_slice(&self.stream.generation.to_le_bytes());
        identity
    }
}

pub struct BatchWriter<'a> {
    sink: &'a mut dyn ReceiveSink,
    progress: &'a mut Progress,
    observer: Option<&'a FaultObserver<'a>>,
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
    first_record: Option<RecordNo>,
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
            first_record: None,
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
        let number = self.append_record(&bytes)?;
        self.progress.first_record.get_or_insert(number);
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
        let descriptor_record = self.append_record(&bytes)?;
        self.sink.complete(CompletionEntry {
            descriptor: *tx,
            first_record: self.progress.first_record.unwrap_or(descriptor_record),
            descriptor_record,
        })?;
        self.progress.first_record = None;
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

    fn append_record(&mut self, bytes: &[u8]) -> Result<RecordNo> {
        let expected = self.sink.head().next_record;
        expected.next()?;
        if let Some(observer) = self.observer {
            observer(FaultPoint::BeforeAppend)?;
        }
        let number = self.sink.append(bytes)?;
        if let Some(observer) = self.observer {
            observer(FaultPoint::AfterAppend)?;
        }
        if number != expected {
            return Err(Error::Gap);
        }
        Ok(number)
    }
}

pub struct Store {
    backend: Backend,
    cfg: Config,
    progress: Progress,
    head: CatalogHead,
    role: Role,
    required_prefix: TxSeq,
    _role_guard: catalog::RoleGuard,
}

pub struct ReadView<'a> {
    raw: &'a dyn CatalogReadView,
    cfg: &'a Config,
}

impl ReadView<'_> {
    pub fn head(&self) -> &CatalogHead {
        self.raw.head()
    }

    pub fn scan_complete(
        &self,
        after: TxSeq,
        mut visit: impl FnMut(&Record) -> Result<()>,
    ) -> Result<()> {
        let (_, first, end) = self.validate(after)?;
        self.raw.scan_records(first, end, &mut |bytes| {
            visit(&decode(bytes, self.cfg.limits)?)
        })
    }

    fn entry(&self, order: TxSeq) -> Result<CompletionEntry> {
        let entry = self.raw.completion(order)?.ok_or(Error::Gap)?;
        entry.validate(self.cfg)?;
        if entry.descriptor.order != order || entry.descriptor_record >= self.head().next_record {
            return Err(Error::Gap);
        }
        Ok(entry)
    }

    fn validate(&self, after: TxSeq) -> Result<(Progress, RecordNo, RecordNo)> {
        self.head().validate(self.cfg)?;
        if after > self.head().published.map_or(TxSeq(0), |tx| tx.order) {
            return Err(Error::Gap);
        }
        let mut progress = Progress::new(self.cfg)?;
        let first = if after == TxSeq(0) {
            RecordNo::FIRST
        } else {
            let anchor = self.entry(after)?;
            let mut seen = false;
            self.raw.scan_records(
                anchor.descriptor_record,
                anchor.descriptor_record.next()?,
                &mut |bytes| {
                    if seen
                        || decode(bytes, self.cfg.limits)? != Record::Completed(anchor.descriptor)
                    {
                        return Err(Error::Gap);
                    }
                    seen = true;
                    Ok(())
                },
            )?;
            if !seen {
                return Err(Error::Gap);
            }
            progress.last = Some(anchor.descriptor);
            anchor.descriptor_record.next()?
        };
        let mut number = first;
        let mut published_end = first;
        // Both passes use this snapshot; a late corrupt record must precede all callbacks.
        self.raw
            .scan_records(first, self.head().next_record, &mut |bytes| {
                let record = decode(bytes, self.cfg.limits)?;
                if let Record::Completed(tx) = &record {
                    let entry = self.entry(tx.order)?;
                    if entry.descriptor != *tx
                        || entry.first_record != progress.first_record.unwrap_or(number)
                        || entry.descriptor_record != number
                    {
                        return Err(Error::Gap);
                    }
                    published_end = number.next()?;
                }
                progress.observe(&record)?;
                match record {
                    Record::Frame(_) => {
                        progress.first_record.get_or_insert(number);
                    }
                    Record::Completed(_) => progress.first_record = None,
                }
                number = number.next()?;
                Ok(())
            })?;
        if number != self.head().next_record || progress.last != self.head().published {
            return Err(Error::Gap);
        }
        Ok((progress, first, published_end))
    }
}

enum Backend {
    Segmented(super::journal::Store),
    Redb(super::redb::Store),
    Sqlite(super::sqlite::Store),
}

impl Backend {
    fn with_receive(
        &mut self,
        expected: u64,
        write: impl FnOnce(&mut dyn ReceiveSink) -> Result<()>,
        observer: Option<&FaultObserver<'_>>,
    ) -> Result<CatalogHead> {
        match self {
            Self::Segmented(store) => match observer {
                Some(observer) => store.with_receive_observed(expected, write, observer),
                None => store.with_receive(expected, write),
            },
            Self::Redb(store) => {
                if observer.is_some() {
                    return Err(Error::Config("C2 observers do not qualify redb"));
                }
                store.with_receive(expected, write)
            }
            Self::Sqlite(store) => match observer {
                Some(observer) => store.with_receive_observed(expected, write, observer),
                None => store.with_receive(expected, write),
            },
        }
    }
    fn with_read_view(&self, read: impl FnOnce(&dyn CatalogReadView) -> Result<()>) -> Result<()> {
        match self {
            Self::Segmented(store) => store.with_read_view(read),
            Self::Redb(store) => store.with_read_view(read),
            Self::Sqlite(store) => store.with_read_view(read),
        }
    }
    fn advance_consumer(&mut self, expected: u64, next: TxSeq) -> Result<CatalogHead> {
        match self {
            Self::Segmented(store) => store.advance_consumer(expected, next),
            Self::Redb(store) => store.advance_consumer(expected, next),
            Self::Sqlite(store) => store.advance_consumer(expected, next),
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
    pub fn open_role(
        candidate: Candidate,
        cfg: &Config,
        role: Role,
        required_prefix: TxSeq,
    ) -> Result<Self> {
        let role_guard = catalog::RoleGuard::acquire(cfg, role)?;
        let backend = match candidate {
            Candidate::Segmented => {
                Backend::Segmented(super::journal::Store::open_role(cfg, role)?)
            }
            Candidate::Redb => Backend::Redb(super::redb::Store::open_role(cfg, role)?),
            Candidate::Sqlite => Backend::Sqlite(super::sqlite::Store::open_role(cfg, role)?),
        };
        let mut recovered = None;
        backend.with_read_view(|raw| {
            let (progress, _, _) = (ReadView { raw, cfg }).validate(TxSeq(0))?;
            progress.require_prefix(required_prefix)?;
            recovered = Some((progress, *raw.head()));
            Ok(())
        })?;
        let (progress, head) =
            recovered.ok_or(Error::Backend("storage read callback was not executed"))?;
        Ok(Self {
            backend,
            cfg: cfg.clone(),
            progress,
            head,
            role,
            required_prefix,
            _role_guard: role_guard,
        })
    }

    pub fn with_receive_observed(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut BatchWriter<'_>) -> Result<()>,
        observer: &FaultObserver<'_>,
    ) -> Result<BatchStats> {
        if matches!(self.backend, Backend::Redb(_)) {
            return Err(Error::Config("C2 observers do not qualify redb"));
        }
        self.receive(expected_receive_revision, write, Some(observer))
    }

    pub fn with_receive(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut BatchWriter<'_>) -> Result<()>,
    ) -> Result<BatchStats> {
        self.receive(expected_receive_revision, write, None)
    }

    fn receive(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut BatchWriter<'_>) -> Result<()>,
        observer: Option<&FaultObserver<'_>>,
    ) -> Result<BatchStats> {
        self.require_role(Role::Receive)?;
        let mut counts = None;
        let mut entered = false;
        let result = self.backend.with_receive(
            expected_receive_revision,
            |sink| {
                entered = true;
                // Unwinding must leave this validator unusable, just as an uncertain commit does.
                self.progress.poisoned = true;
                if sink.head().receive_revision != self.head.receive_revision
                    || sink.head().next_record != self.head.next_record
                    || sink.head().published != self.progress.last
                {
                    return Err(Error::Gap);
                }
                let mut batch = BatchWriter {
                    sink,
                    progress: &mut self.progress,
                    observer,
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
            },
            observer,
        );
        let head = match result {
            Ok(head) => head,
            Err(error) => {
                if entered || !matches!(error, Error::Busy | Error::Conflict) {
                    self.progress.poisoned = true;
                }
                return Err(error);
            }
        };
        let (frames, completed, encoded_bytes) =
            counts.ok_or(Error::Backend("storage batch callback was not executed"))?;
        head.validate(&self.cfg)?;
        if head.receive_revision
            != expected_receive_revision
                .checked_add(1)
                .ok_or(Error::Overflow)?
            || head.published != self.progress.last
        {
            return Err(Error::Gap);
        }
        self.head = head;
        if let Some(observer) = observer {
            observer(FaultPoint::BeforeReceiveReturn)?;
        }
        self.progress.poisoned = false;
        Ok(BatchStats {
            frames,
            completed,
            encoded_bytes,
            durable_frontier: self.progress.frontier(),
            receive_revision: head.receive_revision,
        })
    }

    pub fn advance_consumer(
        &mut self,
        expected_apply_revision: u64,
        next: TxSeq,
    ) -> Result<CatalogHead> {
        self.require_role(Role::Apply)?;
        let head = self
            .backend
            .advance_consumer(expected_apply_revision, next)?;
        head.validate(&self.cfg)?;
        self.head = head;
        Ok(head)
    }

    pub fn with_read_view(&self, read: impl FnOnce(&ReadView<'_>) -> Result<()>) -> Result<()> {
        self.healthy()?;
        self.backend.with_read_view(|raw| {
            raw.head().validate(&self.cfg)?;
            if raw.head().published.map_or(TxSeq(0), |tx| tx.order) < self.required_prefix {
                return Err(Error::Gap);
            }
            read(&ReadView {
                raw,
                cfg: &self.cfg,
            })
        })
    }

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
        let role_guard = catalog::RoleGuard::acquire(cfg, Role::Receive)?;
        Ok(Self {
            backend,
            cfg: cfg.clone(),
            progress: Progress::new(cfg)?,
            head: CatalogHead::empty(),
            role: Role::Receive,
            required_prefix: TxSeq(0),
            _role_guard: role_guard,
        })
    }

    pub fn reopen(candidate: Candidate, cfg: &Config, required_prefix: TxSeq) -> Result<Self> {
        Self::open_role(candidate, cfg, Role::Receive, required_prefix)
    }

    pub fn with_batch(
        &mut self,
        write: impl FnOnce(&mut BatchWriter<'_>) -> Result<()>,
    ) -> Result<BatchStats> {
        self.with_receive(self.head.receive_revision, write)
    }

    pub fn scan_complete(
        &self,
        after: TxSeq,
        visit: impl FnMut(&Record) -> Result<()>,
    ) -> Result<()> {
        self.with_read_view(|view| view.scan_complete(after, visit))
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        self.require_role(Role::Receive)?;
        let result = self.backend.checkpoint();
        if result.is_err() {
            self.progress.poisoned = true;
        }
        result
    }

    fn healthy(&self) -> Result<()> {
        if self.progress.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn require_role(&self, role: Role) -> Result<()> {
        self.healthy()?;
        if self.role == role {
            Ok(())
        } else {
            Err(Error::RoleViolation)
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use format::{RunId, SourceEndLsn, SourceId, TxId};
    use std::cell::RefCell;

    struct Snapshot {
        head: CatalogHead,
        entries: Vec<CompletionEntry>,
        records: Vec<Vec<u8>>,
        ranges: RefCell<Vec<(u64, u64)>>,
    }

    impl CatalogReadView for Snapshot {
        fn head(&self) -> &CatalogHead {
            &self.head
        }

        fn completion(&self, order: TxSeq) -> Result<Option<CompletionEntry>> {
            Ok(self
                .entries
                .iter()
                .find(|entry| entry.descriptor.order == order)
                .copied())
        }

        fn scan_records(
            &self,
            first: RecordNo,
            end: RecordNo,
            visit: &mut RecordSink<'_>,
        ) -> Result<()> {
            self.ranges.borrow_mut().push((first.get(), end.get()));
            if first > end || end > self.head.next_record {
                return Err(Error::Gap);
            }
            for number in first.get()..end.get() {
                visit(self.records.get(number as usize - 1).ok_or(Error::Gap)?)?;
            }
            Ok(())
        }
    }

    fn fixture() -> (Config, Snapshot) {
        let cfg = Config {
            dir: "unused-common-read-test".into(),
            stream: StreamId {
                run: RunId([11; 16]),
                source: SourceId {
                    system_identifier: 12,
                    database_oid: 13,
                },
                generation: 0,
            },
            limits: Limits::default(),
            batch_bytes: 2 * 1024 * 1024,
            cache_bytes: 4096,
            segment_bytes: 2 * 1024 * 1024,
        };
        let mut snapshot = Snapshot {
            head: CatalogHead::empty(),
            entries: Vec::new(),
            records: Vec::new(),
            ranges: RefCell::new(Vec::new()),
        };
        for order in 1..=3 {
            let tx = TxId {
                stream: cfg.stream,
                xid: 7,
                begin_ordinal: 40 - order,
            };
            let frame = Frame {
                tx,
                event_ordinal: 0,
                part: 0,
                last_part: true,
                payload: vec![order as u8],
            };
            let descriptor = CompletedTx {
                tx,
                order: TxSeq(order),
                commit: None,
                end: SourceEndLsn(20),
                event_count: 1,
                encoded_bytes: 217,
            };
            snapshot
                .records
                .push(format::encode_frame(&frame, cfg.limits).unwrap());
            snapshot.records.push(
                format::encode_completed(&descriptor, cfg.limits)
                    .unwrap()
                    .to_vec(),
            );
            snapshot.entries.push(CompletionEntry {
                descriptor,
                first_record: RecordNo::new(order * 2 - 1).unwrap(),
                descriptor_record: RecordNo::new(order * 2).unwrap(),
            });
            snapshot.head.published = Some(descriptor);
        }
        snapshot.head.next_record = RecordNo::new(7).unwrap();
        snapshot.head.receive_revision = 1;
        (cfg, snapshot)
    }

    #[test]
    fn indexed_scan_checks_one_anchor_then_only_the_requested_suffix() {
        let (cfg, snapshot) = fixture();
        let view = ReadView {
            raw: &snapshot,
            cfg: &cfg,
        };
        let mut seen = Vec::new();
        view.scan_complete(TxSeq(2), |record| {
            seen.push(record.clone());
            Ok(())
        })
        .unwrap();
        assert_eq!(seen.len(), 2);
        assert!(matches!(&seen[0], Record::Frame(frame) if frame.payload == [3]));
        assert!(matches!(&seen[1], Record::Completed(tx) if tx.order == TxSeq(3)));
        assert_eq!(*snapshot.ranges.borrow(), [(4, 5), (5, 7), (5, 7)]);
    }

    #[test]
    fn index_mismatch_and_late_crc_failure_precede_every_user_callback() {
        let (cfg, mut snapshot) = fixture();
        ReadView {
            raw: &snapshot,
            cfg: &cfg,
        }
        .scan_complete(TxSeq(0), |_| Ok(()))
        .unwrap();
        snapshot.records[4][88] ^= 1;
        let mut seen = 0;
        assert!(
            ReadView {
                raw: &snapshot,
                cfg: &cfg
            }
            .scan_complete(TxSeq(0), |_| {
                seen += 1;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(seen, 0);
        snapshot.records[4][88] ^= 1;
        snapshot.entries[2].first_record = RecordNo::new(4).unwrap();
        assert_eq!(
            ReadView {
                raw: &snapshot,
                cfg: &cfg
            }
            .scan_complete(TxSeq(0), |_| {
                seen += 1;
                Ok(())
            }),
            Err(Error::Gap)
        );
        assert_eq!(seen, 0);
    }

    #[test]
    fn predecessor_must_match_actual_descriptor_and_missing_index_is_a_gap() {
        let (cfg, mut snapshot) = fixture();
        ReadView {
            raw: &snapshot,
            cfg: &cfg,
        }
        .scan_complete(TxSeq(2), |_| Ok(()))
        .unwrap();
        snapshot.entries[1].descriptor.end = SourceEndLsn(19);
        assert_eq!(
            ReadView {
                raw: &snapshot,
                cfg: &cfg
            }
            .scan_complete(TxSeq(2), |_| panic!("bad anchor emitted")),
            Err(Error::Gap)
        );
        snapshot.entries.remove(1);
        assert_eq!(
            ReadView {
                raw: &snapshot,
                cfg: &cfg
            }
            .scan_complete(TxSeq(2), |_| panic!("missing anchor emitted")),
            Err(Error::Gap)
        );
    }

    #[test]
    fn readonly_callback_abort_keeps_the_same_view_reusable() {
        let (cfg, snapshot) = fixture();
        let view = ReadView {
            raw: &snapshot,
            cfg: &cfg,
        };
        assert_eq!(
            view.scan_complete(TxSeq(2), |_| Err(Error::Workload("visitor stopped"))),
            Err(Error::Workload("visitor stopped"))
        );
        let mut seen = 0;
        view.scan_complete(TxSeq(2), |_| {
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 2);
    }
}
