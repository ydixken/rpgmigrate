//! Segmented experiment data uses SQLite's raw store as its publication catalog.
//! The cache budget applies to SQLite metadata; raw reads retain one bounded record.
//! Unpublished bytes stay on disk because recovery has no authority to truncate them.
//! Read views keep one SQLite snapshot and its fixed published data boundary.

use super::config::{
    CatalogHead, CatalogReadView, CompletionEntry, Config, Error, FaultObserver, FaultPoint,
    ReceiveSink, RecordNo, RecordSink, Result, Role,
};
use super::format::TxSeq;
use super::sqlite;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

const LOCATOR_BYTES: usize = 40;
const LOCATOR_MAGIC: &[u8; 8] = b"RPGLOC01";

pub(super) struct Store {
    cfg: Config,
    catalog: sqlite::Store,
    role: Role,
    directory: File,
    active: Option<Segment>,
    next_segment: u64,
    poisoned: bool,
}

struct Segment {
    file: File,
    number: u64,
    length: u64,
}

impl Store {
    /// Post-commit observer failures require reopening because publication may have succeeded.
    pub(super) fn with_receive_observed(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut dyn ReceiveSink) -> Result<()>,
        observer: &FaultObserver<'_>,
    ) -> Result<CatalogHead> {
        self.require_role(Role::Receive)?;
        let result = self.catalog.with_receive_observed(
            expected_receive_revision,
            |catalog| {
                // Busy and stale CAS refusals happen before this callback and leave us usable.
                self.poisoned = true;
                let mut sink = ReceiveBatch {
                    cfg: &self.cfg,
                    active: &mut self.active,
                    next_segment: &mut self.next_segment,
                    catalog,
                    observer,
                    written: 0,
                    failure: None,
                };
                let callback = write(&mut sink);
                if let Some(error) = sink.failure {
                    return Err(error);
                }
                callback?;
                // Catalog COMMIT is allowed only after every referenced byte and name is durable.
                if let Some(segment) = &self.active {
                    observer(FaultPoint::BeforeFileSync)?;
                    segment.file.sync_all()?;
                    observer(FaultPoint::AfterFileSync)?;
                }
                observer(FaultPoint::BeforeDirectorySync)?;
                self.directory.sync_all()?;
                observer(FaultPoint::AfterDirectorySync)?;
                Ok(())
            },
            observer,
        );
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }

    pub(super) fn with_receive(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut dyn ReceiveSink) -> Result<()>,
    ) -> Result<CatalogHead> {
        self.with_receive_observed(expected_receive_revision, write, &|_| Ok(()))
    }

    pub(super) fn with_read_view(
        &self,
        read: impl FnOnce(&dyn CatalogReadView) -> Result<()>,
    ) -> Result<()> {
        self.check_healthy()?;
        self.catalog.with_read_view(|catalog| {
            let end = catalog.head().next_record;
            let mut cut = None;
            if end != RecordNo::FIRST {
                catalog.scan_records(RecordNo::new(end.get() - 1)?, end, &mut |raw| {
                    let locator = Locator::decode(raw, &self.cfg)?;
                    if cut.is_some() {
                        return Err(Error::Gap);
                    }
                    cut = Some((locator.segment, locator.end()?));
                    Ok(())
                })?;
                if cut.is_none() {
                    return Err(Error::Gap);
                }
            }
            read(&ReadView {
                cfg: &self.cfg,
                catalog,
                cut,
            })
        })
    }

    pub(super) fn advance_consumer(
        &mut self,
        expected_apply_revision: u64,
        next: TxSeq,
    ) -> Result<CatalogHead> {
        self.require_role(Role::Apply)?;
        self.catalog.advance_consumer(expected_apply_revision, next)
    }

    pub(super) fn settings(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "configured_segment_bytes": self.cfg.segment_bytes,
            "configured_sync_policy": {
                "segment": "File::sync_all",
                "directory": "File::sync_all",
                "publication": "segment_and_directory_sync_before_catalog_commit"
            },
            "catalog": self.catalog.settings()?
        }))
    }

    pub(super) fn create(cfg: &Config) -> Result<Self> {
        let catalog_cfg = catalog_config(cfg)?;
        cfg.create_directory()?;
        let catalog = sqlite::Store::create(&catalog_cfg)?;
        let directory = File::open(&cfg.dir)?;
        directory.sync_all()?;
        Ok(Self {
            cfg: cfg.clone(),
            catalog,
            role: Role::Receive,
            directory,
            active: None,
            next_segment: 1,
            poisoned: false,
        })
    }

    #[cfg(test)]
    pub(super) fn reopen(cfg: &Config) -> Result<Self> {
        Self::open_role(cfg, Role::Receive)
    }

    pub(super) fn open_role(cfg: &Config, role: Role) -> Result<Self> {
        cfg.verify_directory()?;
        let catalog = sqlite::Store::open_role(&catalog_config(cfg)?, role)?;
        let mut largest = 0;
        for entry in fs::read_dir(&cfg.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name == "identity"
                || name == "catalog"
                || name == "receive.lock"
                || name == "apply.lock"
            {
                continue;
            }
            let number = name
                .to_str()
                .and_then(|name| name.strip_prefix("segment-"))
                .and_then(|name| name.strip_suffix(".data"))
                .filter(|number| number.len() == 16)
                .and_then(|number| u64::from_str_radix(number, 16).ok())
                .filter(|number| *number != 0)
                .ok_or(Error::Backend("unexpected journal directory entry"))?;
            if entry.path() != segment_path(&cfg.dir, number) {
                return Err(Error::Backend("invalid journal segment name"));
            }
            check_segment_metadata(&fs::symlink_metadata(entry.path())?, cfg.segment_bytes)?;
            largest = largest.max(number);
        }
        let store = Self {
            cfg: cfg.clone(),
            catalog,
            role,
            directory: File::open(&cfg.dir)?,
            active: None,
            next_segment: largest.checked_add(1).ok_or(Error::Overflow)?,
            poisoned: false,
        };
        store.scan_records(&mut |_| Ok(()))?;
        Ok(store)
    }

    #[cfg(test)]
    pub(super) fn with_batch(
        &mut self,
        write: impl FnOnce(&mut RecordSink<'_>) -> Result<()>,
    ) -> Result<()> {
        self.require_role(Role::Receive)?;
        let mut revision = None;
        self.catalog.with_read_view(|view| {
            revision = Some(view.head().receive_revision);
            Ok(())
        })?;
        self.with_receive(revision.ok_or(Error::Gap)?, |sink| {
            write(&mut |record| sink.append(record).map(|_| ()))
        })
        .map(|_| ())
    }

    pub(super) fn scan_records(&self, visit: &mut RecordSink<'_>) -> Result<()> {
        self.with_read_view(|view| {
            view.scan_records(RecordNo::FIRST, view.head().next_record, visit)
        })
    }

    pub(super) fn checkpoint(&mut self) -> Result<()> {
        self.check_healthy()?;
        if self.role == Role::Status {
            return Err(Error::RoleViolation);
        }
        self.poisoned = true;
        if let Some(segment) = &self.active {
            segment.file.sync_all()?;
        }
        self.directory.sync_all()?;
        self.catalog.checkpoint()?;
        self.poisoned = false;
        Ok(())
    }

    fn require_role(&self, role: Role) -> Result<()> {
        self.check_healthy()?;
        if self.role != role {
            return Err(Error::RoleViolation);
        }
        Ok(())
    }

    fn check_healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
}

struct ReceiveBatch<'a> {
    cfg: &'a Config,
    active: &'a mut Option<Segment>,
    next_segment: &'a mut u64,
    catalog: &'a mut dyn ReceiveSink,
    observer: &'a FaultObserver<'a>,
    written: u64,
    failure: Option<Error>,
}

impl ReceiveSink for ReceiveBatch<'_> {
    fn head(&self) -> &CatalogHead {
        self.catalog.head()
    }

    fn append(&mut self, record: &[u8]) -> Result<RecordNo> {
        if self.failure.is_some() {
            return Err(Error::Poisoned);
        }
        // A callback can catch an append panic and still try to publish the batch.
        self.failure = Some(Error::Poisoned);
        let result = append_record(
            self.cfg,
            self.active,
            self.next_segment,
            &mut self.written,
            record,
            self.catalog,
            self.observer,
        );
        self.failure = result.as_ref().err().cloned();
        result
    }

    fn complete(&mut self, entry: CompletionEntry) -> Result<()> {
        if self.failure.is_some() {
            return Err(Error::Poisoned);
        }
        let result = self.catalog.complete(entry);
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
}

struct ReadView<'a> {
    cfg: &'a Config,
    catalog: &'a dyn CatalogReadView,
    // The last published locator fixes the data boundary without retaining an extent list.
    cut: Option<(u64, u64)>,
}

impl CatalogReadView for ReadView<'_> {
    fn head(&self) -> &CatalogHead {
        self.catalog.head()
    }

    fn completion(&self, order: TxSeq) -> Result<Option<CompletionEntry>> {
        self.catalog.completion(order)
    }

    fn scan_records(
        &self,
        first: RecordNo,
        end: RecordNo,
        visit: &mut RecordSink<'_>,
    ) -> Result<()> {
        let mut segment: Option<Segment> = None;
        let mut previous_end = 0;
        self.catalog.scan_records(first, end, &mut |raw| {
            let locator = Locator::decode(raw, self.cfg)?;
            let end = locator.end()?;
            if self.cut.is_none_or(|cut| (locator.segment, end) > cut) {
                return Err(Error::Gap);
            }
            match &segment {
                Some(open)
                    if locator.segment < open.number
                        || (locator.segment == open.number && locator.offset < previous_end) =>
                {
                    return Err(Error::Backend("journal locators overlap or go backwards"));
                }
                Some(open) if locator.segment == open.number => {}
                _ => {
                    let path = segment_path(&self.cfg.dir, locator.segment);
                    let before = fs::symlink_metadata(&path).map_err(required_io)?;
                    check_segment_metadata(&before, self.cfg.segment_bytes)?;
                    let file = OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_NOFOLLOW)
                        .open(path)
                        .map_err(required_io)?;
                    let opened = file.metadata()?;
                    if opened.dev() != before.dev() || opened.ino() != before.ino() {
                        return Err(Error::Backend("journal segment changed while opening"));
                    }
                    segment = Some(Segment {
                        file,
                        number: locator.segment,
                        length: opened.len(),
                    });
                }
            }
            let segment = segment.as_mut().expect("locator opened a segment");
            if end > segment.length {
                return Err(Error::Gap);
            }
            let length = usize::try_from(locator.length).map_err(|_| Error::Overflow)?;
            let mut record = Vec::new();
            record
                .try_reserve_exact(length)
                .map_err(|_| Error::Backend("journal record allocation failed"))?;
            record.resize(length, 0);
            segment.file.seek(SeekFrom::Start(locator.offset))?;
            segment.file.read_exact(&mut record).map_err(required_io)?;
            if crc32fast::hash(&record) != locator.checksum {
                return Err(Error::Backend("journal record checksum differs"));
            }
            previous_end = end;
            visit(&record)
        })
    }
}

fn catalog_config(cfg: &Config) -> Result<Config> {
    cfg.validate()?;
    let mut catalog = cfg.clone();
    catalog.dir = cfg.dir.join("catalog");
    catalog.limits.max_frame_bytes = super::format::COMPLETED_BYTES;
    // A one-byte opaque record still needs a full locator in the catalog.
    catalog.batch_bytes = cfg
        .batch_bytes
        .checked_mul(LOCATOR_BYTES as u64)
        .ok_or(Error::Overflow)?;
    Ok(catalog)
}

fn segment_path(dir: &Path, number: u64) -> PathBuf {
    dir.join(format!("segment-{number:016x}.data"))
}

fn check_segment_metadata(metadata: &fs::Metadata, limit: u64) -> Result<()> {
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.len() > limit
    {
        return Err(Error::Backend("invalid journal segment file"));
    }
    Ok(())
}

fn required_io(error: io::Error) -> Error {
    match error.kind() {
        io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof => Error::Gap,
        _ => error.into(),
    }
}

fn append_record(
    cfg: &Config,
    active: &mut Option<Segment>,
    next_segment: &mut u64,
    written: &mut u64,
    record: &[u8],
    publish: &mut dyn ReceiveSink,
    observer: &FaultObserver<'_>,
) -> Result<RecordNo> {
    publish.head().next_record.next()?;
    if record.is_empty() || record.len() > cfg.limits.max_frame_bytes {
        return Err(Error::Backend("journal record exceeds its byte limit"));
    }
    let length = u64::try_from(record.len()).map_err(|_| Error::Overflow)?;
    let next_written = written.checked_add(length).ok_or(Error::Overflow)?;
    if next_written > cfg.batch_bytes {
        return Err(Error::Backend("journal batch exceeds its byte limit"));
    }
    if let Some(segment) = active
        && segment.length.checked_add(length).ok_or(Error::Overflow)? > cfg.segment_bytes
    {
        // Sync before dropping the handle so a batch retains no list of touched files.
        observer(FaultPoint::BeforeRotationSync)?;
        segment.file.sync_all()?;
        observer(FaultPoint::AfterRotationSync)?;
        *active = None;
    }
    if active.is_none() {
        let number = *next_segment;
        let following = number.checked_add(1).ok_or(Error::Overflow)?;
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(segment_path(&cfg.dir, number))?;
        *active = Some(Segment {
            file,
            number,
            length: 0,
        });
        *next_segment = following;
    }
    let segment = active.as_mut().expect("append opened a segment");
    let end = segment.length.checked_add(length).ok_or(Error::Overflow)?;
    let locator = Locator {
        segment: segment.number,
        offset: segment.length,
        length,
        checksum: crc32fast::hash(record),
    };
    segment.file.write_all(record)?;
    segment.length = end;
    let ordinal = publish.append(&locator.encode())?;
    *written = next_written;
    Ok(ordinal)
}

struct Locator {
    segment: u64,
    offset: u64,
    length: u64,
    checksum: u32,
}

impl Locator {
    fn end(&self) -> Result<u64> {
        self.offset.checked_add(self.length).ok_or(Error::Overflow)
    }

    fn encode(&self) -> [u8; LOCATOR_BYTES] {
        let mut raw = [0; LOCATOR_BYTES];
        raw[..8].copy_from_slice(LOCATOR_MAGIC);
        raw[8..16].copy_from_slice(&self.segment.to_le_bytes());
        raw[16..24].copy_from_slice(&self.offset.to_le_bytes());
        raw[24..32].copy_from_slice(&self.length.to_le_bytes());
        raw[32..36].copy_from_slice(&self.checksum.to_le_bytes());
        let checksum = crc32fast::hash(&raw[..36]);
        raw[36..40].copy_from_slice(&checksum.to_le_bytes());
        raw
    }

    fn decode(raw: &[u8], cfg: &Config) -> Result<Self> {
        if raw.len() != LOCATOR_BYTES || &raw[..8] != LOCATOR_MAGIC {
            return Err(Error::Backend("invalid journal locator"));
        }
        if crc32fast::hash(&raw[..36]) != u32::from_le_bytes(raw[36..40].try_into().unwrap()) {
            return Err(Error::Backend("journal locator checksum differs"));
        }
        let locator = Self {
            segment: u64::from_le_bytes(raw[8..16].try_into().unwrap()),
            offset: u64::from_le_bytes(raw[16..24].try_into().unwrap()),
            length: u64::from_le_bytes(raw[24..32].try_into().unwrap()),
            checksum: u32::from_le_bytes(raw[32..36].try_into().unwrap()),
        };
        if locator.segment == 0
            || locator.length == 0
            || locator.length
                > u64::try_from(cfg.limits.max_frame_bytes).map_err(|_| Error::Overflow)?
            || locator
                .offset
                .checked_add(locator.length)
                .ok_or(Error::Overflow)?
                > cfg.segment_bytes
        {
            return Err(Error::Backend("journal locator exceeds segment bounds"));
        }
        Ok(locator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Limits, RunId, SourceId, StreamId};
    use std::cell::RefCell;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "rpgmigrate-journal-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).expect("create exclusive fixture");
            Self(path)
        }

        fn config(&self) -> Config {
            Config {
                dir: self.0.join("store"),
                stream: StreamId {
                    run: RunId([0x43; 16]),
                    source: SourceId {
                        system_identifier: 0x8912,
                        database_oid: 57,
                    },
                    generation: 1,
                },
                limits: Limits {
                    max_frame_bytes: 128,
                    max_event_bytes: 1024,
                    max_parts: 8,
                },
                batch_bytes: 512,
                cache_bytes: 1024 * 1024,
                segment_bytes: 256,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).expect("remove owned journal fixture");
        }
    }

    fn records(store: &Store) -> Result<Vec<Vec<u8>>> {
        let mut found = Vec::new();
        store.scan_records(&mut |bytes| {
            found.push(bytes.to_vec());
            Ok(())
        })?;
        Ok(found)
    }

    fn c1_records(
        cfg: &Config,
        sequence: u64,
    ) -> (Vec<u8>, Vec<u8>, super::super::format::CompletedTx) {
        use super::super::format::{self, CompletedTx, Frame, SourceEndLsn, TxId};
        let frame = Frame {
            tx: TxId {
                stream: cfg.stream,
                xid: sequence as u32,
                begin_ordinal: sequence,
            },
            event_ordinal: 0,
            part: 0,
            last_part: true,
            payload: vec![sequence as u8],
        };
        let descriptor = CompletedTx {
            tx: frame.tx,
            order: TxSeq(sequence),
            commit: None,
            end: SourceEndLsn(sequence),
            event_count: 1,
            encoded_bytes: 217,
        };
        (
            format::encode_frame(&frame, cfg.limits).unwrap(),
            format::encode_completed(&descriptor, cfg.limits).unwrap(),
            descriptor,
        )
    }

    fn c1_append(sink: &mut dyn ReceiveSink, cfg: &Config, sequence: u64) -> Result<()> {
        let (frame, completed, descriptor) = c1_records(cfg, sequence);
        let first_record = sink.append(&frame)?;
        let descriptor_record = sink.append(&completed)?;
        sink.complete(CompletionEntry {
            descriptor,
            first_record,
            descriptor_record,
        })
    }

    fn c1_head(store: &Store) -> CatalogHead {
        let mut head = None;
        store
            .with_read_view(|view| {
                head = Some(*view.head());
                Ok(())
            })
            .unwrap();
        head.expect("read view callback must execute")
    }

    fn c2_baseline(segment_bytes: u64) -> (Fixture, Config, Store) {
        let fixture = Fixture::new();
        let mut cfg = fixture.config();
        cfg.segment_bytes = segment_bytes;
        let mut store = Store::create(&cfg).unwrap();
        store
            .with_receive(0, |sink| {
                sink.append(&[11; 128])?;
                sink.append(&[22; 128])?;
                Ok(())
            })
            .unwrap();
        (fixture, cfg, store)
    }

    const RAW_SYNC_POINTS: [FaultPoint; 6] = [
        FaultPoint::BeforeRotationSync,
        FaultPoint::AfterRotationSync,
        FaultPoint::BeforeFileSync,
        FaultPoint::AfterFileSync,
        FaultPoint::BeforeDirectorySync,
        FaultPoint::AfterDirectorySync,
    ];

    #[test]
    fn c2_observer_orders_raw_sync_before_nested_catalog_publication() {
        use FaultPoint::*;
        let (_fixture, _cfg, mut store) = c2_baseline(256);
        let trace = RefCell::new(Vec::new());
        let head = store
            .with_receive_observed(
                1,
                |sink| {
                    assert_eq!(sink.append(&[33; 128])?.get(), 3);
                    assert_eq!(sink.append(&[44; 128])?.get(), 4);
                    Ok(())
                },
                &|point| {
                    trace.borrow_mut().push(point);
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(
            *trace.borrow(),
            [
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
            ]
        );
        assert_eq!((head.receive_revision, head.next_record.get()), (2, 5));
        assert_eq!(
            records(&store).unwrap(),
            [vec![11; 128], vec![22; 128], vec![33; 128], vec![44; 128]]
        );
    }

    #[test]
    fn c2_failed_sync_never_emits_its_after_point_or_publishes() {
        use FaultPoint::*;
        for boundary in ["rotation", "file", "directory"] {
            let (_fixture, cfg, mut store) =
                c2_baseline(if boundary == "rotation" { 256 } else { 1024 });
            let (file, _peer) = unsyncable_file();
            if boundary == "directory" {
                store.directory = file;
            } else {
                store.active.as_mut().unwrap().file = file;
            }
            let trace = RefCell::new(Vec::new());
            let result = store.with_receive_observed(
                1,
                |sink| {
                    sink.append(&[33; 128])?;
                    Ok(())
                },
                &|point| {
                    trace.borrow_mut().push(point);
                    Ok(())
                },
            );
            assert!(
                matches!(result, Err(Error::Io(_))),
                "{boundary}: {result:?}"
            );
            let expected = match boundary {
                "rotation" => vec![BeforeRotationSync],
                "file" => vec![BeforeFileSync],
                "directory" => vec![BeforeFileSync, AfterFileSync, BeforeDirectorySync],
                _ => unreachable!(),
            };
            assert_eq!(*trace.borrow(), expected, "{boundary}");
            assert_eq!(store.with_receive(1, |_| Ok(())), Err(Error::Poisoned));
            drop(store);
            assert_eq!(
                records(&Store::reopen(&cfg).unwrap()).unwrap(),
                [vec![11; 128], vec![22; 128]]
            );
        }
    }

    #[test]
    fn c2_raw_observer_error_is_sticky_when_append_error_is_swallowed() {
        for target in RAW_SYNC_POINTS {
            let (_fixture, cfg, mut store) = c2_baseline(256);
            let failure = Error::Workload("injected raw sync observer error");
            let trace = RefCell::new(Vec::new());
            let result = store.with_receive_observed(
                1,
                |sink| {
                    if sink.append(&[33; 128]).is_ok() {
                        let _ = sink.append(&[44; 128]);
                    }
                    Ok(())
                },
                &|point| {
                    trace.borrow_mut().push(point);
                    if point == target {
                        Err(failure.clone())
                    } else {
                        Ok(())
                    }
                },
            );
            assert_eq!(result, Err(failure), "{target:?}");
            assert_eq!(trace.borrow().last(), Some(&target));
            assert_eq!(store.with_receive(1, |_| Ok(())), Err(Error::Poisoned));
            drop(store);
            let reopened = Store::reopen(&cfg).unwrap();
            assert_eq!(
                (
                    c1_head(&reopened).receive_revision,
                    c1_head(&reopened).next_record.get()
                ),
                (1, 3)
            );
            assert_eq!(records(&reopened).unwrap(), [vec![11; 128], vec![22; 128]]);
        }
    }

    #[test]
    fn c2_raw_observer_panic_poisons_receiver_and_aborts_publication() {
        for target in RAW_SYNC_POINTS {
            let (_fixture, cfg, mut store) = c2_baseline(256);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store.with_receive_observed(
                    1,
                    |sink| {
                        sink.append(&[33; 128])?;
                        sink.append(&[44; 128])?;
                        Ok(())
                    },
                    &|point| {
                        assert_ne!(point, target, "injected raw sync observer panic");
                        Ok(())
                    },
                )
            }));
            assert!(result.is_err(), "{target:?}");
            assert_eq!(store.with_receive(1, |_| Ok(())), Err(Error::Poisoned));
            drop(store);
            assert_eq!(
                records(&Store::reopen(&cfg).unwrap()).unwrap(),
                [vec![11; 128], vec![22; 128]]
            );
        }
    }

    #[test]
    fn c2_caught_rotation_observer_panic_cannot_publish_earlier_appends() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        let baseline = store
            .with_receive(0, |sink| {
                sink.append(&[11; 128])?;
                Ok(())
            })
            .unwrap();
        let trace = RefCell::new(Vec::new());
        let result = store.with_receive_observed(
            1,
            |sink| {
                sink.append(&[22; 128])?;
                let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    sink.append(&[33; 128])
                }));
                assert!(caught.is_err());
                assert_eq!(trace.borrow().last(), Some(&FaultPoint::AfterRotationSync));
                Ok(())
            },
            &|point| {
                trace.borrow_mut().push(point);
                assert_ne!(
                    point,
                    FaultPoint::AfterRotationSync,
                    "injected rotation panic"
                );
                Ok(())
            },
        );
        assert_eq!(result, Err(Error::Poisoned));
        assert_eq!(
            store.with_receive(1, |_| panic!("poisoned receiver ran callback")),
            Err(Error::Poisoned)
        );
        let path = segment_path(&cfg.dir, 1);
        let raw = fs::read(&path).unwrap();
        assert_eq!(raw.len(), 256);
        assert_eq!(&raw[..128], &[11; 128]);
        assert_eq!(&raw[128..], &[22; 128]);
        drop(store);
        let reopened = Store::reopen(&cfg).unwrap();
        assert_eq!(c1_head(&reopened), baseline);
        assert_eq!(records(&reopened).unwrap(), [vec![11; 128]]);
        assert_eq!(fs::read(path).unwrap(), raw);
    }

    #[test]
    fn c2_post_commit_observer_error_requires_reopen_with_published_records_intact() {
        use FaultPoint::*;
        for target in [
            AfterCatalogCommit,
            BeforeCatalogDirectorySync,
            AfterCatalogDirectorySync,
        ] {
            let (_fixture, cfg, mut store) = c2_baseline(256);
            let failure = Error::Workload("injected post-commit observer error");
            let result = store.with_receive_observed(
                1,
                |sink| {
                    sink.append(&[33; 128])?;
                    sink.append(&[44; 128])?;
                    Ok(())
                },
                &|point| {
                    if point == target {
                        Err(failure.clone())
                    } else {
                        Ok(())
                    }
                },
            );
            assert_eq!(result, Err(failure), "{target:?}");
            assert_eq!(store.with_receive(1, |_| Ok(())), Err(Error::Poisoned));
            drop(store);
            let reopened = Store::reopen(&cfg).unwrap();
            assert_eq!(
                (
                    c1_head(&reopened).receive_revision,
                    c1_head(&reopened).next_record.get()
                ),
                (2, 5)
            );
            assert_eq!(
                records(&reopened).unwrap(),
                [vec![11; 128], vec![22; 128], vec![33; 128], vec![44; 128]]
            );
        }
    }

    #[test]
    fn c1_receive_ordinals_completion_and_cursor_survive_reopen() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let (frame, complete, descriptor) = c1_records(&cfg, 1);
        let mut store = Store::create(&cfg).unwrap();
        let head = store
            .with_receive(0, |sink| {
                assert_eq!(sink.append(&frame)?, RecordNo::FIRST);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            (
                head.receive_revision,
                head.next_record.get(),
                head.published
            ),
            (1, 2, None)
        );
        drop(store);
        let mut store = Store::reopen(&cfg).unwrap();
        store
            .with_receive(1, |sink| {
                let descriptor_record = sink.append(&complete)?;
                assert_eq!(descriptor_record.get(), 2);
                sink.complete(CompletionEntry {
                    descriptor,
                    first_record: RecordNo::FIRST,
                    descriptor_record,
                })
            })
            .unwrap();
        let mut apply = Store::open_role(&cfg, Role::Apply).unwrap();
        let applied = apply.advance_consumer(0, TxSeq(1)).unwrap();
        assert_eq!(
            (
                applied.receive_revision,
                applied.apply_revision,
                applied.consumer_cursor
            ),
            (2, 1, TxSeq(1))
        );
        let head = store
            .with_receive(2, |sink| c1_append(sink, &cfg, 2))
            .unwrap();
        assert_eq!(
            (
                head.receive_revision,
                head.apply_revision,
                head.consumer_cursor,
                head.next_record.get()
            ),
            (3, 1, TxSeq(1), 5)
        );
        drop(store);
        let reopened = Store::reopen(&cfg).unwrap();
        assert_eq!(c1_head(&reopened), head);
        reopened
            .with_read_view(|view| {
                let first = view.completion(TxSeq(1))?.expect("first completion");
                assert_eq!(
                    (first.first_record.get(), first.descriptor_record.get()),
                    (1, 2)
                );
                let second = view.completion(TxSeq(2))?.expect("second completion");
                assert_eq!(
                    (second.first_record.get(), second.descriptor_record.get()),
                    (3, 4)
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn c1_snapshot_keeps_two_passes_and_index_stable_during_receive() {
        for segment_bytes in [256, 1024] {
            let fixture = Fixture::new();
            let mut cfg = fixture.config();
            cfg.segment_bytes = segment_bytes;
            let mut writer = Store::create(&cfg).unwrap();
            writer
                .with_receive(0, |sink| c1_append(sink, &cfg, 1))
                .unwrap();
            let status = Store::open_role(&cfg, Role::Status).unwrap();
            status
                .with_read_view(|view| {
                    let captured = *view.head();
                    let mut before = Vec::new();
                    view.scan_records(RecordNo::FIRST, captured.next_record, &mut |bytes| {
                        before.push(bytes.to_vec());
                        Ok(())
                    })?;
                    assert_eq!(before.len(), 2);
                    let newer = writer.with_receive(1, |sink| c1_append(sink, &cfg, 2))?;
                    assert_eq!(view.head(), &captured);
                    assert!(view.completion(TxSeq(2))?.is_none());
                    let mut after = Vec::new();
                    view.scan_records(RecordNo::FIRST, captured.next_record, &mut |bytes| {
                        after.push(bytes.to_vec());
                        Ok(())
                    })?;
                    assert_eq!(after, before);
                    let mut visited = false;
                    assert_eq!(
                        view.scan_records(RecordNo::FIRST, newer.next_record, &mut |_| {
                            visited = true;
                            Ok(())
                        }),
                        Err(Error::Gap)
                    );
                    assert!(!visited);
                    Ok(())
                })
                .unwrap();
            assert_eq!(c1_head(&status).next_record.get(), 5);
        }
    }

    #[test]
    fn c1_conflict_and_busy_before_callback_leave_writer_usable() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut writer = Store::create(&cfg).unwrap();
        writer
            .with_receive(0, |sink| c1_append(sink, &cfg, 1))
            .unwrap();
        assert_eq!(
            writer.with_receive(0, |_| panic!("stale CAS ran callback")),
            Err(Error::Conflict)
        );
        let mut peer =
            sqlite::Store::open_role(&catalog_config(&cfg).unwrap(), Role::Receive).unwrap();
        let stop = Error::Workload("release peer write transaction");
        assert_eq!(
            peer.with_receive(1, |_| {
                assert_eq!(
                    writer.with_receive(1, |_| panic!("busy receive ran callback")),
                    Err(Error::Busy)
                );
                Err(stop.clone())
            }),
            Err(stop)
        );
        drop(peer);
        assert_eq!(c1_head(&writer).next_record.get(), 3);
        writer
            .with_receive(1, |sink| c1_append(sink, &cfg, 2))
            .unwrap();
        assert_eq!(c1_head(&writer).next_record.get(), 5);
    }

    #[test]
    fn c1_roles_refuse_wrong_mutations_without_poisoning_read_views() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut writer = Store::create(&cfg).unwrap();
        let original = writer
            .with_receive(0, |sink| c1_append(sink, &cfg, 1))
            .unwrap();
        let mut apply = Store::open_role(&cfg, Role::Apply).unwrap();
        let mut status = Store::open_role(&cfg, Role::Status).unwrap();
        assert_eq!(
            writer.advance_consumer(0, TxSeq(1)),
            Err(Error::RoleViolation)
        );
        for store in [&mut apply, &mut status] {
            assert_eq!(
                store.with_receive(1, |_| panic!("wrong role ran callback")),
                Err(Error::RoleViolation)
            );
            assert_eq!(
                store.with_batch(|_| panic!("wrong role ran legacy callback")),
                Err(Error::RoleViolation)
            );
            assert_eq!(c1_head(store), original);
        }
        assert_eq!(
            status.advance_consumer(0, TxSeq(1)),
            Err(Error::RoleViolation)
        );
        assert_eq!(status.checkpoint(), Err(Error::RoleViolation));
        assert_eq!(c1_head(&status), original);
        let applied = apply.advance_consumer(0, TxSeq(1)).unwrap();
        assert_eq!(applied.consumer_cursor, TxSeq(1));
        assert_eq!(apply.advance_consumer(0, TxSeq(1)), Err(Error::Conflict));
        assert_eq!(c1_head(&apply), applied);
    }

    #[test]
    fn c1_failed_raw_sync_aborts_catalog_head_and_completion() {
        for boundary in ["file", "directory"] {
            let fixture = Fixture::new();
            let mut cfg = fixture.config();
            cfg.segment_bytes = 1024;
            let mut writer = Store::create(&cfg).unwrap();
            let original = writer
                .with_receive(0, |sink| c1_append(sink, &cfg, 1))
                .unwrap();
            let (file, _peer) = unsyncable_file();
            if boundary == "directory" {
                writer.directory = file;
            } else {
                writer.active.as_mut().unwrap().file = file;
            }
            assert!(matches!(
                writer.with_receive(1, |sink| c1_append(sink, &cfg, 2)),
                Err(Error::Io(_))
            ));
            assert_eq!(
                writer.with_receive(1, |_| panic!("poisoned receive ran callback")),
                Err(Error::Poisoned)
            );
            drop(writer);
            let reopened = Store::reopen(&cfg).unwrap();
            assert_eq!(c1_head(&reopened), original);
            reopened
                .with_read_view(|view| {
                    assert!(view.completion(TxSeq(2))?.is_none());
                    Ok(())
                })
                .unwrap();
            assert_eq!(records(&reopened).unwrap().len(), 2);
        }
    }

    #[test]
    fn c1_suppressed_completion_error_aborts_raw_records_and_head() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut writer = Store::create(&cfg).unwrap();
        let (frame, complete, descriptor) = c1_records(&cfg, 1);
        assert!(
            writer
                .with_receive(0, |sink| {
                    let first_record = sink.append(&frame)?;
                    sink.append(&complete)?;
                    assert!(
                        sink.complete(CompletionEntry {
                            descriptor,
                            first_record,
                            descriptor_record: first_record
                        })
                        .is_err()
                    );
                    Ok(())
                })
                .is_err()
        );
        drop(writer);
        let reopened = Store::reopen(&cfg).unwrap();
        assert_eq!(c1_head(&reopened), CatalogHead::empty());
        assert!(records(&reopened).unwrap().is_empty());
    }

    #[test]
    fn settings_report_segment_policy_and_live_catalog_settings() {
        let fixture = Fixture::new();
        let mut cfg = fixture.config();
        cfg.cache_bytes = 65_537;
        cfg.segment_bytes = 512;
        for open in [Store::create, Store::reopen] {
            let store = open(&cfg).unwrap();
            let settings = store.settings().unwrap();
            assert_eq!(
                settings,
                serde_json::json!({
                    "configured_segment_bytes": 512,
                    "configured_sync_policy": {
                        "segment": "File::sync_all",
                        "directory": "File::sync_all",
                        "publication": "segment_and_directory_sync_before_catalog_commit"
                    },
                    "catalog": store.catalog.settings().unwrap()
                })
            );
            assert_eq!(settings["catalog"]["sqlite_version"], "3.54.1");
            assert_eq!(settings["catalog"]["configured_cache_bytes"], 65_537);
            assert_eq!(settings["catalog"]["effective_cache_budget_bytes"], 65_536);
            assert_eq!(settings["catalog"]["cache_budget_is_rss_limit"], false);
            assert_eq!(
                settings["catalog"]["effective_pragmas"]["journal_mode"],
                "wal"
            );
        }
    }

    #[test]
    fn rotates_whole_records_and_reopens_in_append_order() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).expect("create journal");
        store
            .with_batch(|sink| {
                sink(&[11; 128])?;
                sink(&[22; 128])?;
                sink(&[33; 128])
            })
            .expect("append across segments");
        store.checkpoint().expect("checked checkpoint");
        assert_eq!(
            records(&store).unwrap(),
            [vec![11; 128], vec![22; 128], vec![33; 128]]
        );
        drop(store);
        let mut reopened = Store::reopen(&cfg).expect("reopen journal");
        reopened
            .with_batch(|sink| sink(&[44; 128]))
            .expect("append after reopen");
        assert_eq!(
            records(&reopened).unwrap(),
            [vec![11; 128], vec![22; 128], vec![33; 128], vec![44; 128]]
        );
        let mut segment_sizes = fs::read_dir(&cfg.dir)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("segment-"))
            .map(|entry| entry.metadata().unwrap().len())
            .collect::<Vec<_>>();
        segment_sizes.sort_unstable();
        assert_eq!(segment_sizes, [128, 128, 256]);
    }

    #[test]
    fn failed_callback_leaves_bytes_unpublished_and_poisons_writer() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store.with_batch(|sink| sink(b"published")).unwrap();
        let failure = Error::Workload("injected batch failure");
        assert_eq!(
            store.with_batch(|sink| {
                sink(b"unpublished")?;
                Err(failure.clone())
            }),
            Err(failure)
        );
        assert_eq!(
            store.with_batch(|sink| sink(b"refused")),
            Err(Error::Poisoned)
        );
        assert_eq!(store.checkpoint(), Err(Error::Poisoned));
        drop(store);
        let mut reopened = Store::reopen(&cfg).unwrap();
        reopened.with_batch(|sink| sink(b"later")).unwrap();
        assert_eq!(
            records(&reopened).unwrap(),
            [b"published".to_vec(), b"later".to_vec()]
        );
        assert!(
            fs::read_dir(&cfg.dir)
                .unwrap()
                .filter_map(|entry| {
                    let path = entry.ok()?.path();
                    path.file_name()?
                        .to_str()?
                        .starts_with("segment-")
                        .then_some(path)
                })
                .any(|path| fs::read(path)
                    .unwrap()
                    .windows(11)
                    .any(|window| window == b"unpublished"))
        );
    }

    #[test]
    fn swallowed_sink_failure_cannot_publish_the_batch() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        assert!(
            store
                .with_batch(|sink| {
                    sink(b"also unpublished")?;
                    assert!(sink(&[1; 129]).is_err());
                    Ok(())
                })
                .is_err()
        );
        drop(store);
        assert!(records(&Store::reopen(&cfg).unwrap()).unwrap().is_empty());
    }

    #[test]
    fn missing_and_foreign_identity_are_refused_without_adoption() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        assert!(Store::reopen(&cfg).is_err());
        assert!(!cfg.dir.exists());
        drop(Store::create(&cfg).unwrap());
        let identity = fs::read(cfg.dir.join("identity")).unwrap();
        let mut foreign = cfg.clone();
        foreign.stream.generation += 1;
        assert!(matches!(
            Store::reopen(&foreign),
            Err(Error::IdentityMismatch)
        ));
        assert_eq!(fs::read(cfg.dir.join("identity")).unwrap(), identity);
        assert!(Store::create(&cfg).is_err());
    }

    #[test]
    fn referenced_damage_or_missing_segment_refuses_reopen() {
        for damage in ["missing", "truncated", "corrupt"] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut store = Store::create(&cfg).unwrap();
            store
                .with_batch(|sink| {
                    sink(&[21; 128])?;
                    sink(&[22; 128])
                })
                .unwrap();
            drop(store);
            let path = cfg.dir.join("segment-0000000000000001.data");
            match damage {
                "missing" => fs::remove_file(path).unwrap(),
                "truncated" => fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .unwrap()
                    .set_len(129)
                    .unwrap(),
                "corrupt" => {
                    let mut bytes = fs::read(&path).unwrap();
                    bytes[53] ^= 1;
                    fs::write(path, bytes).unwrap();
                }
                _ => unreachable!(),
            }
            assert!(Store::reopen(&cfg).is_err(), "must reject {damage}");
        }
    }

    #[test]
    fn raw_batch_byte_limit_rolls_back_all_locators() {
        let fixture = Fixture::new();
        let mut cfg = fixture.config();
        cfg.batch_bytes = 256;
        let mut store = Store::create(&cfg).unwrap();
        assert!(
            store
                .with_batch(|sink| {
                    sink(&[1; 128])?;
                    sink(&[2; 128])?;
                    let _ = sink(&[3]);
                    Ok(())
                })
                .is_err()
        );
        drop(store);
        assert!(records(&Store::reopen(&cfg).unwrap()).unwrap().is_empty());
    }

    #[test]
    fn tiny_records_use_raw_byte_budget_instead_of_locator_byte_budget() {
        let fixture = Fixture::new();
        let mut cfg = fixture.config();
        cfg.batch_bytes = 128;
        let mut store = Store::create(&cfg).unwrap();
        store
            .with_batch(|sink| {
                for value in 0..128 {
                    sink(&[value])?;
                }
                Ok(())
            })
            .unwrap();
        let mut count = 0;
        store
            .scan_records(&mut |record| {
                assert_eq!(record, &[count]);
                count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(count, 128);
    }

    #[test]
    fn scan_callback_error_is_returned_without_advancing_or_collecting_rest() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store
            .with_batch(|sink| {
                sink(b"first")?;
                sink(b"second")
            })
            .unwrap();
        let failure = Error::Workload("reader stopped");
        let mut seen = 0;
        assert_eq!(
            store.scan_records(&mut |_| {
                seen += 1;
                Err(failure.clone())
            }),
            Err(failure)
        );
        assert_eq!(seen, 1);
        assert_eq!(
            records(&store).unwrap(),
            [b"first".to_vec(), b"second".to_vec()]
        );
    }

    fn unsyncable_file() -> (File, std::os::unix::net::UnixStream) {
        let (socket, peer) = std::os::unix::net::UnixStream::pair().unwrap();
        (File::from(std::os::fd::OwnedFd::from(socket)), peer)
    }

    #[test]
    fn failed_file_or_directory_sync_cannot_publish_locators() {
        for boundary in ["active", "rotation", "directory"] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut store = Store::create(&cfg).unwrap();
            store.with_batch(|sink| sink(&[7; 128])).unwrap();
            let (file, _peer) = unsyncable_file();
            if boundary == "directory" {
                store.directory = file;
            } else {
                store.active.as_mut().unwrap().file = file;
            }
            assert!(
                matches!(
                    store.with_batch(|sink| {
                        sink(&[8; 128])?;
                        if boundary == "rotation" {
                            sink(&[9; 128])?;
                        }
                        Ok(())
                    }),
                    Err(Error::Io(_))
                ),
                "must report {boundary} sync failure"
            );
            assert_eq!(
                store.with_batch(|sink| sink(b"later")),
                Err(Error::Poisoned)
            );
            drop(store);
            assert_eq!(
                records(&Store::reopen(&cfg).unwrap()).unwrap(),
                [vec![7; 128]]
            );
        }
    }

    #[test]
    fn failed_write_is_latched_even_when_callback_ignores_it() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store.with_batch(|sink| sink(b"retained")).unwrap();
        store.active.as_mut().unwrap().file = File::open(segment_path(&cfg.dir, 1)).unwrap();
        assert!(matches!(
            store.with_batch(|sink| {
                assert!(matches!(sink(b"rejected"), Err(Error::Io(_))));
                Ok(())
            }),
            Err(Error::Io(_))
        ));
        drop(store);
        assert_eq!(
            records(&Store::reopen(&cfg).unwrap()).unwrap(),
            [b"retained".to_vec()]
        );
    }

    #[test]
    fn checkpoint_sync_failure_is_explicit_and_poisons_the_store() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store.with_batch(|sink| sink(b"record")).unwrap();
        let (file, _peer) = unsyncable_file();
        store.active.as_mut().unwrap().file = file;
        assert!(matches!(store.checkpoint(), Err(Error::Io(_))));
        assert_eq!(store.checkpoint(), Err(Error::Poisoned));
    }

    #[test]
    fn checkpoint_reports_busy_catalog_instead_of_claiming_drain() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store
            .with_batch(|sink| {
                sink(b"first")?;
                sink(b"second")
            })
            .unwrap();
        let reader = sqlite::Store::reopen(&catalog_config(&cfg).unwrap()).unwrap();
        let mut attempted = false;
        reader
            .scan_records(&mut |_| {
                if !attempted {
                    store.with_batch(|sink| sink(b"newer"))?;
                    assert!(matches!(store.checkpoint(), Err(Error::Backend(_))));
                    attempted = true;
                }
                Ok(())
            })
            .unwrap();
        assert!(attempted);
        assert_eq!(store.checkpoint(), Err(Error::Poisoned));
        drop(reader);
        drop(store);
        let mut reopened = Store::reopen(&cfg).unwrap();
        reopened.checkpoint().unwrap();
        assert_eq!(
            records(&reopened).unwrap(),
            [b"first".to_vec(), b"second".to_vec(), b"newer".to_vec()]
        );
    }

    fn locator_bytes(segment: u64, offset: u64, length: u64, payload: &[u8]) -> [u8; 40] {
        let mut raw = [0; 40];
        raw[..8].copy_from_slice(b"RPGLOC01");
        raw[8..16].copy_from_slice(&segment.to_le_bytes());
        raw[16..24].copy_from_slice(&offset.to_le_bytes());
        raw[24..32].copy_from_slice(&length.to_le_bytes());
        raw[32..36].copy_from_slice(&crc32fast::hash(payload).to_le_bytes());
        let checksum = crc32fast::hash(&raw[..36]);
        raw[36..].copy_from_slice(&checksum.to_le_bytes());
        raw
    }

    #[test]
    fn forged_locators_are_rejected_before_any_record_is_returned() {
        let mut invalid = vec![
            locator_bytes(0, 0, 1, b"x").to_vec(),
            locator_bytes(1, 0, 0, b"").to_vec(),
            locator_bytes(1, 0, 129, &[0; 129]).to_vec(),
            locator_bytes(1, u64::MAX, 2, b"xx").to_vec(),
            locator_bytes(1, 256, 1, b"x").to_vec(),
            locator_bytes(1, 0, u64::MAX, b"").to_vec(),
            locator_bytes(1, 0, 1, b"x")[..39].to_vec(),
            vec![0; 41],
        ];
        let mut checksum = locator_bytes(1, 0, 1, b"x");
        checksum[16] ^= 1;
        invalid.push(checksum.to_vec());
        let mut version = locator_bytes(1, 0, 1, b"x");
        version[..8].copy_from_slice(b"RPGLOC02");
        let checksum = crc32fast::hash(&version[..36]);
        version[36..].copy_from_slice(&checksum.to_le_bytes());
        invalid.push(version.to_vec());

        for locator in invalid {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let store = Store::create(&cfg).unwrap();
            drop(store);
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(segment_path(&cfg.dir, 1))
                .unwrap();
            file.write_all(&[0; 256]).unwrap();
            let mut catalog = sqlite::Store::reopen(&catalog_config(&cfg).unwrap()).unwrap();
            catalog.with_batch(|sink| sink(&locator)).unwrap();
            drop(catalog);
            assert!(Store::reopen(&cfg).is_err());
        }
    }

    #[test]
    fn overlapping_locators_and_file_boundary_overruns_are_rejected() {
        for (segment, offset, length) in [(1, 0, 8), (1, 8, 1), (2, 0, 1)] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut store = Store::create(&cfg).unwrap();
            store.with_batch(|sink| sink(b"retained")).unwrap();
            drop(store);
            let mut catalog = sqlite::Store::reopen(&catalog_config(&cfg).unwrap()).unwrap();
            catalog
                .with_batch(|sink| sink(&locator_bytes(segment, offset, length, b"retained")))
                .unwrap();
            drop(catalog);
            assert!(Store::reopen(&cfg).is_err());
        }
    }

    #[test]
    fn reopening_preserves_unpublished_partial_bytes_and_uses_a_fresh_segment() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store.with_batch(|sink| sink(b"required")).unwrap();
        drop(store);
        let first = segment_path(&cfg.dir, 1);
        OpenOptions::new()
            .append(true)
            .open(&first)
            .unwrap()
            .write_all(b"partial")
            .unwrap();
        let orphan = segment_path(&cfg.dir, 2);
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&orphan)
            .unwrap()
            .write_all(b"unpublished")
            .unwrap();
        let mut reopened = Store::reopen(&cfg).unwrap();
        reopened.with_batch(|sink| sink(b"later")).unwrap();
        assert_eq!(fs::read(first).unwrap(), b"requiredpartial");
        assert_eq!(fs::read(orphan).unwrap(), b"unpublished");
        assert_eq!(fs::read(segment_path(&cfg.dir, 3)).unwrap(), b"later");
        assert_eq!(
            records(&reopened).unwrap(),
            [b"required".to_vec(), b"later".to_vec()]
        );
    }
}
