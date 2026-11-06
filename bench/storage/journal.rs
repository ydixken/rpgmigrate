//! Segmented experiment data uses SQLite's raw store as its publication catalog.
//! The cache budget applies to SQLite metadata; raw reads retain one bounded record.
//! Unpublished bytes stay on disk because recovery has no authority to truncate them.

use super::config::{Config, Error, RecordSink, Result};
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
            directory,
            active: None,
            next_segment: 1,
            poisoned: false,
        })
    }

    pub(super) fn reopen(cfg: &Config) -> Result<Self> {
        cfg.verify_directory()?;
        let catalog = sqlite::Store::reopen(&catalog_config(cfg)?)?;
        let mut largest = 0;
        for entry in fs::read_dir(&cfg.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name == "identity" || name == "catalog" {
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
            directory: File::open(&cfg.dir)?,
            active: None,
            next_segment: largest.checked_add(1).ok_or(Error::Overflow)?,
            poisoned: false,
        };
        store.scan_records(&mut |_| Ok(()))?;
        Ok(store)
    }

    pub(super) fn with_batch(
        &mut self,
        write: impl FnOnce(&mut RecordSink<'_>) -> Result<()>,
    ) -> Result<()> {
        self.check_healthy()?;
        self.poisoned = true;
        let result = self.catalog.with_batch(|publish| {
            let mut written = 0u64;
            let mut failure = None;
            write(&mut |record| {
                if failure.is_some() {
                    return Err(Error::Poisoned);
                }
                let result = append_record(
                    &self.cfg,
                    &mut self.active,
                    &mut self.next_segment,
                    &mut written,
                    record,
                    publish,
                );
                if let Err(error) = &result {
                    failure = Some(error.clone());
                }
                result
            })?;
            if let Some(error) = failure {
                return Err(error);
            }
            // Catalog COMMIT is allowed only after every referenced byte and name is durable.
            if let Some(segment) = &self.active {
                segment.file.sync_all()?;
            }
            self.directory.sync_all()?;
            Ok(())
        });
        if result.is_ok() {
            self.poisoned = false;
        }
        result
    }

    pub(super) fn scan_records(&self, visit: &mut RecordSink<'_>) -> Result<()> {
        self.check_healthy()?;
        let mut segment: Option<Segment> = None;
        let mut previous_end = 0;
        self.catalog.scan_records(&mut |raw| {
            let locator = Locator::decode(raw, &self.cfg)?;
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
            let end = locator
                .offset
                .checked_add(locator.length)
                .ok_or(Error::Overflow)?;
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

    pub(super) fn checkpoint(&mut self) -> Result<()> {
        self.check_healthy()?;
        self.poisoned = true;
        if let Some(segment) = &self.active {
            segment.file.sync_all()?;
        }
        self.directory.sync_all()?;
        self.catalog.checkpoint()?;
        self.poisoned = false;
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
    publish: &mut RecordSink<'_>,
) -> Result<()> {
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
        segment.file.sync_all()?;
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
    publish(&locator.encode())?;
    *written = next_written;
    Ok(())
}

struct Locator {
    segment: u64,
    offset: u64,
    length: u64,
    checksum: u32,
}

impl Locator {
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
