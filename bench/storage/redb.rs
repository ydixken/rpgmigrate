//! Raw-record storage for the isolated persistence experiment.
//! MultiWriter includes two-phase synchronization and allocator-state persistence per commit.
//! Reopen includes a full integrity scan, allocator rebuild, and redb's repair commit.
//! This selection does not qualify overlapping migration processes.

use super::config::{Config, Error, RecordSink, Result};
use ::redb::backends::FileBackend;
use ::redb::{
    Builder, ConcurrencyMode, Database, Durability, ReadableDatabase, ReadableTable,
    TableDefinition, TableHandle,
};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

const RECORDS: TableDefinition<u64, &[u8]> = TableDefinition::new("records");
const METADATA: TableDefinition<u8, u64> = TableDefinition::new("metadata");
const RECORD_COUNT: u8 = 0;
const DATABASE_FILE: &str = "records.redb";

pub struct Store {
    db: Database,
    max_record_bytes: usize,
    batch_bytes: u64,
    cache_bytes: usize,
    poisoned: bool,
}

impl Store {
    pub fn settings(&self) -> Result<serde_json::Value> {
        // redb exposes no getters for these settings, so report the configured policy.
        Ok(serde_json::json!({
            "configured_policy": {
                "concurrency_mode": "MultiWriter",
                "durability": "Immediate",
                "check_integrity_on_open": true,
                "cache_bytes": self.cache_bytes
            },
            "policy_source": "constructor_and_batch_configuration",
            "cache_budget_is_rss_limit": false
        }))
    }

    pub fn create(cfg: &Config) -> Result<Self> {
        cfg.create_directory()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(cfg.dir.join(DATABASE_FILE))?;
        let backend = FileBackend::new(file)
            .map_err(|_| Error::Backend("redb file initialization failed"))?;
        let db = builder(cfg)
            .create_with_backend(backend)
            .map_err(|_| Error::Backend("redb database creation failed"))?;
        let mut transaction = db
            .begin_write()
            .map_err(|_| Error::Backend("redb write transaction failed"))?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| Error::Backend("redb immediate durability is unavailable"))?;
        {
            let _records = transaction.open_table(RECORDS).map_err(table_error)?;
            let mut metadata = transaction.open_table(METADATA).map_err(table_error)?;
            metadata
                .insert(RECORD_COUNT, 0)
                .map_err(|_| Error::Backend("redb metadata write failed"))?;
        }
        transaction
            .commit()
            .map_err(|_| Error::Backend("redb initial commit failed; reopen required"))?;
        // File synchronization does not persist the newly created directory entry.
        File::open(&cfg.dir)?.sync_all()?;
        Ok(Self::new(db, cfg))
    }

    pub fn reopen(cfg: &Config) -> Result<Self> {
        cfg.verify_directory()?;
        let path = cfg.dir.join(DATABASE_FILE);
        let file = fs::symlink_metadata(&path)?;
        if !file.is_file() || file.nlink() != 1 || file.permissions().mode() & 0o777 != 0o600 {
            return Err(Error::IdentityMismatch);
        }
        let db = builder(cfg)
            .open(path)
            .map_err(|_| Error::Backend("redb database open failed"))?;
        {
            let transaction = db
                .begin_read()
                .map_err(|_| Error::Backend("redb read transaction failed"))?;
            let metadata = transaction.open_table(METADATA).map_err(table_error)?;
            let records = transaction.open_table(RECORDS).map_err(table_error)?;
            stored_count(&metadata, &records)?;
        }
        Ok(Self::new(db, cfg))
    }

    fn new(db: Database, cfg: &Config) -> Self {
        Self {
            db,
            max_record_bytes: cfg.limits.max_frame_bytes,
            batch_bytes: cfg.batch_bytes,
            cache_bytes: cfg.cache_bytes,
            poisoned: false,
        }
    }

    pub fn with_batch(
        &mut self,
        write: impl FnOnce(&mut RecordSink<'_>) -> Result<()>,
    ) -> Result<()> {
        self.checkpoint()?;
        // Unwinding and uncertain commit outcomes must also require reopen.
        self.poisoned = true;
        let mut transaction = self
            .db
            .begin_write()
            .map_err(|_| Error::Backend("redb write transaction failed"))?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| Error::Backend("redb immediate durability is unavailable"))?;
        let result = (|| {
            // WriteTransaction::open_table creates absent tables, so check existence first.
            let tables = transaction
                .list_tables()
                .map_err(|_| Error::Backend("redb table inventory failed"))?;
            let mut present = 0;
            for table in tables {
                if table.name() == RECORDS.name() {
                    present |= 1;
                }
                if table.name() == METADATA.name() {
                    present |= 2;
                }
            }
            if present != 3 {
                return Err(Error::Gap);
            }
            let mut records = transaction.open_table(RECORDS).map_err(table_error)?;
            let mut metadata = transaction.open_table(METADATA).map_err(table_error)?;
            let mut count = stored_count(&metadata, &records)?;
            let mut written = 0u64;
            let mut failure = None;
            let callback = write(&mut |bytes| {
                if failure.is_some() {
                    return Err(Error::Poisoned);
                }
                let result = (|| {
                    check_record(bytes, self.max_record_bytes)?;
                    let total = written
                        .checked_add(u64::try_from(bytes.len()).map_err(|_| Error::Overflow)?)
                        .ok_or(Error::Overflow)?;
                    if total > self.batch_bytes {
                        return Err(Error::Backend(
                            "redb batch exceeds the configured byte limit",
                        ));
                    }
                    let next = count.checked_add(1).ok_or(Error::Overflow)?;
                    if records
                        .insert(next, bytes)
                        .map_err(|_| Error::Backend("redb record write failed"))?
                        .is_some()
                    {
                        return Err(Error::Gap);
                    }
                    written = total;
                    count = next;
                    Ok(())
                })();
                if let Err(error) = &result {
                    failure = Some(error.clone());
                }
                result
            });
            if let Some(error) = failure {
                return Err(error);
            }
            callback?;
            metadata
                .insert(RECORD_COUNT, count)
                .map_err(|_| Error::Backend("redb metadata write failed"))?;
            Ok(())
        })();
        if let Err(error) = result {
            transaction
                .abort()
                .map_err(|_| Error::Backend("redb abort failed; reopen required"))?;
            return Err(error);
        }
        transaction
            .commit()
            .map_err(|_| Error::Backend("redb commit outcome is uncertain; reopen required"))?;
        self.poisoned = false;
        Ok(())
    }

    pub fn scan_records(&self, visit: &mut RecordSink<'_>) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let transaction = self
            .db
            .begin_read()
            .map_err(|_| Error::Backend("redb read transaction failed"))?;
        let metadata = transaction.open_table(METADATA).map_err(table_error)?;
        let records = transaction.open_table(RECORDS).map_err(table_error)?;
        let count = stored_count(&metadata, &records)?;
        let mut seen = 0u64;
        for entry in records
            .iter()
            .map_err(|_| Error::Backend("redb record scan failed"))?
        {
            let (key, bytes) = entry.map_err(|_| Error::Backend("redb record read failed"))?;
            seen = seen.checked_add(1).ok_or(Error::Overflow)?;
            if key.value() != seen || seen > count {
                return Err(Error::Gap);
            }
            check_record(bytes.value(), self.max_record_bytes)?;
            visit(bytes.value())?;
        }
        if seen != count {
            return Err(Error::Gap);
        }
        Ok(())
    }

    /// Immediate commits already synchronize data and metadata; redb has no separate WAL to drain.
    /// This checks prior failures without adding an empty transaction or a fake durability barrier.
    pub fn checkpoint(&mut self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }
}

fn builder(cfg: &Config) -> Builder {
    let mut builder = Database::builder();
    builder
        .set_cache_size(cfg.cache_bytes)
        .set_concurrency_mode(ConcurrencyMode::MultiWriter)
        .set_check_integrity_on_open(true);
    builder
}

fn table_error(error: ::redb::TableError) -> Error {
    match error {
        ::redb::TableError::TableDoesNotExist(_) => Error::Gap,
        _ => Error::Backend("redb table open failed"),
    }
}

fn stored_count(
    metadata: &impl ReadableTable<u8, u64>,
    records: &impl ReadableTable<u64, &'static [u8]>,
) -> Result<u64> {
    let count = metadata
        .get(RECORD_COUNT)
        .map_err(|_| Error::Backend("redb metadata read failed"))?
        .ok_or(Error::Gap)?
        .value();
    if records
        .len()
        .map_err(|_| Error::Backend("redb record count failed"))?
        != count
    {
        return Err(Error::Gap);
    }
    if count > 0 {
        let first = records
            .first()
            .map_err(|_| Error::Backend("redb first record failed"))?
            .ok_or(Error::Gap)?
            .0
            .value();
        let last = records
            .last()
            .map_err(|_| Error::Backend("redb last record failed"))?
            .ok_or(Error::Gap)?
            .0
            .value();
        // Unique integer keys with these endpoints and count must cover the entire interval.
        if first != 1 || last != count {
            return Err(Error::Gap);
        }
    }
    Ok(count)
}

fn check_record(bytes: &[u8], limit: usize) -> Result<()> {
    if bytes.is_empty() || bytes.len() > limit {
        return Err(Error::Backend(
            "redb record exceeds the configured byte bounds",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Limits, RunId, SourceId, StreamId};
    use std::fs;
    use std::io;
    use std::ops::Bound;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct Fixture(PathBuf);

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "rpgmigrate-redb-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn config(&self) -> Config {
            Config {
                dir: self.0.join("store"),
                stream: StreamId {
                    run: RunId([0x21; 16]),
                    source: SourceId {
                        system_identifier: 17,
                        database_oid: 42,
                    },
                    generation: 3,
                },
                limits: Limits {
                    max_frame_bytes: 256,
                    ..Limits::default()
                },
                batch_bytes: 512,
                cache_bytes: 1024 * 1024,
                segment_bytes: 1024,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn records(store: &Store) -> Result<Vec<Vec<u8>>> {
        let mut result = Vec::new();
        store.scan_records(&mut |bytes| {
            result.push(bytes.to_vec());
            Ok(())
        })?;
        Ok(result)
    }

    #[derive(Debug, Default)]
    struct SyncProbe {
        calls: AtomicUsize,
        fail_on: AtomicUsize,
    }

    #[derive(Debug)]
    struct ObservedFile {
        file: FileBackend,
        probe: Arc<SyncProbe>,
    }

    impl ::redb::StorageBackend for ObservedFile {
        fn len(&self) -> io::Result<u64> {
            self.file.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            self.file.read(offset, out)
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.file.set_len(len)
        }
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            self.file.write(offset, data)
        }
        fn sync_data(&self) -> io::Result<()> {
            let call = self.probe.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.probe.fail_on.load(Ordering::SeqCst) == call {
                return Err(io::Error::other("injected redb synchronization failure"));
            }
            self.file.sync_data()
        }
        fn close(&self) -> io::Result<()> {
            self.file.close()
        }
        fn try_lock_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> std::result::Result<bool, ::redb::BackendError> {
            self.file.try_lock_range(start, end)
        }
        fn try_lock_shared_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> std::result::Result<bool, ::redb::BackendError> {
            self.file.try_lock_shared_range(start, end)
        }
        fn lock_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> std::result::Result<(), ::redb::BackendError> {
            self.file.lock_range(start, end)
        }
        fn lock_shared_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> std::result::Result<(), ::redb::BackendError> {
            self.file.lock_shared_range(start, end)
        }
        fn unlock_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> std::result::Result<(), ::redb::BackendError> {
            self.file.unlock_range(start, end)
        }
        fn query_lock_range(
            &self,
            start: Bound<u64>,
            end: Bound<u64>,
        ) -> std::result::Result<bool, ::redb::BackendError> {
            self.file.query_lock_range(start, end)
        }
    }

    fn observed_store(cfg: &Config) -> (Store, Arc<SyncProbe>) {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(cfg.dir.join(DATABASE_FILE))
            .unwrap();
        let probe = Arc::new(SyncProbe::default());
        let backend = ObservedFile {
            file: FileBackend::new(file).unwrap(),
            probe: probe.clone(),
        };
        let db = builder(cfg).create_with_backend(backend).unwrap();
        probe.calls.store(0, Ordering::SeqCst);
        (Store::new(db, cfg), probe)
    }

    #[test]
    fn settings_label_configured_policies_and_retain_each_open_cache_budget() {
        let fixture = Fixture::new();
        let mut cfg = fixture.config();
        cfg.cache_bytes = 65_537;
        let store = Store::create(&cfg).unwrap();
        cfg.cache_bytes = 131_073;
        let mut expected = serde_json::json!({
            "configured_policy": {
                "concurrency_mode": "MultiWriter",
                "durability": "Immediate",
                "check_integrity_on_open": true,
                "cache_bytes": 65_537
            },
            "policy_source": "constructor_and_batch_configuration",
            "cache_budget_is_rss_limit": false
        });
        assert_eq!(store.settings().unwrap(), expected);
        drop(store);
        let reopened = Store::reopen(&cfg).unwrap();
        expected["configured_policy"]["cache_bytes"] = serde_json::json!(131_073);
        assert_eq!(reopened.settings().unwrap(), expected);
    }

    #[test]
    fn settings_do_not_add_commits_or_sync_calls() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        drop(Store::create(&cfg).unwrap());
        let (mut store, probe) = observed_store(&cfg);
        store.with_batch(|sink| sink(b"committed")).unwrap();
        assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            store.settings().unwrap()["configured_policy"]["durability"],
            "Immediate"
        );
        assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
        assert_eq!(records(&store).unwrap(), vec![b"committed".to_vec()]);
    }

    #[test]
    fn raw_records_roundtrip_in_append_order_across_reopen() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).expect("redb store creation succeeds");
        let first = [0; 128];
        let second = [0xff; 256];
        store
            .with_batch(|sink| {
                sink(&first)?;
                sink(&second)
            })
            .unwrap();
        assert_eq!(
            records(&store).unwrap(),
            vec![first.to_vec(), second.to_vec()]
        );
        drop(store);
        let mut store = Store::reopen(&cfg).unwrap();
        store.with_batch(|sink| sink(&first)).unwrap();
        assert_eq!(
            records(&store).unwrap(),
            vec![first.to_vec(), second.to_vec(), first.to_vec()]
        );
    }

    #[test]
    fn callback_error_aborts_the_whole_batch_and_requires_reopen() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store.with_batch(|sink| sink(b"committed")).unwrap();
        let failure = Error::Workload("callback failed");
        assert_eq!(
            store.with_batch(|sink| {
                sink(b"aborted")?;
                Err(failure.clone())
            }),
            Err(failure)
        );
        assert_eq!(store.with_batch(|_| Ok(())), Err(Error::Poisoned));
        assert_eq!(store.checkpoint(), Err(Error::Poisoned));
        drop(store);
        assert_eq!(
            records(&Store::reopen(&cfg).unwrap()).unwrap(),
            vec![b"committed".to_vec()]
        );
    }

    #[test]
    fn swallowed_empty_oversize_and_batch_limit_errors_cannot_publish() {
        for invalid in [Vec::new(), vec![1; 257], vec![2; 256]] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut store = Store::create(&cfg).unwrap();
            let outcome = store.with_batch(|sink| {
                sink(&[0; 256])?;
                sink(b"x")?;
                assert!(sink(&invalid).is_err());
                assert_eq!(sink(b"later"), Err(Error::Poisoned));
                Ok(())
            });
            assert!(outcome.is_err());
            drop(store);
            assert!(records(&Store::reopen(&cfg).unwrap()).unwrap().is_empty());
        }
    }

    #[test]
    fn exact_record_and_batch_limits_accept_without_accumulating_batches() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        for _ in 0..2 {
            store
                .with_batch(|sink| {
                    sink(&[7; 256])?;
                    sink(&[8; 256])
                })
                .unwrap();
        }
        assert_eq!(
            records(&store).unwrap(),
            vec![vec![7; 256], vec![8; 256], vec![7; 256], vec![8; 256]]
        );
    }

    #[test]
    fn unwinding_callback_cannot_publish_or_reuse_the_store() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = store.with_batch(|sink| {
                sink(b"aborted")?;
                panic!("callback unwound");
            });
        }));
        assert!(panic.is_err());
        assert_eq!(store.with_batch(|_| Ok(())), Err(Error::Poisoned));
        drop(store);
        assert!(records(&Store::reopen(&cfg).unwrap()).unwrap().is_empty());
    }

    #[test]
    fn missing_directory_and_database_are_never_created_by_reopen() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        assert!(matches!(
            Store::reopen(&cfg),
            Err(Error::Io(std::io::ErrorKind::NotFound))
        ));
        assert!(!cfg.dir.exists());
        cfg.create_directory().unwrap();
        assert!(Store::reopen(&cfg).is_err());
        assert_eq!(fs::read_dir(&cfg.dir).unwrap().count(), 1);
    }

    #[test]
    fn foreign_identity_is_rejected_before_database_open() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        drop(Store::create(&cfg).unwrap());
        let before = fs::read(cfg.dir.join("records.redb")).unwrap();
        for change in 0..4 {
            let mut foreign = cfg.clone();
            match change {
                0 => foreign.stream.run.0[0] ^= 1,
                1 => foreign.stream.source.system_identifier += 1,
                2 => foreign.stream.source.database_oid += 1,
                _ => foreign.stream.generation += 1,
            }
            assert!(matches!(
                Store::reopen(&foreign),
                Err(Error::IdentityMismatch)
            ));
            assert_eq!(fs::read(cfg.dir.join("records.redb")).unwrap(), before);
        }
        assert!(matches!(
            Store::create(&cfg),
            Err(Error::Io(std::io::ErrorKind::AlreadyExists))
        ));
        assert_eq!(fs::read(cfg.dir.join("records.redb")).unwrap(), before);
    }

    #[test]
    fn invalid_config_has_no_creation_side_effects() {
        let fixture = Fixture::new();
        let mut cfg = fixture.config();
        cfg.cache_bytes = 0;
        assert!(matches!(Store::create(&cfg), Err(Error::Config(_))));
        assert!(!cfg.dir.exists());
    }

    #[test]
    fn scan_propagates_visitor_failure_without_poisoning_committed_data() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store
            .with_batch(|sink| {
                sink(b"first")?;
                sink(b"second")
            })
            .unwrap();
        let mut visited = 0;
        let error = Error::Workload("visitor failed");
        assert_eq!(
            store.scan_records(&mut |_| {
                visited += 1;
                Err(error.clone())
            }),
            Err(error)
        );
        assert_eq!(visited, 1);
        store.checkpoint().unwrap();
        assert_eq!(
            records(&store).unwrap(),
            vec![b"first".to_vec(), b"second".to_vec()]
        );
    }

    #[test]
    fn committed_data_survives_process_exit_with_or_without_checkpoint() {
        const PROBE: &str = "RPGMIGRATE_REDB_EXIT_PROBE";
        if let Some(path) = std::env::var_os(PROBE) {
            let fixture = std::mem::ManuallyDrop::new(Fixture(PathBuf::from(path)));
            let mut store = Store::create(&fixture.config()).unwrap();
            store.with_batch(|sink| sink(&[0x55; 128])).unwrap();
            if std::env::var_os("RPGMIGRATE_REDB_CHECKPOINT").is_some() {
                store.checkpoint().unwrap();
            }
            // Bypass Drop so the parent can detect dependence on close-time flushing.
            std::process::exit(83);
        }
        for checkpoint in [false, true] {
            let fixture = Fixture::new();
            let thread = std::thread::current();
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .arg("--exact")
                .arg(thread.name().unwrap())
                .env(PROBE, &fixture.0);
            command.env_remove("RPGMIGRATE_REDB_CHECKPOINT");
            if checkpoint {
                command.env("RPGMIGRATE_REDB_CHECKPOINT", "1");
            }
            let output = command.output().unwrap();
            assert_eq!(
                output.status.code(),
                Some(83),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let store = Store::reopen(&fixture.config()).unwrap();
            assert_eq!(records(&store).unwrap(), vec![vec![0x55; 128]]);
        }
    }

    #[test]
    fn missing_or_reordered_keys_are_gaps_on_scan_and_reopen() {
        for removed in [0, 1, 2, 3, 4] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut store = Store::create(&cfg).unwrap();
            store
                .with_batch(|sink| {
                    sink(b"a")?;
                    sink(b"b")?;
                    sink(b"c")
                })
                .unwrap();
            let transaction = store.db.begin_write().unwrap();
            {
                let mut table = transaction.open_table(RECORDS).unwrap();
                if removed == 0 {
                    for key in 1..=3 {
                        table.remove(key).unwrap();
                    }
                } else if removed == 4 {
                    table.remove(2).unwrap();
                    table.insert(4, b"b".as_slice()).unwrap();
                } else {
                    table.remove(removed).unwrap();
                }
            }
            transaction.commit().unwrap();
            assert_eq!(records(&store), Err(Error::Gap));
            drop(store);
            assert!(
                matches!(Store::reopen(&cfg), Err(Error::Gap)),
                "removed key {removed}"
            );
        }
    }

    #[test]
    fn missing_tables_and_record_count_are_not_recreated() {
        for missing in 0..3 {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut store = Store::create(&cfg).unwrap();
            let transaction = store.db.begin_write().unwrap();
            match missing {
                0 => {
                    transaction.delete_table(RECORDS).unwrap();
                }
                1 => {
                    transaction.delete_table(METADATA).unwrap();
                }
                _ => {
                    transaction
                        .open_table(METADATA)
                        .unwrap()
                        .remove(RECORD_COUNT)
                        .unwrap();
                }
            }
            transaction.commit().unwrap();
            assert_eq!(records(&store), Err(Error::Gap));
            assert_eq!(store.with_batch(|_| Ok(())), Err(Error::Gap));
            drop(store);
            assert!(matches!(Store::reopen(&cfg), Err(Error::Gap)));
        }
    }

    #[test]
    fn oversized_persisted_record_is_rejected_on_read() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store.with_batch(|sink| sink(b"original")).unwrap();
        let transaction = store.db.begin_write().unwrap();
        transaction
            .open_table(RECORDS)
            .unwrap()
            .insert(1, [0; 257].as_slice())
            .unwrap();
        transaction.commit().unwrap();
        assert!(records(&store).is_err());
    }

    #[test]
    fn selected_multiwriter_mode_accepts_peer_handle_and_rejects_none() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        store.with_batch(|sink| sink(b"committed")).unwrap();
        let peer = builder(&cfg).open(cfg.dir.join(DATABASE_FILE)).unwrap();
        let mut transaction = peer.begin_write().unwrap();
        assert!(matches!(
            transaction.set_durability(Durability::None),
            Err(::redb::SetDurabilityError::NonDurableCommitUnsupported)
        ));
        transaction.abort().unwrap();
    }

    #[test]
    fn database_links_and_nonprivate_permissions_are_rejected_before_open() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        for link in [0, 1, 2] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            drop(Store::create(&cfg).unwrap());
            let path = cfg.dir.join(DATABASE_FILE);
            let outside = fixture.0.join("foreign.redb");
            if link == 2 {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
            } else {
                fs::rename(&path, &outside).unwrap();
                if link == 0 {
                    symlink(&outside, &path).unwrap();
                } else {
                    fs::hard_link(&outside, &path).unwrap();
                }
            }
            let before = fs::read(&path).unwrap();
            assert!(
                matches!(Store::reopen(&cfg), Err(Error::IdentityMismatch)),
                "file case {link}"
            );
            assert_eq!(fs::read(&path).unwrap(), before);
        }
    }

    #[test]
    fn multiwriter_commit_syncs_twice_and_checkpoint_adds_no_sync() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        drop(Store::create(&cfg).unwrap());
        let (mut store, probe) = observed_store(&cfg);
        store.with_batch(|sink| sink(b"committed")).unwrap();
        assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
        store.checkpoint().unwrap();
        assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
        drop(store);
        assert_eq!(
            records(&Store::reopen(&cfg).unwrap()).unwrap(),
            vec![b"committed".to_vec()]
        );
    }

    #[test]
    fn failed_commit_sync_requires_reopen_without_claiming_rollback() {
        for fail_on in [1, 2] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut store = Store::create(&cfg).unwrap();
            store.with_batch(|sink| sink(b"before")).unwrap();
            drop(store);
            let (mut store, probe) = observed_store(&cfg);
            probe.fail_on.store(fail_on, Ordering::SeqCst);
            assert!(matches!(
                store.with_batch(|sink| sink(b"uncertain")),
                Err(Error::Backend(_))
            ));
            assert_eq!(probe.calls.load(Ordering::SeqCst), fail_on);
            assert_eq!(store.checkpoint(), Err(Error::Poisoned));
            assert_eq!(
                store.with_batch(|_| panic!("poisoned store ran callback")),
                Err(Error::Poisoned)
            );
            probe.fail_on.store(0, Ordering::SeqCst);
            drop(store);
            let persisted = records(&Store::reopen(&cfg).unwrap()).unwrap();
            assert!(
                persisted == vec![b"before".to_vec()]
                    || persisted == vec![b"before".to_vec(), b"uncertain".to_vec()]
            );
        }
    }

    #[test]
    fn record_keys_preserve_numeric_order_across_byte_boundaries() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        for batch in 0..3u32 {
            store
                .with_batch(|sink| {
                    for index in 0..128 {
                        sink(&(batch * 128 + index).to_le_bytes())?;
                    }
                    Ok(())
                })
                .unwrap();
        }
        drop(store);
        let persisted = records(&Store::reopen(&cfg).unwrap()).unwrap();
        assert_eq!(persisted.len(), 384);
        for (index, bytes) in persisted.iter().enumerate() {
            assert_eq!(bytes, &(index as u32).to_le_bytes());
        }
    }

    #[test]
    fn facade_reopen_returns_error_for_cleanly_closed_corrupt_records_leaf() {
        use super::super::config::{Candidate, Store as FacadeStore};
        use crate::format::{
            CommitLsn, CompletedTx, Frame, SourceEndLsn, TxId, TxSeq, encode_frame,
        };

        let fixture = Fixture::new();
        let cfg = fixture.config();
        let frame = Frame {
            tx: TxId {
                stream: cfg.stream,
                xid: 1,
                begin_ordinal: 1,
            },
            event_ordinal: 0,
            part: 0,
            last_part: true,
            payload: vec![0x5a; 32],
        };
        let completed = CompletedTx {
            tx: frame.tx,
            order: TxSeq(1),
            commit: Some(CommitLsn(10)),
            end: SourceEndLsn(11),
            event_count: 1,
            encoded_bytes: 248,
        };
        let mut store = FacadeStore::create(Candidate::Redb, &cfg).unwrap();
        store
            .with_batch(|batch| {
                batch.append(&frame)?;
                batch.complete(&completed)
            })
            .unwrap();
        drop(store);
        drop(FacadeStore::reopen(Candidate::Redb, &cfg, TxSeq(1)).unwrap());

        let path = cfg.dir.join(DATABASE_FILE);
        let mut bytes = fs::read(&path).unwrap();
        let packed = encode_frame(&frame, cfg.limits).unwrap();
        let mut matches = bytes
            .windows(packed.len())
            .enumerate()
            .filter(|(_, window)| *window == packed);
        let record_offset = matches
            .next()
            .expect("fixture must contain the stored frame")
            .0;
        assert!(
            matches.next().is_none(),
            "fixture must identify one records leaf"
        );
        let leaf = record_offset / 4096 * 4096;
        // redb 4.5 stores the first variable value's end at leaf bytes 4..8.
        assert_eq!(bytes[leaf], 1);
        assert_eq!(&bytes[leaf + 2..leaf + 8], &[2, 0, 148, 0, 0, 0]);
        bytes[leaf + 7] ^= 0x80;
        fs::write(&path, bytes).unwrap();

        assert!(matches!(
            FacadeStore::reopen(Candidate::Redb, &cfg, TxSeq(1)),
            Err(Error::Backend(_))
        ));
    }
}
