//! Opaque records keep the journal catalog independent of the frame codec.

use super::config::{Config, Error, RecordSink, Result};
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

const DATABASE: &str = "records.sqlite3";
const SCHEMA: &str =
    "CREATE TABLE records(ordinal INTEGER PRIMARY KEY, record BLOB NOT NULL) STRICT";

pub(super) struct Store {
    connection: Connection,
    directory: File,
    record_bytes: usize,
    batch_bytes: u64,
    cache_bytes: usize,
    next_ordinal: i64,
    poisoned: bool,
}

impl Store {
    pub(super) fn settings(&self) -> Result<serde_json::Value> {
        let pragma = |name| {
            self.connection
                .pragma_query_value(None, name, |row| row.get::<_, i64>(0))
                .map_err(sqlite_error)
        };
        let journal_mode: String = self
            .connection
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(sqlite_error)?;
        let cache_size = pragma("cache_size")?;
        let page_size = pragma("page_size")?;
        let cache_budget = u64::try_from(cache_size)
            .map_err(|_| Error::Overflow)?
            .checked_mul(u64::try_from(page_size).map_err(|_| Error::Overflow)?)
            .ok_or(Error::Overflow)?;
        let (version, source_id): (String, String) = self
            .connection
            .query_row("SELECT sqlite_version(), sqlite_source_id()", [], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(sqlite_error)?;
        Ok(serde_json::json!({
            "sqlite_version": version,
            "sqlite_source_id": source_id,
            "configured_cache_bytes": self.cache_bytes,
            "effective_pragmas": {
                "journal_mode": journal_mode,
                "synchronous": pragma("synchronous")?,
                "fullfsync": pragma("fullfsync")?,
                "checkpoint_fullfsync": pragma("checkpoint_fullfsync")?,
                "cache_size": cache_size,
                "page_size": page_size,
                "mmap_size": pragma("mmap_size")?,
                "wal_autocheckpoint": pragma("wal_autocheckpoint")?
            },
            "effective_cache_budget_bytes": cache_budget,
            "cache_budget_is_rss_limit": false
        }))
    }

    pub(super) fn create(cfg: &Config) -> Result<Self> {
        cfg.create_directory()?;
        // Close this descriptor before SQLite opens the file and acquires POSIX locks.
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(cfg.dir.join(DATABASE))?
            .sync_all()?;
        File::open(&cfg.dir)?.sync_all()?;
        let store = Self::open(cfg)?;
        configure(&store.connection, cfg)?;
        store
            .connection
            .execute_batch(SCHEMA)
            .map_err(sqlite_error)?;
        store.directory.sync_all()?;
        Ok(store)
    }

    pub(super) fn reopen(cfg: &Config) -> Result<Self> {
        cfg.verify_directory()?;
        let mut store = Self::open(cfg)?;
        let valid: bool = store.connection.query_row(
            "SELECT count(*) = 1 AND coalesce(max(type = 'table' AND name = 'records' AND sql = ?1), 0)
             FROM sqlite_schema",
            [SCHEMA],
            |row| row.get(0),
        ).map_err(sqlite_error)?;
        if !valid {
            return Err(Error::Backend("SQLite storage schema differs"));
        }
        configure(&store.connection, cfg)?;
        store.next_ordinal = store.scan(&mut |_| Ok(()))?;
        Ok(store)
    }

    fn open(cfg: &Config) -> Result<Self> {
        if rusqlite::version_number() != 3_054_001 {
            return Err(Error::Backend(
                "SQLite experiment requires bundled SQLite 3.54.1",
            ));
        }
        // macOS temporary directories have a symlinked /var ancestor.
        let path = cfg.dir.canonicalize()?.join(DATABASE);
        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            return Err(Error::IdentityMismatch);
        }
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(sqlite_error)?;
        Ok(Self {
            connection,
            directory: File::open(&cfg.dir)?,
            record_bytes: cfg.limits.max_frame_bytes,
            batch_bytes: cfg.batch_bytes,
            cache_bytes: cfg.cache_bytes,
            next_ordinal: 1,
            poisoned: false,
        })
    }

    pub(super) fn with_batch(
        &mut self,
        write: impl FnOnce(&mut RecordSink<'_>) -> Result<()>,
    ) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.poisoned = true;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let mut ordinal = self.next_ordinal;
        let outcome = (|| {
            let mut insert = tx
                .prepare("INSERT INTO records(ordinal, record) VALUES (?1, ?2)")
                .map_err(sqlite_error)?;
            let mut bytes = 0_u64;
            let mut failure = None;
            let callback = write(&mut |record| {
                if failure.is_some() {
                    return Err(Error::Poisoned);
                }
                let result = (|| {
                    if record.is_empty() || record.len() > self.record_bytes {
                        return Err(Error::Backend("SQLite record exceeds its byte limit"));
                    }
                    let size = u64::try_from(record.len()).map_err(|_| Error::Overflow)?;
                    let total = bytes.checked_add(size).ok_or(Error::Overflow)?;
                    if total > self.batch_bytes {
                        return Err(Error::Backend("SQLite batch exceeds its byte limit"));
                    }
                    let next = ordinal.checked_add(1).ok_or(Error::Overflow)?;
                    if insert
                        .execute(params![ordinal, record])
                        .map_err(sqlite_error)?
                        != 1
                    {
                        return Err(Error::Backend("SQLite insert did not store one record"));
                    }
                    bytes = total;
                    ordinal = next;
                    Ok(())
                })();
                if let Err(error) = &result {
                    failure = Some(error.clone());
                }
                result
            });
            // A caller cannot turn a failed write into a successful partial batch.
            failure.map_or(callback, Err)
        })();
        if let Err(error) = outcome {
            tx.rollback()
                .map_err(|_| Error::Backend("SQLite rollback failed; reopen required"))?;
            return Err(error);
        }
        tx.commit()
            .map_err(|_| Error::Backend("SQLite COMMIT outcome is uncertain; reopen required"))?;
        // A reopened connection can recreate the WAL directory entry on its first write.
        self.directory.sync_all()?;
        self.next_ordinal = ordinal;
        self.poisoned = false;
        Ok(())
    }

    pub(super) fn scan_records(&self, visit: &mut RecordSink<'_>) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.scan(visit).map(|_| ())
    }

    fn scan(&self, visit: &mut RecordSink<'_>) -> Result<i64> {
        // length(BLOB) avoids materializing an oversized corrupt record for the visitor.
        let mut select = self
            .connection
            .prepare(
                "SELECT ordinal, CASE WHEN length(record) BETWEEN 1 AND ?1 THEN record END
             FROM records ORDER BY ordinal",
            )
            .map_err(sqlite_error)?;
        let limit = i64::try_from(self.record_bytes).map_err(|_| Error::Overflow)?;
        let mut rows = select.query([limit]).map_err(sqlite_error)?;
        let mut expected = 1_i64;
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            let ordinal: i64 = row.get(0).map_err(sqlite_error)?;
            if ordinal != expected {
                return Err(Error::Gap);
            }
            let value = row.get_ref(1).map_err(sqlite_error)?;
            let record = value
                .as_blob()
                .map_err(|_| Error::Backend("SQLite record is not a bounded nonempty BLOB"))?;
            visit(record)?;
            expected = expected.checked_add(1).ok_or(Error::Overflow)?;
        }
        Ok(expected)
    }

    pub(super) fn checkpoint(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        self.poisoned = true;
        let result: (i64, i64, i64) = self
            .connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .map_err(sqlite_error)?;
        if result != (0, 0, 0) {
            return Err(Error::Backend(
                "SQLite WAL checkpoint did not finish truncating",
            ));
        }
        self.directory.sync_all()?;
        self.poisoned = false;
        Ok(())
    }
}

fn configure(connection: &Connection, cfg: &Config) -> Result<()> {
    let page_bytes: i64 = connection
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .map_err(sqlite_error)?;
    let page_bytes = usize::try_from(page_bytes).map_err(|_| Error::Overflow)?;
    let pages = cfg
        .cache_bytes
        .checked_div(page_bytes)
        .ok_or(Error::Overflow)?;
    let pages = i32::try_from(pages).map_err(|_| Error::Overflow)?;
    if pages == 0 {
        return Err(Error::Config(
            "SQLite cache must hold at least one database page",
        ));
    }
    // Checkpoint cost and errors belong to the explicit final drain, not an implicit hook.
    for (name, expected) in [
        ("synchronous", 2),
        ("fullfsync", 1),
        ("checkpoint_fullfsync", 1),
        ("wal_autocheckpoint", 0),
        ("cache_size", i64::from(pages)),
        ("mmap_size", 0),
        ("busy_timeout", 0),
        ("trusted_schema", 0),
    ] {
        connection
            .pragma_update(None, name, expected)
            .map_err(sqlite_error)?;
        let effective: i64 = connection
            .pragma_query_value(None, name, |row| row.get(0))
            .map_err(sqlite_error)?;
        if effective != expected {
            return Err(Error::Backend("SQLite connection setting differs"));
        }
    }
    let mode: String = connection
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
        .map_err(sqlite_error)?;
    if mode != "wal" {
        return Err(Error::Backend("SQLite WAL mode was not enabled"));
    }
    Ok(())
}

fn sqlite_error(_: rusqlite::Error) -> Error {
    Error::Backend("SQLite storage operation failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Limits, RunId, SourceId, StreamId};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Fixture {
        root: PathBuf,
        cfg: Config,
    }

    impl Fixture {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "rpgmigrate-sqlite-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&root).unwrap();
            Self {
                cfg: Config {
                    dir: root.join("store"),
                    stream: StreamId {
                        run: RunId([0x31; 16]),
                        source: SourceId {
                            system_identifier: 123,
                            database_oid: 42,
                        },
                        generation: 7,
                    },
                    limits: Limits {
                        max_frame_bytes: 1024,
                        max_event_bytes: 936,
                        max_parts: 4,
                    },
                    batch_bytes: 2048,
                    cache_bytes: 64 * 1024,
                    segment_bytes: 4096,
                },
                root,
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    fn records(store: &Store) -> Result<Vec<Vec<u8>>> {
        let mut records = Vec::new();
        store.scan_records(&mut |bytes| {
            records.push(bytes.to_vec());
            Ok(())
        })?;
        Ok(records)
    }

    #[test]
    fn settings_report_live_sqlite_values_and_page_rounded_cache_budget() {
        let mut fixture = Fixture::new();
        fixture.cfg.cache_bytes = 65_537;
        for open in [Store::create, Store::reopen] {
            let store = open(&fixture.cfg).unwrap();
            assert_eq!(
                store.settings().unwrap(),
                serde_json::json!({
                    "sqlite_version": "3.54.1",
                    "sqlite_source_id": "2026-10-16 14:45:20 555e07ef33f69a0957d24d8e555b702464a131d6f1607f4c3c1e4551e8e6ca76",
                    "configured_cache_bytes": 65_537,
                    "effective_pragmas": {
                        "journal_mode": "wal",
                        "synchronous": 2,
                        "fullfsync": 1,
                        "checkpoint_fullfsync": 1,
                        "cache_size": 16,
                        "page_size": 4096,
                        "mmap_size": 0,
                        "wal_autocheckpoint": 0
                    },
                    "effective_cache_budget_bytes": 65_536,
                    "cache_budget_is_rss_limit": false
                })
            );
        }
    }

    #[test]
    fn settings_query_the_current_connection_instead_of_repeating_configuration() {
        let fixture = Fixture::new();
        let store = Store::create(&fixture.cfg).unwrap();
        store
            .connection
            .execute_batch(
                "PRAGMA journal_mode = DELETE;
             PRAGMA page_size = 8192;
             VACUUM;
             PRAGMA synchronous = NORMAL;
             PRAGMA fullfsync = OFF;
             PRAGMA checkpoint_fullfsync = OFF;
             PRAGMA cache_size = 7;
             PRAGMA mmap_size = 8192;
             PRAGMA wal_autocheckpoint = 9;",
            )
            .unwrap();
        let settings = store.settings().unwrap();
        assert_eq!(
            settings["effective_pragmas"],
            serde_json::json!({
                "journal_mode": "delete",
                "synchronous": 1,
                "fullfsync": 0,
                "checkpoint_fullfsync": 0,
                "cache_size": 7,
                "page_size": 8192,
                "mmap_size": 8192,
                "wal_autocheckpoint": 9
            })
        );
        assert_eq!(settings["configured_cache_bytes"], 65_536);
        assert_eq!(settings["effective_cache_budget_bytes"], 57_344);
    }

    #[test]
    fn settings_propagate_query_errors_without_private_paths() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        let corrupt = fixture.root.join("private-corrupt.sqlite3");
        fs::write(&corrupt, [0xff; 4096]).unwrap();
        store.connection = Connection::open(corrupt).unwrap();
        assert_eq!(
            store.settings(),
            Err(Error::Backend("SQLite storage operation failed"))
        );
    }

    #[test]
    fn small_opaque_records_reopen_in_append_order_across_batches() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        store
            .with_batch(|sink| {
                sink(b"locator")?;
                sink(&[0, 255, 0, 128])
            })
            .unwrap();
        store.with_batch(|sink| sink(b"third")).unwrap();
        store.checkpoint().unwrap();
        drop(store);

        let mut reopened = Store::reopen(&fixture.cfg).unwrap();
        reopened.with_batch(|sink| sink(b"fourth")).unwrap();
        assert_eq!(
            records(&reopened).unwrap(),
            vec![
                b"locator".to_vec(),
                vec![0, 255, 0, 128],
                b"third".to_vec(),
                b"fourth".to_vec(),
            ]
        );
    }

    #[test]
    fn callback_failure_rolls_back_every_record_in_the_batch() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        store.with_batch(|sink| sink(b"retained")).unwrap();
        let failure = Error::Backend("injected callback failure");
        assert_eq!(
            store.with_batch(|sink| {
                sink(b"discarded")?;
                Err(failure.clone())
            }),
            Err(failure)
        );
        drop(store);
        let reopened = Store::reopen(&fixture.cfg).unwrap();
        assert_eq!(records(&reopened).unwrap(), vec![b"retained".to_vec()]);
    }

    #[test]
    fn suppressed_record_limit_error_still_rolls_back_the_batch() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        assert!(
            store
                .with_batch(|sink| {
                    sink(b"discarded")?;
                    assert!(sink(&vec![0; 1025]).is_err());
                    let _ = sink(b"also discarded");
                    Ok(())
                })
                .is_err()
        );
        drop(store);
        assert!(
            records(&Store::reopen(&fixture.cfg).unwrap())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn batch_byte_limit_is_enforced_without_publishing_a_prefix() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        assert!(
            store
                .with_batch(|sink| {
                    sink(&[1; 1024])?;
                    sink(&[2; 1024])?;
                    let _ = sink(&[3]);
                    Ok(())
                })
                .is_err()
        );
        drop(store);
        assert!(
            records(&Store::reopen(&fixture.cfg).unwrap())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn foreign_identity_refuses_reopen_before_database_side_effects() {
        let fixture = Fixture::new();
        let store = Store::create(&fixture.cfg).unwrap();
        drop(store);
        let before: Vec<_> = fs::read_dir(&fixture.cfg.dir)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect();
        let mut foreign = fixture.cfg.clone();
        foreign.stream.generation += 1;
        assert!(matches!(
            Store::reopen(&foreign),
            Err(Error::IdentityMismatch)
        ));
        for (path, bytes) in &before {
            assert_eq!(&fs::read(path).unwrap(), bytes);
        }
        assert_eq!(
            fs::read_dir(&fixture.cfg.dir).unwrap().count(),
            before.len()
        );
    }

    #[test]
    fn reopen_missing_does_not_create_a_directory() {
        let fixture = Fixture::new();
        assert!(Store::reopen(&fixture.cfg).is_err());
        assert!(!fixture.cfg.dir.exists());
    }

    #[test]
    fn create_refuses_to_adopt_an_existing_store() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        store.with_batch(|sink| sink(b"original")).unwrap();
        assert!(Store::create(&fixture.cfg).is_err());
        assert_eq!(records(&store).unwrap(), vec![b"original".to_vec()]);
    }

    #[test]
    fn missing_database_is_not_recreated_after_identity_validation() {
        let fixture = Fixture::new();
        fixture.cfg.create_directory().unwrap();
        assert!(Store::reopen(&fixture.cfg).is_err());
        assert_eq!(fs::read_dir(&fixture.cfg.dir).unwrap().count(), 1);
    }

    #[test]
    fn record_gap_refuses_scan_and_reopen() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        store
            .with_batch(|sink| {
                sink(b"one")?;
                sink(b"two")?;
                sink(b"three")
            })
            .unwrap();
        let corrupt = rusqlite::Connection::open(fixture.cfg.dir.join("records.sqlite3")).unwrap();
        assert_eq!(
            corrupt
                .execute("DELETE FROM records WHERE ordinal = 2", [])
                .unwrap(),
            1
        );
        drop(corrupt);
        assert_eq!(records(&store), Err(Error::Gap));
        drop(store);
        assert!(matches!(Store::reopen(&fixture.cfg), Err(Error::Gap)));
    }

    #[test]
    fn suppressed_sqlite_write_error_does_not_commit_the_earlier_insert() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        let inject = rusqlite::Connection::open(fixture.cfg.dir.join("records.sqlite3")).unwrap();
        inject
            .execute_batch(
                "CREATE TRIGGER injected_failure BEFORE INSERT ON records
            WHEN NEW.ordinal = 2 BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
            )
            .unwrap();
        assert!(
            store
                .with_batch(|sink| {
                    sink(b"one")?;
                    assert!(sink(b"two").is_err());
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(
            inject
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn empty_record_failure_poisons_the_batch() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        assert!(
            store
                .with_batch(|sink| {
                    sink(b"one")?;
                    let _ = sink(b"");
                    Ok(())
                })
                .is_err()
        );
        drop(store);
        assert!(
            records(&Store::reopen(&fixture.cfg).unwrap())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn oversized_stored_blob_is_refused_before_visiting_it() {
        let fixture = Fixture::new();
        let store = Store::create(&fixture.cfg).unwrap();
        let corrupt = rusqlite::Connection::open(fixture.cfg.dir.join("records.sqlite3")).unwrap();
        corrupt
            .execute("INSERT INTO records VALUES (1, zeroblob(1025))", [])
            .unwrap();
        let mut visited = 0;
        assert!(
            store
                .scan_records(&mut |_| {
                    visited += 1;
                    Ok(())
                })
                .is_err()
        );
        assert_eq!(visited, 0);
        drop(corrupt);
        drop(store);
        assert!(Store::reopen(&fixture.cfg).is_err());
    }

    #[test]
    fn busy_checkpoint_is_an_error_instead_of_a_successful_drain() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        store.with_batch(|sink| sink(b"one")).unwrap();
        let reader = rusqlite::Connection::open(fixture.cfg.dir.join("records.sqlite3")).unwrap();
        reader.execute_batch("BEGIN;").unwrap();
        assert_eq!(
            reader
                .query_row("SELECT count(*) FROM records", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        store.with_batch(|sink| sink(b"two")).unwrap();
        assert!(store.checkpoint().is_err());
        reader.execute_batch("ROLLBACK;").unwrap();
        drop(reader);
        drop(store);
        let mut reopened = Store::reopen(&fixture.cfg).unwrap();
        reopened.checkpoint().unwrap();
        assert_eq!(
            fs::metadata(fixture.cfg.dir.join("records.sqlite3-wal"))
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            records(&reopened).unwrap(),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
    }

    #[test]
    fn missing_schema_is_not_repaired_on_reopen() {
        let fixture = Fixture::new();
        let store = Store::create(&fixture.cfg).unwrap();
        drop(store);
        let corrupt = rusqlite::Connection::open(fixture.cfg.dir.join("records.sqlite3")).unwrap();
        corrupt.execute("DROP TABLE records", []).unwrap();
        assert!(Store::reopen(&fixture.cfg).is_err());
        assert_eq!(
            corrupt
                .query_row(
                    "SELECT count(*) FROM sqlite_schema WHERE name='records'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
    }

    #[test]
    fn foreign_schema_is_not_changed_to_wal_on_reopen() {
        let fixture = Fixture::new();
        fixture.cfg.create_directory().unwrap();
        let path = fixture.cfg.dir.join(DATABASE);
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        let foreign = Connection::open(&path).unwrap();
        foreign
            .execute_batch("CREATE TABLE foreign_data(value TEXT);")
            .unwrap();
        drop(foreign);
        let before = fs::read(&path).unwrap();
        assert!(Store::reopen(&fixture.cfg).is_err());
        assert!(
            fs::read(&path).unwrap() == before,
            "reopen changed the foreign database"
        );
        assert_eq!(fs::read_dir(&fixture.cfg.dir).unwrap().count(), 2);
    }

    #[test]
    fn required_durability_and_cache_settings_hold_on_every_connection() {
        let fixture = Fixture::new();
        for open in [Store::create, Store::reopen] {
            let store = open(&fixture.cfg).unwrap();
            let mode: String = store
                .connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .unwrap();
            assert_eq!(mode, "wal");
            for (pragma, expected) in [
                ("synchronous", 2),
                ("fullfsync", 1),
                ("checkpoint_fullfsync", 1),
                ("wal_autocheckpoint", 0),
                ("mmap_size", 0),
            ] {
                let value: i64 = store
                    .connection
                    .pragma_query_value(None, pragma, |row| row.get(0))
                    .unwrap();
                assert_eq!(value, expected, "{pragma}");
            }
            let pages: i64 = store
                .connection
                .pragma_query_value(None, "cache_size", |row| row.get(0))
                .unwrap();
            let page_bytes: i64 = store
                .connection
                .pragma_query_value(None, "page_size", |row| row.get(0))
                .unwrap();
            assert!(pages > 0);
            assert!(usize::try_from(pages * page_bytes).unwrap() <= fixture.cfg.cache_bytes);
        }
        assert_eq!(rusqlite::version(), "3.54.1");
    }

    #[test]
    fn scan_callback_error_stops_visiting_without_changing_records() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        store
            .with_batch(|sink| {
                sink(b"one")?;
                sink(b"two")
            })
            .unwrap();
        let failure = Error::Backend("injected reader failure");
        let mut visited = 0;
        assert_eq!(
            store.scan_records(&mut |_| {
                visited += 1;
                Err(failure.clone())
            }),
            Err(failure)
        );
        assert_eq!(visited, 1);
        assert_eq!(
            records(&store).unwrap(),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
    }

    #[test]
    fn commit_error_requires_reopen_and_never_reports_batch_success() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        store.connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE parent(id INTEGER PRIMARY KEY);
             CREATE TABLE child(parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER fail_commit AFTER INSERT ON records
               BEGIN INSERT INTO child VALUES (999); END;"
        ).unwrap();
        let mut insert_succeeded = false;
        assert!(
            store
                .with_batch(|sink| {
                    sink(b"discarded")?;
                    insert_succeeded = true;
                    Ok(())
                })
                .is_err()
        );
        assert!(insert_succeeded, "fault must occur at COMMIT, not INSERT");
        assert_eq!(
            store.with_batch(|sink| sink(b"refused")),
            Err(Error::Poisoned)
        );
        assert_eq!(store.checkpoint(), Err(Error::Poisoned));
        assert_eq!(records(&store), Err(Error::Poisoned));
        let count: i64 = store
            .connection
            .query_row("SELECT count(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
        store
            .connection
            .execute_batch("DROP TRIGGER fail_commit; DROP TABLE child; DROP TABLE parent;")
            .unwrap();
        drop(store);
        let mut reopened = Store::reopen(&fixture.cfg).unwrap();
        reopened.with_batch(|sink| sink(b"retained")).unwrap();
        assert_eq!(records(&reopened).unwrap(), vec![b"retained".to_vec()]);
    }
}
