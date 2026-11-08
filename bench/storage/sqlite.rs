//! Opaque records keep the journal catalog independent of the frame codec.

use super::config::{
    CatalogHead, CatalogReadView, CompletionEntry, Config, Error, FaultObserver, FaultPoint,
    ReceiveSink, RecordNo, RecordSink, Result, Role,
};
use super::format::{self, TxSeq};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Statement, TransactionBehavior, params};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

const DATABASE: &str = "records.sqlite3";
const SCHEMA: &str =
    "CREATE TABLE records(ordinal INTEGER PRIMARY KEY, record BLOB NOT NULL) STRICT";
const HEAD_SCHEMA: &str = "CREATE TABLE catalog_head(singleton INTEGER PRIMARY KEY CHECK(singleton = 1), catalog_version INTEGER NOT NULL, receive_revision BLOB NOT NULL CHECK(length(receive_revision) = 8), apply_revision BLOB NOT NULL CHECK(length(apply_revision) = 8), next_record INTEGER NOT NULL CHECK(next_record > 0), published BLOB CHECK(length(published) = 128), consumer_cursor BLOB NOT NULL CHECK(length(consumer_cursor) = 8)) STRICT";
const COMPLETION_SCHEMA: &str = "CREATE TABLE completions(order_no BLOB PRIMARY KEY CHECK(length(order_no) = 8), tx_id BLOB NOT NULL UNIQUE CHECK(length(tx_id) = 48), descriptor BLOB NOT NULL CHECK(length(descriptor) = 128), first_record INTEGER NOT NULL CHECK(first_record > 0), descriptor_record INTEGER NOT NULL CHECK(descriptor_record >= first_record)) STRICT, WITHOUT ROWID";

pub(super) struct Store {
    connection: Connection,
    directory: File,
    cfg: Config,
    role: Role,
    poisoned: bool,
}

impl Store {
    pub(super) fn open_role(cfg: &Config, role: Role) -> Result<Self> {
        cfg.verify_directory()?;
        let store = Self::open(cfg, role)?;
        verify_schema(&store.connection)?;
        configure_reader(&store.connection, cfg)?;
        if role == Role::Status {
            let mode: String = store
                .connection
                .pragma_query_value(None, "journal_mode", |row| row.get(0))
                .map_err(sqlite_error)?;
            if mode != "wal" {
                return Err(Error::Backend("SQLite catalog is not in WAL mode"));
            }
        } else {
            configure_writer(&store.connection)?;
        }
        store.with_read_view(|view| {
            view.scan_records(RecordNo::FIRST, view.head().next_record, &mut |_| Ok(()))
        })?;
        Ok(store)
    }

    pub(super) fn with_receive(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut dyn ReceiveSink) -> Result<()>,
    ) -> Result<CatalogHead> {
        self.receive(expected_receive_revision, write, None)
    }

    pub(super) fn with_receive_observed(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut dyn ReceiveSink) -> Result<()>,
        observer: &FaultObserver<'_>,
    ) -> Result<CatalogHead> {
        self.receive(expected_receive_revision, write, Some(observer))
    }

    fn receive(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut dyn ReceiveSink) -> Result<()>,
        observer: Option<&FaultObserver<'_>>,
    ) -> Result<CatalogHead> {
        self.require_role(Role::Receive)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                let error = begin_error(error);
                if error != Error::Busy {
                    self.poisoned = true;
                }
                error
            })?;
        self.poisoned = true;
        let outcome = (|| {
            let head = load_head(&tx, &self.cfg)?;
            if head.receive_revision != expected_receive_revision {
                return Err(Error::Conflict);
            }
            let first_record = match head.published {
                Some(descriptor) => load_completion(&tx, &self.cfg, descriptor.order)?
                    .ok_or(Error::Gap)?
                    .descriptor_record
                    .next()?,
                None => RecordNo::FIRST,
            };
            let mut sink = ReceiveBatch {
                insert: tx.prepare("INSERT INTO records(ordinal, record) VALUES (?1, ?2)").map_err(sqlite_error)?,
                complete: tx.prepare("INSERT INTO completions(order_no, tx_id, descriptor, first_record, descriptor_record) VALUES (?1, ?2, ?3, ?4, ?5)").map_err(sqlite_error)?,
                cfg: &self.cfg, head, first_record, bytes: 0, failure: None,
            };
            let callback = write(&mut sink);
            if let Some(error) = sink.failure {
                return Err(after_callback(error));
            }
            callback.map_err(after_callback)?;
            sink.head
                .finish_receive(expected_receive_revision)
                .map_err(after_callback)?;
            sink.head.validate(&self.cfg)?;
            let published = sink
                .head
                .published
                .as_ref()
                .map(|descriptor| format::encode_completed(descriptor, self.cfg.limits))
                .transpose()?;
            if tx.execute("UPDATE catalog_head SET receive_revision = ?1, next_record = ?2, published = ?3 WHERE singleton = 1 AND receive_revision = ?4",
                params![sink.head.receive_revision.to_be_bytes().as_slice(), record_key(sink.head.next_record)?, published, expected_receive_revision.to_be_bytes().as_slice()],
            ).map_err(sqlite_error)? != 1 {
                return Err(Error::Poisoned);
            }
            Ok(sink.head)
        })();
        match outcome {
            Err(error) => {
                tx.rollback()
                    .map_err(|_| Error::Backend("SQLite rollback failed; reopen required"))?;
                if error == Error::Conflict {
                    self.poisoned = false;
                }
                Err(error)
            }
            Ok(head) => {
                if let Some(observer) = observer
                    && let Err(error) = observer(FaultPoint::BeforeCatalogCommit)
                {
                    tx.rollback()
                        .map_err(|_| Error::Backend("SQLite rollback failed; reopen required"))?;
                    return Err(after_callback(error));
                }
                tx.commit().map_err(|_| {
                    Error::Backend("SQLite COMMIT outcome is uncertain; reopen required")
                })?;
                if let Some(observer) = observer {
                    observer(FaultPoint::AfterCatalogCommit).map_err(after_callback)?;
                    observer(FaultPoint::BeforeCatalogDirectorySync).map_err(after_callback)?;
                }
                self.directory.sync_all()?;
                if let Some(observer) = observer {
                    observer(FaultPoint::AfterCatalogDirectorySync).map_err(after_callback)?;
                }
                self.poisoned = false;
                Ok(head)
            }
        }
    }

    pub(super) fn with_read_view(
        &self,
        read: impl FnOnce(&dyn CatalogReadView) -> Result<()>,
    ) -> Result<()> {
        self.healthy()?;
        let tx = self
            .connection
            .unchecked_transaction()
            .map_err(begin_error)?;
        let outcome = (|| {
            let head = load_head(&tx, &self.cfg)?;
            let view = ReadSnapshot {
                connection: &tx,
                cfg: &self.cfg,
                head,
            };
            if let Some(published) = head.published {
                let entry = view.completion(published.order)?.ok_or(Error::Gap)?;
                if entry.descriptor != published {
                    return Err(Error::IdentityMismatch);
                }
            }
            read(&view)
        })();
        tx.rollback().map_err(sqlite_error)?;
        outcome
    }

    pub(super) fn advance_consumer(
        &mut self,
        expected_apply_revision: u64,
        next: TxSeq,
    ) -> Result<CatalogHead> {
        self.require_role(Role::Apply)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                let error = begin_error(error);
                if error != Error::Busy {
                    self.poisoned = true;
                }
                error
            })?;
        let mut head = load_head(&tx, &self.cfg)?;
        if let Err(error) = head.advance_consumer(expected_apply_revision, next) {
            tx.rollback().map_err(sqlite_error)?;
            return Err(error);
        }
        if next.0 != 0 && load_completion(&tx, &self.cfg, next)?.is_none() {
            tx.rollback().map_err(sqlite_error)?;
            return Err(Error::Gap);
        }
        self.poisoned = true;
        let updated = tx.execute("UPDATE catalog_head SET apply_revision = ?1, consumer_cursor = ?2 WHERE singleton = 1 AND apply_revision = ?3",
            params![head.apply_revision.to_be_bytes().as_slice(), next.0.to_be_bytes().as_slice(), expected_apply_revision.to_be_bytes().as_slice()],
        ).map_err(sqlite_error);
        if updated != Ok(1) {
            tx.rollback()
                .map_err(|_| Error::Backend("SQLite rollback failed; reopen required"))?;
            return Err(updated.err().unwrap_or(Error::Poisoned));
        }
        tx.commit()
            .map_err(|_| Error::Backend("SQLite COMMIT outcome is uncertain; reopen required"))?;
        self.directory.sync_all()?;
        self.poisoned = false;
        Ok(head)
    }

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
            "configured_cache_bytes": self.cfg.cache_bytes,
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
        let mut store = Self::open(cfg, Role::Receive)?;
        configure_reader(&store.connection, cfg)?;
        configure_writer(&store.connection)?;
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        for schema in [SCHEMA, HEAD_SCHEMA, COMPLETION_SCHEMA] {
            tx.execute_batch(schema).map_err(sqlite_error)?;
        }
        tx.execute(
            "INSERT INTO catalog_head VALUES (1, ?1, ?2, ?2, 1, NULL, ?2)",
            params![
                super::config::CATALOG_VERSION,
                0_u64.to_be_bytes().as_slice()
            ],
        )
        .map_err(sqlite_error)?;
        tx.commit()
            .map_err(|_| Error::Backend("SQLite COMMIT outcome is uncertain; reopen required"))?;
        store.directory.sync_all()?;
        Ok(store)
    }

    #[cfg(test)]
    pub(super) fn reopen(cfg: &Config) -> Result<Self> {
        Self::open_role(cfg, Role::Receive)
    }

    fn open(cfg: &Config, role: Role) -> Result<Self> {
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
            (if role == Role::Status {
                OpenFlags::SQLITE_OPEN_READ_ONLY
            } else {
                OpenFlags::SQLITE_OPEN_READ_WRITE
            }) | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )
        .map_err(sqlite_error)?;
        Ok(Self {
            connection,
            directory: File::open(&cfg.dir)?,
            cfg: cfg.clone(),
            role,
            poisoned: false,
        })
    }

    #[cfg(test)]
    pub(super) fn with_batch(
        &mut self,
        write: impl FnOnce(&mut RecordSink<'_>) -> Result<()>,
    ) -> Result<()> {
        self.require_role(Role::Receive)?;
        let mut revision = 0;
        self.with_read_view(|view| {
            revision = view.head().receive_revision;
            Ok(())
        })?;
        self.with_receive(revision, |sink| {
            write(&mut |bytes| sink.append(bytes).map(|_| ()))
        })
        .map(|_| ())
    }

    #[cfg(test)]
    pub(super) fn scan_records(&self, visit: &mut RecordSink<'_>) -> Result<()> {
        self.with_read_view(|view| {
            view.scan_records(RecordNo::FIRST, view.head().next_record, visit)
        })
    }

    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn require_role(&self, role: Role) -> Result<()> {
        self.healthy()?;
        if self.role != role {
            Err(Error::RoleViolation)
        } else {
            Ok(())
        }
    }

    pub(super) fn checkpoint(&mut self) -> Result<()> {
        self.healthy()?;
        if self.role == Role::Status {
            return Err(Error::RoleViolation);
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

struct ReceiveBatch<'a> {
    insert: Statement<'a>,
    complete: Statement<'a>,
    cfg: &'a Config,
    head: CatalogHead,
    first_record: RecordNo,
    bytes: u64,
    failure: Option<Error>,
}

impl ReceiveSink for ReceiveBatch<'_> {
    fn head(&self) -> &CatalogHead {
        &self.head
    }

    fn append(&mut self, record: &[u8]) -> Result<RecordNo> {
        if self.failure.is_some() {
            return Err(Error::Poisoned);
        }
        let result = (|| {
            if record.is_empty() || record.len() > self.cfg.limits.max_frame_bytes {
                return Err(Error::Backend("SQLite record exceeds its byte limit"));
            }
            let size = u64::try_from(record.len()).map_err(|_| Error::Overflow)?;
            let total = self.bytes.checked_add(size).ok_or(Error::Overflow)?;
            if total > self.cfg.batch_bytes {
                return Err(Error::Backend("SQLite batch exceeds its byte limit"));
            }
            let ordinal = self.head.next_record;
            let next = ordinal.next()?;
            if self
                .insert
                .execute(params![record_key(ordinal)?, record])
                .map_err(sqlite_error)?
                != 1
            {
                return Err(Error::Backend("SQLite insert did not store one record"));
            }
            self.head.next_record = next;
            self.bytes = total;
            Ok(ordinal)
        })();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }

    fn complete(&mut self, entry: CompletionEntry) -> Result<()> {
        if self.failure.is_some() {
            return Err(Error::Poisoned);
        }
        let result = (|| {
            entry.validate(self.cfg)?;
            if entry.first_record != self.first_record
                || entry.descriptor_record.next()? != self.head.next_record
            {
                return Err(Error::Gap);
            }
            match self.head.published {
                Some(previous) => format::validate_next(&previous, &entry.descriptor)?,
                None if entry.descriptor.order != TxSeq(1) => return Err(Error::Gap),
                None => (),
            }
            let descriptor = format::encode_completed(&entry.descriptor, self.cfg.limits)?;
            let result = self.complete.execute(params![
                entry.descriptor.order.0.to_be_bytes().as_slice(),
                tx_key(&entry.descriptor.tx).as_slice(),
                descriptor,
                record_key(entry.first_record)?,
                record_key(entry.descriptor_record)?
            ]);
            let changed = result.map_err(|error| {
                if matches!(&error, rusqlite::Error::SqliteFailure(code, _) if code.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE) {
                    Error::IdentityMismatch
                } else { sqlite_error(error) }
            })?;
            if changed != 1 {
                return Err(Error::Backend(
                    "SQLite completion insert did not store one entry",
                ));
            }
            self.head.published = Some(entry.descriptor);
            self.first_record = self.head.next_record;
            Ok(())
        })();
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
}

struct ReadSnapshot<'a> {
    connection: &'a Connection,
    cfg: &'a Config,
    head: CatalogHead,
}

impl CatalogReadView for ReadSnapshot<'_> {
    fn head(&self) -> &CatalogHead {
        &self.head
    }

    fn completion(&self, order: TxSeq) -> Result<Option<CompletionEntry>> {
        if order.0 == 0 || order > self.head.published.map_or(TxSeq(0), |tx| tx.order) {
            return Ok(None);
        }
        let entry = load_completion(self.connection, self.cfg, order)?;
        if let Some(entry) = entry
            && entry.descriptor_record >= self.head.next_record
        {
            return Err(Error::Gap);
        }
        Ok(entry)
    }

    fn scan_records(
        &self,
        first: RecordNo,
        end: RecordNo,
        visit: &mut RecordSink<'_>,
    ) -> Result<()> {
        if first > end || end > self.head.next_record {
            return Err(Error::Gap);
        }
        let mut select = self.connection.prepare(
            "SELECT ordinal, CASE WHEN length(record) BETWEEN 1 AND ?1 THEN record END FROM records WHERE ordinal >= ?2 AND ordinal < ?3 ORDER BY ordinal",
        ).map_err(sqlite_error)?;
        let mut rows = select
            .query(params![
                self.cfg.limits.max_frame_bytes as i64,
                record_key(first)?,
                record_key(end)?
            ])
            .map_err(sqlite_error)?;
        let mut expected = first;
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            if row.get::<_, i64>(0).map_err(sqlite_error)? != record_key(expected)? {
                return Err(Error::Gap);
            }
            let value = row.get_ref(1).map_err(sqlite_error)?;
            let record = value
                .as_blob()
                .map_err(|_| Error::Backend("SQLite record is not a bounded nonempty BLOB"))?;
            visit(record)?;
            expected = expected.next()?;
        }
        if expected != end {
            return Err(Error::Gap);
        }
        Ok(())
    }
}

fn record_key(record: RecordNo) -> Result<i64> {
    i64::try_from(record.get()).map_err(|_| Error::Overflow)
}

fn decode_record(value: i64) -> Result<RecordNo> {
    RecordNo::new(u64::try_from(value).map_err(|_| Error::Gap)?)
}

fn tx_key(tx: &format::TxId) -> [u8; 48] {
    let mut key = [0; 48];
    key[..16].copy_from_slice(&tx.stream.run.0);
    key[16..24].copy_from_slice(&tx.stream.source.system_identifier.to_be_bytes());
    key[24..28].copy_from_slice(&tx.stream.source.database_oid.to_be_bytes());
    key[28..36].copy_from_slice(&tx.stream.generation.to_be_bytes());
    key[36..40].copy_from_slice(&tx.xid.to_be_bytes());
    key[40..48].copy_from_slice(&tx.begin_ordinal.to_be_bytes());
    key
}

fn unsigned(row: &rusqlite::Row<'_>, column: usize) -> rusqlite::Result<u64> {
    let bytes = row
        .get_ref(column)?
        .as_blob()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
    ))
}

fn load_head(connection: &Connection, cfg: &Config) -> Result<CatalogHead> {
    let (version, receive, apply, next, present, published, cursor) = connection.query_row(
        "SELECT catalog_version, CASE WHEN length(receive_revision) = 8 THEN receive_revision END, CASE WHEN length(apply_revision) = 8 THEN apply_revision END, next_record, published IS NOT NULL, CASE WHEN length(published) = 128 THEN published END, CASE WHEN length(consumer_cursor) = 8 THEN consumer_cursor END FROM catalog_head WHERE singleton = 1",
        [], |row| Ok((row.get::<_, u32>(0)?, unsigned(row, 1)?, unsigned(row, 2)?, row.get::<_, i64>(3)?, row.get::<_, bool>(4)?, row.get::<_, Option<Vec<u8>>>(5)?, unsigned(row, 6)?)),
    ).optional().map_err(sqlite_error)?.ok_or(Error::Gap)?;
    if present != published.is_some() {
        return Err(Error::Gap);
    }
    let head = CatalogHead {
        catalog_version: version,
        receive_revision: receive,
        apply_revision: apply,
        next_record: decode_record(next)?,
        published: published
            .map(|bytes| format::decode_completed(&bytes, cfg.limits))
            .transpose()?,
        consumer_cursor: TxSeq(cursor),
    };
    head.validate(cfg)?;
    let (first, last): (Option<i64>, Option<i64>) = connection
        .query_row(
            "SELECT (SELECT min(ordinal) FROM records), (SELECT max(ordinal) FROM records)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sqlite_error)?;
    if first.is_some_and(|first| first != 1)
        || last.unwrap_or(0).checked_add(1) != Some(record_key(head.next_record)?)
    {
        return Err(Error::Gap);
    }
    Ok(head)
}

fn load_completion(
    connection: &Connection,
    cfg: &Config,
    order: TxSeq,
) -> Result<Option<CompletionEntry>> {
    let row = connection.query_row(
        "SELECT CASE WHEN length(descriptor) = 128 THEN descriptor END, first_record, descriptor_record, CASE WHEN length(tx_id) = 48 THEN tx_id END FROM completions WHERE order_no = ?1",
        [order.0.to_be_bytes().as_slice()], |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, Vec<u8>>(3)?)),
    ).optional().map_err(sqlite_error)?;
    let Some((bytes, first, last, identity)) = row else {
        return Ok(None);
    };
    let entry = CompletionEntry {
        descriptor: format::decode_completed(&bytes, cfg.limits)?,
        first_record: decode_record(first)?,
        descriptor_record: decode_record(last)?,
    };
    entry.validate(cfg)?;
    if entry.descriptor.order != order {
        return Err(Error::Gap);
    }
    if identity != tx_key(&entry.descriptor.tx) {
        return Err(Error::IdentityMismatch);
    }
    Ok(Some(entry))
}

fn verify_schema(connection: &Connection) -> Result<()> {
    let mut statement = connection
        .prepare("SELECT name, sql FROM sqlite_schema WHERE sql IS NOT NULL ORDER BY name")
        .map_err(sqlite_error)?;
    let mut rows = statement.query([]).map_err(sqlite_error)?;
    for (name, sql) in [
        ("catalog_head", HEAD_SCHEMA),
        ("completions", COMPLETION_SCHEMA),
        ("records", SCHEMA),
    ] {
        let row = rows
            .next()
            .map_err(sqlite_error)?
            .ok_or(Error::Backend("SQLite storage schema differs"))?;
        if row.get::<_, String>(0).map_err(sqlite_error)? != name
            || row.get::<_, String>(1).map_err(sqlite_error)? != sql
        {
            return Err(Error::Backend("SQLite storage schema differs"));
        }
    }
    if rows.next().map_err(sqlite_error)?.is_some() {
        return Err(Error::Backend("SQLite storage schema differs"));
    }
    Ok(())
}

fn begin_error(error: rusqlite::Error) -> Error {
    if matches!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    ) {
        Error::Busy
    } else {
        sqlite_error(error)
    }
}

fn after_callback(error: Error) -> Error {
    match error {
        Error::Busy | Error::Conflict => Error::Poisoned,
        _ => error,
    }
}

fn configure_reader(connection: &Connection, cfg: &Config) -> Result<()> {
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
    Ok(())
}

fn configure_writer(connection: &Connection) -> Result<()> {
    for (name, expected) in [
        ("synchronous", 2),
        ("fullfsync", 1),
        ("checkpoint_fullfsync", 1),
        ("wal_autocheckpoint", 0),
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
    use std::cell::RefCell;
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

    fn head(store: &Store) -> CatalogHead {
        let mut head = None;
        store
            .with_read_view(|view| {
                head = Some(*view.head());
                Ok(())
            })
            .unwrap();
        head.unwrap()
    }

    fn entry(cfg: &Config, order: u64, first: u64, last: u64) -> CompletionEntry {
        CompletionEntry {
            descriptor: format::CompletedTx {
                tx: format::TxId {
                    stream: cfg.stream,
                    xid: order as u32,
                    begin_ordinal: order,
                },
                order: TxSeq(order),
                commit: None,
                end: format::SourceEndLsn(order * 10),
                event_count: last - first,
                encoded_bytes: 128 + (last - first) * 89,
            },
            first_record: RecordNo::new(first).unwrap(),
            descriptor_record: RecordNo::new(last).unwrap(),
        }
    }

    fn publish(store: &mut Store, cfg: &Config, revision: u64, order: u64) -> CatalogHead {
        store
            .with_receive(revision, |sink| {
                let no = sink.append(b"opaque locator")?;
                sink.complete(entry(cfg, order, no.get(), no.get()))
            })
            .unwrap()
    }

    const CATALOG_TRACE: [FaultPoint; 4] = [
        FaultPoint::BeforeCatalogCommit,
        FaultPoint::AfterCatalogCommit,
        FaultPoint::BeforeCatalogDirectorySync,
        FaultPoint::AfterCatalogDirectorySync,
    ];

    fn c2_append(sink: &mut dyn ReceiveSink, cfg: &Config) -> Result<()> {
        let first = sink.append(b"event locator")?;
        let last = sink.append(b"descriptor locator")?;
        sink.complete(entry(cfg, 1, first.get(), last.get()))
    }

    fn fail_commit(store: &Store) {
        store.connection.execute_batch(
            "PRAGMA foreign_keys = ON;
             CREATE TABLE parent(id INTEGER PRIMARY KEY);
             CREATE TABLE child(parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
             CREATE TRIGGER fail_commit AFTER INSERT ON records
               BEGIN INSERT INTO child VALUES (999); END;"
        ).unwrap();
    }

    #[test]
    fn c2_catalog_observer_order_tracks_actual_commit_visibility() {
        let fixture = Fixture::new();
        let mut writer = Store::create(&fixture.cfg).unwrap();
        let reader = Store::open_role(&fixture.cfg, Role::Status).unwrap();
        let trace = RefCell::new(Vec::new());
        let received = writer
            .with_receive_observed(0, |sink| c2_append(sink, &fixture.cfg), &|point| {
                trace.borrow_mut().push(point);
                let revision = u64::from(point != FaultPoint::BeforeCatalogCommit);
                assert_eq!(head(&reader).receive_revision, revision);
                assert_eq!(records(&reader).unwrap().len(), revision as usize * 2);
                Ok(())
            })
            .unwrap();
        assert_eq!(*trace.borrow(), CATALOG_TRACE);
        assert_eq!(head(&reader), received);
        assert_eq!(received.next_record.get(), 3);
        writer.with_receive(1, |_| Ok(())).unwrap();
        assert_eq!(*trace.borrow(), CATALOG_TRACE);
    }

    #[test]
    fn c2_observer_errors_poison_and_preserve_the_actual_committed_prefix() {
        for (point, hits, revision) in [
            (FaultPoint::BeforeCatalogCommit, 1, 0),
            (FaultPoint::AfterCatalogCommit, 2, 1),
            (FaultPoint::BeforeCatalogDirectorySync, 3, 1),
            (FaultPoint::AfterCatalogDirectorySync, 4, 1),
        ] {
            let fixture = Fixture::new();
            let mut writer = Store::create(&fixture.cfg).unwrap();
            let trace = RefCell::new(Vec::new());
            let failure = Error::Backend("injected observer failure");
            assert_eq!(
                writer.with_receive_observed(
                    0,
                    |sink| c2_append(sink, &fixture.cfg),
                    &|observed| {
                        trace.borrow_mut().push(observed);
                        if observed == point {
                            Err(failure.clone())
                        } else {
                            Ok(())
                        }
                    }
                ),
                Err(failure)
            );
            assert_eq!(*trace.borrow(), CATALOG_TRACE[..hits]);
            assert_eq!(writer.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
            assert_eq!(writer.checkpoint(), Err(Error::Poisoned));
            drop(writer);
            let reopened = Store::reopen(&fixture.cfg).unwrap();
            assert_eq!(head(&reopened).receive_revision, revision);
            assert_eq!(records(&reopened).unwrap().len(), revision as usize * 2);
            reopened
                .with_read_view(|view| {
                    assert_eq!(view.completion(TxSeq(1))?.is_some(), revision == 1);
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn c2_observer_panics_poison_even_after_a_successful_commit() {
        for (point, hits, revision) in [
            (FaultPoint::BeforeCatalogCommit, 1, 0),
            (FaultPoint::AfterCatalogCommit, 2, 1),
            (FaultPoint::BeforeCatalogDirectorySync, 3, 1),
            (FaultPoint::AfterCatalogDirectorySync, 4, 1),
        ] {
            let fixture = Fixture::new();
            let mut writer = Store::create(&fixture.cfg).unwrap();
            let trace = RefCell::new(Vec::new());
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = writer.with_receive_observed(
                    0,
                    |sink| c2_append(sink, &fixture.cfg),
                    &|observed| {
                        trace.borrow_mut().push(observed);
                        assert_ne!(observed, point, "injected observer panic");
                        Ok(())
                    },
                );
            }));
            assert!(panic.is_err(), "observer must execute at {point:?}");
            assert_eq!(*trace.borrow(), CATALOG_TRACE[..hits]);
            assert_eq!(writer.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
            drop(writer);
            let reopened = Store::reopen(&fixture.cfg).unwrap();
            assert_eq!(head(&reopened).receive_revision, revision);
            assert_eq!(records(&reopened).unwrap().len(), revision as usize * 2);
        }
    }

    #[test]
    fn c2_observer_busy_or_conflict_cannot_advertise_safe_retry() {
        for failure in [Error::Busy, Error::Conflict] {
            for (point, revision) in [
                (FaultPoint::BeforeCatalogCommit, 0),
                (FaultPoint::AfterCatalogCommit, 1),
            ] {
                let fixture = Fixture::new();
                let mut writer = Store::create(&fixture.cfg).unwrap();
                assert_eq!(
                    writer.with_receive_observed(
                        0,
                        |sink| c2_append(sink, &fixture.cfg),
                        &|observed| {
                            if observed == point {
                                Err(failure.clone())
                            } else {
                                Ok(())
                            }
                        }
                    ),
                    Err(Error::Poisoned)
                );
                assert_eq!(writer.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
                drop(writer);
                assert_eq!(
                    head(&Store::reopen(&fixture.cfg).unwrap()).receive_revision,
                    revision
                );
            }
        }
    }

    #[test]
    fn c2_precallback_busy_and_conflict_do_not_invoke_the_observer() {
        let fixture = Fixture::new();
        let mut writer = Store::create(&fixture.cfg).unwrap();
        let blocker = Connection::open(fixture.cfg.dir.join(DATABASE)).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let must_not_observe = |point| panic!("pre-callback refusal reached {point:?}");
        assert_eq!(
            writer.with_receive_observed(0, |_| panic!("busy callback"), &must_not_observe),
            Err(Error::Busy)
        );
        blocker.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            writer.with_receive_observed(1, |_| panic!("stale callback"), &must_not_observe),
            Err(Error::Conflict)
        );
        let trace = RefCell::new(Vec::new());
        writer
            .with_receive_observed(0, |sink| c2_append(sink, &fixture.cfg), &|point| {
                trace.borrow_mut().push(point);
                Ok(())
            })
            .unwrap();
        assert_eq!(*trace.borrow(), CATALOG_TRACE);
    }

    #[test]
    fn c2_failed_callback_does_not_reach_catalog_hooks() {
        let fixture = Fixture::new();
        let mut writer = Store::create(&fixture.cfg).unwrap();
        let trace = RefCell::new(Vec::new());
        let failure = Error::Backend("injected callback failure");
        assert_eq!(
            writer.with_receive_observed(
                0,
                |sink| {
                    c2_append(sink, &fixture.cfg)?;
                    Err(failure.clone())
                },
                &|point| {
                    trace.borrow_mut().push(point);
                    Ok(())
                }
            ),
            Err(failure)
        );
        assert!(trace.borrow().is_empty());
        assert_eq!(writer.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
        drop(writer);
        assert_eq!(
            head(&Store::reopen(&fixture.cfg).unwrap()),
            CatalogHead::empty()
        );
    }

    #[test]
    fn c2_failed_commit_has_no_success_or_directory_hooks() {
        let fixture = Fixture::new();
        let mut writer = Store::create(&fixture.cfg).unwrap();
        fail_commit(&writer);
        let trace = RefCell::new(Vec::new());
        let mut insert_succeeded = false;
        let result = writer.with_receive_observed(
            0,
            |sink| {
                c2_append(sink, &fixture.cfg)?;
                insert_succeeded = true;
                Ok(())
            },
            &|point| {
                trace.borrow_mut().push(point);
                Ok(())
            },
        );
        assert!(matches!(result, Err(Error::Backend(_))));
        assert!(insert_succeeded, "fault must execute at COMMIT");
        assert_eq!(*trace.borrow(), [FaultPoint::BeforeCatalogCommit]);
        assert_eq!(writer.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
        let count: i64 = writer
            .connection
            .query_row("SELECT count(*) FROM records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn c2_failed_directory_sync_has_no_after_hook_and_keeps_committed_records() {
        let fixture = Fixture::new();
        let mut writer = Store::create(&fixture.cfg).unwrap();
        let (socket, _peer) = std::os::unix::net::UnixStream::pair().unwrap();
        let fd: std::os::fd::OwnedFd = socket.into();
        writer.directory = fd.into();
        let trace = RefCell::new(Vec::new());
        let result =
            writer.with_receive_observed(0, |sink| c2_append(sink, &fixture.cfg), &|point| {
                trace.borrow_mut().push(point);
                Ok(())
            });
        assert!(matches!(result, Err(Error::Io(_))));
        assert_eq!(*trace.borrow(), CATALOG_TRACE[..3]);
        assert_eq!(writer.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
        drop(writer);
        let reopened = Store::reopen(&fixture.cfg).unwrap();
        assert_eq!(head(&reopened).receive_revision, 1);
        assert_eq!(records(&reopened).unwrap().len(), 2);
    }

    #[test]
    fn c2_receipt_settings_query_preserves_effective_values_and_status_role() {
        let fixture = Fixture::new();
        let mut writer = Store::create(&fixture.cfg).unwrap();
        writer.connection.execute_batch("PRAGMA synchronous = NORMAL; PRAGMA fullfsync = OFF; PRAGMA checkpoint_fullfsync = OFF; PRAGMA wal_autocheckpoint = 7;").unwrap();
        let before = writer.settings().unwrap();
        writer
            .with_receive_observed(0, |sink| c2_append(sink, &fixture.cfg), &|_| Ok(()))
            .unwrap();
        assert_eq!(writer.settings().unwrap(), before);
        assert_eq!(before["effective_pragmas"]["journal_mode"], "wal");
        assert_eq!(before["effective_pragmas"]["synchronous"], 1);
        assert_eq!(before["effective_pragmas"]["fullfsync"], 0);
        assert_eq!(before["effective_pragmas"]["checkpoint_fullfsync"], 0);
        assert_eq!(before["effective_pragmas"]["wal_autocheckpoint"], 7);
        let mut status = Store::open_role(&fixture.cfg, Role::Status).unwrap();
        let before = status.settings().unwrap();
        assert_eq!(before["effective_pragmas"]["fullfsync"], 0);
        assert_eq!(before["effective_pragmas"]["checkpoint_fullfsync"], 0);
        assert_eq!(status.checkpoint(), Err(Error::RoleViolation));
        assert_eq!(status.settings().unwrap(), before);
    }

    #[test]
    fn c1_record_allocation_uses_the_persisted_head_across_handles() {
        let fixture = Fixture::new();
        let mut first = Store::create(&fixture.cfg).unwrap();
        let mut peer = Store::reopen(&fixture.cfg).unwrap();
        assert_eq!(head(&first), CatalogHead::empty());
        let one = publish(&mut first, &fixture.cfg, 0, 1);
        assert_eq!(one.receive_revision, 1);
        assert_eq!(one.next_record.get(), 2);
        let two = publish(&mut peer, &fixture.cfg, 1, 2);
        assert_eq!(two.receive_revision, 2);
        assert_eq!(two.next_record.get(), 3);
        drop(first);
        drop(peer);
        let reopened = Store::reopen(&fixture.cfg).unwrap();
        assert_eq!(head(&reopened), two);
        reopened
            .with_read_view(|view| {
                assert_eq!(
                    view.completion(TxSeq(1))?,
                    Some(entry(&fixture.cfg, 1, 1, 1))
                );
                assert_eq!(
                    view.completion(TxSeq(2))?,
                    Some(entry(&fixture.cfg, 2, 2, 2))
                );
                assert_eq!(view.completion(TxSeq(3))?, None);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn c1_precallback_busy_and_conflict_do_not_poison_or_invoke_callbacks() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        let blocker = Connection::open(fixture.cfg.dir.join(DATABASE)).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut invoked = false;
        assert_eq!(
            store.with_receive(0, |_| {
                invoked = true;
                Ok(())
            }),
            Err(Error::Busy)
        );
        assert!(!invoked);
        blocker.execute_batch("ROLLBACK").unwrap();
        publish(&mut store, &fixture.cfg, 0, 1);
        assert_eq!(
            store.with_receive(0, |_| {
                invoked = true;
                Ok(())
            }),
            Err(Error::Conflict)
        );
        assert!(!invoked);
        assert_eq!(publish(&mut store, &fixture.cfg, 1, 2).receive_revision, 2);
    }

    #[test]
    fn c1_failed_begin_only_preserves_the_handle_for_known_busy() {
        for role in [Role::Receive, Role::Apply] {
            let fixture = Fixture::new();
            let created = Store::create(&fixture.cfg).unwrap();
            drop(created);
            let mut store = Store::open_role(&fixture.cfg, role).unwrap();
            let corrupt = fixture.root.join("corrupt.sqlite3");
            fs::write(&corrupt, [0xff; 4096]).unwrap();
            store.connection = Connection::open(&corrupt).unwrap();
            let outcome = if role == Role::Receive {
                store.with_receive(0, |_| panic!("failed BEGIN must not call the callback"))
            } else {
                store.advance_consumer(0, TxSeq(0))
            };
            assert!(matches!(outcome, Err(Error::Backend(_))));
            store.connection = Connection::open(fixture.cfg.dir.join(DATABASE)).unwrap();
            assert_eq!(store.with_read_view(|_| Ok(())), Err(Error::Poisoned));
        }
    }

    #[test]
    fn c1_apply_cas_preserves_the_latest_receive_fields() {
        let fixture = Fixture::new();
        let mut receiver = Store::create(&fixture.cfg).unwrap();
        let mut apply = Store::open_role(&fixture.cfg, Role::Apply).unwrap();
        publish(&mut receiver, &fixture.cfg, 0, 1);
        let applied = apply.advance_consumer(0, TxSeq(1)).unwrap();
        assert_eq!(applied.apply_revision, 1);
        assert_eq!(applied.consumer_cursor, TxSeq(1));
        let received = publish(&mut receiver, &fixture.cfg, 1, 2);
        assert_eq!(received.apply_revision, 1);
        assert_eq!(received.consumer_cursor, TxSeq(1));
        assert_eq!(apply.advance_consumer(0, TxSeq(2)), Err(Error::Conflict));
        let advanced = apply.advance_consumer(1, TxSeq(2)).unwrap();
        assert_eq!(advanced.apply_revision, 2);
        assert_eq!(advanced.receive_revision, 2);
        assert_eq!(advanced.next_record, received.next_record);
        assert_eq!(advanced.published, received.published);
        assert_eq!(advanced.consumer_cursor, TxSeq(2));
        assert_eq!(head(&receiver), advanced);
    }

    #[test]
    fn c1_completion_index_and_head_rollback_with_records() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        let failure = Error::Backend("injected callback failure");
        assert_eq!(
            store.with_receive(0, |sink| {
                let no = sink.append(b"discarded")?;
                sink.complete(entry(&fixture.cfg, 1, no.get(), no.get()))?;
                Err(failure.clone())
            }),
            Err(failure)
        );
        assert_eq!(store.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
        drop(store);
        let reopened = Store::reopen(&fixture.cfg).unwrap();
        assert_eq!(head(&reopened), CatalogHead::empty());
        assert!(records(&reopened).unwrap().is_empty());
        reopened
            .with_read_view(|view| {
                assert_eq!(view.completion(TxSeq(1))?, None);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn c1_duplicate_transaction_identity_cannot_be_hidden_by_the_callback() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        publish(&mut store, &fixture.cfg, 0, 1);
        publish(&mut store, &fixture.cfg, 1, 2);
        let mut duplicate = entry(&fixture.cfg, 3, 3, 3);
        duplicate.descriptor.tx = entry(&fixture.cfg, 1, 1, 1).descriptor.tx;
        assert!(
            store
                .with_receive(2, |sink| {
                    sink.append(b"discarded")?;
                    assert!(sink.complete(duplicate).is_err());
                    Ok(())
                })
                .is_err()
        );
        drop(store);
        let reopened = Store::reopen(&fixture.cfg).unwrap();
        assert_eq!(head(&reopened).receive_revision, 2);
        assert_eq!(head(&reopened).next_record.get(), 3);
        assert_eq!(records(&reopened).unwrap().len(), 2);
    }

    #[test]
    fn c1_callback_busy_is_poisoned_instead_of_advertising_safe_retry() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        assert_eq!(
            store.with_receive(0, |sink| {
                sink.append(b"discarded")?;
                Err(Error::Busy)
            }),
            Err(Error::Poisoned)
        );
        assert_eq!(store.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
        drop(store);
        assert_eq!(
            head(&Store::reopen(&fixture.cfg).unwrap()),
            CatalogHead::empty()
        );
    }

    #[test]
    fn c1_full_width_revisions_and_transaction_identity_survive_reopen() {
        let mut fixture = Fixture::new();
        fixture.cfg.stream.source.system_identifier = u64::MAX;
        fixture.cfg.stream.generation = u64::MAX;
        let mut receiver = Store::create(&fixture.cfg).unwrap();
        receiver
            .connection
            .execute(
                "UPDATE catalog_head SET receive_revision = ?1, apply_revision = ?1",
                [(u64::MAX - 1).to_be_bytes().as_slice()],
            )
            .unwrap();
        let mut completion = entry(&fixture.cfg, 1, 1, 1);
        completion.descriptor.tx.xid = u32::MAX;
        completion.descriptor.tx.begin_ordinal = u64::MAX;
        completion.descriptor.end = format::SourceEndLsn(u64::MAX);
        let received = receiver
            .with_receive(u64::MAX - 1, |sink| {
                assert_eq!(sink.append(b"locator")?, RecordNo::FIRST);
                sink.complete(completion)
            })
            .unwrap();
        assert_eq!(received.receive_revision, u64::MAX);
        let mut apply = Store::open_role(&fixture.cfg, Role::Apply).unwrap();
        assert_eq!(
            apply
                .advance_consumer(u64::MAX - 1, TxSeq(1))
                .unwrap()
                .apply_revision,
            u64::MAX
        );
        assert_eq!(
            apply.advance_consumer(u64::MAX, TxSeq(1)),
            Err(Error::Overflow)
        );
        let committed = head(&apply);
        assert_eq!(committed.published, Some(completion.descriptor));
        assert_eq!(committed.receive_revision, u64::MAX);
        assert_eq!(committed.apply_revision, u64::MAX);
        assert_eq!(
            receiver.with_receive(u64::MAX, |sink| {
                sink.append(b"discarded")?;
                Ok(())
            }),
            Err(Error::Overflow)
        );
        drop(receiver);
        drop(apply);
        let reopened = Store::reopen(&fixture.cfg).unwrap();
        assert_eq!(head(&reopened), committed);
        assert_eq!(records(&reopened).unwrap(), vec![b"locator".to_vec()]);
    }

    #[test]
    fn c1_completion_ranges_must_start_after_the_prior_descriptor_and_end_at_latest_append() {
        for (first, last) in [(1, 1), (2, 2)] {
            let fixture = Fixture::new();
            let mut store = Store::create(&fixture.cfg).unwrap();
            assert_eq!(
                store.with_receive(0, |sink| {
                    sink.append(b"first")?;
                    sink.append(b"second")?;
                    assert_eq!(
                        sink.complete(entry(&fixture.cfg, 1, first, last)),
                        Err(Error::Gap)
                    );
                    Ok(())
                }),
                Err(Error::Gap)
            );
            drop(store);
            assert_eq!(
                head(&Store::reopen(&fixture.cfg).unwrap()),
                CatalogHead::empty()
            );
        }
    }

    #[test]
    fn c1_unwinding_callback_aborts_publication_and_poisons_the_handle() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = store.with_receive(0, |sink| {
                sink.append(b"discarded")?;
                sink.complete(entry(&fixture.cfg, 1, 1, 1))?;
                panic!("injected callback panic");
            });
        }));
        assert!(panic.is_err());
        assert_eq!(store.with_receive(0, |_| Ok(())), Err(Error::Poisoned));
        drop(store);
        assert_eq!(
            head(&Store::reopen(&fixture.cfg).unwrap()),
            CatalogHead::empty()
        );
    }

    #[test]
    fn c1_status_is_read_only_and_does_not_configure_a_writer() {
        let fixture = Fixture::new();
        let mut receiver = Store::create(&fixture.cfg).unwrap();
        publish(&mut receiver, &fixture.cfg, 0, 1);
        let before = head(&receiver);
        let mut status = Store::open_role(&fixture.cfg, Role::Status).unwrap();
        assert_eq!(head(&status), before);
        let fullfsync: i64 = status
            .connection
            .pragma_query_value(None, "fullfsync", |row| row.get(0))
            .unwrap();
        assert_eq!(fullfsync, 0);
        assert!(
            status
                .connection
                .execute("DELETE FROM records", [])
                .is_err()
        );
        assert_eq!(
            status.with_receive(1, |_| Ok(())),
            Err(Error::RoleViolation)
        );
        assert_eq!(
            status.advance_consumer(0, TxSeq(1)),
            Err(Error::RoleViolation)
        );
        assert_eq!(status.checkpoint(), Err(Error::RoleViolation));
        assert_eq!(head(&status), before);
        assert_eq!(head(&receiver), before);
    }

    #[test]
    fn c1_read_view_keeps_one_cut_while_a_child_process_commits() {
        const CHILD: &str = "RPGMIGRATE_SQLITE_C1_CHILD";
        if let Some(dir) = std::env::var_os(CHILD) {
            let mut fixture = Fixture::new();
            fixture.cfg.dir = dir.into();
            let mut store = Store::open_role(&fixture.cfg, Role::Receive).unwrap();
            publish(&mut store, &fixture.cfg, 1, 2);
            return;
        }
        let fixture = Fixture::new();
        let mut receiver = Store::create(&fixture.cfg).unwrap();
        publish(&mut receiver, &fixture.cfg, 0, 1);
        let status = Store::open_role(&fixture.cfg, Role::Status).unwrap();
        status
            .with_read_view(|view| {
                let cut = *view.head();
                let mut before = Vec::new();
                view.scan_records(RecordNo::FIRST, cut.next_record, &mut |b| {
                    before.push(b.to_vec());
                    Ok(())
                })?;
                let output = std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        std::thread::current().name().unwrap(),
                        "--nocapture",
                    ])
                    .env(CHILD, &fixture.cfg.dir)
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "child failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                assert_eq!(head(&receiver).receive_revision, 2);
                assert_eq!(*view.head(), cut);
                assert_eq!(view.completion(TxSeq(2))?, None);
                let mut after = Vec::new();
                view.scan_records(RecordNo::FIRST, cut.next_record, &mut |b| {
                    after.push(b.to_vec());
                    Ok(())
                })?;
                assert_eq!(before, after);
                assert_eq!(before, vec![b"opaque locator".to_vec()]);
                assert_eq!(
                    view.scan_records(RecordNo::FIRST, cut.next_record.next()?, &mut |_| Ok(())),
                    Err(Error::Gap)
                );
                Ok(())
            })
            .unwrap();
        assert_eq!(head(&status).receive_revision, 2);
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
        let before = fs::read_dir(&fixture.cfg.dir).unwrap().count();
        assert!(Store::reopen(&fixture.cfg).is_err());
        assert_eq!(fs::read_dir(&fixture.cfg.dir).unwrap().count(), before);
    }

    #[test]
    fn c1_nonpositive_persisted_ordinals_are_not_hidden_by_range_scans() {
        let fixture = Fixture::new();
        let mut store = Store::create(&fixture.cfg).unwrap();
        store.with_batch(|sink| sink(b"valid")).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO records VALUES (0, ?1)",
                [b"invalid".as_slice()],
            )
            .unwrap();
        assert_eq!(records(&store), Err(Error::Gap));
        drop(store);
        assert!(matches!(Store::reopen(&fixture.cfg), Err(Error::Gap)));
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
        let files = fs::read_dir(&fixture.cfg.dir).unwrap().count();
        assert!(Store::reopen(&fixture.cfg).is_err());
        assert!(
            fs::read(&path).unwrap() == before,
            "reopen changed the foreign database"
        );
        assert_eq!(fs::read_dir(&fixture.cfg.dir).unwrap().count(), files);
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
        fail_commit(&store);
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
