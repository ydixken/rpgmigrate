//! Raw-record storage for the isolated persistence experiment.
//! MultiWriter includes two-phase synchronization and allocator-state persistence per commit.
//! Writable reopen includes a full integrity scan, allocator rebuild, and redb's repair commit.
//! redb 4.5 has no bounded writable open; the C1 driver bounds child lifetime instead.
//! Status uses read-only open without repair or upstream integrity checking.
//! This experiment does not qualify production overlap or pruning.

use super::config::{
    CatalogHead, CatalogReadView, CompletionEntry, Config, Error, ReceiveSink, RecordNo,
    RecordSink, Result, Role,
};
use super::format::{self, TxId, TxSeq};
use ::redb::backends::FileBackend;
use ::redb::{
    Builder, ConcurrencyMode, Database, Durability, ReadOnlyDatabase, ReadOnlyTable,
    ReadTransaction, ReadableDatabase, ReadableTable, Table, TableDefinition, TableHandle,
    WriteTransaction,
};
use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

const RECORDS: TableDefinition<u64, &[u8]> = TableDefinition::new("records");
const METADATA: TableDefinition<u8, u64> = TableDefinition::new("metadata");
const RECORD_COUNT: u8 = 0;
const VERSION: u8 = 1;
const RECEIVE_REVISION: u8 = 2;
const APPLY_REVISION: u8 = 3;
const PUBLISHED: u8 = 4;
const CONSUMER_CURSOR: u8 = 5;
const COMPLETIONS: TableDefinition<u64, &[u8]> = TableDefinition::new("completions");
const TX_IDS: TableDefinition<&[u8; 48], u64> = TableDefinition::new("tx_ids");
const ENTRY_BYTES: usize = 16 + format::COMPLETED_BYTES;
const DATABASE_FILE: &str = "records.redb";

pub struct Store {
    db: Handle,
    cfg: Config,
    role: Role,
    poisoned: bool,
}

enum Handle {
    Writable(Database),
    ReadOnly(ReadOnlyDatabase),
}

impl Handle {
    fn begin_read(&self) -> Result<ReadTransaction> {
        match self {
            Self::Writable(db) => db.begin_read(),
            Self::ReadOnly(db) => db.begin_read(),
        }
        .map_err(|_| Error::Backend("redb read transaction failed"))
    }

    fn writable(&self) -> Result<&Database> {
        match self {
            Self::Writable(db) => Ok(db),
            Self::ReadOnly(_) => Err(Error::RoleViolation),
        }
    }
}

impl Store {
    pub fn open_role(cfg: &Config, role: Role) -> Result<Self> {
        cfg.verify_directory()?;
        let path = cfg.dir.join(DATABASE_FILE);
        let file = fs::symlink_metadata(&path)?;
        if !file.is_file() || file.nlink() != 1 || file.permissions().mode() & 0o777 != 0o600 {
            return Err(Error::IdentityMismatch);
        }
        let db = if role == Role::Status {
            Handle::ReadOnly(builder(cfg).open_read_only(path).map_err(open_error)?)
        } else {
            Handle::Writable(builder(cfg).open(path).map_err(open_error)?)
        };
        let store = Self {
            db,
            cfg: cfg.clone(),
            role,
            poisoned: false,
        };
        store.with_read_view(|_| Ok(()))?;
        Ok(store)
    }

    pub fn with_receive(
        &mut self,
        expected_receive_revision: u64,
        write: impl FnOnce(&mut dyn ReceiveSink) -> Result<()>,
    ) -> Result<CatalogHead> {
        self.require_role(Role::Receive)?;
        let transaction = self.try_write()?;
        let result = (|| {
            require_tables(&transaction)?;
            let mut metadata = transaction.open_table(METADATA).map_err(table_error)?;
            let records = transaction.open_table(RECORDS).map_err(table_error)?;
            let completions = transaction.open_table(COMPLETIONS).map_err(table_error)?;
            let tx_ids = transaction.open_table(TX_IDS).map_err(table_error)?;
            let head = load_head(&metadata, &records, &completions, &tx_ids, &self.cfg)?;
            if head.receive_revision != expected_receive_revision {
                return Err(Error::Conflict);
            }
            // Set before callback entry so unwinding also requires reopen; redb aborts on Drop.
            self.poisoned = true;
            let mut sink = ReceiveBatch {
                cfg: &self.cfg,
                head,
                records,
                completions,
                tx_ids,
                written: 0,
                failure: None,
            };
            let callback = write(&mut sink);
            if let Some(error) = sink.failure {
                return Err(error);
            }
            callback?;
            sink.head.finish_receive(expected_receive_revision)?;
            save_head(&mut metadata, &sink.head)?;
            Ok(sink.head)
        })();
        self.finish_write(transaction, result)
    }

    pub fn with_read_view(
        &self,
        read: impl FnOnce(&dyn CatalogReadView) -> Result<()>,
    ) -> Result<()> {
        self.healthy()?;
        // These table handles and both scan passes share one captured MVCC transaction.
        let transaction = self.db.begin_read()?;
        let metadata = transaction.open_table(METADATA).map_err(table_error)?;
        let records = transaction.open_table(RECORDS).map_err(table_error)?;
        let completions = transaction.open_table(COMPLETIONS).map_err(table_error)?;
        let tx_ids = transaction.open_table(TX_IDS).map_err(table_error)?;
        let head = load_head(&metadata, &records, &completions, &tx_ids, &self.cfg)?;
        read(&ReadView {
            cfg: &self.cfg,
            head,
            records,
            completions,
            tx_ids,
        })
    }

    pub fn advance_consumer(
        &mut self,
        expected_apply_revision: u64,
        next: TxSeq,
    ) -> Result<CatalogHead> {
        self.require_role(Role::Apply)?;
        let transaction = self.try_write()?;
        let result = (|| {
            require_tables(&transaction)?;
            let mut metadata = transaction.open_table(METADATA).map_err(table_error)?;
            let records = transaction.open_table(RECORDS).map_err(table_error)?;
            let completions = transaction.open_table(COMPLETIONS).map_err(table_error)?;
            let tx_ids = transaction.open_table(TX_IDS).map_err(table_error)?;
            let mut head = load_head(&metadata, &records, &completions, &tx_ids, &self.cfg)?;
            head.advance_consumer(expected_apply_revision, next)?;
            if next.0 != 0 {
                indexed_completion(&completions, &tx_ids, next, &self.cfg)?.ok_or(Error::Gap)?;
            }
            self.poisoned = true;
            save_head(&mut metadata, &head)?;
            Ok(head)
        })();
        self.finish_write(transaction, result)
    }

    pub fn settings(&self) -> Result<serde_json::Value> {
        // redb exposes no getters for these settings, so report the configured policy.
        Ok(serde_json::json!({
            "configured_policy": {
                "concurrency_mode": "MultiWriter",
                "durability": "Immediate",
                "check_integrity_on_open": self.role != Role::Status,
                "read_only": self.role == Role::Status,
                "cache_bytes": self.cfg.cache_bytes
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
            .try_begin_write()
            .map_err(|_| Error::Backend("redb write transaction failed"))?;
        transaction
            .set_durability(Durability::Immediate)
            .map_err(|_| Error::Backend("redb immediate durability is unavailable"))?;
        {
            let _records = transaction.open_table(RECORDS).map_err(table_error)?;
            let _completions = transaction.open_table(COMPLETIONS).map_err(table_error)?;
            let _tx_ids = transaction.open_table(TX_IDS).map_err(table_error)?;
            let mut metadata = transaction.open_table(METADATA).map_err(table_error)?;
            save_head(&mut metadata, &CatalogHead::empty())?;
        }
        transaction
            .commit()
            .map_err(|_| Error::Backend("redb initial commit failed; reopen required"))?;
        // File synchronization does not persist the newly created directory entry.
        File::open(&cfg.dir)?.sync_all()?;
        Ok(Self::new(db, cfg))
    }

    pub fn reopen(cfg: &Config) -> Result<Self> {
        Self::open_role(cfg, Role::Receive)
    }

    fn new(db: Database, cfg: &Config) -> Self {
        Self {
            db: Handle::Writable(db),
            cfg: cfg.clone(),
            role: Role::Receive,
            poisoned: false,
        }
    }

    pub fn with_batch(
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
        })?;
        Ok(())
    }

    pub fn scan_records(&self, visit: &mut RecordSink<'_>) -> Result<()> {
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

    fn try_write(&mut self) -> Result<WriteTransaction> {
        let result = self.db.writable()?.try_begin_write();
        let mut transaction = match result {
            Ok(transaction) => transaction,
            Err(::redb::TransactionError::WouldBlock) => return Err(Error::Busy),
            Err(_) => {
                self.poisoned = true;
                return Err(Error::Backend(
                    "redb write transaction failed; reopen required",
                ));
            }
        };
        if transaction.set_durability(Durability::Immediate).is_err() {
            self.poisoned = true;
            return Err(Error::Backend("redb immediate durability is unavailable"));
        }
        Ok(transaction)
    }

    fn finish_write(
        &mut self,
        transaction: WriteTransaction,
        result: Result<CatalogHead>,
    ) -> Result<CatalogHead> {
        let head = match result {
            Ok(head) => head,
            Err(error) => {
                if error != Error::Conflict {
                    self.poisoned = true;
                }
                if transaction.abort().is_err() {
                    self.poisoned = true;
                    return Err(Error::Backend("redb abort failed; reopen required"));
                }
                return Err(error);
            }
        };
        transaction
            .commit()
            .map_err(|_| Error::Backend("redb commit outcome is uncertain; reopen required"))?;
        self.poisoned = false;
        Ok(head)
    }

    /// Immediate commits already synchronize data and metadata; redb has no separate WAL to drain.
    /// This checks prior failures without adding an empty transaction or a fake durability barrier.
    pub fn checkpoint(&mut self) -> Result<()> {
        self.healthy()?;
        if self.role == Role::Status {
            Err(Error::RoleViolation)
        } else {
            Ok(())
        }
    }
}

struct ReceiveBatch<'a> {
    cfg: &'a Config,
    head: CatalogHead,
    records: Table<'a, u64, &'static [u8]>,
    completions: Table<'a, u64, &'static [u8]>,
    tx_ids: Table<'a, &'static [u8; 48], u64>,
    written: u64,
    failure: Option<Error>,
}

impl ReceiveBatch<'_> {
    fn latch<T>(&mut self, result: Result<T>) -> Result<T> {
        if let Err(error) = &result {
            self.failure = Some(error.clone());
        }
        result
    }
}

impl ReceiveSink for ReceiveBatch<'_> {
    fn head(&self) -> &CatalogHead {
        &self.head
    }

    fn append(&mut self, bytes: &[u8]) -> Result<RecordNo> {
        if self.failure.is_some() {
            return Err(Error::Poisoned);
        }
        let result = (|| {
            check_record(bytes, self.cfg.limits.max_frame_bytes)?;
            let total = self
                .written
                .checked_add(bytes.len() as u64)
                .ok_or(Error::Overflow)?;
            if total > self.cfg.batch_bytes {
                return Err(Error::Backend(
                    "redb batch exceeds the configured byte limit",
                ));
            }
            let record = self.head.next_record;
            let next = record.next()?;
            if self
                .records
                .insert(record.get(), bytes)
                .map_err(|_| Error::Backend("redb record write failed"))?
                .is_some()
            {
                return Err(Error::Gap);
            }
            self.written = total;
            self.head.next_record = next;
            Ok(record)
        })();
        self.latch(result)
    }

    fn complete(&mut self, entry: CompletionEntry) -> Result<()> {
        if self.failure.is_some() {
            return Err(Error::Poisoned);
        }
        let result = (|| {
            entry.validate(self.cfg)?;
            let order = self.head.published.map_or(0, |last| last.order.0);
            if entry.descriptor.order.0 != order.checked_add(1).ok_or(Error::Overflow)?
                || entry.descriptor_record.next()? != self.head.next_record
                || self.written == 0
            {
                return Err(Error::Gap);
            }
            let first = if order == 0 {
                RecordNo::FIRST
            } else {
                indexed_completion(&self.completions, &self.tx_ids, TxSeq(order), self.cfg)?
                    .ok_or(Error::Gap)?
                    .descriptor_record
                    .next()?
            };
            if entry.first_record != first {
                return Err(Error::Gap);
            }
            let key = tx_key(entry.descriptor.tx);
            if self
                .tx_ids
                .get(&key)
                .map_err(|_| Error::Backend("redb transaction identity read failed"))?
                .is_some()
            {
                return Err(Error::IdentityMismatch);
            }
            let encoded = encode_entry(entry, self.cfg)?;
            if self
                .completions
                .insert(entry.descriptor.order.0, encoded.as_slice())
                .map_err(|_| Error::Backend("redb completion write failed"))?
                .is_some()
            {
                return Err(Error::Gap);
            }
            self.tx_ids
                .insert(&key, entry.descriptor.order.0)
                .map_err(|_| Error::Backend("redb transaction identity write failed"))?;
            self.head.published = Some(entry.descriptor);
            Ok(())
        })();
        self.latch(result)
    }
}

struct ReadView<'a> {
    cfg: &'a Config,
    head: CatalogHead,
    records: ReadOnlyTable<u64, &'static [u8]>,
    completions: ReadOnlyTable<u64, &'static [u8]>,
    tx_ids: ReadOnlyTable<&'static [u8; 48], u64>,
}

impl CatalogReadView for ReadView<'_> {
    fn head(&self) -> &CatalogHead {
        &self.head
    }

    fn completion(&self, order: TxSeq) -> Result<Option<CompletionEntry>> {
        if order.0 == 0 {
            return Err(Error::Gap);
        }
        if order > self.head.published.map_or(TxSeq(0), |last| last.order) {
            return Ok(None);
        }
        let entry = indexed_completion(&self.completions, &self.tx_ids, order, self.cfg)?
            .ok_or(Error::Gap)?;
        if entry.descriptor_record >= self.head.next_record {
            return Err(Error::Gap);
        }
        Ok(Some(entry))
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
        let mut expected = first;
        for entry in self
            .records
            .range(first.get()..end.get())
            .map_err(|_| Error::Backend("redb record scan failed"))?
        {
            let (key, bytes) = entry.map_err(|_| Error::Backend("redb record read failed"))?;
            if key.value() != expected.get() {
                return Err(Error::Gap);
            }
            check_record(bytes.value(), self.cfg.limits.max_frame_bytes)?;
            visit(bytes.value())?;
            expected = expected.next()?;
        }
        if expected != end {
            return Err(Error::Gap);
        }
        Ok(())
    }
}

fn open_error(error: ::redb::DatabaseError) -> Error {
    match error {
        ::redb::DatabaseError::DatabaseAlreadyOpen => Error::Busy,
        _ => Error::Backend("redb database open failed"),
    }
}

fn require_tables(transaction: &WriteTransaction) -> Result<()> {
    // WriteTransaction::open_table creates absent tables; a missing catalog is a gap.
    let mut present = 0;
    for table in transaction
        .list_tables()
        .map_err(|_| Error::Backend("redb table inventory failed"))?
    {
        for (index, name) in [
            RECORDS.name(),
            METADATA.name(),
            COMPLETIONS.name(),
            TX_IDS.name(),
        ]
        .into_iter()
        .enumerate()
        {
            if table.name() == name {
                present |= 1 << index;
            }
        }
    }
    if present != 15 {
        return Err(Error::Gap);
    }
    Ok(())
}

fn load_head(
    metadata: &impl ReadableTable<u8, u64>,
    records: &impl ReadableTable<u64, &'static [u8]>,
    completions: &impl ReadableTable<u64, &'static [u8]>,
    tx_ids: &impl ReadableTable<&'static [u8; 48], u64>,
    cfg: &Config,
) -> Result<CatalogHead> {
    let field = |key| -> Result<u64> {
        Ok(metadata
            .get(key)
            .map_err(|_| Error::Backend("redb metadata read failed"))?
            .ok_or(Error::Gap)?
            .value())
    };
    let next_record = RecordNo::new(
        stored_count(metadata, records)?
            .checked_add(1)
            .ok_or(Error::Overflow)?,
    )?;
    let order = field(PUBLISHED)?;
    if completions
        .len()
        .map_err(|_| Error::Backend("redb completion count failed"))?
        != order
        || tx_ids
            .len()
            .map_err(|_| Error::Backend("redb identity count failed"))?
            != order
    {
        return Err(Error::Gap);
    }
    let published = if order == 0 {
        None
    } else {
        if completions
            .first()
            .map_err(|_| Error::Backend("redb first completion failed"))?
            .ok_or(Error::Gap)?
            .0
            .value()
            != 1
            || completions
                .last()
                .map_err(|_| Error::Backend("redb last completion failed"))?
                .ok_or(Error::Gap)?
                .0
                .value()
                != order
        {
            return Err(Error::Gap);
        }
        let last = indexed_completion(completions, tx_ids, TxSeq(order), cfg)?.ok_or(Error::Gap)?;
        if last.descriptor_record >= next_record {
            return Err(Error::Gap);
        }
        Some(last.descriptor)
    };
    let head = CatalogHead {
        catalog_version: u32::try_from(field(VERSION)?).map_err(|_| Error::Overflow)?,
        receive_revision: field(RECEIVE_REVISION)?,
        apply_revision: field(APPLY_REVISION)?,
        next_record,
        published,
        consumer_cursor: TxSeq(field(CONSUMER_CURSOR)?),
    };
    head.validate(cfg)?;
    Ok(head)
}

fn save_head(metadata: &mut Table<'_, u8, u64>, head: &CatalogHead) -> Result<()> {
    for (key, value) in [
        (RECORD_COUNT, head.next_record.get() - 1),
        (VERSION, u64::from(head.catalog_version)),
        (RECEIVE_REVISION, head.receive_revision),
        (APPLY_REVISION, head.apply_revision),
        (PUBLISHED, head.published.map_or(0, |last| last.order.0)),
        (CONSUMER_CURSOR, head.consumer_cursor.0),
    ] {
        metadata
            .insert(key, value)
            .map_err(|_| Error::Backend("redb metadata write failed"))?;
    }
    Ok(())
}

fn encode_entry(entry: CompletionEntry, cfg: &Config) -> Result<[u8; ENTRY_BYTES]> {
    let mut bytes = [0; ENTRY_BYTES];
    bytes[..8].copy_from_slice(&entry.first_record.get().to_le_bytes());
    bytes[8..16].copy_from_slice(&entry.descriptor_record.get().to_le_bytes());
    bytes[16..].copy_from_slice(&format::encode_completed(&entry.descriptor, cfg.limits)?);
    Ok(bytes)
}

fn indexed_completion(
    completions: &impl ReadableTable<u64, &'static [u8]>,
    tx_ids: &impl ReadableTable<&'static [u8; 48], u64>,
    order: TxSeq,
    cfg: &Config,
) -> Result<Option<CompletionEntry>> {
    let Some(bytes) = completions
        .get(order.0)
        .map_err(|_| Error::Backend("redb completion read failed"))?
    else {
        return Ok(None);
    };
    let bytes: &[u8; ENTRY_BYTES] = bytes.value().try_into().map_err(|_| Error::Gap)?;
    let entry = CompletionEntry {
        first_record: RecordNo::new(u64::from_le_bytes(bytes[..8].try_into().unwrap()))?,
        descriptor_record: RecordNo::new(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))?,
        descriptor: format::decode_completed(&bytes[16..], cfg.limits)?,
    };
    entry.validate(cfg)?;
    if entry.descriptor.order != order {
        return Err(Error::Gap);
    }
    if tx_ids
        .get(&tx_key(entry.descriptor.tx))
        .map_err(|_| Error::Backend("redb transaction identity read failed"))?
        .ok_or(Error::Gap)?
        .value()
        != order.0
    {
        return Err(Error::IdentityMismatch);
    }
    Ok(Some(entry))
}

fn tx_key(tx: TxId) -> [u8; 48] {
    let mut key = [0; 48];
    key[..16].copy_from_slice(&tx.stream.run.0);
    key[16..24].copy_from_slice(&tx.stream.source.system_identifier.to_le_bytes());
    key[24..28].copy_from_slice(&tx.stream.source.database_oid.to_le_bytes());
    key[28..36].copy_from_slice(&tx.stream.generation.to_le_bytes());
    key[36..40].copy_from_slice(&tx.xid.to_le_bytes());
    key[40..].copy_from_slice(&tx.begin_ordinal.to_le_bytes());
    key
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
    use ::redb::ReadableTableMetadata;
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

    fn completion(cfg: &Config, order: u64, first: u64, last: u64) -> CompletionEntry {
        CompletionEntry {
            descriptor: format::CompletedTx {
                tx: format::TxId {
                    stream: cfg.stream,
                    xid: order as u32,
                    begin_ordinal: order,
                },
                order: TxSeq(order),
                commit: None,
                end: format::SourceEndLsn(order),
                event_count: 0,
                encoded_bytes: format::COMPLETED_BYTES as u64,
            },
            first_record: RecordNo::new(first).unwrap(),
            descriptor_record: RecordNo::new(last).unwrap(),
        }
    }

    #[test]
    fn c1_receive_commits_opaque_records_index_and_head_across_reopen() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut store = Store::create(&cfg).unwrap();
        let entry = completion(&cfg, 1, 1, 2);
        let head = store
            .with_receive(0, |sink| {
                assert_eq!(*sink.head(), CatalogHead::empty());
                assert_eq!(sink.append(b"opaque frame")?, RecordNo::FIRST);
                assert_eq!(sink.append(b"opaque descriptor locator")?.get(), 2);
                sink.complete(entry)?;
                assert_eq!(sink.head().next_record.get(), 3);
                assert_eq!(sink.head().published, Some(entry.descriptor));
                assert_eq!(sink.head().receive_revision, 0);
                Ok(())
            })
            .unwrap();
        assert_eq!(head.receive_revision, 1);
        assert_eq!(head.apply_revision, 0);
        assert_eq!(head.consumer_cursor, TxSeq(0));
        drop(store);
        let store = Store::reopen(&cfg).unwrap();
        store
            .with_read_view(|view| {
                assert_eq!(*view.head(), head);
                assert_eq!(view.completion(TxSeq(1))?, Some(entry));
                assert_eq!(view.completion(TxSeq(2))?, None);
                let mut bytes = Vec::new();
                view.scan_records(RecordNo::FIRST, head.next_record, &mut |record| {
                    bytes.push(record.to_vec());
                    Ok(())
                })?;
                assert_eq!(
                    bytes,
                    [
                        b"opaque frame".to_vec(),
                        b"opaque descriptor locator".to_vec()
                    ]
                );
                Ok(())
            })
            .unwrap();
    }

    fn publish(store: &mut Store, revision: u64, entry: CompletionEntry) -> CatalogHead {
        store
            .with_receive(revision, |sink| {
                assert_eq!(sink.append(b"opaque frame")?, entry.first_record);
                assert_eq!(sink.append(b"opaque locator")?, entry.descriptor_record);
                sink.complete(entry)
            })
            .unwrap()
    }

    #[test]
    fn c1_status_pins_both_passes_while_receiver_and_apply_commit() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut receiver = Store::create(&cfg).unwrap();
        let first = completion(&cfg, 1, 1, 2);
        let old = publish(&mut receiver, 0, first);
        let mut apply = Store::open_role(&cfg, Role::Apply).unwrap();
        let status = Store::open_role(&cfg, Role::Status).unwrap();
        status
            .with_read_view(|view| {
                assert_eq!(*view.head(), old);
                let mut validated = Vec::new();
                view.scan_records(RecordNo::FIRST, old.next_record, &mut |bytes| {
                    validated.push(bytes.to_vec());
                    Ok(())
                })?;
                let latest = publish(&mut receiver, 1, completion(&cfg, 2, 3, 4));
                let applied = apply.advance_consumer(0, TxSeq(1))?;
                assert_eq!(applied.receive_revision, latest.receive_revision);
                assert_eq!(applied.published, latest.published);
                assert_eq!(applied.next_record, latest.next_record);
                assert_eq!(applied.apply_revision, 1);
                assert_eq!(applied.consumer_cursor, TxSeq(1));
                assert_eq!(*view.head(), old);
                assert_eq!(view.completion(TxSeq(1))?, Some(first));
                assert_eq!(view.completion(TxSeq(2))?, None);
                let mut emitted = Vec::new();
                view.scan_records(RecordNo::FIRST, old.next_record, &mut |bytes| {
                    emitted.push(bytes.to_vec());
                    Ok(())
                })?;
                assert_eq!(validated, emitted);
                status.with_read_view(|fresh| {
                    assert_eq!(*fresh.head(), applied);
                    Ok(())
                })?;
                Ok(())
            })
            .unwrap();
        let newer = publish(&mut receiver, 2, completion(&cfg, 3, 5, 6));
        assert_eq!(newer.apply_revision, 1);
        assert_eq!(newer.consumer_cursor, TxSeq(1));
        assert_eq!(records(&status).unwrap().len(), 6);
    }

    #[test]
    fn c1_status_uses_read_only_open_and_roles_reject_mutations() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut receiver = Store::create(&cfg).unwrap();
        publish(&mut receiver, 0, completion(&cfg, 1, 1, 2));
        assert_eq!(
            receiver.advance_consumer(0, TxSeq(1)),
            Err(Error::RoleViolation)
        );
        drop(receiver);
        let before = fs::read(cfg.dir.join(DATABASE_FILE)).unwrap();
        let mut status = Store::open_role(&cfg, Role::Status).unwrap();
        assert!(matches!(status.db, Handle::ReadOnly(_)));
        assert_eq!(
            status.settings().unwrap()["configured_policy"]["check_integrity_on_open"],
            false
        );
        assert_eq!(
            status.settings().unwrap()["configured_policy"]["read_only"],
            true
        );
        assert_eq!(
            status.with_receive(1, |_| panic!("status invoked writer")),
            Err(Error::RoleViolation)
        );
        assert_eq!(
            status.advance_consumer(0, TxSeq(1)),
            Err(Error::RoleViolation)
        );
        assert_eq!(status.checkpoint(), Err(Error::RoleViolation));
        assert_eq!(
            status.with_read_view(|_| Err(Error::Workload("visitor stopped"))),
            Err(Error::Workload("visitor stopped"))
        );
        assert_eq!(records(&status).unwrap().len(), 2);
        drop(status);
        assert_eq!(fs::read(cfg.dir.join(DATABASE_FILE)).unwrap(), before);
        let mut apply = Store::open_role(&cfg, Role::Apply).unwrap();
        assert_eq!(
            apply.with_receive(1, |_| panic!("apply invoked receiver")),
            Err(Error::RoleViolation)
        );
        assert_eq!(
            apply.with_batch(|_| panic!("apply invoked raw writer")),
            Err(Error::RoleViolation)
        );
        apply.advance_consumer(0, TxSeq(1)).unwrap();
    }

    #[test]
    fn c1_busy_and_conflict_do_not_enter_callbacks_or_poison_handles() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut receiver = Store::create(&cfg).unwrap();
        let mut apply = Store::open_role(&cfg, Role::Apply).unwrap();
        let blocker = builder(&cfg).open(cfg.dir.join(DATABASE_FILE)).unwrap();
        let held = blocker.try_begin_write().unwrap();
        assert_eq!(
            receiver.with_receive(0, |_| panic!("busy receiver callback")),
            Err(Error::Busy)
        );
        assert_eq!(apply.advance_consumer(0, TxSeq(0)), Err(Error::Busy));
        receiver.checkpoint().unwrap();
        apply.checkpoint().unwrap();
        held.abort().unwrap();
        publish(&mut receiver, 0, completion(&cfg, 1, 1, 2));
        assert_eq!(
            receiver.with_receive(0, |_| panic!("stale receiver callback")),
            Err(Error::Conflict)
        );
        receiver.checkpoint().unwrap();
        publish(&mut receiver, 1, completion(&cfg, 2, 3, 4));
        apply.advance_consumer(0, TxSeq(1)).unwrap();
        assert_eq!(apply.advance_consumer(0, TxSeq(2)), Err(Error::Conflict));
        assert_eq!(
            apply.advance_consumer(1, TxSeq(2)).unwrap().consumer_cursor,
            TxSeq(2)
        );
    }

    #[test]
    fn c1_completed_callback_abort_does_not_publish_records_or_indexes() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut receiver = Store::create(&cfg).unwrap();
        assert_eq!(
            receiver.with_receive(0, |sink| {
                sink.append(b"frame")?;
                sink.append(b"locator")?;
                sink.complete(completion(&cfg, 1, 1, 2))?;
                Err(Error::Conflict)
            }),
            Err(Error::Conflict)
        );
        assert_eq!(receiver.checkpoint(), Err(Error::Poisoned));
        assert_eq!(
            receiver.with_read_view(|_| panic!("poisoned read")),
            Err(Error::Poisoned)
        );
        drop(receiver);
        let receiver = Store::reopen(&cfg).unwrap();
        receiver
            .with_read_view(|view| {
                assert_eq!(*view.head(), CatalogHead::empty());
                assert_eq!(view.completion(TxSeq(1))?, None);
                Ok(())
            })
            .unwrap();
        assert!(records(&receiver).unwrap().is_empty());
    }

    #[test]
    fn c1_swallowed_completion_errors_abort_all_indexes_and_records() {
        for damage in 0..5 {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut receiver = Store::create(&cfg).unwrap();
            let first = completion(&cfg, 1, 1, 2);
            let before = publish(&mut receiver, 0, first);
            let mut bad = completion(&cfg, 2, 3, 4);
            match damage {
                0 => bad.descriptor.tx = first.descriptor.tx,
                1 => bad.descriptor.order = TxSeq(3),
                2 => bad.first_record = RecordNo::new(2).unwrap(),
                3 => bad.descriptor_record = RecordNo::new(3).unwrap(),
                _ => bad.descriptor.tx.stream.generation += 1,
            }
            let result = receiver.with_receive(1, |sink| {
                sink.append(b"uncommitted frame")?;
                sink.append(b"uncommitted locator")?;
                let error = sink.complete(bad).unwrap_err();
                if damage == 0 {
                    assert_eq!(error, Error::IdentityMismatch);
                }
                assert_eq!(sink.append(b"later"), Err(Error::Poisoned));
                assert_eq!(
                    sink.complete(completion(&cfg, 2, 3, 4)),
                    Err(Error::Poisoned)
                );
                Ok(())
            });
            assert!(result.is_err());
            assert_eq!(receiver.checkpoint(), Err(Error::Poisoned));
            drop(receiver);
            let receiver = Store::reopen(&cfg).unwrap();
            receiver
                .with_read_view(|view| {
                    assert_eq!(*view.head(), before);
                    assert_eq!(view.completion(TxSeq(1))?, Some(first));
                    assert_eq!(view.completion(TxSeq(2))?, None);
                    Ok(())
                })
                .unwrap();
            assert_eq!(records(&receiver).unwrap().len(), 2);
        }
    }

    #[test]
    fn c1_completion_index_is_canonical_and_identity_uses_all_fields() {
        let fixture = Fixture::new();
        let mut cfg = fixture.config();
        cfg.stream.generation = u64::MAX;
        cfg.stream.source.system_identifier = u64::MAX;
        let mut receiver = Store::create(&cfg).unwrap();
        let mut first = completion(&cfg, 1, 1, 2);
        first.descriptor.tx.xid = u32::MAX;
        first.descriptor.tx.begin_ordinal = u64::MAX;
        first.descriptor.end = format::SourceEndLsn(u64::MAX);
        first.descriptor.commit = Some(format::CommitLsn(u64::MAX));
        publish(&mut receiver, 0, first);
        let mut second = completion(&cfg, 2, 3, 4);
        second.descriptor.tx.xid = first.descriptor.tx.xid;
        second.descriptor.tx.begin_ordinal = u64::MAX - 1;
        second.descriptor.end = first.descriptor.end;
        publish(&mut receiver, 1, second);
        let transaction = receiver.db.begin_read().unwrap();
        let table = transaction.open_table(COMPLETIONS).unwrap();
        let value = table.get(1).unwrap().unwrap();
        assert_eq!(
            &value.value()[16..],
            format::encode_completed(&first.descriptor, cfg.limits).unwrap()
        );
        receiver
            .with_read_view(|view| {
                assert_eq!(view.completion(TxSeq(1))?, Some(first));
                assert_eq!(view.completion(TxSeq(2))?, Some(second));
                Ok(())
            })
            .unwrap();
        let base = first.descriptor.tx;
        for field in 0..6 {
            let mut different = base;
            match field {
                0 => different.stream.run.0[0] ^= 1,
                1 => different.stream.source.system_identifier ^= 1,
                2 => different.stream.source.database_oid ^= 1,
                3 => different.stream.generation ^= 1,
                4 => different.xid ^= 1,
                _ => different.begin_ordinal ^= 1,
            }
            assert_ne!(tx_key(base), tx_key(different));
        }
    }

    #[test]
    fn c1_revisions_keep_full_u64_width_and_overflow_aborts() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut receiver = Store::create(&cfg).unwrap();
        let transaction = receiver.db.writable().unwrap().try_begin_write().unwrap();
        {
            let mut table = transaction.open_table(METADATA).unwrap();
            table.insert(RECEIVE_REVISION, u64::MAX - 1).unwrap();
            table.insert(APPLY_REVISION, u64::MAX - 1).unwrap();
        }
        transaction.commit().unwrap();
        publish(&mut receiver, u64::MAX - 1, completion(&cfg, 1, 1, 2));
        let mut apply = Store::open_role(&cfg, Role::Apply).unwrap();
        let before = apply.advance_consumer(u64::MAX - 1, TxSeq(1)).unwrap();
        assert_eq!(before.receive_revision, u64::MAX);
        assert_eq!(before.apply_revision, u64::MAX);
        assert_eq!(
            receiver.with_receive(u64::MAX, |sink| {
                sink.append(b"overflow")?;
                Ok(())
            }),
            Err(Error::Overflow)
        );
        assert_eq!(receiver.checkpoint(), Err(Error::Poisoned));
        drop(receiver);
        let receiver = Store::reopen(&cfg).unwrap();
        receiver
            .with_read_view(|view| {
                assert_eq!(*view.head(), before);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn c1_read_range_rejects_outside_snapshot_and_visitor_abort_is_reusable() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let mut receiver = Store::create(&cfg).unwrap();
        publish(&mut receiver, 0, completion(&cfg, 1, 1, 2));
        receiver
            .with_read_view(|view| {
                let first = RecordNo::FIRST;
                let end = view.head().next_record;
                assert_eq!(
                    view.scan_records(end, first, &mut |_| panic!("reversed")),
                    Err(Error::Gap)
                );
                assert_eq!(
                    view.scan_records(first, end.next()?, &mut |_| panic!("outside")),
                    Err(Error::Gap)
                );
                assert_eq!(view.completion(TxSeq(0)), Err(Error::Gap));
                let mut count = 0;
                assert_eq!(
                    view.scan_records(first, end, &mut |_| {
                        count += 1;
                        Err(Error::Workload("visitor stopped"))
                    }),
                    Err(Error::Workload("visitor stopped"))
                );
                assert_eq!(count, 1);
                view.scan_records(end, end, &mut |_| panic!("empty range"))?;
                Ok(())
            })
            .unwrap();
        assert_eq!(records(&receiver).unwrap().len(), 2);
        receiver.checkpoint().unwrap();
    }

    #[test]
    fn c1_record_number_limit_is_checked_before_record_insertion() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        let receiver = Store::create(&cfg).unwrap();
        let transaction = receiver.db.writable().unwrap().try_begin_write().unwrap();
        {
            let mut sink = ReceiveBatch {
                cfg: &cfg,
                head: CatalogHead {
                    next_record: RecordNo::new(i64::MAX as u64).unwrap(),
                    ..CatalogHead::empty()
                },
                records: transaction.open_table(RECORDS).unwrap(),
                completions: transaction.open_table(COMPLETIONS).unwrap(),
                tx_ids: transaction.open_table(TX_IDS).unwrap(),
                written: 0,
                failure: None,
            };
            assert_eq!(sink.append(b"out of ordinal space"), Err(Error::Overflow));
            assert_eq!(sink.records.len().unwrap(), 0);
            assert_eq!(sink.head.next_record.get(), i64::MAX as u64);
            assert_eq!(sink.append(b"later"), Err(Error::Poisoned));
        }
        transaction.abort().unwrap();
        assert!(records(&receiver).unwrap().is_empty());
    }

    #[test]
    fn c1_missing_catalog_data_cannot_be_recreated_or_read_as_eof() {
        for damage in 0..9 {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut receiver = Store::create(&cfg).unwrap();
            publish(&mut receiver, 0, completion(&cfg, 1, 1, 2));
            publish(&mut receiver, 1, completion(&cfg, 2, 3, 4));
            publish(&mut receiver, 2, completion(&cfg, 3, 5, 6));
            let transaction = receiver.db.writable().unwrap().try_begin_write().unwrap();
            match damage {
                0 => {
                    transaction.delete_table(COMPLETIONS).unwrap();
                }
                1 => {
                    transaction.delete_table(TX_IDS).unwrap();
                }
                2 => {
                    transaction
                        .open_table(COMPLETIONS)
                        .unwrap()
                        .remove(2)
                        .unwrap();
                }
                3 => {
                    transaction
                        .open_table(TX_IDS)
                        .unwrap()
                        .remove(&tx_key(completion(&cfg, 2, 3, 4).descriptor.tx))
                        .unwrap();
                }
                _ => {
                    transaction
                        .open_table(METADATA)
                        .unwrap()
                        .remove((damage - 3) as u8)
                        .unwrap();
                }
            }
            transaction.commit().unwrap();
            assert_eq!(
                receiver.with_read_view(|_| panic!("missing catalog exposed a view")),
                Err(Error::Gap)
            );
            assert_eq!(
                receiver.with_receive(3, |_| panic!("missing catalog invoked writer")),
                Err(Error::Gap)
            );
            drop(receiver);
            for role in [Role::Receive, Role::Apply, Role::Status] {
                assert!(
                    matches!(Store::open_role(&cfg, role), Err(Error::Gap)),
                    "damage {damage}, role {role:?}"
                );
            }
        }
    }

    #[test]
    fn c1_corrupt_index_values_fail_lookup_without_false_completion() {
        for damage in 0..4 {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut receiver = Store::create(&cfg).unwrap();
            let first = completion(&cfg, 1, 1, 2);
            publish(&mut receiver, 0, first);
            publish(&mut receiver, 1, completion(&cfg, 2, 3, 4));
            let transaction = receiver.db.writable().unwrap().try_begin_write().unwrap();
            {
                let mut table = transaction.open_table(COMPLETIONS).unwrap();
                let mut encoded = encode_entry(first, &cfg).unwrap();
                match damage {
                    0 => {
                        table.insert(1, b"short".as_slice()).unwrap();
                    }
                    1 => {
                        encoded[ENTRY_BYTES - 1] ^= 1;
                        table.insert(1, encoded.as_slice()).unwrap();
                    }
                    2 => {
                        let mut foreign_order = first;
                        foreign_order.descriptor.order = TxSeq(2);
                        table
                            .insert(1, encode_entry(foreign_order, &cfg).unwrap().as_slice())
                            .unwrap();
                    }
                    _ => {
                        transaction
                            .open_table(TX_IDS)
                            .unwrap()
                            .insert(&tx_key(first.descriptor.tx), 2)
                            .unwrap();
                    }
                }
            }
            transaction.commit().unwrap();
            let status = Store::open_role(&cfg, Role::Status).unwrap();
            assert!(
                status
                    .with_read_view(|view| {
                        view.completion(TxSeq(1))?;
                        Ok(())
                    })
                    .is_err()
            );
            status
                .with_read_view(|view| {
                    assert_eq!(view.completion(TxSeq(2))?, Some(completion(&cfg, 2, 3, 4)));
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn c1_old_identity_is_rejected_before_any_role_opens_database() {
        let fixture = Fixture::new();
        let cfg = fixture.config();
        drop(Store::create(&cfg).unwrap());
        let mut identity = fs::read(cfg.dir.join("identity")).unwrap();
        identity[..8].copy_from_slice(b"RPGEXP01");
        fs::write(cfg.dir.join("identity"), identity).unwrap();
        fs::write(cfg.dir.join(DATABASE_FILE), b"not a database").unwrap();
        for role in [Role::Receive, Role::Apply, Role::Status] {
            assert!(matches!(
                Store::open_role(&cfg, role),
                Err(Error::IdentityMismatch)
            ));
        }
        assert_eq!(
            fs::read(cfg.dir.join(DATABASE_FILE)).unwrap(),
            b"not a database"
        );
    }

    #[test]
    fn c1_uncertain_receive_commit_reopens_to_one_atomic_catalog_cut() {
        for fail_on in [1, 2] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut receiver = Store::create(&cfg).unwrap();
            let before = publish(&mut receiver, 0, completion(&cfg, 1, 1, 2));
            drop(receiver);
            let (mut receiver, probe) = observed_store(&cfg);
            probe.fail_on.store(fail_on, Ordering::SeqCst);
            let second = completion(&cfg, 2, 3, 4);
            assert!(matches!(
                receiver.with_receive(1, |sink| {
                    sink.append(b"uncertain frame")?;
                    sink.append(b"uncertain locator")?;
                    sink.complete(second)
                }),
                Err(Error::Backend(_))
            ));
            assert_eq!(probe.calls.load(Ordering::SeqCst), fail_on);
            assert_eq!(receiver.checkpoint(), Err(Error::Poisoned));
            probe.fail_on.store(0, Ordering::SeqCst);
            drop(receiver);
            let receiver = Store::reopen(&cfg).unwrap();
            receiver
                .with_read_view(|view| {
                    match view.head().receive_revision {
                        1 => {
                            assert_eq!(*view.head(), before);
                            assert_eq!(view.completion(TxSeq(2))?, None);
                            assert_eq!(records(&receiver)?.len(), 2);
                        }
                        2 => {
                            assert_eq!(view.head().next_record.get(), 5);
                            assert_eq!(view.head().published, Some(second.descriptor));
                            assert_eq!(view.completion(TxSeq(2))?, Some(second));
                            assert_eq!(records(&receiver)?.len(), 4);
                        }
                        revision => panic!("partial catalog revision {revision}"),
                    }
                    Ok(())
                })
                .unwrap();
        }
    }

    #[test]
    fn c1_uncertain_apply_commit_poison_preserves_receive_catalog() {
        for fail_on in [1, 2] {
            let fixture = Fixture::new();
            let cfg = fixture.config();
            let mut receiver = Store::create(&cfg).unwrap();
            let before = publish(&mut receiver, 0, completion(&cfg, 1, 1, 2));
            drop(receiver);
            let (mut apply, probe) = observed_store(&cfg);
            apply.role = Role::Apply;
            probe.fail_on.store(fail_on, Ordering::SeqCst);
            assert!(matches!(
                apply.advance_consumer(0, TxSeq(1)),
                Err(Error::Backend(_))
            ));
            assert_eq!(probe.calls.load(Ordering::SeqCst), fail_on);
            assert_eq!(apply.advance_consumer(0, TxSeq(1)), Err(Error::Poisoned));
            probe.fail_on.store(0, Ordering::SeqCst);
            drop(apply);
            let status = Store::reopen(&cfg).unwrap();
            status
                .with_read_view(|view| {
                    let head = view.head();
                    assert_eq!(head.receive_revision, before.receive_revision);
                    assert_eq!(head.published, before.published);
                    assert_eq!(head.next_record, before.next_record);
                    assert!(
                        (head.apply_revision == 0 && head.consumer_cursor == TxSeq(0))
                            || (head.apply_revision == 1 && head.consumer_cursor == TxSeq(1))
                    );
                    Ok(())
                })
                .unwrap();
        }
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
                "read_only": false,
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
        assert_eq!(fs::read_dir(&cfg.dir).unwrap().count(), 3);
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
            let transaction = store.db.writable().unwrap().try_begin_write().unwrap();
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
            let transaction = store.db.writable().unwrap().try_begin_write().unwrap();
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
        let transaction = store.db.writable().unwrap().try_begin_write().unwrap();
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
