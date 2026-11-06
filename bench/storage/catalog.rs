//! The C1 catalog retains all history; its consumer cursor conveys no target proof.

use super::{Config, Error, RecordSink, Result, format};
use format::{CompletedTx, TxSeq};
use std::fs::{self, File, Metadata, OpenOptions, TryLockError};
use std::num::NonZeroU64;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

pub const CATALOG_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RecordNo(NonZeroU64);

impl RecordNo {
    pub const FIRST: Self = Self(NonZeroU64::new(1).unwrap());

    pub fn new(value: u64) -> Result<Self> {
        // All candidates retain SQLite's INTEGER key range; the maximum is an end sentinel.
        if value > i64::MAX as u64 {
            return Err(Error::Overflow);
        }
        NonZeroU64::new(value).map(Self).ok_or(Error::Gap)
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }

    pub fn next(self) -> Result<Self> {
        Self::new(self.get().checked_add(1).ok_or(Error::Overflow)?)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Receive,
    Apply,
    Status,
}

pub(super) const ROLE_FILES: [&str; 2] = ["receive.lock", "apply.lock"];

pub(super) struct RoleGuard {
    _file: Option<File>,
}

impl Drop for RoleGuard {
    fn drop(&mut self) {
        if let Some(file) = &self._file {
            // A retained descriptor must not extend this owner's role lock.
            let _ = file.unlock();
        }
    }
}

impl RoleGuard {
    pub(super) fn acquire(cfg: &Config, role: Role) -> Result<Self> {
        cfg.verify_directory()?;
        let name = match role {
            Role::Receive => ROLE_FILES[0],
            Role::Apply => ROLE_FILES[1],
            Role::Status => return Ok(Self { _file: None }),
        };
        let path = cfg.dir.join(name);
        let before = verify_role_file(&path)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let opened = file.metadata()?;
        if before.dev() != opened.dev() || before.ino() != opened.ino() {
            return Err(Error::IdentityMismatch);
        }
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => Error::Busy,
            TryLockError::Error(error) => Error::from(error),
        })?;
        Ok(Self { _file: Some(file) })
    }
}

pub(super) fn verify_role_file(path: &Path) -> Result<Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.len() != 0
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(Error::IdentityMismatch);
    }
    Ok(metadata)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogHead {
    pub catalog_version: u32,
    pub receive_revision: u64,
    pub apply_revision: u64,
    pub next_record: RecordNo,
    pub published: Option<CompletedTx>,
    pub consumer_cursor: TxSeq,
}

impl CatalogHead {
    pub fn empty() -> Self {
        Self {
            catalog_version: CATALOG_VERSION,
            receive_revision: 0,
            apply_revision: 0,
            next_record: RecordNo::FIRST,
            published: None,
            consumer_cursor: TxSeq(0),
        }
    }

    pub fn validate(&self, cfg: &Config) -> Result<()> {
        if self.catalog_version != CATALOG_VERSION {
            return Err(Error::Backend("storage catalog version differs"));
        }
        if let Some(published) = self.published {
            check_descriptor(&published, cfg)?;
            if published.order.0 >= self.next_record.get() {
                return Err(Error::Gap);
            }
        }
        if self.consumer_cursor > self.published.map_or(TxSeq(0), |tx| tx.order) {
            return Err(Error::Gap);
        }
        Ok(())
    }

    pub fn finish_receive(&mut self, expected: u64) -> Result<()> {
        if self.receive_revision != expected {
            return Err(Error::Conflict);
        }
        self.receive_revision = self
            .receive_revision
            .checked_add(1)
            .ok_or(Error::Overflow)?;
        Ok(())
    }

    pub fn advance_consumer(&mut self, expected: u64, next: TxSeq) -> Result<()> {
        if self.apply_revision != expected {
            return Err(Error::Conflict);
        }
        if next < self.consumer_cursor || next > self.published.map_or(TxSeq(0), |tx| tx.order) {
            return Err(Error::Gap);
        }
        let revision = self.apply_revision.checked_add(1).ok_or(Error::Overflow)?;
        self.apply_revision = revision;
        self.consumer_cursor = next;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletionEntry {
    pub descriptor: CompletedTx,
    pub first_record: RecordNo,
    pub descriptor_record: RecordNo,
}

impl CompletionEntry {
    pub fn validate(&self, cfg: &Config) -> Result<()> {
        check_descriptor(&self.descriptor, cfg)?;
        if self.first_record > self.descriptor_record {
            return Err(Error::Gap);
        }
        self.descriptor_record.next()?;
        Ok(())
    }
}

fn check_descriptor(descriptor: &CompletedTx, cfg: &Config) -> Result<()> {
    if descriptor.tx.stream != cfg.stream {
        return Err(Error::IdentityMismatch);
    }
    format::encode_completed(descriptor, cfg.limits)?;
    Ok(())
}

pub trait ReceiveSink {
    fn head(&self) -> &CatalogHead;
    fn append(&mut self, bytes: &[u8]) -> Result<RecordNo>;
    fn complete(&mut self, entry: CompletionEntry) -> Result<()>;
}

pub trait CatalogReadView {
    fn head(&self) -> &CatalogHead;
    fn completion(&self, order: TxSeq) -> Result<Option<CompletionEntry>>;
    fn scan_records(
        &self,
        first: RecordNo,
        end: RecordNo,
        visit: &mut RecordSink<'_>,
    ) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use format::{Limits, RunId, SourceEndLsn, SourceId, StreamId, TxId};
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    static SERIAL: AtomicU64 = AtomicU64::new(0);

    struct Scratch(Config);

    impl Scratch {
        fn new() -> Self {
            let mut cfg = config();
            cfg.dir = std::env::temp_dir().join(format!(
                "rpgmigrate-c1-common-{}-{}",
                std::process::id(),
                SERIAL.fetch_add(1, Ordering::Relaxed)
            ));
            cfg.create_directory().unwrap();
            Self(cfg)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0.dir).unwrap();
        }
    }

    fn config() -> Config {
        Config {
            dir: "unused-catalog-test".into(),
            stream: StreamId {
                run: RunId([3; 16]),
                source: SourceId {
                    system_identifier: 4,
                    database_oid: 5,
                },
                generation: 6,
            },
            limits: Limits::default(),
            batch_bytes: 2 * 1024 * 1024,
            cache_bytes: 4096,
            segment_bytes: 2 * 1024 * 1024,
        }
    }

    fn published() -> CatalogHead {
        CatalogHead {
            receive_revision: 7,
            next_record: RecordNo::new(4).unwrap(),
            published: Some(CompletedTx {
                tx: TxId {
                    stream: config().stream,
                    xid: 0,
                    begin_ordinal: 0,
                },
                order: TxSeq(3),
                commit: None,
                end: SourceEndLsn(8),
                event_count: 0,
                encoded_bytes: 128,
            }),
            ..CatalogHead::empty()
        }
    }

    #[test]
    fn apply_cas_preserves_fresh_receiver_fields_and_rejects_stale_revision() {
        let mut head = published();
        let before = head;
        head.advance_consumer(0, TxSeq(2)).unwrap();
        assert_eq!(head.apply_revision, 1);
        assert_eq!(head.consumer_cursor, TxSeq(2));
        assert_eq!(head.receive_revision, before.receive_revision);
        assert_eq!(head.next_record, before.next_record);
        assert_eq!(head.published, before.published);
        let committed = head;
        assert_eq!(head.advance_consumer(0, TxSeq(3)), Err(Error::Conflict));
        assert_eq!(head, committed);
        assert_eq!(head.advance_consumer(1, TxSeq(1)), Err(Error::Gap));
        assert_eq!(head.advance_consumer(1, TxSeq(4)), Err(Error::Gap));
        assert_eq!(head, committed);
    }

    #[test]
    fn receiver_revision_cannot_overwrite_consumer_progress_or_wrap() {
        let mut head = published();
        head.consumer_cursor = TxSeq(2);
        head.apply_revision = 9;
        assert_eq!(head.finish_receive(6), Err(Error::Conflict));
        head.finish_receive(7).unwrap();
        assert_eq!(head.receive_revision, 8);
        assert_eq!(head.apply_revision, 9);
        assert_eq!(head.consumer_cursor, TxSeq(2));
        head.receive_revision = u64::MAX;
        let before = head;
        assert_eq!(head.finish_receive(u64::MAX), Err(Error::Overflow));
        assert_eq!(head, before);
        head.apply_revision = u64::MAX;
        let before = head;
        assert_eq!(
            head.advance_consumer(u64::MAX, TxSeq(3)),
            Err(Error::Overflow)
        );
        assert_eq!(head, before);
    }

    #[test]
    fn empty_cursor_cannot_advance_and_persisted_head_must_match_stream() {
        assert_eq!(
            CatalogHead::empty().advance_consumer(0, TxSeq(1)),
            Err(Error::Gap)
        );
        let mut head = published();
        head.validate(&config()).unwrap();
        head.published.as_mut().unwrap().tx.stream.generation += 1;
        assert_eq!(head.validate(&config()), Err(Error::IdentityMismatch));
        head = published();
        head.catalog_version += 1;
        assert!(head.validate(&config()).is_err());
        head = published();
        head.consumer_cursor = TxSeq(4);
        assert_eq!(head.validate(&config()), Err(Error::Gap));
        head = published();
        head.next_record = RecordNo::FIRST;
        assert_eq!(head.validate(&config()), Err(Error::Gap));
    }

    #[test]
    fn completion_extent_rejects_reverse_bounds_and_invalid_descriptor() {
        let mut entry = CompletionEntry {
            descriptor: published().published.unwrap(),
            first_record: RecordNo::new(4).unwrap(),
            descriptor_record: RecordNo::new(3).unwrap(),
        };
        assert_eq!(entry.validate(&config()), Err(Error::Gap));
        entry.first_record = entry.descriptor_record;
        entry.validate(&config()).unwrap();
        entry.descriptor.order = TxSeq(0);
        assert!(entry.validate(&config()).is_err());
        assert_eq!(RecordNo::new(0), Err(Error::Gap));
    }

    #[test]
    fn record_number_bound_reserves_a_checked_exclusive_end() {
        let last_record = RecordNo::new(i64::MAX as u64 - 1).unwrap();
        let sentinel = last_record.next().unwrap();
        assert_eq!(sentinel.get(), i64::MAX as u64);
        assert_eq!(RecordNo::new(i64::MAX as u64 + 1), Err(Error::Overflow));
        assert_eq!(sentinel.next(), Err(Error::Overflow));
        assert_eq!(RecordNo::new(0), Err(Error::Gap));
    }

    #[test]
    fn roles_allow_receive_apply_status_but_refuse_duplicate_mutators() {
        let fixture = Scratch::new();
        let receive = RoleGuard::acquire(&fixture.0, Role::Receive).unwrap();
        let apply = RoleGuard::acquire(&fixture.0, Role::Apply).unwrap();
        let _status = RoleGuard::acquire(&fixture.0, Role::Status).unwrap();
        let _another = RoleGuard::acquire(&fixture.0, Role::Status).unwrap();
        assert!(matches!(
            RoleGuard::acquire(&fixture.0, Role::Receive),
            Err(Error::Busy)
        ));
        assert!(matches!(
            RoleGuard::acquire(&fixture.0, Role::Apply),
            Err(Error::Busy)
        ));
        drop(receive);
        RoleGuard::acquire(&fixture.0, Role::Receive).unwrap();
        drop(apply);
        RoleGuard::acquire(&fixture.0, Role::Apply).unwrap();
    }

    fn drop_owner_with_retained_descriptor(role: Role) {
        use super::super::{Candidate, Store};

        let fixture = Scratch::new();
        let mut cfg = fixture.0.clone();
        cfg.dir.push("store");
        let created = Store::create(Candidate::Sqlite, &cfg).unwrap();
        let owner = if role == Role::Receive {
            created
        } else {
            drop(created);
            Store::open_role(Candidate::Sqlite, &cfg, role, TxSeq(0)).unwrap()
        };
        // A clone retains the same open description as a child before exec closes its descriptors.
        let inherited = owner
            ._role_guard
            ._file
            .as_ref()
            .unwrap()
            .try_clone()
            .unwrap();
        assert!(matches!(
            Store::open_role(Candidate::Sqlite, &cfg, role, TxSeq(0)),
            Err(Error::Busy)
        ));
        drop(owner);
        let next_owner = Store::open_role(Candidate::Sqlite, &cfg, role, TxSeq(0))
            .expect("owner drop must release the role before inherited descriptors close");
        drop(inherited);
        assert!(matches!(
            Store::open_role(Candidate::Sqlite, &cfg, role, TxSeq(0)),
            Err(Error::Busy)
        ));
        drop(next_owner);
        Store::open_role(Candidate::Sqlite, &cfg, role, TxSeq(0)).unwrap();
    }

    #[test]
    fn dropping_receive_owner_releases_lock_with_retained_descriptor() {
        drop_owner_with_retained_descriptor(Role::Receive);
    }

    #[test]
    fn dropping_apply_owner_releases_lock_with_retained_descriptor() {
        drop_owner_with_retained_descriptor(Role::Apply);
    }

    #[test]
    fn old_experiment_identity_and_missing_role_files_fail_without_repair() {
        let fixture = Scratch::new();
        let mut old = fixture.0.identity();
        old[..8].copy_from_slice(b"RPGEXP01");
        old[8..12].copy_from_slice(&1_u32.to_le_bytes());
        fs::write(fixture.0.dir.join("identity"), old).unwrap();
        assert_eq!(fixture.0.verify_directory(), Err(Error::IdentityMismatch));
        assert_eq!(fs::read(fixture.0.dir.join("identity")).unwrap(), old);
        fs::write(fixture.0.dir.join("identity"), fixture.0.identity()).unwrap();
        fs::remove_file(fixture.0.dir.join("receive.lock")).unwrap();
        assert!(RoleGuard::acquire(&fixture.0, Role::Receive).is_err());
        assert!(!fixture.0.dir.join("receive.lock").exists());
    }

    #[test]
    fn role_file_symlinks_and_hardlinks_are_refused_before_locking() {
        let fixture = Scratch::new();
        let lock = fixture.0.dir.join("receive.lock");
        fs::remove_file(&lock).unwrap();
        std::os::unix::fs::symlink("apply.lock", &lock).unwrap();
        assert_eq!(fixture.0.verify_directory(), Err(Error::IdentityMismatch));
        fs::remove_file(&lock).unwrap();
        fs::hard_link(fixture.0.dir.join("apply.lock"), &lock).unwrap();
        assert_eq!(fixture.0.verify_directory(), Err(Error::IdentityMismatch));
    }
}
