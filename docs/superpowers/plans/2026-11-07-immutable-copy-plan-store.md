# Immutable COPY plan store

## Scope and approval

The parent approved the store-only core in parts 1 through 5 below.
The parent authorized Root and the integration coordinator to verify and freeze the protocol transcription against the selected contracts.
The seven protocol sections below include the author's literal header, DDL, index tuples, width conversions, and codec limits.
This scoped plan supersedes the larger joint draft.
Native4, metadata3, catalog streaming, per-table transfer, and source coordination retain their separate ownership and release boundaries.

## Approved core 1: files and prerequisites

Worker creates only `src/copy_plan.rs` and `src/copy_plan/tests.rs`.
Root registers `mod copy_plan` in `src/lib.rs` only after the real file exists and Sagan's namespace handback is usable.
Root moves the exact `rusqlite =0.41.1`, `default-features=false`, `features=[bundled]` stanza from dev to runtime, without version, feature, or lock changes; redb stays dev.
Root owns build-policy refusal for SQLite overrides and runtime source/version checks, release linkage, and image validation.
Consume frozen `contracts::CopyPlanRef`, `SnapshotAnchor`, `COPY_PLAN_REFERENCE_VERSION=1`, and `state::CopySnapshotIntent`.
No alternate IDs, native locks, format conversion, fake CDC transactions, source/catalog/COPY/target/CLI code.
This is a separate immutable plan object, not WAL-backend selection.

## Approved core 2: native file seam

Consume `NativeState::create_plan_file(&mut self, nonce: [u8; 16]) -> Result<NativePlanDraft<'_>>` and `open_plan_file(&self, nonce: [u8; 16]) -> Result<NativePlanPin<'_>>`.
Both provide `checked_path() -> Result<&Path>`; Pin also provides `check() -> Result<()>`; Draft provides `publish(self) -> Result<()>`.
State owns fixed `data/plans/<nonce>/plan.db`, private pending namespace, inode/mode/owner checks, hierarchy sync, and atomic no-replace publication.
Adapter adds no registry, lock file, raw fd getter, or path input.
SQLite opens the existing reserved leaf READ_WRITE/READ_ONLY plus NOFOLLOW, without CREATE.
Never open or close another fd to a live SQLite DB inode; use the State fstatat checks.
Declare Connection before Pin in reader fields.
Builder local drop order closes Connection before Draft.
No cleanup or adoption on failure or panic.

## Approved core 3: payload and publication

Two tables only: seal and records; unique relation and name lookup indexes.
The protocol appendix fixes DDL, header, and CRC exactly.
`PlanEntry::checked(table: CopyTable, relation: Event) -> Result<PlanEntry>` accepts only Relation and exact CopyTable schema/table/ordered column names, then retains only format and Relation.
Stream entries into a single DELETE-journal/FULL-synchronous SQLite transaction, seal row last.
Require acknowledged COMMIT, full seal verification, and successful explicit `Connection::close` before `draft.publish`.
Only successful publication returns `CopyPlanRef`.
Error, panic, ambiguous commit, close failure, or sync failure returns no reference and retains artifacts.
Caller binds the receipt-derived immutable native intent after publication.
Native 16 KiB control never contains the plan.
No artificial small total-plan limit.

## Approved core 4: exact new API

```rust
publish_copy_plan(
    native: &mut NativeState,
    source: &SourceSlotReceipt,
    plan_nonce: [u8; 16],
    buffer_bytes: usize,
    entries: impl Iterator<Item = Result<PlanEntry>>,
    io: &IoControl,
) -> Result<CopyPlanRef>

open_sealed_copy_plan<'n>(
    native: &'n NativeState,
    expected: &CopySnapshotIntent,
    io: &IoControl,
) -> Result<SealedCopyPlan<'n>>
```

Publication validates the current confirmed binding and constructs the header from the supplied receipt without adding a State-to-PG edge.
Open requires exact checked native intent, a READ_ONLY stable read transaction, and full validation before returning the owner.
`SealedCopyPlan` has private fields and is non-Clone and non-Deserialize.
It holds NativePlanPin and the SQLite connection through every consumer operation, with no raw connection, path, or handle getter.
`buffer_bytes(&self) -> usize` returns the checked header value.

## Approved core 5: bounded consumers and tests

Methods take `&mut self` and `&IoControl`.

```rust
scan_from(
    after: u64,
    io: &IoControl,
    visit: impl FnMut(u64, CopyFormat, &Event) -> Result<()>,
) -> Result<()>

lookup_name(
    name: &QualifiedTable,
    io: &IoControl,
    visit: impl FnOnce(u64, CopyFormat, &Event) -> Result<()>,
) -> Result<()>

lookup_relation(
    key: RelationKey,
    io: &IoControl,
    visit: impl FnOnce(u64, CopyFormat, &Event) -> Result<()>,
) -> Result<()>
```

Visitors see one borrowed decoded Relation, never an entire CopyPlan.
Test exact projections, empty plans and zero CRC, identity/epoch/nonce/header mismatch, schema/index substitution, gaps/reordering/corruption, guarded decode budgets, large streamed plans, indexed lookup, callback/iterator error/panic, COMMIT/close/publication failures, and POSIX lock preservation.
Reuse Native4 platform, no-replace, and borrow tests.
Independent review and Root release/link/image gates remain future gates.

## Protocol appendix 1: header

Header is compact `serde_json::to_vec` of this validated private type:

```rust
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PlanHeader {
    reference_version: u32,
    binding: SourceSlotBinding,
    snapshot_token: String,
    consistent_point: SourceEndLsn,
    plan_nonce: [u8; 16],
    buffer_bytes: u64,
}
```

Binding uses its existing serde fields: stream, slot_name, plugin, catalog_epoch.
Stream uses run, source, generation.
Source uses system_identifier, database_oid.
Run and nonce are 16 JSON byte integers.
LSN is an unsigned JSON integer.
Plugin uses the existing exact serde names `pgoutput`, `test_decoding`, and `wal2json`.
Header limit is 16,384 bytes before deserialization.
On read, reject missing, duplicate, or unknown fields, invalid widths, and trailing data.
Reserialize the same structs and require byte equality.
Golden bytes freeze the nested field order and this project encoding recipe.
Any encoding or layout change requires a new reference version.
Require version 1, nonzero nonce and point, token length 1 through 1024 bytes without NUL, and a checked positive buffer size.

## Protocol appendix 2: binding and payload

Before creating the draft, publication calls actual `native.checked_source_binding(&source.intent.binding)` and validates the supplied receipt and header fields.
That method checks the confirmed binding; it does not compare the supplied receipt's snapshot fields with the persisted receipt.
Publication must not require an existing COPY intent, because publication precedes intent binding.
The caller then binds the receipt-derived intent through State, which requires exact confirmed receipt matching.
Reader open calls `native.checked_source_binding(&header.binding)` and requires `native.checked_copy_snapshot()? == Some(expected.clone())`, exact header stream/token/point/nonce equality with `expected.anchor`, and exact seal/reference equality.
Epoch is `binding.catalog_epoch()`, independent of `stream.generation`.
Payload is `format: u8` (Text=0, Binary=1) plus existing `event_codec::encode_event(Relation, limits)` bytes.
PlanEntry keeps only Relation and format.
Before discarding input CopyTable, compare its exact schema, table, and ordered column names.
Refuse any mismatch.
Binary remains distinct.
No plan_columns, target OID, nullability, or collation evidence is sealed.
Publication returns a storage reference, never a source or target proof.
Reader qualification happens only after the caller binds the receipt-derived native intent.

## Protocol appendix 3: schema

Set `PRAGMA user_version=1`.
`SEAL_DDL` is exactly this single-line string, without a trailing semicolon or newline:

```sql
CREATE TABLE seal(singleton INTEGER PRIMARY KEY CHECK(singleton=1),header BLOB NOT NULL,table_count BLOB NOT NULL CHECK(length(table_count)=8),logical_bytes BLOB NOT NULL CHECK(length(logical_bytes)=8),crc32 BLOB NOT NULL CHECK(length(crc32)=4)) STRICT
```

`RECORDS_DDL` is exactly this single-line string, without a trailing semicolon or newline:

```sql
CREATE TABLE records(ordinal INTEGER PRIMARY KEY CHECK(ordinal>0),relation_oid INTEGER NOT NULL CHECK(relation_oid BETWEEN 1 AND 4294967295),catalog_epoch BLOB NOT NULL CHECK(length(catalog_epoch)=8),schema_name BLOB NOT NULL,table_name BLOB NOT NULL,copy_format INTEGER NOT NULL CHECK(copy_format IN (0,1)),relation_payload BLOB NOT NULL,record_crc32 BLOB NOT NULL CHECK(length(record_crc32)=4)) STRICT
```

`RELATION_INDEX_DDL` is exactly this string, without a trailing semicolon or newline:

```sql
CREATE UNIQUE INDEX records_by_relation ON records(relation_oid,catalog_epoch)
```

`NAME_INDEX_DDL` is exactly this string, without a trailing semicolon or newline:

```sql
CREATE UNIQUE INDEX records_by_name ON records(schema_name,table_name)
```

These are protocol constants for the adapter, not shell procedures.

## Protocol appendix 4: schema qualification

Four exact DDL constants omit terminal semicolons.
Reader requires exact `main.sqlite_schema` type/name/tbl_name/sql tuples and positive distinct root pages.
Reject extra objects, including sqlite_stat and autoindex objects.
Temp has no objects.
`main.table_list` allows only SQLite's built-in schema row, seal(table,5,wr=0,strict=1), and records(table,8,wr=0,strict=1).
`table_xinfo` requires the following exact ordered tuples `(cid,name,type,notnull,dflt_value,pk,hidden)`:

```text
seal:
(0,singleton,INTEGER,0,NULL,1,0)
(1,header,BLOB,1,NULL,0,0)
(2,table_count,BLOB,1,NULL,0,0)
(3,logical_bytes,BLOB,1,NULL,0,0)
(4,crc32,BLOB,1,NULL,0,0)
records:
(0,ordinal,INTEGER,0,NULL,1,0)
(1,relation_oid,INTEGER,1,NULL,0,0)
(2,catalog_epoch,BLOB,1,NULL,0,0)
(3,schema_name,BLOB,1,NULL,0,0)
(4,table_name,BLOB,1,NULL,0,0)
(5,copy_format,INTEGER,1,NULL,0,0)
(6,relation_payload,BLOB,1,NULL,0,0)
(7,record_crc32,BLOB,1,NULL,0,0)
```

The rowid-alias primary keys report `notnull=0`: the pinned SQLite STRICT implementation excludes the INTEGER PRIMARY KEY from implicit NOT NULL assignment.
`index_list(seal)` is empty.
Records has exactly both named indexes with unique=1, origin='c', and partial=0; ignore only enumeration sequence order.
`index_xinfo` requires these exact tuples `(seqno,cid,name,desc,coll,key)`:

```text
records_by_relation:
(0,1,relation_oid,0,BINARY,1)
(1,2,catalog_epoch,0,BINARY,1)
(2,-1,NULL,0,BINARY,0)
records_by_name:
(0,3,schema_name,0,BINARY,1)
(1,4,table_name,0,BINARY,1)
(2,-1,NULL,0,BINARY,0)
```

Reject extra, expression, partial, DESC, and collation variants.
Decoded payload projections must also match during full validation and each lookup; schema inspection alone does not qualify an index.
No schema parser or normalization fallback.
Primary semantics: [SQLite PRAGMAs](https://www.sqlite.org/pragma.html) and [SQLite schema table](https://www.sqlite.org/schematab.html).

## Protocol appendix 5: order and lookup

Ordinals are plan-local one-based u64, contiguous in insertion and scan order.
Zero is only the scan cursor before row 1.
Check every counter and size addition, usize/u64 conversion, and u64/i64 conversion before SQL or callback.
Reject ordinal or count beyond `i64::MAX` only in the adapter.
Summary count, logical_bytes, and epoch are big-endian BLOB8.
CRC is big-endian BLOB4.
Names are exact UTF-8 BLOB projections.
Each decoded Relation must equal projected OID, epoch, schema, and table during full validation and every lookup.
No case folding or reordering.
Full validation scans base records, checks contiguous ordinals, and probes both named indexes for each row.
Lookups use INDEXED BY, LIMIT 2, and require one exact result.
Missing or duplicate is an error.
`scan_from` accepts 0 through table_count and verifies suffix completeness.

## Protocol appendix 6: bounds

Limits are `{max_frame_bytes: 1_048_576, max_event_bytes: 1_048_576, max_parts: 1}`.
Both `encode_event` and `with_decoded_event` use the same limit: `min(max_event_bytes, (max_frame_bytes - HEADER_BYTES) * max_parts)`.
Actual codec HEADER_BYTES is 88, so Relation bytes are at most 1,048,488.
Format plus Relation payload is at most 1,048,489.
The R checksum frame adds 1+8+8 bytes, for a total at most 1,048,506.
No CDC frame header is stored.
Store format once in `copy_format` and Relation bytes once in `relation_payload`; reconstruct canonical checksum input without concatenating the whole plan.
DecodeBudget is 4 MiB per entry.
Apply SQL length guards before copying BLOBs.
Re-encode each decoded Relation and require exact bytes before callback.
Use per-name 1024-byte and per-Relation 4096-column limits, a 4 MiB SQLite cache, and mmap=0.
These are record and working-set bounds, not a total-plan cap or RSS guarantee.
Scans retain one entry.
Indexed lookups never rescan or materialize the plan.

## Protocol appendix 7: checksums and controls

Logical bytes are the checked sum of `1 + relation_payload.len()`, excluding header, framing, and SQLite bytes.
Record CRC is `CRC32(R:u8 || ordinal:u64BE || payload_len:u64BE || format:u8 || Relation bytes)`.
Aggregate CRC is `CRC32(b"RPGPLAN1" || H:u8 || header_len:u64BE || header || every R frame || S:u8 || table_count:u64BE || logical_bytes:u64BE)`.
Tags H, R, and S are ASCII.
Empty count and bytes are zero.
CRC zero is valid.
Add golden bytes, noncanonical header refusal, partial/expression/DESC/collation/index substitution, projection mismatch, ordinal zero/gaps/overflows, codec bound plus/minus one, CopyTable case/order/subset mismatch, empty plan, CRC corruption, and bounded indexed-lookup tests.
Source ownership stays fixed.
No edits before Root's saved scoped-checklist readiness.

## Checklist

- [x] Save the approved store-only core with exact file ownership and canonical shared types.
- [x] Save and check the exact literal protocol under the parent's delegated transcription authority.
- [x] Confirm the usable Native4 namespace handback and actual draft/pin methods.
- [ ] Observe meaningful storage and publication failures before implementation changes.
- [ ] Implement only the new store module and its focused tests using the real namespace.
- [ ] Promote the exact locked SQLite dependency and add build-policy/runtime identity checks when the real module can use it.
- [ ] Register the real module after file existence and namespace readiness, without stubs or lint suppression.
- [ ] Freeze source, test evidence, bounds, and error-path controls for independent review.
- [ ] Verify release linkage and exact native runtime images on the supported architectures.

The maintained [task ledger](../../../tasks/todo.md) records handoffs and observed qualification.
