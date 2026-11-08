# CDC, Recovery, and Apply Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development for delegated execution.
> Use superpowers:executing-plans inside each assigned task.
> Track steps with checkboxes.

**Goal:** Apply real source transactions with recoverable checkpoints, all durability profiles, and measured target throughput.

**Architecture:** Decode into one bounded event representation and replay complete source transactions through a serial reference applier.
Keep target checkpoint updates inside the transaction that changes user data.
Compare storage candidates before selecting a production backend.

**Tech Stack:** The E0/E1 package, native libpq, vetted persistence candidates, and disposable PostgreSQL fixtures.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md).
Read the [E0/E1 plan](2026-11-06-engine.md) and [verification plan](2026-11-06-verification-performance.md) before implementation.

## Global Constraints

- The executable is `rpgmigrate`.
- The baseline is `ydixken/pgcopydb` at `86bf908adca02d87063849dae775a02c1659c829` on `origin/v0.18-fixes`.
- The default durability profile is `durable`.
- The default `--apply-jobs` value is `1`.
- `relaxed` requires explicit finite `--loss-max-transactions` and `--loss-max-bytes` values.
- COPY and schema completion are durable in every profile.
- Never import, resume, restart, convert, delete, or clean legacy pgcopydb state.
- Every successful normal completion proves a complete durable target prefix.
- Preserve external-effect exclusions and require idempotency for those effects.
- Use ordinary branches, preserve worker boundaries, and wait for leader review before executing this milestone.

## Review Focus

- Interleaved transactions or XID reuse must not alias journal records (E2).
- A complete transaction larger than memory or free disk must not deadlock ingestion (E2).
- A lost COMMIT reply must not duplicate a committed target transaction (E3).
- A barrier on one database must not release another database's risk reservation (E4).
- A missing descriptor, relation generation, or required segment must cause a gap failure (E2-E4).

## Files, ownership, and dependencies

The integration worker owns canonical identities in `src/identity.rs`, `src/cdc/mod.rs`, `src/cdc/contracts.rs`, and changes to root manifests/modules.
The codec worker owns promotion of `bench/storage/format.rs` to the single canonical `src/cdc/codec.rs` and the new `src/cdc/event_codec.rs`.
The decoder worker owns `src/cdc/decode.rs` and `src/cdc/plugins/test_decoding.rs`.
The catalog worker owns the required metadata extraction in `src/catalog.rs`.
The storage worker owns `src/state/journal.rs`, `src/state/catalog.rs`, `src/state/recovery.rs`, and private candidate modules.
The apply worker owns `src/apply/serial.rs`, `src/apply/checkpoint.rs`, and `src/apply/recovery.rs`.
The durability worker owns `src/state/risk.rs`, `src/apply/durability.rs`, and `src/cdc/feedback.rs` after E3.
The transport worker owns prepared/parameter execution additions under `src/pg/`.
The verification worker owns `tests/cdc/`, `tests/faults/`, `bench/`, and fixture tooling.
The documentation worker owns `docs/durability.md`, `docs/recovery.md`, and `docs/performance.md`.
Do not let multiple workers edit a shared declaration or manifest at once.
The integration worker coordinates existing test imports and visibility during the codec handoff.
Do not retain a second framing implementation or duplicate canonical identity types.
Integrate E2 after the C1 snapshot releases the relevant file ownership.

E2 requires E1's connection and native-state ownership.
E3 requires complete transaction readers from E2 and prepared execution from the transport worker.
E4 requires E3 checkpoint authority and the run-wide catalog transaction primitive from E2.
Storage and decoder workers may proceed together after the shared contract review.
Benchmark workload generation can proceed while these workers implement their modules.

## Shared interfaces for E2-E4

Authority commit `eaf7517f3ef4e77144d81c9aac1d8b819833c851` contains private checkpoint infrastructure with mandatory physical commit positions and target metadata version 1.
The declarations below include the approved source-frontier extension after that baseline.
They do not establish runtime CDC, source ACK policy, or backend qualification.
The `Decoder` and `Applier` declarations remain planned interfaces.
Reuse E0's `Result`, `RunId`, `PgConnection`, and `IoControl`.
Reuse the canonical identity types in `src/identity.rs` rather than defining another identity representation.
Use crate-private plain fields and opaque storage/transport handles.
Use the frozen owner contracts for the codec and target authority.

```rust
struct SourceId { system_identifier: u64, database_oid: u32 }
struct StreamId { run: RunId, source: SourceId, generation: u64 }
struct TxId { stream: StreamId, xid: u32, begin_ordinal: u64 }
struct TxSeq(u64);
struct CommitLsn(u64);
struct SourceEndLsn(u64);
struct TargetWalLsn(u64);
struct RelationKey { oid: u32, generation: u64 }
struct Column { name: String, type_oid: u32, type_mod: i32, key: bool }
enum Value { Null, UnchangedToast, Text(Vec<u8>), Binary(Vec<u8>) }
enum Event {
    Relation { key: RelationKey, schema: String, name: String,
               columns: Vec<Column>, replica_identity: u8 },
    Insert { relation: RelationKey, values: Vec<Value> },
    Update { relation: RelationKey, old: Option<Vec<Value>>, new: Vec<Value> },
    Delete { relation: RelationKey, old: Vec<Value> },
    Truncate { relations: Vec<RelationKey>, cascade: bool, restart_identity: bool },
}
struct Frame {
    tx: TxId, event_ordinal: u64, part: u32,
    last_part: bool, payload: Vec<u8>,
}
struct CompletedTx {
    tx: TxId, order: TxSeq, commit: Option<CommitLsn>, end: SourceEndLsn,
    event_count: u64, encoded_bytes: u64,
}
struct Frontier { tx: TxId, order: TxSeq, commit: Option<CommitLsn>, end: SourceEndLsn }
enum Plugin { Pgoutput, TestDecoding, Wal2Json }
enum JournalWrite { Synchronized, Buffered }
enum TargetCommit { Synchronous, Asynchronous }
struct LossCaps { transactions: u64, bytes: u64 }
enum Profile { Durable, ReplaySafe, Relaxed(LossCaps) }
```

`TxSeq` is local source commit order within a stream, not a WAL position.
Reserve `TxSeq(0)` and require each completed transaction's sequence to follow its predecessor exactly.
`CommitLsn` means the physical commit-record position, which some plugins do not expose.
Use `None` when that evidence is absent and never substitute the transaction-end LSN.
`SourceEndLsn` is mandatory for a source COMMIT that passes all terminal checks.
Always check stream identity, exact positive sequence continuity, and nondecreasing transaction-end LSNs.
Require nondecreasing physical commit positions when both adjacent values are `Some`.
A present physical position must not exceed its transaction-end position.
Equal LSNs do not merge distinct transactions or events.
Every nonempty frontier includes its full transaction identity.
Begin ordinal need not increase in commit order.
An empty checkpoint uses sequence zero and absent transaction fields, separately from a nonempty frontier whose physical position is unknown.
`encoded_bytes` counts the complete canonical wire representation before optional storage compression: each frame's 88-byte header and payload plus one 128-byte completion descriptor.
Include relation context required for replay in the complete transaction's encoded accounting.
Frames can split one event across segments.
Keep one oversize event bounded by the documented exception and never collect a whole transaction in memory.
Relation definitions outside source transactions must attach to the first dependent transaction's recoverable context.
Version the codec and reject unknown required tags, broken ordinals, or conflicting relation generations.
`TailPolicy::Unpublished` applies only to an externally proven unpublished suffix.
Codec tolerance does not authorize pruning.
Persist source identity and generation before decoding can publish complete transactions.

### Source COMMIT evidence and codec version

The [source-frontier review](../../research/2026-11-06-context.md#source-transaction-frontier-evidence) records the plugin-specific evidence and primary sources.
For `test_decoding`, use the original COMMIT envelope's `data_start` as `SourceEndLsn` after terminal checks.
The transport exposes that position as `ReplicationEnvelope::XLogData.wal_start`.
It supplies no physical `CommitLsn`.
Baseline wal2json format 2 uses the same terminal-envelope evidence.
With `include-lsn=true`, its COMMIT object supplies physical `lsn` and terminal `nextlsn`.
Pin the actual wal2json version and options before qualifying that behavior.
For pgoutput v1, retain the BEGIN XID because COMMIT supplies both positions but no XID.

Preserve each plugin message's original boundary, payload, `data_start`, and envelope context until terminal validation finishes.
Generic `wal_end`, keepalive positions, and replication-origin positions are not terminal source transaction evidence.
Do not calculate a missing end position from payload length or a physical position.
Do not drop individual messages merely because their envelope LSN precedes the last completed frontier.
Complete only after the active transaction and terminal COMMIT pass validation.
An interrupted incomplete transaction is not evidence of a source abort.

The codec extension uses RPGS version 2 with descriptor flag `0x02` named `COMMIT_PRESENT`.
Descriptor flags are `0` or `0x02`, while frame flags remain `0` or `1`.
For `None`, the physical-position slot must contain canonical zero bytes.
`Some(CommitLsn(0))` sets the presence flag and remains distinct from `None`.
Frame headers remain 88 bytes and completion descriptors remain 128 bytes, with their CRCs intact.
Reject version 1 instead of inferring presence or converting old records.
Keep the archived version-1 pilot bytes and their historical provenance unchanged.

```rust
struct DecoderPosition { next_begin_ordinal: u64, next_order: TxSeq }
trait RelationResolver {
    fn resolve(&mut self, table: &QualifiedTable) -> Result<&Event>;
}
struct Decoder;
impl Decoder {
    fn new(plugin: Plugin, stream: StreamId, limits: codec::Limits,
           position: DecoderPosition) -> Result<Self>;
    fn feed(&mut self, input: &ReplicationFrame,
            resolver: &mut impl RelationResolver,
            emit: &mut impl FnMut(&Frame) -> Result<()>) -> Result<()>;
    fn take_completed(&mut self) -> Option<CompletedTx>;
}
struct Store;
struct ReadyTxn;
struct TxReader;
impl Store {
    fn open(state: &NativeState) -> Result<Self>;
    fn append(&mut self, frame: &Frame) -> Result<()>;
    fn complete(&mut self, tx: &CompletedTx, write: JournalWrite) -> Result<ReadyTxn>;
    fn sync_through(&mut self, stream: &StreamId, end: SourceEndLsn) -> Result<()>;
    fn reader(&self, tx: &ReadyTxn) -> Result<TxReader>;
    fn next_complete(&self, stream: &StreamId, after: TxSeq) -> Result<Option<ReadyTxn>>;
    fn prune(&mut self, proof: &DurableProof) -> Result<()>;
}
impl ReadyTxn { fn descriptor(&self) -> &CompletedTx; }
impl TxReader { fn next_event(&mut self) -> Result<Option<Event>>; }
struct Checkpoint {
    stream: StreamId, applied: Frontier, barrier_epoch: u64, barrier_counter: u64,
}
struct DurableProof;
struct PreparedApply;
enum CommitOutcome {
    Acknowledged { checkpoint: Checkpoint, durable: Option<DurableProof> },
    Unknown,
}
struct Applier;
impl Applier {
    fn acquire(connection: PgConnection, stream: StreamId, state: &mut NativeState,
               io: &IoControl) -> Result<Self>;
    fn checkpoint(&mut self, io: &IoControl) -> Result<Option<Checkpoint>>;
    fn prepare(&mut self, tx: &ReadyTxn, input: &mut TxReader,
               io: &IoControl) -> Result<PreparedApply>;
    fn commit(&mut self, tx: PreparedApply, mode: TargetCommit,
              io: &IoControl) -> Result<CommitOutcome>;
    fn barrier(&mut self, through: Option<&Frontier>, epoch: u64,
               io: &IoControl) -> Result<DurableProof>;
}
```

The frozen decoder declarations above replace the earlier payload-only feed and separate `frame_bytes` argument.
They define the next implementation slice, not an implemented runtime API.
The resolver borrows verified `Event::Relation` context without cloning its columns for each row.
Check the returned variant, qualified name, and supported relation shape before using that context.
The pure decoder performs no database lookup and owns no persistent catalog or cache policy.
`DecoderPosition` is local control state, not another wire variant.
The receiver supplies it from checked restart context and must not derive begin-ordinal allocation from the last committed transaction alone.
Require a positive `next_order` and use checked counter increments.
The first constructor creates an idle decoder and does not restore an unfinished transaction.
The receiver retains responsibility for unfinished input, replay reconciliation, and durable context.
Keep one pending completion, with no completion queue.
The receiver must consume and publish it before supplying more transaction data.
Raw transport owns an opaque `ReplicationFrame` with byte, envelope, and payload access.
Its feedback queue accepts externally supplied positions and does not implement decoder completion or ACK policy.

`Store::open` uses the candidate recorded in native experiment configuration, then the selected production backend after measurement.
Its input is E1's checked `NativeState`, and it must not reopen foreign paths.
E2 replaces E1's exclusive whole-run access with role/stream locks for the approved overlapping-process topology.
Keep initial identity/publication operations exclusive and progress reads read-only.
`Buffered` completion is process-local until data and references become durable.
Never publish durable metadata references to unsynchronized journal files.
Readers pin required segments until drop.
`DurableProof` is opaque and can originate only from positive synchronous COMMIT evidence for a real logged write on the fenced primary.
The normal data transaction updates its logged checkpoint and increments the barrier counter in that same UPDATE.
Its synchronous COMMIT returns the proof without another COMMIT per group.
An explicit barrier performs a logged counter update and synchronous COMMIT to cover asynchronous work or a flush with no new data.
Both paths check the same target and stream identity.
Each proof carries the exact covered frontier, epoch, barrier counter, and target provenance.
An empty checkpoint can receive a writing barrier with `None` as its frontier, for provisioning reconciliation or a final flush with no CDC transactions.
That proof covers zero CDC transactions and does not claim a requested end position or COPY snapshot coverage.
Never fabricate an LSN for the empty case.
A future COPY snapshot anchor has a separate contract.
An asynchronous acknowledgment carries no durable proof, and a checkpoint read alone cannot construct one.
The first E3 production path always commits synchronously.
Asynchronous execution is test-private until the E4 policy consumer exists.
Target-local WAL samples are diagnostics, never source feedback values.

### First decoder slice: payload, metadata, and limits

Promote the existing RPGSv2 record codec without changing its wire representation.
Move the exact `crc32fast = 1.5.4` dependency into runtime use after root verifies the manifest, lock graph, and features.
Reuse its prior license, MSRV, and security audit because this introduces no new package or version.
Add no binary serialization dependency or JSON byte-array representation.

Each canonical event payload begins with version `u8 = 1` and an event tag `u8`.
Tags `1` through `5` mean `Relation`, `Insert`, `Update`, `Delete`, and `Truncate`, in that order.
Encode variant fields in their declaration order above.
Encode fixed-width integers in little endian, including signed `i32` typmods.
Use checked `u32` byte lengths for strings and byte values, and checked `u32` counts for vectors.
Value tags are `0 = Null`, `1 = UnchangedToast`, `2 = Text`, and `3 = Binary`.
Text and binary values retain their raw bytes.
Booleans use exactly `0` or `1`.
Options use `0` for absent and `1` followed by the value for present.
Reject unknown or reserved tags, invalid UTF8 names, overflow, truncated fields, impossible counts, and trailing bytes.

Reuse `codec::Limits { max_frame_bytes: usize, max_event_bytes: u64, max_parts: u32 }` without another frame-size control.
Its existing valid ranges are 128 bytes through 64 MiB per frame, 1 byte through 1 GiB per event, and 1 through 1,048,576 parts.
Reuse the existing limit validator at construction, with visibility coordinated by integration.
Check lengths and available input before allocation.
Emit frames synchronously and account for their 88-byte headers within `max_frame_bytes`.
Keep event bytes and part counts within their limits, without collecting a whole transaction.
The borrowed libpq message can exceed a nominal frame budget and remains part of memory accounting.

The record codec retains `codec::Result<T> = std::result::Result<T, codec::Error>`.
Decoder and resolver interfaces use the existing `crate::contracts::Result<T>` and `Error { kind: ErrorKind, message: String }`.
Map codec errors at the consuming boundary into existing error categories, with diagnostics that omit row contents.
Propagate resolver and sink errors without replacing their categories.
A parser, resolver, or sink error poisons the unfinished transaction and prevents completion.

The first qualified profile uses DEFAULT replica identity with a verified primary key, static logged metadata, and E1 builtin restrictions.
Keep streaming and two-phase plugin output disabled, and refuse unsupported paths explicitly.
The accepted operator contract requires default `pgoutput` and full semantics for selected plugin configurations; staged unsupported configurations must fail explicitly.
The current `test_decoding` baseline remains unchanged, and full CLI parity outside the [operator matrix](../../operator-compatibility.md) is deferred.
`CopyPlan` does not retain enough metadata for CDC.
Extract relation OID, caller-established generation, qualified name, effective identity, ordered eligible columns, numeric type OIDs/typmods, and verified PK membership.
Reuse E1 validation and session settings, with version-aware catalog queries before claiming older-server coverage.
Do not infer historical relation identity from a fresh name lookup after DDL.

INSERT and UPDATE new values follow all persisted `Relation.columns` in order.
Old PK values follow the `key = true` subset of that same order, never index-column order.
Keep `Update.old = None` when the plugin omits `old-key`, then derive the predicate keys from the new values.
Reject missing, NULL, or unchanged-TOAST key evidence.
Future FULL images use all columns in that order and reconstruct omitted NULLs only with verified FULL metadata.
Use the existing `Value` variants without another canonical representation.

### E2: Decode, journal, and persistence experiment

- [ ] Integrate canonical contracts and the codec promotion after the C1 ownership handoff, with existing framing goldens unchanged.
- [ ] Write failing payload goldens and malformed-length/tag tests before implementing the event codec.
- [ ] Develop bounded metadata extraction and pure decoder tests independently against the frozen borrowed resolver.
- [ ] Test quoted names, embedded newlines, literal backslashes, NULL versus empty values, key changes, TOAST, split frames, and sink failure.
- [ ] Test checked position counters, one pending completion, and caller-supplied restart positions that differ from the last committed begin ordinal.
- [ ] After transport qualification, connect actual slot messages to canonical frames and the C2 journal/E3 recovery tests.
- [ ] Add tests `interleaved_xid_reuse`, `relation_generation_restart`, `split_event`, `truncated_tail`, and `corrupt_required_segment`.
- [ ] Assert unique `(StreamId, TxId, event_ordinal, part)` identity and exact replay order across restart.
- [ ] Check that the intended assertions fail before implementation.
- [ ] Implement the codec and `test_decoding` ingestion from a real source slot first.
- [ ] Implement pgoutput and wal2json adapters against the same event representation and plugin-specific transaction-end evidence.
- [ ] Test missing physical positions, retained BEGIN identity, malformed terminal messages, and missing envelope or transaction-end evidence.
- [ ] Test that keepalives, generic WAL end, origin positions, and row text containing COMMIT cannot complete a transaction.
- [ ] Test RPGSv2 presence flags, canonical absence, distinct `Some(0)`, version-1 refusal, and unchanged wire-byte accounting.
- [ ] Persist decoder context before acknowledging dependent input.
- [ ] Implement synchronized publication, transaction descriptors, pinned readers, and retirement synchronization.
- [ ] Exercise separate receive/apply/status processes with real locks and transactional control updates for each candidate.
- [ ] Run one transaction larger than memory and one larger than available spool capacity.
- [ ] Assert bounded memory, capacity failure without feedback advancement, live heartbeats, and no pruning deadlock.
- [ ] Compare segmented journal/catalog, redb-only, and SQLite FULL/WAL with identical frames and descriptors under V3.
- [ ] Record measured results and rejected candidates in `docs/performance.md` before choosing the production backend.

The segmented candidate initially uses the same vetted transactional catalog implementation as its control comparator.
That choice is an experiment input, not a production decision.
Pin experiment dependencies after graph, license, and security review.
Require cross-process checks before production selection.
Assert effective SQLite pragmas and redb lock outcomes instead of trusting configuration requests.
E2 is incomplete until a real source transaction can survive journal restart and replay through E3.
The pure decoder does not close that gate or establish source ACK authority.
C2 integration must recover relation context and replay identity, make required context durable before ACK, and test target effects after journal restart.
C1 and parser tests do not select a production storage backend.
PostgreSQL 9.6 through 10 cannot supply `test_decoding` TRUNCATE output, which remains an explicit qualification limitation.

The approved first pilot uses the [common contract](../../../bench/storage/config.rs) and [fixed workloads](../../../bench/storage/cases.json).
It permits one stream, one writer, one active source transaction, and synchronous closure-scoped storage batches.
Reopen requires an external `TxSeq` prefix, with zero meaning no completed transaction is required.
Canonical wire byte accounting includes frame headers and completed descriptors.
Unfinished transactions may span batches without becoming visible.
Errors poison a batch even when its callback swallows an error.
A bounded validation pass precedes callback emission during complete scans, and both passes count toward scan timing.
Every candidate exposes an explicit fallible checkpoint and reports queried settings or clearly labeled configured policy.
Each candidate/case/repetition runs in a fresh child process with paired seeded ordering and warm-after-write cache labeling.
This pilot leaves the multi-process, retirement, quota, source-decoding, and wider fault gates above unchecked.

### E3: Fenced target checkpoint and serial apply

- [ ] Add `lost_commit_reply`, `checkpoint_rollback`, `checkpoint_collision`, `wrong_primary`, and `missing_replay_input` tests.
- [ ] Assert that committed user changes and checkpoint progress either exist together or both roll back.
- [ ] Assert one committed target effect after reconnect when the first COMMIT reply was lost.
- [ ] Check expected failing assertions before implementation.
- [ ] Create or check owned logged metadata, target privileges, and the target writer fence.
- [x] Implement parameter binding and prepared reuse through the existing libpq boundary.
- [ ] Apply INSERT/UPDATE/DELETE/TRUNCATE with relation generations, replica identity FULL, NULL, unchanged TOAST, and key changes.
- [ ] Update one logged progress row per stream inside each applied transaction/group.
- [ ] Resolve `Unknown` by reacquiring the fence and reading the target PRIMARY checkpoint.
- [ ] Test nonempty checkpoints with absent physical positions separately from empty checkpoints and known physical zero.
- [ ] Test metadata-version refusal and NULL-safe compare-and-swap with optional physical positions.
- [ ] Replay only complete required input beyond that checkpoint, otherwise return a nonzero gap failure.
- [ ] Add logged COPY checkpoint recovery under a valid plan/snapshot and unlogged-table revalidation after target crash.
- [ ] Reconcile actual state for nontransactional phases, sequences, and vacuum.
- [ ] Check fingerprints and compare target effects against the serial fixture oracle.
- [ ] Run V3's end-to-end apply benchmark before optimizing scheduling.

Use one owned checkpoint table with run/source/database/generation identity, last XID, begin ordinal, applied order, optional physical LSN, and mandatory transaction-end LSN.
Retain barrier epoch and barrier counter in that table.
The approved extension uses target metadata version 2 and preserves the existing 19 SQL columns.
`source_commit_lsn` is already nullable, so this extension adds no SQL CHECK constraint or table-schema change.
For a nonempty checkpoint, XID, begin ordinal, and transaction-end position remain mandatory.
For an empty checkpoint, sequence is zero and all four transaction columns are NULL.
`Checkpoint.applied` remains a nonempty `Frontier`, while the internal control row and durable proof can carry `Option<Frontier>`.
Preserve the existing 26-parameter `IS NOT DISTINCT FROM` compare-and-swap.
Use the shared `src/state.rs` constant `TARGET_METADATA_VERSION = 2` with crate visibility as the canonical version.
The optional-commit-position slice preserved native format two.
The source-slot extension uses native `FORMAT_VERSION = 3`, while target metadata stays at version two.
A later logged COPY anchor needs native format four or a new explicitly versioned record; source-only format three must not be reinterpreted.
Refuse version-1 target objects or provenance without DDL, conversion, or adoption.
Increment the barrier counter in both synchronous data/checkpoint commits and standalone writing barriers so each proof has positive logged-write evidence.
Check exact object identity before querying, excluding, reowning, or removing metadata.
Store checkpoint provenance in the native manifest.
Do not substitute replication-origin progress for this checkpoint.
All supported trigger cases use serial execution until E7 proves a safe alternative.
Test transactional trigger tables separately from external side effects, which require documented idempotency.

We validate the dedicated backend, target writer fence, exact metadata, and complete cached control row at the owned transaction boundary.
We retain the metadata `SHARE ROW EXCLUSIVE` table lock and `FOR UPDATE` row lock until transaction end, so other sessions cannot change the checkpoint while rows are applied.
Prepared row execution checks the local PostgreSQL transaction state before and after dispatch.
Every error poisons the owned transaction, and an unfinished transaction closes its connection.
We retain validation before checkpoint compare-and-swap, after deferred constraints, and immediately before `COMMIT`.
We still require a dedicated backend and namespace DDL quiescence.
Transaction and statement pooling remain unsupported, and these checks do not certify arbitrary failover.
Static call counts are seven RPCs for begin, fifteen for commit, and one per warm prepared DML execution, excluding preparation, deallocation, preflight, and provisioning.

> [!important]
> A metadata or fence violation during a transaction can permit further uncommitted eligible DML before the boundary rejects `COMMIT`.
> This timing preserves transactional logged effects under the trusted DML profile.
> External side effects require separate idempotency and are outside this timing equivalence.

### E4: Durability profiles and aggregate reservations

The catalog worker owns the following atomic run-wide operations in `src/state/risk.rs`.
The apply coordinator calls them through the durable catalog, including from standalone processes.

```rust
struct ReservationId([u8; 16]);
struct Reservation { id: ReservationId, tx: CompletedTx, epoch: u64 }
struct StreamRisk { stream: StreamId, d: Option<Frontier>, h: Option<Frontier> }
struct RiskSnapshot {
    epoch: u64, streams: Vec<StreamRisk>,
    transactions: u64, bytes: u64, caps: LossCaps,
}
struct RiskLedger;
impl RiskLedger {
    fn open(store: &Store, caps: LossCaps) -> Result<Self>;
    fn reserve(&mut self, tx: &CompletedTx, epoch: u64) -> Result<Reservation>;
    fn release(&mut self, proof: &DurableProof) -> Result<()>;
    fn snapshot(&self) -> Result<RiskSnapshot>;
}
```

- [ ] Test absent/invalid caps, overflow, one oversized transaction, and simultaneous reservations from two databases and two processes.
- [ ] Assert combined reservation count and bytes never exceed their explicit caps.
- [ ] Kill a worker after reservation and assert that its capacity remains charged until durable reconciliation.
- [ ] Check expected failures before implementing the profile coordinator.
- [ ] For `durable`, synchronize journal and target before source release.
- [ ] For `replay-safe`, synchronize input/context before positive asynchronous target COMMIT permits source release.
- [ ] Retain journal until a logged synchronous writing barrier returns a durable proof.
- [ ] For `relaxed`, durably reserve D/H/epoch/count/bytes before speculative target work and ACK.
- [ ] Allow feedback only after positive target COMMIT and only within its existing reservation.
- [ ] Force oversized transactions through durable target completion before releasing source WAL.
- [ ] Release budget only after target durability and the durable catalog update of D and counters.
- [ ] Test barrier failure, wrong database proof, read-only COMMIT, and asynchronous checkpoint visibility as insufficient durability evidence.
- [ ] Test all mode transitions through a quiesced durable boundary across every database.
- [ ] Refuse uncertain epochs with missing input rather than relabeling them or accepting loss with success.
- [ ] Publish complete durable receipts only after every required database and phase drains and synchronizes.
- [ ] Run the fault matrix and publish profile tradeoffs and recovery instructions before accepting mode performance results.

Initialize each stream's D/H from explicit frontiers, with `None` representing an empty prefix.
Do not infer a populated prefix from zero-initialized memory.
`reserve` checks both limits with checked arithmetic and persists the reservation before returning it.
Zero caps permit no speculative reservations.
`release` is idempotent and cannot release another stream's reservation.
Keep failure capacity charged when feedback delivery is uncertain.
Expose reserved risk separately from actual sent feedback.

## Bounded replay from the initial source point

We select an internal Gate 3 qualification slice that retains an immutable first context beside the current context.
CP is the actual nonzero slot `consistent_point` in the source receipt, with exact matching in the bound snapshot anchor.
It is not a fabricated transaction commit position.
The first context CAS requires a verified empty receive head and the actual fresh, pre-input Idle decoder state.
That CAS writes identical first and current context bytes at revision 1.
Keep the first bytes unchanged through every later update.
Read both contexts and the receive head in one checked snapshot.

Preserve the full source, records, COPY-plan, and limits bindings and the actual independent decoder seeds.
Do not derive earlier replay identities from post-Idle counters, the last completed transaction, or an individual LSN comparison.
Replay the full retained prefix from CP through a comparison sink, including every frame and completion and transactions with no events.
Compare full identities, canonical bytes, and completion descriptors.
Before issuing a source resume proof or authorizing dependent DML or feedback, require exact agreement on Idle counters, last source end, and receive head.
Preserve buffered input after the reconciled prefix.

The native context catalog uses fresh `user_version = 3` and head layout 1 and refuses version 2 without adoption or conversion.
The standalone layout, outer native format 5, target metadata version 3, `RPGEXP02` wire format, and ACK mode policy remain unchanged.
Retain all journal history and source WAL needed from CP; do not prune this slice.
The source confirmed position and any sent feedback must stay at or below CP.

> [!important]
> Set explicit finite runtime, physical WAL-distance, and disk budgets before any Gate 3 PostgreSQL qualification.
> Stop and refuse when a budget is exhausted or required WAL is lost.
> Retaining the original point makes WAL retention grow with subsequent source traffic; this slice does not provide an advancing replay window or general crash resume.

- [ ] Freeze and review the immutable-first/current catalog implementation and typed adapter.
- [ ] Verify atomic revision-1 initialization, immutable first bytes, exact bindings, and one-snapshot context/head reads.
- [ ] Verify complete-prefix comparisons, zero-event completions, independent seeds, and preservation of subsequent input.
- [ ] Set and test all three finite budgets, lost-WAL refusal, and the CP feedback bound before live qualification.
- [ ] Release the source consumer only after its proof ordering and retained pre-boundary data are sufficient.

## Verification and commit gate

Extend the E0 Taskfile tasks only through the integration worker.
The verification worker defines `task test-cdc`, `task test-faults`, and `task bench-apply` before they appear in runnable guides.
Tests require exact fixture inventories, observed injection, independent fingerprints, and positive target commit evidence.
Run format, lint, unit, database, CDC, and relevant fault checks before requesting a commit.
Record actual commands, case counts, server versions, and results in `tasks/todo.md`.
Storage throughput alone cannot close this milestone.

## Remaining engine feature map

Expand each task immediately before its implementation review.
Keep unfinished commands explicit in generated compatibility coverage.

- E5, `src/cli/` and `src/config.rs`: all registrations/aliases/flags, short options, `PGCOPYDB_*`, precedence, output shapes, and exit categories.
- E5, `src/control.rs`: standalone snapshot/receive/prefetch/catchup/apply, phase composition, SQL stdin, export/debug, and bounded versioned control messages.
- E6, `src/schema.rs`: pg_dump/pg_restore selection, version checks, roles, extensions, ACL/ownership, indexes, constraints, and deferred indexes.
- E6, `src/catalog/` and `src/snapshot.rs`: filters, partitions, consistent snapshots, binary fallback, supported RLS/trigger cases, and non-UTF8 encodings.
- E6, `src/blobs.rs` and `src/sequences.rs`: large objects, sequence state, nontransactional reconciliation, and independent fingerprints.
- E6, `src/migration.rs`: all-databases scheduling with shared worker/connection/byte budgets and run-wide risk accounting.
- E6, `src/compare.rs` and `src/progress.rs`: exact owned-object exclusions, legacy output definitions, native frontiers, and complete durable receipts.
- E7, `src/apply/scheduler.rs`: conservative dependency detection, serial barriers, bounded queues, ordered commit permission, and rollback of later uncommitted work.
- E7, `src/apply/batch.rs`: measured prepared reuse, cache reuse, parameter binding, bounded pipelines, and semantics-preserving bulk DML.

E7 must test repeated keys, key changes, NULL/TOAST, generated columns, partition movement, cross-table foreign keys, ALWAYS triggers, cycles, and cancellation.
Require differential and adversarial scheduling tests before raising apply concurrency above `1` in a production profile.

### First E7 unit: text-session setup

We approved a bounded refactor on 2026-11-08, using the measured E3 source173 `8b6e0cd5` as the private baseline.
Keep the existing ordered eight settings as one source of truth, use bound parameters through the existing prepare and execute calls, and check all eight values with a separate fixed readback SELECT.
These three dispatches preserve independent readback and remove 26 structural dispatch cycles per measured transaction across the source and target paths; this is not a measured speedup.
Before writing code, verify every setting's actual transaction context and stop if SET LOCAL or transaction-control semantics are unresolved.
Preserve all source and fence checks, checkpoint behavior, BEGIN/COMMIT order, synchronous target commit, close/error behavior, and exporter readiness.
Do not add transport APIs, caches, interpolated setting values, production options, formats, or dependencies.
Test each caller and peer, all eight wrong or missing readback values, and closed/error paths before a fresh independent safety review.
After that review, compare the same 24 release-mode cases with unchanged workload, proof, resource, and cleanup requirements under a new exclusive benchmark lease.

## Self-review

E2 covers persistence selection, journal recovery, context, reader pins, capacity, and process topology.
E3 covers checkpoint authority, target ownership, serial apply, and actual applied throughput.
E4 covers all profile resolutions, global reservations, durability proofs, and final success.
E5-E7 retain the selected operator/general-use contract and its complete engine semantics; unused CLI-family expansion is deferred.
Shared transaction identity, frontier, reader, checkpoint, and risk types use the same names across tasks.
The production backend remains a measured decision and cannot block the already approved E0/E1 slice.
