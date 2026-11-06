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

The integration worker owns new shared declarations in `src/cdc/contracts.rs` and changes to root manifests/modules.
The decoder worker owns `src/cdc/decode.rs`, `src/cdc/codec.rs`, and `src/cdc/plugins/`.
The storage worker owns `src/state/journal.rs`, `src/state/catalog.rs`, `src/state/recovery.rs`, and private candidate modules.
The apply worker owns `src/apply/serial.rs`, `src/apply/checkpoint.rs`, and `src/apply/recovery.rs`.
The durability worker owns `src/state/risk.rs`, `src/apply/durability.rs`, and `src/cdc/feedback.rs` after E3.
The transport worker owns prepared/parameter execution additions under `src/pg/`.
The verification worker owns `tests/cdc/`, `tests/faults/`, `bench/`, and fixture tooling.
The documentation worker owns `docs/durability.md`, `docs/recovery.md`, and `docs/performance.md`.
Do not let multiple workers edit a shared declaration or manifest at once.

E2 requires E1's connection and native-state ownership.
E3 requires complete transaction readers from E2 and prepared execution from the transport worker.
E4 requires E3 checkpoint authority and the run-wide catalog transaction primitive from E2.
Storage and decoder workers may proceed together after the shared contract review.
Benchmark workload generation can proceed while these workers implement their modules.

## Shared interfaces for E2-E4

These are declarations for the next milestone's review, not changes to the frozen E0/E1 interfaces.
Reuse E0's `Result`, `RunId`, `PgConnection`, and `IoControl`.
Use crate-private plain fields and opaque storage/transport handles.
Require the leader to accept these declarations before parallel E2 implementation.

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
    tx: TxId, order: TxSeq, commit: CommitLsn, end: SourceEndLsn,
    event_count: u64, encoded_bytes: u64,
}
struct Frontier { order: TxSeq, commit: CommitLsn, end: SourceEndLsn }
enum Plugin { Pgoutput, TestDecoding, Wal2Json }
enum JournalWrite { Synchronized, Buffered }
enum TargetCommit { Synchronous, Asynchronous }
struct LossCaps { transactions: u64, bytes: u64 }
enum Profile { Durable, ReplaySafe, Relaxed(LossCaps) }
```

`TxSeq` is local source commit order within a stream, not a WAL position.
Reserve `TxSeq(0)` and require each completed transaction's sequence to follow its predecessor exactly.
Commit and transaction-end LSNs are nondecreasing and may be equal; they are not unique event identities.
`encoded_bytes` counts the complete canonical wire representation before optional storage compression: each frame's 88-byte header and payload plus one 128-byte completion descriptor.
Include relation context required for replay in the complete transaction's encoded accounting.
Frames can split one event across segments.
Keep one oversize event bounded by the documented exception and never collect a whole transaction in memory.
Relation definitions outside source transactions must attach to the first dependent transaction's recoverable context.
Version the codec and reject unknown required tags, broken ordinals, or conflicting relation generations.
`TailPolicy::Unpublished` applies only to an externally proven unpublished suffix; codec tolerance does not authorize pruning.
Persist source identity and generation before decoding can publish complete transactions.

```rust
struct Decoder;
impl Decoder {
    fn new(plugin: Plugin, stream: StreamId, frame_bytes: usize) -> Result<Self>;
    fn feed(&mut self, input: &[u8],
            emit: &mut dyn FnMut(Frame) -> Result<()>) -> Result<()>;
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
struct Checkpoint { stream: StreamId, applied: Frontier, barrier_epoch: u64 }
struct DurableProof;
struct PreparedApply;
enum CommitOutcome { Acknowledged(Checkpoint), Unknown }
struct Applier;
impl Applier {
    fn acquire(connection: PgConnection, stream: StreamId, state: &mut NativeState,
               io: &IoControl) -> Result<Self>;
    fn checkpoint(&mut self, io: &IoControl) -> Result<Option<Checkpoint>>;
    fn prepare(&mut self, tx: &ReadyTxn, input: &mut TxReader,
               io: &IoControl) -> Result<PreparedApply>;
    fn commit(&mut self, tx: PreparedApply, mode: TargetCommit,
              io: &IoControl) -> Result<CommitOutcome>;
    fn barrier(&mut self, through: &Frontier, epoch: u64,
               io: &IoControl) -> Result<DurableProof>;
}
```

`Store::open` uses the candidate recorded in native experiment configuration, then the selected production backend after measurement.
Its input is E1's checked `NativeState`, and it must not reopen foreign paths.
E2 replaces E1's exclusive whole-run access with role/stream locks for the approved overlapping-process topology.
Keep initial identity/publication operations exclusive and progress reads read-only.
`Buffered` completion is process-local until data and references become durable.
Never publish durable metadata references to unsynchronized journal files.
Readers pin required segments until drop.
`DurableProof` is opaque and can originate only from the fenced primary's positive synchronous writing-barrier result.
It carries exact stream, covered frontier, epoch, and target identity.
Target-local WAL samples are diagnostics, never source feedback values.

### E2: Decode, journal, and persistence experiment

- [ ] Add tests `interleaved_xid_reuse`, `relation_generation_restart`, `split_event`, `truncated_tail`, and `corrupt_required_segment`.
- [ ] Assert unique `(StreamId, TxId, event_ordinal, part)` identity and exact replay order across restart.
- [ ] Check that the intended assertions fail before implementation.
- [ ] Implement the codec and `test_decoding` ingestion from a real source slot first.
- [ ] Implement pgoutput and wal2json adapters against the same event representation and plugin-specific transaction-end evidence.
- [ ] Persist decoder context before acknowledging dependent input.
- [ ] Implement synchronized publication, transaction descriptors, pinned readers, and retirement synchronization.
- [ ] Exercise separate receive/apply/status processes with real locks and transactional control updates for each candidate.
- [ ] Run one transaction larger than memory and one larger than available spool capacity.
- [ ] Assert bounded memory, capacity failure without feedback advancement, live heartbeats, and no pruning deadlock.
- [ ] Compare segmented journal/catalog, redb-only, and SQLite FULL/WAL with identical frames and descriptors under V3.
- [ ] Record measured results and rejected candidates in `docs/performance.md` before choosing the production backend.

The segmented candidate initially uses the same vetted transactional catalog implementation as its control comparator.
That choice is an experiment input, not a production decision.
Pin experiment dependencies after graph, license, and security review; require cross-process checks before production selection.
Assert effective SQLite pragmas and redb lock outcomes instead of trusting configuration requests.
E2 is incomplete until a real source transaction can survive journal restart and replay through E3.

The approved first pilot uses the [common contract](../../../bench/storage/config.rs) and [fixed workloads](../../../bench/storage/cases.json).
It permits one stream, one writer, one active source transaction, and synchronous closure-scoped storage batches.
Reopen requires an external `TxSeq` prefix, with zero meaning no completed transaction is required.
Canonical wire byte accounting includes frame headers and completed descriptors; unfinished transactions may span batches without becoming visible.
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
- [ ] Replay only complete required input beyond that checkpoint, otherwise return a nonzero gap failure.
- [ ] Add logged COPY checkpoint recovery under a valid plan/snapshot and unlogged-table revalidation after target crash.
- [ ] Reconcile actual state for nontransactional phases, sequences, and vacuum.
- [ ] Check fingerprints and compare target effects against the serial fixture oracle.
- [ ] Run V3's end-to-end apply benchmark before optimizing scheduling.

Use one owned checkpoint table with run/source/database/generation identity, applied order, commit LSN, transaction-end LSN, and barrier epoch.
Add a barrier counter that changes on every barrier so it always writes logged WAL.
Check exact object identity before querying, excluding, reowning, or removing metadata.
Store checkpoint provenance in the native manifest.
Do not substitute replication-origin progress for this checkpoint.
All supported trigger cases use serial execution until E7 proves a safe alternative.
Test transactional trigger tables separately from external side effects, which require documented idempotency.

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

## Verification and commit gate

Extend the E0 Make targets only through the integration worker.
The verification worker defines `make test-cdc`, `make test-faults`, and `make bench-apply` before they appear in runnable guides.
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

## Self-review

E2 covers persistence selection, journal recovery, context, reader pins, capacity, and process topology.
E3 covers checkpoint authority, target ownership, serial apply, and actual applied throughput.
E4 covers all profile resolutions, global reservations, durability proofs, and final success.
E5-E7 retain the rest of full CLI and engine scope without speculative implementation bodies.
Shared transaction identity, frontier, reader, checkpoint, and risk types use the same names across tasks.
The production backend remains a measured decision and cannot block the already approved E0/E1 slice.
