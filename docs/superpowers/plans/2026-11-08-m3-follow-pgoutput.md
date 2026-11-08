# M3 Follow and pgoutput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.
> The approved team workflow uses a sequential interface freeze, disjoint parallel build owners, and one independent milestone-closing review.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run bounded, recoverable native follow from an owned slot-exported initial snapshot through durable, ordered pgoutput application on PostgreSQL 16 to 16, 18 to 18, and 16 to 18.

**Architecture:** The source slot retains input until the logged target checkpoint authorizes feedback.
Receive and apply use separate threads, construct their own libpq connections, and exchange bounded ordered messages and opaque durability proofs.
The initial-copy coordinator reuses M2 schema/COPY machinery with the slot snapshot; M4 owns final drain, control commands, and cleanup.

**Tech Stack:** One Rust Cargo package, existing stock libpq transport and pipeline, PostgreSQL pgoutput protocol v1, existing schema utilities, and the existing Python database/container harnesses.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md), [roadmap decisions and M3](../../roadmap.md), [native protocol](../../native-protocol.md), and the [M2 frozen handoff](2026-11-08-m2-clone-slice.md#frozen-shared-handoff-2026-11-08).
The roadmap's dated amendments override older design text.
The [M6 evidence boundary](2026-11-08-m6-native-adapter.md#task-3-qualify-the-independent-target-evidence-boundary) constrains the M4 handoff below.

## Global Constraints

- Keep one Cargo package and the `rpgmigrate` executable.
- Preserve the pinned fixes-branch parser baseline and explicit native refusals.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources.
- Use source-retained WAL with no local CDC journal or copy-phase spool.
- Negotiate exactly `proto_version '1'` and `publication_names`; do not enable binary, streaming, two-phase, or failover-slot options.
- Admit source and target majors 16 and 18 with source major no greater than target major.
- Keep `durable` as the default; defer replay-safe runtime qualification and cadence selection to M9.
- Keep `relaxed` refused until its separately approved paired benchmark gate passes.
- Keep large objects, binary COPY, split COPY, filters, all-database variants, and other unqualified M8 features explicitly refused before mutation.
- Require source namespace/table DDL quiescence and exact owned target identities throughout the operation.
- Keep credentials, conninfo, row contents, and server DETAIL out of diagnostics and recorded fixtures.
- Preserve M2 changes and existing native metadata provenance, exact `copy_done` table/index OIDs, writer fencing, cancellation, and unknown-COMMIT handling.
- Verify a feature branch before implementation; do not commit, push, publish, or deploy without the leader's instruction.

## Review Focus

- A blocked apply thread must leave receive able to send bounded-cadence status without advancing feedback; Task 3 tests a full channel for longer than `wal_sender_timeout`.
- Reconnection must not accept a different transaction at the same end LSN; Tasks 1 and 4 test canonical source identity and strict duplicate handling.
- Keyless FULL rows, NULLs, and unchanged TOAST must preserve row multiplicity and values; Task 5 tests these against the serial reference.
- A pre-anchor crash must not truncate an existing or replaced table; Tasks 4 and 6 require the complete recorded-owned workset before restart.
- An idle source or filtered WAL must not strand safe retention progress or certify pending DML; Tasks 3 and 4 test ordered keepalive certification, including a keepalive inside an active transaction.

## Starting Point and Milestone Boundary

The implementation starts from M2's native-state format 5 and target metadata version 4.
The existing control row has 24 columns, `clone_control`, `certified_lsn`, optional snapshot anchor, and exact target provenance.
`TargetAuthority::begin(next: Frontier, io: &IoControl)` requires a complete frontier before DML, while the new receiver learns the actual transaction-end LSN only at COMMIT.
Preserve that API for its existing callers and add the streaming/group API only with its M3 consumer.

Retire `cdc/codec.rs`, `cdc/event_codec.rs`, `cdc/decode.rs`, and `cdc/plugins/test_decoding*` after their real consumers and required test registrations move to pgoutput.
Replace the replay half of `apply/serial.rs`; keep a serial execution mode over the same decoded pgoutput events as the differential reference.
Reuse `cdc/contracts.rs`, `identity.rs`, `apply/plan.rs`, checkpoint CAS/barriers, and `pg/*` rather than adding another transport or persistence layer.
Do not delete the parser's alternate plugin names or turn their old bounded fixtures into native follow support claims.

M3 proves ongoing catch-up, feedback, and restart safety.
A running follow command reports `streaming`; cancellation is a non-successful suspended/interrupted outcome.
M3 does not emit `drained`, native follow `completed`, `cutover.v1`, or `cleanup.v1`.
M4 supplies the requested-end boundary, final synchronization, proof, successful follow exit, and owned cleanup.

## Shared Interface Freeze

The leader assigns Task 1 first and records the accepted signatures before parallel source edits.
Owners may then change implementation bodies within their files; a shared signature change returns to the leader before consumers continue.
Do not install dependencies or introduce placeholder APIs during the freeze.

### Source-derived transaction identity

Keep the current `Frontier`, `TxId`, `TxSeq`, `CommitLsn`, and `SourceEndLsn` representation to limit changes to checkpoint consumers.
For the new versioned pgoutput profile, `TxId.begin_ordinal` contains the numeric source `BEGIN.final_lsn`, not a receive counter.
Require nonzero `BEGIN.final_lsn == COMMIT.commit_lsn`, and require `COMMIT.end_lsn >= COMMIT.commit_lsn`.
The identity compared during duplicate handling is stream, xid, this source-derived begin value, commit LSN, end LSN, and stored transaction order.
Do not generate a second independently persisted transaction counter for identity.
`TxSeq` remains the contiguous target-checkpoint commit order, initialized from the last checkpoint and incremented only for a new committed source transaction.

PostgreSQL's nonstreaming commit path sets `txn->final_lsn` to `commit_lsn` before replay, and its writers serialize that value in B and the same commit LSN plus a distinct end LSN in C.
Verify the exact runtime fixtures against the official [PG16 reorder buffer](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/replication/logical/reorderbuffer.c), [PG18 reorder buffer](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/reorderbuffer.c), [PG16 protocol writer](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/replication/logical/proto.c), and [PG18 protocol writer](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/replication/logical/proto.c).
Reject old persisted formats without conversion; their receiver-counter semantics cannot authorize pgoutput replay.

### Message and tuple boundary

Keep `Event` and `RelationKey` as the DML boundary.
Change old tuples to `OldTuple { kind: OldTupleKind, values: Vec<Value> }`, where `OldTupleKind` is `Key` or `Full`.
`Event::Update.old` becomes `Option<OldTuple>` and `Event::Delete.old` becomes `OldTuple`.
Retain the relation's full positional tuple width for both K and O, including non-key placeholders; never reinterpret K as a compact vector of key values.
Keep values redacted in `Debug`.

Define these crate-private messages in `cdc/contracts.rs`:

```rust
struct Begin { xid: u32, final_lsn: CommitLsn, commit_time_micros: i64 }
struct Commit { xid: u32, commit: CommitLsn, end: SourceEndLsn,
    commit_time_micros: i64 }
enum FollowMessage {
    Begin(Begin),
    Event(Event),
    Commit(Commit),
    Keepalive { wal_end: SourceEndLsn, decoder_idle: bool },
}
```

Origin/type messages are validated and applied to bounded decoder metadata; they do not create a source frontier or target DML.
Keep BEGIN visible to apply so M4 can later use its exact `final_lsn` as a coverage witness.
Keepalive messages share the same FIFO as DML, so their ordering cannot bypass queued transactions.
The checked `Commit` repeats the active xid from B because C itself does not carry an xid; a missing active B is an error.

Freeze these signatures:

```rust
cdc::pgoutput::Decoder::new(stream: StreamId, binding: &FollowBinding, limits: FollowLimits)
    -> Result<Decoder>
Decoder::feed(&mut self, payload: &[u8], emit: &mut dyn FnMut(FollowMessage) -> Result<()>)
    -> Result<()>
Decoder::is_idle(&self) -> bool
follow::run(options: FollowOptions, io: &IoControl) -> Result<String>
snapshot::FollowSnapshot::create(options: &FollowOptions, plan: &ClonePlan,
    native: &mut NativeState, io: &IoControl) -> Result<FollowSnapshot>
FollowSnapshot::snapshot_mut(&mut self) -> &mut CloneSnapshot
FollowSnapshot::receipt(&self) -> &SourceSlotReceipt
FollowSnapshot::finish(self, io: &IoControl) -> Result<()>
apply::follow::run(options: &FollowOptions, binding: &FollowBinding,
    native: &mut NativeState, source_confirmed_flush: SourceEndLsn,
    ready: std::sync::mpsc::SyncSender<RecoveryStart>, input: &FollowReceiver,
    feedback: &FeedbackSender, io: &IoControl) -> Result<()>
cdc::receive::run(options: &FollowOptions, binding: &FollowBinding,
    start: RecoveryStart, output: &FollowSender, feedback: &FeedbackReceiver,
    io: &IoControl) -> Result<()>
```

`FollowOptions { clone: CloneOptions, slot_name: String, publication_name: String, origin_name: Option<String>, apply_group_txns: u64, limits: FollowLimits }` contains only effective admitted settings.
`FollowLimits` has `channel_bytes`, `channel_messages`, `max_message_bytes`, `group_rows`, and `group_bytes`, all positive `usize` fields with the values below.
The default plugin is pgoutput; other effective plugins are an explicit refusal.
`--origin` is recorded as unowned and never creates an origin.
When no explicit slot/publication name is supplied, derive native names from the run ID and copy generation, within PostgreSQL's name limits; a supplied name still requires absence followed by this run's positive creation receipt.
A collision never authorizes adopting or dropping an existing resource.
`PublicationReceipt { name: String, oid: u32, owner_oid: u32 }` records the exact positively created publication.
`FollowBinding` contains `slot: SourceSlotReceipt`, `publication: PublicationReceipt`, `source_timeline: u32`, `copy_generation: CopyGeneration`, and `source_types: Vec<SourceType>`, without a connection handle.
`SourceType { oid: u32, schema: String, name: String }` maps source custom OIDs to qualified names captured under the slot snapshot; the stream comes from the slot binding.
The source owner publishes the bounded type map before apply starts and the decoder validates Y against it through the immutable follow binding.
The source type map and full follow binding are bounded/validated in native state and in the target `follow_binding`; they are never accepted from an unbound report.
`RecoveryStart { stream: StreamId, applied: Option<Frontier>, certified_lsn: Option<SourceEndLsn>, anchor: Option<SnapshotAnchor>, start_lsn: SourceEndLsn, confirmed_flush_lsn: SourceEndLsn }` contains the barrier-proven recovery state.
Its fields and constructor remain private to the authority/recovery owner; expose read-only accessors to receive.
The coordinator captures source confirmed flush and exact resource health, releases its COPY target sessions, then starts apply.
Apply constructs/reacquires its target connection and sends one `RecoveryStart` only after the fenced recovery barrier and gap comparison pass.
The coordinator starts receive from that readiness result; a failed/closed readiness channel cannot start replication.
Channel sender/receiver types belong to the receiver owner and wrap standard-library synchronization, with no new dependency.
Freeze `channel::bounded(FollowLimits) -> Result<(FollowSender, FollowReceiver)>`, `FollowSender::try_send(FollowMessage) -> std::result::Result<(), std::sync::mpsc::TrySendError<FollowMessage>>`, and `FollowReceiver::recv_timeout(Duration) -> std::result::Result<FollowMessage, std::sync::mpsc::RecvTimeoutError>`.
`FeedbackSender::publish(SafeFeedback) -> Result<()>` replaces a one-entry mailbox only with a same-stream nondecreasing proof; `FeedbackReceiver::take() -> Option<SafeFeedback>` consumes its latest proof.
The receiver owner creates the paired mailbox handles with `channel::feedback() -> (FeedbackSender, FeedbackReceiver)`.

### Authority and pipeline boundary

Add the following methods in `apply/checkpoint.rs` with their real consumer in Task 5:

```rust
TargetAuthority::begin_group(&mut self, io: &IoControl) -> Result<TargetTxn<'_>>
TargetTxn::advance(&mut self, next: Frontier) -> Result<()>
TargetTxn::pipeline(&mut self, io: &IoControl,
    send: impl FnOnce(&mut crate::pg::Pipeline<'_>) -> Result<()>,
    validate: impl FnMut(usize, crate::pg::PgResult) -> Result<()>) -> Result<()>
TargetTxn::commit_pipeline(self, io: &IoControl,
    send: impl FnOnce(&mut crate::pg::Pipeline<'_>) -> Result<()>,
    validate: impl FnMut(usize, crate::pg::PgResult) -> Result<()>) -> Result<CommitOutcome>
TargetAuthority::certify(&mut self, candidate: SourceEndLsn,
    io: &IoControl) -> Result<DurableProof>
TargetAuthority::install_follow_anchor(&mut self, native: &NativeState,
    receipt: &SourceSlotReceipt, io: &IoControl) -> Result<DurableProof>
```

`begin_group` reserves a fenced transaction without a prospective frontier; its warm path queues BEGIN with the first pipeline window instead of waiting separately.
The transport owner re-exports the existing `ffi::pipeline::Pipeline` through `pg`, without adding another transport implementation.
`advance` seals each complete source C into that transaction, validating contiguous order and strictly increasing end LSNs.
`commit_pipeline` requires at least one sealed source transaction and no open source transaction, queues the final pending DML window and one exact checkpoint CAS using the final sealed frontier, checks all results through pipeline sync, then calls `PgConnection::commit_observed(&IoControl) -> Result<CommitObservation>` outside pipeline mode.
It increments the logged barrier counter and returns the existing `CommitOutcome`; normal durable group completion needs no second COMMIT.
Keep the last bounded DML window pending until group closure so DML and CAS share that final pipeline synchronization.
The warm group path does not reread/reverify catalog or checkpoint state per transaction/group: exact CAS and the continuously held fence protect the cached predecessor; reconnect revalidates it.
`begin(next, io)` and its existing `commit` retain their known-frontier behavior through the shared validation path.
The authority owner also freezes `TargetTxn::preflight_follow_relation(&mut self, expected: &Event, binding: &FollowBinding, owned: &OwnedCopyTable, io: &IoControl) -> Result<FollowRelation>` with the catalog owner.
`FollowRelation { relation: Event, target_type_oids: Vec<u32>, override_identity: bool }` contains validated published columns in source order, the corresponding target parameter OIDs, and the explicit identity-write requirement.
The DML owner extends `DmlPlan::new(relation: &FollowRelation, row: &Event) -> Result<DmlPlan>` and uses the existing bind/shape-cache pattern; no plan borrows evictable relation storage.
The pipeline wrapper latches failure before invoking caller closures and keeps the transaction uncommittable after any error or caught panic.
Do not expose an unchecked connection from the authority.

Keep the existing pipeline bounds of 64 commands, one MiB input, and sixteen MiB returned-result budget per pipeline window.
Account for BEGIN/checkpoint commands when they share a window.
A group may require several bounded windows.
The existing observed-COMMIT helper sends `SET LOCAL client_min_messages = warning` and COMMIT as separate query/drain operations; include that guard and all required deferred-effect/authority checks in round-trip measurements.
Do not claim the design's two-round-trip objective has been achieved unless a separately tested helper optimization preserves both result validation and warning visibility.
Every general pipeline window rejects a server COMMIT command tag; neither a callback nor raw pipeline SQL may bypass the observed-COMMIT boundary.
Validate every command and terminal result before the observed-COMMIT call; ordinary warm DML has no per-row waits, with the bounded oversize-row exception below.
Force deferred effects with `SET CONSTRAINTS ALL IMMEDIATE`, validate their checkpoint/session consequences, and leave no trigger or callback able to hide warnings after the helper establishes warning visibility.
Only `CommitObservation::Committed` can create durable proof; `RolledBack` is failure and `Unknown` requires fenced recovery, including COMMIT plus Idle after a synchronous-wait cancellation warning.

`apply::checkpoint::SafeFeedback` has private fields and no serde/deserialization constructor.
Its read-only accessors are `stream(&self) -> StreamId` and `end(&self) -> SourceEndLsn`.
Only the authority can construct it from a positive durable applied-COMMIT proof or a positively committed keepalive certification.
Extend the private durable proof evidence to retain the exact certified state it proved, and add `DurableProof::feedback(&self) -> Option<SafeFeedback>`, selecting the proved maximum of applied end and certified LSN while excluding the copy anchor.
A follow applied frontier always has `commit: Some(CommitLsn)` and the versioned source-derived begin value; missing commit identity is a refusal.
It carries stream identity and one authorized source end, and its accessor supplies the source positions to the receive status builder.
A COPY anchor may set the reconnect starting point; initial status may remain at the captured `confirmed_flush_lsn` until actual applied/certified proof exists.
Received frame positions, later BEGINs, channel enqueue, empty libpq output, and target row visibility never construct feedback authority.

### Finite working budgets

Use the existing pipeline limits and the M2 sixteen-MiB permitted row/frame limit.
Start the FIFO with an eight-MiB aggregate owned-capacity budget and 1024-message ceiling, admitting at most one oversize message up to sixteen MiB while the FIFO is otherwise empty.
Charge allocated capacities, message/tuple metadata, and relation context, with overflow-safe arithmetic.
Keep at most one pending decoder emission outside the FIFO and include it, the raw libpq frame, libpq/socket buffers, and pipeline results in the memory statement.
These limits bound application-owned buffering, not total process RSS.

Close an apply group at a complete source transaction boundary when input empties or one of these caps is met: the effective transaction cap (default 100), 10,000 row events, or eight MiB decoded/bound input.
A source transaction that exceeds a group cap runs alone and streams through repeated bounded pipeline windows; never split its target atomicity or buffer its whole contents.
A frame over sixteen MiB is an explicit supported-size refusal with no feedback advancement.
A single bound row above the one-MiB pipeline input limit flushes the pending window and uses the existing checked single prepared execution, within the sixteen-MiB row limit; measure that explicit oversize-row exception separately.
Bound relation metadata to the admitted 4096-table plan and one MiB inventory, and prepared statements to the existing 1024-entry limit with byte accounting and eviction.
Expose the effective limits in help and a safe structured startup log, without extending M1's strict report payload or claiming that M9 has tuned them.

## Disjoint Build Owners

1. Shared/coordinator: `src/{contracts,config,cli,lib,clone,protocol,progress}.rs`, new `src/follow.rs`, and `src/cdc/contracts.rs`.
   Own option admission, shared DTOs, root registrations, follow orchestration, producer reports, and capability truthfulness.
2. Decoder: `src/cdc/{mod,pgoutput}.rs`, new `src/cdc/pgoutput/**`, decoder fixtures under `tests/fixtures/cdc/pgoutput/**`, and removal of old codec/decode/event-codec/plugin files.
   Exclude `src/cdc/contracts.rs` and new `src/cdc/receive.rs`.
3. Receiver/transport: `src/cdc/receive.rs`, new `src/follow/channel.rs`, `src/pg/**`, and receiver-specific scripted-peer fixtures under `tests/support/follow_receive*`.
   Own byte/FIFO accounting, receive/status scheduling, and transport support required to send status while reads are paused.
4. Authority/state: `src/{state,identity}.rs`, `src/state/**`, `src/apply/{mod,checkpoint,metadata}.rs`, and `src/apply/checkpoint_live_tests.rs`.
   Own source-resource intent receipts, format changes, restart/anchor authority, canonical dedupe, logged certification, and proof constructors.
5. DML/apply: `src/apply/{serial,plan}.rs`, their test subtrees, and new `src/apply/follow.rs`/`src/apply/follow/**`.
   Own prepared caches, streaming group execution, row identity, and differential execution.
6. Source initial copy/catalog: `src/catalog.rs`, `src/catalog/**`, `src/{snapshot,snapshot_live_tests,schema,copy}.rs`, `src/copy/**`, and new `src/catalog/follow.rs`/`src/snapshot/follow_tests.rs`.
   Own publication/slot acquisition effects, retention admission, follow relation catalog validation, and reuse of schema/COPY with the slot snapshot.
7. Harness/docs: `Taskfile.yml`, `tools/test-db.py`, `tools/ci/**`, `tools/e2e/**`, portable tests and manifests under `tests/` excluding owners 2/3's fixtures, and affected `docs/`/README files excluding this plan.
   Own required test registration, runtime fault coverage, smoke measurement, and synchronized roadmap/task evidence.

The Task 1 shared owner publishes signatures, then all seven owners may build concurrently in their assigned files.
Source and authority owners coordinate state methods before either edits a caller.
The state owner freezes intent/confirmation/restart method signatures before the source owner writes their effectful resource calls; no resource effect may precede its durable intent.
The shared owner alone integrates root registrations and final orchestration; repairs return to their assigned owner.
Reserve broad test execution for the closing verifier after all source owners stop editing.

## Task 1: Freeze Interfaces and Admit the Bounded Follow Profile

Owner: Shared/coordinator, with authority and receiver owners confirming the signature handoff.

- [ ] Add failing CLI tests for default pgoutput follow, explicit alternate plugin refusal, `PGCOPYDB_APPLY_GROUP_TXNS=100`/positive override, invalid/overflow values, missing explicit large-object skip, binary/split refusal, and preserved plain clone behavior.
- [ ] Freeze the types and signatures above, define `Invocation::Follow(FollowOptions)`, and pass an unbounded command control with bounded operation/stall deadlines.
- [ ] Persist non-secret effective follow settings and endpoint identities, never conninfo/passfiles/credentials, and require resume to match the recorded mode and identity; a supplied unsupported mode remains a pre-mutation refusal.
- [ ] Record and keep all four operator identity values; produce strict M1 framed reports through the shared producer path.
  Streaming reports must not masquerade as completed clone receipts.
- [ ] Hand each owner its exact files, interfaces, test names, and acceptance conditions; update the leader-owned plan ledger before independent implementation starts.

Run the existing CLI test command after extending its fixture inventory:

```sh
python3 tests/cli_contract.py
```

Expected: the full parser contract and new admission/refusal cases pass, with no database or state effects in refusal cases.

## Task 2: Decode pgoutput v1 Without Transaction Accumulation

Owner: Decoder.

- [ ] Add failing tests for B/C/R/Y/I/U/D/T/O, every truncated fixed field/string/tuple, negative/oversize lengths, unknown flags, missing relation/type context, duplicate BEGIN, rows outside a transaction, COMMIT without BEGIN, and trailing bytes.
- [ ] Implement `Decoder::feed` as a checked cursor over one pgoutput payload and emit rows immediately.
  Pass the immutable `FollowBinding` into `Decoder::new` alongside stream/limits, and validate its source type map at construction.
  Validate B/C timestamps and identity consistency, preserve commit/end distinction, and latch failure before callbacks so caught errors/panics cannot resume the decoder.
- [ ] Cache relation/type metadata within the explicit budgets; key relation generations by validated metadata changes, invalidate plans on change, and reject changes inconsistent with the frozen DDL-quiescent plan.
  Repeated identical R messages retain their generation.
- [ ] Preserve full-width K/O tuples, NULL, empty text, and unchanged TOAST; reject binary values in this profile and unsupported streaming/two-phase/logical-message tags.
  Origin metadata has no target origin effect.
- [ ] Record synthetic-row wire fixtures from actual PG16 and PG18, with server versions, capture inputs, expected decoded events, and source provenance.
  Include multi-relation TRUNCATE, non-built-in type metadata, generated/identity columns, and a large source-spilled transaction.
- [ ] Add a dependency-free decoder fuzz entry that consumes arbitrary byte slices, plus a required seeded mutation test over the retained corpus, every message truncation, arbitrary tags, and length corruption.
  Report this as bounded mutation coverage; coverage-guided fuzzing remains a release gate if it has not been run.
- [ ] Remove the retired codecs/plugins and update their callers/test registrations through the assigned owners before reporting readiness.

Run the focused unit tests after adding them:

```sh
cargo test --locked --lib cdc::pgoutput:: -- --nocapture
```

Expected: nonempty corpus/state/refusal tests pass; malformed bytes cannot panic, allocate beyond a checked bound, emit a completed transaction, or expose values in errors.

## Task 3: Receive, Backpressure, and Safe Status

Owner: Receiver/transport.

- [ ] Add failing FIFO tests for exact byte charging, capacity refunds, oversize single-message admission, count cap, ordering, closure, cancellation, and one pending emission without unbounded side queues.
- [ ] Construct the replication connection inside receive's own thread and retain `!Send` connection ownership.
  Pin output GUCs, TCP keepalives, `wal_sender_timeout=60s`, and version-specific session timeout settings before START_REPLICATION.
- [ ] Start only from the checked `RecoveryStart`, with exactly the two plugin options and exact slot/publication binding.
  Any replication NoticeResponse is fatal.
- [ ] On FIFO backpressure, stop consuming new replication frames while polling feedback, socket-write readiness, cancellation, and the monotonic status timer.
  Extend the existing CopyBoth poll with a read-paused mode if needed; do not call its ordinary read-enabled poll and retain discarded frames elsewhere.
- [ ] Send status every `min(10 s, wal_sender_timeout/2)` and promptly answer reply requests.
  Retry an unaccepted `queue_feedback` status without losing the newer proof; never lower feedback below the connection's captured confirmed-flush floor.
  Do not label `FeedbackDrained` as source receipt or acknowledgment.
  Keep received, target-committed, target-durable, status-submitted, and source-confirmed positions distinct; report source acknowledgment only from an actual slot confirmed-flush observation.
- [ ] Insert keepalives in FIFO order and mark whether the decoder was idle at receipt.
  A keepalive inside B..C cannot certify a source prefix; apply may discard that candidate and use a later idle keepalive.
- [ ] Have the coordinator poll exact slot/publication health on its own SQL connection during copy and follow at a bounded ten-second cadence, so a health query cannot stall the receiver's status timer.
  The source owner supplies `catalog::check_follow_health(&mut PgConnection, &FollowBinding, &IoControl) -> Result<()>` and bounds each health operation independently.
  Missing/replaced publication, missing slot, `wal_status=lost`, invalidation, unexpected plugin/database, active competing owner, or changed source system/timeline cause exit-3 gap refusal.
- [ ] Use scripted peers to test queue saturation while apply is blocked longer than sixty seconds, prompt cancellation, status-write backpressure, pending-status replacement, fatal notices, walsender EOF, and no unauthorized frontier advance.
  Use deterministic shortened timer controls for unit tests and one real timeout-length case at close.

Run the focused unit cases after adding them:

```sh
cargo test --locked --lib follow::channel::
cargo test --locked --lib cdc::receive::
```

Expected: all scheduled heartbeat opportunities complete within their asserted bound, even when input consumption is paused, and safe positions remain unchanged without proof.

## Task 4: Checkpoint, Certification, and Crash Recovery

Owner: Authority/state.

- [ ] Add failing tests for source-derived identity across reconnect, changed xid/commit/stream at equal end, lower end, noncontiguous order, and equal/new end ordering.
  A duplicate is skipped only after its complete B..C identity matches the last persisted checkpoint; no user DML executes during that validation-only replay.
- [ ] Advance native state to format 6 and target metadata to version 5 for pgoutput identity semantics and follow binding.
  Reject earlier formats without rewriting or converting them.
  Preserve exact checkpoint/copy_done/index identities and the existing plain clone path under the new format.
- [ ] Extend the same logged control row with nullable `follow_binding` text containing the checked source timeline, source-slot intent nonce/name/plugin, exact publication name/OID/owner, copy generation, and optional full operator work identity.
  Existing source system/database/run/checkpoint generation and target identity/OIDs remain authoritative in their existing columns; reject disagreement rather than selecting one copy.
  Require a non-null valid binding for any applied/certified follow state.
- [ ] Implement group sealing and pipelined checkpoint CAS through the frozen APIs.
  Require every new end LSN to increase strictly; update the control row only after all source transactions in the group are complete.
  A normal synchronous data/checkpoint COMMIT returns its durable proof without a second barrier COMMIT.
- [ ] Implement `certify` only for an ordered idle keepalive after all earlier channel DML is durable.
  Persist `certified_lsn` and increment the barrier token in a logged synchronous writing transaction before constructing feedback.
  Coalesce idle candidates to at most one certification per ten seconds, with immediate certification available later for M4 drain.
- [ ] On every connect, validate exact source and target binding, reacquire the writer fence, read the row on the target primary, and establish a positive synchronous logged barrier.
  Refuse exit 3 when source `confirmed_flush_lsn > max(applied.end, certified_lsn, anchor.consistent_point)`; otherwise request that maximum.
  A target failover that loses acknowledged work remains a gap refusal.
- [ ] Use the existing `commit_observed` boundary for group, anchor, certification, and recovery-barrier COMMITs.
  Preserve unknown COMMIT on transport loss or synchronous-replication cancel-wait warning even when the final tag is COMMIT and transaction state is Idle.
  Reacquire the fence and write a positive barrier before deciding whether the exact final group checkpoint committed; replay retained WAL only from the resolved authority.
  Never infer rollback from connection loss.
- [ ] Install the follow anchor only after exact current-generation COPY markers, post-data, sequence sync, and ANALYZE are positively complete.
  Bind its token/consistent point to the owned slot receipt and use the recorded source-slot intent nonce for the legacy `plan_nonce` field.
  Anchor installation and follow phase transition commit synchronously in the authority row.
- [ ] Add restart intent stages for replacing recorded owned publication/slot and advancing copy generation.
  A missing/replaced source resource normally refuses; only an exact persisted pre-anchor restart intent authorizes its expected removal/recreation.
  Unknown creation or missing positive pre-data ownership remains a manual/fresh-migration refusal.

Run the affected pure authority/state tests after adding their cases:

```sh
cargo test --locked --lib apply::checkpoint::tests::
cargo test --locked --lib state:: -- --skip live_
```

Expected: proof constructors are inaccessible to arbitrary receive positions, failure cannot commit partial work, and an old/foreign/uncertain state never authorizes replay or mutation.

## Task 5: Streaming Ordered Apply and Complete Row Semantics

Owner: DML/apply.

- [ ] Add failing plan tests for DEFAULT/INDEX identities, key changes, FULL without a key, duplicate FULL rows, NULL identity values, unchanged TOAST, generated columns, identity columns, partition movement, and target-specific custom type OIDs.
- [ ] Extend the existing plan/cache representation rather than borrowing whole relation metadata into cached plans.
  Validate each relation once per generation against the source plan and exact target table; map source columns to target writable columns and bind target type OIDs.
  Admit qualified text for non-built-in types only when catalog/utility reconstruction proves matching target semantics.
- [ ] Use NULL-safe old-key predicates and an exactly-one-row ctid subquery for keyless FULL UPDATE/DELETE.
  Preserve duplicates by modifying one matching physical row, never every identical row.
  Refuse missing/unusable identity or an unavailable old value instead of converting unchanged TOAST into NULL or an empty value.
- [ ] Omit generated columns from writes, preserve explicit identity values using the required overriding syntax, and omit unchanged TOAST assignments.
  Reject zero/multiple-row UPDATE/DELETE results and malformed INSERT results before group COMMIT.
- [ ] Apply multi-relation TRUNCATE as one explicit owned-table statement with ONLY on each named relation and the source restart-identity option.
  Never send CASCADE, even when the source flag is set; require the transmitted relation list and dependency coverage to be sufficient or fail the target transaction.
- [ ] Construct the target connection inside the dedicated apply thread, set `session_replication_role=replica`, `row_security=off`, TCP keepalives, and `transaction_timeout=0` only on servers at least 17.
  Refuse unsupported ALWAYS/REPLICA user triggers or missing parameter-setting privilege before DML.
- [ ] Stream B/event/C through one target transaction per permitted group, using bounded prepared pipelines and the Task 4 sealing API.
  Close only at complete source boundaries, commit before honoring an idle keepalive, and return only authority-created feedback proofs.
- [ ] Preserve the source commit order, repeated keys, foreign-key behavior under replica mode, statement-cache eviction/deallocation, and relation-generation invalidation.
  On any prepare, bind, execute, result-validation, callback, channel, or source-decode failure, poison the open transaction and require rollback/connection close.
- [ ] Keep a serial execution switch private to tests over the same message stream.
  Compare final table multisets, group checkpoint, and refusal outcome under adversarial event sequences; do not retain the retired storage codec as the oracle.

Run the focused plan/apply cases after adding them:

```sh
cargo test --locked --lib apply::plan::
cargo test --locked --lib apply::follow::
```

Expected: serial and pipeline paths agree on data/refusals, warm execution has no per-row waits, and memory stays bounded for a source transaction larger than the group/FIFO budgets.

## Task 6: Publication, Slot Snapshot, and Owned Data-Phase Restart

Owner: Source initial copy/catalog, coordinated with the shared driver and state owner.

- [ ] Add failing catalog tests for exact publication membership, partition leaves, identity/index support, ALWAYS/REPLICA triggers, slot retention settings, timeout settings, and wrong/missing resource identities.
- [ ] Preflight replication privileges/capacity, `wal_level`, `max_slot_wal_keep_size`, available source WAL-storage headroom evidence, PG16 `old_snapshot_threshold=-1`, and PG18 `idle_replication_slot_timeout=0`.
  Record the positive SQL observations separately from filesystem free space, which remains UNKNOWN under standard SQL.
  Document capacity monitoring as an explicit operator obligation; WAL-directory size does not establish free bytes, and UNKNOWN alone adds no new CLI acknowledgment or integration.
  Fail closed on known unsafe retention limits, invalid/lost slot state, or incompatible timeout policy; never issue ALTER SYSTEM or change real-server policy automatically.
- [ ] Read the preliminary plan under DDL quiescence, persist exact resource creation intent, create `FOR TABLE` publication membership with `publish_via_partition_root=false`, then create the persistent pgoutput slot with exported snapshot.
  Capture and confirm exact publication OID/owner/options and slot creation receipt before use.
  Never adopt a colliding name or create a replication origin.
- [ ] Build `FollowSnapshot` around the slot-exported token, imported SQL snapshot, and still-idle replication exporter.
  Disable exporter session timeouts before slot creation, and issue no further exporter command until all snapshot consumers have imported.
  Keep `SnapshotTicket` LSN-free; the separate source-slot receipt carries the consistent point.
- [ ] Recheck the plan under that snapshot and require `pg_publication_tables` to equal its exact physical copy inventory before workers run.
  Run one `pg_dump --snapshot` archive, pre-data restoration, existing parallel per-table COPY/FREEZE with transactional markers, then post-data, sequence synchronization, and ANALYZE.
  The existing COPY runner may conservatively retain the exporter until workers finish; it must never release before every positive import acknowledgment.
- [ ] Hand the complete marker/stage evidence to `install_follow_anchor`, release all coordinator target sessions/fences, and start apply only after its own thread reacquires the per-run target fence.
  Keep the native owner lock continuously held through this handoff.
- [ ] Implement pre-anchor restart only after checking every exact target table/schema/owner OID and recorded created-object receipt.
  Persist the restart intent, replace only this run's recorded slot/publication, obtain a new snapshot/copy generation, and issue one no-CASCADE TRUNCATE over the complete owned physical workset.
  Invalidate every old done marker before new workers can reuse it.
- [ ] Preserve explicit refusal after an uncertain post-data utility outcome; the current parallel restore cannot provide atomic ownership receipts.
  Keep that restart subdeliverable open under the deferred choices below; do not adopt unrecorded indexes/constraints.
  A post-anchor resume never recreates source resources or reruns COPY.

### Post-data restart boundary and deferred choices

The roadmap requires: “resume before the copy anchor restarts the data phase” and limits that restart to “tables whose creation this run recorded.”
Keep the approved four real pg_restore jobs; an uncertain post-data outcome remains an explicit recovery refusal and an open M3 qualification gap.
This gap does not block independent decoder, receive, or apply builds.
Stock [PG16 pg_restore](https://www.postgresql.org/docs/16/app-pgrestore.html) and [PG18 pg_restore](https://www.postgresql.org/docs/18/app-pgrestore.html) reject parallel jobs with a single transaction and ignore jobs for script output.
Two unimplemented alternatives require a later reviewed decision:

1. Preferred if a narrower profile is approved: emit unmodified post-data SQL and run prefix inventory, the file, and a logged ownership/stage receipt in one [psql transaction](https://www.postgresql.org/docs/18/app-psql.html), using `--single-transaction`, `--no-psqlrc`, and `ON_ERROR_STOP=1`.
   This needs no SQL parser, but requires an explicit jobs=1 profile/default amendment; jobs=4 cannot silently run serially.
2. Preserve parallel pg_restore with a scoped owned `ddl_command_end` hook that writes object receipts in the DDL transaction.
   PostgreSQL supports the [transactional hook behavior](https://www.postgresql.org/docs/18/event-trigger-definition.html), but [creation requires superuser](https://www.postgresql.org/docs/18/sql-createeventtrigger.html).
   This database-wide security-sensitive mechanism is outside the approved least-privilege design and requires explicit user architectural approval; do not implement it under this plan.

Neither alternative is selected or implemented.
Do not mark post-data restart or full M3 qualification complete while the uncertainty gap remains.

Run the focused source/snapshot tests after registration:

```sh
cargo test --locked --lib catalog::follow::
cargo test --locked --lib snapshot::follow_tests:: -- --skip live_
```

Expected: wrong bindings fail before target/source effects; follow initial copy uses the slot snapshot while plain M2 clone retains its SQL-exported snapshot behavior.

## M4 and M6 Evidence Handoff

Freeze this contract in the producer documentation at M3 close, without implementing drain/control early.
M4 adds `drain_end_lsn`, `drain_witness_lsn`, and `drain_kind` to the same logged checkpoint row, with all three null or all three present, and increments the metadata version for that persisted change.
Allowed kinds are exactly `keepalive` and `later_begin`.
The independent reader validates row provenance and full expected work/source/target binding, logged storage, exact row cardinality, applied frontier, certified frontier, anchor, and these same-row drain fields.
It must not build any binding from the report under validation.

The token stays `<checkpoint_generation>:<barrier_epoch>:<barrier_counter>`.
Checkpoint generation stays 1, epoch is nondecreasing uint64, counter is positive uint64, and each changed checkpoint/certification/drain publication increments the stored counter by exactly one with overflow refusal.
Resume retains the stored pair; a new token cannot reuse a pair for different semantic row contents.
Durable replay is the barrier-proven maximum of applied end, keepalive-only certified LSN, and copy anchor.
A later-BEGIN witness is exactly B.final_lsn, must be greater than requested E, and may exceed durable replay without raising feedback/reconnect authority.

For independent durability observation, freeze a same-row writing-barrier protocol after worker quiescence: acquire the same per-run advisory fence, validate and lock the exact owned row, sample its semantic contents/token, and issue a no-semantic-change UPDATE of that row followed by positive synchronous COMMIT in that observation session.
That UPDATE generates WAL without changing the sampled token or row meaning.
After positive COMMIT, revalidate the same session's target identity and return only the sampled, unchanged row contents.
Any warning canceling synchronous-replication wait, transport ambiguity, target change, row mismatch, fence failure, or non-primary target prevents proof.
Qualify this protocol on PostgreSQL 16 and 18 before implementing the M6 opaque evidence constructor.

When the claimed contract requires a configured synchronous standby, verify the required synchronous-commit mode and synchronous standby policy in that session before its writing barrier, and require positive COMMIT completion under that policy.
Polling `pg_current_wal_flush_lsn()` proves local flush only and cannot establish that acknowledgment.
A read-only SELECT, old valid token, worker exit, or reported replay LSN cannot set `BarrierConfirmed=true`.
The M4/M6 owners must test the blocked-standby case, synchronous-wait cancellation, failover/session replacement, and a newer asynchronous row under an older token.
Keep M6 evidence-dependent activation refused until these producer/observer tests pass.

## Task 7: Integrated Runtime, Crash, and Performance Gate

Owner: Harness/docs while building; an independent closing verifier after source freeze.

- [ ] Register exact pgoutput/follow unit and live case inventories in the default portable/database gates, with missing-suite, skipped-case, wrong-reason, empty-fixture, and missed-injection negative controls.
- [ ] Extend the existing disposable harness with follow startup/catch-up observation and deliberate non-successful stop, preserving independent SQL/schema/sequence oracles.
  Never use a synthetic completed receipt to make M3 look like M4 completion.
- [ ] Add observed fault controls at publication/slot intent and confirmation, dump/pre-data, snapshot import, per-table COPY COMMIT, post-data, anchor COMMIT, group DML/CAS/COMMIT, feedback queue/drain, and reconnect.
  Check no losses/duplicates against independent SQL and exact recorded checkpoint identity after each restart or explicit refusal.
- [ ] Include target restart, unknown COMMIT, walsender termination, source WAL loss, replaced publication, blocked apply beyond sixty seconds, filtered/idle WAL certification, oversize-frame refusal, source-spilled large transaction, and transaction/source-channel error after partial DML.
  Preserve issue 356-359 and 376 regression intent through named whole-transaction, no-false-progress, exact-checkpoint, and restart cases.
- [ ] Measure the warm prepared/pipeline round-trip count and one bounded catch-up smoke workload, reporting applied target rows/transactions and measured duration.
  Retain exact limits, server/client versions, fixture size, group size, and result status.
  Do not claim improvement over the fork or M9 qualification from this smoke result.
- [ ] Update native protocol, compatibility, testing, durability/retention, recovery and troubleshooting documentation with the actual admitted/refused behavior.
  Synchronize verified status and open gates in roadmap and task ledger through their assigned owner.
- [ ] At file freeze run `task lint test`, the extended `task test-db` PG16/PG18 profiles, and the new registered follow runtime/crash pair gate for 16 to 16, 18 to 18, and 16 to 18.
  The harness owner adds the follow Task targets `test-e2e-follow-16-16`, `test-e2e-follow-18-18`, and `test-e2e-follow-16-18` before this step, invoking `tools/e2e/follow.py --pair <source>-<target>` with the same private container controls as clone.
  Existing lint debt keeps milestone closure open even if focused follow checks pass.
- [ ] Obtain one independent xhigh safety review of feedback constructors, keepalive ordering, bounded backpressure, group atomicity/CAS, strict dedupe, unknown COMMIT, owned restart, and the M4/M6 evidence contract.
  Return repairs to owners and rerun affected failed checks; do not repeat unrelated broad suites after passing results without a changed input or unresolved concern.
- [ ] Keep unchanged operator Go suites out of the engine-only M3 loop; M6 owns its operator milestone-closing Go gates.
  Record actual runtime/crash receipts and distinguish process-kill evidence from power-loss durability claims.
- [ ] Advertise `cdc.pgoutput` only after its required runtime matrix and safety review pass.
  Leave cutover/cleanup and unqualified M8/M9 capabilities unavailable, and request no commit until all required checks are clean and the leader asks.

## Planning Review and Handoff

The plan covers the M3 decoder, threaded receive/apply, source-retained feedback authority, follow snapshot order, and pre/post-anchor recovery boundary.
The shared freeze preserves the existing known-frontier authority API while adding a streaming consumer, explicit byte/count budgets, full-width old tuples, and source-derived reconnect identity.
The seven build assignments have disjoint ownership; the closing verifier owns broad runtime checks after file freeze.

M4 drain/control/cleanup, M6 operator activation, M8 feature widening, M9 profile/performance qualification, and final release fuzz/sanitizer gates remain separate milestones.
Any unsupported recovery boundary remains an explicit refusal and an open roadmap deliverable.
The leader reviews this plan and accepts the signature/evidence handoff before implementation assignments.
