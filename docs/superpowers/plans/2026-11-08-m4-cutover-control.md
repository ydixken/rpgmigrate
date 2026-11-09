# M4 Cutover, Drain, and Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development under the team leader.
> Freeze shared interfaces first, then build with disjoint owners and one independent milestone-closing review.
> Steps use checkboxes for tracking.

**Goal:** Persist a forward-only cutover boundary, finish native follow with durable drain and final sequence proof, and remove only positively owned source resources.

**Architecture:** Keep the M3 source-retained WAL, FIFO receive/apply, target writer fence, and logged checkpoint authority.
Add a separately locked local control request and same-row drain/completion fields; coverage witnesses remain separate from feedback authority.
Reuse exact source-resource receipts and sequence synchronization, retaining the workdir and target metadata for later comparison.

**Tech Stack:** One Rust Cargo package, existing libpq observed-COMMIT transport, PostgreSQL pgoutput v1, standard-library filesystem locking, and the existing Python/database/container harnesses.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md#cutover-and-drain), [roadmap decision 6 and M4](../../roadmap.md), [native protocol v1](../../native-protocol.md), [M3 plan](2026-11-08-m3-follow-pgoutput.md), and [M6 independent evidence boundary](2026-11-08-m6-native-adapter.md#task-3-qualify-the-independent-target-evidence-boundary).
The roadmap's dated amendments override older design text.

## Global Constraints

- Keep one Cargo package and the `rpgmigrate` executable.
- Qualify only PostgreSQL 16 to 16, 18 to 18, and 16 to 18.
- Keep `durable` as the default; M4 does not qualify replay-safe cadence or enable `relaxed`.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources.
- Keep source table/namespace DDL quiescent and require the caller's source write freeze before sampling endpos and through final sequence synchronization.
- Keep large objects explicitly refused in this profile; their final re-sync belongs to M8 before that capability can open.
- Keep all target bookkeeping exclusions tied to exact recorded schema/table/index identities.
- Preserve positive synchronous COMMIT evidence, synchronous-wait-warning handling, fenced unknown-outcome recovery, and gap refusal with exit 3.
- Keep credentials, connection strings, row contents, and server DETAIL out of diagnostics and fixtures.
- Add no dependency, privileged event trigger, second authority table, origin creation, or local CDC spool.
- Use assigned feature branches and preserve other owners' work; do not commit or push without the leader's instruction.

## Review Focus

- A control writer racing finalization must not acknowledge a different boundary from the completed row; Tasks 2 and 4 test sealing, retries, and forward requests at each transition.
- A later BEGIN can prove coverage while applied/certified remain below E; Tasks 3 and 4 test the frozen `0/10`, `0/20`, `0/30` case without raising feedback.
- A visible row or COMMIT tag after synchronous-wait cancellation must not prove durability; Tasks 3 and 7 test unknown outcomes and independent observation.
- A crash after nontransactional sequence changes must not publish completion early; Tasks 3 and 7 require repeatable exact-owned sequence synchronization before completion.
- A same-name foreign or uncertain source resource must survive cleanup; Tasks 6 and 7 test ownership refusal, recorded absence, and replacement after removal.

## Starting Point and Handoff Gate

The leader accepted the frozen M3 candidate manifest SHA-256 `2b4faad37a5a7880ae6c011e7ef8acb1ff370e7c0dd89833c4949927421034d0` as the M4 build interface on 2026-11-08.
Its donor tree is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-native7-candidate-rcuqnac1/tree`; all 295 file hashes/modes and 48 explicit absences match its adjacent `manifest.json`.
This is an exact exported-tree identity, not a Git commit or completed M3 qualification.
Build M4 in isolation from that input, preserve the frozen donor and M3 qualification work, and do not join M4 before the required milestone gates.
The actual donor has native format 7 and target metadata 5 at `src/state.rs:170`, and 25 checkpoint columns at `src/apply/metadata.rs:16`.
M4 therefore targets native 8, metadata 6, and 29 checkpoint columns without converting older runs.
If the accepted M3 input changes, stop and re-freeze its bytes, absences, versions, producer DDL, signatures, and required harness inventory before continuing.

The accepted M3 handoff must provide `FollowMessage::{Begin, Event, Commit, Keepalive}`, `BEGIN.final_lsn`, FIFO idle-keepalive ordering, `TargetAuthority::{acquire_follow, recover_follow, certify}`, opaque `SafeFeedback`, and positively created source-resource receipts.
Reuse its conservative keepalive proof only after M3 qualifies it: the PG16/18 logical sender reports processed `sentPtr` or skipped-transaction progress, not an arbitrary global WAL head.
Do not widen that source proof or substitute an outer XLogData position.

M4 starts after a valid follow anchor.
M3 streaming, cancellation, and ordinary worker shutdown never provide final follow completion or exit zero; M4 alone adds that result.
The M3 default four-job post-data restore has an explicit uncertain-outcome recovery refusal; M4 neither repairs that gap nor claims complete pre-anchor recovery.
Completed cutover is immutable and idempotent; an interrupted sealed cutover resumes the same E, while a recovery gap still requires a fresh migration.

### Source interface map

The following paths and line numbers refer to the frozen donor, not newer primary-checkout edits.

- `src/cdc/contracts.rs:71` defines `Begin { xid: u32, final_lsn: CommitLsn, commit_time_micros: i64 }`; `Commit` at line 78 separately carries `commit: CommitLsn` and `end: SourceEndLsn`.
  `FollowMessage` at line 86 carries `Keepalive { wal_end: SourceEndLsn, decoder_idle: bool }` through the same FIFO.
- `src/apply/checkpoint.rs:349` defines `acquire_follow(PgConnection, &mut NativeState, &FollowBinding, &IoControl) -> Result<TargetAuthority>`.
  `recover_follow(&mut self, &FollowBinding, SourceEndLsn, &IoControl) -> Result<RecoveryStart>` at line 382 writes a counter-incrementing barrier; completed resume must branch before this mutating recovery path.
  `certify(&mut self, SourceEndLsn, &IoControl) -> Result<DurableProof>` at line 418 also increments the token.
- `RecoveryStart` at `src/apply/checkpoint.rs:95` exposes only `stream`, `applied`, `certified_lsn`, `anchor`, `start_lsn`, and `confirmed_flush_lsn` accessors.
  Its recovery maximum at line 1347 includes applied, certified, and anchor; `DurableProof::feedback` at line 154 includes applied/certified only.
  Preserve both distinctions when adding drain fields.
- `Session::acquire` at `src/apply/checkpoint.rs:1073` takes the blocking session fence `(0x5250474d, fence_slot(run))`, where `fence_slot` at line 16 is the run CRC32 interpreted as signed int32.
  Guard SQL at line 1114 binds backend/database/role, writable primary, durable storage, search path, and the exact advisory lock (`objsubid=2`).
  Command observation needs an existing-only try-lock entry point; calling the worker's provisioning-capable acquisition with an inspected fake `NativeState` is forbidden.
- `ControlRow` at `src/apply/mod.rs:36` has seven fields: applied, barrier epoch/counter, anchor, clone, certified LSN, and follow binding.
  `metadata::update` at `src/apply/metadata.rs:224` already supports full-row CAS, including an exact same-value UPDATE; `TargetAuthority::finish_inner` at `src/apply/checkpoint.rs:660` checks deferred effects, durability, session identity, and `commit_observed` before creating proof.
- `src/state.rs:107` records the positively created slot as `SourceSlotReceipt { intent, consistent_point, snapshot_token }`; the intent binds stream/name/plugin/catalog epoch and its nonce.
  `PublicationReceipt` at `src/contracts.rs:348` stores name/OID/owner; `PublicationProvenance::{Provisioning, Created, Confirmed}` at `src/state/follow_state.rs:17` distinguishes uncertain creation from confirmed ownership.
  `FollowBinding` at `src/contracts.rs:364` adds source timeline, copy generation, and the source type map; `FollowControl` at `src/state/follow_state.rs:98` adds operator and unowned-origin bindings.
- `src/state.rs:821` provides descriptor-relative no-follow opening; `Directory::check`, `open_metadata`, `read_manifest`, and publication synchronization enforce the current path/file identities.
  `inspect_clone` at `src/state/clone_state.rs:137` demonstrates inspection without the lifetime owner lock, but omits cleanup intent and does not supply a writable `NativeState`.
  The control owner must re-read a checked current manifest under the control lock instead of caching that inspection through cleanup.
- `catalog::sync_clone_sequences(&mut PgConnection, &mut PgConnection, &ClonePlan, &[OwnedObject], &IoControl) -> Result<()>` is implemented at `src/catalog/clone.rs:559` and exported by `src/catalog.rs:4`.
  It verifies complete target inventory and exact sequence object identity, then transfers both `last_value` and `is_called`; the final target call stays on the fenced authority session.
- `src/catalog/follow.rs:326` validates publication identity/options, line 257 validates membership, and lines 459/514 validate source identity/timeline and slot state/active owner.
  Restart deletion in `src/snapshot/follow.rs:118` supplies an absence/receipt pattern, not a general cleanup authority: its creation-point-only slot check cannot accept a legitimately advanced follow slot.
- `Error` at `src/contracts.rs:18` preserves a constructor origin and private cancellation marker; `Error::is_cancelled` at line 66 is the discriminator.
  `src/follow.rs:437` preserves a real coordinator error and substitutes a queued worker cause only for typed cancellation; keep that behavior and never classify cancellation from diagnostic text.
- `protocol::copy_report` at `src/protocol.rs:165` provides operator framing (`native-report:` followed by one ASCII space) and standalone JSON for COPY payloads; it is not a generic replication/cleanup builder.
  Shared owns the new command payload builders and must preserve those framing rules, strict M1 fields, and the empty discovery capability list until qualification.

The donor registers FollowAuthority 11, FollowApply 11, FollowReceive 4, FollowSource 7, and FollowSnapshot 9 required live cases, plus 21 full follow E2E cases.
These are inventory counts, not passing runtime receipts.
Retain those inventories when adding M4 cases, including `test-follow-snapshot`; do not replace the ongoing-follow profile with cutover-only cases.

## Owners and File Boundaries

- Shared CLI/reports owns `src/{contracts,config,cli,lib,protocol,progress}.rs`, new `src/stream.rs`, and `tests/cli_contract.py`.
- Native control owns new `src/state/control.rs` and its inline tests.
- Target authority/state owns `src/state.rs`, `src/state/follow_state.rs`, `src/state/clone_state.rs`, `src/state/clone_state/**`, `src/apply/{mod,checkpoint,metadata}.rs`, and `src/apply/checkpoint_live_tests.rs`.
- Apply/drain owns `src/apply/follow.rs` and `src/apply/follow/**`.
- Coordinator owns `src/follow.rs` and its inline tests.
- Receiver owns `src/cdc/receive.rs`, `src/follow/channel.rs`, `src/pg/follow_receive_tests.rs`, and the requested-end dead-code annotation in `src/pg/ffi.rs`.
- Source cleanup owns `src/catalog/follow.rs`, new `src/cleanup.rs`, and their inline tests.
- Harness owns `Taskfile.yml`, `tools/{test-db.py,ci/**,e2e/**}`, and all `tests/` except `tests/cli_contract.py`.
- Documentation/M6 handoff owns affected docs/README; operator interfaces are read-only without a separate operator assignment.
- Integration owns `tasks/todo.md` and guarded assembly only after the relevant file owners stop editing.
- The closing verifier has no source ownership.

The authority owner registers the control module and narrow existing filesystem primitives before the control owner builds.
The shared owner registers cleanup and stream dispatch; the cleanup owner does not edit root modules.
The authority owner alone changes persisted schemas and publishes the exact M6 handoff through the docs owner.
Shared signatures change only through the leader.
Receiver owns `src/pg/ffi.rs` only to remove the staged requested-end annotation once its existing API has a real consumer; transport changes require a new leader assignment.
No implementation owner edits another owner's harness manifests, CLI contracts, root registrations, or task ledger.
The integration worker records shared-signature acceptance, owner readiness, exact export hashes/absences, closing commands, and repairs in the ledger on the leader's behalf.
Only the leader allocates independent reviewers and releases runtime leases; an author reports readiness without starting peer review or another owner's checks.
Use `xhigh` for authority, control, apply, coordinator, receiver shutdown, cleanup, and independent safety review because those tasks govern durability, cancellation, or destructive effects.
Use `high` for CLI/report integration and harness cases, and `medium` for a documentation-only or mechanical integration assignment; record the risk reason in each dispatch.

## Frozen M4 Contracts

These are planned additions, not descriptions of existing M3 fields.

### Root clarifications, 2026-11-08

The leader clarified the contracts below before integration; these decisions supersede earlier floor, candidate, and recovery wording in this plan.
The applied/anchor floor veto applies only to first/changed request publication; an already durably admitted exact E survives a whole crossing transaction and crash recovery even when applied later exceeds E.
An E race invalidates a witness selection, not a retained later-BEGIN source event; re-evaluate that same BEGIN against the current E before consuming more input or starting its DML.
A fresh ordered covering witness may re-prove an existing valid drain row for the same sealed E through a same-value guarded synchronous UPDATE, preserving the original witness, kind, token, certification, and feedback.
Native-only validation permits completed observation after cleanup; active validation is required before unfinished effects, and no target fence may be acquired or awaited while the control lock is held.
A valid pending seal may return only the resynchronized durable final unsealed request for an ordinary seal retry; temporary bytes never supply a boundary or proof.
Checked authority closure requires Idle, the full session guard, an exact true advisory unlock result, Idle again, and local connection disposal; it does not acknowledge network closure.
These are planning decisions only; implementation, regression, runtime, review, and milestone gates remain open.

### Local request

Keep the reserved paths `control/lock`, `control/endpos.json`, and `control/.endpos.tmp`.
The bound, strict JSON record has `version=1`, `stream: StreamId`, `copy_generation: CopyGeneration`, `slot_nonce: [u8; 16]`, `target_nonce: [u8; 16]`, `end_lsn: SourceEndLsn`, and `sealed: bool`.
Reuse canonical identity types and reject unknown fields; do not repeat the whole follow binding or fabricate operator identity.
Validate these bindings against the checked native manifest, current follow generation, and exact confirmed target/source receipts on every open.

`ControlHandle::inspect(path: &Path, operator: Option<&OperatorIdentity>, io: &IoControl) -> Result<ControlHandle>` validates existing native state without taking the worker's lifetime owner lock or rewriting its manifest.
`ControlHandle::for_worker(native: &NativeState, io: &IoControl) -> Result<ControlHandle>` derives the same binding under the retained owner lock.
`read(&self, io: &IoControl) -> Result<Option<EndposRequest>>`, `set(&self, end: SourceEndLsn, floor: SourceEndLsn, io: &IoControl) -> Result<EndposRequest>`, and `seal(&self, expected: &EndposRequest, io: &IoControl) -> Result<SealedEndpos>` take the separate bounded control lock.
For a first or changed E, the CLI obtains the admission floor from the exact bound target row as `max(applied.end, anchor.consistent_point)`, excluding `certified_lsn`.
Check an already durably admitted identical E before the floor veto, whether open, sealed, or completed and subject to terminal-state rules; a lower requested E or changed E after sealing still refuses.
The floor is a first/changed-publication veto only, never a repeat veto against newly advanced applied state at sealing, drain, completion, or crash resume.
Certification can cover unrelated WAL after the operator has sampled E, so certification above E must not prevent publishing that unchanged E.
Keep certification in the existing feedback/reconnect maximum and never lower it to the requested E.
Require the established follow state: checked native `ClonePhase::Analyzing`, no pending `FollowRestart`, current copy generation and confirmed source/target receipts, and the exact matching target follow binding and complete copy anchor.
Pre-anchor COPY, restart, missing-anchor, and mismatched-generation states refuse cutover admission rather than queuing an early request.
The command's unfenced floor read is a conservative admission veto only; it establishes no durability proof or report value.
Revalidate current exact E, generation, bindings, active native state, anchor/current row, and witness at sealing and drain/completion without reapplying the publication floor to an accepted E.
Terminal cleanup refuses a new E but preserves validated existing-request reads and identical sealed retries needed to observe prior durable completion.
Request/proof fields stay private with read-only accessors; only `seal` constructs `SealedEndpos`.
Freeze `EndposRequest::{stream() -> StreamId, copy_generation() -> CopyGeneration, slot_nonce() -> [u8; 16], target_nonce() -> [u8; 16], end() -> SourceEndLsn, is_sealed() -> bool}` and `SealedEndpos::{request() -> &EndposRequest, end() -> SourceEndLsn}`.
Expose only checked immutable bindings from `ControlHandle`: `follow() -> &FollowControl`, `plan() -> &ClonePlan`, and `target() -> &ConfirmedProvenance`.
The acquisition and floor-read entry points revalidate the handle's current native identity/admission before consuming those bindings; cached inspection does not bypass manifest replacement, restart, or cleanup checks.
Preserve the frozen `ControlHandle::{validate(&self, io: &IoControl) -> Result<()>, validate_active(&self, io: &IoControl) -> Result<()>, try_clone(&self, io: &IoControl) -> Result<ControlHandle>}` interfaces.
`validate` checks native/control identities and permits completed observation after cleanup; `validate_active` also requires cleanup intent to be absent before unfinished effects.
Existing-only command acquisition and completed observation use `validate`; do not impose `validate_active` as a blanket refusal on those completed-state reads.
Use `validate_active` before unfinished apply/drain/sequence/completion effects, and use `checked_follow_cleanup` before any recopy SQL.
`try_clone` validates, reopens, and checks the same root/control/lock identities and immutable bindings; it is not an unchecked descriptor clone or new authority.
Use the control owner's `with_native_lock<T>(native: &mut NativeState, action: impl FnOnce(&mut NativeState) -> Result<T>) -> Result<T>` for cleanup/restart native mutations under the shared control lock.
Acquire the native owner lock outside that helper, then the control lock; an already held target fence may take a short control lock, but never acquire or wait for a target fence while holding control.
CLI publication releases control before try-fenced observation.
Persisting a request grants no target durability or feedback authority.

### Same-row authority

Advance the accepted native 7 / metadata 5 build input to native 8 / metadata 6 and refuse older formats without conversion.
Keep the existing 25 columns unchanged and append exactly `drain_end_lsn pg_lsn NULL`, `drain_witness_lsn pg_lsn NULL`, `drain_kind text NULL`, and `follow_completed boolean NOT NULL`.
Initialize all three drain columns to NULL and `follow_completed` to false.
The three nullable columns are present together or absent together; kinds are exactly `keepalive` and `later_begin`.
Represent them as `ControlRow.drain: Option<DrainRecord>` plus `ControlRow.follow_completed: bool`; do not add a second independently persisted replay frontier.
Completion requires a valid current follow binding, anchor, drain record, and final sequence synchronization.

Freeze `DrainKind::{Keepalive, LaterBegin}` and `DrainWitness::{Keepalive(SourceEndLsn), LaterBegin(CommitLsn)}`.
`DrainRecord { end: SourceEndLsn, witness: SourceEndLsn, kind: DrainKind }` is persisted; a witness is input to authority, never a `SafeFeedback` constructor.
The authority owner defines these types in `apply/checkpoint.rs`; `DrainRecord` and row serialization stay inside the authority/metadata boundary.
Add `TargetAuthority::record_drain(&mut self, request: &SealedEndpos, witness: DrainWitness, io: &IoControl) -> Result<DurableProof>`.
Add `TargetAuthority::complete_follow(&mut self, source: &mut PgConnection, native: &NativeState, plan: &ClonePlan, request: &SealedEndpos, io: &IoControl) -> Result<CompletedFollow>`.
Add `TargetAuthority::observe_completed(&mut self, request: &SealedEndpos, io: &IoControl) -> Result<CompletedFollow>` for an already completed row.
Freeze `TargetAuthority::try_acquire_follow(connection: PgConnection, control: &ControlHandle, io: &IoControl) -> Result<TargetAuthority>` as the existing-only command acquisition.
It takes the exact try-fence, requires the inspected confirmed receipts and admitted anchored follow state, and validates/prepares the existing metadata without provisioning, epoch takeover, manifest mutation, or reuse of the worker's native owner lock.
Freeze `TargetAuthority::observe_follow(&mut self, io: &IoControl) -> Result<DurableFollowSnapshot>` as the positive same-value logged UPDATE and synchronous-COMMIT observation of that exact row.
`DurableFollowSnapshot` has private authority-created fields and is not deserializable; expose `stream() -> StreamId`, `durable_replay_lsn() -> SourceEndLsn`, `certified_lsn() -> SourceEndLsn`, `checkpoint_id() -> String`, `follow_completed() -> bool`, and `target() -> &ConfirmedProvenance`.
Its durable replay includes the proved applied/certified/anchor maximum.
The completion flag selects `observe_completed` with the existing sealed request; it does not let a caller construct `CompletedFollow`, emit `drained=true`, or mint feedback.
Freeze the authority-owned free function `read_endpos_floor(connection: &mut PgConnection, control: &ControlHandle, io: &IoControl) -> Result<SourceEndLsn>` in `src/apply/checkpoint.rs`.
It validates existing source/target/native bindings, admitted follow phase and complete anchor, then returns only the unfenced `max(applied.end, anchor.consistent_point)` veto bound, using the current metadata reader without acquiring the worker fence.
It returns no proof, token, certified value, or report snapshot; a plain row read through it cannot authorize success.
The existing M3 `DurableProof` has no certified-LSN or anchor accessor, and neither `RecoveryStart` nor a plain progress read is an observation proof; do not fill that gap with raw row access.
`CompletedFollow` has private authority-created fields and is not deserializable.
Expose `end() -> SourceEndLsn`, `witness() -> SourceEndLsn`, `kind() -> DrainKind`, `durable_replay_lsn() -> SourceEndLsn`, `certified_lsn() -> SourceEndLsn`, `checkpoint_id() -> String`, and `target() -> &ConfirmedProvenance` for the exact proved snapshot, without exposing a writable/raw control row.
Its durable replay is the positively barrier-proven maximum of applied end, keepalive-only certified LSN, and copy anchor.
`DurableProof::feedback()` continues to use applied/certified only, excluding the anchor and every later-BEGIN witness.

Each changed checkpoint/drain/completion content increments the stored barrier counter by exactly one without decreasing epoch; reject overflow.
The token remains `<generation>:<barrier_epoch>:<barrier_counter>` with M1's exact numeric rules.
After completion, forbid DML, certification, changed drain contents, new cutover boundaries, and token changes.
An exact same-value logged UPDATE used to re-prove existing contents preserves the token because it changes no authority content.

Add `FollowState.cleanup: Option<CleanupProgress>`, where `CleanupProgress { slot_removed: bool, publication_removed: bool }` is strict persisted JSON and the existing immutable follow binding supplies its resource identities.
`Some(CleanupProgress)` is the terminal cleanup intent; only positively re-observed absence advances its booleans, which never reset.
Freeze `NativeState::{begin_follow_cleanup, confirm_follow_slot_removed, confirm_follow_publication_removed}(&mut self) -> Result<CleanupProgress>` with their cleanup consumer before parallel edits.
Preserve `NativeState::checked_follow_cleanup(&self) -> Result<Option<CleanupProgress>>`; check it before any recopy SQL so terminal cleanup cannot restart unfinished work.
Freeze `TargetAuthority::close(self, io: &IoControl) -> Result<()>`: require Idle and the full session guard, check exact `pg_advisory_unlock` returns true, require Idle again, then dispose of the local connection through `PQfinish`.
Successful proof output waits for checked close; false/malformed unlock, non-Idle state, guard failure, or cancellation remains nonzero, and `PQfinish` is not a network close acknowledgment.

### Worker and command results

Extend `apply::follow::run` to consume the bound control handle and return `Result<CompletedFollow>`; no ordinary streaming stop returns success.
Append `control: &ControlHandle` and `request_keepalive: &AtomicBool` before its existing `io` argument.
Append `request_keepalive: &AtomicBool` and `stop: &AtomicBool` before `cdc::receive::run`'s existing `io` argument; the coordinator owns both flags through `Arc<AtomicBool>`.
These coalescing signals preserve the independent error/user-cancellation signal and add no second FIFO.
The exact apply signature is `run(options: &FollowOptions, binding: &FollowBinding, native: &mut NativeState, source_confirmed_flush: SourceEndLsn, ready: SyncSender<RecoveryStart>, input: &FollowReceiver, feedback: &FeedbackSender, control: &ControlHandle, request_keepalive: &AtomicBool, io: &IoControl) -> Result<CompletedFollow>`.
The exact receive signature is `run(options: &FollowOptions, binding: &FollowBinding, start: RecoveryStart, output: &FollowSender, feedback: &FeedbackReceiver, ready: SyncSender<i32>, request_keepalive: &AtomicBool, stop: &AtomicBool, io: &IoControl) -> Result<()>`.
`src/follow.rs` is their sole production caller in the accepted donor.

The coordinator's private outcome is `WorkerOutcome::{ApplyCompleted(Box<CompletedFollow>), ReceiveStopped, Failed(Error)}`, carried on one `sync_channel(2)` with one terminal outcome per worker.
Use a private `worker<T>(outcomes: SyncSender<WorkerOutcome>, io: &IoControl, run: impl FnOnce() -> Result<T>, success: impl FnOnce(T) -> WorkerOutcome)` wrapper; a panic becomes `Failed`, and a real failure is sent before setting `io.cancelled`.
A successful wrapper publishes its mapped outcome without setting error/user cancellation.
Freeze private `stream(options: &FollowOptions, plan: &ClonePlan, binding: &FollowBinding, native: &mut NativeState, io: &IoControl) -> Result<CompletedFollow>`; it creates the worker's bound control handle after checked anchor readiness.
Freeze private `WaitReady<T>::{Ready(T), Completed(Box<CompletedFollow>)}` and `wait_ready<T>(ready: &Receiver<T>, outcomes: &Receiver<WorkerOutcome>, io: &IoControl) -> Result<WaitReady<T>>` against the same outcome channel.
The leader approved one bounded proof allocation for the 272-byte completion variant on 2026-11-08: move its `Box` without proof copies, then unbox at the `stream` return boundary; add no lint allowance or expectation.
Retain each readiness sender in its worker wrapper through terminal outcome publication, passing a clone into `run`, so readiness disconnection cannot precede an early completion outcome.
Keep the FIFO endpoints borrowed from coordinator scope through both explicit joins; the apply closure must not drop `FollowReceiver` between publishing completion and the coordinator setting receive-stop.
On `ApplyCompleted`, retain the proof, set only receive-stop, and join both workers before selecting success.
Require exactly one apply completion and a checked receiver shutdown when receive was started; unexplained worker success, duplicate/missing outcomes, panic, a genuine health/source/target failure, deadline, or external cancellation remains nonzero.
Recheck the real error/user-cancellation signal after joins and before output; intentional receive-stop never sets that signal or erases a failure origin.
Completed resume takes its existing-row proof before `recover_follow`, `follow_confirmed_flush`, or source-resource creation, allowing observation after exact-owned cleanup.
Shared defines `StreamOptions { state: StateRequest, operator: Option<OperatorIdentity>, source: Option<Conninfo>, target: Option<Conninfo>, command: StreamCommand, json: bool }` in `contracts.rs` and fixes the state action to Resume.
Use `StreamCommand::{Get(SentinelField), SetEndpos(EndposSource), Cleanup}`, `SentinelField::{All, Endpos, ReplayLsn}`, and `EndposSource::{Explicit(SourceEndLsn), Current}`.
Add `Invocation::Stream(StreamOptions)`, `stream::run(options: StreamOptions, io: &IoControl) -> Result<String>`, and `cleanup::run(options: &StreamOptions, io: &IoControl) -> Result<String>`.
Retain the frozen parser and refuse other sentinel/stream operations.

### M1-compatible observation and retry

M1 requires a measured `certified_lsn` in every replication payload, including an endpos acknowledgment; omitting it or substituting zero is not an active-read mode.
Plain `get --endpos` reads only the validated durable local request and emits its canonical E plus newline; an absent request is `BadState`, exit 3, rather than a fabricated zero.
Plain set-endpos can acknowledge local persistence, but a JSON set-endpos acknowledgment also requires positive fenced target observation.
`get --replay-lsn`, `get --json`, and any JSON invocation, including selector combinations, require that observation before exposing certified/replay values, drained status, a token, or proof.
An incomplete row may report `drained=false` only after this positive observation; an unproven visible row grants no informational JSON exception.

Use `pg_try_advisory_lock` with the exact existing target-fence keys for command observation, retaining all M3 backend, primary, provenance, and durability guards after acquisition.
An explicit false result returns `BadState`, exit 3, with no success stdout/report; a malformed result preserves the authority's `Target`, exit 7, refusal.
Preserve other existing error categories without message matching: the inspected authority's unknown-COMMIT refusal is `Target`, exit 7, and raw libpq failures remain `Pgsql`, exit 4, unless an existing endpoint boundary maps them.
Existing argument/configuration refusals remain exits 1/2; no new retry code, diagnostic field, or report phase is introduced.
Every successful observation locks and validates the exact logged row, performs a same-value logged UPDATE, and positively checks synchronous COMMIT in that session; local flush alone never suffices.
Release the fence only after observation and checked closure; preserve configured `remote_apply`, otherwise require at least `on`.

A set-endpos command durably publishes its bound E and releases the control lock before trying target observation.
If the active worker holds the fence, return the bounded refusal while retaining E; the worker independently rereads, seals, drains, and completes it without waiting for command acknowledgment.
After the worker releases its fence, an identical explicit-E retry can positively observe completion and emit the M1 acknowledgment.
The native operator must persist E before its first explicit-E command and reuse that E after every refusal or lost acknowledgment; repeated `--current` sampling is not this retry protocol.
M6 must support observation/acknowledgment through its standalone verification/helper route after worker exit, because the legacy exec path requires a running worker pod.
These M6 retry and helper behaviors are required integration dependencies, not qualified behavior of the existing legacy controller or pure M1 validator.

## Task 1: Accept the M3 Baseline and Publish the Interface Freeze

Owner: Shared, with authority, control, receiver, and cleanup owners confirming their boundaries.

- [x] Record the accepted M3 manifest hash/tree inventory, actual native/metadata versions and column-layout source, resource receipts, `RecoveryStart`, proof/observation API, fence keys, error categories, and actual receive/apply signatures.
  Native 8 / metadata 6 and the 25-column starting layout above apply only to the exact accepted native 7 / metadata 5 M3 hash and interface freeze.
- [x] Freeze the documented existing-only observation/snapshot/floor signatures, certified-independent admission rule, and private worker outcome/readiness contracts with the leader.
  M6's persisted-one-E retry and standalone post-worker helper are required implementation work, not existing behavior or an engine-build prerequisite supplied by the legacy controller.
- [ ] Add failing contract tests for the M4 Invocation routes, sealed-request access rules, same-row shape validation, and proof constructors unavailable to arbitrary positions.
- [ ] Publish the authority/control module registration and shared type declarations before their consumers build; add no placeholder capabilities.
  Authority owns its proof/observation types and root-state registration; control owns request/handle types; Shared owns `StreamOptions` and root command registration.
  Publish the agreed declarations as one interface handoff without making one owner edit another owner's files.
  CLI consumers validate/read the existing request first and honor identical E before reading a publication floor; only first/changed E uses `read_endpos_floor` before publication.
  Separate try-fenced observation follows control-lock release; a held worker fence cannot prevent a valid local request from being published.
- [ ] Hand the fixed explicit-E command and positive-observation contract to the M6 native-control/lifecycle/evidence owners for implementation and qualification.
  Preserve M1's measured-certified requirement and error envelope; a requested immediate successful JSON acknowledgment under the active fence requires contract review before implementation.
- [ ] Assign each build owner its files, focused test names, and handoff dependencies; the leader-assigned integration worker updates `tasks/todo.md`.

Acceptance: consumers agree on one versioned row/request contract, no worker invents an alternate schema, and no M4 capability is advertised by scaffolding.

## Task 2: Durable Concurrent Endpos Control

Owner: Native control, consuming Task 1's filesystem/identity handoff.

- [ ] Add `control_concurrent_forward_only`, `control_seal_races`, and `control_publication_faults` tests.
  Assert zero/overflow/malformed LSN refusal, first/advanced E below the observed irreversible floor refusal, equal-E idempotence after progress, lower-E refusal, and any changed E after sealing refusal.
  Race two writers and a worker seal; exactly one ordered request history survives.
- [ ] Add `control_cutover_requires_anchored_streaming`.
  Reject first/changed requests below actual applied/anchor, and reject native COPY, pending restart, missing confirmed receipts, or changed generation without creating a control request.
  Task 3 owns the target-row anchor checks; Shared/Harness combines both admission paths in CLI runtime cases.
- [ ] Add `control_foreign_paths` and `control_binding_changed` tests for old format, replaced root/control/lock/file, symlink, FIFO, hardlink, unsafe owner/mode, extra entries, malformed JSON, wrong nonce/generation/work, and interrupted temporary files.
  None may mutate native state or be read as an absent successful request.
- [ ] Implement the bound handle using existing descriptor-relative no-follow primitives.
  Explicitly update the root whitelist to admit only the validated `control/` subtree in the new native format.
  Require a private 0700 directory and regular private 0600 single-link files; hold the dedicated lock through read/compare/publication.
- [ ] Publish by writing the fixed temporary file, synchronizing it, renaming, then synchronizing `control/`; synchronize parent directory creation too.
  A failed synchronization never acknowledges persistence.
  Under the lock, retry an identical valid pending publication or resynchronize an identical final request; preserve/refuse conflicting or corrupt temporary state instead of choosing a boundary from it.
  For pending-seal recovery only, require a durable final unsealed E and a valid temporary record differing solely by `sealed: false` to `true` with identical E/bindings.
  Resynchronize and return only that final unsealed record for ordinary `seal` retry; never adopt the temporary seal as a boundary or proof.
  Changed/corrupt temporary state or temporary state without a durable final record refuses.
  Add regressions for the accepted false-to-true pending seal and each refused changed/no-final case.
- [ ] Implement durable open-to-sealed transition after rereading the exact E.
  Sealing fixes E for crash recovery and finalization, without making the local file a completion authority.

Run focused checks with `cargo test --locked --lib state::control::`.
Acceptance: the active worker retains its owner lock while control commands operate, acknowledged E survives reopen, and no interrupted/conflicting write can lower or silently replace E.

## Task 3: Durable Drain, Final Sequences, and Completed Observation

Owner: Target authority/state.

- [ ] Add authority tests for all-or-none drain fields, old versions, invalid kinds, zero/end/witness ordering, wrong work/nonces/generation/source timeline, token overflow, changed completed rows, and unlogged/replaced metadata.
  Add `follow_later_begin_is_not_feedback` with applied/certified=`0/10`, anchor=`0/8`, E=`0/20`, and B.final_lsn=`0/30`: replay remains `0/10` and feedback never becomes `0/30`.
  Add `follow_request_floor_excludes_certification`: with applied=`0/10`, anchor=`0/8`, and operator-persisted E=`0/20`, advancing certification from `0/10` to `0/30` leaves admission floor=`0/10`; independently advance applied or anchor to verify those still raise the floor.
  Add `follow_request_floor_requires_exact_anchor` for missing/partial/wrong-generation anchors, non-streaming target control, pending native restart, and unconfirmed source/target receipts.
  Prove that `read_endpos_floor` acquires no writer fence, returns only the applied/anchor veto bound, changes no row/token/manifest, and cannot construct the private observation/completion/feedback types.
- [ ] Implement the explicit version/DDL/read/insert/CAS/provenance changes, including marker metadata validation and plain-clone regression coverage.
  Validate the same recorded schema/table/marker/index OIDs, source/target database identities, run/provisioning nonce/checkpoint generation, follow binding, copy generation, and operator identity as M3.
- [ ] Implement `record_drain` only with an idle target authority after the apply owner has durably closed every earlier FIFO transaction.
  Revalidate the exact sealed request, active follow phase/bindings/anchor, current row, and covering witness before writing; do not repeat the publication-floor veto for an accepted E.
  A keepalive witness W requires decoder-idle ordering and W >= E; preserve or advance certification to `max(previous_certified, W)` in this immediate synchronous writing barrier.
  Add `follow_equal_certified_keepalive_records_drain`: a fresh ordered W=`0/30` records E=`0/20` when certified already equals `0/30`, without requiring certification to advance or lowering any authority position.
  A later-BEGIN witness is exactly B.final_lsn > E; persist it without changing applied, certified, feedback, or reconnect authority.
  Persist the witness and incremented token in the same logged row before returning proof.
  If a valid drain record already exists for the same sealed E, accept a fresh ordered covering W2 as re-proof input without replacing the stored W1 or kind.
  Re-prove that exact row through a guarded same-value synchronous UPDATE, preserving W1/kind/token/certification/feedback; a different fresh witness alone cannot cause permanent refusal.
  Add restart coverage with persisted W1, fresh different W2, unchanged row/token, repeated final sequence synchronization, and eventual completion.
- [ ] Implement `complete_follow` under the retained target fence and sealed request.
  Reuse `catalog::sync_clone_sequences` with the recorded complete owned-object inventory, including exact sequence OIDs, source identity, and `is_called`; then write `follow_completed=true` and a new token in a positive synchronous transaction.
  Recheck configured storage/durability guards, deferred effects, session identity, and row CAS before `commit_observed`.
  Revalidate active phase, current generation, bindings, anchor, current row, and the exact admitted sealed E before final sequence effects and before the completion write, without rejecting E because a whole crossing commit raised applied.
  Failed or unknown sequence/final-COMMIT outcomes return nonzero and no completion proof.
- [ ] On restart, revalidate the sealed request and same row under the reacquired fence.
  If completion is not proven, repeat final sequence synchronization under the continued caller write freeze before publishing completion; `setval` is nontransactional, so a local stage marker cannot skip it.
  If completed, `observe_completed` performs an exact same-value logged UPDATE and positively checked synchronous COMMIT, preserving all contents/token and returning idempotent proof.
  Preserve an admitted E below the current applied/anchor publication floor; recovery rechecks identities/current state and proof, not first-publication eligibility.
- [ ] Implement command observation through the same guarded authority, using the frozen Task 1 snapshot API for incomplete rows and `observe_completed` for completed rows.
  A held fence refuses promptly through its explicit try-lock result; no command reads around the active owner or releases its fence on that owner's behalf.
  Exercise existing-only acquisition with missing/pending provenance, absent metadata, and a held target fence; prove that refusal creates no resource, rewrites no manifest, takes no epoch, and releases no other session's lock.
  A held native owner lock alone must not block command inspection, the conservative floor read, or control publication.
- [ ] Test transport loss and synchronous-wait cancellation at drain/final/observation COMMIT, including COMMIT tag plus Idle.
  Preserve Unknown; reacquire the fence and establish a positive barrier before resolving the exact row.
  A plain SELECT, local WAL-flush position, asynchronously visible newer row, or old token never suffices.
- [ ] Test checked authority closure for true/false/malformed unlock, non-Idle state, changed guard, and cancellation; successful local disposal never claims a peer acknowledgment.

Run focused pure authority/state tests, then the assigned `task test-follow-authority` cases on PG16 and PG18.
Acceptance: only a positive guarded synchronous commit constructs drain/completion proof, sequences precede successful completion, and later-BEGIN coverage never expands safe feedback.

## Task 4: FIFO Drain and Successful Follow Shutdown

Owners: Apply/drain and Coordinator, with separate file ownership.

- [ ] Add tests for E arriving before B, during a group, between C and its target COMMIT, after an idle keepalive, and during a concurrent forward update.
  Include end_lsn exactly E, a crossing transaction, a later BEGIN whose COMMIT has not arrived, an active-transaction keepalive, and empty/filtered sources.
- [ ] Reread control during bounded input waits and at source transaction/group boundaries.
  Close complete earlier groups before consuming an eligible witness; once E is known, do not start target DML for an eligible later BEGIN.
  Never split a source transaction or report partial target work as drained.
  If E arrived after later work had started, finish or roll back the full affected target transaction before considering coverage.
  A whole crossing commit C may advance applied past the already admitted E; preserve that E and its retry/resume identity rather than introducing a second floor veto.
  Add whole-crossing-C and crash/resume cases with accepted E below the resulting applied position, plus refusal of a genuinely new/changed backdated E.
- [ ] Require all received transactions with end_lsn <= E durable before selecting the next ordered idle keepalive W >= E or B.final_lsn > E.
  Re-read the still-current admitted E and obtain `observe_follow` on the idle fenced authority; recheck exact E/generation/bindings/native active state/current row/witness, then seal without comparing accepted E to the newly advanced publication floor.
  Then call Task 3, which revalidates the target row before effects and final completion.
  A changed request invalidates the old-E selection; an idle keepalive candidate may be discarded, but retain a selected later-BEGIN B as the same source event.
  Re-evaluate B.final_lsn against current E: if B.final_lsn > E, form a new candidate with fresh fenced observation and exact sealing; otherwise apply B normally.
  Do not drop B, advance the FIFO, start its DML before that decision, or promote ACK authority from the retained event.
  Add both re-selection and normal-apply race cases, including multiple forward updates; continuous races remain bounded by the existing `IoControl` or end nonzero without effects/proof.
- [ ] Bypass M3's ordinary ten-second certification throttle for the final drain barrier, then synchronize final sequences and return only `CompletedFollow`.
  Check cutover witnesses before both the `candidate <= durable_floor` skip and timer throttle in `certify_keepalive` at donor `src/apply/follow.rs:383`.
  Add `endpos_sampled_before_certification_still_drains` and `fresh_equal_certified_keepalive_drains`: applied=`0/10`, anchor=`0/8`, E=`0/20`, certified=`0/30`, then fresh idle W=`0/30` must finish through the drain/completion barriers.
  An old visible certified value alone does not replace that fresh ordered witness or any final proof.
  Retain source health checks and source/target-generation/gap guards throughout draining.
- [ ] Replace the coordinator's success-as-error wrapper with explicit completed/failure results.
  Stop and join receive only after positive completion, retain the native owner lock through all worker exits, and emit a successful completed clone report only after finalization and joins.
  Completed resume observes the immutable row without recopy/reapply or recreating removed source resources; unproven state remains nonzero.
  Add `completion_is_published_before_receive_stop`, `completion_does_not_drop_fifo_before_stop`, `completion_during_readiness`, `completed_join_preserves_real_failure`, and `completed_join_preserves_external_cancellation`.
  The readiness case forces apply completion before its startup send and proves that the terminal outcome arrives before readiness sender teardown; a queued completion must not hide a later real worker failure during joins.
  Retain the existing inflight-health-cancellation, genuine health SQL failure, cancellation-looking message, origin-preservation, and deadline regressions at `src/follow.rs:534`.
  Shared owns any completion progress/report adaptation: `src/progress.rs:126` currently returns `streaming` whenever the anchor exists, and `read_clone_progress` is a plain read, so neither can authorize completed follow by itself.
  Reuse the measured complete COPY counters while requiring `CompletedFollow` to select completed output.

Run `cargo test --locked --lib apply::follow::` and `cargo test --locked --lib follow::tests::`.
Acceptance: missing control or cancellation never yields exit zero; successful follow requires the durable completion object, successful shutdown, and exact final report.

## Task 5: Prompt Keepalive Requests and Bounded Receiver Stop

Owner: Receiver.

- [ ] Add tests for drain-request wakeup, reply-requested status with unchanged safe LSNs, full FIFO, pending status output, and stop during a partial later transaction.
  Exercise heartbeat service while apply writes its final barrier and sequences.
- [ ] Pass a coalescing drain-request signal from apply/coordinator to receive.
  Send a reply-requested standby status promptly when E needs a fresh witness, retaining the existing bounded timer/backpressure behavior and proof-only status positions.
  A request does not synthesize a keepalive or advance feedback.
- [ ] Honor the independent receive-stop flag during reads, pending FIFO admission, and output service.
  Return success only for coordinator-requested completed shutdown after checked connection closure; preserve earlier source/decode/transport failures and user cancellation as failures.
  No FIFO message is promoted or reordered during shutdown.
  Reuse `PgConnection::request_replication_end` at `src/pg/ffi.rs:812`, then continue normal input/output polling until `ReplicationPoll::End` validates every terminal result at line 997.
  Do not treat `PQfinish`, an empty output buffer, a full-FIFO stop, or abandoned partial decode as checked terminal success; the slot remains the recovery log for unapplied later input.
  Keep reply-request intent pending until `queue_feedback` accepts it; `Status::submit` at `src/cdc/receive.rs:325` currently always sends `request_reply=false`.

Run `cargo test --locked --lib cdc::receive::` and `cargo test --locked --lib follow::channel::`.
Acceptance: cutover can drain an idle source without stopping heartbeats, and receiver shutdown cannot manufacture successful apply completion.

## Task 6: Stream Commands, Reports, and Exact-Owned Cleanup

Owners: Shared CLI/reports and Source cleanup, in their disjoint files.

- [ ] Shared: add CLI tests for explicit E and `--current`, local `get --endpos`, positively observed `get --replay-lsn`, `get --json`, every selector-plus-JSON combination, JSON acknowledgment retry, bad/missing endpoints, and continued refusal of other sentinel mutations.
  `--current` reads exactly source `pg_current_wal_insert_lsn()` after identity validation; never substitute `pg_current_wal_lsn()` or flush LSN.
  The operator path consumes its supplied E and never samples again.
- [ ] Shared: text selectors emit one canonical LSN and newline; JSON retains the strict M1 replication envelope and explicit measured `certified_lsn`.
  Require the exact persisted E and `end_persisted=true` for acknowledgment.
  Require Task 3's positive fenced observation for every replication JSON payload and replay measurement; an active fence or unknown outcome is bounded refusal with the existing error envelope, never measured zero.
  Assert exact error categories and empty success stdout, including a visible asynchronous row with an older token; do not classify failures through message text.
- [ ] Shared/apply: test local E persistence followed by active-fence refusal, control-lock release, worker sealing/draining without an acknowledgment, and successful identical-E JSON retry after positive completed observation.
  Keep the worker active while asserting the initial refusal, then require completion within the harness deadline; a command and worker waiting on one another fails the case.
  Include certification advancing from `0/10` to `0/30` after the operator persists E=`0/20` but before its first command; every command/retry must retain E=`0/20` and final measured certification/replay must remain at least `0/30`.
- [ ] Shared: produce `stream sentinel get`, `stream sentinel set endpos`, and `stream cleanup` reports through the existing framing helper.
  A completed drain reports E, persisted W as `drain_proof.replay_lsn`, exact kind/token/work, separately proved durable replay, and certified LSN.
  Do not advertise `cutover.v1` or `cleanup.v1` until their required producer checks pass.
- [ ] Cleanup: acquire the native lifetime owner lock before effects, refuse an active worker, and durably record a terminal cleanup intent before dropping anything.
  Reuse existing-only native recovery opening at `src/state.rs:232`, then check the requested operator against the recorded binding; do not call `open_follow` with a fabricated or absent saved origin.
  Hold the control lock while recording that intent, and have control writers revalidate the current manifest under that lock so a stale inspection cannot acknowledge a new E after cleanup starts.
  Use `with_native_lock` for the intent/removal-receipt mutations with native owner outermost, and release control before any target-fence acquisition; preserve the allowed already-held-target-fence to short-control order.
  Build the complete obligations from confirmed native receipts, not argv names, prefixes, report contents, or catalog discovery.
  Unconfirmed provisioning/creation or an incomplete ownership inventory refuses before any deletion; it must not disappear from a successful obligations list.
  Once cleanup begins, forbid recopy/reapply and unfinished-follow restart/resume; retain receipts and partial removal progress for retry, while allowing observation of previously durable completion.
- [ ] Cleanup: verify source system/database/timeline, exact publication OID/owner/options/membership, and the recorded positive slot creation binding before deletion.
  The slot has no catalog OID or creation nonce: name/plugin/database equality alone cannot authenticate a replacement.
  Require exclusive resource administration from positive creation through cleanup; if continuity is uncertain or an identity/active-owner/generation check disagrees, preserve the object and return incomplete/nonzero for manual handling.
  Document this boundary rather than adding a privileged ownership mechanism.
- [ ] Cleanup: remove only the recorded owned slot and publication, checking absence after each effect and persisting each confirmed removal.
  Already absent owned resources satisfy their explicit obligation; any resource reappearing after recorded removal is foreign/uncertain and must be preserved.
  Treat unknown DROP outcomes through positive re-observation and exact ownership checks, without blindly retrying a surviving same-name object.
- [ ] Cleanup: always include the retained owned canonical workdir and any recorded origin as unowned/unremoved.
  Keep target metadata and comparison state; never delete the directory, origin, target data, or unrelated resources.
  Emit `complete=true` only with every exact inventory obligation satisfied; missing/extra/duplicate entries and empty-inventory success fail.

Run `python3 tests/cli_contract.py`, `cargo test --locked --lib cleanup::`, and the assigned cleanup catalog cases.
Acceptance: repeated control/cleanup commands are idempotent, foreign objects survive every refusal, and M1's exact inventory/report validators accept actual producer output.

## Task 7: Qualification, M6 Handoff, and Milestone Close

Owners: Harness and Documentation/M6 handoff, then the independently assigned closing verifier.

- [ ] Extend the existing M3 follow harness and required manifests, preserving ongoing-follow cases and adding loaded full cutover cases on all three pairs.
  Run ordinary and `synchronous_commit=off` source writers, stop writers and outstanding transactions before independently sampling/persisting E, then set that exact E, await successful follow, compare complete schema/data/sequence oracles, clean, and compare again from the retained workdir.
  Keepalive and later-BEGIN coverage each need an independently checked actual runtime case.
  Update current native-7/metadata-5 readers in `tools/e2e/{follow,test_follow}.py` and native-7 readers in `tests/support/follow_snapshot_{fixture,fault}.py` and `tests/support/test_follow_snapshot_harness.py` to the exact new layout.
  Preserve explicit older-format refusal fixtures; do not rewrite unrelated format-4 compatibility fixtures to hide an unresolved baseline gate.
  Register new authority tests through the existing FollowAuthority profile, receiver tests through FollowReceive, and cleanup catalog tests through an explicitly extended FollowSource selector and manifest.
  Keep the original required case names and require each new name to appear in both discovery and nonempty runtime receipts.
- [ ] Cover repeated/equal/forward control writes, malformed/truncated/replaced control state, concurrent seal, crash around file synchronization/rename, drain-row COMMIT, final sequence sync, final COMMIT/report, and every cleanup effect/receipt boundary.
  Include lost acknowledgments, COMMIT warnings/unknown outcomes, source-generation/timeline change, confirmed-flush gap, retained foreign slot/publication/origin, repeated completed resume, and repeated cleanup.
  Preserve the M3 uncertain post-data refusal as an explicit open qualification gap.
- [ ] Qualify the M6 retry workflow with its owner: independently persist E, issue the explicit-E command, observe active-fence refusal, let the worker finish, then use the standalone helper for positive observation and identical-E acknowledgment before independent verification.
  Reject a retry that resamples `--current`, loses the persisted goal after a command error, requires a running worker pod after exit, or treats exit 3 as successful acknowledgment.
- [ ] Make harness negative controls reject missed injections, omitted cases, no writers, empty fixtures, wrong versions, missing sequence inventory, false drain after async visibility, and false cleanup completeness.
  Bind receipts to the frozen source tree and actual image digest; process-kill evidence does not claim power-loss qualification.
- [ ] Publish `docs/native-cutover-handoff.md` with actual versioned DDL/column types, strict row cardinality/catalog validation, all provenance bindings/nonces/OIDs, configured durability mode, canonical token transition, witness/completion representation, and exact cleanup obligations.
  Update native protocol/operator compatibility/testing docs and README pointers with admitted commands, write-freeze duties, retained resources, and unresolved recovery limits.
- [ ] Freeze the M6 independent-reader procedure against that exact layout.
  The recommended observation acquires the same target fence, validates/locks the exact logged row, performs an exact same-value UPDATE, checks every result/warning, and positively commits synchronously in the same session before exposing its independently bound row/token as `BarrierConfirmed`.
  Preserve `remote_apply` when configured, otherwise require at least `on`; local flush alone cannot replace configured synchronous replication.
  Qualify required SQL permissions, primary/failover behavior, timeout, and synchronous-wait-warning detection with the M6 owner; psql exit zero plus COMMIT text is insufficient.
  Keep M6 activation refused until this handoff and PG16/18 helper cases pass; do not infer activation from engine JSON or token syntax.
- [ ] After owners stop editing, obtain one independent xhigh review of no-work source proof, FIFO coverage, later-BEGIN separation, seal/completion races, unknown COMMIT, immutable completion, exact cleanup, and M6 evidence.
  Run integrated `task lint`, `task test`, the required live PG16/18 profiles, and each accepted follow E2E pair once at milestone close.
  The leader schedules the M6 Go/unit/envtest/report-probe suites once against the matching handoff; workers do not run broad Go suites after individual edits.
  Return repairs to their owners and rerun only affected checks before the final result.
- [ ] Record exact commands, nonempty case counts, actual producer/Go validations, review findings, and outstanding gaps in the leader-owned ledger and roadmap.
  Close M4 and enable its capabilities only after required lint/tests/matrix/review pass; do not convert retained component receipts into a whole-milestone pass.

Use the accepted M3 harness targets `task test-follow-authority`, `task test-follow-receive`, `task test-follow-apply`, `task test-follow-source`, `task test-follow-snapshot`, and `task test-e2e-follow-16-16`, `task test-e2e-follow-18-18`, `task test-e2e-follow-16-18`.
Confirm their registrations against the accepted tree before executing them.
Real-cluster operator E2E, deployment, image publication, and production cleanup remain outside M4's local qualification.

## Planning Review

The plan covers roadmap M4 and the producer side of the existing M1/M6 evidence contract.
The leader accepted same-row completion, request sealing, bounded observation refusal with identical-E retries, the exclusive-administration source-slot boundary, and the exact M3 build input above.
The leader also froze the existing-only command observation/private snapshot APIs and the applied/anchor admission floor, explicitly excluding certification.
Task 1 publishes those declarations for parallel consumers; it does not reopen their approved behavior or claim source implementation.
M6's planned `Execer.RunningPod` route does not itself supply post-worker observation; keep that dependency explicit and keep native activation refused until its helper is implemented and qualified.
M1 needs no report-schema amendment for the refusal/retry workflow; a demand for immediate successful JSON acknowledgment while the worker retains the fence would require renewed contract review.
We reuse the existing fence, observed-COMMIT helper, sequence code, FIFO, reports, and harness instead of adding another persistence or integration layer.
No runtime check, database operation, build, test suite, dependency installation, commit, or push forms part of this planning deliverable.

PostgreSQL documents the distinct insert/write/flush positions in [PG16 administration functions](https://www.postgresql.org/docs/16/functions-admin.html#FUNCTIONS-ADMIN-BACKUP), nontransactional sequence behavior in [PG18 sequence functions](https://www.postgresql.org/docs/18/functions-sequence.html), and local versus synchronous-standby durability in [PG18 WAL settings](https://www.postgresql.org/docs/18/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT).
The [PG18 slot catalog](https://www.postgresql.org/docs/18/view-pg-replication-slots.html) supplies names/state rather than an immutable slot-incarnation OID; the cleanup boundary above does not claim otherwise.
