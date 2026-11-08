# M2 Clone Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task by task.
> Freeze the shared interfaces first, then build with the seven disjoint owners below and review once at milestone close.

**Goal:** Run a durable plain clone and independent schema/data verification on 16→16, 18→18, and 16→18, with bounded parallel text COPY, recorded ownership, and explicit safe recovery boundaries.

**Architecture:** Reuse the existing libpq owner, native-state primitives, and logged target authority.
Use a SQL-exported snapshot for plain clone, standard matching-major pg_dump/pg_restore for schema, per-table target transactions for COPY, and read-only progress/compare paths.
Slot/publication creation and logical follow remain M3 work.

**Tech Stack:** Existing Rust dependencies, stock libpq, PostgreSQL client tools 16/18, Python harness, and podman or docker for the E2E layer.

**Spec:** [Roadmap M2](../../roadmap.md#m2-clone-slice-engine), [approved design](../specs/2026-11-06-rpgmigrate-design.md), and [native protocol](../../native-protocol.md).
The leader accepted the plain-clone, catalog, utility, comparison, and recovery interpretations recorded below during planning.

## Global Constraints

- Keep one Cargo package, existing parser registrations, native identity rules, and the pinned fixes-branch baseline.
- Preserve credentials and row contents outside diagnostics, raw utility output, receipts, and version control.
- Refuse unsupported input and foreign target collisions before target mutation; never adopt pgcopydb state or resources.
- Native state and target metadata remain exact-object/OID bound; do not exclude a schema by a similar name or broad prefix.
- Plain clone uses no source replication slot or publication and never forges a zero logical consistent point.
- Keep binary COPY, splitting, large objects, follow, drain, cleanup, all-database mode, and filtering staged until their owning milestones qualify them.
- Keep M0/M1 open while inherited strict gates are red; do not suppress lint or commit while required gates fail.
- Read-only planning does not authorize M2 source edits until the leader approves this plan and freezes signatures.

## Review Focus

- A pre-data utility crash can leave an unknown ownership handoff; refusal must preserve those objects without adopting or dropping them.
- Every committed table has its same-transaction marker, but a lost COPY snapshot invalidates marker reuse across a fresh copy generation.
- Generated columns, identity values, inheritance, and partition leaves must not duplicate, omit, or silently transform rows.
- Late COPY errors, unknown COMMIT, expired I/O, cancellation, and SQL errors must never produce a completed table or clone receipt without proof.
- Comparison must detect a changed byte, duplicate, NULL, missing object, or foreign object in the metadata namespace independently of displayed checksums.

## Admitted Clone Profile

The source and target must be primary databases on majors 16 or 18, with source major no greater than target major.
Select pg_dump/pg_restore by target major and verify their actual versions before mutation.
Use the existing explicit PG_CONFIG/PATH selection where supplied; packaged matching-major directories follow M1.
Keep source/target system identifier, database OID, and source timeline binding rather than equating database names with identity.
Update the existing source SQL identity gate from 14/17 to the qualified 16/18 profile.

The data plan contains logged ordinary heap tables, including each inheritance child and declarative partition leaf exactly once.
Copy each physical heap with ONLY semantics; partition roots supply schema but are not copied as an additional physical table.
Copy explicit identity values and omit generated columns from COPY input, allowing target expressions to compute them.
Handle CHECK/NOT NULL behavior as PostgreSQL defines it, and install indexes, foreign keys, and ordinary user triggers through post-data restoration.
Do not retain the E1 built-in-type, index, deferrable-FK, or partition whitelist as the new clone admission policy.
Text COPY admits qualified built-in types, domains, enums, and arrays whose cross-major text behavior is tested.
Refuse unsupported/custom textual-output gaps explicitly instead of claiming a match or performing a silent conversion.

Foreign tables, materialized views, unlogged tables, and extension-owned/configuration data are explicit preflight refusals in this first profile.
Extension definitions without data dependency may be reconstructed by pg_dump only after positive catalog checks prove no ignored extension data/configuration obligation.
Views, functions, schemas, sequences, and supported types use standard schema utilities.
Require source table/namespace DDL quiescence; this slice does not claim online DDL replication.
Pin source `row_security=off` so hidden-row access fails, and refuse unsupported target ALWAYS/REPLICA user-trigger states before work.

Fresh clone requires a collision-free target apart from its normal empty-database baseline and exact-provenance native metadata.
Record only newly created target objects as owned; existing `public` and default built-in objects do not become owned by association.
Reconcile schema/table identities after positive pre-data completion and retain exact OIDs, kinds, namespace, and ownership facts for later mutation and exclusion.
Run pre-data with pg_restore `--single-transaction --exit-on-error` and no publication/subscription restore.
If a crash loses the positive pre-data completion/ownership handoff, refuse recovery with a precise manual/fresh-migration handoff; do not infer ownership from matching names.

Default table/index/restore jobs are four.
Use real post-data concurrency; if explicit index-jobs and restore-jobs disagree, refuse that staged combination before mutation rather than ignoring one value.
Document the admitted CLI option matrix and explicit refusals for M8 features, filters, destructive restore, roles/all-database variants, and unsupported policy combinations.
Existing help/version and restricted data-only paths retain truthful behavior or receive an explicit documented refusal if their old invariant no longer exists.

## Copy, Recovery, and Completion

Export a SQL snapshot in a read-only REPEATABLE READ source transaction, inspect the plan under that snapshot, and dump pre-data/post-data into one private archive using `--snapshot`.
Keep the exporter alive through pg_dump and every worker's positive snapshot-import acknowledgment.
Each worker creates its own source and target connections; libpq handles remain `!Send` and never cross threads.
Worker source sessions import before their first snapshot-dependent query.
Target table work is `BEGIN; TRUNCATE ONLY <owned table>; COPY ... FROM STDIN (FORMAT text, FREEZE true); done marker; COMMIT`, with terminal results checked on both peers.
Use 64 KiB relay batches and flush on backpressure/end rather than per row; bound all queues, catalogs, workers, and the permitted oversize libpq row.

Use a coordinator-held per-run exclusive advisory fence plus the native workdir owner lock.
The shared driver's CloneAuthority retains the fence for the whole command, including schema restoration and post-data, not only while COPY workers run.
Delegate one exact table/generation/provenance permit to each COPY transaction; workers do not each contend for the coordinator's exclusive fence.
The marker and user data commit together in logged target storage.
Resolve unknown COMMIT only after reacquiring the fence, reading the exact owned marker/checkpoint, and establishing a positive synchronous barrier.
Visible asynchronous state and local completion files do not establish target durability.

Start COPY generation at one and keep it separate from M1's immutable checkpoint generation one.
Before post-data starts, `--restart` or `--resume --not-consistent` may obtain a fresh SQL snapshot, advance copy generation, invalidate prior done markers, and restart only recorded-owned data.
Never reuse snapshot-bound done markers under the fresh snapshot.
Require complete exact ownership proof before the multi-table restart TRUNCATE; use no CASCADE.
Once post-data begins, continue only positively proven completed stages or refuse an uncertain recovery boundary.
Do not add full constraint/index retirement and rebuild to make later recopy appear supported.
Document and crash-test this boundary, including a completed clone's idempotent resume and refused unsafe restart.

Restore post-data, synchronize sequences, and ANALYZE before a durable completed receipt and exit zero.
Reconcile nontransactional sequence/utility work from actual target state; an unknown stage result is not success.
Keep the authoritative checkpoint's applied end, keepalive-only `certified_lsn`, source identity/timeline, copy generation, and table markers separate.
M2 adds the checkpoint-v2 representation and group API, but no synthetic CDC frontier, feedback, or drain proof.
M4 will implement the M1 same-row drain witness; do not add an unused producer here.
Use the next explicit native-state and metadata format versions, refusing older formats without conversion rather than silently rewriting them.

## Shared Interfaces to Freeze Before Parallel Edits

The shared CLI/driver owner defines these crate-private types in `contracts.rs` before consumer edits, without creating placeholder implementations.
`CloneOptions` contains effective source/target Conninfo, StateRequest, optional full OperatorIdentity, table jobs, effective post-data jobs, and the admitted RestorePolicy.
OperatorIdentity preserves all four M1 values; do not reduce its persisted binding to RunId alone.
`CopyGeneration(u64)` is positive, `DatabaseIdentity` contains system identifier/database OID/major plus source timeline where applicable, and CloneTable contains source OID, qualified name, and ordered writable columns.
OwnedCopyTable adds positively recorded target OID, run identity, and copy generation.
ClonePlan contains the bounded source inventory and sequence/schema obligations; snapshot tickets contain token/source identity/generation and no logical LSN.
Keep restore policy as typed admitted options, not arbitrary utility arguments.
`CloneControl` contains the shared cancellation flag and the existing 300-second I/O budget; its `operation() -> IoControl` creates a bounded operation without imposing a whole-run deadline.
Use CloneControl for long orchestration/COPY/utility lifetimes and existing IoControl for individual bounded database operations.

Freeze these signatures at the shared-owner handoff; any change returns to the leader while consumers are paused:

```rust
clone::run(options: CloneOptions, control: &CloneControl) -> Result<CloneReport>
catalog::plan_clone(source: &mut PgConnection, target: &mut PgConnection,
    options: &CloneOptions, io: &IoControl) -> Result<ClonePlan>
catalog::bind_restored(target: &mut PgConnection, plan: &ClonePlan,
    state: &mut NativeState, io: &IoControl) -> Result<Vec<OwnedCopyTable>>
snapshot::CloneSnapshot::export(source: PgConnection, io: &IoControl)
    -> Result<CloneSnapshot>
snapshot::CloneSnapshot::ticket(&self, generation: CopyGeneration) -> SnapshotTicket
snapshot::import(source: &mut PgConnection, ticket: &SnapshotTicket,
    io: &IoControl) -> Result<()>
schema::dump(options: &CloneOptions, ticket: &SnapshotTicket,
    state: &NativeState, control: &CloneControl) -> Result<SchemaArchive>
schema::restore_pre(options: &CloneOptions, archive: &SchemaArchive,
    control: &CloneControl) -> Result<()>
schema::restore_post(options: &CloneOptions, archive: &SchemaArchive,
    control: &CloneControl) -> Result<()>
state::open_clone(request: &StateRequest, work: Option<&OperatorIdentity>,
    io: &IoControl) -> Result<NativeState>
state::inspect_clone(path: &Path, io: &IoControl) -> Result<CloneStateSnapshot>
copy::run_tables(options: &CloneOptions, plan: &ClonePlan,
    owned: &[OwnedCopyTable], snapshot: &mut CloneSnapshot,
    state: &mut NativeState, authority: &mut CloneAuthority,
    control: &CloneControl) -> Result<CopyReport>
progress::read(options: &CloneOptions, io: &IoControl) -> Result<ProgressSnapshot>
compare::schema(options: &CloneOptions, control: &CloneControl)
    -> Result<SchemaComparison>
compare::data(options: &CloneOptions, control: &CloneControl)
    -> Result<Vec<TableComparison>>
```

The state owner produces `CloneAuthority::acquire(target: PgConnection, state: &mut NativeState, generation: CopyGeneration, io: &IoControl) -> Result<CloneAuthority>` before schema work, then `bind_tables(&mut self, tables: &[OwnedCopyTable], io: &IoControl) -> Result<()>` after positive created-object recording and `take_permits(&mut self, io: &IoControl) -> Result<Vec<CopyPermit>>` before worker dispatch.
Worker `CopyTxn::begin(target: &mut PgConnection, permit: CopyPermit, io: &IoControl) -> Result<CopyTxn<'_>>` exposes a checked `connection(&mut self) -> Result<&mut PgConnection>` and `finish(self, report: CopyReport, io: &IoControl) -> Result<CopyCommit>`.
CopyCommit is either `Acknowledged(CopyReport)` or `Unknown`; preserve unknown separately from rollback.
After all worker connections have been closed/reaped, coordinator `recover_unknown(&mut self, table: &OwnedCopyTable, io: &IoControl) -> Result<Option<CopyReport>>` reacquires its fence and establishes same-marker readback plus a synchronous barrier.

Transport adds `PgConnection::pipeline(&mut self, io: &IoControl) -> Result<Pipeline<'_>>`, `Pipeline::send_prepared(&mut self, name: &str, params: &[PgParam<'_>]) -> Result<()>`, `send_command(&mut self, sql: &str) -> Result<()>`, and `finish(self, expected: &[ExpectedResult], io: &IoControl) -> Result<()>`.
ExpectedResult represents the exact command tag/row count; pipeline finish validates incrementally through sync, without retaining an unbounded result vector.
Keep raw SQLSTATE/detail in a private transport diagnostic boundary; public errors use safe classes and do not expose server detail containing row values.

## Seven Disjoint Build Owners

1. Shared CLI/driver: `src/{contracts,config,cli,lib,protocol}.rs` and new `src/clone.rs`.
   Own root module registrations, effective option validation, dispatch, report production, and final capability changes.
2. Transport: `src/pg/**` and its focused tests.
   Own pipeline, buffered COPY, SQLSTATE/detail classification, notice handling, and I/O budgets.
3. Checkpoint/state: `src/{state,identity}.rs`, `src/state/**`, and `src/apply/{mod,checkpoint,metadata}.rs` plus `src/apply/checkpoint_live_tests.rs`.
   Own exact metadata provenance, state format, per-run fence, generation/marker lifecycle, group frontier, and unknown-COMMIT proof.
4. Catalog/schema: `src/catalog.rs`, new `src/catalog/**`, and new `src/schema.rs`/`src/schema/**`.
   Own source/target admission, utility invocation, archive lifecycle, positive created-object inventory, sequences, and schema comparison inputs.
5. Snapshot/COPY: `src/snapshot.rs`, `src/{snapshot_live_tests,snapshot_copy_live_tests}.rs`, `src/copy.rs`, new `src/copy/**`, and removal of `src/copy_plan.rs`/`src/copy_plan/**`.
   Coordinate removal of `src/state/plan_files/**` with its state owner; do not edit those files directly.
6. Progress/compare: new `src/progress.rs`, `src/compare.rs`, and their focused submodules/tests.
   Own read-only reporting and exact canonical comparison, using the catalog/state interfaces instead of duplicating ownership rules.
7. Harness/docs: `Taskfile.yml`, `tools/test-db.py`, new `tools/e2e/**`, portable harness tests/fixtures, `tests/copy_integration.rs`, native fixture updates, and affected README/testing/protocol/operator-compatibility guides.
   Own test inventory changes and cross-repository fixture-copy coordination, with no operator runtime edits.

Run the sequential shared-interface bootstrap first.
Then run independent owners in parallel, with a maximum of seven workers and one owner per file.
The shared owner integrates calls only after handoff; integration repairs return to their file owner.
Independent review and broad/Go gates run once at milestone close, not per worker.

## Task Deliverables and Focused Checks

- [ ] Bootstrap failing CLI/profile tests, freeze shared signatures, and update the plan with the concrete handoff before implementation owners edit consumers.
  Cover identity mismatch before I/O, 16/18 pair validation, unsupported flags before mutation, explicit job conflict, and the documented restart boundaries.
- [ ] Transport: add failing pipeline-result/error/abort-sync tests and COPY buffering/backpressure controls, then implement the minimal libpq APIs.
  Run `cargo test --locked --lib pg::` with live tests excluded, and the selected live transport/prepared profiles through the retained harness.
  Remove the whole-run 300-second deadline; retain meaningful existing connect/no-progress budgets, renew only on actual progress, and let cancellation win.
  Silent pg_restore/index activity is not a stalled I/O witness; its process group must remain cancellable and its actual exit must be observed.
- [ ] State/checkpoint: add failing concurrent marker, wrong generation/OID, fence contention, unknown COMMIT, barrier/token overflow, and foreign-state tests, then implement the new logged metadata/format and worker permits.
  Run the focused state and authority tests; prove failure does not grant mutation authority or allow reuse of another table's completion.
- [ ] Catalog/schema: add admission fixtures for the exact admitted classes and explicit refusals, then implement standard utility execution and created-OID recording.
  Run focused catalog/schema tests and disposable live cases for wrong tools, source/target mismatch, foreign collision, utility failure, lost pre-data handoff, sequences, and post-data constraints/triggers.
- [ ] Snapshot/COPY: replace the sealed-plan/E1 path with the imported SQL snapshot and per-table generation path.
  Add source writes during copy, multiple tables/workers, child/partition duplicate checks, wide rows, empty database/table, late source/target error, worker death, and before/after-marker COMMIT cases.
  Run focused snapshot/COPY cases and remove old tests only where their superseded contract is replaced by a named new test.
- [ ] Progress/compare: add read-only overlap tests and the output fixtures below, then implement actual measured reports and comparison.
  Run focused progress/compare tests, including foreign metadata identity, absent counters, row-byte mutation, NULL, duplicate multiplicity, generated/identity output, and schema drift.
- [ ] Harness/docs: implement executable E2E targets and fixture sanity tests, plus the supported catalog/recovery/privilege instructions.
  Keep `tools/test-db.py` for Cargo live tests and containers as the E2E layer; reject empty cases, wrong major/tool versions, missed fault injection, wrong failure reason, and exit-zero mismatches.

## Verification and Output Contracts

`list progress --json` uses M1's `"native-report: "` transport with measured table/index/byte counters and exact work binding.
Read-only inspection must overlap the worker without taking its exclusive mutation lock or creating/updating files.
Missing measurements remain unknown, and a local completed phase alone does not prove durable target completion.

`compare data --json` retains the operator's per-table array and `"compare progress: "` keys.
Pin client encoding UTF8, DateStyle ISO/YMD, IntervalStyle postgres, TimeZone UTC, extra_float_digits 3, bytea_output hex, row_security off, and search_path pg_catalog on both sides.
Stream canonical text rows from ONLY each physical table in C byte order, preserving duplicates and NULL distinctions.
Determine status from actual canonical bytes and counts; existing crc32fast may produce display checksums, but equal checksums never override a byte mismatch.
Use PostgreSQL sorting and incremental reads rather than an unbounded client-side table collection.
The source must be quiescent for a meaningful final plain-clone comparison; concurrent changes produce a difference, not a false match.

`compare schema` uses the same target-major pg_dump on both peers with the same effective schema policy.
Normalize only explicitly enumerated utility provenance/header noise, preserving executable SQL and meaningful object differences.
Exclude exact recorded metadata objects only after proving their identities and inventory; refuse unknown objects in the owned namespace rather than excluding them by prefix.
No custom SQL parser or new hashing dependency is required.

At close, the integration worker runs:

1. Run the integrated local gate.

   ```sh
   task check lint test
   ```

2. Run the retained database profiles with exact PostgreSQL 16.15 and 18.6 bundle/version overrides.

   ```sh
   task test-db
   ```

3. Run the clone matrix using the new container targets.

   ```sh
   task e2e PG_SOURCE=16 PG_TARGET=16
   task e2e PG_SOURCE=18 PG_TARGET=18
   task e2e PG_SOURCE=16 PG_TARGET=18
   ```

Each pair must demonstrate clone, independent SQL schema/table/sequence fingerprints, progress, comparison match, and supported pre-postdata kill/resume cases.
Also demonstrate precise refusal after lost ownership handoff or uncertain post-data work, wrong pairs, foreign state/resources, and unsupported classes, with no unauthorized mutation.
Require an actual durable target receipt before exit zero, and mutate content to prove the independent oracle detects wrong rows or missing objects.
The matrix target must enumerate exactly these three pairs and refuse an empty selection.

Obtain one independent xhigh safety review of the integrated snapshot/ownership/marker/COMMIT boundaries and one scope review of runtime capability claims.
M2 changes no operator runtime code; Go suites are not repeated per worker, and any required cross-repository fixture validation runs once at close under the operator's current instructions.
Retain failures as unmet gates; update the ledger without marking M0, M1, or M2 complete while strict gates remain red.
Advertise only capability names whose complete supported profile passes its closing checks; never infer capability from parser acceptance or component compilation.
Commit only after required checks pass and the leader requests it.

## Planning Handoff

This plan separates the first runnable plain-clone alpha from follow and operator workflow qualification.
The leader approved this draft scope and seven-owner decomposition.
The shared owner must freeze the concrete API handoff above before parallel source work starts, without generating gratuitous stubs.
The leader dispatched `m2_shared`, `m2_transport`, `m2_checkpoint`, `m2_catalog_schema`, `m2_snapshot_copy`, and `m2_compare` read-only while `m2_shared` coordinates that final signature freeze.
The seventh harness/documentation worker follows when a slot is available.
No additional dependency, schema migration of an existing run, or legacy-state conversion is proposed.

## Frozen Shared Handoff (2026-11-08)

The leader approved the following handoff before consumer implementation.
The seven file owners above remain unchanged.
The shared owner owns this handoff and root module registrations.

- `contracts::CloneOptions` contains `source`, `target`, `state`, `operator`, `table_jobs: usize`, `post_data_jobs: usize`, `policy: RestorePolicy`, and `not_consistent: bool`.
  `RestorePolicy` contains `no_owner`, `no_acl`, `no_comments`, and `no_tablespaces` booleans.
  Both effective job counts default to four.
- `OperatorIdentity` contains `migration_uid: String`, `run: RunId`, `workdir_id: String`, and `checkpoint_generation: u64`.
  Validation remains in `protocol`, while native state binds all four values.
- `CopyGeneration(pub u64)` is separate from the immutable checkpoint generation.
  `CloneSource` contains `identity: SourceId`, `timeline: u32`, and `major: u32`.
  `CloneTarget` contains `identity: SourceId` and `major: u32`.
- `SnapshotTicket` contains `token: String`, `source: CloneSource`, and `generation: CopyGeneration`, with no logical LSN.
  `CloneTable` contains `source_oid: u32`, `name: QualifiedTable`, `writable_columns: Vec<String>`, and `all_columns: Vec<String>`.
  A table with zero writable columns is explicitly refused in this alpha.
- `OwnedCopyTable` contains `table: CloneTable`, `target_oid: u32`, `target_namespace_oid: u32`, `target_owner_oid: u32`, and `generation: CopyGeneration`.
  `OwnedObject` contains `class_oid`, `object_oid`, `object_sub_id`, optional `namespace_oid` and `namespace_name`, `name`, `kind`, and `owner_oid`.
- `ClonePlan` contains separate `source` and `target`, `generation`, `tables`, `sequences: Vec<QualifiedTable>`, and `policy`.
  The admitted inventory is bounded to 4096 physical tables and one MiB of published inventory.
- `snapshot::CloneSnapshot::export(PgConnection, CopyGeneration, &IoControl) -> Result<Self>` owns the exporter.
  It provides `ticket(&self) -> &SnapshotTicket`, `connection(&mut self) -> Result<&mut PgConnection>`, and `finish(self, &IoControl) -> Result<()>`.
  `snapshot::import(&mut PgConnection, &SnapshotTicket, &IoControl) -> Result<()>` positively acknowledges import.
- `catalog::plan_clone(&mut PgConnection, &mut PgConnection, CopyGeneration, RestorePolicy, &[OwnedObject], &[OwnedObject], &IoControl) -> Result<ClonePlan>` inspects the source under the snapshot and preflights the target.
  The two object slices keep exact native metadata separate from exact owned user objects during restart admission.
  `inspect_clone_tables(&mut PgConnection, &[OwnedObject], &IoControl) -> Result<Vec<CloneTable>>` supplies read-only comparison inputs.
  `reconcile_clone_tables(&mut PgConnection, &ClonePlan, &[OwnedObject], &IoControl) -> Result<Vec<OwnedCopyTable>>` binds exact created tables.
  `sync_clone_sequences(&mut PgConnection, &mut PgConnection, &ClonePlan, &[OwnedObject], &IoControl) -> Result<()>` and `analyze_clone(&mut PgConnection, &ClonePlan, &[OwnedCopyTable], &IoControl) -> Result<()>` keep their recovery boundaries separate.
- `schema::preflight(&CloneOptions, &ClonePlan, &IoControl) -> Result<()>` validates tools and utility connection transport before target metadata provisioning.
  `schema::dump(&CloneOptions, &SnapshotTicket, &NativeState, &IoControl) -> Result<SchemaArchive>` creates the private archive.
  `restore_pre(&CloneOptions, &ClonePlan, &SchemaArchive, &mut NativeState, &IoControl) -> Result<Vec<OwnedObject>>` publishes the pending stage before mutation and returns positive created-object evidence.
  The driver reconciles exact tables and calls `confirm_pre_data` immediately afterward.
  `restore_post(&CloneOptions, &SchemaArchive, &IoControl) -> Result<()>` observes actual utility completion.
  `schema::compare(&CloneOptions, &[OwnedObject], &IoControl) -> Result<bool>` owns utility noise normalization.
- `state::open_clone(&StateRequest, Option<&OperatorIdentity>, &IoControl) -> Result<NativeState>` owns the workdir lock and binding.
  `inspect_clone(&StateRequest, Option<&OperatorIdentity>, &IoControl) -> Result<CloneInspection>` is read-only and does not take the mutation lock.
  `CloneInspection` contains `identity`, `clone_state`, and `provenance`.
  `CloneState` contains `plan`, `ticket`, `phase`, `owned_objects`, `owned_tables`, and `completion`.
  The state owner defines the checked stage methods and `owned_copy_tables` accessor.
- `CloneAuthority::acquire(PgConnection, &mut NativeState, &ClonePlan, &IoControl) -> Result<Self>` retains the per-run coordinator fence for the whole command.
  `permit(&OwnedCopyTable) -> Result<CopyPermit>` delegates one exact table and generation.
  `CopyTxn::begin(PgConnection, CopyPermit, &IoControl) -> Result<Self>` owns the worker connection.
  `connection_mut() -> Result<&mut PgConnection>` permits COPY access.
  `mark_and_commit(self, u64, u64, &IoControl) -> Result<CopyCommitOutcome>` returns `Durable` or `Unknown`.
  `resolve_unknown(&OwnedCopyTable, &IoControl) -> Result<Option<CopyReport>>` requires a fenced synchronous barrier before returning exact marker counts or positive absence.
  `copy_report(&IoControl) -> Result<CopyReport>` requires a complete exact current-generation marker set.
  After positive pre-data utility completion, publish target `PreDataPending`, reconcile objects, confirm native ownership, and call `bind_tables(&NativeState, &IoControl)` to publish target `Copying`.
  Native `PreDataPending` precedes the utility, and no worker permit exists before both ownership and target `Copying` are proven.
  `record_complete(&CopyReport, &IoControl) -> Result<()>` and `durable_completed(&IoControl) -> Result<bool>` own the final receipt.
  Logged `copy_done` plus its primary index have recorded exact OIDs in the existing metadata namespace.
- `copy::run_tables(&CloneOptions, &ClonePlan, &mut CloneSnapshot, &mut NativeState, &mut CloneAuthority, &IoControl) -> Result<CopyReport>` creates bounded workers, whose connections are constructed on their threads.
  The authority never crosses a worker thread.
  The application admits at most sixteen MiB per libpq COPY row and makes no claim that this bounds libpq's internal allocation.
- `progress::read(&CloneOptions, &IoControl) -> Result<ProgressSnapshot>` and `compare::{schema,data}(&CloneOptions, &IoControl)` own their measured result types.
  An empty comparison catalog is refused; a positively measured empty table is valid.
- The transport owner implements `IoControl::{unbounded,is_expired}` and `pg::operation_control` without changing legacy struct literals.
  New long-running commands have no whole-run 300-second limit, while each transport operation retains a bounded progress budget and cancellation checks.
  COPY buffering is separate from the exclusively borrowed SQL pipeline.
