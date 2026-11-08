# Source Snapshot Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development for delegated execution.
> Use superpowers:executing-plans inside each assigned worker task.
> Track each step with a checkbox.

**Goal:** Construct an owning source snapshot capability from a newly created native-owned logical slot and an actual imported transaction.

**Architecture:** Keep replication commands inside the existing libpq boundary.
Native state records resource intent and the positive creation receipt.
The source owner verifies both physical connections and retains the imported transaction.

**Tech Stack:** Existing Rust package, controlled libpq, serde, and the private PostgreSQL harness.

**Spec:** The [approved design](../specs/2026-11-06-rpgmigrate-design.md), [source capability contract](../specs/2026-11-07-copy-snapshot-anchor.md#source-capability), and lead-approved control-read profile define this slice.
This plan implements the provider dependency, not the complete snapshot-anchor specification.
The [maintained gate ledger](../../../tasks/todo.md#source-snapshot-and-serial-prerequisites) records the released implementations, component reviews, and pending live qualification.

## Global constraints

- Qualify PostgreSQL 14.24 and 17.11 first.
- Refuse other provider profiles until their qualification, including PostgreSQL 9.6 through 11.
- Preserve selected operator-profile and cross-version qualification as required work; universal drop-in CLI parity is deferred under the [accepted scope](../../operator-compatibility.md).
- Require existing replication, connection, catalog, and `pg_catalog.pg_control_system()` EXECUTE privileges.
- Make no production grants, role changes, configuration changes, or source sentinel objects.
- Require dedicated physical sessions without reconnect, transaction pooling, or statement pooling.
- Preserve `--slot-name/-s`, its `pgcopydb` default, and existing plugin and snapshot environment bindings.
- Add no slot-name environment binding, native prefix, application-name override, or reverse-challenge fallback.
- Keep the native manifest at 16 KiB and store only small source control records.
- Leave sealed COPY plans, source COPY/catalog boundary extraction, target anchors, and CLI composition to separate plans.
- Preserve canonical identities, RPGEXP02 records, codec versions, and the existing absolute `IoControl` deadline.
- Use ordinary feature branches, preserve unrelated changes, and never create worktrees.
- Begin runtime work only after lead plan review and root's dependency handshake.

## Review focus

- A different importer backend or source identity must refuse before returning a capability.
- A missing CREATE reply or terminal result must retain intent without authorizing another creation attempt.
- Missing privileges, prior slots, and stale native records must produce their intended refusal.
- A stored export token must never substitute for a live imported transaction.
- Catalog epoch 7 and stream generation 9 must remain valid independent values.

## Actual seams and ownership

`src/pg/ffi.rs` owns SQL and logical connection modes, simple replication dispatch, result draining, and COPY transport.
Its ordinary `query()` uses the extended protocol and cannot serve logical replication connections.
The test-only `replication_testing::create_slot()` discards the exported snapshot and is not a production provider.
The native version-two planning baseline had target provenance only.
The implemented source-only version-three schema adds source binding, catalog epoch, intent, and receipt; target logged metadata remains version two.
`src/catalog.rs::plan_copy()` begins transactions, and `src/copy.rs::run_copy()` commits them.
Neither function can receive this capability unchanged.
Do not change those boundaries in this plan.

The four work packages have disjoint write ownership.

- Transport: `src/pg/ffi.rs`, `src/pg/replication.rs`, and their existing nested unit tests.
- State: `src/state.rs` and its nested unit tests.
- Provider: new `src/snapshot.rs`, including its private unit tests and live-test child declaration.
- Harness: new `src/snapshot_live_tests.rs` and four support files named in work package 4.
  The harness owner also owns its listed existing Python files.

Root owns `src/lib.rs`, `src/pg/mod.rs`, `src/contracts.rs`, `src/identity.rs`, and `src/cdc/contracts.rs` for shared registration and definitions.
The catalog owner owns the shared source identity helper and its tests in `src/catalog.rs`.
Root also owns Taskfile/CI integration, documentation entry points, and `tasks/todo.md`.
Do not let workers edit root-owned files concurrently.
Create no new production file besides `src/snapshot.rs`.

## Shared contract and dependency handshake

All declarations below are new planned APIs.
Existing `StreamId`, `SourceId`, `SourceEndLsn`, and `cdc::contracts::Plugin` remain canonical.
Root owns `contracts::MAX_SNAPSHOT_TOKEN_BYTES: usize = 1024` as the single token bound.
PG and state import that constant and check bytes, not characters.
Do not retain a local 256-byte bound.

### Native records in src/state.rs

`SourceSlotBinding` has private immutable fields: `stream: StreamId`, `slot_name: String`, `plugin: Plugin`, and `catalog_epoch: u64`.
Require a nonzero epoch independently of stream generation.
This new native-profile rule does not change existing `load_cdc_relation()` generation semantics.
The stream already supplies run and source identity, so add no duplicate fields or `SourceNativeBinding` type.

```rust
impl SourceSlotBinding {
    fn new(stream: StreamId, slot_name: String, plugin: Plugin,
           catalog_epoch: u64) -> Result<Self>;
    fn stream(&self) -> StreamId;
    fn catalog_epoch(&self) -> u64;
    fn slot_name(&self) -> &str;
    fn plugin(&self) -> Plugin;
}
struct SourceSlotIntent { binding: SourceSlotBinding, nonce: [u8; 16] }
struct CreatedSlotData {
    consistent_point: SourceEndLsn,
    snapshot_name: String,
}
struct SourceSlotReceipt {
    intent: SourceSlotIntent,
    consistent_point: SourceEndLsn,
    snapshot_token: String,
}
enum SourceSlotProvenance {
    Provisioning(SourceSlotIntent),
    Confirmed(SourceSlotReceipt),
}
impl NativeState {
    fn begin_source_slot(&mut self, binding: &SourceSlotBinding)
        -> Result<SourceSlotIntent>;
    fn confirm_source_slot(&mut self, intent: &SourceSlotIntent,
                           created: CreatedSlotData) -> Result<SourceSlotReceipt>;
    fn checked_source_binding(&self, expected: &SourceSlotBinding) -> Result<()>;
}
```

Use crate visibility for these interfaces and the plain intent/receipt fields.
`CreatedSlotData` is pure two-field state data with no PG types, slot, or plugin.
The intent already binds the slot, plugin, stream, epoch, and nonce.
Keep the persisted receipt's `snapshot_token` spelling.
Serialize the records with unknown-field refusal, following existing target provenance.
Add one required nullable `Manifest.source_slot` field.
An absent, pending, uncertain, or different record cannot satisfy `checked_source_binding()`.
`Provisioning` also represents an unknown CREATE outcome, never a live snapshot.
That method rechecks the held manifest, format, run, directory identity, and exact binding in `Confirmed` phase.
It is not a live SQL or snapshot proof.
It need not reject an otherwise valid reopened handle solely because the handle reopened.

`begin_source_slot()` requires a newly created run, no previous source record, and no E1 COPY completion.
Track fresh creation in memory rather than inferring it from empty persisted fields.
Require `state.identity().run == binding.stream().run`.
Check source and target stream agreement when both provenance records exist.
Refuse a second begin, even for identical input.
An identical receipt in `Confirmed` phase may synchronize again without replacing any binding.

Set native `FORMAT_VERSION = 3` for this fresh source-control schema.
Keep `TARGET_METADATA_VERSION = 2` and target SQL metadata unchanged.
Reject older native formats without conversion or adoption.
Later anchor control fields require version 4 or an explicitly versioned record.
Never reinterpret a published source-only version 3 manifest.
The linked anchor specification's native-version-3 assignment predates this source-only version rule.
Root owns its separate correction before later anchor work.
Do not edit that frozen specification in this package.

Root adds only the needed serde derives to `SourceEndLsn` and canonical `Plugin`.
Use exact plugin names `pgoutput`, `test_decoding`, and `wal2json`.
Add canonical `Plugin::as_str(self) -> &'static str` for wire spelling.
Do not change CDC wire encoding or introduce a second plugin enum.

### Transport contract in src/pg/

`CreatedLogicalSlot` lives in `src/pg/replication.rs`.
Its crate-visible fields are `slot_name: String`, `plugin: String`, `consistent_point: SourceEndLsn`, and `snapshot_token: String`.
The transport does not import native-state records.
PG and provider check all four wire fields and the slot/plugin match before state translation.
The provider maps `created.snapshot_token` into `CreatedSlotData.snapshot_name`.
State confirmation returns the persisted `SourceSlotReceipt`.

```rust
enum SlotCreateOutcome { Created(CreatedLogicalSlot), Unknown(Error) }
impl PgConnection {
    fn create_logical_slot_export(&mut self, slot: &str, plugin: &str,
                                 io: &IoControl) -> Result<SlotCreateOutcome>;
    fn quote_literal(&mut self, value: &str) -> Result<String>;
    fn backend_pid(&self) -> Result<u32>;
    fn server_version(&self) -> Result<u32>;
    fn is_replication(&self) -> Result<bool>;
}
```

The metadata accessors read libpq connection metadata and do not prove liveness.
`is_replication()` returns an error for closed or bad handles.
CREATE requires logical mode, local protocol Idle, and transaction Idle before dispatch.
Outer errors cover validation or mode/deadline failures before any possible dispatch.
After dispatch can begin, return `Unknown` unless the complete positive creation contract passes.
Close that connection on `Unknown`, including a drained error or malformed result.
Preserve the existing CopyBoth APIs and their separate terminal-result rules.

### Owning provider in src/snapshot.rs

```rust
impl SourceSnapshot {
    fn create(source: PgConnection, exporter: PgConnection,
              binding: SourceSlotBinding, state: &mut NativeState,
              io: &IoControl) -> Result<Self>;
    fn receipt(&self) -> &SourceSlotReceipt;
    fn finish(self, io: &IoControl) -> Result<OwnedSourceSlot>;
}
impl OwnedSourceSlot {
    fn receipt(&self) -> &SourceSlotReceipt;
}
```

The provider owns both actual connections, immutable receipt, and transaction phase.
Neither owner has an unchecked constructor, deserializer, clone, or raw connection getter.
The associated `create()` is the checked constructor.
`finish()` consumes the imported transaction and transfers the exporter into the opaque slot owner.
Dropping either owner closes its connections and never drops a slot.
Finishing supplies no target completion, COPY coverage, feedback permission, or CDC frontier.

Use the new frozen shared `catalog::read_source_identity(&mut PgConnection, &IoControl) -> Result<(SourceId, u32)>` helper.
The result contains the actual SQL system/database identity and backend PID.
The helper preserves the caller's transaction, isolation, imported snapshot, and locks.
It must not issue BEGIN, COMMIT, ROLLBACK, or snapshot-reset commands.
Call it before CREATE and inside the successfully imported transaction.
The provider may query remaining database-name, role, primary, version, and transaction properties.
Do not duplicate control-system, database-OID, or PID identity logic in the provider.
The catalog owner implements `VerifiedCdcSource` separately from this provider.
Its live checks use an actual provider-created confirmed binding within the existing snapshot inventory.

### Required handshake

1. [x] Root freezes the declarations, serde spelling, native version boundary, and exact fixture inventory before assigning runtime writers.
2. [x] Record the catalog-owner agreement on the single binding and independent catalog epoch.
   Copernicus `01a1b347-e258-7900-a873-0800a763e598` requested these fields and checked access.
   Godel `01a1b39f-4953-7d30-bbca-3a58a02d4cd6` owns serial composition.
3. [x] Root registers shared derives and PG exports when their defining files exist.
4. [x] Release transport, state, and harness work in parallel after the handshake.
5. [x] Begin provider implementation after the reviewed transport/state interfaces and shared catalog helper exist.
   The helper dependency does not hold transport or state work.
6. [x] Root registers `mod snapshot;` after the real tests-first source file exists, then the owner implements the actual API without placeholders.

## Work package 1: Production slot export transport

**Files:** Change `src/pg/ffi.rs` and `src/pg/replication.rs`.
**Consumes:** Canonical source positions, existing `IoControl`, simple dispatch, drain, and identifier quoting.
**Produces:** The transport contract above.

1. [ ] Add `create_slot_result` unit cases for exact shape, returned slot/plugin, source LSN, bounded token, and the positive completion tag.
   Reject NULL, empty, extra, truncated, NUL-containing, or inconsistent fields.
   Exercise zero and maximum position values without inventing a transaction.
   Accept 1024 token bytes and reject 1025 through the shared constant.
   Test mode inspection errors on poisoned handles and no dispatch for wrong mode or non-idle state.
2. [ ] Run the existing unit namespace and observe the new negative controls failing for their intended reasons.

   ```sh
   cargo test --locked --lib pg::replication::tests::
   cargo test --locked --lib pg::ffi::tests::
   ```

3. [ ] Implement one checked simple-query `CREATE_REPLICATION_SLOT ... LOGICAL ...` with default snapshot export.
   Request a persistent slot and no temporary, use-snapshot, or no-export option.
   Require one `PGRES_TUPLES_OK` result with one row and four columns.
   Require `CREATE_REPLICATION_SLOT` completion and drain through command end before returning `Created`.
   Require native connection health and transaction Idle after that drain, inside the uncertain-outcome path, without another exporter query.
   Treat the first possibly sending libpq call as the uncertain-outcome boundary.
4. [ ] Add client-side `PQescapeLiteral` quoting through existing allocation cleanup.
   Reject NULs and quote failures without emitting SQL.
   Do not query the exporter to check its token after creation.
5. [ ] Run the unit namespace again and hand root the actual case names, results, and changed interfaces.

## Work package 2: Immutable native source ownership

**Files:** Change `src/state.rs` and its existing nested tests.
**Consumes:** Root's canonical serde changes and the native declarations above.
**Produces:** Fresh version-3 source intent, confirmation, and exact checked binding.

1. [ ] Add `source_slot_` tests for publication ordering, one-shot begin, receipt equality, and every existing publication failure hook.
   Assert pending or changed receipts cannot pass the checked lookup.
   Assert stream generation 9 and catalog epoch 7 survive unchanged.
   Accept 1024 token bytes and reject 1025 without a separate state bound.
2. [ ] Run native tests and observe the intended new failures.

   ```sh
   cargo test --locked --lib state::tests::
   ```

3. [ ] Implement the new records through `checked_manifest()`, `replace_manifest()`, and `synchronize_manifest()`.
   Retain directory locks, ownership checks, unknown-field refusal, and the 16 KiB bound.
   Check nonzero identities/nonce/epoch, slot syntax, and bounded nonempty NUL-free token text.
   Keep the state module independent of PG transport types.
4. [ ] Change fresh native format to 3 and update affected native unit expectations.
   Reject versions 1, 2, and unsupported future formats.
   Preserve E1's unsupported resume/restart behavior and keep source records distinct from `copy_complete`.
5. [ ] Prove failed publication never authorizes CREATE or reconstructs a source snapshot.
   Hand root passing native cases and the exact serialized schema.

## Work package 3: Actual source identity and imported transaction

**Files:** Create `src/snapshot.rs`.
**Consumes:** Reviewed work packages 1 and 2, canonical binding, shared `catalog::read_source_identity()`, and the harness test child.
**Produces:** The owning provider contract above.
**Dependency owner:** The catalog owner implements the frozen helper and its identity-parser tests in `src/catalog.rs`.

1. [ ] Add provider comparison tests for source/role/backend/version mismatch using the shared identity result.
   Require the catalog handoff to cover signed SQL bigint conversion across the full system-ID range.
   Require a transaction-preservation test for an already imported snapshot.
   Reject invalid or missing evidence rather than substituting exporter WAL position or connection-string equality.
2. [ ] Register the live child as `#[cfg(test)] #[path = "snapshot_live_tests.rs"] mod live_tests;`.
   Distinguish the initial API-absence compilation failure from the later runtime guard controls.

   ```sh
   cargo test --locked --lib snapshot::tests::
   ```

3. [ ] Require both connections to be idle initially on the supported direct-session profile.
   Require `source.is_replication()?` to be false and `exporter.is_replication()?` to be true.
   Require transaction Idle and the transport's local protocol Idle guards.
   Call `catalog::read_source_identity()` and compare its `SourceId` with the expected stream.
   Compare exporter `IDENTIFY_SYSTEM` system ID and database name with that actual SQL evidence.
   Compare its backend PID with libpq's startup PID and retain it.
   Check required privileges and reject any preexisting slot before publishing source intent.
4. [ ] Synchronize intent through `state.begin_source_slot(&binding)` and attempt CREATE once.
   Check the four-field slot/plugin result before translating point/token into `CreatedSlotData`.
   Store the receipt returned by `state.confirm_source_slot(&intent, created)`.
   On unknown outcome, close connections and return an explicit failure with retained intent.
   On confirmation publication failure, return no capability and never clean up or retry the resource.
5. [ ] Quote the actual returned token on the client.
   Execute `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`, then `SET TRANSACTION SNAPSHOT` before any query.
   Require exact positive command tags and `TransactionStatus::InTransaction`.
6. [ ] Call the same shared helper inside that imported transaction and repeat the identity/backend comparisons.
   Require the same source, role, physical backend, primary, repeatable-read isolation, and read-only transaction.
   Check the exact native binding in `Confirmed` phase, including its independent catalog epoch.
   Only then return `SourceSnapshot`.
7. [ ] Keep the exporter alive and command-free while the owner remains live.
   Keep the imported transaction intact until `finish()` or failure.
   Finish only with exact source COMMIT evidence and Idle status.
   Close both connections after any failure and never recreate from stored token text.
8. [ ] Pass provider units and hand root the owner for separate live verification.
   Do not expose COPY/catalog accessors or wire a CLI handler in this package.

## Work package 4: Owned role fixtures, faults, and live acceptance

**Create:** `src/snapshot_live_tests.rs`, `tests/support/source_snapshot_fixture.py`, `tests/support/source_snapshot_fault.py`, `tests/support/source_snapshot_cases.json`, and `tests/support/test_source_snapshot_harness.py`.
**Change:** `tests/support/db_harness.py`, `tests/support/e1_cases.py`, `tests/support/test_harness.py`, `tools/test-db.py`, and `tests/support/harness.md`.
**Consumes:** Frozen native/provider interfaces and existing `OwnedCluster`, protocol relay, identity checks, and receipt validation.
**Produces:** One independently checked `SourceSnapshot` profile with twelve named live cases.

1. [ ] Create a separate owned logical primary and ordinary LOGIN REPLICATION role with no superuser, database-create, role-create, or bypass-RLS rights.
   Use fixture administration only to grant existing-function EXECUTE in the positive database and deny it in the negative database.
   Seed only owned tables and grant their required SELECT rights.
   Use a second owned primary to prove wrong-cluster refusal.
2. [ ] Add private `OwnedCluster(..., logical_slots=4)` capacity with the existing default unchanged.
   Require an integer range of 1 through 64 and use 16 slots for this closed profile.
   Check the actual server setting and retain persistent test slots until owned-cluster teardown.
   Do not add per-slot cleanup or change production configuration.
3. [ ] Write these exact `snapshot::live_tests::` cases into the separate inventory:

   - `live_owned_snapshot`: ordinary role, binding in `Confirmed` phase, actual returned position, and stable imported data during concurrent source writes.
   - `live_empty_snapshot`: verified empty database still yields an imported transaction.
   - `live_existing_slot_refused`: a foreign slot remains unchanged and no native source intent authorizes adoption.
   - `live_control_permission_denied`: satisfy earlier prerequisites and prove refusal before CREATE.
   - `live_wrong_source_refused`: different system ID with matching database naming cannot pass.
   - `live_reused_intent_refused`: a second absent slot name isolates native one-shot refusal from existing-slot refusal.
   - `live_create_reply_lost`: observe real creation, then drop its reply.
   - `live_create_tail_lost`: deliver its row but suppress terminal completion.
   - `live_expired_export_refused`: observe token expiry before import and return no capability.
   - `live_imported_identity_mismatch`: fault the post-import identity result and require its specific refusal.
   - `live_source_finish_unknown`: lose the source COMMIT reply and return no finished owner.
   - `live_application_name_preserved`: preserve explicit connection application name and PGAPPNAME behavior.

4. [ ] Reuse the existing private relay pattern for the five fault cases.
   Require exact owner, case, observed fault, affected backend closure, and real slot evidence.
   Missing, unarmed, duplicate, or pending receipts must fail.
   Use private child-test access to inspect the owned importer without adding a production raw getter.
5. [ ] Add mutually exclusive `--source-snapshot` wrapper mode with the `SourceSnapshot` profile and exact inventory/summary checks.
   Publish `RPGMIGRATE_TEST_SNAPSHOT_CONTEXT`, `RPGMIGRATE_TEST_SNAPSHOT_SQL`, `RPGMIGRATE_TEST_SNAPSHOT_EXPORTER`, and `RPGMIGRATE_TEST_SNAPSHOT_ADMIN`.
   Publish `RPGMIGRATE_TEST_SNAPSHOT_OTHER_SQL`, `RPGMIGRATE_TEST_SNAPSHOT_DENIED_SQL`, and `RPGMIGRATE_TEST_SNAPSHOT_FAULT_HELPER`.
   Reuse the existing paired expected-server-version environment variables.
   Preserve the incremental Authority18/Engine41 baseline, COPY40, and four replication-fault inventories.
   Hand root the ready live test child before provider implementation begins.
6. [ ] Update E1 Python native-receipt checks to version 3 with `source_slot == None`.
   Update their positive, prior-version, missing-field, and unexpected-source-record controls.
   Keep the independent E1 fixture-manifest version 1 unchanged.
7. [ ] Prove offline refusal for missing inventories, missing tests, skipped tests, wrong summaries, suppressed faults, and mismatched version pairs.
   Root registers this offline suite and the new profile before accepting runtime results.
8. [ ] After root registration, run the isolated profile on both approved fixture versions.
   Require all twelve named tests, zero failures/ignored tests, and all five observed fault receipts.
   A separate verification worker checks these results against retained native records and server observations.

## Root integration and completion gate

1. [x] Freeze runtime file owners and resolve shared signatures before code workers begin.
   Record the catalog-owner handshake and the lead's schema/version acceptance with this plan.
2. [x] Register PG result exports and the source module after definitions exist.
   Add only narrow documented staging expectations for unconsumed provider entry points.
   Do not suppress crate-wide warnings.
3. [x] Register the new offline suite in the required local gate.
   Add the dedicated `test-source-snapshot` task to `task test-db`.
   Exclude the new ignored live namespace from ordinary units and the incremental Engine41 selection.
4. [x] Update `tests/test_taskfile.py`, `tools/ci/verify.py`, and `tools/ci/test_verify.py` for the required source profile and fault footer.
   Require exactly twelve cases and five observed faults rather than trusting exit zero.
5. [x] Update documentation entry points, `docs/testing.md`, and `tasks/todo.md` with commands and actual qualification results.
   Keep runtime source, schema, and broad compatibility claims tied to those results.
6. [ ] Run the existing required repository gates after the file freeze.

   ```sh
   task check lint test
   task test-db
   ```

7. [ ] Record tool versions, server versions, test inventories, fault observations, and remaining blocked profiles.
   Request independent review before any commit or runtime release.
   Commit only when the lead requests it and every required gate passes.

> [!warning]
> An unknown CREATE can leave a persistent slot retaining source WAL.
> Retain the native intent and return a manual resource-review handoff.
> Never adopt, recreate, or drop a slot automatically.

This slice does not enable clone/follow or make a source receipt into target completion evidence.
The separate COPY/catalog, sealed-plan, and target-anchor plans own those integration boundaries.
The reverse exporter challenge remains deferred until independent proof and application-name/proxy compatibility review.

## Primary references

The [PG14 replication protocol](https://www.postgresql.org/docs/14/protocol-replication.html) defines slot results and export-token lifetime.
The [snapshot import rules](https://www.postgresql.org/docs/14/sql-set-transaction.html) require import before the first query.
The [PG14](https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/utils/misc/pg_controldata.c) and [PG17](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/utils/misc/pg_controldata.c) implementations expose the system identifier through the existing SQL function.
