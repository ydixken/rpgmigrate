# Owning COPY snapshot implementation plan

> [!important]
> The parent approved the fifteen-part owning plan and amendments 1 through 4.
> This file incorporates those amendments into the effective signatures and checklist.
> Saving this file alone does not release operation code.
> Root saves the spec and usage changes before Plato releases the assigned owners.

**Goal:** Publish a bounded source-owned COPY plan and atomically commit its copied data, logged anchor, and synchronous target proof.

**Architecture:** SourceSnapshot retains its importer and exporter.
TargetAuthority retains its target connection, fence, transaction, and proof construction.
Native4 intent binds the immutable plan before an execution reader borrows native state.

**Tech Stack:** Existing Rust package, controlled libpq, frozen immutable SQLite plan store, and PostgreSQL 14/17 qualification profiles.

**Spec:** The [COPY snapshot anchor contract](../specs/2026-11-07-copy-snapshot-anchor.md) and this approved refinement define the operation.
Root corrects the older spec signatures, native version, and source-finish order before operation edits.

For agentic workers, use `superpowers:subagent-driven-development` or `superpowers:executing-plans` within the assigned file ownership.
Root and Plato retain implementation release, integration, and verification coordination.

## Global constraints

- Use the primary feature checkout without worktrees.
- Keep Native4, metadata3, canonical identities, and reference version 1.
- Preserve the nonzero snapshot consistent point and exact confirmed source receipt.
- Never convert or adopt native2, source-only3, or pgcopydb state.
- Keep State independent of PG and concrete SQLite operations.
- Preserve explicit Text/Binary metadata without enabling Binary execution.
- Reuse TargetTxn with an internal optional CDC frontier.
- Never invent an XID, sequence, frontier, or source authorization.
- Retain the frozen absolute IoControl deadline and existing transfer behavior.
- Keep source and target table/namespace DDL quiescent under the accepted profile.
- Keep connection strings, credentials, and row contents out of persisted control data and diagnostics.
- Do not add a public CLI route, production WAL orchestration, WAL backend selection, or performance claim.
- Root owns the index and any later commit or push after genuinely passing required gates.

## Frozen inputs

Paths below are relative to the repository root.
Hashes identify the named retained source files from their handoffs.
They do not establish integrated or live qualification.

Check out these **inputs**:

- Catalog: `target/verify/catalog-streaming-nioa2ja1/final-v2/src/catalog.rs`.
  SHA-256: `f81a407d5a7ea0cee337aeb95f1239d64a029278dd1f075d810738581a52d1b7`.
  Receipt: `target/verify/catalog-streaming-nioa2ja1/handoff-v2.json`.
- Store: `target/verify/copy-plan-author-xnn8ff7h/candidate-v3/source/src/copy_plan.rs`.
  SHA-256: `dc632b58dfb4c65af295d06d170c834ebb7552b7ae819e5a05e7e40c9f48b58e`.
  Paired `src/copy_plan/tests.rs` SHA-256: `88ab5df6803ec30d4c9e0adce2b1c437b5828f9b5469753f85620d2184288f4f`.
  Receipt: `target/verify/copy-plan-author-xnn8ff7h/handoff.json`.
- Transfer: `target/verify/copy-table-transfer-rk39wanb/candidate/source/src/copy.rs`.
  SHA-256: `f258458ee3b6de052007296fa46a16bdef0d1b3e062970c397be3a250aad2070`.
  Receipt: `target/verify/copy-table-transfer-rk39wanb/handoff.json`.
- Native4/metadata3: `target/verify/native4-metadata3-integration-07g649mo/installed-anchor/source`.
  Readiness: `target/verify/native4-metadata3-integration-07g649mo/installed-anchor-readiness.json`.
  Inventory: `target/verify/native4-metadata3-integration-07g649mo/installed-anchor/source-hashes.json`.
  Its `src/apply/checkpoint.rs` SHA-256 is `4dfda94c2f655ad54e87fdcbf7a2d8e15244ddd7a50fc8b8966cfb23d0af8674`.
  Its `src/apply/metadata.rs` SHA-256 is `14c1e18a5b401d36193ed94630bf400ce54a3756ed8c658bf5dbc7910cc76625`.
  Its `src/state.rs` SHA-256 is `694a8018069e52634d2d50989bb72f697c95eb7efab6f408147ddbe2c140cccf`.

Root overlays only each component's owned files onto the authoritative Native4/metadata3 base.
The store export contains metadata2 State, so copying its whole tree would regress the selected base.
Consume the [catalog checklist](2026-11-07-copy-catalog-streaming.md), [transfer checklist](2026-11-07-copy-table-transfer.md), and [store protocol](2026-11-07-immutable-copy-plan-store.md).
Keep prerequisite review and qualification evidence separate from this operation package.

## Review focus

- A late unsupported entry must cause zero COPY starts, even when earlier entries pass admission.
- Enumeration, close, or publication failure must never become successful EOF or return a plan reference.
- Owned metadata exclusion must compare actual target pg_class OIDs, including after locking.
- Unknown target COMMIT must retain source and intent without permitting recopy.
- Target completion recovery must not manufacture source continuity or history authorization.

## Existing seams and format flow

The store's `publish_copy_plan` at line 137 accepts an iterator of `Result<PlanEntry>` and returns `CopyPlanRef`.
Its `open_sealed_copy_plan` at line 283 requires exact checked native intent and returns a pinned reader.
Its `scan_from` at line 340 visits one borrowed Relation and its stored format.
The reader retains its SQLite connection and NativePlanPin through consumer operations.

The final catalog has all five bounded helpers and `CopyRelation::into_parts(self, format: CopyFormat)` at line 238.
Inventory uses one NO SCROLL WITHOUT HOLD cursor and FETCH 128.
CopyRelation retains transient layout until target admission, then transfers CopyTable and canonical Event through into_parts.

The transfer helper at line 88 receives CopyTable and an explicit buffer budget.
The actual COPY restriction is `copy_sql` at lines 119 through 124.
It rejects `table.format != CopyFormat::Text || table.columns.is_empty()` with `COPY requires text format and explicit columns`.
Its SQL emits `FORMAT text` at line 133.
This restriction governs COPY SQL, not CDC row values.

Preserve the following format flow:

1. Accept the requested CopyFormat explicitly in the source publisher.
2. Pass it unchanged to into_parts(format).
3. Reject a returned CopyTable format that differs from the request before PlanEntry::checked.
4. Let PlanEntry retain that exact format with its Relation.
5. Preserve Text tag 0 or Binary tag 1 in the v1 record and CRC input.
6. Read the stored format unchanged through scan_from.
7. Pass the stored format into admission's into_parts(format) and check equality.
8. Build the transfer CopyTable with the stored format, without fallback.

Store publication supports both metadata tags.
Complete admission applies the existing execution restriction to every entry before the first COPY starts.
Binary execution remains outside this operation package.

## Effective signatures

These owner operations are proposed additions, not existing callable APIs.

```rust
impl SourceSnapshot {
    pub(crate) fn publish_copy_plan(
        &mut self,
        native: &mut NativeState,
        plan_nonce: [u8; 16],
        format: CopyFormat,
        buffer_bytes: usize,
        io: &IoControl,
    ) -> Result<CopySnapshotIntent>;

    pub(crate) fn copy_into(
        &mut self,
        target: &mut PgConnection,
        owned_target_relations: &[u32; 2],
        plan: &mut SealedCopyPlan<'_>,
        io: &IoControl,
    ) -> Result<CopyReport>;
}

impl TargetAuthority {
    pub(crate) fn copy_snapshot(
        &mut self,
        source: &mut SourceSnapshot,
        native: &NativeState,
        io: &IoControl,
    ) -> Result<(CopyReport, SnapshotProof)>;

    pub(crate) fn recover_snapshot(
        &mut self,
        native: &NativeState,
        io: &IoControl,
    ) -> Result<SnapshotProof>;

    fn begin_copy(&mut self, io: &IoControl) -> Result<TargetTxn<'_>>;
}

impl TargetTxn<'_> {
    fn complete_snapshot(
        self,
        anchor: &SnapshotAnchor,
        io: &IoControl,
    ) -> Result<SnapshotProof>;
}
```

The existing NativeState bind and checked lookup remain unchanged.
Authority opens its own checked reader from that lookup instead of accepting a separately paired pin and intent.
Source remains getter-free and receives only a confined internal target loan.

The following are narrow helper additions or signature changes:

```rust
metadata::owned_relation_oids(
    connection: &mut PgConnection,
    confirmed: &ConfirmedProvenance,
    io: &IoControl,
) -> Result<[u32; 2]>

catalog::preflight_copy_target(
    target: &mut PgConnection,
    relation: &CopyRelation,
    owned_target_relations: &[u32; 2],
    io: &IoControl,
) -> Result<()>

copy::validate_copy_table(table: &CopyTable) -> Result<()>
```

Extract validate_copy_table from the exact existing copy_sql guard and call it from both paths.
Widen only the existing transfer_table, configure_session, and require_setting visibility needed by these consumers.
Do not enable Binary transfer or change the E1 wrapper bodies.

## Verified owned relation identities

After begin_copy's locked full metadata validation, authority constructs the forbidden array on its own target connection.
Bind confirmed.table_oid to the pg_index lookup for indrelid and indisprimary.
Require exactly one nonzero u32 indexrelid distinct from the confirmed table OID.
The existing metadata catalog validation proves the single owned primary index.
Return exactly `[confirmed.table_oid, indexrelid]`.
Both elements identify pg_class objects.
Do not include confirmed.schema_oid or any source relation OID.

Construct this plain array before lending the target connection.
Source passes it unchanged into every preflight_copy_target call.
Thread the exclusion into the existing private target lookup/layout path without adding a capability framework.
Reject membership immediately after the actual target lookup, before target eligibility, type/layout inspection, or emptiness checks.
Check the actual target OID again after lock and identity revalidation.
Preserve generic CDC's existing prevalidation and error order.

Do not use source Event.key.oid, a schema-name prefix, unsupported pg_lsn types, or later constraints as the exclusion guard.
Do not add a raw authority getter or another persisted identity record.

## Publication and borrow order

1. Source publishes the sealed plan and returns an unbound plain CopySnapshotIntent.
   Construct its anchor from the actual private receipt and supplied nonzero plan nonce.
   Do not bind native intent during this step.
2. End the publisher, draft, and enumeration borrows.
3. Acquire target authority with mutable NativeState and confirm target provenance.
4. Bind and synchronize the exact native intent through the existing State API.
   Native bind requires confirmed target provenance.
5. Authority opens the execution reader through immutable NativeState using the checked intent.
6. Retain that pin through complete admission, transfer, and target finalization.
7. Return report and private proof, ending the reader and immutable native borrow.
8. Finish the retained source only after acknowledged or recovered target proof.
9. Publish advisory counts only after proof, pin drop, and source finish.

No native/run lock is cloned.
No raw handle resolves a borrow conflict.
Native publication and the source iterator borrow different owning objects.

## Bounded source operations

Add a private CopyRelations iterator in snapshot.rs.
Retain CopyInventory, one page of at most 128 identities, and one transient CopyRelation.
Use the receipt's catalog_epoch independently of stream generation.
Retain SourceSnapshot's physical-session, identity, and imported-transaction guards.

Clone only the small receipt data before borrowing SourceSnapshot for enumeration.
Map each relation through into_parts(requested_format), exact format comparison, and PlanEntry::checked.
Pass a mutable iterator reference to the existing store publisher so explicit cleanup ownership survives an early store error.
Publish Relation metadata only, without invented CDC transactions or frontiers.

Successful EOF requires successful cursor close before the iterator returns None.
A failed fetch, mapping, inspection, or close yields failure and never successful EOF.
On an early store error, close only through the same healthy active importer.
Preserve the original error if cleanup also fails.
Use a source failure latch before fallible work, clearing it only after validated success.
Perform no SQL in Drop or automatic reconnect.
Return no reference or intent after publication failure.

copy_into retains the borrowed source owner and sets its one-attempt latch before transfer work.
Call the existing configure_session on both source and target, including empty plans.
Require target session_replication_role=origin through the existing require_setting before admission.
Preserve the imported source transaction and captured target durability setting.
Its first scan_from(0) consumes a fresh source inventory in exact lockstep with the sealed entries.
For each entry, inspect and lock the source, then admit the target while the transient CopyRelation layout still exists.
Only then consume into_parts(stored_format), compare the exact canonical Event and format, and apply validate_copy_table.
Require exact source EOF and successful close after the scan.
No copied row is admitted until every entry succeeds.

The second scan_from(0) builds one bounded CopyTable from each borrowed Relation and its stored format.
Invoke the existing transfer_table with the sealed buffer budget and shared report/active flag.
Retain terminal COPY result checks and checked row, byte, and table counts.
Authority checks report.tables against intent.plan.table_count before completion.
Do not collect CopyPlan.tables, a whole-catalog map, or another unbounded container.
The source operation commits neither database.

## Target finalization and recovery

Change TargetTxn's internal next field to Option<Frontier>.
Existing CDC begin supplies Some with the genuine frontier.
Private COPY begin supplies None and rechecks the complete cached row, absent anchor, and applied=None.
Ordinary CDC commit with None and private COPY completion with Some must poison before mutative finalization SQL.
Reuse the existing unfinished Drop and transaction error latch.

TargetTxn owns SET CONSTRAINTS ALL DEFERRED and finalization.
Complete COPY by installing the anchor and one checked logged counter increment in the copied-data transaction.
Reuse finish(Synchronous) for constraint drain, complete-row validation, final durability restoration, and target COMMIT.
Preserve captured remote_apply and require positive COMMIT plus Idle before proof.
Keep the metadata3 complete-row guards and CAS.
Use bounded Clone or borrows for the anchor string.

SnapshotProof has private anchor and DurableProof fields.
It has no public constructor, deserializer, or generic-barrier conversion.
COPY leaves checkpoint None, order zero, and all CDC transaction fields NULL.
The first real CDC transaction remains sequence one.

Both copy_snapshot and recover_snapshot enclose all fallible work in the existing outcome/close pattern.
This includes checked intent and reader-open failures before BEGIN.
No early error propagation may bypass authority close/poison.
On transfer failure, abort active target COPY only on a usable connection and preserve the original error.
Retain the existing flag order and unfinished-transaction close behavior.

Unknown target COMMIT closes the target, retains source and intent, returns no proof, and forbids recopy.
Recovery first requires a freshly supplied acquire/reacquire connection and the released previous fence.
Open the checked plan and validate exact anchor and provenance before recovery mutation.
Perform a real logged counter UPDATE and confirmed synchronous COMMIT, preserving the anchor and actual optional CDC frontier.
A row read alone cannot construct proof.
Missing, partial, or mismatched anchors and uncertain barriers refuse without adoption, replacement, retry, or drop.

Recovery without a live SourceSnapshot proves target COPY completion only.
It does not reconstruct an importer/exporter, authorize feedback, prove slot continuity, or prove WAL and decoder availability.
CDC continuation separately requires fresh source identity and complete required history through the checked high-water position.
Finish only an actually retained live source owner, after acknowledged or recovered target proof.
Source-finish failure does not imply target rollback or permit recopy.

Counts remain advisory and never authorize completion or feedback.
If NativeState still validates, anchor recovery does not require reconstructed counts.
Damaged native publication remains fail-closed under the existing State rules.

## Task A: helper handoffs and exclusion

**Files:** `src/catalog.rs` and `src/copy.rs`, with their module-local tests.

- [ ] Integrate the exact final-v2 catalog and mechanical transfer handoffs through Root.
- [ ] Add only the approved catalog configuration visibility loans.
- [ ] Add the mandatory owned-target class-OID parameter to preflight_copy_target and its private lookup path.
- [ ] Check actual target membership before and after lock, before target data access.
- [ ] Test owned table/index refusal, invalid identity inputs, ordinary lookalike names, and target-versus-source OID distinction.
- [ ] Widen transfer_table visibility for its real owning consumer.
- [ ] Extract the existing validate_copy_table guard without changing its restriction, message, or E1 behavior.
- [ ] Test both metadata tags and execution refusal without rewriting format.
- [ ] Retain exact E1 wrapper preservation, scoped checks, and independent review evidence.

## Task B: source-owned producer and transfer

**File:** `src/snapshot.rs`, including its focused units and private live-child declaration.

- [ ] Implement the private bounded iterator and explicit cursor cleanup contract.
- [ ] Implement source publication using the actual private receipt and frozen store publisher.
- [ ] Preserve the requested format and reject constructor mismatches before storage accepts an entry.
- [ ] Return an unbound pure intent only after successful publication.
- [ ] Add failure and one-attempt latches without exposing the importer or exporter.
- [ ] Configure both source and target even for empty plans, and require target session_replication_role=origin through require_setting before admission.
- [ ] Implement the mandatory class-OID pass-through and complete first admission scan.
- [ ] Keep transient layout alive until target preflight completes.
- [ ] Implement the second per-table transfer scan without whole-plan collection.
- [ ] Retain source-session guards, terminal checks, original errors, and source finish after target proof.
- [ ] Add the private live-child declaration only when its real test file exists.
- [ ] Freeze source and focused evidence for separate review.

## Task C: authority, proof, and owned OIDs

**Files:** `src/apply/checkpoint.rs`, `src/apply/metadata.rs`, and `src/apply/mod.rs`.

- [ ] Add owned_relation_oids after locked validation, returning only verified table/index pg_class OIDs.
- [ ] Test missing, zero, duplicate, or malformed index identity refusal.
- [ ] Reuse the optional-frontier TargetTxn and poison wrong-mode finalization before SQL.
- [ ] Implement private begin/complete and owning copy_snapshot with internal reader opening.
- [ ] Construct the forbidden array before the confined target loan.
- [ ] Check the report table count before anchor completion.
- [ ] Reuse the existing synchronous finalizer and complete metadata3 row guards.
- [ ] Implement private SnapshotProof and only its required pure accessors/re-export.
- [ ] Implement fresh-fence recovery through an actual logged synchronous writing barrier.
- [ ] Enclose pre-BEGIN intent/reader errors in close/poison handling for both entrypoints.
- [ ] Test no generic proof promotion, unfinished poisoning, counter exhaustion, and preserved real CDC frontier behavior.
- [ ] Freeze the exact source and tests for independent review.

## Task D: private live8 and Root integration

**New files:** `src/snapshot_copy_live_tests.rs`, `tests/support/copy_snapshot_cases.json`, `tests/support/copy_snapshot_fixture.py`, and `tests/support/test_copy_snapshot_harness.py`.

**Root-owned integration:** `tools/test-db.py`, `tests/support/db_harness.py`, `Taskfile.yml`, `tools/ci/verify.py`, `tools/ci/test_verify.py`, and `tests/test_taskfile.py`.

**Narrow test seams:** `tests/support/authority_fault.py` COPY-start observations and the isolated cfg(test) create-failure adapter in `src/state.rs`.

Use existing OwnedCluster and authority/source-snapshot fault drivers.
Create supported primary-key source inventories instead of silently filtering the existing fixture's no-PK snapshot_data table.
Preserve the existing profile inventories and fault receipts.

Register exactly these eight groups under `snapshot::copy_live_tests::`:

1. `live_copy_snapshot_atomic`: verify real-provider binding, exact data, one COPY counter increment, and the first genuine CDC sequence one.
2. `live_copy_snapshot_empty`: establish genuine empty inventory and atomic empty COPY completion.
3. `live_copy_snapshot_admission`: exercise 128/129 tables, plans beyond 16 KiB, late refusals, exact format mismatches, and Binary metadata preservation with zero COPY starts.
4. `live_copy_snapshot_rollback`: require a reachable terminal COPY failure and rollback of earlier copied data plus absent anchor.
5. `live_copy_snapshot_unknown`: lose the actual target COMMIT reply, observe the held/released fence, recover through a writing barrier, and prove no duplicate transfer.
6. `live_copy_snapshot_recovery_refusal`: refuse missing/foreign anchors and failed barriers, including pre-BEGIN intent/reader failures that poison authority.
7. `live_copy_snapshot_source_finish_unknown`: lose source finish after target proof without reinterpreting target completion as rollback.
8. `live_copy_snapshot_advisory_counts`: retain target completion after missing or failed count publication and preserve native fail-closed behavior.

For the terminal UNIQUE case, use matching columns `id int PRIMARY KEY, payload int`.
Insert source rows `(1,7)` and `(2,7)`.
Add only a target-side ordinary UNIQUE index on payload, with no CHECK, FK, expression, partial index, or eligibility expansion.
The frozen target profile permits this plain built-in btree index while requiring the primary key itself to be immediate.
Require successful admission and observed COPY start before terminal SQLSTATE 23505.
Copy an earlier table first and verify that its data and the anchor roll back.
Prove this reachability live; do not label it deferred-constraint coverage.
Keep deferred-constraint mutation cases in existing shared-finalizer component coverage.

Root owns this exact test-only State seam:

```rust
#[cfg(test)]
impl NativeState {
    pub(crate) fn record_copy_complete_with_create_failure(
        &mut self,
        report: &CopyReport,
    ) -> Result<()>;
}
```

The count-write fault adapter invokes actual record_copy_complete under the existing PublicationStep::Create guard.
Expose only the cfg(test) visibility needed for that existing guard.
Create no temporary file in this case, so checked native intent remains reopenable for target recovery without counts.
Test damaged native publication separately as refusal.
Add no State PG or concrete backend dependency.

- [ ] Register a separate CopySnapshot profile with exact nonempty discovery and expected eight-case output.
- [ ] Require zero ignored cases during live execution.
- [ ] Record COPY-start observations so rollback cannot hide early transfer before late refusal.
- [ ] Run meaningful runtime controls for erased format, early transfer, ignored transaction mode, false EOF, missing table, and read-only recovery proof.
- [ ] Use counter deltas from the provisioning baseline instead of assuming an initial zero.
- [ ] Add required offline harness discovery and missing-module/receipt refusal controls.
- [ ] Add the profile to the default database gate while retaining prior profiles.
- [ ] Obtain separate reviews before Root's sole PostgreSQL lease.
- [ ] Qualify the exact frozen candidate on the declared PostgreSQL 14/17 profiles.
- [ ] Retain source, binary, actual native-library, fault, and cleanup receipts.
- [ ] Keep all mutants in disposable ordinary exports, never the primary checkout.
- [ ] Complete required lint/check/test and hosted gates before any later Root-authorized commit or push.

## Root documentation and release checklist

Root retains the spec, usage docs, tasks, source composition, and release readiness.
This plan does not transfer those files to operation workers.

- [ ] Correct the spec's stale Native3 assignment, raw-source signature, bind signature, and source-finish order.
- [ ] Record the verified class-OID flow and target-only recovery boundary in the spec.
- [ ] Update `docs/getting-started.md` to distinguish stored Text/Binary tags from existing Text-only execution.
- [ ] Update `docs/testing.md` for exact live8 discovery, receipts, controls, and retained profile inventories.
- [ ] Update `docs/durability.md` for target proof, source/history limits, Unknown handling, and advisory counts.
- [ ] Integrate the actual frozen store, exact locked runtime dependency, registration, and prerequisite sources without replacing Native4/metadata3 State.
- [ ] Preserve the authoritative gate status in `tasks/todo.md` and prerequisite receipts.
- [ ] Save the reviewed plan/spec/docs before releasing disjoint A/B/C/D operation edits.
- [ ] Keep public CLI, production WAL orchestration, backend selection, and performance outside this package.

No ownership redesign or Binary execution choice remains in this plan.
Concrete helper handoffs and qualification are release prerequisites.
