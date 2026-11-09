# Identity UPDATE Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.
> Implementation begins only after the leader accepts this interface and ownership freeze.

**Goal:** Replay ordinary and identity-changing UPDATE events exactly, including legal source `SET id = DEFAULT`, without target identity errors or omitted changes.

**Architecture:** Retain ordinary UPDATE when the pgoutput old image proves every target ALWAYS identity unchanged.
Otherwise, replace exactly one locked physical row in one statement using DELETE RETURNING followed by INSERT OVERRIDING SYSTEM VALUE.
Carry the actual local identity modes and declarative partition root modes through the existing source plan, and retain the actual target modes in the checked relation cache.

**Tech Stack:** Existing single Rust Cargo package, libpq extended-query prepared execution and pipelines, PostgreSQL pgoutput v1, existing owned PostgreSQL test harness.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md), [roadmap](../../roadmap.md), and [M3 follow plan](2026-11-08-m3-follow-pgoutput.md).

## Global constraints

- Preserve transaction content, atomicity, and source commit order, as required by design lines 204-208.
- Preserve repeated keys, key changes, FULL duplicates, NULL, unchanged TOAST, and partition movement, as required by design line 208.
- Preserve explicit identity values using the required overriding syntax, as required by M3 plan line 386.
- Keep the design's replica apply profile and source/target DDL quiescence.
- Refuse all replica-enabled ALWAYS/REPLICA triggers, including internal FK triggers, under the leader-approved closure of that callback profile.
- Keep generated-column, CHECK/domain CHECK, exclusion, expression-index, unsafe-opclass, RLS, and rule refusals.
- Preserve the existing target transaction/checkpoint/observed-COMMIT authority and failure poisoning.
- Never change a target identity definition, substitute a target DEFAULT, omit a real identity change, or advance a checkpoint after failed DML.
- Keep credentials and row contents out of diagnostics and receipts.
- Add no dependencies, source-spool mechanism, per-DML sequence synchronization, or transport implementation.
- Use native format 7 and target metadata format 5.
- Reject old native format 6 before database side effects, with no compatibility shim or conversion.

## Review focus

- A cached ordinary UPDATE must not accept a later changed-ALWAYS event with the same TOAST mask.
- An ALWAYS column outside the replica identity has no old-value proof in a K tuple or an UPDATE without K/O.
- DELETE and INSERT must share one statement and one selected physical row, including when leaves have equal ctid values.
- A uniqueness or NOT NULL failure after DELETE must restore that row and prevent checkpoint/feedback progress.
- PostgreSQL 16 and 18 represent partition identity differently, and PG16 permits independent leaf identity definitions that PG18 cannot represent.

## Baseline and proven cause

The frozen donor is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-precedence-candidate-ksroj_em/tree`.
Its manifest SHA-256 is `21222aa8bfd191b76bd3a215e5c28f1c8cfd66cedc08d14eee3db4dfd6c0340a`.
Its underlying HEAD is `1983a6a4e2aec62c8096c008316442c310be88d0`, but the frozen assembly contains uncommitted work and must be identified by its manifest.

Actual follow smoke attempt six returned SQLSTATE `428C9` from target DML.
`src/apply/plan.rs:83-87` selects every available new column and lines 133-140 assign all of them.
`src/catalog/follow.rs:642-683` reads `attidentity` but collapses it into one INSERT-only `override_identity` boolean.
The corrected source fixture's ordinary UPDATE is legal.

The PostgreSQL rewriter rejects any explicit ALWAYS assignment in UPDATE, even if the value equals the old value, and UPDATE has no OVERRIDING clause.
The decoded source value from a legal DEFAULT update must therefore be inserted explicitly rather than regenerated on the target.
See [PG16 rewriteHandler.c](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/rewrite/rewriteHandler.c#L938-L952) and [PG18 rewriteHandler.c](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/rewrite/rewriteHandler.c#L942-L956).

## Exact shared interface proposal

### Persisted source metadata

Add these fields to `contracts::CloneTable`, aligned with `all_columns`:

```rust
pub(crate) identity_modes: Vec<u8>,
pub(crate) partition_identity_modes: Option<Vec<u8>>,
```

Each byte is exactly `0`, `b'a'`, or `b'd'` for no identity, ALWAYS, or BY DEFAULT.
`identity_modes` records actual local `pg_attribute.attidentity`.
`partition_identity_modes` records the declarative partition root's modes by matching column name, and is `None` for an ordinary table or ordinary inheritance child.
The vector is present for a declarative leaf even when all its root modes are zero.
Require the local vector and each present root vector to have exactly `all_columns.len()` entries.
Reject missing fields, invalid bytes, width mismatches, missing root attributes, and duplicate/missing metadata rows.
Do not use serde defaults or infer that absent evidence means no identity.

Add the small shared catalog helper:

```rust
pub(super) fn clone_identity_modes(
    connection: &mut PgConnection,
    oid: u32,
    columns: &[String],
    io: &IoControl,
) -> Result<(Vec<u8>, Option<Vec<u8>>)>;
```

Keep `clone_columns(...) -> Result<(Vec<String>, Vec<String>)>` unchanged.
The helper reads actual local attributes and declarative partition membership, matches their ordered names to `columns`, and uses `pg_catalog.pg_partition_root(oid)` plus root column names for the optional second vector.
Do not recurse through ordinary table inheritance or align root columns by attnum.
Use one bounded catalog query with at most 1600 live columns and refuse incomplete evidence.

`inspect_clone_tables` populates both fields.
`reconcile_clone_tables` compares current target modes using the cross-version rule below.
`inspect_source_tables` rereads and compares both vectors before follow resources and on reconnect/restart.
The existing preliminary/snapshot plan equality now covers source identity definitions.

### Partition compatibility

PostgreSQL 16.15 gives ordinary inherited identity columns on declarative leaves an empty local identity flag.
PostgreSQL 18.6 gives those leaves the inherited root flag.
The SQL probes below establish this difference for the supported versions.
Use both vectors so this representational difference cannot hide independent leaf definitions or drift.

- For 16 to 16 and 18 to 18, require exact local and root vectors.
- For 16 to 18, require exact root vectors and require every source local leaf mode to be zero before creating publication/slot or mutating target data.
- For an allowed 16 to 18 leaf, the expected target local vector is its recorded source root vector.
- For non-partition tables, require exact local modes and `None` on both endpoints.
- Preserve a legal PG16 leaf-local identity in 16 to 16.
- Refuse independent PG16 leaf identities for 16 to 18 with an explicit cross-version DDL incompatibility error.

That last case is a PostgreSQL schema incompatibility: PG16 allows independently ADD GENERATED BY DEFAULT on a leaf under an ALWAYS root, whereas PG18 rejects it with `42P16`.
Do not change either schema to make it representable.
See [PG18 ATExecSetIdentity](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/commands/tablecmds.c#L7894-L7916) and the disposable probes below.

No new major-version field is needed.
Source admission has `ClonePlan.source.major` and `ClonePlan.target.major`.
Target relation admission can read its existing server major: an allowed source local zero on a declarative leaf maps to the recorded root mode on target 18, and stays local zero on target 16.
The earlier source admission establishes the source-major compatibility precondition.

### Checked target metadata

Replace `FollowRelation.override_identity: bool` with:

```rust
pub(crate) identity_modes: Vec<u8>,
```

Retain `FollowRelation.relation: Event` and `target_type_oids: Vec<u32>`.
The new vector contains actual target physical-table modes in published-column order.
Target preflight compares actual local/root metadata against `OwnedCopyTable.table` using the rules above, then returns the actual local modes for SQL planning.
An INSERT adds OVERRIDING SYSTEM VALUE exactly when any actual target mode is `b'a'`.

Keep `TargetTxn::preflight_follow_relation` and `DmlPlan::new` signatures unchanged.
Charge `identity_modes.capacity() * size_of::<u8>()` in `serial::Caches::relation` and reserve `1600 * (size_of::<u32>() + size_of::<u8>())` before adding a checked relation.
Include every new owned plan vector in `DmlPlan::owned_capacity_bytes`.

### Plan and result shape

Add `always_columns: Vec<usize>` and `rewrite_identity: bool` to the owned `DmlPlan`.
Keep the existing operation, key columns, write columns, SQL, parameter OIDs, and borrowing bind API.
Freeze these plan and cache-facing signatures:

```rust
pub(super) fn new(checked: &FollowRelation, row: &Event) -> Result<Self>;
pub(super) fn matches(&self, row: &Event) -> Result<bool>;
pub(super) fn bind<'a>(&self, row: &'a Event) -> Result<Vec<PgParam<'a>>>;
pub(super) fn owned_capacity_bytes(&self) -> Result<usize>;
pub(super) fn expected_tag(&self) -> &'static str;
```

It returns `INSERT 0 1` for INSERT and rewritten UPDATE, `UPDATE 1` for ordinary UPDATE, and `DELETE 1` for DELETE.
Change `serial::require_result` to `require_result(plan: &DmlPlan, result: PgResult) -> Result<()>`.
Both the direct/oversize path and pipeline validation select the cached plan and require zero returned client rows plus its exact expected command tag.
Do not accept both INSERT and UPDATE tags for every source UPDATE.

## SQL and safety decisions

### Ordinary UPDATE proof

For each actual target ALWAYS column, accept unchanged only when one of these holds:

1. An O/FULL tuple has the same available value at that position as the new tuple.
2. A K tuple contains that replica-identity column and its old and new values match.
3. There is no K/O tuple and that column belongs to the replica identity, because pgoutput omits K when no identity key changes.

Compare available identity integer values exactly and conservatively.
Do not treat K tuple placeholders for non-key columns as old values.
Do not infer that a non-key ALWAYS column is unchanged merely because K/O is absent.
Reject an unavailable ALWAYS value rather than binding it as NULL.
The protocol's K/O rules are documented for [PG16](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-UPDATE) and [PG18](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html#PROTOCOL-LOGICALREP-MESSAGE-FORMATS-UPDATE).

When every ALWAYS value is proved unchanged, omit only those ALWAYS assignments and keep the existing ordinary UPDATE and unchanged-TOAST omission.
BY DEFAULT columns remain writable, including replica-identity mutations.
If omission leaves no assignment, use the rewrite path so an actual selected row and exact result remain proved without adding a separate result protocol.

`matches` and `bind` must reclassify the event using the stored ALWAYS/key metadata.
A cached plan matches only if operation, rewrite decision, and resulting write-column mask match.
This prevents an unchanged-identity plan from absorbing a real identity change.

### Atomic identity rewrite

Generate one statement in the existing DmlPlan builder:

```sql
WITH rpgmigrate_old AS (
    DELETE FROM ONLY "schema"."physical_heap"
    WHERE ctid = (
        SELECT ctid FROM ONLY "schema"."physical_heap"
        WHERE <existing NULL-safe old identity predicate>
        LIMIT 1 FOR UPDATE
    )
    RETURNING "id", "body", "other"
)
INSERT INTO "schema"."physical_heap" ("id", "body", "other")
OVERRIDING SYSTEM VALUE
SELECT $1, rpgmigrate_old."body", $2 FROM rpgmigrate_old
```

This is the required SQL shape, with identifiers, column lists, parameters, and old predicates derived from the checked relation.
The target is always the exact owned physical heap, including for both ctid reads and deletion.
Do not retarget a partition root or include descendants.
For FULL, reuse the existing C-collated text `IS NOT DISTINCT FROM` predicates, preserving duplicates and NULL.
For DEFAULT/INDEX, reuse the existing typed NULL-safe old-key predicates.
Use a separate raw predicate string so the FULL ctid wrapper is not nested twice.

The selection locks at most one matching physical row and DELETE RETURNING supplies that row to the INSERT.
Each non-TOAST new value uses its parameter; each unchanged-TOAST position uses the corresponding returned target value.
Provide all writable target columns explicitly, so no identity sequence or ordinary column default runs.
Do not use ON CONFLICT, a target-generated DEFAULT, separate DELETE/INSERT commands, or an UPDATE and DELETE of the same tuple in one statement.
The final INSERT 0 1 proves one deleted row reached one insertion because the selector permits at most one row and no admitted callback can suppress either operation.
INSERT 0 0 is a divergence error, with rollback and no checkpoint progress.

PostgreSQL documents DELETE RETURNING feeding a data-modifying CTE's INSERT, shared statement snapshots, and RETURNING as the communication channel between modifications.
The INSERT consumes the deletion output rather than attempting to observe changed base-table state.
See [PG16 WITH](https://www.postgresql.org/docs/16/queries-with.html#QUERIES-WITH-MODIFYING) and [PG18 WITH](https://www.postgresql.org/docs/18/queries-with.html#QUERIES-WITH-MODIFYING).

### Callback, constraint, and sequence equivalence

The preflight must remove the erroneous `NOT t.tgisinternal` exclusion from ALWAYS/REPLICA trigger checks on both endpoints, including the early clone/source inventory before resources.
Ordinary origin-only FK triggers remain admitted and do not fire under replica apply.
Origin-only user triggers/rules also remain disabled on the target.
CHECK/domain CHECK, generated expressions, exclusion constraints, expression/partial indexes, and unsafe methods/opclasses remain refused by the existing execution profile.
See [session_replication_role](https://www.postgresql.org/docs/16/runtime-config-client.html#GUC-SESSION-REPLICATION-ROLE) and [pg_trigger firing modes](https://www.postgresql.org/docs/18/catalog-pg-trigger.html).

Under those admitted conditions, the rewrite's final row contains exactly the decoded new values and preserved old TOAST values.
Built-in NOT NULL and immediate unique constraints still validate the resulting row; an error rolls back both operations and the enclosing target group.
Replica apply suppresses ordinary origin-only deferred UNIQUE rechecks, so retain SET CONSTRAINTS ALL IMMEDIATE without treating it as replica deferred-failure proof.
Test a source-valid deferred-UNIQUE permutation through replica apply with exact row parity and target sequence nonadvance.
Keep deferred-failure rollback coverage in the generic origin-mode Authority profile.
Do not claim equivalence if an active replica callback remains admitted.

OVERRIDING SYSTEM VALUE supplies decoded identity values without consuming target nextval.
The same applies to BY DEFAULT values supplied explicitly.
See [PG18 INSERT overriding semantics](https://www.postgresql.org/docs/18/sql-insert.html#SQL-INSERT-OVERRIDING).
Source and target sequence counters may differ during follow.
Final counter parity remains the existing M4 quiesced sequence-sync obligation.

### WITH admission

Add the exact uppercase first token WITH to `pg::ffi::is_data_command` and the separate `TargetTxn::prepare` DML allowlist.
The shared ffi helper already covers PgConnection::prepare and Pipeline::query.
Retain extended-protocol single-statement preparation/execution, all transaction-state checks, result bounds, poison latches, and the server COMMIT-tag rejection.
Do not add utility commands or an SQL parser.

The extended Parse message rejects multiple statements, while a WITH CTE body and its parent accept preparable DML rather than COMMIT or other transaction control.
See [PG18 extended protocol](https://www.postgresql.org/docs/18/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY), [PG16 grammar](https://github.com/postgres/postgres/blob/REL_16_15/src/backend/parser/gram.y#L11118-L11124), and [PG18 grammar](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/parser/gram.y#L11380-L11386).
Verify those properties through the actual authority and transport APIs, including WITH followed by a second COMMIT statement.

## File ownership and task split

The leader assigns one owner per file before implementation.
Catalog, apply, transport, and authority work can begin in parallel after the field/signature freeze.
Run broad checks once after integration; scoped RED/GREEN checks belong to each task.

### Task 1a: Read and compare source/target identity metadata

**Owner:** Source/catalog worker, high effort because bounded schema matching needs exact cross-version handling.

**Files:** `src/catalog/clone.rs` and `src/catalog/follow.rs`, including their inline tests.

**Consumes:** Existing ClonePlan major versions and OwnedCopyTable identities.
**Produces:** `clone_identity_modes` and FollowRelation.identity_modes exactly as frozen above.

- [ ] Write RED tests for independent inheritance, source mode drift, and target ALWAYS/BY DEFAULT drift.
- [ ] Add real PostgreSQL 16/18 catalog controls for plain ALWAYS/BY DEFAULT, declarative root/leaf modes, normal origin FK acceptance, and user/internal A/R trigger refusal with unchanged foreign sentinel data.
- [ ] Add cross-version catalog cases for a normal 16 to 18 partition and source preflight refusal of an independent PG16 leaf identity before resources/target data.
- [ ] Implement the bounded identity query, source reread, target reconciliation, actual FollowRelation modes, and trigger-guard correction.
- [ ] Run scoped catalog tests on each major and retain exact expected case inventories and failures.

### Task 1b: Freeze persisted format and shared fields

**Owner:** Format/shared worker, high effort because missing persisted identity evidence must fail before side effects.

**Files:** `src/contracts.rs`, `src/state.rs`, `src/state/clone_state.rs`, and `src/state/clone_state/tests.rs`.

**Produces:** The two CloneTable fields, their strict validation, and native 7 / metadata 5 exactly as frozen above.

- [ ] Write RED tests for missing/invalid/short vectors, an empty present partition vector, and native 6 refusal before database side effects.
- [ ] Add both exact CloneTable fields without defaults, validate widths/bytes, and advance only FORMAT_VERSION from 6 to 7.
- [ ] Keep target metadata 5 and its exact 25-column layout and FollowControl serialization unchanged.
- [ ] Update owned-plan/state equality and bounded serialization tests so both vectors contribute to the existing manifest-size limit.
- [ ] Run scoped state unit tests and prove round-trip equality for ordinary tables, inherited tables, standard partitions, and PG16 independent leaf identities.

### Task 2: Admit single-statement WITH through transport

**Owner:** Transport worker, high effort because transaction escape must remain impossible.

**Files:** `src/pg/ffi.rs`, `src/pg/pipeline.rs`, `src/pg/prepared_live_tests.rs`, `src/pg/pipeline_live_tests.rs`, and `tests/support/prepared_cases.json` if new required names are added.

**Produces:** Canonical WITH admission using the existing extended protocol, with unchanged public transport signatures.

- [ ] Write RED preparation and pipeline tests for the exact DELETE RETURNING / INSERT statement.
- [ ] Add negative controls for `WITH x AS (COMMIT) SELECT 1`, `WITH x AS (SELECT 1) COMMIT`, and `WITH x AS (SELECT 1) SELECT 1; COMMIT` through actual extended APIs.
- [ ] Assert rejection cannot commit an earlier sentinel write, change transaction ownership, or bypass the checked COMMIT boundary.
- [ ] Add WITH to the shared data-command helper and update the old test that treats every WITH as forbidden.
- [ ] Keep all other utility and transaction-control pre-dispatch refusals.
- [ ] Run the scoped unit/live transport cases on 16/18 with zero ignored required cases.

### Task 3: Build complete identity UPDATE plans and checked results

**Owner:** Apply worker, xhigh effort because event classification, locked-row replacement, and cache reuse affect data correctness.

**Files:** `src/apply/plan.rs`, `src/apply/plan/tests.rs`, `src/apply/serial.rs`, `src/apply/serial/tests.rs`, and new `src/apply/plan/identity_tests.rs` if the added regression cases need a focused module.

**Consumes:** Task 1 checked actual target modes and Task 2 WITH support.
**Produces:** DmlPlan fields, `expected_tag`, and plan-based `require_result` exactly as frozen above.

- [ ] Write RED planner tests for ordinary non-ID UPDATE on ALWAYS PK tables, actual changed ALWAYS keys, non-key ALWAYS with missing old image, FULL old proof, and legal BY DEFAULT key mutation.
- [ ] Test alternating unchanged/changed identities with identical TOAST masks against matches/bind, including reverse cache reuse and caught bind refusal.
- [ ] Test multiple ALWAYS columns, unchanged identity with no remaining ordinary assignments, unavailable identity values, and exact target parameter OIDs/order.
- [ ] Implement the ordinary proof path and the single-statement rewrite, preserving existing quoting, NULL predicates, and physical ONLY ownership.
- [ ] Use returned target values for unchanged TOAST and explicit new parameters for every other writable column.
- [ ] Update cache capacity accounting and both direct/pipeline result-validation callers.
- [ ] Add GREEN serial/pipeline differential tests for changed ALWAYS values, BY DEFAULT PK mutation, FULL duplicates/NULL, large unchanged TOAST, repeated updates, identity-only rows, and equal ctid partition leaves.
- [ ] Require missing rows to fail via `INSERT 0 0` and prevent group progress.

Run the scoped pure tests:

1. Test plan construction.

   ```sh
   cargo test --locked --lib apply::plan::
   ```

2. Test serial apply.

   ```sh
   cargo test --locked --lib apply::serial::
   ```

3. Test follow apply.

   ```sh
   cargo test --locked --lib apply::follow::
   ```

### Task 4: Preserve authority admission and rollback proof

**Owner:** Authority worker, xhigh effort because checked DML admission and checkpoint atomicity are safety boundaries.

**Files:** `src/apply/checkpoint.rs` and `src/apply/checkpoint_live_tests.rs`.

**Consumes:** Task 2 single-statement transport behavior and Task 3 exact result shape.
**Produces:** WITH admitted by TargetTxn::prepare, preserving its existing API and poison/rollback rules.

- [ ] Write RED authority tests for successful prepared rewrite inside the data/checkpoint transaction and WITH transaction-escape refusal.
- [ ] Add the exact uppercase WITH token to TargetTxn::prepare without exposing an unchecked connection or utility path.
- [ ] Add actual PostgreSQL 16/18 late unique and NOT NULL failures after a successful earlier row update, then prove all user rows and checkpoint/frontier remain at their prior committed values.
- [ ] Test a source-valid deferred-UNIQUE permutation through actual replica follow with exact row parity and target sequence nonadvance.
- [ ] Retain deferred UNIQUE failure at SET CONSTRAINTS ALL IMMEDIATE with rollback proof only in generic origin-mode Authority.
- [ ] Verify missing-row and wrong command-tag results cannot authorize a checkpoint or SafeFeedback proof.
- [ ] Retain existing known/unknown-COMMIT and durable-feedback tests without changing their authority construction.

### Task 5: Integrate fixtures, required inventories, and docs

**Owner:** Integration worker, medium effort for mechanical caller updates and existing harness registration.

**Files after their owners freeze:** Constructor-only updates in `src/copy/clone.rs`, `src/compare/tests.rs`, and any remaining CloneTable literals in authority/catalog-owned files, coordinated with their owners.
Own `tests/support/follow_apply_cases.json`, other affected required case manifests, `tools/test-db.py` only if the exact new module needs dispatch registration, `tools/e2e/follow.py`, `tools/e2e/test_follow.py`, `docs/testing.md`, `docs/roadmap.md`, `tasks/todo.md`, the M3 plan, and the M4 successor-version sentence.

- [ ] Register every new live regression in the required profile and add the missing-case negative control using the existing harness pattern.
- [ ] Update all remaining CloneTable/FollowRelation constructors to explicit correct metadata; do not default all test identity fixtures to no identity.
- [ ] Preserve the existing valid source workload and its BY DEFAULT key-mutation coverage.
- [ ] Add a legal source `UPDATE ... SET id=DEFAULT` workload to identity regression coverage, preserving the existing schema and ordinary-update regression for `428C9`.
- [ ] Keep M3 stopped/ongoing follow distinct from completed M4 follow.
- [ ] Document native 7 refusal, metadata 5, replica-enabled internal-trigger refusal, and the genuine PG16 independent-leaf to PG18 DDL incompatibility.
- [ ] Update M4's accepted-M3 conditional successor to native 8 / metadata 6 if this M3 baseline is accepted as 7 / 5.
- [ ] Record the exact frozen implementation tree before running the final gates.

### Task 6: Independent verification and one closing review

**Owner:** Independent verifier and leader-allocated xhigh safety reviewer, after all writers freeze.

- [ ] Run the required focused live profile on PostgreSQL 16.15 and 18.6, including all new identity, callback, transport, and authority cases with zero missing/ignored cases.
- [ ] Require a real source DEFAULT update to change its ALWAYS value, then verify exact target data, original schema identity mode, nonadvance of the target sequence during replay, and unchanged-TOAST bytes.
- [ ] Compare serial and pipeline results to the independent expected SQL multiset, not only to each other.
- [ ] Run the actual follow smoke on 16 to 16, 18 to 18, and 16 to 18 after focused tests pass, with exact schema/data/owned-state oracles and retained primary stderr/status.
- [ ] Run the repository's required lint and test targets once at milestone close, including formatting, strict all-target Clippy, Cargo suites, harness self-checks, and affected Python tests.
- [ ] Keep unchanged operator Go suites with the operator milestone's existing close gate.
- [ ] Review identity plan classification, cross-major metadata, FULL physical selection, callbacks/defaults, constraints/rollback, WITH escape controls, accounting, and checkpoint/feedback authority.
- [ ] Commit only after the leader requests it and required checks pass.

1. Run the focused live profile inside an owned stock PostgreSQL environment.

   ```sh
   task test-follow-apply
   ```

Check out these **examples** for selecting a version with the existing harness environment:

- PostgreSQL 16.15:

  ```sh
  PG_BINDIR=/usr/lib/postgresql/16/bin RPGMIGRATE_TEST_EXPECTED_PG_VERSION=16.15 RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM=160015 task test-follow-apply
  ```

- PostgreSQL 18.6:

  ```sh
  PG_BINDIR=/usr/lib/postgresql/18/bin RPGMIGRATE_TEST_EXPECTED_PG_VERSION=18.6 RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM=180006 task test-follow-apply
  ```

## Research receipts and limitations

SQL-only disposable probes used the frozen donor's exact OwnedCluster helper on the retained stock container, with no engine or Cargo execution.
Both owned clusters closed after each probe window, and both resource leases were returned explicitly.
The initial probe used invalid ALTER TABLE shorthand for its final trigger control, corrected that probe syntax, and reran both majors successfully.
No product workaround or target schema change was made.

Receipts: [atomic rewrite SQL proof](/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/identity-update-sql-rej0rjkq/receipt.json) and [partition compatibility SQL proof](/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/identity-update-sql-rej0rjkq/partition-receipt.json).
The private container helper/scripts remain at `/work/m3-identity-plan-ab5b5ab195`; the harness removed its owned database and socket directories.

Both PG16.15 and PG18.6 proved legal source DEFAULT changes, ordinary omitted-ALWAYS UPDATE, one-statement INSERT 0 1 replacement, preservation of a 1,280,000-byte body, target sequence nonadvance, 23505 rollback of deleted row and earlier checkpoint work, exactly one FULL duplicate changed, NULL/empty preservation, and ONLY leaf isolation despite equal ctid values.
They also proved that normal FK triggers do not fire in replica mode and that internal ALWAYS FK triggers are detectable by the proposed unrestricted trigger predicate.
The root-mode query passed both majors; the independent leaf identity ADD passed PG16 and failed PG18 with 42P16.

These are SQL semantics probes, not engine qualification.
The new Rust plans, prepared cache behavior, authority integration, default-gate registration, native serialization, and complete three-pair follow convergence still require implementation and the gates above.

## Self-review

The plan covers ordinary ALWAYS updates, legal DEFAULT identity changes, BY DEFAULT key changes, missing old non-key images, cache shape reuse, FULL duplicates, NULL, unchanged TOAST, physical partition ownership, target sequence nonadvance, callbacks, and transactional failure.
It changes the persisted native plan but leaves target DDL/binding semantics untouched, so native 7 / metadata 5 is sufficient.
The only newly documented incompatibility is independently configured PG16 leaf identity DDL that stock PG18 cannot represent.
No temporary identity-update refusal substitutes for support.

## Open issues

No unresolved design issue blocks the implementation tracks above.
The implementation and independent safety review must prove the listed cases through the actual Rust and authority APIs; SQL-only probe success does not close those gates.
