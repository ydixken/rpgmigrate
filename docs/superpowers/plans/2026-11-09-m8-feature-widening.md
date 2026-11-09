# M8 Feature Widening Implementation Plan

Root approved this implementation plan on 2026-11-09 within the standing execution scope through M10.
The approved private source is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m8-plan-20261109-v6ua5u0h/m8-feature-widening-implementation-plan.md`, SHA-256 `0af7deed08cc6f9951820ac91dde0ae4cf968e8932104f8f32f7ee7ca1073f81`.
This repository copy preserves the approved protocol/design and historical planning assertions while applying documentation style.
**Execution note:** Existing approved milestone scope authorizes owned M7 UP/load/legacy/native tests and ordinary fresh owned fault fixtures; source freeze, guard checks, exact ownership, resource admission, and required evidence remain prerequisites rather than a new blanket user-permission gate.
Benchmark execution stays on HOLD for the final actual artifact/argv packet review, and shared-VM poweroff, original retained resources, main pushes/merges, tags, releases, and GHCR publication remain outside this authorization.
This note supersedes historical planning-only or separate-approval wording below where it would request approval again for already approved owned work; it claims no execution result or milestone closure.

> [!IMPORTANT]
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task by task.
> Keep one owner per file and run the independent review and Go suites once at milestone close.
> This private plan grants no implementation, runtime, capability, benchmark, or publication authority.

**Goal:** Qualify binary COPY with text fallback, ctid split completion, and large-object lifecycle, then repeat the operator-default matrix on 16:16, 18:18, and 16:18.

**Architecture:** We extend the existing clone plan, per-run target authority, COPY worker pool, comparison command, and operator adapter.
We keep table completion separate from part completion and large-object completion.
We use the existing asynchronous SQL and prepared-parameter interfaces rather than add blocking libpq large-object calls.

**Tech Stack:** One Cargo package, stock PGDG libpq, PostgreSQL client tools matching target major, retained Python harnesses, and the separate Go operator repository.

**Spec:** `/Users/ydixken/development/rpgmigrate/docs/roadmap.md`, especially M8 at lines 365–370 and the execution/amendment sections, plus `docs/superpowers/specs/2026-11-06-rpgmigrate-design.md` and `docs/operator-compatibility.md`.
Roadmap amendments override conflicting older design details.

## Global constraints

- Preserve feature-branch isolation and one owner per file.
- Native pairs are exactly 16:16, 18:18, and 16:18.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources.
- Keep bookkeeping exclusions tied to exact owned object identities.
- Preserve durable completion and explicit failure for unsupported work or recovery gaps.
- Keep credentials and row contents out of diagnostics and version control.
- Before the follow-copy anchor, restart only the recorded owned data phase with a fresh snapshot and generation.
- After the anchor, continue from the durable checkpoint.
- Final sequence synchronization and large-object re-sync happen after drain, under the operator writer fence, before exit zero.
- The source slot retains WAL through COPY and until target durability proof permits feedback.
- M9 benchmarks remain on HOLD.
- No main push or merge, tags, releases, or GHCR publication.
- Feature pushes have separate standing authorization and require passing applicable gates first.
- Use native format 9 and target metadata version 7 for new M8 runs, as frozen by the leader.
- Reject format 8 / metadata 6 before mutation, with no conversion or repair.
- Preserve existing active M6/M7 workers and their files until their leader releases ownership.

## Evidence scope and inputs

Engine reads used primary branch `feat/native-engine`, HEAD `0836846c7cf605371d4202651c015cdb802724da`.
Only the roadmap, lessons, and todo files were dirty when inspected.
This plan does not read todo or older dated plans as design inputs.
Operator reads used `/Users/ydixken/development/pgcopydb-operator-m4-closing-m6-composed-20261109`, HEAD `4ecbd69cc9e56e4710096b6f0475fc7d2a62fe8b`.
The original operator checkout and its dirty todo/.agents files were not inputs.
Fork references below use Git object `86bf908adca02d87063849dae775a02c1659c829`, independently of the fork checkout HEAD.
No tests, VM operations, installations, benchmarks, commits, or pushes were executed for this plan.

## Review focus

1. A binary stream includes header/trailer data, so buffer count must never become row count.
Task 2 tests empty, one-row, and multi-row binary streams against target COPY command counts.
2. A split transaction must never truncate another part's committed rows or use COPY FREEZE without a same-transaction truncate.
Tasks 3 and 4 test concurrent parts, killed workers, and worker SQL.
3. A new snapshot can change ctid layout and large-object membership.
Tasks 3–5 invalidate old part/object authority before a new generation and test interrupted resets.
4. An OID alone does not prove that a target large object is owned by this run.
Task 5 binds creation and metadata to the native run and preserves unrelated target objects on refusal.
5. A matching table array does not prove large-object equality or an omitted empty table inventory.
Tasks 5–7 require LO comparisons and independently justified empty-inventory handling.

## Source-backed decisions

### Binary predicate: leader-frozen option A

Option A is the selected minimum: a finite, proven built-in scalar allowlist, with whole-table text fallback if any writable column fails.
Candidate scalar names are `bool`, `bytea`, `int2`, `int4`, `int8`, `float4`, `float8`, `numeric`, `text`, `varchar`, `bpchar`, `date`, `time`, `timestamp`, `timestamptz`, `interval`, `uuid`, `json`, and `jsonb`.
This list is a qualification candidate, not an assertion that all entries are already compatible.
The catalog owner must verify each candidate's source and target OID, namespace, type kind, typmod, and send/receive identities against primary PostgreSQL sources and live cases on the three pairs.
Require matching server encodings and effective client encodings wherever the binary representation carries text.
A mismatch selects text before target mutation.
Domains, enums, arrays, composites, ranges, multiranges, extension types, and unknown types select text.
A known built-in OID alone is insufficient.
Keep the existing broader textual-representation admission separate.

Option B would recursively qualify containers and their elements.
It adds wire-format and user-type-OID obligations without serving the selected M8 minimum, so it is deferred.

Fork evidence: `schema.c:3335–3388` and `3503–3515` retain the `tsvectorsend` blocklist and binary-I/O checks.
`table-data.c:1347–1366` falls back for the entire table.
PostgreSQL documents binary COPY portability limits and exact type dependence in [COPY](https://www.postgresql.org/docs/16/sql-copy.html#SQL-COPY-NOTES).
The native predicate is intentionally narrower than the fork's recursive predicate.

### Persistent formats: leader-frozen option A

Option A creates native format 9 / target metadata 7 only for fresh M8 work.
All old formats fail before database or local state mutation.
Keep prior exact binaries/images available to finish earlier native runs.
No in-place conversion or automatic restart is permitted.

Option B would retain separate explicit old/new readers and writers without converting old runs.
It requires two complete recovery matrices and helper profiles, so it is deferred.

Current native format is 8 and target metadata is 6 in `src/state.rs:172–174`.
`src/apply/metadata.rs` validates exact marker columns, primary key, grants, and owned identities.
The M6 helper pins format 8, metadata 6, ClonePlan keys, CloneTable keys, and exact marker DDL in `internal/native/evidence_program.py` and its fixtures.
Changing only engine constants would break the independent reader and could invalidate cleanup proofs.
Task 1 therefore freezes the full new schema and helper reach before implementation.

### LO comparison: leader-frozen option A

Option A keeps the per-table JSON array and `compare progress:` shape unchanged.
The engine must compare exact LO OID membership, logical contents, and selected owner/ACL/comment metadata before command success.
A mismatch or missing comparison returns a redacted nonzero error even if all table rows match.
Never manufacture an LO table row or silently add a report DTO.

Option B would define a versioned separate LO result with a corresponding operator parser.
It expands the public contract and is deferred.

Current `src/lib.rs:139–143` would treat an empty comparison vector as matched, but the actual `compare::union_tables` path at lines 203–207 refuses empty inventory first.
`src/compare/tests.rs:16–24` pins that refusal.
Empty success is therefore not reachable in the inspected engine compare path.
The COPY workset test in `src/copy/clone.rs` explicitly accepts a positively established empty catalog.
Current operator `internal/controller/verification.go:57–72` rejects `[]`, and `TestCompareDataQuery` pins that rejection.
Neither the accepted design nor the compatibility contract requires a physical table to exist in a valid database.
The anti-empty-report rule protects against omission, not against a separately proven empty database.
The root selected the native-only independent empty-inventory guard below before this plan was finalized.

Two bounded correction options were considered:

- Selected by the root: keep legacy strict behavior and permit native `[]` only with independent exact source and target physical-table inventories proving zero, stable under the existing verification/fence boundary, plus successful mandatory LO comparison.
  Bind exclusions to the admitted exact native schema and object OIDs.
  Reject empty stdout, non-array output, a nonzero command, a missing inventory, an omitted nonempty table array, or unproven LO comparison.
  Reuse an existing admitted native helper inventory if M6 supplies one; do not create new authority from observed output.
- Introduce a separately versioned positive empty-inventory result and corresponding native-only wrapper parser.
  This requires public-contract approval and is larger than the selected unchanged-array approach.

Do not silently stage-refuse LO-only databases or remove the legacy `[]` safety check.
Do not treat parsed zero counters alone as independent catalog proof.

## Existing interfaces and file ownership

The leader must assign the following ownership sets without overlap.
Owners may implement in parallel only after Task 1 releases shared signatures.

- Contract/config/state owner: `src/contracts.rs`, `src/config.rs`, `src/state.rs`, `src/state/clone_state.rs`, `src/state/clone_state/tests.rs`, `src/state/follow_state.rs`, `src/state/control.rs`.
- Catalog/schema owner: `src/catalog.rs`, `src/catalog/clone.rs`, relevant tests inside them, and `src/schema.rs`.
- Target authority owner: `src/apply/checkpoint.rs`, `src/apply/metadata.rs`, `src/apply/checkpoint_live_tests.rs`, `src/apply/mod.rs`.
  Use xhigh effort for concurrent parts, unknown COMMIT, reset, and LO cleanup authority.
- COPY/LO worker owner: `src/copy/clone.rs`, `src/copy.rs`, `src/copy/live_tests.rs`, new focused `src/large_objects.rs` if the leader approves that module boundary, and its focused tests.
  Keep the existing serial `copy table-data` text-only route unless separately assigned.
- Integration owner after shared handoff: `src/clone.rs`, `src/follow.rs`, `src/snapshot/follow.rs`, `src/compare.rs`, `src/compare/tests.rs`, `src/lib.rs`, `src/progress.rs`, `src/protocol.rs`.
- Engine harness/docs owner: `tools/e2e/{clone,follow,cutover}.py`, their `test_*.py`, `tests/fixtures/{clone,follow}/`, `tests/fixtures/native/v1/`, `tests/test_native_fixtures.py`, `tests/cli_contract.py`, `Taskfile.yml`, `tests/test_taskfile.py`, `tools/ci/verify.py`, current behavior docs, roadmap, and todo.
- Separate operator owner after M6 handoff: native command/config qualification, helper formats and goldens, comparison wrapper, native E2E profile and docs.
  Keep all operator commits and checks separate from engine commits.
- Independent milestone reviewer/verification owner: read all final source and run the closing gates without reviewing their own changes.

Current immutable consumers:

- `copy::run_tables(&CloneOptions, &ClonePlan, &mut CloneSnapshot, &mut NativeState, &mut CloneAuthority, &IoControl) -> Result<CopyReport>`.
- `CloneAuthority::permit(&mut self, &OwnedCopyTable) -> Result<CopyPermit>` delegates one permit per source table.
- `CopyTxn::begin(PgConnection, CopyPermit, &IoControl) -> Result<CopyTxn>` locks ACCESS EXCLUSIVE and truncates one table.
- `CopyTxn::mark_and_commit(self, rows: u64, bytes: u64, &IoControl) -> Result<CopyCommitOutcome>` commits data and marker atomically.
- `CloneAuthority::resolve_unknown(&mut self, &OwnedCopyTable, &IoControl) -> Result<Option<CopyReport>>` reaps workers, reacquires the fence, writes a barrier, and reads the exact marker.
- `catalog::plan_clone(source, target, generation, policy, exact_metadata, owned_objects, io) -> Result<ClonePlan>` requires the caller's existing source transaction because planning locks tables.
- `TargetAuthority::complete_follow` performs final sequence sync and sets `follow_completed` in a synchronous target transaction after exact drain admission.
- `compare::data(&CloneOptions, &IoControl) -> Result<Vec<TableComparison>>` returns the frozen array.
- `PgConnection::{start_copy_out,read_copy_bounded,start_copy_in,write_copy,finish_copy_in}` already relay bytes with deadlines and cancellation.
  `finish_copy_in` returns the parsed COPY row count.
- `PgConnection::{prepare,execute_prepared}` with `PgParam::{Text,Binary,Null}` provides bounded SQL LO operations without new FFI.

## Dependency graph and release gates

Task 1 freezes shared types and schema.
Tasks 2, 3, and 5 can then build independent predicates, part planning, and LO primitives while the authority owner builds Task 4.
Task 6 integrates them after these owners release their files.
Task 7 covers harness, operator, and docs.
Task 8 performs one independent milestone review and qualification.
Task 9 activates capabilities only after corresponding qualification receipts exist.

Safe before M7 closure: private design decisions, source-only implementation in disjoint worktrees, focused unit tests, reviewed pure fixtures, and bounded component tests within a separately admitted resource budget.
Do not collide with active M6 helper/reader/command owners or active M7 Taskfile/harness owners.
No VM or runtime lease is implied by this plan.

Blocked on M7 closure: claims that M8 preserves the qualified operator workflow, the full operator-default rerun, and capability activation for that workflow.
A passing engine component case does not close this dependency.

M7 CLI gap is an explicit prerequisite, not assumed solved.
At inspected heads, `nativeCloneArgs` calls `pgcopydb.CloneArgs`, which emits `--split-tables-larger-than 0 --split-max-parts 8` even when splitting is disabled.
Rust `clone_options` rejects both names.
The root froze the M6 commands planner correction: omit both selectors only when the effective threshold is explicitly zero on the native route, with no legacy or Rust-admission change.
Actual executable parity must cover the complete narrowed worker argv with only `--dir` rebound to an owned temporary fixture.
The standalone M1 `--split-max-parts 8` refusal remains intact until positive M8 split admission is implemented.
M8 positive split admission must preserve the eventual M7 disabled behavior.

## Task 1: Freeze types, formats, and admission before parallel edits

**Owner:** Contract/config/state owner with catalog, authority, M6 helper, and integration owners consulted.
**Interfaces:** Produce one reviewed new-format schema and exact Rust signatures before workers begin.

- [ ] Carry the root-approved empty/LO-only guard into the exact interface freeze and release existing file owners before assignment.
- [ ] Write failing unit/CLI cases for explicit binary false/true, threshold zero/nonzero, max-parts one/eight/overflow, LO skip/default, LO jobs 1/128/0/129, environment precedence, and read-only compare/progress flag handling.
- [ ] Extend `CloneOptions` with exact fields `use_copy_binary: bool`, `split_tables_larger_than: u64`, `split_max_parts: u32`, `skip_large_objects: bool`, and `large_objects_jobs: usize`.
  Preserve standalone defaults unless the compatibility baseline explicitly fixes them.
  Operator defaults remain binary on, 536870912-byte threshold, eight parts, and LO included.
  Do not use a literal `512Mi` in Rust byte parsing: current parser accepts integer bytes or B/kB/MB/GB/TB/PB/EB.
- [ ] Freeze a serialized per-table execution profile and part descriptor shared by planning, permits, markers, and retry.
  Root-approved exact boundary and proposed concrete type names: `ClonePlan.copy_tables: Vec<CloneTableCopy>`, `CloneTableCopy { source_oid: u32, format: CopyFormat, parts: Vec<CopyPart> }`, and `CopyPart { number: u32, count: u32, start_block: u32, end_block: Option<u32> }`.
  Keep `CloneTable` as source catalog/schema identity.
  `snapshot/follow.rs:99,290,446` compares source-only `inspect_clone_tables` with `plan.tables`; adding target-dependent format/parts to `CloneTable` would break these safety checks.
  Separate execution entries preserve this boundary with less caller churn.
  The root approved this separate execution-entry placement.
  Require a complete unique exact source-OID keyset matching all physical plan tables and canonical ordering.
  Same-generation resume must compare the full immutable format/part profile, not just schema identity.
  A fresh-snapshot restart may rebuild bounds only through its explicit new-generation transition after old permits and markers lose authority.
  Never ignore a parts/format mismatch to reuse existing completion.
  Owners must freeze final Rust names and serialized fields before parallel implementation.
  `CopyFormat` gains serde derives for persisted plans.
  One unsplit descriptor is number 1/count 1/start 0/end None.
  A split descriptor carries its exact generation's bounds through its enclosing owned table.
  The leader must freeze final names before dispatch; workers must not invent competing shapes.
- [ ] Freeze LO policy and ownership records separately from user-table counts.
  Persist skip policy and the run's exact created LO OIDs, owner and selected metadata bindings, generation, and initial/final completion requirements.
  Put durable worker completion in target metadata atomically with LO creation/content, not only in the local manifest.
  Do not put all LO payloads or an unbounded catalog in the one-MiB manifest.
  If the existing metadata table inventory needs a new LO table, freeze its exact table/index identities and all helper/cleanup exclusions in this task.
- [ ] Freeze explicit maximum part-workset and LO inventory/cursor bounds within existing one-MiB state limits.
  CLI `split-max-parts` currently allows up to i32::MAX; retain parser acceptance but refuse unrepresentable runtime plans before mutation.
  Do not allocate `tables * supplied_max_parts` before validating the actual bounded plan.
- [ ] Change fresh state to native 9 / target metadata 7 and add no-mutation refusal tests for 8/6, legacy, unknown future, malformed, and cross-run state.
- [ ] Reconcile every literal constructor reached by changed types, keeping unchanged CloneTable literals intact when the separate execution profile is selected: `contracts.rs`, `copy/{clone,live_tests}.rs`, `schema.rs`, `catalog.rs`, `catalog/{clone,follow}.rs`, `cdc/receive.rs`, `snapshot/follow{_tests,_fault_tests}.rs`, `state/{clone_state/tests,follow_state,control}.rs`, `compare/tests.rs`, `apply/checkpoint{,_live_tests}.rs`, and `apply/metadata.rs`.
- [ ] Preserve `require_same_plan` and `state::follow_state::same_plan` semantics explicitly: stable source schema/policy must match, but new snapshot ctid boundaries and dynamic LO membership must be rebuilt, not compared as immutable schema.
  Preserve the three raw catalog-inventory equality checks in `src/snapshot/follow.rs`.
  Define a separate checked execution-profile validation against each current catalog/table identity.
  Same-generation saved execution entries require exact equality.
  Restart must validate and durably bind a complete new execution profile only after its explicit fresh-snapshot generation transition.
  Keep copy-format or split-option changes on an existing generation as refusal, rather than silently applying new settings.
  Audit every `inspect_clone_tables` caller before splitting LO admission from table inspection: `clone.rs`, `follow.rs`, `schema.rs`, `snapshot/follow.rs`, `compare.rs`, `catalog.rs`, `catalog/{clone,follow}.rs`, `copy/live_tests.rs`, `snapshot/follow{_tests,_fault_tests}.rs`, and `apply/checkpoint.rs`.
  If the catalog signature changes, its owner must update those callers in the same handoff.
- [ ] Run focused contract/state/CLI tests and format/Clippy checks for this deliverable.
  Record actual commands and results in the handoff.
  Commit only after leader request and the required checks.

## Task 2: Prove the conservative binary predicate and relay

**Owner:** Catalog/schema owner for predicate, then COPY owner for relay after a signature handoff.
**Files:** `src/catalog/clone.rs`, `src/copy/clone.rs`, `src/copy/live_tests.rs`, `tests/fixtures/clone/`, relevant catalog harness cases.
**Proposed interfaces:** `catalog::binary_copy_compatible(source: &mut PgConnection, target: &mut PgConnection, table: &CloneTable, io: &IoControl) -> Result<bool>` and recorded `CloneTableCopy.format`.
Target type identity must be rechecked after restore before binary bytes enter COPY.

- [ ] Add failing predicate tests for every candidate scalar, missing send/receive, spoofed namespace/name/OID, typmod mismatch, server/client encoding mismatch, all unsupported containers/user types, and tsvector.
- [ ] Verify primary PostgreSQL implementation paths for candidate send/receive functions on 16 and 18 and retain the exact evidence list in the task receipt.
  Reduce the allowlist when evidence is absent.
- [ ] Add live pair cases proving binary was actually selected for safe tables and text selected for a mixed safe/unsafe table.
  Use independent server COPY metrics or bounded command observation; data equality alone cannot prove branch execution.
- [ ] Change `transfer_clone_table` to select one format for the whole table and relay opaque COPY buffers.
  Preserve source locking, exact OID check, explicit writable columns, 16-MiB receive bound, 64-KiB forwarding, cancellation, and generated-row checks.
- [ ] Use `finish_copy_in` command counts for binary rows.
  Preserve the existing text source-row check where applicable.
  Test binary empty/header-only streams and ensure header/trailer bytes do not inflate rows.
  Keep byte counters defined as relayed COPY bytes, including binary framing.
- [ ] Test late source error, target error, dropped connection, unknown COMMIT, and generated columns under both formats.
- [ ] Update only the native clone/follow profile; the serial text-only `copy table-data` contract remains unchanged.
- [ ] Run focused predicate and COPY cases on 16:16, 18:18, and 16:18 before reporting binary qualification.

## Task 3: Build snapshot-bound ctid parts

**Owner:** Catalog/schema owner after Task 2 predicate handoff.
**Files:** `src/catalog/clone.rs`, shared types via their owner, focused plan tests/fixtures.
**Proposed interface:** `plan_copy_parts(relation_bytes: u64, block_size: u32, threshold_bytes: u64, max_parts: u32) -> Result<Vec<CopyPart>>`.
The catalog wrapper supplies freshly measured physical storage size under the admitted snapshot and table lock.

- [ ] Add failing tests for disabled threshold, zero-size table, below/equal/above threshold, max-parts one/eight, ceil division, overflow, stale relpages, and the bounded workset limit.
- [ ] Build contiguous nonoverlapping page ranges beginning at block zero, with an unbounded final range.
  Use integer checked arithmetic.
  Do not rely solely on stale analyzed relpages to bound the last part.
- [ ] Generate source predicates `ctid >= '(start,0)'::pg_catalog.tid` and, except the last part, `ctid < '(end,0)'::pg_catalog.tid` against `FROM ONLY` physical heaps.
  Do not split partition roots or count a partitioned parent as copied data.
- [ ] Pin the same exporter ticket for every part worker.
  Keep source ACCESS SHARE locks and existing transaction boundaries.
  Document the ctid stability assumptions; table rewrites cannot proceed through the held source locks.
- [ ] Compare with fork `schema.c:1573–1733` and `table-data.c:1710–1727`: finite ranges plus unbounded tail are the behavioral reference.
  Native splitting is ctid-only for this milestone; no integer-key scheduler is added.
- [ ] Test a table with stale small page estimates, rows beyond the last finite bound, empty ranges, TOAST, and partition leaves.
- [ ] On new snapshot/generation, rebuild ranges while preserving stable owned table identity.
  Add tests proving old markers and old permits cannot satisfy the new plan.
- [ ] Run focused part-planning tests and an actual multiworker COPY component case.

## Task 4: Make part completion and reset durable

**Owner:** Target authority owner, xhigh effort.
**Files:** `src/apply/checkpoint.rs`, `src/apply/metadata.rs`, `src/apply/checkpoint_live_tests.rs`, `src/apply/mod.rs`.
**Interfaces:** Extend `CopyDone`, `CopyPermit`, permit issuance, marker readers/writers/primary keys, and unknown-COMMIT lookup with the Task 1 exact part identity.
Proposed new method: `CloneAuthority::prepare_split_tables(&mut self, native: &NativeState, io: &IoControl) -> Result<()>`.
Preserve the public `run_tables` signature.

- [ ] Add failing marker tests for missing, duplicate, wrong-part, wrong-bound, wrong-count, wrong-table/OID/owner/namespace, stale-generation, and over-bound rows.
- [ ] Store data and the exact part marker in the same synchronous target transaction.
  Key marker identity by run/nonce, generation, physical source/target table, and part number, with the recorded part bounds validated against the plan.
- [ ] For split tables, perform one positively committed coordinator reset before issuing any part permit.
  Lock and revalidate exact owned tables, truncate the required workset once with RESTRICT, and bind preparation to the current generation.
  Reuse the existing complete-workset follow reset where suitable; do not add competing truncation paths.
- [ ] Split worker transactions must avoid TRUNCATE, ACCESS EXCLUSIVE serialization, and FREEZE.
  Acquire the compatible lock needed to keep exact target identity stable while parts write concurrently.
  Unsplit transactions retain the existing truncate plus FREEZE behavior.
- [ ] Deduplicate permit issuance by exact part identity, not just source OID.
  Do not allow a prepared old-generation permit to proceed after reset or coordinator fence loss.
- [ ] Resolve unknown part COMMIT only after all workers are reaped and the coordinator reacquires the same run fence and obtains a synchronous barrier.
  Read only the matching part marker.
  Missing marker means failure requiring new-generation data restart, not a blind append retry.
- [ ] Aggregate rows/bytes across parts, but count a physical table complete only when its exact complete part set exists.
  Partial tables keep `tables.done` below `tables.total` and cannot authorize post-data, anchor installation, or completion.
  Preserve checked counter overflow handling and truly empty established catalogs.
- [ ] Test crashes before/after truncate commit, before permit issuance, mid-part, before marker, unknown marker COMMIT, and after last part before coordinator aggregation.
- [ ] Test full-generation restart after partial durable parts and foreign target replacement without deleting unrelated rows.
- [ ] Run focused authority/live cases on both majors, then hand off the exact new metadata schema and catalog SQL to the M6 helper owner.

## Task 5: Copy, verify, resync, and restart only owned large objects

**Owner:** COPY/LO worker owner for primitives; authority owner for durable inventory and cleanup after signature handoff.
**Files:** proposed `src/large_objects.rs`, `src/lib.rs` declaration through integration owner, `src/catalog/clone.rs`, `src/schema.rs`, state/authority files through their owners, focused LO fixtures/tests.
**Proposed interfaces:** `inspect(connection: &mut PgConnection, io: &IoControl) -> Result<LargeObjectInventory>`, `copy_object(source: &mut PgConnection, target: &mut PgConnection, object: &LargeObject, policy: RestorePolicy, io: &IoControl) -> Result<LargeObjectMeasurement>`, and `compare(source: &mut PgConnection, target: &mut PgConnection, policy: RestorePolicy, io: &IoControl) -> Result<()>`.
The inventory must be paged and bounded; exact final names/types join Task 1's freeze.
Authority owns the permitted target transaction and atomically records creation/completion, not the primitive.

- [ ] Add failing cases for no LOs, empty LO, binary data, multichunk data, sparse holes, large offsets, truncation, OID preservation, multiple workers, inaccessible LO, and missing owner role.
- [ ] Split `source_obligations` so extension-config refusal remains unchanged while LO admission follows the selected policy.
  Skip means skip copy and final LO obligations, with explicit recorded semantics.
  Do not accidentally allow extension configuration rows while removing the LO refusal.
- [ ] Capture LO metadata under the source snapshot and import that same snapshot in every initial LO worker.
  Honor `--large-objects-jobs` with the existing scoped-thread/connection ownership style and bounded queue.
  Each worker creates its own connections.
- [ ] Use bounded SQL operations through current nonblocking query/prepared interfaces.
  Use `lo_create` with the exact nonzero source OID and create-on-absence checks.
  Preserve arbitrary bytes through binary parameters or checked hex decoding, never row interpolation in SQL or logs.
  Use bounded read/write operations and 64-bit offsets.
  Avoid whole-object `lo_get(oid)` and blocking client LO APIs.
- [ ] Ensure full logical length and zero-filled sparse regions survive copy.
  Re-copy must truncate/recreate only owned LOs so shortening the source cannot leave a target suffix.
  Physical page layout alone is not a logical equality oracle.
- [ ] Define pre-data LO ownership precisely on both client-tool versions.
  Current schema archive uses `--schema-only`; inspect actual 16 and 18 TOCs rather than assuming BLOB placement.
  Prevent utility-created LOs from becoming unrecorded or silently adopted.
  The selected minimum is one engine-owned creation path with archive LO entries excluded when necessary, exact pre-data handoff preserved, and owner/ACL/comment restoration handled explicitly.
- [ ] Apply selected owner/ACL/comment policy and verify it.
  `no_owner`, `no_acl`, and `no_comments` retain their recorded meaning.
  Native connection privileges do not authorize grants to additional roles.
- [ ] Persist exact created LO identity and completion in the same target transaction as content.
  Unknown COMMIT uses the existing fence/barrier proof pattern before reconciliation.
  Native manifest state alone must not claim creation when the target commit is unknown.
- [ ] During pre-anchor data restart, unlink only the old generation's durably recorded owned LOs, invalidate LO/part markers, and rebuild a fresh snapshot generation before workers start.
  Preserve unrelated or replaced objects and refuse uncertain ownership.
  Make interrupted cleanup/retry idempotent without broad deletion from pg_largeobject_metadata.
- [ ] After drain, enumerate the final source LO set under the held writer freeze.
  Create new source OIDs on target, refresh retained OIDs, remove deleted-source OIDs only if the run owns them, and refuse foreign extras/collisions.
  Record final LO completion before setting `follow_completed`.
  Repeated drain/finalization after crash must either positively reconcile or fail without reporting success.
- [ ] Verify exact OID set, logical content, logical lengths, and selected metadata before compare success.
  Include empty LO versus absent LO, sparse versus explicit zero content, wrong OID with identical bytes, content changes, owner/ACL/comment changes, and foreign target extras.
- [ ] Preserve the existing exclusive target administration assumption through all LO mutation and cleanup.
  The run advisory fence serializes cooperating native actors; it does not prevent a different privileged session from replacing an LO with the same OID.
  Never claim detection of an indistinguishable external replacement when the exclusive administration assumption is broken.
  Refuse all observable metadata/content/provenance drift and test those cases.
- [ ] Test failures after LO create, midchunk, after object commit, during unlink/reset, before final sync, during final sync, and after final sync before completion commit.
- [ ] Use pinned fork `blobs.c:335–498` and `pgsql.c:3537–3650` for OID preservation, snapshot worker behavior, and absent-LO checks only.
  Do not import the fork's permissive drop-if-exists policy into native ownership rules.
  SQL LO operations and transaction constraints are documented in [LO functions](https://www.postgresql.org/docs/18/lo-funcs.html) and [LO interfaces](https://www.postgresql.org/docs/18/lo-interfaces.html).
- [ ] Run focused LO component cases on all three pairs, without treating them as operator workflow qualification.

## Task 6: Integrate phases, recovery, progress, and mandatory LO comparison

**Owner:** Integration owner after Tasks 2–5 handoff.
**Files:** `src/clone.rs`, `src/follow.rs`, `src/compare.rs`, `src/compare/tests.rs`, `src/lib.rs`, `src/progress.rs`, `src/protocol.rs`.

- [ ] Write failing integration cases showing neither post-data nor the follow-copy anchor can pass with an incomplete part set or LO copy obligation.
- [ ] Add initial LO work before post-data/anchor release while the same exporter remains valid.
  Preserve source health checks and do not move planning outside its transaction.
- [ ] Add final LO re-sync to the existing exact-drain finalization path, before `follow_completed=true` and its synchronous commit.
  Keep sequence and final LO obligations under the same held target fence and operator writer freeze.
  Do not raise feedback from an LO observation or later-BEGIN witness.
- [ ] Preserve completed `--resume --not-consistent` behavior: validate durable completion and retain generation/markers rather than recopy.
- [ ] Compare LO set/content/selected metadata before `compare::data` returns success.
  Add the root-approved narrowly proven-empty route to `union_tables` or its admitted caller.
  Current unconditional empty-inventory refusal must remain for missing/unproven inventories.
  Extend `inventories_measure_missing_tables_and_refuse_absent_coverage` with separate proven-empty and unproven-empty cases.
  Actual successful LO comparison is mandatory for both nonempty and empty native table inventories when LOs are selected.
  Keep the table array and table-progress counters unchanged.
  LO mismatch must fail the command even with identical tables.
  Absent execution of the LO comparison must not count as a match.
- [ ] Update progress table aggregation for all-parts completion and leave rows/bytes measured.
  Do not count parts or LOs as user tables.
- [ ] Keep discovery M8 capability names disabled during build and unqualified integration.
  Report transport may state only the command capability needed by the existing contract.
- [ ] Run focused integrated clone, follow restart, cutover, compare, and completion tests.

## Task 7: Extend harnesses, operator helpers, and behavior docs

**Owner:** Harness/docs owner and separate operator owner with disjoint files.

- [ ] Replace current binary/split/LO staged refusal cases with positive M8 cases only when those behavior paths exist.
  Keep unsupported combinations as explicit refusal tests.
  Update exact fixture inventories, manifest digests, portable suite counts, and every Taskfile filter so added tests actually execute.
- [ ] Extend independent SQL fingerprints for safe-binary/fallback tables, all split ranges, LO OID sets, logical content/length, and selected owner/ACL/comment metadata.
  The oracle must not call the engine's compare implementation.
- [ ] Add harness negatives for empty case lists, omitted part, omitted LO check, wrong versions/tools, skipped fault injection, matching-table/LO-mismatch, and receipt replay from a different binary/image.
- [ ] Add explicit branch evidence for binary chosen/fallback chosen and actual simultaneous part workers.
  Successful output alone cannot prove these paths ran.
- [ ] Update native fixture contract stages and paired operator goldens under the exact unchanged protocol version.
  Recompute manifests only after reviewing content changes.
- [ ] Refresh operator `internal/native/evidence_program.py`, `evidence_program_test.py`, `evidence_test.go`, `testdata/evidence/{NativeSQLFixture.pm,EvidencePrototype.pm,fs_probe.pl,admission_probe.pl,native_marker_ddl.sql,native_marker_catalog_16.sql,native_marker_catalog_18.sql}`, and any M6 successor files identified at handoff.
  Refresh exact new plan keys, formats, marker table/index catalogs, LO bookkeeping, ownership exclusions, and cleanup validation.
  Re-read the actual handed-off source rather than copy this baseline list blindly.
- [ ] Preserve `nativeConfigCapabilities` requirements for binary default-on, nonzero split threshold, and LO included.
  Qualify actual `nativeCloneArgs` default argv and the M7 disabled profile.
- [ ] Implement the root-approved native-only empty/LO-only wrapper rule with independent expected inventory and negative omission tests.
  Assign the operator verification owner `internal/controller/verification.go`, `compare_data_test.go`, `verification_test.go`, and `verification_progress_test.go`.
  Include callers in `resources.go`, `resources_test.go`, `native_resources_test.go`, and any M6 successor native verification adapter in the owner handoff.
  Include command-capability goldens in `native_commands_test.go` and `backend_test.go` through their owner.
  Reuse the credential-safe admitted SQL path and source/target connections; do not put URIs or row content in argv or diagnostics.
  Capture independent physical-table inventories with exact owned metadata exclusions under the stable/fenced verification boundary.
  Permit the native empty array only when both inventories prove zero and the actual mandatory LO comparison succeeds.
  Missing, malformed, failed, stale, or skipped inventory is failure, never zero.
  Add negatives for dropped output when either endpoint has any table, matching table output with an LO mismatch, missing/failed LO execution, a table created outside the stable boundary, changed metadata identity, empty stdout, and legacy `[]`.
  Keep legacy strict compare behavior unchanged.
  The existing `TestCompareDataQuery` live SQL test must retain its legacy empty-array refusal and gain a native guarded variant rather than replace that assertion.
  Add positive empty-database and LO-only cases to actual native operator E2E, not just mocked wrapper tests.
- [ ] Extend the M7 native E2E profile to operator defaults without enabling DropIfExists, filters, or other staged options.
  Keep a default-threshold fixture large enough to prove actual splitting at 536870912 bytes, with bounded reviewed data volume.
  A tiny threshold test is useful focused coverage but does not prove the operator-default split branch.
- [ ] Update `docs/operator-compatibility.md`, `docs/testing.md`, `docs/native-protocol.md`, `docs/native-cutover-handoff.md`, and operator operations docs that name the narrowed profile or old format.
  Keep immutable prior receipts scoped to their original versions.
  Update roadmap and todo together when verified subdeliverables close.
- [ ] Run focused portable/harness checks during build.
  Do not run the full Go suites per task.

## Task 8: Close M8 with independent review and exact runtime matrix

**Owner:** Separate verification/review worker, xhigh for authority/recovery safety.
**Prerequisites:** M7 closure, all implementation handoffs, new M6 helper profile qualification, exact tested image/binary identities, and a fresh admitted resource budget.

1. Run engine host gates and retain output.

   ```sh
   task lint test
   ```

2. Run the retained live suite on both admitted database majors and the M8 focused profiles wired into it.

   ```sh
   task test-db
   ```

3. Run engine pair suites after the harness updates and retain all three exact pair receipts.

   ```sh
   task test-e2e-clone
   task test-e2e-follow
   ```

4. Run the operator lint and Go suites once at milestone close under its current instruction/CI policy.
   Use the actual M7 handed-off Task targets for native kind runs; the inspected central operator Taskfile does not yet contain those targets.
   Do not invent a command or set E2E_CONTEXT by hand.

5. Repeat the M7 native matrix with operator defaults on 16:16, 18:18, and 16:18.
   Each pair must prove live-writer follow, manual/automatic cutover, verification, kill mid-COPY, mid-stream, mid-drain, target-primary kill with CNPG synchronous replication, suspend/resume, and deletion cleanup.
   Add M8 fault cases around binary relay, partial split completion, LO copy, LO reset, and final LO re-sync.
   Require Complete/CutoverCompleted, exact durable native drain proof, independent table/sequence/LO/schema/ownership/partition checks, and correct exact-owned cleanup.

6. Run hosted checks on the exact authorized feature refs and bind receipts to the same source/image digests.
   Runtime matrix success cannot be inferred from compilation, unit tests, or parser acceptance.

7. Review the complete final engine and separate operator diffs once.
   Audit crash boundaries, reset ownership, unknown COMMIT, counts, empty inventory, format refusal, and helper exact exclusions.
   Re-delegate findings to their file owners.

Expected passing evidence includes nonempty exact inventories, no unexpected skips, observed injections, source/target major checks, matching client bundles, actual executable and libpq identity, image digests, and complete process/helper cleanup.
Retain failures separately from successful reruns.
No benchmark or durability-mode result belongs to this gate.

M9 handoff reserves native format 9 / metadata 7 for M8 only.
M8 does not add replay-safe/relaxed settings to `FollowOptions`, `FollowLimits`, or the native protocol.
The M9 planning owner received this boundary and must choose a later native format only if its own persisted settings require one.
M9 must consume the handed-off v7 target schema rather than assume metadata version 6.
Shared E2E/protocol/state files need sequential ownership handoff between milestones.

## Task 9: Activate only qualified capabilities

**Owner:** Protocol/qualification owner after leader approval of Task 8 receipts.

- [ ] Enable `copy.binary_fallback` only after safe-binary and unsafe-table fallback qualify on all admitted pairs and architectures claimed by the exact artifact.
- [ ] Enable `copy.split` only after durable per-part completion, reset/restart, counts, concurrent execution, and operator-default threshold cases qualify.
- [ ] Enable `copy.large_objects` only after initial copy, final re-sync, selected metadata, compare, cleanup/restart, and empty/LO-only behavior qualify.
- [ ] Preserve qualification of base clone/follow/progress/cutover/verify/cleanup capabilities from their owning milestones.
  Do not equate adding a name to `Version::current()` with qualification.
- [ ] Update discovery golden fixtures, both operator handshake tests, and current support docs in the same change.
- [ ] Run final `task lint test` after activation and the required hosted/operator closing checks on the activation source.
  If source/image changed after runtime evidence, re-run affected exact-image gates instead of relabeling prior evidence.
- [ ] Update roadmap/todo with per-feature receipts and close M8 only when the operator-default rerun passes.
  Keep M9 benchmark HOLD and M10 exact-image release review open until their own gates close.

## Acceptance criteria

- Binary COPY selects only independently proven built-in scalar profiles and uses text for an unsafe table without dropping or recasting data.
- Safe and fallback branches both execute on every admitted version pair.
- Split ranges cover every physical row once under one snapshot, part commits are atomic with markers, and partial parts cannot satisfy table completion.
- Split workers cannot truncate one another or use invalid FREEZE semantics.
- New-generation restart resets all recorded owned data and owned LOs, invalidates earlier authority, and preserves foreign objects.
- LO OIDs, logical contents/lengths, and selected metadata match initially and after drain.
- LO deletion/creation/shortening during follow is reflected after the writer freeze, and failed final sync cannot complete the migration.
- Compare retains its array shape and refuses any LO mismatch or unproven empty inventory.
- Old native and foreign/legacy formats fail before mutation, with no automatic conversion.
- Engine and M6 independent helper agree on format 9 / metadata 7 and exact bookkeeping identities.
- The M7 matrix passes with real operator defaults on 16:16, 18:18, and 16:18.
- Capability activation follows receipts, and all required lint/test/independent review gates pass.

## Planning correction and handoff

A preliminary message inferred current empty-array success from `lib.rs` alone.
The planner corrected that statement after reading `union_tables` and its test: current compare fails before it can return the empty array.
Lesson for integration: trace the full producer path before reasoning from a terminal iterator or formatter.
Preserve unproven-empty refusal and require actual successful catalog queries and LO verification for the new proven-empty route.
This correction is recorded here because this planning assignment does not authorize repository lessons edits.

## Remaining decisions and handoff

The root approved binary option A, explicit new formats option A, unchanged-array mandatory LO comparison option A, and the native-only independent empty-inventory guard during this planning assignment.
The root also approved the native-only empty/LO-only guard: actual successful mandatory LO comparison plus independent zero-physical-table inventories under the stable/fenced boundary, with exact bookkeeping exclusions.
The root approved separate `ClonePlan.copy_tables` execution entries keyed by source OID.
The contract owner must freeze the actual bounded part/LO storage schema and final shared Rust field/signature names before dependent code dispatch.
This is Task 1, with explicit leader review before workers consume the interface.
The root froze the M7 disabled-split argv correction in the commands planner; require its actual-executable parity handoff before assuming it is implemented.
LO metadata restore and schema utility behavior need live 16/18 TOC evidence before the owner selects the minimal exact archive exclusion.
The LO transfer API choices are implementation proposals within the existing SQL boundary, not permission to introduce another backend or public protocol.

Skipped: source edits, dependency changes, runtime/VM work, benchmarks, tests, commits, pushes, and capabilities activation.
Risk: part concurrency, LO ownership/reset, and independent helper versioning share durability authority and need their assigned xhigh implementation/review gates.
