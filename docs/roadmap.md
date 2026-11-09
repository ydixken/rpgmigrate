# rpgmigrate: assessment and milestone plan

## Context

pgcopydb-operator must run migrations on a native Rust engine, `rpgmigrate`, instead of the forked pgcopydb.
Almost every data-loss defect that reached the operator came from pgcopydb's CDC spool pipeline (op#356, #357, #358, #359, #376, #378).
The engine therefore makes crash/restart correctness and fast WAL apply its core properties, with documented durability modes.
Goal: working operator-driven migrations (copy, replication, recovery, cutover, verification) on PG16→16, PG18→18 and PG16→18, with reproducible E2E tests, Taskfile commands, Docker packaging, GitHub Actions and documentation.

Inputs: the code of all three repos, operator and fork issues, pgcopydb `origin/v0.18-fixes`, the trusted `AGENTS.md`, the [approved design](superpowers/specs/2026-11-06-rpgmigrate-design.md), `tasks/lessons.md`, `docs/operator-compatibility.md`, and the uncommitted work (51 untracked files plus `stash@{0}`).
Not used: `tasks/todo.md` and the dated plans.
A 9-reader audit with adversarial cross-checks and a 5-lens plan review (correctness, scope, operator/E2E, performance, builder) produced the findings below.

## Assessment at the planning baseline

- The binary runs real work for `ping` and a restricted serial `copy table-data` only (`src/lib.rs:56-103`); all eight operator routes return "unimplemented".
- Uncommitted work lives in two places: 51 untracked files on `main` and `stash@{0}` ("WIP on feat/native-copy: bd0925d", 42 files, +11.6k/-2.1k, with the 4.3k-line catalog, COPY anchor, slot creation and state provenance).
  The untracked files compile only with the stash; all Rust files are identical on `main` and `bd0925d`.
- Sound and worth keeping: the libpq FFI (single owners, nonblocking connect, CopyBoth pump with fault tests) and the target checkpoint model (CAS control row in the data transaction, writer fence, unknown-COMMIT recovery by logged barrier).
- Roughly 15-25% of an engine exists: about 4.2k reachable lines, 7.8k unwired engine lines, and about 25k lines of receipts, parity fixtures, storage-pilot benches, image verification scripts and count gates.
- Blocking gaps: no pgoutput decoder, no slot/publication lifecycle or standby feedback, apply at one round trip per row with per-transaction re-verification (57-153 TX/s measured) and a non-strict restart dedupe (`end == applied.end` passes), COPY in one database-wide transaction with a flush per row and no schema step, PG16/PG18 refused by the snapshot gate, a global 300 s deadline, and no cutover, drain proof, cleanup, compare or progress.
- Operator: `spec.engine=rpgmigrate` fails with `UnsupportedEngine`; `internal/native/contract.go` (protocol v1) exists only on branch `feat/rpgmigrate-native-operator` (e8d9b4f) and has no caller; argv0, log markers, `pgcopydb%` application_name patterns, the `pgcopydb version` progress gate and the pg_restore≥17 preflight are hardcoded; PG16 targets are refused by preflight and E2E `checkMajors`; E2E has no kind bootstrap and runs only on a private cluster.

## Decisions

Amendments to the approved design are marked; user-approved ones carry the date and go into the design as a dated revision in M0.

1. **Slot as the durable input log (AMENDMENT, user-approved 2026-11-08: source retains WAL, no copy-phase spool).**
   pgoutput protocol v1 (no streaming) delivers each transaction whole, at commit, in commit order; the walsender spills large transactions on the source.
   Rows stream straight into the open target transaction, so memory stays bounded without a local journal.
   Feedback never exceeds the durable target frontier, so the slot retains everything not yet durable on the target.
   This removes the spool defect class (fork invariants I1-I3, I8, I12, I13, I18) instead of re-fixing it.
   Cost: the source keeps WAL from slot creation through the copy plus apply lag; preflight checks `max_slot_wal_keep_size`, headroom, PG16 `old_snapshot_threshold` and PG18 `idle_replication_slot_timeout`, and the engine polls `wal_status` and refuses on `lost`.
2. **Target checkpoint is the authority, with a certified no-work frontier.**
   One logged control row per stream in an owned schema, CAS-updated in each applied group's transaction; it records `applied.end`, `certified_lsn`, the copy anchor, the source system identifier and timeline.
   `certified_lsn` advances only from a keepalive received after every durable transaction, through a rate-limited logged synchronous barrier written before feedback passes `applied.end`.
   Feedback is a type constructible only from an applied commit end or a certified keepalive, monotonic, and never below the `confirmed_flush` read at connect.
   On every connect the engine refuses (exit 3, gap) when `confirmed_flush_lsn > max(applied.end, certified_lsn, anchor.consistent_point)`, then starts at that max; `end_lsn` must strictly increase per stream.
   The fence is a per-run advisory key; unknown COMMIT is resolved by fence reacquire plus a barrier; a sync-rep "canceling wait" WARNING counts as an unknown outcome.
   No replication origin is created; `--origin` is accepted and recorded, and cleanup reports it as not owned.
3. **Fast apply.**
   Receive and apply run on their own threads, each creating its own connection (handles stay `!Send`), linked by a byte-bounded channel.
   When the channel is full, receive stops reading but keeps a timer-driven status every `min(10 s, wal_sender_timeout/2)`, and sets `wal_sender_timeout=60s` on its connection.
   Apply uses prepared statements cached per relation generation, relation checks once per generation, and libpq pipeline mode per group: BEGIN, pipelined DML and the CAS UPDATE, flush request, verify every result, then COMMIT, giving 2 round trips and 1 fsync per group.
   Groups close naturally when input is empty or a cap is hit (`PGCOPYDB_APPLY_GROUP_TXNS`, default 100, plus row and byte caps, and large-transaction rules).
   Apply sessions use `session_replication_role=replica`, `row_security=off`, server TCP keepalives, and `transaction_timeout=0` only on servers ≥17; pinned output GUCs on the replication connection; plugin options are exactly `proto_version '1'` and `publication_names`.
4. **Durability modes.**
   `durable` (default): synchronous COMMIT per group, feedback after it.
   `replay-safe`: asynchronous commits plus a logged synchronous barrier at a bounded interval; feedback only from barrier proofs.
   `relaxed` (AMENDMENT, user-approved 2026-11-08): built only if paired benchmarks show a real gain over `replay-safe`; if built, it keeps the explicit finite loss caps and D/H/epoch reservations.
   Target failover: acknowledged changes survive a target failover only with a synchronous standby (CNPG synchronous replication with required durability); otherwise a failover leads to a gap refusal and a fresh migration, and the docs say so.
5. **Initial copy order.**
   Create the owned publication (`FOR TABLE` plan list, `publish_via_partition_root=false`), then `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` on a replication connection kept idle with session timeouts disabled, then `pg_dump --snapshot=<token>` (pre-data and post-data, one archive), then `pg_restore` pre-data with `--no-publications --no-subscriptions`.
   Then assert that `pg_publication_tables` equals the copy plan, and start `--table-jobs` workers that import the snapshot and copy unsplit tables as `BEGIN; TRUNCATE; COPY ... FREEZE; done marker; COMMIT`.
   Release the exporter after every worker has imported, then run post-data (`pg_restore --section=post-data`), set sequences and run ANALYZE; apply starts only after post-data.
   COPY relay batches into 64 KiB buffers and flushes only on backpressure and at the end; `row_security=off` on source sessions turns hidden rows into errors.
   Client tools match the target major; a source major above the target major is refused.
6. **Cutover and drain proof.**
   The operator reads `pg_current_wal_insert_lsn()` (which covers source commits made with `synchronous_commit=off`), persists it, and execs `rpgmigrate stream sentinel set endpos <lsn> --json`, which is idempotent and forward-only; `--current` uses the same function.
   Control lives in a `control/` subdirectory with its own lock, written by temp file, fsync, rename and directory fsync, and re-read by the worker.
   Drained means every received transaction with `end_lsn <= endpos` is in the durable frontier, and a keepalive `>= endpos` (requested via reply-requested status) or a later BEGIN arrived after them; the barrier is written before feedback, and the report's `drain_proof.replay_lsn` is that value.
   Final sequence sync and the large-object re-sync happen after the drain, before exit 0.
7. **Operator integration through the native adapter** (design: native handshake, no pgcopydb alias).
   The operator keeps its lifecycle, Job builders, exec and connection helpers, and adds an engine-aware command layer, `rpgmigrate version --json` capabilities parsed in preflight, `native-report: {json}` stdout lines for phase, drain proof, progress and cleanup, and work identity passed as env derived from the Migration UID and PVC.
   Compare keeps the existing per-table JSON array and `compare progress:` shapes; error classes stay in `{error_severity, message}` JSON logs; application_name patterns are ORed for both engines.
8. **Crash during the follow copy (AMENDMENT, user-approved 2026-11-08):** resume before the copy anchor restarts the data phase.
   It drops this run's own slot and publication, creates new ones with a new snapshot, truncates every plan table in one statement, unlinks copied large objects, and invalidates done markers through a new copy generation.
   It applies only to tables whose creation this run recorded; resume after the anchor continues CDC from the checkpoint.
9. **libpq (AMENDMENT, user-approved 2026-11-08, supersedes the lessons entry requiring the patched artifact):** stock PGDG libpq and postgresql-client 16 and 18.
   Delete `native/libpq/**`, `src/pg/native.rs`, the connect-time capability check, `tools/build-libpq.py` and the custom image build; keep the passfile/service live tests minus the stderr assertion, and move their shared helpers.

## Calls I made (check these)

- **Versions (AMENDMENT of design line 303 for this release):** the qualified native profile accepts source and target majors 16 and 18 with source ≤ target and refuses others before mutation.
- **Triggers and FKs:** apply uses `replica`; preflight requires the privilege to set it (superuser or `GRANT SET ON PARAMETER`) and refuses ENABLE ALWAYS/REPLICA user triggers.
- **Missing objects:** after creation, a missing slot or publication or a changed publication OID is a gap refusal, never a silent re-create; any NoticeResponse on the replication connection is fatal; FAILOVER slots stay off.
- **Large objects in follow:** logical decoding does not carry them, so they are re-synced after the drain, under the write freeze, and covered by compare.
- **CLI:** keep the parser baseline and fixtures (design requirement); add a parse-only test oracle and explicit refusals for options outside the native profile; `list tables` stays an explicit staged refusal.
- **Tests:** `tools/test-db.py` stays for cargo live tests (lessons); containers are added only as the E2E layer.
- **Superseded:** `docs/superpowers/specs/2026-11-07-copy-snapshot-anchor.md` (single-transaction COPY, sealed plan nonce); only the token and consistent point carry into per-table markers.
- **Code we drop, in stages:** M0 removes self-contained experiments (`bench/storage*`, `tests/storage_{format,recovery}.rs`, their Cargo entries and Taskfile count gates, `src/snapshot_journal_live_tests.rs`, `test/build.log`); M2 removes `copy_plan`, `state/plan_files` and the E1 catalog whitelist with the COPY rewrite; M3 replaces `cdc/{codec,event_codec,decode}.rs`, `plugins/test_decoding*` and the replay half of `apply/serial.rs`.
  Kept: `src/pg/*` (refactored), `identity.rs`, `contracts.rs`, state primitives, `cdc/contracts.rs`, `apply/plan.rs`, and the checkpoint CAS, barrier, unknown-COMMIT and provenance logic.
- **Preservation:** the stash and untracked files are one archive commit on `wip/native-copy-snapshot`, labelled unverified and never merged; `stash@{0}` and the `archive/stash-0` ref stay until `feat/native-engine` builds.

## Milestones

Each milestone ends with a runnable demonstration and clean `task lint test`; tasks live as checkboxes in `tasks/todo.md` after approval.
After M1, the engine track (M2-M4), packaging (M5) and the operator track (M6, kind bootstrap of M7) run in parallel.
The first operator E2E (M7) runs the vertical slice with binary COPY, split and large objects switched off in the Migration; M8 turns them on.

> [!IMPORTANT]
> M3 engine follow closed at source commit `7b6037f9ac8214696936a21c865cd97307cc5c6e` after passing primary checks and an independent safety review with no actionable findings.
> Formatting, strict Clippy, 433 Rust tests, 24 CLI tests, 254 portable tests, and eighty COPY tests passed.
> Native qualification passed 116 tests per major on PostgreSQL 16.15 and 18.6, totaling 232 PASS with zero failed or ignored.
> Follow qualification passed 63 cases across all three pairs, with 42 actual clone-bound Go report checks.
> Native results combine 142 fresh tests and ninety retained PostgreSQL 16 tests with exact provenance.
> Primary `task check` passed; legacy-stage retirement is integrated and the former scaffold strict-lint blocker is resolved.
> M0–M3 are closed: source is committed at `7b6037f`, and maintained milestone evidence and plans are committed at `23f6d90`.
> M4–M10 remain open; no runnable native operator or cutover workflow is qualified.
> Canonical operator tests remain bound to `5e8cbcb`; full lint passed its equivalent twelve-path successor, committed separately as `2bdc7614c04bc6aa8a8f4c709a84cbcc07340b5c`.
> Cutover, native operator activation, power-loss durability, feature widening, and performance qualification remain open under M4, M6, M8, and M9.

### M0: Preserve and reset (rpgmigrate)

- [x] Guarded preservation: `stash@{0}` plus the 51 untracked files are archive commit `8fceb45` on `wip/native-copy-snapshot` (base `bd0925d`, never merged); `archive/stash-0` pins the stash; a tar of the untracked files sits outside the repo.
- [x] `feat/native-engine` branches from `8fceb45`; this roadmap is committed there.
- [x] Remove the superseded plans, the old task ledger, the superseded anchor spec and the previous team's guides and research notes; point `AGENTS.md` and `README.md` at this roadmap; record this session's corrections in `tasks/lessons.md`.
- [x] Record whether `feat/native-engine` builds as archived (`task check`), before any other change.
- [x] Create `tasks/todo.md` from these milestones; add the dated design revision with the amendments above (and fix its links to removed files).
- [x] Remove the M0 drop list; apply the libpq decision; plain CI for fmt, clippy and unit tests.

- [x] Close M0 using the recorded transport/self-check/review evidence and the integrated strict gates at `7b6037f`.

### M1: Contract freeze (both repos)

- [x] Commands, option names, env (`PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGPASSFILE`, `PGCOPYDB_LOG_JSON`, `PGCOPYDB_APPLY_GROUP_TXNS`), exit codes and error classes for the native profile.
- [x] Protocol v1 amendments: version/capability DTO, `native-report:` stdout transport, work identity env, caller-supplied endpos ack, drain proof with `certified_lsn`, cleanup inventory; compare outside ParseReport.
- [x] application_name scheme, owned schema name, workdir and `control/` layout, native runner image selection and tool layout.
- [x] Shared golden fixtures in both repos with tests on both sides.

- [x] Close M1 using the recorded engine/operator checks and review, with the strict-lint/integration hold resolved.

### M2: Clone slice (engine)

The approved [M2 plan](superpowers/plans/2026-11-08-m2-clone-slice.md) freezes the shared interfaces and seven disjoint owners.
Focused receipts cover the seven-case Clone profile on PostgreSQL 16.15/18.6, the schema drift guard on all three pairs, catalog admission and utility smoke, and canonical comparison SQL.
An earlier candidate passed a quiescent PostgreSQL 16 to 16 clone with eight-table schema/data/sequence proof and measured completion, plus operator-bound clone/progress validation by the authoritative Go parser.
The plain-clone alpha passed staged 16 to 16, 18 to 18, and 16 to 18 profiles: eleven cases and six actual Go report validations per pair, totaling 33 case executions and 18 report validations.
These receipts cover consistent parallel COPY, owned marker/generation recovery, first `--restart`, completed retry, drift refusal, read-only progress, and schema/data comparison within the admitted profile.
Final staged checks passed 417 Rust tests, 24 CLI tests, and 183 portable tests, with formatting and diff checks clean; all independent M2 review findings are resolved.
The frozen M2 Clippy run retained 165 library and 19 library-test unused/dead-code errors.
Later M3 primary integration resolves that technical blocker, and the leader accepted M2 closure from the staged evidence.
The final 58-entry runtime/harness input manifest remained unchanged after qualification.
Aggregate matrix and test Task commands were not rerun to exit zero after their earlier failures; the evidence is the final staged receipts, not an aggregate command pass.
The accepted M3 grouped CDC API/producer and strict closing gates satisfy the remaining M2 handoff.
Operator-controller lifecycle qualification remains M6 work.
See the [task ledger](../tasks/todo.md) for exact commands, evidence, and remaining gates.

- [x] Demonstrate a focused quiescent PostgreSQL 16 to 16 plain clone with durable completion and independent schema/data/sequence verification.
- [x] Resolve all independent closing-review repairs for target ownership, marker readback, generated output, and transaction durability proof.
- [x] Pass the final Prepared10 and Clone7 component profiles on PostgreSQL 16 and 18.
- [x] Pass the eleven-case 16 to 16, 18 to 18, and 16 to 18 profiles, each with six actual Go report checks.
- [x] Transport: noncommitting pipeline validation, private error/warning classification, observed commit proof, per-operation I/O budgets, and buffered COPY.
- [x] COPY authority: logged per-table markers, copy generation, exact owned identities, per-run fence, and durable unknown-COMMIT resolution.
- [x] Accept the M3 grouped CDC frontier/`certified_lsn` producer/API and apply hot-path qualification as the M2 handoff.
- [x] Plain-clone order: SQL-exported snapshot, matching-major pre-data, parallel physical-heap COPY with FREEZE, post-data, sequence sync, and ANALYZE on all three pairs.
- [x] Plain clone `--restart` and `--resume --not-consistent`; `list progress --json`; `compare schema`; `compare data --json` with pinned GUCs and the operator's shapes.
- [x] Podman engine E2E targets for all three pairs, controlled partial-copy/utility interruption and resume cases, and independent schema/data/sequence oracles.
- [x] Close M2 using the staged three-pair clone receipts, prior review, and later strict M3 closing checks; no M2 aggregate rerun is claimed.

### M3: Follow (engine)

Native 7 / metadata 5 ongoing-follow smokes passed on PostgreSQL 16 to 16, 18 to 18, and 16 to 18: initial COPY, valid source DML, independent schema/data parity, target frontier progress, forwarded status, and actual source confirmed flush.
The later full 21-case matrix passed on all three pairs, totaling 63 named case executions and 42 actual clone-bound Go report validations while fixtures were alive.
The matrix retains controlled ongoing-follow stops (`successful_follow_exit=false`); it qualifies M3 follow, while M4 cutover and independent power-loss durability remain open.
On 2026-11-08, the first full-profile 16 to 16 run stopped RED waiting for initial startup in `before_commit_rollback`, with no passed-case count or full-profile receipt.
No native Error, SQLSTATE, or stderr was retained.
Six threaded tests confirmed that the harness proxy misclassifies ordinary COPY `r`/`w` without a logical handshake; the minimal patch passed all 38 portable checks, but its causal connection to the startup timeout remains unconfirmed.
The bounded PostgreSQL 16 diagnostic stopped RED at the same initial readiness wait before fault injection or rollback, with unchanged Rust and zero proxy errors.
The captured RED at 2026-11-08 22:50:55 UTC showed COPY workers progressing and closing shortly before timeout, without a held fault, SQL error, or sampled lock wait; readiness correctly waits for post-data and anchor.
Replanning found that the before-COMMIT fault gate misses the engine's extended-protocol COMMIT; actual Linux relay measurements also identify likely proxy latency, without proving the startup cause fixed.
The leader approved two harness TCP_NODELAY options and bounded extended-COMMIT/data-transaction witness gates; engine Rust and deadlines remain unchanged, and no next runtime run precedes the frozen fix.
The harness also fixes a confirmed fault-gate bug that forwarded queued COMMIT after backend death; its latest focused checks passed 47 follow and 13 clone cases.
The immutable proof and exact input manifest are recorded in the [ledger](../tasks/todo.md); all four owned containers and the network were proved absent after cleanup.
Prior RED attempts and their bounded startup, fixture, oracle, error-precedence, and identity fixes remain recorded in the [ledger](../tasks/todo.md).

All five [identity UPDATE fix](superpowers/plans/2026-11-08-m3-identity-update-fix.md) packages are frozen; the joined candidate passed all-target check, strict Clippy, and formatting without allowances.
Closing-gate discovery assigns 116 required native cases per major, excluding the twelve-case legacy SourceSnapshot stage removed in the private retirement candidate below.
The exact-tree PostgreSQL 16 Engine50 run stopped with 49 PASS and one FAIL at `checkpoint_live_tests.rs:795`: the assertion expects `fsync on`, while the shared diagnostic requires `fsync and full_page_writes on`.
The unsafe target was refused; the assertion-only successor is integrated at manifest `d85e84396608751e161646a3ecc43bba4eaa30ca360c474e037edd62fb57e8ec`, with 295 of 296 files unchanged and 48 explicit absences preserved.
The successor passed PostgreSQL 16 Engine50, including the repaired `async_barrier` assertion and six live-fault receipts.
Catalog13 then stopped with twelve PASS and one FAIL: CDC tests intentionally commit real heaps into the shared catalog/clone database, and the clone collision guard correctly refuses those leftovers.
The two-file fresh-empty-clone fixture repair is frozen at manifest `69086a1592f1885d948d85de380e7f5537e5e540c44aaf4c7cbf61a04d36057f`, with fourteen focused checks passing and Engine50/Authority19 inventory assertions corrected.
It preserves 294 other files and 48 explicit absences, without changing Rust production code or weakening the guard.
After correcting the matching PGDG 16 tool PATH, ninety PostgreSQL 16 tests passed across seven profiles.
FollowAuthority11 initially stopped with ten PASS and one cancellation-test FAIL; the retained diagnostic lacked a same-PID COMMIT/SyncRep witness.
The test-only readiness successor preserves the safety assertions and records positive cancellation witnesses.
All ten required native profiles then passed on PostgreSQL 16.15 and 18.6: 116 tests per major, 232 PASS, zero failed or ignored.
The result combines 142 fresh tests on source manifest `c155b7c3775dbb7f063dadfcaa054b77d086b193f808b73b9f06d5d50a347839` with ninety retained PostgreSQL 16 tests whose selected inputs remain unchanged.
All owned fixtures and processes were cleaned; the [ledger](../tasks/todo.md) records the exact proof and retained-source provenance.
All eleven disjoint legacy-stage retirement paths are joined in private candidate manifest `3a9e30d41b0fb93da5df02aca8e7abbb78f5189d317361be67a21c7a9ad008c2`.
The candidate contains 294 files and fifty explicit absences, with no production behavior change and native116/COPY40 inventories preserved.
Its portable inventory registers thirteen suites and 254 tests; the final offline checks passed before primary promotion.
The source-snapshot profile passed nine exact cases per major on PostgreSQL 16.15/18.6, with zero ignored cases and independent fault receipts.
Legacy-stage retirement is integrated at source commit `7b6037f9ac8214696936a21c865cd97307cc5c6e`, with the offline gates and independent M3 review passing.
The money/cross-locale clone and source-valid deferred-UNIQUE parity have focused per-major proof; deferred-failure rollback remains an origin-mode Authority obligation.
The retained canonical operator lint run at exact checkpoint `5e8cbcb` failed with 25 diagnostics (24 `goconst`, one `unparam`); the repaired successor below clears the full lint gate.
Canonical `task test` passed all 15 packages uncached at exact clean operator checkpoint `5e8cbcb`; PostgreSQL 16 cluster-free race checks passed 508 checks with two expected skips, and PostgreSQL 18 coverage race checks passed 158 without skips.
Both exact owned PostgreSQL containers and the private envtest processes were proved absent after cleanup.
The twelve-path lint repair at tree `d9003a7939eac53ffe471134196440906794f45f` passed full canonical lint: all sixteen stages, zero issues.
Its full source hash `f9993ced4f02d262c2e41336a08a3c8667f38dd08ce6c3ad31bf459a7c4b8d54` was unchanged before and after verification.
The repair changes equivalent test constants and one production comment; earlier Go checks remain distinct exact-source evidence, without a successor rerun claim.
Unknown WAL headroom remains an operator obligation, and the admitted recovery/profile boundaries remain unchanged.

- [x] pgoutput v1 decoder (B, C, R, Y, I, U, D, T, O; others fatal) with relation generations and PostgreSQL 16/18 regression coverage.
- [x] Receive thread with decision 3 backpressure and status rules; slot health checks; gap refusal on connect.
- [x] Applier: RI FULL keyless (ctid single-row), RI INDEX, unchanged TOAST, multi-relation TRUNCATE (ONLY, never CASCADE), identity and generated columns, non-built-in types as text; pipeline groups; strict dedupe.
- [x] Phase-aware restart/resume admission and recovery within the qualified profile, including resume after the anchor.
- [x] Pass native 7 ongoing-follow smokes on all three declared pairs with independent convergence and actual source-confirmation evidence.
- [x] Pass the exact nine-case native source-snapshot profile on PostgreSQL 16 and 18 with independent fault receipts.
- [x] Pass the full 21-case follow profile on all three declared pairs, with 42 actual clone-bound Go report validations.
- [x] Pass all ten native profiles on PostgreSQL 16.15 and 18.6: 116 tests per major, zero failures or ignored tests.
- [x] Join all eleven authorized legacy-stage retirement paths in the private candidate.
- [x] Pass the retired candidate offline closing gates and integrate the reviewed result into the primary tree.
- [x] Complete the independent whole-M3 safety review and commit source as `7b6037f9ac8214696936a21c865cd97307cc5c6e`.
- [x] Pass the declared M3 fault/recovery, filtered-WAL, keepalive, and round-trip controls.
Parser fuzzing and measured catch-up performance remain M10 and M9 qualification gates.

### M4: Cutover, drain proof, cleanup (engine)

The leader accepted the revised [M4 plan](superpowers/plans/2026-11-08-m4-cutover-control.md) design and contracts conditionally on the exact final M3 hash/interface/version freeze.
On 2026-11-08, isolated M4 build work started from the exact `2b4` donor; authority, control, receiver, and apply owners use xhigh effort.
The repaired isolated assembled candidate passed formatting, all-target check, and strict all-target Clippy without allowances, with all 304 files and 48 absences unchanged.
These isolated M4 candidate checks do not qualify runtime, Go producer proof, E2E, or M4 closure.
Catalog13 discovery passed on the attribute-only successor without database qualification.
The M4 harness is frozen with 217 focused checks passing at full candidate manifest `6b63e0a9bff0bd33a5798a51cedd41feb0baccea4b7d560950455aecd87b221f`, preserving 306 files and 48 explicit absences.
The offline native-8 build passed against integrated source `9158fef5601fa3b9c3d7c5c0c1223a761071a0aafce8d9e635ca207f79edb155`, producing binary `c185bedf97c021558092b0930815ff7b17fcc9e85c5a666552bb779607af2187` linked to libpq 180006.
The first PostgreSQL 16 to 16 producer case failed its active-fence assertion, leaving zero qualified M4 runtime cases.
The retained sentinel failure was CLI `BadArgs` exit 1 from a trailing-capture parser bug; worker Target7 after teardown is not causal evidence.
All three owned containers and the network were proved absent.
A two-file retention repair passed 93 focused checks, and a separate one-file CLI fix at `39b791fd` passed 31 parser checks.
The Linux rebuild and first-case retry remain unrun; M4 runtime is unqualified.
Positive cutover Go qualification still awaits the independent M6 reader.
Completed-positive Go validation explicitly depends on the M6 independent reader; the old clone probe cannot accept the new completion fields, so engine producer checks are not a Go gate pass.
The 2026-11-08 clarifications are recorded in the [M4 plan](superpowers/plans/2026-11-08-m4-cutover-control.md), covering endpos, retained frontiers, fresh proof, durable sealing, and guarded closure.
All 41 implementation steps remain open; these static checks establish no M4 runtime qualification.
Local endpos reads are allowed, but informational JSON cannot expose unproven certified or durability counters.
Durable replay, drained status, and proof reads require positive fenced observation, with bounded explicit refusal when busy or unknown.
The native operator must sample and persist one E, then retry that identical E explicitly after a bounded refusal.
The worker processes persisted E independently of acknowledgment; the M6 standalone helper retries identical E for final positive proof after the worker exits.
We add no IPC or fence handoff for this path.
Native 8 / metadata 6 applies only if the accepted M3 identity-fix baseline is 7 / 5; uncertain slot incarnation or reappearance must be preserved under the accepted exclusive-administration prerequisite.

- [ ] `stream sentinel set endpos <lsn>|--current` and `get --endpos|--replay-lsn|--json` per decision 6.
- [ ] Drain certification, final sequence sync, exit 0 only with proof.
- [ ] `stream cleanup`: owned slot and publication only, idempotent, workdir kept for later compares.
- [ ] Engine E2E: full follow migration under pgbench load (including `synchronous_commit=off` writers), endpos, drain, compare match and cleanup, plus a crash in each phase, on 16→16, 18→18 and 16→18.

### M5: Packaging and engine CI (starts with M2)

The development-only M2 harness image builds, and the documentation lint gate passes all 17 discovered Markdown files.
M5 workflow controls (seven tests), image smoke controls (12 tests), actionlint, yamllint, and dependency license/advisory checks pass.
The native ARM64 image passed its read-only packaging smoke with UID 65532, libpq 18.6, and client tools 16.15/18.6; later clone/schema edits mean that receipt does not bind the final source.
The final-source image, AMD64 and hosted checks, and the M5 closing gate remain unqualified.
These checks do not complete the released runner or hosted engine CI gates below.

- [ ] Runner image: rpgmigrate, sh, psql 18, PGDG client tools 16 and 18 under `/usr/lib/postgresql/<major>/bin`, GNU coreutils and sed, uid 65532, read-only rootfs, multi-arch, smoke test.
- [ ] GitHub Actions: lint, test, cargo-deny (licenses, advisories), docs lint, engine E2E matrix on hosted runners, image build, ghcr push on tags only.

### M6: Operator native adapter (operator, on a branch from `feat/rpgmigrate-native-operator` after a fetch)

Pure command and framing helpers passed focused checks on the separate operator branch, including legacy argv preservation and negative identity bindings.
Production `UnsupportedEngine` activation remains unchanged, so none of the lifecycle gates below is complete.
The actual Go producer proof covers the M2 clone/progress prerequisite; durable drain still depends on M3/M4.
Remaining native lifecycle planning preserves independent checkpoint evidence through a bounded existing-psql/private-PVC qualification spike; engine reports or acknowledgments cannot supply that evidence.
The later test-only boundary prototype passed 194 cases: 120 without a database, 37 on PostgreSQL 16, and 37 on PostgreSQL 18.
This component evidence does not complete Task 0, establish a production API, or authorize native activation; independent evidence qualification and review remain open.
Consumer interfaces remain unfrozen pending qualification and review; production constructors/helpers and native activation remain unauthorized.
The leader accepted final M6 plan SHA-256 `c3ba6a68ac5dc4ca2ad269ccf0cd3d6949a2ffc15d5fb888ad28b1815d9b47ac`: informational source-slot telemetry uses twenty-second freshness held in new private constant-size state on the existing Sampler run.
No CRD, new cache, CLI/DTO change, or checkpoint evidence authority is added.
The composed 21-file test-only successor `031f9d4` passed 67 focused Go checks and eight Perl checks.
The composed PostgreSQL 16 run recorded 26 passes before the first lost-COMMIT case failed to qualify.
The fault is proven through fifteen stages and relay observation of COMMIT/Idle without completion evidence.
A fresh retry refused target open; source tracing points to absent environment values becoming empty, with a pure environment correction under work.
No healthy retry or PostgreSQL 18 qualification is claimed.
This prototype does not qualify Task 0 or authorize production activation.
Application write fencing and ending outstanding transactions remain the operator user’s duty in both cutover modes; no fencing subsystem is added.

- [ ] Engine-aware argv0 and scripts (worker, verify, cleanup, compare, catalog, endpos exec); lift `UnsupportedEngine` for the qualified profile; `--rpgmigrate-runner-image` flag and chart value.
- [ ] Preflight: `version --json` handshake into `validateNativeConfig`; native pg_restore gate by target major; skip the origin-grant probe; PG16 target allowed for native.
- [ ] Phase from `native-report:` lines; native drain verification bound to status endpos plus a psql read of the owned control row; cleanup requires `complete=true`; plain-clone completion fails closed on a missing report.
- [ ] ORed application_name patterns for the retry-gate census, sampler and zombie reaper; native error-class prefixes in the log matchers.
- [ ] Unit and envtest coverage; operator docs for the native engine.

### M7: Operator-driven E2E

The leader accepted the [M7 bootstrap slice](superpowers/plans/2026-11-08-m7-kind-bootstrap.md) for code-only implementation by separate lifecycle and suite owners on isolated operator branches.
The frozen eleven-file M7 join includes 57 passing bootstrap controls and its self-check plus focused suite checks; full custom lint remains a closing blocker.
Actual artifact preparation refused the pinned legacy runner: its binary is `0.18.129.gd7547b5`, while the required builder baseline is `0.18.144.g86bf908`.
A separately qualified local ARM64 v0.25.0 candidate matches the exact required `86bf908` builder binary and tool/nonroot/workdir contract; its inventory-pin correction is guarded-joined.
AMD64 runtime, manager/archive/cluster, and full gates remain unqualified; the original refusal is retained.
The ARM manager build passed, but Podman OCI save was rejected because gzip recompression changed the pinned manifest.
Artifact preparation then passed its byte/receipt checks using existing skopeo with published pins preserved: seven role archives, exact selected-node-child verification, and the trusted legacy runner binary.
The approved reproducibility/capacity plan is `831ebb2e4ac1b723b98c5d4c3e1311fce6a1b1f14e9138888cf7aaf3c00418d2`.
Task 10 is frozen at manifest `5069c96437b0268ce7db86507d7f7d4c5e804fbc992197ee540a86cea6d1a4fb`, with 131 focused checks passing.
Task 12 is frozen at manifest `85399fc3e8ebde3aa55a5c8101fd9114503c7c4ef7466e39bb0e82d0e3acfaab`, with 153 focused checks and its self-check passing.
Task 11 documentation is frozen, and the third tracked preparation ran against source `1b7ca392fa63d4bd2c0662b355e3299f334df84fb19c3704a2b24d63baa2df46`.
Its manager build and seven archive roles passed, but the baseline stage refused the multiline version-probe parser.
No tracked check or up followed, all owned containers were proved absent, and actual kind remains unrun.
Task 13 corrective planning found another producer boundary: Podman removal deletes transient CID files, leaving declared CID receipts missing.
The Task 13 correction separates transient CID files from immutable receipts and validates complete version-output framing.
The later tracked prepare and check passed on source `6222196c83af94e5652e72921b1e4010c72160221a90a58cab702942a10ce1bf`, with all 530 source entries unchanged.
Kind up remains blocked by the fresh disk check: 13,300,498,432 bytes available versus 14,780,850,176 required, a shortfall of 1,480,351,744 bytes.
The requirement includes the 10-GiB floor, three-node work bound, and 1-GiB margin.
First up remains blocked on the disk reserve and explicit approval of the read-only AppleHV 100-to-128-GiB resize plan.
Backup Phase A is verified and Phase B is pending.
The original PostgreSQL 18 data uses tmpfs and requires a tested persistent replacement before any VM or original-container restart.
No up, prune, or resize is authorized by the preparation receipt.
Earlier partial run directories remain preserved and cannot be adopted.
The aligned standalone Podman cap probe proved update to two CPUs / 4 GiB with no effective swap; it does not qualify actual kind topology or cluster resource caps.
The successful preparation binds only its exact source; actual kind topology/caps, legacy workflow, and AMD64 remain unqualified.
No manual proof JSON or lease renewal is allowed.

- [ ] kind (podman provider) bootstrap with pinned CNPG, CRD apply and image load as Task targets; validated first with engine=pgcopydb on 17:17 (can start right after M1).
- [ ] Suite knobs: `E2E_ENGINE`, `E2E_RUNNER_IMAGE`, `E2E_SMALL`; DropIfExists off and filters dropped under native; engine-aware `checkMajors`; a positive `native` label set.
- [ ] Native runs on 16:16, 18:18 and 16:18: live writer load follow, manual and automatic cutover, verification, runner kill mid-copy, mid-stream and mid-drain, target primary kill with CNPG synchronous replication, suspend/resume, deletion cleanup.
- [ ] Hosted GitHub Actions job on kind at small scale.

### M8: Feature widening (each item enables its capability)

- [ ] Binary COPY with the per-table built-in-type predicate and text fallback.
- [ ] ctid split parts (`--split-tables-larger-than`, `--split-max-parts`) with per-part done markers.
- [ ] Large objects: copy, follow re-sync after drain, compare coverage, data-phase restart cleanup.
- [ ] Re-run the M7 native matrix with operator defaults.

### M9: Durability modes and measured performance

- [ ] `replay-safe` barrier cadence chosen from benchmarks; effective values in help and status; `relaxed` only if the measured gain exists.
- [ ] Benchmark harness: A/A noise runs first, then interleaved randomized pairs in fresh processes; report target-committed rows and transactions per second, catch-up rate, commit-to-apply p50/p95/p99, and COPY MiB/s on narrow and wide tables; compare fork default against `replay-safe` and label `durable` as the stronger contract.
- [ ] `docs/durability.md` crash matrix per mode: process kill, host power loss, target crash, target failover with and without a synchronous standby, source WAL retention.
- [ ] Optional binary pgoutput, gated per stream on a built-in-type allowlist, only if measured.

### M10: Release readiness

- [ ] README index, getting started, durability, operator guide, troubleshooting; docs drift and STE100 checks.
- [ ] Parser fuzzing and a sanitizer or Miri job over `src/pg/ffi.rs`.
- [ ] Independent reviews of durability, recovery and the operator diff; final matrix on the exact image digest.

## Execution model

The leader decomposes, assigns one owner per file or subtree, and reviews; verification agents never review their own work.
Effort per task: xhigh for WAL, apply, recovery, durability and safety reviews; high for bounded implementation and tests; medium for docs and CI; each brief states the reason.
Each milestone gets a detailed implementation plan (superpowers:writing-plans) before work starts.
Parallelize the build phase as far as file ownership allows: independent owners work at the same time, each in its own git worktree off `feat/native-engine` (user-approved 2026-11-08, overriding the earlier no-worktree rule), and the leader integrates.
Run the independent review (at the effort its risk calls for) and the Go test suites once per milestone, at its close, not after each task; workers run only the focused tests for their own change while building.
Operator work goes on its own branch and PR, and existing operator edits (`tasks/todo.md`, `.agents/`) stay untouched.
The user-approved VM allocation is 14 CPUs, 32 GiB memory, and a 100-GiB disk.
The original 21:35 UTC startup failure predates the CPU/memory resize.
M7 image preparation may overlap database testing within the approved measured-capacity windows, retaining host Cargo headroom.
Initial uncapped kind creation requires database testing and host Cargo builds to be idle until all three node caps are verified.
See the [capacity and preservation record](../tasks/todo.md#capacity-and-parallel-execution-user-update-2026-11-08) for budgets and the restart prerequisites.
Commits are small, conventional and lint-clean; nothing is pushed or merged without the user's go-ahead.

> [!important]
> On 2026-11-08, the user approved exactly two local WIP checkpoint commits: `e544f2d` on `feat/native-engine` and `5e8cbcb` on the isolated operator `feat/native-contract-v1` branch, as an exception to the lint-clean-per-commit rule.
> Those checkpoints predate the qualified M2 staged pair receipts and the normal M3 source commit `7b6037f`, whose primary strict gates resolve the scaffold lint hold.
> The two-commit exception is consumed; the normal lint-clean rule has resumed, and M3 worktrees remain outside their scope.

## Verification

- Engine: `task lint test`, `task test-db` for cargo live tests, and per-pair E2E targets (`task e2e PG_SOURCE=16 PG_TARGET=18` and a matrix target) including crash and drain cases.
- Operator: unit and envtest suites; `task e2e:kind:up` then the native E2E target for each pair; success means Complete with CutoverCompleted, a native drain proof and a data verification match.
- CI runs the same targets on hosted runners; the final release matrix runs on the exact image digest.
