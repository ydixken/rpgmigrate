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
> M0–M5 are closed; M4 engine source and six behavior documents are integrated on `feat/native-engine` at `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476`.
> M4 passed host lint/check, 499 Rust tests, 27 CLI tests, 324 portable tests, forty fresh native sampling cases, and all 93 Follow21/Cutover10 runtime cases.
> The prior nine-profile 266-case native subset retains its original provenance and is separate from the forty fresh samples.
> The single whole-M4 review is resolved with no findings remaining; operator test-only evidence is committed separately as `c262322909ac03a7101ea3bfac2c4823efa62e95`.
> Final repaired-runtime actual-seven Go evidence is private PostgreSQL 18 interoperability with mocked Kubernetes ownership and production admission disabled.
> M6–M10 remain open: production native adapter, kind E2E, feature widening, power-loss/performance qualification, and release gates remain unqualified.
> On 2026-11-09, the user authorized unused Podman image/volume/build-cache pruning, the PostgreSQL 18 preservation/128-GiB maintenance plan, and the scoped M6 Python/ctypes/libpq runner dependency exception.
> VM preservation/resize maintenance and failed-node cleanup passed; corrected legacy-only M7 UP/load passed, then its blocked Loaded workflow was retired with retained evidence and source-only CHECK/index fixes awaiting fresh runtime qualification.
> Feature-branch pushes to origin for hosted CI, including M5/M10 native AMD64 checks, are user-authorized within the dated limits below.

Remaining work follows the existing M6 production adapter and M7 operator E2E dependencies, using the accepted M5 packaging/CI evidence.
On 2026-11-09, the user directed us to push through M10: continue all approved M6–M10 execution gates rather than stopping at intermediate plans or source checkpoints.
Milestone closure still requires evidence, benchmark runs remain unapproved, and main/tag/release/GHCR boundaries remain unchanged; no ETA or measured speedup is implied.
Keep their image, Kubernetes/PVC, dependency, capacity, and release gates explicit; M4 engine qualification does not activate the operator.

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
The matrix retains controlled ongoing-follow stops (`successful_follow_exit=false`); it qualifies M3 follow rather than M4 cutover or independent power-loss durability.
Final M4 closure is recorded below; power-loss qualification remains M9 work.
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

All repaired M4 qualification gates passed on source `2034c420f18ec0f32064eb8bb90c74c14da8a54a8f858095abbb888ab714a926` and ordinary runtime `1fd03e42f9656495816d6c36a5be32eec9fa69a319d2ab28506d9cd979578a52`.
Canonical host lint/test/check passed: 499 Rust tests, 27 CLI tests, and 324 portable tests, with 22 ignored and 107 filtered Rust cases recorded separately.
Fresh SourceCatalog3 and FollowAuthority17 passed on PostgreSQL 16.15 and 18.6, forty cases with zero ignored.
The prior nine-profile 266-case native subset remains separately retained with original provenance; it is not an invented all-current native total.
Follow21 and Cutover10 passed on all three declared pairs, totaling 93 cases, with checked-close GREEN on each pair.
Producer/local report validation remains separate from final repaired-runtime actual-seven Go acceptance.
That single seven-case invocation is private PostgreSQL 18 interoperability with mocked Kubernetes ownership and production admission disabled.
Source/runtime/libpq, 954-file privacy scans, and owned runner/server/network cleanup passed.
The single whole-M4 independent review's P2 is resolved, no findings remain, and the final six-document export is accepted.
The [ledger](../tasks/todo.md) preserves old-runtime RED, repaired GREEN, exact receipts, prior-source evidence, and resource limits.
The separate 24-path operator test-only work is committed as `c262322909ac03a7101ea3bfac2c4823efa62e95`, without activating production native paths.
M4 is complete: the 61-path engine source/document integration is committed and fast-forwarded to primary `feat/native-engine` at `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476`, tree `4e9a151afa82c039aa2bb2c705fded6d358d5d4a`.
The tested source bytes, six accepted documents, fifty absences, and 244 protected files were verified; operator production admission remains disabled until M6.
Local endpos reads are allowed, but informational JSON cannot expose unproven certified or durability counters.
Durable replay, drained status, and proof reads require positive fenced observation, with bounded explicit refusal when busy or unknown.
The native operator must sample and persist one E, then retry that identical E explicitly after a bounded refusal.
The worker processes persisted E independently of acknowledgment; the M6 standalone helper retries identical E for final positive proof after the worker exits.
We add no IPC or fence handoff for this path.
The qualified native 8 / metadata 6 runtime has 29 control columns and refuses older formats; uncertain slot incarnation or reappearance must be preserved under the accepted exclusive-administration prerequisite.

- [x] Retain the ancestor 27-case engine-producer PASS across all three pairs.
- [x] Pass the exact seventeen-case FollowAuthority profile on PostgreSQL 16.15 and 18.6 for the focused repair successor.
- [x] Pass canonical host `task lint test` and `task check` on the repaired utility successor.
- [x] Pass the exact three-case Schema profile on PostgreSQL 16.15 and 18.6 for the repaired utility successor.
- [x] Pass Clone7 on PostgreSQL 16.15 and 18.6 after restoring the source-parity marker-test literal.
- [x] Pass exact Engine50 on PostgreSQL 16.15 and 18.6 after the selector and M3 assertion restorations.
- [x] Pass the nine-profile M4 native closing subset on PostgreSQL 16.15 and 18.6, including uninterrupted Receive11 cadence coverage.
- [x] Retain all ninety reviewed-ancestor Follow21/Cutover9 runtime case executions across the three declared pairs.
- [x] Pass fresh checked-close SourceCatalog3/FollowAuthority17 sampling on both majors: forty cases, zero ignored.
- [x] Pass the first repaired Cutover10 pair on PostgreSQL 16 to 16, including checked-close GREEN and healthy retry.
- [x] Qualify the checked-close successor, including SourceCatalog3/FollowAuthority17 on both majors, the new 93-case matrix, and actual-seven interoperability on its new binary.
- [x] Record the separate 24-path operator test-only commit `c262322909ac03a7101ea3bfac2c4823efa62e95`.
- [x] Close M4 at engine source commit `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476` after verified integration and the separate operator test-only commit.
- [x] Pass fresh current-code runtime matrices and actual engine/Go interoperability, then complete M4 closing review/gates.
- [x] `stream sentinel set endpos <lsn>|--current` and `get --endpos|--replay-lsn|--json` per decision 6.
- [x] Drain certification, final sequence sync, exit 0 only with proof.
- [x] `stream cleanup`: owned slot and publication only, idempotent, workdir kept for later compares.
- [x] Engine E2E: full follow migration under pgbench load (including `synchronous_commit=off` writers), endpos, drain, compare match and cleanup, plus a crash in each phase, on 16→16, 18→18 and 16→18.

### M5: Packaging and engine CI (starts with M2)

Local native ARM64 image `b4631063096125a84d3428571fe81d7057c91cf3356adf54cc738744709e52b0` passed its build/read-only runtime smoke on input digest `296279da95b924a59a903034a89f30853d037039368026859ce5d6ecc6eb9d9a`.
The [ledger](../tasks/todo.md) binds that retained image evidence and the same-review portability correction; the test-only R2 change is outside the 65 image inputs.
The first [hosted run](https://github.com/ydixken/rpgmigrate/actions/runs/42625499630) on pushed `34208b6` ended with five PASS and three FAIL jobs, including all three migration pairs passing.
The docs MD038 failure is fixed in `55ea9c7`; the Dockerfile heredoc-parser failure and version-dependent test are corrected and integrated in `7c04dff673f41b3a27c5d0afbc6dbe5e6b551c03`.
That exact feature SHA is pushed and remote-verified; its [push-triggered hosted run](https://github.com/ydixken/rpgmigrate/actions/runs/42630883414) passed all eight jobs, with all five artifact ZIP digests and both native image input bindings verified.
Hosted AMD64/ARM64 images passed uid 65532/stock-libpq checks on the exact 65-file input digest.
M5 is complete after the same reviewer’s final acceptance of hosted multi-architecture, read-only runtime, source/artifact, clone-inventory, and retained stock PostgreSQL 16/18 evidence bindings.
The retained database evidence preserves its original 266-case subset and forty-case sampling/composite provenance; exact source/Cargo/build/Taskfile/test/harness bindings are unchanged from `67f7fcb` to `7c04dff`.
This acceptance claims neither a fresh aggregate database run nor migration using the production image; broad operator Go checks are not applicable to engine-only packaging.
The operator-publication P1 is resolved by the dated explicit GitLab mirror authorization; it remains separate from engine M5 qualification.
No tags, releases, or GHCR publication are authorized, and these checks do not close M10.

- [x] Runner image: rpgmigrate, sh, psql 18, PGDG client tools 16 and 18 under `/usr/lib/postgresql/<major>/bin`, GNU coreutils and sed, uid 65532, read-only rootfs, multi-arch, smoke test.
- [x] GitHub Actions: lint, test, cargo-deny (licenses, advisories), docs lint, development-image clone jobs on hosted runners, and both native image builds.
- [ ] GHCR publication on tags only, pending separate publication authorization.

### M6: Operator native adapter (operator, on a branch from `feat/rpgmigrate-native-operator` after a fetch)

Production native admission still returns `UnsupportedEngine`; M4's engine and mocked-Kubernetes helper evidence does not qualify the operator lifecycle.
Task 0's five-file pure-helper export is frozen with nineteen Python and eighteen focused Go tests passing, including exact PG16/18 SQL fixture equality.
The earlier admission-reader handoff separately passed thirteen top-level tests and 330 focused checks with zero failures or skips; the final relay-aware Reader composition passed fourteen top-level tests and 344 focused checks with zero failures/skips against the exact new relay/transport inputs.
The new relay export passed 29 Python and nineteen focused Go tests (including ten relay subcases), with five mutation controls RED.
The new transport export passed 97 checks/92 leaf cases; these are focused mechanism proofs, without runtime qualification.
These focused checks do not qualify packaged Python/libpq, database, Kubernetes/PVC, or opaque-file runtime behavior.
Four clean M6 source/tool commits (`ccda81f3`, `3d1bfe6f`, `f6f7de44`, `32dbcfc`) are integrated by fast-forward in the central operator feature branch.
The exact fifteen-path/557-file checkpoint passed both packages with zero lint findings and retained focused tests; remote origin remains `5616d82`, without a checkpoint push or M6 activation/review/full-Go/runtime closure.
The second four-commit M6 packet (`f28c450`, `b8c6f51`, `1a569b7`, `4ecbd69`) is accepted and fast-forwarded into the central operator feature branch at `4ecbd69cc9e56e4710096b6f0475fc7d2a62fe8b`, with all fourteen paths/560 files verified and clean.
This checkpoint adds no push, CI, or runtime result.
The approved [worker construction/dispatch plan](superpowers/plans/2026-11-09-m6-native-worker-construction-dispatch.md) has frozen Commands (91 focused plus thirteen legacy checks, zero lint) and worker builder (52 checks, zero lint, three new connection goldens with legacy preserved) handoffs, not yet committed.
Lifecycle focused tests passed 94 checks, with final owned-test lint repairs underway; the native optimistic status patch is approved and top-level `UnsupportedEngine` remains unchanged.
Actual Linux executable parity is assigned to a network-none ephemeral M5 container, without a result yet; discovery/preflight planning must not reuse legacy raw-DSN argv or raw-stderr scripts.
Production activation, whole-M6 review, full-Go, and runtime closure remain open.
The resolver disposition favors qualifying the existing standard ClusterFirst sandbox resolver; actual runtime/NSS/HOME bindings remain gaps, with no ConfigMap, numeric-only, or HOME change authorized.
The [dated delivery amendment](#m6-default-kubernetes-delivery-amendment-2026-11-09) remains subject to integration and runtime qualification.
The [dated explicit-file/cap amendment](#m6-explicit-connection-file-and-cap-amendment-2026-11-09) has a cap-only admission handoff with nine tests/105 checks on the old relay input; it does not qualify the new Python binding implementation.
A tiny actual Linux ARM primitive probe passed six nonconnecting groups on M5 image `b463`; the later metadata probe found image ENV HOME absent and Podman-synthesized HOME/passwd `/work`, without proving immutable-image/containerd HOME.
Stock libpq 18.6 and absent default paths were observed, with exact probe cleanup/original preservation; TLS/database/Kubernetes/full-helper, ordinary-file, service, and resolver qualification remain open.
The approved Python/ctypes/libpq exception and no-added-Secret-read/RBAC boundary remain in force: bind actual pod/template references, stable privately consumed bytes, and SQL identity rather than latest-Secret-version attestation.
The user explicitly approved existing operator-feature GitLab mirroring on 2026-11-09; the prior authorization hold is resolved, with CI/mirror outcomes tracked separately below.
One independent whole-M6 review belongs at milestone close, and users retain write fencing/outstanding-transaction duties through cutover.

- [ ] Engine-aware argv0 and scripts (worker, verify, cleanup, compare, catalog, endpos exec); lift `UnsupportedEngine` for the qualified profile; `--rpgmigrate-runner-image` flag and chart value.
- [ ] Preflight: `version --json` handshake into `validateNativeConfig`; native pg_restore gate by target major; skip the origin-grant probe; PG16 target allowed for native.
- [ ] Phase from `native-report:` lines; native drain verification bound to status endpos plus a psql read of the owned control row; cleanup requires `complete=true`; plain-clone completion fails closed on a missing report.
- [ ] ORed application_name patterns for the retry-gate census, sampler and zombie reaper; native error-class prefixes in the log matchers.
- [ ] Unit and envtest coverage; operator docs for the native engine.

### M7: Operator-driven E2E

First UP passed on the corrected 530-entry source `5cc72893`: three Ready nodes, API readyz, and verified aggregate caps of six CPUs/12 GiB.
The frozen seven-role inventory is legacy PostgreSQL 17 only; native/PostgreSQL 16/18 entries are null placeholders, and this inventory does not establish M5 native-image qualification.
Tracked CHECK before load and load passed; phase is `Loaded` with six loaded-digest receipts and positive post-load readiness.
The original finite supervisor hit a read timeout during load; guarded DOWN correctly refused the active RUN lock and exited its handoff without deletion or source/receipt repair.
The watcher was restored under the original deadline with a recorded 648.049941-second gap, then intentionally stopped before expiry when the blocked Loaded workflow was retired.
Exact three-node container/process/cgroup removal is complete under standing approval; all 203 volumes, 339 images, immutable network properties, original four services, backups, source, and Loaded RUN evidence remain preserved, with actual CPU/RAM reservation released.
The CHECK state-loss and storage index/child comparison fixes passed 189 source-level unit cases on frozen 530-entry source `5dcceda`; fresh PREPARE/CHECK passed on that corrected source, without a new UP or workflow qualification.
The private S1/S2 handoff implementation `f57314` passed 33 fake controls only; live handover remains unqualified and no lease is renewed.
The approved [tracked kind-only Go pre-uninstall build-info hook](superpowers/plans/2026-11-09-m7-tracked-build-info-capture.md) supersedes the private observer implementation proposal and is building with bounded installed-client-go sanitization/cancellation fixes; metrics permission remains unapplied.
The Python handoff passed 198 source-level tests; neither handoff establishes live qualification or a new UP.
The prepared `5dcceda`/`47a` source/manager RUN has no UP result and requires a fresh source/manager/RUN freeze after the hook.
These bootstrap/load observations do not qualify the legacy migration workflow, native inventory, PVC behavior, or full M7 lifecycle.

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
No tracked check or up followed that third preparation attempt, and its owned containers were proved absent; the later first-UP failure is recorded below.
Task 13 corrective planning found another producer boundary: Podman removal deletes transient CID files, leaving declared CID receipts missing.
The Task 13 correction separates transient CID files from immutable receipts and validates complete version-output framing.
The later tracked prepare and check passed on source `6222196c83af94e5652e72921b1e4010c72160221a90a58cab702942a10ce1bf`, with all 530 source entries unchanged.
The earlier 14.78-GB demand was a planning envelope, not a freshly verified hard threshold or a field in the frozen wrapper/receipts.
The exact frozen wrapper enforces a 10-GiB VM floor and 2-GiB host floor; its seven prepared archives total 798,203,904 bytes.
The pre-maintenance read-only baseline had 12,565,581,824 free bytes and only 419,700,736 conservatively reclaimable allocated bytes, insufficient against the prior envelope.
That read-only check made no resource change; the later authorized maintenance preserved all existing images/volumes and pruned nothing.
M5 image peak and M7 full-phase growth remain unqualified despite the increased disk capacity.
Final maintenance passed: rootless AppleHV is stable at 14 CPUs, 32 GiB memory, and a 128-GiB disk with vda4/XFS grown; the lease is released.
PostgreSQL 18 now uses a verified persistent replacement on the same name/port and preserved database identity/settings/data; the old tmpfs container is retired, stopped, retained, and must never restart.
Original PostgreSQL 16 and M0 identities/configuration/tools are preserved; all pre-existing images and volumes remain retained, with zero pruning.
The maintenance included an idle M0 PID1 forced stop, unexpected first VM stop, stale API-socket failed restart, and corrected detached launch; PostgreSQL 16/18 recovered ready with zero PANIC/FATAL, so this was not an unconditionally graceful restart.
The final proof records 41,988,149,248 free VM bytes and 237,409,554,432 free host bytes.
The preparation receipt itself authorized no cleanup or resize; the later dated user decision authorizes only the stated maintenance and confirmed-unused pruning scope.
The first M7 UP failed after tracked check/source/37-output and capacity admission passed.
The omitted `podman exec -i` source fix is ready with 81 focused checks and the separate 169-check full wrapper/self-check run; these overlapping counts are not additive.
The diagnosed fake marker/label controls did not retain state; the [ledger](../tasks/todo.md) binds the frozen source and check receipts.
The following source-fix and failed-RUN history precedes the corrected first-UP acceptance above.
The real frozen-marker probe passed root-owned mode-0600 readback, no-clobber, and exact owned-probe absence checks; the [ledger](../tasks/todo.md) binds its receipt.
Fresh source `78a663` prepared successfully, and its first UP verified all three node caps before refusing a kubeconfig kind-prefix mismatch.
The fresh RUN remains `creating`; guarded DOWN refused the same naming check, and the approved exact three-container cleanup passed exact absence/preservation checks.
The two-comparison guard export `6277b1f1` and realistic fixture export `aa76c08c` now compose GREEN: 87 focused checks and one 175-check full wrapper run, with original-guard RED.
Those source-level contract tests preceded the corrected first-UP acceptance above; failed RUN evidence remains preserved without repair.
The frozen manager recipe uses three CPUs, 5 GiB memory, no swap, and one build job; PID enforcement is outside that approved recipe.
The one-off user-exception cleanup passed for exactly the three failed-node containers, with no volume flags or other deletions.
The source and every old RUN file remain unchanged in creating/empty state; volumes, kind network, images, backups, and original services are retained.
The quiet reservation is released; this is neither guarded DOWN nor UP qualification.
The failed first-UP window used a finite private supervisor with a monitored 12-GiB growth envelope, not a quota, and a 7200-second maximum lease without renewal; the cleanup exception does not generalize its guards.
Earlier partial run directories remain preserved and cannot be adopted.
The aligned standalone Podman cap probe proved update to two CPUs / 4 GiB with no effective swap; it does not qualify actual kind topology or cluster resource caps.
The successful preparation binds only its exact source; verified node caps in the failed fresh attempt do not qualify the kind lifecycle, legacy workflow, or AMD64.
No manual proof JSON or lease renewal is allowed.

- [ ] kind (podman provider) bootstrap with pinned CNPG, CRD apply and image load as Task targets; validated first with engine=pgcopydb on 17:17 (can start right after M1).
- [ ] Suite knobs: `E2E_ENGINE`, `E2E_RUNNER_IMAGE`, `E2E_SMALL`; DropIfExists off and filters dropped under native; engine-aware `checkMajors`; a positive `native` label set.
- [ ] Native runs on 16:16, 18:18 and 16:18: live writer load follow, manual and automatic cutover, verification, runner kill mid-copy, mid-stream and mid-drain, target primary kill with CNPG synchronous replication, suspend/resume, deletion cleanup.
- [ ] Hosted GitHub Actions job on kind at small scale.

### M8: Feature widening (each item enables its capability)

The approved [M8 implementation plan](superpowers/plans/2026-11-09-m8-feature-widening.md) targets fresh native format 9 / metadata 7, separate `ClonePlan.copy_tables`, conservative whole-table binary fallback, and exact mandatory large-object comparison with the unchanged array contract plus a proven-empty inventory guard.
These are implementation decisions, without format/runtime qualification or capability activation.

- [ ] Binary COPY with the per-table built-in-type predicate and text fallback.
- [ ] ctid split parts (`--split-tables-larger-than`, `--split-max-parts`) with per-part done markers.
- [ ] Large objects: copy, follow re-sync after drain, compare coverage, data-phase restart cleanup.
- [ ] Re-run the M7 native matrix with operator defaults.

### M9: Durability modes and measured performance

The approved [M9 implementation plan](superpowers/plans/2026-11-09-m9-durability-performance.md) follows M8 with native format 10 / metadata 7 and the settings/status capability extension; no ACK may derive from a volatile frontier, and cadence is selected from later measurements.
Benchmark execution remains on HOLD until the final actual artifact/argv packet is approved; no benchmark run or VM reservation is authorized.
M10 remains open.
Initial-copy planning keeps fork async and native per-table durability enabled; a planned full-clone final flush barrier may match end-state durability without equating crash contracts.

- [ ] `replay-safe` barrier cadence chosen from benchmarks; effective values in help and status; `relaxed` only if the measured gain exists.
- [ ] Benchmark harness: A/A noise runs first, then interleaved randomized pairs in fresh processes; report target-committed rows and transactions per second, catch-up rate, commit-to-apply p50/p95/p99, and COPY MiB/s on narrow and wide tables; compare fork default against `replay-safe` and label `durable` as the stronger contract.
- [ ] `docs/durability.md` crash matrix per mode: process kill, host power loss, target crash, target failover with and without a synchronous standby, source WAL retention.
- [ ] Optional binary pgoutput, gated per stream on a built-in-type allowlist, only if measured.

### M10: Release readiness

The approved [M10 implementation plan](superpowers/plans/2026-11-09-m10-release-readiness.md) uses bounded mutation tests, native Linux ASAN with its nightly still unverified, STE source/license provenance, and exact production OCI artifacts for the final matrix.
These preparation decisions close no release or runtime gate.

- [ ] README index, getting started, durability, operator guide, troubleshooting; docs drift and STE100 checks.
- [ ] Parser fuzzing and a sanitizer or Miri job over `src/pg/ffi.rs`.
- [ ] Independent reviews of durability, recovery and the operator diff; final matrix on the exact image digest.

## M6 default Kubernetes delivery amendment, 2026-11-09

We approved the [default Kubernetes delivery correction](superpowers/plans/2026-11-09-m6-default-kubernetes-delivery.md), superseding only the earlier split-log result-collection prerequisite.
SQL authority stays in the admitted helper Job main process; one checked-exec relay carries the separate operation stdout/stderr streams without repeating SQL or creating authority.
Before exec, require the exact admitted container to be Running; after strict exec success, accept that same incarnation either Running or successfully terminated with exit 0 and signal 0.
Reject replacement, restart, failed or ambiguous state; successful post-exec termination does not replace separate terminal Job verification.
`JobCompletedNative` requires the original Job/container's terminal success and complete empty ordinary combined logs without a Stream query, then final unchanged identity/status checks.
Construct completion authority only after checked relay evidence, terminal main success, and final independent live/private closure validation all pass.
Three disjoint source owners are building this correction; earlier focused transport/helper/reader proofs remain historical inputs and do not qualify the changed delivery path.
Runtime, packaged image/libc resolver, opaque-file/service, TLS, and PVC gates remain open; production native admission remains disabled and no new RBAC or feature gate is authorized.

## M6 explicit connection-file and cap amendment, 2026-11-09

We approved the [explicit-file binding plan](superpowers/plans/2026-11-09-m6-explicit-file-binding.md), preserving earlier approved plans as historical inputs.
Before connecting either role, capture ordinary passfile/TLS inputs into sealed mode-0400 memfd snapshots while preserving raw libpq DSN, explicit-empty, original permission, and optional-absence semantics.
The only stable-absence route is `/usr/share/rpgmigrate/.native-file-absent`, an immutable missing path in the existing image; add no file, mount, or metadata and require exact-image, read-only-rootfs, mount-shadow, and runtime qualification.
The approved internal main source cap is 122880 bytes and complete main argument cap is 126976 bytes including NUL; wire, relay, and prelude limits remain unchanged, and native evidence Job JSON is capped at 1 MiB.
Linux's minimum 131072-byte per-argument boundary does not qualify aggregate argv/environment, stack, Kubernetes/proxy delivery, or opaque Secret length; required runtime tests remain open.
A dedicated native helper's final decorated create-site size check remains future integration; generic `ensureJob` remains unchanged.
Two disjoint owners are building the four-file helper and two-file admission changes; resolver, service, PVC, image, and activation gates remain open.
Numeric-alpha resolver research `c567134f` remains a proposal, without qualification clearance.

## M6 native helper Jobs/lifecycle amendment, 2026-11-09

We approved the [direct-image Jobs/lifecycle plan](superpowers/plans/2026-11-09-m6-native-helper-jobs-lifecycle-direct-image.md) with canonical pure policy exports and one fresh helper Job/operation per bounded 90-second invocation, using E persisted in an earlier reconciliation.
The administrator-configured immutable single-platform pin supplies both expected `ImageRef` and `PlatformImageID`; retain exact Job/Pod spec image and original status.ImageID comparisons without observed self-expectations, normalization, or a receipt service.
The narrow future removal of only the status.Image display equality is approved; it does not weaken immutable spec/imageID binding or grant runtime clearance.
D1 policy exports are frozen with seven tests/86 checks and zero native lint findings; Jobs/lifecycle focused verification is joined, while configuration follows later and no full activation is claimed.
Actual direct-runtime profile, identical-E acknowledgment, PVC, file, and resolver gates remain open with production admission disabled.

## User-approved owned-kind metrics observer policy, 2026-11-09

The user approved the temporary metrics observer policy for independently admitted owned kind E2E runs; do not request repeated approval within this exact scope.
Use a run-scoped ServiceAccount with automount false, one ClusterRole granting only GET on `/metrics`, and one binding to that exact ServiceAccount in the independently verified suite-created namespace.
Permit at most one in-memory 600-second TokenRequest without renewal, with at least 600 seconds left on the original run deadline; keep the token out of argv, environment, files, and diagnostics.
Use only the validated exact owned manager Pod's loopback HTTPS tunnel, with the approved tunnel-local certificate pinning and unchanged Kubernetes API TLS checks.
Clean only the recorded observer objects under exact UID/resourceVersion preconditions and verify absence; preserve replacement objects, namespaces, and suite resources.
This approval grants no production RBAC, Secret access, new namespace creation, lease renewal, benchmark run, or broader runtime qualification.
The [ledger](../tasks/todo.md) binds the approved policy and parameterized template hashes; each fresh render still requires exact source/RUN/namespace/manager/deadline admission.

## User-authorized continuation, 2026-11-09

The user authorized continuing the existing M5/M6/M7 plans and pruning unused Podman images, volumes, and build cache, plus growing `podman-machine-default`.
The approved PostgreSQL 18 persistent-preservation maintenance plan must protect tmpfs data before any original-container stop or VM restart.
The leader selected a 128-GiB disk, preserving 14 CPUs, 32 GiB memory, original service endpoints, and data; user-reported host free space is about 212 GiB.
`vm_pg18_preservation_plan` completed maintenance; the exact failed-node cleanup is now verified and its quiet reservation released, with subsequent M5 image results recorded above.
Pruning is limited to confirmed-unused resources under that plan, with retained evidence and data preserved.
The user also approved new runner dependencies, including Python 3 and ctypes/libpq, under the dated M6 exception so opaque URI Secret authentication stays out of argv.
The separate mechanism plan and required validation remain open; this authorization proves no TLS, PVC, image, kind, or production-admission result.

### Feature CI pushes and failed-node cleanup, 2026-11-09

On 2026-11-09, after approving the second exact failed-kind cleanup, the user authorized future removal of independently verified rpgmigrate-owned failed kind-node containers without repeated cleanup approval.
This standing authorization covers containers only: preserve volumes, networks, images, backups, original PostgreSQL containers, source, and RUN evidence; do not extend it to other resources or repair RUN markers.
The user authorized pushing `rpgmigrate` branch `feat/native-engine` and the operator feature branch carrying native work to `origin` for hosted CI, including native AMD64 checks under M5/M10.
This authorizes feature branches only: do not push or merge to main, create tags/releases, or publish to GHCR.
The user separately authorized one-off removal of exactly the three failed kind-node containers identified for the failed RUN.
That exception covers no volumes, networks, images, backups, original services, RUN-marker edits, or generic guard bypass; final independent verification now accepts its exact three-container cleanup.
The cleanup result is recorded separately from its approval; no push, hosted-CI, image-build, or kind workflow success is claimed.
Operator preflight exposed existing all-ref/tag GitLab mirroring and skipped tests; the earlier hold prevented inferring downstream authorization from the origin-only decision.
On 2026-11-09, the user explicitly approved the existing GitLab mirroring triggered by the operator native feature branch.
We resolved the publication P1 by authorization and keep the existing mirror enabled; this decision authorizes no repository settings changes, main edits/pushes, tags, releases, or GHCR publication.
The CI owner restored its mirror-guard edits to baseline and committed only hosted checks in `ccde83912cdbc9e152f0bc51c47c49954ede8a9d`, parent `c262322909ac03a7101ea3bfac2c4823efa62e95`.
The normal feature push to `origin/test/m4-closing-m6-composed-20261109` is verified; its [mirror run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42633254999) passed.
[Operator CI](https://github.com/ydixken/pgcopydb-operator/actions/runs/42633254706) passed docs and its unit/envtest, cluster-free E2E, and PostgreSQL 18 coverage test job; the run failed only lint on SC2015.
The reviewed mechanical correction `5616d82f03e6a917147e55ed4a3fb436a1cc6d89` was pushed normally to the feature ref after prior tests became terminal, without cancellation.
The new [operator CI](https://github.com/ydixken/pgcopydb-operator/actions/runs/42636601113) is in progress and its authorized [mirror run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42636601234) passed; no new CI pass is claimed.
Engine feature pushes of `34208b6` and `7c04dff` retain their exact hosted-run scopes above; no feature CI result closes a milestone by itself.

## Execution model

The leader decomposes, assigns one owner per file or subtree, and reviews; verification agents never review their own work.
Effort per task: xhigh for WAL, apply, recovery, durability and safety reviews; high for bounded implementation and tests; medium for docs and CI; each brief states the reason.
Each milestone gets a detailed implementation plan (superpowers:writing-plans) before work starts.
Parallelize the build phase as far as file ownership allows: independent owners work at the same time, each in its own git worktree off `feat/native-engine` (user-approved 2026-11-08, overriding the earlier no-worktree rule), and the leader integrates.
Run the independent review (at the effort its risk calls for) and the Go test suites once per milestone, at its close, not after each task; workers run only the focused tests for their own change while building.
Operator work goes on its own branch and PR, and existing operator edits (`tasks/todo.md`, `.agents/`) stay untouched.
The verified VM allocation is 14 CPUs, 32 GiB memory, and a 128-GiB disk; the final maintenance proof records preservation and released-lease checks.
The original 21:35 UTC startup failure predates the CPU/memory resize.
Maintenance and failed-node quiet reservations are released; preserve host Cargo headroom and the approved resource budgets for subsequent M6/M7 execution.
Initial uncapped kind creation requires database testing and host Cargo builds to be idle until all three node caps are verified.
See the [capacity and preservation record](../tasks/todo.md#capacity-and-parallel-execution-user-update-2026-11-08) for budgets and the restart prerequisites.
Commits remain small, conventional, and lint-clean; feature-branch CI pushes have the dated user authorization above, while main pushes/merges, tags, releases, and GHCR publication remain unauthorized.

> [!important]
> On 2026-11-08, the user approved exactly two local WIP checkpoint commits: `e544f2d` on `feat/native-engine` and `5e8cbcb` on the isolated operator `feat/native-contract-v1` branch, as an exception to the lint-clean-per-commit rule.
> Those checkpoints predate the qualified M2 staged pair receipts and the normal M3 source commit `7b6037f`, whose primary strict gates resolve the scaffold lint hold.
> The two-commit exception is consumed; the normal lint-clean rule has resumed, and M3 worktrees remain outside their scope.

## Verification

- Engine: `task lint test`, `task test-db` for cargo live tests, and per-pair E2E targets (`task e2e PG_SOURCE=16 PG_TARGET=18` and a matrix target) including crash and drain cases.
- Operator: unit and envtest suites; `task e2e:kind:up` then the native E2E target for each pair; success means Complete with CutoverCompleted, a native drain proof and a data verification match.
- CI runs the same targets on hosted runners; the final release matrix runs on the exact image digest.
