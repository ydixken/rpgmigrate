# rpgmigrate: assessment and milestone plan

## Context

pgcopydb-operator must run migrations on a native Rust engine, `rpgmigrate`, instead of the forked pgcopydb.
Almost every data-loss defect that reached the operator came from pgcopydb's CDC spool pipeline (op#356, #357, #358, #359, #376, #378).
The engine therefore makes crash/restart correctness and fast WAL apply its core properties, with documented durability modes.
Goal: working operator-driven migrations (copy, replication, recovery, cutover, verification) on PG16→16, PG18→18 and PG16→18, with reproducible E2E tests, Taskfile commands, Docker packaging, GitHub Actions and documentation.

Inputs: the code of all three repos, operator and fork issues, pgcopydb `origin/v0.18-fixes`, the trusted `AGENTS.md`, the [approved design](superpowers/specs/2026-11-06-rpgmigrate-design.md), `tasks/lessons.md`, `docs/operator-compatibility.md`, and the uncommitted work (51 untracked files plus `stash@{0}`).
Not used: `tasks/todo.md` and the dated plans.
A 9-reader audit with adversarial cross-checks and a 5-lens plan review (correctness, scope, operator/E2E, performance, builder) produced the findings below.

## Assessment

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

### M0: Preserve and reset (rpgmigrate)

- [x] Guarded preservation: `stash@{0}` plus the 51 untracked files are archive commit `8fceb45` on `wip/native-copy-snapshot` (base `bd0925d`, never merged); `archive/stash-0` pins the stash; a tar of the untracked files sits outside the repo.
- [x] `feat/native-engine` branches from `8fceb45`; this roadmap is committed there.
- [x] Remove the superseded plans, the old task ledger, the superseded anchor spec and the previous team's guides and research notes; point `AGENTS.md` and `README.md` at this roadmap; record this session's corrections in `tasks/lessons.md`.
- [ ] Record whether `feat/native-engine` builds as archived (`task check`), before any other change.
- [ ] Create `tasks/todo.md` from these milestones; add the dated design revision with the amendments above (and fix its links to removed files).
- [ ] Remove the M0 drop list; apply the libpq decision; plain CI for fmt, clippy and unit tests.

### M1: Contract freeze (both repos)

- [ ] Commands, option names, env (`PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGPASSFILE`, `PGCOPYDB_LOG_JSON`, `PGCOPYDB_APPLY_GROUP_TXNS`), exit codes and error classes for the native profile.
- [ ] Protocol v1 amendments: version/capability DTO, `native-report:` stdout transport, work identity env, caller-supplied endpos ack, drain proof with `certified_lsn`, cleanup inventory; compare outside ParseReport.
- [ ] application_name scheme, owned schema name, workdir and `control/` layout, native runner image selection and tool layout.
- [ ] Shared golden fixtures in both repos with tests on both sides.

### M2: Clone slice (engine)

- [ ] Transport: pipeline mode, error classes with SQLSTATE/detail, per-operation deadlines and stall timeouts, buffered COPY relay, notices surfaced.
- [ ] Checkpoint v2: group frontier, `certified_lsn`, per-table done markers and copy generation, per-run fence, slimmed hot path.
- [ ] Copy order of decision 5 with `--table-jobs`, FREEZE, sequences, post-data, ANALYZE; PG16/18 gates and version-specific SQLSTATEs.
- [ ] Plain clone `--restart` and `--resume --not-consistent`; `list progress --json`; `compare schema`; `compare data --json` with pinned GUCs and the operator's shapes.
- [ ] Engine E2E harness on containers (podman or docker) with Task targets per version pair; clone matrix and kill/resume cases; content checked by independent SQL fingerprints.

### M3: Follow (engine)

- [ ] pgoutput v1 decoder (B, C, R, Y, I, U, D, T, O; others fatal) with relation generations; fixtures recorded from PG16 and PG18; regression cases for op#356-359 and #376; a fuzz target.
- [ ] Receive thread with decision 3 backpressure and status rules; slot health checks; gap refusal on connect.
- [ ] Applier: RI FULL keyless (ctid single-row), RI INDEX, unchanged TOAST, multi-relation TRUNCATE (ONLY, never CASCADE), identity and generated columns, non-built-in types as text; pipeline groups; strict dedupe.
- [ ] Data-phase restart of decision 8; resume after anchor.
- [ ] Fault tests: kill in every phase and around COMMIT, target restart, walsender termination, apply blocked longer than `wal_sender_timeout`, idle source with filtered WAL, round-trip counter test, smoke catch-up benchmark.

### M4: Cutover, drain proof, cleanup (engine)

- [ ] `stream sentinel set endpos <lsn>|--current` and `get --endpos|--replay-lsn|--json` per decision 6.
- [ ] Drain certification, final sequence sync, exit 0 only with proof.
- [ ] `stream cleanup`: owned slot and publication only, idempotent, workdir kept for later compares.
- [ ] Engine E2E: full follow migration under pgbench load (including `synchronous_commit=off` writers), endpos, drain, compare match and cleanup, plus a crash in each phase, on 16→16, 18→18 and 16→18.

### M5: Packaging and engine CI (starts with M2)

- [ ] Runner image: rpgmigrate, sh, psql 18, PGDG client tools 16 and 18 under `/usr/lib/postgresql/<major>/bin`, GNU coreutils and sed, uid 65532, read-only rootfs, multi-arch, smoke test.
- [ ] GitHub Actions: lint, test, cargo-deny (licenses, advisories), docs lint, engine E2E matrix on hosted runners, image build, ghcr push on tags only.

### M6: Operator native adapter (operator, on a branch from `feat/rpgmigrate-native-operator` after a fetch)

- [ ] Engine-aware argv0 and scripts (worker, verify, cleanup, compare, catalog, endpos exec); lift `UnsupportedEngine` for the qualified profile; `--rpgmigrate-runner-image` flag and chart value.
- [ ] Preflight: `version --json` handshake into `validateNativeConfig`; native pg_restore gate by target major; skip the origin-grant probe; PG16 target allowed for native.
- [ ] Phase from `native-report:` lines; native drain verification bound to status endpos plus a psql read of the owned control row; cleanup requires `complete=true`; plain-clone completion fails closed on a missing report.
- [ ] ORed application_name patterns for the retry-gate census, sampler and zombie reaper; native error-class prefixes in the log matchers.
- [ ] Unit and envtest coverage; operator docs for the native engine.

### M7: Operator-driven E2E

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
Commits are small, conventional and lint-clean; nothing is pushed or merged without the user's go-ahead.

## Verification

- Engine: `task lint test`, `task test-db` for cargo live tests, and per-pair E2E targets (`task e2e PG_SOURCE=16 PG_TARGET=18` and a matrix target) including crash and drain cases.
- Operator: unit and envtest suites; `task e2e:kind:up` then the native E2E target for each pair; success means Complete with CutoverCompleted, a native drain proof and a data verification match.
- CI runs the same targets on hosted runners; the final release matrix runs on the exact image digest.
