# Native engine task ledger

The [roadmap](../docs/roadmap.md) defines the milestone scope and its 2026-11-08 amendments override conflicting older design text.
The [M0 implementation plan](../docs/superpowers/plans/2026-11-08-m0-preserve-reset.md) defines worker ownership and acceptance.
Keep implementation evidence separate from parser compatibility and operator runtime qualification.

## M0: Preserve and reset (rpgmigrate)

- [x] Preserve stash and untracked work as archive commit `8fceb45` on `wip/native-copy-snapshot`, retain `archive/stash-0`, and preserve the external tar.
- [x] Branch `feat/native-engine` from the archive and commit the approved roadmap.
- [x] Remove superseded planning inputs and point repository instructions at the roadmap.
- [x] Record the independent archived baseline before implementation.
- [x] Recreate this ledger from M0 through M10 and write the detailed M0 plan.
- [x] Leader reviews the M0 plan and assigns disjoint transport, tooling, and documentation owners.
- [x] Remove the private libpq capability check and native module, keep live passfile/service authentication coverage, and move shared test helpers.
- [x] Remove the retired snapshot journal live module and its Rust registration without changing retained runtime state or CDC code.
- [x] Remove the remaining retired journal Task selectors as part of the tooling integration.
- [x] Remove the storage pilot files, manifest entries, experiment-only dependency, and storage count gates.
- [x] Replace the controlled libpq build and custom image pipeline with stock libpq selection and portable Task checks.
- [x] Preserve the isolated database harness and useful portable positive and negative tests, with JournalSerial removed.
- [x] Install plain GitHub Actions checks for formatting, clippy, and tests; defer the replacement runner image to M5.
- [x] Reconcile the approved design with its dated 2026-11-08 revision and fix stale links.
- [x] Update README, operator compatibility, testing instructions, and notices for the implemented scaffold.
- [ ] Integrate reviewed worker outputs and run `task check lint test` on the integrated tree.
- [ ] Run stock-libpq live transport/authentication checks for PostgreSQL 16 and 18 and the retained harness self-check.
- [ ] Complete one independent review at milestone close and resolve findings through their file owners.
- [ ] Record the closing results and limitations below; commit only when the leader requests it after checks pass.

### M0 baseline evidence

The independent baseline worker observed a clean `feat/native-engine` tree at `1983a6a4e2aec62c8096c008316442c310be88d0`.
Archived `task check` exited 201 in `native-check`: `native libpq: native build recipe changed; run python3 tools/build-libpq.py --rebuild`.
Rust compilation did not run because the native prerequisite failed.
We retain this result and do not rebuild the obsolete controlled dependency.

### M0 assignments

The leader approved the plan and dispatched `m0_transport`, `m0_tooling`, and `m0_docs` with the disjoint file sets in the implementation plan.
Workers use the primary checkout for M0 and coordinate Cargo checks to avoid conflicting builds.
`m0_plan` owns this ledger and the implementation plan; integration and independent review remain separate assignments.
The leader split `tools/test-db.py` and `tests/support/**` from tooling to `m0_harness`.
The leader extended transport ownership to the synthetic endpoint-test wording in `src/lib.rs`.
`m0_integrate` owns disposable verification setup and the integrated closing gates.
`m0_review` owns the single independent closing review at xhigh effort.

### M0 focused evidence

`m0_transport` completed the source changes and observed 19 FFI tests plus one endpoint test passing with stock libpq 18.6.
The connection regression demonstrated RED before removing private capability admission.
The worker also reported passing formatting and diff checks.
These focused results do not execute the live PostgreSQL 16/18 authentication matrix.

`m0_harness` removed JournalSerial mode, inventory, fixtures, receipt checks, and unused authority selectors.
It changed `tools/test-db.py`, `tests/support/db_harness.py`, `tests/support/authority_fault.py`, and `tests/support/test_harness.py`, and deleted the three journal-only support files.
All 97 retained portable harness checks passed with Python warnings treated as errors.
The new removed-mode refusal regressions demonstrated RED then GREEN, and the worker reported a passing diff check.

`m0_tooling` completed the tooling changes and reported 134 portable tests passing: the same 97 harness tests, 31 Task tests, and six unit-runner tests.
The harness count is included in 134 and must not be added again.
Actionlint, yamllint, and locked Cargo metadata checks passed.
The dependency change removes `redb` only; retained locked dependency versions remain unchanged.

`m0_docs` completed the documentation changes and checked 20 local links and nine documented Task targets successfully.

`m0_integrate` completed disposable container setup with PostgreSQL 16.15 and 18.6 and stock libpq 18.6.
The live transport profile passed five tests on PostgreSQL 16.15 and five on PostgreSQL 18.6, linked to stock libpq 18.6.
The retained harness self-check passed, including the corrected PostgreSQL 16 test minimum.
All M0 implementation handoffs are ready.
The independent M0 review completed and identified strict lint as its remaining blocker.
Retained scaffold paths exposed by removal of the journal experiment trigger dead-code errors under the unchanged strict lint gate.
The integrated `task test` attempt passed 402 Rust tests with 66 live tests filtered, then passed 24 CLI cases.
Its first portable run failed three Linux copy-controller socket-closure controls because unread ReadyForQuery bytes remained after intentional COMMIT-response loss and caused socket reset on Linux.
The test-only repair demonstrated RED then GREEN for the three controls on Linux and macOS.
The final Linux `python3 -W error tools/ci/unit.py` passed all 134 portable checks; its log is `/tmp/rpgmigrate-m0-linux-harness-repair.log`.
After the narrow unused-import-only cleanup, `task fmt-check check` passed.
These constituent results do not establish a green strict lint gate; M0 remains open solely on structural clippy errors in the retained scaffold.
The independent reviewer completed the fixture-repair follow-up without a finding; structural clippy is the sole M0 hold.
No operator migration workflow is qualified by these component results.

### Sequencing decision, user-approved 2026-11-08

The user approved: "Keep M0 open; proceed with approved milestones".
We may plan and execute the approved later milestones while the retained scaffold lint blocker keeps M0 open.
Preserve strict lint; do not suppress its errors, mark M0 complete, or make commits while the required gate is red.
This changes milestone sequencing, not the runtime contract or the required closing checks.

### M0 closing review

- [ ] Record focused worker checks and their changed-file lists.
- [ ] Record the integrated broad-gate command, exit status, and nonempty test evidence.
- [x] Record selected stock libpq and exact PostgreSQL 16/18 live test versions and results.
- [x] Record the independent review outcome and any remaining findings.
- [ ] Record Go checks as not applicable if M0 remains engine-only, with no operator changes.
- [ ] Record the final reviewed tree before authorizing any commit.

## M1: Contract freeze (both repos)

The leader-approved [M1 implementation plan](../docs/superpowers/plans/2026-11-08-m1-native-contract.md) defines task ownership and reviewed safety invariants.
The leader accepted the canonical identity/layout constants and same-row drain-witness split after independent xhigh analysis.
M1 freezes DTO/evidence contracts only; M2/M4 implement target metadata and runtime sequencing, and M6 implements controller wiring.

- [x] Prepare the M1 implementation plan and return its interfaces for leader review before source work.
- [x] Record leader approval and dispatch disjoint engine, operator-validator, and fixture/documentation owners.
- [x] Commands, option names, env (`PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGPASSFILE`, `PGCOPYDB_LOG_JSON`, `PGCOPYDB_APPLY_GROUP_TXNS`), exit codes and error classes for the native profile.
- [x] Protocol v1 amendments: version/capability DTO, `native-report:` stdout transport, work identity env, caller-supplied endpos ack, drain proof with `certified_lsn`, cleanup inventory; compare outside ParseReport.
- [x] application_name scheme, owned schema name, workdir and `control/` layout, native runner image selection and tool layout.
- [x] Shared golden fixtures in both repos with tests on both sides.

### M1 assignments and implementation boundaries

The leader approved the final plan and dispatched `m1_engine`, `m1_operator`, and `m1_fixtures` with its disjoint file ownership sets.
`m1_engine` validates supplied identity configuration all-or-none before I/O; operational commands with supplied identity explicitly refuse before mutation until M2 can bind it to persisted native state.
Help and version remain independent of operator identity configuration.
Local Rust tests cover meaningful DTO, discovery, and argv behavior, while Go remains the authoritative operational-report validator; do not build a duplicate test-only report parser or claim runtime binding from synthetic evidence.
The token's plus-one transition is a durable producer CAS invariant.
A parser may observe skipped intermediate report tokens and accepts a current token when it exactly matches trusted evidence.
Escalate any gap in the shared parser-oracle contract through the leader before changing interfaces.

### M1 focused evidence

- [x] Receive the engine DTO/CLI handoff with focused checks.
- [x] Receive the operator validator handoff with focused Parse checks.
- [x] Receive the final shared-fixture and documentation handoff.
- [ ] Run the integrated M1 closing gates and independent review.

`m1_engine` completed changes in `src/protocol.rs`, `src/cli.rs`, `src/config.rs`, `src/contracts.rs`, and `src/lib.rs`.
Its focused checks passed 17 CLI tests, three protocol tests, and one diagnostics test.
Checks through the actual built binary confirmed identity-independent version output and explicit identity refusal without workdir mutation.
These results do not establish persisted identity binding or migration runtime support.

`m1_operator` completed its two assigned native-validator files.
Its focused Parse checks passed 20 top-level tests and 159 tests including subtests, with output retained in `/tmp/rpgmigrate-m1-operator-parser.json`.
The 159 includes the 20 top-level tests; do not add the counts together.

`m1_fixtures` completed 186 shared cases across 132 total files, of which 131 payload/contract/case files are manifest-listed; the two fixture trees compare identically.
All 146 local Python checks passed.
All M1 implementation owners handed off their work and froze source for integration.
`m0_integrate` is authorized to run the once-per-milestone engine check/lint/test and operator lint, Go, envtest, and PostgreSQL 16/18 helper gates.
`m0_review` performed the independent xhigh closing review.
The final engine closing checks passed 410 Rust tests, 24 CLI cases, and 146 portable checks.
The new M1 clippy finding was repaired and the three protocol tests passed; final strict lint reports only retained unused/dead-code paths, with 243 library and 27 library-test errors.
The operator passed its Task lint stages and final Go lint with zero issues using Go 1.27.0.
Its PostgreSQL 16 full unit/envtest gate passed all 15 packages, and the final native package run passed 22 top-level tests and 291 cases including subtests.
The PostgreSQL 16 helper gate passed 508 of 510 tests including subtests, with two expected skips and 138 top-level tests.
The PostgreSQL 18 helper gate passed all 158 tests including subtests and 17 top-level tests, including generated virtual columns.
Top-level counts are included in the corresponding subtest totals and package runs may overlap; do not aggregate them as independent cases.
The independent review reported no remaining findings, and both repositories passed `git diff --check`.
The contract build, tests, and review are complete, but M0 and M1 remain open on the inherited strict lint gate under the user-approved sequencing decision.
No commits were made, and these results qualify no native migration workflow.

## M2: Clone slice (engine)

The [M2 implementation plan](../docs/superpowers/plans/2026-11-08-m2-clone-slice.md) records the leader's plain-clone scope decisions, seven-owner map, shared API bootstrap, and closing acceptance gates.
The leader approved the M2 draft scope and seven-owner decomposition; parallel source work waits for the shared-interface handoff.

- [x] Prepare the bounded M2 implementation plan and return its proposed interfaces and ownership for leader review.
- [x] Record leader approval of the draft scope, seven-owner decomposition, and explicit recovery boundaries.
- [x] Record leader approval, concrete shared signatures, and implementation dispatch.
- [x] Demonstrate a focused quiescent PostgreSQL 16 to 16 plain clone with durable completion and independent schema/data/sequence verification.
- [x] Resolve all independent closing-review repairs for target ownership, marker readback, generated output, and transaction durability proof.
- [x] Pass the final Prepared10 and Clone7 component profiles on PostgreSQL 16 and 18.
- [ ] Transport: pipeline mode, error classes with SQLSTATE/detail, per-operation deadlines and stall timeouts, buffered COPY relay, notices surfaced.
- [ ] Checkpoint v2: group frontier, `certified_lsn`, per-table done markers and copy generation, per-run fence, slimmed hot path.
- [ ] Copy order of decision 5 with `--table-jobs`, FREEZE, sequences, post-data, ANALYZE; PG16/18 gates and version-specific SQLSTATEs.
- [ ] Plain clone `--restart` and `--resume --not-consistent`; `list progress --json`; `compare schema`; `compare data --json` with pinned GUCs and the operator's shapes.
- [ ] Engine E2E harness on containers (podman or docker) with Task targets per version pair; clone matrix and kill/resume cases; content checked by independent SQL fingerprints.

### M2 assignments

The leader dispatched `m2_shared`, `m2_transport`, `m2_checkpoint`, `m2_catalog_schema`, `m2_snapshot_copy`, and `m2_compare` read-only pending `m2_shared`'s final signature freeze.
The seventh worker, `m2_harness_docs`, owns the disposable container E2E harness, clone fixtures, Task targets, and affected documentation.
`m2_shared`, `m2_transport`, `m2_checkpoint`, and `m2_snapshot_copy` use xhigh reasoning for coordinator recovery, I/O state, durable authority, and snapshot/transaction concurrency.
`m2_catalog_schema`, `m2_compare`, and `m2_harness_docs` use high reasoning for bounded catalogs, independent comparison, and deterministic qualification.
The leader released all seven owners after the concrete shared signature freeze landed in the M2 plan and `src/contracts.rs`.
`m2_shared` alone owns root module registrations and the new `src/clone.rs` driver, with the exact disjoint file sets recorded in the M2 plan.
The leader approved an explicit `&mut CloneAuthority` argument to `copy::run_tables`.
The shared coordinator retains its fence for the complete command, including schema and post-data; worker permits authorize only their exact data transaction and each worker creates its own local `!Send` connections.
No worker implementation starts before the concrete shared handoff, and no milestone is marked complete or committed while the inherited strict lint gate is red.

### M2 harness preparation

The leader assigned `m2_harness_docs` ownership of roadmap status maintenance as well as this ledger.
We keep the roadmap's completed subdeliverables synchronized with these observed results while leaving milestone closing gates open.
The first combined green integration point must resolve the inherited M0/M1 strict lint hold before any commit; this does not imply that earlier milestones had green standalone commits.
The portable Python gate passed 154 checks after the first M2 harness and live-test inventory additions.
Later clone/catalog inventory changes still require a fresh focused run, and the engine E2E matrix has not run.

## M3: Follow (engine)

- [ ] pgoutput v1 decoder (B, C, R, Y, I, U, D, T, O; others fatal) with relation generations; fixtures recorded from PG16 and PG18; regression cases for op#356-359 and #376; a fuzz target.
- [ ] Receive thread with decision 3 backpressure and status rules; slot health checks; gap refusal on connect.
- [ ] Applier: RI FULL keyless (ctid single-row), RI INDEX, unchanged TOAST, multi-relation TRUNCATE (ONLY, never CASCADE), identity and generated columns, non-built-in types as text; pipeline groups; strict dedupe.
- [ ] Data-phase restart of decision 8; resume after anchor.
- [ ] Fault tests: kill in every phase and around COMMIT, target restart, walsender termination, apply blocked longer than `wal_sender_timeout`, idle source with filtered WAL, round-trip counter test, smoke catch-up benchmark.

## M4: Cutover, drain proof, cleanup (engine)

- [ ] `stream sentinel set endpos <lsn>|--current` and `get --endpos|--replay-lsn|--json` per decision 6.
- [ ] Drain certification, final sequence sync, exit 0 only with proof.
- [ ] `stream cleanup`: owned slot and publication only, idempotent, workdir kept for later compares.
- [ ] Engine E2E: full follow migration under pgbench load (including `synchronous_commit=off` writers), endpos, drain, compare match and cleanup, plus a crash in each phase, on 16→16, 18→18 and 16→18.

## M5: Packaging and engine CI (starts with M2)

- [ ] Runner image: rpgmigrate, sh, psql 18, PGDG client tools 16 and 18 under `/usr/lib/postgresql/<major>/bin`, GNU coreutils and sed, uid 65532, read-only rootfs, multi-arch, smoke test.
- [ ] GitHub Actions: lint, test, cargo-deny (licenses, advisories), docs lint, engine E2E matrix on hosted runners, image build, ghcr push on tags only.

## M6: Operator native adapter (operator, on a branch from `feat/rpgmigrate-native-operator` after a fetch)

- [ ] Engine-aware argv0 and scripts (worker, verify, cleanup, compare, catalog, endpos exec); lift `UnsupportedEngine` for the qualified profile; `--rpgmigrate-runner-image` flag and chart value.
- [ ] Preflight: `version --json` handshake into `validateNativeConfig`; native pg_restore gate by target major; skip the origin-grant probe; PG16 target allowed for native.
- [ ] Phase from `native-report:` lines; native drain verification bound to status endpos plus a psql read of the owned control row; cleanup requires `complete=true`; plain-clone completion fails closed on a missing report.
- [ ] ORed application_name patterns for the retry-gate census, sampler and zombie reaper; native error-class prefixes in the log matchers.
- [ ] Unit and envtest coverage; operator docs for the native engine.

## M7: Operator-driven E2E

- [ ] kind (podman provider) bootstrap with pinned CNPG, CRD apply and image load as Task targets; validated first with engine=pgcopydb on 17:17 (can start right after M1).
- [ ] Suite knobs: `E2E_ENGINE`, `E2E_RUNNER_IMAGE`, `E2E_SMALL`; DropIfExists off and filters dropped under native; engine-aware `checkMajors`; a positive `native` label set.
- [ ] Native runs on 16:16, 18:18 and 16:18: live writer load follow, manual and automatic cutover, verification, runner kill mid-copy, mid-stream and mid-drain, target primary kill with CNPG synchronous replication, suspend/resume, deletion cleanup.
- [ ] Hosted GitHub Actions job on kind at small scale.

## M8: Feature widening (each item enables its capability)

- [ ] Binary COPY with the per-table built-in-type predicate and text fallback.
- [ ] ctid split parts (`--split-tables-larger-than`, `--split-max-parts`) with per-part done markers.
- [ ] Large objects: copy, follow re-sync after drain, compare coverage, data-phase restart cleanup.
- [ ] Re-run the M7 native matrix with operator defaults.

## M9: Durability modes and measured performance

- [ ] `replay-safe` barrier cadence chosen from benchmarks; effective values in help and status; `relaxed` only if the measured gain exists.
- [ ] Benchmark harness: A/A noise runs first, then interleaved randomized pairs in fresh processes; report target-committed rows and transactions per second, catch-up rate, commit-to-apply p50/p95/p99, and COPY MiB/s on narrow and wide tables; compare fork default against `replay-safe` and label `durable` as the stronger contract.
- [ ] `docs/durability.md` crash matrix per mode: process kill, host power loss, target crash, target failover with and without a synchronous standby, source WAL retention.
- [ ] Optional binary pgoutput, gated per stream on a built-in-type allowlist, only if measured.

## M10: Release readiness

- [ ] README index, getting started, durability, operator guide, troubleshooting; docs drift and STE100 checks.
- [ ] Parser fuzzing and a sanitizer or Miri job over `src/pg/ffi.rs`.
- [ ] Independent reviews of durability, recovery and the operator diff; final matrix on the exact image digest.

## Executable command scope

The M0 reset does not implement additional operator command paths.
Retain explicit refusals for unsupported work, keep the parser baseline, and qualify new runtime routes only in their owning milestones.
The [operator compatibility contract](../docs/operator-compatibility.md) defines the consumed interface; later milestone evidence determines support.

### M2 verified component handoffs

The catalog owner's exact two new admission cases and stock utility pre-data smoke passed on PostgreSQL 16.15 and 18.6.
The comparison owner's exact canonical SQL case passed on PostgreSQL 16.15 with libpq 18.6; its retained result is `/work/m2-compare/result.log` in the isolated development container.
The snapshot/COPY owner's six clone cases passed on PostgreSQL 16.15 and 18.6, including RED then GREEN for replaced-snapshot refusal.
The PostgreSQL 18 test binary passed six cases, but its wrapper refused because the required inventory grew to seven during that lease; the expanded exact receipt remains pending.
The updated portable gate `python3 -W error tools/ci/unit.py` passed 156 tests.
The development-only E2E image built as `aa4c2ab94e0ffd4c57319c221f61a099b40d06cd2184d18f8be0b275531994a7` with stock PGDG clients 16.15 and 18.6 and libpq 18.6.
These component checks do not establish the container recovery matrix or operator qualification.

### M2 first plain-clone workflow

`m0_integrate` observed the first focused quiescent PostgreSQL 16 to 16 clone pass with actual server and client tools 16.15.
The executable exited zero with durable completed target proof; independent SQL fingerprints, schema columns, constraints, indexes, triggers, and sequence state matched.
Runtime schema/data comparison covered all eight physical tables, and completed progress contained measured table/index/byte counters.
The owned fixture included identity/generated columns, 2048 wide rows, an empty table, inheritance, partition leaves, and a foreign key.
The retained receipt is `/work/m2-integration/plain16-result.json` in the isolated development container.
This establishes one focused happy-path workflow only; the three-pair matrix, concurrent-write/recovery E2E, closing review, and operator qualification remain open.
No performance result follows from the fixture's elapsed time.

The frozen seven-case Clone profile passed on PostgreSQL 16.15 and 18.6 after coordinating its exact inventory and selectors.
It includes the six snapshot/COPY cases plus the target marker authority case; the earlier six-case wrapper refusal remains preparation history rather than runtime failure.
The E2E partial-copy restart gate now holds a harness-owned target SHARE lock, observes the exact worker blocked at `TRUNCATE ONLY`, and requires other table markers before killing the owned engine child.
Source catalog AccessShare locks prevent the originally proposed source AccessExclusive gate; the replacement changes only test synchronization and makes no mid-stream COPY claim.

The M5 documentation lint command `npm exec --yes --package=markdownlint-cli2@0.24.0 -- markdownlint-cli2 '**/*.md' '#target/**' '#node_modules/**'` passed with 15 Markdown files and zero issues after narrow format-only repairs.
Literal trailing-space prefixes remain exact quoted strings inside code spans; no lint rule was disabled.

The bounded restart pre-data schema-drift guard was an earlier matrix hold and subsequently passed its focused gates before generation publication and owned-data TRUNCATE.
Its existing E2E restart case now includes a same-OID/column default change, explicit drift refusal, and unchanged target data/generation/markers before restoring the source default and testing a valid fresh-generation restart.
M5's seven workflow controls, actionlint, yamllint, and dependency license/advisory checks passed.
The first local native ARM64 production image build preceded the notices rebuild and later read-only packaging receipt; hosted AMD64 verification, publishing, and M5 closure remain unqualified.

The permanent Schema profile requires exactly `schema::tests::live_pre_data_guard_refuses_schema_drift_without_target_mutation`, with two owned empty databases and dedicated schema-admin bindings.
`task test-schema` selects that case; the retained Engine profile explicitly excludes only its separately qualified guard name, and Clone remains frozen at seven cases.
The portable suite now registers the frozen seven M5 workflow controls and 12 image smoke controls exactly once.
`python3 -W error tools/ci/unit.py` passed 177 tests after those registrations, the schema inventory/absence controls, and image Task argument tests.
The new image Tasks preserve explicit runtime/image/Python overrides and quoted argument boundaries.
The local native ARM64 read-only smoke passed with UID/GID 65532, libpq 18.6, clients 16.15/18.6, and positive owned mount writes; its receipt is `/tmp/rpgmigrate-m5-image/arm64-smoke.json`.
That image records its actual build input manifest, but later M2 source edits prevent treating it as final-source qualification.

The schema owner's exact pre-data guard case passed on PostgreSQL 16 to 16, 18 to 18, and 16 to 18, with one passed case and zero ignored cases per receipt.
`task test-schema` permanently registers that helper and its owned source/target-admin environment; the frozen Clone profile stays at seven cases.
The existing E2E restart case's default-drift input and no-target-mutation assertions are frozen for the full matrix.
The final pre-matrix portable gate passed 177 tests; the harness additionally requires exact created container IDs before cleanup, and readiness uses TCP rather than the server image's temporary initialization socket.

### M2 producer conformance

`m0_integrate` verified fresh operator-bound PostgreSQL 16 clone and progress payloads with the actual Go `ParseReport` against caller UUID/run/PVC/generation bindings written before the engine launched.
Both reports used the original raw JSON bytes, completed phase, and all three positively measured completion counters; wrong work and an unknown status member were rejected.
Independent source/target fingerprints covered eight physical tables, and the target control row reported complete generation one with eight markers.
The retained receipt is `/work/m2-integration/operator16-result.json`.
The authoritative Linux ARM64 probe is `/workspace/target/e2e/native-report-probe`, SHA-256 `da115fdcb776f9d0e52fa3d1d2b0b7cbcbd3fab5de471fda76aadc8e4eb8614f`.
The matrix harness now configures operator mode before launch; standalone mode accepts only unframed standalone summaries and never fabricates work identity.
Its optional selected probe fails closed, while default engine-only runs use the narrow M2 contract and explicitly label that evidence.
The updated portable gate passed 179 tests; the complete three-pair matrix and operator controller qualification remain open.

The final documentation lint rerun passed all 17 discovered Markdown files with zero issues after the operational report-mode clarification.

### M2 closing review repairs

Independent review found that COPY's locked relation name was not rebound to the saved target OID before mutation, allowing a foreign replacement name after an owned-table rename.
The checkpoint owner repaired that authority check, and independent review resolved the saved-OID binding finding.
The reviewer also found that unknown-COMMIT resolution read the marker before the synchronous barrier and could retain stale absence; the owner added the post-barrier reread and independent review resolved that finding.
The integration owner will remove the newly added pipeline dead-code expectation without suppressing the retained staged lint failures assigned to M3.
The earlier review-repair receipts preceded the first full matrix attempt and do not independently qualify the final full workflow.

Two confirmed review findings entered the closing repair pass: generated expressions can recompute different stored output, and discarded synchronous-replication cancellation warnings can falsely establish durable completion.
The snapshot/COPY owner added source-snapshot full-row parity before marker COMMIT, including generated output and the 16 MiB full qualification-row cap, within existing live cases.
Transport/checkpoint owners repaired private warning classification and positive durability proof; independent review resolved that changed portion, while final qualification gates remain open.
The harness's reviewed physical row-count baseline now fails loss of nonempty wide-row, duplicate-parent, or separately counted child coverage without adding matrix cases or profiles.
The row-count coverage control demonstrated RED then GREEN, and the updated portable gate passed 180 tests.

Four actual CLI preflight controls rejected an explicitly empty probe argument, empty probe environment variable, missing probe, and nonexecutable file before any container was launched.
These selected-probe failures never fall back to local validation and do not constitute a matrix run.

The generated-output repair passed two exact selected live cases on PostgreSQL 16.15 and 18.6, covering table-OID rejection and generated-only oversize rollback with no marker.
The owner demonstrated actual PostgreSQL 16 RED on the pre-fix code, which falsely completed the mismatched generated values; copy unit tests (three) and canonical helper tests (seven) also passed.
This subset does not close the updated seven-case profile, locale qualification, or the full matrix.
Independent review subsequently resolved generated-output parity and then the synchronous-replication warning/factory/pipeline changes, leaving no code-review must-fix open.

M6 command helpers passed focused controller/legacy argv checks across the assigned four helper files on the separate operator branch.
The framing worker reported three top-level framing tests and 65 subcases passing with negative binding controls; the counts are not aggregated.
Production `UnsupportedEngine` behavior remains unchanged, so helper evidence does not activate or qualify the native operator lifecycle.
The M3 draft keeps unknown WAL headroom as an operator obligation and known unsafe SQL settings as fail-closed checks; post-data recovery options remain under review with no implementation claim.
The transport owner narrowed pipelines to noncommitting windows and routed transaction completion through an observed-commit path with pinned warning visibility and unknown outcome on any warning.
Those private proof semantics add no report fields or ignored test names and still require the closing warning/durability checks.

### M2 final-source verification in progress

The independent closing reviewer cleared all findings, including the notice/factory/pipeline changed portion against four exact handoff hashes.
The accepted proof path keeps private warning text out of diagnostics, preserves safe result-code classification, forces warning visibility, and grants completion only from positive transaction proof.

- [x] Bind the locked relation name to the saved target OID before mutation.
- [x] Reread unknown-COMMIT markers after the synchronous barrier.
- [x] Require snapshot-bound generated-output parity and complete-row cap admission before marker COMMIT.
- [x] Resolve warning visibility, connection-factory, and noncommitting pipeline proof findings through independent changed-portion review.
- [x] Receive final Prepared10 and Clone7 receipts on PostgreSQL 16 and 18 for the frozen input.
- [x] Record final formatting, CLI, portable, and inspection checks and classify new M2 Clippy findings separately from staged carry.
- [ ] Pass the strict broad engine closing gate, including the carried lint hold.
- [ ] Complete the three-pair matrix with eleven-case receipts for every declared pair.

The final runtime, harness, and CLI input manifest contains 58 entries at `/tmp/rpgmigrate-m2-close-xq1Ljk/source-inputs-final.json`.
Prepared10 and Clone7 passed independently on PostgreSQL 16 and 18, each with exact inventory, zero failures, and zero ignored required cases.
Formatting, the 24 CLI contract tests (including 220 long-option and 188 short-binding cases), all 180 portable tests, and `state::clone_state::tests::inspection_overlaps_owner_without_lock_or_write` passed.
The final strict Clippy triage retains 165 library and 19 library-test errors, all unused/dead code, with zero correctable lint categories.
Six newly introduced M2 pipeline staged diagnostics are recorded separately from the older retained scaffold and assigned to M3; strict lint remains red.
The full matrix launched once through `task test-e2e-clone` with the selected actual Go `ParseReport` probe after the component and triage conditions were satisfied.
Its first 16 to 16 attempt stopped at `clone-e2e: owned harness subprocess failed`; no pair receipt or eleven-case success was emitted.
The failing subprocess stage remains unclassified, and the matrix has not been rerun or repaired by this documentation update.
The matrix logs are `/tmp/rpgmigrate-m2-close-xq1Ljk/matrix.stdout` and `/tmp/rpgmigrate-m2-close-xq1Ljk/matrix.stderr`.
M0/M1 closing gates, M2's five broader deliverables, and full M2 closure remain unchecked until their required qualification receipts and strict gates exist.

The M3 workspace completed a stable baseline copy at `/tmp/rpgmigrate-m3-baseline-hnl9za4t/manifest.json` before this roadmap/ledger update.
These documentation edits are later primary-checkout changes and must be preserved during owned-file integration; runtime/harness inputs were not edited.

## User-approved WIP checkpoints, 2026-11-08

The user approved exactly two local checkpoint commits: the reviewed M0/M1/M2/M5 work and plans on `feat/native-engine`, and the M1 contract plus M6 pure helpers on the isolated operator `feat/native-contract-v1` branch.
This is an exception to the lint-clean-per-commit rule for these two WIP commits only; the normal rule resumes afterward.
Strict scaffold lint remains red, and the first matrix attempt failed in an unclassified harness subprocess before any pair receipt.
No milestone closing gate changes, runtime qualification, push, or merge is authorized by this checkpoint decision.
M3 worktrees, archive and stash refs, original operator user work, and the pgcopydb fork remain outside the checkpoint scope.
