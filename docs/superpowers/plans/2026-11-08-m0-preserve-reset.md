# M0 Preserve and Reset Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task by task.
> Use the approved team execution model below, with parallel file owners and one independent review at milestone close.

**Goal:** Preserve the archived work and leave a buildable native engine scaffold using stock libpq, portable local checks, plain CI, and the approved revised design.

**Architecture:** Keep the existing single Cargo package and transport ownership model.
Remove only the M0 experiments and controlled native build pipeline, retaining runtime state, checkpoint, COPY, catalog, decoder, and apply code for their assigned later milestones.
Keep the isolated database harness and useful portable tests, and defer the replacement runner image to M5.

**Tech Stack:** Rust 2024, the existing locked Cargo dependencies, stock PGDG libpq and PostgreSQL client tools 16 and 18, Python 3, Task, and GitHub Actions.

**Spec:** [Roadmap](../../roadmap.md), especially its decisions and M0 scope, and the [approved design](../specs/2026-11-06-rpgmigrate-design.md).
The user approved the roadmap amendments on 2026-11-08.

## Global Constraints

- Build the `rpgmigrate` executable in one Cargo package.
- Preserve `publish = false`, existing third-party notices, and the pinned pgcopydb compatibility baseline `86bf908adca02d87063849dae775a02c1659c829`.
- Preserve all parser registrations and fixtures; parser acceptance does not establish runtime support.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources.
- Keep all target bookkeeping exclusions tied to exact owned object identities.
- Keep credentials and row contents out of diagnostics and version control.
- Preserve durable completion and explicit failure for unsupported work or recovery gaps.
- Keep `stash@{0}`, `archive/stash-0`, and `wip/native-copy-snapshot` intact; never merge the archive branch.
- Use branches off `feat/native-engine`; worktrees are allowed for parallel build work.
- Run focused checks while building and independent review plus broad checks once at milestone close.
- Make no commits until the leader requests them after the required checks pass.
- Do not edit the operator or fork repositories in M0.

## Review Focus

- A machine with stock libpq and no `target/native` artifact must build without a private capability symbol or receipt.
- An invalid passfile must still fail authentication, even though stock libpq can emit its own stderr warning.
- A removed journal or storage target must not leave an empty test invocation that exits successfully.
- Caller environment overrides, including paths with spaces and an explicitly empty executable override, must retain their tested semantics.
- Documentation must distinguish revised contracts from behavior the executable actually implements.

## Baseline and preserved evidence

The independent baseline worker observed clean branch `feat/native-engine` at `1983a6a4e2aec62c8096c008316442c310be88d0` before M0 implementation.
Its archived `task check` exited 201 in `native-check` with `native libpq: native build recipe changed; run python3 tools/build-libpq.py --rebuild`.
This records a failed build prerequisite, not a Rust compiler result.
Do not rebuild the obsolete controlled libpq artifact to change that baseline.

## File ownership and parallel execution

The leader assigns these disjoint ownership sets before dispatch.
Workers report adjacent findings without editing another owner's files.

- Planner: `tasks/todo.md` and this plan only.
- Transport worker (`m0_transport`): `src/pg/native.rs`, `src/pg/mod.rs`, `src/pg/ffi.rs`, `src/pg/passfile_tests.rs`, `src/pg/live_tests.rs`, new `src/pg/test_support.rs`, `src/snapshot.rs`, and `src/snapshot_journal_live_tests.rs`.
The leader also assigned this worker the synthetic endpoint-test wording in `src/lib.rs`.
- Tooling worker (`m0_tooling`): `Cargo.toml`, `Cargo.lock`, `build.rs`, `Taskfile.yml`, `.github/workflows/ci.yml`, `Dockerfile`, `.dockerignore`, `native/libpq/**`, `bench/storage.rs`, `bench/storage/**`, `tests/storage_format.rs`, `tests/storage_recovery.rs`, `tests/test_native_libpq.py`, `tests/test_taskfile.py`, `tools/**` except `tools/test-db.py`, and `test/build.log`.
- Harness worker (`m0_harness`): `tools/test-db.py` and `tests/support/**`, split from the tooling assignment by the leader.
- Documentation worker: `README.md`, `NOTICE`, `docs/operator-compatibility.md`, `docs/superpowers/specs/2026-11-06-rpgmigrate-design.md`, and new `docs/testing.md`.
- Integration and verification worker: no implementation ownership; integrate only reviewed worker outputs and run the closing gates.
Any repair returns to the owning worker.
- Independent reviewer: read-only review of the integrated diff and evidence.

The transport, tooling, harness, and documentation workers run in parallel after leader review of this plan.
The only shared interface change is removal of `pg::native` and relocation of test helpers; the transport worker owns every Rust caller of that change.
The tooling worker removes the stale journal Task selectors when the transport worker removes the journal module.
Documentation follows the final Task names before integration is closed.

## Task 1: Remove the private libpq capability and retired live journal

**Files:** The transport ownership set above.

**Interfaces:**

- Preserve `PgConnection::connect(conninfo: &Conninfo, io: &IoControl) -> Result<PgConnection>` and every existing transport caller.
- Remove the `super::native::check()?` admission call and the private module implementing it.
- Move `Files`, `quote`, and `assert_child_completed` from `passfile_tests.rs` into `test_support.rs`, preserving their current signatures and visibility.
- Keep `pg::live_tests::passfile_and_service_authentication_preserve_security_checks` as the live authentication test identity.
- Remove only `snapshot::journal_live_tests`; preserve `snapshot::live_tests` and runtime snapshot APIs.

- [ ] Read every caller of `native::check`, the passfile helpers, and `snapshot::journal_live_tests` before editing.
- [ ] Establish focused RED evidence by linking a stock libpq build and selecting a connection test, or capture the missing-capability rejection from the current implementation if linking succeeds.
Do not rebuild or patch libpq.
- [ ] Remove `src/pg/native.rs` and its module registration, and remove the connect-time capability check without changing connection cleanup or error redaction.
- [ ] Move the shared test helpers and update their live test imports.
Remove the synthetic patched-stderr test and its `passfile_tests` registration.
- [ ] Keep the live authentication matrix for explicit, environment, default, and service passfiles, with valid, insecure, nonregular, and wrong-password shapes.
Assert success only for valid credentials and `ErrorKind::Pgsql` for the other shapes, and retain the child-completed witness.
Remove only the assertion that libpq stderr is empty.
- [ ] Remove `src/snapshot_journal_live_tests.rs` and its registration in `src/snapshot.rs`.
Do not remove CDC codecs, serial apply, COPY plans, state primitives, or checkpoint code assigned to M2/M3.
- [ ] Run the focused transport Rust tests using the stock libpq selected for this worker.

    ```sh
    cargo test --locked --lib pg::ffi::
    ```

Expected: discovered transport tests pass, with no private capability requirement.
Report the actual discovered test count and selected libpq, rather than treating a zero-test filter as success.
The live authentication matrix runs at the closing gate through `task test-transport`.

## Task 2: Replace controlled native tooling with portable checks

**Files:** The tooling and harness ownership sets above, each edited only by its assigned owner.

**Interfaces:**

- Retain `task check`, `task fmt-check`, `task lint`, `task build`, `task test`, `task test-unit`, `task test-cli`, `task test-harness`, and the non-journal database profile tasks.
- Retain `tools/test-db.py` and its positive and negative harness behavior for retained profiles.
- Remove `native`, `native-check`, `test-native`, storage experiment tasks, `test-journal-serial`, `ci:linux`, `image:build`, and `image:smoke`.
- Retain the current Task executable overrides, database-tool selection, and nonempty Rust test discovery guards.
- Keep stock library selection through pq-sys/pkg-config and optional explicit `PQ_LIB_DIR`; do not inject a `target/native` default.
- Keep `build.rs` SQLite override validation and its tests.

- [ ] Audit all references to the deleted native builder, image pipeline, storage targets, and journal test module.
Use the caller list to keep teardown complete without extending into M2/M3 runtime code.
- [ ] Change `tests/test_taskfile.py` expectations first: retained targets execute direct Cargo or retained Python commands; removed targets fail; caller overrides and paths with spaces still work; failed/empty test discovery fails the task.
Remove tests whose entire purpose is validating the retired container/image tasks.
- [ ] Run the focused Task tests and retain their expected failures against the old Taskfile.

    ```sh
    python3 -m unittest discover -s tests -p test_taskfile.py -v
    ```

- [ ] Remove `bench/storage.rs`, `bench/storage/**`, `tests/storage_format.rs`, `tests/storage_recovery.rs`, and `test/build.log`.
Remove their manifest entries and the experiment-only `redb` dev dependency.
Refresh `Cargo.lock` only for dependencies no longer referenced; do not upgrade retained crates.
Keep `bench/results/**` as historical benchmark evidence.
- [ ] Delete `native/libpq/**`, `tools/build-libpq.py`, and `tests/test_native_libpq.py`.
Remove the automatic controlled artifact selection and `native-check` prerequisite from retained Task targets.
Preserve explicit library-directory validation/rpath handling in `build.rs` where stock library selection still needs it.
- [ ] Retire the custom image pipeline: remove `Dockerfile`, `.dockerignore`, `tools/image/**`, `tools/ci/Containerfile`, `tools/ci/linux.sh`, `tools/ci/image-publish.py`, `tools/ci/test_linux.py`, and `tools/ci/test_publish.py`.
Remove receipt orchestration and its tests from `tools/ci/verify.py` and `tools/ci/test_verify.py`; retain only useful portable inventory checks if still consumed by the unit runner.
Do not implement the M5 runner image in M0.
- [ ] Simplify `tools/ci/unit.py` to run the retained Task/bootstrap and database harness suites without importing deleted image/receipt code.
Keep rejection of optimized Python, missing/invalid suite sources, zero discovered tests, skips, and failed suites.
Preserve the existing `--harness` invocation used by `task test-harness`, and make the no-argument invocation cover every retained portable tooling suite.
- [ ] Remove the retired journal-only mode and imports from `tools/test-db.py` and their journal-only fixture/support files.
Remove journal-only entries from the authority helper only when the caller audit proves they are unreferenced.
Preserve all engine, catalog, source catalog, snapshot, transport, prepared, replication, authority, COPY, and self-check profiles and their refusal controls.
- [ ] Make `task test` cover retained Rust unit, CLI contract, and portable Python tooling/harness suites, with each suite run once.
Remove obsolete journal skips from `UNIT_FILTERS` while preserving retained live test exclusions.
- [ ] Replace `.github/workflows/ci.yml` with a plain read-only CI job that checks out source, selects the manifest's Rust toolchain, installs stock libpq development files/pkg-config and Task, and runs `task check lint test`.
Reuse the existing checkout action pin and Task checksum bootstrap.
Remove image publication, registry credentials, custom fixtures, architecture packaging, and artifact-receipt gates.
Use the stock PGDG package source for the intended libpq major and document the actual package version in CI output.
- [ ] Run the focused portable checks after implementation.

    ```sh
    python3 -m unittest discover -s tests -p test_taskfile.py -v
    python3 tools/ci/unit.py
    ```

Expected: nonempty retained suites pass without building libpq, requiring storage inventory counts, or importing deleted image modules.
Report any fixture removed solely because its production profile was retired.

## Task 3: Reconcile the approved design and operator documentation

**Files:** The documentation ownership set above.

**Interfaces:**

- The roadmap remains the authority for milestone order and the dated amendments.
- Add a dated `2026-11-08` revision to the approved design and reconcile conflicting body text instead of leaving two active contracts.
- Retain the compatibility baseline, parser contract, exact ownership rules, and staged runtime refusals.

- [ ] Carry all roadmap decisions into the dated revision: source slot retention with no COPY spool, authoritative checkpoint and certified frontier, threaded/pipelined apply, measured conditional relaxed mode, copy order, drain proof, native adapter, pre-anchor data-phase restart, and stock libpq/client tools.
Also reconcile the qualified PG16/18 profile, replica trigger policy, missing-resource refusal, large-object drain re-sync, staged `list tables` refusal, retained local harness, staged code removal, and worktree permission.
Do not invent DTO names or runtime semantics reserved for M1.
- [ ] Replace stale persistence-experiment and spool durability requirements with the approved retained-source-WAL contract.
Preserve distinctions between commit order, transaction-end LSNs, target-local LSNs, positive COMMIT outcomes, durable barriers, and unknown-COMMIT refusal.
Mark old benchmark receipts as historical where referenced rather than treating them as current qualification.
- [ ] Repair links to removed plans, guides, the old ledger anchors, and the superseded copy-anchor spec.
Point the compatibility matrix at the roadmap and the maintained task ledger.
Correct cleanup ownership text so `--origin` is accepted/recorded but never created or owned.
- [ ] Update README structure, getting started, and command entries to the actual retained Task targets and stock libpq prerequisite.
Remove the deleted Dockerfile and native source tree from its structure.
Keep the README as an index, with the testing instructions in `docs/testing.md`.
State that the runner image is built in M5 and that M0 component checks qualify no operator migration.
- [ ] Update NOTICE's deleted-file references while preserving existing third-party license texts and fixture attribution.
- [ ] Verify modified Markdown links resolve, no em dashes were introduced, each sentence has its own line, and every documented command is a retained executable command.
Review prose with deslop before handoff.

## Task 4: Integrate, verify, and close M0

**Files:** Reviewed worker outputs only, with repairs delegated to their assigned owners.

**Interfaces:** The integration worker consumes each owner's changed-file list, focused check evidence, dependency/fixture removals, and remaining limitations.
The independent reviewer consumes the integrated diff and the closing evidence without editing source.

- [ ] Review the three handoffs against this plan, preserving unrelated work and every archive ref.
Integrate transport and tooling before running commands that import the relocated helpers or inspect removed journal profiles.
- [ ] Run the broad local gate once on the integrated tree.

    ```sh
    task check lint test
    ```

Expected: every command exits zero, Cargo uses the locked graph and stock libpq, and retained Rust/Python suites are nonempty.
Use the emitted executable if a separate `CARGO_TARGET_DIR` was selected.

- [ ] Select installed complete PostgreSQL 16 and 18 server/client bundles through the existing `PG_BINDIR` or `PG_CONFIG` override and exact expected-version variables.
For each available major, run the transport profile with the matching stock client library and record actual server and client versions.

    ```sh
    task test-transport
    ```

Expected: live transport and the full passfile/service authentication matrix pass.
Report unavailable bundles as an unmet check, never as qualification.
M0 does not widen the snapshot/COPY admission gate, so failures in later-milestone profiles do not authorize changing those semantics here.

- [ ] Run the retained database harness negative controls.

    ```sh
    task test-db-self-check
    ```

Expected: real setup executes, retained negative controls pass, and empty/wrong-version/wrong-reason fixture evidence is refused.

- [ ] Have an independent reviewer examine the integrated diff once, concentrating on the five Review Focus cases, FFI cleanup, preserved checkpoint/state behavior, removed-caller completeness, and unqualified support claims.
Use xhigh effort because a missed ownership or durability regression could invalidate later migration work.
Return findings to owners, then rerun affected checks and the closing gate only if repairs change the verified tree.
- [ ] Keep Go-suite execution at milestone close rather than per task.
M0 edits no Go repository; record Go checks as not applicable to this engine-only milestone.
Read the operator's current instructions before any later operator gate or edit.
- [ ] Record actual results and limitations in `tasks/todo.md`, then request the leader's completion review.
Do not mark M0 complete with failed or unavailable required checks, and do not infer operator workflow support from component evidence.
Commit only on the leader's request after the gates pass.

## Plan self-review

This plan covers the M0 roadmap rows and defers runtime behavior to M1 through M10.
Its ownership sets do not overlap, and the only new Rust file holds moved test helpers.
Task 1 covers stock-library linkage and authentication; Task 2 covers removed-target refusal, nonempty discovery, and override compatibility; Task 3 covers qualification wording.
The archived build failure remains separate from the closing stock-libpq result.
No design decision requires human escalation within this M0 scope.

## Sequencing amendment, user-approved 2026-11-08

The user approved: "Keep M0 open; proceed with approved milestones".
Removing the journal experiment exposed retained scaffold dead-code paths that fail the unchanged strict lint gate.
The independent M0 review completed with strict lint as the remaining blocker; preserve that finding and keep M0 open.
Later approved milestones may proceed while this blocker remains, without suppressing lint or treating component passes as M0 completion.
Do not make commits while the required gate is red.

The live transport profile passed five tests on PostgreSQL 16.15 and five on PostgreSQL 18.6, linked to stock libpq 18.6.
The retained harness self-check passed, including the corrected PostgreSQL 16 test minimum.
The integrated `task test` attempt passed 402 Rust tests with 66 live tests filtered and 24 CLI cases.
Its first portable run failed three Linux socket-closure controls after intentional COMMIT-response loss; a test-only ReadyForQuery repair then demonstrated RED and GREEN on Linux and macOS.
The final Linux `python3 -W error tools/ci/unit.py` passed all 134 portable checks, with output retained in `/tmp/rpgmigrate-m0-linux-harness-repair.log`.
After unused-import-only cleanup, `task fmt-check check` passed.
The independent reviewer completed the fixture-repair follow-up without a finding.
M0 remains open solely on retained-scaffold structural clippy errors.
These results qualify the exercised transport and harness components only.
