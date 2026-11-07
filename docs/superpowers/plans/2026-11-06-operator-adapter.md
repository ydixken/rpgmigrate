# Operator Adapter Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development for delegated execution.
> Use superpowers:executing-plans inside each assigned task.
> Track steps with checkboxes.

**Goal:** Run fresh rpgmigrate migrations through the operator while preserving existing pgcopydb jobs.

**Architecture:** Add a targeted native adapter at the existing engine boundary.
Use versioned native status and durable completion evidence while keeping legacy jobs on their original runtime.

**Tech Stack:** The existing Go operator, its current controller/test framework, and the tested rpgmigrate artifact.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md).
The user accepted the [operator-first compatibility matrix](../../operator-compatibility.md) on 2026-11-07; its eight paths and finite feature contract replace full-CLI parity as the release boundary.
Read the [verification plan](2026-11-06-verification-performance.md) for artifact and matrix gates.

## Global Constraints

- The executable is `rpgmigrate`.
- Existing pgcopydb migrations retain their original runtime.
- Never import, resume, restart, convert, delete, or clean legacy pgcopydb state.
- Every successful normal completion proves a complete durable target prefix.
- The default durability profile is `durable`.
- The default `--apply-jobs` value is `1`.
- `relaxed` requires explicit finite `--loss-max-transactions` and `--loss-max-bytes` values.
- Keep the operator diff and verification gate separate from the engine.
- Preserve all admitted CRD semantics; refuse unsupported staged configurations before mutation rather than ignoring options.
- Default follow is `pgoutput`; large objects, binary fallback, and parallel/split COPY remain explicit gates.
- Do not emulate legacy SQLite or source-sentinel storage, and do not invent arbitrary extra-argument passthrough.

## Review Focus

- An existing job must not switch runtime after a controller upgrade (O1).
- A similarly named user schema must never be excluded or reowned as metadata (O2).
- Visible asynchronous target progress must not satisfy cutover durability (O2).
- A receipt for another run or an incomplete database set must fail completion (O3).
- A candidate artifact must pass the real source/target matrix before promotion (O4).

## Ownership and handoff contract

All paths below are relative to `/Users/ydixken/development/pgcopydb-operator`.
This documentation task makes no changes there.
The future adapter worker must first read its current AGENTS.md and inspect existing uncommitted work.
Do not copy mutable operator tasks or instructions into rpgmigrate.
Use a separate ordinary feature branch and the operator's existing engine/controller boundaries.

The adapter worker owns a native adapter under `internal/rpgmigrate/` after the leader assigns it.
The accepted direction extends existing `ProgressOps`/`SentinelOps` with per-Migration backend selection, reusing current exec, connection, and job helpers.
The API worker owns any required additions in `api/v1beta1/migration_types.go` and compatibility conversion/tests.
Freeze an optional immutable `spec.engine` with values `pgcopydb` and `rpgmigrate`, omission retaining legacy behavior, and new-UID/native-workdir admission before API or generated-file edits.
The controller worker owns integration in `internal/controller/resources.go`, `follow.go`, `sampler.go`, `verification.go`, and `reown.go`.
One worker must own each shared controller file at a time.
The verification worker owns adapter/controller cases and operator E2E fixtures.
Regenerate generator-owned artifacts through existing project targets.

Before O2, the E6 progress worker and adapter worker must freeze the versioned native status/receipt schema together.
Freeze native version/capability, COPY metric, cutover-LSN, verification, and owned-cleanup DTOs with that schema.
It must carry run/stream identity, effective mode/epoch, all five frontiers, aggregate risk caps/usage, and exact owned metadata identities.
Completion evidence must identify the requested and achieved prefix, each database, required phases, and durable target proof.
Large positions and counters must retain integer precision in machine output.
Keep native evidence separate from preserved legacy output shapes.
No adapter may infer completion from exit zero, requested endpos, replication origins, or an async-visible checkpoint alone.

## O1: Runtime selection and arguments

Depends on the eight-path/43-option E5 contract and a runnable tested engine artifact; unrelated CLI families are deferred.

- [ ] Add golden tests for existing pgcopydb jobs and explicit new native jobs.
- [ ] Test immutable engine selection, omitted selection remaining legacy, and refusal to adopt an existing migration or legacy workdir.
- [ ] Cover all 38 worker and five helper option names, defaults, conditional branches, environment precedence, and existing job runtime identity.
- [ ] Use `rpgmigrate` executable/version/log/application identity and fresh native workdirs for native jobs.
- [ ] Replace native dependence on the legacy version-prefix/21-string gate with the frozen native capability contract, without changing the legacy path.
- [ ] Preflight every selected CRD configuration; reject unsupported stage options explicitly and verify no mutation occurs.
- [ ] Qualify default `pgoutput`, included large objects, binary fallback, and parallel/split COPY rather than substituting the current `test_decoding` baseline.
- [ ] Add checked mode, explicit loss caps, and apply-concurrency settings at the existing configuration boundary.
- [ ] Refuse attempts to reuse legacy state or silently change an existing job's runtime.
- [ ] Keep valid legacy jobs unchanged in the regression fixtures.

## O2: Rights, ownership, and progress

Depends on E3/E4 metadata and E6 native progress contracts.

- [ ] Test target CREATE and preprovisioned USAGE/SELECT/INSERT/UPDATE roles.
- [ ] Test cleanup privileges separately from apply privileges.
- [ ] Reject namespace collisions and metadata identity mismatches.
- [ ] Exclude only exact owned bookkeeping objects from comparison and `ownerAfterRestore`.
- [ ] Preserve user objects with similar names in regression fixtures.
- [ ] Map received, journal-durable, target-committed, target-durable, and source-acknowledged frontiers accurately.
- [ ] Expose mode, epoch, and aggregate risk without treating visible progress as durable completion.
- [ ] Keep target-origin SQL on the legacy backend and require checked native checkpoint/proof evidence on the native backend.
- [ ] Test progress, control, and cleanup from the actual worker/helper containers sharing the PVC, private URI/passfile/TLS inputs, and native-owned workdir.
- [ ] Implement no SQLite-layout or source-sentinel-table emulation; preserve explicit native ownership at each operation.

## O3: Completion and refusal

Depends on E4 durable receipts and O2 status decoding.

- [ ] Test valid receipts, stale epochs, wrong run identity, missing database entries, and requested/actual end-position mismatch.
- [ ] Require a complete durable prefix for every required database and phase before cutover success.
- [ ] Bind scalar cutover output, requested endpos, actual replay/durable positions, worker exit, and independent drain without treating any single observation as completion.
- [ ] Reject missing/incomplete comparison reports and native proof even when a child exits zero or progress counters are absent.
- [ ] Propagate gap errors as nonzero refusal requiring a fresh migration.
- [ ] Test retries after unknown COMMIT using target PRIMARY checkpoint authority.
- [ ] Document external/nontransactional trigger effects and the need for idempotency.
- [ ] Preserve pgcopydb completion behavior on existing legacy migrations.

## O4: Separate validation and release

Depends on O1-O3, the selected profile's E5-E7 gates, and V4-V6.

- [ ] Run permitted local formatting/lint and current operator CI functional checks.
- [ ] Preserve source14-18 to supported target17/18 cases without assuming every pair is supported.
- [ ] Run isolated candidate operator E2E with the exact published engine/image digests.
- [ ] Exercise native clone/follow/cutover, resume/refusal, restrictive roles, metadata collisions, and aggregate relaxed caps.
- [ ] Exercise all eight consumed operations, default behavior, and every admitted CRD feature; explicitly reject unsupported configurations outside a staged profile.
- [ ] Update affected API, chart, operations, and durability documentation in the same change.
- [ ] Promote only tested digests after separate operator review.

Expand the selection/generated-API changes, DTO fields, and controller function signatures before O1/O2 implementation.
The native wire contract depends on implemented engine evidence and must not be guessed from legacy sentinel fields.
The current Gate 3 `test_decoding` consumer remains a separate bounded baseline; no operator implementation or PG lease is authorized by this documentation update.

## Self-review

O1 preserves legacy runtime selection.
O2 covers privileges, exact object ownership, and honest progress.
O3 covers complete durable evidence and gap refusal.
O4 retains the separate operator and release gates.
No operator edit or mutable task import belongs to this documentation bootstrap.
The remaining 40 behavioral command paths are optional CLI backlog, not prerequisites for this operator release contract.
