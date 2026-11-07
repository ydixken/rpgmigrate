# Verification and Performance Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development for delegated execution.
> Use superpowers:executing-plans inside each assigned task.
> Track steps with checkboxes.

**Goal:** Prove compatibility, durability, and actual target performance before release.

**Architecture:** Use independent database fingerprints and a fail-closed fixture inventory.
Compare the pinned fork and Rust candidates under the same workload and durability contract.

**Tech Stack:** Existing Rust checks, Python standard-library harness tools, libpq/PostgreSQL utilities, and isolated container or local database fixtures.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md).
The accepted [operator compatibility matrix](../../operator-compatibility.md) defines the runtime release scope; full parser coverage remains regression evidence, not a requirement to implement every CLI family.
The [engine plan](2026-11-06-engine.md) defines E0/E1 assertions.
The [CDC plan](2026-11-06-engine-cdc.md) defines E2-E7 coverage.

## Global Constraints

- The executable is `rpgmigrate`.
- The baseline is `ydixken/pgcopydb` at `86bf908adca02d87063849dae775a02c1659c829` on `origin/v0.18-fixes`.
- Every successful normal completion proves a complete durable target prefix.
- The default durability profile is `durable`.
- The default `--apply-jobs` value is `1`.
- Preserve PG9.6/test_decoding, Timescale PG13, PG16-18, and the approved operator matrix.
- Do not turn missing prerequisites, empty discovery, skipped cases, or absent fault injection into success.
- No performance gain is promised before measurement.

## Review Focus

- A fingerprint must detect a changed value with unchanged row count (V1).
- A fault test must fail when its injection never executes (V2).
- A storage win must not conceal slow apply, synchronization, or recovery (V3).
- A version label must not substitute for an actual server-version check (V4).
- A release rebuild must not replace the tested artifact (V6).

## Ownership and interfaces

The harness worker owns `tools/test-db.py`, `tests/copy_integration.rs`, and their isolated fixture support.
The compatibility worker owns generated parser fixtures and `docs/reference/compatibility.md` after a separate assignment.
The performance worker owns `bench/`, including workload manifests and result schemas.
The release worker owns CI and packaging after E1, with integration-worker coordination for manifest changes.
The integration worker coordinates root Taskfile changes and alone edits root package metadata.
The documentation worker owns the test and performance guides.

The fixed first-slice harness invocation is `python3 tools/test-db.py -- cargo test --test copy_integration -- --test-threads=1`.
Its self-check entrypoint is `python3 tools/test-db.py --self-check`.
The harness supplies disposable endpoints to its child and propagates nonzero child failure.
It must record expected cases, observed cases, fixture counts, server versions, and fault hits without credentials.
Do not create parallel Python integration suites with invented paths.

## V1: Compatibility and independent data checks

Depends on E0 contracts and E1 runtime behavior.

- [ ] Extract registrations, getopt short strings, long options, environments, and exit categories from pinned parser sources.
- [ ] Keep aliases and short-option distinctions even when help omits them.
- [ ] Record intentional identity, safety, serialization, and malformed-input deltas separately.
- [ ] Preserve valid short syntax, including required-value dump `-F` and `-N` behavior matching their long options.
- [ ] Check exact expected schema/table inventories, column/type order, row counts, and full-content digests.
- [ ] Preserve duplicate rows, NULL, empty values, and binary bytes in canonical digest input.
- [ ] Prove failures for omitted tables, changed schema, same-count value changes, wrong versions, missing cases, and exit-zero mismatch.
- [ ] Extend fingerprints to sequences, large objects, ownership, and partitions with E6.
- [ ] Publish a generated feature map whose unfinished entries remain explicit.
- [ ] Check all eight operator paths and 43 emitted behavioral options against actual job/helper callers, including default `pgoutput`, binary/parallel/split COPY, and included large objects.
- [ ] Require full semantics for every admitted CRD feature and explicit pre-mutation refusal for unsupported staged configurations.
- [ ] Reject missing native version/progress/drain/verification proof and preserve the legacy backend's behavior in separate golden fixtures.

## V2: Crash, corruption, and recovery

Depends on E2-E4 and their injectable boundaries.

- [ ] Create a manifest covering journal write/sync/publication, checkpoint update, COMMIT/reply, risk reservation, ACK, barrier, and pruning.
- [ ] Record each observed fault hit and reject absent injection.
- [ ] Exercise reader/writer death, disk exhaustion, short/corrupt writes, snapshot loss, restart, and every profile transition.
- [ ] Check target effects and checkpoint identity independently after each recovery.
- [ ] Test two processes and multiple databases competing for the same finite risk budget.
- [ ] Assert no acknowledged logged-data loss under the durable/replay-safe assumptions.
- [ ] Assert bounded relaxed risk or explicit gap refusal, never accept-loss success.
- [ ] Use separate power-loss simulation cases that can lose unsynchronized writes.
- [ ] Label process-kill results accurately and do not claim they prove power-loss durability.

## V3: Storage, apply, and migration measurements

Depends on E2 for storage and E3/E4 for applied performance.

- [ ] Define deterministic workloads for small commits, large transactions, wide rows, repeated keys, mixed DML, TOAST, indexes, and triggers.
- [ ] Record seed, exact binaries, server versions, storage, profile, caps, buffers, and concurrency for every run.
- [ ] Benchmark all storage candidates with the same canonical frames, descriptors, synchronization, and process topology.
- [ ] Compare fork and Rust with repeated paired randomized runs, separating cold and warm cases.
- [ ] Calibrate run counts and acceptance tolerances from measured repeatability before evaluating candidates.
- [ ] Measure target-committed rows/transactions per second, sustained catch-up, and p50/p95/p99 commit-to-apply latency.
- [ ] Attribute decode/storage cost, target CPU/index work, round trips, pipeline waits, checkpoint WAL, bloat, and commit latency.
- [ ] Include full drain, target/state synchronization, reclamation, restart replay, and end-to-end migration duration.
- [ ] Repeat on local SSD and representative operator storage before production performance acceptance.
- [ ] Reject comparisons that weaken durability, omit work, change failover assumptions, or count queued LSNs as apply throughput.

Store one versioned result per run with fields `case_id`, `seed`, `binary_digest`, `profile`, `server_versions`, and `storage`.
Include `committed_rows`, `committed_transactions`, `elapsed_seconds`, latency percentiles, peak memory/spool, synchronization time, and recovery time.
Keep raw samples and the predeclared acceptance rule with the report.
Candidate selection requires correctness and repeatable value without material apply/full-migration regressions.

## V4-V6: Remaining release gates

- [ ] V4: Preserve exact fork version/plugin cases, PG9.6/test_decoding, Timescale PG13, and operator source14-18 to supported target17/18 cases.
- [ ] V4: Assert actual server versions and matching utility bundles without inventing a universal cross-product.
- [ ] V5: Run format, lint, units, database tests, parser/state fuzzing, FFI ownership/sanitizers, and bounded fixture checks.
- [ ] V5: Check locked dependencies, required notices, native dependencies, generated documentation, and STE100 hard rules.
- [ ] V5: Add serial/parallel differential and adversarial tests when E7 starts.
- [ ] V6: Build Linux amd64/arm64 artifacts and record exact binary/image digests.
- [ ] V6: Run isolated operator E2E on those published candidate artifacts.
- [ ] V6: Promote the same tested digests without rebuilding after every declared operator-profile, recovery, quality, and artifact gate passes.

Expand V4-V6 into release-specific steps after E1 and before their implementation.
macOS results prove development behavior only.
The operator's own current instructions govern its CI-first functional tests.

## Self-review

V1 retains parser regression coverage and proves the selected operator contract and rejection of false-positive fingerprints.
V2 covers every durability boundary and relaxed refusal.
V3 requires target apply evidence beyond storage throughput.
V4-V6 retain cross-version, FFI, dependencies, operator, and exact-artifact release gates.
No empty or skipped suite can close a milestone.
