# M9 Durability Modes and Measured Performance Implementation Plan

Root approved this implementation plan on 2026-11-09 within the standing execution scope through M10.
The approved private source is `/Users/ydixken/.codex/tmp/rpgmigrate-m9-plan-20261109/m9-durability-performance-plan.md`, SHA-256 `11064ab09e50753f956a8a2f8be617e9e07a05988a91dd49f8f09511db43fd4e`.
This repository copy preserves the approved protocol/design and historical planning assertions while applying documentation style.
**Execution note:** Existing approved milestone scope authorizes owned M7 UP/load/legacy/native tests and ordinary fresh owned fault fixtures; source freeze, guard checks, exact ownership, resource admission, and required evidence remain prerequisites rather than a new blanket user-permission gate.
Benchmark execution stays on HOLD for the final actual artifact/argv packet review, and shared-VM poweroff, original retained resources, main pushes/merges, tags, releases, and GHCR publication remain outside this authorization.
This note supersedes historical planning-only or separate-approval wording below where it would request approval again for already approved owned work; it claims no execution result or milestone closure.

> [!IMPORTANT]
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task by task.
> The leader assigns one file owner and the independent closing reviewers.
> Steps use checkbox syntax for tracking.

**Goal:** Qualify replay-safe CDC, report its effective settings, and measure committed target work against the pinned fork without weakening durable completion.

**Architecture:** Reuse the existing target checkpoint, per-run fence, observed COMMIT, synchronous writing barrier, and source-slot recovery path.
Replay-safe groups may commit asynchronously, but only a retained positive synchronous proof authorizes feedback.
Keep the benchmark driver outside the engine, using existing Python/libpq/container and SQL-oracle helpers.

**Tech Stack:** One Rust Cargo package, stock PGDG libpq, PostgreSQL 16 and 18, Python standard library, existing Task and Podman tooling.

**Spec:** `/Users/ydixken/development/rpgmigrate/docs/superpowers/specs/2026-11-06-rpgmigrate-design.md`, its roadmap amendments, `docs/operator-compatibility.md`, and `tasks/lessons.md`.

**Planning provenance:** Engine `feat/native-engine` at `60ba21953a1e4af7899ca7ea3a7919fcc8cec756`.
The inspected operator checkout is `/Users/ydixken/development/pgcopydb-operator-m4-closing-m6-composed-20261109`.
The behavioral reference is `/Users/ydixken/development/pgcopydb`, pinned to `origin/v0.18-fixes` at `86bf908adca02d87063849dae775a02c1659c829`.
Read current instructions and freeze actual successor hashes before implementation.
This private plan records proposed work, not executed checks.

## Global Constraints

- Build the `rpgmigrate` executable in one Cargo package.
- `durable` is the default.
- Profiles apply to CDC.
- COPY and schema completion are durable in every profile.
- Never import, open as a migration, resume, restart, convert, delete, or clean pgcopydb state or resources.
- The qualified native profile targets 16→16, 18→18, and 16→18 only.
- Use stock PGDG libpq and PostgreSQL client tools 16 and 18.
- Positive target COMMIT acknowledgment is required before transaction feedback releases source WAL.
- A read-only transaction or empty COMMIT is not a barrier.
- Every successful normal completion or cutover drains work, synchronizes target and state, and publishes a durable completion receipt.
- A recovery gap requires nonzero refusal and a fresh migration.
- Keep credentials and row contents out of diagnostics and version control.
- Use ordinary feature branches and preserve every unrelated checkout change.
- Add no dependency, production tunable, controller framework, transport replacement, or storage backend for this milestone.
- Run one independent milestone review and the applicable Go suites at milestone close.
- Commit only after required checks pass and the leader requests it.

## Review Focus

- A visible asynchronous checkpoint must never replace the last positive durable proof as feedback authority.
- A timer expiring inside one source transaction must not split that transaction or certify partial work.
- Recovery after lost COMMIT must prove the surviving checkpoint under the same fence before selecting a source restart position.
- Status settings must come from the stored run configuration, including after worker exit, without becoming completion evidence.
- Missing benchmark children, missed faults, wrong refusal reasons, insufficient latency samples, or unfinished drain must fail the evidence gate.

## Decisions and dependencies

The root approved the following planning decisions on 2026-11-09.
These decisions authorize the design below, not benchmark execution or VM reservation.

1. Use a capability-tagged optional protocol-v1 status extension, `durability.status.v1`.
2. Persist effective settings in `FollowState`, separately from target checkpoint and feedback proofs.
3. Base M9 on M8's future native format 9 and target metadata 7.
4. Propose native format 10 for the new local settings representation.
5. Resume uses stored settings.
An explicit conflicting setting refuses before database effects.
6. Add no mode-change operation or extra user tunable.

M8's final part-marker schema and source handoff remain prerequisites.
M8 keeps `CopyReport { tables, rows, bytes }` and protocol-v1 report shapes unchanged.
The root approved `ClonePlan.copy_tables` entries keyed by `source_oid`, with format and parts kept outside catalog-only `CloneTable` comparisons.
Require a complete unique ordered table-keyset and immutable execution equality within a copy generation.
Only the validated fresh-generation restart transition may rebuild part bounds.
Do not freeze speculative M8 column or profile names in M9 source.
M9 changes no target metadata-7 columns and does not reuse native format 9 for a different shape.
Native formats through 9 are refused by M9 before database side effects, without conversion.
The operator's independent private reader must make the same format decision.

M6/M7 activation and M8 qualification precede operator-default measurements.
Production native admission remains `UnsupportedEngine` until its own gates close.
M4 suite elapsed times are not throughput evidence.
The retained runtime matrix is `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/e2e-runtime/runtime-matrix-proof.json`.
Its 10-case cutover cells took about 18.54, 18.87, and 18.62 seconds.
Its 21-case follow cells took about 191.78, 195.20, and 196.94 seconds.
Those timings include setup, faults, and polling.
They set no M9 acceptance threshold.

The current source also differs from the aspirational two-round-trip design.
A warm native group takes approximately `ceil(commands / 62) + 8` request/response phases.
The pinned fork already uses prepared execution, pipelines, grouped synchronous CDC, and approximately two phases per group.
Its default grouping uses 100 source transactions, 10,000 replay statements, and a 200 ms interval.
Native uses 100 transactions, 10,000 logical events, 8 MiB, and approximately 100 ms idle polling.
The units differ.
Treat these source observations as hypotheses to measure, not performance results.
The earlier 0.7–1.1 COPY, 0.5–2 CDC, and 0.7–1.2 full-clone ranges are unmeasured and are not tolerances.

## File ownership and sequence

Assign these owners after the M8 handoff freezes.
An owner may edit only its listed files.
The leader transfers a file explicitly before any second owner edits it.

### Owner A: CLI and settings contract, high effort

Own `src/contracts.rs`, `src/config.rs`, `src/cli.rs`, and their inline tests.
Also own the mechanical `FollowOptions` constructor updates in `src/snapshot/follow_tests.rs`, `src/snapshot/follow_fault_tests.rs`, and `src/cdc/receive.rs`.
Finish the shared type freeze before B, C, or D edits consumers.

Produce these exact representations:

- `DurabilityMode { Durable, ReplaySafe }`, serialized as `durable` or `replay-safe`.
- `EffectiveFollowSettings { durability: DurabilityMode, barrier_interval_ms: Option<u64>, apply_group_txns: u64, limits: FollowLimits }`.
- `FollowOptions.durability: Option<DurabilityMode>` preserves whether the CLI supplied a mode.
- `FollowOptions.apply_group_txns: Option<u64>` preserves whether the environment supplied a group override.
- Keep the existing `FollowOptions.limits: FollowLimits` as validated request input.
Runtime consumers use the resolved stored `EffectiveFollowSettings`, not request defaults.

The effective JSON `limits` object keeps exactly `channel_bytes`, `channel_messages`, `max_message_bytes`, `group_rows`, and `group_bytes`.
Each value is a positive integer within the existing compiled limits: 8 MiB, 1024, 16 MiB, 10,000, and 8 MiB respectively.
`apply_group_txns` is a positive uint64.
Durable requires `barrier_interval_ms=null`.
Replay-safe requires a positive measured finite interval.
Use integer parsing without float coercion.

Do not add a settings epoch.
Existing target barrier epochs retain their existing proof semantics.
With no mode-change operation, a second local epoch would create unused parallel authority.
Any future mode transition needs a separate approved contract for the quiesced durable boundary.

### Owner B: native settings persistence, high effort

Own `src/state.rs`, `src/state/follow_state.rs`, `src/state/clone_state.rs`, and state fixture validation under `tests/support/follow_snapshot_fixture.py` and `tests/support/test_follow_snapshot_harness.py`.
Own required format-constant fixture updates in the state tests.

Add required nullable `effective_settings` to `FollowState`.
It may be null only while INIT has no resource intent.
Once source or target work is admitted, it must contain one validated `EffectiveFollowSettings`.
Keep `FollowControl`, the target row, `SafeFeedback`, and target metadata version 7 unchanged.

Produce:

- `NativeState::resolve_follow_settings(&mut self, requested_mode: Option<DurabilityMode>, requested_group_txns: Option<u64>, requested_limits: FollowLimits) -> Result<EffectiveFollowSettings>`.
- A read-only accessor on the existing clone inspection result for `Option<EffectiveFollowSettings>`.

For a fresh run, choose durable unless the caller explicitly requests replay-safe.
Use group 100 only when no explicit group override exists.
Resolve the compiled measured replay-safe interval in one place.
For a stored run, preserve the entire stored object.
A supplied mode, group, or request limit that conflicts must fail before connecting to either database.
Omitted CLI/environment values do not overwrite stored values.
Publish settings through the existing locked, synchronized manifest replacement.
Do not infer settings from target row visibility or a report.

### Owner C: target commits, scheduler, and coordinator, xhigh effort

Own `src/apply/mod.rs`, `src/apply/checkpoint.rs`, `src/apply/follow.rs`, `src/apply/follow/tests.rs`, `src/apply/serial.rs`, `src/apply/serial/tests.rs`, `src/follow.rs`, and apply-side unit tests.
The single target-authority file owner also implements the live Rust assertions in `src/apply/checkpoint_live_tests.rs`.
Coordinate with the verification owner, who supplies required cases and checks evidence independently.

Produce these exact call changes:

- `Caches::commit(target: TargetTxn<'_>, durability: DurabilityMode, io: &IoControl) -> Result<CommitOutcome>`.
- `TargetTxn::commit_pipeline(durability: DurabilityMode, io: &IoControl, send, validate) -> Result<CommitOutcome>`.
- `committed_group(outcome: CommitOutcome, last: Option<Frontier>, durability: DurabilityMode) -> Result<Option<DurableProof>>`.
- Add `settings: &EffectiveFollowSettings` to the existing `apply::follow::run` boundary.

Retain all existing pipeline result validation, CAS, deferred-effect checks, failure latching, warning handling, session checks, and dedicated-backend ownership.
Promote the existing internal asynchronous `TargetCommit` variant from test-only use.
The row and checkpoint token still advance atomically with each committed group.
A token by itself remains insufficient evidence of durability.
Durable groups return a positive proof.
Replay-safe asynchronous groups return a checked checkpoint with `durable=None`.
Unknown COMMIT remains an error requiring recovery.

Keep the session's captured synchronous requirement for barriers.
Use `SET LOCAL synchronous_commit=off` only for an explicitly replay-safe data-group COMMIT.
A later writing barrier restores the captured `on` or `remote_apply` requirement.
Do not lower the barrier requirement of a synchronous-standby deployment.
COPY, anchor installation, certification, drain, final sequence/large-object work, and completion remain synchronous.

Retain the last positive proof or its `SafeFeedback` as a separate in-memory value.
Never derive feedback from `authority.applied()` after an asynchronous group.
A timer may request a barrier only at a target transaction boundary.
A long source transaction remains whole, then forces the overdue barrier immediately after its COMMIT.
The interval is a scheduling target, not a maximum wall-clock loss or lag window.
On an idle receive timeout, a pending unproven prefix still forces its due writing barrier.
It must not wait for another source transaction or keepalive.
A positive keepalive certification or drain barrier may discharge the same pending prefix.
Failure or uncertain COMMIT must never clear the pending state or publish a new safe frontier.

Wire stored settings into channel creation, receiver limits, startup reporting, and apply.
Resolve and persist them before source/target effects on first entry.
On recovery, reacquire the exact target fence, validate the surviving row, issue the existing positive writing barrier, check the source confirmed-flush floor, and only then publish ready/feedback.
If source confirmed-flush exceeds proven target/anchor authority, return exit 3.
Do not replay a guessed prefix or relabel an uncertain run.

### Owner D: effective status and operator compatibility, high effort

Own engine `src/protocol.rs`, `src/progress.rs`, `src/lib.rs`, `src/stream.rs`, and status-related test fixtures.
Own engine `tests/fixtures/native/v1/{contract.json,cases.json,manifest.json,payloads/}` and `tests/test_native_fixtures.py`.
Own operator `internal/native/{contract.go,contract_test.go,fixtures_test.go,transport_test.go,testdata/v1/}` and `internal/controller/{backend.go,backend_test.go}`.
Keep operator changes and commits separate.

Add optional top-level `settings: EffectiveFollowSettings` with capability `durability.status.v1`.
Participating commands are follow `clone` completion, `list progress` for a follow run, and JSON `stream sentinel get`/`set endpos`.
Plain clone and cleanup omit settings and this capability.
Standalone JSON carries the same settings without an operator identity or framed report.
Do not add settings to comparison arrays.

Change the envelope capability representation from a fixed one-element array only as needed to emit the command capability plus `durability.status.v1`.
Require the settings field and capability together.
A report without that capability remains the old v1 shape.
A follow report expected from an M9-qualified image must include both.
Settings fields do not satisfy any checkpoint, endpos, drain, or completion predicate.
The operator validates exact names, values, nullability, integer bounds, and mode/cadence combinations.
Unknown keys, duplicate members, floats, exponents, wrong case, missing limits, zero values, and unsupported modes refuse.
Update both shared fixture copies byte-for-byte and regenerate their manifest hashes.

Current old-consumer refusal is verified in code, not runtime qualification.
`internal/native/contract.go::RequireCapabilities` returns `unknown native capability` in its default branch.
`ParseVersion` calls that function.
`validateNativeConfig` also calls it.
`ParseReport` uses `DisallowUnknownFields`, and `canonicalMember` currently rejects `settings`.
Therefore an old parser rejects the new capability or payload.
The M6 production path is still disabled, so actual preflight-before-worker refusal must be demonstrated against the final activated successor.
Add that case to M7/M10 closing evidence rather than claiming the current pure parser proves it.
Advertise the new capability from discovery only after the engine/operator qualification gates pass.
Do not create a hidden fallback to old status for a qualified M9 image.

The operator independent-reader owner must also update `internal/native/evidence_program.py` and its tests for native format 10 and required `effective_settings`.
Keep target metadata 7 and the target `follow_binding` contract from M8 unchanged.
Treat this update as an explicit file transfer to D after M6/M8 writers stop.
Regenerate the embedded helper with its owning mechanism and retain its existing image/source binding checks.

### Owner E: deterministic fault and qualification harness, high effort

Own `tools/e2e/{clone.py,follow.py,cutover.py,follow_wire.py,test_clone.py,test_follow.py,test_cutover.py,test_follow_wire_protocol.py}`, follow/cutover fixture inventories, `tests/support/follow_authority_cases.json`, and associated receipt validators.
Coordinate planned Rust authority cases with C before C edits the shared live-test file.

Reuse the existing SQL/wire gates and exact event observations.
Do not add sleeps as fault triggers.
Observe the exact data-group COMMIT, actual barrier request, barrier COMMIT response, and feedback status separately.
The existing bounded wire observer is for fault qualification.
Do not silently increase its 256-transaction evidence map into a throughput collector.

Required deterministic cases are:

1. Durable group: synchronous proof and feedback remain unchanged.
2. Replay-safe group: target rows and checkpoint are visible, but no feedback exceeds the previous proven frontier.
3. Due barrier: one positive logged write covers all prior asynchronous groups, then feedback advances monotonically.
4. Idle timer: a completed asynchronous group becomes durable without further source input.
5. Long source transaction: no mid-transaction barrier, no partial ACK, one whole group, then overdue barrier.
6. Process kill after async COMMIT before barrier: the target stays alive, restart proves the surviving row, and contents have no duplicate effects.
7. Target immediate crash before barrier: recover the surviving prefix, replay retained source input, and compare full contents and transaction counts.
8. Lost async COMMIT response: stop with unknown outcome, reacquire the fence, prove the actual row, and resume exactly.
9. Lost barrier COMMIT response or synchronous-wait WARNING: no proof or ACK is published before a new successful fenced writing barrier.
10. Keepalive beyond an unproven async prefix: certification cannot cross it without the covering synchronous barrier.
11. Endpos/crossing transaction/later-BEGIN: drain forces complete durability and preserves the existing distinction between witness and feedback frontier.
12. Final sequence/large-object synchronization failure: no success receipt, including under replay-safe.
13. Source confirmed-flush ahead of recovered row, lost WAL/slot, changed publication identity, or source identity drift: exact exit-3 refusal and no successful completion.
14. Resume with omitted settings preserves the stored object; explicit mismatch and old format fail before database effects.
15. Wrong/missing fault observation, missing case module, skipped child, empty fixture, forged settings, and wrong refusal reason fail the harness itself.

Register every new live case in the required profile inventory and every portable module in `tools/ci/verify.py`.
A process kill is not host power-loss evidence.
A PostgreSQL immediate crash is not a power cut of the VM or storage.
Use those exact labels in receipts.

### Owner F: minimum benchmark harness, high effort

Own new `tools/bench/m9.py`, `tools/bench/test_m9.py`, and `tests/fixtures/bench/m9.json` plus deterministic seed SQL.
Reuse `clone.run`, `clone.wait_for`, private diagnostics, `clone.Database`, fixture-bound content/schema oracles, and existing libpq helpers where their contracts fit.
Do not reuse `clone.outer`: it builds while running, chooses qualification caps, and does not implement the proposed benchmark resource lease.
Do not change the engine's runtime scheduling to make a harness observation easier.

The new driver has `prepare`, `run`, and `summarize` subcommands.
`prepare` writes a no-clobber run manifest and randomized schedule without starting databases or taking a lease.
`run` requires the approved manifest hash, exact phase, original unexpired exclusive lease, and exact binary/image artifact lock.
`summarize` reads retained receipts only and refuses incomplete schedules.
Each scheduled candidate runs in a fresh child process, including warmups.
No child may inherit `ru_maxrss` or engine caches from another candidate.

Add offline tests for randomized balanced ordering, fresh-process enforcement, missing/tampered child receipts, expired lease, ownership mismatch, unsupported versions, deadline, disk threshold, insufficient samples, incomplete final drain, bad cleanup result, and changed artifact hashes.
Register the module in the existing portable inventory through the integration owner.
Use the Python standard library only.

### Owner G: integration, docs, and final receipt, high effort

Own `Taskfile.yml`, `tools/ci/verify.py`, `tools/ci/test_verify.py`, relevant Taskfile/workflow tests, `docs/durability.md`, `docs/testing.md`, `docs/native-protocol.md`, `docs/operator-compatibility.md`, `docs/roadmap.md`, and `tasks/todo.md`.
Own affected operator documentation after a separate handoff.
Do not replace unrelated historical receipts.
Track the benchmark approval and resource lease explicitly.

## Implementation tasks

### Task 1: freeze settings and persistence

- [ ] A writes parser tests for omitted/default durable, explicit replay-safe, positive group override, relaxed refusal, and invalid modes/caps before I/O.
- [ ] B writes state tests for exact stored settings, omitted resume values, explicit mismatch refusal, required null only in resource-free INIT, unknown fields, and format-9 refusal.
- [ ] Run the targeted unit tests to observe intended failures.
- [ ] A and B implement their frozen interfaces, preserving the existing CLI precedence rules.
- [ ] Update all `FollowOptions` constructors and all callers found by `rg`, including the receiver's direct live fixtures.
- [ ] Run the scoped offline tests and formatting in isolated Cargo targets.

### Task 2: replay-safe target authority and scheduler

- [ ] C adds failing authority tests for asynchronous positive COMMIT without proof, proof-producing barrier, and unchanged durable behavior.
- [ ] C adds fake-clock scheduler tests for idle expiry, transaction-boundary expiry, no pending prefix, barrier failure, and immediate drain.
- [ ] Implement asynchronous group selection through the existing observed-COMMIT path.
- [ ] Wire the last proven frontier and due-barrier schedule without changing receive feedback authority types.
- [ ] Run the targeted tests, then the exact authority profile on PG16 and PG18 when a live-test resource window is assigned.
- [ ] Preserve and review the full diff around session GUC restoration, proof construction, and unknown-COMMIT outcomes.

### Task 3: effective help and status

- [ ] D writes shared positive and negative fixtures before changing producer/consumer DTOs.
- [ ] Implement stored settings in the participating status commands, with capability/field consistency.
- [ ] Update the strict operator validator and helper's native-format validation.
- [ ] Make help render mode availability, measured cadence, and existing effective group/buffer defaults from the same canonical values.
- [ ] Keep the measured cadence unselected in release claims until Task 6 completes.
- [ ] Verify old-parser rejection and new-parser acceptance using the actual Go parser against original emitted bytes.
- [ ] Check that settings never satisfy an independent durability or drain assertion.

### Task 4: fault qualification and documentation

- [ ] E implements the exact deterministic case inventory above using observed boundaries.
- [ ] G writes the complete failure matrix below and links it from the existing testing and operator contract guides.
- [ ] Run the three declared engine pair profiles with both admitted modes, preserving separate process-crash and target-crash evidence.
- [ ] Run independent SQL fingerprints, including duplicate multiplicity, key changes, NULLs, sequences, partitions, M8 parts, and large objects.
- [ ] Keep physical power-loss and synchronous-standby failover evidence open until their separately admitted environments exist.

### Task 5: benchmark preparation without measurement

- [ ] F implements and offline-tests the new driver and manifest validation.
- [ ] Build immutable release binaries outside timed windows from frozen native and pinned-fork source exports.
- [ ] Record exact binary, linked-libpq, compiler, client-tool, source-tree, and image hashes in the artifact lock.
- [ ] Run only offline schedule/receipt negative controls during this preparation task.
- [ ] Present the concrete packet below with actual artifact-lock hash before any benchmark or VM reservation.

### Task 6: approved measurement, selection, and integration

- [ ] Obtain benchmark-plan approval and a new exclusive resource assignment for the first exact phase.
- [ ] Run A/A phases first and publish their tolerances before opening A/B receipts.
- [ ] Run the predeclared randomized comparison phases, rejecting incomplete or contaminated pairs.
- [ ] Select a replay-safe cadence from the measured candidates using the rule below.
- [ ] Rebuild only if the chosen constant differs from the exact tested release candidate, then repeat required qualification on that exact artifact.
- [ ] Update help, stored default settings, status fixtures, and docs from the chosen value.
- [ ] If no candidate qualifies, keep replay-safe unadvertised and report M9 blocked on measured evidence rather than choosing an invented value.
- [ ] Keep relaxed and binary pgoutput conditional under the gates below.
- [ ] After file freeze, run `task check lint test`, required PG16/18 live profiles, three-pair runtime matrix, applicable operator Go suites/CI, and independent safety review once at milestone close.
- [ ] Bind every closing result to the exact source/artifact hashes and update `tasks/todo.md` with observed outputs.

## Benchmark dataset and observation contract

Use only deterministic synthetic data in new benchmark-owned databases.
No endpoint, credential, row content, source database, slot, publication, or directory from an existing migration is an input.
The pinned fork checkout is read-only reference material.
Each fork child gets fresh harness-owned state, which is retained rather than adopted by native code or cleaned through a native command.

The primary hardware cell is local Podman-VM storage, 18→18.
The confirmation cells are 16→16 and 16→18.
Record the actual image digests and server/tool minor versions.
M8 operator defaults must be qualified before default-profile benchmarking.
Use four table workers and the same selected COPY format on both engines.
Record every explicit flag, omitted-default choice, and the different logical-event/replay-statement group units.
Fork follow retains its actual default plugin and defaults, without silent substitution.
A separately forced common-plugin comparison is permitted only if both engines actually admit that plugin and the manifest declares it.
It is not a replacement for the fork-default comparison.

The narrow fixture has 16 ordinary logged heaps with 65,536 rows each.
Each row has an `int8` primary key, an `int8` mutable value, and deterministic 96-byte ASCII text.
The wide fixture has four logged heaps with 4,096 rows each.
Each row has the same key/value columns and deterministic 8,192-byte varied text built with PostgreSQL's built-in `md5`.
Each physical table remains under the existing 100,000-row and 64-MiB oracle bounds.
Add one ordinary logged probe table for source-transaction IDs and timing markers.
Keep a known unchanged probe row for the final logged barrier.
Record exact table counts, relation OIDs, row counts, canonical COPY byte counts, schema fingerprints, seed hash, and content fingerprints.
Refuse an empty or altered fixture.

The four workload profiles are COPY narrow, COPY wide, CDC narrow, and CDC wide.
COPY children record both data-phase bytes/time and full clone elapsed time through post-data, sequences, ANALYZE, final synchronization, and checked closure.
Count bytes from the canonical text representation, and label the metric `canonical COPY MiB/s`.
Also retain actual selected wire-format byte counts when available.
Do not compare binary wire bytes with text payload bytes under one unlabeled denominator.

CDC uses predeclared single-row and 100-row source-transaction segments within each profile.
Use stable 40/20/40 INSERT/UPDATE/DELETE cycles over bounded deterministic key ranges.
Balance INSERT and DELETE work so live table cardinality stays inside the declared oracle bounds.
Cap a child at 10,000 source transactions, 1,000,000 changed business rows, and 1 GiB canonical input.
Stop at the first cap and mark any resulting insufficient latency cell explicitly.
Every source transaction writes one probe marker, which is excluded from business-row throughput but counted separately as benchmark overhead.
Record source transactions and changed business rows only after successful source COMMIT.
Enable source `track_commit_timestamp` identically for both candidates.
Read the real source commit timestamp through the marker row's `xmin` and `pg_xact_commit_timestamp`.
Do not substitute the client's COMMIT-return time for source commit time.

Observe probe visibility through fresh read-committed snapshots on the target.
Visibility proves target COMMIT, not target durability.
Measure source-commit-to-first-target-observation latency with a recorded polling interval and clock-offset bounds.
Report p50/p95/p99 together with observation quantization and sample count.
Require at least 1,000 observed source transactions per latency cell.
Otherwise mark that cell insufficient and refuse its performance claim.
The same observer and its overhead apply to both sides and their A/A controls.
Keep timing markers in private receipts, not engine diagnostics.

Use a ten-second unscored warmup, then a thirty-second scored CDC segment, followed by bounded drain and independent comparison.
Start catch-up from a pre-generated, exactly counted retained backlog with writers stopped.
Report changed rows and source transactions committed on the target per elapsed second until the exact backlog drains.
Do not use received LSN, queued records, decoding bytes, or source injection rate as the apply denominator.
Record steady-load lag, final lag/drain duration, target/source CPU, memory, WAL bytes, checkpoint updates, waits, and process RSS.
Reclamation, synchronization, restart, and cleanup costs remain separately visible and in total completion time where applicable.

Fork COPY stays asynchronous and native COPY retains durable per-table/part markers.
Do not disable native durability to equalize the raw COPY cell.
For the full-clone end-state comparison, issue a positively observed target transaction with `synchronous_commit=local` and a same-value UPDATE of the exact known ordinary logged probe row after all migration workers stop.
Require `UPDATE 1`, positive COMMIT, clean diagnostics, and checked connection closure.
Include that final barrier's time on both sides.
This matches only a final local-flush endpoint.
It does not match per-table crash recovery or synchronous-standby contracts.
Label native `durable` as the stronger ongoing contract in every comparison.

## A/A noise and selection rules

Predeclare and hash the schedule before measurements.
Use separate fresh processes and fresh owned source/target database instances for every repetition.
Use the same deterministic dataset and cache procedure on both halves of a pair.
Balance AB/BA order with a retained random seed and interleave candidates over time.
Do not run all fork repetitions followed by all native repetitions.

Run five A/A pairs per engine for each workload in the primary cell.
For each metric and workload, compute absolute paired log ratios.
Set its equivalence tolerance to the larger of the empirical 95th-percentile absolute A/A ratio and the measured timer/observer resolution bound.
Publish the resulting tolerance file and hash before opening A/B comparison receipts.
Do not import a percentage from the earlier performance estimates.
A/A results are specific to workload, artifact, hardware/storage, and cache profile.
If those change, their tolerances do not transfer.

For each admitted cadence, run seven randomized A/B pairs against fork default in the primary CDC cells.
Use a predeclared paired-log-ratio estimator and 10,000 seeded bootstrap resamples.
Report the median ratio, confidence interval, all pair values, and the A/A tolerance.
Do not delete a slow run or a failed pair.
A resource/fault contamination reason invalidates the complete pair, remains in receipts, and consumes its schedule slot.
Extra repetitions require a new approved schedule, not hidden retries.

The planning candidate grid is 100, 500, and 1,000 ms.
These are experiment settings, not selected production defaults.
After A/A, retain only candidates distinguishable at the observed timing resolution.
Build each as an exact private source-export variant that differs only in the one internal cadence constant.
Do not add an environment or CLI cadence override.
Record each patch/source hash and binary digest.

Select the lowest-latency replay-safe cadence whose throughput is within the A/A equivalence band of the best qualified candidate across both CDC shapes.
Require complete correctness/drain receipts and no latency or drain regression beyond that workload's A/A-derived bound against the other qualified replay-safe candidates.
Use the shorter interval to break a statistically unresolved tie.
If no candidate meets that rule, publish the unresolved trade-off and keep release admission closed.
A speedup over fork is a reportable result only when its confidence bound exceeds the relevant A/A tolerance.
No speedup is a condition for correctness qualification.
Report regressions candidly.

Run three confirmation pairs per workload on 16→16 and 16→18 using the selected cadence.
These are confirmation measurements, not a substitute for per-cell A/A controls when claiming a cross-version speedup.
Run native durable-versus-selected-replay-safe pairs separately and label the durability difference.
The default comparison remains fork default versus replay-safe.

Warm measurements use one declared full read of the owned source and target fixtures before the timed child.
Fresh containers and data directories do not prove a cold host cache.
Do not drop host caches, restart the shared VM, or label such a run cold without a separately approved isolation procedure.
Representative PVC and synchronous-standby storage results require the M7 environment and a separately declared storage cell.
Ordinary fresh owned fixture qualification is already within the approved roadmap.
It needs a leader-assigned resource window, not a newly invented blanket permission gate.
The local-VM campaign cannot close those claims or host power-loss qualification.

## Concrete benchmark approval packet

**Status: HOLD.**
No benchmark, load writer, VM reservation, new database, container, or resource cleanup was performed to prepare this plan.
The following is the exact proposed scope for user approval after artifact preparation.
The packet must contain actual hashes rather than unresolved artifact labels before execution.

### Binaries and command boundary

Freeze a private benchmark export outside all source checkouts.
Its lock file names the exact native release binaries for the retained candidate cadences, the native durable binary, the pinned fork executable, the matching client tools, the observer script, the server image digests, and the source manifests.
Builds and dependency fetches occur before a timed lease and are excluded from performance timing.
Do not build, pull, upgrade, or download during measurements.

The planned driver invocation is an argv array with this shape:

`[python3, tools/bench/m9.py, run, --approval, <private-approval.json>, --manifest, <hashed-m9.json>, --artifacts, <hashed-artifact-lock.json>, --phase, <one-phase-name>, --lease, <original-private-lease.json>, --receipt-root, <fresh-private-directory>]`.

This command is proposed for the new driver in Owner F's task.
It is not an existing runnable target and must not be executed before that task and approval.

The driver launches only the frozen executable's `clone`/follow command against its new owned databases.
Connections use private passfiles or environment references, without passwords in argv.
Native gets the existing `--durability=replay-safe` or `--durability=durable` option.
Fork receives no native flag.
Each gets a unique private workdir and disjoint source slot/publication names.
Use existing sentinel, comparison, and cleanup commands only within that exact child's newly created native state.
Never call native cleanup against fork state.
Retain fork workdirs and their receipts.

### Resource envelope

The VM is the existing `podman-machine-default`: 14 CPUs, 32 GiB memory, 128 GiB disk.
Use one heavy exclusive lease shared with M7 and image work.
Do not overlap M7 UP/load/E2E, image builds, other database qualification, or another benchmark.
The leader grants a fresh quiet window for each admitted phase.
No reservation is implied by this document.

Cap source and target at 2 CPUs and 4 GiB each.
Cap the runner, load writer, and observer together at 2 CPUs and 4 GiB.
The benchmark's total hard envelope is 6 CPUs and 12 GiB, with no swap.
Verify actual Podman cgroup caps before starting the first child.
Keep at least 2 VM CPUs and 6 GiB available after existing original-service reservations.
Existing services remain running and unchanged.
If measured existing commitments do not fit, refuse rather than stop or resize them.
Reserve host Cargo headroom separately: at most two host build jobs and no simultaneous broad Cargo/link job during a timed cell.
Host memory pressure, new swap growth, VM OOM, or missing resource observations invalidate the active pair.

Require at least 32 GiB free VM disk before each phase and 20 GiB throughout it.
Limit phase-owned disk growth to 12 GiB, including source/target data, retained source WAL, fork state, logs, and receipts.
Use a monitored envelope and say that it is not a filesystem quota.
Configure the owned source slot's finite WAL allowance within that envelope and fail on lost/unreserved WAL.
A child may use at most 120 seconds including preparation, warmup, scored work, drain, and oracles.
A phase has at most 7,200 seconds, without renewal.
A timeout is a failed receipt, not a passing truncated result.
Use a watchdog that can terminate only recorded phase-owned processes and containers.

Propose seven serial phase windows, with no automatic lease renewal:

1. `aa-copy-18-18`: 40 child runs, at most 4,800 child-seconds plus bounded preparation.
2. `aa-cdc-18-18`: 40 child runs, at most 4,800 child-seconds plus bounded preparation.
3. `ab-cadence-100-18-18`: at most 28 CDC child runs.
4. `ab-cadence-500-18-18`: at most 28 CDC child runs.
5. `ab-cadence-1000-18-18`: at most 28 CDC child runs.
6. `confirm-16-16-and-16-18`: at most 48 child runs across the four workload profiles.
7. `copy-and-durable-comparisons-18-18`: at most 28 paired COPY child runs against fork default, plus 28 durable-versus-selected CDC child runs.
The 56-child maximum consumes at most 6,720 child-seconds and leaves at most 480 seconds for preparation within this window.

Each phase is bounded by its original 7,200-second lease, even when its child maxima would leave unused time.
The campaign maximum is 14 elapsed VM-hours over those seven independent windows.
This is an authorization cap, not an ETA or a claim that measurements need all fourteen hours.
Do not add a cold-cache, PVC, standby, power-loss, relaxed, or binary-pgoutput phase to this packet.

### Database ownership, reset, and cleanup

Create one fresh private internal Podman network per phase, with no published host port.
Record its actual ID and exact ownership label before further work.
Create only recorded source, target, and runner containers, with phase-owned bind-mounted data directories under a new private root.
Use no anonymous persistent volume and no privileged container.
Preserve original PostgreSQL containers, endpoints, M0 archives, failed RUN evidence, images, caches, volumes, backups, and all other projects.

Each child creates new harness-owned databases and engine state from the declared seed.
Before a reset, verify the phase/container identity and exact database OID/name inventory.
The approval covers dropping only those new benchmark-owned databases.
If identity changes or a database is not in the original creation ledger, stop and retain it.
Do not use broad `DROP`, `prune`, name-prefix deletion, or cleanup of a replacement resource.

Cleanup may remove only the exact recorded phase-owned containers and network after checking their labels and identities.
Delete no volume, image, cache, preserved data, or workdir.
Reuse the two phase-owned PostgreSQL clusters while creating fresh child databases.
The approval packet must explicitly include resetting only the new child databases and their exact recorded benchmark-created slots/publications after all child processes stop.
Verify database OIDs, slot database bindings, publication OIDs, and the creation ledger before each reset.
This reset does not open, adopt, resume, convert, or clean any pre-existing pgcopydb state or resource.
Retain fork/native workdirs, final phase-owned PostgreSQL data directories, and receipts within the disk envelope for review.
Cap cumulative retained campaign growth at 24 GiB and stop before exceeding it.
Verify absence of each removed resource and continued identity/availability of original services.
A failed cleanup means an incomplete phase, with the exact owned resources handed back to the leader.
Any later removal of retained directories needs a separate explicit handoff.

### Reproducibility receipts

Retain a private 0700 root with 0600 files and no-clobber publication.
Bind each run to the approved packet hash, original lease, schedule index/order/seed, exact argv without secrets, binary/source/tool/image hashes, CPU architecture, kernel/filesystem/storage facts, actual server settings, data manifest, limits, and cache procedure.
Record wall-clock timestamps and monotonic durations.
Record process PID/start identity and peak RSS per fresh child.
Keep source-COMMIT count, target-visible committed count, rows changed, latency observations, synchronization proof, exact E, final drain, content/schema fingerprints, exit code, fault evidence, and cleanup identity checks.
Redact DSNs, credentials, and row contents.
Use hash-linked summaries derived from original receipts.
A changed fixture, binary, schema, or schedule invalidates the corresponding comparison and requires a separately approved rerun.

## Required durability documentation matrix

`docs/durability.md` must explain the following for both available modes.
Keep process/host/target failures distinct and name the required recovery evidence.

- Engine process kill, target still running: uncommitted data rolls back.
Durable acknowledged groups already have synchronous proof.
Replay-safe committed but unproven groups may remain visible, but restart must establish the fenced writing barrier before feedback.
- Engine-host power loss with a separate target: target behavior depends on whether that target also failed.
Do not describe loss of the client host as a target power-loss test.
- Target host power loss or PostgreSQL crash: durable proven groups survive when storage honors the configured flush contract.
Replay-safe unproven asynchronous groups may disappear, and the slot must retain their source input.
Recover from the surviving logged checkpoint through a new positive barrier, or refuse a gap.
- Lost data-group or barrier COMMIT response: the outcome is unknown.
Reacquire the writer fence, validate exact target identity/row, write the synchronous barrier, and reconcile before continuing.
- Target failover with the required synchronous standby: proven data survives only when the promoted standby satisfies the deployment's synchronous durability contract.
Revalidate target identity and the exact surviving checkpoint.
Local Podman evidence alone does not qualify CNPG failover.
- Target failover without synchronous durability: acknowledged target changes may be absent on the promoted node.
If source confirmed-flush exceeds recovered authority, refuse with exit 3 and require a fresh migration.
Do not offer an accept-loss success path.
- Source WAL exhaustion, lost slot, missing/changed publication, source identity/timeline change, or lost required native state: refuse when the retained input and bindings cannot prove safe recovery.
Explain source disk growth during COPY and replay-safe lag.
Never adopt a replacement resource or rewind a confirmed source frontier.
- Normal completion and cutover: both modes force complete target durability, required final sequence/large-object synchronization, checked closure, and a durable completion receipt.
Replay-safe never returns successful incomplete work.

Include the conditional relaxed row without claiming it exists.
Its approved contract requires both explicit finite transaction and canonical-byte caps, positive target COMMIT before speculative ACK, and durable D/H/epoch/reservation accounting across the entire migration run and every database/standalone phase.
D is the proven durable prefix.
H is a reservation-authorized upper bound, not evidence of COMMIT.
Leaked capacity stays reserved until positive proof releases it.
Oversized transactions become durable before source release.
One database's proof cannot release another database's reservation.
A gap has no exit-zero accept-loss path.
Encoded-byte caps do not bound business effects such as TRUNCATE.
A cadence does not promise a maximum loss window in wall-clock seconds.

PostgreSQL's documented commit semantics support the distinction between logical COMMIT success, local WAL flush, and standby durability.
See [PG16 WAL settings](https://www.postgresql.org/docs/16/runtime-config-wal.html), [PG18 WAL settings](https://www.postgresql.org/docs/18/runtime-config-wal.html), and [PG18 asynchronous commit](https://www.postgresql.org/docs/18/wal-async-commit.html).
The application recovery and refusal requirements come from the approved project design.
Do not claim storage or failover guarantees beyond the tested platform assumptions.

## Conditional work and remaining decisions

### Relaxed

Keep `--durability=relaxed` as an explicit pre-mutation unsupported refusal unless a separately reviewed measured-gain path qualifies.
Do not implement reservations or speculative ACK merely to fill this milestone.
The current packet measures only durable and replay-safe.
A faster no-barrier diagnostic would be an upper-bound experiment, not a measurement of a correct relaxed implementation.
It cannot authorize relaxed release claims.
If retained results justify further work, prepare a separate concrete prototype/measurement plan that preserves the complete approved D/H/epoch/cap contract before requesting its resource approval.
If gain is absent, record relaxed as intentionally unavailable under the roadmap condition.

### Binary pgoutput

Keep text pgoutput v1 and its exact current plugin options in this plan's product path.
Only measured decode/encoding cost can justify a separate binary-pgoutput task.
That task must define a per-stream built-in-type allowlist, verify every source/target major pair, preserve text for any ineligible stream, and update exact plugin-option admission.
M8's per-table binary COPY predicate does not automatically qualify pgoutput binary decoding.
No new plugin option or claim follows from the optional roadmap item alone.

### Power loss and representative storage

The shared VM must not be power-cycled or faulted by this packet.
Real or simulated storage power-loss qualification needs a separate isolated environment that cannot affect original services or backups.
PVC and synchronous-standby tests depend on M7 and must state their own resource envelope.
Fresh owned fixtures fall within the approved milestone scope.
Mutating retained PVCs, stopping original services, or powering off the shared VM requires separate concrete approval.
The docs may describe approved semantics while clearly marking missing physical qualification.
M9/M10 cannot close those runtime claims from process-kill tests.

## Acceptance criteria and execution handoff

- [ ] M8's exact future native-9/metadata-7 handoff is frozen before M9 ownership starts.
- [ ] Durable remains default, replay-safe feedback uses only positive synchronous proofs, and all normal success remains durable.
- [ ] Stored native-format-10 settings survive resume and appear in help/status through the qualified capability extension.
- [ ] Old operators refuse new capability/payload before worker start in the final production handshake test.
- [ ] The complete deterministic fault inventory passes on PG16 and PG18, with full three-pair engine evidence and independent contents.
- [ ] A/A tolerances precede A/B interpretation, and every accepted metric counts target-committed work.
- [ ] The selected cadence is supported by retained paired receipts, rather than an estimate or an invented constant.
- [ ] COPY reports preserve the asynchronous-fork/per-table-durable-native distinction and final-local-barrier limit.
- [ ] Relaxed and binary pgoutput remain conditional and explicitly unavailable unless their separate gates pass.
- [ ] Documentation contains the complete failure matrix, recovery procedures, source-WAL costs, configuration precedence, measured limits, and missing qualification boundaries.
- [ ] Final `task check lint test`, required live profiles, applicable Go CI/suites, exact-image matrix, and leader-assigned independent review pass on the frozen release candidate.
- [ ] The leader records evidence in `tasks/todo.md`; no main push, merge, tag, release, GHCR publication, production deployment, or broad prune is implied.

Self-review completed against the M9 roadmap and approved design.
The explicit unresolved gates are benchmark approval/artifact lock, measured cadence selection, M6–M8 predecessor closure, and the required isolated power-loss/PVC/standby evidence.
Only operations beyond the existing owned-fixture scope create a new permission gate, including shared-VM power cuts or retained-PVC mutation.
This plan contains no measured speedup or new default cadence claim.
