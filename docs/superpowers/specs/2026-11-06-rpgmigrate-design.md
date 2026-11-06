# rpgmigrate approved design

The user approved this design and delegated team execution on 2026-11-06.
The review resolutions below supersede the draft's durability and completion wording.
The [task status](../../../tasks/todo.md) records implementation evidence and unfinished work.

## Scope and compatibility

We rebuild the migration engine in Rust for PostgreSQL operators who need safe migrations and measured WAL application performance.
The executable is `rpgmigrate`.
The engine has no runtime dependency on the pgcopydb executable.
The repository is `/Users/ydixken/development/rpgmigrate`.
The behavioral baseline is `ydixken/pgcopydb` on `origin/v0.18-fixes`, commit `86bf908adca02d87063849dae775a02c1659c829`.
The completed audit covered 453 issues and 1,088 discussion comments across upstream, the fork, and the operator.
Do not repeat that audit.

Preserve registered commands, aliases, flags, short-option distinctions, existing environment names, precedence, and exit-code categories.
This includes `PGCOPYDB_*` inputs, filters, machine output, standalone phases, intentional safety refusals, and existing apply-group limits.
Preserve all three decoding plugins, all-databases, large objects, roles, extensions, comparison, progress, and deferred indexes.
Parser-derived conformance fixtures are authoritative.
Help text alone does not define compatibility.

Intentional differences are executable/version/log/application identity, native state, corrected false-success and serialization bugs, native operator evidence, and additive durability/concurrency controls.
Default roots are `TMPDIR/rpgmigrate` and `XDG_DATA_HOME/rpgmigrate` with documented platform fallbacks.
Explicit `--dir` retains its meaning.
Record intentional differences in compatibility fixtures.
Unsupported commands and combinations must fail explicitly until their implementation passes tests.
An implemented parser does not prove an implemented migration.

## Native state and resource ownership

Never import, open as a migration, resume, restart, convert, delete, or clean pgcopydb state or resources.
Native state carries a magic, version, run identity, and canonical directory identity.
Check ownership and identity before every mutation, including `--restart`.
Reject foreign nonempty directories, mixed states, symlinks, and replacement races.
Do not silently adopt or delete existing resources named by the user.
Record newly created source and target resources as owned by the exact native run.

Target bookkeeping uses an owned private schema and logged tables.
Check the recorded run identity and object identities before use, comparison exclusion, ownership changes, or cleanup.
Reject namespace collisions.
Exclude only those exact owned objects from filters, comparison, and `ownerAfterRestore`.
A similar schema name does not establish ownership.
Control and sentinel state remain local.
Do not create a new source sentinel table.

## Architecture and transport

Use one Cargo package with a thin binary and private modules.
Modules cover CLI/config, catalog/planning, PostgreSQL transport, snapshot/COPY/schema, CDC decode/apply, persistence/recovery, and progress/comparison.
Rust owns scheduling, representations, parsing, recovery, and errors.
Keep integrations behind narrow boundaries.
Configuration is external, and diagnostics must not contain secrets or row data.

We use a narrow native libpq boundary, which the user explicitly approved.
Give each connection, result, and COPY buffer one owner with audited cleanup.
Copy libpq error text before its owner becomes invalid.
Preserve conninfo, service files, passfiles, authentication, and TLS through libpq.
Do not implement custom authentication or TLS.
Retain `pg_dump` and `pg_restore` with existing `PG_CONFIG`/`PATH` selection and explicit version checks.

Forward raw COPY buffers without row conversion unless required.
Check binary compatibility by type and version before enabling binary COPY.
Keep the established fallback.
Check terminal COPY results on both connections before recording completion, as required by [libpq's COPY protocol](https://www.postgresql.org/docs/18/libpq-copy.html).
After an uncertain COMMIT, do not claim that rollback occurred.

Bound workers, connections, catalogs, caches, queues, and byte budgets across all databases.
Include libpq buffers, socket buffers, pipeline results, and one oversize row or frame in memory accounting.
Spill large transactions instead of collecting them in memory.
Normal clone/follow runs inside one process with bounded tasks or threads.
Standalone receive/prefetch and catchup/apply may overlap in separate processes.

## Ownership across processes

Use one ingest writer and one coordinated apply owner per database/stream.
Hold OS locks, source slot exclusivity, and a target session/advisory fence.
Do not infer liveness from a PID alone or add worker election.
Use backend transactions for catalog and control changes.
Progress reads never mutate state.

The metadata backend must support the actual overlapping-process topology.
Check redb's selected version and lock modes before accepting it.
Its [database API](https://docs.rs/redb/latest/redb/struct.Database.html) documents file-lock and single-writer behavior that must be tested across processes.
Local control notifications wake an owner, but durable control records remain authoritative.
Preserve existing `--host`/`--port` control semantics with bounded versioned native messages.
No C-struct wire compatibility or permanent new service is required.

## Persistence experiment

Compare three candidates with identical packed event encoding and transaction semantics.
Keep candidate implementations private to the experiment rather than building a generic backend framework.

1. A checksummed segmented CDC journal with compact transactional catalogs, commit descriptors, and control state.
2. A redb-only store with chunked event blocks and transaction descriptors.
3. SQLite control with FULL durability, WAL, prepared statements, bounded batches, and packed BLOB blocks.

The segmented journal is a hypothesis, not a selected winner.
SQLite may win when alternatives lack repeatable value.
Do not add RocksDB without measured need.
Check effective SQLite settings because unknown pragmas do not fail, as documented by [SQLite](https://www.sqlite.org/pragma.html).
Do not maintain another durable representation of generated SQL.
Preserve debug/export and SQL-stdin command contracts.

Journal identity contains run, source, database, stream generation, transaction identity, and event ordinal.
LSN or XID alone is insufficient.
Persist decoder/relation context needed to recover acknowledged input.
Replay complete checked transaction descriptors in source commit order.
An exclusive large transaction may stream into an uncommitted target transaction.
It cannot publish commit progress until its complete input passes validation.

Synchronize journal bytes and directory operations before metadata publishes references.
Allow chunks to span segments without one huge storage transaction at source COMMIT.
Use atomic metadata transitions and file/directory synchronization for publication and retirement.
Prune only after durable target completion, with no open-transaction dependency or reader pin.
Discard only uncommitted, unpublished incomplete tails.
Corrupt required history is a failure.

Account for journal, catalog, WAL, temporary files, and reclamation reserve.
Disk exhaustion stops feedback advancement and ingest at a recoverable boundary.
Keep heartbeats at the last permitted frontier and report capacity failure.
Do not wait for a transaction to commit when that transaction needs the space its own pruning would release.

## Positions and target authority

Use separate types for source commit-order LSN, source transaction-end LSN, and target-local WAL LSN.
The first orders source commits.
The second marks the complete source transaction boundary used for release and recovery.
The third belongs to the target server and cannot be compared with source LSNs.
The [logical protocol](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html) carries distinct commit and transaction-end fields.
Plugin adapters must preserve their meanings rather than substitute a requested end position.

Report received, journal-durable, target-committed, target-durable, and source-acknowledged positions separately.
Define legacy output aliases explicitly.
Keep each frontier contiguous within its stream.
Do not compare independent database streams as one scalar LSN.

Each replay transaction or permitted group updates a logged target checkpoint in the same transaction as user changes.
The checkpoint is authoritative for recovery.
Replication origins alone are insufficient, including the failure described by operator issue 359.
Keep one progress row per stream and update it per committed group, not per row.
Measure checkpoint WAL volume, bloat, and contention.
An asynchronously visible checkpoint does not prove durability.

Resolve unknown COMMIT only after acquiring the target writer fence and reading the checkpoint on the target PRIMARY.
Replay only when complete retained input proves it safe.
Missing required input causes an explicit gap failure.
Nontransactional DDL, sequences, and vacuum must reconcile actual target state instead of trusting local markers.

COPY completion may share a transaction with a logged checkpoint for a valid plan and snapshot.
Only then may recovery reuse finished chunks.
Preserve refusal when an unfinished follow COPY loses its snapshot.
After target crash, revalidate or recopy unlogged tables.
Keep existing refusals for unsafe follow combinations.

## Target privileges and guarantee boundary

The user explicitly approved the native dependency and target metadata privilege requirement.
Preflight requires target CREATE, or preprovisioned metadata with USAGE plus SELECT/INSERT/UPDATE.
The normal operator clone role already has CREATE.
Restricted data-only/apply roles may use preprovisioned bookkeeping.
Check cleanup ownership and privileges separately.

The no-loss and duplicate-prevention guarantee covers transactional logged PostgreSQL data under the stated storage assumptions.
Preserve supported triggers with serial or fallback behavior.
External and nontransactional trigger effects can repeat during recovery.
Document their need for idempotency.
Do not claim unconditional exactly-once effects.

## Apply performance and ordering

Preserve transaction content, atomicity, and source commit order.
Prepared execution and pipeline support already exist in the C baseline.
Optimize their reuse, binding, cache behavior, bounded batches, and waits rather than presenting them as new discoveries.
Bulk operations must preserve repeated keys, key changes, replica identity FULL, NULL, unchanged TOAST, generated columns, and partition movement.
Account for triggers, constraints, and schema generation.

The default `--apply-jobs` value is `1`.
Use the serial applier as the differential reference.
Parallel execution schedules only conservatively independent groups.
Unknown dependencies, cross-table effects, DDL, TRUNCATE, and unsafe shapes require barriers or serial execution.
The coordinator grants checkpoint/commit permission in source order and advances only a contiguous committed prefix.
After an earlier failure, roll back later uncommitted work.
Reserve queue capacity so later work cannot prevent prerequisites from completing.
Incomplete parallel support is not production-ready.

## Durability profiles

Profiles apply to CDC.
COPY and schema completion are durable in every profile.
Positive target COMMIT acknowledgment is required before transaction feedback releases source WAL.
No profile releases source WAL merely because work entered a queue.
No-work keepalive advancement requires a separately tested proof that it crosses no pending source transaction.

### Durable

`durable` is the default.
Synchronize required journal input and target commits.
Gate source release on durable target progress.
Acknowledged transactional logged changes survive process, host, and target crashes under the stated storage assumptions.
Losing the spool does not delete target commits already acknowledged as durable.
Loss of all migration state does not promise resumability.

### Replay-safe

Make the journal and required decoder context durable before source acknowledgment.
Target commits may use asynchronous commit, followed by periodic synchronous writing barriers.
Retain input until target durability is proven.
Visible checkpoint rows alone do not permit pruning.
Recovery can replay more work without losing acknowledged changes when the durable journal survives.
Releasing source WAL makes spool-volume loss a separate failure risk.
Expose receive, flush, and apply frontiers accurately to the native operator.

A barrier updates a logged bookkeeping row and commits with synchronous durability after all covered target commits.
It must generate WAL.
A read-only transaction or empty COMMIT is not a barrier.
Record which fenced target and contiguous stream prefix each barrier covers.
PostgreSQL distinguishes logical commit success from persistent WAL in its [asynchronous commit documentation](https://www.postgresql.org/docs/18/wal-async-commit.html).

### Relaxed

`relaxed` requires explicit opt-in and both finite `--loss-max-transactions` and `--loss-max-bytes` values.
Do not supply implicit unlimited values or defaults for these loss caps.
Count source transactions rather than apply groups.
Count the canonical encoded bytes for every reserved source transaction.
Encoded-byte caps do not bound business effects such as TRUNCATE.
Flush cadence is a scheduling target, not a maximum wall-clock loss window.

The budget belongs to one native migration run/workdir across all databases and standalone phases.
Independent runs have separate budgets.
Use durable risk reservations before target work reserves speculative capacity and before any speculative ACK.
A dirty boolean is insufficient.
Persist the epoch, per-stream D and H, aggregate transaction count, aggregate encoded bytes, and individual reservation identities atomically.

D is the proven durable target prefix for each stream.
H is the upper bound on source feedback allowed by durable reservations for each stream.
H may exceed feedback actually sent when a worker fails.
Never treat H as evidence that a transaction committed.
Check both aggregate limits with overflow-safe arithmetic before reserving.
Worker failure leaks capacity until recovery proves release is safe.
It must never create extra budget.

Require positive target COMMIT before sending feedback within a reservation.
An oversized transaction must become durable before source release.
When capacity is exhausted, stop speculative advancement and force writing barriers or wait at the safe frontier.
Release reservations only after durable target proofs and an atomic durable update of D and the aggregate budget.
Different databases require their own target proofs.
One database's barrier cannot release another database's reservations.

Crashes can require replay or a completely fresh migration.
Reconcile target checkpoint, reservations, and retained complete input before resuming.
If they cannot prove recovery, return a nonzero gap refusal and require a new migration.
There is no accept-loss exit-zero path.

## Completion, changes of mode, and failure documentation

Every successful normal completion or cutover drains work, synchronizes target and state, and publishes a durable completion receipt.
Prove the complete requested prefix on every database and all required phases.
Never replace actual completion with requested endpos.
Persist the effective profile and epoch.
Resume uses stored settings.
An explicit mode change requires a quiesced durable boundary across the run.
Relabeling an uncertain relaxed epoch does not establish safety.

Document process crash, host power loss, target crash, spool-volume loss, and failover for each profile.
For `durable`, acknowledged logged target data survives the stated node failures, but missing state can prevent resume.
For `replay-safe`, recovery of asynchronous target loss requires the durable spool or provably retained source input.
For `relaxed`, missing proof causes refusal and potential remigration within the explicit transaction/byte risk bounds.
Spool loss, source retention, external effects, and multiple simultaneous losses require separate explanations.
No profile guarantees arbitrary asynchronous cluster failover.
Document source WAL/disk costs, throughput, latency, replay cost, configuration precedence, unsafe examples, and recovery procedures.
Choose other finite buffers and flush cadences from benchmark data and expose their effective values in help and status.

## Compatibility and operator work

Keep the Rust engine, native operator adapter, and verification/release harness as separate workstreams with separate write ownership.
Existing operator migrations retain their original pgcopydb runtime.
New rpgmigrate jobs require fresh native state.
No binary alias or automatic state adoption is allowed.

The adapter changes executable/workdir identity, version checks, log/application matching, mode and lag fields, and completion evidence.
It also checks metadata privileges and exact owned-object exclusions.
Keep its diff and verification gate separate.
Read current operator instructions before editing it, without copying mutable tasks or policies into this repository.
Preserve its CI-first functional-test policy.

Preserve the fork's PG16-18 suite, PG9.6/test_decoding, Timescale PG13, and operator source14-18 to supported target17/18 cases.
Do not infer a universal cross-product from version ranges.
Assert actual server versions and matching utility bundles in tests.
A client dependency must not silently raise the source floor.

## Verification and performance gates

CLI fixtures cover registrations, aliases, environments, malformed input, stdout/stderr, JSON, exit categories, and phase composition.
Record corrected bugs as explicit exceptions.
Use independent SQL fingerprints for data, schema, sequences, large objects, ownership, and partitions.
Negative harness tests must reject empty fixtures, wrong versions, wrong failure reasons, missed injection, skipped cases, and exit-zero mismatches.

Inject faults around COMMIT, checkpoint writes, publication, ACK, pruning, process ownership, disk full, short/corrupt writes, snapshot loss, restart, and mode changes.
Distinguish process kill from power-loss simulation.
Check lost and duplicate transactional effects, plus bounded loss/refusal under relaxed mode.
Test scheduler conflicts, foreign keys, ALWAYS triggers, key changes, cycles, cancellation, rollback, reconnects, and commit order.
Compare against serial execution under adversarial schedules and mutations.

Benchmark the pinned fork and Rust candidates with repeated paired randomized runs under identical workload and durability/failover contracts.
Separate warm and cold runs, local SSD, and representative operator storage.
Measure decode/storage costs, target CPU/index work, round trips, pipeline waits, and commit latency.
Report target-committed rows/transactions per second, sustained catch-up rate, p50/p95/p99 commit-to-apply latency, and migration duration.
Include complete drain, synchronization, reclamation, and recovery costs.
Received or queued LSN throughput is not apply throughput.
Publish noise-derived tolerances before accepting results.
Storage selection requires correctness plus repeatable value without material apply or migration regressions.
No arbitrary speedup is promised.

CI gates include Rust format/lint/tests, parser/state fuzzing, FFI ownership/sanitizers, dependency/license/security checks, and documentation drift.
Add STE100 hard-rule checks, database integration, crash suites, cross-version cases, and bounded runtime/size fixtures.
Published artifacts require isolated operator E2E.
Promote the exact tested binary/image digests without rebuilding.

## Platforms, workflow, and exclusions

Initial production targets are Linux amd64/arm64 with filesystems honoring locks, atomic publication, and synchronization.
macOS is a development target, not proof of PVC performance or power-loss durability.
Shared multi-host writers and arbitrary network filesystems are outside the guarantee.

Use ordinary feature branches in the primary checkout, never worktrees.
Preserve existing work in old repositories and the operator.
Apply ponytail full, humanizer/deslop, and STE100 to code planning and prose.
Ship tests and current documentation with behavior changes.
The README is an index written when the working scaffold exists.
Keep `publish = false` until licensing and release gates permit publication.
Preserve third-party notices and let the readiness worker check the dependency graph.

Builder decisions include narrow module names, vetted crate versions, codec/checksum details, benchmark-selected storage, measured budgets, and implementation sequence.
Escalate changes that violate these contracts.
Non-goals are legacy import, a pgcopydb alias, universal DDL replication, custom auth/TLS, distributed cross-node scheduling, and a generic backend plugin framework.
