# rpgmigrate approved design

The user approved this design and delegated team execution on 2026-11-06.
On 2026-11-07, the user accepted the operator-first release contract in the [maintained compatibility matrix](../../operator-compatibility.md).
That scope revision supersedes full-CLI parity requirements; it does not weaken the COPY, WAL, ownership, durability, or recovery contracts below.
The dated revision below reconciles the approved [roadmap amendments](../../roadmap.md), which supersede earlier design decisions where they differ.
The [task status](../../../tasks/todo.md) records implementation evidence and unfinished work.

## Revision approved 2026-11-08

We use the source replication slot as the retained input log, without a local CDC journal or copy-phase spool.
The authoritative target checkpoint and certified no-work frontier govern feedback and recovery.
The revised profile uses pgoutput protocol v1, threaded receive/apply, pipelined apply groups, owned per-table COPY, and durable drain proof as specified below.
We restart the data phase after a pre-anchor follow-copy crash only for tables whose creation this run recorded.
We use stock PGDG libpq and PostgreSQL client tools 16 and 18.
The native release profile accepts source and target majors 16 and 18 with source ≤ target: 16→16, 18→18, and 16→18.
Refuse other pairs and unsupported profile options before mutation; `list tables` remains an explicit staged refusal.
The complete option, environment, protocol DTO, application-name, and workdir contracts freeze in M1; this revision does not invent their exact representation.
Parallel build owners may use git worktrees off `feat/native-engine`, overriding the earlier no-worktree rule.
M0 removes the storage experiments, retired journal tests, patched-libpq pipeline, and custom image pipeline; M2 and M3 own the later COPY and CDC replacements.
Retained component code and historical benchmark receipts do not qualify the revised native runtime.
Follow the [roadmap milestones](../../roadmap.md#milestones) and [task status](../../../tasks/todo.md) for implementation evidence.

## Scope and compatibility

We rebuild the migration engine in Rust for PostgreSQL operators who need safe migrations and measured WAL application performance.
The executable is `rpgmigrate`.
The engine has no runtime dependency on the pgcopydb executable.
The repository is `/Users/ydixken/development/rpgmigrate`.
The behavioral baseline is `ydixken/pgcopydb` on `origin/v0.18-fixes`, commit `86bf908adca02d87063849dae775a02c1659c829`.
The completed audit covered 453 issues and 1,088 discussion comments across upstream, the fork, and the operator.
Do not repeat that audit.

The operator inventory covers eight paths: `clone`, `stream cleanup`, `stream sentinel set endpos`, `stream sentinel get`, `list progress`, `list tables`, `compare data`, and `compare schema`.
Preserve their 43 emitted behavioral option names, consumed outputs, existing environment precedence and exit categories, plus general help, version, ping, and explicit connection/work-directory controls.
The other 40 behavioral paths are deferred parity requirements, not a universal CLI compatibility promise.
Retain the registered parser baseline, aliases, and short-option distinctions without removing flags as part of this scope change.
Parser-derived fixtures remain authoritative for parsing, not proof that every registered operation is implemented or required for release.
Every selected CRD feature must have its full semantics; staged profiles must explicitly refuse unsupported configurations during admission or preflight before mutation.
This includes selected plugins, all-databases, large objects, roles, extensions, filters, binary fallback, parallel/split COPY, comparison, progress, deferred indexes, and apply grouping.
Default follow uses `pgoutput`, and large objects are included unless explicitly skipped.
The bounded `test_decoding` core gate does not qualify default operator follow, and silent plugin remapping is forbidden.
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
Control and sentinel requests remain in the native workdir; the logged target checkpoint is authoritative for applied and certified progress.
No replication origin is created.
Accept and record `--origin` without owning it; cleanup reports it as not owned.
Cleanup removes only this run's owned source slot and publication and preserves the workdir for later comparison.
After creation, a missing slot or publication, or a changed publication OID, is a gap refusal rather than permission to recreate it.
The explicit pre-anchor data-phase restart below is the exception for replacing recorded owned resources.
Keep FAILOVER slots disabled.
Do not create a new source sentinel table.

## Architecture and transport

Use one Cargo package with a thin binary and private modules.
Modules cover CLI/config, catalog/planning, PostgreSQL transport, snapshot/COPY/schema, CDC decode/apply, persistence/recovery, and progress/comparison.
Rust owns scheduling, representations, parsing, recovery, and errors.
Keep integrations behind narrow boundaries.
Configuration is external, and diagnostics must not contain secrets or row data.

We use stock PGDG libpq 16 and 18 through the approved narrow native boundary.
Do not require patched sources, a private capability symbol, or a custom artifact receipt.
Keep passfile/service authentication checks, while allowing stock libpq to emit its own stderr warnings.
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
Use pgoutput protocol v1 without streaming, with exactly `proto_version '1'` and `publication_names` plugin options.
The source walsender spills large transactions and delivers each complete transaction in commit order.
Stream its rows into the open target transaction without collecting the transaction in memory.
Treat any NoticeResponse on the replication connection as fatal.
Normal clone/follow runs inside one process with bounded tasks or threads.
Live control and read-only progress observations may overlap the worker and access its native-owned workdir.
Final verification and cleanup require worker quiescence.
Concurrent rehearsal uses isolated scratch storage.
Broader standalone receive/prefetch/catchup/apply CLI parity is deferred; retain the internal process-ownership and state qualification gates.

## Ownership across processes

Use one ingest writer and one coordinated apply owner per database/stream.
Hold OS locks, source slot exclusivity, and a target session/advisory fence.
Do not infer liveness from a PID alone or add worker election.
Use backend transactions for catalog and control changes.
Progress reads never mutate state.

Local control notifications wake an owner, but durable control records remain authoritative.
Use bounded versioned native control for the selected operator contract.
Unconsumed standalone `--host`/`--port` CLI compatibility is deferred.
No C-struct wire compatibility or permanent new service is required.

## Source retention and initial copy

In `durable` and `replay-safe`, the source slot retains all WAL not yet proven durable on the target, including WAL generated throughout the initial copy.
The conditional M9 `relaxed` experiment below would permit bounded release through explicit durable reservations; it is not an available mode claim.
Preflight checks `max_slot_wal_keep_size`, source disk headroom, PG16 `old_snapshot_threshold`, and PG18 `idle_replication_slot_timeout`.
Poll slot `wal_status` and refuse on `lost`.
Source WAL loss is a recovery gap; local state cannot replace missing input.
The removed journal/redb/SQLite storage experiments and retained benchmark receipts are historical evidence, not active backend selection gates.

Create an owned publication with the plan's explicit `FOR TABLE` list and `publish_via_partition_root=false`.
Create the replication slot with `EXPORT_SNAPSHOT` and keep its replication connection idle with session timeouts disabled.
Run `pg_dump --snapshot=<token>` for pre-data and post-data in one archive, then restore pre-data with `--no-publications --no-subscriptions`.
Require `pg_publication_tables` to equal the copy plan.
Start `--table-jobs` workers that import the snapshot and copy unsplit tables with `BEGIN; TRUNCATE; COPY ... FREEZE; done marker; COMMIT`.
Release the exporter only after every worker has imported the snapshot.
Restore post-data, set sequences, and run ANALYZE before starting apply.
Relay COPY in 64 KiB buffers and flush only on backpressure or at the end.
Set `row_security=off` on source sessions so hidden rows cause errors.
Client tools match the target major; refuse a source major above the target major.
Binary COPY, split parts, and large objects widen the initial vertical slice in M8, with explicit refusal before they are qualified.
Large objects require a final re-sync under the write freeze after drain because logical decoding does not carry their changes.

## Positions and target authority

Use separate types for source commit-order LSN, source transaction-end LSN, and target-local WAL LSN.
The first orders source commits.
The second marks the complete source transaction boundary used for release and recovery.
The third belongs to the target server and cannot be compared with source LSNs.
The [logical protocol](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html) carries distinct commit and transaction-end fields.
Plugin adapters must preserve their meanings rather than substitute a requested end position.

Report received, target-committed, target-durable, and source-acknowledged positions separately.
There is no local journal-durable frontier.
Define legacy output aliases explicitly.
Keep each frontier contiguous within its stream.
Do not compare independent database streams as one scalar LSN.

Each replay transaction or permitted group updates a logged target checkpoint in the same transaction as user changes.
The checkpoint is authoritative for recovery.
Replication origins alone are insufficient, including the failure described by operator issue 359.
Keep one logged control row per stream in an owned schema and CAS-update it in each applied group's transaction.
Record `applied.end`, `certified_lsn`, the copy anchor, source system identifier, and timeline.
Keep `end_lsn` strictly increasing per stream.
Advance `certified_lsn` only from a keepalive received after every durable transaction, through a rate-limited logged synchronous barrier before feedback passes `applied.end`.
Construct feedback only from an applied commit end or certified keepalive; it is monotonic and never below the `confirmed_flush` read at connect.
On connect, refuse with exit 3 (gap) when `confirmed_flush_lsn > max(applied.end, certified_lsn, anchor.consistent_point)`, then start from that maximum.
Measure checkpoint WAL volume, bloat, and contention.
An asynchronously visible checkpoint does not prove durability.

Use a per-run advisory fence key.
Resolve unknown COMMIT only after reacquiring the target writer fence, reading the checkpoint on the target PRIMARY, and writing a synchronous barrier.
A synchronous-replication "canceling wait" WARNING is an unknown outcome.
Replay only when complete retained input proves it safe.
Missing required input causes an explicit gap failure.
Nontransactional DDL, sequences, and vacuum must reconcile actual target state instead of trusting local markers.

COPY completion may share a transaction with a logged checkpoint for a valid plan and snapshot.
Only then may recovery reuse finished chunks.
Resume before the follow-copy anchor restarts the data phase with a new copy generation.
Drop only this run's owned slot and publication, create replacements with a new snapshot, truncate all plan tables in one statement, unlink copied large objects, and invalidate prior done markers.
Permit this only for tables whose creation this run recorded; otherwise refuse.
Resume after the anchor continues CDC from the checkpoint.
After target crash, revalidate or recopy unlogged tables.
Keep existing refusals for unsafe follow combinations.

## Target privileges and guarantee boundary

The user explicitly approved the native dependency and target metadata privilege requirement.
Preflight requires target CREATE, or preprovisioned metadata with USAGE plus SELECT/INSERT/UPDATE.
The normal operator clone role already has CREATE.
Restricted data-only/apply roles may use preprovisioned bookkeeping.
Check cleanup ownership and privileges separately.

The no-loss and duplicate-prevention guarantee covers transactional logged PostgreSQL data under the stated storage assumptions.
Apply uses `session_replication_role=replica`; preflight requires superuser or `GRANT SET ON PARAMETER` privilege to set it.
Refuse ENABLE ALWAYS and ENABLE REPLICA user triggers.
External and nontransactional trigger effects can repeat during recovery.
Document their need for idempotency.
Do not claim unconditional exactly-once effects.

## Apply performance and ordering

Preserve transaction content, atomicity, and source commit order.
Prepared execution and pipeline support already exist in the C baseline.
Optimize their reuse, binding, cache behavior, bounded batches, and waits rather than presenting them as new discoveries.
Bulk operations must preserve repeated keys, key changes, replica identity FULL, NULL, unchanged TOAST, generated columns, and partition movement.
Account for triggers, constraints, and schema generation.

Receive and apply run on separate threads, each creating its own connection so handles remain `!Send`.
Connect them through a byte-bounded channel.
When full, receive stops reading but sends timer-driven status every `min(10 s, wal_sender_timeout/2)` and sets its connection's `wal_sender_timeout=60s`.
Apply caches prepared statements per relation generation and checks relations once per generation.
Per group, send BEGIN, pipeline DML and the checkpoint CAS UPDATE, flush, verify every result, then COMMIT.
This design targets two round trips and one fsync per durable group; benchmark the result rather than claiming measured throughput.
Close groups when input empties or a transaction, row, byte, or large-transaction cap is reached.
`PGCOPYDB_APPLY_GROUP_TXNS` defaults to 100.
Set apply sessions to `row_security=off`, enable server TCP keepalives, and set `transaction_timeout=0` only on servers ≥17.
Pin replication output GUCs.
Preserve source commit order and use serial apply as a differential reference.

## Durability profiles

Profiles apply to CDC.
COPY and schema completion are durable in every profile.
Positive target COMMIT acknowledgment is required before transaction feedback releases source WAL.
No profile releases source WAL merely because work entered a queue.
No-work keepalive advancement requires a separately tested proof that it crosses no pending source transaction.

### Durable

`durable` is the default.
Use synchronous target COMMIT per group and send feedback only after its positive durable outcome.
Gate source release on durable target progress.
Acknowledged transactional logged changes survive process, host, and target crashes under the stated storage assumptions.
Loss of all migration state does not promise resumability.

### Replay-safe

Target commits may use asynchronous commit, followed by logged synchronous writing barriers at a bounded interval.
Feedback advances only from barrier proofs, so the source retains input until target durability is proven.
Visible checkpoint rows alone do not authorize source WAL release.
Recovery replays from retained source WAL when target commits were lost.
Expose receive, flush, and apply frontiers accurately to the native operator.

A barrier updates a logged bookkeeping row and commits with synchronous durability after all covered target commits.
It must generate WAL.
A read-only transaction or empty COMMIT is not a barrier.
Record which fenced target and contiguous stream prefix each barrier covers.
PostgreSQL distinguishes logical commit success from persistent WAL in its [asynchronous commit documentation](https://www.postgresql.org/docs/18/wal-async-commit.html).

### Relaxed

Build `relaxed` only if paired benchmarks show a real gain over `replay-safe`.
If built, `relaxed` requires explicit opt-in and both finite `--loss-max-transactions` and `--loss-max-bytes` values.
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

Document process crash, host power loss, target crash, source WAL loss, and failover for each profile.
For `durable`, acknowledged logged target data survives the stated node failures, but missing state can prevent resume.
For `replay-safe`, recovery of asynchronous target loss requires provably retained source WAL.
For `relaxed`, missing proof causes refusal and potential remigration within the explicit transaction/byte risk bounds.
Source retention, external effects, and multiple simultaneous losses require separate explanations.
Acknowledged changes survive target failover only with a synchronous standby providing the required durability.
Otherwise target failover causes a gap refusal and requires a fresh migration.
Document source WAL/disk costs, throughput, latency, replay cost, configuration precedence, unsafe examples, and recovery procedures.
Choose other finite buffers and flush cadences from benchmark data and expose their effective values in help and status.

## Compatibility and operator work

Keep the Rust engine, native operator adapter, and verification/release harness as separate workstreams with separate write ownership.
Existing operator migrations retain their original pgcopydb runtime.
New rpgmigrate jobs require fresh native state.
No binary alias or automatic state adoption is allowed.

The adapter changes executable/workdir identity, version checks, log/application matching, mode and lag fields, and completion evidence.
It also checks metadata privileges and exact owned-object exclusions.
Use an explicit immutable engine selection for new migrations, with omitted selection retaining the legacy runtime and native jobs using a new UID and native-owned workdir.
Reuse the operator's existing progress, sentinel, exec, connection, and job boundaries rather than adding a second controller framework.
Freeze native capability, status, COPY metric, position, checkpoint, certified-drain, cutover, verification, and owned-cleanup DTOs in M1 before implementation.
Use `rpgmigrate version --json` in preflight and `native-report: {json}` stdout lines for phase, drain, progress, and cleanup.
Derive work identity environment values from the Migration UID and PVC.
Keep comparison's existing per-table JSON array and `compare progress:` output shapes outside ParseReport.
Keep `{error_severity, message}` JSON errors and OR both engines' application_name patterns in operator matching.
Legacy target-origin SQL remains legacy-only; missing native proof blocks native completion.
The audited operator does not directly query production SQLite or source-sentinel tables, so no legacy storage emulation is required or permitted.
Keep its diff and verification gate separate.
Read current operator instructions before editing it, without copying mutable tasks or policies into this repository.
Preserve its CI-first functional-test policy.

The qualified native profile targets 16→16, 18→18, and 16→18 only.
Older fork and operator cases remain legacy regression evidence and do not widen native admission.
Assert actual server versions and matching utility bundles in tests.

## Cutover and drain

The operator reads and persists `pg_current_wal_insert_lsn()` under the write freeze, including source commits made with `synchronous_commit=off`.
It passes that position to `stream sentinel set endpos <lsn> --json`; `--current` uses the same source function.
Endpos updates are idempotent and forward-only.
Write control in a separately locked `control/` subdirectory using temp file, fsync, rename, and directory fsync; the worker re-reads it.
Drain requires every received transaction with `end_lsn <= endpos` to be durable, followed by a keepalive `>= endpos` or a later BEGIN.
Request the keepalive through reply-requested status, write the barrier before feedback, and report that frontier as `drain_proof.replay_lsn`.
Synchronize final sequences and, when enabled, large objects before exit zero.
Requested endpos and worker exit alone never prove completion.

## Verification and performance gates

Retain full parser fixtures for registrations, aliases, environments, malformed input, stdout/stderr, JSON, and exit categories.
Runtime compatibility gates cover the accepted operator/general-use contract, selected CRD combinations, and complete migration phases; unused CLI families remain deferred.
Record corrected bugs as explicit exceptions.
Use independent SQL fingerprints for data, schema, sequences, large objects, ownership, and partitions.
Negative harness tests must reject empty fixtures, wrong versions, wrong failure reasons, missed injection, skipped cases, and exit-zero mismatches.

Inject faults around COMMIT, checkpoint writes, publication, ACK, source retention, process ownership, disk full, short/corrupt writes, snapshot loss, restart, and mode changes.
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
Retained storage benchmark receipts do not qualify the replacement engine.
No arbitrary speedup is promised.

M0 CI runs plain Rust format, Clippy, unit, CLI, and portable tooling checks against stock libpq.
The M5 runner image and later release gates remain separate work.
Release CI gates include Rust format/lint/tests, parser/state fuzzing, FFI ownership/sanitizers, dependency/license/security checks, and documentation drift.
Add STE100 hard-rule checks, database integration, crash suites, cross-version cases, and bounded runtime/size fixtures.
Operator-qualified release artifacts require isolated operator E2E.
Promote the exact tested binary/image digests without rebuilding.

## Platforms, workflow, and exclusions

Initial production targets are Linux amd64/arm64 with filesystems honoring locks, atomic publication, and synchronization.
macOS is a development target, not proof of PVC performance or power-loss durability.
Shared multi-host writers and arbitrary network filesystems are outside the guarantee.

Use feature branches and allow independent parallel build owners to use git worktrees off `feat/native-engine`.
Run independent reviews and applicable Go suites once per milestone at its close, rather than after each task.
Retain `tools/test-db.py` for cargo live tests; containers add the later E2E layer.
Preserve existing work in old repositories and the operator.
Apply ponytail full, humanizer/deslop, and STE100 to code planning and prose.
Ship tests and current documentation with behavior changes.
The README is an index written when the working scaffold exists.
Keep `publish = false` until licensing and release gates permit publication.
Preserve third-party notices and let the readiness worker check the dependency graph.

Builder decisions include narrow module names, vetted crate versions, codec/checksum details, measured budgets, and implementation sequence.
Escalate changes that violate these contracts.
Non-goals are legacy import, a pgcopydb alias or storage emulation, universal CLI/drop-in parity, universal DDL replication, custom auth/TLS, distributed cross-node scheduling, and a generic backend plugin framework.
