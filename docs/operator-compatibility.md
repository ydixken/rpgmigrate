# Operator compatibility contract

The user accepted this operator-first release scope on 2026-11-07.
We implement the operator's consumed migration contract and sensible general controls, rather than requiring full pgcopydb CLI parity.
The [approved design](superpowers/specs/2026-11-06-rpgmigrate-design.md), [engine roadmap](superpowers/plans/2026-11-06-engine.md), and [operator adapter plan](superpowers/plans/2026-11-06-operator-adapter.md) use this matrix.
The source audit is pinned to `pgcopydb-operator` commit `639bdf997f2ed32b6e0b88420455428cb5112c06`; it inspected production callers and API renderers, not executed operator E2E.

> [!IMPORTANT]
> The runnable CLI provides `ping` and restricted table-data COPY.
> No native operator workflow is qualified by the existing component results.
> Use the [maintained task ledger](../tasks/todo.md#executable-command-scope) for exact implementation and qualification evidence.

## Release boundary

Eight production operator command paths are reachable from the 48 behavioral paths in the frozen parser inventory.
The other 40 are deferred as parity requirements; existing commands and parser fixtures are retained, not removed.
General `help`, `version`, and `ping`, explicit connection/work-directory inputs, existing environment precedence, and nonzero failure reporting remain part of the native interface.
The counts describe command surface, not implementation effort or a percentage of completion.

New native migrations require explicit runtime selection and a fresh native-owned workdir.
Existing pgcopydb migrations stay on their original runtime.
Never adopt, convert, resume, restart, or clean legacy state as native state.
No universal CLI or drop-in compatibility claim follows from this scope.

## Required command paths

All eight paths below are refused by the [frozen native dispatch](../src/config.rs#L373); parser acceptance is not implementation.
The native adapter must preserve each consumed operation through an explicit, tested native contract.

| Command path | Consumed operation |
| --- | --- |
| `clone` | Clone, optional follow, native restart/resume lifecycle. |
| `stream cleanup` | Idempotent cleanup of exactly owned slot, publication, and origin. |
| `stream sentinel set endpos` | Request cutover at current source position; return a scalar LSN. |
| `stream sentinel get` | Read end position or replay LSN after worker exit. |
| `list progress` | Version-gated JSON progress and completion counters. |
| `compare data` | Drain fallback, final verification, and rehearsal data comparison. |
| `compare schema` | Final schema verification, including selected all-database mode. |
| `list tables` | Rehearsal catalog inventory with matching filters. |

The production call sites are the [job builders][op-jobs], [verification controller][op-verify], [rehearsal builder][op-rehearsal], and [cutover exec][op-sentinel].
There is no production standalone `follow` launch: follow is appended to `clone`.
The first worker attempt uses `--restart`; retries use `--resume --not-consistent`, and permitted snapshot-loss start-over adds cleanup.
Native recovery must prove that these requests are safe; legacy flag spelling never authorizes unsafe snapshot reuse or state adoption.

## Finite option contract

The [renderers][op-args] emit 31 clone option names and seven follow additions.
Helpers add five names, giving 43 behavioral option names, or 45 including `--help` and `--version`.
These are distinct names, not configuration combinations.
There is no arbitrary worker-argument or `WorkerArgsEnv` override in the audited API.

The 31 clone names are:

- Work/state: `--dir`, `--restart`, `--resume`, `--not-consistent`.
- Workers: `--table-jobs`, `--index-jobs`, `--restore-jobs`, `--large-objects-jobs`.
- Splitting: `--split-tables-larger-than`, `--split-max-parts`, `--estimate-table-sizes`.
- Execution: `--use-copy-binary`, `--fail-fast`, `--defer-indexes`.
- Restore policy: `--drop-if-exists`, `--roles`, `--no-role-passwords`, `--no-owner`, `--no-acl`, `--no-comments`, `--no-tablespaces`.
- Selection: `--filters`, `--all-databases`.
- Skips: `--skip-large-objects`, `--skip-extensions`, `--skip-ext-comments`, `--skip-collations`, `--skip-vacuum`, `--skip-analyze`, `--skip-db-properties`, `--skip-split-by-ctid`.

The seven [follow additions][op-follow] are `--follow`, `--slot-name`, `--origin`, `--plugin`, `--publication`, `--wal2json-numeric-as-string`, and `--replay-no-op-updates`.
Helpers add `--current`, `--source`, `--endpos`, `--replay-lsn`, and `--json` through the [cutover][op-set-end], [drain][op-drain], [progress][op-progress], and [verification][op-verify] paths.
Preserve the existing connection, passfile/TLS, JSON-log, and `PGCOPYDB_APPLY_GROUP_TXNS` environment contracts through the native adapter; credentials must remain absent from diagnostics.

## Defaults and selected feature semantics

An optional CRD field is optional for the customer to select, not optional to implement while claiming compatibility with that configuration.
Every admitted feature must execute its selected semantics.
A staged profile must reject unsupported configurations explicitly during admission or preflight, before mutation; it must not ignore fields or substitute different behavior.
Full operator API compatibility requires qualification of all exposed configurations within the declared profile.

The [admitted follow plugin defaults to `pgoutput`][op-plugin], and the renderer emits that value.
Clone-only mode has no default plugin argument.
The existing `test_decoding` Gate 3 baseline does not qualify operator-default follow.
Do not remap `pgoutput` to another plugin.
Qualify its input/context and publication lifecycle separately; qualify the exposed `wal2json` and `test_decoding` selections or refuse them in a narrower staged profile.

The [clone renderer][op-args] includes large objects unless `clone.skip.largeObjects` is explicit.
Binary COPY defaults on and requires the established safe per-table text fallback.
The [parallel/split defaults][op-defaults] use at least four table workers, a 512 MiB split threshold, and eight parts; these are core operator behavior, not deferred CLI extras.

The [CRD and renderers][op-types] also expose these feature groups:

- Full clone phases: schema, data, indexes, constraints, sequences, partitions, extensions, and large objects.
- Parallelism: table, index, restore, and large-object workers; split limits, estimated sizes, and deferred indexes.
- Selection: include/exclude schemas, tables, and extensions, plus excluded indexes and table data.
- Restore policy: requested roles, password/owner/ACL/comment/tablespace omissions, destructive restore, fail-fast, and the eight explicit skips.
- Follow: slot/publication selection, plugin-specific numeric output, no-op updates, and apply grouping.
- Instance clone: all databases and its database-creation/role semantics.
- Verification: schema/data comparison, rehearsal, and partition-aware results.

Retain [API combination constraints][op-admission], including all-database exclusions and ownership-handover requirements.
The E1 precreated-empty-table COPY slice does not implement these clone phases or default parallel, binary, and large-object behavior.

## Native operator adapter

The accepted direction is an optional immutable `spec.engine` selection, `pgcopydb` or `rpgmigrate`, with omission retaining the legacy runtime.
Native selection applies to new migrations with a new UID and marked native workdir; it cannot retarget an existing migration.
Freeze the generated API/admission changes and DTOs before implementation.
Extend the existing `ProgressOps`/`SentinelOps` boundary through per-migration backend selection and reuse exec, connection, and job helpers.

The legacy [job builder hardcodes `pgcopydb`][op-binary].
Its [progress gate][op-progress] strips the exact first-line prefix `pgcopydb version ` and compares against [21 exact configured strings][op-versions].
Unknown versions suppress that progress path; a numeric Rust version alone cannot satisfy the protocol.
Use a native version/capability handshake and native identity, not a fake pgcopydb version or binary alias.

Freeze native DTOs for version/capabilities, status and COPY metrics, distinct source/target positions, durable checkpoints, certified drain, cutover LSN, verification, and owned cleanup.
Map the consumed [log records][op-logs], progress JSON, comparison results, scalar LSN output, exit status, and independent drain checks explicitly.
Legacy missing-counter behavior is not a native completion proof; reject missing or incomplete verification evidence despite exit zero.
Legacy target-origin SQL stays on the legacy backend; missing native proof blocks native completion.

Cutover runs inside the live worker container and shares the PVC, `/work/pgcopydb`, writable `/tmp`, URI files, passfile, and TLS mounts through the existing [exec][op-sentinel] and [mount][op-mounts] paths.
This permits native workdir/control access without reproducing a legacy database layout.
The audit found no direct production SQLite or source-sentinel-table queries.
Do not emulate legacy SQLite files or source-sentinel storage.
Control must use checked native identity and ownership; requested endpos, a visible checkpoint, or worker exit alone cannot certify durable drain.
Exclude only exact owned metadata objects from comparison and ownership changes, and clean only resources owned by that native run.

## Stage gates

These gates retain COPY/WAL atomicity, snapshot ownership, source feedback, unknown-COMMIT recovery, and resource bounds.
They do not change the current bounded `test_decoding` consumer implementation.

- [x] **Scope:** accept the eight paths, finite option inventory, selected CRD semantics, and native-only migration boundary.
- [ ] **Native contract:** freeze engine selection, DTOs, immutable runtime/workdir identity, and legacy regression fixtures at the existing operator boundary.
- [ ] **Current core baseline:** finish the assigned Gate 3 consumer reviews, exact-source qualification, and strict aggregate checks; retain its bounded replay limits.
- [ ] **Default follow:** qualify real `pgoutput` input, decoder/context recovery, and owned or supplied publication behavior with actual slots and source/target evidence.
- [ ] **Runtime clone:** qualify schema utilities and their runtime image, all required clone phases, large objects, binary fallback, parallel/split copy, filters, selected roles, and all-database combinations.
- [ ] **Lifecycle:** qualify native restart/resume or explicit refusal, cutover control, durable drain, progress, comparison, metadata exclusion, and idempotent owned cleanup through the eight routes.
- [ ] **Operator E2E:** run default clone/live migration and selected feature cases using the actual adapter, worker/helper containers, shared storage, and exact candidate artifacts.
- [ ] **Release:** pass strict lint/tests, declared version/architecture profiles, independent review, and artifact gates before publishing operator-complete claims.

Golden argument tests must cover defaults and every conditional renderer branch, including explicit refusal of unsupported stage configurations.
Lifecycle negatives must cover foreign state/resources, stale or missing proof, incomplete reports, unknown COMMIT, lost snapshots, worker/helper failure, and missing fault injection.
Retain independent content checks for tables, sequences, large objects, schemas, ownership, and partitions.
The [operator E2E matrix][op-e2e] lists concrete pairs; source inspection does not prove those runs or a universal version cross-product.
Keep component tests, production/operator workflow gates, and measured performance as separate evidence.
No backend winner, general crash-resume guarantee, throughput gain, or ETA follows from the reduced command count.

[op-args]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/pgcopydb/args.go#L69
[op-follow]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/pgcopydb/args.go#L226
[op-types]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/api/v1beta1/migration_types.go#L179
[op-plugin]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/api/v1beta1/migration_types.go#L375
[op-defaults]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/pgcopydb/defaults.go#L29
[op-jobs]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/controller/resources.go#L139
[op-rehearsal]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/controller/resources.go#L294
[op-verify]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/controller/verification.go#L151
[op-drain]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/controller/resources.go#L249
[op-progress]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/progress/progress.go#L84
[op-versions]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/cmd/main.go#L114
[op-binary]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/controller/resources.go#L1685
[op-sentinel]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/sentinel/sentinel.go#L54
[op-set-end]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/sentinel/sentinel.go#L148
[op-mounts]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/controller/resources.go#L1616
[op-logs]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/internal/pgcopydb/logs.go#L72
[op-admission]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/api/v1beta1/migration_types.go#L649
[op-e2e]: https://github.com/ydixken/pgcopydb-operator/blob/639bdf997f2ed32b6e0b88420455428cb5112c06/hack/e2e-matrix.sh#L10
