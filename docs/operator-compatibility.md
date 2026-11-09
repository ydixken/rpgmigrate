# Operator compatibility contract

The user accepted this operator-first release scope on 2026-11-07.
We implement the operator's consumed migration contract and sensible general controls, rather than requiring full pgcopydb CLI parity.
The [approved design](superpowers/specs/2026-11-06-rpgmigrate-design.md) and [roadmap](roadmap.md) use this matrix.
The roadmap amendments approved on 2026-11-08 narrow the native profile and define the milestone gates below.
The original source audit is pinned to `pgcopydb-operator` commit `639bdf997f2ed32b6e0b88420455428cb5112c06`; it inspected production callers and API renderers, not executed operator E2E.

> [!IMPORTANT]
> Plain clone and ongoing follow passed all three engine pair profiles, including independent comparisons and actual Go clone-report checks.
> Engine M3 is committed at `7b6037f9ac8214696936a21c865cd97307cc5c6e`; cutover and native operator activation remain unqualified.
> No native operator workflow is qualified by the existing component results.
> Use the [maintained task ledger](../tasks/todo.md) for exact implementation and qualification evidence.

The M1 validator contract is pinned to operator commit `0267c787a900f37702181810c37ec313085e1287`.
See the [native protocol v1](native-protocol.md) for exact discovery/report fields, independent authority bindings, shared fixtures, and deferred runtime obligations.

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

The [native dispatch](../src/config.rs) explicitly refuses these paths until their native implementations are qualified; parser acceptance is not implementation.
The native adapter must preserve each admitted operation through an explicit, tested native contract.
`list tables` remains an explicit staged refusal in the revised native profile.
Accept and record `--origin`, but never create or own a replication origin; cleanup reports it as not owned.

| Command path | Consumed operation |
| --- | --- |
| `clone` | Clone, optional follow, native restart/resume lifecycle. |
| `stream cleanup` | Idempotent cleanup of exactly owned slot and publication. |
| `stream sentinel set endpos` | Request cutover at current source position; return a scalar LSN. |
| `stream sentinel get` | Read end position or replay LSN after worker exit. |
| `list progress` | Version-gated JSON progress and completion counters. |
| `compare data` | Drain fallback, final verification, and rehearsal data comparison. |
| `compare schema` | Final schema verification, including selected all-database mode. |
| `list tables` | Rehearsal inventory; explicit staged refusal in the native profile. |

The production call sites are the [job builders][op-jobs], [verification controller][op-verify], [rehearsal builder][op-rehearsal], and [cutover exec][op-sentinel].
There is no production standalone `follow` launch: follow is appended to `clone`.
The first worker attempt uses `--restart`; retries use `--resume --not-consistent`, and permitted snapshot-loss start-over adds cleanup.
Native startup admits the first restart only through checked absent/private-empty initialization, or a matching resource-free native INIT retry; it never rebinds foreign or legacy state.
A completed native retry with `--resume --not-consistent` verifies existing durable completion and preserves generation and table markers instead of recopying.
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
Historical bounded `test_decoding` component results do not qualify operator-default follow.
Do not remap `pgoutput` to another plugin.
The revised native profile uses pgoutput protocol v1 without streaming, with exactly `proto_version '1'` and `publication_names` plugin options.
Refuse other plugins and unsupported plugin options before mutation.

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
The M2 plain-clone implementation covers schema, parallel text COPY, post-data, sequences, and ANALYZE.
Its binary COPY, splitting, large objects, follow, and full operator configuration behavior remain unqualified and explicitly staged.
The M7 vertical slice explicitly switches off binary COPY, splitting, and large objects; M8 qualifies them before using operator defaults.
DropIfExists and filters are excluded from the native E2E profile.
M1 freezes precise native option and environment admission rather than inferring support from the inventory above.

## Native operator adapter

The accepted direction is an optional immutable `spec.engine` selection, `pgcopydb` or `rpgmigrate`, with omission retaining the legacy runtime.
Native selection applies to new migrations with a new UID and marked native workdir; it cannot retarget an existing migration.
Freeze the generated API/admission changes and DTOs before implementation.
Extend the existing `ProgressOps`/`SentinelOps` boundary through per-migration backend selection and reuse exec, connection, and job helpers.

The legacy [job builder hardcodes `pgcopydb`][op-binary].
Its [progress gate][op-progress] strips the exact first-line prefix `"pgcopydb version "` and compares against [21 exact configured strings][op-versions].
Unknown versions suppress that progress path; a numeric Rust version alone cannot satisfy the protocol.
Use a native version/capability handshake and native identity, not a fake pgcopydb version or binary alias.

M1 freezes native DTOs for version/capabilities, status and COPY metrics, distinct source/target positions, durable checkpoints, certified drain, cutover LSN, and owned cleanup.
Comparison keeps its existing transport outside the native Report DTO.
Use `version --json` capabilities in preflight, `native-report: {json}` stdout for phase/progress/drain/cleanup, and work identity derived from the Migration UID and PVC.
Keep the existing per-table comparison JSON array and `"compare progress: "` shapes outside `ParseReport`.
Discovery is identity-free and advertises an explicit empty capability array until runtime features qualify.
Supplied operator identity grants no native-state authority; M2 binds all four values to recorded state and refuses mismatches before mutation.
Only that bound mode emits operational native-report framing; standalone clone/progress emit unframed JSON without synthetic Migration/PVC identity.
Keep JSON error classes in `{error_severity, message}`, and OR both engines' application_name patterns in matching.
M1 freezes exact DTO fields, environment names, owned schema, workdir, application names, and cleanup inventory.
Map the consumed [log records][op-logs], progress JSON, comparison results, scalar LSN output, exit status, and independent drain checks explicitly.
Legacy missing-counter behavior is not a native completion proof; reject missing or incomplete verification evidence despite exit zero.
Legacy target-origin SQL stays on the legacy backend; missing native proof blocks native completion.

Legacy cutover runs inside the live worker container and shares the PVC, `/work/pgcopydb`, writable `/tmp`, URI files, passfile, and TLS mounts through the existing [exec][op-sentinel] and [mount][op-mounts] paths.
The native adapter reuses these mount and exec boundaries with a native-owned workdir whose exact layout freezes in M1.
This permits native workdir/control access without reproducing a legacy database layout.
The audit found no direct production SQLite or source-sentinel-table queries.
Do not emulate legacy SQLite files or source-sentinel storage.
Control must use checked native identity and ownership; requested endpos, a visible checkpoint, or worker exit alone cannot certify durable drain.
Exclude only exact owned metadata objects from comparison and ownership changes, and clean only resources owned by that native run.

## Native profile and milestone gates

The revised release targets 16→16, 18→18, and 16→18 with stock PGDG libpq and client tools 16 and 18.
Refuse other source/target pairs before mutation.
Client tools match the target major; historical legacy version matrices do not widen native admission.
Apply requires permission to set `session_replication_role=replica` and refuses ENABLE ALWAYS/REPLICA user triggers.
After creation, missing slots/publications and changed publication OIDs cause a gap refusal.
Native cleanup never adopts a foreign resource or removes pgcopydb state.

The [roadmap](roadmap.md#milestones) defines these gates, and the [task ledger](../tasks/todo.md) records their evidence:

- M0 retains a buildable component scaffold, stock-libpq authentication tests, and plain CI; it qualifies no operator migration.
- M1 freezes the command, option, environment, protocol, identity, and shared golden-fixture contracts in both repositories.
- M2 through M4 implement clone, pgoutput follow, recovery, cutover, durable drain, and owned cleanup.
- M5 provides the runner image and engine release CI; M0 retires the previous custom image pipeline.
- M6 implements the adapter at the existing operator boundaries with unit and envtest coverage.
- M7 qualifies the narrowed native vertical slice through operator E2E on the three declared version pairs.
- M8 widens binary COPY, split parts, and large objects, then reruns the native matrix with operator defaults.
- M9 enables replay-safe tuning and conditional relaxed mode only with measured evidence; M10 closes release review and artifact gates.

In `durable` and `replay-safe`, the source slot retains WAL throughout copy and until target durability is proven; there is no local copy-phase spool.
M9 keeps `relaxed` conditional on measured gain and explicit loss reservations, without claiming its availability.
The barrier-proven durable replay frontier authorizes feedback.
Later-BEGIN drain coverage can exceed that frontier and never raises feedback or reconnect authority.
Before the follow-copy anchor, resume restarts only the run's recorded owned data phase with a new snapshot and generation; after it, resume uses the checkpoint.
The operator persists a source `pg_current_wal_insert_lsn()` endpos and requires drain proof bound to it, plus an independent read of the owned target control row.
The proof must match the same logged row's work identity, checkpoint token, certified and durable frontiers, persisted endpos, witness, and witness kind.
Keepalive witnesses are covered by the certified frontier; a later-BEGIN witness is the strictly later `BEGIN.final_lsn`.
Requested endpos, missing reports, or exit zero alone cannot complete a native migration.
Cleanup requires the exact caller-declared resource inventory, retains the owned workdir, and leaves recorded origins unowned and unremoved.
Final sequence sync and, when enabled, large-object re-sync happen under the write freeze after drain.

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
