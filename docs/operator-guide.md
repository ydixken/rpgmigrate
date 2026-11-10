# Operator guide

This guide describes the native engine boundary that an operator must satisfy.
The [compatibility contract](operator-compatibility.md) defines consumed commands and defaults.
The [native protocol](native-protocol.md) defines discovery and report validation.

> [!IMPORTANT]
> Production native admission returns `UnsupportedEngine`, and resolver admission remains closed.
> No actual native operator workflow is qualified.
> M6 through M10 remain open.
> Use the [roadmap](roadmap.md) for exact evidence and remaining gates.
> This guide does not authorize deployment or activation.

Use the maintained [Live migration runbook](https://github.com/ydixken/pgcopydb-operator/blob/29edbfba1d03afb890429b2ef69ab8f5a9d088e1/docs/operations/live-migration.md) for established pgcopydb operations.
That procedure does not qualify native activation.

## Select the runtime explicitly

Keep existing pgcopydb migrations on their original runtime.
Never adopt, convert, mutate, resume, restart, or clean pgcopydb state with native commands.
Select native execution explicitly for a new migration only after its operator activation gates pass.
Require a fresh Migration UID, PVC, native workdir, run ID, workdir ID, and checkpoint generation.
Persist caller identity before starting the worker and require an exact match on every report and retry.
A supplied identity alone grants no authority over existing state.

Select one immutable image digest for the intended architecture.
Bind its executable hash and source identity to the retained packaging and runtime receipts.
A tag or successful image smoke cannot replace exact-image database and operator qualification.
Linux discovery requires the ordered seven-capability profile, including `verify.sealed_files.v1`.
The historical six-name profile still parses but fails the native M6 prerequisite before credential projection.
Use the [runner image guide](runner-image.md) for mounts, uid, tools, and artifact identities.

The declared version pairs are PostgreSQL 16 to 16, 18 to 18, and 16 to 18.
The baseline uses unsplit text COPY and `durable` commits.
Follow requires `pgoutput` and explicit large-object skipping.
Binary COPY, splitting, large-object migration, replay-safe cadence, and relaxed execution remain outside baseline discovery.
Isolated M8 and M9 candidates do not widen this operator profile.

## Preserve defaults and admission

Use the [finite option contract](operator-compatibility.md#finite-option-contract) and its documented defaults.
Reject an unsupported configuration before mutation instead of ignoring its selected behavior.
The first worker attempt uses `--restart`.
Supported retries use `--resume --not-consistent`.
Those flags do not authorize lost-snapshot reuse, foreign-state adoption, or uncertain utility recovery.
Native initialization accepts only the checked absent/private-empty path or a matching resource-free INIT retry.
Completed resume requires positive durable target proof and preserves the existing copy generation.

## Cutover and identical-boundary retry

The caller owns the writer fence and the requested end position E.
Keep source writers and outstanding transactions stopped through drain, final synchronization, and verification.
Read the [cutover procedure contract](native-cutover-handoff.md#cutover-and-retry) before implementing this sequence.

1. Stop source writers and finish outstanding transactions under the caller's fence.
2. Sample `pg_current_wal_insert_lsn()` and persist that nonzero E in caller-owned state before invoking the helper.
3. Submit the explicit saved E against the same native workdir and endpoints.
4. Keep E after a helper error and wait for worker exit when the active worker holds the target fence.
5. Retry the identical E with the standalone helper using the retained PVC, passfile, TLS mounts, and writable temporary directory.
6. Require a positively observed response with matching E and `end_persisted=true`.
7. Obtain independent target proof and complete final verification before releasing the writer fence.

A scalar set-endpos success proves local persistence only.
The JSON helper can return nonzero after E became durable locally because the worker still holds the target fence.
Exit 3 is a refusal, never an acknowledgment.
Do not resample `--current` after failure or replace E with a WAL flush position.
A worker exit, visible row, report, or comparison alone cannot prove durable drain.

The independent reader must check exact ownership and the target row under the native advisory fence.
It must complete a checked synchronous same-value barrier before publishing proof.
See [Independent M6 observation](native-cutover-handoff.md#independent-m6-observation) for the required SQL and session checks.
The helper must work after worker exit.
Live-worker exec alone cannot satisfy the contract.

## Cleanup and refusal

Use only complete positively established native receipts to identify owned source resources and target bookkeeping.
Keep comparison exclusions tied to exact recorded object identities.
Cleanup requires the native owner lock, durable terminal intent, and positive observations for each exact owned obligation.
Preserve the workdir, target data, target metadata, unrelated resources, and uncertain replacements.
Do not infer slot continuity from its name, plugin, or database.
See [Exact-owned cleanup](native-cutover-handoff.md#exact-owned-cleanup) and [troubleshooting](troubleshooting.md) before handling a refusal.
