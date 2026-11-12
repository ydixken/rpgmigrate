# Durability

We use source WAL and the owned logical slot as the durable input log.
The baseline has no local CDC spool.
The [cutover handoff](native-cutover-handoff.md) defines authority and completion in detail.

> [!IMPORTANT]
> `durable` is the baseline execution mode.
> Public replay-safe execution is unavailable, its checkpoint interval is `None`, and no cadence is qualified.
> Relaxed execution and binary pgoutput remain unavailable.
> M9 measurements and benchmarks remain on HOLD in the [roadmap](roadmap.md).

## Commit and feedback authority

Apply target row changes and the checkpoint CAS in the same transaction.
Advance feedback only from the positively observed synchronous durable prefix.
A received position, visible target row, COPY anchor, or later-`BEGIN` witness alone grants no ACK authority.
Check the session, deferred effects, warnings, terminal results, and positive synchronous COMMIT through SYNC where the pipeline path requires it.
An unknown COMMIT outcome cannot become rollback, success, or permission to advance feedback.
Reacquire the exact target fence and check the row through a new synchronous barrier with checked results before exposing durable proof.

Require a writable primary with `fsync=on` and `full_page_writes=on`.
Use at least `synchronous_commit=on` and preserve a configured `remote_apply` requirement.
Local WAL flush cannot replace an admitted synchronous-standby requirement.
The COPY anchor binds the snapshot and target authority.
It is distinct from feedback authority.
Completion also requires durable drain and final sequence synchronization under the writer fence.
Do not publish completion after an uncertain sequence or utility boundary.

## Recovery limits

Retain the exact native workdir, manifest, slot/publication receipts, target metadata, and caller identity.
Resume must check their identities and the required retained source WAL before effects.
Missing WAL, a lost required snapshot, or contradictory ownership is an explicit recovery refusal.
Do not accept loss, recreate a same-name slot, delete authority files, or adopt legacy state to force progress.
An uncertain pre-data or post-data outcome requires manual review or a fresh migration with separately established ownership.
See [Troubleshooting](troubleshooting.md) for safe evidence handling.

A process-kill test demonstrates only its observed process recovery boundary.
It does not prove host power-loss durability, target storage behavior, PVC recovery, or standby failover.
Asynchronous standby promotion can lose acknowledged target commits and requires separate evidence and policy.
Synchronous failover also needs exact standby, fencing, and storage qualification.
A runner image receipt or hosted plain-clone pass cannot supply those results.

The asynchronous fork path and native COPY have separate crash contracts.
Do not transfer fork assumptions to native COPY or infer safe snapshot reuse from a surviving process or file.
The admitted native recovery path needs its own checked snapshot and target authority.

## Candidate formats and support

The retained format 8 / target metadata 6 runner evidence is bound to its [recorded source and images](runner-image.md#public-development-images).
The M8 engine candidate uses native format 9 with target metadata 7 and requires fresh native state.
The isolated M9 candidate uses native format 10 with target metadata 7.
Unsupported state or metadata formats refuse without conversion.
Public execution remains durable.
Replay-safe execution, a relaxed loss budget, checkpoint cadence, and benchmark results require their own qualification.
Use the [roadmap](roadmap.md) for handoffs and qualification gates.
