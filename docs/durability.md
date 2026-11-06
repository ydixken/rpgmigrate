# Durability and recovery

This guide explains the [approved durability contract](superpowers/specs/2026-11-06-rpgmigrate-design.md#durability-profiles) for rpgmigrate.
Use it to assess source WAL retention, target durability, and recovery requirements before choosing a profile.

> [!important]
> These are specification requirements, not evidence that CDC, durability profiles, or recovery commands are implemented.
> Check the [CLI runtime gate](cli-compatibility.md#production-test-ownership) before relying on a feature.
> Parser acceptance and help output do not prove runtime support.

Profiles govern change data capture (CDC), which applies source write-ahead log (WAL) changes to the target.
COPY and schema completion must remain durable in every profile.
Every profile requires positive target COMMIT acknowledgment before transaction feedback allows the source to release WAL.
Queueing work does not permit that acknowledgment.

## Read the progress frontiers

A frontier marks a contiguous prefix within one stream.
Keep these positions separate when assessing progress or cutover readiness.

| Frontier | What it proves |
|---|---|
| Received | Input reached the receiver |
| Journal durable | Complete input and required decoder context are durable |
| Target logically committed | The target acknowledged COMMIT |
| Target durable | Target durability covers the prefix |
| Source acknowledged (ACK) | Feedback permits source WAL release through that position |

Logical target commit can precede durable target commit when asynchronous commit is in use.
A visible checkpoint row alone does not prove target durability.
Source commit-order LSN, source transaction-end LSN, and target-local WAL LSN have different meanings.
An LSN is a WAL position, not a transaction identity.
Do not compare source positions with target-local WAL, or combine separate database streams into one scalar position.

## Choose a profile

### Durable: the default

Synchronize the required journal input and target commits before acknowledging the source.
Source release waits for durable target progress.
This places both synchronization costs on the acknowledgment path.
Under the storage assumptions below, acknowledged transactional logged changes survive process, host, and target crashes.
Spool loss does not delete those target commits, but missing migration state can prevent resume.

### Replay-safe: retain input for target recovery

Make the journal and required decoder context durable before source acknowledgment.
The target may acknowledge asynchronous commits, followed by periodic synchronous writing barriers.
This can reduce target synchronization waits, with more replay after a crash.
It does not promise a particular throughput gain.
Retain the journal until target durability is proven, even when checkpoint rows are visible.
Recovery preserves acknowledged changes when the required durable journal survives.
After source release, spool-volume loss can delete input needed to recover asynchronous target commits.
Do not assume the source still retains that input.

### Relaxed: explicit bounded loss risk

Opt in explicitly and supply both a finite transaction limit and a finite encoded-byte limit.
Neither limit has an implicit unlimited value or a default in this contract.
Count source transactions, not apply groups, and count their canonical encoded bytes.
Acknowledgment may advance beyond durable copies only after positive target COMMIT and a durable risk reservation.
This permits loss risk after a crash and can require a fresh migration.
Encoded-byte limits do not bound business effects: a small TRUNCATE event can affect many rows.
A periodic flush cadence is a scheduling target, not a maximum elapsed-time loss window.

## Understand the run-wide risk budget

One budget covers the native migration run/workdir, including every database and overlapping standalone phase.
Independent runs have separate budgets.
For each stream, D is the proven durable target prefix.
H is the upper bound on source acknowledgment permitted by durable reservations.
H can exceed feedback actually sent and does not prove target commit.

Before speculative target work and ACK, atomically persist the epoch, D/H, reservation identities, and aggregate transaction and byte counts.
The epoch identifies the recorded risk interval for the effective profile.
A dirty boolean cannot replace this record.
Check both limits before allocating capacity.
A worker failure leaves its capacity charged until recovery proves release safe.
Failure must never increase the available budget.

Release reservations only after target durability and the durable update of D and the aggregate counters.
Each database needs its own target proof.
One database's barrier cannot release another database's reservations.
At the caps, stop speculative acknowledgment until barriers free capacity.
An oversized transaction must take a durable path before source release.

## Require a writing barrier

One synchronous COMMIT can cover user changes, their logged checkpoint, and a logged barrier-counter update.
After positive COMMIT acknowledgment, that transaction can produce the durable proof without a second COMMIT for the group.
For prior asynchronous commits or no new CDC data, use a standalone synchronous transaction that changes the logged barrier counter.
Both forms must generate WAL and identify the covered contiguous prefix on the fenced target.
A read-only transaction, empty COMMIT, or asynchronously visible checkpoint is insufficient.

An empty checkpoint proof covers zero CDC transactions.
It cannot invent requested end positions or establish COPY snapshot coverage.
The logged target checkpoint remains the recovery authority, not replication-origin progress alone.

## Assess failure consequences

The matrix concerns acknowledged transactional logged data under the storage assumptions below.
Replay requires complete retained input and valid checkpoint evidence.
The relaxed caps bound reserved transaction/byte risk, not arbitrary business effects or asynchronous failover loss.

| Failure | Durable | Replay-safe | Relaxed |
|---|---|---|---|
| Process crash | Reconcile checkpoint | Replay if needed | Replay or refuse |
| Host power loss | Acknowledged data retained | Retained input required for replay | Bounded risk, replay or refuse |
| Target crash | Acknowledged data retained | Retained input required for replay | Bounded risk, replay or refuse |
| Spool-volume loss | Data retained, resume may fail | Recovery proof required | Recovery proof required |
| Asynchronous cluster failover | Not guaranteed | Not guaranteed | Not guaranteed |

> [!warning]
> A durable journal only protects recovery while the required storage survives.
> Losing both source input and replay input can require a fresh migration.
> No profile guarantees arbitrary asynchronous cluster failover or recovery after loss of all migration state.

## Recover without hiding a gap

After an unknown COMMIT outcome, acquire the target writer fence and read the checkpoint on the target PRIMARY.
Do not assume rollback merely because the reply was lost.
Reconcile that checkpoint with the stored epoch, risk reservations, and complete retained input.
Missing or corrupt required history causes an explicit nonzero gap refusal.

Start a fresh native migration when recovery cannot prove a complete prefix.
Do not discard required history to force resume or accept loss with an exit-zero result.
Never import, resume, restart, convert, or clean pgcopydb state or resources.

Journal capacity includes catalogs, WAL, temporary files, and reclamation reserve.
Disk exhaustion must stop ingest and feedback advancement at a recoverable boundary.
Heartbeats remain at the last permitted frontier while the engine reports capacity failure.
Pruning requires durable target completion, no open-transaction dependency, and no reader pin.
An unfinished large transaction must not depend on pruning its own input to make progress.

## Complete or change modes

Every normal success or cutover must drain work, synchronize target and state, and publish a durable receipt.
That receipt covers the complete requested prefix in every database and every required phase.
Requested end positions cannot substitute for actual completion.
Relaxed mode does not weaken this final-success requirement.

Persist the effective profile and epoch, and use those stored settings on resume.
An explicit profile change requires a quiesced durable boundary across the entire run.
Changing an uncertain relaxed epoch's label to durable does not establish safety.

## Check the guarantee boundary

The contract assumes storage that honors locking, atomic publication, and synchronization.
Linux amd64/arm64 are the initial production targets.
macOS development results and process-kill tests do not prove operator-storage or power-loss durability.
Arbitrary network filesystems and shared multi-host writer ownership are outside the guarantee.

Unlogged tables need revalidation or recopy after target crash.
An unfinished follow COPY that loses its snapshot must retain the specified refusal.
Reconcile actual state for nontransactional DDL, sequences, and vacuum.
External or nontransactional trigger effects can repeat, so their handlers need idempotency.
The guarantee does not provide unconditional exactly-once effects.

## Evaluate performance and storage together

Delayed source acknowledgment can retain more source WAL, while replay-safe operation depends on retained spool capacity and recovery work.
Measure target-committed throughput, apply latency, complete drain, synchronization, and recovery costs before choosing operational settings.
The persistence comparison includes a segmented journal/catalog, redb-only storage, and optimized SQLite FULL/WAL with the same encoding and durability semantics.
SQLite remains eligible, and a bounded experiment does not select a production winner.
