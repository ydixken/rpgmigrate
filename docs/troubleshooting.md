# Troubleshooting

Treat a nonzero result as a refusal or failed operation until its cause is resolved.
Preserve private evidence before retrying.
The [native protocol](native-protocol.md) defines diagnostics and the [durability guide](durability.md) defines recovery authority.

## Exit categories

The engine uses these error categories and exit codes.

| Category | Exit |
| --- | --- |
| `BadArgs` | 1 |
| `BadConfig` | 2 |
| `BadState` | 3 |
| `Pgsql` | 4 |
| `Pgctl` | 5 |
| `Source` | 6 |
| `Target` | 7 |
| `Reload` | 9 |
| `Internal` | 12 |
| `Fatal` | 122 |

A recovery gap is `BadState`, exit 3.
JSON errors retain `error_severity="ERROR"` and a message beginning `native:<ErrorKind>:` followed by a space.
The error origin identifies the source basename, line, and column.
Keep credentials, endpoint secrets, and row contents out of diagnostics and retained artifacts.

## Inspect without changing authority

1. Record the checkout identity and inspect safe command help.

   ```sh
   git status --short --branch
   ./target/debug/rpgmigrate --help
   ./target/debug/rpgmigrate version --json
   ```

2. Compare the selected executable and immutable runner identity with the admitted receipt.
3. Preserve redacted diagnostics and exact caller/resource identities in a private evidence directory.
4. Classify the failure before selecting a documented retry path.

Use the executable from `CARGO_TARGET_DIR` if that override is set.
Do not copy real connection strings, passfile contents, raw SQL, or row data into tickets.
Keep workdirs private and preserve ownership and permissions when retaining evidence.

## Lost snapshot or missing source WAL

A lost pre-anchor snapshot cannot be reused.
An admitted restart must obtain a new snapshot and generation, with exact owned target cleanup and the documented boundary checks.
A clone restart advances native state first and records the previous generation's snapshot as a recopy intent in the same write.
If the process dies before the target commits its reset of the previous generation's markers and owned large objects, the next `--restart` or `--resume --not-consistent` publishes that pending generation first and then starts a fresh one.
The target must still hold exactly the recorded previous generation and snapshot, or already hold the native generation with no retained receipts; any other target refuses before a write.
An uncertain pre-data ownership handoff or post-data utility outcome refuses recovery.
Preserve uncertain objects for manual review or use a fresh positively established migration.

## Killed before the pre-data restore

A clone or follow that bound its snapshot and died before it recorded the pre-data handoff (native phase `fresh`) has restored nothing on the target.
The next `--resume --not-consistent` (the operator's retry), or `--restart` for clone, restarts it under the next copy generation the way a copy restart does.
A follow first drops its slot and publication by their bound receipts and creates new ones, and both then restore pre-data from the new snapshot as a first run would.
A target bookkeeping intent left by the kill is resolved first: the schema whose row carries the intent's nonce is this run's, and an absent one is created again.
A plain `--resume` of such a clone refuses with exit 3, `clone-recovery: COPY snapshot was lost; use --restart or --resume --not-consistent before post-data`.

After a durable anchor, resume still requires the exact source identity, slot/publication provenance, target authority, and retained WAL.
Missing WAL or a replaced source resource is a recovery gap.
Do not recreate a same-name slot, silently start from a later position, or turn absent evidence into success.

## Interrupted follow slot creation

`CREATE_REPLICATION_SLOT` waits until every source transaction that holds a transaction ID ends, so a long source transaction can hold a follow at slot creation.
A kill there, such as a node drain or a timeout, leaves a pending slot intent without its receipt.
A lost reply leaves the same state and exits with `source-slot-create-unknown: slot may retain WAL; the next start drops it by its recorded intent and creation shape, then creates it again`.
A stop that lands before the engine sends the CREATE leaves no slot intent, so the next start drops no slot for it.
The next start, a first-run retry or `--restart`, drops exactly the recorded slot if it exists and then creates it again under a new intent.
It never adopts that slot, because the snapshot it exported ended with the session that created it.
It drops only a logical `pgoutput` slot on the source database that is not temporary and has `two_phase`, `failover` and `synced` off; any other slot under that name refuses with exit 3, `follow-source: slot <name> differs from this run's unbound pgoutput creation`, and stays.

> [!note]
> While the creating session still waits, the slot is active and the start refuses with exit 3, naming the slot and its backend.
> End the blocking source transaction or wait for that session to end, then retry.

A slot that another session created under the name before this run's CREATE is not this run's.
The run refuses with exit 3, `follow-source: existing slot or publication is never adopted`, or `follow-source: slot <name> already exists, so this run's CREATE created nothing; it is never adopted or dropped`, and records no slot intent.
Later starts refuse while that slot exists, with the first text on a first run or `follow-restart: a slot reappeared after recorded absence` on a restart, and leave it in place.

> [!caution]
> Two migrations must never share a slot name on one source; with the operator, that means two Migrations with the same slot name.
> Give each its own name, remove the other slot only after you confirm nothing uses it, then retry.

A first run killed after the slot was created but before it bound the snapshot leaves the slot's receipt.
The next start drops that slot only at its recorded consistent point and creates it again, so it does not keep retaining WAL.
It drops this run's slot before its other checks, so a later refusal, such as a source table list that changed meanwhile, does not leave the slot behind.
A slot under that name at another position or of another plugin refuses with exit 3, `follow-source: slot <name> differs from this run's unbound pgoutput creation`, and stays.

## Interrupted follow publication creation

`CREATE PUBLICATION` waits for a `SHARE UPDATE EXCLUSIVE` lock on every member table, and its `COMMIT` can wait for a synchronous standby.
A kill there leaves a publication intent, or its receipt, without the confirmed outcome.
A lost reply leaves the same state and exits with `follow-source-create-unknown: publication COMMIT was not confirmed; the next start drops it by its recorded OID and owner, then creates it again`.
The next start, a first-run retry or a restart, resolves it before it creates a slot.
It retires the intent when no publication has the recorded name, and drops the publication with exactly the recorded OID and owner, so it creates the publication again under a new intent and never adopts the old one.

> [!caution]
> A publication of any other identity under the recorded name refuses with exit 3: `follow-source: publication <name> (OID <oid>) exists, but this run's CREATE under that name never committed`, or `follow-source: publication <name> (OID <oid>, owner <owner>) is not this run's unconfirmed creation (OID <recorded>, owner <recorded>)` when its OID or its owner differs from the receipt.
> The engine never drops it; remove it only after you confirm nothing else uses it, then retry.

A start while the killed run's `COMMIT` still waits on a synchronous standby cannot see that publication.
It waits behind that `COMMIT` and fails with SQLSTATE 23505 once it completes, and later starts refuse with the first text above.
That publication is the killed run's, but nothing recorded proves it, so remove it the same way.

## Unknown COMMIT or target fence conflict

A lost COMMIT reply or synchronous-wait warning leaves the outcome unknown.
Do not infer rollback from the connection error or advance feedback from visible data.
Require the checked independent barrier described in [target observation](native-cutover-handoff.md#independent-m6-observation).
During the initial copy, an absent COPY marker or large-object receipt after that barrier needs `--restart` under a new copy generation.
In the final large-object re-sync the follow is anchored and cannot restart its copy generation, so resume the follow and let its next drain repeat the re-sync.

An active worker can hold the target fence after set-endpos has persisted E locally.
Keep the saved E, wait for worker exit, and retry identical-E JSON observation with the standalone helper.
Require matching E and `end_persisted=true` in a positive response.
Exit 3 is not an acknowledgment.
Never resample `--current` after failure.

## Helper, PVC, authentication, or TLS refusal

Production `UnsupportedEngine` is an activation gate, not a transient error to bypass.
Check the exact selected image, capability handshake, architecture, helper runtime, and qualified version pair.
Retain the same PVC and native workdir after worker exit for the helper's supported observation path.
Check writable work and temporary mounts, effective uid, private passfile/service files, and certificate mounts.
Check TLS peer identity and the intended libpq service selection.
Do not disable certificate verification or expose a URI in argv to bypass admission.
Packaging smoke alone does not qualify authentication, TLS, resolver, PVC, or helper behavior.

## Ownership, cleanup, or format refusal

Stop if caller identity, checkpoint generation, object OIDs, slot continuity, or ownership disagrees.
Preserve the conflicting resource and receipts for review.
A restart or `stream cleanup` drops a publication only at its recorded OID and owner.
They drop a slot under the recorded name only when it has this run's creation shape (logical `pgoutput` on the source database, not temporary, `two_phase`, `failover` and `synced` off) and no active backend, and a restart also requires its recorded consistent point.
Any other slot refuses with exit 3 and stays: `follow-restart: surviving source slot does not match the inactive owned creation receipt` or `cleanup-source: slot identity or inactive ownership disagrees; preserve it for manual inspection`.
One that replaced it under the name, even while the `DROP` waited on its lock, refuses with exit 3 and stays.
Never broaden cleanup by name prefix, delete authority files, or delete a whole PVC to repair a refusal.
An empty cleanup inventory cannot prove completion.

Older or incompatible native state and target metadata formats refuse before effects and have no automatic conversion path.
The M8 engine candidate expects native format 9 and target metadata 7 for fresh migrations.
For retained format 8 / metadata 6 state, consult the receipt for its exact qualified runner and source.
The isolated M9 format 10 candidate requires its own qualification.
Never adopt, convert, resume, restart, mutate, or clean pgcopydb state with native commands.
