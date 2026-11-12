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
An uncertain pre-data ownership handoff or post-data utility outcome refuses recovery.
Preserve uncertain objects for manual review or use a fresh positively established migration.

After a durable anchor, resume still requires the exact source identity, slot/publication provenance, target authority, and retained WAL.
Missing WAL or a replaced source resource is a recovery gap.
Do not recreate a same-name slot, silently start from a later position, or turn absent evidence into success.

## Unknown COMMIT or target fence conflict

A lost COMMIT reply or synchronous-wait warning leaves the outcome unknown.
Do not infer rollback from the connection error or advance feedback from visible data.
Require the checked independent barrier described in [target observation](native-cutover-handoff.md#independent-m6-observation).

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
Never broaden cleanup by name prefix, delete authority files, or delete a whole PVC to repair a refusal.
An empty cleanup inventory cannot prove completion.

Older or incompatible native state and target metadata formats refuse before effects and have no automatic conversion path.
The M8 engine candidate expects native format 9 and target metadata 7 for fresh migrations.
For retained format 8 / metadata 6 state, consult the receipt for its exact qualified runner and source.
The isolated M9 format 10 candidate requires its own qualification.
Never adopt, convert, resume, restart, mutate, or clean pgcopydb state with native commands.
