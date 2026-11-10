# Native protocol v1

We freeze the engine/operator boundary here and in the shared [contract fixtures](../tests/fixtures/native/v1/contract.json).
The [M1 plan](superpowers/plans/2026-11-08-m1-native-contract.md) defines the implementation boundary.
M1 defines discovery and pure contract validation; its empty capability fixture remains accepted parser evidence.
Discovery describes the narrowed core profile and the Linux-only compiled sealed-file comparison interface below; platform advertisement does not replace its runtime prerequisites.
New executable and image bytes require their own packaging and M6 runtime evidence before operator activation.
Shared fixture payloads remain synthetic contracts, separate from actual producer checks.
Use the [Native cutover handoff](native-cutover-handoff.md) for M4 command behavior, native format 8, metadata version 6, and the M6 observation procedure.
Neither fixtures nor parser acceptance qualify follow, recovery, cutover, cleanup, PostgreSQL version support, or durability.

## Discovery

Run discovery without PostgreSQL, a workdir, or operator identity:

1. Request the protocol and capability set.

   ```sh
   cargo run --locked -- version --json
   ```

The command writes one JSON object and a newline to stdout.
Its exact required fields are `protocol_version`, `engine_version`, and `capabilities`.
`protocol_version` is integer `1`, and `engine_version` comes from Cargo package metadata.
Linux producers emit this fixed ordered capability array:

```json
["clone.single_db","cdc.pgoutput","progress.v1","cutover.v1","verify.v1","cleanup.v1","verify.sealed_files.v1"]
```

Non-Linux producers retain the original ordered six:

```json
["clone.single_db","cdc.pgoutput","progress.v1","cutover.v1","verify.v1","cleanup.v1"]
```

The profile covers single-database clone with unsplit text COPY, measured progress, pgoutput protocol v1 follow, persisted endpos and drain, schema/data comparison, and exact owned cleanup.
It admits PostgreSQL 16 to 16, 18 to 18, and 16 to 18, with explicit large-object skipping for follow and `durable` commits.
Binary COPY, splitting, large objects, replay-safe cadence, and relaxed mode remain outside this advertisement.
Operational reports retain their command-specific capability arrays.
The accepted `version-empty-capabilities` and original six-name `version-qualified-profile` fixtures preserve historical discovery syntax.
Both still parse, but the six-name profile lacks the mandatory native M6 sealed-file prerequisite and refuses before credential projection; the empty array also cannot satisfy native clone admission.
The new accepted `version-qualified-linux-profile` fixture records Linux seven-name discovery.
The shared 134-file fixture trees retain the portable historical six as fixture-only `qualified_capabilities`, add fixture-only `qualified_linux_capabilities` for Linux seven, preserve `m1_capabilities: []`, and reserve all ten names in `capabilities`.
These metadata fields do not extend the three-field discovery document.
Discovery MUST NOT contain operational identity or report fields.
Plain `version` retains text identity and libpq version output.
Operational identity and apply-group settings do not gate discovery.

The reserved capability names are `clone.single_db`, `copy.large_objects`, `copy.binary_fallback`, `copy.split`, `cdc.pgoutput`, `progress.v1`, `cutover.v1`, `verify.v1`, `cleanup.v1`, and `verify.sealed_files.v1`.
`verify.sealed_files.v1` identifies compiled support for the private comparison file interface, not universal kernel/NSS/TLS availability or independent database authority.
Runtime descriptor, kernel, file, image, and connection checks remain mandatory.
Unknown or duplicate capabilities MUST fail validation.
A reserved name does not mean that the runtime implements that capability.
Discovery supplies configuration-assessment input; production `UnsupportedEngine`, resolver refusal, helper/PVC/file/SQL qualification, and M6 lifecycle gates remain closed.
Fresh ARM image packaging and exact-image three-pair M6 runtime checks remain required.
AMD64 packaging evidence alone cannot qualify native database runtime; M10 retains that architecture gate.

M8 candidate execution uses fresh native format 9 / metadata 7 while preserving protocol v1 and the existing output DTOs.
Its binary COPY, split, and large-object implementations do not activate their reserved capabilities before the corresponding runtime qualification receipts pass.
The existing format 8 / metadata 6 workflow retains its own artifacts and receipts; no conversion or adoption is permitted.

## Operational transport and document rules

Operational framing requires the validated four-field operator identity bound to stored native state.
Standalone clone, progress, and stream commands emit unframed JSON without a synthetic `work` object.
Unbound standalone output cannot satisfy an operator Report requiring independently supplied work identity.
The M2 container harness can select the actual Go `ParseReport` probe for cross-repository producer qualification; this does not qualify an operator controller workflow.

An operator-bound producer emits each operational record on stdout as `"native-report: "` followed by one JSON object on the same line.
The space after the colon is required.
`ParseReport` receives only that JSON object after framing has been checked.
M6 owns production log-tail selection and MUST bind the selected report to the expected command and work.
The framing fixtures require exactly one matching record in their input; absent, truncated, malformed, and multiple records cannot prove completion.
Unrelated lines carry no operational evidence.

Documents MUST use valid UTF-8, exact member spelling, unique decoded member names, and at most 32 levels of JSON nesting.
Unknown fields, case variants, trailing documents, and trailing non-whitespace bytes MUST fail.
Protocol and counters use exact JSON integer syntax, without floats, exponents, strings, or booleans.
Counters range from zero through signed int64 maximum.
Absent or null measurements stay unknown; they MUST NOT become measured zero or completion evidence.
When both are measured, `done` MUST NOT exceed `total`.

Every operational Report requires `protocol_version`, `engine_version`, `command`, `capabilities`, `work`, and `phase`.
Phases are `planning`, `copying`, `finalizing`, `streaming`, `draining`, `completed`, `failed`, and `suspended`.
The command selects its required capability and payload:

| Command | Capability | Payload |
| --- | --- | --- |
| `clone` | `clone.single_db` | `copy` |
| `list tables` | `clone.single_db` | `copy` |
| `list progress` | `progress.v1` | `copy` |
| `stream sentinel get` | `cutover.v1` | `replication` |
| `stream sentinel set endpos` | `cutover.v1` | `replication` |
| `stream cleanup` | `cleanup.v1` | `cleanup` |

`list tables` remains a staged runtime refusal despite its reserved DTO shape.
`copy` may contain `tables`, `indexes`, and `bytes`, each with nullable or absent `total` and `done` counters.
A parsed `completed` phase alone does not prove that all required counters were measured or completed.
M6 must require positive command-specific completion evidence.

## Compare stays outside Report

`compare data --json` retains the existing per-table JSON array, including `schema`, `name`, `source.rowcount`, `source.checksum`, and `status`.
Compare progress retains the `"compare progress: "` message prefix and keys `plannedTables`, `comparedTables`, `plannedBytes`, `comparedBytes`, `comparedRows`, `differingTables`, `failedTables`, `elapsedMs`, `final`, `inFlightCount`, and `inFlight`.
Each in-flight entry contains `schema`, `name`, `bytes`, and `elapsedMs`.
The operator's existing compare consumer owns these messages.
`ParseReport` MUST reject compare arrays and compare command reports; a native `verification` payload does not replace this contract.

## Private sealed-file inputs for comparison

The native helper can bind already captured credential/TLS file bytes to comparison children without rewriting the raw DSN or exposing those bytes in argv.
This private interface applies only to `compare schema` and `compare data`; both internal variables absent preserve the ordinary CLI and other commands keep their existing connection path.
It introduces no public CLI option or new Report/compare-array shape.

Set `RPGMIGRATE_INTERNAL_COMPARE_SOURCE_FILES` and `RPGMIGRATE_INTERNAL_COMPARE_TARGET_FILES` together for this mode.
Each is a strict JSON object of at most 1024 bytes, with only `passfile`, `sslrootcert`, `sslcert`, `sslkey`, and `sslcrl` keys.
An integer of at least 3 selects an inherited descriptor; null selects only `/usr/share/rpgmigrate/.native-file-absent`, an omitted key leaves libpq unchanged, and an empty object is valid.
Repeated descriptor values are allowed across options and roles; duplicate keys, unknown keys, invalid values, or only one present map refuse before state access or connection.
Keep map values out of argv and public diagnostics, retain canonical snapshot owners through child closure/postcheck, and inherit only the sorted unique selected descriptor set.

Mapped inputs require Linux 5.11 or newer with working `close_range` / `CLOSE_RANGE_CLOEXEC`; unsupported kernels or failed admission refuse without fallback.
Require an effective-user-owned regular descriptor, mode 0400, zero links, at most 1 MiB, all WRITE/GROW/SHRINK/SEAL seals, and matching retained procfd identity.
Sealed RDWR memfds are valid despite mode 0400; owned per-role duplicates remain alive through libpq and utility completion without adopting or closing borrowed originals.
A null selection requires positive missing-path proof, with exact-image immutability, read-only-rootfs, and mount-shadow qualification still required.

Mapped SQL connections pass the unchanged raw DSN first to native libpq parameter expansion, then only the selected fixed file overrides.
Schema `pg_dump` children inherit only their endpoint’s selected owned descriptors; both internal map variables are removed from utility descendants and tool-discovery children inherit none.
Revalidate retained files before connection or utility spawn; credentials, TLS/database behavior, real `pg_dump`, packaged images, and resolver/PVC qualification remain separate runtime gates.

## Work identity and ownership

Operator runs supply all four environment variables together:

- `RPGMIGRATE_MIGRATION_UID`: the canonical lowercase Migration UUID.
- `RPGMIGRATE_RUN_ID`: the same UUID's 32 lowercase hexadecimal digits without hyphens.
- `RPGMIGRATE_WORKDIR_ID`: the PVC UID, represented as a nonempty label of at most 256 bytes without surrounding whitespace, controls, or path separators.
- `RPGMIGRATE_CHECKPOINT_GENERATION`: canonical decimal `1` for this profile.

The report's `work` object uses `migration_uid`, `run_id`, `workdir_id`, and integer `checkpoint_generation`.
Its values MUST exactly match caller-owned state.
Partial, malformed, noncanonical, or mismatched identity MUST fail.
Standalone runs retain random native run IDs without fabricating Migration/PVC identity.
M2 binds the four validated caller identity values to recorded native state before granting any mutation authority.
Mismatched caller identity and older native formats are explicit refusals.
M4 uses native format 8 and target metadata version 6, with no old-state conversion.
Every table records required `identity_modes` and explicitly present nullable `partition_identity_modes`, with one byte per `all_columns` entry: `0`, `97` (ALWAYS), or `100` (BY DEFAULT).
The root vector is null for an ordinary table or ordinary inheritance child; a declarative partition leaf records its root modes by column name.
Native formats through 7 are refused before database side effects, without conversion.
Follow preserves identity-changing UPDATEs with an atomic DELETE RETURNING and INSERT OVERRIDING SYSTEM VALUE in one statement when the old image cannot prove every ALWAYS identity unchanged.
Unchanged TOAST values come from the deleted row; replay does not use target defaults or consume target sequences.
The target uses replica mode, so ordinary origin-only deferred UNIQUE rechecks are suppressed and are not part of the checkpoint proof.
Immediate UNIQUE and NOT NULL failures roll back the data and checkpoint together, and any ALWAYS/REPLICA callback, including an internal callback, is refused before DML.
COPY generations are separate from the immutable checkpoint generation.

The owned metadata identity is `_rpgmigrate_<run_id>.checkpoint` with recorded schema/table OIDs and existing provenance checks.
Exclude target bookkeeping only by its exact owned object identity.
The operator workdir remains `/work/pgcopydb`; its native marker prevents adoption of legacy state.
The control layout is `control/lock`, `control/endpos.json`, and `control/.endpos.tmp`.
Its strict request binds version, stream, copy generation, slot nonce, target nonce, end LSN, and sealed state.
Never adopt, convert, resume, restart, or clean pgcopydb state or resources.

Application names are `rpgmigrate[<pid>] <role>` with exactly `copy`, `apply`, `receive`, and `compare` roles.
The SQL LIKE prefix is `rpgmigrate[%`.
The future runner flag is `--rpgmigrate-runner-image`, its chart value is `rpgmigrateRunnerImage`, and its default stays empty until image qualification.
The runner executable is `/usr/local/bin/rpgmigrate`.
Matching-major tools live in `/usr/lib/postgresql/16/bin` and `/usr/lib/postgresql/18/bin`; default `psql` is major 18.
M5/M6 implement the image and its wiring.

## Replication, endpos, and drain

Every replication payload MUST explicitly measure `certified_lsn`, including an endpos acknowledgment.
Other fields are `source_durable_head_lsn`, `received_lsn`, `durable_replay_lsn`, `end_lsn`, `end_persisted`, `drained`, `checkpoint_id`, and `drain_proof`.
LSNs use PostgreSQL hexadecimal high/low notation within uint32 bounds.
A measured `certified_lsn` may be `0/0`.
Where both are measured, `certified_lsn` MUST NOT exceed `durable_replay_lsn`.

An endpos acknowledgment requires the caller's nonzero expected endpos, a matching report endpos, and `end_persisted=true`.
A drained assertion additionally requires `drain_proof` with `work`, `checkpoint_id`, `committed=true`, `end_lsn`, `replay_lsn`, and `kind`.
The proof's work and checkpoint token MUST match the report and independent expected authority.
The caller supplies `CheckpointEvidence` from the exact owned target row, with matching checkpoint ID, certified LSN, durable replay LSN, drain end, drain witness, drain kind, and `BarrierConfirmed=true`.
Fixtures supply synthetic trusted bindings separately from report payloads.
Production MUST NOT construct those bindings from the report being checked.

We distinguish coverage evidence from feedback authority:

- `keepalive`: witness W is at least caller endpos E and no greater than the persisted certified LSN.
- `later_begin`: W is exactly `BEGIN.final_lsn`, strictly greater than E, and may exceed both certified and durable replay LSNs.

A later-BEGIN proof can therefore cover E=`0/20` with W=`0/30`, while actual applied=`0/10`, certified=`0/10`, copy anchor=`0/8`, and durable replay=`0/10`.
It MUST NOT raise feedback or reconnect authority to W.
Do not substitute an outer WAL frame position or COMMIT transaction-end position for `BEGIN.final_lsn`.
The source field definitions are in the [PG16 logical replication format](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html) and [PG18 logical replication format](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).

The producer computes durable replay as the barrier-proven maximum of actual durable applied end, keepalive-only certified LSN, and copy anchor.
A visible asynchronous row with an older barrier token does not prove that maximum.
The runtime MUST order the witness after all transactions through E, positively prove those transactions durable, and synchronously commit the witness and token in the same logged authority row before emitting drained.
M6 independently proves that exact row through the same fence and a checked synchronous same-value writing barrier.
A plain SELECT, asynchronous visibility, or polling the row/token cannot set `BarrierConfirmed=true`.
M2/M4 own these runtime ordering and persistence checks; pure JSON acceptance does not test them.

The checkpoint token is `<generation>:<barrier_epoch>:<barrier_counter>` with canonical decimal components.
Generation is positive signed int64 and equals `1` in this profile; epoch is uint64 and may be zero; counter is positive uint64.
A transition keeps the epoch at least as large as its stored value and increments the stored counter by exactly one, refusing overflow.
Restart continues that pair; it does not reset the counter or reuse a pair for different checkpoint/drain contents.
An exact same-value observation UPDATE preserves the existing token.
After completion, the checkpoint, drain record, completion flag, and token are immutable.
After unknown COMMIT, reacquire the fence, validate and lock the same row, and establish a positive synchronous writing barrier before emitting proof.
Token syntax fixtures do not qualify runtime token transitions or unknown-COMMIT recovery.

## Cutover command acknowledgment

The operator MUST stop source writers and outstanding transactions, sample `pg_current_wal_insert_lsn()`, and persist E once before invoking `stream sentinel set endpos` with that explicit E.
Retain E through failures and retries, and hold the writer fence through final sequence synchronization and verification.
The first or changed request is checked against `max(applied.end, copy_anchor.consistent_point)`, excluding certification.
A previously admitted identical E remains valid after a whole crossing transaction advances applied past E.

A scalar endpos response proves local request persistence only.
JSON replication reports and measured replay require positive fenced target observation and checked authority closure.
The explicit-E JSON command may persist E and then return nonzero, with no success JSON, while the worker retains the target fence.
The worker processes E independently of that acknowledgment.
After worker exit, the standalone helper retries the identical E to obtain positive observation and acknowledgment.
Do not resample `--current` on retry or treat exit 3 as acknowledgment.
M6 activation remains gated on qualification of this helper and independent reader; the legacy live-worker exec route alone does not implement it.

## Cleanup

The caller supplies the complete resource obligations by exact `kind`, `name`, `owned`, and `retain` values.
The report's `cleanup` requires `complete` and `resources`; each resource requires `kind`, `name`, `owned`, and `removed`.
Allowed kinds are `replication_slot`, `publication`, `replication_origin`, and `workdir`.
The report MUST contain exactly the caller's inventory without omissions, additions, or duplicate identities.
Complete cleanup requires each owned slot/publication to be removed, including explicit treatment of an already absent owned object.
The owned workdir is retained with `removed=false`.
A recorded replication origin is unowned and unremoved; `--origin` never creates or grants ownership of an origin.
An incomplete cleanup observation can parse successfully while retaining `complete=false`.
Exit zero and an empty or truncated resource inventory cannot prove cleanup completion.
Cleanup first durably records terminal intent from complete confirmed receipts, including exactly one confirmed target receipt; it refuses active workers and uncertain inventories before deletion.
Follow the handoff's [exclusive-administration cleanup boundary](native-cutover-handoff.md#exact-owned-cleanup) through every retry.
Retain target bookkeeping and comparison state as well as the workdir and unowned origin.

## CLI, environment, and failures

The fixture contract records all 43 operator option names with command, syntax, environment binding where present, native-stage disposition, and refusal class.
The complete parser baseline remains pinned to `86bf908adca02d87063849dae775a02c1659c829`.
M1 parser acceptance does not make a staged migration route executable.
Argv cases explicitly use `stage=configuration`; `accept=true` means valid parsing/settings, and `expect.runtime=unsupported` requires runtime refusal.
The local filter file is synthetic parser input, not an admitted native filtering feature.

Keep `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGPASSFILE`, `PGCOPYDB_LOG_JSON`, and `PGCOPYDB_APPLY_GROUP_TXNS` by name.
Resolve CLI connection overrides before validating effective conninfo.
Existing numeric environment validation still precedes numeric CLI overrides.
JSON logging uses the existing boolean grammar; invalid values fail.
Delegate passfile, service, and TLS behavior to stock libpq.

Schema utilities inherit the effective settings from the validated SQL connection and stay on its selected peer, database, and role.
They preserve the selected hostname for TLS and passfile matching, do not re-resolve service selection, and keep credentials in the private child environment rather than argv or diagnostics.
Effective options that cannot be represented in that environment fail before pre-data phase intent is recorded.

The follow apply-group default is 100; a supplied value must be positive uint64 and belongs only to follow.
Keep `durable` as the default.
M4 does not qualify replay-safe cadence or enable `relaxed`; see the handoff for the admitted durability boundary.

Exit codes are `BadArgs=1`, `BadConfig=2`, `BadState=3`, `Pgsql=4`, `Pgctl=5`, `Source=6`, `Target=7`, `Reload=9`, `Internal=12`, and `Fatal=122`.
A recovery gap is `BadState`, exit 3.
JSON diagnostics retain existing fields and include `error_severity="ERROR"` with a safe message beginning `"native:<ErrorKind>: "`.
Error diagnostics add `error_origin` with `file`, `line`, and `column` identifying the native error creation site.
The file is a source basename; build paths, credentials, and row contents MUST NOT appear in diagnostics or fixtures.

## Fixture verification

Both repositories carry identical `manifest.json`, `contract.json`, `cases.json`, and payload files.
The sorted manifest lists SHA-256 for every file except itself.
Missing files, extra files, duplicate paths, empty cases, and changed bytes MUST fail local inventory validation.
Each repository reads only its own fixture directory.
Rust executes discovery and real CLI/settings cases; Go executes version/report validation, framing cases, and the existing compare-progress consumer.
The Python suite tests inventory refusal paths, including mutations whose digests were recomputed.
No test substitutes a duplicate migration runtime for the future producer.

1. Run the portable inventory suite from the engine checkout.

   ```sh
   python3 -W error tools/ci/unit.py
   ```

2. At milestone close, compare the fixture trees using the actual sibling checkouts.

   ```sh
   diff -ru tests/fixtures/native/v1 ../pgcopydb-operator-m4-closing-m6-composed-20261109/internal/native/testdata/v1
   ```

The parity gate is explicit and separate from repository-local unit tests.
The [task ledger](../tasks/todo.md) records closing lint/test results and unresolved gates.
