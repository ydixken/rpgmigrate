# Native protocol v1

We freeze the engine/operator boundary here and in the shared [contract fixtures](../tests/fixtures/native/v1/contract.json).
The [M1 plan](superpowers/plans/2026-11-08-m1-native-contract.md) defines the implementation boundary.
M1 defines discovery and pure contract validation.
Its empty capability fixture remains accepted parser evidence.
Discovery describes the narrowed core profile and the Linux-only compiled sealed-file comparison interface below.
Platform advertisement does not replace its runtime prerequisites.
New executable and image bytes require their own packaging and M6 runtime evidence before operator activation.
Shared fixture payloads remain synthetic contracts, separate from actual producer checks.
Use the [Native cutover handoff](native-cutover-handoff.md) for M4 command behavior, native format 8, metadata version 6, and the M6 observation procedure.
Neither fixtures nor parser acceptance qualify follow, recovery, cutover, cleanup, PostgreSQL version support, or durability.
The primary baseline remains format 8 / metadata 6.
Isolated M8 and M9 candidates do not change that contract.

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
Both still parse, but the six-name profile lacks the mandatory native M6 sealed-file prerequisite and refuses before credential projection.
The empty array also cannot satisfy native clone admission.
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
Discovery supplies configuration-assessment input.
Production `UnsupportedEngine`, resolver refusal, helper/PVC/file/SQL qualification, and M6 lifecycle gates remain closed.
Fresh ARM image packaging and exact-image three-pair M6 runtime checks remain required.
AMD64 packaging evidence alone cannot qualify native database runtime.
M10 retains that architecture gate.

M8 candidate execution uses fresh native format 9 / metadata 7 while preserving protocol v1 and the existing output DTOs.
Its binary COPY, split, and large-object implementations do not activate their reserved capabilities before the corresponding runtime qualification receipts pass.
The existing format 8 / metadata 6 workflow retains its own artifacts and receipts.
The candidate refuses conversion or adoption.

## Operational transport and document rules

Operational framing requires the checked four-field operator identity bound to stored native state.
Standalone clone, progress, and stream commands emit unframed JSON without a synthetic `work` object.
Unbound standalone output cannot satisfy an operator Report requiring independently supplied work identity.
The M2 container harness can select the actual Go `ParseReport` probe for cross-repository producer qualification.
This does not qualify an operator controller workflow.

An operator-bound producer emits each operational record on stdout as `"native-report: "` followed by one JSON object on the same line.
The space after the colon is required.
`ParseReport` receives only that JSON object after framing has been checked.
M6 owns production log-tail selection and MUST bind the selected report to the expected command and work.
The framing fixtures require exactly one matching record in their input.
Absent, truncated, malformed, and multiple records cannot prove completion.
Unrelated lines carry no operational evidence.

Documents MUST use valid UTF-8, exact member spelling, unique decoded member names, and at most 32 levels of JSON nesting.
Unknown fields, case variants, trailing documents, and trailing non-whitespace bytes MUST fail.
Protocol and counters use exact JSON integer syntax, without floats, exponents, strings, or booleans.
Counters range from zero through signed int64 maximum.
Absent or null measurements stay unknown.
They MUST NOT become measured zero or completion evidence.
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
`ParseReport` MUST reject compare arrays and compare command reports.
A native `verification` payload does not replace this contract.

## Private sealed-file inputs for comparison

The native helper can bind already captured credential/TLS file bytes to comparison children without rewriting the raw DSN or exposing those bytes in argv.
This private interface applies only to `compare schema` and `compare data`.
Both internal variables absent preserve the ordinary CLI and other commands keep their existing connection path.
It introduces no public CLI option or new Report/compare-array shape.

Set `RPGMIGRATE_INTERNAL_COMPARE_SOURCE_FILES` and `RPGMIGRATE_INTERNAL_COMPARE_TARGET_FILES` together for this mode.
Each is a strict JSON object of at most 1024 bytes, with only `passfile`, `sslrootcert`, `sslcert`, `sslkey`, and `sslcrl` keys.
An integer of at least 3 selects an inherited descriptor.
Null selects only `/usr/share/rpgmigrate/.native-file-absent`, an omitted key leaves libpq unchanged, and an empty object is valid.
Repeated descriptor values are allowed across options and roles.
Duplicate keys, unknown keys, invalid values, or only one present map refuse before state access or connection.
Keep map values out of argv and public diagnostics, retain canonical snapshot owners through child closure/postcheck, and inherit only the sorted unique selected descriptor set.

Mapped inputs require Linux 5.11 or newer with working `close_range` / `CLOSE_RANGE_CLOEXEC`.
Unsupported kernels or failed admission refuse without fallback.
Require an effective-user-owned regular descriptor, mode 0400, zero links, at most 1 MiB, all WRITE/GROW/SHRINK/SEAL seals, and matching retained procfd identity.
Sealed RDWR memfds are valid despite mode 0400.
Owned per-role duplicates remain alive through libpq and utility completion without adopting or closing borrowed originals.
A null selection requires positive missing-path proof, with exact-image immutability, read-only-rootfs, and mount-shadow qualification still required.

Mapped SQL connections pass the unchanged raw DSN first to native libpq parameter expansion, then only the selected fixed file overrides.
Schema `pg_dump` children inherit only their endpoint’s selected owned descriptors.
Both internal map variables are removed from utility descendants and tool-discovery children inherit none.
Revalidate retained files before connection or utility spawn.
Credentials, TLS/database behavior, real `pg_dump`, packaged images, and resolver/PVC qualification remain separate runtime gates.

## Utility connection settings

Schema `pg_dump` and `pg_restore` children get the effective connection settings through libpq environment variables, never argv.
They connect to the peer the engine already validated: `PGHOST`, `PGHOSTADDR`, `PGPORT`, `PGDATABASE` and `PGUSER` are pinned from the live connection.
libpq has no environment variable for the client liveness settings `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count` and `tcp_user_timeout`.
We forward exactly these five through the utility's `--dbname` connection string, for example `--dbname=keepalives=1 keepalives_idle=10`, because the operator appends them to every native URI for dead-peer detection.
libpq expands that string and still takes the database, host, port, user and password from the environment.
Each value MUST be ASCII digits only; any other value refuses.
Without these settings, `pg_dump` gets no `--dbname` and `pg_restore` gets an empty `--dbname=` as before.

> [!important]
> Every other set option without a libpq environment variable, such as `sslpassword`, `oauth_client_secret`, `scram_client_key`, `replication` or a caller-supplied `fallback_application_name`, refuses with exit 2 (`native:BadConfig`) before any mutation.
> Secret-bearing and security settings never reach utility argv.

## Materialized views

Clone and follow admit materialized views that use the `heap` access method; foreign tables, unlogged tables and other access methods still refuse.
A schema-only archive creates every materialized view `WITH NO DATA` and carries no `MATERIALIZED VIEW DATA` entries, on both PostgreSQL 16 and 18, so `pg_restore` never refreshes one.
The engine does it itself, after the post-data restore and before the sequence sync.
It refreshes each view that was populated in the copy snapshot, in `pg_dump`'s dependency order, which follows references through plain views, so a view over another view refreshes after it.
A view that was unpopulated in the snapshot stays unpopulated.
The refresh runs with `search_path` empty and `row_security` off, as `pg_restore` runs its own refresh entries.
The engine then reads every view's population state back and fails unless it matches the snapshot.

> [!warning]
> Clone and follow refuse a source where a populated materialized view depends on an unpopulated one, at the schema dump, before pre-data restores anything.
> A refresh on the target would fail, and `pg_dump` would silently leave the view empty.
> Refresh the dependency on the source, or run `REFRESH MATERIALIZED VIEW ... WITH NO DATA` on the dependent view, then start again.

With `--follow`, the refresh reads the tables as the copy left them, before apply starts.
Follow replicates tables only: it keeps each materialized view's content from the copy snapshot and does not refresh at cutover.
Refresh the views on the target after cutover when their content must reflect later changes.
`compare data` counts no materialized view as a table, and `compare schema` sees only their definitions.

## Large-object admission

With large objects selected, clone and follow refuse a target that holds a large object this run does not own, before the pre-data restore.
A fresh run owns none, so any target large object refuses.
A restart of the copy admits exactly the OIDs that this run's durable large-object receipts record, and refuses any other before the owned reset.
That includes a restart that first finishes an interrupted generation advance, in clone or follow, so the refusal comes before any target write.
The refusal names the count and the lowest unowned OID.
The check reads `pg_catalog.pg_largeobject_metadata` directly, because an object owned by the bootstrap superuser has no `pg_shdepend` row.
With `--skip-large-objects` the check does not apply.
Each large-object permit checks the inventory again, so an object created on the target after planning still refuses before any object copy.

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
M2 binds the four checked caller identity values to recorded native state before granting any mutation authority.
Mismatched caller identity and older native formats are explicit refusals.
M4 uses native format 8 and target metadata version 6, with no old-state conversion.
Every table records required `identity_modes` and explicitly present nullable `partition_identity_modes`, with one byte per `all_columns` entry: `0`, `97` (ALWAYS), or `100` (BY DEFAULT).
The root vector is null for an ordinary table or ordinary inheritance child.
A declarative partition leaf records its root modes by column name.
Native formats through 7 are refused before database side effects, without conversion.
Follow preserves identity-changing UPDATEs when the old image cannot prove every ALWAYS identity unchanged.
It uses atomic `DELETE RETURNING` and `INSERT OVERRIDING SYSTEM VALUE` in one statement.
Unchanged TOAST values come from the row returned by `DELETE`.
Replay does not use target defaults or consume target sequences.
The target uses replica mode, so ordinary origin-only deferred UNIQUE rechecks are suppressed and are not part of the checkpoint proof.
Immediate UNIQUE and NOT NULL failures roll back the data and checkpoint together, and any ALWAYS/REPLICA callback, including an internal callback, is refused before DML.
COPY generations are separate from the immutable checkpoint generation.

The owned metadata identity is `_rpgmigrate_<run_id>.checkpoint` with recorded schema/table OIDs and existing provenance checks.
Exclude target bookkeeping only by its exact owned object identity.
The operator workdir remains `/work/pgcopydb`.
Its native marker prevents adoption of legacy state.
The control layout is `control/lock`, `control/endpos.json`, and `control/.endpos.tmp`.
Its strict request binds version, stream, copy generation, slot nonce, target nonce, end LSN, and sealed state.
Never adopt, convert, resume, restart, or clean pgcopydb state or resources.

Application names are `rpgmigrate[<pid>] <role>` with exactly `copy`, `apply`, `receive`, and `compare` roles.
The engine sets the name as libpq's `fallback_application_name` at connect time, so an `application_name` the caller pins in its conninfo or `PGAPPNAME` still wins.
A caller's own `fallback_application_name`, from the conninfo or a service file, also keeps precedence, and utilities still refuse it.
The phase that opens a session picks its role:

- `receive`: replication-protocol connections, the source health check, slot or publication creation, restart and cleanup, including `stream cleanup` on both sides, and the `--follow` source snapshot session.
- `copy`: planning, plain clone snapshot export and import, schema, table and sequence copy, ANALYZE, large objects, `ping`, `copy table-data`, and every `pg_dump` and `pg_restore` child.
- `apply`: apply, checkpoint, barrier, fence and drain sessions, final source sequence synchronization, and the `stream sentinel` commands.
- `compare`: `compare schema` and `compare data`.

`<pid>` is the engine process ID, including in a child's name.
Children get the name through `PGAPPNAME`, because `pg_dump` and `pg_restore` pass their own `fallback_application_name`.
The SQL LIKE prefix is `rpgmigrate[%`.
The future runner flag is `--rpgmigrate-runner-image`, its chart value is `rpgmigrateRunnerImage`, and its default stays empty until image qualification.
The runner executable is `/usr/local/bin/rpgmigrate`.
Matching-major tools live in `/usr/lib/postgresql/16/bin` and `/usr/lib/postgresql/18/bin`.
Default `psql` is major 18.
M5/M6 implement the image and its wiring.

### Session timeouts

Every engine session sets `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout` and `idle_session_timeout` to `0`, and `transaction_timeout` to `0` on PostgreSQL 17 and later.
The first four travel as `-c` switches in the startup packet's `options`, SQL and replication sessions alike.
We send them there because the server arms `idle_session_timeout` as soon as it reports ready for a query: under CPU contention a 20 ms database setting ended sessions before their first `SET` arrived.
The engine appends its switches after any `options` the caller passes in the conninfo, a service file or `PGOPTIONS`, so the caller's other switches are kept and the engine's values win.
A startup switch is a client setting, which outranks database, role and role-in-database settings (`GucSource` in `src/include/utils/guc.h`, applied by `process_startup_options` before `process_settings` in `postinit.c`, on 16 and 18 alike).
PostgreSQL 16 rejects an unknown name in startup options, so `transaction_timeout` is a `SET` instead: the first statement of a SQL session, and part of a replication session's setup before `CREATE_REPLICATION_SLOT` or `START_REPLICATION`.
It counts only inside a transaction, so the idle gap before that `SET` does not expose it.
The engine then reads all of them back and refuses the session unless each is `0`.

> [!important]
> A connection pooler in front of either server must forward the `options` startup parameter, for SQL and replication sessions.
> Otherwise every engine session fails closed: a pooler that rejects the parameter fails the connection, and one that strips it leaves any server timeout in force, which the read-back then refuses.

We clear them because a copy, an index build or a comparison legitimately outlasts any production value, and because the engine's own copy workers queue on the owned checkpoint lock, so even `lock_timeout` fails a clone without outside contention.
`pg_dump` and `pg_restore` inherit the connection's options, startup switches included, through `PGOPTIONS`, and also clear the first four and `transaction_timeout` themselves.
The engine adds `-c idle_session_timeout=0` to their `PGOPTIONS` as well, because they never clear that one and an idle parallel restore worker would otherwise be ended between items.

> [!warning]
> The source snapshot session stays in one `REPEATABLE READ READ ONLY` transaction from snapshot import to the end of the table copy.
> It holds an `ACCESS SHARE` lock on every copied table, its indexes and its sequences, and its snapshot holds back the source's removal horizon.
> A source `ALTER TABLE`, `TRUNCATE`, `DROP` or `VACUUM FULL` on a copied table waits for the whole copy, and queries queued behind that DDL wait with it.
> Source sessions keep `lock_timeout` at `0`, as `pg_dump` does without `--lock-wait-timeout`, so an engine session that meets such DDL waits for it instead of failing.
> Run source DDL on copied tables before the clone starts or after it ends.

With `--follow` the source snapshot session runs as `receive`, because it is a replication-resource session: it creates the publication, imports the slot's snapshot and checks the copy plan, its publication membership and its type map under that snapshot.
A plain clone names the same session `copy`.

## Follow admission

Follow checks the constraints and indexes on each published table, and on its partition ancestors, before it creates a slot or publication.
It checks the target table again before applying the first change to it.
It admits table and domain CHECK constraints, expression indexes and partial indexes when each stored expression meets all of these rules:

1. Every function and operator is an immutable built-in in `pg_catalog`.
2. Every node is an operator, function, boolean, `CASE`, `COALESCE`, `NULL` test, array, relabel, `COLLATE`, column or constant node, so an I/O cast such as `ts::text` refuses.
3. Every collation it uses is deterministic and its recorded version matches the server's.

A `NOT VALID` CHECK, an exclusion constraint and an expression that calls a user or extension function still refuse.
An index operator class must be built in or belong to an extension.
For an extension operator class, follow takes the target's extension version: the installed one, or else the default version that pre-data installs.
It admits the class when that version equals the source's, or when the target's `pg_extension_update_paths` lists a path from the source's version to it.
It refuses when the target neither holds nor offers the extension, and when no path leads from the source's version, which includes a downgrade and an installed version the source's cannot reach.
We check versions because the operator class carries the extension's semantics.
An update path stands in for equal semantics, the same assumption `ALTER EXTENSION UPDATE` and `pg_upgrade` make, so `citext` 1.6 on PostgreSQL 16 follows into `citext` 1.8 on PostgreSQL 18.

> [!warning]
> Follow checks extension versions only when it plans a copy: on the first run and on a restart that copies again.
> An `ALTER EXTENSION UPDATE` on either side during streaming is not re-checked.

The replica identity index keeps its stricter rule: one valid immediate built-in btree unique index, without expressions or predicates.
Partition keys still need plain columns and built-in operator classes.

### Generated columns

pgoutput leaves generated columns out of every Relation message and tuple on PostgreSQL 16, and on 18 while the publication keeps `publish_generated_columns=none`.
Follow therefore applies only the published columns, and the target computes its generated columns from its own expressions.
Follow admits stored generated columns on 16 and 18, and virtual generated columns on 18, when each generation expression meets the expression rules above.
It still refuses:

1. a generation expression that calls a user or extension function, or uses a nondeterministic or stale collation;
2. a replica identity index that contains a generated column, because 16 then sends an empty key and 18 refuses UPDATE and DELETE on the source;
3. a `REPLICA IDENTITY FULL` table with any generated column on an 18 source, because 18 refuses UPDATE and DELETE on it once the publication omits that column.

Follow refuses the third case before it creates any slot or publication, and keeps `publish_generated_columns=none`.
To follow such a table from an 18 source, give it a primary key with `REPLICA IDENTITY DEFAULT`, or `REPLICA IDENTITY USING INDEX`, on columns that are not generated.

The target table must hold the same columns in the same order, with a generated column wherever the plan has one, and each target generation expression must meet the same rules.

Each publication member must match the plan exactly on both majors:

1. `pg_publication_tables.attnames` lists the table's non-dropped columns whose `attgenerated` is empty, in `attnum` order.
2. The table's non-dropped columns, in `attnum` order, are the plan's full column list, so an added generated column still differs.
3. `rowfilter` is null and the member's `pg_publication_rel.prattrs` is null, so a column list that names every writable column still refuses.
4. The publication's `pubgencols` is `n` on 18; 16 has no such column.

### Slot creation recovery

Follow records a slot intent (the run's stream, slot name, plugin, copy generation and a nonce) before `CREATE_REPLICATION_SLOT`, and its receipt (consistent point and snapshot name) after the command returns.
One intent authorizes one CREATE.
Follow checks that no slot holds the name before it creates the publication, and again after it records the intent, immediately before the CREATE.
A slot that the repeated check finds is not this run's, because the CREATE was never sent: follow retires the intent durably and refuses with exit 3, `follow-source: existing slot or publication is never adopted`.
A complete `ErrorResponse` with SQLSTATE 42710, as the command's only result and followed by `ReadyForQuery`, proves that the server's name check refused the CREATE before it allocated a slot.
PostgreSQL raises 42710 there only (16.15 `slot.c:312-314`, 18.7 `slot.c:412-414`).
Follow retires that intent durably without any drop and refuses with exit 3, `follow-source: slot <name> already exists, so this run's CREATE created nothing; it is never adopted or dropped`.
Every other error, and a lost or incomplete reply, keeps the intent, because the server can fail after it persisted the slot.
For example, `ReplicationSlotPersist` marks the slot persistent before `SaveSlotToPath` can raise an error, and that error leaves the slot in place.
A start that finds an intent without its receipt, on a first run's retry or on a restart, drops the slot of exactly that name, records its absence, and creates the slot again under a new intent.
It drops the slot only when it is a logical `pgoutput` slot on the source database, is not temporary, has `two_phase`, `failover` and `synced` off as this run's CREATE leaves them, and has no active backend.
That check and the drop are one statement, which must drop exactly one slot or none; when none, the start reads the slot again to name the refusal.
It never adopts the slot, because the exported snapshot ended with the session that created it.
A slot of another kind under that name refuses with exit 3 and stays, and so does an active slot of this run's shape, naming the slot and its backend.
A start drops an unbound slot that has a receipt the same way, on a first run's retry or on a restart, and only at its exact consistent point, so a slot from a run killed before its binding does not stay behind retaining WAL.
A start retires this run's pending slot, proven by its intent or its unbound receipt, before any check that does not concern it.
Only the source identity that the plan recorded is checked first; a first run then retires the slot before its plan and inventory comparisons, and a restart before the rest of its source preflight and its pending publication checks, so no later refusal leaves the slot retaining WAL.

> [!important]
> Two migrations must never share a slot name on one source.
> The operator passes one stable `--slot-name` across runs and generations, so the engine's default per-run name does not separate them.
> A pending intent has no receipt, so the start identifies its slot only by the recorded name and the shape check above.
> A foreign slot of this run's shape is therefore dropped when it takes the name after this run's last completed collision check and this run ends without the outcome of its CREATE.
> That covers a CREATE never sent, a lost reply, and an unfinished slot that ended with its session, after which the name stays free until the next start.
> While this run's CREATE holds the name, from the server's name check until its session ends, any concurrent CREATE under the name fails with 42710.

### Publication creation recovery

Follow records a publication intent (name and nonce) before `BEGIN`, and its receipt (name, OID and owner, read inside the creating transaction) before it sends `COMMIT`.
The receipt reaches the native manifest through a file fsync, a rename and a directory fsync, so an intent without a receipt never sent `COMMIT` and never committed.
A start that finds an unconfirmed publication, on a first run's retry or on an unbound restart, resolves it before any slot intent:

1. With only an intent, no publication under the recorded name means the CREATE rolled back; the start retires the intent and creates the publication again under a new one.
2. With a receipt, no publication under the name means its `COMMIT` failed, and the start does the same.
3. A publication with exactly the recorded OID and owner committed without its reply reaching the engine.
   The start drops it and creates it again; it never adopts it.
   The dropping transaction reads the OID again and, after `DROP PUBLICATION`, requires that the object lock the drop took names that OID, so a publication that replaced it meanwhile rolls the drop back.
4. Any other publication under the name is not this run's: the start refuses with exit 3, naming the publication and its OID, and leaves it and the native state unchanged.

### Restart before the pre-data restore

A clone or follow killed after it bound its snapshot and before it recorded the pre-data handoff is in phase `fresh`.
We restart it through the copy restart (roadmap decision 8), because the target holds nothing of it but the run's bookkeeping schema:

1. The target bookkeeping intent is resolved first.
   The intent's nonce is written inside the creating transaction, so a schema whose row carries it is this run's and is used; an absent one is created again under the same intent.
2. The restart takes a new snapshot under the next copy generation.
   A follow drops its slot and publication by their bound receipts before it creates new ones.
3. No table creation is recorded in phase `fresh`, so the restart truncates and reconciles no table and skips the pre-data comparison.
4. It then restores pre-data from the new snapshot and records the handoff as a first run does.

A clone accepts this restart from `--restart` or `--resume --not-consistent`, and a follow from `--restart` or `--resume`.
Phase `pre_data_pending` still refuses, because the utility's outcome on the target is unknown.

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

A later-`BEGIN` proof can therefore cover E=`0/20` with W=`0/30`, while actual applied=`0/10`, certified=`0/10`, copy anchor=`0/8`, and durable replay=`0/10`.
It MUST NOT raise feedback or reconnect authority to W.
Do not substitute an outer WAL frame position or COMMIT transaction-end position for `BEGIN.final_lsn`.
The source field definitions are in the [PG16 logical replication format](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html) and [PG18 logical replication format](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).

The producer computes durable replay as the barrier-proven maximum of actual durable applied end, keepalive-only certified LSN, and copy anchor.
A visible asynchronous row with an older barrier token does not prove that maximum.
The runtime MUST order the witness after all transactions through E and positively prove those transactions durable.
Before emitting drained, it MUST synchronously commit the witness and token in the same logged authority row.
M6 independently proves that exact row through the same fence and a checked synchronous same-value writing barrier.
A plain SELECT, asynchronous visibility, or polling the row/token cannot set `BarrierConfirmed=true`.
M2/M4 own these runtime ordering and persistence checks.
Pure JSON acceptance does not test them.

The checkpoint token is `<generation>:<barrier_epoch>:<barrier_counter>` with canonical decimal components.
Generation is positive signed int64 and equals `1` in this profile.
Epoch is uint64 and may be zero.
Counter is positive uint64.
A transition keeps the epoch at least as large as its stored value and increments the stored counter by exactly one, refusing overflow.
Restart continues that pair.
It does not reset the counter or reuse a pair for different checkpoint/drain contents.
An exact same-value observation UPDATE preserves the existing token.
After completion, the checkpoint, drain record, completion flag, and token are immutable.
After unknown COMMIT, reacquire the fence, check and lock the same row, and establish a positive synchronous writing barrier before emitting proof.
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
M6 activation remains gated on qualification of this helper and independent reader.
The legacy live-worker exec route alone does not implement it.

## Cleanup

The caller supplies the complete resource obligations by exact `kind`, `name`, `owned`, and `retain` values.
The report's `cleanup` requires `complete` and `resources`.
Each resource requires `kind`, `name`, `owned`, and `removed`.
Allowed kinds are `replication_slot`, `publication`, `replication_origin`, and `workdir`.
The report MUST contain exactly the caller's inventory without omissions, additions, or duplicate identities.
Complete cleanup requires each owned slot/publication to be removed, including explicit treatment of an already absent owned object.
The owned workdir is retained with `removed=false`.
A recorded replication origin is unowned and unremoved.
`--origin` never creates or grants ownership of an origin.
An incomplete cleanup observation can parse successfully while retaining `complete=false`.
Exit zero and an empty or truncated resource inventory cannot prove cleanup completion.
Cleanup first durably records terminal intent from complete positively established receipts, including exactly one positively established target receipt.
It refuses active workers and uncertain inventories before deletion.
Follow the handoff's [exclusive-administration cleanup boundary](native-cutover-handoff.md#exact-owned-cleanup) through every retry.
Retain target bookkeeping and comparison state as well as the workdir and unowned origin.

## CLI, environment, and failures

The fixture contract records all 43 operator option names with command, syntax, environment binding where present, native-stage disposition, and refusal class.
The complete parser baseline remains pinned to `86bf908adca02d87063849dae775a02c1659c829`.
M1 parser acceptance does not make a staged migration route executable.
Argv cases explicitly use `stage=configuration`.
`accept=true` means valid parsing/settings, and `expect.runtime=unsupported` requires runtime refusal.
The local filter file is synthetic parser input, not an admitted native filtering feature.

Keep `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGPASSFILE`, `PGCOPYDB_LOG_JSON`, and `PGCOPYDB_APPLY_GROUP_TXNS` by name.
Resolve CLI connection overrides before validating effective conninfo.
Existing numeric environment validation still precedes numeric CLI overrides.
JSON logging uses the existing boolean grammar.
Invalid values fail.
Delegate passfile, service, and TLS behavior to stock libpq.

Schema utilities inherit the effective settings from the checked SQL connection and stay on its selected peer, database, and role.
They preserve the selected hostname for TLS and passfile matching and do not re-resolve service selection.
They keep credentials in the private child environment rather than argv or diagnostics.
Effective options that cannot be represented in that environment fail before pre-data phase intent is recorded.

The follow apply-group default is 100.
A supplied value must be positive uint64 and belongs only to follow.
Keep `durable` as the default.
M4 does not qualify replay-safe cadence or enable `relaxed`.
See the handoff for the admitted durability boundary.

Exit codes are `BadArgs=1`, `BadConfig=2`, `BadState=3`, `Pgsql=4`, `Pgctl=5`, `Source=6`, `Target=7`, `Reload=9`, `Internal=12`, and `Fatal=122`.
A recovery gap is `BadState`, exit 3.
JSON diagnostics retain existing fields and include `error_severity="ERROR"` with a safe message beginning `"native:<ErrorKind>: "`.
Error diagnostics add `error_origin` with `file`, `line`, and `column` identifying the native error creation site.
The file is a source basename.
Build paths, credentials, and row contents MUST NOT appear in diagnostics or fixtures.

## Fixture verification

Both repositories carry identical `manifest.json`, `contract.json`, `cases.json`, and payload files.
The sorted manifest lists SHA-256 for every file except itself.
Missing files, extra files, duplicate paths, empty cases, and changed bytes MUST fail local inventory validation.
Each repository reads only its own fixture directory.
Rust executes discovery and real CLI/settings cases.
Go executes version/report validation, framing cases, and the existing compare-progress consumer.
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
