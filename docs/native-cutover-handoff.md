# Native cutover handoff

This document freezes the M4 producer layout and the M6 independent-reader requirements.
The [native protocol](native-protocol.md) defines report validation; the [operator contract](operator-compatibility.md) defines caller duties.
The [roadmap](roadmap.md) and [task ledger](../tasks/todo.md) own qualification status.
Discovery keeps `capabilities=[]` until the required producer, PostgreSQL 16/18, matrix, and independent review gates pass.

## Format and authority

M4 uses native format 8, target metadata version 6, and exactly 29 checkpoint columns.
Older formats are refused before effects without conversion.
Never adopt, resume, restart, mutate, convert, or clean pgcopydb state or resources.
The source-WAL slot remains the recovery log; no local COPY-phase spool replaces it.
The admitted mode is `durable`, with synchronous target durability before progress authorizes feedback.
M4 does not qualify replay-safe cadence or enable `relaxed`.
The later relaxed profile requires explicit finite transaction/byte caps and durable run-wide reservations; its design is not an availability claim.

Use the exact confirmed native receipts, not names, prefixes, reports, or catalog discovery, to identify owned bookkeeping.
The checkpoint relation is `_rpgmigrate_<run_id>.checkpoint` in its recorded schema/table OIDs.
The confirmed target receipt also records the copy-marker table and index OIDs.
Require exactly one confirmed target receipt before cleanup intent; empty, provisioning, or extra target receipts cannot authorize deletion.
Keep exclusions from comparison tied to those exact recorded objects.

The checkpoint definition below shows the exact column order, types, nullability, collation, primary key, and heap access method emitted by `src/apply/metadata.rs` for the admitted servers.
The producer qualifies the table name with the recorded schema and revokes PUBLIC privileges on the schema and tables.
This is a layout reference, not a provisioning or repair procedure.

```sql
CREATE TABLE checkpoint (
    magic pg_catalog.text COLLATE pg_catalog."C" NOT NULL,
    version pg_catalog.int4 NOT NULL,
    run_id pg_catalog.bytea NOT NULL,
    nonce pg_catalog.bytea NOT NULL,
    source_system pg_catalog.numeric(20,0) NOT NULL,
    source_database pg_catalog.oid NOT NULL,
    generation pg_catalog.numeric(20,0) NOT NULL,
    target_system pg_catalog.numeric(20,0),
    target_database pg_catalog.oid NOT NULL,
    owner_oid pg_catalog.oid NOT NULL,
    schema_oid pg_catalog.oid NOT NULL,
    table_oid pg_catalog.oid NOT NULL,
    applied_order pg_catalog.numeric(20,0) NOT NULL,
    last_xid pg_catalog.oid,
    begin_ordinal pg_catalog.numeric(20,0),
    source_commit_lsn pg_catalog.pg_lsn,
    source_end_lsn pg_catalog.pg_lsn,
    barrier_epoch pg_catalog.numeric(20,0) NOT NULL,
    barrier_counter pg_catalog.numeric(20,0) NOT NULL,
    copy_snapshot_token pg_catalog.text COLLATE pg_catalog."C",
    copy_consistent_point pg_catalog.pg_lsn,
    copy_plan_nonce pg_catalog.bytea,
    clone_state pg_catalog.text COLLATE pg_catalog."C",
    certified_lsn pg_catalog.pg_lsn,
    follow_binding pg_catalog.text COLLATE pg_catalog."C",
    drain_end_lsn pg_catalog.pg_lsn,
    drain_witness_lsn pg_catalog.pg_lsn,
    drain_kind pg_catalog.text COLLATE pg_catalog."C",
    follow_completed pg_catalog.bool NOT NULL,
    PRIMARY KEY (run_id, source_system, source_database, generation)
) USING heap;
```

The reader MUST validate the actual catalog and row before interpreting positions.
Require a logged ordinary heap, exact schema/table/database/owner OIDs and names, the built-in primary-key index on columns 3/5/6/7, and exactly the expected column OIDs, modifiers, nullability, and C text collations.
Reject extra/dropped/generated/identity/default columns, unexpected indexes or constraints, RLS/policies, triggers, rewrite rules, inheritance/partition membership, foreign storage, and unexpected schema objects/dependencies.
PostgreSQL 18's validated enforced local NOT NULL catalog records are the explicit catalog exception.
Validate copy-marker objects against their recorded OIDs as the producer does.
Require the recorded owner and the effective writer's schema USAGE and table SELECT/INSERT/UPDATE privileges with the producer's restrictive ACL checks; owner membership is not a substitute.
Read `ONLY` the exact relation and require exactly one complete 29-column row; missing or multiple rows refuse.
The canonical producer checks live in `src/apply/metadata.rs`; M6 must retain their strictness rather than treating this prose as a weaker alternate validator.

## Provenance bindings

The first twelve row columns bind `magic="rpgmigrate"`, metadata version 6, run ID, target provisioning nonce, source system/database, stream generation, optional target system, target database, owner, and schema/table OIDs to confirmed provenance.
The stream consists of run ID, source system/database identity, and immutable checkpoint generation 1.
The target intent binds that stream, optional system identifier, database OID, schema/table names, owner OID, and metadata version to a nonzero 16-byte nonce.
The confirmed receipt adds schema, checkpoint table, marker table, and marker index OIDs.
Reverify a persisted target system identifier when present; a restricted role's absence of that measurement cannot silently discard stored evidence.

The source slot receipt binds the stream, plugin `pgoutput`, slot name, catalog epoch, creation nonce, consistent point, and exported snapshot token.
The publication receipt binds its exact name, OID, and owner OID; validate publication options and table membership against the saved plan before effects.
`follow_binding` is strict bounded JSON containing `binding`, explicit nullable `operator`, and explicit nullable `origin_name`.
Its binding contains the slot receipt, publication receipt, source timeline, COPY generation, and source type OID/name map.
The source timeline and COPY generation are distinct from immutable checkpoint generation.
Operator identity contains the exact Migration UID, run ID, workdir ID, and checkpoint generation supplied independently by the caller.
Standalone state has no fabricated operator identity and cannot produce operator-bound evidence.

The private workdir contains `manifest.json` and the separately locked `control/lock`, `control/endpos.json`, and `control/.endpos.tmp` layout.
The request is strict bounded JSON with `version=1`, `stream`, `copy_generation`, `slot_nonce`, `target_nonce`, `end_lsn`, and `sealed`.
It is not a second target durability authority.
Descriptor-relative identity checks and synchronization protect the supported single-owner publication path; require exclusive control of the private directory.
Permissions and locks do not protect against arbitrary noncooperating same-user mutation.

## Cutover and retry

The caller MUST stop source writers and outstanding transactions before sampling `pg_current_wal_insert_lsn()`.
Persist this nonzero E once in caller-owned state before invoking set-endpos, and keep the writer fence through drain, final sequence synchronization, and verification.
Pass the explicit saved E on every retry.
Do not sample `pg_current_wal_lsn()` or a flush position instead, and do not resample `--current` after failure.

The implemented command paths are `stream sentinel get`, `stream sentinel set endpos`, and `stream cleanup`.
They require existing native state; unsupported sentinel mutations remain refusals.
`get --endpos` reads the persisted local E without requiring a target measurement.
`get --replay-lsn` requires target observation and returns one canonical LSN plus newline.
`get --json`, including either selector with `--json`, requires target observation and returns the replication document rather than a scalar.
`get` without a selector also emits the replication document and requires a target measurement.
`set endpos` accepts one explicit LSN operand or `--current` with `--source`; both forms require `--target` for admission.
`--current` validates source identity/timeline before sampling exactly `pg_current_wal_insert_lsn()` and is suitable only for an initial standalone request.
`cleanup` requires both `--source` and `--target` before opening native state and returns its cleanup document.
Connection environment precedence and workdir selection retain the existing CLI contract.

A first or changed E must be at least `max(applied.end, copy_anchor.consistent_point)`; certification is excluded from that admission floor.
Repeated identical E is idempotent, and forward changes are allowed before sealing.
Backward changes and new or changed requests after sealing refuse.
A whole crossing transaction C may advance applied beyond already admitted E.
Preserve that E during sealing, completion, identical retries, and crash resume; do not reapply the first-publication floor to it.
A genuinely new or changed backdated request still refuses.

Scalar set-endpos success proves local persistence only.
The JSON form publishes E and releases the control lock before trying the target fence.
While the active worker retains that fence, the command may return nonzero with no success JSON after E is already durable locally.
The worker must process E and finish without waiting for command acknowledgment.
The caller retains E after this error, waits for worker exit, and runs a standalone helper against the same workdir/endpoints for an identical-E JSON retry.
Require matching E and `end_persisted=true` in the positively observed response; exit 3 is refusal, never acknowledgment.
An active fence, cancellation, deadline, malformed state, or unknown outcome cannot become measured zero or completion.

## Drain, token, and completion

An empty applied frontier uses `applied_order=0` with all transaction fields null.
A nonempty frontier retains transaction order, XID, begin ordinal, optional physical commit LSN, and required transaction-end LSN.
Do not fabricate a physical commit position from transaction end.
The COPY anchor fields are either all absent or the snapshot token, consistent point, and 16-byte plan nonce bound to the same stream.

The three drain fields are either all null or all present.
`drain_kind` is exactly `keepalive` or `later_begin`.
A keepalive witness W is ordered after durable transactions through E, W >= E, and W <= certified LSN.
A later-BEGIN witness W is exactly that BEGIN's `final_lsn`, W > E, after all earlier relevant FIFO transactions are durable.
It can exceed both certified and durable replay positions and never grants feedback or reconnect authority.
An outer WAL frame position, active-transaction keepalive, received position, or old visible certification cannot replace a fresh eligible ordered witness.

Report durable replay is the barrier-proven maximum of applied end, keepalive-only certified LSN, and COPY anchor.
Feedback authority continues to use only positively proved applied/certified progress, excluding the anchor and later-BEGIN witness.
`drain_proof.replay_lsn` carries the witness W; the separately reported `durable_replay_lsn` is not replaced by W.
The proof retains exact E, kind, work identity, and checkpoint token.

The canonical token is `<generation>:<barrier_epoch>:<barrier_counter>` with the native protocol's numeric rules.
Each changed checkpoint/drain/completion content increments the stored counter exactly once without decreasing epoch; overflow refuses.
An exact same-value writing barrier preserves the token.
`follow_completed=true` requires the validated binding, anchor, durable drain, and final sequence synchronization under the writer fence.
After completion, DML, certification changes, new cutover boundaries, changed drain contents, and token changes refuse.
Completed resume positively observes the immutable row without recopy/reapply or recreating removed source resources.
A crash after nontransactional sequence changes cannot publish completion early; uncompleted finalization needs the supported validated retry path.
The uncertain pre-data/post-data recovery refusals remain explicit gaps requiring manual review or a fresh migration.

## Independent M6 observation

Engine reports cannot construct the caller's `CheckpointEvidence`.
The independent helper MUST use caller-owned identity, endpoint/provenance receipts, saved E, and the expected exact relation before trusting report fields.
It must work after the worker has exited with the retained PVC, connection/passfile/TLS mounts, and writable temporary directory.
The legacy live-worker `Execer.RunningPod` path and a retry that resamples `--current` do not satisfy this requirement.

Use a dedicated idle target session on the writable primary with `fsync=on` and `full_page_writes=on`.
Acquire the same two-key exclusive advisory fence: class `0x5250474d`, with the second signed int32 key carrying the CRC32 bits of the 16-byte run ID.
Validate backend/database/role/system identity and the held fence throughout the operation.
Validate the existing confirmed metadata, take the row lock, and perform an exact full-row same-value UPDATE with checked `UPDATE 1` and comparison of the expected contents.
Require at least `synchronous_commit=on`, preserving `remote_apply` when configured.
A configured synchronous-standby requirement cannot be replaced by local WAL flush.
Check deferred effects, transaction state, session settings, all results and warnings, and positive synchronous COMMIT in that same session before exposing the row/token as `BarrierConfirmed=true`.
Require the expected end/witness/kind, certified and durable frontiers, completion state, work identity, and canonical token from that proved row.
Check idle state and full session guard, require exactly one true advisory-unlock result, check idle again, and only then emit successful proof output.
Local connection disposal is not peer acknowledgment.

A plain SELECT, row/token polling, an old token with asynchronously visible newer values, local flush position, engine JSON, or `psql` exit zero with COMMIT text is insufficient.
On an unknown COMMIT or warning, discard success evidence and reacquire/revalidate the exact row under the fence for a new checked synchronous same-value barrier.
Do not increment the completed token or infer rollback from an unknown outcome.
Qualify SQL privileges, timeout/cancellation, synchronous-wait warnings, primary/failover behavior, and PG16/18 helper cases before M6 activation.

## Exact-owned cleanup

Acquire the native lifetime owner lock and refuse an active worker before effects.
From complete confirmed native receipts, durably record terminal cleanup intent under the control lock before dropping anything.
Terminal intent forbids recopy/reapply and unfinished-follow restart/resume; it still permits observation of prior durable completion.
Release the control lock before any target-fence acquisition; an already held target fence may take only a short control lock.
Store monotonic `slot_removed` and `publication_removed` booleans only after positive absence observation, retaining intent and partial progress across failures.

Verify source system/database/timeline, exact publication OID/owner/options/membership, and the positive slot creation binding before deletion.
Maintain exclusive administration of the source resources from creation through all cleanup retries.
A slot has no immutable catalog OID or creation nonce; matching name/plugin/database cannot authenticate a replacement.
If continuity is uncertain or an identity, active-owner, or generation check disagrees, preserve the object and return incomplete/nonzero for manual handling.
Do not automatically retry deletion after administration continuity has been lost or contradicted.
After an unknown DROP, re-observe positively instead of blindly dropping a surviving same-name object.
An object reappearing after recorded removal is uncertain and must survive.

Report exactly the saved owned slot/publication obligations, the canonical owned retained workdir, and any recorded unowned/unremoved origin.
An already absent owned resource satisfies only its explicit obligation.
Never turn an empty, incomplete, duplicate, or expanded inventory into `complete=true`.
Retain the workdir, target metadata, comparison state, target data, and unowned origins; preserve unrelated resources.
No production cleanup, operator deployment, or activation is authorized by this document.
