# COPY snapshot anchor contract

## Approved scope

We refine the logged COPY recovery item in the [approved design](2026-11-06-rpgmigrate-design.md#positions-and-target-authority) and [E3 plan](../plans/2026-11-06-engine-cdc.md#e3-fenced-target-checkpoint-and-serial-apply).
The contract binds completed COPY data to its source snapshot without recording a synthetic CDC transaction.
It defines implementation dependencies and acceptance criteria; it does not establish an implemented clone or recovery path.

## Storage decision

- Extend the existing logged authority row with an optional anchor: reuse its confirmed provenance, relation lock, writer fence, and transaction.
- A companion logged table would require another confirmed object identity, privilege check, exclusion, and recovery validation.

We choose the existing row because the first COPY slice commits all copied tables in one target transaction.
This introduces no additional PostgreSQL object.

## Anchor fields

`SnapshotAnchor` is a record with the following fields.

| Field | Type | Meaning |
| --- | --- | --- |
| `stream` | `StreamId` | Run, `SourceId`, and stream generation |
| `snapshot_token` | `String` | Actual source export token |
| `consistent_point` | `SourceEndLsn` | Actual slot initialization position |
| `plan_nonce` | `[u8; 16]` | Identity of one immutable sealed plan |

The existing row supplies the stream and the target identity checked against `ConfirmedProvenance`.
Add nullable `copy_snapshot_token`, `copy_consistent_point`, and `copy_plan_nonce` fields to that row and require all three to be absent or present together.
Validate the token as nonempty, NUL-free UTF8 within `MAX_SNAPSHOT_TOKEN_BYTES = 1024`, and require a nonzero plan nonce.
Preserve the existing source-snapshot provenance refinement: the consistent point must be nonzero.
Generic LSN parsing still permits zero where its separate domain allows it.
Anchor presence records completed COPY; a native intent alone cannot create that meaning.
Install an anchor only while the CDC frontier and anchor are both absent, then preserve it unchanged in subsequent checkpoint updates.
The consistent point is a source position, not evidence of a completed source transaction.

## Sealed COPY plan

The private [immutable plan adapter](../../../src/copy_plan.rs) supplies `SealedCopyPlan` through the Native4 draft and pin APIs.
Its bundled SQLite store does not select the WAL journal backend.
Its header binds the confirmed source-slot receipt, stream, distinct catalog epoch, snapshot token, nonzero consistent point, plan nonce, and forwarding buffer budget.
Each ordered entry binds the canonical Relation event and explicit `CopyFormat` from [the existing contracts](../../../src/contracts.rs).
The source publisher passes its requested format through `CopyRelation::into_parts(format)` and `PlanEntry::checked` without normalization.
Both Text and Binary tags remain valid stored metadata.
Execution retains the existing Text-only, explicit-column restriction and checks every sealed entry before any COPY starts.

Publish the plan and its nonce durably before native control state references the seal or target DML begins.
Validate records with bounded allocations and compare the complete ordered plan exactly through streamed reads on reuse.
Missing, reordered, duplicated, truncated, corrupt, or differently bound records must fail validation.
Keep a stable reader pin while validating and using the seal so replacement or retirement cannot change the compared plan.
The control manifest stores a small qualified seal reference and nonce; it does not contain the ordered plan.
Retain the existing 16 KiB control-manifest limit without deriving a total plan-size limit from it.
The canonical `CopyPlanRef` contains only version, nonce, table count, logical bytes, and CRC32.
Its version is one; its nonzero nonce must match the anchor.
CRC32 zero and an empty plan are valid.
Logical bytes are the checked sum of canonical per-table payload lengths, excluding the identity header, ordinal/length framing, and SQLite container bytes.
The adapter qualifies actual count, bytes, CRC, encoding limits, and checked width conversions.
State does not acquire a path, backend handle, repeated stream identity, or SQLite integer restriction.

## Native intent and authority

Native4 stores required-nullable `CopySnapshotIntent { anchor: SnapshotAnchor, plan: CopyPlanRef }`.
Use this order: publish the sealed plan, acquire target authority and confirm target provenance, bind the native intent, then open its immutable reader inside authority.
Persist and synchronize that small intent after the seal and target provenance are confirmed and before the first COPY DML.
An identical retry must revalidate the same seal and intent; a changed snapshot, plan, source, generation, or target provenance must refuse reuse.
Preserve the existing `TargetBinding`, `ProvisioningIntent`, and `ConfirmedProvenance` checks in [native state](../../../src/state.rs).
Keep connection strings, credentials, and row contents out of these records.

> [!warning]
> Native COPY counts and a sealed-plan reference identify local expectations; they do not prove target data committed.
> Only a matching logged target anchor written with the copied data can establish COPY completion.
> A checkpoint read alone cannot produce a durable proof.

## Source capability

The [source provider](../../../src/snapshot.rs) owns the importing SQL session, exporting replication session, and confirmed receipt.
The provider binds the actual export token and slot `consistent_point` to a freshly verified `SourceId`, including its database OID.
`IdentifySystem.database_name` is not that OID, and `IdentifySystem.wal_position` cannot substitute for the consistent point.

The capability must also identify the source connection that successfully imported the snapshot before its transaction's first query, with the required isolation level.
Keep that imported source transaction intact until the source COPY work completes.
An export token stops being available for import when its exporting replication connection closes or executes another command; storing its text does not preserve the snapshot.
These requirements follow the [PostgreSQL 14 replication protocol](https://www.postgresql.org/docs/14/protocol-replication.html) and [snapshot import rules](https://www.postgresql.org/docs/14/sql-set-transaction.html).
The owning COPY operation must retain and revalidate this actual capability; persisted receipt text cannot replace it.
Its bounded iterator retains one page of at most 128 identities and one transient relation, with successful cursor close before successful EOF.
An error or caught panic poisons the source operation; cleanup performs no SQL in Drop.

## Target ownership and CDC invariants

`TargetAuthority` retains its dedicated physical backend, database-wide session fence, and explicit transaction ownership throughout COPY.
Require the existing writable-primary, `fsync=on`, exact backend/lock, provenance, ownership, ACL, and metadata-shape checks.
Preserve source and target DDL quiescence, preflight locks, and exclusions tied to exact owned metadata identities.
After locked metadata validation, authority derives the actual checkpoint table OID and its one primary-index OID from `pg_index`.
Pass those two `pg_class` OIDs through the confined target loan to catalog admission.
Reject the resolved target relation OID before eligibility, layout, or emptiness checks and again after locking and identity revalidation.
Never compare source relation OIDs, schema OIDs, or name prefixes as a substitute.

COPY completion leaves `Checkpoint=None`, applied sequence zero, and all CDC transaction fields NULL.
The first real CDC transaction still has `TxSeq(1)` and its genuine `TxId`.
The empty-frontier `barrier(None)` contract continues to prove zero completed CDC transactions without proving COPY coverage.
Generic CDC authority remains usable without an anchor; the clone coordinator requires a matching `SnapshotProof` before catch-up.
A coordinator must not substitute the consistent point for a CDC frontier or use it to skip missing input.

## Atomic COPY completion

Reuse the existing [catalog preflight](../../../src/catalog.rs) rules and [E1 COPY transfer](../../../src/copy.rs) behavior, adapting only the transaction and completion boundaries needed for authority ownership and bounded plan reads.
Compare the sealed plan with the verified execution plan under the required source snapshot and target locks before admitting DML.
Consume validated table records without collecting the entire persisted plan into the control manifest or an unbounded vector.
The first complete scan compares source inventory and sealed records, admits every target, checks execution format, and closes the source cursor.
Only the second complete scan transfers rows through the existing per-table helper.
Configure both sessions even for an empty plan and require target `session_replication_role=origin`.

All table data, anchor installation, and one logged barrier-counter increment must commit in the same target transaction.
Retain terminal COPY result checks on both connections and source-transaction completion checks.
Use one shared finalization path to drain deferred constraints, revalidate the complete control row, restore and check final durability, and dispatch target COMMIT.
Preserve captured `remote_apply`; promote weaker synchronous settings to `on`.
Require the exact positive COMMIT tag and Idle transaction status before returning proof.
A failed or unfinished operation must poison or close the connection and cannot return completion evidence.
Keep the immutable reader pinned through positive target COMMIT and Idle evidence, then release it before returning the report and proof.
Finish the retained source only after acknowledged or recovered target proof.
Publish advisory native counts after the reader is released; counts never authorize recovery.

## Approved owning API and proof

The [owning implementation plan](../plans/2026-11-07-copy-snapshot-owning.md) assigns these new operation methods and their qualification.
The existing store and Native4 methods are dependencies, not evidence that the owning route is complete.

`SourceSnapshot::publish_copy_plan(&mut self, native: &mut NativeState, plan_nonce: [u8; 16], format: CopyFormat, buffer_bytes: usize, io: &IoControl) -> Result<CopySnapshotIntent>` publishes the sealed plan and returns an unbound pure intent.
`NativeState::bind_copy_snapshot(&mut self, intent: &CopySnapshotIntent) -> Result<()>` binds it only after target authority has confirmed provenance.
`checked_copy_snapshot(&self) -> Result<Option<CopySnapshotIntent>>` returns the checked immutable intent.

`TargetAuthority::copy_snapshot(&mut self, source: &mut SourceSnapshot, native: &NativeState, io: &IoControl) -> Result<(CopyReport, SnapshotProof)>` opens the exact checked intent's reader internally and owns atomic COPY completion.
Use the sealed plan's buffer budget and existing table semantics.
An unknown COMMIT returns an explicit failure with a poisoned connection and retained intent; it must not authorize another COPY attempt.

`TargetAuthority::recover_snapshot(&mut self, native: &NativeState, io: &IoControl) -> Result<SnapshotProof>` opens the exact checked intent's reader and checks the expected anchor after acquiring or reacquiring a fresh fenced primary connection.
The caller must first supply that connection through the existing `acquire` or `reacquire` API; recovery cannot reuse the connection whose outcome became unknown.
The clone coordinator obtains the expectation from checked native intent and reconciles source identity and required replay input.
Both owning entrypoints close or poison authority on all errors, including intent or reader failures before BEGIN.
Reuse `TargetTxn` with `next: Option<Frontier>`: CDC begins with Some and COPY begins with None.
Wrong-mode finalization must poison before mutative finalization SQL.

`SnapshotProof` has no public constructor, deserializer, or conversion from a visible row or generic empty-frontier proof.
Its private evidence binds the anchor, `ConfirmedProvenance`, barrier epoch/counter, and the synchronous commit evidence for the unchanged CDC frontier.
Construct it only after the atomic COPY commit or the recovery writing barrier succeeds.

## Recovery and failure matrix

Recovery must first wait for the previous backend's fence to release, validate the fresh primary and provenance, and validate the expected sealed plan.
A matching anchor must then survive a real logged counter UPDATE and confirmed synchronous COMMIT before recovery returns `SnapshotProof`.
Preserve the anchor and any existing CDC frontier during that barrier.

| Observation | Required result |
| --- | --- |
| Matching anchor after lost COMMIT reply | Writing barrier, then proof; no recopy |
| Anchor absent or only partially populated | Nonzero refusal; no automatic recopy |
| Anchor, intent, seal, or provenance mismatch | Refuse; do not adopt or replace |
| Missing or invalid plan records | Refuse before COPY or recovery mutation |
| Old backend still holds fence | Wait within deadline; return no proof on timeout |
| Barrier failure or uncertain COMMIT | Close connection; return no proof |
| Source snapshot lost before COPY completes | Abort unfinished work and refuse continuation |
| Required source history missing | Nonzero gap failure |
| Native count publication failed after COMMIT | Reconcile logged anchor before proceeding |

A matching completed anchor can recover COPY completion without reimporting an expired export token because the copied data and anchor committed together.
This target-only recovery does not reconstruct an importer or exporter, authorize source reads or feedback, or prove slot continuity and retained WAL or decoder context.
Source finish applies only to an actually retained live owner.
CDC continuation requires valid source identity and the checked recovery high-water position.
With checkpoint `None`, require complete input from the anchor's consistent point through that high-water position.
For a recovered `TxSeq(N)`, reconcile the checkpoint on a fresh fenced primary.
Complete the logged synchronous writing barrier before resuming.
Require the complete suffix beyond `N` through that position and the decoder context needed to resume.
Do not require a prefix already pruned under durable proof.
Preserve the approved target-crash requirement to revalidate or recopy unlogged tables; an anchor cannot override an unsupported execution or recovery profile.

## Fresh versions and compatibility

Use fresh native `FORMAT_VERSION = 4`, shared `TARGET_METADATA_VERSION = 3`, and `COPY_PLAN_REFERENCE_VERSION = 1`.
Reject earlier versions without import, in-place upgrade, DDL conversion, or adoption.
Retain the native magic, run/directory ownership, and target creation-receipt protections.
The plan adapter's version-one encoding does not change canonical CDC identities or wire records.
Preserve the accepted [operator compatibility contract](../../operator-compatibility.md) and explicit unsupported-operation failures while these capabilities are implemented; universal CLI parity is deferred.

## Acceptance criteria

- Prove copied data and anchor either commit together or both roll back, including an empty COPY workset.
- Prove COPY leaves the CDC checkpoint empty and the first genuine transaction uses sequence one.
- Prove a generic empty-frontier barrier cannot create `SnapshotProof`, and CDC-only authority needs no anchor.
- Inject actual COMMIT reply loss, observe the old backend holding and releasing its fence, and recover through a writing barrier without duplicate effects.
- Refuse missing, mismatched, partial, or foreign anchors and failed recovery barriers.
- Refuse unavailable snapshot imports, lost unfinished source transactions, and missing required source history.
- Prove recovery succeeds after authorized prefix pruning with the required suffix, decoder context, and checked high-water position available.
- Refuse recovery when any required suffix input or decoder context is missing.
- Exercise a supported target-only plain UNIQUE index with duplicate source payloads, observe actual COPY start, and require terminal SQLSTATE 23505, rollback of earlier copied data, and no anchor.
- Retain component coverage for deferred-constraint failure and settings changes during constraint evaluation; the owning catalog profile does not admit CHECK or foreign-key constraints for that fixture.
- Exercise weak synchronous settings, `remote_apply`, read-only/standby connections, and `fsync=off`.
- Exercise plan totals well beyond 16 KiB while keeping control references small and validation memory bounded by record/read budgets.
- Detect changed order, names, columns, format, buffer budget, source metadata, nonce, and missing or corrupt sealed records through exact streamed comparison.
- Inject seal and native-intent publication failures before DML and native-count publication failure after target COMMIT.
- Refuse prior native/metadata versions and retain restricted-role ownership and privilege cases.

## Owners, dependencies, and review

The immutable plan adapter supplies publication, stable readers, streamed exact comparison, bounded records, and retention required by native references.
The native-state owner supplies the small immutable intent, lookup/publication checks, and fresh version handling.
The source owner supplies verified export/import capability and consistent-point evidence.
The authority/COPY/catalog owners share existing transfer and finalization behavior while preserving one physical target owner.
Root owns shared type placement, module registration, CLI composition, and integration gates; do not duplicate canonical identity types.

Implementation depends on those interfaces and their qualification; this specification selects no storage backend or new execution framework.
The approved owning plan divides catalog/COPY helpers, source ownership, target authority, and private live fixtures among disjoint writers.
Root saves exact ordinary source bases, retains shared harness wiring and native leases, and requires independent review before the PostgreSQL 14 and 17 gates.
Public CLI orchestration, source acknowledgments, WAL backend selection, and performance claims remain outside this package.
