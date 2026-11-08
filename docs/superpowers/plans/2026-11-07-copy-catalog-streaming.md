# Bounded COPY catalog admission

## Scope and ownership

The parent approved this catalog package separately from the owning source and target COPY operations.
Copernicus owns `src/catalog.rs` and focused catalog tests.
Root owns shared test call sites, profile registration, frozen exports, and PostgreSQL leases.
Preserve the existing E1 inventory, workset, SQL, and error order with an exact inverse proof.
Do not edit SourceSnapshot, transport, State, authority, COPY transfer, the plan store, Cargo, or CLI.

## Exact helpers and bounded state

```rust
open_copy_inventory(
    source: &mut PgConnection,
    io: &IoControl,
) -> Result<CopyInventory>

fetch_copy_inventory(
    source: &mut PgConnection,
    inventory: &mut CopyInventory,
    io: &IoControl,
) -> Result<Option<Vec<(QualifiedTable, u32)>>>

close_copy_inventory(
    source: &mut PgConnection,
    inventory: CopyInventory,
    io: &IoControl,
) -> Result<()>

inspect_copy_relation(
    source: &mut PgConnection,
    table: &QualifiedTable,
    expected_oid: u32,
    catalog_epoch: u64,
    io: &IoControl,
) -> Result<CopyRelation>

preflight_copy_target(
    target: &mut PgConnection,
    relation: &CopyRelation,
    io: &IoControl,
) -> Result<()>
```

`CopyInventory` has private `previous: Option<QualifiedTable>`, `phase: CopyInventoryPhase`, and `backend_pid: u32` fields.
The private phase is Open, Eof, or Failed.
Consuming close represents closure without another flag.
The state does not borrow or expose a connection, so relation inspection can use the same owned importer between fetches.
Validate the same startup backend PID before fetch or close.
The caller must retain SourceSnapshot's existing physical-session and REPEATABLE READ READ ONLY transaction guards.

`CopyRelation` has private `event: Event` and `layout: Vec<Column>` fields.
Its consuming `into_parts(self, format: CopyFormat) -> Result<(CopyTable, Event)>` derives the real QName and ordered physical column names from the canonical Relation and preserves the supplied format unchanged.
Malformed internal variants return an error, not a panic.
Target admission borrows the transient layout for exact nullability and collation checks.
After that validation, `into_parts` drops the layout and transfers the CopyTable and Event.
This record is not an admission token; the caller still enforces the all-tables-before-COPY boundary.
Do not store transient layouts in the immutable plan seal.
Text and Binary selection are stored metadata, not transfer authorization.
The later owning operation must check the existing Text-only transfer guard for every sealed entry before copying any row.

## Exact inventory SQL

Use the existing query and command APIs for these statements:

```sql
DECLARE rpgmigrate_copy_inventory NO SCROLL CURSOR WITHOUT HOLD FOR
SELECT c.oid, n.nspname, c.relname
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
  AND c.relkind IN ('r','p','v','m','f')
ORDER BY n.nspname::pg_catalog.text COLLATE pg_catalog."C",
         c.relname::pg_catalog.text COLLATE pg_catalog."C", c.oid;
FETCH FORWARD 128 FROM rpgmigrate_copy_inventory;
CLOSE rpgmigrate_copy_inventory;
```

Declare once, fetch forward repeatedly, and close once within the retained importer transaction.
The first fetch can scan or sort the full join and spill under PostgreSQL's normal work_mem policy.
Later fetches retain the portal's execution position instead of repeating that join and sort per page.
The PG14/17 `(relname, relnamespace)` index does not establish a seek for joined qualified-name C ordering; the earlier keyset proposal is withdrawn.
Catalog adds no BEGIN, COMMIT, ROLLBACK, session reset, reconnect, transport method, or connection getter.

## Inventory and EOF validation

Include every r/p/v/m/f relation outside the excluded system namespaces.
Unlogged tables, partition parents and children, views, materialized views, foreign tables, and user-schema extension tables remain candidates.
Sequences, indexes, composite relations, and system namespaces are outside this table inventory.
Do not filter candidates by eligibility or privileges; unsupported candidates must refuse before any copied row.
Validate result shape, at most 128 rows, the matching FETCH command count, nonzero u32 OIDs, bounded names, and strict qualified-name order across pages.
Require `PgResult::columns()? == 3` before accepting any page, including a zero-row result.
Root owns this approved read-only accessor, which checks the native field-count conversion without changing existing transport operations.
Advance the previous name only after validating the entire page.
Query or mapping failure enters Failed, which always refuses and never returns None.
Only a successful validated FETCH establishes EOF.
A full page returns Some with 128 rows and leaves Open.
A positive short page returns Some with the nonempty page and enters Eof; the next call returns None after validating the owning backend.
A validated zero page enters Eof and returns None.
A genuinely empty database is valid; a failed query or unsupported candidate is not empty input.
Application state retains one fetched page and one relation record.
libpq allocates the complete query result before Rust validation, so the Rust byte budget is not a pre-allocation or RSS bound.
Account for server sorting and retained table locks separately.

## Locked relation and target admission

Read the expected source OID with `c.oid = $1::pg_catalog.oid` and the existing `relation_eligibility(false)` expression.
Require its decoded QName and OID to match the fetched identity.
Acquire quoted `LOCK TABLE ONLY ... IN ACCESS SHARE MODE`.
Under that lock, prepare `SELECT pg_catalog.to_regclass($1::pg_catalog.text)::pg_catalog.oid` and bind the existing quoted qualified name.
Require the page OID, then reread metadata by OID and compare the QName again.
Reuse the CDC layout, plain-index, primary-key, and event construction checks with the confirmed catalog epoch.
The catalog epoch is independent of stream generation.
Retain source locks through the imported transaction; closing the cursor ends enumeration only.
Table and namespace DDL quiescence remains a precondition.

Target admission receives the real fenced READ WRITE READ COMMITTED connection and retains ONLY SHARE ROW EXCLUSIVE locks.
Reuse the existing transaction, origin, execution-profile, effective-privilege, column-layout, and emptiness checks.
Extract the checked target layout for transient comparison without querying it twice.
Use an explicit private CDC foreign-key refusal policy instead of the current empty-workset shortcut; preserve E1's real-workset semantics exactly.
Refuse source column-SELECT gaps and unsupported target constraints or shapes.
Finish all tables' metadata, locking, and target-emptiness checks before the owning operation permits its first copied row.

## Cleanup preconditions

The cursor belongs to one live physical importer and its retained transaction.
Consuming close issues CLOSE only when that same connection has a healthy active transaction.
Close may release the portal after a local mapping error if the transaction remains healthy; it does not convert the failed admission into success.
Do not issue CLOSE through an aborted, closed, poisoned, or replaced session.
There is no SQL in Drop, no hidden reset, and no automatic reconnect.
If cleanup also fails, preserve the original admission error and let the owning caller close or roll back its importer.
Catalog helpers do not perform that rollback themselves.

## Frozen base and checklist

Use `target/verify/copy-catalog-prerequisite-base-lj5kynt4/freeze.json` and copy the ordinary export before editing it.
The base retains Native4 State, target metadata version 2, reviewed Native4 fixture expectations, and the qualified catalog/COPY baselines.
It excludes metadata3 integration and the plan-store candidate.
Use a fresh Cargo target for each baseline, candidate, and control, with the controlled libpq path recorded in the freeze.

- [x] Save exact SQL, helper signatures, bounded state, cleanup preconditions, ownership, and the frozen base before edits.
- [ ] Add meaningful failing controls for page shape, command count, ordering, EOF, identity, and failed-state behavior before implementation.
- [ ] Implement bounded enumeration, locked source admission, transient layout comparison, and explicit CDC foreign-key refusal.
- [ ] Prove E1 SQL, workset decisions, and error order unchanged with an exact inverse.
- [ ] Freeze nonempty pure-test discovery and execution, formatting, whitespace, and scoped lint evidence.
- [ ] Propose the precise subassertions in existing Snapshot fixture groups and any minimal Root-owned test call site before live execution.
- [ ] Qualify the actual SourceSnapshot owner, backend continuity, paging boundaries, locks, and late refusal under Root's later sole PostgreSQL lease.
- [ ] Obtain independent review of source, controls, boundedness, and owner-fixture evidence.

Use real SourceSnapshot-created confirmed bindings for owner coverage.
Synthetic receipts and ignored live tests cannot satisfy that gate.
No new profile or count change is implied by this package.
The owning source COPY operation, target COPY authority, snapshot proof, and public CLI route remain unreleased.
