# COPY catalog coverage in owned Snapshot fixtures

## Scope and ownership

The coordinator released this test-only addendum against the approved [catalog helpers](2026-11-07-copy-catalog-streaming.md).
Noether owns only `src/snapshot_live_tests.rs` and `tests/support/source_snapshot_fixture.py`.
Root retains the transport test file, ordinary source exports, integration, and the sole PostgreSQL lease.
Copernicus retains catalog implementation ownership.
Preserve the existing twelve Snapshot test IDs, five fault groups, role capabilities, and receipt checks.
This addendum does not release source or target COPY operations.

## Actual owner coverage

Extend `live_owned_snapshot` through its actual SourceSnapshot-created confirmed binding and retained importer.
Keep stream generation 9 distinct from catalog epoch 7.
Give `public.snapshot_data` a primary key and add 257 ordinary primary-key tables to the intended populated source fixture, with SELECT and namespace USAGE for its existing restricted role.
The existing `snapshot_cap_relation` remains, so the expected inventory contains 259 tables with page lengths 128, 128, and 3.
Keep `snapshot_empty` empty and preserve the wrong-cluster and capability checks.
Assert the exact expected qualified names and source OIDs, C ordering, relation metadata, backend identity, and imported REPEATABLE READ READ ONLY transaction.
After closing enumeration, prove that source AccessShare locks remain until the importer ends.
Do not treat source admission as target admission or a snapshot proof.

## Empty result and refusal coverage

Extend `live_empty_snapshot` using its actual empty-database owner.
Require a successful three-column FETCH 0 before accepting EOF.
In a separate control, replace that owner's portal with a real two-column zero-row query and call the catalog fetch helper.
Require the width error and sticky Failed state, never EOF.
Use the real checked `PgResult::columns` accessor; add no constant result, synthetic source receipt, or replacement connection.
Preserve the initial error during portal cleanup and retain the existing owner finalization rules.

## Verification and release

Use a Root-provided ordinary source copy with explicit catalog, transport, Native4, and fixture identities for compilation and exact test discovery.
A provisional catalog API copy permits compilation only; refresh it to the final reviewed catalog source before native qualification.
Keep API-absence diagnostics separate from behavioral failures.
Record source and executable hashes, exact discovery, formatting, and offline fixture checks before requesting Root's native window.
The later native run must retain all twelve cases and five fault groups, with no ignored or missing cases.
Record real wrong-width and paging negative controls in separate source variants when they are needed to establish the assertions.
The populated-anchor authority tests, COPY regression, global strict gate, and integrated owning-COPY route remain separate gates.
