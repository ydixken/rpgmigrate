# Per-table COPY transfer extraction

## Scope and ownership

The parent approved this mechanical extraction before any source-owned COPY operation.
Herschel owns only `src/copy.rs` and its existing module-local tests.
Root owns integration, frozen exports, the task ledger, and the PostgreSQL lease.
Keep `SourceSnapshot`, catalog, transport, State, authority, storage, formats, CLI, Cargo, and root registration unchanged.

## Exact seam

Extract the current body of the `transfer_tables` loop into this private helper:

```rust
fn transfer_table(
    source: &mut PgConnection,
    target: &mut PgConnection,
    table: &CopyTable,
    buffer_bytes: usize,
    report: &mut CopyReport,
    target_copy_active: &mut bool,
    io: &IoControl,
) -> Result<()>
```

`transfer_tables` keeps its report initialization, loop, and returned report.
It passes the existing `plan.buffer_bytes`, report, and flag to the helper for each table.
Only replace the moved body's `plan.buffer_bytes` access with the helper argument.
The existing `run_copy` positive-budget check remains the caller precondition.
Do not add a new caller or expose this helper before a real consumer exists.

## Preservation requirements

Keep SQL quoting and the existing text-format and ordered-column checks unchanged.
Preserve target COPY start before source COPY start, every read and write, chunk size, counter conversion, checked addition, poll, deadline, cancellation, and error path.
Keep the target flag assignment to false before `finish_copy_in`, exactly as in the accepted baseline.
Keep row accumulation before table-count accumulation.
Leave `run_copy`, abort/rollback handling, source completion, constraint checks, durability checks, target COMMIT classification, and native completion unchanged.
This package adds no SnapshotProof, optional frontier, source getter, snapshot operation, or target COPY operation.

## Frozen base and verification

Use the ordinary source export recorded in `target/verify/copy-catalog-prerequisite-base-lj5kynt4/freeze.json`.
Its COPY baseline is the reviewed private extraction, with Native4 State and the reviewed Native4 fixture expectations.
Target metadata remains version 2; the base contains neither the metadata3 integration nor the plan-store candidate.
Copy the base before editing it and use a fresh Cargo target for the baseline and candidate.
Pure test selection must discover and execute the expected nonempty inventory; ignored native cases do not count as passes.
Use the existing controlled libpq path from the freeze record for pure builds.

- [x] Save the approved scope, exact helper, ownership, and frozen base before edits.
- [ ] Run the existing focused pure COPY tests before extraction and retain source/binary bindings.
- [ ] Move only the loop body and replace the single buffer-budget expression.
- [ ] Prove an exact mechanical inverse restores the full baseline file.
- [ ] Run the same nonempty pure tests, formatting, whitespace, and scoped Clippy checks after extraction.
- [ ] Freeze the file, diff, inverse, commands, binaries, logs, and limits for independent review.
- [ ] Run the appropriate existing COPY and engine regression under Root's later sole PostgreSQL lease.

This refactor does not need an invented behavior failure or an unrelated test.
Any proposed behavior fix requires a separate handoff; do not fold it into the extraction.
Global strict lint and combined source-to-target qualification remain separate gates.
