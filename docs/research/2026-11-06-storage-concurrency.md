# Storage concurrency qualification: C1

C1 checked concurrent Receive, Apply, and Status roles for the segmented journal, redb, and SQLite development candidates.
The experiment retained all history and allowed one active source transaction per store.
We select no production backend from this experiment.

## Contract and scope

The [experiment facade](../../bench/storage/config.rs) uses a [persistent catalog](../../bench/storage/catalog.rs) and completion index.
The index preserves full transaction identity when XIDs repeat or transactions complete in a different order from their starts.
Separate revision checks prevent one role from replacing another role's catalog fields.
Read views check records before emitting completed transactions and preserve their captured snapshot across both scan passes.

| Role | Concurrent owners | Permitted changes |
| --- | --- | --- |
| Receive | One | Append records and publish completed transactions |
| Apply | One | Advance the benchmark consumer cursor |
| Status | Multiple | Read snapshots only |

Role locks enforce these limits for cooperating processes.
Status cannot write application data or control records, repair storage, or run checkpoints.
SQLite Status may perform WAL/SHM coordination required by its read-only connection.
That permission does not authorize application or control writes.

> [!important]
> `consumer_cursor` records benchmark read progress only.
> It proves neither target durability nor permission to acknowledge source WAL.
> C1 does not qualify a complete CDC engine.

`RecordNo` accepts values from 1 through `i64::MAX` to match SQLite's INTEGER key range.
The maximum value is an exclusive-end sentinel, so stored records stop at `i64::MAX - 1`.
Other `u64` fields retain their full unsigned range and existing validity checks.

## Healthy coordination evidence

The healthy run produced `target/verify/storage-c1-driver/c1-proof-03/c1-report.json`.
Its SHA-256 is `2a987852d9c841c9ee7658d45bca4bfc1cd38afc263c4456edc9633162d8ae56`.
The adjacent `hashes.json` identifies the driver, recovery tests, aggregate report, and nine individual receipts.
All 12 hashes matched during this documentation check.

The source files checked for this report have these SHA-256 prefixes:

| Source | SHA-256 prefix |
| --- | --- |
| [Driver](../../bench/storage.rs) | `989eb9320d17` |
| [Recovery tests](../../tests/storage_recovery.rs) | `93dcd6668fa4` |
| [Facade](../../bench/storage/config.rs) | `00e493fa6ee6` |
| [Catalog](../../bench/storage/catalog.rs) | `8df07149f84d` |

Three candidates produced nine receipts from nine distinct child processes.
Each candidate passed eight Receive checks, five Apply checks, and nine Status checks.
Every receipt matched four completed transactions, four frames, 1,024 encoded bytes, and input CRC32 `1217052608`.
The external required prefix was four transactions.

The schedule checked incomplete-transaction hiding, stable snapshots during publication, exclusive scan bounds, peer-field preservation, stale revisions, restart record numbering, and role refusals.
The driver requires the exact role receipts, successful child exits, and clean EOF after each terminal `finished` message.
It limits IPC message payloads to 4 KiB and receipt files to 256 KiB.
One absolute 300-second deadline covers each candidate's child schedule.

Independent reviews accepted the C1 driver helpers, common catalog/facade, SQLite, and segmented journal within their reviewed scopes.
Regression tests first exposed inherited descriptors that retained role locks after owner drop and trailing child output that incorrectly allowed success.
Both corrections passed their regressions and independent probes.
These results establish the scheduled healthy coordination checks, without closing every candidate's corruption or production gate.

## redb corruption failure

A separate redb 4.5.0 probe damaged a leaf in a cleanly closed database, then opened it through Status.
The process panicked with exit code 101 while reading a value, before the facade could check the record.
The probe expected a controlled backend error.

The pinned registry source, `redb-4.5.0/src/db.rs`, exposes no public integrity check on `ReadOnlyDatabase`.
Its writable `Database::check_integrity` method can repair the file.
Using that method for Status would violate the read-only contract.

> [!warning]
> We exclude redb 4.5.0 from production selection under the read-only corruption requirement.
> We retain it as a development comparison and preserve the negative evidence.
> The healthy C1 report does not establish that all candidate fault gates passed.

The probe evidence is under `target/verify/storage-c1-redb-review/evidence/`:

- `status-corruption-probe.rs` contains the reproduction.
- `probe.stderr` contains the panic trace.
- `review-evidence.json` records exit code 101 and the reviewed source hashes.

That review records its own source snapshot, including the catalog before the role-lock correction.
It is separate from the healthy proof above.

## Remaining qualification gates

redb's blocking-open behavior is a separate C2 gap.
The driver's deadline bounds child lifetime, not the backend API's lock-wait behavior.
It does not establish cancellable storage operations.

C2 still requires pruning, quotas, a target-durability proof adapter, power-loss testing, Linux qualification, and performance measurements.
The [engine plan](../superpowers/plans/2026-11-06-engine-cdc.md) carries the integration work.
Root integration gates require their own final verification evidence.
This report makes no claim about their result.

The [historical storage pilot](2026-11-06-storage-pilot.md) retains its original 27 samples.
C1 supplies no new performance measurement or backend ranking.

> [!note]
> The evidence paths in this report identify temporary files under `target/`, not a committed archive.
> Use a separately reviewed archive to retain evidence through cleanup of that directory.

## Run the C1 schedule

Use the repository toolchain and reviewed native library described in [Getting started](../getting-started.md).
Run from the checkout root.

1. Choose an output path that does not exist, then run the storage example.

   ```sh
   PQ_LIB_DIR="$PWD/target/native/libpq/lib" \
     cargo run --locked --release --example storage -- \
     --c1-root target/storage-c1-new-run
   ```

The driver refuses an existing output directory.
Choose another path for each run.
The command writes nine role receipts and `c1-report.json` into the new directory.
Its success covers the healthy schedule above, including the redb development comparison.
The separate corruption probe and remaining qualification gates still apply.
