# Storage pilot: macOS arm64, 2026-11-06

This pilot compared three synchronous storage candidates with one stream, one writer, and one active source transaction.
It selected no production backend and measured no target WAL application.

## Evidence and source identity

The tested inputs match commit `905c3239c39fd3e362dc9b7a6b28ad6b994db49b`.
All ten hashes in [inputs.json](../../bench/results/2026-11-06-macos-arm64/inputs.json) match that commit's file contents.
The run recorded base commit `4e7eeccb2da9ba236db1714c9f7b4801a1155189` before the experiment inputs were committed.
Use the frozen input hashes to associate these measurements with the tested source, rather than later working-tree revisions.

The committed driver generated the aggregate report and case manifest.
The evidence and input files preserve verification receipts.
These four archive files retain the original bytes and relative provenance paths.

| Archive | Original path |
|---|---|
| [report.json](../../bench/results/2026-11-06-macos-arm64/report.json) | `target/storage-pilot-20261106/report.json` |
| [evidence.json](../../bench/results/2026-11-06-macos-arm64/evidence.json) | `target/storage-pilot-evidence.json` |
| [inputs.json](../../bench/results/2026-11-06-macos-arm64/inputs.json) | `target/storage-frozen-inputs.json` |
| [cases.json](../../bench/results/2026-11-06-macos-arm64/cases.json) | `target/storage-pilot-20261106/cases.json` |

The aggregate contains all 27 child reports, so the archive does not duplicate their individual files.
The evidence retains binary and native-artifact hashes without copying those artifacts.
The raw native-artifact manifest is excluded because it contains private absolute paths.

## Conditions and accounting

The run used macOS arm64, Rust/Cargo 1.99.1, a release build, SQLite 3.54.1, and redb 4.5.0.
Three cases, three candidates, and three repetitions produced 27 reports with 27 unique child PIDs.
The driver used seeded candidate shuffles within seeded case order.
The cache state was warm after writing.
A fresh directory does not establish a cold cache.
The configured 64 MiB cache budget was not a process-RSS limit.

For each case, all candidates and repetitions matched encoded bytes, transaction/frame counts, batch boundaries, and input CRC.
The small case stored 128 source transactions in one storage batch, not 128 separate synchronization operations.
The wide case stored four transactions across five batches.
The giant case stored one transaction across 19 batches.

Write time included input generation, encoding, validation, batch setup, and batch commit.
Reopen time measured a clean reopen with integrity checks, not crash recovery.
Verified scan time included two passes and the input oracle.
Total time included creation, writing, checkpoint, first close, reopen, verified scan, final checkpoint, and second close.
It also included intervening driver overhead, such as reading effective settings.
Phase medians are not additive.

## Observed full-cost results

The table uses `total_ns` for median and minimum-to-maximum elapsed time across each three-run group.
RSS is the maximum child-process peak in that group.
The values below were recomputed from the archived aggregate.

| Case | Candidate | Median ms | Minimum ms | Maximum ms | Peak RSS MiB |
|---|---|---:|---:|---:|---:|
| small | segmented | 113.978250 | 99.737375 | 114.169792 | 3.90625 |
| small | redb | 109.266042 | 102.317917 | 127.933208 | 5.15625 |
| small | SQLite | 72.843375 | 65.072541 | 88.912792 | 3.921875 |
| wide | segmented | 233.909042 | 205.721500 | 245.383250 | 6.9375 |
| wide | redb | 253.381833 | 238.905917 | 280.031542 | 72.484375 |
| wide | SQLite | 291.721875 | 268.976959 | 305.856542 | 49.328125 |
| giant | segmented | 725.590500 | 655.274209 | 741.089917 | 6.9375 |
| giant | redb | 693.230292 | 681.660209 | 707.509791 | 73.703125 |
| giant | SQLite | 930.491416 | 925.592000 | 930.900042 | 90.59375 |

The case manifest names these workloads `small-commits`, `wide-events`, and `giant-transaction`.
The segmented and redb total-time ranges overlap in every case.
SQLite's small-case range is below both alternatives in these samples.
The segmented wide-case range is below SQLite's range, while the redb and SQLite ranges overlap.
Both segmented and redb giant-case ranges are below SQLite's range.
Three samples per group do not establish statistical acceptance or a production winner.

## Limits and next evidence

These are storage measurements, not target-committed rows or transactions per second.
The pilot does not qualify production concurrency, quotas, pruning, power-loss recovery, or target WAL apply.
SQLite reported FULL/WAL with `fullfsync` and `checkpoint_fullfsync` enabled.
The segmented journal reported `File::sync_all`, and redb reported `Immediate` durability.
Those settings do not prove equivalent power-loss barriers.
The remaining [verification gates](../superpowers/plans/2026-11-06-verification-performance.md#v3-storage-apply-and-migration-measurements) require matched contracts and actual apply/full-migration evidence.

## Run a separate pilot

Use the tested source and toolchain when comparing against this historical result.
Later revisions produce new evidence.

1. Choose an output directory that does not exist, then run the repository target from the checkout root.

   ```sh
   STORAGE_ROOT=target/storage-pilot-new-run make storage-pilot
   ```

The target requires `STORAGE_ROOT`, and the driver refuses an existing output directory.
Choose another path if the example already exists.
Keep the original pilot directory and this archive unchanged.
