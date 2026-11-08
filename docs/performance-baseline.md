# Serial apply measurements

We compared the accepted E3 serial-apply baseline (A) with the first E7 session-settings refactor (B).
Both measurement windows occurred on 2026-11-08 UTC, as recorded in the sealed execution receipts.
B uses three existing bound-query dispatches and independent readback instead of sixteen dispatches per source or target setup, preserving the same eight settings and transaction guards.
When those receipts were sealed on 2026-11-08, the measured runtime implementations were private and were not wired into an operator workflow.
This documentation checkpoint publishes their measured results; the maintained task tracker records later implementation and publication status.

## Workload and scope

Each candidate passed 24 runs: PostgreSQL 16.15 and 18.6, segmented and SQLite storage, one or sixteen rows per transaction, and three fresh-process repetitions per configuration.
Each run excludes 32 warm-up transactions and measures 256 transactions with a 64-byte payload; each candidate measures 6,144 transactions and 52,224 rows.
All 6,912 warm-up-plus-measured transactions per candidate require positive COMMIT and durable checkpoint proofs, with 168 native witnesses per candidate.
Both use Native5, test_decoding, a Rust cfg(test) release build with opt-level 3 and debug assertions disabled, Linux arm64, local source and target clusters over Unix sockets, two CPUs and 2 GiB.
Target COMMIT remains synchronous; workload, order, native library, SQLite archive, proof predicates and metric clocks are unchanged.
A and B ran in separate quiet windows, with no interleaved A rerun.

Values below are medians [minimum, maximum] across three runs.
Change is 100 × (B median − A median) / A median, not the median of paired repetition changes.

## Pipeline throughput

Pipeline spans the first measured source-write start through the final measured proof.

| PG | Backend | Rows/TX | A TX/s | B TX/s | Delta TX/s | Change |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 16.15 | segmented | 1 | 70.73 [23.25, 75.51] | 125.84 [114.47, 129.37] | +55.11 | +77.9% |
| 16.15 | segmented | 16 | 56.01 [49.46, 61.14] | 74.47 [73.77, 76.26] | +18.45 | +32.9% |
| 16.15 | sqlite | 1 | 87.12 [70.50, 87.72] | 152.73 [151.81, 153.12] | +65.60 | +75.3% |
| 16.15 | sqlite | 16 | 93.82 [86.09, 95.26] | 131.40 [130.99, 133.15] | +37.58 | +40.1% |
| 18.6 | segmented | 1 | 89.41 [79.30, 97.30] | 124.73 [120.88, 128.50] | +35.32 | +39.5% |
| 18.6 | segmented | 16 | 57.92 [56.79, 58.26] | 74.51 [73.75, 75.31] | +16.59 | +28.6% |
| 18.6 | sqlite | 1 | 109.36 [108.94, 110.23] | 150.28 [150.21, 152.81] | +40.92 | +37.4% |
| 18.6 | sqlite | 16 | 90.24 [89.42, 108.05] | 131.37 [129.06, 131.48] | +41.12 | +45.6% |

B pipeline medians were higher in all eight configurations, by 28.6478% to 77.9196%; the observed three-run ranges did not overlap.
Inclusive apply-service medians were higher by 28.3950% to 80.0765%.
Apply-service time includes retained-history validation, decoding, metadata checks, DML, checkpoint work and synchronous target COMMIT; it does not isolate DML or a server bottleneck.

## Latency and resource labels

The retained report gives per-run source-COMMIT-observation-to-proof p50, p95 and p99 quantiles, then their median and range across three runs.
Quantiles use nearest ranks 128, 244 and 254 of 256 measured durations; transactions are not pooled and percentiles are not averaged.
Durations use the same native child's monotonic clock; source COMMIT observation is distinct from replication feedback acknowledgment.
CPU is native engine-client RUSAGE_SELF during the measured loop, excluding Python and PostgreSQL server CPU.
RSS is process-lifetime high-water, including setup, warm-up, finalization and the inherited pre-exec floor, normalized from Linux KiB to bytes.

> [!important]
> Three repetitions in separate windows provide descriptive observations, not formal confidence or a causal or universal speedup estimate.
> These measurements do not select a storage winner or compare against pgcopydb, default pgoutput, a production CLI, an operator workflow, parallel apply, pruning or power-loss behavior.

Both candidates retain a 900-second canonical command limit, 910-second auxiliary wrapper and 930-second host-call envelope, with 180-second child and 170-second engine bounds.
The maximum observed driver durations were 16.10065 seconds for A and 5.627981 seconds for B.
The samples were accepted with that disclosed margin; no strict outermost 900-second timeout guard was qualified.

## Evidence identity

These artifacts are retained in the local workspace under `target/verify`; they are not published by this documentation commit.
Full source/native/image/ownership audits were accepted separately before the statistics review.

- Baseline execution: `e3-b1fa-quiet-live-ly05ooqy/execution-receipt.json`, SHA-256 `2c67aa69b5b59ad7e8a3628e2acc4ee7926330bba4e8b2cd89647e438e4564e3`.
- Candidate execution: `e7-benchmark-B-m0or5m6i/execution-receipt.json`, SHA-256 `a47673f36b9996c2ba3ee6c2e7d332728c0ff712e898076b09c8bdd8518fdb9c`.
- Full report: `e7-ab-statistics-rp7fmd45/REPORT.md`, SHA-256 `d3f20f3577da9d2af1d590b597faf66e5ec4c7983d6d0590433df76bbf1ae36a`.
- Statistics and all 48 per-run values: `e7-ab-statistics-rp7fmd45/statistics.json`, SHA-256 `87373cf14713de6927e9f3b63287e138424ef961bda0ecb3e2b3cae8963d9bda`.
- Bounded output acceptance: `e7-ab-statistics-rp7fmd45/root-acceptance.json`, SHA-256 `80845934270e8ac48d2233700fece2dc55a8c5afa59bc8cb4c41be0842fba0c7`.

Root matched 48 numeric-input references to accepted captures, checked eight displayed rows and metric labels, and recomputed two median percentage changes directly from retained timestamps.
PG16 segmented one-row throughput reproduced +77.9196202%; PG18 segmented sixteen-row throughput reproduced +28.6477937%.
This bounded review did not rerun the benchmark or repeat the full 730/534-artifact audits.

A source manifest is `8b6e0cd57adc4779028d61d386037140f96d03b9671b50d393281782b57c6d09`; B is `a5b3d9fac6fae052a70df30ae7d135204ced6c79f0e4199e576511f6ab2815a6`.
A release libtest is `8c98441d460f7258350f95f797b3fe0f412c5cdae752674ed6a80becd2d9ee31`; B is `3d13b00d9b2418a312a3d37b00bf63ef9efd5d5d731061c8469d7fdad6423e50`.
Both use libpq `d6ab301510e50fd041be5f9ccd88ca4c3431502b1287996769853d1b66b77c86` and SQLite archive `dd4b2dd183665ec3311bafaacbb134c7e57c2475e931fefad6e257fc0e54a277`.

See the [maintained milestone tracker](../tasks/todo.md#verified-component-milestones) for workflow gates that remain open.
