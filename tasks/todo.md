# rpgmigrate task status

This file is the authoritative running status.
The approved design defines scope.
Plans define worker interfaces and acceptance checks.
The team leader reviews worker results before closing a milestone.

## Documentation bootstrap

- [x] Inspect the target and inherited instructions.
- [x] Initialize an ordinary repository on `feat/bootstrap` without remotes.
- [x] Create the initial [engine plan](../docs/superpowers/plans/2026-11-06-engine.md).
- [x] Record the [approved design](../docs/superpowers/specs/2026-11-06-rpgmigrate-design.md) and review resolutions.
- [x] Fix E0/E1 shared interfaces and first-milestone task details for leader review.
- [x] Create the [CDC plan](../docs/superpowers/plans/2026-11-06-engine-cdc.md).
- [x] Create the [verification/performance plan](../docs/superpowers/plans/2026-11-06-verification-performance.md) and [operator plan](../docs/superpowers/plans/2026-11-06-operator-adapter.md).
- [x] Record user corrections in `tasks/lessons.md`.
- [x] Self-review plan coverage, types, tests, and size.
- [x] Check document links, Markdown structure, and STE100 hard rules.
- [x] Obtain the leader's E0/E1 plan review and incorporate accepted review decisions.

## Implementation and release

- [x] E0: Contracts, build checks, private package metadata, notices, and baseline fixtures.
- [x] E1: Real libpq connection/ping, bounded raw COPY, and native-state safety on the recorded fixtures.
- [ ] E2: CDC decoding, packed journal, complete descriptors, and persistence measurements.
- [ ] E3: Logged target checkpoints, serial apply, recovery, and apply benchmarks.
- [ ] E4: Durability profiles, run-wide risk caps, and crash verification.
- [ ] E5: Full CLI and standalone phase compatibility.
- [ ] E6: Schema, filters, snapshots, large objects, and all-databases behavior.
- [ ] E7: Qualified batching and parallel apply.
- [ ] O1-O4: Native operator adapter and isolated operator tests.
- [ ] V1-V6: Compatibility, faults, performance, version matrices, and release checks.
- [ ] Full-scope review against the feature map.
- [ ] Verify each platform's power-loss barrier before journal durability claims, including macOS `fsync` versus `F_FULLFSYNC`.
- [ ] Match SQLite `fullfsync` and `checkpoint_fullfsync` settings to candidate barriers in persistence comparisons.
- [ ] Test the declared minimum Rust toolchain in CI before expanding compiler support claims.
- [ ] Add namespace identity and DDL coordination before removing E1's DDL-quiescence requirement.
- [x] Build the same controlled native recipe in local Linux arm64 Podman and verify loaded-image guards, rpath, and authentication features.

## Evidence and review

The bootstrap inspection found an empty target directory.
Git initialization reported `feat/bootstrap` and no remotes.
Context documentation and environment readiness are complete.
E0 owns package/root contracts, root modules, Makefile, README, and this task status after the documentation handoff.
Transport, CLI, and native-state implementations are handed off, with their focused review evidence below.
Harness integration continues for the revised target execution profile.
State worker `01a1b228-5b80-7b83-9a6d-d32d77098814` completed its assigned module.
COPY/catalog worker `01a1b22d-2a70-7321-bd8d-ca3a33441f56` is active.
Linux/CI worker `01a1b243-2d99-7521-ba4c-9e3912c025b0` owns the workflow, `tools/ci/`, and `docs/testing.md`.
E2 format worker `01a1b241-49b2-7062-af67-c68dea4b8a1c` owns the isolated experiment codec and its test target.
The root entry point routes help, native version, and real ping.
The root COPY route calls native state, connection, planning, and COPY interfaces.
The COPY coordinator is implemented and its live database verification is active.
The bounded E1 profile passed its recorded correctness gates; full migration compatibility and performance remain unverified.
No GitHub workflow has run because this checkout has no remote.
The leader approved a Linux fixture profile with explicit expected PostgreSQL `17.11` and numeric version `170011`.
The native fixture default remains PostgreSQL `14.24`.
Expected versions must come from the pinned profile and be checked against the server, never derived from the server under test.
The E0/E1 review includes exact interfaces, Makefile ownership, late COPY results, unknown COMMIT, and isolated fixture inventories.
The user license answer is pending.
Keep `publish = false`, preserve third-party notices, and omit a new project license until that answer arrives.

## Verification evidence reported to this worker

The independent reviewer passed the E0/E1 plan at SHA-256 `d897788329937a3dee45beafc110c52a5f3c9701daf7bbf4162da6d7bce03e10`.
That pass covers the plan handoff only, not runtime results.
The leader then specified the real harness invocation and temporary 300-second/8 MiB slice limits.
These clarifications do not change the frozen function signatures.

The dependency worker passed the macOS graph audit for 19 locked packages.
It checked checksums, non-yanked crates.io releases, licenses, and minimum Rust versions.
RustSec snapshot `309392b18f6b17022f82c701f300433126901068` had zero matching advisories.
This was not a `cargo-audit` run or a native-library/Windows vulnerability audit.
Host metadata/tree checks passed.
The initial unfiltered offline metadata check needed an uncached Windows `vcpkg` package.
The later Cargo fetch cached that package while resolving the approved experiment dependency.
The worker reported pkg-config linkage for `pq-sys` and dynamic libpq version `180006`.
Preserve the required Unicode-3.0 notice.
The experiment-only `crc32fast 1.5.4` dev dependency adds `cfg-if 1.0.5`, bringing the lockfile to 21 registry packages.
Cargo metadata confirms MIT/Apache licensing and Rust floors of 1.63 and 1.32, respectively.
Reviewer `01a1b248-5643-7dc3-8e2d-7807cbbdafcc` passed the refreshed lockfile delta audit.
The original 19-package lockfile SHA-256 is `4366471a2a3297eed57be8147810c41b043f3956e818e992d14bceef86b037a9`.
Removing only the two new package blocks and root dev-dependency edge in memory reproduced that original hash exactly.
Both new crates match official non-yanked metadata, lockfile and archive checksums, MIT/Apache licensing, and the complete RustSec snapshot with zero package matches.
They are absent from the normal/build graph, and full offline metadata now succeeds.
The standalone `storage_format` target passed independent review and all 22 tests, including the reserved zero-sequence boundary.
It is part of the default test gate, with exact discovery and explicit execution of any ignored annotations.
The root integration check reproduced the absent default format gate and accepted empty discovery, then verified default execution and rejection of empty discovery after the change.
The real `make test-storage-format` passed all 22 cases with no ignored tests and verified the selected native artifact.
The integration worker then ran `make check lint test` successfully: 73 engine/unit, 22 format, 24 CLI, 20 native, and 10 harness checks.
Seven database-dependent Rust cases remain assigned to the separate live engine gate.

## Integration checks

- [x] Create one unpublished Cargo package and pin its 19-package dependency graph.
- [x] Observe five contract tests fail, then pass exit-category and conninfo validation/redaction tests.
- [x] Observe three root-control tests fail, then pass finite-deadline, JSON-diagnostic, and real-signal subprocess tests.
- [x] Verify native libpq linkage through the runtime version test and the executable's dynamic-library listing.
- [x] Run `make test-transport`: four exact live cases passed with no skips.
- [x] Run `make test-db-self-check`: PostgreSQL 14.24, 10,003 rows, full-content fingerprints, and three rejected corruption controls.
- [x] Replace the inline CLI placeholder with the assigned parser and native renderers.
- [x] Connect native state and raw COPY interfaces through the root entry point.
- [x] Verify the completed COPY coordinator with the real executable through the independent harness producer.
- [x] Verify real executable SIGINT and SIGTERM cancellation through the database harness.
- [x] Remove the temporary crate-wide dead-code allowance.
- [x] Run integrated format, lint, unit/CLI, and database checks on the controlled macOS artifact.
- [x] Write the README and getting-started guide after the first real COPY harness pass.
- [x] Obtain the leader's conditional foundation-commit authorization and complete its review gates.
- [x] Resolve the independent COPY findings and obtain final static review of the bounded target profile.
- [x] Review the experiment format and add its passing suite to the default test target.

The first 16 CLI tests passed after wiring, but manual `--version` verification exposed help output instead of version output.
The CLI worker added a failing exact-output regression test and corrected precedence.
Its final handoff reports 22 passing black-box tests, 50 leaf command checks, and parsing coverage for 220 long options and 188 short bindings.
The subsequent three CLI compatibility fixes and help-output routing passed independent review.
The updated CLI suite reports 24 passing tests.
The root unsafe-comment lint failure is corrected.
The harness worker owns the integration-test formatting correction.
Independent transport review found that libpq can write passfile warnings directly to native stderr before notice callbacks are installed.
The warning can expose a path and bypass structured diagnostics.
The leader approved a controlled libpq patch that removes the two raw passfile warnings while preserving resolution and security semantics.
The transport worker reports that the guard and regression tests now pass on the controlled macOS artifact.
Its 17 unit tests include three guard checks and a subprocess regression covering eight warning routes.
Five exact live cases passed, including SCRAM service/passfile tests covering 16 authentication outcomes.
A deliberately stock-linked probe returned exit code 2 and one JSON `BadConfig` diagnostic without a raw warning.
The transport reviewer signed off those changes.
Independent native-build review found a test-only compiler-command parsing mismatch.
The native owner corrected both call sites, and independent recheck passed all 20 cases with default `CC` and with `CC='clang -fno-common'`.
The builder, recipe, selected artifact, and native-library hashes were unchanged by that correction.
Stock libpq cannot silently substitute for the verified native artifact.
The transport guard must return a structured `BadConfig` error before connecting when the loaded library lacks the required capability.
The root endpoint-mapping regression failed when `BadConfig` became `Source`, then passed after limiting remapping to `Pgsql` errors.
The root build script also pins the selected native link directory and runtime search path.
An isolated stock-to-controlled library switch reproduced stale `pq-sys` cache selection before that fix.
After the fix, the same cache linked the controlled artifact and excluded the old system-libpq path.
This link-selection check used macOS `otool`.
The Linux worker reports two identical native builds with the capability marker and OpenSSL/GSSAPI/OAuth enabled.
Its stock-library negative control returned the exact structured exit-2 diagnostic with zero connection attempts.
Independent CI review passed the seven owned files and nine runner guard tests, including official pin checks.
The complete Linux arm64 report passed the final 40-case harness snapshot and the subsequent Python optimization guard.
The native worker completed two clean offline builds with identical hashes for all 19 installed files.
The artifact-set SHA-256 is `4683bca47f886340012470eeafb7aaf214a4b31bc666250da1fb5f1b2194f827`.
The recipe SHA-256 is `e49420f6d4db7f0c1552b75f9c4590231f57ba0178ab26e16f176c7013b63a03`.
Its final `make lint test` passed 73 Rust, 24 CLI, 20 native-builder, and eight harness unit checks.
That run excluded seven live database tests, which require their separate isolated gate.
These results predate the remaining COPY review fixes.
Do not persist raw warning contents or redirect process-global stderr as a workaround.
State verification exposed a lock-release race when a child or duplicate descriptor retained the locked file description.
The state worker added a regression test and explicit native unlocking, with 32 state tests passing.
COPY review identified concurrent namespace DDL as separate from table-lock coverage.
The leader requires schema and namespace DDL quiescence in both databases throughout E1 COPY.
Source DML remains permitted under the snapshot, while target DML waits for retained table locks.
Native state requires exclusive control of the active private `0700` directory.
Arbitrary noncooperating same-user mutation is outside its guarantees because validation and rename are not a compare-and-swap operation.
Independent COPY review reported two successful executions with incorrect target data on disposable PostgreSQL 14.24.
A CHECK function changed client encoding before another table's COPY, causing altered text despite a completion receipt.
Another CHECK function deleted previously copied rows while the receipt still reported all copied rows.
The reviewer also reproduced a valid deferrable foreign key failing when lexical COPY order loaded the child first.
The author implemented the approved target profile and the static reviewer reported no blocker; the expanded live suite is still required before an E1 completion claim.
E1 refuses target CHECK/exclusion constraints, expression/partial indexes, and custom table/index access methods or operator classes.
The six built-in index access methods require matching `pg_catalog` operator classes with OIDs below 16384.
Deferrable foreign keys require parents inside the COPY set, identical built-in column types, and built-in comparison operators.
The coordinator defers supported keys before loading and validates them before final durability checks.
The target must report `session_replication_role=origin` through `SHOW` without an added privilege requirement.
Target internal constraint triggers must also be enabled for origin or always execution (`tgenabled` is `O` or `A`).
Disabled and replica-only triggers can otherwise bypass the promised constraint validation.
The independent harness producer passed all 38 prior executable cases on PostgreSQL 14.24, including genuine late `23505` and `23503` errors with wire evidence, rollback, and no marker.
The author passed the exact seven-case live engine gate.
The author added the internal-trigger guard to initial and locked target inventory before emptiness checks.
After that change, the author reports 73 unit, 22 format, 24 CLI, 20 native, 10 harness, and seven live engine checks passing.
The independent harness producer passed the final 40 executable cases on PostgreSQL 14.24 with the controlled native artifact: one Rust integration entry, zero failures, zero ignored or filtered tests, and 25.61 seconds elapsed.
The two new internal-trigger cases use actual orphan input and require exit 2, zero COPY bytes, no marker, and unchanged target tables and trigger states.
The frozen final inventory is 40 executable cases and seven live engine cases.
Reviewer Planck passed the final static COPY review, and the author confirmed no source changes after the tested final hashes.
The fresh Linux arm64 report passed all 11 stages on PostgreSQL 17.11 with 95 Rust, 24 CLI, 20 native, 11 harness, nine CI guard, seven live engine, and 40 executable checks.
It also passed the three oracle controls, two wire-fault controls, SCRAM cases, and stock-libpq refusal with zero connection attempts.
The report is `target/ci-linux-GaZ46U/report.json`, with source SHA-256 `90add6f2436f37ad4243a77257aeb978d4c5f025b5f18dd8b73dc05d9327b95a`.
This is local arm64 evidence, not a GitHub workflow run or Linux amd64 qualification.
Independent harness review found that optimized Python could remove assert-based validators.
The shared import guard now refuses optimized execution before fixture creation; the author reports 11 harness tests passing with 12 subprocess probes.
The executable and live engine inventories remain 40 and seven.
The Linux report includes the final optimization guard and all 11 harness regression tests.
The foundation commit must exclude unregistered E2B driver/common files and retain a compilable reviewed manifest.
The generated CLI fixture includes upstream help prose, so root `NOTICE` now preserves the exact pgcopydb `LICENSE` from the pinned baseline.
The two E2B files are outside the Linux snapshot; the only subsequent reviewed snapshot changes are the notice addition and this guide's pointer to maintained platform evidence.
The author reports 73 Rust, 24 CLI, 20 native, and 10 harness checks passing after the profile patch; this does not replace the final live gate.

The E1 command deadline is 300 seconds in total and does not reset between operations.
The package declares the verified Rust 1.99.1 toolchain as its minimum.
The 8 MiB limit applies to COPY forwarding/write chunks, not process RSS.
libpq may allocate a complete COPY row larger than that budget; the fixture contains a 9 MiB row.
Configurable operation/stall deadlines and complete transport memory accounting remain later production requirements.
Process-kill tests do not prove power-loss durability.
The E1 native completion marker does not authorize resume.
Successful COPY output includes the native run's 16-byte identity and state format version, with actual table, row, and byte counts.
It does not include the private state path.
The leader approved one synchronous target transaction across all E1 tables, retaining preflight locks until final COMMIT.
Verified empty catalogs and empty tables remain valid inputs with zero-count completion receipts.
Missing expected fixtures, missing test cases, and failed catalog queries remain failures.

## Handoff

Documentation checks passed across eight owned Markdown files with zero STE100 hard violations.
Local links, fenced blocks, trailing whitespace, final newlines, and the em-dash prohibition also passed.
These checks do not establish runtime correctness.
The integration worker takes ownership of running updates to this file after the documentation handoff.
E0/E1 signatures stay frozen unless the leader coordinates a change.
Review E2-E4 interfaces before their workers start.
Expand the E5-E7 and operator/release feature maps before each later milestone.
Full CLI/operator/release scope remains unfinished after E1.
