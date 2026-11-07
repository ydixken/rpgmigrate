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
The integration worker owns package/root contracts, root modules, command integration, README, and this task status after the documentation handoff.
Transport, CLI, and native-state implementations are handed off, with their focused review evidence below.
Harness integration covers the native version-two and target-authority slice.
State worker `01a1b228-5b80-7b83-9a6d-d32d77098814` completed its assigned module.
COPY/catalog worker `01a1b22d-2a70-7321-bd8d-ca3a33441f56` completed its reviewed handoff.
The tooling milestone assigns the workflow and `tools/ci/` to its CI worker; the integration worker owns the active guides.
The canonical E2 codec and decoder work has separate owners and stays outside the tooling snapshot.
The root entry point routes help, native version, and real ping.
The root COPY route calls native state, connection, planning, and COPY interfaces.
The COPY coordinator passed the recorded 40-case executable gate, including native version-two receipts.
The bounded E1 profile passed its recorded correctness gates; full migration compatibility and performance remain unverified.
The publication section records the public repository and exact GitHub run evidence.
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
That 21-package audit excluded them from the normal/build graph, and full offline metadata succeeded.
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
The author implemented the approved target profile; the final expanded live evidence and independent review appear below.
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
The E1 foundation inventory was 40 executable cases and seven live engine cases.
Reviewer Planck passed the final static COPY review, and the author confirmed no source changes after the tested final hashes.
The fresh Linux arm64 report passed all 11 stages on PostgreSQL 17.11 with 95 Rust, 24 CLI, 20 native, 11 harness, nine CI guard, seven live engine, and 40 executable checks.
It also passed the three oracle controls, two wire-fault controls, SCRAM cases, and stock-libpq refusal with zero connection attempts.
The report is `target/ci-linux-GaZ46U/report.json`, with source SHA-256 `90add6f2436f37ad4243a77257aeb978d4c5f025b5f18dd8b73dc05d9327b95a`.
This is local arm64 evidence, not a GitHub workflow run or Linux amd64 qualification.
Independent harness review found that optimized Python could remove assert-based validators.
The shared import guard now refuses optimized execution before fixture creation; the author reports 11 harness tests passing with 12 subprocess probes.
That foundation snapshot retained 40 executable cases and seven live engine cases.
The Linux report includes the final optimization guard and all 11 harness regression tests.
The foundation commit excluded unregistered E2B driver/common files and retained a compilable reviewed manifest.
The generated CLI fixture includes upstream help prose, so root `NOTICE` now preserves the exact pgcopydb `LICENSE` from the pinned baseline.
The two E2B files are outside the Linux snapshot; the only subsequent reviewed snapshot changes are the notice addition and this guide's pointer to maintained platform evidence.
The reviewed foundation is committed as `4e7eeccb2da9ba236db1714c9f7b4801a1155189`.
An exact staged export formatted and compiled without the two unregistered E2B files.

## Bounded persistence experiment

- [x] Approve the common config, finite candidate selection, and closure-scoped batch interface.
- [x] Freeze the unregistered common interface in `bench/storage/config.rs` and notify the leader.
- [x] Implement bounded common validation, required-prefix recovery, and explicit checkpoint finalization.
- [x] Register the driver and recovery tests after the foundation commit and candidate handoffs.
- [x] Run every required nonempty workload against every delivered candidate.
- [x] Obtain independent review before accepting E2B results.

The first experiment uses one stream, one writer, one active source transaction, and synchronous storage batches.
Required recovery progress comes from an external prefix; zero denotes an empty required prefix.
Candidate completion is not production qualification or a storage-backend selection.
Multi-process access, retirement, quotas, and broader fault gates remain unchecked.
The common contract passed independent re-review after a callback-exposure finding.
Its first bounded scan pass validates all records and the captured frontier before the second pass emits completed transactions.
Nineteen focused common tests passed independently, including late-frame and descriptor corruption with zero callbacks.
SQLite passed independent review with 18 backend tests; journal passed with 15 backend and three focused common tests.
The redb reviewer reproduced a panic on clean-file corruption before integrity-on-open was enabled.
The corrected backend passed 19 tests and an independent valid control plus seven corruption probes that return structured errors.
Separate settings review passed seven focused tests and proved all preexisting backend lines unchanged.
SQLite reports queried values, journal nests that catalog evidence, and redb labels configured policies without claiming introspection or RSS limits.
The 31-package dependency delta passed independent checksums, licenses, advisory, features, and bundled SQLite source review.
The previous 21 package records were unchanged; the locked graph SHA-256 is `33778c3872ebac34b2ce9446067729ce9b45b03ce5d633314b0060810a6a9828`.
The reviewed driver passed 86 recovery tests: 19 common, 58 backend, and nine driver cases, with no ignored tests.
The committed experiment's default gate requires exact discovery of all 86 cases and all 22 format cases before execution.
The newer identity slice adds one format/JSON case.
`make lint` passed after the driver freeze.
The manifest requires three nonempty workloads, all three candidates, and three repetitions in fresh child processes.
The runner records storage throughput, checkpoint/reopen/two-pass scan timings, queried or configured settings, and per-child RSS.
Faraday passed independent driver review using an isolated Cargo target directory: nine driver tests, scoped Clippy, formatting, native verification, and three fresh-child checks passed.
The root worker's serialized `make check test` passed, including all 86 storage cases, and `make lint` passed.
That gate passed 76 runtime unit, 24 CLI, 20 native-builder, 12 harness, 22 format, and 86 storage checks; 14 live tests belong to their separate engine gate.
The release pilot returned exit zero with all 27 expected receipts from 27 distinct child PIDs on Darwin arm64 with Rust/Cargo 1.99.1, SQLite 3.54.1, and redb 4.5.0.
Every candidate used the same canonical input and storage-batch boundaries within each workload.

| Workload | Frames | Completed transactions | Encoded bytes | Storage batches |
| --- | --- | --- | --- | --- |
| Small commits | 512 | 128 | 126976 | 1 |
| Wide events | 48 | 4 | 33559168 | 5 |
| Giant transaction | 258 | 1 | 135289136 | 19 |

The raw report is `target/storage-pilot-20261106/report.json`, SHA-256 `7cd98d68dd17b370b2937d62c38c6ffe95a401e9cdd120af61d36abf258424c1`.
Its actual input manifest is `target/storage-pilot-20261106/cases.json`, SHA-256 `f1496e15bdf15ac7bb624c0725783be9195c420e4688dbc651e9509d6a9d5e55`.
`target/storage-pilot-evidence.json` records all 29 JSON file hashes, the executable hash, toolchain versions, and frozen input manifest.
The report contains seven measured phases plus total time, including encoding, checkpoints, reopen integrity work, and both verified-scan passes.
Observed giant-transaction child peak RSS ranges were 7274496 bytes for segmented storage, 77266944 to 77283328 for redb, and 94978048 to 94994432 for SQLite, with the same configured 67108864-byte cache budget.
These are three repetitions on one Mac with warm-after-write reads and do not select a backend or establish production memory or durability bounds.
The actual release executable rejected `LIBSQLITE3_SYS_USE_PKG_CONFIG=1` with structured exit one before creating an output directory.
Jason's [research note](../docs/research/2026-11-06-storage-pilot.md) and four audited JSON archives under `bench/results/2026-11-06-macos-arm64` passed Faraday's final read-only review.
All archived bytes match the originals, all ten input hashes match committed `905c323`, and all nine result rows were independently recomputed.
The note SHA-256 is `4050f4fd9dd57f420518e3f1321323a60f8203c1334899f424680a83ff0a7f41`.
The README now links to it.
The reviewed six-file documentation/archive set and README pointer are committed as `08a63ebb5ab908b8b3d9ce4e5b5344be9dccb019` directly on the qualified `905c323` source base.
Only those seven files were staged.
Review hashes, staged links, Markdown checks, JSON decoding, and whitespace checks passed without runtime reruns.

## Prepared transport and documentation

The private prepared-transport slice passed independent unit and seven-case live review.
The committed prepared snapshot's full engine inventory is 14 cases, preserving all seven E1 cases.
The transport-only inventory remains five and executable COPY remains 40.
Harness and CI guard review passed 12 and ten offline tests on Python 3.15 and 3.9, including 36 negative checks.
The prepared API remains unused by a production CDC consumer and does not enable runtime CDC commands.
Narrow expected-dead-code attributes must be removed when the checkpoint/applier consumer is integrated.
That prepared-transport milestone deferred canonical codec promotion until a consumer and its API were ready.
The initial [durability guide](../docs/durability.md) passed independent review at SHA-256 `b152b61874e55f7fd4214c4644aad8dbfa16642898f74b54200b5d03097cb526` and separates the approved design from runtime gates.
Its later barrier wording passed the same read-only review at SHA-256 `f533ba943ac2a165325419a697d2b4639d428af4dee511cf93b10123e618077c`.
A concurrent broad test run reported a transient missing `target/debug/rpgmigrate` executable in CLI checks.
The root worker's serialized gate passed after file freeze and did not reproduce that failure.
Its original cause remains unproven; no code fix was made on the assumption of a build race.
Prepared transport, its harness/CI inventories, and the durability guide are committed as `3725c8d`.
The exact prepared-only staged export passed `make lint` against a byte-identical copy of the verified native artifact and the foundation dependency graph.

## Canonical identity and E3 consumer integration

- [x] Close the reviewed prepared and storage commits: `3725c8d` and `905c323`.
- [x] Extract the seven reviewed ID/position definitions into private `src/identity.rs` without codec or runtime dependency changes.
- [x] Re-export canonical `RunId` through runtime contracts and preserve benchmark/test imports through the same source file.
- [x] Hand off exact `crate::identity` types to the state and authority authors.
- [x] Close independent state version-two and canonical identity review with 54 state and 23 format tests passing.
- [x] Close review of the metadata role-name correction and quoted/numeric writer regression.
- [x] Close independent CI receipt-guard review at the frozen three-file hashes.
- [x] Close independent harness review at the frozen ten-file hashes.
- [x] Close independent checkpoint review at its frozen hash.
- [x] Require the harness owner's final exact 30-case engine receipt after the metadata correction.
- [x] Resolve staging warnings through the real provenance/authority consumers and narrow expectations on pending stage interfaces only.
- [x] Integrate state-owned version-two provenance and the internal authority consumer after their independent handoffs.
- [x] Verify identity/format regressions and the integrated consumer gates before another commit.

The state worker owns native provenance types and operations, with no SQL or authority dependency.
Canonical `RunId`, `SourceId`, and `StreamId` derive serde for version-two state.
Named identity objects reject unknown fields.
The other four type definitions kept their reviewed fields and traits, and that identity-only snapshot left wire codec logic in the experiment.
The new identity JSON regression failed to compile before serialization support existed, then passed with all 23 format/identity and 86 recovery tests.
The extraction-only check reported six unused-type warnings.
Real consumers now use these types, and the integrated all-targets warning check passed.
No lint suppression or redundant construction was added to the identity file.
The authority's nonempty frontier carries the full `TxId`, `TxSeq`, commit LSN, and end LSN so equal positions and XID reuse cannot alias a descriptor.
An empty frontier uses sequence zero with no transaction identity.
Begin ordinal is not commit order.
The authority worker owns `src/apply/{mod,checkpoint,checkpoint_live_tests}.rs` and imports the state-owned provenance operations without a reverse dependency.
The metadata helper reuses named prepared statements per connection without adding a generic cache or transaction ownership.
Carver owns the role-name correction in `src/apply/metadata.rs` and its quoted/numeric writer regression inside the existing `missing_privileges` live case.
That narrow live-test handoff changes no inventory count or harness fixture.
Descartes coordinates the scoped regression and final combined run.
Its author reports nine focused tests passing at SHA-256 `c713fa39da7cbd4ddad161682ed9630768387ddb60166db61cf0180d0d7fa93d`.
Independent metadata review reran nine tests and found a role-name lookup defect.
Casting raw `CURRENT_USER` through `regrole` reparses quoted or numeric writer names instead of matching the catalog name.
The metadata owner replaced the two reparsing casts with the OID from an exact `pg_roles.rolname = CURRENT_USER` join.
The existing `missing_privileges` case reproduced both quoted-name and numeric-name failures, then passed both after that correction on PostgreSQL 14.24.
The root worker verified the one-case RED and GREEN logs and the new metadata/live-test hashes.
This scoped run does not close the 16-case or 30-case gates.
The corrected metadata hash is `46006ba2cf17418d7f1a1c0e2546359aeca658b6ec981cf175d16af5667f0b8e`, and the live-test hash is `de6fcc1324068fd4fbd1d5fef6a2a7bbeca18a998a2417e43a6a35abadf729e3`.
The owner reports nine metadata unit tests, formatting, and scoped Clippy with warnings denied passing.
The corrected two-file handoff is frozen.
Carson passed the corrected metadata and independently reran all nine metadata unit tests.
Reversing the three SQL changes reproduced the prior reviewed file hash exactly.
Mendel passed the checkpoint implementation without a finding and independently ran its four focused unit tests against the controlled libpq artifact.
He reviewed the local-error poison regression and held-COMMIT evidence without repeating live tests.
Metadata SQL remained a separate review.
Planck passed the frozen harness and closed the read-only review.
Faraday passed the 30-case CI/footer delta without a blocker.
Zeno, Goodall, and Carson have closed their author/reviewer handoffs.
The root worker retains broad-gate ownership after these remaining scopes freeze.
Restricted preprovisioned operation binds the actual object owner separately from the migration role and requires explicit schema/table grants without owner-role membership.
Its production transaction commit is synchronous.
Asynchronous execution is test-private and does not enable relaxed mode or source ACK policy.
The same logged checkpoint UPDATE increments its barrier counter, and positive synchronous COMMIT returns an opaque proof without a second COMMIT.
Standalone barriers use a real logged counter write to cover asynchronous or no-new-data flushes, with the same target and identity checks.
They also support an empty checkpoint through `Option<Frontier>`.
An empty proof covers zero CDC transactions, not a requested end position or COPY snapshot.
The transport owner adds only the checked transaction-status accessor needed by that authority.
The accessor passed independent review and two focused tests.
Its author has frozen the handoff.
The harness defines 16 authority case names under `apply::checkpoint::live_tests::` and requires all 30 engine cases, including the previous 14.
The root `test-authority` target uses that exact harness profile and namespace.
The default unit filter excludes only the separately gated live namespace.
The authority bodies, fixtures, and receipt guards passed independent review and the final integrated live gate.
The authority author reported a 16-case live pass including fsync-off, but the leader then found an uncovered local-error poison gap.
Successful DML followed by a caught local prepare/execute error could leave PostgreSQL in a committable transaction.
The authority owner changed only `checkpoint.rs` and `checkpoint_live_tests.rs` for a sticky failed-state fix and a regression inside the existing `commit_rollback_tag` case.
Explicit rollback remains allowed.
The inventory stays 16 authority and 30 combined engine cases.
The poison regression reproduced successful COMMIT after a caught local error, then passed after a sticky failed-state fix.
The existing case now covers five local prepare/execute error variants after prior DML and verifies unchanged data/checkpoint state.
Explicit rollback remains available.
The corrected author run passed all 16 cases on PostgreSQL 14.24/140024 with both held-COMMIT fault receipts, 13 apply/metadata unit tests, and scoped Clippy.
The revised checkpoint hash is `315a90c78db6a18a5be308cf705d538bc45a8f9fb79ba9c7503fecd824893596`.
That poison run used live-test hash `35f066fe98a8de470ac60adeee8587304603d5641baa868c7c3b33a6b8e5a09b`, before the later role-name regression.
All independent authority, metadata, harness, and CI reviews passed, followed by the serialized root check/lint/test gate.
The poison fix left metadata, state, and canonical identity hashes unchanged.
The state author handed off `src/state.rs` at SHA-256 `16f445d44c593cac570550df770c5ba107068e3eea142b3a8cc21923502f6081` with 54 tests passing and no ignored cases.
The state handoff includes strict persisted-data checks, a single-record/16 KiB cap, immutable provisioning identity, and publication errors before and after rename.
The state author's earlier scoped Clippy run encountered unfinished authority staging diagnostics.
The later integrated gate passed with warnings denied.
Planck passed the frozen state version-two implementation and canonical identity extraction review.
All 54 state and 23 format/identity tests passed with no failures or ignored cases in that independent run.
The reviewed state and identity hashes matched at the end of review, including unchanged codec golden bytes and strict identity JSON.
The active authority body was excluded from that focused review and subsequently passed its separate review and integrated gates.
State and identity owners will make no further edits without a review finding or a concrete integration need.
The root worker removed fulfilled staging expectations on PG prepared calls, transaction status, and their exports after actual authority call sites landed.
Only the binary parameter variant retains a narrow pending-serial-applier expectation.
PG logic and API bodies are unchanged.
The attribute-only comparison is recorded in `target/pg-expectation-cleanup-evidence.json`, and the final root lint gate verified the integrated uses.
The state owner then added one narrow expectation to the internal recovery entry point until the production resume consumer is wired.
Removing that attribute reproduces the reviewed state hash exactly.
The new state hash is `ed3e9ff4caaae7f94432358c65ccdaffe416a6b01d904d25b5ca78f45f7c769b`.
The owner reports scoped library Clippy with warnings denied and one intent/reload regression passing after that nonbehavioral change.
No helper, identity, or state behavior changed, and the complete 54-case suite was not repeated for the attribute alone.
The harness owner updated COPY receipt checks to require literal format version two and empty target provenance, preserving the 40-case inventory.
The new version oracle failed against the old guard before all 13 harness tests passed.
The first authority atomic test reached the intended unavailable-provisioning error.
This is failing implementation evidence, not a passing authority case.
The harness owner subsequently passed all 40 COPY cases against native version-two state on controlled PostgreSQL 14.24 using an isolated target directory.
That 25.40-second run required literal format version two and empty target provenance without changing the expected case inventory.
The harness owner also reports 14 fixture unit tests and authority self-checks passing for restricted ownership/grants, real standby, isolated fsync-off, and two held-COMMIT faults.
The fixture checks were followed by the final 30-case engine gate on the reviewed authority/metadata freeze.
The harness owner's earlier 30-case run preceded the local-error poison fix and is superseded for the final gate.
The frozen harness rejects missing, zero, ignored, or pending authority-fault receipts in both the authority-only and combined-engine profiles.
The owner reports all 14 offline guards passing on Python 3.15 and 3.9.
A real-psql self-check covered restricted ownership, standby, the fsync-off server, and both held/released COMMIT faults.
The root worker verified all ten frozen harness file hashes against `target/verify/harness-authority/harness-freeze.sha256`, SHA-256 `f6470f54aad9e68e018b4f6b4bebf9069f23c6925c45bf268eb0323d33049781`.
Planck independently passed all 14 offline harness tests on both Python versions and 24 inventory negatives, with all ten frozen files unchanged.
A 30-case run had started before the metadata finding arrived and passed in 6.00 seconds with both fault receipts.
It remains intermediate evidence until the role-name correction is covered.
That log is `target/verify/harness-authority/engine30-before-role-fix.log`, SHA-256 `9fa2edf095423c73f11178aab046ce4ff8658409224f18e4bd70a37ef6b89058`.
The leader released the harness owner as sole producer of the final 30-case gate after the corrected metadata freeze, in parallel with independent review.
The final run passed all 30 cases with zero failures or ignored cases in 8.13 seconds on PostgreSQL 14.24 and verified both held-COMMIT/fence-release receipts.
Its log is `target/verify/harness-authority/engine30-final.log`, SHA-256 `495fa37920c31a30cde1613fc652ce666710807e4b05708ff2877c6b08267445`.
The metadata, checkpoint, live-test, and ten harness hashes matched before and after that run.
The root worker verified the final log and matching frozen hashes before starting its gate.
The leader authorized serialized root lint/check/test, documentation and ledger review, and a conventional commit of the reviewed snapshot.
PG/apply source edits remained frozen until that commit.
A fresh Linux gate will use that exact commit.
Unrelated exports and local proof artifacts stay outside the commit.
The CI owner froze its 30-case parser and exact two-fault footer requirement after 12 offline tests and shell, YAML, workflow, syntax, and whitespace checks passed.
The tests reject the old 14-case result and missing, duplicate, wrong-count, unheld, pending, or ignored fault evidence.
Faraday independently passed all 12 offline CI tests, refused 27 negative receipts, and accepted six valid receipt orders.
The checked engine inventory is exactly 16 authority, seven prepared, and seven other cases.
COPY remains 40 and the Linux profile retains PostgreSQL 17.11.
The root worker matched the reviewed hashes: `verify.py` is `42f16c256d2b341c8f32d03afff2a7b27de2043821e09dcf4814cac24e014046`, `test_verify.py` is `1fbbd7c8d0656fafbc1bf80bb883f9925d8c0933723c401b7b14cf1f5dc388dc`, and `docs/testing.md` is `6888ab9981c682de5c0a24b492fc048a350c491b274cf375ef8720a48e9608db`.
No CI review blocker remains.
No Linux run was performed for the new slice.
The serialized root command was `env PATH="/opt/homebrew/bin:$PATH" make -j1 PQ_LIB_DIR="$PWD/target/native/libpq/lib" check lint test`.
Native artifact verification, all-targets Cargo check, formatting, and all-targets Clippy with warnings denied passed.
The default gate passed 222 Rust tests (113 unit, 23 format/identity, and 86 storage recovery/driver), plus 24 CLI, 20 native-build, and 14 harness tests.
The 30 live cases were excluded from the default unit command and passed separately in the exact engine profile.
No selected case was ignored.
The gate log is `target/verify/root-authority/check-lint-test.log`, SHA-256 `15d142b0f2137541c15464d40073ca059da54d68d94431028b70200dc6b00569`.
The existing native version-two COPY40 evidence remains applicable and was not repeated for authority-only changes.
Target system identifiers are optional for restricted roles and older PostgreSQL versions.
Persisted identifiers must be verified when present.
Receipt-only recovery still requires matching target evidence, and absence without strong binding fails closed.
No DSN persistence or new privilege requirement is introduced.
Public CLI resume remains unsupported until the complete authority path is integrated.
The Linux arm64 worker passed all 11 stages for an ordinary export of committed `905c323`, separately from this newer working slice.
The frozen run passed 184 Rust checks (76 unit, 22 format, 86 recovery/driver) on PostgreSQL 17.11/170011.
It also passed 24 CLI, 20 native, 12 harness, ten CI guard, 14 engine, and 40 COPY cases.
Three oracle negatives, two wire faults, SCRAM, and stock-libpq refusal with zero connections also passed.
The report is `target/ci-905c323-n59hsquu/report.json`, SHA-256 `4b0d82414097a73192c0b7a302162131f1b3b1cecd5bbc528eaf0868607fc7e8`, for source SHA-256 `f90665004d5589457b53864ff4f321ae3c175481abc655a3e9397ed3fcd191cc`.
The worker verified all 77 committed files and their modes before and after the run.
This verifies the archived commit, not the subsequent native-state/authority work, amd64, hosted CI, or Linux performance.

## Approved next execution sequence

- [x] Close the authority slice's independent reviews, final live gate, and serialized root checks.
- [x] Freeze the existing source CopyBoth transport proposal through the leader and implement it independently of storage selection.
- [ ] Add E2 complete-transaction readers, storage qualification, and real-slot `test_decoding` ingestion.
- [ ] Qualify multi-process access, reader pins, pruning, quotas, and recovery before choosing a production storage backend.
- [ ] Connect `ReadyTxn`/`TxReader` to E3 serial apply and verify actual target effects and recovery.

Beauvoir's focused read-only review covers source frontier semantics across plugins.
The primary-source review reports that validated `test_decoding` COMMIT envelopes provide a transaction-end LSN but no physical commit-record position on PostgreSQL 9.6 and 18.
It also records that baseline wal2json omits LSN fields by default, while pgoutput v1 provides both positions.
The next receiver contract keeps `CommitLsn` physical, represents its absence explicitly with `Option`, requires `SourceEndLsn` for complete COMMIT, and preserves the decoder's envelope context.
It must not fabricate a commit position from the end position or authorize keepalive ACK advancement.
That separate tested contract change must address codec versioning if required and metadata shape/CHECK/proof semantics before the first decoder.
It does not change this library-only authority snapshot.
The archived single-writer pilot does not replace full E2 qualification or select a production backend.
Newton's exact redb 4.5 three-process prerequisite probe passed on macOS with shared MultiWriter modes, live commit visibility, atomic checkpoint/deletion, and reader-pin retention across 16 commits.
Compaction remained blocked until pin release.
This result requires no IPC architecture change.
Read-only open still skips integrity checks, and role ownership, pruning authority, quotas, crash recovery, Linux, and power-loss qualification remain open.
The probe wrote only ignored artifacts and selects no backend.
This sequence stays within the approved architecture.
The leader freezes the transport API without a new user approval gate.
No WAL runtime or applied-throughput result is claimed by the authority slice or storage pilot.

## Source transport and optional commit positions

- [x] Commit the reviewed private authority slice as `eaf7517f3ef4e77144d81c9aac1d8b819833c851`.
- [x] Freeze and review the source CopyBoth implementation and its fixture contract.
- [x] Freeze and review the experiment codec's optional physical commit position and version-two wire format.
- [x] Freeze and review target authority and state metadata-version-two semantics for an optional physical commit position.
- [x] Close independent review of the state's metadata-version-two constant and refusal fixtures.
- [x] Close independent review of the source slice's CI inventories, fault receipts, and testing guide.
- [x] Integrate the frozen inventories and run the next serialized root gate after component reviews.
- [x] Verify the reviewed contract-only tree against stable `eaf7517` PG/harness code before a separate fix commit.

Herschel owns the source CopyBoth transport implementation.
The frozen source fixture requires exactly nine replication cases within the 39-case engine inventory.
Descartes owns the corresponding fixture and receipt integration.
Dewey owns the approved five-file codec extension, including the bounded constructor changes.
The codec author froze five files after 31 format and 87 recovery tests passed with no ignored cases.
The RED phase produced 15 format failures and one recovery failure before the codec change.
The root worker matched all five frozen hashes, including format `06a46ac128bd48482c31138aa40cf84deb10f74f2329046c97b1db6758439432`.
Owned formatting and whitespace checks passed, and the driver/redb edits change constructors only.
The first codec Clippy attempt encountered 16 known PG staging diagnostics before reaching the codec targets.
The separate contract-only export later passed strict all-targets Clippy against stable PG code.
The codec author added no suppressions or edits outside ownership.
Faraday independently passed all five codec files, 31 format tests, the new cross-candidate recovery case, and four Python golden checks.
The root Makefile now requires exactly 31 format and 87 recovery tests after that accepted review.
Zeno owns `Option<CommitLsn>` in the authority frontier and target metadata version two, without DDL or conversion.
The existing 19-column layout and 26-parameter comparison remain unchanged.
Zeno froze four authority files after 13 unit tests passed.
Mendel independently passed those 13 units and the optional-position semantics with no finding.
Zeno passed the exact 16-case authority run against the contract-only export with stable PG and harness files.
Meitner owns only `src/state.rs` and exposes `TARGET_METADATA_VERSION = 2` as the crate-private single source for authority imports.
Native manifest `FORMAT_VERSION` remains two.
Meitner froze the one-line production change and its literal-version fixtures at SHA-256 `121e7a80a4c7b83611e72c0b74935defa424b25c74160be85f1c732401a41d94`.
The explicit version-one refusal test failed before the constant changed, then all 55 state tests passed with no ignored cases.
The author also passed controlled-native verification, scoped library Clippy with warnings denied, formatting, and whitespace checks.
Planck independently passed all 55 pure state tests and found no issue at the frozen `121e7a80` hash.
That isolated run linked stock libpq but made no PostgreSQL calls, so it provides state evidence only.
This handoff adds no schema, DTO, path, locking, or conversion changes.
Jason owns the CDC plan, durability guide, and assigned source-frontier context documentation.
His three-file handoff is frozen and matches the codec and authority contracts without a new decoder feed signature.
The author reports zero hard STE findings, five advisories, ten valid local links/anchors, and unchanged pilot artifacts.
The root worker matched plan hash `0237d243f9894891d914d65cd2176d701e874b4dce3425e651855437b18e0cbb`, guide hash `16690bd3d2d3377ce2a19662b1cf76218577e848c58f1d949373099a45bf4ddb`, and context hash `8efb2714bde4a733dfcb34f2a0657955fd9f826d49b4841e5b84930a5567fc00`.
Faraday independently passed the three documentation hashes and ten local links/anchors, with the five pilot files unchanged.
That review found no new API or premature CLI, ACK, or performance claim and does not replace runtime gates.
The root worker retains root integration, CLI integration, root guides, and the task ledger, with no additional source change needed for these handoffs.
The next combined source-transport gate waits for its owner freezes and independent review.
The reviewed optional-position contract uses a separate temporary Git index and ordinary tree export based on `eaf7517`.
Its check/lint/test and authority16 gates exclude active PG and replication-harness edits while preserving those working files.
The native verifier rejects external or symlinked prefixes, so the export uses a byte-identical copy of the verified artifact with unchanged guards.
This contract-only change adds no client ABI or dependency change and does not require another Linux run before the later source snapshot.
The optional-position fix is committed as `0dbae0d6d2f7017711f51b07554604cccca83706`, with exact tested tree `6884fb58197a61ce5e366043160b433f37611429` and 16 selected paths.
Its isolated check/fmt/strict-Clippy/test gate passed 232 Rust tests, 24 CLI tests, 20 native tests, and 14 harness tests.
Authority16 passed with both real positive-COMMIT fault receipts.
All 91 exported files retained their exact Git blobs and modes, and the commit preserved other workers' changes.
The [contract handoff](../target/verify/contract-only-ype27e06/handoff.json) records commands, counts, hashes, and the copied native artifact's main-prefix install-name dependency.
This local macOS result makes no relocation or later-source qualification claim.

The source transport, paired harness, and CI changes have completed independent review.
Carson closed the PostgreSQL 9.6 option-name finding after predispatch refusal of embedded quotes, then passed the renamed pure protocol module and nine focused unit tests.
Mendel closed the FFI/pump review after a public-libpq probe showed that `PQflush` can report an empty buffer after discarding a failed write.
The private event is `FeedbackDrained`: it preserves supplied positions but proves no sending, peer receipt, acknowledgment, or durability.
Error-first handling, one pending status update, and positive all-results completion remain required.
Planck passed the paired terminal-error and keepalive harness semantics, including blocked `PQflush`, SQLSTATE `57014`, closure before deadline, and the exact fault inventory.
The [independent rename proof](../target/verify/replication-harness/rename-proof-86fmz_e9/proof.json) reverses the two names and one declared no-delivery sentence to recover all eight reviewed old hashes.
It also records the author's 19 passing offline tests on each Python version without claiming a root rerun.
Faraday passed the three-file CI delta, 14 offline tests, 33 rejected invalid receipts, 24 valid receipt orders, and the documentation links.

The source-only snapshot selects 20 paths over `0dbae0d` and excludes all C1 code, test-count changes, and its uncommitted ledger section.
It retains 31 format and 87 storage-recovery tests from the committed baseline.
`make test-replication` runs the existing wrapper profile and exact `pg::replication_live_tests::` namespace.
The default unit filter excludes that separately gated live namespace, while the combined engine target retains every ignored live case.
Descartes is the sole engine39/COPY40 producer, followed sequentially by the root check/lint/test gate on the same export.
The [source handoff](../target/verify/source-only-0x48g1ar/handoff.json) records the final tree, native receipts, commands, and gate results before a path-scoped commit.
No source CLI, decoder completion, ACK policy, production storage selection, or WAL throughput result follows from this transport milestone.

Goodall captured an ordinary export of exact commit `eaf7517` before starting its separate Linux arm64 gate.
All 91 committed files matched their Git blobs and modes, and the worker hashed 86 pipeline inputs before execution.
The export uses tree `94cc88440fd4f6ae458bb9a44e819c6027dddcf4` and source SHA-256 `1e561d7e584cfb6833a7bde43e4c215ae85d2aa1e930c4abca1af865e7ab9ab2`.
Its archive SHA-256 is `7f34b6f1a38241730c75fbec7fa87e8153cc209c2733b21f143530c050c6c6d3`.
The Linux arm64 gate passed all 11 stages on PostgreSQL 17.11/170011 with Rust/Cargo 1.99.1.
It passed 222 Rust, 24 CLI, 20 native-build, 14 harness, and 12 CI tests, plus engine30 and COPY40.
Both real held-COMMIT fault receipts, three oracle negatives, two wire faults, SCRAM, and stock-libpq refusal with zero connections passed.
All 91 exported files and 86 pipeline inputs remained unchanged after the run.
The root worker verified the report SHA-256 `ca39b1f77e2a9c4f051bccc4cf6d93b8dacb68f205cd2db303bee2a946e22917` and manifest SHA-256 `2bb9446ad00f697d1f513d1ca509d910f7df22cb218ab30fdd19c822b9ea0f31`.
Evidence is under `target/ci-eaf7517-28qtnuzg`, and the worker removed its owned container.
This result qualifies only committed `eaf7517` on that fixture, not later working-tree changes, CopyBoth, codec version two, amd64, hosted CI, or performance.

## C1 development-only storage milestone

- [x] Freeze the seven implementation/test files and the common role/catalog contracts.
- [x] Add persistent revisions, bounded record numbering, completion indexes, and stable two-pass read views.
- [x] Verify the healthy nine-process Receive, Apply, and Status schedule against external input and prefix oracles.
- [x] Close component reviews, including the role-lock and trailing-output corrections, with redb's negative qualification recorded separately.
- [x] Run the exact C1 integration snapshot's required gates and preserve the final receipt.
- [x] Commit the tested frozen C1 blobs while preserving concurrent codec and tooling edits.

The [C1 report](../docs/research/2026-11-06-storage-concurrency.md) records the reviewed sources, healthy proof, negative probe, and remaining qualification gates.
Common/catalog, SQLite, segmented journal, and driver reviews passed within their stated scopes.
The redb healthy checks passed, but its Status corruption probe panicked before returning a controlled error.
The pinned redb 4.5.0 API has no adequate public read-only integrity check, so it is not qualified for production selection under that requirement.
Its blocking-open limitation is a separate C2 gap.
We retain redb only as a development comparison and preserve the negative evidence.

Root discovery found 141 recovery/driver tests, and the frozen Make guard required that exact nonempty inventory.
The format inventory remains 31.
The [C1 integration handoff](../target/verify/c1-integration-h6s1tz_m/handoff.json) records the final snapshot and gate results.
Commit `96f1d2d6ff1edf972688440ef65c8adcac0b7bcc` matches tested tree `1ffdbee4db8603df78013fd88351ff4522de481e` across 99 source files.
The exact export passed `make -j1 check lint test`: 126 unit, 31 format, 141 recovery, 24 CLI, 20 native, and 19 harness tests, without failures or ignored cases.
Its fresh healthy schedule passed nine distinct child processes, each with external prefix four, four frames, four completed transactions, 1024 encoded bytes, and input CRC32 `1217052608`.
Receive, Apply, and Status checked eight, five, and nine phases respectively for each candidate.
The report SHA-256 is `27c65af420c8d2887a6d79f0fcfc57c40b5ff27033946a2e888a58b10f188447`.
The old CI receipt parser accepted the captured output dynamically; it did not hardcode the previous recovery count.
The snapshot excluded new tooling files and retained the existing Make-based gate.
The commit used frozen Git blobs without replacing concurrent working files or staging their changes.
No runtime source, codec wire format, or Cargo dependency change belongs to C1.
Its consumer cursor measures benchmark progress only, with all history retained and one active source transaction.
Pruning, quotas, target proof integration, broader faults, power-loss behavior, Linux qualification, and backend selection remain open.

## C2 publication and recovery experiment

- [x] Freeze the explicit 13-point observer contract for the development driver, common facade, and SQLite/journal hooks.
- [x] Complete independent reviews and integrate the frozen C2 files in a separate snapshot.
- [x] Register the publication driver in default Task/CI verification with a fresh private root and a required 22-case, 20-continuation receipt.
- [x] Verify the required 20 process-kill cases and two complete traces against external record and prefix oracles.
- [x] After every killed child, append the next parent-constructed transaction through a fresh Receive handle and verify the full retained history.

The contract retains the existing receive API and adds a shared per-call observer without global or environment hooks.
The expected prefix is one before catalog COMMIT and two after it; a killed caller's original result remains unknown.
Readable reopen alone does not satisfy recovery acceptance.
The continuation must preserve earlier records and orphan raw history while proving that the next publication succeeds.
C2 remains a development-only process-crash experiment with all history retained.
It does not qualify power-loss behavior, pruning, quotas, retirement, target proofs, production storage selection, or redb's known read-only corruption gap.
Its source, tests, and inventory changes stay outside the tooling snapshot.
Independent common/driver review passed nine focused tests, the 22-job matrix, and all 20 post-crash continuations.
Independent SQLite review passed 42 focused tests, including all nine C2 cases; one older child-process test was outside that review's execution constraints.
The author's complete 43-test SQLite run and 164-test aggregate remain separate evidence.
The 164-test aggregate and 22-child report bind the pre-fix journal; they do not validate its replacement hash.
Journal review found that a caller could catch an observer panic inside the raw callback before its failure latch was set, then commit that raw batch.
The common guard contains this path; the journal author corrected the raw failure latch and observed the new regression fail before all 28 journal tests passed.
Independent journal re-review passed SHA-256 `31049754bf86c48acf0257c7aa1b66595b92f533a9a3a21bf2960c42e2128de4`: one fresh regression passed, and the exact inverse reproduced the reviewed pre-fix source.
That result closes the raw-journal finding; the author's 28-test run remains separate evidence.
The distinct high-effort wiring review passed the four frozen Task/CI files with no findings.
Its 60 scoped tests, 70 report refusal probes, and seven existing receipt refusal probes passed; it inspected the actual driver route and validate-before-report boundary without running Rust or process-crash tests.
The [wiring review](../target/verify/c2-gate-review-py95_lmn/review.txt) records the exact file hashes and scope.
Derive the final test inventory from actual discovery after that freeze; do not promote the anticipated count to a result.
The focused integration uses tooling commit `15a4222` plus the five C2 files, Task/CI registration tests, and affected documentation.
Restore the driver's committed development-codec import only in the export, preserving the primary E2 path retarget and excluding runtime, Cargo, and catalog-profile changes.
The default gate retains 11 CI stages and three Python suite receipts, with one additional publication receipt required inside the existing test stage.
The registration passed 36 Task tests, 24 verifier tests, and the full 152-test CI unit aggregate after observed failing tests for the missing task, validator, and required receipt.
The [wiring proposal](../target/verify/c2-gate-wiring-_sljty6r/proposal.json) records the 13 selected paths and their hashes; it is not a frozen C2 tree or Rust qualification receipt.
The selected export discovered 165 recovery tests, and the Task inventory guard requires that exact count.
The [C2 gate receipt](../target/verify/c2-integration-5qpi5dva/gate-results.json) binds tested tree `d711ec2c73343e81bf1dd50d2ddad94264ed04bb` and all 114 exported files.
On macOS arm64 with Rust 1.99.1, `task check lint test` passed 322 Rust tests and 63 Python tests, including the exact 165-test recovery inventory.
The same export passed all 152 CI unit tests, strict YAML validation, and Python compilation checks.
Its automatic publication gate observed 22 distinct children: 14 segmented-journal cases and eight SQLite cases, with 20 unknown killed-caller outcomes and two returned complete traces.
All 20 recovery continuations passed: 12 reached external prefix two and eight reached prefix three.
The fresh report SHA-256 is `dc2a4d45349c48d09efe28c92a41486db5b400b7a3b37655abb119bc400f1614`.
The copied controlled native artifact passed verification; its macOS install name retains the original immutable local prefix, so this is not relocation evidence.
No runtime source, Cargo dependency, catalog profile, or snapshot-anchor spec change belongs to this integration.
Hosted regression on both architectures remains separate from this local C2 result.

## Publication and tooling boundary

The [public repository](https://github.com/ydixken/rpgmigrate) published the source milestone at `55ac7d4a3907be09d951f9f356975c29133afb8d` and C1 at `96f1d2d6ff1edf972688440ef65c8adcac0b7bcc`.
The source commit's exact tested tree is `21a0f7aa7ac4b6146fa501b38296fe318ca50964`.
The source gate includes 244 Rust, 24 CLI, 20 native, and 19 harness tests, a fresh engine39 result, and explicitly retained prior COPY40 evidence.
The [source handoff](../target/verify/source-only-0x48g1ar/handoff.json) preserves the actual execution order, commands, hashes, and input-equivalence proof.
Local and remote main point to that verified commit, and GitHub main is the default branch.
Implementation stays on feat/bootstrap, whose checkout and upstream remain intact.
The one-time main creation does not authorize later direct implementation pushes to main.
The publisher verified the C1 feature push by GitHub API and `ls-remote` without changing main.
GitHub runs [42312950830](https://github.com/ydixken/rpgmigrate/actions/runs/42312950830) and [42311951675](https://github.com/ydixken/rpgmigrate/actions/runs/42311951675) completed successfully at source commit `55ac7d4`.
The C1 run [42314615234](https://github.com/ydixken/rpgmigrate/actions/runs/42314615234) was in progress at publication handoff; no C1 Linux result is inferred from the local gate.

- [x] Replace repository Make orchestration and active commands with one root Taskfile, preserving the 21 task names, default check, and help.
- [x] Use one standard-library bootstrap helper and canonical Task 3.54.0 pins across local, CI, and container execution.
- [x] Complete hosted native amd64 and arm64 CI gates against the same exported runtime image artifacts.
- [x] Build the current-CLI runtime image and pass its 23 checks on the local native arm64 fixture.
- [x] Configure version-tag-only publication to ghcr.io/ydixken/rpgmigrate after exact-artifact gates, without creating a tag or release.

Preserve archived command/evidence bytes and the native dependency's upstream GNU Make build.
Routine future implementation and tooling workers use high effort, while recovery, durability, concurrency, and critical reviews use max effort.
The immutable C1 export released existing CI, harness-adapter, and codec paths without changing its tested source.
The tooling export uses C1 as its base and includes only reviewed Task, CI, image, harness-adapter, and root-documentation changes.
It excludes canonical codec, decoder, catalog-loader, test-only PG helper, DML planner, and C2 changes.
The tooling snapshot has 26 tasks, including the original 21 names, default check, help, Linux CI, image build, and image smoke.
Its 33 scoped tests and both environment/CLI override and command-prefix corrections passed independent re-review.
The CI offline inventory derives its exact nonempty suite counts from the source, including the updated Task tests.
The 71-case aggregate omitted the image build, runtime, and smoke unit modules.
The CI owner added those three modules to the existing required inventory and verified missing-module refusal.
The final aggregate passed 146 tests with one footer: 21 verifier, 17 publisher, 33 Task, two launcher, seven image-build, 27 runtime, and 39 smoke checks.
The final symlink-refusal test correction establishes every prerequisite and asserts the specific refusal reason; its two-line inverse diff and targeted test passed independent verification.
Live image checks do not replace these negative unit regressions; the runner retains one result footer and 11 stages.
Runtime-image, smoke, and CI code have dedicated independent reviews before root integration and live artifact checks.
Offline checks alone do not qualify the runtime image or hosted CI.
Root integration removes Makefile and migrates active guides to Task; archived command evidence and PostgreSQL's own GNU Make build remain unchanged.
The shared bootstrap and the complete 26-task inventory ran locally.
The runtime reviewer identified artifact-validation gaps covering setuid permissions, protected roots, layer overlay types, actual ELF architecture, and strict schema value types.
The corrected predicates passed independent source inspection and all 31 ordinary build/runtime regression tests.
The [review receipt](../target/verify/runtime-predicate-review-g8v14rgx/review.json) records that metadata/header-only method and its limits.
The original probe was blocked by automatic screening and was not retried; no result is claimed for it.
The CI publisher fixture now supplies its actual ELF architecture, and the independent 71-case CI aggregate passed after that two-line test-only adaptation.
The documentation reviewer closed both remaining active Make references by verifying their exact two-line replacements.
The smoke owner corrected final receipt status and atomic publication with 29 passing tests.
Independent smoke review then found a Podman network-name and inspection-field mismatch that could leave an unrecorded owned resource.
The focused network fix passed independent re-review of the two original Podman cases, a Docker control, and nine network regressions.
All component review blockers closed before the focused tooling integration.
Actual image checks then found and fixed the OpenSSL default CA path, a tool-name predicate that also matched directories, copied Rust-license permissions, and mismatched package-inventory field formats.
The fixes retain HTTPS and package-signature verification, strict artifact permissions, and exact architecture-qualified package versions.

The [tooling handoff](../target/verify/tooling-r4-5ieq15dn/handoff.json) records the frozen packaging tree `4f0ed88d13885a33511bd6630fc3f7a88dd74710`, source hashes, commands, receipts, and limits.
Its fresh static suite passed 146 tests, and its native arm64 image passed all 23 runtime checks against PostgreSQL 17.11/170011.
The checks include COPY contents, persistent state, native-library identity and refusal controls, SIGINT/SIGTERM cancellation, and owned-resource cleanup.
The earlier exported tree `0d1db94f20d51ae4f35e7b787d7792f7554dd1b8` passed macOS Task check/lint/test with 298 Rust and 63 Python tests.
Its Linux arm64 report passed all 11 stages, including 142 CI unit checks, 298 Rust and 63 Python tests, engine39, and COPY40.
Those results retain their original-tree attribution; every Rust, Cargo, Task, harness, and native-build input is byte-identical in the packaging export.
The six changed packaging/test files received fresh static, image, and smoke checks.
The local Skopeo 1.22.2 data-copy check preserved a two-platform fixture index and all seven blobs without running fixture binaries or contacting a registry.
This is not native amd64 qualification or a publication result.
Hosted [run 42320992263](https://github.com/ydixken/rpgmigrate/actions/runs/42320992263) passed native arm64 and amd64 verification for tooling commit `15a42229de6237c5860406d0f3bc39b0758dad0f`.
Each architecture passed all 11 source stages with 146 CI unit, 298 Rust, 63 Python, engine39, and COPY40 checks, followed by exact-artifact image build, smoke, and receipt binding.
The downloaded bounded verification artifacts match their GitHub SHA-256 values and the same source hash on both architectures.
Native image transfer and publication were skipped for this feature-branch push, and no image tag or release was created.

## Canonical codec and decoder delivery

- [x] Release canonical record-codec promotion and pure test_decoding work after the C1 export.
- [x] Independently review the canonical codec, event payload encoding, and pure decoder within the recorded scopes.
- [ ] Complete the additional max-effort decoder ordering and failure-state source review.
- [ ] Complete C2 memory accounting before decoding, including typed-value allocation overhead and bounded relation cardinality; encoded-byte limits alone are not RSS limits.
- [x] Freeze the source catalog-loader API and exact live-test inventory with the harness owner.
- [ ] Register the nine catalog-harness offline checks and six exact live Catalog cases in default E2 gates before merging that slice; the separate profile alone is insufficient.
- [ ] Guard version-specific catalog queries and qualify PostgreSQL 9.6–11 before claiming those source versions; reused `attgenerated` and related queries do not support them yet.
- [ ] Integrate a bounded receiver and replay path before claiming clone/follow behavior.

The codec author reports 25 event-payload tests, 31 format tests, and 141 recovery tests with no failures or ignored cases.
The promotion retains existing record bytes and canonical identity types; it moves the exact approved CRC dependency to the runtime graph without a lockfile change.
These results are scoped author evidence, not a broad lint or runtime CDC gate.
Staging dead-code diagnostics remain visible until consumers are integrated.
Independent codec review passed without a ranked defect.
Independent decoder review passed all 35 decoder/parser and 25 event-codec tests, plus formatting and fixture provenance checks.
That decoder child ran at high effort, not max; the additional max-effort source review covers ordering and failure-state behavior without repeating unchanged tests.
Its evidence is [the decoder review receipt](../target/e2-decoder-review-01a1b34f/verification.json), SHA-256 `c88c9be72d3ad63078271101322c01e93a85cd07fcb199707d738529f015f6e4`.
The recorded strict warning gate failed with 77 staging diagnostics and remains open until real consumers are wired and verified.
The codec review identified typed-value allocation up to roughly 32 times its encoded tags as a C2 accounting requirement, not a satisfied process-memory bound.
The decoder uses one last relation key per transaction, with required relation context emitted on a key change.
The test-only PG frame helper's visibility and re-export passed an exact-diff review; its body and production ABI are unchanged.
Catalog ownership and live inventory must be coordinated before adding ignored cases to the global engine profile.
The six-case Catalog profile uses a fresh owned database with separate ordinary-source and administrator connections; the administrator is limited to the custom-opclass refusal fixture.
The author observed six expected loader-stub failures, then six passing cases with zero ignored tests on PostgreSQL 14.24.
The harness owner independently accepted the saved GREEN receipt and rejected the saved RED receipt without another database run.
Independent catalog-harness review passed its nine new checks and 19 existing harness checks, with syntax, help, source identity, and unchanged Engine39/COPY40 fault footprints verified.
Independent catalog review passed source SHA-256 `d9ca3aae478c57a1e8739899580dbd9d65967600137e95425c689a627f62e7fe`, its 13 unit tests, and the attributed six-case live evidence.
The reviewer verified that the extracted E1 SQL and helper behavior were unchanged.
The reused version-specific catalog queries, including unguarded `attgenerated`, leave PostgreSQL 9.6–11 unsupported until version qualification.
This is a broader inherited compatibility gap, not a compatibility regression introduced by the loader extraction.
The live contract is qualified only on the recorded PostgreSQL 14.24 fixture, and the E2 broad gate remains open.
The three pre-catalog harness files were captured as Git blobs before the primary files changed, so the tooling export retains Engine39 and COPY40.
Catalog default-gate registration belongs to the later E2 integration and must not enter the tooling snapshot.
The catalog producer released the sole database-test window after its RED/GREEN runs.
The pure E3 DML planner owns derived SQL, parameter OIDs, index maps, relation keys, and operation/TOAST shape; binding borrows row values.
Its approved scope is a new private plan module and unit tests, plus one private module declaration.
Both-endpoint static preflight remains a caller requirement, DEFAULT primary keys are the initial identity profile, and binary values and TRUNCATE fail explicitly until their execution contracts are proved.
This planner work uses an isolated Cargo target and stays outside the tooling snapshot, without a live-database or runtime-apply claim.
The planner author reports 20 failing stub tests followed by 20 passing unit tests with none ignored.
Its scoped strict Clippy run still failed with 92 reported dead-code diagnostics across staged E2/E3 code; formatting and whitespace checks passed.
Independent planner review passed the exact source and 20 focused tests without a ranked defect.
Real consumer wiring, transaction poisoning for local plan/bind failures, and a clean integrated warning gate remain required.
Serial-apply integration depends on the budgeted event-decoding and target transaction/preflight interface proposals; their separate owners must use isolated Cargo targets after implementation release.
The next clone/follow bootstrap also requires a separate logged COPY snapshot-anchor contract.
The native COPY receipt is not an atomic logged target anchor, and a barrier over an empty checkpoint proves zero CDC transactions rather than COPY coverage.
Do not substitute a synthetic transaction for that missing contract.

## Retained first-slice limits

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

The documentation bootstrap recorded checks across eight owned Markdown files.
This snapshot's seven-file review passed 37 local links, fenced blocks, trailing whitespace, final newlines, and the em-dash prohibition.
A supplemental strict STE scan reports prose-style findings, so this snapshot makes no full STE-compliance claim.
These checks do not establish runtime correctness.
The integration worker takes ownership of running updates to this file after the documentation handoff.
E0/E1 signatures stay frozen unless the leader coordinates a change.
Review E2-E4 interfaces before their workers start.
Expand the E5-E7 and operator/release feature maps before each later milestone.
Full CLI/operator/release scope remains unfinished after E1.
