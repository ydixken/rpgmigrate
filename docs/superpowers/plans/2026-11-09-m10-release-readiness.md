# M10 Release Readiness Implementation Plan

Root approved this implementation plan on 2026-11-09 within the standing execution scope through M10.
The approved private source is `/Users/ydixken/.codex/tmp/rpgmigrate-m10-plan-20261109/m10-release-readiness-plan.md`, SHA-256 `dc4ce5485b518f6b069a36ef7f93006dac50a9f9642eb00b877294dee0fe30f4`.
This repository copy preserves the approved protocol/design and historical planning assertions while applying documentation style.
**Execution note:** Existing approved milestone scope authorizes owned M7 UP/load/legacy/native tests and ordinary fresh owned fault fixtures; source freeze, guard checks, exact ownership, resource admission, and required evidence remain prerequisites rather than a new blanket user-permission gate.
Benchmark execution stays on HOLD for the final actual artifact/argv packet review, and shared-VM poweroff, original retained resources, main pushes/merges, tags, releases, and GHCR publication remain outside this authorization.
This note supersedes historical planning-only or separate-approval wording below where it would request approval again for already approved owned work; it claims no execution result or milestone closure.

> [!IMPORTANT]
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task by task.
> The leader assigns disjoint owners and independent milestone reviewers.
> This private plan authorizes no installation, build, test, VM operation, CI dispatch, push, benchmark, or publication.

**Goal:** Complete current operator documentation, bounded parser/state mutation testing, real FFI sanitizer checks, independent reviews, and the final migration matrix on retained exact production artifacts.

**Architecture:** We extend the existing single-package tests, Task targets, Python standard-library tooling, production-image smoke, and feature CI.
We qualify one immutable platform image per supported architecture and run the existing operator workflow against those retained bytes.
We preserve the existing native ownership and durability authority instead of adding a release service or another test framework.

**Tech Stack:** Rust from `Cargo.toml`, one separately pinned sanitizer nightly, stock PGDG libpq, PostgreSQL 16/18, existing Task/Podman/GitHub Actions, Python standard library, and the separate Go operator repository.

**Spec:** `docs/roadmap.md` M10 and execution amendments, `docs/superpowers/specs/2026-11-06-rpgmigrate-design.md` verification/platform sections, `docs/operator-compatibility.md`, and `tasks/lessons.md`.

## Global constraints

- Native production targets are Linux amd64 and arm64.
- Native pairs are exactly 16:16, 18:18, and 16:18.
- macOS evidence is development evidence, not Linux/PVC/power-loss qualification.
- Build `rpgmigrate` in one Cargo package and retain `publish = false` until the project license and publication decisions permit a change.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources.
- Keep credentials and row contents out of diagnostics, artifacts, and version control.
- Keep all bookkeeping exclusions tied to exact owned objects.
- Do not rebuild a candidate between qualification and later promotion.
- Feature CI pushes and CI artifacts have standing approval.
- Main merge/push, tags, releases, GHCR publication, and production deployment remain outside authorization.
- M9 benchmarks remain HOLD until their actual artifact/argv packet is approved.
- No shared-VM power cut or retained-data mutation follows from M10.
- Run independent reviews and the applicable Go/full suites once at milestone close for the exact changed inputs.
- Preserve historical receipts with their original scope and format.

## Planning provenance

Engine reads used `feat/native-engine` at `60ba21953a1e4af7899ca7ea3a7919fcc8cec756`.
Operator reads used `/Users/ydixken/development/pgcopydb-operator-m4-closing-m6-composed-20261109` at `4ecbd69cc9e56e4710096b6f0475fc7d2a62fe8b`.
The original operator checkout and its dirty todo/.agents files were not inputs.
M8 private plan SHA-256 is `0af7deed08cc6f9951820ac91dde0ae4cf968e8932104f8f32f7ee7ca1073f81`.
M8 reserves native format 9 / target metadata 7 and separate `ClonePlan.copy_tables` execution entries.
M9 plan `/Users/ydixken/.codex/tmp/rpgmigrate-m9-plan-20261109/m9-durability-performance-plan.md` SHA-256 is `11064ab09e50753f956a8a2f8be617e9e07a05988a91dd49f8f09511db43fd4e`.
M9 proposes native format 10, unchanged target metadata 7, and capability-negotiated stored durability settings.
Read the exact approved successor handoffs before implementation; this plan does not freeze speculative M8/M9 fields into M10 code.

## Review focus

1. Sanitizer compilation without actual libpq calls cannot qualify `ffi.rs` ownership boundaries.
Task 3 requires existing live SQL/COPY/prepared/pipeline/replication tests and actual execution receipts.
2. Development-image/debug-binary tests cannot qualify the production runner image.
Tasks 4 and 7 require retained production platform images and forbid rebuilding inside the matrix.
3. Missing tests, empty corpora, empty inventories, missed injections, or missing artifacts cannot read as success.
Tasks 2–4 and 7 include refusal controls for each omission.
4. A process kill does not establish target power-loss or synchronous-standby durability.
Tasks 1, 6, and 7 preserve the distinct M9 failure/evidence matrix.
5. Capability names and docs can widen support before the corresponding workflow passes.
Tasks 1 and 4 bind documentation/smoke expectations to the approved qualified capability handoff, not observed output alone.

## Root-approved minimum choices

Use the existing deterministic seeded mutation pattern for parser/state fuzz extensions.
Call it bounded mutation testing and make no coverage-guided or exhaustive-search claim.
The existing pgoutput fuzz test in `src/cdc/pgoutput/fuzz.rs` already checks retained PG16/18 corpus truncations and mutations with more than 500,000 assertions.
Extend that style rather than add cargo-fuzz, libfuzzer-sys, a second harness package, or a framework.
The official [Rust Fuzz Book](https://rust-fuzz.github.io/book/cargo-fuzz.html) describes cargo-fuzz/libFuzzer; that is the larger deferred alternative.

Use native Linux AddressSanitizer for the actual Rust FFI wrapper and boundary tests.
Keep stock libpq uninstrumented and state that its internal memory operations have reduced coverage.
The [Rust sanitizer documentation](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html) supports both selected Linux targets and describes partially instrumented dependencies.
The [Miri project](https://github.com/rust-lang/miri) documents limitations for arbitrary FFI and networking.
Do not claim that a pure-Rust Miri test exercises live libpq.
Do not add experimental Miri native-library support as a substitute for this gate.

Extend the current smoke helper with an exact qualified capability expectation and executable hash.
Retain per-platform OCI archives and all distinct artifact digests for the final matrix.
Root approved CI artifacts, not registry or release publication.

Vendor the existing STE structural linter only after validating its exact source behavior, provenance, and MIT license.
Use no PyPI package or new docs framework.
Installed source: `/Users/ydixken/.agents/skills/asd-ste100/scripts/ste-lint.py`.
Source SHA-256: `155bf7bb73caa70735719ef0390cc39568fcd007509b28000a208839df29f3a3`.
MIT license source: `/Users/ydixken/.agents/skills/asd-ste100/LICENSE`.
License SHA-256: `d3c674e8592076c9021a59b55177adbcba6fd7e1710082becb922522b22ec672`.
Keep both exact bytes and license attribution when vendoring.

## Existing gate inventory and limitations

- `task check lint test` executes locked Cargo checks, rustfmt, strict Clippy, Rust/CLI tests, and the exact portable Python inventory.
- `task test-db` selects retained live profiles through `tools/test-db.py` and existing independent receipts.
- `task test-transport`, `task test-prepared`, and `task test-replication` expose the live FFI paths needed by sanitizer work.
- `task test-e2e-clone`, `task test-e2e-follow`, and `task test-e2e-cutover` run the three declared engine pairs.
- Those container engine targets currently build a development image and debug binary in `tools/e2e/clone.py::outer`.
  They remain engine regression evidence; they are not the final production-artifact gate.
- `task image:build` creates the production runner; `task image:smoke` inspects the selected image once and runs its immutable image ID.
- Current image smoke binds source-input hashes, package/library identities, tools, notices, nonroot/read-only behavior, and Python/libpq metadata.
  It does not yet record the engine executable hash or retain the reusable image archive.
- Current smoke requires `version.capabilities == []`.
  It must change in coordination with producer capability activation before a feature image build.
  The M6 discovery/preflight planner received this dependency read-only during this assignment.
- CI has `checks`, `dependencies`, `docs`, `engine-e2e`, and `runner-image` jobs in `.github/workflows/ci.yml`.
- Existing dependency CI pins cargo-deny 0.20.2, audits both Linux targets and all dependency kinds, and restricts the private-license exemption to the unpublished root only.
- Existing docs CI enforces tracked Markdown inventory, no em dashes, and a SHA-pinned markdownlint action.
  It does not yet enforce STE hard rules, internal-link/command drift, or new user-guide completeness.
- Existing native runner build/smoke jobs run on `ubuntu-24.04` and `ubuntu-24.04-arm`.
  ARM and AMD results stay separate.
- Operator feature CI currently runs unit/envtest, cluster-free E2E, and PostgreSQL 18 coverage checks.
  The exact M7 kind successor is pending integration and must supply the supported native Task target interface.
- Operator existing tag/release/image-publication workflows are not M10 execution targets.

## File ownership

Assign one owner per set and transfer shared files explicitly.

- Docs/STE owner, medium effort: `README.md`, new `docs/getting-started.md`, `docs/operator-guide.md`, `docs/troubleshooting.md`, current user docs, new `tools/docs/{check.py,test_check.py,ste-lint.py,LICENSE-ste}`, and docs provenance text.
  `docs/durability.md` transfers from its M9 owner only after the M9 handoff.
- Parser/state mutation owner, high effort: `src/cli.rs`, new `src/cli/fuzz.rs`, `src/state.rs`, new `src/state/fuzz.rs`, existing `src/cdc/pgoutput/fuzz.rs`, and new synthetic seeds under `tests/fixtures/fuzz/`.
  Do not edit M8/M9 active state/config files before release of ownership.
- FFI sanitizer owner, high effort: new `tools/ci/{ffi_asan.py,test_ffi_asan.py}`, a small pinned-toolchain manifest, and narrowly necessary FFI test coverage in `src/pg/ffi.rs` or existing `src/pg/*_live_tests.rs`.
  Production FFI fixes remain separate assigned bugfixes if discovered.
- Artifact owner, high effort: `tools/image/{smoke.py,test_smoke.py}`, new `tools/image/{candidate.py,test_candidate.py}`, and artifact receipt schema/fixtures.
- CI/Task integration owner, medium effort: `.github/workflows/ci.yml`, `Taskfile.yml`, `tests/test_taskfile.py`, `tools/ci/{verify.py,test_verify.py,test_workflow.py}`, `.gitignore` only for new generated outputs, and no publication workflow.
- Separate operator verification owner: the M6/M7/M8/M9 successor native helper, native test profile, kind image admission/loading, and operation docs in the operator feature checkout.
  Read its current AGENTS first and preserve its existing mirror authorization and legacy paths.
- Security/license verification owner, high effort: `Cargo.toml`, `Cargo.lock`, `deny.toml`, `NOTICE`, Dockerfile/source/package inventories and retained report only.
  No dependency or license change is inferred from this read-only ownership.
- Independent reviewers, xhigh where durability/recovery applies: final engine durability, final recovery/ownership, and separate operator diff.
  Each reviewer receives a bounded surface and reports to the leader.
- Milestone verification owner: final lint/test/CI/runtime receipts, artifact identities, and closure ledger.
  The verifier must not review their own source changes.

## Dependency graph and safe preparation

Tasks 1–4 may prepare disjoint docs/tooling/test scaffolds before M8/M9 close, after actual file owners release them.
Task 2 may prepare generic corpus logic against current types but must rebase seeds and run against final formats after M9.
Task 3 may prepare the sanitizer wrapper and validate its toolchain contract without claiming final-source coverage.
Task 4 may prepare artifact retention and refuse mismatched input fixtures before a final candidate exists.
Task 5 integrates those tools into CI only after interfaces and owner handoffs freeze.

Final documentation support claims, final state fuzz receipts, final sanitizer execution, independent reviews, final candidate build, and final production matrix are blocked on exact M6–M9 handoffs.
M9's benchmark approval and measured cadence remain prerequisites if replay-safe is part of the final supported profile.
Do not select a cadence or present replay-safe as qualified because M10 tooling is ready.
Conditional relaxed/binary pgoutput may remain unavailable if M9's conditional gates do not justify them.
Missing physical power-loss/PVC/standby evidence remains an explicit support-claim gate, not something M10 can replace with process kills.

## Task 1: Finish user documentation and deterministic drift checks

**Interfaces:** Proposed `tools/docs/check.py` consumes explicit repository root, an actual binary path for safe help/version discovery, and the nonempty JSON output of the pinned Task's `--list-all --json`.
It never executes commands extracted from Markdown.
It returns nonzero for missing/invalid inputs or drift.

- [ ] Write failing checker fixtures for a missing user guide, broken local link/anchor, missing task, stale help flag, malformed Task JSON, empty inventory, em dash, and STE hard violation.
- [ ] Keep README as the required index: title, two-sentence purpose, `[TOC]`, numbered bold-term features, commented directory tree, shortest first successful run, command table, then one-line guide links.
- [ ] Move substantive setup into `docs/getting-started.md`.
  Separate no-database checks from a first migration on explicitly confirmed fresh owned source/target fixtures.
  Document explicit connection/passfile/TLS handling without inline credentials or fake endpoints that imply production access.
  Show only commands implemented and verified in the final profile.
- [ ] Write `docs/operator-guide.md` for explicit immutable engine selection, fresh UID/PVC/workdir, qualified capability handshake, version pairs, operator defaults, writer fence/E persistence, cutover/retry after worker exit, independent verification, owned cleanup, and staged refusals.
  Link the operator's maintained operational procedure rather than duplicate mutable cluster administration.
- [ ] Reconcile `docs/durability.md` after M9: durable/replay-safe contract, effective stored settings, process kill versus host/target crash, synchronous/async standby failover, unknown COMMIT, source retention/gaps, restart refusal, final sequence/LO synchronization, and actual qualification limits.
  Keep any relaxed/binary-pgoutput unavailable state explicit.
  Preserve the asynchronous fork versus native COPY crash-contract distinction.
- [ ] Write `docs/troubleshooting.md` around nonzero categories, private diagnostics, safe identity inspection, lost snapshot/pre-anchor restart, post-anchor recovery, missing WAL/resources, uncertain utility outcomes, target-fence conflict, helper/PVC/TLS refusal, and format incompatibility.
  Never prescribe legacy-state adoption, broad cleanup, accept-loss success, or deleting authority files.
- [ ] Update `docs/{testing,runner-image,operator-compatibility,native-protocol,native-cutover-handoff}.md` where final behavior differs.
  Keep dated design/history evidence separate from maintained support statements.
- [ ] Vendor the exact MIT STE script/license only after hash and behavior review.
  Retain source path/provenance in a short tools notice.
  Execute its self-test during implementation and test that each required hard-rule fixture fails.
- [ ] Require zero STE hard violations in README and maintained user guides.
  Run existing markdownlint/no-em-dash checks on every tracked Markdown file as before.
  For archived specs/plans, preserve historical meaning rather than bulk rewrite evidence for new prose style.
  The docs inventory must explicitly enumerate user guides, reject a missing expected guide, and never pass on zero selected files.
- [ ] Validate Task names with pinned Task JSON and help flags with the actual safe help output.
  Compare protocol field/capability references against reviewed canonical native fixtures and actual discovery for the same source, without equating fixture presence with runtime qualification.
  Do not invent a second support-status database.
- [ ] Run focused checker tests and later the final docs gate after M9 handoff.

## Task 2: Extend parser and state mutation testing without dependencies

**Interfaces:** `src/cli/fuzz.rs` remains a `#[cfg(test)]` child of `cli.rs` and exercises existing `contract_oracle`/parser entry points without invoking migration dispatch.
`src/state/fuzz.rs` remains a test child of `state.rs` and exercises the production manifest decoder/validation path.
No new public executable or Cargo package is added.

- [ ] Add named failing regressions first for any discovered panic, unsafe acceptance, duplicate field, wrong format, or stale profile bug.
- [ ] Build a synthetic, credential-free seed corpus from canonical CLI fixture cases, PG16/18 pgoutput packets, and final valid native-format-10/metadata-7 state fixtures.
  Preserve separate old-format refusal seeds for M8 9 and older 8.
  Do not commit retained real endpoint/state/row data as fuzz seeds.
- [ ] Use deterministic seed `0x9e3779b97f4a7c15` and explicit finite case inventories.
  Include every truncation of retained compact seeds, one-byte insert/delete/flip, duplicate and escaped JSON keys, unknown fields, integer/byte-size boundaries, NUL/non-UTF8 argv bytes, deeply nested JSON, and bounded oversized inputs.
  Keep the current PG16/18 pgoutput corpus tests intact.
- [ ] CLI mutations exercise `command().try_get_matches_from` and the existing pure contract/config path with a synthetic explicit Environment.
  Assert no panic, deterministic outcome, bounded diagnostic class, and no mutation/connection dispatch.
  Do not call `lib::run` or execute an accepted Invocation.
- [ ] State mutations exercise format-before-body refusal, `deny_unknown_fields`, required-nullable fields, duplicate-field rejection, integer bounds, canonical identities, complete part keysets, LO obligations, and persisted durability settings from the final schema.
  Use the actual producer decoder rather than a separately reimplemented parser.
- [ ] If reuse requires extraction, introduce only private `decode_manifest_bytes(bytes: &[u8]) -> Result<Manifest>` for the existing format/body/provenance decode.
  Keep file-size enforcement, held-file/directory identity checks, permission checks, and directory revalidation in `read_manifest`.
  Test old and refactored paths against the same existing refusals before moving code.
- [ ] Add state-transition mutations for lost snapshot, generation mismatch, unknown COMMIT marker absence, missing/changed slot/publication, cleanup after incomplete proof, and final completion without required LO/part obligations.
  Invalid input must never mint completion, feedback, cleanup, or restart authority.
- [ ] Bound normal per-seed mutations to 64 KiB, with explicit one-MiB boundary and one-byte-over-bound state cases.
  Use production depth/size limits and never disable serde's recursion guard to force acceptance.
  Emit seed, case index, category, and counts on failure, without raw payloads.
- [ ] Add a standalone named test group for bounded mutation runs and wire it into Task/CI with exact nonzero test discovery.
  A panic must fail; do not swallow it with catch_unwind.
  A missing corpus or zero generated cases must fail.
- [ ] Run the final group against the exact M9 handoff and retain source/seed/corpus hashes and counts.
  Describe evidence as bounded mutation testing, not coverage-guided fuzzing or exhaustive safety proof.

## Task 3: Run actual Rust FFI boundaries under native Linux ASAN

**Interfaces:** Proposed `tools/ci/ffi_asan.py` takes a reviewed pinned-toolchain manifest and an explicit native Rust target, builds argv lists, invokes existing harnesses, and publishes a nonempty private/sanitized receipt.
It never installs tools itself.

- [ ] Freeze one exact dated nightly before installation in a small tracked manifest under `tools/ci/`.
  Retain rustc/cargo version, compiler commit, official manifest/checksum identity, native target, and required `rust-src`/LLVM symbolizer components.
  Verify compatibility with manifest MSRV 1.99.1, edition 2024, locked dependencies, and all sanitizer options.
  Validate toolchain license/security/provenance before installing it in CI.
  Do not use moving `nightly`, `RUSTC_BOOTSTRAP`, or ignore-rust-version to bypass compatibility.
- [ ] Stop toolchain preparation if the exact dated release/components cannot be verified.
  This planning worker could not access official static nightly manifests through web and has no rustup in PATH, so no nightly date is falsely asserted here.
  The owning implementation task must return a concrete verified pin before downstream CI integration starts.
- [ ] Add negative wrapper tests for absent/moving toolchain, wrong architecture, empty test selection, wrong binary, missing libpq, ignored live tests, failed test, timeout, missing receipt, and sanitizer diagnostics despite exit zero.
- [ ] Set child-only `RUSTFLAGS=-Zsanitizer=address`, explicit native `--target`, `-Zbuild-std`, and a disjoint `CARGO_TARGET_DIR`.
  Set the appropriate emitted `RPGMIGRATE_BIN` when invoking a harness that launches the CLI.
  Use `ASAN_OPTIONS=detect_leaks=1:abort_on_error=1:detect_stack_use_after_return=1` and a verified symbolizer where supported by the frozen nightly.
  Do not set `CFLAGS`, HOST_CFLAGS, or TARGET_CFLAGS: `build.rs` rejects these bundled-SQLite overrides.
- [ ] Execute a small intentionally invalid-memory private child as a sanitizer-control fixture.
  Require its nonzero termination and expected ASAN failure category before accepting a passing suite.
  A no-op command or missing instrumentation must fail this control.
  Keep the control outside production binaries and do not suppress findings globally.
- [ ] Compile and run nonempty `pg::ffi::tests` with instrumentation, including callback/result lifetime, native binary cells, utility option allocation, quoting, closed connection, COPY count, and drop paths.
- [ ] Through existing `tools/test-db.py`, run transport, prepared/pipeline, and replication live profiles on actual PostgreSQL 16 and 18.
  Require real connect/query/error/drain/COPY/abort, prepared NULL/empty/binary, pipeline callback/error/sync/drop, CopyBoth backpressure/feedback/terminal error, deadline, and cancellation execution.
  Preserve the harness's exact positive and negative receipt requirements.
- [ ] Run on native `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`.
  Hosted AMD and hosted/native ARM receipts are distinct; QEMU/emulation or macOS cannot close a native Linux cell.
  Record actual target-machine architecture, executable hash, selected libpq path/hash/version/package, compiler flags, test counts, profile receipts, sanitizer-control result, and clean process reaping.
- [ ] Label coverage precisely: instrumented Rust wrapper/standard-library boundary behavior with uninstrumented stock libpq and bundled native dependencies.
  Do not claim instrumentation of libpq internals, the PostgreSQL server, all unsafe operations, or all schedules.
- [ ] Preserve sanitizer failures for the leader and assign any source fix to a new bounded owner task before rerunning affected gates.

The Rust documentation recommends `--target` to keep instrumentation flags off build scripts/procedural macros and recommends instrumented standard-library builds.
Use those constraints, not a generic global environment that changes unrelated builds.

## Task 4: Retain and verify the exact production candidate artifacts

**Interfaces:** Extend `tools/image/smoke.py::probe(image, runtime)` with a caller-supplied reviewed qualified capability expectation and executable SHA-256 observation.
Proposed `tools/image/candidate.py` exports/imports and verifies an explicit platform image by immutable identity, with a strict JSON receipt.
Use existing Podman plus Python `json`, `hashlib`, `tarfile`, `pathlib`, and `subprocess`; add no artifact service.

- [ ] Add smoke negatives for missing/unexpected/duplicate capabilities, observed-self-expectation, absent executable hash, source mismatch, wrong architecture, and wrong runtime file identity.
- [ ] Coordinate the capability expectation with the current M6 producer owner and M8/M9 capability gates before image build.
  Use the final reviewed exact capability set, including any qualified durability status extension, rather than accepting whatever discovery returns.
  Keep schema/version/required-capability checks intact.
- [ ] Record SHA-256 of `/usr/local/bin/rpgmigrate` from the actual immutable image alongside the existing build-input and Python/libpq hashes.
  Keep the existing nonroot/read-only mounts, package ownership, source-input comparison, tools, and notice checks.
- [ ] Build once per native architecture after source/lock/Dockerfile/notice/helper-contract inputs freeze.
  The production compiler remains the exact `Cargo.toml` version, currently 1.99.1; the sanitizer nightly is not the release compiler.
- [ ] Export the already inspected platform image using Podman OCI archive format.
  Retain archive-file SHA-256, OCI platform manifest digest, image config/engine ID, layer digests, executable hash, build-input hash, package inventory, architecture, source commit/tree hash, and smoke receipt.
  These are distinct digest domains and must remain separate fields.
- [ ] Validate bounded archive metadata by reading JSON and named blobs without unsafe path extraction.
  Reject ambiguous platforms/manifests, missing referenced blobs, unexpected identity substitution, empty archive, changed bytes, wrong executable, or mismatching smoke receipt.
- [ ] Upload only the intended public-source image archive and sanitized manifest/receipt through pinned GitHub artifact actions.
  Never upload passfiles, environment files, kubeconfigs, runtime workdirs, raw private diagnostics, or caches.
  Pin action commits and retain artifact IDs/digests separately from the OCI/archive hashes.
- [ ] In a clean native consumer, download/load that archive, revalidate original bytes and loaded image identity, and smoke the same image without rebuilding.
  A retag may provide a local kind transport name; it grants no registry publication and cannot replace identity checks.
  Follow M6's exact ImageRef/PlatformImageID comparison contract rather than deriving expectations from the observed Pod.
- [ ] Add omission/substitution controls for archive, blob, platform, source tree, executable, package/libpq identity, and build-input manifest.
  A successful rebuild of similar source is not an accepted recovery path for a lost artifact.
- [ ] Preserve both platform archives through final qualification and handoff.
  If a dependency/security fix changes bytes, create a new candidate identity and rerun affected exact-artifact gates.

[Podman save](https://docs.podman.io/en/latest/markdown/podman-save.1.html) preserves image layers and metadata for loading.
[GitHub workflow artifacts](https://docs.github.com/en/actions/tutorials/store-and-share-data) can transfer retained build outputs between jobs without registry publication.
Neither mechanism itself proves that the final Pod ran the required image.

## Task 5: Integrate preparation gates into read-only feature CI

**Owner:** CI/Task integration owner after Tasks 1–4 interface handoff.

- [ ] Add explicit Task targets for docs drift/STE, bounded mutation, ASAN, and candidate verification only after their helpers exist.
  Do not put proposed target names into user docs before they actually run.
- [ ] Update Task `RUN` propagation only for necessary new explicit inputs, with existing empty-value and precedence behavior preserved.
  Keep ordinary test/build environments free of sanitizer flags.
- [ ] Extend `.github/workflows/ci.yml` with bounded native sanitizer cells and artifact retention/consumer dependencies.
  Keep `contents: read`, `persist-credentials: false`, immutable actions, finite timeouts, and fail-fast false where every required matrix cell must finish.
  Do not add continue-on-error, registry login, package-write permission, tags, release creation, or container publication.
- [ ] Update `tools/ci/test_workflow.py`'s exact job inventory, required commands, pair/architecture inventories, artifact checks, and no-publication assertions.
  Add negative mutations that remove each new required gate or replace its helper with true.
- [ ] Add new Python suites to `tools/ci/verify.py` and its tests so discovery is nonempty and new helpers actually execute.
  Preserve current exact AST-based test inventory and no optimized-Python execution rule.
- [ ] Keep existing engine development-image E2E jobs as distinct regression evidence.
  Do not relabel them as production-image tests or eliminate their prior coverage to fit a job count.
- [ ] Preserve the operator's approved GitLab mirror and existing feature CI.
  Add native final-matrix test-only workflow changes only through its current M7 successor owner.
  Do not run tag/release workflows to obtain artifacts.
- [ ] Run focused workflow/Task/helper negative tests during implementation.
  Reserve broad suites for the final changed-input closure gate.

## Task 6: Dependency, license, security, and independent review closure

**Inputs:** final locked engine source, both actual production image package inventories, operator diff, M6–M9 receipts, docs, candidate lock, and exact intended support claims.

- [ ] Reuse pinned cargo-deny 0.20.2 and `cargo metadata --locked --format-version 1`.
  Verify only the unpublished root receives the private-license exemption.
  Run locked license/advisory checks for both target graphs, build dependencies, and dev dependencies.
  Retain tool/version, lockfile hash, policy hash, advisory database revision/time, exit status, and findings.
- [ ] Verify retained Cargo, Debian/PGDG, Python/libffi, and vendored STE notices/license bytes against what is actually shipped or committed.
  A successful Cargo graph audit does not cover system packages.
- [ ] Review the actual image package/source-version inventory against primary Debian security tracker and PostgreSQL/PGDG security advisories at the freeze date.
  Record applicability, fixed package provenance, unresolved findings, and unknown coverage explicitly.
  Use existing tools and primary-source review; do not build a new CVE scanner or claim zero vulnerabilities from apt updates alone.
  Relevant primary sources are [Debian security tracker](https://security-tracker.debian.org/tracker/) and [PostgreSQL security information](https://www.postgresql.org/support/security/).
- [ ] Preserve `publish = false` and the existing pending project-license decision.
  Do not invent a project license or infer public redistribution permission from CI artifact approval.
  The leader must record whether an unresolved project-license decision blocks only future publication or the requested readiness label.
- [ ] Assign one independent durability reviewer to feedback/barrier/COMMIT and final synchronization, one recovery reviewer to state/markers/LO/restart/gap/cleanup, and one separate operator-diff reviewer to capability/admission/helper/PVC/TLS/fence/verification/runtime boundaries.
  Give each exact source hashes and their bounded surface.
  This is the one milestone closing review cycle, not repeated full reviews after each task.
- [ ] Require each review to identify blockers, test gaps, and actual claim limits.
  A reviewer must not count their own implementation tests as independent approval.
  The leader reconciles results and re-delegates fixes to the owning files.
- [ ] After fixes, rerun only affected focused checks and the final required full gates for the changed exact inputs.
  Preserve old reports and bind new evidence rather than editing historical receipts.

## Task 7: Run the final operator matrix on retained exact image bytes

**Prerequisites:** M6/M7 workflow closure, M8 default-feature closure, M9 measured settings/required evidence closure, Tasks 1–6 handoffs, exact engine and operator source commits, both candidate archives/digests, final helper contract and image pin, and leader resource admission.

- [ ] Use the actual M7 successor Task targets and fresh source/RUN admission.
  The inspected central Taskfile lacks those pending kind targets, so this plan deliberately does not invent a runnable kind command.
  Do not set E2E_CONTEXT manually or bypass context/ownership confirmation.
- [ ] Run the three pairs 16:16, 18:18, and 16:18 against each production platform image whose native runtime support will be claimed.
  Full Linux amd64+arm64 qualification therefore needs six exact architecture/pair cells.
  A single architecture's passing three-pair matrix does not qualify the other architecture.
- [ ] Require actual native node/Pod architecture, expected single-platform image reference, unchanged admitted image identity/status, engine executable hash, matching client-tool versions, and exact helper source/runtime identities.
  Import/load retained archives only; no cargo build, image build, apt update, dependency fetch, or registry publication inside a cell.
- [ ] Exercise the complete M7 live-writer follow/manual+automatic cutover/verification/runner-kill/primary-failover/suspend-resume/deletion matrix with operator defaults and M8 binary/fallback/split/LO cases.
  Include final M9 durability mode and recovery cases selected by its qualified profile.
  Reuse exact predecessor case inventories instead of a new reduced smoke inventory.
- [ ] Require Complete with CutoverCompleted, unchanged persisted E, the bound durable drain proof, final sequences/LO synchronization, independent table/schema/sequence/LO/ownership/partition match, and exact-owned cleanup.
  Require observed fault injections and clean reaping of helper/worker sessions.
- [ ] Keep process kill, target crash, target failover with synchronous standby, asynchronous-standby gap refusal, and isolated power-loss evidence separate.
  Do not fault the shared VM or infer power-loss durability from a killed process.
  Missing required isolated evidence blocks that support claim and milestone closure where the approved gate requires it.
- [ ] Admit one heavy local VM cell at a time, with M7 UP/load, image builds, other DB suites, M9 benchmarks, and other heavy Cargo work excluded from that resource window.
  Existing VM allocation is 14 CPUs/32 GiB/128 GiB, but measure current free memory/disk and original-service commitments immediately before admission.
  Reuse the reviewed M7 kind node/manager caps and finite original deadline from its exact handoff.
  Do not silently invent or broaden a lease from this plan.
- [ ] Hosted runners are separate native architecture evidence and need their own measured capacity/disk admission.
  If a hosted runner cannot satisfy the M7 topology/caps, stop that cell and keep it unmet rather than lower durability/topology requirements.
  Existing [GitHub runner documentation](https://docs.github.com/en/actions/reference/runners/github-hosted-runners) supplies availability context, not evidence of the actual assigned machine.
- [ ] Preserve failed RUN directories, original services/data, volumes, backups, and evidence.
  Use only the standing exact-owned failed-kind container cleanup scope when its independent ownership/failed-state checks pass.
  Do not broaden cleanup to images/networks/volumes or repair creating/empty RUN markers.
- [ ] Stop on wrong source/image/helper identity, unavailable expected test, resource pressure, missing fault, timeout, changed E, missing proof, foreign replacement, cleanup uncertainty, or private-data exposure.
  Retain the failure and hand ownership back to the leader.
  A deadline does not renew because a helper or process restarts.
- [ ] Retain per-cell source/image/binary/helper/platform/pair/case identities, original deadline, actual runtime facts, independent oracles, terminal state, and exact cleanup receipts.
  Keep raw sensitive logs private and publish only reviewed sanitized receipts.

## Task 8: Final gate, readiness record, and promotion handoff

1. Run the final engine gate for the exact changed source.

   ```sh
   task check lint test
   ```

2. Run required live profiles and retained regression matrices, with actual PG16/18 bundles and isolated resource windows.

   ```sh
   task test-db
   task test-e2e-clone
   task test-e2e-follow
   task test-e2e-cutover
   ```

   These retained development-image checks remain distinct from Task 7's production-image matrix.
   Prior unchanged exact-input gates may retain their original scope; do not rerun broad suites merely to produce another count.

3. Require new docs/mutation/ASAN/artifact checks and dependency/security/license review on the final inputs.
   Run applicable operator lint and Go CI/full suites once at milestone close under its current repository policy.
   Confirm hosted status is terminal for the actual feature heads, not a queued run or an older source.

4. Reconcile independent reviews and all six claimed platform/pair runtime cells with the artifact lock.
   A missing cell, missing receipt, skipped required case, or changed artifact keeps readiness incomplete.

5. Write the readiness review and exact acceptance evidence into roadmap and todo together through their assigned owner.
   Record the user-facing supported workflow, remaining unavailable conditional features, storage/platform assumptions, and pending publication/license boundaries.
   Do not report a measured speedup without M9 measurements.

6. Prepare a concrete promotion handoff naming the two retained platform manifests/configs/archive hashes/binary hashes, exact engine/operator sources, complete gate receipts, dependency/security snapshot, and remaining authorization needed.
   Later authorized promotion must reuse these exact bytes and verify resulting digests.
   Do not rebuild from the same commit, re-resolve apt packages, create a tag/release, merge main, or publish GHCR as part of this task.

## Acceptance criteria

- Maintained user docs cover first run, qualified operator workflow, durability/recovery, and troubleshooting with current commands and truthful limits.
- README remains an index and all maintained user docs pass zero-hard-violation STE plus existing Markdown rules.
- Drift checks fail missing files, stale commands/links, malformed inventories, and unqualified capability claims without executing doc snippets.
- CLI, pgoutput, and final-state bounded mutation suites execute nonempty deterministic inventories and refuse invalid authority paths.
- Native Linux ASAN executes actual instrumented Rust/libpq boundary routes on both architectures, with passing instrumentation controls and precise uninstrumented-library limits.
- Production artifacts are built once, exported, retained, independently reloaded, and bound to distinct source/manifest/config/archive/binary/package identities.
- Dependency/license/security checks cover locked Rust dependencies and actual image packages with unresolved findings stated.
- Independent durability/recovery/operator reviews are resolved and applicable full/Go/hosted gates pass on the actual heads.
- All three version pairs pass on every production architecture claimed, using retained exact production images and complete predecessor case inventories.
- No benchmark, publication, production deployment, or destructive retained-resource operation occurs by inference.

## Remaining handoff decisions

The implementation owner must return one actual verified dated nightly/toolchain manifest before ASAN CI integration.
Official static nightly metadata was inaccessible to this planner, so an invented exact date is not supplied.
The leader must freeze the final M9 source/profile, qualified capability set, and its actual storage/failure qualification claims before final docs/artifact creation.
The leader must record the project-license readiness boundary while preserving the user's pending license answer and `publish = false`.
The M7 successor must supply actual native kind Task targets, resource leases, final case inventories, image-loading/admission interfaces, and cleanup ownership before Task 7 starts.
These are prerequisite handoffs, not a new brainstorming or user-approval round for already approved milestone work.

Skipped: repository edits, tests/builds, dependency/tool installation, VM/CI operations, benchmarks, commits, pushes, and publication.
Risk: final readiness cannot close from preparatory tooling or development-image evidence while M6–M9, exact toolchain pins, independent reviews, or a claimed platform's final runtime cells remain unqualified.
