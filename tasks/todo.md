# Native engine task ledger

The [roadmap](../docs/roadmap.md) defines the milestone scope and its 2026-11-08 amendments override conflicting older design text.
The [M0 implementation plan](../docs/superpowers/plans/2026-11-08-m0-preserve-reset.md) defines worker ownership and acceptance.
Keep implementation evidence separate from parser compatibility and operator runtime qualification.
M0–M5 are closed; M4 engine source and six accepted behavior documents are integrated at `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476`, with separate operator test-only commit `c262322909ac03a7101ea3bfac2c4823efa62e95`.
M6–M10 remain open; production native operator admission, kind E2E, power-loss/performance, and release qualification remain unqualified.
On 2026-11-09, the user directed us to push through M10: execute the approved remaining M6–M10 gates and do not stop at intermediate planning or checkpoint stages.
Closure still requires evidence.
Benchmark runs and VM reservations remain unapproved, main/tag/release boundaries persist.
The dated M7 amendment permits only development GHCR publication, and performance ranges are inferences without measured speedup or an ETA.

## User authorization and active ownership, 2026-11-09

The user approved both prior decisions and continuing the existing M5/M6/M7 plans.
Unused Podman images, volumes, and build cache may be pruned, and `podman-machine-default` may be grown under the approved PostgreSQL 18 persistent-preservation maintenance plan.
The leader selected 128 GiB, preserving 14 CPUs, 32 GiB memory, original service endpoints, and data; the user reported about 212 GiB free on the host.
`vm_pg18_preservation_plan` completed maintenance; the later exact failed-node cleanup is independently verified and its quiet reservation released, with subsequent M5 image results recorded below.
Preserve and test persistent PostgreSQL 18 data before stopping its original tmpfs container or restarting the VM; retained resources are not blanket cleanup targets.
The user also approved new runner dependencies including Python 3 and ctypes/stock-libpq for private opaque URI Secret authentication without argv credentials.
The [dated M6 exception](../docs/superpowers/plans/2026-11-08-m6-native-adapter.md#user-approved-runner-dependency-exception-2026-11-09) preserves public API/lifecycle/authority/CLI boundaries and mandatory validation; write the separate mechanism implementation plan before production changes.
The amended M6 plan is frozen at SHA-256 `b8039fb8fecae9e6486fa52517acd0c5ead756244a8f59442d61df3e3b997a6f` for mechanism planners.
The decision alone claimed no execution success; the final maintenance acceptance is recorded below, while dependency/TLS/PVC/image/kind/production qualification remains separate.

### Feature CI pushes and failed-node cleanup, 2026-11-09

On 2026-11-09, after approving the second exact failed-kind cleanup, the user authorized future removal of independently verified rpgmigrate-owned failed kind-node containers without repeated cleanup approval.
This standing authorization covers containers only: preserve volumes, networks, images, backups, original PostgreSQL containers, source, and RUN evidence; do not extend it to other resources or repair RUN markers.
The user authorized pushing `rpgmigrate` `feat/native-engine` and the operator native-work feature branch to `origin` for hosted CI, including M5/M10 native AMD64 checks.
This earlier authorization covered feature branches only, excluding main push/merge, tags, releases, and GHCR.
The dated M7 amendment separately authorizes development GHCR publication.
The one-off cleanup exception permits exactly three failed kind-node containers for the failed RUN, not volumes, networks, images, backups, original services, RUN markers, or a general guard bypass.
Final independent verification accepts the exact three-container cleanup; preserve all source RUN creating/empty files without mutation and use corrected fresh source/RUN.
Operator preflight exposed existing all-ref/tag GitLab mirroring and skipped tests; the earlier hold prevented inferring downstream authorization from the origin-only decision.
On 2026-11-09, the user explicitly approved the existing GitLab mirroring triggered by the operator native feature branch.
We resolved the publication P1 by authorization and keep the existing mirror enabled.
This mirror decision authorizes no repository settings changes, main edits/pushes, tags, releases, or GHCR publication.
Development GHCR approval follows the dated M7 amendment.
The CI owner restored its mirror-guard edits to baseline and committed only `ci.yml` as `ccde83912cdbc9e152f0bc51c47c49954ede8a9d`, parent `c262322909ac03a7101ea3bfac2c4823efa62e95`; normal push to `origin/test/m4-closing-m6-composed-20261109` is verified.
Early receipt: `/tmp/rpgmigrate-feature-ci-preflight-20261109/operator-ccde839-push-early-receipt.json`, SHA-256 `689243c6b62ae2549c1fce755530a3723988ae5e0cc0e1aaa1f165f71a57d149`.
The [mirror run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42633254999) passed; [operator CI](https://github.com/ydixken/pgcopydb-operator/actions/runs/42633254706) passed docs and its unit/envtest, cluster-free E2E, and PostgreSQL 18 coverage test job, failing only lint on SC2015.
The reviewed mechanical fix `5616d82f03e6a917147e55ed4a3fb436a1cc6d89` was pushed normally to the feature ref after prior tests became terminal, without cancellation.
Push/source receipt: `/tmp/rpgmigrate-feature-ci-preflight-20261109/operator-5616d82-push-early-receipt.json`, SHA-256 `2503a3d8cb6464ac90f2a52ad92a993a7dc3e77a588a3d88a7a4320534e26beb`.
The new [operator CI](https://github.com/ydixken/pgcopydb-operator/actions/runs/42636601113) is in progress and its authorized [mirror run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42636601234) passed; no corrected-CI pass is claimed.
Exact engine feature pushes and their hosted outcomes remain recorded under M5.

## Project license decision, 2026-11-10

The user selected Apache License 2.0 for rpgmigrate.
This resolves the pending project-license choice.
`publish = false` remains in force.

- [x] Add the unchanged [official Apache License 2.0 text](https://www.apache.org/licenses/LICENSE-2.0.txt) as root `LICENSE` and set Cargo SPDX metadata to `Apache-2.0`.
- [x] Record the choice in the roadmap and replace the stale pending-license lesson.
- [x] Check exact official bytes, Cargo metadata, affected Markdown, and preservation of all other tracked files.

The official license SHA-256 is `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30`.
Its Appendix example remains unfilled.
Offline locked Cargo metadata reports `Apache-2.0` and disabled publication.
Affected Markdown lint and structural STE checks passed, as did `git diff --check` and byte comparison of all 317 other tracked files, including `NOTICE`, `Cargo.lock`, and the vendored STE license.
This metadata/docs check ran no Cargo build, database, VM, E2E, or benchmark workload.
M6–M10 remain open, benchmark execution remains on HOLD, and tags and releases remain unauthorized.
Development GHCR publication has separate dated approval.

## M7 registry amendment and replan, 2026-11-10

The user approved the [registry amendment](../docs/roadmap.md#approved-registry-amendment-2026-11-10), superseding the local-registry proposal.
Pull public upstream dependencies directly by pinned digest.
Publish only manager/native-runner development images as public packages under the approved user namespace, using immutable `dev-<full-40-hex-commit>-<arm64|amd64>` tags and kind digest references.
The manager namespace is `ghcr.io/ydixken/pgcopydb-operator`.
The native package is exactly `ghcr.io/ydixken/rpgmigrate`.
Version tags, `latest`, releases, and main pushes remain unauthorized.

- [x] Record the narrow development GHCR approval and superseding M7 direction in the maintained roadmap and lessons.
- [x] Freeze the approved GHCR implementation plan and shared schema before granting disjoint implementation leases.
- [ ] Replace M7 archive/containerd-import, alias, and tag-retargeting paths with direct registry pulls.
- [ ] Qualify fresh source and RUN on the registry path, preserving ownership, original clock, caps, and cleanup guards.

Keep archived M7 source and old RUN evidence unchanged.
The old Ready RUN held before LOAD under S1 is not registry-qualified.
M10 offline candidate archive retention remains separate.
M6/M8/M10 work continues, all M6–M10 milestones remain open, and benchmark execution stays on HOLD.
This documentation change performs no registry authentication, image publication, VM, E2E, or benchmark work.

Combined integration review: offline locked metadata reports `Apache-2.0` and `publish = false`.
Affected Markdown lint, changed-prose structural STE at baseline zero, exact five-file scope, official license bytes, and `git diff --check` passed.
All 317 other tracked files retain their base bytes, including `NOTICE`, `Cargo.lock`, and vendored licenses.
Full-document structural STE retains existing findings outside the changed prose.

## GHCR source and qualification checkpoint, 2026-11-10

The engine publication source is `11a28a4d1c77b035f96cc16b7b948768dd6aa6c9`, tree `e33b886b61abdd97314a57c998cc681412c9ba15`.
The first native Linux ARM64 build, read-only smoke, and single development push completed under the approved bootstrap exception.
The immutable native tag is `dev-11a28a4d1c77b035f96cc16b7b948768dd6aa6c9-arm64`, manifest `sha256:8916bba4c10ee30e2b7ed7d6877818c1eaa4ffa1d542e851d9bcb7a8508a4e9a`, config `sha256:420a6f33ef3bf5d46cd57911256b72466f2cdb90f8015bb5c2c52c897a075ece`.
Publication status is `PUBLICITY_PENDING`: the package is private and unlinked, and the user has been asked to complete the manual visibility/linkage step asynchronously because the browser host was unavailable.
Anonymous public verification and owned-node pull qualification remain open.
Publication proof: `/private/var/tmp/rpgmigrate-native-first-publication-11a28a4-apozt_ez/publication.json`, SHA-256 `d5cb68e11305457107bf4f0bfe317a3cc7dfb6514a30603650c160272e3b84a8`.

The build used the physical ARM64 Mac and ARM64 Linux VM with Podman client 6.1.4/server 5.8.3, without a tool upgrade.
All four original containers, 206 volumes, and 124 original unique images were preserved; two owned new build images were retained.
The builder window is released and no builder remains active.
Preservation proof: `/private/var/tmp/rpgmigrate-native-first-publication-11a28a4-apozt_ez/preservation-release-proof.json`, SHA-256 `6f8245aeff5a9a68238ba0df07d606bbbbb8891e4a0f5f6ddb7e9682e3031175`.
These receipts make no benchmark timing or performance claim.

We publish only `ghcr.io/ydixken/pgcopydb-operator` and `ghcr.io/ydixken/rpgmigrate` development images, under `dev-<full-40-hex-commit>-<arm64|amd64>` tags, without overwriting an existing tag.
Ordinary publication uses the corresponding exact feature-branch workflow; the separately approved first native-package bootstrap remains a narrow exception.
Public package visibility is approved only for these packages.
Publication, visibility, anonymous typed manifest/config/blob verification, and actual owned-node pull evidence are separate checks.
No release, version tag, `latest`, main push/merge, crate publication, or benchmark authorization follows.

The tracked upstream catalog is static; the generated private schema 2 image lock binds both clean sources and verified publication receipts without a source/digest circularity.
The private schema 2 suite input requires `image_lock_sha256`, the lowercase SHA-256 of the exact lock copied to `RUN/image-lock.json`.
Go independently checks those bytes and source identities before workload creation.
M7 admits standard kind read-only `extraMounts` for bundled storage, with a manifest override selecting digest references before the first storage Pod.
This replaces the live-patch proposal; the registry route uses no archive/import or local registry and requires a fresh source freeze and RUN.
Historical failures and held RUN evidence remain unchanged.

The M6 stage freeze is parent PREPARE, UP, PULL (eight pulls with three attempts each), optional verified legacy, stable-parent-owned and locked M6 preparation, Q900 native three-by-three in a separate directory without parent mutation, V600, Go900, then final V600.
The recorded Q deadline must not exceed the original lease, independently of the current V cap.
S2 may consume opaque kubeconfig locally in memory only for the necessary identity and authentication checks; it adds no Secret API read or export and makes no zero-secret-memory-read claim.

M7 operator source is signed at `93cc670309e1039e05ee6dc0e462196d23248888`, with 105 checks and scoped lint passing; it has not been pushed.
Actual Guard admission refused the clean source at canonical SSH-origin mapping, so no RUN is qualified; a narrow identity correction remains in preparation.

The signed isolated M8 candidate `ea51e2375e51adc3d42115c588f5d3b23ea6d4a6`, tree `70198152465b405002ec4bf95d68775fa7342ed1`, composes Apache/GHCR changes without changing the M8 core and passed 398 portable tests.
Composition proof: `/tmp/rpgmigrate-m8-primary-source-composition-20261110/signed-checkpoint.json`, SHA-256 `51f840b0fcc65eac496ddd93c1f2ab209f1571e7c0f68ecfb100076ee962a39a`.
The earlier independent binary `2f0b077c` from source `611d1a4` passed one catalog and all ten Clone cases on each of PostgreSQL 16.15 and 18.6, with zero failures or ignored cases.
Component summary: `/private/tmp/rpgmigrate-m8-full-component-20261110/0dc035e4bcfa3ec9/summary.json`, SHA-256 `6babb268e5d86a6d612c785477a071dc882717023909197b7b192e98d4483833`.
This remains a component pass: the six-profile workflow across all three pairs, including 16-to-18, has not run.
Shared tag/cache, network/volume cleanup, and logical-startup harness repairs remain in preparation; the Task PROFILE source patch is ready but has no integration or runtime result.
The format 9 / metadata 7 candidate remains separate from the primary format 8 / metadata 6 baseline, without capability activation.

The final private M9 plan passed review and its interfaces are frozen; source A/B work is underway, with tests incomplete pending a coherent core join.
`REPLAY_SAFE_BARRIER_INTERVAL_MS` remains `None`, release mode is unavailable, and benchmark execution remains on HOLD with no actual benchmark result.

M10 source-tool preparation is committed at `81c5b8c035806e4934806caec4d41e7dcd85d660`: nine tooling files and three portable-suite registrations passed 439 tests across twenty suites with zero skips.
Integration proof: `/private/tmp/rpgmigrate-m10-tool-integration-evidence-jckqtq98/integration-evidence.json`, SHA-256 `591d5168b9475370f918cbedbff1e79fc0abe219d908842fe82ffec62ccf1337`; commit/fast-forward proof SHA-256 `526aedfc157eb9257c28ea0063b268baac15c4ad29e9566f3ef0a462eafdcd14`.
The actual docs checker is not green: getting-started, operator, troubleshooting, and durability guides are absent.
The pinned ASAN manifest refuses execution until the binary, symbolizer, complete build-std dependency graph, and native runtime are qualified.
LLVM tools contain no symbolizer, and the project `Cargo.lock` alone does not describe the complete build-std graph.
No actual ASAN, fuzz, or runtime qualification follows from this source preparation.
Hosted native image publication/build evidence remains separate from hosted kind runtime qualification.
The Guard's Podman VM transport requires a bounded Linux-native-host portability follow-up before hosted kind runtime activation; hosted offline contract checks cannot close that gate.
Run full scoped lint, including `gocyclo`, on the actual joined source without `--new-from-rev` filtering.
M0–M5 remain closed and M6–M10 remain open; no native operator migration or capability activation is qualified.

- [x] Complete the source-bound first native ARM64 build, smoke, single push, and preservation/window-release proof.
- [ ] Complete public visibility/linkage and anonymous descriptor verification for the retained native publication.
- [ ] Integrate and independently review both publisher sources and their exact command interfaces.
- [ ] Bind anonymous public publication receipts, static upstream pins, and clean source identities in a fresh image lock.
- [ ] Qualify fresh M7 legacy and M6 native runtime inputs under the original ownership, deadline, resource, and cleanup gates.
- [ ] Implement and qualify the bounded Linux-native-host Guard transport before activating hosted kind runtime.
- [ ] Run full scoped lint, including `gocyclo`, on the joined inputs and retain PostgreSQL 16/18 evidence.

## M0: Preserve and reset (rpgmigrate)

- [x] Preserve stash and untracked work as archive commit `8fceb45` on `wip/native-copy-snapshot`, retain `archive/stash-0`, and preserve the external tar.
- [x] Branch `feat/native-engine` from the archive and commit the approved roadmap.
- [x] Remove superseded planning inputs and point repository instructions at the roadmap.
- [x] Record the independent archived baseline before implementation.
- [x] Recreate this ledger from M0 through M10 and write the detailed M0 plan.
- [x] Leader reviews the M0 plan and assigns disjoint transport, tooling, and documentation owners.
- [x] Remove the private libpq capability check and native module, keep live passfile/service authentication coverage, and move shared test helpers.
- [x] Remove the retired snapshot journal live module and its Rust registration without changing retained runtime state or CDC code.
- [x] Remove the remaining retired journal Task selectors as part of the tooling integration.
- [x] Remove the storage pilot files, manifest entries, experiment-only dependency, and storage count gates.
- [x] Replace the controlled libpq build and custom image pipeline with stock libpq selection and portable Task checks.
- [x] Preserve the isolated database harness and useful portable positive and negative tests, with JournalSerial removed.
- [x] Install plain GitHub Actions checks for formatting, clippy, and tests; defer the replacement runner image to M5.
- [x] Reconcile the approved design with its dated 2026-11-08 revision and fix stale links.
- [x] Update README, operator compatibility, testing instructions, and notices for the implemented scaffold.
- [x] Integrate reviewed worker outputs and run `task check lint test` on the integrated tree.
- [x] Run stock-libpq live transport/authentication checks for PostgreSQL 16 and 18 and the retained harness self-check.
- [x] Complete one independent review at milestone close and resolve findings through their file owners.
- [x] Record the closing results and limitations below; commit only when the leader requests it after checks pass.

### M0 baseline evidence

The independent baseline worker observed a clean `feat/native-engine` tree at `1983a6a4e2aec62c8096c008316442c310be88d0`.
Archived `task check` exited 201 in `native-check`: `native libpq: native build recipe changed; run python3 tools/build-libpq.py --rebuild`.
Rust compilation did not run because the native prerequisite failed.
We retain this result and do not rebuild the obsolete controlled dependency.

### M0 assignments

The leader approved the plan and dispatched `m0_transport`, `m0_tooling`, and `m0_docs` with the disjoint file sets in the implementation plan.
Workers use the primary checkout for M0 and coordinate Cargo checks to avoid conflicting builds.
`m0_plan` owns this ledger and the implementation plan; integration and independent review remain separate assignments.
The leader split `tools/test-db.py` and `tests/support/**` from tooling to `m0_harness`.
The leader extended transport ownership to the synthetic endpoint-test wording in `src/lib.rs`.
`m0_integrate` owns disposable verification setup and the integrated closing gates.
`m0_review` owns the single independent closing review at xhigh effort.

### M0 focused evidence

`m0_transport` completed the source changes and observed 19 FFI tests plus one endpoint test passing with stock libpq 18.6.
The connection regression demonstrated RED before removing private capability admission.
The worker also reported passing formatting and diff checks.
These focused results do not execute the live PostgreSQL 16/18 authentication matrix.

`m0_harness` removed JournalSerial mode, inventory, fixtures, receipt checks, and unused authority selectors.
It changed `tools/test-db.py`, `tests/support/db_harness.py`, `tests/support/authority_fault.py`, and `tests/support/test_harness.py`, and deleted the three journal-only support files.
All 97 retained portable harness checks passed with Python warnings treated as errors.
The new removed-mode refusal regressions demonstrated RED then GREEN, and the worker reported a passing diff check.

`m0_tooling` completed the tooling changes and reported 134 portable tests passing: the same 97 harness tests, 31 Task tests, and six unit-runner tests.
The harness count is included in 134 and must not be added again.
Actionlint, yamllint, and locked Cargo metadata checks passed.
The dependency change removes `redb` only; retained locked dependency versions remain unchanged.

`m0_docs` completed the documentation changes and checked 20 local links and nine documented Task targets successfully.

`m0_integrate` completed disposable container setup with PostgreSQL 16.15 and 18.6 and stock libpq 18.6.
The live transport profile passed five tests on PostgreSQL 16.15 and five on PostgreSQL 18.6, linked to stock libpq 18.6.
The retained harness self-check passed, including the corrected PostgreSQL 16 test minimum.
All M0 implementation handoffs are ready.
The independent M0 review completed and identified strict lint as its remaining blocker.
Retained scaffold paths exposed by removal of the journal experiment trigger dead-code errors under the unchanged strict lint gate.
The integrated `task test` attempt passed 402 Rust tests with 66 live tests filtered, then passed 24 CLI cases.
Its first portable run failed three Linux copy-controller socket-closure controls because unread ReadyForQuery bytes remained after intentional COMMIT-response loss and caused socket reset on Linux.
The test-only repair demonstrated RED then GREEN for the three controls on Linux and macOS.
The final Linux `python3 -W error tools/ci/unit.py` passed all 134 portable checks; its log is `/tmp/rpgmigrate-m0-linux-harness-repair.log`.
After the narrow unused-import-only cleanup, `task fmt-check check` passed.
Those constituent results left M0 open on structural Clippy errors in the retained scaffold.
The independent reviewer completed the fixture-repair follow-up without a finding; the later M3 candidate clears those diagnostics, the later primary M3 checks resolve the technical blocker, and the leader accepted the recorded earlier closure criteria.
No operator migration workflow is qualified by these component results.

### Sequencing decision, user-approved 2026-11-08

The user approved: "Keep M0 open; proceed with approved milestones".
That sequencing decision allowed later approved milestones while the retained scaffold lint blocker kept M0 open.
The primary M3 strict gates now resolve that technical blocker; the leader accepted M0 closure using the recorded checks and prior review.
Preserve strict lint; do not suppress its errors, mark M0 complete, or make commits while the required gate is red.
This changes milestone sequencing, not the runtime contract or the required closing checks.

### M0 closing review

The leader accepted M0 closure using the retained PostgreSQL 16/18 transport five-plus-five results, harness self-check, and prior independent review.
The integrated strict gates and actual primary `task check` at source commit `7b6037f9ac8214696936a21c865cd97307cc5c6e` resolve its former scaffold lint hold.
Go checks are not applicable to the Rust-only M0 scope; this classification is not a new Go execution.

- [x] Record focused worker checks and their changed-file lists.
- [x] Record the integrated broad-gate command, exit status, and nonempty test evidence.
- [x] Record selected stock libpq and exact PostgreSQL 16/18 live test versions and results.
- [x] Record the independent review outcome and any remaining findings.
- [x] Record Go checks as not applicable if M0 remains engine-only, with no operator changes.
- [x] Record the final reviewed tree before authorizing any commit.

## M1: Contract freeze (both repos)

The leader-approved [M1 implementation plan](../docs/superpowers/plans/2026-11-08-m1-native-contract.md) defines task ownership and reviewed safety invariants.
The leader accepted the canonical identity/layout constants and same-row drain-witness split after independent xhigh analysis.
M1 freezes DTO/evidence contracts only; M2/M4 implement target metadata and runtime sequencing, and M6 implements controller wiring.

- [x] Prepare the M1 implementation plan and return its interfaces for leader review before source work.
- [x] Record leader approval and dispatch disjoint engine, operator-validator, and fixture/documentation owners.
- [x] Commands, option names, env (`PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGPASSFILE`, `PGCOPYDB_LOG_JSON`, `PGCOPYDB_APPLY_GROUP_TXNS`), exit codes and error classes for the native profile.
- [x] Protocol v1 amendments: version/capability DTO, `native-report:` stdout transport, work identity env, caller-supplied endpos ack, drain proof with `certified_lsn`, cleanup inventory; compare outside ParseReport.
- [x] application_name scheme, owned schema name, workdir and `control/` layout, native runner image selection and tool layout.
- [x] Shared golden fixtures in both repos with tests on both sides.

### M1 assignments and implementation boundaries

The leader approved the final plan and dispatched `m1_engine`, `m1_operator`, and `m1_fixtures` with its disjoint file ownership sets.
`m1_engine` validates supplied identity configuration all-or-none before I/O; operational commands with supplied identity explicitly refuse before mutation until M2 can bind it to persisted native state.
Help and version remain independent of operator identity configuration.
Local Rust tests cover meaningful DTO, discovery, and argv behavior, while Go remains the authoritative operational-report validator; do not build a duplicate test-only report parser or claim runtime binding from synthetic evidence.
The token's plus-one transition is a durable producer CAS invariant.
A parser may observe skipped intermediate report tokens and accepts a current token when it exactly matches trusted evidence.
Escalate any gap in the shared parser-oracle contract through the leader before changing interfaces.

### M1 focused evidence

- [x] Receive the engine DTO/CLI handoff with focused checks.
- [x] Receive the operator validator handoff with focused Parse checks.
- [x] Receive the final shared-fixture and documentation handoff.
- [x] Run the integrated M1 closing gates and independent review.

`m1_engine` completed changes in `src/protocol.rs`, `src/cli.rs`, `src/config.rs`, `src/contracts.rs`, and `src/lib.rs`.
Its focused checks passed 17 CLI tests, three protocol tests, and one diagnostics test.
Checks through the actual built binary confirmed identity-independent version output and explicit identity refusal without workdir mutation.
These results do not establish persisted identity binding or migration runtime support.

`m1_operator` completed its two assigned native-validator files.
Its focused Parse checks passed 20 top-level tests and 159 tests including subtests, with output retained in `/tmp/rpgmigrate-m1-operator-parser.json`.
The 159 includes the 20 top-level tests; do not add the counts together.

`m1_fixtures` completed 186 shared cases across 132 total files, of which 131 payload/contract/case files are manifest-listed; the two fixture trees compare identically.
All 146 local Python checks passed.
All M1 implementation owners handed off their work and froze source for integration.
`m0_integrate` is authorized to run the once-per-milestone engine check/lint/test and operator lint, Go, envtest, and PostgreSQL 16/18 helper gates.
`m0_review` performed the independent xhigh closing review.
The final engine closing checks passed 410 Rust tests, 24 CLI cases, and 146 portable checks.
The new M1 clippy finding was repaired and the three protocol tests passed; final strict lint reports only retained unused/dead-code paths, with 243 library and 27 library-test errors.
The operator passed its Task lint stages and final Go lint with zero issues using Go 1.27.0.
Its PostgreSQL 16 full unit/envtest gate passed all 15 packages, and the final native package run passed 22 top-level tests and 291 cases including subtests.
The PostgreSQL 16 helper gate passed 508 of 510 tests including subtests, with two expected skips and 138 top-level tests.
The PostgreSQL 18 helper gate passed all 158 tests including subtests and 17 top-level tests, including generated virtual columns.
Top-level counts are included in the corresponding subtest totals and package runs may overlap; do not aggregate them as independent cases.
The independent review reported no remaining findings, and both repositories passed `git diff --check`.
The contract build, tests, and review are complete.
The leader accepted M0 and M1 closure after the later M3 primary integration and strict gates passed.
These contract results qualify no native migration workflow.

## M2: Clone slice (engine)

The [M2 implementation plan](../docs/superpowers/plans/2026-11-08-m2-clone-slice.md) records the leader's plain-clone scope decisions, seven-owner map, shared API bootstrap, and closing acceptance gates.
The leader approved the M2 draft scope and seven-owner decomposition; parallel source work waited for the shared-interface handoff before implementation began.

- [x] Prepare the bounded M2 implementation plan and return its proposed interfaces and ownership for leader review.
- [x] Record leader approval of the draft scope, seven-owner decomposition, and explicit recovery boundaries.
- [x] Record leader approval, concrete shared signatures, and implementation dispatch.
- [x] Demonstrate a focused quiescent PostgreSQL 16 to 16 plain clone with durable completion and independent schema/data/sequence verification.
- [x] Resolve all independent closing-review repairs for target ownership, marker readback, generated output, and transaction durability proof.
- [x] Pass the final Prepared10 and Clone7 component profiles on PostgreSQL 16 and 18.
- [x] Pass the eleven-case 16 to 16, 18 to 18, and 16 to 18 profiles, each with six actual Go report checks.
- [x] Transport: noncommitting pipeline validation, private error/warning classification, observed commit proof, per-operation I/O budgets, and buffered COPY.
- [x] COPY authority: logged per-table markers, copy generation, exact owned identities, per-run fence, and durable unknown-COMMIT resolution.
- [x] Accept the M3 grouped CDC frontier/`certified_lsn` producer/API and apply hot-path qualification as the M2 handoff.
- [x] Plain-clone order: SQL-exported snapshot, matching-major pre-data, parallel physical-heap COPY with FREEZE, post-data, sequence sync, and ANALYZE on all three pairs.
- [x] Plain clone `--restart` and `--resume --not-consistent`; `list progress --json`; `compare schema`; `compare data --json` with pinned GUCs and the operator's shapes.
- [x] Podman engine E2E targets for all three pairs, controlled partial-copy/utility interruption and resume cases, and independent schema/data/sequence oracles.
- [x] Close M2 using the staged three-pair clone receipts, prior review, and later strict M3 closing checks; no M2 aggregate rerun is claimed.

### M2 assignments

The leader dispatched `m2_shared`, `m2_transport`, `m2_checkpoint`, `m2_catalog_schema`, `m2_snapshot_copy`, and `m2_compare` read-only pending `m2_shared`'s final signature freeze.
The seventh worker, `m2_harness_docs`, owns the disposable container E2E harness, clone fixtures, Task targets, and affected documentation.
`m2_shared`, `m2_transport`, `m2_checkpoint`, and `m2_snapshot_copy` use xhigh reasoning for coordinator recovery, I/O state, durable authority, and snapshot/transaction concurrency.
`m2_catalog_schema`, `m2_compare`, and `m2_harness_docs` use high reasoning for bounded catalogs, independent comparison, and deterministic qualification.
The leader released all seven owners after the concrete shared signature freeze landed in the M2 plan and `src/contracts.rs`.
`m2_shared` alone owns root module registrations and the new `src/clone.rs` driver, with the exact disjoint file sets recorded in the M2 plan.
The leader approved an explicit `&mut CloneAuthority` argument to `copy::run_tables`.
The shared coordinator retains its fence for the complete command, including schema and post-data; worker permits authorize only their exact data transaction and each worker creates its own local `!Send` connections.
No worker implementation starts before the concrete shared handoff, and no milestone is marked complete or committed while the inherited strict lint gate is red.

### M2 harness preparation

The leader assigned `m2_harness_docs` ownership of roadmap status maintenance as well as this ledger.
We keep the roadmap's completed subdeliverables synchronized with these observed results while leaving milestone closing gates open.
Normal lint-clean commits require resolution of the inherited M0/M1 strict lint hold; the two explicitly approved WIP checkpoints below are the only exceptions.
The portable Python gate passed 154 checks after the first M2 harness and live-test inventory additions.
That preparation receipt preceded the later clone/catalog inventory updates and the qualified staged pair receipts recorded below.

The leader accepted M2 closure from its reviewed staged 33 clone cases and eighteen actual Go clone validations, plus the later exact-input M3 closing checks and current strict lint.
The grouped CDC/certified-LSN handoff is accepted from M3; this does not qualify M4 cutover.
No canonical M2 aggregate target rerun is claimed.

## M3: Follow (engine)

An earlier assembled M3 candidate passed `cargo check --all-targets`; focused component proofs do not establish integrated follow qualification.
The guarded join integrated 25 paths from 12 approved manifests; its interim manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-coherent-join-ebh3aa96/manifest.json`, SHA-256 `3eccdff141d85205beaab2260793040cd0f95f710292e470ea105c3cf9de9ff2`.
That interim join is not qualification; the later product candidate and qualified money/Catalog13 handoffs below supersede its incomplete input status.
The first actual 16 to 16 follow smoke failed at startup with command exit 1, engine exit 4, and SQLSTATE `25P01`, before publication/slot intents and before a positive streaming report.
The retained receipt is `/tmp/rpgmigrate-m3-first-smoke-attempt-1.json`, SHA-256 `30449f01fb1cc001cbc6ca1b907bd8a7a745f05d9f67428da71478c21cfab8dd`.
After the planning-transaction fix, the second smoke reached validated `driver.ready()` and then failed when harness SQL tried to update the `GENERATED ALWAYS` identity primary key.
Its immutable attempt receipt is `/tmp/rpgmigrate-m3-second-smoke-attempt-2.json`, SHA-256 `2b58e59edc651c8a7d50fea7bb8bf8b6845f49b1dc9e7866adf4ea43bbf3b76f`, bound to the exact input manifest recorded there.
The raw startup report was not retained, and no convergence receipt was emitted.
The source workload repair reproduced the actual PostgreSQL 16 identity-update failure, then passed eight corrected SQL checks and 14 portable controls.
The third interim smoke against candidate `499ce675` passed startup and corrected source workload, then failed the independent target exact-owned-relation oracle; no convergence or success receipt was emitted, and a product cause is unconfirmed.
Its immutable proof is `/tmp/rpgmigrate-m3-interim-smoke-attempt-3.json`, SHA-256 `7b5669544ec8bcaa928b1273d342b523fe48532224a683a095096d4eb11c7eb0`.
The independent SQL/JSON-shape audit on PostgreSQL 16.15 and 18.6 identified string-valued OID projections as the oracle failure; explicit OID-only bigint casts passed full authority checks and 13 identity-negative controls per major.
The SQL-only research receipt is `/tmp/rpgmigrate-m3-oracle-YuST2D/receipt.json`; it does not qualify engine execution.
The narrow two-file implementation passed 15 portable checks and mutation controls, frozen at `/tmp/rpgmigrate-m3-oracle-oid-export-0tx7b5kq/manifest.json`, SHA-256 `80cda83eb287ec1ea12ac8450da81e69bf4b45999ce6e5c71b7a696009e97f60`.
Authority V3 passed six focused live cases per major and 43 unit tests; the receiver binary passed two cases per major, without establishing whole-workflow qualification.
The leader accepted `/tmp/rpgmigrate-m3-source-snapshot-fault-plan.md` and assigned its nine-case native replacement to `m3_snapshot_fault_build`, owning the new fault module and minimal clone-identity PID guard.
The guard appends SQL `pg_backend_pid()` as the fifth identity field and compares it with libpq's PID in the same query, preserving the tuple contract and privilege requirements.
After actual PostgreSQL 16 RED, the minimal guard passed two focused cases each on PostgreSQL 16.15 and 18.6; its manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-snapshot-pidguard-hibubx0c/manifest.json`, SHA-256 `adb00f732bb66df625ddb970aee76c88780c592511500d7c87f0136651e26b51`.
The full nine-case native source-snapshot profile passed exactly nine cases on PostgreSQL 16.15 and nine on 18.6, with zero ignored cases.
Independent validation accepted five fault receipts per major, including three tail phases; the 294-file runtime source-input manifest stayed unchanged.
The qualified PID guard is unchanged; the final three-file builder export awaits formatting, and aggregate/default-gate qualification remains open.
The old twelve-case `SourceSnapshot` stage still hard-codes PostgreSQL 14/17 and native format 4.
The nine-case per-major prerequisite is satisfied; the later full-profile and native closing proofs below authorize retirement, whose private integrated candidate is recorded below.
The money restore-parse defect reproduced on actual PostgreSQL 16; the two-file fix is frozen at `/tmp/rpgmigrate-m3-source-money-candidate/manifest.json`, SHA-256 `77690530cdc6a26c4472929ae1b4f4e554b03a9fd29ba59ddde255bd04812e63`.
The exact candidate product bytes passed one actual money/cross-locale case on PostgreSQL 16.15 and one on 18.6, retained at `/tmp/rpgmigrate-m3-source-money-qualified/manifest.json`, SHA-256 `67288f984c65e228552e5e99a5171e97d44c90866f74708e5bf9a9f53708416f`.
Ordinary non-superuser German/US role defaults, permission to set C locale, actual SQL snapshot, dump/pre-post restore/parallel COPY, money scalar/array/NULL/negative values, and restored-default INSERT all passed.
The harness Catalog13 registration export is complete; the final combined broad gate and follow convergence remain open.
The assembled product candidate passed `cargo clippy --locked --all-targets --message-format=json -- -D warnings` with exit zero and no compiler diagnostics or lint allowances.
Its manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-product-candidate-qtdju59t/manifest.json`, SHA-256 `499ce675a828a1eecf2e60a027554859cc084dc35da58274d2b3d92804bc05ee`, covering 289 files and 48 absent paths.
Retained command output is `/tmp/rpgmigrate-m3-product-clippy.jsonl` and `/tmp/rpgmigrate-m3-product-clippy.stderr`; the integrator also observed `cargo fmt --all -- --check` exit zero.
This clears the archived scaffold diagnostic blocker on that candidate only, without a primary-tree/Task gate pass, full required tests, or independent milestone review.
The joined PID/oracle candidate `447a81f5` passed renewed strict Clippy and formatting without allowances.
Attempt four passed startup and corrected source DML, then follow exited before convergence; the harness discarded engine stderr/status, so the actual cause is unknown and no live authority-oracle pass is retained.
The immutable RED proof is `/tmp/rpgmigrate-m3-guarded-smoke-attempt-4.json`, SHA-256 `97affd987b52851a0b9ae84bb6780bc4039835d2c2650b4a93a6b70c109da0f4`.
Safe error-origin metadata is assigned as a mechanical `Error::new`/`track_caller` change across 58 constructors, adding stderr `error_origin { file, line, column }` while preserving error categories/messages and M1 report/state contracts.
The harness owner must capture matching structured origins before cleanup, with a subprocess regression, before one changed-input reproduction; the actual engine cause remains unknown until that run.
Attempt five retained only secondary cancellation at `io.rs:44:20`, without convergence or source-confirmed completion; its proof is `/tmp/rpgmigrate-m3-origin-smoke-attempt-5.json`, SHA-256 `aa2117ad20322ca7ce7e61853baa4ffb8a1c0f58cfac52e64c8a33623a8124dd`.
Read-only tracing proved that a worker enqueues its primary error before cancellation, but health SQL cancellation bypassed that queued error.
The bounded typed-cancellation fix preserves a queued worker error only on cancellation, retaining genuine health failures, deadlines, and external cancellation behavior.
Its manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-error-precedence-export-1qy0uoeg/manifest.json`, SHA-256 `afe5db00e0c3e49f86487ff4aef64e3f702f389cfc2658e137b811728d4fa7d3`; 42 focused tests, check, strict Clippy, and formatting passed.
The joined candidate `21222aa8bfd191b76bd3a215e5c28f1c8cfd66cedc08d14eee3db4dfd6c0340a` also passed strict Clippy and formatting; no runtime success or primary-cause claim follows.
Attempt six against `21222aa8` retained primary SQLSTATE `428C9` at target DML: UPDATE assigned every non-TOAST column, including an unchanged `GENERATED ALWAYS` identity.
Its immutable proof is `/tmp/rpgmigrate-m3-primary-smoke-attempt-6.json`, SHA-256 `b902877e4631e9cdaa24dab4171669b7cf632494694d421f44edbb2f21f996a6`; no convergence or success receipt was emitted.
The leader approved the complete [identity UPDATE fix plan](../docs/superpowers/plans/2026-11-08-m3-identity-update-fix.md), copied from the reviewed handoff with repository Markdown formatting only, and assigned five parallel format/source/apply/transport/authority owners.
The planned native 7 / metadata 5 contract persists actual and optional partition-root identity modes, normalizes representable 16 to 18 inherited modes, and refuses independent PG16 leaf identity DDL that PG18 cannot represent.
Normal UPDATE requires old/key proof that ALWAYS values are unchanged; actual or unproved identity changes use one atomic exact-row DELETE RETURNING to INSERT OVERRIDING statement, without silently omitting changes.
The all-ALWAYS/REPLICA trigger guard includes internal FK triggers; the joined native 7 / metadata 5 candidate passed all-target check, strict Clippy, and formatting.
All five identity packages are frozen; format checks passed nine contract and 105 state tests.
Source catalog passed five exact live cases per major with exact-inventory audit; its manifest is `/tmp/identity-source-final/manifest.json`, SHA-256 `e29e7ba1e5c0853e539a2c5bd02f1a2cf34e856e9927c3b00d7a57de20c759e6`.
Authority passed six exact cases per major, including one generic origin-only case; apply passed six exact cases per major, including actual source-valid deferred-UNIQUE permutation/parity and sequence nonadvance, and both exact-inventory audits passed.
The apply manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-identity-apply-final-f19d4aph/manifest.json`, SHA-256 `0a7d878a28c7602829d5fbc0ac206c412dbb6ebbfb62dc68d6d070a500f4699e`.
Transport14 passed on each major, but its copied tree contained 48 stale unselected storage files; this is not exact-inventory qualification.
Engine50 includes all fourteen transport cases exactly once and must close that exact-tree gap without a duplicate Prepared run.
Earlier discovery assigned 126 ignored library tests exactly once; the later closing-gate discovery below supersedes that inventory with 116 required current cases per major and twelve explicitly excluded legacy cases.
The registered inventories include Engine50, FollowApply11, FollowAuthority11, and FollowReceive4, including the exact 70-second cadence case.
The discovery and focused receipts are not aggregate results; the later native closing proof records the combined profile outcome.
Replica apply suppresses ordinary origin-only deferred UNIQUE rechecks; require a source-valid deferred-UNIQUE permutation with exact row parity and target sequence nonadvance, preserving deferred-failure rollback only in generic origin-mode Authority.
This ruling adds no admission delta and retains all ALWAYS/REPLICA callback refusals, including internal triggers.
The first native 7 / metadata 5 PostgreSQL 16 to 16 ongoing-follow smoke passed against exact input `2b4faad37a5a7880ae6c011e7ef8acb1ff370e7c0dd89833c4949927421034d0`, with command exit zero and one selected case.
Actual servers were PostgreSQL 16.15 and linked libpq was 18.6; initial COPY, valid workload, independent schema/data parity, target frontier `27638344`, forwarded status, and actual source confirmed flush all passed.
The smoke proof is `/tmp/rpgmigrate-m3-native7-smoke-attempt-7.json`, SHA-256 `a7e1b0949aa2aa333fe86988edc4749c460a86d0b86b70b4c53ed8c96921844b`; its public receipt is `target/e2e/follow-smoke-16-16.json`, SHA-256 `deaab47548d8f1a21858689701855726561a1a0a8fbf1cf92f49c7592a9ab59d`.
All 295 candidate file bytes/modes and 48 absences matched after the run, and owned containers were cleaned.
The remaining 18 to 18 and 16 to 18 smokes passed one selected case each on the same frozen input and Linux binary, with target progress, independent parity, forwarded feedback, and actual source confirmation.
The 18 to 18 proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-smoke-18-18-psfz6qm1/proof.json`, SHA-256 `0c26006b37a23c2e3893eff7ebe1791d44780ed616bf0135c14074c7df6cf3a2`; its public receipt SHA-256 is `737104288b62b6d5642dd6a4b4b36d2d3f8c69a63f73718d145488d7e2a2f8aa`.
The 16 to 18 proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-smoke-16-18-qfiee7ub/proof.json`, SHA-256 `1ef3f87c325b538c089b2c564d3c0e33cfbd74e319414b60d9ccefcacccac7f2`; its public receipt SHA-256 is `cde88da73fc75d6fe29dadf543c4f5ce9e7cd546ebf7e533b2dd84e92ea9eba4`.
All three ongoing-follow smokes passed, with all 295 file bytes/modes and 48 absences unchanged and owned containers/networks absent after cleanup.
The observational ACK evidence adds no production feedback authority.
At that smoke gate, the harness controlled an ongoing-follow stop with `successful_follow_exit=false`, without a full-profile or milestone claim.
The later full matrix and closing evidence supersede that partial scope; M4 cutover remains unqualified and prior RED receipts remain retained.
The independent PostgreSQL 16/18 SQL-only oracle proof remains separate and valid; the old twelve-case stage was unretired at that smoke gate.

### M3 full-profile attempt, 2026-11-08

The frozen full-matrix source manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-full-matrix-candidate-195jxlj_/manifest.json`, SHA-256 `ce32008f4ff359d5762901db3ed999a08febd6dc78407309999dfb860bfd1c2e`.
The first full 21-case PostgreSQL 16 to 16 run stopped RED at the `before_commit_rollback` initial startup wait.
No passed-case count or full 21-case receipt was produced, and the full 18 to 18 and 16 to 18 profiles were not started.
The retained observations show zero B/C/status counts, target metadata format 5 with order zero and no anchor, and an inactive source slot.
No native Error, SQLSTATE, or stderr was retained.
The immutable proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-full-16-16-red-8kudh4n3/proof.json`, SHA-256 `77d2743502d145f123d86d2a5a98d538d88be950ee119d8d73d77fc9baded010`.
All four owned containers and the owned network were proved absent after cleanup.
Six threaded tests confirmed that the harness proxy misclassifies ordinary COPY `r`/`w` without a logical handshake.
The minimal two-file patch passed all 38 portable checks; its export is `/private/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-proxy-classification-umez92in/export/manifest.json`, SHA-256 `6ae257f94bff91a979f9146cf3240a09f4a4a431420c50d63cb8f2e119f740d8`.
The bounded PostgreSQL 16 diagnostic stopped RED at the same initial readiness wait before fault injection or rollback, with both proxy error counts zero and no B/C/status observations.
Target metadata remained format 5, applied order zero, and no anchor; Rust was unchanged, and the fixed harness does not explain the original startup failure.
The proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-diagnostic-red-9yjq1b3w/proof.json`, SHA-256 `2fc533713fa6524f3fab79849ef014a9509323cfa84b3e258cfb0f3cdb9a7630`.
Research completed the comparison of passing source-only proxy smoke and failing source-and-target proxy startup; all fault gates were armed without an inadvertent hold.
Code-derived counter four bounds committed Copying through before PostDataPending, or waiting for counter-four reply; it is not an observed phase.
The leader approved observability-only ordinary-proxy phase/frame counters, backend waits, process basenames, and exact owned IDs at 45 seconds and failure before cleanup.
The capture patch passed 53 follow and 13 clone portable checks before the next captured RED at 2026-11-08 22:50:55 UTC.
That proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-live-capture-red-dxg8ilgi/proof.json`, SHA-256 `3527a6bdaa65c9aabca27b17208dc31c66eeb7160dcc1ebf9f7d3169d6017408`.
Between 45 and 60 seconds, COPY workers progressed and closed, the last 207 ms before failure; no fault was held, SQL error retained, or lock wait sampled, and readiness correctly waited for post-data and anchor.
Replanning confirmed that the Q-only before-COMMIT gate misses the engine's extended-protocol P/B/D/E/S COMMIT.
Actual Linux measurements for identical 25-exchange workloads were direct 2.130 ms, default relay 2086.426 ms, and two TCP_NODELAY options 7.236 ms; all nine trials retained identical bytes.
The Linux receipt is `/private/tmp/rpgmigrate-m3-tcp-probe-20261108/linux/receipt.json`, SHA-256 `fe22f492eec3589f16c48b943b45d84054f60a175b6381af34e75470f6bde318`; the earlier macOS negative remains separate evidence and cannot exclude Linux behavior.
The leader approved only the two harness socket options and bounded extended-COMMIT/actual-data-transaction witness gates; no next runtime precedes the frozen fix, and engine Rust/deadlines remain unchanged.
Likely proxy latency is measured, but startup repair and engine WAL speedup are not established.
The diagnostic input manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-diagnostic-candidate-l95cydv_/manifest.json`, SHA-256 `2348f106d5e5c0f12cc62e8b6d9428fbf7180e0acfab0969059cce69e914c786`.
Rust is unchanged; the patched harness passed 47 follow and 13 clone portable checks, including the abort-held-frame fix for confirmed queued COMMIT forwarding after backend death.
Require post-crash checkpoint and fingerprint observations after held COMMIT and backend closure, before resume; pre-kill invisibility alone is insufficient.

### M3 full matrix and closing gates, 2026-11-09

The full 21-case profile passed on PostgreSQL 16 to 16, 18 to 18, and 16 to 18: 63 named case executions and 42 actual clone-bound Go report validations while fixtures were alive.
The aggregate proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-full21-matrix-pass-aehu4grh/matrix-proof.json`, SHA-256 `9a63d833bfc38749d90cf04812824f6b37794532f9442c0ba57cb660782187da`.
It binds input manifest `3cbef490709f949db02bb9853c0d959d90b064b2c52a7492362fa28f6621d331`, native format 7 / metadata 5, and engine `dee3822a8a4fbea158ac744b43bbb3478fd08e6b1de51822e356b29bc34d4909`.
All twelve exact owned containers and three private networks were cleaned; prior RED attempts remain retained.
These controlled ongoing-follow stops (`successful_follow_exit=false`) qualify follow, not cutover or power-loss durability; later closing checks and review establish M3 closure.
The legacy twelve-case SourceSnapshot stage is removed in the later private retirement candidate below.

The closing native gate stopped at PostgreSQL 16 Engine50: 49 PASS, one FAIL, zero ignored, Cargo exit 101.
`apply::checkpoint::live_tests::async_barrier` at `src/apply/checkpoint_live_tests.rs:795` expects `fsync on`, while the shared producer diagnostic says `fsync and full_page_writes on`.
The unsafe target was refused; this assertion failure does not qualify Engine50.
At that attempt, the other nine profiles and all PostgreSQL 18 profiles were unrun; discovery assigned 116 cases per major and excluded the twelve legacy cases.
The retained gate proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-native-gates-xjbjoep0/gate-proof.json`, SHA-256 `c812e4fd96679fc5274035ea13ddd6b0a28527003b899dd67fc689e5ab051b97`.
The later assertion-only successor is integrated at `/private/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-assertion-successor-1llvusfd/manifest.json`, SHA-256 `d85e84396608751e161646a3ecc43bba4eaa30ca360c474e037edd62fb57e8ec`.
It preserves 295 of 296 files unchanged and all 48 explicit absences; only the stale test assertion changes.
Independent successor verification passed PostgreSQL 16 Engine50, including the repaired `async_barrier` assertion and six live-fault receipts.
Catalog13 then stopped with twelve PASS and one FAIL; the other eight PostgreSQL 16 profiles and all PostgreSQL 18 profiles were unrun at that point.
The gate proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-native-successor-gates-9sl848l6/gate-proof.json`, SHA-256 `d3ee0f66e92ff067b3a5a352f9019fbfb3e1199f995ea08c0385e28e3ab8bee1`.
The retained diagnosis is the sibling `catalog-diagnosis.json`, SHA-256 `6e3b841b552dd3bcc05e5b55a9755357ab23b70689f81eab7e7d6b07acad5211`.
CDC tests intentionally commit real heaps into the shared catalog/clone database; the clone collision guard correctly refuses the leaked fixture state.
`operator_lint_repair` owns the bounded isolation fix in `tools/test-db.py` and `tests/support/test_catalog_harness.py`: provide a fresh empty clone pair without changing Rust production code or weakening the collision guard.
The two-file isolation successor is frozen at `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-catalog-isolation-successor-btnxznjw/manifest.json`, SHA-256 `69086a1592f1885d948d85de380e7f5537e5e540c44aaf4c7cbf61a04d36057f`.
All fourteen focused checks pass, including the distinct-pair regression and truthful Engine50/Authority19 inventory assertions.
Exact integration preserves 294 other files and all 48 explicit absences; only the two assigned paths change, both mode 0644.
The integration proof is the sibling `integration-proof.json`, SHA-256 `e9817ed5093813798da44704b8921c6a88ef26d14ca9bc8979a821b6e299df73`.
After correcting matching PGDG 16 tool PATH, PostgreSQL 16 reached ninety qualified native cases: retained Engine50 plus fresh Catalog13, SourceCatalog3, Clone7, Schema1, FollowSource7, and FollowSnapshot9.
FollowAuthority11 stopped with ten PASS and one FAIL in `follow_cancelled_sync_wait_never_proves_group_or_certification`.
The actual outcome was not printed; the test checks `reload_conf()==t` without a SHOW-policy wait, asserts Unknown before cancellation-observer join, and retains no positive SyncRep/cancellation witness.
The focused Step 1 diagnostic reproduced an Acknowledged outcome without a same-PID COMMIT/SyncRep witness and with `cancel=false`; this is not production warning-classifier proof.
Receipt: `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-sync-cancel-diagnostic-4hoq3_xj/receipt.json`, SHA-256 `3d2511b40c86b34bc4adf525ec87c28f0061254d1ef4b9a171cd4a97a130acc9`.
Three actual owned servers were cleaned and six fixture paths proved absent; two version probes included in the broader handle list are not additional servers.
The leader approved test-only Step 2 exact SHOW-policy and CHECKPOINT readiness checks, preserving positive cancellation witnesses and unchanged safety assertions.
The later test-only readiness successor completed the remaining profiles without a production fix or assertion weakening.
Gate proof: `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-native-isolated-gates-x8kkfk2v/gate-proof.json`, SHA-256 `1b07a04d5c849312af3ad3fc3ad1200d03e46da5483a84c310a871f154582870`.
All owned fixtures and processes are absent; the earlier gate above remains a partial result.
The final native gate passed all ten required profiles on PostgreSQL 16.15 and 18.6: 116 tests per major, 232 PASS, zero failed or ignored.
The proof is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-native-sync-gates-eef3_vhc/gate-proof.json`, SHA-256 `42c3a6860e41aa52ed36954949f6185687b3784607b1c32936445bf4ae6c0298`.
It combines 142 fresh tests on manifest `c155b7c3775dbb7f063dadfcaa054b77d086b193f808b73b9f06d5d50a347839` with ninety retained PostgreSQL 16 tests whose selected test, helper, and production inputs remain unchanged.
All owned fixtures and processes were cleaned; retained tests were not rerun.
Legacy-stage retirement is authorized across eleven paths with two deletions, preserving native116 and COPY40 inventories.
The two-path Rust export is `/private/tmp/rpgmigrate-retire-rust-qmz6voe9/manifest.json`, SHA-256 `b18e664cbf7f12ace67231c52b1454ea1813a0957a3148df8ca6b059a20c759f`.
It removes twelve legacy cases from compiled Rust and passes four primitive unit checks plus one clone-ticket unit check.
The five-path harness export is `/private/tmp/rpgmigrate-retire-harness-o4c00xdd/manifest.json`, SHA-256 `8f6786fd25e39cba7fd10a13faf41c614fe938475f7f1b56f769ecb1b661f1eb`.
It passes 34 Taskfile and fifteen FollowSnapshot checks.
The four-path portable-inventory export is joined with the Rust and harness exports across all eleven disjoint paths.
The private candidate is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m3-closing-retired-6whavatu/manifest.json`, SHA-256 `3a9e30d41b0fb93da5df02aca8e7abbb78f5189d317361be67a21c7a9ad008c2`.
It contains 294 files and fifty explicit absences, without a production behavior change; native116 and COPY40 inventories remain intact.
The portable inventory registers thirteen suites and 254 tests.
The closing integration worker passed formatting, strict Clippy, 433 Rust tests, 24 CLI tests, and 254 portable tests.
Eighty COPY tests and all 232 native profile tests passed with their recorded exact-source provenance.

Canonical operator lint ran against exact checkpoint `5e8cbcbbe7ad9b89cb80a7edc1d5004af5cd4e58` and failed with 25 diagnostics: 24 `goconst`, one `unparam`.
All other lint stages passed, with no source changes; receipt: `/tmp/rpgmigrate-m3-operator-close-20261109/receipt.json`.
Canonical `task test` passed all 15 packages uncached at exact clean operator checkpoint `5e8cbcbbe7ad9b89cb80a7edc1d5004af5cd4e58`, using a fresh empty Go cache, `GOFLAGS=-p=2`, and `GOMAXPROCS=2`, without a bridge or source overlay.
PostgreSQL 16 cluster-free race checks passed 508 checks with two expected skips: generated virtual columns require PostgreSQL 18, and the live-writer helper is a subprocess entry point.
PostgreSQL 18 Coverage/StampedSchemas race checks passed 158 checks without skips; the cluster-driven `TestE2E` was deliberately excluded from both runs.
Both exact owned PostgreSQL containers and private etcd/kube-apiserver PIDs were proved absent after cleanup.
The evidence is `/tmp/rpgmigrate-m3-operator-close-20261109/go-final-receipt.json`, SHA-256 `3c0204aa37c4040bd6896bfd83f1c1279a22686ae48c4672225dd1069f20b751`.
The minimal twelve-path lint repair passed 37 focused checks and is independently integrated at tree `d9003a7939eac53ffe471134196440906794f45f`.
It changes test constants plus one production comment explaining why, without a production behavior change.
The repaired successor passed full canonical lint: all sixteen stages, zero issues.
Its full source hash `f9993ced4f02d262c2e41336a08a3c8667f38dd08ce6c3ad31bf459a7c4b8d54` was unchanged before and after the run.
Receipt: `/tmp/rpgmigrate-m3-operator-successor-lint-20261109/receipt.json`, SHA-256 `a69aae6f77cb059d9801ac580f51ebbdce8600016b01833ee61667cb2acfa44a`.
Retain the earlier 25-diagnostic failure and clean-checkpoint Go PASS as distinct exact-source evidence.
The repair uses equivalent test constants and one production comment; no successor Go rerun is claimed.
M3 closing checks and the independent xhigh whole-M3 safety review passed with no actionable findings.
The source and testing guide are committed as `7b6037f9ac8214696936a21c865cd97307cc5c6e` on `feat/native-engine`.
Actual primary `task check` returned zero; formatting, strict Clippy, 433 Rust tests, 24 CLI tests, 254 portable tests, and eighty COPY tests passed.
The 232 native cases and 63 follow cases with 42 Go clone-report checks retain their exact-source receipts; they are not additional offline test counts.
The separate operator lint repair is committed as `2bdc7614c04bc6aa8a8f4c709a84cbcc07340b5c`, tree `d9003a7939eac53ffe471134196440906794f45f`.
M4 cutover closed under the qualification recorded below; M6 native activation, M8 widening, and M9 power-loss/performance qualification remain open.
The operator checks above retain their exact-source scope and do not qualify native operator runtime.

- [x] pgoutput v1 decoder (B, C, R, Y, I, U, D, T, O; others fatal) with relation generations and PostgreSQL 16/18 regression coverage.
- [x] Receive thread with decision 3 backpressure and status rules; slot health checks; gap refusal on connect.
- [x] Applier: RI FULL keyless (ctid single-row), RI INDEX, unchanged TOAST, multi-relation TRUNCATE (ONLY, never CASCADE), identity and generated columns, non-built-in types as text; pipeline groups; strict dedupe.
- [x] Phase-aware restart/resume admission and recovery within the qualified profile, including resume after the anchor.
- [x] Pass native 7 ongoing-follow smokes on all three declared pairs with independent convergence and actual source-confirmation evidence.
- [x] Pass the exact nine-case native source-snapshot profile on PostgreSQL 16 and 18 with independent fault receipts.
- [x] Pass the full 21-case follow profile on all three declared pairs, with 42 actual clone-bound Go report validations.
- [x] Pass all ten native profiles on PostgreSQL 16.15 and 18.6: 116 tests per major, zero failures or ignored tests.
- [x] Join all eleven authorized legacy-stage retirement paths in the private candidate.
- [x] Pass the retired candidate offline closing gates and integrate the reviewed result into the primary tree.
- [x] Complete the independent whole-M3 safety review and commit source as `7b6037f9ac8214696936a21c865cd97307cc5c6e`.
- [x] Pass the declared M3 fault/recovery, filtered-WAL, keepalive, and round-trip controls.
Parser fuzzing and measured catch-up performance remain M10 and M9 qualification gates.

## M4: Cutover, drain proof, cleanup (engine)

### Current M4 closing gate

The checked-close successor is frozen at `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/manifest.json`, SHA-256 `2034c420f18ec0f32064eb8bb90c74c14da8a54a8f858095abbb888ab714a926`, inventory `48f3253f662f7bafee9a667d6d1968b7a6db546396b8bc9ba1669da3852b09e4`.
Six disjoint source/test paths changed, with 348 other identities, 304 files, and fifty absences exact; native 8 / metadata 6 / 29-column bytes are unchanged.
Fresh joined-input host gates passed on `2034c420`: Wire24/Cutover36 with warnings as errors, canonical `task lint test` with 499 Rust PASS/22 ignored/107 filtered, 27 CLI tests, and 324 portable tests, followed by explicit `task check` exit zero.
Host proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/host-gate-proof.json`, SHA-256 `6d6451bb367cd1767daac9971febda47d46fc2023961f4776e120121f1180d0e`.
Generated target outputs are retained outside source, and all 304 files/fifty absences remain exact; the production edit still requires live acceptance of its newly built runtime.
The old-runtime RED harness is frozen at sibling `red-harness/manifest.json`, SHA-256 `ef2cb69c58c4bb815f113ccfbbbeacc348acd1a5b9e638176918a5bdd1348da5`, deliberately combining the five new test-harness paths with unchanged `96e1` production and `cadadfbf`/`87a0` build.
That is not the repaired production input.
The old `cadadfbf` PostgreSQL 16-to-16 control reproduced meaningful RED: `engine-completed-resume-close` rejected an actual engine exit zero with one validated completed frame.
The relay delivered the second COMMIT plus Idle, then observed frontend X/`old_drop=true`; independent observation proved the exact BackendKeyData-bound PID absent on the selected target.
All state-preservation checks were true and the worker/relay were reaped; Task exit 201 is rejection of the old behavior, not a green test exit or arbitrary fixture failure.
Frozen old RED proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/red-harness/e2e-runtime/old-runtime-red-proof.json`, SHA-256 `c012d3e0fa9a954e6a8d945712670b37440bc43c889a2d9d7314c784bf1adb8f`.
All three owned containers and the network were proved absent, source remained exact at 304 files/fifty absences, privacy scans were clean, and free disk was 12,515,401,728 bytes.
The repaired ordinary Linux runtime is built from `2034` as SHA-256 `1fd03e42f9656495816d6c36a5be32eec9fa69a319d2ab28506d9cd979578a52`.
Build proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/runtime/build-proof.json`, SHA-256 `e2797532b737dc3bb0ec61abccddb65f190f7bb04e26f1ece49661219f2a84f0`; stock libpq 180006 and its `094cbe08` artifact remain unchanged.
The build runner and network were removed; minimum free disk was 12,515,061,760 bytes and peak cgroup memory was 492,146,688 bytes.
Fresh repaired-input SourceCatalog3 and FollowAuthority17 passed on PostgreSQL 16.15 and 18.6: forty cases total with zero ignored, on `2034`/`1fd03e42`.
Proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/native-sampling-proof.json`, SHA-256 `9e2c66442af6f16bf383633236370503efb2c20bad035aa0df9a84c0648813cc`.
All six owned clusters and sockets were absent, original stock state stayed unchanged, the lease was released, and minimum free disk was 12,237,631,488 bytes.
This is forty fresh sampling cases, not an addition to the prior 266-case subset yielding an invented all-current native total.
Repaired Cutover10 on PostgreSQL 16 to 16 passed all ten cases on `1fd03e42`/`2034`, accepting the first checked-close GREEN and a healthy completed retry.
Proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/e2e-runtime/first-close-green-proof.json`, SHA-256 `c5af7f2b8e4c70fdaf6aacb6db0452494c1ea9200f5e163fa8a32f83601ef0a5`.
The fault delivered the same final COMMIT/Idle, independently proved wire-bound PID 286 absent, then observed frontend B/non-X, Pgsql exit 4, and empty stdout.
Authority/native/E/resource/foreign/oracle fingerprints remained unchanged, healthy completed retry was valid, and worker/relay reaping passed.
Old RED remains an explicit ancestor control; the repaired Follow21/Cutover10 matrix now passed all 93 cases on `2034`/`1fd03e42`, including checked-close GREEN on each pair.
The earlier immutable 52-case partial receipts remain historical, superseded by the final matrix proof below.
The same independent reviewer resolved the P2 on `2034`/`1fd03e42`, with no findings remaining and both documentation corrections resolved.
Resolution: `/Users/ydixken/.codex/tmp/m4-independent-closing-review-r_lw2ucz/p2-resolution.md`, SHA-256 `fd2535519a8c0725f156925a4d4942ecffa2432295f6e0668ab202a7cdf15970`; this continues the original review rather than a second whole review.
Repaired-runtime actual-seven passed final verification on `2034`/`1fd03e42` in one uncached invocation, with seven PASS and zero skips/failures.
Receipt: `/Users/ydixken/.codex/tmp/m4-checked-close-actual7-ovf_vw65/runtime-receipt.json`, SHA-256 `2be671b4335e6188df1650ec25c4dd45b3a9a492f6bf55d42321478f48affc27`; sibling `verification-addendum.json` SHA-256 `c8e6d76cf31c61f0c16281ba27e4490fe01db66ad155aa8dc3d577a85eae8587`; final artifact manifest SHA-256 `dab741629b852f3d46bbd3abf55899eac269445d42afc1bfc8285e545c87532c`.
The nineteen-stage helper, both EOFs, empty stderr, closed source session, independent Go ParseReport, and matching same-E acknowledgment/token are proven.
All 28 fixture caps were observed with zero unknowns or violations; all 31 owned objects (29 containers and two volumes) were independently absent, original three objects exact, secrets/driver/observers absent, and privacy scans clean.
Go 546-file/24-staged-path/tree `7442` stayed unchanged; existing Go closing proof `2f6e44ce` was reused without repeated broad suites.
The 42.612-second lease recorded minimum free disk of 12,527,251,456 bytes; qualification remains private PostgreSQL 18 with mocked Kubernetes ownership and `production_admission=false`, not native activation or power-loss proof.
The private orchestration-only static engine census passed pure controls; the overlapping run released its marker before the boundary, with `held=false`, no process signals, and no interrupted case.
The repaired matrix, final actual-seven, engine integration, and source commit are complete; M4 is closed.
Final repaired composite: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/checked-close-supplemental-proof.json`, SHA-256 `8eda6f19049d59e2a7ba9e356610c60a475f332b8fbc1cfb8d78c89d552c4d7c`.
The sibling `e2e-runtime/runtime-matrix-proof.json`, SHA-256 `98c8578b4872582bb97887f3d49abe794757b511911fdb94be757dd0933c1621`, accepts all 93 Follow21/Cutover10 cases with source/binary/libpq identity unchanged.
The sibling `e2e-runtime/cleanup-proof.json`, SHA-256 `83cc6a8cc549d4d65666a6d0253db4ebce45f6a6089edfb341ebc2fb98440897`, proves all thirteen owned containers and the internal network absent, the native lease released, and original stock state unchanged.
Source remained exact at 304 files/fifty absences, and all 954 artifact privacy scans were clean; minimum matrix free disk was 12,502,048,768 bytes and final free disk 12,565,581,824 bytes.
The composite links fresh host 499/27/324/lint/check, forty fresh native samples, meaningful old RED/new GREEN, new actual-seven, and prior 266-case evidence with original labels; these scopes are not merged into a fictitious current total.
The separate 24-path operator test-only work is committed as `c262322909ac03a7101ea3bfac2c4823efa62e95` (`test: qualify native cutover producer evidence`), preserving production native refusal.
The `m4_engine_integration` owner is preparing a private feature tree from `2034` plus final six-doc `23ca0fd`, preserving primary plans, lessons, M7, and current trackers; no commit is authorized by that preparation.
The same reviewer accepted bounded repair inspection and final six-document export `23ca0fd`, with no actionable delta finding.
Addendum: `/Users/ydixken/.codex/tmp/m4-independent-closing-review-r_lw2ucz/repair-inspection-pending.md`, SHA-256 `8296f191004270d15beff407365d6840eb5edc1c26b9e764e55ad57f36a6a395`.
The original P2 is resolved by the same reviewer after reproduced old-runtime RED and verified repaired-runtime GREEN; all retained 266/90/actual-seven results below remain prior-source evidence.

Ancestor `5de` passed fresh host lint/check, 497 Rust tests with twenty ignored and 107 filtered, 27 CLI tests, and 306 portable tests.
PostgreSQL 16 Source12 and Snapshot9 passed, including five fault receipts and three tail phases.
Authority17 produced eleven PASS and six FAIL: empty advancing serial transactions reached COMMIT without BEGIN, before any CAS or proof.
Independent xhigh diagnosis identified serial lazy-BEGIN omission; the active pipeline route already handles it.
The minimal `ensure_transaction` fix and existing-case regression are frozen at `/Users/ydixken/.codex/tmp/rpgmigrate-m4-empty-group-fix-_2fhql12/manifest.json`, SHA-256 `0a5d97e989c56afb971270a86dd52e0d1086fe8e4fafacbe8f17561bb9dc981e`.
The focused synchronization successor passed all seventeen FollowAuthority cases on PostgreSQL 16.15 and all seventeen on 18.6, with zero failed or ignored cases and three fault receipts per major.
Its source manifest is `866981b9176e4e3e6d0abc93872d0e56f64818e707efec57e87a3ae1168e8edd`, containing 304 files and fifty absences, unchanged after qualification.
Proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/sync-cancel-successor/proof.json`, SHA-256 `7fffafc933f3ff71551bf43247b897d79f43f2a6713b0002862884fad9d931b8`.
Only the library test executable was rebuilt; the ordinary runtime was not rebuilt or relabeled, and no broad host, other native/runtime, or Go gates were run for this focused acceptance.
All four owned clusters and sockets were cleaned, the stock configurations/postmasters remained unchanged, and the resource lease was released.
The effective-connection repair plan is approved at `/Users/ydixken/.codex/tmp/m4-schema-effective-connection-repair-plan-20261109.md`, SHA-256 `588dd3398420bb58bc95937bcf8b7fe80e2242e3c536d312a7e020fd6e13f60c`.
The leader assigned disjoint `m4_utility_connection_fix` implementation and `m4_utility_harness` owners.
The three-case Schema profile must reuse the existing TCP/SCRAM fixture; the Rust-owned selected-peer relay must not stop a shared cluster.
The six-file harness export passed four focused harness and nine Task checks, freezing exact Schema3 at `/Users/ydixken/.codex/tmp/rpgmigrate-m4-utility-harness-j7m0gd8j/manifest.json`, SHA-256 `933af15c163ca320c5b656ad64a3530e176687b5d6f3c440b477f4db32743850`.
All 348 other identities and fifty absences remain unchanged; this export does not establish integrated live qualification.
Both approved exports are now verified and assembled at `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/utility-final-successor/manifest.json`, SHA-256 `6a92ea235076e7d5438997203842af512c3ed4e45c67c3e499b69f2c918a1879`.
The inventory hash is `2877328dedfd7cf7d0b221a673303a070b9df2c9d6b6a4a8ccba70a017473cc0`: twelve disjoint changed identities, 342 unchanged identities, 304 files, and fifty explicit absences.
Checkpoint/state inputs remain exact against `866981b9`, retaining native 8 / metadata 6 and 29 columns.
Integration reconciled signatures, capture from the validated SQL owner, child-only environment removals, selected-peer pinning, option policy, and pre-data capture before phase intent without mismatch.
Canonical `task lint test` on `6a92` passed formatting, then stopped at strict Clippy with outer exit 201 and nested exit 101.
Two `clippy::err_expect` findings point to new `src/schema.rs` test assertions at lines 1132 and 1493.
Failure proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/utility-final-successor/proof.json`, SHA-256 `43bb3e9979eff0510d4b8dc421e357511e9e9435d060b2cd581609f5580861a3`.
The one-file `schema.rs` repair now uses explicit Err/Ok matching and fixed redacted panic text, avoiding Debug of a credential-bearing Command.
Its manifest is `/Users/ydixken/.codex/tmp/rpgmigrate-m4-utility-lint-vozx450b/manifest.json`, SHA-256 `87a0b6a46dcab678d7eb2df83439fefd719a3138f770514d9d6195c07c638d9a`; focused proof SHA-256 is `3906113e330a17e9b22c9089d836400b43872c1e11d6fb63c0bf9136b7efaafc`.
The +9/-7 assertion-only change preserves all 353 other predecessor entries and fifty absences; no production or test semantics change is claimed.
Worker `cargo clippy --lib --tests -- -D warnings` passed, along with three utility tests, ten schema tests, discovery, and formatting.
The original `6a92` invocation ran no downstream unit, CLI, portable, check, Linux-build, or live gates and had no live-lease effects.
The repaired successor is frozen at `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/utility-lint-successor/manifest.json`, SHA-256 `87a0b6a46dcab678d7eb2df83439fefd719a3138f770514d9d6195c07c638d9a`, inventory `4bbb5fdb6afe74e851716d03ff76a210deb6473408e1399cc8a7953a4a163133`.
Its canonical `task lint test` passed with exit zero in 62.605 seconds: formatting, strict Clippy, 499 Rust PASS with 22 ignored and 107 filtered, 27 CLI tests, and 308 portable tests without skips.
Canonical `task check` then passed with exit zero in 2.929 seconds; the log canary scan was clean.
Canonical host proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/utility-lint-successor/host-gate-proof.json`, SHA-256 `6ab8f932b9ef3ecd7296f53d782b7a6a08f1fd5a1e3e6bb0de9aaa14bb0563a1`.
The earlier `6a92` failure remains scoped history.
Exact Schema3 passed on PostgreSQL 16.15 and 18.6 with zero ignored cases on source `87a0` and runtime `cadadfbf`.
PostgreSQL 16 Clone7 then stopped at six PASS and one FAIL in `clone_marker_tests::live_markers_commit_atomically_and_refuse_stale_generation_and_foreign_metadata`, at `checkpoint.rs:3637`.
After `authority.resolve_unknown`, the synchronous-wait cancellation helper's synthetic-table id query returned 5 where the fixture expected 6; this is not a metadata-version or column-count assertion.
All six snapshot/COPY clone cases passed; outer exit 201 and library-test exit 101 stopped later profiles, runtime matrices, and actual seven-case interoperability.
Read-only diagnosis identified a stale literal: COPY supplied `b"5\n"`, while the final assertion expected `b"6"`; the otherwise byte-identical qualified primary M3 helper expects 5.
The leader assigned `m4_empty_group_fix` the one-byte test-literal restoration, grounded in that source parity, before fresh Clone7 acceptance on PostgreSQL 16 and 18.
No runtime defect is established; the failed run did not reach or qualify the final guard conjunct.
Partial closing proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/utility-lint-successor/proof.json`, SHA-256 `20a5b74b194d605549c10d4574b2b8fe74d03582f99cb97fa10d8c7ab717e300`.
Immutable Schema3 proof: sibling `schema-profile-proof.json`, SHA-256 `146a963ab3b9a31cfb1b552bb2f546fafef78d9d6bb97c5a9c6d0b22af4a8cc1`.
Three owned clusters and sockets were proved absent, stock configuration/postmasters remained unchanged, the lease was released, and the build runner/network were removed.
Source remained exact at 304 files and fifty absences, with zero URI/password/canary scan hits.
The one-byte test-only restoration is frozen at `/Users/ydixken/.codex/tmp/rpgmigrate-m4-clone-marker-literal-3zh36foz/manifest.json`, SHA-256 `04d53f79a22433e4a6cb9c18eb299037387cd47028aae373310eb0ac66a78b22`, with 353 other entries and fifty absences exact against `87a0`.
Fresh Clone7 passed all seven cases on PostgreSQL 16.15 and all seven on 18.6, with zero ignored cases and the final marker guards exercised.
PostgreSQL 16 Catalog13 then passed; the intended Engine50 selection expanded to 55 cases and stopped at 49 PASS and six FAIL.
Five catalog-follow cleanup tests lacked Engine context and were wrongly included; the one intended `async_barrier` failure at `checkpoint_live_tests.rs:795` expects the message to contain `fsync on`, while the actual message was not emitted in the failure.
The leader stopped dependent profiles and runtime matrices, assigning read-only exact selection analysis across all remaining profiles to `m4_utility_harness` and assertion/M3-lineage analysis to `m4_clone_marker_diagnosis`.
These are separate findings, not six established engine defects; the pause is a bounded test-lineage/selection replan, not a whole-code audit.
Read-only lineage analysis proved that the stale `contains("fsync on")` assertion omitted the qualified M3 combined-guard correction already recorded above; the actual runtime message was not captured.
The leader assigned `m4_empty_group_fix` the exact M3 assertion restoration, preserving M4 metadata adaptation and the empty-group regression, without runtime changes.
The selector repair is frozen at `/Users/ydixken/.codex/tmp/rpgmigrate-m4-profile-selectors-y62fxrtl/manifest.json`, SHA-256 `e9326b7561600782177f65b4e19c9a87a2de6f1c3a09ebb601590ac65abe22ed`.
All fourteen actual Task selectors match their exact inventories; the ten closing profiles are pairwise disjoint across 45 checks, preserving Engine50 and restoring FollowReceive11 from the old four-case selection.
Thirty-nine focused Task and two receipt checks passed, with proof SHA-256 `f92ffe695733d76195cadf507b14bec1cd0f5434a79dff02221402e5810b0130`.
The exact M3 assertion restoration is frozen at `/Users/ydixken/.codex/tmp/rpgmigrate-m4-async-assertion-z7mlwzds/manifest.json`, SHA-256 `a33bd3c2a5ed5862cd4c05345fc1465eea80958f933a74c52e492daca3d472d2`, with focused strict Clippy, 27 checkpoint tests, and discovery passing.
The three-path successor is frozen at `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/profile-resume-successor/manifest.json`, SHA-256 `96e1fbbbd1e9c6e37b605bfcd6e43da8c81dfbc21c4dd2f7aa6a1ca6afdc8234`, inventory `18f099200e706cf165fc9a4f00ba8ab9e744aa9c6b5e638c1d62e63ee97b1916`.
All 351 other predecessor entries remain unchanged, with 304 files and fifty explicit absences; the ordinary runtime remains `cadadfbf` under the test-only projection.
Exact Engine50 passed on PostgreSQL 16.15 and 18.6 with zero ignored cases; Catalog13 passed on PostgreSQL 18, supplementing the earlier PostgreSQL 16 Catalog13 PASS.
PostgreSQL 16 Source12 and Snapshot9 passed, including five snapshot fault receipts and three tail phases.
These five stages were partial results only; the nine-profile closing-subset acceptance below supersedes their pending-profile status while preserving the immutable partial receipt.
The active lease recorded minimum free disk of 12,135,612,416 bytes with no watchdog violation; no final lease-cleanup claim follows before release.
Immutable five-profile receipt: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/profile-resume-successor/native-progress-five-profiles.json`, SHA-256 `72e7cd943e56c4bac6882f3b9b4fec0266ea53bcc64f84ac644d400dec91ed13`.
The sibling `production-input-projection.json`, SHA-256 `14e67af740c02c430ce3fe25417446df45e9b46ab0a4f1adec131d2ee3b92bd9`, retains the unchanged ordinary runtime and prior host/Schema proof; later profiles are excluded from this frozen five-stage result.
The leader independently authorized one actual seven-case interoperability run on unchanged `cadadfbf`/`87a0` production inputs and the `04d` test projection, with unchanged Go tree `7442e438270f2d3ae47317447f7f07299fcf5322`.
That run retained its separate four-CPU/two-GiB cap and joint safety floor and passed one uncached invocation with all seven exact cases and no cleanup error.
Its receipt is `/Users/ydixken/.codex/tmp/m4-current-actual7-37r9nd_1/runtime-receipt.json`, SHA-256 `92e8cf628a79adab7224660ed5c59fd8554def146679fe5636bae7798e81f266`.
Final verification passed at sibling `verification-addendum.json`, SHA-256 `d94fd59b5e6c5a0c852de1c499f0109cf98fddf2f8fe92ea620d4364d02b2833`, with artifact-manifest SHA-256 `57aa03030cd8e16e01c69871ec7001b36df56038e6b5ab867a519284809b424a`.
The positive case proves actual COPY/follow, a distinct fresh UID 65532 helper with nineteen ordered stages, both EOFs, empty stderr, closed source session, independent Go ParseReport, and a matching same-E drained acknowledgment/token; all six negative controls passed.
All 28 container caps were observed with zero unknowns or violations, all 31 owned objects were absent, Go 546-file/24-staged-path tree `7442` stayed unchanged, secrets were removed, and credential/canary scans passed.
This single 38.948-second lease recorded minimum free disk 12,533,616,640 bytes; its scope is test-only PostgreSQL 18 interoperability with mocked Kubernetes ownership and `production_admission=false`.
No retry or repeated broad Go gate was run, and this result does not close M4 or activate production M6.
Clone7 acceptance proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/clone-marker-successor/clone-profile-proof.json`, SHA-256 `25f2f6ae4b9056dedae7fefa32cf8be91ff2de0c0f76b65dd7219efc496d17b0`.
The sibling `production-input-projection.json`, SHA-256 `c950b9418cc670bf5987e2420eabc483baacfdaa929765183124c586e8501268`, proves the one-byte cfg-test-only change and preserves the exact ordinary runtime `cadadfbf24eaab1d621a846d1a25e6a77dff5d4ce7d6db121b24b90767fd3d6c`, prior host proof, and Schema3 proof.
The sibling final `proof.json`, SHA-256 `c3ac7badb30f8116e071b30e21e56a93f041abb9432f998cb99ae3ab70861fca`, binds Catalog13 PASS and the unqualified Engine55 selection result.
All six owned clusters/sockets were proved absent, original stock configuration/PID1/postmasters stayed unchanged, the lease was released, source remained exact at 304 files/fifty absences, and privacy scans were clean.
Strict lint has no staging-policy exception.
TLS proof remains option-byte and selected-hostname/address mapping with TLS disabled; no TLS handshake, client-certificate, or encrypted-key qualification is claimed.
The nine-profile M4 native closing subset passed on `96e1`, combining 233 fresh named cases with 33 retained Schema3/Clone7/Catalog16 cases for 266 total, 133 per major, with zero required skips.
SourceCatalog3 is a separate registered retained M3 gate; its fresh repaired-input acceptance on both majors is recorded above, separately from this prior nine-profile subset.
Authority17, Receive11, and Apply11 passed on PostgreSQL 16.15 and 18.6; Receive16 ran 72.98 seconds in the test binary and 74.361 seconds total, while Receive18 ran 73.03 seconds and 74.497 seconds total.
Both required 70-second cadence controls completed without interruption; Snapshot9 retained five fault receipts and three tail phases on each major.
Immutable native closing proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/profile-resume-successor/native-closing-proof.json`, SHA-256 `d779e8252d87bcd4b2a0ec331ef767a6431084f403e510b1a547fc95e26938f5`.
All nineteen owned clusters and nineteen owned sockets were proved absent; original stock configuration, postmasters, PID1, and socket inventory remained unchanged, and the lease was released.
Minimum free disk was 12,135,612,416 bytes and peak RSS was 960,483,328 bytes, with zero watchdog violations.
The first current runtime pair, PostgreSQL 16 to 16 Follow21, passed all 21 named cases in 189.99 seconds on unchanged `cadadfbf` with the final `96e1` projection.
Its fourteen original startup reports were validated by `local_m3_contract`; this is not Go/probe validation.
Proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/profile-resume-successor/e2e-runtime/follow-16-16-proof.json`, SHA-256 `23da896ec9430f64acb0db18118b2db4113b730224f77fb4ca8a2f5528ecdc80`.
Both owned PostgreSQL 16 server containers were removed and proved absent; minimum free disk was 12,506,144,768 bytes and peak aggregate RSS was 868,581,376 bytes.
The current PostgreSQL 18 to 18 Follow21 pair then passed all 21 named cases in 192.31 seconds on the same `96e1` source and `cadadfbf` runtime, with fourteen original reports validated by `local_m3_contract`.
Proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/profile-resume-successor/e2e-runtime/follow-18-18-proof.json`, SHA-256 `813623a87ece5b3c1e45a8793bec3172a84f2861a781f06014e2497918c8a5da`.
Actual PostgreSQL 18.6 image/tools and exact source inventory were verified; owned server absence was proved, while the runner remained owned for later pairs.
Minimum free disk was 12,545,638,400 bytes and peak aggregate memory was 879,013,888 bytes, without watchdog violations.
The remaining cross-major Follow21 pair and all three Cutover9 pairs passed, completing 90 of 90 declared runtime case executions on exact `96e1` source and `cadadfbf` runtime.
Cutover16-to-16, 18-to-18, and 16-to-18 took 26.04, 25.89, and 25.91 seconds respectively, with sixteen original streaming/completed reports per pair under `engine_producer`/`local_m4_producer` and `probe_checked=false`.
Follow reports retain `local_m3_contract` validation; neither producer matrix is Go/probe evidence.
Final runtime-matrix proof: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/profile-resume-successor/e2e-runtime/runtime-matrix-proof.json`, SHA-256 `50548ecac0b7ab9536dfad472b67c576111c0d9efe76d078c4ec35b3fb08233f`, accepts Follow63 plus Cutover27, all ninety cases, with exact source/runtime/libpq bindings.
The sibling `cleanup-proof.json`, SHA-256 `4e787dbee1124f9fb0dacf857508f0b65db78e60622551a51a3d4d513c853570`, proves all twelve servers, runner, and network absent, original stock state unchanged, and the lease released.
The aggregate parent `proof.json`, SHA-256 `9257058b6ff1cae83eeda88672b70dfec45fa5ef4bb55794a2d990ac5445dde1`, preserves exact 304-file/fifty-absence source and binary/linkage identity after all gates.
All 411 runtime artifact files passed privacy scans with zero canary hits; matrix minimum free disk was 12,506,144,768 bytes and peak aggregate memory was 880,824,320 bytes.
The single independent whole-M4 xhigh review finished with one confirmed P2: the completed-resume branch in `src/follow.rs` obtains `CompletedFollow` but omits consuming checked `TargetAuthority::close` before reporting success.
The synchronous completion proof is not invalidated; the missing guarantee is observed shutdown guard/unlock/local-disposal failure handling.
No other actionable source finding was identified, and no earlier per-task independent reviews occurred.
Review: `/Users/ydixken/.codex/tmp/m4-independent-closing-review-r_lw2ucz/review.md`, SHA-256 `dc87ac4c075b6109a02f0423a7391d5691cff0a0935946648cabd54a24548295`.
The leader approved the now-joined checked-close plan at `/Users/ydixken/.codex/tmp/m4-completed-resume-close-plan-bbee5qk2/plan.md`, SHA-256 `d627e7fe79a1d72e814fa0ae2a3653e0937bc9e86485737ccb0f2871c3f56e4a`, and its disjoint source/test exports are joined.
The one-line source export is `/Users/ydixken/.codex/tmp/rpgmigrate-m4-resume-close-do6apchb/manifest.json`, SHA-256 `5097b2bcfc802814f680a5f390fa9e4323c4ec26c06bba4d6aff3e2679a1652d`; focused strict Clippy, library compilation, and 23 root-follow tests passed.
Separate test owners implement the test-only wire mode and `completed_resume_requires_checked_authority_close`, without production hooks or relaxed guards.
The new case extends Cutover9 to Cutover10, not Follow21 to Follow22; all 93 repaired three-pair cases are now qualified.
Planned qualification requires meaningful RED on the old runtime, new source/binary identity, SourceCatalog3 and FollowAuthority17 on both majors, the new 93-case matrix, and actual-seven interoperability on the new binary.
The same-review P2 is resolved as recorded above; pending matrix and final actual-seven evidence remain separate milestone gates.
Existing 266-case native, ninety-case runtime, and ancestor actual-seven evidence remains accepted on `96e1`/`cadadfbf`, separately from the final repaired-input proofs above.
The `m4_documentation_reconcile` owner reconciled exactly six behavior documents from current primary, preserving M3/M6 amendments and lessons and stating that cleanup requires both source and target.
The same review requires the nine-profile inventory distinction above and narrows unsupported-option refusal to before pre-data intent, without claiming refusal before all prior effects.
The final six-document export `23ca0fd7872b8624233af6ad88a13ee9421496820f51d29a2bef94f671434acf` preserves current primary and corrects intent, subset, and Cutover10 wording; it claims no new runtime result.
The integration plan now identifies 55 source effects plus six behavior documents, 61 paths total, preserving primary trackers/plans/M7/lessons for one coherent engine commit.
The separate 24-path operator test-only commit is recorded as `c262322909ac03a7101ea3bfac2c4823efa62e95`, without activating production native paths.
The P2, documentation corrections, separate operator test-only commit, and 61-path engine integration/source commit are complete; M4 is closed.
Primary `feat/native-engine` is fast-forwarded to `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476`, tree `4e9a151afa82c039aa2bb2c705fded6d358d5d4a`.
Fast-forward proof: `/Users/ydixken/.codex/tmp/rpgmigrate-m4-engine-integration-hiifg12a/fast-forward-proof.json`, SHA-256 `e02a7e60892fa1f52a8bdfa4e5c26f80f54b3be06f837b0f7bc210d03a1c2c13`; sibling `postcommit-proof.json`, SHA-256 `ddb8da4c816f8655459d53266c3f5258e29ed58997ccd03005dade159ff9e8f4`.
The integration verified 285 source files, six accepted documents, fifty explicit absences, and 244 protected files; only `src/apply/serial/tests.rs` changed from private mode 0600 to repository mode 0644 with identical bytes.
The operator test-only commit `c262322909ac03a7101ea3bfac2c4823efa62e95` remains separate; its commit proof SHA-256 is `74d2e8263f7552025923cf18c3490c227485a910b1a98a8c570e945a04ca2ac4`.
Source integration preserved primary trackers/plans/M7/lessons and kept the index empty; this final tracker closure does not change source or production operator admission.
The nine-profile native closing-subset qualification and actual-seven interoperability remain accepted within their separate explicit scopes.
The earlier `ef4` producer 27-case PASS and `134`/`d75` actual seven-case Go interoperability PASS below are ancestor evidence, pending fresh current-code matrices.
Prior-source/current-Go actual-seven acceptance retains its test-only scope; final repaired-input qualification and same-review resolution are complete as recorded above.

### Ancestor M4 evidence

The leader accepted the revised [M4 plan](../docs/superpowers/plans/2026-11-08-m4-cutover-control.md) design and contracts conditionally on the exact final M3 hash/interface/version freeze.
On 2026-11-08, isolated M4 build work started from the exact `2b4` donor; authority, control, receiver, and apply owners use xhigh effort.
The frozen seven-file authority candidate passed 125 state, 26 metadata, 12 checkpoint, and 12 pure follow-authority checks; its 17 live cases have discovery proof only.
Its manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-authority-candidate1-dl3azsfz/manifest.json`, SHA-256 `6f5b496b1e34997070b17b5bb35e600597ed1cb23e9b409ba1701a40d05c4483`.
The frozen control export passed 18 pure checks including reopen/seal; strict lint awaits consumer wiring.
The three-file receiver export passed 11 receiver, five transport, and five channel checks plus strict library Clippy, without PostgreSQL or new live qualification.
The coordinator passed 23 actual focused checks; the final two-file apply export passed 14 pure checks and is frozen at manifest SHA-256 `f579cd38e1a096d1e05bb14a0f24bfaaca0328b3af34f9aaa791139d9c75fe4f`, included in the later assembled candidate.
The authority empty-receipt guard repair passed four cleanup and 12 follow-state checks in successor `db2d1006`, without database qualification.
All owner exports are assembled at `/tmp/rpgmigrate-m4-guarded-integration`, covering 304 files and 48 absences, manifest SHA-256 `8fb8750a1d28e9854af94caaaab486dfb1fab84967aaf0f13e0f24cfea392d1f`.
Formatting and all-target check passed on the assembled source; strict all-target Clippy failed on three authority unused/obsolete-expectation diagnostics.
The owner removed the unused `DurableFollowSnapshot.request_floor` field/accessor and obsolete expectation only, preserving `read_endpos_floor` and first/changed-publication veto semantics.
The repair export SHA-256 is `3a06813e4d78ecc1304b3bf6ec0cf2b16fa768eb3759506dc43e11c7880046d23`; 12 focused authority checks passed, with 17 live cases compiled only.
The changed-input assembled candidate then passed formatting and `cargo clippy --all-targets -- -D warnings`, both exit zero, with all 304 files and 48 absences unchanged.
The frozen partial manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-candidate-static3-_55101kj/manifest.json`, SHA-256 `efa9e8646e06bec55300ae092359ee5d51275397f8c8fbbac382a2bc4d07db80`.
Static provenance is `/tmp/rpgmigrate-m4-integration-ledger/rust-static3.json`, SHA-256 `bd7192d4cb9e4ebc2734da23209dd2347c56ce6b0685d6260d3c5164f9d80964`.
The approved plan-only successor `1367c9ad` changes no Rust input and requires no repeated Clippy run.
These isolated M4 checks do not qualify runtime, Go producer proof, E2E, or M4 closure.
Catalog13 discovery passed after adding two ignore attributes in `src/catalog.rs`; the successor manifest is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-candidate-catalog13-scer5mea/manifest.json`, SHA-256 `948ba09a37baa0065ddb7010af7d752a4eaa66e872f03d40266ef22745bbe526`.
Rust bodies retain the prior static-green evidence; the attribute-only successor has no database qualification.
Completed-positive Go validation depends explicitly on M6 independent reader evidence; old clone probe `f06` cannot accept EndLSN, Checkpoint, and Resources, so producer checks alone do not pass that Go gate.
The five-document export checked 29 DDL cases, 30 links, and 22 Task references; cleanup passed three cleanup and eight catalog pure checks, with five live cases compiled only.
The M4 harness is frozen with 217 focused checks passing.
The full candidate is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-harness-final-xpv6a7le/candidate/manifest.json`, SHA-256 `6b63e0a9bff0bd33a5798a51cedd41feb0baccea4b7d560950455aecd87b221f`, covering 306 files and 48 explicit absences.
The offline native-8 build passed against integrated source `9158fef5601fa3b9c3d7c5c0c1223a761071a0aafce8d9e635ca207f79edb155`.
Its binary is SHA-256 `c185bedf97c021558092b0930815ff7b17fcc9e85c5a666552bb779607af2187`, linked to libpq 180006.
The private capped tool runner and owned internal network are approved for serial nine-case producer runs on three pairs, starting with PostgreSQL 16 to 16; no runtime qualification result is claimed.
The test-only PostgreSQL 18 M6 interoperability PASS leaves the required PostgreSQL 16/18 helper qualification, broad closing checks, and whole-M4 safety review open.
The first actual PostgreSQL 16 to 16 producer attempt failed the active-fence assertion and qualified no cases at that input.
Failure summary: `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-producer-integration-6z_7deja/runtime/failure-summary.json`, SHA-256 `43806493f8f06570b34c536961966c4d003389e8b071b89f4711829a660f8f07`.
The later retained sentinel result is CLI `BadArgs` exit 1, caused by trailing-capture parsing; post-teardown worker Target7 is not causal evidence.
All three exact owned containers and the network were proved absent.
The two-file retention repair is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-fence-retention-an15cuee/export/manifest.json`, SHA-256 `e5404197bf902d72aa3b99875ea163a12f2ef83210232aa3663b37657c61fcee`.
Its 93 focused checks pass.
The one-file CLI correction `39b791fd` is joined and rebuilt in candidate manifest `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-parser-linux-u77pw5bk/manifest.json`, SHA-256 `ee2aa26dcf86ca48a80dd50360698290754fe6b1556171dc1bcf445fcf7e6cb8`.
The Linux binary is `b9e8f6c424ca9deda8237e5363dc22d281661f1d4d8050e527a5e01655dad5f0`.
All 31 Linux parser tests and the database-free explicit-E/after-E-JSON check passed; host formatting and strict Clippy passed.
All 27 engine-producer cases then passed on the same combined M4/M5 input: nine each on PostgreSQL 16.15 to 16.15, 18.6 to 18.6, and 16.15 to 18.6.
Source manifest: `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-m5-source-q2vjbxdx/manifest.json`, SHA-256 `8bf95549278eafacc60ccc3498b43d0396335a0cc1d18370170d9cf44d8ec312`.
The binary is `ef4ece807906345e3be6645833d1060c11aab3be3bf607ed1699c2795f31840b`.
The sibling `runtime-aggregate-proof.json` has SHA-256 `9c92e1f905e55546938dab9f7fc2e60ae34419eede69117897556f362aa0d9e9`.
Source/binary/linkage/credential checks passed, and all nine owned containers, three networks, and the builder were proved absent.
The scope is `engine_producer` with `probe_checked=false`; independent M6 Go qualification and M4 closing review/gates remain pending.
The harness peer passed six portable checks and registered cases, without actual M4 E2E.
The frozen eight-file CLI export passed 31 adapter/parser checks only; no assembled-source or runtime result follows.
Those 27 cases qualify their ancestor engine-producer input; the current closing gate above supersedes any claim of current-code runtime qualification.
The 2026-11-08 [M4 plan](../docs/superpowers/plans/2026-11-08-m4-cutover-control.md) clarifications freeze endpos/frontier handling, fresh reproof, durable final sealing, guarded closure, and boxed proof without lint allowances.
The plan API text now omits the unused snapshot-floor accessor and refers to the plain publication floor at sealing, preserving the unfenced admission-veto contract.
The reconciled plan has 490 lines and SHA-256 `1367c9ad707381e36032cb203346e19406c78ba60f072dcf9ecde5dd1e1d6e8b`.
The M4 version sentences now use the planned native 7 / metadata 5 identity-fix baseline and conditional native 8 / metadata 6 successor; neither format is qualified by this documentation update.
No informational JSON may expose unproven certified or durability counters; local endpos reads are allowed, while replay, drained, and proof reads require positive fenced observation or bounded explicit refusal when busy or unknown.
The native operator must sample and persist one E, then retry that identical E explicitly after a bounded refusal.
The worker processes persisted E independently of acknowledgment; the M6 standalone helper retries identical E for final positive proof after the worker exits.
We add no IPC or fence handoff for this path.
The qualified runtime uses native 8 / metadata 6 with 29 control columns and strict older-format refusal.
Exclusive slot administration is an accepted prerequisite; uncertain incarnation or reappearance requires preservation and refusal, without an invented slot OID or new privilege mechanism.

- [x] Retain the ancestor 27-case engine-producer PASS across all three pairs.
- [x] Pass the exact seventeen-case FollowAuthority profile on PostgreSQL 16.15 and 18.6 for the focused repair successor.
- [x] Pass canonical host `task lint test` and `task check` on the repaired utility successor.
- [x] Pass the exact three-case Schema profile on PostgreSQL 16.15 and 18.6 for the repaired utility successor.
- [x] Pass Clone7 on PostgreSQL 16.15 and 18.6 after restoring the source-parity marker-test literal.
- [x] Pass exact Engine50 on PostgreSQL 16.15 and 18.6 after the selector and M3 assertion restorations.
- [x] Pass the nine-profile M4 native closing subset on PostgreSQL 16.15 and 18.6, including uninterrupted Receive11 cadence coverage.
- [x] Retain all ninety reviewed-ancestor Follow21/Cutover9 runtime case executions across the three declared pairs.
- [x] Pass fresh checked-close SourceCatalog3/FollowAuthority17 sampling on both majors: forty cases, zero ignored.
- [x] Pass the first repaired Cutover10 pair on PostgreSQL 16 to 16, including checked-close GREEN and healthy retry.
- [x] Qualify the checked-close successor, including SourceCatalog3/FollowAuthority17 on both majors, the new 93-case matrix, and actual-seven interoperability on its new binary.
- [x] Record the separate 24-path operator test-only commit `c262322909ac03a7101ea3bfac2c4823efa62e95`.
- [x] Close M4 at engine source commit `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476` after verified integration and the separate operator test-only commit.
- [x] Pass fresh current-code runtime matrices and actual engine/Go interoperability, then complete M4 closing review/gates.
- [x] `stream sentinel set endpos <lsn>|--current` and `get --endpos|--replay-lsn|--json` per decision 6.
- [x] Drain certification, final sequence sync, exit 0 only with proof.
- [x] `stream cleanup`: owned slot and publication only, idempotent, workdir kept for later compares.
- [x] Engine E2E: full follow migration under pgbench load (including `synchronous_commit=off` writers), endpos, drain, compare match and cleanup, plus a crash in each phase, on 16→16, 18→18 and 16→18.

## M5: Packaging and engine CI (starts with M2)

Local native ARM64 image `b4631063096125a84d3428571fe81d7057c91cf3356adf54cc738744709e52b0` passed the retained build/read-only runtime smoke on exact input digest `296279da95b924a59a903034a89f30853d037039368026859ce5d6ecc6eb9d9a`.
Proof: `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m5-runtime-px1st198/portability-repair/final-proof.json`, SHA-256 `dffc2afd4f08e9265d5fcb34557c44ca485c95eec704f6643a5827fbebea1797`; artifact manifest SHA-256 `73c14c31c42754959252d2e3046b77006b1ff29598ef05b4bbed290b49f217da`.
The final same-review M5 acceptance record is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m5-runtime-px1st198/bounded-portability-delta-review-20261109.json`, SHA-256 `7ec878e14647b3d0e2cd63a26abc0e5b11fe9ef600a313a5389520e60496ab27`.
Its test-only R2 correction is outside the 65 input files, so the retained image needs no rebuild for that change.
Engine feature SHA `34208b6a4bd66d420e506b27bfc12524f8d7ffb1` was pushed; [hosted run 42625499630](https://github.com/ydixken/rpgmigrate/actions/runs/42625499630) is terminal with five passing and three failing jobs, including all three migration pairs passing.
The docs MD038 failure was fixed by one-file commit `55ea9c74216c79663fe04eef585ecef265e65386`, without runtime input changes.
The Dockerfile heredoc-builder failure and version-dependent test were repaired in the same M5 review and integrated as `7c04dff673f41b3a27c5d0afbc6dbe5e6b551c03`, tree `3f1ae57386d513fd4c6d866e83df5b80e9e5df25`.
Integration proof: `/Users/ydixken/.codex/tmp/rpgmigrate-m5-portability-integration-369jdae7/postcommit-proof.json`, SHA-256 `a3b421de7c501cd6379da9146d754e5730eed1e1d1c7b36fdafd846a22a50833`.
Exact `7c04dff` was pushed normally to the feature branch and remote-verified; [hosted run 42630883414](https://github.com/ydixken/rpgmigrate/actions/runs/42630883414) passed all eight jobs from the push event, without manual dispatch.
Verified receipt: `/tmp/rpgmigrate-feature-ci-preflight-20261109/engine-7c04dff-push-ci-receipt.json`, SHA-256 `a5c2e882e4d3b4811e6dc2de2279e1b0b9941c3c76f4c9457be5db32d3653ec4`.
All five artifact ZIPs match their GitHub digests; both image receipts bind the exact 65 Git blobs to input digest `296279da95b924a59a903034a89f30853d037039368026859ce5d6ecc6eb9d9a`, uid 65532, and stock libpq 180006.
Hosted AMD64 image is `e545dff41f51892da0802feb1be12da35b4ad0c7571168165df1d9d9f830ee01`; hosted ARM64 image is `9948a8df526c0259a0cf1c834b2ee5c0f9015c58a064f924a5bcf9ae2093bc57`.
M5 is complete after the same reviewer’s final acceptance of hosted multi-architecture, read-only runtime, source/artifact, clone-inventory, and retained stock PostgreSQL 16/18 evidence bindings.
The retained database evidence preserves its original 266-case subset and forty-case sampling/composite provenance; exact source/Cargo/build/Taskfile/test/harness bindings are unchanged from `67f7fcb` to `7c04dff`.
This acceptance claims neither a fresh aggregate database run nor migration using the production image; broad operator Go checks are not applicable to engine-only packaging.
The operator-publication P1 is resolved by the dated explicit GitLab mirror authorization; it remains separate from engine M5 qualification.
Tags and releases remain unauthorized.
The dated M7 amendment permits only development GHCR publication, and these checks do not close M10.

- [x] Runner image: rpgmigrate, sh, psql 18, PGDG client tools 16 and 18 under `/usr/lib/postgresql/<major>/bin`, GNU coreutils and sed, uid 65532, read-only rootfs, multi-arch, smoke test.
- [x] GitHub Actions: lint, test, cargo-deny (licenses, advisories), docs lint, development-image clone jobs on hosted runners, and both native image builds.
- [ ] Release GHCR publication on version tags only, pending separate release authorization.
Development publication follows the dated M7 amendment.

## M6: Operator native adapter (operator, on a branch from `feat/rpgmigrate-native-operator` after a fetch)

### Evidence checkpoint, 2026-11-10

Production native admission still returns `UnsupportedEngine`; no actual native operator workflow is qualified.
The accepted operator source checkpoint is signed `93bd54c557d70ed2daa7be3120ea093b31cc93cf`, tree `a0739a53ca5edcc816e1904bdb87df3b6bb3a691`; its conditional fast-forward from `cf22e84` verified a clean authoritative checkout before and after integration.
Five signed completion commits add helper receipts (`30686fa`), evidence retention (`50be291`), cleaned completion (`d937958`), follow/E/ACK dependency composition (`d87c920`), and plain-clone completion/verification (`6a27266`).
Each prefix compiled and passed scoped lint with zero findings, with byte-identical qualified final-tree evidence reused where recorded.
Completion checkpoint proof: `/tmp/m6-completion-wave-checkpoint-20261110/manifest.json`, SHA-256 `860652704912ecd72cf14a6d912e8bbc86d74c41bc5fddbfab437755f966eb40`.
Sampler publication and metrics then entered signed checkpoint `f8214a6` through an exact sealed seven-file relocation into an isolated worktree, preserving all 662 tracked peers.
Its source-equivalent 73 named focused race checks passed without failures or skips; fresh compilation and scoped lint passed with zero findings.
Sampler proof: `/tmp/m6-sampler-checkpoint-integration-20261110/checkpoint-manifest.json`, SHA-256 `a1835132bd9fd90de693cd2dcfdaca85ef587f338f121a7f70c213f158c7407e`.
These are source/component checkpoints; no broad Go suite, whole-M6 review, actual SQL/Kubernetes/native operator workflow, or runtime qualification follows.

The root-approved completion-rejoin amendment requires separate partial-cleanup admission for flags `00`, `10`, and `01`, with actual target-row completion authority and completion-only credential-free version discovery.
Ordinary active admission and the fully cleaned predicate remain unchanged; status and resource inventory cannot authorize completion.
Private plan: `/tmp/rpgmigrate-m6-completion-rejoin-plan-20261110/2026-11-10-m6-completion-rejoin-amendment.md`, SHA-256 `1b4dfa9c7051a4037c92be38520d09bbcf4ce52f0e8a7e3f6d2fe5d9fd294dbf`.
The 46-path completion/rejoin/finalization/disposition join is signed in API checkpoint `3ef73545c9f074e1a50cd2c042132977ef170055`, native checkpoint `1d0d67b5001aae48774998f8f4bf5861e5fbceea`, and controller checkpoint `0346f9e8534174243f2d07470afcec5d8275e6de`, tree `1eb28adac4088756694e56006edf3240fbb6a8d0`.
Its actual production/controller builds, four-package compilation, 807 named/154 top-level focused race checks, and scoped lint passed with zero failures, skips, or lint findings.
Joined proof: `/tmp/m6-joined-integration-20261110/preceding-source-manifest.json`, SHA-256 `9c41fcd8591b4b5b04ec64a109ec9ec30990f19ae14c7c44fcc840b08ab5cb5b`.
Qualified checkpoint proof: `/tmp/m6-joined-integration-20261110/preceding-qualified-manifest.json`, SHA-256 `763e07330ce704bddbe0097fe0d4f287ef7c6d5ca0bd9a0c37f666517fc7281f`.
Authoritative integration proof: `/tmp/m6-joined-integration-20261110/preceding-authoritative-ff-receipt.json`, SHA-256 `2f086dfe7a569725792b0e005b96e476a090e88701e10aa014fd1e98ec45e807`.
The five-path main dispatcher is integrated in `cf22e84`; its actual production/controller builds, 71 focused race checks, and scoped lint passed, with production admission still closed.
Dispatcher integration proof: `/tmp/m6-joined-integration-20261110/final-five-checkpoint-ff-receipt.json`, SHA-256 `10968034253cf27950f53c8a63d406ab1ae65daa07f49f8d1285acf5d485d99e`.
The finalizer successor includes the production constant-build repair and stale-failed-condition repair; its actual production build, 39 affected race checks, and scoped lint passed with zero findings.
Its older 335-race-check packet retains separate provenance and is not relabeled as qualification of the repaired successor.
Finalizer proof: `/tmp/m6-clone-stale-failure-repair-export-20261110/manifest.json`, SHA-256 `3dfc5322c44bb678da6d966621a2b91a495bc86590099ce0646d2bbf75b1525c`.
Dispatcher source integration does not qualify terminal replay after helper disposal; exact-UID disposal and terminal replay still require composed lifecycle qualification.
The root-approved interface freeze retains atomic known-create `nativeJobUID`/`nativePVCUID` receipts, refusal to adopt absent receipts, invocation-bound Pod identity, worker TTL retention, and independently observed quiescence after TERM.
Native evidence-bearing helper retention is checkpointed; exact-UID disposal and terminal replay still require composed lifecycle qualification.
Native runner-profile Task 1/Task 2 packets are frozen with passing component checks; actual NSS, authentication/TLS, PVC/provider behavior, and canonical runtime admission remain unqualified.
Canonical NSS/resolver guards and `UnsupportedEngine` remain closed; production runner-profile qualification, three-pair native acceptance, broad Go gates, and the whole-M6 review remain open.
Actual Kubernetes TERM and the full native lifecycle remain unqualified.

Canonical policy preparation is integrated in `93bd54c`: retained resolver arguments and nonempty `sslkeylogfile` refusal passed 90 Python tests, eighteen native race checks, five focused controller checks, builds, and scoped lint.
Its resolver policy still unconditionally refuses; source integration grants no runtime activation.
A separate active-CRI metadata candidate replaces disk-dump authority in source work only; no positive authentication/runtime proof or NRI/configuration change follows.
Preparatory proof: `/tmp/rpgmigrate-m6-canonical-policy-prep-export-20261110/manifest.json`, SHA-256 `ddb33db698f06a018db125f3b421c7c5e75a2f12fabc96a219f1912853377e24`.
Joined checkpoint proof: `/tmp/rpgmigrate-m6-policy-joined-checkpoint-proof-20261110.json`, SHA-256 `3b13946d655e373e025ae199f93826116189fcca05073610e3a7f268da20472b`.

The bounded actual metadata probe refused before Jobs: `containerd config dump` exited zero with 394 stderr bytes, and the frozen metadata policy rejected any stderr before parsing.
Exactly one owned namespace was removed and verified absent; no Jobs, Pods, or Secrets were created, and authentication/TLS Step 2 remained unstarted.
A later read-only taxonomy found both a version-migration warning and an ignored unknown TOML plugin-key warning; a blanket stderr exception is not approved, and effective NRI disablement remains unproven.
Taxonomy proof: `/private/var/tmp/rpgmigrate-m6-independent-live-dg9wwby9/readonly-warning-taxonomy-20261110/taxonomy.json`, SHA-256 `1d21a49bf6560b884bdad00607baa5381a63242788ccf3e9156dc2166c7f4f2b`.
Cause proof: `/private/var/tmp/rpgmigrate-m6-independent-live-dg9wwby9/readonly-node-metadata-cause-20261110/final-index.json`, SHA-256 `10fa3a9cadbaabf201dc34b8a6dc767b67f23b421bf014612904ca99f570cde8`.
The separate deadline-composition candidate preserves the original parent clocks and requires 3300 seconds for cold admission or 2400 seconds with a complete same-source receipt.
It passed 27 host tests, 32 focused Go race checks, and scoped lint with zero findings; it performed no live action and grants no metadata, authentication, or production qualification.
Deadline candidate proof: `/Users/ydixken/.codex/tmp/m6-deadline-composition-packet-20261110/manifest.json`, SHA-256 `98c9ee94e60c6f10aa3434e9b8c2e47696fa48cfa87f518481935bf031a67030`.

The preceding `aaad6c7` lifecycle checkpoint added identity receipts/worker TTL, retained TERM primitives, and bounded clone capture/parsing, with 288 named focused race checks and nineteen final-proof-join race checks.
Prior lifecycle proof: `/tmp/m6-lifecycle-components-integration-export-20261110/checkpoint-manifest.json`, SHA-256 `15fc47e882e9863980e3440b716afc8f13e5d3e4f1a3b6db64071f26baac75f7`.
The earlier follow/E/ACK/target-join export passed 81 focused race checks and zero scoped lint findings before its composition in `d87c920`; it ran no actual SQL, Kubernetes, or native engine process.
Prior follow proof: `/tmp/m6-follow-flow-export-20261110/manifest.json`, SHA-256 `1f78346be36e277a54d3f9cfaa0935f35f51af9af723a562034e401ef9674040`.

The preceding operator checkpoint `9fd9061790f158ee8c65908113029ac667da68ba`, tree `53d2abe0385d28918d99d45cb71057dd24575739`, added worker locale pinning (`73c8a9b`), owned cleanup builders (`35f4ae3`), informational sampling (`f1ca944`), and identity status fields (`9fd9061`); each intermediate tree compiled and passed scoped lint with zero findings.
Prior proof: `/tmp/rpgmigrate-m6-ready-exports-composed-20261110/integrated-manifest.json`, SHA-256 `4709a0d229a45d59d0f59de328fec702b39bf22bc8144032f604fc9f0a276b25`.
The earlier retained-stop component export passed focused race tests and lint; its receipt retains component-only scope.
Prior proof: `/tmp/m6-retained-stop-export-20261110/manifest.json`, SHA-256 `1819a0f3f832e56fd23c256736282363d3b0253b2f095cf8d0bac9adba99f906`.
The runner-profile qualification plan is `/Users/ydixken/.codex/tmp/2026-11-10-m6-runner-profile-qualification-tfhgdc6n.md`, SHA-256 `3b8e016d201b55197c8409c40a5922fff9d5ce3092924a8f11803a8d0a694e69`.
Actual NSS, authentication/TLS, and three-pair acceptance remain open; image metadata is not runtime qualification.

Fresh native ARM64 release packaging on engine `254ab5ba103bfe75a2e1dcd8acad0b9720e88ab8` passed with config `0c755f5e36f3fb88c9bd1ecaa53759d0a741271696b552633eec20fef9b674a9`, binary SHA-256 `4bded8d59ccef2cf73b7cc88e76ec8d9d626caf0b1fc96f879ac0be2bedc9803`, and selected manifest `5af74fb39dfe31057273ae5a9d8268f5f1b982a8edd3868de392c8e69d6883f1`.
The actual image passed Linux-seven capability discovery and the UID 65532/read-only-rootfs smoke with stock libpq 18.6 and clients 16.15/18.6.
Proof: `/private/var/tmp/rpgmigrate-m6-runner-254ab5ba-8z211_yv/packaging-final-proof.json`, SHA-256 `09e096a066f94d6e515e8b2bbf3952575b4147c5914e884118e4e75372f0c265`.
This closes the fresh ARM packaging prerequisite only; actual authentication/TLS/resolver/PVC, three-pair native operator acceptance, matching AMD64 packaging, broad Go gates, and the whole-M6 review remain open.

### Retained source and component evidence

### Capability discovery source checkpoints, 2026-11-09

The post-cap contract keeps protocol v1 and three public discovery fields; Linux emits the original ordered six plus `verify.sealed_files.v1`, while non-Linux retains six.
The 134-file engine candidate and central operator mirror retain portable `qualified_capabilities`, add fixture-only `qualified_linux_capabilities`, and preserve historical empty/six-name parse acceptance; old six cannot satisfy native M6 before credential projection.
The exact joined 321-file FD/cap Linux producer check passed protocol ten/CLI31 and actual binary three-field Linux-seven discovery, plus executable CLI28; no packaged image, ARM helper/runtime, AMD64 database runtime, or production admission is qualified.
Joined source manifest SHA-256 `fbc3d7c2f4c59dd1ad68365d4770612b0505051243a25075d7d5be87f7d0de0e` and binary SHA-256 `275cab235cd9a27934b58766fc9865bf899cf03d694493a0b79e001fe0eef074` are bound by `/Users/ydixken/.codex/tmp/m6-joint-fd-cap-linux-20261110/joined-qualification-proof.json`, SHA-256 `ebdf851566cd2926e25d304be58ae35be6e2071a8a73a7bc8c62c61ebd6f19d2`.
The requested CLI selector was ignored and the broader executable 28-case invocation ran once; it is recorded explicitly without a repeat run or false partial inventory.
Engine FD source is integrated at `3c4d0a5a6212de642aa8f205989fadf80b64ee49`; thirteen focused Linux prerequisite cases passed on retained `8dd89a68` input, with host library Clippy/formatting evidence retained and the separate host transport libpq-minimum failure preserved.
Actual authentication/TLS/real PostgreSQL dump/image/NSS/resolver/lifecycle, broad Go, and whole-M6 review remain open.
The cap export retains macOS protocol ten, CLI 31, executable 28, fixture eighteen, and offline smoke 29 source checks.
Older operator `d28` scoped lint failed on two existing wrong-image constant collisions; the reviewed `6d` composition preserves all ten capability paths and has two Linux-seven/old-six mocked-preflight cases plus one actual native/controller scoped-lint PASS, without relabeling the older RED receipt.
Engine FD proof: `/tmp/rpgmigrate-m6-engine-fd-integration-20261110/fast-forward-proof.json`, SHA-256 `250408b47456fa4072d5dc8686cb3e8374278e126f1dd77dc78ac8feef45a91c`.
Operator comparison proof: `/tmp/rpgmigrate-m6-native-comparison-integration-20261110/checkpoint-ff-proof.json`, SHA-256 `faf9246c5a5f93e8b1c17e2a223ff9c87e94f524de29f1dacf4d783dcbff215b`.
The retained comparison checkpoint `351aa3` binds comparison10/36 plus transport eleven source cases/zero lint and canonical Python111754/54 mechanism cases.
The ten-capability consumer is now fast-forwarded into clean central operator HEAD `14b73f5980768841c68dcb0d699e5889e88d80ad`, tree `1f21d90ebb364241054ada9ad14a8eb54a911d51`, parent `351aa3`; old-six early refusal/new-Linux-seven source-consumer qualification does not clear runtime guards.
Fast-forward proof: `/tmp/rpgmigrate-m6-sealed-capability-composition-20261110/checkpoint-ff-proof.json`, SHA-256 `d5689817826b1b20b79d7582086bb328e4a5342813ce2477e2aeeb2ac7653c0a`.

- [x] Compose the exact fixture, producer/help, and packaging-smoke packets on engine baseline `ef37214b55e7a082f6e2ee949768649e83163ea4`, with a separate fixture/consumer candidate on operator baseline `727e45da94a5b2d099e97380e086a36206e68451`.
- [x] Verify packet manifests, every exported and baseline blob, intended modes, preserved peer files, and identical 133-file fixture trees.
- [x] Update discovery/profile documentation without changing the three-field DTO, command-specific reports, historical empty parser fixture, or operator activation gates.
- [ ] Qualify fresh ARM and AMD64 packaging artifacts from the exact new source and retain observed executable digests.
- [ ] Bind the new ARM executable/image to the scheduled M6 helper/preflight/controller and three-pair native acceptance packet.
- [ ] Qualify exact-image AMD64 native database runtime in M10 before claiming multi-architecture runtime support.

Historical engine `96d3707` and operator `b590` establish the earlier six-name checkpoint only; post-cap fixture/source facts above supersede its current-profile description without adding runtime qualification.
The [native protocol](../docs/native-protocol.md#discovery) records the post-cap platform-specific arrays, restrictions, and separate fixture-only six/seven metadata.
Retained source checks cover sixteen fixture tests, ten protocol tests, 31 CLI unit tests, 28 executable CLI tests, 28 smoke tests, focused Go capability/fixture tests, strict Clippy/formatting, and zero focused Go lint findings.
Input hashes match the retained producer Cargo/CLI scope (217 files), fixture-tool scope (134 files), and focused operator package scope (286 files).
The joined offline smoke reran once against the two changed producer sources and passed all 28 tests.
The five affected Markdown files passed the repository Markdown policy; both candidates passed whitespace checks, and the private composition proof retains exact inventories.
These packets change discovery/help and test/smoke observations without changing migration state, WAL, ACK, checkpoint, SQL, dependencies, or the Dockerfile.
Historical M5 local image `b463` and hosted `7c04dff` receipts retain their original scope and cannot qualify these new bytes.
Production `UnsupportedEngine`, resolver refusal, helper/PVC/file/SQL qualification, and whole-M6 closing gates remain closed.
M0 through M5 stay closed; M6 through M10 remain open.

### Current composed Go closing profile and blockers

The earlier clean central operator capability checkpoint was `14b73f5980768841c68dcb0d699e5889e88d80ad`, tree `1f21d90ebb364241054ada9ad14a8eb54a911d51`; comparison `351aa3` and earlier control/retry receipts retain their separate provenance.
Control passed 78 named checks/zero lint and retry Reader passed six focused checks/zero lint; these retained source proofs grant no activation or new Python/image qualification.
Fast-forward proof: `/tmp/rpgmigrate-m6-control-retry-integration-20261110/fast-forward-proof.json`, SHA-256 `2e1a86f86e7add5f47831584fa5acd1d7d6c379c71fab39df3e3e9b14e9e56b1`.
The earlier private two-file Python export `d1abd09` retained 111754 source bytes/54 mechanism tests before canonical source/registry/golden integration; its component evidence does not supersede the later central checkpoint.
Comparison parser/transport/helper source is integrated at the separate `351aa3` checkpoint above, while the FD implementation is integrated at engine `3c4d0a5a`; actual file/authentication/image/runtime qualification remains open.
ACK transport passed nine focused tests with zero lint; recovery passed four top-level/43 subcases through actual Reader composition with zero lint, as component evidence only.
Fast-forward proof: `/tmp/rpgmigrate-m6-ack-integration-20261109/fast-forward-proof.json`, SHA-256 `5c167ff85f802d716aa1655219a6d2d4c1717a06032f7f6f6ef49e0e2c432c5e`.
Interfaces/dispatch, runtime target-retry producer/Reader, owned-cleanup comparison, and activation remain open; `UnsupportedEngine` is unchanged.
Full canonical in-memory admission remains authoritative; these source-only clients and correlation hashes grant no target durability/completion authority, production native acceptance, or new image qualification.
The four clean M6 source/tool commits `ccda81f3`, `3d1bfe6f`, `f6f7de44`, and `32dbcfc` are fast-forwarded into the central operator feature branch at `32dbcfc`; remote origin remains `5616d82`, without a checkpoint push.
The exact fifteen-path/557-file checkpoint passed both packages with zero lint findings, retaining focused tests; no M6 activation, whole review, full-Go, or runtime closure follows.
Fast-forward proof: `/Users/ydixken/.codex/tmp/rpgmigrate-m6-checkpoint-ff-gp9fk58d/fast-forward-proof.json`, SHA-256 `17e8dd22a95c8b1412f9a50cfe60cefd08e4128cfce7548bce0082e5cb3b1ab8`.
The second clean packet (`f28c450`, `b8c6f51`, `1a569b7`, `4ecbd69`) is accepted and fast-forwarded into the central operator feature branch at `4ecbd69cc9e56e4710096b6f0475fc7d2a62fe8b`, tree `978f450c0976ef9f6d21a1feb0d42438eac7a943`, with exact four commits/fourteen paths/560 files verified clean and index empty.
Fast-forward proof: `/Users/ydixken/.codex/tmp/rpgmigrate-m6-components-ff-gnwrr55e/fast-forward-proof.json`, SHA-256 `6ab717a902a583d57ad04c376280655f2293fb5cfe9857dbc24a52a05d086e8b`; original checkout and all 44 other worktree refs are preserved, with no new push/CI/runtime result.
The [worker construction/dispatch packet](../docs/superpowers/plans/2026-11-09-m6-native-worker-construction-dispatch.md) is integrated as four clean commits `32c4390c13c4b8dcb2921f6c0400cde8944aab96`, `56840f67937df9b8e78f775953361377d77e4b40`, `f42a688f15ecce648f04e2e48f33851865d22400`, and `cba3fa03fe051432da3ef2650db2db2c8a9b2ad4`.
Central tree `2a6420851f0f8676e85adca321b2428561635cd2` is clean/index empty; eleven paths/566 source files are verified at inventory SHA-256 `bc8ad3b51d3cf1b334d3b93554ae809ea9cfda0e31da8065c0549b9cdf302189`.
Fast-forward proof: `/Users/ydixken/.codex/tmp/m6-worker-integration-j93qz9ch/central-fast-forward-proof.json`, SHA-256 `72794f2a4cedb46bd880c4760b388a25c4f2e3e759ff3fb0bbd11ed860a155e7`.
Each commit prefix is lint-qualified; final joined lint passed with zero findings, lifecycle passed 108 checks, and the exact qualified Linux engine passed eleven CLI cases.
The integration-only retry display-label lint correction was refrozen, without a gate bypass; top-level `UnsupportedEngine` is unchanged and no DB/operator migration, push/CI, whole-M6 review, or full-Go qualification follows.
The approved [discovery/preflight plan](../docs/superpowers/plans/2026-11-09-m6-native-discovery-preflight.md) separates credential-free version discovery from fixed Python DB preflight; actual capabilities precede credentials, without legacy raw-DSN argv/raw stderr.
Preflight failure exits 1 with no positive output or new failure envelope; refusal classes stay private, and a cleaned internal work timeout remains paced pending under original total/work/cleanup ceilings of 90/60/30 seconds, without renewal or adoption.
Completed-output transport manifest `/tmp/rpgmigrate-m6-version-transport-20261109/manifest.json`, SHA-256 `71142624e1d3a3ca69fe823b1bab15e63c25c5ff51dd0a99a6f74d8c971fbcf8`, is frozen with 171 named checks/zero lint, strong owner-UID uniqueness, and caller-supplied canonical execution hash; quiet evidence proof remains unchanged.
The new Python helper manifest `37aebdd5` passed 44 tests with 89227 source bytes; Go preflight successor `8e68af8e` passed 54 composed top-level/role cases plus five tmpfs-focused successor checks, with zero native lint.
The preflight packet is fast-forwarded into clean central operator HEAD `727e45da94a5b2d099e97380e086a36206e68451`, tree `a25b24dc3c73f2c652e9acc2304e8a58cb7fb1fa`, with 575 source paths verified.
The four commits are `2d895828` (native), `1041e59d` (Jobs), `f02373ab` (transport), and `727e45da` (lifecycle); exact qualified prefix/source receipts are reused without broad reruns.
Fast-forward proof: `/Users/ydixken/.codex/tmp/m6-preflight-integration-evidence-20261109-fyhieb3e/central-fast-forward-proof.json`, SHA-256 `22c2086a759b8361d8d060b0dba684eec231e614a93ede924bae92d60ad62665`.
Canonical `/tmp` is Memory-backed at 64 MiB and builders must consume that canonical output without overriding it; no runtime/guard activation follows.
The preflight field is not injected; resolver always refuses and remaining consumers, runtime, whole-M6 review, and closing gates remain open.
The capability source candidate above supplies discovery metadata and pure consumer evidence without opening those gates.
The earlier remaining-adapter proposal required root authority correction; the 2026-11-10 checkpoint records the root-approved interface freeze, with implementation and runtime qualification still pending.
No runtime activation, whole-M6 review, or full-Go closure is claimed.
Resolver disposition: `/Users/ydixken/.codex/tmp/m6-containerd-dns-research-20261109/existing-containerd-dns-disposition.md`, SHA-256 `b78e83c046be9441e8d0fd25362b500af30cfb8180e1c4a824a7d7a8d7f24a00`, favors qualifying the existing standard ClusterFirst sandbox resolver.
Actual runtime/NSS/HOME bindings remain gaps; no ConfigMap, numeric-only restriction, or HOME change is authorized.
The approved [Jobs/lifecycle plan](../docs/superpowers/plans/2026-11-09-m6-native-helper-jobs-lifecycle-direct-image.md) and [dated amendment](../docs/roadmap.md#m6-native-helper-jobslifecycle-amendment-2026-11-09) use canonical pure policy exports, fresh bounded 90-second Job/operation, earlier persisted E, and administrator-selected immutable image expectations; D1 exports are frozen and Jobs/lifecycle focused verification is joined, configuration later.
The future status.Image display-only correction preserves exact spec/imageID equality; actual direct runtime, identical-E acknowledgment, PVC/file/resolver gates remain open.
D1 manifest: `/Users/ydixken/.codex/tmp/m6-d1-policy-export-20261109/manifest.json`, SHA-256 `3571c6de4d1cd3a27045fbeeaa31f967f080d1f410b99e4ab1bb3a409ca344ae`; sibling proof SHA-256 `93f72c0797215f9c03bcaf3e5a0c3432b39beb4c997d4406e8fa47c55e4eb400`, recording seven tests/86 checks and zero native lint findings, without activation.
The cap-only two-file admission export `98c097e1` passed nine tests/105 checks against cap-only Go `561533e8` plus the old relay; this does not qualify the new Python file-binding work.
The tiny actual Linux ARM primitive probe passed six nonconnecting groups on image `b4631063096125a84d3428571fe81d7057c91cf3356adf54cc738744709e52b0`, with no canary exposure and the exact probe container absent.
Receipt: `/Users/ydixken/.codex/tmp/m6-explicit-linux-primitives-20261109/receipt.json`, SHA-256 `aca2a3816d8406ca5c40a2e4d92baa43f04014571646653ccbc53b7bb4eff266`; independent runtime qualification is explicitly false and TLS/database/Kubernetes/full-helper gates remain open.
Metadata report: `/Users/ydixken/.codex/tmp/m6-m5-runtime-metadata-20261109/metadata-report.md`, SHA-256 `3124f3bec6d5e283cbda9a5d6c6d6787db77017ea13feef419d1803b94cdbd7a`: image ENV HOME is absent while Podman synthesized HOME/passwd `/work`, so immutable-image/containerd HOME remains unproven.
Stock libpq 18.6/default-path absence and exact probe cleanup/original preservation are narrow observations, not normal helper/resolver/TLS/DB qualification.

The [2026-11-09 explicit-file/cap amendment](../docs/roadmap.md#m6-explicit-connection-file-and-cap-amendment-2026-11-09) and [approved plan](../docs/superpowers/plans/2026-11-09-m6-explicit-file-binding.md) authorize sealed mode-0400 memfd inputs and the sole immutable missing path before connecting, preserving raw libpq DSN/explicit-empty/permission/optional-absence behavior.
The main-only caps are 122880 source bytes and 126976 complete argument bytes including NUL; wire/relay/prelude limits are unchanged and native evidence Job JSON is limited to 1 MiB.
The Linux 131072-byte minimum per-argument limit clears no aggregate argv/env/stack, Kubernetes/proxy, or opaque Secret-length gate; exact image/read-only-rootfs/mount-shadow/runtime tests remain required.
Two disjoint source owners are building the four-file helper and two-file admission delta; the final decorated native create-site size check remains future integration and generic `ensureJob` stays unchanged.
Numeric-alpha resolver research `c567134f` is proposal only; resolver/service/PVC/image/activation remain unqualified.

- [x] Record the root-approved explicit-file/cap plan with private-source provenance.
- [ ] Integrate and qualify explicit-file binding and cap boundaries under the plan’s runtime and failure controls.

Task 0's final five-file pure-helper export is frozen at `/Users/ydixken/.codex/tmp/m6-python-mechanism-export-20261109/manifest.json`, SHA-256 `12a3a70804466b2079eb14e3098009f7e3683c6b5e7bc27cb2b283fcf06dd3e4`.
Its sibling `proof.json`, SHA-256 `bb09761a5e4591f53d72e4bb676e0d071211c70cab9be0bde210f7cbddf42af1`, records nineteen Python and eighteen focused Go tests, with three mutation controls and exact PG16/18 SQL fixture equality.
The admission-reader export is frozen at `/Users/ydixken/.codex/tmp/m6-admission-reader-export-20261109/manifest.json`, SHA-256 `e8d43f56bb813a64ddaa88fe140ab160b04bf04aa4425db1b4a9e961f8c51728`; sibling proof SHA-256 is `22cfb4fad23271dd072b286f822590ca2aab959cb2e4b96b9732644f00b62284`.
Its observed thirteen top-level tests and 330 focused checks passed without failures or skips, using exact peer inputs and same-E/live-binding controls; these are separate from the pure-helper counts.
The earlier Task 0 handoffs remain historical inputs; final relay-aware Reader composition passed fourteen top-level tests and 344 focused checks with zero failures/skips against the exact new relay/transport inputs, without integration, production admission activation, or broad milestone closure.
Final Reader manifest: `/Users/ydixken/.codex/tmp/m6-admission-reader-relay-export-20261109/manifest.json`, SHA-256 `4291d081b125c412a65d4bc4dfaa3bbfad501246e4b6211089130ff8d71b1ebc`; sibling proof SHA-256 `8d799b6a9e702a388691f101afaf20096b1320798567f981a6221d0bd936d15b`.
The new relay export is `/Users/ydixken/.codex/tmp/m6-python-relay-export-20261109/manifest.json`, SHA-256 `4226dd85ee2d4aad7ac80799ab135d1c036b7fcb42c4273bdd49ccda889a72ed`: 29 Python and nineteen focused Go tests (ten relay subcases) passed, with five mutation controls RED.
The new transport manifest `/Users/ydixken/.codex/tmp/m6-default-transport-t35vz_yh/manifest.json`, SHA-256 `512263a8cdd49e56497dc7a75d3fb8913ab0f4451b4f07bc69a92fdd529e8aef`, records 97 checks/92 leaf cases, zero failures/skips; no actual default-cluster/log-completeness/rotation/relay-flock runtime qualification follows.
Runtime qualification and production activation remain false; exact packaged-libc resolver bounds and opaque-file stability before first consumption, including service expansion, are unresolved.
The approved [delivery correction](../docs/superpowers/plans/2026-11-09-m6-default-kubernetes-delivery.md) replaces the default-disabled split-log prerequisite; its source checkpoint is now integrated, while runtime qualification remains open.
The [dated roadmap amendment](../docs/roadmap.md#m6-default-kubernetes-delivery-amendment-2026-11-09) keeps SQL in main, separate result streams in checked exec, and terminal Job success/complete empty ordinary logs as a separate requirement; no Stream query is used.
Require Running before exec and the same incarnation Running or successful exit 0/signal 0 afterward; neither post-exec state bypasses terminal Job verification.
Earlier focused handoffs do not qualify this correction; runtime/image/resolver/file/TLS/PVC gates and production admission remain open.

- [x] Record the root-approved 2026-11-09 default Kubernetes delivery amendment and preserve the reviewed private plan provenance.
- [ ] Integrate and qualify the delivery correction under its required protocol, default-cluster, closure, and failure controls.
Actual/defaulted Pod execution, private file/SQL identity admission, packaging, and real PVC permissions still need their assigned qualification; no permanent contract reduction is accepted.

The 546-file composition has tree `7442e438270f2d3ae47317447f7f07299fcf5322` and source hash `732fcf1f9fefe21db5b6ae15f4f3e3508fc31f8899bfb5e489281358602edd1c`.
Full lint passed sixteen stages with zero issues; `task test` passed fifteen uncached unit/envtest packages.
PG16 race checks passed 508 entries with exactly two expected skips; the PG18 subset passed 158 without skips.
The 128 Go shared-fixture cases from 186 inventory cases are already included.
Each major passed fourteen helper functions and 158 entries with no required skip.
PG16 explicitly combines seven completed helper results from an observer-interrupted invocation and seven fresh exit-zero results; PG18 passed all fourteen in an exit-zero invocation.
Proof: `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-helper-observer-resume-z995h42h/go-closing-proof.json`, SHA-256 `2f6e44cefbf4a39d6944891321300da721c13a72bc58100882cd166398b6710a`.
All owned objects were proved absent; all source files and 24 staged paths remained exact, with no secret or canary leak.
These retained M4 test-only closing checks do not activate production native paths; M4 is closed, while M6 mechanism, real admission, and lifecycle gates remain open.

The retained engine utility-routing reproduction showed explicit or inherited `PGSERVICE` redirecting schema children from parent-validated alpha to beta.
Snapshot-bound dump refused the wrong database, while exact restore invocations succeeded on beta in an isolated empty fixture; no user data was touched.
Receipt: `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/m6-service-precedence-t7l0ql_r/receipt.json`, SHA-256 `10f7d53c6c687e768f7940218d64b9153b9e3d57f31dc5695f6ff3e79cc49320`.
The existing-libpq effective-connection/selected-peer repair passed exact Schema3 on PostgreSQL 16.15 and 18.6 in the joined closing candidate.
Current-engine/current-Go actual seven-case PostgreSQL 18 interoperability passed final verification with mocked Kubernetes ownership and production admission disabled; broad Go suites were not repeated for unchanged source.

Both decisions are user-approved as of 2026-11-09: the preservation/VM maintenance plan and the scoped M6 Python 3/ctypes/stock-libpq runner dependency exception.
The exception supports direct private libpq authentication for opaque URI Secrets without argv credentials; mechanism implementation/qualification remains open.
The mechanism adds no Secret-read calls or RBAC scope: bind actual pod/template references, stable privately consumed bytes, and SQL identity, without claiming latest-Secret-resource-version attestation.
The approved maintenance completed with a verified persistent PostgreSQL 18 replacement and released lease; the retired tmpfs container must never restart.
The failed M7 nodes are removed under the exact user exception; the quiet reservation is released, and fresh resource-budget proof and ordinary kind/image capacity checks govern later work.
The host proof of concept passed nine URI cases and a PostgreSQL 18 connection/empty COMMIT, but is research rather than a package addition or production mechanism choice.
The measured `0b36` tool image lacks Python/ctypes and Perl DBI/DBD::Pg/FFI.
Capability receipt: `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/m6-libpq-connection-poc-rwasyp5n/tool-capabilities.json`, SHA-256 `bb073df851a6a709a6f5a874ac50f30e43e6b9bea9edd0d5166fa29448f1c265`.
The frozen pure helper does not activate production; packaged dependency/opaque connection handling, Task 0 production signatures, checked result delivery, and real Kubernetes/PVC/work permissions remain unqualified.
The private fresh-volume capability PASS does not satisfy real Kubernetes permission qualification.
Task 0 retains the approved root-reviewed addendum; one independent xhigh whole-M6 review belongs at milestone close.

### Retained component and ancestor interoperability evidence

Command and framing helpers have focused evidence only; production `UnsupportedEngine` activation remains blocked pending the native lifecycle gates.
A high-effort planning worker is preparing the remaining native client/evidence/lifecycle plan against clean checkpoint `5e8cbcb` and the frozen M4 contract.
The leader confirmed that independent `CheckpointEvidence` cannot derive from engine reports or native acknowledgments; the plan uses a bounded fail-closed existing-psql/private-PVC qualification spike.
Saved E uses the existing status optimistic patch without a CRD addition.
The corrected M6 plan, SHA-256 `d4bfd6731cd97dc456ca1495392e33e3c2dd742d24ee25e74efcee94ce741484`, passed root review for Task 0 only.
Task 0 mechanics are frozen at `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m6-evidence-mechanics-ru5y4n1m/manifest.json`, SHA-256 `7c8c421ea3b7ca8ff2436d08da7fbacf8ad96d3abd4a8fdf0d10cf362dcd3a35`.
The final export passed 53 focused leaf cases with zero skips, including 15 exact PostgreSQL 18 live cases; the earlier 13 PostgreSQL 16 cases do not qualify the changed final export.
Synchronous-warning exit-zero/COMMIT and post-COMMIT loss with exit-zero/empty-stderr were refused by the required warning/closure guards.
All 32 tool containers, PostgreSQL 16/18 containers, and the network were proved absent, and the pool was released.
The later test-only boundary prototype passed 194 cases: 120 without a database, 37 on PostgreSQL 16, and 37 on PostgreSQL 18.
That receipt does not complete Task 0, establish a production constructor/helper API, or authorize native activation; qualification and a root-reviewed addendum remain required.
Opaque `CheckpointEvidence` must never derive from reports or native acknowledgments; consumer interfaces and native activation remain unfrozen/refused pending evidence and review.
The leader accepted final M6 plan SHA-256 `c3ba6a68ac5dc4ca2ad269ccf0cd3d6949a2ffc15d5fb888ad28b1815d9b47ac`, retaining informational source-slot telemetry for the existing two-sample `CaughtUp` decision.
Twenty-second freshness uses new private constant-size state on the existing Sampler run, without a CRD or new cache.
It adds no CLI, DTO, application-fence subsystem, or `CheckpointEvidence` authority.
The composed 21-file test-only successor `031f9d4` passed 67 focused Go checks and eight Perl checks.
The actual PostgreSQL 16 live run uses a new owned two-CPU/1-GiB database container and a half-CPU/256-MiB helper.
The earlier run recorded 26 PostgreSQL 16 passes before the lost-COMMIT case failed to qualify; its owned resources were cleaned.
The latest `5f` input passed the single focused PostgreSQL 16 lost-COMMIT case.
Receipt: `/Users/ydixken/.codex/tmp/m6-env-single-e7u8etqr/runtime-receipt.json`, SHA-256 `9c13ca3ab8a0cf39295495f9b1f23d0405f84a395ece108d17bf8578c84b56f9`.
The lost-ack attempt retained fifteen stages, relay COMMIT/Idle, refusal, and no observation.
A distinct fresh PID/ID retry passed nineteen stages, EOF, exit zero, and the same row/token; cleanup and source checks passed.
That single-case receipt supplies only PostgreSQL 16 lost-COMMIT evidence; the later PostgreSQL 18 producer result below is separate.
The approved test-only actual-native-8 producer interoperability plan is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-m6-actual-producer-plan-u3xog4xr/2026-11-09-actual-native8-producer-interop-plan.md`, SHA-256 `e89bb99b7c10876130d4b9fa26c61b5f45979239aa00d74d9d10b3b77b450c9c`.
The corrected actual-native-8 fixture is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m6-producer-stream-nwon5ddr/manifest.json`, SHA-256 `134907f9302a745e3c786fe500d205623e742eaf98555f249da6a3ab2c998390`.
It removes only three unsupported `--resume` tokens from scalar endpos, JSON acknowledgment, and cleanup; native resume remains internal.
The other 22 files and caps remain unchanged from the `2b` predecessor: one CPU, 512-MiB memory/swap, and 128 PIDs.
Two named pure tests produced 146 PASS entries without skips; gofmt and diff checks passed.
Independent direct PostgreSQL 18 to 18 execution passed one `positive_keepalive` case and six negative controls, all seven PASS with zero skips.
Receipt: `/Users/ydixken/.codex/tmp/m6-direct-corrected-live-3xaedze9/runtime-receipt.json`, SHA-256 `d75d809991b8fa18a3e6a4014aef2f75e8b5506486abad83c499781bbbda75b0`.
The sibling `verification-addendum.json` has SHA-256 `0f33af8dd984f48152d5f843f47fca1de52b2720af8436ac52e5238f9a8182a8`.
The positive path proves actual COPY/follow, same-E source persistence, successful worker exit, and a fresh nineteen-stage helper with both EOFs, exit zero, empty stderr, and the source connection closed.
Independent Go ParseReport and the same-E drained JSON acknowledgment passed.
All 28 container caps were verified, with no more than two simultaneous containers; all 31 owned objects were proved absent while the original three remained.
All 23 source files remained unchanged and credential scanning passed; the lease lasted 33.483 seconds.
This is test-only interoperability with mocked Kubernetes ownership and false real-PVC/production admission, not a native operator alpha.
Earlier failures are retained in `/Users/ydixken/.codex/tmp/m6-actual-tcp-live-r9dubh_l/diagnostic-summary.json` and its sibling `transport-diagnosis.json`, hashes `617118fb240d0a23172ff443bc060bdbb158ea4ca840caab207a177c75db821f` and `1248e2591e7f37637e5827da3c1b1b7b2b854bdc847142d56682f755d6091902`.
Their causal sequence was private-wrapper stdout buffering, then unsupported fixture `--resume` flags; neither establishes an engine defect.
The independent private fresh-volume capability probe passed: `/Users/ydixken/.codex/tmp/m6-private-parent-independent-o92c12a9/runtime-receipt.json`, SHA-256 `b5573b26d44529bcc04a7c90d55eee5c9b16afccaa4f9113943d051652591803`.
UID/GID 65532 changed a fresh owned empty directory from mode 0755 to 0700 without privilege, then completed actual fsync.
A new container retained owner, mode, device/inode, and empty-state proof; exact owned cleanup passed.
The earlier private-probe stops were stat/id-map list-context harness bugs, fixed through scalar/arity handling and five pure RED/GREEN controls, without engine or production changes.
This does not qualify real Kubernetes fsGroup/PVC or work-PVC ancestor permissions; existing-state/PVC guards remain unchanged, with no guard relaxation or root initialization approved.
Production signatures, checked transport, tool dependencies, URI/SecretRef handling without argv credentials, and real PVC/work permission qualification remain pending.
The exact 23-file ancestor composition fed the later closing profile above; it does not authorize activation.
Task 0 acceptance uses the approved root-reviewed addendum; one independent xhigh whole-M6 review belongs at milestone close.
Application write fencing and ending outstanding transactions remain the operator user’s duty in both Manual and Automatic modes; no new fencing subsystem is authorized.

- [ ] Engine-aware argv0 and scripts (worker, verify, cleanup, compare, catalog, endpos exec); lift `UnsupportedEngine` for the qualified profile; `--rpgmigrate-runner-image` flag and chart value.
- [x] Implement and checkpoint dormant native discovery/preflight source, retaining the closed runtime/activation gates.
- [ ] Preflight: `version --json` handshake into `validateNativeConfig`; native pg_restore gate by target major; skip the origin-grant probe; PG16 target allowed for native.
- [ ] Phase from `native-report:` lines; native drain verification bound to status endpos plus a psql read of the owned control row; cleanup requires `complete=true`; plain-clone completion fails closed on a missing report.
- [ ] ORed application_name patterns for the retry-gate census, sampler and zombie reaper; native error-class prefixes in the log matchers.
- [ ] Unit and envtest coverage; operator docs for the native engine.

## M7: Operator-driven E2E

### Evidence checkpoint, 2026-11-10

The fresh `f59a7f4b` M7 source passed UP, supervisor handover, and LOAD for the PostgreSQL 18 component/API profile.
Its legacy gate FAILED during storage-probe image-ID recognition: the actual probe Pod Succeeded with exit zero and bound PVC/PV, but the wrapper refused the exact declared tag at its selected manifest.
Go migration tests and the metrics observer did not run; no legacy or native workflow acceptance follows.
Failure proof: `/private/var/tmp/rpgmigrate-m7-tagged-legacy-gate-6s2i181n/final-failed-gate-proof.json`, SHA-256 `269d0780ab532117842ce072674dbd4356c41c21f7873c64050fa61872afcc92`.
The private declared-imageID fix has source inventory SHA-256 `b5da9c9aa6a0142fd27eaf69950d639c9be68363376be2f930cbe3662f2259a3` at `/private/var/tmp/rpgmigrate-declared-imageid-candidate-zad1graq/source`.
It changes five paths, preserves 527 peers, and passed one full 259-test Python run, thirty top-level/186 named focused Go checks, and zero scoped lint findings.
Independent pure-data replay accepted the retained actual Pod data under `b5` while `f59` refused it, and rejected all eleven negative controls; fresh workflow acceptance remains pending, and the retained `f59a7f4b` gate remains failed.
Replay proof: `/private/var/tmp/rpgmigrate-m7-declared-imageid-data-proof-m7umsaqh/pure-data-result.json`, SHA-256 `978118f6bcf93701055aae19fd9dcd5f0c426b015067036a34b4c3e601cab8d3`.
Exactly six approved old kind volumes were removed after fresh ownership/reference barriers; the cleanup preserved four original services, 206 protected volumes, and 395 images.
Proof: `/private/var/tmp/rpgmigrate-m7-unused-volume-safety-c3anxbon/final-candidate-proof.json`, SHA-256 `06fe68baef50633e019b070f32605ebdb4f39b9eaeea4ee4f426b2bc09428f12`.
Exact retirement of the failed `f59a7f4b` S2 and three nodes passed, including process/cgroup absence, preservation of all 212 volumes and 395 preexisting images, and protection of four original services.
The narrow retirement-only exception preserved the historical sticky handoff event and grants no runtime-health adoption.
The separate six-volume cleanup passed without force or generic pruning; its exact final proof retains the bounded implicit-network metadata exception.
The fresh `b5` RUN passed all eighteen LOAD entries in 693.867 seconds and valid bootstrap, then its Go gate failed before any selected spec and without authenticated manager-version evidence.
The frozen error path discarded the exact failing assertion, so its cause remains unknown.
The failed workflow is preserved under the fixed 2026-11-10 06:30:27.495346–08:30:27.495346 UTC lease; no retry, retirement, or probe is qualified by this result.
A safe-observability successor has passing focused source checks and lint, with independent reproduction and integration still pending.
Failed Go proof: `/private/var/tmp/rpgmigrate-m7-declared-legacy-gate-vdjrkezj/final-failed-Go-proof.json`, SHA-256 `281aaeae83376c7c6c6f77efcf485ef6b806ccef6e19715538bf0d9963410990`.
Observability successor source SHA-256: `a9f16b40f5906877c9ac31f10f6ea35ac585a3d48d7bb3c6d1df9461012b3a5d`.
Cleanup proof: `/private/var/tmp/rpgmigrate-m7-volume-cleanup-exec-npke035c/final-volume-cleanup-proof.json`, SHA-256 `a2ba5911eaff739a4ee4a326a9f642764af5b4177d9cbd26b36132d6c160c7b4`.
Fresh RUN: `/private/var/tmp/rpgmigrate-m7-declared-run-qwttxyj7`, RunID `369add6677404cdd98b7e1e447f85045`; no legacy or native migration acceptance is recorded.
Retirement proof: `/private/var/tmp/rpgmigrate-m7-f59-retirement-ty2tj0zn/retirement-handoff-manifest.json`, SHA-256 `14d39880a80841fbaf452a14a0f4346fdb5769eccec93a280abffe7a89726de1`.

The M7 `c275` source passed fresh UP/CHECK and qualified supervisor handover after its retained PREPARE/CHECK success.
Its first LOAD pair FAILED: default `ctr images tag` dropped labels and CRI retargeted the existing config-ID key from the prepared child manifest to its one-child archive index.
No legacy or native migration test ran.
Failure proof: `/private/var/tmp/rpgmigrate-m7-component-api-load-khjk5rqw/sealed-first-failure-manifest.json`, SHA-256 `31b5f259d277ad47c3ea83ea7916737bfa03d7fbe1cc4247598959eb5b72dada`.
The `f410` source candidate uses the exact `--local` tag fix with the preservation contract unchanged and passed 246 Python tests.
An independent experiment on an untouched node preserved all prior descriptors, labels, and the config-ID child key, adding exactly one expected alias.
Experiment proof: `/private/var/tmp/rpgmigrate-m7-local-tag-experiment-24mmp_m9/final-experiment-proof.json`, SHA-256 `f024de59fd9f1564ac81ef1a2592f3c2f22debc1db9d9519c8d598393b7330ba`.
Exact retirement of the old `c275` nodes and S2 passed, with their processes/cgroups absent and all 206 volumes, 383 preexisting images, original services, backups, source, and failed RUN evidence preserved.
Retirement proof: `/private/var/tmp/rpgmigrate-m7-c275-retirement-sx7gdvy6/final-retirement-proof.json`, SHA-256 `f184619aef2dbd2f3c7b408415095e4f6d0320da7c0227ac4fc85f5e8f3a9d0a`.
Fresh `f410` PREPARE/CHECK passed for RUN `/private/var/tmp/rpgmigrate-m7-local-run-g3o_x2i9`, RunID `048dc2ed3e044990bc25f7a11d7a91e0`, using manager config `09b96b6eed4b3a55f6a2933c7f98fa0e765191a5559d54b3a1c555388ca8a660` and build VERSION `m7-f41042c308ba`; the preparation proof does not qualify the runtime version.
Preparation proof: `/private/var/tmp/rpgmigrate-m7-local-preparation-1k5qtccq/final-artifact-packet.json`, SHA-256 `16ded43c7d7fd9dae5075eba3d357ebd1fcb09cb13a5326ec21b8e67af53ce5a`.
That earlier fresh UP command returned under a fixed lease ending at 05:06:57.016634 UTC on 2026-11-10; it did not itself qualify full-ready CHECK, supervisor handover, LOAD, or legacy execution.
The old lease was not renewed, and the historical `c275` LOAD receipt remains FAILED.
The capacity baseline is fourteen CPUs, 32 GiB memory, and 160 GiB disk; runtime admission still requires fresh capacity evidence.
M0–M5 remain closed and M6–M10 remain open.
Benchmark execution remains on HOLD.
Main pushes, tags, and releases remain excluded, with development GHCR publication governed by the dated M7 amendment.

### Retained bootstrap and failed-run evidence

The earlier `c275` preparation used source `/private/var/tmp/rpgmigrate-component-api-avwmu909/source`, digest `c275c790cbe74795f08b0936e2bc3b12e3fb0273c39ac8424708df2956fd3cac`, with 242 Python checks and the retained unchanged-input 151-check Go gate.
PREPARE passed in 113.215 seconds and CHECK in 3.484 seconds for RUN `/private/var/tmp/rpgmigrate-m7-component-api-run-51rlqh3s`, RunID `7193d24a35e44a758cb9969cdf2937c8`.
Preparation proof: `/private/var/tmp/rpgmigrate-m7-component-api-preparation-5fre5jm0/final-artifact-packet.json`, SHA-256 `33b870064d96d327d5d5f8561a4a57fa08721afde0b4b6f9a2d81085ea0ca610`.
The prior failed legacy RUN was retired through exact three-node and separately verified unused-volume removal, preserving originals and failure evidence; its legacy gate remains FAILED.

First UP passed on the corrected 530-entry source `5cc72893`: three Ready nodes, API readyz, and verified aggregate caps of six CPUs/12 GiB.
The frozen seven-role inventory is legacy PostgreSQL 17 only; native/PostgreSQL 16/18 entries are null placeholders, and this inventory does not establish M5 native-image qualification.
Tracked CHECK before load and load passed; phase is `Loaded` with six loaded-digest receipts and positive post-load readiness.
The original finite supervisor hit a read timeout during load; guarded DOWN correctly refused the active RUN lock and exited its handoff without deletion or source/receipt repair.
The watch-only replacement restored monitoring under the original deadline with a recorded 648.049941-second gap; do not claim uninterrupted continuity.
The blocked Loaded workflow was then retired under standing container-only authorization, with the watch intentionally stopped before expiry rather than a fabricated deadline event.
The CHECK state-loss and storage index/child comparison fixes passed 189 source-level unit cases on frozen 530-entry source `5dcceda` (manifest `2f46f044`); fresh PREPARE/CHECK passed on that corrected source, without a new UP or workflow qualification.
Fresh PREPARE/CHECK proof: `/private/var/tmp/rpgmigrate-m7-check-index-preparation-mlwoh9ym/final-preparation-proof.json`, SHA-256 `4d89e0731c37a2f8331f07af03bdda713d28b8f7823bcba540ae6e46d9cd394d`; the new manager/RUN has no UP result, and the retired Loaded RUN remains preserved.
The earlier private S1/S2 implementation `f57314` passed 33 fake controls only; the later `c275` checkpoint records qualified live supervisor handover.
The approved [tracked kind-only Go pre-uninstall build-info hook plan](../docs/superpowers/plans/2026-11-09-m7-tracked-build-info-capture.md) supersedes the private observer implementation proposal and is included in the final source freeze; metrics permission remains approved but unapplied.
The final Go six-file packet `9fcb71e2` passed 160 checks with zero lint; Python 198 (wrapper `fa4038`, tests `9670d6`) and the guide are final.
The final 532-entry dirty-source freeze is `d5698bf5f130519336382828ecbfcf89995937df53730fb35537cca55de8897f`, manifest SHA-256 `d3f80323d7641c2a66730011cf360cad92a6e4c9711cff0cde99d1dc44fc46f5`; the full 31-path Git delta includes inherited changes and is neither committed nor lint-clean.
Legacy source-provenance gaps remain explicit; source-equal controller/API/CRD/module evidence binds accepted M4 `c262322`, without qualifying all M7.
Fresh RUN `/private/var/tmp/rpgmigrate-m7-manager-runtime-run-85za8902` has preparation evidence under `/private/var/tmp/rpgmigrate-m7-manager-runtime-preparation-fulc261h`.
The manager build exited 0 with image `0584f2e5fea8ab4cc2c0145b7f55927787f9909ade8e3d3106c1585cbc74fdf1` and source-bound build argv/expected version `m7-d5698bf5f130`; this is not an observed runtime-version claim.
The later legacy gate failed before CNPG apply because the pinned kubectl renderer emitted 26 concatenated JSON objects and the old single-document `json.loads` rejected them.
Read-only exact reproduction: `/private/var/tmp/rpgmigrate-m7-exact-renderer-6r2vq853/capture-report.json`, SHA-256 `57bf2aa5de50bc1aafd34b1d3e8a2c97637d466ecf0695869070742153254aeb`.
Successor `/private/var/tmp/rpgmigrate-m7-manifest-integration-rgpbyxg6/source` is frozen at `8af5c480741caa760294c52ece74c4a89c586bab80d4995a522acd4dcc3be65d`, manifest SHA-256 `1f7d8a9b54641db01d8eee64508888a463799eba9fb53c44fdf35ed64def835f`: only three files changed, 529 peers match, and its source input matches 214 focused passing checks.
It requires a new manager/archive/RUN; no old acceptance reuse, CNPG stage completion, or new runtime qualification is claimed.
Phase-1 retirement proof: `/private/var/tmp/rpgmigrate-m7-phase1-retirement-kf0hbluu/phase1-final-proof.json`, SHA-256 `9c7b65266a8a1fd0d567e60afae3099f79c1682fe534d72e6c59b93d38fecd61`: exact S2 and three node containers/processes/cgroups are absent while all 206 volumes, images/network/originals, source, and RUN evidence remain preserved.
The later independent unused barrier passed: `/private/var/tmp/rpgmigrate-m7-independent-unused-barrier-n3eby4u2/independent-unused-barrier-proof.json`, SHA-256 `01e1de3b257925a8fc7d3850f2950907737d6c8d9b213da13383cc1a9a0000f3`.
The exact nonforced reclaim removed only the three new unused volumes, preserving the protected 203; proof `/private/var/tmp/rpgmigrate-m7-phase2-exact-volume-reclaim-hjg32s04/final-proof.json`, SHA-256 `ddad85dae180a6f13aacab59f9cc3ca13fca40c4c5f3852cac8d939fe223ae67`.
The approved 128-to-160-GiB resize passed with the same fourteen CPUs/32768 MiB, PostgreSQL 16/18 IDs/ports preserved, M0 restored and three original services running, retired f8 still stopped.
Resize proof: `/private/var/tmp/rpgmigrate-m7-disk160-maintenance-yy9hryp5/final-resize-proof.json`, SHA-256 `77f1fc3753b91123e8583ae44b4246832bd681ec8b0547b9f1ebade617327394`.
Fresh `8af` RUN `/private/var/tmp/rpgmigrate-m7-manifest-run-o9aaccp_`, RunID `2f06271bc8dd494ba1ba7025d91dc76e`, passed PREPARE/CHECK/UP/LOAD with six archives/eighteen image pairs; the legacy gate then failed in 334.581 seconds (Task 201) before Go.
CloudNativePG CreateContainerError came from normalizing an existing raw import alias to missing key `docker.io/library/import-2026-11-09@sha256:2e4051608564bbe757b099067e2df55f5c4a960b6191837ce448ec3d1760b297`; content/digests/snapshot exist, and no alias deletion is observed or asserted.
Sealed failed handoff: `/private/var/tmp/rpgmigrate-m7-manifest-legacy-gate-0uu04_9w/sealed-handoff-manifest.json`, SHA-256 `0d1b1bd8fe8631ab99e1d19d4eed411df7a61018d0a1784ea5d7489ad4970a41`.
The single authorized nonforced alias addition on the Pod host made the same original CloudNativePG Pod Ready/restart zero; the historical gate remains FAILED, with no Go/M7 pass.
Sealed experiment: `/private/var/tmp/rpgmigrate-m7-CNPG-alias-experiment-r5ad002c/sealed-experiment-manifest.json`, SHA-256 `f59ae83815c0e1b80107e2abf71df7240f82bfe9334cd3311121d2e5f68163f9`.
The earlier private M7 fixes passed 28 focused/231 full Python checks and 139 named Go checks/zero lint before the composed `c275` successor recorded above.
No fresh build/runtime acceptance follows; the prior failed gate and experiment remain unchanged.
The VM remains fourteen CPUs/32 GiB/160 GiB with originals protected; a separate four-CPU/4-GiB focused Linux Rust window with 6-GiB growth is planning only, without benchmark execution.
These bootstrap/load observations do not qualify the legacy migration workflow, native inventory, PVC behavior, or full M7 lifecycle.

Retirement proof: `/private/var/tmp/rpgmigrate-m7-loaded-retirement-ai592_0t/retirement-proof.json`, SHA-256 `32c45afc9b3962979c2cde03c8d4a847fb9c3ffc0266b6d6b94d6777b114e513`.
Exact three nodes/processes/cgroups are absent; all 203 volumes, 339 images, immutable network properties, original four services, backups, all 530 source entries, and all 42 RUN files’ bytes remain unchanged with phase `Loaded` and six load receipts.
Actual CPU/RAM reservation is released and the historical lease remains untouched; this is exception retirement, not guarded DOWN or M7 closure.
The [user-approved metrics observer policy](../docs/roadmap.md#user-approved-owned-kind-metrics-observer-policy-2026-11-09) applies only to fresh independently admitted owned kind E2E runs, with no repeated approval within scope.
Policy: `/private/var/tmp/rpgmigrate-m7-metrics-policy-7s2s5o81/policy.md`, SHA-256 `2342fdf3350981d5de4346165f70781cb74a3478c4247ac2527038dba11806df`; template `metrics-owned-minimal.template.yaml`, SHA-256 `667625c2045edd030000e3c5065b83a92206d6d499df7ea77009aa2ea572bb9c`.
No observer action, namespace creation, production RBAC/Secret access, lease renewal, or benchmark execution is implied by this decision record.

First-UP proof: `/private/var/tmp/rpgmigrate-m7-kubeconfig-fixed-first-up-5_122d1k/first-UP-handoff-proof.json`, SHA-256 `5e18acb1d9761eb7751c13815898fbf76c5f344b8f5cd077facdbd536aa5f4bd`.
Load proof: `/private/var/tmp/rpgmigrate-m7-declared-load-ihjsd0zy/final-load-proof.json`, SHA-256 `5ffd59a2c1231940f497862e8e5b65525b1ec6e5bf9a6a7fc52ae8a467d3b525`, explicitly `HANDOFF-required` after the final monitor-continuity assertion failed.
Load took 93.117 seconds; supervisor PID 47985 hit its read timeout, and any watch-only replacement retains hard deadline `16:58:11.799173Z` / monotonic `529583.5440380573`, without renewal.

The leader accepted the [M7 bootstrap slice](../docs/superpowers/plans/2026-11-08-m7-kind-bootstrap.md) for two code-only owners based on operator checkpoint `5e8cbcb`: lifecycle in `/Users/ydixken/development/pgcopydb-operator-m7-bootstrap` on `feat/m7-kind-bootstrap`, and suite in `/Users/ydixken/development/pgcopydb-operator-m7-suite` on `feat/m7-kind-suite`.
The read-only image provenance research at `/tmp/m7-image-research.json` resolves the approved major-17 image to 17.11 and retains the existing OTel 0.162.0 constant and approved CNPG 1.30.1 manifest hash.
The bootstrap receipt must prove an actual executed/passed legacy case count and selected spec name, Complete/CutoverCompleted, nonempty data and sequence checks, and fixture cleanup.
Final kind teardown is a separate observed outcome, not part of a fixture-cleanup success claim.
The exact eleven-file M7 join is frozen at `/private/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/m7-integrated-handoff-2v5s7huy/join-manifest.json`, SHA-256 `542c39179e542486dce4d4478851d273ae90ec4bd7746c52bb5e84af581654b8`.
It includes 57 passing bootstrap controls and its self-check plus the focused suite checks.
A compatible custom-lint tool passed focused proof while preserving the log check; the full required closing lint remains pending.
Actual artifact preparation pulled six exact missing images and recorded eight image identities, then refused the pinned legacy runner binary: observed `0.18.129.gd7547b5`, required `0.18.144.g86bf908`, with a different hash from the pinned builder binary.
The refusal is retained at `/private/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m7-v2-run-0mjhw18_/legacy-baseline-rejection.json`.
Both owned inspection containers were removed and absence proved; no manager build, archives, artifact check, or cluster followed, and source files were unchanged.
That attempt returned the resource window to M3; the later user capacity override below permits parallel M7 preparation without an exclusive database lease.
A separate local ARM64 v0.25.0 candidate passed the executable/tool/nonroot/workdir contract with version `0.18.144.g86bf908` and the exact trusted builder binary hash; owned cleanup was proved.
Its receipt is `/private/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/m7-runner-v025-qualification-3fh3l5q2/candidate-qualification.json`, SHA-256 `33bb7d4a33ae0518985a546da3d1d827e107d569b2044fd8eb14d2adcea66f71`.
The ARM inventory-pin correction is guarded-joined into source `6ae1557f82057f898574bcd499ce785d5e3ec8440ab5a4a8c58e770d3eb2f20b`; AMD64 runtime qualification, manager artifact, archive/bundled-storage provenance, and the full gates remain open.
The original runner refusal remains valid evidence for its old pin.
The ARM manager build passed against unchanged pinned source `6ae1557f82057f898574bcd499ce785d5e3ec8440ab5a4a8c58e770d3eb2f20b`, covering 529 files.
Podman OCI save was rejected because gzip recompression changed the pinned manifest; neither pin relaxation nor a configuration-only pass is accepted.
Artifact preparation then passed byte/receipt checks using existing VM skopeo 1.22.2 with `--preserve-digests`, retaining published pins without a new dependency or custom assembler.
Seven OCI role archives total `798203904` bytes, with 65 role-blob references and 28 embedded storage blobs; the node upstream index and selected ARM child remain distinct identities.
The actual archived legacy runner binary matches the retained trusted builder and qualified `86bf908` binary.
Evidence is `/private/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m7-v4-run-p5ajdpha/artifact-preparation-evidence.json`, SHA-256 `292dc3ae493b442f068e4ebd5c23d6c6fa7d611ca5eaba95a98408ddc53dc73d`.
Private files are mode 0600 and directories 0700; observed minimum VM free disk was `19568111616` bytes, above the 10 GiB floor.
The approved reproducibility/capacity plan is SHA-256 `831ebb2e4ac1b723b98c5d4c3e1311fce6a1b1f14e9138888cf7aaf3c00418d2`.
Task 10 is frozen at `/private/tmp/m7-task10-producers-frozen-7d5xow85/manifest.json`, SHA-256 `5069c96437b0268ce7db86507d7f7d4c5e804fbc992197ee540a86cea6d1a4fb`, with 131 focused checks passing.
Task 12 is frozen at `/private/tmp/m7-task12-node-limits-frozen-iyjphj8s/manifest.json`, SHA-256 `85399fc3e8ebde3aa55a5c8101fd9114503c7c4ef7466e39bb0e82d0e3acfaab`, with 153 focused checks and its self-check passing.
Task 11 documentation is frozen; third tracked preparation ran against exact source `1b7ca392fa63d4bd2c0662b355e3299f334df84fb19c3704a2b24d63baa2df46`.
The source-bound manager build and all seven archive roles passed, but the baseline stage refused the multiline version-probe parser.
Refusal: `/private/var/tmp/m7-third-tracked-control-718nbi5s/tracked-preparation-refusal.json`, SHA-256 `34a6bf8e9684d81648c8cc74c6c77491d5d6ecc796447e4652a6071478af722b`.
No tracked check or up followed; all owned containers were proved absent.
Task 13 corrective planning found that Podman removal deletes transient CID files, leaving declared CID receipts missing.
The Task 13 correction separates transient CID files from immutable receipts and validates complete version-output framing.
Tracked prepare and check then passed once each against source `6222196c83af94e5652e72921b1e4010c72160221a90a58cab702942a10ce1bf`, with all 530 entries unchanged.
The retained run directory is `/private/var/tmp/rpgmigrate-m7-task13-run-0sv9j84y`.
Final inspection: `/private/var/tmp/m7-fourth-tracked-control-p42xzbbk/final-preparation-inspection.json`, SHA-256 `e8fbe8d88d3dd40daba6ca2bcf726d2937efd5a72605f83a1ca86c62705573c3`.
Kind up was not attempted; the earlier 14.78-GB demand was a planning envelope incorporating a three-node work bound and margin, not a freshly verified hard requirement.
The exact frozen `hack/e2e-kind.py` wrapper at source `6222196c` enforces only the recorded 10-GiB VM and 2-GiB host floors; no 14.78-GB aggregate-demand field or receipt was found in this bounded check.
Its seven prepared archives total 798,203,904 bytes; the wrapper hash `1c884fe724126e688f11de57a72af7cd8a69b147288358ca4bb2884602799eab` matches the frozen source manifest.
The pre-maintenance read-only owned-cache check started from 12,565,581,824 free bytes and found 419,700,736 allocated bytes conservatively reclaimable.
Whole target plus Cargo upper bounds total 583,225,344 bytes, and the whole-prefix upper bound is 589,815,808 bytes; those upper bounds are not the conservative reclaim claim.
No deletion, prune, or resource change occurred, and the conservative reclaim is insufficient against the prior planning envelope.
Capacity receipt: `/Users/ydixken/.codex/tmp/m4-engine-closing-final-q31o707f/checked-close-successor/capacity-receipt-final.json`, SHA-256 `e9593bb2f2306c6917227a9a2a06d0f2438d5b4138a84108a00ecf623d733f9a`.
It binds the existing creation/source-preflight/cleanup logs and canonicalized read-only capacity observations without claiming a nonexistent raw transcript; no repeated measurement or inspection occurred at receipt finalization.
Bounded M5 image peak and M7 full-phase growth remain unqualified; no image/kind execution qualification follows from this read-only check.
The authorized maintenance passed and the exclusive lease is released, with stability verified across the launcher and two later tool exits.
Final proof: `/private/var/tmp/rpgmigrate-vm-maintenance-1qbpv5lh/final-maintenance-proof.json`, SHA-256 `8049b048872366a5ae98e37f7325f053d7a1bf603b83be2eb8a8ecadf30e9f5f`; the earlier `85bf` proof is provisional.
That earlier rootless AppleHV maintenance established fourteen CPUs, 32768 MiB memory, and 128 GiB disk with vda4/XFS grown; its receipt recorded `/var` 41,988,149,248 free bytes and host 237,409,554,432, before the later 160-GiB resize above.
Persistent PostgreSQL 18 CID `2474b0a76854696548c79e05d2ff23ccde9158b1ef305855b6027f14c87745a4` uses volume `rpgmigrate-pg18-persistent-07949d733b9b4c1d8a199dd7f8077f1f`.
It preserves name/loopback port 38209, system ID `7705919902701522967`, timeline 1, settings, database/role/configuration, and data fingerprints.
Old CID `f8ebf039713a75e4b69208d4871b1c6b9238c0b207995ff0044bd604b680651b` is retired, stopped, and retained: never restart it.
Original PostgreSQL 16 on port 46631 and M0 retain their CIDs/configuration/mounts/tools; backups, existing images, and volumes are retained, with zero pruning.
History includes the idle M0 PID1 forced stop, an unexpected first VM stop of unproven cause, a stale API-socket failed restart, and corrected detached launch.
PostgreSQL 16/18 recovered ready with zero PANIC/FATAL and preserved metadata; this evidence is not an unconditionally graceful shutdown claim.
Preparation success alone authorized no maintenance or kind creation; the later user decision separately authorizes the stated preservation/resize and unused-resource pruning plan.
The first M7 UP failed after tracked check, source, 37 outputs, and capacity admission passed.
Diagnosis identified missing `podman exec -i` and fake marker/label controls that did not retain state; the frozen recovery plan is `/private/var/tmp/rpgmigrate-m7-failed-up-recovery-hebu751n/plan.md`, SHA-256 `aeff403890a7cd4540a7dfc041ff22c9fd1b2c2543594354ac5968bd36413dc8`.
The missing `podman exec -i` source fix is ready with 81 focused checks and a separate 169-check full wrapper/self-check run; these overlapping counts are not additive.
Frozen 530-entry manifest: `/private/var/tmp/rpgmigrate-m7-marker-stdin-4q0vvk_e/manifest.json`, SHA-256 `252beb9266dd1df454e75e9b48a09d92f373f330f1b83e78d4c9b6afee08a223`.
Sibling `focused-green.json` has SHA-256 `a0743c7e5ede8d3bbad0eb42d419f06c1409db844bebc681fba2d1aa51e069bd`; `whole-harness.json` has SHA-256 `9bcbfd77efce0ffd4d1481c2b8b9828f075acfcaa65c38bf94701440cbc2ed95`.
The new source integration preserves 527 unowned paths while changing exactly three fix/guide files; its 530-path manifest is `/private/var/tmp/rpgmigrate-m7-marker-guide-integration-ysunzd95/manifest.json`, SHA-256 `bc3d7721cd999bbd8156b5cbedafe80ced7ef2044d45575584b4acb60c122fbd`.
The real frozen-marker probe passed root-owned mode-0600 readback, no-clobber, and exact owned-probe absence checks.
Proof: `/private/var/tmp/rpgmigrate-m7-marker-roundtrip-1q0do84i/final.json`, SHA-256 `376d4e3a680ea4a0520bafc6b2ce9f38e76f698dc19f35a09600235dcd323de3`.
Fresh source `78a663` prepared successfully; the first UP for RUN `/private/var/tmp/rpgmigrate-m7-marker-fixed-run-0x7obi5w` verified all three caps, then failed at a kubeconfig kind-prefix mismatch.
RUN remains `creating`; guarded DOWN refused the same naming check, with no RUN repair.
Independent recovery observation: `/private/var/tmp/rpgmigrate-m7-kubeconfig-safety-review-8k92oct6/observation.json`, SHA-256 `e6550197bf935ac956f92e4853b9a2d643a787bd6045c2ec7f7c6bd0c9e0f48e`; runtime proof `/private/var/tmp/rpgmigrate-m7-marker-fixed-first-up-twnzphcp/failed-UP-recovery-proof.json`, SHA-256 `17fb5ca3728c323a2beffbaf7d69f558e0cae8be842f4fe7806f123bd090fbe1`.
The approved exact three-container cleanup passed: containers/processes/cgroups are absent and the kind reservation is released.
Final proof: `/private/var/tmp/rpgmigrate-m7-second-exception-cleanup-7xoc3tat/cleanup-proof.json`, SHA-256 `7ef026e0161d9d8ea48c20dc2a2eec9dbe16dc4fb3ba5f4855fcad4ca60ae6b7`.
Volume metadata, images, immutable network properties, original four services, backup hashes, all 530 source entries, and every RUN byte remain preserved with phase `creating`; this exception disposal is not guarded DOWN qualification.
The two-comparison guard export `6277b1f1` and realistic fixture export `aa76c08c` compose GREEN: 87 focused checks and one 175-check full wrapper run, with original-guard RED.
Those source-level contract tests preceded the corrected first-UP acceptance above; failed RUN evidence remains preserved without repair.
The frozen manager recipe uses three CPUs, 5 GiB memory, no swap, and one build job; PID enforcement is outside that approved recipe.
The preceding failed-source readiness was not UP qualification; the later corrected first-UP acceptance above remains a distinct source-bound result.
The exact user-exception cleanup is independently verified at `/private/var/tmp/rpgmigrate-m7-exception-cleanup-xz08ws2q/final-exception-cleanup-verification.json`, SHA-256 `846f6314b9a729fb291adc53947e0464a540687f49c5f65bdda14a8675eaa759`.
The approved containers were `5a59d137a4039907737030d0bf07ec1a511568a3222ea6a27413bef3935371e7`, `ed1456fddbb6c6fbe42178e6699f0c551d00f25ad0ba83374a93b294140a261f`, and `ec087f13078d0ebc16c1c21fce55cc42cc807f7b204f9b8b8a3346610cbe74de`.
Graceful stop/exact removal used no volume flags; all three containers, their init processes/cgroups, and matching controllers are absent.
All three volumes, the kind network, images, backups, and four original services remain retained.
All 530 source entries and every old RUN file remain byte-identical; phase stays `creating`, markers stay empty, and `node_limits` stays absent.
The quiet reservation is released; this is the explicit human container exception, not guarded DOWN, UP qualification, or a general guard bypass.
The source RUN creating/empty markers stay untouched; no volumes, network, images, backups, originals, or generic guard bypass are covered by that one-off exception.
The failed window used an approved private finite supervisor with a monitored 12-GiB first-UP envelope, not a quota, a maximum 7200-second lease without renewal, and guarded DOWN handoff.
Earlier partial runs remain preserved and never adopted.
The historical failed first-UP attempt does not qualify kind topology, caps, or operator workflow; its exact failed nodes were removed by the bounded cleanup recorded above.
The aligned standalone Podman cap probe proved update to two CPUs / 4 GiB with no effective swap.
Its receipt is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m7-node-cap-aligned-probe-8ktx3pxr/receipt.json`, SHA-256 `ab3d8ba884a56203f2f58b8fa384e06e21371a3b870fa806f2d7e4ed01c80d39`.
That existing-container update does not qualify actual kind topology or cluster resource caps.
The successful preparation binds only its exact source; verified node caps in the failed fresh attempt do not qualify the kind lifecycle, legacy workflow, or AMD64.
No manual proof JSON, renewed lease, or new schema is allowed.

- [ ] kind (podman provider) bootstrap with pinned CNPG, CRD apply and image load as Task targets; validated first with engine=pgcopydb on 17:17 (can start right after M1).
- [ ] Suite knobs: `E2E_ENGINE`, `E2E_RUNNER_IMAGE`, `E2E_SMALL`; DropIfExists off and filters dropped under native; engine-aware `checkMajors`; a positive `native` label set.
- [ ] Native runs on 16:16, 18:18 and 16:18: live writer load follow, manual and automatic cutover, verification, runner kill mid-copy, mid-stream and mid-drain, target primary kill with CNPG synchronous replication, suspend/resume, deletion cleanup.
- [ ] Hosted GitHub Actions job on kind at small scale.

## M8: Feature widening (each item enables its capability)

The approved [M8 plan](../docs/superpowers/plans/2026-11-09-m8-feature-widening.md) freezes fresh native format 9 / metadata 7, separate `ClonePlan.copy_tables`, conservative binary fallback, and mandatory exact LO comparison under the unchanged array contract plus a proven-empty inventory guard.
No format/runtime gate or capability is qualified by this plan.
The approved plan permits isolated M8 source work before M7 closure after Task 1 freezes the shared interface.
The root approved the Task 1 interface freeze; isolated state/CLI, catalog, authority, COPY/large-object, and phase/recovery owners are building against it.
Isolated joined source tree `2e528d290bebc9f9f5677bd4fab44dccd53d99ae` passed formatting, strict all-target Clippy, and 75 affected unit cases against confirmed stock libpq 18.6.
Earlier 188 passes and two obsolete-fixture failures retain separate provenance; corrected selectors passed without relabeling that earlier input.
At the earlier joined-source checkpoint, compare/progress wiring had source tests only; the later component checkpoint above records the additional same-major runtime evidence.
The separate seven-file harness/docs slice passed 348 portable tests and discovered exactly ten ignored Rust tests; positive E2E profiles, authority faults, operator metadata-7 helper interoperability, and the full matrix remain open.
Joined source proof: `/tmp/rpgmigrate-m8-joined-source-v5-20261110/verification-receipt.json`, SHA-256 `0a6219ec91c0d1a4d7b6800e78fdbd211fec314b17495b013a656faada328394`.
Actual PostgreSQL 16/18 schema-only TOCs and restores exclude large objects, with positive controls including them, so the tested archive path needs no extra LO exclusions; the later component checkpoint above supersedes the earlier PostgreSQL 18 catalog hold.
PostgreSQL 16 exact catalog-class checks passed; the two owned scratch databases were removed and original resources preserved.
TOC proof: `/tmp/rpgmigrate-m8-catalog-toc-20261110/0942b349889341e7/final-proof.json`, SHA-256 `79a302da68f236c733b23f8061f3f065e2097576c614d056b58327f87946c9e2`.
Keep the active format 8 / metadata 6 profile separate from candidate format 9 / metadata 7.
The isolated M8 code/docs handoff is signed as recorded above; no M8 source fast-forward to the engine primary or activation is authorized.
The approved bounded split policy defaults to max parts 8, rejects explicit zero, and caps the actual plan at 4096 parts; standalone defaults remain binary false, threshold 0, and large-object jobs 4, with operator defaults unchanged.
A provisional execution profile permits schema restore only in `PreDataPending`; resolve the actual source snapshot and post-restore target plan, then atomically `confirm_pre_data` before permits.
A new-generation restart requires positive own-resource absence and a fresh resolved profile.
The approved `sha2` 0.11.1 dependency disables default features; pinned cargo-deny 0.20.2 passed actual-graph license, advisory, and yank gates against the 2026-11-09 database, and the v5 affected units now pass known-vector and chunk-boundary hashing checks.
Approved interface: `/tmp/rpgmigrate-m8-contract-freeze-20261110/contract.md`, SHA-256 `d2ad26fe39e75414c6f475e84e8920a5496c54456e5e9a8ba882dc7c4ef56b8b`.

- [ ] Binary COPY with the per-table built-in-type predicate and text fallback.
- [ ] ctid split parts (`--split-tables-larger-than`, `--split-max-parts`) with per-part done markers.
- [ ] Large objects: copy, follow re-sync after drain, compare coverage, data-phase restart cleanup.
- [ ] Re-run the M7 native matrix with operator defaults.

## M9: Durability modes and measured performance

The approved [M9 plan](../docs/superpowers/plans/2026-11-09-m9-durability-performance.md) follows M8 with native format 10 / metadata 7 and the settings/status capability extension; no ACK derives from volatile frontier and cadence awaits actual measurements.
Benchmark execution remains on HOLD until the final actual artifact/argv packet is approved; no benchmark run or VM reservation is authorized, and M10 remains open.
Initial-copy planning keeps fork async and native per-table durability enabled; a planned full-clone final flush barrier may match end-state durability without equating crash contracts.
The reviewed private final plan and frozen interfaces permit source A/B work; the checkpoint above records incomplete tests, unavailable release mode, and the unchanged benchmark HOLD.

- [ ] `replay-safe` barrier cadence chosen from benchmarks; effective values in help and status; `relaxed` only if the measured gain exists.
- [ ] Benchmark harness: A/A noise runs first, then interleaved randomized pairs in fresh processes; report target-committed rows and transactions per second, catch-up rate, commit-to-apply p50/p95/p99, and COPY MiB/s on narrow and wide tables; compare fork default against `replay-safe` and label `durable` as the stronger contract.
- [ ] `docs/durability.md` crash matrix per mode: process kill, host power loss, target crash, target failover with and without a synchronous standby, source WAL retention.
- [ ] Optional binary pgoutput, gated per stream on a built-in-type allowlist, only if measured.

## M10: Release readiness

The approved [M10 plan](../docs/superpowers/plans/2026-11-09-m10-release-readiness.md) covers bounded mutation testing, native ASAN with nightly unverified, STE source/license provenance, and exact production OCI artifacts/final matrix; preparation is not release qualification.
The unchanged structural STE script, MIT license, and [provenance](../tools/docs/provenance.md) are integrated from reviewed export `/Users/ydixken/.codex/tmp/rpgmigrate-m10-ste-vendor-export-20261109`, manifest SHA-256 `942eae4c135d7125296b1bec02a2755550e8dfbee093ac15516dffa77a939f83`, patch SHA-256 `1ad4b24fcba5e2dcfaf09c92555edef5ca1208bdb13116472acffea602e4e2ef`.
Script SHA-256 `155bf7bb73caa70735719ef0390cc39568fcd007509b28000a208839df29f3a3` is executable mode 0755; MIT license `d3c674e8592076c9021a59b55177adbcba6fd7e1710082becb922522b22ec672` and provenance `4319c7d50c5781d347f28963dcd00f5672b8ddf3d3b96a23415f9209feaf2d93` use mode 0644.
The earlier self-test, ten CLI controls, and provenance STE/Markdown checks retain their original scope.
The source-tool checkpoint above adds the checked inventory and portable registrations; actual documentation and ASAN qualification remain open.

- [x] Vendor the reviewed STE script/license/provenance as preparation only.
- [x] Commit M10 source tooling and register its three portable suites, retaining actual docs/ASAN/fuzz/runtime qualification as open gates.

- [ ] README index, getting started, durability, operator guide, troubleshooting; docs drift and STE100 checks.
- [ ] Parser fuzzing and a sanitizer or Miri job over `src/pg/ffi.rs`.
- [ ] Independent reviews of durability, recovery and the operator diff; final matrix on the exact image digest.

## Capacity and parallel execution, user update 2026-11-08

The verified VM allocation is fourteen CPUs, 32768 MiB memory, and a 160-GiB disk; the M7 section binds the latest resize/preservation proof.
The original 21:35 UTC startup failure predates the CPU/memory resize.
The earlier M7/database overlap permission remains subject to measured capacity and host Cargo headroom; failed-node cleanup and the leader's resource-budget release precede an M5 runtime build.
The two-database-track window reserves six CPUs per track plus two reserve CPUs, totaling fourteen CPUs and 18 GiB.
Image preparation waits for one database track to be idle.
The preparation window is 11 CPUs and 17 GiB: database testing six CPUs/6 GiB, images three CPUs/5 GiB, reserve two CPUs/6 GiB.
The capped kind window is 14 CPUs and 24 GiB: kind six CPUs/12 GiB, database testing six CPUs/6 GiB, reserve two CPUs/6 GiB.
Do not overlap image preparation, full database load, and kind.
Initial uncapped kind creation requires database testing and host Cargo builds to be idle until all three node caps are verified.
Retain at least four host CPUs of headroom when assigning Cargo jobs on the 18-CPU host.
Preserve the enforced 10-GiB VM and 2-GiB host floors; the earlier M7 first-up envelope is a planning estimate, not a verified wrapper threshold.
The M7 section records completed preservation/resize, the final stable proof, retired tmpfs identity, and the separately owned first-UP window.
Never restart the retired PostgreSQL 18 tmpfs container; retain its identity and evidence while using the verified persistent replacement.
Perform the authorized resize and confirmed-unused pruning only under the approved preservation plan and exclusive maintenance owner; this is not blanket deletion authority.

## Executable command scope

The M0 reset does not implement additional operator command paths.
Retain explicit refusals for unsupported work, keep the parser baseline, and qualify new runtime routes only in their owning milestones.
The [operator compatibility contract](../docs/operator-compatibility.md) defines the consumed interface; later milestone evidence determines support.

### M2 verified component handoffs

The catalog owner's exact two new admission cases and stock utility pre-data smoke passed on PostgreSQL 16.15 and 18.6.
The comparison owner's exact canonical SQL case passed on PostgreSQL 16.15 with libpq 18.6; its retained result is `/work/m2-compare/result.log` in the isolated development container.
The snapshot/COPY owner's six clone cases passed on PostgreSQL 16.15 and 18.6, including RED then GREEN for replaced-snapshot refusal.
The PostgreSQL 18 test binary passed six cases, but its wrapper refused because the required inventory grew to seven during that lease; this preparation failure is distinct from the later passing seven-case receipt.
The updated portable gate `python3 -W error tools/ci/unit.py` passed 156 tests.
The development-only E2E image built as `aa4c2ab94e0ffd4c57319c221f61a099b40d06cd2184d18f8be0b275531994a7` with stock PGDG clients 16.15 and 18.6 and libpq 18.6.
These component checks do not establish the container recovery matrix or operator qualification.

### M2 first plain-clone workflow

`m0_integrate` observed the first focused quiescent PostgreSQL 16 to 16 clone pass with actual server and client tools 16.15.
The executable exited zero with durable completed target proof; independent SQL fingerprints, schema columns, constraints, indexes, triggers, and sequence state matched.
Runtime schema/data comparison covered all eight physical tables, and completed progress contained measured table/index/byte counters.
The owned fixture included identity/generated columns, 2048 wide rows, an empty table, inheritance, partition leaves, and a foreign key.
The retained receipt is `/work/m2-integration/plain16-result.json` in the isolated development container.
This establishes one focused happy-path workflow only; the three-pair matrix, concurrent-write/recovery E2E, closing review, and operator qualification remain open.
No performance result follows from the fixture's elapsed time.

The frozen seven-case Clone profile passed on PostgreSQL 16.15 and 18.6 after coordinating its exact inventory and selectors.
It includes the six snapshot/COPY cases plus the target marker authority case; the earlier six-case wrapper refusal remains preparation history rather than runtime failure.
The E2E partial-copy restart gate now holds a harness-owned target SHARE lock, observes the exact worker blocked at `TRUNCATE ONLY`, and requires other table markers before killing the owned engine child.
Source catalog AccessShare locks prevent the originally proposed source AccessExclusive gate; the replacement changes only test synchronization and makes no mid-stream COPY claim.

The M5 documentation lint command `npm exec --yes --package=markdownlint-cli2@0.24.0 -- markdownlint-cli2 '**/*.md' '#target/**' '#node_modules/**'` passed with 15 Markdown files and zero issues after narrow format-only repairs.
Literal trailing-space prefixes remain exact quoted strings inside code spans; no lint rule was disabled.

The bounded restart pre-data schema-drift guard was an earlier matrix hold and subsequently passed its focused gates before generation publication and owned-data TRUNCATE.
Its existing E2E restart case now includes a same-OID/column default change, explicit drift refusal, and unchanged target data/generation/markers before restoring the source default and testing a valid fresh-generation restart.
M5's seven workflow controls, actionlint, yamllint, and dependency license/advisory checks passed.
The first local native ARM64 production image build preceded the notices rebuild and later read-only packaging receipt; hosted AMD64 verification, publishing, and M5 closure remain unqualified.

The permanent Schema profile requires exactly `schema::tests::live_pre_data_guard_refuses_schema_drift_without_target_mutation`, with two owned empty databases and dedicated schema-admin bindings.
`task test-schema` selects that case; the retained Engine profile explicitly excludes only its separately qualified guard name, and Clone remains frozen at seven cases.
The portable suite now registers the frozen seven M5 workflow controls and 12 image smoke controls exactly once.
`python3 -W error tools/ci/unit.py` passed 177 tests after those registrations, the schema inventory/absence controls, and image Task argument tests.
The new image Tasks preserve explicit runtime/image/Python overrides and quoted argument boundaries.
The local native ARM64 read-only smoke passed with UID/GID 65532, libpq 18.6, clients 16.15/18.6, and positive owned mount writes; its receipt is `/tmp/rpgmigrate-m5-image/arm64-smoke.json`.
That image records its actual build input manifest, but later M2 source edits prevent treating it as final-source qualification.

The schema owner's exact pre-data guard case passed on PostgreSQL 16 to 16, 18 to 18, and 16 to 18, with one passed case and zero ignored cases per receipt.
`task test-schema` permanently registers that helper and its owned source/target-admin environment; the frozen Clone profile stays at seven cases.
The existing E2E restart case's default-drift input and no-target-mutation assertions are frozen for the full matrix.
The final pre-matrix portable gate passed 177 tests; the harness additionally requires exact created container IDs before cleanup, and readiness uses TCP rather than the server image's temporary initialization socket.

### M2 producer conformance

`m0_integrate` verified fresh operator-bound PostgreSQL 16 clone and progress payloads with the actual Go `ParseReport` against caller UUID/run/PVC/generation bindings written before the engine launched.
Both reports used the original raw JSON bytes, completed phase, and all three positively measured completion counters; wrong work and an unknown status member were rejected.
Independent source/target fingerprints covered eight physical tables, and the target control row reported complete generation one with eight markers.
The retained receipt is `/work/m2-integration/operator16-result.json`.
The authoritative Linux ARM64 probe is `/workspace/target/e2e/native-report-probe`, SHA-256 `da115fdcb776f9d0e52fa3d1d2b0b7cbcbd3fab5de471fda76aadc8e4eb8614f`.
The matrix harness now configures operator mode before launch; standalone mode accepts only unframed standalone summaries and never fabricates work identity.
Its optional selected probe fails closed, while default engine-only runs use the narrow M2 contract and explicitly label that evidence.
The updated portable gate passed 179 tests; the complete three-pair matrix and operator controller qualification remain open.

The final documentation lint rerun passed all 17 discovered Markdown files with zero issues after the operational report-mode clarification.

### M2 closing review repairs

Independent review found that COPY's locked relation name was not rebound to the saved target OID before mutation, allowing a foreign replacement name after an owned-table rename.
The checkpoint owner repaired that authority check, and independent review resolved the saved-OID binding finding.
The reviewer also found that unknown-COMMIT resolution read the marker before the synchronous barrier and could retain stale absence; the owner added the post-barrier reread and independent review resolved that finding.
The integration owner will remove the newly added pipeline dead-code expectation without suppressing the retained staged lint failures assigned to M3.
The earlier review-repair receipts preceded the first full matrix attempt and do not independently qualify the final full workflow.

Two confirmed review findings entered the closing repair pass: generated expressions can recompute different stored output, and discarded synchronous-replication cancellation warnings can falsely establish durable completion.
The snapshot/COPY owner added source-snapshot full-row parity before marker COMMIT, including generated output and the 16 MiB full qualification-row cap, within existing live cases.
Transport/checkpoint owners repaired private warning classification and positive durability proof; independent review resolved that changed portion, while final qualification gates remain open.
The harness's reviewed physical row-count baseline now fails loss of nonempty wide-row, duplicate-parent, or separately counted child coverage without adding matrix cases or profiles.
The row-count coverage control demonstrated RED then GREEN, and the updated portable gate passed 180 tests.

Four actual CLI preflight controls rejected an explicitly empty probe argument, empty probe environment variable, missing probe, and nonexecutable file before any container was launched.
These selected-probe failures never fall back to local validation and do not constitute a matrix run.

The generated-output repair passed two exact selected live cases on PostgreSQL 16.15 and 18.6, covering table-OID rejection and generated-only oversize rollback with no marker.
The owner demonstrated actual PostgreSQL 16 RED on the pre-fix code, which falsely completed the mismatched generated values; copy unit tests (three) and canonical helper tests (seven) also passed.
This subset does not close the updated seven-case profile, locale qualification, or the full matrix.
Independent review subsequently resolved generated-output parity and then the synchronous-replication warning/factory/pipeline changes, leaving no code-review must-fix open.

M6 command helpers passed focused controller/legacy argv checks across the assigned four helper files on the separate operator branch.
The framing worker reported three top-level framing tests and 65 subcases passing with negative binding controls; the counts are not aggregated.
Production `UnsupportedEngine` behavior remains unchanged, so helper evidence does not activate or qualify the native operator lifecycle.
The M3 plan keeps unknown WAL headroom as an operator obligation and known unsafe SQL settings as fail-closed checks; candidate component evidence and the failed first follow smoke are recorded in the M3 status above.
The transport owner narrowed pipelines to noncommitting windows and routed transaction completion through an observed-commit path with pinned warning visibility and unknown outcome on any warning.
Those private proof semantics add no report fields or ignored test names and still require the closing warning/durability checks.

### M2 final-source verification in progress

The independent closing reviewer cleared all findings, including the notice/factory/pipeline changed portion against four exact handoff hashes.
The accepted proof path keeps private warning text out of diagnostics, preserves safe result-code classification, forces warning visibility, and grants completion only from positive transaction proof.

- [x] Bind the locked relation name to the saved target OID before mutation.
- [x] Reread unknown-COMMIT markers after the synchronous barrier.
- [x] Require snapshot-bound generated-output parity and complete-row cap admission before marker COMMIT.
- [x] Resolve warning visibility, connection-factory, and noncommitting pipeline proof findings through independent changed-portion review.
- [x] Receive final Prepared10 and Clone7 receipts on PostgreSQL 16 and 18 for the frozen input.
- [x] Record final formatting, CLI, portable, and inspection checks and classify new M2 Clippy findings separately from staged carry.
- [x] Accept the later integrated M3 strict broad closing gate as resolving the carried M2 lint hold.
- [x] Complete the staged three-pair matrix with eleven-case receipts for every declared pair.

The final runtime, harness, and CLI input manifest contains 58 entries at `/tmp/rpgmigrate-m2-close-xq1Ljk/source-inputs-final.json`.
Prepared10 and Clone7 passed independently on PostgreSQL 16 and 18, each with exact inventory, zero failures, and zero ignored required cases.
Formatting, the 24 CLI contract tests (including 220 long-option and 188 short-binding cases), all 180 portable tests, and `state::clone_state::tests::inspection_overlaps_owner_without_lock_or_write` passed.
The final strict Clippy triage retains 165 library and 19 library-test errors, all unused/dead code, with zero correctable lint categories.
Six newly introduced M2 pipeline staged diagnostics were recorded separately from the older retained scaffold and assigned to M3; strict lint was red at that attempt.
The full matrix launched once through `task test-e2e-clone` with the selected actual Go `ParseReport` probe after the component and triage conditions were satisfied.
Its first 16 to 16 attempt stopped at `clone-e2e: owned harness subprocess failed`; no pair receipt or eleven-case success was emitted.
The original inner stderr was later recovered from Podman journals: the observer waited for TRUNCATE while the engine correctly blocked earlier at its explicit ACCESS EXCLUSIVE lock.
The matrix logs are `/tmp/rpgmigrate-m2-close-xq1Ljk/matrix.stdout` and `/tmp/rpgmigrate-m2-close-xq1Ljk/matrix.stderr`.
M0/M1 and full M2 closure remained unchecked at that attempt; later primary M3 gates and the leader's acceptance above close them.

The M3 workspace completed a stable baseline copy at `/tmp/rpgmigrate-m3-baseline-hnl9za4t/manifest.json` before this roadmap/ledger update.
These documentation edits are later primary-checkout changes and must be preserved during owned-file integration; runtime/harness inputs were not edited.

## User-approved WIP checkpoints, 2026-11-08

The user approved exactly two local checkpoint commits: `e544f2d0e7da76d6f2fc55f2c0270b3a85ae4496` for the reviewed M0/M1/M2/M5 work and plans on `feat/native-engine`, and `5e8cbcbbe7ad9b89cb80a7edc1d5004af5cd4e58` for the M1 contract plus M6 pure helpers on the isolated operator `feat/native-contract-v1` branch.
Both checkpoint commits are complete, and the two-commit exception is consumed; the normal lint-clean-per-commit rule has resumed.
The assembled M3 candidate later passed strict all-target Clippy without allowances, but that does not establish a primary full `task check lint test` pass.
The checkpoint decision left M0 open until its integrated strict gates passed; the later normal M3 commit resolves that hold.
These checkpoints preceded the later qualified staged receipts recorded below; the first matrix failure's stale TRUNCATE observation boundary does not invalidate the final pair results.
No milestone closing gate changes, runtime qualification, push, or merge is authorized by this checkpoint decision.
M3 worktrees, archive and stash refs, original operator user work, and the pgcopydb fork remain outside the checkpoint scope.

### M2 staged closing pair receipts

After the user-approved WIP checkpoint `e544f2d`, the owned harness observer changed only to the exact pre-TRUNCATE ACCESS EXCLUSIVE lock statement.
Private failure capture now retains redacted stderr in 0600 files under 0700 directories, and public output contains only safe command kind, return code, diagnostic ID, and source basename/function/line.
The original failure is a stale observation predicate, not the separately discovered initial operator restart-admission gap.
State/driver startup repairs passed 89 state tests and 11 driver tests and cleared targeted independent review before the new pair ran.
The first operator launch uses `--restart`; the existing completed retry uses `--resume --not-consistent` and checks unchanged data, generation, and marker row identity.

The valid PostgreSQL 16 to 16 pair exited zero with all eleven cases and six actual Go report validations, server/tools 16.15, and libpq 18.6.
Its receipt is `target/e2e/16-16.json`, SHA-256 `a96e99eaba7cdbc89ddff9507d9e7a39f3462e5dbd9ce748ebe7f0e6fb30c19d`.
It observed seven prior committed table markers, overlapping read-only progress, default-drift refusal, and a fresh COPY generation from one to two.
The PostgreSQL 18 to 18 pair also passed eleven cases and six Go report validations; its receipt SHA-256 is `f2e2c2b3aaa16551df8a3343bcedc62f4c909d2a0f8e864e840db1806fb074ba`.
Those staged runs used the revised 58-entry manifest `/tmp/rpgmigrate-m2-close-xq1Ljk/source-inputs-restart-lock-final.json`, SHA-256 `38e34de96f8c6f9f6d1d31b8415a2c66a862e556df668083cc33270ddd7d835c`, with runtime hashes unchanged during execution.
No aggregate `task test-e2e-clone` exit-zero claim follows from separately successful pair tasks.

The first 16 to 18 attempt stopped at an independent oracle mismatch without a pair receipt and was not retried unchanged.
Read-only comparison of the successful same-major receipts showed that only the schema fingerprint differed, with table and sequence fingerprints equal.
A fresh owned schema-only PostgreSQL 16.15/18.6 proof reproduced the raw mismatch solely through three validated PG18 table NOT NULL catalog rows; `attnotnull`, all other constraints, indexes, and triggers were identical.
Dropping a non-PK NOT NULL remained detectable through `attnotnull`; a real unvalidated PG18 NOT NULL constraint demonstrated why blindly filtering all n rows would be unsound.
The oracle now omits only n rows from the other-constraint stream and explicitly refuses unvalidated NOT NULL, with unchanged FK/CHECK/primary-key/index/trigger checks.
The private metadata proof is `/work/m2-oracle-proof/schema-proof.private.json`, SHA-256 `0556212217316c79d440d5b9931cb5813d5e69f3d0c8e2c5997def14785809b0` in the retained development container.
The new guard demonstrated RED then GREEN; all 13 harness checks and 183 portable checks passed.
The repaired harness is frozen at `a56ddabc72f25374522bf0e62e6e806575025b8a5f0108811f43104c42888d85` for `tools/e2e/clone.py` and `2ce695adb5782fa3716c303baf448e68d9cd6f4f69ead09f369171047fc77e34` for its portable tests.
Integration ran the authorized current-input test attempt and the repaired cross-pair once; the final constituent receipts below supersede the earlier pending state without claiming an aggregate Task exit zero.
No engine source, M3 worktree, case count, or Go report count changed for the schema oracle repair.

### M2 plain-clone alpha qualification

The repaired 16 to 18 pair exited zero with all eleven cases and six actual operator Go report validations, server 16.15 to 18.6, client tools 18.6, and libpq 18.6.
Its receipt is `target/e2e/16-18.json`, SHA-256 `deb11b8206404d2c90c903a19baf63f937534f8f99b4ac57c2f1d8e31b1a000c`.
Together with the preserved same-major receipts, the staged profile comprises 33 case executions (eleven per pair) and 18 actual Go report validations (six per pair).
The same-major oracle amendment was proved semantically neutral before retaining those receipts, including actual NOT NULL negative coverage and refusal of unvalidated NOT NULL.
The final frozen 58-entry runtime/harness/CLI manifest is `/tmp/rpgmigrate-m2-close-xq1Ljk/source-inputs-final-qualified.json`, SHA-256 `98724ebfa70e82798c2eace4609104fe5b349e4b2c2d7ee87a4bd5e5b312263d`, and remained unchanged after the final run.
The summary is `/tmp/rpgmigrate-m2-close-xq1Ljk/final-staged-results.json`.

Final staged tests passed 417 Rust tests, 24 CLI tests, and 183 portable tests; the Rust receipt separately records 12 ignored live cases and 68 filtered cases, not additional passing tests.
Formatting and diff checks passed, and all independent M2 engine/startup review findings are resolved.
The aggregate `task test` and `task test-e2e-clone` commands were not rerun as a whole after their earlier failures; no aggregate exit-zero result is claimed.
The staged M2 strict-lint result was red at 165 library and 19 library-test unused/dead-code errors, including six separately tracked M2 pipeline locations.
The later integrated M3 strict gate passes without allowances.
The COPY marker/generation/fence, consistent COPY order, supported restart/resume, progress/compare, and declared E2E subdeliverables are checked for the qualified alpha scope.
The leader accepted the M3 grouped CDC frontier/producer/API handoff and later strict gate as satisfying the remaining M2 closure criteria.
Unknown pre-data handoff, uncertain post-data recopy, unsupported binary/split/large-object profiles, and generated full-row admission limits remain explicit boundaries.
This is plain-clone engine alpha qualification, not an operator-controller, follow/drain, failover, hosted AMD64, release-image, or universal crash-recovery claim.
The two WIP checkpoints retain their historical scope; normal source commit `7b6037f` follows the later strict gates and leader acceptance.
