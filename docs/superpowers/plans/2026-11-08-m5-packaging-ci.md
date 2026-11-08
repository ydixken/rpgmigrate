# M5 Packaging and Engine CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task.
> Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and smoke-test the stock PostgreSQL runner on amd64 and arm64, and run the existing engine qualification gates on hosted CI.

**Architecture:** We build one executable from the existing Cargo package in a pinned Rust builder and install it in a stock Debian/PGDG runtime.
We reuse the M2 Podman clone harness and existing Task and Python inventory checks.
Image build and smoke checks remain separate from development-container E2E qualification.

**Tech Stack:** Rust 1.99.1, Cargo.lock, Debian trixie, stock PGDG libpq 18 and client tools 16/18, Podman, Python standard library, Task, GitHub Actions, cargo-deny, markdownlint-cli2.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md), [roadmap M5 and amendments](../../roadmap.md), and [native protocol tool layout](../../native-protocol.md).
The roadmap amendments take precedence.

## Global Constraints

- Build the `rpgmigrate` executable in one Cargo package.
- Use stock PGDG libpq and PostgreSQL client tools 16 and 18; introduce no patched libpq build or capability gate.
- Install `/usr/local/bin/rpgmigrate`, matching-major tools under `/usr/lib/postgresql/16/bin` and `/usr/lib/postgresql/18/bin`, and default `psql` major 18.
- Include `sh`, GNU coreutils, and sed; run as uid 65532 with read-only rootfs and writable `/work` and `/tmp` mounts.
- Build and smoke-test `linux/amd64` and `linux/arm64`.
- Keep `publish = false`, omit a project license assertion, and preserve third-party notices.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources.
- Preserve the unresolved M0/M1 lint hold; no skipped gate or suppression turns it into a pass.
- Run independent review and applicable Go suites once at milestone close; engine-only packaging changes have no operator Go suite.
- Commit only after required gates pass and the leader requests it; do not push or publish.

## Review Focus

- A successful shell invocation must not hide a missing executable or missing library; smoke checks execute the real binary and both tool majors.
- A default-root run must not qualify uid 65532; inspect image configuration and verify the effective uid under read-only execution.
- A writable temporary directory must not hide a writable rootfs; test both permitted mounts and a denied rootfs write.
- A partial matrix must not qualify the supported pairs or architectures; workflow tests require every exact entry and nonempty receipts.
- A private-root license exemption must not exempt dependencies; audit the locked graph and retain native package notices separately.

## File Ownership and Handoffs

We assign three implementation owners with disjoint files, followed by one integration owner.
The leader transfers shared files only after their current M2 owners hand them back.

- Image owner: new production `Dockerfile`, `.dockerignore`, `tools/image/smoke.py`, `tools/image/test_smoke.py`, and `docs/runner-image.md`.
- Hosted CI owner: `.github/workflows/ci.yml`, new `deny.toml`, `.markdownlint-cli2.jsonc`, and `tools/ci/test_workflow.py`.
- Integration owner after M2 handoff: `Taskfile.yml`, `tests/test_taskfile.py`, `tools/ci/verify.py`, `tools/ci/test_verify.py`, `docs/testing.md`, and README's testing/image pointers.
- Leader-assigned status owner: `docs/roadmap.md` and `tasks/todo.md` after verified handoffs.
- Leave `tools/e2e/clone.py`, `tools/e2e/test_clone.py`, `tools/e2e/Containerfile`, clone fixtures, Cargo dependencies, engine source, operator repository, and third-party notice texts with their existing owners.

The image and CI owners can work in parallel after the leader reviews this plan.
The CI owner consumes the frozen smoke command below and existing M2 Task names without changing their implementation.
Integration waits for both owners and the M2 shared-file handoff.
Do not resurrect deleted custom-libpq image or publication helpers from M0.

## Task 1: Production Runner and Runtime Smoke

**Files:** Create the image-owner files above.

**Interfaces:** `python3 tools/image/smoke.py --image IMAGE` runs the selected local image through `CONTAINER_RUNTIME`, defaulting to `podman`.
It returns nonzero for missing runtime, failed subprocess, missing observation, invalid image metadata, or a failed smoke assertion.
It writes a nonempty JSON receipt to stdout with image ID, architecture, effective uid, binary version/protocol, linked libpq evidence, both client majors, and observed read-only/mount checks.
It accepts no credentials or database endpoints.
The harness uses argument arrays, bounded subprocess timeouts, and no shell interpolation of image input.

- [ ] Write offline tests in `tools/image/test_smoke.py` for failure of each required subprocess/observation, malformed metadata, uid mismatch, missing utility/major, denied writable mounts, and an unexpectedly writable rootfs.
- [ ] Run the new suite directly and confirm it fails before the smoke helper exists.
- [ ] Create a two-stage Dockerfile using the existing pinned `rust:1.99.1-trixie` builder manifest from `tools/e2e/Containerfile` and a verified digest-pinned Debian trixie runtime manifest covering both architectures.
Build with `cargo build --locked --release`; copy only required manifests, build script, source, and notices into build stages.
Install stock PGDG `libpq-dev=18.*` in the builder and `libpq5=18.*`, `postgresql-client-16`, `postgresql-client-18`, ca-certificates, coreutils, sed, and a POSIX shell in the runtime.
Use the official signed PGDG repository setup and verify its key fingerprint; keep runtime package copyright files and install repository NOTICE.
Remove package indexes and builder-only tools from the runtime.
- [ ] Set `USER 65532:65532`, `WORKDIR /work`, and default command `rpgmigrate --help` without an entrypoint that blocks operator `sh` commands.
Set the default executable search path so `psql` resolves to major 18 while preserving both absolute major paths.
Do not declare volumes or imply that the Dockerfile itself enforces read-only execution.
- [ ] Implement the smoke helper and minimal context exclusions.
Inspect and bind the immutable image ID before testing.
Run with `--read-only`, no network, dropped capabilities, `no-new-privileges`, and tmpfs mounts for `/work` and `/tmp` owned by 65532.
Check effective uid, expected architecture, version JSON, stock libpq major 18 resolved from the real executable by `ldd` and bound to the installed `libpq5` package version, `sh`, coreutils, sed, default psql 18, and pg_dump/pg_restore/psql majors 16 and 18 at their absolute paths.
Require writes in both mounts and refusal of a fresh write outside them.
Require a resolved libpq path with no missing dependencies and verify its package ownership/version; version JSON contains no linked-libpq field, so preserve that protocol unchanged.
A zero exit from a pipeline alone is insufficient evidence.
- [ ] Run the offline smoke suite, then build and run the smoke on each native architecture through Task targets from Task 3.
Retain build logs, image IDs, package/tool versions, and receipts.
Record unavailable architecture/runtime checks as unmet gates.
- [ ] Document local build/smoke commands, writable mount requirements, image layout, notice location, and the distinction between packaging smoke, M2 development E2E, and final release qualification on an exact digest in `docs/runner-image.md`.

## Task 2: Hosted Gates and Dependency Policy

**Files:** Modify/create the hosted-CI owner files above.

**Interfaces:** Preserve the existing `checks` job and Task bootstrap.
Add `dependencies`, `docs`, `engine-e2e`, and `runner-image` jobs.
The engine matrix calls `task test-e2e-clone-16-16`, `task test-e2e-clone-18-18`, and `task test-e2e-clone-16-18` with `CONTAINER_RUNTIME=podman`.
The image matrix consumes Task 3's image commands on `ubuntu-24.04` for amd64 and `ubuntu-24.04-arm` for arm64.

- [ ] Write workflow contract tests asserting all five jobs, three exact version pairs, both native image architectures, no publishing/login/package-write permission, pinned action SHAs, and the unchanged strict local gate.
Test deletion of each required pair/job/architecture and replacement of a required command with an empty/no-op command.
Use Python's existing unittest pattern; avoid adding a YAML parser dependency solely for this bounded contract check.
- [ ] Run the new suite and confirm failure on the pre-M5 workflow.
- [ ] Extend the workflow with read-only contents permissions, no persisted checkout credentials, bounded job timeouts, `fail-fast: false` matrices, and pinned action SHAs.
Read the Rust version from Cargo.toml and retain the existing checksum-pinned Task bootstrap in each job that needs it.
Print actual runtime/tool versions and check Podman availability before the existing E2E commands.
The M2 harness uses Podman-specific `build --jobs` and `image exists`; setting Docker in its environment is not a supported substitution.
- [ ] Add cargo-deny with an exact verified tool version and locked installation, checking licenses and advisories for the complete locked graph, including build/dev dependencies and both image targets.
Configure `licenses.private.ignore = true` only for the unpublished root package.
Derive the explicit SPDX allowlist from actual locked dependency license files/metadata and preserve unknown/disallowed-license refusal.
Keep RustSec fetch failures fatal and introduce no advisory ignores without a separately reviewed reason.
Record native PGDG package versions and retained copyright notices as separate native-dependency evidence.
Do not add a project license field, LICENSE file, or OCI project-license label.
- [ ] Add markdownlint-cli2 through its upstream action pinned to a verified commit, with config honoring semantic line breaks and `[TOC]`.
Lint tracked repository Markdown, including the new plan and guides, while excluding build output and preserved external archives.
Disable only formatting rules that conflict with explicit repository conventions, such as line-length wrapping; do not exclude a failing project document to make CI pass.
Return any necessary substantive documentation corrections to their assigned owner.
- [ ] Run each clone Task in its own hosted matrix job using the M2 development image and exact existing receipt validator.
Upload only sanitized reports/logs from declared paths, not environment files or row contents; missing success receipts fail the job.
Add the native image build/smoke jobs and retain their image-ID receipts as artifacts.
Do not claim the M2 dev-image checks ran against the production runtime.
- [ ] Keep GHCR login and push steps absent.
Document that future authorized publication must be tag-only and depend on successful qualification; project licensing and publishing authorization remain release handoffs.
- [ ] Run the workflow contract suite and YAML lint with the existing `tools/ci/yamllint.yml` configuration, then obtain fresh hosted results at milestone close.

## Task 3: Integrate Task Commands and Portable Inventories

**Files:** Modify the integration-owner files above after their handoff.

**Interfaces:** Add `image:build` and `image:smoke` Task targets.
`image:build` builds the local native platform from the production Dockerfile as `localhost/rpgmigrate:dev` with the selected container runtime.
`image:smoke` tests that local image through the Task 1 helper without implicitly rebuilding it.
Add `IMAGE` as an overridable Task variable with that default; preserve `CONTAINER_RUNTIME` CLI-over-environment precedence and existing command-prefix behavior.

- [ ] Extend Task tests to assert exact build/smoke argv, image override handling, runtime forwarding, failures, and preservation of all existing M2 targets.
Remove only the old assertion that the newly restored image targets must be absent.
Add `tools/image/test_smoke.py` and `tools/ci/test_workflow.py` once each to the exact portable CI inventory and update its inventory tests.
- [ ] Run those focused tests and confirm RED for missing Task targets/inventory entries.
- [ ] Implement the two targets using existing Task quoting/export patterns and register the suites through `tools/ci/verify.py`.
Keep the existing unit runner's nonempty, no-skip, no-optimized-Python behavior.
Do not add a second test runner or replace `tools/test-db.py`.
- [ ] Run the focused Task/inventory/image/workflow suites.
Update `docs/testing.md` and README pointers with the new commands and their qualification scope; replace the statement that all CI builds no runner image.
Preserve the distinction between local `checks`, development E2E, production packaging smoke, and later M3/M4 follow/drain gates.

## Milestone Acceptance

- [ ] Run `task check lint test` and retain command output; any existing strict-lint failure leaves the closing gate open.
- [ ] Run `task test-db` against stock PostgreSQL 16 and 18 profiles with actual server/libpq versions recorded, preserving existing exact receipts and required cases.
- [ ] Obtain hosted clone results for 16→16, 18→18, and 16→18, with independently checked contents and the M2 case inventory intact.
- [ ] Obtain native amd64 and arm64 production image builds and read-only uid-65532 smoke receipts, bound to immutable image IDs.
- [ ] Obtain locked dependency/license/advisory and documentation-lint results without suppressing failures.
- [ ] Complete the leader's independent milestone review and record which Go suite is applicable; no operator change means no operator Go execution.
- [ ] Update roadmap and task ledger together with observed results and remaining release holds.
Leave the GHCR publication checkbox open until licensing and publishing authorization are resolved.
Do not describe image smoke as migration, crash-recovery, power-loss, or operator qualification.

## Verified External References

The [PGDG Debian instructions](https://www.postgresql.org/download/linux/debian/) describe signed packages for Debian trixie on amd64 and arm64.
The [hosted-runner reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners) documents native Ubuntu 24.04 amd64 and arm64 runners.
The [Ubuntu runner inventory](https://github.com/actions/runner-images/blob/main/images/ubuntu/Ubuntu2404-Readme.md) lists Podman; CI still checks the actual executable/version instead of treating that mutable inventory as qualification.
The [Podman build documentation](https://docs.podman.io/en/latest/markdown/podman-build.1.html) requires emulation for foreign-platform RUN instructions, which we avoid by using native runners.
Use the upstream [cargo-deny license policy](https://embarkstudios.github.io/cargo-deny/checks/licenses/cfg.html), [advisory policy](https://embarkstudios.github.io/cargo-deny/checks/advisories/cfg.html), and [markdownlint action](https://github.com/DavidAnson/markdownlint-cli2-action) documentation when fixing exact tool/action pins during implementation.
