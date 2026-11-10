# M7 Kind Bootstrap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development under the team leader.
> Steps use checkboxes for tracking.

**Goal:** Prove one operator-driven legacy PostgreSQL 17 to 17 migration in an owned Podman-backed kind cluster, then reuse that isolation for the qualified native matrix.

**Architecture:** Add guarded Task targets around kind and the existing operator E2E suite.
Keep cluster identity, kubeconfig, image archives, and receipts in one private run directory outside Git.
The bootstrap slice uses the legacy engine; native execution waits for M3, M4, M5, and M6 evidence.

**Tech Stack:** kind 0.33.0, Podman 6.1.4, existing VM skopeo 1.22.2, kubectl, Helm, Task, Python standard library, existing Go/Ginkgo suite, and CloudNativePG.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md), [roadmap M7](../../roadmap.md), [M5 plan](2026-11-08-m5-packaging-ci.md), and [M6 plan](2026-11-08-m6-native-adapter.md).

## Public registry transport amendment, 2026-11-10

The user-approved public GHCR direction supersedes this plan's archive/import/alias and local-manager transport instructions.
Historical failed receipts and old RUN evidence retain their original identities; they do not qualify the successor transport.
Publish only the exact manager and native development packages with no-overwrite `dev-<full-40-hex-commit>-<arm64|amd64>` tags.
Ordinary publication runs in the exact admitted feature-branch workflow.
The separately approved first native-package bootstrap is an explicit exception, not a manager or general local publisher.
Publicity-pending receipts cannot qualify a lock, and visibility continuation checks the original digest without rebuilding or pushing.

Keep the tracked upstream catalog static and generate a private schema 2 public-registry lock from exact source and publication receipts.
Every selected Linux image binds its original public repository, runnable manifest, optional index, config, architecture, variant, and typed public provenance.
PostgreSQL retains the major tag with the digest for CNPG major discovery.
The suite input is schema 2 and requires `image_lock_sha256`, the SHA-256 of the exact lock copied to `RUN/image-lock.json`.
The Go side independently validates the bytes and source identities before workloads start.
The source packet, selected images, per-node CRI bindings, ownership, original clocks, caps, and cleanup authority must all agree.
Reject archive fields, imports, retagging, aliases, local registry references, and schema 1 on this route.

The guarded prepare command requires explicit `RUN_DIR`, `IMAGE_LOCK`, and `RECEIPT_DIR` through its Task target.
Use `pull` after guarded UP; the old `load` target refuses without effects.
The legacy Task selects only the positive bootstrap case inventory and fails when it is empty.
The native Task retains its refusal until its runtime gates pass.
The private M6 runtime driver Task interface remains an integration dependency until its owner freezes the exact source and CLI; tracked commands must not embed private source paths.
Hosted offline contract CI is separate from publication and runtime qualification.
Hosted kind runtime activation requires the bounded Linux-native-host transport follow-up because the Guard currently uses a Podman VM.
M6–M10 remain open; no production native admission, release, or benchmark gate closes from this source amendment.

## Global Constraints

- Implement operator files only in the leader-assigned feature checkout and freeze its complete source inventory before each authorized manager build.
- Read that checkout's current instructions before edits and preserve its existing work.
- Leave the original operator checkout, private ops notes, and unrelated repositories alone.
- Never query or mutate the user's selected Kubernetes context.
- Never overwrite, merge into, or switch the host kubeconfig.
- Never create a registry, publish images, or change registry credentials for this slice.
- Use the compatibility baseline `86bf908adca02d87063849dae775a02c1659c829`; do not rebuild the fork to investigate behavior.
- Keep omitted engine selection and explicit `pgcopydb` on the legacy path.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources belonging to another run.
- Keep credentials and row contents out of committed fixtures, diagnostics, argv, and receipts.
- Run independent review and full Go suites once at milestone close.
- Commit only after required checks pass and the leader requests it.

## Review Focus

- An inherited kubeconfig or Task environment override must fail before a Kubernetes request, covered by Task 1 tests.
- A same-name foreign cluster or replaced node must prevent teardown, covered by Task 1 tests.
- A missing or wrong-architecture image must fail before starting fixtures, covered by Task 2 tests.
- A suite that skips every selected spec must fail qualification, covered by Task 3 receipt tests.
- Native capabilities or drain evidence missing from an otherwise successful Job must prevent native qualification, covered by Task 4 tests.

## Planning evidence and open prerequisites

Read-only inventory on 2026-11-08 found kind 0.33.0, Podman 6.1.4, kubectl 1.37.2, Helm 4.3.1, Task 3.54.0, and an ARM64 host.
That initial inventory observed a Podman machine with 9 CPUs and 12288 MiB configured memory, with cgroup v2 and systemd cgroup management.
The later approved resize and the 2026-11-09 admission correction below supersede that capacity snapshot.
These facts establish tooling availability only.
No cluster was created or contacted for this plan.

The inspected operator HEAD was `5e8cbcbbe7ad9b89cb80a7edc1d5004af5cd4e58` on `feat/native-contract-v1`.
Its suite pins `operatorTag=v0.11.3`, derives `runnerTag` from it, constructs CNPG image names from mutable major tags, and has no kind Task target.
Its default single-process pair requests two 4Gi database pods and a 4Gi seed pod, so reducing scale alone does not make it fit alongside M3.
`E2E_SMALL` must reduce requests and PostgreSQL caches as well as data size.

The initial ARM64 v0.23.2 runner failed executable qualification: it contains `0.18.129.gd7547b5`, not the required `0.18.144.g86bf908`.
The approved ARM64 inventory correction selects v0.25.0 at manifest `sha256:5e5b148d63f4b3327c20d22daeef0e62fc85a530418671cc0b4a88ed2960be2a` and config `sha256:28d72892ba637fd609b77ed36b975adb4603b60b7499287c981e800b05518ee8`.
Its observed executable matches the retained pinned builder binary, as recorded in the roadmap.
Keep the rejected runner evidence and qualify each new preparation from actual bytes.
The builder pin is `sha256:ef596ea105604ef128244057dd34411f433d4c40fdf897bcaa8b33793df9fec8` and its source argument is the required baseline.
No registry authorization failure was observed for those reads; pull access, local runner executable provenance, manager build, and complete image loading remain execution checks.
Do not claim that the cached runner is baseline-qualified from its tag alone.
Podman manifest inspection rejects a reference containing both tag and digest; use the digest-only reference for inspection.

The dated roadmap amendment permits image preparation and kind alongside M3/M4 database work when measured capacity preserves the approved concurrent budget.
Budget kind at 6 CPUs/12Gi, image builds at 3 CPUs/5Gi, the existing PostgreSQL harness at 6 CPUs/6Gi, and VM reserve at 2 CPUs/6Gi.
Preparation with the full PostgreSQL pool requires 11 CPUs/17Gi including reserve; steady kind with that pool requires 14 CPUs/24Gi including reserve.
Do not overlap image preparation, kind, and the full PostgreSQL pool: their 17-CPU allocation including reserve exceeds the approved 14-CPU VM.
Only a leader-admitted lower PostgreSQL profile may permit that overlap, with aggregate VM work at no more than 12 CPUs.
Require at least 10Gi VM disk headroom and preserve host Cargo headroom.
Combined VM work at 12 CPUs permits one host Cargo job allocation of 2; two such host allocations require VM work at no more than 10 CPUs.
Run one database pair and one Ginkgo process at a time.
Schedule uncapped kind creation only while full PostgreSQL experiments and host Cargo builds are idle; allow steady kind/PostgreSQL parallel work after Task 12 verifies node caps.
If measured free resources or node overhead cannot meet these reservations, defer the run.
VM resizing, restarting the machine, or stopping another owner's containers requires a separate user decision.

## Ownership and dependencies

All implementation paths below are relative to the assigned operator checkout.
The leader must resolve overlaps with M6 before assigning these owners.

| Owner | Exclusive files |
| --- | --- |
| Bootstrap | `hack/e2e-kind.py`, `hack/test_e2e_kind.py`, `test/e2e/kind/cluster.yaml`, `test/e2e/kind/images.json` |
| Suite integration | `Taskfile.yml`, `test/e2e/e2e_suite_test.go`, `test/e2e/parallel_test.go`, `test/e2e/external_config_test.go`, `test/e2e/progress_pooling_test.go`, new `test/e2e/kind_config_test.go`, new `test/e2e/kind_bootstrap_test.go` |
| Qualification | new `test/e2e/native_test.go`, new `test/e2e/native_receipt_test.go`, `.github/workflows/e2e-kind.yml`, `test/buildconfig/buildconfig_test.go` |
| Documentation | `CONTRIBUTING.md`, new `docs/operations/e2e-kind.md`, `mkdocs.yml` |
| Closing verifier | No implementation files |

Bootstrap and suite integration can proceed in parallel after freezing the command and image contract below.
Qualification starts with receipt/refusal tests; native runtime specs wait for the producer handoffs.
The leader retains `tasks/todo.md` ownership in rpgmigrate.

## Shared command and isolation contract

`hack/e2e-kind.py` exposes `prepare`, `check`, `up`, `load`, `run`, `down`, and `self-check` subcommands after the amendment below.
Task targets are `e2e:kind:prepare`, `e2e:kind:check`, `e2e:kind:up`, `e2e:kind:load`, `e2e:kind:legacy`, `e2e:kind:native`, and `e2e:kind:down`.
Every target takes the same explicit `RUN_DIR` pointing at an absolute, private, nonsymlink directory created with mode 0700.
Create kubeconfig and secret-bearing files under it with mode 0600; never print their contents.
Keep archives and sanitized receipts there and never add them to Git.

Use a random run ID and cluster name `rpgmigrate-m7-<run-id>`.
Write a nonsecret ownership ledger with run ID, exact cluster name, node container IDs, expected kind labels, kubeconfig path, context name, and image IDs/digests.
Before any Kubernetes request, validate that the kubeconfig is the private file, contains exactly the owned context, and names only the owned cluster.
Every child gets `KUBECONFIG` replaced with that single absolute file, not appended to inherited paths.
Every kubectl call also supplies `--kubeconfig` and `--context`; Helm receives the equivalent flags.
The wrapper, not caller-supplied `E2E_CONTEXT`, invokes a guarded Task target which assigns the confirmed private context in its command.
Reject conflicting environment and Task variable overrides rather than silently accepting them.

Set `KIND_EXPERIMENTAL_PROVIDER=podman` on every kind operation.
Use one control-plane and two worker nodes without host database ports or host data mounts.
Pin `kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed`, an ARM64/AMD64 image listed for kind 0.33.0.
Pin CNPG 1.30.1 from its version tag and verify manifest SHA256 `37237f145d8138256ea25ae830f87759255665ff08f8d552fdd8224a5ec032fb` before applying it.
CNPG 1.30 documents Kubernetes 1.36 and PostgreSQL 14 through 18 as supported.
The references are [kind release images](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0), [Podman requirements](https://kind.sigs.k8s.io/docs/user/rootless/), [CNPG compatibility](https://cloudnative-pg.io/docs/1.30/supported_releases/), and [CNPG installation](https://cloudnative-pg.io/docs/1.30/installation_upgrade/).

`images.json` records exact image references, architecture, expected digest, and role: node, CNPG controller, each PostgreSQL major, manager, legacy runner, native runner, seed/tool runner, and OTel collector.
The implementation owner resolves and records every remaining digest before `up`; missing pins are an explicit preflight failure.
Reuse the runner image for seed/tools where the existing suite does, without confusing it with engine selection.
Build the manager from the actual assigned operator tree, record its source hashes, save images as private archives, and use `kind load image-archive --name <owned-name>`.
Load images into every node and verify containerd image identities before applying workloads.
Use `IfNotPresent` or `Never` consistently for loaded images; never use `latest` or silently pull a substitute.
Use in-cluster source/target Services and generated namespace-local Secrets only.

## Task 1: Guarded lifecycle and teardown

Owner: Bootstrap.

- [ ] Add subprocess-fake tests in `hack/test_e2e_kind.py` for hostile inherited kubeconfig, conflicting overrides, foreign same-name cluster, symlink/private-mode failure, stale node ID, and a failed ownership lookup.
  Assert zero kubectl/Helm mutation calls and zero deletion calls on each refusal.
- [ ] Implement the lifecycle wrapper and pinned kind configuration.
  Create only after a resource lease and successful `check`; use kind's explicit `--kubeconfig`, `--name`, and bounded `--wait` flags.
- [ ] Stamp owned node containers and Kubernetes namespaces with `pgcopydb-operator.io/m7-run=<run-id>` after verifying exact IDs.
  Validate kind's cluster/node labels as well as the custom marker before reuse or deletion.
  Creation failure retains its ledger for bounded cleanup and never claims readiness.
- [ ] Make `down` idempotent for the recorded owned cluster only.
  Require matching ledger and node identities before deletion; missing cluster succeeds only after a successful exact provider lookup proves absence.
  Permission errors, failed enumeration, partial foreign replacements, and unknown ownership fail closed.
  Delete no unrelated containers, images, volumes, contexts, namespaces, or networks.
- [ ] Run `python3 hack/test_e2e_kind.py` and `python3 hack/e2e-kind.py self-check` with fakes only.
  Both must exit zero with all negative controls executed.

## Task 2: Image load, private fixtures, and legacy first slice

Owners: Bootstrap and Suite integration, each editing only its assigned files.

- [ ] Add image-contract tests for missing archive, digest mismatch, wrong architecture, unavailable manifest, and absent loaded image.
  Test that failed registry access reports only image role and error class, without auth-file contents or raw credential-bearing stderr.
- [ ] Add `E2E_ENGINE` with legacy default, `E2E_RUNNER_IMAGE`, `E2E_SMALL`, and an explicit local manager image override to the suite.
  Apply full image selection to manager, migration Jobs, seed Jobs, and helper/tool Jobs; preserve existing tag defaults outside this path.
  Keep legacy preflight and major rules unchanged.
- [ ] Define the small profile as scale 0.01, one process, one instance per shared cluster, each database request 500m/1Gi, each seed/helper 100m/128Mi, and each migration runner 500m/512Mi.
  Use shared buffers 128MB, effective cache size 384MB, maintenance work memory 64MB, and WAL buffers 16MB.
  Preserve existing volume floors and WAL headroom; small data size must not silently shrink safety margins.
  Test effective requests/caches and command-line/environment precedence.
- [ ] Add Task targets that call the wrapper and pass the private context through the existing suite guard.
  Bootstrap installs pinned CNPG, waits for webhook readiness, applies this checkout's Migration CRD, waits for Established, and checks storage provisioning with bounded waits.
  Keep all fixture namespaces inside the owned cluster; retain existing namespace names and ownership-label checks.
- [ ] Add one positively selected `kind-bootstrap` legacy spec that creates an explicit `engine=pgcopydb` 17:17 Migration, follows committed writer load, performs cutover, and independently verifies nonempty fixture data and sequence state.
  Reuse existing lifecycle/verification helpers rather than running the entire legacy suite.
  Record exact source tree/image identities, executed spec count, terminal Complete with CutoverCompleted, verification success, and teardown result.
- [ ] Execute the new commands only after their implementation and the leader's live resource lease.
  Run `task e2e:kind:prepare RUN_DIR=<private-directory>`, `task e2e:kind:check RUN_DIR=<private-directory>`, then `task e2e:kind:up RUN_DIR=<private-directory> LEASE_SECONDS=1800`, `task e2e:kind:load RUN_DIR=<private-directory>`, and `task e2e:kind:legacy RUN_DIR=<private-directory>` from the operator checkout.
  End with `task e2e:kind:down RUN_DIR=<private-directory>` and repeat down to prove idempotence.
  A missing image, skipped spec, failed data check, or unresolved cleanup blocks this slice.

## Task 3: Positive receipts and hosted bootstrap

Owner: Qualification.

- [ ] Add receipt tests for zero specs, skipped-only results, missing pair, wrong image/tree, wrong engine, missing terminal reason, missing verification, and teardown failure.
  Each must refuse qualification even when the suite process exits zero.
- [ ] Add `.github/workflows/e2e-kind.yml` for the same small legacy target on an owned hosted runner cluster, using Podman rather than an implicit Docker fallback.
  Pin actions and tooling under existing repository policy, and do not publish images.
  Retain sanitized failure receipts and run owned teardown in an unconditional final step.
- [ ] Extend build-configuration tests to pin the exact target, positive selection, image archive path, private kubeconfig handling, provider, and cleanup step.
  Hosted AMD64 receipts qualify only their exact artifacts; local ARM64 receipts do not substitute for them.

## Task 4: Native matrix after producer qualification

Owners: Suite integration and Qualification after the leader accepts M3/M4/M5/M6 handoffs.

- [ ] Require qualified discovery capabilities and actual producer evidence for follow, recovery, cutover, verification, progress, and cleanup before selecting native specs.
  Until then, `e2e:kind:native` exits nonzero with the named missing prerequisite before creating a Migration.
  Do not fabricate enabled capabilities or bypass `UnsupportedEngine`.
- [ ] Make `checkMajors` engine-aware with native pairs exactly 16:16, 18:18, and 16:18.
  Under native, disable DropIfExists, filters, binary COPY, split parts, and large objects explicitly in each Migration.
  Preserve legacy configuration and version rules with regression tests.
- [ ] Add positive `native` labels for live writer follow, manual/automatic cutover, verification, runner kills mid-copy/mid-stream/mid-drain, suspend/resume, deletion cleanup, and target primary kill with required synchronous replication.
  Use two target instances for synchronous failover and verify CNPG's actual synchronous durability state before the kill.
  Recheck the resource lease before enabling that larger shape.
- [ ] Run `task e2e:kind:native RUN_DIR=<private-directory> PG_SOURCE=16 PG_TARGET=16`, then 18:18, then 16:18, sequentially with fresh owned database fixtures per pair.
  Receipt success requires executed native cases, Complete with CutoverCompleted, independently bound native drain proof, SQL verification match, and owned cleanup for every pair.
  Missing evidence, recovery gaps, or unsupported operations fail explicitly.

## Task 5: Operator documentation and milestone close

Owner: Documentation, then Closing verifier.

- [ ] Document the Task sequence, private directory lifecycle, tool/resource prerequisites, image resolution failures, and owned teardown in `docs/operations/e2e-kind.md`.
  Link it from `CONTRIBUTING.md` and `mkdocs.yml`; keep private cluster facts and secret values out.
- [ ] Run the operator's mandated prose lint on the new published page, then hand off all owners' focused evidence to the leader.
  Do not run broad Go suites or claim runtime completion during planning.
- [ ] After file freeze, have the independent closing verifier run `task lint`, `task test`, and `task docs` once under current operator policy and the leader's assigned resource window.
  Resolve findings through the owning workers.
  Keep legacy-bootstrap acceptance distinct from full M7 acceptance until the three native pair receipts and hosted job pass.

## Acceptance and deferred work

The first reviewable slice consists of safe bootstrap/teardown, all lifecycle negative controls, pinned loaded images, and one executed legacy 17:17 cutover with an independent nonempty data match.
Full M7 additionally needs all three native pairs, the listed fault cases, bound drain evidence, and hosted execution on exact tested artifacts.
M8 feature widening, performance benchmarking, production/shared-cluster execution, registry publication, and VM reconfiguration are outside this plan.

## 2026-11-08 amendment: reproducible artifact preparation

This amendment closes the demonstrated preparation gap within the approved M7 design.
It supersedes the manual build/save instructions in the private `/tmp/m7-artifact-preparation-plan.md` handoff and the older resource and runner assumptions corrected above.
The leader must review this addendum before assigning implementation or starting the final manager build.
We keep one preparation entry point in the existing wrapper because its ownership, source, archive, baseline, and storage checks already define the acceptance boundary.
We do not add another executable, dependency, configuration schema, chart API, framework, or image-pin change.

### Evidence and scope

Before the first reproducibility amendment, the integrated wrapper could verify the required receipts but could not create them from a fresh private run directory.
The successful artifact attempt used a private Python script with a hardcoded earlier source hash; publishing instructions to run that script or construct JSON would preserve the gap.
Its seven role archives demonstrate the procedure, not final-source manager qualification.
The later guard join changed the source inventory, and the actual Task check correctly refused the stale manager before cluster creation.
Freeze preparation code, focused tests, Taskfile, and operator documentation before the final manager build.
Any later source-file, mode, recipe, or inventory change requires a fresh manager build and archive bound to the new source inventory.

Published images use the existing VM's skopeo 1.22.2 route because the observed Podman OCI save recompressed layers and changed the pinned manifest.
Skopeo's [copy documentation](https://github.com/containers/skopeo/blob/main/docs/skopeo-copy.1.md) specifies that `--preserve-digests` refuses transformations and that `--image-parallel-copies` bounds concurrent layer copying.
Its [inspect documentation](https://github.com/containers/skopeo/blob/main/docs/skopeo-inspect.1.md) distinguishes raw manifests from the formatted inspection result.
Keep the manager's local build/config/archive identity separate from published repository digests.
Use the existing [Podman build options](https://docs.podman.io/en/latest/markdown/podman-build.1.html) and unchanged manager Dockerfile.
Do not accept the rejected recompressed node archive, rewrite published pins, or fabricate an upstream manager digest.

### File ownership and interfaces

- Preparation owner: modify operator `hack/e2e-kind.py`, `hack/test_e2e_kind.py`, and `Taskfile.yml` only.
  This owner replaces the previous lifecycle/Taskfile leases after the leader confirms their handoff.
- Documentation owner: modify operator `CONTRIBUTING.md`, create `docs/operations/e2e-kind.md`, and update `mkdocs.yml` only as needed for the guide link.
  Start from the frozen preparation interface and finish before the source freeze.
- Integration/artifact owner: verify the guarded join, freeze the complete integrated source, and run the tracked preparation Task in a fresh private directory under the assigned resource budget.
  This owner does not repair receipts by hand.
- Independent closing verifier: review the final diff and run required milestone gates once, after the owners' focused checks.
  This owner makes no implementation edits.

Add `Guard.prepare(self) -> dict`, dispatched by `prepare --run-dir <absolute-private-directory>` under the existing lifecycle lock.
Add Task `e2e:kind:prepare` with required `RUN_DIR`, calling that entry point with Task's existing `shellQuote` pattern.
Extend `Guard.inventory(self, *, require_manager_build=True) -> list[dict]` to select the exact existing seven roles before the manager receipt exists.
Only preparation's initial selection may pass `require_manager_build=False`; `check`, `up`, `load`, and suite input retain strict manager receipt requirements.
Do not write a placeholder manager identity to satisfy inventory selection.

Keep `ownership.json:manager_build` at exactly `ref`, `architecture`, `image_id`, `source_tree_sha256`, and `build_argv`.
Generate the existing `source-inventory.json`, `manager-build-execution.json`, `artifacts`, `legacy_baseline`, and `bundled_storage` evidence expected by `manager_provenance`, `archive`, `baseline_provenance`, and `bundle_provenance`.
Reuse `initialize`, `read`, `save`, `write_private`, `source_inventory`, `source_tree`, `validate_image`, `archive`, `layer_members`, and the existing provenance checks rather than adding parallel validation rules.
Preparation must finish by running the same `check` acceptance path as the standalone Task.
It may inspect provider images and capacity, pull exact image pins, build, archive, and run the bounded qualification containers; it must make no cluster creation, kubectl, Helm, load, or migration request.
The preparation command's success does not create a live resource lease.

Add explicit `--lease-seconds` for the existing `up` command, required when invoking the wrapper directly.
Task `e2e:kind:up` supplies `LEASE_SECONDS`, default `1800`, using a quoted argument.
Accept decimal integers from `30` through `7200` only.
After successful source/artifact, fresh-run, ownership, and measured-capacity checks, record the existing `resource_lease` fields as `authorized=true`, `m7_cpus=6`, `m7_memory_mib=12288`, `m3_reserved_cpus=6`, `m3_reserved_memory_mib=6144`, and `expires_at=time.time()+lease_seconds` before kind creation.
The existing `m3_reserved` keys reserve the shared M3/M4 PostgreSQL pool; retain their names.
Require a finite, unexpired lease and recheck actual usable capacity and `MemAvailable` at the existing live effect boundaries.
Include VM reserve in measured admission and follow the mutually bounded image/kind windows above without introducing another lease framework.
Do not add automatic renewal, indefinite authorization, or a JSON-editing step.
Preserve exact-owned cleanup after lease expiry or failed creation.

### Task 6: Generate preparation receipts from tracked commands

**Files:** operator `hack/e2e-kind.py`, `hack/test_e2e_kind.py`, `Taskfile.yml`.

**Consumes:** existing image inventory, Dockerfile/base constants, private ledger, source inventory algorithm, and frozen receipt validators.
**Produces:** `Guard.prepare`, `e2e:kind:prepare`, and all private proofs accepted by the unchanged identity checks.

- [ ] Write a fake-command end-to-end preparation test starting with an empty mode-0700 directory and no prebuilt JSON.
  Assert seven exact role receipts, all expected private proof files, exact five-field manager binding, and successful existing `check` validation.
  Assert no kind creation, Kubernetes request, Helm request, archive load, or migration command.
- [ ] Run `python3 hack/test_e2e_kind.py` and retain the missing-entrypoint failure before implementation.
- [ ] Implement `prepare` with the shared lock and guarded initialization.
  Refuse a foreign nonempty directory, duplicate roles, unsupported architecture, symlink/hardlink, nonprivate modes, concurrent command, or an existing live/down run before external preparation effects.
  Set umask 077 at each actual producer boundary, including a separate explicit umask inside the single remote child command; the host process umask does not constrain guest SSH children.
  Follow Task 9's producer inventory and private execution contract, verify the VM sees the same private output directory using an owned sentinel, and record only exact created paths and returned container IDs before further validation can fail.
  Use the existing evidence file `artifact-preparation-evidence.json` for bounded attempts and failures; do not create a second configuration or recovery state machine.
- [ ] Inspect existing tools and their actual `--help` before acquisition.
  Verify the supported Podman VM route, skopeo raw/copy options, source/target architecture, and available VM/host output space without installing tools or changing VM settings.
  Check actual VM CPU/memory availability and at least 10Gi VM disk headroom before pulls/build/save and after each large output.
  Refuse unavailable tools, unsupported flags, registry errors, or insufficient capacity with role and error class only.
- [ ] Capture `source_inventory()` before any build and write it through `write_private`.
  Its entries remain `[relative_path, file_mode, sha256]`, from sorted deduplicated `git ls-files -z --cached --others --exclude-standard` results, including tracked inputs and untracked implementation files.
  Reject empty, missing, linked, or out-of-root inputs.
  Hash canonical JSON with separators `(',', ':')`; never store this hash in `images.json` or another source input.
- [ ] Fetch exact raw upstream indices and selected child manifests with skopeo `inspect --raw --no-creds` through the validated VM route.
  Hash returned bytes before JSON parsing; formatted Podman manifest output is not raw digest proof.
  For `node`, `builder-base`, and `runtime-base`, write `<role>-index-registry-raw.json` and `<role>-manifest-registry-raw.json` and require exactly one Linux/host-architecture child matching digest and descriptor size.
  Select only the current inventory's ARM64 legacy roles; keep AMD64 unqualified and native-runner/PostgreSQL 16/18 out of this slice.
  Inspect local images first and pull only missing exact pins and existing recipe bases with an explicit Linux/host platform.
- [ ] Build the manager once using the existing Dockerfile and actual source hash `S`.
  Record exact argv, source root, before/after hashes, Dockerfile SHA256, selected base descriptors, exit status, timestamps, and observed image/config identity in `manager-build-execution.json`.
  Use exactly `podman build --platform linux/arm64 --build-arg BUILDPLATFORM=linux/arm64 --build-arg TARGETOS=linux --build-arg TARGETARCH=arm64 --build-arg VERSION=m7-<first-12-characters-of-S> --cpu-period 100000 --cpu-quota 300000 --memory 5g --memory-swap 5g --jobs 1 --pull=never -f Dockerfile -t localhost/pgcopydb-operator-m7:local .` as an argv array with the derived VERSION substituted.
  Recompute the complete inventory immediately after execution and refuse any change or nonzero exit.
  Validate local Linux/ARM64 config identity before saving the exact five-field manager receipt.
- [ ] Create published role OCI archives using the observed successful skopeo procedure.
  In the validated VM route, invoke `skopeo --override-os linux --override-arch arm64 copy --preserve-digests --src-no-creds --dest-no-creds --image-parallel-copies 1 docker://<repository>@<selected-child-digest> oci-archive:<absolute-private-output>:<required-reference>` as an argv array, with a private owned TMPDIR and Task 9's explicit private remote child boundary.
  Use `shlex.join` only for the necessary machine-SSH shell boundary, and never interpolate unchecked shell text.
  Node source selection uses the pinned upstream index's ARM64 child; its output annotation must match the existing `archive` validator's required normalized reference.
  Retain the distinction between node upstream index, child manifest, config ID, and OCI archive index.
  Save only the local manager with `podman save --format oci-archive --output <private-manager-output> localhost/pgcopydb-operator-m7:local`.
  Give seed/tool its own nonsymlink byte copy of the validated legacy archive.
- [ ] Compute each role's receipt from archive bytes, then run the existing archive validator against it.
  Record archive filename/format/SHA256/size, exact ref/digest, selected manifest/config/platform, OCI index hash/descriptor, and sorted raw blob digest inventory/count.
  Reject duplicate or unsafe members, links, missing/changed descriptor bytes, empty layers, transformed published manifests, wrong annotation, wrong platform, and manager config substitution.
  Generate `artifact-receipts.json` and atomically update `ownership.json:artifacts`, preserving other ledger fields.
- [ ] Reproduce baseline qualification from the exact `BASELINE_BUILDER` pin and selected legacy runner, without rebuilding the fork.
  Inspect the pinned builder, create one uniquely named stopped extraction container, retain its actual returned ID in `baseline-builder-create.stdout`, copy `/pgcopydb` privately, and remove only that verified ID.
  Create a separately recorded runner qualification container with no network, read-only root, dropped capabilities, nonroot user, no host mounts, and the existing tool probe; start/attach by exact ID and retain bounded outputs before exact-ID cleanup.
  Require `pgcopydb version 0.18.144.g86bf908`, builder/runner binary SHA256 equality, the image's default nonroot user and `/work` workdir contract, actual uid/gid/workdir and utility results, plus equality with `usr/lib/postgresql/18/bin/pgcopydb` read from the validated runner archive layer.
  Generate `<architecture>-runner-qualification.json`, `legacy-baseline-evidence.json`, `baseline-builder-inspect.json`, `baseline-pgcopydb`, and ledger `legacy_baseline` from those observations.
  Preserve the trusted pin/source chain and never qualify from a version substring, historical receipt, or expected hash alone.
- [ ] Generate bundled-storage proofs by reading the validated node archive, without starting a node or extracting archive paths to disk.
  Reuse `layer_members` for `kind/manifests/default-storage.yaml` and `var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/*`.
  Bind exactly `storage-provisioner` and `storage-helper` references in the storage manifest to their embedded index, one Linux/ARM64 child, config, and all layer bytes.
  Fetch each declared reference's raw registry index anonymously and require byte equality with its embedded index.
  Generate `node-default-storage.yaml`, `<storage-role>-registry-index-raw.json`, `storage-ref-bindings.json`, `bundled-storage-receipt.json`, and ledger `bundled_storage`, including node archive/index/child bindings and the count derived from verified inner blobs.
  Treat the prior 28-blob observation as evidence, not a magic count that bypasses verification.
- [ ] Make reruns accept only the same owned private directory in phase `new`, with unchanged source and fully revalidated completed outputs.
  Preserve failed/partial outputs and failure receipts; never pass check while a required operation or proof is incomplete.
  Resume only recorded, owned pending work after byte/identity checks, and refuse changed or unrecorded collisions rather than overwriting or deleting them.
  A changed source requires a fresh run directory and manager build.
  Never prune images, caches, volumes, foreign containers, previous run directories, or rejected archives.
- [ ] Finish by comparing source inventory again, validating the current manager tag, and executing the existing `check` acceptance path.
  A success receipt requires seven qualified roles and complete baseline/storage proofs, without runtime or AMD64 claims.
- [ ] Run `python3 hack/test_e2e_kind.py` and `python3 hack/e2e-kind.py self-check` with all external operations faked.
  Require all tests to execute and pass; no database, registry, build, VM, or cluster action belongs to these focused checks.

### Task 7: Remove manual lease JSON from the existing up path

**Files:** operator `hack/e2e-kind.py`, `hack/test_e2e_kind.py`, `Taskfile.yml`, owned by the same preparation worker.

**Consumes:** Task 6's complete private proofs and the fixed resource allocation above.
**Produces:** `Guard.up(self, lease_seconds: int)`, explicit wrapper `--lease-seconds`, and bounded existing-ledger lease creation.

- [ ] Add fake tests for absent wrapper lease argument, noninteger/boolean/nonfinite values, `29`, `7201`, `30`, `1800`, and `7200`.
  Assert invalid inputs produce no kind create or Kubernetes call.
  Test insufficient/malformed/duplicated `MemAvailable`, insufficient CPU/disk, stale source, tampered archive, foreign same-name node, expired lease, and failed or contradictory provider enumeration.
  Assert that successful exact lookup plus complete provider enumeration can establish same-name absence, while lookup failure cannot.
- [ ] Run the focused tests and retain failures for the missing interface.
- [ ] Implement the exact bounded argument and fixed existing-field contract above.
  Require source/artifact verification and a successful same-name ownership lookup before recording authorization, then recheck measured resources immediately before kind create.
  Keep the existing private kubeconfig, context, node, volume, and exact-owned cleanup invariants.
- [ ] Run the focused fake suite and assert accepted values produce only the fixed caps with a finite bounded expiry.
  Assert cleanup remains possible for exactly recorded nodes/volumes after authorization expires, and refuses replacements.

### Task 8: Publish and qualify the reproducible workflow

**Files:** operator `CONTRIBUTING.md`, new `docs/operations/e2e-kind.md`, and `mkdocs.yml` only if needed for navigation.

**Consumes:** Tasks 6 and 7's frozen command interface and actual focused evidence.
**Produces:** a guide that requires no private script, copied historical receipt, or caller-authored proof JSON.

- [ ] Document prerequisites, exact source/architecture scope, measured resource budget, private directory rules, anonymous digest-preserving acquisition, manager build receipt, baseline/storage qualification, failure/rerun behavior, and exact-owned teardown.
  Describe Dockerfile/Task reuse and explain why published archives use skopeo while the local manager uses Podman save.
  Keep machine names, private paths, actual capacity snapshots, credentials, kubeconfig contents, and row contents out of the public guide.
- [ ] Document the following intended command sequence after the new targets exist.
  First create a canonical private directory outside Git:

  ```sh
  umask 077
  RUN_DIR="$(python3 -c 'import pathlib,tempfile; print(pathlib.Path(tempfile.mkdtemp(prefix="rpgmigrate-m7-")).resolve())')"
  ```

  Prepare the frozen operator source:

  ```sh
  task e2e:kind:prepare RUN_DIR="$RUN_DIR"
  task e2e:kind:check RUN_DIR="$RUN_DIR"
  ```

  Under the approved bounded live allocation, run the legacy slice:

  ```sh
  task e2e:kind:up RUN_DIR="$RUN_DIR" LEASE_SECONDS=1800
  task e2e:kind:load RUN_DIR="$RUN_DIR"
  task e2e:kind:legacy RUN_DIR="$RUN_DIR"
  task e2e:kind:down RUN_DIR="$RUN_DIR"
  task e2e:kind:down RUN_DIR="$RUN_DIR"
  ```

  Explain that a failure retains private evidence and needs the guarded down command when owned cluster resources exist.
  Native and hosted/AMD64 qualification remain separate gates.
- [ ] Apply the operator's humanizer and STE100 rules, then run `python3 .claude/skills/asd-ste100/scripts/ste-lint.py docs/operations/e2e-kind.md` until it reports zero hard violations.
  Check guide/Task flag spelling against `python3 hack/e2e-kind.py --help`, `task --list-all`, and the focused command tests.
- [ ] Have the integration owner verify each joined file's expected before/after identity and explicit absences, then freeze every operator source input, including documentation, before actual preparation.
  Record the complete source inventory and final source identity privately.
  Run the tracked `task e2e:kind:prepare RUN_DIR="$RUN_DIR"` from a fresh private directory under the assigned image window, followed by `task e2e:kind:check RUN_DIR="$RUN_DIR"`.
  Require actual raw bytes, build process, baseline binary, all archive roles, bundled storage proofs, and zero cluster creation before accepting preparation.
- [ ] Give the independent closing verifier the final source/artifact identities, focused test output, and a negative-control matrix covering every refusal above.
  Include source mutation during build, omitted tracked source, replaced local manager tag, changed recipe/base/platform, failed command, duplicate role/member, transformed/tampered archive, wrong ref/architecture, missing or mismatched baseline binary, changed embedded storage index/config/layer, and partial preparation.
  Require every pre-create refusal to show zero kind create, load, Kubernetes, and Helm calls.
- [ ] Run independent M7 review and the applicable Go unit/envtest/contracts, required PostgreSQL 16/18 checks, `task lint`, `task test`, and `task docs` once at milestone close under the assigned budgets.
  Preserve `task test`'s real PostgreSQL/psql prerequisites from CONTRIBUTING; do not count skipped SQL tests as passing gates.
  Run the full tracked up/load/legacy/down/down sequence only after preparation and the live assignment are accepted.
  Require positive executed-spec, Complete/CutoverCompleted, independent nonempty data/sequence, exact loaded image, bootstrap readiness, and identity-scoped cleanup receipts.
  Do not commit before required checks pass and the leader requests it.

### Amendment success criteria and remaining decisions

An engineer starting with the frozen integrated checkout, existing qualified tools, and a fresh private RUN_DIR can produce every artifact proof with `e2e:kind:prepare`, then pass `e2e:kind:check`, without authoring JSON or referring to the earlier private script.
`up` obtains its bounded existing-ledger resource authorization from the explicit Task argument after measured checks.
All pre-create refusals remain fail-closed, evidence survives failure, and only exact recorded temporary containers or owned cluster resources can be removed.
Preparation claims remain limited to actual ARM64 artifacts; the live legacy receipt, native matrix, AMD64/hosted runs, and closing gates each require their own passing evidence.

The root-selected wrapper/Task approach, exact five-field manager contract, fixed lease fields/caps, lease range, and distinct documentation ownership are resolved decisions.
Before execution, the leader still assigns the exclusive wrapper/Taskfile owner, documentation owner, frozen integration identity, image resource window, and later live window.
Any inability to reproduce existing receipt fields, preserve required OCI manifest identities, share the private VM output path, or qualify baseline bytes is a blocker to report with preserved evidence, not permission to weaken a guard or change pins.

## 2026-11-09 replan: producer boundaries and capacity admission

This replan follows two failed actual tracked preparation attempts and requires leader review before another attempt.
The first attempt exposed an incorrect assumption that skopeo's version output was only a bare version; the accepted correction also recognizes the observed 1.22.2 output with its full commit suffix.
The second attempt built the manager successfully from its unchanged source but refused `archive-node`: the guest SSH process used umask 022, so skopeo created a mode-0644 archive instead of the required mode 0600.
Its refusal receipt is retained privately as `tracked-preparation-refusal.json`, SHA256 `431ae375fe1f5e54e7334afc2040df6ffcd57e7c9dbe9531d989821a6f1796d7`.
No archive receipt was accepted and no preparation container or kind cluster was created by that attempt.
Preserve both failures and the partial archive; do not chmod, repair, adopt, or retry their outputs.
The next attempt requires corrected producer behavior, focused checks, the bounded actual-output research gate, frozen Task 12 provider enforcement, updated documentation, a new complete source freeze, and a fresh RUN_DIR.
The earlier task checklists remain requirements, not completion claims.

### Actual producer map and acceptance risks

The map below comes from the integrated wrapper's actual preparation path.
It covers every filesystem producer, including outputs which the current fakes make artificially private.

- Remote published archives: `save_archive` calls `vm(['env', 'TMPDIR=...', 'skopeo', ..., 'copy', ...])` and then `private(path)`.
  `vm` only quotes argv for SSH; host `os.umask(0o077)` does not set the remote child's umask.
  The actual node output was mode 0644 inside the private directory, and the fake incorrectly forced mode 0600 after writing it.
  Skopeo's private TMPDIR may also contain temporary producer files; inventory and constrain those files without accepting unexplained residual paths.
- Local manager archive: `podman save --output` creates the destination, followed by `protect_output(path)`.
  The helper checks type/owner/link count, then calls pathname `chmod(0600)` without checking current pending-step provenance or holding a verified file descriptor through normalization.
  Actual research proved that an absent output under host umask 022 becomes 0644, while an exclusively precreated mode-0600 output retains that mode and the same inode through save.
  Use that proven secure creation boundary and reject descriptor/path identity changes instead of normalizing arbitrary outputs.
- Seed/tool archive: the host opens the new output with `xb`, calls pathname chmod, and copies bytes from the legacy archive.
  Use exclusive mode-0600 creation and held descriptors; verify the source is the already qualified private legacy file, and reject a replaced source or destination.
- Builder binary: `podman cp` creates `baseline-pgcopydb`, then uses `protect_output`.
  Actual research proved that cp replaces a precreated destination inode and produces mode 0755, including under host subprocess umask 077.
  Only this producer uses the new owned empty staging directory and descriptor normalization contract below.
- Builder/runner CID files: `podman create --cidfile` writes `baseline-builder-create.stdout` or `runner-create.stdout`.
  These files must be private before reading an ID or authorizing cleanup, must agree with returned stdout, and must retain the existing exact-ID/image/labels/name checks.
  Actual host subprocess umask 077 produced a mode-0600 CID file; umask 022 produced 0644.
  A mode failure after successful creation must retain the returned ID for checked cleanup without trusting an invalid CID file or guessing a name.
- Source, registry, cache, storage, and qualification receipts: captured stdout or bytes read from validated layers flow through `write_private`.
  This includes `source-inventory.json`, raw upstream/child indices, cache receipts, manager execution evidence, builder inspection, runner stdout/qualification, storage raw indices/YAML/bindings, artifact maps, and ownership/progress JSON.
  Preserve exclusive mode-0600 temporary creation, fsync, private existing-destination checks, and atomic replacement; no source or layer member is a path to extract directly.
- Build and tool logs: `execute` captures subprocess streams in memory and emits sanitized failure classes; it does not create a build log file.
  Keep that distinction in tests and documentation.
  Any bounded evidence log actually retained by the wrapper must use the same private atomic writer, and diagnostics must not expose raw stderr, credentials, or row contents.

### Private producer interface and stage completion

Extend the existing internal interface to `Guard.vm(self, args, timeout=120, *, private_output=False)`.
For an output-producing remote operation, pass `private_output=True` and construct one remote argv as `['sh', '-c', 'umask 077; exec "$@"', 'm7-private', *args]` before the existing `shlex.join` SSH boundary.
Only the fixed shell body is executable text; paths, TMPDIR, refs, and arguments remain positional values.
This sets umask only inside that child and makes no persistent VM/session configuration change.
Read-only VM calls keep their current interface and semantics.
Test paths with spaces and single quotes through both quoting layers.

Before each external or local producer starts, require the exact role/step and declared output paths to match the current in-memory invocation and persisted pending intent, and prove those paths absent.
Do not use an old persisted pending record as proof that a file was created by the current invocation.
Secure-create the manager-save and seed-copy outputs with exclusive/no-follow mode-0600 descriptors, holding and verifying their file identities through the producer.
Run host writer children under umask 077; retain that boundary for CID-file creation and verify the exact returned CID before use.
For `podman cp` only, create a new empty owned mode-0700 staging directory inside RUN_DIR and record its intended new member against the current pending invocation before the child runs.
Copy into that absent staging member, then open it with `O_NOFOLLOW` and verify the pathname/fstat UID, regular-file type, single link, and matching device/inode identity.
Normalize only that pinned newly produced file with descriptor `fchmod(0600)`, and recheck the same inode, binary SHA256, mode, and path identity before promotion.
Atomically promote to `baseline-pgcopydb` only after proving the final destination absent, both parents safe, and the same filesystem; fsync the file and parent directory through the existing publication practice.
Remove only the now-empty staging directory created by that successful invocation; failed staging remains preserved evidence and is never adopted by a later invocation.
Keep this bounded to the copied binary rather than introducing a generic staging framework.
A specific GID is not an acceptance requirement: research observed valid same-owner outputs with different groups.
Stop if current-invocation ownership or identity cannot be proven; do not fall back to pathname chmod.
A preexisting partial, foreign file, symlink, hardlink, or replaced pathname is always a refusal and must never be chmodded into acceptance.

The existing `step` failure boundary must include producer execution, source recheck, private-output validation/hashing, receipt validation, and completion callbacks.
Currently output hashing occurs after its exception boundary; move it inside so a missing mode or callback failure records failure and leaves the step pending.
Publish accepted artifact metadata only after the corresponding producer and byte/private-path checks pass.
Do not append a completed step, clear pending intent, or preserve a success marker when a declared output, validation callback, ledger save, or progress callback fails.
Keep already completed unrelated stages and all failure evidence.
Set the preparation-wide `complete` flag only after the final existing `check` path returns successfully; the current pre-check assignment can falsely mark a failed final check complete.
No new receipt schema or recovery state machine is needed.

### Corrected admission values and order

The immutable M3 outer harness reserves two PostgreSQL processes at 2 CPUs each and its runner at 2 CPUs, totaling 6 CPUs/6Gi.
The previous 3-CPU PostgreSQL allocation was incorrect.
Do not change those existing M3 hard caps or the approved next M3 case to make M7 fit.

- Preparation admission is image 3 CPUs/5120 MiB plus shared PostgreSQL 6 CPUs/6144 MiB plus VM reserve 2 CPUs/6144 MiB: 11 CPUs/17408 MiB.
- Steady kind admission is kind 6 CPUs/12288 MiB plus shared PostgreSQL 6 CPUs/6144 MiB plus VM reserve 2 CPUs/6144 MiB: 14 CPUs/24576 MiB.
- Live `resource_lease` retains its existing fields with `m7_cpus=6`, `m7_memory_mib=12288`, `m3_reserved_cpus=6`, `m3_reserved_memory_mib=6144`, `authorized=true`, and the existing bounded expiry.
  Reserve remains admission headroom, not a new ledger field.
- Image preparation and kind may each overlap the full PostgreSQL pool, subject to measured capacity.
  Their three-way overlap with that pool requires 17 CPUs including reserve and is refused on the approved 14-CPU VM.
  A lower bounded PostgreSQL profile needs separate leader admission, with aggregate VM work no greater than 12 CPUs; no new CLI flag or automatic profile reduction is authorized.
- On the 18-CPU host, host work must be no greater than `14 - VM_work_cpus`, retaining four host CPUs of headroom.
  Kind plus the full PostgreSQL pool is 12 CPUs of VM work and permits one two-job host Cargo allocation.
  Preparation plus that pool is 9 CPUs of VM work and permits two two-job host Cargo allocations.

Retain strict usable CPU/MemTotal parsing, use the smaller usable memory total, and conservatively require actual MemAvailable to cover the admitted memory allocation including reserve.
Do not substitute MemFree or infer capacity from configured totals alone.
Keep the 10Gi VM disk floor and existing private host-output disk checks.
Remove the old preparation 8-CPU check and old 25Gi all-four-windows UP check; both encode superseded allocation assumptions.
For `up`, perform source/artifact verification, fresh-run/lease checks, successful filtered and complete provider lookup, corrected measured CPU/memory/disk admission, bounded fixed-field lease save, and a fresh lease/capacity check immediately before kind creation in the root-scheduled idle bootstrap window.
After creation and exact identity/marker capture, apply and verify Task 12's per-node limits before wrapper image import, prerequisites, or workloads.
Apply the same full PostgreSQL reservation and reserve headroom at later existing live admission boundaries.
Expired or invalid authorization still refuses live work; exact-owned cleanup remains available.

### Provider enforcement fact and accepted root decision

Read-only inspection of the current integrated `Guard.up` found admission metadata, not applied node CPU or memory cgroups.
It records `resource_lease`, checks capacity, calls kind create, captures node IDs, verifies ownership, captures volumes, and writes run markers.
It makes no `podman update` call and verifies no node CPU/memory limit.
`load` validates source, archives, lease, and exact owned identities before image import, but likewise checks no provider limit.
The static cluster file contains one control-plane and two workers, with no provider resource limits.
Do not describe the lease metadata on this source as an enforced runtime ceiling.

The root's 2026-11-09 decision raises the steady kind allocation to 6 CPUs/12Gi, using 2 CPUs/4096 MiB on each exact one-control-plane/two-worker node.
This operational allocation uses the already authorized VM capacity and avoids a smaller worker memory ceiling; it changes no node count or cluster architecture.
Task 12 applies existing Podman update to those exact freshly captured and ownership-verified CIDs, then verifies provider and effective cgroup limits before image import or prerequisites.
It is a separate sequential task after the frozen Task 10 producer fix, under the same wrapper/tests file lease, and before final documentation/source freeze and the next tracked preparation.
No new API, ledger schema, global manager, confirmation flag, or renewal command is introduced.

Kind creation remains an uncapped bootstrap transient which may consume the VM's 14 CPUs.
The leader schedules `up` only while full PostgreSQL experiments and host Cargo builds are idle, leaving unrelated idle M0/M1 containers untouched.
The user procedure pauses other heavy work for `up` and resumes steady-budget parallel work after all three caps are verified.
Do not promise PostgreSQL headroom during uncapped creation or implement cross-repository M3-label discovery/global leases to simulate that guarantee.
The wrapper checks its own phase and current observations; the idle window is an explicit scheduling prerequisite.
No implementation or third preparation attempt is authorized by this plan edit alone.

### Task 9: Close producer research before implementation

**Owner:** leader-assigned research worker, with no source ownership.
**Inputs:** the producer map, actual two-attempt receipts, installed tool/help output, and existing qualified local artifacts.
**Output:** private factual producer/tool-format evidence reviewed and accepted by the leader.

The original private receipt `receipt.json` has SHA256 `b1ccf9b7bb26e4cd5cf8b9f48191defeee7b4a46d1be5e5f5f55fb820a1e8fbb`.
The accepted follow-up `host-private-followup/receipt.json` has SHA256 `5469581d6233b81f42365801fcbb6b9ee35cfe3c58aebd52e4af34db526e4ea0`.
Both reside under the retained private research directory recorded in the leader handoff; keep that absolute path outside public operator docs.
These receipts prove producer behavior and exact-owned cleanup only, not accepted preparation, actual tool RSS, or node cgroup enforcement.

- [x] Record observed Podman 6.1.4 and skopeo 1.22.2 with its actual commit suffix, guest umask, output owner/type/mode/link/inode, and finite producer command results.
  Keep raw host/provider inventory and private paths in the private receipts.
- [x] Complete the bounded actual-output probe and root-authorized host077 follow-up using existing qualified artifacts.
  Remote child umask 077 produced mode 0600 with space/single-quote paths, while the guest default remained 0022 before and after.
  Selected manifest, config, and blob bytes retained their identities; whole tar hashes changed and are recorded separately.
  Local save preserved an exclusive mode-0600 inode, and actual host077 CID-file creation produced mode 0600.
- [x] Prove the cp-only owned-staging/descriptor/atomic-promotion route.
  Cp produced 0755 and replaced precreated destinations, while descriptor normalization and promotion retained the new binary's exact inode and SHA256 at mode 0600.
  Symlink, path-swap, hardlink, and same-UID preexisting-provenance controls were refused untouched.
  All exact never-started owned probe containers were removed and absence proved; no source, database, kind, Kubernetes, or VM configuration changed.
- [x] Obtain leader acceptance of the original and follow-up research facts and the resulting producer treatment.
  Task 10 implementation still requires the corrected plan's separate review; no third tracked prepare attempt is authorized by research completion.

### Task 10: Correct every owned producer and its focused controls

**Owner/files:** one preparation owner for operator `hack/e2e-kind.py`, `hack/test_e2e_kind.py`, and Taskfile only if its existing interface requires an accompanying correction.
**Interfaces:** preserve public prepare/check/UP commands, pins, five-field manager receipt, ledger schema, and lease duration contract; use the private VM interface and admission values above.

- [ ] Make the fake tools emulate actual default producer behavior before fixing code.
  In particular, remote archive output is mode 0644 under guest umask 022 unless the actual command applies umask 077; remove unconditional mode-0600 writes from the fake skopeo branch.
  Model local save retaining an exclusive mode-0600 inode, host077 CID creation yielding 0600, and cp replacing a precreated inode with mode 0755, from Task 9 evidence instead of silently using `write_private` or test-parent umask.
  Retain captured bare and commit-suffixed skopeo 1.22.2 formats; reject unsupported versions, malformed suffixes, malformed JSON/help/CID/space/memory output, and unavailable capabilities before expensive work.
- [ ] Add parameterized controls for every producer class: expected private success, mode 0644, absent/empty output, wrong owner, symlink, hardlink, substituted pathname/inode, output collision, nonzero child, callback/hash/save failure, and unknown temporary output.
  Assert zero accepted role proof or completed stage for the failed producer, pending/failure evidence retained, and no kind create/load, Kubernetes, Helm, or migration calls.
  Verify a guest default-022 control fails without the private boundary and succeeds with it, including space/quote paths.
  Preserve exact returned-CID cleanup under post-create output failure and refuse contradictory or replaced container identity.
- [ ] Run `python3 hack/test_e2e_kind.py` and retain the intended producer/capacity failures before implementation.
- [ ] Implement the reviewed private producer boundaries, complete failure boundary, and corrected admission values with the existing helpers.
  Use the proved cp-only owned-staging/held-descriptor promotion path and remove generic post-hoc pathname chmod from producer acceptance.
  Keep read-only VM calls, source inventory, pin checks, archive verification, and public Task interfaces unchanged.
  Do not repair either historical partial or relax mode checks.
- [ ] Add capacity boundary tests for preparation 11 CPUs/17408 MiB and steady kind 14 CPUs/24576 MiB, exact live lease fields, insufficient actual MemAvailable/disk, full PostgreSQL pool plus image/kind refusal, and the stated host headroom arithmetic.
  Ensure later live checks do not fall back to a 3-CPU PostgreSQL reservation or omit VM reserve.
- [ ] Run `python3 hack/test_e2e_kind.py` and `python3 hack/e2e-kind.py self-check` with all external operations faked.
  Require every selected control to execute and pass; keep broad Go/runtime checks at milestone close.

### Task 12: Enforce and verify exact owned node provider limits

**Owner/files:** the same wrapper/tests owner, assigned sequentially after Task 10 is frozen, modifying only operator `hack/e2e-kind.py` and `hack/test_e2e_kind.py`.
**Consumes:** frozen producer/admission changes, the existing exact-node ownership/marker/volume capture, and the accepted uniform node allocation.
**Produces:** applied and freshly verified 2-CPU/4096-MiB limits on all three owned nodes, aggregate 6 CPUs/12288 MiB, before subsequent wrapper runtime effects.

- [ ] Read installed Podman update help and the actual provider inspection/cgroup formats relevant to the exact owned nodes.
  Use the existing Podman API boundary; do not change VM configuration, node count, or foreign resources.
- [ ] Add focused fake controls for exact three-node inventory/roles, each node receiving only its exact CID's update, aggregate CPU/memory totals, missing node, duplicate/foreign/replaced ID, missing/wrong labels or marker, malformed provider observations, uncapped/contradictory cgroups, failed update, and partial application.
  Assert zero update calls until complete filtered/provider inventory, exact roles, owned markers, and current-run identity all pass.
  Assert zero archive import, prerequisites, or workloads until every node's provider and effective cgroup limit passes.
- [ ] Implement the smallest existing-helper enforcement path after kind creation and exact identity/marker capture.
  Apply 2 CPUs and 4294967296 bytes memory to each exact fresh owned CID with Podman update.
  Do not select mutation targets by inherited node names, labels alone, an image reference, or a broad container query.
  Require one control-plane and two workers and preserve current marker/label/complete-inventory checks before any update.
- [ ] Verify actual HostConfig CPU/memory values and effective guest cgroup limits for each exact CID.
  Require a finite CPU quota equivalent to 2 CPUs and a memory limit of 4294967296 bytes; `max`, absent, malformed, or conflicting limits refuse.
  If the container root cgroup namespace does not expose its own applied limits, derive its positive init PID from exact-CID inspection and the corresponding VM `/proc/<pid>/cgroup` path.
  Validate that the canonical cgroup path stays under the VM cgroup hierarchy and belongs to that inspected init process before reading limit files; do not guess a cgroup from node names or unvalidated path components.
  Require exactly three matching role/CID proofs with summed limits of 6 CPUs and 12884901888 bytes memory.
  Retain sanitized proof details privately without adding another ledger schema.
- [ ] Re-read ownership, provider limits, and effective cgroups at existing image-load, prerequisite/bootstrap, and workflow effect boundaries.
  A stale saved proof alone does not authorize a later effect; missing, changed, or replaced-node limits refuse.
  Never update an unfamiliar replacement node to make it fit the ledger.
- [ ] Preserve exact owned IDs, captured volumes/markers, partial cap outcomes, and failure evidence when an update or verification fails.
  Keep exact-owned down available without claiming readiness or successful cap enforcement.
  Test cleanup after failed/partial application and refusal when ownership changed; no unrelated resource is mutated or deleted.
- [ ] Run the focused Python suite and self-check with fake provider/cgroup operations.
  Freeze Task 12's wrapper/tests handoff before final documentation/source freeze and the next tracked manager preparation.
  The actual first-up cap proof is deferred to its controlled idle bootstrap window, followed by fresh effective-limit checks before steady parallel work.

### Task 11: Correct the guide, freeze, and retry once through tracked commands

**Owners:** separate documentation owner, then integration/artifact owner, then existing independent closing verifier.
**Files:** documentation owner retains operator `docs/operations/e2e-kind.md`, `CONTRIBUTING.md`, and navigation only as needed; no implementation overlap.

- [ ] Correct the guide's 3-CPU PostgreSQL, all-four-windows simultaneous, 8-CPU preparation, and 25Gi UP forward instructions in place.
  Publish the corrected mutually bounded 11-CPU/17Gi preparation and steady 14-CPU/24Gi kind allocations, six-CPU shared PostgreSQL pool, host headroom rule, and private producer boundary.
  State the uncapped creation window truthfully: pause full PostgreSQL experiments and host Cargo builds for up, verify all three 2-CPU/4Gi node limits, then permit steady kind/PostgreSQL parallel work.
  Explain that a host umask does not configure a guest child and that failed partial outputs remain refused evidence.
  Keep actual machine facts and private attempt paths outside the public repository.
- [ ] Apply operator prose rules and run the published guide's STE100 check; check all commands against the existing Task/wrapper interface.
  Hand off documentation hashes before the source freeze.
- [ ] After both Task 10 and sequential Task 12 are frozen, guarded-join only reviewed owned files, verify explicit absences and before/after identities, and freeze the complete new operator source inventory including tests and docs.
  The accepted earlier manager build remains evidence for its earlier source, not the new source after this correction.
  Assign a fresh RUN_DIR and verify actual capacity under the corrected image-plus-full-PostgreSQL allocation immediately before the next run.
- [ ] Execute exactly the tracked `task e2e:kind:prepare RUN_DIR="$RUN_DIR"`, then tracked `task e2e:kind:check RUN_DIR="$RUN_DIR"` only after preparation succeeds.
  Require the actual newly frozen manager build, seven private archive-role proofs, raw indices/selected children/blob validation, pinned baseline binary qualification, bundled-storage evidence, completed-stage/final-check consistency, and zero kind/Kubernetes effects.
  On any failure retain source identity, stage/command exit, private output modes/owners/hashes, exact owned resources, and failure receipts; stop and return to the leader rather than repairing or immediately retrying.
- [ ] Hand off fresh actual preparation/check receipts for independent milestone review and the existing closing Go/lint/test/docs gates.
  Leave live kind/legacy and wider qualification gates open until their separate authorized sequence and positive receipts pass.
  Do not mark any task complete or commit based on this replan alone.

## 2026-11-09 Task 13 replan: remaining preparation producer contracts

This amendment follows the third actual tracked preparation and supersedes earlier forward instructions to retry after Tasks 10, 12, and 11 alone.
The third run used frozen source `1b7ca392fa63d4bd2c0662b355e3299f334df84fb19c3704a2b24d63baa2df46` with all 530 source identities unchanged before effects, after manager build, and after refusal.
Its manager and seven archive stages passed, but baseline qualification refused `runner probe output is malformed or duplicated` and preparation remained incomplete.
The wrapper progress receipt has SHA256 `ae25251cd8c81c0329f34564f607ecaebcda06c02845be3668804e6fba4ded99`; the external refusal observation has SHA256 `34a6bf8e9684d81648c8cc74c6c77491d5d6ecc796447e4652a6071478af722b`.
Private evidence locations remain in the leader handoff rather than public operator documentation.
Preserve all three failed runs without repair, adoption, receipt reconstruction, or further preparation effects.
This plan authorizes no fourth attempt or implementation until leader review.

### Observed contracts and remaining risks

`Guard.runner_probe()` interpolates the complete `pgcopydb --version` output into one line-oriented value, whereas `prepare_baseline()` requires every line to contain a unique key and `=`.
The retained `runner-probe.stdout`, SHA256 `c21eb4f56b0178ceefc779de8b3b8249ec153847936654f4caee3ddd133d32fb`, contains these exact three version lines:

```text
pgcopydb version 0.18.144.g86bf908
compiled with PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
compatible with Postgres 14, 15, 16, 17, and 18
```

The observed runner and extracted trusted builder binary both hash to `0230465af0537ca4b70c26c7ddf558da99ea964f36d64a855ba132fde2a29f3a`.
The probe also reports UID/GID 65532, workdir `/work`, PostgreSQL tools 18.6, GNU timeout 9.7, and GNU sed 4.9.
These are research inputs, not a completed baseline qualification.

Both declared CID receipts are absent after actual temporary-container cleanup.
The current wrapper gives their declared receipt names directly to Podman `--cidfile`, and its fake `rm` removes the container without removing that file.
The later `baseline_provenance()` requires the builder receipt, and baseline stage hashing requires both receipts, so correcting only version parsing leaves another producer mismatch.
Separate transient Podman CID files from canonical receipts before the next attempt.

Read-only inspection of the retained archives found one outer descriptor per archive and Linux/ARM64 configs throughout.
Their layer counts are node 2, CNPG controller 17, PostgreSQL 17 image 5, manager 14, legacy runner 6, seed/tool runner 6, and OTel collector 3.
The OTel manifest uses Docker distribution v2 media type; the other selected manifests use OCI media type.
The node contains 144 embedded content-blob members overall, not 28.
Its default-storage YAML is 4567 bytes with SHA256 `dc16a44e6a49a1f26fc39f341c361e7939993bd68431efc3a49d216e5914e903` and references provisioner `v20260820-69b56db7` and helper `v20260131-7181c60a`.
The provisioner index is `sha256:fdb5360219dcca6acc7967326f9e04c89b038e2c1e924463b2a41cad2dc752ab`; the helper index is `sha256:ac7c7f08e86f7bef7d2ad6dee944ed89d3d2e7de0ebc00490d7ddf6c26628a06`.
Each has four descriptors and exactly one Linux/ARM64 child.
Their selected index/manifest/config/layer union contains 28 distinct blobs, with the required metadata in the same outer node layer.
Use these shapes to qualify the remaining pure contracts without accepting the failed run as prepared state.

### Task 13: Frame version results, preserve CID evidence, and exercise remaining stages

**Owner/files:** one preparation implementer owns only operator `hack/e2e-kind.py` and `hack/test_e2e_kind.py`, sequentially after the frozen Task 12 handoff.
The leader assigns a distinct integration/artifact worker after this owner freezes its export.
No Taskfile, image pin, Dockerfile, public command, dependency, configuration schema, engine, or Kubernetes behavior change is needed.

**Interfaces:** retain `Guard.runner_probe() -> str`, `Guard.prepare_baseline(images) -> dict`, `Guard.temporary_container(role, image, action)`, `Guard.prepare_bundle(images) -> dict`, and the existing preparation stage order.
Add one internal static `Guard.parse_runner_probe(raw: str) -> dict[str, str]` used by baseline production and its focused tests.
Return the same normalized `actual` keys consumed by the existing baseline checks; retain full command output in the private raw probe record.
Keep the five-field `manager_build`, baseline/archive/bundle identity checks, and existing final `_check()` acceptance path.

- [ ] Add failing tests with the exact three-line version fixture above, real captured single-line psql/pg_dump/pg_restore first lines, and multiline GNU timeout/sed output.
  Assert the normalized pgcopydb version remains exactly `pgcopydb version 0.18.144.g86bf908` and every existing binary, tool, UID/GID, workdir, path, sed-escape, and progress-marker check still runs.
  Make fake `rm` remove its exact registered transient CID file as well as its container, reproducing observed cleanup rather than leaving an idealized receipt behind.
  Retain the focused RED output before changing production code.
- [ ] Frame each version-command result as one canonical base64 value with key `<tool>_version_b64`, using the existing Python `base64` import for strict host decoding.
  Inside the runner, encode the complete command stdout followed by one NUL separator and its decimal exit status.
  Capture the command status explicitly through a shell conditional before appending it; do not infer command success from a pipeline's final encoder or from `head`.
  Require the encoder itself to succeed before emitting the record.
  This retains trailing stdout newlines and exposes embedded NUL, failed commands, and missing status as refusals rather than silently dropping bytes.
  Use the existing runner's `base64 --wrap=0` only after a bounded self-test verifies exact encoding of a known multiline input and an explicit status suffix.
  Read-only archive inspection proves `/usr/bin/base64` exists as an executable regular member; it does not qualify that option or runtime behavior.
  Include the self-test in the bounded runner probe before qualification, refuse unsupported behavior, and install nothing.
- [ ] Implement strict framing acceptance in `parse_runner_probe`.
  Bound accepted UTF-8 probe output to 65536 bytes and each decoded version stdout to 8192 bytes before normalization.
  Require the exact expected scalar/encoded-version key set, unique keys, a final record newline, canonical base64 round-trip, exactly one decoded NUL separator, canonical status `0`, and valid text without CR or NUL in stdout.
  For pgcopydb, require exactly the three complete lines in the pinned ARM64 fixture, allowing only the presence or absence of one terminal LF.
  Derive the existing normalized version field only after that full-output validation.
  For the other tools, retain their full bounded output in the encoded record, validate text and a nonempty first line, then apply existing first-line qualification semantics.
  Do not skip unknown lines, accept raw multiline values, suppress duplicate keys, relax exact baseline identity, or introduce a generic permissive parser.
  The bounds are acceptance limits; do not claim a new streaming-memory or RSS guarantee for the existing subprocess capture helper.
- [ ] Test malformed/truncated base64, noncanonical padding, missing/extra/duplicate records, missing final newline, raw multiline injection, absent/multiple NUL separators, nonzero or malformed status, empty/truncated/changed three-line version output, CR/NUL/invalid text, oversized probe/decoded stdout, failed encoder, and failed capability self-test.
  Include a command that prints the expected version before exiting nonzero, so a successful-looking prefix cannot qualify it.
  Require a failed probe to leave baseline pending, `complete=false`, no accepted qualification, and exact-owned cleanup or explicit retained cleanup failure.
- [ ] Give each Podman `--cidfile` a new absent transient name bound to the current baseline invocation, separate from `baseline-builder-create.stdout` and `runner-create.stdout`.
  Record its exact intended path before creation through current pending intent, preserving the existing strict current-invocation and private-parent checks without broadening arbitrary output acceptance.
  Validate returned stdout, mode-0600 no-follow single-link CID-file bytes, and inspected exact CID/image/name/run-label/role ownership agree.
  Immediately after that agreement and before the action, securely write the canonical declared CID receipt from those validated bytes through the existing private exclusive/atomic writer and verify its identity and contents.
  Hold and revalidate the transient descriptor while the action runs, preserving the canonical receipt when the action fails and checked cleanup runs.
  Let Podman remove its own transient CID file; require successful exact-CID absence and transient-path absence before successful return.
  Do not reconstruct a receipt after cleanup or touch a historical run.
- [ ] Test mismatched/duplicate/invalid stdout or CID bytes, nonprivate/linked/replaced transient and canonical paths, canonical-path collisions, ownership substitution, failed receipt write, action failure, failed removal, failed complete inventory, and an unexpected surviving transient file.
  Preserve the already recorded returned CID as potential checked-cleanup authority on partial failure, without treating an invalid file or canonical receipt as qualification authority.
  Never remove a replacement/foreign container or unknown file, and never mark the baseline stage complete when cleanup or receipt validation fails.
- [ ] Exercise baseline archive-member and receipt readback using captured builder-inspection shape, exact runner binary member, canonical receipts surviving simulated real removal, and the full captured version fixture.
  In new private test fixtures only, read retained archive bytes without changing them or attaching their ownership ledger to a new run.
  Use read-only observations as fixture inputs, not as accepted preparation or cached final-source provenance.
  Keep the default focused suite portable by constructing temporary archives from captured nonsecret metadata and representative layer shapes inside the existing test file; require no fixed private path, registry access, or prebuilt image.
  Separately record the implementer's bounded read-only comparison with the retained actual archive metadata before freezing the export.
- [ ] Exercise `prepare_bundle`, `bundle_provenance`, artifact-map generation, and final `_check` with external operations entirely injected.
  Use the actual two-layer node shape, full YAML bytes, four-descriptor storage indices, selected children/configs, shared inner layers, and the 28-blob required union amid 144 embedded blobs.
  Return captured/embedded index bytes from the fake registry transport for the exact two storage references; no network or VM command is needed by these tests.
  Cover missing/duplicate refs or children, changed registry/index/config/layer bytes or sizes, wrong architecture, whiteout/link/duplicate member, split metadata layers, malformed/oversized YAML, and registry failure.
  Exercise all seven role map entries, shared runner/seed bytes, local-manager identity without upstream digest, and the real OCI/Docker media-type distinction.
  Assert map bytes match accepted artifact receipts and source/manager proof remains tied to the new test source, never the failed run's source receipt.
- [ ] Inject failure in each remaining stage's output hashing, private-mode check, validation, ledger callback, and final completion write.
  Require pending stage plus typed failure, no completed-stage append or new baseline/bundle authority, and `complete=false` on every failure.
  Require fresh fake preparation followed by standalone fake `check` to pass with all declared files present, no transient residuals, and zero kind/load/Kubernetes/Helm/migration effects.
- [ ] Run `python3 hack/test_e2e_kind.py` and `python3 hack/e2e-kind.py self-check` from the assigned implementation checkout, with all external operations injected for the focused tests.
  Freeze exact wrapper/tests predecessor/result bytes and modes with the test evidence; retain closing independent review and broad Go/lint/test/docs gates at milestone close.
  Before authorizing another full preparation, the leader must separately assign and accept one bounded real producer-format probe using the unchanged cached runner pin and a new private scratch directory.
  Prove the base64 option/self-test, complete version stdout/status framing, and transient-CID removal with a canonical receipt retained across exact-owned cleanup, using the existing network-none/read-only/nonroot runner restrictions and finite container caps.
  That separate probe makes no manager build, archive acceptance, old-run modification, kind, or Kubernetes request; this planning assignment does not authorize it.
- [ ] Have the distinct integration worker join only the reviewed two-file export, verify every unchanged identity, and freeze all operator source bytes/modes, including tests and docs, before any new manager build.
  The leader must explicitly authorize the next actual attempt with a fresh empty mode-0700 RUN_DIR under a verified shared parent outside Git.
  Immediately remeasure usable capacity, MemAvailable, at least 10Gi VM disk free plus projected work, and host Cargo headroom.
  Budget image preparation 3 CPUs/5Gi plus active PostgreSQL 6 CPUs/6Gi plus reserve 2 CPUs/6Gi, totaling 11 CPUs/17Gi; no kind overlap.
  Reverify the complete source before effects, after manager build, and after finish.
  Run the unchanged tracked prepare command once, then the unchanged tracked check command once only after prepare succeeds.
  Stop and preserve evidence on the first unexpected failure; no manual artifacts, chmod repair, partial adoption, immediate retry, or old-run cleanup.
  Require complete baseline, storage, map, final-check, and standalone-check receipts before considering a separately authorized controlled first kind UP.

Task 13 acceptance covers reproducible preparation and its pure producer contracts only.
It does not qualify actual kind topology, effective limits on real kind nodes, legacy migration, native migration, or the M7 milestone.
