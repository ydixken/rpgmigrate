# M6 Native Worker Construction and Dispatch Implementation Plan

Root approved this implementation plan on 2026-11-09 within the standing execution scope through M10.
The approved private source is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m6-worker-plan-8wsye8fy/2026-11-09-m6-native-worker-construction-dispatch.md`, SHA-256 `7001fc8a3cad2ec27fc24a8ca406b20c67f9755f9e609d5e04b79bab46a6123e`.
This repository copy preserves the approved protocol/design and historical planning assertions while applying documentation style.
**Execution note:** Existing approved milestone scope authorizes owned M7 UP/load/legacy/native tests and ordinary fresh owned fault fixtures; source freeze, guard checks, exact ownership, resource admission, and required evidence remain prerequisites rather than a new blanket user-permission gate.
Benchmark execution stays on HOLD for the final actual artifact/argv packet review, and shared-VM poweroff, original retained resources, main pushes/merges, tags, releases, and GHCR publication remain outside this authorization.
This note supersedes historical planning-only or separate-approval wording below where it would request approval again for already approved owned work; it claims no execution result or milestone closure.

> [!IMPORTANT]
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development under the leader's disjoint file leases after root reviews this plan.
> Steps use checkbox syntax for tracking.

**Goal:** Produce real native clone/follow worker Jobs and route their construction through a native-only attempt path while retaining the production `UnsupportedEngine` guard.

**Architecture:** Reuse the existing command helpers, connection materialization, worker envelope, resources and controller status machinery.
Keep `buildJob` and the legacy attempt body unchanged in behavior.
Native construction consumes the actual owned PVC UID, and native creation/recovery admits the current owned Job rather than accepting a name alone.

**Tech Stack:** Existing Go/controller-runtime/Kubernetes APIs and the existing Rust executable for a host-only CLI parity test.
No new production dependency, schema, public CLI, controller framework or authority hint is introduced.

**Spec:** Engine `docs/roadmap.md`, `docs/operator-compatibility.md`, `docs/native-protocol.md`, approved `docs/superpowers/plans/2026-11-08-m1-native-contract.md`, `2026-11-08-m6-native-adapter.md`, `2026-11-08-m7-kind-bootstrap.md`, and the approved direct-image helper plan `2026-11-09-m6-native-helper-jobs-lifecycle-direct-image.md`.
The dated amendments and root decisions below override historical implementation checkout locations and incomplete assumptions.

## Global constraints

- Plan against operator `/Users/ydixken/development/pgcopydb-operator-m4-closing-m6-composed-20261109`, branch `test/m4-closing-m6-composed-20261109`, frozen HEAD `4ecbd69cc9e56e4710096b6f0475fc7d2a62fe8b`.
- Engine inspection used `/Users/ydixken/development/rpgmigrate`, HEAD `60ba21953a1e4af7899ca7ea3a7919fcc8cec756`.
  Reverify both source identities at implementation assignment and retain unrelated changes.
- Preserve the original operator checkout's `tasks/todo.md` and `.agents/` unread and unchanged.
- Keep omission and explicit `pgcopydb` on the legacy backend, with existing golden Job bytes, version rules, cleanup scripts, resource defaults and retry behavior preserved.
- Keep the top-level native `UnsupportedEngine` dispatch guard unchanged until every separately required qualification and consumer gate passes.
- Never adopt, convert, restart, resume, mutate or clean pgcopydb state/resources as native state.
- Keep executable `/usr/local/bin/rpgmigrate`, container `pgcopydb`, PVC mount `/work`, workdir `/work/pgcopydb`, and exact current Migration-owned PVC UID binding.
- Preserve canonical Migration UUID, normalized 32-digit run ID, owned PVC UID workdir ID and checkpoint generation `1` across all attempts.
  COPY generation changes belong to the engine, not an incremented operator checkpoint generation.
- Keep raw URI/DSN credentials in the existing environment/projection/prelude path; never put them in argv, labels, annotations, status, events or logs.
- Keep native image selection `runnerImageFor(m, r.RPGMigrateRunnerImage)` with no fallback to `r.RunnerImage`.
  Preserve `spec.runner.image` precedence and exact raw image values.
- Keep immutable direct-image syntax and actual single-platform/archive/import/runtime qualification separate.
  Syntax and a version string alone qualify nothing.
- Keep same-E persistence/acknowledgment, source-only lag, strict completion, cleanup, native retry/recovery qualification and runtime/image/PVC qualification outside this build slice.
- Do not add fake production capabilities, a successful stub admission factory, an activation switch, public fields, resource-version hints, Secret reads or RBAC.
- Workers run assigned focused tests only; independent review and complete Go/PG16/PG18 suites run once at M6 close.
- Commit only after required checks pass and the leader requests it.

## Review focus

- An explicit zero split threshold must produce an argv the actual engine accepts, without enabling positive splitting or changing legacy argv.
- A zero UUID, recreated Migration, replacement PVC or foreign work tuple must refuse before native state or a worker is created.
- A same-name Job collision or unknown Create outcome must not record an empty/local UID, assume ownership from a name or authorize blind replay.
- Native retry routing must stop before legacy retry-gate SQL, publication cleanup, origin handling, zombie reaping or completion inference.
- A native image omission, tag/alias substitution or executable/environment drift must not silently run the legacy image/program.

## Verified source facts

Operator `resources.go:130` contains the legacy-only `buildJob`.
It uses `pgcopydb.CloneArgs`, legacy `FollowArgs`, publication guards and conditional legacy cleanup prefixes.
Its two production callers are deterministic initial validation in `Reconcile` and worker creation in `startAttempt`.
Tests also call it directly in resources, helpers, publication-retry, retry-gate and retry-gate-Job coverage; preserve those calls as legacy regression coverage.

`startAttempt` currently recovers a next-attempt Job by name before validating native ownership and calls legacy retry SQL on retries.
On AlreadyExists it records the locally built object instead of fetching the server's real UID.
These behaviors remain unchanged for legacy and must not be inherited by the native path.
`nextAttempt`, ordinary observation and finish paths are still legacy consumers; this slice does not make them native-safe or activate them.

`nativeWorkIdentity` already checks canonical UUID spelling, owned PVC name/namespace/controller and UID shape.
Rust `src/protocol.rs:24` maps all four environment values into `RunId`/`OperatorIdentity`.
`src/state/clone_state.rs:65` additionally rejects all-zero run bytes.
`open_clone` writes the complete operator tuple into a fresh native manifest and compares it exactly on reuse.
`src/state/follow_state.rs:209` does the same for follow, with separate provenance and ownership checks.
The existing Go helper accepts an all-zero UUID; Task 1 fixes that helper rather than adding a competing validator in the builder.

Rust `Version::current` still advertises `capabilities: []`.
`ParseVersionLine` demands exactly one newline-terminated discovery document, and `validateNativeConfig` demands actual capabilities for every admitted feature.
A real discovery result therefore currently refuses every native worker profile.
Operational reports can carry their command capability without turning empty discovery into profile clearance.

The native image flag/environment/chart wiring exists at this baseline: `--rpgmigrate-runner-image`, `RPGMIGRATE_RUNNER_IMAGE`, and actual chart value `runner.rpgmigrateImage`.
Do not add the older planned flat chart spelling as another setting.
The evidence route already has `nativeEvidenceDirectImage`; reuse its exact validation policy without a second parser or normalization.

## Frozen root decisions

1. Build a real pure native worker producer and native dispatcher/startAttempt branch, dormant behind the unchanged top-level `UnsupportedEngine` guard.
2. Keep native discovery/preflight construction, parsing and producer capability qualification separately owned immediate activation prerequisites.
3. Refuse native retry attempts before legacy retry-gate SQL in this slice.
   Preserve approved retry argv in the pure builder and explicitly mark retry/recovery qualification incomplete.
4. Native collision/reconciliation requires fresh owned-object reads, exact current Migration owner/work/PVC/image/command admission, and a nonempty actual returned Job UID before attempt recording.
   Unknown Create supplies no adoption permission or same-operation retry.
5. Omit both split selectors from native argv only when `splitTablesLargerThan` is explicitly present and effectively zero.
   Do not alter the legacy renderer, loosen Rust admission, omit nonzero selectors or invent a capability.
6. Fix the all-zero identity discrepancy in the existing helper under its current Commands owner, with a focused negative test and coordinated export.

## Actual worker CLI profiles and the split decision

The operator's admitted command helper is narrower than the Rust parser's implemented option set.
Do not widen `nativeConfigCapabilities` merely because Rust accepts an option.

| Profile | Exact effect |
| --- | --- |
| First attempt | `clone --dir /work/pgcopydb ... --restart` |
| Retry construction | Same immutable work tuple; `--resume --not-consistent` replaces `--restart` |
| Follow construction | Append exactly `--follow --plugin pgoutput`; no legacy slot/publication/origin names |
| Default clone | Table jobs at least 4, split threshold 536870912, max parts 8, binary on, large objects enabled |
| Narrow M7 clone | Explicit binary false, explicit split threshold zero, `skip: [largeObjects]`, no DropIfExists or filters |
| Narrow M7 follow | Narrow clone plus follow/pgoutput; existing lag/cutover policy is not another worker argv |

The default profile requires `clone.single_db`, `progress.v1`, `copy.binary_fallback`, `copy.split` and `copy.large_objects` from actual qualified discovery.
Follow additionally requires `cdc.pgoutput`, `cutover.v1` and `cleanup.v1`; selected verification additionally requires `verify.v1`.
The narrowed profile removes only binary/split/large-object capability requirements; it does not remove clone/progress or follow/cleanup/cutover requirements.
Default binary/split/large-object execution remains staged in Rust and cannot be claimed from construction tests.

Preserve current positive table-job and resource-derived job behavior.
Existing operator validation rejects nonzero index/restore/large-object job overrides, restore-policy omissions, filtering, dry-run, custom follow slot/publication/origin, apply-group overrides, unsupported plugins, superuser references and synchronous-commit overrides.
Some are implemented by the engine, but that does not admit them through this operator profile.
Do not append `PGCOPYDB_APPLY_GROUP_TXNS` in the new worker path when the admitted operator profile rejects that setting.

### Why explicit zero needs a coordinated renderer correction

`pgcopydb.CloneArgs` always emits both `--split-tables-larger-than` and `--split-max-parts`, including `0` and `8` after the M7 narrowing.
Rust `src/config.rs::clone_options` refuses both text options, because `Setting.active` treats every text setting as active, including `0`.
Thus the previously rendered narrow vector cannot enter the actual engine.

M1 `tests/fixtures/native/v1/contract.json` preserves the spellings and marks threshold `512MB` and standalone max-parts `8` unsupported with `BadConfig`.
It contains no explicit-zero acceptance contract.
M1's plan preserves syntax and staged refusal; M7's plan requires splitting disabled but did not choose an encoding.
The accepted root correction resolves that gap in native command construction: omit both selectors only for an explicit effective zero threshold.
An omitted threshold still means the operator's 512 MiB default and retains both flags and its capability requirement.
A positive max-parts value has no execution effect when splitting is explicitly disabled; do not convert it into a promise of split support.
Negative threshold/parts validation still precedes rendering.

The considered alternative was engine acceptance of paired inert zero/default-max selectors while retaining standalone/nonzero refusal.
It would alter the engine's staged profile and require coordinated Rust/parser fixture changes.
Root selected native-only omission, so this plan includes no Rust admission loosening and preserves the M1 standalone max-parts refusal.
M8's planning owner was informed; positive split support and default-profile widening remain M8 responsibilities.

## File ownership and interfaces

The leader assigns one owner per listed file before work starts.
Other active leases must finish or export before a successor edits the same file.

- **Commands owner:** `internal/controller/native_commands.go`, `native_commands_test.go`.
  Own the zero-UUID check and explicit-zero split omission together.
- **Jobs owner:** `internal/controller/resources.go`, `native_resources_test.go`, only the relevant new native worker Job goldens under `internal/controller/testdata/`.
  Existing evidence builder/tests and legacy golden files remain preserved.
- **Lifecycle owner:** `internal/controller/migration_controller.go`, new `native_worker.go`, new `native_worker_test.go`, and `migration_engine_dispatch_test.go` only if an added regression requires it.
  Do not edit `follow.go`, cleanup, sampler or the frozen evidence lifecycle for this slice.
- **Parity test owner:** new `internal/controller/native_worker_executable_test.go`, test-only build tag `native_worker_parity`.
  This owner consumes immutable Commands/Jobs exports, changes no production file, and is separate from the engine's M8 source owner.
- **Docs owner:** engine `docs/native-protocol.md` and `docs/operator-compatibility.md`; operator `docs/configuration.md`.
  Record the disabled-split decision and dormant worker support without claiming activation or runtime clearance.
- **Closing verifier:** owns no implementation files; checks joined source and milestone-close evidence.

Preserve these existing interfaces:

```go
func backendFor(m *v1beta1.Migration) (v1beta1.MigrationEngine, error)
func validateNativeConfig(m *v1beta1.Migration, capabilities []native.Capability) error
func nativeWorkIdentity(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) (native.WorkIdentity, error)
func nativeIdentityEnv(work native.WorkIdentity) []corev1.EnvVar
func nativeCloneArgs(m *v1beta1.Migration, attempt int32) ([]string, error)
func buildJob(m *v1beta1.Migration, runnerImage string, attempt int32) (*batchv1.Job, error)
```

Freeze these new private controller interfaces with root before consumers build:

```go
func buildNativeWorkerJob(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim,
    runnerImage string, attempt int32) (*batchv1.Job, error)
func (r *MigrationReconciler) startNativeAttempt(ctx context.Context,
    m, base *v1beta1.Migration) (ctrl.Result, error)
func nativeWorkerJobMatches(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim,
    expected, actual *batchv1.Job) bool
```

`runnerImage` is the native manager default, never the legacy default.
The pure producer returns an expectation with no Job UID, controller reference or completion authority.
The lifecycle caller adds the exact Migration controller reference and records only the admitted server-returned Job.
`nativeWorkerJobMatches` is a private canonical comparison, not completion, report, capability or runtime qualification.
Keep it in `native_worker.go`; add no general admission framework or exported authority type.

## Task 1: Align existing identity and native argv with the narrowed contract

**Files:** Commands owner's files and its coordinated documentation export.
**Consumes:** Existing M1 helpers and root's explicit-zero decision.
**Produces:** Unchanged helper signatures with corrected validation/rendering.

- [ ] Add `TestNativeWorkIdentityRejectsZeroRun` with canonical all-zero UUID and owned PVC.
  Require a fixed sanitized error and no identity; retain existing canonical/foreign/replacement/deletion cases.
- [ ] Extend `TestNativeCloneArgs` with narrow first/retry plain/follow vectors and explicit-zero threshold plus explicit positive max-parts.
  Assert both selectors are absent only for explicit effective zero; retain exact default/nonzero vectors, resource-derived table jobs and credential-free argv.
- [ ] Extend unsupported-profile cases for negative threshold/parts and retain no DropIfExists/filter/slot/publication remapping.
- [ ] Run the focused Commands command below and retain intended RED from the zero-UUID acceptance and zero-selector output.
- [ ] Add the minimum zero-run check to `nativeWorkIdentity`.
  Update only `nativeCloneArgs` to omit the two known selector/value pairs for explicit zero after existing option validation.
  Do not add a generic argv filter, new renderer API or change `pgcopydb.CloneArgs`.
  A missing/malformed expected pair must refuse rather than remove an unrelated operand or silently pass.
- [ ] Run the same focused cases and export exact source/hash plus nonempty results.

## Task 2: Produce the real native worker Job

**Files:** Jobs owner's files and native worker goldens.
**Consumes:** Task 1 export, `jobSkeleton`, `runnerImageFor`, existing direct-image syntax policy and `pgcopydb.EffectiveRunnerResources`.
**Produces:** `buildNativeWorkerJob`.

- [ ] Add `TestNativeWorkerJobProgram`, `TestNativeWorkerJobConnections`, `TestNativeWorkerJobProfiles` and `TestNativeWorkerJobRefusals` before implementing the builder.
  Missing function compilation may establish API RED but is not behavior qualification.
- [ ] Exercise all nine source/target combinations of inline, component Secret and opaque URI Secret references; add source/target TLS projections and credential canaries.
  Assert canonical connection environment, passfile prelude, exact owned PVC claim, writable `/tmp`, no secret API read and no connection bytes in argv/metadata.
- [ ] For initial/narrow/retry/follow/resource-default profiles, assert exact `nativeCloneArgs` and `/usr/local/bin/rpgmigrate` as the fourth prelude command element.
  Require `conn.PreludeScript` with no legacy publication guard or cleanup setup.
  Assert no `pgcopydb stream cleanup`, legacy `FollowArgs`, slot-name/origin/publication selector, heartbeat/nudge or endpos command is added.
- [ ] Reuse `jobSkeleton` with backoff zero, no setup script, existing connection materialization and sync-commit setting path.
  First invoke the existing native option/identity helpers so refused configurations cannot acquire clone filter mounts or superuser credentials.
  Replace argv0, append exactly one copy of the four identity variables and apply existing effective worker resource defaults.
- [ ] Preserve UID/GID/fsGroup `65532`, nonroot, RuntimeDefault seccomp, read-only rootfs, denied privilege escalation, ALL capability drop, one `pgcopydb` container and RestartNever.
  Set token automount false and explicit DNSClusterFirst; add no init/sidecar/probe/EnvFrom/custom DNS/host privilege.
  Preserve resource requests/limits, affinity/work-node preference, tolerations, node selector and TTL.
  Do not copy the evidence helper's 90-second deadline or 1 MiB evidence-only create policy onto a long-running worker.
- [ ] Require a nonempty exact selected canonical immutable native image via the existing direct-image policy.
  Test spec override precedence, empty native default, available legacy-only default, mutable tag, alias/malformed pin and accidental executable/environment mismatch.
  Syntax acceptance is not runtime image clearance.
- [ ] Use existing `jobName(m, attempt)`; reject names/labels that cannot form a valid native worker Job rather than truncating names, changing Reader's name contract or repairing legacy naming.
  A Job name must be a DNS subdomain at most 63 characters under the Kubernetes Jobs contract.
  Preserve all existing valid worker labels; do not invent a role/authority label.
- [ ] Require nondeleting exact owned PVC identity and nonempty UID before construction.
  Do not require an already Bound claim merely to construct its first consumer, because delayed-binding storage may need that Pod to schedule.
  Bound/remount/exclusive execution qualification remains a runtime prerequisite; no chmod/init repair is introduced.
- [ ] Add three native connection golden Jobs using synthetic selectors, retain existing legacy goldens byte-for-byte, and test m/PVC inputs are unchanged and returned UID/owner reference remain unset.
- [ ] Run focused Jobs tests and existing `TestBuildJobGolden`; export only owned files after a nonzero inventory passes.

## Task 3: Route native attempt creation without legacy effects

**Files:** Lifecycle owner's files.
**Consumes:** Tasks 1/2 exports and the existing uncached APIReader/image configuration.
**Produces:** `startNativeAttempt`, the private canonical comparison and one engine dispatch branch at the start of `startAttempt`.

- [ ] Add `TestNativeWorkerDispatch`, `TestNativeWorkerExistingJobBinding`, `TestNativeWorkerCreateUncertainty` and `TestNativeWorkerRetryRefusal` with a recording client.
  Fail tests on Secret access, legacy retry SQL, sentinel calls, sampler/progress/log inference, cleanup or unexpected deletes.
- [ ] Keep `Reconcile`'s `UnsupportedEngine` block byte-for-byte except any unavoidable adjacent formatting.
  Direct component tests may exercise the new internal attempt method; no test-only production admission bypass or fake capability list may open Reconcile.
- [ ] Dispatch inside `startAttempt` before its existing next-attempt GET, budget/retry gate or builder call.
  Native delegates to `startNativeAttempt`; legacy continues its existing body; unknown engines return a fixed refusal without lifecycle effects.
  Keep initial `Reconcile` validation's legacy `buildJob` call on the legacy-only route.
  The later activation owner must add native deterministic validation before PVC creation rather than calling the legacy builder for native.
- [ ] At native entry, reject absent APIReader/client/scheme, canceled context, deleting/suspended/terminal Migration, nonpositive/overflowed next attempt, missing UID/generation or a changed live Migration/spec/attempt binding.
  Require uncached reads; never use the fallback `uncached()` helper when it would use a cached client.
- [ ] Refuse every attempt greater than one before legacy retry-gate SQL, Job creation, gate deletion or legacy resource repair.
  The pure builder still models retry argv; this refusal is an explicit pending gate, not the final supported native behavior.
- [ ] Fresh-read the exact Migration and work PVC; check namespace/name/UID/controller/deletion and compare the current tuple through `nativeWorkIdentity`.
  Build the expected Job from these objects and the native image default, then install the exact Migration controller reference.
  Preserve a Pending first-consumer PVC without granting runtime/PVC qualification.
- [ ] Initial exact-name GET success may record only an independently admitted existing Job with a real UID, exact current Migration owner, native work tuple, selected image, command/args, environment/credential references, volumes/mounts, isolation and current attempt/name.
  Compare against canonical construction, never against fields copied from that existing Job into its own expectation.
  Refuse legacy Jobs, deleting Jobs, replacement PVC tuples, extra identity variables, image or script changes, wrong owner generation/context and conflicting labels/spec.
  Admit only documented server defaults/controller-managed identity fields; unknown execution mutations refuse.
  Keep the comparator bounded to this worker shape, with defaulting fixtures and exact executable/environment comparisons; add no permissive digest suffix, environment subset or name-only ownership check.
- [ ] On NotFound, issue one Create with the decorated expected object.
  A successful Create must return a nonempty UID and exact current owned binding before `recordAttempt` receives that actual object.
  An explicit AlreadyExists triggers a fresh exact-name GET and the same independent admission; it never records the locally built empty-UID Job.
- [ ] A non-AlreadyExists Create error or empty UID is uncertain/refused: return a fixed sanitized error, record no attempt/success/observation, and perform no delete, second Create or immediate adoption.
  A later reconciliation gets no permission from that error; it starts with fresh current reads and the independent existing-Job contract.
  If an original UID/identity is independently known and differs, refuse the replacement.
  Where continuity cannot be independently established, refuse rather than invent a persisted hint or recover by name.
- [ ] Recheck current Migration/PVC/work/attempt before recording after reads/Create.
  Pass only the real returned/admitted Job to existing attempt status machinery; status-write failure is not success and does not authorize another worker in that operation.
  Record at most one AttemptStarted event for a successful new Create, none for admitted overlap recovery.
- [ ] Test successful creation, owned preexisting initial Job, create collision, status conflict, lost Create acknowledgment, empty returned UID, cancellation and object changes between each read and status recording.
  Assert all refused cases leave E and completion/cleanup conditions unchanged and never create a retry or consume helper evidence.
- [ ] Retain `TestReconcileEngineDispatchRejectsBeforeEffects` and `TestReconcileEngineDispatchPreservesLegacy` unchanged in meaning.
  They must prove native rejection still precedes resources/SQL and both legacy selections still start the legacy worker.
- [ ] Run the focused Lifecycle command and return exact admitted versus refused case inventory.

## Task 4: Check complete narrowed argv with the actual executable

**Files:** Parity owner's test-only file, guarded by `//go:build native_worker_parity`.
**Consumes:** Frozen Commands/Jobs source and a leader-supplied actual host `rpgmigrate` executable built from the approved engine candidate.
**Produces:** `TestNativeWorkerExecutableParity`; no engine parser or capability change.

- [ ] Obtain the actual executable through the existing Cargo/artifact route and bind its SHA/source/toolchain in the receipt.
  A missing executable fails this required gate; it is not replaced with a script, help output, mock or parser-only fixture.
  The test-only build tag keeps ordinary operator suites independent of the sibling engine repository.
- [ ] Read the binary path from `RPGMIGRATE_TEST_BINARY` only in this tagged test.
  This is a test input, not a runtime configuration field or activation hint.
- [ ] Build real native Jobs for narrowed initial/retry plain/follow profiles, with explicit binary false, zero threshold and skip-large-objects.
  Assert production argv has exactly `--dir /work/pgcopydb` and both split selectors absent.
  Execute the actual binary with the entire resulting argv and the exact four-field synthetic identity/connection environment.
  Rebind only the independently checked `--dir` operand to an owned temporary test directory; preserve every other option and ordering.
  Never touch real `/work` and do not claim PVC/runtime qualification from this fixture path.
- [ ] Use a bounded missing Unix-socket fixture and the real executable's existing sanitized PostgreSQL connection-failure path to prove valid vectors reach execution instead of BadArgs/BadConfig refusal.
  Require the exact expected `Pgsql` exit 4 and sanitized connection failure, no completed report and no successful migration claim.
  Prepare a matching native INIT fixture through the real initial process where needed for resume; do not forge a manifest or assume any BadState means acceptance.
- [ ] Require the pre-correction complete narrow vector with both split options to fail with the expected unsupported-option category, proving the regression control.
  Retain actual-binary negative controls for nonzero split, binary-on, default profile and standalone `--split-max-parts 8`, without changing M1 refusal fixtures.
  Test partial/all-zero identity separately and preserve their earlier refusal boundary.
- [ ] Assert no unexpected filesystem path, DB mutation, credential output or capability advertisement is produced.
  Report no skipped required cases and no unsupported option silently ignored.
- [ ] Run the tagged focused command below only after the real executable and frozen builder inputs exist.
  Actual server/client-major and complete migration qualification remain separate gates.

## Task 5: Document the dormant producer and hand off activation dependencies

**Files:** Docs owner's three listed docs only.
**Consumes:** Frozen root split decision and actual implemented component results.
**Produces:** Current contract/configuration statements with preserved activation refusal.

- [ ] Document explicit-zero native omission, default/nonzero unchanged capability requirements, current engine staged refusals and the narrow M7 profile.
  Preserve the M1 reserved spellings and historical unsupported max-parts fixture instead of rewriting history as a runtime pass.
- [ ] State that real worker construction exists as a component while production Reconcile still rejects native before resource/SQL effects.
  Keep current chart value and image precedence; do not promise default native image or qualified runtime support.
- [ ] Keep worker retry refusal explicit as a pending qualification boundary, not supported recovery semantics.
  Preserve separate same-E, lag, cleanup, helper and final-completion responsibilities.
- [ ] Apply semantic sentence breaks, humanizer and operator STE100 to affected published docs; retain README as an index.
- [ ] Return documentation hashes for the leader's joined review; do not change the engine/operator task ledgers from a worker lease.

## Immediate separate discovery/preflight handoff

This is a separately assigned next slice, not permission to remove the guard in Task 3.
Freeze its exact result path with the worker/lifecycle owners before activation.

Proposed private result interface for root review:

```go
func (r *MigrationReconciler) ensureNativePreflight(ctx context.Context,
    m *v1beta1.Migration) (version *native.Version, passed bool, failure string, err error)
```

Only `passed=true`, nonnil strictly parsed `version` and no failure/error may pass capabilities to `validateNativeConfig(m, version.Capabilities)`.
Waiting returns `passed=false`, nil version and no failure; deterministic refusal returns no version and a fixed sanitized failure; unavailable/uncertain transport returns no version and a sanitized error.
A Version is capability data, not image/work/state ownership authority.

- Run exactly `/usr/local/bin/rpgmigrate version --json` from the independently selected native image, without operator identity or workdir initialization.
- Capture a single complete stdout document, require successful original Job/container execution and current image/ownership binding, and call existing `native.ParseVersionLine`.
- Reject empty/missing/duplicate/unknown capabilities, malformed/truncated/multiple documents, wrong protocol, changed execution identity and unknown result; zero advertised capabilities currently blocks worker creation.
- Require actual source/target majors exactly 16:16, 18:18 or 16:18 before engine mutation.
- Select `/usr/lib/postgresql/<target-major>/bin/pg_dump` and `pg_restore`, require real named tool versions matching target major, and preserve the engine's own utility selection rules.
- Keep default psql18, existing applicable connectivity, extension/ownership, RLS, publication/WAL and replica-role checks.
  Native must skip legacy origin-grant/repair and legacy PG17 restore logic; never mutate the shared legacy script to make PG16 pass.
- Do not reuse `ensurePreflight` success-by-exit or first-attempt-only preflight status as actual version/capability/image admission.
  A current image/Job/PVC/attempt change requires current readmission rather than retaining an unbound capability cache.
- Producer capability advertising changes belong to the engine qualification owner after actual runtime evidence, not this operator worker owner.

## Activation dependencies and non-goals

No component task, actual-CLI parse acceptance or synthetic pin clears this list:

1. Real discovery/preflight plus truthful qualified capabilities and exact server/client-tool pairs.
2. Accepted direct single-platform image/archive/import/raw imageID profile, original container execution binding, packaged dependencies, resolver/opaque-file/TLS and owned PVC permissions/remount qualification.
3. Native deterministic validation, running-Job identity checks, strict progress/phase reports, no legacy stage inference, native diagnostics/session matching and no legacy supervisor-death reaping.
4. Qualified native retry/recovery gates; this slice's retry refusal cannot remain as the final claimed supported lifecycle.
5. Source-only active lag/catch-up, persisted identical E ordering, separate same-E acknowledgment and Reader-owned completed evidence.
6. Strict successful worker report, final sequence/large-object obligations, verification and exact owned cleanup before Complete.
7. Native deletion/suspend/terminal handling with preserved ownership and no legacy fallback.
8. Whole-M6 source freeze, lint/tests/PG16/18/docs, independent safety review and M7's three-pair operator matrix.

Do not enable native by deleting the top guard after only this slice or preflight passes.
Do not invoke legacy verification, cleanup, sentinel-current/nudge, stage inference or retry SQL for a native worker while these dependencies are unresolved.

## Focused verification commands

These commands are planned gates, not results from this planning assignment.
Run them only after their named tests and exact inputs exist, with Go1.27.3/GOMAXPROCS2 and the leader's resource budget.

1. Verify Commands.

   ```sh
   go test ./internal/controller -run '^TestNative(WorkIdentity|WorkIdentityRejectsZeroRun|CloneArgs|CloneArgsRejectsUnsupportedProfile|CloneArgsKeepsConnectionsOutOfArgv)$' -count=1 -v
   ```

2. Verify builder and unchanged legacy goldens.

   ```sh
   go test ./internal/controller -run '^(TestNativeWorkerJob(Program|Connections|Profiles|Refusals)|TestBuildJobGolden)$' -count=1 -v
   ```

3. Verify native attempt routing and the closed production guard.

   ```sh
   go test ./internal/controller -run '^(TestNativeWorker(Dispatch|ExistingJobBinding|CreateUncertainty|RetryRefusal)|TestReconcileEngineDispatchRejectsBeforeEffects|TestReconcileEngineDispatchPreservesLegacy)$' -count=1 -v
   ```

4. Verify actual executable parity with the preverified test binary path supplied by the leader.

   ```sh
   go test -tags native_worker_parity ./internal/controller -run '^TestNativeWorkerExecutableParity$' -count=1 -v
   ```

5. Run the already qualified linter directly for the owned Go package during the build phase.

   ```sh
   /tmp/rpgmigrate-operator-lint-qualification-20261109/bin/golangci-lint run ./internal/controller/...
   ```

The qualified linter SHA-256 is `285f51df965bb73c0ad69e4fbdf17d167f404d25ccb805af44654660a5a5f59e`.
Do not rebuild it, run Make or broaden full Go/database/runtime work from an individual file lease.
Require nonempty named inventories, exact source hashes, zero required skips and explicit failure categories.

## Milestone-close gate

- [ ] Freeze joined source, verify disjoint exports and review complete legacy/native diff.
- [ ] Run the operator's `task lint`, `task test` and `task docs` once at M6 close, under the leader's assigned verification owner and budgets.
- [ ] Include required PostgreSQL16/18 ownership/preflight/SQL/controller/cluster-free E2E coverage checks with actual measured majors, not skips or parser fixtures.
- [ ] Run one independent xhigh M6 review across identity, native execution/image admission, capability truthfulness, preflight, recovery, same-E ordering, evidence, cleanup and unchanged legacy behavior.
- [ ] Keep production native refusal and M6/M7 open for every unresolved activation prerequisite.
- [ ] Commit only on the leader's request after all required checks are green; no push/deploy/publication follows from this plan.

## Planning review and bounded followups

Read current global/project/operator instructions, operator CONTRIBUTING and both lessons sources, the approved M1/M6/M7 plans, roadmap amendments, current CLI fixtures, actual command helpers/callers and Rust protocol/state/config/utility selection.
Applied ponytail, writing-plans and operator humanizer conventions.
Compared the narrow/full worker vectors against source rather than assuming staged CLI registration meant runtime acceptance.
Confirmed the all-zero discrepancy and coordinated the split decision with root and the M8 planning owner.
No source, dependencies, repository task ledgers, VM/container/Kubernetes/DB resources, commit or push changed in this assignment.
Only this private plan was written.

Root review must freeze the three new private worker signatures and assign the file leases before parallel build.
The exact allowed Kubernetes defaulting cases in existing-Job admission require bounded fixtures in Task 3; do not solve this by accepting an environment subset or generic name-based adoption.
If source inspection or those fixtures expose a need for additional persisted identity or a new resource protocol, stop that subtask for root review rather than expanding this slice.

Primary external source used only for native Job-name validation: [Kubernetes Jobs specification](https://kubernetes.io/docs/concepts/workloads/controllers/job/#writing-a-job-spec).
The source confirms the 63-character Job-name ceiling; it supplies no native runtime or ownership clearance.
