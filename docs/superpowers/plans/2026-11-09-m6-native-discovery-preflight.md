# M6 Native Discovery and Preflight Implementation Plan

Root approved this discovery/preflight plan on 2026-11-09 and assigned its implementation owners.
The approved private source is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m6-discovery-plan-gb40_mhu/2026-11-09-m6-native-discovery-preflight.md`, SHA-256 `46705b07b7edc5c2569bc5d474764c4be1f73c0360a0a411f3ba799b2c72d549`.
This repository copy preserves the reviewed source below with documentation style and the following later root contract clarification.
**Contract clarification:** Preflight adds no failure envelope: failure exits 1 with no positive output, while fixed refusal classes remain private; the historical failure wording below must not be interpreted as an additional result protocol.
Keep total/work/cleanup ceilings at 90/60/30 seconds; an internal work timeout after exact cleanup remains paced pending, without permanent success/failure, renewed deadline, or adoption.
**Execution note:** Existing approved milestone scope covers owned M7 UP/load/legacy/native tests and ordinary fresh owned fixtures; source/ownership/resource/evidence guards remain prerequisites rather than a new blanket approval request.
Benchmark execution remains on HOLD until the actual final artifact/argv packet is approved, and shared-VM/original-resource/main/tag/release/GHCR boundaries remain unchanged.

> [!IMPORTANT]
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans task by task.
> This private plan grants no source lease or production activation.

**Goal:** Read actual native capabilities from the selected immutable image and qualify the exact source/target/tool profile before native effects.

**Architecture:** We separate a credential-free discovery Job from a fixed-role native database preflight Job.
Both use the existing original-execution identity checks and strict parsers, with complete bounded output capture where needed.
The database role reuses the existing private Python/ctypes credential boundary in its canonical embedded source and exposes no arbitrary SQL API.

**Tech Stack:** Go, controller-runtime, client-go, the existing native protocol v1 parsers, Python/ctypes/stock libpq and PGDG PostgreSQL 16/18 tools.

**Spec:** Engine approved design, dated roadmap amendments, operator-compatibility.md, native-protocol.md and the approved M1/M6/direct-image-helper plans.
The approved private worker plan is `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m6-worker-plan-8wsye8fy/2026-11-09-m6-native-worker-construction-dispatch.md`, SHA-256 `7001fc8a3cad2ec27fc24a8ca406b20c67f9755f9e609d5e04b79bab46a6123e`.

## Global Constraints

- Engine: `/Users/ydixken/development/rpgmigrate`, `feat/native-engine`, checkpoint `60ba21953a1e4af7899ca7ea3a7919fcc8cec756`.
- Operator: `/Users/ydixken/development/pgcopydb-operator-m4-closing-m6-composed-20261109`, checkpoint `4ecbd69cc9e56e4710096b6f0475fc7d2a62fe8b`.
- Paths and revisions are separate inputs.
- Exclude the original operator checkout and its dirty task ledger and `.agents` from context and edits.
- Preserve the production `UnsupportedEngine` block and both legacy engine selections.
- Admit only actual pairs 16:16, 18:18 and 16:18, with source <= target.
- Use `/usr/local/bin/rpgmigrate`, default psql 18 and actual target-major pg_dump/pg_restore.
- Select `runnerImageFor(m, r.RPGMigrateRunnerImage)` without legacy image fallback.
- The administrator's immutable single-platform pin independently supplies expected spec ImageRef and raw status.ImageID.
- A syntax-valid pin, observed ImageID, manifest-list alias, image label or version string is not runtime qualification.
- Add no dependency, CRD/status schema, public knob, receipt service, Secret read, RBAC or general query/transport framework.
- Version bytes and preflight results supply no work-state, SQL COMMIT, ACK, durability or cutover authority.
- Pass only actual strictly parsed discovery capabilities to `validateNativeConfig`.
- `src/protocol.rs:Version::current()` advertises `[]` at this checkpoint; never fabricate capabilities to activate admission.
- Source-only lag, reports/strict completion, same-E handling, Reader evidence, native retries, owned cleanup and runtime/PVC qualification remain separate immediate activation dependencies.

## Review Focus

- Unknown Create outcomes or empty UIDs never authorize adoption, output consumption, deletion or a second Create in the invocation.
- Replacement Jobs/Pods/containers, restarts, changed image/spec or duplicate Pods never supply admitted version bytes.
- Extra diagnostic bytes, truncation, overflow, missing EOF/Close or malformed/multiple version documents never pass.
- Empty/missing selected capabilities stop before credential projection, DB probes, PVC creation and worker creation.
- Native preflight never inherits raw-DSN argv, raw error output, origin grants or the legacy PG 17 restore gate.

## Source findings and existing seams

`internal/native/transport.go:ParseVersionLine` already requires one terminated document without trailing bytes.
`contract.go:ParseVersion` rejects unknown/missing fields, wrong protocol and duplicate/unknown capabilities.
An explicit empty capability array is valid syntax but fails every worker configuration because clone/progress capabilities are required.
Keep the frozen parser meaning and shared fixture history.

`backend.go:validateNativeConfig(m, capabilities)` is the capability consumer.
`nativeConfigCapabilities` rejects unsupported profiles and remediation/superuser references before effects.
The narrowed plain-clone profile still requires `clone.single_db` and `progress.v1`; follow also requires `cdc.pgoutput`, `cutover.v1`, `cleanup.v1`; requested verification requires `verify.v1`.
Default binary/split/large-object requirements remain unchanged.

`follow.go:ensurePreflight` trusts exit status and generic name-based ensureJob.
`migration_controller.go:preflightGate` skips attempts after zero.
Neither supplies current image/capability admission.

`podexec.go:JobLogsByUID` chooses the newest Pod and uses TailLines/DoRaw.
It cannot prove original Pod identity or explicit complete EOF/Close.
`native_transport.go` already supplies bounded contexts, `nativeJobPod`, `nativeIncarnation.completed`, `same`, `NativeContainerBinding` and `NativeOutcome`.
Keep `JobCompletedNative` empty-log semantics unchanged.
There is no `privateJob` function in the central checkpoint.

`resources.go:preflightHeader` puts raw DSN/URI in psql argv and echoes raw connection stderr.
Native must use a separate safe role, not inherit or mutate that header.
`src/schema.rs:tools_in` uses PG_BINDIR, then PG_CONFIG, then packaged target-major tools, with PATH fallback only when the packaged directory is absent.
It checks both actual named tool versions against target major.

Worker argv audit: `conn.PreludeScript` ends with `exec "$0" "$@"`.
`nativeCloneArgs` supplies no --source/--target, while Rust `config.rs:ENV_OPTIONS` maps `PGCOPYDB_SOURCE_PGURI` and `PGCOPYDB_TARGET_PGURI` to supported connection inputs.
The worker uses supported environment inputs without shell-expanding credentials into executable argv.
Keep that invariant in actual executable parity tests.

## File ownership and frozen interfaces

The leader assigns leases before edits and transfers shared-file ownership explicitly.
The current lifecycle worker owns `migration_controller.go`, `native_worker.go` and `native_worker_test.go` and confirms its canonical worker matcher grants no transport/capability authority.
Jobs retains resources.go; Commands retains native_commands.go/backend.go.

- Jobs: `internal/controller/resources.go`; new `native_preflight_resources_test.go`; new version/preflight goldens beside existing Job goldens.
- Checked transport: new `internal/podexec/native_version.go`, `native_version_test.go`; read existing native_transport.go helpers without weakening evidence completion.
- Preflight lifecycle: new `internal/controller/native_preflight.go`, `native_preflight_test.go`.
- Wiring after lease transfer: `internal/controller/migration_controller.go`, `cmd/main.go`, existing wiring tests.
- Fixed-query producer: existing `internal/native/evidence_program.py`, `evidence_program.go`, new `preflight.go`, `preflight_test.go`, `preflight_program_test.py`; share the canonical Python source with its current evidence owner only after transfer.
- Docs: operator configuration.md, reference/prerequisites.md, operations/lifecycle.md and coordinated engine native-protocol.md/operator-compatibility.md.
- Engine capability qualification: protocol.rs and actual producer/CLI tests; image-smoke owner separately owns tools/image/smoke.py and its tests.

Proposed controller/transport interfaces:

```go
func buildNativeVersionJob(m *v1beta1.Migration, runnerImage, name string) (*batchv1.Job, error)
func buildNativePreflightJob(m *v1beta1.Migration, runnerImage, name, operationID string) (*batchv1.Job, error)
func (r *MigrationReconciler) ensureNativePreflight(ctx context.Context,
    m *v1beta1.Migration) (version *native.Version, passed bool, failure string, err error)
type NativePreflightReader interface {
    JobVersionNative(context.Context, string, types.UID,
        podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
    JobPreflightNative(context.Context, string, types.UID,
        podexec.NativeContainerBinding) (podexec.NativeOutcome, error)
}
func (e *Exec) JobVersionNative(ctx context.Context, namespace string,
    jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error)
```

The manager injects existing podExec into `MigrationReconciler.NativePreflight`.
JobPreflightNative has the same method signature as JobVersionNative and shares its package-private bounded original-terminal-output reader.
Each controller caller independently admits its exact fixed Job role before the capture call; neither method proves SQL completion or evidence authority.
Keep the shared reader private rather than expose a generic command or output API.
Do not widen the evidence NativeTransport interface or route discovery through the evidence Reader.
JobVersionNative returns complete admitted version bytes, not capability authority.

Only `(nonnil,true,"",nil)` may reach `validateNativeConfig(m, version.Capabilities)` and full preflight success.
Waiting returns `(nil,false,"",nil)` and uses existing pacing.
Deterministic refusal returns nil version plus fixed sanitized failure; transport/API uncertainty returns nil version plus sanitized error.
Cancellation returns no success.
No prior condition, preflight Job, image label or cached Version substitutes for current admission.

## Task 1: Credential-free native version Job

**Owner:** Jobs.
**Consumes:** Existing image policy, resources/scheduling and isolation conventions.
**Produces:** buildNativeVersionJob.

- [ ] Write `TestNativeVersionJob`: source/target inline, password Secret, opaque URI Secret, TLS and superuser canaries never appear in env, argv, volumes, mounts or output.
- [ ] Build one pgcopydb-named container with command `[/usr/local/bin/rpgmigrate]`, args `[version,--json]`, no shell, connection/identity env, EnvFrom, PVC, volume/mount, work initialization, sidecar/init/ephemeral container or probe.
  Do not call jobSkeleton/scriptJob/connection materialization and scrub afterward.
- [ ] Require the exact configured native immutable image; refuse missing native image, legacy-only fallback, tag, malformed pin and unknown engine.
- [ ] Preserve UID/GID 65532, nonroot, RuntimeDefault, read-only rootfs, no privilege escalation, ALL capability drop, RestartNever, token automount false, ClusterFirst and existing effective resources/scheduling.
  Set backoff 0, one completion/parallelism and activeDeadlineSeconds 90.
  Preserve existing configured TTL policy without a new retention knob.
- [ ] Use `rpgmigrate-version-` plus a fresh cryptographic 32-character operation ID.
  Return an undecorated object with no UID/owner; install exact Migration controller reference only at create.
- [ ] Add one full synthetic Job golden, assert input immutability and preserve legacy golden bytes.
- [ ] Run the focused builder test in its leader-assigned slot.

## Task 2: Bounded complete output from original terminal-success execution

**Owner:** Checked transport.
**Produces:** JobVersionNative beside unchanged quiet evidence completion.

- [ ] Write `TestNativeVersionCompletedOutput` using existing fake Kubernetes HTTP/stream patterns and independently supplied JobUID, PodUID, containerID/name, zero restart, raw ImageID and template fingerprint.
- [ ] Reuse bounded/nativeJobPod/completed/same checks.
  Require one exact owned Pod, original main exit 0/signal 0 and matching terminated containerID, JobComplete, succeeded 1, active 0, failed 0, no failure conditions, no restart or last termination.
- [ ] Fetch exact original Pod/container logs with no Follow, Previous, timestamps, tail, since, limitBytes or Stream selector.
  Stream/read through LimitReader 65537; reject output over the existing 65536 native payload cap.
  Require read EOF and successful Close, with fixed sanitized errors for overflow/read/close/cancellation failure.
- [ ] Fresh-read original Job/Pod after EOF and require unchanged identity/spec/owners/labels/annotations/full status and repeated terminal-success checks before returning NativeOutcome bytes.
- [ ] Test absent/duplicate/foreign/replaced Pod, replaced Job/container, changed image/spec/status, restart, prior termination, nonzero/signal, pending/failure state, overflow, read-error/truncated stream, close error and cancellation at every boundary.
- [ ] Retain `TestNativeDefaultClusterCompletion` proving nonempty evidence logs still refuse.
  Evidence Reader never calls the version method.

## Task 3: Fresh discovery lifecycle and actual-capability admission

**Owner:** Preflight lifecycle, with later wiring transfer.
**Consumes:** Tasks 1/2, ParseVersionLine, nativeConfigCapabilities and validateNativeConfig.

- [ ] Write `TestNativePreflightDiscovery`, `TestNativePreflightCreateUncertainty`, `TestNativePreflightReadmission` and `TestNativePreflightCleanup` with a recording uncached client.
  Reject unexpected Secret/DB/control access, PVC/worker creation, legacy preflight/retry/origin/sentinel/evidence calls or completion-status changes.
- [ ] Require uncached APIReader, Client/Scheme/version reader, valid live native Migration UID/generation, unchanged spec/attempts/JobName, nondeleting/nonsuspended/nonterminal state, exact selected image and supported pure configuration before Create.
- [ ] Create one fresh decorated version Job per invocation, bounded to 90 seconds overall, with at most 60 seconds for work and at least 30 seconds reserved for exact cleanup, subordinate to the caller deadline.
  Only server-returned nonempty UID with canonical admitted spec grants observation authority.
  AlreadyExists, lost/error reply or empty UID gives no version, no second Create, no adoption, no delete and no output read for an uncertain UID.
  Later reconciliation begins a fresh operation without persisted hints, capability cache or cross-reconcile replay.
- [ ] Independently admit canonical Job/Pod shape and unique original Pod/container before capture, including exact raw image binding.
  The structural template hash omits literal env/argv/mounts and cannot replace exact comparison.
- [ ] Parse the complete JobVersionNative payload with ParseVersionLine.
  For this discovery parser only, combined logs are acceptable because any extra stderr bytes invalidate the single document.
  Never strip ordinary lines, select a JSON-looking tail or accept operational framing.
- [ ] Pass only actual parsed capabilities into validateNativeConfig.
  Empty array refuses before credential projection, DB probes, PVC or worker creation.
  Cover every required selected capability omitted in turn, unknown/duplicate/null/missing caps, wrong protocol, unknown identity, duplicate keys, invalid UTF-8, leading/trailing logs, missing newline, multiline/truncated/two documents.
  Reuse frozen fixtures without altering their meaning.
- [ ] Re-read live Migration and original execution after capture and before success; changed UID/generation/spec/image/attempt/JobName/suspend/deletion/terminal state discards the result.
  Future image/PVC/attempt changes require readmission rather than an unbound successful-preflight cache.
- [ ] Known server UID: delete only the exact created owned Job with UID precondition, verify absence and absence of its exact owned Pod, then retain only sanitized event/condition/local outcome evidence.
  Refuse same-name replacements and incomplete cleanup.
  Unknown UID: no immediate deletion/adoption; retain no-success and rely only on owner GC and existing configured terminal TTL.
- [ ] Freeze one original deadline for the 90-second invocation, at most 60 seconds of work and at least 30 seconds reserved for exact cleanup, subordinate to any earlier caller deadline.
  An earlier caller deadline can prevent successful cleanup and therefore precludes a successful result.
  Use existing error backoff/pollInterval/conditions to prevent tight Job-creation loops and add no public timeout knob.
- [ ] Inject the existing podExec after the wiring lease transfers.
  Keep UnsupportedEngine unchanged; direct component tests must not create a production bypass.

## Task 4: Fixed native-preflight role over existing safe credential boundary

**Owner:** One assigned fixed-query producer owner, coordinated with Jobs/lifecycle.
**Consumes:** Actual discovery clearance and the existing canonical private Python program.
**Produces:** A fixed role and private result only; no caller-supplied SQL or evidence authority.

Minimum ownership/interface decision for root review:

```python
def run_native_preflight(operation_id: str, program_sha256: str) -> int:
    ...
```

Extend evidence_program.py main dispatch with exactly `native_preflight <op> <source_sha>` beside unchanged completed_evidence.
Reuse `load_runtime`, `validate_environment`, `Materialization.prepare/connection/check/close`, `ConnectionInput`, `Session.query/check/close`, `session_role`, `Diagnostics`, StableFile/SealedFile and resolver qualification directly in the same source.
Do not copy a second libpq client or expose a role/query name supplied by the Migration.
A closed fixed preflight role emits one bounded terminated JSON result with exactly `version`, `kind`, `operation_id`, `program_sha256`, `source_major`, `target_major`, `pg_dump_major`, `pg_restore_major`, `checks`, `source_closed`, `target_closed`, `materialization_closed` and `diagnostics_clean`.
Require version integer 1, kind `native_preflight`, exact caller operation/source hash, actual pair 16:16/18:18/16:18, both tool majors equal target major and every closure flag true.
For clone-only, checks is exactly `[connections,identity,source_primary,target_writable,versions,tools,temporary_privilege,extensions,clone_rights,rls]`.
For follow, append exactly `[wal,publication,slots,replication,replica_role,unlogged,replica_identity]` in that order.
The expected list comes from independently admitted Migration configuration, never from observed result fields.
The result contains no endpoint, role/database identity values or row data; session identity facts remain private in memory.
Failure emits only a fixed failure class and optional validated SQLSTATE, never raw message/conninfo/role/table/row data.
This is one private fixed-role result inside the existing mechanism, not a new public protocol or receipt service.

Proposed Go boundary in new internal/native/preflight.go:

```go
type PreflightResult struct { passed bool }
func (p PreflightResult) Passed() bool
func ParsePreflightResult(raw []byte, operationID string,
    programSHA256 [32]byte, follow bool) (*PreflightResult, error)
func PreflightMaterialization(m *v1beta1.Migration, operationID string) (
    command, args []string, env []corev1.EnvVar,
    volumes []corev1.Volume, mounts []corev1.VolumeMount, err error)
```

The parser uses the existing strict JSON rules: exact fields and integer types, unique decoded keys, no extra/unknown/null fields, exact check list, one terminated document and the 65536-byte cap.
Only the parser constructs passed=true; a zero-value or failed result returns Passed()==false.
This fixed result adds no public protocol field and offers no generic query input.
PreflightResult supplies only a passed bounded preflight and actual pair/tool facts, not resource ownership.

- [ ] Reuse conn.Materialize and conn.PreludeScript for both primary connection projections and private passfile/URI files.
  Launcher argv contains only fixed native_preflight, operation ID and canonical embedded-source hash.
  Add exact environment `LC_ALL=C`, `LANG=C`, `PGCONNECT_TIMEOUT=10` and `RPGMIGRATE_NATIVE_PREFLIGHT_FOLLOW=0` or `1` from the independently admitted follow selection.
  Validate the follow selector canonically in run_native_preflight, reject any other value and bind its exact value in canonical Job/Pod admission.
  It selects only the two fixed query lists and cannot carry SQL or arbitrary probe names.
  Mount `/tmp` and actual credential/TLS projections only; omit work PVC, operator work identity and superuser remediation materialization.
  Honor raw URI/DSN bytes, explicit-empty and original permission/absence semantics with ConnectionInput snapshots.
- [ ] Add fixed-role tests proving URI/keyword/password/TLS canaries never appear in argv, logs, result or errors; all three connection forms remain supported without URI rewriting or fake certificate material.
- [ ] Open exactly one source and one target Session from prepared inputs.
  Pin backend PID, actual database OID, CURRENT_USER/SESSION_USER OIDs, server major, primary/read-only facts and libpq connection facts in memory; compare them again after fixed checks and before closure.
  Compare SQL backend PID with libpq backend PID for the applicable pooler check.
  Keep system_identifier optional where the admitted role cannot expose it, and reverify any obtained value rather than requiring new superuser privileges.
  It is diagnostic only and never substitutes for a required engine authority proof.
  Positively requery every required in-memory source/target session identity fact before returning success; unchanged connection-object pointers alone are insufficient.
  Keep role/database/endpoint bytes private.
- [ ] Require one actual integer server_version_num per side and exact pair 16:16/18:18/16:18.
  Missing/NULL/multiple/noninteger results, major 17, downgrade and every other pair fail before engine mutation.
- [ ] Use one nonrenewable work deadline of at most 60 seconds, connection timeout 10 seconds, statement timeout 4 seconds, lock timeout 1 second and idle-in-transaction timeout 6 seconds, all capped by remaining caller/invocation time.
  Never restart the work budget per connection or query.
- [ ] Run exact target-major `/usr/lib/postgresql/<major>/bin/pg_dump --version` and pg_restore --version via bounded subprocesses without connection argv.
  Require correct named tool, one complete bounded line and exact target major for both.
  Qualify actual effective PG_BINDIR/PG_CONFIG/packaged/PATH choices consistently with src/schema.rs rather than checking an unrelated binary.
  Keep psql 18 available for existing applicable callers without selecting its adjacent pg_restore for a PG 16 target.
- [ ] Implement only fixed SQL for the admitted native profile: connectivity/temporary privilege, selected extension/ownership/clone-rights/RLS, plus follow WAL/publication/slot headroom/replication role/replica-role/unlogged-table/replica-identity checks.
  Source existing SQL semantics from resources.go constants and pin equivalent native query predicates in tests; no generic query string enters the role request.
  Unsupported filters, remediation, all-database or other exposed profiles continue to refuse in nativeConfigCapabilities rather than acquiring new partial semantics.
- [ ] Exclude originGrantsBlock, origin GRANT/repair, legacy restoreMajorBlock/PG 17 gate, pgcopydb help/version and wal2json guidance.
  Legacy script/Job goldens remain byte-for-byte unchanged.
- [ ] Recheck materialization, diagnostics, resolver and both session identities after the final query and require checked positive closure before emitting a passing result.
  Missing check/result/closure, any warning, cancellation or unknown outcome is refusal.
  No workdir, checkpoint, target fence, native state, same-value UPDATE or COMMIT proof is part of this role.
  Ordinary read-only SQL transaction closure is checked, but it cannot manufacture a target flush certificate.
- [ ] Add `TestNativePreflightServerTools`, `TestNativePreflightProbeSelection`, `TestNativePreflightStopsBeforeEffects`, Python fixed-role input/result/canary controls and an actual packaged-runtime PG 16/18/three-pair qualification inventory.
  Stubs protect shape/parsing only; require actual tools/server versions for qualification.
- [ ] Build a separate native preflight Job with backoff 0, bounded deadline and exact image/original execution admission.
  Capture its complete bounded terminal output through JobPreflightNative, parse only the fixed one-document result through ParsePreflightResult and require Passed()==true.
  Extra diagnostic bytes invalidate the result; a successful Job exit alone does not pass.
  Feed that checked result into ensureNativePreflight only after actual capability admission, then apply the same exact known-UID cleanup and final Migration readmission as discovery.
  Use the current preflight credential mechanism, not legacy success-by-exit/generic ensureJob.

Existing reuse limits must stay explicit:

- EvidenceMaterialization and ExecutionRequest require valid Work/PVC/worker fields and cannot be reused unchanged before a PVC exists.
- PrivateState, validate_request, observe and run_completed_evidence require native state, E, logged checkpoint and a completed worker; they are inapplicable and must never be called by preflight.
- InputChannel's completed-evidence request envelope cannot be fabricated to reuse that admission.
- Materialization/ConnectionInput/Session themselves have no PVC requirement and can serve this fixed role directly.
- qualified_resolver_policy currently always raises capability refusal.
  Actual resolver/NSS/HOME/opaque-file/TLS/runtime qualification must clear it through its assigned owner; neither discovery nor fixed-query unit tests may bypass it in production.
- load_runtime currently requires exact stock libpq 180006 metadata and immutable packaged library hash.
  A newer/different runtime requires coordinated qualification, not a broadened version check in preflight.

## Task 5: Truthful capability producer and documentation handoff

**Owner:** Separately assigned engine/image-smoke qualification and docs owners.

- [ ] Freeze the exact capability set from retained actual runtime/consumer evidence, never from reports alone or observed discovery stdout.
  Keep unqualified capability names absent and retain binary/split/large-object gates until M8.
- [ ] Coordinate protocol.rs and actual executable/CLI tests with tools/image/smoke.py, which currently requires capabilities==[].
  Smoke's exact expected set and executable-hash binding must change with the qualified producer before a feature image build.
  Bind source/tree, actual executable, immutable single-platform image/archive/import and raw runtime ImageID independently.
- [ ] Validate actual built version output through the real Go ParseVersionLine boundary and assert exactly the independently frozen set with no PostgreSQL/work initialization or identity output.
- [ ] Update native prerequisites for actual three-pair/target-major tools and skipped origin probe while preserving legacy PG 17 guidance as legacy-only.
  Describe fresh credential-free discovery, fixed safe preflight, exact image selection and remaining activation gates.
  Apply semantic line breaks, humanizer and STE lint to published pages; keep README as an index.
- [ ] Keep production native refusal until source-only lag, strict progress/completion, same-E ordering/acknowledgment, Reader proof, native retry/suspend/deletion, exact cleanup and runtime/PVC gates pass.

## Planned verification

No test/build/runtime command was executed by this planning assignment.
Run focused tests only after their named source/tests exist and the leader grants the resource slot, with Go 1.27.3/GOMAXPROCS 2.
Require nonzero named inventories and zero required skips.

1. Builder and discovery lifecycle:

   ```sh
   go test ./internal/controller -run '^TestNative(VersionJob|PreflightDiscovery|PreflightCreateUncertainty|PreflightReadmission|PreflightCleanup)$' -count=1 -v
   ```

2. Complete original version capture and unchanged evidence quietness:

   ```sh
   go test ./internal/podexec -run '^TestNative(VersionCompletedOutput|DefaultClusterCompletion)$' -count=1 -v
   ```

3. Strict parser and configuration rules:

   ```sh
   go test ./internal/native -run '^TestParseVersion' -count=1 -v
   go test ./internal/controller -run '^TestValidateNativeConfig' -count=1 -v
   ```

4. Fixed safe native DB preflight and unchanged legacy gates:

   ```sh
   go test ./internal/controller -run '^TestNativePreflight(ServerTools|ProbeSelection|StopsBeforeEffects)$' -count=1 -v
   go test ./internal/controller -run '^Test(ReconcileEngineDispatchRejectsBeforeEffects|ReconcileEngineDispatchPreservesLegacy|BuildJobGolden|BuildPreflightJob|PreflightScriptFor_RestoreMajor|PreflightScript_RestoreMajor)$' -count=1 -v
   ```

5. Existing qualified linter:

   ```sh
   /tmp/rpgmigrate-operator-lint-qualification-20261109/bin/golangci-lint run ./internal/controller/... ./internal/podexec/... ./internal/native/...
   ```

Linter SHA-256: `285f51df965bb73c0ad69e4fbdf17d167f404d25ccb805af44654660a5a5f59e`.
Do not rebuild it or broaden into full tests from an individual lease.
Integrate Python fixed-role tests into the existing required evidence program gate, with a missing-module refusal control, rather than an optional standalone driver.
The integration owner runs task lint, task test, task docs and required real PostgreSQL 16/18 coverage once at M6 close, then the leader assigns one independent whole-M6 safety review.
M7 still owns the actual three-pair operator lifecycle matrix.
Commit only after required checks pass and the leader requests it.

## Root decisions before dispatch

- The leader has frozen the minimum fixed-role extension and exact identity/version/check-name/positive-closure result contract.
  Assign one future Python owner for evidence_program.py, its tests and any resolver-policy edit; separate Go owners retain Job/materialization/controller responsibilities.
  A source-file lease means file ownership only; no database lease or new database lock is introduced.
  The concrete reusable functions and inapplicable evidence/PVC/request assumptions are listed above.
- Use the leader-frozen total 90-second bound with at most 60 seconds work and at least 30 seconds cleanup, subordinate to caller deadline.
  Freeze fixed existing condition/event classifications before wiring; no raw failure details reach status.
- Assign TTL/finalizer/owner-GC negative coverage for unknown Create and incomplete known-UID cleanup without adding a retention knob.
- Assign shared-file leases and runtime resolver/image qualification ownership before either fixed DB preflight or capability advertisement can pass.

## Planning review

Applied ponytail full, writing-plans and operator humanizer rules.
Read global/project/operator instructions, CONTRIBUTING, both lessons files, design/roadmap/compatibility/protocol and the approved worker plan, then inspected actual constructors, preflight blocks, parsers, manager wiring, checked transport, Python credential/session functions and Rust env/tool selection.
Root approved direct command, combined-log version parsing, fresh bounded operations, no adoption/cache and exact known-UID cleanup policy.
The worker lifecycle owner confirmed its matcher supplies no capability/transport authority; M8/M10 planning identified the image-smoke empty-capability expectation.
Root also froze private identity/version/check-name/closure results, required in-memory identity rechecks, diagnostic-only optional system_identifier and the 60-second work/30-second cleanup subdivision.
Every review concern has named negative tests in its owner task.
No source/task ledger/dependency/build/test/VM/container/Kubernetes/DB/commit/push changed.
Only this private plan was written.
