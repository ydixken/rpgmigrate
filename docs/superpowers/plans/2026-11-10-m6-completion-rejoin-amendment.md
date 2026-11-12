# M6 Completion Rejoin Admission Amendment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development under root's disjoint leases.
> Steps use checkbox syntax for tracking.

**Goal:** Recover already-completed native executions across stale display status and interrupted cleanup without promoting status or inventory into target completion authority.

**Architecture:** Reuse actual immutable-image discovery with a narrowly admitted retained successful execution.
Add one separate partial-cleanup-intent Reader/producer route around the unchanged target completion predicate and synchronous same-row barrier.
Keep ordinary active admission and the fully cleaned predicate unchanged.

**Tech Stack:** Existing Go/controller-runtime/client-go, checked native transport, fixed Python3/libpq helpers, native format 8 and target metadata 6.

**Spec:** Engine [roadmap](/Users/ydixken/development/rpgmigrate/docs/roadmap.md), [approved design](/Users/ydixken/development/rpgmigrate/docs/superpowers/specs/2026-11-06-rpgmigrate-design.md), [cutover handoff](/Users/ydixken/development/rpgmigrate/docs/native-cutover-handoff.md), and [operator compatibility](/Users/ydixken/development/rpgmigrate/docs/operator-compatibility.md).
This supplements the unchanged final M6 wiring plan SHA-256 `c2e619224e157278bb19d77c2716c85f72cab8de6ee65b5d8bb22e0d9092842f` and UID/retention/TERM amendment SHA-256 `aae59d9908a99a551ba8a4734a9568ac78fdf5d97477ae095cb011aa21c3e384`.
Root froze the decisions below on 2026-11-10 after reviewing these two implementation seams.

## Global constraints

- Frozen input manifest: `/tmp/m6-completion-wave-integration-export-20261110/joined-source-v2-manifest.json`, SHA-256 `bb5b2ff590115c824e6f88e595ac06e4ca09e71bf607df9e26c3c033a402937d`.
- Frozen tar SHA-256 `bd65aba5c0f00f6e3d05c651c1a5d986f18758382e5a46908a28aaa4df7514ec`; patch SHA-256 `1a98ce971e35dfbbc49d5b92537077688bf6745f6a6c8680cc8667b767d62984`; tree `9880a639cd080f966dd87f527f7919b3206fb0b2`.
- The clean central operator authority remains `aaad6c7e117f94e9997194b86ecfe65e1995add0` until the separately owned integration fast-forward.
- Preserve exact recorded worker/PVC receipts and the invocation's unique original Pod/container, image, template and connection bindings throughout all reads and effects.
- Current intent must be nonsuspended and nondeleting; stale Phase/Complete/Failed display values do not independently refuse this narrow completion route or authorize success.
- Preserve the earlier persisted E through conflicts, unknown outcomes and restarts; never resample it, synthesize it, change cleanup flags or derive it from a report.
- Inventory remains inventory; only a qualified Reader path may construct CompletedObservation/ConfirmedCheckpoint.
- Keep target checkpoint contents and token unchanged, including all 29 exact CAS columns and later-BEGIN W>E with certified/durable replay below E.
- Never advance the target frontier after slot removal or use the later-BEGIN witness as feedback/reconnect authority.
- Keep production UnsupportedEngine and canonical NSS/resolver refusal closed through source integration and separate runtime qualification.
- No dependencies, status/API fields, Secret reads, RBAC, dynamic decoder registration, generic recovery framework, live actions or benchmark work belong to this amendment.
- Run owner-focused checks, then the existing single independent whole-M6 review and required broad Go/lint/docs/PG16/18 gates at milestone close.
Commit only after required gates pass and root requests it.

## Why partial cleanup is required

[Roadmap M4 acceptance](/Users/ydixken/development/rpgmigrate/docs/roadmap.md:253) requires idempotent exact-owned cleanup and crash-in-each-phase coverage.
The [handoff](/Users/ydixken/development/rpgmigrate/docs/native-cutover-handoff.md:149) says completed resume observes immutable completion without recreating removed source resources.
The [cleanup contract](/Users/ydixken/development/rpgmigrate/docs/native-cutover-handoff.md:180) retains terminal intent and monotonic partial progress across failures, permits observation of prior durable completion, and requires positive re-observation after an unknown DROP.
The original final M6 plan Task 4 explicitly requires interrupted-cleanup recovery through fresh state admission and the engine's monotonic cleanup.
Its Task 3 requires recovery after cleanup/status conflict, and Task 6 preserves the real target barrier/witness authority.

The engine already supports prior-completion observation: `src/follow.rs:70-106` checks the anchored analyzing state and sealed control, then observes the completed row before selecting copy/stream work.
`src/apply/checkpoint.rs:647-677` requires `follow_completed=true` and exact drain E, then synchronously writes the same row.
`src/cleanup.rs:66-85` reuses durable flags and positively re-observes/removes the saved owned obligations.
`src/state/follow_state.rs:269-340` preserves monotonic cleanup intent and progress; `src/state/control.rs:214-239` permits only the existing sealed boundary under terminal intent.
No new engine behavior is needed.

The safe unsupported boundary remains loss or contradiction of source administration continuity, foreign/reappeared resources, incomplete confirmed provenance, unavailable retained execution, or absent prior completed target seal.
Refusing every partial-cleanup state would reduce the approved recovery scope and would need a separate root/user scope decision.
Root selected the bounded recovery route below.

## Verified source seams

Frozen operator source is the tar above; line references below name its files.

- `internal/controller/native_preflight.go:146-164` rejects terminal display status; `:49-126` performs discovery then provisioning DB checks.
Its phase transport calls live admission at `:171`, `:208` and final closure `:312-324`.
- `internal/controller/native_follow_flow.go:44-58` already admits retained successful follow completion with identical saved E and current nonsuspended/nondeleting intent.
`native_retry.go:60-118` independently rechecks canonical recorded worker/PVC identity and quiescent original Pod.
- `native_clone_report.go:27` and `native_verification.go:41` independently call the active preflight Migration predicate, so their completion-only entry identity checks must consume the new narrow helper too.
- `internal/native/owned_state.go:21-65` returns work/resources only; `owned_inventory_program.py:37,80-82` admits early clone phases and partial flags.
That value cannot choose a completed-state branch.
- `evidence_program.py:873-874` rejects every cleanup intent in the ordinary completed manifest validator.
`cleaned_completion_program.py:9-14` requires both cleanup tombstones true.
Neither predicate changes.
- `owned_inventory_program.py:126-136` already validates current source identity, inactive exact source resources and tombstone-aware absence/reappearance.
`evidence_program.py:963-987` requires the actual completed target seal and exact witness semantics; `:1158-1217` performs the same-value 29-column writing barrier and closure.
- `internal/native/evidence.go:329-370` holds literal strict wire keysets; `:405-409` refuses unregistered roots.
Add only new literal roots under an explicit lease.

## Review focus

- Stale Phase/Complete/Failed display values must not clear current suspend/delete intent, repair missing receipts or supply completion evidence.
- A plain clone must reach optional verification using its exact retained successful execution without requiring or inventing E.
- Cleanup flags false/false, true/false and false/true need fresh target completion authority; early clone, absent intent and fully cleaned state must not enter that route.
- A true tombstone requires current absence; an absent object under a false tombstone needs positive re-observation under the existing uninterrupted administration prerequisite, and uncertainty or reappearance must refuse.
- Unknown COMMIT, helper replacement, incomplete closure or final status conflict must discard proof, retain E/resources and require a fresh qualified invocation.

## Task 1: Completion-specific discovery admission

**Owner lease:** Preflight/admission worker, assigned by root.
Modify `internal/controller/native_preflight.go` and its tests only for the explicit admission callback.
Create `internal/controller/native_completion_preflight.go` and `_test.go`.
Lease the necessary entry checks and tests in `native_clone_report.go` and `native_verification.go` separately from the finalizer's new files.
Do not edit active control/retry matchers, DI, resource builders or proof producers.

**Interfaces produced:**

```go
func (r *MigrationReconciler) nativeCompletionCurrent(ctx context.Context,
    m *v1beta1.Migration, worker *batchv1.Job,
    pvc *corev1.PersistentVolumeClaim, original *corev1.Pod,
) (*v1beta1.Migration, *corev1.Pod, error)
func (r *MigrationReconciler) ensureNativeCompletionPreflight(ctx context.Context,
    m *v1beta1.Migration, worker *batchv1.Job,
    pvc *corev1.PersistentVolumeClaim,
) (*native.Version, bool, string, error)
```

- [ ] Add `TestNativeCompletionCurrent` for stale terminal display acceptance, current suspend/delete refusal, missing/half/wrong receipts, canonical Job/PVC/unique Pod replacement, failed worker and unchanged original identity closure.
Follow requires the same earlier-persisted nonzero E in every live read; plain clone requires follow disabled and never requires, fabricates, mutates or sends E.
- [ ] Add `TestNativeCompletionPreflight` proving actual immutable credential-free version execution, unchanged ParseVersionLine/validateNativeConfig, six-name refusal before credentials, original helper success/closure/UID cleanup, timeout/cancellation and replacement/unknown Create refusal.
Assert no DB provisioning preflight, spare-slot check, clone/stream launch or proof/status constructor is called.
- [ ] Keep `ensureNativePreflight(ctx,m)` as the unchanged active provisioning wrapper.
Thread one explicit private `admit func(context.Context,*v1beta1.Migration,string)(*v1beta1.Migration,error)` argument through `nativePreflightPhase` and its final `nativePreflightExecutionUnchanged` check; ordinary callers pass `r.nativePreflightMigration`.
The completion wrapper's callback repeatedly calls `nativeCompletionCurrent`, binds the first actual original Pod across the whole operation and compares the expected immutable image.
Return actual Version/compatibility data only.
- [ ] Reuse `nativeRecoveryCurrent`/`nativeQuiescentWorker` and existing canonical matchers to implement the narrow identity helper.
Use it for the completion-only entry checks in `readNativeCloneReport` and `ensureNativeVerification`, preserving their own output/comparison authority checks and async scan lifetime.
Do not sanitize a Migration copy or add an allowTerminal parameter.
- [ ] Run the named focused tests and unchanged active preflight regression tests with nonempty test inventory.

```sh
go test ./internal/controller -run '^TestNative(CompletionCurrent|CompletionPreflight|Preflight|CloneOutput|Verification)' -count=1 -v
```

## Task 2: Partial-intent-only completion producer and Reader

**Owner lease:** Separate native producer/Reader worker, assigned by root.
Create `internal/native/cleanup_rejoin_completion.go`, `cleanup_rejoin_admission.go`, `cleanup_rejoin_completion_program.go`, `cleanup_rejoin_completion_program.py`, `cleanup_rejoin_completion_request_writer.py` and focused Go/Python tests.
Modify only literal new wire roots in `internal/native/evidence.go` under its transferred lease.
Existing active, owned and cleaned validators/programs remain unchanged.

**Interfaces produced:**

```go
func CleanupRejoinCompletionProgram() []byte
func CleanupRejoinCompletionProgramSHA256() [32]byte
func CleanupRejoinCompletionRequestWriterProgram() []byte
func CleanupRejoinCompletionMaterialization(m *v1beta1.Migration,
    w ExecutionData) ([]string, []string, []corev1.EnvVar,
    []corev1.Volume, []corev1.VolumeMount, error)
func CleanupRejoinCompletionConnectionReferenceSHA256(m *v1beta1.Migration,
    w ExecutionData) ([32]byte, error)
func (r *Reader) ReadCleanupRejoinCompleted(ctx context.Context,
    w ExecutionData, savedE string) (*CompletedObservation, error)
```

- [ ] Add Python `CleanupRejoinCompletionProgram` cases for exactly the intent tuples `(false,false)`, `(true,false)`, `(false,true)`.
Require full anchored analyzing copy/owned-table state, null local clone completion, exact sealed saved E, complete confirmed receipts and no restart/preliminary state.
Reject absent intent, true/true, early clone, missing files and contradictory binding/control state.
- [ ] Compose existing InventoryState and validate_inventory_manifest with that narrowing, preserving actual cleanup flags.
Acquire the existing lifetime root lock.
Require exact `manifest.json` and `control/{lock,endpos.json}` inventory, and take the existing short control lock while reading/validating the sealed boundary and during private-state revalidation.
Initialization may conditionally skip the control-lock step only before the existing lock FD has been opened; no source/target work starts until full lock/control admission succeeds.
Release the control lock before target-fence acquisition; while the target fence is held, only short control revalidation is allowed.
- [ ] Before and after unchanged `observe`, reuse the actual partial-aware `inventory_source` predicate and positively close the source sessions.
A true removal flag requires absence.
A false flag may match an exact inactive owned resource, or positively observed absence after an unknown DROP under the existing uninterrupted exclusive-administration prerequisite.
No SQL name/plugin/database check proves replacement-slot incarnation or repairs uncertain continuity; contradictory or unknown continuity, active/foreign/reappeared resources refuse and survive.
- [ ] Keep unchanged `validate_control_row`, `observe`, all 29 exact CAS columns, same token/content, `follow_completed=true`, exact E and both drain kinds.
Add positive keepalive and later-BEGIN W>E cases with certified/durable replay below E; assert no source frontier/certification update or new E.
Inject row/witness mutation, false completion seal, unknown same-value COMMIT, premature unlock and every private/materialization/diagnostic closure failure.
- [ ] Add strict request/result kind `cleanup_rejoin_completed_evidence` and literal `cleanup_rejoin_request` roots to the existing decoder map.
Use exact canonical request keys plus saved_e; the bounded result carries existing actual work/checkpoint/resources/guards and positive source/target/materialization/private/diagnostic closure, `cleanup_pending=true` and `source_resources_checked=true`.
Do not reuse cleaned `cleanup_complete`/`source_resources_absent=true` claims for partial state.
Preserve current caps, one-use checked relay, quiet successful main, final live original helper/worker closure and all correlation hashes.
Only after every check may the new Reader construct CompletedObservation through its private fields.
- [ ] Add `TestNativeCleanupRejoinAdmission`, `TestNativeCleanupRejoinAuthority` and `TestNativeCompletedRoutesStayNarrow` covering current nonsuspended/nondeleting intent, stale display status, original successful worker/Pod and recorded receipts before/after transport.
Assert ordinary completion still refuses any cleanup flags, fully cleaned still requires both true, and the new route refuses absent intent/fully cleaned state.
Run the actual existing Go/Python harnesses with explicit nonempty named inventories; a zero selected count is failure.

```sh
go test ./internal/native -run '^TestNative(CleanupRejoin|CompletedRoutesStayNarrow)' -count=1 -v
```

## Task 3: Finalizer consumption and recovery acceptance

**Owner lease:** Existing finalizer owner, new controller files only.
Its `internal/controller/native_cleanup_flow.go` and tests own the helper builder and thin Reader consumer, alongside the already leased new cleanup/follow lifecycle files.
It consumes Tasks 1 and 2; it does not edit their shared source or DI.
Affected operator lifecycle documentation remains a separate root-assigned documentation lease at final integration.

**Interfaces produced:**

```go
func buildNativeCleanupRejoinCompletionJob(m *v1beta1.Migration,
    runnerImage string, w native.ExecutionData) (*batchv1.Job, error)
func (r *MigrationReconciler) readNativeCleanupRejoinCompletion(
    ctx context.Context, m *v1beta1.Migration, worker *batchv1.Job,
    pvc *corev1.PersistentVolumeClaim) (*native.CompletedObservation, error)
```

- [ ] Use JobRoleEvidence, `rpgmigrate-cleanup-rejoin-<op>`, fresh operation/known successful helper Create, exact actual post-create UID, immutable materialization and final decorated size guard.
Reuse the finalizer's known-UID helper terminal/closure lifecycle and completion-specific actual discovery admission.
- [ ] Retain explicit fully cleaned, partial-intent and ordinary completion routes, each requiring its own actual Reader success.
An inventory, stale status, cleanup report or a failed probe cannot positively classify another route.
Unknown output or incomplete cleanup/helper closure holds recovery; discard cached proof and start a fresh qualified same-E invocation later.
- [ ] For partial-intent success, require requested verification and fresh same-invocation owned inventory, then actual engine monotonic cleanup and complete exact obligation/report comparison.
After cleanup, require a new fully cleaned Reader proof before the optimistic Complete patch.
Keep original worker/Pod/PVC/receipts and E on patch conflict; never start a worker, recopy/reapply, clear flags or mark Complete early.
- [ ] Add `TestNativeFollowCleanupRejoin` and `TestNativePlainCloneCompletionPreflight` for stale display status, FF/TF/FT recovery, false-flag actual absence, source reappearance/foreign/unknown continuity refusal, no-E plain-clone verification and final status conflict.
Prove zero legacy SQL/log/sentinel/cleanup fallback and no resource deletion after unknown admission.
- [ ] Add the controlled crash boundaries to the existing authorized actual native producer/Reader fixture on 16:16, 18:18 and 16:18.
Interrupt after cleanup intent, after each positive source removal before/after flag persistence, and after cleanup before final status.
Close the original proof process and start a new producer/Reader; require the original retained 29-column witness/token through a fresh barrier, matching saved E, later-BEGIN C<E control, real flags/absence, positive closure, nonempty required inventory and zero required skips.
No fixture/report/status cache may replace actual SQL/engine evidence.
- [ ] Run focused owner checks, then integrate under root and update affected lifecycle docs.
Leave runtime/activation claims open until the original single whole-M6 review and required broad Go/lint/docs/PG16/18 gates pass.

```sh
go test ./internal/controller -run '^TestNative(FollowCleanupRejoin|PlainCloneCompletionPreflight|FollowCleanupRecovery|CleanupRetry)' -count=1 -v
```

## Planning self-review and handoff

Coverage includes both newly exposed seams, all call sites that independently reused active admission, the three partial intent states, unchanged normal/fully-cleaned rejection boundaries, mandatory target completed seal, same-row barrier/witness semantics and source uncertainty refusal.
Signatures above were coordinated with root and the existing finalizer owner.
The finalizer's helper lifecycle can be shared within its new files; native wire roots remain a separately assigned producer lease.
Root assigns implementation workers after this amendment; this task does not implement or qualify them.

Only this private amendment was written.
No production source, original operator todo/.agents, dependency, test/build, database, Kubernetes, image, VM, benchmark, commit or push changed.
