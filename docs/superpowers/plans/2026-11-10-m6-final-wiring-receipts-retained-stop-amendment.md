# M6 Final Wiring: UID Receipts, Retention and TERM Amendment

> **For agentic workers:** Use the existing writing-plans/subagent-driven-development handoff and root's disjoint leases.
> Steps use checkbox syntax for tracking.

**Goal:** Bind native rejoin to durably recorded Job/PVC UIDs and stop workers without destroying the execution evidence needed by completion, retry and cleanup.

**Architecture:** Add two optional status strings to both API versions, enforce their equality independently at every native admission boundary, and add one narrow TERM requester with independent retained terminal observation.
Keep per-invocation Pod identity and existing proof factories, source-only sampling, inventory limits and cleaned-completion barrier/witness rules.

**Tech Stack:** Existing API generation, controller-runtime/client-go, checked exec and fixed Python3 standard-library code.

**Spec:** Root's 2026-11-10 four-policy freeze and final wiring plan SHA256c2e619224e157278bb19d77c2716c85f72cab8de6ee65b5d8bb22e0d9092842f.
The original plan is preserved unchanged.
This short amendment resolves its original-UID, TTL and active-stop decisions; it is not a new design/research cycle.

## Frozen field and authority contract

Add exactly these optional status fields to MigrationStatus in both api/v1alpha1/migration_types.go and api/v1beta1/migration_types.go:

```go
// NativeJobUID records the known native worker Job for the current attempt.
// +optional
NativeJobUID string `json:"nativeJobUID,omitempty"`
// NativePVCUID records the work PVC bound to that native worker attempt.
// +optional
NativePVCUID string `json:"nativePVCUID,omitempty"`
```

Record both known nonzero UIDs atomically with Attempts/JobName in the optimistic successful-Create attempt status patch, before any operator control/exec rejoin.
The values come from the known successful Job Create response and independently admitted actual work PVC, never an observed preexisting worker or report hint.
Both fields remain optional for legacy/API compatibility; native admission treats missing/empty/invalid/mismatched values as refusal.
Receipt-less preexisting/AlreadyExists workers refuse even if canonical.
Known Create followed by failed/lost/conflicted status persistence is an explicit unsupported unknown outcome: no same-pass adoption, receipt filling, Create replay or deletion.
A later operation must independently find exact recorded status receipts and match them; it cannot infer or repair missing receipts from a Job name.
Preserve both receipts and the winning E through every ordinary status merge/conflict/requeue/suspend/delete path.
Only a new known-successful attempt atomically replaces the recorded Job UID; the admitted PVC UID must remain the exact current work PVC.
No new worker-start protocol or hidden status authority is added.

Controller and independent completed, retry, owned-inventory, cleaned and source-client admission require:

```go
func NativeReceiptsMatch(m *v1beta1.Migration,
    workerJobUID, pvcUID types.UID) bool // internal/native
```

This is an identity comparison, not a completion/proof constructor.
Require valid nonempty recorded strings equal to independently expected actual UIDs, plus all existing work/attempt/name/spec/image/owner checks.
Native sourceMigrationMatches checks its Migration snapshot's receipts against ExecutionData; controller callbacks still obtain fresh uncached Migration data.
Do not add receipt values to credentials, public command operands or proof reports; existing ExecutionData UID fields already carry expectations.

## Pod binding and retained identities

Do not add a Pod UID status field.
Under the recorded original Job UID, freshly obtain the unique complete canonical Pod inventory through APIReader, then bind Pod UID/container ID/image/restart0 for that invocation.
Retain and recheck those immutable execution identities through every command, proof, output and stop observation.
Missing/ambiguous/replaced Pod during the invocation refuses.
Do not claim that a new invocation proves historical Pod continuity across operator restarts.
Retry/resume creates a new operator attempt/Job only after the retained prior execution is independently quiescent and the existing native retry authority passes.
A signal response, empty census or missing name cannot supply that authority.

## Worker retention and TTL

The native worker builder sets Job.Spec.TTLSecondsAfterFinished=nil for initial and retry workers until completion, optional verification and required cleanup are durably recorded.
Keep legacy worker and bounded helper TTL behavior unchanged.
Do not silently copy the user's finite TTL into a still-needed native worker.
Only after the durable final status may the controller restore the selected user TTL or perform exact-UID guarded GC; unknown final status preserves retention.
No deletion of retained originals precedes final report/proof/cleanup closure.
Apply this native-only resources.go change after the integration owner exports locale7/cleanup6/sampler2, not concurrently with its current lease.

## Fixed TERM request and independent stop observation

Add one narrow requester; do not widen legacy Sentinel/Progress/log/exec APIs:

```go
// controller/native_stop.go
type NativeStopRequester interface {
    RequestNativeTerm(context.Context, string, string,
        podexec.NativeContainerBinding) error
}

// podexec/native_stop.go; error/nil is request data, never stop proof.
func (e *Exec) RequestNativeTerm(ctx context.Context, namespace, pod string,
    want NativeContainerBinding) error

// controller/native_stop_lifecycle.go
func (r *MigrationReconciler) stopNativeRetained(ctx context.Context,
    m *v1beta1.Migration, worker *batchv1.Job,
    pvc *corev1.PersistentVolumeClaim) (bool, error)
```

The final DI owner alone adds NativeStop NativeStopRequester to MigrationReconciler and injects the same podExec instance in main.
The stop owner defines the interface and implementation in the new files; it does not edit migration_controller.go or cmd/main.go.

Use one fixed credential-free isolated Python TERM script over existing InPodNative transport.
Before signaling PID1, verify /proc/1/exe is the trusted native /usr/local/bin/rpgmigrate executable in the independently admitted immutable container; do not trust process name/argv/version text.
Bound the native-exec/handler startup race within25s and the enclosing checked exec within30s.
A bounded wait for native executable identity and installed TERM handling may use read-only /proc facts; never signal a shell/prelude or invent readiness from elapsed time.
Recheck executable identity immediately before one TERM request.
No new helper file, binary, SQL connection, credential projection, unbounded retry loop or fallback signal is introduced.
Failure/timeout or a reply lost when PID1 exits means unknown request effect.
Do not expose a positive stop/ACK/proof object from the requester.

Independently poll for at most90s total from stop entry, respecting the earlier caller deadline, using the same recorded Job/PVC and the invocation's exact original Pod/container binding.
Allow expected Running-to-terminal status transitions while preserving full canonical specs, ownership, UIDs, image, container identity and restart0; do not compare mutable status byte-for-byte throughout the transition.
Return done=true only after positive terminal quiescence and a final live identity recheck.
A timeout, missing/replaced/ambiguous execution, failed live read or unqualified handler leaves the stop incomplete and retains all resources/receipts/E.
Do not delete the Job/Pod, set Job.Spec.Suspend, use legacy SQL, or use TERM response as completion/inventory authority.
Stop-specific admission accepts the exact current suspend/deletion intent; it must not clear those fields to reuse an active admission helper.

On suspension, stop sampling, request retained stop, independently observe terminal state, then record Suspended while retaining native state/source resources/PVC/Job/Pod and E.
On deletion, the same retained stop precedes fresh inventory -> actual cleanup report comparison -> closure; pending/no-E partial creation still follows the existing confirmed-state refusal limits.
On resume/retry, require retained prior quiescence and actual native retry authority before a new known-successful attempt/receipt patch.
Natural successful completion racing TERM still requires normal/cleaned completion proof before any Complete claim.

## Exact owner/file deltas

**Mechanical API owner, first and separate:**

- api/v1alpha1/migration_types.go and api/v1beta1/migration_types.go.
- api/v1alpha1/migration_types_test.go and api/v1beta1/migration_types_test.go for optional serialization and both-version round-trip coverage.
- Generated api/v1alpha1/zz_generated.deepcopy.go and api/v1beta1/zz_generated.deepcopy.go as produced; scalar strings may produce no generated diff.
- Generated config/crd/bases/pgcopydb-operator.io_migrations.yaml.
- Generated charts/pgcopydb-operator/templates/crd-migrations.yaml via existing sync script.
- Generated docs/reference/api.md.

There are no Go API conversion functions/files in the current api tree.
Both served schemas must have identical optional string status properties and preserve values through their normal JSON representation.
Do not add a conversion webhook or manual conversion framework.
Use existing make manifests, hack/sync-chart-crd.sh and task docs; inspect generated diffs and keep unrelated paths unchanged.
This owner takes no controller/native/podexec/DI/resource-builder files.

**Receipt/retry owner, after schema export:**

- New internal/native/receipts.go/test with NativeReceiptsMatch.
- New internal/controller/native_receipts.go/test with recordNativeAttempt.
- Existing controller/native_worker.go/test, native_retry.go/test and native_control.go/test for recorded-UID live admission and known-successful creation handoff.
- Existing native/admission.go/test, retry_admission.go/test, owned_admission.go/test and client.go/test for independent receipt equality, without changing their proof/state predicates.
- Native-only TTL change in controller/resources.go and focused native resource tests after the integration owner transfers that lease.
- Existing final retry-owner files consume this one receipt commit method; do not separately design UID persistence.

```go
func (r *MigrationReconciler) recordNativeAttempt(ctx context.Context,
    m, base *v1beta1.Migration, job *batchv1.Job,
    pvc *corev1.PersistentVolumeClaim, announce bool) (ctrl.Result, error)
```

Only known successful Create feeds this method.
All validation/status changes precede events/metrics; atomic status contains attempt/name plus both UIDs.
The generic recordAttempt has no PVC argument, so the DI owner removes/bypasses its native branch instead of letting it write receipt-less native status.

**Retained-stop owner, after receipt primitive export:**

- New controller/native_stop.go/test and native_stop_lifecycle.go/test.
- New podexec/native_stop.go/test.
- Own the fixed requester, bounded startup/handler check and independently observed transition, with no DI/shared-resource edits.

**Existing final owners keep their files:**

- DI owner: migration_controller.go, native_dependencies.go/native_dispatch.go and cmd/main.go; adds NativeStop injection and routes native control solely through receipts.
- Follow owner: native_follow.go/test preserves both receipt strings in nativeControlStatusIdentity/updateNativeStatus/ordinary conflict handling, along with E; native_follow_lifecycle routes retained stop/proof joins.
- Clone/verification/cleanup/sampler owners call receipt-aware common live admissions and retain the per-invocation Pod binding; no duplicate receipt implementation.
- Cleaned-proof owner adds the shared receipt predicate to its separate cleaned admission; fresh retained29-column target witness/barrier/closure remains mandatory.

No two owners edit the same file concurrently.
The integration lease on resources.go/native_commands.go remains intact until its accepted export is transferred.
No runtime-driver or M7 VM files belong to these leases.

## Focused acceptance and activation gates

- [ ] Schema owner proves both versions omit absent fields, preserve exact present strings and produce identical optional CRD/chart properties without unrelated API/default changes.
- [ ] Receipt tests reject missing/half/wrong receipts independently in controller, completed, retry, owned, cleaned and source-client admission before transport/effects.
  Test receipt-less AlreadyExists/canonical workers, unknown Create, known Create plus lost/conflicted status, and ordinary status conflicts preserving winning E and both UIDs.
- [ ] Test atomic recording of attempt/name/Job UID/PVC UID and no control/exec before successful receipt persistence.
  Cached-stale versus live replacement tests now bind recorded UIDs, not whichever object a name lookup returns.
- [ ] Test per-invocation unique canonical Pod binding under recorded Job UID, ambiguous/missing/replaced Pod refusal, restart0 enforcement and no claim of durable historical Pod UID.
- [ ] Test native initial/retry TTL nil, legacy/helper TTL unchanged, and no restoration/GC after unknown or failed final status persistence.
- [ ] TERM tests cover shell/prelude PID1, native identity mismatch, handler-not-ready startup race, one request only, timeout/lost exec response, same-container expected status transitions, natural completion race and immutable identity changes.
  The response never creates completion/inventory/checkpoint or a stop verdict without independent terminal observation.
- [ ] Retained lifecycle tests prove Job/Pod survive suspend/delete TERM through retry/owned inventory/cleanup/ordinary and cleaned completion joins.
  No legacy SQL, Job deletion or Job.Spec.Suspend call may appear in the stop route.

Planned focused commands after the owners add the named tests:

```sh
go test ./api/v1alpha1 ./api/v1beta1 -run 'TestNative.*Receipt' -count=1 -v
go test ./internal/native ./internal/controller -run '^TestNative(Receipts|AttemptReceipt|ReceiptAdmission|ReceiptStatusConflict|WorkerTTLRetention)' -count=1 -v
go test ./internal/podexec ./internal/controller -run '^TestNative(TermRequest|RetainedStop|StopStartupRace|RetainedSuspendResume|RetainedDelete)' -count=1 -v
```

Actual-image Kubernetes TERM/handler/startup and retained-lifecycle tests are mandatory before activation, including source partial-copy confirmed-state refusal, real suspend/resume/delete, lost exec reply, retry quiescence and cleanup -> new-process/patch-conflict cleaned proof.
Do not infer signal or process closure from a host unit test.
The exact joined source still needs runtime/image/NSS/file/PVC/source qualification, one independent whole-M6 review and the broad closing gates from the original plan.
UnsupportedEngine and canonical NSS refusal remain closed until root authorizes activation.

Only this private amendment was written.
No production source, dependency, generation/test/build, live API/image/VM, commit or push ran.
