# M6 default Kubernetes result delivery correction plan

> [!IMPORTANT]
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for the separately assigned file owners after root reviews this plan.
> No source implementation or runtime qualification is authorized by this planning artifact.

Root approved this delivery correction on 2026-11-09 and assigned disjoint source owners.
The source plan is `/Users/ydixken/.codex/tmp/2026-11-09-m6-default-kubernetes-delivery-plan.md`, SHA-256 `985430dde104bdc2d873f6676f2a9dbd8ba6cb3a0ba88bcf159ca11dc67d3d7e`.
This repository copy applies Markdown style; approval questions and implementation checkboxes below preserve the reviewed planning record and do not claim implementation or qualification.

**Goal:** Collect independently checked stdout/stderr on default Kubernetes1.36 without PodLogsQuerySplitStreams, while preserving SQL in the admitted helper Job main process.

**Architecture:** Extend the existing checked-exec request writer into one bounded request/result relay over ephemeral FIFOs.
Keep the existing operation in Job main, collect its separate output streams through checked WebSocket exec, then require the exact original Job/container's successful termination before native authority exists.
Use no result file, durable queue, installed helper, new RBAC or feature gate.

**Tech Stack:** Existing Python/os/fcntl/select, Go/client-go, the frozen checked WebSocket-v5 transport and ordinary Pod status/log APIs.

**Spec:** Primary M6 plan SHA `b8039fb8fecae9e6486fa52517acd0c5ead756244a8f59442d61df3e3b997a6f`; private interface addendum SHA `44a3a80cd9ffbc2516406579ac992888ed6702b7f3b0511044dc13e6ee276414`.
Frozen transport source is `/Users/ydixken/.codex/tmp/m6-checked-transport-ixiz179q/source/internal/podexec/native_transport.go`, receipt SHA `4f680e0269b834039c85c957ad4a5aea0f028d6ed99583062bd8d651737ce6e1`.
This proposal supersedes only the completed-evidence delivery/collection portion after root approval.

## Global constraints

- Keep existing request/success/failure JSON schemas, raw-body hashes, operation ID, saved E and all nineteen SQL guards.
- Keep SQL in main; the relay must never connect, repeat SQL, reconstruct evidence or create completion authority.
- Preserve postcreation independent live admission before any source/target connection effect.
- Keep actual source/target sessions and original private evidence state positively closed before emitting success.
- Preserve strict WebSocket-v5 status, normal closure, bounded separate streams and no SPDY fallback.
- Keep input30s, injection phase5s, observation25s, checked-exec30s, terminal collection30s and Job90s as ceilings, with the earliest enclosing deadline winning.
- Keep source/template argument caps from44a3; no increase is implied by adding relay code.
- No retry of a consumed operation or reconnection to its channel after ambiguous disconnect.
- No feature-gate change, public API/CLI, installed helper binary, new RBAC, result spool or lifecycle framework.

## Review focus

- An exec stream can fail after the database operation succeeded: discard the observation and require a fresh independently admitted operation, never rerun the consumed request automatically.
- Main must remain alive until relay process termination without a fixed sleep or a self-reported success marker.
- Both actual output pipes must reach EOF; empty reads before the main writers exist are not EOF proof.
- Main Job success alone must not authorize anything if the checked exec failed, and checked exec success alone must not authorize anything before main exits successfully.
- Container startup diagnostics precede the relay: preserve a separate quiet-main log check and qualify the complete bounded startup/termination route.

## Verified mismatch and alternatives

Frozen JobResultNative calls Pod logs with Stream=Stdout, Stderr and All.
PodLogsQuerySplitStreams is alpha/defaultfalse, including1.36, so that path cannot qualify the default cluster.
The frozen nativeExec already independently handles separate exec stdout/stderr, explicit success status and normal WebSocket closure, but caps stdout at256 bytes for the input-writer acknowledgment.

1. **Recommend: retain main SQL plus one checked-exec relay.**
   Add two output FIFOs and a process-lifetime directory flock to the existing private one-use operation channel.
   Reuse existing exec permission and strict stream checks.
   This retains the approved main-process operation boundary at the cost of a small explicitly reviewed delivery protocol change.
2. **Direct checked-exec SQL under a waiting main.**
   It gives native separate output/status directly, but moves the database/session lifetime out of Job main and needs another handshake proving main termination and original-state closure.
   It is a larger change to the approved operation boundary and is not selected by this plan.
3. **Attach to main before injection.**
   Ordinary attach can carry separate live streams, but current operator RBAC has pods/exec and pods/log, not pods/attach.
   Attach also needs startup synchronization and separate final Job status instead of an exec child exit result.
   It requires a new permission/transport boundary and is not selected.

Do not replace stderr inspection with a flag, discard stderr, or parse merged logs containing an expected stdout envelope as evidence of empty stderr.

## 1. Exact private pipe protocol

Retain `/tmp/rpgmigrate-evidence-<operation_id>` owned by UID65532 mode0700.
Its exact initial entry set becomes request.fifo, stdout.fifo and stderr.fifo, each single-link mode0600 FIFO.
There is no regular result file or replayable output.

1. Main creates/validates all three FIFOs exclusively, retains original descriptor/path identities, and opens its own nonblocking output read keepers before opening output write descriptors.
   Main retains both output writers from before request admission until final output closure, preventing a pre-open empty pipe from being accepted as final EOF.
   It routes operation stdout/stderr to these output writers before loading/using libpq or running the operation.
   Preserve the existing inner bounded diagnostic capture; unexpected outer Python/runtime diagnostics also reach the actual stderr FIFO.
2. The fixed checked-exec relay opens/validates the original private directory and acquires a nonblocking exclusive flock on that directory descriptor before any request byte is written.
   It holds this descriptor until actual process exit, rather than releasing it in an early finally block.
   A second relay cannot consume the operation: retain exclusive one-use request admission and enforce a single collector lease, with a separate nonblocking same-lock acquisition by main after output closure.
   Main never holds that lock while the relay runs.
3. Relay opens the two output FIFOs for nonblocking reading, validates identities, then consumes its checked-exec stdin to EOF as the original bounded credential-free body.
   It writes exactly the existing4-byte length plus body to request.fifo within5s and closes its request writer.
   Main retains the original complete-frame/no-trailing/actual-request-EOF requirements and validates the exact admitted request before SQL.
   Main checks that the collector lifetime lock is held by another open description at this point; an unleased input refuses.
4. Once accepted, main closes/unlinks request.fifo, drops its output read keepers and performs the existing operation once.
   Relay concurrently drains both output FIFOs and copies each stream without merging: stdout to exec stdout, stderr to exec stderr.
   Retain65536-byte stdout and65536-byte stderr ceilings and readiness/deadline polling so either full pipe cannot deadlock the other.
   Any stderr byte, pipe/descriptor replacement, extra output, overflow or I/O fault invalidates the operation and checked result.
5. After all source/target/original-private-state closure checks pass, main unlinks the output FIFO names and removes the now-empty owned operation directory through its original descriptors.
   Relay already holds its open reader and lifetime descriptors, so removal does not discard data or create a replay route.
   Main emits the existing single success envelope through its retained stdout writer and closes both output writers.
   There must be no additional stdout/stderr writer duplicates; close handling is part of the test inventory.
6. Relay reads both actual EOFs, verifies exactly one bounded result envelope, flushes its exec stdout and exits0 only after its own checks pass.
   Its only success stdout is the evidence envelope, replacing the old native-input acknowledgment.
   It emits no ready marker or second line.
   OS process exit releases its directory flock; intentionally retain the lease until that exit.
7. Main polls nonblocking acquisition of that same original directory lock after closing output, bounded at5s and by Job deadline.
   Successful acquisition establishes relay process closure under the admitted fixed relay program; close the lifetime directory descriptor and exit0 if its own operation/cleanup passed.
   A lost relay may still release its lease and allow main to exit, but it cannot manufacture the missing checked exec outcome.
   Timeout or failed channel cleanup makes main nonzero.

The directory lock is a single operation-lifetime signal, not a new state authority or persistent lock service.
Linux directory flock behavior, original path removal and process-exit release must be positively qualified; if unavailable, keep this route refused and return to root instead of adding a general supervisor.

`private_closed` continues to require closure of original evidence state, work locks, source/target sessions and consumed request input before the envelope.
The two output descriptors are delivery streams, like stdout itself, and close immediately afterward.
The reader additionally requires actual output EOFs, relay process success and original main termination before constructing completion authority; no transport descriptor is presumed closed merely from that boolean.
Root must approve this explicit closure ordering before implementation.

## 2. Controller and transport contract delta

Keep InPodNative's signature and sealed NativeOutcome ownership.
For this relay route, its stdout cap becomes65536 bytes instead of the old256-byte writer-only cap.
Retain all existing strict status/normal-close/empty-stderr/pre-post-incarnation checks.
There is no native-input acknowledgment check; the unchanged result envelope binds exact request/program/operation digests.

### Root-approved pre/post execution state clarification

The frozen InPodNative precheck requires the selected exact container to be Running.
Its nativeIncarnation helper already admits both Running and Terminated objects, and the existing postcheck calls that helper plus same() without requiring Running again.
However, nativeIncarnation does not itself validate a terminated exit code, so the implementation must add the following explicit post-exec predicate rather than rely on that helper alone.

- Before opening checked exec, require exactly Running, no Waiting/Terminated state, RestartCount0 and no previous terminated incarnation.
- After checked exec has completed with strict status/normal closure and empty stderr, preserve all original PodUID/JobUID/container ID/image/template/spec/ownership checks and same().
- Accept the same container either exactly Running, or exactly Terminated with ExitCode0, Signal0 and Terminated.ContainerID equal to the original admitted ContainerID.
- Reject Waiting, conflicting/absent state, nonzero/signal termination, any restart/previous terminated incarnation, replacement or identity/spec change.
- Reject an observed failed Job/Pod; do not wait for a failed incarnation to become a different acceptable execution.
- Successful terminated poststate does not replace JobCompletedNative: that separate call still requires exact final JobComplete/PodSucceeded/container exit0, empty ordinary combined logs and final unchanged identity/status.

This handles the normal race in which main observes relay exit and reaches Succeeded before the transport's post-exec GET.
Do not add a post-exec Running-only requirement, and do not accept an arbitrary Terminated state merely because the checked relay succeeded.
This clarification changes no pre-admission requirement and introduces no restart/resume path.

Replace the native-only JobResultNative method with:

```go
func (e *Exec) JobCompletedNative(ctx context.Context, namespace string,
    jobUID types.UID, want NativeContainerBinding) (NativeOutcome, error)
```

Mirror this name in native.NativeTransport.
The method waits for the exact same original Job/pod/container successful terminated state using the frozen completed()/same() checks.
It requires the main container's ordinary combined Pod log to be empty, using PodLogOptions without Stream, TailLines, Previous, Timestamps or Follow.
Read through actual EOF and close, bound the read, sanitize errors, and revalidate live status/identity afterward.
Return a sealed successful outcome with nil payload only after all checks; this receipt contains no evidence body.
Delete the native authority-path split-log queries; preserve legacy JobLogs behavior.

The main is quiet on its container stdout/stderr because operation output goes only to the FIFOs/checked exec.
An empty complete combined log rejects startup/bootstrap output on either stream without split-stream support.
This is an additional guard, not a substitute for separately checked operation stderr.
Qualify log completeness/rotation assumptions for this fresh bounded quiet main; do not claim a tailed/latest fragment proves historical silence.
If an original complete quiet-main log cannot be established, no proof exists.

Reader order becomes: independent live admission, one InPodNative relay call, strict success/result validation retained privately, JobCompletedNative, final independent live/private closure validation, then the private CompletedObservation constructor.
Do not publish a checkpoint between relay success and terminal main verification.
The same original binding is used throughout; replacement, restart, changed template/mount/program/image/attempt or lost API observation discards the result.

The main Job launch and relay argv remain the44a3 forms; evidence_request_writer.py now implements this relay under the same fixed invocation/name to minimize delivery changes.
Its exact source hash changes and stays template/checked-command bound.
Request/result JSON keysets and correlation digests do not change.

## 3. Failure and deadline rules

The earliest enclosing deadline wins; ceilings are not additive promises.
The relay has a30-second lifetime including input injection and observation/drain; main retains25-second observation and5-second post-output lifetime wait ceilings.
This may refuse at the outer30-second bound even when an inner budget has time remaining, which is safe and must be tested.
Job completion collection has its own30-second cap; Job activeDeadlineSeconds90 remains the orphan bound.
If root requires every maximum inner interval to fit serially, that needs an explicit reviewed budget adjustment rather than a hidden timeout increase.

On disconnect/cancellation after injection, do not reattach, replay the body, relaunch SQL or read a stale result from the same operation.
Unknown COMMIT rules remain unchanged.
A later reconciliation uses the persisted same E, a fresh operation ID/helper and full independent admission/barrier, with no claim that the prior attempt failed to commit.
Main and relay own monotonic deadlines even if API disconnect does not kill exec processes.
No checked outcome means no checkpoint/cleanup/Complete, even if the original Job later succeeds.

## 4. File ownership and implementation tasks

- **Python owner:** evidence_program.py, evidence_request_writer.py, evidence_program_test.py, evidence_program.go and evidence_program_test.go only.
  Extend the finite channel lifetime/stream code, preserve operation logic, and prove exact source/template caps without code-golfing safety checks.
- **Transport owner:** native_transport.go/native_transport_test.go only unless root renews the existing podexec.go/test lease.
  Increase the native relay payload cap and replace split-log result collection with sealed terminal/quiet-main verification.
- **Admission/Reader owner:** evidence.go/evidence_reader_test.go plus existing admission tests when needed; no private proof constructors for tests.
  Change the method consumption/order and retain real checked transport positive fixtures.
- **Jobs owner:** existing resources.go/resources_test.go/native_resources_test.go/verify_script_test.go lease.
  Keep main operation and fixed launcher/template; update the expected script/hash/entry set only.
- **Independent verifier:** focused protocol/transport integration and one actual default Kubernetes1.36 test with split-stream feature disabled when root assigns a runtime window.
  It has no production-source lease.
- **Status/docs owner:** record the reviewed delivery amendment and remaining capability gates in primary plan/roadmap only after root selects this correction.

- [ ] Write failing Python tests for concurrent stdout/stderr drain, no early EOF, exact result, pipe overflow, directory/FIFO replacement, one-use request, unleased/duplicate collector, relay death before/during/after COMMIT, no body replay, original-state closure, output close failures and process-exit flock release.
- [ ] Write failing checked transport tests using real WebSocket-v5/status/normal close and fake API logs: default combined empty succeeds, any combined byte refuses, any exec stderr refuses, missing/abnormal close or status refuses, identity changes refuse, and no Stream query parameter is ever sent.
- [ ] Add post-exec race cases: same original main remains Running, becomes exit0 Terminated before the postcheck, becomes nonzero/signal Terminated, restarts or is replaced; only the first two pass the exec postcheck, and both still require the later exact terminal JobCompletedNative proof before authority exists.
- [ ] Write failing Reader tests proving neither successful relay alone nor successful main alone constructs authority, and proving same-E/fresh-op behavior after ambiguous collection.
- [ ] Implement only this delivery delta, with unchanged existing body/result guard fixtures and unknown-COMMIT behavior.
- [ ] Independently run the named focused tests and record nonzero test inventories; do not count an empty filter or skipped case as a pass.

After these tests exist, run in the assigned operator checkout:

```sh
go test ./internal/podexec -run '^TestNative(CheckedExec|HelperResultBinding|DefaultClusterCompletion)$' -count=1 -v
go test ./internal/native -run '^TestNative(PythonRelay|RelayCompletionAdmission|ClientPersistedEndRetry)$' -count=1 -v
```

No broad Go suite or runtime test was run by this planning assignment.
Later live acceptance must use actual default1.36, strict stderr faults before/during/after operation, disconnected relay after a committed barrier, terminal Job/container verification and exact owned cleanup.
Existing unresolved file/service/resolver/TLS/PVC/image qualification gates remain open and unchanged.

## Decisions required from root

1. Approve or reject the bounded two-output-FIFO plus process-lifetime directory-lock relay while SQL remains in main.
2. Approve the explicit closure ordering and the internal JobCompletedNative status/quiet-main contract replacing gated log-result collection.
3. Accept the existing earliest-deadline rule within30 seconds, or assign a separate explicit budget correction if full serial maxima are required.
4. Require actual Linux pipe/flock/default-cluster/log-completeness qualification before activation; no feature gate or RBAC expansion is proposed.

## Primary platform references

- [Kubernetes feature gates](https://kubernetes.io/docs/reference/command-line-tools-reference/feature-gates/): PodLogsQuerySplitStreams is alpha/defaultfalse and controls specific Pod log-stream queries.
- [Kubernetes1.36 logging architecture](https://v1-36.docs.kubernetes.io/docs/concepts/cluster-administration/logging/): ordinary logs and the separate-stream feature boundary.
- [Kubernetes1.36 API reference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.36/): exec/attach/log operations remain separate API subresources.
