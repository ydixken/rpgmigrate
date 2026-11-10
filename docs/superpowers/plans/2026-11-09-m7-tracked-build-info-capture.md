# Reproducible kind manager build-info capture plan

Root approved this implementation plan on 2026-11-09 within the standing execution scope through M10.
The approved private source is `/private/var/tmp/rpgmigrate-m7-build-info-replan-ky1lv4xw/plan.md`, SHA-256 `6324b61a6a02fbcadb0baaff4c4f567ca8fb2cf650774b3398c35ca09f48faa3`.
This repository copy preserves the approved protocol/design and historical planning assertions while applying documentation style.
**Execution note:** Existing approved milestone scope authorizes owned M7 UP/load/legacy/native tests and ordinary fresh owned fault fixtures; source freeze, guard checks, exact ownership, resource admission, and required evidence remain prerequisites rather than a new blanket user-permission gate.
Benchmark execution stays on HOLD for the final actual artifact/argv packet review, and shared-VM poweroff, original retained resources, main pushes/merges, tags, releases, and GHCR publication remain outside this authorization.
This note supersedes historical planning-only or separate-approval wording below where it would request approval again for already approved owned work; it claims no execution result or milestone closure.

> [!IMPORTANT]
> Planning only.
> No source edit, test, dependency operation, API request, VM/container action, token request, tunnel, build, or runtime execution is authorized here.

**Recommendation:** Add one kind-only capture step to the existing Go suite teardown, immediately before manager uninstall.
Keep the approved three-object GET /metrics policy, existing exact-kind selection, and tracked Task entry.
Do not build a private external observer or a general observation framework.

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

## Inputs and preserved history

Current corrected source: `/private/var/tmp/rpgmigrate-m7-check-index-integration-o75xzw9c/source`.
Its 530-entry source identity is `5dcceda452378c2515c09aabd55f932d666f7a940963f4de8d43e5fc4caa33a5`; source manifest is `2f46f044df660bcb622d65f3c0724c5409003c9de72e0238e4b499ed3aec016c`.
Prepared RUN is `/private/var/tmp/rpgmigrate-m7-check-index-run-axergdcg`, run ID `56dcb728c4fe4536ab2c76638f7aaa93`, manager config ID `sha256:47a06be113392a5e51848a521304cb6a7de154846078396187655ffadb447124`.
Its preparation proof SHA256 is `4d89e0731c37a2f8331f07af03bdda713d28b8f7823bcba540ae6e46d9cd394d`.
No UP is claimed.
The four inspected E2E files e2e_suite_test.go, kind_config_test.go, kind_bootstrap_test.go and metrics_test.go are byte-identical between this source and the prior 5cc source.
The approved policy history remains template `667625c2045edd030000e3c5065b83a92206d6d499df7ea77009aa2ea572bb9c` and policy `2342fdf3350981d5de4346165f70781cb74a3478c4247ac2527038dba11806df`.
Preserve old observer plan `b3f3dde334ec1a906d72eb882354acca956ca7242906132b4c6bbbd8c9bbf6a2` and earlier receipts unchanged.

## Compared approaches

The tracked Go hook reuses the suite's validated kind input/raw kubeconfig, controller-runtime client, exact namespace ownership check, existing UID-aware deletion pattern, teardown runEach, and final private receipt writer.
It uses installed client-go for TokenRequest and in-process port-forward, and installed Prometheus common for exposition parsing.
There is no separately supervised child process, arming permit, interprocess auth-ready handshake, source importer, external stage poller, or second receipt transport.
The source change requires a fresh complete source freeze, manager build/preparation, fresh RUN, and subsequent UP/load qualification.
That is a real cost, but makes the proof part of the tested and reproducible Task workflow.

The external alternative preserves the existing prepared image but must duplicate ownership/input/deadline admission, launch and monitor the suite/observer/identity producer, coordinate namespace/auth readiness, supervise a tunnel process, join private receipts, and separately teach future Task/CI runs to launch that machinery.
The approved RBAC effects and actual scrape do not disappear in that approach.
Leaving its artifacts private means ordinary future Task commands cannot reproduce mandatory version evidence.
Tracking that external orchestration would add a new framework rather than the bounded proof requested.
No measured code-size or speed comparison is claimed; the control flows above, not the already completed preparation, decide the recommendation.

## Available helpers, without assuming capabilities

- Existing `connectSuite` validates activeKindSuite raw config through clientcmd and builds `k8sClient`; its rest.Config is local, so capture must derive its own rest.Config from the same validated activeKindSuite.raw, not assume a saved config helper.
- Existing `requireKindNamespace` validates the live namespace run label; add captured UID equality for the capture sequence.
- Existing `suiteCommandArgs` binds kubectl/Helm argv to the private kind file/context, but is unnecessary for an in-process tunnel.
- Existing `promForward.start/stop` targets a Service, fixed port 19090, inherited kubectl context, HTTP Prometheus readiness, and writes tunnel output to GinkgoWriter.
  Do not repurpose it for exact-Pod HTTPS/token access and do not change existing Prometheus metrics tests.
- Existing `promHTTP/promQuery` implement unauthenticated Prometheus HTTP/JSON queries; they do not parse direct manager text exposition or provide the required TLS/token policy.
- Existing `operatorTeardownSteps` returns uninstall/namespace-removal and OTel cleanup steps; `runEach` catches each Gomega failure and executes every remaining step.
  Outer AfterSuite runEach also continues fixture cleanup.
- Existing `kindSuiteReceipt` and ReportAfterSuite write a mode-0600 exclusive receipt after cleanup; Python Guard.receipt validates executed/pass/skip counts, exact spec, terminal conditions, nonempty data, sequences and cleanup.
- Current `kindImage` omits loaded_digest and current `kindSuiteInput` omits lease expiry; add those exact fields rather than infer unavailable runtime IDs/deadlines.
- Installed APIs were inspected: client-go v0.36.4 CoreV1 ServiceAccounts.CreateToken, transport/spdy.RoundTripperFor/NewDialer, portforward.NewOnAddresses/GetPorts, and Prometheus common v0.72.0 expfmt.NewTextParser.
- No `.github/workflows/e2e-kind.yml` exists in this frozen source.
  Do not claim a hosted kind workflow is already implemented or add it to this bounded task.
  The existing Task entry becomes sufficient for a future separately implemented kind CI caller; unit/parser/receipt checks run through ordinary Go tests.

## Exact file ownership and interfaces

One Go implementation owner:

- Create `test/e2e/kind_build_info_test.go`: kind-only capture, concrete approved temporary objects, exact runtime identity, bounded token/tunnel/HTTP operations, parser and cleanup.
- Create `test/e2e/kind_build_info_helpers_test.go`: focused fake-client/HTTP/tunnel/parser tests, without a new Ginkgo suite or live endpoint.
- Modify `test/e2e/e2e_suite_test.go`: record exact newly created operator namespace ownership in ensureNamespace and prepend the guarded capture closure to operatorTeardownSteps; preserve its existing uninstall and OTel closures.
- Modify `test/e2e/kind_config_test.go`: add `LoadedDigest string json:"loaded_digest"` to kindImage, `LeaseExpiresAt float64 json:"lease_expires_at"` to kindSuiteInput, namespace-absence provenance for this run, and current private-input fixtures.
- Modify `test/e2e/kind_bootstrap_test.go`: add required manager_runtime evidence to the receipt and witness acceptance.
- Modify `test/e2e/otel_collector_helpers_test.go`: extend ordered-teardown failure coverage without changing non-kind behavior.
- Let the existing toolchain update go.mod only if importing the already pinned indirect `github.com/prometheus/common` requires direct classification; no version change/new module or unrelated tidy churn is accepted.

One Python contract owner, coordinated against the exact Go field definitions before edits:

- Modify only `hack/e2e-kind.py` and `hack/test_e2e_kind.py` for lease_expires_at input and required manager_runtime receipt validation/fixtures.
- Keep Guard.suite_input/receipt/run signatures, existing Taskfile/public entry, CHECK/load fixes, image/source admission and suite selection unchanged.

One documentation owner:

- Modify `docs/operations/e2e-kind.md` for automatic authenticated build-info proof, approved temporary policy, failure/cleanup behavior, 600-second token headroom and source-freeze requirement.
- No public private-machine/run facts, new CLI/Task option, generic observer command, or helper manual.

Proposed local Go functions and types:

- `captureKindManagerBuildInfo(ctx context.Context, run *kindSuite) (kindManagerRuntimeEvidence, error)`.
- `forwardKindManagerMetrics(ctx context.Context, config *rest.Config, namespace, podName string) (baseURL string, stop func() error, err error)`.
- `readKindManagerBuildInfo(ctx context.Context, baseURL, bearer, expectedVersion string) (string, error)`.
- `parseKindManagerBuildInfo(body []byte, expectedVersion string) (string, error)`.
- `kindManagerRuntimeEvidence` contains only the exact fields described below; no general event schema, metrics map, observer state machine or exported interface.
- Test the concrete API calls with existing controller-runtime/client-go fake clients and interceptors and a local httptest TLS server.
  Permit one unexported tunnel function variable to inject a fake local URL/stop closure; do not add an interface/factory framework.

## Task 1: Add failing narrow contract tests, then implement capture

- [ ] In ensureNamespace, for the active-kind operator namespace only, require absence before Create and capture the successful server-returned UID and run label into `kindSuite.operatorNamespaceUID types.UID`; reject AlreadyExists for this specific new ownership claim and refuse later replacement.
  The actual ensureNamespace producer already sets kindRunLabel from activeKindSuite.input.RunID; do not add synthetic labels or adopt foreign objects.
- [ ] In the new first operatorTeardownSteps closure, return immediately outside active kind mode.
  In kind mode call capture and assert its final returned error only after all internal deferred cleanup has run.
  Use runEach around the resulting full steps list, so capture failure cannot prevent uninstall, namespace deletion, OTel removal or later fixture teardown.
- [ ] Before temporary effects, require the exact owned namespace UID/run label, the expected chart-derived Deployment `pgcopydb-e2e-pgcopydb-operator`, Helm release name/namespace identity, selector, Deployment -> ReplicaSet -> Pod controller UID chain, one ready manager, node assignment, manager image ref, pull policy, restart count, ContainerID, and imageID.
- [ ] Accept CRI imageID only after explicit normalization to the manager's input config ImageID or LoadedDigest manifest; keep those two identities distinct in evidence.
  Missing loaded_digest or invalid source/config/manifest identities refuses before token issuance.
- [ ] Derive expectedVersion as `"m7-" + run.input.SourceTreeSHA256[:12]` only after strict 64-hex input validation.
  For current source it would be `m7-5dcceda45237`; a Go-hook successor must derive its new value after the new source freeze.
  Do not hardcode current/prior version, read chart appVersion as proof, or compare only manager tags.
- [ ] Create exactly the approved rendered SA, ClusterRole and ClusterRoleBinding, with full validated RunID names, SA automount=false, one binding subject and sole GET nonResourceURL /metrics rule.
  Use create-only semantics and absent-name checks; capture returned UIDs/resourceVersions and verify exact fields/run labels.
  No Secret, Pod, namespace, extra grant, role reuse, upsert or mutation of existing objects.
- [ ] Evaluate one exact-SA nonpersistent SubjectAccessReview for GET /metrics, require positive allow, then request one unbound 600-second token through existing privileged suite config.
  Require at least 600 seconds remaining in the explicit input lease deadline at issuance, returned expiration no later than the input lease deadline, default API audience, and no renewal.
- [ ] Derive the API client and SPDY transport from the same validated private kind raw config, keeping its CA verification/client authentication and context unchanged.
  Establish in-process client-go portforward.NewOnAddresses with address 127.0.0.1 and dynamic `0:8443`, targeted to the exact validated Pod name.
  Poll ready channel/GetPorts rather than parse process output; bound tunnel readiness to 15 seconds and close/join its goroutine on every path.
  There is no child process, fixed port, Service-selector target or token in argv/environment.
- [ ] Use a dedicated HTTP client with at most five seconds per request, no proxy, no redirects, and only the loopback tunnel endpoint.
  Permit the already approved self-signed metrics trust only within that tunnel, pin its first peer certificate for that tunnel, and never weaken API TLS or use this transport for another host.
- [ ] Fetch at most 2 MiB plus one sentinel byte, reject excess, parse in memory with installed expfmt, and discard the body.
  Require exactly one gauge sample in pgcopydb_operator_build_info, exactly one label named version, exact expectedVersion, and finite gauge value exactly 1.
  No raw metrics, bearer, headers, certificate bytes, row contents, environment or free-form HTTP/parser/API errors enter logs or evidence.
  Return fixed typed/sanitized errors for these operations.
- [ ] Inside readKindManagerBuildInfo, retain one dedicated transport/peer pin and permit at most three transient scrape attempts within 20 seconds total with the same token/tunnel and owned identity; reject 401/403, changed version, malformed body, replacement or ownership failure immediately.
- [ ] Recheck exact namespace/Deployment/ReplicaSet/Pod UIDs, ContainerID/imageID/restarts after the accepted scrape; replacements cannot inherit proof.
- [ ] A capture operation has a 60-second overall context and bounded per-request timeouts, capped before input lease expiry with a reserved 30-second cleanup window.
  The unchanged wrapper's outer 1800-second suite subprocess limit and any stricter root deadline still win.
  Root must budget enough original lease time for the late AfterSuite capture, not renew the lease to make it fit.
- [ ] Discard token, close/join tunnel and HTTP idle connections, then delete exact created binding, SA and role, in that order, after UID/run-label/structure verification and with UID/resourceVersion DeleteOptions preconditions.
  Use a separate bounded cleanup context within the reserved deadline so a canceled HTTP request does not skip cleanup.
  Verify absence of each exact object; no namespace deletion or suite-resource cleanup is added here.
- [ ] On partial create response loss, unknown object UID, replacement or cleanup conflict, preserve a typed cleanup handoff and refuse success; never manufacture ownership from names/labels alone.
  Join primary and cleanup errors without leaking response bodies or tokens.

## Task 2: Extend private receipt acceptance without weakening existing gates

- [ ] Add `lease_expires_at` to suite-input from the existing authorized resource_lease.expires_at, as a finite positive number.
  Go validates it and uses it only as an upper bound, never as renewal authority.
- [ ] Add nested `manager_runtime` to kindSuiteReceipt, schema_version remains 1 as an additive private field.
  New readers require it; historical receipts without the field fail new acceptance and remain historical evidence.
  Exact source/input SHA binding prevents mixing old/new source receipts.
- [ ] Evidence fields are `expected_version`, `observed_version`, `authenticated`, `namespace_uid`, `deployment_uid`, `replicaset_uid`, `pod_uid`, `node_name`, `image_ref`, `image_config_id`, `image_manifest_digest`, `container_image_id`, `container_id`, `restart_count`, `observed_at`, and `cleanup`.
  No token/raw metrics/error-message fields; cleanup=true requires all exact-created auth objects and the tunnel to be gone.
- [ ] ReportAfterSuite writes the existing exclusive private receipt with that evidence even on capture failure, but its witness and the new Python Guard.receipt reject missing/mismatched version, authentication=false, identity mismatch or cleanup=false.
  Failed capture must fail the Go suite and wrapper, even if the legacy migration itself completed.
- [ ] Python checks exact expected version against its own bound input source prefix, image ref/config/manifest against the bound manager image, UID/nonempty identity formats, zero-or-positive restart count, finite observed timestamp, authenticated=true and cleanup=true.
  Preserve every existing exact-one-spec, zero-skip, terminal conditions, nonempty data/sequence and suite cleanup assertion.
- [ ] Do not write a second receipt file or require private agent files to obtain version evidence.
  Existing Ginkgo lifecycle output and Migration/verification receipt remain the scoped stage/failure/progress observations; no durable general event-history feature is added.

## Required focused tests before fresh preparation

- [ ] Parser: exact expected source-derived version; missing/duplicate family or samples; wrong/extra/duplicate label; wrong version; non-gauge; NaN/Inf/non-1; malformed/truncated/oversized exposition; raw-body/secret sentinels never appear in errors/evidence.
- [ ] API ownership: preexisting namespace, wrong label/UID, wrong Helm/selector/UID chain, zero/two manager Pods, wrong image ref/config/manifest/CRI mapping, replacement/restart between bracketing checks, missing LoadedDigest, invalid source hash and missing/expired lease.
- [ ] Policy: exactly approved rendered object structures and subject; collisions; automount false; one review and token; insufficient 600-second headroom; token expiry errors; no Secret/Pod/namespace creation, broad grant, adoption or renewal.
- [ ] Cleanup: failure after each create/review/token/tunnel/HTTP/parse step; timeout and cancellation; uncertain create UID; replacement/resourceVersion conflict; stop closure failure; exact deletion and readback of all three objects; no foreign deletion.
- [ ] HTTP/tunnel: dynamic loopback target; readiness timeout/early tunnel exit; redirect/401/403/body limit; peer pin change; request timeout; same token attempt bound; close/join on success and error; no process or log/token persistence.
- [ ] Teardown: a capture failure still invokes existing uninstall, namespace cleanup, OTel cleanup and fixture cleanup; non-kind metrics/local/release behavior unchanged.
- [ ] Receipts: historical/missing evidence, wrong version/source/config/manifest, false auth/cleanup, malformed identities/timestamps, exact valid witness and all preexisting negative controls.

After implementation authorization, run the focused new Go tests plus affected Kind/OperatorTeardown tests with cluster gate unset, and the existing Python suite with external operations injected.
Run the independent broad Go/lint/test/docs milestone gates once at milestone close under the repository's resource/owner rules.
No tests have been run for this planning assignment.

## Reproducible execution and root decisions

The unchanged supported entry remains `task e2e:kind:legacy RUN_DIR=<fresh-private-run>`.
It selects exactly `-run '^TestE2E$' -ginkgo.label-filter=kind-bootstrap -ginkgo.fail-on-empty`, with internal two-hour Go/Ginkgo flags but outer suite subprocess budget 1800 seconds and bootstrap budget 300 seconds.
Future Task callers and a separately implemented kind CI workflow receive mandatory authenticated build-info evidence automatically from the tracked source and receipt.
No agent-private observer, permit file, account producer or handshake is required.

Root decisions before implementation:

1. Approve the kind-only teardown hook and required additive receipt/input contract as the selected smallest reproducible design.
2. Confirm that the already approved temporary metrics policy covers the tracked suite being its concrete creator/UID-scoped cleanup owner, retaining the exact same three objects, one unbound 600-second token, read-only grant and tunnel TLS scope.
3. Assign the Go owner, Python contract owner and documentation owner with the exact shared fields above; require independent security/cleanup review before source freeze.
4. Accept a fresh complete source/preparation/RUN cycle instead of UP on the existing prepared 5dcc/47a run if these source edits are approved.
   Retain current preparation as accurate evidence for its existing source, with no adoption, receipt rewrite or claim that its manager contains the future hook.
5. Keep actual UP/load/one-suite execution, resource deadlines, capacity and all milestone/native gates separately authorized and verified.

Skipped: private observer framework, process supervision, extra suite/Task/CLI, durable general event history, new RBAC scope, new dependency versions, source edits and runtime during planning.
Risk: bounded capture or exact-auth cleanup failure makes the entire focused legacy gate fail; it must still finish the existing ordered teardown and preserve explicit incomplete evidence.
