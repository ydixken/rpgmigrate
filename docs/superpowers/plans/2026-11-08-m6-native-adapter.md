# M6 Native Operator Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task by task under the team leader.
> Steps use checkboxes for tracking.

**Goal:** Add an explicit native operator adapter whose supported operations require qualified engine capabilities and whose completion decisions require independently bound evidence.

**Architecture:** Keep the existing operator lifecycle, connection materialization, Job shape, and legacy default.
Add native command, framing, and evidence helpers behind engine dispatch, with one owner for each shared controller file.
Build the additive adapter now, but leave each operation closed until its engine producer and durability evidence are qualified.

**Tech Stack:** Go, controller-runtime, client-go pod exec, the existing shell/psql runner boundary, Helm, and the existing native protocol v1 validators.

**Spec:** [Approved design](../specs/2026-11-06-rpgmigrate-design.md), [roadmap M6 and dated amendments](../../roadmap.md), [native protocol v1](../../native-protocol.md), and [M1 contract plan](2026-11-08-m1-native-contract.md).
Read all four before implementation.

## Global Constraints

- Execute operator changes only in `/Users/ydixken/development/pgcopydb-operator-native-contract-v1`, branch `feat/native-contract-v1`, at the clean retained baseline `5e8cbcbbe7ad9b89cb80a7edc1d5004af5cd4e58`, including the native API reservation from `e8d9b4f` and the M1/M6 pure helpers.
- Read that checkout's `AGENTS.md`, `CONTRIBUTING.md`, vendored mandatory skills, and current operator documentation before edits.
- Leave the original operator checkout and its dirty `tasks/` and `.agents/` untouched and unread.
- Keep operator changes separate from engine changes; this plan file belongs to rpgmigrate.
- Keep omitted `spec.engine` and explicit `pgcopydb` on the existing legacy path.
- Preserve the existing legacy PostgreSQL 17 restore gate and legacy version allowlist behavior.
- Native accepts only qualified 16 to 16, 18 to 18, and 16 to 18 workflows; reject other pairs before engine mutation.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources.
- Keep `/work/pgcopydb`, the existing worker container name, passfile handling, bounded exec behavior, and pod security settings.
- Use `/usr/local/bin/rpgmigrate`, matching-major tools in `/usr/lib/postgresql/16/bin` and `/usr/lib/postgresql/18/bin`, and default psql 18.
- Keep `--rpgmigrate-runner-image` and `rpgmigrateRunnerImage` empty by default until image qualification.
- Keep credentials and row contents out of argv, status, events, diagnostics, and fixtures.
- Add no dependencies, new API version, lifecycle framework, pgcopydb executable alias, or second authority table.
- Run independent review and the Go suites once at milestone close; do not repeat full Go suites after individual tasks.
- Commit only after the required checks pass and the leader requests a commit.

## Review Focus

- A reused PVC or recreated Migration must fail identity checks before any native state action; Task 1 and Task 5 cover this.
- A successful Job with missing, duplicate, truncated, or wrong-command reports must never complete; Task 2 and Task 5 cover this.
- A visible asynchronous checkpoint with an older barrier token must never become `BarrierConfirmed`; Task 3 and Task 5 cover this.
- A cutover crash between status persistence and engine acknowledgment must reuse the same requested endpos; Task 4 and Task 5 cover this.
- A cleanup Job with an empty inventory, unowned origin, or retained workdir must preserve those ownership distinctions; Task 2 and Task 5 cover this.
- A stale or foreign source-slot sample must remain unknown and reset native catch-up confirmation without producing checkpoint or completion evidence; the informational telemetry amendment covers this.

## Readiness and ownership freeze

M1 supplies strict DTO validation and fixtures, not a production adapter.
Its discovery capability array is empty.
The focused plain PostgreSQL 16 clone evidence does not qualify native follow, recovery, cutover, cleanup, or the three-pair operator profile.
The engine completion producer must also conform exactly to M1 before activation: discovery advertises only qualified capabilities, command reports retain their required protocol capability, and completed reports supply all required measured counters.
Do not weaken `ParseReport` to accept the development clone receipt.
The frozen M4 handoff defines native format 8, metadata version 6, and the 29-column logged authority row.
Its assembled implementation is runtime-unqualified; do not infer qualification from this format freeze or convert older state.

The leader freezes the interfaces below before parallel work.
A worker that needs a different signature returns the decision to the leader before another worker consumes it.
All operator paths below are relative to the assigned operator checkout.

| Owner | Exclusive files |
| --- | --- |
| Commands | `internal/controller/backend.go`, `backend_test.go`, new `native_commands.go`, `native_commands_test.go` |
| Framing | new `internal/native/transport.go`, `transport_test.go`; existing M1 `contract.go` only with a separate leader assignment |
| Evidence | new `internal/native/evidence.go`, `evidence_test.go`, `evidence_live_test.go`, `completion.go`, `completion_test.go` |
| Native control | new `internal/native/client.go`, `client_test.go`; leave `internal/sentinel/**` legacy-only |
| Jobs | `internal/controller/resources.go`, `resources_test.go`, new `native_resources_test.go` |
| Lifecycle | `internal/controller/migration_controller.go`, `follow.go`, `verification.go`, `sampler.go`, existing engine dispatch tests, new `native_lifecycle.go`, `native_lifecycle_test.go` |
| Sessions and diagnostics | `internal/progress/progress.go` and its tests, `internal/controller/retry_gate_job.go` and its tests, `internal/pgcopydb/logs.go` and its tests |
| Configuration and docs | `cmd/main.go`, `main_test.go`, chart values/deployment/README, `test/buildconfig/buildconfig_test.go`, affected operator docs and `mkdocs.yml` |
| Closing verifier | No source ownership; runs integrated checks and reports findings to owners |

Commands, framing, native control, sessions, and configuration can build in parallel after the interface freeze.
The Jobs owner is the sole editor of `resources.go`, including its native preflight branch and all embedded command scripts.
The Lifecycle owner is the sole editor of all listed central lifecycle files.
The evidence owner can implement validation/refusal and tests immediately; production evidence construction waits for Task 0's reviewed mechanism freeze and accepted runtime handoff.
No planning worker edits source or installs tools.

### Shared interfaces

Preserve `backendFor(m *v1beta1.Migration) (v1beta1.MigrationEngine, error)` and `validateNativeConfig(m *v1beta1.Migration, capabilities []native.Capability) error`.
The latter consumes actual parsed discovery capabilities; never call it with a fabricated complete capability list to authorize a worker.

Commands produces these controller-package helpers:

```go
func engineExecutable(m *v1beta1.Migration) (string, error)
func nativeWorkIdentity(m *v1beta1.Migration, pvc *corev1.PersistentVolumeClaim) (native.WorkIdentity, error)
func nativeIdentityEnv(work native.WorkIdentity) []corev1.EnvVar
func nativeCloneArgs(m *v1beta1.Migration, attempt int32) ([]string, error)
```

The existing `nativeCleanupArgs(m *v1beta1.Migration) ([]string, error)` is refusal-only, not a produced cleanup implementation API.
Keep that refusal until Task 0 freezes the independently admitted state type and the leader assigns a coordinated replacement signature to Commands and Jobs.
Do not derive cleanup authority from Migration status to make the old signature succeed.

`engineExecutable` returns `pgcopydb` or `/usr/local/bin/rpgmigrate` from effective engine selection only.
`nativeWorkIdentity` requires a canonical lowercase Migration UUID, derives its 32 lowercase hexadecimal run ID, uses the actual owned PVC UID for workdir ID, and fixes checkpoint generation to 1.
It rejects absent UID, wrong PVC owner, deleting PVC, and malformed identifiers.
The Job admission caller compares that identity against any existing Job binding and rejects a replacement PVC or conflicting Job identity.
It never manufactures identity from a name or namespace.
`nativeIdentityEnv` emits exactly the four protocol environment variables; operational Jobs and execs share those values.
Preflight discovery needs no operational identity and does not initialize a workdir.
Native clone arguments contain only options admitted by `validateNativeConfig`, reuse existing effective resource defaults where applicable, and preserve the approved native restart/resume semantics.
Fresh native attempts must never prepend the legacy publication/slot/origin cleanup script.

Framing produces:

```go
func ParseReportLine(raw []byte, want Binding) (*Report, error)
func ParseVersionLine(raw []byte) (*Version, error)
```

`ParseReportLine` requires exactly one complete line beginning `"native-report: "` in the command observation and passes only its JSON bytes to `ParseReport`.
It rejects malformed or multiple framed records, including duplicate identical records; unrelated lines prove nothing.
`ParseVersionLine` accepts exactly the discovery object, without operational framing.
A preflight wrapper can label captured discovery stdout for its own Job-log transport, but must extract exactly one complete discovery document and call `ParseVersionLine`.
Do not parse raw stderr or an arbitrary substring as native evidence.

The earlier native control interface proposal is shown below for context; its `Execer` and UID-owned selector are not frozen implementation interfaces.
After Task 0, Native control and Lifecycle must publish a coordinated signature addendum that binds owned Job/PVC/pod identity before any consumer implements these methods.
Retain the listed command responsibilities and preserve legacy `RunningPod` unchanged.

```go
type Execer interface {
    RunningPod(context.Context, string, string) (string, error)
    InPod(context.Context, string, string, []string) ([]byte, error)
}
func NewClient(exec Execer) *Client
func (c *Client) CurrentEndLSN(ctx context.Context, namespace, job string) (string, error)
func (c *Client) SetEndpos(ctx context.Context, namespace, job string, want Binding) (*Report, error)
func (c *Client) Read(ctx context.Context, namespace, job string, want Binding) (*Report, error)
```

The live-worker client prefixes shell commands with `conn.URIRecover()`, inherits the container's identity/passfile environment, and uses the existing bounded pod exec transport.
This `RunningPod` surface is live-worker-only; post-worker observation and identical-E acknowledgment run in a separately owned helper Job on the retained PVC.
`CurrentEndLSN` calls source `pg_current_wal_insert_lsn()` and accepts exactly one nonzero LSN.
`SetEndpos` requires nonzero `want.EndLSN` and invokes explicit `stream sentinel set endpos <lsn> --json --dir /work/pgcopydb`; it never rereads a new source position.
`Read` uses `stream sentinel get --json --dir /work/pgcopydb` with expected command binding.
This fenced observation is not an active-worker lag sampler; the informational telemetry amendment below supplies that separate source-only responsibility.
The legacy `SentinelOps` interface and client retain their current behavior.

Evidence produces an opaque `ConfirmedCheckpoint` and a function `func (p ConfirmedCheckpoint) Evidence() CheckpointEvidence`.
Its constructor is private to the qualified target evidence reader.
No controller caller can create it from a Report, phase, exit code, SQL row visibility, token syntax, or reported replay position.
The reader consumes expected work and exact owned target provenance independently of report contents.
Until Task 3's producer handoff and Task 0's observation mechanism are accepted, it returns an explicit unsupported-evidence error and no proof.
Retain the existing `Binding`, `CheckpointEvidence`, and `ResourceObligation` DTOs from M1.

## Task 1: Engine selection, canonical identity, and native argument builders

Owner: Commands.

- [ ] Add table tests for both legacy engine spellings, unsupported engines, canonical Migration/PVC identity, exact four-variable output, invalid UUID spelling, missing PVC UID, wrong owner, and replacement PVC.
- [ ] Add golden native argument cases for clone and cleanup and their call-site command cases for follow, progress, catalog, compare, and explicit endpos routes.
  Cover binary/split/large-object features both disabled and capability-required, unsupported instance mode/options, and credential-free argv.
- [ ] Implement the shared helpers using existing option/default helpers where their semantics match.
  Use native resource names supplied by the engine's owned-state handoff, never namespace/name-derived legacy resources.
  Keep legacy builder results byte-for-byte unchanged in existing tests.
- [ ] Make capabilities required by the adapter itself explicit: any path invoking `list progress` additionally requires `progress.v1`; follow requires `cdc.pgoutput`, `cutover.v1`, and `cleanup.v1`; optional compare requires `verify.v1`.
  Do not require or call staged `list tables` for native completion.
- [ ] Format touched Go files and hand off the tested argument cases and actual unresolved producer dependencies for the once-at-close suite.

## Task 2: Framing, positive completion predicates, and cleanup obligations

Owners: Framing owns only transport parsing and framing tests.
Evidence owns completion predicates and independently admitted cleanup obligations in `internal/native/completion.go` and `completion_test.go`.

- [ ] Framing: add tests for exact required space, line start, newline termination, UTF-8, unrelated log lines, empty output, partial JSON, duplicate/multiple records, wrong command/work, and the shared M1 fixtures.
- [ ] Framing: implement `ParseReportLine` and `ParseVersionLine` around the existing strict validators without a second JSON parser.
- [ ] Evidence: add command-specific predicates for clone completion and cleanup completion.
  Clone success requires the expected `clone` report in `completed` phase and explicitly measured complete table/index/byte counters, including measured zero for an empty database; absent/null counters cannot pass.
  Require the durable completion producer guarantee from Task 3 before activating this predicate in lifecycle code.
- [ ] Evidence: build cleanup obligations from independently admitted native owned state, including every exact owned slot/publication, the retained owned `/work/pgcopydb`, and any recorded unowned origin.
  Keep source resource names and ownership independent of the cleanup report.
  Require `complete=true`, exact inventory, owned slot/publication removal, workdir `removed=false`, and unowned origin `removed=false`.
- [ ] Evidence: add omission, extra-resource, duplicate-identity, already-absent-owned-resource, false ownership, retained-workdir, and empty-inventory cases.
  Exit zero alone must fail every completion predicate.

## Task 3: Qualify the independent target evidence boundary

Owner: Evidence, coordinated with the separately owned M2/M3/M4 engine producers.
This is a release prerequisite, not permission for M6 to invent engine DDL or upgrade metadata.

- [ ] Freeze the qualified engine discovery and operational producer output against M1 on both sides, including an accepted strict clone completion report with all required measured counters.
  Empty advertised capabilities or additional development receipt fields keep the relevant operation refused.
- [ ] Freeze an engine-owned handoff documenting exact persisted target provenance, metadata version, schema/table OIDs, source/target database bindings, run/nonce/generation bindings, owned-resource inventory, clone completion state, token encoding, and same-row drain representation.
  Use the exact M4 native-8/metadata-6 29-column layout and confirmed provenance; reject older layouts rather than upgrading them.
  Assign missing producer work to the engine owners, not backward to M1.
- [ ] Require the handoff to define a positive durability observation that distinguishes an asynchronously visible later row from the checkpoint contents covered by its token.
  It must bind one exact logged authority row, the durable applied/certified/anchor maximum, the persisted drain end/witness/kind, and the same checkpoint token.
  Unknown COMMIT requires engine fence reacquisition and a positive synchronous barrier before proof publication.
- [ ] Specify the bounded psql observation against the frozen layout and its same-session/failover behavior before production evidence SQL is written.
  A plain SELECT and a syntactically valid old token are explicitly insufficient.
  No `BarrierConfirmed=true` default, field copied from Report, heuristic replay-distance test, or second authority table is allowed.
- [ ] Implement the opaque reader only after the leader accepts that producer handoff.
  Check the exact catalog object identities and logged relation, strict single-row cardinality, all independent bindings, and the positive durability result before exposing `CheckpointEvidence`.
- [ ] Add PG16 and PG18 helper cases for missing/replaced relation, wrong run/database/nonce/generation, unlogged row, missing evidence, old token with visible newer asynchronous contents, unknown COMMIT, restarted token, and mismatched witness.
  Use separate valid fixtures: later-BEGIN has applied/certified `0/10`, anchor `0/8`, end `0/20`, witness `0/30`, and durable replay `0/10`.
  Keepalive has applied `0/10`, anchor `0/8`, end `0/20`, witness `0/30`, certified `0/30`, and durable replay `0/30`; both certified and durable replay must cover its witness.
  Add a negative keepalive case with certified/durable `0/10` and the same E/W, which must refuse.
  A later-BEGIN witness must not become feedback/reconnect authority.
- [ ] Keep follow/drain activation refused while any required handoff or live case is absent.

## Task 4: Native control and durable cutover request ordering

Owner: Native control.

- [ ] Add fake-exec tests asserting URI recovery/passfile preservation, exact executable/argv, source insert-LSN query, nonzero LSN validation, expected command/work binding, timeout/error propagation, and no legacy origin query or endpos nudge.
- [ ] Implement the client surface only after Task 0 and the coordinated Native control/Lifecycle signature addendum are accepted, using the existing pod exec helper.
  Preserve native operational identity in both Jobs and exec calls.
- [ ] Give the Lifecycle owner a three-step cutover contract: obtain E only when status has no endpos; durably persist E to `status.replication.endpos` and return; on a later reconcile, send exactly E.
  A bounded nonzero command can follow successful local E persistence while the worker holds the target fence.
  Retain E, wait for the worker's actual successful completion, then require matching `end_persisted=true` from an identical-E standalone helper Job and the separately required checkpoint evidence.
  Never wait for successful live JSON acknowledgment before allowing the worker to finish.
  Failed status persistence must prevent the control command.
  Use a native-only optimistic status patch and a live APIReader read bound to Migration UID/generation; on conflict, discard the candidate and use the stored E.
  Never overwrite a winning E, clear it on retry, or resample it after any command error.
  Requeue explicitly because the Migration watch filters status-only events.
  Retried acknowledgment must reuse E even if source WAL advances.
- [ ] Cover crash/restart after E persistence, lost acknowledgment, unavailable pod, lower/mismatched acknowledgment, and repeated idempotent requests.
  Native control never calls legacy `SetEndposCurrent` or emits the legacy logical-message nudge.

## Task 5: Job construction, capability preflight, and lifecycle integration

Owners: Jobs and Lifecycle, with disjoint files as frozen above.
Consume Tasks 1, 2, and 4 first; keep Task 3-dependent activation closed until its evidence exists.

- [ ] Jobs: route worker argv0, compare schema/data scripts, cleanup, catalog/progress, verify, and explicit endpos commands through the selected engine.
  Preserve connection materialization, passfiles, volumes, current container name, resources, and security context.
  The Lifecycle owner changes `ensureJob` to fetch the owned PVC and bind its UID before admitting a native operational Job and to verify existing Job identity too; the Jobs owner supplies pure construction helpers.
- [ ] Jobs: add native preflight discovery before engine mutation, parse its exact `version --json` document, and pass actual capabilities into `validateNativeConfig`.
  The Jobs owner supplies shell transport; the Lifecycle owner owns final Go validation and effects ordering.
  Empty/unknown/missing capability sets fail closed and do not create a worker.
- [ ] Jobs: select matching-target-major pg_restore for native, check source <= target and both majors in {16,18}, require the selected tool to exist and match, and skip native origin grants/repair.
  Retain applicable connectivity, extension, role, RLS, publication/WAL, and replica-role admission checks.
  Keep legacy restore and origin behavior unchanged.
- [ ] Lifecycle: replace the unconditional native dispatch refusal only with the capability/profile preflight path.
  Reject unknown engines before lifecycle effects.
  An unavailable image or unqualified command remains an explicit refusal; a passing parser fixture or focused engine clone test cannot open it.
- [ ] Lifecycle: bind worker and control reports to the expected Job UID/pod attempt, command, and native work identity.
  Phase/progress updates use native reports; missing measurements remain unknown and no legacy version gate or log marker authorizes native progress.
  Do not let SQL stage inference overwrite a validated native phase.
- [ ] Lifecycle: make native plain-clone completion require the strict completed report and qualified durable completion contract.
  Missing/truncated logs, a vanished Job, empty counters, or exit zero cannot produce `CloneCompleted` or `Complete`.
  Native catalog checks use qualified `list progress --json`, never staged `list tables`.
- [ ] Lifecycle: implement Task 4's persisted-E ordering and post-worker helper Job, then validate drain with `Binding.EndLSN` from status and opaque target evidence from Task 3.
  Wrong/missing proof must retain the slot and keep `CutoverCompleted` false.
  Final sequence/large-object sync is an engine completion prerequisite; optional compare cannot replace drain proof.
- [ ] Lifecycle: require validated native cleanup completion before the successful migration path sets `Complete`.
  Preserve existing deletion/namespace-termination escape behavior as an explicit cleanup failure/manual obligation; do not label it successful cleanup.
  Retain workdir for later compare and never drop an unowned origin.
- [ ] Add envtest cases for both legacy defaults, native admission refusal, feature-capability omissions, existing Job/PVC identity mismatch, native report progress, successful Job without evidence, every cutover persistence boundary, withheld drain proof, and cleanup failure/omission.
  Preserve existing absorbing terminal states, suspend/retry behavior, fresh native state rules, and legacy regression assertions.

## Task 6: Session matching, diagnostics, image configuration, and docs

Owners: Sessions and diagnostics; Configuration and docs.

- [ ] Sessions: OR `pgcopydb[%` and `rpgmigrate[%` in the retry-gate census and SyncRep counts, preserving role scope and the URI-pinned application-name fallback.
  Audit `sampleScript`, `stageScript`, `syncRepStatement`, rehearsal cancellation, retry-gate zombie/session consumers, and supervisor-death reaping.
  Native copy role is exactly `copy`, with `apply`, `receive`, and `compare` as the other roles.
  Never apply the pgcopydb supervisor-death heuristic to a native worker; keep rehearsal unavailable until qualified.
- [ ] Sessions: add SQL/golden cases proving both engines match and unrelated sessions do not gain native ownership.
  Preserve connection/role safety predicates; do not broaden zombie termination by prefix alone.
- [ ] Diagnostics: recognize the native safe `"native:<ErrorKind>: "` prefix inside existing `{error_severity,message}` JSON error records.
  Keep gap/BadState failures explicit and terminal where recovery is unsupported; do not classify every native PostgreSQL error as permission denied.
  Add safe-message and credential-redaction regressions while preserving legacy matchers.
- [ ] Configuration: hand the exact `RPGMigrateRunnerImage string` field addition to the Lifecycle owner for `migration_controller.go`; add it to manager wiring, `rpgmigrateRunnerImage` to manager flags state, flag `--rpgmigrate-runner-image`, and chart value `rpgmigrateRunnerImage: ""`.
  `spec.runner.image` overrides the engine-specific manager image; native never falls back to the legacy runner.
  Keep the legacy chart image and existing `--runner-image` unchanged.
- [ ] Add manager flag tests, Helm render/buildconfig assertions, empty-native-image refusal, explicit override tests, and all auxiliary Job image-selection cases.
- [ ] Update operator configuration, prerequisites, live migration, verification, monitoring, troubleshooting, chart README, and quickstart statements affected by engine selection.
  Describe fresh identity/state, supported profile gates, unavailable capabilities, durable failover limits, required proof, retained resources, and missing-evidence refusal.
  Keep README as an index and add no private infrastructure facts.
- [ ] Avoid API changes unless evidence binding cannot survive reconciliation using existing status plus caller-owned Job/PVC/native state.
  If a new persisted field is required, stop for a leader signature freeze, assign API files to one owner, then run `make manifests`, `hack/sync-chart-crd.sh`, and `task docs`; include generated deepcopies if affected.
  Never hand-edit generated CRDs or API reference output.

## Task 7: Integrated closing gate and independent safety review

Owner: Closing verifier, after all owner edits stop.

- [ ] Verify the assigned checkout, branch, baseline `5e8cbcb`, retained M1/M6 helpers, file ownership, generated outputs, and absence of changes in the original operator checkout.
  Record the exact candidate diff and all closed versus refused native operations.
- [ ] Run `task lint` once on the integrated operator tree.
  Report generated-index sensitivity from the operator's lint rules; do not commit without leader instruction merely to make that check pass.
- [ ] Prepare disposable PostgreSQL 16 and 18 services and psql 18 without reading or exposing secret values.
  With a private PG16 `PGCOPYDB_TEST_PGURI`, run `task test` once, then `go test ./test/e2e -skip '^TestE2E$' -count=1 -race -v`.
  Against disposable PG18, run `go test ./test/e2e -skip '^TestE2E$' -run 'Coverage|StampedSchemas' -count=1 -race -v` and the assigned native evidence helper cases.
  Include the substantive native evidence helper cases against PG16 as well.
  These are milestone-closing functional checks explicitly assigned by the leader, not real-cluster E2E.
- [ ] Never set `E2E_CONTEXT`, run the real-cluster `TestE2E`, bypass a context confirmation, publish an image, or deploy production as part of this gate.
  M7 owns the operator-driven 16 to 16, 18 to 18, and 16 to 18 workflow matrix.
- [ ] Obtain one independent xhigh review of capability truthfulness, immutable work/PVC binding, command selection, persisted-E ordering, independent logged-row durability evidence, cleanup ownership, and unchanged legacy behavior.
  Return repairs to their file owners and rerun only affected failed checks before the final integrated result.
- [ ] Report exact command results, skipped prerequisites, and unresolved activation gates to the leader.
  Update roadmap/task evidence through their assigned owner.
  Do not close M6 or claim a native operator workflow while producer qualification, required tests, lint, or review remains open.

## Planning Review

This plan covers M6 adapter readiness and its explicit activation dependencies.
It does not implement M3/M4 engine producers, M5 image qualification, or M7 cluster E2E.
The cross-track activation requirements are exact M1 producer conformance, the frozen engine-owned provenance/drain layout, and a qualified independent durability observation.
The leader approved the bounded mechanism qualification recorded below; evidence-dependent production paths remain refused until its acceptance criteria and all required runtime gates pass.
The leader reviews this plan and its shared signatures before implementation assignments.

## Remaining lifecycle phase amendment (2026-11-08)

This amendment reuses Tasks 1 through 7 and fixes their execution order against the retained operator baseline.
It supersedes any earlier assumption that live-worker JSON acknowledgment is immediately available or that a format reservation supplies target durability evidence.
The leader approved a bounded qualification spike for the existing psql/private-PVC path and native-only optimistic endpos persistence.
The leader retained independent `CheckpointEvidence`; native CLI acknowledgment alone cannot construct it.
Review this amended plan before build assignments.

### Frozen inputs and actual gaps

We bind this phase to these inputs:

- Operator checkout `/Users/ydixken/development/pgcopydb-operator-native-contract-v1`, branch `feat/native-contract-v1`, clean HEAD `5e8cbcbbe7ad9b89cb80a7edc1d5004af5cd4e58`.
- M4 approved contract reference `2026-11-08-m4-cutover-control.md`, frozen SHA-256 `1e1410cda8424bce87cc16ccc35d8f299359f21eb1fcdb599f4da736d4b33171`.
  The primary plan's subsequent API wording reconciliation has SHA-256 `1367c9ad707381e36032cb203346e19406c78ba60f072dcf9ecde5dd1e1d6e8b`; it removes the obsolete `DurableFollowSnapshot.request_floor` accessor without changing accepted-E semantics.
- M4 CLI export manifest `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-cli-export-y19f9sx2/manifest.json`, SHA-256 `0a24517be345937c9551d0c8b63416a8b4f24bc7de478bc8569aa3944b651ff4`.
- M4 documentation export manifest `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/rpgmigrate-m4-docs-export-8q1isjwo/manifest.json`, SHA-256 `795c8daf0d53f7448d32ebb46686767505c6730cdf1f0c572956b2f5e876c284`.

Use each manifest's actual `tree` and verify its file hashes before consuming its source or documentation.
The exported `docs/native-cutover-handoff.md` defines the exact 29-column catalog, target/source receipts, same-row token, terminal completion, and cleanup obligations.
The exports define interfaces and owner evidence; they do not qualify M4 runtime.
Keep M7's separate checkout, cluster bootstrap, and qualification matrix outside these file assignments.

At this baseline `backend.go`, `native_commands.go`, `internal/native/contract.go`, and `transport.go` contain pure selection, identity, argument, framing, and DTO validation.
`nativeCleanupArgs` deliberately refuses without independently admitted owned state.
There is no `internal/native/client.go`, evidence reader, or native lifecycle activation.
`migration_controller.go` dispatches native to `UnsupportedEngine` before lifecycle effects.
`ensurePreflight` trusts Job success, `preflightGate` skips later attempts, and `ensureJob` accepts an existing Job by name only.
`podexec.RunningPod` selects by job-name and Running phase; `JobLogsByUID` already provides a UID-specific log route.
`follow.go` samples legacy status, invokes `SetEndposCurrent`, then stores the returned LSN and nudges WAL.
Those behaviors remain legacy-only.
Native cannot reuse `ensureCleanup`'s success-on-exhaustion result or legacy origin/content comparisons as completion proof.

### Task 0: Qualify the private evidence boundary before freezing consumers

Owner: Evidence, high effort for the bounded experiment; the leader assigns xhigh effort for the eventual durability implementation and independent safety review.
Files: new `internal/native/evidence.go`, `evidence_test.go`, `evidence_live_test.go`, and private fixture inputs under `internal/native/testdata/evidence/`.
The Jobs owner alone edits `resources.go` for the helper Job after this contract is frozen.
No production caller changes in this task.

The existing contract requires more than a separate command invocation:

> The operator compatibility contract requires "an independent read of the owned target control row".
> It also says "Production MUST NOT construct those bindings from the report being checked."

The frozen M4 handoff says "Engine reports cannot construct the caller's `CheckpointEvidence`" and requires the independently supplied endpoint/provenance receipts, saved E, and expected exact relation.
The existing M6 opaque-reader interface preserves that separation.
`validateDrainProof` in `internal/native/contract.go` compares report fields against caller-supplied `Binding.Checkpoint`; deriving both sides from one report defeats that check.

- [ ] Measure the exact existing runner image's executable inventory and versions before assessing filesystem or psql feasibility.
  Bind the inventory to its image digest, including psql 18 and any other stock tools actually present; do not assume only shell/psql are available or that an unmeasured tool exists.
  Install nothing, and base feasibility conclusions on this measured inventory.
- [ ] Add `TestNativeEvidenceRefusesUnprovedInput` before implementing the reader.
  Assert that raw Report data, visible rows, missing provenance, unsupported layout, unknown COMMIT, and empty helper output cannot return a `ConfirmedCheckpoint`.
- [ ] Prove a bounded private input route from the owned PVC to the independent reader using existing runner tools.
  Freeze exact manifest/control files, byte/depth/record limits, path and descriptor identity checks, permissions, synchronization/locking, interrupted-read behavior, and the admitted receipt type before consumers build.
  Bind admission independently to Migration UUID, actual PVC UID, canonical `/work/pgcopydb`, checkpoint generation 1, owned helper Job/pod identity, and recorded endpoint/target/source provenance.
  A raw `cat`, a path string, a report-derived OID, or matching resource name is insufficient.
  Missing/duplicate/oversized/truncated/replaced/symlinked or partially provisioned state must refuse without exposing file contents.
- [ ] Prove the existing psql 18 channel can expose and distinguish every required command result and warning in one target session.
  Privately separate stdout/stderr, check every result/cardinality/transaction boundary, and reject any unclassified notice/warning or incomplete output.
  Include the synchronous-replication cancellation warning with exit zero, dropped COMMIT reply, failed deferred effect, connection loss after COMMIT, and false/multiple/malformed unlock results.
  Do not publish SQL diagnostics, connection strings, manifest contents, or row values through Job logs or Kubernetes events.
- [ ] Qualify exact catalog/row validation under the same advisory fence, full-row same-value `UPDATE 1`, synchronous COMMIT, guarded idle checks, and positively checked unlock on PG16 and PG18.
  The fence is class `0x5250474d` and signed int32 CRC32 bits of the 16-byte run ID.
  Preserve configured `remote_apply`; otherwise require at least `on`, with `fsync=on` and `full_page_writes=on` on the writable primary.
  Same-value barriers preserve the canonical token; actual authority changes increment its counter exactly once and never regress epoch.
  Reacquire the fence and revalidate for an unknown outcome; no earlier output survives as proof.
- [ ] Publish a reviewed internal input/output and function-signature addendum in this plan before parallel consumers start.
  Keep `ConfirmedCheckpoint` opaque with private constructor and `Evidence() CheckpointEvidence` accessor; keep independently admitted cleanup obligations opaque until validated.
  The addendum must identify which existing tools enforce each private filesystem and SQL check, rather than promise an unspecified helper.
  Add no dependency, executable, authority table, CLI option, public report DTO, or API field.
  If existing psql or private file transport cannot meet every check, stop and return the failing experiment and precise missing capability to the leader for re-planning.

Focused commands, with private `PGCOPYDB_TEST_PGURI` already supplied for the selected disposable server:

1. Verify refusal tests before effects.

   ```sh
   go test ./internal/native -run '^TestNativeEvidenceRefusesUnprovedInput$' -count=1 -v
   ```

2. Qualify the reader against each disposable major, once its declared cases exist.

   ```sh
   go test ./internal/native -run '^TestNativeEvidenceLive' -count=1 -v
   ```

Require nonempty named cases and actual observed server major in each private receipt.
A skipped live test or prerequisite failure leaves Task 0 open.

### Remaining build order and file leases

After Task 0's reviewed freeze, assign these disjoint owners in parallel.
Reuse the existing pure helpers and M1 validators; do not reimplement Tasks 1 and 2 from scratch.

1. **Native control and owned pod selection** owns `internal/native/client{,_test}.go` and `internal/podexec/podexec{,_test}.go`.
   Preserve the proposed command responsibilities of `NewClient`, `CurrentEndLSN`, `SetEndpos`, and `Read`, but do not implement against the earlier `Execer` proposal as a frozen API.
   After Task 0, coordinate the exact `Execer`, native-owned selector and consuming client signatures with Lifecycle in one reviewed addendum.
   Preserve legacy `RunningPod` behavior unchanged.
   Assert exact Job UID/controller owner/container identity, PVC binding and attempt before exec; choose no arbitrary first pod from multiple candidates.
   Reuse `conn.URIRecover`, passfile/TLS environment and bounded transport; sanitize native transport failures before logs/events because existing `InPod` errors can contain raw stderr/argv.
   Add `TestNativeClientPersistedEndRetry`, covering exact explicit E, command refusal, unavailable pod, cancellation, and no resampling/nudge.
   A nonzero result supplies no acknowledgment and never clears E.

2. **Jobs and scripts** owns `resources.go`, `resources_test.go`, `native_resources_test.go`, and `verify_script_test.go`.
   Consume the Task 0 admitted-state types and the commands owner's existing identity/builder helpers.
   Route executable, preflight, worker, catalog/progress, native acknowledgment helper, independent evidence helper, cleanup, and compare through the native image and exact known work identity.
   Reuse `scriptJob` for the post-worker helper shape, retaining the PVC, passfile/TLS mounts, read-only rootfs and writable `/tmp`.
   Invoke `/usr/local/bin/rpgmigrate stream sentinel set endpos <saved-E> --json --dir /work/pgcopydb` after worker completion; never invoke `--current` here.
   Keep this acknowledgment distinct from the Task 0 independent observer.
   Bind helper creation to the already persisted E, Job role, Migration UID and PVC UID, and read its reports by actual Job UID.
   Emit one strict native record per observation, and never scan several historical records as though `ParseReportLine` accepted a stream.
   The Commands owner must first receive a coordinated signature assignment for `nativeCleanupArgs`: its existing Migration-only refusal cannot accept admitted receipts.
   Freeze that replacement signature with Task 0's admitted-state type before Jobs consumes it; do not remove the refusal by deriving names from Migration status.
   Native cleanup argv uses the exact admitted-state boundary, without legacy slot/origin flags or fresh-attempt cleanup prefixes.
   Add `TestNativeJobsPreserveIdentityAndConnections` and shell-stub cases for malformed report, command nonzero, warning, and missing helper result.

3. **Evidence implementation and fixture conformance** owns only the Task 0 files and `internal/native/completion{,_test}.go`.
   Implement the reviewed boundary, clone-completion and cleanup predicates without editing `contract.go` unless separately assigned.
   Assert complete measured table/index/byte counters, including measured zero, and exact cleanup obligations from admitted receipts.
   Use separate fixtures: later-BEGIN has E=`0/20`, W=`0/30`, applied/certified=`0/10`, anchor=`0/8`, and durable replay=`0/10`; its witness never becomes feedback or reconnect authority.
   Keepalive has E=`0/20`, W=`0/30`, applied=`0/10`, anchor=`0/8`, certified=`0/30`, and durable replay=`0/30`.
   Reject keepalive with certified/durable=`0/10` at that same E/W.
   Cover visible newer asynchronous rows with old tokens, token rollback/overflow, same-value preservation, wrong OIDs/nonce/database/timeline/operator/PVC, extra catalog objects, and checked closure failure.
   Cleanup tests require terminal intent, complete inventory and exclusive source-slot administration through all retries; uncertain replacement or reappearance preserves resources and reports incomplete.
   Reuse shared M1 fixtures under `internal/native/testdata/v1/`; a dedicated fixture owner updates both engine/operator copies and hash manifests together only if cases are added.
   Never weaken strict M1 parsing to accept an unqualified producer.

4. **Sessions and diagnostics** retains the existing exclusive lease on `internal/progress/progress{,_test}.go`, `retry_gate_job{,_test}.go`, and `internal/pgcopydb/logs{,_test}.go`.
   OR both application-name patterns while preserving role, PID/client and ownership predicates.
   Native roles are `copy`, `apply`, `receive`, and `compare`; a native worker must not enter legacy supervisor-death reaping or snapshot-loss cleanup.
   Pin safe native error-class extraction and credential-redaction tests; an unsupported/gap failure must remain explicit.

5. **Configuration and documentation** owns `cmd/main{,_test}.go`, `charts/pgcopydb-operator/values.yaml`, `charts/pgcopydb-operator/templates/deployment.yaml`, chart README, `test/buildconfig/buildconfig_test.go`, `mkdocs.yml`, and affected operator docs.
   Coordinate only the `RPGMigrateRunnerImage` reconciler field with the Lifecycle owner.
   Freeze `--rpgmigrate-runner-image`/`rpgmigrateRunnerImage` empty defaults and override precedence; native cannot use the legacy image accidentally.
   Document write freeze and outstanding-transaction duties for both Manual and Automatic cutover; the operator does not itself fence applications.
   Update configuration, prerequisites, live migration, verification, monitoring and troubleshooting with qualified versus refused operations and retained resources.
   Apply the operator's prose and STE100 rules and the existing docs build.

Run each task's named focused tests after its edits, verify that the expected tests ran, and hand exact receipts to the leader.
The Lifecycle owner waits for frozen shared signatures and the relevant helpers before editing central controller files.
Do not run broad Go suites or independent reviews after each file task.

### Native lifecycle integration and durable E

Owner: Lifecycle, exclusive files from the original ownership table plus new `native_lifecycle.go` and `native_lifecycle_test.go`.
Use existing `status.replication.endpos` in both served versions.
The field is already a string in v1alpha1/v1beta1, is carried through conversion, and needs no new schema/CRD field.
Avoid changing its legacy semantics or the general legacy `updateStatus` behavior.
If native field documentation changes, assign both API type comments, regenerate API docs through `task docs`, and inspect generated schema output without hand edits.

- [ ] Add envtest context `native lifecycle` covering missing actual capabilities, empty native image, replacement PVC, stale/same-name Job or pod, lost reports, and terminal/suspend/retry behavior.
  A preflight success exit alone must not authorize native work.
  Native admission parses actual `version --json` and calls `validateNativeConfig` before each new operational attempt/helper; retries cannot bypass it by having `status.attempts > 0`.
  Preserve accepted 16:16, 18:18, 16:18 tool/version checks and every legacy regression.
- [ ] Add failing reconciliation tests for source insert-LSN sampling followed by status-write failure, optimistic conflict, controller crash, newer WAL, lost command reply, and completed worker with no Running pod.
  Stop writers and outstanding transactions before sampling; capture this duty in docs/tests without adding a new fencing feature.
  The user performs this duty in both manual and automatic cutover modes.
  With no stored E, live-read the exact Migration and owned PVC, sample nonzero `pg_current_wal_insert_lsn()`, and patch E with `MergeFromWithOptimisticLock` bound to the read UID/generation.
  Return and explicitly requeue after successful persistence; no set-endpos effect belongs in this reconcile.
  A conflict discards the candidate, rereads live state, and reuses the winning E.
  On a later pass, live-read and validate the persisted E and identities before any explicit-E command or helper creation.
- [ ] Preserve E through every running report merge, retry, suspend/resume, worker Job replacement, and helper error.
  Native status updates must not overwrite it from a report or stale sampler snapshot.
  While the worker holds the target fence, bounded JSON refusal is expected to remain a refusal; requeue with unchanged E while the worker independently drains.
  No observed checkpoint polling, local scalar E, legacy `NudgeEndpos`, or exit 3 turns that result into acknowledgment.
- [ ] After the worker Job succeeds, require its UID-bound strict `clone` completed report with complete measured counters and the qualified durable-completion producer guarantee.
  Launch the standalone identical-E native acknowledgment helper on the same admitted PVC and then validate its exact report against the independent Task 0 evidence.
  Order observation/report collection so the immutable completed row and token match exactly; an unknown observer outcome supplies no proof.
  Only both checks may set `CutoverCompleted`; source slots remain until then.
  Optional schema/data compare never substitutes for drain or final sequence completion.
- [ ] Require strict native cleanup completion and exact admitted obligations before `Complete=true`.
  Preserve workdir, metadata, target data and any unowned origin.
  Deletion may retain the existing namespace-termination/manual escape, but record cleanup failure separately and never set successful cleanup or migration completion from that escape.
  Add retry cases for terminal cleanup intent, partial recorded removals, uncertain same-name slot replacement, and administration continuity loss.
- [ ] Preserve native refusal until required engine capabilities, exact producer conformance, the evidence spike/live gates, and image/profile gates are qualified.
  Add no feature flag to bypass the existing refusal.
  Component tests may inject discovery fixtures, but production never receives a fabricated complete capability set.

Focused checks for the declared new cases:

1. Run the affected pure tests after each owner handoff.

   ```sh
   go test ./internal/native -run 'Test(NativeClient|NativeEvidence|NativeCompletion|SharedNativeFixtures|ParseLinesSharedFixtures)' -count=1 -v
   ```

2. Run controller pure builder/retry tests without invoking the envtest suite.

   ```sh
   go test ./internal/controller -run 'Test(NativeJobs|NativeClone|NativeWork|NativeIdentity|NativeCleanup|EngineExecutable|ValidateNativeConfig|RetryGateScript)' -count=1 -v
   ```

3. Run the native envtest context after the existing envtest assets are prepared.

   ```sh
   go test ./internal/controller -run '^TestControllers$' -count=1 -ginkgo.focus='native lifecycle' -ginkgo.fail-on-empty
   ```

### Milestone close and acceptance

After owner edits stop, run original Task 7 once on the integrated candidate and obtain one independent xhigh safety review.
Use the existing operator commands in its checkout with psql 18 and private disposable PG16/18 connection environments supplied by the verification owner.

1. Run integrated lint and unit/envtest gates against PG16.

   ```sh
   task lint
   task test
   ```

2. Run cluster-free PG16 coverage, then repeat the selected coverage command against PG18.

   ```sh
   go test ./test/e2e -skip '^TestE2E$' -count=1 -race -v
   go test ./test/e2e -skip '^TestE2E$' -run 'Coverage|StampedSchemas' -count=1 -race -v
   ```

3. Run the declared evidence-live cases against each major and shared producer conformance cases against actual engine output, retaining nonempty named-case receipts and exact source/image identities.

   ```sh
   go test ./internal/native -run '^TestNativeEvidenceLive' -count=1 -race -v
   go test ./internal/native -run '^TestSharedNativeFixtures$' -count=1 -v
   ```

The verification owner must check actual M4 producer stdout through the authoritative Go `ParseReport` boundary using independently supplied work/end/checkpoint/cleanup bindings.
Fixture acceptance alone does not satisfy that check.
Retain this actual-producer procedure with the Task 0 signature freeze so no test-only proof constructor becomes production evidence.
4. Run the published-page prose checks and strict docs build once for the listed documentation scope.

   ```sh
   python3 .claude/skills/asd-ste100/scripts/ste-lint.py docs/configuration.md docs/reference/prerequisites.md docs/operations/live-migration.md docs/operations/verification.md docs/operations/monitoring.md docs/troubleshooting.md
   mkdocs build --strict
   ```

Require zero hard prose violations and a successful strict build; generated API reference remains exempt from STE100.
`task lint` rejects uncommitted generated CRD/RBAC outputs; report that condition to the leader rather than committing to conceal it.
No schema change is required by E persistence, so unexpected generated schema drift fails review.

Accept M6 implementation only when capability refusals, identity binding, persisted-E/retry ordering, separate post-worker acknowledgment, independent positive evidence, exact cleanup and unchanged legacy behavior all pass on the exact candidate.
The leader records detailed results in `tasks/todo.md` and synchronizes the roadmap; this planning worker edits neither.
Native support remains unclaimed while any engine/runtime/image/review gate is open.
M7 owns cluster workflow qualification; this phase runs no real-cluster E2E, deployment, image publication, production cleanup, commit, or push.

## Informational source lag amendment (2026-11-09)

The leader selected source-only informational telemetry to preserve the existing two-sample catch-up behavior while the native worker holds its lifetime owner lock and target fence.
The frozen M4 `list progress` implementation has no replication LSNs, its sentinel observation requires the target fence, and the completed-evidence reader requires sealed E and terminal follow completion.
None supplies active-worker lag.
We use the exact independently admitted owned source slot's `confirmed_flush_lsn` and source `pg_current_wal_insert_lsn()` instead, without adding an engine command, CLI option, public DTO, API field, dependency, or application-write-fencing subsystem.
This amendment is a planning decision, not production-interface freeze, safety approval, completed Task 0, or native activation.

### Authority and file boundaries

- Evidence owns the test-only private active-state admission prototype and its filesystem tests under the existing `internal/native/testdata/evidence/` boundary, coordinated with `internal/native/evidence_test.go` and `evidence_live_test.go`.
  Reuse the bounded descriptor and strict-JSON machinery without weakening the completed reader or giving telemetry access to its proof constructor.
- Native control owns the eventual source-only query/transport method in planned `internal/native/client.go` and its tests, after the qualified caller/admission signature addendum is reviewed.
  Reuse `conn.URIRecover()`, current environment/passfile/TLS mounts, psql 18, and bounded pod exec.
  Do not change legacy `internal/sentinel/**`, its `SentinelOps` surface, or `podexec.RunningPod` behavior.
- Lifecycle owns the native mapping and fresh two-sample latch in planned `native_lifecycle.go` and its tests, with coordinated changes to `follow.go`, `sampler.go`, and their focused tests.
  Keep one owner per shared controller file; do not add a second polling framework.
- The verification owner later qualifies the composed reader, actual native-8 source-slot producer, and real owned Job/PVC path under the existing closing gates.
  A test-double ownership binding does not authorize production consumers.

The qualification prototype returns an optional informational byte lag scoped to its admitted work/attempt and observation time.
Keep raw positions private to the sampling operation.
The eventual internal telemetry type must have no checkpoint token, barrier-confirmed field, drain witness, acknowledgment, cleanup inventory, or conversion to `ConfirmedCheckpoint`/`CheckpointEvidence`.
Its only controller mapping is the existing optional replication lag/status/metric and catch-up eligibility.
Do not put its slot value into a native report's certified/durable frontier, claim `SourceDurableHeadLSN` from insert LSN, or fill a durable replay field from it.
No production function signature or admitted-state type is frozen by these responsibilities.

### Active private admission while the worker owns the run

Use a separate read-only active-state admission operation.
It must not acquire or wait for the lifetime directory owner lock, require an endpos file or sealed E, acquire the target fence, query or mutate target state, execute a writing barrier, send replication feedback/ACK, or construct completion evidence.
The completed prototype's `admit` takes an exclusive directory flock and requires sealed E, so it must not be called as a telemetry shortcut.

Before exec, independently live-read the exact Migration UID/generation, owned PVC UID/controller, Job UID/controller, selected pod UID/controller, worker container identity/restart and attempt, native work identity, and connection binding reference.
Bind the private manifest's operator identity to those independently supplied facts; a namespace/name, label prefix, arbitrary first Running pod, report field, or caller-authored slot name is insufficient.
Use only the canonical `/work/pgcopydb` under that owned PVC and the existing supported native-8 private layout.
Reuse bounded no-follow/nonblocking descriptor-relative reads, owner/mode/link/type checks, strict JSON byte/depth/record limits, and before/after path/descriptor identity checks.
Validate the confirmed source-slot receipt and complete matching follow binding, including stream/source identity, plugin, slot name, catalog epoch, creation nonce, consistent point, source timeline, COPY generation, and confirmed publication binding.
Reject provisioning, missing binding, restart/replacement, terminal cleanup intent, inconsistent generation, unsupported format, and interrupted or replaced private input as unknown telemetry.
Do not interpret the receipt's existence as target durability evidence.

Take only a bounded nonblocking short control-lock inspection if the qualified private snapshot procedure needs it, then release that lock before source SQL.
After sampling, revalidate the pinned paths and admitted private binding and re-read the live execution/PVC/work identity before publishing the observation.
Changed or unavailable input invalidates the entire sample; never repair files, resume a worker, or adopt another run to obtain a sample.
Qualify this active read procedure with the worker's actual directory owner lock and target fence held.

Source-slot continuity remains the existing exclusive-administration prerequisite from positive creation through the observed run.
A slot has no immutable catalog OID or catalog creation nonce: matching name, plugin, database, observed PID, or application name cannot authenticate an indistinguishable same-name replacement.
The private nonce/catalog epoch bind the admitted receipt; they are not extra server-side incarnation fields.
If continuity is uncertain or the admitted receipt/binding is stale or missing, return unknown.
Do not invent receiver-PID ownership, report-derived incarnation authority, or a new source-resource fencing mechanism.

### Source sample and freshness contract

Run bounded read-only ordinary source SQL through the existing credential channel.
Validate the actual source primary/system/database/timeline/major against the independently admitted source binding and select exactly one admitted slot.
Preserve the existing engine predicates for `pgoutput`, logical database binding, persistent slot, valid retention/positions, and absence of invalidation/conflict or unsupported failover/synced state on the supported majors.
Require an active slot and positive well-formed source insert and confirmed-flush positions; missing, inactive, foreign, invalidated, duplicate, malformed, reversed, or out-of-range data returns unknown.
Require confirmed flush at least the admitted slot's consistent point and restart LSN no greater than confirmed flush, retaining the existing producer's retention checks.
Compute insert LSN minus confirmed-flush LSN with checked arithmetic and the existing signed lag range.
Fresh equal valid positions may yield measured zero; missing/null/zero-LSN positions never do.
Do not advance a slot, request feedback, emit a logical nudge, or perform any other source mutation to improve a measurement.

PostgreSQL defines confirmed flush as consumer confirmation; the engine's qualified feedback path supplies its target-certified meaning, but this observation is not an independent barrier.
Use the PostgreSQL [16 slot view](https://www.postgresql.org/docs/16/view-pg-replication-slots.html), [18 slot view](https://www.postgresql.org/docs/18/view-pg-replication-slots.html), and [WAL position function definitions](https://www.postgresql.org/docs/18/functions-admin.html#FUNCTIONS-ADMIN-BACKUP) when qualifying those measurements.
It may lag apply because feedback is periodic, and source-wide WAL from other databases or filtered work can leave a conservative positive gap.
Do not substitute server-wide flush/write head, `pg_stat_replication.replay_lsn`, write-LSN fallback, receive position, or a retained prior lag.
Use the same read/identity guards before and after the source observation; a changing active-session observation resets confirmation without authenticating that session as the owner.

Native `CaughtUp` requires two distinct newly completed below-threshold observations for the same current admission/attempt, after strict native progress establishes that base copy is complete.
The leader accepted a freshness bound of `2 * pollInterval`, where `pollInterval` is the existing 10-second constant in `internal/controller/migration_controller.go`: reject a sample or prior confirming sample older than 20 seconds when deciding eligibility.
There is no new public option.
Use operator sample-start and completion times from the same `currentTime()`-compatible clock seam, with controlled-clock injection for native tests.
Do not infer freshness from an unchanged LSN, source SQL clock, or client-reported PostgreSQL timestamp.
Require completion no earlier than start, current observation time no earlier than either recorded time, and total sample duration no greater than `2 * pollInterval`.
An over-budget sample or reversed clock resets eligibility rather than becoming a confirming observation.
Never count the same completed serial twice when `Observe` returns a cached sample.
Reset the native confirming latch on missing tracker, unavailable, invalid, stale, timed-out, reversed-clock, reversed-position, or mismatched samples, controller restart, and Job/pod/container/PVC/work/admission changes.
A restart must collect two fresh observations rather than trusting a persisted condition reason as the first one.
Unknown samples clear current native lag/catch-up eligibility and publish no numeric-zero lag metric; preserve only the separately persisted E.
Keep the legacy prior-sample carry and legacy catch-up code unchanged.

### Private confirmation storage in the existing sampler lifecycle

The retained baseline has no native lag timestamp or attempt-bound confirmation record.
`ReplicationStatus` has no observation timestamp; `CloneProgress.ObservedAt` describes base-copy samples and is not a lag timestamp.
The existing `sampled.at` is a progress sample's operator start time, not native completion time.
`lagSeenBelow` reads a persisted condition reason without observation identity or freshness and cannot supply native confirmation.

The forthcoming implementer adds one constant-size private native confirmation record within each existing `Sampler.runs` entry, under the run's existing synchronization.
It contains the current admission fingerprint, operation observation serial, operator sample-start/completion times, and prior qualifying observation with its serial and times.
The fingerprint binds the independently admitted Migration UID/generation, Job/pod/container/restart/attempt, PVC/native work, and source receipt/binding; names alone do not identify a run.
Do not retain a copy of an unbounded report, manifest, or query result in this record.
This is an explicit private sampler extension, not an existing field, CRD change, condition-message encoding, or separate/global cache.
Only a newly completed, valid observation for the current run/admission serial may advance confirmation.
An already consumed or superseded serial cannot count again, even if the cached value is read at a new controller time.
Reserve and validate operation identity under the existing synchronization, release it during bounded exec/SQL, then recheck the same current run/fingerprint before accepting completion.
Cached `Observe` calls do not advance the serial or confirmation, and late/out-of-order callbacks cannot refresh, confirm, recreate, or publish into a stopped or replaced run.
An unknown completed observation clears prior confirmation and supersedes older in-flight observations; an older callback cannot restore the cleared eligibility.
Both qualifying observations must be distinct and fresh for the same admission/attempt at the decision time.

Use the existing `Stop`/run-replacement lifecycle to discard this record on missing/finished work, suspension, deletion, terminal state, and identity change.
The existing manager context/leadership shutdown empties the runs map, and a process restart begins without confirmation state.
One constant-size record per currently tracked active Migration bounds its lifetime and size; add no second map, process-global cache, persistent timestamp, or polling framework.
A missing or unstarted sampler/tracker leaves native catch-up unconfirmed; do not fall back to a saved condition or base-copy timestamp.
The record is conservative eligibility state only and never durability or completion authority.
Its exact internal signature remains subject to Task 0 qualification and the later reviewed consumer addendum.

Two-sample catch-up only permits the already approved cutover-request path; it proves neither drain nor successful completion.
Users stop source writers and outstanding transactions in both manual and automatic modes, retaining that fence through drain and verification.
With no saved E, retain source insert sampling, optimistic E persistence, live readback bound to UID/generation, return/requeue, and a later reconcile's explicit identical-E command.
Telemetry must not combine these effects, replace E with its sampled head, resample E after error, treat busy JSON refusal as acknowledgment, or open cleanup/Complete.
Post-worker acknowledgment and strong private same-row/token/barrier evidence remain required on their existing paths.

### Task 0A: Qualify informational telemetry before consumer freeze

This task extends the existing qualification work; it does not close Task 0 or authorize lifecycle activation.

- [ ] Add `TestNativeInformationalLagAdmission` with test-double cases for exact native work/Job/pod/container/attempt/PVC binding, missing or ambiguous execution identity, changed private descriptor/path/manifest, control-lock contention, absent endpos, unsealed state, restart, cleanup intent, and confirmed-source-receipt mismatch.
  Require unknown telemetry and zero SQL/engine effects when initial admission fails.
  Demonstrate successful active read admission while the worker owner lock and target fence are held, without acquiring either lock or requiring sealed E.
- [ ] Add `TestNativeInformationalLagSample` with source-query cases for missing/inactive/wrong-database/wrong-plugin slots, invalidation/retention errors, source primary/system/timeline/major changes, malformed or duplicate results, null/zero LSN, head behind confirmation, signed-range overflow, SQL failure, timeout, and uncertain slot continuity.
  Assert unknown remains absent and that active PID/name matches alone never satisfy ownership.
  Assert fresh equal positive positions produce measured zero and source sampling makes no mutation, feedback, or nudge call.
- [ ] Add `TestNativeInformationalLagCatchUp` with native lifecycle/metric cases for one versus two distinct fresh samples, the same observation replayed twice, 20-second freshness expiry, a missing sample between valid samples, controller restart, and every admission/attempt change.
  Assert stale prior lag and persisted confirming conditions cannot trigger native cutover, while valid fresh samples preserve the existing configured lag threshold.
  Require unknown metrics to be absent and E to survive every reset.
- [ ] Add focused sampler tests with a controlled operator clock for exact freshness boundaries, sampling duration above `2 * pollInterval`, reversed start/completion/current times, missing/unstarted tracker, and duplicate cached `Observe` results.
  Assert only newly completed serials advance confirmation and that source SQL/client timestamps and `CloneProgress.ObservedAt` cannot extend freshness.
  Cover `Stop`, shutdown/leadership cancellation, restart, same-name Job replacement, and every admission fingerprint change.
  Race a delayed completion against Stop, replacement, a newer observation, and an unknown sample; it must not resurrect a run, publish stale metrics, or confirm the replacement.
  Assert zero retained native record after Stop/context shutdown and at most one constant-size record per active runs-map entry, without another cache or CRD field.
- [ ] Add `TestNativeInformationalLagNonAuthority` using apparently successful asynchronous target-row visibility, a matching engine report, a plausible token, zero lag, and both cutover modes.
  None may construct or populate checkpoint evidence, satisfy drain/acknowledgment, mark CutoverCompleted/Complete, or authorize cleanup.
  Retain separate tests for stopped-writes duty, failed optimistic E persistence, conflict/readback, explicit requeue, and identical-E retry ordering.
- [ ] Qualify `TestNativeInformationalLagLive` against the actual active native-8 source-slot producer with the matching engine candidate on PostgreSQL 16 and 18, then include all admitted pair/runner bindings in the composed operator gates.
  Use the actual privately admitted creation/follow receipts, active producer and its held locks, source insert position, and source-observed confirmed flush; synthetic receipt/ownership fixtures do not qualify this case.
  Retain exact source/image/binary identities and nonempty named-case receipts, including withheld feedback/apply, filtered/source-wide WAL, unavailable/stale input, and unchanged private/target state before and after sampling.
- [ ] Publish the reviewed active-admission/telemetry signature addendum only after its mechanism qualification and the parent Task 0 consumer-freeze prerequisites pass.
  Use test doubles before that freeze; require actual owned Kubernetes Job/pod/PVC/container/attempt admission before native activation.
  Keep the test-only 194-case prototype evidence scoped to its measured components.
  The composed full-path failure/warning/lost-COMMIT/new-exec re-admission, actual M4 producer, strong completion reader, real owned helper, and independent review gates remain open until their required evidence exists.

Run the declared focused cases only after their test implementation and assigned verification inputs exist:

1. Run the pure admission, source-sample, and non-authority cases.

   ```sh
   go test ./internal/native -run '^TestNativeInformationalLag(Admission|Sample|NonAuthority)$' -count=1 -v
   ```

2. Run the native catch-up lifecycle cases without a real cluster.

   ```sh
   go test ./internal/controller -run '^TestNativeInformationalLagCatchUp$' -count=1 -v
   ```

3. Run the live source-slot cases under the independently assigned native-8 producer and disposable PG16/18 fixture route.

   ```sh
   go test ./internal/native -run '^TestNativeInformationalLagLive$' -count=1 -v
   ```

Require actual nonempty named-case execution, measured server major, binding/source/image identities, and zero skips for required cases; a filter that matches no tests or a missing producer/fixture is not success.

Acceptance for this amendment is a qualified informational-only route that supplies fresh native catch-up/metrics while the active worker retains its locks, with invalid input remaining unknown and every completion boundary unchanged.
No plan checkbox or component receipt closes M6; the existing production refusal and milestone/image/runtime gates remain in force.
