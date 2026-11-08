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

- Execute operator changes only in `/Users/ydixken/development/pgcopydb-operator-native-contract-v1`, branch `feat/native-contract-v1`, based on `0267c78` plus the retained uncommitted M1 validator and fixtures.
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

## Readiness and ownership freeze

M1 supplies strict DTO validation and fixtures, not a production adapter.
Its discovery capability array is empty.
The focused plain PostgreSQL 16 clone evidence does not qualify native follow, recovery, cutover, cleanup, or the three-pair operator profile.
The engine completion producer must also conform exactly to M1 before activation: its discovery and reports must advertise only qualified capabilities, omit non-contract top-level fields, and supply the completion counters this adapter requires.
Do not weaken `ParseReport` to accept the development clone receipt.
M2's metadata version 4 control row has reserved `certified_lsn` without a CDC producer; do not infer future drain columns or overwrite its format.

The leader freezes the interfaces below before parallel work.
A worker that needs a different signature returns the decision to the leader before another worker consumes it.
All operator paths below are relative to the assigned operator checkout.

| Owner | Exclusive files |
| --- | --- |
| Commands | `internal/controller/backend.go`, `backend_test.go`, new `native_commands.go`, `native_commands_test.go` |
| Framing | new `internal/native/transport.go`, `transport_test.go`; existing M1 `contract.go` only with a separate leader assignment |
| Evidence | new `internal/native/evidence.go`, `evidence_test.go`, `evidence_live_test.go` |
| Native control | new `internal/native/client.go`, `client_test.go`; leave `internal/sentinel/**` legacy-only |
| Jobs | `internal/controller/resources.go`, `resources_test.go`, new `native_resources_test.go` |
| Lifecycle | `internal/controller/migration_controller.go`, `follow.go`, `verification.go`, `sampler.go`, existing engine dispatch tests, new `native_lifecycle.go`, `native_lifecycle_test.go` |
| Sessions and diagnostics | `internal/progress/progress.go` and its tests, `internal/controller/retry_gate_job.go` and its tests, `internal/pgcopydb/logs.go` and its tests |
| Configuration and docs | `cmd/main.go`, `main_test.go`, chart values/deployment/README, `test/buildconfig/buildconfig_test.go`, affected operator docs and `mkdocs.yml` |
| Closing verifier | No source ownership; runs integrated checks and reports findings to owners |

Commands, framing, native control, sessions, and configuration can build in parallel after the interface freeze.
The Jobs owner is the sole editor of `resources.go`, including its native preflight branch and all embedded command scripts.
The Lifecycle owner is the sole editor of all listed central lifecycle files.
The evidence owner can implement validation/refusal and tests immediately; production SQL construction waits for the engine handoff below.
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
func nativeCleanupArgs(m *v1beta1.Migration) ([]string, error)
```

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

Native control produces:

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

The client prefixes shell commands with `conn.URIRecover()`, inherits the container's identity/passfile environment, and uses the existing bounded pod exec transport.
`CurrentEndLSN` calls source `pg_current_wal_insert_lsn()` and accepts exactly one nonzero LSN.
`SetEndpos` requires nonzero `want.EndLSN` and invokes explicit `stream sentinel set endpos <lsn> --json --dir /work/pgcopydb`; it never rereads a new source position.
`Read` uses `stream sentinel get --json --dir /work/pgcopydb` with expected command binding.
The legacy `SentinelOps` interface and client retain their current behavior.

Evidence produces an opaque `ConfirmedCheckpoint` and a function `func (p ConfirmedCheckpoint) Evidence() CheckpointEvidence`.
Its constructor is private to the qualified target evidence reader.
No controller caller can create it from a Report, phase, exit code, SQL row visibility, token syntax, or reported replay position.
The reader consumes expected work and exact owned target provenance independently of report contents.
Until Task 3's producer handoff is accepted, it returns an explicit unsupported-evidence error and no proof.
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

Owner: Framing.

- [ ] Add tests for exact required space, line start, newline termination, UTF-8, unrelated log lines, empty output, partial JSON, duplicate/multiple records, wrong command/work, and the shared M1 fixtures.
- [ ] Implement `ParseReportLine` and `ParseVersionLine` around the existing strict validators without a second JSON parser.
- [ ] Add command-specific predicates for clone completion and cleanup completion.
  Clone success requires the expected `clone` report in `completed` phase and explicitly measured complete table/index/byte counters, including measured zero for an empty database; absent/null counters cannot pass.
  Require the durable completion producer guarantee from Task 3 before activating this predicate in lifecycle code.
- [ ] Build cleanup obligations from independently admitted native owned state, including every exact owned slot/publication, the retained owned `/work/pgcopydb`, and any recorded unowned origin.
  Keep source resource names and ownership independent of the cleanup report.
  Require `complete=true`, exact inventory, owned slot/publication removal, workdir `removed=false`, and unowned origin `removed=false`.
- [ ] Add omission, extra-resource, duplicate-identity, already-absent-owned-resource, false ownership, retained-workdir, and empty-inventory cases.
  Exit zero alone must fail every completion predicate.

## Task 3: Qualify the independent target evidence boundary

Owner: Evidence, coordinated with the separately owned M2/M3/M4 engine producers.
This is a release prerequisite, not permission for M6 to invent engine DDL or upgrade metadata.

- [ ] Freeze the qualified engine discovery and operational producer output against M1 on both sides, including an accepted strict clone completion report with all required measured counters.
  Empty advertised capabilities or additional development receipt fields keep the relevant operation refused.
- [ ] Freeze an engine-owned handoff documenting exact persisted target provenance, metadata version, schema/table OIDs, source/target database bindings, run/nonce/generation bindings, owned-resource inventory, clone completion state, token encoding, and same-row drain representation.
  The current 24-column metadata version 4 layout does not yet provide all follow/drain fields.
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
  Include keepalive coverage and later-BEGIN coverage with applied/certified `0/10`, anchor `0/8`, end `0/20`, witness `0/30`, and durable replay `0/10`.
  A later-BEGIN witness must not become feedback/reconnect authority.
- [ ] Keep follow/drain activation refused while any required handoff or live case is absent.

## Task 4: Native control and durable cutover request ordering

Owner: Native control.

- [ ] Add fake-exec tests asserting URI recovery/passfile preservation, exact executable/argv, source insert-LSN query, nonzero LSN validation, expected command/work binding, timeout/error propagation, and no legacy origin query or endpos nudge.
- [ ] Implement the frozen client surface using the existing pod exec helper.
  Preserve native operational identity in both Jobs and exec calls.
- [ ] Give the Lifecycle owner a three-step cutover contract: obtain E only when status has no endpos; persist E to `status.replication.endpos` and return; on a later reconcile, send exactly E and require the matching `end_persisted=true` acknowledgment.
  Failed status persistence must prevent the control command.
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
- [ ] Lifecycle: implement Task 4's persisted-E ordering, then validate drain with `Binding.EndLSN` from status and opaque target evidence from Task 3.
  Wrong/missing proof must retain the slot and keep `CutoverComplete` false.
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

- [ ] Verify the assigned checkout, branch, base, retained M1 changes, file ownership, generated outputs, and absence of changes in the original operator checkout.
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
The unresolved cross-track activation handoff is exact M1 producer conformance plus the engine-owned provenance/drain layout and positive durability observation accepted in Task 3; evidence-dependent production paths remain refused until that handoff is reviewed.
The leader reviews this plan and its shared signatures before implementation assignments.
