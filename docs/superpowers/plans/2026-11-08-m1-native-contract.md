# M1 Native Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task by task.
> Follow the leader's disjoint ownership assignments and review the integrated milestone once at close.

**Goal:** Freeze native protocol v1, CLI and identity constants, and identical cross-repository fixtures without implementing migration phases or controller wiring.

**Architecture:** Extend the engine's existing CLI and the operator's existing strict native validator.
Keep version discovery independent of work identity, bind operational evidence to caller-owned identity, and keep drain coverage distinct from feedback authority.
Each repository tests its local fixture copy; an explicit cross-repository comparison detects drift.

**Tech Stack:** Existing Rust/clap/serde dependencies, Go standard-library JSON and hashing, Python portable test infrastructure, and Task.

**Spec:** [Roadmap M1](../../roadmap.md#m1-contract-freeze-both-repos), [approved design](../specs/2026-11-06-rpgmigrate-design.md), and [operator compatibility](../../operator-compatibility.md).
Operator source input is `origin/main` at `0267c787a900f37702181810c37ec313085e1287`, including the existing `internal/native/contract.go` reservation.

## Global Constraints

- Keep the engine in one Cargo package and preserve the pinned parser baseline `86bf908adca02d87063849dae775a02c1659c829`.
- Preserve explicit refusal of unsupported commands and options; M1 advertises no migration capabilities.
- Never adopt or mutate pgcopydb state or resources, and never report missing proof as successful completion.
- Keep credentials and row contents out of diagnostics and committed fixtures.
- Keep `durable` as the default; accept only the qualified PG16/18 pair contract, without implementing its M2 preflight here.
- Implement no target schema changes, checkpoint writes, control-file writes, migration phases, image packaging, or M6 controller wiring in M1.
- The engine primary checkout remains on `feat/native-engine` with preserved M0 changes.
- Operator changes use `/Users/ydixken/development/pgcopydb-operator-native-contract-v1`, branch `feat/native-contract-v1`, based on the source commit above.
  Preserve the original operator checkout's dirty `tasks/todo.md` and `.agents/` without reading or changing them.
- Keep M0 open and strict lint unchanged under the user-approved 2026-11-08 sequencing decision.
  Do not commit while required gates are red.

## Review Focus

- Identity-free version discovery must work without PostgreSQL, native state, or operator identity environment variables.
- A later-BEGIN drain can cover cutover while feedback authority remains below cutover; accept that proof without promoting the BEGIN position.
- A syntactically valid report with wrong caller identity, endpos, durable token, or target-row evidence must fail closed.
- Missing cleanup obligations and missing completion counters must remain unknown or incomplete, even with exit zero.
- Local unit tests must pass with the other repository absent; a separate parity gate must reject fixture drift.

## Frozen Interfaces and Constants

### Discovery, transport, and CLI

`rpgmigrate version --json` emits one JSON object and a newline: `{"protocol_version":1,"engine_version":"0.1.0","capabilities":[]}` at the current Cargo version.
Obtain `engine_version` from Cargo rather than a second literal version source.
The exact required fields are `protocol_version`, `engine_version`, and `capabilities`; work identity and operational fields are forbidden here.
Plain `version` retains its existing text identity and libpq version output.
Add Go `ParseVersion(raw []byte) (*Version, error)` alongside the retained `ParseReport(raw []byte, want Binding) (*Report, error)`.
Known capability names remain the existing nine operator constants; unknown or duplicate capabilities fail, and actual M1 version output contains an explicit empty array.

Operational records use one stdout line prefixed exactly `"native-report: "` followed by the protocol-v1 Report JSON.
Retain the existing phase enum, counter representation, strict key spelling, duplicate/unknown-member refusal, exact integer ranges, and maximum JSON nesting depth 32.
Transport fixtures cover unrelated lines, truncated records, malformed prefixed records, and more than one record without turning absence into success.
The callable report parser receives the JSON document after the prefix is removed; M6 owns log-tail selection and controller use.
Compare is outside `ParseReport`: preserve the existing per-table JSON array and `"compare progress: "` message shape consumed by `internal/pgcopydb/compare.go` and its tests.
`list tables` stays an explicit staged refusal.

Retain all 43 operator option names and the complete registered parser baseline.
Create a fixture contract entry for every operator name with its command, syntax, environment override where present, native-stage disposition, and expected refusal category.
M1 changes no unsupported migration route into an executable route.
Freeze `PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGPASSFILE`, `PGCOPYDB_LOG_JSON`, and `PGCOPYDB_APPLY_GROUP_TXNS` by name, preserving existing CLI/environment precedence and boolean syntax.
The future apply-group default is 100; M1 does not execute grouping.
Keep passfile/service/TLS behavior delegated to stock libpq.
Keep exit codes `BadArgs=1`, `BadConfig=2`, `BadState=3`, `Pgsql=4`, `Pgctl=5`, `Source=6`, `Target=7`, `Reload=9`, `Internal=12`, and `Fatal=122`.
Gap refusal is `BadState`, exit 3.
JSON diagnostics retain existing fields and add `error_severity: "ERROR"`; the safe message begins `"native:<ErrorKind>: "` for native error-class matching.

### Identity and layout

The identity environment names are `RPGMIGRATE_MIGRATION_UID`, `RPGMIGRATE_RUN_ID`, `RPGMIGRATE_WORKDIR_ID`, and `RPGMIGRATE_CHECKPOINT_GENERATION`.
Operator `migration_uid` is a canonical lowercase UUID, `run_id` is that UUID's 32 lowercase hexadecimal digits without hyphens, and `workdir_id` is the PVC UID.
Validate the supplied run ID equals the decoded Migration UID and maps exactly to the existing engine `RunId([u8;16])`; refuse malformed, partial, mismatched, or noncanonical operator identity.
The current profile's checkpoint generation is immutable decimal `1`; COPY generations are separate and a future identity-generation lifecycle change requires contract review.
Standalone runs retain random native run IDs and do not fabricate an operator Migration/PVC identity.
Identity binding alone grants no native-state mutation authority; M2 must bind it to recorded state, with the required format revision.
Do not change the current state format 4 in M1.
Validate identity configuration all-or-none before I/O for operational commands.
Until M2 persists and proves the binding, explicitly refuse operational commands with supplied identity before mutation, including an otherwise implemented command.
Help and version remain independent of these identity environment variables, including malformed or partial identity input.

Keep owned metadata at `_rpgmigrate_<run_id>.checkpoint`, using recorded schema/table OIDs and existing provenance for authority.
The operator's PVC workdir remains `/work/pgcopydb`; its native identity marker prevents legacy adoption despite the retained mount path.
Freeze `control/lock`, `control/endpos.json`, and `control/.endpos.tmp` as the later M4 control layout.
Application names are `rpgmigrate[<pid>] <role>`, with exactly `copy`, `apply`, `receive`, and `compare` roles and SQL LIKE prefix `rpgmigrate[%`.
Freeze future operator flag `--rpgmigrate-runner-image`, chart value `rpgmigrateRunnerImage`, and an empty default until a qualified image is published.
The runner executable is `/usr/local/bin/rpgmigrate`, matching-major tools live at `/usr/lib/postgresql/{16,18}/bin`, and default `psql` is major 18.
Flag/chart wiring and the image remain M6/M5 work.

### Drain evidence and cleanup

Add `replication.certified_lsn`; retain `durable_replay_lsn` as the producer's barrier-proven maximum of actual durable applied end, keepalive-only certified LSN, and copy anchor.
Take that maximum from a positive durable-proof/barrier snapshot, never a currently visible asynchronous row carrying an older barrier token.
Do not add applied/anchor telemetry solely to let a JSON parser recompute target authority.
For replication/drain reports, require explicit certified LSN measurement rather than interpreting absence as zero.
Add `drain_proof.kind` with exactly `keepalive` or `later_begin`; retain its work, checkpoint ID, committed, end LSN, and replay LSN fields.
Remove the existing requirement that proof replay LSN equal `durable_replay_lsn`.

The same logged authority row will hold `drain_end_lsn`, `drain_replay_lsn`, and `drain_kind` in M2/M4.
Proof end equals caller endpos and persisted drain end; proof replay equals the persisted qualified witness position.
For keepalive, witness W is at least endpos E and no greater than persisted `certified_lsn`.
For later BEGIN, W is exactly `BEGIN.final_lsn` and strictly greater than E; W may exceed both `durable_replay_lsn` and `certified_lsn`.
Do not substitute an outer WAL frame's end position or COMMIT transaction-end LSN for that BEGIN field.
The PG16 and PG18 protocol references distinguish these fields: [PG16 message formats](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html), [PG18 message formats](https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html).

Freeze `checkpoint_id` as `<generation>:<barrier_epoch>:<barrier_counter>`, with canonical decimal components from the same logged row and exact work identity bound separately.
Generation is positive signed int64 and is 1 in this profile; epoch is unsigned u64 and may be zero, while counter is positive u64.
Preserve the existing transition: new epoch is at least stored epoch and new counter is exactly stored counter plus one, with checked overflow refusal.
This is a durable producer CAS invariant, not a requirement that a parser observe every intervening report.
A consumer may skip report tokens when polling; the current token must match its trusted expected evidence exactly.
The durable pair must never identify different checkpoint/drain contents; continue it across restart without a new counter-reset feature.
Unknown COMMIT requires fence reacquisition, same-row readback, and a positive synchronous barrier before proof.
The runtime must order the witness behind all through-E transactions, positively prove those transactions durable, and synchronously commit the same-row witness/token before emitting drained.
Later-BEGIN evidence never constructs feedback or raises reconnect authority.
M1 freezes these invariants and pure evidence fixtures; M2/M4 implement them, and M6 independently reads the exact owned target row.
Extend Go `Binding` with `Checkpoint *CheckpointEvidence` and `Resources []ResourceObligation`.
`CheckpointEvidence` has string fields `CheckpointID`, `CertifiedLSN`, `DurableReplayLSN`, `DrainEndLSN`, `DrainReplayLSN`, and `DrainKind`, plus boolean `BarrierConfirmed`.
`ResourceObligation` has string fields `Kind` and `Name`, plus booleans `Owned` and `Retain`.
These values come from caller-owned evidence, never the report being validated; committed drain requires present expected evidence with `BarrierConfirmed=true` and exact matching snapshot fields.
M1 tests supply explicit synthetic authority bindings; M6 constructs them only after the independent owned-row checks.

Cleanup must contain every caller-declared resource obligation, with exact kind/name/ownership binding and no extra or duplicate identities.
Complete cleanup requires owned slot/publication obligations reported removed, including explicit treatment of an already absent owned object.
The owned workdir is retained, so its `removed` is false; a recorded origin is unowned and unremoved.
An empty/truncated inventory cannot prove completion when obligations exist.
Keep `--origin` accepted/recorded without creating or owning an origin.

## Ownership and Tasks

The leader freezes shared signatures and the fixture manifest before parallel source edits.
Workers report adjacent changes to the leader instead of expanding ownership.
The leader approved this plan and dispatched `m1_engine`, `m1_operator`, and `m1_fixtures` for Tasks 1, 2, and 3 respectively.

### Task 1: Engine DTO, discovery, and parse-only oracle

Owner files: `src/protocol.rs`, `src/cli.rs`, `src/config.rs`, `src/contracts.rs`, and `src/lib.rs`.
Use `Invocation::Version { json: bool }` and `render_version(json: bool) -> String`; preserve existing callers and plain output.
Expose no parse-only production flag: use meaningful tests of existing argument/settings validation before unsupported runtime dispatch.
Use Go's existing validator as the authoritative operational-report parser; do not create a large duplicate Rust parser solely for fixtures or pretend synthetic bindings provide runtime authority.

- [ ] Add failing tests for identity-free `version --json`, unchanged plain version, malformed syntax, capability emptiness, safe native diagnostics, shared argv/environment cases, and all-or-none identity configuration with explicit operational refusal before I/O.
- [ ] Run `cargo test --locked --lib cli::` and the new `cargo test --locked --lib protocol::` filter, confirming the selected tests fail for the intended missing behavior and discovery is nonempty.
- [ ] Implement the minimal DTO/discovery changes and meaningful local fixture checks, reusing existing parser/settings and serde facilities.
  Escalate an actual shared-oracle gap through the leader; do not introduce unreachable migration machinery or lint suppressions.
- [ ] Rerun these focused tests and report their actual counts and exit statuses.

### Task 2: Operator validator amendments

Owner files in the isolated operator worktree: `internal/native/contract.go` and `internal/native/contract_test.go` only.
Produce `Version`/`ParseVersion`, the revised drain-kind and expected-authority binding, caller-declared cleanup obligations, and the canonical identity checks described above.
Keep `UnsupportedEngine` and all production controller/Job/rendering paths unchanged.

- [ ] Add table tests for version parsing, valid later-BEGIN proof with lower durable authority, forged frontier promotion, wrong persisted witness/token/end/work, cleanup omissions, and compare rejection by `ParseReport`.
- [ ] Implement the bounded pure-validator amendments with existing strict JSON helpers and standard-library types.
- [ ] Format with `gofmt`; report focused test cases for the once-at-close Go gate rather than rerunning functional suites after each task.
  The leader has supplied explicit disposable-database closing commands below.

### Task 3: Shared fixtures, contract documentation, and inventory checks

Owner files: engine `tests/fixtures/native/v1/**`, `tests/test_native_fixtures.py`, `tools/ci/verify.py`, `docs/native-protocol.md`, and `docs/operator-compatibility.md`; operator `internal/native/testdata/v1/**` and `internal/native/fixtures_test.go` in its isolated worktree.
Reuse `tools/ci/unit.py`'s existing portable suite discovery by registering the substantive fixture-inventory suite in `verify.py`; add no forwarding wrapper.
Each repository reads only its own fixture directory during unit execution.

- [ ] Create identical `manifest.json`, `contract.json`, `cases.json`, and payload files in both fixture directories.
  Manifest protocol is 1 and its sorted file inventory contains SHA-256 digests of every payload/contract/case file, excluding itself; reject missing/extra files, duplicate paths, empty cases, and changed bytes.
- [ ] Include positive discovery, argv/env precedence, copying/progress, endpos acknowledgment, keepalive drain, later-BEGIN drain, compare-array/progress, and cleanup cases.
  Include a later-BEGIN case with applied `0/10`, certified `0/10`, anchor `0/8`, E `0/20`, W `0/30`, and durable replay `0/10`.
- [ ] Include every strict JSON/counter/identity regression already covered by the operator validator and the reviewed drain/cleanup negatives.
  Distinguish pure DTO acceptance from future producer-ordering tests; mark no runtime capability qualified by a fixture.
- [ ] Wire engine contract tests and the operator's `fixtures_test.go` to the same case manifest, comparing expected outcomes without accessing the other checkout.
  Preserve compare arrays and progress keys from the existing operator consumer; do not translate them into native Report verification payloads.
- [ ] Run `python3 -W error tools/ci/unit.py` for the new portable inventory tests, verify substantive missing/digest-drift negatives, and document exact constants, scope, and later-milestone obligations.

### Task 4: Closing Verification and Independent Review

The integration worker consumes owner diffs and focused results; any source repairs return to the file owner.

- [ ] Run `task check lint test` once on the integrated engine tree and retain the actual failures and counts.
  If retained scaffold clippy remains red, keep M0/M1 open and uncommitted under the approved sequencing decision; do not suppress it to claim completion.
- [ ] Compare the two entire fixture directories with `diff -ru`, using the actual engine and isolated operator paths above.
  Unit-runtime independence and digest-manifest checks remain separate from this cross-repository parity gate.
- [ ] In the isolated operator worktree, run `task lint` once at close.
  Use `psql` major 18 in PATH and a disposable PG16 connection for `PGCOPYDB_TEST_PGURI task test`, then run the PG16 E2E helper suite with `go test ./test/e2e -skip '^TestE2E$' -count=1 -race -v`.
  Against disposable PG18, run `go test ./test/e2e -skip '^TestE2E$' -run 'Coverage|StampedSchemas' -count=1 -race -v`.
  Supply the connection URI privately through `PGCOPYDB_TEST_PGURI`, retain only sanitized results, and never set `E2E_CONTEXT` or run the real-cluster `TestE2E`.
  These once-per-milestone functional commands are explicitly assigned by the leader; do not repeat Go suites per task.
- [ ] Obtain one independent xhigh review of identity binding, same-row drain evidence, token restart behavior, capability truthfulness, fixture parity, and unmodified operator runtime boundaries.
  Runtime WAL/COMMIT/fence/schema tests remain assigned to M2/M4 rather than being claimed by pure M1 fixtures.
- [ ] Update the ledger with exact evidence and unresolved gate failures; request leader review before any commit.

## Planning Review

The leader accepted the identity/layout/image constants, immutable generation-1 lifecycle, and minimal report surface for this plan.
Independent xhigh analysis accepted the same-row drain split and durable token rules; no new authority table or later-BEGIN restriction is introduced.
The `Binding` field types above freeze the owner handoff; any signature change returns to the leader before another worker consumes it.
This plan implements M1 only and preserves the user-approved M0-open sequencing boundary.
