# User corrections

## Baseline and scope

Use `origin/v0.18-fixes` at `86bf908adca02d87063849dae775a02c1659c829` as the compatibility baseline.
Do not substitute an older upstream release or repeat the completed issue audit.
Name the executable `rpgmigrate`.
Never import, resume, restart, convert, or clean pgcopydb state.
Full CLI and operator parity remain objectives after the first working slice.
Never report a scaffold or parser as completed migration functionality.

## Durability and performance

Document both replay-safe acceleration and the explicitly requested loss-permitting mode.
Keep `durable` as the default.
Require both explicit finite transaction and byte caps for `relaxed`.
Reserve risk durably using D/H/epoch/count/bytes across the whole migration run, including all databases and standalone phases.
Use positive target COMMIT evidence before ACK and logged synchronous writing barriers before releasing risk capacity.
Every normal success proves complete durable target work.
A recovery gap requires nonzero refusal and a fresh migration.
Prepared execution and pipelines already exist in the C baseline.
Measure applied target work before claiming improvement.
Distinguish process-kill evidence from power-loss durability evidence.
Verify macOS `fsync` and `F_FULLFSYNC` requirements before comparing journal barriers.
Match SQLite `fullfsync` and `checkpoint_fullfsync` settings to those candidate barriers.

## Planning and delegation

Deliver a reviewable first milestone early so independent workers can start.
Fix exact interfaces, access rules, tests, and file ownership before parallel edits.
Coordinate changes to frozen interfaces with the leader.
Keep later feature maps concise and expand their tasks before implementation.
Use ordinary branches, never worktrees.
Follow the current per-file assignment and transfer ownership only through the leader.
The integration worker owns task-status updates after the documentation handoff.
Commit only after the leader requests it and required checks pass.

## First COPY slice

Separate known pre-COMMIT failure from unknown COMMIT outcome.
Never describe an unknown outcome as proof of rollback.
Check terminal COPY results on both ends.
Reject source/target RLS, user triggers, unsupported shapes, and non-UTF8 servers in E1 before target mutation.
Pin text COPY session settings and test opposing date/interval defaults.
Prove exact schema/table inventory, counts, and full contents, preserving duplicates, NULLs, empty values, and binary bytes.
Negative controls must detect omitted tables, changed schemas, and same-count value changes.
Use the actual harness: `python3 tools/test-db.py -- cargo test --test copy_integration -- --test-threads=1`.
Use `python3 tools/test-db.py --self-check` for its negative controls.
Do not invent parallel `tests/native_state.py` or `tests/libpq_copy.py` suites.
Keep the frozen absolute `IoControl` deadline for E1.
Its 300-second total command limit is a temporary slice restriction pending configurable operation/stall deadlines.
Do not silently reset the frozen absolute deadline between operations.
Treat the 8 MiB COPY cap as a forwarding/write budget, not a strict process-RSS bound.
`PQgetCopyData` allocates a complete row, including the harness's 9 MiB row.
Future memory limits must account for libpq buffers, socket buffers, and the largest permitted row or frame.
Validate native state before database side effects and retain its directory lock through COPY.
State permissions and advisory locks do not protect against arbitrary noncooperating mutation by the same user.
Require exclusive control of the active private directory and avoid claims that all replacement races are prevented.
E1 refuses all prior runs, so its local completion marker cannot authorize recovery or endpoint reuse.
Keep the signal handler limited to lock-free atomic operations over process-lifetime storage.
Verify SIGINT and SIGTERM against the real executable, in addition to handler restoration tests.
Do not assume a libpq notice callback intercepts diagnostics written before connection setup.
Require the reviewed native artifact for the passfile-warning fix and reject silent stock-libpq fallback.
Do not suppress process-wide stderr to hide native library diagnostics.
Do not confuse a verified empty database with missing expected fixtures or tests.
Preserve successful empty-table and empty-catalog receipts when actual catalog queries establish that inventory.
For bounded E1, retain target locks through one synchronous transaction covering every table.
Table locks do not prevent namespace DDL such as `ALTER SCHEMA RENAME`.
Require schema and namespace DDL quiescence on both databases throughout the first COPY slice.
Target CHECK functions can change connection settings or mutate previously copied tables even when user triggers are absent.
Refuse those constraints in E1 and test the refusal against the corruption reproductions.
Defer supported foreign keys before loading and validate them before final durability checks.
Use a genuine late UNIQUE violation for rollback tests instead of an unsupported CHECK constraint.
Apply the same side-effect analysis to target expression/partial indexes, exclusion constraints, access methods, and operator classes.
An `IMMUTABLE` label does not prove that a user-defined function has no effects.
Check the complete bounded execution profile before data access instead of repairing one session setting after callbacks run.
Validate foreign-key parents, built-in comparison semantics, and `session_replication_role=origin` before copying.
An origin session alone does not ensure foreign-key or deferred-constraint checks run.
Refuse disabled and replica-only internal constraint triggers instead of enabling them or silently bypassing validation.
Match the declared Rust minimum to verified standard-library APIs and the tested toolchain, including native file locking.
Preserve valid CLI syntax while recording deliberate malformed/unsafe-input corrections.
Resolve a valid CLI conninfo override before validating the effective connection string.
Do not apply numeric/plugin pre-override validation rules to connection-string precedence.
Dump `-F` and `-N` take required values matching their long options.

## Experiment and tool contracts

Count canonical wire bytes, including each 88-byte frame header and the 128-byte completed-transaction descriptor.
Require exact positive transaction sequence order; reserve `TxSeq(0)` and allow equal, nondecreasing LSNs.
Only an external publication proof can authorize an unpublished tail; codec tolerance does not authorize pruning.
Parse compiler command overrides with the same `shlex.split` rule in production and test fixtures.
Exercise compiler overrides that include flags, such as `CC='clang -fno-common'`.
Refuse optimized Python before importing an assert-based test harness; `-O`, `-OO`, and `PYTHONOPTIMIZE` must not turn missing evidence into success.
Require recovery's minimum transaction prefix from an external caller, never from the candidate's own surviving metadata.
Expose final synchronization/checkpoint errors explicitly before timing completes; `Drop` cannot report a failed SQLite WAL checkpoint.
Keep experimental Cargo registration and automatically discovered tests out of a reviewed foundation commit until their dependencies are ready.
Preserve the pinned upstream license when generated compatibility fixtures retain upstream help prose.
Validate every required frame and descriptor before exposing a completed transaction through a scan callback.
Use bounded validation followed by emission under the documented stable single-writer scan contract; include the extra read cost in measurements.
Enable and test redb integrity checks on reopen, including cleanly closed corrupt files that would otherwise panic before returning an error.
Measure each candidate and repetition in a fresh child process because `ru_maxrss` retains earlier process peaks.
Keep configured engine cache budgets separate from RSS and label storage throughput separately from target-applied work.
Complete multi-process, reader-pin, pruning, quota, and recovery qualification before choosing a production backend.
The archived single-writer pilot does not close those gates.
Label write timing as inclusive generation, encoding, validation, batch setup, and commit work instead of claiming pure synchronization time.
Validate the exact scheduled child receipts in a fresh private root; do not add unrelated directory-inventory restrictions when enumeration is not used as evidence.
Use isolated Cargo target directories for parallel scoped checks and reserve broad lint/test execution for one integration worker after file freeze.
For native test exports, copy and verify the pinned source archive as well as the installed artifact when the suite checks upstream patches.
Separate concurrent milestones with exact-tree exports that retain their reviewed test inventories and exclude unfinished auto-discovered benchmark tests.
Track known staging diagnostics under their active owner instead of reporting them as a new component defect or weakening the final lint gate.
Check each tool's exit code before dependent ledger updates or success reports, especially when another owner can change a handoff file.
Name transport observations by their proven scope: an empty libpq output buffer can follow a discarded write and cannot prove delivery or source acknowledgment.
State whether an estimate uses elapsed agent runtime or aggregate worker-hours, together with its remaining scope and confidence.
Do not substitute human days or present an unmeasured estimate as a commitment.
Separate healthy coordination evidence from fault qualification for each storage candidate.
A candidate can remain useful for development comparisons after failing a production requirement.
Preserve the failed probe and its exact scope instead of weakening read-only roles, catching library panics, or inventing a file validator.
Do not diagnose a transient missing executable as a build race without a clean serialized reproduction check.
Promote the codec to one canonical production source only when its consumer is integrated; avoid competing identity types and unused production scaffolding.
Use narrow `#[expect(dead_code, reason = "E3 applier integration is pending")]` only on staged prepared-transport items and remove it when that consumer is wired.
For canonical plain identity values in version-two state, use the approved serde derives and reject unknown named fields instead of adding duplicate conversion types.
Keep authority proofs and trusted constructors outside that serialization shortcut.
Keep interim identity warnings visible until real consumers use them.
Shared benchmark imports can make a runtime-only dead-code expectation unfulfilled.
Carry full transaction identity with a nonempty authority frontier because equal LSNs and reused XIDs do not identify one completed transaction.
If a source plugin omits a physical commit-record position, preserve that absence in the receiver contract instead of fabricating it from the transaction-end LSN.
Do not require begin ordinal to increase in commit order.
Overlapping source transactions can commit in a different order.
Return durable proof from the normal synchronous data/checkpoint COMMIT after its logged counter increment.
Do not add a second COMMIT per group.
Use a standalone writing barrier for asynchronous work or a flush without new data, and never construct proof from a checkpoint read alone.
Poison a target transaction after any prepare or execute error, including local binding and validation failures that leave PostgreSQL in an active transaction.
A caller that catches that error must not commit earlier DML or advance the checkpoint.
Explicit rollback must remain available.
Represent an empty checkpoint's durable frontier as `None`.
A real logged barrier can prove zero CDC transactions without inventing an LSN or claiming COPY snapshot coverage.
Treat target system identity as optional where roles or PostgreSQL versions cannot expose it, reverify persisted values, and require matching receipt evidence for restricted recovery.
Do not equate a preprovisioned metadata object's owner with the migration role.
Validate the recorded owner and grant only the required schema/table privileges without ownership membership.
Match a role's literal catalog name without reparsing it as an SQL identifier.
Mixed-case, quoted, and numeric role names must retain their identity.
Fresh object creation requires the existing rights needed for the assigned owner.
Do not alter role membership or elevate privileges.

## Licensing and readiness

The user's project license answer is pending.
Use `publish = false` and omit a new project license until that answer arrives.
Preserve third-party notices.
Direct crate metadata checks do not replace a locked dependency graph audit.
Native libpq and target bookkeeping permissions are explicitly approved.
Do not introduce another approval gate for those decisions.
