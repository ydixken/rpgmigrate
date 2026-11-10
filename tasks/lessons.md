# User corrections

## Baseline and scope

Use `origin/v0.18-fixes` at `86bf908adca02d87063849dae775a02c1659c829` as the compatibility baseline.
Do not substitute an older upstream release or repeat the completed issue audit.
Name the executable `rpgmigrate`.
Never import, resume, restart, convert, or clean pgcopydb state.
Derive the release command set from actual operator argv, lifecycle, consumed contracts, exposed configuration, and explicitly approved general options.
A frozen parser inventory is compatibility evidence, not an obligation to implement every upstream command.
Trace operator admission defaults through generated argv before choosing a qualification profile; an explicit alternate plugin does not qualify the default, and silent plugin substitution is not compatibility.
When the user accepts a narrower contract, update the approved spec, roadmap, and matrix while preserving existing evidence and safety guards.
Never report a scaffold or parser as completed migration functionality.

## Durability and performance

Captured source records are not target progress or ACK authority; preserve canonical capture validation without imposing an unapproved namespace-absence requirement on COPY or postdata.
Document both replay-safe acceleration and the explicitly requested loss-permitting mode.
Keep `durable` as the default.
Require both explicit finite transaction and byte caps for `relaxed`.
Reserve risk durably using D/H/epoch/count/bytes across the whole migration run, including all databases and standalone phases.
Use positive target COMMIT evidence before ACK and logged synchronous writing barriers before releasing risk capacity.
Every normal success proves complete durable target work.
A recovery gap requires nonzero refusal and a fresh migration.
Prepared execution and pipelines already exist in the C baseline.
Measure applied target work before claiming improvement.
Date measured results from retained execution timestamps and anchor unpublished-source statements to that evidence date; keep later status in the maintained tracker.
Distinguish process-kill evidence from power-loss durability evidence.
Verify macOS `fsync` and `F_FULLFSYNC` requirements before comparing journal barriers.
Match SQLite `fullfsync` and `checkpoint_fullfsync` settings to those candidate barriers.
Check PostgreSQL's existing transactional execution facilities before adding a transport protocol path; discarded client results do not imply bounded server-side memory.
Audit every admitted mutator's actual effect session before claiming a database-wide writer fence; a newer guarded path does not serialize an older raw COPY caller.

## Planning and delegation

Deliver a reviewable first milestone early so independent workers can start.
Fix exact interfaces, access rules, tests, and file ownership before parallel edits.
Verify the actual module path before assigning ownership; source metadata lives in `src/catalog.rs`, not a parallel PG catalog module.
Coordinate changes to frozen interfaces with the leader.
Only Parent allocates independent reviewers.
Authors report readiness but must not wake closed review threads or request unassigned peer reviews, even when a review skill suggests it.
Peer coordination does not authorize concurrent test work; preserve assigned ownership and quiet resource leases without adding retroactive audits or tests.
Use the accepted final field contract as the single source of truth; remove superseded brief fields instead of retaining duplicate identities, counters, or unused policy data in another layer.
Keep later feature maps concise and expand their tasks before implementation.
Use ordinary branches, never worktrees.
Follow the current per-file assignment and transfer ownership only through the leader.
Apply the canonical [subagent-effort policy](../AGENTS.md#subagent-effort) before every spawn, set effort explicitly, and verify the resulting metadata.
Distinguish global Codex policy from repository-only policy; a request for global persistence requires the effective global instruction source, with project rules linking to it.
Do not describe an inherited high-effort worker as max effort or infer effort from the visible task label.
Starting Codex through a subprocess is still an agent allocation and requires the leader's assignment; an explicit effort setting does not bypass that ownership boundary.
Label author-arranged review feedback separately from the leader's independent safety and release gates.
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

Inspect the actual harness before selecting tests under a no-network boundary; loopback TCP scripted peers also use the network.
An explicit synthetic-peer allowance covers only inspected, owned `127.0.0.1` or `::1` ephemeral listeners within the resource window; it does not permit real PostgreSQL, external DNS, remote endpoints or downloads.
Record an unplanned test scope and its owned cleanup without relabeling it as authorized; retain the evidence for the assigned reviewer.
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
Release disjoint writers after the immutable export exists; commit its frozen blobs without replacing their newer working files.
Track known staging diagnostics under their active owner instead of reporting them as a new component defect or weakening the final lint gate.
Check each tool's exit code before dependent ledger updates or success reports, especially when another owner can change a handoff file.
Name transport observations by their proven scope: an empty libpq output buffer can follow a discarded write and cannot prove delivery or source acknowledgment.
State whether an estimate uses elapsed agent runtime or aggregate worker-hours, together with its remaining scope and confidence.
Do not substitute human days or present an unmeasured estimate as a commitment.
Separate healthy coordination evidence from fault qualification for each storage candidate.
Require crash recovery to append the next externally defined transaction through a fresh writer and verify all retained history; readable reopen alone is insufficient.
Set the failure latch before invoking a callback that can fail or panic, and clear it only after validated success.
A caller that catches a callback panic must not make the raw batch committable.
A candidate can remain useful for development comparisons after failing a production requirement.
Preserve the failed probe and its exact scope instead of weakening read-only roles, catching library panics, or inventing a file validator.
When replacing a task runner, test both environment and command-line overrides, explicit empty values, and command prefixes with arguments.
A quoted executable path test alone does not verify argument handling or override precedence.
Register every new regression module in a required default gate and test refusal when that module is missing.
Require process-level qualification receipts in the default gate as well; a manual driver command does not protect CI against missing cases or continuations.
Treat a missing-method compiler failure as API evidence, not a behavioral regression result; use runtime negative controls before claiming the behavior is protected.
Live acceptance checks do not replace unit tests for malformed artifacts or missing evidence.
Build and inspect an actual runtime archive before treating fixture-only packaging checks as qualification.
Seed a slim image's actual default trust-store path before its first HTTPS package request; the presence of a CA bundle at another path is insufficient.
Normalize permissions only on owned staged license copies, preserving source files and strict final artifact checks.
Use the same architecture-qualified package field in inventory producers and consumers; never strip suffixes or weaken version comparison to hide a mismatch.
Satisfy earlier prerequisites in a refusal test and assert its specific intended error, so a different failure cannot produce a false pass.
Treat an image revision label as caller metadata; bind actual source-file hashes to a frozen tree or independently checked workflow commit.
Do not infer commit membership by comparing two copies of the same unverified label.
Drive image smoke fixtures through bounded host commands on an owned private network; reuse the existing oracles without adding an RPC controller or mounting the engine socket.
Freeze a handoff only after all owner edits stop, and invalidate its hashes before a self-review fix resumes.
Check each container engine's actual resource identifiers and inspection fields instead of assuming Docker and Podman return the same shape.
Record an owned resource before subsequent validation can fail, so cleanup can still identify it.
Do not diagnose a transient missing executable as a build race without a clean serialized reproduction check.
Promote the codec to one canonical production source only when its consumer is integrated; avoid competing identity types and unused production scaffolding.
Use narrow `#[expect(dead_code, reason = "E3 applier integration is pending")]` only on staged prepared-transport items and remove it when that consumer is wired.
For canonical plain identity values in version-two state, use the approved serde derives and reject unknown named fields instead of adding duplicate conversion types.
Keep authority proofs and trusted constructors outside that serialization shortcut.
Keep interim identity warnings visible until real consumers use them.
Trace warning targets through Cargo, the library, main and actual callers before proposing an architectural change.
For Native8, the approved private library and thin main contain genuine unwired consumers; complete their coordinator integration and real unused-import cleanup instead of moving code, widening visibility or suppressing warnings.
Keep the exact baseline delta unknown unless comparable retained checks establish it.
Distinguish a required lifecycle invariant from a reviewed correction artifact; verify the actual reader-release/publication/reopen path and never invent a missing patch or inherit unrelated review.
Match fault evidence to the claimed path: a local error followed by connection-close rollback does not qualify a backend SQL-error path.
Arm commit-loss controls only after unrelated phase commits, and keep freshness-consuming negative checks off the authority reserved for actual recovery.
Shared benchmark imports can make a runtime-only dead-code expectation unfulfilled.
Let a cached DML plan own its derived SQL, parameter types, index maps, keys, and operation shape instead of borrowing full relation metadata.
Borrow row values only during binding, so metadata eviction does not require a self-referential cache.
Carry full transaction identity with a nonempty authority frontier because equal LSNs and reused XIDs do not identify one completed transaction.
If a source plugin omits a physical commit-record position, preserve that absence in the receiver contract instead of fabricating it from the transaction-end LSN.
Keep decoder relation suppression to one last-key value per transaction, reset at BEGIN, completion, and failure.
Skip consecutive repeated keys and emit context on a key change; do not retain an unbounded seen-set or repeat full metadata for every row.
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
Distinguish proposed persisted fields from fields that the existing state constructor validates.
Read the canonical identity definitions before binding provenance, and keep stream generation separate from relation generation even when fixtures use similar values.
Acquiring a SHARE ROW EXCLUSIVE lock does not prove complete CDC DML authorization.
Check effective table DELETE, INSERT and UPDATE for every replicated column, and SELECT for every predicate key.
Use restricted-role table, column, and inherited-grant controls; owner-only fixtures do not prove these checks.
Compile and discover the exact live inventory from an immutable export before allocating its PostgreSQL fixture.
Include test-only API dependencies in that export, not only the changed production module.
Check task filters against every required profile inventory; a shared namespace can contain both an existing engine test and a separate new profile.
Use a distinct Cargo target for every source variant and mutation control, and require the intended runtime failure rather than any nonzero exit.
Pass `--manifest-path` with the absolute private `Cargo.toml` on every Cargo invocation in a nested private export, and set its private working directory and distinct target explicitly.
Do not rely on the working directory alone: Cargo can discover the primary repository's manifest from a packet directory.
Exclude interrupted attempts and binaries with ambiguous source binding from test counts; retain their evidence and bind the corrected manifest and target before verification.
Bind the frozen source and executed test binary hashes in the same verification receipt.
Do not infer that binding from timestamps, a later source hash, or an unrecorded author binary.
After draining CREATE results, check both native connection health and transaction Idle before returning a positive creation receipt.
A local idle phase or NULL result alone does not prove either property.
Exercise helper startup, fault injection, shutdown receipts, and actual worker exit; frame-transformation tests alone do not cover controller teardown.
Signal cancellation before closing sockets and joining a worker that can wait for another packet, while preserving any required client-EOF observation first.
Join a concurrent reader before closing the socket descriptor it can still use; a stop flag does not protect a reader already past its loop guard.
Do not call a race reproduction deterministic when its witness observes only shutdown EOF rather than the descriptor close that triggers the race.
Observe required backend closure before object Drop, relay quit, or cleanup can supply the missing side effect.
Retain actual wire observations and parent-wait results before fixture cleanup on success and failure; keep partial evidence unqualified and never reconstruct missing observations.
Copy container evidence before deleting the container, report capture failures, and preserve an already nonzero command exit.
After a second harness rejection, split semantic validation from evidence retention and outcome preservation, then independently review the composed boundary before PG execution.
Offline builds may overlap review of the exact frozen candidate, but neither a build nor preparation clears unresolved safety findings.
Validate complete unfiltered discovery before excluding exact registered names; substring filters can hide an unexpected case before validation.
Preserve the primary child, discovery, or startup outcome independently of capture and both cleanup attempts; retain final termination output without duplicating earlier bytes or replacing the original timeout and PID.
Open receipt leaves with `O_NONBLOCK | O_NOFOLLOW`, then require a regular file from the opened descriptor before reading.
A path check before a blocking open cannot prevent a replaced FIFO from hanging capture and cleanup.
Owned paths do not make file contents safe to export; keep per-file privacy admission before retention, preserve earlier admitted evidence on later failure, and omit unsafe bytes without an unchecked fallback.
Trace the selected command through dispatch and cleanup before propagating a shared fix; a profile-specific early return can bypass that helper and leave a separate capture boundary unchanged.
Before declaring a build-context exclusion defect, use the actual engine or prove the complete matching semantics, including parent-directory negations; withdraw a mock-based finding when engine evidence contradicts it.
Use a bounded no-writer FIFO regression; keep this artifact boundary separate from State and store checks that must inspect live database inodes without opening them.
Check actual PostgreSQL JSON types before writing fixture mocks; cast OIDs explicitly when the consumer requires an integer rather than loosening the consumer's type check.
Preserve unknown keys and NULLs when normalizing known SQL fields, and check that a whole-row reference cannot resolve to an added column instead.
Keep a source-derived contract defect separate from an unobserved runtime case or conjunct.
Trace intermediate SQL casts, the final result type, and the actual decoder before stating an OID contract; preserve the existing output and guards in a spelling-only type fix.
Check exact nested JSON keys and integer types before identity equality; Python treats some floats and booleans as equal to integers.
Use the canonical unsigned widths and the consuming envelope's refinements without inventing a narrower identity range.
Select exact SQLSTATE expectations from the independently declared and validated PostgreSQL version; do not accept multiple codes or I/O errors to hide a version-specific fixture defect.
Record the library selected by the executable's load command before and after native verification runs; copied-prefix hashes alone do not bind an absolute macOS install-name path.
Write coordination updates with normal spaces and short complete sentences.
Lead with the observed result, state the scope and limits, then give one exact receipt path.
When the leader cannot read local files, relay the requested bounded contract body directly instead of sending only its path.
Verify the frozen hash before relaying it, and keep multipart bodies in order when the message transport imposes a size limit.
Before parallel authors implement a shared harness, freeze its complete input, process, IPC, path and cleanup contract together.
If both authors encounter missing lifecycle glue, pause dependent work and resolve one shared addendum instead of issuing piecemeal fields or allowing parallel inventions.
Derive planned connection counts from actual constructors, including internal probes; captured records do not define the expected inventory.
Match scripted peers to the actual query transport and enclosing close path; a compile or peer-framing error is not the intended behavioral RED.
Use the requested reasoning-effort value in the actual worker dispatch and record that value in the handoff.
An effort label in a brief does not change the spawned worker's configuration; do not present a differently configured run as equivalent.
Keep libpq's full protocol value separate from the startup wire word.
For protocol 3.0 and 3.2, `PQfullProtocolVersion` returns `30000` and `30002`, while the startup words are `196608` and `196610`.
Name and test each representation independently; a qualification observation does not authorize a runtime clamp or new protocol support.
Finish a coherent published PR independently of newer local work.
Inspect its exact head, checks, review threads, and promised scope, then complete its readiness step instead of leaving it in draft because unrelated WIP continues.
Ready for review does not mean approved to merge or that the broader product is complete.
Verify externally reported GitHub changes before any mutation or retry, and stop duplicate operations once the user or another owner has completed them.
Attribute a manual merge to its actual actor and bind later work to the observed commit and tree, preserving local WIP and index bytes.
Apply standing user authorization to routine follow-up work while retaining required quality gates; failed checks are a technical hold, not a reason to repeat an already answered permission question.
Keep full hashes and inverse details in receipts unless resolving a concrete identity dispute.
Keep a failure-closure observation inside the helper's remaining lifetime and require an actual relay-state witness.
A live helper process or a backend that disappears after cleanup does not prove that the operation closed its connection.
Retain an unlocated intermittent failure when an unchanged rerun passes; expose its failing callsite before assigning a cause or claiming a fix.
Prove that a negative fixture completed its setup before accepting its refusal result.
A missing lock privilege or invalid CREATE statement cannot qualify the intended admission boundary.
Observe stored database locale facts before treating a requested locale name as a negative fixture; PostgreSQL can normalize POSIX to C.
Map every caller of a shared assertion before attributing its message to one phase, and retain controlled prerequisite facts without exposing connection data.
For an exception with no message, retain its class and source basename, function, and line without locals or connection data; preserve the original exit behavior.
Diagnostic syntax and a single marker do not prove producer identity.
Use the approved private metadata channel for the actual command, and never promote opaque stdout or stderr into public diagnostic context.
Exercise uncaught exceptions and forced exits through the real consumer; broader catch coverage or an extra marker does not establish provenance.
Preserve failure behavior when trusted metadata is missing or rejected.
Deliver an oversized negative fixture to the boundary under test without exceeding the process argument limit first.
An execution failure before the child starts is fixture setup failure, not evidence that the metadata reader rejected the payload.
Preserve the assertion and use bounded fixture transport, then verify the relevant process behavior on the CI platform and Python version.
Keep large fixture bytes in private input files and prove that the child received the exact payload with the intended exit.
A macOS pass does not qualify Linux's per-argument limit; retain each platform's execution evidence separately.
When composing reviewed helpers, update direct test callers to the combined startup boundary while preserving their original assertions and deadlines.
Declare every expected fixture role before the child runs, including quoted names and names derived from an independently queried owner OID.
Do not admit arbitrary numeric users or derive expected identities from the observations being validated.
A static registry of all accepted phase names is not a qualification inventory.
Select the exact phase set from the declared version pair, and require the same complete set in capture, helper-reaping, and CI receipt checks.
Check current file ownership before starting an adjacent fix, even when its scope is small.
On reassignment, stop edits and hand over retained RED evidence; keep candidates based on different source snapshots separate.
Name the artifact for each digest; a capture record, capture helper, native inventory, and native library are different inputs.
Store file counts and SHA-256 digests in separate fields; never prepend a source count to a digest or describe callback phases as typed phase proofs.
Keep the active protocol contract distinct from a descriptive amendment; preserve frozen receipts and add a binding correction instead of silently changing their meaning.
When checked close returns a live handle, return its owning store with the error and retain the role or pin needed for retry.
Checkpoint failure must preserve that ownership before close; best-effort Drop does not prove successful checked shutdown.
Use the actual returned path when reporting a generated artifact directory; never guess a temporary-directory suffix before reading the command result.
Read the actual retained source path from its handoff or directory inventory before integration.
Do not give a remaining-alpha estimate before the owning COPY cases and the first production runtime crash/restart evidence establish a defensible critical path.
Distinguish elapsed agent time from aggregate worker-hours, state the remaining scope, and preserve uncertainty.
Check the mutation command's exit code and resulting file hash before updating a completion checkbox or sending a success message; a tool call that returns normally can still report a failed command.
If primary already matches the reviewed blob, record that observed identity without claiming a source replacement.
Determine checkpoint inclusion by comparing each captured tree blob with the reviewed handoff, not by inheriting a planned integration status or a private-export location.
Include every executed runner helper in a selective export, and compare its blob with the reviewed implementation before verification.
Test actual suite execution and its nonempty receipt, not only the command-line arguments sent by a task.
Validate receipts from the harness's declared output stream; compiler diagnostics on stderr can contain names that resemble receipts.
Retain and hash stderr separately, and reject a nonzero command even when stdout contains an apparently valid receipt.
After a runner correction, execute a fresh gate instead of promoting the earlier failed report from a retained-output replay.
Keep newly proposed milestone work separate from prerequisite interfaces supplied by the previous milestone; do not create a circular dependency by assigning the new work backward.
Derive stage and case inventories from the exact selected source and its nested task definitions, not from an older public snapshot's totals.
Adding a version must preserve every required private gate; test omission of each new required stage instead of silently reducing the inventory.
Compare the failing export with the primary file before changing code; an omitted working helper requires an export correction, not a new parser.
Set `RPGMIGRATE_BIN` to the emitted executable when a verification run uses an isolated `CARGO_TARGET_DIR`.
A missing executable caused by the gate configuration is not a source regression.
Identify the executable that actually opens PostgreSQL before checking native linkage.
A thin test wrapper can launch the real CLI without linking libpq itself; bind that child executable and its loaded library explicitly.
Separate dependency compilation, native linkage, direct adapter tests, and the runtime routes exercised by smoke tests; a built artifact does not prove an unused API was called.
Express exclusive draft creation with a non-cloneable draft that retains a mutable borrow of its owner before adding a registry or interior locks.
Keep immutable execution pins separate from that exclusive creation borrow.
Publish immutable directories with a real directory-relative no-replace primitive; plain rename can replace an existing empty directory.
Fail closed on unsupported platforms instead of substituting an existence check followed by rename.
Insert an empty final directory at the last publication hook, require `EEXIST` with its inode unchanged, and prove that a plain-rename mutation fails the control.
Read both the canonical type and the consuming envelope's qualified invariants before changing a value domain.
A generic `SourceEndLsn` can parse zero while a confirmed source-snapshot receipt requires a nonzero point.
Preserve that refinement in COPY anchor bindings, and test zero-point refusal without intent, proof, or DML authorization.
Do not weaken a qualified envelope because its underlying scalar accepts more values.
Keep independently released format changes separate in their author packages.
Use a new preserved source copy for a required version overlay, and defer a shared primary-file change until its owner hands the file back.
Old regressions that leave new optional SQL fields NULL do not qualify their populated storage path.
Exercise actual installation, parameter binding, readback, preservation, and malformed-present refusal before claiming a new metadata field is qualified.
Keep the earlier regression receipt scoped to what it executed instead of relabeling it after a coverage gap is found.
Run the test-target lints when new tests use platform-dependent native constants.
A conversion that is necessary on Linux can be redundant on macOS; preserve type safety without disabling the lint or its test.
Check a custom test runner's argument handling before assuming standard unittest flags; invocation errors do not count as behavioral RED evidence.
Explicitly unlock a raw file-lock competitor before handing ownership to the component under test when another descriptor can still refer to the same open file description.
Retain that duplicate through the handoff so removing the unlock causes a meaningful regression; preserve production lock-release behavior and contention assertions.
A controlled reproduction of a test assumption does not identify a withheld hosted panic; retain both evidence scopes and require fresh hosted checks on the reviewed fix.
Include pinned source archives and dependency-cache requirements when preparing an exact verification export.
If a missing cache stops compilation or a native-source test, retain that preparation failure and correct the inputs without treating it as a runtime defect or rebuilding unchanged native artifacts.
Frozen evidence modes are not necessarily usable working-cache modes.
When copying a pinned cache into a verification export, verify its bytes and restore only the copy's required lock-file write and executable permissions; preserve the frozen donor and source files.
Prepare an owned Cargo cache from the actual image or approved existing cache before testing its availability; do not invent a preexistence requirement for a directory that the tool normally creates.
Keep optional inspection-script failures separate from native verification, compilation, and runtime failures.
Verify the actual Cargo release profile instead of requiring a redundant literal compiler flag; missing or enabled debug assertions must still refuse qualification.
Retain built artifacts and their identities before optional metadata checks or cleanup, so a reporting assumption cannot discard successful compilation evidence.
An owning handle returned after failed admission is cleanup authority, not permission to read or append data.
Keep its role and pin for checked close while centrally refusing operations that require successful admission.
Read the actual bound-intent phase before summarizing refusal as absence.
Target refusal preserves an already bound `Some(intent)`; publisher refusal alone requires present-null `copy_snapshot` together with the existing native error.
Require `copy_complete` to be present and null in both cases; a missing required-nullable field is not proof of absence.
When a native format denotes a restricted phase profile, enforce its authority refusal even when that phase's optional intent is null; a null field must not open a CDC or frontier path before aggregate authority exists.
Make a new restricted format an explicit internal opt-in; preserve existing creation defaults and retain the opened flavor across all later manifest writes and metadata selection.
Test namespace loss after successful initialization as well as after failed initialization.
Refusing publication proof must not prevent checked cleanup through held resources; preserve the required synchronization plan across retries.
Keep a bounded core-review PASS separate from an unresolved adapter finding, and retain each frozen candidate's evidence.
Keep a storage-interface brief separate from source iteration and COPY ownership.
Transfer only the minimum entry fields across that boundary, and send existing findings to the assigned owner instead of starting a parallel inquiry.
When splitting a plan, preserve explicit module ownership instead of transferring adjacent responsibilities by inference.

## Licensing and readiness

On 2026-11-10, the user selected Apache License 2.0 for rpgmigrate.
Use SPDX `Apache-2.0` and the unchanged official license text.
Retain `publish = false` and preserve third-party notices.
Do not infer release, tag, GHCR, benchmark, or milestone authorization from a project-license decision.
Direct crate metadata checks do not replace a locked dependency graph audit.
Native libpq and target bookkeeping permissions are explicitly approved.
Do not introduce another approval gate for those decisions.

## Workflow status and estimates

Use the latest explicit reviewer assignment and thread ID; do not assume the previous reviewer owns a new addendum.
Check each tool's actual exit status before sending a dependent PASS, lease, or mutation instruction.
Wait for cache and wrapper preparation to exit successfully before starting dependent checks; a returned session ID is still-running work, not a completed prerequisite.
When compiled producer controls are required for review, run and bind those author controls before READY; do not make their compile window depend on the review that needs their results.
Retain an early setup failure separately and verify the actual prerequisite before rerunning the unchanged candidate.
Do not let a failed assertion fall through to a queued approval message.
Initialize thread handles and mutable Events per instance before startup; preserve inherited behavior when adding passive observations.
A completion event means the function finished, not that it succeeded or that its thread and descriptors were reaped.
Pin the engine's absolute deadline once in the parent before launch, then convert its remaining budget once to monotonic I/O control; process startup and callbacks must not renew it.
Keep diagnostic observers transparent to expected readiness polling; return the original result and let the actual caller decide which failure is fatal.
After reviewing fixed execution inputs, place fresh resource checks immediately before a preauthorized bounded run so a coordination roundtrip cannot consume its execution budget.
When an authorized run has started, retain its actual input plan and record later alternatives as unused instead of interrupting it or starting a duplicate.
Before restricting an acquisition API, trace recovery after each owned artifact is consumed.
Require the admitted artifact for initial provisioning, but preserve an existing-only acquisition route for checked recovery; acquisition alone grants no phase proof or replay permission.
Distinguish existing-object reconciliation from provisioning: require a genuine pending native record, complete matching row bindings and positive durable COMMIT evidence before confirmation; refuse mismatches without repair.
Mirror the existing phase predicate when adding an early guard: preserve matching pending schema intent and target provenance, and check full artifact bindings at the effect boundary before reuse.
Do not extend an initial-entry records exclusion into later COPY or POSTDATA phases where canonical source capture is permitted.
Define immutable artifact identity by the approved content and provenance bindings, not allocation identity; distinguish binding-changing substitution from a byte-identical remint without weakening held-descriptor checks or implying lost-input restart support.
Lead progress reports with the runnable production/operator workflow and the features still missing.
Label component test evidence by its actual scope and keep it secondary to workflow qualification.
Distinguish aggregate agent-hours from elapsed time and state assumptions and uncertainty; estimates are not commitments.
Deferring 40 command paths does not imply an 80 percent reduction in engineering work, because the selected paths still require the migration engine and its safety gates.
Checkpoint a coherent verified subset explicitly when broader WIP is unqualified; never describe it as saving or qualifying all local work.

## Corrections on 2026-11-08

Trust `AGENTS.md`, the approved design and this file; do not use the removed task ledger or dated plans as inputs.
The [roadmap](../docs/roadmap.md) amendments supersede conflicting entries above, including the patched-libpq requirement and the never-worktrees rule.
Git worktrees are allowed for parallel build work.
Parallelize the build phase as far as file ownership allows.
Run independent reviews and the Go test suites once per milestone, at its close, not after each task.
Write finished plans into the repository, not only into the session plan file.

Treat `docs/roadmap.md` as the primary project status overview and keep it synchronized as verified progress, blockers, and milestone closures arrive.
Update the roadmap and task ledger in the same change without waiting for a reminder; the ledger holds detailed evidence.
Tick completed subdeliverables, and leave milestone closing gates unchecked until their required checks pass.
Do not use a current task ledger as a reason to leave the roadmap stale.
Always carry accepted progress and blocker updates into both maintained trackers in the same change, including intermediate source checkpoints.

Before reusing a catalog planner on a new call path, trace its nested SQL transaction requirements, including `LOCK TABLE`, and preserve the established caller transaction boundary.
Separately passing source API checks do not qualify preliminary planning on an idle connection.

Validate costly E2E fixture DML against real PostgreSQL in a rolled-back transaction before launching the full workflow.
Preserve identity-column legality and independent primary-key mutation coverage when correcting the fixture.

Verify required bytes, modes, and explicit absences when overlaying a frozen worktree; copying present files alone can resurrect dropped code.

Honor the user's explicit capacity and concurrency updates over an older exclusive resource lease.
Verify actual VM capacity and free disk before applying a proposed parallel budget, preserve host Cargo headroom, and do not infer causality from a resize that happened after a failed run.

Measure account and server contents in the actual running tool container before changing environment assumptions.
An immutable image does not prove the contents of a mutated running container; do not override HOME to bypass that check.

Use `followup_task` to assign new work to a completed agent.
An idle `send_message` does not start its next turn.

## User-authorized maintenance and dependencies, 2026-11-09

Retain the user's approval for unused Podman image/volume/build-cache pruning, `podman-machine-default` growth, and the scoped Python 3/ctypes/libpq runner dependency exception; do not ask again for those scopes.
Preserve and test persistent PostgreSQL 18 data before stopping its original tmpfs container or restarting the VM, while retaining original service endpoints and data.
Honor the exclusive maintenance lease and prune only confirmed-unused resources under the approved plan, without blanket cleanup of retained data or evidence.
Dependency authorization supports private opaque URI Secret authentication without argv secrets; it does not qualify TLS, real PVC permissions, packaging, or production native admission.

## User-authorized feature CI and one-off cleanup, 2026-11-09

Retain the approval to push rpgmigrate `feat/native-engine` and the operator native-work feature branch to origin for hosted CI, including native AMD64 checks; do not extend it to main push/merge, tags, releases, or GHCR publication.
Treat the failed-kind cleanup exception as exactly three approved container identities, with no volumes, network, images, backups, originals, RUN-marker mutation, or generic guard bypass.
Keep creating/empty failed-RUN markers unchanged, require confirmed cleanup and the leader's resource-budget release before the held M5 build, and use fresh source/RUN after the diagnosed fix.

Origin feature-branch push approval does not authorize downstream default-main GitLab mirroring or all-ref/tag publication; inspect workflow side effects and keep unresolved publication scope separate from CI readiness.

## Existing operator GitLab mirror authorization, 2026-11-09

The user explicitly approved the existing GitLab mirroring of the operator native feature branch, resolving its publication P1 and prior authorization hold.
Keep the existing mirror enabled and retain the earlier origin-only rationale as history; do not ask again for the approved scope or infer repository settings/main edits, tags, releases, or GHCR permission.

## Standing failed-kind container cleanup authorization, 2026-11-09

On 2026-11-09, after approving the second exact failed-kind cleanup, the user authorized future removal of independently verified rpgmigrate-owned failed kind-node containers without repeated cleanup approval.
This standing authorization covers containers only: preserve volumes, networks, images, backups, original PostgreSQL containers, source, and RUN evidence; do not extend it to other resources or repair RUN markers.
Verify ownership and failed state independently before cleanup, preserve exact resource identity evidence, and do not report completion until final absence checks pass.
After repeated bootstrap-contract failures, re-plan bounded startup/DOWN metadata fixtures before another UP; preserve failed RUN evidence rather than repairing it in place.

## Owned-kind temporary metrics observer authorization, 2026-11-09

The user approved the bounded temporary metrics observer policy for owned kind E2E runs; reuse this authorization without asking again only after fresh source/RUN/namespace/manager/deadline admission.
Keep automount false, GET `/metrics` only, one exact ServiceAccount binding, one in-memory 600-second token without renewal, and the admitted owned-Pod loopback TLS tunnel.
Clean exact recorded observer UIDs/resourceVersions on every bounded exit; preserve replacements and never expand this approval to production RBAC, Secrets, new namespaces, lease renewal, or benchmark execution.

## Continue through the approved milestone gates, 2026-11-09

The user directed us to push through M10; continue approved M6–M10 execution instead of treating intermediate plans or source checkpoints as the end of the assignment.
Keep roadmap and todo synchronized, require evidence for closure, and retain the benchmark hold and main/tag/release boundaries and the dated development-only GHCR limits.
Performance ranges are inferences until measured.

## Execution admission and runtime metadata attribution, 2026-11-09

Approved milestone scope already permits owned M7 UP/load/legacy/native tests and ordinary fresh owned fault fixtures; enforce source/ownership/resource guards without inventing another blanket user-approval gate.
Keep the actual benchmark artifact/argv hold and out-of-scope shared-VM/original-resource/main/tag/release boundaries and the dated development-only GHCR limits explicit.
Attribute runtime-synthesized HOME/passwd observations to that runtime; they do not prove immutable-image metadata or a different container runtime’s defaults.

## Canonical secret-bearing temporary storage

Keep secret-bearing temporary storage in one canonical materialization policy and require builders and independent admission to consume the same output.
For the native helper/preflight profile, preserve the canonical Memory-backed 64-MiB `/tmp`; a builder-local override can silently break both credential isolation and exact admission even when its own tests pass.
Keep targeted materialization boundary checks separate from actual Kubernetes, PVC, resolver, and runtime qualification.

## Validate the exact external renderer framing

Capture the pinned external renderer’s real output before defining parser fixtures; `-o json` does not by itself promise one JSON document.
Keep framing coverage faithful to that output and retain bounded object/identity checks, then require a fresh source-bound manager/archive/RUN after a bootstrap parser change instead of reusing old acceptance.

## Keep active execution data separate from completed proof requests

Use `ExecutionData` for active/source/retry observations and reserve `ExecutionRequest` for completed-only proof requests.
Never broaden the completed validator or fabricate persisted E to make an active client fit; retain route-specific validation and the independent completion authority boundary.

## Qualify the actual container creation image path

Content presence or successful image inspection does not prove the runtime’s CreateContainer reference path.
Bind normalized CRI references through the exact node-local OCI index, selected manifest, and config chain; an actual CloudNativePG normalized-index imageID must not be admitted by silently stripping reference strings.
Keep a post-failure alias experiment separate from the failed historical gate, and require the reviewed source/import/runtime profile before claiming workflow acceptance.

## Bind composed checks to the actual input inventory

Derive the Python mechanism inventory from actual AST definitions and compare it with the explicit registry; a 54-case source cannot be qualified by stale 52-case registration.
Prefix-level checks do not qualify a changed compiled package composition: run one scoped lint on the actual joined input and preserve the earlier RED result separately from the repaired successor.
The `d28` constant-collision failure and reviewed `6d` actual scoped-lint PASS have distinct inputs; broad Go suites and independent milestone review still run once at close.
Confirm that a CLI test driver actually supports a requested selector before reporting a partial run; if it ignores the filter, record the real broader inventory once instead of repeating the broad script.

## Synchronize milestone evidence across maintained trackers, 2026-11-10

Update `docs/roadmap.md` and `tasks/todo.md` together when an accepted checkpoint changes milestone evidence or remaining gates.
Keep M0–M5 closed and M6–M10 open until their full acceptance evidence passes; fresh packaging or PREPARE/CHECK does not close native lifecycle or legacy workflow gates.
Continue approved isolated M8 work after its Task 1 interface freeze while M7 closure remains open, and keep live format 8 / metadata 6 inputs separate from candidate format 9 / metadata 7.
Retain historical failures under their original input identities and record successor results separately.

## Verify implementation isolation before granting a file lease, 2026-11-10

Before granting a source-file lease, the leader MUST receive and verify a distinct worktree path, feature branch, and exact HEAD.
The authoritative checkout MUST NOT be an implementation destination.
If owned edits reach the wrong checkout, first preserve an exact sealed copy in the isolated worktree, then reverse only the owned verified patch while preserving all other work.
Do not use `reset --hard`, `clean`, or broad restoration to recover isolation.

## Bind OCI descriptors to their actual media types, 2026-11-10

Check each descriptor media type before calling a digest a manifest.
The retained multiarch node `099e` is an index and ARM child `10210` is the archive manifest; preserve older evidence and do not invent a conversion.

## Preserve safe setup failure identity, 2026-11-10

Retain structured setup-failure identity and writer provenance before discarding raw subprocess output.
Keep credential-bearing raw output discarded, and report an unknown assertion as unknown until independent evidence identifies it.

## Preserve the latest registry approval, 2026-11-10

The user approved direct public-registry pulls by pinned digest for upstream CloudNativePG, operator, PostgreSQL, and other public dependencies.
This supersedes the local-registry proposal.
Do not continue M7 archive/containerd-import, alias, or tag-retargeting work.
The user separately approved public GHCR publication of manager/native-runner development images under the approved user namespace.
Use immutable `dev-<full-40-hex-commit>-<arm64|amd64>` tags and kind digest references.
Use existing manager `ghcr.io/ydixken/pgcopydb-operator`.
Use exactly `ghcr.io/ydixken/rpgmigrate` for the native package.
Do not infer version-tag, `latest`, release, or main-push approval.
Preserve historical origin/mirror authorization limits as provenance and apply this later approval only to its explicit development scope.
Keep exact ownership, original clock, caps, and cleanup guards.
Preserve old source and RUN evidence and require fresh registry-path qualification.
The old Ready RUN held before LOAD under S1 is not registry-qualified.
Keep M10 offline candidate archive retention separate and continue M6/M8/M10 work without closing any M6–M10 gate or lifting benchmark HOLD.

## Keep development publication and runtime qualification separate, 2026-11-10

The public development-package approval covers only the exact manager and native packages; it does not authorize release tags, `latest`, main changes, or benchmarks.
Use the admitted feature-branch workflow for ordinary publication and keep the separately approved first-native bootstrap exception explicit.
Require clean source identity, no-overwrite tags, anonymous public descriptor verification, and exact owned-node pull evidence before a lock qualifies.
Keep a pending visibility receipt distinct from a qualifying publication receipt.
A hosted publication pass or offline contract pass does not prove that the Podman VM-specific Guard runs on a Linux-native host.
Record Linux-host portability as an open runtime gate.

## Run complete scoped lint on joined inputs, 2026-11-10

Run all configured checks, including `gocyclo`, on the actual joined packages.
Do not use `--new-from-rev` or a changed-line filter to turn a scoped lint gate green.
Preserve older results with their original source identity and report a repaired successor separately.

## Preserve bounded server startup evidence, 2026-11-10

Capture bounded startup state and logs, then fsync diagnostics before cleanup so an early server failure retains its actual cause.
Validate traversal permissions on the real parent and PGDATA paths, and inspect the actual Podman `.ID` rather than assuming a Docker-compatible field.
Keep the startup deadline bounded after adapter repairs and retain failed evidence separately from successor passes.

## Test absent Task revisions through the actual command, 2026-11-10

A Task global null value can reach a command as a missing revision.
If the command accepts a missing revision, exercise the actual individual-task dry run and a missing-revision negative control; do not infer that contract from template inspection.

## Retain pending publication evidence without blocking independent work, 2026-11-10

Keep the source-bound first build, smoke, push, and preservation receipts when visibility or repository linkage remains pending.
Request the unavailable browser step asynchronously and continue independent approved work without claiming anonymous pull or runtime qualification.

## Canonicalize only the approved repository identity, 2026-11-10

Map the exact approved GitHub SSH origin to its canonical HTTPS identity before deciding whether a source is qualified.
Preserve the original Git remote configuration and refuse unrelated identities; identity equivalence does not authorize a remote rewrite.
Keep credential-bearing raw snapshots out of diagnostics and retained evidence.
