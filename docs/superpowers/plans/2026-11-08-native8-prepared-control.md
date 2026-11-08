# Native8 prepared-run and callback control proposal

## Decision and boundary

This proposal completes the test interface for approved R2 `36949a5c` and V2 `ad4f1eb5`.
It is not an implementation release.
Both authors are paused until Parent approves one complete addendum and both receive its identical bytes.
Preserve V1, V2, the standalone Rust/Python freezes and all negative evidence.
The twelve relay fields, six startup fields, production APIs, planned 20/9/10 application routes and existing PG lease rules do not change.
Strict lint remains red; Native7 remains unqualified and no PG lease is granted here.

We keep container and cluster ownership in the private outer controller.
The profile consumes prepared endpoint facts and owns only its local child, callback service, relay and capture resources.
The Rust context grants no container, cluster or cleanup authority.

## Actual reuse and the missing parts

Frozen `authority_fault.py:143-184` owns a long-lived `ProtocolFault` and handles closed arm/status/release/quit requests over stdin/stdout.
`checkpoint_live_tests.rs:1934-1980` owns that helper process and its pipes.
`protocol_fault.py:119` exposes the six startup observations, and its existing arm, held-COMMIT, release and join behavior remains unchanged.
These are the fault semantics and process-lifetime patterns to reuse.

There is no callback-to-parent control server in those helpers.
A separately spawned `--capture` child cannot access the relay object while its parent blocks in `run_command`.
We propose one Native8-only Unix socket service, with one request-basename message and one response-hash acknowledgment.
It invokes the existing relay methods and the profile's specific validators; it is not a general RPC service.
Reusing the old Rust-owned stdio helper directly would require a different callback ownership path and would not solve the pre-child relay-alias declaration.

Existing private host drivers demonstrate pinned-image creation, owned command execution, evidence export and removal.
They do not provide an already implemented cross-image Native8 paired-controller API.
The new private driver must explicitly compose two existing `OwnedCluster` holders under the approved socket-mount topology.
Do not claim that this composition is already tested.

## Owners and write sets

Root owns the private `host-driver.py` and `endpoint-holder.py` beside the future accepted execution packet, plus their bounded lifecycle controls.
The holder imports existing `OwnedCluster`; it does not copy its startup, readiness or cleanup implementation.
Root owns this plan, the final contract addendum, `src/apply/checkpoint_live_tests.rs`, `tests/fixtures/schema-postdata-contract.json`, Task/CI inventories and affected docs.
Changes to `tools/test-db.py` and `tests/support/db_harness.py` remain profile selection and dispatch only.
Neither shared file gains endpoint startup, IPC, relay, capture or cleanup behavior.

The Python author retains only `tests/support/schema_postdata_fixture.py` and `tests/support/test_schema_postdata_harness.py`.
Those files implement fixed source/target fixture setup, prepared-input admission, the profile-local control service, callback client, exact oracles and tests.
The Rust author retains only `src/apply/schema_postdata_live_tests.rs`, cfg(test) hooks in `src/apply/schema_effect.rs` and `src/apply/schema_effect_observation.rs`.
No owner edits `protocol_fault.py`, generic capture helpers, production state or metadata formats.

## Prepared input

Change only the new profile's internal signature to `run_profile(command, manifest, root, *, prepared_run)`.
`prepared_run` is a required path to an exclusively created 0600 file inside the pinned 0700 attempt directory.
Dispatch passes the explicit `RPGMIGRATE_TEST_SCHEMA_POSTDATA_PREPARED` path and `RPGMIGRATE_TEST_SCHEMA_POSTDATA_PREPARED_SHA256` from the outer controller's sealed bytes; missing or mismatched input refuses before a child starts.
The normal manifest still selects the target version and existing inventory; it is not extended into a lifecycle registry.

The prepared object has exactly these eleven fields: `version`, `profile`, `owner`, `attempt`, `case`, `version_route`, `lease`, `artifacts`, `endpoints`, `paths`, `limits`.
`version` is exact integer 1 and `profile` is `SchemaPostdataReal`.
Owner and attempt are independent lowercase 32-hex identifiers; case and version route are exact V2 members.
`lease` has only `id` and `expires_unix_ms`, bound to the outer controller's accepted lease and absolute deadline.
This time bound can shorten execution; it cannot grant a lease or extend a contract cap.

`artifacts` has exactly `contract`, `source`, `runtime`, `binary`, `native_library`, `python`, `helper`, `pg_config`, `pg_dump`, `pg_restore`.
Each leaf has `path` and `sha256`; source and runtime also have their explicitly named `source_sha256` and `runtime_sha256` content identities under the frozen manifest rules.
All paths are canonical, allowlisted runtime paths and all hashes are lowercase 64-hex values.
The actual loaded library, source/runtime inventory and tool versions must match these bindings; no discovery fallback supplies a missing value.

`endpoints` has exactly `source` and `target`.
Each has `binding`, `controller_pid`, `server_pid`, `ready_sha256`.
Binding uses the exact V2 `private_endpoint` fields, including declared image/version, actual system/database/role identity and the controlled socket translation.
Controller and server PIDs are positive integers scoped to that endpoint's PID namespace; equal numbers across namespaces do not establish equality.
The outer receipt binds each endpoint's actual container, namespace, mount and ready record.
The inner profile does not accept a caller-created endpoint dictionary without that frozen outer binding and current SQL identity checks.

`paths` uses the exact seven V2 private-path names.
Runtime, Python and helper paths match the artifact bindings.
Control and capture directories exist with held owner/device/inode checks; native and scratch paths are fresh reserved children whose creation follows the existing NativeState/Scratch APIs.
`limits` is the exact V2 caps object, with no caller override.
Private prepared input, endpoint records and connection strings are never exported verbatim.

## Endpoint creation and teardown ownership

For each case, Root creates two owned containers from the route's declared images, with separate PID/network namespaces and the accepted UID, CPU, memory and disk limits.
Each runs one `endpoint-holder.py` process with the explicit source or target manifest and bindir.
The holder validates that bindir before constructing `OwnedCluster`; it never selects a version from observed server output.
Source uses `logical=True` with the declared slot allowance; target uses the ordinary durable cluster configuration.
Both use `fsync=True`, Unix sockets and no TCP listener.

The Python profile supplies fixed `prepare_source(cluster)` and `prepare_target(cluster)` functions to these holders.
They receive the actual locally owned cluster object, not serialized cleanup authority.
They create the declared database/ordinary owner, fixed source table and rows, and empty target database, then return bounded endpoint facts.
Source replication/publication privileges follow the approved logical-source profile; target gains no source or superuser capability.
Privileged observation uses the separate existing fixture administrator and never changes the tested role's grants.

The source holder remains alive through source cleanup.
For 16-to-18, Root mounts only that owned source socket directory into the target18 runner and records both path/stat observations and the actual mount relation.
Source16 server binaries run only in the source16 controller.
The target18 client bundle may observe the source through its declared socket; this does not turn its local HOME/passfile directory into ownership of the remote cluster.

Each holder emits one bounded ready record over its private pipe with exactly `status`, `side`, `owner`, `attempt`, `binding`, `controller_pid`, `server_pid`.
Status is `READY`, binding is the exact endpoint object, and the record's hash becomes `ready_sha256` in prepared input.
The only subsequent command is a bounded `stop` object with exact owner, attempt and side; pipe EOF also starts cleanup but never proves successful execution.
Failure emits only a fixed refusal code and actual process status, not a traceback or arbitrary exception text.
Root retains the actual process handles and cluster receipts, creates the prepared input only after both are ready, and launches the profile in the target container.
On teardown Root signals both holders independently, lets each call its own `OwnedCluster.close()`, reaps both, and removes only its recorded containers/mounts/directories.
A failure on one side cannot skip the other side's cleanup.
The Rust child and callback cannot send holder shutdown commands or remove containers.
Root retains both actual holder outcomes and independent absence checks; a ready hash alone cannot satisfy cleanup or endpoint ownership.

## Relay alias and local control service

The profile validates prepared input and current endpoint identities before creating local control resources.
Only the Unknown case creates `ProtocolFault(target_observer_context, "commit_hold", startup_identity=(database, owner_role))`.
Its actual fresh relay directory, socket path, UID/device/inode and port are pinned before the Rust child starts.
The ordinary target's direct socket remains the target endpoint identity.
The exact relay socket is a separate predeclared alias for only `target_initial`; no `fault-*` prefix or observed-record count can expand that allowance.
V2 `connections.target_initial` uses this actual relay connection; `target_direct` and all other target routes use the direct socket.
All fields of the final V2 Rust context are then written once, including the actual relay alias in the pre-child protocol declaration.

The profile creates `postdata-control.sock` under its pinned control directory, mode 0600, and one non-daemon serving thread before `run_command` starts the Rust child.
That thread is the only control-operation caller of the owned relay.
The existing relay forwarding thread remains owned by `ProtocolFault`.
The main profile thread may block in unchanged `run_command`; the service continues handling callbacks.
There is no engine socket, TCP listener, arbitrary command dispatch or shared helper modification.

The callback validates the held directory and socket identity before connecting.
The Linux gate requires actual `SO_PEERCRED` checks: the client verifies the serving process PID/UID, and the server verifies callback PID/UID and its actual parent Rust PID against the admitted child/binary binding.
The expected serving PID is the callback's observed grandparent: the profile directly launches the bound libtest, which directly launches the callback.
Reject a Cargo, shell or other unbound wrapper in this live command.
Because `run_command` returns its child PID only after completion, the service must first check the Rust process's live parent PID equals the profile PID, its session/group leader equals its PID, and its executable path/hash matches the prepared libtest.
Pin its process start time and check that identity again before acting; after completion require equality with the actual `run_command.child_pid` and reaped result.
Do not infer this relationship from the request's numeric PID alone.
Unsupported peer-credential behavior refuses; host pure models do not qualify this Linux mechanism.
The server derives `callback_pid` from the peer observation, not an untrusted JSON claim.

One connection carries one newline-terminated JSON object with exactly `request` and `sha256`, at most 256 bytes.
Request is the declared `postdata-<case_index>-<seq>.request.json` basename; hash binds the immutable request bytes.
The service opens that file relative to its held directory with the existing no-follow/regular-file/UID/mode/inode/cap checks, validates one byte snapshot, and derives the operation from the full V2 request.
The only operations remain `capture`, `arm_postdata`, `status_postdata`, `release_postdata`, `finish`.
It performs the phase-specific native and administrator SQL checks itself; a callback-supplied acceptance boolean cannot update control state.

The service exclusively writes the exact V2 response and returns only `seq` and `response_sha256` in its bounded acknowledgment.
The callback verifies that response and exits with the preserved result; Rust verifies its actual child PID/status and the full response binding before proceeding.
An unsafe or malformed request is omitted from retained raw evidence and produces only an admitted fixed refusal; earlier safe evidence stays retained.
No unchecked context, request, exception or socket payload is copied into a failure receipt.

## Sequence, phase and observation rules

Request sequence is a positive, consecutive callback ordinal per case, beginning at 1; responses echo it and the exact request hash.
The service serializes requests and consumes each sequence once, including refusal.
Duplicate, skipped, stale or concurrent sequence claims refuse without relay action.
The existing successful COPY check stores its exact sequence, run, anchor and intent binding inside the service; relay arm must match that accepted state.

Stage sequence is a separate increasing record ordinal in each held stage file.
ENTER and terminal records have different sequence numbers but the same declared milestone; success requires the exact ordered pairs, while a failed run may end at ENTER and stays unqualified.
Rust alone writes `postdata-<case_index>.stages.jsonl`.
The controller writes `postdata-<case_index>.controller.stages.jsonl` with the same closed stage fields, separate actor/sequence, beginning before child launch.
No two processes append to the same file.

Observation phase is `predata` or `postdata`; DO-result values map explicitly from the existing hook's `pending`, `ok`, `sql_error`, `local_error` results.
The result and SQLSTATE are null when no DO was attempted; recovery requires zero DO attempts rather than a synthetic successful DO result.
Relay facts stay null outside Unknown and retain all twelve approved fields within its applicable phases.
Only actual armed/HIT/holding/release/closure/startup observations satisfy them.
Cleanup-induced closure cannot satisfy the pre-recovery release gate.

The common callback milestones are PREDATA_EXPORT, PREDATA_CHECK, COPY_CHECK, POSTDATA_EXPORT, POSTDATA_CHECK, the three disposable guards and CAPTURE_FINAL.
Positive adds the twelve mutation/refusal/restoration milestones; rollback adds ROLLBACK_CHECK and USED_AUTHORITY_REFUSE; Unknown adds RELAY_ARM, UNKNOWN_CHECK, FENCE_CONTENDER, COMMITTED_ROW_CHECK, RELAY_RELEASE and CONSUMED_RECOVERY_REFUSE.
Their planned callback counts are 21/11/15, separate from planned application connections and from each actual observer/utility subprocess.
CAPTURE_FINAL uses `finish`; the relay milestones use their matching existing operation and other callbacks use `capture`.
The authors must reconcile the actual call graph and fixed query inventory before READY; bounded readiness/lock polls retain every attempt and never satisfy application route slots.

The separate compiled no-PG control selects exactly `early_error`, `early_panic` or `owned_path_collision` through `RPGMIGRATE_TEST_SCHEMA_POSTDATA_DIAGNOSTIC_MODE`.
It runs only through the already registered auxiliary wrapper outside the three-case prefix.

## SQL and remaining mechanical types

The metadata query must use the explicit whole row `to_jsonb(m.*)` plus the five existing `::pg_catalog.int8` overrides.
Finding chain `13e7e74a` / `47a2993c` proves the source-contract need: bare `m` can resolve to an added column and hide unknown keys.
Keep all unknown keys and NULLs visible, retain exact integer/type checks, and test malicious `m` and null-valued additions.
This is not attribution of a historical runtime failure.

V2 utility members are canonical path strings; their exact hashes and actual versions come from the prepared artifact bindings and declared route.
The fixed table is `public.n8_items`, columns `[id, k, payload]`, primary key `[id]`, secondary index `n8_items_payload_idx`, rows `[[1,10,"one"],[2,20,"two"],[3,30,"three"]]` and owner name `n8_owner` on both endpoints.
Present keys, physical columns, no-FK and final identity remain strict.
All V2 paths are strings, all declared arrays retain their exact element types, and limits equal the complete caps object.
No new persisted field, coercion, implicit default or proof constructor is introduced.

## Failure, cleanup and acceptance

The service stops admitting work on its first failure and records a fixed primary/service outcome independently of the Rust command outcome.
A service failure cannot become success because the child exits zero.
No later request repairs a failed arm, observer, proof, privacy or cleanup check.
Every SQL/utility operation retains its existing bounded timeout, and the service checks the remaining absolute case budget before each operation and before a new poll.
The existing relay hold timeout is also binding; elapsed hold time cannot be extended by the outer lease.

After child completion or termination, preserve its original status and admitted snapshots first.
Stop acceptance and further observer dispatch, finish or reap the sole outstanding bounded observer, join the service, then close/join the relay.
A blocked thread or unreaped helper is a cleanup failure, never absence.
Within the 60-second reserve, allocate at most 35 seconds to the one outstanding observer/service join, three to existing relay join, fifteen to parallel holder shutdown/reaping, and the remainder to owned path/container checks.
If this bound cannot be met in the actual implementation, stop before READY and report the concrete operation; do not increase caps silently.
Root's outer deadline still guarantees best-effort removal of its owned resources and preserves any cleanup failure separately.

Required offline controls cover missing prepared evidence, altered runtime/binary/tool paths, wrong side/version/role/socket translation, undeclared relay alias, wrong peer PID/UID/parent, duplicate/oversized/malformed IPC, lost/duplicate responses and all approved relay refusals.
Also exercise server failure during a blocked child, primary status preservation, unsafe-later-file omission with earlier safe retention, timeout/reap, EOF/shutdown, join-before-close and both-holder cleanup after either side fails.
Use actual Linux no-PG peers and compiled producer controls before any PG lease; host mocks alone do not qualify IPC or namespace identity.
Only the complete reviewed composition, actual Linux PREP and a fresh Root lease may release the nine real-utility cases.
