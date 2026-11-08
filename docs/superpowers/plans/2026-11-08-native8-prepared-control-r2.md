# Native8 prepared-run and callback control R2

## Review boundary

This is one replacement proposal for prepared-control `b8bd92e4`, addressing all five findings in review `53b0a706`.
It preserves accepted nine-run design `36949a5c` / review `55179d7d`, the production APIs, twelve relay fields and six startup fields.
Parent selected the topology below after metadata closure `e6136171`; this proposal still requires Parent and assigned design review before author resumption.
Preserve the old proposals, V1/V2 interfaces and both standalone author HOLD packets.
Native7 has a separate selected-component PASS with synthetic source and fake utilities; it supplies no Native8 real-utility proof.
Strict lint remains red, with 508 normal-library diagnostics on the retained integration candidate; no merge, activation or Native8 PG lease follows from this plan.

## One runtime, two independently owned clusters

Use the pinned Linux arm64 PG18 runtime for every case, with the existing complete PG16 prefix mounted read-only at `/opt/e1/pg16.15`.
Keep `/opt/e1/pg18.6` in its original image path.
The [closure receipt](../../../target/verify/native8-prefix-mounted-vvjlvbku/receipt.json), `e61361714f5a2192176d6545e644fc4e6f220d23b4cc2fc217017ecd5779ab52`, binds full prefix bytes, modes, links, actual ELF dependency selections, shared files, plugins and version commands.
PG16's 1,704 inventory entries and dependency graph matched its original image inside that runtime; PG18 has 1,749 entries.
The initial host-copy failure remains retained: eight symlink modes required exact restoration in a new owned copy.
No server or SQL ran during this check, and backend extensions were not tested as standalone `dlopen` libraries.

Keep the three declared routes: 16.15 to 16.15 with utilities 16.15, 18.6 to 18.6 with utilities 18.6, and 16.15 to 18.6 with utilities 18.6.
Actual versions and tool identities must match the selected route; do not infer a route from observations.
Nine cases create nine sequential runtime containers and eighteen fresh clusters, not eighteen containers or two OS images per case.
Source and target share one Linux PID/network namespace but have distinct `OwnedCluster` objects, postmaster handles, owned roots, socket directories, tokens and system identifiers.
Verify each complete database/role/socket binding; coincident numeric OIDs or ports do not join identities.
No TCP listener, cross-VM socket translation, endpoint-holder process or paired host driver is needed.

The N8 Python profile owns both local cluster objects.
Validate the ambient target `PG_BINDIR`, construct each existing `OwnedCluster` with its declared manifest, then assign its already validated instance `bindir` before `__enter__`.
Do not swap global environment variables between endpoints or alter shared `OwnedCluster` behavior.
Use `fsync=True`, `tcp=False`, source `logical=True` and the existing declared slot allowance; target remains nonlogical.
Existing source ownership/REPLICATION/publication admission and ordinary target ownership remain distinct from administrator observations.
The fixed dataset, source catalog restrictions and preinstalled PL/pgSQL checks remain those of accepted R2.

## Prepared input and ownership

Root owns one small private outer driver, using the existing pinned-image, owned-exec, SAFE-export and container-removal pattern.
It owns the container and read-only mounts, establishes the case clock, creates the pinned private attempt directories and exclusively writes the prepared file before dispatch.
The N8 profile owns cluster startup/setup/cleanup, the Rust child, callback service, relay and admitted capture.
Rust receives checked data and phase inputs, never container or cluster cleanup authority.

The signature is `run_profile(command, manifest, root, *, prepared_run)`.
Dispatch supplies the required path from `RPGMIGRATE_TEST_SCHEMA_POSTDATA_PREPARED` and the required exact-byte SHA-256 from `RPGMIGRATE_TEST_SCHEMA_POSTDATA_PREPARED_SHA256`.
Read one no-follow, bounded 0600 regular-file snapshot through the pinned 0700 directory; validate owner, link count and before/after/name identity before parsing.
Missing hash, changed bytes, duplicate/extra keys, wrong modes or a foreign path refuse before starting either cluster.
The selected live command is only the bound libtest, the exact selected case name, `--exact`, `--ignored` and `--test-threads=1`, in that order.
The dispatcher may launch this profile, but no Cargo, shell, alternate binary or extra selector may sit between the profile and libtest.

Prepared input retains eleven exact top-level fields: `version`, `profile`, `owner`, `attempt`, `case`, `version_route`, `lease`, `artifacts`, `endpoints`, `paths`, `limits`.
Version is integer 1, profile is `SchemaPostdataReal`, and owner/attempt are separate 32-character lowercase hex tokens.
Lease contains only `id` and `expires_unix_ms`; it binds the existing Root lease and cannot extend it.
The ten artifact leaves remain contract, source, runtime, binary, native library, Python, helper, pg_config, pg_dump and pg_restore, with exact canonical paths and SHA-256 identities.
Source/runtime leaves also carry their named content-manifest identities under the existing hash rules.
The runtime manifest includes both prefix manifests, the actual PG18 image and the copied PG16 prefix provenance from the metadata receipt.

Prepared `endpoints.source` and `.target` are declarations, each with exactly `version_num`, `origin_image_id`, `bindir` and `prefix_manifest_sha256`.
They are not fabricated live endpoint facts and contain no ready record or PID.
After both clusters and role/database fixtures exist, the profile derives the full existing V2 endpoint objects from its actual handles and administrator observations, pins them, then writes the Rust context once.
Each context endpoint's `image_id` denotes the actual common PG18 execution image; the prepared prefix origin separately identifies the image that supplied that version's binaries.
Both socket `controller_path` and `runner_path` are the same absolute local directory and must have the same checked device/inode/UID/mode.
The seven private path fields retain their V2 spelling; runtime/helper/Python paths match their artifacts, while native and scratch are fresh reserved children created through the existing owners.
Do not export prepared input, private context, connection strings, passfiles or endpoint setup logs.

## Actual relay observations

Only the Unknown case creates the existing `ProtocolFault` in ordinary `commit_hold` mode with the checked target startup database/role.
Keep `copy_observer=False`; enabling it would change the protocol lifecycle.
Declare the exact fresh relay socket before libtest starts and permit that alias only for `target_initial`; all other routes use the direct target socket.
Retain the original forty-five-second held-COMMIT timeout.

Use a small N8-local subclass solely to retain the actual upstream thread handle and a completion event around `super().upstream(...)` in `try/finally`.
It must not copy forwarding code, alter bytes, override `run`, change arm/release behavior or introduce a shared-helper edit.
Once the connection exists, pin the actual client and server socket objects from this relay instance.
Project `client_closed` from the pinned client's actual `fileno() == -1`, rather than the ordinary mode's unpopulated `client_closed` event.
Project `backend_closed` only when its existing event is set and the pinned server descriptor is closed.
Before a released response or cleanup success, join both the forwarding thread and retained upstream handle within the remaining bound and require neither alive.
An event alone is not thread-reaping evidence, and cleanup-induced closure cannot satisfy the pre-recovery release gate.

The twelve relay fields and six startup fields stay unchanged.
Before `RELAY_ARM`, relay facts are null; after arm, each Unknown callback carries actual bound observations, while other cases always use null.
Arm requires the already admitted COPY sequence, full run, POSTDATA phase, `target_initial` route, backend PID and provisioning nonce, with actual armed/HIT0 after the action.
Status requires actual HIT1/held/not released/backend alive plus independent metadata and fence observations.
Release calls the existing release method once, verifies actual descriptor/thread closure, then observes backend and writer-lock absence before allowing fresh recovery.
Store a conservative N8 hold deadline of arm time plus forty seconds, checked before subsequent operations; this cannot extend the relay's forty-five seconds after HIT.
Early/double arm, stale COPY, zero/two hits, foreign binding, missing startup, expired hold or incomplete closure fails the case without replaying an action.

## Callback IPC and cancellation

Reuse the frozen file snapshot helpers and one N8-local AF_UNIX service at `postdata-control.sock`, mode 0600 in the pinned 0700 control directory.
The profile owns one non-daemon service thread and one in-flight request, with no pool or retry protocol.
One connection sends exactly `{request, sha256}`, newline terminated and at most 256 bytes.
The fixed basename is `postdata-<case_index>-<seq>.request.json`; the service derives the operation from its admitted V2 request, not from an arbitrary command field.
It creates the exact response file exclusively and returns only `{seq, response_sha256}`, at most 256 bytes.
Missing, duplicate or lost acknowledgments fail; they never repeat arm or release.

Client connect/read/write and server read/write each use a two-second ceiling, shortened by the remaining callback and case deadlines.
The listener uses a 0.2-second accept timeout; idle accept is therefore interruptible.
The server pins each accepted socket and closes it on shutdown, so a partial line or a blocked acknowledgment cannot strand its thread.
The total callback ceiling is thirty-five seconds, including relay actions, observer calls and the acknowledgment.
Before each existing thirty-second `psql` call, require that full interval plus the bounded response write remains in the callback, case and applicable hold budgets.
Only one observer may run at a time; bounded polls check this same total deadline before every call, and all attempts are counted separately.
Stop admission and further observer dispatch immediately when shutdown begins.

Require actual Linux `SO_PEERCRED` in both directions.
The client verifies the serving profile PID/UID; the server checks the callback PID/UID and its direct parent libtest against the pinned binary, profile parent, session/group leader and `/proc` start time.
Pin and recheck the Rust process identity before any signal or relay action, then match it against `run_command.child_pid` after return.
If the service fails while the child is blocked, preserve a fixed service failure, close its peer and signal only that verified owned process group with TERM.
The unchanged `run_command` remains responsible for its actual child result and timeout escalation; do not replace that primary result with the service or cleanup exception.
Before a child identity is established, record service failure and let the bounded command/outer owner terminate it; never signal an inferred PID.
Unsupported credentials, wrong ancestry, PID reuse, an unbound executable or a missing group-stop observation refuses qualification.

## Exact operation and DO-result mapping

All cases use `capture` at PREDATA_EXPORT, PREDATA_CHECK, COPY_CHECK, POSTDATA_EXPORT, POSTDATA_CHECK, GUARD_CDC_BEGIN, GUARD_CHECKPOINT_NO_FRONTIER and GUARD_BARRIER; CAPTURE_FINAL uses `finish`.
Positive adds `capture` for each MUTATE, REFUSE and RESTORE milestone for MISSING_PK, WRONG_PK, EXTRA_COLUMN and SELF_FK, giving 21 callbacks.
Rollback adds `capture` at ROLLBACK_CHECK and USED_AUTHORITY_REFUSE, giving 11 callbacks.
Unknown adds `arm_postdata` at RELAY_ARM, `status_postdata` at UNKNOWN_CHECK, FENCE_CONTENDER and COMMITTED_ROW_CHECK, `release_postdata` at RELAY_RELEASE, and `capture` at CONSUMED_RECOVERY_REFUSE, giving 15 callbacks.
These are exhaustive mappings: other milestones write stage records only, and a different operation/stage pair refuses before an effect.
The server consumes positive consecutive callback sequence numbers exactly once; Rust and controller retain separate stage files and independent increasing stage sequence numbers.

When no DO occurred, `do_attempted=false`, `do_result=null` and `sqlstate=null`; an entirely absent phase observation remains null only before that phase has an authority binding.
The actual hook's `ok` maps to `true/OK/null`, `sql_error` to `true/ERROR/<actual five-character SQLSTATE>`, and `local_error` to `true/ERROR/null`.
An actual `pending` observation is nonterminal and refuses; it cannot become a synthetic ERROR or OK.
For ROLLBACK_CHECK only, the separate one-hit post-DO fault overrides `sqlstate` with `PostdataScope::fault_sqlstate()` while retaining `do_result=OK` from the unchanged successful DO.
Require exactly one actual fault hit and SQLSTATE `22012`; a local error or the DO's own SQL error cannot satisfy this backend-failure control.
Subsequent retry and recovery use their own fresh observation scopes; committed recovery requires zero DO attempts, no fault hit and null result/SQLSTATE.
USED_AUTHORITY_REFUSE carries a new no-dispatch observation rather than relabeling the earlier successful DO; prior rollback evidence remains in its earlier callback.

## One effective contract and SQL literal

The packet includes a proposed effective contract and API addendum, with their exact byte hashes in its handoff.
It replaces V2's executable ingress/topology/mapping descriptions after approval; the old `1153054f` bytes remain historical, never a selectable fallback.
Keep wire version 2, existing production formats and the twelve-field relay schema; advance only the private interface revision.
Both authors must apply the same approved contract overlay before dependent work, with its hash in prepared input, context and final evidence.

The single metadata expression is:

```sql
pg_catalog.to_jsonb(m.*) || pg_catalog.jsonb_build_object('source_database', m.source_database::pg_catalog.int8, 'target_database', m.target_database::pg_catalog.int8, 'owner_oid', m.owner_oid::pg_catalog.int8, 'schema_oid', m.schema_oid::pg_catalog.int8, 'table_oid', m.table_oid::pg_catalog.int8)
```

Use it from the actual metadata5 row alias after the existing canonical session settings.
Preserve extra columns and explicit NULLs for strict rejection, including an added column named `m`; never replace it with known-column projection or Python coercion.
The `13e7e74a` / `47a2993c` source findings explain this correction without identifying an earlier runtime failure's conjunct.

## Absolute budgets and cleanup

Keep maxima 270 seconds for engine work, 300 for the libtest child, 900 for the case command and 960 for the host envelope, with sixty seconds reserved for cleanup.
Let H be the original absolute host expiry, E = H minus sixty seconds, P = E minus twenty seconds, and L = P minus sixty seconds.
Root's outer command timeout ends no later than P; the existing runner's worst-case twenty seconds of TERM/KILL/communicate/group waits must end by E.
The profile's normal completion deadline is P, with all new effect/observer dispatch stopped by L and its own final sixty seconds reserved for cleanup.
These are nested bounds, not additional time allowances, and actual preflight/setup consumes the original clock.
Before dispatching libtest, use at most `min(300, L - now - 20)` seconds for `run_command`; its escalation must fit before L, and the engine deadline is no later than that child deadline.
An exhausted or nonpositive budget refuses before dispatch; no coordinator wait or clock reset occurs inside the window.

Shared cluster startup remains unchanged and is admitted only with enough remaining non-cleanup time for its bounded call graph and failure cleanup: 490 seconds for logical source and 460 for ordinary target.
These conservative admission windows include three ten-second version calls, forty-five-second initdb, readiness's final thirty-second call after its twenty-second loop deadline, subsequent thirty-second setup calls, fifteen-second failure reaping and ten seconds for local overhead.
Recheck the bound before each sequential `__enter__`; a slow first startup can therefore refuse the second without exceeding the case envelope.
N8 fixed setup/observer SQL also checks its full existing timeout before each call; slow setup cannot borrow the later cleanup reserve.

On failure, first stop admission and preserve actual primary status plus already admitted snapshots.
Close the active IPC peer and stop token, allow at most thirty-five seconds for its single observer/service shutdown, then at most five seconds for relay release/joins, fifteen seconds for both cluster closes in parallel, and five seconds for bounded local bookkeeping.
Each close has its own actual handle and result; one failure must not skip the other.
Require both postmasters reaped, owned roots/socket directories/passfiles absent and every child/thread stopped before a normal PASS.
Filesystem or process cleanup that exceeds the allowance is a separate failure, not inferred absence.
Root's remaining host reserve retains only admitted SAFE files, terminates/removes its exact owned container if necessary, checks absence and seals primary/secondary truth; timeout or missing evidence always leaves counts unqualified.
Do not expose raw outer streams, refused input, arbitrary exceptions or database diagnostics in that fallback.

## Owners and acceptance

Root retains the private one-container driver, prepared input/contract/API, `src/apply/checkpoint_live_tests.rs`, dispatch-only changes in `tools/test-db.py` and `tests/support/db_harness.py`, Task/CI inventory consumers and affected documentation.
The Python owner keeps only `tests/support/schema_postdata_fixture.py` and `tests/support/test_schema_postdata_harness.py`, including both local clusters, the passive relay subclass, IPC and specific oracles.
The Rust owner keeps `src/apply/schema_postdata_live_tests.rs`, cfg(test) hooks in `src/apply/schema_effect.rs` and `src/apply/schema_effect_observation.rs`.
Shared `protocol_fault.py`, production APIs, state/metadata formats and dependencies remain unchanged.

Required controls cover exact prepared hashes/keys, full prefix/runtime identities, endpoint swaps, socket ownership, canonical whole-row OIDs, every operation/DO mapping and original exception identity.
Use actual no-PG Linux peers for ordinary commit_hold: pre-arm refusal, one observed COMMIT, client descriptor closure while the backend remains held, release, upstream join, and held-timeout/error cleanup.
Exercise partial IPC lines, absent acknowledgment, idle shutdown, server failure during a blocked child, wrong credentials/parent/start time, duplicate sequence and first-failure stop.
Inject an observer timeout and both runner escalation graces into clock controls; test both cluster cleanup attempts and incomplete-absence refusal.
Retain compiled producer no-PG controls, exact three live cases plus the separate auxiliary, native constructor inventory and safe earlier-file retention after unsafe later refusal.
Planned application opens remain 20/9/10, or 117 over the matrix; planned callbacks remain 21/11/15, or 141, separate from counted utilities, SQL observers and polls.
Nine schema, nine COPY and nine POSTDATA proofs remain planned, with six POSTDATA effects and three barrier recoveries; counts are not execution evidence.
Only the reviewed complete implementation, reconciled actual constructor graph, Linux controls/PREP and a fresh Root lease may release the nine Native8 real-utility cases.
