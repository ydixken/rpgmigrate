# Disposable PostgreSQL tests

We run each command in a new owned PostgreSQL cluster with TCP disabled, private Unix sockets, and an explicit nondefault port.
Only that child postmaster and its owned temporary directories are cleaned up.
The fixture database comments and server identity identify the current harness instance.
Inherited `PG*`, `PGCOPYDB*`, and `RPGMIGRATE*` variables are removed before child processes start.
The fixture runner supplies every database, role, socket, port, and passfile explicitly.
The `e1_auth` role uses a SCRAM-only HBA rule before the private cluster's trust rules.
Its generated password is supplied only through the controlled test environment.
Authentication tests use explicit temporary passfiles and service files, never the user's authentication files.

## Run the checks

Run Python with assertions enabled: the shared harness refuses optimized execution before any fixture starts.

1. Set `PG_BINDIR` to a complete PostgreSQL 14.24 server utility directory, or set `PG_CONFIG` to its `pg_config` executable.
   With neither variable set, discovery uses `pg_config` or `postgres` from `PATH`.
   `postgres`, `initdb`, and `psql` must report the exact version in the [fixture manifest](../fixtures/e1/manifest.json).
   A pinned CI case can override that default with both `RPGMIGRATE_TEST_EXPECTED_PG_VERSION` and `RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM`.
   For example, the PG17 case declares `17.11` and `170011` in its image recipe.
   The harness rejects missing or inconsistent pairs and compares actual versions with the declared expectation.
   It never derives expected versions from the server probe.
   Authority and full-engine fixtures also require the matching `pg_basebackup` executable.

2. Check the isolation, independent fingerprints, corruption controls, and wire faults with `psql`.

   ```sh
   python3 tools/test-db.py --self-check
   ```

3. Run the harness validation checks.

   ```sh
   python3 tests/support/test_harness.py
   ```

4. Run the five transport cases against another disposable cluster.

   ```sh
   python3 tools/test-db.py --transport -- cargo test --locked --lib pg::live_tests:: -- --ignored --test-threads=1
   ```

5. Run the seven prepared transport cases.

   ```sh
   python3 tools/test-db.py --prepared -- cargo test --locked --lib pg::prepared_live_tests:: -- --ignored --test-threads=1
   ```

6. Run the sixteen target authority cases.

   ```sh
   python3 tools/test-db.py --authority -- cargo test --locked --lib apply::checkpoint::live_tests:: -- --ignored --test-threads=1
   ```

7. Run the nine raw replication transport cases.

   ```sh
   python3 tools/test-db.py --replication -- cargo test --locked --lib pg::replication_live_tests:: -- --ignored --test-threads=1
   ```

8. Run all thirty-nine transport, prepared, authority, replication, catalog, and COPY module cases together.

   ```sh
   python3 tools/test-db.py --engine -- cargo test --locked --lib -- --ignored --test-threads=1
   ```

9. Run the actual executable against the complete E1 case inventory.

   ```sh
   python3 tools/test-db.py -- cargo test --locked --test copy_integration -- --test-threads=1
   ```

10. Prove the authority fixtures and held-COMMIT faults independently with `psql`.

   ```sh
   python3 tests/support/authority_selfcheck.py
   ```

11. Prove the logical source, oversize frame, four fault groups, and actual libpq backpressure independently of Rust.

    ```sh
    PQ_LIB_DIR=target/native/libpq/lib python3 tests/support/replication_selfcheck.py
    ```

Use a separate `CARGO_TARGET_DIR` for each concurrent Rust verification worker.

## Authority fixtures

Authority tests use a separate database through `RPGMIGRATE_TEST_AUTHORITY_TARGET`, leaving the original engine fixtures isolated.
The context file supplies the private parent directory for native state.
`RPGMIGRATE_TEST_AUTHORITY_ADMIN` permits setup inside this owned database.
`RPGMIGRATE_TEST_AUTHORITY_RESTRICTED` has no CREATE privilege; tests grant only the preprovisioned object privileges they exercise.
The positive preprovisioned case must use `e1_target` as object owner and `e3_restricted` as runtime user, with no ownership membership and only schema USAGE plus table SELECT/INSERT/UPDATE.
The authority database explicitly denies `pg_control_system()` EXECUTE to PUBLIC and the restricted role.
`RPGMIGRATE_TEST_AUTHORITY_READONLY` selects a read-only session on the normal primary.
`RPGMIGRATE_TEST_AUTHORITY_STANDBY` connects to a physical standby copied from this owned primary, with private data and socket directories and TCP disabled.
`RPGMIGRATE_TEST_AUTHORITY_FSYNC_OFF` connects to a third disposable primary started with `fsync=off` for refusal tests.
The normal primary and standby retain `fsync=on` throughout.
The wrapper stops both extra child postmasters before stopping and removing the original cluster.

Launch `RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER` with `RPGMIGRATE_TEST_PYTHON` and `--case lost_commit_reply` or `--case provisioning_recovery`.
The first JSON line supplies `ready`, an explicit `conninfo`, and a private `receipt` path.
Send one JSON object per stdin line with `op` set to `arm`, `status`, `release`, or `quit`.
Each response includes the operation, observed `fault_hits`, `backend_pid`, `commit_tag`, `holding_backend`, `backend_connection_closed`, `held_fence_observed`, and `released_fence_observed`.
Receipt `status` remains `pending` until the COMMIT and both fence observations are complete, then becomes `observed`.
Arming drops the next real positive COMMIT reply, closes the client connection, and retains the old backend until release.
The release operation queries the actual advisory lock and backend before closing the upstream connection, then waits for their observed disappearance.
Tests must independently demonstrate failed fence acquisition while that backend remains held.
Unarmed COMMIT and ROLLBACK messages do not satisfy a fault receipt.
The relay bounds a held backend to 45 seconds and closes it on helper exit or fixture cleanup.

## Replication fixtures

The replication and engine profiles start a separate owned primary with `wal_level=logical`, four slots, four WAL senders, `wal_sender_timeout=30s`, and `fsync=on`.
Existing authority and COPY server settings stay unchanged.
The fixture checks the actual role attributes and loads `test_decoding` through a temporary slot before running the requested command.
Each live test creates its own temporary slot on the same replication connection, with `NOEXPORT_SNAPSHOT`.
The normal admin connection permits synthetic data production and independent server observations inside the owned database.

| Environment variable | Value |
| --- | --- |
| `RPGMIGRATE_TEST_REPLICATION_SOURCE` | Explicit connection with `replication=database`. |
| `RPGMIGRATE_TEST_REPLICATION_SQL` | Same non-superuser REPLICATION role, normal connection. |
| `RPGMIGRATE_TEST_REPLICATION_ADMIN` | Normal admin connection to the owned source database. |
| `RPGMIGRATE_TEST_REPLICATION_DATABASE` | Exact owned database name. |
| `RPGMIGRATE_TEST_REPLICATION_CONTEXT` | Private context file for the separate source cluster. |
| `RPGMIGRATE_TEST_REPLICATION_FAULT_HELPER` | Path to the wire controller. |

Launch the helper with `RPGMIGRATE_TEST_PYTHON` and `--case keepalive`, `duplex`, `terminal_error`, or `missing_completion`.
Its first JSON line supplies `ready`, a replication `conninfo`, and a private `receipt` path.
The fixed receipt filenames are `replication-fault-<case>.json` under the replication context root.
Each filename can be created only once per wrapper run.
The `terminal_error` group retains two independently observed phases in that same file.
After the real CopyBoth response, send one JSON request per stdin line:

- Keepalive: `{"op":"arm","wal_end":900000,"feedback_hex":"72000000000000000000000000000000000000000000000000000000000000000000"}` injects one known `k` frame with timestamp zero and reply requested.
- Other modes: `{"op":"arm"}` enables their single fault.
- `{"op":"status"}` returns the current receipt fields.
- Duplex: `{"op":"release","pqflush_pending":1}` releases the read gate after the test has independently counted actual `PQflush == 1` results and received a server frame while blocked.
- Close the client, then send `{"op":"quit"}` to finish cleanup and receive the final receipt.

Run `terminal_error` first with the normal helper invocation to exercise `late_error`.
That first quit reports `phase_status=observed` and group `status=pending`.
Launch the same case with `--pending-error` for the second phase on a fresh connection and temporary slot.
Arm its read gate, observe actual `PQflush == 1`, then send `{"op":"cancel","pqflush_pending":1}` with the observed count.
The controller matches the PID from BackendKeyData against the owned database, replication role, WAL sender, and active temporary `test_decoding` slot before calling `pg_cancel_backend`.
It forwards the actual server ErrorResponse while the client-read gate remains held.
After asserting SQLSTATE `57014`, connection closure before the deadline, and no `FeedbackDrained` event after cancellation, send `{"op":"client_error","sqlstate":"57014","closed":true,"before_deadline":true,"feedback_drained":0}` with those observations.
Quit without releasing this gate.
The group becomes `observed` only when both closed phase proofs are valid, with `fault_hits=2`; duplicate phase invocations fail without altering the earlier proof.
All other groups retain `fault_hits=1`.

Natural server keepalives can precede the injected one; wait for its exact header within the test deadline.
The duplex gate stops reading the client socket and continues forwarding server traffic, without buffering client frames.
Poll `server_frames_while_held` before releasing to observe completed forwarding.
The controller never derives the `pqflush_pending` counter from its read-gate flag.
The late-error phase and missing-completion fault observe real server CopyDone and CommandComplete before substituting SQLSTATE `57014` or dropping completion and closing the connection.
The pending-error phase requires its own native backpressure count and actual server error, independent of the separate duplex case.
Every receipt requires an observed CopyBoth response and closed owned backend connection.
Keepalive receipts require a positive `post_arm_feedback_count` and zero `feedback_mismatches` across every post-arm frame.
The first post-arm 34-byte feedback must match the supplied bytes; earlier feedback cannot satisfy the receipt.
`feedback_count` retains the total number of captured frames so the test can also detect unsolicited feedback.
Duplex receipts additionally require incoming frames during the hold, release, captured feedback, and a positive native backpressure count.
These tests exercise raw transport; local output-buffer draining does not prove delivery, source receipt, target commit, or permission to advance a recovery frontier.
The self-check loads only the explicitly selected `PQ_LIB_DIR` library through Python's standard-library `ctypes` module.

## Fixture and oracle

The [schema](../fixtures/e1/schema.sql) and [synthetic seed](../fixtures/e1/seed.sql) are independent of the Rust runtime.
The source contains 10,003 rows, paired duplicate rows, NULL and empty values, binary bytes, escaped text, Unicode, opposing date and interval defaults, and rows larger than the 8 MiB COPY budget.
Both databases include a deliberately empty logged table.
The manifest fixes the exact schema, relation, column, type, row-count, and case inventories.

The oracle reads every value through `psql` with canonical session settings.
It encodes each value as a NULL tag or a byte length and value, orders the complete encoded rows, and hashes all rows with SHA-256.
Duplicate rows remain in the digest.
Missing relations, changed types, and same-count value changes must each fail independently.

The source fault relay replaces the terminal COPY result only after all 10,003 real data frames have passed.
The target constraint fault duplicates only the final key of a genuine UNIQUE constraint.
A passive relay must observe PostgreSQL SQLSTATE `23505` after more than 8 MiB of COPY data before this fault case can pass.
The relay preserves the server's error message and never substitutes a synthetic UNIQUE error.
The COMMIT relay drops an observed positive COMMIT response, then the oracle proves the target committed while the executable must return `commit-outcome-unknown` without a completion receipt.
Signal cases hold an owned target lock, observe the executable backend waiting, and send SIGINT or SIGTERM to that exact child process.
CHECK constraints that change client encoding or delete previously copied rows must be refused before target COPY data is sent.
The same refusal applies to expression and partial indexes, custom operator classes, and custom index access methods.
Exclusion constraints and target sessions with `session_replication_role = replica` are also refused before data transfer.
The FK fixture copies `a_child` before `z_parent` and verifies both complete contents under a deferrable, initially immediate FK.
Nondeferrable target FKs, references outside the copied table set, and mixed parent/child column types must be refused before data transfer.
Pre-existing disabled or replica-only internal FK triggers must also be refused, even when the target session uses `origin`.
Both trigger-state fixtures contain an orphan child row, so accepting either state would bypass FK validation.
The refusal must preserve those trigger states, with no automatic re-enabling.
An orphan child row must produce an observed `23503` after all COPY input and roll back both tables without completion.

## Receipt rules

The full wrapper requires one result per manifest case with no duplicates, omissions, skips, or unexpected case IDs.
Every fault case requires an observed hit and nonzero exit.
The transport-only wrapper independently checks its exact five libtest names and rejects ignored cases.
The prepared wrapper requires the seven exact names in the independent [prepared inventory](prepared_cases.json).
The authority wrapper requires the sixteen exact names in the independent [authority inventory](authority_cases.json).
The replication wrapper requires the nine exact names in the independent [replication inventory](replication_cases.json).
The engine wrapper requires all five transport, two catalog/COPY, seven prepared, sixteen authority, and nine replication cases.
Every module profile rejects empty or duplicate expected names, missing cases, changed names, duplicate results, and ignored or failed tests.
Running zero tests or an unrelated command cannot satisfy any suite.
Both authority and full-engine success also require one observed COMMIT fault receipt for each of `lost_commit_reply` and `provisioning_recovery`, including the held and released backend fence evidence.
Both profiles reject missing receipts, zero fault hits, and pending or ignored receipt status through the same wrapper validation function.
Both replication and engine profiles also require all four replication fault receipts and reject missing, extra, wrong-case, zero-hit, pending, ignored, and incomplete evidence.
Both profiles reject a missing terminal-error phase, mismatched server or client SQLSTATE, or client closure after gate release or deadline.
Engine verification retains the two authority fault receipts in addition to the four replication receipts.

The native completion receipt is `manifest.json` in the explicit run directory.
E1 receipts require native format version `2` and an empty `target_provenance` array.
Version `1`, unknown versions, missing provenance, and nonempty provenance fail the COPY receipt oracle.
Successful full COPY must report two tables and 10,003 rows in `copy_complete`.
The command's success JSON must match the manifest's run identity, state format version, and counts without publishing private paths.
Failed or uncertain runs must leave `copy_complete` null, or leave no native manifest when refusal precedes state creation.
The harness never authorizes automatic retry of an uncertain COMMIT.

These fixtures establish only the selected E1 contracts on the pinned PostgreSQL version.
They do not establish the release version matrix, CDC recovery, or performance claims.
