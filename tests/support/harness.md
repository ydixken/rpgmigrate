# E1 disposable PostgreSQL tests

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

5. Run all seven transport, catalog, and COPY module cases together.

   ```sh
   python3 tools/test-db.py --engine -- cargo test --locked --lib -- --ignored --test-threads=1
   ```

6. Run the actual executable against the complete E1 case inventory.

   ```sh
   python3 tools/test-db.py -- cargo test --locked --test copy_integration -- --test-threads=1
   ```

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
The engine wrapper requires those five cases plus the two catalog and COPY cases in the same manifest.
Running zero tests or an unrelated command cannot satisfy either suite.

The native completion receipt is `manifest.json` in the explicit run directory.
Successful full COPY must report two tables and 10,003 rows in `copy_complete`.
The command's success JSON must match the manifest's run identity, state format version, and counts without publishing private paths.
Failed or uncertain runs must leave `copy_complete` null, or leave no native manifest when refusal precedes state creation.
The harness never authorizes automatic retry of an uncertain COMMIT.

These fixtures establish only the selected E1 contracts on the pinned PostgreSQL version.
They do not establish the release version matrix, CDC recovery, or performance claims.
