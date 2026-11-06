# Getting started

Use the isolated harness for the first connection and COPY run.
It creates its own PostgreSQL server, roles, databases, and temporary state, then checks independent data fingerprints.

## Prerequisites

Use Rust 1.99.1 with rustfmt and Clippy, Python 3.12 or newer, and Make.
The native builder also requires a C compiler, pkg-config, patch, Perl, Bison, and Flex.
Provide installed OpenSSL, Kerberos/GSSAPI, and libcurl development libraries with pkg-config metadata.
The default database fixture requires PostgreSQL 14.24 server utilities, including `initdb`, `postgres`, and `psql`.
Each profile checks its declared expected version against the actual server.
For an approved alternate profile, supply both `RPGMIGRATE_TEST_EXPECTED_PG_VERSION` and `RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM`.
Missing, malformed, or inconsistent override pairs fail.
Never derive expected values from the server under test.

We build a controlled libpq 18.6 artifact from a pinned PostgreSQL archive and reviewed patches.
The patches remove two direct passfile warnings and add a capability marker.
They preserve libpq's file checks, configuration precedence, authentication, TLS, GSSAPI, and curl/OAuth support.
The build stays under `target/native/` and does not replace system libraries.
The [native build guide](../native/libpq/docs/build.md) describes source verification, configuration, and attribution.
The controlled build has its own system-wide service-file directory; use `PGSYSCONFDIR` to select an existing directory when required.

Make selects `target/native/libpq/lib` through `PQ_LIB_DIR` and verifies the native receipt before Rust builds or tests.
Remove an old system-libpq `PQ_LIB_DIR` override before using these targets.
Use `PKG_CONFIG_PATH` to locate the native builder's installed dependency metadata when needed.
Stock libpq lacks the required capability and cannot silently replace this artifact for database operations.

Set `PG_BINDIR` when the matching PostgreSQL server utilities are outside `PATH`.
The harness can also discover them through `PG_CONFIG` or `pg_config`.
Check out this **example** for an existing Homebrew PostgreSQL 14 installation:

- Select its server utilities.

  ```sh
  export PG_BINDIR="$(brew --prefix postgresql@14)/bin"
  ```

## Run the checks

1. Confirm the checkout and branch before building.

   ```sh
   git status --short --branch
   ```

2. Build and verify the controlled native dependency.

   ```sh
   make native native-check
   ```

3. Run compilation, formatting, lint, and database-free tests.

   ```sh
   make check lint test
   ```

4. Verify the independent oracle, then run the live engine and executable suites.

   ```sh
   make test-db-self-check test-db
   ```

5. Inspect the native version and accepted COPY syntax.

   ```sh
   target/debug/rpgmigrate version
   target/debug/rpgmigrate copy table-data --help
   ```

The harness creates disposable databases rather than using an existing server.
Its receipt must match the exact expected test inventory, with no missing or skipped cases.
The data oracle checks relation inventories, column layouts, duplicate-preserving counts, and complete content digests.
Fault cases require evidence that the intended fault occurred.
The default test target also runs the isolated persistence-format contracts; those tests do not qualify a storage backend or establish performance.

For direct Cargo commands, select the same controlled library directory explicitly:

1. Set the native library directory and run the version command.

   ```sh
   export PQ_LIB_DIR="$PWD/target/native/libpq/lib"
   cargo run --locked -- version
   ```

## First-slice limits

Only `ping` and restricted `copy table-data` perform database work.
Other registered commands and unsupported options fail explicitly.
Full CLI compatibility, CDC, recovery, schema migration, operator integration, and release qualification remain later milestones.
The [task status](../tasks/todo.md) records each verified platform, toolchain, and PostgreSQL fixture.
Other platform/version combinations require their own checks.

> [!important]
> Keep schema and namespace DDL quiescent in both databases throughout COPY.
> Table locks do not prevent schema renames or namespace replacement.

Source DML can continue under one REPEATABLE READ READ ONLY snapshot.
Target SHARE ROW EXCLUSIVE locks block concurrent target DML from preflight through one synchronous COMMIT covering every copied table.
The source uses ACCESS SHARE locks.
Per-part checkpoints and broader DDL coordination remain future work.

Every command has one 300-second total deadline, without resets between operations.
The nominal 8 MiB COPY limit controls forwarding/write chunks, not process RSS.
libpq allocates a complete COPY row, including the fixture's 9 MiB value.
Catalog collections have a separate 8 MiB budget and use paged discovery.
Configurable operation/stall deadlines and complete transport memory accounting remain required before full-scale qualification.

COPY requires UTF8 on both servers and compatible, precreated, empty, logged ordinary tables using the built-in heap access method.
Column names, order, types, modifiers, nullability, and collations must agree.
E1 rejects RLS, user triggers, rules, partitions, inheritance, foreign tables, views, and dropped, generated, or identity columns.
Target CHECK constraints, exclusion constraints, expression indexes, and partial indexes are unsupported because they can invoke functions that change session settings or other tables.
Target indexes must use a built-in `btree`, `hash`, `gist`, `gin`, `spgist`, or `brin` access method and matching `pg_catalog` operator classes, each with an OID below 16384.
Custom table access methods, index access methods, and operator classes are unsupported.
Target foreign keys must be deferrable, reference a parent inside the validated COPY set, use identical built-in types for each column pair, and use built-in `pg_catalog` comparison operators with OIDs below 16384.
We defer those foreign keys during load and validate them before the final durability check and synchronous COMMIT.
The target must already report `session_replication_role=origin`; we check it with `SHOW` without requiring permission to change it.
Target internal constraint triggers must be enabled for origin or always execution; disabled and replica-only triggers are unsupported.
These constraint and index restrictions apply to target execution; source CHECK constraints and indexes are not executed by COPY TO.
Broader constraint and access-method support requires later ordering and side-effect guarantees.
Binary COPY, filters, follow mode, and table splitting are unsupported.
Verified empty catalogs and empty tables succeed with zero-count receipts.
Failed catalog queries and missing expected fixtures are errors.

Only these `pg_catalog` base scalar types are accepted:

- `bool`, `int2`, `int4`, `int8`, `float4`, `float8`, `numeric`.
- `text`, `varchar`, `bpchar`, `bytea`, `bit`, `varbit`.
- `date`, `time`, `timetz`, `timestamp`, `timestamptz`, `interval`.
- `uuid`, `json`, `jsonb`, `inet`, `cidr`, `macaddr`, `macaddr8`.

Both sessions pin ISO DateStyle, postgres IntervalStyle, UTC, `extra_float_digits=3`, hex bytea output, `row_security=off`, UTF8, and `search_path=pg_catalog`.
The target must enable `fsync` and use synchronous commit.
An existing `remote_apply` setting remains in effect.

## Native state and completion

Use a fresh state directory below an existing parent.
State validation precedes the first database connection.
E1 refuses foreign or legacy state and every prior native run, including completed runs.
It does not resume, restart, import, or clean pgcopydb state.
New manifests use native format version two and require a target-provenance field, which remains empty for the COPY command.
Version-one and unknown-version manifests are rejected without conversion.
The internal provenance recovery API does not expose public CLI resume or authorize target reuse without the fenced authority's checks.
Target checkpoint and barrier APIs are internal library interfaces.
This slice exposes no CDC receiver or serial-applier command.
The private CopyBoth transport preserves replication envelopes and accepts externally supplied feedback positions.
Its `FeedbackDrained` event means libpq output is no longer pending, including when libpq discarded that output after a write failure.
The event proves neither delivery nor source acknowledgment or durability.

> [!important]
> Retain exclusive control of the active private `0700` state directory.
> Permissions and advisory locks protect against other users and cooperating rpgmigrate processes.
> Arbitrary noncooperating mutation by the same user is outside this guarantee.

Pinned directory handles, no-follow opens, and identity checks detect supported replacement cases.
POSIX validation and rename do not provide compare-and-swap publication against arbitrary concurrent mutation.

After confirmed target COMMIT, the native manifest records actual table, row, and byte counts.
The success output includes those counts, the 16-byte run identity, and the state format version.
It omits the private state path.
The receipt does not authorize recovery or reuse of target data.
Process-kill tests do not prove power-loss durability or arbitrary failover safety.

> [!caution]
> A lost target COMMIT response returns exit code 3 with `commit-outcome-unknown` and no completion marker.
> Do not infer rollback or automatically retry that copy.

## Storage pilot

We compare three experiment-only candidates using one stream, one writer, one active source transaction, and synchronous storage batches.
The candidates are segmented files with a SQLite locator catalog, redb, and SQLite record storage.
They are dev dependencies and do not select a runtime journal backend.

1. Run the required format, backend, common recovery, and driver tests.

   ```sh
   make test-storage-format test-storage-recovery
   ```

2. Select a new directory below an existing parent and run the release pilot.
   The runner refuses an existing directory and preserves its generated stores and receipts.

   ```sh
   STORAGE_ROOT=target/storage-pilot-1 make storage-pilot
   ```

The fixed [case manifest](../bench/storage/cases.json) supplies the seed, all three candidates, all three workloads, and three repetitions.
Each candidate/case/repetition uses a fresh child process with a 300-second deadline.
Seeded case ordering and paired candidate shuffling reduce fixed-order bias.
The parent requires every exact receipt and compares canonical input checksums and storage-batch boundaries.
Missing cases, empty workloads, failed children, and missing or inconsistent reports return nonzero.

Each child reports storage completed transactions and encoded bytes, with separate create, write, checkpoint, close, reopen, verified-scan, and finalization timings.
Write time includes input generation, encoding, common validation, batch setup, and commit overhead; it is not an isolated synchronization measurement.
Total time includes explicit checkpoints, redb integrity-on-open work, and both bounded scan passes before completion.
The scan compares each replayed record with the deterministic input stream and requires the workload's external completed-transaction prefix.
The giant transaction spans storage batches without an early completed descriptor or retained transaction payload.
These rates measure storage work, not target-applied transactions or PostgreSQL WAL application.

The report records warm-after-write cache conditions; a fresh directory is not cold-cache proof.
Peak RSS is measured with `RUSAGE_SELF` in each child and normalized to bytes using the [Linux interface](https://man7.org/linux/man-pages/man2/getrusage.2.html) and [Darwin implementation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_resource.c).
The 64 MiB configured cache budget is separate from RSS, allocator state, transport frames, and SQLite WAL growth.
SQLite settings and source identity are queried; redb policy is explicitly labeled as configured because its API does not expose those getters.
The runner refuses captured or active SQLite-specific build overrides and listed CFLAGS overrides, checks the reviewed SQLite source identity, and records compile options.
Use the checked-in lockfile and the normal Cargo build without those overrides.

> [!important]
> This pilot does not select a winner or qualify a production backend.
> Multi-process access, retirement, quotas, crash/fault matrices, power-loss behavior, and target application remain separate gates.
> Keep exclusive control of the generated private directories; the pilot does not defend against arbitrary mutation by the same user.
