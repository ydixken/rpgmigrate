# Testing

We use the root Taskfile for local development and the Linux workflow.
Install the pinned Task binary with the [getting-started procedure](getting-started.md#run-the-checks).
The [verification plan](superpowers/plans/2026-11-06-verification-performance.md) defines the broader compatibility, durability, and release gates.

## Run the Linux fixture

Use an existing Podman installation with a running Linux engine.
The entrypoint installs packages only while building its container image.

1. Confirm the engine you are about to use.

   ```sh
   podman version
   ```

2. Run the same entrypoint as CI from the repository root.

   ```sh
   task ci:linux
   ```

3. Find the report at the path printed by the entrypoint.

   ```sh
   ls target/ci-linux-*/report.json
   ```

The container runs as UID 1000 with all capabilities dropped and no published ports.
It mounts this checkout read-only, copies the required build inputs into `/work`, and builds a fresh native library and Rust executable there.
It does not read host Cargo credentials, host database settings, or the host native build cache.
The harness starts its own PostgreSQL process on a private Unix socket and a random port between 20000 and 59999, with TCP disabled.
Its authentication cases use a private SCRAM role.
The harness checks ownership before cleanup, and the entrypoint removes only the container it created, including after a failed command.
Image layers remain in Podman's build cache; reports remain under `target/ci-linux-*`.

## Checks and evidence

The entrypoint runs its Python regression tests, ShellCheck, yamllint, and actionlint before compiling.
Its Python inventory includes the CI receipt validator, publisher, launcher, Task/bootstrap tests, and image build/runtime/smoke tests, with one nonempty result footer.
It then runs `task native`, `task native-check`, `task lint`, and `task test`.
The shared bootstrap helper supplies Task 3.54.0 to local shells, CI hosts, and the fixture container.
The library unit gate includes the same [SQLite build-policy validator](../build.rs) used by Cargo and the private immutable-plan store tests.
Verification rejects `TASK_DRY` instead of accepting a command preview as a test result.
The native checks verify the reviewed PostgreSQL 18.6 source, patches, installed hashes, and capability marker.
They also require OpenSSL, GSSAPI, and libcurl OAuth support in the native receipt.

After building, the runner forces the executable to load Debian's stock libpq.
It requires the exact safe configuration diagnostic, exit code 2, and no connection attempt to its owned socket listener.
The normal native library must pass the live authentication and COPY checks.

`task test-db-self-check` exercises the independent data oracle, protocol fault injection, and SCRAM controls.
`task test-db` runs the engine, catalog, source-identity, source-snapshot, journal-serial, and COPY profiles in order.
The engine profile requires its exact 41 ignored library tests, including the E1 catalog preflight case.
It excludes the separate CDC catalog, source-identity, snapshot, and journal-serial profiles.
Its focused inventories cover [seven prepared transport cases](../tests/support/prepared_cases.json), [18 target authority cases](../tests/support/authority_cases.json), and [nine raw CopyBoth cases](../tests/support/replication_cases.json).
The transport-only profile retains five cases, and the COPY profile retains 40 cases.
The COPY harness requires version-four native state receipts with empty target provenance and null `source_slot` and `copy_snapshot` fields.
The harness compares exact case names, full data fingerprints, server versions, and required fault hits against [the E1 fixture manifest](../tests/fixtures/e1/manifest.json) and the focused inventories.
The authority and engine profiles each require two observed COMMIT-reply drops, with the old backend fence observed held and then released for each fault.
Their private authority database keeps metadata fixtures separate from the COPY fixtures.
The replication and engine profiles each require four source observations: `keepalive`, `duplex`, `terminal_error`, and `missing_completion`.
The duplex receipt requires observed `PQflush == 1` backpressure while incoming traffic continues.
These profiles use a separate owned logical source, a non-superuser replication role, and temporary `test_decoding` slots.
The dedicated catalog and source-identity profiles require ten and three cases respectively.
The source-snapshot profile requires twelve cases and five fault IDs, including both wrong ReadyForQuery states within the existing terminal-reply case.
Its capability assertions use a real provider-created native binding and an ordinary SQL connection.
The journal-serial profile requires two parent cases, two post-reap SIGKILL receipts, and two committed-Unknown fault receipts.
It exercises the development SQLite and segmented-journal candidates with real source WAL, reopened records, serial target effects, and checkpoint reconciliation.
The private crash child runs only through its parent's owned request and is excluded from the top-level inventory.
The CI runner requires each profile's exact, unique receipt and its required fault or child receipts.
Its report records two authority faults, four replication observations, five source-snapshot faults, and the journal profile's two child and two committed-Unknown observations.
It also rejects absent or incorrect suite receipts and empty or skipped unit suites.
Missing tools and failed commands are failures.
The [maintained gate ledger](../tasks/todo.md#source-snapshot-and-serial-prerequisites) separates implemented checks from completed platform qualification.

`task test-harness` runs five required offline modules through the existing unit runner: shared harness, catalog, source identity, source snapshot, and journal-serial.
It checks each module's discovered count against its source and emits one combined result footer.
Missing or empty modules, optimized Python, and skipped cases fail the gate.
Ordinary Rust tests retain the journal adapter's reused storage tests; only the three ignored parent/child entry points are excluded from that unit command.

`task test` also runs `test-storage-publication` after the format and recovery suites.
This development-only check starts 22 fresh child processes: 20 publication-boundary kills and two complete traces across SQLite and the segmented journal.
Each killed case must reopen against an external required prefix, append the next transaction, and verify the full retained history.
The task creates a private directory under `target/storage-publication.*` and requires its nonempty `run/report.json` before emitting the receipt.
The existing CI test stage requires exactly one 22-case, 20-continuation receipt in addition to the Rust and three Python suite receipts.
Redb remains a development comparison with a known read-only corruption qualification failure; it is outside this publication matrix.
Process kills do not qualify power-loss durability, pruning, quotas, target acknowledgements, or production backend selection.

Each run exports one JSON report capped at 64 KiB.
It contains stage results, observed suite counts, the source snapshot hash, image ID and base digest, package and tool versions, native build identity, and executable hash.
We export structured evidence instead of raw command output because failing database tests can expose connection strings or fixture passwords.
Raw database logs, environment dumps, DSNs, and cluster directories are not uploaded.
A failed report identifies the failed stage; reproduce that target in the disposable fixture to investigate it.

## Run individual tasks

Use the [native build guide](../native/libpq/docs/build.md) for compiler and dependency prerequisites.
These commands need Rust 1.99.1, Python 3.12 or newer, and the reviewed native artifact.
Database targets also need a complete PostgreSQL utility bundle selected through `PG_BINDIR` or `PG_CONFIG`.
The replication profile also requires the `test_decoding` plugin from that PostgreSQL installation.

1. Build and verify the controlled native dependency.

   ```sh
   task native native-check
   ```

2. Run formatting, lint, units, CLI checks, and native/harness regression tests.

   ```sh
   task lint test
   ```

3. Check the disposable database harness and run E1.

   ```sh
   task test-db-self-check test-db
   ```

4. Run only the development publication/recovery matrix in fresh storage.

   ```sh
   task test-storage-publication
   ```

For focused live tests, choose one of these **examples**:

- Run the seven-case prepared-transport profile.

   ```sh
   PQ_LIB_DIR="$PWD/target/native/libpq/lib" \
     python3 tools/test-db.py --prepared -- \
     cargo test --locked --lib pg::prepared_live_tests:: -- --ignored --test-threads=1
   ```

- Run the 18-case target-authority profile.

   ```sh
   task test-authority
   ```

- Run the same authority profile through the wrapper with an isolated Cargo target directory.

   ```sh
   CARGO_TARGET_DIR="$PWD/target/authority-dev" \
     PQ_LIB_DIR="$PWD/target/native/libpq/lib" \
     python3 tools/test-db.py --authority -- \
     cargo test --locked --lib apply::checkpoint::live_tests:: -- --ignored --test-threads=1
   ```

- Run the nine raw CopyBoth cases.

   ```sh
   task test-replication
   ```

- Run the ten catalog cases and three source-identity probes.

   ```sh
   task test-catalog test-source-catalog
   ```

- Run the twelve source-snapshot cases and five required fault observations.

   ```sh
   task test-source-snapshot
   ```

- Run the journal-serial parent cases with their required crash and committed-Unknown receipts.

   ```sh
   task test-journal-serial
   ```

- Run the same replication profile through the wrapper with an isolated Cargo target directory.

   ```sh
   CARGO_TARGET_DIR="$PWD/target/replication-dev" \
     PQ_LIB_DIR="$PWD/target/native/libpq/lib" \
     python3 tools/test-db.py --replication -- \
     cargo test --locked --lib pg::replication_live_tests:: -- --ignored --test-threads=1
   ```

Each focused profile requires every name in its inventory.
Use `task test-db` for all six required database profiles: Engine41, Catalog10, SourceCatalog3, SourceSnapshot12, JournalSerial2, and COPY40.
The engine command keeps the E1 catalog preflight case and excludes the separate CDC catalog, source-identity, snapshot, and journal live tests.
The journal profile does not select a production backend or qualify an aborted unknown-COMMIT path, source acknowledgments, COPY snapshot coverage, or power-loss durability.
The [owning COPY plan](superpowers/plans/2026-11-07-copy-snapshot-owning.md) defines a separate eight-group qualification package.
Its required assertions cover all-entry admission before COPY, exact owned-target OID exclusion, atomic data and anchor completion, unknown COMMIT recovery, and failures after proof.
The existing six profiles do not substitute for that package, and no additional command is documented until its harness registration exists.

The default database fixture expects PostgreSQL 14.24 and numeric version 140024.
The container supplies `RPGMIGRATE_TEST_EXPECTED_PG_VERSION=17.11` and `RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM=170011` from its pinned package recipe.
Both values must be present and consistent when overriding the default.
The harness independently checks `postgres`, `initdb`, `psql`, and the running server against those expected values.
Never derive an expected version from the server being tested.

## Pins and third-party sources

[Containerfile](../tools/ci/Containerfile) selects the official Rust 1.99.1 Trixie image by its multi-architecture digest.
The [official image manifest](https://github.com/docker-library/official-images/blob/master/library/rust) identifies the upstream Dockerfiles and supported architectures.
APT uses the signed [Debian snapshot](https://snapshot.debian.org/) at `20261106T000000Z`, which fixes the dependency graph, including PostgreSQL `17.11-0+deb13u1`.
Only snapshot expiration checks are disabled; package signature checks remain enabled.
The image retains Debian copyright and license files under `/usr/share/doc`.

Actionlint 1.7.7 comes from its [upstream release](https://github.com/rhysd/actionlint/releases/tag/v1.7.7), with separate SHA-256 pins for amd64 and arm64 and its MIT license retained in the image.
ShellCheck 0.10.0 and yamllint 1.37.1 are Debian packages under GPL-3+.
The Rust image's source uses MIT/Apache-2.0 licensing, and the reviewed native library retains the PostgreSQL notices.
These tools form the fixture image, which is separate from the [runtime image](container.md).
The runtime image contains the implemented CLI and its dependencies, without the fixture's PostgreSQL utilities.

## CI scope

[The workflow](../.github/workflows/ci.yml) schedules the same container on hosted Ubuntu 24.04 amd64 and arm64 runners.
Verification jobs use `contents: read`, commit-pinned checkout and artifact actions, no persisted checkout credentials, and seven-day report retention.
They require no repository secrets.
Each native job builds one runtime OCI artifact and smoke-tests that exact archive after the source fixture passes.
The version-tag publication job requires both native jobs and uses `packages: write` only to publish the tested index to `ghcr.io/ydixken/rpgmigrate`.
It does not create a tag or release, and ordinary branch or pull-request runs do not publish images.
The [container guide](container.md) describes artifact identity, smoke checks, and publication limits.

> [!important]
> A successful local report qualifies only its recorded architecture, PostgreSQL fixture, and source snapshot.
> Configured workflow jobs do not prove a successful GitHub run.
> The source fixture does not qualify the PostgreSQL 14–18 operator matrix, PG9.6/test_decoding, TimescaleDB, runtime CDC, production recovery, power-loss durability, or performance.
> Runtime packaging requires its separate exact-artifact smoke and publication gates.

Run those gates under their approved plans before claiming the corresponding support.
