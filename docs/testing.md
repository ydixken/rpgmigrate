# Testing

We use the root Make targets for local development and the E1 Linux workflow.
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
   bash tools/ci/linux.sh
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
It then runs `make native`, `make native-check`, `make lint`, and `make test`.
The native checks verify the reviewed PostgreSQL 18.6 source, patches, installed hashes, and capability marker.
They also require OpenSSL, GSSAPI, and libcurl OAuth support in the native receipt.

After building, the runner forces the executable to load Debian's stock libpq.
It requires the exact safe configuration diagnostic, exit code 2, and no connection attempt to its owned socket listener.
The normal native library must pass the live authentication and COPY checks.

`make test-db-self-check` exercises the independent data oracle, protocol fault injection, and SCRAM controls.
`make test-db` runs the live engine tests followed by the complete COPY integration harness.
The engine profile requires all 30 ignored library tests, including the seven prepared-transport cases in [the prepared inventory](../tests/support/prepared_cases.json) and the 16 target-authority cases in [the authority inventory](../tests/support/authority_cases.json).
The transport-only profile retains five cases, and the COPY profile retains 40 cases.
The COPY harness requires version-two native state receipts with empty target provenance.
The harness compares exact case names, full data fingerprints, server versions, and required fault hits against [the E1 fixture manifest](../tests/fixtures/e1/manifest.json) and the focused inventories.
The authority and engine profiles each require two observed COMMIT-reply drops, with the old backend fence observed held and then released for each fault.
Their private authority database keeps metadata fixtures separate from the COPY fixtures.
The CI runner requires exactly one authority-fault footer and records its two verified faults in the report.
It also rejects absent or incorrect suite receipts and empty or skipped unit suites.
Missing tools and failed commands are failures.

Each run exports one JSON report capped at 64 KiB.
It contains stage results, observed suite counts, the source snapshot hash, image ID and base digest, package and tool versions, native build identity, and executable hash.
We export structured evidence instead of raw command output because failing database tests can expose connection strings or fixture passwords.
Raw database logs, environment dumps, DSNs, and cluster directories are not uploaded.
A failed report identifies the failed stage; reproduce that target in the disposable fixture to investigate it.

## Run individual targets

Use the [native build guide](../native/libpq/docs/build.md) for compiler and dependency prerequisites.
These commands need Rust 1.99.1, Python 3.12 or newer, and the reviewed native artifact.
Database targets also need a complete PostgreSQL utility bundle selected through `PG_BINDIR` or `PG_CONFIG`.

1. Build and verify the controlled native dependency.

   ```sh
   make native native-check
   ```

2. Run formatting, lint, units, CLI checks, and native/harness regression tests.

   ```sh
   make lint test
   ```

3. Check the disposable database harness and run E1.

   ```sh
   make test-db-self-check test-db
   ```

For focused live tests, choose one of these **examples**:

- Run the seven-case prepared-transport profile.

   ```sh
   PQ_LIB_DIR="$PWD/target/native/libpq/lib" \
     python3 tools/test-db.py --prepared -- \
     cargo test --locked --lib pg::prepared_live_tests:: -- --ignored --test-threads=1
   ```

- Run the 16-case target-authority profile.

   ```sh
   make test-authority
   ```

- Run the same authority profile through the wrapper with an isolated Cargo target directory.

   ```sh
   CARGO_TARGET_DIR="$PWD/target/authority-dev" \
     PQ_LIB_DIR="$PWD/target/native/libpq/lib" \
     python3 tools/test-db.py --authority -- \
     cargo test --locked --lib apply::checkpoint::live_tests:: -- --ignored --test-threads=1
   ```

Each focused profile requires every name in its inventory.
Use `make test-db` for the complete 30-case engine profile and 40-case COPY profile.
The engine command selects all ignored library tests without skips.

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
These tools form a test environment; this workflow does not publish a runtime image or release artifact.

## CI scope

[The workflow](../.github/workflows/ci.yml) schedules the same container on hosted Ubuntu 24.04 amd64 and arm64 runners.
It uses `contents: read`, commit-pinned checkout and artifact actions, no persisted checkout credentials, and seven-day report retention.
It requires no repository secrets.

> [!important]
> A successful local report qualifies only its recorded architecture, PostgreSQL fixture, and source snapshot.
> Configured workflow jobs do not prove a successful GitHub run.
> This E1 fixture does not qualify the PostgreSQL 14–18 operator matrix, PG9.6/test_decoding, TimescaleDB, CDC, recovery, power-loss durability, performance, or release packaging.

Run those gates under their approved plans before claiming the corresponding support.
