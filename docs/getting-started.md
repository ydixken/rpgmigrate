# Getting started

Use this guide to inspect and build the engine without connecting to a database.
The [Testing guide](testing.md) defines disposable database checks and their prerequisites.

> [!IMPORTANT]
> Production native operator admission returns `UnsupportedEngine`.
> M6 through M10 remain open in the [roadmap](roadmap.md).
> Discovery and packaging receipts do not qualify an operator migration.

## Build without database access

Install the Rust toolchain required by `Cargo.toml`, Python 3, pkg-config, a C toolchain, and stock libpq development files.
Keep bundled SQLite build overrides unset.
Use the [Testing prerequisites](testing.md#prerequisites) for library selection.

1. Inspect the checkout and branch before running commands.

   ```sh
   git status --short --branch
   ```

2. Bootstrap the repository's pinned Task binary.

   ```sh
   python3 tools/bootstrap-task.py
   export PATH="$PWD/target/tools/task/3.54.0/bin:$PATH"
   ```

3. Run the checks and build the executable.

   ```sh
   task check lint test build
   ```

4. Inspect commands and discovery without opening a database connection.

   ```sh
   ./target/debug/rpgmigrate --help
   ./target/debug/rpgmigrate version --json
   ```

If `CARGO_TARGET_DIR` is set, use the executable in that build directory instead.
Linux discovery contains seven capabilities.
Other platforms contain six.
See the [discovery contract](native-protocol.md#discovery) for the exact arrays and qualification limits.

## First disposable migration

Use the existing harness rather than supplying production database addresses.
It creates its own source and target containers, internal network, fixture credentials, caller identity, and private workdir.
The positive fixture and independent SQL oracle define success.
The harness retains redacted receipts and removes only its owned containers and network.
Read the [container prerequisites and limits](testing.md#container-clone-checks) before starting it.

1. Check that Podman is available and that the harness's disposable resources are the intended target.

   ```sh
   podman version
   ```

2. Check the harness controls without creating containers.

   ```sh
   task test-e2e-clone-unit
   ```

3. Run the owned PostgreSQL 16 to 16 clone fixture.

   ```sh
   task test-e2e-clone-16-16
   ```

This example qualifies only the checks recorded in its receipt.
A plain-clone result does not prove CDC, failover, power-loss durability, or operator support.

## Connections and private state

For separately admitted database work, use explicit source and target settings.
`--source` overrides `PGCOPYDB_SOURCE_PGURI`.
`--target` overrides `PGCOPYDB_TARGET_PGURI`.
The executable checks the effective connections after those overrides.
Stock libpq resolves service files, `PGPASSFILE`, and TLS settings.
Use private service and passfile files and the intended server certificate validation policy.
Keep credentials out of shell history, argv, logs, reports, and version control.
Do not paste real connection strings into retained evidence.

Use a fresh private native workdir for each new migration.
Never point native commands at pgcopydb state or reuse a foreign Migration UID or PVC.
The [operator guide](operator-guide.md) defines caller identity and activation gates.
The [troubleshooting guide](troubleshooting.md) explains refusals without deleting recovery evidence.
