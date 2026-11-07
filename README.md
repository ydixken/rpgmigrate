# rpgmigrate

rpgmigrate is a Rust PostgreSQL migration engine for database operators.
It uses a controlled, patched libpq dependency for connection checks and a restricted table-data COPY slice.

[TOC]

## Features

1. **Connection checks:** `ping` checks explicit source and target connections through libpq.
2. **Table-data COPY:** stream raw text into compatible, precreated empty tables in one target transaction.
3. **Native receipts:** publish completion after confirmed target COMMIT and refuse reuse of prior state.

## Structure

```sh
.
├── Cargo.toml    # One unpublished Rust package
├── Taskfile.yml  # Local checks, native builds, and container tasks
├── Dockerfile    # Runtime image for the implemented CLI
├── src/          # Private engine modules and the executable entry point
├── native/libpq/ # Pinned source, reviewed patches, and dependency notices
├── bench/        # Isolated persistence experiments
├── tests/        # Contracts, fixtures, and database checks
├── tools/        # Controlled native build and isolated database harness
├── docs/         # Guides, approved design, and implementation plans
└── tasks/        # Maintained status and lessons
```

## Getting Started

1. Prepare the [build prerequisites](docs/getting-started.md#prerequisites), confirm the checkout, and install the pinned Task binary locally.

   ```sh
   git status --short --branch
   python3 tools/bootstrap-task.py
   export PATH="$PWD/target/tools/task/3.54.0/bin:$PATH"
   ```

2. Build the controlled native dependency, then run the checks that need no database server.

   ```sh
   task native
   task check lint test
   ```

3. Run real connection and COPY checks against the harness's disposable PostgreSQL server.

   ```sh
   task test-db
   ```

## Commands

| Command | Purpose |
| --- | --- |
| `task help` | List the available tasks. |
| `task native` | Build the pinned, patched libpq artifact. |
| `task native-check` | Verify the selected native artifact. |
| `task check` | Check every Rust target. |
| `task fmt-check` | Check Rust formatting. |
| `task lint` | Check formatting and Clippy warnings. |
| `task test` | Run Rust, CLI, native-build, and harness unit checks. |
| `task test-db-self-check` | Check the independent data oracle. |
| `task test-db` | Run isolated live engine and executable checks. |

## Limits

See the [first-slice limits](docs/getting-started.md#first-slice-limits).

## Testing

See the [Testing guide](docs/testing.md).

## Containers

See the [container build and verification guide](docs/container.md).

## Storage experiments

See the [bounded storage pilot](docs/getting-started.md#storage-pilot).

## Storage results

See the [macOS arm64 pilot results](docs/research/2026-11-06-storage-pilot.md).

## Storage concurrency

See the [C1 role checks and qualification limits](docs/research/2026-11-06-storage-concurrency.md).

## Durability

See the [durability design and runtime gates](docs/durability.md).

## Compatibility

See the [CLI compatibility guide](docs/cli-compatibility.md).

## Operator compatibility

See the [Operator compatibility contract](docs/operator-compatibility.md).

## Design

See the [approved design](docs/superpowers/specs/2026-11-06-rpgmigrate-design.md).

## Status

See the [task status](tasks/todo.md) for review evidence and unfinished migration, operator, and release work.

## Notices

See the [third-party notices](NOTICE).
