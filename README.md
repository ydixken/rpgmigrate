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
├── Cargo.toml     # One unpublished Rust package
├── src/          # Private engine modules and the executable entry point
├── native/libpq/ # Pinned source, reviewed patches, and dependency notices
├── bench/        # Isolated persistence experiments
├── tests/        # Contracts, fixtures, and database checks
├── tools/        # Controlled native build and isolated database harness
├── docs/         # Guides, approved design, and implementation plans
└── tasks/        # Maintained status and lessons
```

## Getting Started

1. Prepare the [build prerequisites](docs/getting-started.md#prerequisites), then build the controlled native dependency.

   ```sh
   make native
   ```

2. Compile and run the checks that need no database server.

   ```sh
   make check lint test
   ```

3. Run real connection and COPY checks against the harness's disposable PostgreSQL server.

   ```sh
   make test-db
   ```

## Commands

| Command | Purpose |
| --- | --- |
| `make native` | Build the pinned, patched libpq artifact. |
| `make native-check` | Verify the selected native artifact. |
| `make check` | Check every Rust target. |
| `make fmt-check` | Check Rust formatting. |
| `make lint` | Check formatting and Clippy warnings. |
| `make test` | Run Rust, CLI, native-build, and harness unit checks. |
| `make test-db-self-check` | Check the independent data oracle. |
| `make test-db` | Run isolated live engine and executable checks. |

## Limits

See the [first-slice limits](docs/getting-started.md#first-slice-limits).

## Testing

See the [Testing guide](docs/testing.md).

## Storage experiments

See the [bounded storage pilot](docs/getting-started.md#storage-pilot).

## Durability

See the [durability design and runtime gates](docs/durability.md).

## Compatibility

See the [CLI compatibility guide](docs/cli-compatibility.md).

## Design

See the [approved design](docs/superpowers/specs/2026-11-06-rpgmigrate-design.md).

## Status

See the [task status](tasks/todo.md) for review evidence and unfinished migration, operator, and release work.

## Notices

See the [third-party notices](NOTICE).
