# rpgmigrate

rpgmigrate is a Rust PostgreSQL migration engine for database operators.
The executable implements plain clone and pgoutput follow on PostgreSQL 16 to 16, 18 to 18, and 16 to 18; the [roadmap](docs/roadmap.md) records verified engine workflows and remaining cutover, operator, and durability gates.

[TOC]

## Features

1. **Connection checks:** `ping` checks explicit source and target connections through stock libpq.
2. **Plain clone:** restore schema, stream parallel text COPY, restore post-data, synchronize sequences, and analyze.
3. **Native receipts:** bind target ownership and copy generation, and publish completion only after durable target proof.
4. **Verification:** inspect measured progress and compare schema and full table rows through read-only commands.
5. **Follow:** receive pgoutput v1, replay committed transactions, and resume within the admitted recovery profile.

## Structure

```sh
.
├── Cargo.toml    # One unpublished Rust package
├── Taskfile.yml  # Local checks and isolated database tasks
├── Dockerfile    # Native runner packaging
├── src/          # Private engine modules and executable entry point
├── bench/        # Historical benchmark evidence
├── tests/        # Contracts, fixtures, and database checks
├── tools/        # Task bootstrap, portable checks, and database harness
├── docs/         # Roadmap, approved design, and operator contract
└── tasks/        # Milestone evidence and user corrections
```

## Getting Started

1. Install the Rust toolchain required by `Cargo.toml`, Python 3, pkg-config, and stock libpq development files.
   Use the [Testing guide](docs/testing.md) for library and database-tool selection.
   Confirm the checkout before running any database checks.

   ```sh
   git status --short --branch
   ```

2. Install the pinned Task binary locally.

   ```sh
   python3 tools/bootstrap-task.py
   export PATH="$PWD/target/tools/task/3.54.0/bin:$PATH"
   ```

3. Run the checks that need no database server.

   ```sh
   task check lint test
   ```

## Commands

| Command | Purpose |
| --- | --- |
| `task help` | List available tasks. |
| `task check` | Check every Rust target. |
| `task build` | Build the executable. |
| `task fmt-check` | Check Rust formatting. |
| `task lint` | Check formatting and Clippy warnings. |
| `task test` | Run Rust, CLI, and portable tooling checks. |
| `task test-db-self-check` | Check the independent data oracle. |
| `task test-transport` | Run isolated live transport and authentication checks. |
| `task test-db` | Run retained live profiles with their existing admission requirements. |
| `task test-clone` | Run seven isolated native clone component cases. |
| `task test-schema` | Run the isolated pre-data restart guard case. |
| `task test-e2e-clone` | Run the three declared container clone pairs. |
| `task image:build` | Build the local native runner image. |
| `task image:smoke` | Inspect the local image and emit a packaging receipt. |

## Testing

See the [Testing guide](docs/testing.md).

## Roadmap

See the [roadmap](docs/roadmap.md).

## Operator compatibility

See the [Operator compatibility contract](docs/operator-compatibility.md).

## Design

See the [approved design](docs/superpowers/specs/2026-11-06-rpgmigrate-design.md).

## Notices

See the [third-party notices](NOTICE).
