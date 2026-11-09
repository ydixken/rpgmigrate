# Testing

We keep local component checks separate from native migration and operator qualification.
The [roadmap](roadmap.md) defines the version matrix and milestone gates; the [task ledger](../tasks/todo.md) records observed results.

> [!IMPORTANT]
> Component checks qualify no operator migration or complete PG16/18 clone/follow workflow.
> The M2 container harness checks plain clone; the released runner image belongs to M5, and operator qualification belongs to M7.

## Prerequisites

Install the Rust toolchain required by `Cargo.toml`, Python 3, pkg-config, a C toolchain for bundled SQLite, and stock libpq development files.
The native release profile uses stock PGDG libpq and PostgreSQL client tools 16 and 18.
Library selection uses pq-sys/pkg-config, with optional `PQ_LIB_DIR` for an explicit existing library directory and `PKG_CONFIG_PATH` for pkg-config discovery.
Do not select an old `target/native` artifact or require a private capability receipt.
Keep SQLite build overrides unset because the bundled SQLite configuration is validated by `build.rs`.

1. Confirm the checkout and install the pinned Task binary.

   ```sh
   git status --short --branch
   python3 tools/bootstrap-task.py
   export PATH="$PWD/target/tools/task/3.54.0/bin:$PATH"
   ```

2. Run the local gate without a database server.

   ```sh
   task check lint test
   ```

`task lint` runs formatting and strict Clippy.
`task test` covers retained Rust unit tests, CLI contracts, and portable Python tooling/harness checks.
Test discovery must be nonempty; missing tests or skipped required cases are failures.
Plain CI runs this local gate with stock PGDG libpq 18 development files, prints actual versions, and builds no custom libpq or runner image.

## Isolated database checks

The retained `tools/test-db.py` harness owns disposable PostgreSQL clusters and verifies test receipts and independent SQL fingerprints.
Install a complete server/client bundle, including `initdb`, `pg_ctl`, `postgres`, `psql`, and `pg_config`.
Select the bundle with `PG_BINDIR` or `PG_CONFIG`; the harness also uses its established platform discovery when neither is set.
Select the matching stock client library separately when validating a specific libpq major.
Record the actual server and client versions; an unavailable bundle is an unmet check, not passing evidence.
Do not infer the later PG16/18 COPY admission or follow behavior from transport checks.

1. Check the harness's negative controls against its disposable server.

   ```sh
   task test-db-self-check
   ```

2. Run transport, connection, and passfile/service authentication checks.

   ```sh
   task test-transport
   ```

3. Run the native clone component profiles against the selected disposable PostgreSQL 16 or 18 bundle.

   ```sh
   task test-prepared test-catalog test-clone test-schema
   ```

The authentication matrix retains valid, insecure, nonregular, and wrong-password passfile cases through direct and service connections.
Pipeline checks enforce 64 commands and one MiB of queued input, validate terminal results through sync, and close dropped or interrupted pipelines.
Pipelines validate noncommitting query/DML windows; an observed COMMIT tag drains through sync and closes with refusal.
Transaction completion uses the observed-commit path after deferred actions have been forced and warning visibility pinned; any warning leaves an unknown outcome requiring independent durable resolution.
The new COPY path flushes 64 KiB batches and admits at most 16 MiB per libpq row; this does not bound libpq's internal allocation.
Stock libpq may emit its own stderr warnings; authentication and child completion remain the checks.
Database checks do not establish power-loss durability, target failover safety, or operator E2E support.
If `CARGO_TARGET_DIR` selects a separate build directory, set `RPGMIGRATE_BIN` to its emitted executable for CLI checks.
Use `task help` for retained focused profiles, including prepared execution, replication, and target authority.

## Container clone checks

The qualified alpha evidence consists of staged PostgreSQL 16 to 16, 18 to 18, and 16 to 18 pair runs, each passing eleven cases and six actual Go report validations.
The final staged test receipts passed 417 Rust tests, 24 CLI tests, and 183 portable tests; the aggregate Task commands were not rerun to exit zero after earlier failures.
Strict lint remains held on staged unused code, and no operator-controller, CDC, power-loss, or failover qualification follows from this alpha.

The M2 harness uses podman by default.
Set `CONTAINER_RUNTIME` to a Podman-compatible executable when it is not named `podman`.
The verified harness and image smoke use Podman-specific options; Docker is not qualified by these commands.
Its development image contains the pinned Rust base plus stock PGDG clients 16 and 18 and stock libpq; it is separate from the M5 released runner.
The fixture pins immutable PostgreSQL server image digests, and the receipt records resolved server/runner images, actual server/client versions, and the executable's actual linked libpq version.
Image setup and the first locked Cargo build require network access.
Database execution uses a disposable internal network with no published ports, two owned server containers, and fixture credentials held outside command arguments and receipts.
The harness removes its own containers and private network, and retains build caches and successful receipts under `target/e2e/`.
Failures retain redacted child stderr and safe stage/return-code/callsite metadata under `target/e2e/failures/`, using private 0700 directories and 0600 files.
Public failure output contains no child argv, SQL, row values, or credentials.

1. Check the harness controls without containers.

   ```sh
   task test-e2e-clone-unit
   ```

2. Run one declared pair.

   ```sh
   task test-e2e-clone-16-16
   ```

3. Run the exact three-pair matrix at milestone close.

   ```sh
   task test-e2e-clone
   ```

The matrix enumerates 16 to 16, 18 to 18, and 16 to 18; an absent case, missed fault, wrong version, wrong refusal reason, or exit-zero mismatch fails the check.
The harness creates a caller-owned Migration UID, run ID, workdir ID, and checkpoint generation before launching each migration, then passes those four values to the engine.
Default engine-only checks validate the narrow M2 report envelope, exact caller binding, successful command capability, and measured completion counters alongside the independent SQL oracles.
They do not embed or claim execution of the operator's general Report parser.
For cross-repository qualification, select an executable built against the operator's actual Go `ParseReport` with `--report-probe` or `RPGMIGRATE_E2E_REPORT_PROBE`.
The selected probe receives the caller binding written before launch and original report JSON bytes after removing only the transport prefix.
An explicit empty, absent, nonexecutable, or failing probe is a failed check with no local-validation fallback.
Receipts distinguish `local_m2_contract` from `operator_go_parse_report` and record the selected probe's hash.
The M2 closing matrix selects the authoritative probe; operator controller qualification remains an M7 gate.
Each pair exercises schema restoration, parallel text COPY, constraints/indexes/triggers, sequence state, completed resume, source writes after snapshot export, owned-data restart under a fresh generation, and explicit unsafe-boundary refusals.
Independent SQL checks inspect both peers and hash canonical full rows with duplicate multiplicity, NULLs, generated/identity output, and physical `ONLY` table semantics.
The schema oracle compares table NOT NULL through `attnotnull`, omitting only PostgreSQL 18's duplicate validated `pg_constraint` not-null records from the other-constraint stream.
Unvalidated not-null constraints refuse qualification; FK, CHECK, primary-key, index, and trigger evidence remains exact.
The fixture includes eight physical heaps, an empty table, wide rows, inheritance, and partition leaves, with bounds of 100,000 rows and 64 MiB of oracle output per table.
After creation, independent `ONLY` row counts must match the reviewed baseline, including 2048 wide rows, two duplicate parent rows, and one separately counted child row.
A target-byte mutation must fail the independent oracle and runtime comparison.
Snapshot tests compare the target with the source fingerprint captured before concurrent writes, then require a current-source comparison difference.
Utility wrappers delegate actual matching-major tools and record observed synchronization points before a controlled kill; the data-phase restart waits on the exact owned `LOCK TABLE ONLY` statement in ACCESS EXCLUSIVE mode, blocked by a harness-owned target SHARE lock after seven other tables committed.
This kills a partially completed COPY phase; it does not claim a pause inside the blocked table's COPY stream.

### Plain clone admission and recovery

The M2 implementation targets primary source/target databases on 16 or 18 with source major no greater than target major, and matching-target-major pg_dump/pg_restore.
Keep source table and namespace DDL quiescent during clone.
The source role needs visibility of every copied row, sequence read access, and permission to inspect the source identity/timeline; `row_security=off` makes hidden-row access fail.
The target role needs schema creation and ownership of the restored objects, native metadata, and copied tables, plus the required session-parameter privileges.
The disposable checks use fixture database owners; they do not qualify an operator least-privilege deployment.
Fresh targets must be collision-free apart from the empty-database baseline and exact owned native bookkeeping.
The first operator `--restart` may initialize an absent native path or an accepted private empty directory through the checked creation path.
Resume on an absent or empty path refuses; a resource-free native INIT state may retry only with its exact existing caller binding, without adopting foreign, legacy, or old-format state.

The admitted profile uses logged heap tables, inheritance children, and partition leaves exactly once, preserves explicit identity values, and computes generated columns on the target.
Generated output must match canonical source bytes under the imported snapshot before the table marker and COMMIT; a mismatch refuses and rolls back.
The 16 MiB row cap applies to the complete qualification row, including generated output, and that parity/cap path remains a PostgreSQL 16/18 closing qualification gate.
Foreign tables, materialized views, unlogged tables, large objects, extension data/configuration obligations, unsupported textual types, and zero-writable-column tables are preflight refusals.
Binary COPY, splitting, follow, filters, all-database/roles variants, destructive restore, and unsupported skip policies remain explicit refusals.
Table and post-data jobs default to four; explicitly conflicting index/restore job counts fail instead of ignoring a setting.
No-owner, no-ACL, no-comments, and no-tablespaces policies are passed through to the schema utilities.

Before post-data begins, restart or resume with `--not-consistent` obtains a new snapshot and COPY generation, invalidates old completion markers, and truncates only positively recorded owned data without CASCADE.
A lost positive pre-data ownership handoff refuses recovery and preserves the uncertain objects for manual review or a fresh migration.
Before advancing a restart generation, pre-data schema comparison under the new exported source snapshot refuses same-OID schema drift without target mutation.
An uncertain post-data boundary refuses recopy; a completed clone accepts `--resume --not-consistent` only after positive durable target proof, preserving its generation and committed marker rows, and refuses unsafe restart.
No path adopts or repairs pgcopydb state or foreign target objects.

Data comparison pins UTF8, ISO/YMD dates, PostgreSQL intervals, UTC, explicit float digits, and hex bytea output.
It compares complete canonical rows and multiplicity; checksum text is diagnostic output, and missing peers remain unknown with a difference result.
An established empty table is valid, but a comparison with no planned tables refuses because an empty array cannot prove coverage.
Comparison byte progress estimates table coverage from measured `pg_table_size`; it does not measure streamed bytes or throughput.
Progress inspection remains read-only while a worker holds its mutation lock; unknown counter fields are omitted instead of measured as zero.
Operational `native-report` framing requires the validated four-field operator identity bound to native state.
Standalone clone/progress emit unframed JSON with no fabricated Migration/PVC work identity.

## Local runner packaging

The M5 runner image is separate from the development E2E image.
It contains the executable, stock libpq 18, and PostgreSQL client tools 16 and 18, and runs as UID/GID 65532.
The smoke helper checks actual binary/library/tool versions, source-input manifest identity, notices, and writable `/work` and `/tmp` mounts with a read-only root filesystem.
It selects Podman's `--read-only-tmpfs=false` and owned tmpfs mounts so a default writable `/var/tmp` cannot masquerade as a read-only root check.
A local architecture's packaging receipt does not qualify another architecture, an operator workflow, or later source edits.

1. Build a local image tag.

   ```sh
   task image:build IMAGE=localhost/rpgmigrate:dev
   ```

2. Emit its packaging receipt as JSON.

   ```sh
   task --silent image:smoke IMAGE=localhost/rpgmigrate:dev
   ```

These commands build and inspect local images; publishing has separate release gates.

## Qualification boundaries

The revised native profile targets 16→16, 18→18, and 16→18 only.
The retained `test-db` and `test-engine` profiles keep their historical admission requirements and do not replace the native clone gate.
`task test-follow-snapshot` registers nine native pgoutput snapshot cases with five independent fault receipts for PostgreSQL 16 and 18.
The obsolete `test-source-snapshot` task, `--source-snapshot` selector, and PostgreSQL 14/17 legacy stage are retired.
Shared lower COPY helpers retain portable test coverage; those checks do not qualify PostgreSQL 14/17 runtime behavior.
Containers supplement the local harness as the engine/operator E2E layer, rather than replacing cargo live tests.
Run independent review and applicable Go suites once per milestone at close; engine-only changes have no operator Go suite.
Historical storage benchmarks remain evidence about their measured candidates, not qualification of the revised source-WAL design.
