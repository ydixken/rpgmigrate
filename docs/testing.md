# Testing

We keep local component checks separate from native migration and operator qualification.
The [roadmap](roadmap.md) defines the version matrix and milestone gates.
The [task ledger](../tasks/todo.md) records observed results.

> [!IMPORTANT]
> Component checks qualify no operator migration or complete PG16/18 clone/follow workflow.
> The M2 container harness checks plain clone.
> Production runner packaging belongs to M5, and operator qualification belongs to M7.

## Prerequisites

Install the Rust toolchain required by `Cargo.toml`, Python 3, pkg-config, a C toolchain for bundled SQLite, and stock libpq development files.
The native release profile uses stock PGDG libpq and PostgreSQL client tools 16 and 18.
Library selection uses pq-sys/pkg-config, with optional `PQ_LIB_DIR` for an explicit existing library directory and `PKG_CONFIG_PATH` for pkg-config discovery.
Do not select an old `target/native` artifact or require a private capability receipt.
Keep SQLite build overrides unset because the bundled SQLite configuration is checked by `build.rs`.

1. Check the checkout and install the pinned Task binary.

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
Test discovery must be nonempty.
Missing tests or skipped required cases are failures.
Plain CI runs this local gate with stock PGDG libpq 18 development files, prints actual versions, and builds no custom libpq or runner image.

## Isolated database checks

The retained `tools/test-db.py` harness owns disposable PostgreSQL clusters and checks test receipts and independent SQL fingerprints.
Its catalog profile includes a non-superuser money clone check across German and US monetary defaults.
That profile requires already installed GNU `localedef`, `/usr/share/i18n/locales/de_DE`, `/usr/share/i18n/locales/en_US`, and `/usr/share/i18n/charmaps/UTF-8.gz`.
It compiles only private locale files, establishes `LOCPATH` before starting the owned server, and passes that environment to the client utilities.
Missing tools, source data, or compiled `LC_MONETARY` output fail the check.
The harness installs no packages and changes no host locale archive.
Install a complete server/client bundle, including `initdb`, `pg_ctl`, `postgres`, `psql`, and `pg_config`.
Select the bundle with `PG_BINDIR` or `PG_CONFIG`.
The harness also uses its established platform discovery when neither is set.
Select the matching stock client library separately when validating a specific libpq major.
Record the actual server and client versions.
An unavailable bundle is an unmet check, not passing evidence.
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
Pipeline checks enforce 64 commands and one MiB of queued input, check terminal results through sync, and close dropped or interrupted pipelines.
Pipelines check noncommitting query/DML windows.
An observed COMMIT tag drains through sync and closes with refusal.
Transaction completion uses the observed-commit path after deferred actions have been forced and warning visibility pinned.
Any warning leaves an unknown outcome requiring independent durable resolution.
The new COPY path flushes 64 KiB batches and admits at most 16 MiB per libpq row.
This does not bound libpq's internal allocation.
Stock libpq may emit its own stderr warnings.
Authentication and child completion remain the checks.
Database checks do not establish power-loss durability, target failover safety, or operator E2E support.
If `CARGO_TARGET_DIR` selects a separate build directory, set `RPGMIGRATE_BIN` to its emitted executable for CLI checks.
Use `task help` for retained focused profiles, including prepared execution, replication, and target authority.

## Container clone checks

The [roadmap](roadmap.md) and [task ledger](../tasks/todo.md) record qualified clone runs and their exact input provenance.
Container clone checks do not qualify an operator controller, power-loss durability, or failover.

The M2 harness uses podman by default.
Set `CONTAINER_RUNTIME` to a Podman-compatible executable when it is not named `podman`.
The verified harness and image smoke use Podman-specific options.
Docker is not qualified by these commands.
Its development image contains the pinned Rust base plus stock PGDG clients 16 and 18 and stock libpq.
It is separate from the M5 production runner.
The fixture pins immutable PostgreSQL server image digests, and the receipt records resolved server/runner images, actual server/client versions, and the executable's actual linked libpq version.
Image setup and the first locked Cargo build require network access.
The harness does not pull images: pull the digest-pinned Rust base from `tools/e2e/Containerfile` and the PostgreSQL server images from `tests/fixtures/clone/cases.json` before a run, as the CI job does.
Database execution uses a disposable internal network with no published ports, two owned server containers, and fixture credentials held outside command arguments and receipts.
The harness removes its own containers and private network, and retains build caches and successful receipts under `target/e2e/`.
With a remote Podman service (the macOS Podman machine), the harness samples capacity and verifies the shared source inside the machine; with a local Linux Podman it does both on the host and samples the engine's storage root.
`RPGMIGRATE_E2E_CAPACITY` selects the container CPU caps, and the harness never picks them from host facts.
Unset selects `local`: 2 CPUs each for the build, source, target and runner containers.
`hosted-linux` keeps the build and runner at 2 CPUs and gives the source and target 1 CPU each.
Admission refuses a host with fewer CPUs than the source, target and runner caps together: 6 for `local`, 4 for `hosted-linux`.
Any other value is refused, and the runtime ledger and receipt record the selected profile.
We run CI with `hosted-linux` because the GitHub `ubuntu-26.04` runner has 4 CPUs.
The job runs there for Podman 5 and refuses an older Podman major before the harness starts.
That runner's pasta AppArmor profile denies the SIGTERM Podman sends at rootless network teardown, so the job puts that one profile in complain mode; we accept this only because the runner is discarded after the job.
The development image builds with `--layers=false`, because cached intermediate images would be unowned resources that the preservation check refuses.
Both profiles keep the same memory caps and memory and disk floors, because that runner reports about 14 GiB free memory and about 86 GiB free on the Podman storage root.
Failures retain redacted child stderr and safe stage/return-code/callsite metadata under `target/e2e/failures/`, using private 0700 directories and 0600 files.
Public failure output contains no child argv, SQL, row values, or credentials.
On failure, the engine-e2e job prints a cleanup summary to the public log, and nothing else:

- runtime ledger statuses, error kinds and verification flags;
- the names of resources added, removed or changed between the baseline and after-run inventories;
- each failure record (command kind, return code, diagnostic id and callsite);
- for raw podman failures, the redacted stderr lines starting with `Error:` or `*`, cut to 300 characters;
- the last 20 AppArmor `DENIED` or `ALLOWED` kernel lines;
- the podman image list (ID, name, creation time and `io.rpgmigrate.e2e` label).

It never prints a runtime ledger itself or any other stderr.

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

The matrix enumerates 16 to 16, 18 to 18, and 16 to 18.
An absent case, missed fault, wrong version, wrong refusal reason, or exit-zero mismatch fails the check.
The harness creates a caller-owned Migration UID, run ID, workdir ID, and checkpoint generation before each migration.
It passes those four values to the engine.
Default engine-only checks check the narrow M2 report envelope, exact caller binding, successful command capability, and measured completion counters alongside the independent SQL oracles.
They do not embed or claim execution of the operator's general Report parser.
For cross-repository qualification, select an executable built against the operator's actual Go `ParseReport` with `--report-probe` or `RPGMIGRATE_E2E_REPORT_PROBE`.
The selected probe receives the caller binding written before start and original report JSON bytes after removing only the transport prefix.
An explicit empty, absent, nonexecutable, or failing probe is a failed check with no local-validation fallback.
Receipts distinguish `local_m2_contract` from `operator_go_parse_report` and record the selected probe's hash.
The M2 closing matrix selects the authoritative probe.
Operator controller qualification remains an M7 gate.
Each pair exercises schema restoration, parallel text COPY, constraints/indexes/triggers, and sequence state.
It also checks completed resume, source writes after snapshot export, owned-data restart under a fresh generation, and explicit unsafe-boundary refusals.
Independent SQL checks inspect both peers and hash canonical full rows with duplicate multiplicity, NULLs, generated/identity output, and physical `ONLY` table semantics.
The schema oracle compares table NOT NULL through `attnotnull`, omitting only PostgreSQL 18's duplicate checked `pg_constraint` not-null records from the other-constraint stream.
Unvalidated not-null constraints refuse qualification.
FK, CHECK, primary-key, index, and trigger evidence remains exact.
The fixture includes eight physical heaps, an empty table, wide rows, inheritance, and partition leaves, with per-table bounds.
Those bounds are 100,000 rows and 64 MiB of oracle output.
The fixture's `rows` heap also carries `citext`, a domain over it, a standalone composite type, and arrays of both, so pre-data must create the extension and text COPY must carry those values.
After creation, independent `ONLY` row counts must match the reviewed baseline, including 2048 wide rows, two duplicate parent rows, and one separately counted child row.
A target-byte mutation must fail the independent oracle and runtime comparison.
Snapshot tests compare the target with the source fingerprint captured before concurrent writes, then require a current-source comparison difference.
Utility wrappers delegate actual matching-major tools and record observed synchronization points before a controlled kill.
The data-phase restart waits on the exact owned `LOCK TABLE ONLY` statement in ACCESS EXCLUSIVE mode, blocked by a harness-owned target SHARE lock after seven other tables committed.
This kills a partially completed COPY phase.
It does not claim a pause inside the blocked table's COPY stream.

### M8 component profile

`task test-clone`, also called by `task test-db`, requires exactly ten named component tests and a matching libtest summary with no ignored or failed cases.
The profile retains the seven clone snapshot, COPY, and marker cases.
The new cases check actual binary COPY row counts for empty, single-row, and multiple-row streams and row preservation across split parts.
They also check initial copying of empty, binary, multi-chunk, and sparse large objects.
The Rust tests use the existing disposable empty source/target databases and construct their own fixtures.
The split component case checks complete row and distinct-key counts.
The direct CLI split profile below checks simultaneous workers and the operator's 536870912-byte split threshold.
The large-object component case checks initial content and retry.
Final re-sync, metadata policies, comparison, fault recovery, and cleanup require separate runtime receipts.

> [!IMPORTANT]
> Wiring component tests does not qualify the M8 workflow or activate `copy.binary_fallback`, `copy.split`, or `copy.large_objects`.
> Qualification requires the admitted 16:16, 18:18, and 16:18 matrix, including operator defaults and independent fault observations.
> Preserve the historical M2/M4/M7 receipts and their formats separately from fresh M8 native format 9 / metadata 7 inputs.

### M8 direct CLI workflow profile

The existing clone pair tasks accept `PROFILE=disabled` (the default) or `PROFILE=m8`.
A CLI value overrides the inherited `PROFILE` environment variable.
Empty or unknown values fail before the harness starts.
`CONTAINER_RUNTIME` retains the same CLI-over-environment precedence.
The matrix task forwards the selected profile through 16 to 16, 18 to 18, and 16 to 18 sequentially and stops at the first failure.

> [!WARNING]
> Standalone workflow results apply to the exact source recorded in each receipt and its cleanup proof.
> The retained component checkpoint passed ten Clone cases and one catalog case on PostgreSQL 16.15 and 18.6.
> Operator deployment and capability activation require the operator closing gates in the roadmap.

Use a clean committed candidate checkout and complete runtime admission before running these commands.
The M8 outer harness builds a debug executable from that source, measures its image and utility identities, and uses fresh owned resources.
It rejects the old Go report probe, so unset `RPGMIGRATE_E2E_REPORT_PROBE` explicitly.

1. Run the first pair in its admitted runtime window.

   ```sh
   env -u RPGMIGRATE_E2E_REPORT_PROBE task test-e2e-clone-16-16 PROFILE=m8 CONTAINER_RUNTIME=podman
   ```

2. Review its receipt and cleanup proof, then run the second pair in a new admitted window.

   ```sh
   env -u RPGMIGRATE_E2E_REPORT_PROBE task test-e2e-clone-18-18 PROFILE=m8 CONTAINER_RUNTIME=podman
   ```

3. Review its receipt and cleanup proof, then run the cross-major pair in a new admitted window.

   ```sh
   env -u RPGMIGRATE_E2E_REPORT_PROBE task test-e2e-clone-16-18 PROFILE=m8 CONTAINER_RUNTIME=podman
   ```

For an explicit receipt destination, use the existing Python interface after setting `M8_RUN_DIR` to a fresh private admitted directory.
The destination file must not exist.

1. Run the selected pair with its receipt path.

   ```sh
   env -u RPGMIGRATE_E2E_REPORT_PROBE CONTAINER_RUNTIME=podman python3 tools/e2e/clone.py --pair 16-16 --profile m8 --receipt "$M8_RUN_DIR/clone-m8-16-16.json"
   ```

The [case manifest](../tests/fixtures/clone/m8-cases.json), [fixtures](../tests/fixtures/clone/), and [profile producer](../tools/e2e/copy_features.py) define six serial cases for each pair:

- `binary_safe` requires unsplit binary COPY, matching independent row fingerprints, and empty large-object inventories.
- `binary_fallback` requires binary COPY for safe columns and whole-table text fallback for an integer-array table, with matching row fingerprints.
- `split_default` measures the heap against the 536870912-byte default threshold.
  It observes contiguous split ranges and simultaneous held backend PIDs.
  It compares source and target wire formats, counts, and row fingerprints.
- `large_objects` checks five objects, including sparse content and EOF, with owner, ACL, and comments preserved initially and after follow drain.
- `lo_only` proves empty physical user-table inventories and five matching large objects before accepting native comparison `[]`.
  It uses `--no-owner` and retains ACL and comment checks.
- `mixed` combines binary COPY, text fallback, and five large objects under `--no-owner --no-acl --no-comments`.
  It does not exercise the split fixture.

The three large-object cases require durable completion at the exact final follow boundary and an injected target mismatch that comparison must reject before restoration.
All cases require stable physical inventories and matching independent table fingerprints.
These profiles do not exercise interrupted binary relay, split restart, interrupted large-object copy, large-object reset/restart, or failure during final re-sync.
The [roadmap](roadmap.md) retains those recovery and operator closing gates separately.

### Plain clone admission and recovery

The M2 implementation targets primary source/target databases on 16 or 18 with source major no greater than target major, and matching-target-major pg_dump/pg_restore.
Keep source table and namespace DDL quiescent during clone.
The source role needs visibility of every copied row, sequence read access, and permission to inspect the source identity/timeline.
`row_security=off` makes hidden-row access fail.
The target role needs schema creation and ownership of the restored objects, native metadata, and copied tables, plus the required session-parameter privileges.
The disposable checks use fixture database owners.
They do not qualify an operator least-privilege deployment.
Fresh targets must be collision-free apart from the empty-database baseline and exact owned native bookkeeping.
The first operator `--restart` may initialize an absent native path or an accepted private empty directory through the checked creation path.
Resume on an absent or empty path refuses.
A resource-free native INIT state may retry only with its exact existing caller binding, without adopting foreign, legacy, or old-format state.

The admitted profile uses logged heap tables, inheritance children, and partition leaves exactly once, preserves explicit identity values, and computes generated columns on the target.
Generated output must match canonical source bytes under the imported snapshot before the table marker and COMMIT.
A mismatch refuses and rolls back.
The 16 MiB row cap applies to the complete qualification row, including generated output.
The historical M2 profile refuses foreign tables, materialized views, unlogged tables, large objects, extension data/configuration obligations, unsupported textual types, and zero-writable-column tables during preflight.
Its binary COPY, splitting, filters, all-database/roles variants, destructive restore, and unsupported skip-policy cases remain historical refusal checks.
Text COPY admits built-in types, enums, domains, arrays, standalone composite types, and extension base types, checking every domain base, array element, and composite member.
It refuses `reg*` types, `aclitem` and `pg_node_tree`, whose text is not portable, as well as table row types and base types that no extension owns.
An extension that owns a column type must be available on the target, or planning refuses before mutation.
Planning does not compare extension versions, because pre-data installs the target's default version; the 16 to 18 container pair runs `citext` across such a version change.
Binary COPY keeps its built-in scalar predicate, so a table with any of these columns falls back to text.
The M8 component and direct CLI profiles above cover the candidate implementation.
The separate follow profile below controls `clone --follow` qualification.
Table and post-data jobs default to four.
Explicitly conflicting index/restore job counts fail instead of ignoring a setting.
No-owner, no-ACL, no-comments, and no-tablespaces policies are passed through to the schema utilities.

Before post-data starts, restart or resume with `--not-consistent` obtains a new snapshot and COPY generation.
It invalidates old completion markers and truncates only positively recorded owned data without CASCADE.
A lost positive pre-data ownership handoff refuses recovery and preserves the uncertain objects for manual review or a fresh migration.
Before advancing a restart generation, pre-data schema comparison under the new exported source snapshot refuses same-OID schema drift without target mutation.
An uncertain post-data boundary refuses recopy.
A completed clone accepts `--resume --not-consistent` only after positive durable target proof, preserving its generation and committed marker rows, and refuses unsafe restart.
No path adopts or repairs pgcopydb state or foreign target objects.

Data comparison pins UTF8, ISO/YMD dates, PostgreSQL intervals, UTC, explicit float digits, and hex bytea output.
It compares complete canonical rows and multiplicity.
Checksum text is diagnostic output, and missing peers remain unknown with a difference result.
An established empty table is valid.
The historical M2 profile refuses comparison with no planned tables.
For an empty array, the M8 candidate requires empty physical user-table inventories on both peers after exact owned target exclusions.
Any required large-object comparison must also succeed.
Comparison byte progress estimates table coverage from measured `pg_table_size`.
It does not measure streamed bytes or throughput.
Progress inspection remains read-only while a worker holds its mutation lock.
Unknown counter fields are omitted instead of measured as zero.
Operational `native-report` framing requires the checked four-field operator identity bound to native state.
Standalone clone/progress emit unframed JSON with no fabricated Migration/PVC work identity.

## Follow component and container checks

We keep pgoutput checks separate from the historical test_decoding profiles.
The component profiles require exact nonempty inventories from the registered harness manifests.
M4 extends them with control, authority, drain, receiver shutdown, and cleanup regressions.
Discovery alone is not runtime evidence.
The default `task test-db` registers these five profiles after the schema profile and before `copy_integration`.
The obsolete `test-source-snapshot` task, `--source-snapshot` selector, and PostgreSQL 14/17 legacy stage are retired.
The source profile uses a real owned logical `OwnedCluster`, including its token, data directory, and configuration file.
Its durable-storage drift control cannot target an external server.
The authority profile preserves the synchronous-wait cancellation case where a visible committed row cannot authorize feedback.
It also checks that lazy checkpoint-only groups commit one durable frontier through both direct and pipeline authority paths without changing user rows.
The receiver profile requires its seventy-second blocked-apply status test exactly once.
The historical Engine profile excludes that exact native test.
The retained executable COPY integration checks accept receipts with an explicit integer native format of 8 or 9.
The completion output must carry the same integer state format version as the receipt.
The current snapshot profile requires nine exact passed cases plus five independent native pgoutput fault receipts.
The CREATE-tail receipt contains three required subphases.
Its owned Unix relay binds the actual source publication, slot, stream, an explicit native format 8 or 9 intent, and importer/exporter process identities.
Missing fields or phases, an unhit post-import PID mutation, or a late fault fail that receipt check.
A live backend, unreaped helper, or ambiguous creation retry also fails.
An explicit child `PGAPPNAME` supports the application-name case without changing the Rust process environment.
Direct unknown source-finish evidence proves no target anchor absence.
The coordinator call-order and process-level qualification remain separate evidence.
Select an actual PostgreSQL 16 or 18 server/client bundle and record the version variables as described above.

The 133-case M4 closing subset per major comprises Schema3, Clone7, Catalog13, FollowSource12, FollowSnapshot9, FollowAuthority17, FollowReceive11, FollowApply11, and Engine50.
The separately registered retained M3 gate, `task test-source-catalog`, requires SourceCatalog3 through `tools/test-db.py --source-catalog` with the `catalog::tests::live_source_identity_` selector.
Schema3 checks pre-data drift, effective service/authentication/TLS option preservation, and binding schema children to the checked database and selected peer.
Its owned TCP/SCRAM relay checks selected-peer loss with TLS disabled.
Option-byte evidence does not qualify TLS handshakes, client certificates, or encrypted keys.
FollowSnapshot additionally requires five fault receipts and three CREATE-tail phases.
The four adapter profiles, Transport, Prepared, Authority, and Replication, intentionally select subsets of Engine50.
They are not additional disjoint cases.
FollowReceive uses `--include-ignored --exact` with all eleven manifest names, because its required inventory includes ignored and nonignored tests.
Using only `--ignored` silently omits required receiver cases and fails the inventory check.
Engine excludes the native receiver and apply cases, snapshot profiles, source cleanup, clone, schema, and follow authority cases through the explicit Task selectors.

1. Run the portable follow harness controls.

   ```sh
   task test-e2e-follow-unit
   ```

2. Run the selected native component profiles.

   ```sh
   task test-follow-source test-follow-snapshot test-follow-authority test-follow-receive test-follow-apply
   ```

3. Debug one initial copy and DML path with its separately scoped smoke receipt.

   ```sh
   task test-e2e-follow-smoke-16-16
   ```

4. Run the full declared matrix at milestone close.

   ```sh
   task test-e2e-follow
   ```

The smoke profile requires exactly one observed `initial_copy_dml` case and writes `target/e2e/follow-smoke-16-16.json` with `receipt_scope=smoke`.
After a controlled stop, that case runs the engine's data comparison and requires a match for every planned table.
Matching `test-e2e-follow-smoke-18-18` and `test-e2e-follow-smoke-16-18` tasks check the other declared pairs.
A smoke receipt does not establish the full fault inventory.
The full profile requires every declared case for each of 16 to 16, 18 to 18, and 16 to 18, and writes `target/e2e/follow-<pair>.json`.
An absent case, unobserved synchronization point, missing FIFO-full event, or successful exit without durable cutover completion fails qualification.
Use the maintained task ledger for actual passed receipts.
These commands define checks, not a support claim.

The follow harness reuses the clone harness's immutable server images, private internal network, exact container cleanup, redacted failure metadata, and matching-target-major tools.
The harness takes the native format and target metadata it expects from `NATIVE_FORMAT` and `TARGET_METADATA` in `tools/e2e/follow.py`, and the cutover and M8 checks import them.
A portable test pins both to the constants in `src/state.rs`, so a format bump fails `task test` until someone reviews the harness.
It starts logical decoding only inside its disposable source container and checks the executable's actual linked libpq version.
Follow refuses a domain CHECK that any column reaches through domains, arrays, or composite members.
The separate positive fixture contains no generated columns, CHECK constraints, domain CHECK, expression or partial indexes, or user callbacks.
Its DEFAULT, INDEX, and keyless FULL identities cover changed keys, duplicate rows, NULLs, unchanged TOAST, TRUNCATE, explicit identity values, and differing source/target custom type OIDs.
The smoke DML inserts, updates, and deletes `citext`, domain, composite, and array values, including case-only changes.
A FULL-identity update picks one of two rows whose composite values differ only in numeric scale and `citext` case, which record equality treats as equal, so only an exact text match changes the right row.
The independent row and schema fingerprints retain the clone oracle's row/byte bounds and multiplicity checks.
The fixture's physical `ONLY` row counts must match its reviewed baseline before start.

A live data comparison requires three observations.
The exact logged target authority row must record an applied or certified frontier at the known source goal.
Captured forwarded status positions must reach that goal.
The actual owned source slot must consume that feedback through `confirmed_flush_lsn`.
The observer checks the native format, exact relation/schema/owner OIDs, complete stream and operator binding, primary identity, and `fsync=on` plus `full_page_writes=on`.
A copy anchor, received WAL header, row visibility, report, or process exit alone cannot satisfy this predicate.
The target COMMIT-reply fault captures an earlier proven frontier and requires feedback to stay there while the later committed row remains unresolved.
This controls the unknown-COMMIT boundary without treating visibility as proof of synchronous durability.
Failure diagnostics retain bounded transaction LSNs and available observations of the exact owned target checkpoint and source slot, without row payloads.
For a transaction beyond the copy anchor, feedback strictly greater than its COMMIT record LSN requires applied coverage through its transaction-end LSN.
A position inside that interval cannot pass.
Feedback equal to the COMMIT record LSN can describe prior-record progress.
Visible target observations check consistency and do not certify durability.

Private TCP gates pause a target COMMIT before dispatch, hold its observed successful response, or stop forwarding source frames after `BEGIN`.
The harness kills the background engine, releases the exact held connection, and resumes from the retained owned source slot.
The backpressure case requires the engine's first actual `follow.backpressure` event.
It keeps that FIFO full beyond the receiver's observed 60-second walsender timeout.
It checks unchanged safe status positions and the same source walsender, then releases apply and checks convergence.
The WAL-loss fault changes [slot retention](https://www.postgresql.org/docs/18/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE) only in the owned disposable source container.
It forces actual slot invalidation and positively observes unlimited retention again before checking the lost-slot refusal.
Foreign active slots/publications and unsafe source profiles must refuse before creating target objects or changing those source resources.

The copying-stage recovery case observes the exact blocked owned pre-truncate statement, advances the COPY generation on retry, and checks new positively recorded source resource identities.
Uncertain pre-data ownership and post-data outcomes remain explicit recovery refusals and open qualification gaps.
Retained Replication adapter checks use test_decoding.
They do not qualify native pgoutput follow.

Ongoing follow without admitted cutover stays in `streaming` and stops with a nonzero suspended/interrupted outcome.
Streaming reports preserve the caller binding and omit drain or successful follow completion evidence.
A selected `--report-probe` must accept the original streaming JSON bytes through the actual operator parser.
An unavailable or failing selected probe has no local fallback.
Receipts distinguish `local_m3_contract` from `operator_go_parse_report`.
M4 owns the requested end, final synchronization, successful follow exit, drain proof, and cleanup.
Process-kill receipts establish no power-loss, target failover, or operator-controller qualification.

## Cutover qualification

Read the [Native cutover handoff](native-cutover-handoff.md) before interpreting control rows or replication reports.
Its native-8/metadata-6 readers must reject older state without conversion.
Discovery retains `capabilities=[]` until the complete producer and milestone checks pass.
The [task ledger](../tasks/todo.md) records actual runtime receipts, unresolved startup/recovery failures, and required remaining checks.
No source inspection, pure contract test, or component receipt establishes a full follow matrix pass.

Require nonempty runtime receipts for every discovered case, exact versions, active writers, complete table/sequence inventories, and observed fault injections.
Cutover cases must stop ordinary and `synchronous_commit=off` writers and outstanding transactions before independently sampling and persisting E.
Require actual keepalive and later-`BEGIN` coverage, full data/schema/sequence comparison, cleanup, and comparison again from the retained workdir.
Exercise active-fence JSON refusal after E persists, worker completion without command acknowledgment, and the post-worker standalone helper's identical-E retry.
Refuse false drain based on asynchronous visibility, omitted cases, missed faults, and incomplete cleanup inventories.
Engine JSON does not replace the independent synchronous row barrier or actual operator Go validation.
The existing uncertain post-data recovery refusal remains a gap.
Neither completed-row observation nor later-`BEGIN` coverage repairs it.
Process-kill tests qualify their injected process failures, not power loss, target failover, or operator E2E.

1. Run one cutover producer pair.

   ```sh
   task test-e2e-cutover-16-16
   ```

2. Run the exact three-pair cutover producer matrix at milestone close.

   ```sh
   task test-e2e-cutover
   ```

The ongoing matrix requires all 21 cases and the cutover producer matrix all ten cases for each declared pair, totaling 93 required executions.
The cutover inventory includes `completed_resume_requires_checked_authority_close`, which requires completed resume to refuse when checked authority closure fails.
The cutover Task routes through `tools/e2e/follow.py --cutover --pair <pair>` and checks the nonempty cutover manifest.
Its receipts use `local_m4_producer` with `probe_checked=false`.
Actual engine/Go interoperability and the independent M6 reader are separate checks.
Run integrated lint/tests, independent safety review, and assigned Go suites once at milestone close against the same frozen handoff.

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

These commands build and inspect local images.
Publishing has separate release gates.

## Qualification boundaries

The revised native profile targets 16→16, 18→18, and 16→18 only.
The retained `test-db` and `test-engine` profiles keep their declared admission requirements and do not replace the native clone gate.
Shared lower COPY helpers retain portable test coverage.
Those checks do not qualify PostgreSQL 14/17 runtime behavior.
Containers supplement the local harness as the engine/operator E2E layer, rather than replacing cargo live tests.
Run independent review and applicable Go suites once per milestone at close.
Engine-only changes have no operator Go suite.
Historical storage benchmarks remain evidence about their measured candidates, not qualification of the revised source-WAL design.
