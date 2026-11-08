# Native8 real-utility qualification plan

**Status:** R2 for Parent review and assigned reviewer recheck; not accepted, with no implementation or PG lease.
**Goal:** Qualify the internal real-snapshot, PRE-DATA, nonempty COPY and POSTDATA path with genuine phase proofs.
**Architecture:** Use existing private APIs, one held opaque archive and the fenced target session in a separate live-test profile; preserve the library/thin-main architecture.
**Tech stack:** Existing Rust/libpq, Python owned fixtures, pinned PostgreSQL utilities and container recipes.
**Spec:** [Approved postdata contract](2026-11-08-schema-postdata.md), including content identity and existing-only acquisition.
R2 addresses design review `3319bfdd`; the preserved R1 is `target/verify/native8-qualification-plan-r1-a0e39.md`, SHA-256 `a0e39a4ddc3bbc4559c91c0703db2ade3a122b790d28eff81a55a5c5aaa93dd0`.

## Candidate and gates

Use frozen source198 `766617d4`, reviewed by A `69d0bf36` and B `22671e87`, plus the approved two-path example wrapper in source199 `71c418b0`.
Include the separate existing-only prose addendum `72460b8b` and exact reviewed timeline-query hunks, preserving final TEXT/OID25, masks and guards.
Frozen `src/snapshot/publication.rs:446` still uses `pg_catalog.bigint`; its `int8` correction is required by real pgoutput admission.
Use packet `05758dc8`, full four-path patch `3daba182` and reviews `60b33c27`/`78c6fe13`; read-only application checks pass on `766617d4`.
Do not substitute a whole Native6, E7 or Native7 diagnostic source overlay.

`task check` and `task test` passed on the wrapper candidate; strict `task lint` failed with 508 normal-library errors: 504 dead-code diagnostics and four unused-import groups containing sixteen bindings.
The graph confirms genuine unwired consumers; CLI/coordinator integration and unused-import cleanup remain merge blockers, with exact baseline delta unknown.
Do not move modules, widen visibility, suppress warnings or add cosmetic calls to claim green.
Internal qualification must retain this failed gate explicitly; it cannot clear merge, activation or global readiness.

## Declared versions and tools

| Source | Target | Matching dump/restore pair | Explicit `pg_config` |
|---|---|---|---|
| 16.15 | 16.15 | 16.15 | `/opt/e1/pg16.15/bin/pg_config` |
| 18.6 | 18.6 | 18.6 | `/opt/e1/pg18.6/bin/pg_config` |
| 16.15 | 18.6 | 18.6 | `/opt/e1/pg18.6/bin/pg_config` |

Use the two pinned images and executable identities in the [available-bundles contract](2026-11-08-schema-postdata.md#available-utility-bundles), receipt `be5ef51f`.
Retain the existing exact-version checks and source-major <= client-major <= target-major rule; never select a fallback from an observed server version.
Freeze independent source16.15 and target18.6 contexts, each with its own controller, cluster/system/database identity, expected version, roles, sessions, PIDs and owned socket paths; the utility context is separately 18.6.
Use the PG18 runner and mount only the owned PG16 source socket directory, with explicit controller-to-runner path translation, matching UID and verified 0700 directory/socket isolation.
Keep private PID/network namespaces and existing owned controllers; add no TCP route, broad shared mount or binary copying.
Bind each route to its full endpoint/system context: numeric database or role OIDs may coincide across clusters.
The new profile must reject swapped endpoints, wrong per-route versions/roles and missing/extra routes without deriving expectations from observed output.
Native20 supplies application libpq, not both utility bundles.

## Common pipeline and isolation

Each case gets a fresh Native8 run, source/target cluster pair, native root, scratch root and explicitly owned publication/slot.
Use C/UTF8 databases and one ordinary table with three fixed rows, an integer PK, a second unique-valued integer column and a text column with a plain secondary index.
Exclude sequences, LOBs, FKs, user callbacks, event triggers and unsupported types from this admitted fixture.
Use matching controlled owner role names for archive ownership statements, with actual per-cluster OID bindings.
Before the child starts, bootstrap only required source CONNECT/USAGE/SELECT, owned-publication creation, REPLICATION and exact control-function EXECUTE privileges; target ownership/CREATE and preinstalled PL/pgSQL USAGE remain separate.
Prove actual tested sessions are nonsuperusers without CREATEDB, CREATEROLE or BYPASSRLS, and that the target has no replication privilege; effects add no grants or extensions.
Admin metadata observations remain separate from tested-role evidence.

1. Create a genuine `SourceSnapshot::create_pgoutput` under the fixed text/publication profile and hold the imported snapshot and exporter.
2. Call `dump_schema` after its whole-profile source admission, then export PRE-DATA from that held `SchemaArchive`.
3. Call `acquire_for_clone` and `restore_predata`; require a genuine `SchemaProof` and actual target PK absence.
4. Publish through `publish_copy_plan_with_schema`, then call `copy_snapshot_with_schema`; require three equal rows, the complete anchor and a genuine `SnapshotProof` while the target PK remains absent.
5. Consume the same `PredataSql` through `export_postdata`, preserving the source receipt, archive digest and validated body/policy bindings; run or recover POSTDATA as specified below.

`pg_dump` produces the archive; `pg_restore --file=-` exports unchanged bodies for the same-session `DO`, without direct target writes.
Retain SQL and proofs until the retry/recovery decision.
Run CDC/frontier/checkpoint/used-authority controls on disposable authorities after releasing the current owner; never consume freshness or close the handle reserved for the intended retry/recovery.
Never start replication feedback, advance ACK, prune WAL, cut over or issue aggregate clone success.

On first effect and retained retry, release any external immutable sealed-plan reader before intent publication, then reopen against the updated held manifest and the same plan.
Preserve the provider's actual sequence at `schema_effect.rs:595-603`; never prebind a fabricated intent or reuse a stale reader across replacement.
Bind the existing `native8_release_reader_publish_reopen_and_refuse_changed_sealed_plan` and `native8_postdata_publication_and_idempotent_resync_failures_return_no_success` controls.
At the effect boundary, publication/reopen failures must close authority and withhold dispatch/proof while retaining inputs and actual checked state for the caller's decision; use existing provider fault/lifetime mechanisms, not a state rewrite.

## Smallest proposed live set

Run three selected cases on each declared version pair: nine invocations and nine fresh run identities.

- **`real_archive_copy_postdata`:** Acknowledged phases, valid final PK/layout/index health, equal rows and unchanged COPY anchor.
- **`postdata_rollback_retained_retry`:** Real POSTDATA rolls back after a controlled pre-COMMIT failure; fresh fenced authority retries the same retained intent/SQL.
- **`postdata_unknown_barrier_recovery`:** Withheld actual COMMIT reply yields Unknown; after backend release, existing-only acquisition recovers through a synchronous barrier without DDL replay.

In the positive case, make four separate owned target mutations after releasing the effect authority, each under the existing `Session` writer fence: missing PK, wrong present PK, extra physical column and a self-FK.
Prove each mutation completed, then require a disposable fresh `recover_postdata` to reach the intended missing/wrong-PK, extra-column or self-FK boundary, close and produce no new proof or row update.
Restore only the fixture object under the writer fence before the next control; unrelated early refusal is not a passing control.
Never edit system catalogs or completion rows to manufacture a state.

Parent selects one fixed test-only backend SQL failure, `SELECT 1 / 0`, through the existing query/error/close path on the same held connection after validated POSTDATA `DO` succeeds in transaction and before nonce CAS/COMMIT.
The original archive and SQL body remain unchanged; record real SQLSTATE `22012`, not a locally constructed error.
Key the one-shot hook by actual run, explicit POSTDATA phase, backend PID and provisioning nonce; PID/nonce alone cannot distinguish PRE-DATA.
Extend the existing scoped test mechanism without arbitrary SQL input or normal-build behavior; require disarmed, wrong-phase/PID/nonce, one-hit and scope-drop controls.
Observe completed DO, backend error and subsequent rollback/release, unchanged full metadata/rows/anchor, absent POSTDATA nonce and absent newly created PK/indexes.
Retain native intent, SQL and genuine prior proofs; require same-authority retry refusal, then exactly one identical retry on a new fenced authority.
This pre-COMMIT error is distinct from Unknown, and cleanup failure must not replace it.

Reuse `commit_hold`, but arm it only after acknowledged provisioning, PRE-DATA and COPY, immediately before the POSTDATA-only attempt, bound to its run/nonce/PID.
Require exactly one withheld server COMMIT tag, Unknown and no proof.
While that backend still holds the writer fence, attempt existing-only acquisition on a separately bound disposable contender with a one-second I/O deadline; prove actual fence contention and no metadata change, then reap it.
Read the complete committed row independently without treating readback as proof; release the relay and observe the old backend and lock disappear before fresh acquisition.
Choose committed recovery and close the SQL artifact, then use a new untouched existing-only authority for artifact-free `recover_postdata`.
Require positive synchronous COMMIT/Idle, exactly one new barrier, unchanged epoch/nonce/anchor/provenance/rows/object identities and zero POSTDATA dispatches.
Only after that successful proof is sealed, check that a second recovery on the consumed authority refuses; run other negative guards on separate disposable handles.
Unknown, held-fence contention and every expected refusal mint zero proofs.

Expected successful outputs are nine `SchemaProof`s, nine `SnapshotProof`s and nine `PostdataProof`s, the last split into six acknowledged effects and three barrier recoveries.
Expect three rolled-back POSTDATA attempts, three Unknown outcomes and twelve strict final-identity refusals.
Do not reuse Native6/7's twenty-witness count: the author must freeze the new exact native-open, utility/helper, role/PID and phase inventories from these call paths before live release.

## Refusal coverage boundary

The nine live runs use an admitted source profile; they do not qualify live source-profile rejection or pending-provenance reconciliation.
Bind the exact existing catalog source-profile negatives, `postdata_source_refusal_checks_entire_workset_before_archive_minting`, and A `69d0bf36`'s pending-acquisition controls as offline gates, including missing/malformed evidence, unchanged rows and no provisioning/repair on refusal.
Keep present-PK, ordinary COPY and WAL admission strict.
Do not claim repeated lost recovery COMMITs, lost-input restart, complete definitions or aggregate authority from these tests.

## Write-set and implementation order

Parent assigns owners after design review.

- [ ] Root mechanically composes the reviewed query/wrapper/prose hunks and freezes the exact source and API inputs without rerunning old full suites for handoff.
- [ ] An xhigh test author adds `src/apply/schema_postdata_live_tests.rs`, its test-only checkpoint registration and the scoped POSTDATA backend-failure/observation control in `src/apply/schema_effect.rs` / `schema_effect_observation.rs`.
  Xhigh is appropriate for rollback, proof lifetime and fence ordering; production algorithms and private API stay unchanged.
- [ ] A high harness author adds `tests/support/schema_postdata_fixture.py` and `test_schema_postdata_harness.py`, with one explicit three-case inventory consumed by discovery, protocol checks and a separate `tools/test-db.py` profile.
  Root owns minimal registration in `tests/support/db_harness.py`, Task/CI inventory consumers and affected schema/testing/harness docs; shared capture behavior stays unchanged.
- [ ] Test missing/extra cases, malformed types, wrong routes/versions, zero proofs, unsafe capture, nonzero status and cleanup failure; independently review the composed call paths and controls.
  Bind exact reviewed shared runner/startup/privacy hunks where reached; the unresolved Native7 callback oracle is not an accepted dependency.
- [ ] Freeze source, CLI/libtest, SQLite, loaded Native20, both images/tool binaries, arguments, route counts, byte caps and deadlines; actual fresh Linux no-run/discovery and offline compiled controls precede any Root lease.

Proposed bounds are an absolute 270-second engine deadline, 300 seconds per child, 900 per case command and a 960-second host envelope reserving 60 seconds for cleanup.
Check capacity and remaining budget immediately before each case, then execute without a manager roundtrip inside the window.
Stop the sequence at the first unexpected runtime, identity, proof or privacy failure; do not retry or continue to the next version.
Capture only admitted bounded proof fields, safe fixed codes and hashes before cleanup, preserving primary outcomes separately from cleanup/capture errors.
Across nine successful cases, prove cleanup of eighteen owned clusters, nine owned slot/publication pairs and nine native/scratch run pairs, plus every utility, helper, relay, socket, passfile and container actually created.
Seal admitted evidence first; close/reap clients and relays, clean exact owned slot/publication resources, stop/reap both clusters, remove private mounts/paths/passfiles and controllers, then verify absence and release.
Preserve checked cleanup after partial setup and primary failure even when another cleanup step fails.
Missing evidence fails acceptance.

## Scope and review decisions

Parent and the same assigned reviewer must accept R2's concrete backend fault, authority lifetimes and dual-context contract before implementation.
Native7 remains synthetic-source/fake-utility negative and on hold; its `SQL_ORACLE_REFUSED` history supplies neither a cause nor a Native8 result.
Complete object-definition comparison, FK support, sequences/LOBs, restart inputs and operator clone activation remain separate gates.
