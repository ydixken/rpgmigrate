# Fenced pre-data restore plan

**Goal:** Apply admitted pre-data DDL and its logged completion receipt in the same fenced target transaction.

**Architecture:** Export SQL with the approved PostgreSQL utilities, validate their bounded outer framing, and execute one PL/pgSQL `DO` through the existing target connection.
Keep utility processes away from target writes.

**Tech stack:** Existing Rust/libpq transport, PostgreSQL 16/18 utilities, installed trusted PL/pgSQL, and existing native/target control boundaries.

**Spec:** The [approved design](../specs/2026-11-06-rpgmigrate-design.md#architecture-and-transport) requires PostgreSQL utilities, a target session fence, and fenced PRIMARY recovery for unknown COMMIT.
The parent approved the same-session direction and the versions below; individual source units and PG leases still require their assigned releases.

The frozen persistence boundary is fresh Native7 plus target metadata4, with older formats refused and no adoption or conversion.
CoreSchema3 remains unchanged, and the context V3/pgoutput text-profile correction remains a separate change.
`SchemaIntentV1` and the opaque `SchemaProof` are distinct from COPY intent and `SnapshotProof`.
Archive and SQL content identities are SHA-256 values represented by an opaque 32-byte `Digest`, not CRC32 plus length.
The parent approved `sha2 = { version = "=0.11.1", default-features = false }` after private dependency validation.
Add it only to a reviewable schema candidate with the actual caller; preserve all existing package versions and resolved features.
The manual RustSec snapshot check and host tests do not replace Linux builds or the integrated gates.

## Native execution check

Both [PG16 SPI](https://www.postgresql.org/docs/16/spi-spi-execute.html) and [PG18 SPI](https://www.postgresql.org/docs/18/spi-spi-execute.html) accept several SQL commands in one string.
PL/pgSQL dynamic `EXECUTE` uses `SPI_execute_extended` in the pinned [PG16 implementation](https://github.com/postgres/postgres/blob/e9c45974adb439fcc6fee6175e11f92619fc5361/src/pl/plpgsql/src/pl_exec.c#L4433) and [PG18 implementation](https://github.com/postgres/postgres/blob/1da6ddbdc32c5bf640429828bc0353ffcb40628e/src/pl/plpgsql/src/pl_exec.c#L4440).
Without `INTO`, results are discarded by PL/pgSQL; the client receives the outer `DO` result.
The existing `PgConnection::query` already dispatches one command through `PQsendQueryParams` and fully drains it.
Prefer this path over a new multi-result transport operation for the admitted transactional pre-data profile.

The [PG16 DO](https://www.postgresql.org/docs/16/sql-do.html) and [PG18 DO](https://www.postgresql.org/docs/18/sql-do.html) rules forbid transaction control when the block runs inside an explicit transaction.
SPI also rejects transaction statements and client `COPY`; dynamic `SELECT INTO` is unsupported, while `CREATE TABLE AS` is supported.
Ordinary transactional `CREATE SCHEMA`, type/domain/function/procedure/table/sequence/view definitions, `ALTER`, comments, ownership/ACL statements, `SET`, and the dump's scalar `set_config` calls use the normal utility/query machinery.
We found no SPI-specific blocker for that ordinary pre-data set.
Commands that require execution outside a transaction, client COPY, psql commands, and unqualified extension behavior are outside this unit.
No generic SQL classifier or SQL splitter is proposed.

> [!important]
> This removes client tuple-result buffering, not server memory consumption.
> PL/pgSQL leaves SPI's destination unset, so [SPI can accumulate result tuples](https://www.postgresql.org/docs/18/spi-spi-execute-extended.html) before freeing them.
> Bound the input and escaped command allocations, retain cancellation/deadlines, and do not claim an RSS bound or support arbitrary result-producing scripts.

PL/pgSQL is normally installed, but admission must find exactly one installed, trusted language with an inline handler and effective `USAGE` privilege.
Require the preinstalled `plpgsql` 1.0 extension in `pg_catalog` and exact `pg_depend` membership for that language.
A re-added source `plpgsql` can produce `CREATE EXTENSION IF NOT EXISTS`; checking only the target language would permit an extension installation, so target extension preexistence is part of the no-install boundary.
An absent or malformed language result is refusal, not permission to install a language, extension, or grant.
Require selected extension dependencies to be preinstalled and qualified before execution; refuse missing dependencies without installation or grants.
`IF NOT EXISTS` alone does not prove an existing extension matches the requested one.
The initial ordinary pre-data profile checks installed source extensions on the held imported snapshot before the dump utility runs.
Admit an empty extension inventory or only `plpgsql`; refuse any other extension or query error without releasing source ownership or changing snapshot lifetime.
This is an explicit restriction of the limited alpha profile, not support for arbitrary preinstalled extensions; do not inspect SQL text to infer extension safety.
[Extension scripts](https://www.postgresql.org/docs/18/extend-extensions.html) already prohibit transaction control and commands such as `VACUUM` that cannot run in a transaction.
SPI support therefore does not certify arbitrary extension code or authorize automatic extension installation.

## Unit 1: utility export and exact framing

Owner paths: `src/clone_phase.rs`, `src/clone_phase/process.rs`, and their focused tests.
Released export API: `SchemaArchive::export_predata(self, receipt: &SourceSlotReceipt, io: &IoControl) -> Result<PredataSql>`.
Opaque `PredataSql` owns the existing `SchemaArchive`, immutable body bytes, and a private profile containing UTF8, the captured `standard_conforming_strings` boolean, and the actual utility version.
Read-only body/source/profile/version/policy accessors confer no effect or completion authority; there is no arbitrary-SQL constructor.
Retain the archive's held file and scratch owner through this artifact's lifetime; do not clean it early and retain only its CRC.
This export unit does not add crypto or persist state.
The subsequent approved digest caller hashes the archive's exact held bytes and the exact retained SQL interior after framing removal as separate fields; the latter is the unmodified SQL supplied to `EXECUTE`, excluding the generated DO wrapper.

- [ ] Keep the existing custom archive containing pre-data and post-data metadata, and select only pre-data for this phase.
- [ ] Run `pg_restore` in export-only mode, with no database destination, create-database, single-transaction, transaction-size, or session-authorization switching option.
- [ ] Feed the held archive through stdin, clear the inherited environment, use a fresh restrict key, and cap SQL output at 16 MiB independently of compressed archive size; overflow or truncated output is refusal.
- [ ] Require actual qualified PG16/18 utility versions and a compatible source/utility/target combination; do not silently run PG18's preamble against PG16.
- [ ] Validate the exact versioned outer restrict/unrestrict framing with the run's fresh key; remove only those outer lines and preserve all interior bytes.
- [ ] Reject missing, truncated, excessive, malformed, or empty export evidence; a verified empty catalog uses an explicit no-work path, never inferred empty-output success.
- [ ] Test quotes, backslashes, Unicode, nested dollar-quoted function bodies, false framing inside SQL literals, changed archive identity, cancellation, and withheld utility diagnostics.
- [ ] Test the held-source extension admission with zero extensions, only `plpgsql`, a foreign extension, and a query error before any dump utility or target effect.

The approved endpoint correction remains an input to this unit; export success is not target DDL or clone completion.

## Unit 2: one statement on the held effect session

Owner paths: `src/apply/checkpoint.rs` and focused authority tests; reuse `src/pg/ffi.rs` without a new transport API.
Selected future caller: `TargetAuthority::restore_predata(&mut self, native: &mut NativeState, sql: &mut PredataSql, io: &IoControl) -> Result<SchemaCommitOutcome>`.
This phase-specific operation owns its transaction internally and does not expose a generic arbitrary-SQL callback.
It obtains the authority's actual confirmed target provenance, revalidates and hashes the held artifact, and durably binds the native intent before DDL.

- [ ] Require the existing authority to retain its database writer fence on the actual effect connection; revalidate native intent, target provenance, archive/SQL identity, empty CDC checkpoint, and absent COPY anchor before beginning the owned transaction.
- [ ] Check the installed language, its exact preinstalled `plpgsql` extension membership, and required privileges; set and verify UTF8 and the captured `standard_conforming_strings` value before quoting or parsing the exported script.
- [ ] Admit the ordinary preinstalled extension and refuse absent extension, absent membership, wrong version, or wrong namespace before `DO`, with pinned PG16/18 dump provenance and regression evidence.
- [ ] Require the controlled dump preamble to agree with that parser profile; do not rely on an inner `SET` to change how an already parsed string was interpreted.
- [ ] Use existing `quote_literal`/`PQescapeLiteral` twice: quote the SQL as the `EXECUTE` argument, build the fixed `BEGIN EXECUTE ...; END` body, then quote that body as the literal for `DO LANGUAGE plpgsql ...`.
- [ ] Reject NUL, invalid/unsupported encoding, and input or expanded-size overflow before dispatch; never choose a delimiter by scanning arbitrary SQL or concatenate unquoted content.
- [ ] Execute through existing `query`, require the `DO` command result and `InTransaction`, and preserve sticky failure for local construction, dispatch, SQL, cancellation, or validation errors.
- [ ] Restore and verify the authority's role/search-path/session/durability contract before the completion update and final COMMIT; a script's timeout settings do not disable the external `IoControl` deadline.
- [ ] Test dependent DDL plus scalar SELECT, quotes/function bodies, a late error rolling back all DDL, inner transaction/COPY refusal, missing language/privilege, cancellation, lost fence/backend, and schema/receipt atomicity on both PG16 and PG18.

No exception handler swallows a script failure, no temporary helper function is created, and no new COMMIT occurs inside the block.
No live DO qualification has run for this plan.

## Unit 3: phase intent, completion, and reconciliation

The released persistent-state unit owns `src/state/schema.rs`, `src/state.rs`, `src/apply/mod.rs`, `src/apply/metadata.rs`, and their focused tests in its private candidate.
It may add the new field to existing `ControlRow` test literals, but must not implement target effects or proof construction in `checkpoint.rs`.
The later effect unit and real clone coordinator have separate ownership.
The frozen `SchemaIntentV1` has exactly eight fields: `intent_version: u32` equal to 1, internally minted nonzero `nonce: [u8; 16]`, `source: SourceSlotReceipt`, `target: ConfirmedProvenance`, `archive_sha256: Digest`, `sql_sha256: Digest`, `policy_version: u32` equal to 1, and `standard_conforming_strings: bool`.
Each digest is exactly 32 bytes.
The full source receipt and the actual snapshot binding retained and rechecked by `SchemaArchive` and `PredataSql` supply the source binding.
No `CopyPlanRef` has been published at this stage; do not add one, duplicate source identities, invent a snapshot-plan wrapper, or reorder phases to obtain one.
Utility versions, byte limits, and actual snapshot checks belong to the sealed export policy, not duplicate intent fields.
The SQL digest binds the exact retained body; framing validation and the fixed policy version define how that body is admitted.
UTF8 is the policy invariant; preserve full source/target/native-run equality and the existing nonzero snapshot point.
Native7 adds a required-nullable `schema_intent: Option<SchemaIntentV1>` with unknown-field refusal.
`NativeState::begin_schema_intent(&mut self, source: &SourceSlotReceipt, target: &ConfirmedProvenance, archive_digest: Digest, sql_digest: Digest, standard_strings: bool) -> Result<SchemaIntentV1>` mints the nonce internally and durably returns the exact idempotent intent; different bindings cannot replace it.
`checked_schema_intent(&self) -> Result<Option<SchemaIntentV1>>` uses the existing checked-manifest discipline.
Metadata4 adds only `ControlRow::predata_nonce: Option<[u8; 16]>`, stored as nullable `bytea`, with exact length and nonzero validation when present; every full-row read, equality check, and CAS preserves it.
Reuse the existing `barrier_counter`; do not store another full intent, phase counter, utility version, or length in the target row.
Preserve full immutable intent/native/source/target equality, namespace and row guards, and stage-specific null checks without advancing the CDC frontier or COPY anchor.
The target control row stores the completed pre-data nonce in the same transaction as DDL and the existing logged barrier increment.
`SchemaCommitOutcome` is `Acknowledged(SchemaProof)` or `Unknown`; the private proof binds the exact intent and existing durable evidence, and is distinct from `SnapshotProof`.
The future `recover_predata(&mut self, native: &NativeState, io: &IoControl) -> Result<SchemaProof>` requires fresh fenced PRIMARY reacquisition, exact matching logged nonce/bindings, and a real writing barrier; absent or mismatched completion refuses.
The existing COPY entry must require an exact matching intent, logged nonce, and `SchemaProof` for clone COPY.
The existing COPY-only path requires absent native schema intent, absent logged pre-data nonce, and no schema proof; all mixed or mismatched combinations refuse.
A bare nonce/readback cannot authorize clone COPY.

- [x] Freeze Native7 and target metadata4 with required-nullable schema state, explicit old-format refusal, and unchanged CoreSchema3; do not reinterpret Native6 qualification.
- [x] Select SHA-256 with an opaque 32-byte digest; no CRC substitution, handwritten crypto, or OpenSSL subprocess.
- [x] Validate and approve the exact SHA-256 dependency: seven additive packages, registry graph 31 to 38, no existing version/checksum/resolved-feature changes, MIT/Apache-2.0 licensing, and maximum declared MSRV 1.88 below the package's 1.99.1.
- [ ] Qualify the released state unit's required-nullable intent, exact retry, stage refusals, nonce-only metadata read/CAS, and old Native6/metadata3 refusal without minting `SchemaProof`.
- [ ] Apply the approved private manifest/lock delta with the actual schema digest caller and qualify Linux builds; automated cargo-audit/cargo-deny were unavailable, and the manual RustSec snapshot is `309392b18f6b17022f82c701f300433126901068`, reviewed at `2026-11-07T22:07:44Z`.
- [ ] Reuse the authority's checked control-row update and always-synchronous finish path, committing DDL, nonce, and barrier together without inventing a CDC frontier or COPY anchor.
- [ ] After unknown COMMIT, close the connection and withhold proof/success/COPY authorization; recover only on a fresh fenced PRIMARY with exact intent/control identity and a real logged writing barrier.
- [ ] Test old-version refusal, missing/null/present malformed fields, intent mismatch, positive commit, real committed/aborted unknown outcomes, restart reconciliation, and inability to start COPY from an unconfirmed phase.

The parent accepted this exact contract and released the export/framing and persistent-state units separately.
The state unit stores digest values and cannot mint `SchemaProof`; it does not need the hashing dependency before the real digest caller exists.
Target execution, recovery proof construction, and proof-aware clone COPY integration require their later effect-unit release and qualification.
Do not extend this serial pre-data phase into a new parallel-worker fence protocol.
Post-data, large-object contents, sequence state, and nontransactional operations remain separate phase gates.

## Required companion: E1 joins the existing writer fence

The production `CopyTableData` branch in `src/lib.rs` currently passes a raw target connection through `plan_copy` and `run_copy`.
It must participate in the same database-wide `RPGM, 1` advisory fence before clone writes are enabled; another probe connection cannot supply this ownership.
The separately assigned E1 worker owns this bounded companion, not the publication-profile or benchmark workers.

- [ ] Consume the existing E1 effect connection into the authority's existing session acquisition, and retain that same connection across planning, COPY, and target COMMIT without adding target metadata or changing CLI options.
- [ ] Preserve blocking acquisition under the finite I/O deadline and close-only ownership; do not change global lock semantics or issue SQL from `Drop`.
- [ ] Reuse the actual backend/database/role/PRIMARY/fence guard before effects and in the approved `run_copy` `before_commit` callback, after source COMMIT and before target COMMIT inside its existing rollback path.
- [ ] Close on every error because the E1 raw transaction does not update the authority session's phase field; preserve unknown-COMMIT refusal and existing completion semantics.
- [ ] Test E1-versus-authority contention in both orders, no target mutation while another writer owns the fence, lost backend/fence before COMMIT, cancellation, release on close, and unchanged ordinary E1 CLI/data receipts.

Serialization of all admitted rpgmigrate writers remains unqualified until this actual caller and the schema effect path pass their concurrency gates.
The advisory fence does not serialize unrelated writers that do not cooperate with it.

## Review and integration

Review the same physical effect connection, atomic schema receipt, unknown-COMMIT handling, literal construction, extension/privilege admission, and every admitted mutator's writer fence.
Run targeted offline tests per unit, independent safety review, then one exact integrated PG16/18 candidate with existing strict lint/default tests.
Keep Native5/G3 and Native6 publication evidence separate from this new schema profile.
Do not add dummy callers, unused public APIs, broad lint exceptions, external target restore, or a quarantine framework to make the candidate appear complete.

The approved [postdata plan](2026-11-08-schema-postdata.md) reserves fresh Native8/metadata5 for a separate phase intent, fenced effect and retained-input recovery.
It does not adopt Native7 or reinterpret this plan's qualification evidence.
