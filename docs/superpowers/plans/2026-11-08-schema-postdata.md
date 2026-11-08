# Fenced postdata implementation plan

> **For assigned workers:** Use the existing subagent-driven implementation workflow for the unit that Parent assigns.
> Parent allocates authors and independent reviewers; Root owns shared registration, integration and PG lease IDs.

**Goal:** Apply the retained archive's admitted POSTDATA section and its completion nonce in one fenced, synchronous target transaction.

**Architecture:** Consume the existing archive-owning `PredataSql` into opaque `PostdataSql` and use the same `TargetAuthority` effect connection.
Keep native intent data, acknowledged phase proof and future aggregate clone authority distinct.
Retain immutable inputs for a same-intent retry under a fresh PRIMARY fence after an unresolved or rolled-back attempt.

**Tech stack:** Existing Rust/libpq, pinned PG16.15/18.6 client utilities, twice-quoted PL/pgSQL `DO`, the approved SHA-256 dependency, native manifests and logged target metadata.

**Spec:** Extend the [approved predata contract](2026-11-07-schema-predata.md) with Parent's accepted option A, revision 2, and second independent design PASS.
The private design source is `/private/tmp/schema-postdata-proposal-v51djs8t/proposal-r2.txt`.
Parent reserved Native8 and target metadata5 for this feature on 2026-11-08; this plan does not release implementation or confer runtime qualification.

## Fixed contract

Opt into fresh Native8 through the private `state::open_postdata(&StateRequest) -> Result<NativeState>` entry; keep existing `state::open`, Create defaults and old schema-absent profiles unchanged.
The selected Native7 base has no creation-profile selector: `open` delegates to `open_native`, and its header and target metadata checks use version constants 7 and 4.
Add the smallest shared version-selected opening path: the old entry selects its old flavor, while `open_postdata` selects Native8 and reads only that flavor, refusing Native7 without conversion.
Preserve the existing action, directory ownership and provenance-recovery checks; opening checked data confers no replay or completion authority.
Do not add a CLI flag or a field to `StateRequest`, and do not globally bump the old default constants.
For Native8, write required-present `postdata_intent: null` during initial manifest publication, before source, target, schema or COPY records exist.
Keep the old flavor's serialized key set unchanged; strict version-specific presence checks must distinguish its absent field from Native8's required null.
Every source, schema, plan and provenance mutator preserves the opened Native8 header rather than rewriting the old default version.
Select target metadata5 from explicit Native8 identity even when postintent is null; never select a target format from `Some(postdata_intent)`.
Reject Native7 adoption, conversion and in-place target metadata upgrades.
Keep CoreSchema3, receiver context V3, COPY references, WAL formats and the 16 KiB manifest limit unchanged.

`PostdataIntentV1` has exactly seven fields:

```rust
intent_version: u32,                 // Exactly 1.
nonce: [u8; 16],                    // Internally minted, nonzero.
predata_nonce: [u8; 16],             // Exact SchemaIntentV1 nonce.
copy_plan_nonce: [u8; 16],           // Exact COPY anchor and plan nonce.
sql_sha256: Digest,                 // SHA-256 of retained POSTDATA interior.
policy_version: u32,                // Exactly 1.
standard_conforming_strings: bool,
```

Refuse a new nonce that equals the schema or plan nonce.
Resolve full immutable schema and COPY identities from already decoded manifest records only when the optional intent is `Some`.
Present null requires no future record; missing, malformed, duplicate or unknown fields refuse.
Do not recurse through checked getters or open a sealed plan while decoding a manifest.
Do not add a duplicated source receipt, archive digest, utility version, length, counter or serialized authority proof.

Input identity comprises the full source receipt, archive SHA-256, validated body SHA-256, policy, string settings and profile, the same persisted intent and nonces, and genuine historical proofs.
A separately minted byte-identical artifact satisfying every existing binding is semantically identical; Rust allocation and object addresses do not participate in identity.
Refusal of substituted or re-exported SQL means refusal of binding-changing input before effects.
Held descriptors must still detect path replacement or content mutation.
This equivalence adds no lost-input restart or replay guarantee.

Metadata5 adds only `ControlRow::postdata_nonce: Option<[u8; 16]>`, giving 24 columns and 36 CAS parameters.
Present nonzero completion requires exact predata nonce and COPY anchor, absent CDC frontier and a positive existing barrier counter.
Installation preserves the barrier epoch, increments the existing counter once and cannot replace or erase a completion.
Keep legacy `copy_complete` null for schema work.

> [!important]
> Native8 is the postdata-enabled profile: refuse every Native8 CDC begin and frontier advance throughout this slice, including fresh `postdata_intent: null` and acknowledged or recovered POSTDATA completion.
> Preserve ordinary schema-absent behavior in the separately qualified older profiles; do not reinterpret or adopt their formats.
> Only a future approved aggregate authority can release Native8 CDC, ACK or whole-clone success.

## Interfaces and ownership

Unit 1 uses the existing `src/state/schema.rs` namespace and exports data-only methods:

```rust
state::open_postdata(request: &StateRequest) -> Result<NativeState>
NativeState::begin_postdata_intent(
    &mut self, schema: &SchemaIntentV1, copy: &CopySnapshotIntent,
    sql_digest: Digest, standard_strings: bool,
) -> Result<PostdataIntentV1>
NativeState::checked_postdata_intent(&self) -> Result<Option<PostdataIntentV1>>
```

Unit 2 consumes the held archive without a pathname reopen or arbitrary-SQL constructor:

```rust
PredataSql::export_postdata(
    self, receipt: &SourceSlotReceipt, io: &IoControl,
) -> Result<PostdataSql>
PostdataSql::checked_digests(&mut self, io: &IoControl) -> Result<(Digest, Digest)>
```

The digest tuple remains archive first, body second.
Keep equivalent read-only body, receipt, lexical-policy and utility accessors and consuming checked cleanup.
Do not invent a retryable cleanup owner; an actual need for one returns to Parent.

Unit 3 provides the effect and committed-completion recovery entries:

```rust
TargetAuthority::restore_postdata(
    &mut self, native: &mut NativeState, sql: &mut PostdataSql,
    schema: &SchemaProof, snapshot: &SnapshotProof, io: &IoControl,
) -> Result<PostdataCommitOutcome>
PostdataCommitOutcome = Acknowledged(PostdataProof) | Unknown
TargetAuthority::recover_postdata(
    &mut self, native: &NativeState, io: &IoControl,
) -> Result<PostdataProof>
PostdataProof::checked_intent(&self, native: &NativeState)
    -> Result<&PostdataIntentV1>
```

Only acknowledged synchronous effect or acknowledged writing-barrier recovery constructs the opaque proof.
No public constructor, serialization, native intent or bare row read substitutes for it.

### Acquisition and pending provisioning

Initial Native8 creation requires validated `acquire_for_clone` with the admitted `PredataSql` and complete source, archive, body, profile and local phase bindings before target provisioning.
Ordinary Native8 acquisition is existing-only: it must not create, repair or upgrade a schema, table or row, or fabricate phase completion.
It may reconcile an already exact, owned metadata row with a genuine pending native `Provisioning` record.
Require all run, source, target, profile and nonce bindings and the expected row state to match.
Apart from the checked barrier update, preserve every existing row field without repair, reset or overwrite.
Confirm native provenance only after a same-fence synchronous checked barrier has positively acknowledged COMMIT, Idle and durable evidence.
Missing, malformed or mismatched objects refuse without provisioning; Unknown closes and withholds confirmation.
This reconciliation grants no schema, COPY or POSTDATA proof or CDC authority.
Keep existing-only recovery available before COPY and after the PRE-DATA artifact is consumed; acquisition alone does not supply phase proof or replay permission.

## Unit 1: coherent persistence and refusal guards

Proposed owner effort: xhigh, because malformed phase state or a null-state CDC gap could authorize unsafe continuation.
Owned private paths: `src/state.rs`, `src/state/schema.rs`, `src/state/schema_tests.rs`, `src/apply/metadata.rs` and its existing tests.
Root supplies the exact `ControlRow` field and any shared re-export or registration hunks before that private compilation; the worker does not edit shared registration files.
The authority-level Native8 begin guard is an explicit dependency for Unit 3, not an authority claim from this data-only unit.

- [ ] Add failing cases for explicit fresh present-null initialization, unchanged old Create/SourceOnly behavior and key set, cross-flavor refusal, and preservation of flavor8 across every origin/schema/plan mutator.
- [ ] Add missing/duplicate/malformed field, nonrecursive `Some` resolution, nonce collision, changed identity, maximum manifest size and synchronization failure cases.
- [ ] Implement opt-in Native8 and metadata5 together, including explicit-flavor metadata selection while intent is null, full-row decode/write/CAS, 24-column/36-parameter inventory and immutable nonce/anchor preservation.
- [ ] Refuse Native8 frontier transitions even with null postintent; retain old-profile controls and test attempts before, during and after phase completion.
- [ ] Run the focused state/metadata tests and freeze the exact interface, patch, inverse, source identity and results for Parent-assigned safety review.

## Unit 2: retained archive POSTDATA export

Proposed owner effort: high, because this extends the existing bounded process and framing path without target effects or persistence changes.
Owned private paths: `src/clone_phase.rs`, `src/clone_phase/process.rs`, their focused tests and the artifact API documentation.
This unit can run independently of Unit 1 after its literal artifact interface is frozen.

- [ ] Add failing cases proving archive ownership transfer, exact source receipt, held-FD identity, complete interior preservation, lexical profile and unchanged qualified utility selection.
- [ ] Reuse export-only argv with `--section=post-data`, exact outer restrict framing, 16 MiB body cap, finite cancellation and redacted child/reaping behavior.
- [ ] Refuse missing, truncated, empty, malformed or failed output; do not synthesize no-work success, filter statements or add a target connection.
- [ ] Freeze the opaque artifact and focused RED/GREEN evidence for independent review; no effect API or format code belongs in this unit.

## Unit 3: fenced effect and retained-input recovery

Proposed owner effort: xhigh, because uncertain COMMIT, one-use freshness, proof coverage and cleanup ordering determine whether DDL can execute again.
Owned private paths: `src/apply/schema_effect.rs`, non-registration effect/guard hunks in `src/apply/checkpoint.rs`, existing schema session/guard tests and effect documentation.
Explicit catalog hunks in `src/catalog.rs` and its focused tests cover early FK/unsupported-index refusal and strict postdata PK/index admission; compose only after Mendel's disjoint COPY-shape patch is reviewed and frozen.
This unit depends on reviewed Units 1 and 2, Root's exact shared hunks, and the genuine schema-aware nonempty COPY prerequisite.

- [ ] Add failing tests for full native/source/target/schema/COPY/proof identity and independent archive/body/lexical mismatches before intent, before effect and after effect.
- [ ] Compare `sql.source_receipt()` with `schema.source` and the held archive SHA-256 with `schema.archive_sha256`, separately from postintent SQL digest, string policy and version.
- [ ] Validate the sealed plan, end its immutable native borrow before publishing or synchronizing intent, then reopen and retain the checked reader through target effects.
- [ ] On the actual fenced PRIMARY connection, lock and compare the full row, verify installed language and lexical/session settings, execute the unchanged body through the existing `DO`, and recheck ownership and bindings.
- [ ] Require strict final PK/replica identity, healthy admitted indexes/constraints and absent unsupported FKs before the single nonce/barrier CAS and synchronous COMMIT.
- [ ] For an existing intent, consume fresh fence eligibility exactly once: matching committed nonce uses a writing barrier with zero DDL; absent completion allows only identical retained SQL and genuine prior proofs with the same intent and nonce.
- [ ] Test known rollback, Unknown, repeated lost replies, used authority, changed or lost inputs, corrupt plans, barrier overflow and cleanup errors; no prior outcome alone proves rollback.
- [ ] Keep every Native8 authority begin and frontier transition refused, including null intent, successful phase proof and recovered completion.
- [ ] Freeze the composed effect/caller tests for independent safety review before any Linux or PG qualification release.

## Integration and remaining gates

Prepare a new ordinary private export from the reviewed Native7 effect base, source197 `6171cc984ca8ef6d174527425b6771e199205f405c44076053b809cbd2dc4436`.
Compose only Mendel's reviewed schema COPY patch `94fab11d997c69a6193928d88589605bcab5e8a859f3ebab04bad4680b6da973`; its frozen result is source198 `e5c1a69b7f99b2f0e8c66c3479c8c8bafc371cdc86486feb16b9d48c2c88ce34`, with independent bounded policy PASS `63a3262f`.
Retain exact parent and per-path identities before adding the assigned Native8 units.
Do not import Hegel's separate Native7 diagnostics, Native6 SourceOrigin integration or the Native5 E7 refactor into this composition.
The inherited effect and COPY reviews do not qualify real nonempty utility-driven COPY or POSTDATA; global strict-lint warnings remain open.

Root alone owns `src/lib.rs`, shared module/re-export lines, the `src/apply/mod.rs` DTO prerequisite, exact candidate composition and eventual Task/CI registration.
No worker edits primary, another unit's paths, Hegel's Native7 diagnostic harness or live fixtures without an explicit new write-set.
Each private Cargo command uses an explicit private manifest, a distinct target and the approved cached dependencies; no unrelated upgrade, generic framework or public retry CLI is authorized.

Before PRE-DATA effects, reject incoming/outgoing/self/cyclic FKs, invalid or unsupported indexes, callbacks, event triggers and other unsupported selected shapes under the held source snapshot.
Keep FK definitions and later FK execution/verification as mandatory operator-activation work; the first internal unit is FK-free.
Preserve complete unfiltered utility output and dependency order.
Do not relax ordinary COPY/WAL identity or confuse temporary PK absence under genuine schema admission with strict final identity.

Retain SQL, prior proofs, intent and plan until the caller chooses retry, recovery or abandonment.
Cleanup failure never replaces the primary effect outcome or creates whole-command success.
Lost inputs or proofs after restart refuse replay; matching committed phase readback is not full restart/resume.

### Available utility bundles

Read-only metadata checks on 2026-11-08 confirmed matching executable `pg_dump`, `pg_restore` and `pg_config` in both existing pinned Linux arm64 fixture images.
The local receipt is `target/verify/postdata-tool-inventory-670f3c02/receipt.json`, SHA-256 `be5ef51f33989f1640c33843120492d12984f438ee8e2f56e15e99548f57e1de`.
Both temporary metadata containers were removed; no database, SQL, dump/restore workload or rebuild ran.

- PG16.15 image `sha256:c4e17fdf9124beaee3c2f3b10daa07120ffe85972e0726eecd64b188bb1a3880` supplies `/opt/e1/pg16.15/bin/pg_dump` and `/opt/e1/pg16.15/bin/pg_restore`, both reporting 16.15.
- PG18.6 image `sha256:0b856c1760dc4604a7ad778e52feacd66d219e243d5447dbe688f2bce9b9718a` supplies `/opt/e1/pg18.6/bin/pg_dump` and `/opt/e1/pg18.6/bin/pg_restore`, both reporting 18.6.

The receipt binds each executable hash and canonical path; version availability alone is not real-utility qualification.
Native20 identifies the application's libpq and does not stand for both client bundles.
If a later source image lacks the required pair, reuse PR4's pinned single-client recipe and separate `target/native/pgtools` prefix; do not rebuild application libpq or Native20 without an actual need.
The four PR4 matrix proofs qualify packaging, not a released GHCR image; the tag publisher was skipped.
Bind a matching 16.15 pair for PG16 targets and an 18.6 pair for PG18 sources.
The future PG16.15-source to PG18.6-target gate must declare its matching dump/restore pair under the existing source-major <= client-major <= target-major rule and exercise the actual utility output through the same-session `DO`.

- [ ] Compose reviewed units and run required exact-candidate lint/default tests and independent integration review.
- [ ] Qualify real utilities, real snapshot and nonempty PRE-DATA to COPY to POSTDATA on PG16.15 and PG18.6, then the separately selected 16-to-18 pair.
- [ ] Qualify unknown-COMMIT contention, same-intent retained retry and committed barrier-only recovery under a new bounded resource lease.
- [ ] Complete FK definitions, sequence values, large-object identity/content/ownership/ACL, durable restart inputs and aggregate verification before operator clone activation.

Native7 synthetic-source/fake-utility negatives and Native6 pgoutput passes remain separate immutable evidence.
This plan neither explains those historical failures nor qualifies new formats, real utilities, full clone or Native8 CDC.
