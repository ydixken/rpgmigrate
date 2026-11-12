# rpgmigrate: assessment and milestone plan

## Context

pgcopydb-operator must run migrations on a native Rust engine, `rpgmigrate`, instead of the forked pgcopydb.
Almost every data-loss defect that reached the operator came from pgcopydb's CDC spool pipeline (op#356, #357, #358, #359, #376, #378).
The engine therefore makes crash/restart correctness and fast WAL apply its core properties, with documented durability modes.
Goal: working operator-driven migrations (copy, replication, recovery, cutover, verification) on PG16→16, PG18→18 and PG16→18, with reproducible E2E tests, Taskfile commands, Docker packaging, GitHub Actions and documentation.

Inputs: the code of all three repos, operator and fork issues, pgcopydb `origin/v0.18-fixes`, the trusted `AGENTS.md`, the [approved design](superpowers/specs/2026-11-06-rpgmigrate-design.md), `tasks/lessons.md`, `docs/operator-compatibility.md`, and the uncommitted work (51 untracked files plus `stash@{0}`).
Not used: `tasks/todo.md` and the dated plans.
A 9-reader audit with adversarial cross-checks and a 5-lens plan review (correctness, scope, operator/E2E, performance, builder) produced the findings below.

## Assessment at the planning baseline

- The binary runs real work for `ping` and a restricted serial `copy table-data` only (`src/lib.rs:56-103`); all eight operator routes return "unimplemented".
- Uncommitted work lives in two places: 51 untracked files on `main` and `stash@{0}` ("WIP on feat/native-copy: bd0925d", 42 files, +11.6k/-2.1k, with the 4.3k-line catalog, COPY anchor, slot creation and state provenance).
  The untracked files compile only with the stash; all Rust files are identical on `main` and `bd0925d`.
- Sound and worth keeping: the libpq FFI (single owners, nonblocking connect, CopyBoth pump with fault tests) and the target checkpoint model (CAS control row in the data transaction, writer fence, unknown-COMMIT recovery by logged barrier).
- Roughly 15-25% of an engine exists: about 4.2k reachable lines, 7.8k unwired engine lines, and about 25k lines of receipts, parity fixtures, storage-pilot benches, image verification scripts and count gates.
- Blocking gaps: no pgoutput decoder, no slot/publication lifecycle or standby feedback, apply at one round trip per row with per-transaction re-verification (57-153 TX/s measured) and a non-strict restart dedupe (`end == applied.end` passes), COPY in one database-wide transaction with a flush per row and no schema step, PG16/PG18 refused by the snapshot gate, a global 300 s deadline, and no cutover, drain proof, cleanup, compare or progress.
- Operator: `spec.engine=rpgmigrate` fails with `UnsupportedEngine`; `internal/native/contract.go` (protocol v1) exists only on branch `feat/rpgmigrate-native-operator` (e8d9b4f) and has no caller; argv0, log markers, `pgcopydb%` application_name patterns, the `pgcopydb version` progress gate and the pg_restore≥17 preflight are hardcoded; PG16 targets are refused by preflight and E2E `checkMajors`; E2E has no kind bootstrap and runs only on a private cluster.

## Decisions

Amendments to the approved design are marked; user-approved ones carry the date and go into the design as a dated revision in M0.

1. **Slot as the durable input log (AMENDMENT, user-approved 2026-11-08: source retains WAL, no copy-phase spool).**
   pgoutput protocol v1 (no streaming) delivers each transaction whole, at commit, in commit order; the walsender spills large transactions on the source.
   Rows stream straight into the open target transaction, so memory stays bounded without a local journal.
   Feedback never exceeds the durable target frontier, so the slot retains everything not yet durable on the target.
   This removes the spool defect class (fork invariants I1-I3, I8, I12, I13, I18) instead of re-fixing it.
   Cost: the source keeps WAL from slot creation through the copy plus apply lag; preflight checks `max_slot_wal_keep_size`, headroom, PG16 `old_snapshot_threshold` and PG18 `idle_replication_slot_timeout`, and the engine polls `wal_status` and refuses on `lost`.
2. **Target checkpoint is the authority, with a certified no-work frontier.**
   One logged control row per stream in an owned schema, CAS-updated in each applied group's transaction; it records `applied.end`, `certified_lsn`, the copy anchor, the source system identifier and timeline.
   `certified_lsn` advances only from a keepalive received after every durable transaction, through a rate-limited logged synchronous barrier written before feedback passes `applied.end`.
   Feedback is a type constructible only from an applied commit end or a certified keepalive, monotonic, and never below the `confirmed_flush` read at connect.
   On every connect the engine refuses (exit 3, gap) when `confirmed_flush_lsn > max(applied.end, certified_lsn, anchor.consistent_point)`, then starts at that max; `end_lsn` must strictly increase per stream.
   The fence is a per-run advisory key; unknown COMMIT is resolved by fence reacquire plus a barrier; a sync-rep "canceling wait" WARNING counts as an unknown outcome.
   No replication origin is created; `--origin` is accepted and recorded, and cleanup reports it as not owned.
3. **Fast apply.**
   Receive and apply run on their own threads, each creating its own connection (handles stay `!Send`), linked by a byte-bounded channel.
   When the channel is full, receive stops reading but keeps a timer-driven status every `min(10 s, wal_sender_timeout/2)`, and sets `wal_sender_timeout=60s` on its connection.
   Apply uses prepared statements cached per relation generation, relation checks once per generation, and libpq pipeline mode per group: BEGIN, pipelined DML and the CAS UPDATE, flush request, verify every result, then COMMIT, giving 2 round trips and 1 fsync per group.
   Groups close naturally when input is empty or a cap is hit (`PGCOPYDB_APPLY_GROUP_TXNS`, default 100, plus row and byte caps, and large-transaction rules).
   Apply sessions use `session_replication_role=replica`, `row_security=off`, server TCP keepalives, and `transaction_timeout=0` only on servers ≥17; pinned output GUCs on the replication connection; plugin options are exactly `proto_version '1'` and `publication_names`.
4. **Durability modes.**
   `durable` (default): synchronous COMMIT per group, feedback after it.
   `replay-safe`: asynchronous commits plus a logged synchronous barrier at a bounded interval; feedback only from barrier proofs.
   `relaxed` (AMENDMENT, user-approved 2026-11-08): built only if paired benchmarks show a real gain over `replay-safe`; if built, it keeps the explicit finite loss caps and D/H/epoch reservations.
   Target failover: acknowledged changes survive a target failover only with a synchronous standby (CNPG synchronous replication with required durability); otherwise a failover leads to a gap refusal and a fresh migration, and the docs say so.
5. **Initial copy order.**
   Create the owned publication (`FOR TABLE` plan list, `publish_via_partition_root=false`), then `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` on a replication connection kept idle with session timeouts disabled, then `pg_dump --snapshot=<token>` (pre-data and post-data, one archive), then `pg_restore` pre-data with `--no-publications --no-subscriptions`.
   Then assert that `pg_publication_tables` equals the copy plan, and start `--table-jobs` workers that import the snapshot and copy unsplit tables as `BEGIN; TRUNCATE; COPY ... FREEZE; done marker; COMMIT`.
   Release the exporter after every worker has imported, then run post-data (`pg_restore --section=post-data`), set sequences and run ANALYZE; apply starts only after post-data.
   COPY relay batches into 64 KiB buffers and flushes only on backpressure and at the end; `row_security=off` on source sessions turns hidden rows into errors.
   Client tools match the target major; a source major above the target major is refused.
6. **Cutover and drain proof.**
   The operator reads `pg_current_wal_insert_lsn()` (which covers source commits made with `synchronous_commit=off`), persists it, and execs `rpgmigrate stream sentinel set endpos <lsn> --json`, which is idempotent and forward-only; `--current` uses the same function.
   Control lives in a `control/` subdirectory with its own lock, written by temp file, fsync, rename and directory fsync, and re-read by the worker.
   Drained means every received transaction with `end_lsn <= endpos` is in the durable frontier, and a keepalive `>= endpos` (requested via reply-requested status) or a later BEGIN arrived after them; the barrier is written before feedback, and the report's `drain_proof.replay_lsn` is that value.
   Final sequence sync and the large-object re-sync happen after the drain, before exit 0.
7. **Operator integration through the native adapter** (design: native handshake, no pgcopydb alias).
   The operator keeps its lifecycle, Job builders, exec and connection helpers, and adds an engine-aware command layer, `rpgmigrate version --json` capabilities parsed in preflight, `native-report: {json}` stdout lines for phase, drain proof, progress and cleanup, and work identity passed as env derived from the Migration UID and PVC.
   Compare keeps the existing per-table JSON array and `compare progress:` shapes; error classes stay in `{error_severity, message}` JSON logs; application_name patterns are ORed for both engines.
8. **Crash during the follow copy (AMENDMENT, user-approved 2026-11-08):** resume before the copy anchor restarts the data phase.
   It drops this run's own slot and publication, creates new ones with a new snapshot, truncates every plan table in one statement, unlinks copied large objects, and invalidates done markers through a new copy generation.
   It applies only to tables whose creation this run recorded; resume after the anchor continues CDC from the checkpoint.
9. **libpq (AMENDMENT, user-approved 2026-11-08, supersedes the lessons entry requiring the patched artifact):** stock PGDG libpq and postgresql-client 16 and 18.
   Delete `native/libpq/**`, `src/pg/native.rs`, the connect-time capability check, `tools/build-libpq.py` and the custom image build; keep the passfile/service live tests minus the stderr assertion, and move their shared helpers.

## Calls I made (check these)

- **Versions (AMENDMENT of design line 303 for this release):** the qualified native profile accepts source and target majors 16 and 18 with source ≤ target and refuses others before mutation.
- **Triggers and FKs:** apply uses `replica`; preflight requires the privilege to set it (superuser or `GRANT SET ON PARAMETER`) and refuses ENABLE ALWAYS/REPLICA user triggers.
- **Missing objects:** after creation, a missing slot or publication or a changed publication OID is a gap refusal, never a silent re-create; any NoticeResponse on the replication connection is fatal; FAILOVER slots stay off.
- **Large objects in follow:** logical decoding does not carry them, so they are re-synced after the drain, under the write freeze, and covered by compare.
- **CLI:** keep the parser baseline and fixtures (design requirement); add a parse-only test oracle and explicit refusals for options outside the native profile; `list tables` stays an explicit staged refusal.
- **Tests:** `tools/test-db.py` stays for cargo live tests (lessons); containers are added only as the E2E layer.
- **Superseded:** `docs/superpowers/specs/2026-11-07-copy-snapshot-anchor.md` (single-transaction COPY, sealed plan nonce); only the token and consistent point carry into per-table markers.
- **Code we drop, in stages:** M0 removes self-contained experiments (`bench/storage*`, `tests/storage_{format,recovery}.rs`, their Cargo entries and Taskfile count gates, `src/snapshot_journal_live_tests.rs`, `test/build.log`); M2 removes `copy_plan`, `state/plan_files` and the E1 catalog whitelist with the COPY rewrite; M3 replaces `cdc/{codec,event_codec,decode}.rs`, `plugins/test_decoding*` and the replay half of `apply/serial.rs`.
  Kept: `src/pg/*` (refactored), `identity.rs`, `contracts.rs`, state primitives, `cdc/contracts.rs`, `apply/plan.rs`, and the checkpoint CAS, barrier, unknown-COMMIT and provenance logic.
- **Preservation:** the stash and untracked files are one archive commit on `wip/native-copy-snapshot`, labelled unverified and never merged; `stash@{0}` and the `archive/stash-0` ref stay until `feat/native-engine` builds.

## Milestones

Each milestone ends with a runnable demonstration and clean `task lint test`; tasks live as checkboxes in `tasks/todo.md` after approval.
After M1, the engine track (M2-M4), packaging (M5) and the operator track (M6, kind bootstrap of M7) run in parallel.
The first operator E2E (M7) runs the vertical slice with binary COPY, split and large objects switched off in the Migration; M8 turns them on.

> [!IMPORTANT]
> M0–M5 are closed; M4 engine source and six behavior documents are integrated on `feat/native-engine` at `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476`.
> M4 passed host lint/check, 499 Rust tests, 27 CLI tests, 324 portable tests, forty fresh native sampling cases, and all 93 Follow21/Cutover10 runtime cases.
> The prior nine-profile 266-case native subset retains its original provenance and is separate from the forty fresh samples.
> The single whole-M4 review is resolved with no findings remaining; operator test-only evidence is committed separately as `c262322909ac03a7101ea3bfac2c4823efa62e95`.
> Final repaired-runtime actual-seven Go evidence is private PostgreSQL 18 interoperability with mocked Kubernetes ownership and production admission disabled.
> M6–M10 remain open: production native adapter, kind E2E, feature widening, power-loss/performance qualification, and release gates remain unqualified.
> On 2026-11-09, the user authorized unused Podman image/volume/build-cache pruning, the PostgreSQL 18 preservation/128-GiB maintenance plan, and the scoped M6 Python/ctypes/libpq runner dependency exception.
> VM preservation/resize maintenance and exact failed-run retirement passed; fresh `f59a7f4b` M7 UP/handover/LOAD passed, then the legacy storage image-ID guard refused the successful probe as recorded below.
> The 2026-11-10 checkpoints below record committed completion/sampler components, separate completion-rejoin packets, and fresh ARM packaging, without native lifecycle/authentication/TLS/PVC or three-pair acceptance.
> Feature-branch pushes to origin for hosted CI, including M5/M10 native AMD64 checks, are user-authorized within the dated limits below.

Remaining work follows the existing M6 production adapter and M7 operator E2E dependencies, using the accepted M5 packaging/CI evidence.
On 2026-11-09, the user directed us to push through M10: continue all approved M6–M10 execution gates rather than stopping at intermediate plans or source checkpoints.
Milestone closure still requires evidence, benchmark runs remain unapproved, and main/tag/release boundaries remain unchanged.
The dated M7 amendment permits only development GHCR publication.
We make no ETA or measured-speedup claim.
Keep their image, Kubernetes/PVC, dependency, capacity, and release gates explicit; M4 engine qualification does not activate the operator.

### GHCR source and qualification checkpoint, 2026-11-10

The engine publication source is `11a28a4d1c77b035f96cc16b7b948768dd6aa6c9`, tree `e33b886b61abdd97314a57c998cc681412c9ba15`.
The first native Linux ARM64 build, read-only smoke, and single development push completed under the approved bootstrap exception.
The immutable native tag is `dev-11a28a4d1c77b035f96cc16b7b948768dd6aa6c9-arm64`, manifest `sha256:8916bba4c10ee30e2b7ed7d6877818c1eaa4ffa1d542e851d9bcb7a8508a4e9a`, config `sha256:420a6f33ef3bf5d46cd57911256b72466f2cdb90f8015bb5c2c52c897a075ece`.
The user made the native package public, linked its repository, and enabled Actions `repositoryWrite`.
Anonymous verification passed for the retained native manifest, config, and all eleven layers.
Public verification proof: `/private/var/tmp/rpgmigrate-native-first-publication-11a28a4-apozt_ez/public-verification-proof.json`, SHA-256 `45930f72ff5bff3311d6f41a8d8ab2d45e084b2bf78108ccb23c243f75b53a35`.
Owned-node pull and runtime qualification remain open.
Publication proof: `/private/var/tmp/rpgmigrate-native-first-publication-11a28a4-apozt_ez/publication.json`, SHA-256 `d5cb68e11305457107bf4f0bfe317a3cc7dfb6514a30603650c160272e3b84a8`.

The build used the physical ARM64 Mac and ARM64 Linux VM with Podman client 6.1.4/server 5.8.3, without a tool upgrade.
All four original containers, 206 volumes, and 124 original unique images were preserved; two owned new build images were retained.
The builder window is released and no builder remains active.
Preservation proof: `/private/var/tmp/rpgmigrate-native-first-publication-11a28a4-apozt_ez/preservation-release-proof.json`, SHA-256 `6f8245aeff5a9a68238ba0df07d606bbbbb8891e4a0f5f6ddb7e9682e3031175`.
These receipts make no benchmark timing or performance claim.

We publish only `ghcr.io/ydixken/pgcopydb-operator` and `ghcr.io/ydixken/rpgmigrate` development images, under `dev-<full-40-hex-commit>-<arm64|amd64>` tags, without overwriting an existing tag.
Ordinary publication uses the corresponding exact feature-branch workflow; the separately approved first native-package bootstrap remains a narrow exception.
Public package visibility is approved only for these packages.
Publication, visibility, anonymous typed manifest/config/blob verification, and actual owned-node pull evidence are separate checks.
No release, version tag, `latest`, main push/merge, crate publication, or benchmark authorization follows.

The tracked upstream catalog is static; the generated private schema 2 image lock binds both clean sources and verified publication receipts without a source/digest circularity.
The private schema 2 suite input requires `image_lock_sha256`, the lowercase SHA-256 of the exact lock copied to `RUN/image-lock.json`.
Go independently checks those bytes and source identities before workload creation.
M7 admits standard kind read-only `extraMounts` for bundled storage, with a manifest override selecting digest references before the first storage Pod.
This replaces the live-patch proposal; the registry route uses no archive/import or local registry and requires a fresh source freeze and RUN.
Historical failures and held RUN evidence remain unchanged.

The M6 stage freeze is parent PREPARE, UP, PULL (eight pulls with three attempts each), optional verified legacy, stable-parent-owned and locked M6 preparation, Q900 native three-by-three in a separate directory without parent mutation, V600, Go900, then final V600.
The recorded Q deadline must not exceed the original lease, independently of the current V cap.
S2 may consume opaque kubeconfig locally in memory only for the necessary identity and authentication checks; it adds no Secret API read or export and makes no zero-secret-memory-read claim.

The historical Guard refused operator source `93cc670309e1039e05ee6dc0e462196d23248888` at canonical SSH-origin mapping.
We pushed successor operator source `29edbfba1d03afb890429b2ef69ab8f5a9d088e1`, preserving that refusal under its original input identity.
Its [offline contract run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42823505464) and [mirror run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42823505340) passed.
The [manager publication run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42823505418) published the AMD64 image publicly at digest `sha256:b728d28ddf9e956e6912853c3541d877482f72b0d0fa86437139dfdbb1cf3136`.
The same run passed ARM64 publication at manifest digest `sha256:b3ccc7ec689bdd721e943a3dd2c2f4844a9684aabaca2640060a8d0a3129e9da`, config `sha256:10a1064b16874241ad9c94f37ac0aa7872bd582cec07a6415b2d52d11ac07154`, without an OCI index.
Independent anonymous graph verification passed for both architectures, including all fourteen layers per image.
The manager package API reports public visibility and repository linkage to `ydixken/pgcopydb-operator`.
Hosted publication proves effective write access, while the manager Actions UI setting remains uninspected.
Sealed eighteen-file manager evidence: `/tmp/rpgmigrate-m7-hosted-ci-public-20261110-ws3q891j/receipt-manifest.json` (lost 2026-11-12), SHA-256 `b21c161a38a24710ef2f6fc52ee10d55d11e8fdd9ee77dfa7017d15dd1581b05`.
Lock-ready image input: `/tmp/rpgmigrate-m7-hosted-ci-public-20261110-ws3q891j/manager-lock-ready-images.json` (lost 2026-11-12), SHA-256 `d9b38e132048e54ac162933eaf1ff0dda4ac2a097f8b3270b7f7337be09a1366`.
Fresh source-bound runtime RUN qualification remains open.

Historical engine source `a38e02d95e47adff912054d9ffc087219e2b82b7` passed all three plain-clone pairs and both architecture runner-image smokes in [CI run 42817666258](https://github.com/ydixken/rpgmigrate/actions/runs/42817666258).
Its Linux Clippy gate failed on three safety-comment placements.
The separate [development publication run](https://github.com/ydixken/rpgmigrate/actions/runs/42817666532) retained `PUSH_OUTCOME_UNKNOWN` for both images with anonymous 404 results.
Keep those immutable tags untouched and retain the unknown outcomes.
Successor source `abd1a36e50726a26a9f655c5f53bb34ed8d182c1` fixes those three comment placements and sanitizes push-failure diagnostics, with 451 portable checks passing.
We pushed the signed successor to the feature branch.
Push proof: `/tmp/rpgmigrate-native-safe-diagnostics-feature-push-proof-20261110.json` (lost 2026-11-12), SHA-256 `aee53d8539b35f329975c7d5a3d6685145c59233a61d4089710cb6c841659f8a`.
Fresh [checks run 42824505133](https://github.com/ydixken/rpgmigrate/actions/runs/42824505133) passed all eight jobs, including Linux Clippy, dependency/docs checks, all three plain-clone pairs, and both architecture runner-image smokes.
The [development publication run 42824505294](https://github.com/ydixken/rpgmigrate/actions/runs/42824505294) passed both Ubuntu 26 native publication jobs.
Typed public/source artifact verification and independent anonymous graph verification passed for both successor native images.
Sealed native evidence: `/private/tmp/rpgmigrate-abd1-ci-verification-wp3sm6jz/manifest.json` (lost 2026-11-12), SHA-256 `bfbdd941990af3f37cd4880bf0faa25c13d422b7904edf715c624e6d94d06f66`.
Lock-ready native input: `/private/tmp/rpgmigrate-abd1-ci-verification-wp3sm6jz/lock-ready-native-images.json` (lost 2026-11-12), SHA-256 `367652be5b567f2bf01d4c1e2b2eb8b1ccf3b326353aca6b0b1b01d3f8e30393`.
These image checks do not qualify an operator migration.

The strict twelve-role ARM64 lock binds public manager/native images and pinned upstream graphs.
Fresh upstream verification hashed 72 layers totaling 1.234 GB, and the canonical operator `29ed` receipt-input checks passed.
Private lock: `/private/var/tmp/rpgmigrate-m7-canonical29ed-clock-successor-20261110-1f92b6ad/image-lock.json`, SHA-256 `0cb3eb6be70f56709274d8d5f55ac6f592663ea98407adad4caba36859752da1`, a byte-identical copy of the cited `/private/tmp/rpgmigrate-m7-arm64-public-lock-20261110-build-a7f2/image-lock.json` (lost 2026-11-12).
The canonical M6 private-envelope rebind manifest is `b293524d39398576cb3a02f13d709221edc3cbb2c909ed021debf29760a8b4c5`, with status `UNBOUND`.
The sealed canonical supervisor packet retains status `UNBOUND`.
Supervisor packet: `/private/var/tmp/rpgmigrate-m7-ghcr-registry-supervisors-canonical29ed-20261110/source-packet.json`, SHA-256 `ced1a9dea61f033729bbea7cc6df956d9eeaf2573471a4d9798d57ba0909410b`.
The canonical source `29ed`, native runner `abd1`, and exact twelve-role lock passed actual PREPARE/CHECK and UP/CHECK.
PREPARE took 34.151 seconds and its CHECK took 1.034 seconds.
UP took 83.445 seconds and its CHECK took 10.388 seconds.
The three owned nodes reached Ready with actual two-CPU, 4-GiB, zero-swap limits and matching API/provider identities.
First Ready proof: `/private/var/tmp/rpgmigrate-m7-canonical29ed-admission-20261110-72d46c83/runtime-supervisors/s1-evidence/first-up-ready-proof.json`, SHA-256 `cc51c796918f48ec963ea75431d463d42a10f7d8b0427928e65234a66147fc7f`.
The later S2 one-shot refused admission because the original wall lease expired.
Wall time advanced 15869.021 seconds against 775.494 monotonic seconds, leaving an unattributed 15093.528-second discontinuity.
The cause remains unresolved.
S1 expired or became absent, and the stored Ready receipt grants no live authority.
No persistent S2, handover, PULL, or MigrationSpec execution followed.
S2 refusal proof: `/private/var/tmp/rpgmigrate-m7-canonical29ed-admission-20261110-72d46c83/S2-failed-admission-proof.json`, SHA-256 `b658467f23a6d544a5605d6b1201f567289c17c3b8cb02a2101b8ee158049e3b`.
Independent retirement verified absence of all three exact owned node containers, their PIDs, and their cgroups, then released the window.
All 425 image IDs, associations, and history remain preserved.
All 209 volumes remain retained, including the original 206 and three new volumes.
The four original and four external containers, both networks, 716 source paths, RUN evidence, and S2 refusal evidence remain preserved.
The kind network is empty.
The only known unrelated delta is the default network creation timestamp.
Retirement proof: `/private/var/tmp/rpgmigrate-m7-canonical29ed-admission-20261110-72d46c83/retirement-preservation-release-proof.json`, SHA-256 `6829508641cb6f208e68014925a9fa40acb29e8b7983800289ba1486c469f4dc`.
No new parent lease exists at this checkpoint.
Bootstrap passed, while native E2E and migration execution remain open.
No renewal or new parent lease follows from the first Ready receipt.

The signed isolated M8 candidate `ea51e2375e51adc3d42115c588f5d3b23ea6d4a6`, tree `70198152465b405002ec4bf95d68775fa7342ed1`, composes Apache/GHCR changes without changing the M8 core and passed 398 portable tests.
Composition proof: `/tmp/rpgmigrate-m8-primary-source-composition-20261110/signed-checkpoint.json` (lost 2026-11-12), SHA-256 `51f840b0fcc65eac496ddd93c1f2ab209f1571e7c0f68ecfb100076ee962a39a`.
The earlier independent binary `2f0b077c` from source `611d1a4` passed one catalog and all ten Clone cases on each of PostgreSQL 16.15 and 18.6, with zero failures or ignored cases.
Component summary: `/private/tmp/rpgmigrate-m8-full-component-20261110/0dc035e4bcfa3ec9/summary.json` (lost 2026-11-12), SHA-256 `6babb268e5d86a6d612c785477a071dc882717023909197b7b192e98d4483833`.
The first six-profile workflow attempt on 16-to-16 failed before Cargo or PostgreSQL started because the Podman pasta inspect shape differed from the harness assumption.
It executed zero profiles and produced zero reports, so it does not qualify the three-pair workflow.
Independent cleanup and preservation released the window.
Failure/preservation proof: `/Users/ydixken/.cache/rpgmigrate-m8-workflow-evidence/20261110-16-16-6d0f41c9/independent-failure-preservation-release-proof.json`, SHA-256 `3dc97cf996c6444327a1edf9c2688e2da0461d476360b35d1e7b824e7e8f5608`.
The signed model-only inspect fix uses isolated source `5f287cdd8fa0a79008b8bc0e9b17863725014f0e`, tree `344cc30d8594c87cb550f19f83a10e4fe7c7d42b`, with all 426 portable checks passing.
Signed checkpoint: `/tmp/rpgmigrate-m8-pasta-successor-integration-20261110/signed-checkpoint.json` (lost 2026-11-12), SHA-256 `36ed365b432435481d54a23f1c2bf2318f02f5718b7bf8de608d1c125519f033`.
The fresh 334-entry source export passed verification.
The earlier pasta failure remains RED under its original input identity.
The `5f287` successor compiled, passed stock-libpq smoke, and started owned PostgreSQL 16, then failed with `actual COPY format observation is missing or invalid`.
Accepted complete profiles and reports both total zero.
The attempted profile ordinal and partial case counts remain unknown.
Failure handoff: `/Users/ydixken/.cache/rpgmigrate-m8-workflow-evidence/20261110-16-16-successor-0b65b7a1/successor-workflow-failure-handoff.json`, SHA-256 `33d044ce10a7b3b31956e7f60225290597a2163442aefb031aff273ac71de7a5`.
Independent cleanup released the window and preserved all 424 protected image IDs, 206 volumes, and four original containers.
The new owned `b32` image remains retained, making the next inventory 425 image IDs.
Release proof: `/Users/ydixken/.cache/rpgmigrate-m8-workflow-evidence/20261110-16-16-successor-0b65b7a1/independent-successor-failure-preservation-release-proof.json`, SHA-256 `86b5fa4cb4d50acbfacac02d2f3ffc64642f0720921f4cb3b0c4c80523e6f25c`.
The engine uses extended-query `PQsendQueryParams` framing, while the observer recognizes COPY only through simple-query `Q` messages.
An in-memory reproduction accepts simple-query COPY and rejects the actual extended Parse/Bind/Describe/Execute/Sync framing.
The two-file extended-COPY observer fix passed 31 in-memory wire checks, with four native extended-query cases failing against the original observer.
Frozen patch: `/private/tmp/rpgmigrate-m8-extended-copy-wire-20261110-d418a2fe/extended-copy-wire.patch` (lost 2026-11-12), SHA-256 `5aacc3e9ba6ec280ed19105c3c3440c8f5a921d007d7550dc51cc72fa6f45646`.
Bounded COPY-feature failure capture remains source work.
No live qualification or weaker COPY evidence requirement follows.
The format 9 / metadata 7 candidate remains separate from the primary format 8 / metadata 6 baseline, without capability activation.

The final private M9 plan passed review and its interfaces are frozen.
Focused tests cover settings, persistence, scheduler, producer, and reader packets.
Signed engine core `9eb3154672979dc3ca9c6e560fdca99af6d716b8` passed focused tests and strict Clippy.
Signed operator consumer `e71949fad962db604b45ec052620245a73af80cf` covers ten paths with zero configured lint findings, 243 native checks, and five controller event checks.
Signed H1 packet `0c507a73db3c969d140829d6351e8bbbbf5819e9` passed 21 authority checks and ten focused checks including the new async selector.
These isolated packets have no production merge, push, or activation evidence.
Coherent live replay proof remains open.
Production discovery remains on HOLD.
`REPLAY_SAFE_BARRIER_INTERVAL_MS` remains `None`, release mode is unavailable, and no measured cadence or benchmark result exists.
Benchmark execution remains on HOLD.
The pinned fork `86bf908` default `clone --follow` path uses synchronous grouped CDC with 100-transaction, 10000-row, and 200-ms caps.
Its COPY path uses asynchronous commit.
Conditional asynchronous replay-file settings do not describe default grouped catch-up.
Native `durable` is stronger than native `replay-safe`, without a blanket comparison against fork default CDC.
The formal forty-child A/A campaign remains unapproved, with no measured speedup.

The approved M9 implementation clarification follows the actual public CLI, which has no limit options.
Compiled default limits represent omission on resume and restore the persisted effective limits.
Explicit nondefault internally injected limits remain strict, and the direct strict resolver remains unchanged.
Test omission and compiled defaults through the real CLI path instead of passing stored limits directly to a helper.

M10 source-tool preparation is committed at `81c5b8c035806e4934806caec4d41e7dcd85d660`: nine tooling files and three portable-suite registrations passed 439 tests across twenty suites with zero skips.
Integration proof: `/private/tmp/rpgmigrate-m10-tool-integration-evidence-jckqtq98/integration-evidence.json` (lost 2026-11-12), SHA-256 `591d5168b9475370f918cbedbff1e79fc0abe219d908842fe82ffec62ccf1337`; commit/fast-forward proof SHA-256 `526aedfc157eb9257c28ea0063b268baac15c4ad29e9566f3ef0a462eafdcd14`.
We integrated signed ASAN commit `0b8ba467f2090375b970ac9fa419d2b1a1e326f5` and guide commit `75b8ff2cadf70833473f8188546487863c88adcc` on the engine feature branch.
The actual same-source ten-guide checker passed with Task 3.54.0, 46 discovered tasks, and binary SHA-256 `0531da02f2155eb041df101e08fee3f2004b8032f1eecfdf390fa6625d53d8c2`.
Signed checkpoint: `/Users/ydixken/.codex/tmp/rpgmigrate-m10-docs-asan-proof-fj1326j8/signed-checkpoint.json`, SHA-256 `d06947ab5c422aaae9713fa990e50484dd40868199c29687d92228807e858ecd`.
The built-in guide checker does not enforce capability fixtures.
A separate read-only observation found the capability-fixture check positive.
The 22 portable ASAN controls qualify source preparation only.
The real toolchain remains unqualified, and the public ASAN manifest refuses execution until the binary, symbolizer, build-std graph, and native runtime qualify.
LLVM tools contain no symbolizer, and the project `Cargo.lock` alone does not describe the complete build-std graph.
Signed isolated fuzz source `24783f717ed2a121388aec171afab64c8d7f14e6` on the M9 core base passed five exact CLI/state selectors and strict Clippy.
Its bounded campaign recorded 6686 cases in seven seconds, preserving frozen metadata and pgoutput.
State mutation bytes and positions depend on the actual temporary-directory device/inode despite the fixed RNG seed.
The algorithm and exact selectors are reproducible, while a normalized seed hash does not prove byte-identical executed mutations across hosts.
No actual ASAN or runtime qualification follows from these source and bounded-fuzz checks.
Hosted native image publication/build evidence remains separate from hosted kind runtime qualification.
The Guard's Podman VM transport requires a bounded Linux-native-host portability follow-up before hosted kind runtime activation; hosted offline contract checks cannot close that gate.
Run full scoped lint, including `gocyclo`, on the actual joined source without `--new-from-rev` filtering.
M0–M5 remain closed and M6–M10 remain open; no native operator migration or capability activation is qualified.

### M0: Preserve and reset (rpgmigrate)

- [x] Guarded preservation: `stash@{0}` plus the 51 untracked files are archive commit `8fceb45` on `wip/native-copy-snapshot` (base `bd0925d`, never merged); `archive/stash-0` pins the stash; a tar of the untracked files sits outside the repo.
- [x] `feat/native-engine` branches from `8fceb45`; this roadmap is committed there.
- [x] Remove the superseded plans, the old task ledger, the superseded anchor spec and the previous team's guides and research notes; point `AGENTS.md` and `README.md` at this roadmap; record this session's corrections in `tasks/lessons.md`.
- [x] Record whether `feat/native-engine` builds as archived (`task check`), before any other change.
- [x] Create `tasks/todo.md` from these milestones; add the dated design revision with the amendments above (and fix its links to removed files).
- [x] Remove the M0 drop list; apply the libpq decision; plain CI for fmt, clippy and unit tests.

- [x] Close M0 using the recorded transport/self-check/review evidence and the integrated strict gates at `7b6037f`.

### M1: Contract freeze (both repos)

- [x] Commands, option names, env (`PGCOPYDB_SOURCE_PGURI`, `PGCOPYDB_TARGET_PGURI`, `PGPASSFILE`, `PGCOPYDB_LOG_JSON`, `PGCOPYDB_APPLY_GROUP_TXNS`), exit codes and error classes for the native profile.
- [x] Protocol v1 amendments: version/capability DTO, `native-report:` stdout transport, work identity env, caller-supplied endpos ack, drain proof with `certified_lsn`, cleanup inventory; compare outside ParseReport.
- [x] application_name scheme, owned schema name, workdir and `control/` layout, native runner image selection and tool layout.
- [x] Shared golden fixtures in both repos with tests on both sides.

- [x] Close M1 using the recorded engine/operator checks and review, with the strict-lint/integration hold resolved.

### M2: Clone slice (engine)

The approved [M2 plan](superpowers/plans/2026-11-08-m2-clone-slice.md) freezes the shared interfaces and seven disjoint owners.
Focused receipts cover the seven-case Clone profile on PostgreSQL 16.15/18.6, the schema drift guard on all three pairs, catalog admission and utility smoke, and canonical comparison SQL.
An earlier candidate passed a quiescent PostgreSQL 16 to 16 clone with eight-table schema/data/sequence proof and measured completion, plus operator-bound clone/progress validation by the authoritative Go parser.
The plain-clone alpha passed staged 16 to 16, 18 to 18, and 16 to 18 profiles: eleven cases and six actual Go report validations per pair, totaling 33 case executions and 18 report validations.
These receipts cover consistent parallel COPY, owned marker/generation recovery, first `--restart`, completed retry, drift refusal, read-only progress, and schema/data comparison within the admitted profile.
Final staged checks passed 417 Rust tests, 24 CLI tests, and 183 portable tests, with formatting and diff checks clean; all independent M2 review findings are resolved.
The frozen M2 Clippy run retained 165 library and 19 library-test unused/dead-code errors.
Later M3 primary integration resolves that technical blocker, and the leader accepted M2 closure from the staged evidence.
The final 58-entry runtime/harness input manifest remained unchanged after qualification.
Aggregate matrix and test Task commands were not rerun to exit zero after their earlier failures; the evidence is the final staged receipts, not an aggregate command pass.
The accepted M3 grouped CDC API/producer and strict closing gates satisfy the remaining M2 handoff.
Operator-controller lifecycle qualification remains M6 work.
See the [task ledger](../tasks/todo.md) for exact commands, evidence, and remaining gates.

- [x] Demonstrate a focused quiescent PostgreSQL 16 to 16 plain clone with durable completion and independent schema/data/sequence verification.
- [x] Resolve all independent closing-review repairs for target ownership, marker readback, generated output, and transaction durability proof.
- [x] Pass the final Prepared10 and Clone7 component profiles on PostgreSQL 16 and 18.
- [x] Pass the eleven-case 16 to 16, 18 to 18, and 16 to 18 profiles, each with six actual Go report checks.
- [x] Transport: noncommitting pipeline validation, private error/warning classification, observed commit proof, per-operation I/O budgets, and buffered COPY.
- [x] COPY authority: logged per-table markers, copy generation, exact owned identities, per-run fence, and durable unknown-COMMIT resolution.
- [x] Accept the M3 grouped CDC frontier/`certified_lsn` producer/API and apply hot-path qualification as the M2 handoff.
- [x] Plain-clone order: SQL-exported snapshot, matching-major pre-data, parallel physical-heap COPY with FREEZE, post-data, sequence sync, and ANALYZE on all three pairs.
- [x] Plain clone `--restart` and `--resume --not-consistent`; `list progress --json`; `compare schema`; `compare data --json` with pinned GUCs and the operator's shapes.
- [x] Podman engine E2E targets for all three pairs, controlled partial-copy/utility interruption and resume cases, and independent schema/data/sequence oracles.
- [x] Close M2 using the staged three-pair clone receipts, prior review, and later strict M3 closing checks; no M2 aggregate rerun is claimed.

### M3: Follow (engine)

Native 7 / metadata 5 ongoing-follow smokes passed on PostgreSQL 16 to 16, 18 to 18, and 16 to 18: initial COPY, valid source DML, independent schema/data parity, target frontier progress, forwarded status, and actual source confirmed flush.
The later full 21-case matrix passed on all three pairs, totaling 63 named case executions and 42 actual clone-bound Go report validations while fixtures were alive.
The matrix retains controlled ongoing-follow stops (`successful_follow_exit=false`); it qualifies M3 follow rather than M4 cutover or independent power-loss durability.
Final M4 closure is recorded below; power-loss qualification remains M9 work.
On 2026-11-08, the first full-profile 16 to 16 run stopped RED waiting for initial startup in `before_commit_rollback`, with no passed-case count or full-profile receipt.
No native Error, SQLSTATE, or stderr was retained.
Six threaded tests confirmed that the harness proxy misclassifies ordinary COPY `r`/`w` without a logical handshake; the minimal patch passed all 38 portable checks, but its causal connection to the startup timeout remains unconfirmed.
The bounded PostgreSQL 16 diagnostic stopped RED at the same initial readiness wait before fault injection or rollback, with unchanged Rust and zero proxy errors.
The captured RED at 2026-11-08 22:50:55 UTC showed COPY workers progressing and closing shortly before timeout, without a held fault, SQL error, or sampled lock wait; readiness correctly waits for post-data and anchor.
Replanning found that the before-COMMIT fault gate misses the engine's extended-protocol COMMIT; actual Linux relay measurements also identify likely proxy latency, without proving the startup cause fixed.
The leader approved two harness TCP_NODELAY options and bounded extended-COMMIT/data-transaction witness gates; engine Rust and deadlines remain unchanged, and no next runtime run precedes the frozen fix.
The harness also fixes a confirmed fault-gate bug that forwarded queued COMMIT after backend death; its latest focused checks passed 47 follow and 13 clone cases.
The immutable proof and exact input manifest are recorded in the [ledger](../tasks/todo.md); all four owned containers and the network were proved absent after cleanup.
Prior RED attempts and their bounded startup, fixture, oracle, error-precedence, and identity fixes remain recorded in the [ledger](../tasks/todo.md).

All five [identity UPDATE fix](superpowers/plans/2026-11-08-m3-identity-update-fix.md) packages are frozen; the joined candidate passed all-target check, strict Clippy, and formatting without allowances.
Closing-gate discovery assigns 116 required native cases per major, excluding the twelve-case legacy SourceSnapshot stage removed in the private retirement candidate below.
The exact-tree PostgreSQL 16 Engine50 run stopped with 49 PASS and one FAIL at `checkpoint_live_tests.rs:795`: the assertion expects `fsync on`, while the shared diagnostic requires `fsync and full_page_writes on`.
The unsafe target was refused; the assertion-only successor is integrated at manifest `d85e84396608751e161646a3ecc43bba4eaa30ca360c474e037edd62fb57e8ec`, with 295 of 296 files unchanged and 48 explicit absences preserved.
The successor passed PostgreSQL 16 Engine50, including the repaired `async_barrier` assertion and six live-fault receipts.
Catalog13 then stopped with twelve PASS and one FAIL: CDC tests intentionally commit real heaps into the shared catalog/clone database, and the clone collision guard correctly refuses those leftovers.
The two-file fresh-empty-clone fixture repair is frozen at manifest `69086a1592f1885d948d85de380e7f5537e5e540c44aaf4c7cbf61a04d36057f`, with fourteen focused checks passing and Engine50/Authority19 inventory assertions corrected.
It preserves 294 other files and 48 explicit absences, without changing Rust production code or weakening the guard.
After correcting the matching PGDG 16 tool PATH, ninety PostgreSQL 16 tests passed across seven profiles.
FollowAuthority11 initially stopped with ten PASS and one cancellation-test FAIL; the retained diagnostic lacked a same-PID COMMIT/SyncRep witness.
The test-only readiness successor preserves the safety assertions and records positive cancellation witnesses.
All ten required native profiles then passed on PostgreSQL 16.15 and 18.6: 116 tests per major, 232 PASS, zero failed or ignored.
The result combines 142 fresh tests on source manifest `c155b7c3775dbb7f063dadfcaa054b77d086b193f808b73b9f06d5d50a347839` with ninety retained PostgreSQL 16 tests whose selected inputs remain unchanged.
All owned fixtures and processes were cleaned; the [ledger](../tasks/todo.md) records the exact proof and retained-source provenance.
All eleven disjoint legacy-stage retirement paths are joined in private candidate manifest `3a9e30d41b0fb93da5df02aca8e7abbb78f5189d317361be67a21c7a9ad008c2`.
The candidate contains 294 files and fifty explicit absences, with no production behavior change and native116/COPY40 inventories preserved.
Its portable inventory registers thirteen suites and 254 tests; the final offline checks passed before primary promotion.
The source-snapshot profile passed nine exact cases per major on PostgreSQL 16.15/18.6, with zero ignored cases and independent fault receipts.
Legacy-stage retirement is integrated at source commit `7b6037f9ac8214696936a21c865cd97307cc5c6e`, with the offline gates and independent M3 review passing.
The money/cross-locale clone and source-valid deferred-UNIQUE parity have focused per-major proof; deferred-failure rollback remains an origin-mode Authority obligation.
The retained canonical operator lint run at exact checkpoint `5e8cbcb` failed with 25 diagnostics (24 `goconst`, one `unparam`); the repaired successor below clears the full lint gate.
Canonical `task test` passed all 15 packages uncached at exact clean operator checkpoint `5e8cbcb`; PostgreSQL 16 cluster-free race checks passed 508 checks with two expected skips, and PostgreSQL 18 coverage race checks passed 158 without skips.
Both exact owned PostgreSQL containers and the private envtest processes were proved absent after cleanup.
The twelve-path lint repair at tree `d9003a7939eac53ffe471134196440906794f45f` passed full canonical lint: all sixteen stages, zero issues.
Its full source hash `f9993ced4f02d262c2e41336a08a3c8667f38dd08ce6c3ad31bf459a7c4b8d54` was unchanged before and after verification.
The repair changes equivalent test constants and one production comment; earlier Go checks remain distinct exact-source evidence, without a successor rerun claim.
Unknown WAL headroom remains an operator obligation, and the admitted recovery/profile boundaries remain unchanged.

- [x] pgoutput v1 decoder (B, C, R, Y, I, U, D, T, O; others fatal) with relation generations and PostgreSQL 16/18 regression coverage.
- [x] Receive thread with decision 3 backpressure and status rules; slot health checks; gap refusal on connect.
- [x] Applier: RI FULL keyless (ctid single-row), RI INDEX, unchanged TOAST, multi-relation TRUNCATE (ONLY, never CASCADE), identity and generated columns, non-built-in types as text; pipeline groups; strict dedupe.
- [x] Phase-aware restart/resume admission and recovery within the qualified profile, including resume after the anchor.
- [x] Pass native 7 ongoing-follow smokes on all three declared pairs with independent convergence and actual source-confirmation evidence.
- [x] Pass the exact nine-case native source-snapshot profile on PostgreSQL 16 and 18 with independent fault receipts.
- [x] Pass the full 21-case follow profile on all three declared pairs, with 42 actual clone-bound Go report validations.
- [x] Pass all ten native profiles on PostgreSQL 16.15 and 18.6: 116 tests per major, zero failures or ignored tests.
- [x] Join all eleven authorized legacy-stage retirement paths in the private candidate.
- [x] Pass the retired candidate offline closing gates and integrate the reviewed result into the primary tree.
- [x] Complete the independent whole-M3 safety review and commit source as `7b6037f9ac8214696936a21c865cd97307cc5c6e`.
- [x] Pass the declared M3 fault/recovery, filtered-WAL, keepalive, and round-trip controls.
Parser fuzzing and measured catch-up performance remain M10 and M9 qualification gates.

### M4: Cutover, drain proof, cleanup (engine)

All repaired M4 qualification gates passed on source `2034c420f18ec0f32064eb8bb90c74c14da8a54a8f858095abbb888ab714a926` and ordinary runtime `1fd03e42f9656495816d6c36a5be32eec9fa69a319d2ab28506d9cd979578a52`.
Canonical host lint/test/check passed: 499 Rust tests, 27 CLI tests, and 324 portable tests, with 22 ignored and 107 filtered Rust cases recorded separately.
Fresh SourceCatalog3 and FollowAuthority17 passed on PostgreSQL 16.15 and 18.6, forty cases with zero ignored.
The prior nine-profile 266-case native subset remains separately retained with original provenance; it is not an invented all-current native total.
Follow21 and Cutover10 passed on all three declared pairs, totaling 93 cases, with checked-close GREEN on each pair.
Producer/local report validation remains separate from final repaired-runtime actual-seven Go acceptance.
That single seven-case invocation is private PostgreSQL 18 interoperability with mocked Kubernetes ownership and production admission disabled.
Source/runtime/libpq, 954-file privacy scans, and owned runner/server/network cleanup passed.
The single whole-M4 independent review's P2 is resolved, no findings remain, and the final six-document export is accepted.
The [ledger](../tasks/todo.md) preserves old-runtime RED, repaired GREEN, exact receipts, prior-source evidence, and resource limits.
The separate 24-path operator test-only work is committed as `c262322909ac03a7101ea3bfac2c4823efa62e95`, without activating production native paths.
M4 is complete: the 61-path engine source/document integration is committed and fast-forwarded to primary `feat/native-engine` at `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476`, tree `4e9a151afa82c039aa2bb2c705fded6d358d5d4a`.
The tested source bytes, six accepted documents, fifty absences, and 244 protected files were verified; operator production admission remains disabled until M6.
Local endpos reads are allowed, but informational JSON cannot expose unproven certified or durability counters.
Durable replay, drained status, and proof reads require positive fenced observation, with bounded explicit refusal when busy or unknown.
The native operator must sample and persist one E, then retry that identical E explicitly after a bounded refusal.
The worker processes persisted E independently of acknowledgment; the M6 standalone helper retries identical E for final positive proof after the worker exits.
We add no IPC or fence handoff for this path.
The qualified native 8 / metadata 6 runtime has 29 control columns and refuses older formats; uncertain slot incarnation or reappearance must be preserved under the accepted exclusive-administration prerequisite.

- [x] Retain the ancestor 27-case engine-producer PASS across all three pairs.
- [x] Pass the exact seventeen-case FollowAuthority profile on PostgreSQL 16.15 and 18.6 for the focused repair successor.
- [x] Pass canonical host `task lint test` and `task check` on the repaired utility successor.
- [x] Pass the exact three-case Schema profile on PostgreSQL 16.15 and 18.6 for the repaired utility successor.
- [x] Pass Clone7 on PostgreSQL 16.15 and 18.6 after restoring the source-parity marker-test literal.
- [x] Pass exact Engine50 on PostgreSQL 16.15 and 18.6 after the selector and M3 assertion restorations.
- [x] Pass the nine-profile M4 native closing subset on PostgreSQL 16.15 and 18.6, including uninterrupted Receive11 cadence coverage.
- [x] Retain all ninety reviewed-ancestor Follow21/Cutover9 runtime case executions across the three declared pairs.
- [x] Pass fresh checked-close SourceCatalog3/FollowAuthority17 sampling on both majors: forty cases, zero ignored.
- [x] Pass the first repaired Cutover10 pair on PostgreSQL 16 to 16, including checked-close GREEN and healthy retry.
- [x] Qualify the checked-close successor, including SourceCatalog3/FollowAuthority17 on both majors, the new 93-case matrix, and actual-seven interoperability on its new binary.
- [x] Record the separate 24-path operator test-only commit `c262322909ac03a7101ea3bfac2c4823efa62e95`.
- [x] Close M4 at engine source commit `67f7fcbff0a4d35cb41ba3dae8da0fe238d87476` after verified integration and the separate operator test-only commit.
- [x] Pass fresh current-code runtime matrices and actual engine/Go interoperability, then complete M4 closing review/gates.
- [x] `stream sentinel set endpos <lsn>|--current` and `get --endpos|--replay-lsn|--json` per decision 6.
- [x] Drain certification, final sequence sync, exit 0 only with proof.
- [x] `stream cleanup`: owned slot and publication only, idempotent, workdir kept for later compares.
- [x] Engine E2E: full follow migration under pgbench load (including `synchronous_commit=off` writers), endpos, drain, compare match and cleanup, plus a crash in each phase, on 16→16, 18→18 and 16→18.

### M5: Packaging and engine CI (starts with M2)

Local native ARM64 image `b4631063096125a84d3428571fe81d7057c91cf3356adf54cc738744709e52b0` passed its build/read-only runtime smoke on input digest `296279da95b924a59a903034a89f30853d037039368026859ce5d6ecc6eb9d9a`.
The [ledger](../tasks/todo.md) binds that retained image evidence and the same-review portability correction; the test-only R2 change is outside the 65 image inputs.
The first [hosted run](https://github.com/ydixken/rpgmigrate/actions/runs/42625499630) on pushed `34208b6` ended with five PASS and three FAIL jobs, including all three migration pairs passing.
The docs MD038 failure is fixed in `55ea9c7`; the Dockerfile heredoc-parser failure and version-dependent test are corrected and integrated in `7c04dff673f41b3a27c5d0afbc6dbe5e6b551c03`.
That exact feature SHA is pushed and remote-verified; its [push-triggered hosted run](https://github.com/ydixken/rpgmigrate/actions/runs/42630883414) passed all eight jobs, with all five artifact ZIP digests and both native image input bindings verified.
Hosted AMD64/ARM64 images passed uid 65532/stock-libpq checks on the exact 65-file input digest.
M5 is complete after the same reviewer’s final acceptance of hosted multi-architecture, read-only runtime, source/artifact, clone-inventory, and retained stock PostgreSQL 16/18 evidence bindings.
The retained database evidence preserves its original 266-case subset and forty-case sampling/composite provenance; exact source/Cargo/build/Taskfile/test/harness bindings are unchanged from `67f7fcb` to `7c04dff`.
This acceptance claims neither a fresh aggregate database run nor migration using the production image; broad operator Go checks are not applicable to engine-only packaging.
The operator-publication P1 is resolved by the dated explicit GitLab mirror authorization; it remains separate from engine M5 qualification.
Tags and releases remain unauthorized.
The dated M7 amendment permits only development GHCR publication, and these checks do not close M10.

- [x] Runner image: rpgmigrate, sh, psql 18, PGDG client tools 16 and 18 under `/usr/lib/postgresql/<major>/bin`, GNU coreutils and sed, uid 65532, read-only rootfs, multi-arch, smoke test.
- [x] GitHub Actions: lint, test, cargo-deny (licenses, advisories), docs lint, development-image clone jobs on hosted runners, and both native image builds.
- [ ] Release GHCR publication on version tags only, pending separate release authorization.
Development publication follows the dated M7 amendment.

### M6: Operator native adapter (operator, on a branch from `feat/rpgmigrate-native-operator` after a fetch)

#### Evidence checkpoint, 2026-11-10

Production native admission still returns `UnsupportedEngine`; no actual native operator workflow is qualified.
The accepted operator source checkpoint is signed `93bd54c557d70ed2daa7be3120ea093b31cc93cf`, tree `a0739a53ca5edcc816e1904bdb87df3b6bb3a691`; its conditional fast-forward from `cf22e84` verified a clean authoritative checkout before and after integration.
Five signed completion commits add helper receipts (`30686fa`), evidence retention (`50be291`), cleaned completion (`d937958`), follow/E/ACK dependency composition (`d87c920`), and plain-clone completion/verification (`6a27266`).
Each prefix compiled and passed scoped lint with zero findings, with byte-identical qualified final-tree evidence reused where recorded.
Completion checkpoint proof: `/tmp/m6-completion-wave-checkpoint-20261110/manifest.json` (lost 2026-11-12), SHA-256 `860652704912ecd72cf14a6d912e8bbc86d74c41bc5fddbfab437755f966eb40`.
Sampler publication and metrics then entered signed checkpoint `f8214a6` through an exact sealed seven-file relocation into an isolated worktree, preserving all 662 tracked peers.
Its source-equivalent 73 named focused race checks passed without failures or skips; fresh compilation and scoped lint passed with zero findings.
Sampler proof: `/tmp/m6-sampler-checkpoint-integration-20261110/checkpoint-manifest.json` (lost 2026-11-12), SHA-256 `a1835132bd9fd90de693cd2dcfdaca85ef587f338f121a7f70c213f158c7407e`.
These are source/component checkpoints; no broad Go suite, whole-M6 review, actual SQL/Kubernetes/native operator workflow, or runtime qualification follows.

The root-approved completion-rejoin amendment requires separate partial-cleanup admission for flags `00`, `10`, and `01`, with actual target-row completion authority and completion-only credential-free version discovery.
Ordinary active admission and the fully cleaned predicate remain unchanged; status and resource inventory cannot authorize completion.
Plan: [completion-rejoin amendment](superpowers/plans/2026-11-10-m6-completion-rejoin-amendment.md), SHA-256 `1b4dfa9c7051a4037c92be38520d09bbcf4ce52f0e8a7e3f6d2fe5d9fd294dbf`, recovered byte-identical from the planning session log after the reboot wiped `/tmp`.
The 46-path completion/rejoin/finalization/disposition join is signed in API checkpoint `3ef73545c9f074e1a50cd2c042132977ef170055`, native checkpoint `1d0d67b5001aae48774998f8f4bf5861e5fbceea`, and controller checkpoint `0346f9e8534174243f2d07470afcec5d8275e6de`, tree `1eb28adac4088756694e56006edf3240fbb6a8d0`.
Its actual production/controller builds, four-package compilation, 807 named/154 top-level focused race checks, and scoped lint passed with zero failures, skips, or lint findings.
The [ledger](../tasks/todo.md#evidence-checkpoint-2026-11-10) binds the immutable source and qualification proofs.
The five-path main dispatcher is integrated in `cf22e84`; its actual production/controller builds, 71 focused race checks, and scoped lint passed, with production admission still closed.
The finalizer successor includes the production constant-build repair and stale-failed-condition repair; its actual production build, 39 affected race checks, and scoped lint passed with zero findings.
Its older 335-race-check packet retains separate provenance and is not relabeled as qualification of the repaired successor.
Finalizer proof: `/tmp/m6-clone-stale-failure-repair-export-20261110/manifest.json` (lost 2026-11-12), SHA-256 `3dfc5322c44bb678da6d966621a2b91a495bc86590099ce0646d2bbf75b1525c`.
Dispatcher source integration does not qualify terminal replay after helper disposal; exact-UID disposal and terminal replay still require composed lifecycle qualification.
The root-approved interface freeze retains atomic known-create `nativeJobUID`/`nativePVCUID` receipts, refusal to adopt absent receipts, invocation-bound Pod identity, worker TTL retention, and independently observed quiescence after TERM.
Native evidence-bearing helper retention is checkpointed; exact-UID disposal and terminal replay still require composed lifecycle qualification.
Native runner-profile Task 1/Task 2 packets are frozen with passing component checks; actual NSS, authentication/TLS, PVC/provider behavior, and canonical runtime admission remain unqualified.
Canonical NSS/resolver guards and `UnsupportedEngine` remain closed; production runner-profile qualification, three-pair native acceptance, broad Go gates, and the whole-M6 review remain open.
Actual Kubernetes TERM and the full native lifecycle remain unqualified.

Canonical policy preparation is integrated in `93bd54c`: retained resolver arguments and nonempty `sslkeylogfile` refusal passed 90 Python tests, eighteen native race checks, five focused controller checks, builds, and scoped lint.
Its resolver policy still unconditionally refuses; source integration grants no runtime activation.
A separate active-CRI metadata candidate replaces disk-dump authority in source work only; no positive authentication/runtime proof or NRI/configuration change follows.
The [ledger](../tasks/todo.md#evidence-checkpoint-2026-11-10) binds the preparatory export.

The bounded actual metadata probe refused before Jobs: `containerd config dump` exited zero with 394 stderr bytes, and the frozen metadata policy rejected any stderr before parsing.
Exactly one owned namespace was removed and verified absent; no Jobs, Pods, or Secrets were created, and authentication/TLS Step 2 remained unstarted.
A later read-only taxonomy found both a version-migration warning and an ignored unknown TOML plugin-key warning; a blanket stderr exception is not approved, and effective NRI disablement remains unproven.
The separate deadline candidate passed offline host/Go/scoped-lint checks and preserves original clocks, requiring 3300 seconds cold or 2400 seconds with a complete same-source receipt.
The [ledger](../tasks/todo.md#evidence-checkpoint-2026-11-10) binds these proofs; neither metadata nor authentication is qualified.

The preceding `aaad6c7` lifecycle checkpoint added identity receipts/worker TTL, retained TERM primitives, and bounded clone capture/parsing, with 288 named focused race checks and nineteen final-proof-join race checks.
Prior lifecycle proof: `/tmp/m6-lifecycle-components-integration-export-20261110/checkpoint-manifest.json` (lost 2026-11-12), SHA-256 `15fc47e882e9863980e3440b716afc8f13e5d3e4f1a3b6db64071f26baac75f7`.
The earlier follow/E/ACK/target-join export passed 81 focused race checks and zero scoped lint findings before its composition in `d87c920`; it ran no actual SQL, Kubernetes, or native engine process.
Prior follow proof: `/tmp/m6-follow-flow-export-20261110/manifest.json` (lost 2026-11-12), SHA-256 `1f78346be36e277a54d3f9cfaa0935f35f51af9af723a562034e401ef9674040`.

The preceding operator checkpoint `9fd9061790f158ee8c65908113029ac667da68ba`, tree `53d2abe0385d28918d99d45cb71057dd24575739`, added worker locale pinning (`73c8a9b`), owned cleanup builders (`35f4ae3`), informational sampling (`f1ca944`), and identity status fields (`9fd9061`); each intermediate tree compiled and passed scoped lint with zero findings.
Prior proof: `/tmp/rpgmigrate-m6-ready-exports-composed-20261110/integrated-manifest.json` (lost 2026-11-12), SHA-256 `4709a0d229a45d59d0f59de328fec702b39bf22bc8144032f604fc9f0a276b25`.
The earlier retained-stop component export passed focused race tests and lint; its receipt retains component-only scope.
Prior proof: `/tmp/m6-retained-stop-export-20261110/manifest.json` (lost 2026-11-12), SHA-256 `1819a0f3f832e56fd23c256736282363d3b0253b2f095cf8d0bac9adba99f906`.
The runner-profile qualification plan is `/Users/ydixken/.codex/tmp/2026-11-10-m6-runner-profile-qualification-tfhgdc6n.md`, SHA-256 `3b8e016d201b55197c8409c40a5922fff9d5ce3092924a8f11803a8d0a694e69`.
Actual NSS, authentication/TLS, and three-pair acceptance remain open; image metadata is not runtime qualification.

Fresh native ARM64 release packaging on engine `254ab5ba103bfe75a2e1dcd8acad0b9720e88ab8` passed with config `0c755f5e36f3fb88c9bd1ecaa53759d0a741271696b552633eec20fef9b674a9`, binary SHA-256 `4bded8d59ccef2cf73b7cc88e76ec8d9d626caf0b1fc96f879ac0be2bedc9803`, and selected manifest `5af74fb39dfe31057273ae5a9d8268f5f1b982a8edd3868de392c8e69d6883f1`.
The actual image passed Linux-seven capability discovery and the UID 65532/read-only-rootfs smoke with stock libpq 18.6 and clients 16.15/18.6.
Proof: `/private/var/tmp/rpgmigrate-m6-runner-254ab5ba-8z211_yv/packaging-final-proof.json`, SHA-256 `09e096a066f94d6e515e8b2bbf3952575b4147c5914e884118e4e75372f0c265`.
This closes the fresh ARM packaging prerequisite only; actual authentication/TLS/resolver/PVC, three-pair native operator acceptance, matching AMD64 packaging, broad Go gates, and the whole-M6 review remain open.

#### Retained source and component evidence

Production native admission still returns `UnsupportedEngine`; M4's engine and mocked-Kubernetes helper evidence does not qualify the operator lifecycle.
Engine FD implementation is integrated at `3c4d0a5a6212de642aa8f205989fadf80b64ee49`, with retained thirteen-case Linux prerequisite evidence and host library Clippy/formatting checks.
The exact joined FD/capability Linux producer check passed protocol ten/CLI31 plus actual binary three-field Linux-seven discovery and executable CLI28; this source qualification preserves the non-Linux six and operational arrays.
The CLI selector was ignored and the broader 28-case executable script ran once; no rerun or fabricated partial-run claim is recorded.
Old six-name discovery still parses but cannot satisfy native M6 before credentials; actual authentication/TLS, real PostgreSQL dump, image, NSS/resolver, lifecycle, broad Go, and whole-M6 review gates remain open.
The clean central operator capability checkpoint is `14b73f5980768841c68dcb0d699e5889e88d80ad`, following comparison `351aa3`; earlier control/retry evidence retains its original 105031-byte Python input.
Control and retry Reader have focused source/lint evidence only; interfaces/dispatch, runtime target-retry producer/Reader, and owned-cleanup comparison remain open, without activation or new producer/image qualification.
The clean central operator comparison checkpoint is `351aa3` with focused comparison/transport coverage and canonical 111754-byte Python/54-mechanism-test binding; it grants no runtime qualification.
The ten-capability operator consumer is now central and source-qualified for six-name pre-credential refusal, Linux-seven mocked admission, and known-extra read-only cases; runtime guards remain refused.
Full canonical in-memory admission remains authoritative; correlation hashes and informational lag supply no target durability or completion authority, runtime acceptance, or new image qualification.
Task 0's five-file pure-helper export is frozen with nineteen Python and eighteen focused Go tests passing, including exact PG16/18 SQL fixture equality.
The earlier admission-reader handoff separately passed thirteen top-level tests and 330 focused checks with zero failures or skips; the final relay-aware Reader composition passed fourteen top-level tests and 344 focused checks with zero failures/skips against the exact new relay/transport inputs.
The new relay export passed 29 Python and nineteen focused Go tests (including ten relay subcases), with five mutation controls RED.
The new transport export passed 97 checks/92 leaf cases; these are focused mechanism proofs, without runtime qualification.
These focused checks do not qualify packaged Python/libpq, database, Kubernetes/PVC, or opaque-file runtime behavior.
Four clean M6 source/tool commits (`ccda81f3`, `3d1bfe6f`, `f6f7de44`, `32dbcfc`) are integrated by fast-forward in the central operator feature branch.
The exact fifteen-path/557-file checkpoint passed both packages with zero lint findings and retained focused tests; remote origin remains `5616d82`, without a checkpoint push or M6 activation/review/full-Go/runtime closure.
The second four-commit M6 packet (`f28c450`, `b8c6f51`, `1a569b7`, `4ecbd69`) is accepted and fast-forwarded into the central operator feature branch at `4ecbd69cc9e56e4710096b6f0475fc7d2a62fe8b`, with all fourteen paths/560 files verified and clean.
This checkpoint adds no push, CI, or runtime result.
The [worker construction/dispatch packet](superpowers/plans/2026-11-09-m6-native-worker-construction-dispatch.md) is now integrated in the central operator branch at `cba3fa03fe051432da3ef2650db2db2c8a9b2ad4`, with eleven paths/566 source files verified, each commit prefix lint-qualified, final joined lint zero, lifecycle 108 checks, and eleven exact Linux engine CLI cases passing.
The integration-only retry display-label lint correction was refrozen under strict lint; it is not a gate exception, and top-level `UnsupportedEngine` remains unchanged.
The approved [discovery/preflight plan](superpowers/plans/2026-11-09-m6-native-discovery-preflight.md) separates a credential-free version Job from fixed Python DB preflight, requires actual capabilities before credentials, and reuses no legacy raw-DSN argv or raw stderr.
The dormant preflight source checkpoint is fast-forwarded into the clean central operator branch at `727e45da94a5b2d099e97380e086a36206e68451`, with 575 source paths verified against the accepted prefix/source receipts; no broad rerun, push, CI, or activation follows.
Canonical secret-bearing `/tmp` is Memory-backed at 64 MiB and builders must consume that canonical materialization without overrides; runtime qualification and activation remain closed.
The preflight field is not injected and resolver policy still always refuses; remaining consumers, runtime, whole-M6 review, and closing gates remain open.
The earlier `96d3707` six-name source checkpoint remains historical evidence; the [native protocol](native-protocol.md#discovery) now defines the post-cap Linux-seven/non-Linux-six contract with fixture-only metadata.
The operator six-capability fixture mirror `b590` is now central; source discovery still does not qualify a newly built executable or image.
The new engine candidate and central operator mirror contain 134 identical fixtures, preserving empty/six-name parser fixtures plus Linux-seven producer metadata; engine source integration and runtime qualification remain separate.
The producer/help, portable fixture/consumer, and packaging-smoke packets have focused source evidence only.
Those source packets and historical M5 `b463`/hosted `7c04dff` receipts retain their original scope; the fresh ARM packaging proof is recorded in the 2026-11-10 checkpoint, without authentication/migration runtime qualification.
The fresh ARM packaging prerequisite passed as recorded in the 2026-11-10 checkpoint; actual-image M6 three-pair native acceptance remains open, and AMD64 retains packaging-only scope until exact-image native database qualification in M10.
Production `UnsupportedEngine` and resolver refusal remain unchanged.
The earlier remaining-adapter proposal required root authority correction; the 2026-11-10 checkpoint records the root-approved interface freeze, with implementation and runtime qualification still pending.
This packet adds no DB/operator-migration qualification, push, CI, whole-M6 review, or full-Go result; production activation and runtime closure remain open.
The resolver disposition favors qualifying the existing standard ClusterFirst sandbox resolver; actual runtime/NSS/HOME bindings remain gaps, with no ConfigMap, numeric-only, or HOME change authorized.
The [dated delivery amendment](#m6-default-kubernetes-delivery-amendment-2026-11-09) remains subject to integration and runtime qualification.
The [dated explicit-file/cap amendment](#m6-explicit-connection-file-and-cap-amendment-2026-11-09) has a cap-only admission handoff with nine tests/105 checks on the old relay input; it does not qualify the new Python binding implementation.
A tiny actual Linux ARM primitive probe passed six nonconnecting groups on M5 image `b463`; the later metadata probe found image ENV HOME absent and Podman-synthesized HOME/passwd `/work`, without proving immutable-image/containerd HOME.
Stock libpq 18.6 and absent default paths were observed, with exact probe cleanup/original preservation; TLS/database/Kubernetes/full-helper, ordinary-file, service, and resolver qualification remain open.
The approved Python/ctypes/libpq exception and no-added-Secret-read/RBAC boundary remain in force: bind actual pod/template references, stable privately consumed bytes, and SQL identity rather than latest-Secret-version attestation.
The user explicitly approved existing operator-feature GitLab mirroring on 2026-11-09; the prior authorization hold is resolved, with CI/mirror outcomes tracked separately below.
One independent whole-M6 review belongs at milestone close, and users retain write fencing/outstanding-transaction duties through cutover.

- [ ] Engine-aware argv0 and scripts (worker, verify, cleanup, compare, catalog, endpos exec); lift `UnsupportedEngine` for the qualified profile; `--rpgmigrate-runner-image` flag and chart value.
- [x] Implement and checkpoint dormant native discovery/preflight source, retaining the closed runtime/activation gates.
- [ ] Preflight: `version --json` handshake into `validateNativeConfig`; native pg_restore gate by target major; skip the origin-grant probe; PG16 target allowed for native.
- [ ] Phase from `native-report:` lines; native drain verification bound to status endpos plus a psql read of the owned control row; cleanup requires `complete=true`; plain-clone completion fails closed on a missing report.
- [ ] ORed application_name patterns for the retry-gate census, sampler and zombie reaper; native error-class prefixes in the log matchers.
- [ ] Unit and envtest coverage; operator docs for the native engine.

### M7: Operator-driven E2E

#### Approved registry amendment, 2026-11-10

The user approved direct public-registry pulls for upstream CloudNativePG, operator, PostgreSQL, and other public dependencies, each pinned by digest.
This direction supersedes the local-registry proposal and the M7 archive/containerd-import, alias, and tag-retargeting path.
We may publish manager and native-runner development images as public GHCR packages under the approved user namespace.
Use existing manager `ghcr.io/ydixken/pgcopydb-operator`.
`ghcr.io/ydixken/rpgmigrate` is the native-image example.
Use immutable `dev-<sha>` image tags and reference the resulting image digest from kind.
This approval covers development publication only.
Version tags, `latest`, releases, and main pushes remain unauthorized.
M10 offline candidate archive retention remains a separate qualification requirement.
Preserve exact ownership, the original execution clock, resource caps, and cleanup guards.
Keep archived source and old RUN evidence unchanged.
The old Ready RUN held before LOAD under S1 does not qualify registry pulls.
Implementation planning and fresh registry-path runtime qualification remain pending.
M6, M8, and M10 work continues.
M6–M10 remain open and benchmark execution stays on HOLD.

#### Evidence checkpoint, 2026-11-10

The retained canonical `29ed` interpreter-pair admission passed live supervisor handover, then PULL failed; a separate actual-help check shows crictl 1.36 rejects canonical `--timeout=10`.
The original pull stderr remains unknown; the later bounded actual-help check accepts `--timeout=10s` with exit 0 in 0.112 seconds, which proves help syntax only.
Exact container-only retirement is complete and the lease released: three owned nodes were removed nonforced, identity-checked S2/caffeinate processes are gone, and all 429 images/215 volumes/original four containers/two networks are retained.
The approved three-file shared `10`-to-`10s` implementation is active in isolation; fresh public-manager provenance, lock, and source rebind are required before kind retry, with no retry pull or M7 acceptance claimed.

The fresh `f59a7f4b` M7 source passed UP, supervisor handover, and LOAD for the PostgreSQL 18 component/API profile.
Its legacy gate FAILED during storage-probe image-ID recognition: the actual probe Pod Succeeded with exit zero and bound PVC/PV, but the wrapper refused the exact declared tag at its selected manifest.
Go migration tests and the metrics observer did not run; no legacy or native workflow acceptance follows.
Failure proof: `/private/var/tmp/rpgmigrate-m7-tagged-legacy-gate-6s2i181n/final-failed-gate-proof.json`, SHA-256 `269d0780ab532117842ce072674dbd4356c41c21f7873c64050fa61872afcc92`.
The private declared-imageID fix has source inventory SHA-256 `b5da9c9aa6a0142fd27eaf69950d639c9be68363376be2f930cbe3662f2259a3` at `/private/var/tmp/rpgmigrate-declared-imageid-candidate-zad1graq/source`.
It changes five paths, preserves 527 peers, and passed one full 259-test Python run, thirty top-level/186 named focused Go checks, and zero scoped lint findings.
Independent pure-data replay accepted the retained actual Pod data under `b5` while `f59` refused it, and rejected all eleven negative controls; fresh workflow acceptance remains pending, and the retained `f59a7f4b` gate remains failed.
The [ledger](../tasks/todo.md#evidence-checkpoint-2026-11-10-1) binds this data-only proof.
Exactly six approved old kind volumes were removed after fresh ownership/reference barriers; the cleanup preserved four original services, 206 protected volumes, and 395 images.
Proof: `/private/var/tmp/rpgmigrate-m7-unused-volume-safety-c3anxbon/final-candidate-proof.json`, SHA-256 `06fe68baef50633e019b070f32605ebdb4f39b9eaeea4ee4f426b2bc09428f12`.
Exact retirement of the failed `f59a7f4b` S2 and three nodes passed, including process/cgroup absence, preservation of all 212 volumes and 395 preexisting images, and protection of four original services.
The narrow retirement-only exception preserved the historical sticky handoff event and grants no runtime-health adoption.
The separate six-volume cleanup passed without force or generic pruning; its exact final proof retains the bounded implicit-network metadata exception.
The fresh `b5` RUN passed all eighteen LOAD entries in 693.867 seconds and valid bootstrap, then its Go gate failed before any selected spec and without authenticated manager-version evidence.
The frozen error path discarded the exact failing assertion, so its cause remains unknown.
The failed workflow is preserved under the fixed 2026-11-10 06:30:27.495346–08:30:27.495346 UTC lease; no retry, retirement, or probe is qualified by this result.
A safe-observability successor has passing focused source checks and lint, with independent reproduction and integration still pending.

The M7 `c275` source passed fresh UP/CHECK and qualified supervisor handover after its retained PREPARE/CHECK success.
Its first LOAD pair FAILED: default `ctr images tag` dropped labels and CRI retargeted the existing config-ID key from the prepared child manifest to its one-child archive index.
No legacy or native migration test ran.
Failure proof: `/private/var/tmp/rpgmigrate-m7-component-api-load-khjk5rqw/sealed-first-failure-manifest.json`, SHA-256 `31b5f259d277ad47c3ea83ea7916737bfa03d7fbe1cc4247598959eb5b72dada`.
The `f410` source candidate uses the exact `--local` tag fix with the preservation contract unchanged and passed 246 Python tests.
An independent experiment on an untouched node preserved all prior descriptors, labels, and the config-ID child key, adding exactly one expected alias.
Experiment proof: `/private/var/tmp/rpgmigrate-m7-local-tag-experiment-24mmp_m9/final-experiment-proof.json`, SHA-256 `f024de59fd9f1564ac81ef1a2592f3c2f22debc1db9d9519c8d598393b7330ba`.
Exact retirement of the old `c275` nodes and S2 passed, with their processes/cgroups absent and all 206 volumes, 383 preexisting images, original services, backups, source, and failed RUN evidence preserved.
Retirement proof: `/private/var/tmp/rpgmigrate-m7-c275-retirement-sx7gdvy6/final-retirement-proof.json`, SHA-256 `f184619aef2dbd2f3c7b408415095e4f6d0320da7c0227ac4fc85f5e8f3a9d0a`.
Fresh `f410` PREPARE/CHECK passed for RUN `/private/var/tmp/rpgmigrate-m7-local-run-g3o_x2i9`, RunID `048dc2ed3e044990bc25f7a11d7a91e0`, using manager config `09b96b6eed4b3a55f6a2933c7f98fa0e765191a5559d54b3a1c555388ca8a660` and build VERSION `m7-f41042c308ba`; the preparation proof does not qualify the runtime version.
Preparation proof: `/private/var/tmp/rpgmigrate-m7-local-preparation-1k5qtccq/final-artifact-packet.json`, SHA-256 `16ded43c7d7fd9dae5075eba3d357ebd1fcb09cb13a5326ec21b8e67af53ce5a`.
That earlier fresh UP command returned under a fixed lease ending at 05:06:57.016634 UTC on 2026-11-10; it did not itself qualify full-ready CHECK, supervisor handover, LOAD, or legacy execution.
The old lease was not renewed, and the historical `c275` LOAD receipt remains FAILED.
The capacity baseline is fourteen CPUs, 32 GiB memory, and 160 GiB disk; runtime admission still requires fresh capacity evidence.
M0–M5 remain closed and M6–M10 remain open.
Benchmark execution remains on HOLD.
Main pushes, tags, and releases remain excluded, with development GHCR publication governed by the dated M7 amendment.

#### Retained bootstrap and failed-run evidence

The earlier `c275` preparation used source `/private/var/tmp/rpgmigrate-component-api-avwmu909/source`, digest `c275c790cbe74795f08b0936e2bc3b12e3fb0273c39ac8424708df2956fd3cac`, with 242 Python checks and the retained unchanged-input 151-check Go gate.
PREPARE passed in 113.215 seconds and CHECK in 3.484 seconds for RUN `/private/var/tmp/rpgmigrate-m7-component-api-run-51rlqh3s`, RunID `7193d24a35e44a758cb9969cdf2937c8`.
Preparation proof: `/private/var/tmp/rpgmigrate-m7-component-api-preparation-5fre5jm0/final-artifact-packet.json`, SHA-256 `33b870064d96d327d5d5f8561a4a57fa08721afde0b4b6f9a2d81085ea0ca610`.
The prior failed legacy RUN was retired through exact three-node and separately verified unused-volume removal, preserving originals and failure evidence; its legacy gate remains FAILED.

First UP passed on the corrected 530-entry source `5cc72893`: three Ready nodes, API readyz, and verified aggregate caps of six CPUs/12 GiB.
The frozen seven-role inventory is legacy PostgreSQL 17 only; native/PostgreSQL 16/18 entries are null placeholders, and this inventory does not establish M5 native-image qualification.
Tracked CHECK before load and load passed; phase is `Loaded` with six loaded-digest receipts and positive post-load readiness.
The original finite supervisor hit a read timeout during load; guarded DOWN correctly refused the active RUN lock and exited its handoff without deletion or source/receipt repair.
The watcher was restored under the original deadline with a recorded 648.049941-second gap, then intentionally stopped before expiry when the blocked Loaded workflow was retired.
Exact three-node container/process/cgroup removal is complete under standing approval; all 203 volumes, 339 images, immutable network properties, original four services, backups, source, and Loaded RUN evidence remain preserved, with actual CPU/RAM reservation released.
The CHECK state-loss and storage index/child comparison fixes passed 189 source-level unit cases on frozen 530-entry source `5dcceda`; fresh PREPARE/CHECK passed on that corrected source, without a new UP or workflow qualification.
The earlier private S1/S2 implementation `f57314` passed 33 fake controls only; the later `c275` checkpoint records qualified live supervisor handover.
The approved [tracked kind-only Go pre-uninstall build-info hook](superpowers/plans/2026-11-09-m7-tracked-build-info-capture.md) supersedes the private observer implementation proposal and is included in the final source freeze; metrics permission remains unapplied.
The final Go six-file packet `9fcb71e2` passed 160 source checks with zero lint; Python 198 and the guide are final.
The final 532-entry dirty-source freeze `d5698bf5f130519336382828ecbfcf89995937df53730fb35537cca55de8897f` retains legacy provenance gaps; the full 31-path Git delta is neither committed nor lint-clean.
Source-equal controller/API/CRD/module evidence binds accepted M4 `c262322`, without qualifying all M7.
The later legacy gate failed before CNPG apply: the pinned kubectl renderer emits 26 concatenated JSON objects, which the old single-document `json.loads` rejected.
A read-only exact-renderer reproduction confirms that framing defect; successor source `8af5c480741caa760294c52ece74c4a89c586bab80d4995a522acd4dcc3be65d` changes only three files with 529 peers preserved and matches the 214 focused passing checks.
Phase-1 retirement retained all 206 volumes; the later independently barred, exact nonforced reclaim removed only the three new unused volumes and preserved the protected 203.
The approved disk resize from 128 to 160 GiB passed with the same fourteen CPUs/32 GiB, preserved PostgreSQL 16/18 IDs/ports, restored M0 and three original running services, and retired f8 still stopped.
The fresh `8af` RUN passed PREPARE/CHECK/UP/LOAD with six archives/eighteen image pairs, then the legacy gate failed in 334.581 seconds (Task 201) before the Go suite.
CloudNativePG hit CreateContainerError because CRI normalized the existing raw import alias to a missing `docker.io/library/import-2026-11-09@sha256:2e4051608564bbe757b099067e2df55f5c4a960b6191837ce448ec3d1760b297` key; retained content and source evidence establish no alias-deletion cause.
One exact nonforced post-failure alias addition on the Pod host made the same original CloudNativePG Pod Ready with restart count zero; this mechanism experiment leaves the historical gate FAILED and supplies no Go/M7 pass.
The private M7 load-alias/actual-CRI-imageID Python, Go, and guide fixes preceded the composed `c275` successor recorded above; the prior failure/experiment remain unchanged.
A separate four-CPU/4-GiB focused Linux Rust window with a 6-GiB growth envelope is being planned; it is not a benchmark reservation or result.
The earlier successor required a new manager/archive/RUN; old acceptance was not reused and no M7 stage completion follows.
The old `5dcceda`/`47a` prepared RUN remains obsolete; the [ledger](../tasks/todo.md) records the failed handoff and experiment separately.
These bootstrap/load observations do not qualify the legacy migration workflow, native inventory, PVC behavior, or full M7 lifecycle.

The leader accepted the [M7 bootstrap slice](superpowers/plans/2026-11-08-m7-kind-bootstrap.md) for code-only implementation by separate lifecycle and suite owners on isolated operator branches.
The frozen eleven-file M7 join includes 57 passing bootstrap controls and its self-check plus focused suite checks; full custom lint remains a closing blocker.
Actual artifact preparation refused the pinned legacy runner: its binary is `0.18.129.gd7547b5`, while the required builder baseline is `0.18.144.g86bf908`.
A separately qualified local ARM64 v0.25.0 candidate matches the exact required `86bf908` builder binary and tool/nonroot/workdir contract; its inventory-pin correction is guarded-joined.
AMD64 runtime, manager/archive/cluster, and full gates remain unqualified; the original refusal is retained.
The ARM manager build passed, but Podman OCI save was rejected because gzip recompression changed the pinned manifest.
Artifact preparation then passed its byte/receipt checks using existing skopeo with published pins preserved: seven role archives, exact selected-node-child verification, and the trusted legacy runner binary.
The approved reproducibility/capacity plan is `831ebb2e4ac1b723b98c5d4c3e1311fce6a1b1f14e9138888cf7aaf3c00418d2`.
Task 10 is frozen at manifest `5069c96437b0268ce7db86507d7f7d4c5e804fbc992197ee540a86cea6d1a4fb`, with 131 focused checks passing.
Task 12 is frozen at manifest `85399fc3e8ebde3aa55a5c8101fd9114503c7c4ef7466e39bb0e82d0e3acfaab`, with 153 focused checks and its self-check passing.
Task 11 documentation is frozen, and the third tracked preparation ran against source `1b7ca392fa63d4bd2c0662b355e3299f334df84fb19c3704a2b24d63baa2df46`.
Its manager build and seven archive roles passed, but the baseline stage refused the multiline version-probe parser.
No tracked check or up followed that third preparation attempt, and its owned containers were proved absent; the later first-UP failure is recorded below.
Task 13 corrective planning found another producer boundary: Podman removal deletes transient CID files, leaving declared CID receipts missing.
The Task 13 correction separates transient CID files from immutable receipts and validates complete version-output framing.
The later tracked prepare and check passed on source `6222196c83af94e5652e72921b1e4010c72160221a90a58cab702942a10ce1bf`, with all 530 source entries unchanged.
The earlier 14.78-GB demand was a planning envelope, not a freshly verified hard threshold or a field in the frozen wrapper/receipts.
The exact frozen wrapper enforces a 10-GiB VM floor and 2-GiB host floor; its seven prepared archives total 798,203,904 bytes.
The pre-maintenance read-only baseline had 12,565,581,824 free bytes and only 419,700,736 conservatively reclaimable allocated bytes, insufficient against the prior envelope.
That read-only check made no resource change; the later authorized maintenance preserved all existing images/volumes and pruned nothing.
M5 image peak and M7 full-phase growth remain unqualified despite the increased disk capacity.
The earlier 128-GiB preservation maintenance passed with fourteen CPUs/32 GiB and vda4/XFS grown; the later verified 160-GiB resize is recorded above.
PostgreSQL 18 now uses a verified persistent replacement on the same name/port and preserved database identity/settings/data; the old tmpfs container is retired, stopped, retained, and must never restart.
Original PostgreSQL 16 and M0 identities/configuration/tools are preserved; all pre-existing images and volumes remain retained, with zero pruning.
The maintenance included an idle M0 PID1 forced stop, unexpected first VM stop, stale API-socket failed restart, and corrected detached launch; PostgreSQL 16/18 recovered ready with zero PANIC/FATAL, so this was not an unconditionally graceful restart.
The final proof records 41,988,149,248 free VM bytes and 237,409,554,432 free host bytes.
The preparation receipt itself authorized no cleanup or resize; the later dated user decision authorizes only the stated maintenance and confirmed-unused pruning scope.
The first M7 UP failed after tracked check/source/37-output and capacity admission passed.
The omitted `podman exec -i` source fix is ready with 81 focused checks and the separate 169-check full wrapper/self-check run; these overlapping counts are not additive.
The diagnosed fake marker/label controls did not retain state; the [ledger](../tasks/todo.md) binds the frozen source and check receipts.
The following source-fix and failed-RUN history precedes the corrected first-UP acceptance above.
The real frozen-marker probe passed root-owned mode-0600 readback, no-clobber, and exact owned-probe absence checks; the [ledger](../tasks/todo.md) binds its receipt.
Fresh source `78a663` prepared successfully, and its first UP verified all three node caps before refusing a kubeconfig kind-prefix mismatch.
The fresh RUN remains `creating`; guarded DOWN refused the same naming check, and the approved exact three-container cleanup passed exact absence/preservation checks.
The two-comparison guard export `6277b1f1` and realistic fixture export `aa76c08c` now compose GREEN: 87 focused checks and one 175-check full wrapper run, with original-guard RED.
Those source-level contract tests preceded the corrected first-UP acceptance above; failed RUN evidence remains preserved without repair.
The frozen manager recipe uses three CPUs, 5 GiB memory, no swap, and one build job; PID enforcement is outside that approved recipe.
The one-off user-exception cleanup passed for exactly the three failed-node containers, with no volume flags or other deletions.
The source and every old RUN file remain unchanged in creating/empty state; volumes, kind network, images, backups, and original services are retained.
The quiet reservation is released; this is neither guarded DOWN nor UP qualification.
The failed first-UP window used a finite private supervisor with a monitored 12-GiB growth envelope, not a quota, and a 7200-second maximum lease without renewal; the cleanup exception does not generalize its guards.
Earlier partial run directories remain preserved and cannot be adopted.
The aligned standalone Podman cap probe proved update to two CPUs / 4 GiB with no effective swap; it does not qualify actual kind topology or cluster resource caps.
The successful preparation binds only its exact source; verified node caps in the failed fresh attempt do not qualify the kind lifecycle, legacy workflow, or AMD64.
No manual proof JSON or lease renewal is allowed.

- [ ] kind (podman provider) bootstrap with pinned CNPG, CRD apply and image load as Task targets; validated first with engine=pgcopydb on 17:17 (can start right after M1).
- [ ] Suite knobs: `E2E_ENGINE`, `E2E_RUNNER_IMAGE`, `E2E_SMALL`; DropIfExists off and filters dropped under native; engine-aware `checkMajors`; a positive `native` label set.
- [ ] Native runs on 16:16, 18:18 and 16:18: live writer load follow, manual and automatic cutover, verification, runner kill mid-copy, mid-stream and mid-drain, target primary kill with CNPG synchronous replication, suspend/resume, deletion cleanup.
- [ ] Hosted GitHub Actions job on kind at small scale.

### M8: Feature widening (each item enables its capability)

Actual standalone CLI qualification on source `8ad2d3f04beb67de814b4e83e4184d03eb6d34fc` passed six profiles and nine local reports on each of 16 to 16 and 18 to 18.
The cross-major 16-to-18 attempt failed before PostgreSQL at a Created-container receipt/adoption race, with zero executed pair profiles/reports; its exact owned empty container/network were retired and release proved while the original RED remains immutable.
The pending-creation fix is signed as private workflow checkpoint `1d8e5f750df2dcfde7178acaca6d30966357d76b`, with clone 48/follow 60/cutover 36 focused checks passing and primary unchanged.
Fresh cross-pair qualification is not yet available and its admission proposal is active without a new VM run; the two positive receipts remain bound to `8ad`, without operator or full M8 acceptance.

The approved [M8 implementation plan](superpowers/plans/2026-11-09-m8-feature-widening.md) targets fresh native format 9 / metadata 7, separate `ClonePlan.copy_tables`, conservative whole-table binary fallback, and exact mandatory large-object comparison with the unchanged array contract plus a proven-empty inventory guard.
These are implementation decisions, without format/runtime qualification or capability activation.
The approved plan permits isolated M8 source work before M7 closure after Task 1 freezes the shared interface.
The root approved the Task 1 interface freeze; isolated state/CLI, catalog, authority, COPY/large-object, and phase/recovery owners are building against it.
Isolated joined source tree `2e528d290bebc9f9f5677bd4fab44dccd53d99ae` passed formatting, strict all-target Clippy, and 75 affected unit cases against confirmed stock libpq 18.6.
Earlier 188 passes and two obsolete-fixture failures retain separate provenance; corrected selectors passed without relabeling that earlier input.
At the earlier joined-source checkpoint, compare/progress wiring had source tests only; the later component checkpoint above records the additional same-major runtime evidence.
The earlier seven-file harness/docs slice passed 348 portable tests and discovered exactly ten ignored Rust tests; authority faults, operator metadata-7 helper interoperability, and the full matrix remain open.
Actual PostgreSQL 16/18 schema-only TOCs and restores exclude large objects, with positive controls including them, so the tested archive path needs no extra LO exclusions; the later component checkpoint above supersedes the earlier PostgreSQL 18 catalog hold.
Keep the active format 8 / metadata 6 profile separate from candidate format 9 / metadata 7.
The isolated M8 code/docs handoff is signed as recorded above; M8 source reaches the engine primary only through the integration branch once its hosted CI is green, and no activation is authorized.
The approved bounded split policy defaults to max parts 8, rejects explicit zero, and caps the actual plan at 4096 parts; standalone defaults remain binary false, threshold 0, and large-object jobs 4, with operator defaults unchanged.
A provisional execution profile permits schema restore only in `PreDataPending`; resolve the actual source snapshot and post-restore target plan, then atomically `confirm_pre_data` before permits.
A new-generation restart requires positive own-resource absence and a fresh resolved profile.
The approved `sha2` 0.11.1 dependency disables default features; pinned cargo-deny 0.20.2 passed actual-graph license, advisory, and yank gates against the 2026-11-09 database, and the v5 affected units now pass known-vector and chunk-boundary hashing checks.
The [ledger](../tasks/todo.md#m8-feature-widening-each-item-enables-its-capability) binds the approved interface.

- [ ] Binary COPY with the per-table built-in-type predicate and text fallback.
- [ ] ctid split parts (`--split-tables-larger-than`, `--split-max-parts`) with per-part done markers.
- [ ] Large objects: copy, follow re-sync after drain, compare coverage, data-phase restart cleanup.
- [ ] Re-run the M7 native matrix with operator defaults.

### M9: Durability modes and measured performance

M9 checkpoint `59abca5308b827d29dfe12cbbed0b7928ca50ff1` uses native format 10 / metadata 7 and retains `REPLAY_SAFE_BARRIER_INTERVAL_MS=None`, H1/H4 ACK source work, and M10 fuzz provenance.
The outer-Durable candidate has 49 focused passes and the five-file benchmark preparation has 29 offline passes; cutover/Task H and coherent runtime qualification remain incomplete.
Benchmark execution remains on HOLD, without measured cadence or speedup.

The approved [M9 implementation plan](superpowers/plans/2026-11-09-m9-durability-performance.md) follows M8 with native format 10 / metadata 7 and the settings/status capability extension; no ACK may derive from a volatile frontier, and cadence is selected from later measurements.
Benchmark execution remains on HOLD until the final actual artifact/argv packet is approved; no benchmark run or VM reservation is authorized.
M10 remains open.
Initial-copy planning keeps fork COPY async and native per-table durability enabled.
A planned full-clone final flush barrier may match end-state durability without equating crash contracts.
The reviewed private final plan and frozen interfaces permit source A/B work.
The checkpoint above records focused packet tests, the approved CLI omission clarification, pending coherent live proof, unavailable release mode, and benchmark HOLD.

- [ ] `replay-safe` barrier cadence chosen from benchmarks; effective values in help and status; `relaxed` only if the measured gain exists.
- [ ] Benchmark harness: Run A/A noise checks, then interleaved randomized pairs in fresh processes.
  Report target-committed rows and transactions per second, catch-up rate, commit-to-apply p50/p95/p99, and COPY MiB/s on narrow and wide tables.
  Compare fork default against native `replay-safe`.
  Label native `durable` as stronger than native `replay-safe`.
- [ ] `docs/durability.md` crash matrix per mode: process kill, host power loss (deferred 2026-11-12, documented as unqualified), target crash, target failover with and without a synchronous standby, source WAL retention.
- [ ] Optional binary pgoutput, gated per stream on a built-in-type allowlist, only if measured.

### M10: Release readiness

On source `cefbc4f6`, ordinary hosted checks run 42898026149 passed all eight jobs, including 512 selected Rust passes (22 ignored, 107 filtered) and eleven plain-clone cases on each declared pair.
Native run 42898026214 failed both architectures at binfmt admission; actual host handler presence remains unknown and no ASAN qualification follows.
The official component inventory confirms 15,119,285-byte and 2,599,602-byte notice files exceed the existing 2-MiB bound.
A strict ELF-disjoint parser/toolchain candidate uses 16-MiB per-file/32-MiB aggregate notice limits and removes installer `--docdir`; that correction is active source work, not toolchain/runtime qualification.

The approved [M10 implementation plan](superpowers/plans/2026-11-09-m10-release-readiness.md) uses bounded mutation tests, native Linux ASAN with its nightly still unverified, STE source/license provenance, and exact production OCI artifacts for the final matrix.
These preparation decisions close no release or runtime gate.
The reviewed structural STE script, exact MIT license, and [provenance](../tools/docs/provenance.md) are now vendored unchanged, reusing the same-input self-test, ten CLI controls, and provenance checks.
The source-tool checkpoint above adds the checked inventory and portable registrations.
The integrated ten-guide checker passed as recorded above, while actual ASAN and runtime qualification remain open.

- [x] Vendor the reviewed STE script, license, and provenance as preparation only.
- [x] Commit M10 source tooling and register its three portable suites, retaining actual docs/ASAN/fuzz/runtime qualification as open gates.

- [ ] README index, getting started, durability, operator guide, troubleshooting; docs drift and STE100 checks.
- [ ] Parser fuzzing and a sanitizer or Miri job over `src/pg/ffi.rs`.
- [ ] Independent reviews of durability, recovery and the operator diff; final matrix on the exact image digest.

## M6 default Kubernetes delivery amendment, 2026-11-09

We approved the [default Kubernetes delivery correction](superpowers/plans/2026-11-09-m6-default-kubernetes-delivery.md), superseding only the earlier split-log result-collection prerequisite.
SQL authority stays in the admitted helper Job main process; one checked-exec relay carries the separate operation stdout/stderr streams without repeating SQL or creating authority.
Before exec, require the exact admitted container to be Running; after strict exec success, accept that same incarnation either Running or successfully terminated with exit 0 and signal 0.
Reject replacement, restart, failed or ambiguous state; successful post-exec termination does not replace separate terminal Job verification.
`JobCompletedNative` requires the original Job/container's terminal success and complete empty ordinary combined logs without a Stream query, then final unchanged identity/status checks.
Construct completion authority only after checked relay evidence, terminal main success, and final independent live/private closure validation all pass.
Three disjoint source owners are building this correction; earlier focused transport/helper/reader proofs remain historical inputs and do not qualify the changed delivery path.
Runtime, packaged image/libc resolver, opaque-file/service, TLS, and PVC gates remain open; production native admission remains disabled and no new RBAC or feature gate is authorized.

## M6 explicit connection-file and cap amendment, 2026-11-09

We approved the [explicit-file binding plan](superpowers/plans/2026-11-09-m6-explicit-file-binding.md), preserving earlier approved plans as historical inputs.
Before connecting either role, capture ordinary passfile/TLS inputs into sealed mode-0400 memfd snapshots while preserving raw libpq DSN, explicit-empty, original permission, and optional-absence semantics.
The only stable-absence route is `/usr/share/rpgmigrate/.native-file-absent`, an immutable missing path in the existing image; add no file, mount, or metadata and require exact-image, read-only-rootfs, mount-shadow, and runtime qualification.
The approved internal main source cap is 122880 bytes and complete main argument cap is 126976 bytes including NUL; wire, relay, and prelude limits remain unchanged, and native evidence Job JSON is capped at 1 MiB.
Linux's minimum 131072-byte per-argument boundary does not qualify aggregate argv/environment, stack, Kubernetes/proxy delivery, or opaque Secret length; required runtime tests remain open.
A dedicated native helper's final decorated create-site size check remains future integration; generic `ensureJob` remains unchanged.
Two disjoint owners are building the four-file helper and two-file admission changes; resolver, service, PVC, image, and activation gates remain open.
Numeric-alpha resolver research `c567134f` remains a proposal, without qualification clearance.

## M6 native helper Jobs/lifecycle amendment, 2026-11-09

We approved the [direct-image Jobs/lifecycle plan](superpowers/plans/2026-11-09-m6-native-helper-jobs-lifecycle-direct-image.md) with canonical pure policy exports and one fresh helper Job/operation per bounded 90-second invocation, using E persisted in an earlier reconciliation.
The administrator-configured immutable single-platform pin supplies both expected `ImageRef` and `PlatformImageID`; retain exact Job/Pod spec image and original status.ImageID comparisons without observed self-expectations, normalization, or a receipt service.
The narrow future removal of only the status.Image display equality is approved; it does not weaken immutable spec/imageID binding or grant runtime clearance.
D1 policy exports are frozen with seven tests/86 checks and zero native lint findings; Jobs/lifecycle focused verification is joined, while configuration follows later and no full activation is claimed.
Actual direct-runtime profile, identical-E acknowledgment, PVC, file, and resolver gates remain open with production admission disabled.

## User-approved owned-kind metrics observer policy, 2026-11-09

The user approved the temporary metrics observer policy for independently admitted owned kind E2E runs; do not request repeated approval within this exact scope.
Use a run-scoped ServiceAccount with automount false, one ClusterRole granting only GET on `/metrics`, and one binding to that exact ServiceAccount in the independently verified suite-created namespace.
Permit at most one in-memory 600-second TokenRequest without renewal, with at least 600 seconds left on the original run deadline; keep the token out of argv, environment, files, and diagnostics.
Use only the validated exact owned manager Pod's loopback HTTPS tunnel, with the approved tunnel-local certificate pinning and unchanged Kubernetes API TLS checks.
Clean only the recorded observer objects under exact UID/resourceVersion preconditions and verify absence; preserve replacement objects, namespaces, and suite resources.
This approval grants no production RBAC, Secret access, new namespace creation, lease renewal, benchmark run, or broader runtime qualification.
The [ledger](../tasks/todo.md) binds the approved policy and parameterized template hashes; each fresh render still requires exact source/RUN/namespace/manager/deadline admission.

## User-authorized continuation, 2026-11-09

The user authorized continuing the existing M5/M6/M7 plans and pruning unused Podman images, volumes, and build cache, plus growing `podman-machine-default`.
The approved PostgreSQL 18 persistent-preservation maintenance plan must protect tmpfs data before any original-container stop or VM restart.
The leader selected a 128-GiB disk, preserving 14 CPUs, 32 GiB memory, original service endpoints, and data; user-reported host free space is about 212 GiB.
`vm_pg18_preservation_plan` completed maintenance; the exact failed-node cleanup is now verified and its quiet reservation released, with subsequent M5 image results recorded above.
Pruning is limited to confirmed-unused resources under that plan, with retained evidence and data preserved.
The user also approved new runner dependencies, including Python 3 and ctypes/libpq, under the dated M6 exception so opaque URI Secret authentication stays out of argv.
The separate mechanism plan and required validation remain open; this authorization proves no TLS, PVC, image, kind, or production-admission result.

### Feature CI pushes and failed-node cleanup, 2026-11-09

On 2026-11-09, after approving the second exact failed-kind cleanup, the user authorized future removal of independently verified rpgmigrate-owned failed kind-node containers without repeated cleanup approval.
This standing authorization covers containers only: preserve volumes, networks, images, backups, original PostgreSQL containers, source, and RUN evidence; do not extend it to other resources or repair RUN markers.
The user authorized pushing `rpgmigrate` branch `feat/native-engine` and the operator feature branch carrying native work to `origin` for hosted CI, including native AMD64 checks under M5/M10.
This earlier authorization covered feature branches only, excluding main pushes/merges, tags/releases, and GHCR.
The dated M7 amendment separately authorizes development GHCR publication.
The user separately authorized one-off removal of exactly the three failed kind-node containers identified for the failed RUN.
That exception covers no volumes, networks, images, backups, original services, RUN-marker edits, or generic guard bypass; final independent verification now accepts its exact three-container cleanup.
The cleanup result is recorded separately from its approval; no push, hosted-CI, image-build, or kind workflow success is claimed.
Operator preflight exposed existing all-ref/tag GitLab mirroring and skipped tests; the earlier hold prevented inferring downstream authorization from the origin-only decision.
On 2026-11-09, the user explicitly approved the existing GitLab mirroring triggered by the operator native feature branch.
We resolved the publication P1 by authorization and keep the existing mirror enabled.
This mirror decision authorizes no repository settings changes, main edits/pushes, tags, releases, or GHCR publication.
Development GHCR approval follows the dated M7 amendment.
The CI owner restored its mirror-guard edits to baseline and committed only hosted checks in `ccde83912cdbc9e152f0bc51c47c49954ede8a9d`, parent `c262322909ac03a7101ea3bfac2c4823efa62e95`.
The normal feature push to `origin/test/m4-closing-m6-composed-20261109` is verified; its [mirror run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42633254999) passed.
[Operator CI](https://github.com/ydixken/pgcopydb-operator/actions/runs/42633254706) passed docs and its unit/envtest, cluster-free E2E, and PostgreSQL 18 coverage test job; the run failed only lint on SC2015.
The reviewed mechanical correction `5616d82f03e6a917147e55ed4a3fb436a1cc6d89` was pushed normally to the feature ref after prior tests became terminal, without cancellation.
The new [operator CI](https://github.com/ydixken/pgcopydb-operator/actions/runs/42636601113) is in progress and its authorized [mirror run](https://github.com/ydixken/pgcopydb-operator/actions/runs/42636601234) passed; no new CI pass is claimed.
Engine feature pushes of `34208b6` and `7c04dff` retain their exact hosted-run scopes above; no feature CI result closes a milestone by itself.

## Project license decision, 2026-11-10

The user selected Apache License 2.0 for rpgmigrate.
We record the SPDX identifier `Apache-2.0` in `Cargo.toml` and retain `publish = false`.
The root [LICENSE](../LICENSE) contains the unchanged [official Apache License 2.0 text](https://www.apache.org/licenses/LICENSE-2.0.txt), including the unfilled Appendix example.
Its SHA-256 is `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30`.
Existing third-party notices retain their original bytes and licenses.
M6–M10 remain open, benchmark execution remains on HOLD, and tags and releases remain unauthorized.
Development GHCR publication has separate dated approval.

## Leader takeover and wave results, 2026-11-12

The Codex leader stopped on a content-filter error at 2026-11-10T22:21Z, and the host rebooted afterwards.
The reboot cleared `/tmp` and `/private/tmp`, while `/private/var/tmp`, `~/.codex/tmp` and `~/.cache` survived.
A citation marked "(lost 2026-11-12)" names a file that no longer exists.
Its recorded SHA-256 still identifies the bytes, and the claim it backs keeps its original scope without fresh verification.
Where a byte-identical copy or a successor exists, the citation names it next to the lost path.

On 2026-11-12 a Claude Code leader took over with the same roadmap, ledger and gates.
Owners now work in standalone clones under `~/development/` instead of git worktrees, because the checked-in agent instructions forbid worktrees.
The [ledger](../tasks/todo.md#leader-takeover-and-wave-results-2026-11-12) binds the evidence for each result below.

### User decisions, 2026-11-12

- Host power-loss crash rows are deferred; the durability documentation states them as unqualified instead of claiming a result.
- Public development GHCR images remain approved for `ghcr.io/ydixken/rpgmigrate` and `ghcr.io/ydixken/pgcopydb-operator` only.
- Feature-branch pushes to origin remain approved; main pushes, merges, tags and releases remain unauthorized.
- Benchmark execution still needs separate approval.
- The project license stays Apache-2.0.

### Engine branches

- `feat/m8-engine-20261112` at `41db2ac` carries M8 and passed the host gate, with 559 Rust tests and the ten-guide check.
  Its hosted run 43210130162 failed on the stale ASAN lock pin and on `podman machine` calls in the e2e runtime guard; the integration branch below fixes both.
  The regenerated M8 contract supersedes the lost `/tmp` contract.
- `feat/engine-integration-20261112` at `3bc1f60` joins M8 with a local-Podman host path in the e2e runtime guard, the re-pinned ASAN lock graph after the `sha2` review, and the M6 lifecycle plans.
  Hosted run 43215645789 passed checks, dependencies, docs and both runner-image jobs.
  All three engine-e2e pairs stop at the capacity floor: the hosted runner reports four CPUs, and the guard requires six.
  `feat/native-engine` is not fast-forwarded and no `dev-<sha>` runner image follows until that floor has a decision.
- `feat/m9-join-20261112` at `0f6fa60` joins M9 commits `9eb3154`, `59abca5`, `371d19c`, `b566fb3` and `795a8c5` on `41db2ac`; H1 `0c507a7` was already contained.
  It adds the follow and cutover case inventories `tests/fixtures/follow/m9-cases.json` and `tests/fixtures/follow/m9-cutover-cases.json`.
  Its focused suites and the live PostgreSQL 16.15 and 18.6 follow-authority and clone runs passed.
- `feat/m10-asan-20261112` at `cc82953`, also fast-forwarded onto `feat/m10-asan-preparation`, passes every hosted ASAN preparation stage on amd64 and arm64 except `graphs`.
  Both unit graphs are `shape-unrecognized`, so ASAN stays unqualified.
- `feat/m6-pkg-20261112` points at `254ab5ba`, whose hosted AMD64 runner image passed the smoke checks with the same build inputs as the ARM packet.
- `docs/m6-lifecycle-20261112` at `f4346cf` restores the completion-rejoin and TERM amendment plans and specifies the native TERM runtime receipt.

### Operator branches

- M6-LINT needed no change: `5616d82` is already integrated in `3f1330e`, and `make lint` reports zero issues there.
- `feat/m6-lifecycle-20261112` at `a5e802d` qualifies composed exact-UID disposal, cleanup on `complete=true` only, and plain-clone failure on a missing report, and adds the TERM receipt checker.
- `feat/m7-harness-20261112` at `9353ac0` keeps the Go gate output, adds the macOS sleep guard, refuses `/tmp` run directories, and regenerates the kind image lock from public digests.
- `feat/m7-suite-20261112` at `7a863ed` shapes suite Migrations for the native engine and labels the native matrix specs.
- `feat/m7-faults-20261112` at `c290cba` adds six native fault specs, which have not run yet.
- `feat/native-operator-integration-20261112` at `5f8e2f5` joins these with the M8 helper rebase, widens the kind label gate under the native engine, and captures TERM receipts in the suspend and delete specs.
  Three M8 helper tests fail on a changed canonical helper hash, and moving that pin needs a decision.

M0–M5 remain closed and M6–M10 remain open; no native operator migration or capability activation is qualified.

## Execution model

The leader decomposes, assigns one owner per file or subtree, and reviews; verification agents never review their own work.
Effort per task: xhigh for WAL, apply, recovery, durability and safety reviews; high for bounded implementation and tests; medium for docs and CI; each brief states the reason.
Each milestone gets a detailed implementation plan (superpowers:writing-plans) before work starts.
Parallelize the build phase as far as file ownership allows: independent owners work at the same time, each in its own standalone clone off `feat/native-engine`, and the leader integrates.
Run the independent review (at the effort its risk calls for) and the Go test suites once per milestone, at its close, not after each task; workers run only the focused tests for their own change while building.
Operator work goes on its own branch and PR, and existing operator edits (`tasks/todo.md`, `.agents/`) stay untouched.
The verified VM allocation is fourteen CPUs, 32 GiB memory, and a 160-GiB disk; the [ledger](../tasks/todo.md) binds the latest resize/preservation proof.
The original 21:35 UTC startup failure predates the CPU/memory resize.
Maintenance and failed-node quiet reservations are released; preserve host Cargo headroom and the approved resource budgets for subsequent M6/M7 execution.
Initial uncapped kind creation requires database testing and host Cargo builds to be idle until all three node caps are verified.
See the [capacity and preservation record](../tasks/todo.md#capacity-and-parallel-execution-user-update-2026-11-08) for budgets and the restart prerequisites.
Commits remain small, conventional, and lint-clean.
Feature-branch CI pushes have the dated user authorization above, while main pushes/merges, tags, and releases remain unauthorized.
Development GHCR publication follows the dated M7 amendment.

> [!important]
> On 2026-11-08, the user approved exactly two local WIP checkpoint commits: `e544f2d` on `feat/native-engine` and `5e8cbcb` on the isolated operator `feat/native-contract-v1` branch, as an exception to the lint-clean-per-commit rule.
> Those checkpoints predate the qualified M2 staged pair receipts and the normal M3 source commit `7b6037f`, whose primary strict gates resolve the scaffold lint hold.
> The two-commit exception is consumed; the normal lint-clean rule has resumed, and M3 worktrees remain outside their scope.

## Verification

- Engine: `task lint test`, `task test-db` for cargo live tests, and per-pair E2E targets (`task e2e PG_SOURCE=16 PG_TARGET=18` and a matrix target) including crash and drain cases.
- Operator: unit and envtest suites; `task e2e:kind:up` then the native E2E target for each pair; success means Complete with CutoverCompleted, a native drain proof and a data verification match.
- CI runs the same targets on hosted runners; the final release matrix runs on the exact image digest.
