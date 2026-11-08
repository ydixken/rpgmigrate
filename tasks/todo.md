# rpgmigrate task status

This file is the authoritative running status.
The approved design and latest user scope decisions define active scope.
Plans define worker interfaces and acceptance checks.
The team leader reviews worker results before closing a milestone.

## Active scope

The user accepted the [operator-first release contract](../docs/operator-compatibility.md) on 2026-11-07.
The completed runtime and API audits select eight operator paths and 43 behavioral option names, plus general help, version, ping, and explicit connection/work-directory controls.
The other 40 behavioral paths are deferred parity requirements; retain the parser baseline and existing implemented commands.
Every selected CRD feature requires its full semantics, with explicit admission or preflight refusal for unsupported staged configurations before mutation.
Default follow uses `pgoutput`; large objects, binary COPY with safe fallback, and parallel/split COPY remain required workflow gates.
The bounded `test_decoding` consumer does not qualify operator-default follow.
Native support applies only to new migrations and native-owned workdirs, through an explicit adapter without legacy SQLite or source-sentinel storage emulation.
No universal CLI parity, operator-complete, alpha, backend-winner, or performance claim follows from this scope change.

- [x] Freeze the consumed command/option/default contract and source citations in the maintained matrix.
- [x] Finish the bounded Native5 test_decoding Gate 3 consumer correction, protocol capture integration, independent review, and four-case PG16/18 live qualification.
- [ ] Freeze shared interfaces once, then use disjoint pgoutput decoder, context/publication/source lifecycle, clone/runtime, and operator adapter/API owners.
- [ ] Qualify selected clone phases, recovery, cutover/drain, progress, verification, cleanup, and default operator configurations through runnable commands.
- [ ] Run targeted risk tests per change and one exact integrated candidate matrix, retaining crash, unknown-COMMIT, source/native/image binding, and strict aggregate gates.

Root owns primary integration, the normal Git index, the task ledger, and unique lease IDs under the parent's explicit execution criteria; the parent owns worker allocation and scope approval.
Do not repeat broad checks without a changed input or unresolved concern, and do not substitute component counts for a working operator workflow.

## Current integration gates

The production CLI still provides ping and restricted table-data COPY; a native operator clone/follow/recovery workflow is not qualified.
The accepted operator-first scope and safety gates remain unchanged.

### Latest verified milestones

- [x] Merge the client-runtime packaging checkpoint through [PR #4](https://github.com/ydixken/rpgmigrate/pull/4), normal merge `bee9ab94`, with reviewed tree `63418e7f` and four PG16/18 by amd64/arm64 matrix jobs, each passing all 24 smoke checks.
- [x] Qualify the restricted E1 writer fence: six cases across PG16.15 and PG18.6, recorded in [Root's acceptance](../target/verify/e1-bbcd-live-qo4mu0kw/root-acceptance.json), `59c519d8`.
- [x] Accept the E3 baseline and first E7 settings comparison: 24 runs per candidate, eight configurations with three repetitions each, and observed pipeline median increases of 28.6% to 77.9% in the [descriptive report](../target/verify/e7-ab-statistics-rp7fmd45/REPORT.md).
- [x] Qualify the selected Native6 pgoutput snapshot/COPY/WAL SourceOrigin component: four PG16.15/18.6 cases, 40 native witnesses and 64 admitted proof files, accepted in [Root's bounded receipt](../target/verify/n6-epoch-865d-live-csru8ioe/root-acceptance.json), `26bda5e3`.
- [x] Qualify the selected Native7 target-only schema component: six PG16.15/18.6 cases, fourteen fresh runs, forty native witnesses and thirty-two callback phases, accepted in [Root's bounded receipt](../target/verify/n7-wholerow-be268-live-wgpyuecq/root-result-acceptance.json), `4633cfdf`.
- [ ] Qualify real-source, real-utility and full-clone schema behavior; the Native7 result uses synthetic source identity and fake utilities.
- [ ] Publish this maintained progress and its scoped evidence in a separate reviewed documentation checkpoint; private runtime work remains outside the packaging PR.

The A/B comparison covers Native5/test_decoding, a release-mode test harness, Linux arm64 and local Unix-socket clusters in separate measurement windows.
It does not establish a causal or universal gain, a backend winner, or production CLI/operator performance.
Receipt links under `target/verify` refer to retained local evidence, not published GitHub artifacts.
The [PR4 merge receipt](../target/verify/runtime-terminal-hop-stable-7dx942gn/pr4-merge-receipt.json), `c99260eb`, binds both merge parents and the exact tree; local `main` fast-forwarded while the feature checkout, index, and all 164 tracked/untracked path identities remained unchanged.
Two redundant push-job API step lists were empty despite successful jobs and retained smoke evidence; the four PR matrix proofs were accepted, and no duplicate eight-job artifact gate was imposed.
The [post-merge run](https://github.com/ydixken/rpgmigrate/actions/runs/42445772554) passed all four native source/build/smoke/seal jobs on `bee9ab94`; the tag publisher was skipped, and no GHCR release is claimed.
The two-path public progress checkpoint [PR #5](https://github.com/ydixken/rpgmigrate/pull/5) merged normally as `d4f4cc86`, preserving reviewed head `419e9718` and tree `ca972983`, after Russell's bounded documentation PASS, exact-candidate check/lint/default tests and both four-job hosted runs passed.
The [post-merge run](https://github.com/ydixken/rpgmigrate/actions/runs/42449357075) passed all four native source/build/smoke/seal jobs on the merge commit; local main advanced without changing the feature checkout, and all 166 working-path identities and index bytes were preserved.
Only the two documentation paths were published; private runtime work and integration plans remain outside this checkpoint.
The one-path Native7 milestone update is open as [PR #6](https://github.com/ydixken/rpgmigrate/pull/6), head `4f0fed6e56434627e5229a26ed27c229973a4ce1`, tree `807f7e486cefd5a39cbb5748ac84ccc643813222`.
Tesla's exact R3 documentation review `8f89bcc4` and the public-base candidate's check, strict lint and default tests passed; all 169 existing working paths, modes and unrelated index entries were preserved.
The first hosted attempt failed in Task installation and actionlint downloads; read-only diagnosis `72fb7e2a` established three download failures and left the Task cause unknown.
Parent authorized one retry of only those four failed jobs on the unchanged head, recorded in [the retry receipt](../target/verify/native7-public-docs-r3-mzs7a4mg/ci-authorized-retry.json), `1774072f`.
Push attempt 2 then failed at step 5 after host Task succeeded; diagnosis `248a1de6` locates fixture Task bootstrap before tests but leaves its suppressed underlying cause unknown.
Merge and further retries remain held; Parent assigned a diagnostic-only private three-path change to bootstrap code, its existing tests and the testing guide, followed by independent review and new public-candidate gates.
The [local gate receipt](../target/verify/native7-public-docs-r3-mzs7a4mg/local-gates.json) applies only to the public documentation candidate, not the private engine.

- [x] Merge the ten-file operator-contract checkpoint through [PR #3](https://github.com/ydixken/rpgmigrate/pull/3), commit `da332950` and normal merge `86b80406`, with exact tree `ace698ba`.
- [x] Pass that checkpoint's local check/lint/tests and both architectures before and after merge; the [completion proof](../target/verify/operator-scope-docs-5tw08pzs/completion.json) records preserved feature checkout, empty index, and 161 working-path identities.
- [x] Accept the [bounded V4 oracle review](../target/verify/independent-gate3-v4-review-ba1d60065b4449ac8a769cea1ff75fcf/gate3-v4-review-receipt.json), `d600030b`: 38 harness, eight protocol, six independent checks, and 8,268 invalid substitutions refused, with zero skips.
- [x] Correct and review the evidenced Origin timeline-query defect, freeze a new candidate, and qualify the two G3 cases on PostgreSQL 16.15 and 18.6 under a new parent-granted lease.
- [x] Close the operator DTO end-position finding and integrate the reviewed early effective-engine refusal in the accepted operator checkpoint; native selection cannot fall through to pgcopydb, while omitted legacy selection retains its behavior.
- [x] Observe the operator checkpoint's post-merge lint/test/docs PASS without treating fail-closed native admission as an implemented native migration workflow.

The seven-file operator API/schema review passed as `e23c6d27`; that component evidence remains distinct from the later DTO and controller integration.
The controller gate has independent PASS `a037b898` in `/tmp/operator-dispatch-independent-vad81iq5/receipt.json`, with 62 focused and 74 independent cases, zero skips, and lint success.
Hilbert completed the separate operator repository's 14-path checkpoint through [PR #401](https://github.com/ydixken/pgcopydb-operator/pull/401), accepted head `e8d9b4f5`, tree `294b1c6f`, and normal merge `0267c787`.
The parent recorded pre-merge CI `42417310145` PASS, full local gates, PG16 race 508 passing/two expected skips, PG18 coverage 158 passing/zero skips, and 32 admission cases; [post-merge CI](https://github.com/ydixken/pgcopydb-operator/actions/runs/42418311574) passed lint/test/docs.
Operator PG ownership was released after cleanup, and its feature branch and user work were preserved.
The pgoutput decoder has independent offline PASS `3e89c58c` (89 plus nine tests and four mutants).
Lane B review `04586b39` confirmed a P1 in the logical exporter's text profile (float precision and encoding); its 238 passing review tests do not clear the finding or authorize activation of source171.
The private correction sets and verifies six fixed settings on the logical session before CREATE/CP, requires `Utf8CanonicalV1` in `PgoutputV1`, and refuses context V2 or missing/unknown V3 profiles.
That correction is frozen as handoff `4580c803`, source173 `2c1444a5`, and has independent PASS `04eeb16e`: 99 repository tests, nine controls, zero ignored, and two mutants; live qualification remains unrun.
Legacy V1 bytes, CoreSchema3, Native6 publication/slot types, and the frozen Native5 G3 source remain unchanged.
The separate legacy test_decoding formatting risk remains open before full-type qualification; the integer-only G3 baseline cannot qualify that behavior.
The private Native6 pgoutput qualification profile is frozen as handoff `05508d3a`, source177 `282b12c4`, on reviewed source173 plus the reviewed Native6 fixture overlay: two explicit segmented/SQLite owned-publication cases covering fresh COPY, WAL apply, reopen/SourceOrigin continuation, and float round trips.
The author reports 14 focused Python passes, two detected mutation controls, compilation and scoped Clippy passes, and exact two-case discovery; no live cases ran.
Root checked the declared five source SQL, two logical, and three target SQL opens per case against the Rust call sites.
That original profile bypasses shared native protocol setup and retains only request/response JSON, so its declared counts do not establish raw native observations.
Corrective Native6 handoff `b576451f`, source177 `0df3e185`, reports 30 authored, 188 shared, and 114 control passes, compilation, scoped Clippy, and exact two-case discovery, with no PG execution.
The correction remains unqualified after review findings in nested identities and capture/outcome handling.
Native6 semantic Unit A is frozen as `7581f63f`, patch `0382dc65`, with 38 fixture, 188 shared, and 100 prior author checks; its five reproduced false acceptances now refuse, but one combined independent review remains required.
Capture Unit B is frozen as `74f963cb`, preserving the original malformed UTF8, overflow, and wrong-role secret exclusion tests: admit safe per-file content before exporting bytes, keep earlier admitted evidence after later errors, omit unsafe offending bytes, and preserve cross-file qualification.
Hume's private composition is frozen as handoff `91d49d29`, source178 `35c3ebb5`, with A+B, the canonical runner, Franklin's startup correction, and the reviewed Origin/publication int8 hunks.
Pauli's independent receipt `8be47beec474683a859c6331673a6d16d3aa109dbbf15b334157e354a73250a3` passes the composed offline harness scope: 46 fixture, 188 shared, 100 prior, ten canonical-runner, and three startup/timeout checks, with prior scenarios, twelve new controls, and two detected mutants.
The review dynamically confirmed that the actual PgoutputOrigin dispatcher returns through its safe CaptureBoundary before the generic collector implicated by the E1 privacy finding; the generic path remains unqualified.
Root handed Hubble the [exact Native6 offline PREP brief](../target/verify/e1-e3-composition-wyakdmm1/native6-PREP.md), SHA-256 `76a5a16547cf78f592af49916e0e30afc80aa2e9e27bcfd7555c9f753d4cb2ff`, for CLI/libtest compilation and exact two-case discovery while review runs.
The brief requires twenty native witnesses per version, five source SQL/two logical/three target SQL per case, the dedicated callback, declared limits, and actual source/native/binary/image bindings; it grants no PG lease or broader suite execution.
Hubble's [actual PREP](../target/verify/native6-linux-prep-w_25r_zf/prep-packet.json), SHA-256 `cc21d6c063bd2730bcaeab24fff137a2a37a69b62e763a71c0bbacab037578f7`, records successful offline CLI and libtest compilation in 2.429133 and 7.552862 seconds, with exactly two Native6 cases and no benchmark discovered.
Root's bounded acceptance matched source178 `35c3ebb5`, the review and brief, Cargo-derived CLI `1e352d4d` and libtest `d783dc18`, loaded native-20 `d6ab3015`, twelve critical logs, both image identities, actual arguments, limits, 32,172,986,368 bytes of held-FD headroom counted once, and owned build cleanup.
Under Parent's conditional authorization, Root granted Hubble the sole four-case Native6 lease: two cases on PG16.15 followed by two on PG18.6, requiring forty native witnesses and eleven separate helper psql observations per case.
The frozen 300-second runtime child and 600-second discovery remain under the 900-second command and 2100-second host bounds; stop at the first failure, retain safe evidence before both cleanup attempts, and release explicitly.
The first PG16.15 command stopped after 2.171148 seconds with exit one and a startup RuntimeError before any Rust child launched; zero cases executed, no live PASS exists, and PG18.6 did not start.
Root checked the [final negative receipt](../target/verify/native6-linux-prep-w_25r_zf/execution-receipt.json), SHA-256 `d20eb4bbeeaceaf371ac8d53788f4b1d16811244b18c7d5c2aaa253ae0de6595`, against all 26 retained artifact hashes, source/native/binary bindings, zero-case counts, and owned cleanup evidence.
Both owned containers are absent, no volumes were created, the private temporary/socket namespaces are empty, no helpers remain, and the lease is released; the failed SQL operation remains unknown.
Parent authorized a separate PG16 startup-only diagnostic on the same source/native/image: Maxwell owns a minimal external overlay that emits only allowlisted source location, utility basename, return code, and strict SQLSTATE, without SQL, arguments, connection details, or raw logs.
The replacement observer plan `66dd3496`, probe `1763af34`, preserves original psql results and readiness polling, then stops on the actual caller's fatal error or the pre-Rust success guard; eight bound offline controls include readiness exit two, success, then fatal fixture exit three.
Root accepted its source/privacy bindings; the old `ff225068` observer is held and must not run.
The [fresh atomic diagnostic](../target/verify/native6-startup-atomic-7gdo8yst/execution-receipt.json), `c4cc656f`, and safe observation `ad9729c9` record psql exit three with SQLSTATE `42501` at `pgoutput_origin_fixture.py:313`, function `probe`, after preserving five expected readiness exits.
It executed no Rust cases or PG18 work, retained no secondary error, and released its owned container and both lease aliases after absence checks; this fresh observation does not identify the original `d20eb4bb` statement.
Maxwell owns the approved fixture-only correction: use the existing admin connection for restricted server metadata and bind the intended tested role/name/OID separately through the catalog, without grants or changes to Rust routes and privileges.
The new patch, independent review, exact PREP, and live qualification remain required.
Maxwell's two-file correction is frozen as handoff `76f4a835`, source178 `f370ae6b`, with the actual dispatcher/fixture subprocess refusal reproduced before the fix and distinct admin/intended-role binding controls passing after it; fresh live evidence remains pending.
Pauli's focused independent receipt `6bdb7f94a1cbc47e8cc12ce69469d05001435850a669f3d7a2cdbb34d05ca51b` passes the offline oracle/role scope; the new PREP below binds reusable binaries through exact compiler-input identity.
Root accepted [role-fix PREP `dc75d1ba`](../target/verify/native6-rolefix-prep-9f0elg7s/prep-packet.json): thirty evidence hashes, source178, only two Python changes, 176 unchanged files including compiler inputs, eleven include bindings, unchanged native20, reused CLI/libtest, fresh Linux loader/discovery, both images, resource caps, and owned cleanup.
The new runtime helper is bound explicitly; twenty Rust witnesses per version remain separate from eleven setup and eleven validation-callback psql calls per case.
After E3 released and Root verified both version containers absent, Root granted the new `n6-rolefix-dc75-hubble` lease for exactly two cases on PG16.15 followed by two on PG18.6.
The contract requires twenty Rust witnesses per version, eleven setup and eleven callback psql queries per case counted separately, fresh atomic runtime preflight, safe retention before both cleanup attempts, and no retry after the first failure.
Hubble reports PG16 exit 101, zero passing and two failing Rust cases in 2.023473 seconds, with ten of twenty required witnesses; PG18 did not start.
The [sealed negative receipt](../target/verify/native6-dc75-live-gjbs8jja/execution-receipt.json), `d7a13df8`, retains SQLSTATE `42501` at `pgoutput_origin_live_tests.rs:957:39` in both cases, without a new diagnosis or debug query.
Root checked all 45 retained artifact hashes, candidate/PREP and old-receipt bindings, the original exit, incomplete proof counts, and actual container absence; the lease is released, and the run remains unqualified.
Missing final records/callback proof is recorded separately from the original Rust failure; runtime, executor, and cleanup secondary-error lists are empty, and PG18 remains unrun.
The bounded diagnosis locates the new `42501` in test-adapter route observation, before TargetAuthority, COPY, or the receiver: the ordinary target role reads restricted server metadata.
Ramanujan's correction is frozen as handoff `628bb0eb`, source178 `81fb9036`: obtain the actual host through a test-only libpq accessor, remove the restricted query column, and retain the complete route equality, admin oracle, roles, and twenty-open inventory.
Pauli's focused independent review `cb4e6246` passes seven Rust, 47 fixture, 188 shared, and 100 prior checks; eleven native peer observations and eighty route mutations verify actual-host binding without substituting the expected host.
The correction remains test-only, with no attributed production defect.
Root accepted [reviewed PREP `96974085`](../target/verify/native6-route-linux-prep-6zvp0pzg/reviewed-prep-packet.json) as [bounded PASS `5e8f2fb5`](../target/verify/native6-route-linux-prep-6zvp0pzg/root-prep-acceptance.json): ninety artifact hashes, source178, fresh compiler-derived CLI `c49dc237` and libtest `19b05a04`, exact two-case discovery, native20, both image/loader identities, arguments, caps, privacy, and three removed build/proof containers.
The initial compiler-container post-build report stopped on a private helper NameError; that missing report remains disclosed, and fresh proof containers verified the complete 201-entry source/native/new-binary subset on both versions without rebuilding or executing a case.
Under Parent's conditional authorization, Root granted Hubble the sole `n6-route-9697-hubble` lease: two cases on PG16.15, then two on PG18.6 only after the first version passes, with twenty native witnesses per version and eleven setup plus eleven callback helper queries per case.
Fresh atomic preflight, 300-second children, 600-second discovery, 900-second version commands, one 2100-second host budget, safe evidence before both cleanup attempts, and first-failure stop bound that attempt.
The [sealed negative receipt](../target/verify/native6-route-9697-live-0od958_m/execution-receipt.json), `93e7e930`, records PG16 exit 101, zero passing and two failing cases in 5.402423 seconds; PG18 did not start.
All twenty route witnesses are retained but unqualified; both panics at `pgoutput_origin_live_tests.rs:306` assert callback-child success, and the admitted evidence contains no failed predicate, SQLSTATE, typed error, or Python location.
Root verified all 57 retained artifacts, source/PREP/review bindings, and exact owned-container absence; the lease is released.
Ramanujan froze the Native6-local diagnostic as handoff `23e16e58`, source178 `efa7f390`, changing only its fixture, tests, and harness documentation.
The author reports fifty fixture and 188 shared checks, with all 75 Rust files and 57 prior negative artifacts unchanged; these are author results, not independent clearance.
Nash's independent diagnostic review `0183266f` passes the three-path scope; the exact earlier predicate remains unknown, and no raw payload reconstruction is authorized.
Root accepted [PREP `a1a30c26`](../target/verify/n6-diagnostic-linux-prep-5dqh7u3z/prep-packet.json) as [bounded PASS `2c57a03e`](../target/verify/n6-diagnostic-linux-prep-5dqh7u3z/root-prep-acceptance.json): 270 artifact entries, runtime201, exact source178, and unchanged Rust75/Cargo/native inputs support explicit reuse of CLI `c49dc237` and libtest `19b05a04`.
Both image/helper/loader/discovery bindings pass, and Root independently verified both PREP containers absent.
After E7 quiet released, Root granted Zeno the unique `n6-diagnostic-a1a30-zeno` lease for the exact two PG16 cases followed by PG18 only after PASS, using the existing controller with metadata-only rebinding.
The [new negative receipt](../target/verify/n6-diagnostic-a1a-live-9rkdregb/execution-receipt.json), `06758834`, records two failed PG16 cases, exit 101, twenty admitted but zero qualified native witnesses, and PG18 unrun, with no retry or secondary error.
Root's [bounded negative acceptance](../target/verify/n6-diagnostic-a1a-live-9rkdregb/root-negative-acceptance.json), `679166eb`, verifies all 276 artifact hashes, the new source/PREP bindings, and exact owned-container absence; the lease is released.
Both new safe diagnostics identify `FINAL_L287/UINT_L54`: the frozen helper requires an exact integer `barrier_epoch` of at least one.
The rejected payload was omitted, so no actual value or type is asserted; the historical `93e7` predicate remains unknown, and the new result requires Parent-owned diagnosis before another candidate or lease.
Ramanujan traced the declared field to `Checkpoint.barrier_epoch: u64`, metadata's zero initialization, its unsigned codec, and the test producer's direct typed serialization; existing transitions preserve the epoch while incrementing the checked counter.
The Native6 oracle's minimum-one requirement excludes a valid zero epoch by contract, without establishing the omitted runtime value; the assigned correction is limited to this field with complete-proof RED/GREEN and malformed-value controls, preserving positive counters and every other domain check.
Ramanujan froze the three-path correction as `bd5886a4`, source178 `b7e97385`, changing only the field's minimum override while retaining all 159 earlier assertions, 75 Rust files, and other domain checks.
Nash's independent review `4d28b2cb` passes 53 fixture, 188 shared, and six independent methods covering 69 cases, including complete-proof zero-epoch RED/GREEN and exact unsigned bounds; the actual omitted runtime value remains unobserved.
Hubble's new PREP may reuse the retained binaries only after exact compiler-input, flag, and path equality; metadata containers wait for Native7's explicit lease release.
After Native7 released resources, Root accepted [PREP `865db12e`](../target/verify/n6-epoch-linux-prep-g_58jy10/prep-packet.json) as [bounded PASS `02b0447e`](../target/verify/n6-epoch-linux-prep-g_58jy10/root-prep-acceptance.json): 71 artifact entries, runtime201, the exact current helper and sixteen image-bound code objects, reused binaries, native20, images, arguments, roles, bounds, and both metadata-container absences.
The `n6-epoch-865d-hubble` lease completed two passing PG16 cases followed by two passing PG18 cases and was released.
Execution receipt `78b3d70a` and Root acceptance `26bda5e3` bind source178 `b7e97385`, the current helper, reused binaries and loaded native library, both images, 131 retained artifacts, 64 admitted proof files and exact PID/role/order/cardinality checks.
Command times were 3.976298 and 2.245623 seconds, with twenty native witnesses per version and eight callback response documents overall; eleven setup plus eleven callback helper queries per case are the declared contract, not counted process invocations.
Both owned containers were independently absent, with empty temporary roots, reaped children and no secondary errors.
This qualifies only the selected Native6 component, not full CLI, full-stream ACK, pruning, cutover or a composition with the measured Native5 E7 refactor.
Earlier `067588` rejected values remain unobserved and the historical `93e7` cause remains unknown.
Parent permits at most two overlapping isolated correctness scopes, E1 and this startup diagnostic, only with verified separate resources and combined capacity; otherwise serialize them without another permission request.
Keep the old nine replication cases, two text-profile cases, and two test_decoding G3 selectors unchanged; Native7 schema work and PG leases remain separate.
Clone review `c0765ff` found lost `PGREQUIRESSL` semantics (P1) and overridden explicit `options=''` (P2); the process-only correction has independent PASS `b37ad7db`, without qualifying target DDL.
Native6 fixture review found null-inner-receipt and non-UTF8 acceptance; corrective receipt `5f343e4e` has independent PASS `237e9c8b` with 188 affected harness, 19 review, and four targeted cases, without PG qualification.
The clone primitive's fake-process tests do not qualify external DDL, and no unused schema-provenance accessor or direct external restore wrapper is released.
Parent approved postdata option A revision 2 after the second independent design PASS and reserved Native8/metadata5 exclusively for that feature; CoreSchema3/contextV3 remain unchanged, with fresh Native8 creation and no Native7 adoption.
The approved private `state::open_postdata(&StateRequest)` entry must select Native8 explicitly, preserve existing open/Create defaults and old schema-absent profiles, retain flavor8 across mutators and select target metadata5 from that flavor even with null postintent.
The [maintained postdata plan](../docs/superpowers/plans/2026-11-08-schema-postdata.md) fixes seven intent fields, present-null initialization, one nullable target nonce, same-archive/source/body binding, retained-input same-intent fresh-fence retry and committed barrier-only recovery.
Parent clarified that complete content and provenance bindings define identity, not Rust allocation: a separately minted byte-identical artifact with every binding preserved is admissible, while binding-changing input refuses before effects.
Include that scope in the combined independent review; held-descriptor mutation checks remain mandatory, with no new owner field or lost-input restart guarantee.
Require every Native8 CDC begin and frontier transition to refuse throughout this slice, including fresh null postintent and successful phase proofs; only future aggregate authority can release it.
Parent assigned Noether (xhigh, `01a1b999-392e-7810-a0e8-2f7a782f1f44`) to private persistence and Laplace (high, `01a1b999-3ac2-7183-9a5c-2167caccd5ef`) to independent retained archive export on the reviewed `e5c1a69b` base.
Both dependency reviews pass; Parent assigned Mill (xhigh, `01a1b9df-105a-7f52-87d0-9c87f30c403b`) to effect/recovery on the frozen v2 composition, with no Native8 activation.
No composed Native8 activation or PG qualification can proceed before the CDC-begin and frontier refusal guards are implemented and verified for every Native8 phase, including null intent.
Root supplied four shared prerequisite paths in `/private/tmp/native8-shared-seed-ux0kf3mx/handoff.json`, SHA-256 `c4d741c39e0ab6ba8fe57ab8cdff2b4b582a185d7cc6fee10fcc530d47514abb`: `ControlRow` nonce, outside-owner null constructors, checked format-selection bridge and existing type re-export, with 194 base files unchanged.
Forward/inverse patch checks and formatting pass; the seed has no standalone green or runtime claim, and the accepted combined persistence review below covers it with Noether's real type, selector and metadata implementation.
Noether retains the earlier 700-pass/one-failure result and reports a final nonlive result of 702 passed, zero failed, two ignored and 82 filtered tests, including 13 Native8 state and 35 metadata cases.
Parent resolved the native-records guard: remove the extra `records.is_some()` ban because canonical Core3 source capture may coexist with COPY/postdata; preserve full canonical/source bindings, `copy_complete: null` and all Native8 target-frontier/ACK denials.
Noether reports an actual records-present RED followed by 13 focused Native8 passes after removing only the extra absence term, including valid reopen/idempotency and malformed or mismatched record refusals.
The provider is frozen in `/private/tmp/native8-persistence-sga4gjm3/handoff.json`, SHA-256 `69ae243fce733c25eafed8855257180ea60538bd9125baaad86426fd32a4f485`, as source198 `80917144`, applying the Root seed before the five-path provider patch `3852477e`.
Formatting and scoped Clippy pass by the author; 495 library, 34 test and three storage warnings remain, and global strict checks are not green.
Mendel's independent combined prerequisite/provider review passed the offline persistence scope: `/private/tmp/native8-independent-8qwisv_q/review-receipt.json`, SHA-256 `36912de31a81a6efa6f072475354bf7a1108c0d38889170af72301790b8165e9`.
Root verified that receipt; fresh 13 state, 35 metadata, one held-manifest guard and four independent checks passed, and controlled mutants were detected without reopening the reviewed PK base.
The review window is released, with no authority-begin, effect or PG clearance.
Captured records confer no target authority, and no authority-begin or runtime qualification is claimed.
Laplace froze the two-path archive export as source `188c4752`, patch `ddeccc1a`, after 75 artifact/process tests and scoped formatting/Clippy passed; inherited warnings remain visible.
Nash's assigned independent review passed the two-path offline artifact scope: receipt `/private/tmp/postdata-review-i46sgyuv/review-receipt.json`, SHA-256 `4c139225322c649dfcdd40aae63e85a7f5d21e9e40b3222ffbb38a8ad11f83ec`, binds source `188c4752`, patch `ddeccc1a`, 75 fresh artifact/process tests and five independent methods covering six cases, with zero failures or ignored tests.
Root verified the receipt hash and recorded the released window; real utilities on PG16/18 and 16-to-18, effects, Unknown/retry/restart and global strict checks remain open, and this artifact confers no target authority.
Root owns documentation updates in the final combined change; the two-file author's write boundary does not create a missing documentation authorization or an extra permission gate.
His 18 additional schema-session tests used loopback TCP peers outside the original literal no-network scope; the retained handoff and independent review preserve the deviation and joined peers/process exits without a rerun or retroactive authorization.
Parent prospectively permits inspected, owned loopback synthetic peers within Root's resource windows, including the metadata/CAS regressions; this does not reclassify the earlier deviation or permit real PostgreSQL, external DNS, remote endpoints or downloads.
Root froze `/private/tmp/native8-reviewed-composition-v2-235li80a/handoff.json`, SHA-256 `559358ec579a19f36e1fd48c7baed6b3ab4b08264c72b62e3d552304352b7c66`: source198 `5a5c698c`, exactly base `e5c1a69b` plus seed, persistence and artifact patches, with one Root product-documentation delta and 187 unchanged paths.
The first `03f74ade` packet and source `cbac8711` remain unchanged; v2 updates only the acquisition lifecycle documentation/API brief after Parent's correction.
Forward/inverse round-trips, all source identities and modes, and strict patch whitespace pass; no 702/75-suite repeat, build or PG run was performed for this handoff.
The frozen `API.md` identifies genuine historical schema/COPY durability coverage, one-use recovery freshness, all-Native8 authority denial and the Root-reserved source/dump/shared-registration hunks.
The approved `acquire_for_clone` entry must check the admitted archive, source/native identity, digests and local phase state before initial Native8 provisioning; whole-profile source FK/index/event-trigger refusal precedes that provisioning.
Ordinary Native8 acquisition remains existing-only, with exact metadata5/provenance/row validation and no creation, repair or upgrade of missing objects, and grants no phase proof.
Parent permits reconciliation of an already exact, owned row against genuine pending native `Provisioning` only with complete run/source/target/profile/nonce and expected-row bindings, preserving all other fields and requiring positive same-fence synchronous COMMIT, Idle and durable evidence before provenance confirmation.
Missing, malformed or mismatched objects refuse without provisioning; Unknown closes and withholds confirmation, and initial creation still requires validated `acquire_for_clone`.
The [canonical acquisition contract](../docs/superpowers/plans/2026-11-08-schema-postdata.md#acquisition-and-pending-provisioning) and separate prose/API addendum `/private/tmp/native8-existing-only-dl8r01k_/handoff.json`, SHA-256 `72460b8be2d53958eb1c0f144d1a6d7bf8350d1e6135504e6b1122f8e6d4d85f`, record this meaning without changing the frozen reviewed runtime source.
This preserves pre-COPY PRE-DATA recovery and committed POSTDATA recovery after the earlier SQL artifact is consumed; absent completion can replay only through retained POSTDATA SQL, genuine prior proofs and one-use fresh-fence eligibility.
Final POSTDATA checks must require strict valid PK, physical shape and FK absence without the temporary COPY PK-absence guard; complete object definitions, sequences, large objects, restart inputs and aggregate authority remain open.
The effect owner has only schema-effect code, non-registration checkpoint hunks, catalog admission/final checks and focused tests; state, metadata, artifact, CLI and dependency changes require an explicit scope decision.
Root retains the held `SourceSnapshot`/`dump_schema` bridge and shared re-exports; the supplied private overlay uses the owner's actual catalog signatures.
That bridge is a prerequisite for source-before-target tests; request a bounded Root resource window before compilation, and keep Native8 PG and activation held.
Root supplied the eight-path author overlay `/private/tmp/native8-root-bridge-k9nz48j7/handoff.json`, SHA-256 `3293ee6cd4362aed41b0bedb74ba785948572e8f2cb94b03d90397dec51722d8`, binding source198 `87407961` and patch `852a6096`; Mill reports exact conflict-free application to his private C candidate.
The added initial guard checks held Native8 identity and the existing pre-COPY/records boundary without changing state or rejecting matching pending schema intent and target provenance.
C must compare the full artifact/source/intent binding before provisioning or reuse, including refusal of mismatched pending intent without effects.
The separate approved test helpers enable POSTDATA on the owned fake utility and supply alternate synthetic archive bytes through the normal opaque archive factory; they add no production constructor or proof.
Four focused helper/guard tests pass on exact v2 plus those four paths, after retained missing-API compile failures; full bridge compilation requires Mill's real catalog/effect definitions, and this is not independent review or live qualification.
The `n8-effect-offline-5a5-mill` and Root author windows are released; neither granted PG access.
Root's follow-up `/private/tmp/native8-root-bridge-v2-jmk79h3o/handoff.json`, SHA-256 `db31711a08c391c69477d0d462d154c2f6f727dfcbceb4400f6336fa4f1fd8d1`, preserves the first packet and supplies only a two-path addendum for an actual owned-fixture cleanup error and a re-export formatting correction.
Five focused tests pass, the new cleanup-erasure control fails as intended and passes after restoration, and the four earlier guard/fixture mutation controls retain their separate evidence.
Both addendum and combined eight-path overlay round-trip all 198 hashes and modes without offsets; formatting passes, while scoped Clippy exits zero with 535 warning diagnostics, so global strict checks remain open.
Mill's composed effect tests include mismatched valid archive refusal before effects, primary outcome preservation when explicit cleanup fails, and the separately assigned test-only COMMIT-tag/non-idle peer response; the combined independent reviews below pass their bounded scopes, while Native8 live qualification remains pending.
Mill reports the byte-identical remint positive control passing; his default and all-target checks retain the inherited `E0433` storage-example visibility failure, with no Cargo or unrelated source change and no global green claim.
Mill froze the combined author candidate at `/private/tmp/native8-effect-final-v2-babk02by/handoff.json`, SHA-256 `0ff6af9dd8b24aca195ec13f086fa0a23c6c63acae09f27ec13936162b579458`, source198 `766617d4`, with seven effect-owned paths and 14 combined paths.
Root verified all 198 source hashes, the base and patch identities, the exact changed-path sets and the included content-identity prose; this intake is not safety review or runtime qualification.
The author reports 748 library tests passing with 84 live tests ignored, 31 format and 171 recovery tests passing, formatting success and scoped Clippy success with 541 compiler warnings; default and all-target checks remain blocked as recorded above.
Parent assigned Linnaeus (xhigh, `01a1ba0d-ddd3-7f31-abac-08b9d3e3203b`) to acquisition/state/metadata/CDC/recovery and Faraday (xhigh, `01a1ba0d-dc4f-7211-94c8-0d894e32dd94`) to SQL/archive/catalog/source-bridge/cleanup, both including the approved Root overlays and content-identity decision.
Their separate `n8-review-a-766-linnaeus` and `n8-review-b-766-faraday` windows allow private targets and scratch directories, two offline locked Cargo jobs each, bounded commands and owned loopback peers; filters must remain disjoint, without duplicate full suites or PG access.
B's [bounded review receipt](/private/tmp/native8-review-b-faraday-th1tbjw5/handoff.json), SHA-256 `22671e8792b343124657d1f9a6621179b6ab2d22024ce79d87a0b315e1b99de3`, passes its SQL/archive/catalog/bridge/cleanup scope with 21 existing focused tests, two independent health cases and three detected runtime guard erasures, followed by exact source restoration and formatting success.
Root verified B's receipt and recorded its window released; frozen source `766617d4` remains unchanged.
A's bounded receipt `/private/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/native8-review-A-grgqhxfb/review-receipt.json`, SHA-256 `69d0bf36e885f027c06008884a9f21b864e078ba78bbce614ef7e8a55af21b62`, passes acquisition/state/metadata/CDC/recovery with 36 distinct existing tests, four private controls and five valid runtime mutants.
It explicitly covers the `72460b8b` existing-only prose and twelve pending-acquisition cases, including Unknown and mismatched-row refusal without provisioning.
Root verified the receipt and exact source scope and recorded A's window released; both reviews bind source198 `766617d4`, not a merged or globally qualified candidate.
The previously approved [storage-example wrapper packet](../target/verify/global-test-wrapper-1_8ydpvj/handoff.json), SHA-256 `da27bc977dbadb6fb5d23b00f4bc03f610526e1f2ac3964ada14ad72c1a6cee1`, changes only the Cargo example path and adds the nine-line `bench/storage-main.rs` wrapper.
Its [independent review](../target/verify/publication-wrapper-review-04fbipgb/receipt.json), SHA-256 `1c019fab18c6c291fcfead2e4634cff666a6b75a8b558a0a3c6344cb56aa1406`, covers that bounded entrypoint correction without production visibility, dependency or lint-suppression changes.
Root's read-only application check of exact patch `45306721` against source `766617d4` passes; it remained outside the frozen safety inputs and was applied in the separate integration candidate below, preserving current Cargo dependencies and publication policy.
Parent assigned medium integration worker `01a1ba11-0708-7252-9779-69e36a69724c` to a separate private `766617d4` plus exact two-path wrapper composition and required non-PG gates, preserving SHA2, `publish = false` and the initial `E0433` negative.
That candidate does not alter the frozen review input; Root granted `n8-wrapper-766-453067-offline` for serial required non-PG gates at one Cargo job while both reviewers reserve two each, with separate targets/scratch, command bounds and fresh memory/disk floors.
After B released its window, Root permitted two jobs for subsequent integration commands without interrupting or repeating the active one-job command.
The required tests may compile their existing synthetic C fixtures and locked bundled SQLite dependency, but must not rebuild packaged libpq or PostgreSQL tools, download dependencies, invent leased test inputs or change source after failures.
The integration worker reports exact source199 `71c418b0` and a reversible two-path composition; `task check` exits zero, while strict `task lint` exits 201 with 508 diagnostics and no observed `E0433` in either log.
The [sealed integration receipt](/private/tmp/native8-wrapper-gates-thtypgpz/handoff.json), SHA-256 `f356b9b83779c590231f53c811e9f514aff3f92de9335655d1160b2c5b88f5d7`, records `task test` exit zero: 748 Rust tests, two ignored and 82 filtered, plus 24 CLI, 20 native, 208 harness, 31 format, 171 recovery and 22 publication cases with 20 continuations.
Root verified all 199 source hashes, the exact two-path change from `766617d4`, patch hashes and all three command-log hashes/exits, and recorded the integration window released.
The genuine unresolved gate is strict lint: Task exits 201 after Cargo exits 101 with 508 errors under `-D warnings`; all-target Clippy did not complete, and neither baseline equivalence nor global readiness is established.
The retained-output triage receipt `/private/tmp/native8-strictlint-triage-28_ie4ae/receipt.json`, SHA-256 `78ffc92ddd8adad35b41ca6cd3cb25229086eda291429be7b1faa05d6c76413f`, attributes all 508 errors to the normal library: 504 dead-code diagnostics and four unused-import groups containing sixteen bindings, with no observed Clippy-specific finding.
The accepted Cargo/lib/main/caller graph confirms the approved private library plus thin main and genuine unwired runtime consumers, not duplicated scaffolding.
Record the remaining work as CLI/coordinator integration plus genuine unused-import cleanup; the exact baseline delta is unknown, and moving modules, widening visibility, suppressing warnings or adding cosmetic calls must not replace that work.
The wrapper changes only Cargo's example path and the new wrapper file; content-identity prose is already in `766617d4`/`71c418b0`, while the reviewed `72460b8b` product/API prose remains a separate addendum and the maintained contract contains its meaning.
Unfiltered default Cargo tests, Linux/CI, Native8 PG and merge remain uncleared; the completed `task test` excludes leased integration paths and does not qualify them.
The [Native8 real-utility qualification proposal](../docs/superpowers/plans/2026-11-08-native8-real-utility-qualification.md) defines three cases on each of PG16.15-to-16.15, PG18.6-to-18.6 and PG16.15-to-18.6, using real source snapshots, the same opaque archive, PK-absent nonempty COPY and genuine POSTDATA proofs.
Both bounded reviews permit this planning step only; Parent design review precedes implementation, and the new composed harness/source gates and a fresh Root lease precede execution.
Strict lint remains an explicit merge/activation blocker.
The proposal must include the already reviewed publication timeline `int8` hunk still absent from frozen `766617d4`; this prerequisite neither attributes Native7's historical failures nor promotes another profile's qualification.
Root reverified `/private/tmp/origin-int8-p1-complete-l9gkq1k9/handoff.json`, SHA-256 `05758dc87384f35c540345ec1b477a8a9f0e8f939f47ba0d419f81e12f55e6f0`, its full four-path patch `3daba182`, publication-only patch `672aad56`, inverse hashes and independent receipts `60b33c27`/`78c6fe13`.
Read-only application checks pass against `766617d4`, with unchanged literal context and line offsets in the source helper and documentation; no patch was applied.
The retained tool receipt `be5ef51f` binds the actual 18.6 dump/restore binaries, and the frozen discovery/framing contract permits a 16.15 source, matching 18.6 utilities and 18.6 target.
[PostgreSQL 18's utility documentation](https://www.postgresql.org/docs/18/app-pgdump.html) supports dumping older servers with exported snapshots; this confirms version-selection compatibility, not the unrun real SQL/privilege/DO path.
The held-reader requirement is a lifecycle invariant, not another known correction packet: frozen `schema_effect.rs:595-603` drops its validation reader before publishing the POSTDATA intent, then opens a new checked reader against the updated manifest.
The design reviewer must require the harness to preserve and test that ordering within the nine-run proposal; no separate patch or inherited review is invented.
Independent design report `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/native8-design-review-me3r64hs/review.txt`, SHA-256 `3319bfdd0e796a3989cf3ad799e67a0454b20c525c7b0e5002743b82aba6b81e`, requests P1/P2 revisions and is not design acceptance.
Root's R2 retains nine runs and expected proof/resource counts, makes post-COPY relay arming and bounded contention explicit, reserves untouched fresh recovery authority, and replaces the proposed local failure with Parent's selected real backend SQL error after unchanged POSTDATA execution.
It also specifies reader release/publication/reopen, independent cross-image contexts and socket paths, ordinary tested-role privileges, isolated refusal authorities and the exact offline coverage limits.
R1 `a0e39a4d` is preserved at `target/verify/native8-qualification-plan-r1-a0e39.md`; R2 must reach Parent before the same assigned reviewer rechecks it, with no new author, runtime edit or PG lease.
Parent acknowledged frozen R2 SHA-256 `36949a5c36e64258d990fd423afe19651f9478251c8f47e749112ba86d4b82ea` and directly assigned the bounded recheck to the same reviewer, Popper `01a1ba35-9ee2-74a0-816b-295e8e93de38`.
Preserve R2 during review without expanding proof scope; strict lint remains red, with no implementation, PG lease or CLI activation.
The same reviewer's R2 report `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/native8-design-r2-review-8l9269_f/review.txt`, SHA-256 `55179d7dbb0da4ec7f2937b9871185ddf70a655adcc45376f3a87b119258a6e9`, records `PASS_DESIGN_BOUNDED` with all five findings addressed and no remaining ranked design finding.
Root verified the report and exact `36949a5c` plan hashes; nine-run proof/resource counts and 270/300/900/960-second bounds remain design expectations, not execution evidence.
Parent accepted R2 and assigned the Rust and Python authors after Root froze the exact private composition `1ba1f780`, source199 `c24fdfec`.
Approved interface V2 is `/private/tmp/native8-shared-interface-v2-wf4496il/handoff.json`, SHA-256 `ad4f1eb50dc0680ae2ab20f4af9bd92cf5d948974634b53cf87ddc27b2d01f3d`, with source200 `c781da35`, API `1bfc8545` and contract `1153054f`.
Its two Root paths add the contract and real test registration; planned application connection counts are 20/9/10, including the internal `dump_schema` probe, and are not runtime evidence.
Parent approved the exact twelve relay facts and six startup fields as actual proxy observations, with acknowledged COPY before arming, one held COMMIT, independent fence/row checks and observed backend/lock release before recovery.

- [ ] Freeze one complete prepared-run, callback IPC and lifecycle addendum before either author resumes contract-dependent implementation.

Both authors are paused because the ordinary manifest lacks the paired-controller/runtime input and a callback child cannot access the parent's relay during blocking execution.
Root alone owns this dependency: inspect existing helper reuse, specify every constructor/path/action/cleanup binding, obtain Parent review, then send the same frozen addendum to both authors.
Do not issue piecemeal field decisions, invent glue, change shared lifecycle behavior or substitute relay observations for authority proofs.
Python's two-path standalone HOLD packet `9d79e2d5`, source202 `99e147c0`, records 20 pure tests and four detected mutants; it is not READY and does not implement the profile lifecycle, callback IPC or real oracle.
Root verified that Python receipt and found zero matching owned processes; its author window is released, with synthetic children reaped and zero reported listeners or PG resources.
Rust's standalone HOLD packet `/private/tmp/native8-rust-r2-author-SLvhs4/standalone-freeze/handoff.json`, SHA-256 `da3e1472b21d6b947cb8159270d154e5524544aecafa48a687e88da34506a6a0`, records eleven hook controls, five detected mutants, formatting and a normal library check with inherited warnings.
Root verified the packet and zero matching owned processes, then released the Rust window; its exact two-file hook freeze and separate incomplete, uncompiled producer remain preserved and are not READY.
Parent closed both author contexts until the complete addendum is accepted; no parallel glue or dependent implementation is released.
The [prepared-run and callback control proposal](../docs/superpowers/plans/2026-11-08-native8-prepared-control.md) separates outer cluster ownership from one proposed Native8-only callback service and names the missing cross-image runner composition explicitly.
Its immutable copy is `/private/tmp/native8-prepared-control-8tqot42p/proposal.md`, SHA-256 `b8bd92e48af851c27ca35465bfcc1871378548ffea60aae845557b41d294e95c`.
Parent assigned the same xhigh design reviewer, Popper `01a1ba35-9ee2-74a0-816b-295e8e93de38`, to actual helper reuse, ownership, namespace, IPC and cleanup feasibility; preserve the proposal during this read-only review.
Neither author resumes before the complete addendum is accepted and shared identically; no Native8 PG lease exists.
Popper's final design report `/var/folders/k2/75psc6cn37z35v6mfyjn22qc0000gn/T/native8-prepared-design-review-azmja5jn/review.txt`, SHA-256 `53b0a706ff3693236d629b2cf25b23dee8fff1f9e587354f3c65394f91a88fb7`, requests changes to socket reachability, actual relay closure/upstream reaping, IPC deadlines and cleanup headroom, plus exact mapping and effective-contract details.
Preserve `b8bd92e4`; Parent selected a simpler topology assessment before one complete revised proposal: one existing Linux runtime, two distinct `OwnedCluster` instances and immutable PostgreSQL 16.15/18.6 prefixes.
Cached image metadata showed identical first eleven layers and distinct versioned prefixes in the final layer; retained tool hashes and the source API supported the option but did not prove combined dependency resolution.
Parent authorized only a bounded metadata export/mount, ELF/dependency/share/extension inventory and version check after the active N7 lease is released and absence is verified, without PG initialization, server startup, rebuild or downloads.
After Native7 release, [metadata closure receipt](../target/verify/native8-prefix-mounted-vvjlvbku/receipt.json), SHA-256 `e61361714f5a2192176d6545e644fc4e6f220d23b4cc2fc217017ecd5779ab52`, passed in the pinned PG18 runtime with the complete PG16 prefix mounted read-only at its original absolute path.
The check bound 1,704 PG16 entries with 68 ELF roots and 71 resolved dependency nodes, plus 1,749 PG18 entries with 71 ELF roots and 74 dependency nodes; all PG16 bytes, modes, links, tool/config paths and dependency selections matched the original image.
The first export check remains negative as `0491f877`: the host copy changed eight symlink modes, which were restored to the recorded image modes in a new owned copy before the successful mount check.
Both versions passed metadata and `--version` checks, including share and plugin inventories, without server startup, SQL, real utility execution, rebuild or download; the owned container is absent and no PG scope remains active.
Use one PG18 runtime and two independent `OwnedCluster` instances in the revised complete proposal, removing paired endpoint-holder/socket translation while retaining nine cases, eighteen clusters and the actual 16-to-18 route.
Root froze the complete [prepared-control R2 proposal](../docs/superpowers/plans/2026-11-08-native8-prepared-control-r2.md) in `/private/tmp/native8-prepared-r2-_1elu84y/handoff.json`, SHA-256 `209a1f5d709c8d414a5c10ddbc2b193b1411db1436bcf97fb10012606c865700`.
It defines one local cluster owner, actual ordinary-mode relay descriptor/upstream observations, bounded IPC/observer/runner teardown, exhaustive operation/DO mapping and one corrected whole-row OID contract.
The proposed contract is `ec410ea40da5b5be7c9357fece05daffad45befa3d2cd9f5993826f2db33f879`; exact forward/inverse roundtrip and input hashes passed without implementation, runtime tests or PG.
Parent accepted this complete design after Popper's bounded review `b382cf3b882d4ce759f48e5f8ef73ea5f149d59bbbc180ddf61d5bea790dc1c8` closed the earlier findings.
Root froze the identical actual overlay in `/private/tmp/native8-prepared-r2-overlay-22uiif16/handoff.json`, SHA-256 `01b95ddd186678065a43a9f57d7a5cac739e8886892a1320d2a6cb957ead466c`.
Its source file count is 200 and its source SHA-256 is `b56be9e8df2eb5572e095ee75260c9e102a77d8d65b196d07ec67c82b14ad6fc`; only the contract changed, with 199 files and all modes preserved through a zero-fuzz forward/inverse check.
The unchanged accepted API and contract hashes are `5f9aaafa2a7d6584a10d0472ac2bb1d5212be2574867aa45ee179cab34073ccb` and `ec410ea40da5b5be7c9357fece05daffad45befa3d2cd9f5993826f2db33f879`.
Planned application-native connections are 39 per triplet and 117 per matrix; planned callbacks are 47 per triplet and 141 per matrix, pending the actual constructor/helper audit.
Parent approved a separate two-path passive relay owner; its per-instance thread handle and completion event do not establish success, descriptor closure or reaping.
Parent resumed both retained authors on this same overlay and assigned the separate passive relay owner; the freeze itself includes no implementation, placeholder helper, build or Native8 lease.
Root granted bounded offline Rust and Python windows; the relay's two unexpected host held-join failures remain negative, and its held path is paused for a source-grounded proposal without a blind Linux repeat.
The Root-only normal-test registration addendum `1c6968bb` adds `schema_postdata_producer_controls` outside the live prefix and preserves the existing ignored auxiliary; actual producer controls and compilation remain the Rust owner's work.
Parent approved the required private absolute engine deadline and a fixed profile-specific observer process with one nonrenewable callback deadline and actual PID/reaping evidence.
The identical addendum `/private/tmp/native8-r2-deadline-observer-g5vthkd6/handoff.json`, SHA-256 `e3b5b41c9442b70cbb900eb0c13cf9c2e1ec8c65dab1f8172638e4465fc8ec00`, binds source file count 200 and source SHA-256 `2d9bbfc70747f02a3b3d31b71244f1ca3564c3e4b24f026e883a06b793d80023`.
Its API is `2e6fd634bfe18460157cd07a42906c280914626860be5a13f05d6d8bc45ccd58` and contract is `3376cb349ce2ed32727b41184a497c2d6cef03e72437ca6c9eebe7a669eb74f8`; 198 source files and all modes are unchanged, with an exact two-path forward/inverse check.
All three authors received the same addendum, which retains wire version 2 and the existing cap values without production changes or PG execution.
The passive helper's controlled base/shim and frontend-lifetime comparison has one bounded no-PG Linux metadata window, `n8-relay-linux-carver-01`, owned by Carver; original host failures and the HOLD packet remain retained.
Exact composition, offline/Linux controls, independent integration review and a fresh Root lease remain required before PG; strict lint remains red and no CLI activation is granted.
The separate transient schema COPY shape change permits absent target primary keys only under genuine schema admission and keeps present keys and ordinary COPY/WAL checks strict.
Nash's assigned independent review `63a3262f` passes the bounded policy scope on Mendel's frozen `e5c1a69b` source; scripted peers do not qualify nonempty PostgreSQL transfer, the real utility gates remain unrun, and inherited global strict warnings remain open.
The runtime packaging unit is frozen as handoff `89144b972e387e3fb74a8fc6eb9ce3588f0b5c5879de4c60ae61eefe74b315e7`, declared source `54b7147e`, and fifteen-path patch `41cddeb6`; independent review requires the two loader corrections below.
James withdrew the Docker context finding after Lorentz's two actual Podman COPY probes accepted the original rules; the reviewer's model had omitted parent-directory negations.
Do not apply the proposed `.dockerignore` change or add a sixteenth path for that withdrawn defect; preserve the contradictory probe evidence and owned scratch cleanup.
Corrected review receipt `301600e384eef2820080741642ec4d77ab7bd43b74237cd8287a9940b11e8972` requests changes for two loader findings: valid absolute `ldd` loader lines are rejected, and claimed dependency paths/hashes do not establish actual final-image selection.
Parent approved Volta's bounded correction: parse actual absolute loader lines and require the existing smoke check to bind every selected dependency path/hash and interpreter under the final image environment, reusing the existing receipts and recipe version.
The approved scope adds no schema or search framework; static inventory alone cannot qualify loader selection, and no native/application image build or PG run is released for this fix.
The six-file loader addendum is frozen as handoff `fa6edc2b`, source `9ee40245`, with independent static packaging PASS `af5c1da88ba77a57e47cc7e00677af4946ff265f9b3f324f27b841d1ebb80aae`.
The review reports 189 fresh tests, preserved original test bodies, workflow lint, and 35 independent assertions; the mandatory final-image loader check is a live gate, while static graph inspection claims only inventory validation.
Root composed that exact addendum with the original fifteen-path base; the complete four-way version/architecture build and 24-check smoke gate remains unqualified.
It adds separate pinned PG16.15 and PG18.6 client-tool variants through existing image/native tooling, without a migration handler or production schema qualification.
Root checked all fifteen patch preimages against actual GitHub main `86b80406341bfbfac6b5b0d7a73410d3703ceb52`, tree `ace698ba`: thirteen existing files match and both new files are absent; all fifteen result blobs match the author manifest.
The [checkpoint candidate](../target/verify/runtime-packaging-checkpoint-cauulrgs/candidate.json) has exact tree `0626bb912c3ad2ec998be79e8d09060d9d9afac4`, all 117 source files verified, and only the fifteen reviewed paths changed from main.
Fresh Task check, strict lint, and default tests passed with a newly built native receipt: 128 unit, 31 format, 171 recovery, 22 publication cases with 20 continuations, 24 CLI, 20 native, and 24 harness checks.
The source-derived CI unit inventory passed 188 tests; its main-derived unselected Task/verifier tests differ from the author's private baseline, so the author's 189 total is not substituted.
Commit `aadbe7b35edcfbfe1a675d02435b3df200797c07` was pushed only to `feat/native-copy`; its tree matches the tested export, all 149 unrelated tracked/untracked path identities and modes remain unchanged, and the normal index is empty.
[PR #4](https://github.com/ydixken/rpgmigrate/pull/4) was opened on that exact head; its [earlier hosted matrix](https://github.com/ydixken/rpgmigrate/actions/runs/42435783979) found `dependency interpreter target mismatch` during amd64 runtime image builds, before smoke and receipt binding.
The [first failed push job](https://github.com/ydixken/rpgmigrate/actions/runs/42435746449/job/128594083899) and its retained log `a5b9fde9` identify the predicate at `tools/image/runtime.py:251`, but not the underlying path/object mismatch.
Merge is blocked until the actual Linux failure has a reviewed correction and the required matrix passes; local checks and the earlier static review do not replace this gate.
The reviewed diagnostic-only follow-up `4f17c6f9` has independent PASS `7a2ff294`; it preserves the rejection predicate and reports bounded system paths, alias steps, ELF presence, hashes, architecture, and source identity.
The first standalone synthetic probe had an import error and remains retained; its corrected probe reaches the exact rejection for both client versions, without establishing the actual hosted cause.
Root committed and pushed only `docs/container.md`, `tools/image/runtime.py`, and `tools/image/test_runtime.py` as `65a9d73cda2835d7e59212bf72807c46fd75af88`, exact tree `6021df68`, preserving all 161 unrelated path identities/modes and an empty normal index.
Fresh candidate check, strict lint, default tests, and 191 CI unit tests pass; the initial isolated-cache omission is retained, and the default suite passed after restoring the exact pinned archive without changing source or rebuilding native code.
The [new PR matrix](https://github.com/ydixken/rpgmigrate/actions/runs/42438931252) and [push matrix](https://github.com/ydixken/rpgmigrate/actions/runs/42438928295) bind this diagnostic head; both amd64 client variants fail during runtime image construction.
The [retained hosted diagnostic](../target/verify/pr4-diagnostic65-gryqzqmy/receipt.json), `e795d619`, binds the exact completed job log and its 699-byte diagnostic: `/lib64/ld-linux-x86-64.so.2` resolves through the sole retained directory alias to `/usr/lib64/ld-linux-x86-64.so.2`, which is not an inventoried ELF, while the recorded `/usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2` is present with hash `c8438e4f`.
Volta's controlled archive reproduces both hosted path/ELF outcomes: the collector excludes `/usr/lib64` before inserting records, while the existing resolver succeeds with the complete same chain.
Parent approved only the exact `/usr/lib64` inventory-prefix inclusion, with unchanged root restrictions, recorded-path/hash equality, and mandatory smoke checks; the actual hosted terminal tar entry was not retained.
The minimal collector patch `22662440`, handoff `7061923e`, source117 `0cea7003`, has James's independent static PASS `270aa5a5`: 195 checks, 83 independent assertions, and exact 117-file roundtrip, with the resolver and strict acceptance unchanged.
Root prepared [candidate tree `a0506d68`](../target/verify/runtime-terminal-hop-checkpoint-udjk_1di/candidate.json) from pushed `65a9d73c`, changing only the collector, its tests, and container documentation while preserving the normal index and unrelated working files.
The candidate passes check, strict lint, and default tests, but its CI-unit run executes 195 tests and fails one new PG18 missing-loader assertion: the validator refuses with `dependency soname mismatch`, while the test requires `interpreter target`.
The original failed log is retained as `315815be`; four narrow fixed-seed runs pass without changing source and do not replace that failure.
Volta reproduced the fixture-order defect on seven of 32 fixed seeds and froze test-only addendum `e62a2e03`, source117 `99d021d2`; production traversal and validation remain unchanged.
The corrected missing-interpreter fixture retains all other required dependency targets and the narrow refusal assertion, while explicit controls preserve the complete default dependency graph.
Root prepared [candidate tree `63418e7f`](../target/verify/runtime-terminal-hop-stable-7dx942gn/candidate.json), verifying all other 116 files against the checked candidate; assigned MEDIUM reviewer Descartes (`01a1b930-b1a4-7e42-a98b-7d73247fc5c5`) passed the focused test-only scope as `9f903bd8`.
Fresh check, strict lint, default tests, and all 195 CI unit tests pass on that exact candidate.
Root committed and pushed the three reviewed paths as `6d47b9e763e4897dcd5b5993a435d32aa96bf195`, preserving unrelated bytes, modes, and symlinks across the 164-path snapshot and an empty normal index.
The [new PR matrix](https://github.com/ydixken/rpgmigrate/actions/runs/42443567449) and [push matrix](https://github.com/ydixken/rpgmigrate/actions/runs/42443563412) are running on this head; all four real builds and 24-check smokes per image remain required before merge.
The maintained public task update is a separate checkpoint from this packaging correction.
This task ledger and private qualification exports are local working state, excluded from the fifteen-file packaging checkpoint.

- [x] After independent packaging review PASS, prepare an ordinary main-derived export containing only the exact fifteen reviewed paths, preserving primary source, index, modes, and user WIP.
- [x] Run candidate Task lint/default tests with a freshly qualified native recipe receipt; changed builder/NOTICE identities cannot inherit the old native-20 receipt.
- [ ] Qualify all four PG16.15/18.6 by native amd64/arm64 image builds and exact-artifact smokes, including actual tool versions, library closure, cache keys, and provenance.
- [x] Push the small verified feature checkpoint and reviewed diagnostic follow-up through PR #4, preserving local implementation work.
- [ ] Merge only after all required checks and independent reviews pass; no direct main push, registry publication, or runtime/alpha claim.

Native6 preparation and qualification take resource priority; defer heavy local native/image builds and prefer the existing hosted matrix for the reviewable packaging candidate.
The [fenced pre-data plan](../docs/superpowers/plans/2026-11-07-schema-predata.md) uses export-only utilities and one native `DO` on the held target transaction, with always-synchronous atomic schema completion; Native7, target metadata4, unchanged CoreSchema3, `SchemaIntentV1`, and opaque SHA-256 identity are frozen, while end-to-end integration and PG16/18 qualification remain open.
The parent approved `sha2 = { version = "=0.11.1", default-features = false }` after Darwin's private audit: seven additive packages, graph 31 to 38, unchanged existing versions/features, MIT/Apache-2.0, and MSRV at most 1.88.
The private integrated lock is `8f057e1b`, delta `accd974e`; default and software-backend host tests passed, manual RustSec snapshot `309392b1` had no applicable advisory, automated audit/deny were unavailable, and Linux builds remain required.
No primary dependency changed; apply the approved delta only with an actual caller in a reviewable schema candidate.
The private export/framing unit retains the held archive for the subsequent SHA-256 caller; final packet `6dbdbd70` has independent PASS `4bcb66ae`, with 62 author and 37 review tests including 25 peer controls, while live utility qualification remains open.
The original private Native7/metadata4 state unit remains frozen as handoff `02d9409b`, source178 `4f904fba`: exactly eight intent fields, required-nullable native storage, and only the optional nonzero pre-data nonce in the target row.
The original author evidence retains 277 scoped passes and 19 copy-plan fixture failures reproduced exactly on the frozen P1 baseline.
Replacement handoff `e634c755`, source178 `816c3695`, adds only the four-line shared-fixture publication intent/confirmation before its pgoutput slot; all other 177 files, including production state and metadata, remain byte-identical.
The actual 19 RED controls and prior 277-test union now pass as 296 scoped tests with zero failures or ignored cases; Parent recorded independent model/metadata PASS `6961cfb0`.
The two compiled-but-unrun metadata live cases, future checkpoint/effect guards, and integrated strict gates remain open; the state review does not authorize schema effects.
Utility versions and limits stay in sealed export policy; there is no CopyPlanRef, duplicate target intent/counter, hashing caller, schema effect, or proof constructor in this state unit.
The actual schema effect implementation and PG16/18 qualification remain open.
Parent released xhigh effect owner `01a1b862-1bc4-71f1-835b-c1b38d1f2719` on a private Native7/framing composition with the exact approved SHA-256 dependency delta; Root supplied the actual API, lock preimage, patch, and cached no-feature library identity.
The ordinary pre-data profile must check the held imported source snapshot before dump, admitting no extensions or only `plpgsql` and refusing foreign extensions or query errors; no SQL scanning, grants, automatic extension installation, or source ownership change is allowed.
Parent approved the companion target admission predicate: preinstalled `plpgsql` 1.0 in `pg_catalog` with exact language membership, because a re-added source extension can emit `CREATE EXTENSION IF NOT EXISTS`.
The effect owner must retain PG16/18 provenance and refusal regressions before `DO`; this is the existing no-install boundary, not general extension attestation or live qualification.
The effect packet `91dcf688`, source187 `f67110d9`, reports 374 affected passes and three compiled-but-unrun live cases; scoped Clippy leaves 36 test and 492 library staging warnings visible.
Independent review `c00d6f832bc83e7f75925bd724c40979a898295c66f6887caad5056489d2dc92` requests changes because the sealed-plan ordinary getter rejects schema COPY and recovery even with a genuine SchemaProof.
Anscombe's additive packet `ae42fccc`, source187 `023ebe43`, preserves ordinary refusal and all fields/formats while admitting the genuine schema phase through both actual sealed-plan callers.
Archimedes' independent receipt `5bb2c9b6226150204528e9830d09af2e9c32b2323a871adfcfd224da1cad5d57` passes the offline effect scope: 377 candidate and two reviewer tests, eight presence combinations, eight schema and six COPY identity refusals, and two detected caller mutants.
The original rejected packet remains immutable; three compiled live cases, Linux bindings, and schema PG qualification remain open, with no full-clone claim.
The target-only Native7 harness is frozen as `cc3a2f8b`, source197 `bb1201c8`; Archimedes' independent review `da51d893` requests a capture-order correction.
The absent-nonce recovery control closes its Session before the later capture requires that Session's fence; Parent assigned a mechanical move of capture before the negative assertion, preserving all eighteen assertions, twenty witnesses, and closure behavior.
The one-call reorder is frozen as `fd053830`, source197 `6171cc98`; focused independent review `d1a50bda` passes 23 fresh Rust and 22 Python checks and detects both old-order and omitted-refusal mutants.
The other 196 files, all eighteen assertions, three case names, twenty-witness checker, and production Session closure remain unchanged.
Root accepted [PREP `7cc5c9de`](../target/verify/native7-linux-prep-sqnax6se/prep-packet.json) as `2758231a`, binding source197, runtime219, fresh libtest `54842702`, native20, ten existing inventories plus exactly three schema cases, both images, finite bounds, and owned cleanup.
The sole `n7-schema-7cc5-zeno` attempt stopped at its first PG16 command: child exit 101, reaped without timeout, no admitted proof or case counts, and PG18 unrun.
The [sealed negative receipt](../target/verify/native7-schema-live-hsck6kr5/execution-receipt.json), `347b9247`, preserves stream/proof/protocol refusals without a safe panic location or predicate; the cause remains unknown.
Root's [negative acceptance](../target/verify/native7-schema-live-hsck6kr5/root-negative-acceptance.json), `f1572a67`, verifies all thirty artifacts and PREP references and independently confirms exact container absence; the lease is released.
Hegel verified the corrected sequencing and source197 without proving a source-only cause.
Parent approved a separate Native7-local diagnostic in its fixture, tests, and documentation: fixed Python codes, with an optional narrowly enumerated known Rust-site projection and `UNLOCATED` for unknown sites.
No dynamic values or raw stdout may be retained, and proof/status/count predicates remain unchanged; independent review, exact PREP, and a new lease precede another execution.
The diagnostic is frozen as `de59f0d2`, source197 `78144ee4`; Nash's independent review `56308ed0` passes 262 support checks, 216 overlapping registered checks, and eight independent methods covering 72 cases, with the 88 Rust files unchanged.
The bounded epoch review confirms that Native7 permits the required zero epoch; the inherited Native6 `validate_final` minimum-one check is unreachable from this profile and does not explain the prior failure.
Root accepted [diagnostic PREP `444e9c35`](../target/verify/n7-diagnostic-linux-prep-gdbaw99v/prep-packet.json) as [bounded PASS `358b3283`](../target/verify/n7-diagnostic-linux-prep-gdbaw99v/root-prep-acceptance.json): 292 artifact entries, runtime219, compiler-input equality for explicit libtest `54842702` reuse, native20, both images, exact three plus ten prior inventories, fixed-code bindings, and owned-container absence.
Under Parent's standing conditions, Root granted Zeno the unique `n7-diagnostic-444e-zeno` lease for three PG16 cases followed by three PG18 cases only after PASS: fourteen fresh runs and forty native witnesses in the target-only synthetic-source/fake-utility scope.
Fresh atomic preflight, 600-second children, 900-second commands, a 2100-second host bound, 300-second cleanup reserve, and safe evidence before both cleanup attempts remain mandatory; first actual failure stops execution without retry.
The first private runner invocation stopped before PG on an old packet-field lookup; its zero-PG result and exact metadata correction remain separate from the subsequent actual command.
The [new sealed negative](../target/verify/n7-diagnostic-444e-live-xzwp31hf/execution-receipt.json), `d02c86e1`, preserves the first actual PG16 child exit 101, reaped without timeout, with zero admitted proof/witnesses, unknown case counts, PG18 unrun, and no retry.
Root's [bounded negative acceptance](../target/verify/n7-diagnostic-444e-live-xzwp31hf/root-negative-acceptance.json), `8ba454e0`, verifies 81 artifacts and the bindings; eight admitted fixed codes include `N7_CHILD_SITE_UNLOCATED` but establish no exact originating predicate or historical cause.
A separate host collector assertion followed safe export because the earlier destination caused a nested copy; the nested safe evidence and original exit 101 are retained, and both owned containers are independently absent.
The lease is released; Parent approved Hegel's six-path test-only structured phase/result channel, without production changes, raw reconstruction or PG execution.
Author-reported Python support 270 and registered harness 224 tests plus formatting pass; a fresh compiled producer control must demonstrate exit 101, reaping, canary exclusion and no database/helper start before READY and independent review.
Root opened one serialized author compile/control window after Noether and Laplace reported idle; Hegel started the earlier authorized host `rustc --test` plan before the alternative Cargo preference arrived, so the Cargo/COW plan `54741b67` is unused and no duplicate run is authorized.
The completed host control binding `891c2a7f` records effective plan `71447a13`, final source `35bf0e34`, fresh producer `7270f942`, mutant driver exit 1 and restored driver exit 0; all six inner children exited 101 and were reaped, with no retained canary, database/helper start, PG or dependency/native build.
The author window is released and the six-path packet is frozen at `/private/tmp/native7-structured-diagnostic-hpap7aay/handoff.json`, SHA-256 `7c9593bc937705a0ec9e6188e66f3c8ab1433d33035a531d6ce025cbb5c8f2b4`; Root checked that handoff hash, and Parent assigned Kierkegaard (xhigh, `01a1b94d-6c84-7951-98eb-b3ab2d3cb838`) to independent review.
Kierkegaard's independent review passed the offline structured-diagnostic scope with no findings: `/private/tmp/n7-structured-independent-2fa95csv/review.json`, SHA-256 `183723c0c5cb751c29b398420359a88ee211998a87c6742411ccb425a6116aa4`.
Root verified that receipt hash; fresh focused Python 36 tests, independent red-driver exit 1/green-driver exit 0 with six reaped inner exit-101 children, and seven adversarial checks passed, preserving three live cases plus one ignored auxiliary control.
The review window is released; Root accepted fresh [Linux PREP `2eed2651`](../target/verify/n7-structured-linux-prep-wuk6d1ij/prep-packet.json) in [receipt `81e61769`](../target/verify/n7-structured-linux-prep-wuk6d1ij/root-prep-acceptance.json).
That receipt binds source197 `35bf0e34`, runtime219, new Linux debug libtest `a4caf7cc`, actual debug SQLite `1faff219`, native20, both images, three live cases plus the auxiliary control, ten prior inventories and the 25-stage/10-kind/9-field maps.
Both images passed the compiled auxiliary driver: three expected reaped inner exit-101 children each, six total, with no retained canary, native DB witness, DB/helper start or live qualification.
Under Parent's standing authorization, Root issued the unique `n7-structured-2eed-zeno` target-only lease with fresh absent SAFE export destinations, 600-second children, 900-second commands, a 2100-second host bound and first-failure stop.
Zeno reports the first actual PG16 command exited 101 without timeout; PG18 was not started and no retry occurred.
The 71 admitted structured records in three failure groups end at `CALLBACK ENTER` after successful acquire, fixture and request stages; there is no terminal callback result or typed error establishing a cause.
The [sealed negative receipt](../target/verify/n7-structured-2eed-live-cm_x7tr6/execution-receipt.json), `e2511574`, records a 2.25-second PG16 command, no admitted proof or native witness, unknown case counts, and no secondary error.
Root's [bounded negative acceptance](../target/verify/n7-structured-2eed-live-cm_x7tr6/root-negative-acceptance.json), `2b21067f`, verifies 46 artifacts, source/binary/native/image/argv/floor bindings and all 71 admitted records, and independently confirms the exact container and all running containers absent.
Seven fixed Python refusal codes add no exact predicate; no raw refused payload was reconstructed, no probe or retry ran, and the lease is released.
Host controls, Linux controls and this live negative remain separate evidence; historical `d02` and `347` causes remain unknown, and no Native8, real-utility or full-clone qualification is claimed.
Hegel's bounded source/runtime audit found a current oracle contract mismatch: the fixture expected the bare script digest, while the actual producer hashes the full validated export body.
This static defect does not establish the first failing predicate in `e251` or explain the historical negatives.
The three-path correction is frozen at `/private/tmp/n7-full-body-oracle-go4yutv1/handoff.json`, SHA-256 `a79273f0386841f2c9d2a33fa727f7d1a5a52feac1c513ac151d8c59ab984734`, source197 `f65900f9`, with Rust and shared helpers unchanged.
Parent's assigned independent xhigh review passed the bounded oracle scope: `/private/tmp/n7-oracle-independent-review-8tpy4e1y/receipt.json`, SHA-256 `5bea6bcf17f2e7913a33729627cb0a87296f2c31f5e806394df0a6f4716adcf9`, binds fresh four focused and 40 module tests, independent byte/phase/refusal controls and exact forward/inverse preservation.
Root verified both receipt hashes and released the review window; the assigned medium executor `01a1b9f2-08d7-7b63-9ae8-12f41ad9f913` initially received only the `n7-oracle-a792-linux-prep` preparation window.
The executor sealed [PREP `f737a325`](../target/verify/n7-oracle-linux-prep-9ji6h61t/prep-packet.json), binding source197 `f65900f9`, runtime219 `4728412c`, the reused Linux debug `a4caf7cc` and its original debug SQLite lineage.
Root's [bounded acceptance `04418f1e`](../target/verify/n7-oracle-linux-prep-9ji6h61t/root-prep-acceptance.json) verifies actual source/runtime/native files, compiler-input equality, both images' six expected reaped exit-101 children with two successful drivers, unchanged inventories and owned-resource absence.
Under Parent's explicit conditional authorization, Root issued the sole `n7-fullbody-f737-einstein` lease to executor `01a1b9f2-08d7-7b63-9ae8-12f41ad9f913` for three selected cases on PG16, then PG18 only after complete PG16 success.
The exact target-only synthetic-source/fake-utility scope retains 18 assertions and 20 native witnesses per version, atomic fresh preflight, 600-second child/900-second command/2100-second host bounds, fresh SAFE destinations, first-failure stop and explicit cleanup/release; a preparation PASS is not runtime qualification.
The [sealed negative `9d2c0374`](../target/verify/n7-fullbody-f737-live-lu0hxkhx/execution-receipt.json) records the first PG16 command exiting 101, no timeout or secondary error, no qualified proof or witness, and unknown admitted case counts; PG18 did not start and no retry ran.
Root's [negative acceptance `d88e540e`](../target/verify/n7-fullbody-f737-live-lu0hxkhx/root-negative-acceptance.json) binds 38 artifacts and 13 SAFE files to source `f65900f9`, runtime `4728412c`, Linux debug binary `a4caf7cc` and native20, and independently confirms owned-resource absence and lease release.
The 71 structured records end at callback entry without a terminal result or typed error; the fixed `SQL_ORACLE_REFUSED` label does not identify the first Python predicate or explain earlier failures.
All new Native7 PG leases are held; Parent assigned xhigh auditor `01a1ba00-4768-7c83-81c2-65b2e77944c5` to source/runtime contract analysis with bounded pure-Python checks after import-side-effect inspection, without refused raw data, source edits or PG access.
The [source-only audit receipt](/private/tmp/n7-callback-contract-9d2c-rptb0uyt/receipt.json), SHA-256 `a2746eb26864ac2839107750971348ec4b93d13b820477f33d598923ffda802c`, found no reproduced sufficient contract bug or runtime cause on the exact source/runtime/binary bindings.
Fourteen producer/oracle byte comparisons, 32 synthetic instances of each request/native/facts/response contract and four refusal controls passed; host Python 3.15.2 checks do not establish Linux Python 3.13.5 runtime behavior.
Root verified the receipt and findings hashes and recorded the audit window released with zero owned processes; no callback, PG run, refused-data read or source edit occurred.
Parent approved Sartre's exact three-path private Python diagnostic refinement: fixture, its tests and documentation, with no shared helper, Rust, framework or validation changes.
Selected guard codes must cover the actual coarse `SQL_ORACLE` helper's statements, using trusted code-object identity and exact statement spans while preserving the existing boundary, fallback and original exception.
Root granted `n7-python-diagnostic-sartre-offline` for serialized host pure-Python commands of at most 60 seconds, private scratch and explicit child cleanup; branch, compound, loop, wrong-code-object and no-leak controls are required.
Actual Linux Python 3.13 mapping remains a future PREP gate; host Python 3.15 evidence cannot replace it.
All N7 PG remains held; the first failing runtime predicate and historical causes remain unknown, and the 14/32 earlier pure checks remain limited host evidence.
Sartre's author packet `/private/tmp/n7-predicate-diagnostic-5z77f6ou/receipt.json`, SHA-256 `4b50cd4c5727ed60208c944d1e228d548e4cc223e7318494d9f30ada89b5e8a5`, is READY on source197 `7919a8be` with the exact fixture/test/docs delta and patch `5c09062e`.
Root verified the receipt hash; the author reports 41 passing host methods, retained nine-method RED with 69 assertion failures, 39 selected sites across 22 frames, 194 unchanged files including 88 Rust files, and the 197-file forward/inverse round-trip.
The offline window is released with zero active children reported; independent review and actual Linux 3.13 pure mapping remain required, with no runtime or PG clearance.
Parent assigned xhigh reviewer `01a1ba46-aa2b-7411-809c-5346b1ac6e9f` to the exact three-path predicate diagnostic packet `4b50cd4c`, source197 `7919a8be`.
Root granted `n7-predicate-4b50-independent-offline` for serialized pure host Python commands, tightened to the reviewer's 55-second foreground bound, with private scratch/copy and frozen-donor preservation, covering original exception identity, trusted code objects and spans, closed codes/fallback, helper fine sites, no leaks and semantic invariance.
That review completed as bounded PASS `b4233696`; Linux PREP `f0fe67a2` subsequently verified both images' actual Python 3.13.5 mappings, source frames, reused `a4caf7cc` binary/native bindings and six no-PG inner controls.
Root acceptance `5da2ef69` allowed one new attempt, now sealed as `target/verify/n7-predicate-f0fe-live-p5fjkvca/execution-receipt.json`, SHA-256 `56583778c2eb8718361a53f96f0a7653579085a35b8afd6c6ef122d12d527986`.
The first PG16 command exited 101 without timeout or secondary error; PG18 did not run, admitted proof counts remain null, and the fixed metadata-identity refusal does not identify a case or conjunct.
Root negative acceptance `df3f9696` verifies the retained bindings and owned-resource absence; that lease is released and all further Native7 PG is held.
Source-only audit `13e7e74a` proves that five raw OID JSON fields conflict with the strict integer contract; it does not establish the observed runtime cause.
Parent authorized only the three-path SQL projection/test/docs correction, frozen as `/private/tmp/n7-meta-projection-fix-Wcdysxow/receipt.json`, SHA-256 `332b9cda8d0b3806c02e4e70e5fa2f73c5753b079caae744adbd61da28403c1c`, source197 `e5d52f23`.
Its author reports 46 host tests, a five-method/68-assertion RED on the old projection, unchanged strict validators and 194 unchanged files including 88 Rust files; these are pure model checks, not SQL execution.
Parent assigned independent xhigh reviewer `01a1ba74-ac56-7d53-9607-2c822cac7f13`; Root granted serialized pure host commands of at most 55 seconds with owned child cleanup and no PG, network, native build, container or primary mutation.
Independent review `/private/tmp/n7-oid-independent-review-rr5viygc/receipt.json`, SHA-256 `47a2993c7c3a93664f846039de19c659127f3fc8dec3738d2f9433c2f51e3bbb`, requests changes: a column named `m` shadows bare `to_jsonb(m)` and can hide unknown fields and incorrect row identity from the unchanged validator.
Its source-backed controls do not establish a runtime cause; the attempted AF_UNIX test was blocked by the pure-only window and two subprocess tests were excluded.
Parent authorized the same author to change only the whole-row spelling to `to_jsonb(m.*)`, retain the five casts and strict guards, and add malicious-column/NULL/unknown-field controls with pinned PostgreSQL 16/18 evidence.
Root granted `n7-oid-wholerow-aristotle-offline` for the new private three-path correction; the old author packet and failed review stay immutable, and the assigned reviewer will recheck only after the new freeze.
The revised packet `/private/tmp/n7-oid-wholerow-fix-8kW5Fsio/receipt.json`, SHA-256 `369b5bf263793828fd6c8a985ab82eb95e64a15247258042e395e7243f2b8631`, is frozen on source197 `42797d2b` with the same three-path scope.
The author reports 49 passing pure methods and eight failing controls against the old spelling, with unchanged strict validators, 194 other files and 88 Rust files; these are source-backed model results, not SQL execution or runtime attribution.
Root verified the receipt hash and zero matching owned processes, then released the author window.
Parent resumed the same independent reviewer for the bounded `47a2993c` finding; `n7-oid-369b-independent-offline` permits serialized pure host commands of at most 55 seconds and reaped no-DB subprocess controls, without sockets, PG, network or builds.
That rereview records `PASS_BOUNDED_OID_PROJECTION` in `/private/tmp/n7-wholerow-rereview-d8m0mqbc/receipt.json`, SHA-256 `cdf02a1bf3af1897f78b8b4c89e67d26fe5666924081a9bfa0fd798464ea25c2`, resolving the prior shadowing finding for `369b5bf2` / `42797d2b`.
It reports 49 focused and 54 safe pure tests, 392 independent assertions, preserved strict validators and the 197-file round-trip; three IPC/process methods were excluded, and SQL execution and actual Linux 3.13 preparation remain unrun for this candidate.
Root verified the receipt hash and zero matching owned processes, then released the review window; the runtime cause remains unknown and no new PG lease is granted.
Parent accepted the bounded rereview and resumed Bohr `01a1ba4f-78ad-7543-b7e7-ac2a67f57dd7` for PREP only; Root granted `n7-wholerow-369b-linux-prep` with one owned container at a time and the existing resource limits.
Both actual Linux Python 3.13.5 images must run the complete module, including the three host-excluded IPC/process controls, mapping/independent controls and the canonical no-PG producer auxiliary.
Reuse of `a4caf7cc`, debug SQLite `1faff` and native20 requires exact compiler-input, flag and path equality; no build or native rebuild is authorized by this window.
The bounded rereview itself authorized neither PG nor a runtime-cause claim; the later preparation and explicit lease below are separate gates.
Bohr's actual PREP `be26838c`, source197 `42797d2b` and runtime219 `6e9f0d21`, passed both Linux 3.13.5 images' full 57-method module, including the three host-excluded controls, plus mapping/independent and compiled no-PG controls.
Root acceptance `target/verify/n7-wholerow-root-accept-_0u__hgs/acceptance.json`, SHA-256 `752b70da34670d150f068138a01edacc29a6030adb7f88a18479dff9e2beaed8`, verifies 197 source files, 219 runtime entries, 616 sealed artifacts, exactly three content changes and 197 write-bit removals with executable bits preserved.
The retained staging-permission and collector-mode-assumption failures preceded no repeated test run; the acceptance preserves their limits and the unchanged compiler/native/binary bindings without claiming a fresh SQLite archive hash.
Parent explicitly authorized one attempt, and Root granted Bohr `n7-wholerow-be268-bohr-01`, lease SHA-256 `96fc10606f3d3cbe71f3e9fc299c38962946646110fd56de640295d3db4333d2`, after independently checking all three preparation containers absent and zero running containers.
The executor reported START in fresh `target/verify/n7-wholerow-be268-live-wgpyuecq`, with exact three PG16 cases before PG18, 600/900/2100-second caps, atomic preflight, no retry, admitted evidence before cleanup and explicit release required.
The [sealed execution receipt](../target/verify/n7-wholerow-be268-live-wgpyuecq/execution-receipt.json), `f70c03c8`, records all six selected cases passing: fourteen fresh runs, ten rollback tails, two committed-reply losses, forty native witnesses and thirty-two callback phases.
Root's [independent result acceptance](../target/verify/n7-wholerow-be268-live-wgpyuecq/root-result-acceptance.json), SHA-256 `4633cfdfb804594e0160f60dfbe29835557f78e20fe3a5e25811ecd19a8b6f71`, verifies all 55 assembly files and 150 admitted SAFE files, source197/runtime219 bytes and modes, native20, binary `a4caf7cc`, both canonical capture validations and the unchanged eighteen-assertion profile per version.
The two commands took 4.751 and 4.725 seconds; both primary outcomes were zero, every child was reaped, and no timeout, capture error or secondary error occurred within the original lease clock and caps.
Root independently confirmed both container IDs absent, no owner resources and zero running containers, then accepted the explicit lease release.
This is selected Native7 component evidence only, with synthetic source identity and fake utilities; it does not qualify real utilities, real-source clone, CLI activation, Native8 or global strict lint.
Earlier negative receipts remain unchanged, and the passing run does not establish their historical causes.
This is target-only synthetic-source/fake-utility qualification; Root's Native8 metadata containers remain queued until this lease is released.
Ramanujan's Native6-local diagnostic remains a separate candidate and ownership scope.
Its synthetic source and fake utilities cover three tests with seven fresh runs, five rollbacks, and one committed-loss scenario per version; no real utility or full SourceClone qualification is claimed.
The author reports 382 serial Rust, 254 support, and 49 CI checks; the retained initial parallel PID-file timeout is not converted into a global parallel PASS.
The E1 writer-fence companion `3dd83d5c` has independent safety PASS `6bb2b482`: 31 plus three methods and 21 independent scenarios; its three live cases were initially compiled without execution, and the later six-case result below qualifies the corrected fixture candidate.
The private dedicated E1 three-case fixture/profile remains frozen as `e3a47d42`, source174 `feba6946`, with 217 affected Python checks and exact three-case discovery reported by the author.
Independent harness review `7b0e95` reported a P1: native readback accepts null, foreign identity, wrong versions, and missing COPY completion; it also reported unsafe root writes and lost timeout status as P2 findings.
These findings block Task/CI integration and live qualification until targeted corrections and independent review; the production fence review remains a separate result.
The corrective E1 packet `ecbc5aa5`, source174 `8071bf76`, reports 228 affected tests and 206 refusal probes, but Nash's second independent review `9fcce525` rejects it with four distinct gaps and eight failing expectations.
Nested run bytes accept floats or booleans, a later parse failure loses earlier raw evidence, discovery capture masks the primary failure, and startup cleanup masks the original exception.
Parent replanned the boundary into two HIGH-effort private units: Ampere `01a1b878-b3a7-71e2-a6b1-d843f24e6313` owns semantic identity validation and tests; Franklin `01a1b878-b4ee-7711-b3e9-cd3a1109cd7a` owns capture/discovery outcome preservation and `OwnedCluster.start`/`__enter__` with focused tests.
Their function scopes are disjoint; Root integrates minimal reviewed patches, without a parallel source fix.
Unit A is frozen as handoff `8a2da936`, source174 `711892a2`, with author evidence of 231 tests, the original 206 controls, and 64 added identity controls.
Root verified its exact two-file forward/inverse patch against base `8071bf76`: three validator lines and focused tests, with 172 manifest entries unchanged; Unit A was then tested in the composition with frozen Unit B `c8e5f5bb`.

- [x] Compose both E1 units, the canonical shared runner, and the reviewed Origin int8 correction, and obtain the fresh independent review disposition.
- [x] Correct the E1 and shared collector content-privacy findings, then independently review the corrected composition.
- [ ] Bind offline Linux CLI/libtest builds, exact three-case discovery, native-20, images, resources, deadlines, privacy, and cleanup to the final candidate; offline preparation may overlap independent review.
- [ ] Only after independent safety PASS, bounded PREP PASS, and an idle PG lease, use the parent's conditional authorization for three cases on PG16.15 followed by three on PG18.6; stop at the first failure, retain evidence before cleanup, and release explicitly.

The frozen E1 composition is handoff `cf8e7073`, source174 `9a7b566c`, in `target/verify/e1-e3-composition-wyakdmm1/e1`; Root's 237 Python checks passed with zero failures, errors, or skips.
Nash's final receipt `79ff91271bf86fc48f9193a00ac6336840f26d92d21fc411b8744937ae3d8300` repairs all prior findings but requests changes for two P1 content-privacy defects: E1 exports unadmitted native manifest bytes and parsed values, and the shared collector retains a rejected wrong-role connection marker.
The review reports 237 tests, ten canonical checks, 206 prior controls, and 91 expanded controls passing, but five of six privacy expectations fail; those passing counts do not clear the candidate.
Parent assigned Franklin only the E1 manifest and shared collector privacy fixes, with per-file content admission before export, earlier-safe retention, offending-byte omission, and code-only sanitized refusals.
His correction is frozen as handoff `b2b9127e`, source `f21efb6c`, with separate shared patch `4949500a` and E1 patch `0fd956cb`.
Nash's focused independent receipt `5f3908f800c0de333427286dce6fd33b64550c269f84d561c66e2e7cb5129ed6` passes the corrected offline harness scope: 240 tests, 206 prior and 91 expanded controls, six privacy cases, 32 bounded controls, and three additional real-runner cases preserve primary outcomes, safe prefixes, and both cleanup attempts.
The original exit-seven leak no longer appears in the checked exports; the canonical runner, reviewed int8 correction, and all 72 Rust files remain unchanged, but new Linux PREP must bind source `f21efb6c` before any E1 PG lease.
The canonical runner stays unchanged; Root makes no parallel semantic fix.
Hubble's old E1 preparation had no dispatched container, build, discovery, or PG command; its interrupted private copy must not be adopted.
Newton `01a1b8b2-7d44-7ef1-a4ac-0152a2d84e30` completed fresh offline E1 preparation from the [corrected brief](../target/verify/e1-e3-composition-wyakdmm1/e1-privacy-PREP.md), SHA-256 `6b33e52d997fb137610a10b5c912e45ac89dd5afb80e00f0893392d08c0b1669`, binding `b2b9127e`/`f21efb6c` and Nash's `5f3908f8` review.
Root accepted Newton's [new E1 PREP](../target/verify/e1-final-linux-prep-o3c51bow/prep-packet.json), SHA-256 `d8cb8624850343ce610ef7054f0be1ff261b6a0f3d4917c8eec039fd6959a6a1`, binding fresh CLI/libtest builds, exact three-case discovery, four inventory refusals, preserved Engine41/Authority18, native20, both images, reserves, and cleanup.
Under Parent's scheduling refinement, Root granted lease `e1-d8cb-newton` without waiting for the ungranted startup diagnostic.
The [E1 negative receipt](../target/verify/e1-d8cb-live-m15gnbvi/execution-receipt.json), SHA-256 `eb340e6d3df4ed98d34a4e25686fbea92a6e1c751f4d70b7a41a4ec9634c2468`, records exit one after 1.173331 seconds during the first PG16 fixture, with an AssertionError before any Rust child, zero cases/witnesses/states, and all later cases unrun.
Root matched 29 artifacts and three admitted lifecycle records; unknown stderr was omitted, the secondary fixture AssertionError remains separate from successful cluster cleanup, owned resources are absent, and the lease is released.
The failed predicate remains unknown; no retry or production fix follows from the receipt alone.
The separate fataltrace plan `26b5121a` passed bounded source, binary, image, privacy, and thirteen offline-control checks.
Its first atomic preparation stopped with ENOMEM while copying compiler artifacts, before any probe or PostgreSQL launch; [receipt `22b3e08f`](../target/verify/e1-startup-fataltrace-atomic-sq0plzu8/execution-receipt.json) records owned absence and release, not a candidate failure.
Parent approved staging only the 174 manifest files, two bound binaries, required native files, and four observer files under the unchanged resource caps.
Hubble completed and released `e1-fatal26b5-subset-hubble`; the [diagnostic receipt](../target/verify/e1-fataltrace-subset-k78wm36m/execution-receipt.json), `f88ac1f6`, binds the exact 201 staged entries, 297.865 seconds remaining, one 4.342-second probe, and 17.128 seconds total.
The safe observation `61b84e86` identifies an AssertionError at frozen `e1_fence_fixture.py:275`, the thirteen-field expected role/database/PRIMARY facts equality; it records no actual field values and does not identify which predicate differs.
Zero Rust cases and no PG18 work ran; there are no secondary errors, owned resources are absent, and the original negative receipts retain their historical attribution limits.
The bounded correction `ddeb75af`, source174 `fc687d12`, changes the shared database comment lookup to `pg_catalog.shobj_description` and adds a connected ownership regression; all thirteen predicates remain unchanged.
After Root released the E3 quiet window, Nash's independent receipt `2c044a175f0784c47fc455f5e67029ee1d2743e754496cca7f1cbf2b6a185114` passed 241 tests, six privacy, 31 role, 17 comment controls, and three callback modes in the offline harness scope.
The owned comment reaches the three fake children, missing or foreign comments reach none, and all 240 prior method bodies and 72 Rust files remain unchanged.
The offline review does not qualify real E1 cases; neither the static API defect nor the controlled test identifies field eleven as the sole cause of the historical failure.
Root accepted the [new E1 PREP](../target/verify/e1-shared-comment-prep-vquu9n53/prep-packet.json), `bbcdb62a`, as bounded receipt `58b35b79`: 77 artifact hashes, source174, 172 unchanged files, all 72 Rust files and eleven include inputs, reused binaries, native20, both Linux image/loader identities, exact inventories, and three owned PREP containers absent.
After Native6 released, Root granted Hubble `e1-comment-bbcd-hubble` under the parent's conditional authorization for exactly three cases on PG16.15 followed by three on PG18.6.
The lease required atomic fresh preflight, all thirteen fixture predicates, 9/5/5 native and 4/1/1 state observations per version, original outcomes, safe evidence before both cleanup attempts, and first-failure stop without retry.
The [sealed E1 execution](../target/verify/e1-bbcd-live-qo4mu0kw/execution-receipt.json), `eca07d46`, passes all six selected cases: three on PG16.15 in 2.942 seconds and three on PG18.6 in 2.630 seconds, with both command exits zero and no retries or secondary errors.
Root's [bounded acceptance](../target/verify/e1-bbcd-live-qo4mu0kw/root-acceptance.json), `59c519d8`, verifies 130 artifact hashes, both copies of all 174 source files, native20, both binaries, image/version/argument bindings, and frozen pure validators over all 38 native witnesses and twelve state captures.
Six fresh fixture owners and twelve native run IDs are distinct; both exact containers are independently absent, temporary roots and helpers are removed, and `e1-comment-bbcd-hubble` is released.
The harness validated evidence before fixture cleanup and retained admitted raw proofs and final lifecycle receipts before container removal; separate `before-cleanup.json` snapshots were not exported and are not reconstructed.
This is positive writer-fence evidence for the exact Native5 `fc687d12` candidate, not Native6 source-flow, general CLI, operator, or historical-failure qualification.
Herschel's frozen addendum `e91be69b` reuses the existing richer E1 helper unchanged, preserving PID/reap/group/cleanup facts, bounded streams, and original exception types; its author reports 14 distinct checks across 34 executions.
Root applied the exact canonical and startup hunks to the pending E3 composition; Native6's composed adapter also reuses the same helper rather than inventing process attributes or termination logic.
The E1 per-case native/state inventories are 9/5/5 and 4/1/1, with target role `e1_target` in database `e3_authority`; exactly two blank raw thread names in each of the first two cases must bind to the actual Rust child PID, endpoint, database, role, and phase without relaxing global Engine validation.
The dedicated profile receives the absolute libtest executable derived from Cargo's compiler-artifact output; Cargo builds separately and cannot stand in for the Rust child's PID.
New spawns follow the canonical [effort policy](../AGENTS.md#subagent-effort); active agents keep their assigned effort, with no inferred speedup or quality-equivalence claim.

### G3 binding acceptance checklist

- [x] Bind [V4 handoff `f05c53e8`](../target/verify/gate3-consumer-v4-z4bx10_d/handoff.json) to source manifest `6ce731d6` and review `d600030b`.
- [x] Verify the Origin `43853f51` to null-helper/backlog `0fdc4fb8`, consumer/wrapper `be95bf01`, protocol/endpoint `dea66fb4`, and V4 chain; V4 changes only two Python files, all 57 production Origin Rust hashes remain unchanged, and the executor freshly verified all 170 source files.
- [x] Resolve the donor runner reference through [exact-delta review `f7ca5e73`](../target/verify/independent-native5-runner-selectors-2cd3c7a095914cc49b13f92eef4c644c/runner-binding-review-receipt.json); the older `freeze.reviewed_runners` fields are historical references, not the actual later runner map `985d9e4e`.
- [x] Freeze the first private runner-evidence unit as handoff `08117a5f`, with 34 methods and 355 subtests; the 170-file V4 source remains immutable.
- [x] Resolve independent review `0d9e9dc` in the frozen V2 evidence boundary: safety review `56077e04` passed 63 methods and 404 subtests with zero failures, preserving child status and both owned cleanup attempts; no PG ran in that review.
- [x] Capture only allowlisted, bounded, owner-checked, no-follow raw proof before cleanup; validate per-case SQL/logical-source/target routes, PIDs, roles, and cardinality, fail closed, and complete owned cleanup without exposing secrets.
- [x] Obtain xhigh independent review of the frozen unit and negative controls, then assign medium executor `01a1b822-b082-7611-94c3-823e0918730d` to exact assembly, Linux build, and two-case discovery.
- [x] Accept [PREP packet `a06066d5`](../target/verify/g3-linux-v2-assembly-njaynjwi/prep-packet.json): exact source/native/image/runner/binary bindings, one successful CLI build and libtest compilation, exact two-case discovery, held-FD reserves, finite timeouts, and both build containers removed; Parent explicitly acknowledged this packet.
- [x] Accept new [PREP `b53c992c`](../target/verify/g3-int8-linux-prep-b3_i2l_7/prep-packet.json): source170 `fc27662a`, fresh CLI/libtest, unchanged native-20/overlay, the fixed source-pin entrypoint, seven offline controls, and exact two G3 plus 12 SourceSnapshot discovery counts with no PG execution during preparation.
- [x] Check explicit G3 SQL/logical-source/target route requirements; generic nonempty capture does not establish those route bindings.
- [x] Bind the inherited Linux native-20 artifact `d6ab3015` and exact arm64 image IDs from the [donor freeze](../target/verify/pg1618-native5-nullfix-ky3bmfc7/freeze.json), override stale image defaults with declared `16.15/160015` and `18.6/180006`, and build/discover the exact candidate once in the accepted PREP.
- [x] Record actual CLI/libtest and loaded-native identities, exactly `segmented_copy_origin_next` and `sqlite_copy_origin_next` per version, finite 30-second/64-MiB WAL/256-MiB WAL-directory/8-MiB comparison limits, both 1-GiB free-space floors, one-second sampling, and owned cleanup before a live PASS claim.

The earlier [prep packet](../target/verify/gate3-linux-v4-prep-01a1b7dc/prep-packet.json) remains reference evidence; PREP establishes build and binding checks, while the corrected execution receipt below supplies the live result.
The earlier medium prep worker closed before writing a runner; the corrected private overlay `67445528` now has independent safety PASS `56077e04` with all 170 source files unchanged.
The reviewed `run-g3.py` entry is `7192b23b`, with evidence helper `9e24950b`; the donor's seven-profile runner must not be invoked unchanged.
After Root's bounded binding PASS, Parent granted executor `01a1b822-b082-7611-94c3-823e0918730d` the sole PG lease for two cases on 16.15 followed by two on 18.6, with 18 native witnesses per run and 600/900/2100-second child/command/host bounds.
The [sealed execution receipt](../target/verify/g3-linux-v2-assembly-njaynjwi/execution-receipt.json), SHA-256 `b2f22560c83b294e3d0c54561fefe47a424e5073ed3dad538d98770428081cc6`, records PG16.15 exit 101: zero passed, two failed, zero ignored, 0.27 seconds in libtest and 3.149861 seconds for the command.
Both cases fail at `snapshot_journal_live_tests.rs:2032` in `SourceOrigin::reconcile`, after `begin_origin` completes, with SQLSTATE `42704`.
Feynman's sealed read-only diagnosis `af5a5f7e` proves a static defect in the frozen `snapshot.rs:738` timeline query: `pg_catalog.bigint` does not name the canonical builtin `pg_catalog.int8`.
The exact historical failing statement remains unknown because server ERROR logs are absent; the diagnosis bounds the attribution without reconstructing those logs or rerunning PostgreSQL.
PG18 did not start, and no retry or source change occurred; 20 safe raw files include only 16 of the required 18 native witnesses, so route and live qualification remain incomplete.
The unqualified schema2 receipt preserves the original exit and secondary `FileNotFoundError`, `FileNotFoundError`, and `AssertionError`; server ERROR logs were not retained before the owned roots and container were removed.
The lease is released: all three owned build/live containers, both cluster roots and sockets are absent, TMPDIR is empty, and no runtime or helper processes remain.
The proposed correction preserves the timeline widening, unsigned mask, comparison, and failure behavior; fixture privileges, native libraries, settings, and retry policy are outside that boundary.
The SourceOrigin correction is frozen as handoff `54118dbf` and receipt `e7f777ae`, with separate source170 `fc27662a` and source173 `910316a1` candidates.
Each three-file patch extracts the actual timeline query into a shared private helper, extends the existing owned-snapshot live case without new connections or case IDs, and updates one coverage sentence.
OID 20 describes the inner int8 widening; the final text result, unsigned mask, canonical u64 parser, stored NonZeroU32 comparison, and session guards remain unchanged.
The author reports scoped compilation, formatting, Clippy, and offline guard passes with existing Rust warnings; the live regression is compiled but unrun, and no SQL RED/GREEN or global strict result is claimed.
Feynman's independent static review `60b33c27` found no blocking issue in the frozen helper, caller, regression, and bindings; he read the author's execution evidence without rerunning tests, builds, or PG.
Parent released an additive correction for the identical invalid cast in source173 `snapshot/publication.rs::check_timeline`: change only the inner qualified type to int8, preserving final TEXT/OID25 and all existing guards, without another helper or decoder.
That additive packet is frozen as `05758dc8`, source173 `e814c2dc`, with one-token incremental patch `672aad56`; Root verified the bound patch changes only the inner type name and retains the final text cast.
The author reports 68 scoped tests, compilation, formatting, and scoped Clippy passes; independent delta review `78c6fe13` found no blocker and verified the exact one-token change without rerunning those checks or PG.
The original `54118dbf` package, static review `60b33c27`, and source170 `fc27662a` remain immutable; source173 `e814c2dc` still requires its own composed binary bindings and live qualification.
Root handed Parent a medium-effort PREP-only executor brief with the exact source, donor paths, image IDs, build commands, inventory, reserves, timeouts, and owned cleanup; it permits no PostgreSQL starts or native rebuild.
After Root's bounded new PREP PASS, Parent granted Hubble the sole corrected G3 lease for two cases on PG16.15 followed by two on PG18.6, stopping on the first failure without retry.
The [corrected execution receipt](../target/verify/g3-int8-linux-prep-b3_i2l_7/execution-receipt.json), SHA-256 `d42b801385c6ecbb1152801c2bdb6266c9cd6ca351783b72f1caae855df093d8`, records four passes, zero failures, and zero ignored cases on Linux arm64.
PG16.15 ran two cases in 3.832385 command seconds (0.39 libtest seconds); PG18.6 ran two in 7.804849 command seconds (0.52 libtest seconds).
Root's bounded receipt acceptance rehashed the final, per-run, raw and stream receipts, source manifest, native-20, actual binaries, unchanged overlay, and 48 retained safe proof files; both version/image identities and all 36 native witnesses matched.
Each case has four source SQL, two logical, and three target SQL observations bound to its actual child, roles, database, endpoints, owner, executable, and protocol version; both capture receipts have no primary or secondary errors.
Cleanup receipts record all three owned containers absent, cluster roots and sockets absent, TMPDIR empty, and no remaining owned runtime/helper processes; Hubble released the lease, and Root ran no PG, build, or cleanup during acceptance.
There were no retries, additional builds, SourceSnapshot12 executions, or other PG workloads; the original failed `b2f22560` receipt and its historical statement uncertainty remain unchanged.
This closes only the exact Native5 test_decoding graceful SourceOrigin component gate; the separate E1, Native6, and E3 gates above retain their own evidence and execution permissions.
Native7 and pgoutput source177 must later receive only the exact reviewed query hunk, without replacing whole source files; Root has made no source fix.
These G3 cases exercise graceful close/reconcile/replay; prior JournalSerial crash results do not become G3 SIGKILL evidence.
The source ACK cap remains the original consistent point, with full-prefix retention and no production pruning qualification; a keepalive's server WAL end is not source-consumption proof for future cutover.
The qualified Native5 `fae98fed` results, earlier failures, and unqualified private runtime remain separate.

### Next production runtime candidate: planning only

- [ ] Assemble one ordinary feature candidate from the reviewed Origin baseline, the V4 null-helper/backlog/wrapper and consumer hunks, decoder A, publication/context B, and the explicit Native6 fixture overlay; preserve each primary working change and resolve overlaps by path rather than replacing whole older exports.
- [ ] Apply A's six declared CDC paths and B's nine declared state/snapshot paths only after their review dispositions, with the shared CDC contract applied once.
- [ ] Keep Native5 Gate2/G3 evidence separate from the new Native6 candidate; the Native6 fixture owner must connect explicit version selection to the actual SourceSnapshot and Journal callers, including independent publication identity expectations.
- [ ] Integrate the clone primitive through `src/clone_phase.rs`, `src/clone_phase/process.rs`, and the root `src/lib.rs` declaration only after review; the endpoint correction has independent PASS `b37ad7db`, while the proposed same-session schema path and E1 companion still need their own implementation and gates.
- [ ] Make the next mutating runtime unit the existing `clone` command through `src/migration.rs`, `src/contracts.rs`, `src/config.rs`, and `src/lib.rs`, with command tests in `tests/cli_contract.py` and a focused `tests/clone_integration.rs` owned fixture.
- [ ] Before enabling clone success, close every phase admitted by its staged profile: source snapshot/publication lifecycle, safe pre-data restore, owning COPY, post-data/index/constraint restoration, sequences and large objects or explicit preflight refusal, durable phase reconciliation, and exact final verification.
- [ ] Refuse unsupported selected options before mutation, including unqualified resume/follow/default parallel or binary behavior; neither pre-data success nor the existing COPY-only receipt means clone completion.
- [ ] Bind utility availability in the runtime image and real PG16/18 CLI execution, then run the single candidate's strict lint/default tests and required live gates; no unused-item allowance, dummy caller, public export, or component-count substitution closes this gate.

The integration owner coordinates root registrations, Cargo inputs, Task/CI inventory, and maintained docs after the actual candidate is frozen.
The remaining coordinator release is a planning and dependency gap within the user's standing authority, not a request for new user permission.
Galileo completed the bounded coordinator-plan proposal from the accepted feature map, with dependency-ordered units and disjoint scopes.
The proposal binds actual interfaces and full admitted clone completion, including post-data, indexes, sequences, and large objects or explicit pre-mutation refusal; it permits no primary writes, builds, PostgreSQL, or new architecture.
Parent selected complete required phases before successful clone activation; parallel assembly alone is not milestone completion.
The shared pgoutput route may define the initial admitted qualification profile with explicit logical-slot, publication, and privilege prerequisites; slot-free and nonlogical clone-only compatibility remains an open requirement, with no test_decoding fallback or universal drop-in claim.
Parent accepted the ephemeral request shape `source`, `target`, `state`, `slot_name`, and `PluginOptions`, using existing Settings resolution and validation without new identity serialization.
Initial qualification requires explicit owned slot/publication names, fresh generation and epoch from existing rules, and qualified owned-resource cleanup before clone-only success; failures retain checked receipts.
Worker-one, text, and pgoutput restrictions belong to that qualification profile, not product defaults; omitted/default arguments and nonlogical clone-only compatibility remain open.
Freeze the request with the actual coordinator when full-phase interfaces are known; do not implement an unused admission shim or another Unsupported wrapper as a milestone.
Curie (`01a1b937-4299-7f20-80c2-955496d68b06`, XHIGH) owns the read-only post-data proposal during quiet, with no implementation or persistent-format choice before Parent review.
The first remaining-phase design reuses the archive retained inside `PredataSql` for post-data effects on the same authority fence; distinct persistent completion, proof, and format choices require Parent review before implementation.
Pre-data and COPY proofs cannot establish aggregate clone success, and unknown COMMIT cannot authorize COPY.
Root retains primary documentation and integration; backend choice, operator defaults, and PostgreSQL leases stay within their existing reviewed boundaries.
The eight-path contract still requires `list tables` to preserve the selected catalog used by comparison; printing catalog rows alone would not complete that operator path.

### E3 to E7: measured serial apply baseline

The [E3/E7 plan](../docs/superpowers/plans/2026-11-06-engine-cdc.md#e3-fenced-target-checkpoint-and-serial-apply) and [V3 measurements](../docs/superpowers/plans/2026-11-06-verification-performance.md#v3-storage-apply-and-migration-measurements) require real target effects before throughput optimization.

- [x] Assign one bounded benchmark unit on the immutable Native5 `test_decoding` baseline, separate from Native6/pgoutput: new `src/snapshot_apply_bench.rs`, its test-only module declaration in `src/snapshot_journal_live_tests.rs`, and a dedicated existing-harness profile in `tools/test-db.py` with `tests/support/apply_benchmark_fixture.py` and `tests/support/test_apply_benchmark.py`.
- [x] Reuse the real source slot/decoder, journal, guarded serial applier, owned clusters, and protocol evidence; introduce no production CLI route, new dependency, backend choice, or benchmark framework.
- [x] Freeze one-table workloads with one and 16 rows per source transaction, a fixed seed and payload, 32 warm-up transactions, and 256 measured transactions; select each already-admitted storage profile explicitly in a fresh child process.
- [x] Count target rows and transactions only after the matching positive synchronous COMMIT proof, then independently check exact data, checkpoint identity/order, and barrier increments; unknown outcomes, rollback, missing/extra work, zero samples, nonzero exit, or timeout invalidate the sample.
- [x] Report apply and full-path rows/transactions per second, raw latency samples and p50/p95/p99, source-COMMIT-acknowledgment-to-target-proof latency, child CPU and peak RSS, and phase timings including scan/decode/guards/checkpoint/COMMIT plus final close/reopen/verification.
- [x] Bind exact source/options/binaries/native/images/versions and record actual finite deadlines and their deviation; label the cache warm after writes, CPU/RSS as engine-process measurements, and configured byte budgets separately from measured RSS.
- [x] After offline controls and independent review, execute the approved PG16/18 workload with a 180-second child bound and three fresh-process repetitions; use measured repeatability to guide E7, without a speedup, backend-winner, target-server CPU, or full-migration claim.

The original author froze five paths as handoff `1fca014b`, source173 `5c9042cf`, with 164 scoped offline checks and no live samples in that packet.
The author's two standalone Codex subprocess reviews were HIGH-effort feedback outside the Parent allocation and do not clear the independent release gate.
Independent review `0e722381` reported P2 findings: the benchmark enters the existing G3 inventory, and failure evidence is lost before retention.
The original benchmark package remains immutable; it did not clear Task/CI registration or live measurement, which required the corrections and reviews below.
The corrective E3 packet `68d1375d`, source173 `344cbfd9`, reports 174 affected checks plus nine adapted review controls, with all Rust files unchanged and no live samples.
Turing's final review `2262ce9abb8ecc8329b2d2cc6cc6546d08dd3df6220dbc6d43d78b8ab3a00361` requests changes for three reproduced failures: substring `--skip` hides unexpected names, cleanup masks exit 9/timeout/validation failures, and timeout capture omits output collected during termination.
Parent replanned the correction as complete unfiltered discovery before exact registered exclusions, independent preservation of the primary result before both cleanup attempts, and retention of the final `communicate` output without duplication while preserving the original timeout and child PID.
Herschel's V4 packet `5743a344` reports 197 checks and all seven reproduced review probes corrected; the separate canonical addendum `e91be69b` reuses the richer E1 runner without a functional E1 change.
The intermediate E3 source173 `bd7c23ef` contains V4, the exact canonical runner and bounded-text test changes, Franklin's startup hunk, and the reviewed int8 correction; all 173 input files matched the manifest before handoff.
The actual benchmark branch calls `retain_benchmark_capture` and returns before registering the generic shared collector callback; inherited ordinary profiles still reach that shared callback.
Source inspection found that the benchmark's own collector exports raw proof files after filesystem checks without per-file content admission, so the shared fix alone cannot close this path.
Parent assigned Herschel the focused E3 fixture privacy correction in a new private copy; Franklin retains sole ownership of the E1/shared fix, and the canonical runner remains unchanged.
Herschel's content-privacy handoff `3e8d97c2` adds ten privacy methods and preserves the prior primary, cleanup, inventory, and termination assertions; its 196 author checks are scoped Python results, not the earlier mixed inventory.
The [final Root composition](../target/verify/e3-final-content-t671e_80/handoff.json), SHA-256 `d6d5441e6aed27afae2b2275ceb99a2f7c4e7fd8a5f80f8bea0199d576686ff2`, binds source173 `8b6e0cd5`, both content-admission fixes, and exact forward/inverse patches to V4.
Fresh composed checks passed 198 Python tests with zero failures or skips, all original test-method names remain, all 72 Rust files match the reviewed int8 input, and canonical runner `164c15f8` is unchanged.
The shared patch required only E3 context reconciliation and exclusion of a newly added E1Fence-only test parameter because that profile is absent from E3; all new Engine/CLI privacy faults remain, and the E1-specific subcases retain their independent `f21efb6c` attribution.
Turing's final independent receipt `5307b65e7c0b26e9baedeb114f41f69c99b9c6377ea55ce518c1862a04e7bd68` passes the offline benchmark scope with 198 existing and seven independent checks, zero failures or skips, and the earlier inventory, cleanup, timeout, and privacy findings closed.
Newton completed release-mode Linux PREP on the unchanged 173-file candidate, with locked offline dependencies, jobs two, and no libpq/native-20 or PostgreSQL toolchain rebuild; ordinary bundled SQLite compilation was authorized.
The first release compile and inventory passed, but a private helper wrongly required a redundant literal compiler flag despite Cargo's `debug_assertions=false` profile, and cleanup discarded the binary before export.
That incomplete PREP remains negative; the fresh [release PREP](../target/verify/e3-release-linux-prep-49ve1x2v/prep-packet.json), `b1fa2369`, retained its artifacts before cleanup and passed Root's bounded 75-artifact acceptance with source173, release binary `8c98441d`, SQLite archive `dd4b2dd1`, unchanged native20, both images, exact inventories, and five refusal controls.
Parent approved the concrete 24-run plan `c88845ad`: PG16 first, then PG18, two backends, rows one and sixteen, three repetitions, 768 warm-up and 6,144 measured transactions, with 170-second engine, 180-second child, 900-second command, and 11,400-second per-version host maxima.
After explicit quiet acknowledgements from Hegel, Hubble, and Maxwell, with Root's local builds complete, Root granted Newton the exclusive `e3-baseline-b1fa-newton` lease for that exact plan.
The [execution receipt](../target/verify/e3-b1fa-quiet-live-ly05ooqy/execution-receipt.json), `2c67aa69`, records twelve passing cases on each version, 768 warm-up and 6,144 measured transactions, 52,224 measured rows, and 168 native witnesses, with no retry, extra pilot, or secondary error.
Root's [bounded acceptance](../target/verify/e3-b1fa-quiet-live-ly05ooqy/root-acceptance.json), `ac3f3eba`, checks all 730 retained artifact hashes, source173, native20, the release binary and SQLite archive, both image/version identities, all 24 raw metric receipts, and 6,912 positive COMMIT/durable proofs including warm-up.
The exact eight configurations each have three repetitions; all 24 child/owner/nonce bindings match, both owned containers are independently absent, all retained roots/sockets/helpers are empty, and the exclusive lease is released.
The auxiliary evidence driver used 910 seconds around the frozen 900-second command; child and engine bounds stayed 180 and 170 seconds, and the longest observed driver took 16.100651 seconds without exercising timeout behavior.
Parent accepts the measured samples with this deviation recorded; the strict outermost 900-second timeout guard is not qualified by this run.
Newton's [statistics report](../target/verify/e3-statistics-7s38g0_9/REPORT.md), `bfc0c495`, and [JSON](../target/verify/e3-statistics-7s38g0_9/statistics.json), `537d8443`, report median/minimum/maximum across three runs per configuration, including per-run latency quantiles without pooling.
Root checked the report artifacts, source/receipt linkage, counts, and calculation definitions against its accepted raw proof; the author's second calculation produced identical JSON and Markdown, without a new PG run.
Parent accepted the analyst's bounded [first E7 unit](../docs/superpowers/plans/2026-11-06-engine-cdc.md#first-e7-unit-text-session-setup): existing bound prepare/execute plus independent readback, three dispatches and 26 fewer structural cycles across source and target per measured transaction.
Locke froze the settings refactor as handoff `1920c4e`, source174 `a5b3d9fa`, derived from baseline source173 `8b6e0cd5`.
Nash's independent offline review `f129e92d` passes eight focused, 543 retained, and thirteen overlay tests, including 21 independent scenarios and an old-helper 16-dispatch RED against the new three-dispatch control.
The same eight bound ASCII values, session-level settings, separate eight-column readback, and source/target guards remain intact; four ignored and 77 filtered retained cases are not qualified by this offline run.
Hubble owns offline Linux preparation with a real debug libtest for canonical Catalog10/SourceCatalog3 correctness capture and a separate release artifact for the same 24 benchmark cases.
No path alias, profile relabeling, or protocol-helper change substitutes for those distinct compiler bindings.
Root accepted [PREP `805a0a46`](../target/verify/e7-linux-prep-nz5rrsmm/prep-packet.json) as `57472b2d`, verifying 132 artifact hashes, both 174-file source copies, the 199-entry runtime subset, both actual profiles, native20, images, discovery, limits, and six absent owned containers.
The `e7-functional-805a-hubble` lease covered Catalog10 plus SourceCatalog3 on PG16.15, then PG18.6: four commands and 26 debug cases, with 600-second children, 900-second commands, 2100 seconds per version, and 300-second cleanup reserves.
Root accepted [functional receipt `93f724c1`](../target/verify/e7-functional-805a-tk0fg4mc/execution-receipt.json) as [bounded PASS `89382a3e`](../target/verify/e7-functional-805a-tk0fg4mc/root-acceptance.json): all 26 cases, 46 native witnesses, 155 artifact hashes, and the four retained protocol captures pass their exact bindings.
Both owned containers are independently absent and the functional lease is released.
Root accepted the [minimal benchmark binding](../target/verify/e7-benchmark-binding-9ydeab6s/binding-packet.json), `052320a5`, as `dcb8d55d`: 209 artifact hashes, the exact source-count/private-path driver changes, release `3d13b00d`, native20, SQLite, both images, and all 24 baseline arguments, order, and workload dimensions match.
Under Parent's conditional authorization, Root granted Hubble the unique [exclusive quiet lease](../target/verify/e7-benchmark-binding-9ydeab6s/quiet-lease.json), `e7-benchmark-B-052320-hubble`, for twelve cases on PG16.15 followed by twelve on PG18.6 only after PASS.
The lease held other local PG, builds, heavy tests, and copies; only design work and remote CI observation continued.
Every case requires fresh identity/resource checks and at least 900 seconds plus 600 seconds of cleanup reserve; the frozen 910-second auxiliary wrapper remains disclosed, and first actual failure stops the run without retry.
Hubble reported all 24 candidate-B cases passing with 168 native witnesses, 768 warm-up and 6,144 measured transactions, no secondary error, and no retry or baseline rerun.
Root independently verified both exact containers absent and retained empty process/root/socket checks, then [released quiet](../target/verify/e7-benchmark-B-m0or5m6i/root-quiet-release.json) as `edd55044`; the owner's [explicit release](../target/verify/e7-benchmark-B-m0or5m6i/lease-release.json) is `144a19a2`.
Root accepted [sealed candidate-B receipt `a47673f3`](../target/verify/e7-benchmark-B-m0or5m6i/execution-receipt.json) as [bounded PASS `bc17f9b4`](../target/verify/e7-benchmark-B-m0or5m6i/root-acceptance.json): all 534 artifact hashes, 24 frozen request/output/metric/protocol validations, source174, release binary, SQLite, native20, images, and workload bindings pass.
The eight configurations each have three repetitions, with 6,912 positive COMMIT/durable proofs including warm-up and 24 distinct fixture-owner/nonce bindings; the baseline and earlier failures remain unchanged.
Parent resumed Newton at MEDIUM for the same accepted aggregation and explicit A/B ranges, latency, and resource-scope comparison; Root does not duplicate that calculation.
Newton's [A/B report](../target/verify/e7-ab-statistics-rp7fmd45/REPORT.md), SHA-256 `d3f20f3577da9d2af1d590b597faf66e5ec4c7983d6d0590433df76bbf1ae36a`, and [statistics JSON](../target/verify/e7-ab-statistics-rp7fmd45/statistics.json), `87373cf1`, preserve all eight configurations and three repetitions per side.
Root's [bounded output acceptance](../target/verify/e7-ab-statistics-rp7fmd45/root-acceptance.json), `80845934`, verifies the four output hashes, 48 numeric-input references against accepted captures, cardinality, metric labels, and all eight displayed pipeline rows without repeating the aggregation or the 730/534-artifact audits.
Two direct raw-timestamp median checks reproduce PG16 segmented one-row throughput from 70.7269245 to 125.8370755 TX/s, up 77.9196202%, and PG18 segmented sixteen-row throughput from 57.9211176 to 74.5142398 TX/s, up 28.6477937%.
Across the eight configurations, reported pipeline median increases range from 28.6478% to 77.9196%; inclusive apply-service increases range from 28.3950% to 80.0765%, with higher medians in eight of eight configurations and no overlap in the observed three-run ranges.
These separate-window observations are descriptive, without a causal claim, formal confidence interval, universal gain, or categorical storage-backend ranking.
All guards, checkpoint/COMMIT ordering, durability, close/error paths, and source readiness remain required; new transport, caches, interpolated values, options, formats, and dependencies are outside this unit.
Functional PASS and measured throughput remain separate evidence; neither supplies a global strict-lint claim.
CPU covers the engine client only; RSS is its lifetime high-water mark including setup and the inherited floor, not target-server usage or a measured allocation budget.
The measured scope is Native5 test_decoding, cfg(test) release mode, Linux arm64, and local source/target clusters, without a pgoutput, production CLI, backend-winner, or pgcopydb comparison claim.
Offline Linux preparation may overlap review for a frozen candidate, but performance execution requires Root's unique lease under Parent's conditional authorization after safety review, exact PREP binding, and the concrete measurement plan, with no other PG scope or build load.
Later E1/E3 composition must use the exact reviewed query fix and a new source binding rather than inherit the old `6ce731d6` pin.
Any future benchmark run needs a new explicit quiet lease; receipt analysis does not hold the machine idle after measurement and cleanup finish.
Existing whole-history validation cost remains in the measured serial baseline; do not remove it to improve the number.

## Documentation bootstrap

- [x] Inspect the target and inherited instructions.
- [x] Initialize an ordinary repository on `feat/bootstrap` without remotes.
- [x] Create the initial [engine plan](../docs/superpowers/plans/2026-11-06-engine.md).
- [x] Record the [approved design](../docs/superpowers/specs/2026-11-06-rpgmigrate-design.md) and review resolutions.
- [x] Fix E0/E1 shared interfaces and first-milestone task details for leader review.
- [x] Create the [CDC plan](../docs/superpowers/plans/2026-11-06-engine-cdc.md).
- [x] Create the [verification/performance plan](../docs/superpowers/plans/2026-11-06-verification-performance.md) and [operator plan](../docs/superpowers/plans/2026-11-06-operator-adapter.md).
- [x] Record user corrections in `tasks/lessons.md`.
- [x] Self-review plan coverage, types, tests, and size.
- [x] Check document links, Markdown structure, and STE100 hard rules.
- [x] Obtain the leader's E0/E1 plan review and incorporate accepted review decisions.

## Implementation and release

- [x] E0: Contracts, build checks, private package metadata, notices, and baseline fixtures.
- [x] E1: Real libpq connection/ping, bounded raw COPY, and native-state safety on the recorded fixtures.
- [ ] E2: CDC decoding, packed journal, complete descriptors, and persistence measurements.
- [ ] E3: Logged target checkpoints, serial apply, recovery, and apply benchmarks.
- [ ] E4: Durability profiles, run-wide risk caps, and crash verification.
- [ ] E5: Implement the eight operator routes, their 43 behavioral option names, and approved general controls.
- [ ] E6: Implement the selected CRD semantics, including full clone phases, filters, large objects, binary fallback, parallel/split COPY, and all-databases behavior.
- [ ] E7: Qualified batching and parallel apply.
- [ ] O1-O4: Native operator adapter and isolated operator tests.
- [ ] V1-V6: Compatibility, faults, performance, version matrices, and release checks.
- [ ] Verify the selected operator profile against the maintained matrix; broader CLI parity remains deferred.
- [ ] Verify each platform's power-loss barrier before journal durability claims, including macOS `fsync` versus `F_FULLFSYNC`.
- [ ] Match SQLite `fullfsync` and `checkpoint_fullfsync` settings to candidate barriers in persistence comparisons.
- [ ] Test the declared minimum Rust toolchain in CI before expanding compiler support claims.
- [ ] Add namespace identity and DDL coordination before removing E1's DDL-quiescence requirement.
- [x] Build the same controlled native recipe in local Linux arm64 Podman and verify loaded-image guards, rpath, and authentication features.

## Executable command scope

The frozen Native5 source `fae98fed` registers 50 leaf commands, including `help` and `version`, leaving 48 behavioral commands in the [command tree](../target/verify/pg1618-native5-nullfix-ky3bmfc7/input/source/src/cli.rs#L255).
Root verified that the registration, admission, dispatch, and exit-mapping blocks are identical in the frozen SourceOrigin `43853f51` source.
The [admission code](../target/verify/pg1618-native5-nullfix-ky3bmfc7/input/source/src/config.rs#L373) and [runtime dispatch](../target/verify/pg1618-native5-nullfix-ky3bmfc7/input/source/src/lib.rs#L54) implement these behavioral routes:

- `ping` checks both explicit database connections.
- `copy table-data` performs restricted serial text COPY into compatible, precreated empty tables, uses synchronous target COMMIT, and records native completion only after confirmed commit.

The COPY boundary is enforced by the [catalog preflight](../target/verify/pg1618-native5-nullfix-ky3bmfc7/input/source/src/catalog.rs#L1197) and [COPY coordinator](../target/verify/pg1618-native5-nullfix-ky3bmfc7/input/source/src/copy.rs#L14).
After argument validation, the remaining 46 behavioral routes return `Invocation::Unsupported`; dispatch returns `BadConfig`, which maps to [exit code 2](../target/verify/pg1618-native5-nullfix-ky3bmfc7/input/source/src/contracts.rs#L47), before database or native-state operations.

| Unsupported routes | Count |
| --- | ---: |
| `clone`, `fork`, `follow` | 3 |
| `snapshot` | 1 |
| `dump` | 2 |
| `restore` | 5 |
| `list` | 10 |
| `compare` | 2 |
| `stream`, including sentinel subcommands | 14 |
| Other `copy` subcommands | 9 |

There are no successful placeholder routes; parser compatibility and private engine qualification do not establish command implementation or flag semantics.
These counts describe registered commands and dispatch behavior, not a fraction of total implementation effort or full compatibility.
The approved E0/E1 slice remains restricted COPY; the accepted operator matrix now defines the release command and feature contract.
All eight selected operator routes remain runtime refusals.
The shared `Parser::Copy` accepts `--follow` syntactically, but `Settings::invocation` rejects it; parsing does not supply combined COPY/WAL behavior.
The operator launches `clone --follow`, not standalone `follow`.
Full clone requires pre-data and post-data schema, indexes and constraints, sequences, requested roles, large objects, and qualified data transfer; E1 precreated-table COPY is insufficient.
The source storage backend remains unselected, and the separate Gate 3 consumer scope is unchanged.

## Evidence and review

The bootstrap inspection found an empty target directory.
Git initialization reported `feat/bootstrap` and no remotes.
Context documentation and environment readiness are complete.
The integration worker owns package/root contracts, root modules, command integration, README, and this task status after the documentation handoff.
Transport, CLI, and native-state implementations are handed off, with their focused review evidence below.
Harness integration covers the native version-two and target-authority slice.
State worker `01a1b228-5b80-7b83-9a6d-d32d77098814` completed its assigned module.
COPY/catalog worker `01a1b22d-2a70-7321-bd8d-ca3a33441f56` completed its reviewed handoff.
The tooling milestone assigns the workflow and `tools/ci/` to its CI worker; the integration worker owns the active guides.
The canonical E2 codec and decoder work has separate owners and stays outside the tooling snapshot.
The root entry point routes help, native version, and real ping.
The root COPY route calls native state, connection, planning, and COPY interfaces.
The COPY coordinator passed the recorded 40-case executable gate, including native version-two receipts.
The bounded E1 profile passed its recorded correctness gates; full migration compatibility and performance remain unverified.
The publication section records the public repository and exact GitHub run evidence.
The leader approved a Linux fixture profile with explicit expected PostgreSQL `17.11` and numeric version `170011`.
The native fixture default remains PostgreSQL `14.24`.
Expected versions must come from the pinned profile and be checked against the server, never derived from the server under test.
The E0/E1 review includes exact interfaces, Makefile ownership, late COPY results, unknown COMMIT, and isolated fixture inventories.
The user license answer is pending.
Keep `publish = false`, preserve third-party notices, and omit a new project license until that answer arrives.

## Verification evidence reported to this worker

The independent reviewer passed the E0/E1 plan at SHA-256 `d897788329937a3dee45beafc110c52a5f3c9701daf7bbf4162da6d7bce03e10`.
That pass covers the plan handoff only, not runtime results.
The leader then specified the real harness invocation and temporary 300-second/8 MiB slice limits.
These clarifications do not change the frozen function signatures.

The dependency worker passed the macOS graph audit for 19 locked packages.
It checked checksums, non-yanked crates.io releases, licenses, and minimum Rust versions.
RustSec snapshot `309392b18f6b17022f82c701f300433126901068` had zero matching advisories.
This was not a `cargo-audit` run or a native-library/Windows vulnerability audit.
Host metadata/tree checks passed.
The initial unfiltered offline metadata check needed an uncached Windows `vcpkg` package.
The later Cargo fetch cached that package while resolving the approved experiment dependency.
The worker reported pkg-config linkage for `pq-sys` and dynamic libpq version `180006`.
Preserve the required Unicode-3.0 notice.
The experiment-only `crc32fast 1.5.4` dev dependency adds `cfg-if 1.0.5`, bringing the lockfile to 21 registry packages.
Cargo metadata confirms MIT/Apache licensing and Rust floors of 1.63 and 1.32, respectively.
Reviewer `01a1b248-5643-7dc3-8e2d-7807cbbdafcc` passed the refreshed lockfile delta audit.
The original 19-package lockfile SHA-256 is `4366471a2a3297eed57be8147810c41b043f3956e818e992d14bceef86b037a9`.
Removing only the two new package blocks and root dev-dependency edge in memory reproduced that original hash exactly.
Both new crates match official non-yanked metadata, lockfile and archive checksums, MIT/Apache licensing, and the complete RustSec snapshot with zero package matches.
That 21-package audit excluded them from the normal/build graph, and full offline metadata succeeded.
The standalone `storage_format` target passed independent review and all 22 tests, including the reserved zero-sequence boundary.
It is part of the default test gate, with exact discovery and explicit execution of any ignored annotations.
The root integration check reproduced the absent default format gate and accepted empty discovery, then verified default execution and rejection of empty discovery after the change.
The real `make test-storage-format` passed all 22 cases with no ignored tests and verified the selected native artifact.
The integration worker then ran `make check lint test` successfully: 73 engine/unit, 22 format, 24 CLI, 20 native, and 10 harness checks.
Seven database-dependent Rust cases remain assigned to the separate live engine gate.

## Integration checks

- [x] Create one unpublished Cargo package and pin its 19-package dependency graph.
- [x] Observe five contract tests fail, then pass exit-category and conninfo validation/redaction tests.
- [x] Observe three root-control tests fail, then pass finite-deadline, JSON-diagnostic, and real-signal subprocess tests.
- [x] Verify native libpq linkage through the runtime version test and the executable's dynamic-library listing.
- [x] Run `make test-transport`: four exact live cases passed with no skips.
- [x] Run `make test-db-self-check`: PostgreSQL 14.24, 10,003 rows, full-content fingerprints, and three rejected corruption controls.
- [x] Replace the inline CLI placeholder with the assigned parser and native renderers.
- [x] Connect native state and raw COPY interfaces through the root entry point.
- [x] Verify the completed COPY coordinator with the real executable through the independent harness producer.
- [x] Verify real executable SIGINT and SIGTERM cancellation through the database harness.
- [x] Remove the temporary crate-wide dead-code allowance.
- [x] Run integrated format, lint, unit/CLI, and database checks on the controlled macOS artifact.
- [x] Write the README and getting-started guide after the first real COPY harness pass.
- [x] Obtain the leader's conditional foundation-commit authorization and complete its review gates.
- [x] Resolve the independent COPY findings and obtain final static review of the bounded target profile.
- [x] Review the experiment format and add its passing suite to the default test target.

The first 16 CLI tests passed after wiring, but manual `--version` verification exposed help output instead of version output.
The CLI worker added a failing exact-output regression test and corrected precedence.
Its final handoff reports 22 passing black-box tests, 50 leaf command checks, and parsing coverage for 220 long options and 188 short bindings.
The subsequent three CLI compatibility fixes and help-output routing passed independent review.
The updated CLI suite reports 24 passing tests.
The root unsafe-comment lint failure is corrected.
The harness worker owns the integration-test formatting correction.
Independent transport review found that libpq can write passfile warnings directly to native stderr before notice callbacks are installed.
The warning can expose a path and bypass structured diagnostics.
The leader approved a controlled libpq patch that removes the two raw passfile warnings while preserving resolution and security semantics.
The transport worker reports that the guard and regression tests now pass on the controlled macOS artifact.
Its 17 unit tests include three guard checks and a subprocess regression covering eight warning routes.
Five exact live cases passed, including SCRAM service/passfile tests covering 16 authentication outcomes.
A deliberately stock-linked probe returned exit code 2 and one JSON `BadConfig` diagnostic without a raw warning.
The transport reviewer signed off those changes.
Independent native-build review found a test-only compiler-command parsing mismatch.
The native owner corrected both call sites, and independent recheck passed all 20 cases with default `CC` and with `CC='clang -fno-common'`.
The builder, recipe, selected artifact, and native-library hashes were unchanged by that correction.
Stock libpq cannot silently substitute for the verified native artifact.
The transport guard must return a structured `BadConfig` error before connecting when the loaded library lacks the required capability.
The root endpoint-mapping regression failed when `BadConfig` became `Source`, then passed after limiting remapping to `Pgsql` errors.
The root build script also pins the selected native link directory and runtime search path.
An isolated stock-to-controlled library switch reproduced stale `pq-sys` cache selection before that fix.
After the fix, the same cache linked the controlled artifact and excluded the old system-libpq path.
This link-selection check used macOS `otool`.
The Linux worker reports two identical native builds with the capability marker and OpenSSL/GSSAPI/OAuth enabled.
Its stock-library negative control returned the exact structured exit-2 diagnostic with zero connection attempts.
Independent CI review passed the seven owned files and nine runner guard tests, including official pin checks.
The complete Linux arm64 report passed the final 40-case harness snapshot and the subsequent Python optimization guard.
The native worker completed two clean offline builds with identical hashes for all 19 installed files.
The artifact-set SHA-256 is `4683bca47f886340012470eeafb7aaf214a4b31bc666250da1fb5f1b2194f827`.
The recipe SHA-256 is `e49420f6d4db7f0c1552b75f9c4590231f57ba0178ab26e16f176c7013b63a03`.
Its final `make lint test` passed 73 Rust, 24 CLI, 20 native-builder, and eight harness unit checks.
That run excluded seven live database tests, which require their separate isolated gate.
These results predate the remaining COPY review fixes.
Do not persist raw warning contents or redirect process-global stderr as a workaround.
State verification exposed a lock-release race when a child or duplicate descriptor retained the locked file description.
The state worker added a regression test and explicit native unlocking, with 32 state tests passing.
COPY review identified concurrent namespace DDL as separate from table-lock coverage.
The leader requires schema and namespace DDL quiescence in both databases throughout E1 COPY.
Source DML remains permitted under the snapshot, while target DML waits for retained table locks.
Native state requires exclusive control of the active private `0700` directory.
Arbitrary noncooperating same-user mutation is outside its guarantees because validation and rename are not a compare-and-swap operation.
Independent COPY review reported two successful executions with incorrect target data on disposable PostgreSQL 14.24.
A CHECK function changed client encoding before another table's COPY, causing altered text despite a completion receipt.
Another CHECK function deleted previously copied rows while the receipt still reported all copied rows.
The reviewer also reproduced a valid deferrable foreign key failing when lexical COPY order loaded the child first.
The author implemented the approved target profile; the final expanded live evidence and independent review appear below.
E1 refuses target CHECK/exclusion constraints, expression/partial indexes, and custom table/index access methods or operator classes.
The six built-in index access methods require matching `pg_catalog` operator classes with OIDs below 16384.
Deferrable foreign keys require parents inside the COPY set, identical built-in column types, and built-in comparison operators.
The coordinator defers supported keys before loading and validates them before final durability checks.
The target must report `session_replication_role=origin` through `SHOW` without an added privilege requirement.
Target internal constraint triggers must also be enabled for origin or always execution (`tgenabled` is `O` or `A`).
Disabled and replica-only triggers can otherwise bypass the promised constraint validation.
The independent harness producer passed all 38 prior executable cases on PostgreSQL 14.24, including genuine late `23505` and `23503` errors with wire evidence, rollback, and no marker.
The author passed the exact seven-case live engine gate.
The author added the internal-trigger guard to initial and locked target inventory before emptiness checks.
After that change, the author reports 73 unit, 22 format, 24 CLI, 20 native, 10 harness, and seven live engine checks passing.
The independent harness producer passed the final 40 executable cases on PostgreSQL 14.24 with the controlled native artifact: one Rust integration entry, zero failures, zero ignored or filtered tests, and 25.61 seconds elapsed.
The two new internal-trigger cases use actual orphan input and require exit 2, zero COPY bytes, no marker, and unchanged target tables and trigger states.
The E1 foundation inventory was 40 executable cases and seven live engine cases.
Reviewer Planck passed the final static COPY review, and the author confirmed no source changes after the tested final hashes.
The fresh Linux arm64 report passed all 11 stages on PostgreSQL 17.11 with 95 Rust, 24 CLI, 20 native, 11 harness, nine CI guard, seven live engine, and 40 executable checks.
It also passed the three oracle controls, two wire-fault controls, SCRAM cases, and stock-libpq refusal with zero connection attempts.
The report is `target/ci-linux-GaZ46U/report.json`, with source SHA-256 `90add6f2436f37ad4243a77257aeb978d4c5f025b5f18dd8b73dc05d9327b95a`.
This is local arm64 evidence, not a GitHub workflow run or Linux amd64 qualification.
Independent harness review found that optimized Python could remove assert-based validators.
The shared import guard now refuses optimized execution before fixture creation; the author reports 11 harness tests passing with 12 subprocess probes.
That foundation snapshot retained 40 executable cases and seven live engine cases.
The Linux report includes the final optimization guard and all 11 harness regression tests.
The foundation commit excluded unregistered E2B driver/common files and retained a compilable reviewed manifest.
The generated CLI fixture includes upstream help prose, so root `NOTICE` now preserves the exact pgcopydb `LICENSE` from the pinned baseline.
The two E2B files are outside the Linux snapshot; the only subsequent reviewed snapshot changes are the notice addition and this guide's pointer to maintained platform evidence.
The reviewed foundation is committed as `4e7eeccb2da9ba236db1714c9f7b4801a1155189`.
An exact staged export formatted and compiled without the two unregistered E2B files.

## Bounded persistence experiment

- [x] Approve the common config, finite candidate selection, and closure-scoped batch interface.
- [x] Freeze the unregistered common interface in `bench/storage/config.rs` and notify the leader.
- [x] Implement bounded common validation, required-prefix recovery, and explicit checkpoint finalization.
- [x] Register the driver and recovery tests after the foundation commit and candidate handoffs.
- [x] Run every required nonempty workload against every delivered candidate.
- [x] Obtain independent review before accepting E2B results.

The first experiment uses one stream, one writer, one active source transaction, and synchronous storage batches.
Required recovery progress comes from an external prefix; zero denotes an empty required prefix.
Candidate completion is not production qualification or a storage-backend selection.
Multi-process access, retirement, quotas, and broader fault gates remain unchecked.
The common contract passed independent re-review after a callback-exposure finding.
Its first bounded scan pass validates all records and the captured frontier before the second pass emits completed transactions.
Nineteen focused common tests passed independently, including late-frame and descriptor corruption with zero callbacks.
SQLite passed independent review with 18 backend tests; journal passed with 15 backend and three focused common tests.
The redb reviewer reproduced a panic on clean-file corruption before integrity-on-open was enabled.
The corrected backend passed 19 tests and an independent valid control plus seven corruption probes that return structured errors.
Separate settings review passed seven focused tests and proved all preexisting backend lines unchanged.
SQLite reports queried values, journal nests that catalog evidence, and redb labels configured policies without claiming introspection or RSS limits.
The 31-package dependency delta passed independent checksums, licenses, advisory, features, and bundled SQLite source review.
The previous 21 package records were unchanged; the locked graph SHA-256 is `33778c3872ebac34b2ce9446067729ce9b45b03ce5d633314b0060810a6a9828`.
The reviewed driver passed 86 recovery tests: 19 common, 58 backend, and nine driver cases, with no ignored tests.
The committed experiment's default gate requires exact discovery of all 86 cases and all 22 format cases before execution.
The newer identity slice adds one format/JSON case.
`make lint` passed after the driver freeze.
The manifest requires three nonempty workloads, all three candidates, and three repetitions in fresh child processes.
The runner records storage throughput, checkpoint/reopen/two-pass scan timings, queried or configured settings, and per-child RSS.
Faraday passed independent driver review using an isolated Cargo target directory: nine driver tests, scoped Clippy, formatting, native verification, and three fresh-child checks passed.
The root worker's serialized `make check test` passed, including all 86 storage cases, and `make lint` passed.
That gate passed 76 runtime unit, 24 CLI, 20 native-builder, 12 harness, 22 format, and 86 storage checks; 14 live tests belong to their separate engine gate.
The release pilot returned exit zero with all 27 expected receipts from 27 distinct child PIDs on Darwin arm64 with Rust/Cargo 1.99.1, SQLite 3.54.1, and redb 4.5.0.
Every candidate used the same canonical input and storage-batch boundaries within each workload.

| Workload | Frames | Completed transactions | Encoded bytes | Storage batches |
| --- | --- | --- | --- | --- |
| Small commits | 512 | 128 | 126976 | 1 |
| Wide events | 48 | 4 | 33559168 | 5 |
| Giant transaction | 258 | 1 | 135289136 | 19 |

The raw report is `target/storage-pilot-20261106/report.json`, SHA-256 `7cd98d68dd17b370b2937d62c38c6ffe95a401e9cdd120af61d36abf258424c1`.
Its actual input manifest is `target/storage-pilot-20261106/cases.json`, SHA-256 `f1496e15bdf15ac7bb624c0725783be9195c420e4688dbc651e9509d6a9d5e55`.
`target/storage-pilot-evidence.json` records all 29 JSON file hashes, the executable hash, toolchain versions, and frozen input manifest.
The report contains seven measured phases plus total time, including encoding, checkpoints, reopen integrity work, and both verified-scan passes.
Observed giant-transaction child peak RSS ranges were 7274496 bytes for segmented storage, 77266944 to 77283328 for redb, and 94978048 to 94994432 for SQLite, with the same configured 67108864-byte cache budget.
These are three repetitions on one Mac with warm-after-write reads and do not select a backend or establish production memory or durability bounds.
The actual release executable rejected `LIBSQLITE3_SYS_USE_PKG_CONFIG=1` with structured exit one before creating an output directory.
Jason's [research note](../docs/research/2026-11-06-storage-pilot.md) and four audited JSON archives under `bench/results/2026-11-06-macos-arm64` passed Faraday's final read-only review.
All archived bytes match the originals, all ten input hashes match committed `905c323`, and all nine result rows were independently recomputed.
The note SHA-256 is `4050f4fd9dd57f420518e3f1321323a60f8203c1334899f424680a83ff0a7f41`.
The README now links to it.
The reviewed six-file documentation/archive set and README pointer are committed as `08a63ebb5ab908b8b3d9ce4e5b5344be9dccb019` directly on the qualified `905c323` source base.
Only those seven files were staged.
Review hashes, staged links, Markdown checks, JSON decoding, and whitespace checks passed without runtime reruns.

## Prepared transport and documentation

The private prepared-transport slice passed independent unit and seven-case live review.
The committed prepared snapshot's full engine inventory is 14 cases, preserving all seven E1 cases.
The transport-only inventory remains five and executable COPY remains 40.
Harness and CI guard review passed 12 and ten offline tests on Python 3.15 and 3.9, including 36 negative checks.
The prepared API remains unused by a production CDC consumer and does not enable runtime CDC commands.
Narrow expected-dead-code attributes must be removed when the checkpoint/applier consumer is integrated.
That prepared-transport milestone deferred canonical codec promotion until a consumer and its API were ready.
The initial [durability guide](../docs/durability.md) passed independent review at SHA-256 `b152b61874e55f7fd4214c4644aad8dbfa16642898f74b54200b5d03097cb526` and separates the approved design from runtime gates.
Its later barrier wording passed the same read-only review at SHA-256 `f533ba943ac2a165325419a697d2b4639d428af4dee511cf93b10123e618077c`.
A concurrent broad test run reported a transient missing `target/debug/rpgmigrate` executable in CLI checks.
The root worker's serialized gate passed after file freeze and did not reproduce that failure.
Its original cause remains unproven; no code fix was made on the assumption of a build race.
Prepared transport, its harness/CI inventories, and the durability guide are committed as `3725c8d`.
The exact prepared-only staged export passed `make lint` against a byte-identical copy of the verified native artifact and the foundation dependency graph.

## Canonical identity and E3 consumer integration

- [x] Close the reviewed prepared and storage commits: `3725c8d` and `905c323`.
- [x] Extract the seven reviewed ID/position definitions into private `src/identity.rs` without codec or runtime dependency changes.
- [x] Re-export canonical `RunId` through runtime contracts and preserve benchmark/test imports through the same source file.
- [x] Hand off exact `crate::identity` types to the state and authority authors.
- [x] Close independent state version-two and canonical identity review with 54 state and 23 format tests passing.
- [x] Close review of the metadata role-name correction and quoted/numeric writer regression.
- [x] Close independent CI receipt-guard review at the frozen three-file hashes.
- [x] Close independent harness review at the frozen ten-file hashes.
- [x] Close independent checkpoint review at its frozen hash.
- [x] Require the harness owner's final exact 30-case engine receipt after the metadata correction.
- [x] Resolve staging warnings through the real provenance/authority consumers and narrow expectations on pending stage interfaces only.
- [x] Integrate state-owned version-two provenance and the internal authority consumer after their independent handoffs.
- [x] Verify identity/format regressions and the integrated consumer gates before another commit.

The state worker owns native provenance types and operations, with no SQL or authority dependency.
Canonical `RunId`, `SourceId`, and `StreamId` derive serde for version-two state.
Named identity objects reject unknown fields.
The other four type definitions kept their reviewed fields and traits, and that identity-only snapshot left wire codec logic in the experiment.
The new identity JSON regression failed to compile before serialization support existed, then passed with all 23 format/identity and 86 recovery tests.
The extraction-only check reported six unused-type warnings.
Real consumers now use these types, and the integrated all-targets warning check passed.
No lint suppression or redundant construction was added to the identity file.
The authority's nonempty frontier carries the full `TxId`, `TxSeq`, commit LSN, and end LSN so equal positions and XID reuse cannot alias a descriptor.
An empty frontier uses sequence zero with no transaction identity.
Begin ordinal is not commit order.
The authority worker owns `src/apply/{mod,checkpoint,checkpoint_live_tests}.rs` and imports the state-owned provenance operations without a reverse dependency.
The metadata helper reuses named prepared statements per connection without adding a generic cache or transaction ownership.
Carver owns the role-name correction in `src/apply/metadata.rs` and its quoted/numeric writer regression inside the existing `missing_privileges` live case.
That narrow live-test handoff changes no inventory count or harness fixture.
Descartes coordinates the scoped regression and final combined run.
Its author reports nine focused tests passing at SHA-256 `c713fa39da7cbd4ddad161682ed9630768387ddb60166db61cf0180d0d7fa93d`.
Independent metadata review reran nine tests and found a role-name lookup defect.
Casting raw `CURRENT_USER` through `regrole` reparses quoted or numeric writer names instead of matching the catalog name.
The metadata owner replaced the two reparsing casts with the OID from an exact `pg_roles.rolname = CURRENT_USER` join.
The existing `missing_privileges` case reproduced both quoted-name and numeric-name failures, then passed both after that correction on PostgreSQL 14.24.
The root worker verified the one-case RED and GREEN logs and the new metadata/live-test hashes.
This scoped run does not close the 16-case or 30-case gates.
The corrected metadata hash is `46006ba2cf17418d7f1a1c0e2546359aeca658b6ec981cf175d16af5667f0b8e`, and the live-test hash is `de6fcc1324068fd4fbd1d5fef6a2a7bbeca18a998a2417e43a6a35abadf729e3`.
The owner reports nine metadata unit tests, formatting, and scoped Clippy with warnings denied passing.
The corrected two-file handoff is frozen.
Carson passed the corrected metadata and independently reran all nine metadata unit tests.
Reversing the three SQL changes reproduced the prior reviewed file hash exactly.
Mendel passed the checkpoint implementation without a finding and independently ran its four focused unit tests against the controlled libpq artifact.
He reviewed the local-error poison regression and held-COMMIT evidence without repeating live tests.
Metadata SQL remained a separate review.
Planck passed the frozen harness and closed the read-only review.
Faraday passed the 30-case CI/footer delta without a blocker.
Zeno, Goodall, and Carson have closed their author/reviewer handoffs.
The root worker retains broad-gate ownership after these remaining scopes freeze.
Restricted preprovisioned operation binds the actual object owner separately from the migration role and requires explicit schema/table grants without owner-role membership.
Its production transaction commit is synchronous.
Asynchronous execution is test-private and does not enable relaxed mode or source ACK policy.
The same logged checkpoint UPDATE increments its barrier counter, and positive synchronous COMMIT returns an opaque proof without a second COMMIT.
Standalone barriers use a real logged counter write to cover asynchronous or no-new-data flushes, with the same target and identity checks.
They also support an empty checkpoint through `Option<Frontier>`.
An empty proof covers zero CDC transactions, not a requested end position or COPY snapshot.
The transport owner adds only the checked transaction-status accessor needed by that authority.
The accessor passed independent review and two focused tests.
Its author has frozen the handoff.
The harness defines 16 authority case names under `apply::checkpoint::live_tests::` and requires all 30 engine cases, including the previous 14.
The root `test-authority` target uses that exact harness profile and namespace.
The default unit filter excludes only the separately gated live namespace.
The authority bodies, fixtures, and receipt guards passed independent review and the final integrated live gate.
The authority author reported a 16-case live pass including fsync-off, but the leader then found an uncovered local-error poison gap.
Successful DML followed by a caught local prepare/execute error could leave PostgreSQL in a committable transaction.
The authority owner changed only `checkpoint.rs` and `checkpoint_live_tests.rs` for a sticky failed-state fix and a regression inside the existing `commit_rollback_tag` case.
Explicit rollback remains allowed.
The inventory stays 16 authority and 30 combined engine cases.
The poison regression reproduced successful COMMIT after a caught local error, then passed after a sticky failed-state fix.
The existing case now covers five local prepare/execute error variants after prior DML and verifies unchanged data/checkpoint state.
Explicit rollback remains available.
The corrected author run passed all 16 cases on PostgreSQL 14.24/140024 with both held-COMMIT fault receipts, 13 apply/metadata unit tests, and scoped Clippy.
The revised checkpoint hash is `315a90c78db6a18a5be308cf705d538bc45a8f9fb79ba9c7503fecd824893596`.
That poison run used live-test hash `35f066fe98a8de470ac60adeee8587304603d5641baa868c7c3b33a6b8e5a09b`, before the later role-name regression.
All independent authority, metadata, harness, and CI reviews passed, followed by the serialized root check/lint/test gate.
The poison fix left metadata, state, and canonical identity hashes unchanged.
The state author handed off `src/state.rs` at SHA-256 `16f445d44c593cac570550df770c5ba107068e3eea142b3a8cc21923502f6081` with 54 tests passing and no ignored cases.
The state handoff includes strict persisted-data checks, a single-record/16 KiB cap, immutable provisioning identity, and publication errors before and after rename.
The state author's earlier scoped Clippy run encountered unfinished authority staging diagnostics.
The later integrated gate passed with warnings denied.
Planck passed the frozen state version-two implementation and canonical identity extraction review.
All 54 state and 23 format/identity tests passed with no failures or ignored cases in that independent run.
The reviewed state and identity hashes matched at the end of review, including unchanged codec golden bytes and strict identity JSON.
The active authority body was excluded from that focused review and subsequently passed its separate review and integrated gates.
State and identity owners will make no further edits without a review finding or a concrete integration need.
The root worker removed fulfilled staging expectations on PG prepared calls, transaction status, and their exports after actual authority call sites landed.
Only the binary parameter variant retains a narrow pending-serial-applier expectation.
PG logic and API bodies are unchanged.
The attribute-only comparison is recorded in `target/pg-expectation-cleanup-evidence.json`, and the final root lint gate verified the integrated uses.
The state owner then added one narrow expectation to the internal recovery entry point until the production resume consumer is wired.
Removing that attribute reproduces the reviewed state hash exactly.
The new state hash is `ed3e9ff4caaae7f94432358c65ccdaffe416a6b01d904d25b5ca78f45f7c769b`.
The owner reports scoped library Clippy with warnings denied and one intent/reload regression passing after that nonbehavioral change.
No helper, identity, or state behavior changed, and the complete 54-case suite was not repeated for the attribute alone.
The harness owner updated COPY receipt checks to require literal format version two and empty target provenance, preserving the 40-case inventory.
The new version oracle failed against the old guard before all 13 harness tests passed.
The first authority atomic test reached the intended unavailable-provisioning error.
This is failing implementation evidence, not a passing authority case.
The harness owner subsequently passed all 40 COPY cases against native version-two state on controlled PostgreSQL 14.24 using an isolated target directory.
That 25.40-second run required literal format version two and empty target provenance without changing the expected case inventory.
The harness owner also reports 14 fixture unit tests and authority self-checks passing for restricted ownership/grants, real standby, isolated fsync-off, and two held-COMMIT faults.
The fixture checks were followed by the final 30-case engine gate on the reviewed authority/metadata freeze.
The harness owner's earlier 30-case run preceded the local-error poison fix and is superseded for the final gate.
The frozen harness rejects missing, zero, ignored, or pending authority-fault receipts in both the authority-only and combined-engine profiles.
The owner reports all 14 offline guards passing on Python 3.15 and 3.9.
A real-psql self-check covered restricted ownership, standby, the fsync-off server, and both held/released COMMIT faults.
The root worker verified all ten frozen harness file hashes against `target/verify/harness-authority/harness-freeze.sha256`, SHA-256 `f6470f54aad9e68e018b4f6b4bebf9069f23c6925c45bf268eb0323d33049781`.
Planck independently passed all 14 offline harness tests on both Python versions and 24 inventory negatives, with all ten frozen files unchanged.
A 30-case run had started before the metadata finding arrived and passed in 6.00 seconds with both fault receipts.
It remains intermediate evidence until the role-name correction is covered.
That log is `target/verify/harness-authority/engine30-before-role-fix.log`, SHA-256 `9fa2edf095423c73f11178aab046ce4ff8658409224f18e4bd70a37ef6b89058`.
The leader released the harness owner as sole producer of the final 30-case gate after the corrected metadata freeze, in parallel with independent review.
The final run passed all 30 cases with zero failures or ignored cases in 8.13 seconds on PostgreSQL 14.24 and verified both held-COMMIT/fence-release receipts.
Its log is `target/verify/harness-authority/engine30-final.log`, SHA-256 `495fa37920c31a30cde1613fc652ce666710807e4b05708ff2877c6b08267445`.
The metadata, checkpoint, live-test, and ten harness hashes matched before and after that run.
The root worker verified the final log and matching frozen hashes before starting its gate.
The leader authorized serialized root lint/check/test, documentation and ledger review, and a conventional commit of the reviewed snapshot.
PG/apply source edits remained frozen until that commit.
A fresh Linux gate will use that exact commit.
Unrelated exports and local proof artifacts stay outside the commit.
The CI owner froze its 30-case parser and exact two-fault footer requirement after 12 offline tests and shell, YAML, workflow, syntax, and whitespace checks passed.
The tests reject the old 14-case result and missing, duplicate, wrong-count, unheld, pending, or ignored fault evidence.
Faraday independently passed all 12 offline CI tests, refused 27 negative receipts, and accepted six valid receipt orders.
The checked engine inventory is exactly 16 authority, seven prepared, and seven other cases.
COPY remains 40 and the Linux profile retains PostgreSQL 17.11.
The root worker matched the reviewed hashes: `verify.py` is `42f16c256d2b341c8f32d03afff2a7b27de2043821e09dcf4814cac24e014046`, `test_verify.py` is `1fbbd7c8d0656fafbc1bf80bb883f9925d8c0933723c401b7b14cf1f5dc388dc`, and `docs/testing.md` is `6888ab9981c682de5c0a24b492fc048a350c491b274cf375ef8720a48e9608db`.
No CI review blocker remains.
No Linux run was performed for the new slice.
The serialized root command was `env PATH="/opt/homebrew/bin:$PATH" make -j1 PQ_LIB_DIR="$PWD/target/native/libpq/lib" check lint test`.
Native artifact verification, all-targets Cargo check, formatting, and all-targets Clippy with warnings denied passed.
The default gate passed 222 Rust tests (113 unit, 23 format/identity, and 86 storage recovery/driver), plus 24 CLI, 20 native-build, and 14 harness tests.
The 30 live cases were excluded from the default unit command and passed separately in the exact engine profile.
No selected case was ignored.
The gate log is `target/verify/root-authority/check-lint-test.log`, SHA-256 `15d142b0f2137541c15464d40073ca059da54d68d94431028b70200dc6b00569`.
The existing native version-two COPY40 evidence remains applicable and was not repeated for authority-only changes.
Target system identifiers are optional for restricted roles and older PostgreSQL versions.
Persisted identifiers must be verified when present.
Receipt-only recovery still requires matching target evidence, and absence without strong binding fails closed.
No DSN persistence or new privilege requirement is introduced.
Public CLI resume remains unsupported until the complete authority path is integrated.
The Linux arm64 worker passed all 11 stages for an ordinary export of committed `905c323`, separately from this newer working slice.
The frozen run passed 184 Rust checks (76 unit, 22 format, 86 recovery/driver) on PostgreSQL 17.11/170011.
It also passed 24 CLI, 20 native, 12 harness, ten CI guard, 14 engine, and 40 COPY cases.
Three oracle negatives, two wire faults, SCRAM, and stock-libpq refusal with zero connections also passed.
The report is `target/ci-905c323-n59hsquu/report.json`, SHA-256 `4b0d82414097a73192c0b7a302162131f1b3b1cecd5bbc528eaf0868607fc7e8`, for source SHA-256 `f90665004d5589457b53864ff4f321ae3c175481abc655a3e9397ed3fcd191cc`.
The worker verified all 77 committed files and their modes before and after the run.
This verifies the archived commit, not the subsequent native-state/authority work, amd64, hosted CI, or Linux performance.

## Approved next execution sequence

- [x] Close the authority slice's independent reviews, final live gate, and serialized root checks.
- [x] Freeze the existing source CopyBoth transport proposal through the leader and implement it independently of storage selection.
- [ ] Add E2 complete-transaction readers, storage qualification, and real-slot `test_decoding` ingestion.
- [ ] Qualify multi-process access, reader pins, pruning, quotas, and recovery before choosing a production storage backend.
- [ ] Connect `ReadyTxn`/`TxReader` to E3 serial apply and verify actual target effects and recovery.

Beauvoir's focused read-only review covers source frontier semantics across plugins.
The primary-source review reports that validated `test_decoding` COMMIT envelopes provide a transaction-end LSN but no physical commit-record position on PostgreSQL 9.6 and 18.
It also records that baseline wal2json omits LSN fields by default, while pgoutput v1 provides both positions.
The next receiver contract keeps `CommitLsn` physical, represents its absence explicitly with `Option`, requires `SourceEndLsn` for complete COMMIT, and preserves the decoder's envelope context.
It must not fabricate a commit position from the end position or authorize keepalive ACK advancement.
That separate tested contract change must address codec versioning if required and metadata shape/CHECK/proof semantics before the first decoder.
It does not change this library-only authority snapshot.
The archived single-writer pilot does not replace full E2 qualification or select a production backend.
Newton's exact redb 4.5 three-process prerequisite probe passed on macOS with shared MultiWriter modes, live commit visibility, atomic checkpoint/deletion, and reader-pin retention across 16 commits.
Compaction remained blocked until pin release.
This result requires no IPC architecture change.
Read-only open still skips integrity checks, and role ownership, pruning authority, quotas, crash recovery, Linux, and power-loss qualification remain open.
The probe wrote only ignored artifacts and selects no backend.
This sequence stays within the approved architecture.
The leader freezes the transport API without a new user approval gate.
No WAL runtime or applied-throughput result is claimed by the authority slice or storage pilot.

## Source transport and optional commit positions

- [x] Commit the reviewed private authority slice as `eaf7517f3ef4e77144d81c9aac1d8b819833c851`.
- [x] Freeze and review the source CopyBoth implementation and its fixture contract.
- [x] Freeze and review the experiment codec's optional physical commit position and version-two wire format.
- [x] Freeze and review target authority and state metadata-version-two semantics for an optional physical commit position.
- [x] Close independent review of the state's metadata-version-two constant and refusal fixtures.
- [x] Close independent review of the source slice's CI inventories, fault receipts, and testing guide.
- [x] Integrate the frozen inventories and run the next serialized root gate after component reviews.
- [x] Verify the reviewed contract-only tree against stable `eaf7517` PG/harness code before a separate fix commit.

Herschel owns the source CopyBoth transport implementation.
The frozen source fixture requires exactly nine replication cases within the 39-case engine inventory.
Descartes owns the corresponding fixture and receipt integration.
Dewey owns the approved five-file codec extension, including the bounded constructor changes.
The codec author froze five files after 31 format and 87 recovery tests passed with no ignored cases.
The RED phase produced 15 format failures and one recovery failure before the codec change.
The root worker matched all five frozen hashes, including format `06a46ac128bd48482c31138aa40cf84deb10f74f2329046c97b1db6758439432`.
Owned formatting and whitespace checks passed, and the driver/redb edits change constructors only.
The first codec Clippy attempt encountered 16 known PG staging diagnostics before reaching the codec targets.
The separate contract-only export later passed strict all-targets Clippy against stable PG code.
The codec author added no suppressions or edits outside ownership.
Faraday independently passed all five codec files, 31 format tests, the new cross-candidate recovery case, and four Python golden checks.
The root Makefile now requires exactly 31 format and 87 recovery tests after that accepted review.
Zeno owns `Option<CommitLsn>` in the authority frontier and target metadata version two, without DDL or conversion.
The existing 19-column layout and 26-parameter comparison remain unchanged.
Zeno froze four authority files after 13 unit tests passed.
Mendel independently passed those 13 units and the optional-position semantics with no finding.
Zeno passed the exact 16-case authority run against the contract-only export with stable PG and harness files.
Meitner owns only `src/state.rs` and exposes `TARGET_METADATA_VERSION = 2` as the crate-private single source for authority imports.
Native manifest `FORMAT_VERSION` remains two.
Meitner froze the one-line production change and its literal-version fixtures at SHA-256 `121e7a80a4c7b83611e72c0b74935defa424b25c74160be85f1c732401a41d94`.
The explicit version-one refusal test failed before the constant changed, then all 55 state tests passed with no ignored cases.
The author also passed controlled-native verification, scoped library Clippy with warnings denied, formatting, and whitespace checks.
Planck independently passed all 55 pure state tests and found no issue at the frozen `121e7a80` hash.
That isolated run linked stock libpq but made no PostgreSQL calls, so it provides state evidence only.
This handoff adds no schema, DTO, path, locking, or conversion changes.
Jason owns the CDC plan, durability guide, and assigned source-frontier context documentation.
His three-file handoff is frozen and matches the codec and authority contracts without a new decoder feed signature.
The author reports zero hard STE findings, five advisories, ten valid local links/anchors, and unchanged pilot artifacts.
The root worker matched plan hash `0237d243f9894891d914d65cd2176d701e874b4dce3425e651855437b18e0cbb`, guide hash `16690bd3d2d3377ce2a19662b1cf76218577e848c58f1d949373099a45bf4ddb`, and context hash `8efb2714bde4a733dfcb34f2a0657955fd9f826d49b4841e5b84930a5567fc00`.
Faraday independently passed the three documentation hashes and ten local links/anchors, with the five pilot files unchanged.
That review found no new API or premature CLI, ACK, or performance claim and does not replace runtime gates.
The root worker retains root integration, CLI integration, root guides, and the task ledger, with no additional source change needed for these handoffs.
The next combined source-transport gate waits for its owner freezes and independent review.
The reviewed optional-position contract uses a separate temporary Git index and ordinary tree export based on `eaf7517`.
Its check/lint/test and authority16 gates exclude active PG and replication-harness edits while preserving those working files.
The native verifier rejects external or symlinked prefixes, so the export uses a byte-identical copy of the verified artifact with unchanged guards.
This contract-only change adds no client ABI or dependency change and does not require another Linux run before the later source snapshot.
The optional-position fix is committed as `0dbae0d6d2f7017711f51b07554604cccca83706`, with exact tested tree `6884fb58197a61ce5e366043160b433f37611429` and 16 selected paths.
Its isolated check/fmt/strict-Clippy/test gate passed 232 Rust tests, 24 CLI tests, 20 native tests, and 14 harness tests.
Authority16 passed with both real positive-COMMIT fault receipts.
All 91 exported files retained their exact Git blobs and modes, and the commit preserved other workers' changes.
The [contract handoff](../target/verify/contract-only-ype27e06/handoff.json) records commands, counts, hashes, and the copied native artifact's main-prefix install-name dependency.
This local macOS result makes no relocation or later-source qualification claim.

The source transport, paired harness, and CI changes have completed independent review.
Carson closed the PostgreSQL 9.6 option-name finding after predispatch refusal of embedded quotes, then passed the renamed pure protocol module and nine focused unit tests.
Mendel closed the FFI/pump review after a public-libpq probe showed that `PQflush` can report an empty buffer after discarding a failed write.
The private event is `FeedbackDrained`: it preserves supplied positions but proves no sending, peer receipt, acknowledgment, or durability.
Error-first handling, one pending status update, and positive all-results completion remain required.
Planck passed the paired terminal-error and keepalive harness semantics, including blocked `PQflush`, SQLSTATE `57014`, closure before deadline, and the exact fault inventory.
The [independent rename proof](../target/verify/replication-harness/rename-proof-86fmz_e9/proof.json) reverses the two names and one declared no-delivery sentence to recover all eight reviewed old hashes.
It also records the author's 19 passing offline tests on each Python version without claiming a root rerun.
Faraday passed the three-file CI delta, 14 offline tests, 33 rejected invalid receipts, 24 valid receipt orders, and the documentation links.

The source-only snapshot selects 20 paths over `0dbae0d` and excludes all C1 code, test-count changes, and its uncommitted ledger section.
It retains 31 format and 87 storage-recovery tests from the committed baseline.
`make test-replication` runs the existing wrapper profile and exact `pg::replication_live_tests::` namespace.
The default unit filter excludes that separately gated live namespace, while the combined engine target retains every ignored live case.
Descartes is the sole engine39/COPY40 producer, followed sequentially by the root check/lint/test gate on the same export.
The [source handoff](../target/verify/source-only-0x48g1ar/handoff.json) records the final tree, native receipts, commands, and gate results before a path-scoped commit.
No source CLI, decoder completion, ACK policy, production storage selection, or WAL throughput result follows from this transport milestone.

Goodall captured an ordinary export of exact commit `eaf7517` before starting its separate Linux arm64 gate.
All 91 committed files matched their Git blobs and modes, and the worker hashed 86 pipeline inputs before execution.
The export uses tree `94cc88440fd4f6ae458bb9a44e819c6027dddcf4` and source SHA-256 `1e561d7e584cfb6833a7bde43e4c215ae85d2aa1e930c4abca1af865e7ab9ab2`.
Its archive SHA-256 is `7f34b6f1a38241730c75fbec7fa87e8153cc209c2733b21f143530c050c6c6d3`.
The Linux arm64 gate passed all 11 stages on PostgreSQL 17.11/170011 with Rust/Cargo 1.99.1.
It passed 222 Rust, 24 CLI, 20 native-build, 14 harness, and 12 CI tests, plus engine30 and COPY40.
Both real held-COMMIT fault receipts, three oracle negatives, two wire faults, SCRAM, and stock-libpq refusal with zero connections passed.
All 91 exported files and 86 pipeline inputs remained unchanged after the run.
The root worker verified the report SHA-256 `ca39b1f77e2a9c4f051bccc4cf6d93b8dacb68f205cd2db303bee2a946e22917` and manifest SHA-256 `2bb9446ad00f697d1f513d1ca509d910f7df22cb218ab30fdd19c822b9ea0f31`.
Evidence is under `target/ci-eaf7517-28qtnuzg`, and the worker removed its owned container.
This result qualifies only committed `eaf7517` on that fixture, not later working-tree changes, CopyBoth, codec version two, amd64, hosted CI, or performance.

## C1 development-only storage milestone

- [x] Freeze the seven implementation/test files and the common role/catalog contracts.
- [x] Add persistent revisions, bounded record numbering, completion indexes, and stable two-pass read views.
- [x] Verify the healthy nine-process Receive, Apply, and Status schedule against external input and prefix oracles.
- [x] Close component reviews, including the role-lock and trailing-output corrections, with redb's negative qualification recorded separately.
- [x] Run the exact C1 integration snapshot's required gates and preserve the final receipt.
- [x] Commit the tested frozen C1 blobs while preserving concurrent codec and tooling edits.

The [C1 report](../docs/research/2026-11-06-storage-concurrency.md) records the reviewed sources, healthy proof, negative probe, and remaining qualification gates.
Common/catalog, SQLite, segmented journal, and driver reviews passed within their stated scopes.
The redb healthy checks passed, but its Status corruption probe panicked before returning a controlled error.
The pinned redb 4.5.0 API has no adequate public read-only integrity check, so it is not qualified for production selection under that requirement.
Its blocking-open limitation is a separate C2 gap.
We retain redb only as a development comparison and preserve the negative evidence.

Root discovery found 141 recovery/driver tests, and the frozen Make guard required that exact nonempty inventory.
The format inventory remains 31.
The [C1 integration handoff](../target/verify/c1-integration-h6s1tz_m/handoff.json) records the final snapshot and gate results.
Commit `96f1d2d6ff1edf972688440ef65c8adcac0b7bcc` matches tested tree `1ffdbee4db8603df78013fd88351ff4522de481e` across 99 source files.
The exact export passed `make -j1 check lint test`: 126 unit, 31 format, 141 recovery, 24 CLI, 20 native, and 19 harness tests, without failures or ignored cases.
Its fresh healthy schedule passed nine distinct child processes, each with external prefix four, four frames, four completed transactions, 1024 encoded bytes, and input CRC32 `1217052608`.
Receive, Apply, and Status checked eight, five, and nine phases respectively for each candidate.
The report SHA-256 is `27c65af420c8d2887a6d79f0fcfc57c40b5ff27033946a2e888a58b10f188447`.
The old CI receipt parser accepted the captured output dynamically; it did not hardcode the previous recovery count.
The snapshot excluded new tooling files and retained the existing Make-based gate.
The commit used frozen Git blobs without replacing concurrent working files or staging their changes.
No runtime source, codec wire format, or Cargo dependency change belongs to C1.
Its consumer cursor measures benchmark progress only, with all history retained and one active source transaction.
Pruning, quotas, target proof integration, broader faults, power-loss behavior, Linux qualification, and backend selection remain open.

## C2 publication and recovery experiment

- [x] Freeze the explicit 13-point observer contract for the development driver, common facade, and SQLite/journal hooks.
- [x] Complete independent reviews and integrate the frozen C2 files in a separate snapshot.
- [x] Register the publication driver in default Task/CI verification with a fresh private root and a required 22-case, 20-continuation receipt.
- [x] Verify the required 20 process-kill cases and two complete traces against external record and prefix oracles.
- [x] After every killed child, append the next parent-constructed transaction through a fresh Receive handle and verify the full retained history.

The contract retains the existing receive API and adds a shared per-call observer without global or environment hooks.
The expected prefix is one before catalog COMMIT and two after it; a killed caller's original result remains unknown.
Readable reopen alone does not satisfy recovery acceptance.
The continuation must preserve earlier records and orphan raw history while proving that the next publication succeeds.
C2 remains a development-only process-crash experiment with all history retained.
It does not qualify power-loss behavior, pruning, quotas, retirement, target proofs, production storage selection, or redb's known read-only corruption gap.
Its source, tests, and inventory changes stay outside the tooling snapshot.
Independent common/driver review passed nine focused tests, the 22-job matrix, and all 20 post-crash continuations.
Independent SQLite review passed 42 focused tests, including all nine C2 cases; one older child-process test was outside that review's execution constraints.
The author's complete 43-test SQLite run and 164-test aggregate remain separate evidence.
The 164-test aggregate and 22-child report bind the pre-fix journal; they do not validate its replacement hash.
Journal review found that a caller could catch an observer panic inside the raw callback before its failure latch was set, then commit that raw batch.
The common guard contains this path; the journal author corrected the raw failure latch and observed the new regression fail before all 28 journal tests passed.
Independent journal re-review passed SHA-256 `31049754bf86c48acf0257c7aa1b66595b92f533a9a3a21bf2960c42e2128de4`: one fresh regression passed, and the exact inverse reproduced the reviewed pre-fix source.
That result closes the raw-journal finding; the author's 28-test run remains separate evidence.
The distinct high-effort wiring review passed the four frozen Task/CI files with no findings.
Its 60 scoped tests, 70 report refusal probes, and seven existing receipt refusal probes passed; it inspected the actual driver route and validate-before-report boundary without running Rust or process-crash tests.
The [wiring review](../target/verify/c2-gate-review-py95_lmn/review.txt) records the exact file hashes and scope.
Derive the final test inventory from actual discovery after that freeze; do not promote the anticipated count to a result.
The focused integration uses tooling commit `15a4222` plus the five C2 files, Task/CI registration tests, and affected documentation.
Restore the driver's committed development-codec import only in the export, preserving the primary E2 path retarget and excluding runtime, Cargo, and catalog-profile changes.
The default gate retains 11 CI stages and three Python suite receipts, with one additional publication receipt required inside the existing test stage.
The registration passed 36 Task tests, 24 verifier tests, and the full 152-test CI unit aggregate after observed failing tests for the missing task, validator, and required receipt.
The [wiring proposal](../target/verify/c2-gate-wiring-_sljty6r/proposal.json) records the 13 selected paths and their hashes; it is not a frozen C2 tree or Rust qualification receipt.
The selected export discovered 165 recovery tests, and the Task inventory guard requires that exact count.
The [C2 gate receipt](../target/verify/c2-integration-5qpi5dva/gate-results.json) binds tested tree `d711ec2c73343e81bf1dd50d2ddad94264ed04bb` and all 114 exported files.
On macOS arm64 with Rust 1.99.1, `task check lint test` passed 322 Rust tests and 63 Python tests, including the exact 165-test recovery inventory.
The same export passed all 152 CI unit tests, strict YAML validation, and Python compilation checks.
Its automatic publication gate observed 22 distinct children: 14 segmented-journal cases and eight SQLite cases, with 20 unknown killed-caller outcomes and two returned complete traces.
All 20 recovery continuations passed: 12 reached external prefix two and eight reached prefix three.
The fresh report SHA-256 is `dc2a4d45349c48d09efe28c92a41486db5b400b7a3b37655abb119bc400f1614`.
The copied controlled native artifact passed verification; its macOS install name retains the original immutable local prefix, so this is not relocation evidence.
No runtime source, Cargo dependency, catalog profile, or snapshot-anchor spec change belongs to this integration.
Hosted [run 42325390460](https://github.com/ydixken/rpgmigrate/actions/runs/42325390460) passed native amd64 and arm64 verification for C2 commit `0f5ae5269a0a18f813e72469b50d60f7b217e7a0`.
Both bounded reports confirm all 11 source stages, 152 CI unit tests, 322 Rust tests, 63 Python tests, the 22-case/20-continuation publication gate, engine39, and COPY40.
The runtime-image build, exact-artifact smoke, and source/run binding steps also passed on both architectures.
The downloaded artifacts match their GitHub digests and the same source hash; the [hosted C2 receipt](../target/verify/c2-integration-5qpi5dva/github-qualification.json) records the evidence.
[PR #1](https://github.com/ydixken/rpgmigrate/pull/1) collected this foundation scope and its development-only limits.
Its later manual merge and post-merge verification are recorded below.

## Publication and tooling boundary

The user-requested partial checkpoint [e2e8887](https://github.com/ydixken/rpgmigrate/commit/e2e8887160a6e9074ff95f36cd2022d77eb37ebc) changes only `src/copy.rs` on the qualified `0f5ae52` base.
Its [verified subset](../target/verify/checkpoint-copy-subset-zj27w8a5/verified-subset.json) passed `task check`, strict `task lint`, `task test`, and a fresh PostgreSQL 14.24 COPY40 run.
The default tests passed 324 Rust and 63 Python cases, plus 22 publication cases and 20 continuations.
The [push receipt](../target/verify/checkpoint-copy-subset-zj27w8a5/push.json) verifies the feature branch, unchanged main, and empty normal index.
The [hosted result](../target/verify/checkpoint-copy-subset-zj27w8a5/hosted-pass.json) records native amd64 and arm64 PostgreSQL 17 jobs passing for `e2e8887`; the publication jobs were skipped.
The [PR readiness receipt](../target/verify/pr1-ready-rjij5v0r/after.json) records PR #1 changing from draft to ready at exact head `e2e8887` and base `55ac7d4`, after four successful checks and two skipped tag-publication jobs were rechecked.
That readiness operation preserved the foundation scope, title, body, branch heads, and normal index.
The [published runtime/storage code review](../target/verify/pr1-runtime-storage-review-01f273c062/review.json) passed within its bounded scope and matched all 114 retained source files to `e2e8887`.
The separate [CI/container code review](../target/verify/pr1-ci-review-1ue7y1wy/review.json) also passed and bound the four hosted verification reports to the committed CI source inventory.
Neither bounded review required a code correction.
The user manually merged PR #1 at `2026-11-07T10:50:42Z`; the integration agent did not perform the merge.
Lagrange's [merge and CI receipt](../target/verify-postmerge-ci-42365656509-8363edbd7e57-20261107.json) records merge commit `8363edbd7e575a11756bb8d4fced2ac7c8c491a6` with parents `55ac7d4` and `e2e8887`.
Its tree is identical to the reviewed head, so the four published commits remain in main's ancestry.
[Post-merge CI](https://github.com/ydixken/rpgmigrate/actions/runs/42365656509) passed native amd64 and arm64 verification; version-index publication was skipped.
The [follow-up branch receipt](../target/verify/followup-branch-6_8xbg8r/receipt.json) records `feat/native-copy` starting at that merge commit with all 160 working files, file modes, status, and index bytes unchanged.
The original feature branch remains intact, and no reset, rebase, main checkout, or remote write was used for that branch transition.
Newer Native4, owning-COPY, and PostgreSQL 16/18 work remain separate local milestones.
The [full WIP capture](../target/verify/checkpoint-source-o7w7ck9v/snapshot-v2.json) remains separate at tree `0b77f3629f9b0a7a3cd80376651faa8d7f9867e4`.
Its [required checks](../target/verify/checkpoint-source-o7w7ck9v/checkpoint-report-v2.json) passed all-target checking but failed strict lint and three unfinished adapter tests, so it was not committed or pushed.
Actual Git blobs prove that capture includes the reviewed B source publisher and excludes the newer C authority replacements; later work remains local.

The [public repository](https://github.com/ydixken/rpgmigrate) published the source milestone at `55ac7d4a3907be09d951f9f356975c29133afb8d` and C1 at `96f1d2d6ff1edf972688440ef65c8adcac0b7bcc`.
The source commit's exact tested tree is `21a0f7aa7ac4b6146fa501b38296fe318ca50964`.
The source gate includes 244 Rust, 24 CLI, 20 native, and 19 harness tests, a fresh engine39 result, and explicitly retained prior COPY40 evidence.
The [source handoff](../target/verify/source-only-0x48g1ar/handoff.json) preserves the actual execution order, commands, hashes, and input-equivalence proof.
The initial main creation used that verified source commit; the later PR merge is recorded above.
GitHub main remains the default branch, and follow-up implementation uses `feat/native-copy`.
Direct implementation pushes to main remain prohibited.
The publisher verified the C1 feature push by GitHub API and `ls-remote` without changing main.
GitHub runs [42312950830](https://github.com/ydixken/rpgmigrate/actions/runs/42312950830) and [42311951675](https://github.com/ydixken/rpgmigrate/actions/runs/42311951675) completed successfully at source commit `55ac7d4`.
The C1 run [42314615234](https://github.com/ydixken/rpgmigrate/actions/runs/42314615234) was in progress at publication handoff; no C1 Linux result is inferred from the local gate.

- [x] Replace repository Make orchestration and active commands with one root Taskfile, preserving the 21 task names, default check, and help.
- [x] Use one standard-library bootstrap helper and canonical Task 3.54.0 pins across local, CI, and container execution.
- [x] Complete hosted native amd64 and arm64 CI gates against the same exported runtime image artifacts.
- [x] Build the current-CLI runtime image and pass its 23 checks on the local native arm64 fixture.
- [x] Configure version-tag-only publication to ghcr.io/ydixken/rpgmigrate after exact-artifact gates, without creating a tag or release.

Preserve archived command/evidence bytes and the native dependency's upstream GNU Make build.
Routine future implementation and tooling workers use high effort, while recovery, durability, concurrency, and critical reviews use max effort.
The immutable C1 export released existing CI, harness-adapter, and codec paths without changing its tested source.
The tooling export uses C1 as its base and includes only reviewed Task, CI, image, harness-adapter, and root-documentation changes.
It excludes canonical codec, decoder, catalog-loader, test-only PG helper, DML planner, and C2 changes.
The tooling snapshot has 26 tasks, including the original 21 names, default check, help, Linux CI, image build, and image smoke.
Its 33 scoped tests and both environment/CLI override and command-prefix corrections passed independent re-review.
The CI offline inventory derives its exact nonempty suite counts from the source, including the updated Task tests.
The 71-case aggregate omitted the image build, runtime, and smoke unit modules.
The CI owner added those three modules to the existing required inventory and verified missing-module refusal.
The final aggregate passed 146 tests with one footer: 21 verifier, 17 publisher, 33 Task, two launcher, seven image-build, 27 runtime, and 39 smoke checks.
The final symlink-refusal test correction establishes every prerequisite and asserts the specific refusal reason; its two-line inverse diff and targeted test passed independent verification.
Live image checks do not replace these negative unit regressions; the runner retains one result footer and 11 stages.
Runtime-image, smoke, and CI code have dedicated independent reviews before root integration and live artifact checks.
Offline checks alone do not qualify the runtime image or hosted CI.
Root integration removes Makefile and migrates active guides to Task; archived command evidence and PostgreSQL's own GNU Make build remain unchanged.
The shared bootstrap and the complete 26-task inventory ran locally.
The runtime reviewer identified artifact-validation gaps covering setuid permissions, protected roots, layer overlay types, actual ELF architecture, and strict schema value types.
The corrected predicates passed independent source inspection and all 31 ordinary build/runtime regression tests.
The [review receipt](../target/verify/runtime-predicate-review-g8v14rgx/review.json) records that metadata/header-only method and its limits.
The original probe was blocked by automatic screening and was not retried; no result is claimed for it.
The CI publisher fixture now supplies its actual ELF architecture, and the independent 71-case CI aggregate passed after that two-line test-only adaptation.
The documentation reviewer closed both remaining active Make references by verifying their exact two-line replacements.
The smoke owner corrected final receipt status and atomic publication with 29 passing tests.
Independent smoke review then found a Podman network-name and inspection-field mismatch that could leave an unrecorded owned resource.
The focused network fix passed independent re-review of the two original Podman cases, a Docker control, and nine network regressions.
All component review blockers closed before the focused tooling integration.
Actual image checks then found and fixed the OpenSSL default CA path, a tool-name predicate that also matched directories, copied Rust-license permissions, and mismatched package-inventory field formats.
The fixes retain HTTPS and package-signature verification, strict artifact permissions, and exact architecture-qualified package versions.

The [tooling handoff](../target/verify/tooling-r4-5ieq15dn/handoff.json) records the frozen packaging tree `4f0ed88d13885a33511bd6630fc3f7a88dd74710`, source hashes, commands, receipts, and limits.
Its fresh static suite passed 146 tests, and its native arm64 image passed all 23 runtime checks against PostgreSQL 17.11/170011.
The checks include COPY contents, persistent state, native-library identity and refusal controls, SIGINT/SIGTERM cancellation, and owned-resource cleanup.
The earlier exported tree `0d1db94f20d51ae4f35e7b787d7792f7554dd1b8` passed macOS Task check/lint/test with 298 Rust and 63 Python tests.
Its Linux arm64 report passed all 11 stages, including 142 CI unit checks, 298 Rust and 63 Python tests, engine39, and COPY40.
Those results retain their original-tree attribution; every Rust, Cargo, Task, harness, and native-build input is byte-identical in the packaging export.
The six changed packaging/test files received fresh static, image, and smoke checks.
The local Skopeo 1.22.2 data-copy check preserved a two-platform fixture index and all seven blobs without running fixture binaries or contacting a registry.
This is not native amd64 qualification or a publication result.
Hosted [run 42320992263](https://github.com/ydixken/rpgmigrate/actions/runs/42320992263) passed native arm64 and amd64 verification for tooling commit `15a42229de6237c5860406d0f3bc39b0758dad0f`.
Each architecture passed all 11 source stages with 146 CI unit, 298 Rust, 63 Python, engine39, and COPY40 checks, followed by exact-artifact image build, smoke, and receipt binding.
The downloaded bounded verification artifacts match their GitHub SHA-256 values and the same source hash on both architectures.
Native image transfer and publication were skipped for this feature-branch push, and no image tag or release was created.

## Canonical codec and decoder delivery

- [x] Release canonical record-codec promotion and pure test_decoding work after the C1 export.
- [x] Independently review the canonical codec, event payload encoding, and pure decoder within the recorded scopes.
- [ ] Complete the additional max-effort decoder ordering and failure-state source review.
- [ ] Complete C2 memory accounting before decoding, including typed-value allocation overhead and bounded relation cardinality; encoded-byte limits alone are not RSS limits.
- [x] Freeze the source catalog-loader API and exact live-test inventory with the harness owner.
- [ ] Register the final nonempty catalog-harness offline inventory and the required ten-case source/target Catalog profile in default E2 gates before merging that slice; a manual profile alone is insufficient.
- [ ] Guard version-specific catalog queries and qualify PostgreSQL 9.6–11 before claiming those source versions; reused `attgenerated` and related queries do not support them yet.
- [ ] Integrate a bounded receiver and replay path before claiming clone/follow behavior.

The codec author reports 25 event-payload tests, 31 format tests, and 141 recovery tests with no failures or ignored cases.
The promotion retains existing record bytes and canonical identity types; it moves the exact approved CRC dependency to the runtime graph without a lockfile change.
These results are scoped author evidence, not a broad lint or runtime CDC gate.
Staging dead-code diagnostics remain visible until consumers are integrated.
Independent codec review passed without a ranked defect.
Independent decoder review passed all 35 decoder/parser and 25 event-codec tests, plus formatting and fixture provenance checks.
That decoder child ran at high effort, not max; the additional max-effort source review covers ordering and failure-state behavior without repeating unchanged tests.
Its evidence is [the decoder review receipt](../target/e2-decoder-review-01a1b34f/verification.json), SHA-256 `c88c9be72d3ad63078271101322c01e93a85cd07fcb199707d738529f015f6e4`.
The recorded strict warning gate failed with 77 staging diagnostics and remains open until real consumers are wired and verified.
The codec review identified typed-value allocation up to roughly 32 times its encoded tags as a C2 accounting requirement, not a satisfied process-memory bound.
The decoder uses one last relation key per transaction, with required relation context emitted on a key change.
The test-only PG frame helper's visibility and re-export passed an exact-diff review; its body and production ABI are unchanged.
Catalog ownership and live inventory must be coordinated before adding ignored cases to the global engine profile.
The six-case source Catalog profile uses a fresh owned database with separate ordinary-source and administrator connections; the administrator is limited to the custom-opclass refusal fixture.
The author observed six expected loader-stub failures, then six passing cases with zero ignored tests on PostgreSQL 14.24.
The harness owner independently accepted the saved GREEN receipt and rejected the saved RED receipt without another database run.
Independent catalog-harness review passed its nine new checks and 19 existing harness checks, with syntax, help, source identity, and unchanged Engine39/COPY40 fault footprints verified.
Independent catalog review passed source SHA-256 `d9ca3aae478c57a1e8739899580dbd9d65967600137e95425c689a627f62e7fe`, its 13 unit tests, and the attributed six-case live evidence.
The reviewer verified that the extracted E1 SQL and helper behavior were unchanged.
The reused version-specific catalog queries, including unguarded `attgenerated`, leave PostgreSQL 9.6–11 unsupported until version qualification.
This is a broader inherited compatibility gap, not a compatibility regression introduced by the loader extraction.
The live contract is qualified only on the recorded PostgreSQL 14.24 fixture, and the E2 broad gate remains open.
The three pre-catalog harness files were captured as Git blobs before the primary files changed, so the tooling export retains Engine39 and COPY40.
Catalog default-gate registration belongs to the later E2 integration and must not enter the tooling snapshot.
The catalog producer released the sole database-test window after its RED/GREEN runs.
The pure E3 DML planner owns derived SQL, parameter OIDs, index maps, relation keys, and operation/TOAST shape; binding borrows row values.
Its approved scope is a new private plan module and unit tests, plus one private module declaration.
Both-endpoint static preflight remains a caller requirement, DEFAULT primary keys are the initial identity profile, and binary values and TRUNCATE fail explicitly until their execution contracts are proved.
This planner work uses an isolated Cargo target and stays outside the tooling snapshot, without a live-database or runtime-apply claim.
The planner author reports 20 failing stub tests followed by 20 passing unit tests with none ignored.
Its scoped strict Clippy run still failed with 92 reported dead-code diagnostics across staged E2/E3 code; formatting and whitespace checks passed.
Independent planner review passed the exact source and 20 focused tests without a ranked defect.
The later `DmlPlan::owned_capacity_bytes` prerequisite accounts for the four owned heap-buffer capacities with checked sums and excludes inline storage.
Its author observed a meaningful failing spare-capacity test followed by 21 passing tests; the leader's bounded source and exact-inverse review passed without claiming an independent fresh build.
The [capacity handoff](../target/apply-dml/capacity-handoff.json) binds plan SHA-256 `f0894ab069df82d8b6e0fc2d8fad4057acace53204d6f85f34cd917308dc9716` and the focused test source.
Real consumer wiring, transaction poisoning for local plan/bind failures, and a clean integrated warning gate remain required.
Serial-apply integration depends on the accepted budgeted event-decoding and target transaction/preflight interfaces, their scoped reviews and live evidence, and real consumers.
`SourceSlotBinding` is the single accepted new source-binding record: `stream`, `slot_name`, canonical `Plugin`, and positive `catalog_epoch`, without duplicate run or source identity fields.
Its checked accessor must require `identity.run == stream.run`, the exact confirmed binding, and the held native format/run; absent, pending, or unknown state fails.
`StreamId.generation` must not be equated with `catalog_epoch` or `RelationKey.generation`.
The source binding and capability implementations have been released; their distinct review and live gates are tracked below.
The budgeted event-codec author reports 29 passing tests after failing controls.
Independent critical review passed the frozen source, all 29 focused tests, and compiler controls that reject borrowed event data escaping the callback.
The [bounded allocation review](../target/cdc-decoded-budget-critical-review-20261107/review.json) records its allocation, drop-order, wire-compatibility, and error-path audit; ten staging warnings remain outside this scoped pass.
That accounting covers retained typed capacities, not process RSS or allocator overhead, and preserves the wire and existing limit semantics.
The reviewed deallocation slice used checkpoint SHA-256 `425bb1c404ff1c0939392a1973805fffae4ef02a5af849bc5c9418df522d4d12`.
Extensions to two existing Authority16 live bodies passed all 16 cases with zero ignored tests and two observed COMMIT faults on owned PostgreSQL 14.24.
Ordinary-snapshot controls produced runtime assertion failures for unsafe `DEALLOCATE ALL` and omitted poisoning, without changing the primary method.
The [authority live receipt](../target/e3-deallocate-worker/authority-live-receipt.json) binds the frozen test SHA-256 `56c4331d516f869f400054d4351130ceb49407db8ea6b4be9da6de2d4863bb61`; root matched its hashes without repeating the database run.
Independent bounded review passed the two changed live bodies and their Authority16, two-fault, and runtime negative-control evidence with the primitive unchanged.
The producer released the exclusive window; broader runtime integration remains a separate gate.
The target catalog helper adds four agreed cases to the six-case source inventory and remains a separate integration.
The [independent Catalog10 harness review](../target/verify/catalog10-harness-review-61bus36q/review.json) passed all four changed paths, fresh warning-strict 10-case and 19-case offline suites, 66 invalid-receipt probes, and two optimized-Python refusals.
The six source IDs and shared fixture helper remain unchanged; inverse checks restore the unrelated committed runner and guide bytes.
Independent target-helper review found that a role with SELECT, INSERT, and UPDATE permissions but no DELETE permission could pass lock acquisition and preflight.
Acceptance requires effective table DELETE permission, INSERT and UPDATE permission for every replicated column, and SELECT permission for keys after lock acquisition, with table or column grants accepted where applicable.
The restricted-role regression produced the intended nine-pass, one-failure result after an actual DELETE permission error, then passed all ten cases with zero ignored tests after the fix.
Table, column-only, and inherited-grant positive controls executed INSERT, UPDATE, and DELETE; each missing required privilege was refused.
The [frozen permission handoff](../target/e3-catalog-p2/handoff.json) binds source SHA-256 `048a066547419524802c9936a066cb7ccc2f115b44f35c721dbcf102d2c579e4`, the runtime RED/GREEN evidence, 28 passing focused units, and scoped formatting/Clippy checks with staging warnings visible.
Root matched the live source and log hashes without repeating PostgreSQL execution, and the producer released the exclusive window after fixture cleanup.
Independent bounded P2 re-review passed the exact patch and inverse, the effective-permission checks, and the attributed live RED/GREEN evidence with no new finding.
The reviewer ran all 28 focused tests in an isolated copied binary with zero ignored tests, without repeating PostgreSQL or claiming a fresh Cargo build or broad strict gate.
Root has added the required E2 offline/live Task and CI registration; independent wiring review and the integrated default gate remain open.
All prerequisite builds use isolated Cargo targets and remain outside the published C2 source.
The next clone/follow bootstrap requires the accepted source-snapshot provider and separate logged COPY snapshot-anchor integration.
The native COPY receipt is not an atomic logged target anchor, and a barrier over an empty checkpoint proves zero CDC transactions rather than COPY coverage.
Do not substitute a synthetic transaction for the unimplemented anchor.
We accepted an initial PostgreSQL 14/17 control-read profile for the source-snapshot provider.
We accepted fresh native source-control format three without older-state conversion; target logged metadata version two and event wire format `RPGEXP02` stay unchanged.
A later snapshot-anchor control schema must use native format four or a new explicitly versioned record; it must not reinterpret a published source-only format three.
The actual importer must already have `pg_control_system` EXECUTE permission and catalog visibility; check both before creation and again inside the imported REPEATABLE READ READ ONLY transaction.
The exporter contract includes `IDENTIFY_SYSTEM`, the server's actual consistent point, the required four-column result shape, and complete result draining.
We retain the default slot name `pgcopydb` and exclude runtime grants, native legacy-state adoption, and automatic slot deletion.
The reverse application-name challenge is deferred, and PostgreSQL 9.6 through 11 qualification remains future work.

- [x] Review the source-snapshot plan and release its transport, state, provider, and harness owners.
- [x] Add the canonical `Plugin` serde support, shared 1024-byte token limit, and private module declarations without duplicate identity types.
- [x] Implement and independently review the provider core and its nine pure tests.
- [ ] Complete source-snapshot live qualification and the integrated required gates.

## Source snapshot and serial prerequisites

- [x] Qualify frozen native source-control format three with an independent source-bound build: 75 state tests, including 20 source cases, passed with none ignored.
- [x] Resolve the PG post-CREATE guard review at `bdf67148`: require a healthy connection and transaction Idle after complete draining, before `Created`.
- [x] Review the provider core at `3e91fa78`, including its nine pure tests and exact child-declaration inverse.
- [x] Run the retained source-identity helper's three live cases on owned PostgreSQL 14.24, with zero failures or ignored cases.
- [x] Review the source capability at `7195cd2a` under the fresh-online, dedicated-session, static-DDL contract.
- [x] Review the target validation hoist at `121abb93`, with Authority18, two COMMIT faults, and five intended 17-pass/one-failure controls.
- [x] Review the frozen snapshot child and harness, including real provider-created binding checks inside the existing twelve cases.
- [x] Run SourceSnapshot12 and five fault IDs on PostgreSQL 14.24, including actual wrong ReadyForQuery T/E replies.
- [x] Run the same frozen SourceSnapshot12/five-fault profile on PostgreSQL 17.11.
- [x] Implement and statically review the same-transaction target preflight wrapper at `99f2458f`.
- [x] Qualify the wrapper's actual same-backend success, refusal, and poisoning behavior, with the recorded native-loader provenance limit.
- [x] Implement and statically review bounded serial apply at `62351166`, using the actual source capability, target wrapper, and budgeted event/DML-plan interfaces.
- [x] Bind the serial baseline and each runtime negative control to a fresh source export and executed binary.
- [x] Verify source WAL, journal reopen, serial effects, atomic checkpoint publication, and crash recovery through the bounded PostgreSQL 14 development bridge.
- [x] Review source-profile Task/CI wiring, including the Engine41 filter regression.
- [x] Review the journal harness and required default-gate wiring before the live bridge run.
- [x] Verify the corrected functional default-test and CI-unit inventories on an immutable combined export.
- [ ] Pass strict integrated root gates and the exact-source hosted gates before publication.

Plato coordinates this bounded integration milestone; root retains shared files, the Git index, commits, exports, and exclusive PostgreSQL leases.
Architecture, production backend selection, and proof changes remain with the parent architect.
The [hoist handoff](../target/e3-guard-hoist-worker/handoff.json) binds the native-format-two RED/GREEN trees and each distinct mutation binary.
The reused-binary mutation attempt is excluded from that evidence.
The [source-probe receipt](../target/verify/source-catalog3-pg-as92jx2o/source-catalog3-receipt.json) binds tree `8abe263c6bb46ddfd5deb356b219a486f2e40cf4`, the retained `4aa732cd` helper, the executed binary, and its three-case PG14 result.
Its predecessor executed zero tests because its old PG dependency lacked `quote_literal`; the failed export is retained and is not a native regression result.
That probe run does not qualify source capability, native format three, or snapshot creation.
The [provider handoff](../target/verify/source-snapshot-provider/handoff.json) records the nine pure tests and scoped review limits.
The [snapshot harness handoff](../target/source-snapshot-harness/handoff.json) records 52 offline tests and twelve discovered live cases; discovery is not live qualification.
The first [PG14 snapshot run](../target/verify/source-snapshot14-n15gw0ly/run-result.json) passed ten cases and failed both CREATE reply-loss cases because the helper closed without a receipt.
Its source tree and executed binary stayed unchanged; the failed export is retained, and the owned PostgreSQL processes were closed before releasing the lease.
The owner reproduced the blocked upstream worker offline and corrected shutdown without moving the required client-EOF witness.
Independent review passed the exact two-file correction and 17 fresh offline tests.
The [corrected PG14 run](../target/verify/source-snapshot14-r2-6rzwgquh/run-result.json) passed all twelve cases and validated five fault IDs with seven phases on tree `c13c07282cb111e4916d9dd1efd69caf23827700`.
Its executed binary and 136 source hashes stayed unchanged; PostgreSQL 17 qualification remains separate.
The same tree's [PG17 run](../target/verify/source-snapshot17-_4yp5ak_/root-intake.json) passed eleven cases and failed the expired-snapshot case with a connection-input error instead of SQLSTATE `22023`, with none ignored.
Source, binary, and native hashes matched before and after that run; the owned processes and container were removed, and the failed gate was retained without a retry.
The exact-version oracle correction passed independent review, followed by a [fresh PG14 run](../target/verify/source-snapshot-oracle14-8twhw91p/handoff.json) with twelve passing cases and five retained fault receipts covering seven phases.
That run observed `22023`, preserved actual loaded-library, copied-native, source, binary, and capture-helper hashes before and after execution, and passed independent review of the bounded capture script.
The [corrected Linux arm64 PG17 run](../target/verify/source-snapshot17-oracle-ub_xfkmv/root-intake.json) passed all twelve cases and retained five fault receipts covering seven phases, with expected and observed expiry SQLSTATE `42704`.
Root checked the source, binary, helper, actual loaded library, retained phase fields, and owned cleanup; complete native/context validation occurred before fixture cleanup and was not recreated afterward.
The [capability handoff](../target/e3-source-catalog/cap-handoff.json) records 41 author unit tests and the exact source inverses.
Its independent copied-binary run passed 13 new guards, but the author packet does not bind that binary to the original source build; no such attestation is inferred.
Root's [Task/CI wiring evidence](../target/verify/source-task-wiring/handoff-p2.json) records meaningful refusal controls, 39 Task tests, 28 CI validator tests, 52 harness tests, and the 159-test offline CI aggregate.
Independent review caught an Engine filter that excluded the required E1 catalog preflight case; the added regression failed with 40 retained cases and passed with all 41 after narrowing the filters.
The narrow re-review passed the revised Task files and two fresh focused tests; the three CI file hashes stayed unchanged.
The [serial qualification export](../target/verify/serial-qualification-cgtrnqnb/freeze.json) freezes 138 source files and a verified native artifact for fresh per-variant checks.
The earlier 21-test and eight-mutation runs reused a Cargo target and remain diagnostic evidence without an executed-binary attestation.
The [fresh serial qualification](../target/verify/serial-qualification-cgtrnqnb/root-intake-verification.json) verifies 21 passing pure tests and eight intended runtime failures across nine distinct source copies, Cargo targets, and executed binaries.
Each receipt binds all 138 source files and the native artifact before and after execution; every mutation changes only the serial implementation and preserves the tests.
The first control-driver rejection remains excluded, and the replacement controls require the exact intended test and failure location.
The test-only journal adapter reuses the canonical codec and existing storage modules; its discovered namespace includes 121 ordinary tests and three ignored process cases.
The [registration evidence](../target/verify/snapshot-journal-registration/receipt.json) and [SQLite selector inverse](../target/verify/snapshot-journal-registration/sqlite-selector.json) preserve the provider's production body and the storage backend's production bytes.
Execution through the integrated default gates remains open.
Independent static review passed the [journal adapter](../target/verify/snapshot-journal-registration/independent-review.json), including post-reap crash receipts and committed-Unknown reconciliation.
The [journal harness handoff](../target/journal-serial-harness/handoff.json) freezes seven files and preserves the existing profiles through exact inverses.
Root's [journal wiring handoff](../target/verify/journal-root-wiring/handoff.json) requires its two parent cases, two post-reap child receipts, and two committed-Unknown fault receipts in the existing default gates.
The new wiring passed 41 Task tests, 29 validator tests, 66 harness tests, and the 162-test offline CI aggregate.
Independent [harness review](../target/journal-serial-harness/independent-review.json) passed all seven frozen files and 12 fresh offline tests; independent [wiring review](../target/verify/journal-root-wiring/independent-review.json) passed all four files, 41 Task tests, and 29 validator tests.
The [wrapper live-test review](../target/e3-target-preflight-worker/live-independent-review.json) passed the two appended case bodies and their inverse without claiming database execution.
The [wrapper live qualification](../target/e3-target-preflight-worker/wrapper18-controls-52938ir3/root-intake.json) then passed 18 cases and both existing COMMIT fault checks; private helper and latch controls failed at the intended assertions with 16/2 and 17/1 results, respectively, and none ignored.
Source, copied native files, executed binaries, and the capture helper matched before and after; the actual macOS load path and library bytes were observed only afterward, so no fresh per-run loaded-library-before claim is made.
The first [journal bridge attempt](../target/verify/journal-serial-b8d_sqnx/run-result.json) stopped during fixture setup and executed zero Rust cases.
An [owned PostgreSQL type probe](../target/verify/journal-serial-b8d_sqnx/identity-type-probe.json) confirmed that the OID JSON value is a string unless the query casts it to an integer type; the fixture retains its strict integer and range checks.
The two-file correction passed independent review and thirteen fresh offline tests, followed by the [corrected journal run](../target/verify/journal-serial-r2-_e52v9f2/root-intake.json): both parent cases passed with no failures or ignored cases.
The frozen wrapper validated two post-reap SIGKILL receipts and two committed-Unknown receipts before emitting its three exact footers and cleaning the owned fixtures.
That run binds all 142 source files, its executed binary, copied native files, and the actual loaded-library bytes before and after; raw fixture JSON was not retained for later replay.
The [independent input trace](../target/verify/journal-serial-r2-_e52v9f2/input-attribution-review.json) confirms actual CopyBoth/test_decoding WAL input, experimental segmented or SQLite storage, reopen, and the real serial applier through a test-only adapter.
Live catalog data supplies relation metadata; synthetic record changes belong to explicit negative controls.
The parent accepted this bounded PostgreSQL 14 development milestone, with the private bridge, experimental backends, and retained-evidence limits stated above.
These scoped results do not close the strict Rust warning gate, qualify an alpha, measure WAL apply speed, or select a production storage backend.
The [first combined root gate](../target/verify/source-journal-integrated-vxchw0c6/gate-result.json) compiled and passed 434 ordinary Rust tests but did not pass lint or complete the default test sequence.
Its export omitted the reviewed harness runner helper, its SQLite test block failed formatting, and its isolated CLI invocation omitted the explicit binary override.
The separate [strict Clippy diagnostic](../target/verify/source-journal-integrated-vxchw0c6/clippy-diagnostic.json) reports 145 non-test library errors and eleven test-library errors, including unused staging code and three test-only lint findings.
The [corrected functional gate](../target/verify/source-journal-root-r2-ihe0crur/root-intake.json) includes the unchanged approved runner and four independently reviewed mechanical lint/format corrections.
It passed compilation, formatting, 630 Rust tests, Python suites of 24 CLI, 20 native and 70 harness tests, 162 CI unit tests, and the 22-case/20-continuation publication gate.
Strict Clippy still reports 145 non-test library and eight test-library errors for unused staging code, so this result does not authorize a commit or publication.
Production snapshot ownership, the receive/journal path, and the serial coordinator remain real missing consumers; the private bridge does not replace them.

## Approved COPY helper extraction

- [x] Approve the preparatory three-part extraction plan with the original cleanup scope preserved.
- [x] Extract private catalog setup and locked-preflight helpers in `src/catalog.rs`, retaining the existing wrapper.
- [x] Extract private transfer helpers in `src/copy.rs`, retaining the existing wrapper.
- [x] Review the exact diff and qualify the frozen change with scoped checks and the existing COPY40/Engine41 inventories.

Keep every fallible helper inside the original error/rollback closure, including work before BEGIN.
Preserve SQL, polling, error order, transaction flags, COMMIT, and native count-publication boundaries.
Move maps without cloning or retaining extra data.
This extraction adds no SourceSnapshot or TargetAuthority API, sealed plan, checkpoint schema, native format, or backend choice.
The active source and journal verification exports retain their frozen catalog and COPY blobs while the primary files change.
The [COPY extraction handoff](../target/verify/copy-transfer-extraction-jg_ya9ni/handoff.json) passed independent static inverse review and one scoped unit test; its ignored live case is not native qualification.
The [catalog extraction handoff](../target/verify/catalog-copy-extract-kphkx7cs/handoff.json) passed independent static inverse review and the same 41 scoped tests before and after the change.
The [joint native gate](../target/verify/copy-extraction-joint-6qn4mfus/root-intake.json) then passed Engine41 and COPY40 on PostgreSQL 14.24, including the existing two authority faults and four replication faults.
All 142 source files, executed binaries, copied native files, and actual engine/CLI libpq bytes matched before and after; owned cleanup completed and the lease was released.
The thin integration wrapper launches the compiled CLI and does not link libpq itself.
This independent regression gate does not close the separate strict-lint failure or implement a COPY snapshot anchor.
Both extraction owners handed their frozen files back for integration.
The parent released the Native4 control-state and qualified-namespace package independently.
The store-only core is approved separately; its saved plan and namespace-readiness conditions govern author start and Root integration.
The bounded catalog and mechanical per-table transfer prerequisites are released separately below.
The parent approved owning source COPY coordination and target anchor operations under the separate saved-plan and source-base conditions below.
The immutable plan store retains the frozen seal-plus-records layout, one CRC per complete record, and an aggregate seal CRC.
Keep duplicate column tables and payload lists out of that proposal.
Copernicus retains catalog streaming.
Herschel's completed mechanical extraction remains the protected base for the narrow helper visibility and validation changes.
Operation authors wait for the coherent ordinary source base and explicit file handoff.
Only the minimum entry fields needed by the consumer cross the storage interface.
This planning handoff does not change the independent Native4 or metadata3 implementation releases.
The resolved anchor domain preserves the existing source-snapshot receipt refinement: its consistent point must be nonzero.
Native intent, the sealed header, and the target row must match the confirmed receipt exactly.
All-null or all-present target columns define absence or presence; the plan nonce must still be nonzero.
The zero-point COPY case must refuse intent, proof, and DML authorization; generic LSN zero parsing and its tests remain unchanged.

The [read-only SQLite dependency assessment](../target/verify/sqlite-runtime-assessment-w1h8jhzr/assessment.json) confirmed the unchanged, previously audited 31-package lockfile.
Moving the exact bundled `rusqlite` declaration to the normal graph would add nine existing normal/build packages and leave redb development-only.
The image's existing notice collector would include their packaged licenses automatically.
The runtime catalog must retain build-override refusal and actual SQLite source/version checks, which the benchmark driver owns today.
No manifest change, promoted build, or image qualification occurred in this assessment; its advisory claim reuses the recorded RustSec snapshot.

The later performance baseline must measure the complete serial path, including source and target preflight work per transaction and realistic network latency.
The earlier `22 + N` hoist estimate is not a pipeline cost or speedup result.
Any later batching design must preserve every row-result check before COMMIT or durable proof, plus transaction count and byte invariants.

## Native4 control state and plan namespace

- [x] Accept the independent Native4 package and preserve the remaining anchor-package release boundaries.
- [x] Freeze the single pure `SnapshotAnchor` and `CopyPlanRef` definitions in `src/contracts.rs` with the state author before State edits.
- [x] Obtain independent static review of the frozen shared records and native-version fixture/documentation changes.
- [x] Observe a fresh-create/layout behavior failure against Native3 before implementing Native4.
- [x] Add required-nullable `copy_snapshot` intent in Native4, with no implicit conversion of older native formats.
- [x] Validate exact confirmed source and target provenance on bind and checked lookup, resynchronize identical binds, and refuse changed bindings.
- [x] Implement the qualified plan namespace with exclusive draft borrowing, immutable execution pins, and no PG or concrete SQLite dependency.
- [x] Test publication ordering, failure paths, file and directory identity, and the real empty-directory collision before publication on Darwin.
- [x] Update native-version fixtures and affected guides without changing unrelated profile inventories.
- [x] Freeze source and evidence for separate critical review.
- [x] Obtain independent bounded static review of the frozen State package and its retained evidence.
- [x] Qualify the exact State component and both runtime controls on Linux arm64.
- [ ] Complete Linux amd64 and integrated native qualification.

Root owns `src/contracts.rs`, existing root registration, native-version fixtures, affected guides, and the task ledger.
Sagan owns `src/state.rs`, `src/state/plan_files.rs`, and their focused tests.
`SnapshotAnchor` and `CopyPlanRef` are plain crate-private values in the existing contracts module; `CopySnapshotIntent` belongs to State.
`COPY_PLAN_REFERENCE_VERSION: u32` is `1`.
`CopyPlanRef` contains `reference_version: u32`, `plan_nonce: [u8; 16]`, `table_count: u64`, `logical_bytes: u64`, and `crc32: u32`.
`SnapshotAnchor` contains `stream: StreamId`, `snapshot_token: String`, `consistent_point: SourceEndLsn`, and `plan_nonce: [u8; 16]`.
`CopySnapshotIntent` contains exactly `anchor: SnapshotAnchor` and `plan: CopyPlanRef`.
Require reference version 1 and a nonzero reference nonce equal to the anchor nonce; CRC32 zero and empty plans are valid.
Logical bytes are the checked sum of canonical per-table payload lengths, excluding the identity header, ordinal/length framing, and SQLite container bytes.
An empty plan has zero tables and zero logical bytes; the storage seal qualifies actual counts, bytes, CRC, and encoding limits with checked adapter arithmetic and width conversions.
Do not add a path, backend, handle, repeated stream identity, or SQLite integer narrowing to State.
The contracts and state modules already have root declarations, so this package needs no new public entry point or duplicate identity types.
`NativeState::bind_copy_snapshot(&mut self, &CopySnapshotIntent) -> Result<()>` and `checked_copy_snapshot(&self) -> Result<Option<CopySnapshotIntent>>` use the existing confirmed provenance records.
The manifest retains its 16 KiB limit, and an omitted required-nullable intent field must fail instead of becoming an implicit null.
Preserve `SourceSlotProvenance::validate` and require its nonzero consistent-point refinement plus exact receipt matching for the COPY anchor and plan binding.
Creating a plan draft borrows NativeState mutably until publication consumes the draft; acquiring an immutable execution pin follows intent binding.
The storage owner must close SQLite before synchronization and publication; this package implements only the namespace boundary.
Use Linux `renameat2(RENAME_NOREPLACE)` or Darwin `renameatx_np(RENAME_EXCL)` relative to pinned directories, and fail closed without a replacement fallback.
The collision control inserts an empty final directory at the last publication hook, requires `EEXIST`, and checks that its inode is unchanged.
Run deliberate mutations only in disposable ordinary exports, and require the plain-rename control to fail for the intended reason.
Use isolated Cargo targets, preserve other writers, and keep PostgreSQL execution under Root's sole lease.
The separate global strict-lint failure remains open; this package must not suppress diagnostics or claim runtime clone/follow readiness.
The [shared-record handoff](../target/verify/native4-shared-2yay6bd9/handoff.json) records seven passing contract tests on the preserved source copy.
Removing only the two new unknown-field refusal attributes in a separate ordinary source copy caused exactly the two intended runtime failures; the initial missing-type compile failure is only API-absence evidence.
The [state author's RED receipt](../target/verify/native4-state-red/receipt.json) records the fresh-create expectation failing at native version 3 versus required version 4.
Root verified the retained source/binary hashes and the byte-identical Native3 production section without repeating the test.
The [fixture handoff](../target/verify/native4-shared-2yay6bd9/fixture-handoff.json) records 52 offline tests with warnings treated as errors and no skips, plus compile-only verification of the Rust live helper.
The five validator/helper deltas invert exactly to their preserved baseline: only native version 4 and required-null COPY intent checks changed.
Poincare passed the bounded static review of the shared records and all 11 fixture/documentation files with no findings.
The review checked the retained source, logs, distinct test binaries, and refusal controls without repeating tests or builds.
State-owned semantic binding checks are outside that review, and the existing native live receipts are not upgraded by it.
Native4 integrated live and Linux amd64 namespace qualification remain open; the old Native3 live receipts retain their original scope.
The [final State author receipt](../target/verify/native4-state-qualification-v2/receipt.json) binds 96 passing Darwin State tests, the preserved collision inode, two failing runtime mutations, and three precise borrow-check failures.
Scoped library Clippy passed with warnings still present; this is not a global strict-lint result.
The author handed back the namespace at native format 4 and target metadata version 2.
Independent static review found no critical defects and checked the six source maps, executed binary bindings, and retained controls without repeating tests.
The [Linux arm64 State intake](../target/verify/native4-linux-or8wlum0/root-intake.json) records 96 passing tests and both intended collision and POSIX-lock failures in separate fresh build targets.
All frozen source maps, retained binaries, logs, and the loaded controlled libpq were checked; the owned non-root container was removed.
This gate ran no PostgreSQL server or native rebuild and provides no power-loss or combined-route proof.

## Metadata3 row, codec, and CAS prerequisite

- [x] Release the independent metadata3 prerequisite without releasing COPY installation, snapshot proof, source coordination, store, or CLI work.
- [x] Hand the metadata author the preserved ordinary source base, shared `SnapshotAnchor`, and exact version-constant ownership.
- [x] Observe fresh-version and strict anchor-shape behavior failures before changing the row implementation.
- [x] Implement the accepted 22-field row and 32-parameter CAS for fresh metadata version 3, refusing prior version 2 without conversion.
- [x] Preserve all-null/all-present anchor shape, nonzero point and nonce, bounded token validation, and exact immutable anchor equality.
- [x] Reject anchored pending provisioning, anchor replacement, and anchor removal while preserving CDC, barriers, fences, hot-path checks, Unknown handling, and finalization.
- [x] Freeze the author's source and evidence for separate critical review.
- [x] Obtain critical review of the frozen four-file implementation.
- [x] Preflight the preserved metadata3 source and run the existing authority inventory under Root's sole native lease.
- [x] Integrate the reviewed four-file delta and coordinated version constant without replacing Native4 State with the preserved older file.
- [x] Save the [installed-anchor SQL coverage addendum](../docs/superpowers/plans/2026-11-07-metadata3-installed-anchor.md) before the test writer starts.
- [x] Qualify populated anchor installation, readback, preservation, CAS refusal, and invalid-present projection on the integrated Native4/metadata3 base.
- [ ] Qualify the combined route when its owning implementation and fixtures exist.

Anscombe owns `src/apply/metadata.rs`, necessary `ControlRow` borrow/Clone adaptations in `src/apply/checkpoint.rs`, and focused tests.
His narrow `src/apply/mod.rs` scope imports the shared anchor, adds `ControlRow.anchor: Option<SnapshotAnchor>`, and removes Copy while retaining Clone.
The [metadata3 base](../target/verify/metadata3-base-jfg6sh1k/freeze.json) is a new ordinary source copy with only `TARGET_METADATA_VERSION` changed from 2 to 3 against the preserved State baseline.
Its original `ControlRow` remains available for meaningful behavior RED before the coordinated declaration and codec changes.
The original Native4 component and earlier author bases retain target metadata version 2.
Root's separate primary integration uses target metadata version 3 after State handback.
Copy the preserved base and use an isolated Cargo target; do not build against partially written primary State files or modify the earlier frozen export.
The shared anchor is the single `crate::contracts::SnapshotAnchor` definition, with no duplicated fields or identities.
No PostgreSQL run, native rebuild, or global strict-lint claim is authorized by this prerequisite's scoped test window.
No commit may pass through the unresolved global strict-lint gate.
The [metadata3 intake](../target/verify/metadata3-base-jfg6sh1k/final-intake.json) verifies the frozen four-file author delta, all 94 preserved source hashes, the emitted binary, and test logs.
The observed RED retained nine existing passes and five intended runtime failures before production changes.
The final author run passed 21 metadata tests and 10 authority tests, with no failures or ignored cases.
Discovery matches the existing 18 live authority cases.
The first intake kept all four primary files unchanged and confined its target-version overlay to the preserved test copy.
The [independent metadata3 review](../target/verify/metadata3-critical-review-jwy9tk7x/review.json) found no critical defects in the frozen row, codec, CAS, or authority-boundary changes.
The reviewer checked retained evidence without rerunning tests; the later primary version-constant integration has its own receipt.
The [native preflight](../target/verify/metadata3-native-2eo6vjtc/preflight.json) verifies a new ordinary copy with all 94 author blobs unchanged and eight missing harness/native files added from the qualified extraction snapshot.
Native verification, a fresh isolated compile, exact 18-case discovery, and the loaded libpq identity passed before the sole producer lease was granted.
That regression copy uses the preserved Native3 state with target metadata version 3; it does not qualify Native4 or the combined COPY route.
The [metadata3 native intake](../target/verify/metadata3-native-2eo6vjtc/root-intake.json) records all 18 authority cases and both COMMIT fault validations passing on PostgreSQL 14.24.
Source, binary, native artifact, archive, and log hashes remain bound; the owned PostgreSQL cleanup completed and the lease was returned.
The unchanged harness removed transient raw fault JSON after validation, so the receipt retains its validation footer and exact log rather than separate raw fault files.
No installed-anchor native fixture is claimed by this regression.
The [Native4/metadata3 integration receipt](../target/verify/native4-metadata3-integration-07g649mo/primary-integration.json) binds the exact four reviewed apply files and the narrow State version/fixture delta now in primary.
The old version-2 constant failed the prior-version refusal control; the integrated candidate passed 96 State, 21 metadata, and 10 checkpoint tests and discovered the unchanged 18-case authority inventory.
Anscombe's [installed-anchor test base](../target/verify/native4-metadata3-integration-07g649mo/installed-anchor-readiness.json) is preserved separately from all active catalog, transfer, and store work.
The [installed-anchor native intake](../target/verify/metadata3-installed-root-xsln2nrm/root-intake.json) records all 18 cases and both COMMIT fault validations passing on PostgreSQL 14.24 with Native4 and metadata3.
The separate NULL-projection control fails only the intended oversized-token assertion, with 17 other cases passing.
The reviewed test append is integrated; production metadata bytes are unchanged, and the synthetic anchor values do not authenticate a source receipt or prove COPY.
The combined owning route remains open; the earlier pure integration checks do not claim PostgreSQL execution or a global strict-lint result.

## Immutable COPY plan store

- [x] Save the [approved store-only core and literal protocol](../docs/superpowers/plans/2026-11-07-immutable-copy-plan-store.md) without importing the superseded larger joint draft.
- [x] Check the protocol against the codec, pinned SQLite implementation, and actual State APIs under the parent's delegated transcription authority.
- [x] Confirm Sagan's usable namespace handback before dependency and module integration.
- [x] Implement and test only `src/copy_plan.rs` and `src/copy_plan/tests.rs` under Confucius's ownership.
- [x] Move the exact locked bundled SQLite dependency to the normal graph after the real module exists and can use the namespace.
- [x] Apply build-policy refusal and runtime SQLite identity checks, then register the real private module and update affected guides.
- [x] Obtain separate critical review of the frozen store implementation and retained test evidence.
- [x] Run the focused Root-integrated checks, 153 tests, warm-cache override control, and local release linkage check.
- [x] Obtain independent review of Root's dependency, build-policy, module, and guide integration.
- [x] Qualify the frozen integrated Linux arm64 build, focused tests, override refusal, and native release linkage.
- [x] Qualify the local Linux arm64 runtime image for the integrated store dependency.

Root owns Cargo, build policy, module registration, and affected guides; Confucius owns the new store module and focused tests.
Keep versions, features, and the lockfile unchanged; redb remains development-only.
Reuse the [dependency assessment](../target/verify/sqlite-runtime-assessment-w1h8jhzr/assessment.json), preserving its recorded advisory-snapshot limits.
The implementation must use Native4's actual draft and pin, with no stub, alternate lock, or State dependency on PG or SQLite.
This store holds immutable plans and does not select the WAL backend or release source, catalog, COPY, target anchor, or CLI implementation.
The [author's validation base](../target/verify/copy-plan-author-base-61purb58/readiness.json) preserves Native4 with target metadata version 2 and only the exact private Cargo dependency-category change.
The [store handoff](../target/verify/copy-plan-author-xnn8ff7h/handoff.json) records 23 tests passing before and after six separately built negative controls.
Those controls cover table-shape mismatch, record CRC, aggregate CRC, zero point, SQLite lock loss, and swallowed close failure.
The author's production-library Clippy check passes with unsuppressed staging warnings; it does not cover the later all-targets gate.
The [critical store review](../target/verify/copy-plan-critical-review-ewxqb0m4/review.json) passed without additional test runs.
Root's broader all-targets Clippy check found one platform-dependent lock-flag conversion in the store tests.
The owner fixed only that line, and the [bounded independent review](../target/verify/copy-plan-portability-review-lnfy0lq4/review.json) accepted the Darwin and Linux type reasoning without claiming a Linux execution.
The [Root integration receipt](../target/verify/copy-plan-root-integration-cfh43gip/root-integration.json) records all-targets check and `-D clippy::all`, plus 3 build-policy, 23 store, 96 State, 21 metadata, and 10 checkpoint tests passing without failures or ignored cases.
An actual warm Cargo target rejected empty `CFLAGS` and succeeded after it was removed; the local release binary linked the controlled libpq and reported native version 180006.
The [Linux prerequisite receipt](../target/verify/copy-prerequisite-linux-guwwlabj/root-intake.json) records 153 focused tests, the warm override refusal, and actual controlled-library release linkage on Linux arm64.
All 110 source files and 20 native files remained unchanged; the artifact was reused without a native rebuild.
The [runtime-image receipt](../target/verify/copy-store-image-edsfxmhh/root-intake-v2.json) binds a fresh Linux arm64 build and all 23 smoke checks to the frozen prerequisite source plus six unchanged image inputs.
The exact archive, source, and native receipt remained unchanged through PostgreSQL 17.11 execution, signal cancellation, and owned-resource cleanup.
The smoke checks execute existing CLI and E1 COPY behavior; direct plan-store operations are covered separately by the 23 store tests in the integrated prerequisite gates.
This private artifact has no Git revision label and does not contain the newer owning-COPY changes; it adds no amd64 or publication claim.
The lockfile is unchanged, Rust staging warnings remain, and global strict lint remains open for the local runtime work.
Poincare accepted the five frozen Root integration files and their retained evidence without repeating builds or tests; the later owning-COPY guide changes are a separate documentation delta.

## Bounded catalog and per-table transfer prerequisites

- [x] Register Herschel's exclusive `src/copy.rs` ownership and save the [mechanical extraction checklist](../docs/superpowers/plans/2026-11-07-copy-table-transfer.md).
- [x] Preserve the [ordinary test base](../target/verify/copy-catalog-prerequisite-base-lj5kynt4/freeze.json) with exact COPY and catalog baselines, Native4 State, and reviewed native-version fixture changes.
- [x] Register Copernicus's exclusive catalog ownership and save the [cursor and admission checklist](../docs/superpowers/plans/2026-11-07-copy-catalog-streaming.md), including exact state, helpers, and cleanup preconditions.
- [x] Obtain the mechanical per-table transfer handoff and independent static review.
- [x] Obtain catalog implementation evidence and independent review.
- [x] Add the checked native result-column accessor and qualify the existing five transport cases, with an intended runtime failure when the accessor discards the count.
- [x] Obtain bounded independent review of the additive result-column accessor.
- [x] Save Noether's [two-file Snapshot fixture addendum](../docs/superpowers/plans/2026-11-07-copy-catalog-fixtures.md) before the actual cursor coverage changes.
- [ ] Qualify catalog owner behavior within the existing actual Snapshot fixture groups and run the appropriate COPY regression under Root's later lease.
- [x] Save the approved owning-COPY operation plan and its final amendments, then hand off the coherent source base before operation edits.

The catalog design uses one NO SCROLL WITHOUT HOLD cursor and FETCH 128; the earlier qualified-name index-seek proposal is withdrawn.
Do not hide unsupported candidates, substitute synthetic source receipts, count ignored cases as passing, or change the E1 workset behavior.
Neither package releases a SourceSnapshot COPY operation, target COPY authority, snapshot proof, or CLI route.
The [transfer intake](../target/verify/copy-table-transfer-root-3as4az3r/intake.json) binds the frozen source and author artifacts to Poincare's independent whole-file inverse review.
One pure test passed in each version; the ignored native test and identical staging-lint errors remain outside that qualification.
The [result-column handoff](../target/verify/pg-result-columns-1dg51m1k/handoff.json) records five transport cases passing on PostgreSQL 14.24 and the intended zero-count control failure.
It preserves all existing FFI bodies and observes actual zero-row SELECT and FETCH widths.
Poincare's bounded independent review passed against that exact source, binary, and native evidence without repeating the tests.
The [final catalog intake](../target/verify/catalog-streaming-root-wbh6a8aw/intake.json) binds explicit format preservation, native-name lookup predicates, and the reviewed bounded cursor implementation.
The [final owner-fixture review](../target/verify/catalog-owner-final-review-r8w3smav/review.json) accepts the actual source-owner assertions, 259-table pagination, retained locks, zero-row field width, and sticky failure controls against the frozen two-file packet.
The [coherent prerequisite gate](../target/verify/copy-snapshot-owning-base-zsbb2wfj/native-intake.json) then passed Catalog10 and Snapshot12, including all five snapshot fault groups, on PostgreSQL 14.24.
All 110 source files, the binary, and 19 native artifact files remained unchanged, and the owned fixtures were removed before Root released the lease.
The same frozen prerequisite composition also passed Catalog10 and Snapshot12 with all five fault groups on PostgreSQL 17.11 in the [Linux gate](../target/verify/copy-prerequisite-linux-guwwlabj/root-intake.json).
These results do not qualify the new owning-COPY route.
The owning plan reuses the optional-frontier TargetTxn, borrowed SourceSnapshot one-attempt latch, private proof, reader pin, finalization, and count-publication order selected by the parent.
Gibbs owns only the exact approved plan transcription; Root owns spec, usage, source composition, and the shared qualification interfaces.

## Owning COPY and logged snapshot proof

- [x] Save the [approved operation plan](../docs/superpowers/plans/2026-11-07-copy-snapshot-owning.md), including all four ownership, failure-boundary, and reachable-fixture amendments.
- [x] Correct the [anchor specification](../docs/superpowers/specs/2026-11-07-copy-snapshot-anchor.md) and usage guides for Native4, metadata3, exact intent APIs, and target-only recovery limits.
- [x] Compile and discover the coherent ordinary base, then release disjoint catalog, COPY helper, source owner, authority, and new private fixture paths.
- [x] Integrate the independently reviewed COPY helper visibility and exact execution-guard extraction.
- [x] Integrate the independently reviewed catalog helper loans and exact target table/index identity exclusion.
- [x] Qualify the retained Snapshot12/Catalog10 prerequisites on PostgreSQL 14 under Root's sole database lease.
- [x] Run the corresponding PostgreSQL 17 prerequisite update.
- [ ] Run retained COPY40/Engine41 regression on the appropriate final owning source.
- [x] Implement and independently review the source publisher and two-pass operation, including failed cleanup and one-attempt behavior.
- [x] Implement and independently review optional-frontier target ownership, actual owned-relation exclusion, snapshot proof, and recovery.
- [x] Provide the approved test-only State count-create-failure hook and prove intact intent recovery, no temporary publication, and guard cleanup.
- [x] Obtain bounded independent review of that test-only State hook before owning live qualification.
- [x] Obtain bounded static review of the eight-group fixture, including closure observations before object or relay cleanup.
- [x] Close the adapter socket-lifetime code finding while retaining the race probe's partial scheduling limit.
- [ ] Register the exact eight live groups and required default Task/CI receipt checks without weakening earlier profiles.
- [x] Obtain independent review of Root's eight-path gate and evidence-retention integration, including bounded FIFO refusal.
- [ ] Run required positive cases and intended runtime controls on PostgreSQL 14 and 17, then close the exact integrated lint/check/test gates.

The fixed sequence is sealed publication, target acquire/confirmation, durable native binding, and an internally opened immutable reader.
The [release readiness](../target/verify/copy-snapshot-owning-base-zsbb2wfj/readiness.json) binds the saved plan, corrected spec/guides, exact source, discovery, component evidence, and five disjoint ownership handoffs.
Authority passes the verified checkpoint table and primary-index `pg_class` OIDs through admission; schema and source OIDs cannot substitute.
Every sealed entry must pass the existing Text execution restriction before the first COPY starts, while both stored format tags remain unchanged.
Both owning entrypoints close or poison authority on intent and reader errors before BEGIN.
Recovery without a live SourceSnapshot proves target COPY completion only; source identity, slot continuity, and required WAL/decoder history remain separate requirements.
The late UNIQUE fixture must observe COPY and SQLSTATE 23505 under the admitted profile; deferred-constraint mutation remains component coverage.
The [COPY helper handoff](../target/verify/copy-a2-7dozwsgu/handoff.json) passed Poincare's independent inverse review and retains three pure passes with one native case ignored.
Its E1 and transfer bodies remain identical, Binary execution remains refused, and the unchanged staging-lint failures are not a passing global gate.
The [catalog integration receipt](../target/verify/catalog-a1-root-t59qyf32/intake.json) binds the reviewed A1 blob, 55 author pure passes, and two intended runtime controls.
Generic CDC lookup order and 25 E1 function bodies remain unchanged; actual owned-table/index admission and lock behavior still require the owning native cases.
The [count-failure hook handoff](../target/verify/copy-count-test-seam-hmsq3qfz/handoff.json) records 97 State tests passing and one intended runtime failure when only the injected Create guard is removed.
Its exact inverse preserves production State bodies; format four and metadata three are unchanged.
Poincare's bounded independent review accepted the exact hook, test-only visibility, retained 97-test evidence, and intended guard-removal failure without repeating tests or database work.
The [separate reviewed composition](../target/verify/copy-owning-reviewed-8x1pilbt/integration.json) preserves actual A1/A2/B/C code and the State test hook in an ordinary 110-file source export.
B's final review accepts 15 pure passes and five intended guard failures; C's final review accepts 15 checkpoint and 22 metadata passes with its retained mode and pre-BEGIN failure controls.
Root observed B already present and applied only C's three exact reviewed blobs after checking their prior identities.
Those C replacements remain outside the published checkpoint, and D's primary child remains unregistered pending corrected fixture qualification and Root integration.
The [fault handshake](../target/verify/copy-fault-adapter-base-6jxn6pj8/handshake-v2.json) separates passive importer observation from late loss-arm and requires the exact 22 receipt phases.
The [Python fixture review closure](../target/verify/copy-snapshot-python-review-d2o5du1_/p2-closure.json) accepts exact nested source-identity types and full unsigned ranges, backed by four retained runtime refusals and 15 author tests passing.
That bounded review does not cover the Rust live bodies, shared controllers, or actual PostgreSQL execution.
The [Rust fixture correction review](../target/verify/copy-snapshot-rust-review-21mg9b_m/p2-static-closure.json) accepts backend-closure checks before reuse, Drop, or relay cleanup, including recovery through the same corrupt reader.
The [adapter correction review](../target/verify/copy-select-race-closure-review-v1yoi05x/review-v2.json) accepts joining the upstream reader before closing its socket while retaining the held backend until release.
Its observed RED and inverse failures were only partly schedule-forced; they do not establish a deterministic race reproduction.
Root's [corrected private gate handoff](../target/verify/copy-root-gates-eyhg2mvr/candidate-v3/handoff.json) passed 169 tooling tests and 33 verifier tests on both Python 3.15 and 3.9 against a frozen 152-file source export.
The unchanged 150 files retain the prior 105-test harness, syntax, shell/YAML, and three omission-control evidence with its original source attribution.
That eight-path delta adds exact eight-case and 22-phase enforcement, default database gating, and raw-evidence retention before fixture and container cleanup on success and failure.
Partial observations remain unqualified, unsafe envelopes are not copied, and the original failed child exit remains intact.
The [bounded independent closure](../target/verify/copy-root-gates-fifo-closure-review-ucsfifba/review.json) accepts the nonblocking receipt open after a real FIFO timeout, a passing refusal case, and a flag-removal timeout control.
It preserves the same-descriptor regular-file check, original child exit, and fixture cleanup without changing State or store inode checks.
The [first owning PostgreSQL 14 run](../target/verify/copy-root-gates-eyhg2mvr/candidate-v3/pg14/report.json) executed eight cases: six passed and two failed, with zero ignored.
Admission failed the canonical plan's greater-than-16-KiB fixture assertion; atomic COPY failed with SQLSTATE 42601 during dynamic fixture setup.
All 14 observed wire receipts and the failed child exit were retained as unqualified evidence before successful cleanup.
The [reviewed three-file correction](../target/verify/copy-fixture-independent-review-188eeca0f5e14718887e933ef665e1a3.json) splits fixture setup statements and expands canonical metadata while preserving both size assertions.
The [corrected PostgreSQL 14 run](../target/verify/copy-root-gates-eyhg2mvr/candidate-v4/pg14/report.json) executed eight cases: seven passed and admission failed, with zero ignored.
Its `late_refusal` phase stopped at the exact error-message assertion before helper completion; 17 safe raw receipts and one generic capture error remain unqualified evidence.
The observed message is `CDC target relation metadata does not match the source relation`; the assertion expected `source and target column layouts or types are incompatible`.
Pasteur traced the actual call path to the earlier CDC metadata refusal and corrected only the expected literal plus a fixture-shape regression.
Harvey's [bounded v5 review](../target/verify/copy-admission-v5-independent-review-ef7d99abd3cf4da9b65f06e0a9df1a22.json) passed the exact two-file delta, retained 17 offline passes and two intended mutation failures, and verified that the other 150 source files were unchanged.
The [fresh v5 PostgreSQL 14.24 run](../target/verify/copy-root-gates-eyhg2mvr/candidate-v5/pg14/report.json) passed all eight cases with zero ignored and validated all 22 actual wire receipts.
All helpers were reaped, no capture errors occurred, owned-directory cleanup passed, and the source and loaded controlled-native hashes were unchanged.
This run includes the late admission zero-COPY observations; it does not fill the missing observation in the retained failed v4 run.
The [Linux arm64 PostgreSQL 17.11 run](../target/verify/copy-owning-linux-s2eb_yz8/root-intake.json) passed the same eight cases and 22 phases from the identical 152-file v5 source, plus 33 verifier tests.
It verified the existing controlled Linux native artifact, bound the actual test binary and loaded library, and copied raw evidence before removing the owned container.
Source and native hashes stayed unchanged, all helpers were reaped, and temporary fixture cleanup passed.
Five semantic controls produced the intended native assertion failures on both [PostgreSQL 14](../target/verify/copy-owning-plan-controls-nrl8f78a/assertion-review.json) and [PostgreSQL 17](../target/verify/copy-owning-linux-controls-n0kimb1t/summary.json): erased format tags, transfer during admission, premature page-boundary EOF, an omitted transfer, and read-only recovery.
Each control changed one production file in a disposable export, preserved the test oracle, compiled successfully, retained its failed observations, and completed owned-resource cleanup.
The page-boundary control proves truncation detection, not failed-FETCH handling; secondary helper failures are not the basis for accepting a control.
Existing reviewed transaction-mode controls retain their pure before-SQL scope and exact unchanged authority source; these were not rerun or presented as database tests.
The separate [close-erasure trace](../target/verify/copy-owning-close-control-65alrg2j/closure-timing-trace.json) found a fixture timing gap: the closure poll can outlast the helper's cleanup deadline.
Its six-pass/two-failure result detects a later poison-state mismatch but does not prove that operation failure closed the backend before helper cleanup.
Pasteur owns the approved private v6 correction in the three D-fixture files and two relay helpers, with an actual EOF-or-Terminate witness before cleanup and an observation budget inside the original helper deadline.
The [accepted helper contract](../target/copy-snapshot-harness/close-timing-v6-vtfgmz6a/interface-proposal-v2.json) preserves the 22-phase receipt schema; its v5 helper delta stays separate from Fermat's protocol changes until Root composes and reviews the overlapping hunks.
The [five-file v2 handoff](../target/copy-snapshot-harness/close-timing-v6-vtfgmz6a/implementation-v2/handoff.json) retains 20 D tests, seven helper tests, intended offline mutation failures, and exact eight-case discovery.
Harvey's [bounded independent review](../target/verify/copy-close-timing-v2-independent-review-f3bd0c9ae95d40d2a4dc9c6777cd9d58.json) passed the frozen timing correction.
Fresh [PostgreSQL 14](../target/verify/copy-close-v2-root-_zh4ai8q/pg14-assertion-review.json) and [PostgreSQL 17](../target/verify/copy-close-v2-root-_zh4ai8q/pg17-assertion-review.json) gates each passed eight cases and 22 wire phases.
The same two-guard erasure produced six passes and two intended closure-assertion failures on each server.
The relayed recovery failure retained a live relay and backend with cleanup inactive and about 15 seconds of helper lifetime remaining; the direct advisory-count case makes no relay-witness claim.
Raw failed-control receipts remain unqualified, and all owned fixtures were removed after evidence capture.
The primary gate files remain separate from these exports; expanded PostgreSQL 16/18 admission coverage and the global strict gate remain open.
This package adds no public CLI route, source acknowledgment policy, WAL backend selection, or throughput claim.

## PostgreSQL matrix qualification

- [x] Assign read-only PG16 research to Hegel, PG18 research to Parfit, and the minimal shared matrix proposal to James with explicit `reasoning_effort=xhigh`.
- [x] Review exact version, expiry SQLSTATE, privileges, catalog, and protocol evidence from those packets and accept Mencius's two corrections.
- [x] Pass the corrected owning PostgreSQL 14 baseline and release three disjoint implementation packages in private ordinary exports.
- [x] Complete the bounded independent review of the frozen catalog package.
- [x] Close the bounded CI wrapper-linkage finding through independent re-review.
- [x] Run isolated PostgreSQL 16/18 SQL diagnostics and retain failures separately from qualification.
- [x] Independently review the catalog diagnostic corrections and profile/protocol fixes.
- [x] Compose A2, B3, C2, and D5 with checked shared-helper hunks and reviewed empty-exception diagnostics.
- [x] Implement the separate declared-version COPY phase consumers and pass their offline harness and CI checks.
- [x] Independently review those consumers and the final admission fixtures before composing the new 28/29-phase native gate.
- [x] Correct the missing pre-child role declarations exposed by the first combined run, then execute all seven version profiles from a fresh reviewed source.
- [ ] Add owning-COPY collation, index-override, empty non-C database, and PostgreSQL 18 historical-NULL admission observations while retaining all eight groups and the original 22 phases.
- [ ] Locate the original PostgreSQL 18 setup assertion if it recurs; passing reruns do not close it.
- [ ] Complete expanded admission and remaining protocol controls on PostgreSQL 16 and 18 with explicit expected versions and Root-owned database leases, while retaining PostgreSQL 14 and 17 coverage.
- [ ] After same-version baselines, qualify one PostgreSQL 16 source to PostgreSQL 18 target migration without expanding to a full Cartesian matrix.

The previous Ohm and Erdos workers are stopped; their work must not be attributed to the replacement workers' effort setting.
Their research is complete; implementation has separate owners and private exports.
Their task identities are Hegel `01a1b588-af78-7f02-b3ac-12af38eb9543`, Parfit `01a1b588-ae38-7603-8642-751d224ac26f`, and James `01a1b588-b0b6-7cd0-89df-e45dd470dc8d`.
Harvey `01a1b58b-1a97-7b92-98c3-d5d7fee52b4e` accepted the prior three-file fixture correction at explicit `xhigh` effort; that bounded review did not qualify its later live run.
Use the private gate export's actual manifest and handoff paths; primary gate files do not yet contain that reviewed integration.
The [combined proposal](../target/verify/pg-matrix-james-m19xy9ht/proposal-v3.json) retains PostgreSQL 17 packaging and separates PostgreSQL 16 and 18 qualification jobs.
The [accepted review](../target/verify/pg1618-preimpl-review-qrmshcf3/review.json) requires a fail-closed effective database, column, and index collation boundary for the C/UTF8 profile before COPY, with unchanged target data and checkpoint controls.
Sartre `01a1b5a8-f041-7af2-80b7-9c1476603b33` froze catalog validation and associated catalog tests in one file.
Euler's [bounded independent review](../target/verify/pg1618-catalog-sartre-zz77qqja/review-independent-catalog-a-7ccfbcff38a34c41bfc4b37c983e4994.json) found no P1/P2 issue; it verified legacy E1 equivalence and the new admission checks without running PostgreSQL.
The approved private interface is `catalog::configure_qualified_session(&mut PgConnection, &IoControl) -> Result<()>` for CDC and owning COPY.
It preserves the legacy E1 `configure_session` behavior and error order, reuses the settings tail without an extra round trip, and applies the qualified column/index policy only to the new profile.
Fermat changes the two owning `copy_into` session calls after the imported-transaction checks; source snapshot ordering still requires explicit proof.
Fermat `01a1b5a8-f17e-7d01-b02d-2fcef8beb02f` owns version guards, source profiles, and protocol observations from the actual test and normal CLI connections, including necessary harness producers.
Pauli `01a1b5a8-f2b9-7e20-b6c5-f992b8a72ffc` owns the six CI matrix paths and consumes Fermat's explicit observation contract.
Those paths are `tools/ci/Containerfile.qualification`, `.github/workflows/ci.yml`, `tools/ci/linux.sh`, `tools/ci/test_linux.py`, `tools/ci/verify.py`, and `tools/ci/test_verify.py`.
All three implementation workers use explicit `reasoning_effort=xhigh`, start from the frozen v4 manifest in unique ordinary exports, and have no primary-source, D-fixture, PostgreSQL, container, or native-build lease.
Root will apply the reviewed v5 fixture delta during composition; no unreviewed version patch enters that baseline.
Reject unsupported PostgreSQL 18 generated, temporal, unenforced, unvalidated-not-null, and index shapes before COPY; do not add ICU support.
The native protocol values `30000` and `30002` differ from startup wire words `196608` and `196610`; record actual observations under distinct names.
Bind the native `30000` observation to each tested executable and connection, and the `196608` startup observation to the normal CLI's actual relay connection.
Test-only probes do not qualify an unwitnessed normal executable; preserve protocol-3.2 refusal without a production clamp, new runtime API, or dependency.
Derive required stage and case inventories from the selected private source, including nested Task database gates; public foundation counts do not define acceptance for a newer candidate.
Preserve each required gate and add a missing-stage control for any new required private stage.
The [version-two observation contract](../target/verify/pg1618-profile-protocol-xzwj1dev/witness-contract-v2.json) binds raw fixture identities, executable kinds, the used-binary subset, and exact dynamic endpoint roles; its earlier version remains retained.
The [B2/C2 source intake](../target/verify/pg1618-root-intake-drvdlp4e/receipt.json) verifies each frozen 153-file manifest and the bounded deltas without claiming behavioral qualification.
B2 adds the missing controlled standby, fsync-off, and restricted-user fixture declarations before execution; its four-file delta includes the harness documentation.
The active contract remains version two; the descriptive version-three artifact does not change its API or schema.
Hypatia's [independent B2 review](../target/verify/independent-b2-review-936852a2115f4a738ac47e45f490994e.json) required exact required-source database bindings and protection against symlinked capture ancestors.
The [bounded B3 re-review](../target/verify/independent-b3-delta-review-9578f0b4c5494a8681a7d64748150f62.json) closes both findings in three files, preserving the active version-two contract and the other 150 B2 files.
The [CI review](../target/verify/c1-ci-review-xkjdcbir/review.json) found that the process-only integration wrapper must have a file hash without a required libpq link.
Pauli's [two-file C2 correction](../target/verify/pg1618-ci-pauli-t1_l6wwc/review-c2/freeze.json) retains mandatory native binding for the actual CLI and library test executable.
The [bounded C2 re-review](../target/verify/c2-ci-delta-review-zxp0ut92/review.json) closes that finding and verifies the retained author evidence of 187 CI tests and 115 harness tests under each declared version environment; those are offline checks.
Pauli tests the actual frozen Python validator with synthetic captures; those offline tests do not qualify live protocol behavior.
The existing controlled service/passfile test remains required, and no new conninfo API or blanket service refusal is authorized.
Pasteur owns the separate admission-fixture extension for empty/non-C databases, collation/index overrides, and PostgreSQL 18 historical NULL values under unvalidated NOT NULL constraints.
The approved [literal plan](../target/copy-snapshot-harness/owning-admission-g2yac1nd/literal-plan.json) retains the eight groups and all original 22 phase IDs, then adds six common phases and one PostgreSQL 18 phase.
Its strict inventories are 28 phases for the declared PostgreSQL 14/16/17 pairs and 29 for PostgreSQL 18; auxiliary observations cannot satisfy the required main-source witness.
The private author export has four explicit dependencies: the reviewed catalog, snapshot, source-profile, and capture-declaration files; its deliverable remains the three D-fixture paths.
The [final admission review](../target/verify/owning-admission-D3-independent-review-aeb4425c11a34f279a4dd9866a331c79.json) accepts the frozen three-file extension and four explicit dependencies; its new 28/29-phase database qualification remains open.
The separate [Root consumer handoff](../target/verify/copy-phase-consumer-7zx4sop3/handoff.json) changes only the runner, CI verifier and tests, Task description, and testing guide.
Two regression tests first reproduced acceptance of stale 22-phase success receipts, then passed with the exact declared phase set required.
The selected export passed 137 harness tests and 191 CI tests with warnings treated as errors under each declared PostgreSQL 16.15 and 18.6 environment, without starting PostgreSQL.
Its fixture and test inputs match the final admission package, but the new Rust admission body must still join the eventual native composition.
The [consumer review](../target/verify/consumer5-ci-review-lcnz607g/review.json) accepts the five-file change; shared harness-document reconciliation and new live coverage remain open.
The [composition review](../target/verify/owning-admission-composition-independent-review-76888edf4dce438388cf2580f841c759.json) accepts the exact eight-path D3 and consumer overlay on the successful 154-file baseline, preserving the other 146 files.
The [new private helper](../target/verify/pg1618-admission-baseline-yyxj3pg8/handoff.json) forwards the fixed declared numeric version alongside its string; five offline receipt tests reproduced the omitted-argument failure and then passed.
Its bounded helper review and fresh database gate remain separate from the old 22-phase evidence.
The old 22-phase observations retain their original source attribution.
Catalog-only tests do not prove zero COPY starts or unchanged checkpoint state.
The [first A+B2 SQL diagnostics](../target/verify/pg1618-diagnostic-ai6kypc5/root-findings.json) used one frozen 153-file source, hash-bound binaries and controlled libpq, and isolated arm64 server images built from the pinned PostgreSQL archives.
PostgreSQL 16.15 passed Catalog 9/10 and SourceSnapshot 12/12; PostgreSQL 18.6 passed Catalog 8/10 and initially stopped before Snapshot test execution with an unlocated setup assertion.
The retained traceback run localizes catalog failures to the role-membership refusal control and PostgreSQL 18 temporal-table setup.
Its later Snapshot 12/12 result and five passing setup-only observations do not resolve or erase the original setup failure.
Protocol capture was disabled for these diagnostic SQL runs, all owned containers were removed after evidence capture, and no full seven-profile or strict gate result follows from them.
Euler's [bounded A2 re-review](../target/verify/pg1618-catalog-sartre-a2-cuiadb7u/review-independent-a2-5a0733e075064aa683838c42a415d4a9.json) accepts the test-only lock prerequisite and valid temporal-table setup corrections, with the production prefix unchanged.
The subsequent diagnostic export passed Catalog 10/10 and SourceSnapshot 12/12 with five fault receipts on both [PostgreSQL 16](../target/verify/pg1618-a2-diagnostic-14jkfikv/run-pg16.15/output/report.json) and [PostgreSQL 18](../target/verify/pg1618-a2-diagnostic-14jkfikv/run-pg18.6/output/report.json).
Those runs remain diagnostic-only and do not resolve the earlier blank setup exception.
The [independently reviewed reporter correction](../target/verify/independent-empty-exception-review-6f41afcd6d684630b402c24120a4c754.json) identifies an empty exception's class and source location without locals or connection details, preserving known messages and exits.
The [composition review](../target/verify/composition-seam-independent-review-ec77e1664a914ac2ae1e31e19074cc56.json) accepts the combined B/D helper and one test's startup arrangement, with exact helper inverses and unchanged cleanup assertions.
The combined source passed formatting, library compilation, 133 harness tests, and 187 CI tests under each declared version environment.
The [first seven-profile attempt](../target/verify/pg1618-reviewed-composition-rrhmumt4/baseline-failure-handoff.json) passed Engine 41/41, two authority faults, and four replication faults on both PostgreSQL 16.15 and 18.6, then failed protocol capture validation.
Each version retained 190 native connection observations from all 41 cases, but the pre-run role declarations omitted the quoted writer and the numeric owner-OID writer used by `missing_privileges`.
The strict validator rejected those two records; the other six profiles did not execute.
Both reports remain failed, and all raw captures were retained before owned-container cleanup.
The [B5 closure review](../target/verify/independent-b5-delta-review-bbfde14b2af54431bb8d5c728de0fd25.json) accepts exact pre-child owner-role declarations and rejects malformed numeric-only auxiliary declarations before processing observations.
The [first B5 run](../target/verify/pg1618-b5-baseline-ll6l8new/run-pg16.15/output/report.json) remains failed: its private runner combined compiler stderr with canonical stdout receipts, then rejected extra SourceSnapshot matches after the database commands passed.
The [independently reviewed runner correction](../target/verify/stdout-runner-review-8t5am5qi/review.json) validates stdout alone, retains both raw streams, and still rejects nonzero commands and missing, duplicate, prefixed, or incorrect receipts.
The [fresh completion receipt](../target/verify/pg1618-stdout-baseline-hmzes653/completion.json) passes all seven profiles on both PostgreSQL 16.15 and 18.6 from the unchanged 154-file source and controlled native inputs.
Each version passed Engine 41, E1 COPY 40, Catalog 10, SourceCatalog 3, SourceSnapshot 12 with five faults, JournalSerial two with two process kills and two committed-Unknown controls, and owning COPY eight groups with the original 22 wire phases.
Authority two and replication four fault receipts also passed.
Protocol evidence covers 516 native handles and two normal CLI COPY connections per version; the receipt explicitly lists other CLI routes as unqualified.
The database tasks took 59.887 seconds and 51.171 seconds, with 139.173 seconds of total host time including build, discovery, and cleanup.
These are Linux arm64 component results, not global strict, new 28/29-phase, cross-version, amd64, hosted CI, or alpha qualification.
The original PostgreSQL 18 setup failure has no established cause, new 28/29-phase live coverage is open, full local WIP remains strict-lint red, and the production WAL runtime and restart path remain unqualified.
The [admission helper review](../target/verify/admission-helper-seam-independent-review-c8f0c77b9c7e477b9a6f8538cf6eec68.json) accepts the declared-version argument and unchanged stdout, exit, ownership, and cleanup checks.
The [first expanded run](../target/verify/pg1618-admission-baseline-yyxj3pg8/failure-summary-v2.json) stopped on PostgreSQL 16.15 before the owning-COPY child because the locale setup facts differed from their declared prerequisite.
Five preceding Rust profiles passed, but the final protocol gate was not reached; owning COPY, E1 COPY40, and PostgreSQL 18 did not execute.
The failed report, raw streams, source identity, and cleanup proof are retained.
The original failure did not retain locale values.
The [separate locale probe](../target/verify/d28-locale-root-w78arydd/receipt.json) recorded eight owned PostgreSQL 16.15 database facts: both requested POSIX databases became `C/C`, while all six requested C databases matched.
Those two negative fixtures therefore fall inside the supported profile; a genuine unsupported-locale fixture and its reviewed regression are required before another admission gate.
The probe executed no COPY child and preserved the failed source and report.
The [two-version availability probe](../target/verify/d28-cutf8-root-bwye3ldu/result.json) separately verified PostgreSQL 16.15 and 18.6 with exact `UTF8/C.UTF-8/C.UTF-8/libc` database facts.
Those diagnostic probes did not execute COPY or qualify the admission inventory.
The [fixture correction](../target/verify/d28-locale-fixture-z3c7osqa/handoff.json) changes four locale literals, adds prerequisite regressions, and updates the testing guide while preserving all production source and 28/29 phase IDs.
The [new Root export](../target/verify/pg1618-admission-locale-7g7c8ynx/intake.json) binds that three-file delta to source manifest `7e91bcc2`.
The [independent locale review](../target/verify/d28-locale-independent-review-5bdf1b874e05409a9720573c23ac3223.json) passes the bounded correction, retaining the effective-provider evidence limit.
The [fresh PostgreSQL 16 run](../target/verify/pg1618-admission-locale-7g7c8ynx/failure-summary.json) reached owning COPY and failed one of eight groups, with zero ignored cases, at `src/snapshot_copy_live_tests.rs:999`.
The [case binding](../target/verify/pg1618-admission-locale-7g7c8ynx/failure-case-binding.json) identifies `admit_column_source` in `publish_refusal`: `checked_copy_snapshot().unwrap()` receives `state-foreign: native plans contain unrecognized entries` after the intended source refusal.
The retained source creates a pending draft before consuming the rejected relation; its existing State regression requires abandoned drafts to block checked access and reopen.
The test did not reach the new phase's wire assertions; 22 retained wire files and one capture error remain unqualified, and E1 COPY40 and PostgreSQL 18 did not execute.
The database task took 37.041 seconds and the host run took 52.338 seconds; source and native bytes remained unchanged, owned temporary resources were cleared, and the container was removed.
Root stopped the sequence and handed the exact failure to the parent before any correction or retry.
Kuhn the 2nd's [read-only diagnosis](../target/verify/native4-copy-plan-refusal-diagnosis-6uguc2cy/diagnosis.md) confirms the retained-draft contract and the failing subcase without a new database probe.
The parent released only the admission test assertion correction in a new export: publisher refusal must assert the exact `BadState` and explicitly present-null `copy_snapshot`, while target refusal preserves the existing checked `Some(intent)` equality.
Both paths require `copy_complete` to be present and null; all data, checkpoint, poison, retry, and wire assertions remain.
The [assertion-only handoff](../target/verify/native4-admission-assertion-fix-yf6v_6_6/handoff.json) changes one Rust test blob and preserves the other 153 files, all production behavior, and the eight-group 28/29-phase inventories.
The [Root export](../target/verify/pg1618-admission-assertion-kyyrto_7/intake.json) binds source manifest `25eee6be` to unchanged runners and native inputs, and the [independent assertion review](../target/verify/native4-admission-assertion-independent-review-2712f378cbc740c1a0551c273105bd3e.json) accepts the exact test-only correction.
The [fresh component results](../target/verify/pg1618-admission-assertion-kyyrto_7/completion.json) record PostgreSQL 16.15 passing all seven profiles, including eight owning COPY groups and 28 wire phases, in 54.559 database-task seconds and 67.780 host seconds.
PostgreSQL 18.6 stopped before journal, owning COPY, and E1 COPY execution at `replication_fixture.py:28`, which expects zero replication slots after the temporary plugin-probe session.
Its actual slot count was not retained; the cause remains unknown, and no retry or production change follows from the assertion alone.
The failed database task took 16.733 seconds and the host run took 29.778 seconds; both version receipts retain unchanged source/native identities and completed owned-resource cleanup.
The [frozen v2 lifecycle diagnostic](../target/verify/pg18-lifecycle-root-j84a1a29/observations.json) did not reproduce that failure on the same PostgreSQL 18.6 image, source, and native library.
It observed one bound temporary slot while the client was alive, then zero slots on the first query after reaping that client; it did not perform later polling.
The [ownership receipt](../target/verify/pg18-lifecycle-root-j84a1a29/receipt.json) records completed cluster cleanup, removal of the labeled container, and unchanged inputs in 1.752 host seconds.
This observation does not establish the cause of either retained setup failure and does not qualify a suite retry.

## Checked WAL-store shutdown

- [x] Freeze the author's consuming checked-close implementation and scoped tests in the admitted experimental backends and existing journal bridge.
- [x] Independently review retained ownership on failure, successful retry, and the bridge's explicit error handling.
- [x] Run the exact two-case journal bridge on an owned PostgreSQL 14 fixture with controlled native linkage.
- [x] Publish the independently reviewed six-file experimental checkpoint after its exact local gates passed.
- [x] Review, qualify, and publish the test-only raw-directory-lock correction while preserving the failed hosted receipt.
- [x] Merge the reviewed checkpoint through normal GitHub checks after fresh hosted gates pass, preserving primary WIP.
- [ ] Attribute the retained earlier amd64 database failure without treating later passing runs as a diagnosis.
- [ ] Integrate checked close into the next reviewed production-consumer slice without selecting a backend default.

Arendt authored the private five-file cut in `bench/storage/config.rs`, `bench/storage/sqlite.rs`, `bench/storage/journal.rs`, `tests/storage_recovery.rs`, and `src/snapshot_journal_live_tests.rs`.
The approved signature is `close(self) -> std::result::Result<(), (Box<Self>, Error)>`.
A checkpoint failure returns the owned store and role guard before close; a SQLite close failure restores its live connection for retry.
Only confirmed resource closure releases the role guards, and the tests must retain the role across a busy statement, then finalize it and retry.
The [author handoff](../target/verify/wal-close-d1bpahv5/handoff.json) records 171 storage tests and one bridge test passing, plus checked-close erasure and borrowed-view controls.
Galileo's [bounded independent review](../target/verify/wal-close-independent-review-t3i_23im.json) passed the five-file implementation and retained evidence.
The [fresh Root PostgreSQL 14 gate](../target/verify/wal-close-root-p2f7lq54/pg14/report.json) passed both journal bridge cases, two pre-COMMIT process kills, and two committed-Unknown reply/fence controls with source, binary, and loaded-native identities retained.
This cut follows the [next-WAL ownership brief](../target/verify/next-wal-design-2qnffx2d/packet.md) and does not qualify a production records adapter, CLI route, or backend default.
The separate [Native5 plan](../target/verify/native5-interface-sb8cbd9i/approved-plan-v2.md) permits private filesystem component work with required records provenance and a creation-time source timeline.
The [independent namespace review](../target/verify/native5-review-locke-29e6f3d769bf.json) accepts the frozen filesystem component and retained 110 State, 16 snapshot, and 23 plan-test results.
The real backend initializer and bridge remain a separate private implementation and qualification gate; pure test fixtures do not supply production authority.
Gate 2 must supply the frozen NativeStore lifetime, owning-error, head/read/receive, and actual JournalSerial bootstrap interfaces.
Its [adapter review](../target/verify/gate2-adapter-review-locke-ae72595a86f9.json) and [core review](../target/verify/core-relocation-review-7227a7f7fcb840a8940e372588e4ccff.json) found one coordinated admission failure: a handle returned for cleanup after a required-prefix gap can still append records.
The parent released the bounded V2 correction, and its [core review](../target/verify/gate2-v2-core-review-e2771c90033247c984d0493b65ab7304.json) accepts the small checked-close extraction.
The [V2 adapter review](../target/verify/gate2-v2-adapter-review-locke-1730b58b90ae.json) closes failed-admission access but finds a separate cleanup defect: namespace loss after successful initialization prevents checked close from reaching its held backend resources.
The frozen V1 and V2 evidence remains unchanged.
The parent accepted Rawls the 2nd's close-outcome matrix and released a narrow V3 change in the adapter, its tests, and the affected durability sentence.
The correction separates the immutable synchronization plan from sticky proof failure while preserving real ownership and the existing core poison policy.
That adapter correction does not change schema, context compare-and-swap, or ACK policy; Gate 3 has separate ownership.

- [x] Reproduce successful initialization followed by namespace loss for both admitted profiles.
- [x] Perform checked synchronized cleanup through held resources and retain the same synchronization plan on every failed-close retry.
- [x] Refuse publication proof after namespace failure, even when resource cleanup succeeds.
- [x] Independently review the correction before Root integration or live qualification.
- [x] Prepare the exact Native5 composition and explicit fixture overlay, preserving the earlier Native4 bridge evidence.
- [x] Run the real bridge and all seven Native5 component profiles on the frozen PostgreSQL 16.15 and 18.6 candidates.
- [ ] Complete global strict lint, aggregate integration checks, and the real Gate 3 consumer before broader runtime qualification.

The [V3 initializer review](../target/verify/gate2-v3-initializer-review-locke-d4f42143e412.json) closes the lifecycle finding and verifies the retained 348 scoped author passes and negative controls without rerunning them.
That review leaves the existing core poison policy unchanged; Native5 live and global qualification require separate gates.
The [static source comparison](../target/verify/native5-composition-intake-uqigtuvs/comparison.json) finds 38 differing common paths between the frozen Native5 candidate and the qualified Native4 version/admission source; a whole-source overlay would discard reviewed changes.
The [reviewed-hunk composition](../target/verify/native5-gate2-composition-ugtgl557/composition.json) contains 165 files and preserves 141 original paths byte-for-byte, including the version, protocol, and admission inputs outside the approved changes.
Its [exact forward/inverse proof](../target/verify/native5-gate2-composition-ugtgl557/roundtrip/receipt.json) reconstructs both the 154-file Native4 source and the 165-file composition.
Fresh [offline component checks](../target/verify/native5-gate2-composition-ugtgl557/component-tests/receipt.json) pass 348 tests with zero ignored cases and the verified native library; formatting and compilation pass, while 353 compiler warnings remain.
The [separate count and guide delta](../target/verify/native5-gate2-composition-ugtgl557/integration/root-delta.json) uses the discovered 171-test storage inventory and passes 42 Task tests after a meaningful failure control.
The fixture V2 and Root child-guard corrections passed their [bounded fixture review](../target/verify/native5-fixture-v2-independent-review-6d614be02428461fbbea4a1a0a556471.json) and [child-guard review](../target/verify/native5-child-guard-independent-review-810360ee7014496f98843964a6abb066.json).
The failure-only diagnostics and original-caller wrapper passed [17 independent offline checks](../target/verify/independent-pg18-diagnostic-execution-9c5d65ee92184b41b42224f30e17375c/execution-review-receipt.json).
The [initial 165-file composition](../target/verify/native5-gate2-composition-ugtgl557/final-pre-review/intake.json), source manifest `ee217e20`, passes [170 harness and 191 CI-unit tests](../target/verify/native5-gate2-composition-ugtgl557/final-pre-review/offline/receipt.json).
Its declared live inventories remain engine 41, catalog 10, source catalog 3, source snapshot 12, journal 2, owning COPY 8 groups with 28/29 phases, and E1 COPY 40.
The [bounded runner review](../target/verify/independent-native5-runner-selectors-2cd3c7a095914cc49b13f92eef4c644c/runner-binding-review-receipt.json) passes the binding delta and six independent exit/stop controls; it does not qualify a failed run.
The [first PostgreSQL 16 run](../target/verify/pg1618-native5-executor-8pu82vbk/run-pg16.15/output/report.json) stopped at five source-snapshot failures; journal, owning COPY, E1 COPY, and full PostgreSQL 18 did not execute.
Those five observed failures are helper stdout EOF after an arm request, before `SourceSnapshot::create`; helper stderr and exit status were not retained.
Huygens reproduces a separate offline defect: `native()` and `record()` index `source_slot["phase"]` when valid fresh state contains `source_slot: null`.
The [two-file helper correction](../target/verify/native5-snapshot-null-1ld3c0jo/handoff.json), source manifest `fae98fed`, preserves required-key and provenance checks, passes three new tests after their meaningful failures, and reports 173 harness and 191 CI-unit passes.
Its [independent review](../target/verify/native5-precreate-null-independent-review-aba5672b72e84b7cad0635475b59a192.json) passes 28 focused tests and rejects 34 invalid native, record, CREATE, and post-fault inputs.
Huygens executed the [new frozen run](../target/verify/pg1618-native5-nullfix-ky3bmfc7/freeze.json), freeze `0a07aa6b`, with the unchanged reviewed runner map `985d9e4e` under the sole delegated PG/container lease.
The [sealed completion receipt](../target/verify/pg1618-native5-nullfix-ky3bmfc7/results.json), SHA-256 `1f708f86bc32b546bb805080cfbf6eacbd2076e7cfedf6aa6db1410b08fa2128`, records a successful PostgreSQL 18 setup check followed by all seven profiles passing on PostgreSQL 16.15 and 18.6.
Each version passes engine 41, catalog 10, source catalog 3, source snapshot 12 with five faults, journal 2 with two process kills and two committed-Unknown controls, owning COPY 8, and E1 COPY 40.
Owning COPY records 28 phases on 16.15 and 29 on 18.6; each version also records two authority and four replication fault controls.
The setup command took 3.540 seconds, and the full-profile commands took 65.193 and 68.780 seconds; their host invocations took 5.060, 78.897, and 83.368 seconds respectively.
Root verified the sealed source, freeze, runner, and result hashes, all 165 copied source files and 20 native entries, both image IDs and exact version/count sets, three binary-role bindings, the review references, and fourteen protocol validation receipts with status passed, child exit zero, and no capture errors.
Both Linux arm64 runs discover 528 library test names with 77 marked ignored; discovery does not qualify ignored tests, and the live claim covers only the explicitly executed profile inventories.
The runs retain identical CLI/libtest/integration binary hashes and controlled native library `d6ab3015`, and report unchanged source/native/runner inputs with empty owned temporary directories.
All three owned containers are recorded absent, the host process exited zero, and Huygens released the lease; Root performed no PG execution or cleanup.
This is Native5 Gate 2 component qualification only: global strict lint, Gate 3, full migration, power-loss, and performance gates remain open.
The offline reproduction and new successful runs do not recover historical child exceptions, identify the older PostgreSQL 18 setup cause, or promote failed evidence.
The [Gate 3 source-context brief](../target/verify/gate3-source-context-ije2mkwg/packet.md) proposes new context catalog fields/version, durable context compare-and-swap/readback, and source suffix/overlap authority; these are Gate 3 work, not missing Gate 2 prerequisites.
The parent selected a 16-KiB typed JSON/CRC context record and a dedicated singleton native-context table with a fresh schema that refuses the old native layout; the standalone layout, outer native format, backend defaults, and ACK policy remain unchanged.
The [literal context shape](../target/verify/context-v1-shape-bb17wsr3/proposal.md) defines the separate codec contract.
The [frozen context codec](../target/verify/context-v1-author-ijdgnrwk/handoff.json) reports 15 codec tests within 462 offline passes and twelve runtime negative controls; its [independent review](../target/verify/context-v1-independent-review-93lornhr/review.json) passes, and Root has not integrated it into Gate 2.
The [decoder-only handoff](../target/verify/gate3-decoder-idle-uiawnb2n/handoff.json) reports 25 decoder and 71 CDC passes plus seven runtime negative controls for checked idle-state extraction/restoration.
Its [independent review](../target/verify/decoder-idle-independent-review-b4ab1a150f6740a9ae01245ede1fbb5f.json) passes the bounded change; consumer integration remains open.
Its metadata revision corrects warning prose without changing source or test evidence; the methods carry plain decoder state and provide no source or feedback authority.
The [core V2 review](../target/verify/gate3-context-core-review-v4pw6c4f/receipt.json) passes the 164-file input and independently runs 28 core tests plus two selected negative controls for immutable FIRST/current context storage.
The [typed adapter review](../target/verify/gate3-typed-adapter-review-locke-07ea4268b75a.json) independently passes 38 adapter tests and six runtime negative controls; the broader 403 passes remain author-attributed.
These are data-component gates, separate from the qualified Native5 Gate 2 source and its retained earlier failures.
The parent delegated private bounded-origin source implementation to Socrates in `snapshot.rs`, `catalog.rs`, associated tests, and the minimal `state.rs` receipt accessor and `records.rs` consumer exports.
The [SourceOrigin author freeze](../target/verify/gate3-origin-reconnect-tond98u9/handoff.json), handoff `702c17fc` and 169-file source `43853f51`, reports 130 scoped passes with zero ignored cases, formatting and scoped Clippy passes, and visible warnings of 380 library, 24 library-test, and three storage diagnostics.
Fermat's [protocol, budget, and source-authority review](../target/verify/independent-source-origin-protocol-fermat-722e9e02112d41ac89c0ad45647ecb66.json), receipt `2e6ead1e`, passes with seven independent offline checks and zero ignored cases.
Halley's replay and ownership review, receipt `bdb88815`, passes with 21 independent checks; neither review requires an API or code revision.
Root retains primary integration, Git, tasks, and later count/CI wiring; the delegated source and consumer files have no parallel Root writer.
The selected internal qualification retains the full journal prefix and immutable initial decoder context, compares replay before granting source authority, keeps ACK at or below the initial consistent point, and requires finite runtime, WAL-distance, and disk budgets before PG execution.
The parent accepted Arendt's frozen-API consumer plan and delegated implementation in a new private composition of SourceOrigin 169, the two reviewed null-helper files, and the reviewed backlog patch.
Arendt owns six files covering the journal bridge, journal fixture and its tests, the test-db entry, and durability/testing guides; Taskfile, CI, the public CLI, and production SourceOrigin remain outside that scope.
The accepted admission path uses `acquire_bound` with no origin context and a checked source binding after COPY, checks the ordinary SQL connection is Idle twice, and completes every mutable target-authority and native operation before taking pins.
The isolated qualification profile must preserve the existing JournalSerial null-COPY profile, its two live tests, and strict protocol validation; Root waits for the actual candidate inventory before count or CI changes.
The accepted qualification limits are 30 seconds, 64 MiB physical WAL distance, 256 MiB WAL-directory size, 8 MiB comparison data, a 1 GiB free-space floor, and one-second sampling with owned-root volume guards; these are qualification-only limits, not production defaults or live evidence.
The [bounded journal delivery review](../target/verify/journal-one-tx-review-7fttfld1/review.json) independently passes 18 bridge tests and three runtime negative controls for the one-file `scan_one` delivery change, source `8b7ec354`.
It preserves full suffix validation and later-record availability; scan cost remains unmeasured, and the change is reserved for a future Gate 3 consumer composition after SourceOrigin freezes, outside the qualified Native5 `fae98fed` source.
The [publication-wrapper review](../target/verify/publication-wrapper-review-04fbipgb/receipt.json), receipt `1c019fab`, passes the two-path example-entrypoint correction: one Cargo path change and a new nine-line nested wrapper, with production visibility, dependencies, and the shared implementation unchanged.
The reviewer independently ran 22 publication cases with 20 continuations and two controls, and verified the retained 171 recovery passes; the candidate clears the two `E0433` errors while 338 library and nine example warnings remain open.
The parent approved that frozen patch as an input to Arendt's private consumer composition; Root performs no parallel Cargo edit, and the added file must be included in the final candidate's actual inventory.
SourceOrigin derives from `ee217e20` without the null-helper correction; its future composition must add the two reviewed `fae98fed` Python files and the reviewed backlog-delivery patch before integration checks.
That source is distinct from the qualified Native5 Gate 2 candidate and cannot inherit its live result; the corrected G3 source `fc27662a` has its own four-case Linux arm64 qualification and released lease in receipt `d42b8013` above.
Idle-boundary recovery does not qualify general restart or alpha readiness.
The checked-close subset checkpoint assessment is separate from the full unpublished runtime and must establish its own dependency closure and green gates.
The [public inventory review](../target/verify/public-inventory-review-54rbtvgb/review.json) accepts the six-file candidate on public main: four reviewed storage files and the exact recovery-inventory update from 165 to 171.
The [Root archive intake](../target/verify/wal-close-root-candidate-j38_b8iv/intake.json) independently reconstructs that candidate tree without changing the primary files or index.
The [independent offline receipt](../target/verify/offline-sixfile-independent-vho5v4ce/verification.json) passes the exact six-file candidate's check, strict lint, test, and offline tool gates with 454 Rust and 226 Python test executions, zero failures or ignored executed cases, and 22 publication cases with 20 continuations.
The [local qualification receipt](../target/verify/wal-close-root-candidate-j38_b8iv/nonoffline/qualification.json) passes all 11 Linux arm64 stages on PostgreSQL 17.11, including 39 engine and 40 COPY cases, plus all 23 exact-artifact runtime smoke checks.
The [commit proof](../target/verify/wal-close-root-candidate-j38_b8iv/commit.json) binds published commit `8298610` to the tested six-file tree and preserves all 160 primary working files and the empty normal index.
[PR #2](https://github.com/ydixken/rpgmigrate/pull/2) excludes the unpublished runtime and separate 28/29-phase Task changes.
At `8298610`, two amd64 jobs and the PR arm64 job passed, but the push arm64 job failed the unchanged `state::tests::directory_lock_conflict_never_creates_a_lock_marker` test.
The hosted log withholds the assertion, so its precise cause remains unattributed.
The [test-fix review](../target/verify/public-state-lock-review-independent-g2t88u6h/review.json) accepts four added test lines that retain a duplicate descriptor through creation and explicitly unlock the raw competitor; production State handling is unchanged.
The [Linux no-database probe](../target/verify/state-lock-linux-v2-1gi8df31/run/evidence/report.json) passed three controlled descriptor cases and all 55 State tests, while the forced original sequence and unlock-erasure control each failed with `state-busy`.
The first offline probe could not resolve uncached Cargo dependencies and executed no tests; its receipt remains separate from the subsequent locked-dependency run.
The [test-only checkpoint](../target/verify/public-state-fix-candidate-1gde9k29/commit.json) publishes `cf00521` after exact-source check, strict lint, and full default tests passed, preserving all 160 primary working files and the empty normal index.
That test target passed 330 Rust and 63 Python cases, plus 22 publication cases and 20 continuations.
Its first test attempt lacked the pinned upstream archive needed by the native test; the corrected export copied and verified that existing archive without rebuilding libpq or changing source.
Fresh [push](https://github.com/ydixken/rpgmigrate/actions/runs/42384521285) and [PR](https://github.com/ydixken/rpgmigrate/actions/runs/42384525731) jobs passed on arm64.
Both amd64 jobs passed the first ten stages, including all 330 Rust and 63 Python tests, then failed `test-db` with exit 201 and no retained test-failure name.
The [push report](../target/verify/public-state-fix-candidate-1gde9k29/hosted-failure/artifact/ci-linux-FIPGK9/report.json) and [PR report](../target/verify/public-state-fix-candidate-1gde9k29/hosted-failure/pr-amd64/artifact/ci-linux-pqkEzH/report.json) retain that separate failure; the verifier withheld the underlying output, so no database cause is established.
The merge remains held for a bounded diagnostic and green required checks; no failed run is retried or waived, and `main` and primary WIP remain unchanged by publication.
The [first public safe-context candidate](../target/verify/public-ci-context-root-21uc61w9/qualification.json), tree `b209ac69`, passed exact-source check, strict lint, default tests, and 156 CI-unit tests.
It is not approved for publication: the [revised independent review](../target/verify/independent-public-safecontext-review-v2-ecd009ee79e447b98bc708418eb31dcb.json) retains one P2, `PUBLIC-CTX-R1`, because an uncaught exception can supply a valid-looking raw marker without authenticated producer context.
The original review and V1 source, tests, and logs remain preserved; no commit or hosted rerun uses V1.
The parent approved the command-scoped private metadata-channel lifecycle and released Dirac's V2 correction in the same five Python/documentation paths.
Dirac uses a new ordinary ignored export copied from the 114-file V1 manifest; V1 remains immutable and Hypatia owns the bounded re-review.
The [frozen V2 packet](../target/verify/public-ci-safecontext-v2-rpadjcow/handoff.json) binds the same five paths to tree `a5ecf13f`.
The [Root offline qualification](../target/verify/public-ci-context-v2-root-gpkyhnrv/qualification.json) passes exact-source check, strict lint, 330 Rust and 68 Python default tests, 22 publication cases with 20 continuations, and 161 CI-unit tests.
The first aggregate attempt failed because the copied frozen Task cache had read-only modes; its failed receipt remains separate from the successful aggregate after correcting only working-cache modes with identical bytes.
The [bounded V2 review](../target/verify/independent-public-safecontext-v2-review-fd790ea4be6f48a0b887b1c88e53a536.json) closes `PUBLIC-CTX-R1` for this exact tree.
The [commit proof](../target/verify/public-ci-context-v2-root-gpkyhnrv/commit.json) records checkpoint `760dce7`, preserving all 160 primary working files and the empty normal index; the feature-only push left `main` unchanged.
Fresh [push CI](https://github.com/ydixken/rpgmigrate/actions/runs/42391044236) and [PR CI](https://github.com/ydixken/rpgmigrate/actions/runs/42391048920) failed on both architectures at the first `ci-unit` stage, naming `test_channel_schema_size_and_duplicate_records_are_strict`.
All four [source-bound reports](../target/verify/public-ci-context-v2-root-gpkyhnrv/hosted-failure/source-binding.json) match the committed input digest; later stages, including the database gate, did not execute.
The [focused Linux test](../target/verify/public-context-linux-unit-iqwmz_11/receipt.json) reproduces subcase 6, and its [execution control](../target/verify/public-context-linux-unit-iqwmz_11/errno-control/receipt.json) observes `E2BIG` for the fixture's 131,188-byte argument before the child writes any metadata.
The hosted reports do not retain the inner assertion, so the local reproduction and hosted observation remain separately attributed.
Those failures held PR #2 until the bounded test-fixture correction, review, and fresh required checks; the original amd64 database failure remains unknown.
The [one-file transport correction](../target/verify/public-context-transport-osxhrxqt/handoff.json) preserves the oversized bytes and all assertions while passing the fixture through a private file instead of a long argument.
The [Root candidate](../target/verify/public-context-transport-root-inrgxit8/intake.json) proves tree `3122fada` changes only that test file, with the other 113 files identical to published `760dce7`.
Its [fresh Linux Python 3.13.5 regression](../target/verify/public-context-transport-root-inrgxit8/linux-focused-v2/receipt.json) passes the named case without PostgreSQL or a native rebuild.
The [Root qualification](../target/verify/public-context-transport-root-inrgxit8/qualification.json) passes exact-tree check, strict lint, 330 Rust and 68 Python default tests, 22 publication cases with 20 continuations, and 161 CI-unit tests.
The [independent transport review](../target/verify/independent-test-transport-review-8aa3b67deed44fdb8fbedcfa7d69ed04.json) accepts the one-file delta and verifies the retained Linux and local-check evidence.
Root [committed the exact tree](../target/verify/public-context-transport-root-inrgxit8/commit.json) as `c6c7191` and [pushed only the feature branch](../target/verify/public-context-transport-root-inrgxit8/push.json), preserving all 160 working files, the empty normal index, and remote `main` at `8363edbd`.
Fresh [push CI](https://github.com/ydixken/rpgmigrate/actions/runs/42394026600) and [PR CI](https://github.com/ydixken/rpgmigrate/actions/runs/42394030189) passed all four native jobs, including runtime artifact build and smoke, with both version-publication jobs skipped.
The [four retained source reports](../target/verify/public-context-transport-root-inrgxit8/hosted-artifacts/source-binding.json) match the committed source and pass all 11 stages.
Root marked PR #2 ready and [merged it normally](../target/verify/public-context-transport-root-inrgxit8/merge.json) as `2b86da7`, with the exact reviewed tree and both parents preserved; the primary feature branch and all 160 working files remain unchanged.
The [non-force fast-forward](../target/verify/public-context-transport-root-inrgxit8/local-main-ff.json) updates the un-checked-out local `main` and `origin/main` refs to that merge without changing the feature checkout, working files, or index.
The [post-merge run](https://github.com/ydixken/rpgmigrate/actions/runs/42394938470) passes both native architectures for merge commit `2b86da7`; tag publication is skipped.
Both [retained post-merge reports](../target/verify/public-context-transport-root-inrgxit8/postmerge-artifacts/source-binding.json) match the exact committed source and pass all 11 stages, including runtime image build and smoke checks.

- [x] Retain actual `execute`/`runpy` RED controls for uncaught `TypeError` and `Exception`, plus unhandled and forced-exit marker injection.
- [x] Accept diagnostic metadata only from the private channel issued for the actual `test-db` command; never promote stdout or stderr markers.
- [x] Reject trailing, duplicate, and oversized channel data by checking total retained-file size or an overflow sentinel; a valid prefix is insufficient.
- [x] Preserve existing exits, re-raise behavior, failure-name reporting, `BaseException`, `SystemExit`, and `KeyboardInterrupt` handling.
- [x] Keep missing or rejected metadata on the existing failed-command fallback, with no raw-output fallback.
- [x] Re-review the frozen correction, then run exact-candidate required gates before its checkpoint publication.
- [x] Correct oversized test-input delivery without weakening the reader or assertions, and verify the reviewed one-file candidate locally and on Linux Python 3.13.5.
- [x] Qualify the new commit on both hosted architectures before merge.
- [x] Record the post-merge CI result for the exact merge commit.

Adding exception coverage or re-raising alone does not close the provenance defect, and neither the diagnostic change nor V1's local passes identify the retained amd64 database failure.
The version and D-fixture qualification gates retain database and container priority.

## Retained first-slice limits

The E1 command deadline is 300 seconds in total and does not reset between operations.
The package declares the verified Rust 1.99.1 toolchain as its minimum.
The 8 MiB limit applies to COPY forwarding/write chunks, not process RSS.
libpq may allocate a complete COPY row larger than that budget; the fixture contains a 9 MiB row.
Configurable operation/stall deadlines and complete transport memory accounting remain later production requirements.
Process-kill tests do not prove power-loss durability.
The E1 native completion marker does not authorize resume.
Successful COPY output includes the native run's 16-byte identity and state format version, with actual table, row, and byte counts.
It does not include the private state path.
The leader approved one synchronous target transaction across all E1 tables, retaining preflight locks until final COMMIT.
Verified empty catalogs and empty tables remain valid inputs with zero-count completion receipts.
Missing expected fixtures, missing test cases, and failed catalog queries remain failures.

## Handoff

The documentation bootstrap recorded checks across eight owned Markdown files.
This snapshot's seven-file review passed 37 local links, fenced blocks, trailing whitespace, final newlines, and the em-dash prohibition.
A supplemental strict STE scan reports prose-style findings, so this snapshot makes no full STE-compliance claim.
These checks do not establish runtime correctness.
The integration worker takes ownership of running updates to this file after the documentation handoff.
E0/E1 signatures stay frozen unless the leader coordinates a change.
Review E2-E4 interfaces before their workers start.
Expand the E5-E7 and operator/release feature maps before each later milestone.
The selected operator workflow and release gates remain unfinished after E1; broader CLI parity is deferred.
