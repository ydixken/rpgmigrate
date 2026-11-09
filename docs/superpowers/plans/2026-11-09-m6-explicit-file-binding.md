# M6 explicit connection file binding implementation plan

> [!IMPORTANT]
> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development under root's file leases after this private plan is reviewed.
> No implementation, package operation or runtime qualification is authorized by this planning artifact.

Root approved this explicit-file binding and internal-cap amendment on 2026-11-09 and assigned two disjoint implementation owners.
The approved private source is `/Users/ydixken/.codex/tmp/2026-11-09-m6-explicit-file-binding-plan.md`, SHA-256 `2baeb55c81def2236fe63b1007d0ec4a62db93d693537f92161d17c2229d8fb3`.
This repository copy applies Markdown style; proposal/review wording and unchecked tasks below preserve the reviewed planning record, while the dated roadmap amendment records approval without claiming implementation or runtime qualification.

**Goal:** Make the stock helper libpq consume pre-admitted stable bytes for ordinary passfile/TLS file options without rewriting URI/DSN semantics.

**Architecture:** Parse the original private connection string with stock PQconninfoParse, capture the ordinary file inputs before connection, and pass sealed anonymous snapshots through nonempty file overrides after the original expanded connection string in PQconnectStartParams.
Retain original materialization checks, pinned source/target session lifetimes and existing completion authority.
Keep unqualified profiles explicitly refused in alpha and recorded as final-M6 gaps.

**Tech Stack:** Existing Python3.13 standard library, ctypes, fcntl/os, Linux memfd/procfs and packaged stock libpq18 ABI180006.

**Spec:** Approved M6 plan SHA `b8039fb8fecae9e6486fa52517acd0c5ead756244a8f59442d61df3e3b997a6f`; interface addendum `44a3a80cd9ffbc2516406579ac992888ed6702b7f3b0511044dc13e6ee276414`; FINAL relay export `/Users/ydixken/.codex/tmp/m6-python-relay-export-20261109`, manifest SHA `4226dd85ee2d4aad7ac80799ab135d1c036b7fcb42c4273bdd49ccda889a72ed`.
Its evidence_program.py is61363 bytes, SHA `4579fb41e62a41fc9c4bc040c307543578a0b118b7d3d7ae1f85d0d213e0ea13`; evidence_request_writer.py is9902 bytes, SHA `4412adaaa55dbd69aff1ad754c7252562da32440e0f2d2a07e60205d4fb6b882`.
The current rendered main argument is61621 bytes including NUL.
These source/export hashes were checked read-only; no tests were rerun.
Admission cap references come from `/Users/ydixken/.codex/tmp/m6-admission-reader-export-20261109`, manifest SHA `e8d43f56bb813a64ddaa88fe140ab160b04bf04aa4425db1b4a9e961f8c51728`, admission.go SHA `ad90a562056196a27de38bcecd302f4bf8da4af3f43343abbe51832dda256833`.
The admission owner confirmed its materialization cap code remains unchanged in the relay successor.
Do not combine speculative partial dependencies or overwrite either frozen owner's work.
Root selected sealed0400 memfd snapshots and only the immutable existing-image absent path below; the revised internal caps remain a root-review proposal before implementation.

## Global constraints

- Use only packaged helper libpq18 ABI180006; qualify PostgreSQL16 and18 servers, not an additional helper-libpq16 matrix.
- No hand-written conninfo/URI/service parser, new package, public CLI/API, credential Secret read/RBAC, installed helper, result spool or process-global environment mutation.
- Preserve the original raw URI/DSN bytes, explicit-empty option semantics, host order, hostname/hostaddr separation, passfile matching and sslpassword.
- Keep all credentials, file paths, private rows, libpq diagnostics and credential-derived digests out of argv, logs, status, events and receipts.
- Keep saved E, all nineteen guards, independent source closure, no reconnect, unknown-COMMIT refusal and fresh admission rules unchanged.
- Snapshots are private connection input, not evidence storage; no named credential-copy files or result persistence.
- Preserve existing1MiB per-source-file bound, at most five file-option snapshots per role, and existing observation/relay/Job deadlines.
- After root approves this amendment, main source is at most122880 bytes and its complete rendered argument at most126976 bytes including NUL; until approval the frozen61440/65536 limits remain authoritative.
- Prelude and relay argument policy remains65536 bytes including NUL, relay source remains16384 bytes, and request/result/stderr remain65536 bytes.
- Proposed full serialized Job cap is1048576 bytes and applies only to native evidence helper Jobs, never generic or legacy Jobs.
- Do not remove or bypass qualified_resolver_policy; resolver work is owned separately by `/Users/ydixken/.codex/tmp/m6-resolver-research-20261109/resolver-qualification-plan.md`.

## Review focus

- Rebuilding parsed arrays drops explicit empty values because libpq ignores empty array entries: retain the original expanded string.
- Copying an insecure original passfile/key into mode0400 would wrongly make it acceptable: validate original permissions before snapshotting.
- Replacing an absent TLS file with an existing empty file changes optional TLS behavior: preserve stable absence explicitly.
- An open original FD pins an inode but not mutable bytes: consume a sealed snapshot, not merely a rechecked path.
- Post-start PQconninfo discovery is too late for pre-consumption binding: finish both role plans before any connecting call.

## Source facts and precise scope

PQconninfoParse calls the native parser with defaults disabled; it does not read service/default files.
PQconndefaults expands services, may perform LDAP and suppresses service errors, so do not use it as a metadata-only resolver.
PQconnectStart derives options and reads passfiles before returning; TLS code later opens explicit/default certificate/key/root/CRL paths.
The public parameter-array API expands the first dbname connection string and applies later nonempty overrides; empty array values are ignored.
The source links at the end are the authority for these distinctions.

Current Materialize paths are `/tmp/pgpass`, `/tmp/pgm-{source,target}-uri`, `/etc/pgcopydb/creds/<prefix>-mnt/<prefix>-password` and `/etc/pgcopydb/tls/{source,target}/{ca.crt,tls.crt,tls.key}`.
Inline connections are password-free generated URIs plus projected password/prelude materialization.
Simple secretRef composes URI/passfile at startup; its DB URI form preserves existing query values.
URISecretRef passes a full URI/DSN unchanged in a role environment reference.
That branch currently returns without adding TLS mounts, so this plan does not pretend that TLS fields create mounts there or add new materialization behavior.
An opaque file reference can use only an actually admitted available file; mounting missing files is outside this lease.
No accepted opaque profile is permanently removed by an alpha capability refusal.

## File ownership and exact private interfaces

The Python implementation owner receives `internal/native/evidence_program.py`, `evidence_program_test.py`, `evidence_program_test.go` and `evidence_program.go` after root approves the cap amendment against the frozen relay successor.
The admission owner separately receives `internal/native/admission.go` and `admission_test.go` for matching limits and the current live-helper Job-size check.
Existing program getter/hash signatures and request/result schemas stay unchanged; only internal cap constants are added for actual shared consumers.
Do not edit evidence_request_writer.py, evidence.go, podexec, generic controller Job creation, M5 packaging or resolver files under these leases.
The independent verifier owns the later stock-library/runtime qualification receipt and fixture code under a separate root lease.
M5 coordinates any additional stdlib import smoke through root, not a direct packaging edit.

## Internal argument-policy amendment proposal

The old64KiB argument bound is our conservative policy, not Linux's single-argument boundary.
Linux defines MAX_ARG_STRLEN as PAGE_SIZE*32; the4KiB-page case permits131072 bytes including NUL, with larger supported page sizes allowing more.
Aggregate argv/environment limits are separate and include stack-derived limits, string terminators, pointer counts and executable/interpreter overhead.
Do not infer that every environment accepted by a Secret can execute with a below-limit argument.

Propose these exact constants in existing `internal/native/evidence_program.go`, consumed by admission and its embedding tests:

```go
const EvidenceProgramSourceLimit = 122880
const EvidenceMainArgumentLimit = 126976
const EvidencePreludeArgumentLimit = 65536
const EvidenceJobJSONLimit = 1048576
```

These are internal Go-package limits, not new CLI/API options.
Keep the existing relay source16384-byte bound and relay/prelude65536-byte complete-argument policy unchanged.
The frozen relay source is already below both; no relay source or transport payload change is required by this amendment.
The main argument has4096 bytes headroom beneath the minimum Linux single-string limit; the120KiB source cap separately reserves4096 bytes inside the124KiB main-argument cap for wrapper growth.
Check byte lengths, not rune counts, and include exactly one terminating NUL for argument checks.
Keep heredoc collision, final LF, exact program SHA and no-NUL checks.
Do not truncate, compress, encode or split the original URI/DSN or script to force admission.

### Current cap owners and exact necessary edits

1. **Python owner: evidence_program.go.**
   Add the four constants above; getter/hash implementations retain exact raw source bytes.
2. **Python owner: evidence_program_test.go, TestNativeEvidenceProgramEmbedding.**
   Replace main61440 with EvidenceProgramSourceLimit and main-argument65536 with EvidenceMainArgumentLimit.
   Retain writer16384 unchanged and all source/hash/copy/LF/heredoc checks.
   Add boundary assertions for the proposed values, valid source over the old64KiB policy, overflow refusal and actual rendered-wrapper headroom.
3. **Python owner: evidence_program_test.py, test_CredentialsPrivate.**
   Its current line178 separately asserts source<=61440; update this assertion to122880 and keep privacy/content checks.
   The program source changes for file binding under the same owner, but its CAP=65536 is a wire limit and must not be raised.
4. **Admission owner: admission.go, expectedEvidenceMaterialization.**
   Current source61440 check at302 becomes EvidenceProgramSourceLimit.
   Split the current combined check at307: main+NUL<=EvidenceMainArgumentLimit, prelude+NUL<=EvidencePreludeArgumentLimit.
   A small private renderer accepting source bytes may be extracted only to test these exact boundaries without patching process-global source variables.
   It must not become an admission/authority constructor or a new exported rendering API.
5. **Admission owner: admission.go/admission_test.go.**
   Add a private actual-caller predicate for whole native evidence Job JSON size and call it from existing live-object admission before any effect.
   Marshal a copy with batch/v1 Job TypeMeta and all remaining fields included; count actual escaped JSON bytes, including metadata, labels, annotations, owner references, complete spec and any observed status/managed fields.
   Require len<=EvidenceJobJSONLimit and sanitize nil/marshal/size failures without printing or hashing the object.
   Test exact size/size+1, JSON escaping inflation, metadata/owner-reference overhead and no live admission of an oversized helper.
   No exported size framework/predicate is added for a hypothetical future caller.
6. **Later dedicated native Job/lifecycle owner.**
   At the native evidence helper's actual decorated creation site, measure the complete Job after ownership, annotations and all other controller decoration, immediately before Create.
   Use the same complete JSON policy and cap; do not put a generic limit in ensureJob.
   Current generic ensureJob in migration_controller.go adds ownership after calling its builder, so builder-only measurement cannot establish this final cap.
   The dedicated native evidence create path is not present in the frozen Task0 exports; its later owner must place/test this check in its already assigned native lifecycle flow and renew the relevant follow.go/controller test lease when that caller freezes.
   No speculative lifecycle function or generic creation hook is introduced now.
7. **Jobs owner, later actual helper construction lease.**
   resources.go/resources_test.go/native_resources_test.go consume the same main/prelude limits and complete decorated creation contract when the dedicated helper is implemented.
   verify_script_test.go changes only if its native evidence launcher assertions duplicate the old caps.
   The frozen baseline currently has no native evidence Job builder or those cap literals, so there is no existing production builder edit to integrate under this planning lease.
8. **Status/docs owner.**
   Amend primary M6 plan and roadmap after root approves these limits, recording that they supersede44a3's conservative main caps only.
   Do not treat this private plan or the larger caps as runtime support evidence.

No numeric change is needed in current evidence.go relay16384 check, native wire65536 checks, evidence_request_writer.py CAP, podexec payload limits, protocol schemas, API/CRD or image dependencies.
Regression tests must ensure those unrelated limits stay unchanged.
Regenerate the negative source-cap control above122880 rather than reusing the frozen61440-bound red receipt; old passes/failures do not qualify the new policy.

After the named tests exist, run the focused cap/admission checks before the file-binding cases:

```sh
go test ./internal/native -run '^TestNative(EvidenceProgramEmbedding|EvidenceProgramLimits|EvidenceJobSize|AdmissionObjects|PythonCredentialsPrivate)$' -count=1 -v
```

Add TestNativeEvidenceProgramLimits and TestNativeEvidenceJobSize as the private renderer/size-policy cases described above; require nonzero named inventories and no required skips.
Keep these source-level checks separate from real kernel/Job/proxy qualification.

### Job/API compatibility and runtime qualification

The1MiB full-Job JSON policy is conservative relative to Kubernetes1.36's upstream3MiB default write-body limit, but it is not a universal cluster/proxy/storage guarantee.
JSON escaping can enlarge the source and all materialization/template metadata counts toward the Job cap.
Live/defaulted Job size may grow beyond the create payload; admission must refuse rather than ignore that growth.
No source/mount/metadata is added to satisfy the new size rule.

Before product activation, independently qualify the exact admitted image/kernel/architecture and stack settings:

- Test main source122879/122880/122881 bytes and complete main arguments around126976 bytes using safe synthetic program padding and no SQL.
  Check the root-reviewed policy boundary separately from the Linux kernel boundary; do not require an argument violating our policy to execute in production.
- Exercise the exact first-shell argv containing both prelude and main, then the second-shell/env/Python exec chain.
  Measure aggregate argv/env bytes, counts, terminators and actual RLIMIT_STACK with synthetic private inputs at both initial kubelet environment and post-prelude expanded environment stages.
  SecretRef prelude expansion can duplicate/add URI environment values; Pod-spec environment references alone do not bound the later aggregate.
- Include4KiB and any actually supported larger-page kernels, Linux arm64/amd64, actual shell/Python/coreutils versions, seccomp/capabilities and matching image digests.
  Record safe counts/versions and boolean outcomes only, never actual production credential lengths or content.
- Use real default Kubernetes1.36 through the intended API/proxy route to create, read, admit and execute the actual near-cap helper Job, requiring checked WS status/normal closure and terminal success/quiet combined logs.
  Qualify API mutations/defaulting and complete serialized size; include over1MiB refusal before effects.
- Preserve relay source16384 and wire65536 boundaries.
  Its checked WebSocket GET encodes trusted source as query arguments, so separately qualify actual escaped request-target length and proxy handling without adding URI credentials to argv/query.
  A larger Job-main argument does not authorize a larger relay query or different fallback transport.
- Include synthetic single-environment-string E2BIG and aggregate argv/env exhaustion controls, bounded startup/collection refusal and canary scans.
  Do not read Secret values in the controller or add Secret length/RV fields to requests/status to predict startup.
  A process that cannot start cannot validate its own environment; that case remains a fixed no-proof/refusal, with no truncation, script splitting or automatic replay.

Full serialized Job<=1MiB and per-main-argument<=126976 do not prove arbitrary opaque Secret lengths fit the kernel or proxy.
No runtime, stack or cluster clearance follows from Linux source arithmetic.

Add these private Python interfaces in evidence_program.py:

```python
def parse_connection_options(library, raw: bytes) -> dict:
    # keyword -> (explicit_value_or_None, envvar_or_None, compiled_or_None)

def selected_option(options: dict, keyword: bytes, envb: dict) -> bytes | None:
    # Explicit non-None value, else existing env bytes, else compiled metadata.

def connection_home(envb: dict) -> bytes | None:
    # Nonempty HOME, else qualified effective-user passwd home, else None.

class SealedFile:
    def __init__(self, raw: bytes, deadline: float): ...
    def check(self) -> None: ...
    def close(self) -> None: ...
    # .path is private nonempty /proc/self/fd/N bytes.

class ConnectionInput:
    def __init__(self, library, raw: bytes, envb: dict,
                 materialization, deadline: float): ...
    def check(self) -> None: ...
    def claim(self) -> tuple: ...
    def close(self) -> None: ...
    # Owns .keywords/.values NULL-terminated ctypes arrays and snapshots.

def Materialization.prepare(self, library, deadline: float) -> None: ...
def Materialization.connection(self, role: str) -> ConnectionInput: ...
def Materialization.pin(self, path) -> StableFile: ...

def Session.__init__(self, library, connection: ConnectionInput,
                     deadline: float, diag) -> None: ...
```

Use the existing ConnOption ctypes layout.
Add `PQconninfoParse` restype POINTER(ConnOption), argtypes `[STR, POINTER(PTR)]`, and `PQconnectStartParams` restype PTR, argtypes `[POINTER(STR), POINTER(STR), INT]`.
Retain existing PQconninfoFree/PQfreemem and remove the direct PQconnectStart requirement once all private callers move.
Use a void-pointer error output and free it without reading/formatting its raw message.
Bound the native option table at256 entries, copy option values before freeing, and preserve None versus b''.

Materialization.prepare creates source and target ConnectionInput plans before observe opens either session.
observe passes materials.connection('source') and then materials.connection('target'); all SQL ordering remains unchanged.
Remove post-consumption bind_options as a discovery path.
Post-connect checks instead corroborate that effective file options equal the prepared overrides and sessions/transport identities remain stable; those checks never substitute for preflight.
Materialization owns all inputs for failure cleanup; Session holds its role input until PQfinish and closes it idempotently afterward.
ConnectionInput.claim validates a live prepared input, marks it started exactly once and returns its two retained arrays; a closed or previously claimed input cannot start another session.
Materialization.check retains original environment/file/observed-absence checks and verifies snapshots only for inputs that remain open.
Closed source input is never reopened and does not make a later target materialization check fail merely because its snapshots were correctly released.

## Task 1: Parse native options and preserve precedence

**Consumes:** Original captured role bytes, unchanged environment bytes and the existing runtime-bound library.
**Produces:** Complete nonconnecting file plan or fixed refusal before either session begins.

- [ ] Add failing tests test_FileOptions and test_FileOptionPrecedence, exposed by TestNativePythonFileOptions and TestNativePythonFileOptionPrecedence Go wrappers.
- [ ] Test URI and keyword/value inputs, duplicate-native-option handling, percent escapes, literal plus, quotes/backslashes, explicit password/sslpassword, and explicit empty values conflicting with environment defaults.
- [ ] Implement parse_connection_options and selected_option from native metadata, without PQconndefaults or environment mutation.
- [ ] Detect service activation using explicit service when non-None, otherwise PGSERVICE; even b'' is selected rather than absent under native service semantics.
  Refuse active service before any connecting call for this alpha scope.
  PGSERVICEFILE/PGSYSCONFDIR alone do not activate a service and must not cause unrelated file consumption.
- [ ] Construct arrays with first pair dbname=original raw input, then only nonempty qualified overrides, then NULL terminators; use expand_dbname=1.
  Never rebuild the whole input from parsed option values.
  Retain byte objects/arrays and their owner until connection start has copied them, and conservatively through session lifetime.
- [ ] Assert that original literal dbname values containing `=` inside URI/DSN are not recursively expanded or altered, and all nonfile options remain native.

## Task 2: Select ordinary file inputs and preserve original acceptance

**Consumes:** Native parsed values/metadata, captured environment and existing original materialization pins.
**Produces:** A selected ordinary source or captured absence for each possibly consumed file keyword.

- [ ] Add failing test_FileSources with explicit paths, environment fallbacks, explicit empty suppressing environment then using home defaults, HOME fallback/no-home, relative paths and literal tilde paths.
- [ ] Handle the finite file keyword set passfile, sslrootcert, sslcert, sslkey and sslcrl.
  Determine native explicit-versus-environment selection through selected_option; for a selected empty/absent file option, use the documented ordinary home default where libpq does.
  Defaults are `.pgpass`, `.postgresql/root.crt`, `.postgresql/postgresql.crt`, `.postgresql/postgresql.key` and `.postgresql/root.crl`.
  A missing home means absence, not a fabricated home.
- [ ] Preserve path bytes through os.fsencode/fsdecode round trips.
  Explicit `~` is literal pathname syntax here, not a request for os.path.expanduser.
  Resolve relative paths against an admitted captured working-directory descriptor, without chdir or lexical normalization that changes symlink/.. behavior.
  Reuse original descriptor and permitted projection-symlink checks; do not reject Kubernetes atomic-writer symlinks generically or permit arbitrary shadow mounts.
- [ ] Use nonempty HOME first; otherwise the effective-user passwd home under the already qualified immutable image NSS/passwd policy.
  The standard-library pwd module is sufficient; it adds no package dependency.
  If this fallback's NSS behavior or home cannot be qualified, return capability rather than guessing `/root`, `/nonexistent` or a compiled directory.
- [ ] For possibly consumed passfile, validate original regular-file/type/readability and no group/world access before copying.
  For sslkey, retain libpq's original root-owner0640-or-less rule and non-root0600-or-less rule.
  Never chmod/chown the source or normalize an invalid source into a valid snapshot.
  Insecure original files refuse with connection before any connecting call.
- [ ] Do not consume passfile when native effective password is nonempty.
  If effective sslmode is definitely disable, do not consume TLS file options.
  Retain native behavior for other modes; do not infer sslcertmode or GSS behavior beyond a qualified source-backed branch.
  Conservatively binding a possibly used ordinary file does not authorize permanent refusal of otherwise accepted profiles; record any such alpha limit.
- [ ] Reject unqualified active sslrootcert=system, sslcrldir and engine-form sslkey profiles as capability before connection.
  Preserve raw sslpassword through the original connection string; do not interpret it or use it as a filename.
  Trusted immutable image OpenSSL configuration remains an M5 prerequisite; unqualified OPENSSL_CONF/module-provider overrides are a separate capability gap, not parsed by this change.
- [ ] Reject TLS filename/home expansions that reach the native1024-byte MAXPGPATH bound rather than silently change libpq's truncation behavior.
  Preserve the existing1MiB file cap; oversize/unrepresentable input is an explicit alpha capability limit, not final acceptance closure.

Current Materialization eagerly pins environment-named files even when overridden or unused.
Replace that eager env discovery with selected role plans; retain required fixed startup URI/passfile/projection stability checks.
This avoids reading a conflicting ignored environment file while preserving the original admitted materialization boundary.

## Task 3: Bind immutable bytes and stable absence

**Consumes:** Selected original source bytes/permissions and original descriptor checks.
**Produces:** Private nonempty override paths consumed by stock libpq, with no mutable read interval.

- [ ] Add failing test_FileSnapshots and test_FileAbsence, exposed by TestNativePythonFileSnapshots and TestNativePythonFileAbsence.
- [ ] For a present source, capture bounded bytes under existing before/after descriptor/path checks, create memfd with a fixed nonsecret name and MFD_CLOEXEC|MFD_ALLOW_SEALING, write bytes with partial-write/deadline handling, set mode0400 and rewind.
  Prefer MFD_NOEXEC_SEAL when exposed by the packaged Python/kernel; otherwise retain non-executable mode and the data-only use, and refuse if the actual kernel policy rejects creation.
  Do not disable namespace/kernel policy or add hardcoded fallback syscall numbers.
- [ ] Apply and verify F_SEAL_WRITE|F_SEAL_GROW|F_SEAL_SHRINK|F_SEAL_SEAL using fcntl.
  Verify regular type, expected UID, mode0400, captured length and byte identity privately, and procfd stat identity.
  Anonymous link count is normally0: do not feed the snapshot through StableFile's nlink1 source-file predicate.
  Retain the original FD or a verified read-only reopened FD until PQfinish; never expose a reusable closed FD number.
- [ ] Append `/proc/self/fd/N` for the actual snapshot.
  The only permitted procfs magic-link following is this internally generated retained descriptor path, not an arbitrary input `/proc` reference.
  Qualify stock libpq/OpenSSL opening and mode checks against the actual packaged library and runtime; documentation alone is not a passed TLS test.
- [ ] Prove write, grow and shrink fail after sealing, including a second descriptor; no source mutation or hardlink/rename may alter consumed snapshot bytes.
  Original materialization drift still causes observation refusal under existing checks, but is not relied on to establish the bytes consumed by libpq.

### Root-selected stable absence route

Use only the fixed private internal path `/usr/share/rpgmigrate/.native-file-absent` that is **absent** inside the admitted image's existing runtime-metadata directory.
It adds no file, mount, ConfigMap, CLI, metadata key or configuration option.
Before using it, require original-path absence, root-owned nonwritable ancestors, read-only rootfs and admission forbidding a shadow mount over that path/ancestors.
The image digest plus these existing integrity assumptions prevents a later helper-created entry; M5 smoke/runtime qualification must independently establish this for the exact image.
If unavailable, refuse capability rather than create/delete a marker or change image contents locally.

An optional missing root/client certificate must map to stable absence, **not an empty existing snapshot**.
Append the fixed absent path for a genuinely absent selected ordinary file so stock libpq retains its own optional-versus-required behavior.
A present but empty or malformed file is copied exactly; it must not be converted into absence.
Only ENOENT/ENOTDIR establish absence; EACCES, I/O failure or an unqualified ancestor refuses instead.
Track/revalidate original materialization changes privately without claiming latest Secret revision equality.

Root selected this one route.
Do not implement the previously researched procfd ENOTDIR absence alternative or any fallback.
If immutable absence, original read-only rootfs, ancestors, runtime absence or mount-shadow admission cannot be established, refuse capability and return the precise gap.
No file, mount, image metadata key or ConfigMap is added by this choice.

## Task 4: Session integration, ownership and failure behavior

**Consumes:** Both prepared ConnectionInputs, original role/session logic and unchanged relay delivery.
**Produces:** The same operation/proof behavior with pre-consumption file binding and deterministic cleanup.

- [ ] Add failing test_FileConnectionStart and test_FileCleanupOrder, exposed by TestNativePythonFileConnectionStart and TestNativePythonFileCleanupOrder.
- [ ] Prepare both roles before any call to PQconnectStartParams, retaining source/target separation and no copying one role's options to the other.
  File preparation shares the existing25-second operation budget; it does not extend that deadline or bypass the independently refused resolver gate.
- [ ] Replace Session's direct PQconnectStart call with PQconnectStartParams(connection.keywords, connection.values, 1), after connection.check.
  Obtain those arrays through the one-use connection.claim method; do not claim an input a second time on a failed start.
  Install notices and use the existing nonblocking polling/result discipline unchanged.
- [ ] Compare post-start effective file paths to prepared overrides as corroboration, retain endpoint/role/TLS checks and source closure before target creation.
  Do not rediscover new paths after consumption or silently reconnect with another credential snapshot.
- [ ] Close PGconn before closing its snapshots; close the source input before target session use where possible and retain target snapshots through complete target closure.
  Session.close must release its claimed input even when start returned NULL and no PGconn exists; do not retain the old early-return path that would skip this cleanup.
  Materialization.close remains an idempotent final owner for partial preparation/connection failures.
  No descriptor, snapshot or parsed-options allocation survives final private closure; no credential-derived digest is emitted.
- [ ] Preserve fixed failure classes: capability for unsupported profile/ABI/kernel/procfs/immutable-absence proof; connection for malformed conninfo or invalid selected credential-file permissions; admission for original ownership/path mismatch; timeout for exhausted budget; closure for cleanup failure.
  Existing unknown_commit dominates later cleanup failures once COMMIT outcome is unknown.
  Never stringify errno paths, parse errors, raw libpq text, snapshots, credentials or rows.
- [ ] Preserve original startup URI/environment bytes once captured, plus existing immutable-reference/materialization guaranteeA.
  This proves consumed private snapshots and actual SQL connection identity, never equality to the latest Secret UID/resourceVersion.

## Focused verification and required runtime evidence

After implementation creates the named wrappers, run only the focused source cases first:

```sh
go test ./internal/native -run '^TestNativePython(FileOptions|FileOptionPrecedence|FileSources|FileSnapshots|FileAbsence|FileConnectionStart|FileCleanupOrder)$' -count=1 -v
go test ./internal/native -run '^TestNative(PythonABI|PythonConnectPolling|PythonCredentialsPrivate|PythonObservationOrder|PythonUnknownCommit|EvidenceProgramEmbedding)$' -count=1 -v
```

Each command must execute a nonzero named inventory, with no required skip.
Host pure tests may fake syscalls and option arrays but cannot qualify Linux memfd/procfs or actual authentication.
Do not count an unavailable kernel capability as a passing Linux qualification case.

The independent verifier's later explicitly leased stages are:

1. **Exact stock library, no database:** call actual PQconninfoParse on private synthetic URI/DSN cases; verify decoded percent/newline/trailing-space/literal-plus bytes, explicit-empty precedence, allocation ownership and no connect call.
   On both admitted Linux architectures, verify memfd seal/mode/procfd behavior under the actual UID65532/seccomp/capability/read-only-rootfs settings.
   Record Python/libpq/package/kernel/image/source identities and boolean/case counts only.
2. **Stock helper libpq180006 against PostgreSQL16 and18:** test all actual inline/simple-secret/DB-URI/URISecretRef password inputs, correct and wrong passfiles, explicit password overriding passfile, host/hostaddr matching, Unix socket and multihost selection without rewriting host order.
   Use private fixture inputs, never URI/password argv or SQL literals in argv.
3. **Actual TLS consumption:** test CA verify-full hostname success/mismatch, client cert/key, original root-owned0640 and current-user0600 key acceptance, insecure original refusal, encrypted key with correct/wrong/missing sslpassword, present/absent/empty root/client cert distinctions, and ordinary CRL present/absent behavior.
   Compare raw stock-libpq controls with prepared-file behavior for each native optional/required outcome.
   Missing root with require/prefer must not become an empty-file parse error; missing required root/key must not become success.
4. **Faults:** swap original projected symlinks or alter source bytes after capture, close/reuse a snapshot FD early, fail a seal, remove/hide procfs, violate immutable absent-path assumptions, fail cleanup and expire budgets.
   Prove libpq either consumes the captured snapshot or the operation refuses, never fresh uncontrolled bytes.
   Verify no SQL/connection call on unsupported-service/system/engine/directory preflight and preserve unknown-COMMIT refusal after connection.
5. **Full helper integration:** run only after separately approved resolver/image/PVC/TLS/relay prerequisites exist, with exact original Job/pod/container/materialization admission and all nineteen guards.
   Direct ConnectionInput/Session qualification does not clear those production gates or authorize invented TLS mounts for URISecretRef.

Keep synthetic credential/certificate inputs private0700/0600, scan argv/output/receipts for their canaries without printing them, and remove only the exact owned fixtures.
Closing snapshots releases volatile kernel references; make no Python zeroization, no-swap or broad secret-erasure claim.
No broad audit, dependency operation, DB/container/cluster work or qualification test was performed during this planning lease.

## Root-review decisions and remaining gaps

1. Root has selected sealed0400 anonymous credential-input snapshots and only the existing-image absent path, with exact image/admission/runtime prerequisites and no fallback implementation.
2. Review the main-only122880/126976 cap amendment, unchanged relay/prelude/wire caps and native-evidence-only1MiB full-Job policy before implementation; these are internal policies, not platform clearance.
3. Renew the four-file Python implementation lease against the now-frozen relay, coordinate the separate admission.go/test cap owner, and assign an independent Linux/libpq18 verifier.
4. Keep service/LDAP, system trust roots, engine keys, CRL directories, unqualified NSS/OpenSSL overrides and oversized/unrepresentable paths as explicit alpha capability gaps; final M6 does not become complete by permanently excluding them.
5. Preserve separate resolver refusal and all existing real-PVC/image/TLS/production-admission gates.

## Primary references

- [libpq18 connection API](https://www.postgresql.org/docs/18/libpq-connect.html): native Parse, array expansion, nonempty override precedence and ownership.
- [REL_18_6 fe-connect.c](https://github.com/postgres/postgres/blob/REL_18_6/src/interfaces/libpq/fe-connect.c): PQconninfoParse defaults=false at5791 onward, service/default expansion, home selection and passwordFromFile regular/perms checks.
- [REL_18_6 TLS implementation](https://github.com/postgres/postgres/blob/REL_18_6/src/interfaces/libpq/fe-secure-openssl.c): initialize_SSL file selection, optional ENOENT/ENOTDIR, required key and root-owned0640 checks, rootcert system and engine behavior.
- [libpq18 password files](https://www.postgresql.org/docs/18/libpq-pgpass.html) and [TLS](https://www.postgresql.org/docs/18/libpq-ssl.html): native password precedence, host matching and certificate/key requirements.
- [MAXPGPATH source](https://github.com/postgres/postgres/blob/REL_18_6/src/include/pg_config_manual.h#L83-L93):1024-byte filename buffer.
- [Python os.memfd_create](https://docs.python.org/3.13/library/os.html#os.memfd_create), [fcntl seals](https://docs.python.org/3.13/library/fcntl.html), [Linux memfd](https://man7.org/linux/man-pages/man2/memfd_create.2.html), [seal API](https://man7.org/linux/man-pages/man2/F_ADD_SEALS.2const.html) and [kernel noexec policy](https://docs.kernel.org/userspace-api/mfd_noexec.html): documented capability and lifetime requirements, not runtime qualification.
- [Linux6.12 per-string constant](https://github.com/torvalds/linux/blob/v6.12/include/uapi/linux/binfmts.h) and [exec aggregate accounting](https://github.com/torvalds/linux/blob/v6.12/fs/exec.c#L479-L528): distinct single-string and stack/argv/environment constraints.
- [Kubernetes1.36 default API body limit](https://github.com/kubernetes/kubernetes/blob/v1.36.0/staging/src/k8s.io/apiserver/pkg/server/config.go#L420-L436):3MiB upstream default, not proof of actual proxy/storage/cluster settings.
