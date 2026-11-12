# Runner image

We package the existing `rpgmigrate` executable with stock PGDG libpq 18, PostgreSQL client tools 16 and 18, and the distro Python 3 standard library.
The image is a packaging artifact.
Migration support follows the engine's qualified capabilities.

## Build and smoke locally

Use a running Podman instance on the native architecture.
The production image build is separate from the M2 development-container build.

1. Check the shared fixture inventory from the same checkout.

   ```sh
   python3 tests/test_native_fixtures.py
   ```

2. Build the runner from the working tree.

   ```sh
   task image:build
   ```

3. Check the built image without rebuilding it.

   ```sh
   task image:smoke
   ```

4. Retain the immutable image ID and observations in a receipt.

   ```sh
   task --silent image:smoke > /tmp/rpgmigrate-image-smoke.json
   ```

Override `IMAGE` to select another local tag.
The smoke helper inspects that tag once, then runs only its immutable image ID.
`CONTAINER_RUNTIME` selects a Podman-compatible executable, defaulting to `podman`.

Check out these **examples**:

- Build under another local tag.

  ```sh
  task image:build IMAGE=localhost/rpgmigrate:review
  ```

- Probe a local image directly.

  ```sh
  CONTAINER_RUNTIME=podman python3 tools/image/smoke.py --image localhost/rpgmigrate:review
  ```

## Runtime layout

- `/usr/local/bin/rpgmigrate` is the release executable.
- `/usr/lib/postgresql/16/bin` and `/usr/lib/postgresql/18/bin` contain matching-major `pg_dump`, `pg_restore`, and `psql`.
- `psql` on `PATH` resolves to major 18.
- `/bin/sh`, GNU coreutils, and GNU sed support operator scripts.
- `/usr/bin/python3` includes `ctypes`, `_ctypes`, and its distro `libffi` dependency.
- `/usr/share/rpgmigrate/python-libpq-runtime.json` binds the private Python runtime to the installed stock libpq file.
- `/usr/share/rpgmigrate/LICENSE` retains the project Apache License 2.0 text.
- `/usr/share/rpgmigrate/NOTICE` retains the repository's third-party notice text.
- `/usr/share/doc/<package>/copyright` retains Debian and PGDG package notices.
- `/usr/share/rpgmigrate/dependency-notices/<name-version>` retains Cargo dependency license and notice files, preserving their relative paths and bytes.
- `/usr/share/rpgmigrate/build-inputs.sha256` records each source, manifest, build script, notice, and image-definition input by content hash.

Schema utility discovery preserves explicit `PG_BINDIR` precedence over `PG_CONFIG` and checks both selected tools against the target major.
With neither override set, we select `/usr/lib/postgresql/<target-major>/bin` before searching `PATH`, so PostgreSQL 16 targets use the packaged 16 tools while default `psql` remains 18.
We search `PATH` only when that packaged path is absent.
An existing packaged path with missing, invalid, or mismatched tools fails discovery.

We exclude Git metadata, tests, local configuration, credentials, and unrelated repository files from the build context with an allowlist.
The build-input manifest describes the working-tree bytes copied into the builder, including uncommitted edits.
The smoke compares the complete manifest and every source hash against its local checkout before emitting a receipt.
Its digest identifies those working-tree inputs without claiming that they match a Git commit.
Run the smoke from the checkout whose bytes built the image.
Base manifests are digest-pinned.
Signed APT repositories select available package updates within the specified majors.
Retain build logs and the receipt because a later build can resolve different package updates.

## Discovery and artifact identity

The smoke requires the exact three-field Linux discovery document in `tests/fixtures/native/v1/payloads/version-qualified-linux-profile.json`, regardless of the smoke host operating system.
It decodes the local golden and `contract.json` with duplicate-field refusal, checks both against the independent ordered producer expectation, and compares the observed document with that golden.
The required capability array is:

```json
["clone.single_db","cdc.pgoutput","progress.v1","cutover.v1","verify.v1","cleanup.v1","verify.sealed_files.v1"]
```

The accepted `version-empty-capabilities` and original six-capability `version-qualified-profile` fixtures preserve historical parser behavior.
Either set fails this Linux producer check.
The portable six-capability fixture and `qualified_capabilities` metadata stay unchanged.
Linux builds add `verify.sealed_files.v1`, matched by fixture-only `qualified_linux_capabilities`.
Non-Linux builds retain six capabilities because the internal sealed-file comparison mode requires Linux.
The new capability describes compiled sealed-file comparison support.
Kernel, descriptor, file, and runtime admission checks remain mandatory.
The narrowed engine profile covers single-database text COPY, measured progress, pgoutput protocol v1 follow, persisted endpos and drain, schema/data comparison, and exact owned cleanup.
The command paths include `clone`, `follow`, `list progress --json`, the supported `stream sentinel get` and `stream sentinel set endpos` forms, `stream cleanup`, and `compare schema` or `compare data`.
The profile requires PostgreSQL 16 or 18 with source major no greater than target, explicit large-object skipping for follow, unsplit text COPY, and `durable` mode.
Binary COPY, splitting, large objects, replay-safe cadence, and relaxed durability remain outside this advertisement.
Discovery permits capability assessment.
It does not open operator admission or certify helper, database, recovery, or cutover behavior.

The probe requires `/usr/local/bin/rpgmigrate` to be a regular file without a final symlink and records its actual SHA-256 from `sha256sum` at that exact path.
The receipt retains this observed digest as `runtime_binary_sha256`, a lowercase 64-digit string.
The hash is an observation from the selected immutable image, with no caller-supplied expected binary hash or qualification authority.
The receipt's `image_id` is the local image configuration ID returned by inspection.
A registry manifest digest is a separate artifact identity.
`build_inputs_sha256` identifies the complete source-input manifest, while `runtime_binary_sha256` identifies the installed executable bytes.
For registry qualification, retain the typed registry index, selected manifest, and config graph with anonymous public receipt hashes.
M7 uses direct public digest pulls, without an archive/import/alias fallback.
A tag, revision label, source-input digest, executable hash, and image manifest digest each describe different evidence and cannot substitute for one another.

Keep the passing fixture-inventory result with the frozen qualification packet or exact CI commit.
Also retain hashes of the smoke script, contract, qualified golden, and fixture manifest.
These qualification inputs remain outside the production build context and its source-input digest.

## Writable mounts and isolation

The configured uid and gid are both `65532`, and the working directory is `/work`.
The default command is `rpgmigrate --help`.
No entrypoint prevents callers from running shell commands.

> [!important]
> Set the root filesystem read-only when starting the container.
> Mount writable `/work` and `/tmp` directories owned by `65532:65532`.
> The Dockerfile declares no volumes and cannot enforce runtime read-only flags.

The smoke uses temporary filesystems for both writable paths, disables networking, drops all capabilities, and enables `no-new-privileges`.
We disable Podman's automatic read-only-container temporary mounts and use `--mount` with `U=true` to assign both explicit mounts to the configured user, following the [Podman runtime documentation](https://docs.podman.io/en/latest/markdown/podman-run.1.html).
It checks the configured and effective uid, actual machine architecture, exact discovery document, and executable hash.
It checks linked libpq resolution, installed PGDG package ownership, both client-tool majors, default `psql`, GNU utilities, and retained notices.
The Python probe imports the required standard-library modules, checks the libpq symbol inventory, and calls `PQlibVersion` with an explicit ctypes ABI without opening a connection.
It checks the fixed runtime metadata file as a regular root-owned mode `0444` file.
It checks the size and exact schema.
It binds the canonical library path and hash to installed package ownership, architecture, version, and the mapped library.
The receipt retains the metadata digest, installed binary/source package versions, and resolved package copyright paths and hashes.
It retains Python license bytes by hash, Cargo notice inventory, and repository NOTICE hash.
Package notices stay in their distro locations with their original bytes.
The probe also runs a private script through a private FIFO.
It requires synthetic input to remain absent from Python's command line and public output.
It also checks the observed root mount is read-only, writes in both allowed mounts, and requires a fresh write in `/var/tmp` to fail.
Missing observations, missing tools or libraries, malformed metadata, and failed subprocesses are failures.
The smoke accepts no database endpoints or credentials.

## Private helper boundary

The operator owns its private Python script, admission, credentials, connection behavior, timeouts, and cleanup.
The image installs no helper executable and adds no engine CLI capability.
Use the fixed start prefix `/usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B` and keep connection input out of arguments and diagnostics.
The script uses no sibling imports or PyPI packages.
The runtime metadata has exactly `version`, `architecture`, `library_path`, `library_sha256`, `libpq_version`, `package_name`, and `package_version` fields.
Its schema version is integer `1`, architecture is OCI `arm64` or `amd64`, and libpq is major 18.
The installed package version is nonempty ASCII without whitespace or control characters, bounded to 256 bytes.
The metadata is generated from actual installed facts during the build and is at most 4096 bytes.

> [!important]
> Exclude untrusted `LD_*` variables from the native Pod environment before the first process starts.
> The `/usr/bin/env` process itself uses the dynamic loader, so its `-u` flags are only defense in depth.
> Packaging smoke does not qualify real endpoint, passfile, URI, TLS, cancellation, or PVC admission behavior.

## Qualification boundaries

Run `python3 tools/image/test_smoke.py` for offline refusal controls.
Those controls check the probe's behavior.
They do not execute a container or qualify an architecture.
Native amd64 and arm64 build-and-smoke receipts are separate gates.
An arm64 result does not qualify amd64.
Hosted production-runner packaging passed separately on both architectures.
Hosted plain clone passed 16 to 16, 18 to 18, and 16 to 18.
Those results do not prove CDC or an actual native operator workflow.
See the [roadmap](roadmap.md) for provenance.

The M2 development image and clone matrix test engine behavior independently of the production runtime.
Packaging smoke does not qualify migration contents, follow/drain, crash recovery, power-loss durability, or operator integration.
Changing discovery metadata or the smoke contract requires fresh image build-and-smoke evidence for the rebuilt bytes.
Earlier M5 receipts remain evidence for their recorded images and empty discovery arrays.
They do not qualify a rebuilt image under this contract.
M6 runtime acceptance must bind the selected image and executable digest to actual runtime checks on 16:16, 18:18, and 16:18.
Those checks cover clone, progress, pgoutput follow, persisted endpos/drain, schema/data comparison, and exact owned cleanup.
Keep that acceptance pending until the exact artifact runs those checks.
Final release qualification runs the required migration matrix on the exact production image digest.
Development publication follows the narrow approval below.
Release publication remains a separate step after the exact release qualification gates pass.

## Public development images

The approved development package is exactly `ghcr.io/ydixken/rpgmigrate`.
Its feature-branch workflow calls `task image:dev:publish` on the exact clean source commit and native architecture.
The Task requires `ARCHITECTURE`, `IMAGE_REVISION`, and `OUTPUT_DIR`.
`RETAINED_RECEIPT` is optional for an admitted continuation.
`OUTPUT_DIR` must identify an existing private external directory with mode 0700.
The publisher enforces the Actions context for ordinary publication.
The Task does not push a branch or dispatch a workflow.

Tags are `dev-<full-40-hex-commit>-arm64` or `dev-<full-40-hex-commit>-amd64` and select one runnable manifest.
An existing tag must never be overwritten.
We do not create a multiarch join tag, release tag, or `latest` in this route.
`IMAGE_REVISION` on `image:build` accepts only a full lowercase 40-hex commit and forwards only `SOURCE_REVISION` as a build argument.
The default local build retains its working-tree packaging role when that value is absent.
A revision label does not replace the complete clean-source inventory, build-input hash, executable hash, or smoke receipt.

The separate first-package bootstrap approval permits only the exact native package and admitted source/platform.
The leader must admit the exact source commit and existing private output directory before this exception runs.
Check out these **examples**:

- Publish the first native package under that separate admission.

  ```sh
  python3 tools/image/dev_publish.py publish --architecture arm64 --expected-commit "$IMAGE_REVISION" --output-dir "$OUTPUT_DIR" --bootstrap-native
  ```

- Continue anonymous visibility verification for a retained receipt without rebuilding or pushing.

  ```sh
  python3 tools/image/dev_publish.py verify-public --receipt "$RETAINED_RECEIPT" --output "$PUBLIC_RECEIPT"
  ```

A `PUBLICITY_PENDING` receipt preserves exact digest, source, and smoke evidence but does not qualify an image lock.
Public package visibility approval covers only the exact manager and native development packages.
An authorized visibility change is followed by verification of the same digest, without rebuild or repush.
Require anonymous manifest, config, and blob reads before a publication receipt qualifies.
A fresh M7/M6 run also requires actual pulls and typed image bindings on every exact owned node.
Keep the generated schema 2 image lock outside Git.
Its exact sources and receipt hashes must agree with the test checkout.
Do not commit the newly built manager digest to its own source catalog.

The retained format 8 / metadata 6 development runner receipts bind the first arm64 publication to source `11a28a4d1c77b035f96cc16b7b948768dd6aa6c9`.
The two-architecture successor receipts bind source `abd1a36e50726a26a9f655c5f53bb34ed8d182c1` and its published image digests.
The M8 engine candidate uses native format 9 and target metadata 7.
Qualification of that candidate requires receipts for its exact source and runner, including the operator helper and operator-default migration matrix.
Hosted image build/publication and offline contract results do not qualify operator runtime, native admission, or the migration matrix.
Hosted kind runtime requires a separate qualified Linux-native-host Guard transport.
Development approval does not authorize releases, crate publication, main pushes or merges, or benchmarks.
