# Runner image

We package the existing `rpgmigrate` executable with stock PGDG libpq 18, PostgreSQL client tools 16 and 18, and the distro Python 3 standard library.
The image is a packaging artifact; migration support follows the engine's qualified capabilities.

## Build and smoke locally

Use a running Podman instance on the native architecture.
The production image build is separate from the M2 development-container build.

1. Verify the shared fixture inventory from the same checkout.

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
- `/usr/share/rpgmigrate/NOTICE` retains the repository's third-party notice text.
- `/usr/share/doc/<package>/copyright` retains Debian and PGDG package notices.
- `/usr/share/rpgmigrate/dependency-notices/<name-version>` retains Cargo dependency license and notice files, preserving their relative paths and bytes.
- `/usr/share/rpgmigrate/build-inputs.sha256` records each source, manifest, build script, notice, and image-definition input by content hash.

Schema utility discovery preserves explicit `PG_BINDIR` precedence over `PG_CONFIG` and validates both selected tools against the target major.
With neither override set, we select `/usr/lib/postgresql/<target-major>/bin` before searching `PATH`, so PostgreSQL 16 targets use the packaged 16 tools while default `psql` remains 18.
We search `PATH` only when that packaged path is absent; an existing packaged path with missing, invalid, or mismatched tools fails discovery.

We exclude Git metadata, tests, local configuration, credentials, and unrelated repository files from the build context with an allowlist.
The build-input manifest describes the working-tree bytes copied into the builder, including uncommitted edits.
The smoke compares the complete manifest and every source hash against its local checkout before emitting a receipt.
Its digest identifies those working-tree inputs without claiming that they match a Git commit.
Run the smoke from the checkout whose bytes built the image.
Base manifests are digest-pinned; signed APT repositories select available package updates within the specified majors.
Retain build logs and the receipt because a later build can resolve different package updates.

## Discovery and artifact identity

The smoke requires the exact three-field Linux discovery document in `tests/fixtures/native/v1/payloads/version-qualified-linux-profile.json`, regardless of the smoke host operating system.
It decodes the local golden and `contract.json` with duplicate-field refusal, checks both against the independent ordered producer expectation, and compares the observed document with that golden.
The required capability array is:

```json
["clone.single_db","cdc.pgoutput","progress.v1","cutover.v1","verify.v1","cleanup.v1","verify.sealed_files.v1"]
```

The accepted `version-empty-capabilities` and original six-capability `version-qualified-profile` fixtures preserve historical parser behavior; either set fails this Linux producer check.
The portable six-capability fixture and `qualified_capabilities` metadata stay unchanged.
Linux builds add `verify.sealed_files.v1`, matched by fixture-only `qualified_linux_capabilities`; non-Linux builds retain six capabilities because the internal sealed-file comparison mode requires Linux.
The new capability describes compiled sealed-file comparison support; kernel, descriptor, file, and runtime admission checks remain mandatory.
The narrowed engine profile covers single-database text COPY, measured progress, pgoutput protocol v1 follow, persisted endpos and drain, schema/data comparison, and exact owned cleanup.
The command paths include `clone`, `follow`, `list progress --json`, the supported `stream sentinel get` and `stream sentinel set endpos` forms, `stream cleanup`, and `compare schema` or `compare data`.
The profile requires PostgreSQL 16 or 18 with source major no greater than target, explicit large-object skipping for follow, unsplit text COPY, and `durable` mode.
Binary COPY, splitting, large objects, replay-safe cadence, and relaxed durability remain outside this advertisement.
Discovery permits capability assessment; it does not open operator admission or certify helper, database, recovery, or cutover behavior.

The probe requires `/usr/local/bin/rpgmigrate` to be a regular file without a final symlink and records its actual SHA-256 from `sha256sum` at that exact path.
The receipt retains this observed digest as `runtime_binary_sha256`, a lowercase 64-digit string.
The hash is an observation from the selected immutable image, with no caller-supplied expected binary hash or qualification authority.
The receipt's `image_id` is the local image configuration ID returned by inspection; a registry manifest digest is a separate artifact identity.
`build_inputs_sha256` identifies the complete source-input manifest, while `runtime_binary_sha256` identifies the installed executable bytes.
Retain any image archive hash and its mapping to the image configuration ID or registry manifest digest alongside these fields.
A tag, revision label, source-input digest, executable hash, and image manifest digest each describe different evidence and cannot substitute for one another.

Keep the passing fixture-inventory result and hashes of the smoke script, contract, qualified golden, and fixture manifest with the frozen qualification packet or exact CI commit.
These qualification inputs remain outside the production build context and its source-input digest.

## Writable mounts and isolation

The configured uid and gid are both `65532`, and the working directory is `/work`.
The default command is `rpgmigrate --help`; no entrypoint prevents callers from running shell commands.

> [!important]
> Set the root filesystem read-only when starting the container.
> Mount writable `/work` and `/tmp` directories owned by `65532:65532`.
> The Dockerfile declares no volumes and cannot enforce runtime read-only flags.

The smoke uses temporary filesystems for both writable paths, disables networking, drops all capabilities, and enables `no-new-privileges`.
We disable Podman's automatic read-only-container temporary mounts and use `--mount` with `U=true` to assign both explicit mounts to the configured user, following the [Podman runtime documentation](https://docs.podman.io/en/latest/markdown/podman-run.1.html).
It verifies the configured and effective uid, actual machine architecture, exact discovery document, executable hash, linked libpq resolution and installed PGDG package ownership, both client-tool majors, default `psql`, GNU utilities, and retained notices.
The Python probe imports the required standard-library modules, checks the libpq symbol inventory, and calls `PQlibVersion` with an explicit ctypes ABI without opening a connection.
It validates the fixed runtime metadata file as a regular root-owned mode `0444` file, checks its size and exact schema, and binds its canonical library path and hash to installed package ownership, architecture, version, and the mapped library.
The receipt retains the metadata digest, installed binary/source package versions, resolved package copyright paths and hashes, Python license bytes by hash, Cargo notice inventory, and repository NOTICE hash.
Package notices stay in their distro locations with their original bytes.
The probe also runs a private script through a private FIFO and requires its synthetic input to remain absent from Python's command line and public output.
It also checks the observed root mount is read-only, writes in both allowed mounts, and requires a fresh write in `/var/tmp` to fail.
Missing observations, missing tools or libraries, malformed metadata, and failed subprocesses are failures.
The smoke accepts no database endpoints or credentials.

## Private helper boundary

The operator owns its private Python script, admission, credentials, connection behavior, timeouts, and cleanup.
The image installs no helper executable and adds no engine CLI capability.
Use the fixed launch prefix `/usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B` and keep connection input out of arguments and diagnostics.
The script uses no sibling imports or PyPI packages.
The runtime metadata has exactly `version`, `architecture`, `library_path`, `library_sha256`, `libpq_version`, `package_name`, and `package_version` fields.
Its schema version is integer `1`, architecture is OCI `arm64` or `amd64`, libpq is major 18, and the installed package version is nonempty ASCII without whitespace or control characters, bounded to 256 bytes.
The metadata is generated from actual installed facts during the build and is at most 4096 bytes.

> [!important]
> Exclude untrusted `LD_*` variables from the native Pod environment before the first process starts.
> The `/usr/bin/env` process itself uses the dynamic loader, so its `-u` flags are only defense in depth.
> Packaging smoke does not qualify real endpoint, passfile, URI, TLS, cancellation, or PVC admission behavior.

## Qualification boundaries

Run `python3 tools/image/test_smoke.py` for offline refusal controls.
Those controls validate the probe's behavior; they do not execute a container or qualify an architecture.
Native amd64 and arm64 build-and-smoke receipts are separate gates.
An arm64 result does not qualify amd64.

The M2 development image and clone matrix test engine behavior independently of the production runtime.
Packaging smoke does not qualify migration contents, follow/drain, crash recovery, power-loss durability, or operator integration.
Changing discovery metadata or the smoke contract requires fresh image build-and-smoke evidence for the rebuilt bytes.
Earlier M5 receipts remain evidence for their recorded images and empty discovery arrays; they do not qualify a rebuilt image under this contract.
M6 runtime acceptance must bind the selected image and executable digest to actual clone, progress, pgoutput follow, persisted endpos/drain, schema/data comparison, and exact owned cleanup on 16:16, 18:18, and 16:18.
Keep that acceptance pending until the exact artifact runs those checks.
Final release qualification runs the required migration matrix on the exact production image digest.
Publication remains a separate authorized release step after licensing and qualification gates pass.
