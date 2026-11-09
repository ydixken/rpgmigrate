# Runner image

We package the existing `rpgmigrate` executable with stock PGDG libpq 18, PostgreSQL client tools 16 and 18, and the distro Python 3 standard library.
The image is a packaging artifact; migration support follows the engine's qualified capabilities.

## Build and smoke locally

Use a running Podman instance on the native architecture.
The production image build is separate from the M2 development-container build.

1. Build the runner from the working tree.

   ```sh
   task image:build
   ```

2. Check the built image without rebuilding it.

   ```sh
   task image:smoke
   ```

3. Retain the immutable image ID and observations in a receipt.

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

## Writable mounts and isolation

The configured uid and gid are both `65532`, and the working directory is `/work`.
The default command is `rpgmigrate --help`; no entrypoint prevents callers from running shell commands.

> [!important]
> Set the root filesystem read-only when starting the container.
> Mount writable `/work` and `/tmp` directories owned by `65532:65532`.
> The Dockerfile declares no volumes and cannot enforce runtime read-only flags.

The smoke uses temporary filesystems for both writable paths, disables networking, drops all capabilities, and enables `no-new-privileges`.
We disable Podman's automatic read-only-container temporary mounts and use `--mount` with `U=true` to assign both explicit mounts to the configured user, following the [Podman runtime documentation](https://docs.podman.io/en/latest/markdown/podman-run.1.html).
It verifies the configured and effective uid, actual machine architecture, version protocol, linked libpq resolution and installed PGDG package ownership, both client-tool majors, default `psql`, GNU utilities, and retained notices.
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
Final release qualification runs the required migration matrix on the exact production image digest.
Publication remains a separate authorized release step after licensing and qualification gates pass.
