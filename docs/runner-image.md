# Runner image

We package the existing `rpgmigrate` executable with stock PGDG libpq 18 and PostgreSQL client tools 16 and 18.
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
- `/usr/share/rpgmigrate/NOTICE` retains the repository's third-party notice text.
- `/usr/share/doc/<package>/copyright` retains Debian and PGDG package notices.
- `/usr/share/rpgmigrate/dependency-notices/<name-version>` retains Cargo dependency license and notice files, preserving their relative paths and bytes.
- `/usr/share/rpgmigrate/build-inputs.sha256` records each source, manifest, build script, notice, and image-definition input by content hash.

We exclude Git metadata, tests, local configuration, credentials, and unrelated repository files from the build context with an allowlist.
The build-input manifest describes the working-tree bytes copied into the builder, including uncommitted edits.
Its digest in the smoke receipt identifies those inputs without claiming that they match a Git commit.
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
It also checks the observed root mount is read-only, writes in both allowed mounts, and requires a fresh write in `/var/tmp` to fail.
Missing observations, missing tools or libraries, malformed metadata, and failed subprocesses are failures.
The smoke accepts no database endpoints or credentials.

## Qualification boundaries

Run `python3 tools/image/test_smoke.py` for offline refusal controls.
Those controls validate the probe's behavior; they do not execute a container or qualify an architecture.
Native amd64 and arm64 build-and-smoke receipts are separate gates.
An arm64 result does not qualify amd64.

The M2 development image and clone matrix test engine behavior independently of the production runtime.
Packaging smoke does not qualify migration contents, follow/drain, crash recovery, power-loss durability, or operator integration.
Final release qualification runs the required migration matrix on the exact production image digest.
Publication remains a separate authorized release step after licensing and qualification gates pass.
