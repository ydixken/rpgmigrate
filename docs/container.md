# Runtime container

The runtime image packages the implemented rpgmigrate CLI with the controlled libpq dependency.
It has the same [first-slice limits](getting-started.md#first-slice-limits) as a local build.
The [task status](../tasks/todo.md) records completed image checks and publication evidence.
A configured workflow or a built archive alone does not establish a passing smoke test or an available registry image.

## Runtime boundary

The [Dockerfile](../Dockerfile) builds the executable and patched libpq in a separate builder stage.
The runtime uses UID and GID 1000, `/work` as its working directory, and SIGTERM as its stop signal.
The executable is its entry point.
Each runtime variant carries exactly one matching `pg_dump`, `pg_restore`, and `pg_config` trio: PostgreSQL 16.15 or 18.6.
No PostgreSQL server, psql, other PostgreSQL utility, Python interpreter, Cargo, or Rust compiler belongs in the runtime image.
The independent fixture image supplies the database used for verification.
These utilities export SQL only; target effects belong to the native fenced session.
Packaging does not enable clone or the eight unsupported operator command paths.

The builder accepts `--client-version 16.15` or `--client-version 18.6`, with 18.6 as the default.
It passes the closed `PG_CLIENT_VERSION` Docker build argument to the existing native builder.
Both variants retain the application's patched libpq18 and its exact capability checks.
The clients use a separately built same-version libpq under `/opt/rpgmigrate/target/native/pgtools`; their embedded library search path keeps it separate from the application's library.
We retain the silent passfile-rejection patch in both client libraries to prevent utility startup diagnostics from exposing paths.
The PG16 library has no OAuth support; it does not replace the application's OAuth-capable libpq18.
`PG_CONFIG` and PATH select the single installed client prefix, and `pg_config --bindir` must report that prefix's actual runtime `bin` directory.
There is no automatic selection between client versions inside an image.
The export policy requires identical dump/restore versions and source major <= client major <= target major.
Use the PG16 variant for PG16 targets and the PG18 variant for PG18 sources.

The native prefix remains `/opt/rpgmigrate/target/native/libpq` so the executable and OAuth module use the controlled library location.
OpenSSL, GSSAPI, libcurl, and certificate packages remain runtime dependencies.
The loader cache includes the controlled directory because libpq loads its OAuth module by basename.
The image preserves the original native build receipt and a separate manifest for the trimmed runtime files.
It retains project notices, PostgreSQL attribution, Rust and dependency notices, and Debian package copyright files.
The project's own license choice remains pending; the container adds no replacement license.

## Build and verify locally

Use an existing native Linux Podman engine and the [pinned Task bootstrap](getting-started.md#run-the-checks).
The engine can run in a local virtual machine.
The image builder refuses a platform that differs from the engine's native architecture.
Use an immutable source checkout or a reviewed source export throughout the build and smoke checks.

1. Confirm the selected engine and source checkout.

   ```sh
   podman version
   git status --short --branch
   ```

2. Run the source fixture and capture the exact fixture image identifier from that invocation.
   The launcher writes its normal CI output fields to the selected file.

   ```sh
   mkdir -p target
   rpgmigrate_fixture_output=$(mktemp "$PWD/target/container-fixture.XXXXXX")
   GITHUB_OUTPUT="$rpgmigrate_fixture_output" task ci:linux
   rpgmigrate_fixture_image=$(sed -n 's/^fixture_image=//p' "$rpgmigrate_fixture_output")
   ```

3. Build one native runtime artifact into a new directory.
   This example uses arm64; use `linux/amd64` on a native amd64 engine.

   ```sh
   task image:build -- --engine podman --platform linux/arm64 --output target/runtime-image
   ```

4. Verify the exported artifact with the exact fixture image from step two.

   ```sh
   task image:smoke -- --engine podman --receipt target/runtime-image/receipt.json \
     --fixture-image "$rpgmigrate_fixture_image" --output target/runtime-smoke
   ```

To build the PG16 variant, repeat the build with a new output directory and the explicit version input.

1. Build the pinned PG16 client variant.

   ```sh
   task image:build -- --engine podman --platform linux/arm64 --client-version 16.15 --output target/runtime-image-pg16
   ```

Both output directories must be new.
The build produces `image.oci.tar` and `receipt.json`; the smoke helper produces `smoke.json`.
Missing inputs, mismatched artifacts, failed checks, and incomplete receipts return nonzero.
Keep the local build log private, because it is diagnostic output rather than a sanitized publication artifact.

## What the smoke checks prove

The host drives the application and fixture containers on a fresh internal network with no published ports.
It does not mount the container-engine socket or add a control service to the application image.
The fixture owns disposable databases, SCRAM credentials, protocol faults, and independent data oracles.
Its optional TCP mode leaves the normal socket-only harness unchanged.
Cleanup targets only resources created for that run.

The smoke inventory checks archive and loaded-image identity, runtime files and permissions, native capabilities, and matching libpq symbols.
It requires a structured refusal when stock libpq is substituted and explicitly loads the OAuth module.
It also checks real ping, COPY contents and negative controls, SIGINT/SIGTERM cancellation, persistent state storage, and state lock/identity refusals.
Every required check must appear once in the receipt with a passing result.
Help and version output alone cannot qualify the image.

The builder records the three actual utility version responses, the configured bindir, and the same-version source and library identities.
The OCI validator independently reads ELF dependencies and checks the recorded inventory against files, architectures, sonames, interpreter aliases, and hashes in the image.
This static inventory check does not prove which library the final runtime loader selects.
The smoke helper runs all three version commands and the actual bindir query inside the loaded nonroot image as part of its runtime-file check.
Its mandatory `client-loader-resolution` check traces each client utility and shared object with the final image's `ldd`, under the image's configured environment and working directory.
It compares every reachable DT_NEEDED edge and interpreter with the recorded canonical targets, then verifies the selected files' hashes inside that image.
This includes the loader's absolute-path trace lines, which have no `=>` separator.
Missing loader evidence, different selected paths or bytes, missing or wrong versions, unlisted utilities, and mixed client variants refuse qualification.
The existing smoke receipt must include the new check; an older receipt without it cannot pass acceptance or publication validation.
Offline fixture tests do not replace full native Linux image builds and smoke checks for both versions on amd64 and arm64.

The receipt binds the archive, manifest, configuration, executable, native files, runtime files, and actual source-file hashes.
The optional `--revision` build argument is a label, not proof that the source bytes belong to a commit.
For a local source export, retain the root's frozen-tree manifest with the receipt.
CI separately compares the captured source bytes with the workflow commit before sealing publication evidence.

## Publication boundary

The [workflow](../.github/workflows/ci.yml) defines native amd64 and arm64 verification jobs.
Each client version uses the same native amd64/arm64 matrix.
Each job must pass the source fixture and smoke the exact artifact that it uploads.
The publisher accepts only the two expected architectures from the same workflow run and source identity.
It combines their tested OCI descriptors without rebuilding the executable and verifies the destination index and child digests.

The PG18 variant preserves the existing version tag.
The PG16 variant uses the same tag with `-pg16` appended.
We reserve that suffix for generated client-variant identities, so a source release tag ending in `-pg16` is refused before publication.
The publisher refuses to combine different client versions in one platform index.

Publication is limited to version-tag pushes for `ghcr.io/ydixken/rpgmigrate`.
Branch and pull-request verification do not publish an image.
No task here creates a Git tag, GitHub release, or production deployment.
Consult the maintained status before referring users to a public image.
