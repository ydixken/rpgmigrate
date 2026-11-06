# Controlled libpq build

We build an explicitly patched native libpq dependency for the Rust transport.
Rust owns migration behavior; authentication, TLS, GSS, OAuth, service files, and passfiles still use native PostgreSQL code.

## Source and patch identity

[source.json](../source.json) pins the official PostgreSQL 18.6 source archive, its published SHA-256, each patch, and the complete input and output hashes of every patched file.
The archive SHA-256 is `555610c24d53e4316da5b7d3fc25c279d96856d5e0e23ee308c328c5fa881d9f`.
The helper checks that hash before extraction and applies patches with zero fuzz after checking the input files.
It checks the resulting files before compilation.
The downloaded PostgreSQL source stays under the ignored `target/` directory.

The [official release notes](https://www.postgresql.org/docs/release/18.6/) date 18.6 to August 13, 2026 and describe its security and bug fixes, including libpq SSL/GSS buffer draining fixes.
The [version policy](https://www.postgresql.org/support/versioning/) listed 18.6 as the current supported 18.x minor release when the source pin was checked on November 6, 2026.
Review those primary sources when updating the pin; this build does not update PostgreSQL servers or client utilities.

The first patch deletes only the two `fprintf(stderr, ...)` calls in `passwordFromFile` that print rejected passfile paths.
It also corrects the adjacent comment.
File opening, `fstat`, file-type and mode checks, close operations, NULL returns, and configuration precedence remain unchanged.
The second patch adds `unsigned int PQrpgmigrateCapabilities(void)`, returning `1U`, its public declaration, and export ordinal 211.
Bit 0 means that the reviewed passfile diagnostics are suppressed.
`PQlibVersion()` still returns `180006`.

## Requirements

Use Python 3.12 or newer with its standard library, a C compiler, GNU Make, `patch`, `pkg-config`, Perl, Bison, and Flex.
The PostgreSQL configure script checks tool and library compatibility.
The helper requires the installed `openssl`, `krb5-gssapi`, and `libcurl` pkg-config packages, including their development headers and libraries.
It installs no dependencies.
On macOS it adds an existing Homebrew Kerberos pkg-config directory after any explicit `PKG_CONFIG_PATH` entries.
Use `PKG_CONFIG_PATH`, `CC`, `CFLAGS`, `CPPFLAGS`, and `LDFLAGS` to select an existing reviewed toolchain.

The implementation accepts macOS and Linux.
The initial build was verified on macOS arm64 with Apple clang 21.0.0, GNU Make 3.81, OpenSSL 4.1.0, Kerberos 1.22.2, and the macOS libcurl development metadata version 8.7.1.
Linux build and runtime validation remain a separate verification step.
Unsupported platforms, missing prerequisites, failed checksums, patch drift, and absent capabilities return nonzero.
There is no fallback to stock libpq.

## Build and verify

Run these steps from the repository root.

1. Build the reviewed library and OAuth module into this checkout.

   ```sh
   python3 tools/build-libpq.py
   ```

2. Verify the installed file hashes and exported capability before using it.

   ```sh
   python3 tools/build-libpq.py --verify
   ```

3. Run the native builder tests against the cached source.

   ```sh
   python3 -m unittest discover -s tests -p test_native_libpq.py -v
   ```

4. Select the same library directory when invoking Cargo directly.

   ```sh
   PQ_LIB_DIR="$PWD/target/native/libpq/lib" cargo build --locked
   ```

The download cache is `target/native/downloads/`.
The install prefix is `target/native/libpq/`.
Temporary source and build directories remain under `target/native/` and are removed after the attempt.
The helper builds only libpq, its OAuth module, their required common/port code, and generated headers.
It installs no server, command-line utility, or system package.
The build log is `target/native/build.log`.
The build lock rejects overlapping helper invocations.
The helper clears inherited Make recursion state so a parent Make target cannot skip PostgreSQL's generated headers.
It sets `ZERO_AR_DATE=1` to prevent macOS archive-member timestamps from changing the static library bytes between local builds.
Existing output symlinks that point outside the controlled prefix cause failure before a rebuild.

Check out these **examples**:

- Require the existing verified download cache.

  ```sh
  python3 tools/build-libpq.py --offline
  ```

- Rebuild the local artifact after an intentional recipe or toolchain change.

  ```sh
  python3 tools/build-libpq.py --offline --rebuild
  ```

Rebuild while no process uses this local library.
Normal repeated invocations verify and reuse an unchanged installation.
`--verify` uses no network and does not compile anything.
It rejects `PQ_LIB_DIR` when that variable selects another directory.

## Runtime selection and configuration

On macOS, the dylib install name contains the canonical local prefix.
On Linux, the library uses the standard `libpq.so.5` SONAME.
The root build integration supplies a runtime search path derived from the selected local library directory.
Rebuild after moving the checkout; this local build is not a relocatable distribution bundle.
OpenSSL, GSS, and libcurl remain external runtime dependencies supplied by the existing toolchain.

The Rust transport looks up `PQrpgmigrateCapabilities` at runtime and uses `dladdr` to require the same loaded image as `PQconnectStart`.
A missing marker, wrong capability, or marker from another library returns structured `BadConfig` before connecting.
A missing shared library can still prevent the operating-system loader from starting the executable.
The marker identifies the patch contract; the receipt verifies exact local bytes.
Neither changes the PostgreSQL version API.

We retain upstream environment, service, passfile, and default precedence.
The compiled system service-file directory is under this local prefix, as with a normal PostgreSQL prefix installation.
Use upstream `PGSYSCONFDIR` to select a different system service directory.
Per-user defaults and explicit `PGSERVICEFILE` and `PGPASSFILE` remain supported.
The helper does not read or change user database services or credentials.

## Artifact identity and notices

`target/native/libpq/artifact.json` records the recipe digest, source and patch provenance, configure arguments, compiler identity, dependency versions, enabled features, capability value, and hashes of installed files.
The helper prints the main library SHA-256 and the digest of the installed file-hash set.
Those digests identify the built bytes; dependency and toolchain changes can produce different bytes.
The source pin alone does not claim identical output across compilers, machines, or install prefixes.

The verified source archive's [COPYRIGHT](../COPYRIGHT) and our [NOTICE](../NOTICE) are installed in `share/licenses/libpq/` under the local prefix.
Retain both when distributing this modified native dependency.
The source is licensed under the [PostgreSQL License](https://www.postgresql.org/about/licence/).
External libraries retain their own notices and are not copied by this helper.
