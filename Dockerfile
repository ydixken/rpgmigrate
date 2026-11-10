FROM docker.io/library/rust:1.99.1-trixie@sha256:8a4e90c7946b74626488b1446f712c0fe0ed1a596cbd3d871684bc9c5433ae07 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl gnupg pkg-config \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl --fail --silent --show-error https://www.postgresql.org/media/keys/ACCC4CF8.asc -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && gpg --batch --show-keys --with-colons /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc > /tmp/pgdg-key \
    && test "$(sed -n 's/^fpr:::::::::\([^:]*\):$/\1/p' /tmp/pgdg-key)" = B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8 \
    && printf '%s\n' 'deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt trixie-pgdg main' > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends 'libpq-dev=18.*' \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs LICENSE NOTICE Dockerfile .dockerignore ./
COPY src/ ./src/
RUN find Cargo.toml Cargo.lock build.rs LICENSE NOTICE Dockerfile .dockerignore src -type f -print0 \
    | LC_ALL=C sort -z | xargs -0 sha256sum > /build-inputs.sha256
RUN CARGO_BUILD_JOBS=2 cargo build --locked --release
RUN set -eu; mkdir /dependency-notices; \
    for package in /usr/local/cargo/registry/src/*/*; do \
         destination="/dependency-notices/$(basename "$package")"; \
         mkdir "$destination"; \
         (cd "$package" && find . -type f \( -iname 'LICENSE*' -o -iname 'LICENCE*' -o -iname 'COPYING*' -o -iname 'COPYRIGHT*' -o -iname 'NOTICE*' \) \
          -exec cp --parents -t "$destination" {} +); \
         test -n "$(find "$destination" -type f -print -quit)"; \
    done; \
    test -s /dependency-notices/unicode-ident-1.0.27/LICENSE-UNICODE

FROM docker.io/library/debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc
COPY --from=builder /etc/apt/sources.list.d/pgdg.list /etc/apt/sources.list.d/pgdg.list
RUN apt-get update && apt-get install -y --no-install-recommends \
       'libpq5=18.*' postgresql-client-16 postgresql-client-18 coreutils sed dash python3 \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o 65532 -g 65532 -m 0700 /work
COPY --from=builder /build/target/release/rpgmigrate /usr/local/bin/rpgmigrate
COPY --from=builder /build/LICENSE /usr/share/rpgmigrate/LICENSE
COPY --from=builder /build/NOTICE /usr/share/rpgmigrate/NOTICE
COPY --from=builder /build-inputs.sha256 /usr/share/rpgmigrate/build-inputs.sha256
COPY --from=builder /dependency-notices/ /usr/share/rpgmigrate/dependency-notices/
# Use continued shell syntax because distro Buildah versions can lack heredoc parsing.
RUN printf '%s\n' \
    'import ctypes' \
    'import hashlib' \
    'import json' \
    'import os' \
    'import stat' \
    'import subprocess' \
    '' \
    '' \
    'def query(*args):' \
    '    return subprocess.run(["/usr/bin/dpkg-query", *args], check=True, text=True,' \
    '                          capture_output=True, timeout=30).stdout.strip()' \
    '' \
    '' \
    'architecture = {"aarch64": "arm64", "x86_64": "amd64"}[os.uname().machine]' \
    'package, version, package_arch, status = query("-W", "-f=${Package}\t${Version}\t${Architecture}\t${Status}", "libpq5").split("\t")' \
    'assert package == "libpq5" and package_arch == architecture and status == "install ok installed"' \
    'assert 0 < len(version) <= 256 and all(33 <= ord(char) <= 126 for char in version)' \
    'paths = {os.path.realpath(path) for path in query("-L", "libpq5").splitlines()' \
    '         if os.path.basename(path).startswith("libpq.so.5") and os.path.isfile(path)}' \
    'assert len(paths) == 1' \
    'path = paths.pop()' \
    'assert os.path.isabs(path) and stat.S_ISREG(os.lstat(path).st_mode)' \
    'assert query("-S", path) == "libpq5:" + architecture + ": " + path' \
    'library = ctypes.CDLL(path)' \
    'library.PQlibVersion.argtypes = []' \
    'library.PQlibVersion.restype = ctypes.c_int' \
    'libpq_version = library.PQlibVersion()' \
    'assert 180000 <= libpq_version < 190000' \
    'with open(path, "rb") as source:' \
    '    digest = hashlib.sha256(source.read()).hexdigest()' \
    'metadata = {"version": 1, "architecture": architecture, "library_path": path,' \
    '            "library_sha256": digest, "libpq_version": libpq_version,' \
    '            "package_name": package, "package_version": version}' \
    'data = (json.dumps(metadata, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")' \
    'assert len(data) <= 4096' \
    'path = "/usr/share/rpgmigrate/python-libpq-runtime.json"' \
    'with open(path, "xb") as target:' \
    '    target.write(data)' \
    'os.chown(path, 0, 0)' \
    'os.chmod(path, 0o444)' \
    | /usr/bin/python3 -I -B
ARG SOURCE_REVISION
LABEL org.opencontainers.image.source="https://github.com/ydixken/rpgmigrate" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      org.opencontainers.image.licenses="Apache-2.0"
ENV PATH="/usr/local/bin:/usr/lib/postgresql/18/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"
USER 65532:65532
WORKDIR /work
CMD ["rpgmigrate", "--help"]
