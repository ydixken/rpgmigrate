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
COPY Cargo.toml Cargo.lock build.rs NOTICE Dockerfile .dockerignore ./
COPY src/ ./src/
RUN find Cargo.toml Cargo.lock build.rs NOTICE Dockerfile .dockerignore src -type f -print0 \
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
       'libpq5=18.*' postgresql-client-16 postgresql-client-18 coreutils sed dash \
    && rm -rf /var/lib/apt/lists/* \
    && install -d -o 65532 -g 65532 -m 0700 /work
COPY --from=builder /build/target/release/rpgmigrate /usr/local/bin/rpgmigrate
COPY --from=builder /build/NOTICE /usr/share/rpgmigrate/NOTICE
COPY --from=builder /build-inputs.sha256 /usr/share/rpgmigrate/build-inputs.sha256
COPY --from=builder /dependency-notices/ /usr/share/rpgmigrate/dependency-notices/
ENV PATH="/usr/local/bin:/usr/lib/postgresql/18/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"
USER 65532:65532
WORKDIR /work
CMD ["rpgmigrate", "--help"]
