FROM docker.io/library/rust:1.99.1-trixie@sha256:8a4e90c7946b74626488b1446f712c0fe0ed1a596cbd3d871684bc9c5433ae07 AS builder

# The signed snapshot fixes transitive dependencies without disabling signature checks.
RUN rm /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261106T000000Z trixie main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/20261106T000000Z trixie-security main' > /etc/apt/sources.list \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      bison flex clang python3 libssl-dev libkrb5-dev libcurl4-openssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/rpgmigrate
ENV PQ_LIB_DIR=/opt/rpgmigrate/target/native/libpq/lib \
    CARGO_TERM_COLOR=never \
    PYTHONDONTWRITEBYTECODE=1
COPY . .
RUN python3 tools/build-libpq.py \
    && python3 tools/build-libpq.py --verify \
    && cargo build --locked --release --bin rpgmigrate \
    && python3 tools/image/runtime.py --stage /runtime \
    && dpkg-query -W -f='${binary:Package}=${Version}\n' > /runtime/usr/share/rpgmigrate/builder-packages.txt

FROM docker.io/library/debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

# The slim base lacks the openssl package's default cert.pem symlink.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /usr/lib/ssl/cert.pem

RUN rm /etc/apt/sources.list.d/debian.sources \
    && printf '%s\n' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/20261106T000000Z trixie main' \
      'deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/20261106T000000Z trixie-security main' > /etc/apt/sources.list \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      ca-certificates libssl3t64=3.5.7-1~deb13u3 \
      libcurl4t64=8.14.1-2+deb13u7 libgssapi-krb5-2=1.21.3-5+deb13u1 \
    && mkdir -p /usr/share/rpgmigrate \
    && dpkg-query -W -f='${binary:Package}=${Version}\n' > /usr/share/rpgmigrate/packages.txt \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /runtime/ /
# OAuth uses a basename with dlopen, so the controlled directory needs a loader-cache entry.
RUN printf '%s\n' /opt/rpgmigrate/target/native/libpq/lib > /etc/ld.so.conf.d/rpgmigrate.conf \
    && ldconfig \
    && groupadd --gid 1000 rpgmigrate \
    && useradd --uid 1000 --gid 1000 --create-home --shell /usr/sbin/nologin rpgmigrate \
    && mkdir /work && chown 1000:1000 /work \
    && /usr/local/bin/rpgmigrate --help > /dev/null
ENV HOME=/home/rpgmigrate TMPDIR=/work
USER 1000:1000
WORKDIR /work
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/rpgmigrate"]
CMD ["--help"]
