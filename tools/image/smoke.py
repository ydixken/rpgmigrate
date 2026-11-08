#!/usr/bin/env python3
"""Prove the local runner's packaging without accepting database credentials."""

import argparse
import json
import os
import re
import subprocess
import sys


HEADER = re.compile(r"^===rpgmigrate-smoke:([a-z0-9_]+)===$", re.MULTILINE)
EMIT = r'''
set -eu
emit() {
    name=$1
    shift
    value=$("$@") || exit 1
    printf '===rpgmigrate-smoke:%s===\n%s\n' "$name" "$value"
}
'''
PROBE = EMIT + r'''
emit uid id -u
emit gid id -g
emit machine uname -m
emit version /usr/local/bin/rpgmigrate version --json
emit plain_version /usr/local/bin/rpgmigrate version
emit ldd ldd /usr/local/bin/rpgmigrate
emit coreutils id --version
emit sed sed --version
emit default_psql psql --version
emit mountinfo cat /proc/self/mountinfo
emit work_owner stat -c '%u:%g:%a' /work
emit tmp_owner stat -c '%u:%g:%a' /tmp
emit inputs sha256sum /usr/share/rpgmigrate/build-inputs.sha256
test -s /usr/share/rpgmigrate/NOTICE
test -s /usr/share/doc/libpq5/copyright
test -s /usr/share/doc/postgresql-client-16/copyright
test -s /usr/share/doc/postgresql-client-18/copyright
test -s /usr/share/rpgmigrate/dependency-notices/unicode-ident-1.0.27/LICENSE-UNICODE
emit notice printf present
touch /work/rpgmigrate-smoke
emit work_write printf ok
touch /tmp/rpgmigrate-smoke
emit tmp_write printf ok
test ! -e /var/tmp/rpgmigrate-smoke-root
if touch /var/tmp/rpgmigrate-smoke-root 2>/dev/null; then exit 1; fi
emit root_write printf denied
for major in 16 18; do
    for tool in pg_dump pg_restore psql; do
        emit "${tool}_${major}" "/usr/lib/postgresql/$major/bin/$tool" --version
    done
done
'''
PACKAGE = EMIT + r'''
path=$(readlink -f "$1")
emit path printf '%s' "$path"
emit owner dpkg-query -S "$path"
emit package dpkg-query -W '-f=${Package}\t${Version}\t${Architecture}\t${Status}' libpq5
'''


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def run(argv):
    try:
        return subprocess.run(argv, check=True, text=True, capture_output=True, timeout=120).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise RuntimeError("image smoke subprocess failed") from error


def records(output):
    matches = list(HEADER.finditer(output))
    require(matches and not output[:matches[0].start()].strip(), "malformed smoke observations")
    values = {}
    for index, match in enumerate(matches):
        name = match[1]
        require(name not in values, "duplicate smoke observation")
        end = matches[index + 1].start() if index + 1 < len(matches) else len(output)
        value = output[match.end():end].strip()
        require(value, "empty smoke observation")
        values[name] = value
    return values


def probe(image, runtime):
    require(image and not image.startswith("-"), "invalid image reference")
    try:
        metadata = json.loads(run([runtime, "image", "inspect", image]))
        require(isinstance(metadata, list) and len(metadata) == 1, "invalid image metadata")
        metadata = metadata[0]
        image_id = metadata["Id"]
        if isinstance(image_id, str) and re.fullmatch(r"[0-9a-f]{64}", image_id):
            image_id = "sha256:" + image_id
        architecture = metadata["Architecture"]
        config = metadata["Config"]
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", image_id), "invalid immutable image ID")
        require(metadata["Os"] == "linux" and architecture in ("amd64", "arm64"), "unsupported image platform")
        require(config["User"] == "65532:65532" and config["WorkingDir"] == "/work", "invalid image user or workdir")
        require(not config.get("Entrypoint") and config["Cmd"] == ["rpgmigrate", "--help"], "invalid image command")
        require(not config.get("Volumes"), "image must not declare volumes")
        base = [runtime, "run", "--rm", "--read-only", "--read-only-tmpfs=false", "--network=none", "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--mount", "type=tmpfs,destination=/work,tmpfs-mode=0700,U=true",
                "--mount", "type=tmpfs,destination=/tmp,tmpfs-mode=0700,U=true", image_id, "/bin/sh", "-c"]
        observed = records(run([*base, PROBE]))
        require(observed["uid"] == "65532" and observed["gid"] == "65532", "wrong effective uid/gid")
        require(observed["machine"] == {"amd64": "x86_64", "arm64": "aarch64"}[architecture], "architecture mismatch")
        version = json.loads(observed["version"])
        require(isinstance(version, dict) and set(version) == {"protocol_version", "engine_version", "capabilities"}, "invalid version protocol")
        require(type(version["protocol_version"]) is int and version["protocol_version"] == 1
                and re.fullmatch(r"\d+\.\d+\.\d+", version["engine_version"])
                and version["capabilities"] == [], "unexpected version contract")
        plain = re.fullmatch(r"rpgmigrate " + re.escape(version["engine_version"]) + r"\nlibpq (18\d{4})", observed["plain_version"])
        require(plain, "wrong runtime libpq version")
        require("not found" not in observed["ldd"], "unresolved native library")
        links = re.findall(r"^\s*libpq\.so\.5\s+=>\s+(/[^\s]+)\s+\(", observed["ldd"], re.MULTILINE)
        require(len(links) == 1, "missing resolved libpq dependency")
        require("GNU coreutils" in observed["coreutils"] and "GNU sed" in observed["sed"], "missing required GNU utilities")
        clients = {}
        for major in (16, 18):
            clients[str(major)] = {}
            for tool in ("pg_dump", "pg_restore", "psql"):
                value = observed[f"{tool}_{major}"]
                require(re.match(re.escape(tool) + rf" \(PostgreSQL\) {major}\.\d+\b", value), "wrong PostgreSQL client major")
                clients[str(major)][tool] = value
        require(re.match(r"psql \(PostgreSQL\) 18\.\d+\b", observed["default_psql"]), "default psql is not major 18")
        mounts = [line.split() for line in observed["mountinfo"].splitlines()]
        mount_modes = {}
        for path, mode in (("/", "ro"), ("/work", "rw"), ("/tmp", "rw")):
            matching = [fields for fields in mounts if len(fields) > 6 and fields[4] == path]
            require(len(matching) == 1 and mode in matching[0][5].split(","), "wrong observed mount mode")
            mount_modes[path] = matching[0][5]
        require(observed["work_write"] == "ok" and observed["tmp_write"] == "ok"
                and observed["root_write"] == "denied", "incorrect write isolation")
        require(observed["work_owner"] == "65532:65532:700" and observed["tmp_owner"] == "65532:65532:700", "incorrect writable mount ownership")
        inputs = re.fullmatch(r"([0-9a-f]{64})  /usr/share/rpgmigrate/build-inputs\.sha256", observed["inputs"])
        require(inputs and observed["notice"] == "present", "missing provenance or notices")
        package = records(run([*base, PACKAGE, "rpgmigrate-smoke", links[0]]))
        require(re.fullmatch(r"/usr/lib/[^/]+/libpq\.so\.5\.\d+", package["path"]), "invalid resolved libpq path")
        require(package["owner"] == f"libpq5:{architecture}: {package['path']}", "libpq is not owned by libpq5")
        fields = package["package"].split("\t")
        require(len(fields) == 4 and fields[0] == "libpq5" and fields[2] == architecture
                and fields[3] == "install ok installed" and re.fullmatch(r"18\.\d+[^\s]*\.pgdg13\+[^\s]+", fields[1]), "wrong stock PGDG libpq package")
        minor = int(re.match(r"18\.(\d+)", fields[1])[1])
        require(int(plain[1]) == 180000 + minor, "runtime libpq differs from installed package")
        return {"image_id": image_id, "architecture": architecture, "effective_uid": 65532,
                "version": version, "libpq": {"runtime_version": int(plain[1]), "ldd": observed["ldd"],
                "resolved_path": package["path"], "package_version": fields[1], "package_owner": package["owner"]},
                "clients": clients, "default_psql": observed["default_psql"], "build_inputs_sha256": inputs[1],
                "mounts": {"modes": mount_modes, "work_owner": observed["work_owner"], "tmp_owner": observed["tmp_owner"]},
                "checks": {"read_only_rootfs": True, "work_write": True, "tmp_write": True,
                           "root_write_denied": True, "notices": True, "gnu_utilities": True}}
    except (KeyError, TypeError, ValueError, IndexError) as error:
        raise RuntimeError("missing or malformed smoke observation/metadata") from error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()
    try:
        receipt = probe(args.image, os.environ.get("CONTAINER_RUNTIME", "podman"))
    except RuntimeError as error:
        print(f"image smoke failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
