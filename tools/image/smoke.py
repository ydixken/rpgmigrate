#!/usr/bin/env python3
"""Prove the local runner's packaging without accepting database credentials."""

import argparse
import json
import os
import hashlib
from pathlib import Path
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
PYTHON_RUNTIME = r'''
import ctypes
import _ctypes
import os
import stat
import fcntl
import selectors
import select
import time
import json
import hashlib
import struct
import errno
import sys
import binascii
import resource
import re
import subprocess

METADATA = '/usr/share/rpgmigrate/python-libpq-runtime.json'
MODULES = ('ctypes', '_ctypes', 'os', 'stat', 'fcntl', 'selectors', 'select', 'time',
           'json', 'hashlib', 'struct', 'errno', 'sys', 'binascii', 'resource')
SYMBOLS = ('PQlibVersion', 'PQconnectStart', 'PQconnectPoll', 'PQsetnonblocking', 'PQsocket',
           'PQstatus', 'PQtransactionStatus', 'PQbackendPID', 'PQsetNoticeReceiver',
           'PQresultErrorField', 'PQsendQueryParams', 'PQflush', 'PQconsumeInput', 'PQisBusy',
           'PQgetResult', 'PQresultStatus', 'PQcmdStatus', 'PQcmdTuples', 'PQntuples',
           'PQnfields', 'PQgetisnull', 'PQgetlength', 'PQgetvalue', 'PQclear', 'PQfinish',
           'PQescapeIdentifier', 'PQfreemem')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, 'duplicate runtime field')
        result[key] = value
    return result


def parse_metadata(data):
    require(0 < len(data) <= 4096, 'runtime metadata size')
    try:
        value = json.loads(data, object_pairs_hook=pairs)
    except (ValueError, UnicodeError) as error:
        raise RuntimeError('invalid runtime JSON') from error
    require(type(value) is dict and set(value) == {'version', 'architecture', 'library_path',
            'library_sha256', 'libpq_version', 'package_name', 'package_version'}, 'runtime fields')
    require(type(value['version']) is int and value['version'] == 1, 'runtime schema version')
    require(value['architecture'] in ('arm64', 'amd64'), 'runtime architecture')
    path = value['library_path']
    require(type(path) is str and path.startswith('/') and os.path.normpath(path) == path
            and '\x00' not in path and not path.startswith('//'), 'runtime library path')
    require(type(value['library_sha256']) is str
            and re.fullmatch('[0-9a-f]{64}', value['library_sha256']), 'runtime library hash')
    require(type(value['libpq_version']) is int and 180000 <= value['libpq_version'] < 190000,
            'runtime libpq version')
    require(value['package_name'] == 'libpq5', 'runtime package name')
    version = value['package_version']
    require(type(version) is str and 0 < len(version) <= 256
            and all(33 <= ord(char) <= 126 for char in version), 'runtime package version')
    return value


def check_ancestors(path):
    parent = os.path.dirname(path)
    while True:
        info = os.lstat(parent)
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == 0 and info.st_gid == 0
                and not info.st_mode & 0o022, 'untrusted runtime ancestor')
        if parent == '/':
            break
        parent = os.path.dirname(parent)


def file_identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_uid, info.st_gid, info.st_size, info.st_mtime, info.st_ctime)


def read_owned(path, limit, exact_mode=None):
    check_ancestors(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_gid == 0
                and not info.st_mode & 0o022 and 0 < info.st_size <= limit, 'untrusted runtime file')
        require(exact_mode is None or stat.S_IMODE(info.st_mode) == exact_mode, 'runtime file mode')
        with os.fdopen(os.dup(descriptor), 'rb') as source:
            data = source.read(limit + 1)
        require(len(data) == info.st_size and file_identity(os.fstat(descriptor)) == file_identity(info), 'runtime file changed')
        return data
    finally:
        os.close(descriptor)


def command(*args):
    return subprocess.run(args, check=True, text=True, capture_output=True, timeout=30).stdout.strip()


def package_for(path):
    owner = command('/usr/bin/dpkg-query', '-S', path)
    require(len(owner.splitlines()) == 1 and owner.endswith(': ' + path), 'ambiguous package owner')
    return owner.rsplit(': ', 1)[0].split(':', 1)[0]


def hashes(directory):
    result = {}
    for root, directories, files in os.walk(directory):
        directories.sort()
        for name in sorted(files):
            path = os.path.join(root, name)
            require(not os.path.islink(path), 'unexpected notice symlink')
            with open(path, 'rb') as source:
                result[os.path.relpath(path, directory)] = hashlib.sha256(source.read()).hexdigest()
    require(result, 'missing notice inventory')
    return result


def verify_library(metadata):
    path = metadata['library_path']
    require(os.path.realpath(path) == path and package_for(path) == 'libpq5', 'runtime package path')
    data = read_owned(path, 64 * 1024 * 1024)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    identity = file_identity(os.fstat(descriptor))
    try:
        require(hashlib.sha256(data).hexdigest() == metadata['library_sha256'], 'runtime hash mismatch')
        library = ctypes.CDLL(path)
        for symbol in SYMBOLS:
            getattr(library, symbol)
        library.PQlibVersion.argtypes = []
        library.PQlibVersion.restype = ctypes.c_int
        require(library.PQlibVersion() == metadata['libpq_version'], 'runtime ABI mismatch')
        with open('/proc/self/maps', encoding='ascii') as source:
            mapped = [line.split(maxsplit=5) for line in source if len(line.split(maxsplit=5)) == 6]
        info = os.fstat(descriptor)
        device = '%02x:%02x' % (os.major(info.st_dev), os.minor(info.st_dev))
        require(any(fields[5].strip() == path and fields[3] == device and fields[4] == str(info.st_ino)
                    for fields in mapped) and file_identity(os.stat(path)) == identity
                and file_identity(os.fstat(descriptor)) == identity
                and hashlib.sha256(read_owned(path, 64 * 1024 * 1024)).hexdigest() == metadata['library_sha256'],
                'loaded libpq identity mismatch')
    finally:
        os.close(descriptor)


def main():
    require(sys.version_info.major == 3 and sys.flags.isolated and sys.dont_write_bytecode,
            'wrong isolated Python runtime')
    raw = read_owned(METADATA, 4096, exact_mode=0o444)
    metadata = parse_metadata(raw)
    architecture = {'aarch64': 'arm64', 'x86_64': 'amd64'}.get(os.uname().machine)
    require(architecture == metadata['architecture'], 'Python architecture mismatch')
    verify_library(metadata)
    rows = command('/usr/bin/dpkg-query', '-W', '-f=${Package}\t${Version}\t${Architecture}\t${db:Status-Status}\t${source:Package}\t${source:Version}\n')
    packages = {}
    for row in rows.splitlines():
        name, version, arch, status, source, source_version = row.split('\t')
        if status != 'installed':
            continue
        copyright_path = os.path.realpath('/usr/share/doc/' + name + '/copyright')
        copyright_data = read_owned(copyright_path, 4 * 1024 * 1024)
        packages[name] = {'version': version, 'architecture': arch, 'source': source,
                          'source_version': source_version, 'copyright_path': copyright_path,
                          'copyright_sha256': hashlib.sha256(copyright_data).hexdigest()}
    required = ('python3', 'python3-minimal', 'python3.%d' % sys.version_info.minor,
                'python3.%d-minimal' % sys.version_info.minor,
                'libpython3.%d-stdlib' % sys.version_info.minor,
                'libpython3.%d-minimal' % sys.version_info.minor, 'libffi8', 'libc6', 'libpq5', 'libssl3t64',
                'postgresql-client-16', 'postgresql-client-18')
    require(all(name in packages for name in required), 'missing runtime package')
    pq = packages['libpq5']
    require(pq['version'] == metadata['package_version'] and pq['architecture'] == architecture,
            'runtime dpkg mismatch')
    require(package_for(os.path.realpath(_ctypes.__file__)) == 'libpython3.%d-stdlib' % sys.version_info.minor,
            'foreign ctypes module')
    license_path = os.path.realpath(os.path.join(os.path.dirname(os.__file__), 'LICENSE.txt'))
    license_data = read_owned(license_path, 4 * 1024 * 1024)
    canary = sys.stdin.buffer.read(65)
    require(canary == b'rpgmigrate-private-input-canary', 'private input mismatch')
    with open('/proc/self/cmdline', 'rb') as source:
        require(canary not in source.read(), 'private input exposed in argv')
    print(json.dumps({'version': sys.version.split()[0], 'isolated': True, 'modules': list(MODULES),
          'symbols': list(SYMBOLS), 'metadata': metadata, 'metadata_sha256': hashlib.sha256(raw).hexdigest(),
          'mapped_library': metadata['library_path'], 'ctypes': True, 'private_input': True, 'packages': packages,
          'python_license': {'path': license_path, 'sha256': hashlib.sha256(license_data).hexdigest()},
          'dependency_notices': hashes('/usr/share/rpgmigrate/dependency-notices'),
          'notice_sha256': hashlib.sha256(read_owned('/usr/share/rpgmigrate/NOTICE', 1024 * 1024)).hexdigest()},
          sort_keys=True))


if __name__ == '__main__':
    main()
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
emit inputs_manifest cat /usr/share/rpgmigrate/build-inputs.sha256
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
PROBE += r"""
private=$(mktemp -d /tmp/rpgmigrate-python.XXXXXXXX)
trap 'rm -rf "$private"' EXIT HUP INT TERM
chmod 0700 "$private"
cat > "$private/probe.py" <<'RPGMIGRATE_PYTHON'
""" + PYTHON_RUNTIME + r"""RPGMIGRATE_PYTHON
chmod 0400 "$private/probe.py"
mkfifo -m 0600 "$private/input"
printf '%s' 'rpgmigrate-private-input-canary' > "$private/input" &
writer=$!
emit python /usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B "$private/probe.py" < "$private/input"
wait "$writer"
"""
PACKAGE = EMIT + r'''
path=$(readlink -f "$1")
emit path printf '%s' "$path"
emit owner dpkg-query -S "$path"
emit package dpkg-query -W '-f=${Package}\t${Version}\t${Architecture}\t${Status}' libpq5
'''


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def unique_fields(items):
    result = {}
    for key, value in items:
        require(key not in result, "duplicate JSON observation field")
        result[key] = value
    return result


def decode_json(value):
    return json.loads(value, object_pairs_hook=unique_fields)


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


def source_manifest():
    root = Path(__file__).resolve().parents[2]
    paths = [root / name for name in ("Cargo.toml", "Cargo.lock", "build.rs", "NOTICE", "Dockerfile", ".dockerignore")]
    paths.extend(path for path in (root / "src").rglob("*") if path.is_file())
    return "".join(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + str(path.relative_to(root)) + "\n"
                   for path in sorted(paths, key=lambda path: str(path.relative_to(root))))


def validate_python(value, architecture, package, runtime_version):
    require(type(value) is dict and set(value) == {"version", "isolated", "modules", "symbols", "metadata",
            "metadata_sha256", "mapped_library", "ctypes", "private_input", "packages", "python_license",
            "dependency_notices", "notice_sha256"}, "invalid Python observations")
    namespace = {"__name__": "runtime_contract"}
    exec(compile(PYTHON_RUNTIME, "image-python-contract", "exec"), namespace)
    metadata = namespace["parse_metadata"](json.dumps(value["metadata"]).encode())
    require(re.fullmatch(r"3\.\d+\.\d+", value["version"]) and value["isolated"] is True
            and value["ctypes"] is True and value["private_input"] is True, "invalid Python runtime")
    require(value["modules"] == list(namespace["MODULES"]) and value["symbols"] == list(namespace["SYMBOLS"]),
            "missing Python imports or libpq symbols")
    require(metadata["architecture"] == architecture and metadata["library_path"] == package["path"]
            and metadata["package_version"] == package["package"].split("\t")[1]
            and metadata["libpq_version"] == runtime_version and value["mapped_library"] == package["path"],
            "Python libpq binding mismatch")
    require(re.fullmatch("[0-9a-f]{64}", value["metadata_sha256"]), "missing runtime metadata digest")
    packages = value["packages"]
    minor = value["version"].split(".")[1]
    required = ("python3", "python3-minimal", "python3." + minor, "python3." + minor + "-minimal",
                "libpython3." + minor + "-stdlib", "libpython3." + minor + "-minimal", "libffi8", "libc6",
                "libpq5", "libssl3t64", "postgresql-client-16", "postgresql-client-18")
    require(type(packages) is dict and all(name in packages for name in required), "missing Python package inventory")
    for name, item in packages.items():
        require(type(item) is dict and set(item) == {"version", "architecture", "source", "source_version",
                "copyright_path", "copyright_sha256"} and item["version"] and item["source"] and item["source_version"]
                and item["architecture"] in (architecture, "all") and item["copyright_path"].startswith("/usr/share/doc/")
                and re.fullmatch("[0-9a-f]{64}", item["copyright_sha256"]), "invalid package/license inventory")
    require(packages["libpq5"]["version"] == metadata["package_version"], "Python package version mismatch")
    require(value["python_license"]["path"].startswith("/usr/")
            and re.fullmatch("[0-9a-f]{64}", value["python_license"]["sha256"]), "missing Python license")
    notices = value["dependency_notices"]
    require(type(notices) is dict and notices and "unicode-ident-1.0.27/LICENSE-UNICODE" in notices
            and all(re.fullmatch("[0-9a-f]{64}", digest) for digest in notices.values())
            and re.fullmatch("[0-9a-f]{64}", value["notice_sha256"]), "missing notice hashes")
    expected_notice = Path(__file__).resolve().parents[2] / "NOTICE"
    require(value["notice_sha256"] == hashlib.sha256(expected_notice.read_bytes()).hexdigest(), "NOTICE source mismatch")
    return value


def probe(image, runtime):
    require(image and not image.startswith("-"), "invalid image reference")
    try:
        metadata = decode_json(run([runtime, "image", "inspect", image]))
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
        require(not any(item.split("=", 1)[0].startswith("LD_") for item in config.get("Env", [])),
                "image contains loader environment overrides")
        base = [runtime, "run", "--rm", "--read-only", "--read-only-tmpfs=false", "--network=none", "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--mount", "type=tmpfs,destination=/work,tmpfs-mode=0700,U=true",
                "--mount", "type=tmpfs,destination=/tmp,tmpfs-mode=0700,U=true", image_id, "/bin/sh", "-c"]
        output = run([*base, PROBE])
        require("rpgmigrate-private-input-canary" not in output, "private input appeared in probe output")
        observed = records(output)
        expected_observations = {"uid", "gid", "machine", "version", "plain_version", "ldd", "coreutils", "sed",
                                 "default_psql", "mountinfo", "work_owner", "tmp_owner", "inputs", "inputs_manifest",
                                 "notice", "work_write", "tmp_write", "root_write", "python"}
        expected_observations.update(f"{tool}_{major}" for major in (16, 18) for tool in ("pg_dump", "pg_restore", "psql"))
        require(set(observed) == expected_observations, "unexpected or missing smoke observation")
        require(observed["uid"] == "65532" and observed["gid"] == "65532", "wrong effective uid/gid")
        require(observed["machine"] == {"amd64": "x86_64", "arm64": "aarch64"}[architecture], "architecture mismatch")
        version = decode_json(observed["version"])
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
        expected_manifest = source_manifest()
        require(observed["inputs_manifest"] + "\n" == expected_manifest
                and hashlib.sha256(expected_manifest.encode()).hexdigest() == inputs[1], "image source inputs mismatch")
        package = records(run([*base, PACKAGE, "rpgmigrate-smoke", links[0]]))
        require(set(package) == {"path", "owner", "package"}, "unexpected package observations")
        require(re.fullmatch(r"/usr/lib/[^/]+/libpq\.so\.5\.\d+", package["path"]), "invalid resolved libpq path")
        require(package["owner"] == f"libpq5:{architecture}: {package['path']}", "libpq is not owned by libpq5")
        fields = package["package"].split("\t")
        require(len(fields) == 4 and fields[0] == "libpq5" and fields[2] == architecture
                and fields[3] == "install ok installed" and re.fullmatch(r"18\.\d+[^\s]*\.pgdg13\+[^\s]+", fields[1]), "wrong stock PGDG libpq package")
        minor = int(re.match(r"18\.(\d+)", fields[1])[1])
        require(int(plain[1]) == 180000 + minor, "runtime libpq differs from installed package")
        python = validate_python(decode_json(observed["python"]), architecture, package, int(plain[1]))
        return {"python": python, "source_inputs": expected_manifest.splitlines(), "image_id": image_id, "architecture": architecture, "effective_uid": 65532,
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
