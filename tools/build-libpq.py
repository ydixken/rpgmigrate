#!/usr/bin/env python3
"""Build the reviewed libpq dependency inside this checkout, without installation privileges."""

import argparse
import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
METADATA = ROOT / "native/libpq"
NATIVE = ROOT / "target/native"
PREFIX = NATIVE / "libpq"
RECEIPT = "artifact.json"


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def verify_sha256(path, expected):
    if not path.is_file() or sha256(path) != expected:
        raise RuntimeError(f"SHA-256 mismatch or missing file: {path.name}")


def extract_archive(archive, expected, destination):
    verify_sha256(archive, expected)
    with tarfile.open(archive, "r:bz2") as source:
        source.extractall(destination, filter="data")


def apply_patches(source, metadata, patches_dir=METADATA):
    for patch in metadata["patches"]:
        patch_file = patches_dir / patch["file"]
        verify_sha256(patch_file, patch["sha256"])
        for name, digest in patch["before"].items():
            verify_sha256(source / name, digest)
        command = ["patch", "-p1", "-F", "0", "--batch", "--forward", "-i", str(patch_file)]
        for arguments in (command + ["--dry-run"], command):
            result = subprocess.run(arguments, cwd=source, capture_output=True, text=True)
            if result.returncode:
                raise RuntimeError(f"reviewed patch did not apply exactly: {patch_file.name}")
        for name, digest in patch["after"].items():
            verify_sha256(source / name, digest)


def library_path(prefix):
    return prefix / "lib" / ("libpq.5.dylib" if sys.platform == "darwin" else "libpq.so.5")


def verify_library(path):
    try:
        library = ctypes.CDLL(str(path.resolve(strict=True)))
        marker = library.PQrpgmigrateCapabilities
        marker.argtypes = []
        marker.restype = ctypes.c_uint
        version = library.PQlibVersion
        version.argtypes = []
        version.restype = ctypes.c_int
        if marker() != 1 or version() != 180006:
            raise RuntimeError("libpq capability or version mismatch")
    except (OSError, AttributeError) as error:
        raise RuntimeError("libpq capability marker is absent or library cannot load") from error


def check_library_directory(prefix, selected):
    if selected and Path(selected).resolve() != (prefix / "lib").resolve():
        raise RuntimeError("PQ_LIB_DIR must select target/native/libpq/lib; no stock fallback")


def identity(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def artifact_files(prefix):
    files = {}
    for path in sorted(prefix.rglob("*")):
        if not path.resolve().is_relative_to(prefix.resolve()):
            raise RuntimeError("native artifact contains an external symlink")
        if path == prefix / RECEIPT:
            continue
        if path.is_file():
            files[str(path.relative_to(prefix))] = sha256(path)
    return files


def verify_install(prefix, recipe):
    receipt_path = prefix / RECEIPT
    if not receipt_path.is_file():
        raise RuntimeError("native build receipt missing; run python3 tools/build-libpq.py")
    receipt = json.loads(receipt_path.read_text())
    if receipt.get("recipe_sha256") != recipe:
        raise RuntimeError("native build recipe changed; run python3 tools/build-libpq.py --rebuild")
    files = artifact_files(prefix)
    if not files or files != receipt.get("files") or identity(files) != receipt.get("artifact_sha256"):
        raise RuntimeError("native artifact SHA-256 mismatch; rebuild the controlled dependency")
    verify_library(library_path(prefix))
    return receipt


def local_directory(path):
    for part in (path, *path.parents):
        if part == ROOT:
            return
        if part.is_symlink():
            raise RuntimeError("native output/cache paths must not contain symlinks")
    raise RuntimeError("native output must remain inside this checkout")


def download(metadata, offline):
    cache = NATIVE / "downloads"
    local_directory(cache)
    cache.mkdir(exist_ok=True)
    archive = cache / metadata["archive"]
    local_directory(archive)
    if not archive.exists():
        if offline:
            raise RuntimeError("pinned source is absent from download cache in offline mode")
        with tempfile.NamedTemporaryFile(dir=cache, delete=False) as temporary:
            pending = Path(temporary.name)
            try:
                with urllib.request.urlopen(metadata["url"], timeout=60) as response:
                    if not response.geturl().startswith("https://ftp.postgresql.org/"):
                        raise RuntimeError("source download redirected outside the official HTTPS host")
                    shutil.copyfileobj(response, temporary)
                temporary.flush()
                verify_sha256(pending, metadata["sha256"])
                pending.replace(archive)
            finally:
                pending.unlink(missing_ok=True)
    verify_sha256(archive, metadata["sha256"])
    return archive


def output(command, env):
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"required native tool/dependency failed: {command[0]}")
    return result.stdout.strip()


def build_environment():
    env = os.environ.copy()
    for name in ("MAKEFLAGS", "MAKELEVEL", "MFLAGS", "DESTDIR"):
        env.pop(name, None)
    env.update(LC_ALL="C", CONFIG_SITE="/dev/null", ZERO_AR_DATE="1")
    env.setdefault("CC", "clang")
    env.setdefault("CFLAGS", "-O2")
    for tool in ("make", "patch", "pkg-config", "perl", "bison", "flex", shlex.split(env["CC"])[0]):
        if not shutil.which(tool):
            raise RuntimeError(f"required build tool missing: {tool}; no automatic installs")
    if sys.platform == "darwin" and shutil.which("brew"):
        result = subprocess.run(["brew", "--prefix", "krb5"], capture_output=True, text=True)
        if result.returncode == 0:
            pkgconfig = Path(result.stdout.strip()) / "lib/pkgconfig"
            env["PKG_CONFIG_PATH"] = os.pathsep.join(filter(None, [env.get("PKG_CONFIG_PATH"), str(pkgconfig)]))
    packages = ["openssl", "krb5-gssapi", "libcurl"]
    versions = {name: output(["pkg-config", "--modversion", name], env) for name in packages}
    env["CPPFLAGS"] = " ".join(filter(None, [env.get("CPPFLAGS"), output(["pkg-config", "--cflags", *packages], env)]))
    env["LDFLAGS"] = " ".join(filter(None, [env.get("LDFLAGS"), output(["pkg-config", "--libs-only-L", *packages], env)]))
    toolchain = {
        "compiler": output(shlex.split(env["CC"]) + ["--version"], env).splitlines()[0],
        "make": output(["make", "--version"], env).splitlines()[0],
        "dependencies": versions,
        "pkg_config_path": env.get("PKG_CONFIG_PATH", ""),
        "flags": {key: env.get(key, "") for key in ("CC", "CFLAGS", "CPPFLAGS", "LDFLAGS")},
        "platform": platform.platform(),
        "architecture": platform.machine(),
    }
    return env, toolchain


def run_build(command, source, env, log):
    log.write((shlex.join(command) + "\n").encode())
    log.flush()
    result = subprocess.run(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f"native build failed; inspect {NATIVE / 'build.log'}")


def build(metadata, recipe, offline, jobs):
    archive = download(metadata, offline)
    env, toolchain = build_environment()
    with tempfile.TemporaryDirectory(prefix="libpq-build-", dir=NATIVE) as temporary:
        work = Path(temporary)
        extract_archive(archive, metadata["sha256"], work)
        source = work / metadata["source_directory"]
        apply_patches(source, metadata)
        verify_sha256(source / "COPYRIGHT", metadata["copyright_sha256"])
        configure = ["./configure", f"--prefix={PREFIX}", "--with-openssl", "--with-gssapi", "--with-libcurl", "--without-icu", "--without-readline", "--without-zlib", "--without-lz4", "--without-zstd"]
        with (NATIVE / "build.log").open("wb") as log:
            run_build(configure, source, env, log)
            config = (source / "src/include/pg_config.h").read_text()
            for feature in ("USE_OPENSSL", "ENABLE_GSS", "USE_LIBCURL"):
                if f"#define {feature} 1" not in config:
                    raise RuntimeError(f"configure did not enable required feature: {feature}")
            for directory in ("src/interfaces/libpq", "src/interfaces/libpq-oauth"):
                run_build(["make", "-C", directory, f"-j{jobs}"], source, env, log)
            PREFIX.mkdir(exist_ok=True)
            (PREFIX / RECEIPT).unlink(missing_ok=True)
            for directory in ("src/interfaces/libpq", "src/interfaces/libpq-oauth"):
                run_build(["make", "-C", directory, "install"], source, env, log)
        for header in ("postgres_ext.h", "pg_config.h", "pg_config_os.h", "pg_config_manual.h"):
            shutil.copyfile(source / "src/include" / header, PREFIX / "include" / header)
        (PREFIX / "include/libpq").mkdir(exist_ok=True)
        shutil.copyfile(source / "src/include/libpq/libpq-fs.h", PREFIX / "include/libpq/libpq-fs.h")
        notices = PREFIX / "share/licenses/libpq"
        notices.mkdir(parents=True, exist_ok=True)
        for name in ("COPYRIGHT", "NOTICE"):
            shutil.copyfile(METADATA / name, notices / name)
        verify_library(library_path(PREFIX))
        files = artifact_files(PREFIX)
        receipt = {
            "recipe_sha256": recipe,
            "artifact_sha256": identity(files),
            "source": metadata,
            "toolchain": toolchain,
            "configure": configure,
            "features": {"openssl": True, "gssapi": True, "oauth_libcurl": True, "service": True, "passfile": True},
            "capabilities": 1,
            "libpq_version": 180006,
            "files": files,
        }
        (PREFIX / RECEIPT).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    return verify_install(PREFIX, recipe)


def client_metadata(version):
    if version not in ('16.15', '18.6'):
        raise RuntimeError('unsupported PostgreSQL client version')
    name = 'source16.json' if version == '16.15' else 'source.json'
    return json.loads((METADATA / name).read_text())


def client_commands(prefix, version):
    if version not in ('16.15', '18.6'):
        raise RuntimeError('unsupported PostgreSQL client version')
    env = {'PATH': '/usr/bin:/bin', 'LC_ALL': 'C'}
    commands = {name: output([str(prefix / 'bin' / name), '--version'], env)
                for name in ('pg_dump', 'pg_restore', 'pg_config')}
    expected = {name: ('PostgreSQL ' if name == 'pg_config' else name + ' (PostgreSQL) ') + version for name in commands}
    if commands != expected:
        raise RuntimeError('client utility version mismatch')
    commands['bindir'] = output([str(prefix / 'bin/pg_config'), '--bindir'], env)
    if commands['bindir'] != str(prefix / 'bin'):
        raise RuntimeError('client pg_config bindir mismatch')
    return commands


def verify_clients(prefix, recipe, version):
    receipt = json.loads((prefix / RECEIPT).read_text())
    files = artifact_files(prefix)
    if (receipt.get('recipe_sha256') != recipe or receipt.get('version') != version
            or receipt.get('source') != client_metadata(version)
            or not files or receipt.get('files') != files
            or receipt.get('artifact_sha256') != identity(files)):
        raise RuntimeError('client artifact identity or version mismatch')
    if receipt.get('commands') != client_commands(prefix, version):
        raise RuntimeError('client command identity mismatch')
    library = ctypes.CDLL(str(library_path(prefix).resolve(strict=True)))
    if library.PQlibVersion() != {'16.15': 160015, '18.6': 180006}[version]:
        raise RuntimeError('client library version mismatch')
    return receipt


def build_clients(version, offline, jobs, rebuild=False):
    prefix = NATIVE / 'pgtools'
    local_directory(prefix)
    local_directory(NATIVE / 'clients-build.log')
    metadata = client_metadata(version)
    recipe = identity({'builder': sha256(Path(__file__)), 'source': metadata})
    if prefix.exists():
        artifact_files(prefix)
    if (prefix / RECEIPT).exists() and not rebuild:
        return verify_clients(prefix, recipe, version)
    archive = download(metadata, offline)
    env, toolchain = build_environment()
    with tempfile.TemporaryDirectory(prefix='pgclients-build-', dir=NATIVE) as temporary:
        work = Path(temporary)
        extract_archive(archive, metadata['sha256'], work)
        source = work / metadata['source_directory']
        apply_patches(source, metadata)
        verify_sha256(source / 'COPYRIGHT', metadata['copyright_sha256'])
        configure = ['./configure', f'--prefix={prefix}', '--with-openssl', '--with-gssapi',
                     '--without-icu', '--without-readline', '--without-zlib', '--without-lz4', '--without-zstd']
        if version == '18.6':
            configure.append('--with-libcurl')
        libraries = ['src/interfaces/libpq'] + (['src/interfaces/libpq-oauth'] if version == '18.6' else [])
        with (NATIVE / 'clients-build.log').open('wb') as log:
            run_build(configure, source, env, log)
            for directory in libraries:
                run_build(['make', '-C', directory, f'-j{jobs}'], source, env, log)
            run_build(['make', '-C', 'src/bin/pg_dump', f'-j{jobs}', 'pg_dump', 'pg_restore'], source, env, log)
            run_build(['make', '-C', 'src/bin/pg_config', f'-j{jobs}'], source, env, log)
            if prefix.exists():
                shutil.rmtree(prefix)
            prefix.mkdir()
            for directory in libraries:
                run_build(['make', '-C', directory, 'install'], source, env, log)
        (prefix / 'bin').mkdir(exist_ok=True)
        for name in ('pg_dump', 'pg_restore', 'pg_config'):
            directory = 'pg_config' if name == 'pg_config' else 'pg_dump'
            shutil.copyfile(source / 'src/bin' / directory / name, prefix / 'bin' / name)
            (prefix / 'bin' / name).chmod(0o755)
        notices = prefix / 'share/licenses/libpq'
        notices.mkdir(parents=True)
        shutil.copyfile(source / 'COPYRIGHT', notices / 'COPYRIGHT')
        (notices / 'NOTICE').write_text('PostgreSQL ' + version + ' clients and libpq; reviewed silent passfile rejection patch.\n')
        files = artifact_files(prefix)
        receipt = {'version': version, 'source': metadata, 'commands': client_commands(prefix, version), 'toolchain': toolchain, 'configure': configure,
                   'recipe_sha256': recipe, 'files': files, 'artifact_sha256': identity(files)}
        (prefix / RECEIPT).write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    return verify_clients(prefix, recipe, version)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-version", choices=("16.15", "18.6"), help="also build one client trio in target/native/pgtools")
    parser.add_argument("--verify", action="store_true", help="verify the installed artifact without network or build")
    parser.add_argument("--offline", action="store_true", help="require the verified archive in target/native/downloads")
    parser.add_argument("--rebuild", action="store_true", help="rebuild the controlled prefix from pinned source")
    parser.add_argument("--jobs", type=int, default=min(os.cpu_count() or 1, 8))
    args = parser.parse_args()
    if sys.version_info < (3, 12) or sys.platform not in ("darwin", "linux"):
        parser.error("requires Python 3.12+ on macOS or Linux; no system-libpq fallback")
    if args.verify and args.client_version:
        parser.error("--client-version cannot be combined with --verify")
    if args.jobs < 1 or (args.verify and args.rebuild):
        parser.error("jobs must be positive; --verify and --rebuild cannot be combined")
    try:
        local_directory(PREFIX)
        local_directory(NATIVE / "build.log")
        local_directory(NATIVE / ".libpq-build.lock")
        check_library_directory(PREFIX, os.environ.get("PQ_LIB_DIR"))
        if PREFIX.exists():
            artifact_files(PREFIX)
        NATIVE.mkdir(parents=True, exist_ok=True)
        with (NATIVE / ".libpq-build.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError("another controlled libpq build or verification is running") from error
            metadata = json.loads((METADATA / "source.json").read_text())
            inputs = [Path(__file__).resolve(), METADATA / "source.json", METADATA / "COPYRIGHT", METADATA / "NOTICE"]
            inputs.extend(METADATA / patch["file"] for patch in metadata["patches"])
            recipe = identity({str(path.relative_to(ROOT)): sha256(path) for path in inputs})
            if args.verify or ((PREFIX / RECEIPT).exists() and not args.rebuild):
                receipt = verify_install(PREFIX, recipe)
            else:
                receipt = build(metadata, recipe, args.offline, args.jobs)
            if args.client_version:
                build_clients(args.client_version, args.offline, args.jobs, args.rebuild)
        print(json.dumps({"prefix": str(PREFIX), "library_sha256": sha256(library_path(PREFIX)), "artifact_sha256": receipt["artifact_sha256"], "recipe_sha256": recipe, "capabilities": 1}))
        return 0
    except (OSError, RuntimeError, ValueError, tarfile.TarError) as error:
        print(f"native libpq: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
