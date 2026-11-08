#!/usr/bin/env python3
"""Build one native runtime image and export its exact OCI archive and receipt."""

import argparse
import hashlib
import json
import re
from pathlib import Path
import subprocess
import sys
import tempfile

from runtime import canonical_sha256, check_platform, execute, inspect_archive, require, sha256

ROOT = Path(__file__).resolve().parents[2]
REQUIRED = ('Cargo.toml', 'Cargo.lock', 'build.rs', 'NOTICE', 'Dockerfile', '.dockerignore',
            'tools/build-libpq.py', 'tools/image/build.py', 'tools/image/runtime.py',
            'tools/image/smoke.py', 'tools/test-db.py',
            'tests/support/db_harness.py', 'tests/support/e1_cases.py', 'tests/support/protocol_fault.py',
            'tests/fixtures/e1/manifest.json', 'tests/fixtures/e1/schema.sql', 'tests/fixtures/e1/seed.sql',
            'native/libpq/source.json', 'native/libpq/source16.json', 'native/libpq/0016-silent-passfile-rejection.patch', 'native/libpq/COPYRIGHT', 'native/libpq/NOTICE')


def source_files(root):
    paths = [Path(name) for name in REQUIRED]
    require((root / 'src').is_dir() and not (root / 'src').is_symlink(), 'source directory is absent or a symlink')
    rust = sorted((root / 'src').rglob('*.rs'))
    require(rust, 'Rust source inventory is empty')
    paths += [path.relative_to(root) for path in rust]
    metadata = json.loads((root / 'native/libpq/source.json').read_text())
    for patch in metadata.get('patches', []):
        name = patch['file']
        require(Path(name).name == name and name.endswith('.patch'), 'invalid native patch path')
        paths.append(Path('native/libpq') / name)
    for path in paths:
        absolute = root / path
        require(not any(part.is_symlink() for part in (absolute, *absolute.parents)), 'source symlinks are not supported')
        require(absolute.is_file(), 'required image source input is missing')
    return sorted(set(paths))


def snapshot_source(root, destination, *, revision=None):
    require(revision is None or re.fullmatch(r"[0-9a-f]{40,64}", revision), "invalid revision label")
    root, destination = Path(root).resolve(strict=True), Path(destination)
    paths = source_files(root)
    destination.mkdir()
    files = {}
    for path in paths:
        source, target = root / path, destination / path
        data = source.read_bytes()
        require(len(data) <= 16 * 1024**2, 'source file exceeds its size limit')
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        target.chmod(0o644)
        files[path.as_posix()] = hashlib.sha256(data).hexdigest()
    require(all(sha256(root / path) == files[path.as_posix()] for path in paths), 'source changed during image snapshot')
    result = subprocess.run(['git', '-C', str(root), 'rev-parse', '--verify', 'HEAD'], capture_output=True, text=True, timeout=10)
    source = {'revision': revision if revision is not None else (result.stdout.strip() if result.returncode == 0 else None),
              'sha256': canonical_sha256(files), 'files': files}
    (destination / 'source.json').write_text(json.dumps(source, indent=2, sort_keys=True) + '\n')
    return source


def client_build_args(version):
    require(version in ('16.15', '18.6'), 'unsupported PostgreSQL client version')
    return ['--build-arg', 'PG_CLIENT_VERSION=' + version]


def build(engine, platform, output, *, revision=None, client_version='18.6'):
    arguments = client_build_args(client_version)
    check_platform(engine, platform)
    output = Path(output).absolute()
    require(not any(path.is_symlink() for path in (output, *output.parents)), 'image output paths must not contain symlinks')
    require(not output.exists(), 'image output directory must be new')
    output.mkdir(parents=True, mode=0o700)
    archive = output / 'image.oci.tar'
    with tempfile.TemporaryDirectory(prefix='rpgmigrate-image-') as temporary:
        context = Path(temporary) / 'context'
        source = snapshot_source(ROOT, context, revision=revision)
        print('Runtime image: build native ' + platform, flush=True)
        if engine == 'podman':
            identity = Path(temporary) / 'image-id'
            execute([engine, 'build', '--format', 'oci', '--platform', platform, '--http-proxy=false',
                     '--force-rm', '--iidfile', str(identity), *arguments, str(context)], timeout=1800, log=output / 'build.log')
            image_id = identity.read_text().strip()
            require(len(image_id) in (64, 71) and all(c in '0123456789abcdef' for c in image_id.removeprefix('sha256:')), 'invalid built image ID')
            execute([engine, 'save', '--format', 'oci-archive', '--output', str(archive), image_id], timeout=300)
        else:
            execute([engine, 'buildx', 'build', '--platform', platform, '--provenance=false',
                     '--output', 'type=oci,compression=gzip,dest=' + str(archive), *arguments, str(context)], timeout=1800, log=output / 'build.log')
        actual = inspect_archive(archive)
        require(actual['runtime']['clients']['version'] == client_version, 'built client variant differs')
        require(actual['source'] == source and actual['platform'] == platform, 'built image differs from requested source or platform')
        receipt = {'schema': 1, 'status': 'built', 'archive': {'path': archive.name, 'sha256': sha256(archive)}, **actual}
        (output / 'receipt.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'receipt': str(output / 'receipt.json'), 'platform': platform, 'manifest_digest': actual['image']['manifest_digest']}))
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--engine', required=True, choices=('podman', 'docker'))
    parser.add_argument('--platform', required=True, choices=('linux/arm64', 'linux/amd64'))
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--client-version', choices=('16.15', '18.6'), default='18.6')
    parser.add_argument('--revision', help='revision label for an export; does not verify commit membership')
    args = parser.parse_args()
    try:
        require(sys.version_info >= (3, 12), 'image tooling requires Python 3.12 or newer')
        build(args.engine, args.platform, args.output, revision=args.revision, client_version=args.client_version)
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        print('Runtime image: ' + (str(error) if isinstance(error, RuntimeError) else 'invalid or unavailable build input'), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
