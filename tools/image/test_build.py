#!/usr/bin/env python3
"""Check the image context boundary without running a container engine."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


def module(name):
    path = Path(__file__).with_name(name + '.py')
    if not path.exists():
        return None
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class SourceTests(unittest.TestCase):
    def setUp(self):
        self.build = module('build')
        self.assertIsNotNone(self.build, 'image build helper must exist')
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'source'
        self.root.mkdir()
        for name in ('Cargo.toml', 'Cargo.lock', 'build.rs', 'NOTICE', 'Dockerfile',
                     '.dockerignore', 'tools/build-libpq.py', 'tools/image/runtime.py',
                     'src/main.rs', 'native/libpq/source.json', 'native/libpq/COPYRIGHT',
                     'native/libpq/NOTICE', 'tools/image/build.py', 'tools/image/smoke.py',
                     'tools/test-db.py', 'tests/support/db_harness.py', 'tests/support/e1_cases.py',
                     'tests/support/protocol_fault.py', 'tests/fixtures/e1/manifest.json',
                     'tests/fixtures/e1/schema.sql', 'tests/fixtures/e1/seed.sql'):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('{}' if name.endswith('source.json') else 'fixture')

    def test_context_excludes_secrets_caches_and_unrelated_work(self):
        for name in ('.env', '.git/config', 'target/native/libpq/lib/libpq.so',
                     'src/password.pem', 'native/libpq/.pgpass', 'tools/image/secret.py',
                     'bench/storage.rs', 'tests/test.rs'):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('DO NOT INCLUDE')
        destination = Path(self.temporary.name) / 'context'
        receipt = self.build.snapshot_source(self.root, destination)
        self.assertEqual((destination / 'src/main.rs').read_text(), 'fixture')
        self.assertNotIn('DO NOT INCLUDE', ''.join(p.read_text() for p in destination.rglob('*') if p.is_file()))
        self.assertIn('src/main.rs', receipt['files'])
        self.assertNotIn('source.json', receipt['files'])
        self.assertEqual(json.loads((destination / 'source.json').read_text()), receipt)

    def test_selected_source_symlink_is_rejected(self):
        path = self.root / 'src/main.rs'
        path.unlink()
        path.symlink_to('/etc/passwd')
        with self.assertRaisesRegex(RuntimeError, 'symlink'):
            self.build.snapshot_source(self.root, Path(self.temporary.name) / 'context')

    def test_missing_source_is_not_empty_success(self):
        (self.root / 'Cargo.lock').unlink()
        with self.assertRaises(RuntimeError):
            self.build.snapshot_source(self.root, Path(self.temporary.name) / 'context')

    def test_explicit_revision_label_is_retained(self):
        receipt = self.build.snapshot_source(self.root, Path(self.temporary.name) / 'context', revision='a' * 40)
        self.assertEqual(receipt['revision'], 'a' * 40)

    def test_malformed_revision_label_is_rejected(self):
        with self.assertRaises(RuntimeError):
            self.build.snapshot_source(self.root, Path(self.temporary.name) / 'context', revision='not-a-commit')

    def test_source_hash_changes_with_included_bytes(self):
        first = self.build.snapshot_source(self.root, Path(self.temporary.name) / 'one')
        (self.root / 'src/main.rs').write_text('changed')
        second = self.build.snapshot_source(self.root, Path(self.temporary.name) / 'two')
        self.assertNotEqual(first['sha256'], second['sha256'])


class RecipeTests(unittest.TestCase):
    def test_runtime_seeds_openssl_default_ca_before_https_apt(self):
        lines = (Path(__file__).resolve().parents[2] / 'Dockerfile').read_text().splitlines()
        final_stage = max(index for index, line in enumerate(lines) if line.startswith('FROM '))
        runtime = lines[final_stage + 1:]
        apt_update = next(index for index, line in enumerate(runtime) if 'apt-get update' in line)
        copies = [line.split() for line in runtime[:apt_update] if line.startswith('COPY ')]
        # The slim base lacks the openssl package's cert.pem symlink.
        self.assertIn(['COPY', '--from=builder', '/etc/ssl/certs/ca-certificates.crt',
                       '/usr/lib/ssl/cert.pem'], copies,
                      'Seed the pinned Debian default CA file before the first HTTPS APT request')


if __name__ == '__main__':
    unittest.main()
