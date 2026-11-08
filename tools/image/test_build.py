#!/usr/bin/env python3
"""Check the image context boundary without running a container engine."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess


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
        git = patch.object(self.build.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', ''))
        git.start()
        self.addCleanup(git.stop)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / 'source'
        self.root.mkdir()
        for name in ('Cargo.toml', 'Cargo.lock', 'build.rs', 'NOTICE', 'Dockerfile',
                     '.dockerignore', 'tools/build-libpq.py', 'tools/image/runtime.py',
                     'src/main.rs', 'native/libpq/source.json', 'native/libpq/COPYRIGHT',
                     'native/libpq/NOTICE', 'native/libpq/source16.json', 'native/libpq/0016-silent-passfile-rejection.patch', 'tools/image/build.py', 'tools/image/smoke.py',
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


class ClientInputTests(unittest.TestCase):
    def test_client_version_build_arguments_are_closed_and_explicit(self):
        build = module('build')
        self.assertTrue(hasattr(build, 'client_build_args'), 'client build inputs are required')
        for version in ('16.15', '18.6'):
            self.assertEqual(build.client_build_args(version), ['--build-arg', 'PG_CLIENT_VERSION=' + version])
        for version in ('16', '16.14', '18.7', '', None):
            with self.assertRaises(RuntimeError):
                build.client_build_args(version)


class ClientCommandTests(unittest.TestCase):
    def test_native_client_commands_require_matching_versions_and_actual_bindir(self):
        spec = importlib.util.spec_from_file_location('client_builder', Path(__file__).resolve().parents[1] / 'build-libpq.py')
        builder = importlib.util.module_from_spec(spec); spec.loader.exec_module(builder)
        prefix = Path('/opt/rpgmigrate/target/native/pgtools')
        for version in ('16.15', '18.6'):
            valid = ['pg_dump (PostgreSQL) ' + version, 'pg_restore (PostgreSQL) ' + version,
                     'PostgreSQL ' + version, str(prefix / 'bin')]
            with patch.object(builder, 'output', side_effect=valid):
                self.assertEqual(builder.client_commands(prefix, version)['bindir'], str(prefix / 'bin'))
            for index in range(4):
                wrong = valid.copy(); wrong[index] = 'wrong'
                with self.subTest(version=version, index=index), patch.object(builder, 'output', side_effect=wrong):
                    with self.assertRaisesRegex(RuntimeError, 'version|bindir'):
                        builder.client_commands(prefix, version)


if __name__ == '__main__':
    unittest.main()
