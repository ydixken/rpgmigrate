#!/usr/bin/env python3
"""Check runtime manifests and OCI evidence against small real archives."""

import hashlib
import importlib.util
import io
import json
import stat
import struct
from pathlib import Path
import tarfile
import tempfile
import unittest


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def archive_bytes(files):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode='w') as archive:
        for name, value in (files.items() if isinstance(files, dict) else files):
            info = tarfile.TarInfo(name)
            info.mode = 0o755 if name.endswith('rpgmigrate') else 0o644
            if isinstance(value, dict):
                info.mode = value.get('mode', 0o755)
                info.uid, info.gid = value.get('uid', 0), value.get('gid', 0)
                if value.get('kind') == 'directory':
                    info.type = tarfile.DIRTYPE
                    archive.addfile(info)
                    continue
                value = value['data']
            if isinstance(value, tuple):
                info.type = tarfile.SYMTYPE
                info.linkname = value[0]
                archive.addfile(info)
            else:
                info.size = len(value)
                archive.addfile(info, io.BytesIO(value))
    return output.getvalue()


def elf_fixture(machine=183, object_type=3):
    header = bytearray(64)
    header[:16] = b'\x7fELF\x02\x01\x01' + b'\0' * 9
    struct.pack_into('<HHI', header, 16, object_type, machine, 1)
    return bytes(header)


def write_fixture(directory, *, mutate=None, extra_layer=None, architecture='arm64'):
    """Provide independent fixture bytes for smoke and publisher validation tests."""
    prefix = '/opt/rpgmigrate/target/native/libpq'
    binary = elf_fixture(183 if architecture == 'arm64' else 62)
    files = {
        '/usr/local/bin/rpgmigrate': binary,
        prefix + '/lib/libpq.so.5.18': binary + b'libpq',
        prefix + '/lib/libpq.so.5': ('libpq.so.5.18',),
        prefix + '/lib/libpq-oauth-18.so': binary + b'oauth',
        prefix + '/share/licenses/libpq/COPYRIGHT': b'copyright',
        prefix + '/share/licenses/libpq/NOTICE': b'notice',
    }
    native_files = {name.removeprefix(prefix + '/'): digest(value if isinstance(value, bytes) else binary + b'libpq')
                    for name, value in files.items() if name.startswith(prefix + '/')}
    native = {'capabilities': 1, 'libpq_version': 180006, 'recipe_sha256': 'a' * 64,
              'artifact_sha256': hashlib.sha256(json.dumps(native_files, sort_keys=True).encode()).hexdigest(),
              'files': native_files,
              'features': {'openssl': True, 'gssapi': True, 'oauth_libcurl': True, 'service': True, 'passfile': True}}
    runtime = {'schema': 1, 'prefix': prefix, 'native_receipt_sha256': digest(encode(native)),
               'files': {name: {'sha256': digest(value)} if isinstance(value, bytes) else {'link': value[0]}
                         for name, value in files.items()}}
    source_files = {'Cargo.toml': 'b' * 64, 'src/main.rs': 'c' * 64}
    source = {'revision': 'd' * 40, 'files': source_files, 'sha256': digest(encode(source_files))}
    for name, value in {'native-artifact.json': native, 'runtime.json': runtime, 'source.json': source}.items():
        files['/usr/share/rpgmigrate/' + name] = encode(value)
    for name in ('packages.txt', 'builder-packages.txt'):
        files['/usr/share/rpgmigrate/' + name] = b'libc6:arm64=2.41-12\n'
    config = {'os': 'linux', 'architecture': architecture, 'config': {
        'User': '1000:1000', 'WorkingDir': '/work', 'Env': ['HOME=/home/rpgmigrate', 'TMPDIR=/work'],
        'Entrypoint': ['/usr/local/bin/rpgmigrate'], 'Cmd': ['--help'], 'StopSignal': 'SIGTERM'}}
    if mutate:
        mutate(files, config)
    layer = archive_bytes({name.lstrip('/'): value for name, value in files.items()})
    layers = [layer] + ([archive_bytes(extra_layer)] if extra_layer is not None else [])
    config['rootfs'] = {'type': 'layers', 'diff_ids': ['sha256:' + digest(value) for value in layers]}
    config_bytes = encode(config)
    manifest = {'schemaVersion': 2, 'mediaType': 'application/vnd.oci.image.manifest.v1+json',
                'config': {'mediaType': 'application/vnd.oci.image.config.v1+json', 'digest': 'sha256:' + digest(config_bytes), 'size': len(config_bytes)},
                'layers': [{'mediaType': 'application/vnd.oci.image.layer.v1.tar', 'digest': 'sha256:' + digest(value), 'size': len(value)} for value in layers]}
    manifest_bytes = encode(manifest)
    index = {'schemaVersion': 2, 'manifests': [{'mediaType': manifest['mediaType'], 'digest': 'sha256:' + digest(manifest_bytes), 'size': len(manifest_bytes)}]}
    archive = directory / 'image.oci.tar'
    archive.write_bytes(archive_bytes({'oci-layout': encode({'imageLayoutVersion': '1.0.0'}), 'index.json': encode(index),
        'blobs/sha256/' + digest(manifest_bytes): manifest_bytes, 'blobs/sha256/' + digest(config_bytes): config_bytes,
        **{'blobs/sha256/' + digest(value): value for value in layers}}))
    receipt = {'schema': 1, 'status': 'built', 'platform': 'linux/' + architecture, 'architecture': architecture,
               'archive': {'path': archive.name, 'sha256': digest(archive.read_bytes())},
               'image': {'manifest_digest': 'sha256:' + digest(manifest_bytes), 'config_digest': 'sha256:' + digest(config_bytes)},
               'source': source, 'native': native, 'runtime': runtime, 'binary_sha256': digest(binary),
               'packages': ['libc6:arm64=2.41-12'], 'builder_packages': ['libc6:arm64=2.41-12']}
    path = directory / 'receipt.json'
    path.write_bytes(encode(receipt))
    return path, receipt


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        path = Path(__file__).with_name('runtime.py')
        self.assertTrue(path.exists(), 'runtime validator must exist')
        spec = importlib.util.spec_from_file_location('runtime', path)
        self.runtime = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.runtime)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_privileged_binary_and_native_object_modes_are_rejected(self):
        for name, data in ((self.runtime.BINARY, elf_fixture()),
                           (self.runtime.PREFIX + '/lib/libpq-oauth-18.so', elf_fixture() + b'oauth')):
            for mode in (0o4755, 0o2755):
                with self.subTest(path=name, mode=oct(mode)):
                    path, _ = write_fixture(self.root, extra_layer={name.lstrip('/'): {'data': data, 'mode': mode}})
                    with self.assertRaises(RuntimeError):
                        self.runtime.load_receipt(path)

    def test_protected_directory_roots_require_safe_real_directories(self):
        for name in (self.runtime.PREFIX, self.runtime.EVIDENCE.rstrip('/')):
            for value in (b'file', ('/tmp/external',), {'kind': 'directory', 'uid': 1000},
                          {'kind': 'directory', 'gid': 1000}, {'kind': 'directory', 'mode': 0o777}):
                with self.subTest(path=name, value=value):
                    path, _ = write_fixture(self.root, extra_layer={name.lstrip('/'): value})
                    with self.assertRaises(RuntimeError):
                        self.runtime.load_receipt(path)

    def test_later_directory_cannot_retain_replaced_file_or_evidence(self):
        for name in (self.runtime.BINARY, self.runtime.PREFIX + '/lib/libpq-oauth-18.so',
                     self.runtime.EVIDENCE + 'runtime.json'):
            with self.subTest(path=name):
                path, _ = write_fixture(self.root, extra_layer={name.lstrip('/'): {'kind': 'directory'}})
                with self.assertRaises(RuntimeError):
                    self.runtime.load_receipt(path)

    def test_duplicate_protected_entries_in_one_layer_are_rejected(self):
        for filename in ('runtime.json', 'source.json', 'native-artifact.json', 'packages.txt'):
            with self.subTest(filename=filename):
                _, receipt = write_fixture(self.root)
                key = {'runtime.json': 'runtime', 'source.json': 'source', 'native-artifact.json': 'native'}
                data = encode(receipt[key[filename]]) if filename in key else b'libc6:arm64=2.41-12\n'
                name = self.runtime.EVIDENCE.lstrip('/') + filename
                path, _ = write_fixture(self.root, extra_layer=[(name, data), (name, ('absent',))])
                with self.assertRaises(RuntimeError):
                    self.runtime.load_receipt(path)

    def test_legitimate_parent_directories_can_repeat_across_layers(self):
        parents = {'opt': {'kind': 'directory'}, self.runtime.PREFIX.lstrip('/'): {'kind': 'directory'},
                   self.runtime.EVIDENCE.strip('/'): {'kind': 'directory'}}
        def add_parents(files, _):
            files.update({'/' + name: value for name, value in parents.items()})
        path, receipt = write_fixture(self.root, mutate=add_parents, extra_layer=parents)
        self.assertEqual(self.runtime.load_receipt(path), receipt)

    def test_payload_machine_must_match_declared_architecture(self):
        for architecture in ('arm64', 'amd64'):
            path, expected = write_fixture(self.root, architecture=architecture)
            self.assertEqual(self.runtime.load_receipt(path), expected)
        path, _ = write_fixture(self.root, mutate=lambda _, config: config.update(architecture='amd64'))
        with self.assertRaises(RuntimeError):
            self.runtime.inspect_archive(path.parent / 'image.oci.tar')

    def test_native_elf_machine_is_checked_independently_of_binary(self):
        for name in ('libpq.so.5.18', 'libpq-oauth-18.so'):
            def replace_object(files, _):
                full = self.runtime.PREFIX + '/lib/' + name
                files[full] = elf_fixture(62)
                native = json.loads(files[self.runtime.EVIDENCE + 'native-artifact.json'])
                native['files']['lib/' + name] = digest(files[full])
                if name == 'libpq.so.5.18':
                    native['files']['lib/libpq.so.5'] = digest(files[full])
                native['artifact_sha256'] = digest(json.dumps(native['files'], sort_keys=True).encode())
                files[self.runtime.EVIDENCE + 'native-artifact.json'] = encode(native)
                manifest = json.loads(files[self.runtime.EVIDENCE + 'runtime.json'])
                manifest['files'][full] = {'sha256': digest(files[full])}
                manifest['native_receipt_sha256'] = digest(encode(native))
                files[self.runtime.EVIDENCE + 'runtime.json'] = encode(manifest)
            with self.subTest(name=name):
                path, _ = write_fixture(self.root, mutate=replace_object)
                with self.assertRaises(RuntimeError):
                    self.runtime.inspect_archive(path.parent / 'image.oci.tar')

    def test_invalid_or_truncated_elf_header_is_rejected(self):
        invalid = [b'not ELF', elf_fixture()[:20], elf_fixture(object_type=1)]
        for offset, value in ((0, 0), (4, 1), (5, 2), (6, 0), (20, 0)):
            header = bytearray(elf_fixture())
            header[offset] = value
            invalid.append(bytes(header))
        for number, data in enumerate(invalid):
            def replace_binary(files, _):
                files[self.runtime.BINARY] = data
                name = self.runtime.EVIDENCE + 'runtime.json'
                manifest = json.loads(files[name])
                manifest['files'][self.runtime.BINARY] = {'sha256': digest(data)}
                files[name] = encode(manifest)
            with self.subTest(case=number):
                path, _ = write_fixture(self.root, mutate=replace_binary)
                with self.assertRaises(RuntimeError):
                    self.runtime.inspect_archive(path.parent / 'image.oci.tar')

    def test_inner_schema_boolean_is_rejected(self):
        def boolean_schema(files, _):
            name = self.runtime.EVIDENCE + 'runtime.json'
            manifest = json.loads(files[name])
            manifest['schema'] = True
            files[name] = encode(manifest)
        path, _ = write_fixture(self.root, mutate=boolean_schema)
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_missing_native_receipt_member_is_rejected_during_staging(self):
        self.assertTrue(hasattr(self.runtime, 'native_runtime_files'), 'native staging selector is required')
        prefix = self.root / 'libpq'
        (prefix / 'lib').mkdir(parents=True)
        (prefix / 'lib/libpq.so.5').write_bytes(b'library')
        (prefix / 'lib/libpq-oauth-18.so').write_bytes(b'oauth')
        with self.assertRaises(RuntimeError):
            self.runtime.native_runtime_files(prefix, {'files': {}})

    def test_native_staging_keeps_symlinks_and_omits_development_files(self):
        self.assertTrue(hasattr(self.runtime, 'native_runtime_files'), 'native staging selector is required')
        prefix = self.root / 'libpq'
        contents = {'lib/libpq.so.5.18': b'library', 'lib/libpq-oauth-18.so': b'oauth',
                    'share/licenses/libpq/COPYRIGHT': b'copyright', 'share/licenses/libpq/NOTICE': b'notice',
                    'include/libpq-fe.h': b'header', 'lib/libpq.a': b'archive'}
        for name, data in contents.items():
            path = prefix / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        (prefix / 'lib/libpq.so.5').symlink_to('libpq.so.5.18')
        receipt = {'files': {name: digest(data) for name, data in contents.items()}}
        receipt['files']['lib/libpq.so.5'] = digest(b'library')
        selected = self.runtime.native_runtime_files(prefix, receipt)
        self.assertEqual({str(path.relative_to(prefix)) for path in selected}, {
            'lib/libpq.so.5', 'lib/libpq.so.5.18', 'lib/libpq-oauth-18.so',
            'share/licenses/libpq/COPYRIGHT', 'share/licenses/libpq/NOTICE'})

    def test_replaced_evidence_cannot_use_stale_layer_bytes(self):
        path, _ = write_fixture(self.root, extra_layer={
            'usr/share/rpgmigrate/runtime.json': ('somewhere-else',)})
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_whiteout_removes_required_runtime_file(self):
        path, _ = write_fixture(self.root, extra_layer={
            'opt/rpgmigrate/target/native/libpq/lib/.wh.libpq-oauth-18.so': b''})
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_boolean_schema_is_not_a_version(self):
        path, receipt = write_fixture(self.root)
        receipt['schema'] = True
        path.write_bytes(encode(receipt))
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_recipe_provenance_requires_pinned_bases_and_one_snapshot(self):
        self.assertTrue(hasattr(self.runtime, 'recipe_identity'), 'recipe identity extractor is required')
        recipe = 'FROM rust@sha256:' + 'a' * 64 + ' AS builder\nFROM debian@sha256:' + 'b' * 64 + '\n'
        recipe += 'https://snapshot.debian.org/archive/debian/20261106T000000Z trixie main\n'
        self.assertEqual(self.runtime.recipe_identity(recipe), {
            'base_images': {'builder': 'rust@sha256:' + 'a' * 64, 'runtime': 'debian@sha256:' + 'b' * 64},
            'debian_snapshot': '20261106T000000Z'})
        with self.assertRaises(RuntimeError):
            self.runtime.recipe_identity(recipe.replace('@sha256:' + 'b' * 64, ':latest'))

    def test_rust_standard_library_notice_is_preserved(self):
        self.assertTrue(hasattr(self.runtime, 'copy_rust_licenses'), 'Rust license copying is required')
        source = self.root / 'rust'
        (source / 'licenses').mkdir(parents=True)
        (source / 'COPYRIGHT-library.html').write_text('standard library attribution')
        (source / 'licenses/MIT.txt').write_text('license text')
        destination = self.root / 'notices'
        self.runtime.copy_rust_licenses(source, destination)
        self.assertEqual((destination / 'COPYRIGHT-library.html').read_text(), 'standard library attribution')
        self.assertEqual((destination / 'licenses/MIT.txt').read_text(), 'license text')

    def test_rust_license_copy_normalizes_only_owned_stage_modes(self):
        source = self.root / 'toolchain'
        (source / 'licenses/nested').mkdir(parents=True)
        (source / 'COPYRIGHT-library.html').write_text('copyright')
        (source / 'licenses/nested/MIT.txt').write_text('license text')
        source_paths = [source, *source.rglob('*')]
        for path in source_paths:
            path.chmod(0o777 if path.is_dir() else 0o666)
        before = {path: (path.read_bytes() if path.is_file() else None,
                         stat.S_IMODE(path.stat().st_mode)) for path in source_paths}
        parent = self.root / 'stage'
        parent.mkdir(mode=0o700)
        destination = parent / 'rust'
        self.runtime.copy_rust_licenses(source, destination)
        for path in [destination, *destination.rglob('*')]:
            with self.subTest(path=path.relative_to(destination)):
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o755 if path.is_dir() else 0o644)
        self.assertEqual((destination / 'licenses/nested/MIT.txt').read_text(), 'license text')
        self.assertEqual(stat.S_IMODE(parent.stat().st_mode), 0o700)
        self.assertEqual(before, {path: (path.read_bytes() if path.is_file() else None,
                                        stat.S_IMODE(path.stat().st_mode)) for path in source_paths})
        staged_manifest = {}
        def include_staged_licenses(files, _):
            manifest = json.loads(files[self.runtime.EVIDENCE + 'runtime.json'])
            for path in [destination, *destination.rglob('*')]:
                name = self.runtime.EVIDENCE + 'licenses/rust'
                if path != destination:
                    name += '/' + path.relative_to(destination).as_posix()
                mode = stat.S_IMODE(path.stat().st_mode)
                if path.is_dir():
                    files[name] = {'kind': 'directory', 'mode': mode}
                else:
                    data = path.read_bytes()
                    files[name] = {'data': data, 'mode': mode}
                    manifest['files'][name] = {'sha256': digest(data)}
            files[self.runtime.EVIDENCE + 'runtime.json'] = encode(manifest)
            staged_manifest.update(manifest)
        receipt_path, receipt = write_fixture(self.root, mutate=include_staged_licenses)
        receipt['runtime'] = staged_manifest
        receipt_path.write_bytes(encode(receipt))
        self.assertEqual(self.runtime.load_receipt(receipt_path), receipt)

    def test_archive_evidence_matches_actual_files(self):
        path, expected = write_fixture(self.root)
        self.assertEqual(self.runtime.load_receipt(path), expected)

    def test_archive_corruption_is_rejected(self):
        path, _ = write_fixture(self.root)
        with (self.root / 'image.oci.tar').open('ab') as stream:
            stream.write(b'corruption')
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_external_archive_path_is_rejected(self):
        path, receipt = write_fixture(self.root)
        receipt['archive']['path'] = '../image.oci.tar'
        path.write_bytes(encode(receipt))
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_receipt_cannot_claim_other_binary_or_architecture(self):
        for field, wrong in [('binary_sha256', 'e' * 64), ('architecture', 'amd64')]:
            with self.subTest(field=field):
                path, receipt = write_fixture(self.root)
                receipt[field] = wrong
                path.write_bytes(encode(receipt))
                with self.assertRaises(RuntimeError):
                    self.runtime.load_receipt(path)

    def test_runtime_library_mutation_is_rejected(self):
        path, _ = write_fixture(self.root, mutate=lambda files, _: files.update({
            '/opt/rpgmigrate/target/native/libpq/lib/libpq-oauth-18.so': b'wrong module'}))
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_empty_debian_tool_named_directory_is_allowed(self):
        path, expected = write_fixture(self.root, extra_layer={
            'usr/share/gcc/python': {'kind': 'directory', 'mode': 0o755}})
        self.assertEqual(self.runtime.load_receipt(path), expected)

    def test_development_tools_in_exported_layers_are_rejected(self):
        for name in ('/usr/bin/python3', '/usr/local/bin/cargo', '/usr/bin/psql',
                     '/opt/rpgmigrate/target/native/libpq/include/libpq-fe.h'):
            for value in (b'tool', ('/usr/bin/other-tool',)):
                with self.subTest(name=name, value=value):
                    path, _ = write_fixture(self.root, mutate=lambda files, _: files.update({name: value}))
                    with self.assertRaises(RuntimeError):
                        self.runtime.load_receipt(path)

    def test_root_runtime_user_is_rejected(self):
        path, _ = write_fixture(self.root, mutate=lambda _, config: config['config'].update(User='0:0'))
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_native_symlink_escape_is_rejected(self):
        path, _ = write_fixture(self.root, mutate=lambda files, _: files.update({
            '/opt/rpgmigrate/target/native/libpq/lib/libpq.so.5': ('/usr/lib/libpq.so.5',)}))
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)

    def test_archive_traversal_is_rejected_without_extraction(self):
        path, _ = write_fixture(self.root, mutate=lambda files, _: files.update({'/../escape': b'bad'}))
        with self.assertRaises(RuntimeError):
            self.runtime.load_receipt(path)
        self.assertFalse((self.root.parent / 'escape').exists())


if __name__ == '__main__':
    unittest.main()
