#!/usr/bin/env python3
"""Stage controlled runtime files and validate exported OCI evidence without extraction."""

import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import posixpath
import shutil
import subprocess
import struct
import sys
import tarfile

PREFIX = '/opt/rpgmigrate/target/native/libpq'
CLIENT_PREFIX = '/opt/rpgmigrate/target/native/pgtools'
CLIENT_TOOLS = ('pg_dump', 'pg_restore', 'pg_config')
CLIENT_VERSIONS = ('16.15', '18.6')
BINARY = '/usr/local/bin/rpgmigrate'
EVIDENCE = '/usr/share/rpgmigrate/'
HEX = re.compile(r'[0-9a-f]{64}')
MAX_JSON = 4 * 1024 * 1024


def sha256(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def canonical_sha256(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def execute(command, timeout=120, *, log=None):
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    except (OSError, subprocess.SubprocessError):
        raise RuntimeError('required image command failed or exceeded its deadline') from None
    if log is not None:
        Path(log).write_text(result.stdout + result.stderr)
        Path(log).chmod(0o600)
    require(result.returncode == 0, 'image command failed; raw output withheld')
    return result.stdout.strip()


def check_platform(engine, platform):
    require(engine in ('podman', 'docker'), 'unsupported container engine')
    require(platform in ('linux/amd64', 'linux/arm64'), 'unsupported image platform')
    output = execute([engine, 'info', '--format', '{{.Host.Arch}}' if engine == 'podman' else '{{.OSType}}/{{.Architecture}}'])
    architecture = output.removeprefix('linux/')
    architecture = {'aarch64': 'arm64', 'x86_64': 'amd64'}.get(architecture, architecture)
    require(architecture == platform.split('/')[1], 'image platform must match the native container engine')
    return architecture


def safe_name(name):
    while name.startswith('./'):
        name = name[2:]
    path = PurePosixPath(name)
    require(name and not path.is_absolute() and '..' not in path.parts, 'unsafe archive path')
    return str(path)


def read_json(data):
    require(len(data) <= MAX_JSON, 'JSON evidence exceeds its size limit')
    return json.loads(data)


def source_identity(source):
    files = source['files']
    require(isinstance(files, dict) and files, 'source inventory is empty')
    for name, digest in files.items():
        require(safe_name(name) == name and HEX.fullmatch(digest), 'invalid source identity')
    require(source['sha256'] == canonical_sha256(files), 'source identity mismatch')
    revision = source['revision']
    require(revision is None or re.fullmatch(r'[0-9a-f]{40,64}', revision), 'invalid source revision')


def check_elf_header(header, architecture, *, library):
    require(len(header) == 64 and header[:7] == b'\x7fELF\x02\x01\x01', 'runtime object requires a complete little-endian ELF64 header')
    require(int.from_bytes(header[18:20], 'little') == {'amd64': 62, 'arm64': 183}[architecture], 'runtime ELF machine differs from OCI architecture')
    require(header[20:24] == b'\x01\x00\x00\x00', 'invalid runtime ELF version')
    require(int.from_bytes(header[16:18], 'little') in ((3,) if library else (2, 3)), 'invalid runtime ELF object type')


def elf_dependencies(data):
    """Read ELF64 loader inputs independently of the supplied dependency receipt."""
    require(len(data) >= 64 and data[:7] == b'\x7fELF\x02\x01\x01', 'invalid dependency ELF')
    offset = struct.unpack_from('<Q', data, 32)[0]
    size, count = struct.unpack_from('<HH', data, 54)
    require(count <= 1024 and (not count or size == 56) and offset + size * count <= len(data), 'invalid dependency program headers')
    loads, dynamic, interpreter = [], None, None
    for number in range(count):
        kind, _, file_offset, address, _, length, _, _ = struct.unpack_from('<IIQQQQQQ', data, offset + number * size)
        require(file_offset + length <= len(data), 'truncated dependency segment')
        if kind == 1:
            loads.append((address, file_offset, length))
        elif kind == 2:
            require(dynamic is None and length % 16 == 0, 'invalid dependency dynamic segment')
            dynamic = data[file_offset:file_offset + length]
        elif kind == 3:
            require(interpreter is None and length > 1 and data[file_offset + length - 1] == 0, 'invalid dependency interpreter')
            interpreter = data[file_offset:file_offset + length - 1].decode('ascii')
    entries = []
    if dynamic is not None:
        for position in range(0, len(dynamic), 16):
            tag, value = struct.unpack_from('<QQ', dynamic, position)
            if tag == 0:
                break
            entries.append((tag, value))
        else:
            raise RuntimeError('unterminated dependency dynamic segment')
    tags = dict(entries)
    strings = b''
    if entries:
        require(5 in tags and 10 in tags, 'dependency string table absent')
        for address, file_offset, length in loads:
            if address <= tags[5] and tags[5] + tags[10] <= address + length:
                start = file_offset + tags[5] - address
                strings = data[start:start + tags[10]]
                break
        require(strings, 'dependency string table outside file')
    def string(index):
        end = strings.find(b'\0', index)
        require(0 <= index < len(strings) and end >= index, 'invalid dependency string')
        return strings[index:end].decode('ascii')
    needed = [string(value) for tag, value in entries if tag == 1]
    require(len(needed) == len(set(needed)) and all(re.fullmatch(r'[A-Za-z0-9_.+-]+', value) for value in needed), 'invalid dependency soname')
    paths = [string(value) for tag, value in entries if tag in (15, 29)]
    return {'needed': needed, 'soname': string(tags[14]) if 14 in tags else None,
            'paths': paths, 'interpreter': interpreter}


def ldd_paths(output):
    """Read loader trace paths without inferring library search order."""
    paths = []
    for line in output.splitlines():
        match = re.fullmatch(r'\s*(?:(\S+) => )?(/\S+) \(0x[0-9a-f]+\)\s*', line)
        if match:
            paths.append(match.groups())
        else:
            require(not line.strip() or re.fullmatch(r'\s*linux-vdso\.so\.1 \(0x[0-9a-f]+\)\s*', line),
                    'utility dependency unresolved or malformed loader trace')
    return paths


def client_dependencies(paths):
    pending, graph = list(paths), {}
    while pending:
        path = Path(pending.pop()).resolve(strict=True)
        name = str(path)
        if name in graph:
            continue
        parsed = elf_dependencies(path.read_bytes())
        output = execute(['ldd', name]) if parsed['needed'] else ''
        resolved = {}
        for soname, target in ldd_paths(output):
            target = Path(target).resolve(strict=True)
            if soname is None or soname.startswith('/'):
                soname = elf_dependencies(target.read_bytes())['soname']
                require(soname, 'absolute loader dependency soname absent')
            require(soname not in resolved or resolved[soname] == str(target), 'conflicting loader dependency resolution')
            resolved[soname] = str(target)
        require(set(parsed['needed']) <= set(resolved), 'utility dependency unresolved')
        needed = {key: resolved[key] for key in parsed['needed']}
        interpreter = str(Path(parsed['interpreter']).resolve(strict=True)) if parsed['interpreter'] else None
        require(not interpreter or not output or interpreter in resolved.values(), 'utility interpreter unresolved')
        graph[name] = {'sha256': sha256(path), 'needed': needed, 'interpreter': interpreter}
        pending.extend(needed.values())
        if interpreter:
            pending.append(interpreter)
    return graph


def resolve_archive_path(name, records, *, aliases=None):
    for _ in range(40):
        parts = PurePosixPath(name).parts
        for length in range(2, len(parts) + 1):
            prefix = str(PurePosixPath(*parts[:length]))
            record = records.get(prefix, {})
            if 'link' in record:
                target = record['link']
                if not target.startswith('/'):
                    target = str(PurePosixPath(prefix).parent / target)
                if aliases is not None:
                    aliases.append((prefix, posixpath.normpath(target)))
                name = posixpath.normpath(str(PurePosixPath(target, *parts[length:])))
                break
        else:
            return name
    raise RuntimeError('dependency symlink cycle or excessive depth')


def interpreter_failure(name, recorded, requested, records, objects, source, architecture):
    """Expose bounded loader facts without logging arbitrary archive paths or data."""
    def path(value):
        pattern = r'(?:/usr/lib(?:64)?|/lib(?:64)?|' + re.escape(CLIENT_PREFIX) + r')(?:/[A-Za-z0-9_.+-]+)*'
        return value if isinstance(value, str) and len(value) <= 256 and re.fullmatch(pattern, value) and posixpath.normpath(value) == value else '<redacted>'
    def digest(value):
        return value if isinstance(value, str) and HEX.fullmatch(value) else None
    aliases, resolved = [], None
    try:
        resolved = resolve_archive_path(requested, records, aliases=aliases)
    except RuntimeError:
        pass
    revision = source.get('revision')
    return json.dumps({
        'object': path(name), 'object_sha256': digest(records.get(name, {}).get('sha256')),
        'recorded_interpreter': path(recorded), 'elf_interpreter': path(requested),
        'resolved_interpreter': path(resolved), 'recorded_is_elf': recorded in objects,
        'resolved_is_elf': resolved in objects,
        'recorded_sha256': digest(records.get(recorded, {}).get('sha256')),
        'resolved_sha256': digest(records.get(resolved, {}).get('sha256')),
        'aliases': [[path(alias), path(target)] for alias, target in aliases[:8]],
        'alias_count': len(aliases), 'source_sha256': digest(source.get('sha256')),
        'source_revision': revision if isinstance(revision, str) and re.fullmatch(r'[0-9a-f]{40,64}', revision) else None,
        'architecture': architecture,
    }, sort_keys=True, separators=(',', ':'))


def validate_clients(runtime, client, source, source_bytes, records, objects, architecture):
    profile = runtime['clients']
    version = profile['version']
    require(version in CLIENT_VERSIONS and version == client['version'] == client['source']['version'], 'client version mismatch')
    require(profile['prefix'] == CLIENT_PREFIX, 'client prefix mismatch')
    require(client['commands'] == {name: ('PostgreSQL ' if name == 'pg_config' else name + ' (PostgreSQL) ') + version for name in CLIENT_TOOLS} | {'bindir': CLIENT_PREFIX + '/bin'}, 'client command version or bindir mismatch')
    require(profile['receipt_sha256'] == hashlib.sha256(source_bytes['receipt']).hexdigest(), 'client receipt identity mismatch')
    metadata_name = 'native/libpq/' + ('source16.json' if version == '16.15' else 'source.json')
    require(read_json(source_bytes['metadata']) == client['source'] and source['files'].get(metadata_name) == hashlib.sha256(source_bytes['metadata']).hexdigest(), 'client source identity mismatch')
    require(hashlib.sha256(json.dumps(client['files'], sort_keys=True).encode()).hexdigest() == client['artifact_sha256'], 'client artifact identity mismatch')
    installed = {name for name in records if name.startswith(CLIENT_PREFIX + '/')}
    require(installed == {name for name in runtime['files'] if name.startswith(CLIENT_PREFIX + '/')}, 'unrecorded client runtime file')
    for name in installed:
        target, visited = name, set()
        while 'link' in records[target]:
            require(target not in visited, 'client symlink cycle')
            visited.add(target)
            target = str(PurePosixPath(target).parent / records[target]['link'])
            require(target in installed, 'client symlink target absent')
        require(client['files'].get(name.removeprefix(CLIENT_PREFIX + '/')) == records[target]['sha256'], 'client artifact file mismatch')
    required = {CLIENT_PREFIX + '/bin/' + name for name in CLIENT_TOOLS}
    require(all(CLIENT_PREFIX + '/' + name in installed for name in ('lib/libpq.so.5', 'share/licenses/libpq/COPYRIGHT', 'share/licenses/libpq/NOTICE')), 'required client library or license missing')
    if version == '18.6':
        require(CLIENT_PREFIX + '/lib/libpq-oauth-18.so' in installed, 'client OAuth library missing')
        required.add(CLIENT_PREFIX + '/lib/libpq-oauth-18.so')
    require(required <= installed, 'required client utility missing')
    allowed = required | {CLIENT_PREFIX + '/' + name for name in ('lib/libpq.so.5', 'share/licenses/libpq/COPYRIGHT', 'share/licenses/libpq/NOTICE')}
    for name in list(allowed):
        if 'link' in records[name]:
            allowed.add(resolve_archive_path(name, records))
    require(installed == allowed, 'unknown client runtime file or utility')
    graph = profile['dependencies']
    pending, visited = list(required), set()
    while pending:
        name = pending.pop()
        while name in records and 'link' in records[name]:
            name = str(PurePosixPath(name).parent / records[name]['link'])
        if name in visited:
            continue
        visited.add(name)
        require(name in graph and name in objects, 'dependency object absent')
        require(not any(str(parent) in records for parent in PurePosixPath(name).parents), 'dependency ancestor is not a directory')
        record, parsed = graph[name], objects[name]
        require(records.get(name) == {'sha256': record['sha256']}, 'dependency identity mismatch')
        require(set(record) == {'sha256', 'needed', 'interpreter'} and set(record['needed']) == set(parsed['needed']), 'dependency inventory mismatch')
        for needed, target in record['needed'].items():
            require(target in objects and objects[target]['soname'] == needed, 'dependency soname mismatch')
            if needed == 'libpq.so.5':
                require(target == resolve_archive_path(CLIENT_PREFIX + '/lib/libpq.so.5', records) and parsed['paths'] == [CLIENT_PREFIX + '/lib'], 'client library resolution mismatch')
            pending.append(target)
        interpreter = record['interpreter']
        require((interpreter is None) == (parsed['interpreter'] is None), 'dependency interpreter mismatch')
        if interpreter:
            if not (interpreter in objects and interpreter == resolve_archive_path(parsed['interpreter'], records)):
                raise RuntimeError('dependency interpreter target mismatch: ' +
                                   interpreter_failure(name, interpreter, parsed['interpreter'], records, objects, source, architecture))
            pending.append(interpreter)
        require(name.startswith((CLIENT_PREFIX + '/', '/usr/lib/', '/lib/', '/lib64/')), 'dependency outside runtime library roots')
    require(visited == set(graph), 'unreachable dependency evidence')


def inspect_archive(path):
    """Validate one native OCI image and return the evidence found in its layers."""
    try:
        return _inspect_archive(Path(path))
    except (OSError, ValueError, KeyError, TypeError, AttributeError, tarfile.TarError) as error:
        raise RuntimeError('invalid or unavailable OCI evidence') from error


def _inspect_archive(path):
    require(not path.is_symlink() and path.is_file(), 'OCI archive must be a regular file')
    require(0 < path.stat().st_size <= 2 * 1024**3, 'OCI archive exceeds its size limit')
    with tarfile.open(path, 'r:*') as archive:
        members = {}
        for member in archive:
            name = safe_name(member.name)
            require(name not in members, 'duplicate OCI archive path')
            require(member.isdir() or member.isfile(), 'OCI layout contains a link or special file')
            members[name] = member

        def read(name, limit=MAX_JSON):
            member = members[name]
            require(member.isfile() and member.size <= limit, 'invalid OCI metadata size or type')
            return archive.extractfile(member).read()

        def blob(descriptor, *, document=False):
            digest = descriptor['digest']
            require(re.fullmatch(r'sha256:[0-9a-f]{64}', digest), 'unsupported OCI digest')
            member = members['blobs/sha256/' + digest[7:]]
            require(member.isfile() and member.size == descriptor['size'], 'OCI descriptor size mismatch')
            with archive.extractfile(member) as stream:
                require(hashlib.file_digest(stream, 'sha256').hexdigest() == digest[7:], 'OCI blob digest mismatch')
            return read_json(read('blobs/sha256/' + digest[7:])) if document else member

        require(read_json(read('oci-layout')) == {'imageLayoutVersion': '1.0.0'}, 'unsupported OCI layout')
        index = read_json(read('index.json'))
        require(index['schemaVersion'] == 2 and len(index['manifests']) == 1, 'expected exactly one OCI image')
        descriptor = index['manifests'][0]
        manifest = blob(descriptor, document=True)
        require(manifest.get('schemaVersion') == 2 and 'layers' in manifest, 'expected an OCI image manifest')
        config = blob(manifest['config'], document=True)
        architecture = config['architecture']
        require(config['os'] == 'linux' and architecture in ('arm64', 'amd64'), 'unsupported OCI platform')
        settings = config['config']
        for key, value in {'User': '1000:1000', 'WorkingDir': '/work', 'Entrypoint': [BINARY],
                           'Cmd': ['--help'], 'StopSignal': 'SIGTERM'}.items():
            require(settings.get(key) == value, 'unexpected runtime image configuration')
        env = dict(item.split('=', 1) for item in settings.get('Env', []))
        require(env.get('PG_CONFIG') == CLIENT_PREFIX + '/bin/pg_config' and env.get('PATH', '').split(':')[0] == CLIENT_PREFIX + '/bin', 'unexpected client selection environment')
        require(env.get('HOME') == '/home/rpgmigrate' and env.get('TMPDIR') == '/work', 'unexpected runtime environment')
        layers = manifest['layers']
        diff_ids = config['rootfs']['diff_ids']
        require(config['rootfs']['type'] == 'layers' and layers and len(layers) == len(diff_ids), 'invalid OCI layer inventory')
        records, documents, objects = {}, {}, {}
        for number, layer in enumerate(layers):
            member = blob(layer)
            media = layer['mediaType']
            require(media in ('application/vnd.oci.image.layer.v1.tar', 'application/vnd.oci.image.layer.v1.tar+gzip'), 'unsupported OCI layer compression')
            with archive.extractfile(member) as raw:
                stream = gzip.GzipFile(fileobj=raw) if media.endswith('+gzip') else raw
                checksum = hashlib.sha256()
                total = 0
                while chunk := stream.read(1024 * 1024):
                    total += len(chunk)
                    require(total <= 1024**3, 'OCI layer exceeds its size limit')
                    checksum.update(chunk)
                require('sha256:' + checksum.hexdigest() == diff_ids[number], 'OCI layer diff ID mismatch')
            with archive.extractfile(member) as raw, tarfile.open(fileobj=raw, mode='r:*') as contents:
                additions, new_documents, seen = {}, {}, set()
                for item in contents:
                    name = '/' + safe_name(item.name)
                    filename = PurePosixPath(name).name
                    tool = name in {CLIENT_PREFIX + '/bin/' + value for value in CLIENT_TOOLS}
                    require(item.isdir() or tool or not re.fullmatch(r'(?:python[0-9.]*|cargo|rustc|rustup|pg_.*|psql|postgres|initdb|createdb|createuser|dropdb|dropuser|clusterdb|reindexdb|vacuumdb|vacuumlo)', filename), 'development or database tooling found in runtime')
                    require(item.isdir() or not name.startswith(CLIENT_PREFIX + '/bin/') or tool, 'unknown client utility')
                    require(not name.startswith(('/usr/local/cargo/', '/usr/local/rustup/', '/opt/rpgmigrate/src/', '/opt/rpgmigrate/tests/', '/opt/rpgmigrate/tools/')), 'build inputs found in runtime')
                    if filename.startswith('.wh.'):
                        parent = str(PurePosixPath(name).parent)
                        target = parent + '/' if filename == '.wh..wh..opq' else parent + '/' + filename[4:]
                        for previous in list(records):
                            if previous == target or previous.startswith(target.rstrip('/') + '/'):
                                records.pop(previous)
                                documents.pop(previous, None)
                                objects.pop(previous, None)
                        continue
                    system = name in ('/lib', '/lib64') or name.startswith(('/usr/lib/', '/lib/', '/lib64/'))
                    relevant = name == BINARY or name.startswith((PREFIX + '/', CLIENT_PREFIX + '/', EVIDENCE)) or system
                    required_directory = name in ('/usr/lib', PREFIX, CLIENT_PREFIX, EVIDENCE.rstrip('/')) or any(
                        path.startswith(name + '/') for path in (BINARY, PREFIX, CLIENT_PREFIX, EVIDENCE.rstrip('/')))
                    if not relevant and not required_directory:
                        continue
                    require(name not in seen, 'duplicate protected path within an OCI layer')
                    seen.add(name)
                    require(not required_directory or item.isdir(), 'runtime ancestor or root is not a directory')
                    if item.isdir():
                        require(item.uid == 0 and item.gid == 0 and not item.mode & 0o6022, 'runtime directory has unsafe ownership or permissions')
                        records.pop(name, None)
                        documents.pop(name, None)
                        objects.pop(name, None)
                        continue
                    require(not any(str(parent) in records or str(parent) in additions for parent in PurePosixPath(name).parents), 'runtime parent is not a directory')
                    require(not any(path.startswith(name + '/') for path in additions), 'runtime object replaces a parent within one layer')
                    for previous in list(records):
                        if previous.startswith(name + '/'):
                            records.pop(previous)
                            documents.pop(previous, None)
                            objects.pop(previous, None)
                    require(item.uid == 0 and item.gid == 0, 'runtime file is not root-owned')
                    require(not item.mode & 0o6000, 'runtime object has setuid or setgid bits')
                    if item.issym():
                        require(system or (not item.linkname.startswith('/') and '..' not in PurePosixPath(item.linkname).parts), 'runtime symlink escapes its directory')
                        objects.pop(name, None)
                        additions[name] = {'link': item.linkname}
                    else:
                        require(item.isfile() and not item.mode & 0o022, 'runtime contains a special or writable file')
                        objects.pop(name, None)
                        with contents.extractfile(item) as stream:
                            if system:
                                require(item.size <= 32 * 1024**2, 'dependency file exceeds size limit')
                                data = stream.read()
                                if data[:7] == b'\x7fELF\x02\x01\x01':
                                    check_elf_header(data[:64], architecture, library=False)
                                    objects[name] = elf_dependencies(data)
                                stream.seek(0)
                            if name.startswith(CLIENT_PREFIX + '/bin/'):
                                require(item.mode & 0o111, 'client utility is not executable')
                            if name.startswith((CLIENT_PREFIX + '/bin/', CLIENT_PREFIX + '/lib/')):
                                require(item.size <= 32 * 1024**2, 'client object exceeds size limit')
                                data = stream.read()
                                check_elf_header(data[:64], architecture, library='/lib/' in name)
                                objects[name] = elf_dependencies(data)
                                stream.seek(0)
                            if name == BINARY or name.startswith(PREFIX + '/lib/'):
                                check_elf_header(stream.read(64), architecture, library=name != BINARY)
                                stream.seek(0)
                            additions[name] = {'sha256': hashlib.file_digest(stream, 'sha256').hexdigest()}
                        if name.startswith(EVIDENCE) and PurePosixPath(name).name in ('source.json', 'native-artifact.json', 'runtime.json', 'packages.txt', 'builder-packages.txt', 'client-artifact.json', 'client-source.json'):
                            require(item.size <= MAX_JSON, 'runtime evidence exceeds its size limit')
                            new_documents[name] = contents.extractfile(item).read()
                for name in additions:
                    documents.pop(name, None)
                records.update(additions)
                documents.update(new_documents)

    native_bytes = documents[EVIDENCE + 'native-artifact.json']
    native = read_json(native_bytes)
    runtime = read_json(documents[EVIDENCE + 'runtime.json'])
    source = read_json(documents[EVIDENCE + 'source.json'])
    source_identity(source)
    require(native['capabilities'] == 1 and native['libpq_version'] == 180006, 'native capability or version mismatch')
    require(all(native['features'].get(key) is True for key in ('openssl', 'gssapi', 'oauth_libcurl', 'service', 'passfile')), 'native feature evidence missing')
    require(hashlib.sha256(json.dumps(native['files'], sort_keys=True).encode()).hexdigest() == native['artifact_sha256'], 'native artifact identity mismatch')
    require(type(runtime['schema']) is int and runtime['schema'] == 1 and runtime['prefix'] == PREFIX, 'invalid runtime manifest')
    require(runtime['native_receipt_sha256'] == hashlib.sha256(native_bytes).hexdigest(), 'native receipt identity mismatch')
    expected = runtime['files']
    require(expected and BINARY in expected, 'runtime file inventory is empty')
    for name, record in expected.items():
        require(name == '/' + safe_name(name.removeprefix('/')), 'invalid runtime file path')
        require(records.get(name) == record, 'runtime file identity mismatch')
    native_paths = {name for name in records if name.startswith(PREFIX + '/')}
    require(native_paths == {name for name in expected if name.startswith(PREFIX + '/')}, 'unrecorded native runtime file')
    for name in native_paths:
        record = records[name]
        target, visited = name, set()
        while 'link' in records[target]:
            require(target not in visited, 'native runtime symlink cycle')
            visited.add(target)
            target = str(PurePosixPath(target).parent / records[target]['link'])
            require(target in native_paths, 'native runtime symlink target is absent')
        require(native['files'].get(name.removeprefix(PREFIX + '/')) == records[target]['sha256'], 'trimmed runtime differs from original native receipt')
    require(all(PREFIX + '/' + suffix in native_paths for suffix in ('lib/libpq.so.5', 'lib/libpq-oauth-18.so', 'share/licenses/libpq/COPYRIGHT', 'share/licenses/libpq/NOTICE')), 'required runtime library or license missing')
    require(not any(name.endswith(('.a', '.h', '.pc')) for name in native_paths), 'development files found in runtime')
    validate_clients(runtime, read_json(documents[EVIDENCE + 'client-artifact.json']), source,
                     {'receipt': documents[EVIDENCE + 'client-artifact.json'], 'metadata': documents[EVIDENCE + 'client-source.json']},
                     records, objects, architecture)
    packages = {}
    for key, filename in (('packages', 'packages.txt'), ('builder_packages', 'builder-packages.txt')):
        values = documents[EVIDENCE + filename].decode().splitlines()
        require(values and len(values) == len(set(values)) and all(re.fullmatch(r'[a-z0-9+.-]+(?::[a-z0-9]+)?=[^\s]+', value) for value in values), 'invalid package receipt')
        packages[key] = values
    return {'platform': 'linux/' + architecture, 'architecture': architecture,
            'image': {'manifest_digest': descriptor['digest'], 'config_digest': manifest['config']['digest']},
            'source': source, 'native': native, 'runtime': runtime, 'binary_sha256': records[BINARY]['sha256'], **packages}


def load_receipt(receipt_path):
    try:
        path = Path(receipt_path)
        require(not path.is_symlink() and path.is_file(), 'receipt must be a regular file')
        receipt = read_json(path.read_bytes())
        require(type(receipt['schema']) is int and receipt['schema'] == 1 and receipt['status'] == 'built', 'image build receipt is incomplete')
        require(receipt['archive']['path'] == 'image.oci.tar', 'archive must be the receipt sibling image.oci.tar')
        archive = path.parent / receipt['archive']['path']
        require(not archive.is_symlink() and sha256(archive) == receipt['archive']['sha256'], 'OCI archive SHA-256 mismatch')
        actual = inspect_archive(archive)
        require(set(receipt) == {'schema', 'status', 'archive', *actual}, 'unexpected image receipt fields')
        require(all(receipt[key] == value for key, value in actual.items()), 'receipt does not match exported OCI image')
        return receipt
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise RuntimeError('invalid or unavailable image receipt') from error


def native_runtime_files(prefix, native):
    prefix = Path(prefix)
    selected = [prefix / name for name in ('lib/libpq.so.5', 'lib/libpq-oauth-18.so',
                'share/licenses/libpq/COPYRIGHT', 'share/licenses/libpq/NOTICE')]
    for path in list(selected):
        require(path.is_file(), 'required native runtime file missing')
        require(path.resolve().is_relative_to(prefix.resolve()), 'native symlink escapes controlled prefix')
        if path.is_symlink():
            require(not path.readlink().is_absolute() and '..' not in path.readlink().parts, 'native symlink escapes controlled prefix')
            selected.append(prefix / path.resolve(strict=True).relative_to(prefix.resolve()))
    for path in selected:
        require(sha256(path) == native['files'].get(str(path.relative_to(prefix))), 'native input differs from its receipt')
    return sorted(set(selected))


def client_runtime_files(prefix, receipt):
    prefix = Path(prefix)
    names = ['bin/' + name for name in CLIENT_TOOLS] + ['lib/libpq.so.5', 'share/licenses/libpq/COPYRIGHT', 'share/licenses/libpq/NOTICE']
    if receipt['version'] == '18.6':
        names.append('lib/libpq-oauth-18.so')
    selected = [prefix / name for name in names]
    for path in list(selected):
        require(path.is_file() and path.resolve().is_relative_to(prefix.resolve()), 'required client runtime file missing or external')
        if path.is_symlink():
            require(not path.readlink().is_absolute() and '..' not in path.readlink().parts, 'client symlink escapes prefix')
            selected.append(path.resolve(strict=True))
    for path in selected:
        require(sha256(path) == receipt['files'].get(str(path.relative_to(prefix))), 'client input differs from receipt')
    return sorted(set(selected))


def recipe_identity(recipe):
    bases = re.findall(r'^FROM (\S+)', recipe, re.M)
    require(len(bases) == 2 and all(re.fullmatch(r'[^@]+@sha256:[0-9a-f]{64}', value) for value in bases), 'runtime recipe requires two digest-pinned bases')
    snapshots = set(re.findall(r'https://snapshot.debian.org/archive/[^/]+/(\d{8}T\d{6}Z)', recipe))
    require(len(snapshots) == 1, 'runtime recipe requires one signed Debian snapshot')
    return {'base_images': dict(zip(('builder', 'runtime'), bases)), 'debian_snapshot': snapshots.pop()}


def stage_runtime(root, destination):
    """Copy only verified shared objects and notices from the native installation."""
    root, destination = Path(root), Path(destination)
    require(str(root) == '/opt/rpgmigrate', 'runtime build must use the controlled absolute prefix')
    prefix = root / 'target/native/libpq'
    original = prefix / 'artifact.json'
    native = read_json(original.read_bytes())
    selected = native_runtime_files(prefix, native)
    clients_prefix = root / 'target/native/pgtools'
    client_receipt = clients_prefix / 'artifact.json'
    client = read_json(client_receipt.read_bytes())
    require(client['version'] in CLIENT_VERSIONS, 'unsupported client version')
    client_selected = client_runtime_files(clients_prefix, client)
    for path in client_selected:
        target = destination / str(path).lstrip('/')
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target, follow_symlinks=False)
    selected += [root / 'target/release/rpgmigrate', root / 'NOTICE']
    for path in selected:
        relative = path.relative_to(prefix) if path.is_relative_to(prefix) else None
        require(path.is_file() and (relative is None or sha256(path) == native['files'].get(str(relative))), 'native input differs from its receipt')
        target = destination / (str(path).lstrip('/') if relative is not None else (BINARY.lstrip('/') if path.name == 'rpgmigrate' else EVIDENCE.lstrip('/') + 'NOTICE'))
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copy2(path, target, follow_symlinks=False)
    provenance = destination / EVIDENCE.lstrip('/')
    shutil.copyfile(original, provenance / 'native-artifact.json')
    shutil.copyfile(client_receipt, provenance / 'client-artifact.json')
    metadata_name = 'source16.json' if client['version'] == '16.15' else 'source.json'
    shutil.copyfile(root / 'native/libpq' / metadata_name, provenance / 'client-source.json')
    shutil.copyfile(root / 'source.json', provenance / 'source.json')
    copy_licenses(provenance / 'licenses')
    files = {}
    for path in sorted(destination.rglob('*')):
        if path.is_symlink():
            files['/' + str(path.relative_to(destination))] = {'link': str(path.readlink())}
        elif path.is_file() and path.name not in ('native-artifact.json', 'source.json', 'client-artifact.json', 'client-source.json'):
            files['/' + str(path.relative_to(destination))] = {'sha256': sha256(path)}
    receipt = {'schema': 1, 'prefix': PREFIX, 'native_receipt_sha256': sha256(original), 'files': files,
               **recipe_identity((root / 'Dockerfile').read_text()),
               'clients': {'version': client['version'], 'prefix': CLIENT_PREFIX, 'receipt_sha256': sha256(client_receipt),
                           'dependencies': client_dependencies([path for path in client_selected if '/bin/' in str(path) or '/lib/' in str(path)])}}
    (provenance / 'runtime.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')


def copy_licenses(destination):
    metadata = json.loads(execute(['cargo', 'metadata', '--locked', '--format-version', '1'], timeout=180))
    nodes = {node['id']: node for node in metadata['resolve']['nodes']}
    pending, selected = [metadata['resolve']['root']], set()
    while pending:
        identity = pending.pop()
        if identity in selected:
            continue
        selected.add(identity)
        pending.extend(dep['pkg'] for dep in nodes[identity]['deps'] if any(kind['kind'] != 'dev' for kind in dep['dep_kinds']))
    for package in metadata['packages']:
        if package['id'] not in selected or package['source'] is None:
            continue
        directory = Path(package['manifest_path']).parent
        licenses = [path for path in directory.rglob('*') if path.is_file() and path.name.upper().startswith(('LICENSE', 'COPYING', 'COPYRIGHT', 'NOTICE'))]
        require(licenses, 'locked crate has no packaged license notice')
        for path in licenses:
            target = destination / (package['name'] + '-' + package['version']) / path.relative_to(directory)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
    sysroot = Path(execute(['rustc', '--print', 'sysroot'])) / 'share/doc/rust'
    copy_rust_licenses(sysroot, destination / 'rust')


def copy_rust_licenses(source, destination):
    require((source / 'COPYRIGHT-library.html').is_file() and (source / 'licenses').is_dir(), 'Rust runtime license notices missing')
    destination.mkdir(parents=True)
    shutil.copyfile(source / 'COPYRIGHT-library.html', destination / 'COPYRIGHT-library.html')
    shutil.copytree(source / 'licenses', destination / 'licenses')
    # Rust's packaged notice modes must not make the runtime copies writable.
    for path in [destination, *destination.rglob('*')]:
        path.chmod(0o755 if path.is_dir() else 0o644)


if __name__ == '__main__':
    try:
        require(len(sys.argv) == 3 and sys.argv[1] == '--stage', 'use runtime.py --stage NEW_DIRECTORY')
        stage_runtime(Path.cwd(), Path(sys.argv[2]))
    except (RuntimeError, OSError, ValueError, KeyError) as error:
        print(f'runtime image: {error}', file=sys.stderr)
        sys.exit(1)
