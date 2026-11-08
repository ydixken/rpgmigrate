#!/usr/bin/env python3
"""Exercise smoke evidence and invocation refusals without a container engine."""

import hashlib
import importlib.util
import inspect
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import subprocess
import sys
import socket
import threading
import unittest
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("smoke.py")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class SmokeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if SCRIPT.is_file():
            spec = importlib.util.spec_from_file_location("image_smoke", SCRIPT)
            cls.smoke = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(cls.smoke)

    def setUp(self):
        self.assertTrue(SCRIPT.is_file(), "smoke adapter implementation is absent")

    def evidence(self):
        build = {
            "platform": "linux/arm64", "architecture": "arm64",
            "archive": {"path": "image.oci.tar", "sha256": "a" * 64},
            "image": {"manifest_digest": "sha256:" + "b" * 64,
                      "config_digest": "sha256:" + "c" * 64},
            "source": {"sha256": "d" * 64}, "binary_sha256": "e" * 64,
            "native": {"capabilities": 1}, "runtime": {"schema": 1},
        }
        record = {
            "schema": 1, "status": "passed", "receipt_sha256": "f" * 64,
            "archive_sha256": "a" * 64, "manifest_digest": "sha256:" + "b" * 64,
            "config_digest": "sha256:" + "c" * 64, "platform": "linux/arm64",
            "architecture": "arm64", "source_sha256": "d" * 64,
            "binary_sha256": "e" * 64, "native_sha256": digest(build["native"]),
            "runtime_sha256": digest(build["runtime"]),
            "fixture_image": "sha256:" + "1" * 64,
            "fixture_version": "17.11", "fixture_version_num": 170011,
            "probe_sha256": "2" * 64,
            "checks": [{"name": name, "status": "passed"} for name in self.smoke.SMOKE_CHECKS],
        }
        return build, record

    def test_client_commands_check_versions_and_runtime_bindir(self):
        from types import SimpleNamespace
        prefix = '/opt/rpgmigrate/target/native/pgtools'
        for version in ('16.15', '18.6'):
            build = {'runtime': {'clients': {'prefix': prefix, 'version': version}}}
            outputs = ['pg_dump (PostgreSQL) ' + version, 'pg_restore (PostgreSQL) ' + version,
                       'PostgreSQL ' + version, prefix + '/bin']
            for bad in (None, 0, 1, 2, 3):
                values = outputs.copy()
                if bad is not None:
                    values[bad] = 'wrong'
                results = [subprocess.CompletedProcess([], 0, value + '\n', '') for value in values]
                engine = SimpleNamespace(run=unittest.mock.Mock(side_effect=results))
                with self.subTest(version=version, bad=bad):
                    if bad is None:
                        self.smoke.client_commands(engine, 'image', build)
                        self.assertEqual(engine.run.call_args_list[-1].kwargs['entrypoint'], prefix + '/bin/pg_config')
                    else:
                        with self.assertRaisesRegex(self.smoke.SmokeError, 'version|bindir'):
                            self.smoke.client_commands(engine, 'image', build)

    def loader_case(self, version='18.6', architecture='arm64'):
        from types import SimpleNamespace
        prefix = '/opt/rpgmigrate/target/native/pgtools'
        libc = '/usr/lib/libc.so.6'
        loader = '/usr/lib/' + ('ld-linux-aarch64.so.1' if architecture == 'arm64' else 'ld-linux-x86-64.so.2')
        soname = loader.rsplit('/', 1)[1]
        libpq = prefix + '/lib/libpq.so.5'
        graph = {loader: {'sha256': 'a' * 64, 'needed': {}, 'interpreter': None},
                 libc: {'sha256': 'b' * 64, 'needed': {soname: loader}, 'interpreter': loader},
                 libpq: {'sha256': 'c' * 64, 'needed': {'libc.so.6': libc}, 'interpreter': None}}
        base_trace = 'linux-vdso.so.1 (0x100)\nlibc.so.6 => /lib/libc.so.6 (0x200)\n/lib/' + soname + ' (0x300)\n'
        traces = {libpq: base_trace}
        for name in ('pg_dump', 'pg_restore', 'pg_config'):
            needed = {'libc.so.6': libc} if name == 'pg_config' else {'libpq.so.5': libpq, 'libc.so.6': libc}
            path = prefix + '/bin/' + name
            graph[path] = {'sha256': 'd' * 64, 'needed': needed, 'interpreter': loader}
            traces[path] = ('' if name == 'pg_config' else 'libpq.so.5 => ' + libpq + ' (0x400)\n') + base_trace
        if version == '18.6':
            oauth = prefix + '/lib/libpq-oauth-18.so'
            graph[oauth] = {'sha256': 'e' * 64, 'needed': {'libc.so.6': libc}, 'interpreter': None}
            traces[oauth] = base_trace
        state = {'traces': traces, 'aliases': {name: name for name in graph} |
                 {'/lib/libc.so.6': libc, '/lib/' + soname: loader},
                 'hashes': {name: node['sha256'] for name, node in graph.items()},
                 'ldd_code': 0, 'ldd_stderr': '', 'calls': []}
        build = {'runtime': {'clients': {'version': version, 'prefix': prefix, 'dependencies': graph}}}
        def run(image, args, *, entrypoint):
            self.assertEqual(image, 'exact-image')
            state['calls'].append((entrypoint, args))
            if entrypoint == '/usr/bin/ldd':
                self.assertEqual(len(args), 1)
                return subprocess.CompletedProcess([], state['ldd_code'], state['traces'][args[0]], state['ldd_stderr'])
            if entrypoint == '/usr/bin/readlink':
                self.assertEqual(args[:2], ['-e', '--'])
                self.assertEqual(len(args), 3)
                value = state['aliases'].get(args[2])
                return subprocess.CompletedProcess([], 0 if value else 1, (value + '\n') if value else '', '')
            self.assertEqual(entrypoint, '/usr/bin/sha256sum')
            self.assertEqual(args[0], '--')
            return subprocess.CompletedProcess([], 0, ''.join(state['hashes'][name] + '  ' + name + '\n' for name in args[1:]), '')
        return build, SimpleNamespace(run=run), state

    def check_loader(self, engine, build):
        self.assertTrue(hasattr(self.smoke, 'client_loader_resolution'), 'final-image loader check is required')
        self.smoke.client_loader_resolution(engine, 'exact-image', build)

    def test_loader_checks_all_client_contexts_for_both_versions_and_architectures(self):
        for version in ('16.15', '18.6'):
            for architecture in ('amd64', 'arm64'):
                with self.subTest(version=version, architecture=architecture):
                    build, engine, state = self.loader_case(version, architecture)
                    self.check_loader(engine, build)
                    self.assertEqual({args[0] for command, args in state['calls'] if command == '/usr/bin/ldd'}, set(state['traces']))
                    loader = 'ld-linux-aarch64.so.1' if architecture == 'arm64' else 'ld-linux-x86-64.so.2'
                    state['traces'] = {name: trace.replace('/lib/' + loader + ' (0x300)', loader + ' => /lib/' + loader + ' (0x300)')
                                       for name, trace in state['traces'].items()}
                    self.check_loader(engine, build)

    def test_unsearched_graph_cannot_leave_actual_default_libc_unbound(self):
        for version in ('16.15', '18.6'):
            for architecture in ('amd64', 'arm64'):
                with self.subTest(version=version, architecture=architecture):
                    build, engine, state = self.loader_case(version, architecture)
                    graph = build['runtime']['clients']['dependencies']
                    old, alternate = '/usr/lib/libc.so.6', '/usr/lib/unsearched/libc.so.6'
                    graph[alternate] = graph.pop(old)
                    for node in graph.values():
                        node['needed'] = {key: alternate if path == old else path for key, path in node['needed'].items()}
                    state['hashes'][old] = 'f' * 64
                    with self.assertRaisesRegex(self.smoke.SmokeError, 'client loader'):
                        self.check_loader(engine, build)

    def test_loader_refuses_changed_bytes_interpreter_alias_and_missing_or_extra_selections(self):
        for fault in ('bytes', 'interpreter', 'missing', 'extra', 'conflict', 'unresolved', 'empty', 'failed', 'stderr', 'edge'):
            with self.subTest(fault=fault):
                build, engine, state = self.loader_case()
                if fault == 'bytes':
                    state['hashes']['/usr/lib/libc.so.6'] = 'f' * 64
                elif fault == 'interpreter':
                    state['aliases']['/lib/ld-linux-aarch64.so.1'] = '/usr/lib/libc.so.6'
                elif fault in ('failed', 'stderr'):
                    state['ldd_code'] = 1 if fault == 'failed' else 0
                    state['ldd_stderr'] = 'loader failed'
                else:
                    for name, trace in state['traces'].items():
                        if fault == 'edge':
                            if name.endswith('/pg_restore'):
                                trace = trace.replace('libc.so.6 => /lib/libc.so.6', 'libc.so.6 => /lib/ld-linux-aarch64.so.1')
                                trace += '/lib/libc.so.6 (0x700)\n'
                        elif fault == 'missing':
                            trace = trace.replace('/lib/ld-linux-aarch64.so.1 (0x300)\n', '')
                        elif fault == 'extra':
                            trace += 'extra.so => /usr/lib/extra.so (0x500)\n'
                        elif fault == 'conflict':
                            trace += 'libc.so.6 => /usr/lib/unsearched/libc.so.6 (0x600)\n'
                        elif fault == 'unresolved':
                            trace = trace.replace('libc.so.6 => /lib/libc.so.6 (0x200)', 'libc.so.6 => not found')
                        else:
                            trace = ''
                        state['traces'][name] = trace
                with self.assertRaisesRegex(self.smoke.SmokeError, 'client loader'):
                    self.check_loader(engine, build)

    def test_loader_empty_graph_cannot_pass(self):
        build, engine, _ = self.loader_case()
        build['runtime']['clients']['dependencies'] = {}
        with self.assertRaisesRegex(self.smoke.SmokeError, 'client loader'):
            self.check_loader(engine, build)

    def test_missing_loader_check_refuses_smoke_acceptance(self):
        build, record = self.evidence()
        record['checks'] = [item for item in record['checks'] if item['name'] != 'client-loader-resolution']
        with self.assertRaisesRegex(self.smoke.SmokeError, 'smoke checks'):
            self.smoke.validate_smoke_receipt(build, record, 'f' * 64)

    def test_qualification_must_run_loader_gate_before_other_probes(self):
        with patch.object(self.smoke, 'runtime_files'), patch.object(self.smoke, 'client_commands'), \
             patch.object(self.smoke, 'client_loader_resolution', create=True, side_effect=self.smoke.SmokeError('client loader refused')), \
             patch.object(self.smoke, 'prepare_probe', side_effect=AssertionError('probe ran before loader gate')):
            with self.assertRaisesRegex(self.smoke.SmokeError, 'client loader refused'):
                self.smoke.qualify(None, 'exact-image', None, {}, lambda _: None)

    def test_qualification_records_loader_check_only_after_success(self):
        for version in ('16.15', '18.6'):
            for architecture in ('amd64', 'arm64'):
                with self.subTest(version=version, architecture=architecture):
                    build, engine, _ = self.loader_case(version, architecture)
                    events = []
                    with patch.object(self.smoke, 'runtime_files'), patch.object(self.smoke, 'client_commands'), \
                         patch.object(self.smoke, 'prepare_probe', side_effect=self.smoke.SmokeError('probe stop')):
                        with self.assertRaisesRegex(self.smoke.SmokeError, 'probe stop'):
                            self.smoke.qualify(engine, 'exact-image', None, build, events.append)
                    self.assertEqual(events, ['runtime-files', 'client-loader-resolution'])

    def test_exact_complete_evidence_is_accepted(self):
        build, record = self.evidence()
        self.smoke.validate_smoke_receipt(build, record, "f" * 64)

    def test_each_identity_mismatch_refuses_evidence(self):
        build, record = self.evidence()
        for key in ("receipt_sha256", "archive_sha256", "manifest_digest", "config_digest",
                    "platform", "architecture", "source_sha256", "binary_sha256",
                    "native_sha256", "runtime_sha256"):
            with self.subTest(key=key):
                bad = {**record, key: "different"}
                with self.assertRaises(self.smoke.SmokeError):
                    self.smoke.validate_smoke_receipt(build, bad, "f" * 64)

    def test_empty_missing_duplicate_extra_or_unpassed_checks_refuse(self):
        build, record = self.evidence()
        checks = record["checks"]
        variants = [[], checks[:-1], checks + checks[:1],
                    checks + [{"name": "invented", "status": "passed"}],
                    [{**checks[0], "status": "skipped"}] + checks[1:]]
        for value in variants:
            with self.subTest(value=value):
                with self.assertRaises(self.smoke.SmokeError):
                    self.smoke.validate_smoke_receipt(build, {**record, "checks": value}, "f" * 64)

    def test_extra_receipt_fields_cannot_publish_raw_diagnostics(self):
        build, record = self.evidence()
        with self.assertRaises(self.smoke.SmokeError):
            self.smoke.validate_smoke_receipt(build, {**record, "stderr": "private-value"}, "f" * 64)

    def test_status_and_boolean_schema_cannot_claim_success(self):
        build, record = self.evidence()
        for update in ({"schema": True}, {"schema": 2}, {"status": "failed"}):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.validate_smoke_receipt(build, {**record, **update}, "f" * 64)

    def test_source_changes_and_symlinks_refuse_before_invocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            file = root / "src/main.rs"
            file.write_bytes(b"source")
            files = {"src/main.rs": hashlib.sha256(b"source").hexdigest()}
            self.smoke.verify_source(root, files)
            file.write_bytes(b"changed")
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.verify_source(root, files)
            file.unlink()
            (root / "outside").write_bytes(b"source")
            file.symlink_to(root / "outside")
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.verify_source(root, files)

    def test_source_inventory_empty_or_escape_refuses(self):
        for files in ({}, {"../outside": "a" * 64}, {"/etc/passwd": "a" * 64}):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.verify_source(Path.cwd(), files)

    def test_bounded_command_hides_raw_failure_and_enforces_timeout(self):
        with self.assertRaisesRegex(self.smoke.SmokeError, "command failed") as failure:
            self.smoke.execute([sys.executable, "-c", "import sys; print('private-canary'); sys.exit(2)"])
        self.assertNotIn("private-canary", str(failure.exception))
        with self.assertRaisesRegex(self.smoke.SmokeError, "deadline"):
            self.smoke.execute([sys.executable, "-c", "import time; time.sleep(10)"], timeout=0.1)

    def test_bounded_command_refuses_oversized_output(self):
        with self.assertRaisesRegex(self.smoke.SmokeError, "output limit"):
            self.smoke.execute([sys.executable, "-c", "print('x' * 10000)"], limit=1000)

    def test_fixture_identity_cannot_be_a_mutable_tag_or_wrong_version(self):
        build, record = self.evidence()
        for update in ({"fixture_image": "latest"}, {"fixture_version": "14.24"},
                       {"fixture_version_num": 140024}, {"probe_sha256": ""}):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.validate_smoke_receipt(build, {**record, **update}, "f" * 64)

    def test_optimized_python_refuses_before_any_engine_command(self):
        result = subprocess.run([sys.executable, "-O", str(SCRIPT), "--help"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("assertions", result.stderr)

    def test_cleanup_identity_requires_exact_created_id_and_label(self):
        cid = "1" * 64
        info = {"Id": cid, "Config": {"Labels": {"rpgmigrate.image-smoke": "token"}}}
        self.smoke.verify_container(info, cid, "token")
        for invalid in ({**info, "Id": "2" * 64},
                        {**info, "Config": {"Labels": {}}}):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.verify_container(invalid, cid, "token")

    def test_private_tcp_fixture_requires_explicit_boolean_opt_in(self):
        sys.path.insert(0, str(SCRIPT.parents[2] / "tests/support"))
        import db_harness
        self.assertIn("tcp", inspect.signature(db_harness.OwnedCluster).parameters,
                      "private TCP fixture opt-in is absent")
        with patch.object(db_harness, "find_bindir", return_value=Path("/fixture/bin")):
            self.assertFalse(getattr(db_harness.OwnedCluster({}), "tcp", False))
            enabled = db_harness.OwnedCluster({}, tcp=True)
            self.assertTrue(enabled.tcp)
            for invalid in (1, "true", None):
                with self.assertRaises(ValueError):
                    db_harness.OwnedCluster({}, tcp=invalid)

    def test_runtime_config_refuses_root_wrapper_and_wrong_tmpdir(self):
        self.assertTrue(hasattr(self.smoke, "validate_runtime_config"), "runtime configuration guard is absent")
        valid = {"User": "1000:1000", "WorkingDir": "/work",
                 "Entrypoint": ["/usr/local/bin/rpgmigrate"], "Cmd": ["--help"],
                 "StopSignal": "SIGTERM", "Env": ["HOME=/home/rpgmigrate", "TMPDIR=/work"]}
        self.smoke.validate_runtime_config(valid)
        for change in ({"User": "0"}, {"Entrypoint": ["/bin/sh", "-c", "rpgmigrate"]},
                       {"WorkingDir": "/tmp"}, {"Env": ["TMPDIR=/tmp"]}):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.validate_runtime_config({**valid, **change})

    def test_refusal_requires_safe_structured_diagnostic_and_nonzero_exit(self):
        self.assertTrue(hasattr(self.smoke, "refusal"), "application refusal guard is absent")
        diagnostic = {"level": "error", "category": "BadState", "exit_code": 3, "message": "state-busy: locked"}
        valid = subprocess.CompletedProcess([], 3, "", json.dumps(diagnostic))
        self.smoke.refusal(valid, category="BadState", contains="state-busy")
        for result in (subprocess.CompletedProcess([], 0, "", ""),
                       subprocess.CompletedProcess([], 137, "", ""),
                       subprocess.CompletedProcess([], 3, "", "raw diagnostic"),
                       subprocess.CompletedProcess([], 3, "private-canary", json.dumps(diagnostic))):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.refusal(result, category="BadState", contains="state-busy")

    def test_native_platform_refuses_before_loading_or_creating_resources(self):
        self.assertTrue(hasattr(self.smoke, "check_engine"), "native engine preflight is absent")
        outputs = iter([subprocess.CompletedProcess([], 0, "5.7.0", ""),
                        subprocess.CompletedProcess([], 0, "amd64", "")])
        calls = []
        def command(argv, **_):
            calls.append(argv)
            return next(outputs)
        with patch.object(self.smoke, "execute", side_effect=command):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.check_engine("podman", "linux/arm64")
        self.assertEqual([item[1] for item in calls], ["version", "info"])

    def test_copy_gate_holds_observed_data_until_release(self):
        self.assertTrue(hasattr(self.smoke, "HoldCopySocket"), "COPY data gate is absent")
        with tempfile.TemporaryDirectory() as directory:
            left, right = socket.socketpair()
            release = threading.Event()
            held = Path(directory) / "held.json"
            wrapper = self.smoke.HoldCopySocket(left, held, release)
            frame = b"d\x00\x00\x00\x07abc"
            thread = threading.Thread(target=wrapper.sendall, args=(frame,))
            try:
                thread.start()
                for _ in range(100):
                    if held.exists():
                        break
                    threading.Event().wait(0.01)
                self.assertEqual(json.loads(held.read_text()), {"copy_bytes": 3})
                right.setblocking(False)
                with self.assertRaises(BlockingIOError):
                    right.recv(1)
                release.set()
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
                right.settimeout(1)
                self.assertEqual(right.recv(len(frame)), frame)
            finally:
                release.set()
                thread.join(timeout=2)
                left.close()
                right.close()

    def test_tcp_startup_keeps_default_isolation_and_requires_scram(self):
        sys.path.insert(0, str(SCRIPT.parents[2] / "tests/support"))
        import db_harness
        class StopBeforeServer(Exception):
            pass
        for tcp in (False, True):
            captured = {}
            def external_run(argv, **_):
                if "--version" in argv:
                    return subprocess.CompletedProcess(argv, 0, "PostgreSQL 17.11", "")
                data = Path(argv[argv.index("-D") + 1])
                data.mkdir(mode=0o700)
                (data / "pg_hba.conf").write_text("local all all trust\nhost all all 0.0.0.0/0 reject\n")
                return subprocess.CompletedProcess(argv, 0, "", "")
            def launch(argv, **_):
                captured["argv"] = argv
                data = Path(argv[argv.index("-D") + 1])
                captured["hba"] = (data / "pg_hba.conf").read_text()
                raise StopBeforeServer
            with patch.object(db_harness, "find_bindir", return_value=Path("/fixture/bin")), \
                 patch.object(db_harness.subprocess, "run", side_effect=external_run), \
                 patch.object(db_harness.subprocess, "Popen", side_effect=launch):
                with self.assertRaises(StopBeforeServer):
                    db_harness.OwnedCluster({"server_version": "17.11"}, tcp=tcp).__enter__()
            self.assertIn("listen_addresses=" + ("0.0.0.0" if tcp else ""), captured["argv"])
            self.assertNotEqual(captured["argv"][captured["argv"].index("-p") + 1], "5432")
            self.assertEqual("host all e1_source,e1_target 0.0.0.0/0 scram-sha-256\n" in captured["hba"], tcp)
            self.assertTrue(captured["hba"].endswith("host all all 0.0.0.0/0 reject\n"))

    def test_help_uses_stderr_and_ping_requires_actual_success_document(self):
        self.assertTrue(hasattr(self.smoke, "validate_cli_result"), "CLI output verification is absent")
        self.smoke.validate_cli_result(subprocess.CompletedProcess([], 0, "", "rpgmigrate help\n"), "help")
        self.smoke.validate_cli_result(subprocess.CompletedProcess([], 0, "rpgmigrate version\n", ""), "version")
        self.smoke.validate_cli_result(subprocess.CompletedProcess([], 0, '{"command":"ping","status":"complete"}', ""), "ping")
        for mode in ("help", "version", "ping"):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.validate_cli_result(subprocess.CompletedProcess([], 0, "", ""), mode)

    def test_runtime_packages_keep_binary_architecture_and_exact_versions(self):
        from types import SimpleNamespace
        image = "sha256:" + "1" * 64
        packages = ["ca-certificates=20250419", "libc6:arm64=2.41-12+deb13u1"]
        build = {"runtime": {"files": {"/usr/local/bin/rpgmigrate": {"sha256": "a" * 64}}},
                 "packages": packages}
        observed = {"packages": packages}
        queries = []
        def run(actual_image, args, *, entrypoint):
            self.assertEqual(actual_image, image)
            if entrypoint == "/bin/sh":
                output = "a" * 64 + "  /usr/local/bin/rpgmigrate\n"
            else:
                self.assertEqual(entrypoint, "/usr/bin/dpkg-query")
                queries.append(args)
                if args == ["-W", "-f=${Package}=${Version}\n"]:
                    output = "ca-certificates=20250419\nlibc6=2.41-12+deb13u1\n"
                else:
                    self.assertEqual(args, ["-W", "-f=${binary:Package}=${Version}\n"])
                    output = "\n".join(observed["packages"]) + "\n"
            return subprocess.CompletedProcess([], 0, output, "")
        containers = SimpleNamespace(run=run)
        try:
            self.smoke.runtime_files(containers, image, build)
        except self.smoke.SmokeError as error:
            self.fail("matching architecture-qualified package inventory was rejected: " + str(error))
        for invalid in (packages[:1], packages + ["extra:arm64=1.0"],
                        [packages[0], "libc6=2.41-12+deb13u1"],
                        [packages[0], "libc6:amd64=2.41-12+deb13u1"],
                        [packages[0], "libc6:arm64=2.41-12+deb13u2"]):
            with self.subTest(packages=invalid):
                observed["packages"] = invalid
                with self.assertRaises(self.smoke.SmokeError):
                    self.smoke.runtime_files(containers, image, build)
        self.assertTrue(queries)
        self.assertTrue(all(args == ["-W", "-f=${binary:Package}=${Version}\n"] for args in queries))

    def test_container_create_uses_exact_image_and_own_network_volume(self):
        engine = self.smoke.Containers("podman")
        engine.network, engine.volume = "owned-network", "owned-volume"
        seen = []
        identity = "3" * 64
        def command(*args, **_):
            seen.append(args)
            if args[0] == "create":
                return subprocess.CompletedProcess(args, 0, identity, "")
            return subprocess.CompletedProcess(args, 0, "", "")
        with patch.object(engine, "command", side_effect=command), patch.object(engine, "owned", return_value={}):
            self.assertEqual(engine.create("sha256:" + "4" * 64, ["ping", "--source", "one argument with spaces"]), identity)
        args = seen[0]
        self.assertIn("--read-only", args)
        self.assertIn("--pull=never", args)
        self.assertEqual(args[args.index("--network") + 1], "owned-network")
        self.assertEqual(args[args.index("--mount") + 1], "type=volume,source=owned-volume,target=/work")
        self.assertEqual(args[-4:], ("sha256:" + "4" * 64, "ping", "--source", "one argument with spaces"))
        self.assertNotIn("--privileged", args)
        self.assertNotIn("--publish", args)

    def test_unowned_container_is_never_signalled_or_removed(self):
        engine = self.smoke.Containers("podman")
        with patch.object(engine, "command", side_effect=AssertionError("unexpected engine mutation")):
            with self.assertRaises(self.smoke.SmokeError):
                engine.remove("1" * 64)
            with self.assertRaises(self.smoke.SmokeError):
                engine.kill("1" * 64, "SIGTERM")

    def test_receipt_rejects_malformed_check_names(self):
        build, record = self.evidence()
        for value in (None, [], 1, {"nested": "value"}):
            broken = [dict(item) for item in record["checks"]]
            broken[0]["name"] = value
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.validate_smoke_receipt(build, {**record, "checks": broken}, "f" * 64)

    def test_image_identity_accepts_only_documented_digest_formats(self):
        self.assertTrue(hasattr(self.smoke, "image_identity"), "immutable image normalization is absent")
        for value in ("1" * 64, "sha256:" + "1" * 64):
            self.assertEqual(self.smoke.image_identity(value), "sha256:" + "1" * 64)
        for value in ("latest", "sha256:short", "prefix" + "1" * 64, "1" * 64 + "\n"):
            with self.assertRaises(self.smoke.SmokeError):
                self.smoke.image_identity(value)

    def test_fixture_evidence_is_private_and_never_overwrites_existing_state(self):
        self.assertTrue(hasattr(self.smoke, "write_new_json"), "private fixture publication is absent")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ready.json"
            self.smoke.write_new_json(path, {"port": 23456})
            self.assertEqual(json.loads(path.read_text()), {"port": 23456})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                self.smoke.write_new_json(path, {"port": 5432})
            self.assertEqual(json.loads(path.read_text()), {"port": 23456})

    def receipt_result(self, qualification, *, cleanup_failure=False):
        sys.path.insert(0, str(SCRIPT.parent))
        import runtime
        from types import SimpleNamespace
        build, _ = self.evidence()
        config = {"User": "1000:1000", "WorkingDir": "/work", "Entrypoint": ["/usr/local/bin/rpgmigrate"],
                  "Cmd": ["--help"], "StopSignal": "SIGTERM", "Env": ["HOME=/home/rpgmigrate", "TMPDIR=/work"]}
        class Engine:
            def __init__(self, _):
                self.resources = []
                self.cleanup_calls = 0
            def command(self, *_args, **_options):
                return subprocess.CompletedProcess([], 0, "", "")
            def inspect(self, _kind, identity):
                return {"Id": identity, "Architecture": "arm64", "Os": "linux", "Config": config}
            def setup(self):
                pass
            def owned(self, _):
                return {"State": {"Running": True}}
            def kill(self, *_):
                pass
            def finish(self, *_, **_options):
                return subprocess.CompletedProcess([], 0, "", "")
            def remove(self, *_):
                pass
            def close(self):
                self.cleanup_calls += 1
                if cleanup_failure and self.cleanup_calls == 2:
                    raise TypeError("synthetic cleanup failure")
        def qualify(_containers, _image, _fixture, _build, record):
            if qualification == "exception":
                raise self.smoke.SmokeError("synthetic qualification failure")
            if qualification == "complete":
                for name in self.smoke.SMOKE_CHECKS:
                    if name not in ("archive-identity", "native-platform", "loaded-image-identity", "cleanup-owned"):
                        record(name)
            return "2" * 64
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "source").write_bytes(b"source")
            (root / "image.oci.tar").write_bytes(b"archive")
            build["source"]["files"] = {"source": hashlib.sha256(b"source").hexdigest()}
            build["archive"]["sha256"] = hashlib.sha256(b"archive").hexdigest()
            receipt = root / "receipt.json"
            receipt.write_text(json.dumps(build))
            with patch.object(self.smoke, "ROOT", root), patch.object(runtime, "load_receipt", return_value=build), \
                 patch.object(self.smoke, "check_engine"), patch.object(self.smoke, "Containers", Engine), \
                 patch.object(self.smoke, "Fixture", return_value=SimpleNamespace(identity="fixture")), \
                 patch.object(self.smoke, "qualify", side_effect=qualify), redirect_stdout(io.StringIO()):
                if qualification == "complete" and not cleanup_failure:
                    self.smoke.smoke("podman", receipt, "sha256:" + "1" * 64, root / "output")
                else:
                    with self.assertRaises(TypeError if cleanup_failure else self.smoke.SmokeError):
                        self.smoke.smoke("podman", receipt, "sha256:" + "1" * 64, root / "output")
            return json.loads((root / "output/smoke.json").read_text())

    def test_missing_live_checks_leave_failed_receipt_even_after_cleanup(self):
        self.assertEqual(self.receipt_result("missing")["status"], "failed")

    def test_qualification_exception_leaves_failed_receipt(self):
        self.assertEqual(self.receipt_result("exception")["status"], "failed")

    def test_final_cleanup_exception_cannot_publish_pass(self):
        self.assertEqual(self.receipt_result("complete", cleanup_failure=True)["status"], "failed")

    def test_complete_inventory_and_cleanup_publish_pass(self):
        self.assertEqual(self.receipt_result("complete")["status"], "passed")

    def test_partial_receipt_write_never_publishes_a_final_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "smoke.json"
            def partial_write(_value, stream):
                stream.write('{"status":"passed"')
                raise OSError("synthetic short write")
            with patch.object(self.smoke.json, "dump", side_effect=partial_write):
                with self.assertRaises(OSError):
                    self.smoke.write_new_json(path, {"status": "passed"})
            self.assertFalse(path.exists())
            self.assertEqual(list(path.parent.iterdir()), [])

    def network_fixture(self, engine_name):
        engine = self.smoke.Containers(engine_name)
        name = "rpgmigrate-smoke-" + engine.token
        identity = "5" * 64
        info = ({"id": identity, "name": name, "labels": {self.smoke.LABEL: engine.token}}
                if engine_name == "podman" else
                {"Id": identity, "Name": name, "Labels": {self.smoke.LABEL: engine.token}})
        state = {"info": info, "create_output": name if engine_name == "podman" else identity,
                 "create_error": None, "inspect_error": None, "remove_error": None}
        calls = []
        def command(*args, **_):
            calls.append(args)
            kind, operation = args[:2]
            if kind == "network":
                error = state.get({"create": "create_error", "inspect": "inspect_error", "rm": "remove_error"}[operation])
                if error is not None:
                    raise error
                output = state["create_output"] if operation == "create" else json.dumps([state["info"]]) if operation == "inspect" else ""
            elif kind == "volume":
                output = json.dumps([{"Name": name, "Labels": {self.smoke.LABEL: engine.token}}]) if operation == "inspect" else name
            else:
                raise AssertionError("unexpected engine operation")
            return subprocess.CompletedProcess(args, 0, output, "")
        engine.command = command
        return engine, state, calls, identity

    def test_podman_network_name_and_lowercase_inspect_round_trip(self):
        engine, _state, calls, identity = self.network_fixture("podman")
        engine.setup()
        self.assertEqual(engine.network, identity)
        engine.close()
        self.assertIn(("network", "rm", identity), calls)
        self.assertEqual(engine.resources, [])

    def test_docker_network_hex_and_uppercase_inspect_round_trip(self):
        engine, _state, calls, identity = self.network_fixture("docker")
        engine.setup()
        self.assertEqual(engine.network, identity)
        engine.close()
        self.assertIn(("network", "rm", identity), calls)
        self.assertEqual(engine.resources, [])

    def test_malformed_create_output_keeps_provable_cleanup_intent(self):
        for engine_name in ("podman", "docker"):
            with self.subTest(engine=engine_name):
                engine, state, calls, identity = self.network_fixture(engine_name)
                state["create_output"] = "malformed-output"
                with self.assertRaises(self.smoke.SmokeError):
                    engine.setup()
                self.assertTrue(engine.resources)
                engine.close()
                self.assertIn(("network", "rm", identity), calls)
                self.assertNotIn(("network", "inspect", "malformed-output"), calls)

    def test_failed_create_cleans_only_proven_owned_network(self):
        for owned in (True, False):
            with self.subTest(owned=owned):
                engine, state, calls, identity = self.network_fixture("podman")
                state["create_error"] = self.smoke.SmokeError("network create child failed")
                if not owned:
                    state["info"]["labels"] = {self.smoke.LABEL: "foreign-token"}
                with self.assertRaises(self.smoke.SmokeError):
                    engine.setup()
                self.assertTrue(engine.resources)
                if owned:
                    engine.close()
                    self.assertIn(("network", "rm", identity), calls)
                else:
                    with self.assertRaises(self.smoke.SmokeError):
                        engine.close()
                    self.assertFalse(any(call[:2] == ("network", "rm") for call in calls))

    def test_setup_inspect_failure_retains_intent_for_later_owned_cleanup(self):
        for engine_name in ("podman", "docker"):
            with self.subTest(engine=engine_name):
                engine, state, calls, identity = self.network_fixture(engine_name)
                state["inspect_error"] = self.smoke.SmokeError("network inspect child failed")
                with self.assertRaises(self.smoke.SmokeError):
                    engine.setup()
                self.assertTrue(engine.resources)
                state["inspect_error"] = None
                engine.close()
                self.assertIn(("network", "rm", identity), calls)

    def test_malformed_or_foreign_network_inspect_never_authorizes_removal(self):
        for engine_name in ("podman", "docker"):
            keys = ("id", "name", "labels") if engine_name == "podman" else ("Id", "Name", "Labels")
            for key, value in ((keys[0], "short"), (keys[0], "6" * 64), (keys[1], "foreign-name"),
                               (keys[2], {}), (keys[2], None), (keys[2], {self.smoke.LABEL: "foreign-token"})):
                with self.subTest(engine=engine_name, key=key, value=value):
                    engine, state, calls, _identity = self.network_fixture(engine_name)
                    engine.setup()
                    state["info"][key] = value
                    with self.assertRaises(self.smoke.SmokeError):
                        engine.close()
                    self.assertFalse(any(call[:2] == ("network", "rm") for call in calls))

    def test_cleanup_inspect_failure_does_not_remove_an_unproven_network(self):
        engine, state, calls, identity = self.network_fixture("podman")
        engine.setup()
        state["inspect_error"] = self.smoke.SmokeError("network inspect child failed")
        with self.assertRaises(self.smoke.SmokeError):
            engine.close()
        self.assertFalse(any(call[:2] == ("network", "rm") for call in calls))
        state["inspect_error"] = None
        engine.close()
        self.assertIn(("network", "rm", identity), calls)

    def test_cleanup_child_failure_retains_network_and_never_falls_back_to_name(self):
        engine, state, calls, identity = self.network_fixture("podman")
        engine.setup()
        state["remove_error"] = self.smoke.SmokeError("network remove child failed")
        with self.assertRaises(self.smoke.SmokeError):
            engine.close()
        self.assertTrue(engine.resources)
        self.assertEqual([call for call in calls if call[:2] == ("network", "rm")], [("network", "rm", identity)])
        state["remove_error"] = None
        engine.close()
        self.assertEqual(engine.resources, [])

    def test_other_child_cleanup_failure_still_checks_and_removes_owned_network(self):
        engine, _state, calls, identity = self.network_fixture("podman")
        engine.setup()
        engine.containers.append("7" * 64)
        with patch.object(engine, "remove", side_effect=self.smoke.SmokeError("container cleanup child failed")):
            with self.assertRaises(self.smoke.SmokeError):
                engine.close()
        self.assertIn(("network", "rm", identity), calls)


if __name__ == "__main__":
    unittest.main()
