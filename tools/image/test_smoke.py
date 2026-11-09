#!/usr/bin/env python3
"""Offline refusal controls for the production image smoke."""

import copy
import ast
import contextlib
import importlib.util
import json
import hashlib
import io
import os
import stat
import shlex
import sys
import tempfile
import types
from pathlib import Path
import subprocess
import unittest
from unittest.mock import Mock, patch


PATH = Path(__file__).with_name("smoke.py")
QUALIFIED_CAPABILITIES = [
    "clone.single_db", "cdc.pgoutput", "progress.v1", "cutover.v1", "verify.v1", "cleanup.v1", "verify.sealed_files.v1",
]


class SmokeTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(PATH.is_file(), "the smoke helper must exist")
        spec = importlib.util.spec_from_file_location("image_smoke", PATH)
        self.smoke = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.smoke)
        self.metadata = [{"Id": "sha256:" + "a" * 64, "Architecture": "arm64", "Os": "linux",
                          "Config": {"User": "65532:65532", "WorkingDir": "/work",
                                     "Entrypoint": None, "Cmd": ["rpgmigrate", "--help"]}}]
        self.observed = {
            "uid": "65532", "gid": "65532", "machine": "aarch64",
            "binary_sha256": "1" * 64 + "  /usr/local/bin/rpgmigrate",
            "version": json.dumps({"protocol_version": 1, "engine_version": "0.1.0", "capabilities": QUALIFIED_CAPABILITIES}),
            "plain_version": "rpgmigrate 0.1.0\nlibpq 180001",
            "ldd": "libpq.so.5 => /lib/aarch64-linux-gnu/libpq.so.5 (0xffff)",
            "coreutils": "id (GNU coreutils) 9.7", "sed": "sed (GNU sed) 4.9",
            "default_psql": "psql (PostgreSQL) 18.1 (Debian 18.1-1.pgdg13+2)",
            "mountinfo": "25 20 0:1 / / ro,relatime - overlay overlay ro\n26 25 0:2 / /work rw - tmpfs tmpfs rw\n27 25 0:3 / /tmp rw - tmpfs tmpfs rw",
            "work_write": "ok", "tmp_write": "ok", "root_write": "denied",
            "work_owner": "65532:65532:700", "tmp_owner": "65532:65532:700",
            "inputs": "b" * 64 + "  /usr/share/rpgmigrate/build-inputs.sha256",
            "notice": "present",
        }
        for major in (16, 18):
            for tool in ("pg_dump", "pg_restore", "psql"):
                self.observed[f"{tool}_{major}"] = f"{tool} (PostgreSQL) {major}.1 (Debian {major}.1-1.pgdg13+2)"
        self.package = {"path": "/usr/lib/aarch64-linux-gnu/libpq.so.5.18",
                        "owner": "libpq5:arm64: /usr/lib/aarch64-linux-gnu/libpq.so.5.18",
                        "package": "libpq5\t18.1-1.pgdg13+2\tarm64\tinstall ok installed"}
        manifest = self.smoke.source_manifest()
        self.observed["inputs_manifest"] = manifest.rstrip("\n")
        self.observed["inputs"] = hashlib.sha256(manifest.encode()).hexdigest() + "  /usr/share/rpgmigrate/build-inputs.sha256"
        runtime = self.runtime_metadata()
        names = ("python3", "python3-minimal", "python3.13", "python3.13-minimal", "libpython3.13-stdlib",
                 "libpython3.13-minimal", "libffi8", "libc6", "libpq5", "libssl3t64",
                 "postgresql-client-16", "postgresql-client-18")
        packages = {name: {"version": "1", "architecture": "arm64", "source": name, "source_version": "1",
                          "copyright_path": "/usr/share/doc/" + name + "/copyright", "copyright_sha256": "d" * 64}
                    for name in names}
        packages["libpq5"]["version"] = runtime["package_version"]
        self.python = {"version": "3.13.5", "isolated": True,
                       "modules": ["ctypes", "_ctypes", "os", "stat", "fcntl", "selectors", "select", "time",
                                   "json", "hashlib", "struct", "errno", "sys", "binascii", "resource"],
                       "symbols": ['PQlibVersion', 'PQconnectStart', 'PQconnectPoll', 'PQsetnonblocking', 'PQsocket',
                                   'PQstatus', 'PQtransactionStatus', 'PQbackendPID', 'PQsetNoticeReceiver',
                                   'PQresultErrorField', 'PQsendQueryParams', 'PQflush', 'PQconsumeInput', 'PQisBusy',
                                   'PQgetResult', 'PQresultStatus', 'PQcmdStatus', 'PQcmdTuples', 'PQntuples',
                                   'PQnfields', 'PQgetisnull', 'PQgetlength', 'PQgetvalue', 'PQclear', 'PQfinish',
                                   'PQescapeIdentifier', 'PQfreemem'],
                       "metadata": runtime, "metadata_sha256": "e" * 64,
                       "mapped_library": runtime["library_path"], "ctypes": True, "private_input": True,
                       "packages": packages, "python_license": {"path": "/usr/share/doc/libpython3.13-stdlib/copyright",
                                                              "sha256": "d" * 64},
                       "dependency_notices": {"unicode-ident-1.0.27/LICENSE-UNICODE": "f" * 64},
                       "notice_sha256": hashlib.sha256((PATH.parents[2] / "NOTICE").read_bytes()).hexdigest()}
        self.observed["python"] = json.dumps(self.python)
        self.calls = []
        self.fail_at = None

    def invoke(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if len(self.calls) == self.fail_at:
            raise subprocess.CalledProcessError(1, argv, output="private output")
        if "inspect" in argv:
            output = json.dumps(self.metadata)
        else:
            records = self.package if len(self.calls) == 3 else self.observed
            output = "".join(f"===rpgmigrate-smoke:{name}===\n{value}\n" for name, value in records.items())
        return subprocess.CompletedProcess(argv, 0, output, "")

    def probe(self):
        with patch.object(self.smoke.subprocess, "run", side_effect=self.invoke):
            return self.smoke.probe("localhost/test:dev", "podman")


    def test_python_observation_is_required(self):
        del self.observed["python"]
        with self.assertRaises(RuntimeError):
            self.probe()

    def runtime_namespace(self):
        self.assertTrue(hasattr(self.smoke, "PYTHON_RUNTIME"), "runtime probe must validate real metadata")
        namespace = {"__name__": "test_runtime"}
        exec(compile(self.smoke.PYTHON_RUNTIME, "image-python-probe", "exec"), namespace)
        return namespace

    def runtime_metadata(self):
        return {"version": 1, "architecture": "arm64",
                "library_path": "/usr/lib/aarch64-linux-gnu/libpq.so.5.18",
                "library_sha256": "c" * 64, "libpq_version": 180001,
                "package_name": "libpq5", "package_version": "18.1-1.pgdg13+2"}

    def test_runtime_metadata_refuses_malformed_json_and_types(self):
        parse = self.runtime_namespace()["parse_metadata"]
        valid = self.runtime_metadata()
        self.assertEqual(parse(json.dumps(valid).encode()), valid)
        changes = {"version": [True, 1.0, "1", 2], "architecture": ["aarch64", "s390x"],
                   "library_path": ["relative", "/usr/lib/../lib/libpq.so.5", "/lib//libpq.so.5"],
                   "library_sha256": ["C" * 64, "c" * 63], "libpq_version": [True, 180001.0, 170001, 190000],
                   "package_name": ["libpq5:arm64", "foreign"],
                   "package_version": ["", "a" * 257, "18.1 x", "18.1\n", "é"]}
        for key, values in changes.items():
            for value in values:
                with self.subTest(key=key, value=value), self.assertRaises(RuntimeError):
                    parse(json.dumps({**valid, key: value}).encode())
        encoded = json.dumps(valid).encode()
        bad = [b"", encoded + b"{}", b" " * 4097,
               encoded.replace(b'"version": 1,', b'"version": 1,"version": 1,'),
               encoded.replace(b'"version": 1,', b'"version": 1e0,'),
               json.dumps({**valid, "extra": 1}).encode()]
        bad += [json.dumps({k: v for k, v in valid.items() if k != key}).encode() for key in valid]
        for value in bad:
            with self.subTest(value=value[:100]), self.assertRaises(RuntimeError):
                parse(value)

    def test_checked_files_reject_symlinks_writes_and_wrong_owners(self):
        namespace = self.runtime_namespace()
        check = namespace["read_owned"]
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "metadata"
            file.write_bytes(b"facts")
            file.chmod(0o444)
            # Host test directories need not belong to image root.
            with patch.dict(namespace, {"check_ancestors": lambda path: None}):
                real_fstat = os.fstat
                def root_stat(descriptor):
                    values = list(real_fstat(descriptor))
                    values[4:6] = [0, 0]
                    return os.stat_result(values)
                with patch.object(namespace["os"], "fstat", side_effect=root_stat):
                    self.assertEqual(check(str(file), 4096, exact_mode=0o444), b"facts")
                    with self.assertRaises(RuntimeError):
                        check(str(file), 4, exact_mode=0o444)
                if os.getuid() != 0:
                    with self.assertRaises(RuntimeError):
                        check(str(file), 4096, exact_mode=0o444)
                link = Path(directory) / "link"
                link.symlink_to(file)
                with self.assertRaises((RuntimeError, OSError)):
                    check(str(link), 4096, exact_mode=0o444)
                file.chmod(0o666)
                with self.assertRaises(RuntimeError):
                    check(str(file), 4096, exact_mode=0o444)
                with self.assertRaises(RuntimeError):
                    check(directory, 4096, exact_mode=0o444)


    def test_runtime_loads_only_hashed_absolute_package_library_with_explicit_abi(self):
        namespace = self.runtime_namespace()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "libpq.so.5.18"
            path.write_bytes(b"actual library bytes")
            metadata = {**self.runtime_metadata(), "library_path": str(path.resolve()),
                        "library_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            library = Mock()
            library.PQlibVersion.return_value = 180001
            info = path.stat()
            maps = "0000-1000 r-xp 00000000 %02x:%02x %d %s\n" % (
                os.major(info.st_dev), os.minor(info.st_dev), info.st_ino, path.resolve())
            bindings = {"package_for": lambda path: "libpq5", "read_owned": lambda path, limit: Path(path).read_bytes(),
                        "open": lambda *args, **kwargs: io.StringIO(maps)}
            with patch.dict(namespace, bindings):
                with patch.object(namespace["ctypes"], "CDLL", return_value=library) as load:
                    namespace["verify_library"](metadata)
                    load.assert_called_once_with(str(path.resolve()))
                    self.assertEqual(library.PQlibVersion.argtypes, [])
                    self.assertIs(library.PQlibVersion.restype, namespace["ctypes"].c_int)
                with patch.object(namespace["ctypes"], "CDLL", side_effect=OSError("private loader output")):
                    with self.assertRaises(OSError):
                        namespace["verify_library"](metadata)
                for version in (170001, 180002):
                    library.PQlibVersion.return_value = version
                    with patch.object(namespace["ctypes"], "CDLL", return_value=library), self.assertRaises(RuntimeError):
                        namespace["verify_library"](metadata)
                library.PQlibVersion.return_value = 180001
                with patch.object(namespace["ctypes"], "CDLL", return_value=object()), self.assertRaises(AttributeError):
                    namespace["verify_library"](metadata)
                with patch.object(namespace["ctypes"], "CDLL") as load, self.assertRaises(RuntimeError):
                    namespace["verify_library"]({**metadata, "library_sha256": "0" * 64})
                load.assert_not_called()
                with patch.dict(namespace, {"package_for": lambda path: "foreign"}), self.assertRaises(RuntimeError):
                    namespace["verify_library"](metadata)
                with patch.dict(namespace, {"open": lambda *args, **kwargs: io.StringIO(maps.replace("\n", " (deleted)\n"))}), \
                     patch.object(namespace["ctypes"], "CDLL", return_value=library), self.assertRaises(RuntimeError):
                    namespace["verify_library"](metadata)

    def test_runtime_main_emits_the_verified_library_identity(self):
        namespace = self.runtime_namespace()
        metadata = self.runtime_metadata()
        rows = "\n".join("\t".join((name, item["version"], item["architecture"], "installed",
                                  item["source"], item["source_version"]))
                         for name, item in self.python["packages"].items())
        system = types.SimpleNamespace(version_info=types.SimpleNamespace(major=3, minor=13),
                                       flags=types.SimpleNamespace(isolated=True), dont_write_bytecode=True,
                                       version="3.13.5 test", stdin=types.SimpleNamespace(buffer=io.BytesIO(b"rpgmigrate-private-input-canary")))
        bindings = {"sys": system, "verify_library": lambda value: self.assertEqual(value, metadata),
                    "read_owned": lambda path, limit, **kwargs: json.dumps(metadata).encode() if path == namespace["METADATA"] else b"notice",
                    "command": lambda *args: rows, "package_for": lambda path: "libpython3.13-stdlib",
                    "hashes": lambda path: self.python["dependency_notices"],
                    "open": lambda *args, **kwargs: io.BytesIO(b"/usr/bin/python3\0-I\0-B\0/private.py")}
        output = io.StringIO()
        with patch.dict(namespace, bindings), patch.object(namespace["os"], "uname", return_value=types.SimpleNamespace(machine="aarch64")), \
             contextlib.redirect_stdout(output):
            namespace["main"]()
        observed = json.loads(output.getvalue())
        self.assertEqual(observed["mapped_library"], metadata["library_path"])
        self.assertTrue(observed["private_input"])
        self.assertNotIn("rpgmigrate-private-input-canary", output.getvalue())

    def build_generator(self):
        dockerfile = (PATH.parents[2] / "Dockerfile").read_text()
        instructions = [line for line in dockerfile.replace("\\\n", "").splitlines()
                        if line.strip() and not line.lstrip().startswith("#")]
        supported = {"FROM", "RUN", "WORKDIR", "COPY", "ENV", "USER", "CMD"}
        self.assertTrue(all(line.split()[0] in supported for line in instructions),
                        "generator must remain inside one continued Dockerfile RUN for older Buildah")
        commands = [line[4:] for line in instructions if line.startswith("RUN ") and "/usr/bin/python3 -I -B" in line]
        self.assertEqual(len(commands), 1)
        command = commands[0]
        subprocess.run(["/bin/sh", "-n", "-c", command], check=True, capture_output=True)
        capture = shlex.join([sys.executable, "-I", "-B", "-c", "import sys; sys.stdout.write(sys.stdin.read())"])
        result = subprocess.run(["/bin/sh", "-c", command.replace("/usr/bin/python3 -I -B", capture)],
                                check=True, text=True, capture_output=True, timeout=30)
        ast.parse(result.stdout)
        return result.stdout

    def test_build_generator_delivery_preserves_reviewed_python_bytes(self):
        generator = self.build_generator()
        # Keep shell quoting changes from silently changing the reviewed metadata generator.
        self.assertEqual(hashlib.sha256(generator.encode()).hexdigest(),
                         "02bbc68bd71bbd5cb40448cedc835b835b3a5fd065b5912dd3f72d044b91622f")

    def test_build_metadata_uses_actual_installed_library_and_package(self):
        dockerfile = (PATH.parents[2] / "Dockerfile").read_text()
        self.assertIn("coreutils sed dash python3", dockerfile)
        generator = self.build_generator()
        with tempfile.TemporaryDirectory() as directory:
            library_path = Path(directory) / "libpq.so.5.18"
            library_path.write_bytes(b"installed package library")
            metadata_path = Path(directory) / "runtime.json"
            real_open = open
            def output_open(path, *args, **kwargs):
                if path == "/usr/share/rpgmigrate/python-libpq-runtime.json":
                    path = metadata_path
                return real_open(path, *args, **kwargs)
            def query(argv, **kwargs):
                if "-W" in argv:
                    value = "libpq5\t18.6-1.pgdg13+1\tarm64\tinstall ok installed"
                elif "-L" in argv:
                    value = str(library_path)
                elif "-S" in argv:
                    value = "libpq5:arm64: " + str(library_path.resolve())
                else:
                    self.fail("unexpected package query")
                return subprocess.CompletedProcess(argv, 0, value, "")
            library = Mock()
            library.PQlibVersion.return_value = 180006
            with patch("subprocess.run", side_effect=query), patch("ctypes.CDLL", return_value=library) as load, \
                 patch("os.uname", return_value=types.SimpleNamespace(machine="aarch64")), \
                 patch("os.chown") as chown, patch("os.chmod") as chmod:
                exec(compile(generator, "Dockerfile runtime generator", "exec"), {"open": output_open})
            metadata = self.runtime_namespace()["parse_metadata"](metadata_path.read_bytes())
            self.assertEqual(metadata["library_path"], str(library_path.resolve()))
            self.assertEqual(metadata["library_sha256"], hashlib.sha256(library_path.read_bytes()).hexdigest())
            self.assertEqual(metadata["package_version"], "18.6-1.pgdg13+1")
            self.assertEqual(metadata["libpq_version"], 180006)
            load.assert_called_once_with(str(library_path.resolve()))
            chown.assert_called_once_with("/usr/share/rpgmigrate/python-libpq-runtime.json", 0, 0)
            chmod.assert_called_once_with("/usr/share/rpgmigrate/python-libpq-runtime.json", 0o444)

    def test_runtime_ancestor_checks_refuse_symlinks_and_foreign_or_writable_parents(self):
        namespace = self.runtime_namespace()
        check = namespace["check_ancestors"]
        for mode, uid, gid in ((stat.S_IFLNK | 0o777, 0, 0), (stat.S_IFDIR | 0o755, 1, 0),
                               (stat.S_IFDIR | 0o755, 0, 1), (stat.S_IFDIR | 0o775, 0, 0),
                               (stat.S_IFDIR | 0o757, 0, 0)):
            observed = types.SimpleNamespace(st_mode=mode, st_uid=uid, st_gid=gid)
            with patch.object(namespace["os"], "lstat", return_value=observed), self.assertRaises(RuntimeError):
                check("/usr/share/rpgmigrate/python-libpq-runtime.json")
        owned = types.SimpleNamespace(st_mode=stat.S_IFDIR | 0o755, st_uid=0, st_gid=0)
        with patch.object(namespace["os"], "lstat", return_value=owned) as observe:
            check("/usr/share/rpgmigrate/python-libpq-runtime.json")
        self.assertEqual([call.args[0] for call in observe.call_args_list], ["/usr/share/rpgmigrate", "/usr/share", "/usr", "/"])

    def test_python_evidence_refuses_independent_binding_and_missing_capabilities(self):
        changes = [("version", "2.7.18"), ("isolated", False), ("ctypes", False), ("private_input", False),
                   ("modules", self.python["modules"][:-1]), ("symbols", self.python["symbols"][:-1]),
                   ("mapped_library", "/foreign/libpq.so.5"), ("metadata_sha256", ""),
                   ("packages", {}), ("python_license", {"path": "/usr/LICENSE", "sha256": ""}),
                   ("dependency_notices", {}), ("notice_sha256", "0" * 64)]
        for key, value in changes:
            with self.subTest(key=key):
                observed = {**self.python, key: value}
                with self.assertRaises(RuntimeError):
                    self.smoke.validate_python(observed, "arm64", self.package, 180001)
        for key, value in (("architecture", "amd64"), ("library_path", "/usr/lib/foreign/libpq.so.5.18"),
                           ("package_version", "18.2-1.pgdg13+2"), ("libpq_version", 180002)):
            with self.subTest(key=key):
                observed = {**self.python, "metadata": {**self.python["metadata"], key: value}}
                with self.assertRaises(RuntimeError):
                    self.smoke.validate_python(observed, "arm64", self.package, 180001)

    def test_source_manifest_binds_contents_and_not_only_digest(self):
        self.observed["inputs_manifest"] += "\n" + "0" * 64 + "  foreign"
        manifest = self.observed["inputs_manifest"] + "\n"
        self.observed["inputs"] = hashlib.sha256(manifest.encode()).hexdigest() + "  /usr/share/rpgmigrate/build-inputs.sha256"
        with self.assertRaisesRegex(RuntimeError, "source inputs mismatch"):
            self.probe()

    def test_duplicate_python_json_and_unexpected_probe_records_are_refused(self):
        self.observed["python"] = self.observed["python"].replace('"isolated": true,', '"isolated": false,"isolated": true,')
        with self.assertRaisesRegex(RuntimeError, "duplicate JSON"):
            self.probe()
        self.observed["python"] = json.dumps(self.python)
        self.observed["unexpected"] = "surprise"
        with self.assertRaisesRegex(RuntimeError, "unexpected or missing"):
            self.probe()

    def test_loader_environment_in_image_is_refused_before_run(self):
        self.metadata[0]["Config"]["Env"] = ["PATH=/usr/bin", "LD_PRELOAD=/tmp/hostile.so"]
        with self.assertRaisesRegex(RuntimeError, "loader environment"):
            self.probe()
        self.assertEqual(len(self.calls), 1)

    def test_probe_uses_fixed_private_isolated_invocation(self):
        self.assertIn("/usr/bin/env -u LD_PRELOAD -u LD_LIBRARY_PATH /usr/bin/python3 -I -B", self.smoke.PROBE)
        self.assertIn("/usr/share/rpgmigrate/python-libpq-runtime.json", self.smoke.PYTHON_RUNTIME)
        self.assertNotIn("find_library", self.smoke.PYTHON_RUNTIME)
        self.assertNotIn("ctypes.CDLL('libpq", self.smoke.PYTHON_RUNTIME)
        self.assertIn('mkfifo -m 0600', self.smoke.PROBE)
        self.assertIn('chmod 0400', self.smoke.PROBE)

    def test_real_probe_contract_and_immutable_binding(self):
        receipt = self.probe()
        self.assertEqual(receipt["image_id"], self.metadata[0]["Id"])
        self.assertEqual(receipt["effective_uid"], 65532)
        self.assertEqual(receipt["architecture"], "arm64")
        self.assertEqual(set(receipt["clients"]), {"16", "18"})
        self.assertEqual(receipt["libpq"]["package_version"], "18.1-1.pgdg13+2")
        self.assertTrue(receipt["checks"]["read_only_rootfs"])
        for argv, options in self.calls[1:]:
            for option in ("--read-only", "--network=none", "--cap-drop=ALL", "--security-opt=no-new-privileges"):
                self.assertIn(option, argv)
            self.assertIn(self.metadata[0]["Id"], argv)
            self.assertNotIn("localhost/test:dev", argv)
            self.assertNotIn("--user", argv)
            self.assertIn("--read-only-tmpfs=false", argv)
            self.assertIn("type=tmpfs,destination=/work,tmpfs-mode=0700,U=true", argv)
            self.assertIn("type=tmpfs,destination=/tmp,tmpfs-mode=0700,U=true", argv)
            self.assertGreater(options["timeout"], 0)
            self.assertFalse(options.get("shell", False))

    def test_each_subprocess_failure_is_fatal_without_echoing_output(self):
        for step in range(1, 4):
            with self.subTest(step=step):
                self.calls = []
                self.fail_at = step
                with self.assertRaisesRegex(RuntimeError, "subprocess failed") as error:
                    self.probe()
                self.assertNotIn("private output", str(error.exception))

    def test_podman_bare_content_id_is_normalized(self):
        self.metadata[0]["Id"] = "a" * 64
        receipt = self.probe()
        self.assertEqual(receipt["image_id"], "sha256:" + "a" * 64)
        self.assertIn(receipt["image_id"], self.calls[1][0])

    def test_missing_runtime_and_timeout_are_fatal(self):
        for error in (FileNotFoundError(), subprocess.TimeoutExpired("podman", 1)):
            with patch.object(self.smoke.subprocess, "run", side_effect=error):
                with self.assertRaises(RuntimeError):
                    self.smoke.probe("image", "podman")

    def test_every_required_observation_is_required(self):
        for records in (self.observed, self.package):
            for name in tuple(records):
                with self.subTest(name=name):
                    value = records.pop(name)
                    self.calls = []
                    with self.assertRaises(RuntimeError):
                        self.probe()
                    records[name] = value

    def test_wrong_observations_fail_closed(self):
        changes = {"uid": "0", "gid": "0", "machine": "x86_64", "version": "{}",
                   "plain_version": "rpgmigrate 0.1.0\nlibpq 170001",
                   "ldd": "libpq.so.5 => not found", "coreutils": "busybox", "sed": "busybox",
                   "default_psql": "psql (PostgreSQL) 16.1", "mountinfo": "25 20 0:1 / / rw - overlay overlay rw",
                   "work_write": "denied", "tmp_write": "denied", "root_write": "ok", "inputs": "", "notice": "",
                   "work_owner": "0:0:700", "tmp_owner": "0:0:777"}
        for major in (16, 18):
            for tool in ("pg_dump", "pg_restore", "psql"):
                changes[f"{tool}_{major}"] = f"{tool} (PostgreSQL) 17.1"
        for name, value in changes.items():
            with self.subTest(name=name):
                before = self.observed[name]
                self.observed[name] = value
                self.calls = []
                with self.assertRaises(RuntimeError):
                    self.probe()
                self.observed[name] = before

    def test_wrong_package_binding_is_refused(self):
        for name, value in (("path", "relative"), ("owner", "foreign: /lib/libpq.so.5"),
                            ("package", "libpq5\t17.1-1.pgdg13+2\tarm64\tinstall ok installed"),
                            ("package", "libpq5\t18.1-1\tarm64\tinstall ok installed"),
                            ("package", "libpq5\t18.2-1.pgdg13+2\tarm64\tinstall ok installed"),
                            ("package", "libpq5\t18.1-1.pgdg13+2\tamd64\tinstall ok installed")):
            with self.subTest(name=name, value=value):
                before = self.package[name]
                self.package[name] = value
                self.calls = []
                with self.assertRaises(RuntimeError):
                    self.probe()
                self.package[name] = before

    def test_invalid_metadata_is_refused(self):
        valid = copy.deepcopy(self.metadata)
        for metadata in ([], {}, [{}], [{**valid[0], "Id": "tag"}],
                         [{**valid[0], "Architecture": "s390x"}], [{**valid[0], "Os": "windows"}],
                         [{**valid[0], "Config": {**valid[0]["Config"], "User": "0"}}],
                         [{**valid[0], "Config": {**valid[0]["Config"], "Entrypoint": ["/missing"]}}]):
            with self.subTest(metadata=metadata):
                self.metadata = metadata
                self.calls = []
                with self.assertRaises(RuntimeError):
                    self.probe()

    def test_malformed_or_duplicate_probe_output_is_refused(self):
        with self.assertRaises(RuntimeError):
            self.smoke.records("not a probe")
        with self.assertRaises(RuntimeError):
            self.smoke.records("===rpgmigrate-smoke:uid===\n65532\n===rpgmigrate-smoke:uid===\n0\n")

    def test_malformed_metadata_json_is_refused(self):
        with patch.object(self.smoke.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "not json", "")):
            with self.assertRaises(RuntimeError):
                self.smoke.probe("image", "podman")

    def test_shell_metacharacters_stay_one_inspect_argument(self):
        image = "local/test;$(printf secret):dev"
        with patch.object(self.smoke.subprocess, "run", side_effect=self.invoke):
            self.smoke.probe(image, "podman")
        self.assertEqual(self.calls[0][0], ["podman", "image", "inspect", image])

    def test_historical_six_capability_image_is_refused(self):
        path = PATH.parents[2] / "tests/fixtures/native/v1/payloads/version-qualified-profile.json"
        self.observed["version"] = path.read_text()
        with self.assertRaises(RuntimeError):
            self.probe()

    def test_version_contract_requires_exact_qualified_capabilities(self):
        self.assertEqual(self.probe()["version"], {
            "protocol_version": 1, "engine_version": "0.1.0", "capabilities": QUALIFIED_CAPABILITIES,
        })
        valid = json.loads(self.observed["version"])
        sets = [[], None, True, "clone.single_db", {}, list(reversed(QUALIFIED_CAPABILITIES)),
                QUALIFIED_CAPABILITIES + [QUALIFIED_CAPABILITIES[0]],
                QUALIFIED_CAPABILITIES[:-1] + ["copy.split"],
                QUALIFIED_CAPABILITIES[:-1] + ["unknown"],
                *[QUALIFIED_CAPABILITIES[:index] + QUALIFIED_CAPABILITIES[index + 1:]
                  for index in range(len(QUALIFIED_CAPABILITIES))],
                *[QUALIFIED_CAPABILITIES + [extra] for extra in
                  ("copy.binary_fallback", "copy.split", "copy.large_objects", "unknown")]]
        versions = [{**valid, "capabilities": capabilities} for capabilities in sets]
        versions += [{**valid, "protocol_version": True}, {**valid, "protocol_version": 1.0},
                     {**valid, "engine_version": None}, {**valid, "engine_version": "0.2.0"},
                     {**valid, "linked_libpq": 180001}]
        versions += [{key: value for key, value in valid.items() if key != missing} for missing in valid]
        for version in versions:
            with self.subTest(version=version):
                self.observed["version"] = json.dumps(version)
                self.calls = []
                with self.assertRaises(RuntimeError):
                    self.probe()
        self.observed["version"] = json.dumps(valid).replace('"protocol_version": 1,',
                                                           '"protocol_version": 1,"protocol_version": 1,')
        with self.assertRaises(RuntimeError):
            self.probe()

    def test_runtime_binary_hash_is_required_and_retained(self):
        for digest in ("1" * 64, "2" * 64):
            self.calls = []
            self.observed["binary_sha256"] = digest + "  /usr/local/bin/rpgmigrate"
            self.assertEqual(self.probe()["runtime_binary_sha256"], digest)
        invalid = ["", True, False, None, 1, {}, [], "x" * 64 + "  /usr/local/bin/rpgmigrate",
                   "A" * 64 + "  /usr/local/bin/rpgmigrate",
                   "1" * 63 + "  /usr/local/bin/rpgmigrate",
                   "1" * 64 + "  /usr/local/bin/other",
                   "1" * 64 + "  /usr/local/bin/../bin/rpgmigrate",
                   "1" * 64 + "  /tmp/rpgmigrate",
                   "1" * 64 + "  /usr/local/bin/rpgmigrate extra",
                   "1" * 64 + "  /usr/local/bin/rpgmigrate\n" + "2" * 64 + "  /usr/local/bin/rpgmigrate"]
        for value in invalid:
            with self.subTest(value=value):
                self.calls = []
                self.observed["binary_sha256"] = value
                with self.assertRaises(RuntimeError):
                    self.probe()
        del self.observed["binary_sha256"]
        self.calls = []
        with self.assertRaises(RuntimeError):
            self.probe()
        with self.assertRaises(RuntimeError):
            self.smoke.records("===rpgmigrate-smoke:binary_sha256===\n" + "1" * 64
                               + "  /usr/local/bin/rpgmigrate\n===rpgmigrate-smoke:binary_sha256===\n"
                               + "2" * 64 + "  /usr/local/bin/rpgmigrate\n")

    def test_binary_probe_requires_regular_exact_file_and_successful_hash(self):
        prefix = self.smoke.PROBE.split("emit uid", 1)[0]
        with tempfile.TemporaryDirectory() as directory:
            runner = Path(directory) / "rpgmigrate"
            runner.write_bytes(b"reviewed runner bytes")
            script = prefix.replace("/usr/local/bin/rpgmigrate", shlex.quote(str(runner)))
            result = subprocess.run(["/bin/sh", "-c", script], text=True, capture_output=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.smoke.records(result.stdout)["binary_sha256"],
                             hashlib.sha256(b"reviewed runner bytes").hexdigest() + "  " + str(runner))
            runner.rename(Path(directory) / "target")
            runner.symlink_to(Path(directory) / "target")
            for kind in ("symlink", "directory", "fifo", "missing"):
                with self.subTest(kind=kind):
                    result = subprocess.run(["/bin/sh", "-c", script], text=True, capture_output=True, timeout=5)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(result.stdout, "")
                if kind == "symlink":
                    runner.unlink()
                    runner.mkdir()
                elif kind == "directory":
                    runner.rmdir()
                    os.mkfifo(runner)
                elif kind == "fifo":
                    runner.unlink()
            runner.write_bytes(b"reviewed runner bytes")
            command = Path(directory) / "sha256sum"
            command.write_text("#!/bin/sh\nprintf private-output\nexit 1\n")
            command.chmod(0o700)
            result = subprocess.run(["/bin/sh", "-c", script], env={**os.environ, "PATH": directory},
                                    text=True, capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn("private-output", result.stdout + result.stderr)

    def test_qualified_expectation_refuses_invalid_local_golden_or_metadata(self):
        fixtures = PATH.parents[2] / "tests/fixtures/native/v1"
        golden_path = fixtures / "payloads/version-qualified-linux-profile.json"
        contract_path = fixtures / "contract.json"
        golden = json.loads(golden_path.read_bytes())
        contract = json.loads(contract_path.read_bytes())
        wrong_set = QUALIFIED_CAPABILITIES[:-1] + ["copy.split"]
        variants = [
            {golden_path: b"{}"}, {golden_path: b"not json"}, {golden_path: b"null"},
            {contract_path: b"{}"}, {contract_path: b"not json"}, {contract_path: b"null"},
            {golden_path: json.dumps({**golden, "extra": 1}).encode()},
            {golden_path: json.dumps({**golden, "protocol_version": True}).encode()},
            {golden_path: json.dumps({**golden, "capabilities": []}).encode()},
            {golden_path: json.dumps({key: value for key, value in golden.items() if key != "capabilities"}).encode()},
            {contract_path: json.dumps({**contract, "qualified_linux_capabilities": []}).encode()},
            {contract_path: contract_path.read_bytes().replace(b'"protocol_version": 1,',
                            b'"protocol_version": 1,"protocol_version": 1,')},
            {golden_path: json.dumps({**golden, "capabilities": wrong_set}).encode(),
             contract_path: json.dumps({**contract, "qualified_linux_capabilities": wrong_set}).encode()},
            {golden_path: golden_path.read_bytes().replace(b'"protocol_version": 1,',
                        b'"protocol_version": 1,"protocol_version": 1,')},
        ]
        read_bytes = Path.read_bytes
        for replacement in variants:
            def read(path):
                return replacement[path] if path in replacement else read_bytes(path)
            with self.subTest(replacement=replacement), patch.object(Path, "read_bytes", read), self.assertRaises(RuntimeError):
                self.probe()
        with patch.object(Path, "read_bytes", side_effect=FileNotFoundError("private path")), self.assertRaises(RuntimeError) as error:
            self.probe()
        self.assertNotIn("private path", str(error.exception))


if __name__ == "__main__":
    unittest.main()
