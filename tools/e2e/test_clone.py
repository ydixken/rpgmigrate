"""Portable controls for the disposable clone qualification harness."""

import importlib.util
import copy
from contextlib import closing, contextmanager, ExitStack
from datetime import datetime, timezone
import json
import sqlite3
import stat
from pathlib import Path
import tempfile
import subprocess
import sys
import types
import io
import tarfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("clone_e2e", Path(__file__).with_name("clone.py"))
harness = importlib.util.module_from_spec(SPEC)
if SPEC.loader and Path(SPEC.origin).is_file():
    SPEC.loader.exec_module(harness)


class CloneHarnessTests(unittest.TestCase):
    def test_fixture_inventory_is_nonempty_exact_and_bounded(self):
        manifest = harness.load_manifest()
        self.assertEqual(tuple(manifest["pairs"]), ("16-16", "18-18", "16-18"))
        self.assertGreater(len(manifest["tables"]), 5)
        for key, value in (("tables", []), ("pairs", []), ("cases", []), ("max_rows", 0)):
            bad = {**manifest, key: value}
            with self.subTest(key=key), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "cases.json"
                path.write_text(json.dumps(bad))
                with self.assertRaisesRegex(RuntimeError, "inventory|bound"):
                    harness.load_manifest(path)

    def test_physical_fixture_row_counts_cannot_lose_nonempty_or_duplicate_coverage(self):
        manifest = harness.load_manifest()
        expected = {"empty": 0, "reference": 2, "rows": 100, "wide": 2048, "parent": 2, "child": 1, "part_low": 1, "part_high": 1}
        self.assertEqual(manifest["row_counts"], expected)
        harness.validate_fixture_rows(expected, manifest)
        for bad in ({}, {**expected, "wide": 0}, {**expected, "parent": 1}, {**expected, "parent": 3}, {**expected, "child": 0}):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                harness.validate_fixture_rows(bad, manifest)

    def test_oracle_detects_bytes_null_duplicates_and_empty(self):
        baseline = harness.row_fingerprint([b"abc\n", b"def\n", b"abc\n"], 9, 100)
        self.assertEqual(baseline, harness.row_fingerprint([b"def\n", b"abc\n", b"abc\n"], 9, 100))
        for rows in ([b"abc\n", b"def\n"], [b"abc\n", b"deg\n", b"abc\n"], [b"null\n"], []):
            self.assertNotEqual(baseline, harness.row_fingerprint(rows, 9, 100))
        self.assertEqual(harness.row_fingerprint([], 9, 100)["rows"], 0)

    def test_schema_oracle_preserves_not_null_and_other_constraint_semantics(self):
        columns = b"value|text|t\n"
        obligations = b"CHECK length(value)>0\nFOREIGN KEY reference\nPRIMARY KEY id\nINDEX value\nTRIGGER preserve_row\n"
        baseline = harness.schema_fingerprint(columns, obligations, b"0")
        self.assertNotEqual(baseline, harness.schema_fingerprint(columns.replace(b"|t", b"|f"), obligations, b"0"))
        for original in (b"CHECK", b"FOREIGN KEY", b"PRIMARY KEY", b"INDEX", b"TRIGGER"):
            with self.subTest(original=original):
                self.assertNotEqual(baseline, harness.schema_fingerprint(columns, obligations.replace(original, b"changed"), b"0"))
        for invalid in (b"1", b"", b"false", None):
            with self.subTest(invalid=invalid), self.assertRaises(RuntimeError):
                harness.schema_fingerprint(columns, obligations, invalid)

    def test_oracle_refuses_row_and_byte_overflow(self):
        for rows, count, size in (([b"a\n", b"b\n"], 1, 10), ([b"abcd\n"], 2, 2)):
            with self.assertRaisesRegex(RuntimeError, "bound"):
                harness.row_fingerprint(rows, count, size)

    def test_receipt_requires_independent_observed_case_and_versions(self):
        manifest = harness.load_manifest()
        report = {"pair": "16-18", "server_versions": ["16.15", "18.6"], "client_versions": {"pg_dump": "18.6", "pg_restore": "18.6", "psql": "18.6"}, "linked_libpq": 180006,
                  "cases": {name: {"observed": True} for name in manifest["cases"]}}
        work = {"migration_uid": "12345678-9abc-def0-1234-56789abcdef0", "run_id": "123456789abcdef0123456789abcdef0", "workdir_id": "fixture", "checkpoint_generation": 1}
        records = []
        for command in ("clone",) * 4 + ("list progress",) * 2:
            binding = {"command": command, "work": work}
            raw = {"protocol_version": 1, "engine_version": "0.1.0", "command": command,
                   "capabilities": ["clone.single_db" if command == "clone" else "progress.v1"],
                   "work": work, "phase": "completed", "copy": {name: {"total": 0, "done": 0} for name in ("tables", "indexes", "bytes")}}
            records.append({"binding": binding, "raw_json": json.dumps(raw), "probe_checked": False})
        report["report_validation"] = {"mode": "local_m2_contract", "reports": records}
        harness.validate_receipt(report, manifest)
        variants = [{**report, "report_validation": {}}, {**report, "report_validation": {"mode": "operator_go_parse_report", "reports": records}}, {**report, "cases": {}}, {**report, "linked_libpq": None},
                    {**report, "server_versions": ["16.15", "16.15"]},
                    {**report, "client_versions": {"pg_dump": "16.15"}},
                    {**report, "cases": {**report["cases"], manifest["cases"][0]: {"observed": False}}}]
        for bad in variants:
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                harness.validate_receipt(bad, manifest)

    def test_missing_fault_and_wrong_failure_reason_fail(self):
        harness.require_failure(3, "uncertain pre-data ownership", "pre-data", True)
        for status, output, observed in ((0, "pre-data", True), (3, "wrong refusal", True), (3, "pre-data", False)):
            with self.subTest(status=status, observed=observed), self.assertRaises(RuntimeError):
                harness.require_failure(status, output, "pre-data", observed)

    def test_bound_report_contract_requires_trusted_work_and_measured_completion(self):
        work = {"migration_uid": "12345678-9abc-def0-1234-56789abcdef0", "run_id": "123456789abcdef0123456789abcdef0", "workdir_id": "caller-pvc", "checkpoint_generation": 1}
        binding = {"command": "clone", "work": work}
        report = {"protocol_version": 1, "engine_version": "0.1.0", "command": "clone", "capabilities": ["clone.single_db"], "work": work, "phase": "completed", "copy": {name: {"total": 0, "done": 0} for name in ("tables", "indexes", "bytes")}}
        harness.validate_operator_report(json.dumps(report).encode(), binding, "clone", True)
        for bad in ({**report, "complete": True}, {**report, "work": {**work, "run_id": "0" * 32}},
                    {**report, "capabilities": []}, {**report, "phase": "copying"},
                    {**report, "copy": {**report["copy"], "indexes": {}}},
                    {**report, "copy": {**report["copy"], "bytes": {"total": None, "done": None}}}):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                harness.validate_operator_report(json.dumps(bad).encode(), binding, "clone", True)
        with self.assertRaises(RuntimeError):
            harness.validate_operator_report(json.dumps(report).replace('"phase": "completed"', '"phase":"completed","phase":"copying"').encode(), binding, "clone", True)

    def test_operator_mode_requires_selected_probe_and_prepares_caller_binding_before_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaises(RuntimeError):
                harness.Driver(root / "engine", {}, root, mode="operator", report_probe=root / "missing")
            engine = root / "engine"
            engine.write_text("#!" + sys.executable + "\nimport glob,json,os,pathlib\n" +
                              "files=list(pathlib.Path(__file__).parent.glob('binding-*.json'))\n" +
                              "assert len(files)==1\n" +
                              "assert __import__('sys').argv.count('--restart')==1\n" +
                              "binding=json.loads(files[0].read_text())\n" +
                              "assert binding['work']['migration_uid']==os.environ['RPGMIGRATE_MIGRATION_UID']\n" +
                              "assert binding['work']['run_id']==os.environ['RPGMIGRATE_RUN_ID']\n" +
                              "print('binding-before-producer')\n")
            engine.chmod(0o700)
            driver = harness.Driver(engine, {}, root, mode="operator")
            process = driver.start("source", "target", root / "native")
            self.assertEqual(process.wait(timeout=5), 0)
            self.assertEqual(process.output.read_text().strip(), "binding-before-producer")
            binding = json.loads(process.binding.read_text())
            self.assertEqual(binding["command"], "clone")
            self.assertEqual(binding["work"]["run_id"], binding["work"]["migration_uid"].replace("-", ""))

    def test_candidate_profile_can_include_los_without_weakening_disabled_default(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            engine = root / "engine"
            engine.write_text("#!" + sys.executable + "\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n")
            engine.chmod(0o700)
            driver = harness.Driver(engine, {}, root)
            for include in (False, True):
                process = driver.start("source", "target", root / "native", skip_large_objects=not include)
                self.assertEqual(process.wait(timeout=5), 0)
                self.assertEqual("--skip-large-objects" in json.loads(process.output.read_text()), not include)

    def test_restart_observer_matches_only_owned_pretruncate_lock(self):
        with closing(sqlite3.connect(":memory:")) as database:
            database.create_function("current_database", 0, lambda: "owned-db")
            database.execute("CREATE TABLE pg_stat_activity(pid INTEGER, datname TEXT, application_name TEXT, query TEXT, wait_event_type TEXT)")
            expected = 'LOCK TABLE ONLY "M2 space"."wide" IN ACCESS EXCLUSIVE MODE'
            rows = [
                (42, "owned-db", "owned-app", expected, "Lock"),
                (43, "owned-db", "owned-app", 'TRUNCATE ONLY "M2 space"."wide"', "Lock"),
                (44, "foreign-db", "owned-app", expected, "Lock"),
                (45, "owned-db", "foreign-app", expected, "Lock"),
                (46, "owned-db", "owned-app", expected, "Client"),
                (47, "owned-db", "owned-app", expected + "; SELECT 1", "Lock"),
                (48, "owned-db", "owned-app", expected.replace('"wide"', '"other"'), "Lock"),
            ]
            database.executemany("INSERT INTO pg_stat_activity VALUES (?,?,?,?,?)", rows)
            self.assertEqual(database.execute(harness.blocked_worker_sql("owned-app")).fetchall(), [(42,)])

    def test_failed_child_retains_private_redacted_stderr_and_safe_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "failures"
            script = "import sys; print('child-output-row'); sys.stderr.write(\"fixture-secret-password postgresql://fixture:uri-password@localhost/private password='keyword-secret' SELECT private_row_value\\n\"); sys.exit(7)"
            with patch.object(harness, "DIAGNOSTIC_ROOT", root, create=True), \
                    self.assertRaises(RuntimeError) as failure:
                harness.run([sys.executable, "-c", script], environment={"PGPASSWORD": "fixture-secret-password"})
            metadata = failure.exception.metadata
            self.assertEqual(metadata["returncode"], 7)
            self.assertTrue(metadata["retained"])
            self.assertEqual(metadata["callsite"]["file"], Path(__file__).name)
            self.assertEqual(metadata["callsite"]["function"], self._testMethodName)
            self.assertGreater(metadata["callsite"]["line"], 0)
            self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o700)
            self.assertEqual(len(list(root.iterdir())), 2)
            for path in root.iterdir():
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                value = path.read_text()
                for secret in ("fixture-secret-password", "uri-password", "keyword-secret", "child-output-row"):
                    self.assertNotIn(secret, value)
            private = next(root.glob("*.stderr")).read_text()
            self.assertIn("SELECT private_row_value", private)
            public = str(failure.exception) + json.dumps(metadata)
            self.assertNotIn("private_row_value", public)
            self.assertNotIn("SELECT", public)
            self.assertNotIn("argv", public)
            engine = root.parent / "engine"
            engine.write_text("#!" + sys.executable + "\nimport sys\nsys.stderr.write('fixture-secret-password private-engine-detail')\nsys.exit(9)\n")
            engine.chmod(0o700)
            driver = harness.Driver(engine, {"PGPASSWORD": "fixture-secret-password"}, root.parent)
            process = driver.start("source", "target", root.parent / "native")
            with patch.object(harness, "DIAGNOSTIC_ROOT", root), self.assertRaises(RuntimeError) as clone_failure:
                driver.finish(process)
            self.assertEqual(clone_failure.exception.metadata["command_kind"], "engine-clone-diagnostics")
            self.assertEqual(clone_failure.exception.metadata["returncode"], 9)
            self.assertTrue(any("private-engine-detail" in path.read_text() for path in root.glob("*.stderr")))
            self.assertTrue(all("fixture-secret-password" not in path.read_text() for path in root.iterdir()))

    def test_utility_gate_delegates_version_and_success_before_observation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bindir = root / "bin"
            bindir.mkdir()
            called = root / "called"
            for tool in ("pg_dump", "pg_restore"):
                path = bindir / tool
                path.write_text("#!" + sys.executable + "\nimport pathlib,sys\n" +
                                "if '--version' in sys.argv: print('pg_restore (PostgreSQL) 18.6')\n" +
                                f"else: pathlib.Path({str(called)!r}).touch()\n")
                path.chmod(0o700)
            gate = harness.ToolGate(root / "gate", bindir, "predata")
            self.assertEqual(harness.tool_version(gate.root / "pg_restore"), "18.6")
            self.assertFalse(gate.hit.exists())
            self.assertFalse(called.exists())
            process = subprocess.Popen([str(gate.root / "pg_restore"), "--section=pre-data"],
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                gate.wait(process)
                self.assertTrue(called.exists())
                self.assertIsNone(process.poll())
                gate.release.touch()
                self.assertEqual(process.wait(timeout=5), 0)
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)

    def test_source_and_target_oracles_must_match_nonempty_inventory(self):
        row = harness.row_fingerprint([b"a\n"], 5, 10)
        harness.require_equal({"table": row}, {"table": row})
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness, "DIAGNOSTIC_ROOT", Path(temporary) / "failures"):
            for source, target in (({}, {}), ({"table": row}, {}), ({"table": row}, {"table": {**row, "rows": 2}})):
                with self.subTest(source=source, target=target), self.assertRaises(RuntimeError) as failure:
                    harness.require_equal(source, target)
                self.assertEqual(failure.exception.metadata["command_kind"], "independent-oracle")
                self.assertEqual(failure.exception.metadata["callsite"]["function"], self._testMethodName)
                self.assertTrue(failure.exception.metadata["retained"])


def pasta_placeholder():
    return {"pasta": {"EndpointID": "", "Gateway": "", "IPAddress": "", "IPPrefixLen": 0,
                      "IPv6Gateway": "", "GlobalIPv6Address": "", "GlobalIPv6PrefixLen": 0,
                      "MacAddress": "", "NetworkID": "pasta", "DriverOpts": None, "IPAMConfig": None, "Links": None}}


class ModelRuntime:
    def __init__(self, root, fault=None, native=False):
        self.root, self.fault, self.commands, self.removed, self.native = root, fault, [], [], native
        self.containers = {}
        self.networks = {}
        self.images = {}
        self.references = {}
        for major, value in (("16", "1"), ("18", "2")):
            reference = "docker.io/library/postgres:" + major + "@" + harness.load_manifest()["images"][major]
            self.references[reference] = "sha256:" + value*64
        self.references["example.invalid/rust@sha256:" + "6"*64] = "sha256:" + "3"*64
        for reference, identity in self.references.items():
            self.images[identity] = {"Id": identity, "RepoTags": [], "RepoDigests": [reference], "Size": 1, "Labels": {}, "Architecture": "arm64"}
        self.images["sha256:" + "4"*64] = {"Id": "sha256:" + "4"*64, "RepoTags": ["localhost/rpgmigrate-m2-dev:stock"], "RepoDigests": [], "Size": 1, "Labels": {}, "Architecture": "arm64"}

    def host(self, argv):
        if argv[0] == "which": return b"/usr/bin/pasta\n"
        if argv[0] in ("sha256sum", "test"): return b""
        if argv[0] == "cat": return b"MemAvailable: 7340032 kB\n" if self.fault == "capacity" else b"MemAvailable: 25165824 kB\n"
        if argv[0] == "df":
            assert argv[2] == ("/home/runner/.local/share/containers/storage" if self.native else "/var/home/core"), "capacity sampled the wrong filesystem"
            return b"Filesystem 1B-blocks Used Available Use% Mounted\nroot 100 10 40802189312 1% /\n"
        assert argv == ["getconf", "_NPROCESSORS_ONLN"], "unexpected host probe"
        return b"14\n"

    def call(self, argv, **kwargs):
        self.commands.append(list(argv))
        if argv[0] in ("which", "cat", "df", "getconf", "sha256sum", "test"):
            assert self.native, "a host probe bypassed the podman machine"
            return self.host(argv)
        if argv[0] == "git":
            if argv[-1] == "--porcelain" and self.fault == "dirty": return b" M tracked-source\n"
            source = self.root / "tools/e2e/Containerfile"
            data = source.read_bytes()
            if "ls-tree" in argv:
                blob = __import__("hashlib").sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
                return ("100644 blob " + blob + " " + str(len(data)) + "\ttools/e2e/Containerfile\0").encode()
            if "archive" in argv:
                stream = io.BytesIO()
                with tarfile.open(fileobj=stream, mode="w") as archive:
                    entry = tarfile.TarInfo("tools/e2e/Containerfile"); entry.mode = 0o644; entry.size = len(data)
                    archive.addfile(entry, io.BytesIO(data))
                return stream.getvalue()
            return b"1"*40 + b"\n" if argv[-1] == "HEAD" else b"2"*40 + b"\n" if argv[-1] == "HEAD^{tree}" else b"feat/model\n" if argv[-1] == "--show-current" else b""
        args = argv[1:]
        if args[:2] == ["version", "--format"]:
            # A native podman reports no Server; its engine version is in info.
            return json.dumps({"Client": {"Version": "4.9.3"}} if self.native else {"Client": {"Version": "6.1.4"}, "Server": {"Version": "5.8.3"}}).encode()
        if args[:2] == ["info", "--format"]:
            return json.dumps({"host": {"security": {"rootless": True}, "networkBackend": "netavark", "pasta": {"executable": "/usr/bin/pasta", "package": "passt-model"},
                                        **({} if self.fault == "locality" else {"serviceIsRemote": not self.native})},
                               "store": {"graphRoot": "relative/storage" if self.fault == "storage" else
                                         "/home/runner/.local/share/containers/storage" if self.native else "/var/home/core/.local/share/containers/storage"},
                               "version": {"Version": "4.9.3" if self.native else "5.8.3"}}).encode()
        if args[:2] == ["run", "--help"]: return b"--network stringArray"
        if args[:2] == ["machine", "ssh"]:
            assert not self.native, "a native host probe went through a podman machine"
            return self.host(args[2:])
        if args[0] == "ps":
            if "--filter" in args:
                identity = args[args.index("--filter")+1].split("=",1)[1]
                return "\n".join(self.networks[identity]["containers"]).encode()
            return "\n".join(self.containers).encode()
        if args[:2] == ["image", "ls"]: return "\n".join(self.images).encode()
        if args[:2] == ["image", "inspect"]:
            identity = self.references.get(args[2], args[2])
            return json.dumps([self.images[identity]]).encode()
        if args[0] == "history": return b'[{"id":"safe-id","created":1,"size":1,"createdBy":"private-never-persist"}]'
        if args[:2] == ["network", "ls"]: return "\n".join(value["name"] for value in self.networks.values()).encode()
        if args[:2] == ["network", "inspect"]:
            value = self.networks.get(args[2]) or next(value for value in self.networks.values() if value["name"] == args[2])
            return json.dumps([value]).encode()
        if args[:2] == ["volume", "ls"]: return b"old-volume\n"
        if args[:2] == ["volume", "inspect"]: return b'[{"Name":"old-volume","Driver":"local","Mountpoint":"/old","CreatedAt":"old","Scope":"local","Options":null,"Labels":null}]'
        if args[0] == "inspect": return json.dumps([self.containers[args[1]]]).encode()
        if args[:2] == ["build", "--help"]: return b"--cpu-period uint --cpu-quota int --memory string\n"
        if args[0] == "build":
            tag = args[args.index("--tag")+1]; label = args[args.index("--label")+1].split("=", 1)[1]
            paths = list(self.root.rglob("*." + label + ".runtime.json"))
            assert len(paths) == 1, "build requires one exact run-bound ledger"
            path = paths[0]
            assert path.is_file() and not path.is_symlink() and stat.S_IMODE(path.stat().st_mode) == 0o600
            ledger = json.loads(path.read_text())
            assert ledger["run"] == label, "build ledger belongs to a different run"
            assert Path(ledger["private_path"]) == Path(args[args.index("--file")+1]).parents[3], "build ledger belongs to a different private source"
            assert isinstance(ledger.get("private_identity"), dict), "created private identity was not durably retained before mutation"
            identity = ledger["private_identity"]
            info = Path(ledger["private_path"]).lstat()
            assert identity["device"] == info.st_dev and identity["inode"] == info.st_ino
            assert identity["uid"] == info.st_uid and identity["mode"] == stat.S_IMODE(info.st_mode) == 0o700
            assert identity["nonce"] == ledger["run"].rsplit("-", 1)[1] and len(identity["nonce"]) == 32
            assert identity["absent_before"] is True
            if hasattr(info, "st_birthtime"): assert identity["birthtime"] == info.st_birthtime
            self.build_ledger_path = path
            identity = "sha256:" + "5"*64
            self.references[tag] = identity
            self.images[identity] = {"Id": identity, "RepoTags": [tag], "RepoDigests": [], "Size": 100, "Labels": {"io.rpgmigrate.e2e": label}, "Architecture": "arm64"}
            return b""
        if args[:2] == ["network", "create"]:
            name = args[-1]; identity = "7"*64
            self.networks[identity] = {"id": identity, "name": name, "internal": True, "driver": "bridge", "labels": {"io.rpgmigrate.e2e": name}, "containers": {}}
            return name.encode()
        if args[0] == "create":
            name = args[args.index("--name")+1]; role = name.rsplit("-", 1)[1]
            cid = {"source": "a", "target": "b", "build": "c", "runner": "d"}[role]*64
            label = args[args.index("--label")+1].split("=",1)[1]
            image = next(value for value in args if value.startswith("sha256:"))
            memory = {"1g": 1073741824, "4g": 4294967296}[args[args.index("--memory")+1]]
            mounts = []
            for i, value in enumerate(args):
                if value == "--volume":
                    source, destination, *mode = args[i+1].split(":")
                    if destination == "/workspace":
                        assert Path(source) != self.root and not (Path(source) / ".env").exists() and not (Path(source) / ".git").exists()
                    mounts.append({"Type": "bind", "Source": source, "Destination": destination, "RW": mode != ["ro"]})
            networks = {}
            if "--network" in args:
                network_name = args[args.index("--network")+1]
                if network_name == "pasta":
                    networks = pasta_placeholder()
                else:
                    network = next(value for value in self.networks.values() if value["name"] == network_name)
                    network["containers"][cid] = {"name": name}
                    networks[network_name] = {"NetworkID": network["id"], "Aliases": [role]}
            self.containers[cid] = {"Id": cid, "Name": name, "Image": image, "Config": {"Labels": {"io.rpgmigrate.e2e": label}, "Env": ["PASSWORD=must-not-persist"]},
                "Mounts": mounts, "HostConfig": {"Memory": memory, "NanoCpus": 2000000000, "Privileged": False, "NetworkMode": "pasta" if role == "build" else "bridge", "PortBindings": {}},
                "NetworkSettings": {"Ports": {}, "Networks": networks}, "Created": datetime.fromtimestamp(harness.time.time(), timezone.utc).isoformat(), "RestartCount": 0,
                "State": {"Running": False, "Status": "created", "StartedAt": "0001-01-01T00:00:00Z", "FinishedAt": "0001-01-01T00:00:00Z", "Pid": 0}}
            self.containers[cid]["model_role"] = role
            self.containers[cid]["model_pgdata"] = next((value[7:] for value in args if value.startswith("PGDATA=")), None)
            return cid.encode()
        if args[0] == "start":
            cid = args[-1]; actual = self.containers[cid]; role = actual["model_role"]
            actual["State"].update(Running=True, Status="running", Pid=42, StartedAt=datetime.fromtimestamp(harness.time.time(), timezone.utc).isoformat())
            if role == "build":
                cache = Path(next(row["Source"] for row in actual["Mounts"] if row["Destination"] == "/build"))
                (cache / "debug").mkdir(); (cache / "debug/rpgmigrate").write_bytes(b"debug executable"); (cache / "debug/rpgmigrate").chmod(0o700)
            if role == "runner":
                receipt = Path(next(row["Source"] for row in actual["Mounts"] if row["Destination"] == "/receipt"))
                binding = json.loads((receipt / "m8-input.json").read_text()) if (receipt / "m8-input.json").exists() else None
                smoke = None
                if binding:
                    smoke = {"binary_sha256": binding["binary_sha256"], "source_tree_sha256": binding["source_tree_sha256"], "runner_image_id": binding["runner_image_id"],
                             "architecture": "arm64", "client_versions": {tool: "16.15" for tool in ("psql", "pg_dump", "pg_restore")}, "linked_libpq": 180006,
                             "library_path": "/usr/lib/aarch64-linux-gnu/libpq.so.5.18", "library_sha256": "9"*64,
                             "packages": {name: {"version": version + "-1.pgdg13+1", "architecture": "arm64"} for name, version in (("libpq5", "18.6"), ("postgresql-client-16", "16.15"), ("postgresql-client-18", "18.6"))},
                             "version_json": {"protocol_version": 1, "engine_version": "0.1.0", "capabilities": ["clone.single_db", "cdc.pgoutput", "progress.v1", "cutover.v1", "verify.v1", "cleanup.v1", "verify.sealed_files.v1"]},
                             "version_text": "rpgmigrate 0.1.0\nlibpq 180006\n"}
                    (receipt / "m8-smoke.json").write_text(json.dumps(smoke)); (receipt / "m8-smoke.json").chmod(0o600)
                if self.fault == "runner": raise RuntimeError("modeled runner failure")
                (receipt / "result.json").write_text(json.dumps({"pair": "16-16", "binding": binding, "build_smoke": smoke}))
                if self.fault == "foreign_private":
                    private = receipt.parent
                    private.rename(private.with_name(private.name + ".retained"))
                    private.mkdir(mode=0o700)
                    (private / "foreign").write_text("preserve foreign leaf")
            return b""
        if args[0] == "exec":
            actual = self.containers[args[1]]
            if args[2] == "cat": return b"200000 100000\n" + str(actual["HostConfig"]["Memory"]).encode() + b"\n"
            if args[2] == "stat": return b"700|postgres\n"
            if "pg_ls_waldir" in args[-1]: return b"1000|1000\n"
            role = actual["model_role"]
            data = actual["model_pgdata"]
            if self.fault == "data_directory": data = "/borrowed"
            return json.dumps({"system_identifier": "770600000000000000" + ("1" if role == "source" else "2"), "version": "16.15", "data_directory": data, "wal_level": "logical"}).encode()
        if args[0] == "logs": return b"bounded startup diagnostic"
        if args[0] == "rm":
            cid = args[-1]; self.removed.append(cid)
            self.containers.pop(cid)
            for network in self.networks.values(): network["containers"].pop(cid, None)
            return cid.encode()
        if args[:2] == ["network", "rm"]:
            self.networks.pop(args[-1]); return b""
        raise AssertionError("unexpected modeled command: " + repr(argv))

    def subprocess(self, argv, **kwargs):
        if len(argv)>2 and argv[2] == "exists":
            kind, identity = argv[1], argv[3]
            if kind == "image" and self.fault == "missing_ref" and identity.startswith("example.invalid/"):
                return subprocess.CompletedProcess(argv, 1, stdout=b"", stderr=b"")
            if kind == "image" and self.fault == "image_collision" and identity.startswith("localhost/rpgmigrate-"):
                return subprocess.CompletedProcess(argv, 0, stdout=b"", stderr=b"")
            present = identity in self.references or identity in self.images if kind == "image" else identity in self.networks or any(value["name"] == identity for value in self.networks.values()) if kind == "network" else identity in self.containers or any(value["Name"] == identity for value in self.containers.values())
            return subprocess.CompletedProcess(argv, 0 if present else 1, stdout=b"", stderr=b"")
        if len(argv)>3 and argv[1] == "exec" and argv[3] == "pg_isready":
            return subprocess.CompletedProcess(argv, 0, stdout=b"accepting connections", stderr=b"")
        raise AssertionError("unexpected modeled low-level subprocess")


class CreationClock:
    def __init__(self): self.now = 100.0
    def monotonic(self): return self.now
    def wall(self): return 1794300000.0 + self.now


class CreationProcess:
    def __init__(self, command, model, clock, role, boundary, pid):
        self.command, self.clock, self.boundary = command, clock, boundary
        self.pid, self.returncode, self.waited = pid, None, False
        self.trigger = (command[1] == "create" and command[command.index("--name") + 1].endswith("-" + role)
                        or role == "image" and command[1] == "build" or role == "network" and command[1:3] == ["network", "create"])
        self.attached = command[1] == "start" and "--attach" in command
        self.output = model.call(command)

    def communicate(self, timeout):
        if self.trigger and self.waited and self.boundary == "poll" and self.returncode is None:
            self.clock.now += 5
        if self.trigger and not self.waited:
            self.waited = True
            self.clock.now += 5
            if self.boundary == "poll":
                raise subprocess.TimeoutExpired(self.command, timeout)
        if self.attached and not self.waited:
            self.clock.now += 5
            self.waited = True
        self.returncode = 0 if self.returncode is None else self.returncode
        return self.output, b""

    def poll(self): return self.returncode


class PendingCreationTests(unittest.TestCase):
    @contextmanager
    def creation(self, *, boundary="poll", effect=None, output=None, code=0, hang=False, reap_output=b""):
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            root, clock = Path(temporary).resolve(), CreationClock()
            model = ModelRuntime(root)
            model.networks["7"*64] = {"id": "7"*64, "name": "fresh", "internal": True, "driver": "bridge", "labels": {"io.rpgmigrate.e2e": "fresh"}, "containers": {}}
            guard = harness.RuntimeGuard.__new__(harness.RuntimeGuard)
            guard.runtime, guard.token, guard.path = "podman", "fresh", root / "ledger.json"
            guard.machine, guard.storage = True, "/var/home/core"
            guard.deadline, guard.work_deadline, guard.last_sample = 900, 600, clock.now
            guard.baseline = {"containers": {}, "external": [], "images": {}, "networks": {}, "volumes": {}}
            guard.ledger = {"containers": [], "samples": [], "admitted_images": [], "network": {"id": "7"*64, "name": "fresh", "cleanup_status": "pending"}}
            guard.initial_capacity = {"vm_free": 40802189312, "host_free": 57*harness.GIB, "vm_available": 24*harness.GIB, "vm_cpus": 14}
            expected = {"kind": "container", "role": "source", "image": "sha256:" + "1"*64, "name": "fresh-source", "mounts": [], "memory": harness.GIB, "network": {"name": "fresh", "id": "7"*64}}
            command = ["podman", "create", "--name", "fresh-source", "--network", "fresh", "--network-alias", "source", "--label", "io.rpgmigrate.e2e=fresh", "--memory", "1g", "--cpus", "2", expected["image"]]
            children = []
            def launch(argv, **kwargs):
                self.assertEqual(argv, command, "an unexpected subprocess escaped the fake creation boundary")
                persisted = json.loads(guard.path.read_text())
                self.assertEqual(persisted["pending_creation"]["status"], "intent")
                child = CreationProcess(argv, model, clock, "source", boundary, 1000 + len(children))
                if output is not None: child.output = output
                original = child.communicate
                def communicate(timeout):
                    if effect is not None: effect(guard, model, clock, child)
                    if hang and child.returncode is None:
                        clock.now += min(5, timeout)
                        raise subprocess.TimeoutExpired(argv, timeout)
                    if child.returncode is not None and hang:
                        return reap_output, b""
                    result = original(timeout)
                    if child.returncode == 0: child.returncode = code
                    return result
                child.communicate = communicate
                children.append(child)
                return child
            def terminate(pid, sig):
                self.assertEqual(pid, children[-1].pid)
                self.assertEqual(sig, harness.signal.SIGKILL)
                children[-1].returncode = -sig
            stack.enter_context(patch.object(harness.RuntimeGuard, "__init__", side_effect=AssertionError("real admission constructor forbidden")))
            stack.enter_context(patch.object(harness, "ROOT", root))
            stack.enter_context(patch.object(harness, "DIAGNOSTIC_ROOT", root / "diagnostics"))
            stack.enter_context(patch.object(harness.shutil, "disk_usage", return_value=types.SimpleNamespace(free=57*harness.GIB)))
            stack.enter_context(patch.object(harness.time, "monotonic", side_effect=clock.monotonic))
            stack.enter_context(patch.object(harness.time, "time", side_effect=clock.wall))
            stack.enter_context(patch.object(harness, "run", side_effect=model.call))
            stack.enter_context(patch.object(harness.subprocess, "run", side_effect=model.subprocess))
            stack.enter_context(patch.object(harness.subprocess, "Popen", side_effect=launch))
            stack.enter_context(patch.object(harness.os, "killpg", side_effect=terminate))
            yield guard, model, clock, command, expected, children

    def test_pending_candidate_specification_and_never_started_state_are_closed(self):
        mutations = {
            "nonce": lambda x: x["Config"]["Labels"].update({"io.rpgmigrate.e2e": "foreign"}),
            "image": lambda x: x.update(Image="sha256:" + "2"*64), "name": lambda x: x.update(Name="foreign"),
            "mount": lambda x: x["Mounts"].append({"Type": "bind", "Source": "/foreign", "Destination": "/data", "RW": True}),
            "memory": lambda x: x["HostConfig"].update(Memory=0), "cpu": lambda x: x["HostConfig"].update(NanoCpus=0),
            "network": lambda x: x["NetworkSettings"]["Networks"]["fresh"].update(NetworkID="8"*64),
            "alias": lambda x: x["NetworkSettings"]["Networks"]["fresh"].update(Aliases=["target"]),
            "stale": lambda x: x.update(Created="2000-01-01T00:00:00Z"),
            "future": lambda x: x.update(Created="2100-01-01T00:00:00Z"),
            "running": lambda x: x["State"].update(Running=True), "status": lambda x: x["State"].update(Status="running"),
            "pid": lambda x: x["State"].update(Pid=42), "boolean_pid": lambda x: x["State"].update(Pid=False),
            "started": lambda x: x["State"].update(StartedAt="2026-11-10T00:00:00Z"),
            "finished": lambda x: x["State"].update(FinishedAt="2026-11-10T00:00:00Z"), "restarts": lambda x: x.update(RestartCount=1),
        }
        for name, mutate in mutations.items():
            def effect(guard, model, clock, child): mutate(model.containers["a"*64])
            with self.subTest(fault=name), self.creation(effect=effect, output=b"") as (guard, model, _, command, expected, children):
                with self.assertRaises(RuntimeError): guard._create(command, expected)
                self.assertFalse(guard.ledger["containers"])
                self.assertEqual(guard.ledger["pending_creation"]["status"], "unresolved")
                self.assertFalse(model.removed)
                self.assertIsNotNone(children[0].returncode)

    def test_wrong_missing_multiple_and_replaced_receipts_never_own_candidates(self):
        for receipt in (b"", b"a"*12, b"b"*64, b"a"*64 + b"\n" + b"b"*64):
            with self.subTest(receipt=receipt), self.creation(output=receipt) as (guard, model, _, command, expected, _):
                with self.assertRaises((RuntimeError, KeyError)): guard._create(command, expected)
                self.assertFalse(guard.ledger["containers"])
                self.assertFalse(model.removed)
                self.assertTrue(guard._creation_unresolved())

    def test_foreign_peer_in_either_network_inventory_is_never_explained(self):
        for channel in ("endpoint", "filtered"):
            def effect(guard, model, clock, child):
                if channel == "endpoint": model.networks["7"*64]["containers"]["f"*64] = {}
            with self.subTest(channel=channel), self.creation(effect=effect, output=b"") as (guard, model, _, command, expected, _):
                raw = guard.raw
                def probe(arguments, **kwargs):
                    result = raw(arguments, **kwargs)
                    return result + b"\n" + b"f"*64 if channel == "filtered" and arguments[0] == "ps" and "--filter" in arguments else result
                with patch.object(guard, "raw", side_effect=probe), self.assertRaises(RuntimeError): guard._create(command, expected)
                self.assertFalse(model.removed)
                self.assertFalse(guard.ledger["containers"])

    def test_partial_creation_without_receipt_remains_unresolved_and_untouched(self):
        for mode in ("nonzero", "timeout"):
            with self.subTest(mode=mode), self.creation(output=b"", code=9 if mode == "nonzero" else 0, hang=mode == "timeout") as (guard, model, _, command, expected, children):
                with self.assertRaises(RuntimeError): guard._create(command, expected, timeout=10)
                self.assertFalse(guard.ledger["containers"])
                self.assertTrue(guard._creation_unresolved())
                guard.close()
                self.assertFalse(guard.ledger["cleanup_complete"])
                self.assertFalse(model.removed)
                self.assertIn("a"*64, model.containers)
                self.assertIsNotNone(children[0].returncode)

    def test_authoritative_receipt_after_failure_grants_cleanup_only(self):
        for mode in ("nonzero", "timeout", "capacity", "deadline", "persist"):
            def effect(guard, model, clock, child):
                if mode == "capacity": model.fault = "capacity"
                if mode == "deadline": clock.now = guard.work_deadline + 1
            with self.subTest(mode=mode), self.creation(effect=effect, code=9 if mode == "nonzero" else 0, hang=mode == "timeout", reap_output=b"a"*64) as (guard, model, _, command, expected, children):
                deadlines = guard.work_deadline, guard.deadline
                persist = guard.persist
                def store():
                    if mode == "persist" and guard.ledger.get("pending_creation", {}).get("status") == "admitted":
                        raise OSError("modeled durable admission failure")
                    persist()
                with patch.object(guard, "persist", side_effect=store), self.assertRaises(RuntimeError if mode != "persist" else OSError):
                    guard._create(command, expected, timeout=10)
                self.assertEqual(len(guard.ledger["containers"]), 1)
                self.assertTrue(guard.ledger["containers"][0]["cleanup_only"])
                self.assertFalse(any(argv[1] == "start" for argv in model.commands))
                self.assertEqual(len(children), 1)
                self.assertEqual((guard.work_deadline, guard.deadline), deadlines)
                guard.cleanup_container(guard.ledger["containers"][0])
                self.assertEqual(model.removed, ["a"*64])

    def test_only_the_live_bound_child_can_explain_a_provisional_candidate(self):
        for fault in ("second", "pid", "inactive", "expired", "baseline", "external", "crash"):
            def effect(guard, model, clock, child):
                pending = guard._pending_creation
                if fault == "second": guard._create(child.command, pending["expected"])
                if fault == "pid": pending["projection"]["pid"] += 1
                if fault == "inactive": pending["projection"]["status"] = "intent"
                if fault == "expired": pending["deadline"] = clock.now
                if fault == "baseline": guard.baseline["containers"]["a"*64] = {}
                if fault == "external": guard.baseline["external"].append("a"*64)
                if fault == "crash": guard._pending_creation = None
            with self.subTest(fault=fault), self.creation(effect=effect, output=b"") as (guard, model, _, command, expected, children):
                with self.assertRaises(RuntimeError): guard._create(command, expected)
                self.assertEqual(len(children), 1)
                self.assertFalse(guard.ledger["containers"])
                self.assertFalse(model.removed)

    def test_candidate_identity_and_receipt_are_pinned_idempotently(self):
        def replace(guard, model, clock, child):
            if child.waited:
                model.containers["b"*64] = model.containers.pop("a"*64)
                model.containers["b"*64]["Id"] = "b"*64
                model.networks["7"*64]["containers"] = {"b"*64: {}}
                child.output = b"b"*64
        with self.creation(effect=replace) as (guard, model, _, command, expected, _):
            with self.assertRaisesRegex(RuntimeError, "identity changed"): guard._create(command, expected)
            self.assertFalse(guard.ledger["containers"])
            self.assertEqual(guard.ledger["pending_creation"]["candidate_id"], "a"*64)
            self.assertFalse(model.removed)
        def changed(guard, model, clock, child):
            if child.waited: model.containers["a"*64]["HostConfig"]["Memory"] = 0
        with self.creation(effect=changed) as (guard, model, _, command, expected, _):
            with self.assertRaisesRegex(RuntimeError, "caps"): guard._create(command, expected)
            self.assertFalse(guard.ledger["containers"])
            self.assertFalse(model.removed)
        with self.creation() as (guard, _, _, command, expected, _):
            admit = guard._admit_creation
            def repeated(output):
                entry = admit(output)
                self.assertIs(admit(output), entry)
                with self.assertRaisesRegex(RuntimeError, "receipt changed"): admit(b"b"*64)
                return entry
            with patch.object(guard, "_admit_creation", side_effect=repeated):
                entry = guard._create(command, expected)
            self.assertEqual(guard.ledger["containers"], [entry])

    def test_positive_absence_resolves_without_deletion_and_unavailable_probe_does_not(self):
        for fault in (None, "unavailable", "ambiguous"):
            def effect(guard, model, clock, child):
                model.containers.clear()
                model.networks["7"*64]["containers"].clear()
            with self.subTest(fault=fault), self.creation(effect=effect, output=b"", code=9, boundary="return") as (guard, model, _, command, expected, _):
                probe = model.subprocess
                def exists(argv, **kwargs):
                    if fault and children_started[0] and argv[1] == "container":
                        if fault == "unavailable": raise OSError("unavailable")
                        return subprocess.CompletedProcess(argv, 125)
                    return probe(argv, **kwargs)
                children_started = [False]
                launch = harness.subprocess.Popen.side_effect
                def popen(argv, **kwargs):
                    child = launch(argv, **kwargs)
                    children_started[0] = True
                    return child
                with patch.object(harness.subprocess, "Popen", side_effect=popen), patch.object(harness.subprocess, "run", side_effect=exists), self.assertRaises(RuntimeError):
                    guard._create(command, expected)
                self.assertEqual(guard.ledger["pending_creation"]["status"], "absent" if fault is None else "unresolved")
                self.assertFalse(model.removed)

    def test_initial_persistence_failure_launches_nothing_and_first_error_survives_reaping(self):
        with self.creation() as (guard, model, _, command, expected, children):
            with patch.object(guard, "persist", side_effect=OSError("first durable intent")), self.assertRaisesRegex(OSError, "first durable intent"):
                guard._create(command, expected)
            self.assertFalse(children)
            self.assertFalse(model.containers)
        with self.creation(hang=True) as (guard, model, _, command, expected, _):
            with patch.object(harness.os, "killpg", side_effect=OSError("secondary reap")), self.assertRaisesRegex(RuntimeError, "deadline"):
                guard._create(command, expected, timeout=10)
            self.assertEqual(guard.ledger["processes"][0]["reaping_error_kind"], "OSError")
            self.assertTrue(guard._creation_unresolved())
            self.assertFalse(model.removed)

    def test_pending_projection_rejects_secret_fields_and_unbound_commands(self):
        for fault in ("secret", "command", "existing"):
            with self.subTest(fault=fault), self.creation() as (guard, model, _, command, expected, children):
                if fault == "secret": expected["environment"] = {"PASSWORD": "never-persist"}
                if fault == "command": command[1] = "rm"
                if fault == "existing": model.call(command)
                with self.assertRaises(RuntimeError): guard._create(command, expected)
                self.assertFalse(children)
                self.assertNotIn("never-persist", json.dumps(guard.ledger))

    def test_outer_retains_unresolved_private_paths_and_preserves_first_failure(self):
        for fault in ("unconfirmed", "cleanup", "persist"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
                root, clock, guards, children = Path(temporary).resolve(), CreationClock(), [], []
                (root / "tools/e2e").mkdir(parents=True)
                (root / "tools/e2e/Containerfile").write_text("FROM example.invalid/rust@sha256:" + "6"*64 + "\n")
                model = ModelRuntime(root, None if fault == "unconfirmed" else "runner")
                initialize, persist = harness.RuntimeGuard.__init__, harness.RuntimeGuard.persist
                def admission(guard, *args):
                    initialize(guard, *args)
                    guards.append(guard)
                def store(guard):
                    if fault == "persist" and "failure" in guard.ledger: raise OSError("secondary ledger failure")
                    persist(guard)
                def raw(argv, **kwargs):
                    if fault == "cleanup" and argv[1] == "rm" and "d"*64 in model.containers: raise OSError("secondary cleanup failure")
                    return model.call(argv, **kwargs)
                def launch(argv, **kwargs):
                    self.assertIn(argv[0], ("git", "podman"))
                    child = CreationProcess(argv, model, clock, "source", "return", 1000 + len(children))
                    if fault == "unconfirmed" and child.trigger:
                        original = child.communicate
                        def no_receipt(timeout):
                            original(timeout)
                            child.returncode = 9
                            return b"", b"PASSWORD=must-not-persist private-row"
                        child.communicate = no_receipt
                    children.append(child)
                    return child
                stack.enter_context(patch.object(harness, "ROOT", root))
                stack.enter_context(patch.object(harness, "DIAGNOSTIC_ROOT", root / "diagnostics"))
                stack.enter_context(patch.object(harness.shutil, "which", return_value="/fake/podman"))
                stack.enter_context(patch.object(harness.shutil, "disk_usage", return_value=types.SimpleNamespace(free=57*harness.GIB)))
                stack.enter_context(patch.object(harness.time, "monotonic", side_effect=clock.monotonic))
                stack.enter_context(patch.object(harness.time, "time", side_effect=clock.wall))
                stack.enter_context(patch.object(harness, "run", side_effect=raw))
                stack.enter_context(patch.object(harness.subprocess, "run", side_effect=model.subprocess))
                stack.enter_context(patch.object(harness.subprocess, "Popen", side_effect=launch))
                stack.enter_context(patch.object(harness.RuntimeGuard, "__init__", admission))
                stack.enter_context(patch.object(harness.RuntimeGuard, "persist", store))
                stack.enter_context(patch.object(harness.os, "killpg", side_effect=AssertionError("unexpected child termination")))
                args = types.SimpleNamespace(pair="16-16", receipt=str(root / "result.json"), report_probe=None, profile="m8")
                with self.assertRaises(RuntimeError) as raised: harness.outer(args)
                ledger = guards[0].ledger
                if fault == "unconfirmed":
                    self.assertIsInstance(raised.exception, harness.SubprocessFailure)
                    self.assertEqual(raised.exception.metadata["returncode"], 9)
                    self.assertTrue(guards[0]._creation_unresolved())
                    self.assertNotIn("a"*64, model.removed)
                    self.assertFalse(any(argv[1] == "start" and argv[-1] == "a"*64 for argv in model.commands))
                else:
                    self.assertEqual(str(raised.exception), "modeled runner failure")
                if fault != "persist":
                    self.assertFalse(ledger["cleanup_complete"])
                    self.assertFalse(ledger["private_removed"])
                    self.assertTrue(Path(ledger["private_path"]).is_dir())
                self.assertFalse(Path(args.receipt).exists())
                retained = json.dumps(ledger)
                self.assertNotIn("must-not-persist", retained)
                self.assertNotIn("private-row", retained)
                self.assertNotIn('"argv"', retained)

    def test_real_execute_admits_creation_before_poll_and_final_samples(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import copy_features
        try:
            for role in ("source", "target", "runner", "build", "network", "image"):
                for boundary in ("poll", "return"):
                    with self.subTest(role=role, boundary=boundary), tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
                        root = Path(temporary).resolve(); (root / "tools/e2e").mkdir(parents=True)
                        (root / "tools/e2e/Containerfile").write_text("FROM example.invalid/rust@sha256:" + "6"*64 + "\n")
                        model, clock, children = ModelRuntime(root), CreationClock(), []
                        observed = []
                        sample = harness.RuntimeGuard.sample
                        def guarded_sample(guard):
                            pending = guard.ledger.get("pending_creation", {})
                            if pending.get("status") == "admitted":
                                persisted = json.loads(guard.path.read_text())
                                self.assertEqual(persisted["pending_creation"]["admitted_id"], pending["admitted_id"])
                                if pending["kind"] == "image":
                                    self.assertIn(pending["admitted_id"], persisted["admitted_images"])
                                elif pending["kind"] == "network":
                                    self.assertEqual(persisted["network"]["id"], pending["admitted_id"])
                                else:
                                    self.assertTrue(any(entry["id"] == pending["admitted_id"] and entry["ownership_verified"] for entry in persisted["containers"]))
                                observed.append(pending["kind"])
                            return sample(guard)
                        def launch(command, **kwargs):
                            self.assertIn(command[0], ("podman", "git"))
                            if command[1] == "start":
                                ledger = json.loads(model.build_ledger_path.read_text())
                                owned = [entry for entry in ledger["containers"] if entry["id"] == command[-1]]
                                self.assertEqual(len(owned), 1)
                                self.assertIs(owned[0].get("ownership_verified"), True)
                            child = CreationProcess(command, model, clock, role, boundary, 1000 + len(children))
                            children.append(child)
                            return child
                        def terminate(pid, sig):
                            matches = [child for child in children if child.pid == pid]
                            self.assertEqual(len(matches), 1)
                            self.assertEqual(sig, harness.signal.SIGKILL)
                            matches[0].returncode = -sig
                        stack.enter_context(patch.object(harness, "ROOT", root))
                        stack.enter_context(patch.object(harness, "DIAGNOSTIC_ROOT", root / "diagnostics"))
                        stack.enter_context(patch.object(harness.shutil, "which", return_value="/fake/podman"))
                        stack.enter_context(patch.object(harness.shutil, "disk_usage", return_value=types.SimpleNamespace(free=57*harness.GIB)))
                        stack.enter_context(patch.object(harness.time, "monotonic", side_effect=clock.monotonic))
                        stack.enter_context(patch.object(harness.time, "time", side_effect=clock.wall))
                        stack.enter_context(patch.object(harness, "run", side_effect=model.call))
                        stack.enter_context(patch.object(harness.subprocess, "run", side_effect=model.subprocess))
                        stack.enter_context(patch.object(harness.subprocess, "Popen", side_effect=launch))
                        stack.enter_context(patch.object(harness.RuntimeGuard, "sample", guarded_sample))
                        stack.enter_context(patch.object(harness.os, "killpg", side_effect=terminate))
                        stack.enter_context(patch.object(copy_features, "validate_receipt"))
                        output = root / "result.json"
                        harness.outer(types.SimpleNamespace(pair="16-16", receipt=str(output), report_probe=None, profile="m8"), receipt_validator=lambda *_: None)
                        self.assertTrue(output.is_file())
                        self.assertTrue(all(child.returncode == 0 for child in children))
                        self.assertFalse(model.containers)
                        ledger = json.loads(model.build_ledger_path.read_text())
                        self.assertTrue(ledger["cleanup_complete"])
                        self.assertEqual(len(ledger["containers"]), 4)
                        self.assertIn("container" if role in ("source", "target", "runner", "build") else role, observed)
        finally:
            sys.path.pop(0)


class RuntimeOwnershipTests(unittest.TestCase):
    def test_server_arguments_enable_logical_only_for_m8(self):
        self.assertTrue(callable(getattr(harness, "server_start_arguments", None)), "missing reviewed startup selection")
        self.assertEqual(harness.server_start_arguments("disabled", ()), ())
        logical = ("-c", "wal_level=logical", "-c", "max_replication_slots=32", "-c", "max_wal_senders=32", "-c", "wal_sender_timeout=10s")
        self.assertEqual(harness.server_start_arguments("m8", ()), logical)
        self.assertEqual(harness.server_start_arguments("disabled", logical), logical)
        with self.assertRaises(RuntimeError):
            harness.server_start_arguments("m8", ("-c", "wal_level=replica"))

    def test_exact_container_identity_caps_and_mounts_are_required(self):
        self.assertTrue(callable(getattr(harness, "validate_owned_container", None)), "missing exact ownership admission")
        entry = {"id": "a" * 64, "image": "sha256:" + "b" * 64, "name": "fresh-source", "memory": 1073741824, "mounts": [], "network": {"name": "fresh", "id": "c"*64}}
        actual = {"Id": entry["id"], "Image": entry["image"], "Name": entry["name"], "Config": {"Labels": {"io.rpgmigrate.e2e": "fresh"}},
                  "Mounts": [], "HostConfig": {"Memory": 1073741824, "NanoCpus": 2000000000, "Privileged": False, "NetworkMode": "fresh", "PortBindings": {}},
                  "NetworkSettings": {"Ports": {"5432/tcp": None}, "Networks": {"fresh": {"NetworkID": "c"*64}}}}
        harness.validate_owned_container(actual, entry, "fresh", set())
        for mutate in (lambda x: x.update(Id="c" * 64), lambda x: x.update(Image="sha256:" + "c" * 64),
                       lambda x: x["Config"]["Labels"].clear(), lambda x: x["Mounts"].append({"Type": "volume", "Destination": "/var/lib/postgresql/data"}),
                       lambda x: x["HostConfig"].update(Memory=0), lambda x: x["HostConfig"].update(NanoCpus=0),
                       lambda x: x["HostConfig"].update(Privileged=True), lambda x: x["HostConfig"].update(NetworkMode="host"),
                       lambda x: x["HostConfig"].update(PortBindings={"5432/tcp": [{"HostPort": "5432"}]}),
                       lambda x: x["NetworkSettings"]["Networks"].update(foreign={"NetworkID": "d"*64}),
                       lambda x: x["NetworkSettings"]["Networks"]["fresh"].update(NetworkID="d"*64)):
            bad = copy.deepcopy(actual); mutate(bad)
            with self.subTest(actual=bad), self.assertRaises(RuntimeError):
                harness.validate_owned_container(bad, entry, "fresh", set())
        with self.assertRaises(RuntimeError):
            harness.validate_owned_container(actual, entry, "fresh", {entry["id"]})
        cfs = copy.deepcopy(actual); cfs["HostConfig"].update(NanoCpus=0, CpuPeriod=100000, CpuQuota=200000)
        harness.validate_owned_container(cfs, entry, "fresh", set())

    def test_network_cleanup_requires_exact_identity_label_internal_and_empty_membership(self):
        self.assertTrue(callable(getattr(harness, "validate_owned_network", None)), "missing exact network admission")
        expected = {"id": "a" * 64, "name": "fresh"}
        actual = {"id": expected["id"], "name": "fresh", "internal": True, "labels": {"io.rpgmigrate.e2e": "fresh"}, "containers": {}}
        harness.validate_owned_network(actual, expected, "fresh", set(), empty=True)
        for change in ({"id": "b" * 64}, {"name": "foreign"}, {"internal": False}, {"labels": {}}, {"containers": {"foreign": {}}}, {"containers": None}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                harness.validate_owned_network({**actual, **change}, expected, "fresh", set(), empty=True)
        with self.assertRaises(RuntimeError):
            harness.validate_owned_network(actual, expected, "fresh", {expected["id"]}, empty=True)

    def test_capacity_and_fixed_deadline_fail_closed(self):
        self.assertTrue(callable(getattr(harness, "validate_capacity", None)), "missing capacity admission")
        gib = 1024 ** 3
        initial = {"vm_free": 38*gib, "host_free": 57*gib, "vm_available": 24*gib, "vm_cpus": 14}
        harness.validate_capacity(initial, initial, initial=True)
        for change in ({"vm_free": 31*gib}, {"host_free": 31*gib}, {"vm_available": 11*gib}, {"vm_cpus": 5}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                harness.validate_capacity({**initial, **change}, initial, initial=True)
        for change in ({"vm_free": 19*gib}, {"host_free": 19*gib}, {"vm_available": 7*gib}, {"host_free": 44*gib}, {"vm_free": 25*gib}, {"vm_free": None}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                harness.validate_capacity({**initial, **change}, initial)

    def test_build_cpu_caps_require_supported_cfs_options(self):
        self.assertTrue(callable(getattr(harness, "build_cpu_arguments", None)), "missing supported build CPU admission")
        self.assertEqual(harness.build_cpu_arguments(b"--cpu-period uint --cpu-quota int"), ["--cpu-period", "100000", "--cpu-quota", "200000"])
        for help_text in (b"--jobs int --memory string", b"--cpus float", b"--cpu-period uint"):
            with self.assertRaises(RuntimeError):
                harness.build_cpu_arguments(help_text)


    def test_preservation_refuses_changed_old_associations_and_unowned_resources(self):
        self.assertTrue(callable(getattr(harness, "validate_preservation", None)), "missing preservation proof")
        before = {"containers": {"a"*64: {"state": "running"}}, "external": ["a"*64], "images": {"b"*64: {"tags": ["old:tag"], "digests": ["old@digest"]}}, "networks": {"c"*64: {"name": "old"}}, "volumes": {"old-volume": {"name": "old-volume"}}}
        harness.validate_preservation(before, copy.deepcopy(before), set())
        after = copy.deepcopy(before); after["images"]["d"*64] = {"tags": ["fresh:run"]}
        harness.validate_preservation(before, after, {"d"*64})
        for kind in before:
            bad = copy.deepcopy(before)
            if isinstance(bad[kind], dict):
                bad[kind].clear()
            else:
                bad[kind] = []
            with self.subTest(kind=kind), self.assertRaises(RuntimeError):
                harness.validate_preservation(before, bad, set())
        for kind, value in (("volumes", {"anonymous": {}}), ("containers", {"unowned": {}}), ("networks", {"foreign": {}}), ("images", {"unowned": {}})):
            bad = copy.deepcopy(before); bad[kind].update(value)
            with self.subTest(kind=kind), self.assertRaises(RuntimeError):
                harness.validate_preservation(before, bad, set())

    def test_cleanup_refuses_wrong_image_label_foreign_network_and_failed_absence(self):
        self.assertTrue(hasattr(harness, "RuntimeGuard"), "missing bounded shared ownership lifecycle")
        for fault in ("image", "label", "unavailable", "remove", "absence", "ok"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                guard = harness.RuntimeGuard.__new__(harness.RuntimeGuard)
                guard.runtime, guard.token = "podman", "fresh"
                guard.path = Path(temporary) / "ledger.json"
                guard.deadline = harness.time.monotonic() + 30
                guard.baseline = {"containers": {}, "networks": {}}
                entry = {"id": "a"*64, "image": "sha256:" + "b"*64, "name": "fresh-source", "memory": 1073741824, "mounts": [], "role": "source"}
                guard.ledger = {"containers": [entry]}
                actual = {"Id": entry["id"], "Image": entry["image"], "Name": entry["name"], "Config": {"Labels": {"io.rpgmigrate.e2e": "fresh"}}, "Mounts": [],
                          "HostConfig": {"Memory": 1073741824, "NanoCpus": 2000000000, "Privileged": False, "NetworkMode": "fresh", "PortBindings": {}}, "NetworkSettings": {"Ports": {}}}
                if fault == "image": actual["Image"] = "sha256:" + "c"*64
                if fault == "label": actual["Config"]["Labels"] = {}
                calls = []
                def command(argv, **kwargs):
                    calls.append(argv)
                    if argv[1] == "inspect":
                        if fault == "unavailable": raise RuntimeError("unavailable")
                        return json.dumps([actual]).encode()
                    if argv[1] == "rm" and fault == "remove": raise RuntimeError("failed removal")
                    return b""
                def absent(*args, **kwargs):
                    return subprocess.CompletedProcess(args[0], 125 if fault == "absence" else 1)
                with patch.object(harness, "run", side_effect=command), patch.object(harness.subprocess, "run", side_effect=absent):
                    guard.cleanup_container(entry)
                removed = any(argv[1] == "rm" for argv in calls)
                self.assertEqual(removed, fault not in ("image", "label", "unavailable"))
                self.assertTrue(all("--volumes" not in argv for argv in calls))
                self.assertEqual(entry.get("cleanup_status") == "absent", fault == "ok")

    def test_expired_lease_starts_no_child_and_cleanup_deadline_never_extends(self):
        self.assertTrue(hasattr(harness, "RuntimeGuard"), "missing fixed runtime deadline")
        guard = harness.RuntimeGuard.__new__(harness.RuntimeGuard)
        guard.work_deadline = harness.time.monotonic() - 1
        guard.deadline = guard.work_deadline + 300
        with patch.object(harness.subprocess, "Popen") as launch, self.assertRaisesRegex(RuntimeError, "deadline"):
            guard.execute(["must-not-start"])
        launch.assert_not_called()


    def test_outer_uses_guard_and_refuses_unadmitted_work_before_build_for_both_profiles(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        try:
            for profile in ("disabled", "m8"):
                args = types.SimpleNamespace(pair="16-16", receipt=None, report_probe=None, profile=profile)
                with self.subTest(profile=profile), tempfile.TemporaryDirectory() as temporary:
                    (Path(temporary) / "source").write_text("fixture")
                    with patch.object(harness, "ROOT", Path(temporary)), patch.object(harness.shutil, "which", return_value="/fake/podman"), \
                            patch.object(harness.RuntimeGuard, "__init__", side_effect=RuntimeError("admission closed")) as admission, \
                            patch.object(harness, "run", side_effect=RuntimeError("unadmitted subprocess")):
                        with self.assertRaisesRegex(RuntimeError, "admission closed"):
                            harness.outer(args)
                        admission.assert_called_once()
        finally:
            sys.path.pop(0)


    def test_retained_inspection_projection_excludes_environment_errors_and_history_commands(self):
        self.assertTrue(callable(getattr(harness, "container_snapshot", None)), "missing closed safe projection")
        actual = {"Id": "a"*64, "Name": "old", "Created": "today", "Image": "sha256:" + "b"*64, "RestartCount": 0,
                  "Config": {"Env": ["PASSWORD=must-not-persist"], "Cmd": ["private-row"], "Labels": {"foreign": "must-not-persist"}},
                  "HostConfig": {"Memory": 1024, "LogConfig": {"secret": "must-not-persist"}}, "Mounts": [],
                  "State": {"Running": True, "Status": "running", "Pid": 42, "StartedAt": "now", "FinishedAt": "zero", "Error": "must-not-persist"},
                  "NetworkSettings": {"Ports": {}}}
        value = harness.container_snapshot(actual)
        self.assertEqual(value["id"], actual["Id"])
        self.assertEqual(value["state"]["Pid"], 42)
        self.assertNotIn("must-not-persist", json.dumps(value))
        self.assertNotIn("private-row", json.dumps(value))


    def test_actual_outer_argv_private_image_no_volumes_and_failure_cleanup(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import copy_features
        try:
            for profile, fault, location in (("disabled", None, "default"), ("disabled", None, "root"), ("disabled", None, "nested"),
                                             ("m8", None, "cache"), ("m8", "runner", "cache"), ("m8", "data_directory", "cache"), ("m8", "foreign_private", "cache"),
                                             ("disabled", None, "native")):
                with self.subTest(profile=profile, fault=fault, location=location), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary).resolve(); (root / "tools/e2e").mkdir(parents=True)
                    (root / ".env").write_text("must-not-persist")
                    (root / "tools/e2e/Containerfile").write_text("FROM example.invalid/rust@sha256:" + "6"*64 + "\n")
                    model = ModelRuntime(root, fault, native=location == "native")
                    output = root / ({"default": "target/e2e/16-16.json", "root": "result.json", "nested": "nested/result.json", "cache": "target/e2e/result.json",
                                      "native": "target/e2e/result.json"}[location])
                    args = types.SimpleNamespace(pair="16-16", receipt=None if location == "default" else str(output), report_probe=None, profile=profile)
                    def execute(guard, command, **kwargs):
                        guard.checkpoint()
                        result = model.call(command, **kwargs)
                        if command[1] == "start" and "--attach" in command:
                            guard.sample()
                        return result
                    with patch.object(harness, "ROOT", root), patch.object(harness, "DIAGNOSTIC_ROOT", root / "target/e2e/failures"), \
                            patch.object(harness.shutil, "which", return_value="/fake/podman"), patch.object(harness.shutil, "disk_usage", return_value=types.SimpleNamespace(free=57*1024**3)), \
                            patch.object(harness, "run", side_effect=model.call), patch.object(harness.subprocess, "run", side_effect=model.subprocess), \
                            patch.object(harness.RuntimeGuard, "execute", execute), patch.object(copy_features, "validate_receipt"):
                        if fault:
                            with self.assertRaises((RuntimeError, OSError)): harness.outer(args, receipt_validator=lambda *_: None)
                        else:
                            harness.outer(args, receipt_validator=lambda *_: None)
                    self.assertFalse(model.containers)
                    self.assertFalse(model.networks)
                    self.assertEqual(len(model.removed), 2 if fault == "data_directory" else 4)
                    builds = [argv for argv in model.commands if len(argv)>1 and argv[1] == "build" and "--tag" in argv]
                    self.assertEqual(len(builds), 1)
                    self.assertNotIn("localhost/rpgmigrate-m2-dev:stock", builds[0])
                    self.assertIn("--cpu-quota", builds[0])
                    creates = [argv for argv in model.commands if len(argv)>1 and argv[1] == "create"]
                    self.assertTrue(creates)
                    for argv in creates:
                        self.assertIn("--image-volume=ignore", argv)
                        self.assertNotIn("--privileged", argv)
                        self.assertNotIn("--volumes", argv)
                        self.assertNotIn(str(root / "target/e2e/build")+":/build", argv)
                        name = argv[argv.index("--name")+1]
                        if name.endswith(("-source", "-target")):
                            self.assertEqual("wal_level=logical" in argv, profile == "m8")
                    token = builds[0][builds[0].index("--label")+1].split("=", 1)[1]
                    ledger_path = output.parent / (output.name + "." + token + ".runtime.json")
                    self.assertEqual(model.build_ledger_path, ledger_path)
                    ledger = json.loads(ledger_path.read_text())
                    self.assertTrue(ledger["cleanup_complete"])
                    self.assertEqual(ledger["runtime_api"]["machine"], location != "native")
                    self.assertEqual(ledger["runtime_api"]["server"], "4.9.3" if location == "native" else "5.8.3")
                    self.assertEqual(any(argv[1:2] == ["machine"] for argv in model.commands), location != "native")
                    self.assertEqual(["getconf", "_NPROCESSORS_ONLN"] in model.commands, location == "native")
                    self.assertEqual(ledger["private_removed"], fault != "foreign_private")
                    self.assertEqual(ledger["secret_file_absent"], fault != "foreign_private")
                    if fault == "foreign_private":
                        private = Path(ledger["private_path"])
                        self.assertEqual((private / "foreign").read_text(), "preserve foreign leaf")
                        original = private.with_name(private.name + ".retained").lstat()
                        self.assertEqual(ledger["private_identity"]["inode"], original.st_ino)
                        self.assertNotEqual(ledger["private_identity"]["inode"], private.lstat().st_ino)
                    self.assertNotIn("must-not-persist", json.dumps(ledger))
                    self.assertNotIn("private-never-persist", json.dumps(ledger))
                    self.assertEqual(output.exists(), fault is None)
        finally:
            sys.path.pop(0)


    def test_unknown_runtime_locality_or_storage_refuses_admission(self):
        for fault, native in (("locality", False), ("storage", True)):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                model, path = ModelRuntime(Path(temporary), fault, native=native), Path(temporary) / "ledger.json"
                with patch.object(harness, "run", side_effect=model.call), self.assertRaisesRegex(RuntimeError, "host locality"):
                    harness.RuntimeGuard("podman", "fresh", path)
                self.assertTrue(json.loads(path.read_text())["admission_failed"])
                self.assertEqual([argv[1] for argv in model.commands], ["version", "info"])


    def test_source_payload_contains_only_bounded_tracked_regular_archive_files(self):
        self.assertTrue(callable(getattr(harness, "materialize_source", None)), "missing tracked source materialization")
        def archive(name, kind=tarfile.REGTYPE):
            stream = io.BytesIO()
            with tarfile.open(fileobj=stream, mode="w") as output:
                entry = tarfile.TarInfo(name); entry.type = kind; entry.mode = 0o755; entry.size = 6 if kind == tarfile.REGTYPE else 0
                output.addfile(entry, io.BytesIO(b"source") if kind == tarfile.REGTYPE else None)
            return stream.getvalue()
        for name, kind in (("src/main.rs", tarfile.REGTYPE), ("../escape", tarfile.REGTYPE), (".git/config", tarfile.REGTYPE), ("link", tarfile.SYMTYPE)):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                destination = Path(temporary) / "source"
                blob = __import__("hashlib").sha1(b"blob 6\0source").hexdigest()
                def git(command, **kwargs):
                    return ("100755 blob " + blob + " 6\tsrc/main.rs\0").encode() if "ls-tree" in command else archive(name, kind)
                with patch.object(harness, "run", side_effect=git):
                    if name == "src/main.rs":
                        harness.materialize_source(Path(temporary), destination)
                        self.assertEqual((destination / name).read_bytes(), b"source")
                        self.assertEqual(stat.S_IMODE((destination / name).stat().st_mode), 0o755)
                        self.assertFalse((destination / ".git").exists())
                    else:
                        with self.assertRaises(RuntimeError): harness.materialize_source(Path(temporary), destination)


    def test_source_export_refuses_export_ignore_substitution_and_lfs_pointer(self):
        for content, omitted in ((b"changed", False), (b"version https://git-lfs.github.com/spec/v1\noid sha256:abc\nsize 1\n", False), (b"source", True)):
            with self.subTest(content=content, omitted=omitted), tempfile.TemporaryDirectory() as temporary:
                stream = io.BytesIO()
                with tarfile.open(fileobj=stream, mode="w") as output:
                    if not omitted:
                        member = tarfile.TarInfo("file"); member.mode = 0o644; member.size = len(content)
                        output.addfile(member, io.BytesIO(content))
                blob_content = content if content.startswith(b"version ") else b"source"
                blob = __import__("hashlib").sha1(b"blob " + str(len(blob_content)).encode() + b"\0" + blob_content).hexdigest()
                def git(command, **kwargs):
                    return ("100644 blob " + blob + " " + str(len(blob_content)) + "\tfile\0").encode() if "ls-tree" in command else stream.getvalue()
                with patch.object(harness, "run", side_effect=git), self.assertRaises(RuntimeError):
                    harness.materialize_source(Path(temporary), Path(temporary) / "payload")


    def test_cgroup_limits_require_live_exact_owned_process_and_hard_cpu_memory_caps(self):
        self.assertTrue(hasattr(harness.RuntimeGuard, "observe_caps"), "missing effective cgroup admission")
        entry = {"id": "a"*64, "name": "fresh-source", "role": "source", "image": "sha256:"+"b"*64, "memory": 1073741824, "mounts": []}
        actual = {"Id": entry["id"], "Name": entry["name"], "Image": entry["image"], "State": {"Running": True}, "Config": {"Labels": {"io.rpgmigrate.e2e": "fresh"}},
                  "Mounts": [], "HostConfig": {"Memory": 1073741824, "NanoCpus": 2000000000, "Privileged": False, "NetworkMode": "bridge", "PortBindings": {}}, "NetworkSettings": {"Ports": {}}}
        for limits in (b"200000 100000\n1073741824\n", b"max 100000\n1073741824\n", b"200000 100000\nmax\n", b"400000 100000\n1073741824\n", b"200000 100000\n4294967296\n", b""):
            guard = harness.RuntimeGuard.__new__(harness.RuntimeGuard)
            guard.token, guard.baseline = "fresh", {"containers": {}}
            with self.subTest(limits=limits), patch.object(guard, "inspect", return_value=actual), patch.object(guard, "raw", return_value=limits):
                if limits.startswith(b"200000 100000\n1073741824"):
                    guard.observe_caps(entry)
                    self.assertTrue(entry["cgroup_verified"])
                else:
                    with self.assertRaises(RuntimeError): guard.observe_caps(entry)


    def test_network_cleanup_refuses_foreign_attachment_wrong_identity_and_unavailable_absence(self):
        for fault in ("id", "label", "inspect", "foreign_map", "foreign_filter", "remove", "absence", "ok"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                guard = harness.RuntimeGuard.__new__(harness.RuntimeGuard)
                guard.token, guard.path, guard.baseline = "fresh", Path(temporary) / "ledger.json", {"networks": {}}
                entry = {"id": "a"*64, "name": "fresh"}
                guard.ledger = {"network": entry}
                actual = {"id": "a"*64, "name": "fresh", "internal": True, "labels": {"io.rpgmigrate.e2e": "fresh"}, "containers": {}}
                if fault == "id": actual["id"] = "b"*64
                if fault == "label": actual["labels"].clear()
                if fault == "foreign_map": actual["containers"] = {"foreign": {}}
                calls = []
                def raw(arguments, **kwargs):
                    calls.append(arguments)
                    if arguments[:2] == ["network", "inspect"]:
                        if fault == "inspect": raise RuntimeError("unavailable")
                        return json.dumps([actual]).encode()
                    if arguments[0] == "ps": return b"foreign-id\n" if fault == "foreign_filter" else b""
                    if arguments[:2] == ["network", "rm"] and fault == "remove": raise RuntimeError("removal failed")
                    return b""
                def exists(*_):
                    if fault == "absence": raise RuntimeError("absence unavailable")
                    return False
                with patch.object(guard, "raw", side_effect=raw), patch.object(guard, "exists", side_effect=exists):
                    guard.cleanup_network()
                removed = [command for command in calls if command[:2] == ["network", "rm"]]
                self.assertEqual(bool(removed), fault in ("remove", "absence", "ok"))
                self.assertEqual(entry["cleanup_status"] == "absent", fault == "ok")
                self.assertTrue(all("--force" not in command for command in calls))

    def test_only_independently_recorded_builtin_network_created_field_can_vary(self):
        before = {"containers": {}, "external": [], "images": {}, "volumes": {}, "networks": {"a"*64: {"name": "podman", "id": "a"*64, "driver": "bridge", "internal": False, "created": "first"}, "b"*64: {"name": "foreign", "created": "stable"}}}
        for fault in (None, "driver", "foreign", "identity"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                after = copy.deepcopy(before); after["networks"]["a"*64]["created"] = "third"
                if fault == "driver": after["networks"]["a"*64]["driver"] = "changed"
                if fault == "foreign": after["networks"]["b"*64]["created"] = "changed"
                if fault == "identity": after["networks"].pop("a"*64)
                guard = harness.RuntimeGuard.__new__(harness.RuntimeGuard)
                guard.baseline, guard.path = before, Path(temporary) / "ledger.json"
                guard.ledger = {"containers": [], "builtin_network_created_observations": [{"id": "a"*64, "before": "first", "after": "second"}]}
                with patch.object(guard, "inventory", return_value=after): guard.close()
                self.assertEqual(guard.ledger["preservation_verified"], fault is None)
                self.assertEqual(guard.ledger["after"], after)

    def test_total_work_deadline_kills_only_owned_child_and_keeps_cleanup_reserve(self):
        guard = harness.RuntimeGuard.__new__(harness.RuntimeGuard)
        guard.work_deadline = harness.time.monotonic() + .1
        guard.deadline = guard.work_deadline + 300
        guard.last_sample = harness.time.monotonic()
        guard.ledger = {"processes": []}
        started = harness.time.monotonic()
        with patch.object(guard, "persist"), self.assertRaisesRegex(RuntimeError, "deadline"):
            guard.execute([sys.executable, "-c", "import time;time.sleep(30)"], timeout=60)
        self.assertEqual(len(guard.ledger["processes"]), 1)
        self.assertIsInstance(guard.ledger["processes"][0]["pid"], int)
        self.assertIsNotNone(guard.ledger["processes"][0]["returncode"])
        self.assertNotIn("argv", guard.ledger["processes"][0])
        self.assertLess(harness.time.monotonic() - started, 3)
        self.assertGreater(guard.deadline - harness.time.monotonic(), 295)


    def test_missing_pinned_ref_image_collision_dirty_source_and_capacity_refuse_without_effects(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        try:
            for fault in ("missing_ref", "image_collision", "dirty", "capacity"):
                with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary).resolve(); (root / "tools/e2e").mkdir(parents=True)
                    (root / ".env").write_text("must-not-persist")
                    (root / "tools/e2e/Containerfile").write_text("FROM example.invalid/rust@sha256:" + "6"*64 + "\n")
                    model = ModelRuntime(root, fault)
                    args = types.SimpleNamespace(pair="16-16", receipt=str(root / "target/e2e/result.json"), report_probe=None, profile="m8")
                    def execute(guard, command, **kwargs):
                        return model.call(command, **kwargs)
                    with patch.object(harness, "ROOT", root), patch.object(harness, "DIAGNOSTIC_ROOT", root / "target/e2e/failures"), \
                            patch.object(harness.shutil, "which", return_value="/fake/podman"), patch.object(harness.shutil, "disk_usage", return_value=types.SimpleNamespace(free=57*1024**3)), \
                            patch.object(harness, "run", side_effect=model.call), patch.object(harness.subprocess, "run", side_effect=model.subprocess), patch.object(harness.RuntimeGuard, "execute", execute):
                        with self.assertRaises(RuntimeError): harness.outer(args)
                    self.assertFalse(Path(args.receipt).exists())
                    self.assertFalse(model.containers)
                    self.assertFalse(model.networks)
                    self.assertFalse(any(len(command)>1 and command[1] in ("pull", "build", "create", "start", "rm") and "--help" not in command for command in model.commands))
                    ledger = json.loads(next((root / "target/e2e").glob("*.runtime.json")).read_text())
                    if fault == "capacity": self.assertTrue(ledger["admission_failed"])
                    else:
                        self.assertTrue(ledger["cleanup_complete"])
                        self.assertTrue(ledger["private_removed"])
        finally:
            sys.path.pop(0)


    def test_actual_addressless_pasta_shape_is_build_only_and_closed(self):
        entry = {"id": "a"*64, "image": "sha256:"+"b"*64, "name": "fresh-build", "role": "build", "memory": 4*1024**3, "mounts": []}
        actual = {"Id": entry["id"], "Image": "b"*64, "Name": entry["name"], "Config": {"Labels": {"io.rpgmigrate.e2e": "fresh"}}, "Mounts": [],
                  "HostConfig": {"Memory": entry["memory"], "NanoCpus": 2000000000, "CpuPeriod": 100000, "CpuQuota": 200000, "Privileged": False, "NetworkMode": "pasta", "PortBindings": {}},
                  "NetworkSettings": {"Ports": {}, "Networks": pasta_placeholder()}}
        for networks in (pasta_placeholder(), {}, None):
            with self.subTest(accepted=networks):
                admitted = True
                try:
                    harness.validate_owned_container({**actual, "NetworkSettings": {"Ports": {}, "Networks": networks}}, entry, "fresh", set())
                except RuntimeError:
                    admitted = False
                self.assertTrue(admitted, "actual addressless rootless pasta placeholder must be admitted")
        for key, value in (("EndpointID", "foreign"), ("Gateway", "10.88.0.1"), ("IPAddress", "10.88.0.2"), ("IPPrefixLen", 24),
                           ("IPv6Gateway", "::1"), ("GlobalIPv6Address", "::2"), ("GlobalIPv6PrefixLen", 64), ("MacAddress", "aa:bb:cc:dd:ee:ff"),
                           ("NetworkID", "c"*64), ("DriverOpts", {}), ("IPAMConfig", {}), ("Links", []), ("Aliases", None), ("IPPrefixLen", False)):
            bad = copy.deepcopy(actual); bad["NetworkSettings"]["Networks"]["pasta"][key] = value
            with self.subTest(endpoint_key=key), self.assertRaises(RuntimeError):
                harness.validate_owned_container(bad, entry, "fresh", set())
        for key in pasta_placeholder()["pasta"]:
            bad = copy.deepcopy(actual); del bad["NetworkSettings"]["Networks"]["pasta"][key]
            with self.subTest(missing=key), self.assertRaises(RuntimeError):
                harness.validate_owned_container(bad, entry, "fresh", set())
        for change in (lambda x: x["HostConfig"].update(NetworkMode="bridge"),
                       lambda x: x["NetworkSettings"]["Networks"].update(foreign=pasta_placeholder()["pasta"]),
                       lambda x: x["NetworkSettings"].update(Networks={"foreign": pasta_placeholder()["pasta"]}),
                       lambda x: x["NetworkSettings"].update(Ports={"5432/tcp": [{"HostPort": "12345"}]}),
                       lambda x: x["HostConfig"].update(Memory=0), lambda x: x["HostConfig"].update(NanoCpus=0, CpuQuota=0),
                       lambda x: x["Mounts"].append({"Type": "volume", "Source": "foreign", "Destination": "/data", "RW": True}),
                       lambda x: x["Config"]["Labels"].update({"io.rpgmigrate.e2e": "foreign"})):
            bad = copy.deepcopy(actual); change(bad)
            with self.subTest(changed=bad), self.assertRaises(RuntimeError):
                harness.validate_owned_container(bad, entry, "fresh", set())
        with self.assertRaises(RuntimeError):
            harness.validate_owned_container(actual, entry, "fresh", {entry["id"]})
        with self.assertRaises(RuntimeError):
            harness.validate_owned_container(actual, {**entry, "role": "source", "network": {"name": "fresh", "id": "c"*64}}, "fresh", set())


    def test_existing_private_leaf_is_refused_without_adopting_identity_or_mutation(self):
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve(); (root / "tools/e2e").mkdir(parents=True)
                (root / "tools/e2e/Containerfile").write_text("FROM example.invalid/rust@sha256:" + "6"*64 + "\n")
                private = root / "target/e2e" / ("rpgmigrate-m8-" + "f"*32 + ".private")
                private.mkdir(mode=0o700, parents=True); (private / "foreign").write_text("preserve stale leaf")
                original = private.lstat()
                model = ModelRuntime(root)
                args = types.SimpleNamespace(pair="16-16", receipt=str(root / "target/e2e/result.json"), report_probe=None, profile="m8")
                with patch.object(harness, "ROOT", root), patch.object(harness.uuid, "uuid4", return_value=types.SimpleNamespace(hex="f"*32)), \
                        patch.object(harness.shutil, "which", return_value="/fake/podman"), patch.object(harness.shutil, "disk_usage", return_value=types.SimpleNamespace(free=57*1024**3)), \
                        patch.object(harness, "run", side_effect=model.call), patch.object(harness.subprocess, "run", side_effect=model.subprocess), \
                        self.assertRaisesRegex(RuntimeError, "private path already exists"):
                    harness.outer(args)
                self.assertEqual((private / "foreign").read_text(), "preserve stale leaf")
                self.assertEqual(private.lstat().st_ino, original.st_ino)
                ledger = json.loads(next((root / "target/e2e").glob("*.runtime.json")).read_text())
                self.assertNotIn("private_identity", ledger)
                self.assertFalse(any(len(command)>1 and command[1] in ("create", "start", "rm", "pull") for command in model.commands))
        finally:
            sys.path.pop(0)


    def test_network_descriptor_ids_are_the_protected_inventory_identity(self):
        expected = {"id": "a"*64, "name": "fresh"}
        actual = {"id": expected["id"], "name": "fresh", "internal": True, "labels": {"io.rpgmigrate.e2e": "fresh"}, "containers": {}}
        for before in ({"kind": {"id": "a"*64, "name": "kind"}}, {"kind": {"name": "kind"}}):
            with self.subTest(before=before), self.assertRaises(RuntimeError):
                harness.validate_owned_network(actual, expected, "fresh", before, empty=True)
        harness.validate_owned_network(actual, expected, "fresh", {"kind": {"id": "b"*64, "name": "kind"}}, empty=True)
        with self.subTest(inventory="daemon_names"), tempfile.TemporaryDirectory() as temporary:
            model = ModelRuntime(Path(temporary))
            model.networks["b"*64] = {"id": "b"*64, "name": "kind", "internal": False, "driver": "bridge", "labels": {}, "containers": {}}
            guard = harness.RuntimeGuard.__new__(harness.RuntimeGuard)
            guard.runtime, guard.deadline = "podman", harness.time.monotonic() + 60
            with patch.object(harness, "run", side_effect=model.call):
                before = guard.inventory()
            self.assertEqual(set(before["networks"]), {"b"*64})
            self.assertEqual(before["networks"]["b"*64]["name"], "kind")


    def test_model_build_uses_only_one_ledger_bound_to_its_run_and_private_path(self):
        for fault in (None, "missing", "wrong_run", "wrong_private", "ambiguous"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                token, old_token = "rpgmigrate-m8-" + "f"*32, "rpgmigrate-m8-" + "e"*32
                private = root / "target/e2e" / (token + ".private")
                private.mkdir(mode=0o700, parents=True)
                info = private.lstat()
                identity = {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid, "mode": stat.S_IMODE(info.st_mode), "nonce": "f"*32, "absent_before": True}
                if hasattr(info, "st_birthtime"): identity["birthtime"] = info.st_birthtime
                record = {"run": token, "private_path": str(private), "private_identity": identity}
                decoy = root / "target/e2e" / ("stale.json." + old_token + ".runtime.json")
                decoy.write_text(json.dumps({**record, "run": old_token, "private_identity": {**identity, "nonce": "e"*32}})); decoy.chmod(0o600)
                chosen = root / "nested" / ("explicit.json." + token + ".runtime.json")
                chosen.parent.mkdir()
                if fault != "missing":
                    value = copy.deepcopy(record)
                    if fault == "wrong_run": value["run"] = old_token
                    if fault == "wrong_private": value["private_path"] = str(root / "foreign")
                    chosen.write_text(json.dumps(value)); chosen.chmod(0o600)
                if fault == "ambiguous":
                    copied = root / ("copied.json." + token + ".runtime.json")
                    copied.write_text(json.dumps(record)); copied.chmod(0o600)
                model = ModelRuntime(root)
                command = ["podman", "build", "--tag", "localhost/fresh:dev", "--label", "io.rpgmigrate.e2e=" + token,
                           "--file", str(private / "source/tools/e2e/Containerfile")]
                if fault:
                    with self.assertRaises(AssertionError): model.call(command)
                    self.assertNotIn("localhost/fresh:dev", model.references)
                else:
                    model.call(command)
                    self.assertEqual(getattr(model, "build_ledger_path", None), chosen)


if __name__ == "__main__":
    unittest.main()
