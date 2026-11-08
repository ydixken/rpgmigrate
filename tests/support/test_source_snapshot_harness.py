"""Run with python3 -W error tests/support/test_source_snapshot_harness.py."""

from contextlib import contextmanager, redirect_stderr, redirect_stdout
import copy
import io
import json
import os
from pathlib import Path
import runpy
import select
import socket
import struct
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from db_harness import OwnedCluster, expected_version, load_manifest, module_cases
from protocol_fault import packet

SUPPORT = Path(__file__).resolve().parent
WRAPPER = SUPPORT.parents[1] / "tools" / "test-db.py"
CASES = [
    "snapshot::live_tests::live_owned_snapshot",
    "snapshot::live_tests::live_empty_snapshot",
    "snapshot::live_tests::live_existing_slot_refused",
    "snapshot::live_tests::live_control_permission_denied",
    "snapshot::live_tests::live_wrong_source_refused",
    "snapshot::live_tests::live_reused_intent_refused",
    "snapshot::live_tests::live_create_reply_lost",
    "snapshot::live_tests::live_create_tail_lost",
    "snapshot::live_tests::live_expired_export_refused",
    "snapshot::live_tests::live_imported_identity_mismatch",
    "snapshot::live_tests::live_source_finish_unknown",
    "snapshot::live_tests::live_application_name_preserved",
]
FAULTS = ("create_reply_lost", "create_tail_lost", "expired_export_refused",
          "imported_identity_mismatch", "source_finish_unknown")
COMMAND = ["cargo", "test", "--locked", "--lib", "snapshot::live_tests::",
           "--", "--ignored", "--test-threads=1"]
RECORDS = "".join(f"test {name} ... ok\n" for name in CASES)
SUMMARY = "test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 17 filtered out; finished in 0.01s\n"
OUTPUT = RECORDS + SUMMARY


def run_create_controller(case, phase):
    script = '''import json,sys,threading
sys.path.insert(0,sys.argv.pop(1))
import source_snapshot_fault as fault
fault.assert_identity=lambda context: None
def psql(context,database,sql):
    if "json_build_object" in sql:
        slot=context["slots"]["live_"+sys.argv[sys.argv.index("--case")+1]]
        phase=sys.argv[sys.argv.index("--phase")+1] if "--phase" in sys.argv else ""
        if phase in ("ready_transaction","ready_failed"):
            slot += "_t" if phase=="ready_transaction" else "_e"
        return json.dumps({"name":slot,"plugin":"test_decoding","database":context["database"],"temporary":False,"consistent_point":"0/10"})
    assert "FROM pg_stat_activity WHERE pid=" in sql, "unexpected offline SQL"
    return ""
fault.psql=psql
fault.main()
assert all(thread is threading.current_thread() or not thread.is_alive() for thread in threading.enumerate()), "offline relay thread leak"
'''

    def frame(kind, payload):
        return kind + struct.pack("!I", len(payload) + 4) + payload

    with tempfile.TemporaryDirectory(prefix="ss-controller-", dir="/tmp") as directory:
        root = Path(directory)
        context = {"root": directory, "socket_dir": directory, "port": 23456, "token": "owned-offline",
                   "database": "snapshot_source", "database_oid": 42, "system_identifier": "123",
                   "server_version_num": 140024, "expected_server_version": "14.24", "expected_server_version_num": 140024,
                   "replication_role": "snapshot_fixture", "slots": {"live_" + case: "ss_owned"}}
        record = fault_record(context, case, phase)
        if phase in ("ready_transaction", "ready_failed"):
            phases = ("tail_lost", "ready_transaction", "ready_failed")
            previous = {name: fault_record(context, case, name) for name in phases[:phases.index(phase)]}
            (root / "snapshot-fault-create_tail_lost.json").write_text(json.dumps(
                {"owner": context["token"], "case": case, "status": "pending",
                 "fault_hits": len(previous), "phases": previous}))
        context_path = root / "context.json"
        context_path.write_text(json.dumps(context))
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(root / ".s.PGSQL.23456"))
        listener.listen(1)
        listener.settimeout(5)
        errors = []

        def peer():
            try:
                with listener.accept()[0] as server:
                    server.settimeout(5)
                    packet(server, typed=False)
                    server.sendall(frame(b"K", struct.pack("!II", 101, 1)) + frame(b"Z", b"I"))
                    kind, _raw, payload = packet(server)
                    assert kind == b"Q" and payload.startswith(b"CREATE_REPLICATION_SLOT")
                    fields = [record["slot"]["name"].encode("ascii"), b"0/10", b"owned-token", b"test_decoding"]
                    row = struct.pack("!H", 4) + b"".join(struct.pack("!i", len(field)) + field for field in fields)
                    server.sendall(frame(b"D", row) + frame(b"C", b"CREATE_REPLICATION_SLOT\0") + frame(b"Z", b"I"))
                    assert server.recv(1) == b""
            except Exception as error:
                errors.append(repr(error))

        worker = threading.Thread(target=peer)
        worker.start()
        environment = {**os.environ, "RPGMIGRATE_TEST_SNAPSHOT_CONTEXT": str(context_path)}
        command = [sys.executable, "-W", "error", "-c", script, str(SUPPORT), "--case", case]
        if case == "create_tail_lost":
            command += ["--phase", phase]
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, env=environment)
        client = None

        def read():
            assert select.select([process.stdout], [], [], 5)[0], "offline controller response deadline"
            line = process.stdout.readline(65537)
            return json.loads(line) if line else None

        def request(value):
            process.stdin.write(json.dumps(value) + "\n")
            process.stdin.flush()
            return read()

        try:
            ready = read()
            host = ready["conninfo"].split("host='")[1].split("'", 1)[0]
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.settimeout(5)
            client.connect(str(Path(host) / ".s.PGSQL.23456"))
            client.sendall(struct.pack("!II", 8, 196608))
            packet(client)
            packet(client)
            armed = request({"op": "arm", "native_path": record["native_path"], "peer_pid": 102})
            assert armed["armed"] is True and armed["fault_hits"] == 0
            client.sendall(frame(b"Q", b"CREATE_REPLICATION_SLOT ss_owned LOGICAL test_decoding\0"))
            if phase in ("ready_transaction", "ready_failed"):
                assert packet(client)[0] == b"D"
                assert packet(client)[0] == b"C"
                kind, _raw, payload = packet(client)
                assert kind == b"Z" and payload == (b"T" if phase == "ready_transaction" else b"E")
                held = request({"op": "status"})
                assert held["fault_hits"] == 1 and held["details"]["client_closed_after_ready"] is False
            else:
                if phase == "tail_lost":
                    assert packet(client)[0] == b"D"
                assert client.recv(1) == b""
            client.close()
            failed = request({"op": "failed"})
            finished = request({"op": "quit"}) if failed is not None else None
            process.wait(timeout=5)
        finally:
            if client is not None:
                client.close()
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            stderr = process.stderr.read(16385)
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
            listener.close()
            worker.join(timeout=5)
        assert not worker.is_alive() and not errors, errors
        assert len(stderr) <= 16384, "offline helper stderr exceeds diagnostic bound"
        return process.returncode, finished, stderr


def fault_record(context, case, phase=None):
    phase = phase or case
    slot = context["slots"]["live_" + case]
    if phase in ("ready_transaction", "ready_failed"):
        slot += "_t" if phase == "ready_transaction" else "_e"
    pending = case in ("create_reply_lost", "create_tail_lost")
    nonce = [2 if phase == "ready_transaction" else 3 if phase == "ready_failed" else 1] * 16
    directory = Path(context["root"]) / ("snapshot-" + phase)
    directory.mkdir(exist_ok=True)
    native_path = directory / "manifest.json"
    intent = {"nonce": nonce, "binding": {"stream": {"run": [1] * 16, "generation": 9,
              "source": {"system_identifier": 123, "database_oid": 42}}, "slot_name": slot,
              "plugin": "test_decoding", "catalog_epoch": 7}}
    record = intent if pending else {"intent": intent, "consistent_point": 16, "snapshot_token": "owned-token"}
    native_path.write_text(json.dumps({"magic": "RPGMIGRATE", "format_version": 4, "run": [1] * 16,
        "canonical_path": str(directory), "copy_complete": None, "target_provenance": [],
        "copy_snapshot": None,
        "source_slot": {"phase": "provisioning" if pending else "confirmed", "record": record}}))
    return {"owner": context["token"], "case": case, "phase": phase, "status": "observed", "armed": True,
            "fault_hits": 1, "backend_pid": 101, "peer_pid": 102, "helper_pid": 103,
            "backend_closed": True, "peer_closed": True, "client_failed": True,
            "fault_before_deadline": True, "failure_before_deadline": True, "deadline_seconds": 20,
            "helper_reaped": True, "helper_returncode": 0,
            "slot": {"name": slot, "plugin": "test_decoding", "database": context["database"],
                     "temporary": False, "consistent_point": "0/10"},
            "native_path": str(native_path), "native_nonce": nonce,
            "native_phase": "provisioning" if pending else "confirmed", "intent_before_create": pending,
            "details": {"row_seen": True, "completion_seen": True, "command_tag": "CREATE_REPLICATION_SLOT" if pending else "COMMIT",
                        "row_delivered": case == "create_tail_lost", "ready_before": "I",
                        "ready_after": "T" if phase == "ready_transaction" else "E" if phase == "ready_failed" else None,
                        "client_closed_after_ready": True, "import_seen": True, "exporter_expired": True,
                        "expected_sqlstate": "42704" if context["server_version_num"] == 170011 else "22023",
                        "sqlstate": "42704" if context["server_version_num"] == 170011 else "22023",
                        "identity_before": 101, "identity_after": 201}}


class SourceSnapshotHarnessTests(unittest.TestCase):
    def test_lost_create_controller_stops_upstream_and_returns_quit_receipt(self):
        for case, phase in (("create_reply_lost", "create_reply_lost"), ("create_tail_lost", "tail_lost")):
            with self.subTest(case=case):
                status, receipt, stderr = run_create_controller(case, phase)
                self.assertEqual(status, 0, stderr)
                self.assertIsNotNone(receipt, stderr)
                self.assertEqual(receipt["status"], "observed")
                self.assertEqual(receipt["fault_hits"], 1)
                self.assertTrue(receipt["backend_closed"])
                self.assertTrue(receipt["intent_before_create"])

    def test_ready_controller_observes_client_close_before_stopping_upstream(self):
        for phase, byte in (("ready_transaction", "T"), ("ready_failed", "E")):
            with self.subTest(phase=phase):
                status, receipt, stderr = run_create_controller("create_tail_lost", phase)
                self.assertEqual(status, 0, stderr)
                self.assertIsNotNone(receipt, stderr)
                self.assertEqual(receipt["status"], "observed")
                self.assertEqual(receipt["details"]["ready_before"], "I")
                self.assertEqual(receipt["details"]["ready_after"], byte)
                self.assertTrue(receipt["details"]["client_closed_after_ready"])

    def test_snapshot_fixture_provisions_cap_relation_for_the_ordinary_role(self):
        from source_snapshot_fixture import setup_source
        context = {"token": "owned-snapshot"}
        calls = []

        def psql(actual, database, sql, *, role="e1_admin"):
            self.assertIs(actual, context)
            calls.append((database, sql, role))
            if "shobj_description" in sql:
                return context["token"]
            if "json_build_array(rolreplication" in sql:
                return json.dumps([True, False, False, False, False, False, False, database != "snapshot_denied"])
            if "snapshot_plugin_probe" in sql:
                return "snapshot_plugin_probe\ntest_decoding"
            if "count(*) FROM pg_replication_slots" in sql:
                return "0"
            return ""

        with patch("source_snapshot_fixture.psql", psql), \
                patch("source_snapshot_fixture.assert_identity", lambda _context: None):
            setup_source(SimpleNamespace(context=context), "snapshot_nonce", controls=True)
        cap = [sql for database, sql, role in calls
               if database == "snapshot_source" and "CREATE TABLE public.snapshot_cap_relation" in sql]
        self.assertEqual(len(cap), 1, "missing owned primary-key relation for the real capability check")
        self.assertIn("PRIMARY KEY", cap[0])
        self.assertIn('GRANT SELECT ON public.snapshot_cap_relation TO "snapshot_nonce"', cap[0])
        self.assertTrue(all(role == "e1_admin" or role == "snapshot_nonce" for _, _, role in calls))

    def fault_context(self, directory):
        return {"root": directory, "token": "owned-snapshot", "database": "snapshot_source",
                "system_identifier": "123", "database_oid": 42, "server_version_num": 140024,
                "expected_server_version": "14.24", "expected_server_version_num": 140024,
                "slots": {"live_" + case: "ss_owned_" + str(index) for index, case in enumerate(FAULTS)}}

    def test_expiry_forwards_the_declared_profile_error_and_ready(self):
        from source_snapshot_fault import SnapshotFault
        from source_snapshot_fixture import validate_phase
        profiles = (("14.24", 140024, "22023"), ("17.11", 170011, "42704"))
        for version, number, expected in profiles:
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                context = self.fault_context(directory)
                context.update(expected_server_version=version, expected_server_version_num=number, server_version_num=number)
                relay = SnapshotFault(context, "expired_export_refused", "expired_export_refused")
                relay.armed.set()
                relay.deadline = float("inf")
                relay.details["exporter_expired"] = True
                payload = b"SERROR\0VERROR\0C" + expected.encode("ascii") + b"\0Mowned expiry\0\0"
                frame = b"E" + struct.pack("!I", len(payload) + 4) + payload
                self.assertEqual(relay.server_frame(b"E", frame, payload), frame)
                self.assertEqual(relay.details["expected_sqlstate"], expected)
                self.assertEqual(relay.details["sqlstate"], expected)
                ready = b"Z" + struct.pack("!I", 5) + b"E"
                self.assertEqual(relay.server_frame(b"Z", ready, b"E"), ready)
                self.assertEqual(relay.hits, 1)
                self.assertFalse(relay.close_after_frame)
                self.assertFalse(relay.stopping.is_set())
                proof = fault_record(context, "expired_export_refused")
                validate_phase(proof, context, "expired_export_refused", "expired_export_refused")

    def test_expiry_rejects_cross_profile_codes_io_and_incomplete_receipts(self):
        from source_snapshot_fault import SnapshotFault
        from source_snapshot_fixture import validate_phase
        for version, number, expected, wrong in (("14.24", 140024, "22023", "42704"),
                                                 ("17.11", 170011, "42704", "22023")):
            with self.subTest(version=version), tempfile.TemporaryDirectory() as directory:
                context = self.fault_context(directory)
                context.update(expected_server_version=version, expected_server_version_num=number, server_version_num=number)
                for code in (wrong, "08006", "57P01"):
                    relay = SnapshotFault(context, "expired_export_refused", "expired_export_refused")
                    relay.armed.set()
                    relay.deadline = float("inf")
                    relay.details["exporter_expired"] = True
                    payload = b"SERROR\0VERROR\0C" + code.encode("ascii") + b"\0Mowned error\0\0"
                    frame = b"E" + struct.pack("!I", len(payload) + 4) + payload
                    with self.subTest(code=code), self.assertRaises(AssertionError):
                        relay.server_frame(b"E", frame, payload)
                    self.assertEqual(relay.hits, 0)
                for code in (wrong, "08006", "PostgreSQL connection input failed", None):
                    proof = fault_record(context, "expired_export_refused")
                    proof["details"]["sqlstate"] = code
                    with self.subTest(receipt_code=code), self.assertRaises(AssertionError):
                        validate_phase(proof, context, "expired_export_refused", "expired_export_refused")
                proof = fault_record(context, "expired_export_refused")
                proof["details"]["expected_sqlstate"] = wrong
                with self.assertRaises(AssertionError):
                    validate_phase(proof, context, "expired_export_refused", "expired_export_refused")

    def test_expiry_profile_requires_qualified_declaration_matching_actual_server(self):
        try:
            from source_snapshot_fixture import expiry_sqlstate
        except ImportError:
            self.fail("declared expiry oracle is missing")
        with tempfile.TemporaryDirectory() as directory:
            original = self.fault_context(directory)
            self.assertEqual(expiry_sqlstate(original), "22023")
            for changes in ({"expected_server_version": "17.11"}, {"expected_server_version_num": 170011},
                            {"server_version_num": 170011}, {"expected_server_version_num": "140024"},
                            {"expected_server_version": "17.12", "expected_server_version_num": 170012, "server_version_num": 170012}):
                with self.subTest(changes=changes), self.assertRaises(AssertionError):
                    expiry_sqlstate({**original, **changes})
            for key in ("expected_server_version", "expected_server_version_num", "server_version_num"):
                missing = dict(original)
                del missing[key]
                with self.subTest(missing=key), self.assertRaises((AssertionError, KeyError)):
                    expiry_sqlstate(missing)

    def write_faults(self, context):
        records = {}
        for case in FAULTS:
            if case == "create_tail_lost":
                value = {"owner": context["token"], "case": case, "status": "observed", "fault_hits": 3,
                         "phases": {phase: fault_record(context, case, phase) for phase in
                                    ("tail_lost", "ready_transaction", "ready_failed")}}
            else:
                value = fault_record(context, case)
            (Path(context["root"]) / f"snapshot-fault-{case}.json").write_text(json.dumps(value))
            records[case] = value
        return records

    def test_snapshot_inventory_is_exact_and_separate(self):
        manifest = load_manifest()
        try:
            actual = module_cases(manifest, "SourceSnapshot")
        except ValueError:
            self.fail("SourceSnapshot profile is missing")
        self.assertEqual(actual, CASES)
        for profile, count in (("Catalog", 10), ("SourceCatalog", 3), ("Authority", 18),
                               ("Engine", 41), ("Replication", 9)):
            with self.subTest(profile=profile):
                names = module_cases(manifest, profile)
                self.assertEqual(len(names), count)
                self.assertTrue(set(CASES).isdisjoint(names))
        self.assertEqual(len(manifest["cases"]), 40)
        self.assertEqual(manifest["format_version"], 1)

    def test_snapshot_inventory_rejects_missing_empty_duplicate_short_and_extra(self):
        from source_snapshot_fixture import snapshot_cases
        self.assertEqual(snapshot_cases(), CASES)
        inventory = SUPPORT / "source_snapshot_cases.json"
        for value in ([], CASES[:-1], CASES + [CASES[0]], [CASES[0]] * 12,
                      CASES[:-1] + ["other::live_case"], {}, None):
            with self.subTest(value=value), patch.object(Path, "read_text", return_value=json.dumps(value)), \
                    self.assertRaises((AssertionError, ValueError)):
                snapshot_cases()
        with patch.object(Path, "read_text", side_effect=FileNotFoundError(inventory)), \
                self.assertRaises(FileNotFoundError):
            snapshot_cases()

    def test_snapshot_receipt_rejects_missing_extra_duplicate_failed_and_ignored(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        for output in ("", *(OUTPUT.replace(f"test {name} ... ok\n", "") for name in CASES),
                       OUTPUT + f"test {CASES[0]} ... ok\n", OUTPUT + "test unexpected ... ok\n",
                       OUTPUT.replace(CASES[0], "catalog::tests::live_cdc_relation_metadata"),
                       OUTPUT.replace(" ... ok", " ... FAILED", 1),
                       OUTPUT.replace(" ... ok", " ... ignored", 1)):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate("SourceSnapshot", CASES, output, {})
        for expected in ([], [CASES[0]] * 12, CASES[:-1]):
            with self.subTest(expected=expected), self.assertRaises(AssertionError):
                validate("SourceSnapshot", expected, OUTPUT, {})

    def test_snapshot_requires_one_consistent_zero_ignored_summary(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        for output in (RECORDS, OUTPUT + SUMMARY, OUTPUT.replace("12 passed", "0 passed"),
                       OUTPUT.replace("0 ignored", "1 ignored"), OUTPUT.replace("0 failed", "1 failed"),
                       OUTPUT.replace("0 measured", "1 measured"),
                       OUTPUT.replace("test result: ok", "test result: FAILED")):
            with self.subTest(output=output), self.assertRaisesRegex(AssertionError, "summary"):
                validate("SourceSnapshot", CASES, output, {})

    def test_logical_slot_capacity_is_bounded_and_default_is_unchanged(self):
        with patch("db_harness.find_bindir", return_value=Path("/owned/pg/bin")):
            self.assertEqual(OwnedCluster(load_manifest()).logical_slots, 4)
            for value in (1, 16, 64):
                self.assertEqual(OwnedCluster(load_manifest(), logical=True, logical_slots=value).logical_slots, value)
            for value in (0, 65, -1, True, False, 4.0, "16", None):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    OwnedCluster(load_manifest(), logical=True, logical_slots=value)

    @staticmethod
    def forbid_fixture(*_args, **_kwargs):
        raise AssertionError("unexpected fixture start")

    def test_snapshot_rejects_missing_command_and_mixed_profiles_before_start(self):
        main = runpy.run_path(str(WRAPPER))["main"]
        for args in (["--source-snapshot"], ["--source-snapshot", "--"],
                     *(["--source-snapshot", mode, "--", *COMMAND] for mode in
                       ("--catalog", "--source-catalog", "--authority", "--engine", "--transport",
                        "--prepared", "--replication", "--self-check"))):
            with self.subTest(args=args), patch.object(sys, "argv", [str(WRAPPER), *args]), \
                    patch.dict(main.__globals__, {"OwnedCluster": self.forbid_fixture}), \
                    redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main()
            self.assertEqual(error.exception.code, 2)

    def run_snapshot(self, directory, output=OUTPUT, returncode=0, setup_failure=False):
        main = runpy.run_path(str(WRAPPER))["main"]
        context = {"root": directory, "socket_dir": "/owned/socket", "port": 24567}
        cluster = SimpleNamespace(context=context, context_path=Path(directory) / "context.json",
                                  root=Path(directory), bindir=Path("/owned/pg/bin"),
                                  environment={"PATH": "/usr/bin"}, auth_password="synthetic")
        observed = {}

        @contextmanager
        def snapshot_fixture(manifest):
            self.assertEqual(manifest["server_version_num"], 140024)
            if setup_failure:
                raise RuntimeError("snapshot fixture privilege setup failed")
            snapshot = {"root": directory, "token": "owned-snapshot", "database": "snapshot_source"}
            observed["snapshot"] = snapshot
            yield snapshot, {f"RPGMIGRATE_TEST_SNAPSHOT_{key}": value for key, value in (
                ("CONTEXT", str(Path(directory) / "snapshot-context.json")),
                ("SQL", "owned source SQL"), ("EXPORTER", "owned source replication"),
                ("ADMIN", "owned source admin"), ("OTHER_SQL", "owned second cluster SQL"),
                ("DENIED_SQL", "owned denied SQL"), ("FAULT_HELPER", "owned fault helper"))}

        @contextmanager
        def cluster_fixture(_manifest):
            yield cluster

        def run_command(command, **kwargs):
            self.assertIn("snapshot", observed, "command started before snapshot fixture setup")
            observed.update(command=command, **kwargs)
            return subprocess.CompletedProcess(command, returncode, output, "")

        def validate_faults(actual):
            self.assertIs(actual, observed["snapshot"])
            observed["faults_checked"] = True

        replacements = {"OwnedCluster": cluster_fixture, "source_snapshot_fixture": snapshot_fixture,
                        "validate_source_snapshot_faults": validate_faults,
                        "databases": lambda *_args, **_kwargs: ("ordinary_source", "ordinary_target"),
                        "run_command": run_command, "authority_fixture": self.forbid_fixture,
                        "replication_fixture": self.forbid_fixture}
        with patch.dict(main.__globals__, replacements), \
                patch.object(sys, "argv", [str(WRAPPER), "--source-snapshot", "--", *COMMAND]), \
                redirect_stdout(io.StringIO()) as stdout:
            status = main()
        return status, observed, stdout.getvalue()

    def test_snapshot_runner_publishes_explicit_endpoints_and_observed_fault_footer(self):
        with tempfile.TemporaryDirectory() as directory:
            status, observed, output = self.run_snapshot(directory)
        self.assertEqual(status, 0)
        self.assertEqual(observed["command"], COMMAND)
        self.assertEqual(observed["timeout"], 600)
        self.assertTrue(observed["capture_output"])
        self.assertTrue(observed["faults_checked"])
        environment = observed["environment"]
        for key in ("CONTEXT", "SQL", "EXPORTER", "ADMIN", "OTHER_SQL", "DENIED_SQL", "FAULT_HELPER"):
            self.assertIn("RPGMIGRATE_TEST_SNAPSHOT_" + key, environment)
        self.assertEqual(environment["RPGMIGRATE_TEST_EXPECTED_PG_VERSION"], "14.24")
        self.assertEqual(environment["RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM"], "140024")
        self.assertIn("SourceSnapshot faults verified 5 observed cases and closed backends", output)
        self.assertIn("SourceSnapshot verified 12 exact cases; full E1 suite not selected", output)

    def test_snapshot_child_failure_and_setup_failure_cannot_report_success(self):
        with tempfile.TemporaryDirectory() as directory:
            status, observed, output = self.run_snapshot(directory, returncode=9)
            self.assertEqual(status, 9)
            self.assertNotIn("faults_checked", observed)
            self.assertNotIn("SourceSnapshot verified", output)
            with self.assertRaisesRegex(RuntimeError, "privilege setup failed"):
                self.run_snapshot(directory, setup_failure=True)

    def test_snapshot_requires_all_five_observed_faults_and_reaped_children(self):
        from source_snapshot_fixture import validate_source_snapshot_faults
        with tempfile.TemporaryDirectory() as directory:
            context = self.fault_context(directory)
            records = self.write_faults(context)
            validate_source_snapshot_faults(context)
            for case, value in records.items():
                path = Path(directory) / f"snapshot-fault-{case}.json"
                path.unlink()
                with self.subTest(case=case), self.assertRaises(AssertionError):
                    validate_source_snapshot_faults(context)
                path.write_text(json.dumps(value))
            extra = Path(directory) / "snapshot-fault-extra.json"
            extra.write_text("{}")
            with self.assertRaises(AssertionError):
                validate_source_snapshot_faults(context)
            extra.unlink()
            for key, bad in (("owner", "foreign"), ("case", "wrong"), ("status", "pending"),
                             ("armed", False), ("fault_hits", 0), ("fault_hits", True),
                             ("backend_closed", False), ("peer_closed", False), ("client_failed", False),
                             ("fault_before_deadline", False), ("failure_before_deadline", False),
                             ("helper_reaped", False), ("helper_returncode", 9),
                             ("native_phase", "confirmed"), ("intent_before_create", False)):
                value = {**records["create_reply_lost"], key: bad}
                path = Path(directory) / "snapshot-fault-create_reply_lost.json"
                path.write_text(json.dumps(value))
                with self.subTest(key=key, bad=bad), self.assertRaises(AssertionError):
                    validate_source_snapshot_faults(context)
            path.write_text(json.dumps(records["create_reply_lost"]))
            value = records["create_tail_lost"]
            path = Path(directory) / "snapshot-fault-create_tail_lost.json"
            for phase in ("tail_lost", "ready_transaction", "ready_failed"):
                corrupt = copy.deepcopy(value)
                del corrupt["phases"][phase]
                path.write_text(json.dumps(corrupt))
                with self.subTest(phase=phase), self.assertRaises(AssertionError):
                    validate_source_snapshot_faults(context)
            path.write_text(json.dumps(value))

    def test_snapshot_faults_require_retained_native_intent_and_exact_binding(self):
        from source_snapshot_fixture import validate_source_snapshot_faults
        from source_snapshot_fault import SnapshotFault
        with tempfile.TemporaryDirectory() as directory:
            context = self.fault_context(directory)
            records = self.write_faults(context)
            path = Path(records["create_reply_lost"]["native_path"])
            original = json.loads(path.read_text())
            validate_source_snapshot_faults(context)
            relay = SnapshotFault(context, "create_reply_lost", "create_reply_lost")
            relay.native_path = path
            self.assertEqual(relay.native(), original["source_slot"])
            for changes in ({"format_version": 2}, {"format_version": 3}, {"format_version": 5}, {"source_slot": None},
                            {"copy_snapshot": {}},
                            {"copy_complete": {"tables": 0, "rows": 0, "bytes": 0}}, {"run": [0] * 16}):
                path.write_text(json.dumps({**original, **changes}))
                with self.subTest(changes=changes), self.assertRaises((AssertionError, KeyError, TypeError)):
                    validate_source_snapshot_faults(context)
            for replacement in ({}, "missing"):
                changed = copy.deepcopy(original)
                if replacement == "missing":
                    del changed["copy_snapshot"]
                else:
                    changed["copy_snapshot"] = replacement
                path.write_text(json.dumps(changed))
                for check in (lambda: validate_source_snapshot_faults(context), relay.native):
                    with self.subTest(copy_snapshot=replacement), self.assertRaises((AssertionError, KeyError)):
                        check()
            path.write_text(json.dumps(original))
            for field, bad in (("slot_name", "another_slot"), ("plugin", "pgoutput"), ("catalog_epoch", 9)):
                changed = copy.deepcopy(original)
                changed["source_slot"]["record"]["binding"][field] = bad
                path.write_text(json.dumps(changed))
                with self.subTest(field=field), self.assertRaises(AssertionError):
                    validate_source_snapshot_faults(context)
            path.unlink()
            with self.assertRaises((AssertionError, FileNotFoundError)):
                validate_source_snapshot_faults(context)

    def test_snapshot_wrong_ready_byte_cannot_pass_without_observed_mutation(self):
        from source_snapshot_fixture import validate_source_snapshot_faults
        with tempfile.TemporaryDirectory() as directory:
            context = self.fault_context(directory)
            records = self.write_faults(context)
            path = Path(directory) / "snapshot-fault-create_tail_lost.json"
            for phase in ("ready_transaction", "ready_failed"):
                for key, bad in (("ready_before", "T"), ("ready_after", "I"),
                                 ("client_closed_after_ready", False), ("row_delivered", False)):
                    value = copy.deepcopy(records["create_tail_lost"])
                    value["phases"][phase]["details"][key] = bad
                    path.write_text(json.dumps(value))
                    with self.subTest(phase=phase, key=key), self.assertRaises(AssertionError):
                        validate_source_snapshot_faults(context)

    def test_snapshot_duplicate_json_receipt_fields_cannot_hide_extra_evidence(self):
        from source_snapshot_fixture import validate_source_snapshot_faults
        with tempfile.TemporaryDirectory() as directory:
            context = self.fault_context(directory)
            self.write_faults(context)
            path = Path(directory) / "snapshot-fault-create_reply_lost.json"
            original = path.read_text()
            duplicated = original.replace('"case": "create_reply_lost"',
                                          '"case": "foreign", "case": "create_reply_lost"', 1)
            self.assertNotEqual(duplicated, original)
            path.write_text(duplicated)
            with self.assertRaisesRegex(AssertionError, "duplicate"):
                validate_source_snapshot_faults(context)

    def test_ready_fault_edits_only_observed_idle_and_keeps_backend_open(self):
        try:
            from source_snapshot_fault import SnapshotFault
        except ImportError:
            self.fail("snapshot relay is missing")
        for phase, after in (("ready_transaction", b"T"), ("ready_failed", b"E")):
            with self.subTest(phase=phase):
                relay = SnapshotFault({"token": "owned"}, "create_tail_lost", phase)
                relay.armed.set()
                relay.deadline = float("inf")
                relay.command = 'CREATE_REPLICATION_SLOT "owned_slot" LOGICAL "test_decoding"'
                relay.details.update(row_seen=True, completion_seen=True, row_delivered=True,
                                     command_tag="CREATE_REPLICATION_SLOT")
                frame = b"Z" + struct.pack("!I", 5) + b"I"
                self.assertEqual(relay.server_frame(b"Z", frame, b"I"), b"Z" + struct.pack("!I", 5) + after)
                self.assertEqual(relay.hits, 1)
                self.assertFalse(relay.close_after_frame)
                self.assertFalse(relay.backend_closed.is_set())
                self.assertEqual(relay.details["ready_before"], "I")
                self.assertEqual(relay.details["ready_after"], after.decode("ascii"))
                relay = SnapshotFault({"token": "owned"}, "create_tail_lost", phase)
                relay.armed.set()
                relay.command = "CREATE_REPLICATION_SLOT owned_slot LOGICAL test_decoding"
                with self.assertRaises(AssertionError):
                    relay.server_frame(b"Z", b"Z\0\0\0\5T", b"T")

    def test_snapshot_version_pair_must_be_explicitly_consistent_and_qualified(self):
        from source_snapshot_fixture import source_snapshot_fixture
        for version, number in (("14.24", "140024"), ("17.11", "170011")):
            self.assertEqual(expected_version({"RPGMIGRATE_TEST_EXPECTED_PG_VERSION": version,
                              "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM": number}, "0", 0), (version, int(number)))
        for environment in ({"RPGMIGRATE_TEST_EXPECTED_PG_VERSION": "14.24"},
                            {"RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM": "170011"},
                            {"RPGMIGRATE_TEST_EXPECTED_PG_VERSION": "14.24", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM": "170011"}):
            with self.subTest(environment=environment), self.assertRaises(ValueError):
                expected_version(environment, "14.24", 140024)
        for version, number in (("14.23", 140023), ("17.10", 170010), ("16.11", 160011)):
            with self.subTest(version=version), patch("source_snapshot_fixture.OwnedCluster", self.forbid_fixture), \
                    self.assertRaisesRegex(AssertionError, "unqualified"):
                with source_snapshot_fixture({"server_version": version, "server_version_num": number}):
                    self.fail("unqualified source fixture started")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(SourceSnapshotHarnessTests)
    if len(sys.argv) > 1:
        suite = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:], module=sys.modules[__name__])
    if not suite.countTestCases():
        raise RuntimeError("no source snapshot harness tests selected")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() and not result.skipped else 1)
