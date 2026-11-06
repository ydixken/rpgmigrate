"""Run with python3 tests/support/test_harness.py."""

import json
import os
from pathlib import Path
import runpy
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest

from db_harness import (clean_environment, conninfo, expected_version, load_manifest, module_cases, run_command, validate_libtest_output,
                        validate_report, validate_success_output)
from protocol_fault import error_sqlstate
from e1_cases import Cases
from protocol_fault import packet
from replication_fault import ReplicationFault


def replication_records(context):
    feedback = (b'r' + bytes(33)).hex()
    records = []
    for case in ("keepalive", "duplex", "terminal_error", "missing_completion"):
        value = {"owner": context["token"], "case": case, "status": "observed", "fault_hits": 1,
                 "copyboth_seen": True, "backend_closed": True, "feedback_hex": feedback,
                 "expected_feedback_hex": feedback, "feedback_count": 1, "keepalive_sent": True,
                 "post_arm_feedback_count": 1, "feedback_mismatches": 0,
                 "held": False, "released": True, "server_frames_while_held": 1, "pqflush_pending": 1,
                 "copydone_seen": True, "terminal_seen": True,
                 "terminal_fault": "error" if case == "terminal_error" else "missing",
                 "error_sqlstate": "57014" if case == "terminal_error" else None}
        if case == "terminal_error":
            late = dict(value)
            pending = {**value, "backend_pid": 123, "cancel_requested": True, "owned_backend_verified": True, "server_error_seen": True,
                       "gate_held_at_error": True, "client_error_sqlstate": "57014", "client_closed_before_release": True,
                       "client_before_deadline": True, "feedback_drained_after_cancel": 0, "held": True, "released": False}
            value["phases"] = {"late_error": late, "pending_error": pending}
            value["fault_hits"] = 2
        path = Path(context["root"]) / f"replication-fault-{case}.json"
        path.write_text(json.dumps(value))
        records.append((path, value))
    return records


def authority_records(context):
    for case in ("lost_commit_reply", "provisioning_recovery"):
        value = {"owner": context["token"], "case": case, "status": "observed", "fault_hits": 1, "backend_pid": 123,
                 "commit_tag": "COMMIT", "holding_backend": False, "backend_connection_closed": True,
                 "held_fence_observed": True, "released_fence_observed": True}
        (Path(context["root"]) / f"authority-fault-{case}.json").write_text(json.dumps(value))


def keepalive_receipt(before, after):
    relay = ReplicationFault({"token": "owned-fixture"}, "keepalive")
    client, frontend = socket.socketpair()
    backend, server = socket.socketpair()
    relay.client = client
    relay.state["copyboth_seen"] = True
    for connection in (client, frontend, backend, server):
        connection.settimeout(2)
    worker = threading.Thread(target=relay.upstream, args=(client, backend))
    worker.start()
    try:
        frontend.sendall(struct.pack("!II", 8, 196608))
        packet(server, typed=False)
        for payload in before:
            frontend.sendall(b'd' + struct.pack("!I", 4 + len(payload)) + payload)
            packet(server)
        relay.operate({"op": "arm", "wal_end": 900000, "feedback_hex": (b'r' + bytes(33)).hex()})
        packet(frontend)
        for payload in after:
            frontend.sendall(b'd' + struct.pack("!I", 4 + len(payload)) + payload)
            packet(server)
    finally:
        relay.stopping.set()
        for connection in (client, frontend, backend, server):
            connection.close()
        worker.join(timeout=3)
    assert not worker.is_alive()
    relay.backend_closed.set()
    return relay.snapshot()


class HarnessTests(unittest.TestCase):
    def assert_replication_profiles_reject(self, value):
        support = Path(__file__).resolve().parent
        validate = runpy.run_path(str(support.parents[1] / "tools" / "test-db.py"))["validate_module_receipt"]
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned-fixture"}
            context["replication"] = dict(context)
            authority_records(context)
            for profile in ("Replication", "Engine"):
                replication_records(context)
                expected = module_cases(load_manifest(), profile)
                output = "".join(f"test {name} ... ok\n" for name in expected)
                validate(profile, expected, output, context)
                for receipt in (value, {**value, "status": "observed"}):
                    (Path(directory) / "replication-fault-keepalive.json").write_text(json.dumps(receipt))
                    with self.subTest(profile=profile, status=receipt["status"]), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)

    def test_keepalive_receipt_cannot_use_feedback_from_before_arm(self):
        correct = b'r' + bytes(33)
        self.assertEqual(keepalive_receipt([], [correct])["status"], "observed")
        invalid = keepalive_receipt([correct], [])
        self.assertEqual(invalid["status"], "pending")
        self.assert_replication_profiles_reject(invalid)

    def test_keepalive_receipt_rejects_a_later_mismatching_feedback(self):
        correct = b'r' + bytes(33)
        mismatch = b'r' + struct.pack("!QQQQB", 900000, 900000, 900000, 0, 0)
        self.assertEqual(keepalive_receipt([], [correct, correct])["status"], "observed")
        invalid = keepalive_receipt([], [correct, mismatch])
        self.assertEqual(invalid["status"], "pending")
        self.assert_replication_profiles_reject(invalid)

    def test_optimized_python_cannot_accept_invalid_receipts(self):
        support = Path(__file__).resolve().parent
        code = ("from db_harness import validate_libtest_output; "
                "validate_libtest_output(['pg::live_tests::copy'], '')")
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "zero-test-command-ran"
            commands = (
                ["-c", code],
                [str(support.parents[1] / "tools" / "test-db.py"), "--engine", "--", sys.executable,
                 "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()", str(marker)],
                [str(support.parents[1] / "tools" / "test-db.py"), "--prepared", "--", sys.executable,
                 "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()", str(marker)],
                [str(support.parents[1] / "tools" / "test-db.py"), "--authority", "--", sys.executable,
                 "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()", str(marker)],
                [str(support.parents[1] / "tools" / "test-db.py"), "--replication", "--", sys.executable,
                 "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()", str(marker)],
                [str(support / "e1_cases.py"), sys.executable],
                [str(support / "authority_fault.py"), "--case", "lost_commit_reply"],
                [str(support / "authority_selfcheck.py")],
                [str(support / "replication_fault.py"), "--case", "keepalive"],
                [str(support / "replication_selfcheck.py")],
            )
            for flags, optimization in ((["-O"], None), (["-OO"], None), ([], "1"), ([], "2")):
                for command in commands:
                    with self.subTest(flags=flags, optimization=optimization, command=command[0]):
                        environment = clean_environment(os.environ)
                        environment.pop("PYTHONOPTIMIZE", None)
                        environment.update(PG_BINDIR=str(Path(directory) / "missing"), TMPDIR=directory)
                        if optimization is not None:
                            environment["PYTHONOPTIMIZE"] = optimization
                        result = subprocess.run(
                            [sys.executable, *flags, *command], cwd=support,
                            env=environment, capture_output=True, text=True, timeout=10,
                        )
                        self.assertNotEqual(result.returncode, 0, "optimized harness accepted an invalid receipt")
                        self.assertIn("requires Python assertions", result.stderr)
                        self.assertEqual(list(Path(directory).iterdir()), [], "fixture or zero-test command started")

    def test_sqlstate_evidence_uses_the_protocol_code_field(self):
        self.assertEqual(error_sqlstate(b"SERROR\0C23505\0Mfixture duplicate\0\0"), "23505")
        self.assertEqual(error_sqlstate(b"SERROR\0MC23505 appears in text\0C57014\0\0"), "57014")
        for invalid in (b"SERROR\0Mno code\0\0", b"Csecret\0\0", b"C23505"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                error_sqlstate(invalid)

    def test_expected_version_requires_a_consistent_explicit_pair(self):
        version_key = "RPGMIGRATE_TEST_EXPECTED_PG_VERSION"
        number_key = "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM"
        self.assertEqual(expected_version({}, "14.24", 140024), ("14.24", 140024))
        self.assertEqual(expected_version({version_key: "17.11", number_key: "170011"}, "14.24", 140024), ("17.11", 170011))
        for invalid in ({version_key: "17.11"}, {number_key: "170011"},
                        {version_key: "17.11", number_key: "170010"},
                        {version_key: "17", number_key: "170000"},
                        {version_key: "", number_key: ""}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                expected_version(invalid, "14.24", 140024)

    def test_direct_runner_rejects_empty_selection(self):
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "-k", "__e1_no_matching_test__"],
            env=clean_environment(os.environ), capture_output=True, text=True, timeout=10,
        )
        self.assertNotEqual(result.returncode, 0, "zero selected harness tests reported success")

    def test_module_profiles_require_an_explicit_command(self):
        wrapper = Path(__file__).resolve().parents[2] / "tools" / "test-db.py"
        for profile in ("--prepared", "--authority", "--replication"):
            with self.subTest(profile=profile):
                result = subprocess.run(
                    [sys.executable, str(wrapper), profile], env=clean_environment(os.environ),
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn("provide --self-check or -- followed by", result.stderr)

    def test_auth_base_requires_explicit_fixture_connection_without_credentials(self):
        context = {"socket_dir": "/tmp/fixture-only", "port": 25432, "root": "/tmp/fixture-root"}
        result = conninfo(context, "auth_fixture", "e1_auth", include_passfile=False)
        self.assertIn("host='/tmp/fixture-only'", result)
        self.assertIn("port='25432'", result)
        self.assertIn("dbname='auth_fixture'", result)
        self.assertIn("user='e1_auth'", result)
        self.assertNotIn("password=", result)
        self.assertNotIn("passfile=", result)

    def test_success_output_cannot_disagree_with_native_receipt_or_expose_path(self):
        state = {"format_version": 2, "run": list(range(16)), "target_provenance": [],
                 "copy_complete": {"tables": 2, "rows": 10003, "bytes": 12345678}}
        valid = {"command": "copy table-data", "status": "complete", "run": list(range(16)),
                 "state_format_version": 2, "tables": 2, "rows": 10003, "bytes": 12345678}
        validate_success_output(json.dumps(valid), state)
        for changes in ({"run": [0] * 16}, {"state_format_version": 1}, {"state_format_version": 3}, {"tables": 0},
                        {"rows": 0}, {"bytes": 0}, {"canonical_path": "/private/fixture"},
                        {"status": "running"}, {"command": "ping"}):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                validate_success_output(json.dumps({**valid, **changes}), state)

    def test_copy_receipts_require_v2_and_empty_target_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            case = SimpleNamespace(state=state)
            for complete in (None, {"tables": 1, "rows": 1, "bytes": 8}):
                value = {"magic": "RPGMIGRATE", "format_version": 2, "target_provenance": [],
                         "run": list(range(16)), "canonical_path": str(state.resolve()), "copy_complete": complete}
                output = json.dumps({"command": "copy table-data", "status": "complete", "run": list(range(16)),
                                     "state_format_version": 2, "tables": 1, "rows": 1, "bytes": 8})
                check = (lambda: Cases.no_completion(case)) if complete is None else (
                    lambda: Cases.completed(case, 1, 1, output))
                (state / "manifest.json").write_text(json.dumps(value))
                check()
                for changes in ({"format_version": 1}, {"format_version": 3}, {"target_provenance": None},
                                {"target_provenance": [{"phase": "confirmed", "record": {}}]}):
                    with self.subTest(complete=complete, changes=changes), self.assertRaises(AssertionError):
                        (state / "manifest.json").write_text(json.dumps({**value, **changes}))
                        check()
                del value["target_provenance"]
                (state / "manifest.json").write_text(json.dumps(value))
                with self.assertRaises((AssertionError, KeyError)):
                    check()

    def test_timed_out_command_terminates_its_descendant(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "terminated"
            descendant = "import pathlib,signal,sys,time; signal.signal(signal.SIGTERM,lambda *_: (pathlib.Path(sys.argv[1]).touch(),sys.exit(0))); time.sleep(3)"
            parent = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]]); time.sleep(3)"
            with self.assertRaises(subprocess.TimeoutExpired):
                run_command([sys.executable, "-c", parent, descendant, str(marker)],
                            environment=clean_environment(os.environ), cwd=directory, timeout=0.5)
            deadline = time.monotonic() + 1
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(marker.exists(), "owned grandchild was not terminated with the timed-out command")

    def test_module_selection_cannot_be_empty_missing_or_ignored(self):
        expected = ["pg::live_tests::copy", "pg::live_tests::query"]
        good = "test pg::live_tests::copy ... ok\ntest pg::live_tests::query ... ok\n"
        validate_libtest_output(expected, good)
        for output in ("", good.replace(" ... ok", " ... ignored", 1), good.splitlines()[0], good + good,
                       good.replace("::copy", "::unexpected"), good.replace(" ... ok", " ... FAILED", 1)):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate_libtest_output(expected, output)
        for inventory, output in (([], ""), ([expected[0]] * 2, good.splitlines()[0] + "\n" + good.splitlines()[0])):
            with self.subTest(inventory=inventory), self.assertRaises(AssertionError):
                validate_libtest_output(inventory, output)
        manifest = load_manifest()
        previous = manifest["transport_cases"] + manifest["copy_cases"]
        prepared = module_cases(manifest, "Prepared")
        for subset in (previous, prepared, previous + prepared):
            with self.subTest(subset=subset), self.assertRaises(AssertionError):
                validate_libtest_output(module_cases(manifest, "Engine"),
                                        "".join(f"test {name} ... ok\n" for name in subset))

    def test_child_environment_drops_database_context(self):
        environment = {
            "PATH": "/usr/bin", "HOME": "/home/test", "CARGO_HOME": "/cargo",
            "PGHOST": "existing", "PGPASSWORD": "secret", "PG_CONFIG": "old",
            "PGCOPYDB_SOURCE_PGURI": "secret", "RPGMIGRATE_TARGET": "secret",
        }
        result = clean_environment(environment)
        self.assertEqual(result, {"PATH": "/usr/bin", "HOME": "/home/test", "CARGO_HOME": "/cargo"})

    def test_absent_duplicate_skipped_and_fabricated_cases_fail(self):
        manifest = {"server_version_num": 140024, "cases": [{"id": "copy", "outcome": "success"}]}
        valid = {"server_version_num": 140024, "cases": [{"id": "copy", "status": "passed", "returncode": 0}]}
        validate_report(manifest, valid)
        for cases in ([], valid["cases"] * 2, [{"id": "copy", "status": "skipped", "returncode": 0}],
                      [{"id": "unlisted", "status": "passed", "returncode": 0}]):
            with self.subTest(cases=cases), self.assertRaises(AssertionError):
                validate_report(manifest, {**valid, "cases": cases})

    def test_wrong_version_and_suppressed_fault_fail(self):
        manifest = {"server_version_num": 140024, "cases": [{"id": "fault", "outcome": "failure", "fault": True}]}
        valid = {"server_version_num": 140024, "cases": [{"id": "fault", "status": "passed", "returncode": 6, "fault_hits": 1}]}
        validate_report(manifest, valid)
        with self.assertRaises(AssertionError):
            validate_report(manifest, {**valid, "server_version_num": 140023})
        for changes in ({"fault_hits": 0}, {"returncode": 0}):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                validate_report(manifest, {**valid, "cases": [{**valid["cases"][0], **changes}]})

    def test_authority_and_engine_require_observed_fault_receipts(self):
        support = Path(__file__).resolve().parent
        validate = runpy.run_path(str(support.parents[1] / "tools" / "test-db.py"))["validate_module_receipt"]
        manifest = load_manifest()
        prepared = json.loads((support / "prepared_cases.json").read_text())
        authority = json.loads((support / "authority_cases.json").read_text())
        replication = json.loads((support / "replication_cases.json").read_text())
        profiles = {"Authority": authority, "Engine": manifest["transport_cases"] + manifest["copy_cases"] + prepared + authority + replication}
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned-fixture"}
            context["replication"] = dict(context)
            replication_records(context["replication"])
            records = []
            for case in ("lost_commit_reply", "provisioning_recovery"):
                value = {"owner": "owned-fixture", "case": case, "status": "observed", "fault_hits": 1, "backend_pid": 123,
                         "commit_tag": "COMMIT", "holding_backend": False, "backend_connection_closed": True,
                         "held_fence_observed": True, "released_fence_observed": True}
                path = Path(directory) / f"authority-fault-{case}.json"
                path.write_text(json.dumps(value))
                records.append((path, value))
            path, valid = records[0]
            for profile, expected in profiles.items():
                output = "".join(f"test {name} ... ok\n" for name in expected)
                path.write_text(json.dumps(valid))
                validate(profile, expected, output, context)
                path.unlink()
                with self.subTest(profile=profile, missing=True), self.assertRaises(AssertionError):
                    validate(profile, expected, output, context)
                for changes in ({"fault_hits": 0}, {"fault_hits": True}, {"status": "ignored"}, {"status": "pending"},
                                {"backend_pid": 0}, {"owner": "foreign"}, {"case": "other"}, {"commit_tag": "ROLLBACK"},
                                {"held_fence_observed": False}, {"released_fence_observed": False},
                                {"backend_connection_closed": False}, {"holding_backend": True}):
                    with self.subTest(profile=profile, changes=changes), self.assertRaises(AssertionError):
                        path.write_text(json.dumps({**valid, **changes}))
                        validate(profile, expected, output, context)

    def test_replication_and_engine_require_observed_wire_receipts(self):
        support = Path(__file__).resolve().parent
        validate = runpy.run_path(str(support.parents[1] / "tools" / "test-db.py"))["validate_module_receipt"]
        manifest = load_manifest()
        replication = json.loads((support / "replication_cases.json").read_text())
        old = manifest["transport_cases"] + manifest["copy_cases"]
        old += json.loads((support / "prepared_cases.json").read_text()) + json.loads((support / "authority_cases.json").read_text())
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned-fixture"}
            context["replication"] = dict(context)
            authority_records(context)
            records = replication_records(context)
            for profile, expected in (("Replication", replication), ("Engine", old + replication)):
                output = "".join(f"test {name} ... ok\n" for name in expected)
                validate(profile, expected, output, context)
                for path, valid in records:
                    path.unlink()
                    with self.subTest(profile=profile, case=valid["case"], missing=True), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)
                    for changes in ({"fault_hits": 0}, {"fault_hits": True}, {"status": "pending"}, {"status": "ignored"},
                                    {"case": "other"}, {"owner": "foreign"}, {"copyboth_seen": False}, {"backend_closed": False}):
                        path.write_text(json.dumps({**valid, **changes}))
                        with self.subTest(profile=profile, case=valid["case"], changes=changes), self.assertRaises(AssertionError):
                            validate(profile, expected, output, context)
                    path.write_text(json.dumps(valid))
                for case, changes in (("duplex", {"pqflush_pending": 0}), ("duplex", {"pqflush_pending": True}),
                                      ("duplex", {"server_frames_while_held": 0}), ("duplex", {"released": False}),
                                      ("keepalive", {"feedback_hex": (b'r' + bytes(32) + b'\1').hex()}),
                                      ("keepalive", {"post_arm_feedback_count": 0}),
                                      ("keepalive", {"feedback_mismatches": 1}),
                                      ("keepalive", {"keepalive_sent": False}), ("terminal_error", {"error_sqlstate": "23505"}),
                                      ("missing_completion", {"terminal_seen": False})):
                    path, valid = next(item for item in records if item[1]["case"] == case)
                    path.write_text(json.dumps({**valid, **changes}))
                    with self.subTest(profile=profile, case=case, changes=changes), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)
                    path.write_text(json.dumps(valid))
                extra = Path(directory) / "replication-fault-unexpected.json"
                extra.write_text("{}")
                with self.subTest(profile=profile, extra=True), self.assertRaises(AssertionError):
                    validate(profile, expected, output, context)
                extra.unlink()

    def test_replication_inventory_is_exact_and_preserves_old_thirty(self):
        support = Path(__file__).resolve().parent
        manifest = load_manifest()
        replication = json.loads((support / "replication_cases.json").read_text())
        old = manifest["transport_cases"] + manifest["copy_cases"]
        old += json.loads((support / "prepared_cases.json").read_text()) + json.loads((support / "authority_cases.json").read_text())
        self.assertEqual(len(replication), 9)
        self.assertEqual(len(old), 30)
        self.assertEqual(module_cases(manifest, "Engine"), old + replication)
        self.assertEqual(module_cases(manifest, "Replication"), replication)
        for expected in (replication, old + replication):
            good = "".join(f"test {name} ... ok\n" for name in expected)
            for bad in ("", good.replace(" ... ok", " ... ignored", 1), "\n".join(good.splitlines()[1:]),
                        good + good, good.replace(expected[0], "unlisted_case")):
                with self.subTest(expected=len(expected), output=bad), self.assertRaises(AssertionError):
                    validate_libtest_output(expected, bad)

    def test_terminal_error_requires_both_independent_phase_proofs(self):
        support = Path(__file__).resolve().parent
        validate = runpy.run_path(str(support.parents[1] / "tools" / "test-db.py"))["validate_module_receipt"]
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned-fixture"}
            context["replication"] = dict(context)
            authority_records(context)
            records = replication_records(context)
            path, valid = next(item for item in records if item[1]["case"] == "terminal_error")
            for profile in ("Replication", "Engine"):
                expected = module_cases(load_manifest(), profile)
                output = "".join(f"test {name} ... ok\n" for name in expected)
                path.write_text(json.dumps(valid))
                validate(profile, expected, output, context)
                for phase in ("late_error", "pending_error"):
                    bad = {**valid, "phases": {key: value for key, value in valid["phases"].items() if key != phase}}
                    path.write_text(json.dumps(bad))
                    with self.subTest(profile=profile, missing=phase), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)
                for changes in ({"pqflush_pending": 0}, {"cancel_requested": False}, {"backend_pid": 0},
                                {"gate_held_at_error": False}, {"client_error_sqlstate": "57013"},
                                {"client_closed_before_release": False}, {"client_before_deadline": False},
                                {"feedback_drained_after_cancel": 1}, {"released": True}, {"backend_closed": False}):
                    pending = {**valid["phases"]["pending_error"], **changes}
                    path.write_text(json.dumps({**valid, "phases": {**valid["phases"], "pending_error": pending}}))
                    with self.subTest(profile=profile, changes=changes), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)
                path.write_text(json.dumps(valid))


class NonemptyProgram(unittest.TestProgram):
    def runTests(self):
        if self.test.countTestCases() == 0:
            raise SystemExit("No harness tests were selected")
        super().runTests()


if __name__ == "__main__":
    NonemptyProgram()
