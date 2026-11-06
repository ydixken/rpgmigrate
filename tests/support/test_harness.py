"""Run with python3 tests/support/test_harness.py."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from db_harness import (clean_environment, conninfo, expected_version, load_manifest, module_cases, run_command, validate_libtest_output,
                        validate_report, validate_success_output)
from protocol_fault import error_sqlstate


class HarnessTests(unittest.TestCase):
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
                [str(support / "e1_cases.py"), sys.executable],
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

    def test_prepared_profile_requires_an_explicit_command(self):
        wrapper = Path(__file__).resolve().parents[2] / "tools" / "test-db.py"
        result = subprocess.run(
            [sys.executable, str(wrapper), "--prepared"], env=clean_environment(os.environ),
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
        state = {"format_version": 1, "run": list(range(16)),
                 "copy_complete": {"tables": 2, "rows": 10003, "bytes": 12345678}}
        valid = {"command": "copy table-data", "status": "complete", "run": list(range(16)),
                 "state_format_version": 1, "tables": 2, "rows": 10003, "bytes": 12345678}
        validate_success_output(json.dumps(valid), state)
        for changes in ({"run": [0] * 16}, {"state_format_version": 2}, {"tables": 0},
                        {"rows": 0}, {"bytes": 0}, {"canonical_path": "/private/fixture"},
                        {"status": "running"}, {"command": "ping"}):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                validate_success_output(json.dumps({**valid, **changes}), state)

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
        for subset in (previous, prepared):
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


class NonemptyProgram(unittest.TestProgram):
    def runTests(self):
        if self.test.countTestCases() == 0:
            raise SystemExit("No harness tests were selected")
        super().runTests()


if __name__ == "__main__":
    NonemptyProgram()
