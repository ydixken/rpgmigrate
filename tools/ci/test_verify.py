"""Exercise CI refusal paths without a database or container engine."""

import importlib.util
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("verify.py")
runner = None
if SCRIPT.is_file():
    spec = importlib.util.spec_from_file_location("ci_verify", SCRIPT)
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(runner, "the Linux verification runner is missing")

    def test_empty_or_zero_unit_suites_cannot_pass(self):
        for output in ("", "test result: ok. 0 passed; 0 failed; 0 ignored;", "Ran 0 tests\nOK"):
            with self.subTest(output=output), self.assertRaises(runner.VerificationError):
                runner.validate_output("test", output)

    def test_all_task_test_suites_must_be_observed(self):
        rust = "test result: ok. 73 passed; 0 failed; 0 ignored; 7 filtered out;\n"
        python = "Ran 24 tests in 1.0s\nOK\nRan 20 tests in 1.0s\nOK\nRan 14 tests in 1.0s\nOK\n"
        publication = "Storage publication verified 22 cases and 20 continuations\n"
        counts = runner.validate_output("test", rust + python + publication)
        self.assertEqual(counts, {"rust": 73, "python": [24, 20, 14], "storage_publication": 22,
                                  "storage_continuations": 20})
        for output in (rust, python, rust + python.replace("Ran 20", "Ran 0"), rust.replace("0 ignored", "1 ignored") + python):
            with self.subTest(output=output), self.assertRaises(runner.VerificationError):
                runner.validate_output("test", output)

    def test_publication_footer_must_be_exact_and_unique(self):
        suites = ("test result: ok. 73 passed; 0 failed; 0 ignored;\n"
                  "Ran 24 tests in 1.0s\nOK\nRan 20 tests in 1.0s\nOK\nRan 19 tests in 1.0s\nOK\n")
        footer = "Storage publication verified 22 cases and 20 continuations\n"
        for value in ("", footer * 2, footer.replace("22", "0"), footer.replace("22", "21"),
                      footer.replace("20", "19"), "[test] " + footer, footer + footer.replace("22", "23")):
            with self.subTest(value=value), self.assertRaises(runner.VerificationError):
                runner.validate_output("test", suites + value)

    def test_publication_report_requires_exact_cases_and_continuations(self):
        self.assertTrue(hasattr(runner, "storage_publication"), "publication receipt validator is absent")
        good = {"version": 1, "receipts": [
            {"version": 1, "case": i, "candidate": "segmented" if i < 14 else "sqlite",
             "required_prefix": 2, "caller_outcome": "returned" if i in (13, 21) else "unknown",
             "continuation": None if i in (13, 21) else {"prefix": 3, "input_crc32": 123}}
            for i in range(22)]}
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "report.json"
            path.write_text(json.dumps(good))
            self.assertEqual(runner.storage_publication(path),
                             "Storage publication verified 22 cases and 20 continuations")
            changes = [((), {}), (("version",), True), (("receipts",), []),
                       (("receipts",), good["receipts"][:-1]),
                       (("receipts",), good["receipts"] + [good["receipts"][0]]),
                       (("receipts", 1, "case"), 0), (("receipts", 1, "case"), True),
                       (("receipts", 0, "candidate"), "redb"),
                       (("receipts", 0, "continuation"), None),
                       (("receipts", 0, "continuation", "prefix"), 2),
                       (("receipts", 0, "caller_outcome"), "returned"),
                       (("receipts", 13, "continuation"), {"prefix": 3, "input_crc32": 123})]
            for keys, value in changes:
                bad = copy.deepcopy(good)
                parent = bad
                for key in keys[:-1]:
                    parent = parent[key]
                if keys:
                    parent[keys[-1]] = value
                else:
                    bad = value
                path.write_text(json.dumps(bad))
                with self.subTest(keys=keys, value=value), self.assertRaises(runner.VerificationError):
                    runner.storage_publication(path)

    def test_publication_report_cli_refuses_absent_invalid_or_oversized_input(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "report.json"
            for content in (None, b"", b"{", b" " * 65537):
                if content is not None:
                    path.write_bytes(content)
                with self.subTest(content=None if content is None else len(content)):
                    result = subprocess.run([sys.executable, "-O", str(SCRIPT), "--storage-publication", str(path)],
                                            capture_output=True, text=True)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(result.stdout, "")
                    self.assertIn("invalid storage publication receipt", result.stderr)

    def test_database_receipts_require_both_exact_inventories_and_version(self):
        good = ("Authority faults verified 2 held COMMIT replies and observed fence releases\n"
                "Replication faults verified 4 wire cases and observed PQflush backpressure\n"
                "Engine verified 39 exact cases; full E1 suite not selected\n"
                "E1 verified 40 exact cases on PostgreSQL 17.11\n")
        self.assertEqual(runner.validate_output("test-db", good),
                         {"engine": 39, "integration": 40, "authority_faults": 2, "replication_faults": 4})
        for output in ("", good.replace("40", "25"), good.replace("40", "38"), good.replace("17.11", "14.24"), good.splitlines()[0]):
            with self.subTest(output=output), self.assertRaises(runner.VerificationError):
                runner.validate_output("test-db", output)

    def test_engine_receipt_cannot_omit_the_prepared_profile(self):
        previous = ("Authority faults verified 2 held COMMIT replies and observed fence releases\n"
                    "Replication faults verified 4 wire cases and observed PQflush backpressure\n"
                    "Engine verified 7 exact cases; full E1 suite not selected\n"
                    "E1 verified 40 exact cases on PostgreSQL 17.11\n")
        with self.assertRaises(runner.VerificationError):
            runner.validate_output("test-db", previous)

    def test_engine_receipt_cannot_omit_the_authority_profile(self):
        previous = ("Authority faults verified 2 held COMMIT replies and observed fence releases\n"
                    "Replication faults verified 4 wire cases and observed PQflush backpressure\n"
                    "Engine verified 14 exact cases; full E1 suite not selected\n"
                    "E1 verified 40 exact cases on PostgreSQL 17.11\n")
        with self.assertRaises(runner.VerificationError):
            runner.validate_output("test-db", previous)

    def test_engine_receipt_cannot_omit_the_replication_profile(self):
        previous = ("Authority faults verified 2 held COMMIT replies and observed fence releases\n"
                    "Replication faults verified 4 wire cases and observed PQflush backpressure\n"
                    "Engine verified 30 exact cases; full E1 suite not selected\n"
                    "E1 verified 40 exact cases on PostgreSQL 17.11\n")
        with self.assertRaises(runner.VerificationError):
            runner.validate_output("test-db", previous)

    def test_authority_faults_require_one_complete_observed_lifecycle_receipt(self):
        suites = ("Replication faults verified 4 wire cases and observed PQflush backpressure\n"
                  "Engine verified 39 exact cases; full E1 suite not selected\n"
                  "E1 verified 40 exact cases on PostgreSQL 17.11\n")
        fault = "Authority faults verified 2 held COMMIT replies and observed fence releases\n"
        for evidence in ("", fault * 2, fault.replace("2", "0"), fault.replace("2", "1"),
                         fault.replace("2", "3"), fault.replace("held", "unheld"),
                         fault.replace("observed", "pending"), fault.replace("verified", "ignored"),
                         fault + fault.replace("2", "0")):
            with self.subTest(evidence=evidence), self.assertRaises(runner.VerificationError):
                runner.validate_output("test-db", evidence + suites)

    def test_replication_faults_require_one_complete_backpressure_receipt(self):
        suites = ("Authority faults verified 2 held COMMIT replies and observed fence releases\n"
                  "Engine verified 39 exact cases; full E1 suite not selected\n"
                  "E1 verified 40 exact cases on PostgreSQL 17.11\n")
        fault = "Replication faults verified 4 wire cases and observed PQflush backpressure\n"
        for evidence in ("", fault * 2, fault.replace("4", "0"), fault.replace("4", "3"),
                         fault.replace("4", "5"), fault.replace("verified", "ignored"),
                         fault.replace("observed", "pending"), fault.replace("PQflush", "socket"),
                         fault.replace(" and observed PQflush backpressure", ""),
                         fault + fault.replace("4", "0")):
            with self.subTest(evidence=evidence), self.assertRaises(runner.VerificationError):
                runner.validate_output("test-db", evidence + suites)

    def test_nonzero_child_and_missing_tool_are_safe_failures(self):
        for command in ([sys.executable, "-c", "print('password=do-not-export'); raise SystemExit(9)"], ["/nonexistent-e1-tool"]):
            with self.subTest(command=command), self.assertRaises(runner.VerificationError) as error:
                runner.execute(command)
            self.assertNotIn("do-not-export", str(error.exception))

    def test_failed_test_names_are_retained_without_raw_diagnostics(self):
        command = [sys.executable, "-c", "print('test fixture::failing_case ... FAILED'); print('password=do-not-export'); raise SystemExit(9)"]
        with self.assertRaises(runner.VerificationError) as error:
            runner.execute(command)
        self.assertIn("fixture::failing_case", str(error.exception))
        self.assertNotIn("do-not-export", str(error.exception))

    def test_timeout_cannot_pass(self):
        with self.assertRaises(runner.VerificationError):
            runner.execute([sys.executable, "-c", "import time; time.sleep(10)"], timeout=0.05)

    def test_ci_checks_cannot_report_empty_or_skipped_tests(self):
        self.assertEqual(runner.validate_output("ci-unit", f"Ran {sum(runner.ci_unit_inventory().values())} tests in 0.1s\nOK\n"), {"python": [sum(runner.ci_unit_inventory().values())]})
        for output in ("", "Ran 0 tests in 0.1s\nOK\n", "Ran 6 tests in 0.1s\nOK (skipped=1)\n"):
            with self.subTest(output=output), self.assertRaises(runner.VerificationError):
                runner.validate_output("ci-unit", output)

    def test_source_snapshot_refuses_symlinked_input_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("Cargo.toml", "Cargo.lock", "Taskfile.yml", "Dockerfile", ".dockerignore", "build.rs", "NOTICE"):
                (root / name).touch()
            for name in ("tests", "tools", "native", "bench", "docs", ".github", "outside"):
                (root / name).mkdir()
            (root / "outside/hidden.rs").write_text("private input")
            (root / "src").symlink_to(root / "outside", target_is_directory=True)
            with self.assertRaisesRegex(runner.VerificationError, "^source symlinks are not supported$"):
                runner.source_files(root)

    def test_prefixed_receipts_and_dry_run_output_cannot_pass(self):
        rust = "test result: ok. 73 passed; 0 failed; 0 ignored; 7 filtered out;\n"
        python = "Ran 24 tests in 1.0s\nOK\nRan 20 tests in 1.0s\nOK\nRan 19 tests in 1.0s\nOK\n"
        for output in ("[test] " + rust + python, "task: [test] cargo test\n", rust + "[test] " + python):
            with self.subTest(output=output), self.assertRaises(runner.VerificationError):
                runner.validate_output("test", output)

    def test_task_dry_run_environment_refuses_before_commands(self):
        from unittest.mock import patch
        for value in ("true", "1", "TRUE", "yes"):
            with patch.dict(runner.os.environ, {"TASK_DRY": value}), self.assertRaises(runner.VerificationError):
                runner.task_command("native")

    def test_missing_task_cannot_pass(self):
        with tempfile.TemporaryDirectory() as empty:
            with self.assertRaises(runner.VerificationError):
                runner.execute(["task", "--version"], environment={"PATH": empty})

    def test_source_snapshot_requires_taskfile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("Cargo.toml", "Cargo.lock", "Makefile", "build.rs", "NOTICE", "Dockerfile", ".dockerignore"):
                (root / name).touch()
            for name in ("src", "tests", "tools", "native", "bench", "docs", ".github"):
                (root / name).mkdir()
            with self.assertRaises(runner.VerificationError):
                runner.source_files(root)

    def test_ci_inventory_matches_source_and_refuses_empty_modules(self):
        self.assertTrue(hasattr(runner, "ci_unit_inventory"), "source inventory calculation is absent")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in runner.CI_SUITES:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("class Tests(unittest.TestCase):\n    def test_required(self): pass\n")
            self.assertEqual(set(runner.ci_unit_inventory(root).values()), {1})
            (root / next(iter(runner.CI_SUITES))).write_text("class Tests(unittest.TestCase): pass\n")
            with self.assertRaises(runner.VerificationError):
                runner.ci_unit_inventory(root)

    def test_missing_image_unit_module_refuses_the_aggregate(self):
        image_suites = ("tools/image/test_build.py", "tools/image/test_runtime.py", "tools/image/test_smoke.py")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            contents = "class Tests(unittest.TestCase):\n    def test_required(self): pass\n"
            for name in (*runner.CI_SUITES, *image_suites):
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(contents)
            for name in image_suites:
                path = root / name
                path.unlink()
                with self.subTest(module=name), self.assertRaises(runner.VerificationError):
                    runner.ci_unit_inventory(root)
                path.write_text(contents)

    def test_source_snapshot_includes_all_image_unit_modules(self):
        image_suites = ("tools/image/test_build.py", "tools/image/test_runtime.py", "tools/image/test_smoke.py")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("Cargo.toml", "Cargo.lock", "Taskfile.yml", "Dockerfile", ".dockerignore", "build.rs", "NOTICE"):
                (root / name).touch()
            for name in ("src", "tests", "tools", "native", "bench", "docs", ".github"):
                (root / name).mkdir()
            for name in image_suites:
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("class Tests(unittest.TestCase):\n    def test_required(self): pass\n")
            files = runner.source_files(root)
            for name in image_suites:
                with self.subTest(module=name):
                    self.assertIn(Path(name), files)

    def test_child_output_is_not_echoed(self):
        result = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import verify; verify.execute([sys.executable, '-c', 'print(\"password=do-not-export\")'])", str(SCRIPT.parent)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout + result.stderr, "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
