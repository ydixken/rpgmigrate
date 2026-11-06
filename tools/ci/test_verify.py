"""Exercise CI refusal paths without a database or container engine."""

import importlib.util
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

    def test_all_make_test_suites_must_be_observed(self):
        rust = "test result: ok. 73 passed; 0 failed; 0 ignored; 7 filtered out;\n"
        python = "Ran 24 tests in 1.0s\nOK\nRan 20 tests in 1.0s\nOK\nRan 14 tests in 1.0s\nOK\n"
        counts = runner.validate_output("test", rust + python)
        self.assertEqual(counts, {"rust": 73, "python": [24, 20, 14]})
        for output in (rust, python, rust + python.replace("Ran 20", "Ran 0"), rust.replace("0 ignored", "1 ignored") + python):
            with self.subTest(output=output), self.assertRaises(runner.VerificationError):
                runner.validate_output("test", output)

    def test_database_receipts_require_both_exact_inventories_and_version(self):
        good = "Engine verified 14 exact cases; full E1 suite not selected\nE1 verified 40 exact cases on PostgreSQL 17.11\n"
        self.assertEqual(runner.validate_output("test-db", good), {"engine": 14, "integration": 40})
        for output in ("", good.replace("40", "25"), good.replace("40", "38"), good.replace("17.11", "14.24"), good.splitlines()[0]):
            with self.subTest(output=output), self.assertRaises(runner.VerificationError):
                runner.validate_output("test-db", output)

    def test_engine_receipt_cannot_omit_the_prepared_profile(self):
        previous = "Engine verified 7 exact cases; full E1 suite not selected\nE1 verified 40 exact cases on PostgreSQL 17.11\n"
        with self.assertRaises(runner.VerificationError):
            runner.validate_output("test-db", previous)

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
        self.assertEqual(runner.validate_output("ci-unit", "Ran 6 tests in 0.1s\nOK\n"), {"python": [6]})
        for output in ("", "Ran 0 tests in 0.1s\nOK\n", "Ran 6 tests in 0.1s\nOK (skipped=1)\n"):
            with self.subTest(output=output), self.assertRaises(runner.VerificationError):
                runner.validate_output("ci-unit", output)

    def test_source_snapshot_refuses_symlinked_input_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name in ("Cargo.toml", "Cargo.lock", "Makefile", "build.rs", "NOTICE"):
                (root / name).touch()
            for name in ("tests", "tools", "native", "bench", "docs", ".github", "outside"):
                (root / name).mkdir()
            (root / "outside/hidden.rs").write_text("private input")
            (root / "src").symlink_to(root / "outside", target_is_directory=True)
            with self.assertRaises(runner.VerificationError):
                runner.source_files(root)

    def test_child_output_is_not_echoed(self):
        result = subprocess.run([sys.executable, "-c", "import sys; sys.path.insert(0, sys.argv[1]); import verify; verify.execute([sys.executable, '-c', 'print(\"password=do-not-export\")'])", str(SCRIPT.parent)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout + result.stderr, "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
