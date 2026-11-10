"""Exercise the portable tooling inventory and unit runner refusal paths."""

import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("verify.py")
spec = importlib.util.spec_from_file_location("ci_verify", SCRIPT)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)
HARNESS = (
    "tests/support/test_harness.py", "tests/support/test_catalog_harness.py",
    "tests/support/test_source_catalog_harness.py",
    "tests/support/test_copy_snapshot_harness.py", "tests/support/test_follow_snapshot_harness.py",
    "tests/support/test_follow_receive_peer.py", "tests/support/test_follow_m4_harness.py",
)
CONTENT = "import unittest\nclass Tests(unittest.TestCase):\n    def test_required(self): pass\n"


class VerificationTests(unittest.TestCase):
    def test_inventory_contains_each_retained_suite_once(self):
        self.assertEqual(runner.HARNESS_SUITES, HARNESS)
        self.assertEqual(runner.CI_SUITES, ("tools/ci/test_verify.py", "tools/ci/test_workflow.py", "tests/test_taskfile.py", "tests/test_native_fixtures.py", "tools/e2e/test_clone.py", "tools/e2e/test_follow.py", "tools/e2e/test_follow_wire_protocol.py", "tools/e2e/test_cutover.py", "tools/image/test_smoke.py", "tools/image/test_dev_publish.py", *HARNESS))
        self.assertEqual(len(set(runner.CI_SUITES)), len(runner.CI_SUITES))

    def populate(self, root):
        for name in runner.CI_SUITES:
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(CONTENT)

    def test_inventory_refuses_missing_invalid_and_empty_sources(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.populate(root)
            self.assertEqual(runner.ci_unit_inventory(root), dict.fromkeys(runner.CI_SUITES, 1))
            self.assertEqual(runner.harness_unit_inventory(root), dict.fromkeys(HARNESS, 1))
            for name in runner.CI_SUITES:
                path = root / name
                for content in (None, "", "class Tests(unittest.TestCase): pass\n", "def invalid("):
                    if content is None:
                        path.unlink()
                    else:
                        path.write_text(content)
                    with self.subTest(name=name, content=content), self.assertRaises(runner.VerificationError):
                        runner.ci_unit_inventory(root)
                path.write_text(CONTENT)

    def make_runner(self, root):
        self.populate(root)
        helper = root / "tools/ci/unit.py"
        helper.write_bytes(SCRIPT.with_name("unit.py").read_bytes())
        (helper.parent / "verify.py").write_bytes(SCRIPT.read_bytes())
        return helper

    def invoke(self, helper, *arguments, flags=()):
        return subprocess.run([sys.executable, *flags, str(helper), *arguments],
                              capture_output=True, text=True, timeout=10)

    def test_runner_executes_each_selected_suite_once(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            helper = self.make_runner(root)
            for arguments, expected in (((), len(runner.CI_SUITES)), (("--harness",), len(HARNESS))):
                with self.subTest(arguments=arguments):
                    result = self.invoke(helper, *arguments)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stderr.count(f"Ran {expected} tests in "), 1)

    def test_runner_refuses_missing_empty_invalid_skipped_and_failed_suites(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            helper = self.make_runner(root)
            path = root / HARNESS[-1]
            for content in (None, "", "def invalid(", CONTENT.replace("pass", "self.fail('fixture')"),
                            CONTENT.replace("    def test_required", "    @unittest.skip('fixture')\n    def test_required"),
                            CONTENT.replace("test_required", "not_a_test"),
                            CONTENT + "def load_tests(*args):\n    return unittest.TestSuite()\n"):
                if content is None:
                    path.unlink()
                else:
                    path.write_text(content)
                for arguments in ((), ("--harness",)):
                    with self.subTest(content=content, arguments=arguments):
                        result = self.invoke(helper, *arguments)
                        self.assertNotEqual(result.returncode, 0, result.stderr)
                path.write_text(CONTENT)

    def test_runner_refuses_optimized_python_and_unknown_groups(self):
        with tempfile.TemporaryDirectory() as temporary:
            helper = self.make_runner(Path(temporary))
            for arguments, flags, message in (((), ("-O",), "optimized Python"),
                                             (("--unknown",), (), "unknown unit test group")):
                result = self.invoke(helper, *arguments, flags=flags)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)

    def test_runner_adds_support_module_path_in_both_modes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            helper = self.make_runner(root)
            (root / "tests/support/helper.py").write_text("VALUE = 7\n")
            (root / HARNESS[-1]).write_text(CONTENT.replace("import unittest", "import unittest\nfrom helper import VALUE")
                                              .replace("pass", "self.assertEqual(VALUE, 7)"))
            for arguments in ((), ("--harness",)):
                result = self.invoke(helper, *arguments)
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
