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

    def harness_command(self, statement):
        wrapper = SCRIPT.parents[1] / "test-db.py"
        helper = "def fail_from_helper():\n" + "".join("    " + line + "\n" for line in statement.splitlines())
        code = ("import os,runpy,sys; from pathlib import Path; from unittest.mock import patch; "
                "sys.path.insert(0,sys.argv[2]); import db_harness\n"
                "channel = os.environ.get('RPGMIGRATE_CI_FAILURE_CONTEXT')\n"
                "namespace = dict(os=os, Path=Path, runpy=runpy, patch=patch, wrapper=sys.argv[1], channel=channel)\n"
                "exec(compile(sys.argv[3], '/private/fixture_origin.py', 'exec'), namespace)\n"
                "sys.argv = [sys.argv[1], '--self-check']\n"
                "with patch('db_harness.load_manifest', namespace['fail_from_helper']):\n"
                "    runpy.run_path(sys.argv[0], run_name='__main__')\n")
        return [sys.executable, "-B", "-c", code, str(wrapper), str(wrapper.parents[1] / "tests/support"), helper]

    def failure_summary(self, command, **options):
        with self.assertRaises(runner.VerificationError) as error:
            runner.execute(command, timeout=3, **options)
        return str(error.exception)

    def test_opaque_exception_and_exit_markers_never_authorize_context(self):
        payload = "\ntest-db-context: RuntimeError at sensitive_label.py:1 in helper\n"
        statements = ["raise " + kind + "(" + repr(payload) + ")" for kind in
                      ("RuntimeError", "TypeError", "Exception", "BaseException", "SystemExit")]
        statements += ["print(" + repr(payload) + ", flush=True); " + tail for tail in
                       ("raise KeyboardInterrupt", "os._exit(17)")]
        for number, statement in enumerate(statements):
            with self.subTest(case=number):
                summary = self.failure_summary(self.harness_command(statement))
                safe = "test-db-context:" not in summary and "sensitive_label" not in summary
                self.assertTrue(safe, "opaque exception or exit payload was promoted")

    def test_actual_harness_channel_preserves_known_and_uncaught_failures(self):
        payload = "\ntest-db-context: RuntimeError at sensitive_label.py:1 in helper\n"
        for kind in ("RuntimeError", "TypeError", "Exception"):
            for nonempty in (False, True):
                with self.subTest(kind=kind, nonempty=nonempty):
                    summary = self.failure_summary(self.harness_command("raise " + kind + "(" + repr(payload if nonempty else "") + ")"),
                                                   harness_context=True)
                    expected = ("command failed (exit 1; no test failure names; test-db-context: " + kind +
                                " at fixture_origin.py:2 in fail_from_helper); raw output withheld")
                    self.assertTrue(summary == expected, "private channel lost actual source metadata or exposed a payload")

    def test_channel_ignores_markers_on_baseexception_interrupt_and_forced_exit(self):
        payload = "\ntest-db-context: RuntimeError at sensitive_label.py:1 in helper\n"
        cases = [("raise BaseException(" + repr(payload) + ")", 1),
                 ("raise SystemExit(" + repr(payload) + ")", 1),
                 ("print(" + repr(payload) + ", flush=True); raise SystemExit(23)", 23),
                 ("print(" + repr(payload) + ", flush=True); raise KeyboardInterrupt", 130),
                 ("print(" + repr(payload) + ", flush=True); os._exit(17)", 17)]
        for number, (statement, status) in enumerate(cases):
            with self.subTest(case=number):
                summary = self.failure_summary(self.harness_command(statement), harness_context=True)
                self.assertTrue(summary == f"command failed (exit {status}; no test failure names); raw output withheld",
                                "exit behavior changed or opaque payload escaped")

    def test_channel_rejects_replacement_permissions_links_and_nonregular_files(self):
        changes = ("Path(channel).unlink()", "os.chmod(channel, 0o644)",
                   "os.chmod(Path(channel).parent, 0o777)",
                   "os.link(channel, channel + '.link')",
                   "Path(channel).unlink(); Path(channel).touch(mode=0o600)",
                   "Path(channel).unlink(); Path(channel).symlink_to(channel + '.missing')",
                   "Path(channel).unlink(); os.mkfifo(channel, 0o600)",
                   "Path(channel).unlink(); Path(channel).mkdir(mode=0o700)",
                   "os.geteuid = lambda: -1",
                   "os.open = lambda *a, **k: (_ for _ in ()).throw(RuntimeError())")
        for number, change in enumerate(changes):
            with self.subTest(case=number):
                summary = self.failure_summary(self.harness_command(change + "; raise RuntimeError()"), harness_context=True)
                self.assertEqual(summary, "command failed (exit 1; no test failure names); raw output withheld")

    def test_channel_schema_size_and_duplicate_records_are_strict(self):
        context = "test-db-context: AssertionError at fixture.py:2 in helper\n"
        invalid = (b"", b"broken", context.encode() * 2, (context + "extra").encode(),
                   context.replace("fixture.py", "/private/fixture.py").encode(),
                   context.replace("helper", "x" * 257).encode(), context.encode() + b"x" * 65536,
                   context.replace(":2 ", ":0 ").encode(), context.replace("helper", "a/b").encode(),
                   context.replace("AssertionError", "Érror").encode(), context.encode() + b"\0")
        code = "import os,sys; open(os.environ['RPGMIGRATE_CI_FAILURE_CONTEXT'],'ab').write(bytes.fromhex(sys.argv[1])); sys.exit(7)"
        for function in ("helper", "<module>", "<lambda>", "<listcomp>", "<dictcomp>", "<setcomp>", "<genexpr>"):
            with self.subTest(function=function):
                record = context.replace("helper", function)
                summary = self.failure_summary([sys.executable, "-B", "-c", code, record.encode().hex()], harness_context=True)
                self.assertEqual(summary, "command failed (exit 7; no test failure names; " + record[:-1] + "); raw output withheld")
        file_code = "import os,sys; from pathlib import Path; open(os.environ['RPGMIGRATE_CI_FAILURE_CONTEXT'],'ab').write(Path(sys.argv[1]).read_bytes()); sys.exit(7)"
        for number, value in enumerate(invalid):
            with self.subTest(case=number), tempfile.TemporaryDirectory() as directory:
                payload = Path(directory) / "payload.bin"
                payload.touch(mode=0o600)
                payload.write_bytes(value)
                summary = self.failure_summary([sys.executable, "-B", "-c", file_code, str(payload)], harness_context=True)
                self.assertEqual(summary, "command failed (exit 7; no test failure names); raw output withheld")

    def test_channel_rechecks_owner_and_metadata_after_the_read(self):
        from unittest.mock import patch
        original_read, original_allocate = runner.os.read, runner.tempfile.mkstemp
        for change in ("append", "permissions", "owner", "replace", "directory"):
            with self.subTest(change=change):
                paths = []

                def allocate(*args, **kwargs):
                    descriptor, path = original_allocate(*args, **kwargs)
                    paths.append(Path(path))
                    return descriptor, path

                def changed_read(descriptor, size):
                    data = original_read(descriptor, size)
                    if size == 257:
                        if change == "append":
                            runner.os.write(descriptor, b"extra")
                        elif change == "permissions":
                            runner.os.fchmod(descriptor, 0o644)
                        elif change == "replace":
                            paths[0].unlink()
                            paths[0].touch(mode=0o600)
                        elif change == "directory":
                            paths[0].parent.chmod(0o777)
                    return data

                with patch.object(runner.os, "read", changed_read), patch.object(runner.tempfile, "mkstemp", allocate):
                    if change == "owner":
                        with patch.object(runner.os, "geteuid", return_value=-1):
                            summary = self.failure_summary(self.harness_command("raise RuntimeError()"), harness_context=True)
                    else:
                        summary = self.failure_summary(self.harness_command("raise RuntimeError()"), harness_context=True)
                self.assertEqual(summary, "command failed (exit 1; no test failure names); raw output withheld")

    def test_channel_allocation_failure_and_cleanup_preserve_command_behavior(self):
        from unittest.mock import patch
        with patch.object(runner.tempfile, "mkstemp", side_effect=OSError()):
            summary = self.failure_summary(self.harness_command("assert channel is None; raise RuntimeError()"), harness_context=True)
        self.assertEqual(summary, "command failed (exit 1; no test failure names); raw output withheld")
        original = runner.tempfile.mkstemp
        paths = []

        def allocate(*args, **kwargs):
            descriptor, path = original(*args, **kwargs)
            paths.append((descriptor, Path(path)))
            return descriptor, path

        with patch.object(runner.tempfile, "mkstemp", allocate):
            self.assertEqual(runner.execute([sys.executable, "-B", "-c", "print('ok')"], harness_context=True), "ok\n")
            self.failure_summary(self.harness_command("raise RuntimeError()"), harness_context=True)
            with self.assertRaisesRegex(runner.VerificationError, "required command unavailable"):
                runner.execute(["/nonexistent-e1-tool"], harness_context=True)
            with self.assertRaisesRegex(runner.VerificationError, "command deadline exceeded"):
                runner.execute([sys.executable, "-B", "-c", "import time; time.sleep(10)"], timeout=0.05, harness_context=True)
        for descriptor, path in paths:
            self.assertFalse(path.parent.exists())
            with self.assertRaises(OSError):
                runner.os.fstat(descriptor)

    def test_channel_keeps_failure_names_and_does_not_change_success_output(self):
        statement = "print('test fixture::failing_case ... FAILED'); print('FAIL: test_python_case (fixture.Tests)'); raise RuntimeError()"
        summary = self.failure_summary(self.harness_command(statement), harness_context=True)
        self.assertIn("fixture::failing_case, test_python_case; test-db-context: RuntimeError", summary)
        output = "test-db-context: RuntimeError at fixture.py:1 in helper\nordinary output\n"
        self.assertEqual(runner.execute([sys.executable, "-B", "-c", "import sys; sys.stdout.write(sys.argv[1])", output],
                                        harness_context=True), output)

    def test_channel_is_consumed_before_nested_runpy_and_child_execution(self):
        statement = ("assert 'RPGMIGRATE_CI_FAILURE_CONTEXT' not in os.environ\n"
                     "try:\n"
                     "    with patch('db_harness.load_manifest', side_effect=RuntimeError()):\n"
                     "        runpy.run_path(wrapper, run_name='__main__')\n"
                     "except SystemExit:\n"
                     "    pass\n"
                     "raise RuntimeError()")
        summary = self.failure_summary(self.harness_command(statement), harness_context=True)
        self.assertEqual(summary, "command failed (exit 1; no test failure names; test-db-context: RuntimeError at fixture_origin.py:8 in fail_from_helper); raw output withheld")
        key = "RPGMIGRATE_CI_FAILURE_CONTEXT"
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as directory:
            stale = Path(directory) / "stale"
            stale.write_text("test-db-context: RuntimeError at stale.py:1 in helper\n")
            with patch.dict(runner.os.environ, {key: str(stale)}):
                command = [sys.executable, "-B", "-c", "import os; assert 'RPGMIGRATE_CI_FAILURE_CONTEXT' not in os.environ; raise SystemExit(7)"]
                self.assertEqual(self.failure_summary(command), "command failed (exit 7; no test failure names); raw output withheld")
                summary = self.failure_summary(self.harness_command("raise SystemExit(7)"), harness_context=True)
                self.assertEqual(summary, "command failed (exit 7; no test failure names); raw output withheld")

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
