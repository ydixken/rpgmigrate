"""Portable controls for the disposable clone qualification harness."""

import importlib.util
import json
from pathlib import Path
import tempfile
import subprocess
import sys
import unittest


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
        for source, target in (({}, {}), ({"table": row}, {}), ({"table": row}, {"table": {**row, "rows": 2}})):
            with self.subTest(source=source, target=target), self.assertRaises(RuntimeError):
                harness.require_equal(source, target)


if __name__ == "__main__":
    unittest.main()
