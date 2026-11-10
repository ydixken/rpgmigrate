"""Portable negative controls for the current pgoutput snapshot qualification gate."""

from contextlib import redirect_stderr, redirect_stdout
import copy
import io
import json
from pathlib import Path
import runpy
import struct
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from db_harness import load_manifest, module_cases
import follow_snapshot_fixture as fixture
import follow_snapshot_fault as relay

ROOT = Path(__file__).resolve().parents[2]
WRAPPER = ROOT / "tools/test-db.py"
CASES = ["snapshot::follow_fault_tests::" + name for name in (
    "live_follow_empty_snapshot", "live_follow_wrong_source_refused", "live_follow_control_permission_denied",
    "live_follow_create_reply_lost", "live_follow_create_tail_lost", "live_follow_expired_export_refused",
    "live_follow_imported_identity_mismatch", "live_follow_source_finish_unknown", "live_follow_application_name_preserved",
)]
COMMAND = ["cargo", "test", "--locked", "--lib", "snapshot::follow_fault_tests::live_follow_",
           "--", "--ignored", "--test-threads=1"]
RECORDS = "".join(f"test {name} ... ok\n" for name in CASES)
OUTPUT = RECORDS + "test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 600 filtered out; finished in 0.01s\n"


class FollowSnapshotHarnessTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="follow-snapshot-portable-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.context = {"root": str(self.root), "token": "ab" * 16, "server_version_num": 160015,
                        "socket_dir": str(self.root / "socket"), "port": 24567}
        (self.root / ".e1-owner").write_text(self.context["token"])
        self.number = 0

    def phase(self, case, phase=None):
        self.number += 1
        number = self.number
        name = f"m3_fault_{number}"
        native_dir = self.root / name
        native_dir.mkdir()
        run = [number] * 16
        nonce = [number + 20] * 16
        stream = {"run": run, "source": {"system_identifier": 7141234567890123456, "database_oid": 20000 + number}, "generation": 1}
        publication = {"name": name + "_pub", "oid": 30000 + number, "owner_oid": 10}
        binding = {"stream": stream, "slot_name": name + "_slot", "plugin": "pgoutput", "catalog_epoch": 1}
        intent = {"binding": binding, "nonce": nonce}
        create_loss = case in ("create_reply_lost", "create_tail_lost")
        point = 256 + number
        record = intent if create_loss else {"intent": intent, "consistent_point": point, "snapshot_token": "00000001-00000001-1"}
        native = {
            "magic": "RPGMIGRATE", "format_version": 10, "run": run, "canonical_path": str(native_dir),
            "directory_device": native_dir.stat().st_dev, "directory_inode": native_dir.stat().st_ino,
            "copy_complete": None, "target_provenance": [], "clone_state": None, "operator": None,
            "source_slot": {"phase": "provisioning" if create_loss else "confirmed", "record": record},
            "follow_state": {"effective_settings": {"durability": "durable", "barrier_interval_ms": None, "apply_group_txns": 100, "limits": {"channel_bytes": 8388608, "channel_messages": 1024, "max_message_bytes": 16777216, "group_rows": 10000, "group_bytes": 8388608}}, "cleanup": None, "origin_name": None, "binding": None, "restart": None,
                             "preliminary": {"source": {"identity": stream["source"], "timeline": 1, "major": self.context["server_version_num"] // 10000}, "generation": 1, "tables": [{"source_oid": 42, "name": {"schema": "public", "name": "items"}, "writable_columns": ["id", "value"], "all_columns": ["id", "value"], "identity_modes": [97, 0], "partition_identity_modes": None}]},
                             "publication": {"phase": "confirmed", "intent": {"run": run, "name": publication["name"], "nonce": [number + 40] * 16}, "receipt": publication}},
        }
        path = native_dir / "manifest.json"
        path.write_text(json.dumps(native))
        phase = phase or case
        changed = {"ready_transaction": "T", "ready_failed": "E"}.get(phase)
        value = {
            "owner": self.context["token"], "profile": "follow_snapshot", "case": case, "phase": phase, "status": "observed",
            "server_version_num": self.context["server_version_num"], "native_path": str(path), "stream": stream,
            "publication": publication, "slot": {"name": binding["slot_name"], "plugin": "pgoutput", "database": name,
                                                   "database_oid": stream["source"]["database_oid"], "temporary": False, "consistent_point": f"0/{point:X}"},
            "source_slot_phase": native["source_slot"]["phase"], "slot_nonce": nonce, "publication_before_slot": True,
            "intent_before_create": True, "sql_pid": 40000 + number, "exporter_pid": 50000 + number,
            "helper_pid": 60000 + number, "armed": True, "fault_hits": 1, "client_failed": True,
            "fault_before_deadline": True, "failure_before_deadline": True, "deadline_seconds": 20,
            "sql_closed": True, "exporter_closed": True, "helper_reaped": True, "helper_returncode": 0,
            "retry_refused": create_loss, "create_dispatches": 1,
            "details": {"import_seen": not create_loss, "imported": case in ("imported_identity_mismatch", "source_finish_unknown"),
                        "identity_before": 40000 + number if case == "imported_identity_mismatch" else None,
                        "identity_after": 70000 + number if case == "imported_identity_mismatch" else None,
                        "row_seen": create_loss, "row_delivered": case == "create_tail_lost", "completion_seen": create_loss,
                        "command_tag": "CREATE_REPLICATION_SLOT" if create_loss else "COMMIT" if case == "source_finish_unknown" else None,
                        "ready_before": "I" if create_loss else None, "ready_after": changed,
                        "client_closed_after_ready": changed is not None, "exporter_expired": case == "expired_export_refused",
                        "sqlstate": fixture.EXPIRY_STATES[self.context["server_version_num"]] if case == "expired_export_refused" else None},
        }
        return value

    def records(self):
        result = {}
        for case in fixture.FAULT_CASES:
            if case == "create_tail_lost":
                value = {"owner": self.context["token"], "profile": "follow_snapshot", "case": case, "status": "observed",
                         "server_version_num": self.context["server_version_num"], "fault_hits": 3,
                         "phases": {phase: self.phase(case, phase) for phase in fixture.TAIL_PHASES}}
            else:
                value = self.phase(case)
            result[case] = value
            (self.root / f"follow-snapshot-fault-{case}.json").write_text(json.dumps(value))
        return result

    def test_inventory_is_exact_nine_and_separate_from_source_cleanup(self):
        self.assertEqual(fixture.follow_snapshot_cases(), CASES)
        self.assertEqual(module_cases({}, "FollowSnapshot"), CASES)
        self.assertEqual(len(module_cases({}, "FollowSource")), 12)
        self.assertTrue(set(CASES).isdisjoint(module_cases({}, "FollowSource")))
        for bad in ([], CASES[:-1], [CASES[0]] * 9, [*CASES[:-1], "unrelated::case"]):
            with self.subTest(bad=bad), patch.object(fixture, "read_json", return_value=bad), self.assertRaises(AssertionError):
                fixture.follow_snapshot_cases()

    def test_all_five_faults_and_three_tail_phases_have_valid_native_bindings(self):
        self.records()
        fixture.validate_follow_snapshot_faults(self.context)

    def relay_state(self, value):
        relay_state = relay.Relay.__new__(relay.Relay)
        relay_state.native_path = Path(value["native_path"])
        relay_state.context = self.context
        relay_state.source_identity = value["stream"]["source"]
        relay_state.source_timeline = 1
        relay_state.publication_name = value["publication"]["name"]
        relay_state.slot_name = value["slot"]["name"]
        return relay_state

    def test_candidate_native_format_is_explicit_and_relay_checks_it(self):
        value = self.phase("source_finish_unknown")
        path = Path(value["native_path"])
        native = json.loads(path.read_text())
        relay_state = self.relay_state(value)
        for version in (10,):
            native["format_version"] = version
            path.write_text(json.dumps(native))
            fixture.validate_phase(value, self.context, value["case"], value["phase"])
            self.assertEqual(relay_state.native()[0]["format_version"], version)
        for version in (1, 2, 3, 4, 5, 6, 7, 8, 9, 999, True, 10.0):
            native["format_version"] = version
            path.write_text(json.dumps(native))
            with self.subTest(version=version):
                with self.assertRaises(AssertionError):
                    relay_state.native()
                with self.assertRaises(AssertionError):
                    fixture.validate_phase(value, self.context, value["case"], value["phase"])

    def test_m9_native_settings_are_required_and_strict_before_resource_evidence(self):
        value = self.phase("source_finish_unknown")
        path = Path(value["native_path"])
        native = json.loads(path.read_text())
        valid = native["follow_state"]["effective_settings"]
        relay_state = self.relay_state(value)
        for mode, cadence in (("durable", None), ("replay-safe", 100)):
            candidate = copy.deepcopy(native)
            candidate["follow_state"]["effective_settings"].update(durability=mode, barrier_interval_ms=cadence)
            path.write_text(json.dumps(candidate))
            fixture.validate_phase(value, self.context, value["case"], value["phase"])
            relay_state.native()
        invalid = [None, {}, {**valid, "unknown": 1}]
        invalid.extend({key: item for key, item in valid.items() if key != missing} for missing in valid)
        for key, bad in (("durability", "relaxed"), ("durability", "Durable"),
                         ("barrier_interval_ms", 1), ("apply_group_txns", 0),
                         ("apply_group_txns", True), ("apply_group_txns", 1.0),
                         ("apply_group_txns", 2**64), ("limits", None)):
            invalid.append({**valid, key: bad})
        for cadence in (None, 0, -1, True, 1.0, 2**64):
            invalid.append({**valid, "durability": "replay-safe", "barrier_interval_ms": cadence})
        for key, maximum in valid["limits"].items():
            for bad in (0, True, 1.0, maximum + 1):
                invalid.append({**valid, "limits": {**valid["limits"], key: bad}})
            invalid.append({**valid, "limits": {name: item for name, item in valid["limits"].items() if name != key}})
        invalid.append({**valid, "limits": {**valid["limits"], "extra": 1}})
        for settings in invalid:
            candidate = copy.deepcopy(native)
            candidate["follow_state"]["effective_settings"] = settings
            path.write_text(json.dumps(candidate))
            with self.subTest(settings=settings):
                with self.assertRaises(AssertionError):
                    fixture.validate_phase(value, self.context, value["case"], value["phase"])
                with self.assertRaises(AssertionError):
                    relay_state.native()
        encoded = json.dumps(native)
        duplicate = encoded.replace('"apply_group_txns": 100', '"apply_group_txns": 100,"apply_group_txns": 23')
        self.assertNotEqual(duplicate, encoded)
        path.write_text(duplicate)
        with self.assertRaises(AssertionError):
            fixture.validate_phase(value, self.context, value["case"], value["phase"])
        with self.assertRaises(AssertionError):
            relay_state.native()
        del native["follow_state"]["effective_settings"]
        path.write_text(json.dumps(native))
        with self.assertRaises(AssertionError):
            fixture.validate_phase(value, self.context, value["case"], value["phase"])
        with self.assertRaises(AssertionError):
            relay_state.native()

    def test_fault_inventory_rejects_missing_extra_duplicate_and_pending_records(self):
        records = self.records()
        first = self.root / "follow-snapshot-fault-create_reply_lost.json"
        first.unlink()
        with self.assertRaises(AssertionError):
            fixture.validate_follow_snapshot_faults(self.context)
        first.write_text(json.dumps(records["create_reply_lost"]))
        extra = self.root / "follow-snapshot-fault-unexpected.json"
        extra.write_text("{}")
        with self.assertRaises(AssertionError):
            fixture.validate_follow_snapshot_faults(self.context)
        extra.unlink()
        first.write_text(json.dumps(records["create_reply_lost"]).replace('"status": "observed"', '"status":"observed","status":"pending"'))
        with self.assertRaises(AssertionError):
            fixture.validate_follow_snapshot_faults(self.context)
        first.write_text(json.dumps({**records["create_reply_lost"], "status": "pending"}))
        with self.assertRaises(AssertionError):
            fixture.validate_follow_snapshot_faults(self.context)

    def test_phase_rejects_missing_fields_bad_processes_unreaped_helpers_and_late_faults(self):
        value = self.phase("create_reply_lost")
        for key in fixture.PHASE_FIELDS:
            bad = copy.deepcopy(value)
            del bad[key]
            with self.subTest(missing=key), self.assertRaises(AssertionError):
                fixture.validate_phase(bad, self.context, "create_reply_lost", "create_reply_lost")
        for key, bad_value in (("owner", "foreign"), ("fault_hits", 0), ("fault_hits", True), ("create_dispatches", 2),
                               ("retry_refused", False), ("helper_returncode", 9), ("helper_reaped", False),
                               ("sql_closed", False), ("exporter_closed", False), ("sql_pid", 0), ("helper_pid", True),
                               ("exporter_pid", value["sql_pid"]), ("fault_before_deadline", False),
                               ("failure_before_deadline", False), ("publication_before_slot", False), ("intent_before_create", False)):
            with self.subTest(key=key), self.assertRaises(AssertionError):
                fixture.validate_phase({**value, key: bad_value}, self.context, "create_reply_lost", "create_reply_lost")

    def test_native_identity_modes_require_exact_fields_widths_and_bytes(self):
        value = self.phase("source_finish_unknown")
        path = Path(value["native_path"])
        native = json.loads(path.read_text())
        fixture.validate_phase(value, self.context, value["case"], value["phase"])
        table = native["follow_state"]["preliminary"]["tables"][0]
        for key in ("identity_modes", "partition_identity_modes"):
            bad = copy.deepcopy(native)
            del bad["follow_state"]["preliminary"]["tables"][0][key]
            path.write_text(json.dumps(bad))
            with self.subTest(missing=key), self.assertRaises(AssertionError):
                fixture.validate_phase(value, self.context, value["case"], value["phase"])
            for modes in ([], [0], [0, 0, 0], [True, 0], [98, 0], ["a", 0], {}):
                bad = copy.deepcopy(native)
                bad["follow_state"]["preliminary"]["tables"][0][key] = modes
                path.write_text(json.dumps(bad))
                with self.subTest(key=key, modes=modes), self.assertRaises(AssertionError):
                    fixture.validate_phase(value, self.context, value["case"], value["phase"])
        for change in ({"identity_modes": None}, {"unknown_identity": []}):
            bad = copy.deepcopy(native)
            bad["follow_state"]["preliminary"]["tables"][0].update(change)
            path.write_text(json.dumps(bad))
            with self.subTest(change=change), self.assertRaises(AssertionError):
                fixture.validate_phase(value, self.context, value["case"], value["phase"])
        for modes in ([0, 0], [97, 100]):
            table["partition_identity_modes"] = modes
            path.write_text(json.dumps(native))
            fixture.validate_phase(value, self.context, value["case"], value["phase"])

    def test_native_nullable_fields_are_required_and_old_or_foreign_state_is_refused(self):
        value = self.phase("source_finish_unknown")
        path = Path(value["native_path"])
        native = json.loads(path.read_text())
        for field in fixture.NATIVE_FIELDS:
            bad = copy.deepcopy(native)
            del bad[field]
            path.write_text(json.dumps(bad))
            with self.subTest(missing=field), self.assertRaises(AssertionError):
                fixture.validate_phase(value, self.context, value["case"], value["phase"])
        for key, bad_value in (("format_version", 4), ("format_version", 5), ("format_version", 6), ("format_version", 7), ("format_version", 8), ("format_version", 9), ("format_version", True), ("format_version", 10.0), ("copy_complete", {}),
                               ("target_provenance", [{}]), ("clone_state", {}), ("run", [0] * 16)):
            path.write_text(json.dumps({**native, key: bad_value}))
            with self.subTest(key=key), self.assertRaises(AssertionError):
                fixture.validate_phase(value, self.context, value["case"], value["phase"])
        for field in ("effective_settings", "origin_name", "binding", "restart", "preliminary", "publication", "cleanup"):
            bad = copy.deepcopy(native)
            del bad["follow_state"][field]
            path.write_text(json.dumps(bad))
            with self.subTest(missing_follow=field), self.assertRaises(AssertionError):
                fixture.validate_phase(value, self.context, value["case"], value["phase"])
        bad = copy.deepcopy(native)
        bad["follow_state"]["cleanup"] = {"slot_removed": False, "publication_removed": False}
        path.write_text(json.dumps(bad))
        with self.assertRaises(AssertionError):
            fixture.validate_phase(value, self.context, value["case"], value["phase"])
        path.write_text(json.dumps(native))
        outside = self.root.parent / "missing-manifest.json"
        with self.assertRaises((AssertionError, OSError)):
            fixture.validate_phase({**value, "native_path": str(outside)}, self.context, value["case"], value["phase"])

    def test_native_slot_publication_and_source_identities_are_exact(self):
        value = self.phase("expired_export_refused")
        for key, bad_value in (("publication", {**value["publication"], "oid": "30001"}),
                               ("publication", {**value["publication"], "oid": 99999}),
                               ("slot_nonce", [0] * 16), ("source_slot_phase", "provisioning"),
                               ("slot", {**value["slot"], "database_oid": "20001"}),
                               ("slot", {**value["slot"], "plugin": "test_decoding"}),
                               ("slot", {**value["slot"], "temporary": True}),
                               ("slot", {**value["slot"], "consistent_point": "0/FF"}),
                               ("stream", {**value["stream"], "generation": True})):
            with self.subTest(key=key), self.assertRaises(AssertionError):
                fixture.validate_phase({**value, key: bad_value}, self.context, value["case"], value["phase"])

    def test_tail_phases_require_all_three_distinct_resource_receipts(self):
        value = self.records()["create_tail_lost"]
        for phases in ({}, {key: item for key, item in value["phases"].items() if key != "ready_failed"},
                       {**value["phases"], "unexpected": value["phases"]["tail_lost"]}):
            with self.subTest(phases=list(phases)), self.assertRaises(AssertionError):
                fixture.validate_fault({**value, "phases": phases}, self.context, "create_tail_lost")
        phase = value["phases"]["ready_transaction"]
        for details in ({**phase["details"], "ready_after": "I"}, {**phase["details"], "client_closed_after_ready": False}):
            with self.subTest(details=details), self.assertRaises(AssertionError):
                fixture.validate_phase({**phase, "details": details}, self.context, "create_tail_lost", "ready_transaction")

    def test_post_import_pid_mismatch_cannot_pass_without_the_intended_mutation(self):
        value = self.phase("imported_identity_mismatch")
        for key, bad_value in (("import_seen", False), ("imported", False), ("identity_before", None),
                               ("identity_after", None), ("identity_after", value["sql_pid"]), ("identity_after", True)):
            details = {**value["details"], key: bad_value}
            with self.subTest(key=key), self.assertRaises(AssertionError):
                fixture.validate_phase({**value, "details": details}, self.context, value["case"], value["phase"])

    def test_slot_export_expiry_requires_actual_major_specific_sqlstate(self):
        for number, expected in ((160015, "22023"), (180006, "42704")):
            self.context["server_version_num"] = number
            value = self.phase("expired_export_refused")
            self.assertEqual(value["details"]["sqlstate"], expected)
            fixture.validate_phase(value, self.context, value["case"], value["phase"])
            for state in (None, "XX000", "22023" if expected == "42704" else "42704"):
                with self.subTest(number=number, state=state), self.assertRaises(AssertionError):
                    fixture.validate_phase({**value, "details": {**value["details"], "sqlstate": state}}, self.context, value["case"], value["phase"])

    def test_finish_unknown_requires_real_import_then_commit_fault(self):
        value = self.phase("source_finish_unknown")
        for key, bad_value in (("imported", False), ("import_seen", False), ("command_tag", "CREATE_REPLICATION_SLOT")):
            with self.subTest(key=key), self.assertRaises(AssertionError):
                fixture.validate_phase({**value, "details": {**value["details"], key: bad_value}}, self.context, value["case"], value["phase"])

    def test_helper_data_row_parser_and_pid_edit_are_bounded_and_change_only_fifth_field(self):
        def row(values):
            return struct.pack("!H", len(values)) + b"".join(struct.pack("!i", len(value)) + value for value in values)
        original = [b"7141234567890123456", b"20000", b"1", b"f", b"123"]
        payload = row(original)
        self.assertEqual(relay.fields(payload), original)
        frame, replacement = relay.changed_pid(payload, 123)
        self.assertEqual(frame[:1], b"D")
        self.assertEqual(struct.unpack("!I", frame[1:5])[0], len(frame) - 1)
        self.assertEqual(relay.fields(frame[5:])[:4], original[:4])
        self.assertEqual(relay.fields(frame[5:])[4], str(replacement).encode())
        self.assertNotEqual(replacement, 123)
        for bad in (b"", payload[:-1], payload + b"extra", b"\0\1" + struct.pack("!i", -1), b"\0\2" + struct.pack("!i", 0)):
            with self.subTest(payload=bad), self.assertRaises(AssertionError):
                relay.fields(bad)
        for bad in (row(original[:4]), row([*original[:4], b"124"])):
            with self.subTest(payload=bad), self.assertRaises(AssertionError):
                relay.changed_pid(bad, 123)
        with self.assertRaises(AssertionError):
            relay.unique_fields([("op", "arm"), ("op", "quit")])

    def test_runner_requires_exact_nine_successes_one_summary_and_all_fault_receipts(self):
        self.records()
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        validate("FollowSnapshot", CASES, OUTPUT, self.context)
        for output in ("", RECORDS, OUTPUT + OUTPUT[len(RECORDS):], OUTPUT.replace("9 passed", "0 passed"),
                       OUTPUT.replace("0 ignored", "1 ignored"), OUTPUT.replace(" ... ok", " ... ignored", 1),
                       OUTPUT.replace(CASES[0], "unrelated::case"), OUTPUT + RECORDS):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate("FollowSnapshot", CASES, output, self.context)
        (self.root / "follow-snapshot-fault-source_finish_unknown.json").unlink()
        with self.assertRaises(AssertionError):
            validate("FollowSnapshot", CASES, OUTPUT, self.context)

    def test_runner_prepares_logical_owned_environment_and_validates_faults_after_child(self):
        self.records()
        context_path = self.root / "context.json"
        context_path.write_text(json.dumps(self.context))
        helper = self.root / "tests/support/follow_snapshot_fault.py"
        helper.parent.mkdir(parents=True)
        helper.touch()
        cluster = SimpleNamespace(context=self.context, context_path=context_path, root=self.root,
                                  bindir=self.root / "bin", environment={"PATH": "/usr/bin"}, auth_password="synthetic")
        manifest = {**load_manifest(), "server_version": "16.15", "server_version_num": 160015}
        seen = {}
        class Cluster:
            def __enter__(self):
                return cluster
            def __exit__(self, *_args):
                return False
        def owned(actual, **kwargs):
            self.assertEqual(actual, manifest)
            seen["cluster"] = kwargs
            return Cluster()
        def databases(_context, _case, *, empty=False):
            self.assertTrue(empty)
            return "owned_source", "owned_target"
        def command(argv, **kwargs):
            seen.update(command=argv, **kwargs)
            return subprocess.CompletedProcess(argv, 0, OUTPUT, "")
        main = runpy.run_path(str(WRAPPER))["main"]
        with patch.dict(main.__globals__, {"ROOT": self.root, "OwnedCluster": owned, "load_manifest": lambda: manifest,
                                          "databases": databases, "run_command": command}), \
                patch.object(sys, "argv", [str(WRAPPER), "--follow-snapshot", "--", *COMMAND]), redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(), 0)
        self.assertEqual(seen["cluster"], {"logical": True, "logical_slots": 32})
        self.assertEqual(seen["command"], COMMAND)
        env = seen["environment"]
        self.assertEqual(env["RPGMIGRATE_TEST_FOLLOW_SNAPSHOT_CONTEXT"], str(context_path))
        self.assertEqual(env["RPGMIGRATE_TEST_FOLLOW_SNAPSHOT_HELPER"], str(helper))
        self.assertEqual(env["PGAPPNAME"], "rpgmigrate_follow_snapshot_" + self.context["token"][:12])
        self.assertIn("dbname='postgres' user='e1_admin'", env["RPGMIGRATE_TEST_FOLLOW_SOURCE_ADMIN"])
        self.assertEqual(env["RPGMIGRATE_TEST_PYTHON"], sys.executable)
        self.assertIn("FollowSnapshot faults verified 5 cases, 3 CREATE tail phases", output.getvalue())

    def test_runner_refuses_missing_command_or_mixed_profiles_before_owned_server(self):
        main = runpy.run_path(str(WRAPPER))["main"]
        for args in (["--follow-snapshot"], ["--follow-snapshot", "--"],
                     ["--follow-snapshot", "--follow-source", "--", *COMMAND],
                     ["--source-snapshot", "--", *COMMAND]):
            with self.subTest(args=args), patch.object(sys, "argv", [str(WRAPPER), *args]), \
                    patch.dict(main.__globals__, {"OwnedCluster": lambda *_args, **_kwargs: self.fail("unexpected server start")}), \
                    redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main()
            self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(FollowSnapshotHarnessTests)
    if len(sys.argv) > 1:
        suite = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:], module=sys.modules[__name__])
    if not suite.countTestCases():
        raise RuntimeError("no current follow snapshot harness tests selected")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() and not result.skipped else 1)
