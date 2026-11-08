"""Run with python3 -W error tests/support/test_journal_harness.py, without PostgreSQL."""

from contextlib import contextmanager, redirect_stderr, redirect_stdout
import copy
import io
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from db_harness import load_manifest, module_cases

SUPPORT = Path(__file__).resolve().parent
WRAPPER = SUPPORT.parents[1] / "tools" / "test-db.py"
CASES = ["snapshot::journal_live_tests::segmented_real_wal_reopens_and_serial_replays",
         "snapshot::journal_live_tests::sqlite_real_wal_reopens_and_serial_replays"]
CHILD = "snapshot::journal_live_tests::journal_serial_crash_child"
COMMAND = ["cargo", "test", "--locked", "--lib", "snapshot::journal_live_tests::", "--", "--ignored",
           "--skip", CHILD, "--test-threads=1"]
RECORDS = "".join(f"test {name} ... ok\n" for name in CASES)
SUMMARY = "test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 17 filtered out; finished in 0.01s\n"
OUTPUT = RECORDS + SUMMARY


def records(context):
    root = Path(context["root"])
    for index, candidate in enumerate(("segmented", "sqlite")):
        binding = {"stream": {"run": [index + 1] * 16, "generation": 9,
                   "source": {"system_identifier": 123, "database_oid": 42}},
                   "slot_name": "js_source1234_" + candidate, "plugin": "test_decoding", "catalog_epoch": 7}
        native = root / f"journal-{candidate}-native"
        native.mkdir(exist_ok=True)
        (native / "manifest.json").write_text(json.dumps({"magic": "RPGMIGRATE", "format_version": 4,
            "run": binding["stream"]["run"], "canonical_path": str(native), "copy_complete": None,
            "copy_snapshot": None,
            "source_slot": {"phase": "confirmed", "record": {"intent": {"binding": binding, "nonce": [1] * 16},
                           "consistent_point": 16, "snapshot_token": "owned-token"}}}))
        journal = root / f"journal-{candidate}-records"
        journal.mkdir(exist_ok=True)
        proof = root / f"journal-{candidate}-external.bin"
        # The Rust producer decodes descriptors; this oracle must match every retained byte.
        descriptor = b"RPGEXP02" + bytes(range(120))
        proof.write_bytes(descriptor)
        witness = root / f"journal-{candidate}-kill.json"
        child_pid = 100 + index
        witness.write_text(json.dumps({"phase": "before_commit", "pid": child_pid,
                                      "binding": binding, "proof": str(proof)}))
        request = {"native": str(native), "journal": str(journal), "proof": str(proof),
                   "witness": str(witness), "binding": binding, "candidate": candidate}
        witness.with_suffix(".request.json").write_text(json.dumps(request))
        crash = {"owner": context["token"], "candidate": candidate, "child_test": CHILD,
                 "parent_pid": 90, "child_pid": child_pid, "binding": binding,
                 "descriptor_hex": descriptor.hex(), "witness_path": str(witness),
                 "before_commit": True, "signal": 9, "reaped": True, "deadline_ms": 30000, "elapsed_ms": 10}
        (root / f"journal-{candidate}-crash-receipt.json").write_text(json.dumps(crash))
        fault = {"owner": context["token"], "case": "journal_" + candidate, "status": "observed",
                 "fault_hits": 1, "backend_pid": 200 + index, "commit_tag": "COMMIT", "holding_backend": False,
                 "backend_connection_closed": True, "held_fence_observed": True, "released_fence_observed": True}
        (root / f"authority-fault-journal_{candidate}.json").write_text(json.dumps(fault))


class JournalHarnessTests(unittest.TestCase):
    def context(self, directory):
        return {"root": directory, "token": "owned-journal", "journal_source_identity":
                {"system_identifier": 123, "database_oid": 42}, "replication": {"token": "source1234567890"}}

    def test_exact_two_parents_are_separate_from_all_existing_profiles(self):
        manifest = load_manifest()
        try:
            actual = module_cases(manifest, "JournalSerial")
        except ValueError:
            self.fail("JournalSerial profile missing")
        self.assertEqual(actual, CASES)
        self.assertNotIn(CHILD, actual)
        for profile, count in (("Engine", 41), ("Authority", 18), ("SourceSnapshot", 12),
                               ("SourceCatalog", 3), ("Catalog", 10), ("Replication", 9)):
            names = module_cases(manifest, profile)
            self.assertEqual(len(names), count)
            self.assertTrue(set(names).isdisjoint(CASES))
        self.assertEqual(len(manifest["cases"]), 40)

    def test_summary_cannot_hide_failed_ignored_missing_duplicate_or_extra_cases(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        for output in (RECORDS, OUTPUT + SUMMARY, OUTPUT.replace("2 passed", "0 passed"),
                       OUTPUT.replace("0 failed", "1 failed"), OUTPUT.replace("0 ignored", "1 ignored"),
                       OUTPUT.replace("0 measured", "1 measured")):
            with self.subTest(output=output), self.assertRaisesRegex(AssertionError, "summary"):
                validate("JournalSerial", CASES, output, {})
        for output in ("", OUTPUT + f"test {CHILD} ... ok\n", OUTPUT + f"test {CASES[0]} ... ok\n",
                       OUTPUT.replace(f"test {CASES[0]} ... ok\n", ""),
                       OUTPUT.replace(" ... ok", " ... ignored", 1), OUTPUT.replace(" ... ok", " ... FAILED", 1)):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate("JournalSerial", CASES, output, {})

    def test_exact_receipts_require_both_post_wait_children_and_both_commit_faults(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        with tempfile.TemporaryDirectory() as directory:
            context = self.context(directory)
            records(context)
            validate("JournalSerial", CASES, OUTPUT, context)
            for candidate in ("segmented", "sqlite"):
                for name in (f"journal-{candidate}-crash-receipt.json", f"authority-fault-journal_{candidate}.json"):
                    path = Path(directory) / name
                    value = path.read_bytes()
                    path.unlink()
                    with self.subTest(name=name), self.assertRaises((AssertionError, FileNotFoundError)):
                        validate("JournalSerial", CASES, OUTPUT, context)
                    path.write_bytes(value)
            for name in ("journal-extra-crash-receipt.json", "authority-fault-lost_commit_reply.json"):
                path = Path(directory) / name
                path.write_text("{}")
                with self.assertRaises(AssertionError):
                    validate("JournalSerial", CASES, OUTPUT, context)
                path.unlink()

    def test_child_receipt_must_prove_the_exact_selection_signal_reap_deadline_and_binding(self):
        from journal_fixture import validate_journal_receipts
        with tempfile.TemporaryDirectory() as directory:
            context = self.context(directory)
            records(context)
            path = Path(directory) / "journal-segmented-crash-receipt.json"
            original = json.loads(path.read_text())
            for key, bad in (("owner", "foreign"), ("candidate", "redb"), ("child_test", CASES[0]),
                             ("child_pid", 0), ("child_pid", True), ("child_pid", 90), ("before_commit", False),
                             ("signal", 15), ("signal", True), ("reaped", False), ("deadline_ms", 0),
                             ("deadline_ms", 30001), ("deadline_ms", True), ("elapsed_ms", 30000),
                             ("elapsed_ms", -1), ("descriptor_hex", "00"), ("descriptor_hex", "AB" * 128),
                             ("witness_path", str(Path(directory).parent / "foreign.json"))):
                path.write_text(json.dumps({**original, key: bad}))
                with self.subTest(key=key, bad=bad), self.assertRaises(AssertionError):
                    validate_journal_receipts(context)
            for key in original:
                changed = dict(original)
                del changed[key]
                path.write_text(json.dumps(changed))
                with self.subTest(missing=key), self.assertRaises((AssertionError, KeyError)):
                    validate_journal_receipts(context)
            path.write_text(json.dumps({**original, "unexpected": True}))
            with self.assertRaises(AssertionError):
                validate_journal_receipts(context)

    def test_descriptor_witness_request_and_native_record_must_match_the_receipt(self):
        from journal_fixture import validate_journal_receipts
        with tempfile.TemporaryDirectory() as directory:
            context = self.context(directory)
            records(context)
            root = Path(directory)
            validate_journal_receipts(context)
            for name, key, bad in (("journal-segmented-kill.json", "phase", "after_commit"),
                                  ("journal-segmented-kill.json", "pid", 999),
                                  ("journal-segmented-kill.request.json", "candidate", "sqlite"),
                                  ("journal-segmented-native/manifest.json", "format_version", 2),
                                  ("journal-segmented-native/manifest.json", "format_version", 3),
                                  ("journal-segmented-native/manifest.json", "format_version", 5),
                                  ("journal-segmented-native/manifest.json", "copy_snapshot", {}),
                                  ("journal-segmented-native/manifest.json", "source_slot", None)):
                path = root / name
                original = path.read_bytes()
                changed = json.loads(original)
                changed[key] = bad
                path.write_text(json.dumps(changed))
                with self.subTest(name=name, key=key), self.assertRaises((AssertionError, KeyError, TypeError)):
                    validate_journal_receipts(context)
                path.write_bytes(original)
            native = root / "journal-segmented-native/manifest.json"
            value = json.loads(native.read_text())
            del value["copy_snapshot"]
            native.write_text(json.dumps(value))
            with self.assertRaises(KeyError):
                validate_journal_receipts(context)
            records(context)
            path = root / "journal-segmented-external.bin"
            path.write_bytes(b"changed" + bytes(121))
            with self.assertRaises(AssertionError):
                validate_journal_receipts(context)

    def test_journal_faults_cannot_use_pending_unarmed_or_unreleased_commit_evidence(self):
        from journal_fixture import validate_journal_receipts
        from authority_fault import FAULT_CASES, JOURNAL_CASES
        self.assertEqual(FAULT_CASES, ("lost_commit_reply", "provisioning_recovery"))
        self.assertEqual(JOURNAL_CASES, ("journal_segmented", "journal_sqlite"))
        with tempfile.TemporaryDirectory() as directory:
            context = self.context(directory)
            records(context)
            path = Path(directory) / "authority-fault-journal_segmented.json"
            original = json.loads(path.read_text())
            for key, bad in (("owner", "foreign"), ("case", "lost_commit_reply"), ("status", "pending"),
                             ("fault_hits", 0), ("fault_hits", True), ("backend_pid", 0),
                             ("commit_tag", "ROLLBACK"), ("holding_backend", True),
                             ("backend_connection_closed", False), ("held_fence_observed", False),
                             ("released_fence_observed", False)):
                path.write_text(json.dumps({**original, key: bad}))
                with self.subTest(key=key), self.assertRaises(AssertionError):
                    validate_journal_receipts(context)

    @staticmethod
    def forbidden(*_args, **_kwargs):
        raise AssertionError("unexpected fixture start")

    def test_profile_rejects_missing_command_or_combination_before_fixture_start(self):
        main = runpy.run_path(str(WRAPPER))["main"]
        for args in (["--journal-serial"], ["--journal-serial", "--"],
                     *(["--journal-serial", mode, "--", *COMMAND] for mode in
                       ("--engine", "--authority", "--replication", "--catalog", "--source-catalog",
                        "--source-snapshot", "--prepared", "--transport", "--self-check"))):
            with patch.dict(main.__globals__, {"OwnedCluster": self.forbidden}), \
                    patch.object(sys, "argv", [str(WRAPPER), *args]), redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit) as error:
                main()
            self.assertEqual(error.exception.code, 2)

    def run_journal(self, output=OUTPUT, returncode=0, setup_error=False):
        main = runpy.run_path(str(WRAPPER))["main"]
        context = {"root": "/owned/journal", "token": "owned-target", "socket_dir": "/owned/target-socket", "port": 24567}
        source = {"root": "/owned/journal-source", "token": "owned-source", "database": "replication_source",
                  "replication_role": "replication_stream"}
        cluster = SimpleNamespace(context=context, context_path=Path(context["root"]) / "context.json",
                                  root=Path(context["root"]), bindir=Path("/owned/pg/bin"),
                                  environment={"PATH": "/usr/bin"}, auth_password="synthetic")
        observed = []

        @contextmanager
        def owned(_manifest):
            yield cluster

        @contextmanager
        def authority(actual):
            self.assertIs(actual, cluster)
            observed.append("authority")
            yield {"RPGMIGRATE_TEST_AUTHORITY_TARGET": "owned ordinary target",
                   "RPGMIGRATE_TEST_AUTHORITY_ADMIN": "owned target admin",
                   "RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER": "owned authority helper"}

        @contextmanager
        def replication(_manifest):
            observed.append("replication")
            yield source, {"RPGMIGRATE_TEST_REPLICATION_SQL": "owned ordinary source",
                           "RPGMIGRATE_TEST_REPLICATION_SOURCE": "owned logical source",
                           "RPGMIGRATE_TEST_REPLICATION_ADMIN": "owned source admin",
                           "RPGMIGRATE_TEST_REPLICATION_CONTEXT": "/owned/journal-source/context.json"}

        def prepare(actual, actual_source):
            self.assertIs(actual, context)
            self.assertIs(actual_source, source)
            self.assertEqual(observed, ["authority", "replication"])
            if setup_error:
                raise RuntimeError("owned journal setup failed")
            observed.append("prepared")

        def command(actual_command, **options):
            self.assertEqual(actual_command, COMMAND)
            self.assertEqual(observed, ["authority", "replication", "prepared"])
            self.assertTrue(options["capture_output"])
            self.assertEqual(options["timeout"], 600)
            for key in ("RPGMIGRATE_TEST_CONTEXT", "RPGMIGRATE_TEST_AUTHORITY_TARGET",
                        "RPGMIGRATE_TEST_AUTHORITY_ADMIN", "RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER",
                        "RPGMIGRATE_TEST_REPLICATION_SQL", "RPGMIGRATE_TEST_REPLICATION_SOURCE",
                        "RPGMIGRATE_TEST_REPLICATION_ADMIN", "RPGMIGRATE_TEST_REPLICATION_CONTEXT"):
                self.assertIn(key, options["environment"])
            observed.append("ran")
            return subprocess.CompletedProcess(actual_command, returncode, output, "")

        def validate(actual):
            self.assertIs(actual, context)
            observed.append("receipts")

        replacements = {"OwnedCluster": owned, "authority_fixture": authority, "replication_fixture": replication,
                        "source_snapshot_fixture": self.forbidden, "prepare_journal_fixture": prepare,
                        "validate_journal_receipts": validate, "validate_authority_faults": self.forbidden,
                        "validate_replication_faults": self.forbidden,
                        "databases": lambda *_args, **_options: ("ordinary_source", "ordinary_target"),
                        "run_command": command}
        with patch.dict(main.__globals__, replacements), \
                patch.object(sys, "argv", [str(WRAPPER), "--journal-serial", "--", *COMMAND]), \
                redirect_stdout(io.StringIO()) as stdout:
            status = main()
        return status, observed, stdout.getvalue()

    def test_wrapper_uses_engine_endpoints_and_requires_journal_receipts_only(self):
        status, observed, output = self.run_journal()
        self.assertEqual(status, 0)
        self.assertEqual(observed, ["authority", "replication", "prepared", "ran", "receipts"])
        self.assertIn("JournalSerial children verified 2 pre-COMMIT SIGKILL receipts", output)
        self.assertIn("JournalSerial faults verified 2 committed Unknown replies and released fences", output)
        self.assertIn("JournalSerial verified 2 exact cases; full E1 suite not selected", output)

    def test_wrapper_child_failure_setup_failure_and_empty_output_cannot_pass(self):
        status, observed, output = self.run_journal(returncode=9)
        self.assertEqual(status, 9)
        self.assertNotIn("receipts", observed)
        self.assertNotIn("JournalSerial verified", output)
        with self.assertRaisesRegex(RuntimeError, "journal setup failed"):
            self.run_journal(setup_error=True)
        with self.assertRaises(AssertionError):
            self.run_journal(output="")

    def test_fixture_grants_are_scoped_to_the_owned_source_and_tables_match(self):
        from journal_fixture import prepare_journal_fixture
        target = {"root": "/owned/target", "token": "target"}
        source = {"root": "/owned/source", "token": "source", "database": "replication_source",
                  "replication_role": "replication_stream"}
        calls = []

        def psql(context, database, sql, *, role="e1_admin"):
            calls.append((context, database, sql, role))
            if "shobj_description" in sql:
                return context["token"]
            if "json_build_array(rolreplication" in sql:
                return json.dumps([True, False, False, False, False, False, False, True])
            if "json_build_array(c.system_identifier" in sql:
                # PostgreSQL emits raw OIDs as JSON strings and bigint casts as numbers.
                return json.dumps(["123", 42 if "d.oid::bigint)" in sql else "42"])
            return "0" if "SELECT count(*)" in sql else ""

        with patch("journal_fixture.assert_identity", lambda _context: None), patch("journal_fixture.psql", psql):
            prepare_journal_fixture(target, source)
        self.assertEqual(target["journal_source_identity"], {"system_identifier": 123, "database_oid": 42})
        for candidate in ("segmented", "sqlite"):
            ddl = f'CREATE TABLE public."journal_serial_{candidate}" (id integer PRIMARY KEY, payload text);'
            self.assertIn((target, "e3_authority", ddl, "e1_target"), calls)
            self.assertIn((source, "replication_source", ddl + f' GRANT SELECT ON public."journal_serial_{candidate}" TO "replication_stream";', "e1_admin"), calls)
        grants = [(context, sql) for context, _database, sql, _role in calls if "GRANT EXECUTE" in sql]
        self.assertEqual(len(grants), 1)
        self.assertIs(grants[0][0], source)
        self.assertIn('TO "replication_stream"', grants[0][1])
        self.assertFalse(any("ALTER ROLE" in sql for _context, _database, sql, _role in calls))
        calls.clear()
        with patch("journal_fixture.assert_identity", lambda _context: None), \
                patch("journal_fixture.psql", side_effect=lambda *_args, **_options: "foreign"), \
                self.assertRaisesRegex(AssertionError, "outside an owned"):
            prepare_journal_fixture(target, source)

    def test_fixture_identity_keeps_integer_oid_bounds_and_system_checks(self):
        from journal_fixture import prepare_journal_fixture

        def prepare(system, oid):
            target = {"token": "target"}
            source = {"token": "source", "database": "replication_source", "replication_role": "replication_stream"}
            calls = []

            def psql(context, _database, sql, **_options):
                calls.append(sql)
                if "shobj_description" in sql:
                    return context["token"]
                if "json_build_array(rolreplication" in sql:
                    return json.dumps([True, False, False, False, False, False, False, True])
                if "json_build_array(c.system_identifier" in sql:
                    return json.dumps([system, oid])
                return "0" if "SELECT count(*)" in sql else ""

            with patch("journal_fixture.assert_identity", lambda _context: None), patch("journal_fixture.psql", psql):
                try:
                    prepare_journal_fixture(target, source)
                except (AssertionError, ValueError):
                    self.assertNotIn("journal_source_identity", target)
                    self.assertFalse(any("CREATE TABLE" in sql for sql in calls))
                    raise
            return target["journal_source_identity"]

        for system, oid, expected_system in (("123", 1, 123), ("-1", 2**32 - 1, 2**64 - 1)):
            with self.subTest(system=system, oid=oid):
                self.assertEqual(prepare(system, oid), {"system_identifier": expected_system, "database_oid": oid})
        for oid in ("42", "invalid", 0, -1, 2**32, True, False, None, 42.0, [], {}):
            with self.subTest(oid=oid), self.assertRaisesRegex(AssertionError, "missing journal source identity"):
                prepare("123", oid)
        with self.assertRaisesRegex(AssertionError, "missing journal source identity"):
            prepare("0", 42)
        with self.assertRaises(ValueError):
            prepare("invalid", 42)

    def test_helper_choices_keep_authority_two_and_exclude_unapproved_abort(self):
        import authority_fault
        main = authority_fault.main
        for case in ("journal_segmented", "journal_sqlite"):
            with patch.object(sys, "argv", ["authority_fault.py", "--case", case]), \
                    patch.dict(main.__globals__, {"assert_identity": self.forbidden}), \
                    patch("authority_fault.Path.read_text", return_value="{}"), \
                    patch.dict("os.environ", {"RPGMIGRATE_TEST_CONTEXT": "/owned/context.json"}), \
                    self.assertRaisesRegex(AssertionError, "unexpected fixture start"):
                main()
        with patch.object(sys, "argv", ["authority_fault.py", "--case", "journal_segmented_abort"]), \
                redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
            main()
        self.assertEqual(error.exception.code, 2)

    def test_duplicate_json_fields_and_changed_native_bindings_fail(self):
        from journal_fixture import validate_journal_receipts
        with tempfile.TemporaryDirectory() as directory:
            context = self.context(directory)
            records(context)
            path = Path(directory) / "journal-segmented-crash-receipt.json"
            original = path.read_text()
            path.write_text(original.replace('"reaped": true', '"reaped": false, "reaped": true'))
            with self.assertRaisesRegex(AssertionError, "duplicate"):
                validate_journal_receipts(context)
            path.write_text(original)
            native_path = Path(directory) / "journal-segmented-native/manifest.json"
            native = json.loads(native_path.read_text())
            for field, bad in (("catalog_epoch", 9), ("plugin", "pgoutput"), ("slot_name", "foreign")):
                changed = copy.deepcopy(native)
                changed["source_slot"]["record"]["intent"]["binding"][field] = bad
                native_path.write_text(json.dumps(changed))
                with self.subTest(field=field), self.assertRaises(AssertionError):
                    validate_journal_receipts(context)
            native_path.write_text(json.dumps(native))


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(JournalHarnessTests)
    if len(sys.argv) > 1:
        suite = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:], module=sys.modules[__name__])
    if not suite.countTestCases():
        raise RuntimeError("no journal harness tests selected")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() and not result.skipped else 1)
