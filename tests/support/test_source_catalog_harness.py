"""Run with python3 -W error tests/support/test_source_catalog_harness.py."""

from contextlib import redirect_stderr, redirect_stdout
import io
from pathlib import Path
import runpy
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from db_harness import load_manifest, module_cases

SUPPORT = Path(__file__).resolve().parent
WRAPPER = SUPPORT.parents[1] / "tools" / "test-db.py"
CASES = [
    "catalog::tests::live_source_identity_exact_backend",
    "catalog::tests::live_source_identity_preserves_imported_snapshot",
    "catalog::tests::live_source_identity_refuses_missing_control_privilege",
]
COMMAND = ["cargo", "test", "--locked", "--lib", "catalog::tests::live_source_identity_",
           "--", "--ignored", "--test-threads=1"]
RECORDS = "".join(f"test {name} ... ok\n" for name in CASES)
OUTPUT = RECORDS + "test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 0.01s\n"


class SourceCatalogHarnessTests(unittest.TestCase):
    def test_source_catalog_inventory_is_exact_and_separate(self):
        manifest = load_manifest()
        self.assertEqual(module_cases(manifest, "SourceCatalog"), CASES)
        for profile, count in (("Catalog", 12), ("Authority", 18), ("Engine", 45), ("Replication", 9)):
            with self.subTest(profile=profile):
                names = module_cases(manifest, profile)
                self.assertEqual(len(names), count)
                self.assertTrue(set(CASES).isdisjoint(names))
        self.assertEqual(len(manifest["cases"]), 40)

    def test_source_catalog_receipt_rejects_missing_extra_duplicate_failed_and_ignored_cases(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        validate("SourceCatalog", CASES, OUTPUT, {})
        for output in ("", *(OUTPUT.replace(f"test {name} ... ok\n", "") for name in CASES),
                       OUTPUT + f"test {CASES[0]} ... ok\n", OUTPUT + "test unexpected_case ... ok\n",
                       OUTPUT.replace(CASES[0], "catalog::tests::live_cdc_relation_metadata"),
                       OUTPUT.replace(" ... ok", " ... FAILED", 1),
                       OUTPUT.replace(" ... ok", " ... ignored", 1)):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate("SourceCatalog", CASES, output, {})
        for expected in ([], [CASES[0]] * 3):
            with self.subTest(expected=expected), self.assertRaises(AssertionError):
                validate("SourceCatalog", expected, OUTPUT, {})

    def test_source_catalog_requires_one_consistent_zero_ignored_summary(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        for output in (RECORDS, OUTPUT + OUTPUT[len(RECORDS):],
                       OUTPUT.replace("3 passed", "0 passed"), OUTPUT.replace("0 ignored", "1 ignored"),
                       OUTPUT.replace("0 failed", "1 failed"), OUTPUT.replace("0 measured", "1 measured"),
                       OUTPUT.replace("test result: ok", "test result: FAILED")):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate("SourceCatalog", CASES, output, {})

    @staticmethod
    def forbid_fixture(*_args, **_kwargs):
        raise AssertionError("unexpected fixture start")

    def test_source_catalog_rejects_missing_command_or_mixed_profiles_before_fixture_start(self):
        main = runpy.run_path(str(WRAPPER))["main"]
        for args in (["--source-catalog"], ["--source-catalog", "--"],
                     *(["--source-catalog", mode, "--", *COMMAND] for mode in
                       ("--catalog", "--authority", "--engine", "--transport", "--prepared", "--replication", "--self-check"))):
            with self.subTest(args=args), patch.object(sys, "argv", [str(WRAPPER), *args]), \
                    patch.dict(main.__globals__, {"OwnedCluster": self.forbid_fixture}), \
                    redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main()
            self.assertEqual(error.exception.code, 2)

    def run_source_catalog(self, output=OUTPUT, returncode=0, setup_failure=False):
        main = runpy.run_path(str(WRAPPER))["main"]
        context = {"root": "/owned/source-catalog", "socket_dir": "/owned/socket", "port": 24567}
        cluster = SimpleNamespace(context=context, context_path=Path("/owned/source-catalog/context.json"),
                                  root=Path(context["root"]), bindir=Path("/owned/pg/bin"),
                                  environment={"PATH": "/usr/bin"}, auth_password="synthetic")
        observed = {}

        def databases(actual_context, case_id, *, empty=False):
            self.assertIs(actual_context, context)
            self.assertEqual((case_id, empty), ("source_catalog", True))
            return "e1_s_source_catalog", "e1_t_source_catalog"

        def psql(actual_context, database, sql):
            self.assertIs(actual_context, context)
            observed["setup"] = (database, sql)
            if setup_failure:
                raise RuntimeError("source fixture privilege setup failed")

        def run_command(command, **kwargs):
            self.assertIn("setup", observed, "test command started before privilege setup")
            observed.update(command=command, **kwargs)
            return subprocess.CompletedProcess(command, returncode, output, "")

        class ClusterContext:
            def __enter__(self):
                return cluster

            def __exit__(self, *_args):
                return False

        replacements = {"OwnedCluster": lambda _manifest: ClusterContext(), "databases": databases,
                        "psql": psql, "run_command": run_command,
                        "authority_fixture": self.forbid_fixture, "replication_fixture": self.forbid_fixture}
        with patch.dict(main.__globals__, replacements), \
                patch.object(sys, "argv", [str(WRAPPER), "--source-catalog", "--", *COMMAND]), \
                redirect_stdout(io.StringIO()) as stdout:
            status = main()
        return status, observed, stdout.getvalue()

    def test_source_catalog_uses_owned_role_grants_and_same_database_connections(self):
        status, observed, output = self.run_source_catalog()
        self.assertEqual(status, 0)
        self.assertEqual(observed["setup"], ("e1_s_source_catalog",
                         "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC, e1_auth; "
                         "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO e1_source;"))
        self.assertEqual(observed["command"], COMMAND)
        self.assertTrue(observed["capture_output"])
        self.assertEqual(observed["timeout"], 600)
        environment = observed["environment"]
        self.assertEqual(environment["RPGMIGRATE_TEST_CONTEXT"], "/owned/source-catalog/context.json")
        for key, role in (("RPGMIGRATE_TEST_SOURCE_CATALOG_SOURCE", "e1_source"),
                          ("RPGMIGRATE_TEST_SOURCE_CATALOG_ADMIN", "e1_admin")):
            self.assertEqual(environment[key], "host='/owned/socket' port='24567' dbname='e1_s_source_catalog' "
                             f"user='{role}' connect_timeout='3' sslmode='disable' "
                             "application_name='rpgmigrate_e1_fixture' passfile='/owned/source-catalog/empty.pgpass'")
        self.assertIn("SourceCatalog verified 3 exact cases; full E1 suite not selected", output)

    def test_source_catalog_propagates_child_failure(self):
        status, _, output = self.run_source_catalog(returncode=9)
        self.assertEqual(status, 9)
        self.assertNotIn("SourceCatalog verified", output)

    def test_source_catalog_rejects_zero_or_unrelated_test_success(self):
        for output in ("test result: ok. 0 passed; 0 failed; 0 ignored;\n",
                       "test unrelated::case ... ok\n"):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                self.run_source_catalog(output=output)

    def test_source_catalog_privilege_setup_failure_cannot_pass(self):
        with self.assertRaisesRegex(RuntimeError, "source fixture privilege setup failed"):
            self.run_source_catalog(setup_failure=True)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(SourceCatalogHarnessTests)
    if len(sys.argv) > 1:
        suite = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:], module=sys.modules[__name__])
    if not suite.countTestCases():
        raise RuntimeError("no source catalog harness tests selected")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() and not result.skipped else 1)
