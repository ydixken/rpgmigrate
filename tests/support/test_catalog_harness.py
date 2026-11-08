"""Run with python3 tests/support/test_catalog_harness.py, without PostgreSQL."""

from contextlib import redirect_stderr, redirect_stdout
import io
import json
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
    "catalog::tests::live_cdc_relation_metadata",
    "catalog::tests::live_cdc_identity_requires_default_primary_key",
    "catalog::tests::live_cdc_rejects_unsupported_columns",
    "catalog::tests::live_cdc_rejects_unsupported_relations",
    "catalog::tests::live_cdc_rejects_unsupported_indexes",
    "catalog::tests::live_cdc_retains_caller_snapshot",
    "catalog::tests::live_cdc_target_matches_and_retains_lock",
    "catalog::tests::live_cdc_target_rejects_metadata_mismatch",
    "catalog::tests::live_cdc_target_rejects_execution_profile",
    "catalog::tests::live_cdc_target_requires_owned_transaction_settings",
]
COMMAND = ["cargo", "test", "--locked", "--lib", "catalog::tests::live_cdc_",
           "--", "--ignored", "--test-threads=1"]
OUTPUT = ("".join(f"test {name} ... ok\n" for name in CASES)
          + "test result: ok. 10 passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 0.01s\n")


class CatalogHarnessTests(unittest.TestCase):
    def test_catalog_profile_requires_the_ten_independent_names(self):
        expected = module_cases(load_manifest(), "Catalog")
        self.assertEqual(expected, CASES)
        self.assertEqual(json.loads((SUPPORT / "catalog_cases.json").read_text()), CASES)

    def test_catalog_cases_do_not_enter_engine_or_copy_inventory(self):
        manifest = load_manifest()
        engine = module_cases(manifest, "Engine")
        self.assertEqual(len(engine), 41)
        self.assertEqual(len(manifest["cases"]), 40)
        self.assertTrue(set(CASES).isdisjoint(engine))
        self.assertEqual(len(module_cases(manifest, "Authority")), 18)
        self.assertEqual(len(module_cases(manifest, "Replication")), 9)

    def test_catalog_receipt_accepts_exact_ten_without_other_fixture_receipts(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        validate("Catalog", module_cases(load_manifest(), "Catalog"), OUTPUT, {})

    def test_catalog_receipt_rejects_missing_extra_duplicate_ignored_failed_and_wrong_cases(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        expected = module_cases(load_manifest(), "Catalog")
        for output in ("", *(OUTPUT.replace(f"test {name} ... ok\n", "") for name in CASES),
                       OUTPUT + "test catalog::tests::live_cdc_extra ... ok\n",
                       OUTPUT + f"test {CASES[0]} ... ok\n",
                       OUTPUT.replace(" ... ok", " ... ignored", 1),
                       OUTPUT.replace(" ... ok", " ... FAILED", 1),
                       OUTPUT.replace(CASES[0], "pg::live_tests::unrelated")):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate("Catalog", expected, output, {})
        for inventory in ([], [CASES[0]] * 10):
            with self.subTest(inventory=inventory), self.assertRaises(AssertionError):
                validate("Catalog", inventory, OUTPUT, {})

    def test_catalog_receipt_rejects_source_only_and_target_only_success(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        expected = module_cases(load_manifest(), "Catalog")
        for names, count in ((CASES[:6], 6), (CASES[6:], 4)):
            output = ("".join(f"test {name} ... ok\n" for name in names)
                      + f"test result: ok. {count} passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 0.01s\n")
            with self.subTest(count=count), self.assertRaises(AssertionError):
                validate("Catalog", expected, output, {})

    def test_catalog_receipt_requires_one_consistent_zero_ignored_summary(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        expected = module_cases(load_manifest(), "Catalog")
        records, _, summary = OUTPUT.rpartition("test result:")
        for output in (records, OUTPUT + "test result:" + summary,
                       OUTPUT.replace("10 passed", "0 passed"),
                       OUTPUT.replace("10 passed", "6 passed"),
                       OUTPUT.replace("0 failed", "1 failed"),
                       OUTPUT.replace("0 ignored", "1 ignored"),
                       OUTPUT.replace("0 measured", "1 measured"),
                       OUTPUT.replace("test result: ok", "test result: FAILED")):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate("Catalog", expected, output, {})

    def test_catalog_profile_rejects_missing_command_and_mixed_profiles_before_database_start(self):
        main = runpy.run_path(str(WRAPPER))["main"]
        for args in (["--catalog"], ["--catalog", "--"],
                     *(["--catalog", mode, "--", *COMMAND] for mode in
                       ("--engine", "--transport", "--prepared", "--authority", "--replication", "--self-check"))):
            with self.subTest(args=args), patch.object(sys, "argv", [str(WRAPPER), *args]), \
                    patch.dict(main.__globals__, {"OwnedCluster": self.forbid_database}), \
                    redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as error:
                main()
            self.assertEqual(error.exception.code, 2)

    @staticmethod
    def forbid_database(*_args, **_kwargs):
        raise AssertionError("unexpected database fixture start")

    def run_catalog(self, output=OUTPUT, returncode=0):
        main = runpy.run_path(str(WRAPPER))["main"]
        context = {"root": "/owned/catalog", "socket_dir": "/owned/socket", "port": 24567}
        cluster = SimpleNamespace(context=context, context_path=Path("/owned/catalog/context.json"),
                                  root=Path(context["root"]), bindir=Path("/owned/pg/bin"),
                                  environment={"PATH": "/usr/bin"}, auth_password="synthetic")
        observed = {}

        def databases(actual_context, case_id, *, empty=False):
            self.assertIs(actual_context, context)
            self.assertEqual((case_id, empty), ("catalog", True))
            return "e1_s_catalog", "e1_t_catalog"

        def run_command(command, **kwargs):
            observed.update(command=command, **kwargs)
            return subprocess.CompletedProcess(command, returncode, output, "")

        class ClusterContext:
            def __enter__(self):
                return cluster

            def __exit__(self, *_args):
                return False

        replacements = {"OwnedCluster": lambda _manifest: ClusterContext(), "databases": databases,
                        "run_command": run_command, "authority_fixture": self.forbid_database,
                        "replication_fixture": self.forbid_database}
        with patch.dict(main.__globals__, replacements), \
                patch.object(sys, "argv", [str(WRAPPER), "--catalog", "--", *COMMAND]), \
                redirect_stdout(io.StringIO()) as stdout:
            status = main()
        return status, observed, stdout.getvalue()

    def test_catalog_runner_uses_empty_owned_databases_and_dedicated_connections(self):
        status, observed, output = self.run_catalog()
        self.assertEqual(status, 0)
        self.assertEqual(observed["command"], COMMAND)
        self.assertTrue(observed["capture_output"])
        self.assertEqual(observed["timeout"], 600)
        environment = observed["environment"]
        self.assertEqual(environment["RPGMIGRATE_TEST_CONTEXT"], "/owned/catalog/context.json")
        for key, role, database in (("RPGMIGRATE_TEST_CATALOG_SOURCE", "e1_source", "e1_s_catalog"),
                                    ("RPGMIGRATE_TEST_CATALOG_ADMIN", "e1_admin", "e1_s_catalog"),
                                    ("RPGMIGRATE_TEST_CATALOG_TARGET", "e1_target", "e1_t_catalog"),
                                    ("RPGMIGRATE_TEST_CATALOG_TARGET_ADMIN", "e1_admin", "e1_t_catalog")):
            self.assertIn(key, environment)
            self.assertEqual(environment[key], f"host='/owned/socket' port='24567' dbname='{database}' "
                             f"user='{role}' connect_timeout='3' sslmode='disable' "
                             "application_name='rpgmigrate_e1_fixture' passfile='/owned/catalog/empty.pgpass'")
        self.assertIn("Catalog verified 10 exact cases; full E1 suite not selected", output)

    def test_catalog_runner_propagates_child_failure(self):
        status, _, output = self.run_catalog(returncode=7)
        self.assertEqual(status, 7)
        self.assertNotIn("Catalog verified", output)

    def test_catalog_runner_rejects_zero_or_unrelated_test_success(self):
        for output in ("test result: ok. 0 passed; 0 failed; 0 ignored;\n",
                       "test pg::live_tests::unrelated ... ok\n"):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                self.run_catalog(output=output)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CatalogHarnessTests)
    if len(sys.argv) > 1:
        suite = unittest.defaultTestLoader.loadTestsFromNames(sys.argv[1:], module=sys.modules[__name__])
    if not suite.countTestCases():
        raise RuntimeError("no catalog harness tests selected")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() and not result.skipped else 1)
