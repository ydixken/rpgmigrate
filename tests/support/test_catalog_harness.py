"""Run with python3 tests/support/test_catalog_harness.py, without PostgreSQL."""

from contextlib import contextmanager, redirect_stderr, redirect_stdout
import io
import json
import os
import tempfile
from pathlib import Path
import runpy
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from db_harness import load_manifest, module_cases
import money_locale_fixture as locales

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
    "catalog::tests::live_clone_admission_selects_each_heap_once_and_omits_generated_input",
    "catalog::tests::live_clone_admission_refuses_unsupported_relations_and_trigger_states",
    "catalog::tests::live_clone_admission_requires_column_type_extensions_on_the_target",
    "catalog::tests::live_money_clone_preserves_values_and_defaults_across_role_locales",
]
COMMAND = ["cargo", "test", "--locked", "--lib", "catalog::tests::live_",
           "--", "--ignored", "--test-threads=1", "--skip", "catalog::tests::live_source_identity_",
           "--skip", "catalog::tests::live_preflight_"]
OUTPUT = ("".join(f"test {name} ... ok\n" for name in CASES)
          + "test result: ok. 14 passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 0.01s\n")


class CatalogHarnessTests(unittest.TestCase):
    def test_catalog_profile_requires_the_fourteen_independent_names(self):
        expected = module_cases(load_manifest(), "Catalog")
        self.assertEqual(expected, CASES)
        self.assertEqual(json.loads((SUPPORT / "catalog_cases.json").read_text()), CASES)

    def test_catalog_cases_do_not_enter_engine_or_copy_inventory(self):
        manifest = load_manifest()
        engine = module_cases(manifest, "Engine")
        self.assertEqual(len(engine), 50)
        self.assertEqual(len(manifest["cases"]), 40)
        self.assertTrue(set(CASES).isdisjoint(engine))
        self.assertEqual(len(module_cases(manifest, "Authority")), 19)
        self.assertEqual(len(module_cases(manifest, "Replication")), 9)

    def test_catalog_receipt_accepts_exact_fourteen_without_other_fixture_receipts(self):
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
        for inventory in ([], [CASES[0]] * len(CASES)):
            with self.subTest(inventory=inventory), self.assertRaises(AssertionError):
                validate("Catalog", inventory, OUTPUT, {})

    def test_catalog_receipt_rejects_source_only_and_target_only_success(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        expected = module_cases(load_manifest(), "Catalog")
        for names, count in ((CASES[:6], 6), (CASES[6:], 6)):
            output = ("".join(f"test {name} ... ok\n" for name in names)
                      + f"test result: ok. {count} passed; 0 failed; 0 ignored; 0 measured; 24 filtered out; finished in 0.01s\n")
            with self.subTest(count=count), self.assertRaises(AssertionError):
                validate("Catalog", expected, output, {})

    def test_catalog_receipt_requires_one_consistent_zero_ignored_summary(self):
        validate = runpy.run_path(str(WRAPPER))["validate_module_receipt"]
        expected = module_cases(load_manifest(), "Catalog")
        records, _, summary = OUTPUT.rpartition("test result:")
        for output in (records, OUTPUT + "test result:" + summary,
                       OUTPUT.replace("14 passed", "0 passed"),
                       OUTPUT.replace("14 passed", "6 passed"),
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
                       ("--engine", "--transport", "--prepared", "--authority", "--replication", "--schema", "--clone", "--self-check"))):
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
            observed.setdefault("databases", []).append((case_id, empty))
            return "e1_s_" + case_id, "e1_t_" + case_id

        def run_command(command, **kwargs):
            observed.update(command=command, **kwargs)
            return subprocess.CompletedProcess(command, returncode, output, "")

        @contextmanager
        def locale_context(enabled):
            self.assertTrue(enabled)
            observed["locales_before_server"] = True
            yield Path("/owned/locales")

        class ClusterContext:
            def __enter__(self):
                assert observed.get("locales_before_server") is True
                return cluster

            def __exit__(self, *_args):
                observed["cluster_closed"] = True
                return False

        replacements = {"OwnedCluster": lambda _manifest: ClusterContext(), "databases": databases,
                        "run_command": run_command, "authority_fixture": self.forbid_database,
                        "replication_fixture": self.forbid_database, "money_locales": locale_context}
        with patch.dict(main.__globals__, replacements), \
                patch.object(sys, "argv", [str(WRAPPER), "--catalog", "--", *COMMAND]), \
                redirect_stdout(io.StringIO()) as stdout:
            status = main()
        return status, observed, stdout.getvalue()

    def test_catalog_runner_isolates_empty_clone_pair_from_committed_cdc_fixtures(self):
        status, observed, output = self.run_catalog()
        self.assertEqual(status, 0)
        self.assertTrue(observed["locales_before_server"])
        self.assertTrue(observed["cluster_closed"])
        self.assertEqual(observed["databases"], [("catalog", True), ("catalog_clone", True)])
        self.assertEqual(observed["command"], COMMAND)
        self.assertTrue(observed["capture_output"])
        self.assertEqual(observed["timeout"], 600)
        environment = observed["environment"]
        self.assertEqual(environment["RPGMIGRATE_TEST_CONTEXT"], "/owned/catalog/context.json")
        for key, role, database in (("RPGMIGRATE_TEST_CATALOG_SOURCE", "e1_source", "e1_s_catalog"),
                                    ("RPGMIGRATE_TEST_CATALOG_ADMIN", "e1_admin", "e1_s_catalog"),
                                    ("RPGMIGRATE_TEST_CATALOG_TARGET", "e1_target", "e1_t_catalog"),
                                    ("RPGMIGRATE_TEST_CATALOG_TARGET_ADMIN", "e1_admin", "e1_t_catalog"),
                                    ("RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN", "e1_admin", "e1_s_catalog_clone"),
                                    ("RPGMIGRATE_TEST_CLONE_TARGET_ADMIN", "e1_admin", "e1_t_catalog_clone")):
            self.assertIn(key, environment)
            self.assertEqual(environment[key], f"host='/owned/socket' port='24567' dbname='{database}' "
                             f"user='{role}' connect_timeout='3' sslmode='disable' "
                             "application_name='rpgmigrate_e1_fixture' passfile='/owned/catalog/empty.pgpass'")
        self.assertIn("Catalog verified 14 exact cases; full E1 suite not selected", output)

    def test_money_locales_disabled_preserves_environment_without_compilation(self):
        with patch.dict(os.environ, {"LOCPATH": "inherited"}), patch.object(locales.shutil, "which", side_effect=AssertionError("unexpected compiler")):
            with locales.money_locales(False) as root:
                self.assertIsNone(root)
                self.assertEqual(os.environ["LOCPATH"], "inherited")

    def test_money_locales_require_installed_compiler_and_source_data(self):
        with patch.object(locales.shutil, "which", return_value=None), self.assertRaisesRegex(RuntimeError, "requires installed GNU localedef"):
            with locales.money_locales(True):
                self.fail("missing compiler yielded a locale fixture")
        with tempfile.TemporaryDirectory() as temporary, patch.object(locales, "I18N_ROOT", Path(temporary)), \
                patch.object(locales.shutil, "which", return_value="/installed/localedef"), self.assertRaisesRegex(RuntimeError, "locale sources"):
            with locales.money_locales(True):
                self.fail("missing locale data yielded a fixture")

    def locale_sources(self, root):
        for name in ("locales/de_DE", "locales/en_US", "charmaps/UTF-8.gz"):
            path = root / name
            path.parent.mkdir(exist_ok=True, parents=True)
            path.touch()

    def test_money_locales_are_private_inherited_before_server_and_restored(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            self.locale_sources(source)
            calls = []
            def compile_locale(argv, **kwargs):
                calls.append(argv)
                output = Path(argv[-1])
                output.mkdir()
                (output / "LC_MONETARY").touch()
                return subprocess.CompletedProcess(argv, 0)
            with patch.object(locales, "I18N_ROOT", source), patch.object(locales.shutil, "which", return_value="/installed/localedef"), \
                    patch.object(locales.subprocess, "run", compile_locale), patch.dict(os.environ, {"LOCPATH": "inherited"}):
                with locales.money_locales(True) as root:
                    self.assertEqual(os.environ["LOCPATH"], str(root))
                    self.assertEqual(root.stat().st_mode & 0o777, 0o700)
                    self.assertEqual([argv[1:-1] for argv in calls], [["--no-archive", "-i", name, "-f", "UTF-8"] for name in ("de_DE", "en_US")])
                    self.assertTrue(all((root / (name + ".UTF-8/LC_MONETARY")).is_file() for name in ("de_DE", "en_US")))
                self.assertEqual(os.environ["LOCPATH"], "inherited")
                self.assertFalse(root.exists())
                with self.assertRaisesRegex(RuntimeError, "stop owned fixture"):
                    with locales.money_locales(True):
                        raise RuntimeError("stop owned fixture")
                self.assertEqual(os.environ["LOCPATH"], "inherited")

    def test_money_locales_fail_closed_on_compile_error_or_absent_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            self.locale_sources(source)
            for status in (0, 1):
                with self.subTest(status=status), patch.object(locales, "I18N_ROOT", source), \
                        patch.object(locales.shutil, "which", return_value="/installed/localedef"), \
                        patch.object(locales.subprocess, "run", return_value=subprocess.CompletedProcess([], status)), \
                        patch.dict(os.environ, {"LOCPATH": "inherited"}), self.assertRaisesRegex(RuntimeError, "could not compile"):
                    with locales.money_locales(True):
                        self.fail("incomplete locale compilation yielded a fixture")
                    self.assertEqual(os.environ["LOCPATH"], "inherited")

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
