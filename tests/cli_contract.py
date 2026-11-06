"""Check CLI behavior without database services or inherited migration settings."""

import os
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
BINARY = Path(os.environ.get("RPGMIGRATE_BIN", ROOT / "target/debug/rpgmigrate"))


class CliContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.baseline = json.loads((ROOT / "tests/fixtures/cli/baseline.json").read_text())

    def invoke(self, *args, env=None):
        controlled = {"PATH": os.environ.get("PATH", ""), "LC_ALL": "C"}
        controlled.update(env or {})
        return subprocess.run(
            [str(BINARY), *args],
            env=controlled,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

    def test_help_and_version_use_native_identity(self):
        for args in [("--help",), ("help",), ("--version",), ("version",)]:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.returncode, 0, result.stderr)
                output, other = (result.stderr, result.stdout) if args == ("--help",) else (result.stdout, result.stderr)
                self.assertIn("rpgmigrate", output)
                self.assertNotIn("pgcopydb", output.splitlines()[0])
                self.assertEqual(other, "")

    def test_version_renders_version_instead_of_help(self):
        for args in [("--version",), ("-V",), ("version",), ("ping", "-V")]:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertRegex(result.stdout, r"^rpgmigrate \d+\.\d+\.\d+\nlibpq \d+\n$")
                self.assertNotIn("Usage:", result.stdout)

    def test_unknown_command_and_option_are_argument_errors(self):
        for args in [("unknown-command",), ("ping", "--unknown-option")]:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(result.stdout, "")

    def test_malformed_arguments_are_not_help_masked(self):
        for args in [
            ("ping", "--source"),
            ("copy", "table-data", "--table-jobs", "bad"),
            ("copy", "table-data", "--table-jobs", "0"),
            ("copy", "table-data", "--resume", "--restart"),
        ]:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertNotIn("Usage:", result.stdout)

    def test_explicit_connections_are_required(self):
        for args in [("ping",), ("copy", "table-data")]:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("source", result.stderr)

    def test_unimplemented_command_does_not_create_state(self):
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-") as directory:
            state = Path(directory) / "state"
            result = self.invoke(
                "clone", "--source", "host=invalid", "--target", "host=invalid",
                "--dir", str(state),
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("unimplemented", result.stderr.lower())
            self.assertFalse(state.exists())

    def test_behavioral_copy_option_is_rejected_before_state_creation(self):
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-") as directory:
            state = Path(directory) / "state"
            result = self.invoke(
                "copy", "table-data", "--source", "host=invalid",
                "--target", "host=invalid", "--dir", str(state), "--follow",
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("follow", result.stderr)
            self.assertFalse(state.exists())

    def test_invalid_environment_is_checked_even_with_cli_override(self):
        result = self.invoke(
            "clone", "--table-jobs", "1",
            env={"PGCOPYDB_TABLE_JOBS": "invalid"},
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("PGCOPYDB_TABLE_JOBS", result.stderr)

    def test_cli_connection_overrides_malformed_environment_connection(self):
        result = self.invoke(
            "clone", "--source", "host=invalid", "--target", "host=invalid",
            env={"PGCOPYDB_SOURCE_PGURI": "not conninfo", "PGCOPYDB_TARGET_PGURI": ""},
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("unimplemented", result.stderr.lower())

    def test_diagnostics_never_echo_connection_credentials(self):
        secret = "cli-contract-private-password"
        for args in [
            ("ping", "--source", f"password={secret}", "--not-an-option"),
            ("clone", "--source", f"password={secret}", "--target", "host=invalid"),
        ]:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn(secret, result.stdout + result.stderr)

    def test_relaxed_requires_both_explicit_loss_caps(self):
        for extra in [[], ["--loss-max-transactions", "10"], ["--loss-max-bytes", "1024"]]:
            with self.subTest(extra=extra):
                result = self.invoke("clone", "--durability", "relaxed", *extra)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("loss-max-", result.stderr)

    def test_hardening_deltas_reject_malformed_values(self):
        # The baseline can ignore these errors, overflow, or select a fallback.
        cases = [
            ("clone", "--plugin", "unknown"),
            ("clone", "--split-at", "1 XB"),
            ("clone", "--split-at", "18446744073709551615 EB"),
            ("clone", "--split-max-parts", "2147483648"),
            ("clone", "--port", "65536"),
            ("version", "--unknown-option"),
            ("clone", "--endpos", "1/2tail"),
            ("clone", "--resume", "--restart"),
            ("dump", "schema", "-F"),
            ("dump", "schema", "-N"),
        ]
        for args in cases:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.returncode, 1, result.stderr)

    def test_boolean_environment_is_validated_before_cli_override(self):
        result = self.invoke(
            "clone", "--use-copy-binary",
            env={"PGCOPYDB_USE_COPY_BINARY": "neither"},
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("PGCOPYDB_USE_COPY_BINARY", result.stderr)

    def test_command_local_short_options_keep_their_meanings(self):
        environment = {
            "PGCOPYDB_SOURCE_PGURI": "host=invalid",
            "PGCOPYDB_TARGET_PGURI": "host=invalid",
        }
        cases = [
            ("clone", "-J", "1"),
            ("compare", "data", "-j", "1", "-J"),
            ("list", "tables", "-J"),
            ("clone", "-I", "1"),
            ("stream", "apply", "-I", "-T", "-"),
            ("list", "tables", "-I"),
            ("restore", "schema", "-E"),
            ("clone", "-E", "1/2"),
            ("stream", "sentinel", "get", "-e"),
            ("list", "tables", "-k", "ignored"),
            ("list", "tables", "--skip-split-by-ctid"),
            ("list", "progress", "--summary"),
            ("stream", "apply", "--target", "-", "-"),
        ]
        for args in cases:
            with self.subTest(args=args):
                result = self.invoke(*args, env=environment)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertIn("unimplemented", result.stderr.lower())
        self.assertEqual(self.invoke("list", "tables", "-k").returncode, 1)

    def test_dump_short_flags_use_the_intended_long_option_arity(self):
        # The baseline optstring omits colons, but both handlers consume optarg.
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-") as directory:
            filters = Path(directory) / "filters.ini"
            filters.write_text("[exclude-table]\npublic.ignored\n")
            result = self.invoke(
                "dump", "schema", "-S", "host=invalid", "-F", str(filters),
                "-N", "00000001-00000002-1",
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("unimplemented", result.stderr.lower())

    def test_root_flags_are_not_accepted_between_group_and_leaf(self):
        result = self.invoke("stream", "--verbose", "receive")
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_sentinel_validates_operands_and_repeated_selectors(self):
        for args in [
            ("setup",), ("setup", "1/2"), ("setup", "1/2", "invalid"),
            ("get", "--startpos", "--startpos"),
            ("get", "--startpos", "--endpos"), ("get", "extra"),
            ("set", "startpos"), ("set", "endpos", "--current", "1/2"),
            ("set", "apply", "true"),
        ]:
            with self.subTest(args=args):
                result = self.invoke("stream", "sentinel", *args)
                self.assertEqual(result.returncode, 1, result.stderr)

    def test_valid_additive_controls_are_still_explicitly_unimplemented(self):
        result = self.invoke(
            "clone", "--source", "host=invalid", "--target", "host=invalid",
            "--durability", "relaxed", "--apply-jobs", "2",
            "--loss-max-transactions", "10", "--loss-max-bytes", "1 MB",
        )
        self.assertEqual(result.returncode, 2, result.stderr)
        self.assertIn("unimplemented", result.stderr.lower())

    def test_pinned_fixture_has_a_nonempty_complete_inventory(self):
        self.assertEqual(self.baseline["baseline"]["commit"], "86bf908adca02d87063849dae775a02c1659c829")
        self.assertEqual(self.baseline["inventory"]["leaves"], 50)
        self.assertEqual(self.baseline["inventory"]["long_option_names"], 77)
        self.assertEqual(len(self.baseline["parsers"]), 11)
        self.assertEqual(sum(bool(command["handler"]) for command in self.baseline["commands"]), 50)

    def test_every_registered_leaf_has_help_and_rejects_unknown_options(self):
        for command in self.baseline["commands"]:
            if not command["handler"]:
                continue
            path = command["path"]
            with self.subTest(path=path):
                help_args = path if path == ["help"] else [*path, "--help"]
                result = self.invoke(*help_args)
                self.assertEqual(result.returncode, 0, result.stderr)
                output, other = (result.stdout, result.stderr) if path == ["help"] else (result.stderr, result.stdout)
                self.assertIn("Usage: rpgmigrate", output)
                self.assertEqual(other, "")
                result = self.invoke(*path, "--not-a-registered-option")
                self.assertEqual(result.returncode, 1, result.stderr)

    def test_missing_commands_fail_for_root_and_every_registered_group(self):
        groups = [command["path"] for command in self.baseline["commands"] if not command["handler"]]
        self.assertEqual(len(groups), 9)
        for path in groups:
            with self.subTest(path=path):
                result = self.invoke(*path)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertIn("command", result.stderr)

    def test_help_flags_use_stderr_for_root_and_all_registered_groups(self):
        groups = [command["path"] for command in self.baseline["commands"] if not command["handler"] and command["path"]]
        self.assertEqual(len(groups), 8)
        for path in [[], *groups]:
            for flag in ["--help", "-h"]:
                with self.subTest(path=path, flag=flag):
                    result = self.invoke(*path, flag)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(result.stdout, "")
                    self.assertIn("Usage: " + " ".join(["rpgmigrate", *path]) + " ", result.stderr)

    def test_every_long_option_is_parsed_without_help_masking(self):
        self.check_inventory_options(short=False)

    def test_every_short_binding_is_parsed_without_help_masking(self):
        self.check_inventory_options(short=True)

    def check_inventory_options(self, short):
        representatives = {
            "root_options": [],
            "cli_print_version_getopts": ["version"],
            "cli_copy_db_getopts": ["clone"],
            "cli_compare_getopts": ["compare", "data"],
            "cli_dump_schema_getopts": ["dump", "schema"],
            "cli_restore_schema_getopts": ["restore", "schema"],
            "cli_list_db_getopts": ["list", "tables"],
            "cli_stream_getopts": ["stream", "replay"],
            "cli_sentinel_getopts": ["stream", "sentinel", "get"],
            "cli_create_snapshot_getopts": ["snapshot"],
            "cli_ping_getopts": ["ping"],
        }
        environment = {"PGCOPYDB_SOURCE_PGURI": "host=invalid", "PGCOPYDB_TARGET_PGURI": "host=invalid"}
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-") as directory:
            fixture = Path(directory) / "input"
            fixture.write_text("[exclude-table]\npublic.ignored\n")
            values = {
                "source": "host=invalid", "target": "host=invalid", "dir": directory,
                "jobs": "1", "table-jobs": "1", "index-jobs": "1", "restore-jobs": "1",
                "large-objects-jobs": "1", "split-max-parts": "1", "plugin": "wal2json",
                "split-at": "1 MB", "split-tables-larger-than": "1 MB", "max-replaydb-size": "1 MB",
                "port": "54321", "endpos": "1/2", "filter": str(fixture), "filters": str(fixture),
                "requirements": str(fixture),
            }
            count = 0
            for parser, definition in self.baseline["parsers"].items():
                options = definition["long_options"]
                if short:
                    options = [
                        {**next(option for option in options if option["action"] == letter),
                         "short": letter,
                         "arity": 1 if parser == "cli_dump_schema_getopts" and letter in {"F", "N"} else arity}
                        for letter, arity in definition["short_options"].items()
                    ]
                for option in options:
                    name = option["name"]
                    path = representatives[parser]
                    spelling = "-" + option["short"] if short else "--" + name
                    with self.subTest(parser=parser, option=spelling):
                        args = [*path, spelling]
                        if option["arity"] == 1:
                            self.assertEqual(self.invoke(*args, env=environment).returncode, 1)
                            args.append(values.get(name, "example"))
                        if name == "wal2json-numeric-as-string":
                            args += ["--plugin", "wal2json"]
                        if parser == "root_options" and name not in {"help", "version"}:
                            args += ["clone"]
                        if parser == "cli_ping_getopts" and name not in {"help", "version"}:
                            args += ["unsupported-operand"]
                        result = self.invoke(*args, env=environment)
                        expected = 0 if name in {"help", "version"} else 2
                        self.assertEqual(result.returncode, expected, result.stderr)
                        if expected == 2:
                            self.assertIn("unimplemented", result.stderr.lower())
                        count += 1
            self.assertEqual(count, 188 if short else 220)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CliContract)
    if suite.countTestCases() == 0:
        raise SystemExit("No CLI contract tests were discovered")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
