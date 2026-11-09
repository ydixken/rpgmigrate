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
        cls.qualified_version = json.loads(
            (ROOT / "tests/fixtures/native/v1/payloads/version-qualified-profile.json").read_text()
        )
        storage = tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-offline-")
        cls.addClassCleanup(storage.cleanup)
        socket = Path(storage.name) / "absent-socket"
        cls.offline_conninfo = f"host={socket} dbname=offline connect_timeout=1"

    def invoke(self, *args, env=None):
        controlled = {"PATH": os.environ.get("PATH", ""), "LC_ALL": "C"}
        controlled.update(env or {})
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-invoke-") as temporary:
            controlled.setdefault("TMPDIR", temporary)
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

    def test_json_version_is_identity_free_and_matches_qualified_profile(self):
        result = self.invoke("version", "--json", env={
            "PGCOPYDB_SOURCE_PGURI": "invalid connection",
            "PGCOPYDB_TARGET_PGURI": "invalid connection",
            "RPGMIGRATE_MIGRATION_UID": "invalid",
            "RPGMIGRATE_RUN_ID": "invalid",
            "RPGMIGRATE_WORKDIR_ID": "invalid",
            "RPGMIGRATE_CHECKPOINT_GENERATION": "invalid",
            "PGCOPYDB_APPLY_GROUP_TXNS": "invalid",
            "PGCOPYDB_LOG_JSON": "on",
        })
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), self.qualified_version)
        self.assertEqual(result.stdout.count("\n"), 1)
        self.assertTrue(result.stdout.endswith("\n"))
        self.assertEqual(result.stderr, "")

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

    def test_unsupported_clone_option_does_not_create_state(self):
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-") as directory:
            state = Path(directory) / "state"
            result = self.invoke(
                "clone", "--source", self.offline_conninfo, "--target", self.offline_conninfo,
                "--dir", str(state), "--use-copy-binary",
            )
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertIn("use-copy-binary", result.stderr)
            self.assertIn("no database or state action was started", result.stderr)
            self.assertFalse(state.exists())

    def test_behavioral_copy_option_is_rejected_before_state_creation(self):
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-") as directory:
            state = Path(directory) / "state"
            result = self.invoke(
                "copy", "table-data", "--source", self.offline_conninfo,
                "--target", self.offline_conninfo, "--dir", str(state), "--follow",
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
            "clone", "--source", self.offline_conninfo, "--target", self.offline_conninfo,
            env={"PGCOPYDB_SOURCE_PGURI": "not conninfo", "PGCOPYDB_TARGET_PGURI": ""},
        )
        self.assertEqual(result.returncode, 6, result.stderr)
        self.assertEqual(json.loads(result.stderr)["category"], "Source")
        self.assertIn("connection failed", result.stderr)

    def test_diagnostics_never_echo_connection_credentials(self):
        secret = "cli-contract-private-password"
        for args in [
            ("ping", "--source", f"{self.offline_conninfo} password={secret}", "--not-an-option"),
            ("clone", "--source", f"{self.offline_conninfo} password={secret}", "--target", self.offline_conninfo),
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
            "PGCOPYDB_SOURCE_PGURI": self.offline_conninfo,
            "PGCOPYDB_TARGET_PGURI": self.offline_conninfo,
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
                if args[:2] in (("clone", "-J"), ("clone", "-I")):
                    self.assertEqual(result.returncode, 6, result.stderr)
                    self.assertEqual(json.loads(result.stderr)["category"], "Source")
                    self.assertIn("connection failed", result.stderr)
                elif args == ("stream", "sentinel", "get", "-e"):
                    self.assertEqual(result.returncode, 3, result.stderr)
                    self.assertEqual(result.stdout, "")
                    self.assertEqual(json.loads(result.stderr)["category"], "BadState")
                else:
                    self.assertEqual(result.returncode, 2, result.stderr)
                    self.assertIn("unimplemented", result.stderr.lower())
        self.assertEqual(self.invoke("list", "tables", "-k").returncode, 1)

    def test_dump_short_flags_use_the_intended_long_option_arity(self):
        # The baseline optstring omits colons, but both handlers consume optarg.
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-") as directory:
            filters = Path(directory) / "filters.ini"
            filters.write_text("[exclude-table]\npublic.ignored\n")
            result = self.invoke(
                "dump", "schema", "-S", self.offline_conninfo, "-F", str(filters),
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

    def test_m4_stream_commands_refuse_missing_state_without_success_output(self):
        commands = [
            ("stream", "sentinel", "get", "--endpos"),
            ("stream", "sentinel", "get", "--target", self.offline_conninfo, "--json"),
            ("stream", "sentinel", "get", "--target", self.offline_conninfo, "--endpos", "--json"),
            ("stream", "sentinel", "get", "--target", self.offline_conninfo, "--replay-lsn", "--json"),
            ("stream", "sentinel", "get", "--target", self.offline_conninfo, "--replay-lsn"),
            ("stream", "sentinel", "set", "endpos", "--target", self.offline_conninfo, "--json", "0/20"),
            ("stream", "sentinel", "set", "endpos", "--target", self.offline_conninfo,
             "--source", self.offline_conninfo, "--current", "--json"),
            ("stream", "cleanup", "--source", self.offline_conninfo,
             "--target", self.offline_conninfo, "--json"),
        ]
        for args in commands:
            with self.subTest(args=args), tempfile.TemporaryDirectory(prefix="rpgmigrate-m4-missing-") as directory:
                state = Path(directory) / "missing"
                result = self.invoke(*args, "--dir", str(state))
                self.assertEqual(result.returncode, 3, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertEqual(json.loads(result.stderr)["category"], "BadState")
                self.assertFalse(state.exists())
                self.assertEqual(list(Path(directory).iterdir()), [])

    def test_m4_stream_argument_refusals_do_not_create_state(self):
        for args in [
            ("stream", "sentinel", "get", "--json"),
            ("stream", "sentinel", "get", "--endpos", "--json"),
            ("stream", "sentinel", "get", "--replay-lsn"),
            ("stream", "sentinel", "set", "endpos", "--target", self.offline_conninfo, "0/0"),
            ("stream", "sentinel", "set", "endpos", "--target", self.offline_conninfo, "100000000/1"),
            ("stream", "sentinel", "get", "--target", "not conninfo", "--json"),
        ]:
            with self.subTest(args=args), tempfile.TemporaryDirectory(prefix="rpgmigrate-m4-refuse-") as directory:
                state = Path(directory) / "missing"
                result = self.invoke(*args, "--dir", str(state))
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertEqual(json.loads(result.stderr)["category"], "BadArgs")
                self.assertFalse(state.exists())

    def test_m4_other_sentinel_mutations_remain_unimplemented(self):
        for args in [("set", "startpos", "0/20"), ("set", "apply"), ("setup", "0/10", "0/20")]:
            with self.subTest(args=args):
                result = self.invoke("stream", "sentinel", *args)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertEqual(result.stdout, "")
                self.assertEqual(json.loads(result.stderr)["category"], "BadConfig")

    def test_valid_additive_controls_are_still_explicitly_unimplemented(self):
        result = self.invoke(
            "clone", "--source", self.offline_conninfo, "--target", self.offline_conninfo,
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
        environment = {"PGCOPYDB_SOURCE_PGURI": self.offline_conninfo, "PGCOPYDB_TARGET_PGURI": self.offline_conninfo}
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-cli-") as directory:
            fixture = Path(directory) / "input"
            fixture.write_text("[exclude-table]\npublic.ignored\n")
            values = {
                "source": self.offline_conninfo, "target": self.offline_conninfo,
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
                    state = Path(directory) / f"state-{count}"
                    with self.subTest(parser=parser, option=spelling):
                        args = [*path, spelling]
                        if option["arity"] == 1:
                            self.assertEqual(self.invoke(*args, env=environment).returncode, 1)
                            args.append(str(state) if name == "dir" else values.get(name, "example"))
                        if name == "wal2json-numeric-as-string":
                            args += ["--plugin", "wal2json"]
                        if parser == "root_options" and name not in {"help", "version"}:
                            args += ["clone"]
                        if parser == "cli_ping_getopts" and name not in {"help", "version"}:
                            args += ["unsupported-operand"]
                        if parser in {"cli_copy_db_getopts", "cli_compare_getopts"} and name != "dir":
                            args += ["--dir", str(state)]
                        result = self.invoke(*args, env=environment)
                        json_version = parser == "cli_print_version_getopts" and name == "json"
                        admitted_clone = parser == "cli_copy_db_getopts" and name in {
                            "source", "target", "dir", "jobs", "table-jobs", "index-jobs", "restore-jobs",
                            "no-owner", "no-acl", "no-comments", "no-tablespaces", "skip-blobs", "skip-large-objects", "restart",
                        }
                        missing_clone = parser == "cli_copy_db_getopts" and name == "resume"
                        missing_compare = parser == "cli_compare_getopts" and name in {"source", "target", "dir", "json"}
                        missing_sentinel = parser == "cli_sentinel_getopts" and name in {
                            "source", "dir", "endpos", "replay-lsn", "json",
                            "verbose", "notice", "debug", "trace", "quiet",
                        }
                        inconsistent = parser == "cli_copy_db_getopts" and name == "not-consistent"
                        invalid_current = parser == "cli_sentinel_getopts" and name == "current"
                        if name in {"help", "version"} or json_version:
                            expected = 0
                        elif admitted_clone:
                            expected = 6
                        elif missing_clone or missing_compare or missing_sentinel:
                            expected = 3
                        elif inconsistent or invalid_current:
                            expected = 1
                        else:
                            expected = 2
                        self.assertEqual(result.returncode, expected, result.stderr)
                        if expected == 6:
                            self.assertEqual(json.loads(result.stderr)["category"], "Source")
                            self.assertIn("connection failed", result.stderr)
                            self.assertTrue((state / "manifest.json").is_file())
                        elif expected == 3:
                            self.assertEqual(json.loads(result.stderr)["category"], "BadState")
                            if missing_sentinel:
                                self.assertIn("state-control: existing native state is unavailable", result.stderr)
                                self.assertEqual(result.stdout, "")
                            else:
                                self.assertIn("state-missing", result.stderr)
                            self.assertFalse(state.exists())
                        elif inconsistent:
                            self.assertIn("--not-consistent requires --resume", result.stderr)
                            self.assertFalse(state.exists())
                        elif invalid_current:
                            self.assertEqual(json.loads(result.stderr)["category"], "BadArgs")
                            self.assertIn("--current requires sentinel set endpos", result.stderr)
                            self.assertEqual(result.stdout, "")
                            self.assertFalse(state.exists())
                        elif expected == 2:
                            if parser == "cli_copy_db_getopts" and name == "follow":
                                error = json.loads(result.stderr)
                                self.assertEqual(error["category"], "BadConfig")
                                self.assertEqual(error["exit_code"], 2)
                                self.assertIn("follow requires explicit --skip-large-objects; no database or state action was started", error["message"])
                                self.assertEqual(result.stdout, "")
                            else:
                                self.assertIn("unimplemented", result.stderr.lower())
                            self.assertFalse(state.exists())
                        if json_version:
                            report = json.loads(result.stdout)
                            self.assertEqual(report, self.qualified_version)
                            self.assertEqual(result.stderr, "")
                    count += 1
            self.assertEqual(count, 188 if short else 220)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CliContract)
    if suite.countTestCases() == 0:
        raise SystemExit("No CLI contract tests were discovered")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
