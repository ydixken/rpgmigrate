#!/usr/bin/env python3
"""Run the documentation gate against disposable, complete repository fixtures."""
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

CHECKER = Path(__file__).with_name("check.py")
GUIDES = (
    "README.md", "docs/getting-started.md", "docs/operator-guide.md",
    "docs/troubleshooting.md", "docs/durability.md", "docs/testing.md",
    "docs/runner-image.md", "docs/operator-compatibility.md",
    "docs/native-protocol.md", "docs/native-cutover-handoff.md",
)


class CheckTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in GUIDES:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# Guide\n\nRead the guide.\n", encoding="utf-8")
        docs = self.root / "tools/docs"
        docs.mkdir(parents=True)
        shutil.copyfile(CHECKER.with_name("ste-lint.py"), docs / "ste-lint.py")
        self.calls = self.root / "help-calls.jsonl"
        self.binary = self.root / "engine"
        self.binary.write_text(
            f"#!{sys.executable}\n"
            "import json, pathlib, sys\n"
            f"with pathlib.Path({str(self.calls)!r}).open('a') as stream:\n"
            "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
            "if sys.argv[1:] == ['--help']:\n"
            "    print('Usage: rpgmigrate [OPTIONS] [COMMAND]\\n\\nCommands:\\n  clone\\n  version\\n\\nOptions:\\n  -h, --help\\n  -V, --version', file=sys.stderr)\n"
            "elif sys.argv[1:] == ['clone', '--help']:\n"
            "    print('Usage: rpgmigrate clone [OPTIONS]\\n\\nOptions:\\n  -S, --source <SOURCE>\\n  -h, --help', file=sys.stderr)\n"
            "elif sys.argv[1:] == ['version', '--help']:\n"
            "    print('Usage: rpgmigrate version [OPTIONS]\\n\\nOptions:\\n  -J, --json\\n  -h, --help')\n"
            "else:\n"
            "    sys.exit(91)\n",
            encoding="utf-8",
        )
        self.binary.chmod(0o700)
        self.task_json = self.root / "task.json"
        self.task_json.write_text(json.dumps({
            "tasks": [
                {"name": "check", "task": "check", "desc": "Check", "summary": "",
                 "aliases": [], "up_to_date": False, "location": {"line": 1, "column": 1, "taskfile": "Taskfile.yml"}},
                {"name": "image:smoke", "task": "image:smoke", "desc": "Smoke", "summary": "",
                 "aliases": ["smoke"], "up_to_date": False, "location": {"line": 2, "column": 1, "taskfile": "Taskfile.yml"}},
            ], "location": "Taskfile.yml",
        }), encoding="utf-8")

    def append(self, text, name="README.md"):
        with (self.root / name).open("a", encoding="utf-8") as stream:
            stream.write("\n" + text + "\n")

    def run_check(self, *extra):
        return subprocess.run(
            [sys.executable, str(CHECKER), "--root", str(self.root),
             "--binary", str(self.binary), "--task-json", str(self.task_json), *extra],
            capture_output=True, text=True, timeout=20,
        )

    def refused(self, category):
        result = self.run_check()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(category, result.stderr)
        return result

    def test_complete_guides_links_tasks_and_actual_help_pass(self):
        self.append("[Setup](docs/getting-started.md#guide)\n\nUse `task check smoke`.\n\n"
                    "```sh\nrpgmigrate clone --source '$SOURCE'\n```\n\n"
                    "Use `rpgmigrate version --json`.")
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("10 guides", result.stdout)

    def test_missing_expected_guide_fails_before_children(self):
        (self.root / "docs/durability.md").unlink()
        self.refused("missing guide")
        self.assertFalse(self.calls.exists())

    def test_empty_guide_is_not_an_inventory_entry(self):
        (self.root / "docs/operator-guide.md").write_text("")
        self.refused("empty guide")

    def test_broken_local_link_and_anchor_fail(self):
        for target, category in (("docs/absent.md", "broken link"),
                                 ("docs/testing.md#absent", "broken anchor")):
            with self.subTest(target=target):
                (self.root / "README.md").write_text(f"# Guide\n\n[Read]({target}).\n")
                self.refused(category)

    def test_reference_links_and_duplicate_setext_unicode_anchors(self):
        self.append("## Same\n\n## Same\n\nRésumé\n------\n")
        self.append("[First][setup]\n\n[setup]: docs/testing.md#guide\n\n"
                    "[Duplicate](#same-1)\n\n[Unicode](#r%C3%A9sum%C3%A9)")
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.append("[Broken][missing]")
        self.refused("undefined link reference")

    def test_links_do_not_escape_repository_even_through_symlinks(self):
        (self.root / "escape.md").symlink_to(CHECKER.with_name("provenance.md"))
        self.append("[Read](escape.md)")
        self.refused("outside repository")

    def test_fenced_links_and_historical_commands_are_not_live_claims(self):
        self.append("```text\n[Read](missing.md)\n```\n\n[History](docs/history.md)")
        (self.root / "docs/history.md").write_text("# History\n\n`task removed` and `rpgmigrate clone --removed`.\n")
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_separate_shell_lines_and_cli_operands_use_only_safe_help(self):
        self.append("```sh\ntask check\ntask image:smoke\nrpgmigrate clone --source '$SOURCE'\nrpgmigrate version --json\n```\n\n"
                    "Use `rpgmigrate help`.\n\nUse `rpgmigrate clone table_name --source '$SOURCE'`.")
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_reference_destination_fails(self):
        self.append("[Setup][setup]\n\n[setup]: missing.md")
        self.refused("broken link")

    def test_missing_task_fails_in_second_task_and_multiline_fence(self):
        for command in ("`task check missing`", "```sh\ntask --silent check \\\n  missing\n```"):
            with self.subTest(command=command):
                (self.root / "README.md").write_text("# Guide\n\n" + command + "\n")
                self.refused("unknown task")

    def test_bad_task_json_and_empty_task_inventory_fail(self):
        for payload in ("not-json", "{}", '{"tasks":[]}', '{"tasks":[{}]}',
                        '{"tasks":[{"name":"check","aliases":"bad"}]}',
                        '{"tasks":[],"tasks":[{"name":"check","aliases":[]}]}'):
            with self.subTest(payload=payload):
                self.task_json.write_text(payload)
                self.refused("Task inventory")

    def test_stale_flag_short_flag_and_command_fail(self):
        for command in ("rpgmigrate clone --removed", "rpgmigrate clone -Z",
                        "rpgmigrate vanished --help", "rpgmigrate clone --json"):
            with self.subTest(command=command):
                (self.root / "README.md").write_text("# Guide\n\n`" + command + "`\n")
                self.refused("CLI")

    def test_markdown_arguments_are_never_executed_or_passed_to_binary(self):
        marker = self.root / "must-not-exist"
        self.append(f'```sh\nrpgmigrate clone --source "$(touch {marker})"\n```')
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(marker.exists())
        calls = [json.loads(line) for line in self.calls.read_text().splitlines()]
        self.assertTrue(calls)
        self.assertTrue(all(call in (["--help"], ["clone", "--help"]) for call in calls), calls)

    def test_missing_or_nonexecutable_binary_fails(self):
        self.binary.chmod(0o600)
        self.refused("executable")
        self.binary.unlink()
        self.refused("executable")

    def test_failed_or_empty_help_fails(self):
        for body in ("import sys; sys.exit(1)", "pass", "print('--source')"):
            with self.subTest(body=body):
                self.binary.write_text(f"#!{sys.executable}\n{body}\n")
                self.append("`rpgmigrate --help`")
                self.refused("help")

    def test_em_dash_and_real_ste_hard_findings_fail(self):
        for prose, category in (("Use this — carefully.", "em dash"),
                                ("Read this; stop.", "STE hard"),
                                ("Spin up the job.", "STE hard"),
                                ("Use the seamless tool.", "STE hard"),
                                ("Perform an analysis of the log.", "STE hard"),
                                (" ".join(["word"] * 26) + ".", "STE hard")):
            with self.subTest(prose=prose):
                (self.root / "README.md").write_text("# Guide\n\n" + prose + "\n")
                self.refused(category)

    def test_ste_advisory_does_not_fail(self):
        self.append("The file is removed.")
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_ste_failure_malformed_json_and_inconsistent_counts_fail(self):
        ste = self.root / "tools/docs/ste-lint.py"
        for body in ("raise SystemExit(2)", "print('not-json')", "print('{}')",
                     "print('{\"violations\":[],\"count\":0,\"hard_count\":1,\"baseline\":0,\"words\":3}')"):
            with self.subTest(body=body):
                ste.write_text(body + "\n")
                self.refused("STE")

    def test_oversized_guide_and_help_output_fail(self):
        (self.root / "README.md").write_text("x" * (2 * 1024 * 1024 + 1))
        self.refused("oversized input")
        (self.root / "README.md").write_text("# Guide\n\nRead the guide.\n")
        self.binary.write_text(f"#!{sys.executable}\nprint('x' * (2 * 1024 * 1024 + 1))\n")
        self.refused("oversized output")

    def test_missing_ste_linter_fails(self):
        (self.root / "tools/docs/ste-lint.py").unlink()
        self.refused("STE: missing")

    def test_malformed_ste_findings_fail_even_with_exit_zero(self):
        payload = {"violations": [{"level": "advisory-free"}], "count": 1,
                   "hard_count": 1, "baseline": 0, "words": 10}
        (self.root / "tools/docs/ste-lint.py").write_text(f"print({json.dumps(payload)!r})\n")
        self.refused("STE: malformed finding")

    def test_invalid_utf8_guide_fails_without_traceback(self):
        (self.root / "docs/testing.md").write_bytes(b"\xff")
        result = self.refused("UTF-8")
        self.assertNotIn("Traceback", result.stderr)

    def test_empty_inventory_and_help_timeout_fail_closed(self):
        spec = importlib.util.spec_from_file_location("docs_check", CHECKER)
        checker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(checker)
        with mock.patch.object(checker, "GUIDES", ()):
            with self.assertRaisesRegex(checker.CheckError, "empty guide inventory"):
                checker.check(self.root, self.binary, self.task_json)
        self.append("`rpgmigrate --help`")
        self.binary.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(1)\n")
        with mock.patch.object(checker, "TIMEOUT", 0.05):
            with self.assertRaisesRegex(checker.CheckError, "timeout"):
                checker.check(self.root, self.binary, self.task_json)


if __name__ == "__main__":
    unittest.main()
