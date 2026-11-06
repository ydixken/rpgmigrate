"""Check source extraction and frozen evidence without running either CLI."""

import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
EXTRACTOR = ROOT / "tools/extract-cli-contract.py"
FIXTURES = Path(__file__).resolve().parent


class ExtractionTests(unittest.TestCase):
    def module(self):
        self.assertTrue(EXTRACTOR.is_file(), "The CLI evidence extractor is missing")
        spec = importlib.util.spec_from_file_location("cli_evidence", EXTRACTOR)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_rejects_empty_source(self):
        module = self.module()
        with self.assertRaisesRegex(ValueError, "No command registrations"):
            module.extract_commands({"empty.c": ""})
        with self.assertRaisesRegex(ValueError, "No option parsers"):
            module.extract_parsers({"empty.c": ""})

    def test_rejects_changed_inventory(self):
        module = self.module()
        manifest = json.loads((FIXTURES / "baseline.json").read_text())
        for change in ("command", "parser", "long-option", "short-option", "help"):
            with self.subTest(change=change):
                changed = deepcopy(manifest)
                if change == "command":
                    changed["commands"].pop()
                elif change == "parser":
                    changed["parsers"].pop("root_options")
                elif change == "long-option":
                    changed["parsers"]["root_options"]["long_options"].pop()
                elif change == "short-option":
                    changed["parsers"]["root_options"]["short_options"].pop("J")
                else:
                    changed["help_documents"].pop()
                with self.assertRaisesRegex(ValueError, "inventory changed"):
                    module.check_inventory(changed["commands"], changed["parsers"],
                                           changed["help_documents"])

    def test_reproducible_output_and_read_only_check(self):
        module = self.module()
        frozen = (FIXTURES / "baseline.json").read_bytes()
        manifest = json.loads(frozen)
        # Replace only Git access so output and check behavior use the real code.
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(module, "generate", return_value=manifest), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()) as errors:
            output = Path(directory) / "manifest.json"
            args = ["--repo", directory, "--output", str(output)]
            self.assertEqual(module.main(args + ["--check"]), 1)
            self.assertFalse(output.exists())
            self.assertEqual(module.main(args), 0)
            self.assertEqual(output.read_bytes(), frozen)
            timestamp = output.stat().st_mtime_ns
            self.assertEqual(module.main(args + ["--check"]), 0)
            self.assertEqual(output.stat().st_mtime_ns, timestamp)
            second = Path(directory) / "second.json"
            manifest["sources"] = dict(reversed(list(manifest["sources"].items())))
            self.assertEqual(module.main(["--repo", directory, "--output", str(second)]), 0)
            self.assertEqual(second.read_bytes(), frozen)
            output.write_bytes(frozen + b" ")
            self.assertEqual(module.main(args + ["--check"]), 1)
            self.assertEqual(output.read_bytes(), frozen + b" ")
            self.assertIn("Frozen manifest differs", errors.getvalue())

    def test_resolves_spliced_help_without_description_flags(self):
        module = self.module()
        source = r'''
#define HELP \
    "  --skip-analyze  Run vacuumdb --analyze-only\n" \
    "  --filters       Select, filter, copy\n"
/* CommandLine ghost = make_command("ghost", "", "", "", NULL, run); */
CommandLine root = make_command_set("pgcopydb", "", NULL, NULL, NULL, children);
CommandLine leaf = make_command("copy", "Copy data", "", HELP, parse, run);
CommandLine *children[] = { &leaf, NULL };
void cli_help(void)
{
    CommandLine command = root;
}
'''
        commands = module.extract_commands({"cli.c": source})
        self.assertEqual([row["path"] for row in commands], [[], ["copy"]])
        self.assertEqual(commands[1]["advertised_options"], ["skip-analyze", "filters"])
        self.assertIn("Select, filter, copy", commands[1]["help"])

    def test_rejects_unresolved_tree_and_help(self):
        module = self.module()
        for body, message in [
            ('CommandLine *children[] = { &missing, NULL };', "Unresolved command"),
            ('CommandLine leaf = make_command("copy", "", "", UNKNOWN, NULL, run);\n'
             'CommandLine *children[] = { &leaf, NULL };', "Unresolved string"),
        ]:
            with self.subTest(message=message):
                source = ('CommandLine root = make_command_set('
                          '"pgcopydb", "", NULL, NULL, NULL, children);\n' + body)
                with self.assertRaisesRegex(ValueError, message):
                    module.extract_commands({"cli.c": source})

    def test_rejects_unrecognized_registration_expression(self):
        module = self.module()
        source = 'CommandLine root = make_command("pgcopydb", "", "", dynamic(), NULL, run);'
        with self.assertRaises(ValueError):
            module.extract_commands({"cli.c": source})

    def test_keeps_short_arity_separate_and_scopes_optstring(self):
        module = self.module()
        source = '''
int
version(int argc, char **argv)
{
    static struct option long_options[] = {
        { "json", no_argument, NULL, 'J' },
        { NULL, 0, NULL, 0 }
    };
    unsetenv("POSIXLY_CORRECT");
    while ((c = getopt_long(argc, argv, "Jh", long_options, &option_index)) != -1) {}
}
int
dump(int argc, char **argv)
{
    static struct option long_options[] = {
        { "filter", required_argument, NULL, 'F' },
        { "filters", required_argument, NULL, 'F' },
        { NULL, 0, NULL, 0 }
    };
    const char *optstring = "Fh";
    while ((c = getopt_long(argc, argv, optstring, long_options, &option_index)) != -1) {}
}
'''
        parsers = module.extract_parsers({"cli.c": source})
        self.assertEqual(parsers["version"]["optstring"], "Jh")
        self.assertEqual(parsers["dump"]["short_options"]["F"], 0)
        self.assertEqual(parsers["dump"]["long_options"][0]["arity"], 1)
        self.assertEqual(parsers["dump"]["aliases"], [["filter", "filters"]])

    def test_rejects_unresolved_option_row(self):
        module = self.module()
        source = '''
parse(int argc, char **argv)
{
    static struct option long_options[] = { BROKEN_ROW, { NULL, 0, NULL, 0 } };
    getopt_long(argc, argv, "h", long_options, &option_index);
}
'''
        with self.assertRaisesRegex(ValueError, "option"):
            module.extract_parsers({"cli.c": source})

    def test_missing_repository_fails_without_output(self):
        self.module()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "manifest.json"
            result = subprocess.run(
                [sys.executable, "-B", str(EXTRACTOR), "--repo", directory,
                 "--output", str(output)], capture_output=True, text=True, check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("error:", result.stderr)
            self.assertFalse(output.exists())


class FrozenEvidenceTests(unittest.TestCase):
    def read(self, name):
        path = FIXTURES / name
        self.assertTrue(path.is_file(), f"Missing evidence: {name}")
        return json.loads(path.read_text())

    def test_complete_command_paths_are_not_just_counted(self):
        manifest = self.read("baseline.json")
        expected = {"clone", "fork", "follow", "snapshot", "ping", "help", "version"}
        families = {
            "compare": "schema data",
            "copy": "db roles extensions schema data table-data blobs sequences indexes constraints",
            "dump": "schema roles",
            "restore": "schema pre-data post-data roles parse-list",
            "list": "databases extensions collations tables table-parts sequences indexes depends schema progress",
            "stream": "setup cleanup prune prefetch catchup replay receive apply",
            "stream sentinel": "setup get",
            "stream sentinel set": "startpos endpos apply prefetch",
        }
        for parent, children in families.items():
            expected.update(parent + " " + child for child in children.split())
        self.assertEqual({" ".join(c["path"]) for c in manifest["commands"] if c["kind"] == "leaf"}, expected)
        self.assertEqual(manifest["inventory"]["nodes"], 59)
        self.assertEqual(manifest["inventory"]["long_option_names"], 77)
        self.assertEqual(manifest["inventory"]["long_option_rows"], 220)
        parsers = manifest["parsers"]
        self.assertEqual(len(parsers), 11)
        self.assertEqual(parsers["cli_copy_db_getopts"]["short_options"]["J"], 1)
        self.assertEqual(parsers["root_options"]["short_options"]["J"], 0)
        self.assertEqual(parsers["cli_list_db_getopts"]["short_options"]["k"], 1)
        self.assertEqual(parsers["cli_dump_schema_getopts"]["short_options"]["N"], 0)
        self.assertEqual(manifest["sources"]["src/bin/pgcopydb/cli_common.c"]["sha256"],
                         "c066c07327bb9f0f5fa608248426456d870f4dc2b6849549cf652782f5cb251e")

    def test_frozen_references_and_reviewed_deltas_are_resolvable(self):
        manifest = self.read("baseline.json")
        cases = self.read("cases.json")["cases"]
        anomalies = self.read("anomalies.json")["anomalies"]
        deltas = self.read("target-deltas.json")["deltas"]
        self.read("schema.json")
        self.assertGreaterEqual(len(cases), 30)
        self.assertGreaterEqual(len(anomalies), 8)
        by_id = {d["id"]: d for d in deltas}
        self.assertEqual(len(by_id), len(deltas))
        self.assertEqual(len({c["id"] for c in cases}), len(cases))
        self.assertTrue(any(c["scope"] == "environment" for c in cases))
        self.assertTrue(any(c["scope"] == "defaults" for c in cases))
        for case in cases:
            self.assertEqual(case["argv"][0], "pgcopydb")
            self.assertEqual(case["evidence"], "source")
            self.assertIn("baseline_observation", case)
            if case.get("delta"):
                self.assertIn(case["delta"], by_id)
                self.assertEqual(by_id[case["delta"]]["status"], "reviewed")
                self.assertIsNotNone(case["target_expectation"])
            elif case["baseline_observation"]["outcome"] == "unsafe":
                self.assertIsNone(case["target_expectation"])
            self.assertTrue(case["at"])
        for record in anomalies:
            if record["unsafe"]:
                self.assertEqual(record["oracle"], "do-not-run")
                self.assertIsNone(record.get("target_requirement"))

        def visit(value):
            if isinstance(value, dict):
                if set(value) == {"path", "start", "end"}:
                    self.assertIn(value["path"], manifest["sources"])
                    self.assertGreaterEqual(value["start"], 1)
                    self.assertGreaterEqual(value["end"], value["start"])
                    self.assertLessEqual(value["end"], manifest["sources"][value["path"]]["lines"])
                for child in value.values():
                    visit(child)
            elif isinstance(value, list):
                for child in value:
                    visit(child)

        for artifact in (manifest, cases, anomalies, deltas):
            visit(artifact)


if __name__ == "__main__":
    unittest.main()
