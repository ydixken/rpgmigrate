"""Exercise Task commands without compiling Rust or building native libraries."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
FLAGS = ["--silent", "--output=interleaved", "--color=false"]
FILTERS = ["--skip", "pg::live_tests::", "--skip", "pg::prepared_live_tests::",
           "--skip", "pg::replication_live_tests::", "--skip", "apply::checkpoint::live_tests::",
           "--skip", "catalog::tests::live_", "--skip", "copy::tests::live_",
           "--skip", "catalog::clone::tests::live_identity_", "--skip", "catalog::follow::tests::live_identity_",
           "--skip", "snapshot::follow_tests::live_", "--skip", "snapshot::follow_fault_tests::live_",
           "--skip", "cdc::receive::tests::live_", "--skip", "pg::follow_receive_tests::live_",
           "--skip", "apply::plan::tests::live_"]
FOLLOW_FILTERS = ["--skip", "snapshot::follow_tests::live_", "--skip", "snapshot::follow_fault_tests::live_", "--skip", "cdc::receive::tests::live_",
                  "--skip", "pg::follow_receive_tests::live_", "--skip", "apply::plan::tests::live_",
                  "--skip", "apply::checkpoint::live_tests::follow_",
                  "--skip", "cdc::receive::tests::full_fifo_status_survives_walsender_timeout"]
TARGETS = {"check", "fmt-check", "lint", "build", "test", "test-unit", "test-cli", "test-harness",
           "test-db", "test-engine", "test-transport", "test-prepared", "test-replication", "test-authority",
           "test-db-self-check", "default", "help", "test-catalog", "test-source-catalog", "test-clone", "test-schema", "image:build", "image:smoke", "image:dev:publish", "test-e2e-clone-unit", "test-e2e-clone",
           "test-e2e-clone-16-16", "test-e2e-clone-18-18", "test-e2e-clone-16-18",
           "test-e2e-follow-smoke-16-16", "test-e2e-follow-smoke-18-18", "test-e2e-follow-smoke-16-18",
           "test-follow-source", "test-follow-snapshot", "test-follow-authority", "test-follow-receive", "test-follow-apply", "test-e2e-follow-unit", "test-e2e-follow", "test-e2e-follow-16-16", "test-e2e-follow-18-18", "test-e2e-follow-16-18", "test-e2e-cutover", "test-e2e-cutover-16-16", "test-e2e-cutover-18-18", "test-e2e-cutover-16-18"}
FORWARDED = ("RPGMIGRATE_E2E_REPORT_PROBE", "CONTAINER_RUNTIME", "CC", "CFLAGS", "CPPFLAGS", "LDFLAGS", "PKG_CONFIG_PATH", "PG_BINDIR", "PG_CONFIG",
             "CARGO_TARGET_DIR", "RPGMIGRATE_BIN", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION",
             "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM")


def load_bootstrap():
    spec = importlib.util.spec_from_file_location("bootstrap_task", ROOT / "tools/bootstrap-task.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((ROOT / "tools/bootstrap-task.py").is_file(), "Task bootstrap is missing")
        self.module = load_bootstrap()
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz") as archive:
            for name, data in (("task", b"#!/bin/sh\nprintf '3.54.0\\n'\n"), ("LICENSE", b"MIT fixture\n")):
                member = tarfile.TarInfo(name)
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))
        self.archive = stream.getvalue()
        self.key = "darwin_arm64"
        self.patches = [patch.object(self.module, "ROOT", self.root),
                        patch.object(self.module.platform, "system", return_value="Darwin"),
                        patch.object(self.module.platform, "machine", return_value="arm64"),
                        patch.dict(self.module.SHA256, {self.key: hashlib.sha256(self.archive).hexdigest()}),
                        patch.dict(os.environ, {}, clear=True)]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def install(self):
        with patch.object(self.module.urllib.request, "urlopen", return_value=io.BytesIO(self.archive)):
            return self.module.bootstrap()

    def test_install_and_offline_cache_verify(self):
        binary = self.install()
        self.assertEqual(binary, self.root / "target/tools/task/3.54.0/bin/task")
        self.assertTrue(os.access(binary, os.X_OK))
        self.assertEqual(subprocess.check_output([binary, "--version"], text=True).strip(), "3.54.0")
        with patch.object(self.module.urllib.request, "urlopen", side_effect=AssertionError("network on cache hit")):
            self.assertEqual(self.module.bootstrap(), binary)

    def test_bad_download_does_not_install(self):
        self.module.SHA256[self.key] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            self.install()
        self.assertFalse((self.root / "target/tools/task/3.54.0/bin/task").exists())

    def test_cached_binary_tampering_fails(self):
        binary = self.install()
        binary.write_text("#!/bin/sh\nexit 0\n")
        with self.assertRaisesRegex(RuntimeError, "binary"):
            self.module.bootstrap()

    def test_cached_archive_tampering_fails(self):
        self.install()
        next(self.root.rglob("*.tar.gz")).write_bytes(b"corrupt")
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            self.module.bootstrap()

    def test_missing_archive_member_fails(self):
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w:gz"):
            pass
        self.archive = stream.getvalue()
        self.module.SHA256[self.key] = hashlib.sha256(self.archive).hexdigest()
        with self.assertRaisesRegex(RuntimeError, "task"):
            self.install()

    def test_unsupported_platform_fails(self):
        with patch.object(self.module.platform, "machine", return_value="riscv64"):
            with self.assertRaisesRegex(RuntimeError, "platform"):
                self.install()

    def test_symlink_cache_fails(self):
        (self.root / "target").symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "symlink"):
            self.install()

    def test_dry_environment_fails(self):
        with patch.dict(os.environ, {"TASK_DRY": "true"}):
            with self.assertRaisesRegex(RuntimeError, "TASK_DRY"):
                self.install()



class TaskfileTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not (ROOT / "tools/bootstrap-task.py").is_file():
            raise AssertionError("Task bootstrap is missing")
        module = load_bootstrap()
        cls.binary = os.environ.get("TASK_BIN") or module.bootstrap()
        version = subprocess.check_output([str(cls.binary), "--version"], text=True).strip()
        if version != module.VERSION:
            raise AssertionError("Task binary does not match the configured version")

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="task contract $TASK_PATH 'quote' ")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        shutil.copyfile(ROOT / "Taskfile.yml", self.root / "Taskfile.yml")
        self.log = self.root / "calls.jsonl"
        self.bin = self.root / "fake bin"
        self.bin.mkdir()
        self.env = {key: value for key, value in os.environ.items()
                    if not key.startswith("TASK_") and key not in
                    ("CARGO", "PYTHON", "PROFILE", "IMAGE", "IMAGE_REVISION", "PQ_LIB_DIR", "STORAGE_ROOT", *FORWARDED)}
        self.env.update(PATH=str(self.bin) + os.pathsep + os.environ["PATH"], CALL_LOG=str(self.log))
        script = "#!" + sys.executable + "\n" + '''import json, os, pathlib, sys
args = sys.argv[1:]
name = pathlib.Path(sys.argv[0]).name
keys = ["RPGMIGRATE_E2E_REPORT_PROBE", "CONTAINER_RUNTIME", "PQ_LIB_DIR", "STORAGE_ROOT", "CC", "CFLAGS", "CPPFLAGS", "LDFLAGS", "PKG_CONFIG_PATH", "PG_BINDIR", "PG_CONFIG", "CARGO_TARGET_DIR", "RPGMIGRATE_BIN", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM"]
with open(os.environ["CALL_LOG"], "a") as out:
    out.write(json.dumps({"name": name, "argv": args, "env": {k: os.environ.get(k) for k in keys}}) + "\\n")
if args and args[0].endswith(".py") and not pathlib.Path(args[0]).is_file():
    sys.exit(2)
if "--list" in args:
    default = "1"
    count = int(os.environ.get("DISCOVERY_COUNT", default))
    for number in range(count):
        print(f"case_{number}: test")
if os.environ.get("FAIL_MATCH") and os.environ["FAIL_MATCH"] in " ".join(args):
    sys.exit(9)
'''
        for name in ("cargo", "python3", "chosen cargo", "chosen python", "podman", "chosen runtime"):
            path = self.bin / name
            path.write_text(script)
            path.chmod(0o755)
        for path in ("tools/test-db.py", "tests/cli_contract.py", "tools/ci/unit.py", "tools/e2e/clone.py", "tools/e2e/test_clone.py", "tools/e2e/test_copy_features.py", "tools/e2e/follow.py", "tools/e2e/test_follow.py", "tools/image/smoke.py", "tools/image/dev_publish.py"):
            fixture = self.root / path
            fixture.parent.mkdir(parents=True, exist_ok=True)
            fixture.touch()
    def run_task(self, *args, env=None):
        return subprocess.run([str(self.binary), *FLAGS, *args], cwd=self.root,
                              env={**self.env, **(env or {})}, capture_output=True, text=True, timeout=20)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def passed(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_default_checks_stock_cargo_without_library_override(self):
        self.passed(self.run_task())
        self.assertEqual([row["argv"] for row in self.calls()], [["check", "--locked", "--all-targets"]])
        self.assertIsNone(self.calls()[0]["env"]["PQ_LIB_DIR"])

    def test_inventory_and_help(self):
        result = self.run_task("--list-all")
        self.passed(result)
        names = set(result.stdout.splitlines())
        self.assertEqual(names, TARGETS)
        self.passed(self.run_task("help"))
        self.assertEqual(self.calls(), [])

    def test_retired_targets_fail_without_executing_children(self):
        self.assertFalse((ROOT / "tests/support/test_source_snapshot_harness.py").exists())
        for target in ("native", "native-check", "test-native", "test-storage-format",
                       "test-storage-recovery", "test-storage-publication", "storage-pilot",
                       "test-journal-serial", "ci:linux", "test-source-snapshot"):
            with self.subTest(target=target):
                self.assertNotEqual(self.run_task(target).returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_cli_overrides_environment_and_quotes_paths(self):
        result = self.run_task("check", "CARGO=" + shlex.quote(str(self.bin / "chosen cargo")),
                               "PYTHON=" + shlex.quote(str(self.bin / "chosen python")), "PQ_LIB_DIR=/cli path/'quoted'",
                               env={"CARGO": "missing", "PYTHON": "missing", "PQ_LIB_DIR": "/env path"})
        self.passed(result)
        self.assertEqual([row["name"] for row in self.calls()], ["chosen cargo"])
        self.assertEqual({row["env"]["PQ_LIB_DIR"] for row in self.calls()}, {"/cli path/'quoted'"})

    def test_environment_overrides_defaults_and_passes_through(self):
        values = {"CC": "clang -fno-common", "CFLAGS": "-O2", "PKG_CONFIG_PATH": "/pkg path",
                  "PG_BINDIR": "/pg bin", "PG_CONFIG": "/pg config", "CARGO_TARGET_DIR": "/cargo out",
                  "RPGMIGRATE_BIN": "/migration bin", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION": "17.11",
                  "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM": "170011"}
        self.passed(self.run_task("check", env={**values, "CARGO": shlex.quote(str(self.bin / "chosen cargo")),
                                               "PYTHON": shlex.quote(str(self.bin / "chosen python")), "PQ_LIB_DIR": "/env pq"}))
        for row in self.calls():
            self.assertEqual({key: row["env"][key] for key in values}, values)
            self.assertEqual(row["env"]["PQ_LIB_DIR"], "/env pq")

    def test_cli_toolchain_overrides_reach_children(self):
        values = {name: "cli value '$literal' " + name for name in FORWARDED}
        self.passed(self.run_task("check", *(name + "=" + value for name, value in values.items()),
                                 env={name: "env-value" for name in FORWARDED}))
        for row in self.calls():
            self.assertEqual({name: row["env"][name] for name in FORWARDED}, values)

    def test_toolchain_unset_and_defined_empty_remain_distinct(self):
        self.passed(self.run_task("check"))
        self.assertEqual({name: self.calls()[-1]["env"][name] for name in FORWARDED},
                         dict.fromkeys(FORWARDED))
        self.passed(self.run_task("check", *(name + "=" for name in FORWARDED),
                                 env={name: "inherited" for name in FORWARDED}))
        self.assertEqual({name: self.calls()[-1]["env"][name] for name in FORWARDED},
                         dict.fromkeys(FORWARDED, ""))

    def test_command_prefix_arguments_from_cli_and_environment(self):
        for cli in (False, True):
            with self.subTest(cli=cli):
                values = {"CARGO": "cargo --offline", "PYTHON": "python3 -B"}
                self.passed(self.run_task("check", *(name + "=" + value for name, value in values.items())
                                         if cli else (), env={name: "missing" for name in values} if cli else values))
                self.assertEqual(self.calls()[-1]["argv"], ["--offline", "check", "--locked", "--all-targets"])
                self.passed(self.run_task("test-harness", *(name + "=" + value for name, value in values.items())
                                         if cli else (), env={name: "missing" for name in values} if cli else values))
                self.assertEqual(self.calls()[-1]["argv"], ["-B", "tools/ci/unit.py", "--harness"])

    def test_quoted_command_paths_and_arguments_through_database_wrapper(self):
        cargo = shlex.join([str(self.bin / "chosen cargo"), "--offline", "--config", "value with 'quotes' $literal"])
        python = shlex.join([str(self.bin / "chosen python"), "-B"])
        self.passed(self.run_task("test-engine", "CARGO=" + cargo, "PYTHON=" + python, "PG_BINDIR=/cli pg",
                                 env={"CARGO": "missing", "PYTHON": "missing", "PG_BINDIR": "/env pg"}))
        self.assertEqual(self.calls()[-1]["argv"],
                         ["-B", "tools/test-db.py", "--engine", "--", str(self.bin / "chosen cargo"),
                          "--offline", "--config", "value with 'quotes' $literal", "test", "--locked", "--lib",
                          "--", "--ignored", "--test-threads=1", "--skip", "catalog::tests::live_cdc_",
                          "--skip", "catalog::tests::live_clone_admission_",
                          "--skip", "catalog::tests::live_source_identity_",
                          "--skip", "catalog::clone::tests::live_identity_", "--skip", "catalog::follow::tests::live_identity_",
                                  "--skip", "catalog::follow::tests::live_cleanup_",
                          "--skip", "catalog::tests::live_money_clone_preserves_values_and_defaults_across_role_locales",
                          *FOLLOW_FILTERS,
                                  "--skip", "snapshot::clone_tests::", "--skip", "copy::live_tests::", "--skip", "apply::checkpoint::clone_marker_tests::", "--skip", "schema::tests::live_"])
        self.assertEqual(self.calls()[-1]["env"]["PG_BINDIR"], "/cli pg")

    def test_defined_empty_never_uses_default(self):
        for variable, target in (("CARGO", "check"), ("PYTHON", "test-harness"), ("PQ_LIB_DIR", "check")):
            for cli in (False, True):
                with self.subTest(variable=variable, cli=cli):
                    result = self.run_task(target, *([variable + "="] if cli else []),
                                           env={variable: ""} if not cli else {variable: "inherited"})
                    self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)


    def test_missing_task_tool_and_file_fail(self):
        self.assertNotEqual(self.run_task("absent-task").returncode, 0)
        self.assertNotEqual(self.run_task("fmt-check", "CARGO=/missing/tool").returncode, 0)
        (self.root / "tests/cli_contract.py").unlink()
        self.assertNotEqual(self.run_task("test-cli").returncode, 0)

    def test_child_failure_stops_sequential_lint(self):
        self.assertNotEqual(self.run_task("lint", env={"FAIL_MATCH": "fmt"}).returncode, 0)
        self.assertEqual([row["argv"] for row in self.calls()], [["fmt", "--all", "--", "--check"]])

    def test_force_cannot_bypass_child_failure(self):
        self.assertNotEqual(self.run_task("--force", "check", env={"FAIL_MATCH": "check"}).returncode, 0)
        self.assertEqual(len(self.calls()), 1)

    def test_dry_environment_cannot_report_success(self):
        self.assertNotEqual(self.run_task("check", env={"TASK_DRY": "true"}).returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_all_local_suites_run_sequentially(self):
        self.passed(self.run_task("test"))
        self.assertEqual([row["argv"] for row in self.calls()], [
            ["test", "--locked", "--lib", "--", "--list", *FILTERS],
            ["test", "--locked", "--lib", "--bins", "--", *FILTERS],
            ["build", "--locked"], ["tests/cli_contract.py"], ["tools/ci/unit.py"],
        ])




    def test_unit_discovery_and_filters(self):
        self.passed(self.run_task("test-unit"))
        self.assertEqual([row["argv"] for row in self.calls()],
                         [["test", "--locked", "--lib", "--", "--list", *FILTERS],
                          ["test", "--locked", "--lib", "--bins", "--", *FILTERS]])

    def test_empty_discovery_fails_before_execution(self):
        self.assertNotEqual(self.run_task("test-unit", env={"DISCOVERY_COUNT": "0"}).returncode, 0)
        self.assertEqual(len(self.calls()), 1)

    def test_failed_discovery_pipeline_fails_even_with_test_output(self):
        self.assertNotEqual(self.run_task("test-unit", env={"FAIL_MATCH": "--list"}).returncode, 0)
        self.assertEqual(len(self.calls()), 1)


    def test_live_wrapper_arguments(self):
        cases = {"transport": "pg::live_tests::", "prepared": "pg::",
                 "replication": "pg::replication_live_tests::", "authority": "apply::checkpoint::live_tests::",
                 "catalog": "catalog::tests::live_",
                 "source-catalog": "catalog::tests::live_source_identity_",
                 "follow-source": "snapshot::follow_tests::live_follow_",
                 "follow-snapshot": "snapshot::follow_fault_tests::live_follow_",
                 "follow-authority": "apply::checkpoint::live_tests::follow_",
                 "follow-apply": "apply::plan::tests::live_"}
        for profile, namespace in cases.items():
            with self.subTest(profile=profile):
                self.passed(self.run_task("test-" + profile))
                expected = ["tools/test-db.py", "--" + profile, "--", "cargo", "test", "--locked",
                            "--lib", namespace, "--", "--ignored", "--test-threads=1"]
                if profile == "follow-source":
                    expected = ["tools/test-db.py", "--follow-source", "--", "cargo", "test", "--locked",
                                "--lib", "--", "--ignored", "--test-threads=1", namespace,
                                "catalog::follow::tests::live_cleanup_"]
                if profile == "follow-apply":
                    expected = ["tools/test-db.py", "--follow-apply", "--", "cargo", "test", "--locked",
                                "--lib", "--", "--ignored", "--test-threads=1", namespace,
                                "catalog::clone::tests::live_identity_", "catalog::follow::tests::live_identity_"]
                if profile == "prepared":
                    expected += ["--skip", "pg::live_tests::", "--skip", "pg::replication_live_tests::", "--skip", "pg::follow_receive_tests::live_"]
                if profile == "authority":
                    expected += ["--skip", "apply::checkpoint::live_tests::follow_"]
                if profile == "catalog":
                    expected += ["--skip", "catalog::tests::live_source_identity_", "--skip", "catalog::tests::live_preflight_"]
                self.assertEqual(self.calls()[-1]["argv"], expected)



    def test_follow_source_filters_select_snapshot_and_cleanup_without_other_profiles(self):
        self.passed(self.run_task("test-follow-source"))
        command = self.calls()[-1]["argv"][3:]
        separator = command.index("--")
        filters = [argument for argument in command[command.index("--lib") + 1:separator]
                   + command[separator + 1:] if not argument.startswith("--")]
        with patch.object(sys, "path", [str(ROOT / "tests/support"), *sys.path]):
            from db_harness import load_manifest, module_cases
            manifest = load_manifest()
            expected = set(module_cases(manifest, "FollowSource"))
            self.assertEqual(len(expected), 12)
            self.assertEqual(sum(name.startswith("snapshot::follow_tests::") for name in expected), 7)
            self.assertEqual(sum(name.startswith("catalog::follow::tests::live_cleanup_") for name in expected), 5)
            universe = {name for profile in ("Transport", "Prepared", "Authority", "Replication", "Engine",
                                            "Catalog", "SourceCatalog", "Clone", "Schema", "FollowSource",
                                            "FollowSnapshot", "FollowAuthority", "FollowReceive", "FollowApply")
                        for name in module_cases(manifest, profile)}
            selected = {name for name in universe if any(value in name for value in filters)}
            self.assertEqual(selected, expected)

    def test_schema_task_selects_exact_current_inventory(self):
        self.passed(self.run_task("test-schema"))
        command = self.calls()[-1]["argv"]
        self.assertEqual(command, ["tools/test-db.py", "--schema", "--", "cargo", "test", "--locked",
                                   "--lib", "--", "--ignored", "--test-threads=1", "schema::tests::live_"])
        cargo = command[command.index("cargo") + 1:]
        separator = cargo.index("--")
        self.assertEqual(cargo[:separator], ["test", "--locked", "--lib"])
        filters = [value for value in cargo[separator + 1:] if not value.startswith("--")]
        with patch.object(sys, "path", [str(ROOT / "tests/support"), *sys.path]):
            from db_harness import load_manifest, module_cases
            manifest = load_manifest()
            expected = set(module_cases(manifest, "Schema"))
            self.assertEqual(len(expected), 3)
            universe = {name for profile in ("Transport", "Prepared", "Authority", "Replication", "Engine",
                                            "Catalog", "SourceCatalog", "Clone", "Schema", "FollowSource",
                                            "FollowSnapshot", "FollowAuthority", "FollowReceive", "FollowApply")
                        for name in module_cases(manifest, profile)}
            selected = {name for name in universe if any(value in name for value in filters)}
            self.assertEqual(selected, expected)

    def test_image_tasks_preserve_runtime_image_and_python_overrides(self):
        self.passed(self.run_task("image:build"))
        self.assertEqual(self.calls()[-1]["name"], "podman")
        self.assertEqual(self.calls()[-1]["argv"], ["build", "--tag", "localhost/rpgmigrate:dev", "--file", "Dockerfile", "."])
        image = "localhost/chosen:tag-$literal-'quoted'"
        runtime = str(self.bin / "chosen runtime")
        self.passed(self.run_task("image:build", "CONTAINER_RUNTIME=" + runtime, "IMAGE=" + image,
                                 env={"CONTAINER_RUNTIME": "missing", "IMAGE": "wrong"}))
        self.assertEqual(self.calls()[-1]["name"], "chosen runtime")
        self.assertEqual(self.calls()[-1]["argv"], ["build", "--tag", image, "--file", "Dockerfile", "."])
        self.passed(self.run_task("image:smoke", "CONTAINER_RUNTIME=" + runtime, "IMAGE=" + image))
        self.assertEqual(self.calls()[-1]["argv"], ["tools/image/smoke.py", "--image", image])
        self.assertEqual(self.calls()[-1]["env"]["CONTAINER_RUNTIME"], runtime)

    def test_clone_unit_target_includes_copy_feature_controls_and_stops_on_failure(self):
        self.passed(self.run_task("test-e2e-clone-unit"))
        self.assertEqual([row["argv"] for row in self.calls()],
                         [["-W", "error", "tools/e2e/" + name] for name in
                          ("test_clone.py", "test_copy_features.py")])
        for name, expected in (("test_clone.py", 1), ("test_copy_features.py", 2)):
            self.log.unlink()
            self.assertNotEqual(self.run_task("test-e2e-clone-unit", env={"FAIL_MATCH": name}).returncode, 0)
            self.assertEqual(len(self.calls()), expected)

    def test_image_revision_is_narrow_and_precedes_container_effects(self):
        revision = "a" * 40
        self.passed(self.run_task("image:build", "IMAGE_REVISION=" + revision,
                                 env={"IMAGE_REVISION": "b" * 40}))
        self.assertEqual(self.calls()[-1]["argv"], ["build", "--tag", "localhost/rpgmigrate:dev",
                         "--build-arg", "SOURCE_REVISION=" + revision, "--file", "Dockerfile", "."])
        self.log.unlink()
        for value in ("a" * 39, "a" * 41, "A" * 40, "$(touch injected)", "abc def"):
            with self.subTest(value=value):
                self.assertNotEqual(self.run_task("image:build", "IMAGE_REVISION=" + value).returncode, 0)
        self.assertEqual(self.calls(), [])
        self.assertFalse((self.root / "injected").exists())

    def test_dev_publication_task_quotes_inputs_and_requires_all_values(self):
        output = str(self.root / "private $literal 'quoted'")
        receipt = output + "/receipt.json"
        revision = "c" * 40
        self.assertNotEqual(self.run_task("image:dev:publish").returncode, 0)
        self.assertEqual(self.calls(), [])
        self.passed(self.run_task("image:dev:publish", "ARCHITECTURE=arm64", "IMAGE_REVISION=" + revision,
                                 "OUTPUT_DIR=" + output, "RETAINED_RECEIPT=" + receipt))
        self.assertEqual(self.calls()[-1]["argv"], ["tools/image/dev_publish.py", "publish", "--architecture",
                         "arm64", "--expected-commit", revision, "--output-dir", output,
                         "--retained-receipt", receipt])
        self.assertNotIn("--bootstrap-native", self.calls()[-1]["argv"])

    def test_dev_publication_dry_run_refuses_each_missing_or_invalid_input(self):
        required = {"ARCHITECTURE": "arm64", "IMAGE_REVISION": "a" * 40, "OUTPUT_DIR": str(self.root / "private")}
        for missing in required:
            values = [name + "=" + value for name, value in required.items() if name != missing]
            with self.subTest(missing=missing):
                self.assertNotEqual(self.run_task("--dry", "image:dev:publish", *values).returncode, 0)
        for revision in ("", "a" * 39, "A" * 40, "$(touch injected)"):
            with self.subTest(revision=revision):
                values = [name + "=" + value for name, value in {**required, "IMAGE_REVISION": revision}.items()]
                self.assertNotEqual(self.run_task("--dry", "image:dev:publish", *values).returncode, 0)
        self.passed(self.run_task("--dry", "image:dev:publish", *[name + "=" + value for name, value in required.items()]))
        self.assertEqual(self.calls(), [])
        self.assertFalse((self.root / "injected").exists())

    def test_clone_profiles_reach_each_pair_and_sequential_matrix(self):
        pairs = ("16-16", "18-18", "16-18")
        for profile in (None, "disabled", "m8"):
            args = () if profile is None else ("PROFILE=" + profile,)
            for target, selected in [("test-e2e-clone-" + pair, (pair,)) for pair in pairs] + [("test-e2e-clone", pairs)]:
                with self.subTest(profile=profile, target=target):
                    self.log.unlink(missing_ok=True)
                    self.passed(self.run_task(target, *args))
                    self.assertEqual([row["argv"] for row in self.calls()],
                                     [["tools/e2e/clone.py", "--pair", pair, "--profile", profile or "disabled"]
                                      for pair in selected])
        for profile in ("disabled", "m8"):
            self.log.unlink()
            self.assertNotEqual(self.run_task("test-e2e-clone", "PROFILE=" + profile,
                                             env={"FAIL_MATCH": "18-18"}).returncode, 0)
            self.assertEqual([row["argv"][2] for row in self.calls()], ["16-16", "18-18"])

    def test_clone_profile_environment_cli_precedence_and_runtime_quoting(self):
        self.passed(self.run_task("test-e2e-clone", env={"PROFILE": "m8", "CONTAINER_RUNTIME": "inherited runtime"}))
        self.assertEqual([row["argv"][-1] for row in self.calls()], ["m8"] * 3)
        self.assertEqual([row["env"]["CONTAINER_RUNTIME"] for row in self.calls()], ["inherited runtime"] * 3)
        runtime = "runtime path 'quoted' $(touch injected); $literal"
        self.log.unlink()
        self.passed(self.run_task("test-e2e-clone", "PROFILE=disabled", "CONTAINER_RUNTIME=" + runtime,
                                 env={"PROFILE": "m8", "CONTAINER_RUNTIME": "missing"}))
        self.assertEqual([row["argv"][-1] for row in self.calls()], ["disabled"] * 3)
        self.assertEqual([row["env"]["CONTAINER_RUNTIME"] for row in self.calls()], [runtime] * 3)
        self.assertFalse((self.root / "injected").exists())

    def test_clone_profile_invalid_empty_and_injection_fail_before_children_even_dry(self):
        for target in ("test-e2e-clone", "test-e2e-clone-16-16", "test-e2e-clone-18-18", "test-e2e-clone-16-18"):
            for profile in ("", "unknown", "m8; touch injected", "$(touch injected)", "m8'\"quote"):
                for dry in (False, True):
                    for cli in (False, True):
                        with self.subTest(target=target, profile=profile, dry=dry, cli=cli):
                            self.log.unlink(missing_ok=True)
                            args = (["--dry"] if dry else []) + [target] + (["PROFILE=" + profile] if cli else [])
                            result = self.run_task(*args, env={"PROFILE": "m8" if cli else profile})
                            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                            self.assertIn("PROFILE must be disabled or m8", result.stderr)
                            self.assertEqual(self.calls(), [])
                            self.assertFalse((self.root / "injected").exists())

    def test_clone_dry_run_has_exact_profile_argv_without_children(self):
        for profile in (None, "m8"):
            result = self.run_task("--silent=false", "--dry", "test-e2e-clone", *(() if profile is None else ("PROFILE=" + profile,)))
            self.passed(result)
            commands = [line for line in (result.stdout + result.stderr).splitlines() if "tools/e2e/clone.py" in line]
            self.assertEqual(len(commands), 3)
            for pair, line in zip(("16-16", "18-18", "16-18"), commands):
                self.assertEqual(shlex.split(line)[-5:], ["tools/e2e/clone.py", "--pair", pair, "--profile", profile or "disabled"])
            self.assertEqual(self.calls(), [])

    def test_follow_matrix_enumerates_exact_pairs_and_stops_on_failure(self):
        self.passed(self.run_task("test-e2e-follow"))
        self.assertEqual([row["argv"] for row in self.calls()],
                         [["tools/e2e/follow.py", "--pair", pair] for pair in ("16-16", "18-18", "16-18")])
        self.log.unlink()
        self.assertNotEqual(self.run_task("test-e2e-follow", env={"FAIL_MATCH": "18-18"}).returncode, 0)
        self.assertEqual([row["argv"][-1] for row in self.calls()], ["16-16", "18-18"])

    def test_database_gate_runs_retained_and_follow_profiles_once_in_order(self):
        with patch.object(sys, "path", [str(ROOT / "tests/support"), *sys.path]):
            from db_harness import load_manifest, module_cases
            receive_cases = module_cases(load_manifest(), "FollowReceive")
        self.passed(self.run_task("test-db"))
        calls = [row["argv"] for row in self.calls()]
        self.assertEqual(calls, [["build", "--locked"],
                                 ["tools/test-db.py", "--engine", "--", "cargo", "test", "--locked", "--lib",
                                  "--", "--ignored", "--test-threads=1", "--skip", "catalog::tests::live_cdc_",
                                  "--skip", "catalog::tests::live_clone_admission_",
                                  "--skip", "catalog::tests::live_source_identity_",
                                  "--skip", "catalog::clone::tests::live_identity_", "--skip", "catalog::follow::tests::live_identity_",
                                  "--skip", "catalog::follow::tests::live_cleanup_",
                                  "--skip", "catalog::tests::live_money_clone_preserves_values_and_defaults_across_role_locales", *FOLLOW_FILTERS,
                                  "--skip", "snapshot::clone_tests::", "--skip", "copy::live_tests::", "--skip", "apply::checkpoint::clone_marker_tests::", "--skip", "schema::tests::live_"],
                                 ["tools/test-db.py", "--catalog", "--", "cargo", "test", "--locked", "--lib",
                                  "catalog::tests::live_", "--", "--ignored", "--test-threads=1",
                              "--skip", "catalog::tests::live_source_identity_", "--skip", "catalog::tests::live_preflight_"],
                                 ["tools/test-db.py", "--source-catalog", "--", "cargo", "test", "--locked", "--lib",
                                  "catalog::tests::live_source_identity_", "--", "--ignored", "--test-threads=1"],
                                 ["tools/test-db.py", "--clone", "--", "cargo", "test", "--locked", "--lib",
                              "--", "--ignored", "--test-threads=1", "snapshot::clone_tests::", "copy::live_tests::", "apply::checkpoint::clone_marker_tests::"],
                             ["tools/test-db.py", "--schema", "--", "cargo", "test", "--locked", "--lib",
                              "--", "--ignored", "--test-threads=1", "schema::tests::live_"],
                             ["tools/test-db.py", "--follow-source", "--", "cargo", "test", "--locked", "--lib",
                              "--", "--ignored", "--test-threads=1", "snapshot::follow_tests::live_follow_",
                              "catalog::follow::tests::live_cleanup_"],
                             ["tools/test-db.py", "--follow-snapshot", "--", "cargo", "test", "--locked", "--lib",
                              "snapshot::follow_fault_tests::live_follow_", "--", "--ignored", "--test-threads=1"],
                             ["tools/test-db.py", "--follow-authority", "--", "cargo", "test", "--locked", "--lib",
                              "apply::checkpoint::live_tests::follow_", "--", "--ignored", "--test-threads=1"],
                             ["tools/test-db.py", "--follow-receive", "--", "cargo", "test", "--locked", "--lib",
                              "--", "--include-ignored", "--exact", "--test-threads=1", *receive_cases],
                             ["tools/test-db.py", "--follow-apply", "--", "cargo", "test", "--locked", "--lib",
                              "--", "--ignored", "--test-threads=1", "apply::plan::tests::live_",
                              "catalog::clone::tests::live_identity_", "catalog::follow::tests::live_identity_"],
                             ["tools/test-db.py", "--", "cargo", "test", "--locked", "--test", "copy_integration",
                                  "--", "--test-threads=1"]])

    def test_all_harness_modules_share_one_required_runner(self):
        self.passed(self.run_task("test-harness"))
        self.assertEqual(self.calls()[-1]["argv"], ["tools/ci/unit.py", "--harness"])
        (self.root / "tools/ci/unit.py").unlink()
        self.assertNotEqual(self.run_task("test-harness").returncode, 0)

    def test_live_profiles_select_exact_required_inventories(self):
        profiles = {
            "Engine": "engine", "Catalog": "catalog", "SourceCatalog": "source-catalog",
            "FollowSource": "follow-source", "FollowSnapshot": "follow-snapshot",
            "FollowAuthority": "follow-authority", "FollowReceive": "follow-receive",
            "FollowApply": "follow-apply", "Schema": "schema", "Clone": "clone",
            "Transport": "transport", "Prepared": "prepared", "Authority": "authority",
            "Replication": "replication",
        }
        with patch.object(sys, "path", [str(ROOT / "tests/support"), *sys.path]):
            from db_harness import load_manifest, module_cases
            from test_follow_m4_harness import RECEIVE_ADDITIONS
            manifest = load_manifest()
            inventories = {profile: set(module_cases(manifest, profile)) for profile in profiles}
        universe = set().union(*inventories.values())
        normal_cases = set(RECEIVE_ADDITIONS)
        for profile, target in profiles.items():
            with self.subTest(profile=profile):
                self.passed(self.run_task("test-" + target))
                command = self.calls()[-1]["argv"]
                cargo = command[command.index("cargo") + 1:]
                separator = cargo.index("--")
                args = cargo[cargo.index("--lib") + 1:separator] + cargo[separator + 1:]
                skips = [args[index + 1] for index, value in enumerate(args) if value == "--skip"]
                filters = [value for index, value in enumerate(args)
                           if not value.startswith("--") and (index == 0 or args[index - 1] != "--skip")]
                candidates = universe - normal_cases if "--ignored" in args else universe
                selected = {name for name in candidates
                            if (not filters or any(name == value if "--exact" in args else value in name
                                                   for value in filters))
                            and not any(value in name for value in skips)}
                self.assertEqual(selected, inventories[profile])
                if profile == "FollowReceive":
                    self.assertIn("--include-ignored", args)
                    self.assertNotIn("--ignored", args)
                    self.assertIn("--exact", args)
                    self.assertEqual(set(filters), inventories[profile])
                    self.assertTrue(normal_cases.issubset(selected))
        closing = [profile for profile in profiles if profile not in
                   ("Transport", "Prepared", "Authority", "Replication")]
        for index, profile in enumerate(closing):
            for other in closing[index + 1:]:
                with self.subTest(profile=profile, other=other):
                    self.assertTrue(inventories[profile].isdisjoint(inventories[other]))
        for profile in ("Transport", "Prepared", "Authority", "Replication"):
            with self.subTest(profile=profile):
                self.assertTrue(inventories[profile].issubset(inventories["Engine"]))

    def test_engine_skips_preserve_its_required_cases(self):
        self.passed(self.run_task("test-engine"))
        args = self.calls()[-1]["argv"]
        skips = [args[index + 1] for index, value in enumerate(args) if value == "--skip"]
        with patch.object(sys, "path", [str(ROOT / "tests/support"), *sys.path]):
            from db_harness import load_manifest, module_cases
            manifest = load_manifest()
            engine = module_cases(manifest, "Engine")
            self.assertEqual(len(engine), 50)
            retained = [name for name in engine if not any(skip in name for skip in skips)]
            self.assertEqual(retained, engine)
            for profile in ("Catalog", "SourceCatalog", "Schema", "FollowSource"):
                cases = module_cases(manifest, profile)
                self.assertTrue(cases)
                self.assertEqual([name for name in cases if not any(skip in name for skip in skips)], [])
            with self.assertRaisesRegex(ValueError, "unknown module test profile"):
                module_cases(manifest, "SourceSnapshot")


    def test_default_database_gate_refuses_missing_or_failed_follow_profiles(self):
        path = self.root / "Taskfile.yml"
        original = path.read_text()
        for profile in ("source", "snapshot", "authority", "receive", "apply"):
            with self.subTest(profile=profile, failure="command"):
                self.log.unlink(missing_ok=True)
                self.assertNotEqual(self.run_task("test-db", env={"FAIL_MATCH": "--follow-" + profile}).returncode, 0)
                self.assertIn("--follow-" + profile, self.calls()[-1]["argv"])
                self.assertFalse(any("copy_integration" in row["argv"] for row in self.calls()))
            with self.subTest(profile=profile, failure="missing task"):
                start = original.index("  test-follow-" + profile + ":\n")
                end = original.index("\n  test-", start)
                path.write_text(original[:start] + original[end:])
                self.log.unlink(missing_ok=True)
                self.assertNotEqual(self.run_task("test-db").returncode, 0)
                self.assertFalse(any("copy_integration" in row["argv"] for row in self.calls()))
                path.write_text(original)

    def test_follow_unit_target_keeps_m3_protocol_controls_and_adds_cutover(self):
        self.passed(self.run_task("test-e2e-follow-unit"))
        self.assertEqual([row["argv"] for row in self.calls()],
                         [["-W", "error", "tools/e2e/" + name] for name in
                          ("test_follow.py", "test_follow_wire_protocol.py", "test_cutover.py")])

    def test_cutover_matrix_is_separate_and_preserves_exact_pairs_and_failures(self):
        self.passed(self.run_task("test-e2e-cutover"))
        self.assertEqual([row["argv"] for row in self.calls()],
                         [["tools/e2e/follow.py", "--cutover", "--pair", pair] for pair in ("16-16", "18-18", "16-18")])
        self.log.unlink()
        self.assertNotEqual(self.run_task("test-e2e-cutover", env={"FAIL_MATCH": "18-18"}).returncode, 0)
        self.assertEqual([row["argv"][-1] for row in self.calls()], ["16-16", "18-18"])



if __name__ == "__main__":
    unittest.main()
