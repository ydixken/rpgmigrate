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
           "--skip", "catalog::tests::live_", "--skip", "copy::tests::live_"]
TARGETS = {"check", "fmt-check", "lint", "native", "native-check", "build", "test", "test-unit",
           "test-cli", "test-native", "test-harness", "test-storage-format", "test-storage-recovery",
           "storage-pilot", "test-db", "test-engine", "test-transport", "test-prepared",
           "test-replication", "test-authority", "test-db-self-check", "default", "help", "ci:linux", "image:build",
           "image:smoke", "test-storage-publication"}
FORWARDED = ("CC", "CFLAGS", "CPPFLAGS", "LDFLAGS", "PKG_CONFIG_PATH", "PG_BINDIR", "PG_CONFIG",
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
                    ("CARGO", "PYTHON", "PQ_LIB_DIR", "STORAGE_ROOT", *FORWARDED)}
        self.env.update(PATH=str(self.bin) + os.pathsep + os.environ["PATH"], CALL_LOG=str(self.log))
        script = "#!" + sys.executable + "\n" + '''import json, os, pathlib, sys
args = sys.argv[1:]
name = pathlib.Path(sys.argv[0]).name
keys = ["PQ_LIB_DIR", "STORAGE_ROOT", "CC", "CFLAGS", "CPPFLAGS", "LDFLAGS", "PKG_CONFIG_PATH", "PG_BINDIR", "PG_CONFIG", "CARGO_TARGET_DIR", "RPGMIGRATE_BIN", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM"]
with open(os.environ["CALL_LOG"], "a") as out:
    out.write(json.dumps({"name": name, "argv": args, "env": {k: os.environ.get(k) for k in keys}}) + "\\n")
if args and args[0].endswith(".py") and not pathlib.Path(args[0]).is_file():
    sys.exit(2)
if "--list" in args:
    default = "31" if "storage_format" in args else "165" if "storage_recovery" in args else "1"
    count = int(os.environ.get("DISCOVERY_COUNT", default))
    for number in range(count):
        print(f"case_{number}: test")
if os.environ.get("FAIL_MATCH") and os.environ["FAIL_MATCH"] in " ".join(args):
    sys.exit(9)
if "--c2-root" in args:
    root = pathlib.Path(args[args.index("--c2-root") + 1])
    if root.exists() or root.parent.stat().st_mode & 0o777 != 0o700:
        sys.exit(8)
    root.mkdir(mode=0o700)
    receipts = [{"version": 1, "case": i, "candidate": "segmented" if i < 14 else "sqlite",
                 "pid": i + 100, "hits": ["before_append"], "settings": {}, "input_crc32": 123,
                 "required_prefix": 2, "caller_outcome": "returned" if i in (13, 21) else "unknown",
                 "continuation": None if i in (13, 21) else {"prefix": 3, "input_crc32": 456}}
                for i in range(22)]
    mode = os.environ.get("PUBLICATION_REPORT", "valid")
    if mode != "missing":
        (root / "report.json").write_text(json.dumps({"version": 1, "receipts": receipts if mode == "valid" else []}))
if args[:2] == ["tools/ci/verify.py", "--storage-publication"]:
    os.execv(sys.executable, [sys.executable, *args])
'''
        for name in ("cargo", "python3", "chosen cargo", "chosen python"):
            path = self.bin / name
            path.write_text(script)
            path.chmod(0o755)
        for path in ("tools/build-libpq.py", "tools/test-db.py", "tests/cli_contract.py",
                     "tests/test_native_libpq.py", "tests/support/test_harness.py"):
            fixture = self.root / path
            fixture.parent.mkdir(parents=True, exist_ok=True)
            fixture.touch()
        verifier = self.root / "tools/ci/verify.py"
        verifier.parent.mkdir(parents=True)
        shutil.copyfile(ROOT / "tools/ci/verify.py", verifier)

    def run_task(self, *args, env=None):
        return subprocess.run([str(self.binary), *FLAGS, *args], cwd=self.root,
                              env={**self.env, **(env or {})}, capture_output=True, text=True, timeout=20)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def passed(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_default_checks_native_before_cargo(self):
        self.passed(self.run_task())
        self.assertEqual([row["argv"] for row in self.calls()],
                         [["tools/build-libpq.py", "--verify"], ["check", "--locked", "--all-targets"]])
        self.assertEqual(self.calls()[1]["env"]["PQ_LIB_DIR"], str(self.root / "target/native/libpq/lib"))

    def test_inventory_and_help(self):
        result = self.run_task("--list-all")
        self.passed(result)
        names = set(result.stdout.splitlines())
        self.assertEqual(names, TARGETS)
        self.passed(self.run_task("help"))
        self.assertEqual(self.calls(), [])

    def test_native_check_never_builds(self):
        self.passed(self.run_task("native-check"))
        self.assertEqual([row["argv"] for row in self.calls()], [["tools/build-libpq.py", "--verify"]])

    def test_cli_overrides_environment_and_quotes_paths(self):
        result = self.run_task("check", "CARGO=" + shlex.quote(str(self.bin / "chosen cargo")),
                               "PYTHON=" + shlex.quote(str(self.bin / "chosen python")), "PQ_LIB_DIR=/cli path/'quoted'",
                               env={"CARGO": "missing", "PYTHON": "missing", "PQ_LIB_DIR": "/env path"})
        self.passed(result)
        self.assertEqual([row["name"] for row in self.calls()], ["chosen python", "chosen cargo"])
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
        self.passed(self.run_task("native"))
        self.assertEqual({name: self.calls()[-1]["env"][name] for name in FORWARDED},
                         dict.fromkeys(FORWARDED))
        self.passed(self.run_task("native", *(name + "=" for name in FORWARDED),
                                 env={name: "inherited" for name in FORWARDED}))
        self.assertEqual({name: self.calls()[-1]["env"][name] for name in FORWARDED},
                         dict.fromkeys(FORWARDED, ""))

    def test_command_prefix_arguments_from_cli_and_environment(self):
        for cli in (False, True):
            with self.subTest(cli=cli):
                values = {"CARGO": "cargo --offline", "PYTHON": "python3 -B"}
                self.passed(self.run_task("check", *(name + "=" + value for name, value in values.items())
                                         if cli else (), env={name: "missing" for name in values} if cli else values))
                self.assertEqual([row["argv"] for row in self.calls()[-2:]],
                                 [["-B", "tools/build-libpq.py", "--verify"],
                                  ["--offline", "check", "--locked", "--all-targets"]])

    def test_quoted_command_paths_and_arguments_through_database_wrapper(self):
        cargo = shlex.join([str(self.bin / "chosen cargo"), "--offline", "--config", "value with 'quotes' $literal"])
        python = shlex.join([str(self.bin / "chosen python"), "-B"])
        self.passed(self.run_task("test-engine", "CARGO=" + cargo, "PYTHON=" + python, "PG_BINDIR=/cli pg",
                                 env={"CARGO": "missing", "PYTHON": "missing", "PG_BINDIR": "/env pg"}))
        self.assertEqual(self.calls()[-1]["argv"],
                         ["-B", "tools/test-db.py", "--engine", "--", str(self.bin / "chosen cargo"),
                          "--offline", "--config", "value with 'quotes' $literal", "test", "--locked", "--lib",
                          "--", "--ignored", "--test-threads=1"])
        self.assertEqual(self.calls()[-1]["env"]["PG_BINDIR"], "/cli pg")

    def test_defined_empty_never_uses_default(self):
        for variable in ("CARGO", "PYTHON", "PQ_LIB_DIR"):
            for cli in (False, True):
                with self.subTest(variable=variable, cli=cli):
                    result = self.run_task("check", *([variable + "="] if cli else []),
                                           env={variable: ""} if not cli else {variable: "cargo"})
                    self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_storage_root_precedence_and_required_value(self):
        self.passed(self.run_task("storage-pilot", "STORAGE_ROOT=/cli root", env={"STORAGE_ROOT": "/env root"}))
        self.assertEqual(self.calls()[-1]["argv"],
                         ["run", "--locked", "--release", "--example", "storage", "--", "--root", "/cli root"])
        self.assertEqual(self.calls()[-1]["env"]["STORAGE_ROOT"], "/cli root")
        for args, env in (([], {}), (["STORAGE_ROOT="], {"STORAGE_ROOT": "/env root"}), ([], {"STORAGE_ROOT": ""})):
            self.assertNotEqual(self.run_task("storage-pilot", *args, env=env).returncode, 0)

    def test_missing_task_tool_and_file_fail(self):
        self.assertNotEqual(self.run_task("absent-task").returncode, 0)
        self.assertNotEqual(self.run_task("fmt-check", "CARGO=/missing/tool").returncode, 0)
        (self.root / "tools/build-libpq.py").unlink()
        self.assertNotEqual(self.run_task("check").returncode, 0)
        self.assertFalse(any(row["argv"][0] == "check" for row in self.calls()))

    def test_child_failure_stops_sequential_lint(self):
        self.assertNotEqual(self.run_task("lint", env={"FAIL_MATCH": "fmt"}).returncode, 0)
        self.assertEqual([row["argv"] for row in self.calls()], [["fmt", "--all", "--", "--check"]])

    def test_force_cannot_bypass_native_gate(self):
        self.assertNotEqual(self.run_task("--force", "check", env={"FAIL_MATCH": "--verify"}).returncode, 0)
        self.assertEqual(len(self.calls()), 1)

    def test_dry_environment_cannot_report_success(self):
        self.assertNotEqual(self.run_task("check", env={"TASK_DRY": "true"}).returncode, 0)
        self.assertEqual(self.calls(), [])

    def test_all_local_suites_run_sequentially(self):
        result = self.run_task("test")
        self.passed(result)
        calls = [row["argv"] for row in self.calls()]
        self.assertIn("Storage publication verified 22 cases and 20 continuations\n", result.stdout)
        self.assertEqual(calls[:-3], [
            ["tools/build-libpq.py", "--verify"],
            ["test", "--locked", "--lib", "--", "--list", *FILTERS],
            ["test", "--locked", "--lib", "--bins", "--", *FILTERS],
            ["tools/build-libpq.py", "--verify"], ["build", "--locked"], ["tests/cli_contract.py"],
            ["tools/build-libpq.py", "--verify"], ["tests/test_native_libpq.py"],
            ["tests/support/test_harness.py"], ["tools/build-libpq.py", "--verify"],
            ["test", "--locked", "--test", "storage_format", "--", "--list"],
            ["test", "--locked", "--test", "storage_format", "--", "--include-ignored"],
            ["tools/build-libpq.py", "--verify"],
            ["test", "--locked", "--test", "storage_recovery", "--", "--list"],
            ["test", "--locked", "--test", "storage_recovery", "--", "--include-ignored", "--test-threads=1"],
        ])
        self.assertEqual(calls[-3], ["tools/build-libpq.py", "--verify"])
        self.assertEqual(calls[-2][:-1], ["run", "--locked", "--example", "storage", "--", "--c2-root"])
        self.assertEqual(calls[-1], ["tools/ci/verify.py", "--storage-publication", calls[-2][-1] + "/report.json"])

    def test_publication_uses_a_new_private_root_on_every_run(self):
        roots = []
        for _ in range(2):
            result = self.run_task("test-storage-publication")
            self.passed(result)
            self.assertEqual(result.stdout, "Storage publication verified 22 cases and 20 continuations\n")
            root = Path(self.calls()[-2]["argv"][-1])
            self.assertEqual(root.parent.parent, self.root / "target")
            self.assertEqual(root.parent.stat().st_mode & 0o777, 0o700)
            self.assertTrue((root / "report.json").is_file())
            roots.append(root)
        self.assertNotEqual(*roots)

    def test_publication_missing_or_empty_report_cannot_pass(self):
        for mode in ("missing", "empty"):
            with self.subTest(mode=mode):
                result = self.run_task("test-storage-publication", env={"PUBLICATION_REPORT": mode})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("invalid storage publication receipt", result.stderr)
                self.assertNotIn("Storage publication verified", result.stdout)

    def test_publication_command_failure_cannot_emit_a_receipt(self):
        result = self.run_task("test-storage-publication", env={"FAIL_MATCH": "--c2-root"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.calls()), 2)
        self.assertNotIn("Storage publication verified", result.stdout)

    def test_unit_discovery_and_filters(self):
        self.passed(self.run_task("test-unit"))
        self.assertEqual([row["argv"] for row in self.calls()][1:],
                         [["test", "--locked", "--lib", "--", "--list", *FILTERS],
                          ["test", "--locked", "--lib", "--bins", "--", *FILTERS]])

    def test_empty_discovery_fails_before_execution(self):
        self.assertNotEqual(self.run_task("test-unit", env={"DISCOVERY_COUNT": "0"}).returncode, 0)
        self.assertEqual(len(self.calls()), 2)

    def test_failed_discovery_pipeline_fails_even_with_test_output(self):
        self.assertNotEqual(self.run_task("test-unit", env={"FAIL_MATCH": "--list"}).returncode, 0)
        self.assertEqual(len(self.calls()), 2)

    def test_exact_storage_inventory(self):
        for name, count in (("storage-format", 31), ("storage-recovery", 165)):
            for actual in (0, count - 1, count, count + 1):
                with self.subTest(name=name, actual=actual):
                    result = self.run_task("test-" + name, env={"DISCOVERY_COUNT": str(actual)})
                    self.assertEqual(result.returncode == 0, actual == count, result.stdout + result.stderr)
        self.assertEqual(self.calls()[-3]["argv"],
                         ["test", "--locked", "--test", "storage_recovery", "--", "--include-ignored", "--test-threads=1"])

    def test_live_wrapper_arguments(self):
        cases = {"transport": "pg::live_tests::", "prepared": "pg::prepared_live_tests::",
                 "replication": "pg::replication_live_tests::", "authority": "apply::checkpoint::live_tests::"}
        for profile, namespace in cases.items():
            with self.subTest(profile=profile):
                self.passed(self.run_task("test-" + profile))
                self.assertEqual(self.calls()[-1]["argv"],
                                 ["tools/test-db.py", "--" + profile, "--", "cargo", "test", "--locked",
                                  "--lib", namespace, "--", "--ignored", "--test-threads=1"])

    def test_image_build_argument_passthrough(self):
        helper = self.root / "tools/image/build.py"
        helper.parent.mkdir(parents=True)
        helper.touch()
        args = ["--engine", "podman", "--platform", "linux/arm64", "--output", "/new output/'quoted'"]
        self.passed(self.run_task("image:build", "--", *args))
        self.assertEqual(self.calls()[-1]["argv"], ["tools/image/build.py", *args])
        helper.unlink()
        self.assertNotEqual(self.run_task("image:build", "--", *args).returncode, 0)

    def test_image_smoke_argument_passthrough_and_required_fixture(self):
        helper = self.root / "tools/image/smoke.py"
        helper.parent.mkdir(parents=True)
        shutil.copyfile(ROOT / "tools/image/smoke.py", helper)
        args = ["--engine", "podman", "--receipt", "/receipt path/'quoted'.json",
                "--fixture-image", "sha256:" + "a" * 64, "--output", "/new output"]
        self.passed(self.run_task("image:smoke", "--", *args))
        self.assertEqual(self.calls()[-1]["argv"], ["tools/image/smoke.py", *args])
        result = self.run_task("image:smoke", "PYTHON=" + shlex.quote(sys.executable), "--",
                               "--engine", "podman", "--receipt", "/absent/receipt", "--output", "/absent/output")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("--fixture-image", result.stderr)

    def test_ci_linux_forwards_cli_environment(self):
        helper = self.root / "tools/ci/linux.sh"
        helper.parent.mkdir(parents=True, exist_ok=True)
        helper.write_text("exec python3 ci-linux\n")
        self.passed(self.run_task("ci:linux", "CARGO_TARGET_DIR=/cli target",
                                 env={"CARGO_TARGET_DIR": "/env target"}))
        self.assertEqual(self.calls()[-1]["argv"], ["ci-linux"])
        self.assertEqual(self.calls()[-1]["env"]["CARGO_TARGET_DIR"], "/cli target")

    def test_database_build_engine_copy_order(self):
        self.passed(self.run_task("test-db"))
        calls = [row["argv"] for row in self.calls()]
        self.assertEqual(calls, [["tools/build-libpq.py", "--verify"], ["build", "--locked"],
                                 ["tools/build-libpq.py", "--verify"],
                                 ["tools/test-db.py", "--engine", "--", "cargo", "test", "--locked", "--lib",
                                  "--", "--ignored", "--test-threads=1"],
                                 ["tools/test-db.py", "--", "cargo", "test", "--locked", "--test", "copy_integration",
                                  "--", "--test-threads=1"]])


if __name__ == "__main__":
    unittest.main()
