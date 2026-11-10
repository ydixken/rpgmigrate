"""Portable refusal controls for the separate native ASAN preparation entry point."""

import ast
import copy
from contextlib import ExitStack
import importlib.util
import io
import json
import hashlib
import lzma
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("ffi_asan_prepare.py")
TARGET = "x86_64-unknown-linux-gnu"
CANARY = "PRIVATE_CANARY_password_and_row_contents"


class PreparationTests(unittest.TestCase):
    def setUp(self):
        previous_umask = os.umask(0o077)
        os.umask(previous_umask)
        self.addCleanup(os.umask, previous_umask)
        self.assertTrue(SCRIPT.is_file(), "preparation helper is missing")
        spec = importlib.util.spec_from_file_location("ffi_asan_prepare", SCRIPT)
        self.prep = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.prep)

    def test_private_environment_refuses_overrides_and_cargo_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for key in ("RUSTC", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "LD_LIBRARY_PATH",
                        "LD_PRELOAD", "CFLAGS", "SQLITE3_LIB_DIR", "CARGO_REGISTRIES_CRATES_IO_INDEX",
                        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER", "RUSTUP_DIST_SERVER"):
                with self.subTest(key=key), self.assertRaises(RuntimeError):
                    self.prep.reject_overrides({key: CANARY}, root)
            cargo = root / ".cargo"
            cargo.mkdir()
            (cargo / "config.toml").write_text("[build]\nrustc='foreign'\n")
            with self.assertRaises(RuntimeError):
                self.prep.reject_overrides({}, root)
            (cargo / "config.toml").unlink()
            env = self.prep.private_environment(root)
            self.assertEqual(env["PATH"], "/usr/bin:/bin")
            self.assertEqual(env["RUSTFLAGS"], "-Zsanitizer=address")
            self.assertEqual(env["DEBUGINFOD_URLS"], "")
            self.assertNotIn(CANARY, json.dumps(env))

    def test_run_directory_and_receipt_must_be_fresh_and_private(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run = root / "run"
            self.prep.create_private_run(run, root)
            self.assertEqual(run.stat().st_mode & 0o777, 0o700)
            with self.assertRaises(RuntimeError):
                self.prep.create_private_run(run, root)
            with self.assertRaises(RuntimeError):
                self.prep.create_private_run(root / "../foreign", root)
            path = root / "receipt.json"
            self.prep.write_json(path, {"safe": True})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(RuntimeError):
                self.prep.write_json(path, {"safe": True})

    def test_component_manifest_and_std_lock_are_exact(self):
        pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
        self.assertEqual(pin["external_symbolizer"]["status"], "qualification-required")
        self.assertEqual(self.prep.STD_LOCK, "e3007071f73ee4fddcf817e4a19c1ecea766b590c9a5469980341aa79473914e")
        manifest = {"pkg": {}}
        for name in self.prep.COMPONENTS:
            entry = pin["components"][name]["*" if name == "rust-src" else TARGET]
            manifest["pkg"][name] = {"target": {"*" if name == "rust-src" else TARGET: {
                "available": True, "xz_url": entry["url"], "xz_hash": entry["sha256"]}}}
        self.prep.validate_component_manifest(manifest, pin, TARGET)
        bad = copy.deepcopy(manifest)
        bad["pkg"]["rustc"]["target"][TARGET]["xz_hash"] = "0" * 64
        with self.assertRaises(RuntimeError):
            self.prep.validate_component_manifest(bad, pin, TARGET)
        with self.assertRaises(RuntimeError):
            self.prep.validate_component_manifest(manifest, pin, "foreign")

    def test_archive_extraction_refuses_links_escape_duplicates_and_special_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for names, kind in ((["../escape"], None), (["/absolute"], None),
                                (["ok", "ok"], None), (["link"], tarfile.SYMTYPE),
                                (["fifo"], tarfile.FIFOTYPE)):
                archive = root / "archive.tar"
                with tarfile.open(archive, "w") as bundle:
                    for name in names:
                        item = tarfile.TarInfo(name)
                        item.size = 1 if kind is None else 0
                        if kind is not None:
                            item.type = kind
                            item.linkname = "foreign"
                        bundle.addfile(item, io.BytesIO(b"x") if kind is None else None)
                with self.subTest(names=names), self.assertRaises(RuntimeError):
                    self.prep.extract_component(archive, root / "extract")

    def test_actual_graph_collection_never_compiles_project_or_runs_build_scripts(self):
        calls = []
        class Supervisor:
            def run(self, argv, **kwargs):
                calls.append(argv)
                return subprocess.CompletedProcess(argv, 0, json.dumps({"version": 1, "units": [], "roots": []}), "")
        shape = self.prep.collect_graphs(Supervisor(), Path("/private/bin/cargo"), TARGET)
        self.assertEqual(len(calls), 2)
        self.assertEqual([argv[1:4] for argv in calls], [["build", "--bin", "rpgmigrate"], ["test", "--no-run", "--lib"]])
        for argv in calls:
            for flag in ("--unit-graph", "-Zunstable-options", "-Zbuild-std", "--locked"):
                self.assertIn(flag, argv)
        self.assertEqual(shape["selected_closure"], "not-extracted")
        self.assertEqual(set(shape["graphs"]), {"cli", "libtest"})
        self.assertNotIn("sha256", json.dumps(shape))

    def test_graph_shapes_are_closed_and_never_publish_private_values(self):
        graph = {"version": 1, "units": [{"pkg_id": CANARY, "target": {"src_path": CANARY},
                  "mode": "build", "dependencies": [{"index": 0, "extern_crate_name": CANARY}]}], "roots": [0]}
        shape = self.prep.graph_shape(graph)
        self.assertEqual(shape["units"], 1)
        self.assertNotIn(CANARY, json.dumps(shape))
        for bad in ({}, {"units": CANARY}, {"version": 1, "units": [CANARY], "roots": []},
                    {"version": 1, "units": [], "roots": [], CANARY: CANARY}):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                self.prep.graph_shape(bad)
        with self.assertRaises(RuntimeError):
            self.prep.require_selected_closure(shape)

    def test_package_records_require_authenticated_exact_native_payload(self):
        record = {"Package": "llvm-18", "Version": "1:18.1.3-1ubuntu1", "Architecture": "amd64",
                  "Filename": "pool/universe/l/llvm-toolchain-18/llvm-18_18.1.3-1ubuntu1_amd64.deb",
                  "SHA256": self.prep.DEB_PINS["amd64"], "Size": "123"}
        self.prep.validate_package_record(record, "llvm-18", "1:18.1.3-1ubuntu1", "amd64")
        for key, value in (("SHA256", "bad"), ("Filename", "../../escape"), ("Architecture", "arm64"),
                           ("Version", CANARY), ("Size", "0")):
            bad = {**record, key: value}
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.prep.validate_package_record(bad, "llvm-18", "1:18.1.3-1ubuntu1", "amd64")
        self.assertEqual(self.prep.deb_file_payload(record, "llvm-18", "1:18.1.3-1ubuntu1", "amd64", True), record["SHA256"])
        with self.assertRaises(RuntimeError):
            self.prep.deb_file_payload(record, "llvm-18", "1:18.1.3-1ubuntu1", "amd64", False)

    def test_cgroup_requires_effective_native_limits_and_finite_scratch_admission(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "cell").mkdir()
            for directory in (root, root / "cell"):
                for name, value in (("cpu.max", "200000 100000"), ("memory.max", str(6 * 1024 ** 3)), ("pids.max", "512")):
                    (directory / name).write_text(value)
            self.prep.cgroup_limits("0::/cell\n", root)
            (root / "cell/memory.max").write_text("max")
            self.prep.cgroup_limits("0::/cell\n", root)
            (root / "memory.max").write_text("max")
            with self.assertRaises(RuntimeError):
                self.prep.cgroup_limits("0::/cell\n", root)
            with self.assertRaises(RuntimeError):
                self.prep.cgroup_limits("0::/../../escape\n", root)

    @patch("os.killpg")
    def test_supervisor_clips_deadline_caps_output_and_reaps_owned_process_group(self, killpg):
        killpg.side_effect = ProcessLookupError
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            supervisor = self.prep.Supervisor(root, {"PATH": "/usr/bin:/bin"}, deadline_seconds=2,
                                              capture_limit=1024, total_capture_limit=2048, monitor_scratch=False)
            result = supervisor.run([sys.executable, "-c", "print('safe')"], timeout=30)
            self.assertEqual(result.stdout, "safe\n")
            with self.assertRaisesRegex(RuntimeError, "capture-limit"):
                supervisor.run([sys.executable, "-c", "print('x'*4096)"], timeout=30)
            self.assertTrue(supervisor.owned_children_reaped)
            expired = self.prep.Supervisor(root, {}, deadline_seconds=0, monitor_scratch=False)
            with self.assertRaisesRegex(RuntimeError, "deadline"):
                expired.run([sys.executable, "-c", "pass"], timeout=1)

    @patch("os.killpg")
    def test_supervisor_timeout_retains_no_raw_output_in_failure(self, killpg):
        real_kill = os.kill
        killpg.side_effect = lambda pid, sig: real_kill(pid, sig) if sig else (_ for _ in ()).throw(ProcessLookupError())
        with tempfile.TemporaryDirectory() as temporary:
            supervisor = self.prep.Supervisor(Path(temporary), {}, deadline_seconds=3, monitor_scratch=False)
            with self.assertRaisesRegex(RuntimeError, "child-timeout") as failure:
                supervisor.run([sys.executable, "-c", f"import time; print({CANARY!r},flush=True); time.sleep(3)"], timeout=0.05)
            self.assertNotIn(CANARY, str(failure.exception))
            self.assertTrue(supervisor.owned_children_reaped)

    def test_control_pair_requires_actual_positive_and_unsymbolized_negative(self):
        source = Path("/private/owned/asan-control.rs")
        line = self.prep.asan.control_projection()["source_line"]
        output = ("==1==ERROR: AddressSanitizer: heap-use-after-free on address 0x1234\n"
                  "READ of size 8 at 0x1234 thread T0\n"
                  f"    #0 0x1234 in asan_control::main {source}:{line}:3\n")
        positive = subprocess.CompletedProcess([], -6, "", output)
        negative = subprocess.CompletedProcess([], -6, "", output.replace(f"{source}:{line}:3", "(/private/asan-control+0x1234)"))
        self.assertEqual(self.prep.validate_controls(positive, negative, source)["positive"], self.prep.asan.control_projection())
        for bad in (positive, subprocess.CompletedProcess([], -9, "", negative.stderr), subprocess.CompletedProcess([], 0, "", ""),
                    subprocess.CompletedProcess([], -6, "", "AddressSanitizer:DEADLYSIGNAL")):
            with self.subTest(bad=bad.stderr[:20]), self.assertRaises(RuntimeError):
                self.prep.validate_controls(positive, bad, source)

    def test_receipt_is_separate_closed_and_cannot_claim_qualification(self):
        attempt = self.prep.new_attempt(TARGET)
        self.prep.validate_attempt(attempt)
        self.assertEqual(attempt["kind"], "rpgmigrate-asan-preparation")
        self.assertEqual(attempt["qualification"], "not-assessed")
        for extra in ({"profiles": []}, {"raw_output_sha256": "a" * 64}, {"private": CANARY},
                      {"qualification": "qualified"}, {"status": "passed"}):
            with self.subTest(extra=extra), self.assertRaises(RuntimeError):
                self.prep.validate_attempt({**attempt, **extra})
        self.assertNotIn(CANARY, json.dumps(attempt))
        self.assertEqual(self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))["external_symbolizer"]["status"], "qualification-required")

    def test_wrong_native_elf_and_wrong_target_fail_before_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "binary"
            elf = bytearray(64)
            elf[:6] = b"\x7fELF\x02\x01"
            elf[18:20] = (62).to_bytes(2, "little")
            binary.write_bytes(elf)
            self.prep.native_elf(binary, TARGET)
            for target in ("aarch64-unknown-linux-gnu", "foreign"):
                with self.subTest(target=target), self.assertRaises(RuntimeError):
                    self.prep.native_elf(binary, target)
            binary.write_bytes(b"not ELF")
            with self.assertRaises(RuntimeError):
                self.prep.native_elf(binary, TARGET)

    def test_component_checksum_mismatch_never_reaches_archive_extraction_or_installer(self):
        pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "downloads").mkdir()
            supervisor = SimpleNamespace(run_dir=root)
            def download(_, url, path, checksum, **kwargs):
                if url == pin["manifest_url"]:
                    path.write_text("")
                    return {"sha256": checksum, "bytes": 1}
                raise RuntimeError("toolchain")
            with patch.object(self.prep, "download", side_effect=download), \
                    patch.object(self.prep, "validate_component_manifest"), \
                    patch.object(self.prep, "extract_component") as extract:
                with self.assertRaises(RuntimeError):
                    self.prep.prepare_toolchain(supervisor, pin, TARGET)
                extract.assert_not_called()

    def test_verified_archive_safe_extraction_preserves_only_owned_contents(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = root / "archive.tar"
            with tarfile.open(archive, "w") as bundle:
                item = tarfile.TarInfo("component/install.sh")
                item.size = 4
                item.mode = 0o755
                bundle.addfile(item, io.BytesIO(b"true"))
            self.prep.extract_component(archive, root / "extract")
            install = root / "extract/component/install.sh"
            self.assertEqual(install.read_bytes(), b"true")
            self.assertEqual(install.stat().st_mode & 0o777, 0o700)

    def test_graph_capture_collects_second_actual_shape_after_first_unknown_shape(self):
        calls = []
        class Supervisor:
            def run(self, argv, **kwargs):
                calls.append(argv)
                graph = {CANARY: CANARY} if len(calls) == 1 else {"version": 1, "units": [], "roots": []}
                return subprocess.CompletedProcess(argv, 0, json.dumps(graph), "")
        shapes = self.prep.collect_graphs(Supervisor(), Path("/owned/cargo"), TARGET)
        self.assertEqual(len(calls), 2)
        self.assertEqual(shapes["graphs"]["cli"], {"status": "shape-unrecognized"})
        self.assertNotIn(CANARY, json.dumps(shapes))
        with self.assertRaisesRegex(RuntimeError, "inventory-incomplete"):
            self.prep.require_selected_closure(shapes)

    def test_authenticated_package_payload_comparison_refuses_changed_installed_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = root / "usr/bin/true"
            payload.parent.mkdir(parents=True)
            payload.write_bytes(Path("/usr/bin/true").read_bytes())
            packages = self.prep.UbuntuPackages.__new__(self.prep.UbuntuPackages)
            packages.installed = lambda _: ["test-package", "1", "amd64"]
            packages.archive = lambda *args: {"payload": root}
            self.assertTrue(packages.compare(Path("/usr/bin/true"))["authenticated_payload_equal"])
            payload.write_bytes(b"changed")
            with self.assertRaisesRegex(RuntimeError, "package-payload"):
                packages.compare(Path("/usr/bin/true"))

    def test_unresolved_symbolizer_dependency_never_executes_candidate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            candidate = root / "llvm-symbolizer"
            candidate.write_text("not executable")
            calls = []
            class Supervisor:
                def run(self, argv, **kwargs):
                    calls.append(argv)
                    output = "" if "ldconfig" in argv[0] else "  (NEEDED) Shared library: [missing.so]"
                    return subprocess.CompletedProcess(argv, 0, output, "")
            package = SimpleNamespace(installed=lambda _: ["llvm-18", "1:18.1.3-1ubuntu1", "amd64"],
                                      compare=lambda *args, **kwargs: {"path": str(candidate), "sha256": "a" * 64})
            with patch.object(self.prep, "SYMBOLIZER", candidate), patch.object(self.prep, "native_elf"), \
                    patch.object(self.prep.asan, "package", return_value={"package": "llvm-18", "package_version": "1:18.1.3-1ubuntu1", "architecture": "amd64"}), \
                    patch.object(self.prep, "UbuntuPackages", return_value=package):
                with self.assertRaisesRegex(RuntimeError, "symbolizer"):
                    self.prep.authenticate_symbolizer(Supervisor(), TARGET)
            self.assertFalse(any(argv[0] == str(candidate) for argv in calls))

    def test_source_binding_refuses_empty_missing_and_changed_source_inventories(self):
        class Supervisor:
            def __init__(self, names): self.names = names
            def run(self, argv):
                output = self.names if "ls-files" in argv else ("" if "status" in argv else "a" * 40)
                return subprocess.CompletedProcess(argv, 0, output, "")
        for names in ("", "missing.py\0", "../escape\0", CANARY + "/file\0"):
            with self.subTest(names=names), self.assertRaises(RuntimeError):
                self.prep.source_binding(Supervisor(names))

    def test_only_frozen_helper_arguments_are_accepted(self):
        for arguments in (("--target", "foreign"), ("--command", "true"), ("--manifest", "/foreign"),
                          ("--symbolizer", "/foreign"), ("--url", "https://foreign.invalid")):
            result = subprocess.run([sys.executable, "-B", str(SCRIPT), *arguments], capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2)
        tree = ast.parse(SCRIPT.read_text())
        self.assertFalse(any(isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and
                             isinstance(node.func.value, ast.Name) and node.func.value.id == "asan" and
                             node.func.attr in ("execute", "publish_receipt") for node in ast.walk(tree)))

    def test_private_growth_and_free_floor_breaches_stop_before_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            supervisor = self.prep.Supervisor(root, {})
            with patch.object(self.prep.shutil, "disk_usage", return_value=SimpleNamespace(free=1)), \
                    patch.object(self.prep.subprocess, "Popen") as launch:
                with self.assertRaisesRegex(RuntimeError, "scratch-limit"):
                    supervisor.run(["/bin/true"])
                launch.assert_not_called()

    def test_supervisor_kills_only_exact_owned_group_and_refuses_unreaped_group(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        supervisor = self.prep.Supervisor(Path(temporary.name), {}, monitor_scratch=False)
        process = SimpleNamespace(pid=123456789, wait=lambda **kwargs: 0, poll=lambda: 0)
        with patch.object(self.prep.os, "killpg") as kill, patch.object(self.prep.asan, "group_exists", return_value=False):
            supervisor.reap(process)
            kill.assert_called_once_with(process.pid, self.prep.signal.SIGTERM)
        supervisor.deadline = self.prep.time.monotonic() - 1
        with patch.object(self.prep.os, "killpg") as kill, patch.object(self.prep.asan, "group_exists", return_value=True):
            with self.assertRaisesRegex(RuntimeError, "owned-group"):
                supervisor.reap(process)
            self.assertEqual([call.args[0] for call in kill.call_args_list], [process.pid, process.pid])

    def test_incomplete_inventory_continues_independent_control_and_keeps_failed_attempt(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public = root / "public"
            public.mkdir(mode=0o700)
            args = SimpleNamespace(target=TARGET, run_dir=root / "private", receipt=public / "attempt.json")
            def prepare(supervisor, pin, target, report, notices):
                (supervisor.run_dir / "prefix/bin").mkdir()
                for name in ("rustc", "cargo"):
                    binary = supervisor.run_dir / "prefix/bin" / name
                    binary.write_text("fixture")
                    report[name + "_sha256"] = self.prep.sha256(binary)
                return report
            source = {"commit": "a" * 40, "tree": "b" * 40, "files": [{"source": "Cargo.toml", "sha256": "c" * 64}]}
            graph = {"graphs": {"cli": {"units": 1}, "libtest": {"units": 1}}, "selected_closure": "not-extracted"}
            def admission(*arguments):
                arguments[-1]["admission_substage"] = "complete"
                return {"machine": "x86_64"}
            with patch.dict(os.environ, {"RUNNER_TEMP": str(root)}, clear=True), \
                    patch.object(self.prep, "reject_overrides"), patch.object(self.prep, "cgroup_limits", return_value={}), \
                    patch.object(self.prep.Path, "read_text", autospec=True, side_effect=lambda path, *a, **k: "0::/cell\n" if str(path) == "/proc/self/cgroup" else Path.read_bytes(path).decode()), \
                    patch.object(self.prep, "admit", side_effect=admission), \
                    patch.object(self.prep, "source_binding", return_value=source), \
                    patch.object(self.prep.resource, "setrlimit"), \
                    patch.object(self.prep, "prepare_toolchain", side_effect=prepare), \
                    patch.object(self.prep, "collect_graphs", return_value=graph), \
                    patch.object(self.prep, "authenticate_symbolizer", return_value=({"files": []}, [])), \
                    patch.object(self.prep, "run_controls", return_value={"positive": self.prep.asan.control_projection()}) as control:
                self.assertEqual(self.prep.execute(args), 1)
                control.assert_called_once()
            attempt = json.loads(args.receipt.read_text())
            self.prep.validate_attempt(attempt)
            self.assertEqual(attempt["stages"]["graphs"]["status"], "incomplete")
            self.assertEqual(attempt["stages"]["control"]["status"], "passed")
            self.assertEqual(attempt["first_failure"], {"stage": "graphs", "reason": "inventory-incomplete"})
            self.assertTrue(attempt["public_pin_unchanged"])
            self.assertEqual(attempt["admission_substage"], "complete")
            for path in public.iterdir():
                self.assertNotIn(CANARY, path.read_text())
                self.assertNotIn(str(root), path.read_text())

    def test_missing_component_license_or_installer_blocks_official_script_execution(self):
        pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
        for missing in ("install.sh", "LICENSE-APACHE"):
            with self.subTest(missing=missing), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                for name in ("downloads", "components", "prefix"):
                    (root / name).mkdir()
                supervisor = SimpleNamespace(run_dir=root, run=lambda *a, **k: self.fail("installer must not execute"),
                                             remaining=lambda: 60, check_scratch=lambda: None)
                def download(_, url, path, checksum, **kwargs):
                    path.write_text("")
                    return {"sha256": checksum, "bytes": 1}
                def extract(archive, path, supervisor):
                    directory = path / "distribution"
                    directory.mkdir(parents=True)
                    for name in ("install.sh", "LICENSE-APACHE"):
                        if name != missing:
                            (directory / name).write_text("fixture")
                    return path
                with patch.object(self.prep, "download", side_effect=download), \
                        patch.object(self.prep, "validate_component_manifest"), \
                        patch.object(self.prep, "extract_component", side_effect=extract):
                    with self.assertRaises(RuntimeError):
                        self.prep.prepare_toolchain(supervisor, pin, TARGET)

    def test_admission_failure_publishes_sanitized_attempt_without_child_or_canary(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public = root / "public"
            public.mkdir(mode=0o700)
            args = SimpleNamespace(target=TARGET, run_dir=root / "private", receipt=public / "attempt.json")
            with patch.dict(os.environ, {"RUNNER_TEMP": str(root), "RUSTC": CANARY}, clear=True), \
                    patch.object(self.prep.resource, "setrlimit"), \
                    patch.object(self.prep.subprocess, "Popen") as launch:
                self.assertEqual(self.prep.execute(args), 1)
                launch.assert_not_called()
            attempt = json.loads(args.receipt.read_text())
            self.assertEqual(attempt["first_failure"], {"stage": "admission", "reason": "configuration"})
            self.assertEqual(attempt["stages"]["toolchain"]["status"], "not-run")
            self.assertIsNone(attempt["admission_substage"])
            self.assertNotIn(CANARY, "".join(path.read_text() for path in public.iterdir()))

    def test_receipt_rejects_inconsistent_stage_reason_and_first_failure(self):
        attempt = self.prep.new_attempt(TARGET)
        for name, value in (("control", {"status": "passed", "reason": "control"}),
                            ("graphs", {"status": "incomplete", "reason": "none"}),
                            ("toolchain", {"status": "incomplete", "reason": "inventory-incomplete"})):
            bad = copy.deepcopy(attempt)
            bad["stages"][name] = value
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                self.prep.validate_attempt(bad)
        bad = copy.deepcopy(attempt)
        bad["first_failure"] = {"stage": "control", "reason": "control"}
        with self.assertRaises(RuntimeError):
            self.prep.validate_attempt(bad)

    def test_reaping_failure_blocks_every_later_child_and_cannot_be_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            supervisor = self.prep.Supervisor(Path(temporary), {}, monitor_scratch=False)
            with patch.object(self.prep.asan, "group_exists", return_value=True), \
                    patch.object(self.prep.os, "killpg", side_effect=PermissionError):
                with self.assertRaises(PermissionError):
                    supervisor.run([sys.executable, "-c", "pass"])
            self.assertTrue(supervisor.poisoned)
            self.assertFalse(supervisor.owned_children_reaped)
            with patch.object(self.prep.subprocess, "Popen") as launch:
                with self.assertRaisesRegex(RuntimeError, "owned-group"):
                    supervisor.run([sys.executable, "-c", "pass"])
                launch.assert_not_called()

    def test_signed_release_index_and_exact_package_records_preserve_input_chain(self):
        record = ("Package: llvm-18\nVersion: 1:18.1.3-1ubuntu1\nArchitecture: amd64\n"
                  "Filename: pool/universe/l/llvm-toolchain-18/llvm-18_18.1.3-1ubuntu1_amd64.deb\n"
                  f"SHA256: {self.prep.DEB_PINS['amd64']}\nSize: 123\n\n").encode()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            index = root / "Packages.xz"
            index.write_bytes(lzma.compress(record))
            release = root / "InRelease"
            compressed_hash = self.prep.sha256(index)
            source_hash = hashlib.sha256(record).hexdigest()
            release.write_text("-----BEGIN PGP SIGNED MESSAGE-----\nHash: SHA512\n\nSHA256:\n"
                               f" {source_hash} {len(record)} universe/binary-amd64/Packages\n"
                               f" {compressed_hash} {index.stat().st_size} universe/binary-amd64/Packages.xz\n"
                               "-----BEGIN PGP SIGNATURE-----\nfixture\n")
            checksums = self.prep.release_checksums(release)
            supervisor = SimpleNamespace(remaining=lambda: 60)
            records = self.prep.package_index_records(index, *checksums["universe/binary-amd64/Packages"], supervisor)
            self.assertEqual(len(records), 1)
            self.prep.validate_package_record(records[0], "llvm-18", "1:18.1.3-1ubuntu1", "amd64")
            for checksum, size in (("0" * 64, len(record)), (source_hash, len(record) - 1)):
                with self.assertRaises(RuntimeError):
                    self.prep.package_index_records(index, checksum, size, supervisor)
            index.write_bytes(lzma.compress(record.replace(b"Size: 123\n", b"")))
            with self.assertRaises(RuntimeError):
                self.prep.package_index_records(index, source_hash, len(record), supervisor)

    def public_fixture(self, root):
        (root / "apt/public-indexes").mkdir(parents=True)
        source = root / "apt/public-indexes/noble-universe.packages.xz"
        source.write_bytes(lzma.compress(b"public source bytes\n"))
        supervisor = SimpleNamespace(run_dir=root, remaining=lambda: 60, check_scratch=lambda: None)
        public = self.prep.PublicProvenance(supervisor, "amd64")
        origin = "https://archive.ubuntu.com/ubuntu/dists/noble/universe/binary-amd64/Packages.xz"
        return public, source, origin

    def test_public_bundle_keeps_exact_original_bytes_and_only_allowlisted_names(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public, source, origin = self.public_fixture(root)
            public.add("package-index", source, origin, self.prep.sha256(source))
            output = root / "output"
            output.mkdir()
            public.publish(output)
            manifest = json.loads((output / "public-provenance.json").read_text())
            archive = output / "public-provenance.tar"
            self.prep.validate_public_bundle(manifest, archive)
            with tarfile.open(archive) as bundle:
                self.assertEqual(bundle.getnames(), [manifest["files"][0]["filename"]])
                self.assertEqual(bundle.extractfile(bundle.getmembers()[0]).read(), source.read_bytes())
            self.assertNotIn(str(root), json.dumps(manifest))
            self.assertNotIn(CANARY, json.dumps(manifest))
            self.assertNotIn(CANARY.encode(), archive.read_bytes())

    def test_public_bundle_refuses_private_capture_unknown_role_symlink_and_over_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public, source, origin = self.public_fixture(root)
            capture = root / "captures/stdout"
            capture.parent.mkdir()
            capture.write_text(CANARY)
            for role, path, url in (("package-index", capture, origin), ("stdout", source, origin),
                                    ("package-index", source, "https://private.invalid/" + CANARY)):
                with self.subTest(role=role), self.assertRaises(RuntimeError):
                    public.add(role, path, url, self.prep.sha256(path))
            link = source.with_name("symlink.packages.xz")
            link.symlink_to(source)
            with self.assertRaises(RuntimeError):
                public.add("package-index", link, origin, self.prep.sha256(source))
            with patch.object(self.prep, "PUBLIC_LIMIT", 1):
                with self.assertRaises(RuntimeError):
                    public.add("package-index", source, origin, self.prep.sha256(source))
            self.assertEqual(public.entries, [])
            self.assertFalse(any(public.directory.iterdir()))

    def test_public_bundle_validation_refuses_duplicate_escape_link_unknown_and_changed_members(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            public, source, origin = self.public_fixture(root)
            public.add("package-index", source, origin, self.prep.sha256(source))
            output = root / "output"
            output.mkdir()
            public.publish(output)
            manifest = json.loads((output / "public-provenance.json").read_text())
            name = manifest["files"][0]["filename"]
            archive = output / "test.tar"
            for names, kind, content in (([name, name], tarfile.REGTYPE, source.read_bytes()),
                                         (["../escape"], tarfile.REGTYPE, source.read_bytes()),
                                         ([name], tarfile.SYMTYPE, b""),
                                         (["unknown"], tarfile.REGTYPE, source.read_bytes()),
                                         ([name], tarfile.REGTYPE, b"changed")):
                with tarfile.open(archive, "w") as bundle:
                    for filename in names:
                        item = tarfile.TarInfo(filename)
                        item.type, item.mode, item.size = kind, 0o600, len(content)
                        item.linkname = "private" if kind == tarfile.SYMTYPE else ""
                        bundle.addfile(item, io.BytesIO(content))
                changed = {**manifest, "bundle_sha256": self.prep.sha256(archive)}
                with self.subTest(names=names, kind=kind), self.assertRaises(RuntimeError):
                    self.prep.validate_public_bundle(changed, archive)
            with tarfile.open(archive, "w") as bundle:
                item = tarfile.TarInfo(name)
                item.mode, item.size, item.uname = 0o600, source.stat().st_size, CANARY
                bundle.addfile(item, io.BytesIO(source.read_bytes()))
            with self.assertRaises(RuntimeError):
                self.prep.validate_public_bundle({**manifest, "bundle_sha256": self.prep.sha256(archive)}, archive)
            archive.write_bytes((output / "public-provenance.tar").read_bytes() + CANARY.encode())
            with self.assertRaises(RuntimeError):
                self.prep.validate_public_bundle({**manifest, "bundle_sha256": self.prep.sha256(archive)}, archive)
            for change in ({"role": "stdout"}, {"filename": "../private"}, {"public_origin": CANARY},
                           {"bytes": True}, {"private_path": CANARY}):
                changed = {**manifest["files"][0], **change}
                with self.subTest(change=change), self.assertRaises(RuntimeError):
                    self.prep.validate_public_entry(changed, "amd64")

    def test_signature_failure_stops_before_index_download_or_candidate_execution(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "apt").mkdir()
            calls = []
            class Supervisor:
                run_dir = root
                environment = {}
                def run(self, argv, **kwargs):
                    calls.append(argv)
                    if argv[0] == "/usr/bin/gpgv":
                        raise RuntimeError("package-authentication")
                    lists = root / "apt/state/lists"
                    (lists / "archive.ubuntu.com_ubuntu_dists_noble_InRelease").write_text("fixture")
                    (lists / "archive.ubuntu.com_ubuntu_dists_noble_universe_binary-amd64_Packages.lz4").write_text("fixture")
                    return subprocess.CompletedProcess(argv, 0, "", "")
            real_is_file = Path.is_file
            with patch.object(self.prep.Path, "is_file", autospec=True, side_effect=lambda path: True if str(path).endswith("ubuntu-archive-keyring.gpg") else real_is_file(path)), \
                    patch.object(self.prep, "sha256", return_value="a" * 64), \
                    patch.object(self.prep, "PublicProvenance", return_value=SimpleNamespace(add=lambda *args: None)), \
                    patch.object(self.prep, "download") as download:
                with self.assertRaisesRegex(RuntimeError, "package-authentication"):
                    self.prep.UbuntuPackages(Supervisor(), "amd64")
                download.assert_not_called()
            self.assertEqual([call[0] for call in calls], ["/usr/bin/apt-get", "/usr/bin/gpgv"])

    def test_native_admission_requires_measured_memory_disk_and_matching_runner(self):
        pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
        supervisor = SimpleNamespace(run_dir=Path("/private/run"), run=lambda argv: subprocess.CompletedProcess(argv, 0, "amd64\n", ""))
        parent = {"ImageOS": "ubuntu24", "ImageVersion": "20261101.360.1"}
        for memory_kib, free, admitted in ((7 * 1024 ** 2, 13 * self.prep.GIB, True),
                                           (5 * 1024 ** 2, 13 * self.prep.GIB, False),
                                           (7 * 1024 ** 2, 11 * self.prep.GIB, False)):
            def text(path, *args, **kwargs):
                return 'ID=ubuntu\nVERSION_ID="24.04"\n' if str(path) == "/etc/os-release" else f"MemAvailable: {memory_kib} kB\n"
            with patch.object(self.prep.asan, "native_target"), patch.object(self.prep.os, "getuid", return_value=1000), \
                    patch.object(self.prep, "check_native_tools"), \
                    patch.object(self.prep, "inspect_binfmt", return_value={"inspection": "complete-snapshot"}), \
                    patch.object(self.prep.Path, "read_text", autospec=True, side_effect=text), \
                    patch.object(self.prep.Path, "is_dir", return_value=False), \
                    patch.object(self.prep.shutil, "disk_usage", return_value=SimpleNamespace(free=free)), \
                    patch.object(self.prep.resource, "getrlimit", return_value=(0, 0)):
                if admitted:
                    attempt = self.prep.new_attempt(TARGET)
                    value = self.prep.admit(supervisor, TARGET, parent, pin, attempt)
                    self.assertEqual(attempt["admission_substage"], "complete")
                    self.assertEqual(value["host_available_memory_bytes"], memory_kib * 1024)
                    self.assertEqual(value["scratch"]["enforcement"], "bounded-monitor-not-hard-quota")
                else:
                    with self.assertRaises(RuntimeError):
                        self.prep.admit(supervisor, TARGET, parent, pin, self.prep.new_attempt(TARGET))

    def test_first_graph_child_failure_preserves_failure_and_collects_independent_libtest_shape(self):
        calls = []
        class Supervisor:
            def run(self, argv, **kwargs):
                calls.append(argv)
                if len(calls) == 1:
                    raise RuntimeError("child-failed")
                return subprocess.CompletedProcess(argv, 0, json.dumps({"version": 1, "units": [], "roots": []}), "")
        shapes = self.prep.collect_graphs(Supervisor(), Path("/owned/cargo"), TARGET)
        self.assertEqual(len(calls), 2)
        self.assertEqual(shapes["graphs"]["cli"], {"status": "failed", "reason": "child-failed"})
        self.assertEqual(shapes["graphs"]["libtest"]["units"], 0)
        with self.assertRaisesRegex(RuntimeError, "graphs"):
            self.prep.require_selected_closure(shapes)

    def admission_attempt(self, refusal):
        pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
        if refusal == "public-pin":
            pin["external_symbolizer"]["status"] = "qualified"
        real_read = Path.read_text
        real_is_dir, real_iterdir = Path.is_dir, Path.iterdir
        real_sha, real_admit = self.prep.sha256, self.prep.admit
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            root = Path(temporary)
            public = root / "public"
            public.mkdir(mode=0o700)
            args = SimpleNamespace(target=TARGET, run_dir=root / "private", receipt=public / "attempt.json")
            parent = {"RUNNER_TEMP": str(root), "ImageOS": "ubuntu24", "ImageVersion": "20261101.360.1"}
            if refusal == "runner-image":
                parent["ImageVersion"] = CANARY

            def read(path, *args, **kwargs):
                name = str(path)
                if refusal == "os-release-exception" and name == "/etc/os-release":
                    raise OSError(CANARY)
                if refusal == "memory-exception" and name == "/proc/meminfo":
                    raise OSError(CANARY)
                values = {"/proc/self/cgroup": "0::/cell\n", "/etc/os-release": 'ID=ubuntu\nVERSION_ID="24.04"\n',
                          "/proc/meminfo": f"MemAvailable: {7 * 1024 ** 2} kB\n"}
                if refusal == "os-release":
                    values["/etc/os-release"] = CANARY
                if refusal == "available-memory":
                    values["/proc/meminfo"] = f"MemAvailable: {5 * 1024 ** 2} kB\n"
                return values[name] if name in values else real_read(path, *args, **kwargs)

            def is_dir(path):
                if str(path) == "/proc/sys/fs/binfmt_misc":
                    if refusal == "binfmt-exception":
                        raise OSError(CANARY)
                    return refusal == "binfmt"
                return real_is_dir(path)

            def children(path):
                if str(path) == "/proc/sys/fs/binfmt_misc":
                    return iter([Path("/proc/sys/fs/binfmt_misc/qemu-foreign")])
                return real_iterdir(path)

            def digest(path):
                if Path(path) == self.prep.ROOT / "Cargo.lock":
                    if refusal == "lock-exception":
                        raise OSError(CANARY)
                    if refusal == "project-lock":
                        return "0" * 64
                return real_sha(path)

            def native(*args):
                if refusal == "native-exception":
                    raise RuntimeError(CANARY)

            def command(argv, **kwargs):
                if refusal == "dpkg-exception":
                    raise RuntimeError(CANARY)
                self.assertEqual(argv, ["/usr/bin/dpkg", "--print-architecture"])
                return subprocess.CompletedProcess(argv, 0, "arm64\n" if refusal == "dpkg-architecture" else "amd64\n", "")

            def admission(*args):
                with patch.object(self.prep.os, "getuid", return_value=0 if refusal == "uid" else 1000):
                    return real_admit(*args)

            def core(_):
                if refusal == "core-exception":
                    raise OSError(CANARY)
                return (1, 1) if refusal == "core-limit" else (0, 0)

            stack.enter_context(patch.dict(os.environ, parent, clear=True))
            stack.enter_context(patch.object(self.prep.resource, "setrlimit"))
            stack.enter_context(patch.object(self.prep.resource, "getrlimit", side_effect=core))
            stack.enter_context(patch.object(self.prep, "reject_overrides"))
            stack.enter_context(patch.object(self.prep.asan, "load_pin", return_value=pin))
            stack.enter_context(patch.object(self.prep.asan, "native_target", side_effect=native))
            stack.enter_context(patch.object(self.prep, "check_native_tools"))
            def binfmt():
                if refusal == "binfmt":
                    raise RuntimeError("binfmt-possible-elf-handler")
                if refusal == "binfmt-exception":
                    raise RuntimeError("binfmt-lookup-failed")
                if refusal == "binfmt-unknown":
                    raise RuntimeError(CANARY)
                return {"inspection": "complete-snapshot"}
            stack.enter_context(patch.object(self.prep, "inspect_binfmt", side_effect=binfmt))
            stack.enter_context(patch.object(self.prep, "admit", side_effect=admission))
            stack.enter_context(patch.object(self.prep, "sha256", side_effect=digest))
            stack.enter_context(patch.object(self.prep.Path, "read_text", autospec=True, side_effect=read))
            stack.enter_context(patch.object(self.prep.Path, "is_dir", autospec=True, side_effect=is_dir))
            stack.enter_context(patch.object(self.prep.Path, "iterdir", autospec=True, side_effect=children))
            stack.enter_context(patch.object(self.prep, "cgroup_limits", return_value={"ancestor_limits_checked": True}))
            stack.enter_context(patch.object(self.prep.Supervisor, "run", side_effect=command))
            stack.enter_context(patch.object(self.prep.shutil, "disk_usage", return_value=SimpleNamespace(
                free=(11 if refusal == "scratch-disk" else 13) * self.prep.GIB)))
            source = stack.enter_context(patch.object(self.prep, "source_binding"))
            toolchain = stack.enter_context(patch.object(self.prep, "prepare_toolchain"))
            child = stack.enter_context(patch.object(self.prep.subprocess, "Popen"))
            self.assertEqual(self.prep.execute(args), 1)
            source.assert_not_called()
            toolchain.assert_not_called()
            child.assert_not_called()
            attempt = json.loads(real_read(args.receipt))
            retained = "".join(real_read(path) for path in real_iterdir(public))
            self.assertNotIn(CANARY, retained)
            self.assertNotIn(str(root), retained)
            self.assertFalse(attempt["public_pin_unchanged"])
            self.assertEqual(attempt["first_failure"], {"stage": "admission", "reason": "admission"})
            self.assertTrue(all(attempt["stages"][stage]["status"] == "not-run" for stage in self.prep.STAGES if stage != "admission"))
            return attempt

    def test_nine_admission_refusals_identify_closed_substage_without_changing_failure(self):
        for refusal in ("uid", "dpkg-architecture", "os-release", "binfmt", "project-lock", "runner-image",
                        "scratch-disk", "available-memory", "core-limit"):
            with self.subTest(refusal=refusal):
                attempt = self.admission_attempt(refusal)
                self.assertEqual(attempt.get("admission_substage"), refusal)

    def test_admission_exceptions_identify_native_file_and_resource_operation(self):
        for refusal, expected in (("native-exception", "native-architecture"), ("dpkg-exception", "dpkg-architecture"),
                                  ("os-release-exception", "os-release"), ("binfmt-exception", "binfmt"),
                                  ("lock-exception", "project-lock"), ("memory-exception", "available-memory"),
                                  ("core-exception", "core-limit"), ("public-pin", "public-pin")):
            with self.subTest(refusal=refusal):
                self.assertEqual(self.admission_attempt(refusal).get("admission_substage"), expected)

    def test_attempt_schema_three_requires_nullable_closed_admission_operation_details(self):
        self.assertEqual(self.prep.ADMISSION_SUBSTAGES,
                         ("uid", "native-architecture", "dpkg-architecture", "os-release", "binfmt", "project-lock",
                          "public-pin", "runner-image", "scratch-disk", "available-memory", "core-limit", "complete"))
        attempt = self.prep.new_attempt(TARGET)
        self.assertEqual(attempt["schema_version"], 3)
        self.assertIn("admission_substage", attempt)
        self.assertIsNone(attempt["admission_substage"])
        self.assertIsNone(attempt["admission_detail"])
        self.assertEqual(self.prep.BINFMT_DETAILS, ("binfmt-lookup-failed", "binfmt-enumeration-failed", "binfmt-entry-read-failed",
                         "binfmt-entry-invalid", "binfmt-budget-exceeded", "binfmt-possible-elf-handler"))
        self.prep.validate_attempt(attempt)
        for change in ({"admission_substage": CANARY}, {"admission_substage": 1},
                       {"admission_substage": []}, {"schema_version": 1}, {"schema_version": 2}, {"admission_substage": "uid"},
                       {"admission_detail": CANARY}, {"admission_detail": []}, {"admission_detail": "binfmt-entry-invalid"}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                self.prep.validate_attempt({**attempt, **change})
        for missing in ("admission_substage", "admission_detail"):
            changed = dict(attempt)
            del changed[missing]
            with self.assertRaises(RuntimeError):
                self.prep.validate_attempt(changed)

    def binfmt_record(self, *, magic="cb0d0d0a", mask=None, state="enabled", flags="", offset=0):
        text = f"{state}\ninterpreter /{CANARY}/python\nflags: {flags}\noffset {offset}\nmagic {magic}\n"
        return (text + (f"mask {mask}\n" if mask is not None else "")).encode()

    def binfmt_directory(self, root, records=()):
        root.mkdir()
        (root / "register").write_bytes(b"")
        (root / "status").write_bytes(b"enabled\n")
        for index, record in enumerate(records):
            (root / f"{CANARY}-{index}").write_bytes(record)
        return root

    def test_disjoint_synthetic_registration_admits_both_native_targets(self):
        pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
        real_dir, real_iter = Path.is_dir, Path.iterdir
        with tempfile.TemporaryDirectory() as temporary:
            registry = self.binfmt_directory(Path(temporary) / "registry", [self.binfmt_record()])
            for target, (_, _, architecture) in self.prep.asan.TARGETS.items():
                def is_dir(path):
                    return True if str(path) == "/proc/sys/fs/binfmt_misc" else real_dir(path)
                def iterdir(path):
                    return real_iter(registry) if str(path) == "/proc/sys/fs/binfmt_misc" else real_iter(path)
                def text(path, *args, **kwargs):
                    return 'ID=ubuntu\nVERSION_ID="24.04"\n' if str(path) == "/etc/os-release" else f"MemAvailable: {7 * 1024 ** 2} kB\n"
                events = []
                real_inspect = self.prep.inspect_binfmt
                def inspect():
                    value = real_inspect()
                    events.append("binfmt-complete")
                    return value
                def command(argv):
                    self.assertEqual(events, ["native-elf", "binfmt-complete"])
                    return subprocess.CompletedProcess(argv, 0, architecture + "\n", "")
                supervisor = SimpleNamespace(run_dir=Path(temporary), run=command)
                attempt = self.prep.new_attempt(target)
                with patch.object(self.prep, "BINFMT_ROOT", registry, create=True), \
                        patch.object(self.prep, "check_native_tools", side_effect=lambda target: events.append("native-elf")), \
                        patch.object(self.prep, "inspect_binfmt", side_effect=inspect), \
                        patch.object(self.prep.asan, "native_target"), patch.object(self.prep.os, "getuid", return_value=1000), \
                        patch.object(self.prep.Path, "is_dir", autospec=True, side_effect=is_dir), \
                        patch.object(self.prep.Path, "iterdir", autospec=True, side_effect=iterdir), \
                        patch.object(self.prep.Path, "read_text", autospec=True, side_effect=text), \
                        patch.object(self.prep.shutil, "disk_usage", return_value=SimpleNamespace(free=13 * self.prep.GIB)), \
                        patch.object(self.prep.resource, "getrlimit", return_value=(0, 0)):
                    try:
                        value = self.prep.admit(supervisor, target, {"ImageOS": "ubuntu24", "ImageVersion": "20261101.360.1"}, pin, attempt)
                    except RuntimeError:
                        value = None
                self.assertIsNotNone(value, "a strict offset-zero pyc-like magic cannot match either native ELF")
                self.assertEqual(value["binfmt"], {"inspection": "complete-snapshot", "handlers_examined": 1,
                    "elf_disjoint_magic_handlers": 1, "enabled_handlers": 1, "disabled_handlers": 0, "registry_state": "enabled"})
                self.assertNotIn(CANARY, json.dumps(value))

    def test_binfmt_parser_requires_first_four_byte_masked_elf_disjointness(self):
        for record in (self.binfmt_record(), self.binfmt_record(mask="ffffffff"),
                       self.binfmt_record(magic="80", mask="80", state="disabled", flags="POCF")):
            self.assertIn(self.prep.parse_binfmt_record(record), ("enabled", "disabled"))
        for machine in (62, 183):
            header = bytearray(20)
            header[:6] = b"\x7fELF\x02\x01"
            header[18:20] = machine.to_bytes(2, "little")
            self.assertTrue(any((header[index] ^ value) & 255 for index, value in enumerate(bytes.fromhex("cb0d0d0a"))))
        for record in (self.binfmt_record(magic="7f454c46"), self.binfmt_record(mask="00000000"),
                       self.binfmt_record(magic="7f454c4601"), self.binfmt_record(magic="7f454c4601", state="disabled")):
            with self.subTest(record=record[:8]), self.assertRaisesRegex(RuntimeError, "binfmt-possible-elf-handler"):
                self.prep.parse_binfmt_record(record)

    def test_binfmt_parser_refuses_extensions_offsets_unknown_flags_and_malformed_fields(self):
        invalid = [b"enabled\ninterpreter /python\nflags: \nextension .pyc\n", self.binfmt_record(offset=1),
                   self.binfmt_record(flags="Q"), self.binfmt_record(flags="PP"), self.binfmt_record(flags="OP"),
                   self.binfmt_record(mask="ff"), self.binfmt_record(magic="abc"), self.binfmt_record(magic=""),
                   self.binfmt_record().replace(b"enabled", b"unknown"), self.binfmt_record() + b"unknown value\n",
                   self.binfmt_record().replace(b"interpreter /", b"interpreter relative/")]
        for record in invalid:
            with self.subTest(record=record[:16]), self.assertRaisesRegex(RuntimeError, "binfmt-entry-invalid"):
                self.prep.parse_binfmt_record(record)

    def test_binfmt_snapshot_refuses_missing_unreadable_incomplete_and_over_budget_inventory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(RuntimeError, "binfmt-lookup-failed"):
                self.prep.inspect_binfmt(root / "missing")
            registry = self.binfmt_directory(root / "registry", [self.binfmt_record(state="disabled")])
            summary = self.prep.inspect_binfmt(registry)
            self.assertEqual(summary["disabled_handlers"], 1)
            self.assertEqual(summary["inspection"], "complete-snapshot")
            self.assertNotIn(CANARY, json.dumps(summary))
            (registry / "register").rename(registry / "missing-control")
            with self.assertRaisesRegex(RuntimeError, "binfmt-lookup-failed"):
                self.prep.inspect_binfmt(registry)
            (registry / "missing-control").rename(registry / "register")
            real_iter = Path.iterdir
            with patch.object(self.prep.Path, "iterdir", autospec=True, side_effect=PermissionError(CANARY)):
                with self.assertRaisesRegex(RuntimeError, "binfmt-enumeration-failed") as error:
                    self.prep.inspect_binfmt(registry)
                self.assertNotIn(CANARY, str(error.exception))
            with patch.object(self.prep.os, "open", side_effect=PermissionError(CANARY)):
                with self.assertRaisesRegex(RuntimeError, "binfmt-lookup-failed"):
                    self.prep.inspect_binfmt(registry)
            handler = next(path for path in real_iter(registry) if path.name not in ("register", "status"))
            real_open = os.open
            def open_file(path, *args, **kwargs):
                if Path(path) == handler:
                    raise FileNotFoundError(CANARY)
                return real_open(path, *args, **kwargs)
            with patch.object(self.prep.os, "open", side_effect=open_file):
                with self.assertRaisesRegex(RuntimeError, "binfmt-entry-read-failed"):
                    self.prep.inspect_binfmt(registry)
            handler.write_bytes(b"x" * 4097)
            with self.assertRaisesRegex(RuntimeError, "binfmt-budget-exceeded"):
                self.prep.inspect_binfmt(registry)
            handler.write_bytes(self.binfmt_record())
            for index in range(64):
                (registry / f"extra-{index}").write_bytes(self.binfmt_record())
            with self.assertRaisesRegex(RuntimeError, "binfmt-budget-exceeded"):
                self.prep.inspect_binfmt(registry)

    def test_binfmt_total_budget_and_no_handler_name_authority(self):
        with tempfile.TemporaryDirectory() as temporary:
            registry = self.binfmt_directory(Path(temporary) / "registry", [self.binfmt_record()])
            for name in ("python", "qemu-aarch64", CANARY):
                existing = next(path for path in registry.iterdir() if path.name not in ("register", "status"))
                existing.rename(registry / name)
                self.assertEqual(self.prep.inspect_binfmt(registry)["handlers_examined"], 1)
            with patch.object(self.prep, "BINFMT_TOTAL_LIMIT", 8):
                with self.assertRaisesRegex(RuntimeError, "binfmt-budget-exceeded"):
                    self.prep.inspect_binfmt(registry)
            (registry / CANARY).write_bytes(self.binfmt_record(magic="7f454c46"))
            (registry / "status").write_bytes(b"disabled\n")
            with self.assertRaisesRegex(RuntimeError, "binfmt-possible-elf-handler"):
                self.prep.inspect_binfmt(registry)

    def test_native_tools_check_self_and_bash_without_treating_ldd_script_as_elf(self):
        checked = []
        with patch.object(self.prep.Path, "is_file", return_value=True), patch.object(self.prep.Path, "is_symlink", return_value=False), \
                patch.object(self.prep, "native_elf", side_effect=lambda path, target: checked.append(str(path))), \
                patch.object(self.prep.Path, "open", return_value=io.BytesIO(b"#!/bin/bash\npublic script\n")):
            self.prep.check_native_tools(TARGET)
        self.assertEqual(checked[0], "/proc/self/exe")
        self.assertIn("/bin/bash", checked)
        self.assertNotIn("/usr/bin/ldd", checked)
        tree = ast.parse(SCRIPT.read_text())
        argv = [node.args[0] for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr == "run" and node.args and isinstance(node.args[0], ast.List)]
        self.assertTrue(any([getattr(value, "value", None) for value in node.elts[:2]] == ["/bin/bash", "/usr/bin/ldd"] for node in argv))
        with patch.object(self.prep.Path, "is_file", return_value=True), patch.object(self.prep.Path, "is_symlink", return_value=False), \
                patch.object(self.prep, "native_elf"), patch.object(self.prep.Path, "open", return_value=io.BytesIO(b"#!/usr/bin/env bash\n")):
            with self.assertRaises(RuntimeError):
                self.prep.check_native_tools(TARGET)

    def test_current_self_machine_mismatch_refuses_before_any_child(self):
        with tempfile.TemporaryDirectory() as temporary:
            binary = Path(temporary) / "self"
            header = bytearray(20)
            header[:6] = b"\x7fELF\x02\x01"
            header[18:20] = (183).to_bytes(2, "little")
            binary.write_bytes(header)
            real_elf = self.prep.native_elf
            checked = []
            def native(path, target):
                checked.append(str(path))
                return real_elf(binary, target)
            with patch.object(self.prep, "native_elf", side_effect=native), patch.object(self.prep.subprocess, "Popen") as child:
                with self.assertRaisesRegex(RuntimeError, "admission"):
                    self.prep.check_native_tools(TARGET)
                child.assert_not_called()
            self.assertEqual(checked, ["/proc/self/exe"])

    def toolchain_notice_fixture(self, root, records):
        for name in ("downloads", "components", "prefix"):
            (root / name).mkdir()
        calls, notices = [], []
        def run(argv, **kwargs):
            calls.append(argv)
            raise RuntimeError("fixture-installer-stop")
        supervisor = SimpleNamespace(run_dir=root, remaining=lambda: 60, check_scratch=lambda: None, run=run)
        def download(_, url, path, checksum, **kwargs):
            path.write_text("")
            return {"sha256": checksum, "bytes": 1}
        def extract(archive, directory, supervisor):
            component = archive.name.removesuffix(".tar.xz")
            distribution = directory / "distribution"
            distribution.mkdir(parents=True)
            (distribution / "install.sh").write_text("fixture")
            for name, data in records.get(component, [("LICENSE-APACHE", b"notice\n")]):
                (distribution / name).write_bytes(data)
            return directory
        return supervisor, calls, notices, download, extract

    def test_observed_toolchain_notice_sizes_preserve_all_text_before_first_installer(self):
        records = {"rustc": [("COPYRIGHT.html", b"A" * 15119285), ("COPYRIGHT-library.html", b"B" * 2599602)]}
        for component, count, total in (("rustc", 3, 12362), ("cargo", 6, 160626), ("rust-src", 78, 442901),
                                         ("rust-std", 3, 12362), ("llvm-tools-preview", 1, 15141)):
            quotient, remainder = divmod(total, count)
            records.setdefault(component, []).extend((f"LICENSE-{index}.txt", b"L" * (quotient + (index < remainder))) for index in range(count))
        with tempfile.TemporaryDirectory() as temporary:
            supervisor, calls, notices, download, extract = self.toolchain_notice_fixture(Path(temporary), records)
            pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
            with patch.object(self.prep, "download", side_effect=download), patch.object(self.prep, "validate_component_manifest"), \
                    patch.object(self.prep, "extract_component", side_effect=extract):
                with self.assertRaisesRegex(RuntimeError, "fixture-installer-stop"):
                    self.prep.prepare_toolchain(supervisor, pin, TARGET, notices=notices)
            self.assertEqual(len(calls), 1)
            self.assertEqual(len(notices), 93)
            self.assertEqual(sum(len(item["text"].encode()) for item in notices), 18362279)
            expected = {(component, name): data for component, entries in records.items() for name, data in entries}
            for item in notices:
                data = expected[(item["component"], item["source"])]
                self.assertEqual(item["text"].encode(), data)
                self.assertEqual(item["sha256"], hashlib.sha256(data).hexdigest())

    def test_toolchain_notice_file_and_aggregate_caps_refuse_before_any_installer(self):
        pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
        for entries in ([("COPYRIGHT.html", b"X" * (16 * 1024 ** 2 + 1))],
                        [("LICENSE-a", b"X" * (16 * 1024 ** 2)), ("LICENSE-b", b"Y" * (16 * 1024 ** 2)), ("LICENSE-c", b"Z")]):
            with tempfile.TemporaryDirectory() as temporary:
                supervisor, calls, notices, download, extract = self.toolchain_notice_fixture(Path(temporary), {"rustc": entries})
                with patch.object(self.prep, "download", side_effect=download), patch.object(self.prep, "validate_component_manifest"), \
                        patch.object(self.prep, "extract_component", side_effect=extract):
                    with self.assertRaisesRegex(RuntimeError, "toolchain"):
                        self.prep.prepare_toolchain(supervisor, pin, TARGET, notices=notices)
                self.assertEqual(calls, [])

    def test_official_installer_uses_distinct_product_docs_below_private_prefix(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            supervisor, calls, notices, download, extract = self.toolchain_notice_fixture(root, {})
            pin = self.prep.asan.load_pin(SCRIPT.with_name("ffi_asan_toolchain.toml"))
            with patch.object(self.prep, "download", side_effect=download), patch.object(self.prep, "validate_component_manifest"), \
                    patch.object(self.prep, "extract_component", side_effect=extract):
                with self.assertRaisesRegex(RuntimeError, "fixture-installer-stop"):
                    self.prep.prepare_toolchain(supervisor, pin, TARGET, notices=notices)
            options = dict(value[2:].split("=", 1) for value in calls[0] if value.startswith("--") and "=" in value)
            self.assertNotIn("docdir", options)
            self.assertEqual(set(options), {"prefix", "sysconfdir", "bindir", "libdir", "datadir", "mandir"})
            self.assertTrue(all(Path(value).is_relative_to(root / "prefix") for value in options.values()))
            self.assertIn("--disable-ldconfig", calls[0])
            rust = Path(options["datadir"]) / "doc/rust/README.md"
            cargo = Path(options["datadir"]) / "doc/cargo/README.md"
            self.assertNotEqual(rust, cargo)

    def test_binfmt_operation_details_stay_closed_and_failed_admission_runs_nothing_later(self):
        for refusal, expected in (("binfmt", "binfmt-possible-elf-handler"), ("binfmt-exception", "binfmt-lookup-failed"),
                                  ("binfmt-unknown", "binfmt-entry-invalid")):
            with self.subTest(refusal=refusal):
                attempt = self.admission_attempt(refusal)
                self.assertEqual(attempt["schema_version"], 3)
                self.assertEqual(attempt["admission_substage"], "binfmt")
                self.assertEqual(attempt["admission_detail"], expected)
                self.prep.validate_attempt(attempt)
        for detail in self.prep.BINFMT_DETAILS:
            attempt = self.prep.new_attempt(TARGET)
            attempt["admission_substage"] = "binfmt"
            attempt["admission_detail"] = detail
            attempt["stages"]["admission"] = {"status": "failed", "reason": "admission"}
            attempt["first_failure"] = {"stage": "admission", "reason": "admission"}
            self.prep.validate_attempt(attempt)

    def test_workflow_is_feature_only_fixed_native_readonly_and_failure_preserving(self):
        workflow = SCRIPT.parents[2] / ".github/workflows/asan-prepare.yml"
        self.assertTrue(workflow.is_file())
        text = workflow.read_text()
        for value in ("branches: [feat/m10-asan-preparation]", "refs/heads/feat/m10-asan-preparation",
                      "ubuntu-24.04", "ubuntu-24.04-arm", "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
                      "contents: read", "persist-credentials: false", "if: always()", "if-no-files-found: error",
                      "3d3c42e5aac5ba805825da76410c181273ba90b1", "cf430e030ddbb5b0abf93d22962f4752f3646cd9",
                      "CPUQuota=200%", "MemoryMax=6G", "TasksMax=512", "RuntimeMaxSec=2400", "LimitCORE=0",
                      "validate_public_bundle", "public-provenance.json", "public-provenance.tar",
                      "steps.evidence.outcome == 'success'"):
            self.assertIn(value, text)
        for value in ("continue-on-error", "secrets.", "packages: write", "id-token:", "workflow_dispatch", "**", "apt-get install", "rustup"):
            self.assertNotIn(value, text)


if __name__ == "__main__":
    unittest.main()
