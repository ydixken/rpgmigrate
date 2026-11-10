"""Portable refusal controls for the separate native ASAN preparation entry point."""

import ast
import copy
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
            with patch.dict(os.environ, {"RUNNER_TEMP": str(root)}, clear=True), \
                    patch.object(self.prep, "reject_overrides"), patch.object(self.prep, "cgroup_limits", return_value={}), \
                    patch.object(self.prep.Path, "read_text", autospec=True, side_effect=lambda path, *a, **k: "0::/cell\n" if str(path) == "/proc/self/cgroup" else Path.read_bytes(path).decode()), \
                    patch.object(self.prep, "admit", return_value={"machine": "x86_64"}), \
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
                    patch.object(self.prep.Path, "read_text", autospec=True, side_effect=text), \
                    patch.object(self.prep.Path, "is_dir", return_value=False), \
                    patch.object(self.prep.shutil, "disk_usage", return_value=SimpleNamespace(free=free)), \
                    patch.object(self.prep.resource, "getrlimit", return_value=(0, 0)):
                if admitted:
                    value = self.prep.admit(supervisor, TARGET, parent, pin)
                    self.assertEqual(value["host_available_memory_bytes"], memory_kib * 1024)
                    self.assertEqual(value["scratch"]["enforcement"], "bounded-monitor-not-hard-quota")
                else:
                    with self.assertRaises(RuntimeError):
                        self.prep.admit(supervisor, TARGET, parent, pin)

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
