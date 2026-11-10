"""Offline controls for the fail-closed native Linux ASAN wrapper."""

import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


PATH = Path(__file__).with_name("ffi_asan.py")
PIN = PATH.with_name("ffi_asan_toolchain.toml")
TARGET = "x86_64-unknown-linux-gnu"


class AsanTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(PATH.is_file(), "ASAN wrapper is missing")
        spec = importlib.util.spec_from_file_location("ffi_asan", PATH)
        self.asan = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.asan)

    def qualified(self):
        pin = self.asan.load_pin(PIN)
        pin["external_symbolizer"] = {
            "coverage": pin["external_symbolizer"]["coverage"],
            "status": "qualified", "targets": {TARGET: {
                "path": "/usr/bin/llvm-symbolizer-23", "sha256": "a" * 64,
                "version": "LLVM version 23.1.0", "package": "llvm-23",
                "package_version": "23.1.0-1", "architecture": "amd64"}}}
        return pin

    def test_tracked_pin_requires_external_symbolizer_qualification(self):
        pin = self.asan.load_pin(PIN)
        self.assertEqual(pin["toolchain"], "nightly-2026-11-09")
        self.assertEqual(pin["manifest_sha256"], "61ad7ceb52dafd0ef49bfe0a379259d2587039e840986021c49df008640e0d23")
        with patch.object(self.asan.subprocess, "Popen") as child:
            with self.assertRaisesRegex(RuntimeError, "symbolizer.*qualification"):
                self.asan.qualified_symbolizer(pin, TARGET)
            with self.assertRaisesRegex(RuntimeError, "symbolizer.*qualification"):
                self.asan.execute(SimpleNamespace(manifest=PIN, target=TARGET))
            child.assert_not_called()

    def test_manifest_rejects_missing_moving_malformed_or_incomplete_pin(self):
        original = PIN.read_text()
        changes = [
            original.replace('nightly-2026-11-09', 'nightly'),
            original.replace('nightly-2026-11-09', 'nightly-2026-02-30'),
            original.replace('61ad7ceb52dafd0ef49bfe0a379259d2587039e840986021c49df008640e0d23', 'bad'),
            original.replace('5b7b37aefcb5b2fba3c2392bc9065b9e4602d494', 'bad'),
            original.replace('rust-src', 'missing-source'),
            original.replace('https://static.rust-lang.org/', 'https://foreign.invalid/'),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pin.toml"
            with self.assertRaises(RuntimeError):
                self.asan.load_pin(path)
            for text in changes:
                path.write_text(text)
                with self.subTest(text=text[:40]), self.assertRaises(RuntimeError):
                    self.asan.load_pin(path)

    def test_native_target_refuses_macos_emulation_and_wrong_architecture(self):
        with patch.object(self.asan.platform, "system", return_value="Linux"), patch.object(
                self.asan.platform, "machine", return_value="x86_64"):
            self.asan.native_target(TARGET)
            with self.assertRaises(RuntimeError):
                self.asan.native_target("aarch64-unknown-linux-gnu")
        for system, machine in (("Darwin", "arm64"), ("Linux", "aarch64"), ("Linux", "s390x")):
            with patch.object(self.asan.platform, "system", return_value=system), patch.object(
                    self.asan.platform, "machine", return_value=machine), self.assertRaises(RuntimeError):
                self.asan.native_target(TARGET)

    def test_child_settings_are_exact_and_do_not_mutate_parent(self):
        parent = {"PATH": "/usr/bin", "RUSTFLAGS": "unsafe-old", "RUSTC_BOOTSTRAP": "1",
                  "CARGO_ENCODED_RUSTFLAGS": "old", "RUSTC_WRAPPER": "foreign"}
        before = dict(parent)
        with tempfile.TemporaryDirectory() as directory:
            env = self.asan.child_environment(parent, Path(directory), "/usr/bin/llvm-symbolizer-23")
        self.assertEqual(parent, before)
        self.assertEqual(env["RUSTFLAGS"], "-Zsanitizer=address")
        self.assertEqual(env["ASAN_OPTIONS"], "detect_leaks=1:abort_on_error=1:detect_stack_use_after_return=1")
        for key in ("RUSTC_BOOTSTRAP", "CARGO_ENCODED_RUSTFLAGS"):
            self.assertNotIn(key, env)
        self.assertEqual(env["RUSTC_WRAPPER"], "")
        self.assertEqual(env["RUSTC_WORKSPACE_WRAPPER"], "")
        with self.assertRaises(RuntimeError):
            self.asan.child_environment({"CFLAGS": "override"}, Path("/tmp/private"), "/usr/bin/llvm-symbolizer")

    def test_inherited_leak_suppressions_and_compiler_overrides_cannot_weaken_evidence(self):
        env = self.asan.child_environment({"LSAN_OPTIONS": "suppressions=/tmp/all"}, Path("/tmp/owned"), "/usr/bin/llvm-symbolizer")
        self.assertNotIn("LSAN_OPTIONS", env)
        for key in ("RUSTC", "CARGO_BUILD_RUSTC", "LD_PRELOAD"):
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.asan.child_environment({key: "foreign"}, Path("/tmp/owned"), "/usr/bin/llvm-symbolizer")

    def test_binary_requires_correct_native_elf_inside_disjoint_target_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / TARGET / "debug" / "rpgmigrate"
            binary.parent.mkdir(parents=True)
            elf = bytearray(64); elf[:6] = b"\x7fELF\x02\x01"; elf[18:20] = (62).to_bytes(2, "little")
            binary.write_bytes(elf); binary.chmod(0o700)
            self.assertEqual(len(self.asan.check_binary(binary, root, TARGET)), 64)
            for change in (b"not ELF", bytes(elf[:18]) + (183).to_bytes(2, "little") + bytes(elf[20:])):
                binary.write_bytes(change)
                with self.assertRaises(RuntimeError):
                    self.asan.check_binary(binary, root, TARGET)
            with self.assertRaises(RuntimeError):
                self.asan.check_binary(binary, root / "other", TARGET)

    def test_inventory_and_live_receipts_are_exact_and_nonempty(self):
        expected = ["pg::ffi::tests::one"]
        good = "test pg::ffi::tests::one ... ok\n\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n"
        self.asan.validate_tests(expected, good)
        for names, output in (([], good), (expected, ""), (expected, good.replace("... ok", "... ignored")),
                              (expected, good.replace("one", "other")), (expected, good + good),
                              (expected, good.replace("1 passed", "0 passed"))):
            with self.subTest(names=names, output=output[:30]), self.assertRaises(RuntimeError):
                self.asan.validate_tests(names, output)
        self.asan.validate_profile("Transport", expected, good + "Transport verified 1 exact cases; full E1 suite not selected\n")
        with self.assertRaises(RuntimeError):
            self.asan.validate_profile("Transport", expected, good)
        with self.assertRaises(RuntimeError):
            self.asan.validate_profile("Replication", expected, good + "Replication verified 1 exact cases; full E1 suite not selected\n")

    def test_sanitizer_control_requires_failure_and_expected_category(self):
        self.asan.validate_control(subprocess.CompletedProcess([], 1, "", "ERROR: AddressSanitizer: heap-use-after-free"))
        for result in (subprocess.CompletedProcess([], 0, "", "ERROR: AddressSanitizer: heap-use-after-free"),
                       subprocess.CompletedProcess([], 1, "", "private failure"),
                       subprocess.CompletedProcess([], 1, "", "AddressSanitizer: stack-buffer-overflow")):
            with self.assertRaises(RuntimeError):
                self.asan.validate_control(result)

    def test_failed_timed_out_or_sanitizer_reporting_children_refuse(self):
        for stdout, stderr in (("", "ERROR: AddressSanitizer: bad"), ("SUMMARY: LeakSanitizer: bad", ""),
                               ("", "AddressSanitizer:DEADLYSIGNAL")):
            process = unittest.mock.Mock(returncode=0, pid=1234)
            process.communicate.return_value = (stdout, stderr)
            with patch.object(self.asan.subprocess, "Popen", return_value=process), patch.object(
                    self.asan.os, "killpg", side_effect=ProcessLookupError), self.assertRaises(RuntimeError):
                self.asan.run(["fixture"], {}, Path("/tmp"), 2)
        process = unittest.mock.Mock(returncode=1, pid=1234)
        process.communicate.return_value = ("private://credentials", "private-row")
        with patch.object(self.asan.subprocess, "Popen", return_value=process), patch.object(
                self.asan.os, "killpg", side_effect=ProcessLookupError), self.assertRaisesRegex(RuntimeError, "child failed"):
            self.asan.run(["fixture"], {}, Path("/tmp"), 2)
        process.communicate.side_effect = [subprocess.TimeoutExpired(["fixture"], 2), ("", "")]
        with patch.object(self.asan.subprocess, "Popen", return_value=process), patch.object(
                self.asan.os, "killpg", side_effect=ProcessLookupError), self.assertRaisesRegex(RuntimeError, "timed out"):
            self.asan.run(["fixture"], {}, Path("/tmp"), 2)

    def test_sanitizer_failure_evidence_is_invariant_under_secret_canaries(self):
        messages = []
        for canary in ("row-secret-A", "credential-secret-B-with-a-different-length"):
            process = unittest.mock.Mock(returncode=0, pid=1234)
            process.communicate.return_value = (canary, "ERROR: AddressSanitizer: heap-use-after-free " + canary)
            with patch.object(self.asan.subprocess, "Popen", return_value=process), patch.object(
                    self.asan.os, "killpg", side_effect=ProcessLookupError), self.assertRaises(RuntimeError) as error:
                self.asan.run(["fixture"], {}, Path("/tmp"), 2)
            messages.append(str(error.exception))
            self.assertIn("heap-use-after-free", messages[-1])
            self.assertRegex(messages[-1], r"sanitized_evidence_sha256=[0-9a-f]{64}")
            self.assertNotIn(canary, messages[-1])
            self.assertNotIn("output_sha256", messages[-1])
        self.assertEqual(messages[0], messages[1])

    def test_passing_evidence_hashes_only_the_closed_validated_projection(self):
        expected = ["pg::ffi::tests::one"]
        output = "test pg::ffi::tests::one ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n"
        records = []
        for canary in ("row-secret-A", "credential-secret-B-with-a-different-length"):
            result = subprocess.CompletedProcess([], 0, output + canary, canary)
            records.append(self.asan.profile_evidence("FFI", expected, result))
            self.assertNotIn(canary, json.dumps(records[-1]))
        self.assertEqual(records[0], records[1])
        projection = {"stage": "FFI", "status": "passed", "returncode": 0, "timed_out": False,
                      "sanitizer_category": "none", "test_count": 1}
        self.assertEqual(records[0]["sanitized_evidence"], projection)
        expected_hash = hashlib.sha256(json.dumps(projection, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(records[0]["sanitized_evidence_sha256"], expected_hash)

    def test_evidence_refuses_unknown_stage_status_category_and_raw_projection_fields(self):
        for key, value in (("stage", "secret-stage"), ("status", "secret-status"),
                           ("sanitizer_category", "secret-category"), ("returncode", "secret-code"),
                           ("test_count", "secret-count"), ("timed_out", "secret-timeout")):
            values = {"stage": "child", "status": "passed", "returncode": 0}
            values[key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.asan.sanitized_evidence(**values)
        for field in ("stdout", "stderr", "raw_output_sha256", "output_length", "message", "test_names"):
            with self.subTest(field=field), self.assertRaises(TypeError):
                self.asan.sanitized_evidence("child", status="passed", returncode=0, **{field: "secret"})

    def test_unsupported_sanitizer_text_refuses_without_fingerprinting_its_canaries(self):
        messages = []
        for canary in ("private-A", "private-B-is-longer"):
            process = unittest.mock.Mock(returncode=0, pid=1234)
            process.communicate.return_value = ("", "AddressSanitizer: unsupported-category " + canary)
            with patch.object(self.asan.subprocess, "Popen", return_value=process), patch.object(
                    self.asan.os, "killpg", side_effect=ProcessLookupError), self.assertRaises(RuntimeError) as error:
                self.asan.run(["fixture"], {}, Path("/tmp"), 2)
            messages.append(str(error.exception))
        self.assertEqual(messages[0], messages[1])
        self.assertIn("unsupported", messages[0])

    def test_missing_libpq_and_unqualified_symbolizer_refuse(self):
        pin = self.qualified()
        self.assertEqual(self.asan.qualified_symbolizer(pin, TARGET)["package"], "llvm-23")
        pin["external_symbolizer"]["targets"][TARGET]["version"] = "Ubuntu LLVM version 23.1.0"
        self.assertEqual(self.asan.qualified_symbolizer(pin, TARGET)["version"], "Ubuntu LLVM version 23.1.0")
        for key in ("sha256", "version", "package", "package_version", "path", "architecture"):
            invalid = copy.deepcopy(pin); del invalid["external_symbolizer"]["targets"][TARGET][key]
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.asan.qualified_symbolizer(invalid, TARGET)
        with self.assertRaises(RuntimeError):
            self.asan.linked_libpq("libc.so.6 => /lib/libc.so.6 (0x1)\n")
        with self.assertRaises(RuntimeError):
            self.asan.linked_libpq("libpq.so.5 => not found\n")
        self.assertEqual(self.asan.linked_libpq("libpq.so.5 => /usr/lib/libpq.so.5 (0x1)\n"), Path("/usr/lib/libpq.so.5"))

    def test_missing_or_empty_receipt_cannot_be_published(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            with self.assertRaises(RuntimeError):
                self.asan.publish_receipt(path, {})
            receipt = {"profiles": [{"cases": ["synthetic-case"]}], "control": "heap-use-after-free"}
            self.asan.publish_receipt(path, receipt)
            self.assertEqual(json.loads(path.read_text()), receipt)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(RuntimeError):
                self.asan.publish_receipt(path, receipt)

    def test_mocked_pipeline_keeps_argv_instrumentation_and_all_six_live_receipts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            args = SimpleNamespace(manifest=PIN, target=TARGET, target_dir=root / "asan-target",
                                   receipt=root / "receipt.json", pg16_bindir=root / "pg16",
                                   pg18_bindir=root / "pg18", timeout=42)
            pin = self.qualified()
            symbolizer = root / "llvm-symbolizer"
            symbolizer.write_bytes(b"synthetic-symbolizer")
            symbolizer_entry = pin["external_symbolizer"]["targets"][TARGET]
            symbolizer_entry.update(path=str(symbolizer), sha256=self.asan.digest(symbolizer))
            pin["compatibility"]["license_security_review"] = "qualified"
            library = root / "libpq.so.5"
            library.write_bytes(b"synthetic-libpq")
            binary = args.target_dir / TARGET / "debug/rpgmigrate"
            test_binary = binary.with_name("test-binary")
            manifest = json.loads((PATH.parents[2] / "tests/fixtures/e1/manifest.json").read_text())
            cases = {profile: self.asan.module_cases(manifest, profile) for profile in ("Transport", "Prepared", "Replication")}
            inventory = ["pg::ffi::tests::synthetic_unit", *(name for names in cases.values() for name in names)]
            observed_profiles = []

            def elf(path):
                path.parent.mkdir(parents=True, exist_ok=True)
                header = bytearray(64); header[:6] = b"\x7fELF\x02\x01"; header[18:20] = (62).to_bytes(2, "little")
                path.write_bytes(header); path.chmod(0o700)

            def output(names):
                return "".join(f"test {name} ... ok\n" for name in names) + f"test result: ok. {len(names)} passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s\n"

            def invoke(argv, environment, cwd, timeout, *, control=False):
                self.assertIsInstance(argv, list)
                self.assertEqual(timeout, 42)
                self.assertEqual(environment["RUSTFLAGS"], "-Zsanitizer=address")
                self.assertEqual(environment["CARGO_TARGET_DIR"], str(args.target_dir))
                if argv == [str(symbolizer), "--version"]:
                    text = "LLVM version 23.1.0\n"
                elif argv[:2] == ["dpkg-query", "-S"]:
                    name = "llvm-23" if argv[-1] == str(symbolizer) else "libpq5"
                    text = f"{name}:amd64: {argv[-1]}\n"
                elif argv[:2] == ["dpkg-query", "-W"]:
                    name = argv[-1].split(":")[0]
                    version = "23.1.0-1" if name == "llvm-23" else "18.6-1.pgdg24.04+1"
                    text = f"{name}\t{version}\tamd64\tinstall ok installed"
                elif "--verbose" in argv:
                    text = f"rustc {pin['rustc_version']}\ncommit-hash: {pin['compiler_commit']}\nhost: {TARGET}\n"
                elif argv[-2:] == ["cargo", "--version"]:
                    text = "cargo 1.102.0-nightly (c07ac2c4d 2026-11-06)\n"
                elif argv[:3] == ["rustup", "component", "list"]:
                    text = f"rust-src\nllvm-tools-{TARGET}\n"
                elif "cargo" in argv:
                    self.assertIn("--locked", argv); self.assertIn("-Zbuild-std", argv)
                    self.assertEqual(argv[argv.index("--target") + 1], TARGET)
                    is_test = "test" in argv; path = test_binary if is_test else binary
                    elf(path)
                    text = json.dumps({"reason": "compiler-artifact", "target": {"name": "rpgmigrate"},
                                       "profile": {"test": is_test}, "executable": str(path)})
                elif argv[0] == "ldd":
                    text = f"libpq.so.5 => {library} (0x123)\n"
                elif "-c" in argv:
                    text = "180006\n"
                elif "rustc" in argv:
                    self.assertIn("-Zsanitizer=address", argv)
                    elf(Path(argv[-1])); text = ""
                elif control:
                    return subprocess.CompletedProcess(argv, 1, "", "ERROR: AddressSanitizer: heap-use-after-free")
                elif "--list" in argv:
                    text = "".join(name + ": test\n" for name in inventory)
                elif argv[0] == str(test_binary):
                    self.assertIn("--exact", argv)
                    text = output([inventory[0]])
                elif argv[-1] == "--version":
                    text = "postgres (PostgreSQL) " + ("16.14" if "pg16" in argv[0] else "18.6") + "\n"
                else:
                    self.assertEqual(argv[1], str(PATH.parents[2] / "tools/test-db.py"))
                    profile = argv[2].removeprefix("--").title()
                    observed_profiles.append((profile, environment["RPGMIGRATE_TEST_EXPECTED_PG_VERSION"]))
                    self.assertEqual(argv[3:5], ["--", str(test_binary)])
                    self.assertIn("--ignored", argv); self.assertIn("--exact", argv)
                    self.assertEqual(environment["RPGMIGRATE_BIN"], str(binary))
                    text = output(cases[profile]) + f"{profile} verified {len(cases[profile])} exact cases; full E1 suite not selected\n"
                    if profile == "Replication":
                        text += "Replication faults verified 4 wire cases and observed PQflush backpressure\n"
                return subprocess.CompletedProcess(argv, 0, text, "")

            with patch.object(self.asan, "load_pin", return_value=pin), patch.object(self.asan, "native_target"), patch.object(
                    self.asan, "run", side_effect=invoke), patch.dict(os.environ, {}, clear=True):
                self.asan.execute(args)
            receipt = json.loads(args.receipt.read_text())
            self.assertEqual(len(receipt["profiles"]), 7)
            self.assertEqual(observed_profiles, [(profile, version) for version in ("16.14", "18.6")
                                                for profile in ("Transport", "Prepared", "Replication")])
            self.assertEqual(receipt["libpq"]["version"], 180006)
            self.assertNotIn("synthetic-libpq", args.receipt.read_text())


if __name__ == "__main__":
    unittest.main()
