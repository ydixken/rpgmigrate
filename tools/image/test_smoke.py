#!/usr/bin/env python3
"""Offline refusal controls for the production image smoke."""

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch


PATH = Path(__file__).with_name("smoke.py")


class SmokeTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(PATH.is_file(), "the smoke helper must exist")
        spec = importlib.util.spec_from_file_location("image_smoke", PATH)
        self.smoke = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.smoke)
        self.metadata = [{"Id": "sha256:" + "a" * 64, "Architecture": "arm64", "Os": "linux",
                          "Config": {"User": "65532:65532", "WorkingDir": "/work",
                                     "Entrypoint": None, "Cmd": ["rpgmigrate", "--help"]}}]
        self.observed = {
            "uid": "65532", "gid": "65532", "machine": "aarch64",
            "version": json.dumps({"protocol_version": 1, "engine_version": "0.1.0", "capabilities": []}),
            "plain_version": "rpgmigrate 0.1.0\nlibpq 180001",
            "ldd": "libpq.so.5 => /lib/aarch64-linux-gnu/libpq.so.5 (0xffff)",
            "coreutils": "id (GNU coreutils) 9.7", "sed": "sed (GNU sed) 4.9",
            "default_psql": "psql (PostgreSQL) 18.1 (Debian 18.1-1.pgdg13+2)",
            "mountinfo": "25 20 0:1 / / ro,relatime - overlay overlay ro\n26 25 0:2 / /work rw - tmpfs tmpfs rw\n27 25 0:3 / /tmp rw - tmpfs tmpfs rw",
            "work_write": "ok", "tmp_write": "ok", "root_write": "denied",
            "work_owner": "65532:65532:700", "tmp_owner": "65532:65532:700",
            "inputs": "b" * 64 + "  /usr/share/rpgmigrate/build-inputs.sha256",
            "notice": "present",
        }
        for major in (16, 18):
            for tool in ("pg_dump", "pg_restore", "psql"):
                self.observed[f"{tool}_{major}"] = f"{tool} (PostgreSQL) {major}.1 (Debian {major}.1-1.pgdg13+2)"
        self.package = {"path": "/usr/lib/aarch64-linux-gnu/libpq.so.5.18",
                        "owner": "libpq5:arm64: /usr/lib/aarch64-linux-gnu/libpq.so.5.18",
                        "package": "libpq5\t18.1-1.pgdg13+2\tarm64\tinstall ok installed"}
        self.calls = []
        self.fail_at = None

    def invoke(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if len(self.calls) == self.fail_at:
            raise subprocess.CalledProcessError(1, argv, output="private output")
        if "inspect" in argv:
            output = json.dumps(self.metadata)
        else:
            records = self.package if len(self.calls) == 3 else self.observed
            output = "".join(f"===rpgmigrate-smoke:{name}===\n{value}\n" for name, value in records.items())
        return subprocess.CompletedProcess(argv, 0, output, "")

    def probe(self):
        with patch.object(self.smoke.subprocess, "run", side_effect=self.invoke):
            return self.smoke.probe("localhost/test:dev", "podman")

    def test_real_probe_contract_and_immutable_binding(self):
        receipt = self.probe()
        self.assertEqual(receipt["image_id"], self.metadata[0]["Id"])
        self.assertEqual(receipt["effective_uid"], 65532)
        self.assertEqual(receipt["architecture"], "arm64")
        self.assertEqual(set(receipt["clients"]), {"16", "18"})
        self.assertEqual(receipt["libpq"]["package_version"], "18.1-1.pgdg13+2")
        self.assertTrue(receipt["checks"]["read_only_rootfs"])
        for argv, options in self.calls[1:]:
            for option in ("--read-only", "--network=none", "--cap-drop=ALL", "--security-opt=no-new-privileges"):
                self.assertIn(option, argv)
            self.assertIn(self.metadata[0]["Id"], argv)
            self.assertNotIn("localhost/test:dev", argv)
            self.assertNotIn("--user", argv)
            self.assertIn("--read-only-tmpfs=false", argv)
            self.assertIn("type=tmpfs,destination=/work,tmpfs-mode=0700,U=true", argv)
            self.assertIn("type=tmpfs,destination=/tmp,tmpfs-mode=0700,U=true", argv)
            self.assertGreater(options["timeout"], 0)
            self.assertFalse(options.get("shell", False))

    def test_each_subprocess_failure_is_fatal_without_echoing_output(self):
        for step in range(1, 4):
            with self.subTest(step=step):
                self.calls = []
                self.fail_at = step
                with self.assertRaisesRegex(RuntimeError, "subprocess failed") as error:
                    self.probe()
                self.assertNotIn("private output", str(error.exception))

    def test_podman_bare_content_id_is_normalized(self):
        self.metadata[0]["Id"] = "a" * 64
        receipt = self.probe()
        self.assertEqual(receipt["image_id"], "sha256:" + "a" * 64)
        self.assertIn(receipt["image_id"], self.calls[1][0])

    def test_missing_runtime_and_timeout_are_fatal(self):
        for error in (FileNotFoundError(), subprocess.TimeoutExpired("podman", 1)):
            with patch.object(self.smoke.subprocess, "run", side_effect=error):
                with self.assertRaises(RuntimeError):
                    self.smoke.probe("image", "podman")

    def test_every_required_observation_is_required(self):
        for records in (self.observed, self.package):
            for name in tuple(records):
                with self.subTest(name=name):
                    value = records.pop(name)
                    self.calls = []
                    with self.assertRaises(RuntimeError):
                        self.probe()
                    records[name] = value

    def test_wrong_observations_fail_closed(self):
        changes = {"uid": "0", "gid": "0", "machine": "x86_64", "version": "{}",
                   "plain_version": "rpgmigrate 0.1.0\nlibpq 170001",
                   "ldd": "libpq.so.5 => not found", "coreutils": "busybox", "sed": "busybox",
                   "default_psql": "psql (PostgreSQL) 16.1", "mountinfo": "25 20 0:1 / / rw - overlay overlay rw",
                   "work_write": "denied", "tmp_write": "denied", "root_write": "ok", "inputs": "", "notice": "",
                   "work_owner": "0:0:700", "tmp_owner": "0:0:777"}
        for major in (16, 18):
            for tool in ("pg_dump", "pg_restore", "psql"):
                changes[f"{tool}_{major}"] = f"{tool} (PostgreSQL) 17.1"
        for name, value in changes.items():
            with self.subTest(name=name):
                before = self.observed[name]
                self.observed[name] = value
                self.calls = []
                with self.assertRaises(RuntimeError):
                    self.probe()
                self.observed[name] = before

    def test_wrong_package_binding_is_refused(self):
        for name, value in (("path", "relative"), ("owner", "foreign: /lib/libpq.so.5"),
                            ("package", "libpq5\t17.1-1.pgdg13+2\tarm64\tinstall ok installed"),
                            ("package", "libpq5\t18.1-1\tarm64\tinstall ok installed"),
                            ("package", "libpq5\t18.2-1.pgdg13+2\tarm64\tinstall ok installed"),
                            ("package", "libpq5\t18.1-1.pgdg13+2\tamd64\tinstall ok installed")):
            with self.subTest(name=name, value=value):
                before = self.package[name]
                self.package[name] = value
                self.calls = []
                with self.assertRaises(RuntimeError):
                    self.probe()
                self.package[name] = before

    def test_invalid_metadata_is_refused(self):
        valid = copy.deepcopy(self.metadata)
        for metadata in ([], {}, [{}], [{**valid[0], "Id": "tag"}],
                         [{**valid[0], "Architecture": "s390x"}], [{**valid[0], "Os": "windows"}],
                         [{**valid[0], "Config": {**valid[0]["Config"], "User": "0"}}],
                         [{**valid[0], "Config": {**valid[0]["Config"], "Entrypoint": ["/missing"]}}]):
            with self.subTest(metadata=metadata):
                self.metadata = metadata
                self.calls = []
                with self.assertRaises(RuntimeError):
                    self.probe()

    def test_malformed_or_duplicate_probe_output_is_refused(self):
        with self.assertRaises(RuntimeError):
            self.smoke.records("not a probe")
        with self.assertRaises(RuntimeError):
            self.smoke.records("===rpgmigrate-smoke:uid===\n65532\n===rpgmigrate-smoke:uid===\n0\n")

    def test_malformed_metadata_json_is_refused(self):
        with patch.object(self.smoke.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "not json", "")):
            with self.assertRaises(RuntimeError):
                self.smoke.probe("image", "podman")

    def test_shell_metacharacters_stay_one_inspect_argument(self):
        image = "local/test;$(printf secret):dev"
        with patch.object(self.smoke.subprocess, "run", side_effect=self.invoke):
            self.smoke.probe(image, "podman")
        self.assertEqual(self.calls[0][0], ["podman", "image", "inspect", image])

    def test_version_contract_does_not_advertise_unqualified_capabilities(self):
        for version in ({"protocol_version": True, "engine_version": "0.1.0", "capabilities": []},
                        {"protocol_version": 1, "engine_version": "0.1.0", "capabilities": ["clone"]},
                        {"protocol_version": 1, "engine_version": "0.1.0", "capabilities": [], "linked_libpq": 180001}):
            self.observed["version"] = json.dumps(version)
            self.calls = []
            with self.assertRaises(RuntimeError):
                self.probe()


if __name__ == "__main__":
    unittest.main()
