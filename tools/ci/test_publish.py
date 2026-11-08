"""Exercise publication identity and index refusals without registry access."""

import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("image-publish.py")


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(SCRIPT.is_file(), "publication helper is absent")
        spec = importlib.util.spec_from_file_location("image_publish", SCRIPT)
        self.publisher = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.publisher)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def layouts(self):
        layouts = []
        for architecture in ("amd64", "arm64"):
            path = self.root / architecture
            blobs = path / "blobs/sha256"
            blobs.mkdir(parents=True)
            config = encode({"os": "linux", "architecture": architecture})
            config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
            manifest = encode({"schemaVersion": 2, "config": {"digest": config_digest}})
            digest = "sha256:" + hashlib.sha256(manifest).hexdigest()
            (blobs / config_digest[7:]).write_bytes(config)
            (blobs / digest[7:]).write_bytes(manifest)
            descriptor = {"mediaType": "application/vnd.oci.image.manifest.v1+json",
                          "digest": digest, "size": len(manifest)}
            (path / "index.json").write_bytes(encode({"schemaVersion": 2, "manifests": [descriptor]}))
            receipt = {"architecture": architecture, "image": {"manifest_digest": digest,
                       "config_digest": config_digest}}
            layouts.append((path, receipt))
        return layouts

    def test_variant_tag_preserves_default_pg18_and_suffixes_pg16(self):
        self.assertTrue(hasattr(self.publisher, 'variant_tag'), 'variant release identity is required')
        self.assertEqual(self.publisher.variant_tag('v1.2.3', '18.6'), 'v1.2.3')
        self.assertEqual(self.publisher.variant_tag('v1.2.3-rc.1', '16.15'), 'v1.2.3-rc.1-pg16')
        with self.assertRaises(RuntimeError):
            self.publisher.variant_tag('v1.2.3', '17.11')
        for version in ('16.15', '18.6'):
            with self.assertRaisesRegex(RuntimeError, 'reserved'):
                self.publisher.variant_tag('v1.2.3-pg16', version)

    def test_same_source_different_client_variants_cannot_form_one_index(self):
        self.bundle_fixture('amd64')
        directory, identity = self.bundle_fixture('arm64')
        spec = importlib.util.spec_from_file_location('variant_fixture', SCRIPT.parents[1] / 'image/test_runtime.py')
        fixture = importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture)
        path, _ = fixture.write_fixture(directory, architecture='arm64', client_version='16.15')
        # All prerequisite bindings must match before the cross-variant guard is exercised.
        runtime = self.publisher.image_module('runtime')
        receipt = runtime.load_receipt(path)
        smoke_path = directory / 'smoke.json'; smoke = json.loads(smoke_path.read_bytes())
        smoke.update(self.publisher.image_module('smoke').bindings(receipt, self.publisher.digest(path)))
        smoke_path.write_bytes(encode(smoke))
        _, envelope = self.publisher.evidence(directory, identity, 'arm64')
        (directory / 'ci.json').write_bytes(encode(envelope))
        with self.assertRaisesRegex(RuntimeError, 'client variants'):
            self.publisher.bundles(self.root, identity)

    def test_index_preserves_two_exact_native_manifests(self):
        layouts = self.layouts()
        index = self.publisher.assemble_index(layouts, self.root / "merged")
        decoded = json.loads(index)
        self.assertEqual([entry["platform"] for entry in decoded["manifests"]],
                         [{"os": "linux", "architecture": "amd64"}, {"os": "linux", "architecture": "arm64"}])
        self.assertEqual([entry["digest"] for entry in decoded["manifests"]],
                         [receipt["image"]["manifest_digest"] for _, receipt in layouts])
        self.publisher.validate_index(index, index)

    def test_missing_duplicate_and_unknown_architectures_refuse(self):
        layouts = self.layouts()
        variants = [[], layouts[:1], layouts + layouts[:1], [layouts[0], layouts[0]]]
        wrong = copy.deepcopy(layouts)
        wrong[1][1]["architecture"] = "riscv64"
        variants.append(wrong)
        for i, entries in enumerate(variants):
            with self.subTest(case=i), self.assertRaises(RuntimeError):
                self.publisher.assemble_index(entries, self.root / str(i))

    def test_changed_manifest_or_blob_refuses_assembly(self):
        layouts = self.layouts()
        manifest = layouts[0][0] / "blobs/sha256" / layouts[0][1]["image"]["manifest_digest"][7:]
        manifest.write_bytes(b"changed")
        with self.assertRaises(RuntimeError):
            self.publisher.assemble_index(layouts, self.root / "merged")

    def test_extra_local_descriptors_refuse_assembly(self):
        layouts = self.layouts()
        index_path = layouts[0][0] / "index.json"
        index = json.loads(index_path.read_bytes())
        index["manifests"] *= 2
        index_path.write_bytes(encode(index))
        with self.assertRaises(RuntimeError):
            self.publisher.assemble_index(layouts, self.root / "merged")

    def test_remote_index_rewrite_or_platform_change_refuses(self):
        expected = self.publisher.assemble_index(self.layouts(), self.root / "merged")
        variants = [b"", expected + b"\n"]
        for update in ([], [{"os": "linux", "architecture": "arm64"}]) :
            changed = json.loads(expected)
            if update:
                changed["manifests"][0]["platform"] = update[0]
            else:
                changed["manifests"] = []
            variants.append(encode(changed))
        for actual in variants:
            with self.subTest(actual=actual), self.assertRaises(RuntimeError):
                self.publisher.validate_index(actual, expected)

    def test_run_and_source_identity_cannot_be_empty_or_malformed(self):
        self.assertEqual(self.publisher.run_identity("d" * 40, "123", "2"),
                         {"source_revision": "d" * 40, "run_id": "123", "run_attempt": "2"})
        for args in (("", "123", "2"), ("d" * 40, "", "2"), ("d" * 40, "123", "0")):
            with self.assertRaises(RuntimeError):
                self.publisher.run_identity(*args)

    def test_release_destination_only_accepts_version_tags(self):
        self.assertEqual(self.publisher.destination("ghcr.io/ydixken/rpgmigrate", "v1.2.3-rc.1"),
                         "docker://ghcr.io/ydixken/rpgmigrate:v1.2.3-rc.1")
        for tag in ("latest", "main", "v1", "v1.2.3/other", "v1.2.3+metadata"):
            with self.subTest(tag=tag), self.assertRaises(RuntimeError):
                self.publisher.destination("ghcr.io/ydixken/rpgmigrate", tag)

    def bundle_fixture(self, architecture="arm64"):
        runtime = self.publisher.image_module("runtime")
        smoke = self.publisher.image_module("smoke")
        spec = importlib.util.spec_from_file_location("oci_fixture", SCRIPT.parents[1] / "image/test_runtime.py")
        fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(fixture)
        directory = self.root / ("native-" + architecture)
        directory.mkdir(exist_ok=True)
        path, _ = fixture.write_fixture(directory, architecture=architecture)
        receipt = runtime.inspect_archive(directory / "image.oci.tar")
        receipt.update(schema=1, status="built", archive={"path": "image.oci.tar", "sha256": self.publisher.digest(directory / "image.oci.tar")})
        path.write_bytes(encode(receipt))
        record = {"schema": 1, "status": "passed", "receipt_sha256": self.publisher.digest(path),
                  "archive_sha256": receipt["archive"]["sha256"], **receipt["image"],
                  "platform": receipt["platform"], "architecture": architecture,
                  "source_sha256": receipt["source"]["sha256"], "binary_sha256": receipt["binary_sha256"],
                  "native_sha256": runtime.canonical_sha256(receipt["native"]),
                  "runtime_sha256": runtime.canonical_sha256(receipt["runtime"]),
                  "fixture_image": "sha256:" + "1" * 64, "fixture_version": "17.11",
                  "fixture_version_num": 170011, "probe_sha256": "2" * 64,
                  "checks": [{"name": name, "status": "passed"} for name in smoke.SMOKE_CHECKS]}
        (directory / "smoke.json").write_bytes(encode(record))
        verification = {"status": "passed", "source_sha256": "3" * 64,
                        "image_id": record["fixture_image"], "architecture": architecture,
                        "stages": [{"name": name, "status": "passed"} for name in self.publisher.STAGES]}
        (directory / "verification.json").write_bytes(encode(verification))
        identity = self.publisher.run_identity("d" * 40, "123", "2")
        _, envelope = self.publisher.evidence(directory, identity, architecture)
        (directory / "ci.json").write_bytes(encode(envelope))
        return directory, identity

    def test_missing_client_loader_check_refuses_publication_evidence(self):
        directory, identity = self.bundle_fixture('amd64')
        path = directory / 'smoke.json'
        record = json.loads(path.read_bytes())
        record['checks'] = [item for item in record['checks'] if item['name'] != 'client-loader-resolution']
        path.write_bytes(encode(record))
        with self.assertRaisesRegex(RuntimeError, 'smoke checks'):
            self.publisher.evidence(directory, identity, 'amd64')

    def test_two_same_run_bundles_validate_actual_oci_and_smoke(self):
        self.bundle_fixture("amd64")
        _, identity = self.bundle_fixture("arm64")
        self.assertEqual(len(self.publisher.bundles(self.root, identity)), 2)

    def test_wrong_source_run_or_attempt_refuses(self):
        self.bundle_fixture("amd64")
        _, identity = self.bundle_fixture("arm64")
        for field, value in (("source_revision", "e" * 40), ("run_id", "124"), ("run_attempt", "3")):
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                self.publisher.bundles(self.root, {**identity, field: value})

    def test_missing_receipt_refuses(self):
        self.bundle_fixture("amd64")
        directory, identity = self.bundle_fixture("arm64")
        for name in ("receipt.json", "smoke.json", "ci.json", "verification.json"):
            data = (directory / name).read_bytes()
            (directory / name).unlink()
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                self.publisher.bundles(self.root, identity)
            (directory / name).write_bytes(data)

    def test_wrong_smoke_digest_or_missing_case_refuses(self):
        directory, identity = self.bundle_fixture()
        path = directory / "smoke.json"
        original = json.loads(path.read_bytes())
        for changes in ({"archive_sha256": "f" * 64}, {"manifest_digest": "sha256:" + "f" * 64},
                        {"config_digest": "sha256:" + "f" * 64}, {"checks": []}, {"status": "failed"}):
            path.write_bytes(encode({**original, **changes}))
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                self.publisher.evidence(directory, identity, "arm64")

    def test_source_verification_cannot_omit_a_stage_or_change_fixture(self):
        directory, identity = self.bundle_fixture()
        path = directory / "verification.json"
        original = json.loads(path.read_bytes())
        for changes in ({"stages": original["stages"][:-1]}, {"image_id": "sha256:" + "f" * 64}, {"status": "failed"}):
            path.write_bytes(encode({**original, **changes}))
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                self.publisher.evidence(directory, identity, "arm64")

    def test_boolean_ci_schema_refuses(self):
        self.bundle_fixture("amd64")
        directory, identity = self.bundle_fixture("arm64")
        path = directory / "ci.json"
        envelope = json.loads(path.read_bytes())
        envelope["schema"] = True
        path.write_bytes(encode(envelope))
        with self.assertRaises(RuntimeError):
            self.publisher.bundles(self.root, identity)

    def test_preflight_records_installed_tools_and_refuses_foreign_architecture(self):
        from unittest.mock import patch
        outputs = [b"podman version 5.4.2\n", b"skopeo version 1.18.0\n", b"arm64\n"]
        self.assertTrue(hasattr(self.publisher, "preflight"), "host preflight is absent")
        with patch.object(self.publisher, "execute", side_effect=outputs):
            result = self.publisher.preflight("arm64")
        self.assertEqual(result, {"podman": "5.4.2", "skopeo": "1.18.0", "architecture": "arm64"})
        with patch.object(self.publisher, "execute", side_effect=outputs), self.assertRaises(RuntimeError):
            self.publisher.preflight("amd64")

    def test_source_label_alone_cannot_seal_an_export(self):
        directory, identity = self.bundle_fixture()
        receipt = self.publisher.image_module("runtime").load_receipt(directory / "receipt.json")
        verification = json.loads((directory / "verification.json").read_bytes())
        self.assertTrue(hasattr(self.publisher, "bind_checkout"), "checkout binding is absent")
        with self.assertRaises(RuntimeError):
            self.publisher.bind_checkout(self.root, receipt, verification, identity)

    def test_remote_child_manifest_must_match_tested_digest_and_config(self):
        self.assertTrue(hasattr(self.publisher, "validate_manifest"), "remote child validation is absent")
        path, receipt = self.layouts()[0]
        actual = (path / "blobs/sha256" / receipt["image"]["manifest_digest"][7:]).read_bytes()
        self.publisher.validate_manifest(actual, receipt)
        with self.assertRaises(RuntimeError):
            self.publisher.validate_manifest(actual + b"\n", receipt)
        wrong = copy.deepcopy(receipt)
        wrong["image"]["config_digest"] = "sha256:" + "f" * 64
        with self.assertRaises(RuntimeError):
            self.publisher.validate_manifest(actual, wrong)

    def test_report_is_bounded_and_does_not_copy_raw_diagnostics(self):
        output = self.root / "report.json"
        self.publisher.write_report(output, {"status": "failed", "stderr": "private-canary" * 10000})
        self.assertLessEqual(output.stat().st_size, 65536)
        self.assertNotIn("private-canary", output.read_text())


if __name__ == "__main__":
    unittest.main(verbosity=2)
