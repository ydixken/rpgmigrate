#!/usr/bin/env python3
"""Synthetic archive controls for offline candidate identity verification."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import subprocess
import sys
import unittest
from unittest.mock import patch


PATH = Path(__file__).with_name("candidate.py")
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
CONFIG = "application/vnd.oci.image.config.v1+json"
LAYER = "application/vnd.oci.image.layer.v1.tar"
INDEX = "application/vnd.oci.image.index.v1+json"
CAPABILITIES = ["clone.single_db", "cdc.pgoutput", "progress.v1", "cutover.v1", "verify.v1", "cleanup.v1", "verify.sealed_files.v1"]


def encoded(value):
    return json.dumps(value, indent=2).encode()


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(PATH.is_file(), "candidate retention helper must exist")
        spec = importlib.util.spec_from_file_location("image_candidate", PATH)
        self.candidate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.candidate)
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / "candidate.tar"
        self.layer = b"opaque layer bytes, never extracted"
        self.config = encoded({"architecture": "arm64", "os": "linux",
                               "config": {"Labels": {"org.opencontainers.image.source": "https://github.com/ydixken/rpgmigrate",
                                                      "org.opencontainers.image.revision": "a" * 40,
                                                      "org.opencontainers.image.licenses": "Apache-2.0"}},
                               "rootfs": {"type": "layers", "diff_ids": [digest(self.layer)]}})
        self.manifest = encoded({"schemaVersion": 2, "mediaType": MANIFEST,
                                 "config": self.descriptor(self.config, CONFIG),
                                 "layers": [self.descriptor(self.layer, LAYER)]})
        self.index = encoded({"schemaVersion": 2, "mediaType": INDEX,
                              "manifests": [{**self.descriptor(self.manifest, MANIFEST), "platform": {"os": "linux", "architecture": "arm64"}}]})
        self.entries = {"oci-layout": b'{"imageLayoutVersion":"1.0.0"}', "index.json": self.index}
        for data in (self.layer, self.config, self.manifest):
            self.entries["blobs/sha256/" + digest(data)[7:]] = data
        self.write()
        self.platform = {"os": "linux", "architecture": "arm64"}
        self.source = {"commit": "a" * 40, "tree": "b" * 40}
        self.inputs = ["1" * 64 + "  Cargo.toml",
                       hashlib.sha256((PATH.parents[2] / "LICENSE").read_bytes()).hexdigest() + "  LICENSE",
                       hashlib.sha256((PATH.parents[2] / "NOTICE").read_bytes()).hexdigest() + "  NOTICE",
                       "2" * 64 + "  src/main.rs"]
        names = ("python3", "python3-minimal", "python3.13", "python3.13-minimal", "libpython3.13-stdlib",
                 "libpython3.13-minimal", "libffi8", "libc6", "libpq5", "libssl3t64", "postgresql-client-16", "postgresql-client-18")
        self.packages = {name: {"version": "1", "architecture": "arm64", "source": name, "source_version": "1",
                               "copyright_path": "/usr/share/doc/" + name + "/copyright", "copyright_sha256": "c" * 64} for name in names}
        self.packages["libpq5"]["version"] = "18.1-1.pgdg13+2"
        library = "/usr/lib/aarch64-linux-gnu/libpq.so.5.18"
        self.python = {"version": "3.13.5", "isolated": True, "ctypes": True, "private_input": True,
                       "modules": ["ctypes", "_ctypes", "os", "stat", "fcntl", "selectors", "select", "time", "json", "hashlib", "struct", "errno", "sys", "binascii", "resource"],
                       "symbols": ["PQlibVersion", "PQconnectStart", "PQconnectPoll", "PQsetnonblocking", "PQsocket", "PQstatus", "PQtransactionStatus", "PQbackendPID", "PQsetNoticeReceiver", "PQresultErrorField", "PQsendQueryParams", "PQflush", "PQconsumeInput", "PQisBusy", "PQgetResult", "PQresultStatus", "PQcmdStatus", "PQcmdTuples", "PQntuples", "PQnfields", "PQgetisnull", "PQgetlength", "PQgetvalue", "PQclear", "PQfinish", "PQescapeIdentifier", "PQfreemem"],
                       "metadata": {"version": 1, "architecture": "arm64", "library_path": library, "library_sha256": "e" * 64, "libpq_version": 180001, "package_name": "libpq5", "package_version": "18.1-1.pgdg13+2"},
                       "metadata_sha256": "e" * 64, "mapped_library": library, "packages": self.packages,
                       "python_license": {"path": "/usr/share/doc/python3/copyright", "sha256": "e" * 64},
                       "dependency_notices": {"unicode-ident-1.0.27/LICENSE-UNICODE": "f" * 64},
                       "notice_sha256": hashlib.sha256((PATH.parents[2] / "NOTICE").read_bytes()).hexdigest(),
                       "license_sha256": hashlib.sha256((PATH.parents[2] / "LICENSE").read_bytes()).hexdigest()}
        self.receipt = {"runtime_binary_sha256": "d" * 64, "python": self.python,
                        "source_inputs": self.inputs, "image_id": digest(self.config), "architecture": "arm64", "effective_uid": 65532,
                        "version": {"protocol_version": 1, "engine_version": "0.1.0", "capabilities": CAPABILITIES},
                        "libpq": {"runtime_version": 180001, "package_version": "18.1-1.pgdg13+2", "resolved_path": library, "package_owner": "libpq5:arm64: " + library, "ldd": "libpq.so.5 => " + library},
                        "clients": {"16": {"psql": "16"}, "18": {"psql": "18"}}, "default_psql": "18",
                        "build_inputs_sha256": hashlib.sha256(("\n".join(self.inputs) + "\n").encode()).hexdigest(),
                        "mounts": {"modes": {"/": "ro", "/work": "rw", "/tmp": "rw"}},
                        "checks": {"read_only_rootfs": True, "work_write": True, "tmp_write": True, "root_write_denied": True, "notices": True, "gnu_utilities": True}}
        self.smoke = encoded(self.receipt)
        self.lock = {"schema_version": 1, "archive_sha256": hashlib.sha256(self.path.read_bytes()).hexdigest(),
                     "archive_index_digest": digest(self.index), "upstream_index_digest": None,
                     "platform_manifest_digest": digest(self.manifest), "config_digest": digest(self.config), "config_variant": None, "image_id": digest(self.config),
                     "layer_digests": [digest(self.layer)], "platform": self.platform,
                     "source_commit": self.source["commit"], "source_tree": self.source["tree"],
                     "runtime_binary_sha256": "d" * 64, "build_inputs_sha256": self.receipt["build_inputs_sha256"],
                     "source_inputs": self.inputs, "packages": self.packages, "smoke_sha256": hashlib.sha256(self.smoke).hexdigest()}

    def descriptor(self, data, media):
        return {"mediaType": media, "digest": digest(data), "size": len(data)}

    def write(self, extras=()):
        with tarfile.open(self.path, "w", format=tarfile.USTAR_FORMAT) as archive:
            for name, data in list(self.entries.items()) + list(extras):
                item = tarfile.TarInfo(name)
                item.size = len(data)
                archive.addfile(item, io.BytesIO(data))

    def verify(self):
        return self.candidate.verify_candidate(self.path, self.lock, self.smoke, self.source)

    def replace_config(self, changes):
        config = encoded({**json.loads(self.config), **changes})
        manifest = encoded({**json.loads(self.manifest), "config": self.descriptor(config, CONFIG)})
        index = encoded({"schemaVersion": 2, "mediaType": INDEX,
                         "manifests": [{**self.descriptor(manifest, MANIFEST), "platform": self.platform}]})
        self.entries.pop("blobs/sha256/" + digest(self.config)[7:])
        self.entries.pop("blobs/sha256/" + digest(self.manifest)[7:])
        self.entries["blobs/sha256/" + digest(config)[7:]] = config
        self.entries["blobs/sha256/" + digest(manifest)[7:]] = manifest
        self.entries["index.json"] = index
        self.write()
        return config, manifest, index

    def test_current_python_schema_binds_official_apache_license(self):
        self.assertEqual(len(self.python), 14)
        self.assertEqual(self.python["license_sha256"], "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30")
        self.assertEqual(self.verify()["source_inputs"], self.inputs)
        self.assertEqual(self.candidate.verify_loaded(self.lock, self.smoke)["image_id"], digest(self.config))

    def test_refuses_legacy_python_schema_and_license_substitution(self):
        legacy = {key: value for key, value in self.python.items() if key != "license_sha256"}
        for python in (legacy, {**self.python, "license_sha256": "0" * 64},
                       {**self.python, "license_sha256": None}, {**self.python, "extra": True}):
            smoke = encoded({**self.receipt, "python": python})
            expected = {**self.lock, "smoke_sha256": hashlib.sha256(smoke).hexdigest()}
            with self.subTest(python=python), self.assertRaises(RuntimeError):
                self.candidate.verify_candidate(self.path, expected, smoke, self.source)
            with self.subTest(loaded=python), self.assertRaises(RuntimeError):
                self.candidate.verify_loaded(expected, smoke)

    def test_refuses_missing_or_substituted_license_source_inventory(self):
        for inputs in ([value for value in self.inputs if not value.endswith("  LICENSE")],
                       ["0" * 64 + "  LICENSE" if value.endswith("  LICENSE") else value for value in self.inputs]):
            build_hash = hashlib.sha256(("\n".join(inputs) + "\n").encode()).hexdigest()
            smoke = encoded({**self.receipt, "source_inputs": inputs, "build_inputs_sha256": build_hash})
            expected = {**self.lock, "source_inputs": inputs, "build_inputs_sha256": build_hash,
                        "smoke_sha256": hashlib.sha256(smoke).hexdigest()}
            with self.subTest(inputs=inputs), self.assertRaises(RuntimeError):
                self.candidate.verify_candidate(self.path, expected, smoke, self.source)
            with self.subTest(loaded=inputs), self.assertRaises(RuntimeError):
                self.candidate.verify_loaded(expected, smoke)

    def test_refuses_archive_source_revision_or_license_label_substitution(self):
        original = dict(self.entries)
        labels = json.loads(self.config)["config"]["Labels"]
        for name, value in (("org.opencontainers.image.source", "https://example.invalid/foreign"),
                            ("org.opencontainers.image.revision", "0" * 40),
                            ("org.opencontainers.image.licenses", "MIT"),
                            ("org.opencontainers.image.licenses", None)):
            self.entries = dict(original)
            changed = {**labels, name: value}
            if value is None:
                changed.pop(name)
            config, manifest, index = self.replace_config({"config": {"Labels": changed}})
            smoke = encoded({**self.receipt, "image_id": digest(config)})
            expected = {**self.lock, "archive_sha256": hashlib.sha256(self.path.read_bytes()).hexdigest(),
                        "config_digest": digest(config), "image_id": digest(config),
                        "platform_manifest_digest": digest(manifest), "archive_index_digest": digest(index),
                        "smoke_sha256": hashlib.sha256(smoke).hexdigest()}
            with self.subTest(name=name, value=value), self.assertRaises(RuntimeError):
                self.candidate.verify_candidate(self.path, expected, smoke, self.source)

    def test_arm64_v8_preserves_raw_variant_and_digest_without_changing_profile(self):
        config, manifest, index = self.replace_config({"variant": "v8"})
        facts = self.candidate.inspect_archive(self.path, self.platform)
        self.assertEqual(facts["config_variant"], "v8")
        self.assertEqual(facts["platform"], {"os": "linux", "architecture": "arm64"})
        self.assertEqual(facts["config_digest"], digest(config))
        self.assertEqual(facts["image_id"], digest(config))
        self.assertEqual(facts["platform_manifest_digest"], digest(manifest))
        self.assertEqual(facts["archive_index_digest"], digest(index))

    def test_v8_candidate_requires_independently_pinned_raw_variant(self):
        config, manifest, index = self.replace_config({"variant": "v8"})
        receipt = {**self.receipt, "image_id": digest(config)}
        smoke = encoded(receipt)
        expected = {**self.lock, "archive_sha256": hashlib.sha256(self.path.read_bytes()).hexdigest(),
                    "config_digest": digest(config), "image_id": digest(config), "config_variant": "v8",
                    "platform_manifest_digest": digest(manifest), "archive_index_digest": digest(index),
                    "smoke_sha256": hashlib.sha256(smoke).hexdigest()}
        result = self.candidate.verify_candidate(self.path, expected, smoke, self.source)
        self.assertEqual(result["config_variant"], "v8")
        with self.assertRaises(RuntimeError):
            self.candidate.verify_candidate(self.path, {**expected, "config_variant": None}, smoke, self.source)
        with self.assertRaises(RuntimeError):
            self.candidate.verify_loaded({**expected, "config_variant": "v9"}, smoke)
        with self.assertRaises(RuntimeError):
            self.candidate.verify_loaded({**expected, "platform": {"os": "linux", "architecture": "amd64"}}, smoke)

    def test_absent_arm64_variant_is_retained_separately(self):
        facts = self.candidate.inspect_archive(self.path, self.platform)
        self.assertIn("config_variant", facts)
        self.assertIsNone(facts["config_variant"])

    def test_refuses_explicit_null_ill_typed_or_unknown_variants_and_architecture_drift(self):
        original = dict(self.entries)
        changes = [{"variant": value} for value in (None, True, False, 8, 8.0, [], {}, "", "V8", "v8.1", "v7", "v9", "unknown")]
        changes += [{"architecture": "amd64", "variant": "v8"}, {"os": "windows", "variant": "v8"}]
        for mutation in changes:
            self.entries = dict(original)
            self.replace_config(mutation)
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                self.candidate.inspect_archive(self.path, self.platform)
        self.entries = dict(original)
        self.platform = {"os": "linux", "architecture": "amd64"}
        self.replace_config({"architecture": "amd64"})
        facts = self.candidate.inspect_archive(self.path, self.platform)
        self.assertIsNone(facts["config_variant"])
        self.assertEqual(facts["platform"], self.platform)
        for variant in (None, "v1", "v2", "v8"):
            self.entries = dict(original)
            self.replace_config({"architecture": "amd64", "variant": variant})
            with self.subTest(amd64_variant=variant), self.assertRaises(RuntimeError):
                self.candidate.inspect_archive(self.path, self.platform)

    def test_retains_raw_digest_domains_without_inventing_upstream_index(self):
        facts = self.verify()
        for name in ("archive_sha256", "archive_index_digest", "platform_manifest_digest", "config_digest", "config_variant", "image_id", "layer_digests", "platform"):
            self.assertEqual(facts[name], self.lock[name])
        self.assertIsNone(facts["upstream_index_digest"])
        self.assertNotEqual(digest(self.manifest), digest(json.dumps(json.loads(self.manifest)).encode()))
        self.lock["upstream_index_digest"] = "sha256:" + "e" * 64
        self.assertEqual(self.verify()["upstream_index_digest"], "sha256:" + "e" * 64)

    def test_refuses_missing_or_changed_expected_and_observed_identities(self):
        for name in tuple(self.lock):
            with self.subTest(missing=name):
                previous = self.lock.pop(name)
                with self.assertRaises(RuntimeError):
                    self.verify()
                self.lock[name] = previous
        for name in ("archive_sha256", "archive_index_digest", "platform_manifest_digest", "config_digest", "image_id", "runtime_binary_sha256", "build_inputs_sha256", "source_commit", "source_tree", "smoke_sha256"):
            with self.subTest(changed=name):
                previous = self.lock[name]
                self.lock[name] = "sha256:" + "0" * 64 if previous.startswith("sha256:") else "0" * len(previous)
                with self.assertRaises(RuntimeError):
                    self.verify()
                self.lock[name] = previous
        for name, value in (("source_inputs", []), ("packages", {}), ("layer_digests", ["sha256:" + "0" * 64]), ("platform", {"os": "linux", "architecture": "amd64"})):
            with self.subTest(changed=name):
                previous = self.lock[name]
                self.lock[name] = value
                with self.assertRaises(RuntimeError):
                    self.verify()
                self.lock[name] = previous
        for name in self.source:
            with self.subTest(source=name), self.assertRaises(RuntimeError):
                self.candidate.verify_candidate(self.path, self.lock, self.smoke, {**self.source, name: "0" * 40})

    def test_refuses_smoke_substitution_even_with_revised_receipt_hash(self):
        changes = {"image_id": "sha256:" + "f" * 64, "runtime_binary_sha256": "f" * 64,
                   "architecture": "amd64", "build_inputs_sha256": "f" * 64, "source_inputs": [],
                   "python": {"packages": {}}, "version": {"protocol_version": 1, "engine_version": "0.1.0", "capabilities": []},
                   "effective_uid": 0, "checks": {"read_only_rootfs": True}}
        for name, value in changes.items():
            with self.subTest(name=name):
                data = encoded({**self.receipt, name: value})
                lock = {**self.lock, "smoke_sha256": hashlib.sha256(data).hexdigest()}
                with self.assertRaises(RuntimeError):
                    self.candidate.verify_candidate(self.path, lock, data, self.source)
        with self.assertRaises(RuntimeError):
            self.candidate.verify_candidate(self.path, self.lock, self.smoke + b"\n", self.source)

    def test_requires_complete_producer_python_and_smoke_observations(self):
        for key in tuple(self.receipt):
            receipt = {name: value for name, value in self.receipt.items() if name != key}
            data = encoded(receipt)
            with self.subTest(missing=key), self.assertRaises(RuntimeError):
                self.candidate.verify_candidate(self.path, {**self.lock, "smoke_sha256": hashlib.sha256(data).hexdigest()}, data, self.source)
        for key in tuple(self.python):
            receipt = {**self.receipt, "python": {name: value for name, value in self.python.items() if name != key}}
            data = encoded(receipt)
            with self.subTest(python_missing=key), self.assertRaises(RuntimeError):
                self.candidate.verify_candidate(self.path, {**self.lock, "smoke_sha256": hashlib.sha256(data).hexdigest()}, data, self.source)

    def test_consumer_accepts_new_volatile_receipt_with_same_immutable_facts(self):
        self.assertTrue(hasattr(self.candidate, "verify_loaded"), "consumer verification must exist")
        receipt = {**self.receipt, "mounts": {"modes": {"/": "ro,new-mount-id", "/work": "rw", "/tmp": "rw"}},
                   "libpq": {**self.receipt["libpq"], "ldd": "libpq.so.5 => mapped at another address"}}
        data = encoded(receipt)
        self.assertNotEqual(hashlib.sha256(data).hexdigest(), self.lock["smoke_sha256"])
        result = self.candidate.verify_loaded(self.lock, data)
        self.assertEqual(result["consumer_smoke_sha256"], hashlib.sha256(data).hexdigest())
        self.assertEqual(result["image_id"], digest(self.config))
        self.assertEqual(result["platform"], self.platform)
        self.assertEqual(self.lock["smoke_sha256"], hashlib.sha256(self.smoke).hexdigest())

    def test_consumer_refuses_mutated_immutable_facts_and_unadmitted_lock(self):
        self.assertTrue(hasattr(self.candidate, "verify_loaded"), "consumer verification must exist")
        changes = {"image_id": "sha256:" + "f" * 64, "runtime_binary_sha256": "f" * 64,
                   "architecture": "amd64", "source_inputs": self.inputs[:-1], "build_inputs_sha256": "f" * 64,
                   "python": {**self.python, "packages": {**self.packages, "libpq5": {**self.packages["libpq5"], "version": "18.2"}}},
                   "version": {"protocol_version": 1, "engine_version": "0.1.0", "capabilities": CAPABILITIES[:-1]},
                   "checks": {**self.receipt["checks"], "root_write_denied": False}}
        for key, value in changes.items():
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.candidate.verify_loaded(self.lock, encoded({**self.receipt, key: value}))
        for key in tuple(self.lock):
            with self.subTest(missing=key), self.assertRaises(RuntimeError):
                self.candidate.verify_loaded({name: value for name, value in self.lock.items() if name != key}, self.smoke)

    def test_rejects_duplicate_traversing_unknown_and_nonregular_members(self):
        for name in ("index.json", "../escape", "/escape", "./index.json", "blobs//sha256/" + "1" * 64, "unexpected"):
            with self.subTest(name=name):
                self.write([(name, b"x")])
                with self.assertRaises(RuntimeError):
                    self.candidate.inspect_archive(self.path, self.platform)
        for kind in (tarfile.SYMTYPE, tarfile.LNKTYPE, tarfile.FIFOTYPE, tarfile.XHDTYPE):
            with self.subTest(kind=kind):
                self.write()
                with tarfile.open(self.path, "a", format=tarfile.USTAR_FORMAT) as archive:
                    item = tarfile.TarInfo("unsafe")
                    item.type = kind
                    item.linkname = "index.json"
                    archive.addfile(item)
                with self.assertRaises(RuntimeError):
                    self.candidate.inspect_archive(self.path, self.platform)

    def test_rejects_empty_missing_substituted_truncated_and_oversized_archives(self):
        for value in (b"", b"\0" * 10240, self.path.read_bytes()[:700], self.path.read_bytes() + b"hostile trailer"):
            self.path.write_bytes(value)
            with self.assertRaises(RuntimeError):
                self.candidate.inspect_archive(self.path, self.platform)
        self.entries.pop("blobs/sha256/" + digest(self.layer)[7:])
        self.write()
        with self.assertRaises(RuntimeError):
            self.candidate.inspect_archive(self.path, self.platform)
        self.entries["blobs/sha256/" + digest(self.layer)[7:]] = b"x" * len(self.layer)
        self.write()
        with self.assertRaises(RuntimeError):
            self.candidate.inspect_archive(self.path, self.platform)
        self.write()
        with patch.object(self.candidate, "MAX_ARCHIVE_BYTES", 512), self.assertRaises(RuntimeError):
            self.candidate.inspect_archive(self.path, self.platform)

    def test_refuses_ambiguous_platforms_manifests_and_media_types(self):
        index = json.loads(self.index)
        descriptor = index["manifests"][0]
        for manifests in ([], [descriptor, descriptor], [{**descriptor, "mediaType": INDEX}],
                          [{**descriptor, "platform": {"os": "linux", "architecture": "amd64"}}],
                          [{**descriptor, "size": True}], [{**descriptor, "digest": "sha256:" + "f" * 64}]):
            with self.subTest(manifests=manifests):
                self.entries["index.json"] = encoded({**index, "manifests": manifests})
                self.write()
                with self.assertRaises(RuntimeError):
                    self.candidate.inspect_archive(self.path, self.platform)
        self.entries["index.json"] = self.index.replace(b'"schemaVersion": 2,', b'"schemaVersion": 2,"schemaVersion": 2,')
        self.write()
        with self.assertRaises(RuntimeError):
            self.candidate.inspect_archive(self.path, self.platform)

    def test_bounds_metadata_entries_and_layers_before_acceptance(self):
        for bound in ("MAX_JSON_BYTES", "MAX_ENTRIES", "MAX_LAYERS", "MAX_BLOB_BYTES"):
            with self.subTest(bound=bound), patch.object(self.candidate, bound, 0), self.assertRaises(RuntimeError):
                self.candidate.inspect_archive(self.path, self.platform)

    def test_refuses_invalid_config_layer_and_descriptor_graphs(self):
        for key, value in (("layers", []), ("layers", [self.descriptor(self.layer, INDEX)]),
                           ("config", self.descriptor(self.config, MANIFEST))):
            manifest = encoded({**json.loads(self.manifest), key: value})
            entries = dict(self.entries)
            entries.pop("blobs/sha256/" + digest(self.manifest)[7:])
            entries["blobs/sha256/" + digest(manifest)[7:]] = manifest
            entries["index.json"] = encoded({"schemaVersion": 2, "manifests": [self.descriptor(manifest, MANIFEST)]})
            original, self.entries = self.entries, entries
            self.write()
            with self.subTest(key=key, value=value), self.assertRaises(RuntimeError):
                self.candidate.inspect_archive(self.path, self.platform)
            self.entries = original
        extra = b"unreferenced blob"
        self.write([("blobs/sha256/" + digest(extra)[7:], extra)])
        with self.assertRaises(RuntimeError):
            self.candidate.inspect_archive(self.path, self.platform)

    def test_refuses_non_json_constants_and_duplicate_receipt_fields(self):
        for data in (b'{"a":NaN}', b'{"a":Infinity}', b'{"a":-Infinity}', b'{"a":1,"a":2}', b'{}' * 2, b'[' * 2000):
            with self.subTest(data=data[:30]), self.assertRaises(RuntimeError):
                self.candidate.decode(data)
        data = self.smoke.replace(b'"effective_uid": 65532,', b'"effective_uid": 65532,"effective_uid": 65532,')
        with self.assertRaises(RuntimeError):
            self.candidate.verify_candidate(self.path, {**self.lock, "smoke_sha256": hashlib.sha256(data).hexdigest()}, data, self.source)

    def test_missing_smoke_contract_fails_without_private_diagnostic(self):
        with patch.object(self.candidate.importlib.util, "spec_from_file_location", side_effect=FileNotFoundError("private source path")):
            with self.assertRaises(RuntimeError) as error:
                self.candidate.verify_loaded(self.lock, self.smoke)
        self.assertNotIn("private source path", str(error.exception))

    def test_json_file_inputs_refuse_symlinks_special_files_and_oversize(self):
        source = Path(self.temporary.name) / "input.json"
        source.write_bytes(b"{}")
        self.assertEqual(self.candidate.read_json_bytes(source), b"{}")
        link = source.with_name("link.json")
        link.symlink_to(source)
        with self.assertRaises(RuntimeError):
            self.candidate.read_json_bytes(link)
        source.write_bytes(b" " * (self.candidate.MAX_JSON_BYTES + 1))
        with self.assertRaises(RuntimeError):
            self.candidate.read_json_bytes(source)
        source.unlink()
        os.mkfifo(source)
        command = [sys.executable, str(PATH), "--archive", str(self.path), "--expected", str(source), "--smoke", str(source), "--source-identity", str(source)]
        result = subprocess.run(command, text=True, capture_output=True, timeout=2)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("Traceback", result.stderr)

    def test_cli_verifies_packet_and_refuses_substitution_without_runtime(self):
        paths = {}
        for name, data in (("expected", encoded(self.lock)), ("smoke", self.smoke), ("source-identity", encoded(self.source))):
            paths[name] = Path(self.temporary.name) / (name + ".json")
            paths[name].write_bytes(data)
        argv = [sys.executable, str(PATH), "--archive", str(self.path)]
        for name, path in paths.items():
            argv += ["--" + name, str(path)]
        result = subprocess.run(argv, text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["platform_manifest_digest"], digest(self.manifest))
        paths["smoke"].write_bytes(b"{}")
        result = subprocess.run(argv, text=True, capture_output=True, timeout=10)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertNotIn("Traceback", result.stderr)

    def test_argv_builders_pin_image_id_and_local_archive_without_execution(self):
        image = "sha256:" + "a" * 64
        self.assertEqual(self.candidate.export_argv(image, self.path), ["podman", "image", "save", "--format=oci-archive", "--output", str(self.path), image])
        self.assertEqual(self.candidate.import_argv(self.path), ["podman", "image", "load", "--input", str(self.path)])
        for image in ("localhost/name:tag", "--help", "a" * 64, ""):
            with self.subTest(image=image), self.assertRaises(RuntimeError):
                self.candidate.export_argv(image, self.path)
        for path in ("https://example.invalid/image.tar", "-input", ""):
            with self.subTest(path=path), self.assertRaises(RuntimeError):
                self.candidate.import_argv(path)


if __name__ == "__main__":
    unittest.main()
