#!/usr/bin/env python3
"""Offline controls for exact native development publication."""

import base64
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import io
import urllib.error
import types
import unittest
from unittest.mock import patch


PATH = Path(__file__).with_name("dev_publish.py")
COMMIT = "1" * 40
SOURCE = {"repository": "https://github.com/ydixken/rpgmigrate", "commit": COMMIT,
          "git_tree": "2" * 40, "payload_inventory_sha256": "3" * 64}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(PATH.is_file(), "development publisher must exist")
        spec = importlib.util.spec_from_file_location("dev_publish", PATH)
        self.dev = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.dev)
        self.config = {"architecture": "arm64", "os": "linux", "rootfs": {"type": "layers", "diff_ids": []},
                       "config": {"Labels": {"org.opencontainers.image.source": SOURCE["repository"],
                                              "org.opencontainers.image.revision": COMMIT,
                                              "org.opencontainers.image.licenses": "Apache-2.0"}}}
        self.config_raw = encoded(self.config)
        self.layer = b"compressed layer fixture"
        self.manifest = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                         "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                                    "digest": digest(self.config_raw), "size": len(self.config_raw)},
                         "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                                     "digest": digest(self.layer), "size": len(self.layer)}]}
        self.raw = encoded(self.manifest)
        self.smoke = {"image_id": digest(self.config_raw), "architecture": "arm64",
                      "build_inputs_sha256": "4" * 64, "runtime_binary_sha256": "5" * 64,
                      "python": {"notice_sha256": "6" * 64, "license_sha256": "7" * 64,
                                 "dependency_notices": {"unicode-ident-1.0.27/LICENSE-UNICODE": "8" * 64}}}

    def graph(self):
        return self.dev.validate_graph(self.raw, self.config_raw, "arm64", SOURCE, self.smoke)

    def test_tags_are_exact_commit_and_single_platform_only(self):
        self.assertEqual(self.dev.tag_for(COMMIT, "arm64"), "dev-" + COMMIT + "-arm64")
        for commit, arch in ((COMMIT[:12], "arm64"), ("A" * 40, "amd64"), (COMMIT, "x86_64")):
            with self.subTest(commit=commit, arch=arch), self.assertRaises(RuntimeError):
                self.dev.tag_for(commit, arch)

    def test_graph_binds_raw_manifest_config_source_and_smoked_id(self):
        graph = self.graph()
        self.assertEqual(graph["manifest_digest"], digest(self.raw))
        self.assertEqual(graph["config_digest"], self.smoke["image_id"])
        self.assertEqual(graph["index_digest"], None)
        self.assertEqual(base64.b64decode(graph["manifest_base64"]), self.raw)
        self.assertEqual(graph["variant"], None)

    def test_graph_rejects_wrong_source_platform_index_and_duplicate_fields(self):
        changes = [("architecture", "amd64"), ("os", "windows"), ("variant", "v9")]
        for key, value in changes:
            config = {**self.config, key: value}
            raw = encoded(config)
            manifest = copy.deepcopy(self.manifest)
            manifest["config"].update(digest=digest(raw), size=len(raw))
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.dev.validate_graph(encoded(manifest), raw, "arm64", SOURCE, self.smoke)
        for key in ("org.opencontainers.image.source", "org.opencontainers.image.revision", "org.opencontainers.image.licenses"):
            config = copy.deepcopy(self.config)
            config["config"]["Labels"][key] = "wrong"
            raw = encoded(config)
            manifest = copy.deepcopy(self.manifest)
            manifest["config"].update(digest=digest(raw), size=len(raw))
            with self.subTest(label=key), self.assertRaises(RuntimeError):
                self.dev.validate_graph(encoded(manifest), raw, "arm64", SOURCE, self.smoke)
        for raw in (self.raw.replace(b'"schemaVersion":2', b'"schemaVersion":2,"schemaVersion":2'),
                    encoded({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": []}),
                    self.raw + b"{}"):
            with self.assertRaises(RuntimeError):
                self.dev.validate_graph(raw, self.config_raw, "arm64", SOURCE, self.smoke)

    def test_descriptor_hash_sizes_and_media_types_cannot_be_substituted(self):
        for key, value in (("digest", "sha256:" + "0" * 64), ("size", True), ("size", 0),
                           ("mediaType", "application/vnd.oci.image.index.v1+json")):
            manifest = copy.deepcopy(self.manifest)
            manifest["config"][key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.dev.validate_graph(encoded(manifest), self.config_raw, "arm64", SOURCE, self.smoke)

    def test_source_inventory_matches_existing_guard_encoding_and_refuses_dirty(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-b", "feature"], cwd=root, check=True, capture_output=True)
            subprocess.run(["git", "config", "user.name", "Fixture"], cwd=root, check=True)
            subprocess.run(["git", "config", "user.email", "fixture@example.invalid"], cwd=root, check=True)
            subprocess.run(["git", "remote", "add", "origin", SOURCE["repository"] + ".git"], cwd=root, check=True)
            (root / "input").write_bytes(b"source")
            subprocess.run(["git", "add", "input"], cwd=root, check=True)
            subprocess.run(["git", "-c", "commit.gpgsign=false", "commit", "-m", "fixture"], cwd=root, check=True, capture_output=True)
            head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
            value = self.dev.source_identity(root, head)
            inventory = [["input", 0o644, hashlib.sha256(b"source").hexdigest()]]
            self.assertEqual(value["payload_inventory_sha256"], hashlib.sha256(json.dumps(inventory, separators=(",", ":")).encode()).hexdigest())
            subprocess.run(["git", "update-index", "--assume-unchanged", "input"], cwd=root, check=True)
            (root / "input").write_bytes(b"dirty")
            with self.assertRaisesRegex(RuntimeError, "SOURCE"):
                self.dev.source_identity(root, head)

    def publisher(self, registry, directory, **options):
        return self.dev.Publisher(registry, Path(directory), SOURCE, "arm64", **options)

    def test_present_tag_without_exact_retained_receipt_never_builds_or_pushes(self):
        class Registry:
            def package(self, bootstrap=False): return {"visibility": "public"}
            def lookup(registry, tag): return self.graph()
        with tempfile.TemporaryDirectory() as directory:
            publisher = self.publisher(Registry(), directory)
            with patch.object(publisher, "build_and_smoke") as build, patch.object(publisher, "push") as push:
                with self.assertRaisesRegex(RuntimeError, "retained"):
                    publisher.execute()
                build.assert_not_called()
                push.assert_not_called()

    def test_unknown_lookup_never_means_absence(self):
        class Registry:
            def package(self, bootstrap=False): return {"visibility": "public"}
            def lookup(self, tag): raise RuntimeError("REGISTRY_UNCERTAIN")
        with tempfile.TemporaryDirectory() as directory:
            publisher = self.publisher(Registry(), directory)
            with patch.object(publisher, "build_and_smoke") as build, self.assertRaises(RuntimeError):
                publisher.execute()
            build.assert_not_called()

    def test_missing_package_ci_refuses_without_first_creation_authority(self):
        class Registry:
            def package(self, bootstrap=False):
                raise RuntimeError("FIRST_PACKAGE_HOST_ONLY")
        with tempfile.TemporaryDirectory() as directory:
            publisher = self.publisher(Registry(), directory)
            with patch.object(publisher, "build_and_smoke") as build, self.assertRaisesRegex(RuntimeError, "HOST_ONLY"):
                publisher.execute()
            build.assert_not_called()

    def test_second_lookup_collision_and_unknown_push_are_not_retried(self):
        for collision in (True, False):
            class Registry:
                calls = 0
                def package(self, bootstrap=False): return {"visibility": "public"}
                def lookup(registry, tag):
                    registry.calls += 1
                    return self.graph() if collision and registry.calls == 2 else None
                def raw_graph(registry, reference): return self.graph()
            with tempfile.TemporaryDirectory() as directory:
                publisher = self.publisher(Registry(), directory)
                with patch.object(publisher, "build_and_smoke", return_value=self.smoke), \
                     patch.object(publisher, "recheck_source"), \
                     patch.object(publisher, "push", side_effect=RuntimeError("PUSH_OUTCOME_UNKNOWN")) as push:
                    with self.assertRaises(RuntimeError): publisher.execute()
                    self.assertEqual(push.call_count, 0 if collision else 1)
                    if not collision:
                        self.assertEqual(json.loads((Path(directory) / "publication.json").read_text())["status"], "PUSH_OUTCOME_UNKNOWN")

    def test_publicity_pending_retains_exact_image_then_verifies_without_push(self):
        class Registry:
            calls = 0
            def package(self, bootstrap=False): return {"visibility": "public"}
            def lookup(registry, tag):
                registry.calls += 1
                return self.graph() if registry.calls >= 3 else None
            def raw_graph(registry, reference): return self.graph()
            def public(registry, graph): raise RuntimeError("PRIVATE_OR_UNAVAILABLE")
        with tempfile.TemporaryDirectory() as directory:
            publisher = self.publisher(Registry(), directory)
            with patch.object(publisher, "build_and_smoke", return_value=self.smoke), \
                 patch.object(publisher, "recheck_source"), patch.object(publisher, "push", return_value=digest(self.raw)):
                receipt = publisher.execute()
            self.assertEqual(receipt["status"], "PUBLICITY_PENDING")
            self.assertEqual(receipt["manifest_digest"], digest(self.raw))
            self.assertIsNone(receipt["resolved_image"])
            public = {"graph": self.graph(), "blobs": [{"digest": digest(self.layer), "size": len(self.layer)}]}
            with patch.object(self.dev.GHCR, "public", return_value=public), patch.object(self.dev.Publisher, "push") as push:
                result = self.dev.verify_public(receipt)
                self.assertEqual(result["status"], "PUBLIC")
                self.assertEqual(result["resolved_image"]["source"]["commit"], COMMIT)
                push.assert_not_called()

    def test_secret_subprocess_output_is_discarded_and_push_retry_is_disabled(self):
        with tempfile.TemporaryDirectory() as directory:
            publisher = self.publisher(types.SimpleNamespace(token="canary-secret", username="ydixken"), directory)
            publisher.smoke = self.smoke
            with patch.object(self.dev.subprocess, "run", side_effect=subprocess.CalledProcessError(1, [], output="canary-secret", stderr="canary-secret")):
                with self.assertRaises(RuntimeError) as failure:
                    publisher.push()
                self.assertNotIn("canary-secret", str(failure.exception))

    def test_ghcr_package_missing_requires_fresh_owner_admin_scope_and_host(self):
        registry = self.dev.GHCR(SOURCE, "arm64", "canary-secret", "ydixken", True)
        responses = [(404, {}, b"{}"), (200, {"X-OAuth-Scopes": "repo, write:packages"}, b'{"login":"ydixken"}'),
                     (200, {}, b'{"full_name":"ydixken/rpgmigrate","permissions":{"admin":true}}')]
        with patch.dict(os.environ, {}, clear=True), patch.object(registry, "api", side_effect=responses):
            authority = registry.package(bootstrap=True)
        self.assertEqual(authority["visibility"], "absent")
        self.assertTrue(authority["authority"]["repository_admin"])
        for position, replacement in ((0, (403, {}, b"private-secret")),
                                       (1, (200, {"X-OAuth-Scopes": "repo"}, b'{"login":"ydixken"}')),
                                       (1, (200, {"X-OAuth-Scopes": "write:packages"}, b'{"login":"other"}')),
                                       (2, (200, {}, b'{"full_name":"ydixken/rpgmigrate","permissions":{"admin":false}}'))):
            invalid = list(responses)
            invalid[position] = replacement
            with self.subTest(position=position), patch.dict(os.environ, {}, clear=True), \
                 patch.object(registry, "api", side_effect=invalid), self.assertRaises(RuntimeError):
                registry.package(bootstrap=True)
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}), \
             patch.object(registry, "api", return_value=(404, {}, b"{}")), self.assertRaisesRegex(RuntimeError, "HOST_ONLY"):
            registry.package(bootstrap=True)

    def test_existing_package_requires_exact_repository_link_and_public_visibility(self):
        registry = self.dev.GHCR(SOURCE, "arm64", "secret", "actor")
        valid = {"name": "rpgmigrate", "package_type": "container", "repository": {"full_name": "ydixken/rpgmigrate"}, "visibility": "public"}
        for change in ({"name": "other"}, {"repository": {"full_name": "other/rpgmigrate"}}, {"visibility": "private"}):
            with patch.object(registry, "api", return_value=(200, {}, encoded({**valid, **change}))), self.assertRaises(RuntimeError):
                registry.package()
        with patch.object(registry, "api", return_value=(200, {}, encoded(valid))):
            self.assertEqual(registry.package()["visibility"], "public")

    def test_manifest_absence_requires_exact_typed_authenticated_not_found(self):
        registry = self.dev.GHCR(SOURCE, "arm64", "secret", "actor")
        registry.namespace_accessible = True
        with patch.object(registry, "auth_headers", return_value={}), \
             patch.object(registry, "http", return_value=(404, {}, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}')):
            self.assertIsNone(registry.lookup(self.dev.tag_for(COMMIT, "arm64")))
        for status, raw in ((401, b"private-secret"), (403, b"private-secret"), (429, b"rate-limit"),
                             (500, b"failed"), (404, b'{}'), (404, b'{"errors":[{"code":"NAME_UNKNOWN"}]}')):
            with self.subTest(status=status), patch.object(registry, "auth_headers", return_value={}), \
                 patch.object(registry, "http", return_value=(status, {}, raw)), self.assertRaises(RuntimeError) as failure:
                registry.lookup(self.dev.tag_for(COMMIT, "arm64"))
            self.assertNotIn("private-secret", str(failure.exception))

    def test_http_network_failure_is_unknown_and_redirects_drop_authentication(self):
        registry = self.dev.GHCR(SOURCE, "arm64", "secret", "actor")
        with patch.object(registry.opener, "open", side_effect=urllib.error.URLError("secret-canary")):
            with self.assertRaisesRegex(RuntimeError, "HTTP_UNCERTAIN") as failure:
                registry.http("https://ghcr.io/v2/")
            self.assertNotIn("secret-canary", str(failure.exception))
        class Response(io.BytesIO):
            status = 200
            headers = {}
        redirect = urllib.error.HTTPError("https://ghcr.io/blob", 307, "redirect", {"Location": "https://example.invalid/blob"}, io.BytesIO())
        response = Response(self.layer)
        with patch.object(registry.opener, "open", side_effect=[redirect, response]) as request:
            _, _, result = registry.http("https://ghcr.io/blob", headers={"Authorization": "Bearer secret"},
                                         blob={"digest": digest(self.layer), "size": len(self.layer)}, limit=len(self.layer))
        self.assertEqual(result["digest"], digest(self.layer))
        self.assertFalse(request.call_args_list[1].args[0].has_header("Authorization"))
        with patch.object(registry.opener, "open", return_value=Response(b"wrong")), self.assertRaises(RuntimeError):
            registry.http("https://ghcr.io/blob", blob={"digest": digest(self.layer), "size": len(self.layer)}, limit=len(self.layer))

    def test_exact_retained_tag_is_reused_but_changed_graph_never_written(self):
        class Registry:
            calls = 0
            def package(self, bootstrap=False): return {"visibility": "public"}
            def lookup(registry, tag):
                registry.calls += 1
                return self.graph() if registry.calls >= 3 else None
            def raw_graph(registry, reference): return self.graph()
            def public(registry, graph): return {"graph": graph, "blobs": [{"digest": digest(self.layer), "size": len(self.layer)}]}
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            publisher = self.publisher(Registry(), first)
            with patch.object(publisher, "build_and_smoke", return_value=self.smoke), patch.object(publisher, "recheck_source"), \
                 patch.object(publisher, "push", return_value=digest(self.raw)):
                receipt = publisher.execute()
            registry = Registry()
            registry.calls = 3
            publisher = self.publisher(registry, second, retained=receipt)
            with patch.object(publisher, "build_and_smoke") as build, patch.object(publisher, "push") as push:
                self.assertEqual(publisher.execute()["status"], "PUBLIC")
                build.assert_not_called()
                push.assert_not_called()
        for key in ("manifest_digest", "config_digest", "smoke_receipt_sha256", "build_input_sha256"):
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                self.dev.validate_retained({**receipt, key: "0" * 64})

    def test_push_uses_immutable_id_private_ephemeral_auth_and_no_hidden_retries(self):
        registry = types.SimpleNamespace(token="secret-canary", username="ydixken")
        with tempfile.TemporaryDirectory() as directory:
            publisher = self.publisher(registry, directory)
            publisher.smoke = self.smoke
            def invoke(argv, **kwargs):
                self.assertIn("--retry=0", argv)
                self.assertIn("--tls-verify=true", argv)
                self.assertEqual(argv[-2], self.smoke["image_id"])
                self.assertEqual(argv[-1], "docker://ghcr.io/ydixken/rpgmigrate:dev-" + COMMIT + "-arm64")
                authfile = Path(argv[argv.index("--authfile") + 1])
                self.assertEqual(authfile.stat().st_mode & 0o777, 0o600)
                self.assertEqual(authfile.parent.stat().st_mode & 0o777, 0o700)
                self.assertNotIn("GITHUB_TOKEN", kwargs["env"])
                self.assertNotIn("secret-canary", repr(argv))
                Path(argv[argv.index("--digestfile") + 1]).write_text(digest(self.raw))
                return subprocess.CompletedProcess(argv, 0, b"", b"secret-canary")
            with patch.object(self.dev.subprocess, "run", side_effect=invoke):
                self.assertEqual(publisher.push(), digest(self.raw))
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_config_blob_redirect_can_be_fetched_without_forwarding_auth(self):
        registry = self.dev.GHCR(SOURCE, "arm64", "secret", "actor")
        class Response(io.BytesIO):
            status = 200
            headers = {}
        redirect = urllib.error.HTTPError("https://ghcr.io/blob", 307, "redirect", {"Location": "https://example.invalid/config"}, io.BytesIO())
        with patch.object(registry.opener, "open", side_effect=[redirect, Response(self.config_raw)]) as request:
            status, _, raw = registry.http("https://ghcr.io/blob", headers={"Authorization": "Bearer secret"},
                                           redirect_blob=True)
        self.assertEqual(status, 200)
        self.assertEqual(raw, self.config_raw)
        self.assertFalse(request.call_args_list[1].args[0].has_header("Authorization"))

    def test_host_first_writer_lock_excludes_a_competing_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "writer.lock"
            with self.dev.host_writer_lock(path):
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                with self.assertRaisesRegex(RuntimeError, "WRITER_BUSY"):
                    with self.dev.host_writer_lock(path): self.fail("second writer entered")
            with self.dev.host_writer_lock(path): pass
            path.unlink()
            path.symlink_to(Path(directory) / "unowned")
            with self.assertRaises((RuntimeError, OSError)):
                with self.dev.host_writer_lock(path): self.fail("symlink lock accepted")

    def test_build_context_rejects_ignored_source_bytes_absent_from_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "src" / "private.env").write_text("canary-secret")
            source = types.SimpleNamespace(source_manifest=lambda: "0" * 64 + "  src/private.env\n")
            with patch.object(self.dev, "ROOT", root), patch.object(self.dev, "command", return_value=b"src/main.rs\0"):
                with self.assertRaisesRegex(RuntimeError, "BUILD_INPUT_NOT_TRACKED"):
                    self.dev.validate_build_inputs(source)

    def test_missing_revision_label_refuses_before_smoke_or_push(self):
        with tempfile.TemporaryDirectory() as directory:
            publisher = self.publisher(object(), directory)
            inspect = [{"Id": self.smoke["image_id"], "Os": "linux", "Architecture": "arm64", "Config": {"Labels": {}}}]
            responses = [b'{"host":{"os":"linux","arch":"arm64"}}', b"--retry --authfile --digestfile --tls-verify", b"", encoded(inspect)]
            with patch.object(self.dev.platform, "system", return_value="Linux"), \
                 patch.object(self.dev.platform, "machine", return_value="aarch64"), \
                 patch.object(self.dev, "validate_build_inputs"), patch.object(self.dev, "command", side_effect=responses) as run:
                with self.assertRaisesRegex(RuntimeError, "LABEL"):
                    publisher.build_and_smoke()
                self.assertEqual(run.call_count, 4)

    def test_host_bootstrap_is_only_one_absent_package_write(self):
        class Registry:
            def package(self, bootstrap=False): return {"visibility": "public"}
            def lookup(self, tag): return None
        with tempfile.TemporaryDirectory() as directory:
            publisher = self.publisher(Registry(), directory, bootstrap=True)
            with patch.object(publisher, "build_and_smoke") as build, self.assertRaisesRegex(RuntimeError, "FIRST_PACKAGE"):
                publisher.execute()
            build.assert_not_called()

    def test_darwin_bootstrap_requires_same_native_linux_vm_architecture(self):
        for architecture in ("amd64", "arm64"):
            with tempfile.TemporaryDirectory() as directory:
                publisher = self.publisher(object(), directory, bootstrap=True)
                with patch.object(self.dev.platform, "system", return_value="Darwin"), \
                     patch.object(self.dev.platform, "machine", return_value="arm64"), \
                     patch.object(self.dev, "command", return_value=encoded({"host": {"os": "linux", "arch": architecture}})), \
                     patch.object(self.dev, "validate_build_inputs"):
                    with self.assertRaisesRegex(RuntimeError, "NATIVE_VM" if architecture == "amd64" else "PODMAN_PUSH_FLAGS"):
                        publisher.build_and_smoke()

    def test_workflow_restricts_branch_permissions_native_hosts_and_writer_window(self):
        text = (PATH.parents[2] / ".github/workflows/dev-runner-image.yml").read_text()
        for value in ("feat/native-engine", "ubuntu-24.04-arm", "ubuntu-24.04", "packages: write", "contents: read", "cancel-in-progress: false", "dev-ghcr-ydixken-rpgmigrate"):
            self.assertIn(value, text)
        for forbidden in ("pull_request", "workflow_dispatch", "latest", "docker/login-action", "secrets.GH_PAT"):
            self.assertNotIn(forbidden, text)
        for line in text.splitlines():
            if "uses:" in line: self.assertRegex(line, r"@[0-9a-f]{40}")
        self.assertLess(text.index("jobs:"), text.index("packages: write"))


if __name__ == "__main__":
    unittest.main()
