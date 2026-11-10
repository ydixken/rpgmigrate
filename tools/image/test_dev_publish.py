#!/usr/bin/env python3
"""Offline controls for exact native development publication."""

import base64
import copy
import contextlib
import hashlib
import importlib.util
import json
import os
import re
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
            responses = self.tool_responses() + [b"", encoded(inspect)]
            with patch.object(self.dev.platform, "system", return_value="Linux"), \
                 patch.object(self.dev.platform, "machine", return_value="aarch64"), \
                 patch.object(self.dev, "validate_build_inputs"), patch.object(self.dev, "command", side_effect=responses) as run, \
                 contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "LABEL"):
                    publisher.build_and_smoke()
                self.assertEqual(run.call_count, 6)

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
        responses = self.tool_responses(client="6.1.4", server="5.8.3", client_os="darwin")
        responses[1] = b'{"host":{"os":"linux","arch":"amd64"}}'
        with self.assertRaisesRegex(RuntimeError, "NATIVE_VM"):
            self.tool_probe(responses, system="Darwin", machine="arm64", bootstrap=True)

    def tool_responses(self, *, client="5.7.0", server=None, client_os="linux", architecture="arm64"):
        version = {"Client": {"Version": client, "OsArch": client_os + "/" + architecture}}
        if server is not None:
            version["Server"] = {"Version": server, "OsArch": "linux/" + architecture}
        info = {"host": {"os": "linux", "arch": architecture}, "version": {"Version": server or client}}
        push = b"      --retry uint\n      --authfile string\n      --digestfile string\n      --tls-verify\n"
        pull = b"      --retry uint\n      --authfile string\n      --tls-verify\n"
        return [encoded(version), encoded(info), push, pull]

    def tool_probe(self, responses, *, system="Linux", machine="aarch64", bootstrap=False):
        self.assertTrue(hasattr(self.dev, "tool_preflight"), "selected tools must be probed before credentials")
        output = io.StringIO()
        self.tool_output = output
        with patch.object(self.dev.platform, "system", return_value=system), \
             patch.object(self.dev.platform, "machine", return_value=machine), \
             patch.object(self.dev, "command", side_effect=responses) as run, contextlib.redirect_stdout(output):
            value = self.dev.tool_preflight("arm64", bootstrap=bootstrap)
        return value, output.getvalue(), run

    def test_tool_preflight_records_local_engine_without_fabricated_server(self):
        evidence, output, run = self.tool_probe(self.tool_responses())
        self.assertEqual(evidence["client_version"], "5.7.0")
        self.assertIsNone(evidence["server_version"])
        self.assertEqual(evidence["engine_mode"], "local")
        self.assertEqual(evidence["engine_version"], "5.7.0")
        self.assertEqual(evidence["architecture"], "arm64")
        self.assertEqual(evidence["os"], "linux")
        self.assertEqual(json.loads(output)["status"], "READY")
        self.assertEqual([call.args[0][1] for call in run.call_args_list], ["version", "info", "push", "pull"])

    def test_tool_preflight_accepts_independent_darwin_and_native_vm_versions(self):
        evidence, _, _ = self.tool_probe(self.tool_responses(client="6.1.4", server="5.8.3", client_os="darwin"),
                                        system="Darwin", machine="arm64", bootstrap=True)
        self.assertEqual((evidence["client_version"], evidence["server_version"], evidence["engine_mode"]),
                         ("6.1.4", "5.8.3", "remote"))
        with self.assertRaisesRegex(RuntimeError, "SERVER"):
            self.tool_probe(self.tool_responses(client_os="darwin"), system="Darwin", machine="arm64", bootstrap=True)

    def test_tool_preflight_rejects_old_push_or_pull_and_retry_delay_substring(self):
        self.assertTrue(hasattr(self.dev, "tool_preflight"), "exact options must be checked")
        for index in (2, 3):
            for invalid in (self.tool_responses()[index].replace(b"--retry uint", b"--retry-delay string"),
                            b"--authfile string\n--tls-verify\n", b"notes mention --retry but do not declare it\n", b"canary-secret\xff"):
                responses = self.tool_responses()
                responses[index] = invalid
                output = io.StringIO()
                with self.subTest(index=index, invalid=invalid), contextlib.redirect_stdout(output), self.assertRaises(RuntimeError):
                    self.tool_probe(responses)
                self.assertNotIn("canary-secret", self.tool_output.getvalue())

    def test_tool_preflight_rejects_malformed_versions_platform_and_unknown_output(self):
        self.assertTrue(hasattr(self.dev, "tool_preflight"), "malformed tool metadata must fail")
        valid = self.tool_responses()
        valid[0] = encoded({**json.loads(valid[0]), "unknown_secret": "canary-secret"})
        _, output, _ = self.tool_probe(valid)
        self.assertNotIn("canary-secret", output)
        for client in (None, True, 5.7, "", "5.7", "5.7.0-canary-secret", "secret\n5.7.0"):
            responses = self.tool_responses(client=client)
            with self.subTest(client=client), self.assertRaises(RuntimeError):
                self.tool_probe(responses)
        for index, field, value in ((0, "Client", {"Version": "5.7.0", "OsArch": "linux/amd64"}),
                                    (0, "Server", {"Version": "canary-secret", "OsArch": "linux/arm64"}),
                                    (0, "Server", {"Version": "5.8.3", "OsArch": "windows/arm64"}),
                                    (1, "host", {"os": "linux", "arch": "amd64"}),
                                    (1, "version", {"Version": "canary-secret"}),
                                    (1, "version", {"Version": "5.8.1"})):
            responses = self.tool_responses()
            values = json.loads(responses[index]);values[field] = value;values["unknown_secret"] = "canary-secret"
            responses[index] = encoded(values)
            output = io.StringIO()
            with self.subTest(field=field, value=value), contextlib.redirect_stdout(output), self.assertRaises(RuntimeError):
                self.tool_probe(responses)
            self.assertNotIn("canary-secret", self.tool_output.getvalue())

    def test_tool_preflight_missing_command_discards_private_diagnostics(self):
        self.assertTrue(hasattr(self.dev, "tool_preflight"), "missing tools must refuse")
        output = io.StringIO()
        with patch.object(self.dev.platform, "system", return_value="Linux"), \
             patch.object(self.dev.platform, "machine", return_value="aarch64"), \
             patch.object(self.dev.subprocess, "run", side_effect=FileNotFoundError("canary-secret")), \
             contextlib.redirect_stdout(output), self.assertRaisesRegex(RuntimeError, "COMMAND_FAILED") as failure:
            self.dev.tool_preflight("arm64")
        self.assertNotIn("canary-secret", output.getvalue() + str(failure.exception))

    def test_main_tool_refusal_precedes_host_or_workflow_credentials(self):
        self.assertTrue(hasattr(self.dev, "tool_preflight"), "tool check must run before credentials")
        for bootstrap in (False, True):
            with tempfile.TemporaryDirectory() as directory:
                argv = ["dev_publish.py", "publish", "--architecture", "arm64", "--expected-commit", COMMIT,
                        "--output-dir", directory] + (["--bootstrap-native"] if bootstrap else [])
                env = {} if bootstrap else {"GITHUB_ACTIONS": "true", "GITHUB_EVENT_NAME": "push", "GITHUB_REPOSITORY": "ydixken/rpgmigrate",
                         "GITHUB_REF": "refs/heads/feat/native-engine", "GITHUB_SHA": COMMIT, "GITHUB_TOKEN": "canary-secret"}
                with patch.object(self.dev.sys, "argv", argv), patch.dict(os.environ, env, clear=True), \
                     patch.object(self.dev, "source_identity", return_value=SOURCE), \
                     patch.object(self.dev, "command", return_value=b"feat/native-engine\n") as run, \
                     patch.object(self.dev, "tool_preflight", side_effect=RuntimeError("PODMAN_PUSH_FLAGS_UNSUPPORTED")), \
                     patch.object(self.dev, "GHCR") as registry, contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(self.dev.main(), 1)
                    self.assertEqual(run.call_count, 1)
                    self.assertNotIn("gh", run.call_args.args[0])
                    registry.assert_not_called()
                    if not bootstrap: self.assertEqual(os.environ["GITHUB_TOKEN"], "canary-secret")

    def source_fixture(self, directory):
        root = Path(directory).resolve()
        for argv in (["git", "init", "-b", "feature"], ["git", "config", "user.name", "Fixture"],
                     ["git", "config", "user.email", "fixture@example.invalid"],
                     ["git", "remote", "add", "origin", SOURCE["repository"] + ".git"]):
            subprocess.run(argv, cwd=root, check=True, capture_output=True)
        file = root / "input";file.write_bytes(b"actual committed input")
        subprocess.run(["git", "add", "input"], cwd=root, check=True)
        subprocess.run(["git", "-c", "commit.gpgsign=false", "commit", "-m", "fixture"], cwd=root, check=True, capture_output=True)
        return root, file, subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()

    def test_source_real_clean_blob_accepts_read_atime_observation_change(self):
        with tempfile.TemporaryDirectory() as directory:
            root, file, head = self.source_fixture(directory)
            real_read, real_stat = Path.read_bytes, Path.stat
            was_read = []
            def read(path):
                value = real_read(path)
                if path == file: was_read.append(True)
                return value
            def observe(path, *args, **kwargs):
                value = real_stat(path, *args, **kwargs)
                if path == file and was_read:
                    fields = {name: getattr(value, name) for name in dir(value) if name.startswith("st_")}
                    fields.update(st_atime=value.st_atime + 1, st_atime_ns=value.st_atime_ns + 1000000000)
                    return types.SimpleNamespace(**fields)
                return value
            with patch.object(Path, "read_bytes", read), patch.object(Path, "stat", observe):
                identity = self.dev.source_identity(root, head)
            self.assertEqual(identity["commit"], head)
            self.assertEqual(file.read_bytes(), b"actual committed input")

    def test_source_read_refuses_each_stable_identity_and_nanosecond_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            root, file, head = self.source_fixture(directory)
            real_read, real_stat = Path.read_bytes, Path.stat
            for field in ("st_dev", "st_ino", "st_mode", "st_nlink", "st_uid", "st_gid", "st_size", "st_mtime_ns", "st_ctime_ns"):
                was_read = []
                def read(path):
                    value = real_read(path)
                    if path == file: was_read.append(True)
                    return value
                def observe(path, *args, **kwargs):
                    value = real_stat(path, *args, **kwargs)
                    if path == file and was_read:
                        fields = {name: getattr(value, name) for name in dir(value) if name.startswith("st_")}
                        fields[field] += 1
                        return types.SimpleNamespace(**fields)
                    return value
                with self.subTest(field=field), patch.object(Path, "read_bytes", read), patch.object(Path, "stat", observe), \
                     self.assertRaisesRegex(RuntimeError, "SOURCE_CHANGED"):
                    self.dev.source_identity(root, head)

    def test_workflow_preflight_invocation_selects_only_safe_metadata_before_credentials(self):
        text = (PATH.parents[2] / ".github/workflows/dev-runner-image.yml").read_text()
        code = re.search(r'python3 -B -c \\\n\s+"([^\n]+)"', text)[1].replace("${{ matrix.arch }}", "arm64")
        responses = self.tool_responses()
        version = json.loads(responses[0]);version["connection_secret"] = "canary-secret";responses[0] = encoded(version)
        calls, output = [], io.StringIO()
        def invoke(argv, **kwargs):
            calls.append(argv)
            self.assertFalse(kwargs.get("shell", False))
            for key in ("GITHUB_TOKEN", "HTTPS_PROXY", "REGISTRY_AUTH_FILE"):
                self.assertNotIn(key, kwargs["env"])
            return subprocess.CompletedProcess(argv, 0, responses.pop(0), b"canary-secret")
        with patch.object(self.dev.platform, "system", return_value="Linux"), \
             patch.object(self.dev.platform, "machine", return_value="aarch64"), \
             patch.object(self.dev.subprocess, "run", side_effect=invoke), \
             patch.dict(os.environ, {"GITHUB_TOKEN": "canary-secret", "HTTPS_PROXY": "https://canary-secret.invalid", "REGISTRY_AUTH_FILE": "/private/canary-secret"}), \
             contextlib.chdir(PATH.parents[2]), contextlib.redirect_stdout(output):
            exec(code, {"__name__": "workflow_preflight_test"})
        self.assertEqual(calls, [["podman", "version", "--format=json"], ["podman", "info", "--format=json"],
                                 ["podman", "push", "--help"], ["podman", "pull", "--help"]])
        self.assertEqual(json.loads(output.getvalue())["status"], "READY")
        self.assertNotIn("canary-secret", output.getvalue())

    def test_workflow_restricts_branch_permissions_native_hosts_and_writer_window(self):
        text = (PATH.parents[2] / ".github/workflows/dev-runner-image.yml").read_text()
        for value in ("feat/native-engine", "ubuntu-26.04-arm", "ubuntu-26.04", "packages: write", "contents: read", "cancel-in-progress: false", "dev-ghcr-ydixken-rpgmigrate"):
            self.assertIn(value, text)
        for forbidden in ("pull_request", "workflow_dispatch", "latest", "docker/login-action", "secrets.GH_PAT"):
            self.assertNotIn(forbidden, text)
        for line in text.splitlines():
            if "uses:" in line: self.assertRegex(line, r"@[0-9a-f]{40}")
        self.assertLess(text.index("jobs:"), text.index("packages: write"))
        self.assertIn("tool_preflight", text)
        self.assertLess(text.index("tool_preflight"), text.index("GITHUB_TOKEN:"))


if __name__ == "__main__":
    unittest.main()
