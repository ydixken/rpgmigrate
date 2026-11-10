#!/usr/bin/env python3
"""Publish one clean native development image, or verify its retained public digest."""

import argparse
import base64
import contextlib
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


REPOSITORY = "https://github.com/ydixken/rpgmigrate"
PACKAGE = "ghcr.io/ydixken/rpgmigrate"
BRANCH = "feat/native-engine"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
DOCKER_MANIFEST = "application/vnd.docker.distribution.manifest.v2+json"
CONFIGS = {"application/vnd.oci.image.config.v1+json", "application/vnd.docker.container.image.v1+json"}
LAYERS = {"application/vnd.oci.image.layer.v1.tar+gzip", "application/vnd.oci.image.layer.v1.tar+zstd",
          "application/vnd.oci.image.layer.v1.tar", "application/vnd.docker.image.rootfs.diff.tar.gzip"}
MAX_JSON = 1024 * 1024
ROOT = Path(__file__).resolve().parents[2]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def unique(items):
    value = {}
    for key, item in items:
        require(key not in value, "DUPLICATE_JSON_FIELD")
        value[key] = item
    return value


def decode(data):
    require(0 < len(data) <= MAX_JSON, "JSON_SIZE_INVALID")
    try:
        return json.loads(data, object_pairs_hook=unique)
    except (ValueError, UnicodeError) as error:
        raise RuntimeError("JSON_INVALID") from error


def tag_for(commit, architecture):
    require(type(commit) is str and re.fullmatch("[0-9a-f]{40}", commit), "SOURCE_COMMIT_INVALID")
    require(architecture in ("arm64", "amd64"), "ARCHITECTURE_INVALID")
    return "dev-" + commit + "-" + architecture


def stderr_classification(value):
    if value is None or not isinstance(value, (bytes, str)):
        return "UNOBSERVED"
    data = value.encode("utf-8", errors="replace") if isinstance(value, str) else value
    if not data:
        return "NONE"
    data = data.lower()
    categories = (
        ("PERMISSION", (b"permission denied", b"permission_denied", b"insufficient_scope", b"requested access to the resource is denied")),
        ("AUTHENTICATION", (b"unauthorized", b"authentication required", b"authentication failed", b"invalid username/password")),
        ("TLS", (b"x509:", b"tls handshake", b"certificate verify failed", b"certificate signed by unknown authority")),
        ("NETWORK", (b"connection refused", b"connection reset", b"network is unreachable", b"no such host", b"i/o timeout", b"context deadline exceeded")),
        ("OPTION", (b"unknown flag:", b"unknown shorthand flag:", b"unrecognized option", b"unknown option", b"flag provided but not defined")),
    )
    return next((category for category, phrases in categories if any(phrase in data for phrase in phrases)), "OTHER")


class CommandFailure(RuntimeError):
    def __init__(self, code, failure, *, return_code=None, timed_out=False, stderr_class="UNOBSERVED"):
        super().__init__(code)
        self.stage = "LAUNCH" if failure == "LAUNCH_ERROR" else "RETURN"
        self.failure = failure
        self.launch_attempted = True
        self.return_code = return_code if type(return_code) is int else None
        self.timed_out = timed_out
        self.stderr_class = stderr_class


def command(argv, *, cwd=ROOT, timeout=120, env=None):
    # Subprocess output can contain credentials even when the command fails.
    safe_env = {key: os.environ[key] for key in ("PATH", "HOME", "XDG_RUNTIME_DIR", "LANG", "LC_ALL", "TMPDIR")
                if key in os.environ}
    safe_env["PYTHONDONTWRITEBYTECODE"] = "1"
    if env:
        safe_env.update(env)
    failure = None
    try:
        result = subprocess.run(argv, cwd=cwd, env=safe_env, check=True, capture_output=True, timeout=timeout)
    except subprocess.CalledProcessError as error:
        failure = CommandFailure("COMMAND_FAILED", "NONZERO_EXIT", return_code=error.returncode,
                                 stderr_class=stderr_classification(error.stderr))
    except subprocess.TimeoutExpired as error:
        failure = CommandFailure("COMMAND_FAILED", "TIMEOUT", timed_out=True,
                                 stderr_class=stderr_classification(error.stderr))
    except OSError:
        failure = CommandFailure("COMMAND_FAILED", "LAUNCH_ERROR")
    except subprocess.SubprocessError:
        failure = CommandFailure("COMMAND_FAILED", "COMMAND_ERROR")
    if failure is not None:
        raise failure
    if len(result.stdout) > 16 * MAX_JSON:
        failure = CommandFailure("COMMAND_OUTPUT_TOO_LARGE", "OUTPUT_LIMIT", return_code=result.returncode,
                                 stderr_class=stderr_classification(result.stderr))
        del result
        raise failure
    return result.stdout


def stable_file_identity(info):
    # Reading may update atime without changing the committed payload.
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_uid, info.st_gid,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def source_identity(root, expected_commit):
    root = Path(root).resolve()
    tag_for(expected_commit, "amd64")
    git = lambda *args: command(["git", *args], cwd=root).decode().strip()
    require(not git("status", "--porcelain", "--untracked-files=all"), "SOURCE_MUST_BE_clean")
    require(git("rev-parse", "HEAD") == expected_commit, "SOURCE_COMMIT_CHANGED")
    require(git("branch", "--show-current") not in ("main", "master", ""), "SOURCE_BRANCH_INVALID")
    require(git("remote", "get-url", "origin") in (REPOSITORY, REPOSITORY + ".git", "git@github.com:ydixken/rpgmigrate.git"),
            "SOURCE_REPOSITORY_INVALID")
    paths = command(["git", "ls-files", "-z"], cwd=root).decode().split("\0")[:-1]
    require(paths and len(paths) <= 8192 and len(set(paths)) == len(paths), "SOURCE_INVENTORY_INVALID")
    committed = {}
    for entry in command(["git", "ls-tree", "-rz", "--full-tree", "HEAD"], cwd=root).split(b"\0"):
        if entry:
            attributes, name = entry.split(b"\t", 1)
            mode, kind, blob = attributes.decode().split()
            require(kind == "blob" and mode in ("100644", "100755"), "SOURCE_ENTRY_INVALID")
            committed[name.decode()] = (int(mode, 8) & 0o777, blob)
    require(set(committed) == set(paths), "SOURCE_INDEX_CHANGED")
    inventory, total = [], 0
    for name in sorted(paths):
        require(name and not name.startswith("/") and ".." not in Path(name).parts
                and not any(ord(char) < 32 for char in name), "SOURCE_PATH_INVALID")
        path = root / name
        require(all(not (root / Path(*Path(name).parts[:index])).is_symlink()
                    for index in range(1, len(Path(name).parts) + 1)),
                "SOURCE_SYMLINK_INVALID")
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= 16 * MAX_JSON,
                "SOURCE_FILE_INVALID")
        data = path.read_bytes()
        total += len(data)
        require(total <= 128 * MAX_JSON and len(data) == info.st_size
                and stable_file_identity(path.stat()) == stable_file_identity(info), "SOURCE_CHANGED")
        blob = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        require(committed[name] == (stat.S_IMODE(info.st_mode), blob), "SOURCE_COMMIT_CONTENT_CHANGED")
        inventory.append([name, stat.S_IMODE(info.st_mode), sha(data)])
    require(not git("status", "--porcelain", "--untracked-files=all"), "SOURCE_MUST_BE_clean")
    tree = git("rev-parse", "HEAD^{tree}")
    require(re.fullmatch("[0-9a-f]{40}", tree), "SOURCE_TREE_INVALID")
    return {"repository": REPOSITORY, "commit": expected_commit, "git_tree": tree,
            "payload_inventory_sha256": sha(json.dumps(inventory, separators=(",", ":")).encode())}


def descriptor(value, media_types):
    require(type(value) is dict and value.get("mediaType") in media_types
            and type(value.get("digest")) is str and re.fullmatch("sha256:[0-9a-f]{64}", value["digest"])
            and type(value.get("size")) is int and 0 < value["size"] <= 1024 * MAX_JSON
            and not value.get("urls"), "DESCRIPTOR_INVALID")
    return {key: value[key] for key in ("mediaType", "digest", "size")}


def validate_graph(manifest_raw, config_raw, architecture, source, smoke=None):
    manifest, config = decode(manifest_raw), decode(config_raw)
    require(type(manifest) is dict and type(manifest.get("schemaVersion")) is int
            and manifest["schemaVersion"] == 2 and manifest.get("mediaType") in (MANIFEST, DOCKER_MANIFEST)
            and "manifests" not in manifest and "subject" not in manifest, "SINGLE_PLATFORM_MANIFEST_REQUIRED")
    config_descriptor = descriptor(manifest.get("config"), CONFIGS)
    require(config_descriptor["digest"] == "sha256:" + sha(config_raw)
            and config_descriptor["size"] == len(config_raw), "CONFIG_IDENTITY_INVALID")
    require(type(config) is dict and config.get("os") == "linux" and config.get("architecture") == architecture,
            "CONFIG_PLATFORM_INVALID")
    variant = config.get("variant")
    require(variant is None or architecture == "arm64" and variant == "v8", "CONFIG_VARIANT_INVALID")
    require(type(config.get("config")) is dict and type(config["config"].get("Labels")) is dict, "CONFIG_LABELS_INVALID")
    labels = config["config"]["Labels"]
    require(labels.get("org.opencontainers.image.source") == source["repository"]
            and labels.get("org.opencontainers.image.revision") == source["commit"]
            and labels.get("org.opencontainers.image.licenses") == "Apache-2.0", "SOURCE_LABELS_INVALID")
    layers = manifest.get("layers")
    require(type(layers) is list and 0 < len(layers) <= 128, "LAYERS_INVALID")
    layers = [descriptor(layer, LAYERS) for layer in layers]
    if smoke is not None:
        require(smoke.get("image_id") == config_descriptor["digest"]
                and smoke.get("architecture") == architecture, "SMOKED_IMAGE_IDENTITY_INVALID")
    return {"manifest_digest": "sha256:" + sha(manifest_raw), "config_digest": config_descriptor["digest"],
            "index_digest": None, "os": "linux", "architecture": architecture, "variant": variant,
            "manifest_base64": base64.b64encode(manifest_raw).decode(),
            "config_base64": base64.b64encode(config_raw).decode(), "layers": layers}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, message, headers, newurl):
        return None


class GHCR:
    def __init__(self, source, architecture, token=None, username=None, bootstrap=False):
        self.source, self.architecture = source, architecture
        self.token, self.username, self.bootstrap = token, username, bootstrap
        self.bearer = None
        self.deadline = time.monotonic() + 2400
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
        self.namespace_accessible = False

    def http(self, url, *, headers=None, limit=MAX_JSON, blob=None, redirect_blob=False):
        require(url.startswith("https://") and urllib.parse.urlparse(url).hostname in
                ("api.github.com", "ghcr.io"), "HTTP_ORIGIN_INVALID")
        headers = dict(headers or {})
        redirects = 0
        while True:
            require(time.monotonic() < self.deadline, "HTTP_DEADLINE_EXCEEDED")
            try:
                response = self.opener.open(urllib.request.Request(url, headers=headers), timeout=60)
            except urllib.error.HTTPError as error:
                if error.code in (301, 302, 303, 307, 308) and (blob is not None or redirect_blob) and redirects < 3:
                    location = urllib.parse.urljoin(url, error.headers.get("Location", ""))
                    require(urllib.parse.urlparse(location).scheme == "https"
                            and urllib.parse.urlparse(location).hostname
                            and not urllib.parse.urlparse(location).username, "BLOB_REDIRECT_INVALID")
                    error.close()
                    url, headers, redirects = location, {}, redirects + 1
                    continue
                with error:
                    data = error.read(MAX_JSON + 1)
                require(len(data) <= MAX_JSON, "HTTP_ERROR_RESPONSE_TOO_LARGE")
                return error.code, dict(error.headers), data
            except (OSError, urllib.error.URLError, TimeoutError) as error:
                raise RuntimeError("HTTP_UNCERTAIN") from error
            try:
                size, digest, chunks = 0, hashlib.sha256(), []
                while True:
                    require(time.monotonic() < self.deadline, "HTTP_DEADLINE_EXCEEDED")
                    chunk = response.read(65536)
                    if not chunk:
                        break
                    size += len(chunk)
                    require(size <= limit, "HTTP_RESPONSE_TOO_LARGE")
                    if blob is None:
                        chunks.append(chunk)
                    digest.update(chunk)
                if blob is not None:
                    require(size == blob["size"] and "sha256:" + digest.hexdigest() == blob["digest"],
                            "BLOB_IDENTITY_INVALID")
                    return response.status, dict(response.headers), {"size": size, "digest": blob["digest"]}
                return response.status, dict(response.headers), b"".join(chunks)
            except (OSError, TimeoutError) as error:
                raise RuntimeError("HTTP_UNCERTAIN") from error
            finally:
                response.close()

    def api(self, path):
        require(self.token, "AUTHENTICATION_REQUIRED")
        return self.http("https://api.github.com" + path,
                         headers={"Authorization": "Bearer " + self.token, "Accept": "application/vnd.github+json",
                                  "X-GitHub-Api-Version": "2022-11-28"})

    def package(self, bootstrap=False):
        status, _, data = self.api("/users/ydixken/packages/container/rpgmigrate")
        if status == 404:
            require(bootstrap and self.bootstrap and not os.environ.get("GITHUB_ACTIONS"), "FIRST_PACKAGE_HOST_ONLY")
            user_status, headers, user_raw = self.api("/user")
            scopes = {item.strip() for item in next((value for key, value in headers.items()
                                                   if key.lower() == "x-oauth-scopes"), "").split(",")}
            repo_status, _, repo_raw = self.api("/repos/ydixken/rpgmigrate")
            require(user_status == 200 and decode(user_raw).get("login") == "ydixken"
                    and "write:packages" in scopes and repo_status == 200
                    and decode(repo_raw).get("full_name") == "ydixken/rpgmigrate"
                    and decode(repo_raw).get("permissions", {}).get("admin") is True,
                    "FIRST_PACKAGE_AUTHORITY_UNPROVEN")
            self.namespace_accessible = False
            return {"visibility": "absent", "authority": {"owner": "ydixken", "package_status": 404,
                    "repository_admin": True, "package_write_scope": True, "checked_at": int(time.time())}}
        require(status == 200, "PACKAGE_ACCESS_UNCERTAIN")
        value = decode(data)
        require(value.get("name") == "rpgmigrate" and value.get("package_type") == "container"
                and value.get("repository", {}).get("full_name") == "ydixken/rpgmigrate", "PACKAGE_NAMESPACE_COLLISION")
        require(value.get("visibility") == "public" or bootstrap and self.bootstrap, "PACKAGE_NOT_PUBLIC")
        self.namespace_accessible = True
        return {"visibility": value["visibility"]}

    def auth_headers(self):
        if self.bearer is None:
            scope = "repository:ydixken/rpgmigrate:" + ("pull,push" if self.token else "pull")
            headers = {}
            if self.token:
                require(type(self.username) is str and re.fullmatch("[A-Za-z0-9-]{1,100}", self.username), "AUTH_USER_INVALID")
                headers["Authorization"] = "Basic " + base64.b64encode((self.username + ":" + self.token).encode()).decode()
            status, _, raw = self.http("https://ghcr.io/token?" + urllib.parse.urlencode({"service": "ghcr.io", "scope": scope}), headers=headers)
            require(status == 200, "REGISTRY_AUTH_UNCERTAIN")
            value = decode(raw)
            token = value.get("token", value.get("access_token"))
            require(type(token) is str and 0 < len(token) <= 65536 and not any(ord(char) < 33 for char in token), "REGISTRY_TOKEN_INVALID")
            self.bearer = token
        return {"Authorization": "Bearer " + self.bearer, "Accept": MANIFEST + ", " + DOCKER_MANIFEST}

    def raw_graph(self, reference, allow_absent=False):
        status, headers, raw = self.http("https://ghcr.io/v2/ydixken/rpgmigrate/manifests/" + reference,
                                         headers=self.auth_headers())
        if status == 404 and allow_absent and self.namespace_accessible:
            error = decode(raw)
            require(type(error) is dict and type(error.get("errors")) is list and len(error["errors"]) == 1
                    and error["errors"][0].get("code") == "MANIFEST_UNKNOWN", "TAG_ABSENCE_UNPROVEN")
            return None
        require(status == 200, "MANIFEST_ACCESS_UNCERTAIN")
        returned_digest = next((value for key, value in headers.items() if key.lower() == "docker-content-digest"), None)
        require(returned_digest == "sha256:" + sha(raw), "MANIFEST_RESPONSE_DIGEST_INVALID")
        if reference.startswith("sha256:"):
            require(reference == returned_digest, "MANIFEST_REFERENCE_MISMATCH")
        manifest = decode(raw)
        require(manifest.get("mediaType") in (MANIFEST, DOCKER_MANIFEST), "SINGLE_PLATFORM_MANIFEST_REQUIRED")
        config = descriptor(manifest.get("config"), CONFIGS)
        status, _, config_raw = self.http("https://ghcr.io/v2/ydixken/rpgmigrate/blobs/" + config["digest"],
                                          headers=self.auth_headers(), redirect_blob=True)
        require(status == 200, "CONFIG_ACCESS_UNCERTAIN")
        return validate_graph(raw, config_raw, self.architecture, self.source)

    def lookup(self, tag):
        require(tag == tag_for(self.source["commit"], self.architecture), "TAG_INVALID")
        if not self.namespace_accessible:
            require(self.bootstrap, "PACKAGE_ABSENCE_UNPROVEN")
            return None
        return self.raw_graph(tag, allow_absent=True)

    def public(self, graph):
        anonymous = GHCR(self.source, self.architecture)
        observed = anonymous.raw_graph(graph["manifest_digest"])
        require(observed == graph, "PUBLIC_GRAPH_MISMATCH")
        blobs = []
        for layer in graph["layers"]:
            status, _, identity = anonymous.http("https://ghcr.io/v2/ydixken/rpgmigrate/blobs/" + layer["digest"],
                                                  headers=anonymous.auth_headers(), limit=layer["size"], blob=layer)
            require(status == 200, "PUBLIC_BLOB_UNAVAILABLE")
            blobs.append(identity)
        return {"graph": observed, "blobs": blobs}


def read_private(path):
    path = Path(path)
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and not path.is_symlink() and info.st_nlink == 1
            and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o600
            and 0 < info.st_size <= 8 * MAX_JSON, "PRIVATE_RECEIPT_INVALID")
    return decode(path.read_bytes())


def write_private(path, value):
    path = Path(path)
    info = path.parent.lstat()
    require(stat.S_ISDIR(info.st_mode) and not path.parent.is_symlink() and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700 and path.parent.resolve() != ROOT
            and ROOT not in path.parent.resolve().parents, "RECEIPT_DIRECTORY_INVALID")
    require(not path.is_symlink(), "RECEIPT_PATH_REPLACED")
    raw = canonical(value) + b"\n"
    require(len(raw) <= 8 * MAX_JSON, "RECEIPT_TOO_LARGE")
    descriptor, temporary = tempfile.mkstemp(prefix=".receipt-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as target:
            target.write(raw)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def completed_receipt(receipt, public):
    receipt = dict(receipt)
    graph = receipt["registry_receipt"]
    require(type(public) is dict and set(public) == {"graph", "blobs"}
            and public["graph"] == graph and public["blobs"] ==
            [{"digest": layer["digest"], "size": layer["size"]} for layer in graph["layers"]], "PUBLIC_RECEIPT_INVALID")
    receipt.update(status="PUBLIC", public_receipt=public)
    receipt["resolved_image"] = {"role": "native-runner", "ref": PACKAGE + "@" + graph["manifest_digest"],
        **{key: graph[key] for key in ("os", "architecture", "variant", "index_digest")},
        "digest": graph["manifest_digest"], "config_digest": graph["config_digest"],
        "provenance": {"origin_ref": PACKAGE + ":" + receipt["tag"],
                       "registry_receipt_sha256": sha(canonical(graph)), "public_receipt_sha256": sha(canonical(public))},
        "source": {**receipt["source"], "build_input_sha256": receipt["build_input_sha256"],
                   "executable_sha256": receipt["executable_sha256"], "smoke_receipt_sha256": receipt["smoke_receipt_sha256"]}}
    return receipt


def validate_retained(receipt):
    require(type(receipt) is dict and receipt.get("schema_version") == 1 and receipt.get("role") == "native-runner"
            and receipt.get("status") in ("PUBLIC", "PUBLICITY_PENDING"), "RETAINED_RECEIPT_INVALID")
    require(set(receipt) == {"schema_version", "status", "role", "source", "tag", "architecture", "smoke_receipt",
            "smoke_receipt_sha256", "authority", "os", "variant", "index_digest", "manifest_digest", "config_digest",
            "registry_receipt", "build_input_sha256", "executable_sha256", "public_receipt", "resolved_image"},
            "RETAINED_FIELDS_INVALID")
    authority = receipt["authority"]
    require(authority == {"visibility": "public"} or type(authority) is dict
            and set(authority) == {"visibility", "authority"} and authority["visibility"] == "absent"
            and type(authority["authority"]) is dict and set(authority["authority"]) ==
            {"owner", "package_status", "repository_admin", "package_write_scope", "checked_at"}
            and authority["authority"]["owner"] == "ydixken" and authority["authority"]["package_status"] == 404
            and authority["authority"]["repository_admin"] is True and authority["authority"]["package_write_scope"] is True
            and type(authority["authority"]["checked_at"]) is int, "RETAINED_AUTHORITY_INVALID")
    source, architecture = receipt["source"], receipt["architecture"]
    require(type(source) is dict and set(source) == {"repository", "commit", "git_tree", "payload_inventory_sha256"}
            and source["repository"] == REPOSITORY and re.fullmatch("[0-9a-f]{40}", source["git_tree"])
            and re.fullmatch("[0-9a-f]{64}", source["payload_inventory_sha256"])
            and receipt["tag"] == tag_for(source["commit"], architecture), "RETAINED_SOURCE_INVALID")
    smoke = receipt["smoke_receipt"]
    graph = receipt["registry_receipt"]
    require(graph == validate_graph(base64.b64decode(graph["manifest_base64"], validate=True),
            base64.b64decode(graph["config_base64"], validate=True), architecture, source, smoke), "RETAINED_GRAPH_INVALID")
    for field in ("build_input_sha256", "executable_sha256", "smoke_receipt_sha256"):
        require(type(receipt.get(field)) is str and re.fullmatch("[0-9a-f]{64}", receipt[field]), "RETAINED_HASH_INVALID")
    require(receipt["build_input_sha256"] == smoke["build_inputs_sha256"]
            and receipt["executable_sha256"] == smoke["runtime_binary_sha256"]
            and receipt["smoke_receipt_sha256"] == sha(canonical(smoke)), "RETAINED_SMOKE_INVALID")
    require(receipt["manifest_digest"] == graph["manifest_digest"] and receipt["config_digest"] == graph["config_digest"]
            and receipt["index_digest"] is None and receipt["variant"] == graph["variant"] and receipt["os"] == "linux",
            "RETAINED_IDENTITY_INVALID")
    return receipt


def verify_public(receipt):
    receipt = validate_retained(receipt)
    public = GHCR(receipt["source"], receipt["architecture"]).public(receipt["registry_receipt"])
    return completed_receipt(receipt, public)


def tool_preflight(architecture, bootstrap=False):
    evidence = {"status": "REFUSED", "client_version": None, "server_version": None, "engine_version": None,
                "engine_mode": None, "client_os": None, "client_architecture": None,
                "server_os": None, "server_architecture": None, "os": None, "architecture": None,
                "push_flags": {}, "pull_flags": {}}

    def numeric(value):
        require(type(value) is str and re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", value),
                "PODMAN_VERSION_INVALID")
        return value

    def version_fields(value, expected_os):
        require(type(value) is dict and type(value.get("OsArch")) is str, "PODMAN_PLATFORM_INVALID")
        os_arch = value["OsArch"].split("/")
        require(os_arch == [expected_os, architecture]
                and value.get("Os", expected_os) == expected_os, "PODMAN_PLATFORM_INVALID")
        return numeric(value.get("Version")), os_arch[0], os_arch[1]

    try:
        physical_arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
        system = platform.system()
        require(architecture in ("arm64", "amd64") and physical_arch == architecture and (system == "Linux"
                or bootstrap and system == "Darwin" and physical_arch == "arm64"), "NATIVE_LINUX_BUILD_REQUIRED")
        version = decode(command(["podman", "version", "--format=json"]))
        require(type(version) is dict, "PODMAN_VERSION_INVALID")
        client = version_fields(version.get("Client"), system.lower())
        evidence.update(client_version=client[0], client_os=client[1], client_architecture=client[2])
        if "Server" in version:
            server = version_fields(version["Server"], "linux")
            evidence.update(server_version=server[0], server_os=server[1], server_architecture=server[2], engine_mode="remote")
        else:
            require(system == "Linux", "PODMAN_REMOTE_SERVER_REQUIRED")
        runtime = decode(command(["podman", "info", "--format=json"]))
        require(type(runtime) is dict and type(runtime.get("host")) is dict
                and runtime["host"].get("os") == "linux" and runtime["host"].get("arch") == physical_arch,
                "NATIVE_VM_PLATFORM_MISMATCH")
        evidence.update(os="linux", architecture=physical_arch,
                        engine_mode="remote" if "Server" in version else "local")
        engine = runtime.get("version")
        require(engine is None or type(engine) is dict, "PODMAN_VERSION_INVALID")
        if engine is not None and "Version" in engine:
            evidence["engine_version"] = numeric(engine["Version"])
            require(evidence["engine_version"] == (evidence["server_version"] or evidence["client_version"]),
                    "PODMAN_ENGINE_VERSION_MISMATCH")
        for operation, flags in (("push", ("retry", "authfile", "digestfile", "tls-verify")),
                                 ("pull", ("retry", "authfile", "tls-verify"))):
            try:
                help_text = command(["podman", operation, "--help"]).decode()
            except UnicodeError:
                raise RuntimeError("PODMAN_HELP_INVALID") from None
            declared = set(re.findall(r"(?m)^[ \t]*(?:-[A-Za-z0-9],[ \t]*)?(--[a-z][a-z0-9-]*)(?=[ \t=]|$)", help_text))
            evidence[operation + "_flags"] = {flag.replace("-", "_"): "--" + flag in declared for flag in flags}
        require(all(evidence["push_flags"].values()), "PODMAN_PUSH_FLAGS_UNSUPPORTED")
        require(all(evidence["pull_flags"].values()), "PODMAN_PULL_FLAGS_UNSUPPORTED")
        evidence["status"] = "READY"
        return evidence
    finally:
        print(json.dumps(evidence, sort_keys=True), flush=True)


def validate_build_inputs(smoke):
    tracked = set(command(["git", "ls-files", "-z"]).decode().split("\0")[:-1])
    inputs = [line.split("  ", 1)[1] for line in smoke.source_manifest().splitlines()]
    require(inputs and all(name in tracked for name in inputs), "BUILD_INPUT_NOT_TRACKED")


class Publisher:
    def __init__(self, registry, output, source, architecture, *, retained=None, bootstrap=False):
        self.registry, self.output, self.source, self.architecture = registry, Path(output), source, architecture
        self.tag, self.retained, self.bootstrap = tag_for(source["commit"], architecture), retained, bootstrap
        info = self.output.lstat()
        require(stat.S_ISDIR(info.st_mode) and not self.output.is_symlink() and info.st_uid == os.getuid()
                and stat.S_IMODE(info.st_mode) == 0o700 and self.output.resolve() != ROOT
                and ROOT not in self.output.resolve().parents,
                "OUTPUT_DIRECTORY_MUST_BE_PRIVATE_EXTERNAL")
        require(not (self.output / "publication.json").exists(), "PUBLICATION_STATE_ALREADY_EXISTS")
        self.smoke = None
        self.toolchain = None

    def state(self, value):
        write_private(self.output / "publication.json", value)
        return value

    def recheck_source(self):
        require(source_identity(ROOT, self.source["commit"]) == self.source, "SOURCE_CHANGED")

    def build_and_smoke(self):
        if self.toolchain is None:
            self.toolchain = tool_preflight(self.architecture, bootstrap=self.bootstrap)
        spec = importlib.util.spec_from_file_location("publication_smoke", ROOT / "tools/image/smoke.py")
        smoke = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(smoke)
        validate_build_inputs(smoke)
        command(["task", "--silent", "image:build", "IMAGE=" + PACKAGE + ":" + self.tag,
                 "IMAGE_REVISION=" + self.source["commit"]], timeout=2100)
        image = decode(command(["podman", "image", "inspect", PACKAGE + ":" + self.tag]))
        require(type(image) is list and len(image) == 1, "BUILD_IMAGE_INSPECTION_INVALID")
        image_id = image[0].get("Id")
        if type(image_id) is str and re.fullmatch("[0-9a-f]{64}", image_id):
            image_id = "sha256:" + image_id
        require(type(image_id) is str and re.fullmatch("sha256:[0-9a-f]{64}", image_id), "BUILD_IMAGE_ID_INVALID")
        labels = image[0].get("Config", {}).get("Labels", {})
        require(type(labels) is dict and labels.get("org.opencontainers.image.source") == REPOSITORY
                and labels.get("org.opencontainers.image.revision") == self.source["commit"]
                and labels.get("org.opencontainers.image.licenses") == "Apache-2.0", "BUILD_SOURCE_LABELS_INVALID")
        require(image[0].get("Os") == "linux" and image[0].get("Architecture") == self.architecture,
                "BUILD_PLATFORM_INVALID")
        receipt = decode(command(["task", "--silent", "image:smoke", "IMAGE=" + image_id], timeout=300))
        require(receipt.get("image_id") == image_id and receipt.get("architecture") == self.architecture,
                "SMOKE_IDENTITY_INVALID")
        require(receipt.get("build_inputs_sha256") == sha(smoke.source_manifest().encode()), "SMOKE_INPUTS_INVALID")
        python = receipt.get("python", {})
        require(python.get("notice_sha256") == sha((ROOT / "NOTICE").read_bytes())
                and python.get("license_sha256") == sha((ROOT / "LICENSE").read_bytes())
                and python.get("dependency_notices"), "SMOKE_NOTICES_INVALID")
        write_private(self.output / "smoke.json", receipt)
        return receipt

    def push(self):
        diagnostic = {"schema_version": 1, "source": self.source, "architecture": self.architecture, "tag": self.tag,
                      "stage": "AUTH_VALIDATION", "failure": "AUTH_INVALID", "launch_attempted": False,
                      "return_code": None, "timed_out": False, "stderr_class": "UNOBSERVED"}
        try:
            token, username = getattr(self.registry, "token", None), getattr(self.registry, "username", None)
            require(type(token) is str and token and not any(ord(char) < 33 for char in token)
                    and type(username) is str and re.fullmatch("[A-Za-z0-9-]{1,100}", username), "PUSH_AUTH_REQUIRED")
            diagnostic["failure"] = "NO_INVOCATION"
            with tempfile.TemporaryDirectory(prefix=".auth-", dir=self.output) as directory:
                authfile = Path(directory) / "auth.json"
                write_private(authfile, {"auths": {"ghcr.io": {"auth": base64.b64encode((username + ":" + token).encode()).decode()}}})
                digestfile = Path(directory) / "digest"
                diagnostic["stage"] = "LAUNCH"
                command(["podman", "push", "--retry=0", "--tls-verify=true", "--authfile", str(authfile),
                         "--digestfile", str(digestfile), self.smoke["image_id"], "docker://" + PACKAGE + ":" + self.tag], timeout=600)
                diagnostic.update(stage="DIGEST", failure="DIGEST_IO_ERROR", launch_attempted=True, return_code=0)
                digest = digestfile.read_text().strip()
                diagnostic["failure"] = "DIGEST_INVALID"
                require(re.fullmatch("sha256:[0-9a-f]{64}", digest), "PUSH_DIGEST_INVALID")
                diagnostic.update(stage="RETURN", failure="CLEANUP_ERROR")
            return digest
        except CommandFailure as error:
            diagnostic.update(stage=error.stage, failure=error.failure, launch_attempted=error.launch_attempted,
                              return_code=error.return_code, timed_out=error.timed_out, stderr_class=error.stderr_class)
            failure = error
        except (RuntimeError, OSError, ValueError, KeyError, TypeError, AttributeError):
            failure = RuntimeError("PUSH_" + diagnostic["failure"])
        diagnostic_unavailable = False
        try:
            write_private(self.output / "push-failure.json", diagnostic)
        except (RuntimeError, OSError):
            diagnostic_unavailable = True
        if diagnostic_unavailable:
            raise RuntimeError("PUSH_DIAGNOSTIC_UNAVAILABLE")
        raise failure

    def execute(self):
        authority = self.registry.package(bootstrap=self.bootstrap)
        existing = self.registry.lookup(self.tag)
        if existing is not None:
            require(self.retained is not None, "EXISTING_TAG_REQUIRES_EXACT_retained_RECEIPT")
            prior = validate_retained(self.retained)
            require(prior["source"] == self.source and prior["tag"] == self.tag and prior["registry_receipt"] == existing,
                    "EXISTING_TAG_COLLISION")
            try:
                result = completed_receipt(prior, self.registry.public(existing))
            except RuntimeError:
                result = {**prior, "status": "PUBLICITY_PENDING", "public_receipt": None, "resolved_image": None}
            return self.state(result)
        require(not self.bootstrap or authority["visibility"] == "absent", "FIRST_PACKAGE_ALREADY_EXISTS")
        self.state({"schema_version": 1, "status": "PREPARED", "source": self.source, "tag": self.tag,
                    "architecture": self.architecture, "authority": authority})
        self.smoke = self.build_and_smoke()
        self.recheck_source()
        current_authority = self.registry.package(bootstrap=self.bootstrap)
        require(not self.bootstrap or current_authority["visibility"] == "absent", "FIRST_PACKAGE_CHANGED_BEFORE_PUSH")
        require(self.registry.lookup(self.tag) is None, "TAG_CHANGED_BEFORE_PUSH")
        self.recheck_source()
        pending = {"schema_version": 1, "status": "PUSH_OUTCOME_UNKNOWN", "role": "native-runner", "source": self.source,
                   "tag": self.tag, "architecture": self.architecture, "smoke_receipt": self.smoke,
                   "smoke_receipt_sha256": sha(canonical(self.smoke)), "authority": current_authority}
        self.state(pending)
        try:
            digest = self.push()
        except RuntimeError:
            # Read once after an uncertain write; never convert uncertainty into a retry.
            try:
                observed = self.registry.raw_graph(self.tag)
                require(observed["config_digest"] == self.smoke["image_id"], "UNKNOWN_PUSH_FOREIGN_IDENTITY")
                pending["observed_after_unknown_push"] = observed
            except RuntimeError:
                pending["observed_after_unknown_push"] = None
            self.state(pending)
            raise RuntimeError("PUSH_OUTCOME_UNKNOWN") from None
        graph = self.registry.raw_graph(self.tag)
        require(graph is not None and graph["manifest_digest"] == digest, "PUSH_READBACK_DIGEST_MISMATCH")
        graph = validate_graph(base64.b64decode(graph["manifest_base64"], validate=True),
                               base64.b64decode(graph["config_base64"], validate=True), self.architecture, self.source, self.smoke)
        require(self.registry.raw_graph(digest) == graph, "PUSH_DIGEST_READBACK_MISMATCH")
        result = {**pending, "status": "PUBLICITY_PENDING", "os": "linux", "variant": graph["variant"], "index_digest": None,
                  "manifest_digest": digest, "config_digest": graph["config_digest"], "registry_receipt": graph,
                  "build_input_sha256": self.smoke["build_inputs_sha256"], "executable_sha256": self.smoke["runtime_binary_sha256"],
                  "public_receipt": None, "resolved_image": None}
        self.state(result)
        try:
            result = completed_receipt(result, self.registry.public(graph))
        except RuntimeError:
            return result
        return self.state(result)


@contextlib.contextmanager
def host_writer_lock(path):
    path = Path(path)
    info = path.parent.lstat()
    require(stat.S_ISDIR(info.st_mode) and not path.parent.is_symlink() and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, "WRITER_LOCK_DIRECTORY_INVALID")
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1
                and stat.S_IMODE(info.st_mode) == 0o600, "WRITER_LOCK_INVALID")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("WRITER_BUSY") from None
        yield
    finally:
        os.close(descriptor)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="operation", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--architecture", choices=("arm64", "amd64"), required=True)
    publish.add_argument("--expected-commit", required=True)
    publish.add_argument("--output-dir", type=Path, required=True)
    publish.add_argument("--retained-receipt", type=Path)
    publish.add_argument("--bootstrap-native", action="store_true")
    verify = commands.add_parser("verify-public")
    verify.add_argument("--receipt", type=Path, required=True)
    verify.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.operation == "verify-public":
            require(not args.output.exists(), "OUTPUT_RECEIPT_ALREADY_EXISTS")
            result = verify_public(read_private(args.receipt))
            write_private(args.output, result)
        else:
            source = source_identity(ROOT, args.expected_commit)
            require(command(["git", "branch", "--show-current"]).decode().strip() == BRANCH, "SOURCE_BRANCH_NOT_AUTHORIZED")
            if args.bootstrap_native:
                require(not os.environ.get("GITHUB_ACTIONS"), "FIRST_PACKAGE_HOST_ONLY")
            else:
                require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_EVENT_NAME") == "push"
                        and os.environ.get("GITHUB_REPOSITORY") == "ydixken/rpgmigrate"
                        and os.environ.get("GITHUB_REF") == "refs/heads/" + BRANCH
                        and os.environ.get("GITHUB_SHA") == source["commit"], "WORKFLOW_SOURCE_NOT_AUTHORIZED")
            with contextlib.redirect_stdout(sys.stderr):
                toolchain = tool_preflight(args.architecture, bootstrap=args.bootstrap_native)
            if args.bootstrap_native:
                token = command(["gh", "auth", "token"]).decode().strip()
                username = "ydixken"
            else:
                token, username = os.environ.pop("GITHUB_TOKEN", None), os.environ.get("GITHUB_ACTOR")
            require(type(token) is str and token and not any(ord(char) < 33 for char in token), "PUBLICATION_TOKEN_REQUIRED")
            registry = GHCR(source, args.architecture, token, username, args.bootstrap_native)
            retained = read_private(args.retained_receipt) if args.retained_receipt else None
            publisher = Publisher(registry, args.output_dir, source, args.architecture, retained=retained,
                                  bootstrap=args.bootstrap_native)
            publisher.toolchain = toolchain
            if args.bootstrap_native:
                lock_root = Path.home() / ".rpgmigrate-publication"
                lock_root.mkdir(mode=0o700, exist_ok=True)
                with host_writer_lock(lock_root / "first-native-writer.lock"):
                    result = publisher.execute()
            else:
                result = publisher.execute()
        print(json.dumps({"status": result["status"], "tag": result["tag"], "manifest_digest": result.get("manifest_digest")}, sort_keys=True))
        return 0 if result["status"] == "PUBLIC" else 2
    except (RuntimeError, OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        message = str(error) if type(error) in (RuntimeError, CommandFailure) and re.fullmatch("[A-Za-z_]+", str(error)) else "PUBLICATION_REFUSED"
        print("development publication failed: " + message, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
