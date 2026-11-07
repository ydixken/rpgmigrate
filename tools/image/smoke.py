#!/usr/bin/env python3
"""Qualify one exported runtime image with disposable, owned database fixtures."""

if not __debug__:
    raise SystemExit("image smoke requires Python assertions; remove -O/-OO and unset PYTHONOPTIMIZE")

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import secrets
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
LABEL = "rpgmigrate.image-smoke"
SMOKE_CHECKS = (
    "archive-identity", "native-platform", "loaded-image-identity", "runtime-files",
    "runtime-config", "help-version", "native-capability", "native-same-dso",
    "native-stock-refusal", "oauth-load", "ping", "copy-contents", "copy-oracle-controls",
    "synthetic-redaction", "signal-sigint", "signal-sigterm", "persistent-workdir",
    "state-permission", "state-lock", "state-symlink", "legacy-state", "prior-native-state",
    "cleanup-owned",
)


class SmokeError(RuntimeError):
    pass


def canonical_sha256(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def bindings(build, receipt_sha256):
    return {
        "receipt_sha256": receipt_sha256, "archive_sha256": build["archive"]["sha256"],
        "manifest_digest": build["image"]["manifest_digest"],
        "config_digest": build["image"]["config_digest"], "platform": build["platform"],
        "architecture": build["architecture"], "source_sha256": build["source"]["sha256"],
        "binary_sha256": build["binary_sha256"], "native_sha256": canonical_sha256(build["native"]),
        "runtime_sha256": canonical_sha256(build["runtime"]),
    }


def validate_smoke_receipt(build_receipt, smoke_record, receipt_sha256):
    expected = bindings(build_receipt, receipt_sha256)
    fields = set(expected) | {"schema", "status", "checks", "fixture_image", "fixture_version",
                              "fixture_version_num", "probe_sha256"}
    if (not isinstance(smoke_record, dict) or set(smoke_record) != fields
            or type(smoke_record.get("schema")) is not int or smoke_record["schema"] != 1
            or smoke_record.get("status") != "passed"
            or any(smoke_record.get(key) != value for key, value in expected.items())):
        raise SmokeError("smoke receipt identity or status differs")
    if (not re.fullmatch(r"sha256:[0-9a-f]{64}", str(smoke_record["fixture_image"]))
            or smoke_record["fixture_version"] != "17.11"
            or smoke_record["fixture_version_num"] != 170011
            or not re.fullmatch(r"[0-9a-f]{64}", str(smoke_record["probe_sha256"]))):
        raise SmokeError("smoke fixture identity differs")
    checks = smoke_record["checks"]
    if (not isinstance(checks, list) or len(checks) != len(SMOKE_CHECKS)
            or any(not isinstance(item, dict) or set(item) != {"name", "status"}
                   or item["status"] != "passed" or not isinstance(item["name"], str) for item in checks)
            or sorted(item["name"] for item in checks) != sorted(SMOKE_CHECKS)):
        raise SmokeError("smoke checks are missing, duplicate, unknown, or unpassed")


def verify_source(root, files):
    if not isinstance(files, dict) or not files:
        raise SmokeError("source inventory is empty")
    root = root.resolve(strict=True)
    for name, expected in files.items():
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or str(relative) != name:
            raise SmokeError("source inventory path is unsafe")
        path = root / relative
        if any((root / PurePosixPath(*relative.parts[:n])).is_symlink()
               for n in range(1, len(relative.parts) + 1)) or not path.is_file():
            raise SmokeError("source file is missing or symlinked")
        require(path.stat().st_size <= 16 * 1024**2, "source file exceeds its size limit")
        with path.open("rb") as stream:
            observed = hashlib.file_digest(stream, "sha256").hexdigest()
        if observed != expected:
            raise SmokeError("source differs from the built image")


def execute(command, *, timeout=30, limit=1048576, check=True, input=None):
    # Raw commands and diagnostics can contain synthetic credentials.
    with tempfile.TemporaryFile() as stdout, tempfile.TemporaryFile() as stderr:
        try:
            child = subprocess.Popen(command, stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
                                     stdout=stdout, stderr=stderr, start_new_session=True)
        except OSError:
            raise SmokeError("required command is unavailable") from None
        try:
            if input is not None:
                child.stdin.write(input.encode())
                child.stdin.close()
            deadline = time.monotonic() + timeout
            while child.poll() is None:
                if stdout.tell() + stderr.tell() > limit:
                    raise SmokeError("command output limit exceeded")
                if time.monotonic() >= deadline:
                    raise SmokeError("command deadline exceeded")
                time.sleep(0.02)
            if stdout.tell() + stderr.tell() > limit:
                raise SmokeError("command output limit exceeded")
            stdout.seek(0)
            stderr.seek(0)
            result = subprocess.CompletedProcess(command, child.returncode,
                                                 stdout.read().decode("utf-8"), stderr.read().decode("utf-8"))
            if check and result.returncode:
                raise SmokeError("command failed; raw output withheld")
            return result
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)


def verify_container(info, container_id, token):
    if (not re.fullmatch(r"[0-9a-f]{64}", container_id) or info.get("Id") != container_id
            or info.get("Config", {}).get("Labels", {}).get(LABEL) != token):
        raise SmokeError("container ownership differs; refusing mutation")


def require(condition, message):
    if not condition:
        raise SmokeError(message)


def validate_runtime_config(config):
    for key, expected in {"User": "1000:1000", "WorkingDir": "/work",
                          "Entrypoint": ["/usr/local/bin/rpgmigrate"], "Cmd": ["--help"],
                          "StopSignal": "SIGTERM"}.items():
        require(config.get(key) == expected, "runtime configuration differs")
    environment = dict(value.split("=", 1) for value in config.get("Env", []))
    require(environment.get("HOME") == "/home/rpgmigrate" and environment.get("TMPDIR") == "/work",
            "runtime environment differs")
    require(not config.get("Volumes"), "runtime image declares untracked anonymous volumes")


def refusal(result, *, category=None, contains=None):
    require(0 < result.returncode < 128 and not result.stdout, "application did not report a handled refusal")
    try:
        record = json.loads(result.stderr)
    except ValueError:
        raise SmokeError("application refusal is not structured") from None
    require(isinstance(record, dict) and set(record) == {"level", "category", "exit_code", "message"}
            and record["level"] == "error" and record["exit_code"] == result.returncode,
            "application refusal fields differ")
    require(category is None or record["category"] == category, "application refusal category differs")
    require(contains is None or contains in record["message"], "application refusal reason differs")
    return record


def image_identity(value):
    require(isinstance(value, str) and re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", value),
            "image identity is not a complete SHA-256 digest")
    return "sha256:" + value.removeprefix("sha256:")


def validate_cli_result(result, mode):
    require(result.returncode == 0, "runtime CLI command failed")
    if mode == "help":
        require(not result.stdout and "rpgmigrate" in result.stderr, "runtime help output is absent")
    elif mode == "version":
        require(not result.stderr and "rpgmigrate" in result.stdout, "runtime version output is absent")
    elif mode == "ping":
        try:
            require(not result.stderr and json.loads(result.stdout) == {"command": "ping", "status": "complete"},
                    "runtime ping output differs")
        except ValueError:
            raise SmokeError("runtime ping output is absent") from None
    else:
        raise SmokeError("unsupported runtime CLI check")


def check_engine(engine, platform):
    require(engine in ("podman", "docker") and platform in ("linux/arm64", "linux/amd64"),
            "unsupported native engine or platform")
    version = execute([engine, "version", "--format", "{{.Client.Version}}"], timeout=15).stdout.strip()
    require(re.fullmatch(r"\d+\.\d+\.\d+[^\s]*", version), "container engine version is unavailable")
    architecture = execute([engine, "info", "--format",
                            "{{.Host.Arch}}" if engine == "podman" else "{{.OSType}}/{{.Architecture}}"],
                           timeout=15).stdout.strip().removeprefix("linux/")
    architecture = {"aarch64": "arm64", "x86_64": "amd64"}.get(architecture, architecture)
    require(platform == "linux/" + architecture, "image platform differs from the native engine")
    return version


class Containers:
    def __init__(self, engine):
        self.engine = engine
        self.token = secrets.token_hex(16)
        self.containers = []
        self.resources = []
        self.serial = 0

    def command(self, *arguments, **options):
        return execute([self.engine, *arguments], **options)

    def inspect(self, kind, identity):
        values = json.loads(self.command(kind, "inspect", identity).stdout)
        require(isinstance(values, list) and len(values) == 1, "container inventory is absent or ambiguous")
        return values[0]

    def setup(self):
        name = "rpgmigrate-smoke-" + self.token
        network = {"name": name, "id": None}
        self.resources.append(("network", network))
        created = self.command("network", "create", "--internal", "--label", LABEL + "=" + self.token,
                               name).stdout.strip()
        if self.engine == "podman":
            require(created == name, "created network name differs")
        else:
            require(re.fullmatch(r"[0-9a-f]{64}", created), "invalid created network ID")
            network["id"] = created
        self.network = self.owned_network(network)
        self.volume = self.command("volume", "create", "--label", LABEL + "=" + self.token, name).stdout.strip()
        require(self.volume == name, "invalid owned volume name")
        self.resources.append(("volume", self.volume))

    def owned_network(self, network):
        info = self.inspect("network", network["id"] or network["name"])
        require(isinstance(info, dict), "network inspect evidence is malformed")
        if self.engine == "podman":
            identity, name, labels = info.get("id"), info.get("name"), info.get("labels")
        else:
            identity, name, labels = info.get("Id") or info.get("ID"), info.get("Name"), info.get("Labels")
        require(isinstance(identity, str) and re.fullmatch(r"[0-9a-f]{64}", identity), "invalid owned network ID")
        require(name == network["name"] and isinstance(labels, dict) and labels.get(LABEL) == self.token,
                "network ownership differs; refusing mutation")
        require(network["id"] is None or identity == network["id"], "network identity differs; refusing mutation")
        network["id"] = identity
        return identity

    def create(self, image, args, *, fixture=False, entrypoint=None, environment=None):
        require(re.fullmatch(r"sha256:[0-9a-f]{64}", image), "container image must be an immutable ID")
        self.serial += 1
        command = ["create", "--pull=never", "--name", "rpgmigrate-smoke-" + self.token + "-" + str(self.serial),
                   "--label", LABEL + "=" + self.token, "--network", self.network,
                   "--cap-drop=ALL", "--security-opt=no-new-privileges", "--pids-limit=128",
                   "--mount", "type=volume,source=" + self.volume + ",target=/work"]
        if fixture:
            command += ["--user", "1000:1000", "--network-alias", "fixture", "--mount",
                        "type=bind,source=" + str(ROOT) + ",target=/source,readonly",
                        "--env", "TMPDIR=/work", "--env", "PG_BINDIR=/usr/lib/postgresql/17/bin",
                        "--env", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION=17.11",
                        "--env", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM=170011",
                        "--env", "PYTHONDONTWRITEBYTECODE=1"]
        else:
            command += ["--read-only"]
        if entrypoint is not None:
            command += ["--entrypoint", entrypoint]
        for key, value in (environment or {}).items():
            require(key in ("LD_LIBRARY_PATH", "LD_PRELOAD"), "unsupported runtime test environment")
            command += ["--env", key + "=" + value]
        identity = self.command(*command, image, *args).stdout.strip()
        require(re.fullmatch(r"[0-9a-f]{64}", identity), "invalid created container ID")
        self.containers.append(identity)
        self.owned(identity)
        self.command("start", identity)
        return identity

    def owned(self, identity):
        require(identity in self.containers, "container was not created by this smoke run")
        info = self.inspect("container", identity)
        verify_container(info, identity, self.token)
        return info

    def kill(self, identity, signum):
        require(signum in ("SIGINT", "SIGTERM", "SIGKILL"), "unsupported test signal")
        self.owned(identity)
        self.command("kill", "--signal", signum, identity)

    def finish(self, identity, timeout=45):
        self.owned(identity)
        output = self.command("wait", identity, timeout=timeout).stdout.strip()
        require(re.fullmatch(r"\d+", output), "container exit status is absent")
        logs = self.command("logs", identity, limit=65536)
        result = subprocess.CompletedProcess([], int(output), logs.stdout, logs.stderr)
        for canary in ("Grüße 日本語", "e1-wide-seed"):
            require(canary not in result.stdout + result.stderr, "row contents appeared in diagnostics")
        return result

    def remove(self, identity):
        self.owned(identity)
        self.command("rm", "--force", identity)
        self.containers.remove(identity)

    def run(self, image, args, **options):
        identity = self.create(image, args, **options)
        try:
            return self.finish(identity)
        finally:
            self.remove(identity)

    def close(self):
        failures = []
        for identity in reversed(self.containers[:]):
            try:
                self.remove(identity)
            except (SmokeError, OSError, ValueError, KeyError):
                failures.append("container")
        for resource in reversed(self.resources[:]):
            kind, identity = resource
            try:
                if kind == "network":
                    identity = self.owned_network(identity)
                else:
                    info = self.inspect(kind, identity)
                    require(info.get("Labels", {}).get(LABEL) == self.token, "resource ownership differs")
                    require(info.get("Name") == identity, "resource identity differs")
                self.command(kind, "rm", identity)
                self.resources.remove(resource)
            except (SmokeError, OSError, ValueError, KeyError):
                failures.append(kind)
        require(not failures, "owned resource cleanup failed")


class Fixture:
    def __init__(self, containers, image):
        self.containers = containers
        self.identity = containers.create(image, ["python3", "/source/tools/image/smoke.py", "--fixture"], fixture=True)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            require(containers.owned(self.identity)["State"]["Running"], "owned fixture exited before readiness")
            result = containers.command("exec", self.identity, "cat", "/work/image-fixture.json", check=False)
            if result.returncode == 0:
                self.context = json.loads(result.stdout)
                require(self.context["port"] != 5432 and self.context["server_version_num"] == 170011,
                        "fixture identity or server version differs")
                return
            time.sleep(0.05)
        raise SmokeError("owned fixture readiness deadline exceeded")

    def python(self, code, *arguments, timeout=45):
        prelude = (
            "import json,sys,os; from pathlib import Path; "
            "sys.path.insert(0,'/source/tests/support'); import db_harness as h; "
            "from e1_cases import Cases; "
            "c=json.loads(Path('/work/image-fixture.json').read_text()); h.assert_identity(c); "
            "m=h.load_manifest(); a=json.loads(sys.argv[1]); "
        )
        output = self.containers.command("exec", self.identity, "python3", "-c", prelude + code,
                                         json.dumps(arguments), timeout=timeout).stdout
        return json.loads(output) if output.strip() else None

    def databases(self, name):
        return self.python("print(json.dumps(h.databases(c,a[0])))", name)

    def sql(self, database, sql):
        return self.python("print(json.dumps(h.psql(c,a[0],a[1])))", database, sql)

    def conninfo(self, database, role, *, port=None, password=True):
        values = {"host": "fixture", "port": port or self.context["port"], "dbname": database,
                  "user": role, "connect_timeout": 3, "sslmode": "disable", "application_name": "rpgmigrate_image_smoke"}
        if password:
            values["password"] = self.context["tcp_password"]
        return " ".join(key + "='" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
                        for key, value in values.items())

    def state_check(self, path, output=None):
        code = "checker=Cases('/bin/false',c,m); checker.state=Path(a[0]); "
        code += "checker.no_completion()" if output is None else "checker.completed(2,10003,a[1],min_bytes=8*1024*1024)"
        self.python(code, path, output)


def fixture_main():
    sys.path.insert(0, str(ROOT / "tests/support"))
    from db_harness import OwnedCluster, load_manifest
    require(sys.platform == "linux" and os.getuid() == 1000 and Path.cwd() == Path("/work"),
            "run fixture mode only in the owned verifier container")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    with OwnedCluster(load_manifest(), tcp=True) as cluster:
        write_new_json(Path("/work/image-fixture.json"), cluster.context)
        require(stop.wait(timeout=900), "fixture lifetime deadline exceeded")
    return 0


def write_new_json(path, value):
    # Publish only complete evidence; readers must not observe a partial readiness file.
    fd, name = tempfile.mkstemp(prefix=".image-evidence-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream)
        os.link(temporary, path)
    finally:
        temporary.unlink()


class HoldCopySocket:
    def __init__(self, connection, held, release):
        self.connection, self.held, self.release = connection, held, release
        self.observed = False

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def sendall(self, frame):
        if frame[:1] == b"d" and not self.observed:
            require(len(frame) > 5, "COPY gate observed empty data")
            self.observed = True
            write_new_json(self.held, {"copy_bytes": len(frame) - 5})
            require(self.release.wait(timeout=60), "COPY gate release deadline exceeded")
        self.connection.sendall(frame)


def copy_gate_main(directory):
    sys.path.insert(0, str(ROOT / "tests/support"))
    from protocol_fault import ProtocolFault
    context = json.loads(Path("/work/image-fixture.json").read_text())
    directory = Path(directory)
    require(directory.parent == Path(context["root"]) and directory.name.startswith("gate-"),
            "COPY gate directory is not owned")
    directory.mkdir(mode=0o700)
    release = threading.Event()
    with socket.socket() as listener:
        listener.bind(("0.0.0.0", 0))
        listener.listen(1)
        listener.settimeout(0.2)
        class Listener:
            def accept(self):
                connection, address = listener.accept()
                return HoldCopySocket(connection, directory / "held.json", release), address
        fault = ProtocolFault(context, "observe")
        fault.listener = Listener()
        thread = threading.Thread(target=fault.run, daemon=True)
        thread.start()
        write_new_json(directory / "ready.json", {"port": listener.getsockname()[1]})
        try:
            deadline = time.monotonic() + 90
            while not (directory / "release").exists():
                require(not fault.errors and time.monotonic() < deadline, "COPY gate failed or expired")
                time.sleep(0.02)
        finally:
            fault.stopping.set()
            release.set()
            for connection in fault.sockets:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            thread.join(timeout=5)
            require(not thread.is_alive() and not fault.errors, "COPY gate did not stop cleanly")
        (directory / "finished").touch()
    return 0


PROBE = r'''#define _GNU_SOURCE
#include <dlfcn.h>
#include <stdio.h>
#include <unistd.h>
int main(void) {
    void *pq = dlopen("/opt/rpgmigrate/target/native/libpq/lib/libpq.so.5", RTLD_NOW | RTLD_GLOBAL);
    if (!pq) return 2;
    unsigned (*caps)(void) = (unsigned (*)(void))dlsym(pq, "PQrpgmigrateCapabilities");
    int (*version)(void) = (int (*)(void))dlsym(pq, "PQlibVersion");
    void *connect = dlsym(pq, "PQconnectStart");
    Dl_info marker_info, connect_info;
    if (!caps || !version || !connect || !dladdr((void *)caps, &marker_info) || !dladdr(connect, &connect_info)) return 3;
    void *oauth = dlopen("libpq-oauth-18.so", RTLD_NOW | RTLD_LOCAL);
    if (!oauth) return 4;
    printf("{\"uid\":%u,\"gid\":%u,\"pid\":%u,\"capabilities\":%u,\"version\":%d,\"same_dso\":%s,\"oauth\":true}\n",
           getuid(), getgid(), (unsigned)getpid(), caps(), version(),
           marker_info.dli_fbase == connect_info.dli_fbase ? "true" : "false");
    dlclose(oauth); dlclose(pq);
    return 0;
}
'''
SPOOF = r'''#include <fcntl.h>
#include <unistd.h>
unsigned PQrpgmigrateCapabilities(void) {
    int fd = open("/work/capability-called", O_WRONLY | O_CREAT, 0600);
    if (fd >= 0) close(fd);
    return 1;
}
'''


def prepare_probe(fixture):
    return fixture.python(
        "import hashlib,shutil,subprocess; "
        "Path('/work/image-probe.c').write_text(a[0]); Path('/work/image-spoof.c').write_text(a[1]); "
        "subprocess.run(['clang','-Wall','-Wextra','-Werror','/work/image-probe.c','-o','/work/image-probe','-ldl'],check=True,capture_output=True,timeout=30); "
        "subprocess.run(['clang','-shared','-fPIC','/work/image-spoof.c','-o','/work/image-spoof.so'],check=True,capture_output=True,timeout=30); "
        "paths=subprocess.run(['dpkg-query','-L','libpq5'],check=True,capture_output=True,text=True,timeout=10).stdout.splitlines(); "
        "libraries=[p for p in paths if p.endswith('/libpq.so.5')]; assert len(libraries)==1; "
        "Path('/work/stock').mkdir(); shutil.copyfile(libraries[0],'/work/stock/libpq.so.5'); "
        "print(json.dumps(hashlib.sha256(Path('/work/image-probe').read_bytes()).hexdigest()))",
        PROBE, SPOOF)


def runtime_files(containers, image, build):
    files = build["runtime"]["files"]
    regular = [name for name, record in files.items() if "sha256" in record]
    require(regular, "runtime file inventory is empty")
    script = "sha256sum -- " + " ".join(shlex.quote(name) for name in regular)
    result = containers.run(image, ["-c", script], entrypoint="/bin/sh")
    require(result.returncode == 0 and not result.stderr, "runtime file checks failed")
    actual = {}
    for line in result.stdout.splitlines():
        digest, name = line.split("  ", 1)
        require(name not in actual, "duplicate runtime file digest")
        actual[name] = {"sha256": digest}
    for name, record in files.items():
        if "link" in record:
            link = containers.run(image, [name], entrypoint="/usr/bin/readlink")
            require(link.returncode == 0 and not link.stderr, "runtime symlink check failed")
            actual[name] = {"link": link.stdout.rstrip("\n")}
    require(actual == files, "loaded runtime files differ from OCI receipt")
    result = containers.run(image, ["-W", "-f=${binary:Package}=${Version}\n"], entrypoint="/usr/bin/dpkg-query")
    require(result.returncode == 0 and result.stdout.splitlines() == build["packages"], "runtime package inventory differs")


def wait_fixture(fixture, expression, *arguments, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fixture.python("print(json.dumps(" + expression + "))", *arguments)
        if value:
            return value
        time.sleep(0.02)
    raise SmokeError("fixture observation deadline exceeded")


def copy_args(fixture, source, target, state=None, *, source_port=None):
    args = ["copy", "table-data", "--source", fixture.conninfo(source, "e1_source", port=source_port),
            "--target", fixture.conninfo(target, "e1_target")]
    return args + ["--dir", state] if state is not None else args


def signal_case(containers, image, fixture, signum, record):
    source, target = fixture.databases("image_" + signum.lower())
    directory = fixture.context["root"] + "/gate-" + signum.lower()
    state = fixture.context["root"] + "/state-" + signum.lower()
    containers.command("exec", "--detach", fixture.identity, "python3", "/source/tools/image/smoke.py",
                       "--copy-gate", directory)
    wait_fixture(fixture, "Path(a[0]).is_file()", directory + "/ready.json")
    port = fixture.python("print(Path(a[0]).read_text())", directory + "/ready.json")["port"]
    identity = containers.create(image, copy_args(fixture, source, target, state, source_port=port))
    try:
        wait_fixture(fixture, "Path(a[0]).is_file()", directory + "/held.json")
        held = fixture.python("print(Path(a[0]).read_text())", directory + "/held.json")
        require(held["copy_bytes"] > 0, "signal fault did not observe COPY data")
        info = containers.owned(identity)
        require(info["State"]["Running"], "application exited before COPY signal")
        executable = containers.command("exec", identity, "readlink", "/proc/1/exe").stdout.strip()
        require(executable == "/usr/local/bin/rpgmigrate", "application is not PID 1")
        status = containers.command("exec", identity, "cat", "/proc/1/status").stdout
        require(re.search(r"^Uid:\s+1000\s+1000\s+1000\s+1000$", status, re.M), "application UID differs")
        require(re.search(r"^Gid:\s+1000\s+1000\s+1000\s+1000$", status, re.M), "application GID differs")
        if signum == "SIGINT":
            competing = containers.run(image, copy_args(fixture, source, target, state))
            refusal(competing, category="BadState", contains="state-busy")
            fixture.state_check(state)
            record("state-lock")
        containers.kill(identity, signum)
        result = containers.finish(identity, timeout=10)
        failure = refusal(result)
        require("cancel" in failure["message"].lower() or "interrupt" in failure["message"].lower(),
                "signal did not reach application cancellation")
        fixture.state_check(state)
        require(fixture.sql(target, 'SELECT count(*) FROM "E1 space"."row ""payload";') == "0",
                "cancelled COPY committed target rows")
    finally:
        fixture.python("Path(a[0]).touch()", directory + "/release")
        wait_fixture(fixture, "Path(a[0]).is_file()", directory + "/finished", timeout=10)
        containers.remove(identity)
    record("signal-" + signum.lower())


def qualify(containers, image, fixture, build, record):
    runtime_files(containers, image, build)
    record("runtime-files")
    probe_sha256 = prepare_probe(fixture)
    probe = containers.run(image, [], entrypoint="/work/image-probe")
    require(probe.returncode == 0 and not probe.stderr, "native runtime probe failed")
    require(json.loads(probe.stdout) == {"uid": 1000, "gid": 1000, "pid": 1, "capabilities": 1,
                                       "version": 180006, "same_dso": True, "oauth": True},
            "native runtime probe evidence differs")
    record("runtime-config")
    record("native-capability")
    record("oauth-load")
    for args, mode in (([], "help"), (["--help"], "help"), (["--version"], "version")):
        result = containers.run(image, args)
        validate_cli_result(result, mode)
    record("help-version")
    source, target = fixture.databases("image_copy")
    ping = ["ping", "--source", fixture.conninfo(source, "e1_source"), "--target", fixture.conninfo(target, "e1_target")]
    validate_cli_result(containers.run(image, ping), "ping")
    record("ping")
    for env, name in (({"LD_LIBRARY_PATH": "/work/stock"}, "native-stock-refusal"),
                      ({"LD_LIBRARY_PATH": "/work/stock", "LD_PRELOAD": "/work/image-spoof.so"}, "native-same-dso")):
        result = containers.run(image, ping, environment=env)
        refusal(result, category="BadConfig", contains="native libpq lacks the required passfile redaction capability")
        require(not fixture.python("print(json.dumps(Path('/work/capability-called').exists()))"),
                "foreign capability marker was invoked")
        record(name)
    fixture.python("h.fingerprint(c,a[0],m,empty=True)", target)
    result = containers.run(image, copy_args(fixture, source, target))
    require(result.returncode == 0 and not result.stderr, "actual runtime COPY failed")
    fixture.python("h.assert_equal(c,a[0],a[1],m)", source, target)
    fixture.state_check("/work/rpgmigrate", result.stdout)
    record("copy-contents")
    manifest_sha = fixture.python("import hashlib; print(json.dumps(hashlib.sha256(Path('/work/rpgmigrate/manifest.json').read_bytes()).hexdigest()))")
    repeat = containers.run(image, copy_args(fixture, source, target))
    refusal(repeat, category="BadState")
    require(manifest_sha == fixture.python("import hashlib; print(json.dumps(hashlib.sha256(Path('/work/rpgmigrate/manifest.json').read_bytes()).hexdigest()))"),
            "repeat invocation changed completed native state")
    fixture.python("h.assert_equal(c,a[0],a[1],m)", source, target)
    record("persistent-workdir")
    record("prior-native-state")
    for case in ("oracle_missing_table", "oracle_changed_schema", "oracle_changed_value"):
        fixture.python("s,t=h.databases(c,a[0]); checker=Cases('/bin/false',c,m); checker.oracle_control(a[0],s,t); assert checker.result['oracle_rejected']", case)
    record("copy-oracle-controls")
    canary = "image-private-canary-" + secrets.token_hex(8)
    bad_passfile = fixture.context["root"] + "/" + canary
    fixture.python("p=Path(a[0]); p.write_text(a[1]); p.chmod(0o644)", bad_passfile, "*:*:*:*:" + canary)
    bad = fixture.conninfo(source, "e1_source", password=False) + " passfile='" + bad_passfile + "'"
    result = containers.run(image, ["ping", "--source", bad, "--target", fixture.conninfo(target, "e1_target")])
    refusal(result)
    require(canary not in result.stdout + result.stderr, "synthetic credential or passfile path leaked")
    bad = fixture.conninfo(source, "e1_source", password=False) + " password='" + canary + "'"
    result = containers.run(image, ["ping", "--source", bad, "--target", fixture.conninfo(target, "e1_target")])
    refusal(result)
    require(canary not in result.stdout + result.stderr, "synthetic password leaked")
    record("synthetic-redaction")
    for name in ("state-permission", "state-symlink", "legacy-state"):
        source, target = fixture.databases(name)
        state = fixture.context["root"] + "/" + name
        fixture.python("p=Path(a[0]); p.mkdir(mode=0o700)", state)
        if name == "state-permission":
            fixture.python("Path(a[0]).chmod(0o755)", state)
        else:
            fixture.python("Path(a[0],'source.db').write_text('disposable-legacy-canary')", state)
            if name == "state-symlink":
                fixture.python("Path(a[0]+'-link').symlink_to(a[0])", state)
                state += "-link"
        snapshot = fixture.python("import hashlib,stat; p=Path(a[0]); print(json.dumps([stat.S_IMODE(p.lstat().st_mode),sorted([q.name,hashlib.sha256(q.read_bytes()).hexdigest()] for q in p.iterdir())]))", state)
        result = containers.run(image, copy_args(fixture, source, target, state))
        refusal(result, category="BadState")
        fixture.state_check(state)
        after = fixture.python("import hashlib,stat; p=Path(a[0]); print(json.dumps([stat.S_IMODE(p.lstat().st_mode),sorted([q.name,hashlib.sha256(q.read_bytes()).hexdigest()] for q in p.iterdir())]))", state)
        require(snapshot == after, "state refusal changed existing files or permissions")
        fixture.python("h.fingerprint(c,a[0],m,empty=True)", target)
        record(name)
    for signum in ("SIGINT", "SIGTERM"):
        signal_case(containers, image, fixture, signum, record)
    return probe_sha256


def smoke(engine, receipt_path, fixture_image, output):
    sys.path.insert(0, str(Path(__file__).parent))
    from runtime import load_receipt, sha256
    require(sys.version_info >= (3, 12), "image smoke requires Python 3.12 or newer")
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", fixture_image), "fixture image must be an immutable ID")
    receipt_path = Path(receipt_path)
    build = load_receipt(receipt_path)
    receipt_sha256 = sha256(receipt_path)
    verify_source(ROOT, build["source"]["files"])
    output = Path(output).absolute()
    require(not output.exists() and not any(path.is_symlink() for path in (output, *output.parents)),
            "smoke output directory must be new and not symlinked")
    output.mkdir(mode=0o700, parents=True)
    report = {"schema": 1, "status": "failed", **bindings(build, receipt_sha256),
              "fixture_image": fixture_image, "fixture_version": "17.11", "fixture_version_num": 170011,
              "probe_sha256": "", "checks": []}
    def record(name):
        require(name in SMOKE_CHECKS and name not in [item["name"] for item in report["checks"]],
                "invalid smoke check accounting")
        report["checks"].append({"name": name, "status": "passed"})
        print("Runtime smoke: " + name, flush=True)
    containers = Containers(engine)
    fixture = None
    qualified = False
    try:
        record("archive-identity")
        check_engine(engine, build["platform"])
        record("native-platform")
        containers.command("load", "--input", str(receipt_path.parent / "image.oci.tar"), timeout=180)
        image = build["image"]["config_digest"]
        info = containers.inspect("image", image)
        require(image_identity(info["Id"]) == image and info["Architecture"] == build["architecture"] and info["Os"] == "linux",
                "loaded image identity differs")
        validate_runtime_config(info["Config"])
        info = containers.inspect("image", fixture_image)
        require(image_identity(info["Id"]) == fixture_image and info["Architecture"] == build["architecture"] and info["Os"] == "linux",
                "fixture image identity or native platform differs")
        record("loaded-image-identity")
        containers.setup()
        fixture = Fixture(containers, fixture_image)
        report["probe_sha256"] = qualify(containers, image, fixture, build, record)
        containers.kill(fixture.identity, "SIGTERM")
        require(containers.finish(fixture.identity, timeout=20).returncode == 0, "owned fixture did not stop cleanly")
        containers.remove(fixture.identity)
        fixture = None
        containers.close()
        containers.resources.clear()
        record("cleanup-owned")
        verify_source(ROOT, build["source"]["files"])
        require(sha256(receipt_path) == receipt_sha256 and sha256(receipt_path.parent / "image.oci.tar") == build["archive"]["sha256"],
                "image evidence changed during smoke")
        validate_smoke_receipt(build, {**report, "status": "passed"}, receipt_sha256)
        qualified = True
    except BaseException:
        qualified = False
        raise
    finally:
        try:
            if fixture is not None and containers.owned(fixture.identity)["State"]["Running"]:
                containers.kill(fixture.identity, "SIGTERM")
                containers.finish(fixture.identity, timeout=20)
            containers.close()
            if qualified:
                report["status"] = "passed"
        except (RuntimeError, OSError, ValueError, KeyError):
            report["status"] = "failed"
            raise SmokeError("owned resource cleanup failed") from None
        finally:
            encoded = json.dumps(report, sort_keys=True, indent=2) + "\n"
            require(len(encoded.encode()) <= 65536, "smoke receipt exceeds its size limit")
            write_new_json(output / "smoke.json", report)
    return report


def main():
    if sys.argv[1:] == ["--fixture"]:
        return fixture_main()
    if len(sys.argv) == 3 and sys.argv[1] == "--copy-gate":
        return copy_gate_main(sys.argv[2])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=("podman", "docker"))
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--fixture-image", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    smoke(args.engine, args.receipt, args.fixture_image, args.output)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        message = str(error) if isinstance(error, SmokeError) else "invalid or unavailable smoke input; raw output withheld"
        print("Runtime smoke: " + message, file=sys.stderr)
        sys.exit(1)
