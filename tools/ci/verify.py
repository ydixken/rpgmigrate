#!/usr/bin/env python3
"""Run E1 in the owned Linux container and export only bounded, structured evidence."""

import ast
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile

PG_VERSION = "17.11"
ENGINE_CASES = 39
INTEGRATION_CASES = 40
CI_SUITES = (
    "tools/ci/test_verify.py", "tools/ci/test_publish.py", "tests/test_taskfile.py", "tools/ci/test_linux.py",
    "tools/image/test_build.py", "tools/image/test_runtime.py", "tools/image/test_smoke.py",
)


class VerificationError(RuntimeError):
    pass


def execute(command, *, timeout=900, environment=None):
    try:
        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              text=True, env=environment, start_new_session=True) as child:
            try:
                output, _ = child.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.communicate()
                raise VerificationError("command deadline exceeded") from None
    except OSError:
        raise VerificationError("required command unavailable") from None
    if child.returncode:
        names = re.findall(r"^test ([A-Za-z0-9_:.-]{1,200}) \.\.\. FAILED$|^(?:FAIL|ERROR): ([A-Za-z0-9_]{1,200}) \(", output, re.M)
        failures = ", ".join(first or second for first, second in names[:12]) or "no test failure names"
        raise VerificationError(f"command failed (exit {child.returncode}; {failures}); raw output withheld")
    return output


def ci_unit_inventory(root=None):
    root = Path(root) if root is not None else Path(__file__).resolve().parents[2]
    inventory = {}
    for name in CI_SUITES:
        try:
            tree = ast.parse((root / name).read_text())
        except (OSError, SyntaxError):
            raise VerificationError("CI unit source is missing or invalid") from None
        count = sum(isinstance(method, ast.FunctionDef) and method.name.startswith("test_")
                    for node in tree.body if isinstance(node, ast.ClassDef) for method in node.body)
        if count == 0:
            raise VerificationError("CI unit source inventory is empty")
        inventory[name] = count
    return inventory


def task_command(name):
    if os.environ.get("TASK_DRY", "false").lower() not in ("", "0", "false"):
        raise VerificationError("unset TASK_DRY before verification")
    return ["task", "--silent", "--output=interleaved", "--color=false", name]


def validate_output(stage, output):
    if stage == "ci-unit":
        python = [int(n) for n in re.findall(r"^Ran (\d+) tests? in ", output, re.M)]
        if python != [sum(ci_unit_inventory().values())] or "skipped=" in output:
            raise VerificationError("missing, empty, or skipped CI unit suite")
        return {"python": python}
    if stage == "test":
        rust = re.findall(r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", output, re.M)
        python = [int(n) for n in re.findall(r"^Ran (\d+) tests? in ", output, re.M)]
        if not rust or not sum(int(row[0]) for row in rust) or any(int(failed) or int(ignored) for _, failed, ignored in rust):
            raise VerificationError("missing, empty, failed, or ignored Rust unit suite")
        if len(python) != 3 or not all(python) or "skipped=" in output:
            raise VerificationError("missing, empty, or skipped Python suite")
        return {"rust": sum(int(row[0]) for row in rust), "python": python}
    if stage == "test-db":
        engine = re.findall(r"^Engine verified (\d+) exact cases; full E1 suite not selected$", output, re.M)
        full = re.findall(r"^E1 verified (\d+) exact cases on PostgreSQL ([\d.]+)$", output, re.M)
        if engine != [str(ENGINE_CASES)] or full != [(str(INTEGRATION_CASES), PG_VERSION)]:
            raise VerificationError("missing or incorrect E1 inventory/version receipt")
        faults = [line for line in output.splitlines() if line.startswith("Authority faults ")]
        if faults != ["Authority faults verified 2 held COMMIT replies and observed fence releases"]:
            raise VerificationError("missing or incorrect authority fault/fence receipt")
        replication_faults = [line for line in output.splitlines() if line.startswith("Replication faults ")]
        if replication_faults != ["Replication faults verified 4 wire cases and observed PQflush backpressure"]:
            raise VerificationError("missing or incorrect replication fault/backpressure receipt")
        return {"engine": ENGINE_CASES, "integration": INTEGRATION_CASES,
                "authority_faults": 2, "replication_faults": 4}
    if stage == "test-db-self-check":
        records = [json.loads(line) for line in output.splitlines() if line.startswith('{"fingerprints":')]
        if len(records) != 1 or any(records[0].get(key) != value for key, value in {
            "self_check": "passed", "server_version": PG_VERSION, "negative_controls": 3,
            "wire_faults": 2, "scram_auth": "passed",
        }.items()):
            raise VerificationError("missing or incorrect harness negative-control receipt")
        return {"negative_controls": 3, "wire_faults": 2, "scram_auth": "passed"}
    return {}


def source_files(source):
    files = []
    for name in ("Cargo.toml", "Cargo.lock", "Taskfile.yml", "Dockerfile", ".dockerignore", "build.rs", "NOTICE", "src", "tests", "tools", "native", "bench", "docs", ".github"):
        path = source / name
        if path.is_symlink():
            raise VerificationError("source symlinks are not supported")
        if not path.exists():
            raise VerificationError("required source input missing")
        for item in ([path] if path.is_file() else sorted(path.rglob("*"))):
            if "__pycache__" in item.parts or item.suffix == ".pyc":
                continue
            if item.is_symlink():
                raise VerificationError("source symlinks are not supported")
            if item.is_file():
                files.append(item.relative_to(source))
    return sorted(files)


def source_digest(source, files):
    digest = hashlib.sha256()
    for path in files:
        digest.update(str(path).encode() + b"\0" + hashlib.sha256((source / path).read_bytes()).digest())
    return digest.hexdigest()


def stock_refusal(binary):
    paths = execute(["dpkg-query", "-L", "libpq5"]).splitlines()
    libraries = [Path(path) for path in paths if path.endswith("/libpq.so.5")]
    if len(libraries) != 1 or not libraries[0].is_file():
        raise VerificationError("stock libpq fixture missing")
    with tempfile.TemporaryDirectory(prefix="e1-stock-") as temporary, socket.socket(socket.AF_UNIX) as listener:
        listener.bind(str(Path(temporary) / ".s.PGSQL.65431"))
        listener.listen(1)
        listener.setblocking(False)
        environment = dict(os.environ, LD_LIBRARY_PATH=str(libraries[0].parent))
        conninfo = f"host={temporary} port=65431 dbname=fixture user=fixture password=private-fixture connect_timeout=1"
        result = subprocess.run([str(binary), "ping", "--source", conninfo, "--target", conninfo],
                                env=environment, capture_output=True, text=True, timeout=10)
        expected = {"level": "error", "category": "BadConfig", "exit_code": 2,
                    "message": "native libpq lacks the required passfile redaction capability"}
        try:
            valid = result.returncode == 2 and not result.stdout and json.loads(result.stderr) == expected
        except ValueError:
            valid = False
        if not valid:
            raise VerificationError("stock library did not produce the exact safe refusal")
        try:
            connection, _ = listener.accept()
        except BlockingIOError:
            pass
        else:
            connection.close()
            raise VerificationError("stock library reached connection setup")
    return {"exit_code": 2, "category": "BadConfig", "connections": 0}


def main():
    if sys.platform != "linux" or os.geteuid() == 0 or Path.cwd() != Path("/work"):
        raise VerificationError("run tools/ci/linux.sh in its owned container")
    report = {"schema": 1, "status": "failed", "architecture": platform.machine(), "stages": []}
    artifacts = Path("target/ci-artifacts")
    artifacts.mkdir(parents=True, exist_ok=True)
    stage = "source-copy"
    try:
        source = Path("/source")
        files = source_files(source)
        report["source_sha256"] = source_digest(source, files)
        for path in files:
            path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / path, path)
        if source_digest(Path.cwd(), files) != report["source_sha256"]:
            raise VerificationError("source changed during container snapshot")
        report["image_id"] = os.environ["E1_IMAGE_ID"]
        report["base_image"] = os.environ["E1_BASE_IMAGE"]
        report["packages"] = Path("/opt/e1/packages.txt").read_text().splitlines()
        report["versions"] = {name: execute(command).strip() for name, command in {
            "rustc": ["rustc", "--version"], "cargo": ["cargo", "--version"],
            "python": ["python3", "--version"], "postgres": [os.environ["PG_BINDIR"] + "/postgres", "--version"],
            "shellcheck": ["shellcheck", "--version"], "actionlint": ["actionlint", "--version"],
            "task": ["task", "--version"],
        }.items()}
        expected_task = execute(["python3", "tools/bootstrap-task.py", "--version"]).strip()
        if report["versions"]["task"] != expected_task:
            raise VerificationError("Task version differs from the shared bootstrap pin")
        workflows = [str(path) for path in sorted(Path(".github/workflows").glob("*.y*ml"))]
        if not workflows:
            raise VerificationError("workflow inventory is empty")
        for stage, command in (
            ("ci-unit", ["python3", "tools/ci/unit.py"]),
            ("shellcheck", ["shellcheck", "tools/ci/linux.sh"]),
            ("yamllint", ["yamllint", "--strict", "-c", "tools/ci/yamllint.yml", "Taskfile.yml", "tools/ci/yamllint.yml", *workflows]),
            ("actionlint", ["actionlint", *workflows]),
            ("native", task_command("native")),
            ("native-check", task_command("native-check")),
            ("lint", task_command("lint")),
            ("test", task_command("test")),
            ("stock-refusal", None),
            ("test-db-self-check", task_command("test-db-self-check")),
            ("test-db", task_command("test-db")),
        ):
            print(f"E1: {stage}", flush=True)
            counts = stock_refusal(Path.cwd() / "target/debug/rpgmigrate") if command is None else validate_output(stage, execute(command))
            report["stages"].append({"name": stage, "status": "passed", **counts})
            if stage == "native-check":
                native = json.loads(execute(["python3", "tools/build-libpq.py", "--verify"]))
                receipt = json.loads(Path("target/native/libpq/artifact.json").read_text())
                report["native"] = {**native, "features": receipt["features"], "source_sha256": receipt["source"]["sha256"],
                                    "libpq_version": receipt["libpq_version"], "toolchain": receipt["toolchain"],
                                    "configure": receipt["configure"]}
                if not all(receipt["features"].get(key) is True for key in ("openssl", "gssapi", "oauth_libcurl")):
                    raise VerificationError("required native features absent")
            if stage == "test":
                report["binary_sha256"] = hashlib.sha256(Path("target/debug/rpgmigrate").read_bytes()).hexdigest()
        report["status"] = "passed"
        return 0
    except (VerificationError, OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        report["stages"].append({"name": stage, "status": "failed"})
        report["failure"] = str(error) if isinstance(error, VerificationError) else "invalid or unavailable verification input"
        print(f"E1 failed: {stage}: {report['failure']}", file=sys.stderr)
        return 1
    finally:
        encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if len(encoded.encode()) > 65536:
            raise VerificationError("evidence exceeds 64 KiB limit")
        (artifacts / "report.json").write_text(encoded)


if __name__ == "__main__":
    sys.exit(main())
