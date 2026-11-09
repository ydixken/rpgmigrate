"""Run the exact receiver live cases against privately owned stock PostgreSQL images."""

if not __debug__:
    raise RuntimeError("receiver live checks require Python assertions")

import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
import uuid

IMAGES = {
    "16": ("94a8e6634f916ffafee47d9be81c53d964e6ed2f5175224fab66b2abacae7279", "160015"),
    "18": ("91e2759ab4ea7238bc7bed3893439d7abb8edd5c8ac24bab0571e4179f405235", "180006"),
}
CASES = (
    "cdc::receive::tests::live_configuration_and_logical_query_preserve_export_lifetime",
    "pg::follow_receive_tests::live_pipeline_commands_counts_dispatched_commands",
    "cdc::receive::tests::live_startup_refuses_slot_advanced_beyond_recovery_before_readiness",
)
LABEL = "io.rpgmigrate.receiver-test"


def command(arguments, **kwargs):
    return subprocess.run(arguments, capture_output=True, text=True, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    root = Path(tempfile.mkdtemp(prefix="rpgmigrate-follow-receive-live-"))
    root.chmod(0o700)
    token = uuid.uuid4().hex
    (root / "context.json").write_text(json.dumps({"root": str(root), "token": token}))
    secret = secrets.token_hex(24)
    environment_file = root / "environment"
    environment_file.touch(mode=0o600)
    environment_file.write_text("POSTGRES_PASSWORD=" + secret + "\n")
    receipts = []
    for major, (image, version) in IMAGES.items():
        container = None
        try:
            container = command([
                "podman", "run", "--pull=never", "--detach", "--name", f"rpgmigrate-receive-{token[:12]}-{major}",
                "--label", LABEL + "=" + token, "--publish", "127.0.0.1::5432",
                "--memory", "1g", "--cpus", "1", "--env-file", str(environment_file), image,
                "-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4",
            ]).stdout.strip()
            assert len(container) == 64
            deadline = time.monotonic() + 45
            while True:
                result = subprocess.run(["podman", "exec", container, "pg_isready", "-U", "postgres", "-h", "127.0.0.1"],
                                        capture_output=True, timeout=5)
                if result.returncode == 0:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("owned receiver fixture did not become ready")
                time.sleep(0.1)
            inspection = json.loads(command(["podman", "inspect", container]).stdout)[0]
            assert inspection["Config"]["Labels"][LABEL] == token
            port = inspection["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
            actual_version = command(["podman", "exec", container, "psql", "-X", "-U", "postgres", "-Atqc", "SHOW server_version_num"]).stdout.strip()
            assert actual_version == version
            environment = dict(os.environ)
            environment.update(RPGMIGRATE_TEST_FOLLOW_RECEIVE_CONTEXT=str(root / "context.json"),
                               RPGMIGRATE_TEST_FOLLOW_RECEIVE_SOURCE=f"host=127.0.0.1 port={port} dbname=postgres user=postgres password={secret} connect_timeout=3 sslmode=disable")
            results = []
            for index, case in enumerate(CASES):
                result = subprocess.run([str(binary), case, "--exact", "--ignored", "--nocapture", "--test-threads=1"],
                                        env=environment, capture_output=True, text=True, timeout=45)
                stdout = root / f"pg{major}-{index}.stdout"
                stderr = root / f"pg{major}-{index}.stderr"
                stdout.write_text(result.stdout)
                stderr.write_text(result.stderr)
                passed = result.returncode == 0 and f"test {case} ... ok" in result.stdout and "1 passed; 0 failed" in result.stdout
                results.append({"case": case, "exit": result.returncode, "passed": passed,
                                "stdout": str(stdout), "stderr": str(stderr),
                                "stdout_sha256": hashlib.sha256(stdout.read_bytes()).hexdigest(),
                                "stderr_sha256": hashlib.sha256(stderr.read_bytes()).hexdigest()})
            receipt = {"major": major, "server_version_num": actual_version, "image_id": image, "cases": results}
            receipts.append(receipt)
            print(json.dumps(receipt), flush=True)
        finally:
            if container:
                inspection = json.loads(command(["podman", "inspect", container]).stdout)[0]
                assert inspection["Config"]["Labels"][LABEL] == token
                command(["podman", "rm", "--force", "--volumes", container])
    receipt = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "results": receipts}
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print("receipt: " + str(root / "receipt.json"), flush=True)
    assert len(receipts) == 2 and all(case["passed"] for result in receipts for case in result["cases"])


if __name__ == "__main__":
    main()
