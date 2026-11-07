#!/usr/bin/env python3
"""Install and verify the pinned Task binary in this checkout's download cache."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
VERSION = "3.54.0"
# Verified against the release API digests and v3.54.0/task_checksums.txt.
SHA256 = {
    "darwin_amd64": "0543b0844a725814c0412d3f4bed12873ce9ed0da207b15775f63a97d734aac0",
    "darwin_arm64": "12a69fcf37bb8d5bc9150c44f9a936775b73e59ff044c2c245232f198c135637",
    "linux_amd64": "680859dbb4d881a9c72d4d9a8f510825450849af8567deacd7302c01124416fb",
    "linux_arm64": "d7189d439d4a6058e39e1b2acd7c8b27acd4856691b408f23b242e4294e1df5b",
}
RELEASE = f"https://github.com/go-task/task/releases/download/v{VERSION}"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def local_path(path):
    for part in (path, *path.parents):
        if part == ROOT:
            return
        if part.is_symlink():
            raise RuntimeError("Task cache must not contain symlinks")
    raise RuntimeError("Task cache must remain inside the checkout")


def bootstrap():
    if os.environ.get("TASK_DRY", "false").lower() not in ("", "0", "false"):
        raise RuntimeError("unset TASK_DRY before executing verification tasks")
    architecture = {"x86_64": "amd64", "amd64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(platform.machine())
    key = f"{platform.system().lower()}_{architecture}"
    if key not in SHA256:
        raise RuntimeError("unsupported Task platform; use macOS or Linux on amd64 or arm64")
    cache = ROOT / "target/tools/task" / VERSION
    local_path(cache)
    cache.mkdir(parents=True, exist_ok=True)
    lock = cache / ".lock"
    local_path(lock)
    with lock.open("a") as locked:
        fcntl.flock(locked, fcntl.LOCK_EX)
        archive = cache / f"task_{key}.tar.gz"
        local_path(archive)
        if not archive.exists():
            with tempfile.TemporaryDirectory(prefix="download-", dir=cache) as temporary:
                download = Path(temporary) / "task.tar.gz"
                with urllib.request.urlopen(f"{RELEASE}/{archive.name}", timeout=60) as response, download.open("wb") as output:
                    shutil.copyfileobj(response, output)
                if digest(download) != SHA256[key]:
                    raise RuntimeError("Task archive SHA-256 mismatch")
                download.replace(archive)
        if not archive.is_file() or digest(archive) != SHA256[key]:
            raise RuntimeError("cached Task archive SHA-256 mismatch")
        binary = cache / "bin/task"
        local_path(binary)
        binary.parent.mkdir(exist_ok=True)
        with tarfile.open(archive, "r:gz") as source:
            for name, destination in (("task", binary), ("LICENSE", cache / "LICENSE")):
                members = [member for member in source.getmembers() if member.name == name]
                if len(members) != 1 or not members[0].isfile():
                    raise RuntimeError(f"Task archive lacks a regular {name} file")
                local_path(destination)
                with source.extractfile(members[0]) as stream:
                    contents = stream.read()
                if destination.exists():
                    if not destination.is_file() or destination.read_bytes() != contents:
                        raise RuntimeError("cached Task binary or license differs from verified archive")
                else:
                    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as output:
                        staged = Path(output.name)
                        try:
                            output.write(contents)
                            output.flush()
                            staged.chmod(0o755 if name == "task" else 0o644)
                            staged.replace(destination)
                        finally:
                            staged.unlink(missing_ok=True)
        version = subprocess.run([str(binary), "--version"], capture_output=True, text=True, timeout=10, check=True)
        if version.stdout.strip() != VERSION:
            raise RuntimeError("cached Task binary version differs from configured version")
        return binary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="store_true", help="print the configured version without installing or verifying")
    args = parser.parse_args()
    try:
        print(VERSION if args.version else bootstrap())
        return 0
    except (OSError, RuntimeError, tarfile.TarError, subprocess.SubprocessError):
        print(json.dumps({"level": "error", "message": "Task bootstrap failed; check the platform, cache, network, and TASK_DRY"}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
