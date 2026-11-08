#!/usr/bin/env python3
"""Bind native CI evidence and publish its exact two-platform OCI index."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
ARCHITECTURES = ("amd64", "arm64")
INDEX_MEDIA = "application/vnd.oci.image.index.v1+json"
MANIFEST_MEDIA = "application/vnd.oci.image.manifest.v1+json"
STAGES = ("ci-unit", "shellcheck", "yamllint", "actionlint", "native", "native-check",
          "lint", "test", "stock-refusal", "test-db-self-check", "test-db")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read_json(path):
    path = Path(path)
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 4 * 1024**2,
            "missing, unsafe, or oversized receipt")
    return json.loads(path.read_bytes())


def image_module(name):
    spec = importlib.util.spec_from_file_location("publish_" + name, ROOT / "tools/image" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_identity(source, run, attempt):
    require(re.fullmatch(r"[0-9a-f]{40}", source) is not None, "invalid source revision")
    require(all(re.fullmatch(r"[1-9][0-9]*", value) for value in (run, attempt)), "invalid workflow run identity")
    return {"source_revision": source, "run_id": run, "run_attempt": attempt}


def variant_tag(tag, version):
    require(version in ('16.15', '18.6'), 'unsupported client variant')
    require(not tag.endswith('-pg16'), 'pg16 suffix is reserved for the client variant')
    return tag + '-pg16' if version == '16.15' else tag


def destination(repository, tag):
    require(repository == "ghcr.io/ydixken/rpgmigrate", "unapproved image destination")
    require(len(tag) <= 128 and re.fullmatch(r"v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z]+(?:[.-][0-9A-Za-z]+)*)?", tag),
            "publication requires an existing version tag")
    return "docker://" + repository + ":" + tag


def execute(command, *, stdin=None, timeout=900):
    try:
        result = subprocess.run(command, input=stdin, capture_output=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        raise RuntimeError("required publication command unavailable or timed out") from None
    require(result.returncode == 0, "publication command failed; raw output withheld")
    require(len(result.stdout) <= 65536, "publication command output exceeds limit")
    return result.stdout


def evidence(directory, identity, architecture):
    require(architecture in ARCHITECTURES, "unsupported native architecture")
    directory = Path(directory)
    require(directory.is_dir() and not directory.is_symlink(), "missing or unsafe artifact directory")
    receipt_path = directory / "receipt.json"
    receipt = image_module("runtime").load_receipt(receipt_path)
    require(receipt["architecture"] == architecture and receipt["platform"] == "linux/" + architecture,
            "native artifact architecture mismatch")
    require(receipt["source"]["revision"] == identity["source_revision"], "artifact source revision mismatch")
    smoke_path = directory / "smoke.json"
    smoke = read_json(smoke_path)
    image_module("smoke").validate_smoke_receipt(receipt, smoke, digest(receipt_path))
    verification_path = directory / "verification.json"
    verification = read_json(verification_path)
    require(verification_path.stat().st_size <= 65536, "verification report exceeds limit")
    require(verification.get("status") == "passed" and
            [item.get("name") for item in verification.get("stages", [])] == list(STAGES) and
            all(item.get("status") == "passed" for item in verification["stages"]),
            "source verification is incomplete")
    require(verification.get("image_id") == smoke["fixture_image"], "smoke fixture differs from verified image")
    require(verification.get("architecture") in ({"amd64": ("x86_64", "amd64"), "arm64": ("aarch64", "arm64")}[architecture]),
            "source verification architecture mismatch")
    # The verifier hashes a larger source inventory than the runtime build context.
    require(re.fullmatch(r"[0-9a-f]{64}", verification.get("source_sha256", "")), "source verification identity missing")
    return receipt, {"schema": 1, **identity, "architecture": architecture,
                     "receipt_sha256": digest(receipt_path), "smoke_sha256": digest(smoke_path),
                     "verification_sha256": digest(verification_path),
                     "source_sha256": receipt["source"]["sha256"],
                     "verification_source_sha256": verification["source_sha256"]}


def bind_checkout(root, receipt, verification, identity):
    image_module("smoke").verify_source(root, receipt["source"]["files"])
    git = ["git", "-C", str(root)]
    require(execute(git + ["rev-parse", "HEAD"]).decode().strip() == identity["source_revision"],
            "checkout revision differs from this workflow")
    paths = sorted(receipt["source"]["files"])
    entries = execute(git + ["ls-tree", "-r", "-z", identity["source_revision"], "--", *paths])
    expected = {}
    for entry in entries.split(b"\0"):
        if entry:
            metadata, name = entry.split(b"\t", 1)
            mode, kind, object_id = metadata.decode().split()
            require(kind == "blob" and mode in ("100644", "100755"), "source tree contains unsupported input")
            expected[name.decode()] = object_id
    require(set(expected) == set(paths), "image includes source absent from the workflow commit")
    actual = execute(git + ["hash-object", "--no-filters", "--", *paths]).decode().splitlines()
    require(actual == [expected[path] for path in paths], "image source differs from the workflow commit")
    spec = importlib.util.spec_from_file_location("publish_verify", ROOT / "tools/ci/verify.py")
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    require(verifier.source_digest(root, verifier.source_files(root)) == verification["source_sha256"],
            "source changed after verification")


def seal(directory, smoke, verification, identity, architecture):
    directory = Path(directory)
    for source, name in ((smoke, "smoke.json"), (verification, "verification.json")):
        require(not (directory / name).exists(), "CI receipt destination already exists")
        require(Path(source).is_file() and not Path(source).is_symlink(), "required CI receipt missing")
        shutil.copyfile(source, directory / name)
    receipt, envelope = evidence(directory, identity, architecture)
    bind_checkout(ROOT, receipt, read_json(directory / "verification.json"), identity)
    with (directory / "ci.json").open("xb") as output:
        output.write(encode(envelope))


def bundles(root, identity):
    root = Path(root)
    require(root.is_dir() and not root.is_symlink(), "artifact root missing or unsafe")
    expected = {"native-" + arch for arch in ARCHITECTURES}
    require({path.name for path in root.iterdir()} == expected, "expected exactly two native artifacts")
    result = []
    for architecture in ARCHITECTURES:
        directory = root / ("native-" + architecture)
        require({path.name for path in directory.iterdir()} == {"image.oci.tar", "receipt.json", "smoke.json", "verification.json", "ci.json"},
                "native artifact file inventory mismatch")
        receipt, expected_envelope = evidence(directory, identity, architecture)
        envelope = read_json(directory / "ci.json")
        require(type(envelope.get("schema")) is int and envelope == expected_envelope, "CI receipt or workflow run mismatch")
        result.append((directory, receipt))
    require(result[0][1]["runtime"]["clients"]["version"] == result[1][1]["runtime"]["clients"]["version"], "native images use different client variants")
    require(result[0][1]["source"] == result[1][1]["source"], "native images use different source bytes")
    envelopes = [read_json(path / "ci.json") for path, _ in result]
    require(envelopes[0]["verification_source_sha256"] == envelopes[1]["verification_source_sha256"],
            "native verifications use different source bytes")
    return result


def assemble_index(layouts, destination_path):
    require(len(layouts) == 2 and sorted(receipt["architecture"] for _, receipt in layouts) == list(ARCHITECTURES),
            "expected exactly linux/amd64 and linux/arm64")
    destination_path = Path(destination_path)
    blobs = destination_path / "blobs/sha256"
    blobs.mkdir(parents=True, exist_ok=False)
    descriptors = []
    for path, receipt in sorted(layouts, key=lambda item: item[1]["architecture"]):
        index = read_json(path / "index.json")
        require(index.get("schemaVersion") == 2 and len(index.get("manifests", [])) == 1, "local OCI descriptor inventory mismatch")
        descriptor = index["manifests"][0]
        require(descriptor.get("mediaType") == MANIFEST_MEDIA and descriptor.get("digest") == receipt["image"]["manifest_digest"],
                "local OCI manifest changed")
        manifest_path = path / "blobs/sha256" / descriptor["digest"][7:]
        require(manifest_path.stat().st_size == descriptor["size"] and
                read_json(manifest_path)["config"]["digest"] == receipt["image"]["config_digest"], "local OCI config changed")
        for blob in (path / "blobs/sha256").iterdir():
            require(blob.is_file() and not blob.is_symlink() and re.fullmatch(r"[0-9a-f]{64}", blob.name) and digest(blob) == blob.name,
                    "local OCI blob digest mismatch")
            target = blobs / blob.name
            if not target.exists():
                shutil.copyfile(blob, target)
        descriptors.append({"mediaType": MANIFEST_MEDIA, "digest": descriptor["digest"], "size": descriptor["size"],
                            "platform": {"os": "linux", "architecture": receipt["architecture"]}})
    index_bytes = encode({"schemaVersion": 2, "mediaType": INDEX_MEDIA, "manifests": descriptors})
    index_digest = hashlib.sha256(index_bytes).hexdigest()
    (blobs / index_digest).write_bytes(index_bytes)
    (destination_path / "oci-layout").write_bytes(encode({"imageLayoutVersion": "1.0.0"}))
    (destination_path / "index.json").write_bytes(encode({"schemaVersion": 2, "manifests": [{
        "mediaType": INDEX_MEDIA, "digest": "sha256:" + index_digest, "size": len(index_bytes),
        "annotations": {"org.opencontainers.image.ref.name": "release"}}]}))
    return index_bytes


def validate_index(actual, expected):
    require(hashlib.sha256(actual).digest() == hashlib.sha256(expected).digest(), "destination OCI index digest mismatch")
    index = json.loads(actual)
    require(index.get("schemaVersion") == 2 and index.get("mediaType") == INDEX_MEDIA and
            [entry.get("platform") for entry in index.get("manifests", [])] ==
            [{"os": "linux", "architecture": arch} for arch in ARCHITECTURES], "destination platform inventory mismatch")


def validate_manifest(actual, receipt):
    require("sha256:" + hashlib.sha256(actual).hexdigest() == receipt["image"]["manifest_digest"],
            "destination child manifest digest mismatch")
    require(json.loads(actual)["config"]["digest"] == receipt["image"]["config_digest"],
            "destination child config digest mismatch")


def publish(root, identity, repository, tag):
    destination(repository, tag)
    inputs = bundles(root, identity)
    target = destination(repository, variant_tag(tag, inputs[0][1]["runtime"]["clients"]["version"]))
    with tempfile.TemporaryDirectory(prefix="rpgmigrate-publish-") as temporary:
        temporary = Path(temporary)
        layouts = []
        for directory, receipt in inputs:
            layout = temporary / receipt["architecture"]
            execute(["skopeo", "copy", "--preserve-digests", "oci-archive:" + str(directory / "image.oci.tar"), "oci:" + str(layout)])
            layouts.append((layout, receipt))
        merged = temporary / "index"
        expected = assemble_index(layouts, merged)
        require(os.environ.get("GHCR_TOKEN") and os.environ.get("GHCR_USER"), "registry credentials missing")
        auth = temporary / "auth.json"
        execute(["skopeo", "login", "--authfile", str(auth), "--username", os.environ["GHCR_USER"],
                 "--password-stdin", "ghcr.io"], stdin=os.environ["GHCR_TOKEN"].encode())
        execute(["skopeo", "copy", "--authfile", str(auth), "--all", "--preserve-digests", "oci:" + str(merged) + ":release", target])
        actual = execute(["skopeo", "inspect", "--authfile", str(auth), "--raw", target])
        validate_index(actual, expected)
        for _, receipt in inputs:
            child = "docker://" + repository + "@" + receipt["image"]["manifest_digest"]
            validate_manifest(execute(["skopeo", "inspect", "--authfile", str(auth), "--raw", child]), receipt)
        return "sha256:" + hashlib.sha256(expected).hexdigest()


def preflight(architecture):
    versions = {}
    for tool in ("podman", "skopeo"):
        output = execute([tool, "--version"], timeout=15).decode().strip()
        match = re.fullmatch(tool + r" version ([0-9]+(?:\.[0-9]+){1,3})", output)
        require(match is not None, "installed container tool version is invalid")
        versions[tool] = match[1]
    actual = execute(["podman", "info", "--format", "{{.Host.Arch}}"], timeout=30).decode().strip()
    actual = {"x86_64": "amd64", "aarch64": "arm64"}.get(actual, actual)
    require(actual == architecture and architecture in ARCHITECTURES, "host engine architecture mismatch")
    return {**versions, "architecture": actual}


def write_report(path, record):
    safe = {"schema": 1, "status": "passed" if record.get("status") == "passed" else "failed"}
    for name in ("index_digest", "source_revision"):
        value = record.get(name, "")
        if isinstance(value, str) and re.fullmatch(r"(?:sha256:)?[0-9a-f]{40,64}", value):
            safe[name] = value
    versions = record.get("tools", {})
    if isinstance(versions, dict):
        safe["tools"] = {key: value for key, value in versions.items() if key in ("podman", "skopeo")
                         and isinstance(value, str) and re.fullmatch(r"[0-9]+(?:\.[0-9]+){1,3}", value)}
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(encode(safe) + b"\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("seal", "publish"):
        child = subparsers.add_parser(name)
        child.add_argument("--source", required=True)
        child.add_argument("--run-id", required=True)
        child.add_argument("--run-attempt", required=True)
        child.add_argument("--directory", type=Path, required=True)
        if name == "seal":
            child.add_argument("--smoke", type=Path, required=True)
            child.add_argument("--verification", type=Path, required=True)
            child.add_argument("--architecture", choices=ARCHITECTURES, required=True)
        else:
            child.add_argument("--repository", required=True)
            child.add_argument("--tag", required=True)
            child.add_argument("--report", type=Path, required=True)
    host = subparsers.add_parser("preflight")
    host.add_argument("--architecture", choices=ARCHITECTURES, required=True)
    host.add_argument("--report", type=Path, required=True)
    summary = subparsers.add_parser("report")
    summary.add_argument("--report", type=Path, required=True)
    summary.add_argument("--status", choices=("success", "failure", "cancelled", "skipped"), required=True)
    args = parser.parse_args()
    if args.command in ("preflight", "report"):
        record = {"status": "failed"}
        try:
            if args.command == "preflight":
                record["tools"] = preflight(args.architecture)
            else:
                if args.report.is_file():
                    record = read_json(args.report)
                record["status"] = "passed" if args.status == "success" else "failed"
            return 0
        except (RuntimeError, OSError, ValueError, KeyError, TypeError):
            print("container tooling preflight failed; raw output withheld", file=sys.stderr)
            return 1
        finally:
            write_report(args.report, record)
    report = {"status": "failed", "source_revision": args.source}
    if args.command == "publish" and args.report.is_file():
        try:
            report["tools"] = read_json(args.report).get("tools", {})
        except (OSError, ValueError, RuntimeError):
            pass
    try:
        identity = run_identity(args.source, args.run_id, args.run_attempt)
        if args.command == "seal":
            seal(args.directory, args.smoke, args.verification, identity, args.architecture)
        else:
            report["index_digest"] = publish(args.directory, identity, args.repository, args.tag)
        report["status"] = "passed"
        return 0
    except (RuntimeError, OSError, ValueError, KeyError, TypeError, ImportError):
        print("image publication refused: missing or mismatched evidence; raw output withheld", file=sys.stderr)
        return 1
    finally:
        if args.command == "publish":
            write_report(args.report, report)


if __name__ == "__main__":
    sys.exit(main())
