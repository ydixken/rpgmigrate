#!/usr/bin/env python3
"""Verify retained single-platform OCI bytes against an independently reviewed lock."""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys
import tarfile


MAX_ARCHIVE_BYTES = 8 * 1024**3
MAX_BLOB_BYTES = 4 * 1024**3
MAX_JSON_BYTES = 1024**2
MAX_ENTRIES = 512
MAX_LAYERS = 128
CHUNK_BYTES = 1024**2
INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
CONFIG = "application/vnd.oci.image.config.v1+json"
LAYERS = {"application/vnd.oci.image.layer.v1.tar", "application/vnd.oci.image.layer.v1.tar+gzip", "application/vnd.oci.image.layer.v1.tar+zstd"}
ARCHIVE_FIELDS = {"archive_sha256", "archive_index_digest", "platform_manifest_digest", "config_digest", "config_variant", "image_id", "layer_digests", "platform"}
LOCK_FIELDS = ARCHIVE_FIELDS | {"schema_version", "upstream_index_digest", "source_commit", "source_tree", "runtime_binary_sha256", "build_inputs_sha256", "source_inputs", "packages", "smoke_sha256"}
SMOKE_FIELDS = {"runtime_binary_sha256", "python", "source_inputs", "image_id", "architecture", "effective_uid", "version", "libpq", "clients", "default_psql", "build_inputs_sha256", "mounts", "checks"}
CHECKS = {"read_only_rootfs", "work_write", "tmp_write", "root_write_denied", "notices", "gnu_utilities"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def valid_hash(value):
    return type(value) is str and re.fullmatch("[0-9a-f]{64}", value) is not None


def valid_digest(value):
    return type(value) is str and value.startswith("sha256:") and valid_hash(value[7:])


def unique_fields(items):
    result = {}
    for key, value in items:
        require(key not in result, "duplicate JSON field")
        result[key] = value
    return result


def invalid_constant(value):
    raise RuntimeError("non-JSON numeric constant")


def decode(data):
    require(type(data) is bytes and 0 < len(data) <= MAX_JSON_BYTES, "JSON size bound")
    try:
        value = json.loads(data, object_pairs_hook=unique_fields, parse_constant=invalid_constant)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise RuntimeError("invalid candidate JSON") from error
    require(type(value) is dict, "expected JSON object")
    return value


def platform(value):
    require(type(value) is dict and set(value) == {"os", "architecture"} and value["os"] == "linux"
            and value["architecture"] in ("amd64", "arm64"), "unsupported or ambiguous platform")
    return value


def file_identity(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def inspect_archive(path, expected_platform, source_commit=None):
    """Read named blobs only; unsupported archive variants fail before metadata reads."""
    platform(expected_platform)
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            require(stat.S_ISREG(before.st_mode) and 1024 <= before.st_size <= MAX_ARCHIVE_BYTES
                    and before.st_size % 512 == 0, "archive size or file type")
            entries = {}
            seen = set()
            offset = 0
            terminated = False
            while offset + 512 <= before.st_size:
                source.seek(offset)
                header = source.read(512)
                if header == b"\0" * 512:
                    require(source.read(512) == b"\0" * 512, "missing archive terminator")
                    require(not any(source.read(CHUNK_BYTES)), "nonzero archive trailer")
                    while chunk := source.read(CHUNK_BYTES):
                        require(not any(chunk), "nonzero archive trailer")
                    terminated = True
                    break
                require(len(seen) < MAX_ENTRIES, "archive entry count bound")
                try:
                    item = tarfile.TarInfo.frombuf(header, "utf-8", "strict")
                except (tarfile.HeaderError, UnicodeError, ValueError) as error:
                    raise RuntimeError("invalid tar header") from error
                require(item.type in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE)
                        and not item.linkname and not item.sparse, "unsupported tar entry or extension")
                name = item.name
                if item.isdir() and name.endswith("/"):
                    name = name[:-1]
                require(name not in seen and name and not name.startswith("/") and "\\" not in name
                        and all(part not in ("", ".", "..") for part in name.split("/")), "duplicate or unsafe archive path")
                seen.add(name)
                require(type(item.size) is int and 0 <= item.size <= MAX_BLOB_BYTES, "blob size bound")
                end = offset + 512 + item.size
                require(end <= before.st_size, "truncated archive entry")
                if item.isdir():
                    require(name in ("blobs", "blobs/sha256") and item.size == 0, "unexpected archive directory")
                else:
                    require(name in ("oci-layout", "index.json") or re.fullmatch("blobs/sha256/[0-9a-f]{64}", name), "unexpected archive entry")
                    require(item.size > 0, "empty archive entry")
                    entries[name] = (offset + 512, item.size)
                offset = ((end + 511) // 512) * 512
            require(terminated and entries, "empty or unterminated archive")

            def read(name, limit=MAX_JSON_BYTES):
                require(name in entries, "missing archive blob")
                position, size = entries[name]
                require(size <= limit, "metadata size bound")
                source.seek(position)
                data = source.read(size)
                require(len(data) == size, "truncated blob")
                return data

            used = {"oci-layout", "index.json"}

            def blob(item, media, metadata=False):
                require(type(item) is dict and item.get("mediaType") in media and valid_digest(item.get("digest"))
                        and type(item.get("size")) is int and 0 < item["size"] <= MAX_BLOB_BYTES
                        and not any(key in item for key in ("urls", "data", "artifactType")), "invalid or unsupported OCI descriptor")
                name = "blobs/sha256/" + item["digest"][7:]
                require(name in entries and entries[name][1] == item["size"], "missing blob or descriptor size mismatch")
                used.add(name)
                position, size = entries[name]
                source.seek(position)
                hashed = hashlib.sha256()
                remaining = size
                while remaining:
                    chunk = source.read(min(CHUNK_BYTES, remaining))
                    require(chunk, "truncated blob")
                    hashed.update(chunk)
                    remaining -= len(chunk)
                require(hashed.hexdigest() == item["digest"][7:], "blob digest mismatch")
                return decode(read(name)) if metadata else None

            require(decode(read("oci-layout")) == {"imageLayoutVersion": "1.0.0"}, "unsupported OCI layout")
            raw_index = read("index.json")
            index = decode(raw_index)
            require(type(index.get("schemaVersion")) is int and index["schemaVersion"] == 2
                    and index.get("mediaType", INDEX) == INDEX, "invalid OCI wrapper index")
            manifests = index.get("manifests")
            require(type(manifests) is list and len(manifests) == 1, "ambiguous or empty platform manifests")
            selected = manifests[0]
            require(type(selected) is dict and selected.get("mediaType") == MANIFEST, "unsupported nested index or manifest media type")
            if "platform" in selected:
                require(platform(selected["platform"]) == expected_platform, "manifest platform mismatch")
            manifest = blob(selected, {MANIFEST}, metadata=True)
            require(type(manifest.get("schemaVersion")) is int and manifest["schemaVersion"] == 2
                    and manifest.get("mediaType", MANIFEST) == MANIFEST
                    and not any(key in manifest for key in ("artifactType", "subject")), "invalid OCI platform manifest")
            config_descriptor = manifest.get("config")
            config = blob(config_descriptor, {CONFIG}, metadata=True)
            require({"os": config.get("os"), "architecture": config.get("architecture")} == expected_platform
                    and ("variant" not in config or (expected_platform["architecture"] == "arm64"
                         and config["variant"] == "v8")), "config platform mismatch")
            if source_commit is not None:
                publication = image_contract("dev_publish")
                publication.validate_graph(read("blobs/sha256/" + selected["digest"][7:]),
                                           read("blobs/sha256/" + config_descriptor["digest"][7:]),
                                           expected_platform["architecture"],
                                           {"repository": publication.REPOSITORY, "commit": source_commit})
            layers = manifest.get("layers")
            require(type(layers) is list and 0 < len(layers) <= MAX_LAYERS, "layer count bound")
            layer_digests = []
            for layer in layers:
                blob(layer, LAYERS)
                layer_digests.append(layer["digest"])
            rootfs = config.get("rootfs")
            require(type(rootfs) is dict and rootfs.get("type") == "layers" and type(rootfs.get("diff_ids")) is list
                    and len(rootfs["diff_ids"]) == len(layers) and all(valid_digest(value) for value in rootfs["diff_ids"]), "invalid config layer identity")
            require(set(entries) == used, "unreferenced archive entries")
            source.seek(0)
            hashed = hashlib.sha256()
            while chunk := source.read(CHUNK_BYTES):
                hashed.update(chunk)
            require(file_identity(os.fstat(source.fileno())) == file_identity(before)
                    and file_identity(os.stat(path, follow_symlinks=False)) == file_identity(before), "archive changed during verification")
            return {"archive_sha256": hashed.hexdigest(), "archive_index_digest": "sha256:" + sha256(raw_index),
                    "platform_manifest_digest": selected["digest"], "config_digest": config_descriptor["digest"],
                    "image_id": config_descriptor["digest"], "config_variant": config.get("variant"),
                    "layer_digests": layer_digests, "platform": dict(expected_platform)}
    except (OSError, KeyError, TypeError, ValueError, OverflowError) as error:
        raise RuntimeError("cannot inspect candidate archive") from error


def image_contract(name):
    try:
        spec = importlib.util.spec_from_file_location("candidate_image_" + name, Path(__file__).with_name(name + ".py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except (OSError, ImportError, AttributeError) as error:
        raise RuntimeError("unavailable local image contract") from error


def validate_lock(expected):
    require(type(expected) is dict and set(expected) == LOCK_FIELDS
            and type(expected["schema_version"]) is int and expected["schema_version"] == 1, "invalid candidate lock fields")
    platform(expected["platform"])
    require(expected["config_variant"] is None or (expected["platform"]["architecture"] == "arm64"
            and expected["config_variant"] == "v8"), "unsupported expected config variant")
    for name in ("source_commit", "source_tree"):
        require(type(expected[name]) is str and re.fullmatch("(?:[0-9a-f]{40}|[0-9a-f]{64})", expected[name]), "invalid source identity")
    for name in ("archive_sha256", "runtime_binary_sha256", "build_inputs_sha256", "smoke_sha256"):
        require(valid_hash(expected[name]), "invalid expected SHA-256")
    for name in ("archive_index_digest", "platform_manifest_digest", "config_digest", "image_id"):
        require(valid_digest(expected[name]), "invalid expected OCI digest")
    require(expected["config_digest"] == expected["image_id"], "config and image ID mismatch")
    require(expected["upstream_index_digest"] is None or valid_digest(expected["upstream_index_digest"]), "invalid upstream index provenance")
    require(type(expected["layer_digests"]) is list and 0 < len(expected["layer_digests"]) <= MAX_LAYERS
            and all(valid_digest(value) for value in expected["layer_digests"]), "invalid expected layers")


def verify_smoke(expected, smoke_bytes):
    try:
        receipt = decode(smoke_bytes)
        require(set(receipt) == SMOKE_FIELDS, "missing or unsupported smoke fields")
        require(receipt["image_id"] == expected["image_id"] and receipt["architecture"] == expected["platform"]["architecture"]
                and receipt["runtime_binary_sha256"] == expected["runtime_binary_sha256"], "smoke image or binary mismatch")
        require(type(receipt["effective_uid"]) is int and receipt["effective_uid"] == 65532
                and receipt["version"] == image_contract("smoke").qualified_version() and type(receipt["version"].get("protocol_version")) is int, "unqualified smoke contract")
        inputs = expected["source_inputs"]
        require(type(inputs) is list and inputs and all(type(value) is str and re.fullmatch("[0-9a-f]{64}  [^\\r\\n]+", value) for value in inputs), "missing or invalid source input inventory")
        names = [value[66:] for value in inputs]
        require(names == sorted(set(names)) and all(not name.startswith("/") and "\\" not in name
                and all(part not in ("", ".", "..") for part in name.split("/")) for name in names), "ambiguous source input inventory")
        require(receipt["source_inputs"] == inputs and receipt["build_inputs_sha256"] == expected["build_inputs_sha256"]
                and sha256(("\n".join(inputs) + "\n").encode()) == expected["build_inputs_sha256"], "build input identity mismatch")
        require(dict(zip(names, (value[:64] for value in inputs))).get("LICENSE") == receipt["python"]["license_sha256"],
                "source license identity mismatch")
        packages = expected["packages"]
        require(type(packages) is dict and packages and "libpq5" in packages
                and receipt["python"]["packages"] == packages, "package inventory mismatch")
        for name, item in packages.items():
            require(type(name) is str and name and type(item) is dict and set(item) == {"version", "architecture", "source", "source_version", "copyright_path", "copyright_sha256"}
                    and all(type(item[key]) is str and item[key] for key in item) and valid_hash(item["copyright_sha256"])
                    and item["architecture"] in (expected["platform"]["architecture"], "all")
                    and item["copyright_path"].startswith("/usr/share/doc/"), "invalid package identity")
        require(receipt["libpq"]["package_version"] == packages["libpq5"]["version"], "libpq package mismatch")
        package = {"path": receipt["libpq"]["resolved_path"], "package": "libpq5\t" + packages["libpq5"]["version"]}
        image_contract("smoke").validate_python(receipt["python"], expected["platform"]["architecture"], package, receipt["libpq"]["runtime_version"])
        require(type(receipt["checks"]) is dict and set(receipt["checks"]) == CHECKS
                and all(value is True for value in receipt["checks"].values()), "incomplete smoke checks")
        return receipt
    except (KeyError, TypeError, ValueError, AttributeError, IndexError) as error:
        raise RuntimeError("missing or malformed smoke identity") from error


def verify_candidate(path, expected, smoke_bytes, source_identity):
    """Expect reviewed lock/source admission and the original, unmodified smoke JSON bytes."""
    validate_lock(expected)
    require(type(source_identity) is dict and source_identity == {"commit": expected["source_commit"], "tree": expected["source_tree"]}, "source identity mismatch")
    require(type(smoke_bytes) is bytes and sha256(smoke_bytes) == expected["smoke_sha256"], "smoke receipt hash mismatch")
    facts = inspect_archive(path, expected["platform"], expected["source_commit"])
    for name in ARCHIVE_FIELDS:
        require(facts[name] == expected[name], "archive identity mismatch: " + name)
    verify_smoke(expected, smoke_bytes)
    return {**expected, **facts}


def verify_loaded(expected, consumer_smoke_bytes):
    """Compare a fresh producer-compatible probe to an already admitted immutable lock."""
    validate_lock(expected)
    verify_smoke(expected, consumer_smoke_bytes)
    return {"consumer_smoke_sha256": sha256(consumer_smoke_bytes), "image_id": expected["image_id"],
            "platform": dict(expected["platform"]), "runtime_binary_sha256": expected["runtime_binary_sha256"],
            "build_inputs_sha256": expected["build_inputs_sha256"]}


def local_archive_path(path):
    value = os.fspath(path)
    require(type(value) is str and value and not value.startswith("-") and ":" not in value and "\0" not in value, "expected local archive path")
    return value


def export_argv(image_id, path):
    require(valid_digest(image_id), "export requires immutable image config ID")
    return ["podman", "image", "save", "--format=oci-archive", "--output", local_archive_path(path), image_id]


def import_argv(path):
    return ["podman", "image", "load", "--input", local_archive_path(path)]


def read_json_bytes(path):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= MAX_JSON_BYTES, "JSON file size or type")
            data = source.read(MAX_JSON_BYTES + 1)
            require(len(data) == before.st_size and file_identity(os.fstat(source.fileno())) == file_identity(before), "JSON input changed")
    except OSError as error:
        raise RuntimeError("cannot read candidate JSON input") from error
    decode(data)
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--expected", required=True)
    parser.add_argument("--smoke", required=True)
    parser.add_argument("--source-identity", required=True)
    args = parser.parse_args()
    try:
        result = verify_candidate(args.archive, decode(read_json_bytes(args.expected)), read_json_bytes(args.smoke), decode(read_json_bytes(args.source_identity)))
    except RuntimeError as error:
        print("candidate verification failed: " + str(error), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
