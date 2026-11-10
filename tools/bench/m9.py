#!/usr/bin/env python3
"""Prepare, admit, run, and summarize one explicitly approved M9 COPY A/A phase."""

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import json
import math
import os
import pwd
from pathlib import Path
import random
import re
import itertools
import signal
import subprocess
import tempfile
import stat
import time
import sys
import uuid


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/bench"
PHASE = "aa-copy-18-18"
SCOPE = "COPY_AA_PG18_40_CHILDREN"
NATIVE_SOURCE = "9eb3154672979dc3ca9c6e560fdca99af6d716b8"
NATIVE_TREE = "f531f305c7cb66f0e0a18f4194a5e3059e2cb0cc"
FORK_SOURCE = "86bf908adca02d87063849dae775a02c1659c829"
FORK_TREE = "88479efa4cc5814403fbbbec12770f0cee27c361"
GIB = 1024 ** 3
RESOURCE_POLICY = {
    "cpus": 6, "memory_bytes": 12 * GIB, "swap_bytes": 0,
    "roles": {role: {"cpus": 2, "memory_bytes": 4 * GIB, "swap_bytes": 0}
              for role in ("source", "target", "runner")},
    "disk_start_bytes": 32 * GIB, "disk_floor_bytes": 20 * GIB,
    "phase_growth_bytes": 12 * GIB, "campaign_growth_bytes": 24 * GIB,
    "slot_wal_bytes": GIB, "pg_wal_bytes": 2 * GIB,
    "remaining_cpus": 2, "remaining_memory_bytes": 6 * GIB,
}
STOP_POLICY = {"children": 40, "child_seconds": 120, "phase_seconds": 7200,
               "mechanics_checkpoint_children": 4, "replacement_retries": 0}
CACHE_POLICY = "fresh-child-databases-no-global-cache-reset"
CORRECTNESS_CASES = ("fixture_inventory", "copy_wire_format", "one_part_plan", "independent_schema",
                     "independent_contents", "writing_barrier", "checked_closure")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def keys(value, expected, name):
    require(isinstance(value, dict) and set(value) == set(expected), name + " fields differ")


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate JSON field")
        value[key] = item
    return value


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def sha256(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def identifier(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{32}", value) is not None


def read_bytes(path, private=False):
    path = Path(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid(), "input is not an owned regular file")
        require(info.st_size <= 16 * 1024 * 1024, "JSON or configuration input exceeds bound")
        if private:
            require(stat.S_IMODE(info.st_mode) == 0o600, "private input permissions differ")
        return stream.read()


def read_json(path, private=False):
    try:
        return json.loads(read_bytes(path, private), object_pairs_hook=unique_object,
                          parse_constant=lambda _value: require(False, "nonfinite JSON number"))
    except (ValueError, UnicodeError):
        raise RuntimeError("malformed JSON input") from None


def publish(path, value):
    publish_bytes(path, encode(value))


def publish_bytes(path, data):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def private_root(path, create=False):
    path = Path(path)
    if create:
        path.mkdir(mode=0o700)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, "receipt root must be private, owned, and nonsymlink")
    return path


def envelope(value, kind, fields):
    keys(value, ("schema_version", "kind", *fields), kind)
    require(type(value["schema_version"]) is int and value["schema_version"] == 1
            and value["kind"] == "m9-benchmark-" + kind, "unsupported benchmark schema")


def make_schedule(run_id, seed):
    require(identifier(run_id) and type(seed) is int and 0 <= seed < 2 ** 63, "invalid schedule identity or seed")
    rng = random.Random(seed)
    pairs = []
    for cell, (engine, shape) in enumerate((engine, shape) for shape in ("narrow", "wide") for engine in ("native", "fork")):
        orders = ["AB", "BA", "AB", "BA", "AB" if cell % 2 == 0 else "BA"]
        rng.shuffle(orders)
        for pair, order in enumerate(orders):
            pairs.append((engine, shape, pair, order))
    early = [pairs.pop(0), pairs.pop(4)]
    rng.shuffle(pairs)
    result = []
    for engine, shape, pair, order in early + pairs:
        for sample in order:
            child_id = uuid.uuid5(uuid.UUID(hex=run_id), f"{engine}:{shape}:{pair}:{sample}").hex
            result.append({"child_id": child_id, "engine": engine, "shape": shape,
                           "pair_index": pair, "sample": sample,
                           "source_db": "m9_" + child_id + "_s", "target_db": "m9_" + child_id + "_t"})
    return result


def load_fixture(path=FIXTURES / "m9.json"):
    fixture = read_json(path)
    envelope(fixture, "fixture", ("schema", "seed", "shapes", "probe", "oracle"))
    for field, filename in (("schema", "schema.sql"), ("seed", "seed.sql")):
        keys(fixture[field], ("path", "sha256"), "fixture SQL")
        require(fixture[field]["path"] == filename and sha256(fixture[field]["sha256"])
                and digest(read_bytes(Path(path).parent / filename)) == fixture[field]["sha256"], "fixture SQL missing or altered")
    require(fixture["shapes"] == {
        "narrow": {"tables": 16, "rows_per_table": 65536, "text_bytes_per_row": 96},
        "wide": {"tables": 4, "rows_per_table": 4096, "text_bytes_per_row": 8192}}, "fixture shape differs")
    require(fixture["probe"] == {"table": "probe", "id": 1, "initial_value": 0}
            and fixture["oracle"] == {"max_rows_per_table": 100000, "max_bytes_per_table": 67108864}, "fixture oracle differs")
    return fixture


def prepare(output, artifacts, seed=20261110, run_id=None, fixture_path=FIXTURES / "m9.json"):
    fixture = load_fixture(fixture_path)
    validate_artifacts(artifacts)
    run_id = run_id or uuid.uuid4().hex
    schedule = make_schedule(run_id, seed)
    root = private_root(output, create=True)
    schedule_record = {"schema_version": 1, "kind": "m9-benchmark-schedule", "run_id": run_id,
                       "phase": PHASE, "children": schedule}
    publish(root / "schedule.json", schedule_record)
    manifest = {"schema_version": 1, "kind": "m9-benchmark-manifest", "run_id": run_id,
                "phase": PHASE, "pair": "18-18", "seed": seed, "fixture": fixture,
                "schedule": {"path": "schedule.json", "sha256": digest(encode(schedule_record))},
                "artifact_lock_sha256": digest(read_bytes(artifacts)), "resource_policy": RESOURCE_POLICY,
                "cache_policy": CACHE_POLICY, "stop_policy": STOP_POLICY}
    publish(root / "manifest.json", manifest)
    return manifest


def file_hash(path):
    path = Path(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid(), "artifact is not an owned regular file")
        result = hashlib.sha256()
        while data := stream.read(1024 * 1024):
            result.update(data)
        return result.hexdigest()


def locked_file(record, executable=False):
    keys(record, ("path", "sha256"), "locked file")
    path = Path(record["path"])
    require(path.is_absolute() and str(path.resolve()) == str(path) and sha256(record["sha256"])
            and file_hash(path) == record["sha256"], "artifact identity differs")
    if executable:
        require(os.access(path, os.X_OK), "locked executable unavailable")
    return path


def private_reference(record, scope="host"):
    keys(record, ("path", "host", "runner"), "private authentication reference")
    path = Path(record["path"])
    require(path.is_absolute() and str(path.resolve()) == str(path) and not path.is_symlink(), "authentication path differs")
    info = path.stat()
    for identity in (record["host"], record["runner"]):
        keys(identity, ("uid", "mode", "device", "inode", "size"), "authentication identity")
        require(all(type(value) is int and value >= 0 for value in identity.values()), "authentication identity absent")
    require(stat.S_ISREG(info.st_mode) and record[scope] == {"uid": info.st_uid,
        "mode": stat.S_IMODE(info.st_mode), "device": info.st_dev, "inode": info.st_ino, "size": info.st_size}
        and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o600 and 0 < info.st_size <= 16384,
        "private authentication identity differs")
    return path


def validate_manifest(path, artifact_path):
    manifest = read_json(path, private=True)
    envelope(manifest, "manifest", ("run_id", "phase", "pair", "seed", "fixture", "schedule", "artifact_lock_sha256",
                                    "resource_policy", "cache_policy", "stop_policy"))
    require(manifest["phase"] == PHASE and manifest["pair"] == "18-18", "unsupported benchmark version or phase")
    require(manifest["fixture"] == load_fixture(), "fixture identity differs")
    require(manifest["resource_policy"] == RESOURCE_POLICY and manifest["stop_policy"] == STOP_POLICY
            and manifest["cache_policy"] == CACHE_POLICY, "resource, cache, or stop policy differs")
    keys(manifest["schedule"], ("path", "sha256"), "schedule binding")
    require(manifest["schedule"]["path"] == "schedule.json", "schedule path differs")
    schedule_path = Path(path).parent / "schedule.json"
    schedule = read_json(schedule_path, private=True)
    envelope(schedule, "schedule", ("run_id", "phase", "children"))
    require(schedule["run_id"] == manifest["run_id"] and schedule["phase"] == PHASE
            and schedule["children"] == make_schedule(manifest["run_id"], manifest["seed"])
            and digest(read_bytes(schedule_path)) == manifest["schedule"]["sha256"], "schedule missing, altered, or incomplete")
    require(digest(read_bytes(artifact_path)) == manifest["artifact_lock_sha256"], "artifact lock identity differs")
    return manifest, schedule["children"]


def validate_artifacts(path, scope="host"):
    lock = read_json(path, private=True)
    def no_placeholders(value):
        if isinstance(value, str):
            require(re.search(r"[<>]|(?:^|[/ _-])(?:TODO|TBD|PLACEHOLDER|REPLACE_ME|UNSET|UNQUALIFIED|PENDING)(?:$|[/ _-])", value, re.IGNORECASE) is None,
                    "artifact lock contains a placeholder")
        elif isinstance(value, dict):
            for item in value.values():
                no_placeholders(item)
        elif isinstance(value, list):
            for item in value:
                no_placeholders(item)
    no_placeholders(lock)
    envelope(lock, "artifact-lock", ("native", "fork", "server_image", "runner_image", "tools", "driver", "profile", "qualification", "runtime"))
    keys(lock["runtime"], ("path", "sha256", "version"), "container runtime")
    if scope == "host":
        locked_file({key: lock["runtime"][key] for key in ("path", "sha256")}, executable=True)
    require(isinstance(lock["runtime"]["version"], str) and lock["runtime"]["version"], "container runtime version absent")
    for engine in ("native", "fork"):
        item = lock[engine]
        keys(item, ("binary", "source", "build", "libpq", "qualification"), "engine artifact")
        locked_file(item["binary"], executable=True)
        keys(item["source"], ("commit", "tree", "export", "patch"), "source provenance")
        require(item["source"]["commit"] == (NATIVE_SOURCE if engine == "native" else FORK_SOURCE), "source baseline differs")
        require(re.fullmatch(r"[0-9a-f]{40}", item["source"]["tree"]) is not None, "source tree absent")
        require(item["source"]["tree"] == (NATIVE_TREE if engine == "native" else FORK_TREE), "exact source tree differs")
        locked_file(item["source"]["export"])
        patch = locked_file(item["source"]["patch"])
        require(patch.stat().st_size == 0, "first COPY reference requires unmodified source")
        keys(item["build"], ("compiler", "target", "flags", "cargo_lock"), "build provenance")
        require(all(isinstance(item["build"][key], str) and item["build"][key] for key in ("compiler", "target"))
                and isinstance(item["build"]["flags"], list) and all(isinstance(flag, str) for flag in item["build"]["flags"]), "build identity absent")
        if engine == "native":
            locked_file(item["build"]["cargo_lock"])
        else:
            require(item["build"]["cargo_lock"] is None, "fork does not use a Cargo lock")
        keys(item["libpq"], ("path", "sha256", "pq_version"), "linked libpq")
        locked_file({key: item["libpq"][key] for key in ("path", "sha256")})
        require(type(item["libpq"]["pq_version"]) is int and item["libpq"]["pq_version"] // 10000 == 18, "linked libpq major differs")
        locked_file(item["qualification"])
    for role in ("server_image", "runner_image"):
        image = lock[role]
        keys(image, ("reference", "index_digest", "manifest_digest", "config_digest", "platform"), "image identity")
        require((image["index_digest"] is None or isinstance(image["index_digest"], str)
                 and re.fullmatch(r"sha256:[0-9a-f]{64}", image["index_digest"]))
                and all(isinstance(image[key], str) and re.fullmatch(r"sha256:[0-9a-f]{64}", image[key]) for key in ("manifest_digest", "config_digest"))
                and isinstance(image["reference"], str) and image["reference"].endswith("@" + image["manifest_digest"])
                and image["platform"] in ("linux/amd64", "linux/arm64"), "immutable image identity absent")
    keys(lock["tools"], ("psql", "pg_dump", "pg_restore", "pg_config"), "client tools")
    for tool in lock["tools"].values():
        keys(tool, ("path", "sha256", "version"), "client tool")
        locked_file({key: tool[key] for key in ("path", "sha256")}, executable=True)
        require(isinstance(tool["version"], str) and re.fullmatch(r"18\.[0-9]+", tool["version"]), "client version differs")
    keys(lock["driver"], ("files",), "driver inventory")
    records = lock["driver"]["files"]
    expected = {str(ROOT / name) for name in ("tools/bench/m9.py", "tools/e2e/clone.py", "tools/e2e/follow_wire.py", "tests/support/protocol_fault.py")}
    require(isinstance(records, list) and len(records) == len(expected)
            and {str(locked_file(record)) for record in records} == expected, "driver inventory differs")
    keys(lock["profile"], ("copy_format", "table_jobs", "split_threshold", "native_barrier_interval_ms", "auth", "cache_policy"), "profile")
    profile = lock["profile"]
    require({key: profile[key] for key in profile if key != "auth"} == {
        "copy_format": 0, "table_jobs": 4, "split_threshold": 0,
        "native_barrier_interval_ms": None, "cache_policy": CACHE_POLICY}, "COPY profile differs")
    keys(profile["auth"], ("source", "target"), "authentication references")
    for reference in profile["auth"].values():
        keys(reference, ("passfile", "password_file"), "authentication references")
        private_reference(reference["passfile"], scope)
        password = private_reference(reference["password_file"], scope)
        require(re.fullmatch(rb"[A-Za-z0-9]{32,128}\n?", read_bytes(password)) is not None,
                "benchmark authentication must use a strong generated test secret")
    keys(lock["qualification"], ("copy_wire_observer_sha256", "required_cases"), "qualification")
    wire = next(record["sha256"] for record in records if record["path"] == str(ROOT / "tools/e2e/follow_wire.py"))
    require(lock["qualification"] == {"copy_wire_observer_sha256": wire, "required_cases": list(CORRECTNESS_CASES)}, "correctness inventory differs")
    for engine in ("native", "fork"):
        proof = read_json(locked_file(lock[engine]["qualification"]), private=True)
        envelope(proof, "correctness-receipt", ("engine", "source_commit", "binary_sha256", "fixture_sha256",
                                                "profile_sha256", "libpq_sha256", "wire_sha256", "executed_at", "cases"))
        require(proof["engine"] == engine and proof["source_commit"] == lock[engine]["source"]["commit"]
                and proof["binary_sha256"] == lock[engine]["binary"]["sha256"]
                and proof["fixture_sha256"] == digest(encode(load_fixture()))
                and proof["profile_sha256"] == digest(encode(profile))
                and proof["libpq_sha256"] == lock[engine]["libpq"]["sha256"]
                and proof["wire_sha256"] == wire and type(proof["executed_at"]) in (int, float)
                and math.isfinite(proof["executed_at"]) and proof["executed_at"] > 0, "correctness receipt context differs")
        require(isinstance(proof["cases"], list) and len(proof["cases"]) == len(CORRECTNESS_CASES), "executed correctness cases absent")
        names = []
        for case in proof["cases"]:
            keys(case, ("case", "observed", "outcome", "evidence"), "correctness case")
            require(case["observed"] is True and case["outcome"] == "passed"
                    and locked_file(case["evidence"]).stat().st_size > 0, "correctness case was not positively observed")
            names.append(case["case"])
        require(sorted(names) == sorted(CORRECTNESS_CASES), "executed correctness inventory differs")
    return lock


def validate_lease(lease, run_id, now):
    envelope(lease, "lease", ("lease_id", "run_id", "phase", "started", "deadline", "exclusive", "quiet_window", "host", "original_services", "campaign_disk_free_bytes"))
    require(identifier(lease["lease_id"]) and lease["run_id"] == run_id and lease["phase"] == PHASE, "original lease identity differs")
    require(all(type(lease[key]) in (int, float) and math.isfinite(lease[key]) for key in ("started", "deadline"))
            and lease["started"] <= now < lease["deadline"] <= lease["started"] + 7200, "original lease expired or deadline differs")
    require(lease["exclusive"] is True and lease["quiet_window"] == {
        "m6_m7_idle": True, "original_commitments_preserved": True, "host_cargo_idle": True,
        "other_heavy_work_idle": True}, "exclusive quiet window absent")
    keys(lease["host"], ("identity", "cpus", "memory_bytes"), "host admission")
    require(isinstance(lease["host"]["identity"], str) and lease["host"]["identity"]
            and type(lease["host"]["cpus"]) is int and lease["host"]["cpus"] >= 8
            and type(lease["host"]["memory_bytes"]) is int and lease["host"]["memory_bytes"] >= 18 * GIB,
            "host admission lacks headroom")
    require(type(lease["campaign_disk_free_bytes"]) is int and lease["campaign_disk_free_bytes"] >= 32 * GIB, "campaign disk baseline absent")
    require(isinstance(lease["original_services"], list) and bool(lease["original_services"]), "original service inventory absent")
    ids = set()
    for service in lease["original_services"]:
        keys(service, ("id", "running", "cpus", "memory_bytes"), "original service")
        require(sha256(service["id"]) and service["id"] not in ids and type(service["running"]) is bool
                and type(service["cpus"]) in (int, float) and service["cpus"] >= 0
                and type(service["memory_bytes"]) is int and service["memory_bytes"] >= 0, "original service identity differs")
        ids.add(service["id"])
    require(lease["host"]["cpus"] - sum(service["cpus"] for service in lease["original_services"]) - 6 >= 2
            and lease["host"]["memory_bytes"] - sum(service["memory_bytes"] for service in lease["original_services"]) - 12 * GIB >= 6 * GIB,
            "original commitments leave insufficient headroom")


def host_coordination_paths():
    home = Path(pwd.getpwuid(os.getuid()).pw_dir).resolve()
    cache = home / "Library/Caches" if sys.platform == "darwin" else home / ".cache"
    parent = cache / "rpgmigrate-m9-benchmark"
    return {"parent": str(parent), "path": str(parent / "phase.lock"), "uid": os.getuid()}


@contextmanager
def exclusive_host_lock(record):
    parent, path = Path(record["parent"]), Path(record["path"])
    base = parent.parent.lstat()
    require(stat.S_ISDIR(base.st_mode) and base.st_uid == os.getuid() and not stat.S_IMODE(base.st_mode) & 0o022,
            "host coordination cache parent is unsafe")
    if not parent.exists():
        parent.mkdir(mode=0o700)
    private_root(parent)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    acquired = False
    try:
        info = os.fstat(descriptor)
        observed = path.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() == record["uid"]
                and stat.S_IMODE(info.st_mode) == 0o600 and info.st_nlink == 1
                and (info.st_dev, info.st_ino) == (observed.st_dev, observed.st_ino), "host coordination lock identity is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            raise RuntimeError("exclusive benchmark host lock is busy") from None
        observed = path.lstat()
        require((info.st_dev, info.st_ino) == (observed.st_dev, observed.st_ino), "host coordination lock identity changed")
        yield {"path": str(path), "uid": info.st_uid, "mode": stat.S_IMODE(info.st_mode), "device": info.st_dev,
               "inode": info.st_ino, "links": info.st_nlink}
    finally:
        changed = acquired and (not path.exists() or (path.lstat().st_dev, path.lstat().st_ino) != (info.st_dev, info.st_ino))
        if acquired:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
        require(not changed, "host coordination lock identity changed before release")


def retained_paths(receipt_root, run_id, host_lock=None):
    vm = "/var/home/core/rpgmigrate-m9-" + run_id
    host_lock = host_lock or host_coordination_paths()
    return [str(Path(receipt_root).resolve()), vm, vm + "/source", vm + "/target", host_lock["parent"], host_lock["path"]]


def admit(approval_path, manifest_path, artifacts_path, lease_path, receipt_root, phase=PHASE, now=None, child_id=None):
    require(approval_path is not None, "explicit user approval is required; benchmark HOLD")
    approval = read_json(approval_path, private=True)
    envelope(approval, "approval", ("decision", "packet_sha256", "authorized_at", "scope"))
    require(approval["decision"] == "approved" and approval["scope"] == SCOPE
            and sha256(approval["packet_sha256"]), "explicit phase approval absent")
    packet_path = Path(manifest_path).parent / "packet.json"
    packet = read_json(packet_path, private=True)
    envelope(packet, "packet", ("manifest_sha256", "schedule_sha256", "artifact_lock_sha256", "execution", "ownership", "reset", "cleanup"))
    require(digest(read_bytes(packet_path)) == approval["packet_sha256"], "approved packet identity differs")
    manifest, schedule = validate_manifest(manifest_path, artifacts_path)
    require(phase == PHASE and packet["manifest_sha256"] == digest(read_bytes(manifest_path))
            and packet["schedule_sha256"] == manifest["schedule"]["sha256"]
            and packet["artifact_lock_sha256"] == manifest["artifact_lock_sha256"], "packet bindings differ")
    lock = validate_artifacts(artifacts_path, "runner" if child_id is not None else "host")
    lease = read_json(lease_path, private=True)
    now = time.time() if now is None else now
    validate_lease(lease, manifest["run_id"], now)
    require(type(approval["authorized_at"]) in (int, float) and math.isfinite(approval["authorized_at"])
            and approval["authorized_at"] <= lease["started"], "approval timestamp differs")
    execution = packet["execution"]
    keys(execution, ("phase", "argv", "original_lease_sha256", "deadline", "receipt_root"), "execution")
    argv = [sys.executable, str(Path(__file__).resolve()), "run", "--approval", str(Path(approval_path).resolve()),
            "--manifest", str(Path(manifest_path).resolve()), "--artifacts", str(Path(artifacts_path).resolve()),
            "--phase", PHASE, "--lease", str(Path(lease_path).resolve()), "--receipt-root", str(Path(receipt_root).resolve())]
    require(execution == {"phase": PHASE, "argv": argv, "original_lease_sha256": digest(read_bytes(lease_path)),
                          "deadline": lease["deadline"], "receipt_root": str(Path(receipt_root).resolve())}, "exact execution or original lease differs")
    ownership = packet["ownership"]
    keys(ownership, ("owner", "original_services", "network_name", "labels", "retain_paths", "host_lock"), "ownership")
    keys(ownership["host_lock"], ("parent", "path", "uid"), "host coordination contract")
    require(Path(ownership["host_lock"]["parent"]).is_absolute()
            and ownership["host_lock"]["path"] == str(Path(ownership["host_lock"]["parent"]) / "phase.lock")
            and type(ownership["host_lock"]["uid"]) is int and ownership["host_lock"]["uid"] == os.getuid(), "host coordination contract differs")
    if child_id is None:
        require(ownership["host_lock"] == host_coordination_paths(), "host coordination path differs")
    require(ownership["owner"] == manifest["run_id"] and ownership["original_services"] == lease["original_services"]
            and ownership["network_name"] == "m9-" + manifest["run_id"]
            and ownership["labels"] == {"io.rpgmigrate.m9": manifest["run_id"]}
            and ownership["retain_paths"] == retained_paths(receipt_root, manifest["run_id"], ownership["host_lock"]), "phase ownership differs")
    require(packet["reset"] == {"fresh_child_databases": True, "slots": [], "publications": []}
            and packet["cleanup"] == {"containers": True, "network": True, "delete_retained": False}, "reset or cleanup approval differs")
    if child_id is None:
        require(not Path(receipt_root).exists(), "receipt root already exists")
    else:
        require(sys.platform == "linux" and any(child["child_id"] == child_id for child in schedule), "internal child identity or platform differs")
        private_root(receipt_root)
    return {"manifest": manifest, "schedule": schedule, "artifacts": lock, "packet": packet,
            "approval": approval, "lease": lease,
            "paths": {"approval": str(Path(approval_path).resolve()), "manifest": str(Path(manifest_path).resolve()),
                      "artifacts": str(Path(artifacts_path).resolve()), "lease": str(Path(lease_path).resolve())},
            "hashes": {"packet_sha256": digest(read_bytes(packet_path)), "approval_sha256": digest(read_bytes(approval_path)),
                       "lease_sha256": digest(read_bytes(lease_path)), "artifact_sha256": digest(read_bytes(artifacts_path)),
                       "fixture_sha256": digest(encode(manifest["fixture"]))}}


def run_phase(approval_path, manifest_path, artifacts_path, lease_path, receipt_root, phase=PHASE):
    admission = admit(approval_path, manifest_path, artifacts_path, lease_path, receipt_root, phase)
    result, root = None, None
    try:
        with exclusive_host_lock(admission["packet"]["ownership"]["host_lock"]) as identity:
            admission["host_lock_identity"] = identity
            root = private_root(receipt_root, create=True)
            result = execute_phase(admission, root)
    except Exception:
        if result is not None:
            result["status"] = "failed"
            publish(root / "phase.json", result)
        raise
    publish(root / "phase.json", result)
    return result


def validate_resources(value, initial=False):
    fields = ("disk_free_bytes", "phase_growth_bytes", "campaign_growth_bytes", "source_pg_wal_bytes",
              "target_pg_wal_bytes", "slot_wal_bytes", "swap_bytes", "oom_events", "memory_pressure_events", "roles")
    keys(value, fields, "resource observations")
    require(all(type(value[key]) is int and value[key] >= 0 for key in fields if key != "roles"), "resource observation missing")
    require(value["disk_free_bytes"] >= (32 if initial else 20) * GIB
            and value["phase_growth_bytes"] <= 12 * GIB and value["campaign_growth_bytes"] <= 24 * GIB
            and value["source_pg_wal_bytes"] <= 2 * GIB and value["target_pg_wal_bytes"] <= 2 * GIB
            and value["slot_wal_bytes"] <= GIB
            and value["swap_bytes"] == value["oom_events"] == value["memory_pressure_events"] == 0,
            "resource limit violated")
    keys(value["roles"], ("source", "target", "runner"), "resource roles")
    for role in value["roles"].values():
        keys(role, ("requested_cpus", "requested_memory_bytes", "requested_swap_bytes", "effective_cpu_max",
                    "effective_memory_max", "effective_swap_max"), "resource enforcement")
        require(type(role["requested_cpus"]) is int and role["requested_cpus"] == 2
                and type(role["requested_memory_bytes"]) is int and role["requested_memory_bytes"] == 4 * GIB
                and type(role["requested_swap_bytes"]) is int and role["requested_swap_bytes"] == 0
                and role["effective_cpu_max"] == [200000, 100000]
                and all(type(number) is int for number in role["effective_cpu_max"])
                and type(role["effective_memory_max"]) is int and role["effective_memory_max"] == 4 * GIB
                and type(role["effective_swap_max"]) is int and role["effective_swap_max"] == 0,
                "resource enforcement differs from requested limits")


def validate_barrier(value):
    require(value == {"synchronous_commit": "local", "logged_probe": True, "update_tag": "UPDATE 1",
                      "commit_tag": "COMMIT", "diagnostics_empty": True, "closed": True}, "final writing barrier proof differs")


def expected_rows(shape):
    definition = load_fixture()["shapes"][shape]
    return {**{"data_" + str(index): definition["rows_per_table"] for index in range(1, definition["tables"] + 1)}, "probe": 1}


def validate_wire(value, shape):
    keys(value, ("source", "target"), "COPY wire evidence")
    expected = expected_rows(shape)
    for side, direction in (("source", "out"), ("target", "in")):
        evidence = value[side]
        keys(evidence, ("invalid", "overflow", "records"), "COPY wire side")
        require(evidence["invalid"] is False and evidence["overflow"] is False
                and isinstance(evidence["records"], list) and len(evidence["records"]) == len(expected), "COPY wire coverage differs")
        seen = set()
        for record in evidence["records"]:
            require(isinstance(record, dict) and set(record).issubset({"relation", "direction", "range", "format", "connection", "backend_pid", "columns", "rows", "data_frames", "eof", "complete", "held", "released", "wire_bytes"})
                    and isinstance(record.get("relation"), list) and len(record["relation"]) == 2
                    and record["relation"][0] == "m9", "COPY relation differs")
            table = record["relation"][1]
            require(table in expected and table not in seen and record.get("direction") == direction
                    and record.get("format") == "text" and record.get("range") == [None, None]
                    and record.get("columns") == (2 if table == "probe" else 3)
                    and type(record.get("rows")) is int and record["rows"] == expected[table] and record.get("complete") is True
                    and record.get("eof") is True and type(record.get("backend_pid")) is int
                    and record["backend_pid"] > 0, "actual text COPY format or one-part coverage differs")
            seen.add(table)


def validate_child(receipt, child, context):
    envelope(receipt, "receipt", ("run_id", "child_id", "packet_sha256", "approval_sha256", "lease_sha256",
                                  "artifact_sha256", "fixture_sha256", "started", "finished", "measurements", "oracles", "resources", "cleanup", "status"))
    require(receipt["run_id"] == context["manifest"]["run_id"] and receipt["child_id"] == child["child_id"]
            and all(receipt[key] == value for key, value in context["hashes"].items()), "child receipt identity differs")
    require(receipt["status"] == "success" and type(receipt["started"]) in (int, float)
            and type(receipt["finished"]) in (int, float) and context["lease"]["started"] <= receipt["started"]
            < receipt["finished"] <= context["lease"]["deadline"]
            and receipt["finished"] - receipt["started"] <= 120, "child receipt failed or deadline exceeded")
    measurement = receipt["measurements"]
    keys(measurement, ("engine", "shape", "pair_index", "sample", "pid", "start_identity", "clone_elapsed_seconds", "child_elapsed_seconds",
                       "canonical_text_bytes", "wire_bytes", "peak_tree_rss_bytes", "wait4_rss_bytes", "cpu_user_seconds", "cpu_system_seconds", "timer_resolution_seconds"), "child measurements")
    require(all(type(measurement[key]) is type(child[key]) and measurement[key] == child[key] for key in ("engine", "shape", "pair_index", "sample"))
            and type(measurement["pid"]) is int and measurement["pid"] > 0
            and isinstance(measurement["start_identity"], str) and measurement["start_identity"], "fresh child process identity absent")
    require(all(type(measurement[key]) in (int, float) and math.isfinite(measurement[key]) and measurement[key] > 0
                for key in ("clone_elapsed_seconds", "child_elapsed_seconds", "canonical_text_bytes", "peak_tree_rss_bytes", "wait4_rss_bytes", "timer_resolution_seconds"))
            and measurement["clone_elapsed_seconds"] <= measurement["child_elapsed_seconds"] <= 120
            and all(type(measurement[key]) in (int, float) and math.isfinite(measurement[key]) and measurement[key] >= 0
                    for key in ("cpu_user_seconds", "cpu_system_seconds"))
            and (measurement["wire_bytes"] is None or type(measurement["wire_bytes"]) is int and measurement["wire_bytes"] > 0), "measured work or process resources absent")
    oracles = receipt["oracles"]
    keys(oracles, ("source", "target", "fixture_observed", "wire", "barrier", "completion", "runtime"), "child oracles")
    require(isinstance(oracles["source"], dict) and oracles["source"] == oracles["target"]
            and set(oracles["source"]) == {*expected_rows(child["shape"]), "schema"}
            and sha256(oracles["source"]["schema"]), "independent schema or contents differ")
    for table, count in expected_rows(child["shape"]).items():
        keys(oracles["source"][table], ("rows", "sha256"), "content fingerprint")
        require(type(oracles["source"][table]["rows"]) is int and oracles["source"][table]["rows"] == count and sha256(oracles["source"][table]["sha256"]), "actual fixture row coverage differs")
    observed = oracles["fixture_observed"]
    require(isinstance(observed, list) and len(observed) == len(expected_rows(child["shape"])), "measured fixture inventory absent")
    seen = set()
    for table in observed:
        keys(table, ("name", "oid", "rows", "canonical_text_bytes", "heap_bytes", "index_bytes"), "actual fixture table")
        require(table["name"] not in seen and table["name"] in expected_rows(child["shape"])
                and type(table["oid"]) is int and table["oid"] > 0
                and type(table["rows"]) is int and table["rows"] == expected_rows(child["shape"])[table["name"]]
                and all(type(table[key]) is int and table[key] > 0 for key in ("canonical_text_bytes", "heap_bytes"))
                and type(table["index_bytes"]) is int and table["index_bytes"] >= 0, "measured fixture dimensions differ")
        seen.add(table["name"])
        minimum = 4 if table["name"] == "probe" else table["rows"] * (context["manifest"]["fixture"]["shapes"][child["shape"]]["text_bytes_per_row"] + 5)
        require(table["canonical_text_bytes"] >= minimum, "fixture payload bytes differ from shape")
    require(sum(table["canonical_text_bytes"] for table in observed) == measurement["canonical_text_bytes"], "canonical COPY byte count differs")
    validate_wire(oracles["wire"], child["shape"])
    validate_barrier(oracles["barrier"])
    runtime = oracles["runtime"]
    keys(runtime, ("servers", "client"), "child runtime")
    keys(runtime["servers"], ("source", "target"), "actual PostgreSQL servers")
    for server in runtime["servers"].values():
        keys(server, ("server_version_num", "fsync", "full_page_writes", "wal_level", "max_slot_wal_keep_size", "max_wal_size", "server_encoding", "system_identifier"), "actual PostgreSQL settings")
        require(type(server["server_version_num"]) is int and server["server_version_num"] // 10000 == 18
                and {key: server[key] for key in server if key not in ("server_version_num", "system_identifier")} == {
                    "fsync": "on", "full_page_writes": "on", "wal_level": "logical", "max_slot_wal_keep_size": "1GB", "max_wal_size": "1GB", "server_encoding": "UTF8"}
                and isinstance(server["system_identifier"], str) and server["system_identifier"].isdecimal()
                and int(server["system_identifier"]) > 0, "actual PostgreSQL settings differ")
    keys(runtime["client"], ("tools", "libpq_sha256", "pq_version"), "actual client runtime")
    lock = context["artifacts"]
    require(runtime["client"]["tools"] == {name: tool["version"] for name, tool in lock["tools"].items()}
            and runtime["client"]["libpq_sha256"] == lock[child["engine"]]["libpq"]["sha256"]
            and runtime["client"]["pq_version"] == lock[child["engine"]]["libpq"]["pq_version"], "actual client runtime differs")
    require(oracles["completion"] == {"workers_stopped": True, "postdata": True, "sequences": True,
        "analyze": True, "synchronization": True, "closed": True}, "plain clone completion is incomplete")
    require(isinstance(receipt["resources"], list) and bool(receipt["resources"]), "resource samples absent")
    for sample in receipt["resources"]:
        validate_resources(sample)
    keys(receipt["cleanup"], ("workers_stopped", "databases"), "child cleanup")
    require(receipt["cleanup"]["workers_stopped"] is True and len(receipt["cleanup"]["databases"]) == 2, "cleanup incomplete")
    for side, database in zip(("source", "target"), receipt["cleanup"]["databases"]):
        keys(database, ("side", "name", "oid", "identity_checked", "status"), "database cleanup")
        require(database["side"] == side and database["name"] == child[side + "_db"]
                and type(database["oid"]) is int and database["oid"] > 0 and database["identity_checked"] is True
                and database["status"] in ("removed", "retained"), "database cleanup identity or result differs")


def summarize(manifest_path, artifact_path, receipt_root):
    manifest, schedule = validate_manifest(manifest_path, artifact_path)
    root = private_root(receipt_root)
    phase = read_json(root / "phase.json", private=True)
    envelope(phase, "phase-receipt", ("manifest_sha256", "packet_sha256", "approval_sha256", "lease_sha256", "artifact_sha256", "fixture_sha256",
                                       "lease", "children", "cleanup", "runtime", "status"))
    require(phase["manifest_sha256"] == digest(read_bytes(manifest_path)) and phase["status"] == "success", "phase failed or incomplete")
    hashes = {key: phase[key] for key in ("packet_sha256", "approval_sha256", "lease_sha256", "artifact_sha256", "fixture_sha256")}
    require(all(sha256(value) for value in hashes.values()) and hashes["artifact_sha256"] == digest(read_bytes(artifact_path))
            and hashes["fixture_sha256"] == digest(encode(manifest["fixture"])), "phase hash bindings differ")
    validate_lease(phase["lease"], manifest["run_id"], phase["lease"]["started"])
    require(digest(read_bytes(root / "lease.json", private=True)) == phase["lease_sha256"]
            and read_json(root / "lease.json", private=True) == phase["lease"], "retained original lease hash differs")
    approval = read_json(root / "approval.json", private=True)
    packet = read_json(root / "packet.json", private=True)
    require(digest(read_bytes(root / "approval.json")) == phase["approval_sha256"]
            and digest(read_bytes(root / "packet.json")) == phase["packet_sha256"]
            and approval.get("decision") == "approved" and approval.get("scope") == SCOPE
            and approval.get("packet_sha256") == phase["packet_sha256"]
            and packet.get("manifest_sha256") == phase["manifest_sha256"]
            and packet.get("artifact_lock_sha256") == phase["artifact_sha256"]
            and packet.get("schedule_sha256") == manifest["schedule"]["sha256"], "retained approval packet binding differs")
    facts = phase["runtime"]
    keys(facts, ("host", "kernel", "filesystem", "storage", "images", "runtime_version", "initial_vm_free_bytes", "admission_resources", "host_lock_identity"), "actual phase runtime")
    require(facts["host"] == phase["lease"]["host"] and all(isinstance(facts[key], str) and facts[key] for key in ("kernel", "filesystem", "runtime_version"))
            and isinstance(facts["storage"], str) and facts["storage"]
            and set(facts["images"]) == {"server", "runner"}, "actual phase runtime facts absent")
    lock = read_json(artifact_path, private=True)
    require(facts["runtime_version"] == lock["runtime"]["version"], "actual runtime version differs")
    for role in ("server", "runner"):
        require(facts["images"][role] == {key: lock[role + "_image"][key] for key in ("manifest_digest", "config_digest", "platform")}, "actual phase image facts differ")
    require(type(facts["initial_vm_free_bytes"]) is int and facts["initial_vm_free_bytes"] >= 32 * GIB, "initial VM disk admission absent")
    lock_identity = facts["host_lock_identity"]
    keys(lock_identity, ("path", "uid", "mode", "device", "inode", "links"), "retained host coordination identity")
    require(lock_identity["path"] == packet["ownership"]["host_lock"]["path"] and lock_identity["uid"] == os.getuid()
            and lock_identity["mode"] == 0o600 and lock_identity["links"] == 1
            and type(lock_identity["device"]) is int and type(lock_identity["inode"]) is int and lock_identity["inode"] > 0,
            "retained host coordination identity differs")
    validate_resources(facts["admission_resources"])
    cleanup = phase["cleanup"]
    keys(cleanup, ("containers", "network", "original_services_unchanged", "retained_paths"), "phase cleanup")
    require(isinstance(cleanup["containers"], list) and len(cleanup["containers"]) == 3
            and {record.get("role") for record in cleanup["containers"]} == {"source", "target", "runner"}
            and all(sha256(record.get("id")) and record.get("ownership_checked") is True and record.get("absent") is True for record in cleanup["containers"])
            and cleanup["network"].get("ownership_checked") is True and cleanup["network"].get("absent") is True
            and cleanup["original_services_unchanged"] is True
            and cleanup["retained_paths"] == retained_paths(root, manifest["run_id"]), "phase cleanup incomplete")
    require(isinstance(phase["children"], list) and len(phase["children"]) == 40, "complete forty-child schedule required")
    context = {"manifest": manifest, "hashes": hashes, "lease": phase["lease"], "artifacts": lock}
    identities, children, reports = set(), [], []
    for child, binding in zip(schedule, phase["children"]):
        keys(binding, ("child_id", "sha256"), "child hash binding")
        require(binding["child_id"] == child["child_id"], "scheduled child identity differs")
        path = root / (child["child_id"] + ".json")
        require(digest(read_bytes(path, private=True)) == binding["sha256"], "child receipt tampered")
        receipt = read_json(path, private=True)
        validate_child(receipt, child, context)
        identity = (receipt["measurements"]["pid"], receipt["measurements"]["start_identity"])
        require(identity not in identities, "child process identity was reused")
        identities.add(identity)
        children.append(binding)
        reports.append(receipt)
    cells = {}
    for engine, shape in itertools.product(("native", "fork"), ("narrow", "wide")):
        pairs, timer_floor = [], 0.0
        for pair in range(5):
            samples = {receipt["measurements"]["sample"]: receipt["measurements"] for receipt in reports
                       if receipt["measurements"]["engine"] == engine and receipt["measurements"]["shape"] == shape
                       and receipt["measurements"]["pair_index"] == pair}
            require(set(samples) == {"A", "B"} and samples["A"]["canonical_text_bytes"] == samples["B"]["canonical_text_bytes"], "paired A/A samples incomplete or unequal")
            pairs.append(abs(math.log(samples["A"]["clone_elapsed_seconds"] / samples["B"]["clone_elapsed_seconds"])))
            timer_floor = max(timer_floor, sum(value["timer_resolution_seconds"] / value["clone_elapsed_seconds"] for value in samples.values()))
        empirical = sorted(pairs)[math.ceil(.95 * len(pairs)) - 1]
        cells[engine, shape] = {"engine": engine, "shape": shape, "pairs": 5,
            "empirical_95_absolute_paired_log_ratio": empirical, "timer_resolution_log_ratio_floor": timer_floor,
            "absolute_paired_log_ratio_tolerance": max(empirical, timer_floor),
            "measurement_scope": {"engine_cpu": "wait4", "engine_rss": "wait4_and_sampled_process_tree",
                "process_tree_sample_interval_seconds": .01, "source_cpu": None, "target_cpu": None,
                "source_process_rss": None, "target_process_rss": None, "psi_stall_deltas": None}}
    children = [{**binding, "copy_mib_per_second": receipt["measurements"]["canonical_text_bytes"] / 1048576 / receipt["measurements"]["clone_elapsed_seconds"],
                 "aa_cell": cells[receipt["measurements"]["engine"], receipt["measurements"]["shape"]]}
                for binding, receipt in zip(children, reports)]
    summary = {"schema_version": 1, "kind": "m9-benchmark-summary", "manifest_sha256": phase["manifest_sha256"],
               "packet_sha256": phase["packet_sha256"], "approval_sha256": phase["approval_sha256"],
               "scheduled": 40, "completed": len(children), "children": children, "status": "success"}
    publish(root / "summary.json", summary)
    return summary


def harness_module(name):
    path = ROOT / "tools/e2e" / (name + ".py")
    spec = importlib.util.spec_from_file_location("m9_" + name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def copy_observer_types(shape):
    wire = harness_module("follow_wire")
    queries = {}
    for table in expected_rows(shape):
        columns = ("id", "value") if table == "probe" else ("id", "value", "payload")
        native_columns = ", ".join('"' + column + '"' for column in columns)
        native_relation = '"m9"."' + table + '"'
        for direction, query in (("out", f"COPY (SELECT {native_columns} FROM ONLY {native_relation}) TO STDOUT WITH (FORMAT text)"),
                                 ("in", f"COPY {native_relation} ({native_columns}) FROM STDIN WITH (FORMAT text)"),
                                 ("in", f"COPY {native_relation} ({native_columns}) FROM STDIN WITH (FORMAT text, FREEZE true)")):
            queries[(query + "\0").encode()] = {"relation": ["m9", table], "direction": direction,
                                                "range": [None, None], "format": "text"}
        for schema_name, table_name in itertools.product(("m9", '"m9"'), (table, '"' + table + '"')):
            for attr_names in itertools.product(*((column, '"' + column + '"') for column in columns)):
                for separator in (", ", ","):
                    attrs = separator.join(attr_names)
                    relation = schema_name + "." + table_name
                    for direction, query in (("out", f"copy (SELECT {attrs} FROM ONLY {relation}) to stdout"),
                                             ("in", f"copy {relation}({attrs}) from stdin"),
                                             ("in", f"copy {relation}({attrs}) from stdin with (freeze)")):
                        queries[(query + "\0").encode()] = {"relation": ["m9", table], "direction": direction,
                                                            "range": [None, None], "format": "text"}

    class Observation(wire.PgoutputObservation):
        def copy_query(self, payload):
            if payload.lower().startswith(b"copy "):
                record = queries.get(payload)
                self.copy_invalid |= record is None
                return dict(record) if record else None

        def copy_frontend(self, kind, payload):
            if kind == b'd' and self.copy_current is not None:
                self.copy_current["wire_bytes"] = self.copy_current.get("wire_bytes", 0) + len(payload) + 5
            super().copy_frontend(kind, payload)

        def copy_backend(self, kind, payload):
            if kind == b'd' and self.copy_current is not None:
                self.copy_current["wire_bytes"] = self.copy_current.get("wire_bytes", 0) + len(payload) + 5
            super().copy_backend(kind, payload)

    class Gate(wire.WireGate):
        def __init__(self, host):
            super().__init__(host, "observe")

        def new_observation(self):
            with self.lock:
                observation = Observation(len(self.observers) + 1)
                if len(self.observers) == 64:
                    self.observer_overflow = True
                else:
                    self.observers.append(observation)
                return observation

    return Observation, Gate


def process_snapshot(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    require(len(fields) >= 22 and fields[0] != "Z", "fresh process observation unavailable")
    pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
    return {"start_identity": fields[19], "rss": pages * os.sysconf("SC_PAGE_SIZE"),
            "parent": int(fields[1]), "group": int(fields[2])}


def process_tree_rss(pid):
    values = {}
    for path in Path("/proc").iterdir():
        if path.name.isdecimal():
            try:
                values[int(path.name)] = process_snapshot(int(path.name))
            except (OSError, ValueError, RuntimeError):
                continue
    require(pid in values, "engine process resource observation absent")
    owned = {pid}
    while True:
        expanded = owned | {child for child, value in values.items() if value["parent"] in owned}
        if expanded == owned:
            return sum(values[child]["rss"] for child in owned)
        owned = expanded


def process_group_members(group):
    members = []
    for path in Path("/proc").iterdir():
        if path.name.isdecimal():
            try:
                if process_snapshot(int(path.name))["group"] == group:
                    members.append(int(path.name))
            except (OSError, ValueError, RuntimeError):
                continue
    return members


def measured_process(argv, environment, timeout, monitor=None):
    require(timeout > 0, "child absolute deadline expired")
    started = time.monotonic()
    process, identity = None, None
    with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as diagnostics:
        try:
            process = subprocess.Popen(argv, env=environment, stdout=output, stderr=diagnostics, start_new_session=True)
            identity = process_snapshot(process.pid)["start_identity"]
            peak = process_tree_rss(process.pid)
            while True:
                waited, status, usage = os.wait4(process.pid, os.WNOHANG)
                if waited:
                    process.returncode = os.waitstatus_to_exitcode(status)
                    break
                require(time.monotonic() - started < timeout, "child absolute deadline expired")
                peak = max(peak, process_tree_rss(process.pid))
                if monitor:
                    monitor()
                time.sleep(.01)
            if process_group_members(process.pid):
                os.killpg(process.pid, signal.SIGKILL)
                raise RuntimeError("clone workers did not stop with their owned parent")
            output.seek(0)
            diagnostics.seek(0)
            stdout, stderr = output.read(16 * 1024 * 1024 + 1), diagnostics.read(16 * 1024 * 1024 + 1)
            require(len(stdout) <= 16 * 1024 * 1024 and len(stderr) <= 16 * 1024 * 1024, "child diagnostic output exceeded bound")
            return {"pid": process.pid, "start_identity": identity, "peak_tree_rss_bytes": peak,
                    "wait4_rss_bytes": int(usage.ru_maxrss * (1024 if sys.platform == "linux" else 1)),
                    "cpu_user_seconds": usage.ru_utime, "cpu_system_seconds": usage.ru_stime,
                    "returncode": process.returncode, "stdout": stdout, "stderr": stderr,
                    "elapsed_seconds": time.monotonic() - started}
        finally:
            if process is not None and process.returncode is None:
                try:
                    require(identity is None or process_snapshot(process.pid)["start_identity"] == identity, "owned process identity changed")
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                _pid, status, _usage = os.wait4(process.pid, 0)
                process.returncode = os.waitstatus_to_exitcode(status)


class Runtime:
    def __init__(self, admission, root):
        self.admission, self.root = admission, root
        self.lock, self.lease = admission["artifacts"], admission["lease"]
        self.token = admission["manifest"]["run_id"]
        self.binary = self.lock["runtime"]["path"]
        require(os.environ.get("CONTAINER_RUNTIME", self.binary) == self.binary, "container runtime override differs from artifact lock")
        self.deadline = time.monotonic() + self.lease["deadline"] - time.time()
        self.containers, self.network, self.samples, self.images = {}, None, [], {}
        self.vm_root = retained_paths(root, self.token)[1]
        self.disk_start = None
        self.child_deadline = None
        self.facts = {}

    def remaining(self):
        values = [self.deadline - time.monotonic(), self.lease["deadline"] - time.time()]
        if self.child_deadline is not None:
            values.append(self.child_deadline - time.monotonic())
        return min(values)

    def raw(self, arguments, *, input=None, timeout=20, allowed=(0,), cleanup=False):
        if not cleanup:
            require(self.remaining() > 0, "original phase deadline expired")
            timeout = min(timeout, self.remaining())
        try:
            result = subprocess.run([self.binary, *arguments], input=input, capture_output=True, timeout=timeout)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("fixed runtime operation failed or deadline expired") from None
        require(result.returncode in allowed, "fixed runtime operation failed")
        return result.stdout, result.returncode

    def json(self, arguments, cleanup=False):
        raw, _status = self.raw(arguments, cleanup=cleanup)
        try:
            return json.loads(raw, object_pairs_hook=unique_object)
        except (ValueError, UnicodeError):
            raise RuntimeError("runtime returned malformed observation") from None

    def originals(self, cleanup=False):
        actual, _status = self.raw(["ps", "--all", "--no-trunc", "--format", "{{.ID}}"], cleanup=cleanup)
        ids = set(actual.decode().splitlines())
        expected = {service["id"] for service in self.lease["original_services"]} | {entry["id"] for entry in self.containers.values() if not entry.get("removed")}
        require(ids == expected, "exclusive runtime inventory changed")
        for service in self.lease["original_services"]:
            observed = self.json(["inspect", "--format", '{"Id":{{json .Id}},"Running":{{json .State.Running}}}', service["id"]], cleanup=cleanup)
            require(isinstance(observed, dict) and observed.get("Id") == service["id"]
                    and observed.get("Running") is service["running"],
                    "original service identity or availability changed")
        if not cleanup:
            names = fixed_command(["ps", "-axo", "comm="], {"PATH": "/usr/bin:/bin"}, timeout=min(5, self.remaining())).decode().splitlines()
            require(not any(Path(name.strip()).name in ("cargo", "rustc", "clang", "clang++", "ld", "lld") for name in names),
                    "host compiler or linker overlaps the exclusive benchmark window")

    def vm_capacity(self):
        memory, _status = self.raw(["machine", "ssh", "cat", "/proc/meminfo"])
        fields = {key.decode(): int(value) * 1024 for key, value in re.findall(rb"^(MemTotal|MemAvailable|SwapTotal|SwapFree):\s+(\d+) kB$", memory, re.MULTILINE)}
        require(set(fields) == {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}, "VM memory observation absent")
        disk, _status = self.raw(["machine", "ssh", "df", "-B1", "/var/home/core"])
        columns = disk.splitlines()[-1].split()
        cpus, _status = self.raw(["machine", "ssh", "getconf", "_NPROCESSORS_ONLN"])
        return {"memory_bytes": fields["MemTotal"], "available_memory_bytes": fields["MemAvailable"],
                "swap_bytes": fields["SwapTotal"] - fields["SwapFree"], "disk_free_bytes": int(columns[3]), "cpus": int(cpus)}

    def preflight(self):
        require(self.raw(["--version"])[0].decode().strip() == self.lock["runtime"]["version"], "actual runtime version differs")
        self.originals()
        identity = self.raw(["machine", "ssh", "cat", "/etc/machine-id"])[0].decode().strip()
        capacity = self.vm_capacity()
        require(identity == self.lease["host"]["identity"] and capacity["cpus"] == self.lease["host"]["cpus"]
                and capacity["memory_bytes"] == self.lease["host"]["memory_bytes"]
                and capacity["available_memory_bytes"] >= 18 * GIB and capacity["disk_free_bytes"] >= 32 * GIB
                and capacity["swap_bytes"] == 0, "actual VM admission differs or lacks headroom")
        self.disk_start = capacity["disk_free_bytes"]
        require(self.lease["campaign_disk_free_bytes"] - self.disk_start <= 24 * GIB, "retained campaign growth exceeded")
        parent = self.raw(["machine", "ssh", "stat", "--format=%u %a %d %i", "/var/home/core"])[0].split()
        uid = int(self.raw(["machine", "ssh", "id", "-u"])[0])
        require(len(parent) == 4 and int(parent[0]) == uid and not int(parent[1], 8) & 0o022,
                "VM parent ownership or permissions differ")
        _output, exists = self.raw(["machine", "ssh", "test", "-e", self.vm_root], allowed=(0, 1))
        require(exists == 1, "VM retained directory already exists")
        for role, field in (("server", "server_image"), ("runner", "runner_image")):
            image = self.json(["image", "inspect", self.lock[field]["reference"]])
            require(isinstance(image, list) and len(image) == 1, "actual immutable image absent")
            actual, expected = image[0], self.lock[field]
            require(actual.get("Id", actual.get("ID")) in (expected["config_digest"], expected["config_digest"].removeprefix("sha256:"))
                    and actual.get("Digest") == expected["manifest_digest"]
                    and actual.get("Os", actual.get("OS")) + "/" + actual.get("Architecture", "") == expected["platform"],
                    "actual image manifest, config, or platform differs")
            require(expected["index_digest"] is None or any(value.endswith("@" + expected["index_digest"]) for value in actual.get("RepoDigests", [])), "actual image index binding absent")
            self.images[role] = {"manifest_digest": actual["Digest"], "config_digest": expected["config_digest"], "platform": expected["platform"]}
        self.facts = {"host": {"identity": identity, "cpus": capacity["cpus"], "memory_bytes": capacity["memory_bytes"]},
                      "kernel": self.raw(["machine", "ssh", "uname", "-srvmo"])[0].decode().strip(),
                      "filesystem": self.raw(["machine", "ssh", "stat", "--file-system", "--format=%T:%s:%b", "/var/home/core"])[0].decode().strip(),
                      "storage": self.raw(["machine", "ssh", "findmnt", "--noheadings", "--output", "SOURCE,FSTYPE", "--target", "/var/home/core"])[0].decode().strip(),
                      "images": self.images, "runtime_version": self.lock["runtime"]["version"], "initial_vm_free_bytes": self.disk_start,
                      "host_lock_identity": self.admission["host_lock_identity"]}
        self.raw(["machine", "ssh", "mkdir", "--mode=0700", self.vm_root])
        for role in ("source", "target"):
            self.raw(["machine", "ssh", "mkdir", "--mode=0700", self.vm_root + "/" + role])

    def create_network(self):
        name = "m9-" + self.token
        publish(self.root / "network-intent.json", {"name": name, "owner": self.token})
        raw, _status = self.raw(["network", "create", "--internal", "--label", "io.rpgmigrate.m9=" + self.token, name])
        created = raw.decode().strip()
        require(created == name or sha256(created), "created network returned an unknown identity")
        actual = self.json(["network", "inspect", name])
        require(isinstance(actual, list) and len(actual) == 1, "created network observation absent")
        network_id = actual[0].get("id", actual[0].get("Id"))
        require(sha256(network_id), "created network identity absent")
        require(created in (name, network_id), "created network identity differs")
        self.network = {"id": network_id, "name": name}
        publish(self.root / "network-created.json", self.network)
        self.check_network()

    def check_network(self, cleanup=False):
        actual = self.json(["network", "inspect", self.network["id"]], cleanup=cleanup)[0]
        require(actual.get("id", actual.get("Id")) == self.network["id"]
                and actual.get("name", actual.get("Name")) == self.network["name"]
                and actual.get("internal", actual.get("Internal")) is True
                and actual.get("labels", actual.get("Labels", {})).get("io.rpgmigrate.m9") == self.token,
                "exact owned internal network differs")
        attached = actual.get("containers")
        require(isinstance(attached, dict) and not set(attached).difference(entry["id"] for entry in self.containers.values()), "foreign network attachment observed")
        return actual

    def mounts(self, role):
        if role != "runner":
            password = self.lock["profile"]["auth"][role]["password_file"]["path"]
            return [(self.vm_root + "/" + role, "/var/lib/postgresql/data", True), (password, password, False)]
        paths = {str(ROOT)}
        def collect(value):
            if isinstance(value, dict):
                if isinstance(value.get("path"), str) and Path(value["path"]).is_absolute():
                    paths.add(value["path"])
                for item in value.values():
                    collect(item)
            elif isinstance(value, list):
                for item in value:
                    collect(item)
        collect({key: value for key, value in self.lock.items() if key != "runtime"})
        for engine in ("native", "fork"):
            collect(read_json(self.lock[engine]["qualification"]["path"], private=True))
        paths.update(self.admission["paths"].values())
        paths.update(str(Path(self.admission["paths"]["manifest"]).parent / name) for name in ("packet.json", "schedule.json"))
        return [(path, path, False) for path in sorted(paths) if path == str(ROOT) or not Path(path).is_relative_to(ROOT)] + [(str(self.root), str(self.root), True)]

    def inspect_container(self, entry, cleanup=False):
        observed = self.json(["inspect", entry["id"]], cleanup=cleanup)
        require(isinstance(observed, list) and len(observed) == 1, "owned container observation absent")
        actual = observed[0]
        host = actual.get("HostConfig", {})
        cpu = host.get("NanoCpus") == 2000000000 or host.get("CpuQuota") == 200000 and host.get("CpuPeriod") == 100000
        mounts = actual.get("Mounts", [])
        require(actual.get("Id") == entry["id"] and actual.get("Name", "").lstrip("/") == entry["name"]
                and actual.get("Config", {}).get("Labels", {}).get("io.rpgmigrate.m9") == self.token
                and actual.get("Image", "").removeprefix("sha256:") == entry["image"].removeprefix("sha256:")
                and host.get("Memory") == 4 * GIB and host.get("MemorySwap") == 4 * GIB and cpu
                and host.get("Privileged") is False and host.get("PortBindings") in ({}, None)
                and host.get("NetworkMode") not in (None, "host", "container")
                and not any(actual.get("NetworkSettings", {}).get("Ports", {}).values())
                and all(mount.get("Type") == "bind" for mount in mounts)
                and sorted((mount.get("Source"), mount.get("Destination"), mount.get("RW")) for mount in mounts) == sorted(entry["mounts"]),
                "owned container identity, requested caps, or mounts differ")
        networks = actual.get("NetworkSettings", {}).get("Networks", {})
        running = actual.get("State", {}).get("Running") is True
        require((not running and not networks) or set(networks) == {self.network["name"]}
                and networks[self.network["name"]].get("NetworkID") in ((self.network["id"],) if running else ("", self.network["id"])), "owned container network differs")
        if running:
            require(entry["role"] in networks[self.network["name"]].get("Aliases", []), "owned container role alias differs")
        return actual

    def create_container(self, role):
        mounts = self.mounts(role)
        image = self.lock["runner_image" if role == "runner" else "server_image"]
        name = "m9-" + self.token + "-" + role
        argv = ["create", "--pull=never", "--name", name, "--network", self.network["name"], "--network-alias", role,
                "--label", "io.rpgmigrate.m9=" + self.token, "--cpus=2", "--memory=4g", "--memory-swap=4g"]
        for source, target, writable in mounts:
            require(":" not in source and "\n" not in source, "bind path cannot be represented safely")
            argv += ["--volume", source + ":" + target + (":rw" if writable else ":ro")]
        if role == "runner":
            argv += ["--user", str(os.getuid()) + ":" + str(os.getgid()), "--workdir", str(ROOT), image["reference"],
                     "python3", "-c", "import time; time.sleep(7200)"]
        else:
            password = self.lock["profile"]["auth"][role]["password_file"]["path"]
            argv += ["--env", "POSTGRES_PASSWORD_FILE=" + password, "--env", "PGDATA=/var/lib/postgresql/data",
                     "--env", "POSTGRES_INITDB_ARGS=--no-locale --encoding=UTF8", image["reference"],
                     "postgres", "-c", "wal_level=logical", "-c", "max_slot_wal_keep_size=1GB", "-c", "max_wal_size=1GB",
                     "-c", "fsync=on", "-c", "full_page_writes=on", "-c", "synchronous_commit=on"]
        cid = self.raw(argv)[0].decode().strip()
        require(sha256(cid) and cid not in {service["id"] for service in self.lease["original_services"]}, "fresh container identity absent")
        entry = {"id": cid, "role": role, "name": name, "image": image["config_digest"], "mounts": mounts, "removed": False}
        self.containers[role] = entry
        publish(self.root / (role + "-created.json"), entry)
        self.inspect_container(entry)
        self.raw(["start", cid])
        self.inspect_container(entry)
        if role != "runner":
            deadline = time.monotonic() + min(30, self.remaining())
            while True:
                _output, status = self.raw(["exec", cid, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"], allowed=(0, 1, 2))
                if status == 0:
                    break
                require(time.monotonic() < deadline, "owned PostgreSQL readiness absent")
                time.sleep(.05)

    def observe(self, initial=False):
        self.originals()
        self.check_network()
        capacity = self.vm_capacity()
        require(capacity["available_memory_bytes"] >= 6 * GIB, "VM memory headroom fell below resource floor")
        value = {"disk_free_bytes": capacity["disk_free_bytes"], "phase_growth_bytes": max(0, self.disk_start - capacity["disk_free_bytes"]),
                 "campaign_growth_bytes": max(0, self.lease["campaign_disk_free_bytes"] - capacity["disk_free_bytes"]),
                 "source_pg_wal_bytes": 0, "target_pg_wal_bytes": 0, "slot_wal_bytes": 0,
                 "swap_bytes": capacity["swap_bytes"], "oom_events": 0, "memory_pressure_events": 0, "roles": {}}
        owned_bytes = 0
        for role, entry in self.containers.items():
            actual = self.inspect_container(entry)
            require(actual.get("State", {}).get("Running") is True and actual.get("State", {}).get("OOMKilled") is False,
                    "owned container stopped or was OOM killed")
            observed = self.raw(["exec", entry["id"], "cat", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max",
                                 "/sys/fs/cgroup/memory.swap.max", "/sys/fs/cgroup/memory.events"])[0].splitlines()
            require(len(observed) >= 6, "effective cgroup observations absent")
            events = dict(line.split() for line in observed[3:])
            require({b"oom", b"oom_kill", b"high", b"max"}.issubset(events), "memory events observation absent")
            value["oom_events"] += int(events[b"oom"]) + int(events[b"oom_kill"])
            value["memory_pressure_events"] += int(events[b"high"]) + int(events[b"max"])
            value["roles"][role] = {"requested_cpus": 2, "requested_memory_bytes": 4 * GIB, "requested_swap_bytes": 0,
                "effective_cpu_max": [int(item) for item in observed[0].split()], "effective_memory_max": int(observed[1]), "effective_swap_max": int(observed[2])}
            if role != "runner":
                query = "SELECT (SELECT coalesce(sum(size),0) FROM pg_catalog.pg_ls_waldir()),(SELECT coalesce(sum(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)),0) FROM pg_catalog.pg_replication_slots), (SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE wal_status NOT IN ('reserved','extended') OR wal_status IS NULL);"
                raw = self.raw(["exec", entry["id"], "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c", query])[0].strip().split(b"|")
                require(len(raw) == 3 and int(raw[2]) == 0, "unreserved or lost WAL observed")
                value[role + "_pg_wal_bytes"] = int(raw[0])
                value["slot_wal_bytes"] += int(raw[1])
                growth = self.raw(["exec", entry["id"], "du", "--summarize", "--bytes", "/var/lib/postgresql/data"])[0].split()
                require(growth and growth[0].isdigit(), "phase-owned PGDATA size observation absent")
                owned_bytes += int(growth[0])
        require(owned_bytes <= 12 * GIB, "VM phase-owned growth exceeds resource limit")
        validate_resources(value, initial)
        self.samples.append(value)
        return value

    def cleanup(self):
        result = {"containers": [], "network": {"ownership_checked": False, "absent": False},
                  "original_services_unchanged": False, "retained_paths": retained_paths(self.root, self.token)}
        for role, entry in reversed(list(self.containers.items())):
            record = {"role": role, "id": entry["id"], "ownership_checked": False, "absent": False}
            result["containers"].append(record)
            try:
                self.inspect_container(entry, cleanup=True)
                record["ownership_checked"] = True
                self.raw(["stop", "--time", "10", entry["id"]], cleanup=True)
                stopped = self.inspect_container(entry, cleanup=True)
                require(stopped.get("State", {}).get("Running") is False
                        and (role == "runner" or stopped.get("State", {}).get("ExitCode") == 0), "owned PostgreSQL clean stop absent")
                self.raw(["rm", entry["id"]], cleanup=True)
                _output, status = self.raw(["container", "exists", entry["id"]], allowed=(0, 1), cleanup=True)
                record["absent"] = status == 1
                entry["removed"] = record["absent"]
            except (OSError, RuntimeError, ValueError):
                continue
        if self.network is not None:
            try:
                require(not self.check_network(cleanup=True)["containers"], "owned network still has attached containers")
                result["network"]["ownership_checked"] = True
                self.raw(["network", "rm", self.network["id"]], cleanup=True)
                _output, status = self.raw(["network", "exists", self.network["id"]], allowed=(0, 1), cleanup=True)
                result["network"]["absent"] = status == 1
            except (OSError, RuntimeError, ValueError):
                pass
        try:
            self.originals(cleanup=True)
            result["original_services_unchanged"] = True
        except (OSError, RuntimeError, ValueError):
            pass
        return result


def fixed_command(argv, environment, input=None, timeout=20):
    require(timeout > 0, "child absolute deadline expired")
    try:
        result = subprocess.run(argv, env=environment, input=input, capture_output=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        raise RuntimeError("fixed child operation failed or deadline expired") from None
    require(result.returncode == 0 and not result.stderr, "fixed child operation failed or emitted diagnostics")
    return result.stdout


def child_environment(lock, engine, root):
    tools = lock["tools"]
    require(len({str(Path(value["path"]).parent) for value in tools.values()}) == 1, "PostgreSQL tools must share one locked bindir")
    return {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": str(root), "LANG": "C", "LC_ALL": "C",
            "PG_CONFIG": tools["pg_config"]["path"], "PG_BINDIR": str(Path(tools["psql"]["path"]).parent),
            "PGCOPYDB_USE_COPY_BINARY": "false", "LD_LIBRARY_PATH": str(Path(lock[engine]["libpq"]["path"]).parent)}


def verify_child_artifacts(lock, engine, environment, timeout):
    import ctypes
    for name, tool in lock["tools"].items():
        version = fixed_command([tool["path"], "--version"], environment, timeout=timeout).decode().strip()
        require(re.search(r"PostgreSQL\)?\s+" + re.escape(tool["version"]) + r"(?:\s|$)", version), "actual client version differs")
    bindir = fixed_command([lock["tools"]["pg_config"]["path"], "--bindir"], environment, timeout=timeout).decode().strip()
    require(Path(bindir) == Path(lock["tools"]["psql"]["path"]).parent, "actual pg_config bindir differs from locked tools")
    libpq = lock[engine]["libpq"]
    library = ctypes.CDLL(libpq["path"])
    library.PQlibVersion.restype = ctypes.c_int
    require(library.PQlibVersion() == libpq["pq_version"], "actual PQlibVersion differs")
    linked = fixed_command(["ldd", lock[engine]["binary"]["path"]], environment, timeout=timeout).decode()
    matches = re.findall(r"libpq[^\s]*\s+=>\s+([^\s]+)", linked)
    require(len(matches) == 1 and Path(matches[0]).resolve() == Path(libpq["path"]).resolve(), "actual engine linked libpq differs")
    return {"tools": {name: tool["version"] for name, tool in lock["tools"].items()},
            "libpq_sha256": libpq["sha256"], "pq_version": library.PQlibVersion()}


def bench_database_type():
    clone = harness_module("clone")

    class Database(clone.Database):
        def __init__(self, side, lock, environment, deadline):
            super().__init__(side, Path(lock["tools"]["psql"]["path"]).parent, environment)
            self.side, self.lock, self.deadline = side, lock, deadline
            self.created = {}
            self.ledger = Path(environment["HOME"])

        def connection(self, name):
            path = self.lock["profile"]["auth"][self.side]["passfile"]["path"]
            require("'" not in path and "\\" not in path and "\n" not in path, "authentication path cannot be represented safely")
            return super().connection(name) + " passfile='" + path + "'"

        def sql(self, name, sql, *, tagged=False, variables=()):
            argv = [self.lock["tools"]["psql"]["path"], "-X", "-At", "-v", "ON_ERROR_STOP=1", "--dbname", self.connection(name)]
            if not tagged:
                argv.append("-q")
            for key, value in variables:
                argv += ["--set", key + "=" + value]
            return fixed_command(argv, self.environment, (clone.SETTINGS + sql).encode(), self.deadline - time.monotonic())

        def database_oid(self, name):
            require(re.fullmatch(r"m9_[0-9a-f]{32}_[st]", name), "child database name differs")
            value = self.sql("postgres", "SELECT oid FROM pg_catalog.pg_database WHERE datname='" + name + "';").strip()
            require(value.isdigit() and int(value) > 0, "fresh child database identity absent")
            return int(value)

        def create_child(self, name):
            exists = self.sql("postgres", "SELECT count(*) FROM pg_catalog.pg_database WHERE datname='" + name + "';").strip()
            require(exists == b"0", "child database already exists")
            publish(self.ledger / (self.side + "-database-intent.json"), {"side": self.side, "name": name, "observed_absent": True})
            self.sql("postgres", "CREATE DATABASE " + clone.quote(name) + ";")
            oid = self.database_oid(name)
            self.created[name] = oid
            publish(self.ledger / (self.side + "-database-created.json"), {"side": self.side, "name": name, "oid": oid})
            return oid

        def seed(self, name, shape):
            self.sql(name, (FIXTURES / "schema.sql").read_text() + (FIXTURES / "seed.sql").read_text(), variables=(("shape", shape),))

        def verify_runtime(self):
            raw = self.sql("postgres", "SELECT current_setting('server_version_num'),current_setting('fsync'),current_setting('full_page_writes'),current_setting('wal_level'),current_setting('max_slot_wal_keep_size'),current_setting('max_wal_size'),current_setting('server_encoding'),(SELECT system_identifier FROM pg_catalog.pg_control_system());").strip().split(b"|")
            require(len(raw) == 8 and raw[0].isdigit() and int(raw[0]) // 10000 == 18
                    and raw[1:7] == [b"on", b"on", b"logical", b"1GB", b"1GB", b"UTF8"] and raw[7].isdigit() and int(raw[7]) > 0, "actual PostgreSQL version or settings differ")
            result = dict(zip(("server_version_num", "fsync", "full_page_writes", "wal_level", "max_slot_wal_keep_size", "max_wal_size", "server_encoding", "system_identifier"), (value.decode() for value in raw)))
            result["server_version_num"] = int(result["server_version_num"])
            return result

        def fingerprint(self, name, shape):
            tables = expected_rows(shape)
            inventory = self.sql(name, "SELECT c.relname,c.oid,c.relpersistence,c.relkind,am.amname,pg_relation_size(c.oid),pg_indexes_size(c.oid) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_catalog.pg_am am ON am.oid=c.relam WHERE n.nspname='m9' AND c.relkind IN ('r','p','f','v','m','S') ORDER BY c.relname;").decode().splitlines()
            require(len(inventory) == len(tables), "actual fixture table inventory differs")
            result, observed = {}, []
            for line in inventory:
                fields = line.split("|")
                require(len(fields) == 7 and fields[0] in tables and fields[2:5] == ["p", "r", "heap"], "fixture is not an ordinary logged heap")
                table, oid, _persistence, _kind, _method, heap, indexes = fields
                columns = '"id","value"' + (',"payload"' if table != "probe" else '')
                data = self.sql(name, "COPY (SELECT " + columns + " FROM ONLY m9." + clone.quote(table) + ") TO STDOUT WITH (FORMAT text);")
                rows = data.splitlines(keepends=True)
                require(len(rows) == tables[table], "actual fixture row count differs")
                result[table] = clone.row_fingerprint(rows, 100000, 67108864)
                observed.append({"name": table, "oid": int(oid), "rows": len(rows), "canonical_text_bytes": len(data),
                                 "heap_bytes": int(heap), "index_bytes": int(indexes)})
            schema = self.sql(name, "SELECT c.relname,a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity,a.attgenerated,coalesce(pg_catalog.pg_get_expr(d.adbin,d.adrelid),'') FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE n.nspname='m9' AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum;")
            obligations = self.sql(name, "SELECT c.relname,con.conname,pg_catalog.pg_get_constraintdef(con.oid) FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid=con.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='m9' AND con.contype<>'n' ORDER BY 1,2; SELECT c.relname,pg_catalog.pg_get_indexdef(i.indexrelid) FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='m9' ORDER BY 1,2; SELECT c.relname,t.tgname,t.tgenabled,pg_catalog.pg_get_triggerdef(t.oid) FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='m9' AND NOT t.tgisinternal ORDER BY 1,2;")
            invalid = self.sql(name, "SELECT count(*) FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid=con.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='m9' AND con.contype='n' AND NOT con.convalidated;").strip()
            result["schema"] = clone.schema_fingerprint(schema, obligations, invalid)
            return result, observed

        def barrier(self, name):
            probe = self.sql(name, "SELECT c.relpersistence,(SELECT count(*) FROM ONLY m9.probe WHERE id=1 AND value=0) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='m9' AND c.relname='probe' AND c.relkind='r';").strip()
            require(probe == b"p|1", "known logged writing barrier row absent")
            raw = self.sql(name, "SET synchronous_commit=local; SELECT current_setting('synchronous_commit'); BEGIN; UPDATE ONLY m9.probe SET value=value WHERE id=1 AND value=0; COMMIT;", tagged=True)
            tags = raw.decode().splitlines()
            # The common session settings emit SET tags before the writing barrier.
            require(tags[-5:] == ["SET", "local", "BEGIN", "UPDATE 1", "COMMIT"], "positive writing barrier COMMIT or UPDATE 1 absent")
            return {"synchronous_commit": "local", "logged_probe": True, "update_tag": "UPDATE 1",
                    "commit_tag": "COMMIT", "diagnostics_empty": True, "closed": True}

        def verify_bookkeeping(self, name, state, engine, shape):
            namespaces = self.sql(name, "SELECT oid,nspname FROM pg_catalog.pg_namespace WHERE oid>=16384 AND nspname<>'m9' ORDER BY oid;").decode().splitlines()
            if engine == "fork":
                require(not namespaces, "unexpected target bookkeeping namespace")
                return
            native = read_json(state / "manifest.json", private=True)
            require(native.get("format_version") == 10, "native state format differs")
            records = native.get("target_provenance", [])
            require(len(records) == 1 and records[0].get("phase") == "confirmed", "native confirmed target bookkeeping absent")
            record = records[0]["record"]
            binding = record["intent"]["binding"]
            require(binding["target_database_oid"] == self.created[name]
                    and namespaces == [str(record["schema_oid"]) + "|" + binding["schema_name"]], "native target namespace identity differs")
            actual = self.sql(name, "SELECT oid FROM pg_catalog.pg_class WHERE relnamespace=" + str(record["schema_oid"]) + " ORDER BY oid;").decode().splitlines()
            expected = sorted(record[key] for key in ("table_oid", "marker_table_oid", "marker_index_oid", "large_object_table_oid", "large_object_index_oid"))
            primary = self.sql(name, "SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid=" + str(record["table_oid"]) + " AND indisprimary;").strip()
            require(primary.isdigit() and int(primary) > 0, "native control primary index identity absent")
            expected = sorted([*expected, int(primary)])
            require(actual == [str(value) for value in expected], "native bookkeeping object identities differ")
            self.completion(name, state, {"tables": list(expected_rows(shape))})

        def reset(self, name, retain=False):
            oid = self.database_oid(name)
            require(self.created.get(name) == oid, "fresh child database was replaced before reset")
            active = self.sql("postgres", "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datid=" + str(oid) + ";").strip()
            require(active == b"0", "child database still has workers before reset")
            slots = self.sql("postgres", "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE datoid=" + str(oid) + ";").strip()
            publications = self.sql(name, "SELECT count(*) FROM pg_catalog.pg_publication;").strip()
            require(slots == publications == b"0", "plain COPY child has unexpected replication resources")
            if not retain:
                self.sql("postgres", "DROP DATABASE " + clone.quote(name) + ";")
                require(self.sql("postgres", "SELECT count(*) FROM pg_catalog.pg_database WHERE oid=" + str(oid) + ";").strip() == b"0", "exact child database removal not observed")
            result = {"side": self.side, "name": name, "oid": oid, "identity_checked": True, "status": "retained" if retain else "removed"}
            publish(self.ledger / (self.side + "-database-reset.json"), result)
            return result

    return Database


def execute_child(admission, root, child_id):
    require(sys.platform == "linux", "benchmark engine measurement requires the owned Linux runner")
    child = next(value for value in admission["schedule"] if value["child_id"] == child_id)
    require(time.time() < admission["lease"]["deadline"], "original lease expired before child")
    authority = admission["worker_authority"]
    started, monotonic, wall = authority["started"], time.monotonic(), time.time()
    prelude = wall - started
    require(0 <= prelude < 120, "child original start or clock continuity differs")
    deadline = monotonic + min(admission["lease"]["deadline"], authority["child_deadline"]) - wall
    work = private_root(root / (child_id + ".work"), create=True)
    environment = child_environment(admission["artifacts"], child["engine"], work)
    client_runtime = verify_child_artifacts(admission["artifacts"], child["engine"], environment, deadline - time.monotonic())
    Database = bench_database_type()
    databases = {side: Database(side, admission["artifacts"], environment, deadline) for side in ("source", "target")}
    server_runtime = {}
    for side, database in databases.items():
        server_runtime[side] = database.verify_runtime()
        database.create_child(child[side + "_db"])
    databases["source"].seed(child["source_db"], child["shape"])
    source, observed = databases["source"].fingerprint(child["source_db"], child["shape"])
    _Observer, Gate = copy_observer_types(child["shape"])
    state = work / (child["engine"] + "-state")
    clone_started = time.monotonic()
    with Gate("source") as source_gate, Gate("target") as target_gate:
        argv = [admission["artifacts"][child["engine"]]["binary"]["path"], "clone", "--source",
                source_gate.connection(databases["source"].connection(child["source_db"])), "--target",
                target_gate.connection(databases["target"].connection(child["target_db"])), "--dir", str(state),
                "--table-jobs", "4", "--split-tables-larger-than", "0"]
        process = measured_process(argv, environment, deadline - time.monotonic())
        require(process["returncode"] == 0, "plain clone child failed")
        if child["engine"] == "native":
            try:
                report = json.loads(process["stdout"], object_pairs_hook=unique_object)
            except (ValueError, UnicodeError):
                raise RuntimeError("plain native completion report absent") from None
            require(report.get("complete") is True and report.get("phase") == "completed", "plain native completion report incomplete")
        barrier = databases["target"].barrier(child["target_db"])
    elapsed = time.monotonic() - clone_started
    wire = {"source": source_gate.copy_evidence(), "target": target_gate.copy_evidence()}
    validate_wire(wire, child["shape"])
    target, _target_observed = databases["target"].fingerprint(child["target_db"], child["shape"])
    require(source == target, "independent schema or contents differ")
    databases["target"].verify_bookkeeping(child["target_db"], state, child["engine"], child["shape"])
    clone = harness_module("clone")
    clone.wait_for(lambda: databases["target"].sql(child["target_db"], "SELECT count(*) FROM pg_catalog.pg_stat_user_tables WHERE schemaname='m9' AND last_analyze IS NOT NULL;").strip()
                   == str(len(expected_rows(child["shape"]))).encode(), "clone ANALYZE completion", timeout=min(3, deadline - time.monotonic()))
    retain = child == admission["schedule"][-1]
    cleanup = {"workers_stopped": True, "databases": [databases[side].reset(child[side + "_db"], retain) for side in ("source", "target")]}
    measurements = {key: process[key] for key in ("pid", "start_identity", "peak_tree_rss_bytes", "wait4_rss_bytes", "cpu_user_seconds", "cpu_system_seconds")}
    measurements.update({key: child[key] for key in ("engine", "shape", "pair_index", "sample")})
    measurements.update(clone_elapsed_seconds=elapsed, child_elapsed_seconds=time.monotonic() - monotonic + prelude,
                        canonical_text_bytes=sum(table["canonical_text_bytes"] for table in observed),
                        wire_bytes=sum(record.get("wire_bytes", 0) for record in wire["source"]["records"]),
                        timer_resolution_seconds=max(.01, time.get_clock_info("monotonic").resolution))
    require(time.monotonic() < deadline and time.time() <= min(admission["lease"]["deadline"], authority["child_deadline"]), "child absolute deadline expired")
    return {"schema_version": 1, "kind": "m9-benchmark-receipt", "run_id": admission["manifest"]["run_id"],
            "child_id": child_id, **admission["hashes"], "started": started, "finished": time.time(),
            "measurements": measurements, "oracles": {"source": source, "target": target, "fixture_observed": observed,
                "wire": wire, "barrier": barrier, "runtime": {"servers": server_runtime, "client": client_runtime}, "completion": {"workers_stopped": True, "postdata": True,
                    "sequences": True, "analyze": True, "synchronization": True, "closed": True}},
            "resources": [], "cleanup": cleanup, "status": "success"}


def dispatch_child(runtime, child):
    paths = runtime.admission["paths"]
    command = [runtime.binary, "exec", runtime.containers["runner"]["id"], "python3", str(Path(__file__).resolve()), "run",
               "--approval", paths["approval"], "--manifest", paths["manifest"], "--artifacts", paths["artifacts"],
               "--phase", PHASE, "--lease", paths["lease"], "--receipt-root", str(runtime.root), "--child-id", child["child_id"]]
    require(runtime.remaining() > 150, "original phase lacks child and cleanup budget")
    started = time.monotonic()
    started_wall = time.time()
    runtime.child_deadline = started + 120
    sample_start = len(runtime.samples)
    publish(runtime.root / (child["child_id"] + ".admission.json"), {
        "schema_version": 1, "kind": "m9-benchmark-child-admission", "child_id": child["child_id"],
        "run_id": runtime.token, **runtime.admission["hashes"], "runner_id": runtime.containers["runner"]["id"],
        "server_ids": {side: runtime.containers[side]["id"] for side in ("source", "target")},
        "resources": runtime.observe(), "deadline": runtime.lease["deadline"],
        "started": started_wall, "child_deadline": started_wall + 120})
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    last_observation = started
    try:
        while True:
            require(runtime.remaining() > 0 and time.monotonic() - started < 120, "child or original phase absolute deadline expired")
            try:
                output, diagnostics = process.communicate(timeout=min(.25, 120 - (time.monotonic() - started), runtime.remaining()))
                break
            except subprocess.TimeoutExpired:
                if time.monotonic() - last_observation >= 1:
                    runtime.observe()
                    last_observation = time.monotonic()
        require(process.returncode == 0 and not diagnostics and len(output) <= 4 * 1024 * 1024, "owned Linux child failed or emitted diagnostics")
        try:
            receipt = json.loads(output, object_pairs_hook=unique_object)
        except (ValueError, UnicodeError):
            raise RuntimeError("owned Linux child receipt malformed") from None
        receipt["resources"] = [*runtime.samples[sample_start:], runtime.observe()]
        validate_child(receipt, child, runtime.admission)
        return receipt
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
        runtime.child_deadline = None


def verify_worker_authority(admission, root, child_id):
    record = read_json(root / (child_id + ".admission.json"), private=True)
    envelope(record, "child-admission", ("child_id", "run_id", "packet_sha256", "approval_sha256", "lease_sha256",
        "artifact_sha256", "fixture_sha256", "runner_id", "server_ids", "resources", "deadline", "started", "child_deadline"))
    require(record["child_id"] == child_id and record["run_id"] == admission["manifest"]["run_id"]
            and all(record[key] == value for key, value in admission["hashes"].items())
            and record["deadline"] == admission["lease"]["deadline"] and sha256(record["runner_id"]), "internal child admission differs")
    require(type(record["started"]) in (int, float) and type(record["child_deadline"]) in (int, float)
            and admission["lease"]["started"] <= record["started"] <= time.time() < record["child_deadline"]
            <= min(record["started"] + 120, admission["lease"]["deadline"]), "internal child original deadline or clock continuity differs")
    keys(record["server_ids"], ("source", "target"), "admitted server identities")
    require(all(sha256(value) for value in record["server_ids"].values()), "admitted server identities absent")
    container = Path("/run/.containerenv").read_text()
    require(re.search(r'^id="' + record["runner_id"] + r'"$', container, re.MULTILINE), "internal child is not in its exact owned runner")
    require(Path("/sys/fs/cgroup/cpu.max").read_text().split() == ["200000", "100000"]
            and Path("/sys/fs/cgroup/memory.max").read_text().strip() == str(4 * GIB)
            and Path("/sys/fs/cgroup/memory.swap.max").read_text().strip() == "0", "internal child effective limits differ")
    validate_resources(record["resources"])
    admission["worker_authority"] = record


def execute_phase(admission, root):
    runtime = Runtime(admission, root)
    children, error = [], None
    for name, path in admission["paths"].items():
        publish_bytes(root / (name + ".json"), read_bytes(path, private=True))
    publish_bytes(root / "packet.json", read_bytes(Path(admission["paths"]["manifest"]).parent / "packet.json", private=True))
    try:
        runtime.preflight()
        runtime.create_network()
        for role in ("source", "target", "runner"):
            runtime.create_container(role)
        runtime.facts["admission_resources"] = runtime.observe()
        for index, child in enumerate(admission["schedule"]):
            started = time.time()
            try:
                receipt = dispatch_child(runtime, child)
            except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError):
                receipt = {"schema_version": 1, "kind": "m9-benchmark-receipt", "run_id": admission["manifest"]["run_id"],
                    "child_id": child["child_id"], **admission["hashes"], "started": started, "finished": time.time(),
                    "measurements": {}, "oracles": {}, "resources": runtime.samples[-1:],
                    "cleanup": {"workers_stopped": False, "databases": []}, "status": "failed"}
                publish(root / (child["child_id"] + ".json"), receipt)
                children.append({"child_id": child["child_id"], "sha256": digest(encode(receipt))})
                raise
            publish(root / (child["child_id"] + ".json"), receipt)
            children.append({"child_id": child["child_id"], "sha256": digest(encode(receipt))})
            if index == 3:
                publish(root / "mechanics-checkpoint.json", {"schema_version": 1, "kind": "m9-benchmark-mechanics-checkpoint",
                    "children": children, "status": "passed-incomplete"})
        require(len(children) == 40, "complete forty-child schedule required")
    except (OSError, RuntimeError, ValueError, KeyError, subprocess.SubprocessError):
        error = "phase failed; retained exact ownership and child evidence require review"
    finally:
        cleanup = runtime.cleanup()
        successful_cleanup = len(cleanup["containers"]) == 3 and all(record["ownership_checked"] and record["absent"] for record in cleanup["containers"])
        successful_cleanup &= cleanup["network"]["ownership_checked"] and cleanup["network"]["absent"] and cleanup["original_services_unchanged"]
        if not successful_cleanup:
            error = "phase cleanup incomplete; retain exact owned resources for handoff"
        phase = {"schema_version": 1, "kind": "m9-benchmark-phase-receipt", **admission["hashes"],
                 "manifest_sha256": digest(read_bytes(admission["paths"]["manifest"])), "lease": admission["lease"],
                 "children": children, "cleanup": cleanup, "runtime": runtime.facts, "status": "success" if error is None else "failed"}
        if error is not None:
            publish(root / "phase.json", phase)
    require(error is None, error)
    return phase


def main(argv=None):
    require(not sys.flags.optimize, "optimized Python cannot run benchmark controls")
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    prepare_parser.add_argument("--output", required=True)
    prepare_parser.add_argument("--artifacts", required=True)
    prepare_parser.add_argument("--seed", type=int, default=20261110)
    run_parser = commands.add_parser("run")
    for option in ("approval", "manifest", "artifacts", "phase", "lease", "receipt-root"):
        run_parser.add_argument("--" + option, required=True)
    run_parser.add_argument("--child-id", help=argparse.SUPPRESS)
    summary_parser = commands.add_parser("summarize")
    for option in ("manifest", "artifacts", "receipt-root"):
        summary_parser.add_argument("--" + option, required=True)
    args = parser.parse_args(argv)
    if args.command == "prepare":
        prepare(args.output, args.artifacts, args.seed)
        return 0
    if args.command == "summarize":
        summarize(args.manifest, args.artifacts, args.receipt_root)
        return 0
    if args.child_id is not None:
        admission = admit(args.approval, args.manifest, args.artifacts, args.lease, args.receipt_root, args.phase, child_id=args.child_id)
        verify_worker_authority(admission, private_root(args.receipt_root), args.child_id)
        receipt = execute_child(admission, private_root(args.receipt_root), args.child_id)
        sys.stdout.buffer.write(encode(receipt))
        return 0
    run_phase(args.approval, args.manifest, args.artifacts, args.lease, args.receipt_root, args.phase)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except RuntimeError as error:
        print("m9-benchmark: " + str(error), file=sys.stderr)
        sys.exit(1)
    except Exception:
        print("m9-benchmark: refused or failed; private retained evidence requires review", file=sys.stderr)
        sys.exit(1)
