#!/usr/bin/env python3
"""Capture synthetic pgoutput v1 packets from private disposable PG16/18 containers."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parent


def run(*args, input=None):
    return subprocess.run(args, input=input, text=True, check=True, capture_output=True, timeout=60).stdout.strip()


def capture(major):
    name = f"rpgmigrate-decoder-{major}-{uuid.uuid4().hex[:10]}"
    run("podman", "run", "--detach", "--name", name, "-e", "POSTGRES_HOST_AUTH_METHOD=trust", f"docker.io/library/postgres:{major}", "postgres", "-c", "wal_level=logical", "-c", "logical_decoding_work_mem=64kB")
    try:
        deadline = time.monotonic() + 40
        while subprocess.run(["podman", "exec", name, "pg_isready", "-U", "postgres"], capture_output=True, timeout=10).returncode:
            if time.monotonic() >= deadline:
                raise RuntimeError("private fixture server did not become ready")
            time.sleep(0.1)
        def sql(statement):
            return run("podman", "exec", "-i", name, "psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", input=statement)
        version = json.loads(sql("SELECT json_build_object('server_version', current_setting('server_version'), 'server_version_num', current_setting('server_version_num'), 'logical_decoding_work_mem', current_setting('logical_decoding_work_mem'))"))
        sql((ROOT / "capture.sql").read_text())
        types = json.loads(sql("""WITH RECURSIVE types AS (
            SELECT oid AS source_oid, oid, typbasetype FROM pg_type WHERE typnamespace = 'public'::regnamespace
            UNION ALL SELECT types.source_oid, base.oid, base.typbasetype FROM types JOIN pg_type base ON base.oid = types.typbasetype
        ) SELECT json_agg(json_build_object('oid', source_oid::bigint, 'schema', n.nspname, 'name', t.typname) ORDER BY source_oid)
        FROM types JOIN pg_type t USING (oid) JOIN pg_namespace n ON n.oid = t.typnamespace WHERE types.typbasetype = 0"""))
        raw = sql("SET client_encoding='UTF8'; SET DateStyle='ISO,YMD'; SET IntervalStyle='postgres'; SET TimeZone='UTC'; SET extra_float_digits=3; SET bytea_output='hex'; SELECT json_build_object('lsn', lsn::text, 'xid', xid::text, 'hex', encode(data, 'hex')) FROM pg_logical_slot_get_binary_changes('decoder_fixture_slot', NULL, NULL, 'proto_version', '1', 'publication_names', 'decoder_fixture_publication'); SELECT pg_stat_force_next_flush();")
        packets = [json.loads(line) for line in raw.splitlines() if line.startswith('{')]
        deadline = time.monotonic() + 10
        while True:
            spill = json.loads(sql("SELECT json_build_object('spill_txns', spill_txns, 'spill_count', spill_count, 'spill_bytes', spill_bytes) FROM pg_stat_replication_slots WHERE slot_name='decoder_fixture_slot'"))
            if spill['spill_txns'] and spill['spill_bytes']:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("source spill proof is missing")
            time.sleep(0.1)
        inventory = {}
        for packet in packets:
            tag = chr(bytes.fromhex(packet['hex'])[0])
            inventory[tag] = inventory.get(tag, 0) + 1
        if set(inventory) != set('BCRYIUDTO') or inventory['I'] < 512:
            raise RuntimeError("required packet inventory is incomplete")
        result = {"version": 1, "server": version, "plugin_options": {"proto_version": "1", "publication_names": "decoder_fixture_publication"}, "capture_sql_sha256": hashlib.sha256((ROOT/'capture.sql').read_bytes()).hexdigest(), "source_types": types, "source_spill": spill, "expected_tags": inventory, "expected_spilled_inserts": 512, "packets": packets}
        (ROOT/f'pg{major}.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({"major": major, "server": version, "tags": inventory, "spill": spill}))
    finally:
        run("podman", "rm", "--force", name)


FILES = ("PROVENANCE.md", "capture.py", "capture.sql", "pg16.json", "pg18.json")
TAGS = {"B": 10, "C": 10, "R": 7, "Y": 3, "I": 518, "U": 5, "D": 3, "T": 1, "O": 1}


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate fixture key")
        value[key] = item
    return value


def read_json(path):
    return json.loads(path.read_bytes(), object_pairs_hook=unique_object)


def write_manifest(root=ROOT):
    entries = [{"path": name, "sha256": hashlib.sha256((root/name).read_bytes()).hexdigest()} for name in FILES]
    (root/"manifest.json").write_text(json.dumps({"version": 1, "files": entries}, indent=2) + "\n")


def verify(root=ROOT):
    manifest = read_json(root/"manifest.json")
    if set(manifest) != {"version", "files"} or manifest["version"] != 1:
        raise ValueError("invalid fixture manifest")
    entries = manifest["files"]
    if not isinstance(entries, list) or [entry.get("path") for entry in entries] != list(FILES):
        raise ValueError("fixture inventory differs")
    actual = sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file())
    if actual != sorted((*FILES, "manifest.json")):
        raise ValueError("fixture files differ")
    for entry in entries:
        path = root/entry["path"]
        if set(entry) != {"path", "sha256"} or path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise ValueError("missing or changed fixture")
    capture_hash = hashlib.sha256((root/"capture.sql").read_bytes()).hexdigest()
    for major in (16,18):
        data = read_json(root/f"pg{major}.json")
        if data.get("version") != 1 or int(data["server"]["server_version_num"])//10000 != major or data.get("capture_sql_sha256") != capture_hash:
            raise ValueError("fixture provenance differs")
        packets = data.get("packets")
        if not isinstance(packets, list) or not packets or data.get("expected_tags") != TAGS or data.get("expected_spilled_inserts") != 512:
            raise ValueError("empty or changed packet inventory")
        tags = {}
        for packet in packets:
            payload = bytes.fromhex(packet["hex"])
            if not payload or len(payload)>16*1024*1024:
                raise ValueError("empty or oversized packet")
            tag = chr(payload[0]);tags[tag] = tags.get(tag,0)+1
        if tags != TAGS or not data["source_types"] or data["source_spill"]["spill_txns"]<1 or data["source_spill"]["spill_bytes"]<1:
            raise ValueError("fixture coverage is incomplete")
    return 1116


def self_test():
    verify()
    controls = [
        lambda root: (root/"pg16.json").unlink(),
        lambda root: (root/"pg18.json").write_text("{}"),
        lambda root: (root/"extra.json").write_text("{}"),
    ]
    for mutation in controls:
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-decoder-inventory-") as temporary:
            root = Path(temporary)/"fixtures";shutil.copytree(ROOT,root)
            mutation(root)
            try:
                verify(root)
            except (ValueError,OSError):
                continue
            raise RuntimeError("inventory mutation was accepted")
    for key in ("packets", "source_types"):
        with tempfile.TemporaryDirectory(prefix="rpgmigrate-decoder-inventory-") as temporary:
            root = Path(temporary)/"fixtures";shutil.copytree(ROOT,root)
            data = read_json(root/"pg16.json");data[key] = []
            (root/"pg16.json").write_text(json.dumps(data));write_manifest(root)
            try:
                verify(root)
            except ValueError:
                continue
            raise RuntimeError("empty coverage was accepted after rehash")
    with tempfile.TemporaryDirectory(prefix="rpgmigrate-decoder-inventory-") as temporary:
        root = Path(temporary)/"fixtures";shutil.copytree(ROOT,root)
        manifest = read_json(root/"manifest.json");manifest["files"].append(copy.deepcopy(manifest["files"][0]))
        (root/"manifest.json").write_text(json.dumps(manifest))
        try:
            verify(root)
        except ValueError:
            return 6
        raise RuntimeError("duplicate inventory entry was accepted")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--verify", action="store_true")
    modes.add_argument("--self-test", action="store_true")
    arguments = parser.parse_args()
    if arguments.verify:
        print(json.dumps({"packets_verified": verify()}))
    elif arguments.self_test:
        print(json.dumps({"inventory_refusals": self_test()}))
    else:
        for major in (16,18):
            capture(major)
        write_manifest()
        verify()
