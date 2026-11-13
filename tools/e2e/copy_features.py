"""Candidate M8 profiles with SQL and wire observations independent of the engine."""

import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path
import re

import clone
from follow import NATIVE_FORMAT, TARGET_METADATA
from follow_wire import WireGate

FIXTURES = clone.FIXTURES
CHUNK_BYTES = 65536
MAX_OBJECTS = 16
MAX_LOGICAL_BYTES = 16777216
DEFAULT_THRESHOLD = 536870912
MAX_HEAP_BYTES = 671088640
CASES = ("binary_safe", "binary_fallback", "split_default", "large_objects", "lo_only", "mixed")


def valid_oid(value):
    return type(value) is int and 0 < value < 2**32


def acl_key(grant):
    return (grant["grantor"], grant["grantee"] is not None, grant["grantee"] or "", grant["privilege"], grant["grantable"])


def decode_large_objects(raw, *, max_objects=MAX_OBJECTS, max_logical_bytes=MAX_LOGICAL_BYTES):
    if len(raw) > max_logical_bytes * 2 + max_objects * 65536:
        raise RuntimeError("independent LO oracle output exceeded its bound")
    try:
        lines = [json.loads(line, object_pairs_hook=clone.unique_object) for line in raw.splitlines()]
        inventory = lines.pop(0)
        if set(inventory) != {"kind", "objects"} or inventory["kind"] != "inventory" or not isinstance(inventory["objects"], list) or len(inventory["objects"]) > max_objects:
            raise ValueError
        result, total, previous = {}, 0, 0
        for row in inventory["objects"]:
            if (set(row) != {"oid", "length", "owner", "acl_default", "acl", "comment"}
                    or not valid_oid(row["oid"]) or row["oid"] <= previous or type(row["length"]) is not int or row["length"] < 0
                    or not isinstance(row["owner"], str) or not row["owner"] or type(row["acl_default"]) is not bool
                    or not isinstance(row["acl"], list) or row["comment"] is not None and not isinstance(row["comment"], str)):
                raise ValueError
            seen = set()
            for grant in row["acl"]:
                if (set(grant) != {"grantor", "grantee", "privilege", "grantable"} or not isinstance(grant["grantor"], str) or not grant["grantor"]
                        or grant["grantee"] is not None and (not isinstance(grant["grantee"], str) or not grant["grantee"])
                        or grant["privilege"] not in ("SELECT", "UPDATE") or type(grant["grantable"]) is not bool or acl_key(grant) in seen):
                    raise ValueError
                seen.add(acl_key(grant))
            if row["acl_default"] and len(seen) != 2:
                raise ValueError
            total += row["length"]
            if total > max_logical_bytes:
                raise ValueError
            previous = row["oid"]
            result[previous] = {**row, "acl": sorted(row["acl"], key=acl_key)}
        for oid, row in result.items():
            digest = hashlib.sha256()
            offset = 0
            while offset < row["length"]:
                chunk = lines.pop(0)
                if (set(chunk) != {"kind", "oid", "offset", "hex"} or chunk["kind"] != "chunk"
                        or chunk["oid"] != oid or type(chunk["offset"]) is not int or chunk["offset"] != offset
                        or not isinstance(chunk["hex"], str) or not re.fullmatch(r"(?:[0-9a-f]{2})+", chunk["hex"])):
                    raise ValueError
                data = bytes.fromhex(chunk["hex"])
                if len(data) != min(CHUNK_BYTES, row["length"] - offset):
                    raise ValueError
                digest.update(data)
                offset += len(data)
            eof = lines.pop(0)
            if eof != {"kind": "eof", "oid": oid, "offset": offset, "hex": ""}:
                raise ValueError
            row["sha256"] = digest.hexdigest()
        if lines:
            raise ValueError
        return result
    except (KeyError, TypeError, ValueError, IndexError, json.JSONDecodeError):
        raise RuntimeError("independent LO inventory, content, or EOF observation is missing or invalid") from None


def large_object_fingerprint(database, name):
    lengths = "SELECT m.*,COALESCE((SELECT max(p.pageno::bigint*(current_setting('block_size')::bigint/4)+octet_length(p.data)) FROM pg_catalog.pg_largeobject p WHERE p.loid=m.oid),0) AS length FROM pg_catalog.pg_largeobject_metadata m"
    inventory = "SELECT m.oid::bigint AS oid,m.length,pg_catalog.pg_get_userbyid(m.lomowner) AS owner,m.lomacl IS NULL AS acl_default,COALESCE((SELECT json_agg(a ORDER BY a.grantor,a.grantee,a.privilege,a.grantable) FROM (SELECT pg_catalog.pg_get_userbyid(x.grantor) AS grantor,CASE WHEN x.grantee=0 THEN NULL ELSE pg_catalog.pg_get_userbyid(x.grantee) END AS grantee,x.privilege_type AS privilege,x.is_grantable AS grantable FROM pg_catalog.aclexplode(COALESCE(m.lomacl,pg_catalog.acldefault('L',m.lomowner))) x) a),'[]'::json) AS acl,pg_catalog.obj_description(m.oid,'pg_largeobject') AS comment FROM measured m ORDER BY m.oid"
    sql = ("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; "
           f"DO $$ BEGIN IF (SELECT count(*)>{MAX_OBJECTS} OR COALESCE(sum(length),0)>{MAX_LOGICAL_BYTES} FROM ({lengths}) b) THEN RAISE EXCEPTION 'LO oracle bound exceeded'; END IF; END $$; "
           f"WITH measured AS ({lengths}) SELECT json_build_object('kind','inventory','objects',COALESCE(json_agg(i ORDER BY i.oid),'[]'::json))::jsonb FROM ({inventory}) i; "
           f"WITH measured AS ({lengths}) SELECT json_build_object('kind',r.kind,'oid',m.oid::bigint,'offset',r.position,'hex',encode(pg_catalog.lo_get(m.oid,r.position,r.amount),'hex')) FROM measured m CROSS JOIN LATERAL (SELECT 'chunk' AS kind,g AS position,least({CHUNK_BYTES}::bigint,m.length-g)::int AS amount FROM generate_series(0::bigint,m.length-1,{CHUNK_BYTES}::bigint) g UNION ALL SELECT 'eof',m.length,1) r ORDER BY m.oid,r.position; COMMIT;")
    return decode_large_objects(database.sql(name, sql))


def expected_large_objects(source, *, target_owner, no_owner=False, no_acl=False, no_comments=False):
    result = {}
    for oid, row in source.items():
        owner = target_owner if no_owner else row["owner"]
        selected = {"oid": oid, "length": row["length"], "sha256": row["sha256"], "owner": owner}
        if not no_acl:
            defaults = [{"grantor": owner, "grantee": owner, "privilege": privilege, "grantable": False} for privilege in ("SELECT", "UPDATE")]
            grants = list(defaults)
            if not row["acl_default"]:
                source_defaults = [{"grantor": row["owner"], "grantee": row["owner"], "privilege": privilege, "grantable": False} for privilege in ("SELECT", "UPDATE")]
                full_default = all(grant in row["acl"] for grant in source_defaults)
                if not full_default:
                    grants = [grant for grant in grants if grant["grantee"] != row["owner"]]
                for grant in row["acl"]:
                    if full_default and grant in source_defaults:
                        continue
                    restored = {**grant, "grantor": owner if grant["grantor"] == row["owner"] else grant["grantor"]}
                    if restored not in grants:
                        grants.append(restored)
            selected["acl"] = sorted(grants, key=acl_key)
        if not no_comments:
            selected["comment"] = row["comment"]
        result[oid] = selected
    return result


def selected_large_objects(actual, *, no_acl=False, no_comments=False):
    omitted = {"acl_default"} | ({"acl"} if no_acl else set()) | ({"comment"} if no_comments else set())
    return {oid: {key: value for key, value in row.items() if key not in omitted} for oid, row in actual.items()}


def require_copy_branch(evidence, relation, format, *, split=False, simultaneous=()):
    records = [row for row in evidence.get("records", []) if row.get("relation") == relation and row.get("direction") == "out"]
    if evidence.get("invalid") is not False or evidence.get("overflow") is not False or not records:
        raise RuntimeError("actual COPY format observation is missing or invalid")
    if any(row.get("format") != format or row.get("complete") is not True or row.get("eof") is not True
           or not valid_oid(row.get("backend_pid")) or type(row.get("rows")) is not int for row in records):
        raise RuntimeError("COPY response format, identity or completion differs")
    if not split:
        if len(records) != 1 or records[0].get("range") != [None, None]:
            raise RuntimeError("unsplit COPY observation differs")
        return records
    records = sorted(records, key=lambda row: row["range"][0] if type(row.get("range", [None])[0]) is int else -1)
    if len(records) < 2 or records[0]["range"][0] != 0 or records[-1]["range"][1] is not None:
        raise RuntimeError("split COPY omitted a boundary part")
    for index, row in enumerate(records):
        start, end = row["range"]
        if type(start) is not int or start < 0 or end is not None and (type(end) is not int or end <= start):
            raise RuntimeError("split COPY bounds are invalid")
        if index and records[index - 1]["range"][1] != start:
            raise RuntimeError("split COPY omitted or overlapped a range")
    pids = {session.get("pid") for session in simultaneous if valid_oid(session.get("pid")) and session.get("backend_start") and valid_oid(session.get("database_oid"))}
    held = {row["backend_pid"] for row in records if row.get("held") is True and row.get("released") is True}
    if len(pids & held) < 2:
        raise RuntimeError("simultaneous held COPY workers were not independently observed")
    return records


def validate_artifact_binding(binding):
    keys = {"binary_sha256", "source_tree_sha256", "source_commit", "source_git_tree", "runner_image_id", "server_image_ids", "servers", "run"}
    if (not isinstance(binding, dict) or set(binding) != keys
            or any(not re.fullmatch(r"[a-f0-9]{64}", binding.get(key, "")) for key in ("binary_sha256", "source_tree_sha256"))
            or any(not re.fullmatch(r"[a-f0-9]{40}", binding.get(key, "")) for key in ("source_commit", "source_git_tree"))
            or not re.fullmatch(r"[a-f0-9]{32}", binding.get("run", ""))
            or not re.fullmatch(r"sha256:[a-f0-9]{64}", binding.get("runner_image_id", ""))
            or not isinstance(binding.get("server_image_ids"), list) or len(binding["server_image_ids"]) != 2
            or any(not re.fullmatch(r"sha256:[a-f0-9]{64}", value) for value in binding["server_image_ids"])
            or not isinstance(binding.get("servers"), list) or len(binding["servers"]) != 2):
        raise RuntimeError("candidate artifact identity is absent or malformed")
    seen_ids, seen_systems = set(), {"7705796426308472853", "7705919902701522967"}
    for server, role, image in zip(binding["servers"], ("source", "target"), binding["server_image_ids"]):
        if (not isinstance(server, dict) or set(server) != {"id", "image", "role", "system_identifier", "version", "data_directory", "wal_level", "started_at", "pgdata_permissions"}
                or server.get("role") != role or server.get("image") != image
                or not re.fullmatch(r"[a-f0-9]{64}", server.get("id", "")) or server["id"] in seen_ids
                or not re.fullmatch(r"[0-9]{19,20}", server.get("system_identifier", "")) or server["system_identifier"] in seen_systems
                or server.get("version") not in ("16.15", "18.6") or server.get("wal_level") != "logical"
                or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.+\-]+Z?", server.get("started_at", ""))
                or server.get("data_directory") != {"16.15": "/var/lib/postgresql/data", "18.6": "/var/lib/postgresql/18/docker"}.get(server.get("version"))
                or server.get("pgdata_permissions") != "700|postgres"):
            raise RuntimeError("candidate fresh server identity or startup evidence differs")
        seen_ids.add(server["id"])
        seen_systems.add(server["system_identifier"])


def validate_build_smoke(smoke, binding, pair):
    expected_client = {"16": "16.15", "18": "18.6"}[pair.split("-")[1]]
    discovery = smoke.get("version_json", {})
    if (set(smoke) != {"binary_sha256", "source_tree_sha256", "runner_image_id", "architecture", "client_versions", "linked_libpq", "library_path", "library_sha256", "packages", "version_json", "version_text"}
            or [server["version"] for server in binding["servers"]] != [{"16": "16.15", "18": "18.6"}[major] for major in pair.split("-")]
            or any(smoke.get(key) != binding[key] for key in ("binary_sha256", "source_tree_sha256", "runner_image_id"))
            or smoke.get("architecture") not in ("arm64", "amd64") or smoke.get("linked_libpq") != 180006
            or smoke.get("client_versions") != {tool: expected_client for tool in ("psql", "pg_dump", "pg_restore")}
            or not isinstance(smoke.get("library_path"), str) or not smoke["library_path"].startswith("/usr/lib/")
            or not re.fullmatch(r"[a-f0-9]{64}", smoke.get("library_sha256", ""))
            or set(discovery) != {"protocol_version", "engine_version", "capabilities"} or type(discovery.get("protocol_version")) is not int or discovery["protocol_version"] != 1
            or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.\-]+)?", discovery.get("engine_version", ""))
            or discovery.get("capabilities") != ["clone.single_db", "cdc.pgoutput", "progress.v1", "cutover.v1", "verify.v1", "cleanup.v1", "verify.sealed_files.v1"]
            or smoke.get("version_text") != "rpgmigrate " + discovery["engine_version"] + "\nlibpq 180006\n"):
        raise RuntimeError("candidate source-bound debug version, tools, or library smoke differs")
    packages = smoke.get("packages", {})
    if set(packages) != {"libpq5", "postgresql-client-16", "postgresql-client-18"}:
        raise RuntimeError("candidate measured client package inventory is absent")
    for name, version in (("libpq5", "18.6"), ("postgresql-client-16", "16.15"), ("postgresql-client-18", "18.6")):
        if (set(packages[name]) != {"version", "architecture"} or packages[name].get("version", "").split("-")[0] != version or packages[name].get("architecture") != smoke["architecture"]):
            raise RuntimeError("candidate measured client package minor or architecture differs")


def validate_feature_receipt(report, binding, required_cases=CASES):
    if not isinstance(binding, dict) or not required_cases or len(set(required_cases)) != len(required_cases) or set(report.get("cases", {})) != set(required_cases) or report.get("binding") != binding:
        raise RuntimeError("candidate feature receipt inventory or artifact binding differs")
    validate_artifact_binding(binding)
    for case in report["cases"].values():
        for stage in ("initial", "final"):
            value = case.get(stage, {})
            if value.get("lo_verified") is not True or "source" not in value or "target" not in value or value["source"] != value["target"]:
                raise RuntimeError("mandatory independent LO observation is missing or differs")
            validate_selected_inventory(value["source"])
            validate_selected_inventory(value["target"])
        if "inventories" not in case or set(case["inventories"]) != {"source", "target"} or any(not isinstance(value, list) for value in case["inventories"].values()):
            raise RuntimeError("independent physical inventories are missing")


def validate_physical_inventory(rows, record):
    if not isinstance(rows, list) or len(rows) > 256 or any(not isinstance(row, dict) or not valid_oid(row.get("oid")) for row in rows) or len({row["oid"] for row in rows}) != len(rows):
        raise RuntimeError("physical catalog inventory is missing, duplicated or exceeds its bound")
    for row in rows:
        if (set(row) != {"oid", "schema_oid", "owner_oid", "persistence", "kind", "schema", "name", "index_parent", "primary"}
                or not valid_oid(row["schema_oid"]) or not valid_oid(row["owner_oid"]) or row["persistence"] not in ("p", "u", "t")
                or row["kind"] not in ("r", "p", "f", "m", "i") or any(not isinstance(row[key], str) or not row[key] or len(row[key].encode()) > 63 for key in ("schema", "name"))
                or row["kind"] == "i" and (not valid_oid(row["index_parent"]) or type(row["primary"]) is not bool)
                or row["kind"] != "i" and (row["index_parent"] is not None or row["primary"] is not None)):
            raise RuntimeError("physical catalog row is missing or malformed")
    excluded = set()
    if record is not None:
        try:
            binding = record["intent"]["binding"]
            identities = [record[key] for key in ("table_oid", "marker_table_oid", "marker_index_oid", "large_object_table_oid", "large_object_index_oid")]
            if binding["metadata_version"] != TARGET_METADATA or any(not valid_oid(value) for value in identities) or len(set(identities)) != 5:
                raise ValueError
            primary = [row for row in rows if row.get("index_parent") == record["table_oid"] and row.get("primary") is True]
            if len(primary) != 1 or primary[0]["oid"] in identities:
                raise ValueError
            expected = [(record["table_oid"], "r", binding["table_name"], None), (primary[0]["oid"], "i", binding["table_name"] + "_pkey", record["table_oid"]),
                        (record["marker_table_oid"], "r", "copy_done", None), (record["marker_index_oid"], "i", "copy_done_pkey", record["marker_table_oid"]),
                        (record["large_object_table_oid"], "r", "large_object_done", None), (record["large_object_index_oid"], "i", "large_object_done_pkey", record["large_object_table_oid"])]
            for oid, kind, name, parent in expected:
                actual = [row for row in rows if row["oid"] == oid]
                if len(actual) != 1:
                    raise ValueError
                row = actual[0]
                if (row.get("schema_oid") != record["schema_oid"] or row.get("owner_oid") != binding["owner_oid"]
                        or row.get("schema") != binding["schema_name"] or row.get("name") != name or row.get("kind") != kind
                        or row.get("persistence") != "p" or row.get("index_parent") != parent or kind == "i" and row.get("primary") is not True):
                    raise ValueError
                if kind == "r":
                    excluded.add(oid)
        except (KeyError, TypeError, ValueError):
            raise RuntimeError("exact owned metadata heap/index identities differ") from None
    return [row for row in rows if row.get("kind") in ("r", "p", "f", "m") and row["oid"] not in excluded]


def physical_inventory(database, name, *, state=None):
    record = None
    if state is not None:
        native = json.loads((state / "manifest.json").read_bytes(), object_pairs_hook=clone.unique_object)
        provenance = native.get("target_provenance", [])
        if native.get("format_version") != NATIVE_FORMAT or len(provenance) != 1 or provenance[0].get("phase") != "confirmed":
            raise RuntimeError("candidate inventory requires one confirmed native provenance")
        record = provenance[0]["record"]
    query = "SELECT COALESCE(json_agg(r ORDER BY r.oid),'[]'::json) FROM (SELECT c.oid::bigint AS oid,c.relnamespace::bigint AS schema_oid,c.relowner::bigint AS owner_oid,c.relpersistence AS persistence,c.relkind AS kind,n.nspname AS schema,c.relname AS name,i.indrelid::bigint AS index_parent,i.indisprimary AS primary FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_catalog.pg_index i ON i.indexrelid=c.oid WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND c.relkind IN ('r','p','f','m','i') ORDER BY c.oid LIMIT 257) r;"
    return validate_physical_inventory(json.loads(database.sql(name, query), object_pairs_hook=clone.unique_object), record)


def validate_native_comparison(output, inventories, *, lo_verified, native=True):
    if (not isinstance(output, list) or not isinstance(inventories, dict) or set(inventories) != {"source", "target"}
            or any(not isinstance(value, list) for value in inventories.values()) or lo_verified is not True):
        raise RuntimeError("native comparison requires independent inventories and mandatory LO proof")
    if not output and (not native or any(inventories.values())):
        raise RuntimeError("empty comparison lacks independently proven zero physical inventories")
    if any(not isinstance(row, dict) or row.get("status") != "match" for row in output):
        raise RuntimeError("native comparison differs or omitted a status")
    if len(output) != len(inventories["source"]) or len(output) != len(inventories["target"]):
        raise RuntimeError("native comparison omitted physical table coverage")


def validate_manifest(manifest):
    expected = {"version": 1, "native_format": NATIVE_FORMAT, "metadata_version": TARGET_METADATA, "cases": list(CASES), "max_objects": MAX_OBJECTS,
                "max_logical_bytes": MAX_LOGICAL_BYTES, "split_threshold": DEFAULT_THRESHOLD, "max_heap_bytes": MAX_HEAP_BYTES,
                "split_rows": 286720, "max_rows": 400000, "max_bytes": 33554432}
    fixtures = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(FIXTURES.glob("m8-*.sql"))}
    if len(fixtures) != 5 or manifest != {**expected, "fixtures": fixtures}:
        raise RuntimeError("M8 fixture inventory, bounds or reviewed digest differs")
    return manifest


def load_manifest():
    return validate_manifest(json.loads((FIXTURES / "m8-cases.json").read_bytes(), object_pairs_hook=clone.unique_object))


def table_fingerprint(database, name, inventory, manifest):
    result = {}
    for table in inventory:
        relation = clone.quote(table["schema"]) + "." + clone.quote(table["name"])
        rows = database.sql(name, f"SELECT count(*) FROM ONLY {relation};").strip()
        if not rows.isdigit() or int(rows) > manifest["max_rows"]:
            raise RuntimeError("M8 row inventory exceeds its bound")
        if table["name"] == "m8_split":
            # PostgreSQL's SHA-256 keeps the independently read row oracle bounded for the large heap.
            query = f"COPY (SELECT id,encode(pg_catalog.sha256(data),'hex') FROM ONLY {relation}) TO STDOUT;"
        else:
            query = f"COPY (SELECT encode(convert_to(row_to_json(t)::text,'UTF8'),'hex') FROM ONLY {relation} t) TO STDOUT;"
        result[(table["schema"] + "." + table["name"])] = clone.row_fingerprint(database.sql(name, query).splitlines(keepends=True), manifest["max_rows"], manifest["max_bytes"])
        if result[table["schema"] + "." + table["name"]]["rows"] != int(rows):
            raise RuntimeError("M8 row oracle omitted physical rows")
    return result


def observe_large_objects(source, target, srcname, dstname, policy, *, has_lo=False, final=False):
    observed = large_object_fingerprint(source, srcname)
    if has_lo:
        validate_lo_fixture(observed, final=final)
    elif observed:
        raise RuntimeError("no-LO fixture unexpectedly contains large objects")
    expected = expected_large_objects(observed, target_owner="postgres", **policy)
    actual = selected_large_objects(large_object_fingerprint(target, dstname), no_acl=policy.get("no_acl", False), no_comments=policy.get("no_comments", False))
    if expected != actual:
        raise RuntimeError("independent LO logical contents or selected metadata differ")
    return {"source": expected, "target": actual, "lo_verified": True}


def simultaneous_workers(database, name, gate, relation):
    records = [row for row in gate.copy_evidence()["records"] if row["relation"] == relation and row["held"] and not row["released"]]
    pids = {row["backend_pid"] for row in records}
    if len(pids) < 2:
        return None
    rows = database._observe(lambda: json.loads(database.sql(name, "SELECT COALESCE(json_agg(s ORDER BY s.pid),'[]'::json) FROM (SELECT pid,backend_start::text,datid::bigint AS database_oid FROM pg_catalog.pg_stat_activity WHERE datname=current_database() AND pid IN (" + ",".join(map(str, sorted(pids))) + ")) s;")))
    if not isinstance(rows, list):
        return None
    if {row["pid"] for row in rows} != pids or any(not row.get("backend_start") or not valid_oid(row.get("database_oid")) for row in rows):
        raise RuntimeError("held COPY sessions lack independent live backend identity")
    return rows


def native_compare(driver, source, target, srcname, dstname, state, lo_verified):
    inventories = {"source": physical_inventory(source, srcname), "target": physical_inventory(target, dstname, state=state)}
    environment, _ = driver.prepare(state, "compare data")
    result = clone.run([str(driver.binary), "compare", "data", "--source", source.connection(srcname), "--target", target.connection(dstname), "--dir", str(state), "--json"], environment=environment)
    output = json.loads(result, object_pairs_hook=clone.unique_object)
    validate_native_comparison(output, inventories, lo_verified=lo_verified)
    after = {"source": physical_inventory(source, srcname), "target": physical_inventory(target, dstname, state=state)}
    require_stable_inventories(inventories, after)
    return inventories, {"before": inventories, "after": after}


def progress_unavailable():
    try:
        print('m8-progress: {"status":"unavailable","reason":"capture_or_bound_failure"}', file=sys.stderr)
    except Exception:
        pass


class ProfileProgress:
    LIMIT = 262144
    STAGES = {"initializing", "fixture", "clone_start", "clone_running", "startup_observed", "initial_oracles", "drain",
              "final_oracles", "wire_validation", "lo_mismatch", "wire_cleanup", "case_complete", "receipt_validation", "completed"}

    def __init__(self, path, binding, pair):
        validate_artifact_binding(binding)
        if pair not in clone.PAIRS or path.exists() or path.is_symlink() or path.parent.is_symlink() or not path.parent.is_dir():
            raise RuntimeError("fresh M8 progress destination or pair is unavailable")
        self.path, self.binding, self.pair = path, binding, pair
        self.latest, self.failure_attempted = None, False

    @staticmethod
    def uint(value):
        return type(value) is int and 0 <= value <= 2**64 - 1

    @classmethod
    def wire(cls, gate):
        try:
            copy = gate.copy_evidence(timeout=.02)
            wire = gate.snapshot(timeout=.02)
            if wire.get("status") != "observed":
                raise ValueError
            records, connections = copy["records"], wire["connections"]
            if len(records) > 256 or len(connections) > 64 or type(copy["invalid"]) is not bool or type(copy["overflow"]) is not bool or type(wire["overflow"]) is not bool:
                raise ValueError
            selected = []
            for row in records:
                keys = ("relation", "direction", "range", "format", "connection", "backend_pid", "columns", "rows", "data_frames", "eof", "complete", "held", "released")
                if (row["relation"] not in (["public", "m8_safe"], ["public", "m8_fallback"], ["public", "m8_split"])
                        or row["direction"] not in ("in", "out") or row["format"] not in ("text", "binary")
                        or not isinstance(row["range"], list) or len(row["range"]) != 2 or any(value is not None and not cls.uint(value) for value in row["range"])
                        or any(not cls.uint(row[key]) for key in ("connection", "backend_pid", "columns", "data_frames"))
                        or not 0 < row["connection"] <= 64 or not 0 < row["backend_pid"] < 2**31 or row["columns"] > 1600
                        or row["rows"] is not None and not cls.uint(row["rows"])
                        or any(type(row[key]) is not bool for key in ("eof", "complete", "held", "released"))):
                    raise ValueError
                selected.append({key: row[key] for key in keys})
            safe_connections = []
            for row in connections:
                if (not cls.uint(row["connection"]) or not 0 < row["connection"] <= 64
                        or row["backend_pid"] is not None and (not cls.uint(row["backend_pid"]) or not 0 < row["backend_pid"] < 2**31)
                        or row["mode"] not in ("ordinary", "copy_in", "copy_out", "logical", "unknown")
                        or any(type(row[key]) is not bool for key in ("unknown", "overflow"))):
                    raise ValueError
                directions, counts = {}, {}
                for side in ("client", "server"):
                    state = row["directions"][side]
                    if (state["stage"] not in ("startup_wait", "connect_wait", "frame_wait", "received", "forwarding", "forwarded", "gate_wait", "closed")
                            or state["closure"] not in ("open", "eof", "socket_error", "protocol_error", "proxy_shutdown", "connect_error") or not cls.uint(state["last_age_ms"])):
                        raise ValueError
                    directions[side] = {key: state[key] for key in ("stage", "closure", "last_age_ms")}
                    counts[side] = {}
                    for event in ("received", "forwarded"):
                        values = row["counts"][side][event]
                        if any((key not in ("startup", "other") and (len(key) != 1 or key not in "QPBDECSpdcfXRAKZN123tTGHWVsnI")) or not cls.uint(value) for key, value in values.items()):
                            raise ValueError
                        counts[side][event] = dict(values)
                safe_connections.append({**{key: row[key] for key in ("connection", "backend_pid", "mode", "unknown", "overflow")}, "directions": directions, "counts": counts})
            gates = {key: wire["gates"][key] for key in ("armed", "held", "released", "abort_requested", "stopping")}
            if any(type(value) is not bool for value in gates.values()):
                raise ValueError
            return {"status": "observed", "copy": {"invalid": copy["invalid"], "overflow": copy["overflow"], "records": selected},
                    "wire": {"overflow": wire["overflow"], "gates": gates, "connections": safe_connections}}
        except Exception:
            return {"status": "unavailable", "reason": "observer_unavailable_or_invalid"}

    def document(self, case, stage, cases, driver, wires, failed):
        if case is not None and case not in CASES or stage not in self.STAGES or list(cases) != list(CASES[:len(cases)]) or len(cases) > 6 or len(driver.reports) > 9:
            raise ValueError
        reports = []
        for record in driver.reports:
            if set(record) != {"binding", "raw_json", "probe_checked"} or record["probe_checked"] is not False or not isinstance(record["raw_json"], str) or len(record["raw_json"].encode()) > 65536:
                raise ValueError
            binding, raw = record["binding"], json.loads(record["raw_json"], object_pairs_hook=clone.unique_object)
            work = binding["work"]
            if (set(binding) != {"command", "work"} or set(work) != {"migration_uid", "run_id", "workdir_id", "checkpoint_generation"}
                    or not re.fullmatch(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", work["migration_uid"])
                    or work["run_id"] != work["migration_uid"].replace("-", "") or not re.fullmatch(r"m2-pvc-[a-f0-9]{32}", work["workdir_id"])
                    or type(work["checkpoint_generation"]) is not int or work["checkpoint_generation"] != 1 or raw.get("phase") not in ("streaming", "completed")
                    or not isinstance(raw.get("engine_version"), str) or len(raw["engine_version"]) > 64 or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.+-]+)?", raw["engine_version"])):
                raise ValueError
            clone.validate_operator_report(record["raw_json"], binding, "clone", raw["phase"] == "completed", phases=(raw["phase"],))
            owners = [path.name for path, value in driver.work.items() if value == work]
            if len(owners) != 1 or owners[0] not in CASES:
                raise ValueError
            reports.append({"case": owners[0], **record})
        if len(wires) not in (0, 2):
            raise ValueError
        observed = {role: self.wire(gate) for role, gate in zip(("source", "target"), wires)} if wires else None
        return {"version": 1, "scope": "m8_profile_diagnostic", "binding": self.binding, "pair": self.pair,
                "recorded_at_unix_ns": time.time_ns(), "status": "failed" if failed else "completed" if stage == "completed" else "running",
                "capture_status": "partial" if observed and any(value["status"] != "observed" for value in observed.values()) else "available",
                "current": {"case": case, "ordinal": CASES.index(case) + 1 if case is not None else None, "stage": stage},
                "completed_case_ids": list(cases), "producer_report_count": len(reports), "producer_reports": reports, "wire": observed}

    def publish(self, value):
        raw = json.dumps(value, sort_keys=True, allow_nan=False).encode() + b"\n"
        if len(raw) > self.LIMIT:
            raise ValueError("M8 progress output exceeded its bound")
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode="wb", dir=self.path.parent, delete=False) as stream:
                temporary = Path(stream.name)
                os.fchmod(stream.fileno(), 0o600)
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            descriptor = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def capture(self, case, stage, cases, driver, wires=(), *, failed=False):
        self.latest = (case, stage, cases, driver, wires)
        self.failure_attempted |= failed
        try:
            self.publish(self.document(case, stage, cases, driver, wires, failed))
            return True
        except Exception:
            value = {"version": 1, "scope": "m8_profile_diagnostic", "binding": self.binding, "pair": self.pair,
                     "capture_status": "unavailable", "reason": "capture_or_bound_failure", "status": "failed" if failed else "running",
                     "current": {"case": case if isinstance(case, str) and case in CASES else None, "ordinal": CASES.index(case) + 1 if isinstance(case, str) and case in CASES else None, "stage": stage if isinstance(stage, str) and stage in self.STAGES else "unavailable"},
                     "completed_case_ids": None, "producer_report_count": None, "producer_reports": None, "wire": None}
            try:
                self.publish(value)
            except Exception:
                pass
            progress_unavailable()
            if not failed:
                raise RuntimeError("M8 progress capture is unavailable") from None
            return False

    def failure(self):
        if self.latest is not None and not self.failure_attempted:
            self.capture(*self.latest, failed=True)


def database_identity(database, name):
    return json.loads(database.sql(name, "SELECT json_build_object('name',current_database(),'oid',(SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()),'system_identifier',(SELECT system_identifier::text FROM pg_catalog.pg_control_system()));"), object_pairs_hook=clone.unique_object)


def validate_database_identities(cases, binding):
    if set(cases) != set(CASES):
        raise RuntimeError("fresh database case inventory is absent")
    for role, prefix, server in zip(("source", "target"), ("src_m8_", "dst_m8_"), binding["servers"]):
        seen = set()
        for name, case in cases.items():
            databases = case.get("databases", {})
            if set(databases) != {"source", "target"}:
                raise RuntimeError("fresh database endpoint inventory is absent")
            actual = databases[role]
            if (set(actual) != {"name", "oid", "system_identifier"} or actual["name"] != prefix + name
                    or not valid_oid(actual["oid"]) or actual["oid"] in seen or actual["system_identifier"] != server["system_identifier"]):
                raise RuntimeError("fresh database identity changed or differs from its exact owned server")
            seen.add(actual["oid"])


def run_profiles(source, target, driver, root, manifest, *, progress=None):
    import cutover
    cases = {}
    for database in (source, target):
        database.sql("postgres", "CREATE ROLE m8_source_owner; CREATE ROLE m8_reader;")
    for case in CASES:
        if progress is not None:
            progress.capture(case, "fixture", cases, driver)
        srcname, dstname = "src_m8_" + case, "dst_m8_" + case
        src, dst = source.create(srcname), target.create(dstname)
        databases = {"source": database_identity(source, srcname), "target": database_identity(target, dstname)}
        state = root / case
        has_lo = case in ("large_objects", "lo_only", "mixed")
        policy = {"no_owner": case != "large_objects", "no_acl": case == "mixed", "no_comments": case == "mixed"}
        flags = ["--use-copy-binary", "--split-tables-larger-than", str(DEFAULT_THRESHOLD), "--split-max-parts", "8", "--large-objects-jobs", "4"]
        flags += ["--" + key.replace("_", "-") for key, value in policy.items() if value]
        if case != "lo_only":
            source.sql(srcname, (FIXTURES / "m8-safe.sql").read_text())
        if case in ("binary_fallback", "mixed"):
            source.sql(srcname, (FIXTURES / "m8-fallback.sql").read_text())
        if case == "split_default":
            source.sql(srcname, (FIXTURES / "m8-split.sql").read_text())
            heap = int(source.sql(srcname, "SELECT pg_relation_size('public.m8_split');").strip())
            if not DEFAULT_THRESHOLD <= heap <= MAX_HEAP_BYTES:
                raise RuntimeError("default-threshold fixture physical heap is outside its admitted budget")
        if has_lo:
            source.sql(srcname, (FIXTURES / "m8-large-objects.sql").read_text())
        inventory = physical_inventory(source, srcname)
        expected_tables = table_fingerprint(source, srcname, inventory, manifest)
        with WireGate(source.host, "observe") as wire, WireGate(target.host, "observe") as target_wire:
            if case == "split_default":
                wire.hold_copy(["public", "m8_split"])
            worker, stage, wires = None, "clone_start", (wire, target_wire)
            try:
                if progress is not None:
                    progress.capture(case, stage, cases, driver, wires)
                worker = driver.start(wire.connection(src), target_wire.connection(dst), state,
                                      *(["--follow", "--plugin", "pgoutput"] if has_lo else []), *flags, skip_large_objects=False)
                stage = "clone_running"
                if progress is not None:
                    progress.capture(case, stage, cases, driver, wires)
                sessions = ()
                if case == "split_default":
                    sessions = clone.wait_for(lambda: simultaneous_workers(source, srcname, wire, ["public", "m8_split"]), "simultaneous actual default-threshold COPY workers", timeout=45)
                    wire.copy_release.set()
                if has_lo:
                    ready_follow(driver, worker)
                else:
                    driver.finish(worker)
                stage = "startup_observed"
                if progress is not None:
                    progress.capture(case, stage, cases, driver, wires)
                initial = observe_large_objects(source, target, srcname, dstname, policy, has_lo=has_lo)
                target_inventory = physical_inventory(target, dstname, state=state)
                require_table_fingerprints(expected_tables, table_fingerprint(target, dstname, target_inventory, manifest), inventory, target_inventory)
                stage = "initial_oracles"
                if progress is not None:
                    progress.capture(case, stage, cases, driver, wires)
                if has_lo:
                    stage = "drain"
                    if progress is not None:
                        progress.capture(case, stage, cases, driver, wires)
                    source.sql(srcname, (FIXTURES / "m8-large-objects-final.sql").read_text())
                    end = cutover.frozen_end(source, srcname)
                    result, _ = cutover.stream(driver, state, ("stream", "sentinel", "set", "endpos"), end, source=src, target=dst)
                    if result.returncode:
                        cutover.require_fence_refusal(result)
                    cutover.persisted_endpos(driver, state, end)
                    cutover.finish_completed(driver, worker)
                    completed_row = target.authority(dstname, state, driver.work[state])
                    if completed_row is None or completed_row["follow_completed"] is not True or completed_row["drain_end_lsn"] != end:
                        raise RuntimeError("candidate target did not durably complete the exact final LO boundary")
                stage = "final_oracles"
                if progress is not None:
                    progress.capture(case, stage, cases, driver, wires)
                final = observe_large_objects(source, target, srcname, dstname, policy, has_lo=has_lo, final=has_lo)
                inventories, boundary = native_compare(driver, source, target, srcname, dstname, state, final["lo_verified"])
                require_table_fingerprints(expected_tables, table_fingerprint(target, dstname, inventories["target"], manifest), inventory, inventories["target"])
                stage = "wire_validation"
                if progress is not None:
                    progress.capture(case, stage, cases, driver, wires)
                evidence, target_evidence = wire.copy_evidence(), target_wire.copy_evidence()
                branches = {}
                for relation in inventory:
                    name = relation["name"]
                    branches[name] = require_copy_branch(evidence, [relation["schema"], name], "text" if name == "m8_fallback" else "binary", split=name == "m8_split", simultaneous=sessions)
                if target_evidence["invalid"] or target_evidence["overflow"]:
                    raise RuntimeError("target COPY wire observations are invalid or incomplete")
                outgoing = sorted((tuple(row["relation"]), row["format"], row["columns"], row["rows"]) for row in evidence["records"])
                incoming = sorted((tuple(row["relation"]), row["format"], row["columns"], row["rows"]) for row in target_evidence["records"] if row["complete"] and row["eof"])
                if outgoing != incoming:
                    raise RuntimeError("source/target actual COPY response format or row counts differ")
                value = {"databases": databases, "initial": initial, "final": final, "inventories": inventories, "inventory_boundary": boundary, "tables": expected_tables, "copy": evidence, "target_copy": target_evidence,
                         "branches": branches, "simultaneous": list(sessions), "scope": "direct_cli_operator_flags"}
                if case == "split_default":
                    value.update(heap_bytes=heap, threshold=DEFAULT_THRESHOLD)
                if has_lo:
                    value["final_end_lsn"] = end
                    stage = "lo_mismatch"
                    if progress is not None:
                        progress.capture(case, stage, cases, driver, wires)
                    value["lo_mismatch_refusal"] = lo_mismatch_refusal(driver, source, target, srcname, dstname, state, policy, final, expected_tables, manifest, inventories["target"])
                if databases != {"source": database_identity(source, srcname), "target": database_identity(target, dstname)}:
                    raise RuntimeError("fresh profile database identity changed during the oracle boundary")
                stage = "wire_cleanup"
                if progress is not None:
                    progress.capture(case, stage, cases, driver, wires)
            except Exception:
                if progress is not None:
                    try:
                        progress.capture(case, stage, cases, driver, wires, failed=True)
                    except Exception:
                        progress_unavailable()
                raise
            finally:
                if worker is not None:
                    driver.kill(worker)
        cases[case] = value
        if progress is not None:
            progress.capture(case, "case_complete", cases, driver, wires)
    return cases


def ready_follow(driver, worker):
    def observed():
        if worker.poll() is not None:
            raise RuntimeError("candidate follow exited before positive startup")
        lines = worker.output.read_bytes().splitlines()
        if not lines:
            return None
        if len(lines) != 1 or not lines[0].startswith(b"native-report: "):
            raise RuntimeError("candidate follow startup framing differs")
        binding = json.loads(worker.binding.read_bytes())
        report = clone.validate_operator_report(lines[0][15:], binding, "clone", False, phases=("streaming",))
        driver.reports.append({"binding": binding, "raw_json": lines[0][15:].decode(), "probe_checked": False})
        return report
    return clone.wait_for(observed, "candidate follow positive streaming report", timeout=60)


def validate_receipt(report, manifest, *, binding):
    clone.validate_runtime(report)
    validate_feature_receipt(report, binding)
    validate_build_smoke(report.get("build_smoke", {}), binding, report["pair"])
    validate_database_identities(report["cases"], binding)
    if report.get("fixture_sha256") != hashlib.sha256((FIXTURES / "m8-cases.json").read_bytes()).hexdigest():
        raise RuntimeError("candidate feature fixture binding differs")
    validation = report.get("report_validation", {})
    records = validation.get("reports", [])
    if validation.get("mode") != "local_m8_direct_cli" or len(records) != 9:
        raise RuntimeError("candidate actual startup/completion report coverage is absent")
    phases = []
    for record in records:
        raw = json.loads(record.get("raw_json", "null"), object_pairs_hook=clone.unique_object)
        phase = raw.get("phase") if isinstance(raw, dict) else None
        if phase not in ("streaming", "completed") or record.get("probe_checked") is not False:
            raise RuntimeError("candidate direct CLI report claimed unsupported lifecycle admission")
        clone.validate_operator_report(record["raw_json"], record["binding"], "clone", phase == "completed", phases=(phase,))
        phases.append(phase)
    if phases.count("streaming") != 3 or phases.count("completed") != 6:
        raise RuntimeError("candidate direct CLI report phases differ")
    for name, case in report["cases"].items():
        if case.get("scope") != "direct_cli_operator_flags":
            raise RuntimeError("candidate direct CLI evidence scope is absent")
        inventories = case["inventories"]
        for rows in inventories.values():
            validate_physical_inventory(rows, None)
        boundary = case.get("inventory_boundary", {})
        require_stable_inventories(boundary.get("before"), boundary.get("after"))
        if boundary["before"] != inventories:
            raise RuntimeError("physical inventory receipt differs from its observed boundary")
        if name == "lo_only" and any(inventories.values()):
            raise RuntimeError("LO-only case has unproven zero physical inventory")
        for stage in ("initial", "final"):
            if name in ("large_objects", "lo_only", "mixed") and len(case[stage]["source"]) != 5:
                raise RuntimeError("positive LO case omitted expected OID coverage")
        if name in ("large_objects", "lo_only", "mixed"):
            if set(case["initial"]["source"]) == set(case["final"]["source"]) or not case.get("final_end_lsn"):
                raise RuntimeError("final LO re-sync membership change was not observed")
            validate_mismatch_refusal(case.get("lo_mismatch_refusal"))
        relations = {(row["schema"], row["name"]) for row in inventories["source"]}
        for schema, table in relations:
            require_copy_branch(case["copy"], [schema, table], "text" if table == "m8_fallback" else "binary", split=table == "m8_split", simultaneous=case["simultaneous"])
        if name == "split_default" and (case.get("threshold") != DEFAULT_THRESHOLD or type(case.get("heap_bytes")) is not int or not DEFAULT_THRESHOLD <= case["heap_bytes"] <= MAX_HEAP_BYTES):
            raise RuntimeError("operator-default split threshold was not measured")
    return report


def inner(args):
    import ctypes
    import cutover
    # The CLI script and imported clone module have separate globals.
    clone.DIAGNOSTIC_ROOT = Path("/diagnostics")
    manifest = load_manifest()
    if not args.feature_binding or args.report_probe is not None:
        raise RuntimeError("candidate profile requires independent artifact inputs without the old Go probe")
    binding = json.loads(Path(args.feature_binding).read_bytes(), object_pairs_hook=clone.unique_object)
    validate_artifact_binding(binding)
    binary = Path(args.binary)
    if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("candidate source-bound debug executable is absent or replaced")
    if hashlib.sha256(binary.read_bytes()).hexdigest() != binding.get("binary_sha256") or cutover.source_inventory(clone.ROOT)["sha256"] != binding.get("source_tree_sha256"):
        raise RuntimeError("candidate actual binary/source identity differs from admitted inputs")
    bindir = Path("/usr/lib/postgresql") / args.pair.split("-")[1] / "bin"
    clients = {name: clone.tool_version(bindir / name) for name in ("pg_dump", "pg_restore", "psql")}
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("PG", "RPGMIGRATE_"))}
    environment.update(PGPASSWORD=os.environ["PGPASSWORD"], PG_BINDIR=str(bindir), PATH=str(bindir) + os.pathsep + os.environ["PATH"], LC_ALL="C.UTF-8")
    import follow
    source, target = (follow.Database(host, bindir, environment) for host in ("source", "target"))
    linked = re.search(r"libpq\.so\.5 => (\S+)", clone.run(["ldd", str(binary)]).decode())
    if not linked:
        raise RuntimeError("actual candidate libpq linkage is unavailable")
    report = {"pair": args.pair, "server_versions": [database.sql("postgres", "SHOW server_version;").decode().split()[0] for database in (source, target)],
              "client_versions": clients, "linked_libpq": int(ctypes.CDLL(linked[1]).PQlibVersion()), "binding": binding,
              "fixture_sha256": hashlib.sha256((FIXTURES / "m8-cases.json").read_bytes()).hexdigest()}
    clone.validate_runtime(report)
    for database, expected in zip((source, target), binding["servers"]):
        actual = json.loads(database.sql("postgres", "SELECT json_build_object('system_identifier',(SELECT system_identifier::text FROM pg_catalog.pg_control_system()),'version',split_part(current_setting('server_version'),' ',1),'data_directory',current_setting('data_directory'),'wal_level',current_setting('wal_level'));"))
        if any(actual.get(key) != expected[key] for key in ("system_identifier", "version", "data_directory", "wal_level")):
            raise RuntimeError("candidate runner reached a different server identity")
    library = Path(linked[1]).resolve(strict=True)
    packages = {}
    package_output = clone.run(["dpkg-query", "-W", "-f=${Package}|${Version}|${Architecture}|${db:Status-Status}\n", "libpq5", "postgresql-client-16", "postgresql-client-18"])
    for row in package_output.decode().splitlines():
        fields = row.split("|")
        if len(fields) != 4 or fields[3] != "installed" or fields[0] in packages:
            raise RuntimeError("candidate measured client package output differs")
        packages[fields[0]] = {"version": fields[1], "architecture": fields[2]}
    smoke = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "source_tree_sha256": binding["source_tree_sha256"], "runner_image_id": binding["runner_image_id"],
             "architecture": {"aarch64": "arm64", "x86_64": "amd64"}.get(os.uname().machine), "client_versions": clients, "linked_libpq": report["linked_libpq"],
             "library_path": str(library), "library_sha256": hashlib.sha256(library.read_bytes()).hexdigest(), "packages": packages,
             "version_json": json.loads(clone.run([str(binary), "version", "--json"]), object_pairs_hook=clone.unique_object),
             "version_text": clone.run([str(binary), "version"]).decode()}
    validate_build_smoke(smoke, binding, args.pair)
    report["build_smoke"] = smoke
    with Path(args.receipt).with_name("m8-smoke.json").open("x") as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(smoke, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    with tempfile.TemporaryDirectory(prefix="rpgmigrate-m8-") as temporary:
        root = Path(temporary)
        driver = clone.Driver(binary, environment, root, mode="operator")
        progress = ProfileProgress(clone.DIAGNOSTIC_ROOT / "m8-progress.json", binding, args.pair)
        try:
            progress.capture(None, "initializing", {}, driver)
            report["cases"] = run_profiles(source, target, driver, root, manifest, progress=progress)
            report["report_validation"] = {"mode": "local_m8_direct_cli", "reports": driver.reports}
            progress.capture(None, "receipt_validation", report["cases"], driver)
            validate_receipt(report, manifest, binding=binding)
            Path(args.receipt).write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
            progress.capture(None, "completed", report["cases"], driver)
        except Exception:
            try:
                progress.failure()
            except Exception:
                progress_unavailable()
            raise
    print(json.dumps({"pair": args.pair, "cases": len(CASES), "scope": "direct_cli_operator_flags"}, sort_keys=True))


def require_table_fingerprints(source, target, source_inventory, target_inventory):
    if (not isinstance(source_inventory, list) or not isinstance(target_inventory, list)
            or not isinstance(source, dict) or not isinstance(target, dict) or source != target
            or set(source) != {row["schema"] + "." + row["name"] for row in source_inventory}
            or set(target) != {row["schema"] + "." + row["name"] for row in target_inventory}):
        raise RuntimeError("independent table fingerprints omitted or changed physical contents")


def validate_lo_fixture(source, *, final=False):
    payloads = {90001: b"", 90003: b"Z" * (17 if final else 2097159),
                90004: (b"\xff\0" + b"\0" * 2097157 if final else b"\0" * 2097159) + b"\0\xff", 90005: b"\1\2\3"}
    payloads[90006 if final else 90002] = bytes.fromhex("ff000102" if final else "00ff275c")
    if set(source) != set(payloads) or any(source[oid]["length"] != len(data) or source[oid]["sha256"] != hashlib.sha256(data).hexdigest() or source[oid]["owner"] != "m8_source_owner" for oid, data in payloads.items()):
        raise RuntimeError("source LO fixture omitted or changed exact OIDs, logical holes, length or contents")


def validate_mismatch_refusal(proof):
    required = {"returncode", "source_lo", "target_lo", "source_tables", "target_tables", "restored_source", "restored_target"}
    if (not isinstance(proof, dict) or set(proof) != required or type(proof["returncode"]) is not int or not 0 < proof["returncode"] < 256
            or not proof["source_lo"] or proof["source_lo"] == proof["target_lo"] or proof["source_tables"] != proof["target_tables"]
            or proof["restored_source"] != proof["source_lo"] or proof["restored_target"] != proof["source_lo"]):
        raise RuntimeError("matching-table LO-mismatch refusal or checked restore observation is missing")


def lo_mismatch_refusal(driver, source, target, srcname, dstname, state, policy, final, tables, manifest, inventory):
    import subprocess
    proof = {"source_lo": final["source"], "source_tables": tables}
    target.sql(dstname, "SELECT lo_put(90005,1,decode('ff','hex'));")
    try:
        proof["target_lo"] = selected_large_objects(large_object_fingerprint(target, dstname), no_acl=policy["no_acl"], no_comments=policy["no_comments"])
        proof["target_tables"] = table_fingerprint(target, dstname, inventory, manifest)
        environment, _ = driver.prepare(state, "compare data")
        result = subprocess.run([str(driver.binary), "compare", "data", "--source", source.connection(srcname), "--target", target.connection(dstname), "--dir", str(state), "--json"],
                                env=environment, capture_output=True, timeout=120)
        clone.require_failure(result.returncode, result.stderr.decode(errors="replace"), "large")
        if result.stdout:
            raise RuntimeError("failed mandatory LO comparison emitted a successful table array")
        proof["returncode"] = result.returncode
    finally:
        target.sql(dstname, "SELECT lo_put(90005,1,decode('02','hex'));")
    restored = observe_large_objects(source, target, srcname, dstname, policy, has_lo=True, final=True)
    proof.update(restored_source=restored["source"], restored_target=restored["target"])
    validate_mismatch_refusal(proof)
    native_compare(driver, source, target, srcname, dstname, state, restored["lo_verified"])
    return proof


def require_stable_inventories(before, after):
    if not isinstance(before, dict) or not isinstance(after, dict) or set(before) != {"source", "target"} or before != after or any(not isinstance(value, list) for value in before.values()):
        raise RuntimeError("physical inventory changed outside the stable verification boundary")


def validate_selected_inventory(inventory):
    if not isinstance(inventory, dict) or len(inventory) > MAX_OBJECTS:
        raise RuntimeError("selected LO inventory is missing or exceeds its bound")
    total = 0
    for oid, row in inventory.items():
        if (not isinstance(row, dict) or not {"oid", "length", "sha256", "owner"} <= row.keys()
                or not row.keys() <= {"oid", "length", "sha256", "owner", "acl", "comment"}
                or not valid_oid(row.get("oid")) or str(oid) != str(row["oid"])
                or type(row.get("length")) is not int or row["length"] < 0
                or not isinstance(row.get("owner"), str) or not row["owner"]
                or not isinstance(row.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", row["sha256"])):
            raise RuntimeError("selected LO identity, length, metadata or content hash is invalid")
        total += row["length"]
    if total > MAX_LOGICAL_BYTES:
        raise RuntimeError("selected LO logical contents exceed their bound")
