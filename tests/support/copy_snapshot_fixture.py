"""Owned fixtures for the private atomic COPY snapshot profile."""

from contextlib import contextmanager
import json
from pathlib import Path
import re

from db_harness import ADMIN, OwnedCluster, assert_identity, conninfo, identifier, psql, quote
from source_snapshot_fixture import SNAPSHOT_PROFILES

GROUPS = ("atomic", "empty", "admission", "rollback", "unknown", "recovery_refusal",
          "source_finish_unknown", "advisory_counts")
VARIANTS = {"atomic": 2, "empty": 0, "page128": 128, "page129": 129, "late_refusal": 129,
            "late_binary": 129, "format_mismatch": 2, "rollback": 2, "unknown": 2,
            "recovery": 2, "source_finish": 2, "counts": 2, "owned_table": 1, "owned_index": 1}
COPY_TARGET_PHASES = {
    "atomic": ("copy",), "empty": ("copy",), "page128": ("copy",), "page129": ("copy",),
    "late_refusal": ("admission",), "late_binary": ("admission",), "format_mismatch": ("admission",),
    "rollback": ("copy",), "unknown": ("copy_unknown", "recovery"),
    "recovery": ("setup", "missing_anchor", "foreign_anchor", "prebegin_intent", "prebegin_reader", "barrier_unknown"),
    "source_finish": ("copy",), "counts": ("copy", "recovery"),
    "owned_table": ("admission",), "owned_index": ("admission",),
}
COPY_SOURCE_PHASES = {"source_finish": ("finish_unknown",)}
COPY_HOLD_PHASES = frozenset({("unknown", "copy_unknown"), ("recovery", "barrier_unknown")})
WIRE_FIELDS = {"owner", "profile", "side", "case", "phase", "status", "backend_pid", "helper_pid",
               "native_path", "stream", "armed", "copy_in_starts", "client_copy_bytes", "copy_complete_tags",
               "error_sqlstates", "commit_tags", "update_tags", "fault_hits", "client_closed", "backend_closed",
               "held_fence_observed", "released_fence_observed", "deadline_ms", "elapsed_ms", "helper_reaped",
               "helper_returncode"}
SOURCE_WIRE_FIELDS = {"source_owner", "peer_pid", "peer_closed", "imported"}


def copy_snapshot_receipt_path(context, side, case, phase):
    assert side in ("target", "source"), "unknown COPY observation side"
    registry = COPY_TARGET_PHASES if side == "target" else COPY_SOURCE_PHASES
    assert case in registry and phase in registry[case], "unknown COPY observation phase"
    root = Path(context["root"])
    assert root.is_absolute(), "COPY observation root must be absolute"
    return root / f"copy-snapshot-wire-{side}-{case}-{phase}.json"


def unique_fields(fields):
    result = dict(fields)
    assert len(result) == len(fields), "duplicate COPY receipt field"
    return result


def read_wire(path):
    assert path.stat().st_size <= 16 * 1024, "COPY wire receipt exceeds its bound"
    return json.loads(path.read_text(), object_pairs_hook=unique_fields)


def validate_copy_snapshot_phase(value, context, side, case, phase, *, reaped=True):
    copy_snapshot_receipt_path(context, side, case, phase)
    fields = WIRE_FIELDS | (SOURCE_WIRE_FIELDS if side == "source" else set())
    assert type(value) is dict and set(value) == fields, "missing or unexpected COPY wire fields"
    assert value["owner"] == context["token"] and value["profile"] == "copy_snapshot"
    assert value["side"] == side and value["case"] == case and value["phase"] == phase
    assert value["status"] == "observed" and value["armed"] is True, "unobserved COPY phase"
    assert value["client_closed"] is True and value["backend_closed"] is True, "COPY relay connection still open"
    for key in ("backend_pid", "helper_pid"):
        assert type(value[key]) is int and 0 < value[key] < 2**32, "invalid COPY process identity"
    assert value["backend_pid"] != value["helper_pid"]
    for key in ("copy_in_starts", "client_copy_bytes", "fault_hits"):
        assert type(value[key]) is int and value[key] >= 0, "invalid COPY observation count"
    assert type(value["deadline_ms"]) is int and 0 < value["deadline_ms"] <= 30000
    assert type(value["elapsed_ms"]) is int and 0 <= value["elapsed_ms"] <= value["deadline_ms"]
    assert value["helper_reaped"] is reaped, "COPY helper was not actually reaped"
    if reaped:
        assert type(value["helper_returncode"]) is int and value["helper_returncode"] == 0
    else:
        assert value["helper_returncode"] is None
    native = Path(value["native_path"])
    assert native.name == "manifest.json" and native.parent.parent == Path(context["root"])
    assert native.parent.name.startswith("owning-copy-") and native.is_absolute(), "foreign COPY native path"
    stream = value["stream"]
    assert type(stream) is dict and set(stream) == {"run", "source", "generation"}
    assert type(stream["run"]) is list and len(stream["run"]) == 16 and any(stream["run"])
    assert all(type(byte) is int and 0 <= byte <= 255 for byte in stream["run"])
    assert type(stream["generation"]) is int and stream["generation"] == 9
    source = stream["source"]
    assert type(source) is dict and set(source) == {"system_identifier", "database_oid"}
    assert type(source["system_identifier"]) is int and 0 < source["system_identifier"] < 2**64
    assert type(source["database_oid"]) is int and 0 < source["database_oid"] < 2**32
    assert source == context["copy_snapshot_variants"][case]["source_identity"]
    patterns = {"copy_complete_tags": r"COPY (?:0|[1-9][0-9]*)", "commit_tags": r"COMMIT",
                "update_tags": r"UPDATE (?:0|[1-9][0-9]*)", "error_sqlstates": r"[0-9A-Z]{5}"}
    for key, pattern in patterns.items():
        assert type(value[key]) is list and all(type(tag) is str and re.fullmatch(pattern, tag) for tag in value[key])
    held = side == "target" and (case, phase) in COPY_HOLD_PHASES
    assert value["held_fence_observed"] is held and value["released_fence_observed"] is held
    starts, tags, errors, commits, updates, hits = 0, [], [], [], [], int(held)
    if side == "source":
        assert value["source_owner"] == context["copy_snapshot_source"]["token"]
        assert type(value["peer_pid"]) is int and 0 < value["peer_pid"] < 2**32
        assert value["peer_pid"] not in (value["backend_pid"], value["helper_pid"])
        assert value["peer_closed"] is True and value["imported"] is True
        commits, hits = ["COMMIT"], 1
    elif case == "rollback":
        starts, tags, errors = 2, ["COPY 2"], ["23505"]
    elif phase in ("copy", "setup", "copy_unknown"):
        starts = VARIANTS[case]
        tags = ["COPY 2"] * starts if case != "atomic" else ["COPY 0", "COPY 2"]
        commits, updates = ["COMMIT"], ["UPDATE 1"]
    elif phase in ("recovery", "barrier_unknown"):
        commits, updates = ["COMMIT"], ["UPDATE 1"]
    assert value["copy_in_starts"] == starts, "COPY started before complete admission or required starts are absent"
    assert value["copy_complete_tags"] == tags and value["error_sqlstates"] == errors
    assert value["commit_tags"] == commits and value["update_tags"] == updates, "missing or extra logged completion"
    assert value["fault_hits"] == hits
    assert (value["client_copy_bytes"] > 0) is (starts > 0), "COPY byte witness differs from actual starts"


def validate_copy_snapshot_receipts(context):
    expected = [(side, case, phase) for side, registry in (("target", COPY_TARGET_PHASES), ("source", COPY_SOURCE_PHASES))
                for case, phases in registry.items() for phase in phases]
    paths = [copy_snapshot_receipt_path(context, *key) for key in expected]
    assert sorted(path.name for path in Path(context["root"]).glob("copy-snapshot-wire-*.json")) == sorted(path.name for path in paths), "missing or extra COPY wire receipts"
    records = {}
    for key, path in zip(expected, paths):
        value = read_wire(path)
        validate_copy_snapshot_phase(value, context, *key)
        records[key] = value
    assert len({value["helper_pid"] for value in records.values()}) == len(records), "duplicate COPY relay helper"
    for case in ("unknown", "counts"):
        first = records[("target", case, "copy_unknown" if case == "unknown" else "copy")]
        recovered = records[("target", case, "recovery")]
        assert first["stream"] == recovered["stream"] and first["native_path"] == recovered["native_path"]
        assert first["backend_pid"] != recovered["backend_pid"], "COPY recovery reused the old backend"
    source = records[("source", "source_finish", "finish_unknown")]
    target = records[("target", "source_finish", "copy")]
    assert source["native_path"] == target["native_path"] and source["stream"] == target["stream"], "source finish lacks its matching target receipt"


def copy_snapshot_cases():
    values = json.loads(Path(__file__).with_name("copy_snapshot_cases.json").read_text())
    expected = ["snapshot::copy_live_tests::live_copy_snapshot_" + group for group in GROUPS]
    assert values == expected, "missing, duplicate, or wrong owning-COPY inventory"
    return values


def owned_database(context, database):
    owner = psql(context, database, "SELECT pg_catalog.shobj_description(oid,'pg_database') "
                 "FROM pg_catalog.pg_database WHERE datname=current_database();")
    assert owner == context["token"], "refusing COPY fixture changes outside an owned database"


def prepare_copy_snapshot_fixture(target, source):
    assert_identity(target)
    assert_identity(source)
    owned_database(target, "e3_authority")
    role = source["replication_role"]
    psql(source, "postgres", f"CREATE ROLE {identifier(role)} LOGIN REPLICATION NOSUPERUSER "
         "NOCREATEDB NOCREATEROLE NOBYPASSRLS;")
    variants = {}
    for variant, count in VARIANTS.items():
        dynamic = "lookalike" if variant == "atomic" else variant if variant in ("owned_table", "owned_index") else None
        initial = 1 if variant == "atomic" else 0 if dynamic else count
        database = "copy_" + variant
        psql(source, "postgres", f"CREATE DATABASE {identifier(database)} OWNER {ADMIN} TEMPLATE template0 ENCODING 'UTF8';")
        psql(source, "postgres", f"COMMENT ON DATABASE {identifier(database)} IS {quote(source['token'])};")
        owned_database(source, database)
        psql(source, database, "REVOKE ALL ON DATABASE " + identifier(database) + " FROM PUBLIC; "
             f"GRANT CONNECT ON DATABASE {identifier(database)} TO {identifier(role)}; "
             "REVOKE CREATE ON SCHEMA public FROM PUBLIC; "
             f"GRANT USAGE ON SCHEMA public TO {identifier(role)}; "
             "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC; "
             f"GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {identifier(role)};")
        privileges = json.loads(psql(source, database, "SELECT json_build_array(rolreplication,rolsuper,rolcreatedb,"
                               "rolcreaterole,rolbypassrls,has_database_privilege(current_database(),'CREATE'),"
                               "has_schema_privilege('public','CREATE'),"
                               "has_function_privilege('pg_catalog.pg_control_system()','EXECUTE')) "
                               "FROM pg_roles WHERE rolname=current_user;", role=role))
        assert privileges == [True, False, False, False, False, False, False, True], "COPY fixture source privileges differ"
        identity = json.loads(psql(source, database, "SELECT json_build_array(c.system_identifier::text,d.oid::bigint) "
                                   "FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_database d "
                                   "WHERE d.datname=current_database();", role=role))
        assert type(identity) is list and len(identity) == 2
        assert type(identity[0]) is str and re.fullmatch(r"-?[0-9]+", identity[0])
        system = int(identity[0])
        assert -(2**63) <= system < 2**64 and system % 2**64 != 0, "invalid COPY source system identity"
        assert type(identity[1]) is int and 0 < identity[1] < 2**32, "invalid COPY source database identity"
        variants[variant] = {"source_database": database, "target_database": "e3_authority",
                             "prefix": "cs_" + variant, "tables": count, "initial_tables": initial,
                             "dynamic_qname": dynamic,
                             "source_identity": {"system_identifier": system % 2**64, "database_oid": identity[1]}}
        source_sql, target_sql = [], []
        for number in range(initial):
            table = f"cs_{variant}_{number:03}"
            # Cross the control-file limit with relation metadata, without adding rows or tables.
            if count >= 128:
                table = table.ljust(63, "_")
            source_sql.append(f"CREATE TABLE public.{table} (id integer PRIMARY KEY, payload integer);")
            source_sql.append(f"INSERT INTO public.{table} VALUES "
                              + ("(1,7),(2,7);" if variant == "rollback" and number == 1 else "(1,7),(2,8);"))
            source_sql.append(f"GRANT SELECT ON public.{table} TO {identifier(role)};")
            payload = "bigint" if variant == "late_refusal" and number == count - 1 else "integer"
            target_sql.append(f"CREATE TABLE public.{table} (id integer PRIMARY KEY, payload {payload});")
        if initial:
            psql(source, database, " ".join(source_sql))
            psql(target, "e3_authority", " ".join(target_sql), role="e1_target")
        if variant == "rollback":
            psql(target, "e3_authority", "CREATE UNIQUE INDEX cs_rollback_payload_unique "
                 "ON public.cs_rollback_001 (payload);", role="e1_target")
        actual = psql(source, database, "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                      "WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' "
                      "AND c.relkind IN ('r','p','v','m','f');", role=role)
        assert actual == str(initial), "COPY source inventory differs from the explicit fixture"
    return variants


@contextmanager
def copy_snapshot_fixture(target_cluster, manifest):
    profile = manifest["server_version"], manifest["server_version_num"]
    assert type(profile[0]) is str and type(profile[1]) is int and profile in SNAPSHOT_PROFILES, "unqualified owning-COPY profile"
    with OwnedCluster(manifest, logical=True, logical_slots=16) as source:
        source.context.update(database="copy_atomic", replication_role="copy_snapshot_" + source.token[:12],
                              logical_slots=16)
        variants = prepare_copy_snapshot_fixture(target_cluster.context, source.context)
        slots = {variant: f"cp_{source.token[:10]}_{number}" for number, variant in enumerate(VARIANTS)}
        assert len(slots) <= 16 and len(set(slots.values())) == len(slots)
        context = {**target_cluster.context, "copy_snapshot_source": source.context,
                   "copy_snapshot_variants": variants, "copy_snapshot_slots": slots,
                   "expected_server_version": manifest["server_version"],
                   "expected_server_version_num": manifest["server_version_num"]}
        path = Path(context["root"]) / "copy-snapshot-context.json"
        path.touch(mode=0o600, exist_ok=False)
        path.write_text(json.dumps(context))
        base = conninfo(source.context, "copy_atomic", source.context["replication_role"])
        yield context, {
            "RPGMIGRATE_TEST_COPY_SNAPSHOT_CONTEXT": str(path),
            "RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE": base,
            "RPGMIGRATE_TEST_COPY_SNAPSHOT_EXPORTER": base + " replication=database",
            "RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE_ADMIN": conninfo(source.context, "copy_atomic", ADMIN),
        }
