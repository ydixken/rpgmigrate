"""Journal-only setup and receipt checks on the existing Engine fixtures."""

import json
from pathlib import Path
import re

from authority_fault import JOURNAL_CASES
from db_harness import assert_identity, identifier, psql

CANDIDATES = ("segmented", "sqlite")
CHILD = "snapshot::journal_live_tests::journal_serial_crash_child"


def journal_cases():
    values = json.loads(Path(__file__).with_name("journal_cases.json").read_text())
    expected = [f"snapshot::journal_live_tests::{candidate}_real_wal_reopens_and_serial_replays"
                for candidate in CANDIDATES]
    assert values == expected, "missing, duplicate, or wrong journal parent inventory"
    return values


def prepare_journal_fixture(target, source):
    for context, database in ((target, "e3_authority"), (source, source["database"])):
        assert_identity(context)
        owner = psql(context, database, "SELECT pg_catalog.shobj_description(oid,'pg_database') "
                     "FROM pg_catalog.pg_database WHERE datname=current_database();")
        assert owner == context["token"], "refusing journal setup outside an owned fixture database"
    role = source["replication_role"]
    database = source["database"]
    psql(source, database, "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC; "
         f"GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {identifier(role)}; "
         f"GRANT USAGE ON SCHEMA public TO {identifier(role)};")
    actual = json.loads(psql(source, database, "SELECT json_build_array(rolreplication,rolsuper,rolcreatedb,"
                           "rolcreaterole,rolbypassrls,has_database_privilege(current_database(),'CREATE'),"
                           "has_schema_privilege('public','CREATE'),"
                           "has_function_privilege('pg_catalog.pg_control_system()','EXECUTE')) "
                           "FROM pg_roles WHERE rolname=current_user;", role=role))
    assert actual == [True, False, False, False, False, False, False, True], "journal source role privileges differ"
    identity = json.loads(psql(source, database, "SELECT json_build_array(c.system_identifier::text,d.oid::bigint) "
                              "FROM pg_catalog.pg_control_system() c CROSS JOIN pg_catalog.pg_database d "
                              "WHERE d.datname=current_database();", role=role))
    system, oid = int(identity[0]) % 2**64, identity[1]
    assert system != 0 and type(oid) is int and 0 < oid < 2**32, "missing journal source identity"
    target["journal_source_identity"] = {"system_identifier": system, "database_oid": oid}
    for candidate in CANDIDATES:
        table = identifier("journal_serial_" + candidate)
        ddl = f"CREATE TABLE public.{table} (id integer PRIMARY KEY, payload text);"
        psql(source, database, ddl + f" GRANT SELECT ON public.{table} TO {identifier(role)};")
        psql(target, "e3_authority", ddl, role="e1_target")
        for context, name, user in ((source, database, role), (target, "e3_authority", "e1_target")):
            assert psql(context, name, f"SELECT count(*) FROM public.{table};", role=user) == "0", "journal table is not empty"


def unique_fields(fields):
    result = dict(fields)
    assert len(result) == len(fields), "duplicate journal receipt field"
    return result


def read_json(path):
    assert path.stat().st_size <= 16 * 1024, "journal control file exceeds its bound"
    return json.loads(path.read_text(), object_pairs_hook=unique_fields)


def validate_crash(context, candidate):
    root = Path(context["root"])
    value = read_json(root / f"journal-{candidate}-crash-receipt.json")
    assert set(value) == {"owner", "candidate", "child_test", "parent_pid", "child_pid", "binding",
                          "descriptor_hex", "witness_path", "before_commit", "signal", "reaped",
                          "deadline_ms", "elapsed_ms"}, "missing or unexpected journal child receipt fields"
    assert value["owner"] == context["token"] and value["candidate"] == candidate
    assert value["child_test"] == CHILD and value["before_commit"] is True and value["reaped"] is True
    assert type(value["signal"]) is int and value["signal"] == 9
    for key in ("parent_pid", "child_pid"):
        assert type(value[key]) is int and value[key] > 0
    assert value["parent_pid"] != value["child_pid"]
    assert type(value["deadline_ms"]) is int and 0 < value["deadline_ms"] <= 30000
    assert type(value["elapsed_ms"]) is int and 0 <= value["elapsed_ms"] < value["deadline_ms"]
    descriptor = value["descriptor_hex"]
    assert isinstance(descriptor, str) and re.fullmatch(r"[0-9a-f]{256}", descriptor)
    proof = root / f"journal-{candidate}-external.bin"
    assert proof.stat().st_size == 128 and proof.read_bytes().hex() == descriptor, "external descriptor differs"
    witness_path = root / f"journal-{candidate}-kill.json"
    assert value["witness_path"] == str(witness_path)
    witness = read_json(witness_path)
    assert set(witness) == {"phase", "pid", "binding", "proof"}
    assert witness["phase"] == "before_commit" and witness["pid"] == value["child_pid"]
    assert witness["binding"] == value["binding"] and witness["proof"] == str(proof)
    request = read_json(witness_path.with_suffix(".request.json"))
    native_path = root / f"journal-{candidate}-native"
    assert request == {"native": str(native_path), "journal": str(root / f"journal-{candidate}-records"),
                       "proof": str(proof), "witness": str(witness_path), "binding": value["binding"],
                       "candidate": candidate}, "foreign journal child request"
    native = read_json(native_path / "manifest.json")
    assert native["magic"] == "RPGMIGRATE" and native["format_version"] == 4
    assert native["copy_snapshot"] is None, "journal fixture contains a COPY anchor intent"
    assert native["canonical_path"] == str(native_path) and native["copy_complete"] is None
    assert native["source_slot"]["phase"] == "confirmed"
    binding = native["source_slot"]["record"]["intent"]["binding"]
    assert binding == value["binding"]
    assert binding == {"stream": {"run": native["run"], "source": context["journal_source_identity"], "generation": 9},
                       "slot_name": f"js_{context['replication']['token'][:10]}_{candidate}",
                       "plugin": "test_decoding", "catalog_epoch": 7}, "journal native binding differs"
    assert type(native["run"]) is list and len(native["run"]) == 16 and any(native["run"])
    assert all(type(byte) is int and 0 <= byte <= 255 for byte in native["run"])
    return value["child_pid"]


def validate_journal_receipts(context):
    root = Path(context["root"])
    assert sorted(path.name for path in root.glob("journal-*-crash-receipt.json")) == sorted(
        f"journal-{candidate}-crash-receipt.json" for candidate in CANDIDATES), "missing or unexpected journal child receipts"
    children = [validate_crash(context, candidate) for candidate in CANDIDATES]
    assert len(set(children)) == 2, "duplicate journal crash child"
    assert sorted(path.name for path in root.glob("authority-fault-*.json")) == sorted(
        f"authority-fault-{case}.json" for case in JOURNAL_CASES), "missing or unexpected journal COMMIT receipts"
    for case in JOURNAL_CASES:
        value = read_json(root / f"authority-fault-{case}.json")
        assert set(value) == {"owner", "case", "status", "fault_hits", "backend_pid", "commit_tag", "holding_backend",
                              "backend_connection_closed", "held_fence_observed", "released_fence_observed"}
        assert value["owner"] == context["token"] and value["case"] == case and value["status"] == "observed"
        assert type(value["fault_hits"]) is int and value["fault_hits"] == 1 and value["commit_tag"] == "COMMIT"
        assert type(value["backend_pid"]) is int and value["backend_pid"] > 0
        assert value["holding_backend"] is False and value["backend_connection_closed"] is True
        assert value["held_fence_observed"] is True and value["released_fence_observed"] is True
