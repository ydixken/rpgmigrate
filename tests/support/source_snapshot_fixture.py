"""Owned source-only snapshot fixtures and independently checked fault receipts."""

from contextlib import contextmanager
import json
from pathlib import Path
import re

from db_harness import ADMIN, OwnedCluster, assert_identity, conninfo, identifier, psql, quote

DATABASE = "snapshot_source"
DENIED_DATABASE = "snapshot_denied"
EMPTY_DATABASE = "snapshot_empty"
FAULT_CASES = ("create_reply_lost", "create_tail_lost", "expired_export_refused",
               "imported_identity_mismatch", "source_finish_unknown")
TAIL_PHASES = ("tail_lost", "ready_transaction", "ready_failed")
SNAPSHOT_PROFILES = {("14.24", 140024): "22023", ("17.11", 170011): "42704"}


def expiry_sqlstate(context):
    profile = context["expected_server_version"], context["expected_server_version_num"]
    assert type(profile[0]) is str and type(profile[1]) is int and profile in SNAPSHOT_PROFILES, "unqualified declared snapshot profile"
    assert type(context["server_version_num"]) is int and context["server_version_num"] == profile[1], "actual snapshot server differs from declared profile"
    return SNAPSHOT_PROFILES[profile]


def unique_fields(fields):
    result = dict(fields)
    assert len(result) == len(fields), "duplicate snapshot receipt field"
    return result


def snapshot_cases():
    values = json.loads(Path(__file__).with_name("source_snapshot_cases.json").read_text())
    assert isinstance(values, list) and len(values) == 12, "missing source snapshot test inventory"
    assert all(isinstance(value, str) and re.fullmatch(r"snapshot::live_tests::live_[a-z_]+", value)
               for value in values), "invalid source snapshot test inventory"
    assert len(set(values)) == 12, "duplicate source snapshot test inventory"
    return values


def setup_source(cluster, role, *, controls):
    context = cluster.context
    assert_identity(context)
    psql(context, "postgres", f"CREATE ROLE {identifier(role)} LOGIN REPLICATION NOSUPERUSER "
         "NOCREATEDB NOCREATEROLE NOBYPASSRLS;")
    names = (DATABASE, DENIED_DATABASE, EMPTY_DATABASE) if controls else (DATABASE,)
    for database in names:
        psql(context, "postgres", f"CREATE DATABASE {identifier(database)} OWNER {ADMIN} TEMPLATE template0 ENCODING 'UTF8';")
        psql(context, "postgres", f"COMMENT ON DATABASE {identifier(database)} IS {quote(context['token'])}; "
             f"REVOKE ALL ON DATABASE {identifier(database)} FROM PUBLIC; "
             f"GRANT CONNECT ON DATABASE {identifier(database)} TO {identifier(role)};")
        owned = psql(context, database, "SELECT pg_catalog.shobj_description(oid,'pg_database') "
                     "FROM pg_catalog.pg_database WHERE datname=current_database();")
        assert owned == context["token"], "refusing grants outside the owned fixture database"
        # EXECUTE changes are confined to this owned database's copy of the existing function.
        psql(context, database, "REVOKE CREATE ON SCHEMA public FROM PUBLIC; "
             f"GRANT USAGE ON SCHEMA public TO {identifier(role)}; "
             "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC; "
             f"REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM {identifier(role)}; "
             + (f"GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {identifier(role)};"
                if database != DENIED_DATABASE else ""))
        actual = psql(context, database, "SELECT json_build_array(rolreplication,rolsuper,rolcreatedb,"
                      "rolcreaterole,rolbypassrls,has_database_privilege(current_database(),'CREATE'),"
                      "has_schema_privilege('public','CREATE'),"
                      "has_function_privilege('pg_catalog.pg_control_system()','EXECUTE')) "
                      "FROM pg_roles WHERE rolname=current_user;", role=role)
        assert json.loads(actual) == [True, False, False, False, False, False, False,
                                     database != DENIED_DATABASE], "snapshot fixture role privileges differ"
    psql(context, DATABASE, "CREATE TABLE public.snapshot_data (value integer NOT NULL); "
         "INSERT INTO public.snapshot_data VALUES (10),(20); "
         f"GRANT SELECT ON public.snapshot_data TO {identifier(role)};")
    psql(context, DATABASE, 'CREATE TABLE public.snapshot_cap_relation ("first key" integer NOT NULL, '
         'payload varchar(12), "last key" bigint NOT NULL, '
         'PRIMARY KEY ("last key","first key") INCLUDE (payload)); '
         f"GRANT SELECT ON public.snapshot_cap_relation TO {identifier(role)};")
    if controls:
        psql(context, DATABASE, "ALTER TABLE public.snapshot_data ADD PRIMARY KEY (value); " + " ".join(
            f"CREATE TABLE public.snapshot_page_{number:03} (value integer PRIMARY KEY); "
            f"GRANT SELECT ON public.snapshot_page_{number:03} TO {identifier(role)};"
            for number in range(257)))
    proof = psql(context, DATABASE, "SELECT slot_name FROM pg_create_logical_replication_slot("
                 "'snapshot_plugin_probe','test_decoding',true); "
                 "SELECT plugin FROM pg_replication_slots WHERE slot_name='snapshot_plugin_probe';", role=role)
    assert proof.splitlines() == ["snapshot_plugin_probe", "test_decoding"], "snapshot fixture plugin unavailable"
    assert psql(context, DATABASE, "SELECT count(*) FROM pg_replication_slots;") == "0"


@contextmanager
def source_snapshot_fixture(manifest):
    assert (manifest["server_version"], manifest["server_version_num"]) in SNAPSHOT_PROFILES, "unqualified source snapshot fixture version"
    with OwnedCluster(manifest, logical=True, logical_slots=16) as cluster, \
            OwnedCluster(manifest, logical=True) as other:
        role = "snapshot_" + cluster.token[:12]
        setup_source(cluster, role, controls=True)
        setup_source(other, role, controls=False)
        context = cluster.context
        system = psql(context, DATABASE, "SELECT system_identifier::text FROM pg_catalog.pg_control_system();")
        other_system = psql(other.context, DATABASE, "SELECT system_identifier::text FROM pg_catalog.pg_control_system();")
        assert system != other_system, "wrong-cluster fixture must have a different system identity"
        slots = {name.rsplit("::", 1)[1]: f"ss_{cluster.token[:10]}_{number}"
                 for number, name in enumerate(snapshot_cases())}
        context.update(database=DATABASE, denied_database=DENIED_DATABASE, empty_database=EMPTY_DATABASE,
                       replication_role=role, other_context=other.context, slots=slots,
                       system_identifier=system, other_system_identifier=other_system, logical_slots=16,
                       database_oid=int(psql(context, DATABASE, "SELECT oid FROM pg_database WHERE datname=current_database();")),
                       application_name_env="snapshot_env_" + cluster.token[:12],
                       expected_server_version=manifest["server_version"],
                       expected_server_version_num=manifest["server_version_num"])
        cluster.context_path.write_text(json.dumps(context))
        source = conninfo(context, DATABASE, role)
        yield context, {
            "RPGMIGRATE_TEST_SNAPSHOT_CONTEXT": str(cluster.context_path),
            "RPGMIGRATE_TEST_SNAPSHOT_SQL": source,
            "RPGMIGRATE_TEST_SNAPSHOT_EXPORTER": source + " replication=database",
            "RPGMIGRATE_TEST_SNAPSHOT_ADMIN": conninfo(context, DATABASE, ADMIN),
            "RPGMIGRATE_TEST_SNAPSHOT_OTHER_SQL": conninfo(other.context, DATABASE, role),
            "RPGMIGRATE_TEST_SNAPSHOT_DENIED_SQL": conninfo(context, DENIED_DATABASE, role),
            "RPGMIGRATE_TEST_SNAPSHOT_FAULT_HELPER": str(Path(__file__).with_name("source_snapshot_fault.py")),
            "PGAPPNAME": context["application_name_env"],
        }


def validate_source_snapshot_faults(context):
    root = Path(context["root"])
    assert sorted(path.name for path in root.glob("snapshot-fault-*.json")) == sorted(
        f"snapshot-fault-{case}.json" for case in FAULT_CASES), "missing or unexpected snapshot fault receipts"
    for case in FAULT_CASES:
        value = json.loads((root / f"snapshot-fault-{case}.json").read_text(), object_pairs_hook=unique_fields)
        validate_fault(value, context, case)


def validate_fault(value, context, case):
    assert value["owner"] == context["token"] and value["case"] == case, "foreign snapshot fault receipt"
    assert value["status"] == "observed", "pending or ignored snapshot fault receipt"
    if case == "create_tail_lost":
        assert set(value["phases"]) == set(TAIL_PHASES), "missing or duplicate CREATE tail phase"
        assert type(value["fault_hits"]) is int and value["fault_hits"] == 3
        for phase, proof in value["phases"].items():
            validate_phase(proof, context, case, phase)
        assert len({proof["slot"]["name"] for proof in value["phases"].values()}) == 3
        assert len({tuple(proof["native_nonce"]) for proof in value["phases"].values()}) == 3
    else:
        validate_phase(value, context, case, case)


def validate_phase(value, context, case, phase, *, reaped=True):
    assert value["owner"] == context["token"] and value["case"] == case and value["phase"] == phase
    assert value["status"] == "observed" and value["armed"] is True
    assert type(value["fault_hits"]) is int and value["fault_hits"] == 1
    assert all(value[key] is True for key in ("backend_closed", "peer_closed", "client_failed",
                                             "fault_before_deadline", "failure_before_deadline"))
    for key in ("backend_pid", "peer_pid", "helper_pid"):
        assert type(value[key]) is int and value[key] > 0
    assert value["backend_pid"] != value["peer_pid"]
    assert type(value["deadline_seconds"]) is int and 0 < value["deadline_seconds"] <= 30
    if reaped:
        assert value["helper_reaped"] is True and type(value["helper_returncode"]) is int
        assert value["helper_returncode"] == 0, "snapshot helper child did not exit successfully"
    slot = value["slot"]
    expected_slot = context["slots"]["live_" + case]
    if phase in ("ready_transaction", "ready_failed"):
        expected_slot += "_t" if phase == "ready_transaction" else "_e"
    assert slot["name"] == expected_slot and slot["plugin"] == "test_decoding"
    assert slot["database"] == context["database"] and slot["temporary"] is False
    assert isinstance(slot["consistent_point"], str) and re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", slot["consistent_point"])
    assert type(value["native_nonce"]) is list and len(value["native_nonce"]) == 16
    assert all(type(byte) is int and 0 <= byte <= 255 for byte in value["native_nonce"]) and any(value["native_nonce"])
    native_path = Path(value["native_path"])
    assert native_path.name == "manifest.json" and native_path.parent.parent == Path(context["root"])
    assert native_path.parent.name.startswith("snapshot-")
    native = json.loads(native_path.read_text())
    assert native["magic"] == "RPGMIGRATE" and native["format_version"] == 4
    assert native["copy_snapshot"] is None, "snapshot fixture contains a COPY anchor intent"
    assert native["canonical_path"] == str(native_path.parent)
    assert native["copy_complete"] is None and native["target_provenance"] == []
    assert type(native["run"]) is list and len(native["run"]) == 16 and any(native["run"])
    source = native["source_slot"]
    assert source["phase"] == value["native_phase"]
    intent = source["record"] if source["phase"] == "provisioning" else source["record"]["intent"]
    assert intent["nonce"] == value["native_nonce"]
    binding = intent["binding"]
    assert binding == {"slot_name": expected_slot, "plugin": "test_decoding", "catalog_epoch": 7,
                       "stream": {"run": native["run"], "generation": 9,
                                  "source": {"system_identifier": int(context["system_identifier"]) % 2**64,
                                             "database_oid": context["database_oid"]}}}
    if source["phase"] == "confirmed":
        high, low = (int(part, 16) for part in slot["consistent_point"].split("/"))
        assert source["record"]["consistent_point"] == (high << 32) | low
        assert isinstance(source["record"]["snapshot_token"], str) and source["record"]["snapshot_token"]
    details = value["details"]
    if case in ("create_reply_lost", "create_tail_lost"):
        assert value["native_phase"] == "provisioning" and value["intent_before_create"] is True
        assert details["row_seen"] is True and details["completion_seen"] is True
        assert details["command_tag"] == "CREATE_REPLICATION_SLOT" and details["ready_before"] == "I"
        assert details["row_delivered"] is (case == "create_tail_lost")
        if phase in ("ready_transaction", "ready_failed"):
            assert details["ready_after"] == ("T" if phase == "ready_transaction" else "E")
            assert details["client_closed_after_ready"] is True
        else:
            assert details["ready_after"] is None
    else:
        assert value["native_phase"] == "confirmed" and details["import_seen"] is True
        if case == "expired_export_refused":
            expected = expiry_sqlstate(context)
            assert details["exporter_expired"] is True and details["expected_sqlstate"] == expected
            assert details["sqlstate"] == expected
        elif case == "imported_identity_mismatch":
            assert details["identity_before"] == value["backend_pid"]
            assert type(details["identity_after"]) is int and details["identity_after"] > 0
            assert details["identity_after"] != details["identity_before"]
        else:
            assert case == "source_finish_unknown" and details["command_tag"] == "COMMIT"
