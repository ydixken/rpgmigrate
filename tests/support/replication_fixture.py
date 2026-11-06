"""A separate owned logical source and strict raw-transport fault receipts."""

from contextlib import contextmanager
import json
from pathlib import Path
import re

from db_harness import ADMIN, OwnedCluster, conninfo, psql, quote

DATABASE = "replication_source"
ROLE = "replication_stream"
FAULT_CASES = ("keepalive", "duplex", "terminal_error", "missing_completion")


@contextmanager
def replication_fixture(manifest):
    with OwnedCluster(manifest, logical=True) as cluster:
        context = cluster.context
        psql(context, "postgres", f"CREATE ROLE {ROLE} LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;")
        psql(context, "postgres", f"CREATE DATABASE {DATABASE} OWNER {ADMIN} TEMPLATE template0 ENCODING 'UTF8';")
        psql(context, "postgres", f"COMMENT ON DATABASE {DATABASE} IS {quote(cluster.token)}; REVOKE ALL ON DATABASE {DATABASE} FROM PUBLIC; GRANT CONNECT ON DATABASE {DATABASE} TO {ROLE};")
        psql(context, DATABASE, "REVOKE CREATE ON SCHEMA public FROM PUBLIC;")
        actual = psql(context, DATABASE, "SELECT json_build_array(current_user,rolreplication,rolsuper,rolcreatedb,rolcreaterole,rolbypassrls,has_database_privilege(current_database(),'CREATE'),has_schema_privilege('public','CREATE')) FROM pg_roles WHERE rolname=current_user;", role=ROLE)
        assert json.loads(actual) == [ROLE, True, False, False, False, False, False, False]
        # Loading a temporary slot proves plugin availability without leaving retained WAL.
        proof = psql(context, DATABASE, "SELECT slot_name FROM pg_create_logical_replication_slot('fixture_plugin_probe','test_decoding',true); SELECT plugin FROM pg_replication_slots WHERE slot_name='fixture_plugin_probe';", role=ROLE)
        assert proof.splitlines() == ["fixture_plugin_probe", "test_decoding"], "test_decoding fixture plugin did not load"
        assert psql(context, DATABASE, "SELECT count(*) FROM pg_replication_slots;") == "0"
        context.update(database=DATABASE, replication_role=ROLE)
        cluster.context_path.write_text(json.dumps(context))
        source = conninfo(context, DATABASE, ROLE)
        yield context, {
            "RPGMIGRATE_TEST_REPLICATION_SOURCE": source + " replication=database",
            "RPGMIGRATE_TEST_REPLICATION_SQL": source,
            "RPGMIGRATE_TEST_REPLICATION_ADMIN": conninfo(context, DATABASE, ADMIN),
            "RPGMIGRATE_TEST_REPLICATION_DATABASE": DATABASE,
            "RPGMIGRATE_TEST_REPLICATION_CONTEXT": str(cluster.context_path),
            "RPGMIGRATE_TEST_REPLICATION_FAULT_HELPER": str(Path(__file__).with_name("replication_fault.py")),
        }


def validate_common(value, context, case, hits=1):
    assert value["owner"] == context["token"] and value["case"] == case, "foreign replication fault receipt"
    assert value["status"] == "observed", "pending or ignored replication fault receipt"
    assert type(value["fault_hits"]) is int and value["fault_hits"] == hits, "replication fault count differs"
    assert value["copyboth_seen"] is True and value["backend_closed"] is True


def validate_terminal_phase(value, context, phase):
    validate_common(value, context, "terminal_error")
    assert value["error_sqlstate"] == "57014"
    if phase == "late_error":
        assert value["copydone_seen"] is True and value["terminal_seen"] is True
        assert value["terminal_fault"] == "error"
    else:
        assert phase == "pending_error"
        assert type(value["backend_pid"]) is int and value["backend_pid"] > 0
        assert value["cancel_requested"] is True and value["owned_backend_verified"] is True
        assert value["server_error_seen"] is True and value["gate_held_at_error"] is True
        assert value["held"] is True and value["released"] is False
        assert type(value["pqflush_pending"]) is int and value["pqflush_pending"] > 0
        assert value["client_error_sqlstate"] == "57014" and value["client_closed_before_release"] is True
        assert value["client_before_deadline"] is True
        assert type(value["feedback_drained_after_cancel"]) is int and value["feedback_drained_after_cancel"] == 0


def validate_fault(value, context, case):
    validate_common(value, context, case, 2 if case == "terminal_error" else 1)
    if case == "terminal_error":
        assert set(value["phases"]) == {"late_error", "pending_error"}, "both terminal error phases are required"
        for phase, proof in value["phases"].items():
            validate_terminal_phase(proof, context, phase)
        assert value["error_sqlstate"] == "57014"
        return
    if case in ("keepalive", "duplex"):
        assert type(value["feedback_count"]) is int and value["feedback_count"] > 0
        assert re.fullmatch(r"72[0-9a-f]{66}", value["feedback_hex"]), "missing complete r34 feedback"
    if case == "keepalive":
        assert value["keepalive_sent"] is True
        assert type(value["post_arm_feedback_count"]) is int and value["post_arm_feedback_count"] > 0
        assert type(value["feedback_mismatches"]) is int and value["feedback_mismatches"] == 0
        assert value["feedback_hex"] == value["expected_feedback_hex"], "feedback differs from supplied bytes"
    elif case == "duplex":
        assert value["held"] is False and value["released"] is True
        assert type(value["server_frames_while_held"]) is int and value["server_frames_while_held"] > 0
        assert type(value["pqflush_pending"]) is int and value["pqflush_pending"] > 0, "no observed PQflush==1 backpressure"
    else:
        assert value["copydone_seen"] is True and value["terminal_seen"] is True
        expected = "error" if case == "terminal_error" else "missing"
        assert value["terminal_fault"] == expected
        assert value["error_sqlstate"] == ("57014" if expected == "error" else None)


def validate_replication_faults(context):
    root = Path(context["root"])
    assert sorted(path.name for path in root.glob("replication-fault-*.json")) == sorted(
        f"replication-fault-{case}.json" for case in FAULT_CASES), "missing or unexpected replication fault receipts"
    for case in FAULT_CASES:
        validate_fault(json.loads((root / f"replication-fault-{case}.json").read_text()), context, case)
