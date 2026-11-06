"""Prove authority fixtures against psql before using them for runtime tests."""

import json
import os
import select
import signal
import subprocess
import sys
import time

from db_harness import OwnedCluster, load_manifest
from authority_fixture import authority_fixture, validate_authority_faults


def query(cluster, connection, sql):
    return subprocess.run(
        [str(cluster.bindir / "psql"), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname", connection],
        input=sql, env=cluster.environment, capture_output=True, text=True, timeout=10,
    )


def line_from(process, timeout=10):
    deadline = time.monotonic() + timeout
    value = bytearray()
    while len(value) < 16384:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
            raise AssertionError("fixture child response deadline exceeded")
        byte = os.read(process.stdout.fileno(), 1)
        if not byte:
            raise AssertionError("fixture child exited without its expected response")
        if byte == b"\n":
            return value.decode("utf-8")
        value.extend(byte)
    raise AssertionError("fixture child response exceeds its bound")


def fault_self_check(cluster, environment, case, key):
    child_environment = {**cluster.environment, "RPGMIGRATE_TEST_CONTEXT": str(cluster.context_path)}
    helper = subprocess.Popen([sys.executable, environment["RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER"], "--case", case],
                              env=child_environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
    client = None
    try:
        ready = json.loads(line_from(helper))
        assert ready["ready"] is True

        def operation(op):
            helper.stdin.write((json.dumps({"op": op}) + "\n").encode())
            value = json.loads(line_from(helper))
            assert value["op"] == op
            return value

        client = subprocess.Popen([str(cluster.bindir / "psql"), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname", ready["conninfo"]],
                                  env=cluster.environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        table = "authority_fixture_" + case
        setup = f"CREATE TABLE public.{table}(value integer);" if case == "lost_commit_reply" else ""
        client.stdin.write((f"SELECT pg_advisory_lock({key}); {setup} BEGIN; COMMIT; BEGIN; ROLLBACK; SELECT 'READY';\n").encode())
        for _ in range(4):
            if line_from(client) == "READY":
                break
        else:
            raise AssertionError("psql did not reach its fixture setup marker")
        assert operation("status")["fault_hits"] == 0, "unarmed COMMIT fired the fault"
        operation("arm")
        create = f"CREATE TABLE public.{table}(value integer);" if case == "provisioning_recovery" else ""
        client.communicate(input=(f"BEGIN; {create} INSERT INTO public.{table} VALUES(23); COMMIT;\n").encode(), timeout=10)
        assert client.returncode != 0, "lost COMMIT reply was reported as success"
        observed = operation("status")
        assert observed["fault_hits"] == 1 and observed["commit_tag"] == "COMMIT"
        assert observed["holding_backend"] and observed["held_fence_observed"]
        target = environment["RPGMIGRATE_TEST_AUTHORITY_TARGET"]
        result = query(cluster, target, f"SELECT value FROM public.{table}; SELECT pg_try_advisory_lock({key});")
        assert result.returncode == 0 and result.stdout.splitlines() == ["23", "f"], "committed row or held old fence missing"
        released = operation("release")
        assert released["released_fence_observed"] and released["backend_connection_closed"] and not released["holding_backend"]
        result = query(cluster, target, f"SELECT pg_try_advisory_lock({key});")
        assert result.returncode == 0 and result.stdout.strip() == "t", "fence remains unavailable after observed release"
        operation("quit")
        helper.communicate(timeout=5)
        assert helper.returncode == 0, "fault controller failed during cleanup"
    finally:
        for process in (client, helper):
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=5)


def self_check(cluster, environment):
    assert "RPGMIGRATE_TEST_AUTHORITY_STANDBY" in environment, "owned standby fixture is missing"
    assert "RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER" in environment, "held COMMIT fault controller is missing"
    assert "RPGMIGRATE_TEST_AUTHORITY_FSYNC_OFF" in environment, "separate fsync-off primary is missing"
    target = environment["RPGMIGRATE_TEST_AUTHORITY_TARGET"]
    restricted = environment["RPGMIGRATE_TEST_AUTHORITY_RESTRICTED"]
    readonly = environment["RPGMIGRATE_TEST_AUTHORITY_READONLY"]
    standby = environment["RPGMIGRATE_TEST_AUTHORITY_STANDBY"]
    result = query(cluster, environment["RPGMIGRATE_TEST_AUTHORITY_FSYNC_OFF"],
                   "SELECT pg_is_in_recovery(); SHOW transaction_read_only; SHOW fsync; CREATE TABLE public.fsync_probe(value integer); INSERT INTO public.fsync_probe VALUES(1);")
    assert result.returncode == 0 and result.stdout.splitlines() == ["f", "off", "off"], "unsafe fixture must be a separate writable fsync-off primary"
    result = query(cluster, target, "SHOW fsync;")
    assert result.returncode == 0 and result.stdout.strip() == "on", "normal fixture durability changed"
    result = query(cluster, target, "CREATE SCHEMA authority_fixture_owned; CREATE TABLE authority_fixture_owned.probe(value integer); GRANT USAGE ON SCHEMA authority_fixture_owned TO e3_restricted; GRANT SELECT, INSERT, UPDATE ON authority_fixture_owned.probe TO e3_restricted;")
    assert result.returncode == 0, "normal target role cannot create preprovisioned objects"
    result = query(cluster, restricted, "SELECT current_user;")
    assert result.returncode == 0 and result.stdout.strip() == "e3_restricted"
    assert query(cluster, restricted, "CREATE SCHEMA authority_denied;").returncode != 0, "restricted role can create metadata"
    result = query(cluster, restricted, """SELECT json_build_array(current_user, pg_get_userbyid(c.relowner),
        pg_has_role(current_user,c.relowner,'MEMBER'),
        has_schema_privilege('authority_fixture_owned','USAGE'),has_schema_privilege('authority_fixture_owned','CREATE'),
        has_table_privilege(c.oid,'SELECT'),has_table_privilege(c.oid,'INSERT'),has_table_privilege(c.oid,'UPDATE'),
        has_table_privilege(c.oid,'DELETE'),has_table_privilege(c.oid,'TRUNCATE'),has_table_privilege(c.oid,'REFERENCES'),has_table_privilege(c.oid,'TRIGGER'),
        has_database_privilege(current_database(),'CREATE'),has_function_privilege('pg_catalog.pg_control_system()','EXECUTE'))
        FROM pg_class c WHERE c.oid='authority_fixture_owned.probe'::regclass;""")
    assert result.returncode == 0
    privileges = json.loads(result.stdout)
    assert privileges == [
        "e3_restricted", "e1_target", False, True, False, True, True, True, False, False, False, False, False, False
    ], "restricted runtime role does not have the exact independent-owner privilege profile"
    assert query(cluster, restricted, "SELECT * FROM pg_catalog.pg_control_system();").returncode != 0
    assert query(cluster, restricted, "CREATE TABLE authority_fixture_owned.denied(value integer);").returncode != 0
    result = query(cluster, restricted, "INSERT INTO authority_fixture_owned.probe VALUES(16); UPDATE authority_fixture_owned.probe SET value=17; SELECT value FROM authority_fixture_owned.probe;")
    assert result.returncode == 0 and result.stdout.strip() == "17", "preprovisioned role cannot perform granted DML"
    result = query(cluster, readonly, "SHOW transaction_read_only;")
    assert result.returncode == 0 and result.stdout.strip() == "on"
    assert query(cluster, readonly, "INSERT INTO authority_fixture_owned.probe VALUES(18);").returncode != 0
    result = query(cluster, standby, "SELECT pg_is_in_recovery(); SHOW transaction_read_only;")
    assert result.returncode == 0 and result.stdout.splitlines() == ["t", "on"], "standby is not a real read-only recovery server"
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        result = query(cluster, standby, "SELECT value FROM authority_fixture_owned.probe;")
        if result.returncode == 0 and result.stdout.strip() == "17":
            break
        time.sleep(0.05)
    else:
        raise AssertionError("standby did not replay the owned primary fixture")
    assert query(cluster, standby, "INSERT INTO authority_fixture_owned.probe VALUES(19);").returncode != 0
    fault_self_check(cluster, environment, "lost_commit_reply", 641709)
    fault_self_check(cluster, environment, "provisioning_recovery", 641710)
    validate_authority_faults(cluster.context)
    return {"authority_fixture": "passed", "restricted_role": True, "readonly": True, "standby_replay": True,
            "preprovisioned_owner": privileges[1], "runtime_user": privileges[0],
            "owner_membership": privileges[2], "control_system_execute": privileges[-1],
            "fsync_off_primary": True, "commit_faults": 2, "held_fences": 2, "released_fences": 2}


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    with OwnedCluster(load_manifest()) as cluster, authority_fixture(cluster) as environment:
        print(json.dumps(self_check(cluster, environment), sort_keys=True))
