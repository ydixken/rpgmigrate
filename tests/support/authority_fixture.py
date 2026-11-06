"""Additional owned PostgreSQL fixtures for target authority tests."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

from db_harness import ADMIN, OwnedCluster, assert_identity, conninfo, psql, quote


@contextmanager
def owned_standby(cluster):
    assert_identity(cluster.context)
    backup = cluster.bindir / "pg_basebackup"
    if not backup.is_file() or not os.access(backup, os.X_OK):
        raise RuntimeError("authority fixture requires pg_basebackup in PG_BINDIR")
    version = subprocess.run([str(backup), "--version"], env=cluster.environment,
                             capture_output=True, text=True, check=True, timeout=5).stdout
    assert re.search(r"\b(\d+(?:\.\d+)+)\b", version).group(1) == cluster.manifest["server_version"]
    assert psql(cluster.context, "postgres", "SELECT count(*) FROM pg_tablespace WHERE pg_tablespace_location(oid) <> '';") == "0"
    root = cluster.directory("authority-standby-", cluster.root)
    data = root / "data"
    socket = cluster.directory("rpg-e3-s-", "/tmp")
    subprocess.run([str(backup), "--dbname", conninfo(cluster.context, "postgres", ADMIN),
                    "-D", str(data), "--write-recovery-conf", "--wal-method=stream", "--checkpoint=fast", "--no-password"],
                   env=cluster.environment, capture_output=True, check=True, timeout=45)
    context = {**cluster.context, "socket_dir": str(socket)}
    log_path = root / "postgres.log"
    log_path.touch(mode=0o600)
    with log_path.open("wb") as log:
        process = subprocess.Popen([
            str(cluster.bindir / "postgres"), "-D", str(data), "-k", str(socket), "-p", str(cluster.port),
            "-c", "listen_addresses=", "-c", "unix_socket_permissions=0700", "-c", "hot_standby=on",
            "-c", "fsync=on",
            "-c", "cluster_name=" + cluster.token, "-c", "max_connections=30", "-c", "shared_buffers=16MB",
            "-c", "log_min_error_statement=panic", "-c", "log_error_verbosity=terse", "-c", "log_statement=none"],
            env=cluster.environment, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("owned standby exited during startup")
                try:
                    values = json.loads(psql(context, "postgres", "SELECT json_build_array(pg_is_in_recovery(), current_setting('transaction_read_only'), current_setting('listen_addresses'), current_setting('data_directory'), current_setting('cluster_name'), current_setting('fsync'));"))
                    if values == [True, "on", "", str(data), cluster.token, "on"]:
                        break
                    raise AssertionError("owned standby identity or settings differ")
                except RuntimeError:
                    time.sleep(0.05)
            else:
                raise RuntimeError("owned standby readiness deadline exceeded")
            assert data.stat().st_mode & 0o777 == 0o700
            assert (socket / f".s.PGSQL.{cluster.port}").stat().st_mode & 0o777 == 0o700
            yield context
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


@contextmanager
def authority_fixture(cluster):
    context = cluster.context
    assert_identity(context)
    psql(context, "postgres", "CREATE ROLE e3_restricted LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;")
    psql(context, "postgres", "CREATE DATABASE e3_authority OWNER e1_target TEMPLATE template0 ENCODING 'UTF8';")
    psql(context, "postgres", f"COMMENT ON DATABASE e3_authority IS {quote(context['token'])};")
    psql(context, "e3_authority", "REVOKE CREATE ON DATABASE e3_authority FROM PUBLIC; REVOKE CREATE ON SCHEMA public FROM PUBLIC; GRANT USAGE, CREATE ON SCHEMA public TO e1_target;")
    psql(context, "e3_authority", "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC, e3_restricted;")
    target = conninfo(context, "e3_authority", "e1_target")
    with owned_standby(cluster) as standby, OwnedCluster(cluster.manifest, fsync=False) as unsafe:
        psql(unsafe.context, "postgres", "CREATE DATABASE e3_authority OWNER e1_target TEMPLATE template0 ENCODING 'UTF8';")
        psql(unsafe.context, "postgres", f"COMMENT ON DATABASE e3_authority IS {quote(unsafe.token)};")
        yield {
            "RPGMIGRATE_TEST_AUTHORITY_TARGET": target,
            "RPGMIGRATE_TEST_AUTHORITY_ADMIN": conninfo(context, "e3_authority", ADMIN),
            "RPGMIGRATE_TEST_AUTHORITY_RESTRICTED": conninfo(context, "e3_authority", "e3_restricted"),
            "RPGMIGRATE_TEST_AUTHORITY_READONLY": target + " options='-c default_transaction_read_only=on'",
            "RPGMIGRATE_TEST_AUTHORITY_STANDBY": conninfo(standby, "e3_authority", "e1_target"),
            "RPGMIGRATE_TEST_AUTHORITY_FAULT_HELPER": str(Path(__file__).with_name("authority_fault.py")),
            "RPGMIGRATE_TEST_AUTHORITY_FSYNC_OFF": conninfo(unsafe.context, "e3_authority", "e1_target"),
        }


def validate_authority_faults(context):
    from authority_fault import FAULT_CASES

    root = Path(context["root"])
    assert sorted(path.name for path in root.glob("authority-fault-*.json")) == sorted(
        f"authority-fault-{case}.json" for case in FAULT_CASES), "missing or unexpected authority fault receipts"
    for case in FAULT_CASES:
        value = json.loads((root / f"authority-fault-{case}.json").read_text())
        assert value.get("status") == "observed", "pending or ignored authority fault receipt"
        assert value["owner"] == context["token"] and value["case"] == case
        assert type(value["fault_hits"]) is int and value["fault_hits"] == 1 and value["commit_tag"] == "COMMIT", "COMMIT reply fault did not fire"
        assert type(value["backend_pid"]) is int and value["backend_pid"] > 0
        assert value["held_fence_observed"] is True and value["released_fence_observed"] is True, "backend fence lifecycle was not observed"
        assert value["backend_connection_closed"] is True and value["holding_backend"] is False
