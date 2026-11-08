"""Private PostgreSQL fixtures for executable integration tests."""

if not __debug__:
    raise RuntimeError("DB harness requires Python assertions; remove -O/-OO and unset PYTHONOPTIMIZE")

import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import struct
import subprocess
import tempfile
import time

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "e1"
ADMIN = "e1_admin"
SETTINGS = """SET DateStyle = 'ISO,YMD'; SET IntervalStyle = 'postgres';
SET TimeZone = 'UTC'; SET extra_float_digits = 3; SET bytea_output = 'hex';
SET row_security = off; SET client_encoding = 'UTF8';"""


def clean_environment(environment):
    return {key: value for key, value in environment.items()
            if not key.upper().startswith(("PG", "RPGMIGRATE"))}


def expected_version(environment, version, number):
    keys = ("RPGMIGRATE_TEST_EXPECTED_PG_VERSION", "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM")
    if not any(key in environment for key in keys):
        return version, number
    if not all(key in environment for key in keys):
        raise ValueError("both expected PostgreSQL version variables are required")
    version, numeric = (environment[key] for key in keys)
    match = re.fullmatch(r"([1-9][0-9]*)\.(0|[1-9][0-9]*)", version)
    if not match or not re.fullmatch(r"[1-9][0-9]*", numeric):
        raise ValueError("expected PostgreSQL version must be major.patch and its numeric version")
    major, patch = (int(value) for value in match.groups())
    number = int(numeric)
    if major < 10 or patch >= 10000 or number != major * 10000 + patch:
        raise ValueError("expected PostgreSQL version string and number disagree")
    return version, number


def load_manifest():
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    manifest["server_version"], manifest["server_version_num"] = expected_version(
        os.environ, manifest["server_version"], manifest["server_version_num"])
    return manifest


def module_cases(manifest, profile):
    if profile == "Schema":
        return ["schema::tests::live_pre_data_guard_refuses_schema_drift_without_target_mutation"]
    if profile == "Clone":
        return [
            "snapshot::clone_tests::live_clone_snapshot_is_shared_and_exporter_lifetime_is_required",
            "copy::live_tests::live_parallel_clone_preserves_snapshot_physical_heaps_identity_generated_and_wide_rows",
            "copy::live_tests::live_clone_late_target_error_rolls_back_truncate_copy_and_marker",
            "copy::live_tests::live_clone_oversize_row_refuses_before_table_commit",
            "copy::live_tests::live_clone_established_empty_inventory_returns_zero_receipt",
            "copy::live_tests::live_clone_rejects_replaced_snapshot_in_same_generation_before_copy",
            "apply::checkpoint::clone_marker_tests::live_markers_commit_atomically_and_refuse_stale_generation_and_foreign_metadata",
        ]
    if profile == "Transport":
        return manifest["transport_cases"]
    if profile == "SourceSnapshot":
        from source_snapshot_fixture import snapshot_cases
        return snapshot_cases()
    if profile == "Catalog":
        catalog = json.loads(Path(__file__).with_name("catalog_cases.json").read_text())
        assert isinstance(catalog, list) and catalog, "missing catalog test inventory"
        return catalog
    if profile == "SourceCatalog":
        source = json.loads(Path(__file__).with_name("source_catalog_cases.json").read_text())
        assert isinstance(source, list) and source, "missing source catalog test inventory"
        return source
    if profile not in ("Prepared", "Authority", "Replication", "Engine"):
        raise ValueError("unknown module test profile")
    if profile == "Authority":
        authority = json.loads(Path(__file__).with_name("authority_cases.json").read_text())
        assert isinstance(authority, list) and authority, "missing authority test inventory"
        return authority
    if profile == "Replication":
        replication = json.loads(Path(__file__).with_name("replication_cases.json").read_text())
        assert isinstance(replication, list) and replication, "missing replication test inventory"
        return replication
    prepared = json.loads(Path(__file__).with_name("prepared_cases.json").read_text())
    assert isinstance(prepared, list) and prepared, "missing prepared test inventory"
    prepared += [
        "pg::pipeline_live_tests::pipeline_validates_ordered_rows_and_every_terminal_result",
        "pg::pipeline_live_tests::pipeline_sql_and_callback_errors_drain_through_sync",
        "pg::pipeline_live_tests::pipeline_drop_deadline_and_limits_fail_closed",
    ]
    if profile == "Prepared":
        return prepared
    return (manifest["transport_cases"] + manifest["copy_cases"] + prepared
            + module_cases(manifest, "Authority") + module_cases(manifest, "Replication")
            + ["compare::tests::live_canonical_rows_preserve_only_generated_identity_null_and_duplicates"])


def run_command(command, *, environment, cwd, timeout, capture_output=False):
    process = subprocess.Popen(command, env=environment, cwd=cwd, text=True,
                               stdout=subprocess.PIPE if capture_output else None,
                               stderr=subprocess.PIPE if capture_output else None,
                               start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
    except BaseException:
        # Cargo, Rust tests, and the Python runner must stop together before fixture teardown.
        for sig, grace in ((signal.SIGTERM, 5), (signal.SIGKILL, 5)):
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                pass
            try:
                process.communicate(timeout=grace)
            except subprocess.TimeoutExpired:
                continue
            deadline = time.monotonic() + grace
            stopped = False
            while time.monotonic() < deadline:
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    stopped = True
                    break
                except PermissionError:
                    # macOS can report EPERM while the last owned group member exits.
                    pass
                time.sleep(0.01)
            if stopped:
                break
        raise


def validate_libtest_output(expected, output):
    actual = re.findall(r"^test (\S+) \.\.\. (ok|FAILED|ignored[^\n]*)$", output, flags=re.MULTILINE)
    assert expected and len(expected) == len(set(expected)), "empty or duplicate expected module tests"
    assert len(actual) == len(expected), "missing, duplicate, or zero module tests"
    assert {name for name, _ in actual} == set(expected), "wrong module test selection"
    assert all(status == "ok" for _, status in actual), "failed or ignored module tests"


def validate_success_output(output, state):
    record = json.loads(output)
    assert set(record) == {"command", "status", "run", "state_format_version", "tables", "rows", "bytes"}, "unexpected success output fields"
    assert record["command"] == "copy table-data" and record["status"] == "complete", "success output is not COPY completion"
    assert record["run"] == state["run"], "success output run differs from native receipt"
    assert record["state_format_version"] == state["format_version"], "success output state version differs"
    assert {key: record[key] for key in ("tables", "rows", "bytes")} == state["copy_complete"], "success output counts differ from native receipt"


def validate_report(manifest, report):
    assert report["server_version_num"] == manifest["server_version_num"], "wrong server version"
    expected = {case["id"]: case for case in manifest["cases"]}
    actual = report["cases"]
    ids = [case["id"] for case in actual]
    assert expected and len(expected) == len(manifest["cases"]), "empty or duplicate expected cases"
    assert len(ids) == len(set(ids)) and set(ids) == set(expected), "missing, duplicate, or unexpected cases"
    for case in actual:
        want = expected[case["id"]]
        assert case["status"] == "passed", f"case failed or skipped: {case['id']}"
        if want["outcome"] in ("success", "failure"):
            assert (case["returncode"] == 0) == (want["outcome"] == "success"), f"wrong exit: {case['id']}"
        if want.get("fault"):
            assert case.get("fault_hits", 0) > 0, f"fault not observed: {case['id']}"


def find_bindir(environment):
    if environment.get("PG_BINDIR"):
        directory = Path(environment["PG_BINDIR"])
    else:
        config = environment.get("PG_CONFIG") or shutil.which("pg_config", path=environment.get("PATH"))
        if config:
            result = subprocess.run([config, "--bindir"], env=clean_environment(environment),
                                    capture_output=True, text=True, timeout=10, check=True)
            directory = Path(result.stdout.strip())
        else:
            postgres = shutil.which("postgres", path=environment.get("PATH"))
            if not postgres:
                raise RuntimeError("PostgreSQL missing: set PG_BINDIR or PG_CONFIG to a complete server bundle")
            directory = Path(postgres).parent
    directory = directory.resolve(strict=True)
    for name in ("postgres", "initdb", "psql"):
        if not (directory / name).is_file() or not os.access(directory / name, os.X_OK):
            raise RuntimeError(f"incomplete PostgreSQL bundle: missing {name}; set PG_BINDIR")
    return directory


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def conninfo(context, database, role, socket_dir=None, application_name="rpgmigrate_e1_fixture", include_passfile=True):
    values = {"host": socket_dir or context["socket_dir"], "port": context["port"],
              "dbname": database, "user": role, "connect_timeout": 3,
              "sslmode": "disable", "application_name": application_name}
    if include_passfile:
        values["passfile"] = str(Path(context["root"]) / "empty.pgpass")
    return " ".join(key + "='" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
                    for key, value in values.items())


def psql(context, database, sql, *, role=ADMIN, canonical=True, raw=False):
    environment = clean_environment(os.environ)
    environment["HOME"] = str(Path(context["root"]) / "home")
    result = subprocess.run([str(Path(context["bindir"]) / "psql"), "-X", "-w", "-qAt",
                             "-v", "ON_ERROR_STOP=1", "--dbname", conninfo(context, database, role)],
                            input=(SETTINGS if canonical else "") + "\n" + sql, env=environment,
                            text=True, encoding="utf-8", capture_output=True, timeout=30)
    if result.returncode:
        # Fixture SQL can contain row values; keep those out of failure diagnostics.
        raise RuntimeError(f"fixture psql failed (exit {result.returncode})")
    return result.stdout if raw else result.stdout.strip()


class OwnedCluster:
    def __init__(self, manifest, *, fsync=True, logical=False, tcp=False, logical_slots=4):
        if any(type(value) is not bool for value in (fsync, logical, tcp)):
            raise ValueError("fixture fsync, logical, and tcp settings must be boolean")
        if type(logical_slots) is not int or not 1 <= logical_slots <= 64:
            raise ValueError("fixture logical_slots must be an integer from 1 through 64")
        self.manifest = manifest
        self.fsync = fsync
        self.logical = logical
        self.tcp = tcp
        self.logical_slots = logical_slots
        self.bindir = find_bindir(os.environ)
        self.environment = clean_environment(os.environ)
        self.token = secrets.token_hex(16)
        self.auth_password = secrets.token_urlsafe(24)
        self.process = None
        self.log = None
        self.owned = []

    def directory(self, prefix, parent=None):
        path = Path(tempfile.mkdtemp(prefix=prefix, dir=parent)).resolve()
        path.chmod(0o700)
        marker = path / ".e1-owner"
        marker.write_text(self.token, encoding="ascii")
        marker.chmod(0o600)
        info = path.stat()
        self.owned.append((path, info.st_dev, info.st_ino))
        return path

    def __enter__(self):
        try:
            self.root = self.directory("rpgmigrate-e1-")
            self.socket = self.directory("rpg-e1-", "/tmp")
            self.data = self.root / "data"
            (self.root / "home").mkdir(mode=0o700)
            (self.root / "empty.pgpass").touch(mode=0o600)
            self.environment["HOME"] = str(self.root / "home")
            self.environment["TMPDIR"] = str(self.root)
            self.port = 20000 + secrets.randbelow(40000)
            self.context = {"root": str(self.root), "socket_dir": str(self.socket), "port": self.port,
                            "bindir": str(self.bindir), "token": self.token}
            versions = []
            for name in ("postgres", "initdb", "psql"):
                output = subprocess.run([str(self.bindir / name), "--version"], env=self.environment,
                                        capture_output=True, text=True, check=True, timeout=10).stdout
                versions.append(re.search(r"\b(\d+(?:\.\d+)+)\b", output).group(1))
            assert versions == [self.manifest["server_version"]] * 3, "server/client utility version mismatch"
            subprocess.run([str(self.bindir / "initdb"), "-D", str(self.data), "-U", ADMIN,
                            "--locale=C", "--encoding=UTF8", "--auth-local=trust", "--auth-host=reject"],
                           env=self.environment, check=True, capture_output=True, timeout=45)
            hba = self.data / "pg_hba.conf"
            network_auth = "host all e1_source,e1_target 0.0.0.0/0 scram-sha-256\n" if self.tcp else ""
            hba.write_text("local all e1_auth scram-sha-256\n" + network_auth + hba.read_text())
            self.log = (self.root / "postgres.log").open("wb")
            logical_options = (["-c", "wal_level=logical", "-c", "max_wal_senders=4",
                                "-c", f"max_replication_slots={self.logical_slots}", "-c", "wal_sender_timeout=30s"]
                               if self.logical else [])
            self.process = subprocess.Popen([
                str(self.bindir / "postgres"), "-D", str(self.data), "-k", str(self.socket),
                "-p", str(self.port), "-c", "listen_addresses=" + ("0.0.0.0" if self.tcp else ""), "-c", "unix_socket_permissions=0700",
                "-c", "cluster_name=" + self.token, "-c", "max_connections=30",
                "-c", "shared_buffers=16MB", "-c", "log_min_error_statement=panic",
                "-c", "fsync=" + ("on" if self.fsync else "off"),
                "-c", "log_error_verbosity=terse", "-c", "log_statement=none", *logical_options],
                env=self.environment, stdout=self.log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError("owned PostgreSQL exited during startup")
                try:
                    identity = psql(self.context, "postgres", "SELECT current_setting('cluster_name');")
                    if identity == self.token:
                        break
                    raise RuntimeError("fixture server identity mismatch")
                except RuntimeError:
                    time.sleep(0.05)
            else:
                raise RuntimeError("owned PostgreSQL readiness deadline exceeded")
            assert (self.data.stat().st_mode & 0o777) == 0o700
            assert (self.socket.stat().st_mode & 0o777) == 0o700
            assert stat.S_IMODE((self.socket / f".s.PGSQL.{self.port}").stat().st_mode) == 0o700
            settings = psql(self.context, "postgres", "SELECT json_build_array(current_setting('listen_addresses'), current_setting('data_directory'), current_setting('port'), current_setting('fsync'));")
            assert json.loads(settings) == ["0.0.0.0" if self.tcp else "", str(self.data), str(self.port), "on" if self.fsync else "off"], "fixture isolation settings differ"
            if self.logical:
                settings = psql(self.context, "postgres", "SELECT json_build_array(current_setting('wal_level'), current_setting('max_wal_senders'), current_setting('max_replication_slots'), current_setting('wal_sender_timeout'));")
                assert json.loads(settings) == ["logical", "4", str(self.logical_slots), "30s"], "logical fixture settings differ"
            self.context["server_version_num"] = int(psql(self.context, "postgres", "SHOW server_version_num;"))
            assert self.context["server_version_num"] == self.manifest["server_version_num"], "unexpected server version"
            password = " PASSWORD " + quote(self.auth_password) if self.tcp else ""
            encryption = "SET password_encryption='scram-sha-256'; " if self.tcp else ""
            psql(self.context, "postgres", encryption +
                 f"CREATE ROLE e1_source LOGIN{password}; CREATE ROLE e1_target LOGIN{password};")
            if self.tcp:
                self.context["tcp_password"] = self.auth_password
            psql(self.context, "postgres", "SET password_encryption='scram-sha-256'; "
                 f"CREATE ROLE e1_auth LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD {quote(self.auth_password)};")
            assert psql(self.context, "postgres", "SELECT rolpassword LIKE 'SCRAM-SHA-256$%' FROM pg_authid WHERE rolname='e1_auth';") == "t"
            for role, date, interval, zone in (("e1_source", "SQL,DMY", "sql_standard", "Pacific/Auckland"),
                                                ("e1_target", "German,DMY", "iso_8601", "America/Los_Angeles")):
                psql(self.context, "postgres", f"ALTER ROLE {role} SET DateStyle={quote(date)}; ALTER ROLE {role} SET IntervalStyle={quote(interval)}; ALTER ROLE {role} SET TimeZone={quote(zone)};")
            self.context_path = self.root / "context.json"
            self.context_path.write_text(json.dumps(self.context), encoding="utf-8")
            self.context_path.chmod(0o600)
            return self
        except BaseException:
            self.close()
            raise

    def close(self):
        if self.process is not None and self.process.poll() is None:
            # A live child handle identifies our postmaster; never trust a PID file.
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log is not None:
            self.log.close()
        for path, device, inode in reversed(self.owned):
            info = path.lstat()
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
                    or (info.st_dev, info.st_ino) != (device, inode)
                    or (path / ".e1-owner").read_text(encoding="ascii") != self.token):
                raise RuntimeError("refusing cleanup: owned directory identity changed")
            shutil.rmtree(path)
        self.owned.clear()

    def __exit__(self, *_):
        self.close()


def assert_identity(context):
    assert context["port"] != 5432 and Path(context["socket_dir"]).is_absolute()
    assert (Path(context["root"]) / ".e1-owner").read_text() == context["token"]
    assert psql(context, "postgres", "SELECT current_setting('cluster_name');") == context["token"]


def databases(context, case_id, *, empty=False, encoding="UTF8"):
    assert_identity(context)
    suffix = re.sub(r"[^a-z0-9]", "_", case_id)[:40]
    names = ("e1_s_" + suffix, "e1_t_" + suffix)
    encodings = (encoding, encoding) if isinstance(encoding, str) else encoding
    for name, role, database_encoding in zip(names, ("e1_source", "e1_target"), encodings):
        psql(context, "postgres", f"CREATE DATABASE {identifier(name)} OWNER {role} TEMPLATE template0 ENCODING {quote(database_encoding)};")
        psql(context, "postgres", f"COMMENT ON DATABASE {identifier(name)} IS {quote(context['token'])};")
        if not empty:
            psql(context, name, (FIXTURES / "schema.sql").read_text(), role=role)
    if not empty:
        psql(context, names[0], (FIXTURES / "seed.sql").read_text(), role="e1_source")
    return names


def inventory(context, database):
    schemas = json.loads(psql(context, database, """SELECT COALESCE(json_agg(nspname ORDER BY nspname COLLATE "C"), '[]')
      FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema';"""))
    relations = json.loads(psql(context, database, """SELECT COALESCE(json_agg(r ORDER BY r.schema COLLATE "C", r.name COLLATE "C"), '[]') FROM (
      SELECT n.nspname AS schema, c.relname AS name, c.relkind AS kind, c.relpersistence AS persistence,
        (SELECT json_agg(json_build_array(a.attname, pg_catalog.format_type(a.atttypid,a.atttypmod)) ORDER BY a.attnum)
         FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' AND c.relkind IN ('r','p','v','m','f')) r;"""))
    return {"schemas": schemas, "relations": relations}


def fingerprint(context, database, expected, *, empty=False):
    actual = inventory(context, database)
    assert actual == {key: expected[key] for key in ("schemas", "relations")}, "relation/column/type inventory mismatch"
    result = {}
    for relation in expected["relations"]:
        name = identifier(relation["schema"]) + "." + identifier(relation["name"])
        fields = []
        for column, kind in relation["columns"]:
            column = identifier(column)
            value = column if kind == "bytea" else f"convert_to({column}::text, 'UTF8')"
            fields.append(f"encode({value}, 'hex')")
        raw = psql(context, database, f"SELECT json_build_array({','.join(fields)}) FROM {name};")
        rows = []
        for line in raw.splitlines():
            row = bytearray(struct.pack("!I", len(fields)))
            for field in json.loads(line):
                if field is None:
                    row += b"N"
                else:
                    value = bytes.fromhex(field)
                    row += b"V" + struct.pack("!Q", len(value)) + value
            rows.append(bytes(row))
        count = int(psql(context, database, f"SELECT count(*) FROM {name};"))
        want = 0 if empty else expected["row_counts"][relation["name"]]
        assert count == len(rows) == want, f"row count mismatch for {name}"
        digest = hashlib.sha256()
        for row in sorted(rows):
            digest.update(struct.pack("!Q", len(row)))
            digest.update(row)
        result[relation["name"]] = {"rows": count, "sha256": digest.hexdigest()}
    assert result, "empty fingerprint inventory"
    return result


def assert_equal(context, source, target, expected):
    source_digest = fingerprint(context, source, expected)
    assert source_digest == fingerprint(context, target, expected), "ordered full-content digest mismatch"
    return source_digest


def reference_copy(context, source, target, expected):
    for relation in expected["relations"]:
        table = identifier(relation["schema"]) + "." + identifier(relation["name"])
        columns = ",".join(identifier(column[0]) for column in relation["columns"])
        sql = f"COPY {table} ({columns}) TO STDOUT;"
        data = psql(context, source, sql, raw=True)
        psql(context, target, f"COPY {table} ({columns}) FROM STDIN;\n" + data + "\\.\n")
