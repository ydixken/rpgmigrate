#!/usr/bin/env python3
"""Qualify plain clone in disposable PostgreSQL containers, never operator runtime."""

import argparse
import ctypes
from datetime import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import tarfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/clone"
DIAGNOSTIC_ROOT = ROOT / "target/e2e/failures"
FIXTURE_SECRETS = set()
PAIRS = ("16-16", "18-18", "16-18")
SETTINGS = "SET client_encoding='UTF8'; SET DateStyle='ISO,YMD'; SET IntervalStyle='postgres'; SET TimeZone='UTC'; SET extra_float_digits=3; SET bytea_output='hex';"


def load_manifest(path=FIXTURES / "cases.json"):
    manifest = json.loads(Path(path).read_text())
    if (manifest.get("version") != 1 or tuple(manifest.get("pairs", ())) != PAIRS
            or not manifest.get("tables") or not manifest.get("cases")
            or len(set(manifest["tables"])) != len(manifest["tables"])
            or len(set(manifest["cases"])) != len(manifest["cases"])):
        raise RuntimeError("clone fixture inventory differs or is empty")
    if not 0 < manifest.get("max_rows", 0) <= 100000 or not 0 < manifest.get("max_bytes", 0) <= 67108864:
        raise RuntimeError("clone fixture bounds are invalid")
    rows = manifest.get("row_counts", {})
    if (not isinstance(rows, dict) or set(rows) != set(manifest["tables"])
            or any(type(count) is not int or not 0 <= count <= manifest["max_rows"] for count in rows.values())
            or not sum(rows.values())):
        raise RuntimeError("physical fixture row inventory is absent or invalid")
    return manifest


def validate_fixture_rows(actual, manifest):
    if actual != manifest["row_counts"]:
        raise RuntimeError("physical fixture row coverage differs from its reviewed baseline")


def row_fingerprint(rows, max_rows, max_bytes):
    hashes = []
    size = 0
    for row in rows:
        size += len(row)
        if len(hashes) >= max_rows or size > max_bytes:
            raise RuntimeError("independent oracle exceeded its bound")
        hashes.append(hashlib.sha256(row).digest())
    return {"rows": len(hashes), "sha256": hashlib.sha256(b"".join(sorted(hashes))).hexdigest()}


def schema_fingerprint(columns, obligations, unvalidated_not_null):
    if unvalidated_not_null != b"0":
        raise RuntimeError("independent oracle refuses unvalidated NOT NULL constraints")
    return hashlib.sha256(columns + obligations).hexdigest()


def require_equal(source, target):
    if not source or source != target:
        fingerprints = json.dumps({"source": source, "target": target}, sort_keys=True).encode()
        raise SubprocessFailure(private_failure("independent-oracle", None, fingerprints, None, sys._getframe(1)))


def require_failure(status, output, reason, observed=True):
    if status == 0 or not observed or reason.lower() not in output.lower():
        raise RuntimeError("required refusal reason or observed fault is absent")


def validate_runtime(report):
    if report.get("pair") not in PAIRS:
        raise RuntimeError("wrong version pair")
    source, target = report["pair"].split("-")
    versions = report.get("server_versions", [])
    if len(versions) != 2 or any(not value.startswith(major + ".") for value, major in zip(versions, (source, target))):
        raise RuntimeError("actual server version mismatch")
    clients = report.get("client_versions", {})
    if set(clients) != {"pg_dump", "pg_restore", "psql"} or any(not value.startswith(target + ".") for value in clients.values()):
        raise RuntimeError("actual client version mismatch")
    if not isinstance(report.get("linked_libpq"), int) or report["linked_libpq"] // 10000 not in (16, 18):
        raise RuntimeError("actual linked libpq version missing or unsupported")


def validate_receipt(report, manifest, *, require_reports=True):
    validate_runtime(report)
    if require_reports:
        validation = report.get("report_validation", {})
        records = validation.get("reports", [])
        mode = validation.get("mode")
        if mode not in ("local_m2_contract", "operator_go_parse_report") or len(records) != 6:
            raise RuntimeError("actual producer report validation is absent or incomplete")
        commands = []
        for record in records:
            binding = record.get("binding", {})
            command = binding.get("command")
            if command not in ("clone", "list progress") or not record.get("raw_json"):
                raise RuntimeError("trusted producer binding or raw report is absent")
            validate_operator_report(record["raw_json"], binding, command, command == "clone")
            if record.get("probe_checked") is not (mode == "operator_go_parse_report"):
                raise RuntimeError("selected authoritative probe coverage is absent")
            commands.append(command)
        if commands.count("clone") != 4 or commands.count("list progress") != 2:
            raise RuntimeError("producer command report coverage differs")
    cases = report.get("cases", {})
    if set(cases) != set(manifest["cases"]) or any(value.get("observed") is not True for value in cases.values()):
        raise RuntimeError("required observed case inventory differs")


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RuntimeError("duplicate report field")
        result[key] = value
    return result


def validate_operator_report(raw, binding, command, completed, *, phases=("planning", "copying", "finalizing", "completed")):
    report = json.loads(raw, object_pairs_hook=unique_object)
    keys = {"protocol_version", "engine_version", "command", "capabilities", "work", "phase", "copy"}
    capability = {"clone": "clone.single_db", "list progress": "progress.v1"}[command]
    if (not isinstance(report, dict) or set(report) != keys or type(report["protocol_version"]) is not int or report["protocol_version"] != 1
            or not isinstance(report["engine_version"], str) or not report["engine_version"]
            or binding["command"] != command or report["command"] != command
            or report["work"] != binding["work"] or type(report["work"].get("checkpoint_generation")) is not int or report["capabilities"] != [capability]
            or report["phase"] not in phases
            or completed and report["phase"] != "completed"):
        raise RuntimeError("M2 report envelope differs from the trusted caller contract")
    counters = report["copy"]
    if not isinstance(counters, dict) or set(counters) != {"tables", "indexes", "bytes"}:
        raise RuntimeError("M2 report lacks required measured counter groups")
    for value in counters.values():
        if (not isinstance(value, dict) or not set(value).issubset({"total", "done"})
                or any(type(number) is not int or not 0 <= number <= 2**63 - 1 for number in value.values())
                or "total" in value and "done" in value and value["done"] > value["total"]
                or completed and (set(value) != {"total", "done"} or value["total"] != value["done"])):
            raise RuntimeError("M2 report completion counters are absent or inconsistent")
    return report


class SubprocessFailure(RuntimeError):
    def __init__(self, metadata):
        super().__init__("owned harness subprocess failed")
        self.metadata = metadata


def private_failure(kind, returncode, stderr, environment, frame, line=None):
    identifier = uuid.uuid4().hex
    metadata = {"command_kind": kind, "returncode": returncode, "diagnostic_id": identifier,
                "callsite": {"file": Path(frame.f_code.co_filename).name,
                             "function": frame.f_code.co_name, "line": line or frame.f_lineno}, "retained": False}
    values = environment if environment is not None else os.environ
    secrets = set(FIXTURE_SECRETS)
    secrets.update(value for key, value in values.items() if value and ("PASSWORD" in key or key.endswith("_PGURI")))
    redacted = stderr or b""
    for value in sorted(secrets, key=len, reverse=True):
        redacted = redacted.replace(value.encode(), b"[redacted]")
    redacted = re.sub(rb"postgres(?:ql)?://[^\s\"'<>]+", b"[redacted-uri]", redacted, flags=re.IGNORECASE)
    redacted = re.sub(rb"\bpassword\s*=\s*(?:'(?:\\.|[^'])*'|\"(?:\\.|[^\"])*\"|[^\s;]+)", b"password=[redacted]", redacted, flags=re.IGNORECASE)
    try:
        DIAGNOSTIC_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
        if DIAGNOSTIC_ROOT.is_symlink():
            raise OSError("diagnostic directory is a symlink")
        DIAGNOSTIC_ROOT.chmod(0o700)
        metadata["retained"] = True
        for suffix, contents in (("stderr", redacted), ("json", json.dumps(metadata, sort_keys=True).encode())):
            descriptor = os.open(DIAGNOSTIC_ROOT / (identifier + "." + suffix), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(contents)
                stream.flush()
                os.fsync(stream.fileno())
    except OSError:
        metadata["retained"] = False
    return metadata


def run(command, *, environment=None, input=None, timeout=300):
    caller = sys._getframe(1)
    name = Path(command[0]).name
    kind = name if name in ("psql", "pg_dump", "pg_restore", "cargo", "ldd", "report-probe", "native-report-probe") else "subprocess"
    if name in ("podman", "docker") and len(command) > 1 and command[1] in ("build", "create", "start", "run", "network", "image", "pull", "exec"):
        kind = "container-" + command[1]
    try:
        result = subprocess.run(command, env=environment, input=input, capture_output=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as error:
        metadata = private_failure(kind, None, getattr(error, "stderr", None) or str(error).encode(), environment, caller)
        raise SubprocessFailure(metadata) from None
    if result.returncode:
        raise SubprocessFailure(private_failure(kind, result.returncode, result.stderr, environment, caller))
    return result.stdout


def wait_for(probe, description, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = probe()
        if value:
            return value
        time.sleep(0.02)
    raise RuntimeError("required observation absent: " + description)


def tool_version(path):
    value = run([str(path), "--version"]).decode()
    match = re.search(r"PostgreSQL\)?\s+(\d+\.\d+)", value)
    if not match:
        raise RuntimeError("actual PostgreSQL utility version missing")
    return match[1]


def quote(value):
    return '"' + value.replace('"', '""') + '"'


class Database:
    def __init__(self, host, bindir, environment):
        self.host, self.bindir, self.environment = host, bindir, environment
        self.application = "rpgmigrate_m2_" + uuid.uuid4().hex

    def connection(self, name):
        return f"host={self.host} dbname={name} user=postgres sslmode=disable connect_timeout=3 application_name={self.application}"

    def sql(self, name, sql):
        return run([str(self.bindir / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname", self.connection(name)],
                   environment=self.environment, input=(SETTINGS + sql).encode())

    def create(self, name, fixture=False):
        self.sql("postgres", "CREATE DATABASE " + quote(name))
        if fixture:
            self.sql(name, (FIXTURES / "schema.sql").read_text())
            manifest = load_manifest()
            counts = {table: int(self.sql(name, f"SELECT count(*) FROM ONLY {quote('M2 space')}.{quote(table)};").strip()) for table in manifest["tables"]}
            validate_fixture_rows(counts, manifest)
        return self.connection(name)

    def catalog_signature(self, name):
        data = self.sql(name, "SELECT n.oid,n.nspname,c.oid,c.relname,c.relkind,c.relowner FROM pg_catalog.pg_namespace n LEFT JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') ORDER BY n.oid,c.oid;")
        return hashlib.sha256(data).hexdigest()

    def completion(self, name, state, manifest):
        native = json.loads((state / "manifest.json").read_text())
        provenance = native["target_provenance"]
        if len(provenance) != 1 or provenance[0].get("phase") != "confirmed":
            raise RuntimeError("confirmed target provenance is absent")
        record = provenance[0]["record"]
        binding = record["intent"]["binding"]
        table = quote(binding["schema_name"]) + "." + quote(binding["table_name"])
        marker = quote(binding["schema_name"]) + '."copy_done"'
        generation = native["clone_state"]["plan"]["generation"]
        schema_literal = binding["schema_name"].replace("'", "''")
        table_literal = binding["table_name"].replace("'", "''")
        actual = self.sql(name, f"SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='{schema_literal}' AND c.relname='{table_literal}';").decode().strip()
        if actual != str(record["table_oid"]):
            raise RuntimeError("durable target receipt object identity differs")
        control = json.loads(self.sql(name, f"SELECT clone_state FROM ONLY {table};").decode())
        markers = self.sql(name, f"SELECT count(*) FROM ONLY {marker} WHERE copy_generation={generation};").decode().strip()
        if control.get("phase") != "complete" or control.get("generation") != generation or control.get("table_count") != len(manifest["tables"]) or markers != str(len(manifest["tables"])):
            raise RuntimeError("independent completed target control/marker receipt is incomplete")
        marker_rows = self.sql(name, f"SELECT row_to_json(m)::text,m.xmin::text,m.ctid::text FROM ONLY {marker} m ORDER BY source_table;")
        return {"phase": control["phase"], "generation": generation, "markers": int(markers),
                "marker_sha256": hashlib.sha256(marker_rows).hexdigest()}

    def fingerprint(self, name, manifest):
        inventory = self.sql(name, "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='M2 space' AND c.relkind='r' ORDER BY c.relname;").decode().splitlines()
        if inventory != sorted(manifest["tables"]):
            raise RuntimeError("independent table inventory differs")
        result = {}
        for table in manifest["tables"]:
            bounds = self.sql(name, f"SELECT count(*),coalesce(sum(octet_length(row_to_json(t)::text))*2,0) FROM ONLY {quote('M2 space')}.{quote(table)} t;").decode().strip().split("|")
            if len(bounds) != 2 or int(bounds[0]) > manifest["max_rows"] or int(bounds[1]) > manifest["max_bytes"]:
                raise RuntimeError("independent SQL row/byte bound exceeded")
            rows = self.sql(name, f"COPY (SELECT encode(convert_to(row_to_json(t)::text,'UTF8'),'hex') FROM ONLY {quote('M2 space')}.{quote(table)} t) TO STDOUT;")
            result[table] = row_fingerprint(rows.splitlines(keepends=True), manifest["max_rows"], manifest["max_bytes"])
        schema = self.sql(name, "SELECT c.relname,a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity,a.attgenerated,coalesce(pg_catalog.pg_get_expr(d.adbin,d.adrelid),'') FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE n.nspname='M2 space' AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum;")
        # PostgreSQL 18 duplicates table NOT NULL here; attnotnull above retains that semantic check.
        obligations = self.sql(name, "SELECT c.relname,con.conname,pg_catalog.pg_get_constraintdef(con.oid) FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid=con.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='M2 space' AND con.contype<>'n' ORDER BY 1,2; SELECT c.relname,pg_catalog.pg_get_indexdef(i.indexrelid) FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON c.oid=i.indrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='M2 space' ORDER BY 1,2; SELECT c.relname,t.tgname,t.tgenabled,pg_catalog.pg_get_triggerdef(t.oid) FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='M2 space' AND NOT t.tgisinternal ORDER BY 1,2;")
        invalid_not_null = self.sql(name, "SELECT count(*) FROM pg_catalog.pg_constraint con JOIN pg_catalog.pg_class c ON c.oid=con.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='M2 space' AND con.contype='n' AND NOT con.convalidated;").strip()
        result["schema"] = schema_fingerprint(schema, obligations, invalid_not_null)
        result["sequences"] = self.sql(name, 'SELECT last_value,is_called FROM "M2 space".rows_id_seq;').decode().strip()
        return result


class ToolGate:
    def __init__(self, root, bindir, mode):
        self.root, self.bindir, self.mode = root, bindir, mode
        root.mkdir(mode=0o700)
        self.hit = root / "observed"
        self.release = root / "release"
        for tool in ("pg_dump", "pg_restore"):
            path = root / tool
            path.write_text("#!" + sys.executable + "\n" +
                "import os,pathlib,subprocess,sys,time\n" +
                f"real={str(bindir / tool)!r}\nmode={mode!r}\nroot=pathlib.Path({str(root)!r})\n" +
                "args=sys.argv[1:]; parent=os.getppid()\n" +
                "if '--version' in args: os.execv(real,[real,*args])\n" +
                "pre=any('pre-data' in a for a in args)\npost=any('post-data' in a for a in args)\n" +
                "gate=(mode=='snapshot' and pathlib.Path(real).name=='pg_dump') or (mode=='predata' and pre) or (mode=='postdata' and post)\n" +
                "if gate and mode=='predata':\n r=subprocess.run([real,*args]);\n if r.returncode: sys.exit(r.returncode)\n" +
                "if gate:\n (root/'observed').touch()\n deadline=time.monotonic()+120\n while not (root/'release').exists():\n  if time.monotonic()>deadline: sys.exit(98)\n  time.sleep(.02)\n" +
                "if not(gate and mode=='predata'): os.execv(real,[real,*args])\n")
            path.chmod(0o700)
        config = root / "pg_config"
        config.write_text("#!" + sys.executable + "\nimport os,sys\n" +
                          f"print({str(root)!r}) if sys.argv[1:]==['--bindir'] else os.execv('/usr/bin/pg_config',['/usr/bin/pg_config',*sys.argv[1:]])\n")
        config.chmod(0o700)

    def wait(self, process):
        def observed():
            if self.hit.exists():
                return True
            if process.poll() is not None:
                raise RuntimeError("driver exited before required utility observation")
            return False
        wait_for(observed, self.mode + " utility invocation")


class Driver:
    def __init__(self, binary, environment, root, *, mode="standalone", report_probe=None):
        if mode == "standalone" and report_probe is not None:
            raise RuntimeError("authoritative operator probe requires explicit operator mode")
        if mode not in ("operator", "standalone"):
            raise RuntimeError("configure one explicit producer mode")
        if report_probe is not None and (not str(report_probe) or not Path(report_probe).is_file() or not os.access(report_probe, os.X_OK)):
            raise RuntimeError("selected authoritative report probe is unavailable")
        self.binary, self.environment, self.root = binary, environment, root
        self.mode, self.report_probe = mode, report_probe
        self.number = 0
        self.work = {}
        self.reports = []

    def prepare(self, state, command, *, create=False):
        environment = dict(self.environment)
        if self.mode == "standalone":
            return environment, None
        if state not in self.work:
            if not create:
                raise RuntimeError("operator inspection requires an existing caller binding")
            uid = str(uuid.uuid4())
            self.work[state] = {"migration_uid": uid, "run_id": uid.replace("-", ""),
                                "workdir_id": "m2-pvc-" + uuid.uuid4().hex, "checkpoint_generation": 1}
        work = self.work[state]
        environment.update({"RPGMIGRATE_MIGRATION_UID": work["migration_uid"], "RPGMIGRATE_RUN_ID": work["run_id"],
                            "RPGMIGRATE_WORKDIR_ID": work["workdir_id"], "RPGMIGRATE_CHECKPOINT_GENERATION": "1"})
        binding = self.root / f"binding-{self.number}-{command.replace(' ', '-')}.json"
        binding.write_text(json.dumps({"command": command, "work": work}))
        return environment, binding

    def report(self, output, binding_path, command, completed):
        prefix = b"native-report: "
        framed = [line[len(prefix):] for line in output.splitlines() if line.startswith(prefix)]
        if self.mode == "standalone":
            if framed:
                raise RuntimeError("standalone command fabricated an operator report frame")
            report = json.loads(output, object_pairs_hook=unique_object)
            if (report.get("command") != command or "work" in report or "protocol_version" in report
                    or completed and (report.get("complete") is not True or report.get("phase") != "completed")):
                raise RuntimeError("standalone summary differs from its configured mode")
            return report
        if len(framed) != 1 or binding_path is None:
            raise RuntimeError("operator command requires one framed report and prior caller binding")
        raw = framed[0]
        binding = json.loads(binding_path.read_text())
        checked = False
        if self.report_probe is not None:
            raw_path = binding_path.with_suffix(".report")
            raw_path.write_bytes(raw)
            probe = json.loads(run([str(self.report_probe), str(binding_path), str(raw_path)]))
            if probe.get("accepted") is not True or completed and not all(probe.get(key) is True for key in ("tables_complete", "indexes_complete", "bytes_complete")):
                raise RuntimeError("authoritative operator report probe refused completion")
            checked = True
        report = validate_operator_report(raw, binding, command, completed)
        self.reports.append({"binding": binding, "raw_json": raw.decode("utf-8"), "probe_checked": checked})
        return report

    def start(self, source, target, state, *arguments, gate=None, skip_large_objects=True):
        self.number += 1
        output = self.root / f"command-{self.number}.log"
        output.touch(mode=0o600)
        stderr_path = output.with_suffix(".stderr")
        stderr_path.touch(mode=0o600)
        if self.mode == "operator" and state not in self.work and not any(flag in arguments for flag in ("--restart", "--resume")):
            arguments = ("--restart", *arguments)
        environment, binding = self.prepare(state, "clone", create=True)
        if gate:
            environment["PG_CONFIG"] = str(gate.root / "pg_config")
            environment.pop("PG_BINDIR", None)
        with output.open("wb") as stream, stderr_path.open("wb") as errors:
            process = subprocess.Popen([str(self.binary), "clone", "--source", source, "--target", target,
                                        "--dir", str(state), "--table-jobs", "4", "--index-jobs", "4", "--restore-jobs", "4",
                                        *(["--skip-large-objects"] if skip_large_objects else []), *arguments], env=environment, stdout=stream,
                                       stderr=errors, start_new_session=True)
        process.output = output
        process.error_output = stderr_path
        process.environment = environment
        process.binding = binding
        return process

    def finish(self, process, success=True, reason=None):
        try:
            status = process.wait(timeout=180)
        except subprocess.TimeoutExpired:
            self.kill(process)
            raise SubprocessFailure(private_failure("engine-clone-timeout", process.returncode, process.error_output.read_bytes(), process.environment, sys._getframe())) from None
        stderr = process.error_output.read_bytes()
        output = (process.output.read_bytes() + stderr).decode(errors="replace")
        if self.environment.get("PGPASSWORD", "\x00") in output or "fixture-sensitive-row-" in output:
            raise SubprocessFailure(private_failure("engine-clone-diagnostics", status, stderr, process.environment, sys._getframe()))
        if success:
            if status:
                raise SubprocessFailure(private_failure("engine-clone", status, stderr, process.environment, sys._getframe()))
            self.report(process.output.read_bytes(), process.binding, "clone", True)
        elif reason:
            try:
                require_failure(status, output, reason)
            except RuntimeError:
                raise SubprocessFailure(private_failure("engine-clone-refusal", status, stderr, process.environment, sys._getframe())) from None
        return output

    def inspect(self, path, source, target, state, *, json_output=False, different=False):
        self.number += 1
        environment, binding = self.prepare(state, " ".join(path))
        result = subprocess.run([str(self.binary), *path, "--source", source, "--target", target,
                                 "--dir", str(state), *(["--json"] if json_output else [])],
                                env=environment, capture_output=True, text=True, timeout=120)
        output = result.stdout + result.stderr
        if self.environment.get("PGPASSWORD", "\x00") in output or "fixture-sensitive-row-" in output:
            raise SubprocessFailure(private_failure("engine-inspection-diagnostics", result.returncode, result.stderr.encode(), environment, sys._getframe()))
        if (result.returncode != 0) != different:
            raise SubprocessFailure(private_failure("engine-inspection", result.returncode, result.stderr.encode(), environment, sys._getframe()))
        if path == ("compare", "data"):
            data = json.loads(result.stdout)
            if not isinstance(data, list) or not data or any(item.get("status") not in ("match", "different") for item in data):
                raise RuntimeError("comparison table coverage is empty or incomplete")
            if any(item.get("status") == "different" for item in data) != different:
                raise RuntimeError("comparison disagrees with independent oracle")
            for item in data:
                for peer in ("source", "target"):
                    if not isinstance(item.get(peer, {}).get("rowcount"), int) or not item[peer].get("checksum"):
                        raise RuntimeError("comparison omitted measured peer evidence")
            progress = [line for line in result.stderr.splitlines() if line.startswith("compare progress: ")]
            if not progress or json.loads(progress[-1][len("compare progress: "):]).get("final") is not True:
                raise RuntimeError("comparison lacks final measured progress")
            return data
        if path == ("list", "progress"):
            return self.report(result.stdout.encode(), binding, "list progress", False)
        if path == ("compare", "schema") and json.loads(result.stdout).get("matched") is not True:
            raise RuntimeError("schema comparison lacks an explicit match")
        return result.stdout

    @staticmethod
    def kill(process):
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)


def inner(args):
    manifest = load_manifest()
    source_major, target_major = args.pair.split("-")
    bindir = Path("/usr/lib/postgresql") / target_major / "bin"
    clients = {name: tool_version(bindir / name) for name in ("pg_dump", "pg_restore", "psql")}
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("PG", "RPGMIGRATE_"))}
    environment.update(PGPASSWORD=os.environ["PGPASSWORD"], PG_BINDIR=str(bindir),
                       PATH=str(bindir) + os.pathsep + os.environ["PATH"], LC_ALL="C.UTF-8")
    source, target = (Database(host, bindir, environment) for host in ("source", "target"))
    versions = [db.sql("postgres", "SHOW server_version;").decode().split()[0] for db in (source, target)]
    binary = Path(args.binary)
    if not binary.is_file():
        raise RuntimeError("required compiled Linux engine binary is absent")
    linked_path = re.search(r"libpq\.so\.5 => (\S+)", run(["ldd", str(binary)]).decode())
    if not linked_path:
        raise RuntimeError("actual executable libpq linkage is unavailable")
    libpq = ctypes.CDLL(linked_path[1])
    linked = int(libpq.PQlibVersion())
    receipt = {"pair": args.pair, "server_versions": versions, "client_versions": clients, "linked_libpq": linked, "cases": {}}
    # Refuse wrong inputs before the engine can create target objects.
    validate_runtime(receipt)
    with tempfile.TemporaryDirectory(prefix="rpgmigrate-m2-") as temporary:
        root = Path(temporary)
        driver = Driver(binary, environment, root, mode="operator", report_probe=args.report_probe)
        def case(name):
            srcname, dstname = "src_" + name, "dst_" + name
            return srcname, dstname, source.create(srcname, True), target.create(dstname), root / name
        srcname, dstname, src, dst, state = case("plain")
        expected = source.fingerprint(srcname, manifest)
        driver.finish(driver.start(src, dst, state))
        require_equal(expected, target.fingerprint(dstname, manifest))
        driver.inspect(("compare", "schema"), src, dst, state)
        comparisons = driver.inspect(("compare", "data"), src, dst, state, json_output=True)
        if len(comparisons) != len(manifest["tables"]):
            raise RuntimeError("comparison omitted a planned physical table")
        progress = driver.inspect(("list", "progress"), src, dst, state, json_output=True)
        for counter in ("tables", "indexes", "bytes"):
            measured = progress.get("copy", {}).get(counter, {})
            if measured.get("total") is None or measured.get("total") != measured.get("done"):
                raise RuntimeError("completed progress lacks measured counters")
        receipt["cases"]["plain"] = {"observed": True, "fingerprints": expected, "target_receipt": target.completion(dstname, state, manifest)}
        completed = target.completion(dstname, state, manifest)
        driver.finish(driver.start(src, dst, state, "--resume", "--not-consistent"))
        require_equal(expected, target.fingerprint(dstname, manifest))
        if completed != target.completion(dstname, state, manifest):
            raise RuntimeError("completed operator retry changed generation or committed marker rows")
        receipt["cases"]["completed_resume"] = {"observed": True, "unchanged_generation_and_markers": True}
        target.sql(dstname, 'UPDATE "M2 space".rows SET value=\'changed\' WHERE id=1;')
        if expected == target.fingerprint(dstname, manifest):
            raise RuntimeError("independent oracle accepted mutated target bytes")
        driver.inspect(("compare", "data"), src, dst, state, json_output=True, different=True)
        receipt["cases"]["oracle_mutation"] = {"observed": True}
        srcname, dstname, src, dst, state = case("snapshot")
        expected = source.fingerprint(srcname, manifest)
        gate = ToolGate(root / "snapshot-tools", bindir, "snapshot")
        process = driver.start(src, dst, state, gate=gate)
        try:
            gate.wait(process)
            source.sql(srcname, 'INSERT INTO "M2 space".wide VALUES (99999,\'concurrent\');')
            gate.release.touch()
            driver.finish(process)
        finally:
            driver.kill(process)
        require_equal(expected, target.fingerprint(dstname, manifest))
        if expected == source.fingerprint(srcname, manifest):
            raise RuntimeError("source concurrent-write control made no change")
        driver.inspect(("compare", "data"), src, dst, state, json_output=True, different=True)
        receipt["cases"]["snapshot_writes"] = {"observed": True}
        for mode, reason in (("predata", "pre-data"), ("postdata", "post-data")):
            srcname, dstname, src, dst, state = case(mode)
            gate = ToolGate(root / (mode + "-tools"), bindir, mode)
            process = driver.start(src, dst, state, gate=gate)
            try:
                gate.wait(process)
            finally:
                driver.kill(process)
            before = target.fingerprint(dstname, manifest)
            driver.finish(driver.start(src, dst, state, "--resume", "--not-consistent"), success=False, reason=reason)
            require_equal(before, target.fingerprint(dstname, manifest))
            receipt["cases"]["predata_handoff" if mode == "predata" else "postdata_refusal"] = {"observed": True}
        copy_restart(source, target, driver, root, bindir, manifest, receipt, case)
        srcname, dstname, src, dst, state = case("foreign")
        target.sql(dstname, "CREATE TABLE public.foreign_probe(value integer); INSERT INTO public.foreign_probe VALUES (73);")
        signature = target.catalog_signature(dstname)
        before = target.sql(dstname, "SELECT oid FROM pg_class WHERE oid='public.foreign_probe'::regclass; SELECT * FROM public.foreign_probe;")
        driver.finish(driver.start(src, dst, state), success=False, reason="target")
        if signature != target.catalog_signature(dstname) or before != target.sql(dstname, "SELECT oid FROM pg_class WHERE oid='public.foreign_probe'::regclass; SELECT * FROM public.foreign_probe;"):
            raise RuntimeError("foreign target object changed")
        receipt["cases"]["foreign_collision"] = {"observed": True}
    receipt["report_validation"] = {"mode": "operator_go_parse_report" if args.report_probe is not None else "local_m2_contract", "reports": driver.reports}
    if args.report_probe is not None:
        receipt["report_validation"]["probe_sha256"] = hashlib.sha256(Path(args.report_probe).read_bytes()).hexdigest()
    validate_receipt(receipt, manifest)
    Path(args.receipt).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"pair": args.pair, "cases": len(receipt["cases"]), "server_versions": versions, "client_versions": clients, "linked_libpq": linked}, sort_keys=True))


def blocked_worker_sql(application):
    application = application.replace("'", "''")
    return ("SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
            f"AND application_name='{application}' "
            "AND query='LOCK TABLE ONLY \"M2 space\".\"wide\" IN ACCESS EXCLUSIVE MODE' "
            "AND wait_event_type='Lock';")


def copy_restart(source, target, driver, root, bindir, manifest, receipt, case):
    srcname, dstname, src, dst, state = case("restart")
    gate = ToolGate(root / "restart-tools", bindir, "predata")
    process = driver.start(src, dst, state, gate=gate)
    lock = None
    try:
        gate.wait(process)
        application = "m2_fixture_lock_" + uuid.uuid4().hex
        lock = subprocess.Popen([str(bindir / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname", dst + " application_name=" + application],
                                env=target.environment, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, start_new_session=True)
        lock.stdin.write(b'BEGIN; LOCK TABLE ONLY "M2 space".wide IN SHARE MODE;\n')
        lock.stdin.flush()
        wait_for(lambda: target.sql(dstname, "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name='" + application + "' AND l.mode='ShareLock' AND l.granted;").strip() == b"1", "owned target SHARE lock")
        gate.release.touch()
        blocked_pid = int(wait_for(lambda: target.sql(dstname, blocked_worker_sql(target.application)).strip(), "owned worker blocked on exact pre-truncate ACCESS EXCLUSIVE lock"))
        saved = json.loads((state / "manifest.json").read_text())
        metadata = saved["target_provenance"][0]["record"]
        marker = quote(metadata["intent"]["binding"]["schema_name"]) + '."copy_done"'
        previous = saved["clone_state"]["plan"]["generation"]
        required_markers = len(manifest["tables"]) - 1
        wait_for(lambda: target.sql(dstname, f"SELECT count(*) FROM ONLY {marker} WHERE copy_generation={previous};").strip() == str(required_markers).encode(), "other physical tables committed with markers")
        before = {str(path.relative_to(state)): hashlib.sha256(path.read_bytes()).hexdigest() for path in state.rglob("*") if path.is_file()}
        progress = driver.inspect(("list", "progress"), src, dst, state, json_output=True)
        after = {str(path.relative_to(state)): hashlib.sha256(path.read_bytes()).hexdigest() for path in state.rglob("*") if path.is_file()}
        if before != after or progress.get("phase") != "copying" or progress.get("copy", {}).get("bytes", {}).get("total") is not None:
            raise RuntimeError("active progress mutated state or invented completion measurements")
        previous = json.loads((state / "manifest.json").read_text())["clone_state"]["plan"]["generation"]
    finally:
        driver.kill(process)
        if lock:
            os.killpg(lock.pid, signal.SIGTERM)
            lock.communicate(timeout=10)
    interrupted = target.fingerprint(dstname, manifest)
    source.sql(srcname, 'ALTER TABLE "M2 space".wide ALTER COLUMN value SET DEFAULT \'drift\';')
    driver.finish(driver.start(src, dst, state, "--restart"), success=False, reason="pre-data schema drift")
    require_equal(interrupted, target.fingerprint(dstname, manifest))
    unchanged = json.loads((state / "manifest.json").read_text())["clone_state"]["plan"]["generation"]
    markers = target.sql(dstname, f"SELECT count(*) FROM ONLY {marker} WHERE copy_generation={previous};").decode().strip()
    if unchanged != previous or markers != str(required_markers):
        raise RuntimeError("refused pre-data drift changed generation or committed markers")
    source.sql(srcname, 'ALTER TABLE "M2 space".wide ALTER COLUMN value DROP DEFAULT;')
    source.sql(srcname, 'INSERT INTO "M2 space".wide VALUES (99998,\'fresh-generation\');')
    expected = source.fingerprint(srcname, manifest)
    driver.finish(driver.start(src, dst, state, "--resume", "--not-consistent"))
    current = json.loads((state / "manifest.json").read_text())["clone_state"]["plan"]["generation"]
    if not isinstance(previous, int) or current != previous + 1:
        raise RuntimeError("COPY restart did not advance the independently observed generation")
    require_equal(expected, target.fingerprint(dstname, manifest))
    driver.finish(driver.start(src, dst, state, "--restart"), success=False, reason="post-data")
    require_equal(expected, target.fingerprint(dstname, manifest))
    receipt["cases"]["copy_restart"] = {"observed": True, "previous_generation": previous, "current_generation": current, "prior_committed_tables": required_markers, "predata_drift_refused": True, "blocked_backend_pid": blocked_pid, "target_receipt": target.completion(dstname, state, manifest)}


LOGICAL_SERVER_ARGUMENTS = ("-c", "wal_level=logical", "-c", "max_replication_slots=32", "-c", "max_wal_senders=32", "-c", "wal_sender_timeout=10s")
GIB = 1024 ** 3


def server_start_arguments(profile, arguments):
    if profile == "m8":
        if arguments and tuple(arguments) != LOGICAL_SERVER_ARGUMENTS:
            raise RuntimeError("candidate logical server startup arguments conflict")
        return LOGICAL_SERVER_ARGUMENTS
    return arguments


def build_cpu_arguments(help_text):
    if not all(re.search(rb"(?<![\w-])" + flag + rb"(?:[ =\n]|$)", help_text) for flag in (b"--cpu-period", b"--cpu-quota")):
        raise RuntimeError("runtime build has no supported hard CFS CPU cap")
    return ["--cpu-period", "100000", "--cpu-quota", "200000"]


def image_identity(value):
    value = value.removeprefix("sha256:") if isinstance(value, str) else ""
    if not re.fullmatch(r"[a-f0-9]{64}", value):
        raise RuntimeError("immutable image identity is absent")
    return "sha256:" + value


def validate_owned_container(actual, entry, token, baseline):
    host = actual.get("HostConfig", {})
    cpu = host.get("NanoCpus") == 2000000000 or (host.get("CpuPeriod") == 100000 and host.get("CpuQuota") == 200000)
    mounts = actual.get("Mounts")
    observed = sorted((row.get("Source"), row.get("Destination"), row.get("RW")) for row in mounts) if isinstance(mounts, list) else None
    if (not re.fullmatch(r"[a-f0-9]{64}", entry.get("id", "")) or entry["id"] in baseline
            or actual.get("Id", actual.get("ID")) != entry["id"] or image_identity(actual.get("Image")) != entry["image"]
            or actual.get("Name", "").lstrip("/") != entry["name"] or actual.get("Config", {}).get("Labels", {}).get("io.rpgmigrate.e2e") != token
            or host.get("Memory") != entry["memory"] or not cpu or host.get("Privileged") is not False
            or host.get("NetworkMode") in ("host", "container", None) or host.get("PortBindings") not in ({}, None)
            or any(value for value in actual.get("NetworkSettings", {}).get("Ports", {}).values())
            or observed != sorted(entry["mounts"]) or any(row.get("Type") != "bind" for row in mounts)):
        raise RuntimeError("exact owned container identity, caps, or mounts differ")
    if entry.get("role") == "build":
        networks = actual.get("NetworkSettings", {}).get("Networks")
        empty_endpoint = {"EndpointID": "", "Gateway": "", "IPAddress": "", "IPPrefixLen": 0,
                          "IPv6Gateway": "", "GlobalIPv6Address": "", "GlobalIPv6PrefixLen": 0,
                          "MacAddress": "", "NetworkID": "pasta", "DriverOpts": None, "IPAMConfig": None, "Links": None}
        placeholder = (isinstance(networks, dict) and set(networks) == {"pasta"} and networks["pasta"] == empty_endpoint
                       and all(type(networks["pasta"][key]) is type(value) for key, value in empty_endpoint.items()))
        if host.get("NetworkMode") != "pasta" or networks not in ({}, None) and not placeholder:
            raise RuntimeError("owned build has a foreign or populated pasta network attachment")
    if entry.get("network"):
        expected = entry["network"]
        networks = actual.get("NetworkSettings", {}).get("Networks") or {}
        if (set(networks).difference({expected["name"]})
                or any(row.get("NetworkID") not in ("", expected["id"]) for row in networks.values())
                or actual.get("State", {}).get("Running") is True and (set(networks) != {expected["name"]} or networks[expected["name"]].get("NetworkID") != expected["id"])):
            raise RuntimeError("owned container has a foreign or unknown network attachment")
    return actual


def validate_owned_network(actual, expected, token, baseline, *, empty=False):
    if isinstance(baseline, dict):
        identities = [row.get("id", row.get("Id")) if isinstance(row, dict) else None for row in baseline.values()]
        if any(not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value) for value in identities):
            raise RuntimeError("pre-existing network descriptor identity is unavailable")
        baseline = set(identities)
    cid = actual.get("id", actual.get("Id"))
    if (not re.fullmatch(r"[a-f0-9]{64}", expected.get("id", "")) or expected["id"] in baseline or cid != expected["id"]
            or actual.get("name", actual.get("Name")) != expected["name"] or actual.get("internal", actual.get("Internal")) is not True
            or actual.get("labels", actual.get("Labels", {})).get("io.rpgmigrate.e2e") != token
            or not isinstance(actual.get("containers"), dict) or empty and actual["containers"]):
        raise RuntimeError("exact owned internal network identity or membership differs")
    return actual


def validate_capacity(sample, baseline, *, initial=False):
    if any(type(sample.get(key)) is not int or sample[key] <= 0 for key in ("vm_free", "host_free", "vm_available", "vm_cpus")):
        raise RuntimeError("resource capacity sample is unavailable")
    disk, memory = (32 * GIB, 12 * GIB) if initial else (20 * GIB, 8 * GIB)
    if (sample["vm_cpus"] < 6 or min(sample["vm_free"], sample["host_free"]) < disk or sample["vm_available"] < memory
            or any(baseline[key] - sample[key] > 12 * GIB for key in ("vm_free", "host_free"))):
        raise RuntimeError("resource capacity floor or growth budget crossed")


def materialize_source(root, destination, execute=None):
    execute = run if execute is None else execute
    expected, total_size = {}, 0
    records = execute(["git", "-C", str(root), "ls-tree", "-r", "-l", "-z", "HEAD"]).split(b"\0")
    for record in records:
        if not record:
            continue
        metadata, name = record.decode().split("\t", 1)
        mode, kind, identity, size = metadata.split()
        if mode not in ("100644", "100755") or kind != "blob" or not re.fullmatch(r"[a-f0-9]{40}", identity) or name in expected:
            raise RuntimeError("tracked source tree has unsupported modes or object identities")
        if not size.isdecimal() or int(size) > 16 * 1024 * 1024:
            raise RuntimeError("tracked source Git blob exceeded its bound")
        total_size += int(size)
        if total_size > 64 * 1024 * 1024:
            raise RuntimeError("tracked source Git tree exceeded its bound")
        expected[name] = (int(mode[-3:], 8), identity)
    if not expected or len(expected) > 4096:
        raise RuntimeError("tracked source tree is empty or exceeded its bound")
    raw = execute(["git", "-C", str(root), "-c", "tar.umask=0022", "archive", "--format=tar", "HEAD"])
    if len(raw) > 72 * 1024 * 1024:
        raise RuntimeError("tracked source archive exceeded its bound")
    destination.mkdir(mode=0o700)
    files, total, seen, extracted = 0, 0, set(), set()
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
        for member in archive:
            parts = member.name.rstrip("/").split("/")
            if (not member.name or member.name.startswith("/") or any(part in ("", ".", "..", ".git", "target", "__pycache__") for part in parts)
                    or member.name in seen or not (member.isfile() or member.isdir())):
                raise RuntimeError("tracked source archive contains a replaced or unsafe entry")
            seen.add(member.name)
            path = destination.joinpath(*parts)
            if member.isdir():
                path.mkdir(mode=0o700, parents=True, exist_ok=True)
                continue
            files += 1
            total += member.size
            if member.mode not in (0o644, 0o755) or member.size > 16 * 1024 * 1024 or total > 64 * 1024 * 1024 or files > 4096:
                raise RuntimeError("tracked source archive mode or size bound differs")
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with archive.extractfile(member) as source:
                data = source.read(16 * 1024 * 1024 + 1)
            identity = hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
            if (len(data) != member.size or expected.get(member.name) != (member.mode, identity)
                    or data.startswith(b"version https://git-lfs.github.com/spec/v1\n")):
                raise RuntimeError("tracked source archive differs from exact Git blobs or contains an LFS pointer")
            with path.open("xb") as target:
                target.write(data)
            path.chmod(member.mode)
            extracted.add(member.name)
    if extracted != set(expected):
        raise RuntimeError("tracked source archive omitted expected Git files")


def container_snapshot(value):
    host_keys = ("Memory", "MemorySwap", "NanoCpus", "CpuPeriod", "CpuQuota", "CpusetCpus", "PidsLimit", "Privileged", "ReadonlyRootfs", "NetworkMode", "PortBindings", "Binds", "Tmpfs")
    mount_keys = ("Type", "Name", "Source", "Destination", "Driver", "Mode", "RW", "Propagation")
    return {"id": value.get("Id", value.get("ID")), "name": value["Name"], "created": value["Created"], "image": value["Image"], "restarts": value["RestartCount"],
            "state": {key: value["State"].get(key) for key in ("Running", "Status", "Pid", "StartedAt", "FinishedAt")},
            "host_config": {key: value["HostConfig"].get(key) for key in host_keys},
            "restart_policy": {key: value["HostConfig"].get("RestartPolicy", {}).get(key) for key in ("Name", "MaximumRetryCount")},
            "mounts": [{key: row.get(key) for key in mount_keys} for row in value["Mounts"]],
            "ports": value.get("NetworkSettings", {}).get("Ports"),
            "networks": {name: {key: row.get(key) for key in ("NetworkID", "Aliases")} for name, row in (value.get("NetworkSettings", {}).get("Networks") or {}).items()}, "run_label": value.get("Config", {}).get("Labels", {}).get("io.rpgmigrate.e2e")}


def network_snapshot(value):
    keys = {"name", "id", "driver", "network_interface", "created", "subnets", "ipv6_enabled", "internal", "dns_enabled", "network_dns_servers", "ipam_options", "routes"}
    options = value.get("options") or {}
    if not isinstance(options, dict) or set(options).difference({"isolate", "mtu", "metric", "interface_name", "mode", "vlan"}):
        raise RuntimeError("network descriptor has unreviewed options")
    return {**{key: value.get(key) for key in keys}, "options": options,
            "run_label": (value.get("labels") or {}).get("io.rpgmigrate.e2e"),
            "containers": sorted(value["containers"]) if isinstance(value.get("containers"), dict) else None}


def validate_preservation(before, after, owned_images):
    if set(before) != {"containers", "external", "images", "networks", "volumes"} or set(after) != set(before):
        raise RuntimeError("preservation inventory is unavailable")
    for kind in before:
        if kind == "images":
            if (any(after[kind].get(key) != value for key, value in before[kind].items())
                    or not set(after[kind]).difference(before[kind]).issubset(owned_images)):
                raise RuntimeError("pre-existing image associations changed or an unowned image appeared")
        elif before[kind] != after[kind]:
            raise RuntimeError("pre-existing resources changed or an unresolved " + kind + " resource remains")


class RuntimeGuard:
    def __init__(self, runtime, token, path):
        self.runtime, self.token, self.path = runtime, token, path
        self.deadline = time.monotonic() + 5400
        self.work_deadline = self.deadline - 300
        self.last_sample = 0
        self.ledger = {"version": 2, "scope": "runtime_resource_ownership", "run": token, "containers": [], "cleanup_complete": False,
                       "deadline_unix": time.time() + 5400, "cleanup_reserve_seconds": 300, "samples": []}
        self.persist()
        try:
            versions = json.loads(self.raw(["version", "--format", "json"]))
            info = json.loads(self.raw(["info", "--format", "json"]))
            host = info.get("host", {})
            pasta = host.get("pasta") or {}
            # A remote service is the macOS podman machine; a local one shares this Linux host's kernel and filesystem.
            self.machine = host.get("serviceIsRemote")
            self.storage = "/var/home/core" if self.machine else (info.get("store") or {}).get("graphRoot")
            if type(self.machine) is not bool or not isinstance(self.storage, str) or not self.storage.startswith("/"):
                raise RuntimeError("runtime host locality is unavailable")
            server = versions.get("Server", {}) if self.machine else info.get("version") or {}
            if (host.get("security", {}).get("rootless") is not True or pasta.get("executable") != "/usr/bin/pasta"
                    or self.host(["which", "pasta"]).strip() != b"/usr/bin/pasta"
                    or b"--network" not in self.raw(["run", "--help"])):
                raise RuntimeError("isolated rootless pasta build networking is unavailable")
            self.ledger["runtime_api"] = {"client": versions.get("Client", {}).get("Version"), "server": server.get("Version"), "machine": self.machine,
                                          "rootless": True, "network_backend": host.get("networkBackend"), "pasta_executable": pasta["executable"],
                                          "pasta_package": pasta.get("package"), "cargo_network": "pasta", "image_build_network": "private"}
            if not all(isinstance(self.ledger["runtime_api"][key], str) and self.ledger["runtime_api"][key] for key in ("client", "server")):
                raise RuntimeError("actual runtime version identity unavailable")
            self.baseline = self.inventory()
            self.ledger["baseline"] = self.baseline
            self.ledger["builtin_network_created_observations"] = []
            for identity, before in self.baseline["networks"].items():
                if before.get("name") == "podman" and before.get("driver") == "bridge" and before.get("internal") is False:
                    after = network_snapshot(json.loads(self.raw(["network", "inspect", identity]))[0])
                    if {key: value for key, value in before.items() if key != "created"} != {key: value for key, value in after.items() if key != "created"}:
                        raise RuntimeError("builtin network changed during independent baseline reads")
                    self.ledger["builtin_network_created_observations"].append({"id": identity, "before": before["created"], "after": after["created"]})
            self.initial_capacity = self.sample()
            validate_capacity(self.initial_capacity, self.initial_capacity, initial=True)
            self.persist()
        except Exception:
            self.ledger["admission_failed"] = True
            self.persist()
            raise

    def persist(self):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self.path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(self.ledger, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(self.path)
        descriptor = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def raw(self, arguments, timeout=5, *, runtime=True):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("owned cleanup deadline expired")
        return run([self.runtime, *arguments] if runtime else list(arguments), timeout=min(timeout, remaining))

    def host(self, arguments, timeout=5):
        return self.raw(["machine", "ssh", *arguments], timeout=timeout) if self.machine else self.raw(arguments, timeout=timeout, runtime=False)

    def inspect(self, cid):
        rows = json.loads(self.raw(["inspect", cid]))
        if not isinstance(rows, list) or len(rows) != 1:
            raise RuntimeError("exact container inspection is unavailable")
        return rows[0]

    def inventory(self):
        def identifiers(arguments):
            return sorted(set(self.raw(arguments, timeout=20).decode().splitlines()))
        result = {"containers": {}, "external": identifiers(["ps", "--all", "--external", "--no-trunc", "--format", "{{.ID}}"]),
                  "images": {}, "networks": {}, "volumes": {}}
        for cid in identifiers(["ps", "--all", "--no-trunc", "--format", "{{.ID}}"]):
            value = self.inspect(cid)
            result["containers"][cid] = container_snapshot(value)
        for identity in identifiers(["image", "ls", "--all", "--no-trunc", "--quiet"]):
            value = json.loads(self.raw(["image", "inspect", identity]))[0]
            key = image_identity(value["Id"])
            history = json.loads(self.raw(["history", "--no-trunc", "--format", "json", identity]))
            if not isinstance(history, list):
                raise RuntimeError("image history associations are unavailable")
            safe_history = [{key: next((item for name, item in row.items() if name.lower() == key), None) for key in ("id", "created", "size")} for row in history]
            result["images"][key] = {"tags": sorted(value.get("RepoTags") or []), "digests": sorted(value.get("RepoDigests") or []), "size": value["Size"],
                                     "history": safe_history, "run_label": (value.get("Labels") or {}).get("io.rpgmigrate.e2e")}
        for identity in identifiers(["network", "ls", "--no-trunc", "--quiet"]):
            value = json.loads(self.raw(["network", "inspect", identity]))[0]
            network = network_snapshot(value)
            identifier = network.get("id")
            if not isinstance(identifier, str) or not re.fullmatch(r"[a-f0-9]{64}", identifier) or identifier in result["networks"]:
                raise RuntimeError("network inventory has missing or duplicate full identities")
            result["networks"][identifier] = network
        for identity in identifiers(["volume", "ls", "--quiet"]):
            value = json.loads(self.raw(["volume", "inspect", identity]))[0]
            if value.get("Options") not in ({}, None):
                raise RuntimeError("volume descriptor has unreviewed options")
            result["volumes"][identity] = {key: value.get(key) for key in ("Name", "Driver", "Mountpoint", "CreatedAt", "Scope", "Options")}
            result["volumes"][identity]["run_label"] = (value.get("Labels") or {}).get("io.rpgmigrate.e2e")
        return result

    def observe_caps(self, entry, actual=None):
        actual = validate_owned_container(actual or self.inspect(entry["id"]), entry, self.token, self.baseline["containers"])
        if actual.get("State", {}).get("Running") is not True:
            raise RuntimeError("effective limits require a live exact owned container")
        observed = self.raw(["exec", entry["id"], "cat", "/sys/fs/cgroup/cpu.max", "/sys/fs/cgroup/memory.max"]).splitlines()
        try:
            quota, period = (int(value) for value in observed[0].split())
            memory = int(observed[1])
            if len(observed) != 2 or period <= 0 or quota != period * 2 or memory != entry["memory"]:
                raise ValueError
        except (ValueError, IndexError):
            raise RuntimeError("actual owned cgroup CPU or memory limit is absent or differs") from None
        entry["cgroup_verified"] = True
        entry["cgroup_limits"] = {"cpu_quota": quota, "cpu_period": period, "memory_bytes": memory}

    def _create(self, command, expected, *, timeout=300):
        if getattr(self, "_pending_creation", None) is not None or self._creation_unresolved():
            raise RuntimeError("another resource creation remains pending")
        fields = {"container": {"kind", "role", "image", "name", "mounts", "memory", "network"},
                  "network": {"kind", "name"}, "image": {"kind", "tag"}}
        kind = expected.get("kind")
        if kind not in fields or set(expected) != fields[kind]:
            raise RuntimeError("pending creation specification differs")
        prefix = [self.runtime, "network", "create"] if kind == "network" else [self.runtime, "build" if kind == "image" else "create"]
        if command[:len(prefix)] != prefix or "io.rpgmigrate.e2e=" + self.token not in command:
            raise RuntimeError("pending creation command binding differs")
        self.checkpoint()
        identity = expected["tag" if kind == "image" else "name"]
        if self.exists(kind, identity):
            raise RuntimeError("fresh resource name or tag already exists")
        projection = {**expected, "run": self.token, "absent_before": True, "status": "intent", "invoked_at": time.time(),
                      "maximum_seconds": min(self.work_deadline - time.monotonic(), timeout)}
        self.ledger["pending_creation"] = projection
        pending = {"command": tuple(command), "expected": dict(expected), "projection": projection,
                   "deadline": min(self.work_deadline, time.monotonic() + timeout)}
        self._pending_creation = pending
        try:
            self.persist()
            output = self.execute(command, timeout=timeout)
            entry = self._admit_creation(output)
            self.checkpoint()
            return entry
        except BaseException:
            if "admitted" in pending:
                pending["admitted"]["cleanup_only"] = True
                projection["status"] = "cleanup_only"
            else:
                projection["status"] = "unresolved"
                try:
                    absent = not self.exists(kind, identity)
                    for cid in pending.get("observed_ids", set()):
                        absent = absent and not self.exists("container", cid)
                    if absent:
                        projection["status"] = "absent"
                except Exception as error:
                    projection["absence_error_kind"] = type(error).__name__
            try:
                self.persist()
            except Exception as error:
                projection["persistence_error_kind"] = type(error).__name__
            raise
        finally:
            if not self._creation_unresolved():
                self._pending_creation = None

    def _creation_unresolved(self):
        return self.ledger.get("pending_creation", {}).get("status") not in (None, "admitted", "cleanup_only", "absent")

    def _created_container(self, cid):
        pending = self._pending_creation
        expected = pending["expected"]
        if re.fullmatch(r"[a-f0-9]{64}", cid):
            pending.setdefault("observed_ids", set()).add(cid)
        if pending.get("candidate") not in (None, cid) or cid in self.baseline["external"]:
            raise RuntimeError("pending container identity changed or predates creation")
        entry = {key: value for key, value in expected.items() if key != "kind"}
        entry["id"] = cid
        actual = validate_owned_container(self.inspect(cid), entry, self.token, self.baseline["containers"])
        state = actual.get("State", {})
        zero = (None, "", "0001-01-01T00:00:00Z", "0001-01-01T00:00:00.000000000Z")
        try:
            created = datetime.fromisoformat(actual["Created"].replace("Z", "+00:00"))
            if created.tzinfo is None:
                raise ValueError
            created = created.timestamp()
        except (KeyError, TypeError, ValueError):
            raise RuntimeError("pending container creation time is unavailable") from None
        upper = pending["projection"].get("started_at", pending["projection"]["invoked_at"]) + pending["projection"]["maximum_seconds"]
        if (not pending["projection"]["invoked_at"] <= created <= min(time.time(), upper)
                or state.get("Status") != "created" or state.get("Running") is not False
                or type(state.get("Pid")) is not int or state["Pid"] != 0
                or type(actual.get("RestartCount")) is not int or actual["RestartCount"] != 0
                or state.get("StartedAt") not in zero or state.get("FinishedAt") not in zero):
            raise RuntimeError("pending container is stale or has already started")
        if expected["role"] in ("source", "target"):
            networks = actual.get("NetworkSettings", {}).get("Networks") or {}
            endpoint = networks.get(expected["network"]["name"], {})
            if expected["role"] not in (endpoint.get("Aliases") or []):
                raise RuntimeError("pending database role alias differs")
        return entry, actual

    def _observe_pending_container(self, cid):
        pending = getattr(self, "_pending_creation", None)
        if pending and time.monotonic() >= pending["deadline"]:
            raise RuntimeError("pending creation command deadline expired")
        if (not pending or pending["expected"]["kind"] != "container" or "admitted" in pending
                or pending["projection"]["status"] != "active" or not pending.get("process")
                or type(pending["projection"].get("pid")) is not int or pending["projection"]["pid"] <= 0
                or pending["process"].pid != pending["projection"].get("pid") or pending["process"].poll() is not None):
            raise RuntimeError("unknown container has no active bound creation")
        _, actual = self._created_container(cid)
        pending["candidate"] = cid
        pending["projection"].update(candidate_id=cid, observation=container_snapshot(actual))
        self.persist()
        return actual

    def _admit_creation(self, output):
        pending = self._pending_creation
        if "admitted" in pending:
            if pending["receipt"] != output:
                raise RuntimeError("resource creation receipt changed")
            return pending["admitted"]
        expected, projection = pending["expected"], pending["projection"]
        if expected["kind"] == "container":
            cid = output.decode().strip()
            if not re.fullmatch(r"[a-f0-9]{64}", cid):
                raise RuntimeError("fresh creation returned no single exact container identity")
            _, actual = self._created_container(cid)
            entry = self.retain_container(cid, expected["role"], expected["image"], expected["name"], expected["mounts"], expected["memory"], expected["network"], actual=actual)
            entry["absent_before"] = True
        elif expected["kind"] == "network":
            receipt = output.decode().strip()
            actual = json.loads(self.raw(["network", "inspect", expected["name"]]))[0]
            entry = {"id": actual.get("id", actual.get("Id")), "name": expected["name"], "absent_before": True, "cleanup_status": "pending"}
            if receipt not in (entry["name"], entry["id"]):
                raise RuntimeError("fresh network creation result differs from inspected identity")
            validate_owned_network(actual, entry, self.token, self.baseline["networks"], empty=True)
            if self.raw(["ps", "--all", "--filter", "network=" + entry["id"], "--no-trunc", "--format", "{{.ID}}"]).strip():
                raise RuntimeError("fresh network has an unexpected filtered attachment")
            entry["created_observation"] = network_snapshot(actual)
            self.ledger["network"] = entry
        else:
            actual = json.loads(self.raw(["image", "inspect", expected["tag"]]))[0]
            identity = image_identity(actual["Id"])
            if identity in self.baseline["images"] or actual.get("Labels", {}).get("io.rpgmigrate.e2e") != self.token:
                raise RuntimeError("built development image identity or run label differs")
            entry = self.ledger["development_image"]
            entry.update(id=identity, bytes=actual["Size"], architecture=actual["Architecture"])
            self.ledger["admitted_images"].append(identity)
        pending.update(admitted=entry, receipt=output)
        projection.update(status="admitted", admitted_id=entry["id"])
        self.persist()
        return entry


    def sample(self):
        memory = self.host(["cat", "/proc/meminfo"])
        match = re.search(rb"^MemAvailable:\s+(\d+) kB$", memory, re.MULTILINE)
        disk = self.host(["df", "-B1", self.storage]).splitlines()[-1].split()
        sample = {"vm_free": int(disk[3]), "host_free": shutil.disk_usage(ROOT).free,
                  "vm_available": int(match[1]) * 1024 if match else None,
                  "vm_cpus": int(self.host(["getconf", "_NPROCESSORS_ONLN"]).strip())}
        storage = {}
        for entry in self.ledger["containers"]:
            if entry.get("cleanup_status") == "absent":
                continue
            actual = validate_owned_container(self.inspect(entry["id"]), entry, self.token, self.baseline["containers"])
            if actual.get("State", {}).get("Running") is True:
                self.observe_caps(entry, actual)
            elif entry.get("ready"):
                raise RuntimeError("owned database stopped during resource sampling")
            if entry["role"] in ("source", "target") and entry.get("ready"):
                raw = self.raw(["exec", entry["id"], "psql", "-X", "-qAt", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
                                "SELECT (SELECT sum(pg_database_size(oid)) FROM pg_catalog.pg_database),(SELECT coalesce(sum(size),0) FROM pg_catalog.pg_ls_waldir());"])
                values = [int(value) for value in raw.strip().split(b"|")]
                if len(values) != 2 or any(value < 0 or value > 2 * GIB for value in values):
                    raise RuntimeError("owned database or WAL storage bound exceeded")
                storage[entry["role"]] = values
        network = self.ledger.get("network")
        if network and network.get("cleanup_status") != "absent":
            actual = json.loads(self.raw(["network", "inspect", network["id"]]))[0]
            validate_owned_network(actual, network, self.token, self.baseline["networks"])
            attached = sorted(actual["containers"])
            filtered = sorted(self.raw(["ps", "--all", "--filter", "network=" + network["id"], "--no-trunc", "--format", "{{.ID}}"]).decode().splitlines())
            known = {entry["id"] for entry in self.ledger["containers"] if entry["role"] != "build"}
            unknown = set(attached).union(filtered).difference(known)
            if unknown:
                pending = getattr(self, "_pending_creation", None)
                if pending and pending["expected"]["kind"] == "container":
                    pending.setdefault("observed_ids", set()).update(cid for cid in unknown if re.fullmatch(r"[a-f0-9]{64}", cid))
                if len(unknown) != 1:
                    raise RuntimeError("owned network has a foreign container attachment")
                self._observe_pending_container(next(iter(unknown)))
            network.setdefault("membership_observations", []).append({"at": time.time(), "attached_ids": attached, "filtered_ids": filtered})
        self.last_sample = time.monotonic()
        external = self.raw(["ps", "--all", "--external", "--no-trunc", "--format", "{{.ID}}"], timeout=5).decode().splitlines()
        unknown = sorted(set(external).difference(self.baseline["external"], (entry["id"] for entry in self.ledger["containers"])))
        self.ledger["samples"].append({**sample, "at": time.time(), "storage": storage, "unattributed_external_build_ids": unknown})
        return sample

    def checkpoint(self):
        if time.monotonic() >= self.work_deadline:
            raise RuntimeError("runtime work deadline expired; cleanup reserve started")
        if time.monotonic() - self.last_sample >= 5:
            validate_capacity(self.sample(), self.initial_capacity)
            self.persist()
        if time.monotonic() >= self.work_deadline:
            raise RuntimeError("runtime work deadline expired during resource observation")

    def execute(self, command, *, timeout=300):
        self.checkpoint()
        started = time.monotonic()
        deadline = min(self.work_deadline, started + timeout)
        caller = sys._getframe(1)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        record = {"pid": process.pid, "kind": command[1] if Path(command[0]).name in ("podman", "docker") else Path(command[0]).name,
                  "started_at": time.time(), "maximum_seconds": deadline - started}
        pending = getattr(self, "_pending_creation", None)
        if pending and pending["command"] == tuple(command):
            pending.update(process=process, deadline=deadline)
            pending["projection"].update(status="active", pid=process.pid, started_at=record["started_at"], maximum_seconds=deadline - started)
        else:
            pending = None
        failure, output = None, b""
        try:
            self.ledger.setdefault("processes", []).append(record)
            self.persist()
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError("owned subprocess deadline expired")
                try:
                    output, error = process.communicate(timeout=min(5, remaining))
                    break
                except subprocess.TimeoutExpired:
                    self.checkpoint()
            if process.returncode:
                raise SubprocessFailure(private_failure("owned-runtime-subprocess", process.returncode, error, None, caller))
            if pending:
                self._admit_creation(output)
            self.checkpoint()
            return output
        except BaseException as error:
            failure = error
            raise
        finally:
            try:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
                    output, _ = process.communicate(timeout=min(5, max(.01, self.deadline - time.monotonic())))
                elif failure is not None and pending and not output:
                    output, _ = process.communicate(timeout=min(5, max(.01, self.deadline - time.monotonic())))
                if failure is not None and pending and pending["expected"]["kind"] != "image" and "admitted" not in pending and output:
                    try:
                        self._admit_creation(output)
                        pending["admitted"]["cleanup_only"] = True
                        pending["projection"]["status"] = "cleanup_only"
                    except Exception as error:
                        pending["projection"]["admission_error_kind"] = type(error).__name__
            except Exception as error:
                record["reaping_error_kind"] = type(error).__name__
                if failure is None:
                    raise
            record.update(returncode=process.returncode, finished_at=time.time())
            try:
                self.persist()
            except Exception:
                record["ledger_write_failed"] = True
                if failure is None:
                    raise

    def exists(self, kind, identity):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("owned cleanup deadline expired")
        result = subprocess.run([self.runtime, kind, "exists", identity], capture_output=True, timeout=min(5, remaining))
        if result.returncode not in (0, 1):
            raise RuntimeError("resource absence probe is unavailable")
        return result.returncode == 0

    def retain_container(self, cid, role, image, name, mounts, memory, network=None, *, actual=None):
        if not re.fullmatch(r"[a-f0-9]{64}", cid) or cid in self.baseline["containers"] or cid in self.baseline["external"]:
            raise RuntimeError("fresh owned creation returned no exact new container identity")
        entry = {"id": cid, "role": role, "image": image, "name": name, "mounts": mounts, "memory": memory, "network": network, "cleanup_status": "pending"}
        actual = validate_owned_container(actual or self.inspect(cid), entry, self.token, self.baseline["containers"])
        entry["observed"] = container_snapshot(actual)
        entry["ownership_verified"] = True
        self.ledger["containers"].append(entry)
        # Creation admission fsyncs this entry together with its authoritative receipt.
        return entry

    def cleanup_container(self, entry):
        try:
            validate_owned_container(self.inspect(entry["id"]), entry, self.token, self.baseline["containers"])
            entry["ownership_verified"] = True
            self.raw(["rm", "--force", entry["id"]], timeout=10)
            entry["cleanup_status"] = "present" if self.exists("container", entry["id"]) else "absent"
        except Exception as error:
            entry["cleanup_status"] = "unresolved"
            entry["cleanup_error_kind"] = type(error).__name__
        try:
            self.persist()
        except OSError:
            entry["ledger_write_failed"] = True

    def cleanup_network(self):
        entry = self.ledger.get("network")
        if not entry:
            return
        try:
            actual = json.loads(self.raw(["network", "inspect", entry["id"]]))[0]
            validate_owned_network(actual, entry, self.token, self.baseline["networks"], empty=True)
            members = self.raw(["ps", "--all", "--filter", "network=" + entry["id"], "--no-trunc", "--format", "{{.ID}}"])
            if members.strip():
                raise RuntimeError("owned network still has an exact filtered container attachment")
            observed = network_snapshot(actual)
            entry["cleanup_observation"] = observed
            if entry.get("created_observation") is not None and observed != entry["created_observation"]:
                raise RuntimeError("owned empty network descriptor changed since creation")
            entry["empty_membership_verified"] = True
            self.persist()
            self.raw(["network", "rm", entry["id"]], timeout=10)
            entry["cleanup_status"] = "present" if self.exists("network", entry["id"]) else "absent"
        except Exception as error:
            entry["cleanup_status"] = "unresolved"
            entry["cleanup_error_kind"] = type(error).__name__
        try:
            self.persist()
        except OSError:
            entry["ledger_write_failed"] = True

    def close(self):
        for entry in reversed(self.ledger["containers"]):
            if entry.get("cleanup_status") != "absent":
                self.cleanup_container(entry)
        self.cleanup_network()
        try:
            after = self.inventory()
            self.ledger["after"] = after
            normalized = json.loads(json.dumps(after))
            deltas = []
            for observation in self.ledger.get("builtin_network_created_observations", []):
                identity = observation["id"]
                if identity not in after["networks"]:
                    raise RuntimeError("builtin network identity disappeared")
                previous, current = self.baseline["networks"][identity]["created"], after["networks"][identity]["created"]
                if previous != current:
                    deltas.append({"id": identity, "field": "created", "before": previous, "after": current})
                    normalized["networks"][identity]["created"] = previous
            self.ledger["builtin_network_created_deltas"] = deltas
            validate_preservation(self.baseline, normalized, set(self.ledger.get("admitted_images", [])))
            self.ledger["preservation_verified"] = True
        except Exception as error:
            self.ledger["preservation_verified"] = False
            self.ledger["preservation_error_kind"] = type(error).__name__
        self.ledger["cleanup_complete"] = (self.ledger.get("preservation_verified") is True
            and not self._creation_unresolved()
            and all(entry.get("cleanup_status") == "absent" for entry in self.ledger["containers"])
            and ("network" not in self.ledger or self.ledger["network"].get("cleanup_status") == "absent"))
        self.persist()


def directory_identity(path):
    try:
        info = path.lstat()
    except OSError:
        return None
    if not stat.S_ISDIR(info.st_mode):
        return None
    identity = {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid, "mode": stat.S_IMODE(info.st_mode)}
    if hasattr(info, "st_birthtime"):
        identity["birthtime"] = info.st_birthtime
    return identity


def outer(args, *, program="clone", manifest=None, receipt_validator=validate_receipt, server_arguments=()):
    manifest = load_manifest() if manifest is None else manifest
    diagnostic_case = getattr(args, "diagnostic_case", None)
    cutover_mode = getattr(args, "cutover", False)
    feature_mode = getattr(args, "profile", "disabled") == "m8"
    if feature_mode:
        if program != "clone" or args.report_probe is not None:
            raise RuntimeError("candidate M8 direct CLI profile requires clone without the old Go report probe")
        import copy_features
        import cutover
        copy_features.load_manifest()
    if cutover_mode:
        if program != "follow" or args.report_probe is not None or diagnostic_case or getattr(args, "smoke", False):
            raise RuntimeError("cutover producer mode requires its distinct follow route without the clone-only probe")
        import cutover
    if diagnostic_case and (program != "follow" or diagnostic_case != "before_commit_rollback" or getattr(args, "smoke", False) or not args.receipt):
        raise RuntimeError("follow diagnostic requires one supported case and an explicit receipt")
    if not args.pair or args.pair not in PAIRS:
        raise RuntimeError("select exactly one nonempty supported pair")
    if args.report_probe is not None and (not args.report_probe or not Path(args.report_probe).is_file() or not os.access(args.report_probe, os.X_OK)):
        raise RuntimeError("selected authoritative report probe is unavailable")
    runtime = os.environ.get("CONTAINER_RUNTIME", "podman")
    if not shutil.which(runtime):
        raise RuntimeError("required container runtime unavailable")
    token = "rpgmigrate-" + ("m8" if feature_mode else "m2" if program == "clone" else "m3") + "-" + uuid.uuid4().hex
    label = "io.rpgmigrate.e2e=" + token
    image = "localhost/" + token + ":dev"
    output = Path(args.receipt).absolute() if args.receipt else ROOT / "target/e2e" / (("clone-m8-" if feature_mode else "" if program == "clone" else program + ("-cutover-" if cutover_mode else "-smoke-" if getattr(args, "smoke", False) else "-")) + args.pair + ".json")
    if output.is_symlink() or (diagnostic_case or feature_mode) and output.exists():
        raise RuntimeError("explicit diagnostic/candidate receipt already exists or is replaced")
    output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    output = output.parent.resolve() / output.name
    ledger_path = output.parent / (output.name + "." + token + ".runtime.json")
    if ledger_path.exists() or ledger_path.is_symlink():
        raise RuntimeError("fresh runtime ledger destination already exists")
    guard = RuntimeGuard(runtime, token, ledger_path)
    private_parent = ROOT / "target/e2e"
    if any(path.is_symlink() for path in (private_parent, private_parent.parent)):
        raise RuntimeError("run-owned shared parent is replaced")
    private_parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    private = private_parent / (token + ".private")
    if private.exists() or private.is_symlink():
        raise RuntimeError("fresh runtime private path already exists")
    private.mkdir(mode=0o700)
    private_identity = directory_identity(private)
    if private_identity is None:
        raise RuntimeError("owned private directory identity is unavailable")
    private_identity.update(nonce=token.rsplit("-", 1)[1], absent_before=True)
    guard.ledger["private_path"] = str(private)
    guard.ledger["private_absent_before"] = True
    guard.ledger["private_identity"] = private_identity
    guard.persist()
    server_arguments = server_start_arguments(getattr(args, "profile", "disabled"), server_arguments)
    guard.ledger["server_arguments"] = list(server_arguments)
    server_images, server_identities = [], []
    try:
        import cutover
        guard.ledger["stage"] = "source"
        source_identity = {"commit": guard.execute(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).decode().strip(),
                           "git_tree": guard.execute(["git", "-C", str(ROOT), "rev-parse", "HEAD^{tree}"]).decode().strip(),
                           "branch": guard.execute(["git", "-C", str(ROOT), "branch", "--show-current"]).decode().strip()}
        if (not re.fullmatch(r"[a-f0-9]{40}", source_identity["commit"]) or not re.fullmatch(r"[a-f0-9]{40}", source_identity["git_tree"])
                or source_identity["branch"] in ("", "main", "master") or guard.execute(["git", "-C", str(ROOT), "status", "--porcelain"])):
            raise RuntimeError("workflow source must be a clean committed feature branch")
        source_context = private / "source"
        materialize_source(ROOT, source_context, execute=guard.execute)
        source_tree = cutover.source_inventory(source_context)
        source_identity["inventory"] = source_tree
        guard.ledger["source"] = source_identity
        cache, cargo_cache = private / "build", private / "cargo"
        for path in (cache, cargo_cache):
            path.mkdir(mode=0o700)
        secret = uuid.uuid4().hex
        FIXTURE_SECRETS.add(secret)
        envfile = private / "environment"
        with envfile.open("x") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write("POSTGRES_PASSWORD=" + secret + "\nPGPASSWORD=" + secret + "\n")
        diagnostics = DIAGNOSTIC_ROOT / token
        diagnostics.mkdir(mode=0o700, parents=True)
        receipt_dir = private / "receipt"
        receipt_dir.mkdir(mode=0o700)
        checksums = private / "source-checksums"
        lines = []
        for path in sorted(source_context.rglob("*")):
            if path.is_file():
                if any(character in str(path) for character in ("\n", "\r", "\\")):
                    raise RuntimeError("shared source checksum filename is unsupported")
                lines.append(hashlib.sha256(path.read_bytes()).hexdigest() + "  " + str(path))
        with checksums.open("x") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write("\n".join(lines) + "\n")
        guard.host(["sha256sum", "--check", "--status", str(checksums)], timeout=20)
        guard.host(["test", "-r", str(envfile)])
        guard.ledger["shared_source_verified"] = {"files": len(lines), "sha256": hashlib.sha256(checksums.read_bytes()).hexdigest()}
        guard.persist()
        def ensure_image(reference):
            if not guard.exists("image", reference):
                raise RuntimeError("pinned public image reference is absent; provision it before protected baseline admission")
            identity = image_identity(json.loads(guard.raw(["image", "inspect", reference]))[0]["Id"])
            guard.ledger.setdefault("admitted_images", []).append(identity)
            guard.persist()
            return identity
        base = re.search(r"^FROM (\S+)$", (source_context / "tools/e2e/Containerfile").read_text(), re.MULTILINE)
        if not base or not re.fullmatch(r"[^\s]+@sha256:[a-f0-9]{64}", base[1]):
            raise RuntimeError("development builder base is not digest pinned")
        ensure_image(base[1])
        if guard.exists("image", image):
            raise RuntimeError("run-unique development image already exists")
        guard.ledger["development_image"] = {"tag": image, "absent_before": True, "retained": True}
        guard.persist()
        cpu_arguments = build_cpu_arguments(guard.raw(["build", "--help"]))
        guard.ledger["image_build_limits"] = {"requested_memory_bytes": 4*GIB, "requested_cpu_arguments": cpu_arguments,
                                               "effective_cgroup_status": "not_proven_for_ephemeral_image_build"}
        guard.ledger["stage"] = "development_image"
        guard.persist()
        built = guard._create([runtime, "build", "--jobs", "2", "--memory", "4g", *cpu_arguments, "--label", label,
                              "--network", "private", "--tag", image, "--file", str(source_context / "tools/e2e/Containerfile"), str(source_context / "tools/e2e")],
                             {"kind": "image", "tag": image}, timeout=1200)
        image_id = built["id"]
        external = set(guard.raw(["ps", "--all", "--external", "--no-trunc", "--format", "{{.ID}}"]).decode().splitlines())
        unresolved = sorted(external.difference(guard.baseline["external"]))
        guard.ledger["unresolved_external_build_ids"] = unresolved
        if unresolved:
            guard.persist()
            raise RuntimeError("development image build left unattributed external containers")
        guard.persist()
        def owned_container(arguments, *, role, image_id, mounts, memory, timeout=None):
            name = token + "-" + role
            if guard.exists("container", name):
                raise RuntimeError("fresh container name already exists")
            entry = guard._create([runtime, "create", "--name", name, "--image-volume=ignore", *arguments],
                                  {"kind": "container", "role": role, "image": image_id, "name": name, "mounts": mounts,
                                   "memory": memory, "network": None if role == "build" else {"name": token, "id": network_id}})
            cid = entry["id"]
            if timeout is None:
                guard.execute([runtime, "start", cid])
                return entry
            try:
                return guard.execute([runtime, "start", "--attach", cid], timeout=timeout)
            finally:
                failed = sys.exc_info()[0] is not None
                guard.cleanup_container(entry)
                if entry["cleanup_status"] != "absent" and not failed:
                    raise RuntimeError("exact owned " + role + " cleanup could not be verified")
        build_mounts = [(str(source_context), "/workspace", False), (str(cache), "/build", True), (str(cargo_cache), "/cargo", True)]
        fetch = ["--network", "pasta", "--memory", "4g", "--cpus", "2", "--label", label, "--env", "CARGO_TARGET_DIR=/build", "--env", "CARGO_HOME=/cargo", "--env", "CARGO_BUILD_JOBS=2",
                 "--volume", str(source_context) + ":/workspace:ro", "--volume", str(cache) + ":/build", "--volume", str(cargo_cache) + ":/cargo", image_id, "cargo", "build", "--locked"]
        guard.ledger["stage"] = "cargo_build"
        guard.persist()
        owned_container(fetch, timeout=1200, role="build", image_id=image_id, mounts=build_mounts, memory=4*GIB)
        if cutover.source_inventory(source_context) != source_identity["inventory"]:
            raise RuntimeError("source changed during source-bound debug build")
        if guard.exists("network", token):
            raise RuntimeError("fresh network name already exists")
        network = guard._create([runtime, "network", "create", "--internal", "--label", label, token], {"kind": "network", "name": token})
        network_id = network["id"]
        for role, major in zip(("source", "target"), args.pair.split("-")):
            guard.ledger["stage"] = role + "_start"
            guard.persist()
            server_image = "docker.io/library/postgres:" + major + "@" + manifest["images"][major]
            identity = ensure_image(server_image)
            server_images.append(identity)
            data_directory = "/var/lib/postgresql/data" if major == "16" else "/var/lib/postgresql/18/docker"
            entry = owned_container(["--network", token, "--network-alias", role, "--label", label, "--env-file", str(envfile),
                                     "--env", "PGDATA=" + data_directory, "--memory", "1g", "--cpus", "2", identity, *server_arguments],
                                    role=role, image_id=identity, mounts=[], memory=GIB)
            cid = entry["id"]
            def ready():
                guard.checkpoint()
                result = subprocess.run([runtime, "exec", cid, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"], capture_output=True, timeout=5)
                guard.checkpoint()
                if result.returncode not in (0, 1, 2):
                    raise RuntimeError("owned server readiness observation unavailable")
                return result.returncode == 0
            try:
                wait_for(ready, role + " server readiness")
                details = json.loads(guard.execute([runtime, "exec", cid, "psql", "-X", "-qAt", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-c",
                    "SELECT json_build_object('system_identifier',(SELECT system_identifier::text FROM pg_catalog.pg_control_system()),'version',split_part(current_setting('server_version'),' ',1),'data_directory',current_setting('data_directory'),'wal_level',current_setting('wal_level'));"]))
                permissions = guard.execute([runtime, "exec", cid, "stat", "-c", "%a|%U", data_directory]).strip()
                actual = validate_owned_container(guard.inspect(cid), entry, token, guard.baseline["containers"])
                network = actual.get("NetworkSettings", {}).get("Networks", {}).get(token, {})
                if (details.get("data_directory") != data_directory or permissions != b"700|postgres"
                        or not re.fullmatch(r"[0-9]{19,20}", details.get("system_identifier", ""))
                        or details["system_identifier"] in {"7705796426308472853", "7705919902701522967", *(row["system_identifier"] for row in server_identities)}
                        or feature_mode and (details.get("version") != {"16": "16.15", "18": "18.6"}[major] or details.get("wal_level") != "logical")
                        or role not in network.get("Aliases", []) or network.get("NetworkID") != network_id):
                    raise RuntimeError("actual fresh server identity, startup settings, alias or PGDATA differ")
                guard.observe_caps(entry, actual)
                entry.update(ready=True, server=details)
                server_identities.append({**details, "id": cid, "image": identity, "role": role, "started_at": actual["State"]["StartedAt"], "pgdata_permissions": "700|postgres"})
                guard.persist()
            except Exception:
                try:
                    entry["startup_state"] = {key: guard.inspect(cid)["State"].get(key) for key in ("Running", "Status", "StartedAt", "FinishedAt")}
                    entry["startup_diagnostic"] = private_failure("owned-server-startup", None, guard.raw(["logs", "--tail", "200", cid]), None, sys._getframe())
                    guard.persist()
                except Exception:
                    entry["startup_diagnostic_unavailable"] = True
                raise
        if feature_mode:
            binary = cache / "debug/rpgmigrate"
            if binary.is_symlink() or not binary.is_file() or not os.access(binary, os.X_OK):
                raise RuntimeError("source-bound debug executable is absent or replaced")
            feature_binding = {"binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "source_tree_sha256": source_tree["sha256"],
                               "source_commit": source_identity["commit"], "source_git_tree": source_identity["git_tree"],
                               "runner_image_id": image_id, "server_image_ids": server_images, "servers": server_identities, "run": token.rsplit("-", 1)[1]}
            (receipt_dir / "m8-input.json").write_text(json.dumps(feature_binding))
            guard.ledger["binding"] = feature_binding
            guard.persist()
        command = ["--network", token, "--label", label, "--memory", "4g", "--cpus", "2", "--env-file", str(envfile),
                   "--env", "CARGO_TARGET_DIR=/build", "--env", "CARGO_HOME=/cargo", "--volume", str(source_context) + ":/workspace:ro",
                   "--volume", str(cache) + ":/build", "--volume", str(cargo_cache) + ":/cargo", "--volume", str(receipt_dir) + ":/receipt", "--volume", str(diagnostics) + ":/diagnostics"]
        mounts = [*build_mounts, (str(receipt_dir), "/receipt", True), (str(diagnostics), "/diagnostics", True)]
        if args.report_probe is not None:
            command += ["--volume", str(Path(args.report_probe).resolve()) + ":/report-probe:ro"]
            mounts.append((str(Path(args.report_probe).resolve()), "/report-probe", False))
        guard.ledger["stage"] = "profiles"
        guard.persist()
        result = owned_container([*command, image_id, "python3", "tools/e2e/" + program + ".py", "--inside", "--pair", args.pair,
                      "--binary", "/build/debug/rpgmigrate", "--receipt", "/receipt/result.json",
                      *(["--report-probe", "/report-probe"] if args.report_probe is not None else []),
                      *(["--profile", "m8", "--feature-binding", "/receipt/m8-input.json"] if feature_mode else []),
                      *(["--smoke"] if getattr(args, "smoke", False) else []), *(["--cutover"] if cutover_mode else []),
                      *(["--diagnostic-case", diagnostic_case] if diagnostic_case else [])], timeout=1200, role="runner", image_id=image_id, mounts=mounts, memory=4*GIB)
        report = json.loads((receipt_dir / "result.json").read_text())
        report.update(runner_image_id=image_id, server_image_ids=server_images)
        if (cutover.source_inventory(source_context) != source_identity["inventory"] or guard.execute(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).decode().strip() != source_identity["commit"]
                or guard.execute(["git", "-C", str(ROOT), "status", "--porcelain"])):
            raise RuntimeError("actual workflow source changed during qualification")
        if cutover_mode and report.get("source_tree") != source_tree:
            raise RuntimeError("cutover producer receipt differs from the actual unchanged runner source tree")
        guard.ledger["stage"] = "receipt_validation"
        guard.persist()
        if feature_mode:
            copy_features.validate_receipt(report, manifest, binding=feature_binding)
        else:
            receipt_validator(report, manifest)
        guard.ledger["expected_roles_verified"] = sorted(entry["role"] for entry in guard.ledger["containers"]) == ["build", "runner", "source", "target"]
        if not guard.ledger["expected_roles_verified"] or not all(entry.get("cgroup_verified") is True for entry in guard.ledger["containers"]):
            raise RuntimeError("four exact owned runtime roles and effective cgroup caps were not observed")
    except Exception as error:
        error.runtime_ledger = str(ledger_path)
        if isinstance(error, SubprocessFailure):
            error.metadata["runtime_ledger"] = str(ledger_path)
        guard.ledger["failure"] = {"kind": type(error).__name__, "stage": guard.ledger.get("stage", "setup")}
        raise
    finally:
        failed = sys.exc_info()[0] is not None
        try:
            smoke_path = private / "receipt/m8-smoke.json"
            if smoke_path.is_file() and not smoke_path.is_symlink():
                if smoke_path.stat().st_size > 65536 or smoke_path.stat().st_mode & 0o777 != 0o600:
                    raise RuntimeError("retained build smoke exceeded its bound or private mode differs")
                smoke = json.loads(smoke_path.read_bytes(), object_pairs_hook=unique_object)
                if not feature_mode or "binding" not in guard.ledger:
                    raise RuntimeError("retained build smoke lacks a reviewed candidate binding")
                copy_features.validate_artifact_binding(guard.ledger["binding"])
                copy_features.validate_build_smoke(smoke, guard.ledger["binding"], args.pair)
                guard.ledger["build_smoke"] = smoke
        except Exception:
            guard.ledger["build_smoke_unavailable"] = True
        try:
            guard.close()
        except Exception as error:
            guard.ledger["close_error_kind"] = type(error).__name__
            if not failed:
                raise
        try:
            if (private_identity.get("absent_before") is True and private_identity.get("nonce") == token.rsplit("-", 1)[1]
                    and directory_identity(private) == {key: value for key, value in private_identity.items() if key not in ("nonce", "absent_before")}
                    and guard.ledger.get("cleanup_complete") is True
                    and not guard._creation_unresolved()
                    and all(entry.get("cleanup_status") == "absent" for entry in guard.ledger["containers"])):
                shutil.rmtree(private)
            guard.ledger["private_removed"] = not private.exists() and not private.is_symlink()
            guard.ledger["secret_file_absent"] = guard.ledger["private_removed"]
            guard.persist()
        except Exception as error:
            guard.ledger["private_cleanup_error_kind"] = type(error).__name__
            if not failed:
                raise
    if not guard.ledger["cleanup_complete"] or not guard.ledger["private_removed"]:
        raise RuntimeError("exact owned cleanup or preservation could not be verified; inspect " + str(ledger_path))
    if feature_mode and (guard.ledger.get("build_smoke_unavailable") or guard.ledger.get("build_smoke") != report.get("build_smoke") or not guard.ledger.get("build_smoke")):
        raise RuntimeError("source-bound pre-workflow debug smoke is missing or differs; inspect " + str(ledger_path))
    report["runtime_ledger"] = str(ledger_path)
    with output.open("x" if diagnostic_case or feature_mode else "w") as stream:
        stream.write(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(result.decode(), end="")
    print(program + " receipt: " + str(output))


def main():
    global DIAGNOSTIC_ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair", required=True, choices=PAIRS)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--binary", default="/build/debug/rpgmigrate")
    parser.add_argument("--profile", choices=("disabled", "m8"), default="disabled", help="explicit candidate profile; M8 requires separate runtime admission")
    parser.add_argument("--feature-binding", help=argparse.SUPPRESS)
    parser.add_argument("--receipt")
    parser.add_argument("--report-probe", default=os.environ.get("RPGMIGRATE_E2E_REPORT_PROBE"), help="optional authoritative operator Go ParseReport executable")
    args = parser.parse_args()
    if sys.flags.optimize:
        raise RuntimeError("optimized Python cannot run qualification")
    if args.inside:
        DIAGNOSTIC_ROOT = Path("/diagnostics")
        if not args.receipt:
            raise RuntimeError("inside qualification requires a receipt destination")
        if args.profile == "m8":
            import copy_features
            copy_features.inner(args)
        else:
            inner(args)
    else:
        outer(args)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        if isinstance(error, SubprocessFailure):
            metadata = error.metadata
        else:
            trace = error.__traceback__
            while trace.tb_next is not None:
                trace = trace.tb_next
            metadata = private_failure("python-exception", None, str(error).encode(), None, trace.tb_frame, trace.tb_lineno)
        if getattr(error, "runtime_ledger", None):
            metadata["runtime_ledger"] = error.runtime_ledger
        print("clone-e2e: " + json.dumps(metadata, sort_keys=True), file=sys.stderr)
        sys.exit(1)
