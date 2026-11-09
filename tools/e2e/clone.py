#!/usr/bin/env python3
"""Qualify plain clone in disposable PostgreSQL containers, never operator runtime."""

import argparse
from contextlib import ExitStack
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
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


def validate_receipt(report, manifest, *, require_reports=True):
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

    def start(self, source, target, state, *arguments, gate=None):
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
                                        "--skip-large-objects", *arguments], env=environment, stdout=stream,
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
    validate_receipt({**receipt, "cases": {name: {"observed": True} for name in manifest["cases"]}}, manifest, require_reports=False)
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
        for name, flags in (("binary_refusal", ("--use-copy-binary",)), ("split_refusal", ("--split-tables-larger-than", "1MB"))):
            srcname, dstname, src, dst, state = case(name)
            before = target.catalog_signature(dstname)
            driver.finish(driver.start(src, dst, state, *flags), success=False, reason="unimplemented option " + flags[0])
            if before != target.catalog_signature(dstname):
                raise RuntimeError("unsupported option mutated target")
            receipt["cases"][name] = {"observed": True}
        srcname, dstname, src, dst, state = case("lo")
        source.sql(srcname, "SELECT lo_create(90001);")
        before = target.catalog_signature(dstname)
        driver.finish(driver.start(src, dst, state), success=False, reason="large")
        if before != target.catalog_signature(dstname) or target.sql(dstname, "SELECT count(*) FROM pg_largeobject_metadata;").strip() != b"0":
            raise RuntimeError("unsupported large object mutated target")
        receipt["cases"]["large_object_refusal"] = {"observed": True}
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


def outer(args, *, program="clone", manifest=None, receipt_validator=validate_receipt, server_arguments=()):
    manifest = load_manifest() if manifest is None else manifest
    diagnostic_case = getattr(args, "diagnostic_case", None)
    cutover_mode = getattr(args, "cutover", False)
    if cutover_mode:
        if program != "follow" or args.report_probe is not None or diagnostic_case or getattr(args, "smoke", False):
            raise RuntimeError("cutover producer mode requires its distinct follow route without the clone-only probe")
        import cutover
        source_tree = cutover.source_inventory(ROOT)
    if diagnostic_case and (program != "follow" or diagnostic_case != "before_commit_rollback" or getattr(args, "smoke", False) or not args.receipt):
        raise RuntimeError("follow diagnostic requires one supported case and an explicit receipt")
    if not args.pair or args.pair not in PAIRS:
        raise RuntimeError("select exactly one nonempty supported pair")
    if args.report_probe is not None and (not args.report_probe or not Path(args.report_probe).is_file() or not os.access(args.report_probe, os.X_OK)):
        raise RuntimeError("selected authoritative report probe is unavailable")
    runtime = os.environ.get("CONTAINER_RUNTIME", "podman")
    if not shutil.which(runtime):
        raise RuntimeError("required container runtime unavailable")
    token = "rpgmigrate-" + ("m2" if program == "clone" else "m3") + "-" + uuid.uuid4().hex[:12]
    label = "io.rpgmigrate.e2e=" + token
    image = "localhost/rpgmigrate-m2-dev:stock"
    output = Path(args.receipt).resolve() if args.receipt else ROOT / "target/e2e" / (("" if program == "clone" else program + ("-cutover-" if cutover_mode else "-smoke-" if getattr(args, "smoke", False) else "-")) + args.pair + ".json")
    if diagnostic_case and output.exists():
        raise RuntimeError("follow diagnostic receipt already exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    ledger_path = output.parent / (output.name + "." + token + ".runtime.json")
    ledger = {"version": 1, "scope": "runtime_resource_ownership", "run": token, "containers": [], "cleanup_complete": False}
    def persist_ledger():
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=ledger_path.parent, delete=False) as stream:
            temporary_ledger = Path(stream.name)
            json.dump(ledger, stream, sort_keys=True)
            stream.write("\n")
        temporary_ledger.replace(ledger_path)
    def cleanup_container(entry):
        cid = entry["id"]
        try:
            owned = subprocess.run([runtime, "inspect", "--format", '{{ index .Config.Labels "io.rpgmigrate.e2e" }}', cid], capture_output=True, timeout=5)
            entry["ownership_label_verified"] = owned.returncode == 0 and owned.stdout.strip() == token.encode()
            if entry["ownership_label_verified"]:
                removed = subprocess.run([runtime, "rm", "--force", "--volumes", cid], capture_output=True, timeout=10)
                entry["remove_exit"] = removed.returncode
            absent = subprocess.run([runtime, "container", "exists", cid], capture_output=True, timeout=5)
            entry["cleanup_status"] = "absent" if absent.returncode == 1 else "present" if absent.returncode == 0 else "unavailable"
        except Exception:
            entry["cleanup_status"] = "unavailable"
        try:
            persist_ledger()
        except Exception:
            entry["ledger_write_failed"] = True
    def retain_container(cid, role, cleanup):
        entry = {"id": cid, "role": role, "ownership_label_verified": False, "cleanup_status": "pending"}
        ledger["containers"].append(entry)
        cleanup.callback(cleanup_container, entry)
        persist_ledger()
    def finalize_ledger(_kind, error, _trace):
        ledger["cleanup_complete"] = bool(ledger["containers"]) and all(entry["cleanup_status"] == "absent" and entry["ownership_label_verified"] for entry in ledger["containers"])
        try:
            persist_ledger()
        except Exception:
            if error is None:
                raise RuntimeError("runtime ownership ledger could not be retained") from None
        if isinstance(error, SubprocessFailure):
            error.metadata["runtime_ledger"] = str(ledger_path)
        if error is None and not ledger["cleanup_complete"]:
            raise RuntimeError("exact owned container cleanup could not be verified")
        return False
    persist_ledger()
    cache = ROOT / "target/e2e/build"
    cargo_cache = ROOT / "target/e2e/cargo"
    for path in (cache, cargo_cache):
        path.mkdir(parents=True, exist_ok=True)
    run([runtime, "build", "--jobs", "2", "--memory", "4g", "--tag", image, "--file", str(ROOT / "tools/e2e/Containerfile"), str(ROOT / "tools/e2e")], timeout=1200)
    image_id = run([runtime, "image", "inspect", image, "--format", "{{.Id}}"]).decode().strip()
    with tempfile.TemporaryDirectory(prefix=token, dir=ROOT / "target/e2e") as temporary, ExitStack() as cleanup:
        cleanup.push(finalize_ledger)
        private = Path(temporary)
        secret = uuid.uuid4().hex
        FIXTURE_SECRETS.add(secret)
        diagnostics = DIAGNOSTIC_ROOT / token
        diagnostics.mkdir(mode=0o700, parents=True)
        envfile = private / "environment"
        envfile.touch(mode=0o600)
        envfile.write_text("POSTGRES_PASSWORD=" + secret + "\nPGPASSWORD=" + secret + "\n")
        run([runtime, "network", "create", "--internal", "--label", label, token])
        cleanup.callback(subprocess.run, [runtime, "network", "rm", token], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        server_images = []
        for role, major in zip(("source", "target"), args.pair.split("-")):
            server_image = "docker.io/library/postgres:" + major + "@" + manifest["images"][major]
            # Pull happens outside the private network; record the resolved immutable image.
            if subprocess.run([runtime, "image", "exists", server_image], capture_output=True).returncode:
                run([runtime, "pull", server_image], timeout=600)
            server_images.append(json.loads(run([runtime, "image", "inspect", server_image]).decode())[0]["Id"])
            name = token + "-" + role
            cid = run([runtime, "run", "--detach", "--network", token, "--network-alias", role,
                       "--label", label, "--name", name, "--env-file", str(envfile),
                       "--memory", "1g", "--cpus", "2", server_image, *server_arguments]).decode().strip()
            if not re.fullmatch(r"[a-f0-9]{64}", cid):
                raise RuntimeError("owned server creation returned no exact container identity")
            retain_container(cid, role, cleanup)
            def ready():
                result = subprocess.run([runtime, "exec", cid, "pg_isready", "-h", "127.0.0.1", "-U", "postgres"], capture_output=True, timeout=5)
                return result.returncode == 0
            wait_for(ready, role + " server readiness")
        receipt_dir = private / "receipt"
        receipt_dir.mkdir()
        command = ["--network", token, "--label", label, "--memory", "4g", "--cpus", "2",
                   "--name", token + "-runner", "--env-file", str(envfile), "--env", "CARGO_TARGET_DIR=/build", "--env", "CARGO_HOME=/cargo",
                   "--volume", str(ROOT) + ":/workspace:ro", "--volume", str(cache) + ":/build",
                   "--volume", str(cargo_cache) + ":/cargo", "--volume", str(receipt_dir) + ":/receipt",
                   "--volume", str(diagnostics) + ":/diagnostics", image_id]
        if args.report_probe is not None:
            command[-1:-1] = ["--volume", str(Path(args.report_probe).resolve()) + ":/report-probe:ro"]
        def owned_container(arguments, timeout, role):
            cid = run([runtime, "create", *arguments]).decode().strip()
            if not re.fullmatch(r"[a-f0-9]{64}", cid):
                raise RuntimeError("owned container creation returned no exact identity")
            retain_container(cid, role, cleanup)
            return run([runtime, "start", "--attach", cid], timeout=timeout)
        # The dependency fetch is allowed network access; runtime database checks use only the private network.
        fetch = ["--memory", "4g", "--cpus", "2", "--label", label,
                 "--env", "CARGO_TARGET_DIR=/build", "--env", "CARGO_HOME=/cargo", "--env", "CARGO_BUILD_JOBS=2",
                 "--volume", str(ROOT) + ":/workspace:ro", "--volume", str(cache) + ":/build",
                 "--volume", str(cargo_cache) + ":/cargo", image_id, "cargo", "build", "--locked"]
        owned_container(fetch, timeout=1200, role="build")
        result = owned_container([*command, "python3", "tools/e2e/" + program + ".py", "--inside", "--pair", args.pair,
                      "--binary", "/build/debug/rpgmigrate", "--receipt", "/receipt/result.json",
                      *(["--report-probe", "/report-probe"] if args.report_probe is not None else []),
                      *(["--smoke"] if getattr(args, "smoke", False) else []),
                      *(["--cutover"] if cutover_mode else []),
                      *(["--diagnostic-case", diagnostic_case] if diagnostic_case else [])], timeout=1200, role="runner")
        report = json.loads((receipt_dir / "result.json").read_text())
        report.update(runner_image_id=image_id, server_image_ids=server_images)
        if cutover_mode and (report.get("source_tree") != source_tree or cutover.source_inventory(ROOT) != source_tree):
            raise RuntimeError("cutover producer receipt differs from the actual unchanged runner source tree")
        receipt_validator(report, manifest)
        with output.open("x" if diagnostic_case else "w") as stream:
            stream.write(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(result.decode(), end="")
        print(program + " receipt: " + str(output))


def main():
    global DIAGNOSTIC_ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair", required=True, choices=PAIRS)
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--binary", default="/build/debug/rpgmigrate")
    parser.add_argument("--receipt")
    parser.add_argument("--report-probe", default=os.environ.get("RPGMIGRATE_E2E_REPORT_PROBE"), help="optional authoritative operator Go ParseReport executable")
    args = parser.parse_args()
    if sys.flags.optimize:
        raise RuntimeError("optimized Python cannot run qualification")
    if args.inside:
        DIAGNOSTIC_ROOT = Path("/diagnostics")
        if not args.receipt:
            raise RuntimeError("inside qualification requires a receipt destination")
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
        print("clone-e2e: " + json.dumps(metadata, sort_keys=True), file=sys.stderr)
        sys.exit(1)
