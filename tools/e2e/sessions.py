"""Census of the engine's PostgreSQL session names across clone, follow to endpos, compare and cleanup."""

import ipaddress
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time
import uuid

import clone

NAME = re.compile(r"rpgmigrate\[([1-9][0-9]*)\] (copy|apply|receive|compare)")
VARIANTS = ("default", "environment", "conninfo")
# Roles each engine invocation may open; follow compares at completion, and pg_dump children carry copy.
PHASE_ROLES = {"follow": {"copy", "apply", "receive", "compare"}, "endpos": {"apply"}, "compare-data": {"compare"},
               "compare-schema": {"compare", "copy"}, "cleanup": {"receive"}}
# (side, phase, role) by name; a fourth item names the client process or walsender that held it.
REQUIRED = sorted({
    ("source", "follow", "copy"), ("source", "follow", "copy", "pg_dump"),
    ("source", "follow", "receive"), ("source", "follow", "receive", "walsender"),
    ("target", "follow", "copy"), ("target", "follow", "copy", "pg_restore"), ("target", "follow", "apply"),
    ("target", "endpos", "apply"), ("source", "compare-data", "compare"), ("target", "compare-data", "compare"),
    ("source", "compare-schema", "compare"), ("target", "compare-schema", "compare"),
    ("source", "compare-schema", "copy", "pg_dump"), ("target", "compare-schema", "copy", "pg_dump"),
    ("source", "cleanup", "receive"), ("target", "cleanup", "receive"),
})
# A pinned name hides the PID and role, so these require sessions joined to the long-lived follow processes.
REQUIRED_PINNED = sorted({("source", "follow", "rpgmigrate"), ("source", "follow", "pg_dump"), ("source", "follow", "walsender"),
                          ("target", "follow", "rpgmigrate"), ("target", "follow", "pg_restore")})
SAMPLE = """CREATE FUNCTION {sample}(databases text[], target int DEFAULT NULL) RETURNS jsonb LANGUAGE sql AS $sample$
  SELECT coalesce(jsonb_object_agg(concat_ws('/', a.pid, a.backend_start, a.state, a.application_name),
      jsonb_build_object('pid', a.pid, 'start', a.backend_start::text, 'type', a.backend_type, 'name', a.application_name,
                         'port', a.client_port, 'state', a.state)), '{{}}')
  FROM pg_catalog.pg_stat_activity a
  WHERE a.datname = ANY(databases) AND a.backend_type IN ('client backend', 'walsender') AND (target IS NULL OR a.pid = target)
$sample$;"""
# The census servers log each authorized connection as JSON in their data directory, where a superuser can read it back.
SERVER_ARGUMENTS = ("-c", "logging_collector=on", "-c", "log_destination=jsonlog", "-c", "log_connections=on", "-c", "log_timezone=UTC")
LINES = """CREATE FUNCTION {lines}() RETURNS SETOF text LANGUAGE sql AS $lines$
  SELECT pg_catalog.regexp_split_to_table(pg_catalog.pg_read_file(pg_catalog.current_setting('log_directory') || '/' || f.name), E'\\n')
  FROM pg_catalog.pg_ls_logdir() f WHERE f.name LIKE '%.json'
$lines$;"""
# The log keeps a backend's start to the second (MyStartTime), so pid, start second, client port and database identify it.
LOGGED = """CREATE FUNCTION {logged}(databases text[], backend jsonb) RETURNS jsonb LANGUAGE sql AS $logged$
  SELECT coalesce(jsonb_agg(jsonb_build_object('user', e->>'user', 'database', e->>'dbname', 'message', e->>'message')), '[]')
  FROM (SELECT line::jsonb AS e FROM {lines}() line WHERE pg_catalog.pg_input_is_valid(line, 'jsonb')) l
  WHERE e->>'backend_type' = 'client backend' AND e->>'message' LIKE 'connection authorized: %' AND e->>'dbname' = ANY(databases)
    AND e->>'pid' = backend->>'pid' AND e->>'remote_port' = backend->>'port'
    AND (e->>'session_start')::timestamptz = pg_catalog.date_trunc('second', (backend->>'start')::timestamptz)
$logged$;"""
# pgstat_bestart (16) and pgstat_bestart_final (18) publish a NULL state before pgstat_report_appname, so a session sampled
# only there looks unnamed; that backend alone is re-read until it leaves the window or a second passes, and one gone by then
# gets its logged connections.
SAMPLER = """CREATE TABLE {stop}();
CREATE PROCEDURE {procedure}(databases text[], INOUT seen jsonb DEFAULT '{{}}') LANGUAGE plpgsql AS $census$
DECLARE deadline timestamptz := clock_timestamp() + interval '900 seconds';
  sample jsonb; latest jsonb; pending record; settle timestamptz;
BEGIN
  LOOP
    PERFORM pg_catalog.pg_stat_clear_snapshot();
    sample := {sample}(databases);
    seen := seen || sample;
    FOR pending IN SELECT key, value, (value->>'pid')::int AS pid, value->>'start' AS start FROM jsonb_each(sample)
                   WHERE coalesce(value->>'name', '') = '' AND value->>'state' IS NULL LOOP
      settle := clock_timestamp() + interval '1 second';
      LOOP
        PERFORM pg_catalog.pg_sleep(0.0002);
        PERFORM pg_catalog.pg_stat_clear_snapshot();
        latest := {sample}(databases, pending.pid);
        seen := seen || latest;
        IF NOT EXISTS (SELECT FROM jsonb_each(latest) WHERE value->>'start' = pending.start) THEN
          seen := seen || jsonb_build_object('gone/' || pending.key, pending.value || '{{"gone": true}}');
          EXIT;
        END IF;
        EXIT WHEN clock_timestamp() > settle OR NOT EXISTS (SELECT FROM jsonb_each(latest) WHERE value->>'start' = pending.start
          AND coalesce(value->>'name', '') = '' AND value->>'state' IS NULL);
      END LOOP;
    END LOOP;
    EXIT WHEN EXISTS (SELECT FROM {stop}) OR clock_timestamp() > deadline;
    PERFORM pg_catalog.pg_sleep(0.002);
  END LOOP;
  FOR pending IN SELECT key, value FROM jsonb_each(seen) WHERE value ? 'gone' LOOP
    seen := seen || jsonb_build_object(pending.key, pending.value || jsonb_build_object('logged', {logged}(databases, pending.value)));
  END LOOP;
END
$census$;"""


class ServerCensus:
    """Samples pg_stat_activity inside the server, so sessions of a few milliseconds are seen."""

    def __init__(self, database, name):
        self.database, self.name = database, name
        token = uuid.uuid4().hex[:12]
        self.stop_table, self.procedure, sample = "census_stop_" + token, "census_" + token, "census_sample_" + token
        lines, logged = "census_lines_" + token, "census_logged_" + token
        database.sql("postgres", SAMPLE.format(sample=sample) + LINES.format(lines=lines) + LOGGED.format(logged=logged, lines=lines)
                     + SAMPLER.format(stop=self.stop_table, procedure=self.procedure, sample=sample, logged=logged))
        self.address = database.sql("postgres", "SELECT host(inet_server_addr());").decode().strip()
        call = f"CALL {self.procedure}(ARRAY['{name}'], '{{}}'::jsonb);"
        self.process = subprocess.Popen([str(database.bindir / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
                                         "--dbname", database.connection("postgres"), "-c", call],
                                        env=database.environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        clone.wait_for(lambda: self.running(), "server session census running", timeout=10)

    def running(self):
        if self.process.poll() is not None:
            raise RuntimeError("server session census exited early")
        query = f"SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'CALL {self.procedure}(%' AND state='active';"
        return self.database.sql("postgres", query).strip() == b"1"

    def stop(self):
        self.database.sql("postgres", f"INSERT INTO {self.stop_table} DEFAULT VALUES;")
        try:
            output, _ = self.process.communicate(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            raise RuntimeError("server session census did not stop") from None
        if self.process.returncode != 0:
            raise RuntimeError("server session census failed")
        return list(json.loads(output).values())


# Scripted pg_stat_activity samples and connection log lines for one backend caught before its name; PASSING may pass the judge.
WINDOW = {"pid": 7, "start": "2026-11-16 10:00:00.25+00", "type": "client backend", "name": "", "port": 9000, "state": None}


def logline(name, start="2026-11-16 10:00:00 UTC", message=None, **fields):
    if message is None:
        message = "connection authorized: user=postgres database=control" + ("" if name is None else " application_name=" + name)
    return json.dumps({"user": "postgres", "dbname": "control", "pid": 7, "remote_port": 9000, "session_start": start,
                       "backend_type": "client backend", "message": message, **fields})


ENGINE = logline("rpgmigrate[100] compare")
ENGINE_OTHER_DATABASE = "connection authorized: user=postgres database=other application_name=rpgmigrate[100] compare"
DISCONNECTED = logline(None, message="disconnection: session time: 0:00:00.010 user=postgres")
CONTROLS = {"settles": ([[WINDOW], [WINDOW], [{**WINDOW, "name": "rpgmigrate[100] compare"}]], []),
            "stays_unnamed": ([[WINDOW]], [ENGINE]), "unnamed_idle": ([[WINDOW], [{**WINDOW, "state": "idle"}]], [ENGINE]),
            "vanishes": ([[WINDOW], []], []), "vanishes_logged": ([[WINDOW], []], [ENGINE, DISCONNECTED]),
            "vanishes_logged_other": ([[WINDOW], []], [logline("psql")]), "vanishes_logged_unnamed": ([[WINDOW], []], [logline(None)]),
            "vanishes_logged_elsewhere": ([[WINDOW], []], [logline("rpgmigrate[100] compare", "2026-11-16 10:00:01 UTC")]),
            "vanishes_logged_twice": ([[WINDOW], []], [ENGINE, ENGINE]),
            "vanishes_logged_other_backend": ([[WINDOW], []], [logline("rpgmigrate[100] compare", **field) for field in
                                                               ({"pid": 8}, {"remote_port": 9001}, {"backend_type": "walsender"},
                                                                {"dbname": "other", "message": ENGINE_OTHER_DATABASE})]),
            "vanishes_logged_harness": ([[WINDOW], []], [logline("harness")])}
PASSING = {"settles", "vanishes_logged", "vanishes_logged_harness"}
SCRIPT = """CREATE SEQUENCE {sample}_calls;
CREATE FUNCTION {sample}(databases text[], target int DEFAULT NULL) RETURNS jsonb LANGUAGE sql AS $script$
  SELECT samples->(least(nextval('{sample}_calls'), jsonb_array_length(samples)) - 1)::int FROM (SELECT $json${samples}$json$::jsonb AS samples) s
$script$;
CREATE FUNCTION {lines}() RETURNS SETOF text LANGUAGE sql AS $script$ SELECT jsonb_array_elements_text($json${log}$json$::jsonb) $script$;"""


def controls(source, target):
    """Runs the real sampler and log matcher over each scripted backend, then reads each server's own connection log back."""
    held = {}
    for side, database in (("source", source), ("target", target)):
        for name, (script, log) in CONTROLS.items():
            token = uuid.uuid4().hex[:12]
            stop, procedure, sample = "control_stop_" + token, "control_" + token, "control_sample_" + token
            lines, logged = "control_lines_" + token, "control_logged_" + token
            samples = json.dumps([{"/".join(str(row[key]) for key in ("pid", "start", "state", "name") if row[key] is not None): row
                                   for row in rows} for rows in script])
            output = database.sql("postgres", SCRIPT.format(sample=sample, samples=samples, lines=lines, log=json.dumps(log))
                                  + LOGGED.format(logged=logged, lines=lines)
                                  + SAMPLER.format(stop=stop, procedure=procedure, sample=sample, logged=logged)
                                  + f"INSERT INTO {stop} DEFAULT VALUES; CALL {procedure}(ARRAY['control'], '{{}}'::jsonb);")
            try:
                judge(side, list(json.loads(output).values()), {}, {100: "compare-data"}, "harness", None)
                passed = True
            except RuntimeError:
                passed = False
            if passed != (name in PASSING):
                raise RuntimeError(f"{side} session sampler control {name} did not hold")
            held.setdefault(side, []).append(name)
        connection_log(database)
        held[side].append("connection_log")
    return held


def connection_log(database):
    """Requires the real log reader to find exactly one line for an earlier harness connection, under its harness name."""
    token = uuid.uuid4().hex[:12]
    lines, logged = "census_lines_" + token, "census_logged_" + token
    backend = database.sql("postgres", LINES.format(lines=lines) + LOGGED.format(logged=logged, lines=lines)
                           + "SELECT jsonb_build_object('pid', pid, 'start', backend_start::text, 'port', client_port) "
                           + "FROM pg_catalog.pg_stat_activity WHERE pid = pg_catalog.pg_backend_pid();").decode().strip()
    expected = [{"user": "postgres", "database": "postgres", "message": "connection authorized: user=postgres database=postgres application_name="
                 + database.application}]
    query = f"SELECT {logged}(ARRAY['postgres'], $json${backend}$json$::jsonb);"
    clone.wait_for(lambda: json.loads(database.sql("postgres", query)) == expected, "server connection log line", timeout=10)


def address(text):
    """Decodes a /proc/net/tcp{,6} address, which stores each 32-bit word little-endian."""
    raw = bytes.fromhex(text)
    if len(raw) == 4:
        return str(ipaddress.IPv4Address(raw[::-1]))
    value = ipaddress.IPv6Address(b"".join(raw[index:index + 4][::-1] for index in range(0, 16, 4)))
    return str(value.ipv4_mapped or value)


class ProcessCensus(threading.Thread):
    """Maps (server address, local port) to the engine and its pg_dump and pg_restore children while they live."""

    KINDS = ("rpgmigrate", "pg_dump", "pg_restore")

    def __init__(self, proc=Path("/proc")):
        super().__init__(daemon=True)
        self.proc, self.ports, self.finished, self.failure = proc, {}, threading.Event(), None

    def run(self):
        try:
            while not self.finished.is_set():
                self.sample()
                time.sleep(0.002)
        except Exception as error:
            self.failure = error

    def sample(self):
        processes = {}
        for entry in self.proc.iterdir():
            if not entry.name.isdecimal():
                continue
            try:
                parent = int((entry / "stat").read_bytes().rsplit(b") ", 1)[1].split()[1])
                processes[int(entry.name)] = (parent, (entry / "exe").readlink().name)
            except (OSError, IndexError, ValueError):
                continue
        sockets = {}
        for pid, (_, kind) in processes.items():
            if kind not in self.KINDS:
                continue
            try:
                for descriptor in (self.proc / str(pid) / "fd").iterdir():
                    link = os.readlink(descriptor)
                    if link.startswith("socket:["):
                        sockets[int(link[8:-1])] = pid
            except OSError:
                continue
        if not sockets:
            return
        for table in ("net/tcp", "net/tcp6"):
            try:
                lines = (self.proc / table).read_text().splitlines()[1:]
            except OSError:
                continue
            for line in lines:
                fields = line.split()
                pid = sockets.get(int(fields[9]))
                if pid is None or fields[3] != "01":
                    continue
                engine = pid
                while engine in processes and processes[engine][1] != "rpgmigrate":
                    engine = processes[engine][0]
                # Connections to different servers may share a local port, so the server address is part of the key.
                key = (address(fields[2].rsplit(":", 1)[0]), int(fields[1].rsplit(":", 1)[1], 16))
                self.ports[key] = {"kind": processes[pid][1], "pid": pid, "engine": engine if engine in processes else None}

    def stop(self):
        self.finished.set()
        self.join(timeout=10)
        if self.is_alive() or self.failure is not None:
            raise RuntimeError("process session census failed")
        return self.ports


def logged_name(entries):
    """The application name in a backend's only connection-authorized line, or None."""
    if not isinstance(entries, list) or len(entries) != 1:
        return None
    entry = entries[0]
    prefix = f"connection authorized: user={entry['user']} database={entry['database']} application_name="
    message = entry["message"] or ""
    return message[len(prefix):] if message.startswith(prefix) and len(message) > len(prefix) else None


def judge(side, rows, ports, phases, harness, pinned):
    """Every non-harness session must carry the engine's name; returns observed coverage."""
    sessions, gone = {}, {}
    for row in rows:
        if row.get("gone"):
            gone[(row["pid"], row["start"])] = row.get("logged")
            continue
        session = sessions.setdefault((row["pid"], row["start"]), {"names": set(), "states": set(), "type": row["type"], "port": row["port"]})
        session["names"].add(row["name"] or "")
        session["states"].add(row["state"] or "")
    coverage, counts = set(), {"sessions": 0, "logged": 0, "joined": 0}
    for key, session in sessions.items():
        names = session["names"] - {""}
        # Only a client backend seen solely before its name and gone at the re-read may take its logged name.
        logged = not names and session["type"] == "client backend" and session["states"] == {""} and key in gone
        if logged:
            names = {logged_name(gone[key])} - {None}
        if harness in names:
            continue
        counts["logged"] += bool(logged and names)
        if not names:
            raise RuntimeError(f"{side} engine session has no application name")
        if len(names) != 1:
            raise RuntimeError(f"{side} engine session changed its application name")
        name = names.pop()
        client = ports.get(session["port"])
        kind = "walsender" if session["type"] == "walsender" else client["kind"] if client else None
        counts["sessions"] += 1
        counts["joined"] += client is not None
        if pinned is not None:
            if name != pinned:
                raise RuntimeError(f"{side} engine session replaced the caller's pinned name")
            phase = phases.get(client["engine"]) if client else None
            if phase is not None:
                coverage.add((side, phase, kind))
            continue
        match = NAME.fullmatch(name)
        if not match or int(match[1]) not in phases:
            raise RuntimeError(f"{side} session name is not the contract name of an engine invocation")
        pid, role = int(match[1]), match[2]
        phase = phases[pid]
        if role not in PHASE_ROLES[phase]:
            raise RuntimeError(f"{side} {phase} session has the wrong role {role}")
        if kind == "walsender" and role != "receive":
            raise RuntimeError(f"{side} walsender is not named receive")
        if client is not None and client["engine"] != pid:
            raise RuntimeError(f"{side} session name carries another invocation's PID")
        if kind in ("pg_dump", "pg_restore") and role != "copy":
            raise RuntimeError(f"{side} utility child is not named copy")
        if phase == "compare-schema" and role == "copy" and kind != "pg_dump":
            raise RuntimeError(f"{side} compare opened a copy session outside its pg_dump child")
        coverage.add((side, phase, role))
        if kind is not None:
            coverage.add((side, phase, role, kind))
    return coverage, counts


def engine(driver, state, phase, phases, argv):
    """Runs one engine command and records its PID, which its session names must carry."""
    environment, _ = driver.prepare(state, phase)
    process = subprocess.Popen([str(driver.binary), *argv, "--dir", str(state)], env=environment,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    phases[process.pid] = phase
    try:
        stdout, stderr = process.communicate(timeout=120)
    except subprocess.TimeoutExpired:
        process.kill()
        raise RuntimeError(phase + " command timed out") from None
    if driver.environment.get("PGPASSWORD", "\x00").encode() in stdout + stderr:
        raise RuntimeError(phase + " command leaked a credential")
    if process.returncode != 0:
        raise clone.SubprocessFailure(clone.private_failure("engine-" + phase, process.returncode, stderr, environment, sys._getframe()))
    return stdout


LOGS = """SELECT coalesce(json_object_agg(f.name, pg_catalog.pg_read_file(pg_catalog.current_setting('log_directory') || '/' || f.name)), '{}')
  FROM pg_catalog.pg_ls_logdir() f;"""


def retain_logs(databases):
    """Copies each census server's log files into the run's diagnostics, so a judged connection line can be read back."""
    for side, database in databases.items():
        files = json.loads(database.sql("postgres", LOGS))
        if not any(name.endswith(".json") for name in files) or not all(re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9._-]*", name) for name in files):
            raise RuntimeError(side + " census server log files are absent or misnamed")
        secret = database.environment.get("PGPASSWORD")
        if secret and any(secret in text for text in files.values()):
            raise RuntimeError(side + " census server log carries a credential")
        directory = clone.DIAGNOSTIC_ROOT / "census-logs" / side
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        for name, text in files.items():
            (directory / name).write_text(text)


def run(source, target, case, launch, driver):
    try:
        return variants(source, target, case, launch, driver)
    finally:
        failed = sys.exc_info()[0] is not None
        try:
            retain_logs({"source": source, "target": target})
        except Exception:
            if not failed:
                raise


def variants(source, target, case, launch, driver):
    import cutover
    cases = {}
    for variant in VARIANTS:
        srcname, dstname, _, _, state = case("names_" + variant)
        pinned = {"default": None, "environment": "census_env_" + uuid.uuid4().hex[:12],
                  "conninfo": "census_dsn_" + uuid.uuid4().hex[:12]}[variant]
        def dsn(database, name):
            value = f"host={database.host} dbname={name} user=postgres sslmode=disable connect_timeout=3"
            return value + (" application_name=" + pinned if variant == "conninfo" else "")
        src, dst = dsn(source, srcname), dsn(target, dstname)
        previous = driver.environment.pop("PGAPPNAME", None)
        if variant == "environment":
            driver.environment["PGAPPNAME"] = pinned
        try:
            processes = ProcessCensus()
            processes.start()
            censuses = {"source": ServerCensus(source, srcname), "target": ServerCensus(target, dstname)}
            worker = launch(src, dst, state, source_name=srcname, target_name=dstname)
            phases = {worker.pid: "follow"}
            driver.ready(worker)
            source.sql(srcname, '''UPDATE "M2 space".wide SET changed=changed+1 WHERE id=1;''')
            end = source.sql(srcname, "SELECT pg_current_wal_insert_lsn();").decode().strip()
            if engine(driver, state, "endpos", phases, ["stream", "sentinel", "set", "endpos", end, "--target", dst]) != (end + "\n").encode():
                raise RuntimeError("endpos command did not persist the sampled end")
            cutover.finish_completed(driver, worker)
            data = json.loads(engine(driver, state, "compare-data", phases, ["compare", "data", "--source", src, "--target", dst, "--json"]))
            if not data or any(item.get("status") != "match" for item in data):
                raise RuntimeError("followed data does not match after endpos")
            engine(driver, state, "compare-schema", phases, ["compare", "schema", "--source", src, "--target", dst])
            engine(driver, state, "cleanup", phases, ["stream", "cleanup", "--source", src, "--target", dst])
            rows = {side: census.stop() for side, census in censuses.items()}
            ports = processes.stop()
        finally:
            driver.environment.pop("PGAPPNAME", None)
            if previous is not None:
                driver.environment["PGAPPNAME"] = previous
        coverage, counts = set(), {}
        for side, database in (("source", source), ("target", target)):
            local = {port: client for (host, port), client in ports.items() if host == censuses[side].address}
            observed, counts[side] = judge(side, rows[side], local, phases, database.application, pinned)
            coverage |= observed
        required = REQUIRED if pinned is None else REQUIRED_PINNED
        missing = [item for item in required if item not in coverage]
        if missing:
            raise RuntimeError("session census lacks required observations: " + json.dumps(missing))
        cases[variant] = {"pinned": pinned is not None, "counts": counts, "tables": len(data),
                          "observed": sorted(list(item) for item in coverage)}
    return cases


def validate_receipt(report):
    if report.get("sampler_controls") != {side: [*CONTROLS, "connection_log"] for side in ("source", "target")}:
        raise RuntimeError("session census receipt lacks its sampler controls")
    cases = report.get("cases")
    if report.get("mode") != "session_names" or not isinstance(cases, dict) or tuple(sorted(cases)) != tuple(sorted(VARIANTS)):
        raise RuntimeError("session census receipt lacks its variants")
    for variant, value in cases.items():
        required = REQUIRED if variant == "default" else REQUIRED_PINNED
        observed = {tuple(item) for item in value.get("observed", [])}
        if value.get("pinned") is not (variant != "default") or any(item not in observed for item in required):
            raise RuntimeError("session census receipt lacks required observations")
        if any(value.get("counts", {}).get(side, {}).get("sessions", 0) < 1 for side in ("source", "target")):
            raise RuntimeError("session census receipt observed no engine sessions")
