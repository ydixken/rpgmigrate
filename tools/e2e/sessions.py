"""Census of the engine's PostgreSQL session names across clone, follow to endpos, compare and cleanup."""

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
SAMPLER = """CREATE TABLE {stop}();
CREATE PROCEDURE {procedure}(databases text[], INOUT seen jsonb DEFAULT '{{}}') LANGUAGE plpgsql AS $census$
DECLARE deadline timestamptz := clock_timestamp() + interval '900 seconds';
BEGIN
  LOOP
    PERFORM pg_catalog.pg_stat_clear_snapshot();
    seen := seen || coalesce((SELECT jsonb_object_agg(concat_ws('/', a.pid, a.backend_start, a.state, a.application_name),
        jsonb_build_object('pid', a.pid, 'start', a.backend_start::text, 'type', a.backend_type, 'name', a.application_name,
                           'port', a.client_port, 'state', a.state))
      FROM pg_catalog.pg_stat_activity a
      WHERE a.datname = ANY(databases) AND a.backend_type IN ('client backend', 'walsender')), '{{}}');
    EXIT WHEN EXISTS (SELECT FROM {stop}) OR clock_timestamp() > deadline;
    PERFORM pg_catalog.pg_sleep(0.002);
  END LOOP;
END
$census$;"""


class ServerCensus:
    """Samples pg_stat_activity inside the server, so sessions of a few milliseconds are seen."""

    def __init__(self, database, name):
        self.database, self.name = database, name
        token = uuid.uuid4().hex[:12]
        self.stop_table, self.procedure = "census_stop_" + token, "census_" + token
        database.sql("postgres", SAMPLER.format(stop=self.stop_table, procedure=self.procedure))
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


class ProcessCensus(threading.Thread):
    """Maps local TCP ports to the engine and its pg_dump and pg_restore children while they live."""

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
                self.ports[int(fields[1].rsplit(":", 1)[1], 16)] = {"kind": processes[pid][1], "pid": pid,
                                                                    "engine": engine if engine in processes else None}

    def stop(self):
        self.finished.set()
        self.join(timeout=10)
        if self.is_alive() or self.failure is not None:
            raise RuntimeError("process session census failed")
        return self.ports


def judge(side, rows, ports, phases, harness, pinned):
    """Every non-harness session must carry the engine's name; returns observed coverage."""
    sessions = {}
    for row in rows:
        session = sessions.setdefault((row["pid"], row["start"]), {"names": set(), "states": set(), "type": row["type"], "port": row["port"]})
        session["names"].add(row["name"] or "")
        session["states"].add(row["state"] or "")
    coverage, counts = set(), {"sessions": 0, "pre_startup_only": 0, "joined": 0}
    for session in sessions.values():
        if harness in session["names"]:
            continue
        names = session["names"] - {""}
        if not names:
            if session["states"] <= {"starting"}:
                counts["pre_startup_only"] += 1
                continue
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


def run(source, target, case, launch, driver):
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
            observed, counts[side] = judge(side, rows[side], ports, phases, database.application, pinned)
            coverage |= observed
        required = REQUIRED if pinned is None else REQUIRED_PINNED
        missing = [item for item in required if item not in coverage]
        if missing:
            raise RuntimeError("session census lacks required observations: " + json.dumps(missing))
        cases[variant] = {"pinned": pinned is not None, "counts": counts, "tables": len(data),
                          "observed": sorted(list(item) for item in coverage)}
    return cases


def validate_receipt(report):
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
