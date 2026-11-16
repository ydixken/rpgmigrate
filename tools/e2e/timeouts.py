"""Runs clone, follow to endpos, compare and cleanup under small database-level session timeouts."""

import json
import time

import clone
import sessions

# Database-level values a production server may set; each is far below one engine phase.
# The third item is the first server major that has the setting.
SETTINGS = (("statement_timeout", "20ms", 16), ("lock_timeout", "1ms", 16),
            ("idle_in_transaction_session_timeout", "20ms", 16), ("idle_session_timeout", "20ms", 16),
            ("transaction_timeout", "50ms", 17))
# Keeps the follow worker idle well past every idle timeout before the endpos request.
HOLD_SECONDS = 2
# A timeout ends its session with FATAL, which pg_stat_database counts even when a pg_restore worker shrugs it off.
FATAL = "SELECT coalesce(sum(sessions_fatal), -1) FROM pg_catalog.pg_stat_database WHERE datname = '{}';"


def harness_options(major):
    """Startup options that keep the harness's own sessions out of the sessions_fatal count."""
    return " options='" + " ".join(f"-c {name}=0" for name, _, minimum in SETTINGS if major >= minimum) + "'"


def expected_sides(majors, minimum):
    return sorted(side for side, major in majors.items() if major >= minimum)


def hold(worker, seconds, clock=time.monotonic):
    """Keeps the follow worker idle for a measured span; exiting meanwhile fails."""
    start = clock()
    clone.wait_for(lambda: worker.poll() is not None or clock() - start >= seconds, "follow worker idle hold", timeout=seconds + 30)
    if worker.poll() is not None:
        raise RuntimeError("follow worker exited during the idle hold")
    return round(clock() - start, 3)


def fatal_sessions(source, target, srcname, dstname):
    return {side: int(database.sql("postgres", FATAL.format(name)).strip())
            for side, database, name in (("source", source, srcname), ("target", target, dstname))}


def run(source, target, case, launch, driver):
    import cutover
    majors = {side: int(database.sql("postgres", "SHOW server_version_num;").strip()) // 10000
              for side, database in (("source", source), ("target", target))}
    cases = {}
    for name, value, minimum in SETTINGS:
        source.harness_options = target.harness_options = ""
        srcname, dstname, _, _, state = case("timeout_" + name)
        applied = expected_sides(majors, minimum)
        cases[name] = {"value": value, "majors": majors, "applied": applied}
        if not applied:
            continue
        for side, database, database_name in (("source", source, srcname), ("target", target, dstname)):
            if side not in applied:
                continue
            database.sql(database_name, f"ALTER DATABASE {clone.quote(database_name)} SET {name} = '{value}';")
            # A fresh session must see the setting, so the run cannot pass because nothing was set.
            if database.sql(database_name, f"SELECT pg_catalog.current_setting('{name}');").decode().strip() != value:
                raise RuntimeError(f"{side} database setting {name} is not in force")
        # From here the harness's own sessions clear the case setting at startup, so a FATAL is an engine session's.
        source.harness_options, target.harness_options = harness_options(majors["source"]), harness_options(majors["target"])
        before = fatal_sessions(source, target, srcname, dstname)
        if any(count < 0 for count in before.values()):
            raise RuntimeError("session statistics are unavailable")
        src = f"host={source.host} dbname={srcname} user=postgres sslmode=disable connect_timeout=3"
        dst = f"host={target.host} dbname={dstname} user=postgres sslmode=disable connect_timeout=3"
        worker = launch(src, dst, state, source_name=srcname, target_name=dstname)
        phases = {worker.pid: "follow"}
        driver.ready(worker)
        held = hold(worker, HOLD_SECONDS)
        source.sql(srcname, '''UPDATE "M2 space".wide SET changed=changed+1 WHERE id=1;''')
        end = source.sql(srcname, "SELECT pg_current_wal_insert_lsn();").decode().strip()
        if sessions.engine(driver, state, "endpos", phases, ["stream", "sentinel", "set", "endpos", end, "--target", dst]) != (end + "\n").encode():
            raise RuntimeError("endpos command did not persist the sampled end")
        cutover.finish_completed(driver, worker)
        data = json.loads(sessions.engine(driver, state, "compare-data", phases, ["compare", "data", "--source", src, "--target", dst, "--json"]))
        if not data or any(item.get("status") != "match" for item in data):
            raise RuntimeError("followed data does not match after endpos")
        schema = json.loads(sessions.engine(driver, state, "compare-schema", phases, ["compare", "schema", "--source", src, "--target", dst]))
        if schema.get("matched") is not True:
            raise RuntimeError("schema comparison lacks an explicit match")
        sessions.engine(driver, state, "cleanup", phases, ["stream", "cleanup", "--source", src, "--target", dst])
        lost = {side: count - before[side] for side, count in fatal_sessions(source, target, srcname, dstname).items()}
        if any(lost.values()):
            raise RuntimeError(f"{name} ended engine sessions: " + json.dumps(lost, sort_keys=True))
        cases[name].update(held_seconds=held, tables=len(data), completed=True, fatal_sessions=lost)
    source.harness_options = target.harness_options = ""
    return cases


def validate_receipt(report):
    cases = report.get("cases")
    if report.get("mode") != "session_timeouts" or not isinstance(cases, dict) or sorted(cases) != sorted(name for name, _, _ in SETTINGS):
        raise RuntimeError("session timeout receipt lacks its settings")
    for name, value, minimum in SETTINGS:
        item = cases[name]
        majors = item.get("majors")
        if (item.get("value") != value or not isinstance(majors, dict) or sorted(majors) != ["source", "target"]
                or any(type(major) is not int or major not in (16, 18) for major in majors.values())
                or item.get("applied") != expected_sides(majors, minimum)):
            raise RuntimeError(f"session timeout receipt misstates where {name} applied")
        if not item["applied"]:
            continue
        if (item.get("completed") is not True or type(item.get("tables")) is not int or item["tables"] < 1
                or not isinstance(item.get("held_seconds"), (int, float)) or item["held_seconds"] < HOLD_SECONDS
                or item.get("fatal_sessions") != {"source": 0, "target": 0}):
            raise RuntimeError(f"session timeout receipt lacks a completed run under {name}")
