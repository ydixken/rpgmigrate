"""Cutover command checks shared by the existing follow qualification driver."""

import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import time
import zlib

import clone
from follow import lsn
from authority_selfcheck import line_from


CASES = (
    "local_endpos_survives_active_fence_json_refusal",
    "same_endpos_retry_after_completed_observation",
    "endpos_sampled_before_certification_retains_exact_request",
    "async_visible_row_cannot_emit_replication_json",
    "loaded_cutover_durable_source",
    "loaded_cutover_async_source",
    "later_begin_coverage_preserves_pre_later_oracle",
    "crossing_transaction_remains_whole_across_cutover_and_resume",
    "cleanup_retains_foreign_resources_and_comparison_state",
    "completed_resume_requires_checked_authority_close",
)


def load_manifest(path=None):
    from follow import load_manifest as ongoing_manifest
    path = clone.ROOT / "tests/fixtures/follow/cutover-cases.json" if path is None else path
    value = json.loads(path.read_bytes(), object_pairs_hook=clone.unique_object)
    if not isinstance(value, dict) or set(value) != {"version", "cases"} or type(value["version"]) is not int or value["version"] != 1 or tuple(value["cases"]) != CASES:
        raise RuntimeError("cutover case inventory differs from required producer coverage")
    return {**ongoing_manifest(), "cases": list(CASES)}


def validate_receipt(report, manifest, *, require_independent_go=False, require_images=False):
    if require_independent_go:
        raise RuntimeError("M4 closing requires the qualified independent M6 reader and actual positive Go evidence")
    clone.validate_receipt(report, manifest, require_reports=False)
    if (report.get("receipt_scope") != "engine_producer" or report.get("mode") != "cutover"
            or report.get("successful_follow_exit") is not True or report.get("power_loss_tested") is not False
            or report.get("probe_checked") is not False
            or report.get("independent_go_evidence") != "pending_qualified_m6_reader"):
        raise RuntimeError("cutover producer receipt has an invalid evidence scope")
    source = report.get("source_tree", {})
    if (not isinstance(source, dict) or set(source) != {"file_count", "sha256"}
            or type(source["file_count"]) is not int or not 0 < source["file_count"] <= 4096
            or not isinstance(source["sha256"], str) or not re.fullmatch(r"[a-f0-9]{64}", source["sha256"])):
        raise RuntimeError("cutover receipt lacks the actual bounded source-tree identity")
    if require_images:
        identities = [report.get("runner_image_id"), *report.get("server_image_ids", [])]
        if len(identities) != 3 or any(not isinstance(value, str) or not re.fullmatch(r"(?:sha256:)?[a-f0-9]{64}", value) for value in identities):
            raise RuntimeError("cutover receipt lacks actual immutable runner/source/target image identities")
    validate_cases(report["cases"])
    records = report.get("report_validation", {}).get("reports", [])
    if report.get("report_validation", {}).get("mode") != "local_m4_producer" or not records:
        raise RuntimeError("actual cutover startup/completion reports are absent")
    phases = []
    for record in records:
        phase = json.loads(record["raw_json"])["phase"]
        if phase not in ("streaming", "completed") or record.get("probe_checked") is not False:
            raise RuntimeError("cutover producer report claims unsupported Go evidence or lifecycle")
        clone.validate_operator_report(record["raw_json"], record["binding"], "clone", phase == "completed", phases=(phase,))
        phases.append(phase)
    if "streaming" not in phases or "completed" not in phases:
        raise RuntimeError("cutover producer receipt lacks both streaming and completed lifecycle reports")


def completed_output(output, binding, *, already_completed=False):
    lines = output.splitlines(keepends=True)
    if len(lines) != (1 if already_completed else 2) or any(not line.startswith(b"native-report: ") or not line.endswith(b"\n") for line in lines):
        raise RuntimeError("successful follow requires exact startup and completed report frames")
    if not already_completed:
        clone.validate_operator_report(lines[0][15:], binding, "clone", False, phases=("streaming",))
    clone.validate_operator_report(lines[-1][15:], binding, "clone", True)
    return lines[-1]


def bound_report(output, binding, command, payload, capability):
    lines = output.splitlines(keepends=True)
    if len(lines) != 1 or not lines[0].startswith(b"native-report: ") or not lines[0].endswith(b"\n"):
        raise RuntimeError("cutover command requires exactly one complete report frame")
    report = json.loads(lines[0][15:], object_pairs_hook=clone.unique_object)
    if (not isinstance(report, dict)
            or set(report) != {"protocol_version", "engine_version", "command", "capabilities", "work", "phase", payload}
            or type(report["protocol_version"]) is not int or report["protocol_version"] != 1
            or not isinstance(report["engine_version"], str) or not report["engine_version"]
            or binding["command"] != command or report["command"] != command or report["work"] != binding["work"]
            or type(report["work"].get("checkpoint_generation")) is not int
            or report["capabilities"] != [capability] or report["phase"] != "completed"):
        raise RuntimeError("cutover report differs from its trusted caller binding")
    return report


def replication_output(output, binding, end, *, certified_floor=0):
    if binding["command"] not in ("stream sentinel get", "stream sentinel set endpos"):
        raise RuntimeError("cutover acknowledgment has a different caller command")
    report = bound_report(output, binding, binding["command"], "replication", "cutover.v1")
    value = report["replication"]
    if (not isinstance(value, dict)
            or set(value) != {"certified_lsn", "durable_replay_lsn", "end_lsn", "end_persisted", "drained", "checkpoint_id", "drain_proof"}
            or value["end_lsn"] != end or value["end_persisted"] is not True or value["drained"] is not True
            or not isinstance(value["checkpoint_id"], str) or not re.fullmatch(r"[0-9]+:[0-9]+:[0-9]+", value["checkpoint_id"])):
        raise RuntimeError("cutover acknowledgment lacks the exact persisted endpos")
    certified, durable = lsn(value["certified_lsn"]), lsn(value["durable_replay_lsn"])
    if certified < certified_floor or durable < certified:
        raise RuntimeError("cutover acknowledgment regresses independent durable certification")
    proof = value["drain_proof"]
    if (not isinstance(proof, dict)
            or set(proof) != {"work", "checkpoint_id", "committed", "end_lsn", "replay_lsn", "kind"}
            or proof["work"] != binding["work"] or proof["checkpoint_id"] != value["checkpoint_id"]
            or proof["committed"] is not True or proof["end_lsn"] != end
            or proof["kind"] not in ("keepalive", "later_begin") or lsn(proof["replay_lsn"]) < lsn(end)
            or proof["kind"] == "later_begin" and lsn(proof["replay_lsn"]) == lsn(end)
            or proof["kind"] == "keepalive" and lsn(proof["replay_lsn"]) > certified):
        raise RuntimeError("cutover acknowledgment lacks a bound committed coverage proof")
    return report


def require_fence_refusal(result):
    errors = []
    for line in result.stderr.splitlines():
        try:
            value = json.loads(line, object_pairs_hook=clone.unique_object)
        except (ValueError, RuntimeError):
            continue
        if isinstance(value, dict) and value.get("level") == "error":
            errors.append(value)
    if (result.returncode != 3 or result.stdout != b"" or len(errors) != 1
            or errors[0].get("category") != "BadState" or type(errors[0].get("exit_code")) is not int
            or errors[0]["exit_code"] != 3):
        raise RuntimeError("active target fence did not produce an empty-stdout BadState refusal")


def stream(driver, state, path, *arguments, source=None, target=None, result_observer=None):
    if driver.mode != "operator":
        raise RuntimeError("cutover qualification requires the existing operator driver")
    driver.number += 1
    environment, binding = driver.prepare(state, " ".join(path))
    argv = [str(driver.binary), *path, "--dir", str(state)]
    for flag, value in (("--source", source), ("--target", target)):
        if value is not None:
            argv += [flag, value]
    try:
        result = subprocess.run([*argv, *arguments], env=environment, capture_output=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        raise clone.SubprocessFailure(clone.private_failure("engine-cutover-command", None, b"", environment, sys._getframe())) from None
    if result_observer is not None:
        result_observer(result)
    check_diagnostics(result.stdout, result.stderr, environment, result.returncode)
    return result, binding


def check_diagnostics(stdout, stderr, environment, status):
    output = stdout + stderr
    secrets = (*clone.FIXTURE_SECRETS, environment.get("PGPASSWORD"), "fixture-sensitive-row-")
    if len(output) > 1024 * 1024 or any(secret.encode() in output for secret in secrets if secret):
        raise clone.SubprocessFailure(clone.private_failure("engine-cutover-diagnostics", status, json.dumps(command_observation(
            subprocess.CompletedProcess([], status, stdout, stderr))).encode(), environment, sys._getframe()))


def finish_completed(driver, worker, *, already_completed=False):
    try:
        status = worker.wait(timeout=120)
    except subprocess.TimeoutExpired:
        driver.kill(worker)
        raise RuntimeError("cutover worker did not finish within the harness deadline") from None
    if status != 0:
        raise clone.SubprocessFailure(clone.private_failure("engine-cutover-worker", status, worker.error_output.read_bytes(), worker.environment, sys._getframe()))
    stdout, stderr = worker.output.read_bytes(), worker.error_output.read_bytes()
    check_diagnostics(stdout, stderr, worker.environment, status)
    binding = json.loads(worker.binding.read_text())
    final = completed_output(stdout, binding, already_completed=already_completed)
    return driver.report(final, worker.binding, "clone", True)


def persisted_endpos(driver, state, end):
    result, _ = stream(driver, state, ("stream", "sentinel", "get"), "--endpos")
    if result.returncode != 0 or result.stdout != (end + "\n").encode():
        raise RuntimeError("local cutover read did not retain the caller's exact endpos")
    request = json.loads((state / "control/endpos.json").read_bytes(), object_pairs_hook=clone.unique_object)
    if type(request.get("end_lsn")) is not int or request["end_lsn"] != lsn(end):
        raise RuntimeError("local control record changed the independently sampled endpos")
    return request


def command_observation(result):
    categories = {"BadArgs": 1, "BadConfig": 2, "BadState": 3, "Pgsql": 4, "Pgctl": 5,
                  "Source": 6, "Target": 7, "Reload": 9, "Internal": 12, "Fatal": 122}
    known = {"target-follow: writer fence is held by an active owner": "active_owner_fence",
             "state-control: boundary is below admission floor or cleanup is terminal": "admission_floor_or_cleanup",
             "state-control: control lock is busy": "control_lock_busy",
             "stream commands require existing native state": "existing_state_required",
             "--target is required for measured stream state": "target_required"}
    value = {"returncode": result.returncode, "errors": []}
    for name in ("stdout", "stderr"):
        data = getattr(result, name)
        value[name] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    for line in result.stderr[-65536:].splitlines():
        try:
            event = json.loads(line, object_pairs_hook=clone.unique_object)
        except (ValueError, RuntimeError, RecursionError):
            continue
        if (not isinstance(event, dict) or event.get("level") != "error"
                or not isinstance(event.get("category"), str) or event["category"] not in categories
                or type(event.get("exit_code")) is not int or categories[event["category"]] != event["exit_code"]):
            continue
        record = {"category": event["category"], "exit_code": event["exit_code"]}
        origin = event.get("error_origin")
        if (isinstance(origin, dict) and set(origin) == {"file", "line", "column"}
                and isinstance(origin["file"], str) and len(origin["file"]) <= 256
                and re.fullmatch(r"(?:src/(?:[A-Za-z_][A-Za-z0-9_]*/)*)?[A-Za-z_][A-Za-z0-9_]*\.rs", origin["file"])
                and all(type(origin[key]) is int and 0 < origin[key] < 2**32 for key in ("line", "column"))):
            record["error_origin"] = origin
        message = event.get("message")
        if isinstance(message, str):
            state = re.search(r"\(SQLSTATE ([A-Z0-9]{5})\)", message)
            if state:
                record["sqlstate"] = state[1]
            discriminator = known.get(message.removeprefix("native:" + event["category"] + ": "))
            if discriminator:
                record["diagnostic"] = discriminator
        value["errors"] = (value["errors"] + [record])[-16:]
    return value


def diagnostic_authority(driver, target, name, state, deadline):
    from follow import checkpoint_observation
    previous = getattr(target, "_observation_deadline", None)
    target._observation_deadline = min(previous, deadline) if previous is not None else deadline
    try:
        row = target.authority(name, state, driver.work[state])
        evidence = producer_authority(target, name, state, driver.work[state], row)
        catalog = evidence["catalog"]
        return {"scope": "diagnostic", "durability_confirmed": False, "checkpoint": checkpoint_observation(row),
                "catalog": {"sha256": hashlib.sha256(json.dumps(catalog, sort_keys=True).encode()).hexdigest(),
                            **{key: len(catalog[key]) for key in ("objects", "columns", "constraints", "indexes")}},
                "provenance_sha256": hashlib.sha256(json.dumps(evidence["target_provenance"], sort_keys=True).encode()).hexdigest()}
    finally:
        if previous is None:
            del target._observation_deadline
        else:
            target._observation_deadline = previous


def retain_fence_failure(driver, state, worker, gate, facts, observe):
    from follow import bounded_wire_observation
    deadline = time.monotonic() + 4
    previous = getattr(worker, "follow_capture_deadline", None)
    worker.follow_capture_deadline = min(previous, deadline) if previous is not None else deadline
    deadline = worker.follow_capture_deadline
    details = {"scope": "diagnostic", "durability_confirmed": False,
               "capture_stage": "before_gate_and_workdir_teardown", **facts}
    def capture(name, read):
        try:
            details[name] = read() if time.monotonic() < deadline else {"status": "unavailable"}
        except Exception:
            details[name] = {"status": "unavailable"}
    try:
        def native():
            directory = state.lstat()
            path = state / "manifest.json"
            identity = path.lstat()
            if not stat.S_ISDIR(directory.st_mode) or not stat.S_ISREG(identity.st_mode):
                raise RuntimeError("private native identity is unavailable")
            with path.open("rb") as stream:
                data = stream.read(1024 * 1024 + 1)
            if len(data) > 1024 * 1024:
                raise RuntimeError("native diagnostic exceeded its bound")
            value = json.loads(data, object_pairs_hook=clone.unique_object)
            return {"scope": "diagnostic", "reusable_m6_identity": False,
                    "directory": {"device": directory.st_dev, "inode": directory.st_ino, "mode": directory.st_mode & 0o777},
                    "manifest": {"device": identity.st_dev, "inode": identity.st_ino, "bytes": len(data),
                                 "sha256": hashlib.sha256(data).hexdigest()},
                    "recorded_directory_matches": value.get("directory_device") == directory.st_dev and value.get("directory_inode") == directory.st_ino,
                    "recorded_path_matches": value.get("canonical_path") == str(state.resolve(strict=True)),
                    "target_provenance_sha256": hashlib.sha256(json.dumps(value.get("target_provenance"), sort_keys=True).encode()).hexdigest(),
                    "follow_state_sha256": hashlib.sha256(json.dumps(value.get("follow_state"), sort_keys=True).encode()).hexdigest()}
        capture("native_identity", native)
        def held():
            fault = gate.fault_observation()
            tags = fault["command_tags"]
            if (fault.get("status") != "observed" or fault.get("protocol") not in ("simple", "extended")
                    or fault.get("boundary") != "before_commit" or fault.get("scope") != "wire_fault_observation"
                    or any(type(fault.get(key)) is not int or not 0 < fault[key] < 2**32 for key in ("connection", "backend_pid"))
                    or any(type(fault.get(key)) is not bool for key in ("begin_observed", "overflow", "classification_invalid"))
                    or set(tags) != {"insert_0_1", "update_1", "delete_1", "other_dml"}
                    or any(type(count) is not int or not 0 <= count < 2**64 for count in tags.values())):
                raise RuntimeError("held target observation is unavailable")
            return {key: fault[key] for key in ("status", "scope", "boundary", "protocol", "connection", "backend_pid",
                                                "begin_observed", "command_tags", "overflow", "classification_invalid")}
        capture("held_target", held)
        wire = getattr(worker, "follow_wire", None)
        capture("wire", lambda: bounded_wire_observation(wire.evidence(timeout=max(0, deadline - time.monotonic()))))
        def errors():
            with worker.error_output.open("rb") as stream:
                size = stream.seek(0, os.SEEK_END)
                stream.seek(max(0, size - 65536))
                data = stream.read(65536)
            value = command_observation(subprocess.CompletedProcess([], worker.poll(), b"", data))
            value["stderr"].update(bytes=size, captured_bytes=len(data), sha256_scope="captured_tail")
            return value
        capture("worker_output", errors)
        capture("current_checkpoint", lambda: worker.follow_observe())
        if observe is not None:
            capture("authority", lambda: observe(deadline))
        capture("startup", lambda: driver.capture_startup(worker, "before_cleanup", deadline=deadline))
        details["worker_before_teardown"] = worker.poll()
        details["gate_held_before_teardown"] = gate.held.is_set()
        pid = getattr(worker, "pid", None)
        details["worker_pid"] = pid if type(pid) is int and 0 < pid < 2**31 else None
        return clone.private_failure("engine-cutover-fence-observation", facts["sentinel"]["returncode"],
                                     json.dumps(details).encode(), getattr(worker, "environment", {}), sys._getframe())
    finally:
        if previous is None:
            del worker.follow_capture_deadline
        else:
            worker.follow_capture_deadline = previous


def active_fence_request(driver, state, target, worker, gate, end, *, observe=None):
    if worker.poll() is not None or not gate.held.is_set() or gate.evidence()["hits"] != 1:
        raise RuntimeError("active-fence case did not hold the actual worker at its target boundary")
    facts = {"worker_before_command": worker.poll(), "gate_hits_before_command": 1,
             "expected_refusal": {"category": "BadState", "returncode": 3, "stdout_bytes": 0},
             "sentinel": {"status": "unavailable", "returncode": None}}
    def returned(result):
        if facts["sentinel"].get("status") != "unavailable":
            return
        facts.update(sentinel=command_observation(result), worker_after_child_return=worker.poll(),
                     gate_held_after_child_return=gate.held.is_set())
        try:
            facts["child_result_record"] = clone.private_failure("engine-cutover-command-result", result.returncode,
                json.dumps({"scope": "diagnostic", "durability_confirmed": False,
                            "capture_stage": "child_return_before_assertion", **facts}).encode(),
                getattr(worker, "environment", {}), sys._getframe())
        except Exception:
            facts["child_result_record"] = {"retained": False}
    stage = "stream_command"
    try:
        result, _ = stream(driver, state, ("stream", "sentinel", "set", "endpos"), end, "--json", target=target,
                           result_observer=returned)
        returned(result)
        stage = "refusal_assertion"
        require_fence_refusal(result)
        stage = "worker_still_held"
        if worker.poll() is not None or not gate.held.is_set():
            raise RuntimeError("worker exited before the active-fence command refusal was observed")
        stage = "local_endpos_read"
        persisted_endpos(driver, state, end)
    except Exception:
        facts["failed_check"] = stage
        metadata = None
        try:
            metadata = retain_fence_failure(driver, state, worker, gate, facts, observe)
        except Exception:
            pass
        if metadata is None:
            raise
        raise clone.SubprocessFailure(metadata) from None
    return {"observed": True, "end_lsn": end, "worker_active_during_refusal": True,
            "error_category": "BadState", "exit_code": result.returncode, "success_stdout_empty": True,
            "local_request_retained": True, "control_lock_released": True, "fault_hits": 1}


def completed_retry(driver, state, target, end, *, certified_floor=0):
    if driver.report_probe is not None:
        raise RuntimeError("positive cutover Go validation requires a qualified independent M6 reader binding")
    reports = []
    first = None
    for _ in range(2):
        result, binding_path = stream(driver, state, ("stream", "sentinel", "set", "endpos"), end, "--json", target=target)
        if result.returncode != 0:
            raise RuntimeError("identical endpos retry failed after worker completion")
        binding = json.loads(binding_path.read_text())
        value = replication_output(result.stdout, binding, end, certified_floor=certified_floor)
        if first is not None and value["replication"] != first:
            raise RuntimeError("completed retry changed immutable drain or checkpoint authority")
        first = value["replication"]
        reports.append({"binding": binding, "raw_json": result.stdout[15:].decode(), "probe_checked": False,
                        "independent_go_evidence": "pending_qualified_m6_reader"})
        persisted_endpos(driver, state, end)
    return {"observed": True, "end_lsn": end, "identical_retries": 2,
            "checkpoint_id": first["checkpoint_id"], "certified_lsn": first["certified_lsn"],
            "durable_replay_lsn": first["durable_replay_lsn"], "reports": reports}


def require_wire_witness(evidence, report):
    value = report["replication"]
    end, witness, durable = lsn(value["end_lsn"]), lsn(value["drain_proof"]["replay_lsn"]), lsn(value["durable_replay_lsn"])
    kind = value["drain_proof"]["kind"]
    try:
        logical = evidence["logical"]
        if logical["overflow"] or not logical["streams"]:
            raise ValueError
        matched, status_count, previous = False, 0, 0
        for stream in logical["streams"]:
            if not stream["confirmed"] or stream["invalid"] or stream["overflow"]:
                raise ValueError
            if kind == "keepalive":
                matched |= witness >= end and any(item["forwarded"] and item["idle"] and item["lsn"] == witness for item in stream["keepalives"])
            elif kind == "later_begin":
                matched |= witness > end and any(item["begin_forwarded"] and item["final_lsn"] == witness for item in stream["transactions"])
            else:
                raise ValueError
            for positions in stream["statuses"]:
                if (len(positions) != 3 or any(type(point) is not int for point in positions)
                        or not previous <= positions[0] <= durable or positions[0] != positions[1] or positions[1] != positions[2]):
                    raise ValueError
                status_count += 1
                previous = positions[0]
        if not matched or not status_count:
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("drain lacks independently forwarded coverage or promoted its witness into feedback") from None


def certification_race(driver, source, target, srcname, dstname, state, worker, *, end):
    if not lsn(end):
        raise RuntimeError("certification race requires the caller's saved endpos")
    marker = source.sql("postgres", "SELECT pg_logical_emit_message(false,'rpgmigrate_m4','certification-race');").decode().strip()
    if lsn(marker) <= lsn(end):
        raise RuntimeError("certification race did not generate later unrelated WAL")

    def advanced():
        if worker.poll() is not None:
            raise RuntimeError("worker exited before certification advanced beyond sampled endpos")
        row = target.authority(dstname, state, driver.work[state])
        return row if row and row["certified_lsn"] is not None and lsn(row["certified_lsn"]) > lsn(end) else None

    row = clone.wait_for(advanced, "actual certification above the unchanged sampled endpos", timeout=90)
    floor = max(lsn(row["source_end_lsn"]) if row["source_end_lsn"] is not None else 0,
                lsn(row["copy_consistent_point"]))
    if floor > lsn(end):
        raise RuntimeError("sampled endpos is below actual applied/anchor admission floor")
    result, _ = stream(driver, state, ("stream", "sentinel", "set", "endpos"), end, target=target.connection(dstname))
    if result.returncode != 0 or result.stdout != (end + "\n").encode():
        raise RuntimeError("certification above endpos prevented the unchanged explicit request")
    persisted_endpos(driver, state, end)
    finish_completed(driver, worker)
    retry = completed_retry(driver, state, target.connection(dstname), end, certified_floor=lsn(row["certified_lsn"]))
    return {**retry, "scope": "actual_sampled_lsn_certification_race", "sampled_end_lsn": end,
            "certified_before_request": row["certified_lsn"], "unrelated_wal_lsn": marker,
            "applied_anchor_floor": floor, "worker_completed": True}


def async_visible_refusal(driver, target, dstname, state):
    before = target.authority(dstname, state, driver.work[state])
    if not before:
        raise RuntimeError("async visibility fixture lacks an established exact owned authority row")
    native = json.loads((state / "manifest.json").read_bytes(), object_pairs_hook=clone.unique_object)
    record = native["target_provenance"][0]["record"]
    binding = record["intent"]["binding"]
    table = clone.quote(binding["schema_name"]) + "." + clone.quote(binding["table_name"])
    old_xmin = target.sql(dstname, f"SELECT xmin::text FROM ONLY {table};").decode().strip()
    if not old_xmin.isdecimal():
        raise RuntimeError("async visibility fixture lacks one physical row version")
    unsigned = zlib.crc32(bytes(binding["stream"]["run"]))
    slot = unsigned if unsigned < 2**31 else unsigned - 2**32
    lock = f"pid=pg_backend_pid() AND locktype='advisory' AND granted AND classid=1380992845 AND objid={unsigned} AND objsubid=2"
    sql = f"""DO $$ BEGIN
IF NOT pg_try_advisory_lock(1380992845,{slot}) THEN RAISE EXCEPTION 'fixture fence unavailable'; END IF;
END $$;
SET synchronous_commit=off;
BEGIN;
DO $$ DECLARE affected integer; BEGIN
UPDATE ONLY {table} SET barrier_counter=barrier_counter WHERE tableoid={record['table_oid']};
GET DIAGNOSTICS affected=ROW_COUNT;
IF affected<>1 THEN RAISE EXCEPTION 'fixture row cardinality differs'; END IF;
END $$;
COMMIT;
SELECT json_build_array(pg_backend_pid(),current_setting('synchronous_commit'),xmin::text,barrier_epoch,barrier_counter,
EXISTS(SELECT FROM pg_locks WHERE {lock})) FROM ONLY {table};
"""
    holder = subprocess.Popen([str(target.bindir / "psql"), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1",
                               "--dbname", target.connection(dstname)], env=target.environment,
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        holder.stdin.write(sql.encode())
        holder.stdin.flush()
        marker = json.loads(line_from(holder, timeout=10))
        if (not isinstance(marker, list) or len(marker) != 6 or type(marker[0]) is not int or marker[0] <= 0
                or marker[1] != "off" or not isinstance(marker[2], str) or not marker[2].isdecimal()
                or marker[2] == old_xmin or marker[3:5] != [before["barrier_epoch"], before["barrier_counter"]]
                or marker[5] is not True or holder.poll() is not None):
            raise RuntimeError("async writer did not retain the actual fence, old token, off setting and new row version")
        visible = json.loads(target.sql(dstname, f"SELECT json_build_array(xmin::text,barrier_epoch,barrier_counter) FROM ONLY {table};"))
        if visible != marker[2:5] or target.authority(dstname, state, driver.work[state]) != before:
            raise RuntimeError("independent reader did not observe the exact same-value asynchronous row")
        result, _ = stream(driver, state, ("stream", "sentinel", "get"), "--json", target=target.connection(dstname))
        require_fence_refusal(result)
        held = target.sql(dstname, f"SELECT EXISTS(SELECT FROM pg_locks WHERE {lock.replace('pg_backend_pid()', str(marker[0]))});").strip()
        if holder.poll() is not None or held != b"t":
            raise RuntimeError("async writer lost its fence before the command refusal completed")
        holder.communicate(input=f"SELECT pg_advisory_unlock(1380992845,{slot});\n\\q\n".encode(), timeout=5)
        if holder.returncode != 0:
            raise RuntimeError("async fixture failed to release its own fence cleanly")
    finally:
        if holder.poll() is None:
            holder.terminate()
            try:
                holder.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                holder.kill()
                holder.communicate(timeout=5)
    replay, _ = stream(driver, state, ("stream", "sentinel", "get"), "--replay-lsn", target=target.connection(dstname))
    floor = max(lsn(before["source_end_lsn"]) if before["source_end_lsn"] is not None else 0,
                lsn(before["certified_lsn"]), lsn(before["copy_consistent_point"]))
    if replay.returncode != 0 or not replay.stdout.endswith(b"\n") or lsn(replay.stdout.decode().strip()) < floor:
        raise RuntimeError("post-release command did not prove that the same fixture supports positive fenced observation")
    return {"observed": True, "scope": "active_fence_refusal_around_visible_async_same_value_write",
            "synchronous_commit": "off", "new_xmin_observed": True, "token_unchanged": True,
            "fence_held_through_command": True, "error_category": "BadState", "exit_code": 3,
            "success_stdout_empty": True, "positive_replay_after_release": replay.stdout.decode().strip(),
            "power_loss_tested": False, "unflushed_wal_proven": False}


def require_runtime_frontiers(row, slot, report):
    value = report["replication"]
    proof = value["drain_proof"]
    applied = lsn(row["source_end_lsn"]) if row["source_end_lsn"] is not None else 0
    certified = lsn(row["certified_lsn"]) if row["certified_lsn"] is not None else 0
    durable = max(applied, certified, lsn(row["copy_consistent_point"]))
    token = f"{row['generation']}:{row['barrier_epoch']}:{row['barrier_counter']}"
    if (row["follow_completed"] is not True or row["drain_end_lsn"] != value["end_lsn"]
            or row["drain_witness_lsn"] != proof["replay_lsn"] or row["drain_kind"] != proof["kind"]
            or token != value["checkpoint_id"] or certified != lsn(value["certified_lsn"])
            or durable != lsn(value["durable_replay_lsn"])
            or lsn(slot["confirmed_flush_lsn"]) > durable or lsn(slot["restart_lsn"]) > durable):
        raise RuntimeError("independent final row or source reconnect floor differs from proved authority")


def source_batch(source, name, setting):
    from follow import validate_crash_sessions
    if setting not in ("on", "off"):
        raise RuntimeError("source writer fixture requires one declared commit setting")
    before = int(source.sql(name, 'SELECT count(*) FROM ONLY "M2 space".rows;').strip())
    writer = subprocess.Popen([str(source.bindir / "psql"), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname", source.connection(name)],
                              env=source.environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        sql = "SET synchronous_commit=" + setting + ";\n"
        sql += 'BEGIN; INSERT INTO "M2 space".rows(value,feeling,positive) VALUES(\'cutover-load\',\'busy\',1); COMMIT;\n' * 32
        sql += "SELECT json_build_array(pg_backend_pid(),(SELECT backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()),current_setting('synchronous_commit'),"
        sql += '(SELECT count(*) FROM ONLY "M2 space".rows),(SELECT oid::bigint FROM pg_database WHERE datname=current_database()));\n'
        writer.stdin.write(sql.encode())
        writer.stdin.flush()
        marker = json.loads(line_from(writer, timeout=20))
        if not isinstance(marker, list) or len(marker) != 5:
            raise RuntimeError("source writer did not emit its actual bounded commit receipt")
        session = {"pid": marker[0], "backend_start": marker[1], "database_oid": marker[4]}
        validate_crash_sessions([session])
        if marker[2] != setting or type(marker[3]) is not int or marker[3] - before != 32:
            raise RuntimeError("source writer fixture did not commit its exact nonempty batch")
        _output, errors = writer.communicate(input=b"\\q\n", timeout=10)
        if writer.returncode != 0:
            raise clone.SubprocessFailure(clone.private_failure("cutover-source-writer", writer.returncode, errors, source.environment, sys._getframe()))
        identity = json.dumps([session]).replace("'", "''")
        query = "SELECT NOT EXISTS(SELECT FROM pg_stat_activity a JOIN jsonb_to_recordset('" + identity + "'::jsonb) AS owned(pid integer,backend_start timestamptz,database_oid bigint) ON a.pid=owned.pid AND a.backend_start=owned.backend_start AND a.datid::bigint=owned.database_oid);"
        clone.wait_for(lambda: source.sql(name, query).strip() == b't', "actual source writer session closed", timeout=10)
        return {"sessions": [session], "synchronous_commit": setting, "transactions": 32, "rows_inserted": 32,
                "rows_before": before, "rows_after": marker[3], "writer_reaped": True, "writer_sessions_closed": True}
    finally:
        if writer.poll() is None:
            writer.terminate()
            try:
                writer.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                writer.kill()
                writer.communicate(timeout=5)



def cleanup_output(output, binding, obligations):
    report = bound_report(output, binding, "stream cleanup", "cleanup", "cleanup.v1")
    value = report["cleanup"]
    if not obligations or not isinstance(value, dict) or set(value) != {"complete", "resources"} or value["complete"] is not True or not isinstance(value["resources"], list):
        raise RuntimeError("cleanup lacks its nonempty exact caller-owned obligations")
    def inventory(resources):
        result = {}
        for item in resources:
            if (not isinstance(item, dict) or set(item) != {"kind", "name", "owned", "removed"}
                    or not isinstance(item["kind"], str) or not isinstance(item["name"], str) or not item["name"]
                    or type(item["owned"]) is not bool or type(item["removed"]) is not bool):
                raise RuntimeError("cleanup resource record is invalid")
            key = item["kind"], item["name"]
            if key in result:
                raise RuntimeError("cleanup duplicated an inventory obligation")
            result[key] = item
        return result
    expected = inventory(obligations)
    if not {"replication_slot", "publication", "workdir"}.issubset({kind for kind, _name in expected}) or inventory(value["resources"]) != expected:
        raise RuntimeError("cleanup changed, omitted or fabricated an exact inventory obligation")
    return report


def frozen_end(source, name):
    query = "SELECT count(*) FROM pg_stat_activity WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND pid<>pg_backend_pid() AND backend_type='client backend' AND xact_start IS NOT NULL;"
    clone.wait_for(lambda: source.sql(name, query).strip() == b'0', "source writers and outstanding client transactions stopped", timeout=10)
    return source.sql(name, "SELECT pg_current_wal_insert_lsn();").decode().strip()


def require_completed_close_fault(value, *, delivered):
    fields = {"status", "scope", "connection", "backend_pid", "commit_ordinal", "protocol", "begin_observed",
              "terminal_idle", "upstream_disconnected", "terminal_delivered", "next_frontend_kind",
              "old_drop_terminate", "overflow", "classification_invalid"}
    if (not isinstance(value, dict) or set(value) != fields or value["status"] != "observed"
            or value["scope"] != "completed_resume_close_fault" or value["protocol"] not in ("simple", "extended")
            or any(type(value[key]) is not int or not 0 < value[key] < 2**31 for key in ("connection", "backend_pid"))
            or type(value["commit_ordinal"]) is not int or value["commit_ordinal"] != 2
            or any(value[key] is not True for key in ("begin_observed", "terminal_idle", "upstream_disconnected"))
            or any(value[key] is not False for key in ("overflow", "classification_invalid"))
            or value["terminal_delivered"] is not delivered
            or delivered and (value["next_frontend_kind"] not in ("Q", "P", "B", "E", "D", "C", "S", "H")
                              or value["old_drop_terminate"] is not False)):
        raise RuntimeError("completed resume lacks the exact delivered second COMMIT/Idle close fault")
    if not delivered and (value["next_frontend_kind"] is not None or value["old_drop_terminate"] is not None):
        raise RuntimeError("completed resume close fault recorded a request before terminal delivery")


def require_checked_close_refusal(value):
    if not isinstance(value, dict):
        raise RuntimeError("completed resume close lacks its actual command observation")
    empty = {"bytes": 0, "sha256": hashlib.sha256(b"").hexdigest()}
    errors = value.get("errors", [])
    stderr = value.get("stderr", {})
    if (type(value.get("returncode")) is not int or value["returncode"] != 4 or value.get("stdout") != empty
            or type(value["stdout"]["bytes"]) is not int
            or not isinstance(errors, list) or len(errors) != 1 or not isinstance(errors[0], dict)
            or errors[0].get("category") != "Pgsql"
            or type(errors[0].get("exit_code")) is not int or errors[0]["exit_code"] != 4
            or not isinstance(stderr, dict) or type(stderr.get("bytes")) is not int or not 0 < stderr["bytes"] <= 1024 * 1024
            or not isinstance(stderr.get("sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", stderr["sha256"])):
        raise RuntimeError("completed resume close must refuse with Pgsql exit4 and empty stdout")


def validate_completed_close(value, clean):
    try:
        fields = {"observed", "scope", "work", "caller_goal", "before", "after", "recovered", "fault_held", "fault",
                  "fault_hits", "target_wire", "backend_absence", "worker_result", "worker_reaped", "relay_workers_reaped",
                  "fault_free_resume"}
        if (set(value) != fields or value["observed"] is not True or value["scope"] != "completed_resume_close_fault"
                or value["work"] != clean["caller_goal"]["work"] or value["caller_goal"] != clean["caller_goal"]
                or type(value["fault_hits"]) is not int or value["fault_hits"] != 1
                or any(value[key] is not True for key in ("worker_reaped", "relay_workers_reaped"))):
            raise RuntimeError("completed resume close receipt lacks owned worker/fault bindings")
        before = value["before"]
        if (set(before) != {"authority", "native_manifest_sha256", "local_endpos_sha256", "source_resources", "foreign_inventory",
                           "source_fingerprints", "target_fingerprints"}
                or before["authority"] != clean["producer_authority"]["checkpoint_row"]
                or before["authority"].get("follow_completed") is not True or len(before["authority"]) != 29
                or before["authority"]["drain_end_lsn"] != value["caller_goal"]["end_lsn"]
                or before["authority"]["run_id"] != "\\x" + value["work"]["run_id"]
                or any(type(before["authority"].get(key)) is not int or not 0 < before["authority"][key] < limit
                       for key, limit in (("target_database", 2**32), ("target_system", 2**64)))
                or any(not isinstance(before[key], str) or not re.fullmatch(r"[a-f0-9]{64}", before[key])
                       for key in ("native_manifest_sha256", "local_endpos_sha256"))
                or before["source_resources"] != [None, None]
                or before["foreign_inventory"] != clean["foreign_inventory_after"]
                or set(before["foreign_inventory"]) != {"slot", "publication", "origin"}
                or any(not isinstance(item, dict) or not item for item in before["foreign_inventory"].values())
                or before["source_fingerprints"] != clean["source_fingerprints"]
                or before["target_fingerprints"] != clean["target_fingerprints"]
                or value["after"] != before or value["recovered"] != before):
            raise RuntimeError("completed resume close changed authority, native identity, source resources or fingerprints")
        held, final = value["fault_held"], value["fault"]
        require_completed_close_fault(held, delivered=False)
        require_completed_close_fault(final, delivered=True)
        pending = {"terminal_delivered", "next_frontend_kind", "old_drop_terminate"}
        if {key: item for key, item in held.items() if key not in pending} != {key: item for key, item in final.items() if key not in pending}:
            raise RuntimeError("completed resume close fault changed its selected transaction/backend")
        wire = value["target_wire"]
        connections = wire["connections"]
        if not isinstance(connections, list) or not all(isinstance(item, dict) for item in connections):
            raise RuntimeError("completed resume close has malformed target connection witnesses")
        matches = [item for item in connections if item.get("connection") == final["connection"]]
        absence = value["backend_absence"]
        if (wire.get("status") != "observed" or wire.get("overflow") is not False
                or not 1 <= len(connections) <= 64 or len(matches) != 1
                or matches[0].get("backend_pid") != final["backend_pid"]
                or matches[0].get("unknown") is not False or matches[0].get("overflow") is not False
                or absence != {"connection": final["connection"], "backend_pid": final["backend_pid"],
                               "database_oid": before["authority"]["target_database"],
                               "system_identifier": before["authority"]["target_system"], "absent": True}):
            raise RuntimeError("completed resume close lacks wire-observed backend and independent SQL absence")
        require_checked_close_refusal(value["worker_result"])
        record = value["fault_free_resume"]
        if (not isinstance(record, dict) or record.get("probe_checked") is not False or record.get("independent_go_evidence") != "pending_qualified_m6_reader"
                or record["binding"] != {"command": "clone", "work": value["work"]}):
            raise RuntimeError("completed resume close lacks bound fault-free producer completion")
        completed_output(b"native-report: " + record["raw_json"].encode().rstrip(b"\n") + b"\n", record["binding"], already_completed=True)
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("completed resume close receipt is missing or malformed") from None


def validate_cases(cases):
    from follow import authority_frontier, validate_crash_sessions, validate_drain_row, validate_data_fault
    manifest = load_manifest()
    if set(cases) != set(CASES):
        raise RuntimeError("cutover producer case inventory is missing or unexpected")

    def oracles(value):
        for side in ("source_fingerprints", "target_fingerprints"):
            result = value[side]
            if not isinstance(result, dict) or set(result) != {*manifest["tables"], "schema", "sequences"}:
                raise RuntimeError("cutover oracle omitted schema, data or sequence inventory")
            total = 0
            for name in manifest["tables"]:
                item = result[name]
                if (not isinstance(item, dict) or set(item) != {"rows", "sha256"} or type(item["rows"]) is not int
                        or not 0 <= item["rows"] <= manifest["max_rows"] or not isinstance(item["sha256"], str)
                        or not re.fullmatch(r"[a-f0-9]{64}", item["sha256"])):
                    raise RuntimeError("cutover oracle contains an invalid table fingerprint")
                total += item["rows"]
            if not total or not re.fullmatch(r"[a-f0-9]{64}", result["schema"]) or not re.fullmatch(r"-?[0-9]+\|[tf]", result["sequences"]):
                raise RuntimeError("cutover oracle is empty or omitted final sequence state")
        if value["source_fingerprints"] != value["target_fingerprints"]:
            raise RuntimeError("cutover complete schema/data/sequence oracles differ")

    def completed(value):
        if value.get("worker_completed") is not True or type(value.get("identical_retries")) is not int or value["identical_retries"] != 2:
            raise RuntimeError("cutover lacks a joined successful worker and identical endpos retries")
        records = value.get("reports", [])
        if not isinstance(records, list) or len(records) != 2:
            raise RuntimeError("cutover positive acknowledgment records are absent")
        previous = None
        for record in records:
            if record.get("probe_checked") is not False or record.get("independent_go_evidence") != "pending_qualified_m6_reader":
                raise RuntimeError("local producer evidence fabricated positive independent Go validation")
            raw = record["raw_json"].encode()
            report = replication_output(b"native-report: " + raw + (b"" if raw.endswith(b"\n") else b"\n"), record["binding"], value["end_lsn"])
            if previous is not None and previous != report["replication"]:
                raise RuntimeError("identical completed acknowledgment changed its immutable row/token")
            previous = report["replication"]
            if value.get("caller_goal") != {"end_lsn": value["end_lsn"], "work": record["binding"]["work"]}:
                raise RuntimeError("cutover report differs from the independently saved caller goal")
            validate_drain_row(value["target_row"])
            authority_frontier(value["target_row"])
            require_runtime_frontiers(value["target_row"], value["source_slot"], report)
            require_wire_witness(value["wire"], report)
        handoff = value["producer_authority"]
        catalog = handoff["catalog"]
        full_row = handoff["checkpoint_row"]
        provenance = handoff["target_provenance"]
        if (handoff.get("receipt_scope") != "engine_producer" or handoff.get("independent_go_evidence") != "pending_qualified_m6_reader"
                or type(handoff.get("native_format")) is not int or handoff["native_format"] != 8
                or type(handoff.get("metadata_version")) is not int or handoff["metadata_version"] != 6
                or handoff.get("work") != value["caller_goal"]["work"] or type(full_row.get("version")) is not int
                or full_row["version"] != 6 or len(full_row) != 29
                or any(full_row.get(key) != item for key, item in value["target_row"].items())
                or {item["attname"] for item in catalog["columns"]} != set(full_row) or len(catalog["columns"]) != 29
                or {item["oid"] for item in catalog["objects"]} != {provenance[key] for key in ("table_oid", "marker_table_oid", "marker_index_oid")}
                or not catalog["constraints"] or not catalog["indexes"] or not isinstance(handoff["follow_state"]["binding"], dict)):
            raise RuntimeError("cutover producer omitted its actual native8 catalog/provenance handoff")
        oracles(value)
        return report

    try:
        if any(value.get("observed") is not True for value in cases.values()):
            raise RuntimeError("cutover case did not observe its required boundary")
        local = cases[CASES[0]]
        if (not lsn(local["end_lsn"]) or local.get("worker_active_during_refusal") is not True
                or local.get("fault_hits") != 1 or local.get("error_category") != "BadState" or local.get("exit_code") != 3
                or any(local.get(key) is not True for key in ("success_stdout_empty", "local_request_retained", "control_lock_released"))):
            raise RuntimeError("active-fence refusal lost the local goal or its real worker/gate boundary")
        validate_data_fault(local["data_fault"], "before_commit", {local["caller_goal"]["work"]["run_id"]})
        if local.get("caller_goal") != cases[CASES[1]].get("caller_goal"):
            raise RuntimeError("active-fence refusal lost the independently saved caller goal")
        if cases[CASES[1]]["end_lsn"] != local["end_lsn"]:
            raise RuntimeError("completed retry changed the endpos retained after active-fence refusal")
        for name in (CASES[1], CASES[2], CASES[4], CASES[5], CASES[6], CASES[7], CASES[8]):
            completed(cases[name])
        cert = cases[CASES[2]]
        if (cert.get("scope") != "actual_sampled_lsn_certification_race" or cert["sampled_end_lsn"] != cert["end_lsn"]
                or not cert["applied_anchor_floor"] <= lsn(cert["end_lsn"]) < lsn(cert["certified_before_request"])
                or lsn(cert["unrelated_wal_lsn"]) <= lsn(cert["end_lsn"])
                or lsn(cert["target_row"]["certified_lsn"]) < lsn(cert["certified_before_request"])):
            raise RuntimeError("actual sampled-endpos certification race was not retained through completion")
        asynchronous = cases[CASES[3]]
        if (asynchronous.get("scope") != "active_fence_refusal_around_visible_async_same_value_write"
                or asynchronous.get("synchronous_commit") != "off" or asynchronous.get("error_category") != "BadState"
                or asynchronous.get("exit_code") != 3 or asynchronous.get("power_loss_tested") is not False
                or asynchronous.get("unflushed_wal_proven") is not False
                or any(asynchronous.get(key) is not True for key in ("new_xmin_observed", "token_unchanged", "fence_held_through_command", "success_stdout_empty"))
                or not lsn(asynchronous["positive_replay_after_release"])):
            raise RuntimeError("visible asynchronous row did not retain its scoped active-fence refusal proof")
        for name, setting in ((CASES[4], "on"), (CASES[5], "off")):
            value = cases[name]
            writer = value["writer"]
            validate_crash_sessions(writer["sessions"])
            if (value.get("source_write_freeze") is not True or type(value.get("outstanding_transactions")) is not int or value["outstanding_transactions"] != 0
                    or value.get("oracle_scope") != "post_freeze_complete_source" or writer["synchronous_commit"] != setting
                    or type(writer["transactions"]) is not int or writer["transactions"] != 32
                    or type(writer["rows_inserted"]) is not int or writer["rows_inserted"] != 32
                    or writer["rows_after"] - writer["rows_before"] != 32
                    or writer.get("writer_reaped") is not True or writer.get("writer_sessions_closed") is not True):
                raise RuntimeError("loaded cutover omitted real writes, a checked writer stop or pre-sampling source freeze")
        later = cases[CASES[6]]
        wire = later["wire"]
        streams = wire["logical"]["streams"]
        received = sum(len(stream["keepalives"]) for stream in streams)
        forwarded = sum(item["forwarded"] for stream in streams for item in stream["keepalives"])
        begins = [item for stream in streams for item in stream["transactions"]]
        if (later.get("scope") != "algorithm_fault_scope_later_begin" or later.get("oracle_scope") != "saved_pre_later_source"
                or wire["hits"] != 1 or type(wire["dropped_keepalives"]) is not int or wire["dropped_keepalives"] != received
                or later["dropped_keepalives"] != received or later["received_keepalives"] != received
                or forwarded != 0 or later["forwarded_keepalives"] != 0 or len(begins) != 1 or not begins[0]["begin_forwarded"]
                or later["begin_received"] != 1 or later["begin_forwarded"] != 1
                or later["target_row"]["drain_kind"] != "later_begin" or later["later_source_fingerprints"] == later["source_fingerprints"]
                or any(later["target_row"][key] != later["baseline_target_row"][key] for key in ("source_end_lsn", "certified_lsn", "copy_consistent_point"))):
            raise RuntimeError("controlled later-BEGIN case lost its real frame/loss/pre-later oracle separation")
        cross = cases[CASES[7]]
        transaction = cross["transaction"]
        if (cross.get("scope") != "algorithm_fault_scope_crossing_commit_record"
                or not transaction["commit_lsn"] <= lsn(cross["end_lsn"]) < transaction["end_lsn"]
                or lsn(cross["target_row"]["source_end_lsn"]) != transaction["end_lsn"]
                or lsn(cross["target_row"]["source_commit_lsn"]) != transaction["commit_lsn"]
                or cross["target_row"]["last_xid"] != transaction["xid"] or transaction["dml_count"] < 2
                or any(transaction.get(key) is not True for key in ("complete", "begin_forwarded", "commit_forwarded"))
                or not any(item == transaction for stream in cross["wire"]["logical"]["streams"] for item in stream["transactions"])
                or cross.get("completed_resumes") != 2 or cross.get("resume_row_unchanged") is not True):
            raise RuntimeError("crossing transaction was partial, missed its target group or changed across completed same-E resume")
        validate_data_fault(cross["data_fault"], "before_commit", {cross["caller_goal"]["work"]["run_id"]})
        if {key: value for key, value in cross["data_fault"]["source"].items() if key != "baseline_end"} != transaction:
            raise RuntimeError("crossing transaction differs from the actual held DATA proof")
        clean = cases[CASES[8]]
        records = clean["cleanup_reports"]
        if (not isinstance(records, list) or len(records) != 2 or clean.get("foreign_resource_count") != 3
                or not re.fullmatch(r"[a-f0-9]{64}", clean["foreign_inventory_before_sha256"])
                or clean["foreign_inventory_before_sha256"] != clean["foreign_inventory_after_sha256"]
                or clean.get("completed_resumes") != 2 or clean.get("resume_row_unchanged") is not True
                or any(clean.get(key) is not True for key in ("retained_workdir", "metadata_retained", "comparisons_before_and_after", "owned_resources_absent"))):
            raise RuntimeError("cleanup omitted exact foreign-resource preservation, retained comparison state or completed resume")
        for record in records:
            raw = record["raw_json"].encode()
            cleanup_output(b"native-report: " + raw + (b"" if raw.endswith(b"\n") else b"\n"), record["binding"], clean["obligations"])
        validate_completed_close(cases[CASES[9]], clean)
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("cutover producer evidence is missing or malformed") from None


def saved_goal(driver, state, end):
    if not lsn(end):
        raise RuntimeError("caller cutover goal must be a nonzero sampled endpos")
    expected = {"end_lsn": end, "work": driver.work[state]}
    path = driver.root / (state.name + ".caller-endpos.json")
    if path.is_symlink():
        raise RuntimeError("caller cutover goal was replaced")
    if not path.exists():
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(json.dumps(expected, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        directory = os.open(driver.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    value = json.loads(path.read_bytes(), object_pairs_hook=clone.unique_object)
    if value != expected or path.stat().st_mode & 0o777 != 0o600:
        raise RuntimeError("caller cutover goal changed after its independent measurement")
    return value


def source_inventory(root):
    entries, total, directories = [], 0, 0
    excluded = {".git", "target", "__pycache__"}
    for current, children, files in os.walk(root, followlinks=False):
        directories += 1
        children[:] = sorted(name for name in children if name not in excluded)
        if directories > 4096 or any(os.path.islink(os.path.join(current, name)) for name in children):
            raise RuntimeError("cutover source directory inventory is replaced or exceeded its bound")
        for name in sorted(files):
            if name in excluded:
                continue
            path = type(root)(current) / name
            if path.is_symlink() or not path.is_file():
                raise RuntimeError("cutover source inventory contains a replaced regular file")
            with path.open("rb") as stream:
                data = stream.read(16 * 1024 * 1024 + 1)
            total += len(data)
            if len(data) > 16 * 1024 * 1024 or total > 64 * 1024 * 1024 or len(entries) >= 4096:
                raise RuntimeError("cutover source inventory exceeded its declared bound")
            entries.append([str(path.relative_to(root)), path.stat().st_mode & 0o777, hashlib.sha256(data).hexdigest()])
    if not entries:
        raise RuntimeError("cutover source inventory is empty")
    encoded = json.dumps(sorted(entries), separators=(",", ":"), ensure_ascii=True).encode()
    return {"file_count": len(entries), "sha256": hashlib.sha256(encoded).hexdigest()}


def completion(driver, source, target, srcname, dstname, state, worker, wire, end, expected, *, retry=None):
    if worker.poll() != 0:
        raise RuntimeError("completion evidence requires a positively joined successful worker")
    retry = completed_retry(driver, state, target.connection(dstname), end) if retry is None else retry
    report = json.loads(retry["reports"][0]["raw_json"])
    row = target.authority(dstname, state, driver.work[state])
    slot = clone.wait_for(lambda: (value if (value := source.slot(srcname, state))["active"] is False else None),
                          "completed source walsender stopped", timeout=10)
    evidence = wire.evidence(timeout=0.02)
    streams = evidence["logical"]["streams"]
    if len(streams) != 1 or any(len(stream["statuses"]) > 128 for stream in streams):
        raise RuntimeError("cutover witness capture is incomplete, ambiguous or over its bound")
    require_runtime_frontiers(row, slot, report)
    require_wire_witness(evidence, report)
    actual = target.fingerprint(dstname, load_manifest())
    clone.require_equal(expected, actual)
    fields = ("generation", "barrier_epoch", "barrier_counter", "applied_order", "last_xid", "begin_ordinal",
              "source_commit_lsn", "source_end_lsn", "certified_lsn", "copy_consistent_point", "drain_end_lsn",
              "drain_witness_lsn", "drain_kind", "follow_completed")
    return {**retry, "caller_goal": saved_goal(driver, state, end), "worker_completed": True,
            "source_fingerprints": expected, "target_fingerprints": actual,
            "target_row": {key: row[key] for key in fields}, "wire": evidence,
            "source_slot": {key: slot[key] for key in ("confirmed_flush_lsn", "restart_lsn")}}


def resource_snapshot(source, srcname, state):
    native = json.loads((state / "manifest.json").read_bytes(), object_pairs_hook=clone.unique_object)
    binding = native["follow_state"]["binding"]
    slot = "'" + binding["slot"]["intent"]["binding"]["slot_name"].replace("'", "''") + "'"
    publication = binding["publication"]["oid"]
    return source.sql(srcname, "SELECT json_build_array((SELECT json_build_array(slot_name,plugin,database,active) FROM pg_replication_slots WHERE slot_name=" + slot + "),(SELECT json_build_array(oid::bigint,pubname,pubowner::bigint) FROM pg_publication WHERE oid=" + str(publication) + "));" )



def completed_resumes(driver, source, target, srcname, dstname, state, launch, expected):
    before = target.authority(dstname, state, driver.work[state])
    if not before or before["follow_completed"] is not True:
        raise RuntimeError("single-frame completed resume requires an already completed exact owned row")
    inventory = resource_snapshot(source, srcname, state)
    native = json.loads((state / "manifest.json").read_bytes(), object_pairs_hook=clone.unique_object)
    origin = native["follow_state"]["origin_name"]
    extra = ("--origin", origin) if origin is not None else ()
    for _ in range(2):
        worker = launch(source.connection(srcname), target.connection(dstname), state, "--resume", "--not-consistent", *extra,
                        source_name=srcname, target_name=dstname)
        finish_completed(driver, worker, already_completed=True)
        if target.authority(dstname, state, driver.work[state]) != before or resource_snapshot(source, srcname, state) != inventory:
            raise RuntimeError("completed resume changed immutable authority or source resource inventory")
        clone.require_equal(expected, target.fingerprint(dstname, load_manifest()))
    return {"completed_resumes": 2, "resume_row_unchanged": True}


def pending_data_fault(gate, transaction):
    if not hasattr(gate, "fault_observation"):
        raise RuntimeError("qualified extended COMMIT fault observation is unavailable")
    value = gate.fault_observation()
    tags = value.get("command_tags", {})
    if (value.get("status") != "observed" or value.get("scope") != "wire_fault_observation"
            or value.get("boundary") != "before_commit" or value.get("protocol") not in ("simple", "extended")
            or type(value.get("connection")) is not int or value["connection"] <= 0
            or type(value.get("backend_pid")) is not int or value["backend_pid"] <= 0
            or value.get("begin_observed") is not True or value.get("overflow") is not False or value.get("classification_invalid") is not False
            or set(tags) != {"insert_0_1", "update_1", "delete_1", "other_dml"}
            or any(type(count) is not int or not 0 <= count < 2**64 for count in tags.values())
            or tags["insert_0_1"] < 1 or tags["update_1"] < 2 or tags["delete_1"] != 0 or tags["other_dml"] != 0):
        raise RuntimeError("held COMMIT did not observe the actual fixture DATA transaction")
    fields = ("xid", "final_lsn", "commit_lsn", "end_lsn", "dml_count")
    if (any(type(transaction.get(name)) is not int for name in fields) or not 3 <= transaction["xid"] < 2**32
            or not 0 < transaction["final_lsn"] == transaction["commit_lsn"] <= transaction["end_lsn"] < 2**64
            or transaction["dml_count"] != 2
            or any(transaction.get(name) is not True for name in ("complete", "begin_forwarded", "commit_forwarded"))):
        raise RuntimeError("held target DATA commands lack the exact forwarded source transaction identity")
    return value


def idle_certification(driver, source, target, srcname, dstname, state, worker):
    marker = lsn(source.sql("postgres", "SELECT pg_logical_emit_message(false,'rpgmigrate_m4','idle-certification');").decode().strip())
    def certified():
        if worker.poll() is not None:
            raise RuntimeError("worker exited before the fixture's fresh idle certification")
        row = target._observe(lambda: target.authority(dstname, state, driver.work[state]))
        return row["certified_lsn"] if isinstance(row, dict) and row.get("certified_lsn") is not None and lsn(row["certified_lsn"]) >= marker else None
    return clone.wait_for(certified, "fresh independently observed idle target certification", timeout=90)


def producer_authority(target, name, state, work, row):
    native = json.loads((state / "manifest.json").read_bytes(), object_pairs_hook=clone.unique_object)
    record = native["target_provenance"][0]["record"]
    if native["format_version"] != 8 or len(row) != 29 or row["version"] != 6:
        raise RuntimeError("M6 producer handoff requires the exact native8 metadata6 layout")
    oid = record["table_oid"]
    catalog = json.loads(target.sql(name, f"""SELECT json_build_object(
'objects',(SELECT json_agg(c ORDER BY c.oid) FROM (SELECT oid::bigint,relnamespace::bigint,relowner::bigint,relname,relkind,relpersistence,relrowsecurity,relforcerowsecurity,relreplident,relam::bigint FROM pg_class WHERE oid IN ({oid},{record['marker_table_oid']},{record['marker_index_oid']})) c),
'columns',(SELECT json_agg(a ORDER BY a.attnum) FROM (SELECT attnum,attname,atttypid::bigint,atttypmod,attnotnull,attcollation::bigint,attisdropped,attgenerated,attidentity,atthasdef FROM pg_attribute WHERE attrelid={oid} AND attnum>0) a),
'constraints',(SELECT coalesce(json_agg(c ORDER BY c.oid),'[]') FROM (SELECT oid::bigint,contype,conkey,convalidated FROM pg_constraint WHERE conrelid={oid}) c),
'indexes',(SELECT json_agg(i ORDER BY i.indexrelid) FROM (SELECT indexrelid::bigint,indisunique,indisprimary,indisvalid,indisready,indkey::text FROM pg_index WHERE indrelid={oid}) i));"""))
    if (not isinstance(catalog, dict) or len(catalog.get("objects", [])) != 3
            or len(catalog.get("columns", [])) != 29 or not catalog.get("indexes") or not catalog.get("constraints")):
        raise RuntimeError("actual M6 producer catalog/provenance output is incomplete")
    return {"receipt_scope": "engine_producer", "probe_checked": False, "independent_go_evidence": "pending_qualified_m6_reader",
            "native_format": 8, "metadata_version": 6, "work": work, "target_provenance": record,
            "follow_state": native["follow_state"], "checkpoint_row": row, "catalog": catalog}


def set_goal(driver, state, target, end):
    saved_goal(driver, state, end)
    result, _ = stream(driver, state, ("stream", "sentinel", "set", "endpos"), end, target=target)
    if result.returncode != 0 or result.stdout != (end + "\n").encode():
        raise RuntimeError("explicit cutover request did not persist the caller's saved endpos")
    persisted_endpos(driver, state, end)


def foreign_resource_inventory(source, srcname, foreign):
    raw = source.sql(srcname, "SELECT json_build_object('slot',(SELECT row_to_json(s) FROM (SELECT slot_name,plugin,database,active,temporary,restart_lsn,confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='" + foreign["slot"] + "') s),'publication',(SELECT row_to_json(p) FROM (SELECT oid::bigint,pubname,pubowner::bigint,puballtables,pubinsert,pubupdate,pubdelete,pubtruncate FROM pg_publication WHERE pubname='" + foreign["publication"] + "') p),'origin',(SELECT row_to_json(o) FROM (SELECT roident::bigint,roname FROM pg_replication_origin WHERE roname='" + foreign["origin"] + "') o));")
    value = json.loads(raw)
    if set(value) != set(foreign) or any(not isinstance(item, dict) or not item for item in value.values()):
        raise RuntimeError("three exact foreign resources were not present in the cleanup fixture")
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def completed_resume_close(driver, source, target, srcname, dstname, state, launch, clean):
    from follow import WireGate
    work, end = driver.work[state], clean["end_lsn"]
    native = json.loads((state / "manifest.json").read_bytes(), object_pairs_hook=clone.unique_object)
    if (native.get("format_version") != 8
            or native["follow_state"].get("cleanup") != {"slot_removed": True, "publication_removed": True}):
        raise RuntimeError("completed close fault requires exact source cleanup and native8 state")
    origin = native["follow_state"]["origin_name"]
    extra = ("--origin", origin) if origin is not None else ()
    suffix = work["run_id"][:16]
    foreign = {kind: "m4_foreign_" + kind + "_" + suffix for kind in ("slot", "publication", "origin")}
    def snapshot():
        request = json.loads((state / "control/endpos.json").read_bytes(), object_pairs_hook=clone.unique_object)
        if type(request.get("end_lsn")) is not int or request["end_lsn"] != lsn(end):
            raise RuntimeError("completed close fault lost the saved exact endpos")
        return {"authority": target.authority(dstname, state, work),
                "native_manifest_sha256": hashlib.sha256((state / "manifest.json").read_bytes()).hexdigest(),
                "local_endpos_sha256": hashlib.sha256((state / "control/endpos.json").read_bytes()).hexdigest(),
                "source_resources": json.loads(resource_snapshot(source, srcname, state)),
                "foreign_inventory": json.loads(foreign_resource_inventory(source, srcname, foreign)),
                "source_fingerprints": source.fingerprint(srcname, load_manifest()),
                "target_fingerprints": target.fingerprint(dstname, load_manifest())}
    before = snapshot()
    if (before["authority"] != clean["producer_authority"]["checkpoint_row"]
            or before["authority"].get("follow_completed") is not True or before["source_resources"] != [None, None]
            or before["foreign_inventory"] != clean["foreign_inventory_after"]):
        raise RuntimeError("completed close fault lacks its immutable completed cleanup prerequisite")
    clone.require_equal(clean["source_fingerprints"], before["source_fingerprints"])
    clone.require_equal(clean["target_fingerprints"], before["target_fingerprints"])
    value = {"observed": True, "scope": "completed_resume_close_fault", "work": work,
             "caller_goal": saved_goal(driver, state, end), "before": before}
    worker = None
    with WireGate(target.host, "completed_resume_close") as gate:
        deadline = time.monotonic() + 10
        def remaining():
            budget = deadline - time.monotonic()
            if budget <= 0:
                raise RuntimeError("completed resume close fault exceeded its shared ten-second deadline")
            return budget
        gate.arm()
        try:
            worker = launch(source.connection(srcname), gate.connection(target.connection(dstname)), state,
                            "--resume", "--not-consistent", *extra, source_name=srcname, target_name=dstname)
            worker.follow_target_wire = gate
            if not gate.held.wait(timeout=remaining()):
                raise RuntimeError("completed resume did not hold the second COMMIT/Idle within ten seconds")
            held = gate.fault_observation()
            require_completed_close_fault(held, delivered=False)
            wire = gate.snapshot()
            matches = [item for item in wire.get("connections", []) if item.get("connection") == held["connection"]]
            if (wire.get("status") != "observed" or wire.get("overflow") is not False or len(matches) != 1
                    or matches[0].get("backend_pid") != held["backend_pid"]
                    or matches[0].get("unknown") is not False or matches[0].get("overflow") is not False):
                raise RuntimeError("completed close fault has no bound BackendKeyData connection witness")
            def absent():
                def read():
                    return json.loads(target.sql(dstname, "SELECT json_build_object('system_identifier',(pg_control_system()).system_identifier,"
                        "'database_oid',(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),"
                        "'absent',NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=" + str(held["backend_pid"]) + "));"))
                remaining()
                previous = getattr(target, "_observation_deadline", None)
                target._observation_deadline = min(previous, deadline) if previous is not None else deadline
                try:
                    observed = target._observe(read)
                finally:
                    if previous is None:
                        del target._observation_deadline
                    else:
                        target._observation_deadline = previous
                remaining()
                expected = {"system_identifier": before["authority"]["target_system"],
                            "database_oid": before["authority"]["target_database"], "absent": True}
                return {"connection": held["connection"], "backend_pid": held["backend_pid"], **observed} if observed == expected else None
            absence = clone.wait_for(absent, "wire-observed target backend independently absent", timeout=remaining())
            value.update(fault_held=held, target_wire=wire, backend_absence=absence)
            gate.release.set()
            try:
                status = worker.wait(timeout=remaining())
            except subprocess.TimeoutExpired:
                raise RuntimeError("completed resume close worker exceeded its ten-second fault deadline") from None
            stdout, stderr = worker.output.read_bytes(), worker.error_output.read_bytes()
            check_diagnostics(stdout, stderr, worker.environment, status)
            value["worker_result"] = command_observation(subprocess.CompletedProcess([], status, stdout, stderr))
            value["worker_reaped"] = worker.poll() is not None
            def next_request():
                remaining()
                observation = gate.fault_observation()
                remaining()
                return observation if observation.get("terminal_delivered") is True and observation.get("next_frontend_kind") is not None else None
            value["fault"] = clone.wait_for(next_request, "completed resume first post-terminal frontend frame", timeout=remaining())
            value["fault_hits"] = gate.evidence()["hits"]
        finally:
            gate.release.set()
            if worker is not None and worker.poll() is None:
                driver.kill(worker)
    value["relay_workers_reaped"] = True
    value["fault"] = gate.fault_observation()
    value["after"] = snapshot()
    try:
        require_completed_close_fault(value["fault"], delivered=True)
        require_checked_close_refusal(value["worker_result"])
        if value["after"] != before:
            raise RuntimeError("completed resume close fault changed immutable state")
    except RuntimeError:
        completed_report = None
        try:
            if value["worker_result"]["returncode"] == 0:
                binding = json.loads(worker.binding.read_bytes(), object_pairs_hook=clone.unique_object)
                completed_report = json.loads(completed_output(stdout, binding, already_completed=True)[15:])
        except (RuntimeError, ValueError):
            pass
        details = {key: value[key] for key in ("scope", "work", "caller_goal", "fault_held", "fault", "fault_hits",
                   "target_wire", "backend_absence", "worker_result", "worker_reaped", "relay_workers_reaped")}
        details.update(receipt_scope="diagnostic", durability_confirmed=False)
        details["preservation"] = {key: {
            "before_sha256": hashlib.sha256(json.dumps(item, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "after_sha256": hashlib.sha256(json.dumps(value["after"][key], sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "unchanged": value["after"][key] == item} for key, item in before.items()}
        details.update(completed_report=completed_report, authority_unchanged=value["after"]["authority"] == before["authority"],
                       state_unchanged=value["after"] == before,
                       native_manifest_sha256=before["native_manifest_sha256"], local_endpos_sha256=before["local_endpos_sha256"])
        raise clone.SubprocessFailure(clone.private_failure("engine-completed-resume-close", value["worker_result"]["returncode"],
                                     json.dumps(details).encode(), worker.environment, sys._getframe())) from None
    recovery = launch(source.connection(srcname), target.connection(dstname), state, "--resume", "--not-consistent", *extra,
                      source_name=srcname, target_name=dstname)
    finish_completed(driver, recovery, already_completed=True)
    value["fault_free_resume"] = {"binding": json.loads(recovery.binding.read_bytes()),
                                 "raw_json": recovery.output.read_bytes()[15:].decode().rstrip("\n"),
                                 "probe_checked": False, "independent_go_evidence": "pending_qualified_m6_reader"}
    value["recovered"] = snapshot()
    validate_completed_close(value, clean)
    return value


def cleanup_case(driver, source, target, srcname, dstname, state, launch, completed):
    native = json.loads((state / "manifest.json").read_bytes(), object_pairs_hook=clone.unique_object)
    binding = native["follow_state"]["binding"]
    owned_slot = binding["slot"]["intent"]["binding"]["slot_name"]
    publication = binding["publication"]
    suffix = driver.work[state]["run_id"][:16]
    foreign = {kind: "m4_foreign_" + kind + "_" + suffix for kind in ("slot", "publication", "origin")}
    source.sql(srcname, "SELECT pg_create_logical_replication_slot('" + foreign["slot"] + "','pgoutput');")
    source.sql(srcname, "CREATE PUBLICATION " + clone.quote(foreign["publication"]) + ";")
    source.sql(srcname, "SELECT pg_replication_origin_create('" + foreign["origin"] + "');")
    def foreign_inventory():
        return foreign_resource_inventory(source, srcname, foreign)
    before = foreign_inventory()
    row = target.authority(dstname, state, driver.work[state])
    expected = completed["source_fingerprints"]
    obligations = [{"kind": "replication_slot", "name": owned_slot, "owned": True, "removed": True},
                   {"kind": "publication", "name": publication["name"], "owned": True, "removed": True},
                   {"kind": "workdir", "name": str(state.resolve(strict=True)), "owned": True, "removed": False}]
    origin = native["follow_state"]["origin_name"]
    if origin is not None:
        obligations.append({"kind": "replication_origin", "name": origin, "owned": False, "removed": False})
    def compare():
        driver.inspect(("compare", "schema"), source.connection(srcname), target.connection(dstname), state)
        return driver.inspect(("compare", "data"), source.connection(srcname), target.connection(dstname), state, json_output=True)
    comparisons_before = compare()
    records = []
    for _ in range(2):
        result, path = stream(driver, state, ("stream", "cleanup"), source=source.connection(srcname), target=target.connection(dstname))
        if result.returncode != 0:
            raise RuntimeError("exact owned cleanup command did not complete")
        caller = json.loads(path.read_bytes())
        cleanup_output(result.stdout, caller, obligations)
        records.append({"binding": caller, "raw_json": result.stdout[15:].decode(), "probe_checked": False,
                        "independent_go_evidence": "pending_qualified_m6_reader"})
        inventory = json.loads(resource_snapshot(source, srcname, state))
        if inventory != [None, None] or foreign_inventory() != before:
            raise RuntimeError("cleanup retained an owned resource or changed an exact foreign resource")
        if not state.is_dir() or target.authority(dstname, state, driver.work[state]) != row:
            raise RuntimeError("cleanup removed the retained workdir or changed target authority")
        clone.require_equal(expected, target.fingerprint(dstname, load_manifest()))
    comparisons_after = compare()
    resumes = completed_resumes(driver, source, target, srcname, dstname, state, launch, expected)
    after = foreign_inventory()
    return {**completed, **resumes, "cleanup_reports": records, "obligations": obligations,
            "retained_workdir": True, "metadata_retained": True, "owned_resources_absent": True,
            "comparisons_before_and_after": True, "comparison_tables_before": comparisons_before,
            "comparison_tables_after": comparisons_after, "foreign_resource_count": 3,
            "foreign_inventory_before": json.loads(before), "foreign_inventory_after": json.loads(after),
            "foreign_inventory_before_sha256": hashlib.sha256(before).hexdigest(),
            "foreign_inventory_after_sha256": hashlib.sha256(after).hexdigest()}


def run(args, source, target, manifest, case, launch, driver):
    from contextlib import ExitStack
    from follow import WireGate, require_data_fault
    if args.report_probe is not None or getattr(driver, "report_probe", None) is not None:
        raise RuntimeError("cutover producer checks cannot use the clone-only Go probe")
    cases = {}
    srcname, dstname, src, dst, state = case("cutover_retry")
    with ExitStack() as gates:
        def start(srcname, dstname, src, dst, state, wire, *, fault=None):
            worker = launch(wire.connection(src), fault.connection(dst) if fault else dst, state,
                            source_name=srcname, target_name=dstname, wire=wire)
            if fault is not None:
                worker.follow_target_wire = fault
            driver.ready(worker)
            return worker
        def collect(srcname, dstname, state, worker, wire, end, expected, **arguments):
            result = completion(driver, source, target, srcname, dstname, state, worker, wire, end, expected, **arguments)
            row = target.authority(dstname, state, driver.work[state])
            result["producer_authority"] = producer_authority(target, dstname, state, driver.work[state], row)
            return result
        def hold_data(srcname, dstname, state, worker, wire, fault):
            idle_certification(driver, source, target, srcname, dstname, state, worker)
            previous = target.authority(dstname, state, driver.work[state])
            initial = wire.evidence()
            fault.arm()
            source.sql(srcname, '''BEGIN; UPDATE "M2 space".wide SET changed=changed+1 WHERE id=1; INSERT INTO "M2 space".empty VALUES(5001,'cutover-pending'); COMMIT;''')
            clone.wait_for(lambda: fault.held.is_set(), "actual cutover DATA COMMIT held", timeout=30)
            observation = fault.fault_observation()
            proof = require_data_fault(initial, wire.evidence(), observation, previous, driver.work[state],
                                       target.held_session(dstname, observation, driver.work[state]))
            pending_data_fault(fault, proof["source"])
            return proof
        wire = gates.enter_context(WireGate("source", "observe"))
        fault = gates.enter_context(WireGate("target", "before_commit"))
        worker = start(srcname, dstname, src, dst, state, wire, fault=fault)
        data = hold_data(srcname, dstname, state, worker, wire, fault)
        end = frozen_end(source, srcname)
        goal = saved_goal(driver, state, end)
        expected = source.fingerprint(srcname, manifest)
        cases[CASES[0]] = {**active_fence_request(driver, state, dst, worker, fault, end,
                              observe=lambda deadline: diagnostic_authority(driver, target, dstname, state, deadline)),
                          "caller_goal": goal, "data_fault": data}
        fault.release.set()
        finish_completed(driver, worker)
        completed = collect(srcname, dstname, state, worker, wire, end, expected)
        cases[CASES[1]] = completed
        cases[CASES[3]] = async_visible_refusal(driver, target, dstname, state)
        cases[CASES[8]] = cleanup_case(driver, source, target, srcname, dstname, state, launch, completed)
        cases[CASES[9]] = completed_resume_close(driver, source, target, srcname, dstname, state, launch, cases[CASES[8]])

        srcname, dstname, src, dst, state = case("cutover_certification")
        wire = gates.enter_context(WireGate("source", "observe"))
        worker = start(srcname, dstname, src, dst, state, wire)
        end = frozen_end(source, srcname)
        saved_goal(driver, state, end)
        expected = source.fingerprint(srcname, manifest)
        retry = certification_race(driver, source, target, srcname, dstname, state, worker, end=end)
        cases[CASES[2]] = collect(srcname, dstname, state, worker, wire, end, expected, retry=retry)

        for index, setting in ((4, "on"), (5, "off")):
            srcname, dstname, src, dst, state = case("cutover_load_" + setting)
            wire = gates.enter_context(WireGate("source", "observe"))
            worker = start(srcname, dstname, src, dst, state, wire)
            writer = source_batch(source, srcname, setting)
            end = frozen_end(source, srcname)
            expected = source.fingerprint(srcname, manifest)
            set_goal(driver, state, dst, end)
            finish_completed(driver, worker)
            cases[CASES[index]] = {**collect(srcname, dstname, state, worker, wire, end, expected),
                                  "writer": writer, "source_write_freeze": True, "outstanding_transactions": 0,
                                  "oracle_scope": "post_freeze_complete_source"}

        srcname, dstname, src, dst, state = case("cutover_later_begin")
        wire = gates.enter_context(WireGate("source", "later_begin_only"))
        wire.arm()
        worker = start(srcname, dstname, src, dst, state, wire)
        clone.wait_for(lambda: wire.evidence()["dropped_keepalives"] > 0, "actual keepalive loss before later BEGIN", timeout=20)
        end = frozen_end(source, srcname)
        expected = source.fingerprint(srcname, manifest)
        baseline = target.authority(dstname, state, driver.work[state])
        set_goal(driver, state, dst, end)
        source.sql(srcname, '''INSERT INTO "M2 space".empty VALUES(6001,'later-only');''')
        clone.wait_for(lambda: wire.held.is_set(), "later BEGIN received before forwarding", timeout=20)
        before = wire.evidence()
        transactions = [tx for stream in before["logical"]["streams"] for tx in stream["transactions"]]
        if len(transactions) != 1 or transactions[0]["begin_forwarded"] or transactions[0]["final_lsn"] <= lsn(end):
            raise RuntimeError("controlled source did not hold exactly one real later BEGIN")
        later_oracle = source.fingerprint(srcname, manifest)
        if later_oracle == expected:
            raise RuntimeError("later source transaction did not change the saved pre-later oracle")
        wire.release.set()
        finish_completed(driver, worker)
        value = collect(srcname, dstname, state, worker, wire, end, expected)
        streams = value["wire"]["logical"]["streams"]
        begins = [tx for stream in streams for tx in stream["transactions"]]
        received = sum(len(stream["keepalives"]) for stream in streams)
        cases[CASES[6]] = {**value, "scope": "algorithm_fault_scope_later_begin", "oracle_scope": "saved_pre_later_source",
                           "baseline_target_row": baseline, "later_source_fingerprints": later_oracle,
                           "dropped_keepalives": value["wire"]["dropped_keepalives"], "received_keepalives": received,
                           "forwarded_keepalives": sum(item["forwarded"] for stream in streams for item in stream["keepalives"]),
                           "begin_received": len(begins), "begin_forwarded": sum(tx["begin_forwarded"] for tx in begins)}

        srcname, dstname, src, dst, state = case("cutover_crossing")
        wire = gates.enter_context(WireGate("source", "observe"))
        fault = gates.enter_context(WireGate("target", "before_commit"))
        worker = start(srcname, dstname, src, dst, state, wire, fault=fault)
        data = hold_data(srcname, dstname, state, worker, wire, fault)
        transaction = data["source"]
        frozen_end(source, srcname)
        point = transaction["commit_lsn"]
        end = f"{point >> 32:X}/{point & 0xffffffff:X}"
        expected = source.fingerprint(srcname, manifest)
        set_goal(driver, state, dst, end)
        fault.release.set()
        finish_completed(driver, worker)
        value = collect(srcname, dstname, state, worker, wire, end, expected)
        canonical = {key: item for key, item in transaction.items() if key != "baseline_end"}
        cases[CASES[7]] = {**value, "scope": "algorithm_fault_scope_crossing_commit_record", "transaction": canonical,
                           "data_fault": data, **completed_resumes(driver, source, target, srcname, dstname, state, launch, expected)}
    validate_cases(cases)
    return cases
