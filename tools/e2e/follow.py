#!/usr/bin/env python3
"""Qualify ongoing follow with private source/target containers and independent SQL."""

import argparse
from contextlib import ExitStack
import ctypes
import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import zlib

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clone
from follow_wire import WireGate
sys.path.insert(0, str(clone.ROOT / "tests/support"))
from follow_snapshot_fixture import validate_identity_modes

FIXTURES = clone.ROOT / "tests/fixtures/follow"
CASES = ("initial_copy_dml", "truncate", "full_duplicates_nulls", "target_type_oids", "oracle_mutation",
         "before_commit_rollback", "after_commit_before_feedback", "partial_receive_transaction", "repeated_restart",
         "confirmed_flush_gap", "source_wal_loss", "active_foreign_slot", "copy_restart", "predata_handoff_refusal",
         "postdata_refusal", "backpressure", "generated_refusal", "check_refusal", "domain_check_refusal",
         "expression_index_refusal", "partial_index_refusal")


INITIAL_DML = '''INSERT INTO "M2 space".rows(id,value,feeling,positive) OVERRIDING SYSTEM VALUE VALUES(2001,'insert','busy',4);
UPDATE "M2 space".rows SET value='updated',nullable=NULL WHERE id=2001;
DELETE FROM "M2 space".rows WHERE id=1;
INSERT INTO "M2 space".empty VALUES(10,'primary key');
UPDATE "M2 space".empty SET id=11,value='changed primary key' WHERE id=10;
UPDATE "M2 space".indexed SET id=2,value='changed key' WHERE id=1;
UPDATE "M2 space".wide SET changed=1 WHERE id=1;
INSERT INTO "M2 space".rows(id,value,email,emails,address,size,sizes) OVERRIDING SYSTEM VALUE VALUES(2002,'typed','Typed@Example.COM','{A,a,NULL}','Mixed@Case','(1.50,,"label, ""quoted"" (x)",busy)','{"(2,3,B,calm)",NULL}');
UPDATE "M2 space".rows SET email='typed@example.com',size=ROW(9.25,NULL,'',NULL),sizes=NULL WHERE id=2002;
UPDATE "M2 space".rows SET size=NULL,emails='{}',address='BOX 2' WHERE id=2;
DELETE FROM "M2 space".rows WHERE id=3;
INSERT INTO "M2 space".full_rows VALUES(2,'case','Alpha'),(2,'case','ALPHA');
UPDATE "M2 space".full_rows SET value='upper' WHERE id=2 AND label::text='ALPHA';'''


def check_workload(source, name, manifest):
    before = source.fingerprint(name, manifest)
    source.sql(name, "BEGIN;\n" + INITIAL_DML + "\nROLLBACK;")
    clone.require_equal(before, source.fingerprint(name, manifest))


def load_manifest(path=FIXTURES / "cases.json"):
    manifest = clone.load_manifest(path)
    if tuple(manifest["cases"]) != CASES:
        raise RuntimeError("follow case inventory differs from reviewed coverage")
    return manifest


def lsn(value):
    if not isinstance(value, str) or len(value) > 17 or not re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", value):
        raise RuntimeError("missing or invalid independently observed LSN")
    high, low = (int(part, 16) for part in value.split("/"))
    if high > 0xffffffff or low > 0xffffffff:
        raise RuntimeError("observed LSN exceeds uint64")
    return high * 2**32 + low


def validate_drain_row(row):
    names = ("drain_end_lsn", "drain_witness_lsn", "drain_kind", "follow_completed")
    if not set(names) <= row.keys() or type(row["follow_completed"]) is not bool:
        raise RuntimeError("checkpoint drain fields are absent or invalid")
    values = [row[name] for name in names[:3]]
    if all(value is None for value in values):
        if row["follow_completed"]:
            raise RuntimeError("completed checkpoint lacks drain coverage")
        return
    if any(value is None for value in values) or row["drain_kind"] not in ("keepalive", "later_begin"):
        raise RuntimeError("checkpoint drain coverage is partial or unsupported")
    end, witness = lsn(row["drain_end_lsn"]), lsn(row["drain_witness_lsn"])
    if not end or witness < end or row["drain_kind"] == "later_begin" and witness == end:
        raise RuntimeError("checkpoint drain witness does not cover endpos")


def authority_frontier(row):
    applied = lsn(row["source_end_lsn"]) if row.get("source_end_lsn") is not None else 0
    certified = lsn(row["certified_lsn"]) if row.get("certified_lsn") is not None else 0
    if applied:
        commit = lsn(row.get("source_commit_lsn"))
        if (type(row.get("applied_order")) is not int or row["applied_order"] < 1
                or row.get("begin_ordinal") != commit or not 0 < commit <= applied
                or type(row.get("last_xid")) is not int or not 0 < row["last_xid"] <= 0xffffffff):
            raise RuntimeError("applied frontier lacks canonical transaction identity")
    elif row.get("applied_order") != 0 or any(row.get(key) is not None for key in ("last_xid", "begin_ordinal", "source_commit_lsn")):
        raise RuntimeError("partial applied frontier")
    return max(applied, certified)


def require_feedback(evidence, row, goal, start=0):
    frontier = authority_frontier(row)
    statuses = evidence["statuses"][start:]
    if not frontier or frontier < goal or not statuses or statuses[-1][1] < goal:
        return False
    previous = 0
    for write, flush, apply in statuses:
        if write != flush or flush != apply or flush > frontier or flush < previous:
            raise RuntimeError("standby status exceeds observed target authority or regresses")
        previous = flush
    return True


def checkpoint_observation(row):
    if row is None:
        return {"status": "missing"}
    if not {"source_commit_lsn", "source_end_lsn", "certified_lsn", "copy_consistent_point", "last_xid", "begin_ordinal"} <= row.keys():
        raise RuntimeError("checkpoint observation nullable fields are absent")
    authority_frontier(row)
    validate_drain_row(row)
    result = {"status": "observed", "drain_kind": row["drain_kind"], "follow_completed": row["follow_completed"]}
    for key in ("version", "source_system", "source_database", "generation", "target_system", "target_database",
                "owner_oid", "schema_oid", "table_oid", "applied_order", "last_xid", "begin_ordinal", "barrier_epoch", "barrier_counter"):
        value = row.get(key)
        if value is None and key in ("last_xid", "begin_ordinal"):
            result[key] = None
            continue
        bound = 2**32 if key in ("source_database", "target_database", "owner_oid", "schema_oid", "table_oid", "last_xid") else 2**64
        minimum = 0 if key in ("applied_order", "barrier_epoch") else 1
        if type(value) is not int or not minimum <= value < bound:
            raise RuntimeError("checkpoint observation numeric identity or counter is invalid")
        result[key] = value
    if result["version"] != 6:
        raise RuntimeError("checkpoint observation metadata version differs")
    for key in ("source_commit_lsn", "source_end_lsn", "certified_lsn", "copy_consistent_point", "drain_end_lsn", "drain_witness_lsn"):
        result[key] = lsn(row[key]) if row.get(key) is not None else None
        if result[key] == 0:
            raise RuntimeError("checkpoint observation has a zero authority LSN")
    return result


def failure_feedback_consistency(evidence, observation, initial):
    def verdict(status, reason):
        result = {"scope": "visible-observation", "status": status, "reason": reason}
        if status == "consistent":
            result["meaning"] = "consistent with observed wire/checkpoint frontiers"
        return result

    def integer(value, bound=2**64, minimum=0):
        if type(value) is not int or not minimum <= value < bound:
            raise ValueError
        return value

    def checkpoint(value):
        if value["status"] != "observed":
            raise ValueError
        row = dict(value)
        for key in ("source_commit_lsn", "source_end_lsn", "certified_lsn", "copy_consistent_point", "drain_end_lsn", "drain_witness_lsn"):
            number = row[key]
            if number is not None:
                integer(number)
                row[key] = f"{number >> 32:X}/{number & 0xffffffff:X}"
        return checkpoint_observation(row)

    try:
        if observation["scope"] != "visible-observation" or initial["scope"] != "visible-observation":
            raise ValueError
        target, baseline = checkpoint(observation["target"]), checkpoint(initial["target"])
        identities = ("source_system", "source_database", "generation", "target_system", "target_database", "owner_oid", "schema_oid", "table_oid", "copy_consistent_point")
        if any(target[key] != baseline[key] for key in identities):
            return verdict("inconsistent", "owned_checkpoint_identity_changed")
        if observation["slot"]["status"] != "observed" or initial["slot"]["status"] != "observed":
            raise ValueError
        floor = integer(initial["slot"]["confirmed_flush_lsn"])
        confirmed = integer(observation["slot"]["confirmed_flush_lsn"])
        applied, certified = target["source_end_lsn"] or 0, target["certified_lsn"] or 0
        anchor = baseline["copy_consistent_point"] or 0
        baseline_frontier = max(baseline["source_end_lsn"] or 0, baseline["certified_lsn"] or 0)
        if (floor > max(baseline_frontier, anchor) or confirmed < floor
                or applied < (baseline["source_end_lsn"] or 0) or target["applied_order"] < baseline["applied_order"]
                or max(applied, certified) < baseline_frontier):
            return verdict("inconsistent", "confirmed_floor_or_checkpoint_regressed")
        logical = evidence["logical"]
        if logical["overflow"] is not False:
            raise ValueError
        connection = integer(initial["connection"], minimum=1)
        streams = [stream for stream in logical["streams"] if stream["connection"] == connection]
        if len(streams) != 1:
            raise ValueError
        stream = streams[0]
        if stream["confirmed"] is not True or stream["complete"] is not True or stream["invalid"] is not False or stream["overflow"] is not False:
            raise ValueError
        start = integer(initial["status_start"])
        statuses = stream["statuses"]
        if not start < len(statuses) <= 10000 or len(stream["transactions"]) > 256 or len(stream["keepalives"]) > 256:
            raise ValueError
        previous = floor
        for positions in statuses[start:]:
            if len(positions) != 3:
                raise ValueError
            write, flush, apply = (integer(value) for value in positions)
            if write != flush or flush != apply or flush < previous or flush > max(applied, certified, floor):
                return verdict("inconsistent", "feedback_positions_differ_regress_or_exceed_checkpoint")
            previous = flush
        if confirmed > max(applied, certified, floor):
            return verdict("inconsistent", "source_confirmed_exceeds_checkpoint")
        feedback = max(previous, confirmed)
        previous_end = 0
        for tx in stream["transactions"]:
            if integer(tx["connection"], minimum=1) != connection or tx["complete"] is not True:
                raise ValueError
            final, commit, end = (integer(tx[key], minimum=1) for key in ("final_lsn", "commit_lsn", "end_lsn"))
            xid = integer(tx["xid"], 2**32, 3)
            integer(tx["dml_count"])
            if (final != commit or end < commit or final < previous_end or end <= previous_end
                    or type(tx["begin_forwarded"]) is not bool or type(tx["commit_forwarded"]) is not bool
                    or tx["commit_forwarded"] and not tx["begin_forwarded"]):
                raise ValueError
            previous_end = end
            covered = end <= anchor or end <= applied
            if not covered and end == commit:
                return verdict("unavailable", "unobserved_commit_record_span")
            if end == applied and (target["source_commit_lsn"] != commit or target["begin_ordinal"] != final or target["last_xid"] != xid):
                return verdict("inconsistent", "applied_canonical_identity_differs")
            if not covered and feedback >= end:
                return verdict("inconsistent", "feedback_covers_unapplied_transaction")
            # Stock decoding skips when its restart position is strictly beyond C.commit.
            if not covered and commit < feedback < end:
                return verdict("inconsistent", "feedback_inside_unapplied_commit_record")
        keepalive_basis = False
        for keepalive in stream["keepalives"]:
            point = integer(keepalive["lsn"])
            if integer(keepalive["connection"], minimum=1) != connection or type(keepalive["idle"]) is not bool or type(keepalive["forwarded"]) is not bool:
                raise ValueError
            keepalive_basis |= keepalive["idle"] and keepalive["forwarded"] and feedback <= point <= certified
        if feedback > max(baseline_frontier, applied, floor) and not keepalive_basis:
            return verdict("unavailable", "new_certified_progress_lacks_forwarded_idle_keepalive")
        if confirmed > previous:
            return verdict("unavailable", "source_feedback_is_later_than_wire_snapshot")
        return verdict("consistent", "bounded_frontiers_agree")
    except (KeyError, TypeError, ValueError, RuntimeError, OverflowError):
        return verdict("unavailable", "missing_incomplete_or_malformed_evidence")


def require_failure_feedback(evidence, observation, initial):
    result = failure_feedback_consistency(evidence, observation, initial)
    if result["status"] == "inconsistent":
        raise RuntimeError("follow feedback observation is inconsistent: " + result["reason"])
    return result["status"] == "consistent"


def bounded_wire_observation(evidence):
    def number(value, nullable=False):
        if nullable and value is None:
            return None
        if type(value) is not int or not 0 <= value < 2**64:
            raise ValueError
        return value

    def positions(values):
        result = []
        for value in values:
            if len(value) != 3:
                raise ValueError
            result.append(tuple(number(position) for position in value))
        return result

    try:
        result = {key: number(evidence[key]) for key in ("hits", "data_bytes", "begins", "commits")}
        result.update(status_count=len(evidence["statuses"]), statuses=positions(evidence["statuses"][-32:]))
        if "logical" not in evidence:
            return result
        logical = evidence["logical"]
        if type(logical["overflow"]) is not bool:
            raise ValueError
        streams, remaining = [], {"transactions": 32, "keepalives": 32, "statuses": 32}
        for stream in reversed(logical["streams"][-16:]):
            retained = {"connection": number(stream["connection"]), "start_lsn": number(stream["start_lsn"], True)}
            for key in ("confirmed", "invalid", "overflow", "complete"):
                if type(stream[key]) is not bool:
                    raise ValueError
                retained[key] = stream[key]
            for collection in ("transactions", "keepalives", "statuses"):
                records = stream[collection]
                count = min(len(records), remaining[collection])
                remaining[collection] -= count
                retained[collection] = []
                if len(records) > count:
                    retained.update(overflow=True, complete=False)
                for record in records[len(records) - count:]:
                    if collection == "statuses":
                        retained[collection].extend(positions([record]))
                        continue
                    keys = ("connection", "final_lsn", "xid", "dml_count", "commit_lsn", "end_lsn") if collection == "transactions" else ("connection", "lsn")
                    projected = {key: number(record[key], key in ("commit_lsn", "end_lsn")) for key in keys}
                    flags = ("complete", "begin_forwarded", "commit_forwarded") if collection == "transactions" else ("idle", "forwarded")
                    for key in flags:
                        if type(record[key]) is not bool:
                            raise ValueError
                        projected[key] = record[key]
                    retained[collection].append(projected)
            streams.append(retained)
        result["logical"] = {"overflow": logical["overflow"] or len(logical["streams"]) > 16, "streams": list(reversed(streams))}
        return result
    except (KeyError, TypeError, ValueError, OverflowError):
        return {"observer_failed": True}


def backpressure_event(output):
    events = []
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except (ValueError, UnicodeDecodeError):
            continue
        if isinstance(event, dict) and event.get("event") == "follow.backpressure":
            events.append(event)
    if not events:
        return None
    if len(events) != 1:
        raise RuntimeError("follow backpressure event is not emitted exactly once")
    queue = events[0].get("queue", {})
    if (not isinstance(queue, dict) or set(queue) != {"messages", "owned_bytes", "limit_messages", "limit_bytes"}
            or any(type(value) is not int or value <= 0 for value in queue.values())
            or queue["messages"] > queue["limit_messages"] or queue["owned_bytes"] > queue["limit_bytes"]):
        raise RuntimeError("follow backpressure event lacks bounded observed FIFO occupancy")
    return events[0]


def require_stopped(status):
    if status is None or status == 0:
        raise RuntimeError("M3 ongoing follow cannot exit successfully")


def validate_crash_sessions(sessions, database_oid=None):
    if not isinstance(sessions, list) or not 1 <= len(sessions) <= 64:
        raise RuntimeError("target crash preservation requires a bounded nonempty session identity set")
    seen = set()
    for session in sessions:
        if (not isinstance(session, dict) or set(session) != {"pid", "backend_start", "database_oid"}
                or type(session["pid"]) is not int or not 0 < session["pid"] < 2**31
                or type(session["database_oid"]) is not int or not 0 < session["database_oid"] < 2**32
                or database_oid is not None and session["database_oid"] != database_oid
                or not isinstance(session["backend_start"], str) or not 1 <= len(session["backend_start"]) <= 64):
            raise RuntimeError("target crash session identity is invalid")
        try:
            started = datetime.datetime.fromisoformat(session["backend_start"])
        except ValueError:
            raise RuntimeError("target crash backend start is invalid") from None
        identity = session["pid"], session["backend_start"], session["database_oid"]
        if started.tzinfo is None or identity in seen:
            raise RuntimeError("target crash session identity is incomplete or duplicated")
        seen.add(identity)


def validate_data_fault(proof, boundary, runs):
    try:
        def number(value, maximum=2**64, minimum=0):
            if type(value) is not int or not minimum <= value < maximum:
                raise ValueError
            return value
        if set(proof) != {"source", "fault", "session", "run_id"} or proof["run_id"] not in runs:
            raise ValueError
        run = proof["run_id"]
        if not isinstance(run, str) or re.fullmatch("[0-9a-f]{32}", run) is None:
            raise ValueError
        tx, fault, session = (proof[key] for key in ("source", "fault", "session"))
        if set(tx) != {"connection", "baseline_end", "final_lsn", "commit_lsn", "end_lsn", "xid", "dml_count", "complete", "begin_forwarded", "commit_forwarded"}:
            raise ValueError
        number(tx["connection"], minimum=1)
        number(tx["xid"], 2**32, 3)
        floor, final, commit, end = (number(tx[key]) for key in ("baseline_end", "final_lsn", "commit_lsn", "end_lsn"))
        # The isolated fixture emits one UPDATE and one INSERT in one transaction.
        # Source telemetry counts DML; target command tags distinguish DATA from certification.
        if (not floor < final == commit < end or type(tx["dml_count"]) is not int or tx["dml_count"] != 2
                or any(tx[key] is not True for key in ("complete", "begin_forwarded", "commit_forwarded"))):
            raise ValueError
        if (set(fault) != {"status", "scope", "boundary", "protocol", "connection", "backend_pid", "begin_observed", "command_tags", "overflow", "classification_invalid"}
                or fault["status"] != "observed" or fault["scope"] != "wire_fault_observation" or fault["boundary"] != boundary
                or boundary not in ("before_commit", "after_commit") or fault["protocol"] not in ("simple", "extended")
                or fault["begin_observed"] is not True or fault["overflow"] is not False or fault["classification_invalid"] is not False):
            raise ValueError
        number(fault["connection"], minimum=1)
        number(fault["backend_pid"], 2**31, 1)
        tags = fault["command_tags"]
        if set(tags) != {"insert_0_1", "update_1", "delete_1", "other_dml"}:
            raise ValueError
        for value in tags.values():
            number(value)
        if tags["insert_0_1"] < 1 or tags["update_1"] < 2 or tags["delete_1"] or tags["other_dml"]:
            raise ValueError
        if set(session) != {"pid", "backend_start", "database_oid", "fence_class", "fence_slot"}:
            raise ValueError
        validate_crash_sessions([{key: session[key] for key in ("pid", "backend_start", "database_oid")}])
        if (session["pid"] != fault["backend_pid"] or type(session["fence_class"]) is not int or session["fence_class"] != 0x5250474d
                or type(session["fence_slot"]) is not int or session["fence_slot"] != zlib.crc32(bytes.fromhex(run))):
            raise ValueError
    except (KeyError, TypeError, ValueError, RuntimeError):
        raise RuntimeError("held COMMIT lacks canonical source DATA and owned target session evidence") from None


def require_data_fault(initial, current, fault, previous, work, session):
    try:
        snapshots = []
        for evidence in (initial, current):
            logical = evidence["logical"]
            if logical["overflow"] is not False or len(logical["streams"]) != 1:
                raise ValueError
            stream = logical["streams"][0]
            if (stream["confirmed"] is not True or stream["complete"] is not True
                    or stream["invalid"] is not False or stream["overflow"] is not False):
                raise ValueError
            snapshots.append(stream)
        before, after = snapshots
        transactions = after["transactions"]
        if (after["connection"] != before["connection"] or len(transactions) != len(before["transactions"]) + 1
                or transactions[:-1] != before["transactions"] or len(transactions) > 256):
            raise ValueError
        tx = transactions[-1]
        if tx["connection"] != after["connection"] or session["database_oid"] != previous["target_database"]:
            raise ValueError
        proof = {"source": {**tx, "baseline_end": lsn(previous["source_end_lsn"]) if previous["source_end_lsn"] is not None else 0},
                 "fault": fault, "session": session, "run_id": work["run_id"]}
        validate_data_fault(proof, fault["boundary"], {work["run_id"]})
        return proof
    except (KeyError, TypeError, ValueError, RuntimeError):
        raise RuntimeError("held COMMIT is not the newly forwarded fixture DATA transaction") from None


def validate_receipt(report, manifest, *, diagnostic_case=None):
    scope = report.get("receipt_scope", "full")
    if diagnostic_case is not None:
        if diagnostic_case != "before_commit_rollback" or scope != "diagnostic" or report.get("diagnostic_case") != diagnostic_case:
            raise RuntimeError("follow diagnostic receipt differs from the selected case")
        selected = {**manifest, "cases": [diagnostic_case]}
    else:
        if scope not in ("full", "smoke") or report.get("diagnostic_case") is not None:
            raise RuntimeError("unknown follow receipt scope")
        selected = manifest if scope == "full" else {**manifest, "cases": ["initial_copy_dml"]}
    clone.validate_receipt(report, selected, require_reports=False)
    if report.get("mode") != "ongoing_follow" or report.get("successful_follow_exit") is not False:
        raise RuntimeError("follow receipt fabricates completion")
    validation = report.get("report_validation", {})
    records = validation.get("reports", [])
    mode = validation.get("mode")
    if (mode not in ("local_m3_contract", "operator_go_parse_report") or not records
            or scope == "smoke" and len(records) != 1 or scope == "diagnostic" and len(records) != 2):
        raise RuntimeError("actual bound follow startup report validation is absent")
    for record in records:
        clone.validate_operator_report(record["raw_json"], record["binding"], "clone", False, phases=("streaming",))
        if record.get("probe_checked") is not (mode == "operator_go_parse_report"):
            raise RuntimeError("selected authoritative follow report probe coverage is absent")
    runs = {record["binding"]["work"]["run_id"] for record in records}
    for name, boundary in (("before_commit_rollback", "before_commit"), ("after_commit_before_feedback", "after_commit")):
        if name in report["cases"]:
            validate_data_fault(report["cases"][name].get("data_fault", {}), boundary, runs)
    if scope == "smoke":
        tables = report["cases"]["initial_copy_dml"].get("comparison_tables")
        if (not isinstance(tables, list) or len(tables) != len(manifest["tables"])
                or any(not isinstance(table, dict) or table.get("status") != "match" for table in tables)):
            raise RuntimeError("follow smoke lacks a matching engine data comparison for every planned table")
    for name, value in report["cases"].items():
        if name in ("initial_copy_dml", "truncate", "full_duplicates_nulls", "target_type_oids", "repeated_restart") or name == diagnostic_case:
            if type(value.get("frontier")) is not int or value["frontier"] <= 0 or value.get("feedback_observed") is not True:
                raise RuntimeError("follow data case lacks independent applied/certified authority and feedback")
    for name, key, outcome in (("before_commit_rollback", "post_crash_rollback", "rolled_back"),
                               ("after_commit_before_feedback", "post_crash_preservation", "committed_preserved")):
        if name not in report["cases"]:
            continue
        case = report["cases"][name]
        preservation = case.get(key, {})
        if (not isinstance(preservation, dict) or case.get("fault_hits") != 1 or case.get("feedback_held") is not True
                or set(preservation) != {"sessions", "checkpoint_unchanged", "fingerprints_unchanged", "outcome"}
                or preservation.get("checkpoint_unchanged") is not True or preservation.get("fingerprints_unchanged") is not True
                or preservation.get("outcome") != outcome):
            raise RuntimeError("follow target crash case lacks its post-crash preservation evidence")
        validate_crash_sessions(preservation["sessions"])
        held = {key: case["data_fault"]["session"][key] for key in ("pid", "backend_start", "database_oid")}
        if held not in preservation["sessions"]:
            raise RuntimeError("post-crash preservation does not include the held DATA session")
    if scope == "full":
        cases = report["cases"]
        for name in ("before_commit_rollback", "after_commit_before_feedback", "partial_receive_transaction"):
            if cases[name].get("fault_hits") != 1 or cases[name].get("feedback_held") is not True:
                raise RuntimeError("follow crash case lacks its exact observed wire fault")
        for name in ("generated_refusal", "check_refusal", "domain_check_refusal", "expression_index_refusal", "partial_index_refusal"):
            if cases[name].get("source_and_target_unchanged") is not True:
                raise RuntimeError("follow preflight case lacks both no-mutation observations")
        for name in ("confirmed_flush_gap", "source_wal_loss"):
            if cases[name].get("target_unchanged") is not True:
                raise RuntimeError("follow recovery refusal lacks unchanged target evidence")
        for name in ("predata_handoff_refusal", "postdata_refusal"):
            if cases[name].get("uncertain_stage_refused") is not True:
                raise RuntimeError("follow stage refusal lacks an observed uncertain boundary")
        if (cases["repeated_restart"].get("restarts") != 3 or cases["oracle_mutation"].get("mutation_detected") is not True
                or cases["active_foreign_slot"].get("resources_unchanged") is not True
                or type(cases["active_foreign_slot"].get("active_pid")) is not int or cases["active_foreign_slot"]["active_pid"] <= 0
                or type(cases["copy_restart"].get("previous_generation")) is not int or cases["copy_restart"]["previous_generation"] <= 0
                or type(cases["copy_restart"].get("blocked_backend_pid")) is not int or cases["copy_restart"]["blocked_backend_pid"] <= 0
                or cases["backpressure"].get("feedback_held") is not True):
            raise RuntimeError("follow inventory lacks independently observed restart/oracle/ownership evidence")
        backpressure_event(json.dumps({"event": "follow.backpressure", "queue": cases["backpressure"].get("queue")}))
    if scope == "full":
        elapsed = report["cases"]["backpressure"].get("seconds")
        if (type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 60
                or report["cases"]["backpressure"].get("wal_sender_timeout_seconds") != 60):
            raise RuntimeError("backpressure did not outlast the actual 60 second server timeout")
    if report.get("power_loss_tested") is not False:
        raise RuntimeError("process kill must not claim power-loss qualification")


def process_snapshot(root_pid, *, proc_root=Path("/proc"), limit=128, deadline=None):
    deadline = time.monotonic() + 0.25 if deadline is None else deadline
    limit = max(0, min(limit, 128))
    records, unknown, overflow = {}, False, False
    try:
        for index, entry in enumerate(proc_root.iterdir()):
            if index >= 4096 or time.monotonic() >= deadline:
                overflow = True
                break
            if not entry.name.isdecimal():
                continue
            try:
                with (entry / "stat").open("rb") as stream:
                    data = stream.read(4097)
                if len(data) > 4096:
                    unknown = True
                    continue
                fields = data.rsplit(b") ", 1)[1].split()
                pid, parent = int(entry.name), int(fields[1])
                state = fields[0].decode("ascii")
                executable = (entry / "exe").readlink().name
                executable = executable if executable in {"rpgmigrate", "rpgmigrate-linux", "python3", "python3.11", "python3.12", "python3.13", "pg_dump", "pg_restore", "psql", "sh", "bash"} else "other"
                records[pid] = {"pid": pid, "ppid": parent, "executable": executable, "state": state if state in ("R", "S", "D", "Z", "T", "t", "X", "I") else "other"}
            except FileNotFoundError:
                continue
            except (OSError, ValueError, IndexError, UnicodeError):
                unknown = True
        owned = {root_pid}
        while True:
            children = {pid for pid, record in records.items() if record["ppid"] in owned}
            if children <= owned:
                break
            owned |= children
        selected = [records[pid] for pid in sorted(owned) if pid in records]
        return {"status": "observed" if root_pid in records else "unavailable", "overflow": overflow or len(selected) > limit,
                "unknown": unknown or any(record["executable"] == "other" or record["state"] == "other" for record in selected), "processes": selected[:limit]}
    except OSError:
        return {"status": "unavailable"}


def startup_capture(process, source, target, source_name, target_name, deadline):
    result = {"version": 1, "scope": "startup_diagnostic", "databases": {}, "proxies": {}}
    result["processes"] = process_snapshot(os.getpid(), deadline=min(deadline, time.monotonic() + 0.25))
    pid = getattr(process, "pid", None)
    result["engine_pid"] = pid if type(pid) is int and 0 < pid < 2**31 else None
    for side, proxy in (("source", getattr(process, "follow_wire", None)), ("target", getattr(process, "follow_target_wire", None))):
        result["proxies"][side] = proxy.snapshot(timeout=max(0, deadline - time.monotonic())) if proxy is not None else {"status": "absent"}
    for side, database, name in (("source", source, source_name), ("target", target, target_name)):
        previous = getattr(database, "_observation_deadline", None)
        database._observation_deadline = min(previous, deadline) if previous is not None else deadline
        try:
            result["databases"][side] = database.startup_snapshot(name) if time.monotonic() < deadline else {"status": "unavailable"}
        finally:
            if previous is None:
                del database._observation_deadline
            else:
                database._observation_deadline = previous
    return result


class Database(clone.Database):
    def sql(self, name, sql):
        deadline = getattr(self, "_observation_deadline", None)
        if deadline is None:
            return super().sql(name, sql)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("follow observation read budget expired")
        try:
            result = subprocess.run([str(self.bindir / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname", self.connection(name)],
                                    env=self.environment, input=(clone.SETTINGS + "SET statement_timeout='1000ms'; SET lock_timeout='1000ms';" + sql).encode(),
                                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=remaining)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeError("follow observation read unavailable") from None
        if result.returncode or len(result.stdout) > 65536:
            raise RuntimeError("follow observation read failed or exceeded its output bound")
        return result.stdout

    def _observe(self, read):
        previous = getattr(self, "_observation_deadline", None)
        self._observation_deadline = min(previous, time.monotonic() + 2) if previous is not None else time.monotonic() + 2
        try:
            return read()
        except Exception:
            return {"status": "unavailable"}
        finally:
            if previous is None:
                del self._observation_deadline
            else:
                self._observation_deadline = previous

    def observe_checkpoint(self, name, state, work):
        return self._observe(lambda: checkpoint_observation(self.authority(name, state, work)))

    def observe_slot(self, name, state):
        def read():
            row = self.slot(name, state)
            pid = row.get("active_pid")
            if type(row.get("active")) is not bool or (pid is not None and (type(pid) is not int or not 0 < pid < 2**31)):
                raise RuntimeError("follow slot observation backend identity is invalid")
            return {"status": "observed", "confirmed_flush_lsn": lsn(row.get("confirmed_flush_lsn")),
                    "restart_lsn": lsn(row["restart_lsn"]) if row.get("restart_lsn") is not None else None, "active_pid": pid}
        return self._observe(read)

    def startup_snapshot(self, name):
        def read():
            query = "SELECT json_build_object('database_oid',(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),'backends',coalesce((SELECT json_agg(b) FROM (SELECT pid,backend_start::text,backend_type,datid::bigint AS database_oid,state,wait_event_type,wait_event,application_name FROM pg_stat_activity WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND pid<>pg_backend_pid() ORDER BY pid LIMIT 65) b),'[]'),'locks',coalesce((SELECT json_agg(l) FROM (SELECT pid,database::bigint,classid::bigint,objid::bigint,objsubid,mode,granted FROM pg_locks WHERE locktype='advisory' AND classid=" + str(0x5250474d) + " AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) ORDER BY pid,objid LIMIT 129) l),'[]'));"
            raw = json.loads(self.sql(name, query))
            oid = raw["database_oid"]
            if type(oid) is not int or not 0 < oid < 2**32 or not isinstance(raw["backends"], list) or not isinstance(raw["locks"], list):
                raise RuntimeError("startup database identity unavailable")
            result = {"status": "observed", "database_oid": oid, "overflow": len(raw["backends"]) > 64 or len(raw["locks"]) > 128, "unknown": False, "backends": [], "locks": []}
            labels = {"backend_type": {"client backend", "walsender", "parallel worker", "autovacuum worker", "logical replication worker"},
                      "state": {"active", "idle", "idle in transaction", "idle in transaction (aborted)", "fastpath function call", "disabled"},
                      "wait_event_type": {"Activity", "BufferPin", "Client", "Extension", "IO", "IPC", "Lock", "LWLock", "Timeout"},
                      "wait_event": {"ClientRead", "ClientWrite", "WalSenderWaitForWAL", "WalSenderWriteData", "WalWrite", "WALWrite", "WALSync", "DataFileRead", "DataFileWrite", "DataFileSync", "BufferIO", "BufferMapping", "SyncRep", "advisory", "transactionid", "virtualxid", "relation"}}
            for backend in raw["backends"][:64]:
                validate_crash_sessions([{key: backend[key] for key in ("pid", "backend_start", "database_oid")}], oid)
                record = {key: backend[key] for key in ("pid", "backend_start", "database_oid")}
                for key, allowed in labels.items():
                    value = backend.get(key)
                    record[key] = value if value is None or isinstance(value, str) and value in allowed else "other"
                    result["unknown"] |= record[key] == "other"
                app = backend.get("application_name")
                record["application_name"] = "fixture" if app == self.application else app if app in ("pg_dump", "pg_restore", "psql", "rpgmigrate") else "other"
                result["unknown"] |= record["application_name"] == "other"
                result["backends"].append(record)
            for lock in raw["locks"][:128]:
                if (lock.get("database") != oid or lock.get("classid") != 0x5250474d
                        or any(type(lock.get(key)) is not int or not 0 <= lock[key] < 2**32 for key in ("pid", "database", "classid", "objid", "objsubid"))
                        or lock.get("mode") not in ("ExclusiveLock", "ShareLock") or type(lock.get("granted")) is not bool):
                    result["unknown"] = True
                    continue
                result["locks"].append({key: lock[key] for key in ("pid", "database", "classid", "objid", "objsubid", "mode", "granted")})
            return result
        return self._observe(read)

    def crash_baseline(self, name, state, work, manifest):
        checkpoint = self.authority(name, state, work)
        fingerprints = self.fingerprint(name, manifest)
        sessions = json.loads(self.sql(name, "SELECT coalesce(json_agg(json_build_object('pid',pid,'backend_start',backend_start::text,'database_oid',datid::bigint)),'[]') FROM pg_stat_activity WHERE datid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND pid<>pg_backend_pid();"))
        validate_crash_sessions(sessions, checkpoint["target_database"])
        return {"checkpoint": checkpoint, "fingerprints": fingerprints, "sessions": sessions}

    def held_session(self, name, fault, work):
        def read():
            pid, run = fault["backend_pid"], work["run_id"]
            if type(pid) is not int or not 0 < pid < 2**31 or re.fullmatch("[0-9a-f]{32}", run) is None:
                raise RuntimeError("held target identity unavailable")
            slot = zlib.crc32(bytes.fromhex(run))
            query = ("SELECT coalesce(json_agg(json_build_object('pid',a.pid,'backend_start',a.backend_start::text,'database_oid',a.datid::bigint,"
                     "'fence_class',l.classid::bigint,'fence_slot',l.objid::bigint)),'[]') FROM pg_stat_activity a JOIN pg_locks l ON l.pid=a.pid "
                     "WHERE a.pid=" + str(pid) + " AND a.datid=(SELECT oid FROM pg_database WHERE datname=current_database()) "
                     "AND l.locktype='advisory' AND l.database=a.datid AND l.classid=" + str(0x5250474d) + " AND l.objid=" + str(slot)
                     + " AND l.objsubid=2 AND l.mode='ExclusiveLock' AND l.granted;")
            sessions = json.loads(self.sql(name, query))
            if not isinstance(sessions, list) or len(sessions) != 1:
                raise RuntimeError("held target session lacks its exact owned fence")
            return sessions[0]
        return self._observe(read)

    def require_crash_preservation(self, name, state, work, manifest, baseline, *, outcome):
        if outcome not in ("rolled_back", "committed_preserved"):
            raise RuntimeError("unknown target crash preservation outcome")
        sessions = baseline["sessions"]
        validate_crash_sessions(sessions, baseline["checkpoint"]["target_database"])
        identities = json.dumps(sessions).replace("'", "''")
        query = "SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity a JOIN jsonb_to_recordset('" + identities + "'::jsonb) AS owned(pid integer,backend_start timestamptz,database_oid bigint) ON a.pid=owned.pid AND a.backend_start=owned.backend_start AND a.datid::bigint=owned.database_oid);"
        clone.wait_for(lambda: self._observe(lambda: self.sql(name, query).strip() == b't') is True,
                       "exact pre-crash target sessions closed", timeout=10)
        if self.authority(name, state, work) != baseline["checkpoint"]:
            raise RuntimeError("post-crash target checkpoint differs from the held baseline")
        clone.require_equal(baseline["fingerprints"], self.fingerprint(name, manifest))
        return {"sessions": sessions, "checkpoint_unchanged": True, "fingerprints_unchanged": True, "outcome": outcome}

    def connection(self, name):
        return super().connection(name) + " gssencmode=disable"

    def create(self, name, fixture=False):
        connection = super().create(name)
        if fixture:
            self.sql(name, "CREATE TYPE public.oid_skew AS ENUM ('fixture'); DROP TYPE public.oid_skew;")
            self.sql(name, (FIXTURES / "schema.sql").read_text())
            manifest = load_manifest()
            counts = {table: int(self.sql(name, f'SELECT count(*) FROM ONLY "M2 space".{clone.quote(table)};').strip()) for table in manifest["tables"]}
            clone.validate_fixture_rows(counts, manifest)
        return connection

    def slot(self, name, state):
        native = json.loads((state / "manifest.json").read_text())
        binding = native["follow_state"]["binding"]
        slot = binding["slot"]["intent"]["binding"]["slot_name"]
        literal = "'" + slot.replace("'", "''") + "'"
        rows = self.sql(name, f"SELECT row_to_json(s) FROM (SELECT slot_name,plugin,database,active,active_pid,confirmed_flush_lsn,restart_lsn,wal_status FROM pg_replication_slots WHERE slot_name={literal}) s;").splitlines()
        if len(rows) != 1:
            raise RuntimeError("exact owned follow slot is absent")
        row = json.loads(rows[0])
        if row["slot_name"] != slot or row["database"] != name or row["plugin"] != "pgoutput" or row["wal_status"] == "lost":
            raise RuntimeError("exact owned follow slot binding or WAL differs")
        return row

    def authority(self, name, state, work, *, candidate=False):
        path = state / "manifest.json"
        if not path.is_file():
            return None
        native = json.loads(path.read_text())
        records = native.get("target_provenance", [])
        binding = native.get("follow_state", {}).get("binding")
        if not records or not binding or records[0].get("phase") != "confirmed":
            return None
        if len(records) != 1 or type(native.get("format_version")) is not int or native["format_version"] != (9 if candidate else 8):
            raise RuntimeError("independent follow native identity/version differs")
        try:
            validate_identity_modes(native["clone_state"]["plan"])
            for owned in native["clone_state"]["owned_tables"]:
                validate_identity_modes({"tables": [owned["table"]]})
        except (AssertionError, KeyError, TypeError) as error:
            raise RuntimeError("independent follow native table identity differs") from error
        record = records[0]["record"]
        expected = record["intent"]["binding"]
        table = clone.quote(expected["schema_name"]) + "." + clone.quote(expected["table_name"])
        identity = self.sql(name, f"SELECT row_to_json(c) FROM (SELECT c.oid::bigint AS oid,c.relnamespace::bigint AS relnamespace,c.relowner::bigint AS relowner,c.relpersistence,c.relkind FROM pg_catalog.pg_class c WHERE c.oid={record['table_oid']} AND c.oid='{table}'::regclass) c;")
        observed = json.loads(identity) if identity.strip() else None
        if observed != {"oid": record["table_oid"], "relnamespace": record["schema_oid"], "relowner": expected["owner_oid"], "relpersistence": "p", "relkind": "r"}:
            raise RuntimeError("independent target exact owned relation identity differs")
        projection = ("t.magic,t.version,t.run_id,t.nonce,t.source_system,t.source_database::bigint AS source_database,"
                      "t.generation,t.target_system,t.target_database::bigint AS target_database,t.owner_oid::bigint AS owner_oid,"
                      "t.schema_oid::bigint AS schema_oid,t.table_oid::bigint AS table_oid,t.applied_order,t.last_xid::bigint AS last_xid,"
                      "t.begin_ordinal,t.source_commit_lsn,t.source_end_lsn,t.barrier_epoch,t.barrier_counter,t.copy_snapshot_token,"
                      "t.copy_consistent_point,t.copy_plan_nonce,t.clone_state,t.certified_lsn,t.follow_binding,"
                      "t.drain_end_lsn,t.drain_witness_lsn,t.drain_kind,t.follow_completed")
        rows = self.sql(name, f"SELECT row_to_json(t) FROM (SELECT {projection} FROM ONLY {table} t) t;").splitlines()
        if len(rows) != 1:
            raise RuntimeError("independent target authority cardinality differs")
        row = json.loads(rows[0])
        if len(row) != 29:
            raise RuntimeError("independent target authority column inventory differs")
        validate_drain_row(row)
        if "cleanup" not in native["follow_state"]:
            raise RuntimeError("independent native follow cleanup field is absent")
        cleanup = native["follow_state"]["cleanup"]
        if cleanup is not None and (not isinstance(cleanup, dict) or set(cleanup) != {"slot_removed", "publication_removed"}
                                    or any(type(value) is not bool for value in cleanup.values())):
            raise RuntimeError("independent native follow cleanup receipt differs")
        stream = expected["stream"]
        same = {"magic": "rpgmigrate", "version": 7 if candidate else 6, "run_id": "\\x" + bytes(stream["run"]).hex(),
                "nonce": "\\x" + bytes(record["intent"]["nonce"]).hex(), "source_system": stream["source"]["system_identifier"],
                "source_database": stream["source"]["database_oid"], "generation": stream["generation"],
                "target_system": expected["target_system_identifier"], "target_database": expected["target_database_oid"],
                "owner_oid": expected["owner_oid"], "schema_oid": record["schema_oid"], "table_oid": record["table_oid"]}
        if any(row.get(key) != value for key, value in same.items()):
            raise RuntimeError("independent target full stream/object binding differs")
        follow = json.loads(row["follow_binding"]) if row.get("follow_binding") else None
        if follow != {"binding": binding, "operator": {"migration_uid": work["migration_uid"], "run": list(bytes.fromhex(work["run_id"])), "workdir_id": work["workdir_id"], "checkpoint_generation": work["checkpoint_generation"]}, "origin_name": None}:
            raise RuntimeError("independent target follow binding differs")
        actual = json.loads(self.sql(name, "SELECT json_build_array((pg_control_system()).system_identifier,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()),(SELECT oid::bigint FROM pg_roles WHERE rolname=current_user),NOT pg_is_in_recovery(),current_setting('fsync'),current_setting('full_page_writes'));").decode())
        if (actual != [expected["target_system_identifier"], expected["target_database_oid"], expected["owner_oid"], True, "on", "on"]
                or type(row.get("barrier_counter")) is not int or not 0 < row["barrier_counter"] < 2**64
                or type(row.get("barrier_epoch")) is not int or not 0 <= row["barrier_epoch"] < 2**64):
            raise RuntimeError("independent target primary identity/durability policy is absent")
        owned_oids = [record["table_oid"], record["marker_table_oid"], record["marker_index_oid"]]
        kinds = ["r", "r", "i"]
        if candidate:
            if expected.get("metadata_version") != 7:
                raise RuntimeError("candidate metadata version differs")
            owned_oids += [record["large_object_table_oid"], record["large_object_index_oid"]]
            kinds += ["r", "i"]
        objects = self.sql(name, f"SELECT oid,relnamespace,relowner,relpersistence,relkind FROM pg_class WHERE oid IN ({','.join(map(str, owned_oids))}) ORDER BY oid;").decode().splitlines()
        expected_objects = sorted(f"{oid}|{record['schema_oid']}|{expected['owner_oid']}|p|{kind}" for oid, kind in zip(owned_oids, kinds))
        if sorted(objects) != expected_objects:
            raise RuntimeError("independent target owned checkpoint/marker identities differ")
        return row


class Driver(clone.Driver):
    def start(self, source, target, state, *arguments, gate=None):
        return super().start(source, target, state, "--follow", "--plugin", "pgoutput", *arguments, gate=gate)

    @staticmethod
    def capture_startup(process, phase, *, deadline=None):
        capture = getattr(process, "follow_startup_capture", None)
        if capture is None:
            return {"status": "unavailable"}
        deadline = min(time.monotonic() + 4, deadline) if deadline is not None else time.monotonic() + 4
        try:
            result = capture(deadline)
        except Exception:
            result = {"status": "unavailable"}
        snapshots = getattr(process, "follow_startup_snapshots", [])
        entry = {"phase": phase, "snapshot": result}
        try:
            entry["retained"] = clone.private_failure("follow-startup-snapshot", process.poll(), json.dumps(result).encode(),
                                                     getattr(process, "environment", {}), sys._getframe())
        except Exception:
            entry["retention_status"] = "unavailable"
        snapshots.append(entry)
        process.follow_startup_snapshots = snapshots[-2:]
        return result

    def watch(self, process, cleanup):
        def failed(_kind, error, trace):
            if error is None:
                return False
            if (isinstance(error, clone.SubprocessFailure)
                    and (error.metadata.get("command_kind") == "engine-follow-observation" or "follow_observation" in error.metadata)):
                return False
            process.follow_capture_deadline = time.monotonic() + 4
            self.capture_startup(process, "before_cleanup", deadline=process.follow_capture_deadline)
            wire = getattr(process, "follow_wire", None)
            wire_evidence = None
            if wire is not None:
                try:
                    wire_evidence = wire.evidence(timeout=0.02) if isinstance(wire, WireGate) else wire.evidence()
                except Exception:
                    wire_evidence = {"observer_failed": True}
            capture = getattr(process, "follow_observe", None)
            observation = None
            if capture is not None:
                try:
                    observation = capture()
                except Exception:
                    observation = {"scope": "visible-observation", "target": {"status": "unavailable"}, "slot": {"status": "unavailable"}}
            if isinstance(error, clone.SubprocessFailure) and observation is None and getattr(process, "follow_startup_capture", None) is None:
                return False
            status = process.poll()
            frames = []
            while trace is not None:
                frames.append({"file": Path(trace.tb_frame.f_code.co_filename).name,
                               "function": trace.tb_frame.f_code.co_name, "line": trace.tb_lineno})
                last = trace
                trace = trace.tb_next
            details = {"exception_type": type(error).__name__, "trace": frames[-16:], "engine_errors": [], "free_text_omitted": True}
            details["startup_snapshots"] = getattr(process, "follow_startup_snapshots", [])
            details["proxy_errors"] = {}
            for side, proxy in (("source", wire), ("target", getattr(process, "follow_target_wire", None))):
                if proxy is None:
                    continue
                try:
                    if not proxy.lock.acquire(timeout=0.02):
                        raise RuntimeError("proxy observation lock unavailable")
                    try:
                        error_count = len(proxy.errors)
                        errors = list(proxy.errors[:16])
                    finally:
                        proxy.lock.release()
                    allowed = {"ValueError", "RuntimeError", "TypeError", "AttributeError", "OverflowError", "KeyError", "IndexError", "OSError", "ConnectionError", "TimeoutError", "EOFError", "error"}
                    details["proxy_errors"][side] = {"count": error_count, "classes": [name if name in allowed else "OtherError" for name in errors], "truncated": error_count > 16}
                except Exception:
                    details["proxy_errors"][side] = {"status": "unavailable"}
            if observation is not None:
                details["follow_observation"] = observation
            previous = getattr(process, "follow_last_observation", None)
            if previous is not None:
                details["last_follow_observation"] = previous
            try:
                with process.error_output.open("rb") as stream:
                    size = stream.seek(0, os.SEEK_END)
                    stream.seek(max(0, size - 65536))
                    stderr = stream.read(65536)
                details.update(stderr_bytes=size, stderr_truncated=size > len(stderr), stderr_sha256=hashlib.sha256(stderr).hexdigest())
                categories = {"BadArgs": 1, "BadConfig": 2, "BadState": 3, "Pgsql": 4, "Pgctl": 5,
                              "Source": 6, "Target": 7, "Reload": 9, "Internal": 12, "Fatal": 122}
                for line in stderr.splitlines():
                    try:
                        event = json.loads(line)
                    except (ValueError, UnicodeDecodeError, RecursionError):
                        continue
                    if (not isinstance(event, dict) or event.get("level") != "error"
                            or not isinstance(event.get("category"), str) or event["category"] not in categories
                            or type(event.get("exit_code")) is not int
                            or categories[event["category"]] != event["exit_code"]):
                        continue
                    observed = {"category": event["category"], "exit_code": event["exit_code"]}
                    origin = event.get("error_origin")
                    if (isinstance(origin, dict) and set(origin) == {"file", "line", "column"}
                            and isinstance(origin["file"], str) and len(origin["file"]) <= 256
                            and re.fullmatch(r"(?:src/(?:[A-Za-z_][A-Za-z0-9_]*/)*)?[A-Za-z_][A-Za-z0-9_]*\.rs", origin["file"])
                            and all(type(origin[key]) is int and 0 < origin[key] < 2**32 for key in ("line", "column"))):
                        observed["error_origin"] = origin
                    message = event.get("message", "")
                    if isinstance(message, str):
                        sqlstate = re.search(r"\(SQLSTATE ([A-Z0-9]{5})\)", message)
                        if sqlstate:
                            observed["sqlstate"] = sqlstate[1]
                        stage = message.removeprefix("native:" + event["category"] + ": ").split(":", 1)[0]
                        if stage in ("follow", "follow-recovery", "follow-recovery-gap", "follow-gap", "follow-execution",
                                     "follow-preflight", "follow-retention", "follow-source", "follow-target", "pgoutput"):
                            observed["stage"] = stage
                    details["engine_errors"] = (details["engine_errors"] + [observed])[-16:]
            except OSError:
                details["stderr_unavailable"] = True
            if wire is not None:
                try:
                    evidence = wire_evidence
                    if evidence.get("observer_failed"):
                        raise RuntimeError("follow wire observer unavailable")
                    details["wire"] = bounded_wire_observation(evidence)
                except RuntimeError:
                    details["wire"] = {"observer_failed": True}
            initial = getattr(process, "follow_initial_observation", None)
            if initial is not None:
                details["initial_follow_observation"] = initial
            details["feedback_consistency"] = failure_feedback_consistency(details.get("wire"), observation, initial)
            metadata = clone.private_failure("engine-follow-observation", status, json.dumps(details).encode(),
                                             process.environment, last.tb_frame, last.tb_lineno)
            if isinstance(error, clone.SubprocessFailure):
                error.metadata["follow_observation"] = metadata
                return False
            raise clone.SubprocessFailure(metadata) from None
        cleanup.callback(self.kill, process)
        # Snapshot observations before killing the process or removing its temporary logs.
        cleanup.push(failed)

    def ready(self, process):
        deadline = time.monotonic() + 60
        sampled = False
        def observed():
            nonlocal sampled
            if time.monotonic() >= deadline:
                raise RuntimeError("positive follow startup deadline expired")
            if not sampled and time.monotonic() >= deadline - 15:
                sampled = True
                self.capture_startup(process, "startup_45s", deadline=deadline)
            if time.monotonic() >= deadline:
                raise RuntimeError("positive follow startup deadline expired")
            if process.poll() is not None:
                raise RuntimeError("follow exited before positive startup")
            lines = process.output.read_bytes().splitlines()
            framed = [line[len(b"native-report: "):] for line in lines if line.startswith(b"native-report: ")]
            if not framed:
                return False
            if len(framed) != 1:
                raise RuntimeError("follow startup report cardinality differs")
            raw = framed[0]
            binding = json.loads(process.binding.read_text())
            report = clone.validate_operator_report(raw, binding, "clone", False, phases=("streaming",))
            checked = False
            if self.report_probe is not None:
                raw_path = process.binding.with_suffix(".report")
                raw_path.write_bytes(raw)
                probe = json.loads(clone.run([str(self.report_probe), str(process.binding), str(raw_path)]))
                if probe.get("accepted") is not True:
                    raise RuntimeError("selected authoritative follow report probe refused startup")
                checked = True
            if time.monotonic() >= deadline:
                raise RuntimeError("positive follow startup deadline expired")
            self.reports.append({"binding": binding, "raw_json": raw.decode("utf-8"), "probe_checked": checked})
            return report
        report = clone.wait_for(observed, "positive follow startup", timeout=max(0, deadline - time.monotonic()))
        capture = getattr(process, "follow_observe", None)
        if capture is not None:
            try:
                initial = capture()
                wire = getattr(process, "follow_wire", None)
                if wire is not None:
                    streams = wire.evidence()["logical"]["streams"]
                    if streams:
                        initial.update(connection=streams[-1]["connection"], status_start=len(streams[-1]["statuses"]))
            except Exception:
                initial = {"scope": "visible-observation", "target": {"status": "unavailable"}, "slot": {"status": "unavailable"}}
            process.follow_initial_observation = initial
        return report

    def stop(self, process, *, crash=False):
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL if crash else signal.SIGTERM)
        try:
            status = process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            raise RuntimeError("follow controlled stop timed out") from None
        require_stopped(status)
        output = process.output.read_bytes() + process.error_output.read_bytes()
        if b'"phase":"completed"' in output or b'"phase":"drained"' in output:
            raise RuntimeError("M3 stop fabricated a final drain")
        return status


def inner(args):
    diagnostic_case = getattr(args, "diagnostic_case", None)
    cutover_mode = getattr(args, "cutover", False)
    if cutover_mode:
        import cutover
    manifest = cutover.load_manifest() if cutover_mode else load_manifest()
    bindir = Path("/usr/lib/postgresql") / args.pair.split("-")[1] / "bin"
    environment = {key: value for key, value in os.environ.items() if not key.startswith(("PG", "RPGMIGRATE_"))}
    environment.update(PGPASSWORD=os.environ["PGPASSWORD"], PG_BINDIR=str(bindir), PATH=str(bindir) + os.pathsep + os.environ["PATH"], LC_ALL="C.UTF-8")
    source, target = (Database(host, bindir, environment) for host in ("source", "target"))
    binary = Path(args.binary)
    linked_path = re.search(r"libpq\.so\.5 => (\S+)", clone.run(["ldd", str(binary)]).decode())
    if not linked_path:
        raise RuntimeError("actual executable libpq linkage is unavailable")
    receipt = {"pair": args.pair, "server_versions": [db.sql("postgres", "SHOW server_version;").decode().split()[0] for db in (source, target)],
               "client_versions": {name: clone.tool_version(bindir / name) for name in ("pg_dump", "pg_restore", "psql")},
               "linked_libpq": int(ctypes.CDLL(linked_path[1]).PQlibVersion()), "mode": "ongoing_follow",
               "successful_follow_exit": False, "power_loss_tested": False, "receipt_scope": "diagnostic" if diagnostic_case else "smoke" if args.smoke else "full", "cases": {}}
    if diagnostic_case:
        receipt["diagnostic_case"] = diagnostic_case
    clone.validate_receipt({**receipt, "cases": dict.fromkeys(manifest["cases"], {"observed": True})}, manifest, require_reports=False)
    with tempfile.TemporaryDirectory(prefix="rpgmigrate-m3-") as temporary, ExitStack() as cleanup:
        root = Path(temporary)
        driver = Driver(binary, environment, root, mode="operator", report_probe=args.report_probe)
        def case(name):
            srcname, dstname = "src_" + name, "dst_" + name
            return srcname, dstname, source.create(srcname, True), target.create(dstname), root / name
        def launch(src, dst, state, *arguments, gate=None, wire=None, source_name=None, target_name=None):
            source_name = srcname if source_name is None else source_name
            target_name = dstname if target_name is None else target_name
            if "--resume" in arguments:
                native = json.loads((state / "manifest.json").read_text())
                binding = native.get("follow_state", {}).get("binding")
                if binding:
                    name = binding["slot"]["intent"]["binding"]["slot_name"].replace("'", "''")
                    removed = native["follow_state"].get("cleanup")
                    query = (f"SELECT NOT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name='{name}');"
                             if isinstance(removed, dict) and removed.get("slot_removed") is True
                             else f"SELECT NOT active FROM pg_replication_slots WHERE slot_name='{name}';")
                    clone.wait_for(lambda: source.sql(source_name, query).strip() == b"t", "old owned source walsender closed or confirmed removed")
            process = driver.start(src, dst, state, *arguments, gate=gate)
            if wire is not None:
                process.follow_wire = wire
            def observe(srcname=source_name, dstname=target_name):
                deadline = getattr(process, "follow_capture_deadline", None)
                previous = {database: getattr(database, "_observation_deadline", None) for database in (source, target)}
                try:
                    if deadline is not None:
                        for database in (source, target):
                            database._observation_deadline = min(previous[database], deadline) if previous[database] is not None else deadline
                    return {"scope": "visible-observation", "slot": source.observe_slot(srcname, state),
                            "target": target.observe_checkpoint(dstname, state, driver.work[state])}
                finally:
                    for database, value in previous.items():
                        if value is None:
                            if hasattr(database, "_observation_deadline"):
                                del database._observation_deadline
                        else:
                            database._observation_deadline = value
            process.follow_observe = observe
            process.follow_startup_capture = lambda deadline, srcname=source_name, dstname=target_name: startup_capture(process, source, target, srcname, dstname, deadline)
            driver.watch(process, cleanup)
            return process
        def finish_receipt():
            receipt["report_validation"] = {"mode": "operator_go_parse_report" if args.report_probe is not None else "local_m3_contract", "reports": driver.reports}
            if args.report_probe is not None:
                receipt["report_validation"]["probe_sha256"] = hashlib.sha256(Path(args.report_probe).read_bytes()).hexdigest()
            validate_receipt(receipt, manifest, diagnostic_case=diagnostic_case)
            with Path(args.receipt).open("x" if diagnostic_case else "w") as stream:
                stream.write(json.dumps(receipt, indent=2, sort_keys=True) + "\n")

        def converge(srcname, dstname, state, wire, process):
            process.follow_wire = wire
            goal = lsn(source.sql(srcname, "SELECT pg_current_wal_insert_lsn();").decode().strip())
            def applied():
                if process.poll() is not None:
                    raise RuntimeError("follow exited before positive target/feedback predicate")
                row = target.authority(dstname, state, driver.work[state])
                if row:
                    process.follow_last_observation = {"scope": "visible-observation", "target": checkpoint_observation(row)}
                if row and require_feedback(wire.evidence(), row, goal):
                    slot = source.slot(srcname, state)
                    process.follow_last_observation["slot"] = {"status": "observed", "confirmed_flush_lsn": lsn(slot["confirmed_flush_lsn"])}
                    if slot["active"] is True and lsn(slot["confirmed_flush_lsn"]) >= goal:
                        return row
                return None
            row = clone.wait_for(applied, "applied/certified target authority and observed feedback", timeout=90)
            expected = source.fingerprint(srcname, manifest)
            clone.require_equal(expected, target.fingerprint(dstname, manifest))
            return {"observed": True, "frontier": authority_frontier(row), "feedback_observed": True, "fingerprints": expected}
        if cutover_mode:
            receipt["cases"] = cutover.run(args, source, target, manifest, case, launch, driver)
            receipt.update(mode="cutover", successful_follow_exit=True, receipt_scope="engine_producer", probe_checked=False,
                           independent_go_evidence="pending_qualified_m6_reader", source_tree=cutover.source_inventory(clone.ROOT))
            receipt["report_validation"] = {"mode": "local_m4_producer", "reports": driver.reports}
            cutover.validate_receipt(receipt, manifest)
            Path(args.receipt).write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
            print(json.dumps({"follow_e2e": "engine_producer_passed", "pair": args.pair, "cases": len(receipt["cases"])}, sort_keys=True))
            return
        if not diagnostic_case:
            srcname, dstname, src, dst, state = case("dml")
            check_workload(source, srcname, manifest)
            wire = cleanup.enter_context(WireGate("source", "observe"))
            process = launch(wire.connection(src), dst, state, wire=wire)
            driver.ready(process)
            source.sql(srcname, "BEGIN;\n" + INITIAL_DML + "\nCOMMIT;")
            receipt["cases"]["initial_copy_dml"] = {**converge(srcname, dstname, state, wire, process), "workload_preflight": "rolled_back"}
            if args.smoke:
                driver.stop(process)
                comparisons = driver.inspect(("compare", "data"), source.connection(srcname), target.connection(dstname), state, json_output=True)
                receipt["cases"]["initial_copy_dml"]["comparison_tables"] = comparisons
                finish_receipt()
                print(json.dumps({"follow_e2e": "smoke_passed", "pair": args.pair, "cases": 1}, sort_keys=True))
                return
            source.sql(srcname, 'UPDATE "M2 space".full_rows SET value=\'one duplicate\' WHERE ctid=(SELECT ctid FROM ONLY "M2 space".full_rows WHERE id=1 LIMIT 1); DELETE FROM ONLY "M2 space".full_rows WHERE ctid=(SELECT ctid FROM ONLY "M2 space".full_rows WHERE id IS NULL LIMIT 1);')
            receipt["cases"]["full_duplicates_nulls"] = converge(srcname, dstname, state, wire, process)
            source.sql(srcname, 'TRUNCATE ONLY "M2 space".truncated, ONLY "M2 space".empty, ONLY "M2 space".rows RESTART IDENTITY;')
            receipt["cases"]["truncate"] = converge(srcname, dstname, state, wire, process)
            custom = "SELECT oid FROM pg_type WHERE typnamespace='\"M2 space\"'::regnamespace AND typname IN ('mood','positive') ORDER BY typname;"
            if source.sql(srcname, custom) == target.sql(dstname, custom):
                raise RuntimeError("custom source/target OIDs accidentally match")
            receipt["cases"]["target_type_oids"] = converge(srcname, dstname, state, wire, process)
            for _ in range(3):
                driver.stop(process, crash=True)
                process = launch(wire.connection(src), dst, state, "--resume", "--not-consistent", wire=wire)
                driver.ready(process)
                source.sql(srcname, 'UPDATE "M2 space".wide SET changed=changed+1 WHERE id=1;')
                result = converge(srcname, dstname, state, wire, process)
            receipt["cases"]["repeated_restart"] = {**result, "restarts": 3}
            driver.stop(process)
            expected = source.fingerprint(srcname, manifest)
            target.sql(dstname, 'UPDATE "M2 space".wide SET changed=-1 WHERE id=1;')
            if expected == target.fingerprint(dstname, manifest):
                raise RuntimeError("independent follow oracle accepted changed target bytes")
            receipt["cases"]["oracle_mutation"] = {"observed": True, "mutation_detected": True}
        for name, mode, side in (("before_commit_rollback", "before_commit", "target"),
                                 ("after_commit_before_feedback", "after_commit", "target"),
                                 ("partial_receive_transaction", "after_begin", "source")):
            if diagnostic_case and name != diagnostic_case:
                continue
            srcname, dstname, src, dst, state = case(name)
            fault = cleanup.enter_context(WireGate(side, mode))
            observed = fault if side == "source" else cleanup.enter_context(WireGate("source", "observe"))
            process = launch(observed.connection(src), fault.connection(dst) if side == "target" else dst, state, wire=observed)
            if side == "target":
                process.follow_target_wire = fault
            driver.ready(process)
            source.sql(srcname, 'UPDATE "M2 space".wide SET changed=1 WHERE id=1;')
            baseline = converge(srcname, dstname, state, observed, process)
            previous = target.authority(dstname, state, driver.work[state])
            initial_wire = observed.evidence()
            first_status = len(initial_wire["statuses"])
            fault.arm()
            source.sql(srcname, '''BEGIN; UPDATE "M2 space".wide SET changed=2 WHERE id=1; INSERT INTO "M2 space".empty VALUES (1,'pending'); COMMIT;''')
            clone.wait_for(lambda: fault.held.is_set(), "exact wire fault " + name)
            data_fault = None
            if side == "target":
                held = fault.fault_observation()
                if held.get("boundary") != mode:
                    raise RuntimeError("held target COMMIT boundary differs")
                data_fault = require_data_fault(initial_wire, observed.evidence(), held, previous, driver.work[state],
                                                target.held_session(dstname, held, driver.work[state]))
            if mode == "after_commit":
                clone.wait_for(lambda: target.authority(dstname, state, driver.work[state])["applied_order"] > previous["applied_order"], "target committed checkpoint before withheld reply")
                committed = target.authority(dstname, state, driver.work[state])
                tx = data_fault["source"]
                if (lsn(committed["source_end_lsn"]), lsn(committed["source_commit_lsn"]), committed["begin_ordinal"], committed["last_xid"]) != (tx["end_lsn"], tx["commit_lsn"], tx["final_lsn"], tx["xid"]):
                    raise RuntimeError("held committed DATA differs from target canonical checkpoint")
            else:
                clone.require_equal(baseline["fingerprints"], target.fingerprint(dstname, manifest))
            clone.wait_for(lambda: len(observed.evidence()["statuses"]) > first_status, "fresh status while target/receive is held", timeout=20)
            pending = observed.evidence()["statuses"][first_status:]
            if (any(max(status) > authority_frontier(previous) for status in pending)
                    or lsn(source.slot(srcname, state)["confirmed_flush_lsn"]) > authority_frontier(previous)):
                raise RuntimeError("unresolved transaction advanced source feedback")
            held_baseline = target.crash_baseline(dstname, state, driver.work[state], manifest) if side == "target" else None
            if side == "target":
                expected_checkpoint = previous if mode == "before_commit" else committed
                if any(held_baseline["checkpoint"][key] != expected_checkpoint[key] for key in
                       ("applied_order", "last_xid", "begin_ordinal", "source_commit_lsn", "source_end_lsn")):
                    raise RuntimeError("held target checkpoint differs from its expected DATA outcome")
                expected_data = baseline["fingerprints"] if mode == "before_commit" else source.fingerprint(srcname, manifest)
                clone.require_equal(expected_data, held_baseline["fingerprints"])
                session = {key: data_fault["session"][key] for key in ("pid", "backend_start", "database_oid")}
                if session not in held_baseline["sessions"]:
                    raise RuntimeError("pre-crash session baseline omits held DATA backend")
            driver.stop(process, crash=True)
            if mode == "before_commit":
                fault.abort_hold()
            else:
                fault.release.set()
            preservation = target.require_crash_preservation(dstname, state, driver.work[state], manifest, held_baseline,
                                outcome="rolled_back" if mode == "before_commit" else "committed_preserved") if side == "target" else None
            process = launch(observed.connection(src), dst, state, "--resume", "--not-consistent", wire=observed)
            if side == "target":
                process.follow_target_wire = fault
            driver.ready(process)
            receipt["cases"][name] = {**converge(srcname, dstname, state, observed, process),
                                      "fault_hits": fault.evidence()["hits"], "feedback_held": True}
            if data_fault is not None:
                receipt["cases"][name]["data_fault"] = data_fault
            if side == "target":
                receipt["cases"][name]["post_crash_rollback" if mode == "before_commit" else "post_crash_preservation"] = preservation
            driver.stop(process)
        if diagnostic_case:
            finish_receipt()
            print(json.dumps({"follow_e2e": "diagnostic_passed", "pair": args.pair, "cases": [diagnostic_case]}, sort_keys=True))
            return
        for name, ddl, reason in (
                ("generated_refusal", '''ALTER TABLE "M2 space".rows ADD COLUMN computed text GENERATED ALWAYS AS (value || 'generated') STORED;''', "follow-preflight: generated columns are unsupported"),
                ("check_refusal", 'ALTER TABLE "M2 space".rows ADD CONSTRAINT checked CHECK (id>0);', "follow-execution: table/domain CHECK"),
                ("domain_check_refusal", 'ALTER DOMAIN "M2 space".positive ADD CONSTRAINT checked CHECK (VALUE>0);', "follow-execution: table/domain CHECK"),
                ("expression_index_refusal", 'CREATE INDEX expression_idx ON "M2 space".rows ((id+1));', "follow-execution: require plain indexes"),
                ("partial_index_refusal", 'CREATE INDEX partial_idx ON "M2 space".rows (id) WHERE id>0;', "follow-execution: require plain indexes")):
            srcname, dstname, src, dst, state = case(name)
            source.sql(srcname, ddl)
            before = target.catalog_signature(dstname)
            resources = source.sql(srcname, "SELECT slot_name,plugin,database,active FROM pg_replication_slots ORDER BY slot_name; SELECT pubname,oid FROM pg_publication ORDER BY pubname;")
            driver.finish(launch(src, dst, state), success=False, reason=reason)
            if before != target.catalog_signature(dstname) or resources != source.sql(srcname, "SELECT slot_name,plugin,database,active FROM pg_replication_slots ORDER BY slot_name; SELECT pubname,oid FROM pg_publication ORDER BY pubname;"):
                raise RuntimeError("unsupported follow profile mutated target or source resources")
            receipt["cases"][name] = {"observed": True, "source_and_target_unchanged": True}
        for mode, name in (("predata", "predata_handoff_refusal"), ("postdata", "postdata_refusal")):
            srcname, dstname, src, dst, state = case(name)
            gate = clone.ToolGate(root / (name + "-tools"), bindir, mode)
            process = launch(src, dst, state, gate=gate)
            gate.wait(process)
            driver.stop(process, crash=True)
            before = target.fingerprint(dstname, manifest)
            driver.finish(launch(src, dst, state, "--resume", "--not-consistent"), success=False, reason="follow-recovery:")
            clone.require_equal(before, target.fingerprint(dstname, manifest))
            receipt["cases"][name] = {"observed": True, "uncertain_stage_refused": True}
        srcname, dstname, src, dst, state = case("copy_restart")
        gate = clone.ToolGate(root / "copy-restart-tools", bindir, "predata")
        process = launch(src, dst, state, gate=gate)
        gate.wait(process)
        application = "m3_fixture_copy_lock"
        lock = subprocess.Popen([str(bindir / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname", dst + " application_name=" + application],
                                env=target.environment, stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, start_new_session=True)
        try:
            lock.stdin.write(b'BEGIN; LOCK TABLE ONLY "M2 space".wide IN SHARE MODE;\n')
            lock.stdin.flush()
            clone.wait_for(lambda: target.sql(dstname, "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.application_name='m3_fixture_copy_lock' AND l.mode='ShareLock' AND l.granted;").strip() == b"1", "owned target COPY lock")
            gate.release.touch()
            blocked = int(clone.wait_for(lambda: target.sql(dstname, clone.blocked_worker_sql(target.application)).strip(), "owned COPY worker blocked before truncate"))
            native = json.loads((state / "manifest.json").read_text())
            previous = native["clone_state"]["plan"]["generation"]
            owned_slot = native["follow_state"]["binding"]["slot"]["intent"]["binding"]["slot_name"]
            owned_publication = native["follow_state"]["binding"]["publication"]
            if native["clone_state"]["phase"] != "copying" or source.slot(srcname, state)["slot_name"] != owned_slot:
                raise RuntimeError("COPY restart lacks exact source resource receipt")
            driver.stop(process, crash=True)
        finally:
            os.killpg(lock.pid, signal.SIGTERM)
            lock.communicate(timeout=10)
        source.sql(srcname, 'UPDATE "M2 space".wide SET changed=9 WHERE id=1;')
        observed = cleanup.enter_context(WireGate("source", "observe"))
        process = launch(observed.connection(src), dst, state, "--resume", "--not-consistent", wire=observed)
        driver.ready(process)
        current = json.loads((state / "manifest.json").read_text())
        fresh_binding = current["follow_state"]["binding"]
        if current["clone_state"]["plan"]["generation"] != previous + 1 or fresh_binding["publication"]["oid"] == owned_publication["oid"]:
            raise RuntimeError("owned COPY restart did not replace exact resources and generation")
        receipt["cases"]["copy_restart"] = {**converge(srcname, dstname, state, observed, process), "previous_generation": previous, "blocked_backend_pid": blocked}
        driver.stop(process)
        srcname, dstname, src, dst, state = case("backpressure")
        fault = cleanup.enter_context(WireGate("target", "before_commit"))
        observed = cleanup.enter_context(WireGate("source", "observe"))
        process = launch(observed.connection(src), fault.connection(dst), state, wire=observed)
        driver.ready(process)
        source.sql(srcname, 'UPDATE "M2 space".wide SET changed=1 WHERE id=1;')
        converge(srcname, dstname, state, observed, process)
        previous = target.authority(dstname, state, driver.work[state])
        first_status = len(observed.evidence()["statuses"])
        fault.arm()
        source.sql(srcname, ''.join('UPDATE "M2 space".wide SET changed=changed+1 WHERE id=1;\n' for _ in range(3000)))
        clone.wait_for(lambda: fault.held.is_set(), "target COMMIT paused for real source backpressure")
        event = clone.wait_for(lambda: backpressure_event(process.error_output.read_bytes()), "actual full bounded follow FIFO")
        started = time.monotonic()
        sender = source.slot(srcname, state)["active_pid"]
        last_probe = started - 1
        def timeout_survived():
            nonlocal last_probe
            if process.poll() is not None:
                raise RuntimeError("follow died while real walsender timeout was exceeded")
            statuses = observed.evidence()["statuses"][first_status:]
            if any(max(status) > authority_frontier(previous) for status in statuses):
                raise RuntimeError("full FIFO advanced unproved feedback")
            if time.monotonic() - last_probe >= 1:
                current = source.slot(srcname, state)
                last_probe = time.monotonic()
                if current["active_pid"] != sender or lsn(current["confirmed_flush_lsn"]) > authority_frontier(previous):
                    raise RuntimeError("walsender was replaced or confirmed unproved work")
            return time.monotonic() - started > 65 and len(statuses) >= 6
        clone.wait_for(timeout_survived, "same actual walsender survives full FIFO beyond 60 second timeout", timeout=80)
        seconds = time.monotonic() - started
        fault.release.set()
        receipt["cases"]["backpressure"] = {**converge(srcname, dstname, state, observed, process),
                                              "queue": event["queue"], "seconds": seconds,
                                              "wal_sender_timeout_seconds": 60, "same_walsender_pid": sender,
                                              "feedback_held": True}
        driver.stop(process)
        for name in ("confirmed_flush_gap", "source_wal_loss"):
            srcname, dstname, src, dst, state = case(name)
            observed = cleanup.enter_context(WireGate("source", "observe"))
            process = launch(observed.connection(src), dst, state, wire=observed)
            driver.ready(process)
            source.sql(srcname, 'UPDATE "M2 space".wide SET changed=1 WHERE id=1;')
            converge(srcname, dstname, state, observed, process)
            driver.stop(process, crash=True)
            clone.wait_for(lambda: source.slot(srcname, state)["active"] is False, "owned source slot becomes inactive")
            previous = target.authority(dstname, state, driver.work[state])
            slot_name = source.slot(srcname, state)["slot_name"]
            slot_literal = "'" + slot_name.replace("'", "''") + "'"
            before = target.fingerprint(dstname, manifest)
            source.sql(srcname, 'UPDATE "M2 space".wide SET changed=2 WHERE id=1;')
            if name == "confirmed_flush_gap":
                source.sql(srcname, f"SELECT pg_replication_slot_advance({slot_literal},pg_current_wal_insert_lsn());")
                if lsn(source.slot(srcname, state)["confirmed_flush_lsn"]) <= max(authority_frontier(previous), lsn(previous["copy_consistent_point"])):
                    raise RuntimeError("owned source slot did not advance beyond target authority")
                reason = "follow-recovery-gap: source confirmed flush is ahead of durable target authority"
            else:
                # The server is an owned disposable container, never an operator endpoint.
                source.sql("postgres", "ALTER SYSTEM SET max_slot_wal_keep_size='0';")
                source.sql("postgres", "SELECT pg_reload_conf();")
                try:
                    clone.wait_for(lambda: source.sql("postgres", "SHOW max_slot_wal_keep_size;").strip() == b"0", "owned WAL-loss policy applied")
                    for _ in range(4):
                        source.sql(srcname, "SELECT pg_logical_emit_message(false,'follow_fixture',repeat('x',1048576)) FROM generate_series(1,20); SELECT pg_switch_wal(); CHECKPOINT;")
                        status = source.sql(srcname, f"SELECT wal_status FROM pg_replication_slots WHERE slot_name={slot_literal};").strip()
                        if status == b"lost":
                            break
                    else:
                        raise RuntimeError("owned source WAL loss was not observed")
                finally:
                    source.sql("postgres", "ALTER SYSTEM RESET max_slot_wal_keep_size;")
                    source.sql("postgres", "SELECT pg_reload_conf();")
                    clone.wait_for(lambda: source.sql("postgres", "SHOW max_slot_wal_keep_size;").strip() == b"-1", "owned source retention policy restored before lost-slot refusal")
                reason = "follow-gap: source slot is missing, foreign, invalidated, or has unsafe WAL retention"
            driver.finish(launch(src, dst, state, "--resume", "--not-consistent"), success=False, reason=reason)
            clone.require_equal(before, target.fingerprint(dstname, manifest))
            if target.authority(dstname, state, driver.work[state])["applied_order"] != previous["applied_order"]:
                raise RuntimeError("refused recovery changed target applied checkpoint")
            receipt["cases"][name] = {"observed": True, "target_unchanged": True}
        srcname, dstname, src, dst, state = case("active_foreign_slot")
        slot_name, publication = "fixture_foreign_slot", "fixture_foreign_publication"
        source.sql(srcname, f"CREATE PUBLICATION {publication} FOR ALL TABLES; SELECT pg_create_logical_replication_slot('{slot_name}','pgoutput');")
        receiver = subprocess.Popen([str(bindir / "pg_recvlogical"), "--dbname", src, "--slot", slot_name, "--start", "--no-loop", "--no-password", "--file", "/dev/null", "--option", "proto_version=1", "--option", "publication_names=" + publication],
                                    env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        cleanup.callback(driver.kill, receiver)
        try:
            pid = int(clone.wait_for(lambda: source.sql(srcname, f"SELECT active_pid FROM pg_replication_slots WHERE slot_name='{slot_name}' AND active;").strip(), "active independently owned foreign slot"))
            before = target.catalog_signature(dstname)
            resources = source.sql(srcname, f"SELECT slot_name,plugin,database,active_pid FROM pg_replication_slots WHERE slot_name='{slot_name}'; SELECT oid,pubname FROM pg_publication WHERE pubname='{publication}';")
            driver.finish(launch(src, dst, state, "--slot-name", slot_name, "--publication", publication), success=False, reason="follow-source: existing slot or publication is never adopted")
            if before != target.catalog_signature(dstname) or resources != source.sql(srcname, f"SELECT slot_name,plugin,database,active_pid FROM pg_replication_slots WHERE slot_name='{slot_name}'; SELECT oid,pubname FROM pg_publication WHERE pubname='{publication}';"):
                raise RuntimeError("foreign active source resource was mutated or adopted")
            receipt["cases"]["active_foreign_slot"] = {"observed": True, "active_pid": pid, "resources_unchanged": True}
        finally:
            driver.kill(receiver)
        # Every declared fault must have a real runtime receipt before qualification can pass.
        missing = sorted(set(CASES) - set(receipt["cases"]))
        if missing:
            raise RuntimeError("follow fault execution is incomplete: " + ", ".join(missing))
        finish_receipt()
        print(json.dumps({"follow_e2e": "passed", "pair": args.pair, "cases": len(receipt["cases"])}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair", required=True, choices=clone.PAIRS)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--cutover", action="store_true", help="check M4 engine producer cases; independent M6/Go evidence remains required")
    selection.add_argument("--smoke", action="store_true", help="run only the separately scoped initial copy and DML smoke case")
    selection.add_argument("--diagnostic-case", choices=("before_commit_rollback",), help="run one separately scoped fault diagnostic; requires a new explicit receipt path")
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--binary", default="/build/debug/rpgmigrate")
    parser.add_argument("--receipt")
    parser.add_argument("--report-probe", default=os.environ.get("RPGMIGRATE_E2E_REPORT_PROBE"))
    args = parser.parse_args()
    if args.cutover and args.report_probe is not None:
        parser.error("--cutover requires the qualified independent M6 reader; omit the clone-only --report-probe")
    if args.diagnostic_case and not args.receipt:
        parser.error("--diagnostic-case requires --receipt")
    if sys.flags.optimize:
        raise RuntimeError("optimized Python cannot run qualification")
    if args.inside:
        clone.DIAGNOSTIC_ROOT = Path("/diagnostics")
        if not args.receipt:
            raise RuntimeError("inside qualification requires a receipt destination")
        inner(args)
    else:
        if args.cutover:
            import cutover
            manifest = cutover.load_manifest()
            validator = lambda report, manifest: cutover.validate_receipt(report, manifest, require_images=True)
        else:
            manifest = load_manifest()
            validator = lambda report, manifest: validate_receipt(report, manifest, diagnostic_case=args.diagnostic_case)
        clone.outer(args, program="follow", manifest=manifest,
                    receipt_validator=validator,
                    server_arguments=("-c", "wal_level=logical", "-c", "max_replication_slots=32", "-c", "max_wal_senders=32", "-c", "wal_sender_timeout=10s"))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        if isinstance(error, clone.SubprocessFailure):
            metadata = error.metadata
        else:
            trace = error.__traceback__
            while trace.tb_next is not None:
                trace = trace.tb_next
            metadata = clone.private_failure("python-exception", None, str(error).encode(), None, trace.tb_frame, trace.tb_lineno)
        print("follow-e2e: " + json.dumps(metadata, sort_keys=True), file=sys.stderr)
        sys.exit(1)
