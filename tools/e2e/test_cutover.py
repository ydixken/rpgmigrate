"""Portable controls for the actual cutover command and completion checks."""

import copy
import hashlib
import io
import re
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import patch
import unittest
import zlib

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clone
import cutover
from test_clone import ModelRuntime


WORK = {"migration_uid": "12345678-9abc-def0-1234-56789abcdef0", "run_id": "123456789abcdef0123456789abcdef0",
        "workdir_id": "owned", "checkpoint_generation": 1}


def report(command="clone", phase="completed"):
    value = {"protocol_version": 1, "engine_version": "0.1.0", "command": command,
             "capabilities": ["clone.single_db"], "work": WORK, "phase": phase,
             "copy": {name: {"total": 1, "done": 1} for name in ("tables", "indexes", "bytes")}}
    return value


def framed(value):
    return b"native-report: " + json.dumps(value).encode() + b"\n"


def replication():
    return {"protocol_version": 1, "engine_version": "0.1.0", "command": "stream sentinel set endpos",
            "capabilities": ["cutover.v1"], "work": WORK, "phase": "completed",
            "replication": {"certified_lsn": "0/30", "durable_replay_lsn": "0/30", "end_lsn": "0/20",
                            "end_persisted": True, "drained": True, "checkpoint_id": "1:0:3",
                            "drain_proof": {"work": WORK, "checkpoint_id": "1:0:3", "committed": True,
                                            "end_lsn": "0/20", "replay_lsn": "0/30", "kind": "keepalive"}}}


def producer_receipt():
    manifest = cutover.load_manifest()
    fingerprints = {name: {"rows": count, "sha256": "a" * 64} for name, count in manifest["row_counts"].items()}
    fingerprints.update(schema="b" * 64, sequences="1032|t")
    fingerprints["rows"]["rows"] = 132
    raw = replication()
    row = {"generation": 1, "barrier_epoch": 0, "barrier_counter": 3, "applied_order": 1, "last_xid": 3,
           "begin_ordinal": 16, "source_commit_lsn": "0/10", "source_end_lsn": "0/10", "certified_lsn": "0/30",
           "copy_consistent_point": "0/8", "drain_end_lsn": "0/20", "drain_witness_lsn": "0/30",
           "drain_kind": "keepalive", "follow_completed": True}
    wire = {"hits": 0, "logical": {"overflow": False, "streams": [{"confirmed": True, "invalid": False, "overflow": False,
            "transactions": [], "keepalives": [{"lsn": 48, "idle": True, "forwarded": True}], "statuses": [[48, 48, 48]]}]}}
    records = [{"binding": {"command": "stream sentinel set endpos", "work": WORK}, "raw_json": json.dumps(raw),
                "probe_checked": False, "independent_go_evidence": "pending_qualified_m6_reader"} for _ in range(2)]
    common = {"observed": True, "end_lsn": "0/20", "worker_completed": True, "identical_retries": 2,
              "reports": records, "caller_goal": {"end_lsn": "0/20", "work": WORK}, "target_row": row, "source_slot": {"confirmed_flush_lsn": "0/10", "restart_lsn": "0/8"},
              "wire": wire, "source_fingerprints": fingerprints, "target_fingerprints": copy.deepcopy(fingerprints)}
    cases = {name: copy.deepcopy(common) for name in cutover.CASES}
    cases["local_endpos_survives_active_fence_json_refusal"] = {
        "observed": True, "end_lsn": "0/20", "worker_active_during_refusal": True, "fault_hits": 1,
        "error_category": "BadState", "exit_code": 3, "success_stdout_empty": True,
        "local_request_retained": True, "control_lock_released": True, "caller_goal": {"end_lsn": "0/20", "work": WORK}}
    cases["async_visible_row_cannot_emit_replication_json"] = {
        "observed": True, "scope": "active_fence_refusal_around_visible_async_same_value_write", "synchronous_commit": "off",
        "new_xmin_observed": True, "token_unchanged": True, "fence_held_through_command": True,
        "error_category": "BadState", "exit_code": 3, "success_stdout_empty": True,
        "positive_replay_after_release": "0/30", "power_loss_tested": False, "unflushed_wal_proven": False}
    session = {"pid": 123, "backend_start": "2026-11-08 12:00:00+00", "database_oid": 42}
    for name, setting in (("loaded_cutover_durable_source", "on"), ("loaded_cutover_async_source", "off")):
        cases[name].update(source_write_freeze=True, outstanding_transactions=0, oracle_scope="post_freeze_complete_source",
                           writer={"sessions": [session], "synchronous_commit": setting, "transactions": 32, "rows_inserted": 32,
                                   "rows_before": 100, "rows_after": 132, "writer_reaped": True, "writer_sessions_closed": True})
    cases["endpos_sampled_before_certification_retains_exact_request"].update(
        scope="actual_sampled_lsn_certification_race", sampled_end_lsn="0/20", certified_before_request="0/30",
        unrelated_wal_lsn="0/40", applied_anchor_floor=16)
    later = cases["later_begin_coverage_preserves_pre_later_oracle"]
    later.update(scope="algorithm_fault_scope_later_begin", oracle_scope="saved_pre_later_source", dropped_keepalives=1,
                 received_keepalives=1, forwarded_keepalives=0, begin_received=1, begin_forwarded=1)
    later["reports"] = copy.deepcopy(records)
    for record in later["reports"]:
        value = json.loads(record["raw_json"])
        value["replication"].update(certified_lsn="0/0", durable_replay_lsn="0/10")
        value["replication"]["drain_proof"]["kind"] = "later_begin"
        record["raw_json"] = json.dumps(value)
    later["target_row"].update(certified_lsn=None, drain_kind="later_begin")
    later["baseline_target_row"] = {"source_end_lsn": "0/10", "certified_lsn": None, "copy_consistent_point": "0/8"}
    later["later_source_fingerprints"] = copy.deepcopy(fingerprints)
    later["later_source_fingerprints"]["rows"]["rows"] += 1
    later["wire"] = {"hits": 1, "dropped_keepalives": 1, "logical": {"overflow": False, "streams": [
        {"confirmed": True, "invalid": False, "overflow": False,
         "transactions": [{"final_lsn": 48, "begin_forwarded": True, "dml_count": 1}],
         "keepalives": [{"lsn": 32, "idle": True, "forwarded": False}], "statuses": [[16, 16, 16]]}]}}
    cross = cases["crossing_transaction_remains_whole_across_cutover_and_resume"]
    cross.update(scope="algorithm_fault_scope_crossing_commit_record", completed_resumes=2, resume_row_unchanged=True,
                 transaction={"connection": 1, "final_lsn": 32, "commit_lsn": 32, "end_lsn": 40, "xid": 3, "dml_count": 2, "complete": True,
                              "begin_forwarded": True, "commit_forwarded": True})
    fault = {"status": "observed", "scope": "wire_fault_observation", "boundary": "before_commit", "protocol": "extended",
             "connection": 1, "backend_pid": 123, "begin_observed": True, "overflow": False, "classification_invalid": False,
             "command_tags": {"insert_0_1": 1, "update_1": 2, "delete_1": 0, "other_dml": 0}}
    session = {"pid": 123, "backend_start": "2026-11-08 12:00:00+00", "database_oid": 42,
               "fence_class": 0x5250474d, "fence_slot": zlib.crc32(bytes.fromhex(WORK["run_id"]))}
    cross["data_fault"] = {"source": {**cross["transaction"], "baseline_end": 16}, "fault": fault, "session": session, "run_id": WORK["run_id"]}
    cases["local_endpos_survives_active_fence_json_refusal"]["data_fault"] = copy.deepcopy(cross["data_fault"])
    cross["target_row"].update(source_commit_lsn="0/20", source_end_lsn="0/28", begin_ordinal=32)
    cross["wire"]["logical"]["streams"][0]["transactions"] = [copy.deepcopy(cross["transaction"])]
    obligations = [{"kind": "replication_slot", "name": "owned_slot", "owned": True, "removed": True},
                   {"kind": "publication", "name": "owned_pub", "owned": True, "removed": True},
                   {"kind": "workdir", "name": "/owned/state", "owned": True, "removed": False},
                   {"kind": "replication_origin", "name": "foreign_origin", "owned": False, "removed": False}]
    clean = {"protocol_version": 1, "engine_version": "0.1.0", "command": "stream cleanup", "work": WORK,
             "capabilities": ["cleanup.v1"], "phase": "completed", "cleanup": {"complete": True, "resources": obligations}}
    cases["cleanup_retains_foreign_resources_and_comparison_state"].update(
        obligations=obligations, cleanup_reports=[{"binding": {"command": "stream cleanup", "work": WORK}, "raw_json": json.dumps(clean)} for _ in range(2)],
        retained_workdir=True, metadata_retained=True, comparisons_before_and_after=True, owned_resources_absent=True,
        foreign_inventory_before_sha256="d" * 64, foreign_inventory_after_sha256="d" * 64,
        completed_resumes=2, resume_row_unchanged=True, foreign_resource_count=3)
    columns = re.findall(r'\("([a-z_]+)", "[a-z0-9_]+", \d+, [A-Z0-9_-]+, (?:true|false)\)',
                         (clone.ROOT / "src/apply/metadata.rs").read_text().split("];", 1)[0])
    for index in (1, 2, *range(4, 9)):
        value = cases[cutover.CASES[index]]
        full = {**dict.fromkeys(columns), **value["target_row"], "version": 6}
        value["producer_authority"] = {"receipt_scope": "engine_producer", "independent_go_evidence": "pending_qualified_m6_reader",
            "native_format": 8, "metadata_version": 6, "work": WORK, "checkpoint_row": full,
            "target_provenance": {"table_oid": 7, "marker_table_oid": 8, "marker_index_oid": 9}, "follow_state": {"binding": {}},
            "catalog": {"columns": [{"attname": name} for name in columns], "objects": [{"oid": oid} for oid in (7, 8, 9)],
                        "constraints": [{"contype": "p"}], "indexes": [{"indisprimary": True}]}}
    clean = cases["cleanup_retains_foreign_resources_and_comparison_state"]
    foreign = {"slot": {"slot_name": "m4_foreign_slot"}, "publication": {"oid": 10}, "origin": {"roident": 11}}
    foreign_hash = hashlib.sha256(json.dumps(foreign, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    clean.update(foreign_inventory_before=foreign, foreign_inventory_after=copy.deepcopy(foreign),
                 foreign_inventory_before_sha256=foreign_hash, foreign_inventory_after_sha256=foreign_hash)
    full = clean["producer_authority"]["checkpoint_row"]
    full["target_database"] = 42
    full["target_system"] = 100
    full["run_id"] = "\\x" + WORK["run_id"]
    fault = {"status": "observed", "scope": "completed_resume_close_fault", "connection": 1, "backend_pid": 123,
             "commit_ordinal": 2, "protocol": "extended", "begin_observed": True, "terminal_idle": True,
             "upstream_disconnected": True, "terminal_delivered": True, "next_frontend_kind": "P",
             "old_drop_terminate": False, "overflow": False, "classification_invalid": False}
    held = {**fault, "terminal_delivered": False, "next_frontend_kind": None, "old_drop_terminate": None}
    snapshot = {"authority": copy.deepcopy(full), "native_manifest_sha256": "a" * 64, "local_endpos_sha256": "b" * 64,
                "source_resources": [None, None], "foreign_inventory": foreign,
                "source_fingerprints": copy.deepcopy(fingerprints), "target_fingerprints": copy.deepcopy(fingerprints)}
    error = json.dumps({"level": "error", "category": "Pgsql", "exit_code": 4}).encode()
    cases["completed_resume_requires_checked_authority_close"] = {
        "observed": True, "scope": "completed_resume_close_fault", "work": WORK, "caller_goal": clean["caller_goal"],
        "before": copy.deepcopy(snapshot), "after": copy.deepcopy(snapshot), "recovered": copy.deepcopy(snapshot),
        "fault_held": held, "fault": fault, "fault_hits": 1,
        "target_wire": {"status": "observed", "overflow": False,
                        "connections": [{"connection": 1, "backend_pid": 123, "unknown": False, "overflow": False}]},
        "backend_absence": {"connection": 1, "backend_pid": 123, "database_oid": 42, "system_identifier": 100, "absent": True},
        "worker_result": cutover.command_observation(subprocess.CompletedProcess([], 4, b"", error)),
        "worker_reaped": True, "relay_workers_reaped": True,
        "fault_free_resume": {"binding": {"command": "clone", "work": WORK}, "raw_json": json.dumps(report()),
                              "probe_checked": False, "independent_go_evidence": "pending_qualified_m6_reader"}}
    return {"pair": "16-18", "server_versions": ["16.15", "18.6"], "client_versions": dict.fromkeys(("pg_dump", "pg_restore", "psql"), "18.6"),
            "linked_libpq": 180006, "mode": "cutover", "successful_follow_exit": True, "power_loss_tested": False,
            "receipt_scope": "engine_producer", "probe_checked": False, "independent_go_evidence": "pending_qualified_m6_reader", "cases": cases,
            "source_tree": {"file_count": 320, "sha256": "e" * 64},
            "report_validation": {"mode": "local_m4_producer", "reports": [
                {"binding": {"command": "clone", "work": WORK}, "raw_json": json.dumps(report(phase=phase)), "probe_checked": False}
                for phase in ("streaming", "completed")]}}


M9_REPORTS = clone.ROOT / "tests/fixtures/native/v1/payloads"
M9_FAILURES = ("m9_final_sequence_sync_failure", "m9_final_large_object_sync_failure")


def m9_frame(mode, command):
    raw = (M9_REPORTS / f"m9-{mode}-{command}.json").read_bytes()
    return b"native-report: " + raw.rstrip(b"\n") + b"\n"


def m9_wire(value, prefix, *, ordinal=4):
    wire = value["wire"]
    positions = wire["logical"]["streams"][0]["statuses"]
    proof = {"ordinal": ordinal, "connection": 2, "backend_pid": 123, "prefix": prefix}
    commit = {**proof, "mode": "sync", "writing": True, "warning_free": True}
    wire.update(statuses=positions, ack_oracle={"profile": "m9", "bound": True, "invalid": False, "overflow": False,
        "ordinal": ordinal + len(positions), "proofs": [proof], "commits": [commit],
        "statuses": [{"ordinal": ordinal + index, "connection": 1, "positions": item, "prefix": prefix,
                      "proof_ordinal": ordinal, "safe": True, "forwarded": True} for index, item in enumerate(positions, 1)]})
    return wire


def m9_producer_authority(value, settings):
    handoff = value["producer_authority"]
    row = handoff["checkpoint_row"]
    row.update(version=7, source_system=101, source_database=41, target_system=100, target_database=42,
               owner_oid=10, schema_oid=6, table_oid=7, run_id="\\x" + WORK["run_id"])
    handoff.update(native_format=10, metadata_version=7)
    handoff["follow_state"]["effective_settings"] = settings
    provenance = handoff["target_provenance"]
    provenance.update(large_object_table_oid=10, large_object_index_oid=11, schema_oid=6,
                      intent={"binding": {"metadata_version": 7, "table_name": "checkpoint", "owner_oid": 10}})
    handoff["catalog"]["objects"] = [
        {"oid": oid, "relnamespace": 6, "relowner": 10, "relkind": kind, "relname": name, "relpersistence": "p",
         "index_parent": {9: 8, 11: 10}.get(oid)}
        for oid, kind, name in ((7, "r", "checkpoint"), (8, "r", "copy_done"), (9, "i", "copy_done_pkey"),
                                (10, "r", "large_object_done"), (11, "i", "large_object_done_pkey"))]
    return handoff


def m9_failure_receipt(name):
    legacy = producer_receipt()["cases"]
    value = copy.deepcopy(legacy[cutover.CASES[7]])
    settings = json.loads(m9_frame("durable", "clone-streaming")[15:])["settings"]
    artifact = {"input_sha256": "a" * 64, "input": {"source_commit": "b" * 40, "source_git_tree": "c" * 40}}
    row = value["target_row"]
    row.update(drain_end_lsn="0/30", drain_witness_lsn="0/40", certified_lsn="0/40", follow_completed=False)
    baseline = {**row, "barrier_counter": 2, "drain_end_lsn": None, "drain_witness_lsn": None, "drain_kind": None}
    handoff = m9_producer_authority(value, settings)
    handoff["checkpoint_row"].update(row)
    handoff["checkpoint_row"]["clone_state"] = json.dumps({"large_objects": {
        "generation": 1, "initial": {"objects": 0, "bytes": 0}, "final_sync": None}})
    value["wire"]["logical"]["streams"][0]["keepalives"] = [{"lsn": 64, "idle": True, "forwarded": True}]
    wire = m9_wire(value, 64, ordinal=9)
    raw = json.loads(m9_frame("durable", "clone-streaming")[15:])
    raw["work"] = WORK
    output = framed(raw)
    sequence = name == M9_FAILURES[0]
    code = 4 if sequence else 7
    message = "fixture failure (SQLSTATE 22003)" if sequence else "target-large-object: unrecorded target object identity refuses admission"
    error = json.dumps({"level": "error", "category": "Pgsql" if sequence else "Target", "exit_code": code, "message": message}).encode()
    value["data_fault"]["fault"]["connection"] = 2
    result = {
        "observed": True, "scope": name, "caller_goal": {"end_lsn": "0/30", "work": WORK},
        "local_endpos": {"end_lsn": 48, "sealed": True}, "before_drain": baseline,
        "producer_authority": handoff, "wire": wire, "data_fault": value["data_fault"],
        "drain_start_ordinal": 8, "worker_reaped": True,
        "worker_result": cutover.command_observation(subprocess.CompletedProcess([], code, output, error)),
        "worker_reports": [{"binding": {"command": "clone", "work": WORK}, "raw_json": output[15:-1].decode(), "probe_checked": False}],
        "source_slot": {"confirmed_flush_lsn": "0/28", "restart_lsn": "0/8"},
    }
    if sequence:
        result["sequence_identity"] = {"source": {"oid": 50, "namespace_oid": 51, "owner_oid": 10},
                                       "target": {"oid": 52, "namespace_oid": 53, "owner_oid": 10}}
        binding = {"case": name, "source_commit": "b" * 40, "source_tree": "c" * 40,
                   "artifact_inputs_sha256": "a" * 64, "work_identity": WORK,
                   "source_identity": {"system_identifier": 101, "database_oid": 41},
                   "target_identity": {"system_identifier": 100, "database_oid": 42},
                   "sequence_identity": result["sequence_identity"],
                   "target_sequence_facts": {"maximum": 1000, "current": 1000, "start": 1, "is_called": True},
                   "source_sequence_facts": {"current": 1001, "is_called": True},
                   "applied_transaction": result["data_fault"]["source"], "endpos": "0/30"}
        result["sequence_failure"] = {
            "version": 1, "statement_class": "final_sequence_setval",
            "fixture_binding_sha256": hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
            "statement_sha256": hashlib.sha256(b"""SELECT pg_catalog.setval('"M2 space"."rows_id_seq"'::pg_catalog.regclass,1001,true)""").hexdigest(),
            "connection": 2, "backend_pid": 123, "parameter_count": 0, "parse_forwarded": True, "bind_forwarded": True,
            "execute_forwarded": True, "error_observed": True, "sqlstate": "22003", "invalid": False,
            "overflow": False, "matches": 1, "pre_error_ack": copy.deepcopy(wire["ack_oracle"])}
        wire["ack_oracle"].update(invalid=True, ordinal=12)
    else:
        result["lo_setup"] = {"initial": {"objects": 0, "bytes": 0}, "before": [[], [], 0], "introduced": [
            [90007, hashlib.sha256(b"\x01").hexdigest()], [90007, hashlib.sha256(b"\x02").hexdigest()], 0]}
        result["lo_after"] = copy.deepcopy(result["lo_setup"]["introduced"])
    return result, settings, artifact


def m9_cutover_receipt():
    from follow import M9_INPUT_FIELDS, m9_fixture_sha256, m9_settings_sha256
    value = producer_receipt()
    manifest = {**cutover.load_manifest(), "cases": [*cutover.CASES, *M9_FAILURES]}
    records = []
    for phase in ("streaming", "completed"):
        body = json.loads(m9_frame("durable", "clone-" + phase)[15:])
        body["work"] = WORK
        records.append({"binding": {"command": "clone", "work": WORK}, "raw_json": json.dumps(body), "probe_checked": False})
    settings = body["settings"]
    for index in (1, 2, *range(4, 9)):
        case = value["cases"][cutover.CASES[index]]
        m9_producer_authority(case, settings)
        for record in case["reports"]:
            body = json.loads(record["raw_json"])
            body["capabilities"].append("durability.status.v1")
            body["settings"] = settings
            record["raw_json"] = json.dumps(body)
        m9_wire(case, cutover.lsn(body["replication"]["durable_replay_lsn"]))
        case["worker_reports"] = copy.deepcopy(records)
        case["large_objects"] = {"source": {}, "target": {}, "lo_verified": True}
    close = value["cases"][cutover.CASES[9]]
    row = value["cases"][cutover.CASES[8]]["producer_authority"]["checkpoint_row"]
    for phase in ("before", "after", "recovered"):
        close[phase]["authority"] = copy.deepcopy(row)
    close["fault_free_resume"]["raw_json"] = records[-1]["raw_json"]
    for name in M9_FAILURES:
        case, _, artifact = m9_failure_receipt(name)
        value["cases"][name] = case
        records.extend(record for record in case["worker_reports"] if record not in records)
    packet = dict.fromkeys(M9_INPUT_FIELDS, "d" * 64)
    packet.update(artifact["input"], source_tree_sha256=value["source_tree"]["sha256"],
                  settings_sha256=m9_settings_sha256(settings), fixture_sha256=m9_fixture_sha256(clone.ROOT))
    artifact.update(input=packet, schema_sha256=hashlib.sha256(json.dumps(sorted(M9_INPUT_FIELDS), separators=(",", ":")).encode()).hexdigest())
    value.update(profile="m9", artifact=artifact, settings=settings)
    value["report_validation"]["reports"] = records
    return value, manifest


class M9CutoverTests(unittest.TestCase):
    def test_runner_uses_original_conninfo_and_exact_gates_for_m9(self):
        import follow
        class ReachedWorker(Exception):
            pass

        def no_rewrite(_):
            self.fail("cutover rewrote a conninfo before the shared launcher")
        source_wire = SimpleNamespace(connection=no_rewrite)
        fault = SimpleNamespace(connection=no_rewrite)
        from contextlib import nullcontext
        def gate(host, mode):
            return nullcontext({("source", "observe"): source_wire, ("target", "before_commit"): fault}[host, mode])
        worker = SimpleNamespace(follow_target_wire=fault, follow_wire=source_wire)
        def launch(source, target, state, **kwargs):
            self.assertEqual((source, target, state), ("source-original", "target-original", Path("private-state")))
            self.assertEqual(kwargs, {"source_name": "src", "target_name": "dst", "wire": source_wire,
                                      "target_wire": fault, "skip_large_objects": True})
            return worker
        def ready(actual):
            self.assertIs(actual, worker)
            raise ReachedWorker
        driver = SimpleNamespace(ready=ready, report_probe=None)
        case = lambda _: ("src", "dst", "source-original", "target-original", Path("private-state"))
        args = SimpleNamespace(profile="m9", durability=None, report_probe=None)
        manifest = {"cases": [*cutover.CASES, *M9_FAILURES]}
        with patch.object(follow, "WireGate", side_effect=gate), self.assertRaises(ReachedWorker):
            cutover.run(args, None, None, manifest, case, launch, driver)

    def test_m9_receipt_rejects_missing_case_comparison_and_worker_completion(self):
        value, manifest = m9_cutover_receipt()
        cutover.validate_receipt(value, manifest, profile="m9")
        for action in (
            lambda bad: bad["cases"].pop(M9_FAILURES[0]),
            lambda bad: bad["cases"][M9_FAILURES[1]].update(observed=False),
            lambda bad: bad["cases"][cutover.CASES[1]].pop("large_objects"),
            lambda bad: bad["cases"][cutover.CASES[1]]["target_fingerprints"].pop("sequences"),
            lambda bad: bad["cases"][cutover.CASES[1]]["worker_reports"].pop(),
            lambda bad: bad["cases"][cutover.CASES[1]].update(worker_completed=False),
            lambda bad: bad["cases"][cutover.CASES[1]]["wire"]["ack_oracle"].update(invalid=True),
            lambda bad: bad["artifact"]["input"].update(settings_sha256="f" * 64),
            lambda bad: bad["artifact"]["input"].update(source_tree_sha256="f" * 64),
        ):
            with self.subTest(action=action), self.assertRaises(RuntimeError):
                bad = copy.deepcopy(value)
                action(bad)
                cutover.validate_receipt(bad, manifest, profile="m9")

    def test_m9_artifact_refuses_new_manifest_changes_without_fixture_hash_change(self):
        from follow import m9_artifact_binding, m9_fixture_sha256, m9_settings_sha256
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkout = root / "source"
            for name in ("clone/cases.json", "clone/schema.sql", "follow/cases.json", "follow/schema.sql"):
                path = checkout / "tests/fixtures" / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes((clone.ROOT / "tests/fixtures" / name).read_bytes())
            registry = checkout / "tests/fixtures/follow/m9-cutover-cases.json"
            registry.write_text(json.dumps({"version": 1, "cases": [*cutover.CASES, *M9_FAILURES]}))
            binary = root / "private-binary"
            binary.write_bytes(b"never executed")
            binary.chmod(0o700)
            settings = json.loads(m9_frame("durable", "clone-streaming")[15:])["settings"]
            images = ["a" * 64, "b" * 64]
            servers = [{"system_identifier": str(index), "version": "18.6", "data_directory": "/fixture", "wal_level": "logical",
                        "id": str(index) * 64, "image": image, "role": role, "started_at": "2026-11-10T00:00:00Z",
                        "pgdata_permissions": "700|postgres"}
                       for index, role, image in zip((1, 2), ("source", "target"), images)]
            fixture = m9_fixture_sha256(checkout)
            packet = root / "packet.json"
            packet.write_text(json.dumps({
                "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                "source_tree_sha256": cutover.source_inventory(checkout)["sha256"], "source_commit": "c" * 40,
                "source_git_tree": "d" * 40, "runner_image_id": "e" * 64, "server_image_ids": images, "servers": servers,
                "run": "f" * 32, "settings_sha256": m9_settings_sha256(settings), "fixture_sha256": fixture,
            }))
            m9_artifact_binding(binary, settings, servers, packet=packet, source_root=checkout)
            registry.write_text(registry.read_text() + "\n")
            self.assertEqual(m9_fixture_sha256(checkout), fixture)
            with self.assertRaisesRegex(RuntimeError, "artifact inputs differ"):
                m9_artifact_binding(binary, settings, servers, packet=packet, source_root=checkout)

    def test_finalization_failure_needs_exact_drain_and_error_on_top_of_applied_c(self):
        for name in M9_FAILURES:
            value, settings, artifact = m9_failure_receipt(name)
            def check(candidate):
                cutover.validate_finalization_failure(candidate, name, mode="durable", settings=settings, artifact=artifact)
            check(value)
            for mutation in (
                lambda bad: bad.update(worker_reaped=False),
                lambda bad: bad.update(worker_reports=[]),
                lambda bad: bad["worker_result"]["errors"][0].update(exit_code=12),
                lambda bad: bad["caller_goal"].update(end_lsn="0/31"),
                lambda bad: bad["local_endpos"].update(sealed=False),
                lambda bad: bad["producer_authority"]["checkpoint_row"].update(drain_end_lsn=None),
                lambda bad: bad["producer_authority"]["checkpoint_row"].update(follow_completed=True),
                lambda bad: bad["before_drain"].update(source_end_lsn=None),
                lambda bad: bad["source_slot"].update(confirmed_flush_lsn="0/50"),
                lambda bad: bad.update(drain_start_ordinal=9),
            ):
                with self.subTest(case=name, mutation=mutation), self.assertRaises(RuntimeError):
                    bad = copy.deepcopy(value)
                    mutation(bad)
                    check(bad)

    def test_sequence_error_cannot_replace_prior_ack_or_exact_statement_witness(self):
        name = M9_FAILURES[0]
        value, settings, artifact = m9_failure_receipt(name)
        for target, fields in (("sequence_failure", {"connection": 3}), ("sequence_failure", {"sqlstate": "23505"}),
                               ("sequence_failure", {"matches": True}), ("sequence_failure", {"statement_sha256": "0" * 64}),
                               ("sequence_failure", {"fixture_binding_sha256": "0" * 64}),
                               ("sequence_failure", {"parameter_count": 3}), ("sequence_failure", {"error_observed": False}),
                               ("sequence_failure", {"pre_error_ack": {}}),
                               ("worker_result", {"errors": [{"category": "Pgsql", "exit_code": 4}]})):
            with self.subTest(target=target, fields=fields), self.assertRaises(RuntimeError):
                bad = copy.deepcopy(value)
                bad[target].update(fields)
                cutover.validate_finalization_failure(bad, name, mode="durable", settings=settings, artifact=artifact)
        for field, mutation in (("pre_error_ack", {"invalid": True}), ("final_ack", {"invalid": False}),
                                ("final_ack", {"proofs": []})):
            with self.subTest(field=field, mutation=mutation), self.assertRaises(RuntimeError):
                bad = copy.deepcopy(value)
                target = bad["sequence_failure"]["pre_error_ack"] if field == "pre_error_ack" else bad["wire"]["ack_oracle"]
                target.update(mutation)
                cutover.validate_finalization_failure(bad, name, mode="durable", settings=settings, artifact=artifact)

    def test_large_object_collision_requires_empty_initial_owned_receipts_and_exact_refusal(self):
        name = M9_FAILURES[1]
        value, settings, artifact = m9_failure_receipt(name)
        for section, mutation in (("lo_setup", {"before": [[], [], 1]}), ("lo_setup", {"initial": None}),
                                  ("lo_setup", {"introduced": [[], [], 0]}),
                                  ("worker_result", {"errors": [{"category": "Target", "exit_code": 7, "diagnostic": "other"}]})):
            with self.subTest(section=section, mutation=mutation), self.assertRaises(RuntimeError):
                bad = copy.deepcopy(value)
                bad[section].update(mutation)
                cutover.validate_finalization_failure(bad, name, mode="durable", settings=settings, artifact=artifact)
        bad = copy.deepcopy(value)
        control = json.loads(bad["producer_authority"]["checkpoint_row"]["clone_state"])
        control["large_objects"]["final_sync"] = {"objects": 1, "bytes": 1}
        bad["producer_authority"]["checkpoint_row"]["clone_state"] = json.dumps(control)
        with self.assertRaises(RuntimeError):
            cutover.validate_finalization_failure(bad, name, mode="durable", settings=settings, artifact=artifact)

    def test_native10_authority_has_exact_checkpoint_and_owned_object_inventory(self):
        case = producer_receipt()["cases"][cutover.CASES[1]]
        settings = json.loads(m9_frame("durable", "clone-streaming")[15:])["settings"]
        handoff = m9_producer_authority(case, settings)
        cutover.require_producer_authority(handoff, case["caller_goal"]["work"], case["target_row"], mode="durable", settings=settings)
        for field, mutation in (
            ("format", lambda bad: bad.update(native_format=8)),
            ("metadata", lambda bad: bad.update(metadata_version=6)),
            ("row", lambda bad: bad["checkpoint_row"].update(version=6)),
            ("object", lambda bad: bad["catalog"]["objects"].pop()),
            ("duplicate", lambda bad: bad["catalog"]["objects"].append(bad["catalog"]["objects"][-1])),
            ("owner", lambda bad: bad["catalog"]["objects"][-1].update(relowner=11)),
            ("index-parent", lambda bad: bad["catalog"]["objects"][-1].update(index_parent=8)),
            ("name", lambda bad: bad["catalog"]["objects"][-1].update(relname="foreign")),
            ("column", lambda bad: bad["catalog"]["columns"][-1].update(attname="other")),
            ("stored-settings", lambda bad: bad["follow_state"]["effective_settings"].update(apply_group_txns=1)),
        ):
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                bad = copy.deepcopy(handoff)
                mutation(bad)
                cutover.require_producer_authority(bad, case["caller_goal"]["work"], case["target_row"], mode="durable", settings=settings)

    def test_m9_frontier_requires_a_real_writing_proof_covering_the_row(self):
        case = producer_receipt()["cases"][cutover.CASES[6]]
        wire = m9_wire(case, 16)
        cutover.require_sync_prefix(wire, 16)
        for change in (lambda evidence: evidence.update(proofs=[]),
                       lambda evidence: evidence.update(invalid=True),
                       lambda evidence: evidence["proofs"][0].update(ordinal=evidence["ordinal"]),
                       lambda evidence: evidence["commits"][0].update(writing=False),
                       lambda evidence: evidence["commits"][0].update(mode="async"),
                       lambda evidence: evidence["commits"][0].update(warning_free=False)):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                bad = copy.deepcopy(wire)
                change(bad["ack_oracle"])
                cutover.require_sync_prefix(bad, 16)
        with self.assertRaises(RuntimeError):
            cutover.require_sync_prefix(wire, 48)

    def test_manifest_selects_twelve_cases_only_for_explicit_m9(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "m9-cutover-cases.json"
            required = [*cutover.CASES, *M9_FAILURES]
            path.write_text(json.dumps({"version": 1, "cases": required}))
            for durability in (None, "durable", "replay-safe"):
                self.assertEqual(cutover.load_manifest(path, profile="m9", durability=durability)["cases"], required)
            for profile, cases in ((None, required), ("m9", required[:-1]), ("m9", required[::-1]), ("m9", {})):
                with self.subTest(profile=profile, cases=cases), self.assertRaises(RuntimeError):
                    path.write_text(json.dumps({"version": 1, "cases": cases}))
                    cutover.load_manifest(path, profile=profile)
            self.assertEqual(cutover.load_manifest()["cases"], list(cutover.CASES))
            for durability in (None, "durable", "replay-safe"):
                self.assertEqual(cutover.load_manifest(profile="m9", durability=durability)["cases"], required)

    def test_m9_completion_requires_both_actual_frames_and_equal_settings(self):
        for mode in ("durable", "replay-safe"):
            start = m9_frame(mode, "clone-streaming")
            final = m9_frame(mode, "clone-completed")
            body = json.loads(final[15:])
            binding = {key: body[key] for key in ("command", "work")}
            self.assertEqual(cutover.completed_output(start + final, binding, profile="m9", durability=mode), final)
            self.assertEqual(cutover.completed_output(final, binding, profile="m9", durability=mode, already_completed=True), final)
            bad = copy.deepcopy(body)
            bad["copy"]["tables"]["done"] -= 1
            mismatched = copy.deepcopy(body)
            mismatched["settings"]["apply_group_txns"] += 1
            for output in (start + framed(bad), start + framed(mismatched), final, start + final + final):
                with self.subTest(mode=mode, output=output), self.assertRaises(RuntimeError):
                    cutover.completed_output(output, binding, profile="m9", durability=mode)
            with self.assertRaises(RuntimeError):
                cutover.completed_output(start + final, binding, profile="m9", durability="durable" if mode == "replay-safe" else "replay-safe")
            with self.assertRaises(RuntimeError):
                cutover.completed_output(start + final, binding)

    def test_m9_actual_sentinel_keeps_later_begin_out_of_durable_replay(self):
        for mode in ("durable", "replay-safe"):
            for command in ("sentinel-get", "sentinel-set"):
                output = m9_frame(mode, command)
                report = json.loads(output[15:])
                binding = {key: report[key] for key in ("command", "work")}
                value = report["replication"]
                self.assertLess(cutover.lsn(value["durable_replay_lsn"]), cutover.lsn(value["end_lsn"]))
                self.assertGreater(cutover.lsn(value["drain_proof"]["replay_lsn"]), cutover.lsn(value["end_lsn"]))
                self.assertEqual(cutover.replication_output(output, binding, value["end_lsn"], profile="m9", durability=mode), report)
                malformed = [output.replace(b'"settings":', b'"settings":{},"settings":'),
                             output.replace(b'"channel_messages":17', b'"channel_messages":1e1'),
                             framed({**report, "capabilities": ["cutover.v1"]}),
                             framed({**report, "unexpected": True}),
                             framed({**report, "replication": {**value, "drained": False}})]
                bad_work = {**report["work"], "foreign": True}
                forged = {**report, "work": bad_work, "replication": {**value, "drain_proof": {**value["drain_proof"], "work": bad_work}}}
                with self.assertRaises(RuntimeError):
                    cutover.replication_output(framed(forged), {**binding, "work": bad_work}, value["end_lsn"], profile="m9", durability=mode)
                for candidate in malformed:
                    with self.subTest(mode=mode, command=command, candidate=candidate), self.assertRaises(RuntimeError):
                        cutover.replication_output(candidate, binding, value["end_lsn"], profile="m9", durability=mode)

class CutoverHarnessTests(unittest.TestCase):
    def test_completion_requires_exact_startup_and_final_frames(self):
        binding = {"command": "clone", "work": WORK}
        startup, final = report(phase="streaming"), report()
        output = framed(startup) + framed(final)
        self.assertEqual(cutover.completed_output(output, binding), framed(final))
        bad_final = copy.deepcopy(final)
        bad_final["copy"]["tables"]["done"] = 0
        for bad in (b"", framed(final), framed(startup), output + framed(final),
                    framed(final) + framed(startup), b"unexpected\n" + output,
                    framed(startup) + framed(bad_final), output.replace(b'"protocol_version": 1', b'"protocol_version": 1, "protocol_version": 1', 1),
                    output.replace(b'"checkpoint_generation": 1', b'"checkpoint_generation": 2', 1), output[:-2]):
            with self.subTest(output=bad), self.assertRaises((RuntimeError, ValueError)):
                cutover.completed_output(bad, binding)

    def test_replication_requires_exact_end_durable_proof_and_independent_certification_floor(self):
        binding = {"command": "stream sentinel set endpos", "work": WORK}
        value = replication()
        self.assertEqual(cutover.replication_output(framed(value), binding, "0/20", certified_floor=48), value)
        mutations = [({"phase": "draining"}, {}), ({"work": {**WORK, "workdir_id": "foreign"}}, {}),
                     ({}, {"end_lsn": "0/21"}), ({}, {"end_persisted": False}), ({}, {"drained": False}),
                     ({}, {"certified_lsn": "0/10"}), ({}, {"durable_replay_lsn": "0/10"}),
                     ({}, {"drain_proof": {}}), ({}, {"checkpoint_id": "1:0:4"}),
                     ({}, {"extra": True})]
        for outer, inner in mutations:
            bad = copy.deepcopy(value)
            bad.update(outer)
            bad["replication"].update(inner)
            with self.subTest(outer=outer, inner=inner), self.assertRaises(RuntimeError):
                cutover.replication_output(framed(bad), binding, "0/20", certified_floor=48)
        for key, change in (("committed", False), ("end_lsn", "0/21"), ("replay_lsn", "0/10"), ("replay_lsn", "0/40"),
                            ("work", {}), ("kind", "outer_wal_end"), ("checkpoint_id", "1:0:2")):
            bad = copy.deepcopy(value)
            bad["replication"]["drain_proof"][key] = change
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                cutover.replication_output(framed(bad), binding, "0/20", certified_floor=48)

    def test_refusal_requires_category_exit_and_empty_stdout(self):
        error = {"level": "error", "category": "BadState", "exit_code": 3, "message": "opaque"}
        good = subprocess.CompletedProcess([], 3, b"", json.dumps(error).encode())
        cutover.require_fence_refusal(good)
        for result in (subprocess.CompletedProcess([], 0, b"", good.stderr),
                       subprocess.CompletedProcess([], 3, framed(replication()), good.stderr),
                       subprocess.CompletedProcess([], 3, b"", b"BadState"),
                       subprocess.CompletedProcess([], 3, b"", json.dumps({**error, "category": "Pgsql"}).encode()),
                       subprocess.CompletedProcess([], 3, b"", good.stderr + b"\n" + good.stderr)):
            with self.subTest(result=result), self.assertRaises(RuntimeError):
                cutover.require_fence_refusal(result)

    def test_completed_worker_is_joined_and_only_final_frame_is_added_as_completion(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            executable = root / "worker"
            output = framed(report(phase="streaming")) + framed(report())
            executable.write_text("#!" + sys.executable + "\nimport sys\nsys.stdout.buffer.write(" + repr(output) + ")\n")
            executable.chmod(0o700)
            driver = clone.Driver(executable, {"PATH": "/usr/bin"}, root, mode="operator")
            state = root / "state"
            driver.work[state] = WORK
            worker = driver.start("source-fixture", "target-fixture", state)
            try:
                self.assertEqual(cutover.finish_completed(driver, worker)["phase"], "completed")
                self.assertEqual(worker.returncode, 0)
                self.assertEqual(len(driver.reports), 1)
            finally:
                if worker.poll() is None:
                    driver.kill(worker)
                worker.wait(timeout=3)

    def test_local_endpos_read_requires_exact_persisted_value_and_command_success(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            (state / "control").mkdir()
            path = state / "control/endpos.json"
            path.write_text(json.dumps({"end_lsn": 32}))
            result = subprocess.CompletedProcess([], 0, b"0/20\n", b"")
            with patch.object(cutover, "stream", return_value=(result, None)):
                self.assertEqual(cutover.persisted_endpos(None, state, "0/20")["end_lsn"], 32)
                path.write_text(json.dumps({"end_lsn": 48}))
                with self.assertRaises(RuntimeError):
                    cutover.persisted_endpos(None, state, "0/20")
            for result in (subprocess.CompletedProcess([], 3, b"0/20\n", b""),
                           subprocess.CompletedProcess([], 0, b"0/30\n", b"")):
                with patch.object(cutover, "stream", return_value=(result, None)), self.assertRaises(RuntimeError):
                    cutover.persisted_endpos(None, state, "0/20")

    def test_active_fence_request_requires_a_live_worker_and_exact_gate_hit(self):
        worker = SimpleNamespace(poll=lambda: 0)
        gate = SimpleNamespace(held=SimpleNamespace(is_set=lambda: True), evidence=lambda: {"hits": 1})
        with self.assertRaises(RuntimeError):
            cutover.active_fence_request(None, None, None, worker, gate, "0/20")
        worker.poll = lambda: None
        gate.evidence = lambda: {"hits": 0}
        with self.assertRaises(RuntimeError):
            cutover.active_fence_request(None, None, None, worker, gate, "0/20")

    def test_identical_completed_retry_checks_both_actual_reports_and_unchanged_token(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            binding = root / "binding.json"
            binding.write_text(json.dumps({"command": "stream sentinel set endpos", "work": WORK}))
            driver = SimpleNamespace(report_probe=None)
            result = subprocess.CompletedProcess([], 0, framed(replication()), b"")
            with patch.object(cutover, "stream", return_value=(result, binding)) as invoke, \
                    patch.object(cutover, "persisted_endpos", return_value={"end_lsn": 32}):
                receipt = cutover.completed_retry(driver, root, "target-fixture", "0/20", certified_floor=48)
                self.assertTrue(receipt["observed"])
                self.assertEqual(len(receipt["reports"]), 2)
                self.assertEqual(invoke.call_count, 2)
                for call in invoke.call_args_list:
                    self.assertIn("0/20", call.args)
                    self.assertNotIn("--current", call.args)
            different = replication()
            different["replication"]["checkpoint_id"] = "1:0:4"
            different["replication"]["drain_proof"]["checkpoint_id"] = "1:0:4"
            with patch.object(cutover, "stream", side_effect=[(result, binding), (subprocess.CompletedProcess([], 0, framed(different), b""), binding)]), \
                    patch.object(cutover, "persisted_endpos", return_value={"end_lsn": 32}), self.assertRaises(RuntimeError):
                cutover.completed_retry(driver, root, "target-fixture", "0/20", certified_floor=48)

    def test_wire_witness_keeps_later_begin_separate_from_feedback_authority(self):
        value = replication()
        value["replication"].update(certified_lsn="0/10", durable_replay_lsn="0/10")
        value["replication"]["drain_proof"]["kind"] = "later_begin"
        stream = {"confirmed": True, "invalid": False, "overflow": False,
                  "transactions": [{"begin_forwarded": True, "final_lsn": 48}], "keepalives": [],
                  "statuses": [(16, 16, 16)]}
        evidence = {"logical": {"overflow": False, "streams": [stream]}}
        cutover.require_wire_witness(evidence, value)
        for change in ({"statuses": [(48, 48, 48)]}, {"transactions": []}, {"invalid": True},
                       {"transactions": [{"begin_forwarded": False, "final_lsn": 48}]}):
            bad = copy.deepcopy(evidence)
            bad["logical"]["streams"][0].update(change)
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                cutover.require_wire_witness(bad, value)

    def test_certification_race_retains_sampled_end_instead_of_later_wal_marker(self):
        source = SimpleNamespace(sql=lambda db, sql: b"0/20\n" if db == "owned_source" else b"0/40\n")
        row = {"certified_lsn": "0/30", "source_end_lsn": "0/10", "copy_consistent_point": "0/8"}
        target = SimpleNamespace(authority=lambda *args: row, connection=lambda name: "target-fixture")
        driver = SimpleNamespace(work={Path("state"): WORK})
        worker = SimpleNamespace(poll=lambda: None)
        result = subprocess.CompletedProcess([], 0, b"0/20\n", b"")
        with patch.object(cutover, "stream", return_value=(result, None)) as invoke, \
                patch.object(cutover, "persisted_endpos"), patch.object(cutover, "finish_completed"), \
                patch.object(cutover, "completed_retry", return_value={"observed": True}) as retry:
            receipt = cutover.certification_race(driver, source, target, "owned_source", "owned_target", Path("state"), worker, end="0/20")
            self.assertEqual(receipt["sampled_end_lsn"], "0/20")
            self.assertEqual(receipt["certified_before_request"], "0/30")
            self.assertEqual(receipt["unrelated_wal_lsn"], "0/40")
            self.assertIn("0/20", invoke.call_args.args)
            self.assertNotIn("0/40", invoke.call_args.args)
            self.assertNotIn("--current", invoke.call_args.args)
            self.assertEqual(retry.call_args.kwargs["certified_floor"], 48)

    def test_async_visibility_requires_actual_off_setting_new_xmin_same_token_and_held_fence(self):
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            record = {"table_oid": 42, "intent": {"binding": {"stream": {"run": list(bytes.fromhex(WORK["run_id"]))},
                       "schema_name": "owned", "table_name": "checkpoint"}}}
            (state / "manifest.json").write_text(json.dumps({"target_provenance": [{"phase": "confirmed", "record": record}]}))
            row = {"barrier_epoch": 0, "barrier_counter": 3, "source_end_lsn": "0/10", "certified_lsn": "0/30", "copy_consistent_point": "0/8"}
            for marker, visible, refused in (([123, "off", "11", 0, 3, True], ["11", 0, 3], False),
                                             ([123, "on", "11", 0, 3, True], ["11", 0, 3], True),
                                             ([123, "off", "10", 0, 3, True], ["10", 0, 3], True),
                                             ([123, "off", "11", 0, 3, True], ["11", 0, 4], True)):
                holder = SimpleNamespace(stdin=io.BytesIO(), stdout=io.BytesIO(), stderr=io.BytesIO(), returncode=None)
                holder.poll = lambda: holder.returncode
                def communicate(**kwargs):
                    holder.returncode = 0
                    return b"", b""
                holder.communicate = communicate
                holder.terminate = lambda: None
                holder.wait = lambda **kwargs: 0
                target = SimpleNamespace(authority=lambda *args: row, connection=lambda name: "target-fixture",
                                         bindir=Path("/owned/bin"), environment={},
                                         sql=lambda db, sql: b"10\n" if "xmin::text FROM" in sql else json.dumps(visible).encode() if "json_build_array" in sql else b"t\n")
                driver = SimpleNamespace(work={state: WORK})
                error = {"level": "error", "category": "BadState", "exit_code": 3}
                commands = [(subprocess.CompletedProcess([], 3, b"", json.dumps(error).encode()), None),
                            (subprocess.CompletedProcess([], 0, b"0/30\n", b""), None)]
                with self.subTest(marker=marker, visible=visible), patch.object(cutover.subprocess, "Popen", return_value=holder), \
                        patch.object(cutover, "line_from", return_value=json.dumps(marker), create=True), \
                        patch.object(cutover, "stream", side_effect=commands):
                    if refused:
                        with self.assertRaises(RuntimeError):
                            cutover.async_visible_refusal(driver, target, "owned_target", state)
                    else:
                        receipt = cutover.async_visible_refusal(driver, target, "owned_target", state)
                        self.assertEqual(receipt["scope"], "active_fence_refusal_around_visible_async_same_value_write")
                        self.assertTrue(receipt["token_unchanged"])
                        self.assertIn(b"SET synchronous_commit=off", holder.stdin.getvalue())

    def test_final_frontiers_match_independent_row_and_do_not_promote_witness_to_reconnect(self):
        value = replication()
        value["replication"].update(certified_lsn="0/10", durable_replay_lsn="0/10")
        value["replication"]["drain_proof"]["kind"] = "later_begin"
        row = {"generation": 1, "barrier_epoch": 0, "barrier_counter": 3, "source_end_lsn": "0/10",
               "certified_lsn": "0/10", "copy_consistent_point": "0/8", "drain_end_lsn": "0/20",
               "drain_witness_lsn": "0/30", "drain_kind": "later_begin", "follow_completed": True}
        slot = {"confirmed_flush_lsn": "0/10", "restart_lsn": "0/8"}
        cutover.require_runtime_frontiers(row, slot, value)
        crossing = copy.deepcopy(value)
        crossing["replication"]["durable_replay_lsn"] = "0/28"
        cutover.require_runtime_frontiers({**row, "source_end_lsn": "0/28"}, slot, crossing)
        for changed_row, changed_slot in (({"certified_lsn": "0/30"}, {}), ({}, {"confirmed_flush_lsn": "0/30"}),
                                           ({}, {"restart_lsn": "0/30"}), ({"follow_completed": False}, {}),
                                           ({"drain_witness_lsn": "0/40"}, {})):
            with self.subTest(row=changed_row, slot=changed_slot), self.assertRaises(RuntimeError):
                cutover.require_runtime_frontiers({**row, **changed_row}, {**slot, **changed_slot}, value)

    def test_verified_completed_resume_requires_one_final_frame(self):
        binding = {"command": "clone", "work": WORK}
        final = framed(report())
        self.assertEqual(cutover.completed_output(final, binding, already_completed=True), final)
        for bad in (framed(report(phase="streaming")), framed(report(phase="streaming")) + final,
                    final + final, final[:-1]):
            with self.subTest(output=bad), self.assertRaises(RuntimeError):
                cutover.completed_output(bad, binding, already_completed=True)

    def test_cutover_refuses_the_old_positive_probe_before_command_execution(self):
        with patch.object(cutover, "stream") as invoke, self.assertRaisesRegex(RuntimeError, "independent"):
            cutover.completed_retry(SimpleNamespace(report_probe="old-command-work-only-probe"), Path("state"), "target", "0/20")
        invoke.assert_not_called()

    def test_source_writer_fixture_requires_committed_rows_correct_setting_and_checked_reap(self):
        for setting, after, bad in (("on", 132, False), ("off", 132, False), ("on", 100, True)):
            holder = SimpleNamespace(stdin=io.BytesIO(), stdout=io.BytesIO(), stderr=io.BytesIO(), returncode=None)
            holder.poll = lambda: holder.returncode
            def communicate(**kwargs):
                holder.returncode = 0
                return b"", b""
            holder.communicate = communicate
            holder.terminate = lambda: None
            calls = []
            def sql(name, query):
                calls.append(query)
                if "count(*) FROM ONLY" in query:
                    return b"100\n"
                return b"t\n"
            source = SimpleNamespace(sql=sql, bindir=Path("/owned/bin"), environment={}, connection=lambda name: "owned")
            marker = [123, "2026-11-08 12:00:00+00", setting, after, 42]
            with self.subTest(setting=setting, after=after), patch.object(cutover.subprocess, "Popen", return_value=holder), \
                    patch.object(cutover, "line_from", return_value=json.dumps(marker)):
                if bad:
                    with self.assertRaises(RuntimeError):
                        cutover.source_batch(source, "owned", setting)
                else:
                    result = cutover.source_batch(source, "owned", setting)
                    self.assertEqual(result["rows_inserted"], 32)
                    self.assertTrue(result["writer_reaped"])
                    self.assertEqual(holder.stdin.getvalue().count(b"COMMIT;"), 32)
                    self.assertIn(("SET synchronous_commit=" + setting).encode(), holder.stdin.getvalue())

    def test_end_sampling_waits_for_absence_of_outstanding_source_transactions(self):
        queries = []
        source = SimpleNamespace(sql=lambda name, query: (queries.append(query) or (b"0\n" if "pg_stat_activity" in query else b"0/20\n")))
        self.assertEqual(cutover.frozen_end(source, "owned"), "0/20")
        self.assertIn("pg_stat_activity", queries[0])
        self.assertEqual(queries[1], "SELECT pg_current_wal_insert_lsn();")
        queries.clear()
        with patch.object(cutover.clone, "wait_for", side_effect=RuntimeError("outstanding source transaction")), self.assertRaises(RuntimeError):
            cutover.frozen_end(source, "owned")
        self.assertEqual(queries, [])

    def test_cleanup_report_matches_caller_owned_inventory_and_retained_obligations(self):
        binding = {"command": "stream cleanup", "work": WORK}
        obligations = [{"kind": "replication_slot", "name": "owned_slot", "owned": True, "removed": True},
                       {"kind": "publication", "name": "owned_pub", "owned": True, "removed": True},
                       {"kind": "workdir", "name": "/owned/state", "owned": True, "removed": False},
                       {"kind": "replication_origin", "name": "foreign_origin", "owned": False, "removed": False}]
        value = {"protocol_version": 1, "engine_version": "0.1.0", "command": "stream cleanup", "work": WORK,
                 "capabilities": ["cleanup.v1"], "phase": "completed", "cleanup": {"complete": True, "resources": obligations}}
        self.assertEqual(cutover.cleanup_output(framed(value), binding, obligations), value)
        for resources in ([], obligations[:-1], obligations + [obligations[0]],
                          [{**item, "removed": True} if item["kind"] == "workdir" else item for item in obligations],
                          [{**item, "owned": True} if item["kind"] == "replication_origin" else item for item in obligations]):
            bad = {**value, "cleanup": {"complete": True, "resources": resources}}
            with self.subTest(resources=resources), self.assertRaises(RuntimeError):
                cutover.cleanup_output(framed(bad), binding, obligations)
        with self.assertRaises(RuntimeError):
            cutover.cleanup_output(framed(value), binding, [])
        incomplete = [{**item, "kind": "unrelated"} if item["kind"] == "workdir" else item for item in obligations]
        with self.assertRaises(RuntimeError):
            cutover.cleanup_output(framed({**value, "cleanup": {"complete": True, "resources": incomplete}}), binding, incomplete)

    def test_engine_receipt_requires_every_nonempty_case_and_refuses_missing_runtime_evidence(self):
        value = producer_receipt()
        manifest = cutover.load_manifest()
        cutover.validate_receipt(value, manifest)
        for name in cutover.CASES:
            bad = copy.deepcopy(value)
            del bad["cases"][name]
            with self.subTest(missing=name), self.assertRaises(RuntimeError):
                cutover.validate_receipt(bad, manifest)
        with self.assertRaises(RuntimeError):
            cutover.validate_receipt({**value, "probe_checked": True}, manifest)
        changes = [("loaded_cutover_durable_source", "writer", {}),
                   ("loaded_cutover_async_source", "source_write_freeze", False),
                   ("local_endpos_survives_active_fence_json_refusal", "success_stdout_empty", False),
                   ("same_endpos_retry_after_completed_observation", "reports", []),
                   ("same_endpos_retry_after_completed_observation", "producer_authority", {}),
                   ("endpos_sampled_before_certification_retains_exact_request", "end_lsn", "0/30"),
                   ("async_visible_row_cannot_emit_replication_json", "unflushed_wal_proven", True),
                   ("later_begin_coverage_preserves_pre_later_oracle", "forwarded_keepalives", 1),
                   ("crossing_transaction_remains_whole_across_cutover_and_resume", "data_fault", {}),
                   ("cleanup_retains_foreign_resources_and_comparison_state", "cleanup_reports", [])]
        for name, key, change in changes:
            bad = copy.deepcopy(value)
            bad["cases"][name][key] = change
            with self.subTest(case=name, key=key), self.assertRaises((RuntimeError, KeyError, ValueError)):
                cutover.validate_receipt(bad, manifest)
        for fields in ({"receipt_scope": "full"}, {"independent_go_evidence": "BarrierConfirmed"}, {"source_tree": {}},
                       {"report_validation": {}}, {"successful_follow_exit": False}):
            with self.subTest(fields=fields), self.assertRaises(RuntimeError):
                cutover.validate_receipt({**value, **fields}, manifest)
        with self.assertRaisesRegex(RuntimeError, "independent M6"):
            cutover.validate_receipt(value, manifest, require_independent_go=True)

    def test_completed_resume_close_receipt_requires_refusal_and_bound_terminal_fault(self):
        value = producer_receipt()
        manifest = cutover.load_manifest()
        cutover.validate_receipt(value, manifest)
        name = "completed_resume_requires_checked_authority_close"
        mutations = [
            (("worker_result", "returncode"), 0),
            (("worker_result", "stdout", "bytes"), 1),
            (("worker_result", "stdout", "sha256"), "f" * 64),
            (("worker_result", "errors"), [{"category": "BadState", "exit_code": 3}]),
            (("worker_result", "errors"), [{"category": "Pgsql", "exit_code": 3}]),
            (("fault", "commit_ordinal"), 1), (("fault", "status"), "unavailable"),
            (("fault", "next_frontend_kind"), None), (("fault", "next_frontend_kind"), "X"),
            (("fault", "terminal_delivered"), False), (("fault", "old_drop_terminate"), True),
            (("fault_held", "terminal_delivered"), True), (("fault_held", "next_frontend_kind"), "P"),
            (("after", "authority", "barrier_counter"), 99),
            (("after", "native_manifest_sha256"), "c" * 64),
            (("after", "local_endpos_sha256"), "c" * 64),
            (("after", "source_resources"), [{"slot": "unexpected"}, None]),
            (("after", "foreign_inventory"), {}),
            (("recovered", "authority", "barrier_counter"), 99),
            (("before", "authority", "follow_completed"), False),
            (("work", "run_id"), "f" * 32),
            (("backend_absence", "backend_pid"), 124), (("backend_absence", "database_oid"), 43),
            (("backend_absence", "absent"), False), (("backend_absence", "system_identifier"), 101),
            (("target_wire", "connections"), []),
            (("target_wire", "connections"), [{"connection": 1, "backend_pid": 124}]),
            (("worker_reaped",), False), (("relay_workers_reaped",), False), (("fault_hits",), 0),
            (("fault_free_resume", "binding", "work"), {}),
            (("fault_free_resume", "raw_json"), json.dumps(report(phase="streaming"))),
        ]
        for path, change in mutations:
            bad = copy.deepcopy(value)
            item = bad["cases"][name]
            for key in path[:-1]:
                item = item[key]
            item[path[-1]] = change
            with self.subTest(path=path, change=change), self.assertRaises(RuntimeError):
                cutover.validate_receipt(bad, manifest)
        for path, change in [(("worker_result",), []), (("worker_result", "stdout", "bytes"), False),
                             (("worker_result", "errors"), [None]), (("target_wire", "connections"), {}),
                             (("fault_free_resume",), []), (("fault",), None)]:
            bad = copy.deepcopy(value)
            item = bad["cases"][name]
            for key in path[:-1]:
                item = item[key]
            item[path[-1]] = change
            with self.subTest(malformed=path), self.assertRaises(RuntimeError):
                cutover.validate_receipt(bad, manifest)
        for section in ("fault", "fault_held"):
            for key in value["cases"][name][section]:
                bad = copy.deepcopy(value)
                del bad["cases"][name][section][key]
                with self.subTest(section=section, missing=key), self.assertRaises(RuntimeError):
                    cutover.validate_receipt(bad, manifest)
        for section in ("fault", "fault_held"):
            for key, change in (("connection", 0), ("backend_pid", 0), ("backend_pid", True),
                                ("protocol", "unknown"), ("begin_observed", False), ("terminal_idle", False),
                                ("upstream_disconnected", False), ("overflow", True), ("classification_invalid", True)):
                bad = copy.deepcopy(value)
                bad["cases"][name][section][key] = change
                with self.subTest(section=section, key=key), self.assertRaises(RuntimeError):
                    cutover.validate_receipt(bad, manifest)
        for key in value["cases"][name]:
            bad = copy.deepcopy(value)
            del bad["cases"][name][key]
            with self.subTest(missing=key), self.assertRaises(RuntimeError):
                cutover.validate_receipt(bad, manifest)

    def test_old_nine_case_inventory_cannot_qualify_cutover(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "cases.json"
            path.write_text(json.dumps({"version": 1, "cases": list(cutover.CASES[:9])}))
            with self.assertRaisesRegex(RuntimeError, "inventory"):
                cutover.load_manifest(path)

    def test_completed_close_runner_requires_bound_fault_and_retains_old_success_diagnostic(self):
        import follow
        for scenario in ("fixed", "old", "unbound", "hold_timeout", "absence_timeout", "shared_deadline"):
            old = scenario == "old"
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                state = root / "state"
                (state / "control").mkdir(parents=True)
                (state / "manifest.json").write_text(json.dumps({"format_version": 8,
                    "follow_state": {"origin_name": "preserved_origin", "cleanup": {"slot_removed": True, "publication_removed": True}}}))
                (state / "control/endpos.json").write_text('{"end_lsn":32}')
                driver = clone.Driver(Path("unused"), {}, root, mode="operator")
                driver.work[state] = WORK
                cutover.saved_goal(driver, state, "0/20")
                receipt = producer_receipt()["cases"]
                clean = receipt["cleanup_retains_foreign_resources_and_comparison_state"]
                value = receipt["completed_resume_requires_checked_authority_close"]
                events, now = [], [100.0]
                gate = SimpleNamespace()
                def arm():
                    events.append("armed")
                    now[0] += 1
                def held_wait(timeout):
                    self.assertEqual(timeout, 8)
                    events.append(("held", timeout))
                    now[0] += 1
                    return scenario != "hold_timeout"
                gate.arm = arm
                gate.held = SimpleNamespace(wait=held_wait)
                gate.release = SimpleNamespace(set=lambda: events.append("released"))
                gate.connection = lambda connection: connection.replace("host=target", "host=127.0.0.1 port=1234")
                final = {**value["fault"], "next_frontend_kind": "X" if old else "P", "old_drop_terminate": old}
                gate.fault_observation = lambda: final if "released" in events else value["fault_held"]
                gate.snapshot = lambda: {**value["target_wire"], "connections": []} if scenario == "unbound" else value["target_wire"]
                gate.evidence = lambda: {"hits": 1}
                class Gate:
                    def __init__(self, host, mode):
                        self_test.assertEqual((host, mode), ("target", "completed_resume_close"))
                    def __enter__(self):
                        return gate
                    def __exit__(self, *_):
                        events.append("relay_reaped")
                self_test = self
                def sql(name, query):
                    self.assertEqual(name, "dst_owned")
                    self.assertIn("pid=123", query)
                    self.assertNotIn("released", events)
                    events.append("sql_absence")
                    self.assertEqual(target._observation_deadline, 110.0)
                    now[0] += 10 if scenario == "shared_deadline" else 1
                    observed = {key: item for key, item in value["backend_absence"].items()
                                if key not in ("connection", "backend_pid")}
                    if scenario == "absence_timeout":
                        observed["absent"] = False
                    return json.dumps(observed).encode()
                expected = clean["source_fingerprints"]
                source = SimpleNamespace(connection=lambda _: "host=source", fingerprint=lambda *_: expected)
                target = SimpleNamespace(host="target", connection=lambda _: "host=target", _observe=lambda read: read(), sql=sql,
                                         authority=lambda *_: value["before"]["authority"], fingerprint=lambda *_: expected)
                launches = []
                def launch(src, dst, path, *arguments, **keywords):
                    self.assertEqual(src, "host=source")
                    self.assertEqual(path, state)
                    self.assertEqual(arguments, ("--resume", "--not-consistent", "--origin", "preserved_origin"))
                    self.assertEqual(keywords, {"source_name": "src_owned", "target_name": "dst_owned"})
                    if not launches:
                        self.assertEqual(dst, "host=127.0.0.1 port=1234")
                        self.assertEqual(events, ["armed"])
                    else:
                        self.assertEqual(dst, "host=target")
                        self.assertIn("relay_reaped", events)
                    index = len(launches)
                    if index == 0:
                        now[0] += 1
                    output, errors, binding = (root / f"{index}.{suffix}" for suffix in ("stdout", "stderr", "binding"))
                    status = 0 if old or index else 4
                    output.write_bytes(framed(report()) if status == 0 else b"")
                    errors.write_bytes(b"" if status == 0 else json.dumps({"level": "error", "category": "Pgsql", "exit_code": 4}).encode())
                    binding.write_text(json.dumps({"command": "clone", "work": WORK}))
                    def wait(timeout):
                        self.assertEqual(timeout, 120 if index else 6)
                        now[0] += 1
                        return status
                    worker = SimpleNamespace(output=output, error_output=errors, binding=binding, environment={},
                                             wait=wait, poll=lambda: status)
                    launches.append(worker)
                    return worker
                def wait_for(probe, description, timeout):
                    self.assertGreater(timeout, 0)
                    self.assertLessEqual(timeout, 10)
                    observed = probe()
                    if observed is None:
                        raise RuntimeError("required observation absent: " + description)
                    return observed
                with patch.object(follow, "WireGate", Gate), patch.object(clone, "wait_for", side_effect=wait_for), \
                        patch.object(cutover.time, "monotonic", side_effect=lambda: now[0]), \
                        patch.object(cutover, "resource_snapshot", return_value=b"[null,null]"), \
                        patch.object(cutover, "foreign_resource_inventory", return_value=json.dumps(value["before"]["foreign_inventory"]).encode()), \
                        patch.object(clone, "DIAGNOSTIC_ROOT", root / "diagnostics"):
                    if scenario in ("unbound", "hold_timeout", "absence_timeout", "shared_deadline"):
                        reason = {"unbound": "BackendKeyData", "hold_timeout": "ten seconds", "absence_timeout": "independently absent",
                                  "shared_deadline": "shared ten-second deadline"}[scenario]
                        with self.assertRaisesRegex(RuntimeError, reason):
                            cutover.completed_resume_close(driver, source, target, "src_owned", "dst_owned", state, launch, clean)
                        self.assertIn("released", events)
                        self.assertIn("relay_reaped", events)
                    elif old:
                        with self.assertRaises(clone.SubprocessFailure) as failure:
                            cutover.completed_resume_close(driver, source, target, "src_owned", "dst_owned", state, launch, clean)
                        retained = json.loads((root / "diagnostics" / (failure.exception.metadata["diagnostic_id"] + ".stderr")).read_bytes())
                        self.assertEqual(retained["worker_result"]["returncode"], 0)
                        self.assertEqual(retained["completed_report"]["phase"], "completed")
                        self.assertEqual(retained["fault"]["next_frontend_kind"], "X")
                        self.assertIs(retained["fault"]["terminal_delivered"], True)
                        self.assertIs(retained["authority_unchanged"], True)
                        self.assertEqual(retained["preservation"]["authority"]["before_sha256"],
                                         retained["preservation"]["authority"]["after_sha256"])
                        self.assertIs(retained["relay_workers_reaped"], True)
                        self.assertNotIn("fixture-sensitive-row-", json.dumps(retained))
                    else:
                        result = cutover.completed_resume_close(driver, source, target, "src_owned", "dst_owned", state, launch, clean)
                        cutover.validate_completed_close(result, clean)
                        self.assertEqual(result["worker_result"]["returncode"], 4)
                        self.assertEqual(result["after"], result["before"])
                        self.assertEqual(result["recovered"], result["before"])
                if "sql_absence" in events:
                    self.assertLess(events.index("sql_absence"), events.index("released"))
                self.assertEqual(len(launches), 2 if scenario == "fixed" else 1)

    def test_caller_goal_is_saved_before_command_and_cannot_be_resampled(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / "state"
            driver = SimpleNamespace(root=root, work={state: WORK})
            goal = cutover.saved_goal(driver, state, "0/20")
            self.assertEqual(goal, {"end_lsn": "0/20", "work": WORK})
            self.assertEqual(cutover.saved_goal(driver, state, "0/20"), goal)
            path = root / "state.caller-endpos.json"
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(RuntimeError):
                cutover.saved_goal(driver, state, "0/30")
            path.write_text('{"end_lsn":')
            with self.assertRaises((RuntimeError, ValueError)):
                cutover.saved_goal(driver, state, "0/20")

    def test_source_inventory_binds_bytes_modes_and_rejects_symlinks(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "src").mkdir()
            path = root / "src/main.rs"
            path.write_text("fn main() {}\n")
            before = cutover.source_inventory(root)
            path.write_text("fn main() { panic!(); }\n")
            self.assertNotEqual(before, cutover.source_inventory(root))
            path.chmod(0o700)
            changed = cutover.source_inventory(root)
            path.chmod(0o600)
            self.assertNotEqual(changed, cutover.source_inventory(root))
            (root / "target").mkdir()
            (root / "target/private").write_text("not source")
            self.assertEqual(cutover.source_inventory(root)["file_count"], 1)
            path.unlink()
            path.symlink_to("/outside/owned-source")
            with self.assertRaises(RuntimeError):
                cutover.source_inventory(root)

    def test_cutover_runner_cannot_emit_success_when_fixture_setup_is_absent(self):
        def missing(name):
            raise RuntimeError("fixture never created")
        with self.assertRaisesRegex(RuntimeError, "fixture never created"):
            cutover.run(SimpleNamespace(report_probe=None), None, None, cutover.load_manifest(), missing, None, None)

    def test_completion_collection_refuses_a_still_running_worker(self):
        with self.assertRaisesRegex(RuntimeError, "joined"):
            cutover.completion(None, None, None, None, None, None, SimpleNamespace(poll=lambda: None), None, "0/20", None)

    def test_completed_resumes_use_verified_single_frame_lifecycle_and_reject_token_changes(self):
        state = Path("state")
        driver = SimpleNamespace(work={state: WORK})
        source = SimpleNamespace(connection=lambda name: "source-fixture")
        before = {"follow_completed": True, "barrier_counter": 3}
        expected = {"fixture": "unchanged"}
        target = SimpleNamespace(authority=lambda *args: before, connection=lambda name: "target-fixture", fingerprint=lambda *args: expected)
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            driver.work[state] = WORK
            (state / "manifest.json").write_text(json.dumps({"follow_state": {"origin_name": None}}))
            for changed in (False, True):
                values = iter([before, {**before, "barrier_counter": 4}]) if changed else None
                target.authority = (lambda *args: next(values)) if changed else (lambda *args: before)
                with self.subTest(changed=changed), patch.object(cutover, "resource_snapshot", return_value=b"owned-catalog"), \
                        patch.object(cutover, "finish_completed") as finish:
                    launch = unittest.mock.Mock(return_value=object())
                    if changed:
                        with self.assertRaises(RuntimeError):
                            cutover.completed_resumes(driver, source, target, "src", "dst", state, launch, expected)
                    else:
                        self.assertEqual(cutover.completed_resumes(driver, source, target, "src", "dst", state, launch, expected),
                                         {"completed_resumes": 2, "resume_row_unchanged": True})
                        self.assertEqual(launch.call_count, 2)
                    for call in finish.call_args_list:
                        self.assertIs(call.kwargs["already_completed"], True)
            target.authority = lambda *args: {"follow_completed": False}
            with self.assertRaises(RuntimeError):
                cutover.completed_resumes(driver, source, target, "src", "dst", state, None, expected)

    def test_pending_data_fault_requires_the_held_transaction_and_real_successful_dml_tags(self):
        fault = {"status": "observed", "scope": "wire_fault_observation", "boundary": "before_commit", "protocol": "extended",
                 "connection": 1, "backend_pid": 123, "begin_observed": True, "overflow": False, "classification_invalid": False,
                 "command_tags": {"insert_0_1": 1, "update_1": 2, "delete_1": 0, "other_dml": 0}}
        transaction = {"xid": 3, "final_lsn": 32, "commit_lsn": 32, "end_lsn": 40, "dml_count": 2,
                       "complete": True, "begin_forwarded": True, "commit_forwarded": True}
        gate = SimpleNamespace(fault_observation=lambda: fault)
        self.assertEqual(cutover.pending_data_fault(gate, transaction), fault)
        for change in ({"status": "unavailable"}, {"protocol": "backend"}, {"backend_pid": None}, {"begin_observed": False},
                       {"overflow": True}, {"classification_invalid": True}, {"command_tags": {"insert_0_1": 0, "update_1": 1, "delete_1": 0, "other_dml": 0}}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                cutover.pending_data_fault(SimpleNamespace(fault_observation=lambda: {**fault, **change}), transaction)
        for change in ({"complete": False}, {"commit_forwarded": False}, {"dml_count": 1}, {"final_lsn": 31}):
            with self.subTest(transaction=change), self.assertRaises(RuntimeError):
                cutover.pending_data_fault(gate, {**transaction, **change})

    def test_idle_certification_waits_for_a_real_nonnull_target_frontier(self):
        state = Path("state")
        rows = iter([None, {"certified_lsn": None}, {"certified_lsn": "0/30"}])
        source = SimpleNamespace(sql=lambda *args: b"0/20\n")
        target = SimpleNamespace(_observe=lambda read: read(), authority=lambda *args: next(rows))
        driver = SimpleNamespace(work={state: WORK})
        self.assertEqual(cutover.idle_certification(driver, source, target, "src", "dst", state, SimpleNamespace(poll=lambda: None)), "0/30")
        with self.assertRaises(RuntimeError):
            cutover.idle_certification(driver, source, target, "src", "dst", state, SimpleNamespace(poll=lambda: 3))

    def test_stream_invokes_actual_executable_with_exact_explicit_boundary(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            executable = root / "command"
            executable.write_text("#!" + sys.executable + "\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n")
            executable.chmod(0o700)
            driver = clone.Driver(executable, {"PATH": "/usr/bin"}, root, mode="operator")
            state = root / "state"
            driver.prepare(state, "clone", create=True)
            result, binding = cutover.stream(driver, state, ("stream", "sentinel", "set", "endpos"),
                                             "0/20", "--json", source="source-fixture", target="target-fixture")
            self.assertEqual(result.returncode, 0)
            self.assertEqual(json.loads(result.stdout), ["stream", "sentinel", "set", "endpos", "--dir", str(state),
                                                       "--source", "source-fixture", "--target", "target-fixture", "0/20", "--json"])
            self.assertEqual(json.loads(binding.read_text())["command"], "stream sentinel set endpos")


    def test_inner_cutover_dispatch_requires_actual_runner_cases_before_receipt(self):
        import follow
        fixture = producer_receipt()
        for failure in (False, True):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                queries = []
                def source_sql(name, query):
                    queries.append((name, query))
                    return b"16.15" if query == "SHOW server_version;" else b"t"
                source = SimpleNamespace(sql=source_sql)
                target = SimpleNamespace(sql=lambda *_: b"18.6")
                driver = SimpleNamespace(reports=fixture["report_validation"]["reports"],
                                         start=unittest.mock.Mock(return_value=SimpleNamespace()), watch=unittest.mock.Mock())
                def cases(*arguments):
                    launch = arguments[5]
                    state = root / "completed-cleanup"
                    state.mkdir()
                    (state / "manifest.json").write_text(json.dumps({"follow_state": {"cleanup": {"slot_removed": True},
                        "binding": {"slot": {"intent": {"binding": {"slot_name": "owned_slot"}}}}}}))
                    launch("source-fixture", "target-fixture", state, "--resume", "--not-consistent",
                           source_name="saved_source", target_name="saved_target")
                    self.assertIn(("saved_source", "SELECT NOT EXISTS(SELECT FROM pg_replication_slots WHERE slot_name='owned_slot');"), queries)
                    return fixture["cases"]
                args = SimpleNamespace(pair="16-18", binary="/owned/engine", receipt=str(root / "receipt.json"),
                                       report_probe=None, cutover=True, smoke=False, diagnostic_case=None)
                with patch.object(follow, "Database", side_effect=[source, target]), patch.object(follow, "Driver", return_value=driver), \
                        patch.object(clone, "run", return_value=b"libpq.so.5 => /owned/libpq.so.5"), \
                        patch.object(clone, "tool_version", return_value="18.6"), \
                        patch.object(follow.ctypes, "CDLL", return_value=SimpleNamespace(PQlibVersion=lambda: 180006)), \
                        patch.dict(follow.os.environ, {"PGPASSWORD": "synthetic"}), \
                        patch.object(cutover, "source_inventory", return_value=fixture["source_tree"]), \
                        patch.object(cutover, "run", side_effect=RuntimeError("actual fixture failed") if failure else cases) as runner, \
                        patch.object(follow, "check_workload", side_effect=AssertionError("ongoing case dispatched")), \
                        patch("sys.stdout", new=io.StringIO()):
                    if failure:
                        with self.assertRaisesRegex(RuntimeError, "actual fixture failed"):
                            follow.inner(args)
                        self.assertFalse(Path(args.receipt).exists())
                    else:
                        follow.inner(args)
                        value = json.loads(Path(args.receipt).read_bytes())
                        self.assertEqual(value["receipt_scope"], "engine_producer")
                        self.assertEqual(set(value["cases"]), set(cutover.CASES))
                        self.assertEqual(value["source_tree"], fixture["source_tree"])
                    runner.assert_called_once()
                    self.assertIs(runner.call_args.args[1], source)
                    self.assertIs(runner.call_args.args[2], target)

    def test_outer_forwards_cutover_and_joins_images_source_before_final_validation(self):
        fixture = producer_receipt()
        manifest = cutover.load_manifest()
        for changed in (False, True):
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary).resolve()
                (root / "tools/e2e").mkdir(parents=True)
                (root / "tools/e2e/Containerfile").write_text("FROM example.invalid/rust@sha256:" + "6" * 64 + "\n")
                args = SimpleNamespace(pair="16-18", receipt=str(root / "result.json"), report_probe=None,
                                       cutover=True, smoke=False, diagnostic_case=None)
                model = ModelRuntime(root)
                source_tree = cutover.source_inventory(root)
                def execute(guard, command, **kwargs):
                    guard.checkpoint()
                    result = model.call(command, **kwargs)
                    if command[1] == "start" and "--attach" in command:
                        guard.sample()
                        actual = model.containers[command[-1]]
                        if actual["model_role"] == "runner":
                            created = next(call for call in model.commands if call[1] == "create" and "tools/e2e/follow.py" in call)
                            self.assertIn("--cutover", created)
                            receipt = Path(next(row["Source"] for row in actual["Mounts"] if row["Destination"] == "/receipt"))
                            value = copy.deepcopy(fixture)
                            value["source_tree"] = {**source_tree, "sha256": "f" * 64} if changed else source_tree
                            (receipt / "result.json").write_text(json.dumps(value))
                    return result
                def validate(value, manifest):
                    self.assertEqual(value["runner_image_id"], "sha256:" + "5" * 64)
                    self.assertEqual(value["server_image_ids"], ["sha256:" + digit * 64 for digit in ("1", "2")])
                    self.assertEqual(value["source_tree"], source_tree)
                    cutover.validate_receipt(value, manifest, require_images=True)
                with patch.object(cutover, "load_manifest", return_value=manifest), \
                        patch.object(clone, "ROOT", root), patch.object(clone, "DIAGNOSTIC_ROOT", root / "target/e2e/diagnostics"), \
                        patch.object(clone.shutil, "which", return_value="/unit/podman"), \
                        patch.object(clone.shutil, "disk_usage", return_value=SimpleNamespace(free=57 * 1024**3)), \
                        patch.object(clone, "run", side_effect=model.call), \
                        patch.object(clone.subprocess, "run", side_effect=model.subprocess), \
                        patch.object(clone.RuntimeGuard, "execute", execute), \
                        patch("sys.stdout", new=io.StringIO()):
                    validator = unittest.mock.Mock(side_effect=validate)
                    if changed:
                        with self.assertRaisesRegex(RuntimeError, "unchanged runner source"):
                            clone.outer(args, program="follow", manifest=cutover.load_manifest(), receipt_validator=validator)
                        validator.assert_not_called()
                        self.assertFalse(Path(args.receipt).exists())
                    else:
                        manifest = cutover.load_manifest()
                        clone.outer(args, program="follow", manifest=manifest, receipt_validator=validator)
                        validator.assert_called_once()
                        self.assertEqual(json.loads(Path(args.receipt).read_bytes())["runner_image_id"], "sha256:" + "5" * 64)
                self.assertFalse(model.containers)
                self.assertFalse(model.networks)
                self.assertEqual(len(model.removed), 4)


    def test_failed_sentinel_result_is_retained_before_gate_and_workdir_cleanup_without_free_text(self):
        import follow
        from contextlib import ExitStack
        import hashlib
        import shutil
        events = []
        secret = "fixture-password-private"
        canary = "fixture-sensitive-row-private"
        raw = json.dumps({"level": "error", "category": "BadArgs", "exit_code": 1,
                          "message": "native:BadArgs: " + secret + " " + canary + " (SQLSTATE 42P01)",
                          "error_origin": {"file": "src/stream.rs", "line": 75, "column": 3}}).encode()
        child = subprocess.CompletedProcess([], 1, canary.encode(), raw)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            state = root / "native"
            state.mkdir(mode=0o700)
            (state / "manifest.json").write_text('{"format_version":8}')
            status = {"worker": None, "held": True}
            stderr = state / "worker.stderr"
            stderr.write_bytes(raw)
            worker = SimpleNamespace(pid=123, poll=lambda: status["worker"], error_output=stderr,
                                     environment={"PGPASSWORD": secret},
                                     follow_startup_capture=lambda deadline: events.append(("startup", deadline)) or {"scope": "startup_diagnostic", "engine_pid": 123},
                                     follow_observe=lambda: events.append(("checkpoint", None)) or {"scope": "visible-observation", "target": {"status": "unavailable"}})
            gate = SimpleNamespace(held=SimpleNamespace(is_set=lambda: status["held"]), evidence=lambda: {"hits": 1},
                                   fault_observation=lambda: producer_receipt()["cases"][cutover.CASES[0]]["data_fault"]["fault"])
            driver = SimpleNamespace(capture_startup=follow.Driver.capture_startup)
            original = cutover.require_fence_refusal
            def assertion(result):
                retained = [json.loads(p.read_text()) for p in (root / "diagnostics").glob("*.json")]
                self.assertTrue(any(item["command_kind"] == "engine-cutover-command-result" for item in retained))
                events.append(("assertion", None))
                original(result)
            def teardown():
                events.append(("teardown", None))
                status.update(worker=7, held=False)
                shutil.rmtree(state)
            with patch.object(clone, "DIAGNOSTIC_ROOT", root / "diagnostics"), \
                    patch.object(cutover, "stream", return_value=(child, None)), \
                    patch.object(cutover, "require_fence_refusal", side_effect=assertion):
                with self.assertRaises(clone.SubprocessFailure) as failure:
                    with ExitStack() as cleanup:
                        cleanup.callback(teardown)
                        cutover.active_fence_request(driver, state, "private-connection", worker, gate, "0/20")
            self.assertFalse(state.exists())
            self.assertEqual(status["worker"], 7)
            metadata = failure.exception.metadata
            self.assertEqual(metadata["command_kind"], "engine-cutover-fence-observation")
            value = json.loads((root / "diagnostics" / (metadata["diagnostic_id"] + ".stderr")).read_bytes())
            self.assertEqual(value["scope"], "diagnostic")
            self.assertIs(value["durability_confirmed"], False)
            self.assertEqual(value["failed_check"], "refusal_assertion")
            self.assertEqual(value["expected_refusal"], {"category": "BadState", "returncode": 3, "stdout_bytes": 0})
            self.assertEqual(value["sentinel"]["returncode"], 1)
            self.assertEqual(value["sentinel"]["stdout"], {"bytes": len(child.stdout), "sha256": hashlib.sha256(child.stdout).hexdigest()})
            self.assertEqual(value["sentinel"]["stderr"]["bytes"], len(raw))
            self.assertEqual(value["sentinel"]["errors"][0]["category"], "BadArgs")
            self.assertEqual(value["sentinel"]["errors"][0]["sqlstate"], "42P01")
            self.assertIsNone(value["worker_before_command"])
            self.assertIsNone(value["worker_after_child_return"])
            self.assertIsNone(value["worker_before_teardown"])
            self.assertIs(value["gate_held_after_child_return"], True)
            self.assertEqual(value["held_target"]["backend_pid"], 123)
            self.assertLess([item[0] for item in events].index("checkpoint"), [item[0] for item in events].index("teardown"))
            for path in (root / "diagnostics").iterdir():
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                self.assertNotIn(secret, path.read_text())
                self.assertNotIn(canary, path.read_text())
                self.assertNotIn("private-connection", path.read_text())


    def test_command_observation_whitelists_categories_origins_and_fixed_discriminators(self):
        good = {"level": "error", "category": "BadState", "exit_code": 3,
                "message": "native:BadState: target-follow: writer fence is held by an active owner",
                "error_origin": {"file": "src/apply/checkpoint.rs", "line": 1549, "column": 21},
                "arbitrary": "fixture-sensitive-row-private"}
        result = subprocess.CompletedProcess([], 3, b"fixture-sensitive-row-stdout", json.dumps(good).encode())
        value = cutover.command_observation(result)
        self.assertEqual(value["errors"], [{"category": "BadState", "exit_code": 3, "diagnostic": "active_owner_fence",
                                           "error_origin": good["error_origin"]}])
        self.assertNotIn("fixture-sensitive-row-", json.dumps(value))
        for change in ({"exit_code": True}, {"exit_code": 7}, {"category": "fixture-sensitive-row-category"}):
            bad = subprocess.CompletedProcess([], 3, b"", json.dumps({**good, **change}).encode())
            self.assertEqual(cutover.command_observation(bad)["errors"], [])
        bad = {**good, "error_origin": {"file": "/private/fixture-sensitive-row-origin.rs", "line": 1, "column": 1}}
        observed = cutover.command_observation(subprocess.CompletedProcess([], 3, b"", (json.dumps(bad) + "\n").encode() * 20))
        self.assertEqual(len(observed["errors"]), 16)
        self.assertTrue(all("error_origin" not in item for item in observed["errors"]))

    def test_stream_observes_child_before_diagnostic_refusal_without_retaining_canaries(self):
        import hashlib
        secret = "fixture-password-private"
        raw = json.dumps({"level": "error", "category": "BadArgs", "exit_code": 1,
                          "message": secret + " fixture-sensitive-row-private"}).encode()
        child = subprocess.CompletedProcess([], 1, b"fixture-sensitive-row-stdout", raw)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            driver = clone.Driver(Path("unit-command"), {"PGPASSWORD": secret}, root, mode="operator")
            state = root / "state"
            driver.prepare(state, "clone", create=True)
            seen = []
            with patch.object(clone, "DIAGNOSTIC_ROOT", root / "diagnostics"), \
                    patch.object(cutover.subprocess, "run", return_value=child), self.assertRaises(clone.SubprocessFailure) as failure:
                cutover.stream(driver, state, ("stream", "sentinel", "set", "endpos"), "0/20", "--json",
                               target="private-connection", result_observer=lambda value: seen.append(cutover.command_observation(value)))
            self.assertEqual(len(seen), 1)
            self.assertEqual(seen[0]["stderr"]["sha256"], hashlib.sha256(raw).hexdigest())
            metadata = failure.exception.metadata
            self.assertEqual(metadata["command_kind"], "engine-cutover-diagnostics")
            retained = json.loads((root / "diagnostics" / (metadata["diagnostic_id"] + ".stderr")).read_text())
            self.assertEqual(retained, seen[0])
            for path in (root / "diagnostics").iterdir():
                self.assertNotIn(secret, path.read_text())
                self.assertNotIn("fixture-sensitive-row-", path.read_text())
                self.assertNotIn("private-connection", path.read_text())

    def test_secondary_retention_failure_preserves_the_original_refusal_assertion(self):
        worker = SimpleNamespace(poll=lambda: None, environment={})
        gate = SimpleNamespace(held=SimpleNamespace(is_set=lambda: True), evidence=lambda: {"hits": 1})
        child = subprocess.CompletedProcess([], 7, b"", b"")
        with patch.object(cutover, "stream", return_value=(child, None)), \
                patch.object(clone, "private_failure", side_effect=OSError("secondary-observer-private")), \
                patch.object(cutover, "retain_fence_failure", side_effect=RuntimeError("secondary-observer-private")), \
                self.assertRaisesRegex(RuntimeError, "active target fence did not produce an empty-stdout BadState refusal"):
            cutover.active_fence_request(None, None, None, worker, gate, "0/20")

    def test_failure_capture_shares_existing_deadline_and_skips_expired_observers(self):
        for previous in (None, 102.5):
            with self.subTest(previous=previous), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                state = root / "state"
                state.mkdir()
                (state / "manifest.json").write_text('{"format_version":8}')
                now = [100.0]
                def checkpoint():
                    self.assertEqual(worker.follow_capture_deadline, min(104.0, previous) if previous is not None else 104.0)
                    now[0] = 104.0
                    return {"status": "unavailable"}
                worker = SimpleNamespace(poll=lambda: None, follow_observe=checkpoint)
                if previous is not None:
                    worker.follow_capture_deadline = previous
                driver = SimpleNamespace(capture_startup=unittest.mock.Mock())
                authority = unittest.mock.Mock()
                gate = SimpleNamespace(held=SimpleNamespace(is_set=lambda: True))
                facts = {"sentinel": cutover.command_observation(subprocess.CompletedProcess([], 7, b"", b""))}
                with patch.object(clone, "DIAGNOSTIC_ROOT", root / "diagnostics"), patch.object(cutover.time, "monotonic", side_effect=lambda: now[0]):
                    metadata = cutover.retain_fence_failure(driver, state, worker, gate, facts, authority)
                driver.capture_startup.assert_not_called()
                authority.assert_not_called()
                if previous is None:
                    self.assertFalse(hasattr(worker, "follow_capture_deadline"))
                else:
                    self.assertEqual(worker.follow_capture_deadline, previous)
                value = json.loads((root / "diagnostics" / (metadata["diagnostic_id"] + ".stderr")).read_bytes())
                self.assertEqual(value["authority"], {"status": "unavailable"})
                self.assertEqual(value["startup"], {"status": "unavailable"})
                self.assertIs(value["native_identity"]["reusable_m6_identity"], False)

    def test_diagnostic_authority_keeps_numeric_checkpoint_and_catalog_hashes_only(self):
        row = producer_receipt()["cases"][cutover.CASES[1]]["target_row"]
        row.update(version=6, source_system=1, source_database=2, target_system=3, target_database=4,
                   owner_oid=5, schema_oid=6, table_oid=7, clone_state="fixture-sensitive-row-private")
        catalog = {"objects": [{"unsafe": "fixture-sensitive-row-private"}], "columns": [None] * 29,
                   "constraints": [None], "indexes": [None]}
        target = SimpleNamespace(_observation_deadline=199.0, authority=lambda *_: row)
        driver = SimpleNamespace(work={Path("state"): WORK})
        def producer(*_):
            self.assertEqual(target._observation_deadline, 105.0)
            return {"catalog": catalog, "target_provenance": {"unsafe": "fixture-sensitive-row-private"}}
        with patch.object(cutover, "producer_authority", side_effect=producer):
            value = cutover.diagnostic_authority(driver, target, "owned", Path("state"), 105.0)
        self.assertEqual(target._observation_deadline, 199.0)
        self.assertEqual(value["scope"], "diagnostic")
        self.assertIs(value["durability_confirmed"], False)
        self.assertEqual(value["catalog"]["columns"], 29)
        self.assertEqual(value["checkpoint"]["source_end_lsn"], 16)
        self.assertNotIn("fixture-sensitive-row-", json.dumps(value))
        self.assertNotIn("clone_state", value["checkpoint"])


if __name__ == "__main__":
    unittest.main()
