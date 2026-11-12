"""Portable negative controls for candidate M8 evidence, without runtime claims."""

import copy
import hashlib
import json
import stat
import tempfile
import types
from unittest.mock import patch
from contextlib import redirect_stderr
import io
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import copy_features as harness


def acl(grantor, grantee, privilege, grantable=False):
    return {"grantor": grantor, "grantee": grantee, "privilege": privilege, "grantable": grantable}


def lo_lines(content=b"\0\0\xff", *, oid=90001):
    metadata = {"oid": oid, "length": len(content), "owner": "source_owner", "acl_default": True,
                "acl": [acl("source_owner", "source_owner", "SELECT"), acl("source_owner", "source_owner", "UPDATE")], "comment": ""}
    records = [{"kind": "inventory", "objects": [metadata]}]
    if content:
        records.append({"kind": "chunk", "oid": oid, "offset": 0, "hex": content.hex()})
    records.append({"kind": "eof", "oid": oid, "offset": len(content), "hex": ""})
    return records


def decode(records):
    return harness.decode_large_objects(b"\n".join(json.dumps(value).encode() for value in records), max_objects=16, max_logical_bytes=16777216)


def artifact_binding():
    return {"binary_sha256": "a" * 64, "source_tree_sha256": "b" * 64, "source_commit": "1"*40, "source_git_tree": "2"*40,
            "runner_image_id": "sha256:" + "c" * 64, "server_image_ids": ["sha256:" + "d"*64, "sha256:" + "e"*64], "run": "f"*32,
            "servers": [{"id": value*64, "image": "sha256:" + image*64, "role": role, "system_identifier": system, "version": version,
                         "data_directory": directory, "wal_level": "logical", "started_at": "2026-11-10T00:00:00Z", "pgdata_permissions": "700|postgres"}
                        for value, image, role, system, version, directory in (("3", "d", "source", "7706000000000000001", "16.15", "/var/lib/postgresql/data"),
                                                                             ("4", "e", "target", "7706000000000000002", "18.6", "/var/lib/postgresql/18/docker"))]}


def build_smoke(binding):
    return {"binary_sha256": binding["binary_sha256"], "source_tree_sha256": binding["source_tree_sha256"], "runner_image_id": binding["runner_image_id"],
            "architecture": "arm64", "client_versions": {tool: "18.6" for tool in ("psql", "pg_dump", "pg_restore")}, "linked_libpq": 180006,
            "library_path": "/usr/lib/aarch64-linux-gnu/libpq.so.5.18", "library_sha256": "7"*64,
            "packages": {"libpq5": {"version": "18.6-1.pgdg13+1", "architecture": "arm64"}, "postgresql-client-16": {"version": "16.15-1.pgdg13+1", "architecture": "arm64"},
                         "postgresql-client-18": {"version": "18.6-1.pgdg13+1", "architecture": "arm64"}},
            "version_json": {"protocol_version": 1, "engine_version": "0.1.0", "capabilities": ["clone.single_db", "cdc.pgoutput", "progress.v1", "cutover.v1", "verify.v1", "cleanup.v1", "verify.sealed_files.v1"]},
            "version_text": "rpgmigrate 0.1.0\nlibpq 180006\n"}


def progress_driver():
    work = {"migration_uid": "12345678-9abc-def0-1234-56789abcdef0", "run_id": "123456789abcdef0123456789abcdef0", "workdir_id": "m2-pvc-"+"1"*32, "checkpoint_generation": 1}
    record = {"binding": {"command": "clone", "work": work}, "probe_checked": False,
              "raw_json": json.dumps({"protocol_version": 1, "engine_version": "0.1.0", "command": "clone", "capabilities": ["clone.single_db"], "work": work,
                                      "phase": "completed", "copy": {name: {"total": 1, "done": 1} for name in ("tables", "indexes", "bytes")}})}
    return types.SimpleNamespace(reports=[record], work={Path("binary_safe"): work})


class CopyFeatureTests(unittest.TestCase):
    def test_feature_manifest_pins_budget_and_reviewed_fixture_bytes(self):
        manifest = harness.load_manifest()
        self.assertEqual(manifest["split_rows"], 286720)
        self.assertEqual(manifest["split_threshold"], 536870912)
        self.assertEqual(manifest["max_heap_bytes"], 671088640)
        self.assertEqual(tuple(manifest["cases"]), ("binary_safe", "binary_fallback", "split_default", "large_objects", "lo_only", "mixed"))
        for change in ({"cases": []}, {"split_threshold": 1}, {"fixtures": {}}, {"max_logical_bytes": 0}):
            with self.assertRaises(RuntimeError):
                harness.validate_manifest({**manifest, **change})

    def test_logical_oracle_refuses_missing_chunk_eof_extra_oid_and_nonempty_eof(self):
        records = lo_lines()
        variants = [records[:-1], [records[0], records[-1]], records + [records[-1]],
                    [*records[:-1], {**records[-1], "hex": "00"}],
                    [records[0], {**records[1], "oid": 90002}, records[-1]],
                    [records[0], {**records[1], "offset": 1}, records[-1]],
                    [records[0], {**records[1], "hex": "0000FE"}, records[-1]], []]
        for bad in variants:
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                decode(bad)

    def test_logical_oracle_distinguishes_empty_absent_holes_and_identical_bytes_wrong_oid(self):
        actual = decode(lo_lines())
        self.assertEqual(actual[90001]["sha256"], hashlib.sha256(b"\0\0\xff").hexdigest())
        self.assertEqual(actual[90001]["length"], 3)
        self.assertEqual(actual[90001]["comment"], "")
        self.assertNotEqual(actual, decode(lo_lines(b"\0\xff")))
        self.assertNotEqual(actual, decode(lo_lines(oid=90002)))
        self.assertNotEqual(decode(lo_lines(b"")), decode([{"kind": "inventory", "objects": []}]))
        for mutation in (lambda row: row.update(length=16777217), lambda row: row.update(oid=0),
                         lambda row: row.update(comment=None, owner=""), lambda row: row.update(acl=[])):
            bad = lo_lines(); mutation(bad[0]["objects"][0])
            with self.assertRaises(RuntimeError):
                decode(bad)

    def test_lo_inventory_producer_serializes_outer_result_as_one_jsonb_record(self):
        fixtures = [lo_lines(b""), lo_lines(b"\0\xff", oid=90002)]
        for count in (0, 1, 2):
            with self.subTest(objects=count):
                records = [{"kind": "inventory", "objects": [value[0]["objects"][0] for value in fixtures[:count]]}]
                records.extend(record for value in fixtures[:count] for record in value[1:])
                raw = b"\n".join(json.dumps(record).encode() for record in records)

                def sql(name, query):
                    self.assertEqual(name, "fixture_db")
                    inventory = query[query.index("SELECT json_build_object('kind','inventory'"):].split(" FROM (", 1)[0]
                    self.assertRegex(inventory, r"^SELECT json_build_object\('kind','inventory',.*\)::jsonb$")
                    return raw

                actual = harness.large_object_fingerprint(types.SimpleNamespace(sql=sql), "fixture_db")
                expected = {
                    90001: {"oid": 90001, "length": 0, "owner": "source_owner", "acl_default": True,
                            "acl": [acl("source_owner", "source_owner", "SELECT"), acl("source_owner", "source_owner", "UPDATE")],
                            "comment": "", "sha256": hashlib.sha256(b"").hexdigest()},
                    90002: {"oid": 90002, "length": 2, "owner": "source_owner", "acl_default": True,
                            "acl": [acl("source_owner", "source_owner", "SELECT"), acl("source_owner", "source_owner", "UPDATE")],
                            "comment": "", "sha256": hashlib.sha256(b"\0\xff").hexdigest()},
                }
                self.assertEqual(actual, {oid: expected[oid] for oid in tuple(expected)[:count]})

    def test_lo_inventory_composite_newlines_remain_invalid_record_framing(self):
        fixtures = [lo_lines(b""), lo_lines(b"\0\xff", oid=90002)]
        for count in (1, 2):
            with self.subTest(objects=count):
                objects = [value[0]["objects"][0] for value in fixtures[:count]]
                # json_agg(composite) inserts physical LFs between ACL and inventory records.
                inventory = b'{"kind" : "inventory", "objects" : [' + b", \n ".join(
                    json.dumps(row).encode().replace(b"}, {", b"}, \n {") for row in objects) + b"]}"
                self.assertEqual(json.loads(inventory), {"kind": "inventory", "objects": objects})
                tail = [record for value in fixtures[:count] for record in value[1:]]
                with self.assertRaisesRegex(RuntimeError, "independent LO inventory, content, or EOF observation is missing or invalid"):
                    harness.decode_large_objects(b"\n".join([inventory, *(json.dumps(record).encode() for record in tail)]))

    def test_mismatch_refusal_requires_changed_lo_matching_tables_and_successful_restore(self):
        proof = {"returncode": 7, "source_lo": {"90005": {"sha256": "a" * 64}}, "target_lo": {"90005": {"sha256": "b" * 64}},
                 "source_tables": {"public.items": {"rows": 3, "sha256": "c" * 64}}, "target_tables": {"public.items": {"rows": 3, "sha256": "c" * 64}},
                 "restored_source": {"90005": {"sha256": "a" * 64}}, "restored_target": {"90005": {"sha256": "a" * 64}}}
        harness.validate_mismatch_refusal(proof)
        for change in ({"returncode": 0}, {"source_lo": proof["target_lo"]}, {"target_tables": {}}, {"restored_target": {}}, {"returncode": None}):
            with self.assertRaises(RuntimeError):
                harness.validate_mismatch_refusal({**proof, **change})

    def test_source_lo_fixture_requires_exact_oids_lengths_hashes_and_empty_eof(self):
        source = {}
        for oid, data in ((90001, b""), (90002, bytes.fromhex("00ff275c")), (90003, b"Z" * 2097159),
                          (90004, b"\0" * 2097159 + b"\0\xff"), (90005, b"\1\2\3")):
            source[oid] = {"length": len(data), "sha256": hashlib.sha256(data).hexdigest(), "owner": "m8_source_owner"}
        harness.validate_lo_fixture(source)
        for bad in ({**source, 90006: source[90001]}, {oid: row for oid, row in source.items() if oid != 90001},
                    {**source, 90004: {**source[90004], "length": 2}}, {**source, 90003: {**source[90003], "sha256": "0" * 64}}):
            with self.assertRaises(RuntimeError):
                harness.validate_lo_fixture(bad)

    def test_selected_acl_applies_literal_delta_to_target_owner_defaults(self):
        source = decode(lo_lines())
        expected = harness.expected_large_objects(source, target_owner="target_owner", no_owner=True)
        self.assertEqual(expected[90001]["acl"], [acl("target_owner", "target_owner", "SELECT"), acl("target_owner", "target_owner", "UPDATE")])
        row = source[90001]
        row.update(acl_default=False, acl=[acl("source_owner", "source_owner", "SELECT"), acl("source_owner", None, "SELECT")])
        expected = harness.expected_large_objects(source, target_owner="target_owner", no_owner=True)
        self.assertCountEqual(expected[90001]["acl"], [acl("target_owner", "target_owner", "SELECT"), acl("target_owner", "target_owner", "UPDATE"), acl("target_owner", "source_owner", "SELECT"), acl("target_owner", None, "SELECT")])
        selected = harness.expected_large_objects(source, target_owner="target_owner", no_owner=True, no_acl=True, no_comments=True)
        self.assertNotIn("acl", selected[90001])
        self.assertNotIn("comment", selected[90001])
        self.assertEqual(selected[90001]["owner"], "target_owner")

    def test_copy_branch_evidence_requires_complete_exact_parts_and_simultaneous_sessions(self):
        records = [{"connection": i + 1, "backend_pid": 100 + i, "relation": ["public", "m8_split"], "direction": "out", "range": bounds,
                    "format": "binary", "columns": 2, "rows": 3, "data_frames": 1, "eof": True, "complete": True, "held": True, "released": True} for i, bounds in enumerate(([0, 10], [10, None]))]
        evidence = {"invalid": False, "overflow": False, "records": records}
        sessions = [{"pid": 100, "backend_start": "2026-11-10 00:00:00+00", "database_oid": 42}, {"pid": 101, "backend_start": "2026-11-10 00:00:00+00", "database_oid": 42}]
        harness.require_copy_branch(evidence, ["public", "m8_split"], "binary", split=True, simultaneous=sessions)
        for bad in ({**evidence, "records": records[:-1]}, {**evidence, "invalid": True}, {**evidence, "overflow": True},
                    {**evidence, "records": [records[0], {**records[1], "range": [11, None]}]},
                    {**evidence, "records": [records[0], {**records[1], "complete": False}]},
                    {**evidence, "records": [records[0], {**records[1], "format": "text"}]},
                    {**evidence, "records": [records[0], {**records[1], "backend_pid": 100}]}):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                harness.require_copy_branch(bad, ["public", "m8_split"], "binary", split=True, simultaneous=sessions)
        with self.assertRaises(RuntimeError):
            harness.require_copy_branch(evidence, ["public", "m8_split"], "binary", split=True, simultaneous=[])

    def test_inventory_excludes_only_all_six_positively_validated_owned_classes(self):
        record = {"schema_oid": 20, "table_oid": 21, "marker_table_oid": 23, "marker_index_oid": 24,
                  "large_object_table_oid": 25, "large_object_index_oid": 26,
                  "intent": {"binding": {"metadata_version": 7, "owner_oid": 30, "schema_name": "_rpgmigrate_owned", "table_name": "checkpoint"}}}
        rows = [{"oid": oid, "schema_oid": 20, "owner_oid": 30, "persistence": "p", "kind": kind,
                 "schema": "_rpgmigrate_owned", "name": name, "index_parent": parent, "primary": True if kind == "i" else None}
                for oid, kind, name, parent in ((21, "r", "checkpoint", None), (22, "i", "checkpoint_pkey", 21),
                                               (23, "r", "copy_done", None), (24, "i", "copy_done_pkey", 23),
                                               (25, "r", "large_object_done", None), (26, "i", "large_object_done_pkey", 25))]
        user = {"oid": 27, "schema_oid": 20, "owner_oid": 30, "persistence": "p", "kind": "r", "schema": "_rpgmigrate_owned", "name": "checkpoint_other", "index_parent": None, "primary": None}
        self.assertEqual(harness.validate_physical_inventory(rows + [user], record), [user])
        self.assertEqual(harness.validate_physical_inventory(rows, record), [])
        for bad in (rows[:-1], rows + [rows[0]], [{**row, "owner_oid": 31} if row["oid"] == 25 else row for row in rows],
                    [{**row, "index_parent": 23} if row["oid"] == 26 else row for row in rows],
                    [{**row, "oid": 33} if row["oid"] == 23 else row for row in rows]):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                harness.validate_physical_inventory(bad, record)
        self.assertEqual(harness.validate_physical_inventory([user], None), [user])
        with self.assertRaises(RuntimeError):
            harness.validate_physical_inventory(rows, {**record, "large_object_table_oid": 23})

    def test_malformed_physical_inventory_cannot_become_empty_success(self):
        for rows in ([{"oid": 42}], [{"oid": 42, "kind": "r"}], [None], None):
            with self.assertRaises(RuntimeError):
                harness.validate_physical_inventory(rows, None)

    def test_table_oracle_accepts_proven_empty_and_rejects_dropped_table_output(self):
        harness.require_table_fingerprints({}, {}, [], [])
        table = {"schema": "public", "name": "m8_safe"}
        with self.assertRaises(RuntimeError):
            harness.require_table_fingerprints({}, {}, [table], [table])
        with self.assertRaises(RuntimeError):
            harness.require_table_fingerprints({}, {}, None, [])

    def test_inventory_boundary_refuses_table_creation_and_same_name_replacement(self):
        before = {"source": [], "target": []}
        harness.require_stable_inventories(before, before)
        for after in ({"source": [{"oid": 42}], "target": []}, {"source": [], "target": [{"oid": 43}]}, {"source": []}):
            with self.assertRaises(RuntimeError):
                harness.require_stable_inventories(before, after)
        with self.assertRaises(RuntimeError):
            harness.require_stable_inventories({"source": [{"oid": 42, "name": "items"}], "target": []}, {"source": [{"oid": 43, "name": "items"}], "target": []})

    def test_guarded_empty_array_requires_real_zero_inventories_and_lo_check(self):
        harness.validate_native_comparison([], {"source": [], "target": []}, lo_verified=True)
        for output, inventories, verified in (([], {"source": [], "target": []}, False),
                                                ([], {"source": [{"oid": 42}], "target": []}, True),
                                                ([], {"source": []}, True), (None, {"source": [], "target": []}, True)):
            with self.assertRaises(RuntimeError):
                harness.validate_native_comparison(output, inventories, lo_verified=verified)
        with self.assertRaises(RuntimeError):
            harness.validate_native_comparison([], {"source": [], "target": []}, lo_verified=True, native=False)

    def test_retained_lo_evidence_rejects_matching_but_malformed_inventory(self):
        binding = artifact_binding()
        for inventory in ([], {"90001": {}}, {"90001": {"oid": 90002, "length": 0, "sha256": "0" * 64, "owner": "owner"}}):
            stage = {"source": inventory, "target": inventory, "lo_verified": True}
            report = {"binding": binding, "cases": {"lo_only": {"initial": stage, "final": stage, "inventories": {"source": [], "target": []}}}}
            with self.assertRaises(RuntimeError):
                harness.validate_feature_receipt(report, binding, ("lo_only",))

    def test_receipts_reject_binary_image_replay_omitted_lo_and_empty_case_inventory(self):
        binding = artifact_binding()
        report = {"binding": binding, "cases": {"lo_only": {"initial": {"source": {}, "target": {}, "lo_verified": True}, "final": {"source": {}, "target": {}, "lo_verified": True}, "inventories": {"source": [], "target": []}}}}
        harness.validate_feature_receipt(report, binding, ("lo_only",))
        for actual, expected, cases in (({**report, "binding": {**binding, "binary_sha256": "0" * 64}}, binding, ("lo_only",)),
                                        ({**report, "binding": {**binding, "runner_image_id": "sha256:" + "0" * 64}}, binding, ("lo_only",)),
                                        (report, binding, ()), ({**report, "cases": {}}, binding, ("lo_only",))):
            with self.assertRaises(RuntimeError):
                harness.validate_feature_receipt(actual, expected, cases)
        for section in ("initial", "final"):
            bad = copy.deepcopy(report); del bad["cases"]["lo_only"][section]["lo_verified"]
            with self.assertRaises(RuntimeError):
                harness.validate_feature_receipt(bad, binding, ("lo_only",))
        bad = copy.deepcopy(report); bad["cases"]["lo_only"]["final"]["target"] = {"90001": {"sha256": "0" * 64}}
        with self.assertRaises(RuntimeError):
            harness.validate_feature_receipt(bad, binding, ("lo_only",))


    def test_artifact_binding_requires_clean_source_and_distinct_fresh_server_identities(self):
        self.assertTrue(callable(getattr(harness, "validate_artifact_binding", None)), "missing complete artifact binding")
        binding = artifact_binding()
        harness.validate_artifact_binding(binding)
        for change in ({"servers": []}, {"source_commit": ""}, {"source_git_tree": ""}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                harness.validate_artifact_binding({**binding, **change})
        for key, value in (("id", binding["servers"][0]["id"]), ("system_identifier", binding["servers"][0]["system_identifier"]),
                           ("system_identifier", "7705796426308472853"), ("data_directory", "/tmp/borrowed"), ("wal_level", "replica"), ("version", "18.5"), ("pgdata_permissions", "755|root")):
            bad = copy.deepcopy(binding); bad["servers"][1][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(RuntimeError):
                harness.validate_artifact_binding(bad)

    def test_source_bound_smoke_requires_actual_version_tools_library_and_debug_executable(self):
        self.assertTrue(callable(getattr(harness, "validate_build_smoke", None)), "missing source-bound debug smoke")
        binding = artifact_binding(); smoke = build_smoke(binding)
        harness.validate_build_smoke(smoke, binding, "16-18")
        for change in ({"binary_sha256": "0"*64}, {"runner_image_id": "sha256:" + "0"*64}, {"client_versions": {}}, {"linked_libpq": 180005},
                       {"version_text": "rpgmigrate 0.1.0\nlibpq 180005\n"}, {"version_json": {}}, {"packages": {}}, {"library_sha256": ""}, {"architecture": "unknown"}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                harness.validate_build_smoke({**smoke, **change}, binding, "16-18")


    def test_database_receipts_bind_each_fresh_oid_to_exact_server_and_case(self):
        self.assertTrue(callable(getattr(harness, "validate_database_identities", None)), "missing exact database identity evidence")
        binding = artifact_binding()
        cases = {name: {"databases": {role: {"name": prefix + name, "oid": 100 + number, "system_identifier": server["system_identifier"]}
                                      for role, prefix, server in zip(("source", "target"), ("src_m8_", "dst_m8_"), binding["servers"])}}
                 for number, name in enumerate(harness.CASES)}
        harness.validate_database_identities(cases, binding)
        for key, value in (("oid", 0), ("name", "borrowed"), ("system_identifier", "7705796426308472853")):
            bad = copy.deepcopy(cases); bad["binary_safe"]["databases"]["source"][key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError): harness.validate_database_identities(bad, binding)
        bad = copy.deepcopy(cases); bad["binary_fallback"]["databases"]["source"]["oid"] = 100
        with self.assertRaises(RuntimeError): harness.validate_database_identities(bad, binding)
        with self.assertRaises(RuntimeError): harness.validate_database_identities({}, binding)


    def test_progress_retains_closed_case_wire_and_validated_partial_report(self):
        self.assertTrue(hasattr(harness, "ProfileProgress"), "missing pre-cleanup M8 progress capture")
        binding = artifact_binding()
        driver = progress_driver()
        record = {"relation": ["public", "m8_safe"], "direction": "out", "range": [None, None], "format": "binary", "connection": 1,
                  "backend_pid": 123, "columns": 1, "rows": 4, "data_frames": 1, "eof": True, "complete": True, "held": False, "released": False,
                  "sql": "fixture-secret-row"}
        wire = types.SimpleNamespace(copy_evidence=lambda **_: {"invalid": True, "overflow": False, "records": [record], "raw_packet": "fixture-secret-row"},
                                     snapshot=lambda **_: {"status": "observed", "overflow": False, "gates": {key: False for key in ("armed", "held", "released", "abort_requested", "stopping")}, "connections": [], "environment": "fixture-secret-row"})
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "m8-progress.json"
            progress = harness.ProfileProgress(path, binding, "16-18")
            progress.capture("binary_safe", "wire_validation", {}, driver, (wire, wire), failed=True)
            value = json.loads(path.read_text())
            self.assertEqual(value["scope"], "m8_profile_diagnostic")
            self.assertEqual(value["current"], {"case": "binary_safe", "ordinal": 1, "stage": "wire_validation"})
            self.assertEqual(value["status"], "failed")
            self.assertEqual(value["completed_case_ids"], [])
            self.assertEqual(value["producer_report_count"], 1)
            self.assertEqual(value["producer_reports"][0]["case"], "binary_safe")
            self.assertTrue(value["wire"]["source"]["copy"]["invalid"])
            self.assertEqual(value["wire"]["source"]["copy"]["records"][0]["rows"], 4)
            self.assertNotIn("fixture-secret-row", path.read_text())
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_progress_capture_unavailable_cannot_mask_original_failure(self):
        self.assertTrue(hasattr(harness, "ProfileProgress"), "missing bounded failure capture")
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "m8-progress.json"
            progress = harness.ProfileProgress(path, artifact_binding(), "16-18")
            driver = types.SimpleNamespace(reports=[], work={})
            progress.capture("binary_safe", "clone_start", {}, driver)
            stderr = io.StringIO()
            with patch.object(harness.os, "replace", side_effect=OSError("fixture-secret-row")), redirect_stderr(stderr):
                progress.capture("binary_safe", "wire_validation", {}, driver, failed=True)
            self.assertTrue(progress.failure_attempted)
            self.assertIn("unavailable", stderr.getvalue())
            self.assertNotIn("fixture-secret-row", stderr.getvalue())
            self.assertEqual(json.loads(path.read_text())["current"]["stage"], "clone_start")
            wire = types.SimpleNamespace(copy_evidence=lambda **_: (_ for _ in ()).throw(RuntimeError("fixture-secret-row")), snapshot=lambda **_: {})
            progress.capture("binary_safe", "wire_validation", {}, driver, (wire, wire), failed=True)
            value = json.loads(path.read_text())
            self.assertEqual(value["wire"]["source"]["status"], "unavailable")
            self.assertNotIn("fixture-secret-row", path.read_text())


    def test_progress_report_and_total_caps_fail_unavailable_without_raw_data(self):
        for fault in ("report_bytes", "report_count", "total_bytes", "report_unknown_field"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                driver = progress_driver()
                if fault == "report_bytes": driver.reports[0]["raw_json"] += " "*65536
                if fault == "report_count": driver.reports *= 10
                if fault == "total_bytes":
                    driver.reports[0]["raw_json"] += " "*30000
                    driver.reports *= 9
                if fault == "report_unknown_field":
                    raw = json.loads(driver.reports[0]["raw_json"]); raw["sql"] = "fixture-secret-row"
                    driver.reports[0]["raw_json"] = json.dumps(raw)
                path = Path(temporary) / "m8-progress.json"
                progress = harness.ProfileProgress(path, artifact_binding(), "16-18")
                with redirect_stderr(io.StringIO()):
                    self.assertFalse(progress.capture("binary_safe", "wire_validation", {}, driver, failed=True))
                value = json.loads(path.read_text())
                self.assertEqual(value["capture_status"], "unavailable")
                self.assertIsNone(value["producer_report_count"])
                self.assertIsNone(value["completed_case_ids"])
                self.assertLessEqual(path.stat().st_size, 262144)
                self.assertNotIn("fixture-secret-row", path.read_text())
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "m8-progress.json"; path.write_text("old evidence")
            with self.assertRaises(RuntimeError): harness.ProfileProgress(path, artifact_binding(), "16-18")
            self.assertEqual(path.read_text(), "old evidence")

    def test_profile_failure_context_precedes_worker_and_wire_cleanup_and_survives_temp_state(self):
        binding, events = artifact_binding(), []
        inventory = [{"schema": "public", "name": "m8_safe"}]
        tables = {"public.m8_safe": {"rows": 4, "sha256": "7"*64}}
        source, target = (types.SimpleNamespace(host=role, sql=lambda *_: b"", create=lambda name: name) for role in ("source", "target"))
        driver = progress_driver(); pending = driver.reports.pop()
        driver.start = lambda *_args, **_kwargs: object()
        driver.finish = lambda _worker: driver.reports.append(pending)
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary); state = base / "temporary-state"; state.mkdir()
            diagnostics = base / "diagnostics"; diagnostics.mkdir(mode=0o700)
            path = diagnostics / "m8-progress.json"
            progress = harness.ProfileProgress(path, binding, "16-18")
            def cleanup(_worker):
                events.append("worker_cleanup")
                self.assertEqual(json.loads(path.read_text())["status"], "failed")
            driver.kill = cleanup
            class Gate:
                def __enter__(self): return self
                def __exit__(self, *_):
                    events.append("wire_cleanup")
                    self_test.assertEqual(json.loads(path.read_text())["status"], "failed")
                def connection(self, value): return value
                def copy_evidence(self, **_): return {"invalid": True, "overflow": False, "records": []}
                def snapshot(self, **_):
                    return {"status": "observed", "overflow": False, "gates": {key: False for key in ("armed", "held", "released", "abort_requested", "stopping")}, "connections": []}
            self_test = self
            with patch.object(harness, "WireGate", side_effect=lambda *_: Gate()), \
                    patch.object(harness, "database_identity", return_value={"oid": 42}), patch.object(harness, "physical_inventory", return_value=inventory), \
                    patch.object(harness, "table_fingerprint", return_value=tables), patch.object(harness, "observe_large_objects", return_value={"lo_verified": True}), \
                    patch.object(harness, "native_compare", return_value=({"source": inventory, "target": inventory}, {})), \
                    self.assertRaisesRegex(RuntimeError, "actual COPY format observation is missing or invalid"):
                harness.run_profiles(source, target, driver, state, harness.load_manifest(), progress=progress)
            import shutil
            shutil.rmtree(state)
            value = json.loads(path.read_text())
            self.assertEqual(value["current"], {"case": "binary_safe", "ordinal": 1, "stage": "wire_validation"})
            self.assertEqual(value["status"], "failed")
            self.assertEqual(value["producer_report_count"], 1)
            self.assertEqual(value["completed_case_ids"], [])
            self.assertEqual(events, ["worker_cleanup", "wire_cleanup", "wire_cleanup"])
            self.assertFalse(state.exists())


    def test_progress_wire_caps_and_unknown_scalar_values_are_explicitly_unavailable(self):
        record = {"relation": ["public", "m8_safe"], "direction": "out", "range": [None, None], "format": "binary", "connection": 1,
                  "backend_pid": 123, "columns": 1, "rows": 4, "data_frames": 1, "eof": True, "complete": True, "held": False, "released": False}
        for fault in ("records", "connections", "counter", "relation"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                selected = copy.deepcopy(record)
                if fault == "counter": selected["rows"] = "fixture-secret-row"
                if fault == "relation": selected["relation"] = ["public", "fixture-secret-row"]
                wire = types.SimpleNamespace(copy_evidence=lambda **_: {"invalid": False, "overflow": False, "records": [selected]* (257 if fault == "records" else 1)},
                    snapshot=lambda **_: {"status": "observed", "overflow": False, "gates": {key: False for key in ("armed", "held", "released", "abort_requested", "stopping")}, "connections": [{}]* (65 if fault == "connections" else 0)})
                path = Path(temporary) / "m8-progress.json"
                progress = harness.ProfileProgress(path, artifact_binding(), "16-18")
                progress.capture("binary_safe", "wire_validation", {}, progress_driver(), (wire, wire), failed=True)
                value = json.loads(path.read_text())
                self.assertEqual(value["capture_status"], "partial")
                self.assertEqual(value["wire"]["source"]["status"], "unavailable")
                self.assertNotIn("copy", value["wire"]["source"])
                self.assertNotIn("fixture-secret-row", path.read_text())


    def test_inside_m8_selects_persistent_diagnostic_mount_before_validation(self):
        with patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path("/unmounted-workspace/target/failures")):
            with self.assertRaises(RuntimeError):
                harness.inner(types.SimpleNamespace(feature_binding=None, report_probe=None))
            self.assertEqual(harness.clone.DIAGNOSTIC_ROOT, Path("/diagnostics"))


if __name__ == "__main__":
    unittest.main()
