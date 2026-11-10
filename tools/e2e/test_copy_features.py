"""Portable negative controls for candidate M8 evidence, without runtime claims."""

import copy
import hashlib
import json
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
        binding = {"binary_sha256": "a" * 64, "source_tree_sha256": "b" * 64, "runner_image_id": "sha256:" + "c" * 64,
                   "server_image_ids": ["sha256:" + "d" * 64, "sha256:" + "e" * 64], "run": "f" * 32}
        for inventory in ([], {"90001": {}}, {"90001": {"oid": 90002, "length": 0, "sha256": "0" * 64, "owner": "owner"}}):
            stage = {"source": inventory, "target": inventory, "lo_verified": True}
            report = {"binding": binding, "cases": {"lo_only": {"initial": stage, "final": stage, "inventories": {"source": [], "target": []}}}}
            with self.assertRaises(RuntimeError):
                harness.validate_feature_receipt(report, binding, ("lo_only",))

    def test_receipts_reject_binary_image_replay_omitted_lo_and_empty_case_inventory(self):
        binding = {"binary_sha256": "a" * 64, "source_tree_sha256": "b" * 64, "runner_image_id": "sha256:" + "c" * 64,
                   "server_image_ids": ["sha256:" + "d" * 64, "sha256:" + "e" * 64], "run": "f" * 32}
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


if __name__ == "__main__":
    unittest.main()
