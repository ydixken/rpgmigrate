"""Offline guards for the private owning-COPY profile."""

import importlib
import json
from pathlib import Path
import re
from types import SimpleNamespace
import tempfile
import unittest
from copy import deepcopy
from unittest.mock import patch

GROUPS = ("atomic", "empty", "admission", "rollback", "unknown", "recovery_refusal",
          "source_finish_unknown", "advisory_counts")
CASES = ["snapshot::copy_live_tests::live_copy_snapshot_" + group for group in GROUPS]
VARIANTS = {"atomic": 2, "empty": 0, "page128": 128, "page129": 129, "late_refusal": 129,
            "late_binary": 129, "format_mismatch": 2, "rollback": 2, "unknown": 2,
            "recovery": 2, "source_finish": 2, "counts": 2, "owned_table": 1, "owned_index": 1}


def initial_tables(variant):
    return 1 if variant == "atomic" else 0 if variant in ("owned_table", "owned_index") else VARIANTS[variant]


class CopySnapshotHarnessTests(unittest.TestCase):
    def fixture(self):
        try:
            return importlib.import_module("copy_snapshot_fixture")
        except ModuleNotFoundError:
            self.fail("owning-COPY fixture module is missing")

    def test_manifest_has_exact_eight_private_groups(self):
        path = Path(__file__).with_name("copy_snapshot_cases.json")
        self.assertTrue(path.is_file(), "owning-COPY inventory is missing")
        self.assertEqual(json.loads(path.read_text()), CASES)

    def test_case_loader_refuses_missing_empty_duplicate_extra_short_or_wrong_ids(self):
        module = self.fixture()
        self.assertEqual(module.copy_snapshot_cases(), CASES)
        invalid = [[], CASES[:-1], CASES + [CASES[0]], CASES[:-1] + [CASES[0]],
                   CASES[:-1] + ["snapshot::copy_live_tests::unknown"],
                   [name.rsplit("::", 1)[1] for name in CASES], None, {}]
        for value in invalid:
            with self.subTest(value=value), patch.object(Path, "read_text", return_value=json.dumps(value)), \
                    self.assertRaises(AssertionError):
                module.copy_snapshot_cases()
        with patch.object(Path, "read_text", side_effect=FileNotFoundError), self.assertRaises(FileNotFoundError):
            module.copy_snapshot_cases()

    def prepare(self, *, foreign=False, identity=("123", 42), privileges=None):
        module = self.fixture()
        calls = []
        target = {"root": "/owned-target", "token": "owned-target"}
        source = {"root": "/owned-source", "token": "owned-source", "replication_role": "copy_snapshot_nonce"}

        def query(context, database, sql, *, role="e1_admin"):
            self.assertIn(context, (target, source))
            calls.append((context is source, database, sql, role))
            if "shobj_description" in sql:
                return "foreign" if foreign else context["token"]
            if "json_build_array(rolreplication" in sql:
                return json.dumps(privileges or [True, False, False, False, False, False, False, True])
            if "json_build_array(c.system_identifier" in sql:
                return json.dumps(identity)
            if "count(*) FROM pg_class" in sql:
                return str(initial_tables(database.removeprefix("copy_")))
            return ""

        with patch.object(module, "psql", query), patch.object(module, "assert_identity"):
            result = module.prepare_copy_snapshot_fixture(target, source)
        return calls, result

    def test_fixture_has_fourteen_bounded_supported_source_inventories(self):
        calls, variants = self.prepare()
        self.assertEqual({name: value["tables"] for name, value in variants.items()}, VARIANTS)
        for name, expected in VARIANTS.items():
            database = "copy_" + name
            source_sql = " ".join(sql for source, db, sql, _ in calls if source and db == database)
            target_sql = " ".join(sql for source, _, sql, _ in calls if not source)
            tables = re.findall(r"CREATE TABLE public\.(cs_[a-z_0-9]+) ", source_sql)
            expected_tables = [f"cs_{name}_{index:03}" for index in range(initial_tables(name))]
            if expected >= 128:
                expected_tables = [name.ljust(63, "_") for name in expected_tables]
            self.assertEqual(tables, expected_tables)
            self.assertIn("PRIMARY KEY", source_sql) if initial_tables(name) else self.assertNotIn("CREATE TABLE", source_sql)
            self.assertEqual(variants[name]["initial_tables"], initial_tables(name))
            for table in tables:
                self.assertIn(f'GRANT SELECT ON public.{table} TO "copy_snapshot_nonce"', source_sql)
                self.assertIn(f"CREATE TABLE public.{table} ", target_sql)
            self.assertEqual(variants[name]["source_identity"], {"system_identifier": 123, "database_oid": 42})
        self.assertEqual(len(variants), 14)
        self.assertTrue(all(db == "e3_authority" for source, db, sql, _ in calls if not source))

    def test_large_inventory_names_expand_metadata_without_changing_rows(self):
        calls, _ = self.prepare()
        target_sql = " ".join(sql for source, _, sql, _ in calls if not source)
        for variant, count in (("page128", 128), ("page129", 129), ("late_refusal", 129), ("late_binary", 129)):
            with self.subTest(variant=variant):
                source_sql = " ".join(sql for source, db, sql, _ in calls if source and db == "copy_" + variant)
                tables = re.findall(r"CREATE TABLE public\.(cs_[a-z_0-9]+) ", source_sql)
                self.assertEqual(len(tables), count)
                self.assertEqual(len(set(tables)), count)
                for number, table in enumerate(tables):
                    self.assertEqual(len(table.encode()), 63)
                    self.assertTrue(table.startswith(f"cs_{variant}_{number:03}"))
                    self.assertIn(f"INSERT INTO public.{table} VALUES (1,7),(2,8);", source_sql)
                    self.assertIn(f"CREATE TABLE public.{table} ", target_sql)

    def test_admission_type_mismatch_is_only_the_129th_target_payload(self):
        calls, _ = self.prepare()
        target_sql = " ".join(sql for source, _, sql, _ in calls if not source)
        for variant, count in (("page128", 128), ("page129", 129), ("late_refusal", 129),
                               ("late_binary", 129), ("format_mismatch", 2)):
            with self.subTest(variant=variant):
                source_sql = " ".join(sql for source, db, sql, _ in calls
                                      if source and db == "copy_" + variant)
                pattern = rf"CREATE TABLE public\.(cs_{variant}_[a-z_0-9]+) \(([^;]+)\);"
                names = [f"cs_{variant}_{number:03}" for number in range(count)]
                if count >= 128:
                    names = [name.ljust(63, "_") for name in names]
                expected = [(name, "id integer PRIMARY KEY, payload integer") for name in names]
                self.assertEqual(re.findall(pattern, source_sql), expected)
                if variant == "late_refusal":
                    expected[-1] = (names[-1], "id integer PRIMARY KEY, payload bigint")
                self.assertEqual(re.findall(pattern, target_sql), expected)

    def test_fixture_uses_only_plain_unique_for_the_reachable_terminal_fault(self):
        calls, _ = self.prepare()
        source = " ".join(sql for is_source, _, sql, _ in calls if is_source)
        target = " ".join(sql for is_source, _, sql, _ in calls if not is_source)
        self.assertIn("INSERT INTO public.cs_rollback_001 VALUES (1,7),(2,7)", source)
        self.assertIn("CREATE UNIQUE INDEX cs_rollback_payload_unique ON public.cs_rollback_001 (payload)", target)
        for unsupported in ("CHECK (", "FOREIGN KEY", "CREATE TRIGGER", "WHERE payload"):
            self.assertNotIn(unsupported, target)
        late = "cs_late_refusal_128".ljust(63, "_")
        self.assertIn(f"{late} (id integer PRIMARY KEY, payload bigint)", target)

    def test_atomic_defers_empty_lookalike_until_canonical_name_setup(self):
        calls, variants = self.prepare()
        source = " ".join(sql for is_source, database, sql, _ in calls
                          if is_source and database == "copy_atomic")
        self.assertNotIn("CREATE TABLE public.cs_atomic_001", source)
        self.assertNotIn("INSERT INTO public.cs_atomic_001", source)
        self.assertIn("INSERT INTO public.cs_atomic_000 VALUES (1,7),(2,8)", source)
        self.assertEqual(variants["atomic"]["dynamic_qname"], "lookalike")
        self.assertEqual(variants["atomic"]["tables"], 2)
        for variant in ("owned_table", "owned_index"):
            self.assertEqual(variants[variant]["dynamic_qname"], variant)
            self.assertEqual(variants[variant]["initial_tables"], 0)
            self.assertEqual(variants[variant]["tables"], 1)

    def test_fixture_keeps_restricted_role_and_control_permission_scoped(self):
        calls, _ = self.prepare()
        roles = [sql for source, _, sql, _ in calls if source and "CREATE ROLE" in sql]
        self.assertEqual(len(roles), 1)
        self.assertIn("LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS", roles[0])
        grants = [(database, sql) for source, database, sql, _ in calls if source and "pg_control_system() TO" in sql]
        self.assertEqual({database for database, _ in grants}, {"copy_" + name for name in VARIANTS})
        self.assertTrue(all('TO "copy_snapshot_nonce"' in sql for _, sql in grants))
        self.assertTrue(all(role == "e1_target" for source, _, sql, role in calls if not source and "CREATE TABLE" in sql))

    def test_fixture_rejects_foreign_ownership_and_noninteger_identity(self):
        with self.assertRaises(AssertionError):
            self.prepare(foreign=True)
        for identity in (("123", "42"), ("0", 42), ("123", 0), ("123", True), ("123", 2**32)):
            with self.subTest(identity=identity), self.assertRaises(AssertionError):
                self.prepare(identity=identity)
        with self.assertRaises(AssertionError):
            self.prepare(privileges=[True, True, False, False, False, False, False, True])

    def test_unqualified_profile_cannot_start_a_source_cluster(self):
        module = self.fixture()
        for version, number in (("17.12", 170012), ("14.24", 140024.0), ("14.24", "140024"),
                                ("14.24", True), ("14.24", 170011)):
            with self.subTest(version=version, number=number), \
                    patch.object(module, "OwnedCluster", side_effect=AssertionError("cluster started")) as cluster:
                with self.assertRaises(AssertionError):
                    with module.copy_snapshot_fixture(SimpleNamespace(), {"server_version": version, "server_version_num": number}):
                        self.fail("unqualified owning-COPY profile started")
                cluster.assert_not_called()

    def test_factory_exports_one_owned_source_and_fourteen_distinct_slots(self):
        module = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = SimpleNamespace(context={"root": str(root), "token": "target-owner"})
            events = []

            class SourceCluster:
                def __init__(inner, manifest, *, logical, logical_slots):
                    self.assertEqual(manifest, {"server_version": "14.24", "server_version_num": 140024})
                    self.assertIs(logical, True)
                    self.assertEqual(logical_slots, 16)
                    inner.token = "c" * 32
                    inner.context = {"root": str(root / "source"), "token": inner.token,
                                     "socket_dir": str(root / "source-socket"), "port": 23456,
                                     "passfile": str(root / "source.pgpass"), "server_version_num": 140024}

                def __enter__(inner):
                    events.append("enter")
                    return inner

                def __exit__(inner, *_args):
                    events.append("exit")

            with patch.object(module, "OwnedCluster", SourceCluster), \
                    patch.object(module, "prepare_copy_snapshot_fixture", return_value={"atomic": {"tables": 2}}):
                with module.copy_snapshot_fixture(target, {"server_version": "14.24", "server_version_num": 140024}) as (context, environment):
                    path = Path(environment["RPGMIGRATE_TEST_COPY_SNAPSHOT_CONTEXT"])
                    self.assertEqual(path.parent, root)
                    self.assertEqual(path.stat().st_mode & 0o777, 0o600)
                    self.assertEqual(json.loads(path.read_text()), context)
                    self.assertEqual(context["token"], "target-owner")
                    self.assertEqual(context["copy_snapshot_source"]["replication_role"], "copy_snapshot_" + "c" * 12)
                    self.assertEqual(context["copy_snapshot_source"]["logical_slots"], 16)
                    self.assertEqual(set(context["copy_snapshot_slots"]), set(VARIANTS))
                    self.assertEqual(len(set(context["copy_snapshot_slots"].values())), 14)
                    self.assertEqual(set(environment), {"RPGMIGRATE_TEST_COPY_SNAPSHOT_CONTEXT",
                                     "RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE", "RPGMIGRATE_TEST_COPY_SNAPSHOT_EXPORTER",
                                     "RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE_ADMIN"})
                    self.assertIn("dbname='copy_atomic'", environment["RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE"])
                    self.assertIn("user='copy_snapshot_" + "c" * 12 + "'", environment["RPGMIGRATE_TEST_COPY_SNAPSHOT_SOURCE"])
                    self.assertTrue(environment["RPGMIGRATE_TEST_COPY_SNAPSHOT_EXPORTER"].endswith(" replication=database"))
            self.assertEqual(events, ["enter", "exit"])

    def test_wire_registry_has_only_finite_expected_side_case_phase_pairs(self):
        module = self.fixture()
        target = {"atomic": ("copy",), "empty": ("copy",), "page128": ("copy",), "page129": ("copy",),
                  "late_refusal": ("admission",), "late_binary": ("admission",),
                  "format_mismatch": ("admission",), "rollback": ("copy",),
                  "unknown": ("copy_unknown", "recovery"),
                  "recovery": ("setup", "missing_anchor", "foreign_anchor", "prebegin_intent", "prebegin_reader", "barrier_unknown"),
                  "source_finish": ("copy",), "counts": ("copy", "recovery"),
                  "owned_table": ("admission",), "owned_index": ("admission",)}
        self.assertEqual(module.COPY_TARGET_PHASES, target)
        self.assertEqual(module.COPY_SOURCE_PHASES, {"source_finish": ("finish_unknown",)})
        self.assertEqual(module.COPY_HOLD_PHASES, frozenset({("unknown", "copy_unknown"), ("recovery", "barrier_unknown")}))
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory}
            paths = set()
            for side, registry in (("target", module.COPY_TARGET_PHASES), ("source", module.COPY_SOURCE_PHASES)):
                for case, phases in registry.items():
                    for phase in phases:
                        path = module.copy_snapshot_receipt_path(context, side, case, phase)
                        self.assertEqual(path, Path(directory) / f"copy-snapshot-wire-{side}-{case}-{phase}.json")
                        paths.add(path)
            self.assertEqual(len(paths), 22)
            for side, case, phase in (("foreign", "atomic", "copy"), ("source", "atomic", "copy"),
                                      ("target", "../atomic", "copy"), ("target", "atomic", "unknown")):
                with self.subTest(side=side, case=case, phase=phase), self.assertRaises(AssertionError):
                    module.copy_snapshot_receipt_path(context, side, case, phase)

    def wire_context(self, directory):
        return {"root": directory, "token": "target-owner", "copy_snapshot_source": {"token": "source-owner"},
                "copy_snapshot_variants": {case: {"source_identity": {"system_identifier": 123, "database_oid": number + 42}}
                                           for number, case in enumerate(VARIANTS)}}

    def wire(self, context, side, case, phase):
        module = self.fixture()
        positive = side == "target" and phase in ("copy", "setup", "copy_unknown")
        count = VARIANTS[case] if positive else 0
        tags = ["COPY 2"] * count
        if case == "atomic":
            tags = ["COPY 0", "COPY 2"]
        if case == "rollback":
            count, tags = 2, ["COPY 2"]
        hold = side == "target" and (case, phase) in module.COPY_HOLD_PHASES
        writing = positive and case != "rollback" or phase in ("recovery", "barrier_unknown")
        value = {"owner": context["token"], "profile": "copy_snapshot", "side": side,
                 "case": case, "phase": phase, "status": "observed", "backend_pid": 101, "helper_pid": 201,
                 "native_path": str(Path(context["root"]) / ("owning-copy-" + case) / "manifest.json"),
                 "stream": {"run": [7] * 16, "source": context["copy_snapshot_variants"][case]["source_identity"], "generation": 9},
                 "armed": True, "copy_in_starts": count, "client_copy_bytes": 20 if count and case != "empty" else 0,
                 "copy_complete_tags": tags, "error_sqlstates": ["23505"] if case == "rollback" else [],
                 "commit_tags": ["COMMIT"] if writing else [], "update_tags": ["UPDATE 1"] if writing else [],
                 "fault_hits": 1 if hold else 0, "client_closed": True, "backend_closed": True,
                 "held_fence_observed": hold, "released_fence_observed": hold,
                 "deadline_ms": 30000, "elapsed_ms": 10, "helper_reaped": True, "helper_returncode": 0}
        if side == "source":
            value.update(copy_in_starts=0, copy_complete_tags=[], client_copy_bytes=0, commit_tags=["COMMIT"],
                         update_tags=[], error_sqlstates=[], fault_hits=1, source_owner="source-owner",
                         peer_pid=102, peer_closed=True, imported=True)
        return value

    def test_every_wire_phase_requires_exact_observations_and_real_helper_exit(self):
        module = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            context = self.wire_context(directory)
            for side, registry in (("target", module.COPY_TARGET_PHASES), ("source", module.COPY_SOURCE_PHASES)):
                for case, phases in registry.items():
                    for phase in phases:
                        with self.subTest(side=side, case=case, phase=phase):
                            value = self.wire(context, side, case, phase)
                            module.validate_copy_snapshot_phase(value, context, side, case, phase)
                            pending = deepcopy(value)
                            pending.update(helper_reaped=False, helper_returncode=None)
                            module.validate_copy_snapshot_phase(pending, context, side, case, phase, reaped=False)
                            with self.assertRaises(AssertionError):
                                module.validate_copy_snapshot_phase(pending, context, side, case, phase)
                            for key in value:
                                missing = deepcopy(value)
                                del missing[key]
                                with self.subTest(missing=key), self.assertRaises(AssertionError):
                                    module.validate_copy_snapshot_phase(missing, context, side, case, phase)
                            invalid = [("extra", True), ("status", "pending"), ("armed", False),
                                       ("client_closed", False), ("backend_closed", False), ("helper_returncode", 1),
                                       ("backend_pid", True), ("helper_pid", 0), ("elapsed_ms", 30001),
                                       ("copy_in_starts", True), ("client_copy_bytes", True), ("native_path", "/foreign/manifest.json"),
                                       ("commit_tags", ["COMMIT\u0000"]), ("update_tags", ["UPDATE 01"])]
                            for key, changed in invalid:
                                bad = deepcopy(value)
                                bad[key] = changed
                                with self.subTest(changed=key), self.assertRaises(AssertionError):
                                    module.validate_copy_snapshot_phase(bad, context, side, case, phase)

    def test_refusal_receipts_reject_copy_before_admission_or_a_read_only_recovery(self):
        module = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            context = self.wire_context(directory)
            for case in ("late_refusal", "late_binary", "format_mismatch", "owned_table", "owned_index"):
                value = self.wire(context, "target", case, "admission")
                for key, mutation in (("copy_in_starts", 1), ("copy_complete_tags", ["COPY 0"]),
                                      ("update_tags", ["UPDATE 1"]), ("commit_tags", ["COMMIT"])):
                    bad = deepcopy(value)
                    bad[key] = mutation
                    with self.subTest(case=case, key=key), self.assertRaises(AssertionError):
                        module.validate_copy_snapshot_phase(bad, context, "target", case, "admission")
            for case in ("unknown", "counts"):
                value = self.wire(context, "target", case, "recovery")
                value["update_tags"] = []
                with self.subTest(case=case), self.assertRaises(AssertionError):
                    module.validate_copy_snapshot_phase(value, context, "target", case, "recovery")

    def write_wires(self, context):
        module = self.fixture()
        result = {}
        for side, registry in (("target", module.COPY_TARGET_PHASES), ("source", module.COPY_SOURCE_PHASES)):
            for case, phases in registry.items():
                for phase in phases:
                    value = self.wire(context, side, case, phase)
                    value.update(backend_pid=300 + len(result), helper_pid=600 + len(result))
                    path = module.copy_snapshot_receipt_path(context, side, case, phase)
                    path.write_text(json.dumps(value))
                    result[(side, case, phase)] = path
        return result

    def test_aggregate_requires_every_exact_wire_file_and_rejects_duplicate_json_or_helper(self):
        module = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            context = self.wire_context(directory)
            paths = self.write_wires(context)
            module.validate_copy_snapshot_receipts(context)
            missing = paths[("target", "empty", "copy")]
            data = missing.read_bytes()
            missing.unlink()
            with self.assertRaises(AssertionError):
                module.validate_copy_snapshot_receipts(context)
            missing.write_bytes(data)
            extra = Path(directory) / "copy-snapshot-wire-target-extra-copy.json"
            extra.write_bytes(data)
            with self.assertRaises(AssertionError):
                module.validate_copy_snapshot_receipts(context)
            extra.unlink()
            missing.write_text(data.decode().replace('"owner": "target-owner"', '"owner":"foreign","owner":"target-owner"', 1))
            with self.assertRaises(AssertionError):
                module.validate_copy_snapshot_receipts(context)
            missing.write_bytes(data)
            value = json.loads(data)
            value["helper_pid"] = 600
            missing.write_text(json.dumps(value))
            with self.assertRaises(AssertionError):
                module.validate_copy_snapshot_receipts(context)

    def test_source_finish_and_recovery_cannot_borrow_a_foreign_receipt(self):
        module = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            context = self.wire_context(directory)
            paths = self.write_wires(context)
            for key in (("source", "source_finish", "finish_unknown"), ("target", "unknown", "recovery"),
                        ("target", "counts", "recovery")):
                path = paths[key]
                original = path.read_bytes()
                value = json.loads(original)
                value["native_path"] = str(Path(directory) / "owning-copy-foreign" / "manifest.json")
                path.write_text(json.dumps(value))
                with self.subTest(key=key), self.assertRaises(AssertionError):
                    module.validate_copy_snapshot_receipts(context)
                path.write_bytes(original)
            path = paths[("target", "unknown", "recovery")]
            value = json.loads(path.read_text())
            value["backend_pid"] = json.loads(paths[("target", "unknown", "copy_unknown")].read_text())["backend_pid"]
            path.write_text(json.dumps(value))
            with self.assertRaises(AssertionError):
                module.validate_copy_snapshot_receipts(context)

    def test_wire_source_identity_requires_exact_canonical_integer_fields(self):
        module = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            context = self.wire_context(directory)
            context["copy_snapshot_variants"]["atomic"]["source_identity"] = {"system_identifier": 1, "database_oid": 1}
            original = self.wire(context, "target", "atomic", "copy")
            invalid = [
                {"system_identifier": 1, "database_oid": 1.0},
                {"system_identifier": 1.0, "database_oid": 1},
                {"system_identifier": True, "database_oid": 1},
                {"system_identifier": 1, "database_oid": True},
                {"system_identifier": 1}, {"database_oid": 1},
                {"system_identifier": 1, "database_oid": 1, "extra": 0},
                {"system_identifier": 0, "database_oid": 1},
                {"system_identifier": 1, "database_oid": 0},
                {"system_identifier": -1, "database_oid": 1},
                {"system_identifier": 1, "database_oid": -1},
                {"system_identifier": 2**64, "database_oid": 1},
                {"system_identifier": 1, "database_oid": 2**32}, None, [],
            ]
            for identity in invalid:
                value = deepcopy(original)
                value["stream"]["source"] = identity
                with self.subTest(identity=identity), self.assertRaises(AssertionError):
                    module.validate_copy_snapshot_phase(value, context, "target", "atomic", "copy")
            context["copy_snapshot_variants"]["atomic"]["source_identity"] = {"system_identifier": 2**64 - 1, "database_oid": 2**32 - 1}
            module.validate_copy_snapshot_phase(self.wire(context, "target", "atomic", "copy"), context, "target", "atomic", "copy")


if __name__ == "__main__":
    unittest.main(verbosity=2)
