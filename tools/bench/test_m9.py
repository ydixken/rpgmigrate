"""Offline controls for the approval-gated M9 COPY A/A driver."""

import importlib.util
import copy
from contextlib import ExitStack
import json
import os
import shutil
import stat
import struct
import sys
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


if sys.flags.optimize:
    raise RuntimeError("optimized Python cannot run benchmark controls")

SPEC = importlib.util.spec_from_file_location("m9_benchmark", Path(__file__).with_name("m9.py"))
bench = importlib.util.module_from_spec(SPEC) if Path(SPEC.origin).is_file() else None
if bench is not None:
    SPEC.loader.exec_module(bench)


class BenchmarkTests(unittest.TestCase):
    def packet(self, root):
        root = root.resolve()
        def save(name, value):
            path = root / name
            path.write_bytes(bench.encode(value))
            path.chmod(0o600)
            return path

        def record(path):
            return {"path": str(path), "sha256": bench.file_hash(path)}

        binary = root / "noop"
        binary.write_text("#!/bin/sh\nexit 0\n")
        binary.chmod(0o700)
        export = save("source-export", {"synthetic": True})
        empty = root / "empty-patch"
        empty.write_bytes(b"")
        auth = save("passfile", "secret-canary")
        password = root / "password"
        password.write_bytes(b"0" * 64)
        password.chmod(0o600)
        def private_ref(path):
            info = path.stat()
            identity = {"uid": info.st_uid, "mode": stat.S_IMODE(info.st_mode),
                        "device": info.st_dev, "inode": info.st_ino, "size": info.st_size}
            return {"path": str(path), "host": identity, "runner": identity}
        profile = {"copy_format": 0, "table_jobs": 4, "split_threshold": 0,
                   "native_barrier_interval_ms": None, "cache_policy": bench.CACHE_POLICY,
                   "auth": {side: {"passfile": private_ref(auth), "password_file": private_ref(password)} for side in ("source", "target")}}
        evidence = save("evidence", {"synthetic": "only tests"})
        wire = bench.file_hash(bench.ROOT / "tools/e2e/follow_wire.py")
        engines = {}
        for engine in ("native", "fork"):
            commit = bench.NATIVE_SOURCE if engine == "native" else bench.FORK_SOURCE
            proof = save(engine + "-correctness", {"schema_version": 1, "kind": "m9-benchmark-correctness-receipt",
                "engine": engine, "source_commit": commit, "binary_sha256": bench.file_hash(binary),
                "fixture_sha256": bench.digest(bench.encode(bench.load_fixture())), "profile_sha256": bench.digest(bench.encode(profile)),
                "libpq_sha256": bench.file_hash(export), "wire_sha256": wire, "executed_at": 1,
                "cases": [{"case": name, "observed": True, "outcome": "passed", "evidence": record(evidence)} for name in bench.CORRECTNESS_CASES]})
            engines[engine] = {"binary": record(binary), "source": {"commit": commit, "tree": bench.NATIVE_TREE if engine == "native" else bench.FORK_TREE,
                "export": record(export), "patch": record(empty)},
                "build": {"compiler": "synthetic", "target": "synthetic", "flags": [], "cargo_lock": record(export) if engine == "native" else None},
                "libpq": {**record(export), "pq_version": 180001}, "qualification": record(proof)}
        image = {"reference": "localhost/synthetic@sha256:" + "a" * 64, "index_digest": None,
                 "manifest_digest": "sha256:" + "a" * 64, "config_digest": "sha256:" + "b" * 64, "platform": "linux/amd64"}
        lock = {"schema_version": 1, "kind": "m9-benchmark-artifact-lock", **engines,
                "server_image": image, "runner_image": image, "runtime": {**record(binary), "version": "podman 5.0"},
                "tools": {name: {**record(binary), "version": "18.1"} for name in ("psql", "pg_dump", "pg_restore", "pg_config")},
                "driver": {"files": [record(bench.ROOT / name) for name in ("tools/bench/m9.py", "tools/e2e/clone.py", "tools/e2e/follow_wire.py", "tests/support/protocol_fault.py")]},
                "profile": profile, "qualification": {"copy_wire_observer_sha256": wire, "required_cases": list(bench.CORRECTNESS_CASES)}}
        artifacts = save("artifacts.json", lock)
        bench.prepare(root / "prepared", artifacts, run_id="1" * 32)
        manifest = root / "prepared/manifest.json"
        approval = root / "approval.json"
        lease = save("lease.json", {"schema_version": 1, "kind": "m9-benchmark-lease", "lease_id": "2" * 32,
            "run_id": "1" * 32, "phase": bench.PHASE, "started": 1000, "deadline": 8200, "exclusive": True,
            "quiet_window": {"m6_m7_idle": True, "original_commitments_preserved": True, "host_cargo_idle": True, "other_heavy_work_idle": True},
            "host": {"identity": "synthetic-host", "cpus": 14, "memory_bytes": 32 * bench.GIB},
            "original_services": [{"id": "c" * 64, "running": True, "cpus": 2, "memory_bytes": 4 * bench.GIB}],
            "campaign_disk_free_bytes": 80 * bench.GIB})
        receipts = root / "receipts"
        packet = save("prepared/packet.json", {"schema_version": 1, "kind": "m9-benchmark-packet",
            "manifest_sha256": bench.file_hash(manifest), "schedule_sha256": bench.file_hash(root / "prepared/schedule.json"),
            "artifact_lock_sha256": bench.file_hash(artifacts),
            "execution": {"phase": bench.PHASE, "argv": [sys.executable, str(bench.ROOT / "tools/bench/m9.py"), "run",
                "--approval", str(approval), "--manifest", str(manifest), "--artifacts", str(artifacts), "--phase", bench.PHASE,
                "--lease", str(lease), "--receipt-root", str(receipts)], "original_lease_sha256": bench.file_hash(lease),
                "deadline": 8200, "receipt_root": str(receipts)},
            "ownership": {"owner": "1" * 32, "original_services": bench.read_json(lease)["original_services"],
                "network_name": "m9-" + "1" * 32, "labels": {"io.rpgmigrate.m9": "1" * 32}, "retain_paths": bench.retained_paths(receipts, "1" * 32),
                "host_lock": bench.host_coordination_paths()},
            "reset": {"fresh_child_databases": True, "slots": [], "publications": []},
            "cleanup": {"containers": True, "network": True, "delete_retained": False}})
        save("approval.json", {"schema_version": 1, "kind": "m9-benchmark-approval", "decision": "approved",
            "packet_sha256": bench.file_hash(packet), "authorized_at": 999, "scope": bench.SCOPE})
        return approval, manifest, artifacts, lease, receipts

    def test_prepare_has_forty_balanced_fresh_children_and_early_checkpoint(self):
        self.assertIsNotNone(bench, "approval-gated benchmark driver is missing")
        schedule = bench.make_schedule("1" * 32, 20261110)
        self.assertEqual(len(schedule), 40)
        self.assertEqual([(child["engine"], child["shape"]) for child in schedule[:4]],
                         [("native", "narrow")] * 2 + [("fork", "narrow")] * 2)
        self.assertEqual(schedule, bench.make_schedule("1" * 32, 20261110))
        self.assertEqual(len({child["child_id"] for child in schedule}), 40)
        self.assertEqual(len({child[side] for child in schedule for side in ("source_db", "target_db")}), 80)
        for engine in ("native", "fork"):
            for shape in ("narrow", "wide"):
                children = [child for child in schedule if child["engine"] == engine and child["shape"] == shape]
                self.assertEqual(len(children), 10)
                self.assertEqual([child["sample"] for child in children].count("A"), 5)
                self.assertEqual([child["sample"] for child in children].count("B"), 5)
                first = [children[index]["sample"] for index in range(0, 10, 2)]
                self.assertIn(first.count("A"), (2, 3))

    def test_prepare_is_private_no_clobber_and_never_starts_a_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            lock = self.packet(root)[2]
            with patch("subprocess.Popen", side_effect=AssertionError("prepare launched a process")):
                manifest = bench.prepare(root / "packet", lock, run_id="1" * 32)
            self.assertEqual(manifest["stop_policy"]["children"], 40)
            self.assertEqual(stat.S_IMODE((root / "packet").stat().st_mode), 0o700)
            for path in (root / "packet").iterdir():
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            with self.assertRaises(FileExistsError):
                bench.prepare(root / "packet", lock, run_id="1" * 32)

    def test_fixture_missing_empty_altered_or_duplicate_fields_refuse(self):
        fixture = bench.load_fixture()
        self.assertEqual(fixture["shapes"]["narrow"]["rows_per_table"], 65536)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for path in bench.FIXTURES.iterdir():
                shutil.copyfile(path, root / path.name)
            for change in ("empty", "missing", "altered", "duplicate"):
                with self.subTest(change=change):
                    schema = root / "schema.sql"
                    schema.write_bytes((bench.FIXTURES / "schema.sql").read_bytes())
                    (root / "m9.json").write_bytes((bench.FIXTURES / "m9.json").read_bytes())
                    if change == "missing":
                        schema.unlink()
                    elif change == "empty":
                        schema.write_bytes(b"")
                    elif change == "altered":
                        schema.write_bytes(schema.read_bytes() + b"SELECT 1;\n")
                    else:
                        text = (root / "m9.json").read_text()
                        (root / "m9.json").write_text(text.replace('"schema_version": 1', '"schema_version": 1,"schema_version": 1'))
                    with self.assertRaises((RuntimeError, OSError)):
                        bench.load_fixture(root / "m9.json")

    def test_malformed_json_and_nonfinite_fields_refuse_without_echoing_secrets(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "input"
            for content in ('{"secret":"password=canary",', '{"a":1,"a":2}', '{"a":NaN}'):
                path.write_text(content)
                with self.assertRaises(RuntimeError) as error:
                    bench.read_json(path)
                self.assertNotIn("canary", str(error.exception))

    def test_run_without_explicit_approval_never_launches_even_with_noop_artifact(self):
        with patch("subprocess.Popen", side_effect=AssertionError("unapproved child")):
            with self.assertRaisesRegex(RuntimeError, "approval"):
                bench.run_phase(None, None, None, None, None)

    def test_positive_hash_bound_admission_requires_original_unexpired_exclusive_lease(self):
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.packet(Path(temporary))
            admitted = bench.admit(*paths, now=1001)
            self.assertEqual(len(admitted["schedule"]), 40)
            for now in (999, 8200, 8201):
                with self.subTest(now=now), self.assertRaisesRegex(RuntimeError, "lease"):
                    bench.admit(*paths, now=now)

    def test_changed_artifact_or_unsupported_version_cannot_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.packet(Path(temporary))
            Path(temporary, "noop").write_text("#!/bin/sh\nexit 1\n")
            with patch("subprocess.Popen", side_effect=AssertionError("tampered child")):
                with self.assertRaisesRegex(RuntimeError, "artifact identity"):
                    bench.admit(*paths, now=1001)

    def test_hash_bound_but_missing_or_unobserved_correctness_case_refuses(self):
        for mutation in ("empty", "missing", "unobserved"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                paths = self.packet(root)
                lock = bench.read_json(paths[2])
                proof_path = root / "native-correctness"
                proof = bench.read_json(proof_path)
                if mutation == "empty":
                    proof["cases"] = []
                elif mutation == "missing":
                    proof["cases"].pop()
                else:
                    proof["cases"][0]["observed"] = False
                proof_path.write_bytes(bench.encode(proof))
                lock["native"]["qualification"]["sha256"] = bench.file_hash(proof_path)
                paths[2].write_bytes(bench.encode(lock))
                with self.assertRaisesRegex(RuntimeError, "correctness"):
                    bench.validate_artifacts(paths[2])

    def resources(self):
        return {"disk_free_bytes": 40 * bench.GIB, "phase_growth_bytes": 0, "campaign_growth_bytes": 0,
                "source_pg_wal_bytes": 0, "target_pg_wal_bytes": 0, "slot_wal_bytes": 0,
                "swap_bytes": 0, "oom_events": 0, "memory_pressure_events": 0,
                "roles": {role: {"requested_cpus": 2, "requested_memory_bytes": 4 * bench.GIB,
                    "requested_swap_bytes": 0, "effective_cpu_max": [200000, 100000],
                    "effective_memory_max": 4 * bench.GIB, "effective_swap_max": 0}
                    for role in ("source", "target", "runner")}}

    def test_missing_observations_or_any_disk_wal_swap_effective_limit_violation_fails(self):
        bench.validate_resources(self.resources(), initial=True)
        for key, value in (("disk_free_bytes", 19 * bench.GIB), ("phase_growth_bytes", 12 * bench.GIB + 1),
                ("campaign_growth_bytes", 24 * bench.GIB + 1), ("source_pg_wal_bytes", 2 * bench.GIB + 1),
                ("target_pg_wal_bytes", 2 * bench.GIB + 1), ("slot_wal_bytes", bench.GIB + 1),
                ("swap_bytes", 1), ("oom_events", 1), ("memory_pressure_events", 1)):
            value = {**self.resources(), key: value}
            with self.subTest(key=key), self.assertRaisesRegex(RuntimeError, "resource"):
                bench.validate_resources(value)
        for mutation in ("missing", "unlimited", "wrong_swap", "requested_only"):
            value = self.resources()
            if mutation == "missing":
                value.pop("slot_wal_bytes")
            elif mutation == "unlimited":
                value["roles"]["runner"]["effective_cpu_max"] = ["max", 100000]
            elif mutation == "wrong_swap":
                value["roles"]["source"]["effective_swap_max"] = 1
            else:
                value["roles"]["target"].pop("effective_memory_max")
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                bench.validate_resources(value)

    def test_empty_readonly_wrong_tag_warning_or_unclean_barrier_fails(self):
        good = {"synchronous_commit": "local", "logged_probe": True, "update_tag": "UPDATE 1",
                "commit_tag": "COMMIT", "diagnostics_empty": True, "closed": True}
        bench.validate_barrier(good)
        for key, value in (("synchronous_commit", "off"), ("logged_probe", False), ("update_tag", "UPDATE 0"),
                           ("commit_tag", ""), ("diagnostics_empty", False), ("closed", False)):
            with self.subTest(key=key), self.assertRaisesRegex(RuntimeError, "barrier"):
                bench.validate_barrier({**good, key: value})

    def completed(self, paths):
        context = bench.admit(*paths, now=1001)
        root = bench.private_root(paths[4], create=True)
        for name, path in (("approval", paths[0]), ("lease", paths[3]), ("packet", paths[1].parent / "packet.json")):
            bench.publish_bytes(root / (name + ".json"), path.read_bytes())
        bindings = []
        for index, child in enumerate(context["schedule"]):
            fingerprints = {name: {"rows": count, "sha256": "d" * 64} for name, count in bench.expected_rows(child["shape"]).items()}
            fingerprints["schema"] = "e" * 64
            observed = [{"name": name, "rows": count, "oid": i + 100,
                         "canonical_text_bytes": 4 if name == "probe" else count * (context["manifest"]["fixture"]["shapes"][child["shape"]]["text_bytes_per_row"] + 32),
                         "heap_bytes": count * 100, "index_bytes": 0}
                        for i, (name, count) in enumerate(bench.expected_rows(child["shape"]).items())]
            receipt = {"schema_version": 1, "kind": "m9-benchmark-receipt", "run_id": "1" * 32,
                "child_id": child["child_id"], **context["hashes"], "started": 1001 + index * 2,
                "finished": 1002 + index * 2, "status": "success",
                "measurements": {**{key: child[key] for key in ("engine", "shape", "pair_index", "sample")},
                    "pid": 1000 + index, "start_identity": str(100 + index), "clone_elapsed_seconds": 0.5,
                    "child_elapsed_seconds": 1, "canonical_text_bytes": sum(table["canonical_text_bytes"] for table in observed),
                    "wire_bytes": None, "peak_tree_rss_bytes": 10000, "wait4_rss_bytes": 10000,
                    "cpu_user_seconds": 0.1, "cpu_system_seconds": 0.1, "timer_resolution_seconds": 0.001},
                "oracles": {"source": fingerprints, "target": fingerprints, "fixture_observed": observed,
                    "runtime": {"servers": {side: {"server_version_num": 180001, "fsync": "on", "full_page_writes": "on", "wal_level": "logical",
                        "max_slot_wal_keep_size": "1GB", "max_wal_size": "1GB", "server_encoding": "UTF8", "system_identifier": "123456789"}
                        for side in ("source", "target")}, "client": {"tools": {name: "18.1" for name in ("psql", "pg_dump", "pg_restore", "pg_config")},
                        "libpq_sha256": context["artifacts"][child["engine"]]["libpq"]["sha256"], "pq_version": 180001}},
                    "wire": {side: {"invalid": False, "overflow": False, "records": [
                        {"relation": ["m9", name], "direction": direction, "format": "text", "range": [None, None],
                         "columns": 2 if name == "probe" else 3, "rows": count, "complete": True, "eof": True, "backend_pid": 100}
                        for name, count in bench.expected_rows(child["shape"]).items()]}
                        for side, direction in (("source", "out"), ("target", "in"))},
                    "barrier": {"synchronous_commit": "local", "logged_probe": True, "update_tag": "UPDATE 1",
                        "commit_tag": "COMMIT", "diagnostics_empty": True, "closed": True},
                    "completion": {"workers_stopped": True, "postdata": True, "sequences": True,
                        "analyze": True, "synchronization": True, "closed": True}},
                "resources": [self.resources()], "cleanup": {"workers_stopped": True,
                    "databases": [{"side": side, "name": child[side + "_db"], "oid": 100,
                        "identity_checked": True, "status": "removed"} for side in ("source", "target")]}}
            bench.publish(root / (child["child_id"] + ".json"), receipt)
            bindings.append({"child_id": child["child_id"], "sha256": bench.digest(bench.encode(receipt))})
        phase = {"schema_version": 1, "kind": "m9-benchmark-phase-receipt", **context["hashes"],
            "manifest_sha256": bench.file_hash(paths[1]), "lease": context["lease"], "children": bindings,
            "status": "success", "runtime": {"host": context["lease"]["host"], "kernel": "test-kernel", "filesystem": "test-filesystem", "storage": "test-disk",
                "images": {role: {key: context["artifacts"][role + "_image"][key] for key in ("manifest_digest", "config_digest", "platform")} for role in ("server", "runner")},
                "runtime_version": "podman 5.0", "initial_vm_free_bytes": 40 * bench.GIB, "admission_resources": self.resources(),
                "host_lock_identity": {"path": bench.host_coordination_paths()["path"], "uid": os.getuid(), "mode": 0o600, "device": 1, "inode": 1, "links": 1}},
            "cleanup": {"containers": [{"role": role, "id": value * 64,
                "ownership_checked": True, "absent": True} for role, value in (("source", "a"), ("target", "b"), ("runner", "c"))],
                "network": {"ownership_checked": True, "absent": True}, "original_services_unchanged": True,
                "retained_paths": bench.retained_paths(root, "1" * 32)}}
        bench.publish(root / "phase.json", phase)
        return context, phase

    def test_summary_requires_all_forty_exact_receipts_and_clean_owned_cleanup(self):
        for mutation in ("none", "early_checkpoint_only", "missing", "tampered", "failed_cleanup", "readonly_barrier", "reused_process", "missing_wire"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                paths = self.packet(Path(temporary))
                context, phase = self.completed(paths)
                root = paths[4]
                first = root / (context["schedule"][0]["child_id"] + ".json")
                if mutation == "early_checkpoint_only":
                    phase["children"] = phase["children"][:4]
                elif mutation == "missing":
                    first.unlink()
                elif mutation == "tampered":
                    first.write_bytes(first.read_bytes() + b" ")
                elif mutation == "failed_cleanup":
                    phase["cleanup"]["containers"][0]["absent"] = False
                elif mutation in ("readonly_barrier", "reused_process", "missing_wire"):
                    receipt = bench.read_json(first)
                    if mutation == "readonly_barrier":
                        receipt["oracles"]["barrier"]["update_tag"] = "UPDATE 0"
                    elif mutation == "missing_wire":
                        receipt["oracles"]["wire"]["target"]["records"] = []
                    else:
                        next_path = root / (context["schedule"][1]["child_id"] + ".json")
                        second = bench.read_json(next_path)
                        receipt["measurements"]["pid"] = second["measurements"]["pid"]
                        receipt["measurements"]["start_identity"] = second["measurements"]["start_identity"]
                    first.write_bytes(bench.encode(receipt))
                    phase["children"][0]["sha256"] = bench.file_hash(first)
                (root / "phase.json").write_bytes(bench.encode(phase))
                if mutation == "none":
                    result = bench.summarize(paths[1], paths[2], root)
                    self.assertEqual(result["completed"], 40)
                else:
                    with self.assertRaises((RuntimeError, OSError)):
                        bench.summarize(paths[1], paths[2], root)
                    self.assertFalse((root / "summary.json").exists())

    def test_local_copy_observer_requires_actual_format_complete_bound_relation_and_exact_sql(self):
        Observer, _Gate = bench.copy_observer_types("narrow")
        for query in (b'COPY (SELECT "id", "value", "payload" FROM ONLY "m9"."data_1") TO STDOUT WITH (FORMAT text)\0',
                      b'copy (SELECT id, value, payload FROM ONLY m9.data_1) to stdout\0'):
            observer = Observer(1)
            observer.backend_pid = 123
            observer.copy_frontend(b'Q', query)
            observer.copy_backend(b'H', b'\0\0\3' + b'\0\0' * 3)
            observer.copy_backend(b'd', b"secret-row-canary\n")
            observer.copy_backend(b'c', b"")
            observer.copy_backend(b'C', b"COPY 65536\0")
            evidence = observer.copy_evidence()
            self.assertFalse(evidence["invalid"])
            self.assertEqual(evidence["records"][0]["rows"], 65536)
            self.assertNotIn("secret-row-canary", json.dumps(evidence))
        for mutation in ("none", "binary", "wrong_relation", "unknown_option", "partial", "unbound", "stale"):
            observer = Observer(1)
            observer.backend_pid = 123
            query = b'copy m9.data_1(id, value, payload) from stdin with (freeze)\0'
            if mutation == "wrong_relation": query = query.replace(b'data_1', b'other')
            if mutation == "unknown_option": query = query.replace(b'freeze', b'freeze, header')
            if mutation != "unbound": observer.copy_frontend(b'Q', query)
            if mutation == "stale": observer.copy_frontend(b'Q', b'SELECT 1\0')
            observer.copy_backend(b'G', bytes([mutation == "binary", 0, 3]) + b'\0\0' * 3)
            observer.copy_frontend(b'd', b"secret-row-canary\n")
            if mutation != "partial": observer.copy_frontend(b'c', b"")
            observer.copy_backend(b'C', b"COPY 65536\0")
            with self.subTest(mutation=mutation):
                self.assertEqual(observer.copy_evidence()["invalid"], mutation != "none")

    def test_native_extended_copy_requires_exact_parse_bind_describe_execute_sync_and_actual_response(self):
        Observer, _Gate = bench.copy_observer_types("narrow")
        query = b'COPY "m9"."data_1" ("id", "value", "payload") FROM STDIN WITH (FORMAT text)\0'
        messages = [(b'P', b'\0' + query + b'\0\0'), (b'B', b'\0\0\0\0\0\0\0\1\0\0'),
                    (b'D', b'P\0'), (b'E', b'\0\0\0\0\0'), (b'S', b'')]
        for mutation in ("none", "missing_bind", "wrong_describe", "wrong_execute", "wrong_format"):
            observer = Observer(1)
            observer.backend_pid = 123
            for kind, payload in messages:
                if mutation == "missing_bind" and kind == b'B': continue
                if mutation == "wrong_describe" and kind == b'D': payload = b'S\0'
                if mutation == "wrong_execute" and kind == b'E': payload = b'\0\0\0\0\1'
                observer.copy_frontend(kind, payload)
            observer.copy_backend(b'G', bytes([mutation == "wrong_format", 0, 3]) + b'\0\0' * 3)
            observer.copy_frontend(b'd', b"never-retained-row\n")
            observer.copy_frontend(b'c', b"")
            observer.copy_backend(b'C', b"COPY 65536\0")
            with self.subTest(mutation=mutation):
                self.assertEqual(observer.copy_evidence()["invalid"], mutation != "none")

    def test_fresh_process_capture_keeps_pid_identity_rss_wait4_and_no_secret_diagnostics(self):
        results = []
        with ExitStack() as stack:
            if sys.platform != "linux":
                stack.enter_context(patch.object(bench, "process_snapshot", side_effect=lambda pid: {"start_identity": str(pid)}))
                stack.enter_context(patch.object(bench, "process_tree_rss", return_value=1000000))
                stack.enter_context(patch.object(bench, "process_group_members", return_value=[]))
            for _ in range(2):
                result = bench.measured_process([sys.executable, "-c", "import time; x=bytearray(1000000); time.sleep(.03); print('secret-row-canary')"],
                    dict(os.environ), timeout=2)
                results.append(result)
        for result in results:
            self.assertGreater(result["pid"], 0)
            self.assertTrue(result["start_identity"])
            self.assertGreater(result["peak_tree_rss_bytes"], 0)
            self.assertGreater(result["wait4_rss_bytes"], 0)
            self.assertEqual(result["returncode"], 0)
            self.assertNotIn("secret-row-canary", json.dumps({key: value for key, value in result.items() if key not in ("stdout", "stderr")}))
        self.assertNotEqual((results[0]["pid"], results[0]["start_identity"]), (results[1]["pid"], results[1]["start_identity"]))

    def test_child_absolute_timeout_kills_only_its_fresh_process(self):
        with ExitStack() as stack:
            if sys.platform != "linux":
                stack.enter_context(patch.object(bench, "process_snapshot", side_effect=lambda pid: {"start_identity": str(pid)}))
                stack.enter_context(patch.object(bench, "process_tree_rss", return_value=1000000))
                stack.enter_context(patch.object(bench, "process_group_members", return_value=[]))
            with self.assertRaisesRegex(RuntimeError, "deadline"):
                bench.measured_process([sys.executable, "-c", "import time; time.sleep(10)"], dict(os.environ), timeout=.03)

    def test_owned_container_requested_limits_labels_images_and_network_are_checked_before_removal(self):
        runtime = object.__new__(bench.Runtime)
        runtime.token = "1" * 32
        runtime.network = {"id": "d" * 64, "name": "m9-" + "1" * 32}
        entry = {"id": "a" * 64, "role": "source", "name": "owned", "image": "sha256:" + "b" * 64,
                 "mounts": [("/owned", "/data", True)]}
        actual = {"Id": entry["id"], "Name": "/owned", "Image": entry["image"], "Config": {"Labels": {"io.rpgmigrate.m9": runtime.token}},
            "HostConfig": {"NanoCpus": 2000000000, "Memory": 4 * bench.GIB, "MemorySwap": 4 * bench.GIB,
                "Privileged": False, "PortBindings": {}, "NetworkMode": "bridge"}, "State": {"Running": True},
            "Mounts": [{"Type": "bind", "Source": "/owned", "Destination": "/data", "RW": True}],
            "NetworkSettings": {"Ports": {}, "Networks": {runtime.network["name"]: {"NetworkID": runtime.network["id"], "Aliases": ["source"]}}}}
        with patch.object(runtime, "json", return_value=[actual]):
            runtime.inspect_container(entry)
        for change in ("id", "image", "memory", "swap", "cpu", "privileged", "port", "label", "mount", "network"):
            bad = copy.deepcopy(actual)
            if change == "id": bad["Id"] = "f" * 64
            elif change == "image": bad["Image"] = "sha256:" + "f" * 64
            elif change == "memory": bad["HostConfig"]["Memory"] = 0
            elif change == "swap": bad["HostConfig"]["MemorySwap"] = -1
            elif change == "cpu": bad["HostConfig"]["NanoCpus"] = 0
            elif change == "privileged": bad["HostConfig"]["Privileged"] = True
            elif change == "port": bad["HostConfig"]["PortBindings"] = {"5432/tcp": [{"HostPort": "5432"}]}
            elif change == "label": bad["Config"]["Labels"]["io.rpgmigrate.m9"] = "foreign"
            elif change == "mount": bad["Mounts"][0]["Source"] = "/foreign"
            else: bad["NetworkSettings"]["Networks"][runtime.network["name"]]["NetworkID"] = "f" * 64
            with self.subTest(change=change), patch.object(runtime, "json", return_value=[bad]):
                with self.assertRaisesRegex(RuntimeError, "container"):
                    runtime.inspect_container(entry)

    def test_wrong_container_ownership_is_never_removed_and_cleanup_cannot_pass(self):
        runtime = object.__new__(bench.Runtime)
        runtime.token = "1" * 32
        runtime.root = Path("/private/fake")
        runtime.network = None
        runtime.containers = {"runner": {"id": "a" * 64}}
        with patch.object(runtime, "inspect_container", side_effect=RuntimeError("foreign identity")), \
                patch.object(runtime, "raw", side_effect=AssertionError("foreign resource removal")), \
                patch.object(runtime, "originals", return_value=None):
            result = runtime.cleanup()
        self.assertFalse(result["containers"][0]["ownership_checked"])
        self.assertFalse(result["containers"][0]["absent"])

    def test_phase_failure_stops_without_retries_and_retains_an_incomplete_checkpoint(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            source_paths = self.packet(Path(first))
            source_context, _phase = self.completed(source_paths)
            templates = {child["child_id"]: bench.read_json(source_paths[4] / (child["child_id"] + ".json")) for child in source_context["schedule"]}
            paths = self.packet(Path(second))
            context = bench.admit(*paths, now=1001)
            root = bench.private_root(paths[4], create=True)
            calls = []

            class FakeRuntime:
                def __init__(self, admission, output):
                    self.samples = []
                    self.facts = {}
                def preflight(self): pass
                def create_network(self): pass
                def create_container(self, role): pass
                def observe(self, initial=False): return None
                def cleanup(self):
                    return {"containers": [{"ownership_checked": True, "absent": True}] * 3,
                            "network": {"ownership_checked": True, "absent": True}, "original_services_unchanged": True}

            def dispatch(_runtime, child):
                calls.append(child["child_id"])
                if len(calls) == 5:
                    raise RuntimeError("synthetic child fault")
                receipt = copy.deepcopy(templates[child["child_id"]])
                receipt.update(context["hashes"])
                return receipt

            with patch.object(bench, "Runtime", FakeRuntime), patch.object(bench, "dispatch_child", side_effect=dispatch), \
                    self.assertRaisesRegex(RuntimeError, "phase failed"):
                bench.execute_phase(context, root)
            self.assertEqual(len(calls), 5)
            self.assertEqual(bench.read_json(root / "mechanics-checkpoint.json")["status"], "passed-incomplete")
            phase = bench.read_json(root / "phase.json")
            self.assertEqual(phase["status"], "failed")
            self.assertEqual(len(phase["children"]), 5)
            failed = bench.read_json(root / (calls[-1] + ".json"))
            self.assertEqual(failed["status"], "failed")
            self.assertFalse((root / "summary.json").exists())

    def test_database_barrier_checks_real_command_tags_and_reset_refuses_a_replacement(self):
        Database = bench.bench_database_type()
        database = object.__new__(Database)
        for output in (b"SET\nlocal\nBEGIN\nUPDATE 1\nCOMMIT\n", b"SET\nlocal\nBEGIN\nUPDATE 0\nCOMMIT\n", b"SET\nlocal\nBEGIN\nCOMMIT\n", b"SET\noff\nBEGIN\nUPDATE 1\nCOMMIT\n"):
            with patch.object(database, "sql", side_effect=(b"p|1\n", output)):
                if b"local\nBEGIN\nUPDATE 1\nCOMMIT" in output:
                    bench.validate_barrier(database.barrier("owned"))
                else:
                    with self.assertRaisesRegex(RuntimeError, "barrier"):
                        database.barrier("owned")
        database.created = {"m9_" + "1" * 32 + "_s": 100}
        with patch.object(database, "database_oid", return_value=101), \
                patch.object(database, "sql", side_effect=AssertionError("replacement database was touched")):
            with self.assertRaisesRegex(RuntimeError, "replaced"):
                database.reset("m9_" + "1" * 32 + "_s")

    def test_postgres_runtime_version_and_wal_settings_are_observed_before_child_admission(self):
        Database = bench.bench_database_type()
        database = object.__new__(Database)
        for actual in (b"180001|on|on|logical|1GB|1GB|UTF8|123456789\n", b"160001|on|on|logical|1GB|1GB|UTF8|123456789\n", b"180001|off|on|logical|1GB|1GB|UTF8|123456789\n", b"180001|on|on|logical|-1|1GB|UTF8|123456789\n"):
            with patch.object(database, "sql", return_value=actual):
                if actual.startswith(b"180001|on|on|logical|1GB"):
                    database.verify_runtime()
                else:
                    with self.assertRaisesRegex(RuntimeError, "PostgreSQL"):
                        database.verify_runtime()

    def test_zero_exit_noop_clone_cannot_replace_independent_target_contents(self):
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.packet(Path(temporary))
            context = bench.admit(*paths, now=1001)
            root = bench.private_root(paths[4], create=True)
            child = context["schedule"][0]
            context["lease"]["deadline"] = __import__("time").time() + 100
            context["worker_authority"] = {"started": __import__("time").time(), "child_deadline": context["lease"]["deadline"]}
            class Database:
                def __init__(self, side, *_args): self.side = side
                def verify_runtime(self): pass
                def create_child(self, _name): pass
                def seed(self, *_args): pass
                def connection(self, _name): return "host=" + self.side
                def fingerprint(self, *_args):
                    return ({"source_has_rows": True} if self.side == "source" else {}), []
                def barrier(self, _name): return {}
            class Gate:
                def __init__(self, side): self.side = side
                def __enter__(self): return self
                def __exit__(self, *_args): pass
                def connection(self, value): return value
                def copy_evidence(self): return {}
            with patch.object(bench.sys, "platform", "linux"), patch.object(bench, "bench_database_type", return_value=Database), \
                    patch.object(bench, "verify_child_artifacts", return_value={}), \
                    patch.object(bench, "copy_observer_types", return_value=(None, Gate)), \
                    patch.object(bench, "validate_wire", return_value=None), \
                    patch.object(bench, "measured_process", return_value={"returncode": 0, "stdout": b'{"complete":true,"phase":"completed"}' } ):
                with self.assertRaisesRegex(RuntimeError, "independent schema or contents"):
                    bench.execute_child(context, root, child["child_id"])

    def test_unapproved_wrong_packet_version_schedule_lease_and_ownership_refuse_before_process(self):
        for mutation, reason in (("unapproved", "approval"), ("packet_hash", "packet"), ("version", "version"),
                ("schedule", "schedule"), ("quiet_window", "quiet"), ("ownership", "ownership")):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                paths = self.packet(Path(temporary))
                approval, manifest, artifacts, lease, root = paths
                if mutation in ("unapproved", "packet_hash"):
                    value = bench.read_json(approval)
                    value["decision" if mutation == "unapproved" else "packet_sha256"] = "held" if mutation == "unapproved" else "f" * 64
                    approval.write_bytes(bench.encode(value))
                elif mutation == "version":
                    value = bench.read_json(manifest)
                    value["pair"] = "16-18"
                    manifest.write_bytes(bench.encode(value))
                elif mutation == "schedule":
                    path = manifest.parent / "schedule.json"
                    value = bench.read_json(path)
                    value["children"].pop()
                    path.write_bytes(bench.encode(value))
                elif mutation == "quiet_window":
                    value = bench.read_json(lease)
                    value["quiet_window"]["host_cargo_idle"] = False
                    lease.write_bytes(bench.encode(value))
                else:
                    path = manifest.parent / "packet.json"
                    value = bench.read_json(path)
                    value["ownership"]["owner"] = "f" * 32
                    path.write_bytes(bench.encode(value))
                    value = bench.read_json(approval)
                    value["packet_sha256"] = bench.file_hash(path)
                    approval.write_bytes(bench.encode(value))
                with patch("subprocess.Popen", side_effect=AssertionError("inadmissible child")), \
                        self.assertRaisesRegex(RuntimeError, reason):
                    bench.admit(*paths, now=1001)
                self.assertFalse(root.exists())

    def test_placeholder_unknown_artifact_fields_and_missing_worker_authority_refuse(self):
        for mutation in ("placeholder", "unknown", "libpq_major"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                paths = self.packet(Path(temporary))
                lock = bench.read_json(paths[2])
                if mutation == "placeholder": lock["native"]["build"]["compiler"] = "TODO"
                elif mutation == "unknown": lock["qualification"]["correctness_qualified"] = True
                else: lock["fork"]["libpq"]["pq_version"] = 160001
                paths[2].write_bytes(bench.encode(lock))
                with self.assertRaises(RuntimeError):
                    bench.validate_artifacts(paths[2])
        with tempfile.TemporaryDirectory() as temporary:
            paths = self.packet(Path(temporary))
            context = bench.admit(*paths, now=1001)
            root = bench.private_root(paths[4], create=True)
            with patch("subprocess.Popen", side_effect=AssertionError("unauthorized internal child")), self.assertRaises(OSError):
                bench.verify_worker_authority(context, root, context["schedule"][0]["child_id"])

    def test_complete_phase_runs_exactly_forty_and_cleanup_failure_invalidates_the_phase(self):
        with tempfile.TemporaryDirectory() as first:
            source_paths = self.packet(Path(first))
            source_context, source_phase = self.completed(source_paths)
            templates = {child["child_id"]: bench.read_json(source_paths[4] / (child["child_id"] + ".json")) for child in source_context["schedule"]}
            for fail_cleanup in (False, True):
                with self.subTest(fail_cleanup=fail_cleanup), tempfile.TemporaryDirectory() as temporary:
                    paths = self.packet(Path(temporary))
                    context = bench.admit(*paths, now=1001)
                    root = bench.private_root(paths[4], create=True)
                    calls, resources = [], self.resources()
                    class FakeRuntime:
                        def __init__(self, admission, output):
                            self.samples, self.facts = [], copy.deepcopy(source_phase["runtime"])
                        def preflight(self): pass
                        def create_network(self): pass
                        def create_container(self, role): pass
                        def observe(self, initial=False): return resources
                        def cleanup(self):
                            result = copy.deepcopy(source_phase["cleanup"])
                            result["retained_paths"] = bench.retained_paths(root, "1" * 32)
                            if fail_cleanup: result["containers"][0]["absent"] = False
                            return result
                    def dispatch(_runtime, child):
                        calls.append(child["child_id"])
                        receipt = copy.deepcopy(templates[child["child_id"]])
                        receipt.update(context["hashes"])
                        return receipt
                    with patch.object(bench, "Runtime", FakeRuntime), patch.object(bench, "dispatch_child", side_effect=dispatch):
                        if fail_cleanup:
                            with self.assertRaisesRegex(RuntimeError, "cleanup incomplete"):
                                bench.execute_phase(context, root)
                        else:
                            result = bench.execute_phase(context, root)
                            self.assertFalse((root / "phase.json").exists())
                            bench.publish(root / "phase.json", result)
                    self.assertEqual(calls, [child["child_id"] for child in context["schedule"]])
                    if fail_cleanup:
                        with self.assertRaisesRegex(RuntimeError, "failed or incomplete"):
                            bench.summarize(paths[1], paths[2], root)
                    else:
                        result = bench.summarize(paths[1], paths[2], root)
                        self.assertEqual(result["completed"], 40)
                        self.assertGreater(result["children"][0]["aa_cell"]["absolute_paired_log_ratio_tolerance"], 0)
                        self.assertIsNone(result["children"][0]["aa_cell"]["measurement_scope"]["source_cpu"])

    def test_podman_network_create_name_is_resolved_to_exact_owned_id_before_use(self):
        with tempfile.TemporaryDirectory() as temporary:
            runtime = object.__new__(bench.Runtime)
            runtime.token = "1" * 32
            runtime.root = Path(temporary)
            name = "m9-" + runtime.token
            for created in (name, "a" * 64):
                for filename in ("network-intent.json", "network-created.json"):
                    (runtime.root / filename).unlink(missing_ok=True)
                with patch.object(runtime, "raw", return_value=(created.encode(), 0)), \
                        patch.object(runtime, "json", return_value=[{"id": "a" * 64}]), \
                        patch.object(runtime, "check_network", return_value={}):
                    runtime.create_network()
                self.assertEqual(runtime.network, {"id": "a" * 64, "name": name})

    def test_cleanup_stops_exact_owned_servers_cleanly_and_keeps_all_volumes_and_directories(self):
        runtime = object.__new__(bench.Runtime)
        runtime.token, runtime.root, runtime.network = "1" * 32, Path("/private/fake"), None
        runtime.containers = {"source": {"id": "a" * 64}}
        commands = []
        def raw(arguments, **_kwargs):
            commands.append(arguments)
            return b"", 1 if arguments[:2] == ["container", "exists"] else 0
        with patch.object(runtime, "inspect_container", return_value={"State": {"Running": False, "ExitCode": 0}}), \
                patch.object(runtime, "raw", side_effect=raw), patch.object(runtime, "originals", return_value=None):
            result = runtime.cleanup()
        self.assertTrue(result["containers"][0]["absent"])
        self.assertEqual(commands, [["stop", "--time", "10", "a" * 64], ["rm", "a" * 64], ["container", "exists", "a" * 64]])

    def test_host_lock_is_nonblocking_retained_and_rejects_unsafe_existing_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary) / "coordination"
            record = {"parent": str(parent), "path": str(parent / "phase.lock"), "uid": os.getuid()}
            with bench.exclusive_host_lock(record) as first:
                with self.assertRaisesRegex(RuntimeError, "busy"):
                    with bench.exclusive_host_lock(record): pass
            with bench.exclusive_host_lock(record) as second:
                self.assertEqual(first["inode"], second["inode"])
            path = Path(record["path"])
            self.assertTrue(path.exists())
            path.chmod(0o644)
            with self.assertRaisesRegex(RuntimeError, "unsafe"):
                with bench.exclusive_host_lock(record): pass

    def test_host_lock_replacement_on_release_is_refused_without_unlinking_replacement(self):
        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary) / "coordination"
            record = {"parent": str(parent), "path": str(parent / "phase.lock"), "uid": os.getuid()}
            path = Path(record["path"])
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                with bench.exclusive_host_lock(record):
                    path.unlink()
                    path.write_text("replacement-canary")
                    path.chmod(0o600)
            self.assertEqual(path.read_text(), "replacement-canary")

    def test_host_lock_release_failure_cannot_publish_a_successful_phase(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            parent = base / "coordination"
            record = {"parent": str(parent), "path": str(parent / "phase.lock"), "uid": os.getuid()}
            root = base / "receipts"
            context = {"packet": {"ownership": {"host_lock": record}}}
            def execute(_admission, _root):
                path = Path(record["path"])
                path.unlink()
                path.write_text("replacement")
                path.chmod(0o600)
                return {"status": "success"}
            with patch.object(bench, "admit", return_value=context), patch.object(bench, "execute_phase", side_effect=execute), \
                    self.assertRaisesRegex(RuntimeError, "identity changed"):
                bench.run_phase(None, None, None, None, root)
            self.assertEqual(bench.read_json(root / "phase.json")["status"], "failed")


if __name__ == "__main__":
    unittest.main()
