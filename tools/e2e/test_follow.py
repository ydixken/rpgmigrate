"""Portable negative controls for ongoing follow qualification."""

import importlib.util
import copy
import json
import re
import runpy
from pathlib import Path
from contextlib import ExitStack, contextmanager, nullcontext, redirect_stderr, redirect_stdout
import io
import subprocess
from types import SimpleNamespace
import socket
import struct
import stat
import sys
import tempfile
import threading
import unittest
import zlib
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import follow as harness
from follow_wire import WireGate, PgoutputObservation


class FollowHarnessTests(unittest.TestCase):
    @staticmethod
    def logical_observer():
        observer = PgoutputObservation(1)
        observer.startup(struct.pack("!I", 196608) + b"user\0fixture\0replication\0database\0\0")
        observer.query(b"""START_REPLICATION SLOT "owned" LOGICAL 0/10 ("proto_version" '1', "publication_names" '"owned"')\0""")
        observer.received(b'W', b'\0\0\0')
        return observer

    @staticmethod
    def wal(message):
        return b'w' + struct.pack("!QQq", 1, 2**64 - 1, 0) + message

    def test_numeric_transaction_headers_match_actual_pg16_and_pg18_fixtures(self):
        for major in (16, 18):
            observer = self.logical_observer()
            fixture = json.loads((harness.clone.ROOT / f"tests/fixtures/cdc/pgoutput/pg{major}.json").read_text())
            for packet in fixture["packets"]:
                token = observer.received(b'd', self.wal(bytes.fromhex(packet["hex"])))
                observer.forwarded(token)
            evidence = observer.evidence()
            self.assertTrue(evidence["complete"])
            self.assertEqual(len(evidence["transactions"]), 10)
            self.assertEqual(sum(tx["dml_count"] for tx in evidence["transactions"]), 527)
            for tx in evidence["transactions"]:
                self.assertEqual(tx["final_lsn"], tx["commit_lsn"])
                self.assertLessEqual(tx["commit_lsn"], tx["end_lsn"])
                self.assertTrue(tx["complete"] and tx["commit_forwarded"])
            self.assertNotIn("hex", json.dumps(evidence))

    def test_transaction_evidence_separates_received_from_forwarded_and_refuses_partial(self):
        observer = self.logical_observer()
        begin = b'B' + struct.pack("!QqI", 16, 0, 3)
        commit = b'C' + struct.pack("!BQQq", 0, 16, 24, 0)
        observer.forwarded(observer.received(b'd', self.wal(begin)))
        self.assertFalse(observer.evidence()["complete"])
        token = observer.received(b'd', self.wal(commit))
        tx = observer.evidence()["transactions"][0]
        self.assertTrue(tx["complete"])
        self.assertFalse(tx["commit_forwarded"])
        self.assertTrue(observer.evidence()["complete"])
        observer.forwarded(token)
        self.assertTrue(observer.evidence()["transactions"][0]["commit_forwarded"])
        self.assertEqual(tx["final_lsn"], 16)
        self.assertNotEqual(tx["end_lsn"], 2**64 - 1)

    def test_malformed_correlated_headers_and_evidence_overflow_are_explicit(self):
        begin = b'B' + struct.pack("!QqI", 16, 0, 3)
        commit = b'C' + struct.pack("!BQQq", 0, 16, 24, 0)
        for messages in ((begin[:-1],), (commit,), (begin, begin),
                         (begin, commit[:-1]), (begin, commit + b'x'),
                         (begin, b'C' + struct.pack("!BQQq", 1, 16, 24, 0)),
                         (begin, b'C' + struct.pack("!BQQq", 0, 17, 24, 0)),
                         (begin, b'C' + struct.pack("!BQQq", 0, 16, 15, 0)),
                         (begin, b'C' + struct.pack("!BQQq", 0, 16, 24, 1)),
                         (b'B' + struct.pack("!QqI", 0, 0, 3),),
                         (b'B' + struct.pack("!QqI", 16, 0, 2),)):
            with self.subTest(messages=messages):
                observer = self.logical_observer()
                for message in messages:
                    observer.forwarded(observer.received(b'd', self.wal(message)))
                self.assertFalse(observer.evidence()["complete"])
                self.assertTrue(observer.evidence()["invalid"])
        observer = self.logical_observer()
        for index in range(observer.LIMIT + 1):
            final = 16 + index * 16
            for message in (b'B' + struct.pack("!QqI", final, 0, 3), b'C' + struct.pack("!BQQq", 0, final, final + 8, 0)):
                observer.forwarded(observer.received(b'd', self.wal(message)))
        evidence = observer.evidence()
        self.assertTrue(evidence["overflow"])
        self.assertFalse(evidence["complete"])
        self.assertEqual(len(evidence["transactions"]), observer.LIMIT)

    def test_normal_copy_row_canaries_and_unconfirmed_logical_stream_are_ignored(self):
        for observer in (PgoutputObservation(1), self.logical_observer()):
            if observer.confirmed:
                observer.received(b'Z', b'I')
            observer.received(b'd', self.wal(b'Bfixture-sensitive-row-private'))
            observer.received(b'd', b'fixture-password')
            evidence = observer.evidence()
            self.assertEqual(evidence["transactions"], [])
            self.assertNotIn("fixture-", json.dumps(evidence))

    @staticmethod
    def wire_frame(kind, payload):
        return kind + struct.pack("!I", len(payload) + 4) + payload

    @staticmethod
    def wire_startup(logical=False):
        payload = struct.pack("!I", 196608) + b"user\0fixture\0"
        if logical:
            payload += b"replication\0database\0"
        payload += b"\0"
        return struct.pack("!I", len(payload) + 4) + payload

    @contextmanager
    def wire_pair(self, mode):
        with WireGate("fixture", mode) as gate:
            client, producer = socket.socketpair()
            server, backend = socket.socketpair()
            gate.sockets.extend((client, server))
            producer.settimeout(2)
            backend.settimeout(2)
            worker = threading.Thread(target=gate.downstream, args=(client, server), daemon=True)
            gate.threads.append(worker)
            worker.start()
            try:
                yield gate, producer, backend
            finally:
                producer.close()
                backend.close()

    def test_wire_gate_forwards_ordinary_copy_input_without_status_classification(self):
        from protocol_fault import receive
        for logical in (False, True):
            for payload in (b'row\n', b'r' + struct.pack("!QQQQB", 10, 10, 10, 0, 0)):
                with self.subTest(logical=logical, size=len(payload)), self.wire_pair("before_commit") as (gate, producer, backend):
                    request = self.wire_startup(logical) + self.wire_frame(b'Q', b'COPY fixture FROM STDIN\0')
                    producer.sendall(request)
                    self.assertTrue(receive(backend, len(request)) == request)
                    response = self.wire_frame(b'G', b'\0\0\1\0\0')
                    backend.sendall(response)
                    self.assertTrue(receive(producer, len(response)) == response)
                    data = self.wire_frame(b'd', payload) + self.wire_frame(b'c', b'')
                    producer.sendall(data)
                    try:
                        forwarded = receive(backend, len(data))
                    except (EOFError, OSError):
                        self.fail("ordinary COPY input was interrupted")
                    self.assertTrue(forwarded == data)
                    evidence = gate.evidence()
                    self.assertEqual(evidence["statuses"], [])
                    self.assertEqual(evidence["logical"]["streams"], [])

    def test_wire_gate_forwards_ordinary_copy_output_without_wal_counts_or_holds(self):
        from protocol_fault import receive
        for payload in (b'wide\n', self.wal(b'B' + struct.pack("!QqI", 16, 0, 3))):
            with self.subTest(size=len(payload)), self.wire_pair("after_begin") as (gate, producer, backend):
                request = self.wire_startup() + self.wire_frame(b'Q', b'COPY fixture TO STDOUT\0')
                producer.sendall(request)
                self.assertTrue(receive(backend, len(request)) == request)
                response = self.wire_frame(b'H', b'\0\0\1\0\0')
                backend.sendall(response)
                self.assertTrue(receive(producer, len(response)) == response)
                gate.arm()
                data = self.wire_frame(b'd', payload) + self.wire_frame(b'c', b'')
                backend.sendall(data)
                try:
                    forwarded = receive(producer, len(data))
                except (EOFError, OSError):
                    self.fail("ordinary COPY output was interrupted or held")
                self.assertTrue(forwarded == data)
                evidence = gate.evidence()
                self.assertEqual((evidence["data_bytes"], evidence["begins"], evidence["commits"], evidence["hits"]), (0, 0, 0, 0))
                self.assertEqual(evidence["logical"]["streams"], [])
                self.assertEqual(gate.snapshot()["connections"][0]["mode"], "copy_out")

    def test_wire_gate_refuses_malformed_status_only_after_logical_copyboth(self):
        from protocol_fault import receive
        for payload in (b'rshort', b'r' + bytes(34)):
            with self.subTest(size=len(payload)), self.wire_pair("observe") as (gate, producer, backend):
                request = self.wire_startup(True) + self.wire_frame(b'Q', b"""START_REPLICATION SLOT "owned" LOGICAL 0/10 ("proto_version" '1', "publication_names" '"owned"')\0""")
                producer.sendall(request)
                self.assertTrue(receive(backend, len(request)) == request)
                response = self.wire_frame(b'W', b'\0\0\0')
                backend.sendall(response)
                self.assertTrue(receive(producer, len(response)) == response)
                producer.sendall(self.wire_frame(b'd', payload))
                self.assertEqual(backend.recv(1), b'')
                harness.clone.wait_for(lambda: bool(gate.errors), "malformed logical status refusal", timeout=2)
                self.assertEqual(gate.errors, ["ValueError"])
                with self.assertRaises(RuntimeError):
                    gate.evidence()

    @staticmethod
    def checkpoint_row(**changes):
        return {"version": 5, "source_system": 1, "source_database": 2, "generation": 1,
                "target_system": 3, "target_database": 4, "owner_oid": 5, "schema_oid": 6, "table_oid": 7,
                "applied_order": 1, "last_xid": 3, "begin_ordinal": 16,
                "source_commit_lsn": "0/10", "source_end_lsn": "0/18", "certified_lsn": None,
                "copy_consistent_point": "0/8", "barrier_epoch": 0, "barrier_counter": 1, **changes}

    def test_checkpoint_observation_whitelists_numeric_fields_and_refuses_bad_bounds(self):
        row = self.checkpoint_row(password="fixture-password", payload="fixture-sensitive-row-private")
        observation = harness.checkpoint_observation(row)
        self.assertEqual(observation["status"], "observed")
        self.assertEqual(observation["source_end_lsn"], 24)
        self.assertNotIn("fixture-", json.dumps(observation))
        self.assertTrue(all(type(value) is int or value is None for key, value in observation.items() if key != "status"))
        self.assertEqual(harness.checkpoint_observation(None), {"status": "missing"})
        for changes in ({"source_system": 2**64}, {"applied_order": 2**64}, {"last_xid": True},
                        {"schema_oid": 0}, {"barrier_counter": 0}, {"source_end_lsn": "0/F"},
                        {"source_commit_lsn": None}, {"begin_ordinal": 15}, {"certified_lsn": "0/0"}):
            with self.subTest(changes=changes), self.assertRaises(RuntimeError):
                harness.checkpoint_observation(self.checkpoint_row(**changes))

    def test_failure_observation_reads_are_bounded_and_unavailable_does_not_leak(self):
        database = object.__new__(harness.Database)
        database.authority = Mock(side_effect=RuntimeError("fixture-password fixture-sensitive-row-private"))
        self.assertEqual(database.observe_checkpoint("owned", Path("state"), {}), {"status": "unavailable"})
        self.assertFalse(hasattr(database, "_observation_deadline"))
        database.authority = Mock(return_value=self.checkpoint_row())
        self.assertEqual(database.observe_checkpoint("owned", Path("state"), {})["source_end_lsn"], 24)
        database.slot = Mock(return_value={"confirmed_flush_lsn": "0/18", "restart_lsn": "0/8", "active": True, "active_pid": 10,
                                          "slot_name": "fixture-sensitive-row-private"})
        observed = database.observe_slot("owned", Path("state"))
        self.assertEqual(observed, {"status": "observed", "confirmed_flush_lsn": 24, "restart_lsn": 8, "active_pid": 10})
        database.bindir, database.environment, database.host, database.application = Path("/owned/bin"), {}, "owned", "owned"
        database._observation_deadline = harness.time.monotonic() + 2
        with patch.object(harness.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=b"1")) as run:
            self.assertEqual(database.sql("owned", "SELECT 1;"), b"1")
            self.assertLessEqual(run.call_args.kwargs["timeout"], 2)
            self.assertEqual(run.call_args.kwargs["stderr"], subprocess.DEVNULL)
            self.assertIn(b"statement_timeout", run.call_args.kwargs["input"])
        with patch.object(harness.subprocess, "run", side_effect=subprocess.TimeoutExpired("fixture-password", 2)):
            self.assertEqual(database.observe_checkpoint("owned", Path("state"), {})["status"], "observed")
            with self.assertRaises(RuntimeError):
                database.sql("owned", "SELECT 1;")

    def test_failure_capture_observes_before_cleanup_and_keeps_last_success(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path(temporary) / "diagnostics"):
            root = Path(temporary)
            error_output = root / "stderr"
            error_output.write_bytes(b"")
            events = []
            process = SimpleNamespace(poll=lambda: 7, error_output=error_output, environment={},
                                      follow_last_observation={"scope": "visible-observation", "target": harness.checkpoint_observation(self.checkpoint_row())})
            def capture():
                events.append("observe")
                return {"scope": "visible-observation", "target": {"status": "unavailable"}, "slot": {"status": "unavailable"}}
            process.follow_observe = capture
            driver = object.__new__(harness.Driver)
            driver.kill = lambda child: events.append("kill")
            with self.assertRaises(harness.clone.SubprocessFailure) as failed:
                with ExitStack() as cleanup:
                    driver.watch(process, cleanup)
                    raise RuntimeError("fixture-password fixture-sensitive-row-private")
            self.assertEqual(events, ["observe", "kill"])
            private = json.loads((harness.clone.DIAGNOSTIC_ROOT / (failed.exception.metadata["diagnostic_id"] + ".stderr")).read_text())
            self.assertEqual(private["follow_observation"]["target"]["status"], "unavailable")
            self.assertEqual(private["last_follow_observation"]["target"]["source_end_lsn"], 24)
            self.assertNotIn("fixture-", json.dumps(private))

    def feedback_case(self, *, commit_forwarded=True, applied=True, certified=None, feedback=24):
        observer = self.logical_observer()
        observer.forwarded(observer.received(b'd', self.wal(b'B' + struct.pack("!QqI", 16, 0, 3))))
        observer.received(b'd', self.wal(b'I' + b'fixture-sensitive-row-private'))
        token = observer.received(b'd', self.wal(b'C' + struct.pack("!BQQq", 0, 16, 24, 0)))
        if commit_forwarded:
            observer.forwarded(token)
        observer.statuses = [(feedback, feedback, feedback)]
        initial_row = self.checkpoint_row(applied_order=0, last_xid=None, begin_ordinal=None, source_commit_lsn=None, source_end_lsn=None)
        row = self.checkpoint_row(certified_lsn=certified) if applied else {**initial_row, "certified_lsn": certified}
        initial = {"scope": "visible-observation", "connection": 1, "status_start": 0,
                   "target": harness.checkpoint_observation(initial_row), "slot": {"status": "observed", "confirmed_flush_lsn": 8}}
        current = {"scope": "visible-observation", "target": harness.checkpoint_observation(row),
                   "slot": {"status": "observed", "confirmed_flush_lsn": feedback}}
        return observer, initial, current

    def test_feedback_cannot_cover_unapplied_published_or_held_commit(self):
        for forwarded in (False, True):
            observer, initial, current = self.feedback_case(commit_forwarded=forwarded, applied=False, certified="0/20")
            evidence = {"logical": {"overflow": False, "streams": [observer.evidence()]}}
            verdict = harness.failure_feedback_consistency(evidence, current, initial)
            self.assertEqual(verdict["status"], "inconsistent")
            with self.assertRaises(RuntimeError):
                harness.require_failure_feedback(evidence, current, initial)
        observer, initial, current = self.feedback_case()
        evidence = {"logical": {"overflow": False, "streams": [observer.evidence()]}}
        self.assertTrue(harness.require_failure_feedback(evidence, current, initial))
        self.assertEqual(harness.failure_feedback_consistency(evidence, current, initial)["meaning"],
                         "consistent with observed wire/checkpoint frontiers")
        for position in (17, 23):
            observer, initial, current = self.feedback_case(applied=False, certified="0/20", feedback=position)
            self.assertNotEqual(harness.failure_feedback_consistency({"logical": {"overflow": False, "streams": [observer.evidence()]}}, current, initial)["status"], "consistent")
        observer, initial, current = self.feedback_case(applied=False, certified="0/20", feedback=16)
        self.assertNotEqual(harness.failure_feedback_consistency({"logical": {"overflow": False, "streams": [observer.evidence()]}}, current, initial)["status"], "consistent")

    def test_feedback_observation_refuses_incomplete_missing_overflow_and_u64_values(self):
        observer, initial, current = self.feedback_case()
        stream = observer.evidence()
        for change in ({"complete": False}, {"overflow": True}, {"invalid": True}, {"confirmed": False},
                       {"statuses": [(2**64, 2**64, 2**64)]}, {"transactions": [{**stream["transactions"][0], "xid": True}]}):
            with self.subTest(change=change):
                evidence = {"logical": {"overflow": False, "streams": [{**stream, **change}]}}
                self.assertFalse(harness.require_failure_feedback(evidence, current, initial))
        for observation in ({}, {**current, "target": {"status": "unavailable"}}, {**current, "slot": {"status": "unavailable"}}):
            self.assertFalse(harness.require_failure_feedback({"logical": {"overflow": False, "streams": [stream]}}, observation, initial))
        for statuses in ([(24, 23, 24)], [(24, 24, 24), (16, 16, 16)], [(7, 7, 7)], [(25, 25, 25)]):
            with self.subTest(statuses=statuses), self.assertRaises(RuntimeError):
                harness.require_failure_feedback({"logical": {"overflow": False, "streams": [{**stream, "statuses": statuses}]}}, current, initial)

    def test_anchor_replay_and_existing_certification_baseline_need_no_historical_keepalive(self):
        observer, initial, current = self.feedback_case()
        stream = observer.evidence()
        # Reconnect can replay an already observed canonical commit without a new target commit.
        initial["target"] = current["target"]
        initial["slot"]["confirmed_flush_lsn"] = 24
        self.assertTrue(harness.require_failure_feedback({"logical": {"overflow": False, "streams": [stream]}}, current, initial))
        row = self.checkpoint_row(applied_order=0, last_xid=None, begin_ordinal=None, source_commit_lsn=None, source_end_lsn=None, copy_consistent_point="0/18")
        initial["target"] = current["target"] = harness.checkpoint_observation(row)
        self.assertTrue(harness.require_failure_feedback({"logical": {"overflow": False, "streams": [stream]}}, current, initial))
        row["certified_lsn"] = "0/28"
        initial["target"] = current["target"] = harness.checkpoint_observation(row)
        initial["slot"]["confirmed_flush_lsn"] = current["slot"]["confirmed_flush_lsn"] = 40
        stream["transactions"], stream["statuses"] = [], [(40, 40, 40)]
        self.assertTrue(harness.require_failure_feedback({"logical": {"overflow": False, "streams": [stream]}}, current, initial))

    def test_new_no_work_certification_requires_forwarded_idle_keepalive_basis(self):
        observer, initial, current = self.feedback_case(applied=False, certified="0/28", feedback=40)
        stream = observer.evidence()
        stream["transactions"] = []
        for keepalive in (None, {"connection": 1, "lsn": 40, "idle": False, "forwarded": True},
                          {"connection": 1, "lsn": 40, "idle": True, "forwarded": False}):
            stream["keepalives"] = [] if keepalive is None else [keepalive]
            self.assertFalse(harness.require_failure_feedback({"logical": {"overflow": False, "streams": [stream]}}, current, initial))
        stream["keepalives"] = [{"connection": 1, "lsn": 40, "idle": True, "forwarded": True}]
        self.assertTrue(harness.require_failure_feedback({"logical": {"overflow": False, "streams": [stream]}}, current, initial))

    def test_exact_commit_equality_is_not_skip_evidence_but_commit_plus_one_is(self):
        for feedback, expected in ((16, "consistent"), (17, "inconsistent"), (23, "inconsistent"), (24, "inconsistent")):
            with self.subTest(feedback=feedback):
                observer, initial, current = self.feedback_case(applied=False, certified="0/20", feedback=feedback)
                stream = observer.evidence()
                stream["keepalives"] = [{"connection": 1, "lsn": 16, "idle": True, "forwarded": True}]
                result = harness.failure_feedback_consistency({"logical": {"overflow": False, "streams": [stream]}}, current, initial)
                self.assertEqual(result["status"], expected)
        observer, initial, current = self.feedback_case(applied=False, certified="0/20", feedback=24)
        initial["target"]["copy_consistent_point"] = current["target"]["copy_consistent_point"] = 24
        initial["slot"]["confirmed_flush_lsn"] = 24
        self.assertTrue(harness.require_failure_feedback({"logical": {"overflow": False, "streams": [observer.evidence()]}}, current, initial))

    def test_keepalive_observation_uses_idle_forwarded_position_and_bounds(self):
        observer = self.logical_observer()
        keepalive = b'k' + struct.pack("!QqB", 16, 0, 1)
        token = observer.received(b'd', keepalive)
        self.assertEqual(observer.evidence()["keepalives"], [{"connection": 1, "lsn": 16, "idle": True, "forwarded": False}])
        observer.forwarded(token)
        observer.received(b'd', self.wal(b'B' + struct.pack("!QqI", 24, 0, 3)))
        observer.received(b'd', keepalive)
        self.assertFalse(observer.evidence()["keepalives"][-1]["idle"])
        for _index in range(observer.LIMIT):
            observer.received(b'd', keepalive)
        self.assertTrue(observer.evidence()["overflow"])
        self.assertEqual(len(observer.evidence()["keepalives"]), observer.LIMIT)
        for payload in (keepalive[:-1], keepalive[:-1] + b'\2', keepalive + b'x'):
            observer = self.logical_observer()
            observer.received(b'd', payload)
            self.assertTrue(observer.evidence()["invalid"])
        observer = self.logical_observer()
        maximum = 2**64 - 1
        for message in (b'B' + struct.pack("!QqI", maximum - 8, 0, 2**32 - 1),
                        b'C' + struct.pack("!BQQq", 0, maximum - 8, maximum, 0)):
            observer.forwarded(observer.received(b'd', self.wal(message)))
        self.assertEqual(observer.evidence()["transactions"][0]["end_lsn"], maximum)
        self.assertTrue(observer.evidence()["complete"])

    def test_retained_wire_tail_is_bounded_and_truncation_cannot_pass(self):
        observer, initial, current = self.feedback_case()
        stream = observer.evidence()
        raw = {"hits": 1, "data_bytes": 1024, "begins": 1, "commits": 1, "statuses": [(24, 24, 24)] * 100,
               "logical": {"overflow": False, "streams": [{**stream, "statuses": [(24, 24, 24)] * 100}]},
               "payload": "fixture-sensitive-row-private", "password": "fixture-password"}
        retained = harness.bounded_wire_observation(raw)
        self.assertEqual(retained["status_count"], 100)
        self.assertEqual(len(retained["statuses"]), 32)
        self.assertTrue(retained["logical"]["streams"][0]["overflow"])
        self.assertFalse(harness.require_failure_feedback(retained, current, initial))
        self.assertLess(len(json.dumps(retained)), 32768)
        self.assertNotIn("fixture-", json.dumps(retained))
        raw["logical"]["streams"][0]["transactions"][0]["xid"] = "fixture-password"
        self.assertEqual(harness.bounded_wire_observation(raw), {"observer_failed": True})

    def test_original_subprocess_error_survives_secondary_observation_failure(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path(temporary) / "diagnostics"):
            error_output = Path(temporary) / "stderr"
            error_output.write_bytes(b"")
            events = []
            def unavailable():
                events.append("observe")
                raise RuntimeError("fixture-password fixture-sensitive-row-private")
            process = SimpleNamespace(poll=lambda: 7, error_output=error_output, environment={}, follow_observe=unavailable)
            driver = object.__new__(harness.Driver)
            driver.kill = lambda child: events.append("kill")
            original = harness.clone.SubprocessFailure({"diagnostic_id": "primary", "retained": True})
            with self.assertRaises(harness.clone.SubprocessFailure) as failed:
                with ExitStack() as cleanup:
                    driver.watch(process, cleanup)
                    raise original
            self.assertIs(failed.exception, original)
            self.assertEqual(events, ["observe", "kill"])
            metadata = original.metadata["follow_observation"]
            private = json.loads((harness.clone.DIAGNOSTIC_ROOT / (metadata["diagnostic_id"] + ".stderr")).read_text())
            self.assertEqual(private["follow_observation"]["target"]["status"], "unavailable")
            self.assertEqual(private["feedback_consistency"]["status"], "unavailable")
            self.assertNotIn("fixture-", json.dumps(private))

    def test_logical_gate_observes_complete_commit_before_forwarding_it(self):
        from protocol_fault import receive
        def frame(kind, payload):
            return kind + struct.pack("!I", len(payload) + 4) + payload
        startup_payload = struct.pack("!I", 196608) + b"user\0fixture\0replication\0database\0\0"
        startup = struct.pack("!I", len(startup_payload) + 4) + startup_payload
        query = frame(b'Q', b"""START_REPLICATION SLOT "owned" LOGICAL 0/10 ("proto_version" '1', "publication_names" '"owned"')\0""")
        response = frame(b'W', b'\0\0\0')
        begin = frame(b'd', self.wal(b'B' + struct.pack("!QqI", 16, 0, 3)))
        commit = frame(b'd', self.wal(b'C' + struct.pack("!BQQq", 0, 16, 24, 0)))
        with WireGate("fixture", "after_begin") as gate:
            client, producer = socket.socketpair()
            server, backend = socket.socketpair()
            gate.sockets.extend((client, server))
            producer.settimeout(2)
            backend.settimeout(2)
            worker = threading.Thread(target=gate.downstream, args=(client, server), daemon=True)
            gate.threads.append(worker)
            worker.start()
            try:
                producer.sendall(startup + query)
                self.assertEqual(receive(backend, len(startup + query)), startup + query)
                backend.sendall(response)
                self.assertEqual(receive(producer, len(response)), response)
                gate.arm()
                backend.sendall(begin + commit)
                self.assertEqual(receive(producer, len(begin)), begin)
                self.assertTrue(gate.held.wait(timeout=2))
                evidence = gate.evidence()
                tx = evidence["logical"]["streams"][0]["transactions"][0]
                self.assertTrue(tx["complete"] and tx["begin_forwarded"])
                self.assertFalse(tx["commit_forwarded"])
                self.assertEqual((evidence["begins"], evidence["commits"]), (1, 0))
                connection = gate.snapshot()["connections"][0]
                self.assertEqual(connection["mode"], "logical")
                self.assertEqual(connection["counts"]["server"]["received"]["d"], 2)
                self.assertEqual(connection["counts"]["server"]["forwarded"]["d"], 1)
                gate.release.set()
                self.assertEqual(receive(producer, len(commit)), commit)
                harness.clone.wait_for(lambda: gate.evidence()["logical"]["streams"][0]["transactions"][0]["commit_forwarded"], "forwarded numeric commit", timeout=2)
                self.assertEqual(gate.evidence()["commits"], 1)
                # A frame that ends before its declared BEGIN bytes cannot become complete evidence.
                backend.sendall(b'd' + struct.pack("!I", 50) + b'wshort')
                backend.shutdown(socket.SHUT_WR)
                worker.join(timeout=2)
                self.assertFalse(worker.is_alive())
                self.assertFalse(gate.evidence()["logical"]["streams"][0]["complete"])
            finally:
                producer.close()
                backend.close()

    def test_ready_retains_initial_validated_observation_without_changing_report(self):
        report = {"phase": "streaming"}
        initial = {"scope": "visible-observation", "target": harness.checkpoint_observation(self.checkpoint_row()),
                   "slot": {"status": "observed", "confirmed_flush_lsn": 24}}
        for capture in (Mock(return_value=dict(initial)), Mock(side_effect=RuntimeError("fixture-password"))):
            process = SimpleNamespace(follow_observe=capture,
                                      follow_wire=SimpleNamespace(evidence=lambda: {"logical": {"streams": [{"connection": 3, "statuses": [(24, 24, 24)]}]}}))
            driver = object.__new__(harness.Driver)
            with patch.object(harness.clone, "wait_for", return_value=report):
                self.assertIs(driver.ready(process), report)
            if capture.side_effect:
                self.assertEqual(process.follow_initial_observation["target"]["status"], "unavailable")
            else:
                self.assertEqual(process.follow_initial_observation, {**initial, "connection": 3, "status_start": 1})

    def test_failure_cleanup_captures_only_latest_process_and_preserves_its_receipt(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path(temporary) / "diagnostics"):
            error_output = Path(temporary) / "stderr"
            error_output.write_bytes(b"")
            events = []
            driver = object.__new__(harness.Driver)
            driver.kill = lambda process: events.append("kill-" + process.name)
            with self.assertRaises(harness.clone.SubprocessFailure):
                with ExitStack() as cleanup:
                    for name in ("old", "latest"):
                        def capture(name=name):
                            events.append("observe-" + name)
                            return {"scope": "visible-observation", "target": {"status": "unavailable"}, "slot": {"status": "unavailable"}}
                        process = SimpleNamespace(name=name, poll=lambda: 7, error_output=error_output, environment={}, follow_observe=capture)
                        driver.watch(process, cleanup)
                    raise RuntimeError("failed")
            self.assertEqual(events, ["observe-latest", "kill-latest", "kill-old"])
            self.assertEqual(len(list(harness.clone.DIAGNOSTIC_ROOT.glob("*.stderr"))), 1)

    def test_fixture_inventory_is_exact_nonempty_and_bounded(self):
        manifest = harness.load_manifest()
        self.assertEqual(tuple(manifest["cases"]), harness.CASES)
        self.assertEqual(manifest["row_counts"]["full_rows"], 3)
        for key, value in (("cases", []), ("cases", list(harness.CASES[:-1])), ("tables", []), ("max_rows", 0), ("row_counts", {})):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "cases.json"
                path.write_text(json.dumps({**manifest, key: value}))
                with self.assertRaises(RuntimeError):
                    harness.load_manifest(path)

    def test_fixture_has_no_generated_check_expression_partial_or_callback_obligation(self):
        sql = (harness.FIXTURES / "schema.sql").read_text()
        self.assertIn("REPLICA IDENTITY FULL", sql)
        self.assertIn("REPLICA IDENTITY USING INDEX", sql)
        self.assertIn("GENERATED ALWAYS AS IDENTITY", sql)
        self.assertNotIn("CHECK", sql)
        self.assertNotIn("STORED", sql)
        self.assertNotIn("CREATE FUNCTION", sql)
        self.assertNotIn("CREATE TRIGGER", sql)

    def test_workload_preflight_requires_sql_success_rollback_and_unchanged_fingerprint(self):
        manifest = harness.load_manifest()
        fingerprint = {"rows": {"rows": 100, "sha256": "fixture"}}
        source = SimpleNamespace(sql=Mock(), fingerprint=Mock(side_effect=[fingerprint, fingerprint]))
        harness.check_workload(source, "owned_source", manifest)
        source.sql.assert_called_once_with("owned_source", "BEGIN;\n" + harness.INITIAL_DML + "\nROLLBACK;")
        self.assertEqual(source.fingerprint.call_count, 2)
        source = SimpleNamespace(sql=Mock(side_effect=RuntimeError("owned PostgreSQL rejected incompatible workload")),
                                 fingerprint=Mock(return_value=fingerprint))
        with self.assertRaisesRegex(RuntimeError, "incompatible workload"):
            harness.check_workload(source, "owned_source", manifest)
        self.assertEqual(source.fingerprint.call_count, 1)
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path(temporary)):
            source = SimpleNamespace(sql=Mock(), fingerprint=Mock(side_effect=[fingerprint, {}]))
            with self.assertRaises(RuntimeError):
                harness.check_workload(source, "owned_source", manifest)

    def test_frontier_requires_complete_canonical_identity_and_excludes_anchor(self):
        empty = {"applied_order": 0, "source_end_lsn": None, "certified_lsn": None, "copy_consistent_point": "0/FF"}
        self.assertEqual(harness.authority_frontier(empty), 0)
        row = {"applied_order": 1, "last_xid": 3, "begin_ordinal": 16, "source_commit_lsn": "0/10", "source_end_lsn": "0/18", "certified_lsn": "0/20"}
        self.assertEqual(harness.authority_frontier(row), 32)
        for bad in ({**row, "source_commit_lsn": None}, {**row, "begin_ordinal": 15}, {**row, "source_end_lsn": "0/F"}, {**row, "applied_order": 0}, {**empty, "last_xid": 3}):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                harness.authority_frontier(bad)

    def test_authority_projects_recorded_postgresql_oid_json_and_preserves_strict_checks(self):
        # PG16.15/18.6 SQL probes emit oid as strings and numeric(20,0) as JSON numbers.
        columns = re.findall(r'\("([a-z_]+)", "([a-z0-9_]+)", \d+, [A-Z0-9_-]+, (?:true|false)\)',
                             (harness.clone.ROOT / "src/apply/metadata.rs").read_text().split("];", 1)[0])
        self.assertEqual(len(columns), 25)
        oid_columns = {name for name, kind in columns if kind == "oid"}
        self.assertEqual(oid_columns, {"source_database", "target_database", "owner_oid", "schema_oid", "table_oid", "last_xid"})
        maximum = 2**64 - 1
        stream = {"run": list(range(1, 17)), "source": {"system_identifier": maximum, "database_oid": 16384}, "generation": maximum}
        binding = {"slot": {"intent": {"binding": {"stream": stream, "slot_name": "owned_slot", "plugin": "pgoutput"},
                                      "nonce": list(range(17, 33))}, "consistent_point": 16, "snapshot_token": "00000001-00000001-1"},
                   "publication": {"name": "owned_publication", "oid": 16390, "owner_oid": 10}, "copy_generation": 1}
        work = {"migration_uid": "12345678-1234-1234-1234-123456789abc", "run_id": bytes(stream["run"]).hex(),
                "workdir_id": "owned-oracle", "checkpoint_generation": maximum}
        expected = {"stream": stream, "target_system_identifier": maximum, "target_database_oid": 16385,
                    "schema_name": "owned", "table_name": "control", "owner_oid": 10}
        record = {"intent": {"binding": expected, "nonce": list(range(33, 49))}, "schema_oid": 16386, "table_oid": 16387,
                  "marker_table_oid": 16388, "marker_index_oid": 16389}
        native = {"format_version": 7, "clone_state": {"plan": {"tables": [{"source_oid": 1, "name": {"schema": "public", "name": "items"}, "writable_columns": ["id"], "all_columns": ["id"], "identity_modes": [97], "partition_identity_modes": None}]}, "owned_tables": []}, "target_provenance": [{"phase": "confirmed", "record": record}], "follow_state": {"binding": binding}}
        catalog = {"oid": "16387", "relnamespace": "16386", "relowner": "10", "relpersistence": "p", "relkind": "r"}
        row = {"magic": "rpgmigrate", "version": 5, "run_id": "\\x" + work["run_id"], "nonce": "\\x" + bytes(record["intent"]["nonce"]).hex(),
               "source_system": maximum, "source_database": "16384", "generation": maximum, "target_system": maximum,
               "target_database": "16385", "owner_oid": "10", "schema_oid": "16386", "table_oid": "16387", "applied_order": maximum,
               "last_xid": "4294967295", "begin_ordinal": maximum, "source_commit_lsn": "FFFFFFFF/FFFFFFFF", "source_end_lsn": "FFFFFFFF/FFFFFFFF",
               "barrier_epoch": maximum, "barrier_counter": maximum, "copy_snapshot_token": "00000001-00000001-1",
               "copy_consistent_point": "0/10", "copy_plan_nonce": "\\x" + "01" * 16, "clone_state": None, "certified_lsn": None,
               "follow_binding": json.dumps({"binding": binding, "operator": {"migration_uid": work["migration_uid"], "run": stream["run"],
                                            "workdir_id": work["workdir_id"], "checkpoint_generation": maximum}, "origin_name": None})}
        self.assertEqual(set(row), {name for name, _kind in columns})
        actual = [maximum, "16385", "10", True, "on", "on"]
        objects = ["16387|16386|10|p|r", "16388|16386|10|p|r", "16389|16386|10|p|i"]
        def sql(name, query):
            self.assertEqual(name, "owned_target")
            if query.startswith("SELECT row_to_json(c)"):
                result = dict(catalog)
                for key in ("oid", "relnamespace", "relowner"):
                    if f"c.{key}::bigint AS {key}" in query:
                        result[key] = int(result[key])
            elif query.startswith("SELECT row_to_json(t)"):
                result = dict(row)
                if "FROM (SELECT " in query:
                    projection = query.split("FROM (SELECT ", 1)[1].split(" FROM ONLY ", 1)[0]
                    self.assertEqual(projection.split(","), ["t." + key + ("::bigint AS " + key if kind == "oid" else "") for key, kind in columns])
                    for key in oid_columns:
                        if result[key] is not None:
                            result[key] = int(result[key])
            elif query.startswith("SELECT json_build_array"):
                result = list(actual)
                for index, table in ((1, "pg_database"), (2, "pg_roles")):
                    if f"SELECT oid::bigint FROM {table}" in query:
                        result[index] = int(result[index])
            elif query.startswith("SELECT oid,relnamespace"):
                return ("\n".join(objects) + "\n").encode()
            else:
                self.fail("unexpected authority SQL")
            return json.dumps(result).encode()
        database = object.__new__(harness.Database)
        database.sql = sql
        with tempfile.TemporaryDirectory() as temporary:
            state = Path(temporary)
            (state / "manifest.json").write_text(json.dumps(native))
            observed = database.authority("owned_target", state, work)
            self.assertEqual(harness.authority_frontier(observed), maximum)
            self.assertTrue(harness.require_feedback({"statuses": [(maximum, maximum, maximum)]}, observed, maximum))
            for version in (6, 8, True):
                (state / "manifest.json").write_text(json.dumps({**native, "format_version": version}))
                with self.subTest(version=version), self.assertRaisesRegex(RuntimeError, "identity/version"):
                    database.authority("owned_target", state, work)
            table = native["clone_state"]["plan"]["tables"][0]
            for key in ("identity_modes", "partition_identity_modes"):
                modes = table.pop(key)
                (state / "manifest.json").write_text(json.dumps(native))
                with self.subTest(missing=key), self.assertRaisesRegex(RuntimeError, "table identity"):
                    database.authority("owned_target", state, work)
                table[key] = modes
            (state / "manifest.json").write_text(json.dumps(native))
            for key in ("run_id", "nonce", "copy_snapshot_token", "copy_consistent_point", "copy_plan_nonce", "clone_state", "follow_binding"):
                self.assertEqual(observed[key], row[key])
            for target, key, bad, reason in ((catalog, "oid", "16399", "relation identity"),
                                            (catalog, "relkind", "v", "relation identity"),
                                            (row, "source_database", "16399", "full stream/object binding"),
                                            (row, "owner_oid", "16399", "full stream/object binding"),
                                            (row, "barrier_counter", 0, "durability policy"),
                                            (row, "barrier_counter", str(maximum), "durability policy"),
                                            (row, "barrier_epoch", 2**64, "durability policy"),
                                            (row, "barrier_epoch", True, "durability policy")):
                with self.subTest(key=key, bad=bad):
                    previous = target[key]
                    target[key] = bad
                    with self.assertRaisesRegex(RuntimeError, reason):
                        database.authority("owned_target", state, work)
                    target[key] = previous
            row.update(applied_order=0, last_xid=None, begin_ordinal=None, source_commit_lsn=None, source_end_lsn=None)
            observed = database.authority("owned_target", state, work)
            self.assertIsNone(observed["last_xid"])
            self.assertEqual(harness.authority_frontier(observed), 0)
            row["last_xid"] = "4294967295"
            with self.assertRaisesRegex(RuntimeError, "partial applied frontier"):
                harness.authority_frontier(database.authority("owned_target", state, work))

    def test_feedback_predicate_needs_positive_applied_or_certified_target_and_status(self):
        row = {"applied_order": 1, "last_xid": 3, "begin_ordinal": 16, "source_commit_lsn": "0/10", "source_end_lsn": "0/18", "certified_lsn": None}
        self.assertTrue(harness.require_feedback({"statuses": [(24, 24, 24)]}, row, 24))
        self.assertFalse(harness.require_feedback({"statuses": []}, row, 24))
        self.assertFalse(harness.require_feedback({"statuses": [(16, 16, 16)]}, row, 24))
        for statuses in ([(25, 25, 25)], [(24, 23, 24)], [(24, 24, 24), (16, 16, 16)]):
            with self.subTest(statuses=statuses), self.assertRaises(RuntimeError):
                harness.require_feedback({"statuses": statuses}, row, 16)

    def test_lsn_parser_rejects_missing_malformed_and_overflow(self):
        self.assertEqual(harness.lsn("1/10"), 2**32 + 16)
        for value in (None, "", "0/", "-1/0", "1/100000000", "100000000/0", 16):
            with self.subTest(value=value), self.assertRaises(RuntimeError):
                harness.lsn(value)

    def test_unexpected_follow_exit_and_timeout_retain_private_safe_cause_before_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            diagnostics = Path(temporary) / "diagnostics"
            for status in (7, 0, None):
                with self.subTest(status=status), patch.object(harness.clone, "DIAGNOSTIC_ROOT", diagnostics):
                    events = []
                    with self.assertRaises(harness.clone.SubprocessFailure) as failure:
                        with tempfile.TemporaryDirectory(dir=temporary) as owned, ExitStack() as cleanup:
                            root = Path(owned)
                            engine = root / "engine"
                            diagnostic = {"level": "error", "category": "Target", "exit_code": 7, "error_severity": "ERROR",
                                          "message": "native:Target: follow-target: PostgreSQL command failed (SQLSTATE 23505) fixture-password fixture-sensitive-row-private",
                                          "arbitrary": "fixture-sensitive-row-extra",
                                          "error_origin": {"file": "src/apply/follow.rs", "line": 91, "column": 17}}
                            if status == 0:
                                diagnostic["error_origin"]["file"] = "/private/fixture-password/fixture-sensitive-row-origin.rs"
                            engine.write_text("#!" + sys.executable + "\nimport sys,time\n" +
                                              "sys.stderr.write(" + repr(("unstructured fixture-password fixture-sensitive-row-plain" + "x" * 70000 + '\n{"level":"error","category":[]}\n' + json.dumps(diagnostic) + "\n")) + "); sys.stderr.flush()\n" +
                                              "print('ready',flush=True)\n" + ("time.sleep(30)\n" if status is None else "sys.exit(" + str(status) + ")\n"))
                            engine.chmod(0o700)
                            driver = harness.Driver(engine, {"PGPASSWORD": "fixture-password"}, root)
                            process = driver.start("source", "target", root / "native")
                            harness.clone.wait_for(lambda: process.output.read_bytes().strip() == b"ready", "fake engine stderr written", timeout=5)
                            if status is not None:
                                self.assertEqual(process.wait(timeout=5), status)
                            original_kill = driver.kill
                            def kill(child):
                                events.append(("kill", child.poll(), len(list(diagnostics.glob("*.json")))))
                                original_kill(child)
                            driver.kill = kill
                            driver.watch(process, cleanup)
                            process.follow_wire = SimpleNamespace(evidence=lambda: {"hits": 1, "data_bytes": 64, "begins": 1,
                                                                                  "commits": 0, "statuses": [(16, 16, 16)] * 100})
                            if status is None:
                                raise subprocess.TimeoutExpired("private-command fixture-sensitive-row-timeout", 0)
                            raise RuntimeError("follow exited before target predicate fixture-password fixture-sensitive-row-error")
                    metadata = failure.exception.metadata
                    self.assertEqual(metadata["command_kind"], "engine-follow-observation")
                    self.assertEqual(metadata["returncode"], status)
                    self.assertTrue(metadata["retained"])
                    self.assertFalse(Path(owned).exists())
                    self.assertEqual(events, [("kill", status, len(list(diagnostics.glob("*.json"))))])
                    private = json.loads((diagnostics / (metadata["diagnostic_id"] + ".stderr")).read_text())
                    expected_error = {"category": "Target", "exit_code": 7, "sqlstate": "23505", "stage": "follow-target"}
                    if status != 0:
                        expected_error["error_origin"] = diagnostic["error_origin"]
                    self.assertEqual(private["engine_errors"], [expected_error])
                    self.assertTrue(private["stderr_truncated"])
                    self.assertGreater(private["stderr_bytes"], 65536)
                    self.assertEqual(private["exception_type"], "TimeoutExpired" if status is None else "RuntimeError")
                    self.assertEqual(private["wire"]["status_count"], 100)
                    self.assertEqual(len(private["wire"]["statuses"]), 32)
                    self.assertTrue(private["trace"])
                    self.assertEqual(stat.S_IMODE(diagnostics.stat().st_mode), 0o700)
                    for path in diagnostics.iterdir():
                        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                        self.assertLess(path.stat().st_size, 65536)
                        self.assertNotIn("fixture-password", path.read_text())
                        self.assertNotIn("fixture-sensitive-row-", path.read_text())
                    public = str(failure.exception) + json.dumps(metadata)
                    self.assertNotIn("fixture-password", public)
                    self.assertNotIn("fixture-sensitive-row-", public)

    def test_follow_failure_watch_preserves_existing_private_failure(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path(temporary) / "diagnostics"):
            driver = object.__new__(harness.Driver)
            driver.kill = Mock()
            process = SimpleNamespace()
            original = harness.clone.SubprocessFailure({"diagnostic_id": "already-retained", "retained": True})
            with self.assertRaises(harness.clone.SubprocessFailure) as failure:
                with ExitStack() as cleanup:
                    driver.watch(process, cleanup)
                    raise original
            self.assertIs(failure.exception, original)
            driver.kill.assert_called_once_with(process)
            self.assertFalse(harness.clone.DIAGNOSTIC_ROOT.exists())
            with ExitStack() as cleanup:
                driver.watch(process, cleanup)
            self.assertEqual(driver.kill.call_count, 2)
            self.assertFalse(harness.clone.DIAGNOSTIC_ROOT.exists())

    def test_follow_stop_cannot_treat_exit_zero_or_still_running_as_success(self):
        for status in (3, 130, -9, -15):
            harness.require_stopped(status)
        for status in (0, None):
            with self.subTest(status=status), self.assertRaises(RuntimeError):
                harness.require_stopped(status)

    def test_receipt_cannot_pass_with_missing_faults_or_fabricated_completion(self):
        manifest = harness.load_manifest()
        report = {"pair": "16-18", "server_versions": ["16.15", "18.6"],
                  "client_versions": dict.fromkeys(("pg_dump", "pg_restore", "psql"), "18.6"), "linked_libpq": 180006,
                  "mode": "ongoing_follow", "successful_follow_exit": False, "power_loss_tested": False,
                  "cases": {name: {"observed": True, "frontier": 16, "feedback_observed": True} for name in harness.CASES}}
        work = {"migration_uid": "12345678-9abc-def0-1234-56789abcdef0", "run_id": "123456789abcdef0123456789abcdef0", "workdir_id": "fixture", "checkpoint_generation": 1}
        raw = {"protocol_version": 1, "engine_version": "0.1.0", "command": "clone", "capabilities": ["clone.single_db"],
               "work": work, "phase": "streaming", "copy": {name: {"total": 0, "done": 0} for name in ("tables", "indexes", "bytes")}}
        report["report_validation"] = {"mode": "local_m3_contract", "reports": [{"binding": {"command": "clone", "work": work}, "raw_json": json.dumps(raw), "probe_checked": False}]}
        cases = report["cases"]
        for name in ("before_commit_rollback", "after_commit_before_feedback", "partial_receive_transaction"):
            cases[name].update(fault_hits=1, feedback_held=True)
        for name, boundary in (("before_commit_rollback", "before_commit"), ("after_commit_before_feedback", "after_commit")):
            cases[name]["data_fault"] = self.data_fault_fixture(boundary)
        rollback = self.diagnostic_receipt()["cases"]["before_commit_rollback"]["post_crash_rollback"]
        cases["before_commit_rollback"]["post_crash_rollback"] = rollback
        cases["after_commit_before_feedback"]["post_crash_preservation"] = {**rollback, "outcome": "committed_preserved"}
        for name in ("generated_refusal", "check_refusal", "domain_check_refusal", "expression_index_refusal", "partial_index_refusal"):
            cases[name]["source_and_target_unchanged"] = True
        for name in ("confirmed_flush_gap", "source_wal_loss"):
            cases[name]["target_unchanged"] = True
        for name in ("predata_handoff_refusal", "postdata_refusal"):
            cases[name]["uncertain_stage_refused"] = True
        cases["repeated_restart"]["restarts"] = 3
        cases["oracle_mutation"]["mutation_detected"] = True
        cases["active_foreign_slot"].update(resources_unchanged=True, active_pid=12)
        cases["copy_restart"].update(previous_generation=1, blocked_backend_pid=13)
        cases["backpressure"].update(seconds=65, wal_sender_timeout_seconds=60, feedback_held=True,
                                      queue={"messages": 1024, "owned_bytes": 8192, "limit_messages": 1024, "limit_bytes": 8 * 1024 * 1024})
        harness.validate_receipt(report, manifest)
        harness.validate_receipt({**report, "receipt_scope": "smoke", "cases": {"initial_copy_dml": report["cases"]["initial_copy_dml"]}}, manifest)
        for bad in ({**report, "report_validation": {}}, {**report, "cases": {}}, {**report, "successful_follow_exit": True}, {**report, "power_loss_tested": True},
                    {**report, "cases": {**report["cases"], "initial_copy_dml": {"observed": True}}},
                    {**report, "cases": {**report["cases"], "backpressure": {"observed": True, "seconds": 60, "wal_sender_timeout_seconds": 60}}}):
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                harness.validate_receipt(bad, manifest)
        for name, key in (("before_commit_rollback", "post_crash_rollback"), ("after_commit_before_feedback", "post_crash_preservation")):
            preserved = cases[name][key]
            for witness in (None, False, {}, {**preserved, "sessions": []}, {**preserved, "checkpoint_unchanged": False},
                            {**preserved, "fingerprints_unchanged": False}, {**preserved, "outcome": "unknown"},
                            {**preserved, "outcome": "committed_preserved" if key == "post_crash_rollback" else "rolled_back"},
                            {**preserved, "sessions": [{**preserved["sessions"][0], "pid": 124}]}):
                bad = copy.deepcopy(report)
                bad["cases"][name][key] = witness
                with self.subTest(case=name, preservation=witness), self.assertRaises(RuntimeError):
                    harness.validate_receipt(bad, manifest)
            bad = copy.deepcopy(report)
            del bad["cases"][name][key]
            with self.assertRaises(RuntimeError):
                harness.validate_receipt(bad, manifest)

    @staticmethod
    def data_fault_fixture(boundary="before_commit"):
        run = "123456789abcdef0123456789abcdef0"
        return {"source": {"connection": 1, "baseline_end": 8, "final_lsn": 16, "commit_lsn": 16, "end_lsn": 24,
                           "xid": 3, "dml_count": 2, "complete": True, "begin_forwarded": True, "commit_forwarded": True},
                "fault": {"status": "observed", "scope": "wire_fault_observation", "boundary": boundary, "protocol": "extended",
                          "connection": 2, "backend_pid": 123, "begin_observed": True,
                          "command_tags": {"insert_0_1": 1, "update_1": 2, "delete_1": 0, "other_dml": 0},
                          "overflow": False, "classification_invalid": False},
                "session": {"pid": 123, "backend_start": "2026-11-08 12:00:00.123+00", "database_oid": 42,
                            "fence_class": 0x5250474d, "fence_slot": zlib.crc32(bytes.fromhex(run))}, "run_id": run}

    def test_data_fault_requires_actual_data_canonical_source_and_owned_session(self):
        proof = self.data_fault_fixture()
        harness.validate_data_fault(proof, "before_commit", {proof["run_id"]})
        mutations = [("source", "dml_count", 0), ("source", "dml_count", 1), ("source", "complete", False),
                     ("source", "begin_forwarded", False), ("source", "commit_forwarded", False),
                     ("source", "commit_lsn", 17), ("source", "baseline_end", 24), ("source", "xid", 2),
                     ("fault", "status", "unavailable"), ("fault", "backend_pid", 0), ("fault", "backend_pid", 124),
                     ("fault", "connection", True), ("fault", "boundary", "after_commit"), ("fault", "protocol", "backend"),
                     ("fault", "begin_observed", False), ("fault", "overflow", True), ("fault", "classification_invalid", True),
                     ("session", "fence_slot", 0), ("session", "fence_class", 0), ("session", "database_oid", 0)]
        for section, key, value in mutations:
            bad = copy.deepcopy(proof)
            bad[section][key] = value
            with self.subTest(section=section, key=key, value=value), self.assertRaises(RuntimeError):
                harness.validate_data_fault(bad, "before_commit", {proof["run_id"]})
        for key, value in (("insert_0_1", 0), ("update_1", 1), ("delete_1", 1), ("other_dml", 1)):
            bad = copy.deepcopy(proof)
            bad["fault"]["command_tags"][key] = value
            with self.subTest(tag=key), self.assertRaises(RuntimeError):
                harness.validate_data_fault(bad, "before_commit", {proof["run_id"]})
        for bad in ({}, {**proof, "run_id": "0" * 32}, {**proof, "private": "forbidden"}):
            with self.assertRaises(RuntimeError):
                harness.validate_data_fault(bad, "before_commit", {proof["run_id"]})

    def test_data_fault_selects_exactly_one_new_forwarded_fixture_transaction(self):
        proof = self.data_fault_fixture()
        tx = {key: value for key, value in proof["source"].items() if key != "baseline_end"}
        stream = {"connection": 1, "confirmed": True, "complete": True, "invalid": False, "overflow": False, "transactions": []}
        initial = {"logical": {"overflow": False, "streams": [stream]}}
        current = copy.deepcopy(initial)
        current["logical"]["streams"][0]["transactions"] = [tx]
        previous = {"source_end_lsn": "0/8", "target_database": 42}
        work = {"run_id": proof["run_id"]}
        self.assertEqual(harness.require_data_fault(initial, current, proof["fault"], previous, work, proof["session"]), proof)
        changes = [initial, {"logical": {"overflow": True, "streams": [stream]}},
                   {"logical": {"overflow": False, "streams": []}},
                   {"logical": {"overflow": False, "streams": [stream, stream]}}]
        for key, value in (("transactions", [tx, tx]), ("connection", 2), ("complete", False), ("invalid", True), ("overflow", True)):
            changed = copy.deepcopy(current)
            changed["logical"]["streams"][0][key] = value
            changes.append(changed)
        for changed in changes:
            with self.subTest(evidence=changed), self.assertRaises(RuntimeError):
                harness.require_data_fault(initial, changed, proof["fault"], previous, work, proof["session"])
        for wrong_previous in ({**previous, "target_database": 43}, {**previous, "source_end_lsn": "0/18"}):
            with self.assertRaises(RuntimeError):
                harness.require_data_fault(initial, current, proof["fault"], wrong_previous, work, proof["session"])
        prior = copy.deepcopy(initial)
        prior["logical"]["streams"][0]["transactions"] = [{**tx, "xid": 9}]
        current["logical"]["streams"][0]["transactions"] = [tx, tx]
        with self.assertRaises(RuntimeError):
            harness.require_data_fault(prior, current, proof["fault"], previous, work, proof["session"])

    def test_held_session_reads_exact_numeric_pid_database_and_run_fence(self):
        proof = self.data_fault_fixture()
        database = object.__new__(harness.Database)
        database.sql = Mock(return_value=json.dumps([proof["session"]]).encode())
        work = {"run_id": proof["run_id"]}
        self.assertEqual(database.held_session("owned", proof["fault"], work), proof["session"])
        name, query = database.sql.call_args.args
        self.assertEqual(name, "owned")
        for required in ("a.pid=123", "l.database=a.datid", "l.classid=" + str(0x5250474d),
                         "l.objid=" + str(proof["session"]["fence_slot"]), "l.objsubid=2", "l.mode='ExclusiveLock'", "l.granted", "current_database()"):
            self.assertIn(required, query)
        for rows in ([], [proof["session"], proof["session"]]):
            database.sql.return_value = json.dumps(rows).encode()
            self.assertEqual(database.held_session("owned", proof["fault"], work), {"status": "unavailable"})
        database.sql.reset_mock()
        self.assertEqual(database.held_session("owned", {**proof["fault"], "backend_pid": True}, work), {"status": "unavailable"})
        database.sql.assert_not_called()

    @staticmethod
    def diagnostic_receipt():
        work = {"migration_uid": "12345678-9abc-def0-1234-56789abcdef0", "run_id": "123456789abcdef0123456789abcdef0", "workdir_id": "fixture", "checkpoint_generation": 1}
        raw = {"protocol_version": 1, "engine_version": "0.1.0", "command": "clone", "capabilities": ["clone.single_db"],
               "work": work, "phase": "streaming", "copy": {name: {"total": 0, "done": 0} for name in ("tables", "indexes", "bytes")}}
        record = {"binding": {"command": "clone", "work": work}, "raw_json": json.dumps(raw), "probe_checked": False}
        sessions = [{"pid": 123, "backend_start": "2026-11-08 12:00:00.123+00", "database_oid": 42}]
        return {"pair": "16-16", "server_versions": ["16.15", "16.15"], "client_versions": dict.fromkeys(("pg_dump", "pg_restore", "psql"), "16.15"),
                "linked_libpq": 160015, "mode": "ongoing_follow", "successful_follow_exit": False, "power_loss_tested": False,
                "receipt_scope": "diagnostic", "diagnostic_case": "before_commit_rollback",
                "report_validation": {"mode": "local_m3_contract", "reports": [record, record]},
                "cases": {"before_commit_rollback": {"observed": True, "frontier": 24, "feedback_observed": True, "fault_hits": 1, "feedback_held": True,
                          "data_fault": FollowHarnessTests.data_fault_fixture(),
                          "post_crash_rollback": {"sessions": sessions, "checkpoint_unchanged": True, "fingerprints_unchanged": True, "outcome": "rolled_back"}}}}

    def test_diagnostic_receipt_is_bound_to_one_complete_case_and_never_full_or_smoke(self):
        manifest, report = harness.load_manifest(), self.diagnostic_receipt()
        harness.validate_receipt(report, manifest, diagnostic_case="before_commit_rollback")
        with self.assertRaises(RuntimeError):
            harness.validate_receipt(report, manifest)
        case = report["cases"]["before_commit_rollback"]
        for change in ({"receipt_scope": "full"}, {"receipt_scope": "smoke"}, {"diagnostic_case": "initial_copy_dml"},
                       {"cases": {}}, {"cases": {**report["cases"], "initial_copy_dml": {"observed": True}}},
                       {"cases": {"before_commit_rollback": {**case, "frontier": 0}}},
                       {"cases": {"before_commit_rollback": {**case, "fault_hits": 0}}},
                       {"cases": {"before_commit_rollback": {**case, "data_fault": {}}}},
                       {"cases": {"before_commit_rollback": {**case, "post_crash_rollback": {**case["post_crash_rollback"], "sessions": [{"pid": 124, "backend_start": "2026-11-08 12:00:00.123+00", "database_oid": 42}]}}}},
                       {"cases": {"before_commit_rollback": {**case, "post_crash_rollback": {}}}},
                       {"cases": {"before_commit_rollback": {**case, "post_crash_rollback": {**case["post_crash_rollback"], "outcome": "committed_preserved"}}}},
                       {"report_validation": {**report["report_validation"], "reports": report["report_validation"]["reports"][:1]}}):
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                harness.validate_receipt({**report, **change}, manifest, diagnostic_case="before_commit_rollback")

    def test_diagnostic_cli_requires_explicit_receipt_and_preserves_default_selection(self):
        for arguments, selected in (([], None), (["--smoke"], None),
                                    (["--diagnostic-case", "before_commit_rollback", "--receipt", "owned-diagnostic.json"], "before_commit_rollback")):
            seen = {}
            def outer(args, **kwargs):
                seen.update(args=args, **kwargs)
            with self.subTest(arguments=arguments), patch.object(sys, "argv", ["follow.py", "--pair", "16-16", *arguments]), patch.object(harness.clone, "outer", outer):
                self.assertEqual(harness.main(), 0)
            self.assertEqual(seen["args"].diagnostic_case, selected)
            if selected:
                seen["receipt_validator"](self.diagnostic_receipt(), harness.load_manifest())
            else:
                with self.assertRaises(RuntimeError):
                    seen["receipt_validator"](self.diagnostic_receipt(), harness.load_manifest())
        for arguments in (["--diagnostic-case", "before_commit_rollback"],
                          ["--diagnostic-case", "before_commit_rollback", "--smoke", "--receipt", "owned.json"],
                          ["--diagnostic-case", "initial_copy_dml", "--receipt", "owned.json"]):
            with self.subTest(arguments=arguments), patch.object(sys, "argv", ["follow.py", "--pair", "16-16", *arguments]), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as failed:
                harness.main()
            self.assertEqual(failed.exception.code, 2)

    def test_outer_forwards_diagnostic_selector_and_never_overwrites_existing_receipt(self):
        for race in (False, True):
            with self.subTest(race=race), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                output, calls, created = root / "diagnostic.json", [], {}
                args = SimpleNamespace(pair="16-16", report_probe=None, receipt=str(output), smoke=False, diagnostic_case="before_commit_rollback")
                def run(command, **_kwargs):
                    calls.append(command)
                    if command[1:3] == ["image", "inspect"]:
                        return b"fixture-image" if "--format" in command else b'[{"Id":"fixture-image"}]'
                    if command[1] == "run":
                        return (b'a' if "source" in command else b'b') * 64
                    if command[1] == "create":
                        cid = str(len(created) + 1).encode() * 64
                        created[cid.decode()] = command
                        return cid
                    if command[1:3] == ["start", "--attach"]:
                        created_command = created[command[-1]]
                        if "--inside" in created_command:
                            self.assertEqual(created_command[-2:], ["--diagnostic-case", "before_commit_rollback"])
                            receipt_mount = next(value.removesuffix(":/receipt") for value in created_command if value.endswith(":/receipt"))
                            (Path(receipt_mount) / "result.json").write_text(json.dumps(self.diagnostic_receipt()))
                            if race:
                                output.write_text("existing full receipt")
                        return b""
                    return b""
                cleanup_calls = []
                def external(command, **_kwargs):
                    cleanup_calls.append(command)
                    if command[1] == "inspect":
                        label = next(item for call in calls if "--label" in call for item in [call[call.index("--label") + 1]])
                        return SimpleNamespace(returncode=0, stdout=b"other-owner" if race else label.split("=", 1)[1].encode())
                    return SimpleNamespace(returncode=1 if command[1:3] == ["container", "exists"] else 0, stdout=b"")
                with patch.object(harness.clone, "ROOT", root), patch.object(harness.clone, "DIAGNOSTIC_ROOT", root / "diagnostics"), \
                        patch.object(harness.clone.shutil, "which", return_value="/owned/podman"), patch.object(harness.clone, "run", run), \
                        patch.object(harness.clone.subprocess, "run", external), redirect_stdout(io.StringIO()):
                    validator = lambda report, manifest: harness.validate_receipt(report, manifest, diagnostic_case=args.diagnostic_case)
                    if race:
                        with self.assertRaises(FileExistsError):
                            harness.clone.outer(args, program="follow", manifest=harness.load_manifest(), receipt_validator=validator)
                        self.assertEqual(output.read_text(), "existing full receipt")
                    else:
                        harness.clone.outer(args, program="follow", manifest=harness.load_manifest(), receipt_validator=validator)
                        self.assertEqual(json.loads(output.read_text())["receipt_scope"], "diagnostic")
                    ledger_path, = root.glob("diagnostic.json.*.runtime.json")
                    ledger = json.loads(ledger_path.read_text())
                    self.assertEqual(ledger["scope"], "runtime_resource_ownership")
                    self.assertEqual({entry["role"] for entry in ledger["containers"]}, {"source", "target", "build", "runner"})
                    self.assertEqual(ledger["cleanup_complete"], not race)
                    self.assertEqual(stat.S_IMODE(ledger_path.stat().st_mode), 0o600)
                    owned_ids = {entry["id"] for entry in ledger["containers"]}
                    self.assertEqual({command[-1] for command in cleanup_calls if command[1] == "rm"}, set() if race else owned_ids)
                    self.assertEqual({command[-1] for command in cleanup_calls if command[1:3] == ["container", "exists"]}, owned_ids)
                    before = len(calls)
                    with self.assertRaises((RuntimeError, FileExistsError)):
                        harness.clone.outer(args, program="follow", manifest=harness.load_manifest(), receipt_validator=validator)
                    self.assertEqual(len(calls), before)

    def test_inner_diagnostic_runs_only_selected_case_and_checks_rollback_before_resume(self):
        for diagnostic, noop in ((None, False), ("before_commit_rollback", False), ("before_commit_rollback", True)):
            with self.subTest(diagnostic=diagnostic, noop=noop), tempfile.TemporaryDirectory() as temporary:
                root, events, created = Path(temporary), [], []
                expected_case = "before_commit_rollback" if diagnostic else "dml"
                report = self.diagnostic_receipt()
                row = self.checkpoint_row()
                databases = []
                for host in ("source", "target"):
                    database = Mock()
                    def create(name, *_args, host=host):
                        created.append((host, name))
                        if name not in ("src_" + expected_case, "dst_" + expected_case):
                            self.fail("unselected follow case executed")
                        return host + "-connection"
                    database.create.side_effect = create
                    database.sql.side_effect = lambda _name, query: b"16.15" if query == "SHOW server_version;" else b"t" if "SELECT NOT active" in query else b"0/18"
                    database.fingerprint.return_value = {"table": {"rows": 1}}
                    database.authority.return_value = row
                    database.slot.return_value = {"active": True, "confirmed_flush_lsn": "0/18"}
                    databases.append(database)
                source, target = databases
                witness = report["cases"]["before_commit_rollback"]["post_crash_rollback"]
                proof = self.data_fault_fixture()
                if noop:
                    proof["fault"]["command_tags"].update(insert_0_1=0, update_1=1)
                proof["session"]["database_oid"] = row["target_database"]
                witness["sessions"][0]["database_oid"] = row["target_database"]
                target.held_session.return_value = proof["session"]
                posthold = {"checkpoint": {**row, "certified_lsn": "0/18", "barrier_counter": 2}, "fingerprints": {"table": {"rows": 1}}, "sessions": witness["sessions"]}
                target.crash_baseline.side_effect = lambda *_args: events.append("baseline") or posthold
                def preserved(*arguments, outcome):
                    self.assertIs(arguments[-1], posthold)
                    self.assertEqual(outcome, "rolled_back")
                    events.append("rollback_checked")
                    return witness
                target.require_crash_preservation.side_effect = preserved
                class Driver:
                    def __init__(self, *_args, **_kwargs):
                        self.work, self.reports = {}, []
                    def start(self, _source, _target, state, *arguments, **_kwargs):
                        events.append("resume" if "--resume" in arguments else "start")
                        self.work[state] = report["report_validation"]["reports"][0]["binding"]["work"]
                        state.mkdir(exist_ok=True)
                        (state / "manifest.json").write_text(json.dumps({"follow_state": {"binding": {"slot": {"intent": {"binding": {"slot_name": "owned"}}}}}}))
                        return SimpleNamespace(poll=lambda: None)
                    def watch(self, *_args):
                        pass
                    def ready(self, _process):
                        self.reports.append(report["report_validation"]["reports"][0])
                    def stop(self, _process, *, crash=False):
                        events.append("crash" if crash else "stop")
                wires = []
                class Wire:
                    def __init__(self, *_args):
                        wires.append(self)
                        self.held, self.release = threading.Event(), threading.Event()
                        self.statuses, self.hits = [(24, 24, 24)], 0
                    def __enter__(self):
                        return self
                    def __exit__(self, *_args):
                        pass
                    def connection(self, connection):
                        return connection
                    def arm(self):
                        self.hits = 1
                        self.held.set()
                        for wire in wires:
                            wire.statuses.append((24, 24, 24))
                            wire.hits = 1
                    def abort_hold(self):
                        events.append("abort_commit")
                        self.release.set()
                    def evidence(self):
                        tx = {key: value for key, value in proof["source"].items() if key != "baseline_end"}
                        tx.update(final_lsn=32, commit_lsn=32, end_lsn=40)
                        stream = {"connection": 1, "confirmed": True, "complete": True, "invalid": False, "overflow": False,
                                  "transactions": [tx] if self.hits else []}
                        return {"statuses": self.statuses, "hits": self.hits, "logical": {"overflow": False, "streams": [stream]}}
                    def fault_observation(self):
                        events.append("data_checked")
                        return proof["fault"]
                args = SimpleNamespace(pair="16-16", binary="/owned/engine", receipt=str(root / "receipt.json"), report_probe=None,
                                       smoke=diagnostic is None, diagnostic_case=diagnostic)
                with patch.object(harness, "Database", side_effect=databases), patch.object(harness, "Driver", Driver), patch.object(harness, "WireGate", Wire), \
                        patch.object(harness.clone, "run", return_value=b"libpq.so.5 => /owned/libpq.so.5"), patch.object(harness.clone, "tool_version", return_value="16.15"), \
                        patch.object(harness.ctypes, "CDLL", return_value=SimpleNamespace(PQlibVersion=lambda: 160015)), patch.dict(harness.os.environ, {"PGPASSWORD": "fixture-password"}), \
                        patch.object(harness, "check_workload", side_effect=lambda *_args: events.append("workload_preflight")), redirect_stdout(io.StringIO()) as stdout:
                    if noop:
                        with self.assertRaisesRegex(RuntimeError, "newly forwarded fixture DATA"):
                            harness.inner(args)
                        self.assertNotIn("crash", events)
                        self.assertNotIn("baseline", events)
                        self.assertNotIn("resume", events)
                        self.assertFalse(Path(args.receipt).exists())
                        continue
                    harness.inner(args)
                receipt = json.loads(Path(args.receipt).read_text())
                self.assertEqual(set(receipt["cases"]), {"before_commit_rollback" if diagnostic else "initial_copy_dml"})
                self.assertEqual(receipt["receipt_scope"], "diagnostic" if diagnostic else "smoke")
                self.assertEqual(len(created), 2)
                if diagnostic:
                    self.assertNotIn("workload_preflight", events)
                    self.assertLess(events.index("baseline"), events.index("crash"))
                    self.assertLess(events.index("data_checked"), events.index("baseline"))
                    self.assertLess(events.index("crash"), events.index("abort_commit"))
                    self.assertLess(events.index("abort_commit"), events.index("rollback_checked"))
                    self.assertLess(events.index("rollback_checked"), events.index("resume"))
                    self.assertIn('"follow_e2e": "diagnostic_passed"', stdout.getvalue())
                    self.assertNotIn('"follow_e2e": "passed"', stdout.getvalue())
                else:
                    self.assertIn("workload_preflight", events)
                    self.assertNotIn("crash", events)

    def test_failure_capture_bounds_source_and_target_proxy_classes_without_replacing_cause(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path(temporary) / "diagnostics"):
            stderr = Path(temporary) / "stderr"
            stderr.write_bytes(b"")
            source = SimpleNamespace(lock=threading.Lock(), errors=["ValueError"], evidence=lambda: {"hits": 0, "data_bytes": 0, "begins": 0, "commits": 0, "statuses": []})
            target = SimpleNamespace(lock=threading.Lock(), errors=["RuntimeError"] * 20 + ["fixture-sensitive-row-private"])
            process = SimpleNamespace(poll=lambda: 7, error_output=stderr, environment={}, follow_wire=source, follow_target_wire=target,
                                      follow_observe=lambda: {"scope": "visible-observation", "target": {"status": "unavailable"}})
            driver = object.__new__(harness.Driver)
            driver.kill = lambda _process: None
            original = harness.clone.SubprocessFailure({"diagnostic_id": "primary", "retained": True})
            with self.assertRaises(harness.clone.SubprocessFailure) as failed:
                with ExitStack() as cleanup:
                    driver.watch(process, cleanup)
                    raise original
            self.assertIs(failed.exception, original)
            metadata = original.metadata["follow_observation"]
            retained = json.loads((harness.clone.DIAGNOSTIC_ROOT / (metadata["diagnostic_id"] + ".stderr")).read_text())
            self.assertEqual(retained["proxy_errors"]["source"]["classes"], ["ValueError"])
            self.assertEqual(retained["proxy_errors"]["target"]["count"], 21)
            self.assertEqual(len(retained["proxy_errors"]["target"]["classes"]), 16)
            self.assertTrue(retained["proxy_errors"]["target"]["truncated"])
            self.assertNotIn("fixture-", json.dumps(retained))

    def test_crash_preservation_requires_exact_closed_sessions_then_checkpoint_and_fingerprint(self):
        database = object.__new__(harness.Database)
        checkpoint, fingerprints = {"target_database": 42}, {"table": {"rows": 1}}
        sessions = [{"pid": 123, "backend_start": "2026-11-08 12:00:00.123+00", "database_oid": 42}]
        database.authority = Mock(return_value=checkpoint)
        database.fingerprint = Mock(return_value=fingerprints)
        database.sql = Mock(return_value=json.dumps(sessions).encode())
        baseline = database.crash_baseline("owned", Path("state"), {}, {})
        for bad in ([], [{**sessions[0], "database_oid": 43}], [{**sessions[0], "backend_start": "fixture-sensitive-row-private"}], sessions * 65):
            database.sql.return_value = json.dumps(bad).encode()
            with self.subTest(bad=bad), self.assertRaises(RuntimeError):
                database.crash_baseline("owned", Path("state"), {}, {})
        for outcome in ("rolled_back", "committed_preserved"):
            database.sql = Mock(side_effect=[b'f', b't'])
            result = database.require_crash_preservation("owned", Path("state"), {}, {}, baseline, outcome=outcome)
            self.assertTrue(result["checkpoint_unchanged"] and result["fingerprints_unchanged"])
            self.assertEqual(result["sessions"], sessions)
            self.assertEqual(result["outcome"], outcome)
        query = database.sql.call_args.args[1]
        for field in ("backend_start", "database_oid", "pid"):
            self.assertIn(field, query)
        database.sql = Mock(return_value=b't')
        for outcome in ("rolled_back", "committed_preserved"):
            for field, value in (("authority", {"target_database": 42, "changed": True}), ("fingerprint", {"table": {"rows": 2}})):
                method = getattr(database, field)
                method.return_value = value
                with self.subTest(outcome=outcome, field=field), self.assertRaises(RuntimeError):
                    database.require_crash_preservation("owned", Path("state"), {}, {}, baseline, outcome=outcome)
                method.return_value = checkpoint if field == "authority" else fingerprints
        database.sql.reset_mock()
        with self.assertRaises(RuntimeError):
            database.require_crash_preservation("owned", Path("state"), {}, {}, baseline, outcome="unknown")
        database.sql.assert_not_called()

    def test_before_commit_abort_discards_held_request_and_closes_only_its_session(self):
        from protocol_fault import receive
        with self.wire_pair("before_commit") as (gate, producer, backend):
            startup = self.wire_startup()
            producer.sendall(startup)
            self.assertTrue(receive(backend, len(startup)) == startup)
            gate.arm()
            producer.sendall(self.wire_frame(b'Q', b'COMMIT\0'))
            self.assertTrue(gate.held.wait(timeout=2))
            producer.shutdown(socket.SHUT_RDWR)
            producer.close()
            gate.abort_hold()
            self.assertEqual(backend.recv(1), b'')
            harness.clone.wait_for(lambda: all(not worker.is_alive() for worker in gate.threads), "aborted target session reaped", timeout=2)
            self.assertEqual(gate.evidence()["hits"], 1)
            other_client, other_producer = socket.socketpair()
            other_server, other_backend = socket.socketpair()
            gate.sockets.extend((other_client, other_server))
            other_backend.settimeout(2)
            worker = threading.Thread(target=gate.downstream, args=(other_client, other_server), daemon=True)
            gate.threads.append(worker)
            worker.start()
            try:
                request = startup + self.wire_frame(b'Q', b'COMMIT\0')
                other_producer.sendall(request)
                self.assertTrue(receive(other_backend, len(request)) == request)
            finally:
                other_producer.close()
                other_backend.close()
            with self.assertRaises(RuntimeError):
                gate.abort_hold()

    def test_abort_hold_refuses_other_modes_and_absent_or_released_holds(self):
        for mode in ("observe", "before_commit", "after_begin", "after_commit"):
            with self.subTest(mode=mode), WireGate("fixture", mode) as gate:
                with self.assertRaises(RuntimeError):
                    gate.abort_hold()
                gate.held.set()
                if mode != "before_commit":
                    with self.assertRaises(RuntimeError):
                        gate.abort_hold()
                gate.release.set()
                with self.assertRaises(RuntimeError):
                    gate.abort_hold()

    def test_existing_before_commit_gate_can_forward_queued_commit_after_client_death(self):
        from protocol_fault import receive
        with self.wire_pair("before_commit") as (gate, producer, backend):
            startup = self.wire_startup()
            producer.sendall(startup)
            self.assertTrue(receive(backend, len(startup)) == startup)
            gate.arm()
            commit = self.wire_frame(b'Q', b'COMMIT\0')
            producer.sendall(commit)
            self.assertTrue(gate.held.wait(timeout=2))
            producer.shutdown(socket.SHUT_RDWR)
            producer.close()
            gate.release.set()
            self.assertTrue(receive(backend, len(commit)) == commit)

    def test_live_wire_snapshot_retains_ordinary_frames_and_pid_without_secrets(self):
        from protocol_fault import receive
        with self.wire_pair("observe") as (gate, producer, backend):
            startup = self.wire_startup()
            producer.sendall(startup)
            self.assertTrue(receive(backend, len(startup)) == startup)
            response = self.wire_frame(b'K', struct.pack("!II", 123, 0x1234abcd)) + self.wire_frame(b'G', b'\0\0\1\0\0')
            backend.sendall(response)
            self.assertTrue(receive(producer, len(response)) == response)
            data = self.wire_frame(b'd', b'fixture-sensitive-row-private')
            producer.sendall(data)
            self.assertTrue(receive(backend, len(data)) == data)
            snapshot = gate.snapshot()
            connection = snapshot["connections"][0]
            self.assertEqual(connection["backend_pid"], 123)
            self.assertEqual(connection["mode"], "copy_in")
            self.assertEqual(connection["counts"]["client"]["forwarded"]["d"], 1)
            self.assertNotIn("fixture-", json.dumps(snapshot))
            self.assertNotIn(str(0x1234abcd), json.dumps(snapshot))
            with gate.lock:
                self.assertEqual(gate.snapshot()["status"], "unavailable")
            for _ in range(64):
                gate.new_observation()
            snapshot = gate.snapshot()
            self.assertTrue(snapshot["overflow"])
            self.assertEqual(len(snapshot["connections"]), 64)

    def test_live_backend_snapshot_drops_query_unknown_apps_and_unowned_locks(self):
        database = object.__new__(harness.Database)
        database.application = "owned-app"
        raw = {"database_oid": 42, "backends": [{"pid": 123, "backend_start": "2026-11-08 12:00:00+00", "backend_type": "client backend", "database_oid": 42, "state": "active", "wait_event_type": "Client", "wait_event": "ClientRead", "application_name": "fixture-sensitive-row-private", "query": "fixture-password"}], "locks": [{"pid": 123, "database": 42, "classid": 0x5250474d, "objid": 2, "objsubid": 1, "mode": "ExclusiveLock", "granted": True}, {"pid": 123, "database": 42, "classid": 1, "objid": 2, "objsubid": 1, "mode": "ExclusiveLock", "granted": True}]}
        database.sql = Mock(return_value=json.dumps(raw).encode())
        snapshot = database.startup_snapshot("owned")
        self.assertEqual(snapshot["backends"][0]["application_name"], "other")
        self.assertEqual(len(snapshot["locks"]), 1)
        self.assertTrue(snapshot["unknown"])
        self.assertNotIn("fixture-", json.dumps(snapshot))
        raw["backends"] *= 65
        database.sql.return_value = json.dumps(raw).encode()
        self.assertTrue(database.startup_snapshot("owned")["overflow"])
        database.sql.side_effect = RuntimeError("fixture-password")
        self.assertEqual(database.startup_snapshot("owned")["status"], "unavailable")

    def test_live_process_snapshot_is_owned_bounded_and_never_reads_arguments(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for pid, parent in ((10, 1), (11, 10), (12, 1)):
                folder = root / str(pid)
                folder.mkdir()
                (folder / "stat").write_text(f"{pid} (fixture-sensitive-row-private) S {parent} 0 0")
                (folder / "exe").symlink_to("/owned/pg_restore")
                (folder / "cmdline").write_text("fixture-password")
            snapshot = harness.process_snapshot(10, proc_root=root)
            self.assertEqual([p["pid"] for p in snapshot["processes"]], [10, 11])
            self.assertNotIn("fixture-", json.dumps(snapshot))
            self.assertTrue(harness.process_snapshot(10, proc_root=root, limit=1)["overflow"])

    def test_startup_database_captures_share_one_four_second_budget(self):
        clock, deadlines = [0.0], []
        class Database:
            def startup_snapshot(self, _name):
                deadlines.append(self._observation_deadline)
                clock[0] += 2
                return {"status": "observed"}
        source, target = Database(), Database()
        with patch.object(harness.time, "monotonic", side_effect=lambda: clock[0]), patch.object(harness, "process_snapshot", return_value={"status": "unavailable"}):
            snapshot = harness.startup_capture(SimpleNamespace(pid=42), source, target, "source", "target", 4)
        self.assertEqual(deadlines, [4, 4])
        self.assertEqual(clock[0], 4)
        self.assertFalse(hasattr(source, "_observation_deadline"))
        self.assertFalse(hasattr(target, "_observation_deadline"))
        self.assertEqual(snapshot["scope"], "startup_diagnostic")

    def test_startup_passive_capture_keeps_original_sixty_second_deadline(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            output.write_bytes(b"")
            clock, captured = [0.0], []
            def capture(deadline):
                captured.append(deadline)
                clock[0] += 4
                return {"scope": "startup_diagnostic"}
            process = SimpleNamespace(poll=lambda: None, output=output, follow_startup_capture=capture)
            driver = object.__new__(harness.Driver)
            def wait(probe, _description, *, timeout):
                self.assertEqual(timeout, 60)
                clock[0] = 45
                self.assertFalse(probe())
                self.assertFalse(probe())
                self.assertEqual(captured, [49])
                clock[0] = 60
                probe()
            with patch.object(harness.time, "monotonic", side_effect=lambda: clock[0]), patch.object(harness.clone, "wait_for", wait), self.assertRaises(RuntimeError):
                driver.ready(process)
            self.assertEqual(len(process.follow_startup_snapshots), 1)

    def test_startup_failure_does_not_capture_older_stopped_processes(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path(temporary) / "diagnostics"):
            stderr = Path(temporary) / "stderr"
            stderr.write_bytes(b"")
            events = []
            driver = object.__new__(harness.Driver)
            driver.kill = lambda _process: None
            with self.assertRaises(harness.clone.SubprocessFailure):
                with ExitStack() as cleanup:
                    for name in ("old", "latest"):
                        capture = lambda _deadline, name=name: events.append(name) or {"status": "unavailable"}
                        process = SimpleNamespace(poll=lambda: 7, error_output=stderr, environment={}, follow_startup_capture=capture)
                        driver.watch(process, cleanup)
                    raise RuntimeError("failure")
            self.assertEqual(events, ["latest"])

    def test_startup_capture_failure_is_retained_without_replacing_primary(self):
        with tempfile.TemporaryDirectory() as temporary, patch.object(harness.clone, "DIAGNOSTIC_ROOT", Path(temporary) / "diagnostics"):
            stderr = Path(temporary) / "stderr"
            stderr.write_bytes(b"")
            process = SimpleNamespace(poll=lambda: None, error_output=stderr, environment={},
                                      follow_startup_capture=Mock(side_effect=RuntimeError("fixture-password")))
            driver = object.__new__(harness.Driver)
            driver.kill = lambda _process: None
            primary = harness.clone.SubprocessFailure({"diagnostic_id": "primary", "retained": True})
            with self.assertRaises(harness.clone.SubprocessFailure) as failed:
                with ExitStack() as cleanup:
                    driver.watch(process, cleanup)
                    raise primary
            self.assertIs(failed.exception, primary)
            metadata = primary.metadata["follow_observation"]
            retained = json.loads((harness.clone.DIAGNOSTIC_ROOT / (metadata["diagnostic_id"] + ".stderr")).read_text())
            self.assertEqual(retained["startup_snapshots"][0]["snapshot"]["status"], "unavailable")
            self.assertNotIn("fixture-", json.dumps(retained))

    def test_component_modes_select_private_cluster_and_role_contracts(self):
        from db_harness import module_cases
        wrapper = harness.clone.ROOT / "tools/test-db.py"
        context = {"root": "/owned/follow", "socket_dir": "/owned/socket", "port": 24567}
        cluster = SimpleNamespace(context=context, context_path=Path("/owned/follow/context.json"),
                                  root=Path(context["root"]), bindir=Path("/owned/pg/bin"),
                                  environment={"PATH": "/usr/bin"}, auth_password="synthetic")
        for mode, profile in (("source", "FollowSource"), ("authority", "FollowAuthority"), ("receive", "FollowReceive"), ("apply", "FollowApply")):
            main = runpy.run_path(str(wrapper))["main"]
            cases = module_cases({}, profile)
            output = "".join(f"test {name} ... ok\n" for name in cases)
            output += f"test result: ok. {len(cases)} passed; 0 failed; 0 ignored; 0 measured; 600 filtered out; finished in 0.01s\n"
            seen = {}
            def owned(_manifest, **kwargs):
                seen["cluster"] = kwargs
                return nullcontext(cluster)
            def databases(_context, _case, *, empty=False):
                self.assertTrue(empty)
                return "owned_source", "owned_target"
            def command(argv, **kwargs):
                seen.update(argv=argv, **kwargs)
                return subprocess.CompletedProcess(argv, 0, output, "")
            with self.subTest(mode=mode), patch.dict(main.__globals__, {
                    "OwnedCluster": owned, "databases": databases, "run_command": command,
                    "authority_fixture": lambda _cluster: nullcontext({"RPGMIGRATE_TEST_AUTHORITY_ADMIN": "owned-admin"})}), \
                    patch.object(sys, "argv", [str(wrapper), "--follow-" + mode, "--", "cargo", "test"]), \
                    redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(main(), 0)
            self.assertEqual(seen["cluster"], {"logical": True, "logical_slots": 32} if mode in ("source", "receive") else {})
            self.assertEqual(seen["environment"]["RPGMIGRATE_TEST_CONTEXT"], str(cluster.context_path))
            self.assertIn(f"{profile} verified {len(cases)} exact cases", stdout.getvalue())
            self.assertNotIn("Authority faults verified", stdout.getvalue())
            if mode == "source":
                self.assertIn("dbname='postgres' user='e1_admin'", seen["environment"]["RPGMIGRATE_TEST_FOLLOW_SOURCE_ADMIN"])
            if mode == "receive":
                self.assertIn("dbname='postgres' user='e1_admin'", seen["environment"]["RPGMIGRATE_TEST_FOLLOW_RECEIVE_SOURCE"])
            if mode == "apply":
                self.assertIn("dbname='owned_source' user='e1_admin'", seen["environment"]["RPGMIGRATE_TEST_SOURCE"])

    def test_backpressure_requires_one_actual_bounded_fifo_event(self):
        queue = {"messages": 1024, "owned_bytes": 8192, "limit_messages": 1024, "limit_bytes": 8 * 1024 * 1024}
        event = {"event": "follow.backpressure", "queue": queue}
        self.assertIsNone(harness.backpressure_event(b"unrelated log\n"))
        self.assertEqual(harness.backpressure_event(json.dumps(event).encode()), event)
        for output in ((json.dumps(event) + "\n" + json.dumps(event)).encode(),
                       *(json.dumps({**event, "queue": bad}).encode() for bad in
                         ({}, {**queue, "messages": 0}, {**queue, "owned_bytes": 99999999}, {**queue, "limit_bytes": True}))):
            with self.subTest(output=output), self.assertRaises(RuntimeError):
                harness.backpressure_event(output)

    def test_component_receipts_require_exact_new_profiles_and_zero_ignored_summary(self):
        from db_harness import module_cases
        validate = runpy.run_path(str(harness.clone.ROOT / "tools/test-db.py"))["validate_module_receipt"]
        for profile, count in (("FollowSource", 7), ("FollowAuthority", 11), ("FollowReceive", 4), ("FollowApply", 11)):
            cases = module_cases({}, profile)
            self.assertEqual(len(cases), count)
            records = "".join(f"test {name} ... ok\n" for name in cases)
            output = records + f"test result: ok. {count} passed; 0 failed; 0 ignored; 0 measured; 600 filtered out; finished in 0.01s\n"
            validate(profile, cases, output, {})
            for bad in ("", output.replace(cases[0], "unrelated::case"), output + records,
                        output.replace("0 ignored", "1 ignored"), output.replace(f"{count} passed", "0 passed"),
                        records, output.replace(" ... ok", " ... ignored", 1),
                        *(output.replace(f"test {case} ... ok\n", "") for case in cases)):
                with self.subTest(profile=profile, output=bad), self.assertRaises(AssertionError):
                    validate(profile, cases, bad, {})

    def test_wire_gate_connection_scope_and_arming_are_exact(self):
        with WireGate("source", "before_commit") as gate:
            self.assertEqual(gate.connection("host=source dbname=fixture"), f"host=127.0.0.1 port={gate.port} dbname=fixture")
            self.assertEqual(gate.connection("host=target dbname=fixture"), "host=target dbname=fixture")
            gate.arm()
            with self.assertRaises(RuntimeError):
                gate.arm()
            self.assertEqual(gate.evidence()["statuses"], [])
        with self.assertRaises(ValueError):
            WireGate("source", "invented")

    def test_wire_gates_hold_exact_before_commit_after_commit_and_partial_begin_boundaries(self):
        from protocol_fault import receive
        def frame(kind, payload):
            return kind + struct.pack("!I", len(payload) + 4) + payload
        commit_request = frame(b'Q', b'COMMIT\0')
        commit_response = frame(b'C', b'COMMIT\0')
        begin = frame(b'd', b'w' + struct.pack("!QQQ", 100, 9999, 0) + b'B' + struct.pack("!QqI", 100, 0, 3))
        following = frame(b'd', b'k' + struct.pack("!QqB", 100, 0, 0))
        for mode in ("before_commit", "after_commit", "after_begin"):
            with self.subTest(mode=mode), WireGate("fixture", mode) as gate:
                client, producer = socket.socketpair()
                server, backend = socket.socketpair()
                gate.sockets.extend((client, server))
                producer.settimeout(1)
                backend.settimeout(1)
                thread = threading.Thread(target=gate.downstream, args=(client, server), daemon=True)
                gate.threads.append(thread)
                thread.start()
                try:
                    startup = self.wire_startup(mode == "after_begin")
                    producer.sendall(startup)
                    self.assertEqual(receive(backend, len(startup)), startup)
                    if mode == "after_begin":
                        query = frame(b'Q', b"""START_REPLICATION SLOT "owned" LOGICAL 0/10 ("proto_version" '1', "publication_names" '"owned"')\0""")
                        producer.sendall(query)
                        self.assertEqual(receive(backend, len(query)), query)
                        response = frame(b'W', b'\0\0\0')
                        backend.sendall(response)
                        self.assertEqual(receive(producer, len(response)), response)
                    gate.arm()
                    if mode == "before_commit":
                        producer.sendall(commit_request)
                        held_stream, expected = backend, commit_request
                    elif mode == "after_commit":
                        backend.sendall(commit_response)
                        held_stream, expected = producer, commit_response
                    else:
                        backend.sendall(begin + following)
                        self.assertEqual(receive(producer, len(begin)), begin)
                        held_stream, expected = producer, following
                    self.assertTrue(gate.held.wait(timeout=1))
                    held_stream.settimeout(0.02)
                    with self.assertRaises(socket.timeout):
                        held_stream.recv(1)
                    held_stream.settimeout(1)
                    gate.release.set()
                    self.assertEqual(receive(held_stream, len(expected)), expected)
                    self.assertEqual(gate.evidence()["hits"], 1)
                finally:
                    producer.close()
                    backend.close()

    def test_wire_gate_records_only_status_positions_during_logical_copyboth(self):
        from protocol_fault import receive
        with self.wire_pair("observe") as (gate, producer, backend):
            request = self.wire_startup(True) + self.wire_frame(b'Q', b"""START_REPLICATION SLOT "owned" LOGICAL 0/10 ("proto_version" '1', "publication_names" '"owned"')\0""")
            producer.sendall(request)
            self.assertTrue(receive(backend, len(request)) == request)
            response = self.wire_frame(b'W', b'\0\0\0')
            backend.sendall(response)
            self.assertTrue(receive(producer, len(response)) == response)
            status = self.wire_frame(b'd', b'r' + struct.pack("!QQQQB", 10, 10, 10, 0, 0))
            producer.sendall(status + self.wire_frame(b'c', b''))
            self.assertTrue(receive(backend, len(status) + 5) == status + self.wire_frame(b'c', b''))
            response = self.wire_frame(b'c', b'') + self.wire_frame(b'C', b'START_REPLICATION\0') + self.wire_frame(b'Z', b'I')
            backend.sendall(response)
            self.assertTrue(receive(producer, len(response)) == response)
            request = self.wire_frame(b'Q', b'COPY fixture FROM STDIN\0')
            producer.sendall(request)
            self.assertTrue(receive(backend, len(request)) == request)
            response = self.wire_frame(b'G', b'\0\0\1\0\0')
            backend.sendall(response)
            self.assertTrue(receive(producer, len(response)) == response)
            data = self.wire_frame(b'd', b'row\n') + self.wire_frame(b'c', b'')
            producer.sendall(data)
            self.assertTrue(receive(backend, len(data)) == data)
            evidence = gate.evidence()
            self.assertEqual(evidence["statuses"], [(10, 10, 10)])
            self.assertEqual(evidence["logical"]["streams"][0]["statuses"], [(10, 10, 10)])


if __name__ == "__main__":
    if sys.flags.optimize:
        raise RuntimeError("optimized Python cannot run qualification")
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(FollowHarnessTests)
    if not suite.countTestCases():
        raise RuntimeError("no follow harness tests selected")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() and not result.skipped else 1)
