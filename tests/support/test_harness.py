"""Run with python3 tests/support/test_harness.py."""

import json
import os
from pathlib import Path
import runpy
import select
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

from db_harness import (clean_environment, conninfo, expected_version, load_manifest, module_cases, run_command, validate_libtest_output,
                        validate_report, validate_success_output)
from protocol_fault import error_sqlstate
from e1_cases import Cases
from protocol_fault import ProtocolFault, packet
from replication_fault import ReplicationFault


def replication_records(context):
    feedback = (b'r' + bytes(33)).hex()
    records = []
    for case in ("keepalive", "duplex", "terminal_error", "missing_completion"):
        value = {"owner": context["token"], "case": case, "status": "observed", "fault_hits": 1,
                 "copyboth_seen": True, "backend_closed": True, "feedback_hex": feedback,
                 "expected_feedback_hex": feedback, "feedback_count": 1, "keepalive_sent": True,
                 "post_arm_feedback_count": 1, "feedback_mismatches": 0,
                 "held": False, "released": True, "server_frames_while_held": 1, "pqflush_pending": 1,
                 "copydone_seen": True, "terminal_seen": True,
                 "terminal_fault": "error" if case == "terminal_error" else "missing",
                 "error_sqlstate": "57014" if case == "terminal_error" else None}
        if case == "terminal_error":
            late = dict(value)
            pending = {**value, "backend_pid": 123, "cancel_requested": True, "owned_backend_verified": True, "server_error_seen": True,
                       "gate_held_at_error": True, "client_error_sqlstate": "57014", "client_closed_before_release": True,
                       "client_before_deadline": True, "feedback_drained_after_cancel": 0, "held": True, "released": False}
            value["phases"] = {"late_error": late, "pending_error": pending}
            value["fault_hits"] = 2
        path = Path(context["root"]) / f"replication-fault-{case}.json"
        path.write_text(json.dumps(value))
        records.append((path, value))
    return records


def authority_records(context):
    for case in ("lost_commit_reply", "provisioning_recovery"):
        value = {"owner": context["token"], "case": case, "status": "observed", "fault_hits": 1, "backend_pid": 123,
                 "commit_tag": "COMMIT", "holding_backend": False, "backend_connection_closed": True,
                 "held_fence_observed": True, "released_fence_observed": True}
        (Path(context["root"]) / f"authority-fault-{case}.json").write_text(json.dumps(value))


def keepalive_receipt(before, after):
    relay = ReplicationFault({"token": "owned-fixture"}, "keepalive")
    client, frontend = socket.socketpair()
    backend, server = socket.socketpair()
    relay.client = client
    relay.state["copyboth_seen"] = True
    for connection in (client, frontend, backend, server):
        connection.settimeout(2)
    worker = threading.Thread(target=relay.upstream, args=(client, backend))
    worker.start()
    try:
        frontend.sendall(struct.pack("!II", 8, 196608))
        packet(server, typed=False)
        for payload in before:
            frontend.sendall(b'd' + struct.pack("!I", 4 + len(payload)) + payload)
            packet(server)
        relay.operate({"op": "arm", "wal_end": 900000, "feedback_hex": (b'r' + bytes(33)).hex()})
        packet(frontend)
        for payload in after:
            frontend.sendall(b'd' + struct.pack("!I", 4 + len(payload)) + payload)
            packet(server)
    finally:
        relay.stopping.set()
        for connection in (client, frontend, backend, server):
            connection.close()
        worker.join(timeout=3)
    assert not worker.is_alive()
    relay.backend_closed.set()
    return relay.snapshot()


def copy_wire_observations(before=(), after=(), client_before=(), client_after=()):
    failures = []
    pre_done, post_done, armed = (threading.Event() for _ in range(3))
    with tempfile.TemporaryDirectory(prefix="copy-wire-", dir="/tmp") as directory:
        context = {"socket_dir": directory, "port": 15432}
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(Path(directory) / ".s.PGSQL.15432"))
        listener.listen(1)
        listener.settimeout(3)

        def send(connection, kind, payload):
            connection.sendall(kind + struct.pack("!I", len(payload) + 4) + payload)

        def backend():
            try:
                connection, _ = listener.accept()
                with connection:
                    connection.settimeout(3)
                    packet(connection, typed=False)
                    send(connection, b"K", struct.pack("!II", 4321, 0))
                    send(connection, b"Z", b"I")
                    for kind, payload in before:
                        send(connection, kind, payload)
                    for _ in client_before:
                        packet(connection)
                    pre_done.set()
                    if not armed.wait(3):
                        raise TimeoutError("COPY frame test was not armed")
                    for kind, payload in after:
                        send(connection, kind, payload)
                    for _ in client_after:
                        packet(connection)
                    post_done.set()
                    try:
                        connection.recv(1)
                    except OSError:
                        pass
            except Exception as error:
                failures.append(type(error).__name__)

        worker = threading.Thread(target=backend)
        worker.start()
        try:
            with ProtocolFault(context, "copy_observe") as relay:
                relay.copy_observer = True
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                    client.settimeout(3)
                    client.connect(str(relay.path))
                    client.sendall(struct.pack("!II", 8, 196608))
                    for _ in range(2 + len(before)):
                        packet(client)
                    for kind, payload in client_before:
                        send(client, kind, payload)
                    if not pre_done.wait(3):
                        raise TimeoutError("missing pre-arm frame forwarding")
                    # Frame tests set the observation window; controller tests exercise the arm operation.
                    relay.copy_started_at = time.monotonic()
                    relay.armed.set()
                    armed.set()
                    for kind, payload in client_after:
                        send(client, kind, payload)
                    for _ in after:
                        packet(client)
                    if not post_done.wait(3):
                        raise TimeoutError("missing post-arm frame forwarding")
            value = {"copy_in_starts": getattr(relay, "copy_in_starts", 0),
                     "copy_complete_tags": list(getattr(relay, "copy_complete_tags", [])),
                     "commit_tags": list(getattr(relay, "commit_tags", [])),
                     "update_tags": list(getattr(relay, "update_tags", [])),
                     "error_sqlstates": list(relay.error_states),
                     "client_copy_bytes": relay.client_copy_bytes,
                     "backend_pid": relay.backend_pid, "backend_closed": relay.backend_closed.is_set()}
        finally:
            armed.set()
            listener.close()
            worker.join(timeout=4)
        if worker.is_alive() or failures:
            raise AssertionError(f"COPY frame test backend did not stop cleanly: {failures}")
        return value


def copy_native_fixture(directory):
    from copy_snapshot_fixture import copy_snapshot_receipt_path
    root = Path(directory).resolve()
    native = root / "owning-copy-offline" / "manifest.json"
    native.parent.mkdir()
    stream = {"run": list(range(1, 17)), "source": {"system_identifier": 17, "database_oid": 19}, "generation": 9}
    binding = {"stream": stream, "slot_name": "cp_owned_0", "plugin": "test_decoding", "catalog_epoch": 7}
    source = {"intent": {"binding": binding, "nonce": [2] * 16},
              "consistent_point": 23, "snapshot_token": "private-snapshot-token"}
    target = {"intent": {"binding": {"stream": stream, "target_system_identifier": 29, "target_database_oid": 31,
                                     "schema_name": "owned", "table_name": "checkpoint", "owner_oid": 37,
                                     "metadata_version": 3}, "nonce": [3] * 16}, "schema_oid": 41, "table_oid": 43}
    value = {"magic": "RPGMIGRATE", "format_version": 4, "run": stream["run"],
             "canonical_path": str(native.parent), "copy_complete": None,
             "source_slot": {"phase": "confirmed", "record": source},
             "target_provenance": [{"phase": "confirmed", "record": target}],
             "copy_snapshot": {"anchor": {"stream": stream, "consistent_point": 23,
                                          "snapshot_token": source["snapshot_token"], "plan_nonce": [4] * 16},
                               "plan": {"reference_version": 1, "plan_nonce": [4] * 16,
                                        "table_count": 2, "logical_bytes": 100, "crc32": 0}}}
    native.write_text(json.dumps(value))
    context = {"root": str(root), "token": "target-owner", "socket_dir": str(root), "port": 15432,
               "copy_snapshot_source": {"root": directory, "token": "source-owner", "socket_dir": directory,
                                        "port": 15433, "database": "copy_atomic", "replication_role": "copy_role"},
               "copy_snapshot_variants": {"source_finish": {"source_database": "copy_source_finish",
                                                             "target_database": "e3_authority", "tables": 2,
                                                             "source_identity": stream["source"]}},
               "copy_snapshot_slots": {"source_finish": "cp_owned_0"}}
    target_receipt = {"owner": context["token"], "profile": "copy_snapshot", "side": "target",
                      "case": "source_finish", "phase": "copy", "status": "observed", "armed": True,
                      "native_path": str(native), "stream": stream, "backend_pid": 6001,
                      "copy_in_starts": 2, "copy_complete_tags": ["COPY 2", "COPY 2"],
                      "error_sqlstates": [], "commit_tags": ["COMMIT"], "update_tags": ["UPDATE 1"],
                      "client_copy_bytes": 16, "fault_hits": 0, "client_closed": True, "backend_closed": True,
                      "held_fence_observed": False, "released_fence_observed": False,
                      "deadline_ms": 20000, "elapsed_ms": 10, "helper_pid": 6002,
                      "helper_reaped": True, "helper_returncode": 0}
    receipt = copy_snapshot_receipt_path(context, "target", "source_finish", "copy")
    receipt.write_text(json.dumps(target_receipt))
    return context, native, value, receipt, target_receipt


def copy_controller(side, *, held=False, invalid=None, select_barrier=False):
    script = '''import json,os,pathlib,socket,sys,threading,time
sys.path.insert(0,sys.argv.pop(1))
side=sys.argv.pop(1)
import authority_fault as target
import source_snapshot_fault as source
if os.environ.get("COPY_TEST_SELECT_BARRIER")=="1":
    import protocol_fault as wire
    root=pathlib.Path(json.loads(pathlib.Path(os.environ["RPGMIGRATE_TEST_COPY_SNAPSHOT_CONTEXT"]).read_text())["root"])
    actual_packet,actual_select=wire.packet,target.select.select
    copies=0
    pause=threading.Event()
    def packet(*args,**kwargs):
        global copies
        value=actual_packet(*args,**kwargs)
        if value[0]==b"c":
            copies+=1
            if copies==2:
                pause.set()
        return value
    def select(reads,writes,errors,timeout=None):
        if reads and isinstance(reads[0],socket.socket) and pause.is_set():
            pause.clear()
            (root/"at-select").touch()
            deadline=time.monotonic()+3
            while not (root/"resume-select").exists():
                assert time.monotonic()<deadline,"COPY select barrier deadline"
                time.sleep(0.001)
            try:
                return actual_select(reads,writes,errors,timeout)
            finally:
                (root/"select-done").touch()
        return actual_select(reads,writes,errors,timeout)
    wire.packet=packet
    target.select.select=select
target.assert_identity=source.assert_identity=lambda context: None
def psql(context,database,sql):
    root=pathlib.Path(context["root"])
    if "pg_replication_slots" in sql:
        return json.dumps({"name":"cp_owned_0","plugin":"test_decoding","database":database,"temporary":False,"consistent_point":"0/17"})
    assert "pg_stat_activity" in sql, "unexpected offline SQL"
    assert "pid=5001" in sql or "pid=5002" in sql, "wrong offline backend"
    alive=(root/("backend-live" if "pid=5001" in sql else "exporter-live")).exists()
    if "pg_locks" in sql:
        return json.dumps([alive, int(alive)])
    return json.dumps([5001 if "pid=5001" in sql else 5002,context["replication_role"],database]) if alive else ""
target.psql=source.psql=psql
(target if side=="target" else source).main()
assert all(thread is threading.current_thread() or not thread.is_alive() for thread in threading.enumerate()), "offline COPY thread leak"
'''

    def send(connection, kind, payload):
        connection.sendall(kind + struct.pack("!I", len(payload) + 4) + payload)

    with tempfile.TemporaryDirectory(prefix="copy-controller-", dir="/tmp") as directory:
        context, native, _, prior, _ = copy_native_fixture(directory)
        root = Path(context["root"])
        case, phase = ("unknown", "copy_unknown") if held else ("source_finish", "copy" if side == "target" else "finish_unknown")
        if held:
            context["copy_snapshot_variants"][case] = context["copy_snapshot_variants"]["source_finish"]
            context["copy_snapshot_slots"][case] = "cp_owned_0"
        if side == "target":
            prior.unlink()
        (root / "exporter-live").touch()
        context_path = root / "context.json"
        context_path.write_text(json.dumps(context))
        port = context["port"] if side == "target" else context["copy_snapshot_source"]["port"]
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(root / f".s.PGSQL.{port}"))
        listener.listen(1)
        listener.settimeout(5)
        errors = []

        def backend():
            try:
                with listener.accept()[0] as server:
                    (root / "backend-live").touch()
                    server.settimeout(5)
                    packet(server, typed=False)
                    send(server, b"K", struct.pack("!II", 5001, 0))
                    send(server, b"Z", b"I")
                    if side == "source":
                        assert packet(server)[2].startswith(b"SET TRANSACTION SNAPSHOT")
                        send(server, b"C", b"SET\0")
                        send(server, b"Z", b"T")
                        assert packet(server)[2] == b"COMMIT\0"
                    else:
                        for _ in range(2):
                            assert packet(server)[2] == b"COPY fixture FROM STDIN\0"
                            send(server, b"G", b"\x00\x00\x02\x00\x00\x00\x00")
                            assert packet(server)[0] == b"d"
                            assert packet(server)[0] == b"c"
                            send(server, b"C", b"COPY 2\0")
                        if select_barrier:
                            deadline = time.monotonic() + 3
                            while not (root / "at-select").exists():
                                assert time.monotonic() < deadline, "upstream did not enter the select barrier"
                                time.sleep(0.001)
                        send(server, b"C", b"UPDATE 1\0")
                    send(server, b"C", b"COMMIT\0")
                    send(server, b"Z", b"I")
                    assert server.recv(1) == b""
            except (EOFError, OSError):
                if invalid is None:
                    errors.append("unexpected offline COPY socket closure")
            except Exception as error:
                errors.append(repr(error))
            finally:
                (root / "backend-live").unlink(missing_ok=True)
                (root / "exporter-live").unlink(missing_ok=True)

        worker = None if invalid in ("unarmed", "oversized") else threading.Thread(target=backend)
        if worker is not None:
            worker.start()
        environment = {**clean_environment(os.environ), "RPGMIGRATE_TEST_COPY_SNAPSHOT_CONTEXT": str(context_path)}
        if select_barrier:
            environment["COPY_TEST_SELECT_BARRIER"] = "1"
        command = [sys.executable, "-Werror", "-c", script, str(Path(__file__).resolve().parent), side,
                   "--copy-case", case, "--phase", phase]
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, env=environment)
        client = final = None

        def read():
            assert select.select([process.stdout], [], [], 5)[0], "offline COPY controller response deadline"
            line = process.stdout.readline(65537)
            return json.loads(line) if line else None

        def request(value):
            process.stdin.write(json.dumps(value) + "\n")
            process.stdin.flush()
            return read()

        try:
            ready = read()
            if invalid in ("unarmed", "oversized"):
                final = request({"op": "quit"} if invalid == "unarmed" else {"op": "x" * 4097})
            else:
                host = ready["conninfo"].split("host='")[1].split("'", 1)[0]
                client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                client.settimeout(5)
                client.connect(str(Path(host) / f".s.PGSQL.{port}"))
                client.sendall(struct.pack("!II", 8, 196608))
                packet(client)
                packet(client)
                if side == "source":
                    send(client, b"Q", b"SET TRANSACTION SNAPSHOT 'private-snapshot-token'\0")
                    packet(client)
                    packet(client)
                arm = {"op": "arm", "native_path": str(native)}
                if side == "source":
                    arm["peer_pid"] = 5002
                armed = request(arm)
                assert armed["armed"] and not armed["fault_hits"]
                if invalid is not None:
                    if invalid == "early_failed":
                        final = request({"op": "failed"})
                        if final is not None:
                            request({"op": "quit"})
                    else:
                        final = request(arm if invalid == "duplicate" else {"op": "release"})
                else:
                    if side == "source":
                        send(client, b"Q", b"COMMIT\0")
                    else:
                        for _ in range(2):
                            send(client, b"Q", b"COPY fixture FROM STDIN\0")
                            assert packet(client)[0] == b"G"
                            send(client, b"d", b"1\tprivate-row\n2\tprivate-row\n")
                            send(client, b"c", b"")
                            assert packet(client)[2] == b"COPY 2\0"
                        assert packet(client)[2] == b"UPDATE 1\0"
                    if side == "source" or held:
                        assert client.recv(1) == b"", "COPY fault did not lose the actual COMMIT reply"
                        if select_barrier:
                            assert (root / "at-select").exists() and (root / "backend-live").exists()
                            (root / "resume-select").touch()
                            deadline = time.monotonic() + 3
                            while not (root / "select-done").exists():
                                assert time.monotonic() < deadline, "upstream did not resume before hold release"
                                time.sleep(0.001)
                            assert (root / "backend-live").exists(), "held backend closed before release"
                        if side == "source":
                            request({"op": "failed"})
                        else:
                            status = request({"op": "status"})
                            assert status["held_fence_observed"] and not status["backend_closed"]
                            request({"op": "release"})
                    else:
                        assert packet(client)[2] == b"COMMIT\0"
                        assert packet(client)[0] == b"Z"
                    client.close()
                    final = request({"op": "quit"})
            process.wait(timeout=5)
            stored = json.loads(Path(ready["receipt"]).read_text())
            assert Path(ready["receipt"]).stat().st_mode & 0o777 == 0o600
        finally:
            if client is not None:
                client.close()
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            stderr = process.stderr.read(16385)
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
            listener.close()
            if worker is not None:
                worker.join(timeout=5)
        assert (worker is None or not worker.is_alive()) and not errors, errors
        assert len(stderr) <= 16384, "offline COPY helper diagnostic bound exceeded"
        return process.returncode, final, stored, stderr, context


class HarnessTests(unittest.TestCase):
    def test_copy_controller_rejects_duplicate_json_operations(self):
        from authority_fault import copy_request
        reader, writer = os.pipe()
        with os.fdopen(reader) as source, os.fdopen(writer, "w") as sink:
            sink.write('{"op":"arm","op":"quit"}\n')
            sink.flush()
            with mock.patch.object(sys, "stdin", source), self.assertRaises(AssertionError):
                copy_request(SimpleNamespace(copy_started_at=time.monotonic(), copy_deadline_ms=100))

    def test_copy_controller_deadline_covers_an_incomplete_stdin_line(self):
        script = '''import sys,time
from types import SimpleNamespace
sys.path.insert(0,sys.argv[1])
from authority_fault import copy_request
relay=SimpleNamespace(copy_started_at=time.monotonic(),copy_deadline_ms=100)
print("ready",flush=True)
copy_request(relay)
'''
        process = subprocess.Popen([sys.executable, "-Werror", "-c", script, str(Path(__file__).resolve().parent)],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 3)[0])
            self.assertEqual(process.stdout.readline(), "ready\n")
            process.stdin.write('{"op":')
            process.stdin.flush()
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.fail("incomplete COPY controller input blocked past its deadline")
            self.assertNotEqual(process.returncode, 0)
            self.assertIn("COPY controller deadline", process.stderr.read())
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=3)
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()

    def test_copy_wire_rejects_malformed_backend_starts_and_tags(self):
        relay = ProtocolFault({}, "copy_observe")
        relay.arm()
        for payload in (b"", b"\x02\x00\x00", b"\x00\x00\x01", b"\x00\x00\x01\x00\x02"):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                relay.observe_copy_response(b"G", payload)
        for payload in (b"COPY 01\0", b"COPY -1\0", b"COPY 2", b"UPDATE 01\0"):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                relay.observe_copy_response(b"C", payload)
        self.assertEqual(relay.copy_observations()["copy_in_starts"], 0)

    def test_copy_target_controller_closes_and_requires_parent_wait(self):
        from copy_snapshot_fixture import validate_copy_snapshot_phase
        code, final, stored, stderr, context = copy_controller("target")
        self.assertEqual(code, 0, stderr)
        self.assertEqual(final["op"], "quit")
        self.assertLessEqual(final["elapsed_ms"], stored["elapsed_ms"])
        self.assertEqual({key: value for key, value in final.items() if key not in ("op", "elapsed_ms")},
                         {key: value for key, value in stored.items() if key != "elapsed_ms"})
        self.assertFalse(stored["helper_reaped"])
        self.assertIsNone(stored["helper_returncode"])
        self.assertEqual(stored["copy_in_starts"], 2)
        self.assertNotIn("private-row", json.dumps(stored))
        self.assertNotIn("private-snapshot-token", json.dumps(stored))
        with self.assertRaises(AssertionError):
            validate_copy_snapshot_phase(stored, context, "target", "source_finish", "copy")
        validate_copy_snapshot_phase({**stored, "helper_reaped": True, "helper_returncode": code},
                                     context, "target", "source_finish", "copy")

    def test_copy_target_controller_holds_and_releases_observed_commit(self):
        code, _, stored, stderr, _ = copy_controller("target", held=True)
        self.assertEqual(code, 0, stderr)
        self.assertTrue(stored["held_fence_observed"])
        self.assertTrue(stored["released_fence_observed"])
        self.assertEqual(stored["commit_tags"], ["COMMIT"])
        self.assertEqual(stored["fault_hits"], 1)

    def test_copy_held_commit_cannot_close_a_socket_before_paused_select(self):
        code, _, stored, stderr, _ = copy_controller("target", held=True, select_barrier=True)
        self.assertEqual(code, 0, stderr)
        self.assertTrue(stored["held_fence_observed"])
        self.assertTrue(stored["released_fence_observed"])
        self.assertTrue(stored["backend_closed"])

    def test_copy_source_controller_imports_before_late_loss_arm(self):
        from copy_snapshot_fixture import validate_copy_snapshot_phase
        code, _, stored, stderr, context = copy_controller("source")
        self.assertEqual(code, 0, stderr)
        self.assertTrue(stored["imported"])
        self.assertTrue(stored["peer_closed"])
        self.assertEqual(stored["commit_tags"], ["COMMIT"])
        self.assertEqual(stored["copy_in_starts"], 0)
        self.assertNotIn("private-row", json.dumps(stored))
        self.assertNotIn("private-snapshot-token", json.dumps(stored))
        validate_copy_snapshot_phase({**stored, "helper_reaped": True, "helper_returncode": code},
                                     context, "source", "source_finish", "finish_unknown")

    def test_copy_source_controller_cannot_accept_failure_before_the_fault(self):
        code, final, stored, _, _ = copy_controller("source", invalid="early_failed")
        self.assertNotEqual(code, 0)
        self.assertIsNone(final, "source controller acknowledged a failure before its observed fault")
        self.assertEqual(stored["status"], "pending")

    def test_copy_controller_refuses_unused_duplicate_oversized_and_early_release(self):
        for invalid in ("unarmed", "duplicate", "oversized", "early_release"):
            with self.subTest(invalid=invalid):
                code, final, stored, _, _ = copy_controller("target", held=True, invalid=invalid)
                self.assertNotEqual(code, 0)
                self.assertIsNone(final)
                self.assertEqual(stored["status"], "pending")
                self.assertFalse(stored["helper_reaped"])

    def test_copy_wire_counts_empty_copy_from_actual_backend_messages(self):
        response = b"\x00\x00\x02\x00\x00\x00\x00"
        value = copy_wire_observations(after=((b"G", response), (b"C", b"COPY 0\0")))
        self.assertEqual(value["copy_in_starts"], 1)
        self.assertEqual(value["copy_complete_tags"], ["COPY 0"])
        self.assertEqual(value["client_copy_bytes"], 0)
        self.assertEqual(value["backend_pid"], 4321)
        self.assertTrue(value["backend_closed"])

    def test_copy_wire_excludes_all_prearm_counters(self):
        response = b"\x00\x00\x02\x00\x00\x00\x00"
        old_error = b"SERROR\0C23514\0Mprivate earlier error\0\0"
        value = copy_wire_observations(
            before=((b"G", response), (b"C", b"COPY 99\0"), (b"C", b"COMMIT\0"),
                    (b"C", b"UPDATE 9\0"), (b"E", old_error)),
            after=((b"G", response), (b"C", b"COPY 0\0"), (b"C", b"COMMIT\0"),
                   (b"C", b"UPDATE 1\0")),
            client_before=((b"d", b"earlier bytes"),), client_after=((b"d", b"now"),),
        )
        self.assertEqual(value["copy_in_starts"], 1)
        self.assertEqual(value["copy_complete_tags"], ["COPY 0"])
        self.assertEqual(value["commit_tags"], ["COMMIT"])
        self.assertEqual(value["update_tags"], ["UPDATE 1"])
        self.assertEqual(value["error_sqlstates"], [])
        self.assertEqual(value["client_copy_bytes"], 3)

    def test_copy_wire_does_not_guess_start_or_completion_from_client_text(self):
        value = copy_wire_observations(
            after=((b"C", b"COMMIT\0"), (b"C", b"UPDATE 1\0")),
            client_after=((b"Q", b"COPY private_table FROM STDIN\0"), (b"d", b"COPY 42\0")),
        )
        self.assertEqual(value["copy_in_starts"], 0)
        self.assertEqual(value["copy_complete_tags"], [])
        self.assertEqual(value["commit_tags"], ["COMMIT"])
        self.assertEqual(value["update_tags"], ["UPDATE 1"])

    def test_copy_wire_retains_sqlstate_without_error_text_or_row_payload(self):
        response = b"\x00\x00\x02\x00\x00\x00\x00"
        error = b"SERROR\0C23505\0Mprivate error text\0\0"
        value = copy_wire_observations(after=((b"G", response), (b"E", error)),
                                       client_after=((b"d", b"private row payload"),))
        self.assertEqual(value["error_sqlstates"], ["23505"])
        self.assertEqual(value["copy_complete_tags"], [])
        self.assertNotIn("private", json.dumps(value))

    def test_copy_loss_binding_requires_populated_exact_native4_envelope(self):
        import source_snapshot_fault as fault
        with tempfile.TemporaryDirectory() as directory:
            context, native, value, _, _ = copy_native_fixture(directory)
            path, stream = fault.copy_native_binding(context, "source_finish", str(native))
            self.assertEqual(path, native)
            self.assertEqual(stream, value["copy_snapshot"]["anchor"]["stream"])
            self.assertNotIn("private-snapshot-token", json.dumps(stream))
            for change in ("missing", "zero_point", "wrong_nonce", "wrong_ref", "wrong_source", "wrong_epoch"):
                altered = json.loads(json.dumps(value))
                if change == "missing":
                    altered["copy_snapshot"] = None
                elif change == "zero_point":
                    altered["copy_snapshot"]["anchor"]["consistent_point"] = 0
                elif change == "wrong_nonce":
                    altered["copy_snapshot"]["plan"]["plan_nonce"] = [5] * 16
                elif change == "wrong_ref":
                    altered["copy_snapshot"]["plan"]["reference_version"] = 2
                elif change == "wrong_source":
                    altered["source_slot"]["record"]["intent"]["binding"]["stream"]["source"]["database_oid"] = 20
                else:
                    altered["source_slot"]["record"]["intent"]["binding"]["catalog_epoch"] = 9
                native.write_text(json.dumps(altered))
                with self.subTest(change=change), self.assertRaises(AssertionError):
                    fault.copy_native_binding(context, "source_finish", str(native))

    def test_copy_loss_binding_rejects_equal_values_with_wrong_native_types(self):
        import source_snapshot_fault as fault
        with tempfile.TemporaryDirectory() as directory:
            context, native, value, _, _ = copy_native_fixture(directory)
            for field in ("plan_nonce", "anchor_stream", "target_stream"):
                altered = json.loads(json.dumps(value))
                if field == "plan_nonce":
                    altered["copy_snapshot"]["plan"]["plan_nonce"] = [4.0] * 16
                elif field == "anchor_stream":
                    altered["copy_snapshot"]["anchor"]["stream"]["generation"] = 9.0
                else:
                    altered["target_provenance"][0]["record"]["intent"]["binding"]["stream"]["generation"] = 9.0
                native.write_text(json.dumps(altered))
                with self.subTest(field=field), self.assertRaises(AssertionError):
                    fault.copy_native_binding(context, "source_finish", str(native))

    def test_copy_source_rechecks_authorization_at_terminal_commit(self):
        import source_snapshot_fault as fault
        with tempfile.TemporaryDirectory() as directory:
            context, native, value, _, _ = copy_native_fixture(directory)
            relay = fault.CopySnapshotFault(context, "source_finish", "finish_unknown")
            relay.server_frame(b"K", b"", struct.pack("!II", 5001, 0))
            relay.client_frame(b"Q", b"SET TRANSACTION SNAPSHOT 'private-snapshot-token'\0")
            relay.server_frame(b"C", b"", b"SET\0")
            with mock.patch.object(relay, "backend_present", return_value=True):
                relay.arm(str(native), 5002)
            value["copy_snapshot"] = None
            native.write_text(json.dumps(value))
            relay.client_frame(b"Q", b"COMMIT\0")
            with self.assertRaisesRegex(AssertionError, "missing populated COPY intent"):
                relay.server_frame(b"C", b"", b"COMMIT\0")
            self.assertEqual(relay.hits, 0)
            self.assertFalse(relay.close_after_frame)

    def test_copy_source_rejects_duplicate_target_receipt_fields(self):
        import source_snapshot_fault as fault
        with tempfile.TemporaryDirectory() as directory:
            context, native, value, receipt, target = copy_native_fixture(directory)
            text = json.dumps(target)
            receipt.write_text(text[:-1] + ',"helper_reaped":true}')
            with self.assertRaises(AssertionError):
                fault.completed_target_copy(context, "source_finish", native,
                                            value["copy_snapshot"]["anchor"]["stream"])

    def test_copy_source_loss_arm_rejects_missing_import_or_unreaped_target(self):
        import source_snapshot_fault as fault
        with tempfile.TemporaryDirectory() as directory:
            context, native, _, receipt, target = copy_native_fixture(directory)
            relay = fault.CopySnapshotFault(context, "source_finish", "finish_unknown")
            relay.server_frame(b"K", b"", struct.pack("!II", 5001, 0))
            with mock.patch.object(relay, "backend_present", return_value=True):
                with self.assertRaises(AssertionError):
                    relay.arm(str(native), 5002)
                self.assertFalse(relay.armed.is_set())
                relay.client_frame(b"Q", b"SET TRANSACTION SNAPSHOT 'private-snapshot-token'\0")
                frame = b"C" + struct.pack("!I", 8) + b"SET\0"
                self.assertEqual(relay.server_frame(b"C", frame, b"SET\0"), frame)
                target["helper_reaped"] = False
                target["helper_returncode"] = None
                receipt.write_text(json.dumps(target))
                with self.assertRaises(AssertionError):
                    relay.arm(str(native), 5002)
                self.assertFalse(relay.armed.is_set())

    def test_copy_source_passive_import_cannot_authorize_early_reply_loss(self):
        import source_snapshot_fault as fault
        with tempfile.TemporaryDirectory() as directory:
            context, _, _, _, _ = copy_native_fixture(directory)
            relay = fault.CopySnapshotFault(context, "source_finish", "finish_unknown")
            relay.client_frame(b"Q", b"SET TRANSACTION SNAPSHOT 'private-snapshot-token'\0")
            frame = b"C" + struct.pack("!I", 8) + b"SET\0"
            relay.server_frame(b"C", frame, b"SET\0")
            self.assertTrue(relay.imported)
            relay.client_frame(b"Q", b"COMMIT\0")
            commit = b"C" + struct.pack("!I", 11) + b"COMMIT\0"
            self.assertEqual(relay.server_frame(b"C", commit, b"COMMIT\0"), commit)
            self.assertEqual(relay.hits, 0)
            self.assertEqual(relay.copy_observations()["commit_tags"], [])
            self.assertFalse(relay.imported, "a pre-arm COMMIT ended the imported transaction")

    def test_copy_source_passive_state_does_not_retain_snapshot_sql(self):
        import source_snapshot_fault as fault
        with tempfile.TemporaryDirectory() as directory:
            context, _, _, _, _ = copy_native_fixture(directory)
            relay = fault.CopySnapshotFault(context, "source_finish", "finish_unknown")
            sql = b"SET TRANSACTION SNAPSHOT '" + b"private-token" * 1000 + b"'\0"
            for kind, payload in ((b"Q", sql), (b"P", b"statement\0" + sql + b"\0\0")):
                relay.client_frame(kind, payload)
                self.assertLessEqual(len(relay.command), len("SET TRANSACTION SNAPSHOT"))
                self.assertNotIn("private-token", relay.command)
                relay.server_frame(b"C", b"", b"SET\0")
                self.assertTrue(relay.imported)
                self.assertFalse(relay.armed.is_set())

    def assert_replication_profiles_reject(self, value):
        support = Path(__file__).resolve().parent
        validate = runpy.run_path(str(support.parents[1] / "tools" / "test-db.py"))["validate_module_receipt"]
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned-fixture"}
            context["replication"] = dict(context)
            authority_records(context)
            for profile in ("Replication", "Engine"):
                replication_records(context)
                expected = module_cases(load_manifest(), profile)
                output = "".join(f"test {name} ... ok\n" for name in expected)
                validate(profile, expected, output, context)
                for receipt in (value, {**value, "status": "observed"}):
                    (Path(directory) / "replication-fault-keepalive.json").write_text(json.dumps(receipt))
                    with self.subTest(profile=profile, status=receipt["status"]), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)

    def test_keepalive_receipt_cannot_use_feedback_from_before_arm(self):
        correct = b'r' + bytes(33)
        self.assertEqual(keepalive_receipt([], [correct])["status"], "observed")
        invalid = keepalive_receipt([correct], [])
        self.assertEqual(invalid["status"], "pending")
        self.assert_replication_profiles_reject(invalid)

    def test_keepalive_receipt_rejects_a_later_mismatching_feedback(self):
        correct = b'r' + bytes(33)
        mismatch = b'r' + struct.pack("!QQQQB", 900000, 900000, 900000, 0, 0)
        self.assertEqual(keepalive_receipt([], [correct, correct])["status"], "observed")
        invalid = keepalive_receipt([], [correct, mismatch])
        self.assertEqual(invalid["status"], "pending")
        self.assert_replication_profiles_reject(invalid)

    def test_optimized_python_cannot_accept_invalid_receipts(self):
        support = Path(__file__).resolve().parent
        code = ("from db_harness import validate_libtest_output; "
                "validate_libtest_output(['pg::live_tests::copy'], '')")
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "zero-test-command-ran"
            commands = (
                ["-c", code],
                [str(support.parents[1] / "tools" / "test-db.py"), "--engine", "--", sys.executable,
                 "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()", str(marker)],
                [str(support.parents[1] / "tools" / "test-db.py"), "--prepared", "--", sys.executable,
                 "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()", str(marker)],
                [str(support.parents[1] / "tools" / "test-db.py"), "--authority", "--", sys.executable,
                 "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()", str(marker)],
                [str(support.parents[1] / "tools" / "test-db.py"), "--replication", "--", sys.executable,
                 "-c", "import pathlib,sys; pathlib.Path(sys.argv[1]).touch()", str(marker)],
                [str(support / "e1_cases.py"), sys.executable],
                [str(support / "authority_fault.py"), "--case", "lost_commit_reply"],
                [str(support / "authority_selfcheck.py")],
                [str(support / "replication_fault.py"), "--case", "keepalive"],
                [str(support / "replication_selfcheck.py")],
            )
            for flags, optimization in ((["-O"], None), (["-OO"], None), ([], "1"), ([], "2")):
                for command in commands:
                    with self.subTest(flags=flags, optimization=optimization, command=command[0]):
                        environment = clean_environment(os.environ)
                        environment.pop("PYTHONOPTIMIZE", None)
                        environment.update(PG_BINDIR=str(Path(directory) / "missing"), TMPDIR=directory)
                        if optimization is not None:
                            environment["PYTHONOPTIMIZE"] = optimization
                        result = subprocess.run(
                            [sys.executable, *flags, *command], cwd=support,
                            env=environment, capture_output=True, text=True, timeout=10,
                        )
                        self.assertNotEqual(result.returncode, 0, "optimized harness accepted an invalid receipt")
                        self.assertIn("requires Python assertions", result.stderr)
                        self.assertEqual(list(Path(directory).iterdir()), [], "fixture or zero-test command started")

    def test_sqlstate_evidence_uses_the_protocol_code_field(self):
        self.assertEqual(error_sqlstate(b"SERROR\0C23505\0Mfixture duplicate\0\0"), "23505")
        self.assertEqual(error_sqlstate(b"SERROR\0MC23505 appears in text\0C57014\0\0"), "57014")
        for invalid in (b"SERROR\0Mno code\0\0", b"Csecret\0\0", b"C23505"):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                error_sqlstate(invalid)

    def test_expected_version_requires_a_consistent_explicit_pair(self):
        version_key = "RPGMIGRATE_TEST_EXPECTED_PG_VERSION"
        number_key = "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM"
        self.assertEqual(expected_version({}, "14.24", 140024), ("14.24", 140024))
        self.assertEqual(expected_version({version_key: "17.11", number_key: "170011"}, "14.24", 140024), ("17.11", 170011))
        for invalid in ({version_key: "17.11"}, {number_key: "170011"},
                        {version_key: "17.11", number_key: "170010"},
                        {version_key: "17", number_key: "170000"},
                        {version_key: "", number_key: ""}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                expected_version(invalid, "14.24", 140024)

    def test_direct_runner_rejects_empty_selection(self):
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "-k", "__e1_no_matching_test__"],
            env=clean_environment(os.environ), capture_output=True, text=True, timeout=10,
        )
        self.assertNotEqual(result.returncode, 0, "zero selected harness tests reported success")

    def test_module_profiles_require_an_explicit_command(self):
        wrapper = Path(__file__).resolve().parents[2] / "tools" / "test-db.py"
        for profile in ("--prepared", "--authority", "--replication"):
            with self.subTest(profile=profile):
                result = subprocess.run(
                    [sys.executable, str(wrapper), profile], env=clean_environment(os.environ),
                    capture_output=True, text=True, timeout=10,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn("provide --self-check or -- followed by", result.stderr)

    def test_auth_base_requires_explicit_fixture_connection_without_credentials(self):
        context = {"socket_dir": "/tmp/fixture-only", "port": 25432, "root": "/tmp/fixture-root"}
        result = conninfo(context, "auth_fixture", "e1_auth", include_passfile=False)
        self.assertIn("host='/tmp/fixture-only'", result)
        self.assertIn("port='25432'", result)
        self.assertIn("dbname='auth_fixture'", result)
        self.assertIn("user='e1_auth'", result)
        self.assertNotIn("password=", result)
        self.assertNotIn("passfile=", result)

    def test_success_output_cannot_disagree_with_native_receipt_or_expose_path(self):
        state = {"format_version": 2, "run": list(range(16)), "target_provenance": [],
                 "copy_complete": {"tables": 2, "rows": 10003, "bytes": 12345678}}
        valid = {"command": "copy table-data", "status": "complete", "run": list(range(16)),
                 "state_format_version": 2, "tables": 2, "rows": 10003, "bytes": 12345678}
        validate_success_output(json.dumps(valid), state)
        for changes in ({"run": [0] * 16}, {"state_format_version": 1}, {"state_format_version": 3}, {"tables": 0},
                        {"rows": 0}, {"bytes": 0}, {"canonical_path": "/private/fixture"},
                        {"status": "running"}, {"command": "ping"}):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                validate_success_output(json.dumps({**valid, **changes}), state)

    def test_copy_receipts_require_v4_and_empty_provenance_and_copy_intent(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            case = SimpleNamespace(state=state)
            for complete in (None, {"tables": 1, "rows": 1, "bytes": 8}):
                value = {"magic": "RPGMIGRATE", "format_version": 4, "target_provenance": [], "source_slot": None,
                         "copy_snapshot": None,
                         "run": list(range(16)), "canonical_path": str(state.resolve()), "copy_complete": complete}
                output = json.dumps({"command": "copy table-data", "status": "complete", "run": list(range(16)),
                                     "state_format_version": 4, "tables": 1, "rows": 1, "bytes": 8})
                check = (lambda: Cases.no_completion(case)) if complete is None else (
                    lambda: Cases.completed(case, 1, 1, output))
                (state / "manifest.json").write_text(json.dumps(value))
                check()
                for changes in ({"format_version": 1}, {"format_version": 2}, {"format_version": 3}, {"format_version": 5},
                                {"target_provenance": None}, {"source_slot": {}},
                                {"copy_snapshot": {}},
                                {"source_slot": {"phase": "provisioning", "record": {}}},
                                {"source_slot": {"phase": "confirmed", "record": {}}},
                                {"target_provenance": [{"phase": "confirmed", "record": {}}]}):
                    with self.subTest(complete=complete, changes=changes), self.assertRaises(AssertionError):
                        (state / "manifest.json").write_text(json.dumps({**value, **changes}))
                        check()
                for field in ("target_provenance", "source_slot", "copy_snapshot"):
                    missing = dict(value)
                    del missing[field]
                    (state / "manifest.json").write_text(json.dumps(missing))
                    with self.assertRaises((AssertionError, KeyError)):
                        check()

    def test_timed_out_command_terminates_its_descendant(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "terminated"
            descendant = "import pathlib,signal,sys,time; signal.signal(signal.SIGTERM,lambda *_: (pathlib.Path(sys.argv[1]).touch(),sys.exit(0))); time.sleep(3)"
            parent = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c',sys.argv[1],sys.argv[2]]); time.sleep(3)"
            with self.assertRaises(subprocess.TimeoutExpired):
                run_command([sys.executable, "-c", parent, descendant, str(marker)],
                            environment=clean_environment(os.environ), cwd=directory, timeout=0.5)
            deadline = time.monotonic() + 1
            while not marker.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(marker.exists(), "owned grandchild was not terminated with the timed-out command")

    def test_module_selection_cannot_be_empty_missing_or_ignored(self):
        expected = ["pg::live_tests::copy", "pg::live_tests::query"]
        good = "test pg::live_tests::copy ... ok\ntest pg::live_tests::query ... ok\n"
        validate_libtest_output(expected, good)
        for output in ("", good.replace(" ... ok", " ... ignored", 1), good.splitlines()[0], good + good,
                       good.replace("::copy", "::unexpected"), good.replace(" ... ok", " ... FAILED", 1)):
            with self.subTest(output=output), self.assertRaises(AssertionError):
                validate_libtest_output(expected, output)
        for inventory, output in (([], ""), ([expected[0]] * 2, good.splitlines()[0] + "\n" + good.splitlines()[0])):
            with self.subTest(inventory=inventory), self.assertRaises(AssertionError):
                validate_libtest_output(inventory, output)
        manifest = load_manifest()
        previous = manifest["transport_cases"] + manifest["copy_cases"]
        prepared = module_cases(manifest, "Prepared")
        for subset in (previous, prepared, previous + prepared):
            with self.subTest(subset=subset), self.assertRaises(AssertionError):
                validate_libtest_output(module_cases(manifest, "Engine"),
                                        "".join(f"test {name} ... ok\n" for name in subset))

    def test_child_environment_drops_database_context(self):
        environment = {
            "PATH": "/usr/bin", "HOME": "/home/test", "CARGO_HOME": "/cargo",
            "PGHOST": "existing", "PGPASSWORD": "secret", "PG_CONFIG": "old",
            "PGCOPYDB_SOURCE_PGURI": "secret", "RPGMIGRATE_TARGET": "secret",
        }
        result = clean_environment(environment)
        self.assertEqual(result, {"PATH": "/usr/bin", "HOME": "/home/test", "CARGO_HOME": "/cargo"})

    def test_absent_duplicate_skipped_and_fabricated_cases_fail(self):
        manifest = {"server_version_num": 140024, "cases": [{"id": "copy", "outcome": "success"}]}
        valid = {"server_version_num": 140024, "cases": [{"id": "copy", "status": "passed", "returncode": 0}]}
        validate_report(manifest, valid)
        for cases in ([], valid["cases"] * 2, [{"id": "copy", "status": "skipped", "returncode": 0}],
                      [{"id": "unlisted", "status": "passed", "returncode": 0}]):
            with self.subTest(cases=cases), self.assertRaises(AssertionError):
                validate_report(manifest, {**valid, "cases": cases})

    def test_wrong_version_and_suppressed_fault_fail(self):
        manifest = {"server_version_num": 140024, "cases": [{"id": "fault", "outcome": "failure", "fault": True}]}
        valid = {"server_version_num": 140024, "cases": [{"id": "fault", "status": "passed", "returncode": 6, "fault_hits": 1}]}
        validate_report(manifest, valid)
        with self.assertRaises(AssertionError):
            validate_report(manifest, {**valid, "server_version_num": 140023})
        for changes in ({"fault_hits": 0}, {"returncode": 0}):
            with self.subTest(changes=changes), self.assertRaises(AssertionError):
                validate_report(manifest, {**valid, "cases": [{**valid["cases"][0], **changes}]})

    def test_authority_and_engine_require_observed_fault_receipts(self):
        support = Path(__file__).resolve().parent
        validate = runpy.run_path(str(support.parents[1] / "tools" / "test-db.py"))["validate_module_receipt"]
        manifest = load_manifest()
        prepared = json.loads((support / "prepared_cases.json").read_text())
        authority = json.loads((support / "authority_cases.json").read_text())
        replication = json.loads((support / "replication_cases.json").read_text())
        profiles = {"Authority": authority, "Engine": manifest["transport_cases"] + manifest["copy_cases"] + prepared + authority + replication}
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned-fixture"}
            context["replication"] = dict(context)
            replication_records(context["replication"])
            records = []
            for case in ("lost_commit_reply", "provisioning_recovery"):
                value = {"owner": "owned-fixture", "case": case, "status": "observed", "fault_hits": 1, "backend_pid": 123,
                         "commit_tag": "COMMIT", "holding_backend": False, "backend_connection_closed": True,
                         "held_fence_observed": True, "released_fence_observed": True}
                path = Path(directory) / f"authority-fault-{case}.json"
                path.write_text(json.dumps(value))
                records.append((path, value))
            path, valid = records[0]
            for profile, expected in profiles.items():
                output = "".join(f"test {name} ... ok\n" for name in expected)
                path.write_text(json.dumps(valid))
                validate(profile, expected, output, context)
                path.unlink()
                with self.subTest(profile=profile, missing=True), self.assertRaises(AssertionError):
                    validate(profile, expected, output, context)
                for changes in ({"fault_hits": 0}, {"fault_hits": True}, {"status": "ignored"}, {"status": "pending"},
                                {"backend_pid": 0}, {"owner": "foreign"}, {"case": "other"}, {"commit_tag": "ROLLBACK"},
                                {"held_fence_observed": False}, {"released_fence_observed": False},
                                {"backend_connection_closed": False}, {"holding_backend": True}):
                    with self.subTest(profile=profile, changes=changes), self.assertRaises(AssertionError):
                        path.write_text(json.dumps({**valid, **changes}))
                        validate(profile, expected, output, context)

    def test_replication_and_engine_require_observed_wire_receipts(self):
        support = Path(__file__).resolve().parent
        validate = runpy.run_path(str(support.parents[1] / "tools" / "test-db.py"))["validate_module_receipt"]
        manifest = load_manifest()
        replication = json.loads((support / "replication_cases.json").read_text())
        old = manifest["transport_cases"] + manifest["copy_cases"]
        old += json.loads((support / "prepared_cases.json").read_text()) + json.loads((support / "authority_cases.json").read_text())
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned-fixture"}
            context["replication"] = dict(context)
            authority_records(context)
            records = replication_records(context)
            for profile, expected in (("Replication", replication), ("Engine", old + replication)):
                output = "".join(f"test {name} ... ok\n" for name in expected)
                validate(profile, expected, output, context)
                for path, valid in records:
                    path.unlink()
                    with self.subTest(profile=profile, case=valid["case"], missing=True), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)
                    for changes in ({"fault_hits": 0}, {"fault_hits": True}, {"status": "pending"}, {"status": "ignored"},
                                    {"case": "other"}, {"owner": "foreign"}, {"copyboth_seen": False}, {"backend_closed": False}):
                        path.write_text(json.dumps({**valid, **changes}))
                        with self.subTest(profile=profile, case=valid["case"], changes=changes), self.assertRaises(AssertionError):
                            validate(profile, expected, output, context)
                    path.write_text(json.dumps(valid))
                for case, changes in (("duplex", {"pqflush_pending": 0}), ("duplex", {"pqflush_pending": True}),
                                      ("duplex", {"server_frames_while_held": 0}), ("duplex", {"released": False}),
                                      ("keepalive", {"feedback_hex": (b'r' + bytes(32) + b'\1').hex()}),
                                      ("keepalive", {"post_arm_feedback_count": 0}),
                                      ("keepalive", {"feedback_mismatches": 1}),
                                      ("keepalive", {"keepalive_sent": False}), ("terminal_error", {"error_sqlstate": "23505"}),
                                      ("missing_completion", {"terminal_seen": False})):
                    path, valid = next(item for item in records if item[1]["case"] == case)
                    path.write_text(json.dumps({**valid, **changes}))
                    with self.subTest(profile=profile, case=case, changes=changes), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)
                    path.write_text(json.dumps(valid))
                extra = Path(directory) / "replication-fault-unexpected.json"
                extra.write_text("{}")
                with self.subTest(profile=profile, extra=True), self.assertRaises(AssertionError):
                    validate(profile, expected, output, context)
                extra.unlink()

    def test_module_inventories_require_authority_boundaries_and_replication(self):
        support = Path(__file__).resolve().parent
        manifest = load_manifest()
        replication = json.loads((support / "replication_cases.json").read_text())
        authority = module_cases(manifest, "Authority")
        boundaries = ["apply::checkpoint::live_tests::metadata_boundary_locks",
                      "apply::checkpoint::live_tests::transaction_boundary_faults"]
        self.assertEqual(len(authority), 18)
        self.assertEqual(authority[-2:], boundaries)
        old = manifest["transport_cases"] + manifest["copy_cases"]
        old += json.loads((support / "prepared_cases.json").read_text()) + authority
        self.assertEqual(len(replication), 9)
        self.assertEqual(len(old), 32)
        self.assertEqual(len(module_cases(manifest, "Engine")), 41)
        self.assertEqual(module_cases(manifest, "Engine"), old + replication)
        self.assertEqual(module_cases(manifest, "Replication"), replication)
        for expected in (authority, replication, old + replication):
            good = "".join(f"test {name} ... ok\n" for name in expected)
            validate_libtest_output(expected, good)
            for bad in ("", good.replace(" ... ok", " ... ignored", 1), "\n".join(good.splitlines()[1:]),
                        good + good, good.replace(expected[0], "unlisted_case"),
                        good + "test unexpected_case ... ok\n", good.replace(" ... ok", " ... FAILED", 1),
                        *(good.replace(f"test {case} ... ok\n", "") for case in boundaries if case in expected)):
                with self.subTest(expected=len(expected), output=bad), self.assertRaises(AssertionError):
                    validate_libtest_output(expected, bad)
            for inventory in ([], expected + [expected[0]]):
                with self.subTest(inventory=inventory), self.assertRaises(AssertionError):
                    validate_libtest_output(inventory, good)

    def test_terminal_error_requires_both_independent_phase_proofs(self):
        support = Path(__file__).resolve().parent
        validate = runpy.run_path(str(support.parents[1] / "tools" / "test-db.py"))["validate_module_receipt"]
        with tempfile.TemporaryDirectory() as directory:
            context = {"root": directory, "token": "owned-fixture"}
            context["replication"] = dict(context)
            authority_records(context)
            records = replication_records(context)
            path, valid = next(item for item in records if item[1]["case"] == "terminal_error")
            for profile in ("Replication", "Engine"):
                expected = module_cases(load_manifest(), profile)
                output = "".join(f"test {name} ... ok\n" for name in expected)
                path.write_text(json.dumps(valid))
                validate(profile, expected, output, context)
                for phase in ("late_error", "pending_error"):
                    bad = {**valid, "phases": {key: value for key, value in valid["phases"].items() if key != phase}}
                    path.write_text(json.dumps(bad))
                    with self.subTest(profile=profile, missing=phase), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)
                for changes in ({"pqflush_pending": 0}, {"cancel_requested": False}, {"backend_pid": 0},
                                {"gate_held_at_error": False}, {"client_error_sqlstate": "57013"},
                                {"client_closed_before_release": False}, {"client_before_deadline": False},
                                {"feedback_drained_after_cancel": 1}, {"released": True}, {"backend_closed": False}):
                    pending = {**valid["phases"]["pending_error"], **changes}
                    path.write_text(json.dumps({**valid, "phases": {**valid["phases"], "pending_error": pending}}))
                    with self.subTest(profile=profile, changes=changes), self.assertRaises(AssertionError):
                        validate(profile, expected, output, context)
                path.write_text(json.dumps(valid))


class NonemptyProgram(unittest.TestProgram):
    def runTests(self):
        if self.test.countTestCases() == 0:
            raise SystemExit("No harness tests were selected")
        super().runTests()


if __name__ == "__main__":
    NonemptyProgram()
