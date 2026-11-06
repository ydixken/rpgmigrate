"""Exercise the owned logical source and four faults with controlled native libpq."""

import ctypes as c
import json
import os
from pathlib import Path
import select
import signal
import socket
import struct
import subprocess
import sys
import time

from db_harness import clean_environment, load_manifest, psql
from authority_selfcheck import line_from
from replication_fixture import FAULT_CASES, replication_fixture, validate_replication_faults


class Client:
    def __init__(self, connection):
        directory = Path(os.environ["PQ_LIB_DIR"]).resolve(strict=True)
        name = "libpq.dylib" if sys.platform == "darwin" else "libpq.so"
        self.lib = c.CDLL(str(directory / name))
        signatures = {
            "PQconnectdb": (c.c_void_p, [c.c_char_p]), "PQstatus": (c.c_int, [c.c_void_p]),
            "PQexec": (c.c_void_p, [c.c_void_p, c.c_char_p]), "PQresultStatus": (c.c_int, [c.c_void_p]),
            "PQclear": (None, [c.c_void_p]), "PQfinish": (None, [c.c_void_p]),
            "PQsetnonblocking": (c.c_int, [c.c_void_p, c.c_int]), "PQsocket": (c.c_int, [c.c_void_p]),
            "PQgetCopyData": (c.c_int, [c.c_void_p, c.POINTER(c.c_void_p), c.c_int]),
            "PQfreemem": (None, [c.c_void_p]), "PQconsumeInput": (c.c_int, [c.c_void_p]),
            "PQputCopyData": (c.c_int, [c.c_void_p, c.c_char_p, c.c_int]),
            "PQputCopyEnd": (c.c_int, [c.c_void_p, c.c_char_p]), "PQflush": (c.c_int, [c.c_void_p]),
            "PQisBusy": (c.c_int, [c.c_void_p]), "PQgetResult": (c.c_void_p, [c.c_void_p]),
            "PQresultErrorField": (c.c_char_p, [c.c_void_p, c.c_int]),
        }
        for function, (result, arguments) in signatures.items():
            value = getattr(self.lib, function)
            value.restype, value.argtypes = result, arguments
        self.raw = self.lib.PQconnectdb(connection.encode())
        self.error_states = []
        if not self.raw or self.lib.PQstatus(self.raw) != 0:
            self.close()
            raise AssertionError("owned replication connection failed")

    def execute(self, sql, expected):
        result = self.lib.PQexec(self.raw, sql.encode())
        try:
            assert result and self.lib.PQresultStatus(result) == expected, "unexpected owned replication result"
        finally:
            if result:
                self.lib.PQclear(result)

    def start(self, slot):
        self.execute(f"CREATE_REPLICATION_SLOT {slot} TEMPORARY LOGICAL test_decoding NOEXPORT_SNAPSHOT", 2)
        self.execute(f"START_REPLICATION SLOT {slot} LOGICAL 0/0", 8)
        assert self.lib.PQsetnonblocking(self.raw, 1) == 0

    def read(self, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            pointer = c.c_void_p()
            length = self.lib.PQgetCopyData(self.raw, c.byref(pointer), 1)
            if length > 0:
                try:
                    return c.string_at(pointer, length)
                finally:
                    self.lib.PQfreemem(pointer)
            if length < 0:
                return None
            if select.select([self.lib.PQsocket(self.raw)], [], [], min(0.1, max(0, deadline-time.monotonic())))[0]:
                assert self.lib.PQconsumeInput(self.raw) == 1, "replication connection closed"
        raise AssertionError("replication frame deadline exceeded")

    def flush(self):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            state = self.lib.PQflush(self.raw)
            assert state >= 0, "replication flush failed"
            if state == 0:
                return
            select.select([], [self.lib.PQsocket(self.raw)], [], 0.05)
            assert self.lib.PQconsumeInput(self.raw) == 1
        raise AssertionError("replication flush deadline exceeded")

    def finish(self):
        assert self.lib.PQputCopyEnd(self.raw, None) == 1
        self.flush()
        while self.read() is not None:
            pass
        return self.drain_results()

    def drain_results(self):
        states = []
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self.lib.PQisBusy(self.raw):
                select.select([self.lib.PQsocket(self.raw)], [], [], 0.05)
                assert self.lib.PQconsumeInput(self.raw) == 1, "terminal connection closed"
                continue
            result = self.lib.PQgetResult(self.raw)
            if not result:
                return states
            states.append(self.lib.PQresultStatus(result))
            state = self.lib.PQresultErrorField(result, ord('C'))
            if state is not None:
                self.error_states.append(state.decode('ascii'))
            self.lib.PQclear(result)
        raise AssertionError("terminal result deadline exceeded")

    def close(self):
        if self.raw:
            self.lib.PQfinish(self.raw)
            self.raw = None


def self_check(context, environment):
    # Native libpq also reads this process's environment, outside child execution.
    for name in list(os.environ):
        if name.startswith(("PG", "RPGMIGRATE")):
            del os.environ[name]
    helper_path = Path(environment["RPGMIGRATE_TEST_REPLICATION_FAULT_HELPER"])
    assert helper_path.is_file(), "replication fault controller is missing"
    psql(context, context["database"], "CREATE TABLE public.replication_probe(value text);")
    child_environment = {**clean_environment(os.environ), **environment, "HOME": str(Path(context["root"]) / "home")}
    feedback = b'r' + bytes(33)
    scenarios = [(case, pending) for case in FAULT_CASES for pending in ((False, True) if case == "terminal_error" else (False,))]
    for case, pending_error in scenarios:
        command = [sys.executable, str(helper_path), "--case", case]
        if pending_error:
            command.append("--pending-error")
        helper = subprocess.Popen(command, env=child_environment,
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=0)
        client = None
        try:
            ready = json.loads(line_from(helper))
            assert ready["ready"] is True

            def operation(op, **fields):
                helper.stdin.write((json.dumps({"op": op, **fields}) + "\n").encode())
                result = json.loads(line_from(helper))
                assert result["op"] == op
                return result

            client = Client(ready["conninfo"])
            client.start("selfcheck_" + case + ("_pending" if pending_error else ""))
            if case == "keepalive":
                operation("arm", wal_end=900000, feedback_hex=feedback.hex())
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    frame = client.read(timeout=max(0.01, deadline - time.monotonic()))
                    assert frame is not None and frame[:1] == b'k' and len(frame) == 18
                    if frame == b'k' + struct.pack("!QQB", 900000, 0, 1):
                        break
                else:
                    raise AssertionError("known keepalive was not delivered")
                assert client.lib.PQputCopyData(client.raw, feedback, len(feedback)) == 1
                client.flush()
                deadline = time.monotonic() + 5
                while operation("status")["feedback_count"] == 0 and time.monotonic() < deadline:
                    time.sleep(0.01)
                psql(context, context["database"], "INSERT INTO public.replication_probe VALUES(repeat('x',8388621));")
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    frame = client.read()
                    assert frame is not None
                    if len(frame) > 8 * 1024 * 1024:
                        assert frame.startswith(b'w') and b"'" + b'x' * 8388621 + b"'" in frame
                        break
                else:
                    raise AssertionError("oversize replication frame missing")
                states = client.finish()
                assert states and all(state == 1 for state in states)
            elif case == "duplex" or pending_error:
                descriptor = client.lib.PQsocket(client.raw)
                duplicate = socket.fromfd(descriptor, socket.AF_UNIX, socket.SOCK_STREAM)
                duplicate.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
                duplicate.close()
                operation("arm")
                pending = 0
                for _ in range(100000):
                    accepted = client.lib.PQputCopyData(client.raw, feedback, len(feedback))
                    assert accepted >= 0
                    state = client.lib.PQflush(client.raw)
                    assert state >= 0
                    if state == 1:
                        pending += 1
                        break
                assert pending > 0, "read gate did not create actual PQflush==1"
                if pending_error:
                    deadline = time.monotonic() + 5
                    operation("cancel", pqflush_pending=pending)
                    while client.read(timeout=max(0.01, deadline - time.monotonic())) is not None:
                        assert time.monotonic() < deadline
                    states = client.drain_results()
                    assert states and client.error_states == ["57014"] and time.monotonic() < deadline
                    client.close()
                    operation("client_error", sqlstate=client.error_states[0], closed=client.raw is None,
                              before_deadline=time.monotonic() < deadline, feedback_drained=0)
                else:
                    psql(context, context["database"], "INSERT INTO public.replication_probe VALUES('duplex-live');")
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        frame = client.read()
                        assert frame is not None
                        if frame.startswith(b'w'):
                            break
                    else:
                        raise AssertionError("no WAL frame arrived during feedback backpressure")
                    while operation("status")["server_frames_while_held"] == 0:
                        assert time.monotonic() < deadline
                        time.sleep(0.01)
                    operation("release", pqflush_pending=pending)
                    client.flush()
                    states = client.finish()
                    assert states and all(state == 1 for state in states)
            else:
                operation("arm")
                failed = False
                try:
                    states = client.finish()
                    failed = not states or any(state != 1 for state in states)
                except AssertionError:
                    failed = True
                assert failed, "terminal fault was reported as successful completion"
                if case == "terminal_error":
                    assert client.error_states == ["57014"], "terminal error did not reach the client with its actual SQLSTATE"
            client.close()
            final = operation("quit")
            if case == "terminal_error":
                assert final["phase_status"] == "observed"
            expected = "pending" if case == "terminal_error" and not pending_error else "observed"
            assert final["status"] == expected, "wire fault did not satisfy its independent receipt"
            helper.communicate(timeout=5)
            assert helper.returncode == 0
        finally:
            if client is not None:
                client.close()
            if helper.poll() is None:
                helper.terminate()
                try:
                    helper.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    helper.kill()
                    helper.communicate(timeout=5)
    validate_replication_faults(context)
    receipt = Path(context["root"]) / "replication-fault-terminal_error.json"
    before = receipt.read_bytes()
    for flags in ([], ["--pending-error"]):
        duplicate = subprocess.run([sys.executable, str(helper_path), "--case", "terminal_error", *flags],
                                   env=child_environment, capture_output=True, timeout=10)
        assert duplicate.returncode != 0 and receipt.read_bytes() == before, "duplicate phase altered an observed receipt"
    deadline = time.monotonic() + 5
    while psql(context, context["database"], "SELECT count(*) FROM pg_replication_slots;") != "0":
        assert time.monotonic() < deadline, "owned temporary replication slots survived client close"
        time.sleep(0.02)
    return {"replication_fixture": "passed", "wire_faults": 4, "pqflush_pending": True,
            "oversize_frame": True, "temporary_slots_removed": True, "terminal_error_phases": 2}


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    with replication_fixture(load_manifest()) as (context, environment):
        print(json.dumps(self_check(context, environment), sort_keys=True))
