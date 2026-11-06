"""Four bounded faults on a real owned CopyBoth stream; no acknowledgment policy."""

import argparse
import json
import os
from pathlib import Path
import re
import select
import signal
import socket
import struct
import sys
import threading
import time

from db_harness import assert_identity, conninfo, psql, quote
from protocol_fault import ProtocolFault, error_sqlstate, packet
from replication_fixture import FAULT_CASES, validate_fault, validate_terminal_phase


class ReplicationFault(ProtocolFault):
    def __init__(self, context, case, pending_error=False):
        super().__init__(context, "replication")
        self.case = case
        self.pending_error = pending_error
        self.state_lock = threading.RLock()
        self.send_lock = threading.Lock()
        self.gate_active = threading.Event()
        self.gate_release = threading.Event()
        self.client = None
        self.state = {"owner": context["token"], "case": case, "fault_hits": 0, "copyboth_seen": False,
                      "feedback_hex": "", "expected_feedback_hex": "", "feedback_count": 0,
                      "post_arm_feedback_count": 0, "feedback_mismatches": 0,
                      "keepalive_sent": False, "held": False, "released": False,
                      "server_frames_while_held": 0, "pqflush_pending": 0,
                      "copydone_seen": False, "terminal_seen": False, "terminal_fault": None,
                      "error_sqlstate": None}
        self.state.update(backend_pid=None, cancel_requested=False, owned_backend_verified=False,
                          server_error_seen=False, gate_held_at_error=False, client_error_sqlstate=None,
                          client_closed_before_release=False, client_before_deadline=False,
                          feedback_drained_after_cancel=None)

    def snapshot(self):
        with self.state_lock:
            value = {**self.state, "backend_closed": self.backend_closed.is_set(), "status": "observed"}
        try:
            if self.case == "terminal_error":
                validate_terminal_phase(value, self.context, "pending_error" if self.pending_error else "late_error")
            else:
                validate_fault(value, self.context, self.case)
            assert not self.errors
        except (AssertionError, KeyError):
            value["status"] = "pending"
        return value

    def send_client(self, frame):
        with self.send_lock:
            self.client.sendall(frame)

    def operate(self, request):
        operation = request.get("op")
        if operation == "arm":
            with self.state_lock:
                assert self.state["copyboth_seen"] and not self.armed.is_set(), "arm requires one live CopyBoth stream"
            if self.case == "keepalive":
                assert set(request) == {"op", "wal_end", "feedback_hex"}
                end, feedback = request["wal_end"], request["feedback_hex"]
                assert type(end) is int and 0 <= end < 2**64
                assert isinstance(feedback, str) and re.fullmatch(r"72[0-9a-f]{66}", feedback)
                with self.state_lock:
                    self.state["expected_feedback_hex"] = feedback
                    self.armed.set()
                payload = b'k' + struct.pack("!QQB", end, 0, 1)
                self.send_client(b'd' + struct.pack("!I", len(payload) + 4) + payload)
                with self.state_lock:
                    self.state.update(keepalive_sent=True, fault_hits=1)
            else:
                assert set(request) == {"op"}
                self.armed.set()
                if self.case == "duplex" or self.pending_error:
                    assert self.gate_active.wait(timeout=3), "upstream read gate did not become active"
                    with self.state_lock:
                        if not self.pending_error:
                            self.state["fault_hits"] = 1
        elif operation == "cancel":
            assert self.pending_error and set(request) == {"op", "pqflush_pending"}
            pending = request["pqflush_pending"]
            assert type(pending) is int and pending > 0
            with self.state_lock:
                assert self.state["held"] and not self.state["cancel_requested"]
                pid = self.state["backend_pid"]
                assert type(pid) is int and pid > 0
                self.state.update(cancel_requested=True, pqflush_pending=pending)
            result = psql(self.context, self.context["database"], f"""SELECT pg_cancel_backend(a.pid)
                FROM pg_stat_activity a WHERE a.pid={pid} AND a.datname={quote(self.context['database'])}
                AND a.usename={quote(self.context['replication_role'])} AND a.backend_type='walsender'
                AND EXISTS(SELECT FROM pg_replication_slots s WHERE s.active_pid=a.pid
                           AND s.database=a.datname AND s.plugin='test_decoding' AND s.temporary);""")
            assert result == "t", "cancel target is not the observed owned logical backend"
            with self.state_lock:
                self.state["owned_backend_verified"] = True
        elif operation == "client_error":
            assert self.pending_error and set(request) == {"op", "sqlstate", "closed", "before_deadline", "feedback_drained"}
            assert request["sqlstate"] == "57014" and request["closed"] is True and request["before_deadline"] is True
            assert type(request["feedback_drained"]) is int and request["feedback_drained"] == 0
            with self.state_lock:
                assert self.state["held"] and not self.gate_release.is_set()
                assert self.state["server_error_seen"] and self.state["error_sqlstate"] == "57014"
                self.state.update(client_error_sqlstate="57014", client_closed_before_release=True,
                                  client_before_deadline=True, feedback_drained_after_cancel=0)
        elif operation == "release":
            assert self.case == "duplex" and set(request) == {"op", "pqflush_pending"}
            pending = request["pqflush_pending"]
            assert type(pending) is int and pending > 0, "require test-observed PQflush==1"
            with self.state_lock:
                assert self.state["held"] and self.state["server_frames_while_held"] > 0
                self.state.update(pqflush_pending=pending, held=False, released=True)
            self.gate_release.set()
        else:
            assert operation in ("status", "quit") and set(request) == {"op"}, "unknown controller operation"

    def upstream(self, client, server):
        try:
            _, frame, _ = packet(client, typed=False)
            server.sendall(frame)
            while not self.stopping.is_set():
                if (self.case == "duplex" or self.pending_error) and self.armed.is_set() and not self.gate_release.is_set():
                    with self.state_lock:
                        self.state["held"] = True
                    self.gate_active.set()
                    deadline = time.monotonic() + 45
                    while not self.gate_release.wait(timeout=0.02):
                        if self.stopping.is_set():
                            return
                        if time.monotonic() >= deadline:
                            raise RuntimeError("duplex gate release deadline")
                if not select.select([client], [], [], 0.02)[0]:
                    continue
                kind, frame, payload = packet(client)
                if kind == b'd':
                    assert len(payload) == 34 and payload[:1] == b'r', "expected one full r34 feedback frame"
                    with self.state_lock:
                        self.state["feedback_count"] += 1
                        if self.case != "keepalive" or self.armed.is_set():
                            if not self.state["feedback_hex"]:
                                self.state["feedback_hex"] = payload.hex()
                            if self.case == "keepalive":
                                self.state["post_arm_feedback_count"] += 1
                                if payload.hex() != self.state["expected_feedback_hex"]:
                                    self.state["feedback_mismatches"] += 1
                server.sendall(frame)
        except (EOFError, OSError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__)

    def run(self):
        while not self.stopping.is_set():
            try:
                client, _ = self.listener.accept()
                break
            except socket.timeout:
                continue
            except OSError:
                return
        else:
            return
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.client = client
        self.sockets = [client, server]
        upstream = None
        try:
            client.settimeout(30)
            server.settimeout(30)
            client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            server.connect(str(Path(self.context["socket_dir"]) / f".s.PGSQL.{self.context['port']}"))
            upstream = threading.Thread(target=self.upstream, args=(client, server), daemon=True)
            upstream.start()
            while not self.stopping.is_set():
                kind, frame, payload = packet(server)
                with self.state_lock:
                    if kind == b'K':
                        assert len(payload) == 8
                        self.state["backend_pid"] = struct.unpack("!I", payload[:4])[0]
                    if kind == b'W':
                        assert payload == b'\0\0\0', "unexpected CopyBoth response shape"
                        self.state["copyboth_seen"] = True
                    if kind == b'c':
                        self.state["copydone_seen"] = True
                    if self.pending_error and kind == b'E':
                        assert self.state["cancel_requested"]
                        self.state.update(server_error_seen=True, error_sqlstate=error_sqlstate(payload),
                                          fault_hits=1, gate_held_at_error=self.state["held"] and not self.gate_release.is_set())
                    terminal = (not self.pending_error and self.armed.is_set() and self.case in ("terminal_error", "missing_completion")
                                and self.state["copydone_seen"] and kind == b'C')
                    if terminal:
                        self.state.update(fault_hits=1, terminal_seen=True,
                                          terminal_fault="error" if self.case == "terminal_error" else "missing")
                if terminal:
                    if self.case == "terminal_error":
                        payload = b'SERROR\0VERROR\0C57014\0Mfixture terminal replication failure\0\0'
                        self.send_client(b'E' + struct.pack("!I", len(payload) + 4) + payload)
                        with self.state_lock:
                            self.state["error_sqlstate"] = "57014"
                    return
                self.send_client(frame)
                if kind == b'd' and payload[:1] in (b'w', b'k'):
                    with self.state_lock:
                        if self.state["held"]:
                            self.state["server_frames_while_held"] += 1
        except (EOFError, OSError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
            self.stopping.set()
            for sock in self.sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
            if upstream:
                upstream.join(timeout=2)
                if upstream.is_alive():
                    self.errors.append("upstream did not stop")
            self.backend_closed.set()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=FAULT_CASES, required=True)
    parser.add_argument("--pending-error", action="store_true")
    args = parser.parse_args()
    case = args.case
    assert not args.pending_error or case == "terminal_error", "pending error belongs to the terminal group"
    context = json.loads(Path(os.environ["RPGMIGRATE_TEST_REPLICATION_CONTEXT"]).read_text())
    assert_identity(context)
    receipt = Path(context["root"]) / f"replication-fault-{case}.json"
    previous = {}
    if args.pending_error:
        prior = json.loads(receipt.read_text())
        assert prior["owner"] == context["token"] and prior["case"] == case and prior["status"] == "pending"
        assert set(prior["phases"]) == {"late_error"}, "missing or duplicate terminal phase"
        validate_terminal_phase(prior["phases"]["late_error"], context, "late_error")
        previous = prior["phases"]
    else:
        receipt.touch(mode=0o600, exist_ok=False)
    relay = ReplicationFault(context, case, args.pending_error)

    def record():
        value = relay.snapshot()
        if case != "terminal_error":
            return value, value
        phase = "pending_error" if args.pending_error else "late_error"
        phases = {**previous, phase: value}
        group = {**phases.get("late_error", value), "phases": phases,
                 "fault_hits": sum(proof["fault_hits"] for proof in phases.values()), "status": "observed"}
        try:
            validate_fault(group, context, case)
        except (AssertionError, KeyError):
            group["status"] = "pending"
        return group, {**value, "status": group["status"], "phase_status": value["status"], "phase": phase}
    quit_requested = False
    try:
        with relay:
            source = conninfo(context, context["database"], context["replication_role"], str(relay.directory))
            print(json.dumps({"ready": True, "conninfo": source + " replication=database", "receipt": str(receipt)}), flush=True)
            for line in iter(lambda: sys.stdin.readline(4097), ""):
                assert len(line) <= 4096, "controller request exceeds bound"
                request = json.loads(line)
                relay.operate(request)
                if request["op"] == "quit":
                    quit_requested = True
                    break
                group, response = record()
                receipt.write_text(json.dumps(group))
                print(json.dumps({"op": request["op"], **response}), flush=True)
    finally:
        receipt.write_text(json.dumps(record()[0]))
    if quit_requested:
        print(json.dumps({"op": "quit", **record()[1]}), flush=True)


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
