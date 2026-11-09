"""Private socket controls for extended COMMIT fault boundaries."""

from contextlib import contextmanager
import json
from pathlib import Path
import socket
import struct
import sys
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import follow_wire
from follow_wire import WireGate, CommitProtocol
from protocol_fault import receive


def frame(kind, payload=b""):
    return kind + struct.pack("!I", len(payload) + 4) + payload


def parse(sql=b"COMMIT", statement=b"", types=()):
    return frame(b'P', statement + b'\0' + sql + b'\0' + struct.pack("!H", len(types)) + b''.join(struct.pack("!I", oid) for oid in types))


def bind(statement=b"", portal=b"", values=()):
    return frame(b'B', portal + b'\0' + statement + b'\0' + struct.pack("!H", len(values)) + b'\0\0' * len(values) + struct.pack("!H", len(values)) + b''.join(struct.pack("!i", -1) if value is None else struct.pack("!I", len(value)) + value for value in values) + b'\0\1\0\0')


def execute(portal=b"", limit=0):
    return frame(b'E', portal + b'\0' + struct.pack("!I", limit))


def extended(sql=b"COMMIT", statement=b"", portal=b""):
    prefix = parse(sql, statement) + bind(statement, portal) + frame(b'D', b'P' + portal + b'\0')
    return prefix, execute(portal) + frame(b'S')


class ExtendedWireTests(unittest.TestCase):
    @contextmanager
    def relay(self, mode):
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
                payload = struct.pack("!I", 196608) + b'user\0fixture\0\0'
                startup = struct.pack("!I", len(payload) + 4) + payload
                producer.sendall(startup)
                self.assertTrue(receive(backend, len(startup)) == startup)
                pid = frame(b'K', struct.pack("!II", 123, 0x1234abcd))
                backend.sendall(pid)
                self.assertTrue(receive(producer, len(pid)) == pid)
                yield gate, producer, backend
            finally:
                producer.close()
                backend.close()

    def complete(self, producer, backend, sql, tag, state=b'T'):
        prefix, terminal = extended(sql)
        producer.sendall(prefix + terminal)
        self.assertTrue(receive(backend, len(prefix + terminal)) == prefix + terminal)
        response = frame(b'1') + frame(b'2') + frame(b'n') + frame(b'C', tag + b'\0') + frame(b'Z', state)
        backend.sendall(response)
        self.assertTrue(receive(producer, len(response)) == response)

    def test_unnamed_and_named_commit_hold_before_execute_then_release(self):
        for statement, portal in ((b'', b''), (b'commit_statement', b'commit_portal')):
            with self.subTest(named=bool(statement)), self.relay("before_commit") as (gate, producer, backend):
                self.complete(producer, backend, b'BEGIN', b'BEGIN')
                gate.arm()
                prefix, terminal = extended(statement=statement, portal=portal)
                producer.sendall(prefix + terminal)
                self.assertTrue(receive(backend, len(prefix)) == prefix)
                self.assertTrue(gate.held.wait(timeout=1), "extended COMMIT did not reach its gate")
                backend.settimeout(0.02)
                with self.assertRaises(socket.timeout):
                    backend.recv(1)
                backend.settimeout(2)
                observed = gate.fault_observation()
                self.assertEqual(observed["status"], "observed")
                self.assertEqual(observed["protocol"], "extended")
                self.assertEqual(observed["backend_pid"], 123)
                gate.release.set()
                self.assertTrue(receive(backend, len(terminal)) == terminal)

    def test_abort_drops_extended_execute_and_other_connection_stays_transparent(self):
        with self.relay("before_commit") as (gate, producer, backend):
            self.complete(producer, backend, b'BEGIN', b'BEGIN')
            gate.arm()
            prefix, terminal = extended()
            producer.sendall(prefix + terminal)
            self.assertTrue(receive(backend, len(prefix)) == prefix)
            self.assertTrue(gate.held.wait(timeout=1))
            producer.shutdown(socket.SHUT_RDWR)
            gate.abort_hold()
            self.assertEqual(backend.recv(1), b'')
            other_client, other_producer = socket.socketpair()
            other_server, other_backend = socket.socketpair()
            gate.sockets.extend((other_client, other_server))
            other_backend.settimeout(2)
            worker = threading.Thread(target=gate.downstream, args=(other_client, other_server), daemon=True)
            gate.threads.append(worker)
            worker.start()
            try:
                startup = struct.pack("!II", 8, 196608)
                other_producer.sendall(startup + prefix + terminal)
                self.assertTrue(receive(other_backend, len(startup + prefix + terminal)) == startup + prefix + terminal)
            finally:
                other_producer.close()
                other_backend.close()

    def test_pipeline_data_cas_and_commit_keeps_transaction_tag_witness(self):
        with self.relay("before_commit") as (gate, producer, backend):
            plans = ((b'update', b'UPDATE fixture SET x=$1'), (b'insert', b'INSERT INTO fixture VALUES($1)'), (b'cas', b'UPDATE metadata SET x=$1'))
            prepared = b''.join(parse(sql, name, (23,)) for name, sql in plans) + frame(b'S')
            producer.sendall(prepared)
            self.assertTrue(receive(backend, len(prepared)) == prepared)
            response = frame(b'1') * 3 + frame(b'Z', b'I')
            backend.sendall(response)
            self.assertTrue(receive(producer, len(response)) == response)
            batch = parse(b'BEGIN') + bind() + execute()
            batch += b''.join(bind(name, values=(b'1',)) + frame(b'D', b'P\0') + execute() for name, _ in plans) + frame(b'S')
            producer.sendall(batch)
            self.assertTrue(receive(backend, len(batch)) == batch)
            response = frame(b'1') + b''.join(frame(b'2') + frame(b'C', tag + b'\0') for tag in (b'BEGIN', b'UPDATE 1', b'INSERT 0 1', b'UPDATE 1')) + frame(b'Z', b'T')
            backend.sendall(response)
            self.assertTrue(receive(producer, len(response)) == response)
            gate.arm()
            prefix, terminal = extended()
            producer.sendall(prefix + terminal)
            self.assertTrue(receive(backend, len(prefix)) == prefix)
            self.assertTrue(gate.held.wait(timeout=1))
            observed = gate.fault_observation()
            self.assertTrue(observed["begin_observed"])
            self.assertEqual(observed["command_tags"], {"insert_0_1": 1, "update_1": 2, "delete_1": 0, "other_dml": 0})
            self.assertNotIn("fixture", json.dumps(observed))
            gate.abort_hold()

    def test_after_commit_holds_command_complete_and_ready_after_server_outcome(self):
        with self.relay("after_commit") as (gate, producer, backend):
            self.complete(producer, backend, b'BEGIN', b'BEGIN')
            self.complete(producer, backend, b'INSERT INTO fixture VALUES(1)', b'INSERT 0 1')
            gate.arm()
            prefix, terminal = extended()
            producer.sendall(prefix + terminal)
            self.assertTrue(receive(backend, len(prefix + terminal)) == prefix + terminal)
            acknowledgments = frame(b'1') + frame(b'2') + frame(b'n')
            completion = frame(b'C', b'COMMIT\0') + frame(b'Z', b'I')
            backend.sendall(acknowledgments + completion)
            self.assertTrue(receive(producer, len(acknowledgments)) == acknowledgments)
            self.assertTrue(gate.held.wait(timeout=1))
            producer.settimeout(0.02)
            with self.assertRaises(socket.timeout):
                producer.recv(1)
            producer.settimeout(2)
            observed = gate.fault_observation()
            self.assertEqual(observed["status"], "observed")
            self.assertEqual(observed["protocol"], "extended")
            self.assertEqual(observed["command_tags"]["insert_0_1"], 1)
            gate.release.set()
            self.assertTrue(receive(producer, len(completion)) == completion)

    def test_simple_query_commit_compatibility(self):
        with self.relay("before_commit") as (gate, producer, backend):
            gate.arm()
            request = frame(b'Q', b'COMMIT\0')
            producer.sendall(request)
            self.assertTrue(gate.held.wait(timeout=1))
            gate.release.set()
            self.assertTrue(receive(backend, len(request)) == request)

    def test_query_literals_bind_values_and_replaced_portal_do_not_become_commit(self):
        with self.relay("before_commit") as (gate, producer, backend):
            gate.arm()
            request = parse() + bind() + parse(b"SELECT 'COMMIT'", b'other') + bind(b'other') + execute() + frame(b'S')
            producer.sendall(request)
            self.assertTrue(receive(backend, len(request)) == request)
            request = parse(b'SELECT $1', types=(25,)) + bind(values=(b'COMMIT',)) + execute() + frame(b'S')
            producer.sendall(request)
            self.assertTrue(receive(backend, len(request)) == request)
            request = parse(b'SELECT 1') + bind() + execute(limit=1) + frame(b'S')
            producer.sendall(request)
            self.assertTrue(receive(backend, len(request)) == request)
            self.assertFalse(gate.held.is_set())
            self.assertEqual(gate.evidence()["hits"], 0)

    def test_closed_or_unknown_portal_and_malformed_or_limited_execute_refuse(self):
        for request in (execute(b'unknown'), parse() + bind() + frame(b'C', b'P\0') + execute(),
                        parse() + bind() + execute(limit=1), parse() + frame(b'B', b'\0\0\0'),
                        frame(b'P', b'\0COMMIT\0\0'), frame(b'E', b'\0\0'),
                        parse() + bind() + frame(b'D', b'Punknown\0') + execute(),
                        parse(statement=b'\xff') + bind(b'\xff') + execute(),
                        parse() + frame(b'B', b'\0\0\0\0\0\0\0\2\0\0\0\0') + execute()):
            with self.subTest(size=len(request)), self.relay("before_commit") as (gate, producer, backend):
                gate.arm()
                producer.sendall(request)
                backend.settimeout(0.1)
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline and not gate.errors:
                    try:
                        if not backend.recv(65536):
                            break
                    except socket.timeout:
                        continue
                self.assertTrue(gate.errors)
                self.assertFalse(gate.held.is_set())
                with self.assertRaises(RuntimeError):
                    gate.evidence()

    def test_close_replacement_and_large_noncommit_streams_keep_bounded_state(self):
        protocol = CommitProtocol()
        def accept(encoded):
            return protocol.frontend(encoded[:1], encoded[5:])
        accept(parse(statement=b'named'))
        accept(frame(b'C', b'Snamed\0'))
        accept(parse(b'SELECT 1', statement=b'named'))
        accept(bind(b'named'))
        self.assertFalse(accept(execute()))
        for _ in range(1100):
            accept(parse(b'SELECT ' + b'x' * 4096))
            accept(bind())
            self.assertFalse(accept(execute()))
        self.assertEqual(len(protocol.statements), 2)
        self.assertEqual(len(protocol.portals), 1)
        self.assertFalse(protocol.invalid)
        self.assertNotIn(b'SELECT', repr(vars(protocol)).encode())

    def test_uncertain_named_commit_mapping_cannot_be_reused_or_reparsed_as_fresh(self):
        for operation in (bind(b'commit'), parse(statement=b'commit')):
            protocol = CommitProtocol()
            protocol.frontend(b'P', parse(statement=b'commit')[5:])
            protocol.frontend(b'P', parse(b'SELECT 1')[5:])
            protocol.frontend(b'B', bind()[5:])
            protocol.frontend(b'E', execute()[5:])
            with self.assertRaises(ValueError):
                protocol.frontend(operation[:1], operation[5:])
            self.assertTrue(protocol.invalid)

    def test_parameter_and_outstanding_sync_bounds_refuse_explicitly(self):
        for encoded in (parse(types=(25,) * 513), bind(values=(b'x',) * 513)):
            protocol = CommitProtocol()
            protocol.frontend(b'P', parse(b'SELECT 1')[5:])
            with self.assertRaises(ValueError):
                protocol.frontend(encoded[:1], encoded[5:])
            self.assertTrue(protocol.overflow)
        protocol = CommitProtocol()
        for _ in range(64):
            protocol.frontend(b'S', b'')
        with self.assertRaises(ValueError):
            protocol.frontend(b'S', b'')
        self.assertTrue(protocol.overflow)

    def test_real_tcp_accept_path_enables_both_nodelay_options_and_preserves_frames(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        listener.settimeout(2)
        connect = socket.create_connection
        try:
            with patch.object(follow_wire.socket, "create_connection", side_effect=lambda _address, timeout: connect(listener.getsockname(), timeout=timeout)), WireGate("fixture", "observe") as gate:
                producer = connect(("127.0.0.1", gate.port), timeout=2)
                backend, _ = listener.accept()
                backend.settimeout(2)
                try:
                    payload = struct.pack("!I", 196608) + b'user\0fixture\0\0'
                    startup = struct.pack("!I", len(payload) + 4) + payload
                    producer.sendall(startup)
                    self.assertTrue(receive(backend, len(startup)) == startup)
                    self.assertEqual(len(gate.sockets), 2)
                    self.assertTrue(all(stream.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) != 0 for stream in gate.sockets))
                    request = frame(b'P', b'\0SELECT 1\0\0\0') + frame(b'S')
                    producer.sendall(request)
                    self.assertTrue(receive(backend, len(request)) == request)
                    response = frame(b'1') + frame(b'Z', b'I')
                    backend.sendall(response)
                    self.assertTrue(receive(producer, len(response)) == response)
                finally:
                    producer.close()
                    backend.close()
        finally:
            listener.close()

    def test_parse_error_and_statement_overflow_cannot_qualify_fault(self):
        with self.relay("before_commit") as (gate, producer, backend):
            request = parse(b'SELECT invalid') + frame(b'S')
            producer.sendall(request)
            self.assertTrue(receive(backend, len(request)) == request)
            response = frame(b'E', b'SERROR\0C42601\0Mfixture-sensitive-row-private\0\0') + frame(b'Z', b'I')
            backend.sendall(response)
            self.assertTrue(receive(producer, len(response)) == response)
            gate.arm()
            producer.sendall(parse() + bind() + execute() + frame(b'S'))
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not gate.errors:
                time.sleep(0.005)
            self.assertTrue(gate.errors)
            self.assertFalse(gate.held.is_set())
        with self.relay("before_commit") as (gate, producer, backend):
            gate.arm()
            producer.sendall(b''.join(parse(b'SELECT 1', b'n' + str(index).encode()) for index in range(257)))
            backend.settimeout(0.1)
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and not gate.errors:
                try:
                    if not backend.recv(65536):
                        break
                except socket.timeout:
                    continue
            self.assertTrue(gate.errors)
            self.assertEqual(gate.fault_observation()["status"], "unavailable")


if __name__ == "__main__":
    unittest.main()
