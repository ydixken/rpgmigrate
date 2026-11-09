"""Protocol-level checks for the owned receiver peer."""

import contextlib
import json
from pathlib import Path
import socket
import struct
import subprocess
import sys
import unittest

import follow_receive_peer as peer


class ReceiverPeerTests(unittest.TestCase):
    @contextlib.contextmanager
    def connect(self, case):
        process = subprocess.Popen(
            [sys.executable, str(Path(peer.__file__)), "--case", case],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        connection = None
        try:
            line = process.stdout.readline()
            self.assertTrue(line, process.stderr.read() if process.poll() is not None else "peer did not start")
            ready = json.loads(line)
            connection = socket.create_connection(("127.0.0.1", ready["port"]), timeout=4)
            startup = struct.pack("!I", 196608) + b"user\x00peer\x00replication\x00database\x00\x00"
            connection.sendall(struct.pack("!I", len(startup) + 4) + startup)
            while self.read(connection)[0] != b"Z":
                pass
            connection.sendall(peer.message(b"Q", b"START_REPLICATION SLOT owned LOGICAL 0/11\x00"))
            self.assertEqual(self.read(connection), (b"W", b"\x00\x00\x00"))
            yield connection, process
            connection.close()
            connection = None
            process.wait(timeout=5)
            self.assertEqual(process.returncode, 0, process.stderr.read())
        finally:
            if connection is not None:
                connection.close()
            if process.poll() is None:
                process.kill()
            process.communicate()
            process.stdout.close()
            process.stderr.close()

    def read(self, connection):
        kind = peer.exact(connection, 1)
        size = struct.unpack("!I", peer.exact(connection, 4))[0]
        return kind, peer.exact(connection, size - 4)

    def status(self, connection, reply=False):
        connection.sendall(peer.message(b"d", b"r" + struct.pack("!QQQqB", 17, 17, 17, 0, reply)))

    def terminal(self, connection, count=2):
        self.assertEqual(self.read(connection), (b"c", b""))
        self.assertEqual(self.read(connection), (b"C", b"COPY 0\x00"))
        self.assertEqual(self.read(connection), (b"C", b"START_REPLICATION\x00"))
        if count == 3:
            self.assertEqual(self.read(connection), (b"C", b"COPY 0\x00"))
        self.assertEqual(self.read(connection), (b"Z", b"I"))
        connection.sendall(peer.message(b"X", b""))

    def receipt(self, process, feedback, replies):
        receipt = json.loads(process.stdout.readline())
        self.assertEqual(receipt, {
            "status": "observed", "feedback_count": feedback,
            "positions_unchanged": True, "closed": True,
            "copy_done": True, "reply_requested_count": replies,
        })

    def test_idle_request_receipt_is_once_and_feedback_stays_unchanged(self):
        with self.connect("stop_idle") as (connection, process):
            connection.settimeout(0.1)
            with self.assertRaises(socket.timeout):
                connection.recv(1)
            connection.settimeout(4)
            self.status(connection)
            self.status(connection, True)
            self.assertEqual(json.loads(process.stdout.readline()), {
                "reply_requested": True, "positions_unchanged": True,
            })
            self.status(connection, True)
            connection.sendall(peer.message(b"c", b""))
            self.terminal(connection)
            self.receipt(process, 3, 2)

    def test_partial_later_frame_finishes_only_after_copy_done(self):
        with self.connect("stop_partial") as (connection, process):
            kind, begin = self.read(connection)
            self.assertEqual(kind, b"d")
            self.assertEqual(begin[25:], b"B" + struct.pack("!QqI", 100, 123, 9))
            prefix = peer.exact(connection, 17)
            self.assertEqual(prefix[:1], b"d")
            connection.sendall(peer.message(b"c", b""))
            remaining = struct.unpack("!I", prefix[1:5])[0] + 1 - len(prefix)
            later = prefix + peer.exact(connection, remaining)
            self.assertEqual(later[5:6], b"w")
            self.assertEqual(struct.unpack("!QQq", later[6:30]), (500, 900, 0))
            self.assertEqual(later[30:], b"invalid-later-pgoutput")
            self.terminal(connection)
            self.receipt(process, 0, 0)

    def test_error_is_real_protocol_error_and_close_is_observed(self):
        with self.connect("stop_error") as (connection, process):
            connection.sendall(peer.message(b"c", b""))
            kind, payload = self.read(connection)
            self.assertEqual(kind, b"E")
            self.assertIn(b"CXX000\x00", payload)
            self.assertTrue(payload.endswith(b"\x00\x00"))
            self.assertEqual(self.read(connection), (b"Z", b"I"))
            connection.shutdown(socket.SHUT_RDWR)
            self.receipt(process, 0, 0)

    def test_extra_terminal_is_deliberately_third_command_result(self):
        with self.connect("stop_extra") as (connection, process):
            connection.sendall(peer.message(b"c", b""))
            self.terminal(connection, 3)
            self.receipt(process, 0, 0)

    def test_missing_terminals_waits_for_checked_client_close(self):
        with self.connect("stop_never") as (connection, process):
            connection.sendall(peer.message(b"c", b""))
            self.assertEqual(json.loads(process.stdout.readline()), {
                "copy_done_waiting": True, "positions_unchanged": True,
            })
            connection.settimeout(0.1)
            with self.assertRaises(socket.timeout):
                connection.recv(1)
            connection.shutdown(socket.SHUT_RDWR)
            self.receipt(process, 0, 0)

    def test_existing_stream_cases_accept_checked_copy_done(self):
        for case in ("follow", "blocked", "paused"):
            with self.subTest(case=case), self.connect(case) as (connection, process):
                self.assertEqual(self.read(connection)[0], b"d")
                if case == "follow":
                    for _ in range(6):
                        self.assertEqual(self.read(connection)[0], b"d")
                self.status(connection, True)
                connection.sendall(peer.message(b"c", b""))
                self.terminal(connection)
                self.receipt(process, 1, 1)


if __name__ == "__main__":
    unittest.main()
