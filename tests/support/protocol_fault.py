"""Relay real local PostgreSQL traffic and fault only an observed terminal message."""

import os
from pathlib import Path
import re
import select
import socket
import struct
import tempfile
import threading
import time


def error_sqlstate(payload):
    if not payload.endswith(b"\0\0"):
        raise ValueError("unterminated PostgreSQL error fields")
    codes = [field[1:] for field in payload.split(b"\0") if field.startswith(b"C")]
    if len(codes) != 1 or re.fullmatch(rb"[0-9A-Z]{5}", codes[0]) is None:
        raise ValueError("missing or invalid PostgreSQL SQLSTATE")
    return codes[0].decode("ascii")


def receive(sock, length):
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise EOFError
        data.extend(chunk)
    return bytes(data)


def packet(sock, typed=True):
    kind = receive(sock, 1) if typed else b""
    size = receive(sock, 4)
    length = struct.unpack("!I", size)[0]
    if not 4 <= length <= 64 * 1024 * 1024:
        raise ValueError("invalid fixture protocol frame size")
    payload = receive(sock, length - 4)
    return kind, kind + size + payload, payload


class ProtocolFault:
    def __init__(self, context, mode, *, copy_observer=False):
        self.context = context
        self.mode = mode
        self.hits = 0
        self.server_copy_bytes = 0
        self.client_copy_bytes = 0
        self.copy_frames = 0
        self.errors = []
        self.error_states = []
        self.sockets = []
        self.stopping = threading.Event()
        self.armed = threading.Event()
        self.release_requested = threading.Event()
        self.holding = threading.Event()
        self.backend_closed = threading.Event()
        self.backend_pid = None
        self.copy_observer = copy_observer or mode == "copy_observe"
        self.client_closed = threading.Event()
        self.copy_lock = threading.RLock()
        self.copy_started_at = None
        self.copy_deadline_ms = 20000
        self.copy_in_starts = 0
        self.copy_complete_tags = []
        self.commit_tags = []
        self.update_tags = []

    def arm(self):
        if self.copy_observer:
            if self.armed.is_set() or self.hits or self.client_closed.is_set() or self.backend_closed.is_set():
                raise ValueError("COPY observer cannot be armed in this state")
            self.copy_started_at = time.monotonic()
            self.armed.set()
            return
        if self.mode != "commit_hold" or self.hits:
            raise ValueError("COMMIT relay cannot be armed in this state")
        self.armed.set()

    def copy_window(self):
        return (self.copy_observer and self.armed.is_set()
                and not self.client_closed.is_set() and not self.backend_closed.is_set())

    def observe_copy_response(self, kind, payload):
        if not self.copy_window():
            return
        with self.copy_lock:
            if kind == b"G":
                if len(payload) < 3 or payload[0] not in (0, 1):
                    raise ValueError("invalid COPY input response")
                columns = struct.unpack("!H", payload[1:3])[0]
                if len(payload) != 3 + 2 * columns:
                    raise ValueError("invalid COPY input column formats")
                if any(struct.unpack_from("!H", payload, 3 + 2 * column)[0] not in (0, 1)
                       for column in range(columns)):
                    raise ValueError("invalid COPY input column format")
                self.copy_in_starts += 1
            elif kind == b"C":
                if payload.startswith(b"COPY"):
                    if re.fullmatch(rb"COPY (0|[1-9][0-9]*)\x00", payload) is None:
                        raise ValueError("invalid COPY completion tag")
                    self.copy_complete_tags.append(payload[:-1].decode("ascii"))
                elif payload == b"COMMIT\0":
                    self.commit_tags.append("COMMIT")
                elif payload.startswith(b"UPDATE"):
                    if re.fullmatch(rb"UPDATE (0|[1-9][0-9]*)\x00", payload) is None:
                        raise ValueError("invalid UPDATE completion tag")
                    self.update_tags.append(payload[:-1].decode("ascii"))

    def copy_observations(self):
        with self.copy_lock:
            elapsed = 0 if self.copy_started_at is None else int((time.monotonic() - self.copy_started_at) * 1000)
            return {"backend_pid": self.backend_pid, "armed": self.armed.is_set(),
                    "copy_in_starts": self.copy_in_starts, "copy_complete_tags": list(self.copy_complete_tags),
                    "error_sqlstates": list(self.error_states), "commit_tags": list(self.commit_tags),
                    "update_tags": list(self.update_tags), "fault_hits": self.hits,
                    "client_copy_bytes": self.client_copy_bytes, "client_closed": self.client_closed.is_set(),
                    "backend_closed": self.backend_closed.is_set(), "deadline_ms": self.copy_deadline_ms,
                    "elapsed_ms": elapsed, "helper_pid": os.getpid(),
                    "helper_reaped": False, "helper_returncode": None}

    def completed_copy_observations(self):
        value = self.copy_observations()
        assert value["armed"] and type(value["backend_pid"]) is int and value["backend_pid"] > 0, "missing armed COPY backend witness"
        assert value["client_closed"] and value["backend_closed"] and not self.errors, "COPY relay teardown is incomplete"
        assert value["elapsed_ms"] <= value["deadline_ms"], "COPY observation deadline exceeded"
        return value

    def release_backend(self):
        if not self.hits:
            raise ValueError("no held COMMIT backend to release")
        self.release_requested.set()
        if not self.backend_closed.wait(timeout=5):
            raise RuntimeError("held backend connection did not close")

    def __enter__(self):
        self.directory = Path(tempfile.mkdtemp(prefix="fault-", dir=self.context["socket_dir"]))
        self.directory.chmod(0o700)
        self.path = self.directory / f".s.PGSQL.{self.context['port']}"
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(self.path))
        self.path.chmod(0o700)
        self.listener.listen(1)
        self.listener.settimeout(0.2)
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        return self

    def upstream(self, client, server):
        try:
            _, frame, _ = packet(client, typed=False)
            server.sendall(frame)
            while not self.stopping.is_set():
                if self.copy_observer and not select.select([client], [], [], 0.02)[0]:
                    continue
                kind, frame, payload = packet(client)
                if kind == b"d" and (not self.copy_observer or self.copy_window()):
                    with self.copy_lock:
                        self.client_copy_bytes += len(payload)
                server.sendall(frame)
        except EOFError:
            if self.copy_observer:
                self.client_closed.set()
        except OSError:
            pass
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
            if self.copy_observer and not self.holding.is_set():
                try:
                    server.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

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
        self.sockets = [client, server]
        upstream = None
        try:
            client.settimeout(30)
            server.settimeout(30)
            server.connect(str(Path(self.context["socket_dir"]) / f".s.PGSQL.{self.context['port']}"))
            upstream = threading.Thread(target=self.upstream, args=(client, server), daemon=True)
            upstream.start()
            while not self.stopping.is_set():
                kind, frame, payload = packet(server)
                if kind == b"K":
                    if len(payload) != 8:
                        raise ValueError("invalid backend key frame")
                    self.backend_pid = struct.unpack("!I", payload[:4])[0]
                if kind == b"d" and (not self.copy_observer or self.copy_window()):
                    self.server_copy_bytes += len(payload)
                    self.copy_frames += 1
                self.observe_copy_response(kind, payload)
                if kind == b"E":
                    state = error_sqlstate(payload)
                    if not self.copy_observer or self.copy_window():
                        with self.copy_lock:
                            self.error_states.append(state)
                    if self.mode == "observe_unique" and state == "23505" and self.client_copy_bytes:
                        self.hits += 1
                if not self.hits and self.mode == "source_terminal" and kind == b"C" and payload.startswith(b"COPY ") and self.server_copy_bytes:
                    self.hits += 1
                    error = b"SERROR\0VERROR\0C57014\0Me1 injected terminal COPY failure\0\0"
                    frame = b"E" + struct.pack("!I", len(error) + 4) + error
                if not self.hits and self.mode == "commit_unknown" and kind == b"C" and payload == b"COMMIT\0" and self.client_copy_bytes:
                    self.hits += 1
                    return
                if not self.hits and self.mode == "commit_hold" and self.armed.is_set() and kind == b"C" and payload == b"COMMIT\0":
                    self.hits += 1
                    self.holding.set()
                    client.shutdown(socket.SHUT_RDWR)
                    if self.copy_observer:
                        self.stopping.set()
                        upstream.join(timeout=2)
                        assert not upstream.is_alive(), "COPY upstream did not stop before held client close"
                    client.close()
                    if self.copy_observer:
                        self.client_closed.set()
                    if not self.release_requested.wait(timeout=45):
                        self.errors.append("held backend release deadline exceeded")
                    return
                client.sendall(frame)
        except (EOFError, OSError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
            if self.copy_observer:
                self.stopping.set()
            for sock in self.sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
            if upstream:
                upstream.join(timeout=2)
                if self.copy_observer and upstream.is_alive():
                    self.errors.append("COPY upstream did not stop")
            self.backend_closed.set()

    def __exit__(self, *_):
        self.stopping.set()
        self.release_requested.set()
        self.listener.close()
        for sock in self.sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        self.thread.join(timeout=3)
        assert not self.thread.is_alive(), "protocol relay did not stop"
        assert not self.errors, f"protocol relay error: {self.errors}"
        os.unlink(self.path)
        self.directory.rmdir()
