"""Relay real local PostgreSQL traffic and fault only an observed terminal message."""

import os
from pathlib import Path
import re
import socket
import struct
import tempfile
import threading


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
    def __init__(self, context, mode):
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
                kind, frame, payload = packet(client)
                if kind == b"d":
                    self.client_copy_bytes += len(payload)
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
                if kind == b"d":
                    self.server_copy_bytes += len(payload)
                    self.copy_frames += 1
                if kind == b"E":
                    state = error_sqlstate(payload)
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
                client.sendall(frame)
        except (EOFError, OSError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
            for sock in self.sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()
            if upstream:
                upstream.join(timeout=2)

    def __exit__(self, *_):
        self.stopping.set()
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
