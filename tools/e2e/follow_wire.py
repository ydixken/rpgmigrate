"""Private follow-wire observations and bounded SQL completion fault controls."""

import re
import select
import hashlib
import socket
import struct
import sys
from pathlib import Path
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests/support"))
from protocol_fault import packet


class CommitProtocol:
    MAP_LIMIT = 256
    NAME_LIMIT = 128
    PARAM_LIMIT = 512

    def __init__(self):
        self.statements, self.portals = {}, {}
        self.invalid = self.overflow = False
        self.syncs = 0
        self.pending_commits = []
        self.active = False
        self.tags = self.empty_tags()
        self.completed = None

    @staticmethod
    def empty_tags():
        return {"insert_0_1": 0, "update_1": 0, "delete_1": 0, "other_dml": 0}

    def fail(self, overflow=False):
        self.invalid = True
        self.overflow |= overflow
        raise ValueError("unsupported or invalid COMMIT protocol classification")

    def string(self, payload, offset, limit):
        end = payload.find(b'\0', offset, offset + limit + 1)
        if end < 0:
            self.fail()
        return payload[offset:end], end + 1

    def query(self, payload, offset):
        end = payload.find(b'\0', offset)
        if end < 0:
            self.fail()
        return payload[offset:end] if end - offset <= 160 else None, end + 1

    def name(self, payload, offset=0):
        value, offset = self.string(payload, offset, self.NAME_LIMIT)
        try:
            value.decode("utf-8")
        except UnicodeError:
            self.fail()
        return hashlib.sha256(value).digest() if value else b'', offset

    def short(self, payload, offset):
        if offset + 2 > len(payload):
            self.fail()
        return struct.unpack_from("!H", payload, offset)[0], offset + 2

    def formats(self, payload, offset):
        count, offset = self.short(payload, offset)
        if count > self.PARAM_LIMIT or offset + 2 * count > len(payload):
            self.fail(overflow=count > self.PARAM_LIMIT)
        if any(struct.unpack_from("!H", payload, offset + 2 * index)[0] not in (0, 1) for index in range(count)):
            self.fail()
        return count, offset + 2 * count

    def store(self, mapping, name, value):
        if name and name in mapping:
            self.fail()
        if name not in mapping and len(mapping) == self.MAP_LIMIT:
            self.fail(overflow=True)
        mapping[name] = value

    def invalidate_commits(self):
        for mapping in (self.statements, self.portals):
            for name, value in mapping.items():
                if value[0]:
                    mapping[name] = (None, None)

    def commit(self, protocol):
        if self.invalid or len(self.pending_commits) == 64:
            self.fail(overflow=len(self.pending_commits) == 64)
        self.pending_commits.append(protocol)
        return True

    def frontend(self, kind, payload):
        if kind == b'P':
            name, offset = self.name(payload)
            query, offset = self.query(payload, offset)
            count, offset = self.short(payload, offset)
            if count > self.PARAM_LIMIT or len(payload) != offset + 4 * count:
                self.fail(overflow=count > self.PARAM_LIMIT)
            commit = query == b'COMMIT'
            if commit and count:
                self.fail()
            deallocate = re.fullmatch(rb'DEALLOCATE "(fa_[0-9a-f]{16})"', query) if query is not None else None
            drop = hashlib.sha256(deallocate[1]).digest() if deallocate else None
            self.store(self.statements, name, (commit, drop))
        elif kind == b'B':
            portal, offset = self.name(payload)
            statement, offset = self.name(payload, offset)
            formats, offset = self.formats(payload, offset)
            count, offset = self.short(payload, offset)
            if count > self.PARAM_LIMIT or formats not in (0, 1, count):
                self.fail(overflow=count > self.PARAM_LIMIT)
            for _ in range(count):
                if offset + 4 > len(payload):
                    self.fail()
                length = struct.unpack_from("!i", payload, offset)[0]
                offset += 4
                if length < -1 or length > len(payload) - offset:
                    self.fail()
                offset += max(0, length)
            result_formats, offset = self.formats(payload, offset)
            if offset != len(payload) or statement not in self.statements:
                self.fail()
            classification = self.statements[statement]
            if classification[0] is None or classification[0] and (count or result_formats not in (0, 1)):
                self.fail()
            self.store(self.portals, portal, classification)
        elif kind == b'E':
            portal, offset = self.name(payload)
            if len(payload) != offset + 4 or portal not in self.portals:
                self.fail()
            commit, drop = self.portals[portal]
            if commit is None:
                self.fail()
            if commit:
                if struct.unpack_from("!I", payload, offset)[0] != 0:
                    self.fail()
                return self.commit("extended")
            if drop is not None:
                self.statements.pop(drop, None)
            # SQL outside the exact COMMIT request can change server-side named state.
            self.invalidate_commits()
        elif kind in (b'D', b'C'):
            if payload[:1] not in (b'S', b'P'):
                self.fail()
            name, offset = self.name(payload, 1)
            if offset != len(payload):
                self.fail()
            mapping = self.statements if payload[:1] == b'S' else self.portals
            if kind == b'D' and name not in mapping:
                self.fail()
            if kind == b'C':
                mapping.pop(name, None)
        elif kind == b'S':
            if payload or self.syncs == 64:
                self.fail(overflow=self.syncs == 64)
            self.syncs += 1
        elif kind == b'H':
            if payload:
                self.fail()
        elif kind == b'Q':
            query, offset = self.query(payload, 0)
            if offset != len(payload):
                self.fail()
            self.statements.pop(b'', None)
            self.portals.pop(b'', None)
            if query == b'COMMIT':
                return self.commit("simple")
            self.invalidate_commits()
        return False

    def backend(self, kind, payload):
        if kind == b'E':
            self.invalid = True
            self.statements.clear()
            self.portals.clear()
            self.pending_commits.clear()
        elif kind == b'Z':
            if payload not in (b'I', b'T', b'E'):
                self.fail()
            self.syncs = max(0, self.syncs - 1)
            if payload in (b'I', b'E'):
                self.portals.clear()
                self.active = False
                self.tags = self.empty_tags()
        elif kind == b'C':
            if payload == b'BEGIN\0':
                self.active, self.tags = True, self.empty_tags()
            elif payload == b'COMMIT\0':
                protocol = self.pending_commits.pop(0) if self.pending_commits else "backend"
                self.completed = self.active, dict(self.tags), protocol
                self.active, self.tags = False, self.empty_tags()
            elif payload == b'ROLLBACK\0':
                self.active, self.tags = False, self.empty_tags()
                self.pending_commits.clear()
            elif self.active:
                tag = {b'INSERT 0 1\0': "insert_0_1", b'UPDATE 1\0': "update_1", b'DELETE 1\0': "delete_1"}.get(payload)
                if tag is None and payload.startswith((b'INSERT ', b'UPDATE ', b'DELETE ', b'TRUNCATE')):
                    tag = "other_dml"
                if tag is not None:
                    if self.tags[tag] == 2**64 - 1:
                        self.fail(overflow=True)
                    self.tags[tag] += 1

    def observation(self, boundary, protocol):
        active, tags = self.active, dict(self.tags)
        if boundary == "after_commit" and self.completed is not None:
            active, tags, protocol = self.completed
        return {"begin_observed": active, "command_tags": tags, "protocol": protocol,
                "overflow": self.overflow, "classification_invalid": self.invalid}


class PgoutputObservation:
    # These are test observations; no value can construct production feedback authority.
    LIMIT = 256

    def __init__(self, connection):
        self.connection = connection
        self.logical = self.requested = self.confirmed = self.copy_active = False
        self.invalid = self.overflow = False
        self.transactions, self.keepalives, self.statuses = [], [], []
        self.active = None
        self.last_end = 0
        self.start_lsn = None
        self.commit_protocol = CommitProtocol()
        self.backend_pid, self.wire_mode = None, "ordinary"
        self.wire_unknown = self.wire_overflow = False
        self.stages = {side: {"stage": "startup_wait" if side == "client" else "connect_wait", "at": time.monotonic(), "closure": "open"} for side in ("client", "server")}
        self.copy_records, self.copy_pending, self.copy_current = [], None, None
        self.copy_invalid = self.copy_overflow = False
        self.counts = {side: {event: {} for event in ("received", "forwarded")} for side in ("client", "server")}

    def copy_frontend(self, kind, payload):
        if kind == b'Q':
            self.copy_pending = None
            if not payload.startswith(b'COPY '):
                return
            identifier = rb'"(?:[^"\x00]|"")+"'
            columns = identifier + rb'(?:, ' + identifier + rb')*'
            relation = rb'(' + identifier + rb')\.(' + identifier + rb')'
            bounds = rb"(?: WHERE ctid >= '\(([0-9]+),0\)'::pg_catalog.tid(?: AND ctid < '\(([0-9]+),0\)'::pg_catalog.tid)?)?"
            source = re.fullmatch(rb'COPY \(SELECT ' + columns + rb' FROM ONLY ' + relation + bounds + rb'\) TO STDOUT WITH \(FORMAT (text|binary)\)\x00', payload)
            target = re.fullmatch(rb'COPY ' + relation + rb' \(' + columns + rb'\) FROM STDIN WITH \(FORMAT (text|binary)(?:, FREEZE true)?\)\x00', payload)
            match = source or target
            if match is None or len(payload) > 65536 or self.copy_current is not None:
                self.copy_invalid = True
                return
            names = [value[1:-1].replace(b'""', b'"').decode("utf-8") for value in match.group(1, 2)]
            start, end = ((int(source[3]), int(source[4]) if source[4] else None) if source and source[3] else (None, None))
            self.copy_pending = {"relation": names, "direction": "out" if source else "in",
                                 "range": [start, end], "format": match[5 if source else 3].decode()}
        elif kind in (b'd', b'c') and self.copy_current is not None and self.copy_current["direction"] == "in":
            if kind == b'd':
                self.copy_current["data_frames"] += 1
            elif payload:
                self.copy_invalid = True
            else:
                self.copy_current["eof"] = True

    def copy_backend(self, kind, payload):
        if kind in (b'G', b'H'):
            pending = self.copy_pending
            self.copy_pending = None
            if pending is None or self.backend_pid is None or len(payload) < 3:
                self.copy_invalid = True
                return
            count = struct.unpack_from("!H", payload, 1)[0]
            expected = int(pending["format"] == "binary")
            if (payload[0] != expected or len(payload) != 3 + count * 2 or count > 1600
                    or any(struct.unpack_from("!H", payload, 3 + index * 2)[0] != expected for index in range(count))
                    or pending["direction"] != ("in" if kind == b'G' else "out")):
                self.copy_invalid = True
                return
            if len(self.copy_records) == self.LIMIT:
                self.copy_overflow = True
                return
            self.copy_current = {**pending, "connection": self.connection, "backend_pid": self.backend_pid,
                                 "columns": count, "rows": None, "data_frames": 0, "eof": False,
                                 "complete": False, "held": False, "released": False}
            self.copy_records.append(self.copy_current)
        elif self.copy_current is not None:
            current = self.copy_current
            if kind == b'd' and current["direction"] == "out":
                current["data_frames"] += 1
            elif kind == b'c' and current["direction"] == "out":
                self.copy_invalid |= bool(payload)
                current["eof"] = not payload
            elif kind == b'C':
                count = re.fullmatch(rb'COPY (0|[1-9][0-9]{0,18})\x00', payload)
                current["complete"] = current["eof"] and count is not None
                current["rows"] = int(count[1]) if count else None
                self.copy_invalid |= not current["complete"]
                self.copy_current = None
            elif kind in (b'E', b'Z'):
                self.copy_invalid = True
                self.copy_current = None

    def copy_evidence(self):
        return {"invalid": self.copy_invalid, "overflow": self.copy_overflow,
                "records": [dict(record) for record in self.copy_records]}

    def wire_event(self, side, stage, kind=None, payload=b""):
        state = self.stages[side]
        state.update(stage=stage, at=time.monotonic())
        if stage in ("received", "forwarded"):
            label = kind.decode("ascii") if kind in tuple(bytes([value]) for value in b"QPBDECSpd c fXRAKZN123tTGHWVsnI".replace(b" ", b"")) else "startup" if kind is None else "other"
            self.wire_unknown |= label == "other"
            counters = self.counts[side][stage]
            value = counters.get(label, 0)
            self.wire_overflow |= value == 2**64 - 1
            counters[label] = min(value + 1, 2**64 - 1)
        if side == "server" and stage == "received":
            self.copy_backend(kind, payload)
            if kind == b'K':
                if len(payload) == 8 and 0 < struct.unpack_from("!I", payload)[0] < 2**31:
                    self.backend_pid = struct.unpack_from("!I", payload)[0]
                else:
                    self.wire_unknown = True
            if kind in (b'G', b'H', b'W', b'Z'):
                self.wire_mode = {b'G': "copy_in", b'H': "copy_out", b'W': "logical" if self.copy_active else "unknown", b'Z': "ordinary"}[kind]
                self.wire_unknown |= self.wire_mode == "unknown"

    def wire_snapshot(self, now):
        return {"connection": self.connection, "backend_pid": self.backend_pid, "mode": self.wire_mode,
                "unknown": self.wire_unknown, "overflow": self.wire_overflow,
                "directions": {side: {"stage": state["stage"], "last_age_ms": max(0, int((now - state["at"]) * 1000)), "closure": state["closure"]} for side, state in self.stages.items()},
                "counts": {side: {event: dict(counts) for event, counts in events.items()} for side, events in self.counts.items()}}

    def startup(self, payload):
        if payload[:4] != struct.pack("!I", 196608) or not payload.endswith(b'\0\0'):
            return
        fields = payload[4:-1].split(b'\0')[:-1]
        if len(fields) % 2 or len(set(fields[::2])) != len(fields[::2]):
            return
        self.logical = dict(zip(fields[::2], fields[1::2])).get(b'replication') == b'database'

    def query(self, payload):
        # Admit only the engine's pgoutput-v1 request, never infer replication from COPY bytes.
        match = re.fullmatch(rb"START_REPLICATION SLOT \"[a-z0-9_]+\" LOGICAL ([0-9A-F]{1,8})/([0-9A-F]{1,8}) \(\"proto_version\" '1', \"publication_names\" '(?:[^'\x00]|'')+'\)\x00", payload)
        self.requested = self.logical and match is not None
        if self.requested:
            self.start_lsn = int(match[1], 16) * 2**32 + int(match[2], 16)

    def received(self, kind, payload):
        if kind == b'W' and self.requested:
            self.confirmed = self.copy_active = payload == b'\0\0\0'
            self.invalid |= not self.confirmed
            return None
        if kind == b'Z':
            self.copy_active = False
        if not self.copy_active or kind != b'd' or self.invalid:
            return None
        if payload[:1] == b'k':
            if len(payload) != 18 or payload[-1] not in (0, 1):
                self.invalid = True
                return None
            record = {"connection": self.connection, "lsn": struct.unpack_from("!Q", payload, 1)[0],
                      "idle": self.active is None, "forwarded": False}
            if len(self.keepalives) == self.LIMIT:
                self.overflow = True
                return None
            self.keepalives.append(record)
            return record, "forwarded"
        if len(payload) < 26 or payload[:1] != b'w':
            self.invalid = True
            return None
        message = payload[25:]
        tag = message[:1]
        if tag == b'B':
            if len(message) != 21 or self.active is not None:
                self.invalid = True
                return None
            final, timestamp, xid = struct.unpack("!QqI", message[1:])
            if not final or final < self.last_end or xid < 3 or not -211813488000000000 <= timestamp < 9223371331200000000:
                self.invalid = True
                return None
            record = {"connection": self.connection, "final_lsn": final, "xid": xid, "dml_count": 0,
                      "commit_lsn": None, "end_lsn": None, "complete": False,
                      "begin_forwarded": False, "commit_forwarded": False}
            self.active = record, timestamp
            if len(self.transactions) == self.LIMIT:
                self.overflow = True
            else:
                self.transactions.append(record)
            return record, "begin_forwarded"
        if tag == b'C':
            if len(message) != 26 or self.active is None:
                self.invalid = True
                return None
            flags, commit, end, timestamp = struct.unpack("!BQQq", message[1:])
            record, begin_timestamp = self.active
            if flags or commit != record["final_lsn"] or end < commit or end <= self.last_end or timestamp != begin_timestamp:
                self.invalid = True
                return None
            record.update(commit_lsn=commit, end_lsn=end, complete=True)
            self.last_end, self.active = end, None
            return record, "commit_forwarded"
        if tag in (b'I', b'U', b'D', b'T'):
            if self.active is None:
                self.invalid = True
            elif self.active[0]["dml_count"] == 2**64 - 1:
                self.overflow = True
            else:
                self.active[0]["dml_count"] += 1
        elif tag not in (b'R', b'Y', b'O'):
            self.invalid = True
        return None

    @staticmethod
    def forwarded(token):
        if token is not None:
            record, field = token
            record[field] = True

    def evidence(self):
        return {"connection": self.connection, "start_lsn": self.start_lsn,
                "confirmed": self.confirmed, "invalid": self.invalid, "overflow": self.overflow,
                "complete": not self.invalid and not self.overflow and self.active is None,
                "transactions": [dict(record) for record in self.transactions],
                "keepalives": [dict(record) for record in self.keepalives], "statuses": list(self.statuses)}


class WireGate:
    def __init__(self, host, mode):
        if mode not in ("observe", "before_commit", "after_commit", "after_begin", "later_begin_only", "completed_resume_close"):
            raise ValueError("unknown follow wire gate")
        self.host, self.mode = host, mode
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.stopping, self.armed, self.held, self.release = (threading.Event() for _ in range(4))
        self.lock = threading.Lock()
        self.abort_requested = threading.Event()
        self.sockets, self.threads, self.errors = [], [], []
        self.statuses = []
        self.observers, self.observer_overflow = [], False
        self.data_bytes = self.begins = self.commits = self.hits = 0
        self.dropped_keepalives = 0
        self.fault_record = self.fault_observer = None
        self.close_deadline = self.close_candidate = None
        self.copy_relation = None
        self.copy_held, self.copy_release = threading.Event(), threading.Event()
        self.thread = threading.Thread(target=self.accept, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def connection(self, connection):
        return connection.replace("host=" + self.host, "host=127.0.0.1 port=" + str(self.port))

    def hold_copy(self, relation):
        if self.mode != "observe" or self.copy_relation is not None or not isinstance(relation, list) or len(relation) != 2:
            raise RuntimeError("COPY hold needs one exact relation on an observation gate")
        self.copy_relation = list(relation)

    def copy_evidence(self):
        with self.lock:
            values = [observer.copy_evidence() for observer in self.observers]
            return {"invalid": bool(self.errors) or any(value["invalid"] for value in values),
                    "overflow": self.observer_overflow or any(value["overflow"] for value in values),
                    "records": [record for value in values for record in value["records"]]}

    def arm(self):
        if self.armed.is_set() or self.mode == "observe":
            raise RuntimeError("wire gate cannot be armed twice or without a fault")
        if self.mode == "completed_resume_close":
            self.close_deadline = time.monotonic() + 10
        self.armed.set()

    def hold(self, observation=None, boundary=None, protocol=None):
        with self.lock:
            if self.hits:
                return False
            self.hits += 1
            if observation is not None:
                self.fault_observer = observation
                self.fault_record = {"scope": "wire_fault_observation", "boundary": boundary,
                                     "connection": observation.connection, "backend_pid": observation.backend_pid,
                                     **observation.commit_protocol.observation(boundary, protocol)}
        self.held.set()
        deadline = time.monotonic() + 30 if self.mode == "later_begin_only" else None
        while not self.release.wait(0.1):
            if deadline is not None and time.monotonic() >= deadline:
                raise RuntimeError("controlled later-BEGIN gate deadline expired")
            if self.stopping.is_set():
                break
        return True

    def close_remaining(self):
        remaining = (self.close_deadline or time.monotonic() + 10) - time.monotonic()
        if remaining <= 0 or self.stopping.is_set():
            raise TimeoutError("completed-resume close fixture deadline expired")
        return remaining

    def close_packet(self, stream, observation, terminal=False, typed=True):
        def read(length):
            data = bytearray()
            while len(data) < length:
                stream.settimeout(self.close_remaining())
                chunk = stream.recv(length - len(data))
                if not chunk:
                    raise EOFError
                data.extend(chunk)
            return bytes(data)
        kind = read(1) if typed else b''
        size = read(4)
        length = struct.unpack("!I", size)[0]
        limit = 1024 if terminal or kind == b'C' and observation.close_commits == 1 else 64 * 1024 * 1024
        if length < 4 or length + len(kind) > limit:
            observation.commit_protocol.overflow |= length + len(kind) > limit
            observation.commit_protocol.fail()
        payload = read(length - 4)
        return kind, kind + size + payload, payload

    def close_backend(self, observation, kind, frame, payload):
        if not self.armed.is_set() or self.close_candidate not in (None, observation):
            return False
        protocol = observation.commit_protocol
        if observation.close_pending:
            if kind != b'Z' or payload != b'I':
                protocol.fail()
            observation.close_pending = False
            observation.close_commits += 1
            if observation.close_commits == 2:
                observation.close_terminal += frame
                if len(observation.close_terminal) > 1024:
                    protocol.fail(overflow=True)
                self.fault_record["terminal_idle"] = True
                return True
        elif kind == b'C' and payload == b'COMMIT\0':
            active, _, classification = protocol.completed
            if classification not in ("simple", "extended") or protocol.invalid or protocol.overflow:
                protocol.fail()
            observation.close_pending = True
            if observation.close_commits == 1:
                if not active or observation.backend_pid is None or observation.wire_unknown or self.observer_overflow:
                    protocol.fail()
                self.close_candidate = self.fault_observer = observation
                observation.close_terminal = frame
                self.fault_record = {
                    "scope": "completed_resume_close_fault", "connection": observation.connection,
                    "backend_pid": observation.backend_pid, "commit_ordinal": 2, "protocol": classification,
                    "begin_observed": active, "terminal_idle": False, "upstream_disconnected": False,
                    "terminal_delivered": False, "next_frontend_kind": None, "old_drop_terminate": None,
                    "overflow": False, "classification_invalid": False,
                }
        return False

    def close_frontend(self, observation, kind, payload):
        if self.close_candidate is not None and self.close_candidate is not observation:
            return False
        if self.close_candidate is not observation:
            observation.commit_protocol.frontend(kind, payload)
            return False
        self.close_remaining()
        if not self.fault_record["terminal_delivered"]:
            if kind == b'S' and not self.fault_record["terminal_idle"]:
                observation.commit_protocol.frontend(kind, payload)
                return False
            observation.commit_protocol.fail()
        if kind == b'X':
            if payload:
                observation.commit_protocol.fail()
        elif kind in (b'Q', b'P', b'B', b'E', b'D', b'C', b'S', b'H'):
            observation.commit_protocol.frontend(kind, payload)
        else:
            observation.commit_protocol.fail()
        self.fault_record.update(next_frontend_kind=kind.decode("ascii"), old_drop_terminate=kind == b'X')
        observation.close_next.set()
        return True

    def close_hold(self, client, server, observation):
        self.close_remaining()
        if select.select((server,), (), (), 0)[0]:
            raise ValueError("unexpected completed-resume terminal continuation")
        server.shutdown(socket.SHUT_RDWR)
        with self.lock:
            self.fault_record["upstream_disconnected"] = True
            self.hits += 1
        self.held.set()
        self.trace(observation, "server", "gate_wait")
        if not self.release.wait(self.close_remaining()):
            raise TimeoutError("completed-resume close release deadline expired")
        self.close_remaining()
        with self.lock:
            client.settimeout(self.close_remaining())
            client.sendall(observation.close_terminal)
            self.fault_record["terminal_delivered"] = True
            observation.close_terminal = b''
        if not observation.close_next.wait(self.close_remaining()):
            raise TimeoutError("completed-resume close request deadline expired")
        self.close_remaining()
        if self.fault_record["next_frontend_kind"] is None:
            raise ValueError("completed-resume close request witness missing")

    def abort_hold(self):
        with self.lock:
            if self.mode != "before_commit" or not self.held.is_set() or self.release.is_set():
                raise RuntimeError("wire gate has no abortable held COMMIT")
            self.abort_requested.set()
            self.release.set()

    def fault_observation(self):
        if not self.lock.acquire(timeout=0.02):
            scope = "completed_resume_close_fault" if self.mode == "completed_resume_close" else "wire_fault_observation"
            return {"status": "unavailable", "scope": scope}
        try:
            if self.mode == "completed_resume_close":
                if self.fault_record is None:
                    return {"status": "unavailable", "scope": "completed_resume_close_fault"}
                record = dict(self.fault_record)
                state = self.fault_observer.commit_protocol
                record["overflow"] |= self.observer_overflow or state.overflow
                record["classification_invalid"] |= state.invalid or bool(self.errors)
                pending = (not record["terminal_delivered"] and not self.release.is_set() and not self.stopping.is_set()
                           and self.close_deadline is not None and time.monotonic() < self.close_deadline)
                complete = record["terminal_delivered"] and record["next_frontend_kind"] is not None
                record["status"] = "observed" if (record["terminal_idle"] and record["upstream_disconnected"]
                    and (pending or complete) and not record["overflow"] and not record["classification_invalid"]) else "unavailable"
                return record
            if self.fault_record is None:
                return {"status": "unavailable", "scope": "wire_fault_observation"}
            record = {**self.fault_record, "command_tags": dict(self.fault_record["command_tags"])}
            state = self.fault_observer.commit_protocol
            record["overflow"] |= self.observer_overflow or state.overflow
            record["classification_invalid"] |= state.invalid or bool(self.errors)
            record["status"] = "observed" if (record["backend_pid"] is not None and record["begin_observed"]
                               and record["protocol"] in ("simple", "extended") and not record["overflow"]
                               and not record["classification_invalid"]) else "unavailable"
            return record
        finally:
            self.lock.release()

    def new_observation(self):
        with self.lock:
            observation = PgoutputObservation(len(self.observers) + 1)
            if self.mode == "completed_resume_close":
                observation.close_commits = 0
                observation.close_pending = False
                observation.close_terminal = b''
                observation.close_next = threading.Event()
            if len(self.observers) == 64:
                self.observer_overflow = True
            else:
                self.observers.append(observation)
            return observation

    def trace(self, observation, side, stage, kind=None, payload=b"", closure=None):
        if observation is not None:
            with self.lock:
                observation.wire_event(side, stage, kind, payload)
                if closure is not None and observation.stages[side]["closure"] == "open":
                    observation.stages[side]["closure"] = closure

    def snapshot(self, timeout=0.02):
        if not self.lock.acquire(timeout=max(0, min(timeout, 0.02))):
            return {"status": "unavailable", "reason": "lock_timeout"}
        try:
            return {"status": "observed", "overflow": self.observer_overflow,
                    "gates": {"armed": self.armed.is_set(), "held": self.held.is_set(), "released": self.release.is_set(), "abort_requested": self.abort_requested.is_set(), "stopping": self.stopping.is_set()},
                    "connections": [observer.wire_snapshot(time.monotonic()) for observer in self.observers]}
        finally:
            self.lock.release()

    def upstream(self, client, server, observation=None):
        try:
            self.trace(observation, "client", "startup_wait")
            _, frame, payload = self.close_packet(client, observation, typed=False) if self.mode == "completed_resume_close" else packet(client, typed=False)
            self.trace(observation, "client", "received")
            if observation is not None:
                with self.lock:
                    observation.startup(payload)
            self.trace(observation, "client", "forwarding")
            server.sendall(frame)
            self.trace(observation, "client", "forwarded")
            while not self.stopping.is_set():
                self.trace(observation, "client", "frame_wait")
                kind, frame, payload = self.close_packet(client, observation) if self.mode == "completed_resume_close" else packet(client)
                self.trace(observation, "client", "received", kind)
                status = commit = False
                if observation is not None:
                    with self.lock:
                        observation.copy_frontend(kind, payload)
                        if kind == b'Q':
                            observation.query(payload)
                        status = observation.copy_active and kind == b'd' and payload[:1] == b'r'
                        if self.mode == "completed_resume_close" and self.close_frontend(observation, kind, payload):
                            return
                        if self.mode in ("before_commit", "after_commit"):
                            commit = observation.commit_protocol.frontend(kind, payload)
                if status:
                    if len(payload) != 34:
                        raise ValueError("invalid standby status frame")
                if self.armed.is_set() and self.mode == "before_commit" and commit:
                    self.trace(observation, "client", "gate_wait")
                    held = self.hold(observation, "before_commit", "simple" if kind == b'Q' else "extended")
                    with self.lock:
                        if held and self.abort_requested.is_set():
                            return
                self.trace(observation, "client", "forwarding")
                server.sendall(frame)
                self.trace(observation, "client", "forwarded", kind)
                if status:
                    with self.lock:
                        positions = struct.unpack("!QQQ", payload[1:25])
                        self.statuses.append(positions)
                        if observation is not None and observation.confirmed:
                            observation.statuses.append(positions)
                        if len(self.statuses) > 10000:
                            raise ValueError("standby status observation bound exceeded")
        except (EOFError, OSError) as error:
            if self.mode == "completed_resume_close" and isinstance(error, TimeoutError) and not self.stopping.is_set():
                self.errors.append(type(error).__name__)
            self.trace(observation, "client", "closed", closure="eof" if isinstance(error, EOFError) else "socket_error")
        except Exception as error:
            self.errors.append(type(error).__name__)
            self.trace(observation, "client", "closed", closure="protocol_error")
        finally:
            if self.mode == "completed_resume_close":
                observation.close_next.set()
            self.trace(observation, "client", "closed", closure="proxy_shutdown")
            try:
                server.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def downstream(self, client, server, observation=None):
        observation = self.new_observation() if observation is None else observation
        upstream = threading.Thread(target=self.upstream, args=(client, server, observation), daemon=True)
        self.threads.append(upstream)
        upstream.start()
        gate_next = False
        try:
            while not self.stopping.is_set():
                self.trace(observation, "server", "frame_wait")
                if self.mode != "completed_resume_close" and not server.recv(1, socket.MSG_PEEK):
                    raise EOFError
                try:
                    kind, frame, payload = self.close_packet(server, observation, terminal=bool(observation.close_terminal)) if self.mode == "completed_resume_close" else packet(server)
                except (EOFError, OSError, ValueError):
                    with self.lock:
                        observation.invalid |= observation.copy_active
                    raise
                with self.lock:
                    token = observation.received(kind, payload)
                    observation.wire_event("server", "received", kind, payload)
                    if self.mode in ("before_commit", "after_commit", "completed_resume_close"):
                        observation.commit_protocol.backend(kind, payload)
                    close_ready = self.close_backend(observation, kind, frame, payload) if self.mode == "completed_resume_close" else False
                    logical_wal = observation.copy_active and kind == b'd' and payload[:1] == b'w'
                if kind == b'H' and self.copy_relation is not None:
                    with self.lock:
                        current = observation.copy_current
                        hold_copy = current is not None and current["relation"] == self.copy_relation
                        if hold_copy:
                            current["held"] = True
                            self.copy_held.set()
                    if hold_copy:
                        deadline = time.monotonic() + 60
                        while not self.copy_release.wait(0.1):
                            if self.stopping.is_set() or time.monotonic() >= deadline:
                                raise RuntimeError("COPY observation hold expired before release")
                        with self.lock:
                            current["released"] = True
                if close_ready:
                    self.close_hold(client, server, observation)
                    return
                if self.mode == "completed_resume_close" and observation.close_terminal:
                    continue
                if self.mode == "later_begin_only" and observation.copy_active:
                    if not self.armed.is_set() or observation.invalid or observation.overflow:
                        raise RuntimeError("controlled later-BEGIN fixture was not armed before logical startup")
                    if kind == b'd' and payload[:1] == b'k':
                        with self.lock:
                            self.dropped_keepalives += 1
                            if self.dropped_keepalives > PgoutputObservation.LIMIT:
                                raise RuntimeError("controlled keepalive loss exceeded its observation bound")
                        continue
                    if logical_wal and payload[25:26] == b'B':
                        if self.hits:
                            raise RuntimeError("controlled later-BEGIN fixture received an unexpected extra transaction")
                        self.trace(observation, "server", "gate_wait")
                        self.hold()
                if gate_next:
                    self.trace(observation, "server", "gate_wait")
                    self.hold()
                    gate_next = False
                if logical_wal:
                    if len(payload) < 26:
                        raise ValueError("invalid logical WAL frame")
                    with self.lock:
                        self.data_bytes += len(payload) - 25
                        self.begins += payload[25:26] == b'B'
                        self.commits += payload[25:26] == b'C'
                    gate_next = self.armed.is_set() and self.mode == "after_begin" and payload[25:26] == b'B'
                if self.armed.is_set() and self.mode == "after_commit" and kind == b'C' and payload == b'COMMIT\0':
                    self.trace(observation, "server", "gate_wait")
                    self.hold(observation, "after_commit", "backend")
                self.trace(observation, "server", "forwarding")
                client.sendall(frame)
                self.trace(observation, "server", "forwarded", kind)
                with self.lock:
                    observation.forwarded(token)
        except (EOFError, OSError) as error:
            if self.mode == "completed_resume_close" and isinstance(error, TimeoutError) and not self.stopping.is_set():
                self.errors.append(type(error).__name__)
            self.trace(observation, "server", "closed", closure="eof" if isinstance(error, EOFError) else "socket_error")
        except Exception as error:
            self.errors.append(type(error).__name__)
            self.trace(observation, "server", "closed", closure="protocol_error")
        finally:
            self.trace(observation, "server", "closed", closure="proxy_shutdown")
            for stream in (client, server):
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                if self.mode != "completed_resume_close":
                    stream.close()
            if self.mode == "completed_resume_close":
                # Shutdown wakes the reader; closing its descriptor first can strand it in poll.
                upstream.join(timeout=3)
                if upstream.is_alive():
                    self.errors.append("upstream did not stop")
                for stream in (client, server):
                    stream.close()

    def accept(self):
        while not self.stopping.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            observation = self.new_observation()
            server = None
            try:
                server = socket.create_connection((self.host, 5432), timeout=10)
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                server.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                client.settimeout(None)
                server.settimeout(None)
            except OSError:
                client.close()
                if server is not None:
                    server.close()
                self.errors.append("upstream connection")
                self.trace(observation, "server", "closed", closure="connect_error")
                continue
            self.sockets.extend((client, server))
            worker = threading.Thread(target=self.downstream, args=(client, server, observation), daemon=True)
            self.threads.append(worker)
            worker.start()

    def evidence(self, timeout=None):
        acquired = self.lock.acquire() if timeout is None else self.lock.acquire(timeout=max(0, min(timeout, 0.02)))
        if not acquired:
            raise RuntimeError("follow wire observer lock unavailable")
        try:
            if self.errors:
                raise RuntimeError("follow wire observer failed")
            return {"hits": self.hits, "data_bytes": self.data_bytes, "begins": self.begins,
                    "commits": self.commits, "statuses": list(self.statuses),
                    **({"dropped_keepalives": self.dropped_keepalives} if self.mode == "later_begin_only" else {}),
                    "logical": {"overflow": self.observer_overflow,
                                "streams": [observer.evidence() for observer in self.observers if observer.start_lsn is not None]}}
        finally:
            self.lock.release()

    def __exit__(self, *_args):
        try:
            self.stopping.set()
            self.release.set()
            self.copy_release.set()
            self.listener.close()
            if self.mode == "completed_resume_close":
                for observation in self.observers:
                    observation.close_next.set()
            for stream in self.sockets:
                try:
                    stream.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                if self.mode != "completed_resume_close":
                    stream.close()
            cleanup_deadline = time.monotonic() + 3
            self.thread.join(timeout=3)
            for worker in self.threads:
                timeout = max(0, cleanup_deadline - time.monotonic()) if self.mode == "completed_resume_close" else 3
                worker.join(timeout=timeout)
            if self.mode == "completed_resume_close":
                for stream in self.sockets:
                    stream.close()
            if self.thread.is_alive() or any(worker.is_alive() for worker in self.threads):
                raise RuntimeError("follow wire observer did not stop")
        except Exception as error:
            if self.mode == "completed_resume_close":
                self.errors.append(type(error).__name__)
            raise
