"""Private follow-wire observations and bounded SQL completion fault controls."""

import re
import select
import hashlib
import json
import socket
import struct
import sys
from pathlib import Path
import threading
import time
import zlib

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


class AckOracle:
    COLUMNS = tuple(zip(
        "magic version run_id nonce source_system source_database generation target_system target_database owner_oid schema_oid table_oid applied_order last_xid begin_ordinal source_commit_lsn source_end_lsn barrier_epoch barrier_counter copy_snapshot_token copy_consistent_point copy_plan_nonce clone_state certified_lsn follow_binding drain_end_lsn drain_witness_lsn drain_kind follow_completed".split(),
        "text int4 bytea bytea numeric oid numeric numeric oid oid oid oid numeric oid numeric pg_lsn pg_lsn numeric numeric text pg_lsn bytea text pg_lsn text pg_lsn pg_lsn text bool".split()))
    LIMIT = 256

    def __init__(self, profile):
        if profile != "m9":
            raise ValueError("independent ACK oracle requires M9")
        self.lock = threading.RLock()
        self.ordinal = 0
        self.bound = self.invalid = self.overflow = False
        self.connections = {"source": 0, "target": 0}
        self.targets, self.transactions, self.keepalives, self.proofs, self.statuses, self.commits = {}, [], [], [], [], []
        self.head = self.table_hash = self.guard_hash = None
        self.target_transactions = 0

    def step(self):
        if self.ordinal == 2**64 - 1:
            self.refuse(True)
        self.ordinal += 1
        return self.ordinal

    def refuse(self, overflow=False):
        self.invalid = True
        self.overflow |= overflow
        raise ValueError("incomplete or invalid independent ACK observation")

    @classmethod
    def metadata_sql(cls, table):
        assignments = ", ".join(f"{name} = ${index + 1}::pg_catalog.{kind}" for index, (name, kind) in enumerate(cls.COLUMNS) if index >= 12)
        identity = [f"{name} IS NOT DISTINCT FROM ${index + 1}::pg_catalog.{kind}" for index, (name, kind) in enumerate(cls.COLUMNS[:12])]
        predecessor = [f"{name} IS NOT DISTINCT FROM ${index + 30}::pg_catalog.{kind}" for index, (name, kind) in enumerate(cls.COLUMNS[12:])]
        return f"UPDATE ONLY {table} SET {assignments} WHERE " + " AND ".join(identity + predecessor)

    @staticmethod
    def guard_sql(run):
        slot = zlib.crc32(bytes.fromhex(run[2:]))
        return ("SELECT pg_catalog.pg_backend_pid() = $1::integer\n"
                "            AND (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()) = $2::oid\n"
                "            AND (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = CURRENT_USER) = $3::oid\n"
                "            AND NOT pg_catalog.pg_is_in_recovery()\n"
                "            AND pg_catalog.current_setting('transaction_read_only') = 'off'\n"
                "            AND pg_catalog.current_setting('fsync') = 'on'\n"
                "            AND pg_catalog.current_setting('full_page_writes') = 'on'\n"
                "            AND pg_catalog.current_setting('search_path') = 'pg_catalog'\n"
                "            AND EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype = 'advisory'\n"
                f"                AND database = $2::oid AND pid = $1::integer AND classid = {0x5250474d}::oid\n"
                f"                AND objid = {slot}::oid AND objsubid = 2 AND mode = 'ExclusiveLock' AND granted)")

    def project(self, values):
        if len(values) != 29:
            self.refuse()
        result = []
        for (name, kind), value in zip(self.COLUMNS, values):
            if value is None:
                if name not in {"target_system", "last_xid", "begin_ordinal", "source_commit_lsn", "source_end_lsn", "copy_snapshot_token", "copy_consistent_point", "copy_plan_nonce", "clone_state", "certified_lsn", "follow_binding", "drain_end_lsn", "drain_witness_lsn", "drain_kind"}:
                    self.refuse()
                result.append(None)
            elif kind in ("numeric", "oid", "int4"):
                if not re.fullmatch(rb"0|[1-9][0-9]{0,19}", value):
                    self.refuse()
                number = int(value)
                minimum = 0 if name in ("applied_order", "barrier_epoch") else 1
                if not minimum <= number < (2**32 if kind in ("oid", "int4") else 2**64):
                    self.refuse()
                result.append(number)
            elif kind == "pg_lsn":
                if not re.fullmatch(rb"[0-9A-F]{1,8}/[0-9A-F]{1,8}", value):
                    self.refuse()
                high, low = value.split(b'/')
                number = (int(high, 16) << 32) + int(low, 16)
                if not number:
                    self.refuse()
                result.append(number)
            elif name == "follow_completed":
                if value not in (b't', b'f'):
                    self.refuse()
                result.append(value == b't')
            elif name == "drain_kind":
                if value not in (b'keepalive', b'later_begin'):
                    self.refuse()
                result.append(value.decode())
            else:
                if len(value) > (262144 if name == "follow_binding" else 4096) or b'\0' in value:
                    self.refuse()
                if kind == "bytea" and (not re.fullmatch(rb"\\x[0-9a-f]{32}", value) or value == b'\\x' + b'00' * 16):
                    self.refuse()
                try:
                    value.decode("utf-8")
                except UnicodeError:
                    self.refuse()
                if name == "magic" and value != b'rpgmigrate':
                    self.refuse()
                result.append(hashlib.sha256(value).hexdigest())
        row = dict(zip((name for name, _ in self.COLUMNS), result))
        if row["version"] != 7:
            self.refuse()
        applied = [row[key] for key in ("last_xid", "begin_ordinal", "source_commit_lsn", "source_end_lsn")]
        if (row["applied_order"] == 0 and any(value is not None for value in applied)
                or row["applied_order"] > 0 and (any(value is None for value in applied) or not 3 <= row["last_xid"] < 2**32
                or row["begin_ordinal"] != row["source_commit_lsn"] or row["source_end_lsn"] < row["source_commit_lsn"])):
            self.refuse()
        anchor = [row[key] for key in ("copy_snapshot_token", "copy_consistent_point", "copy_plan_nonce")]
        if any(value is not None for value in anchor):
            if any(value is None for value in anchor) or values[22] is None or values[24] is None:
                self.refuse()
            try:
                follow, clone = json.loads(values[24])["binding"], json.loads(values[22])
                slot = follow["slot"]
                if (values[19].decode() != slot["snapshot_token"] or row["copy_consistent_point"] != slot["consistent_point"]
                        or values[21] != ("\\x" + bytes(slot["intent"]["nonce"]).hex()).encode()
                        or clone["phase"] != "analyzing" or clone["generation"] != follow["copy_generation"]
                        or clone["source_timeline"] != follow["source_timeline"] or clone["snapshot_token"] != slot["snapshot_token"]):
                    self.refuse()
            except (KeyError, TypeError, ValueError, UnicodeError):
                self.refuse()
        elif row["applied_order"] or row["certified_lsn"] is not None:
            self.refuse()
        drain = [row[key] for key in ("drain_end_lsn", "drain_witness_lsn", "drain_kind")]
        if any(value is not None for value in drain):
            if (any(value is None for value in drain) or row["copy_consistent_point"] is None
                    or row["drain_witness_lsn"] < row["drain_end_lsn"] or row["copy_consistent_point"] > row["drain_end_lsn"]
                    or row["drain_kind"] == "later_begin" and row["drain_witness_lsn"] == row["drain_end_lsn"]
                    or row["drain_kind"] == "keepalive" and (row["certified_lsn"] or 0) < row["drain_witness_lsn"]):
                self.refuse()
        elif row["follow_completed"]:
            self.refuse()
        return tuple(result)

    def bind(self, owned, settings):
        from follow import validate_settings
        with self.lock:
            self.step()
            if self.bound:
                self.refuse()
            validate_settings(settings)
            table = owned["table"]
            if not isinstance(table, str) or not re.fullmatch(r'"(?:[^"\x00]|"")+"\."(?:[^"\x00]|"")+"', table) or len(table.encode()) > 512:
                self.refuse()
            values = [None if owned[name] is None else ("t" if owned[name] is True else "f" if owned[name] is False else str(owned[name])).encode() for name, _ in self.COLUMNS]
            self.head = self.project(values)
            self.table_hash = hashlib.sha256(table.encode()).digest()
            self.guard_hash = hashlib.sha256(self.guard_sql(owned["run_id"]).encode()).digest()
            self.bound = True

    def connection(self, side):
        with self.lock:
            self.step()
            if side not in self.connections or self.connections[side] == 64:
                self.refuse(True)
            self.connections[side] += 1
            connection = self.connections[side]
            if side == "target":
                self.targets[connection] = AckTargetProtocol(self, connection)
            return connection

    def target_frontend(self, connection, kind, payload):
        with self.lock:
            self.step()
            return self.targets[connection].frontend(kind, payload)

    def target_backend(self, connection, kind, payload):
        with self.lock:
            self.step()
            return self.targets[connection].backend(kind, payload)

    def synchronous_request(self, connection, token):
        with self.lock:
            target = self.targets[connection]
            return (self.bound and not self.invalid and not self.overflow and token is not None and token["kind"] == "commit" and target.active
                    and target.next is not None and target.checked in ("on", "remote_apply")
                    and not target.local_off and not target.warned and target.warning_visibility)

    def forwarded(self, token):
        if token is None:
            return
        with self.lock:
            ordinal = self.step()
            if token["forwarded"]:
                self.refuse()
            token["forwarded"] = True
            if token.get("event") == "ready":
                self.targets[token["connection"]].publish(token["terminal"], ordinal)

    def source_transaction(self, tx):
        with self.lock:
            self.step()
            if len(self.transactions) == self.LIMIT:
                self.refuse(True)
            if tx.get("complete") is not True or tx.get("begin_forwarded") is not True or tx.get("commit_forwarded") is not True:
                self.refuse()
            if (any(type(tx.get(key)) is not int for key in ("connection", "final_lsn", "commit_lsn", "end_lsn", "xid"))
                    or not 0 < tx["connection"] <= self.connections["source"] or not 3 <= tx["xid"] < 2**32
                    or not 0 < tx["final_lsn"] == tx["commit_lsn"] <= tx["end_lsn"] < 2**64
                    or any(old["connection"] == tx["connection"] and old["end_lsn"] >= tx["end_lsn"] for old in self.transactions)):
                self.refuse()
            self.transactions.append({key: tx[key] for key in ("connection", "final_lsn", "commit_lsn", "end_lsn", "xid")})

    def source_keepalive(self, record):
        with self.lock:
            self.step()
            if len(self.keepalives) == self.LIMIT:
                self.refuse(True)
            if (type(record.get("connection")) is not int or not 0 < record["connection"] <= self.connections["source"]
                    or type(record.get("lsn")) is not int or not 0 < record["lsn"] < 2**64
                    or type(record.get("idle")) is not bool or record.get("forwarded") is not True):
                self.refuse()
            self.keepalives.append({key: record[key] for key in ("connection", "lsn", "idle", "forwarded")})

    def source_status(self, connection, positions):
        with self.lock:
            ordinal = self.step()
            if len(self.statuses) == 10000:
                self.refuse(True)
            if len(positions) != 3 or any(type(value) is not int or not 0 <= value < 2**64 for value in positions):
                self.refuse()
            if type(connection) is not int or not 0 < connection <= self.connections["source"]:
                self.refuse()
            proof = self.proofs[-1] if self.proofs else None
            prefix = proof["prefix"] if proof else 0
            previous = next((record["positions"][1] for record in reversed(self.statuses) if record["connection"] == connection), 0)
            record = {"ordinal": ordinal, "connection": connection, "positions": tuple(positions), "prefix": prefix,
                      "proof_ordinal": proof["ordinal"] if proof else None,
                      "safe": positions[0] == positions[1] == positions[2] and previous <= positions[1] <= prefix,
                      "forwarded": False}
            self.statuses.append(record)
            return record

    def probe_status(self, connection, positions):
        with self.lock:
            probe = AckOracle("m9")
            probe.ordinal, probe.bound, probe.invalid, probe.overflow = self.ordinal, self.bound, self.invalid, self.overflow
            probe.connections = dict(self.connections)
            probe.proofs = [dict(record) for record in self.proofs]
            probe.statuses = [dict(record) for record in self.statuses]
            probe.forwarded(probe.source_status(connection, positions))
            evidence = probe.evidence()
            return {"statuses": [tuple(record["positions"]) for record in evidence["statuses"] if record["forwarded"]], "ack_oracle": evidence}

    def close(self, side, connection, *, incomplete=False):
        with self.lock:
            self.step()
            self.invalid |= incomplete
            if side == "target":
                target = self.targets[connection]
                self.invalid |= target.active or target.terminal is not None or bool(target.expected) or target.delivery is not None

    def evidence(self):
        with self.lock:
            return {"profile": "m9", "bound": self.bound, "invalid": self.invalid, "overflow": self.overflow,
                    "ordinal": self.ordinal, "proofs": [dict(record) for record in self.proofs],
                    "commits": [dict(record) for record in self.commits],
                    "statuses": [{**record, "positions": tuple(record["positions"])} for record in self.statuses]}


class AckTargetProtocol(CommitProtocol):
    def __init__(self, oracle, connection):
        super().__init__()
        self.oracle, self.connection = oracle, connection
        self.expected, self.pid, self.terminal = [], None, None
        self.delivery = None
        self.initial_ready = False
        self.next = self.checked = self.setting = None
        self.local_off = self.warned = self.warning_visibility = False

    def fail(self, overflow=False):
        self.invalid = True
        self.oracle.refuse(overflow)

    def classify(self, sql):
        exact = {b'BEGIN': "begin", b'BEGIN ISOLATION LEVEL READ COMMITTED': "begin", b'COMMIT': "commit", b'ROLLBACK': "rollback",
                 b'SHOW synchronous_commit': "show", b'SET LOCAL synchronous_commit = off': "off",
                 b'SET LOCAL client_min_messages = warning': "warning_guard"}
        if sql in exact:
            return exact[sql], None
        setting = re.fullmatch(rb"SET synchronous_commit TO '(on|remote_apply|off|local|remote_write)'", sql)
        if setting:
            return "setting", setting[1].decode()
        table = re.match(rb'UPDATE ONLY ("(?:[^"\x00]|"")+"\."(?:[^"\x00]|"")+") SET ', sql)
        if table:
            try:
                if sql == self.oracle.metadata_sql(table[1].decode()).encode():
                    return "metadata", hashlib.sha256(table[1]).digest()
            except UnicodeError:
                self.fail()
        return "other", hashlib.sha256(sql).digest()

    def queue(self, value):
        if len(self.expected) == 4096:
            self.fail(True)
        self.expected.append(value)

    def operation(self, classification, values):
        kind, detail = classification
        if kind == "other" and detail == self.oracle.guard_hash:
            kind = "guard"
        op = {"kind": kind, "detail": detail, "values": values, "forwarded": False, "rows": 0, "value": None, "description": None, "columns": None}
        if self.active and kind not in ("commit", "show", "setting", "off", "warning_guard", "guard"):
            self.checked = None
        self.queue(op)
        return op

    def frontend(self, kind, payload):
        if kind == b'P':
            name, offset = self.name(payload)
            sql, offset = self.string(payload, offset, 65536)
            count, offset = self.short(payload, offset)
            if count > self.PARAM_LIMIT or len(payload) != offset + count * 4:
                self.fail(count > self.PARAM_LIMIT)
            self.store(self.statements, name, self.classify(sql))
            self.queue(b'1')
        elif kind == b'B':
            portal, offset = self.name(payload)
            statement, offset = self.name(payload, offset)
            count, offset = self.short(payload, offset)
            if count > self.PARAM_LIMIT or offset + count * 2 > len(payload):
                self.fail(count > self.PARAM_LIMIT)
            formats = [struct.unpack_from("!H", payload, offset + 2 * index)[0] for index in range(count)]
            offset += count * 2
            number, offset = self.short(payload, offset)
            if number > self.PARAM_LIMIT or count not in (0, 1, number) or any(value not in (0, 1) for value in formats):
                self.fail(number > self.PARAM_LIMIT)
            values = []
            for _ in range(number):
                if offset + 4 > len(payload):
                    self.fail()
                length = struct.unpack_from("!i", payload, offset)[0]
                offset += 4
                if length < -1 or length > len(payload) - offset:
                    self.fail()
                values.append(None if length == -1 else payload[offset:offset + length])
                offset += max(0, length)
            results, offset = self.formats(payload, offset)
            if offset != len(payload) or statement not in self.statements:
                self.fail()
            classification = self.statements[statement]
            if classification[0] == "metadata":
                if number != 46 or any(formats) or results not in (0, 1):
                    self.fail()
                values = (self.oracle.project(values[:29]), self.oracle.project(values[:12] + values[29:]))
            elif classification[0] == "other" and classification[1] == self.oracle.guard_hash:
                if number != 3 or any(formats):
                    self.fail()
                values = tuple(hashlib.sha256(value).digest() if value is not None else None for value in values)
            else:
                if classification[0] != "other" and number:
                    self.fail()
                values = None
            self.store(self.portals, portal, (classification, values))
            self.queue(b'2')
        elif kind == b'E':
            portal, offset = self.name(payload)
            if len(payload) != offset + 4 or portal not in self.portals or struct.unpack_from("!I", payload, offset)[0]:
                self.fail()
            return self.operation(*self.portals[portal])
        elif kind in (b'D', b'C'):
            if payload[:1] not in (b'S', b'P'):
                self.fail()
            name, offset = self.name(payload, 1)
            mapping = self.statements if payload[:1] == b'S' else self.portals
            if offset != len(payload) or name not in mapping:
                self.fail()
            if kind == b'C':
                mapping.pop(name)
                self.queue(b'3')
            else:
                if payload[:1] == b'S':
                    self.queue(b't')
                self.queue(b'description')
        elif kind == b'Q':
            sql, offset = self.string(payload, 0, 65536)
            if offset != len(payload):
                self.fail()
            self.statements.pop(b'', None)
            self.portals.pop(b'', None)
            op = self.operation(self.classify(sql), None)
            op["simple"] = True
            self.queue(b'Z')
            return op
        elif kind == b'S':
            if payload:
                self.fail()
            self.queue(b'Z')
        elif kind == b'H':
            if payload:
                self.fail()
        elif kind not in (b'p', b'd', b'c', b'f', b'X'):
            self.fail()
        return None

    def command(self, op, tag):
        kind = op["kind"]
        if not op["forwarded"]:
            self.fail()
        if kind == "begin":
            if tag != b'BEGIN\0' or self.active:
                self.fail()
            self.active = True
            self.next = self.checked = self.setting = None
            self.local_off = self.warned = self.warning_visibility = False
        elif kind == "metadata":
            if not self.active or tag != b'UPDATE 1\0' or op["values"] is None:
                self.fail()
            next_row, old = op["values"]
            if self.oracle.bound:
                previous = self.next if self.next is not None else self.oracle.head
                if op["detail"] != self.oracle.table_hash or next_row[:12] != previous[:12] or old != previous or next_row[24] != previous[24] or next_row[18] != previous[18] + 1 or next_row[17] < previous[17]:
                    self.fail()
                if next_row[12] < old[12] or (next_row[16] or 0) < (old[16] or 0) or (next_row[23] or 0) < (old[23] or 0):
                    self.fail()
                if (any(value is not None for value in old[19:22]) and next_row[19:22] != old[19:22]
                        or old[25] is not None and next_row[25:28] != old[25:28] or old[28] and next_row != old):
                    self.fail()
                if next_row[12] > old[12]:
                    covered = [tx for tx in self.oracle.transactions if (old[16] or 0) < tx["end_lsn"] <= next_row[16]]
                    if (len(covered) != next_row[12] - old[12] or not covered or (covered[-1]["xid"], covered[-1]["final_lsn"], covered[-1]["commit_lsn"], covered[-1]["end_lsn"]) != next_row[13:17]):
                        self.fail()
                elif next_row[12:17] != old[12:17]:
                    self.fail()
                if (next_row[23] or 0) > (old[23] or 0) and not any(record["idle"] and record["forwarded"] and record["lsn"] >= next_row[23] for record in self.oracle.keepalives):
                    self.fail()
                self.next = next_row
        elif kind in ("setting", "off", "warning_guard"):
            if tag != b'SET\0':
                self.fail()
            if kind == "setting":
                self.setting, self.checked = op["detail"], None
            elif kind == "off":
                if not self.active:
                    self.fail()
                self.local_off, self.checked = True, None
            else:
                self.warning_visibility = self.active
        elif kind == "show":
            if tag != b'SHOW\0' or op["rows"] != 1 or op["description"] != (b'synchronous_commit', 25, 0) or op["value"] not in (b'on', b'remote_apply', b'off', b'local', b'remote_write'):
                self.fail()
            self.checked = op["value"].decode() if self.active and op["value"].decode() == self.setting and not self.local_off else None
        elif kind == "guard":
            expected = (self.pid, self.oracle.head[8], self.oracle.head[9]) if self.oracle.bound else ()
            wanted = tuple(hashlib.sha256(str(value).encode()).digest() for value in expected)
            if tag != b'SELECT 1\0' or op["values"] != wanted or op["rows"] != 1 or op["value"] != b't' or op["description"] != (b'?column?', 16, 0):
                self.checked = None
        elif kind == "commit":
            if tag != b'COMMIT\0' or not self.active:
                self.next = None
            self.terminal = {"next": self.next, "sync": self.checked in ("on", "remote_apply") and not self.local_off,
                             "mode": "async" if self.local_off else "sync" if self.checked in ("on", "remote_apply") else "unproven",
                             "begin": self.active, "clean": self.active and not self.warned and self.warning_visibility, "tag": {"forwarded": False}}
            self.active = False
            return self.terminal["tag"]
        elif kind == "rollback" or tag == b'ROLLBACK\0':
            self.active, self.next, self.checked, self.terminal = False, None, None, None
        return None

    def backend(self, kind, payload):
        if kind == b'K':
            if (len(payload) != 8 or self.pid is not None or not 0 < struct.unpack_from("!I", payload)[0] < 2**31
                    or any(target is not self and target.pid == struct.unpack_from("!I", payload)[0] for target in self.oracle.targets.values())):
                self.fail()
            self.pid = struct.unpack_from("!I", payload)[0]
            return None
        if kind == b'N':
            self.warned = True
            if self.terminal is not None:
                self.terminal["clean"] = False
            return None
        if kind == b'E':
            self.oracle.invalid = True
            self.active, self.next, self.terminal = False, None, None
            self.expected.clear()
            return None
        if kind == b'R':
            if self.initial_ready or self.active or self.expected or len(payload) < 4:
                self.fail()
            return None
        if kind == b'S':
            name, offset = self.string(payload, 0, 128)
            _, offset = self.string(payload, offset, 4096)
            if offset != len(payload):
                self.fail()
            if name == b'synchronous_commit':
                self.checked = None
            return None
        if not self.expected:
            if kind == b'Z' and payload == b'I' and not self.initial_ready and not self.active and self.terminal is None:
                self.initial_ready = True
                return None
            self.fail()
        expected = self.expected[0]
        op = next((item for item in self.expected if isinstance(item, dict)), None)
        if kind in (b'T', b'n'):
            if expected != b'description' and not isinstance(expected, dict):
                self.fail()
            if kind == b'n':
                if payload:
                    self.fail()
            else:
                count, offset = self.short(payload, 0)
                if count > 1600 or op is None:
                    self.fail(count > 1600)
                op["columns"] = count
                for _ in range(count):
                    name, offset = self.string(payload, offset, 1024)
                    if len(payload) < offset + 18:
                        self.fail()
                    table, attr, oid, width, modifier, form = struct.unpack_from("!IhIhih", payload, offset)
                    offset += 18
                    if not oid or form not in (0, 1):
                        self.fail()
                    if op["kind"] in ("show", "guard"):
                        if count != 1 or table or attr or modifier != -1 or form != 0 or width != (-1 if op["kind"] == "show" else 1):
                            self.fail()
                        op["description"] = name, oid, form
                if offset != len(payload):
                    self.fail()
            if expected == b'description':
                self.expected.pop(0)
        elif kind == b'D':
            if not isinstance(expected, dict):
                self.fail()
            count, offset = self.short(payload, 0)
            if count != expected["columns"]:
                self.fail()
            for _ in range(count):
                if len(payload) < offset + 4:
                    self.fail()
                length = struct.unpack_from("!i", payload, offset)[0]
                offset += 4
                if length < -1 or length > len(payload) - offset:
                    self.fail()
                offset += max(0, length)
            if offset != len(payload):
                self.fail()
            expected["rows"] += 1
            if expected["kind"] in ("show", "guard"):
                if expected["rows"] != 1 or len(payload) < 6 or payload[:2] != b'\0\1' or struct.unpack_from("!i", payload, 2)[0] != len(payload) - 6 or len(payload) > 32:
                    self.fail()
                expected["value"] = payload[6:]
        elif kind == b'C':
            if not isinstance(expected, dict) or not payload.endswith(b'\0') or payload.count(b'\0') != 1:
                self.fail()
            if expected["kind"] == "other" and expected.get("simple"):
                expected["rows"], expected["columns"] = 0, None
                return None
            self.expected.pop(0)
            return self.command(expected, payload)
        elif kind == b'Z':
            if isinstance(expected, dict) and expected["kind"] == "other" and expected.get("simple"):
                self.expected.pop(0)
                expected = self.expected[0] if self.expected else None
                if payload == b'I':
                    self.active, self.next, self.checked = False, None, None
            if expected != b'Z' or payload != (b'T' if self.active else b'I'):
                self.fail()
            self.expected.pop(0)
            self.initial_ready = True
            if not self.active:
                self.portals.clear()
                self.local_off, self.checked = False, None
            if self.terminal is not None:
                terminal, self.terminal = self.terminal, None
                self.delivery = {"event": "ready", "connection": self.connection, "terminal": terminal, "forwarded": False}
                return self.delivery
        elif kind in (b'1', b'2', b'3', b't'):
            if expected != kind or kind != b't' and payload:
                self.fail()
            if kind == b't':
                count, offset = self.short(payload, 0)
                if count > self.PARAM_LIMIT or len(payload) != offset + count * 4:
                    self.fail(count > self.PARAM_LIMIT)
            self.expected.pop(0)
        elif kind in (b'G', b'H', b'd', b'c') and isinstance(expected, dict) and expected["kind"] == "other":
            return None
        else:
            self.fail()
        return None

    def publish(self, terminal, ordinal):
        self.delivery = None
        if self.oracle.invalid or self.oracle.overflow:
            return
        if terminal["begin"] and terminal["tag"]["forwarded"]:
            if self.oracle.target_transactions == self.oracle.LIMIT:
                self.fail(True)
            self.oracle.target_transactions += 1
            self.oracle.commits.append({"ordinal": ordinal, "connection": self.connection, "backend_pid": self.pid,
                                        "mode": terminal["mode"], "writing": terminal["next"] is not None, "warning_free": terminal["clean"],
                                        "prefix": max(terminal["next"][16] or 0, terminal["next"][20] or 0, terminal["next"][23] or 0) if terminal["next"] is not None else 0})
        next_row = terminal["next"]
        if not self.oracle.bound or next_row is None or not terminal["tag"]["forwarded"] or self.pid is None:
            return
        self.oracle.head = next_row
        if not terminal["sync"] or not terminal["clean"]:
            return
        prefix = max(next_row[16] or 0, next_row[20] or 0, next_row[23] or 0)
        if not prefix:
            return
        if len(self.oracle.proofs) == self.oracle.LIMIT:
            self.fail(True)
        self.oracle.proofs.append({"ordinal": ordinal, "connection": self.connection, "backend_pid": self.pid, "prefix": prefix})


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
        self.copy_statement, self.copy_stage = None, None
        self.counts = {side: {event: {} for event in ("received", "forwarded")} for side in ("client", "server")}

    def copy_query(self, payload):
        if not payload.startswith(b'COPY '):
            return
        if len(payload) > 65536 or self.copy_current is not None:
            self.copy_invalid = True
            return
        identifier = rb'"(?:[^"\x00]|"")+"'
        columns = identifier + rb'(?:, ' + identifier + rb')*'
        relation = rb'(' + identifier + rb')\.(' + identifier + rb')'
        bounds = rb"(?: WHERE ctid >= '\(([0-9]+),0\)'::pg_catalog.tid(?: AND ctid < '\(([0-9]+),0\)'::pg_catalog.tid)?)?"
        source = re.fullmatch(rb'COPY \(SELECT ' + columns + rb' FROM ONLY ' + relation + bounds + rb'\) TO STDOUT WITH \(FORMAT (text|binary)\)\x00', payload)
        target = re.fullmatch(rb'COPY ' + relation + rb' \(' + columns + rb'\) FROM STDIN WITH \(FORMAT (text|binary)(?:, FREEZE true)?\)\x00', payload)
        match = source or target
        if match is None:
            self.copy_invalid = True
            return
        names = [value[1:-1].replace(b'""', b'"').decode("utf-8") for value in match.group(1, 2)]
        start, end = ((int(source[3]), int(source[4]) if source[4] else None) if source and source[3] else (None, None))
        return {"relation": names, "direction": "out" if source else "in",
                "range": [start, end], "format": match[5 if source else 3].decode()}

    def copy_frontend(self, kind, payload):
        if kind in (b'Q', b'P'):
            if self.copy_pending is not None or self.copy_current is not None:
                self.copy_invalid = True
            self.copy_statement, self.copy_stage, self.copy_pending = None, None, None
            if kind == b'Q':
                self.copy_pending = self.copy_query(payload)
                return
            name_end = payload.find(b'\0', 0, 129)
            query_end = payload.find(b'\0', name_end + 1, name_end + 65538) if name_end >= 0 else -1
            if name_end < 0 or query_end < 0:
                self.copy_invalid = True
                return
            query = payload[name_end + 1:query_end + 1]
            if not query.startswith(b'COPY '):
                return
            if name_end != 0 or payload[query_end + 1:] != b'\0\0':
                self.copy_invalid = True
                return
            self.copy_statement = self.copy_query(query)
            self.copy_stage = "parsed" if self.copy_statement is not None else None
        elif kind in (b'B', b'D', b'E') and self.copy_statement is not None:
            expected = {b'B': ("parsed", b'\0\0\0\0\0\0\0\1\0\0', "bound"),
                        b'D': ("bound", b'P\0', "described"),
                        b'E': ("described", b'\0\0\0\0\0', "executed")}[kind]
            if self.copy_stage != expected[0] or payload != expected[1]:
                self.copy_invalid = True
                self.copy_statement, self.copy_stage, self.copy_pending = None, None, None
                return
            self.copy_stage = expected[2]
            if kind == b'E':
                self.copy_pending = dict(self.copy_statement)
        elif kind == b'S' and (self.copy_statement is not None or self.copy_pending is not None or self.copy_current is not None):
            if payload or self.copy_stage not in (None, "executed", "synced"):
                self.copy_invalid = True
                self.copy_statement, self.copy_stage, self.copy_pending = None, None, None
            elif self.copy_stage is not None:
                self.copy_stage = "synced"
        elif kind in (b'C', b'X'):
            self.copy_invalid |= self.copy_pending is not None or self.copy_current is not None
            self.copy_statement, self.copy_stage, self.copy_pending = None, None, None
        elif kind in (b'B', b'D', b'E') and (self.copy_pending is not None or self.copy_current is not None):
            self.copy_invalid = True
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
            self.copy_statement, self.copy_stage = None, None
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
        elif kind in (b'E', b'Z') and (self.copy_pending is not None or self.copy_statement is not None):
            self.copy_invalid = True
            self.copy_pending, self.copy_statement, self.copy_stage = None, None, None
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
    def __init__(self, host, mode, *, ack_oracle=None, side=None):
        if mode not in ("observe", "before_commit", "after_commit", "before_sync_commit", "after_begin", "later_begin_only", "completed_resume_close"):
            raise ValueError("unknown follow wire gate")
        if ack_oracle is not None and (not isinstance(ack_oracle, AckOracle) or side not in ("source", "target")) or ack_oracle is None and side is not None:
            raise ValueError("ACK wire observation requires an exact shared oracle and side")
        self.host, self.mode = host, mode
        self.ack_oracle, self.side = ack_oracle, side
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

    def attach_ack_oracle(self, oracle, side):
        if not isinstance(oracle, AckOracle) or side not in ("source", "target"):
            raise ValueError("ACK wire observation requires an exact shared oracle and side")
        with self.lock:
            self.ack_oracle, self.side = oracle, side

    def hold_copy(self, relation):
        if self.mode not in ("observe", "after_begin") or self.copy_relation is not None or not isinstance(relation, list) or len(relation) != 2:
            raise RuntimeError("COPY hold needs one exact relation on an observation gate")
        self.copy_relation = list(relation)

    def copy_evidence(self, timeout=None):
        acquired = self.lock.acquire() if timeout is None else self.lock.acquire(timeout=max(0, min(timeout, 0.02)))
        if not acquired:
            raise RuntimeError("COPY observer lock unavailable")
        try:
            values = [observer.copy_evidence() for observer in self.observers]
            return {"invalid": bool(self.errors) or any(value["invalid"] for value in values),
                    "overflow": self.observer_overflow or any(value["overflow"] for value in values),
                    "records": [record for value in values for record in value["records"]]}
        finally:
            self.lock.release()

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
            observation.ack_oracle, observation.ack_side = self.ack_oracle, self.side
            if self.ack_oracle is not None:
                observation.connection = self.ack_oracle.connection(self.side)
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
                ack_token = status_token = None
                oracle = getattr(observation, "ack_oracle", None)
                if oracle is not None:
                    if observation.ack_side == "target":
                        ack_token = oracle.target_frontend(observation.connection, kind, payload)
                    elif observation.copy_active and kind == b'd' and payload[:1] == b'r':
                        if len(payload) != 34 or payload[-1] not in (0, 1):
                            raise ValueError("invalid standby status frame")
                        status_token = oracle.source_status(observation.connection, struct.unpack("!QQQ", payload[1:25]))
                self.trace(observation, "client", "received", kind)
                status = commit = False
                if observation is not None:
                    with self.lock:
                        observation.copy_frontend(kind, payload)
                        if oracle is not None:
                            with oracle.lock:
                                oracle.invalid |= observation.copy_invalid
                                oracle.overflow |= observation.copy_overflow
                        if kind == b'Q':
                            observation.query(payload)
                        status = observation.copy_active and kind == b'd' and payload[:1] == b'r'
                        if self.mode == "completed_resume_close" and self.close_frontend(observation, kind, payload):
                            return
                        if self.mode in ("before_commit", "after_commit", "before_sync_commit"):
                            commit = observation.commit_protocol.frontend(kind, payload)
                if status:
                    if len(payload) != 34:
                        raise ValueError("invalid standby status frame")
                sync_commit = (self.mode == "before_sync_commit" and oracle is not None and observation.ack_side == "target"
                               and oracle.synchronous_request(observation.connection, ack_token))
                if self.armed.is_set() and (self.mode == "before_commit" and commit or sync_commit):
                    self.trace(observation, "client", "gate_wait")
                    held = self.hold(observation, self.mode, "simple" if kind == b'Q' else "extended")
                    with self.lock:
                        if held and self.abort_requested.is_set():
                            return
                self.trace(observation, "client", "forwarding")
                server.sendall(frame)
                if oracle is not None:
                    oracle.forwarded(ack_token)
                    oracle.forwarded(status_token)
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
                oracle = getattr(observation, "ack_oracle", None)
                ack_token = oracle.target_backend(observation.connection, kind, payload) if oracle is not None and observation.ack_side == "target" else None
                with self.lock:
                    token = observation.received(kind, payload)
                    observation.wire_event("server", "received", kind, payload)
                    if oracle is not None:
                        with oracle.lock:
                            oracle.invalid |= observation.copy_invalid
                            oracle.overflow |= observation.copy_overflow
                    if self.mode in ("before_commit", "after_commit", "before_sync_commit", "completed_resume_close"):
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
                if oracle is not None:
                    oracle.forwarded(ack_token)
                self.trace(observation, "server", "forwarded", kind)
                with self.lock:
                    observation.forwarded(token)
                    if oracle is not None and observation.ack_side == "source" and token is not None:
                        record, field = token
                        if field == "commit_forwarded":
                            oracle.source_transaction(record)
                        elif field == "forwarded":
                            oracle.source_keepalive(record)
        except (EOFError, OSError) as error:
            if self.mode == "completed_resume_close" and isinstance(error, TimeoutError) and not self.stopping.is_set():
                self.errors.append(type(error).__name__)
            self.trace(observation, "server", "closed", closure="eof" if isinstance(error, EOFError) else "socket_error")
        except Exception as error:
            self.errors.append(type(error).__name__)
            self.trace(observation, "server", "closed", closure="protocol_error")
        finally:
            oracle = getattr(observation, "ack_oracle", None)
            if oracle is not None:
                oracle.close(observation.ack_side, observation.connection,
                             incomplete=observation.ack_side == "source" and (observation.invalid or observation.overflow or observation.active is not None))
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
            oracle = self.ack_oracle.evidence() if self.ack_oracle is not None else None
            observers = [observer for observer in self.observers if oracle is None or observer.ack_oracle is self.ack_oracle]
            return {"hits": self.hits, "data_bytes": self.data_bytes, "begins": self.begins,
                    "commits": self.commits, "statuses": list(self.statuses) if oracle is None else [tuple(status["positions"]) for status in oracle["statuses"] if status["forwarded"]],
                    **({"ack_oracle": oracle} if oracle is not None else {}),
                    **({"dropped_keepalives": self.dropped_keepalives} if self.mode == "later_begin_only" else {}),
                    "logical": {"overflow": self.observer_overflow,
                                "streams": [observer.evidence() for observer in observers if observer.start_lsn is not None]}}
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
