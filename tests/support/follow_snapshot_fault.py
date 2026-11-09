"""Fault the production pgoutput snapshot lifecycle on owned local sessions."""

import argparse
import json
import os
from pathlib import Path
import re
import select
import socket
import struct
import sys
import tempfile
import threading
import time

from db_harness import psql, quote
from follow_snapshot_fixture import validate_identity_modes
from protocol_fault import error_sqlstate, packet


FAULT_CASES = ("create_reply_lost", "create_tail_lost", "expired_export_refused",
               "imported_identity_mismatch", "source_finish_unknown")
TAIL_PHASES = ("tail_lost", "ready_transaction", "ready_failed")


def fields(payload):
    assert len(payload) >= 2, "missing DataRow count"
    count, offset = struct.unpack_from("!H", payload)[0], 2
    values = []
    for _ in range(count):
        assert offset + 4 <= len(payload), "missing DataRow length"
        size = struct.unpack_from("!i", payload, offset)[0]
        offset += 4
        assert size >= 0 and offset + size <= len(payload), "invalid DataRow value"
        values.append(payload[offset:offset + size])
        offset += size
    assert offset == len(payload), "extra DataRow bytes"
    return values


def changed_pid(payload, pid):
    values = fields(payload)
    assert len(values) == 5 and values[4] == str(pid).encode("ascii"), "missing physical SQL PID field"
    replacement = pid + 1 if pid < 2**31 - 1 else pid - 1
    assert replacement > 0 and replacement != pid
    values[4] = str(replacement).encode("ascii")
    data = struct.pack("!H", len(values)) + b"".join(struct.pack("!i", len(value)) + value for value in values)
    return b"D" + struct.pack("!I", len(data) + 4) + data, replacement


def unique_fields(pairs):
    value = dict(pairs)
    assert len(value) == len(pairs), "duplicate helper JSON field"
    return value


class Relay:
    def __init__(self, context, case, phase):
        self.context, self.case, self.phase = context, case, phase
        self.stop = threading.Event()
        self.lock = threading.RLock()
        self.sessions, self.threads, self.errors = [], [], []
        self.armed = False
        self.hits = self.create_dispatches = 0
        self.failed = False
        self.finish_armed = False
        self.retry_refused = False
        self.details = {"import_seen": False, "imported": False, "identity_before": None,
                        "identity_after": None, "row_seen": False, "row_delivered": False,
                        "completion_seen": False, "command_tag": None, "ready_before": None,
                        "ready_after": None, "client_closed_after_ready": False,
                        "exporter_expired": False, "sqlstate": None}
        self.publication_commit = self.intent_before_create = False
        self.slot = None
        self.creation_intent = self.creation_publication = None
        self.fault_before_deadline = self.failure_before_deadline = False

    def __enter__(self):
        root = Path(self.context["root"])
        assert (root / ".e1-owner").read_text() == self.context["token"], "foreign fixture root"
        assert (Path(self.context["socket_dir"]) / ".e1-owner").read_text() == self.context["token"], "foreign fixture socket directory"
        facts = json.loads(psql(self.context, "postgres", "SELECT json_build_array(current_setting('data_directory'),current_setting('cluster_name'));"))
        assert facts == [str(root / "data"), self.context["token"]], "foreign PostgreSQL fixture"
        self.directory = Path(tempfile.mkdtemp(prefix="fs-", dir=self.context["socket_dir"]))
        self.directory.chmod(0o700)
        self.path = self.directory / f".s.PGSQL.{self.context['port']}"
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(self.path))
        self.path.chmod(0o700)
        self.listener.listen(4)
        self.listener.settimeout(0.1)
        self.acceptor = threading.Thread(target=self.accept)
        self.acceptor.start()
        return self

    def arm(self, request):
        assert not self.armed
        assert set(request) == {"op", "native_path", "database", "role", "slot_name", "publication_name"}
        path = Path(request["native_path"])
        assert path.name == "manifest.json" and path.parent.parent == Path(self.context["root"])
        assert path.parent.name.startswith("m3_fault_") and path.is_file()
        for key in ("database", "role", "slot_name", "publication_name"):
            assert re.fullmatch(r"[a-z][a-z0-9_]{0,62}", request[key]), "invalid owned fixture name"
            setattr(self, key, request[key])
        owner = psql(self.context, self.database, "SELECT shobj_description(oid,'pg_database') FROM pg_database WHERE datname=current_database();")
        assert owner == self.context["token"], "foreign fixture database"
        facts = json.loads(psql(self.context, self.database, "SELECT json_build_array(c.system_identifier::text,d.oid::bigint,k.timeline_id::bigint) FROM pg_control_system() c CROSS JOIN pg_control_checkpoint() k CROSS JOIN pg_database d WHERE d.datname=current_database();"))
        self.source_identity = {"system_identifier": int(facts[0]), "database_oid": facts[1]}
        self.source_timeline = facts[2]
        self.native_path, self.deadline = path, time.monotonic() + 20
        self.armed = True

    def native(self):
        assert self.native_path.stat().st_size <= 2 * 1024 * 1024
        value = json.loads(self.native_path.read_text(), object_pairs_hook=unique_fields)
        assert value["magic"] == "RPGMIGRATE" and type(value["format_version"]) is int and value["format_version"] == 7
        assert value["canonical_path"] == str(self.native_path.parent)
        assert value["copy_complete"] is None and value["clone_state"] is None
        assert value["target_provenance"] == [] and value["operator"] is None
        follow = value["follow_state"]
        validate_identity_modes(follow["preliminary"])
        assert follow["binding"] is None and follow["restart"] is None
        source = follow["preliminary"]["source"]
        assert source["major"] == self.context["server_version_num"] // 10000
        assert source["identity"] == self.source_identity and source["timeline"] == self.source_timeline
        publication = follow["publication"]
        assert publication["phase"] == "confirmed" and publication["receipt"]["name"] == self.publication_name
        slot = value["source_slot"]
        intent = slot["record"] if slot["phase"] == "provisioning" else slot["record"]["intent"]
        assert intent["binding"]["slot_name"] == self.slot_name
        assert intent["binding"]["plugin"] == "pgoutput" and intent["binding"]["catalog_epoch"] == 1
        stream = intent["binding"]["stream"]
        assert stream == {"run": value["run"], "source": source["identity"], "generation": 1}
        assert follow["preliminary"]["generation"] == 1
        assert len(intent["nonce"]) == 16 and any(intent["nonce"])
        return value, slot, intent, publication

    def accept(self):
        while not self.stop.is_set():
            try:
                client, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            try:
                assert len(self.sessions) < 4, "excess snapshot connections"
                _, startup, payload = packet(client, typed=False)
                assert payload[:4] == b"\0\3\0\0" and payload.endswith(b"\0\0")
                values = payload[4:-2].split(b"\0")
                assert len(values) % 2 == 0
                startup_fields = unique_fields(list(zip(values[::2], values[1::2])))
                role = startup_fields[b"user"].decode("ascii")
                database = startup_fields[b"database"].decode("ascii")
                assert self.armed and role == self.role and database == self.database
                replication = startup_fields.get(b"replication") == b"database"
                server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                server.connect(str(Path(self.context["socket_dir"]) / f".s.PGSQL.{self.context['port']}"))
                session = {"client": client, "server": server, "replication": replication,
                           "pid": None, "command": "", "imported": False, "publication": False,
                           "client_closed": False, "backend_closed": False}
                if any(s["replication"] == replication for s in self.sessions):
                    assert not replication and self.failed and self.case in ("create_reply_lost", "create_tail_lost"), "duplicate session role"
                self.sessions.append(session)
                server.sendall(startup)
                for function in (self.upstream, self.downstream):
                    thread = threading.Thread(target=function, args=(session,))
                    self.threads.append(thread)
                    thread.start()
            except Exception as error:
                self.errors.append(type(error).__name__ + ": " + str(error))
                client.close()

    def hit(self):
        assert self.armed and self.hits == 0, "duplicate or unarmed fault"
        self.hits = 1
        self.fault_before_deadline = time.monotonic() < self.deadline

    def client_frame(self, session, kind, payload):
        if kind not in (b"Q", b"P"):
            return
        if kind == b"P":
            payload = payload.split(b"\0", 1)[1]
        command = payload.split(b"\0", 1)[0].decode("utf-8")
        session["command"] = command
        if command.startswith("CREATE PUBLICATION"):
            session["publication"] = True
        if command.startswith("CREATE_REPLICATION_SLOT"):
            self.create_dispatches += 1
            _, slot, intent, publication = self.native()
            assert self.creation_intent is None, "a second CREATE attempted to replace the original intent"
            self.creation_intent, self.creation_publication = intent, publication["receipt"]
            assert session["replication"] and self.publication_commit and slot["phase"] == "provisioning"
            assert f'"{self.slot_name}"' in command and '"pgoutput"' in command
            self.intent_before_create = True
        if command.startswith("SET TRANSACTION SNAPSHOT"):
            self.details["import_seen"] = True
            self.native()
            if self.case == "expired_export_refused":
                self.expire()

    def server_frame(self, session, kind, frame, payload):
        if kind == b"K":
            assert len(payload) == 8
            session["pid"] = struct.unpack("!I", payload[:4])[0]
        command = session["command"]
        if command == "COMMIT" and kind == b"C" and session["publication"] and not session["imported"]:
            assert payload == b"COMMIT\0"
            self.publication_commit = True
        if command.startswith("SET TRANSACTION SNAPSHOT") and kind == b"C":
            assert payload == b"SET\0"
            session["imported"] = self.details["imported"] = True
        if (self.case == "imported_identity_mismatch" and session["imported"] and kind == b"D"
                and "pg_control_system()" in command):
            assert "pg_backend_pid()" in command, "post-import query omitted SQL PID"
            values = fields(payload)
            assert len(values) == 5 and values[4] == str(session["pid"]).encode("ascii")
            self.details["identity_before"] = session["pid"]
            if self.phase != "control":
                frame, replacement = changed_pid(payload, session["pid"])
                self.details["identity_after"] = replacement
                self.hit()
        creating = command.startswith("CREATE_REPLICATION_SLOT")
        if creating and self.case in ("create_reply_lost", "create_tail_lost"):
            if kind == b"D":
                values = fields(payload)
                assert len(values) == 4 and all(values), "incomplete CREATE row"
                name, point, _token, plugin = (value.decode("ascii") for value in values)
                assert name == self.slot_name and plugin == "pgoutput"
                self.slot = {"name": name, "plugin": plugin, "database": self.database,
                             "database_oid": self.native()[2]["binding"]["stream"]["source"]["database_oid"],
                             "temporary": False, "consistent_point": point}
                self.details["row_seen"] = True
                self.details["row_delivered"] = self.case == "create_tail_lost"
                if self.case == "create_reply_lost":
                    return None
            if kind == b"C":
                assert payload == b"CREATE_REPLICATION_SLOT\0" and self.details["row_seen"]
                self.details.update(completion_seen=True, command_tag="CREATE_REPLICATION_SLOT")
                if self.phase not in ("ready_transaction", "ready_failed"):
                    return None
            if kind == b"Z":
                assert payload == b"I" and self.details["row_seen"] and self.details["completion_seen"]
                self.details["ready_before"] = "I"
                self.hit()
                if self.phase in ("ready_transaction", "ready_failed"):
                    state = "T" if self.phase == "ready_transaction" else "E"
                    self.details["ready_after"] = state
                    return b"Z" + struct.pack("!I", 5) + state.encode("ascii")
                session["close_after"] = True
                return None
        if (self.case == "expired_export_refused" and self.details["exporter_expired"]
                and not session["replication"] and command.startswith("SET TRANSACTION SNAPSHOT") and kind == b"E"):
            state = error_sqlstate(payload)
            assert state == {160015: "22023", 180006: "42704"}[self.context["server_version_num"]], f"expiry SQLSTATE {state} from {'replication' if session['replication'] else 'SQL'} session"
            self.details["sqlstate"] = state
            self.hit()
        if (self.case == "source_finish_unknown" and session["imported"] and command == "COMMIT" and kind == b"C"):
            assert self.finish_armed and payload == b"COMMIT\0"
            self.details["command_tag"] = "COMMIT"
            self.hit()
            session["close_after"] = True
            return None
        return frame

    def backend_present(self, session):
        assert type(session["pid"]) is int and session["pid"] > 0
        raw = psql(self.context, self.database, "SELECT json_build_array(pid,usename,datname,backend_type) FROM pg_stat_activity "
                   f"WHERE pid={session['pid']};")
        if not raw:
            return False
        assert json.loads(raw) == [session["pid"], self.role, self.database,
                                  "walsender" if session["replication"] else "client backend"], "foreign backend"
        return True

    def slot_evidence(self):
        raw = psql(self.context, self.database, "SELECT json_build_object('name',slot_name,'plugin',plugin,"
                   "'database',database,'database_oid',datoid::bigint,'temporary',temporary,'consistent_point',confirmed_flush_lsn::text) "
                   f"FROM pg_replication_slots WHERE slot_name={quote(self.slot_name)};")
        assert raw, "owned slot is missing"
        value = json.loads(raw, object_pairs_hook=unique_fields)
        native, slot, intent, publication = self.native()
        assert intent == self.creation_intent and publication["receipt"] == self.creation_publication, "original source ownership was replaced"
        assert value["name"] == self.slot_name and value["plugin"] == "pgoutput"
        assert value["database"] == self.database and value["database_oid"] == intent["binding"]["stream"]["source"]["database_oid"]
        assert value["temporary"] is False
        if self.slot is not None:
            assert value == self.slot, "slot changed after CREATE"
        if slot["phase"] == "confirmed":
            high, low = value["consistent_point"].split("/")
            assert (int(high, 16) << 32) | int(low, 16) == slot["record"]["consistent_point"]
        actual = psql(self.context, self.database, "SELECT json_build_object('name',pubname,'oid',oid::bigint,'owner_oid',pubowner::bigint) "
                      f"FROM pg_publication WHERE pubname={quote(self.publication_name)};")
        assert json.loads(actual) == publication["receipt"], "owned publication changed"
        self.slot = value
        return native, slot, intent, publication

    def expire(self):
        exporter = next(session for session in self.sessions if session["replication"])
        self.slot_evidence()
        assert self.backend_present(exporter)
        result = psql(self.context, self.database, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                      f"WHERE pid={exporter['pid']} AND usename={quote(self.role)} AND datname={quote(self.database)} "
                      "AND backend_type='walsender';")
        assert result == "t", "owned exporter termination was not confirmed"
        while self.backend_present(exporter):
            assert time.monotonic() < self.deadline, "exporter expiry deadline"
            self.stop.wait(0.01)
        self.details["exporter_expired"] = True

    def complete(self):
        assert self.hits == 1 and self.failed, "missing intended fault or client refusal"
        assert 2 <= len(self.sessions) <= 4 and sum(session["replication"] for session in self.sessions) == 1
        for session in self.sessions:
            while self.backend_present(session) or not session["backend_closed"]:
                assert time.monotonic() < self.deadline, "owned session closure deadline"
                self.stop.wait(0.01)
        assert not self.errors, self.errors
        native, source_slot, intent, publication = self.slot_evidence()
        create_loss = self.case in ("create_reply_lost", "create_tail_lost")
        assert source_slot["phase"] == ("provisioning" if create_loss else "confirmed")
        assert not create_loss or self.retry_refused, "missing one-shot retry refusal"
        assert self.create_dispatches == 1 and self.publication_commit and self.intent_before_create
        if self.phase in ("ready_transaction", "ready_failed"):
            assert self.details["client_closed_after_ready"]
        return {"owner": self.context["token"], "profile": "follow_snapshot", "case": self.case,
                "phase": self.phase, "status": "observed", "server_version_num": self.context["server_version_num"],
                "native_path": str(self.native_path), "stream": intent["binding"]["stream"],
                "publication": publication["receipt"], "slot": self.slot, "source_slot_phase": source_slot["phase"],
                "slot_nonce": intent["nonce"], "publication_before_slot": self.publication_commit,
                "intent_before_create": self.intent_before_create,
                "sql_pid": next(s["pid"] for s in self.sessions if not s["replication"]),
                "exporter_pid": next(s["pid"] for s in self.sessions if s["replication"]),
                "helper_pid": os.getpid(), "armed": self.armed, "fault_hits": self.hits,
                "client_failed": self.failed, "fault_before_deadline": self.fault_before_deadline,
                "failure_before_deadline": self.failure_before_deadline, "deadline_seconds": 20,
                "sql_closed": True, "exporter_closed": True, "helper_reaped": False, "helper_returncode": None,
                "retry_refused": self.retry_refused, "create_dispatches": self.create_dispatches,
                "details": dict(self.details)}

    def upstream(self, session):
        try:
            while not self.stop.is_set():
                kind, frame, payload = packet(session["client"])
                with self.lock:
                    self.client_frame(session, kind, payload)
                    if kind == b"X":
                        session["client_closed"] = True
                        if self.details["ready_after"] is not None and session["replication"]:
                            self.details["client_closed_after_ready"] = True
                session["server"].sendall(frame)
        except EOFError:
            session["client_closed"] = True
            if self.details["ready_after"] is not None and session["replication"]:
                self.details["client_closed_after_ready"] = True
        except OSError:
            pass
        except Exception as error:
            self.errors.append(type(error).__name__ + ": " + str(error))
        finally:
            self.close_socket(session["server"])

    def downstream(self, session):
        try:
            while not self.stop.is_set():
                kind, frame, payload = packet(session["server"])
                with self.lock:
                    changed = self.server_frame(session, kind, frame, payload)
                if changed is not None:
                    session["client"].sendall(changed)
                if session.get("close_after", False):
                    break
        except (EOFError, OSError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__ + ": " + str(error))
        finally:
            session["backend_closed"] = True
            self.close_socket(session["client"])

    @staticmethod
    def close_socket(sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def stats(self):
        assert not self.errors, self.errors
        return {"fault_hits": self.hits, "details": self.details, "create_dispatches": self.create_dispatches}

    def __exit__(self, *_):
        self.stop.set()
        self.close_socket(self.listener)
        for session in self.sessions:
            for key in ("client", "server"):
                self.close_socket(session[key])
        self.acceptor.join(3)
        for thread in self.threads:
            thread.join(3)
        assert not self.acceptor.is_alive() and all(not thread.is_alive() for thread in self.threads)
        self.path.unlink()
        self.directory.rmdir()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", choices=FAULT_CASES, required=True)
    parser.add_argument("--phase")
    args = parser.parse_args()
    phase = args.phase or args.case
    assert phase in (TAIL_PHASES if args.case == "create_tail_lost" else ((args.case, "control") if args.case == "imported_identity_mismatch" else (args.case,))), "invalid snapshot fault phase"
    context = json.loads(Path(os.environ["RPGMIGRATE_TEST_FOLLOW_SNAPSHOT_CONTEXT"]).read_text(), object_pairs_hook=unique_fields)
    with Relay(context, args.case, phase) as relay:
        print(json.dumps({"ready": True, "proxy_socket_dir": str(relay.directory), "helper_pid": os.getpid()}), flush=True)
        for line in sys.stdin:
            request = json.loads(line, object_pairs_hook=unique_fields)
            operation = request["op"]
            if operation == "arm":
                relay.arm(request)
            elif operation == "stats":
                assert set(request) == {"op"}
            elif operation == "finish":
                assert set(request) == {"op"} and args.case == "source_finish_unknown"
                assert relay.details["imported"] and not relay.finish_armed and relay.hits == 0
                relay.finish_armed = True
            elif operation == "failed":
                assert set(request) == {"op"} and relay.hits == 1 and not relay.failed
                relay.failed = True
                relay.failure_before_deadline = time.monotonic() < relay.deadline
            elif operation == "retry_refused":
                assert set(request) == {"op"} and relay.failed and relay.create_dispatches == 1
                assert args.case in ("create_reply_lost", "create_tail_lost")
                relay.retry_refused = True
            elif operation == "quit":
                assert set(request) == {"op"}
                value = relay.stats() if phase == "control" else relay.complete()
                print(json.dumps(value), flush=True)
                break
            else:
                raise AssertionError("unknown snapshot helper operation")
            print(json.dumps(relay.stats()), flush=True)
        else:
            raise AssertionError("snapshot helper stdin closed without quit")


if __name__ == "__main__":
    main()
