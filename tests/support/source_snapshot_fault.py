"""Observe five faults on dedicated connections to an owned snapshot fixture."""

import argparse
import json
import os
from pathlib import Path
import select
import signal
import socket
import struct
import sys
import threading
import time

from db_harness import assert_identity, conninfo, psql, quote
from protocol_fault import ProtocolFault, error_sqlstate, packet
from source_snapshot_fixture import FAULT_CASES, TAIL_PHASES, expiry_sqlstate, validate_phase


def data_fields(payload):
    assert len(payload) >= 2
    count = struct.unpack_from("!H", payload)[0]
    fields, offset = [], 2
    for _ in range(count):
        assert offset + 4 <= len(payload)
        length = struct.unpack_from("!i", payload, offset)[0]
        offset += 4
        assert length >= 0 and offset + length <= len(payload), "missing snapshot wire field"
        fields.append(payload[offset:offset + length])
        offset += length
    assert offset == len(payload), "extra snapshot wire data"
    return fields


class SnapshotFault(ProtocolFault):
    def __init__(self, context, case, phase):
        super().__init__(context, "snapshot")
        self.case, self.phase = case, phase
        self.expected_sqlstate = expiry_sqlstate(context) if case == "expired_export_refused" else None
        self.command = ""
        self.state_lock = threading.RLock()
        self.client_closed = threading.Event()
        self.close_after_frame = False
        self.deadline = 0
        self.peer_pid = None
        self.native_path = None
        self.intent_before_create = False
        self.client_failed = self.fault_before_deadline = self.failure_before_deadline = False
        self.imported = False
        self.slot = None
        self.details = {"row_seen": False, "row_delivered": False, "completion_seen": False,
                        "command_tag": None, "ready_before": None, "ready_after": None,
                        "client_closed_after_ready": False, "import_seen": False,
                        "exporter_expired": False, "expected_sqlstate": self.expected_sqlstate, "sqlstate": None,
                        "identity_before": None, "identity_after": None}

    def arm(self, native_path, peer_pid):
        assert not self.armed.is_set() and not self.hits
        assert type(peer_pid) is int and peer_pid > 0
        native_path = Path(native_path)
        assert native_path.name == "manifest.json" and native_path.parent.parent == Path(self.context["root"])
        assert native_path.parent.name.startswith("snapshot-") and native_path.is_file()
        self.native_path, self.peer_pid = native_path, peer_pid
        self.deadline = time.monotonic() + 20
        self.armed.set()

    def native(self):
        assert self.native_path.stat().st_size <= 16 * 1024
        value = json.loads(self.native_path.read_text())
        assert value["magic"] == "RPGMIGRATE" and value["format_version"] == 4
        assert value["copy_snapshot"] is None, "snapshot fault contains a COPY anchor intent"
        assert value["copy_complete"] is None and value["target_provenance"] == []
        return value["source_slot"]

    def slot_name(self):
        name = self.context["slots"]["live_" + self.case]
        if self.phase in ("ready_transaction", "ready_failed"):
            name += "_t" if self.phase == "ready_transaction" else "_e"
        return name

    def slot_evidence(self):
        result = psql(self.context, self.context["database"], "SELECT json_build_object('name',slot_name,"
                      "'plugin',plugin,'database',database,'temporary',temporary,"
                      "'consistent_point',confirmed_flush_lsn::text) FROM pg_replication_slots "
                      f"WHERE slot_name={quote(self.slot_name())};")
        assert result, "observed CREATE did not retain its owned slot"
        value = json.loads(result)
        assert value["name"] == self.slot_name() and value["plugin"] == "test_decoding"
        assert value["database"] == self.context["database"] and value["temporary"] is False
        if self.slot is not None:
            assert self.slot == value, "wire consistent point differs from real slot"
        self.slot = value
        return value

    def mark_fault(self):
        assert self.armed.is_set() and self.hits == 0, "duplicate or unarmed snapshot fault"
        self.hits = 1
        self.fault_before_deadline = time.monotonic() < self.deadline

    def backend_present(self, pid):
        value = psql(self.context, self.context["database"], "SELECT json_build_array(pid,usename,datname) "
                     f"FROM pg_stat_activity WHERE pid={pid};")
        if not value:
            return False
        assert json.loads(value) == [pid, self.context["replication_role"], self.context["database"]], "foreign backend identity"
        return True

    def expire_exporter(self):
        assert self.backend_pid != self.peer_pid and self.backend_present(self.peer_pid)
        self.slot_evidence()
        result = psql(self.context, self.context["database"], "SELECT pg_terminate_backend(pid) "
                      f"FROM pg_stat_activity WHERE pid={self.peer_pid} AND "
                      f"usename={quote(self.context['replication_role'])} AND "
                      f"datname={quote(self.context['database'])} AND backend_type='walsender';")
        assert result == "t", "exporter expiry target is not the owned logical backend"
        while self.backend_present(self.peer_pid):
            assert time.monotonic() < self.deadline, "exporter expiry deadline"
            self.stopping.wait(0.01)
        self.details["exporter_expired"] = True

    def client_frame(self, kind, payload):
        if kind not in (b"P", b"Q"):
            return
        if kind == b"P":
            _, payload = payload.split(b"\0", 1)
        self.command = payload.split(b"\0", 1)[0].decode("utf-8")
        if not self.armed.is_set():
            return
        if self.command.startswith("CREATE_REPLICATION_SLOT"):
            native = self.native()
            assert native["phase"] == "provisioning", "CREATE preceded durable source intent"
            assert native["record"]["binding"]["slot_name"] == self.slot_name()
            assert native["record"]["binding"]["plugin"] == "test_decoding"
            assert native["record"]["binding"]["catalog_epoch"] == 7
            self.intent_before_create = True
        if self.command.startswith("SET TRANSACTION SNAPSHOT"):
            self.details["import_seen"] = True
            if self.case == "expired_export_refused":
                self.expire_exporter()

    def server_frame(self, kind, frame, payload):
        if kind == b"K":
            assert len(payload) == 8
            self.backend_pid = struct.unpack("!I", payload[:4])[0]
        if not self.armed.is_set():
            return frame
        creating = self.command.startswith("CREATE_REPLICATION_SLOT")
        if creating and self.case in ("create_reply_lost", "create_tail_lost"):
            if kind == b"D":
                fields = data_fields(payload)
                assert len(fields) == 4 and all(fields), "expected complete positive CREATE row"
                slot, point, _token, plugin = (field.decode("ascii") for field in fields)
                assert slot == self.slot_name() and plugin == "test_decoding"
                self.slot = {"name": slot, "plugin": plugin, "database": self.context["database"],
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
                self.mark_fault()
                if self.phase in ("ready_transaction", "ready_failed"):
                    after = b"T" if self.phase == "ready_transaction" else b"E"
                    self.details["ready_after"] = after.decode("ascii")
                    return b"Z" + struct.pack("!I", 5) + after
                self.close_after_frame = True
                return None
        if self.command.startswith("SET TRANSACTION SNAPSHOT") and kind == b"C":
            assert payload == b"SET\0"
            self.imported = True
        if self.case == "expired_export_refused" and kind == b"E" and self.details["exporter_expired"]:
            self.details["sqlstate"] = error_sqlstate(payload)
            assert self.details["sqlstate"] == self.expected_sqlstate, "snapshot import SQLSTATE differs from declared profile"
            self.mark_fault()
        if (self.case == "imported_identity_mismatch" and self.imported and kind == b"D"
                and "pg_control_system()" in self.command and "pg_backend_pid()" in self.command):
            fields = data_fields(payload)
            assert len(fields) == 4 and int(fields[2]) == self.backend_pid
            before = int(fields[2])
            after = before + 1 if before < 2**31 - 1 else before - 1
            fields[2] = str(after).encode("ascii")
            changed = struct.pack("!H", 4) + b"".join(struct.pack("!i", len(field)) + field for field in fields)
            self.details.update(identity_before=before, identity_after=after)
            self.mark_fault()
            return b"D" + struct.pack("!I", len(changed) + 4) + changed
        if self.case == "source_finish_unknown" and self.imported and kind == b"C" and self.command == "COMMIT":
            assert payload == b"COMMIT\0"
            self.details["command_tag"] = "COMMIT"
            self.mark_fault()
            self.close_after_frame = True
            return None
        return frame

    def upstream(self, client, server):
        try:
            _, frame, _ = packet(client, typed=False)
            server.sendall(frame)
            while not self.stopping.is_set():
                if not select.select([client], [], [], 0.02)[0]:
                    continue
                kind, frame, payload = packet(client)
                with self.state_lock:
                    self.client_frame(kind, payload)
                server.sendall(frame)
        except EOFError:
            with self.state_lock:
                self.client_closed.set()
                if self.details["ready_after"] is not None:
                    self.details["client_closed_after_ready"] = True
        except OSError:
            pass
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
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
                with self.state_lock:
                    frame = self.server_frame(kind, frame, payload)
                if frame is not None:
                    client.sendall(frame)
                if self.close_after_frame:
                    break
        except (EOFError, OSError):
            pass
        except Exception as error:
            self.errors.append(type(error).__name__)
        finally:
            if self.details["ready_after"] is not None and not self.client_closed.is_set():
                if not self.client_closed.wait(timeout=max(0, self.deadline - time.monotonic())):
                    self.errors.append("client did not close after edited ReadyForQuery")
            self.stopping.set()
            for connection in self.sockets:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                connection.close()
            if upstream:
                upstream.join(timeout=2)
                if upstream.is_alive():
                    self.errors.append("snapshot upstream did not stop")
            self.backend_closed.set()

    def observe_closed(self):
        assert self.hits == 1 and self.client_failed, "missing observed fault or client refusal"
        assert self.backend_closed.wait(timeout=max(0, self.deadline - time.monotonic())), "relay closure deadline"
        for pid in (self.backend_pid, self.peer_pid):
            assert type(pid) is int and pid > 0
            while self.backend_present(pid):
                assert time.monotonic() < self.deadline, "owned backend closure deadline"
                self.stopping.wait(0.01)
        self.slot_evidence()

    def record(self, closed=False):
        native = self.native() if self.native_path is not None else None
        intent = None if native is None else (native["record"] if native["phase"] == "provisioning" else native["record"]["intent"])
        value = {"owner": self.context["token"], "case": self.case, "phase": self.phase,
                 "status": "pending", "armed": self.armed.is_set(), "fault_hits": self.hits,
                 "backend_pid": self.backend_pid, "peer_pid": self.peer_pid, "helper_pid": os.getpid(),
                 "backend_closed": closed, "peer_closed": closed, "client_failed": self.client_failed,
                 "fault_before_deadline": self.fault_before_deadline, "failure_before_deadline": self.failure_before_deadline,
                 "deadline_seconds": 20, "helper_reaped": False, "helper_returncode": None,
                 "slot": self.slot, "native_path": str(self.native_path),
                 "native_nonce": None if intent is None else intent["nonce"],
                 "native_phase": None if native is None else native["phase"],
                 "intent_before_create": self.intent_before_create, "details": dict(self.details)}
        if closed and not self.errors:
            value["status"] = "observed"
            validate_phase(value, self.context, self.case, self.phase, reaped=False)
        return value


def copy_uint(value, bits, *, positive=False):
    assert type(value) is int and 0 <= value < 2**bits and (not positive or value > 0), "invalid COPY native integer"


def copy_nonce(value):
    assert type(value) is list and len(value) == 16 and any(value), "invalid COPY native nonce"
    assert all(type(byte) is int and 0 <= byte <= 255 for byte in value), "invalid COPY native nonce bytes"


def copy_stream(value):
    assert type(value) is dict and set(value) == {"run", "source", "generation"}
    copy_nonce(value["run"])
    copy_uint(value["generation"], 64)
    source = value["source"]
    assert type(source) is dict and set(source) == {"system_identifier", "database_oid"}
    copy_uint(source["system_identifier"], 64, positive=True)
    copy_uint(source["database_oid"], 32, positive=True)


def copy_manifest(context, case, native_path):
    path = Path(native_path)
    assert path.is_absolute() and path.name == "manifest.json" and path.parent.parent == Path(context["root"]), "foreign COPY native path"
    with path.open() as source:
        text = source.read(16 * 1024 + 1)
    assert len(text.encode("utf-8")) <= 16 * 1024, "oversized COPY native manifest"
    value = json.loads(text)
    assert value["magic"] == "RPGMIGRATE" and type(value["format_version"]) is int and value["format_version"] == 4
    assert value["canonical_path"] == str(path.parent.resolve()), "COPY native path differs from its manifest"
    copy_nonce(value["run"])
    source = value["source_slot"]
    assert source["phase"] == "confirmed", "COPY needs confirmed source provenance"
    receipt = source["record"]
    binding = receipt["intent"]["binding"]
    copy_nonce(receipt["intent"]["nonce"])
    stream = binding["stream"]
    copy_stream(stream)
    assert stream["run"] == value["run"] and stream["source"] == context["copy_snapshot_variants"][case]["source_identity"]
    assert stream["generation"] == 9 and type(binding["catalog_epoch"]) is int and binding["catalog_epoch"] == 7
    assert binding["plugin"] == "test_decoding" and binding["slot_name"] == context["copy_snapshot_slots"][case]
    copy_uint(receipt["consistent_point"], 64, positive=True)
    token = receipt["snapshot_token"]
    assert type(token) is str and 0 < len(token.encode("utf-8")) <= 1024 and "\0" not in token
    return path, value, receipt


def copy_source_binding(context, case, native_path):
    path, _value, source = copy_manifest(context, case, native_path)
    return path, source["intent"]["binding"]["stream"]


def copy_native_binding(context, case, native_path):
    path, value, source = copy_manifest(context, case, native_path)
    intent = value["copy_snapshot"]
    assert type(intent) is dict and set(intent) == {"anchor", "plan"}, "missing populated COPY intent"
    anchor, plan = intent["anchor"], intent["plan"]
    assert set(anchor) == {"stream", "snapshot_token", "consistent_point", "plan_nonce"}
    assert set(plan) == {"reference_version", "plan_nonce", "table_count", "logical_bytes", "crc32"}
    stream = source["intent"]["binding"]["stream"]
    copy_stream(anchor["stream"])
    copy_uint(anchor["consistent_point"], 64, positive=True)
    assert anchor["stream"] == stream and anchor["consistent_point"] == source["consistent_point"]
    assert anchor["snapshot_token"] == source["snapshot_token"], "COPY anchor differs from confirmed source"
    copy_nonce(anchor["plan_nonce"])
    copy_nonce(plan["plan_nonce"])
    assert plan["plan_nonce"] == anchor["plan_nonce"], "COPY plan nonce differs"
    copy_uint(plan["reference_version"], 32)
    assert plan["reference_version"] == 1
    for name in ("table_count", "logical_bytes"):
        copy_uint(plan[name], 64)
    copy_uint(plan["crc32"], 32)
    assert plan["table_count"] == context["copy_snapshot_variants"][case]["tables"]
    targets = value["target_provenance"]
    assert type(targets) is list and len(targets) == 1 and targets[0]["phase"] == "confirmed"
    target = targets[0]["record"]
    copy_nonce(target["intent"]["nonce"])
    target_binding = target["intent"]["binding"]
    copy_stream(target_binding["stream"])
    assert target_binding["stream"] == stream and type(target_binding["metadata_version"]) is int and target_binding["metadata_version"] == 3
    for name in ("target_database_oid", "owner_oid"):
        copy_uint(target_binding[name], 32, positive=True)
    for name in ("schema_oid", "table_oid"):
        copy_uint(target[name], 32, positive=True)
    return path, stream


def completed_target_copy(context, case, native_path, stream):
    from copy_snapshot_fixture import copy_snapshot_receipt_path, read_wire, validate_copy_snapshot_phase
    path = copy_snapshot_receipt_path(context, "target", case, "copy")
    value = read_wire(path)
    validate_copy_snapshot_phase(value, context, "target", case, "copy", reaped=True)
    assert value["native_path"] == str(native_path) and value["stream"] == stream, "target COPY receipt binds another native state"
    return value


class CopySnapshotFault(SnapshotFault):
    def __init__(self, context, case, phase):
        from copy_snapshot_fixture import copy_snapshot_receipt_path
        copy_snapshot_receipt_path(context, "source", case, phase)
        network = {**context["copy_snapshot_source"],
                   "database": context["copy_snapshot_variants"][case]["source_database"]}
        super().__init__(network, "source_finish_unknown", phase)
        self.copy_context, self.copy_case, self.copy_phase = context, case, phase
        self.copy_stream = None
        self.copy_observer = True

    def slot_name(self):
        return self.copy_context["copy_snapshot_slots"][self.copy_case]

    def loss_binding(self, native_path):
        path, stream = copy_native_binding(self.copy_context, self.copy_case, native_path)
        completed_target_copy(self.copy_context, self.copy_case, path, stream)
        if self.copy_stream is not None:
            assert stream == self.copy_stream, "COPY stream changed after loss arm"
        return path, stream

    def arm(self, native_path, peer_pid):
        assert not self.armed.is_set() and not self.hits and self.imported, "COPY source loss needs an observed successful import"
        assert type(self.backend_pid) is int and self.backend_pid > 0
        assert type(peer_pid) is int and peer_pid > 0 and peer_pid != self.backend_pid
        path, stream = self.loss_binding(native_path)
        assert self.backend_present(self.backend_pid) and self.backend_present(peer_pid), "COPY source backend is not live and owned"
        self.native_path, self.peer_pid, self.copy_stream = path, peer_pid, stream
        ProtocolFault.arm(self)
        self.deadline = self.copy_started_at + self.copy_deadline_ms / 1000

    def client_frame(self, kind, payload):
        if kind == b"d" and self.copy_window():
            with self.copy_lock:
                self.client_copy_bytes += len(payload)
        if kind in (b"P", b"Q"):
            offset = payload.index(b"\0") + 1 if kind == b"P" else 0
            if payload.startswith(b"SET TRANSACTION SNAPSHOT ", offset):
                command = b"SET TRANSACTION SNAPSHOT"
            elif payload.startswith(b"COMMIT\0", offset):
                command = b"COMMIT"
            else:
                command = b""
            super().client_frame(b"Q", command + b"\0")

    def server_frame(self, kind, frame, payload):
        # Import/session facts precede loss-arm; no COPY or command counters do.
        if self.command.startswith("SET TRANSACTION SNAPSHOT") and kind == b"C" and payload == b"SET\0":
            self.imported = True
        if not self.armed.is_set() and kind == b"C" and payload in (b"COMMIT\0", b"ROLLBACK\0"):
            self.imported = False
        self.observe_copy_response(kind, payload)
        if kind == b"E" and self.copy_window():
            with self.copy_lock:
                self.error_states.append(error_sqlstate(payload))
        if self.armed.is_set() and self.imported and kind == b"C" and self.command == "COMMIT":
            self.loss_binding(self.native_path)
        return super().server_frame(kind, frame, payload)

    def run(self):
        super().run()
        if self.close_after_frame and self.backend_closed.is_set():
            self.client_closed.set()

    def record(self, closed=False):
        wire = self.completed_copy_observations() if closed else self.copy_observations()
        value = {"owner": self.copy_context["token"], "profile": "copy_snapshot", "side": "source",
                 "case": self.copy_case, "phase": self.copy_phase, "status": "pending",
                 "source_owner": self.context["token"], "native_path": None if self.native_path is None else str(self.native_path),
                 "stream": self.copy_stream, "peer_pid": self.peer_pid, "peer_closed": closed,
                 "imported": self.imported, "held_fence_observed": False, "released_fence_observed": False, **wire}
        if closed:
            assert self.hits == 1 and self.client_failed and self.fault_before_deadline and self.failure_before_deadline
            self.loss_binding(self.native_path)
            value["status"] = "observed"
        return value


def copy_main(case, phase):
    from authority_fault import copy_request
    from copy_snapshot_fixture import copy_snapshot_receipt_path, validate_copy_snapshot_phase
    context = json.loads(Path(os.environ["RPGMIGRATE_TEST_COPY_SNAPSHOT_CONTEXT"]).read_text())
    receipt = copy_snapshot_receipt_path(context, "source", case, phase)
    relay = CopySnapshotFault(context, case, phase)
    assert_identity(context)
    assert_identity(relay.context)
    receipt.touch(mode=0o600, exist_ok=False)
    closed = False

    def publish():
        value = relay.record(closed)
        if closed:
            validate_copy_snapshot_phase(value, context, "source", case, phase, reaped=False)
        receipt.write_text(json.dumps(value))
        return value

    try:
        with relay:
            publish()
            source = conninfo(relay.context, relay.context["database"], relay.context["replication_role"], str(relay.directory))
            print(json.dumps({"ready": True, "conninfo": source, "receipt": str(receipt)}), flush=True)
            while True:
                request = copy_request(relay)
                operation = request["op"]
                if operation == "arm":
                    assert set(request) == {"op", "native_path", "peer_pid"}
                    relay.arm(request["native_path"], request["peer_pid"])
                else:
                    assert set(request) == {"op"}
                    if operation == "failed":
                        assert relay.armed.is_set() and relay.hits == 1 and relay.close_after_frame and not relay.client_failed
                        relay.client_failed = True
                        relay.failure_before_deadline = time.monotonic() < relay.deadline
                    elif operation == "quit":
                        relay.observe_closed()
                        break
                    elif operation != "status":
                        raise ValueError("unknown COPY source controller operation")
                print(json.dumps({"op": operation, **publish()}), flush=True)
        closed = True
        print(json.dumps({"op": "quit", **publish()}), flush=True)
    finally:
        publish()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--case", choices=FAULT_CASES)
    selector.add_argument("--copy-case")
    parser.add_argument("--phase")
    args = parser.parse_args()
    if args.copy_case is not None:
        from copy_snapshot_fixture import COPY_SOURCE_PHASES
        if args.copy_case not in COPY_SOURCE_PHASES or args.phase not in COPY_SOURCE_PHASES[args.copy_case]:
            parser.error("unknown COPY source case or phase")
        copy_main(args.copy_case, args.phase)
        return
    if args.phase is not None and args.phase not in TAIL_PHASES:
        parser.error("unknown source snapshot phase")
    assert args.phase is None or args.case == "create_tail_lost"
    phase = (args.phase or "tail_lost") if args.case == "create_tail_lost" else args.case
    context = json.loads(Path(os.environ["RPGMIGRATE_TEST_SNAPSHOT_CONTEXT"]).read_text())
    assert_identity(context)
    receipt = Path(context["root"]) / f"snapshot-fault-{args.case}.json"
    previous = {}
    if args.case == "create_tail_lost" and phase != "tail_lost":
        prior = json.loads(receipt.read_text())
        assert prior["owner"] == context["token"] and prior["case"] == args.case
        expected = set(TAIL_PHASES[:TAIL_PHASES.index(phase)])
        assert set(prior["phases"]) == expected, "missing or duplicate previous CREATE phase"
        previous = prior["phases"]
        for name, proof in previous.items():
            validate_phase(proof, context, args.case, name)
    else:
        receipt.touch(mode=0o600, exist_ok=False)
    relay = SnapshotFault(context, args.case, phase)
    closed = False

    def publish():
        value = relay.record(closed)
        if args.case == "create_tail_lost":
            phases = {**previous, phase: value}
            group = {"owner": context["token"], "case": args.case,
                     "status": "observed" if len(phases) == 3 and value["status"] == "observed" else "pending",
                     "fault_hits": sum(proof["fault_hits"] for proof in phases.values()), "phases": phases}
        else:
            group = value
        receipt.write_text(json.dumps(group))
        return value

    try:
        with relay:
            source = conninfo(context, context["database"], context["replication_role"], str(relay.directory))
            if args.case in ("create_reply_lost", "create_tail_lost"):
                source += " replication=database"
            print(json.dumps({"ready": True, "conninfo": source, "receipt": str(receipt)}), flush=True)
            while True:
                timeout = max(0, relay.deadline - time.monotonic()) if relay.armed.is_set() else 20
                assert select.select([sys.stdin], [], [], timeout)[0], "snapshot controller deadline"
                line = sys.stdin.readline(4097)
                assert line and len(line) <= 4096, "missing or oversized snapshot controller request"
                request = json.loads(line)
                operation = request["op"]
                if operation == "arm":
                    assert set(request) == {"op", "native_path", "peer_pid"}
                    relay.arm(request["native_path"], request["peer_pid"])
                elif operation == "failed":
                    assert set(request) == {"op"} and relay.armed.is_set() and not relay.client_failed
                    relay.client_failed = True
                    relay.failure_before_deadline = time.monotonic() < relay.deadline
                else:
                    assert operation in ("status", "quit") and set(request) == {"op"}
                if operation == "quit":
                    relay.observe_closed()
                    closed = True
                    break
                print(json.dumps({"op": operation, **publish()}), flush=True)
        print(json.dumps({"op": "quit", **publish()}), flush=True)
    finally:
        publish()


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
