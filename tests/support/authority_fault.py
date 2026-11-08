"""Drop one observed COMMIT reply while retaining its owned backend fence."""

import argparse
import json
import os
from pathlib import Path
import select
import signal
import sys
import time

from db_harness import assert_identity, conninfo, psql
from protocol_fault import ProtocolFault

FAULT_CASES = ("lost_commit_reply", "provisioning_recovery")


def backend_evidence(context, pid):
    assert type(pid) is int and pid > 0, "missing observed backend identity"
    return json.loads(psql(context, "e3_authority", f"SELECT json_build_array(EXISTS(SELECT FROM pg_stat_activity WHERE pid={pid}), (SELECT count(*) FROM pg_locks WHERE pid={pid} AND locktype='advisory' AND granted));"))


def copy_request(relay):
    from copy_snapshot_fixture import unique_fields
    deadline = time.monotonic() + 20 if relay.copy_started_at is None else relay.copy_started_at + relay.copy_deadline_ms / 1000
    line = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        assert remaining > 0 and select.select([sys.stdin], [], [], remaining)[0], "COPY controller deadline"
        byte = os.read(sys.stdin.fileno(), 1)
        assert byte and len(line) < 4096, "missing or oversized COPY controller request"
        line.extend(byte)
        if byte == b"\n":
            break
    request = json.loads(line, object_pairs_hook=unique_fields)
    assert type(request) is dict and type(request.get("op")) is str, "invalid COPY controller request"
    return request


def copy_backend_closed(context, relay):
    assert type(relay.backend_pid) is int and relay.backend_pid > 0, "missing actual COPY backend identity"
    deadline = relay.copy_started_at + relay.copy_deadline_ms / 1000
    while time.monotonic() < deadline:
        if backend_evidence(context, relay.backend_pid) == [False, 0]:
            return
        time.sleep(0.01)
    raise AssertionError("COPY backend did not close before deadline")


def copy_main(case, phase):
    from copy_snapshot_fixture import COPY_HOLD_PHASES, copy_snapshot_receipt_path, validate_copy_snapshot_phase
    from source_snapshot_fault import copy_source_binding
    context = json.loads(Path(os.environ["RPGMIGRATE_TEST_COPY_SNAPSHOT_CONTEXT"]).read_text())
    receipt = copy_snapshot_receipt_path(context, "target", case, phase)
    assert_identity(context)
    receipt.touch(mode=0o600, exist_ok=False)
    held = released = closed = False
    native_path = stream = None
    holding = (case, phase) in COPY_HOLD_PHASES
    relay = ProtocolFault(context, "commit_hold" if holding else "copy_observe", copy_observer=True)

    def publish():
        wire = relay.completed_copy_observations() if closed else relay.copy_observations()
        value = {"owner": context["token"], "profile": "copy_snapshot", "side": "target",
                 "case": case, "phase": phase, "status": "observed" if closed else "pending",
                 "native_path": None if native_path is None else str(native_path), "stream": stream,
                 "held_fence_observed": held, "released_fence_observed": released, **wire}
        if closed:
            validate_copy_snapshot_phase(value, context, "target", case, phase, reaped=False)
        receipt.write_text(json.dumps(value))
        return value

    try:
        with relay:
            publish()
            print(json.dumps({"ready": True, "conninfo": conninfo(context, "e3_authority", "e1_target", str(relay.directory)),
                              "receipt": str(receipt)}), flush=True)
            while True:
                request = copy_request(relay)
                operation = request["op"]
                if operation == "arm":
                    assert set(request) == {"op", "native_path"}
                    native_path, stream = copy_source_binding(context, case, request["native_path"])
                    relay.arm()
                else:
                    assert set(request) == {"op"}
                    if operation in ("status", "release"):
                        if holding and relay.hits and relay.holding.is_set() and not relay.backend_closed.is_set():
                            alive, locks = backend_evidence(context, relay.backend_pid)
                            assert alive and locks > 0, "old COPY backend fence disappeared before release"
                            held = True
                        if operation == "release":
                            assert holding and held and not released, "COPY fence release is not authorized"
                            relay.release_backend()
                            copy_backend_closed(context, relay)
                            released = True
                    elif operation == "quit":
                        assert relay.armed.is_set() and type(relay.backend_pid) is int and relay.backend_pid > 0
                        assert not holding or (relay.hits == 1 and held and released), "COPY held backend was not released"
                        remaining = relay.copy_deadline_ms / 1000 - (time.monotonic() - relay.copy_started_at)
                        assert relay.client_closed.wait(timeout=max(0, remaining)), "COPY client closure was not observed"
                        break
                    else:
                        raise ValueError("unknown COPY controller operation")
                print(json.dumps({"op": operation, **publish()}), flush=True)
        copy_backend_closed(context, relay)
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
        from copy_snapshot_fixture import COPY_TARGET_PHASES
        if args.copy_case not in COPY_TARGET_PHASES or args.phase not in COPY_TARGET_PHASES[args.copy_case]:
            parser.error("unknown COPY target case or phase")
        copy_main(args.copy_case, args.phase)
        return
    if args.phase is not None:
        parser.error("--phase requires --copy-case")
    case = args.case
    context = json.loads(Path(os.environ["RPGMIGRATE_TEST_CONTEXT"]).read_text())
    assert_identity(context)
    receipt = Path(context["root"]) / f"authority-fault-{case}.json"
    receipt.touch(mode=0o600, exist_ok=False)
    held = released = False
    with ProtocolFault(context, "commit_hold") as relay:
        def record():
            observed = relay.hits == 1 and held and released and relay.backend_closed.is_set()
            return {"owner": context["token"], "case": case, "status": "observed" if observed else "pending", "fault_hits": relay.hits,
                    "backend_pid": relay.backend_pid, "commit_tag": "COMMIT" if relay.hits else None,
                    "holding_backend": relay.holding.is_set() and not relay.backend_closed.is_set(),
                    "backend_connection_closed": relay.backend_closed.is_set(),
                    "held_fence_observed": held, "released_fence_observed": released}

        print(json.dumps({"ready": True, "conninfo": conninfo(context, "e3_authority", "e1_target", str(relay.directory)),
                          "receipt": str(receipt)}), flush=True)
        try:
            for line in sys.stdin:
                request = json.loads(line)
                if set(request) != {"op"}:
                    raise ValueError("expected one fault controller operation")
                operation = request["op"]
                if operation == "arm":
                    relay.arm()
                elif operation in ("status", "release"):
                    if relay.hits and relay.holding.is_set() and not relay.backend_closed.is_set():
                        alive, locks = backend_evidence(context, relay.backend_pid)
                        assert alive and locks > 0, "old backend fence disappeared before controlled release"
                        held = True
                    if operation == "release":
                        assert held, "old backend fence was not observed before release"
                        relay.release_backend()
                        deadline = time.monotonic() + 5
                        while time.monotonic() < deadline:
                            if backend_evidence(context, relay.backend_pid) == [False, 0]:
                                released = True
                                break
                            time.sleep(0.02)
                        assert released, "old backend fence release was not observed"
                elif operation == "quit":
                    if relay.hits and not released:
                        raise ValueError("release the observed backend before quitting")
                else:
                    raise ValueError("unknown fault controller operation")
                value = record()
                receipt.write_text(json.dumps(value))
                print(json.dumps({"op": operation, **value}), flush=True)
                if operation == "quit":
                    break
        finally:
            receipt.write_text(json.dumps(record()))


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
