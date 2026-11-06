"""Drop one observed COMMIT reply while retaining its owned backend fence."""

import argparse
import json
import os
from pathlib import Path
import signal
import sys
import time

from db_harness import assert_identity, conninfo, psql
from protocol_fault import ProtocolFault

FAULT_CASES = ("lost_commit_reply", "provisioning_recovery")


def backend_evidence(context, pid):
    assert type(pid) is int and pid > 0, "missing observed backend identity"
    return json.loads(psql(context, "e3_authority", f"SELECT json_build_array(EXISTS(SELECT FROM pg_stat_activity WHERE pid={pid}), (SELECT count(*) FROM pg_locks WHERE pid={pid} AND locktype='advisory' AND granted));"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=FAULT_CASES, required=True)
    case = parser.parse_args().case
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
