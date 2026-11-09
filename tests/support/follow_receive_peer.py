"""Owned loopback PostgreSQL protocol peer for receiver transport checks."""

if not __debug__:
    raise RuntimeError("receiver peer requires Python assertions")

import argparse
import json
import socket
import struct
import time


def message(kind, payload):
    return kind + struct.pack("!I", len(payload) + 4) + payload


def exact(connection, size):
    data = b""
    while len(data) < size:
        part = connection.recv(size - len(data))
        if not part:
            raise EOFError
        data += part
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", choices=("paused", "notice", "eof", "blocked", "follow", "follow_long",
                                                     "stop_idle", "stop_partial", "stop_error", "stop_extra", "stop_never"), required=True)
    args = parser.parse_args()
    with socket.socket() as listener:
        if args.case == "blocked":
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        print(json.dumps({"ready": True, "port": listener.getsockname()[1]}), flush=True)
        listener.settimeout(10)
        with listener.accept()[0] as connection:
            if args.case == "blocked":
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            connection.settimeout(90 if args.case == "follow_long" else 10)
            startup = exact(connection, struct.unpack("!I", exact(connection, 4))[0] - 4)
            assert startup[:4] == struct.pack("!I", 196608)
            assert b"replication\x00database\x00" in startup
            connection.sendall(message(b"R", struct.pack("!I", 0))
                               + message(b"S", b"server_version\x0018.6\x00")
                               + message(b"S", b"client_encoding\x00UTF8\x00")
                               + message(b"K", struct.pack("!II", 12345, 1))
                               + message(b"Z", b"I"))
            kind = exact(connection, 1)
            query = exact(connection, struct.unpack("!I", exact(connection, 4))[0] - 4)
            assert kind == b"Q" and query.startswith(b"START_REPLICATION SLOT ")
            connection.sendall(message(b"W", b"\x00\x00\x00"))
            if args.case == "eof":
                print(json.dumps({"status": "observed", "closed": True}), flush=True)
                return
            feedback_count = 0
            reply_requested_count = 0
            copy_done = False
            if args.case == "notice":
                kind = exact(connection, 1)
                payload = exact(connection, struct.unpack("!I", exact(connection, 4))[0] - 4)
                assert kind == b"d" and len(payload) == 34 and payload[:1] == b"r"
                assert struct.unpack("!QQQ", payload[1:25]) == (17, 17, 17)
                feedback_count += 1
                reply_requested_count += payload[33] == 1
                connection.sendall(message(b"N", b"SNOTICE\x00VNOTICE\x00C00000\x00Mprivate-peer-value\x00\x00"))
            if args.case.startswith("follow"):
                payloads = [b"B" + struct.pack("!QqI", 100, 123, 9),
                            b"R" + struct.pack("!I", 4242) + b"public\x00t\x00d" + struct.pack("!H", 1)
                            + b"\x01id\x00" + struct.pack("!Ii", 23, -1),
                            b"I" + struct.pack("!I", 4242) + b"N" + struct.pack("!H", 1) + b"t" + struct.pack("!I", 1) + b"1",
                            b"I" + struct.pack("!I", 4242) + b"N" + struct.pack("!H", 1) + b"t" + struct.pack("!I", 1) + b"2",
                            b"C\x00" + struct.pack("!QQq", 100, 120, 123)]
                frames = [message(b"d", b"w" + struct.pack("!QQq", 500, 9000, 0) + payload) for payload in payloads]
                frames.insert(3, message(b"d", b"k" + struct.pack("!QqB", 500, 0, 1)))
                frames.append(message(b"d", b"k" + struct.pack("!QqB", 777, 0, 1)))
                frame = b"".join(frames)
            elif args.case == "stop_partial":
                begin = b"B" + struct.pack("!QqI", 100, 123, 9)
                later = message(b"d", b"w" + struct.pack("!QQq", 500, 900, 0) + b"invalid-later-pgoutput")
                frame = message(b"d", b"w" + struct.pack("!QQq", 500, 900, 0) + begin) + later[:17]
            elif args.case.startswith("stop_"):
                frame = b""
            else:
                frame = message(b"d", b"w" + struct.pack("!QQq", 500, 900, 0) + b"partial-frame-body")
            try:
                parts = (frame,) if args.case.startswith("stop_") else (frame[:2], frame[2:17], frame[17:])
                for part in parts:
                    connection.sendall(part)
                    if not args.case.startswith("stop_"):
                        time.sleep(0.005)
            except (BrokenPipeError, ConnectionResetError):
                pass
            if args.case == "blocked":
                time.sleep(2)
            try:
                while True:
                    kind = exact(connection, 1)
                    payload = exact(connection, struct.unpack("!I", exact(connection, 4))[0] - 4)
                    if kind == b"X":
                        break
                    if kind == b"c":
                        assert not payload and not copy_done
                        assert args.case in ("follow", "blocked", "paused", "stop_idle", "stop_partial", "stop_error", "stop_extra", "stop_never")
                        copy_done = True
                        if args.case == "stop_never":
                            print(json.dumps({"copy_done_waiting": True, "positions_unchanged": True}), flush=True)
                            continue
                        if args.case == "stop_partial":
                            connection.sendall(later[17:])
                        if args.case == "stop_error":
                            terminal = message(b"E", b"SERROR\x00VERROR\x00CXX000\x00Mreceiver stop failed\x00\x00")
                        else:
                            terminal = (message(b"c", b"") + message(b"C", b"COPY 0\x00")
                                        + message(b"C", b"START_REPLICATION\x00"))
                            if args.case == "stop_extra":
                                terminal += message(b"C", b"COPY 0\x00")
                        connection.sendall(terminal + message(b"Z", b"I"))
                        continue
                    assert not copy_done
                    assert kind == b"d" and len(payload) == 34 and payload[:1] == b"r"
                    assert struct.unpack("!QQQ", payload[1:25]) == (17, 17, 17)
                    feedback_count += 1
                    reply_requested_count += payload[33] == 1
                    if args.case == "stop_idle" and payload[33] == 1 and reply_requested_count == 1:
                        print(json.dumps({"reply_requested": True, "positions_unchanged": True}), flush=True)
                    minimum = 8 if args.case == "follow_long" else 3
                    if args.case.startswith("follow") and feedback_count == minimum:
                        print(json.dumps({"blocked_status_count": minimum, "positions_unchanged": True}), flush=True)
            except (EOFError, ConnectionResetError):
                pass
            print(json.dumps({"status": "observed", "feedback_count": feedback_count,
                              "positions_unchanged": True, "closed": True, "copy_done": copy_done,
                              "reply_requested_count": reply_requested_count}), flush=True)


if __name__ == "__main__":
    main()
