"""Portable controls for the session-name census judgement and process-port mapping."""

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sessions

HARNESS = "rpgmigrate_m2_harness"
ENGINE_LINE = sessions.logline("rpgmigrate[100] compare")


def row(pid, name, port, kind="client backend", state="active", start="t0"):
    return {"pid": pid, "start": start, "type": kind, "name": name, "port": port, "state": state}


class JudgeTests(unittest.TestCase):
    phases = {100: "follow", 200: "compare-schema", 300: "cleanup"}
    ports = {5001: {"kind": "rpgmigrate", "pid": 100, "engine": 100},
             5002: {"kind": "pg_dump", "pid": 101, "engine": 100},
             5003: {"kind": "pg_dump", "pid": 201, "engine": 200}}

    def judge(self, rows, pinned=None):
        return sessions.judge("source", rows, self.ports, self.phases, HARNESS, pinned)

    def test_contract_names_cover_roles_children_and_walsenders(self):
        coverage, counts = self.judge([
            row(1, "rpgmigrate[100] copy", 5001), row(2, "rpgmigrate[100] copy", 5002),
            row(3, "rpgmigrate[100] receive", 5001, kind="walsender"), row(4, "", 5003, state="starting"),
            row(4, "rpgmigrate[200] copy", 5003), row(5, HARNESS, 9001), row(5, "", 9001),
            row(6, "rpgmigrate[300] receive", 9002)])
        self.assertTrue({("source", "follow", "copy", "pg_dump"), ("source", "follow", "receive", "walsender"),
                         ("source", "compare-schema", "copy", "pg_dump"), ("source", "cleanup", "receive")} <= coverage)
        self.assertEqual(counts, {"sessions": 5, "pre_startup_only": 0, "logged": 0, "joined": 4})

    def test_every_misnamed_engine_session_refuses(self):
        for rows in ([row(1, "", 5001)], [row(1, "", 9000, state="idle")],
                     [row(1, "rpgmigrate[100] copy", 5001), row(1, "rpgmigrate[100] apply", 5001)],
                     [row(1, "rpgmigrate[999] copy", 9000)], [row(1, "rpgmigrate[300] apply", 9000)],
                     [row(1, "rpgmigrate[100] copy", 5001, kind="walsender")],
                     [row(1, "rpgmigrate[100] receive", 5002)], [row(1, "rpgmigrate[200] copy", 5001)],
                     [row(1, "rpgmigrate[200] copy", 9000)], [row(1, "rpgmigrate[200] compare", 5001)],
                     [row(1, "rpgmigrate[100] copier", 5001)], [row(1, "pg_dump", 5002)]):
            with self.subTest(rows=rows), self.assertRaises(RuntimeError):
                self.judge(rows)

    def test_sampler_controls_pass_only_once_the_backend_is_named(self):
        for name, (script, _) in sessions.CONTROLS.items():
            rows = [row for sample in script for row in sample]
            with self.subTest(name=name):
                if name == "settles":
                    self.assertEqual(self.judge(rows)[0], {("source", "follow", "compare")})
                else:
                    self.assertRaises(RuntimeError, self.judge, rows)

    def test_vanished_window_backend_takes_only_its_one_logged_engine_name(self):
        window = row(7, "", 9000, state=None)
        def gone(*messages, start="t0"):
            return {"pid": 7, "start": start, "gone": True,
                    "logged": [{"user": "postgres", "database": "db", "message": message} for message in messages]}
        engine = "connection authorized: user=postgres database=db application_name=rpgmigrate[100] compare"
        coverage, counts = self.judge([window, gone(engine)])
        self.assertEqual((coverage, counts["logged"]), ({("source", "follow", "compare")}, 1))
        for rows in ([window], [window, gone()], [window, gone(engine, engine)], [window, gone(engine, start="t1")],
                     [window, gone("connection authorized: user=postgres database=db")],
                     [window, gone("connection authorized: user=postgres database=db application_name=")],
                     [window, gone("connection authorized: user=postgres database=db application_name=psql")],
                     [window, gone(engine.replace("rpgmigrate[100]", "rpgmigrate[999]"))],
                     [window, gone(engine + " SSL enabled (protocol=TLSv1.3, cipher=x, bits=256)")],
                     [window, gone(engine.replace("database=db", "database=other"))],
                     [window, {**gone(engine), "logged": None}],
                     [window, row(7, "", 9000, state="idle"), gone(engine)], [{**window, "type": "walsender"}, gone(engine)],
                     [window, gone(engine), row(8, "", 9001, state=None)]):
            with self.subTest(rows=rows), self.assertRaises(RuntimeError):
                self.judge(rows)
        harness = engine.replace("rpgmigrate[100] compare", HARNESS)
        self.assertEqual(self.judge([window, gone(harness)]), (set(), {"sessions": 0, "pre_startup_only": 0, "logged": 0, "joined": 0}))
        for rows in ([window, gone(harness, harness)], [window, gone(harness, start="t1")], [{**window, "type": "walsender"}, gone(harness)],
                     [window, row(7, "", 9000, state="idle"), gone(harness)]):
            with self.subTest(rows=rows), self.assertRaises(RuntimeError):
                self.judge(rows)
        with self.assertRaises(RuntimeError):
            self.judge([window, gone(engine)], pinned="pinned")
        self.assertEqual(self.judge([window, gone(engine.replace("rpgmigrate[100] compare", "pinned"))], pinned="pinned")[1]["logged"], 1)

    def test_census_logs_are_copied_whole_and_refused_when_absent_misnamed_or_secret(self):
        class Server:
            environment = {"PGPASSWORD": "fixture-secret"}
            def __init__(self, files):
                self.files = files
            def sql(self, name, sql):
                return json.dumps(self.files).encode()
        logged = {"postgresql.json": ENGINE_LINE + "\n", "postgresql.log": ""}
        with tempfile.TemporaryDirectory() as temporary, patch.object(sessions.clone, "DIAGNOSTIC_ROOT", Path(temporary)):
            sessions.retain_logs({"source": Server(logged), "target": Server(logged)})
            for side in ("source", "target"):
                self.assertEqual({path.name: path.read_text() for path in (Path(temporary) / "census-logs" / side).iterdir()}, logged)
            for files in ({}, {"postgresql.log": ""}, {"../postgresql.json": ""}, {".json": ""},
                          {"postgresql.json": "password=fixture-secret"}):
                with self.subTest(files=files), self.assertRaises(RuntimeError):
                    sessions.retain_logs({"source": Server(files)})

    def test_pinned_names_must_survive_on_every_session(self):
        coverage, _ = self.judge([row(1, "pinned", 5001), row(2, "pinned", 5002)], pinned="pinned")
        self.assertEqual(coverage, {("source", "follow", "rpgmigrate"), ("source", "follow", "pg_dump")})
        for name in ("rpgmigrate[100] copy", "pg_dump", "other"):
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                self.judge([row(1, "pinned", 5001), row(2, name, 5002)], pinned="pinned")

    def test_receipt_requires_every_variant_and_observation(self):
        receipt = {"mode": "session_names", "sampler_controls": {side: [*sessions.CONTROLS, "connection_log"] for side in ("source", "target")}, "cases": {
            variant: {"pinned": variant != "default", "counts": {"source": {"sessions": 1}, "target": {"sessions": 1}},
                      "observed": [list(item) for item in (sessions.REQUIRED if variant == "default" else sessions.REQUIRED_PINNED)]}
            for variant in sessions.VARIANTS}}
        sessions.validate_receipt(receipt)
        broken = {**receipt, "cases": {**receipt["cases"], "default": {**receipt["cases"]["default"], "observed": receipt["cases"]["default"]["observed"][1:]}}}
        missing = {**receipt, "cases": {key: value for key, value in receipt["cases"].items() if key != "conninfo"}}
        empty = {**receipt, "cases": {**receipt["cases"], "environment": {**receipt["cases"]["environment"], "counts": {"source": {"sessions": 0}}}}}
        uncontrolled = {**receipt, "sampler_controls": {"source": [*sessions.CONTROLS, "connection_log"], "target": list(sessions.CONTROLS)}}
        for value in (broken, missing, empty, uncontrolled, {key: item for key, item in receipt.items() if key != "sampler_controls"},
                      {**receipt, "mode": "smoke"}):
            with self.assertRaises(RuntimeError):
                sessions.validate_receipt(value)


class ProcessCensusTests(unittest.TestCase):
    def test_sockets_map_to_the_owning_engine_invocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            proc = Path(temporary)
            for pid, parent, exe, inode in ((10, 1, "rpgmigrate", 7001), (11, 10, "pg_restore", 7002), (12, 11, "pg_restore", 7003),
                                            (13, 1, "psql", 7004)):
                (proc / str(pid) / "fd").mkdir(parents=True)
                (proc / str(pid) / "stat").write_bytes(f"{pid} (x) S {parent} 0".encode())
                os.symlink("/usr/bin/" + exe, proc / str(pid) / "exe")
                os.symlink(f"socket:[{inode}]", proc / str(pid) / "fd" / "3")
            (proc / "net").mkdir()
            header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
            lines = [f"   0: 0100007F:{port:04X} {remote}:1538 {state} 0:0 00:0 0 0 0 {inode}" for port, remote, state, inode in
                     ((40001, "0200007F", "01", 7001), (40002, "0300007F", "01", 7002), (40001, "0300007F", "01", 7003),
                      (40004, "0200007F", "01", 7004), (40005, "0200007F", "0A", 7001))]
            (proc / "net/tcp").write_text(header + "\n".join(lines) + "\n")
            mapped = "0000000000000000FFFF00000400007F"
            (proc / "net/tcp6").write_text(header + f"   0: {mapped}:9C46 {mapped}:1538 01 0:0 00:0 0 0 0 7002\n")
            census = sessions.ProcessCensus(proc)
            census.sample()
            self.assertEqual(census.ports, {("127.0.0.2", 40001): {"kind": "rpgmigrate", "pid": 10, "engine": 10},
                                            ("127.0.0.3", 40002): {"kind": "pg_restore", "pid": 11, "engine": 10},
                                            ("127.0.0.3", 40001): {"kind": "pg_restore", "pid": 12, "engine": 10},
                                            ("127.0.0.4", 40006): {"kind": "pg_restore", "pid": 11, "engine": 10}})

    def test_addresses_decode_little_endian_words(self):
        self.assertEqual(sessions.address("0A580A0A"), "10.10.88.10")
        self.assertEqual(sessions.address("0000000000000000FFFF0000050A580A"), "10.88.10.5")
        self.assertEqual(sessions.address("B80D01200000000000000000FF000000"), "2001:db8::ff")


if __name__ == "__main__":
    unittest.main()
