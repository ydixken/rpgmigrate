"""Portable controls for the session timeout run's receipt and idle hold."""

import copy
from pathlib import Path
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import clone
import timeouts


def receipt(majors):
    cases = {}
    for name, value, minimum in timeouts.SETTINGS:
        applied = timeouts.expected_sides(majors, minimum)
        cases[name] = {"value": value, "majors": dict(majors), "applied": applied}
        if applied:
            cases[name].update(completed=True, tables=6, held_seconds=timeouts.HOLD_SECONDS + 0.01,
                               fatal_sessions={"source": 0, "target": 0})
    return {"mode": "session_timeouts", "cases": cases}


class Worker:
    def __init__(self, exits_after=None):
        self.polls, self.exits_after = 0, exits_after

    def poll(self):
        self.polls += 1
        return 1 if self.exits_after is not None and self.polls > self.exits_after else None


class ReceiptTests(unittest.TestCase):
    def test_every_pair_validates_and_records_where_each_setting_applied(self):
        for majors in ({"source": 16, "target": 16}, {"source": 18, "target": 18}, {"source": 16, "target": 18}):
            with self.subTest(majors=majors):
                timeouts.validate_receipt(receipt(majors))
        self.assertEqual(receipt({"source": 16, "target": 18})["cases"]["transaction_timeout"]["applied"], ["target"])
        self.assertEqual(receipt({"source": 16, "target": 16})["cases"]["transaction_timeout"]["applied"], [])

    def test_incomplete_or_misstated_runs_refuse(self):
        good = receipt({"source": 18, "target": 18})
        mutations = [
            lambda r: r.update(mode="session_names"),
            lambda r: r["cases"].pop("lock_timeout"),
            lambda r: r["cases"]["statement_timeout"].update(value="0"),
            lambda r: r["cases"]["transaction_timeout"].update(applied=["target"]),
            lambda r: r["cases"]["idle_session_timeout"].update(applied=[]),
            lambda r: r["cases"]["idle_session_timeout"]["majors"].update(source=17),
            lambda r: r["cases"]["lock_timeout"].pop("completed"),
            lambda r: r["cases"]["lock_timeout"].update(tables=0),
            lambda r: r["cases"]["idle_in_transaction_session_timeout"].update(held_seconds=0.5),
            lambda r: r["cases"]["transaction_timeout"].update(fatal_sessions={"source": 0, "target": 1}),
            lambda r: r["cases"]["statement_timeout"].pop("fatal_sessions"),
        ]
        for index, mutate in enumerate(mutations):
            changed = copy.deepcopy(good)
            mutate(changed)
            with self.subTest(index=index), self.assertRaises(RuntimeError):
                timeouts.validate_receipt(changed)


class HarnessOptionsTests(unittest.TestCase):
    def test_harness_sessions_clear_every_setting_the_server_has(self):
        self.assertEqual(timeouts.harness_options(16), " options='-c statement_timeout=0 -c lock_timeout=0 "
                         "-c idle_in_transaction_session_timeout=0 -c idle_session_timeout=0'")
        self.assertEqual(timeouts.harness_options(18), timeouts.harness_options(16)[:-1] + " -c transaction_timeout=0'")

    def test_engine_conninfo_never_carries_the_harness_options(self):
        database = clone.Database("source", Path("/bin"), {})
        database.harness_options = timeouts.harness_options(18)
        self.assertNotIn("options", database.connection("x"))
        self.assertNotIn("options", database.operator_uri("x", "source"))


class FreshSession:
    """Answers each psql call with the next (returncode, stderr); refuses calls past the script."""

    def __init__(self, *answers):
        self.answers, self.calls = list(answers), []

    def __call__(self, command, **kwargs):
        self.calls.append((command, kwargs))
        if not self.answers:
            raise AssertionError("fresh-session check ran past its bound")
        returncode, stderr = self.answers.pop(0)
        return subprocess.CompletedProcess(command, returncode, b"20ms\n" if returncode == 0 else b"", stderr)


IDLE = (2, b"FATAL:  57P05\nserver closed the connection unexpectedly\nconnection to server was lost\n")


class FreshSettingTests(unittest.TestCase):
    def setUp(self):
        self.database = clone.Database("source", Path("/bin"), {})
        self.database.harness_options = timeouts.harness_options(18)

    def test_one_fresh_session_without_options_reads_the_value(self):
        session = FreshSession((0, b""))
        self.assertEqual(timeouts.fresh_setting(self.database, "x", "idle_session_timeout", run=session), "20ms")
        command, kwargs = session.calls[0]
        self.assertNotIn("options", " ".join(command))
        self.assertIn("VERBOSITY=sqlstate", command)
        self.assertEqual(kwargs["input"], b"SELECT pg_catalog.current_setting('idle_session_timeout');")

    def test_only_the_idle_session_fatal_is_retried(self):
        session = FreshSession(IDLE, IDLE, (0, b""))
        self.assertEqual(timeouts.fresh_setting(self.database, "x", "idle_session_timeout", run=session), "20ms")
        self.assertEqual(len(session.calls), 3)
        for stderr in (b"ERROR:  57014\n", b"FATAL:  57P01\nconnection to server was lost\n", b"FATAL:  57P055\n",
                       b"psql: error: connection to server failed\n", b"", b"FATAL:  terminating connection due to idle-session timeout\n"):
            session = FreshSession((2, stderr), (0, b""))
            with self.subTest(stderr=stderr), self.assertRaises(RuntimeError):
                timeouts.fresh_setting(self.database, "x", "idle_session_timeout", run=session)
            self.assertEqual(len(session.calls), 1)

    def test_retries_are_bounded_and_never_read_as_success(self):
        session = FreshSession(*[IDLE] * timeouts.FRESH_ATTEMPTS)
        with self.assertRaisesRegex(RuntimeError, "ended every fresh session"):
            timeouts.fresh_setting(self.database, "x", "idle_session_timeout", run=session)
        self.assertEqual(len(session.calls), timeouts.FRESH_ATTEMPTS)


class HoldTests(unittest.TestCase):
    def test_hold_measures_the_span_and_refuses_an_exited_worker(self):
        ticks = iter(range(100))
        self.assertGreaterEqual(timeouts.hold(Worker(), 3, clock=lambda: next(ticks)), 3)
        ticks = iter(range(100))
        with self.assertRaises(RuntimeError):
            timeouts.hold(Worker(exits_after=1), 3, clock=lambda: next(ticks))


if __name__ == "__main__":
    unittest.main()
