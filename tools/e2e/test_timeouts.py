"""Portable controls for the session timeout run's receipt and idle hold."""

import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
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


class HoldTests(unittest.TestCase):
    def test_hold_measures_the_span_and_refuses_an_exited_worker(self):
        ticks = iter(range(100))
        self.assertGreaterEqual(timeouts.hold(Worker(), 3, clock=lambda: next(ticks)), 3)
        ticks = iter(range(100))
        with self.assertRaises(RuntimeError):
            timeouts.hold(Worker(exits_after=1), 3, clock=lambda: next(ticks))


if __name__ == "__main__":
    unittest.main()
