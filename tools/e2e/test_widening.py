"""Portable controls for the widening fixture and its follow receipt."""

import copy
from pathlib import Path
import re
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import widening

# Each fact the receipt requires, and the fixture statement that must create it.
OBJECTS = {
    "domain_check": r"CREATE DOMAIN widening\.percent AS numeric CHECK \(",
    "expression_index": r"CREATE INDEX users_name_lower_idx ON widening\.users \(lower\(display_name\)\);",
    "partial_index": r"CREATE INDEX readings_recent_partial_idx ON widening\.readings \(captured_at\) WHERE quantity > 5;",
    "citext_unique": r"email citext NOT NULL,[^;]*CONSTRAINT users_email_key UNIQUE \(email\)",
    "generated": r"total_price numeric\(14,4\) GENERATED ALWAYS AS \(unit_price \* quantity\) STORED",
    "matview_populated": r"CREATE MATERIALIZED VIEW widening\.event_counts AS\s+SELECT [^;]+ FROM widening\.events",
}


def receipt(source, target):
    facts = widening.expected_facts(source)
    versions = {"source": "1.6" if source == 16 else "1.8", "target": "1.6" if target == 16 else "1.8"}
    compared = {relation: {"match": True, "written": True} for relation in widening.relations(source)}
    compared["widening.event_counts"] = {"match": True, "written": False, "refreshed_at_cutover": False}
    return {"pair": f"{source}-{target}", "mode": "widening", "cases": {
        "majors": {"source": source, "target": target},
        "follow": {"completed": True, "schema_matched": True, "engine_data_differences": 0,
                   "engine_data": sorted([*widening.relations(source), "M2 space.rows"]),
                   "facts": {side: {**copy.deepcopy(facts), "citext_version": versions[side]} for side in ("source", "target")},
                   "compared": compared},
        "full_generated_refusal": ({"applies": True, "refused": True, "resources_unchanged": True, "source_writable": True}
                                   if source == 18 else {"applies": False})}}


class FixtureTests(unittest.TestCase):
    def test_fixture_creates_every_required_object(self):
        source = widening.FIXTURE.read_text()
        self.assertEqual(set(OBJECTS), set(widening.expected_facts(16)) - {"full_identity"})
        for name, pattern in OBJECTS.items():
            with self.subTest(object=name):
                self.assertRegex(source, pattern)
        self.assertRegex(widening.FULL_GENERATED, r"GENERATED ALWAYS AS \([^)]*\) STORED\);ALTER TABLE widening\.full_generated REPLICA IDENTITY FULL;")
        self.assertRegex(widening.VIRTUAL, r"ADD COLUMN discounted .* VIRTUAL;$")
        self.assertNotIn("REFRESH", source + widening.STREAMING)

    def test_streaming_writes_every_followed_table(self):
        for major, statements in ((16, widening.STREAMING + widening.STREAMING_FULL), (18, widening.STREAMING)):
            written = set(re.findall(r"(?:INSERT INTO|UPDATE|DELETE FROM) (widening\.\w+)", statements))
            with self.subTest(major=major):
                self.assertEqual(written, set(widening.relations(major)))


class ReceiptTests(unittest.TestCase):
    def test_every_pair_validates(self):
        for source, target in ((16, 16), (18, 18), (16, 18)):
            with self.subTest(pair=(source, target)):
                widening.validate_receipt(receipt(source, target))

    def test_incomplete_or_misstated_runs_refuse(self):
        follow = lambda r: r["cases"]["follow"]
        mutations = {
            "mode": lambda r: r.update(mode="session_timeouts"),
            "pair": lambda r: r.update(pair="16-18"),
            "refusal case": lambda r: r["cases"].pop("full_generated_refusal"),
            "not completed": lambda r: follow(r).update(completed=False),
            "schema": lambda r: follow(r).update(schema_matched=False),
            "data difference": lambda r: follow(r).update(engine_data_differences=1),
            "engine omits table": lambda r: follow(r)["engine_data"].remove("widening.readings"),
            "partial index": lambda r: follow(r)["facts"]["target"].update(partial_index=False),
            "domain check": lambda r: follow(r)["facts"]["target"].update(domain_check=0),
            "opclass": lambda r: follow(r)["facts"]["source"].update(citext_unique=None),
            "matview": lambda r: follow(r)["facts"]["target"].update(matview_populated=False),
            "virtual": lambda r: follow(r)["facts"]["target"]["generated"].update({"widening.readings.discounted": "s"}),
            "no version": lambda r: follow(r)["facts"]["target"].pop("citext_version"),
            "same-major version change": lambda r: follow(r)["facts"]["target"].update(citext_version="1.9"),
            "row differs": lambda r: follow(r)["compared"]["widening.users"].update(match=False),
            "not written": lambda r: follow(r)["compared"]["widening.events"].update(written=False),
            "view refreshed": lambda r: follow(r)["compared"]["widening.event_counts"].update(refreshed_at_cutover=True),
            "view compared": lambda r: follow(r)["compared"].pop("widening.event_counts"),
            "refusal resources": lambda r: r["cases"]["full_generated_refusal"].update(resources_unchanged=False),
            "refusal skipped": lambda r: r["cases"].update(full_generated_refusal={"applies": False}),
        }
        good = receipt(18, 18)
        for name, mutate in mutations.items():
            changed = copy.deepcopy(good)
            mutate(changed)
            with self.subTest(mutation=name), self.assertRaises(RuntimeError):
                widening.validate_receipt(changed)

    def test_cross_major_requires_the_update_path_and_16_sources_the_full_table(self):
        mutations = {
            "same citext version": lambda r: r["cases"]["follow"]["facts"]["target"].update(citext_version="1.6"),
            "full table absent": lambda r: r["cases"]["follow"]["compared"].pop("widening.full_generated"),
            "full identity lost": lambda r: r["cases"]["follow"]["facts"]["target"].update(full_identity=[]),
            "refusal claimed": lambda r: r["cases"].update(full_generated_refusal={"applies": True, "refused": True}),
        }
        for name, mutate in mutations.items():
            changed = receipt(16, 18)
            mutate(changed)
            with self.subTest(mutation=name), self.assertRaises(RuntimeError):
                widening.validate_receipt(changed)


if __name__ == "__main__":
    unittest.main()
