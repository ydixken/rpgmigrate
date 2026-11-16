"""Follows the M8 widening objects through endpos, then compares each one and checks the 18 FULL refusal."""

import json

import clone
import sessions

FIXTURE = clone.ROOT / "tests/fixtures/follow/widening.sql"
# PostgreSQL 18 fails the source's own UPDATE and DELETE on this table once a publication omits `doubled` (decision cl).
FULL_GENERATED = ("CREATE TABLE widening.full_generated (id integer, quantity integer, doubled integer GENERATED ALWAYS AS (quantity * 2) STORED);"
                  "ALTER TABLE widening.full_generated REPLICA IDENTITY FULL;"
                  "INSERT INTO widening.full_generated (id, quantity) VALUES (1, 1), (1, 1), (2, NULL);")
VIRTUAL = "ALTER TABLE widening.readings ADD COLUMN discounted numeric GENERATED ALWAYS AS (unit_price * 0.9) VIRTUAL;"
STREAMING = """INSERT INTO widening.users VALUES (3, 'Carol@Example.COM', 'Carol', 75);
UPDATE widening.users SET email = 'ALICE@example.com', quota = 100 WHERE id = 1;
UPDATE widening.users SET display_name = 'Robert' WHERE id = 2;
INSERT INTO widening.readings (id, unit_price, quantity, captured_at) VALUES (3, 2.5, 8, '2026-01-03');
UPDATE widening.readings SET quantity = 9 WHERE id = 1;
DELETE FROM widening.readings WHERE id = 2;
INSERT INTO widening.events VALUES (4, '2026-01-02', 'warn');"""
STREAMING_FULL = """UPDATE widening.full_generated SET quantity = 5 WHERE ctid = (SELECT ctid FROM widening.full_generated WHERE id = 1 LIMIT 1);
DELETE FROM widening.full_generated WHERE id = 2;"""
REFUSAL = "follow-preflight: PostgreSQL 18 refuses UPDATE and DELETE on a REPLICA IDENTITY FULL table"
RESOURCES = "SELECT slot_name,plugin,database,active FROM pg_replication_slots ORDER BY slot_name; SELECT pubname,oid FROM pg_publication ORDER BY pubname;"
FACTS = """SELECT json_build_object(
 'domain_check', (SELECT count(*) FROM pg_constraint WHERE contypid = 'widening.percent'::regtype AND contype = 'c' AND convalidated),
 'expression_index', (SELECT indexprs IS NOT NULL FROM pg_index WHERE indexrelid = 'widening.users_name_lower_idx'::regclass),
 'partial_index', (SELECT indpred IS NOT NULL FROM pg_index WHERE indexrelid = 'widening.readings_recent_partial_idx'::regclass),
 'citext_unique', (SELECT e.extname FROM pg_index i JOIN pg_depend d ON d.classid = 'pg_opclass'::regclass AND d.objid = i.indclass[0] AND d.deptype = 'e'
                   JOIN pg_extension e ON e.oid = d.refobjid WHERE i.indexrelid = 'widening.users_email_key'::regclass AND i.indisunique),
 'generated', (SELECT json_object_agg(attrelid::regclass::text || '.' || attname, attgenerated) FROM pg_attribute
               WHERE attrelid IN (SELECT oid FROM pg_class WHERE relnamespace = 'widening'::regnamespace) AND attgenerated <> ''),
 'full_identity', (SELECT coalesce(json_agg(oid::regclass::text ORDER BY relname), '[]') FROM pg_class
                   WHERE relnamespace = 'widening'::regnamespace AND relkind = 'r' AND relreplident = 'f'),
 'matview_populated', (SELECT relispopulated FROM pg_class WHERE oid = 'widening.event_counts'::regclass),
 'citext_version', (SELECT extversion FROM pg_extension WHERE extname = 'citext'));"""
ROWS = "SELECT coalesce(json_agg(t ORDER BY t::text), '[]') FROM {} t;"
# The view keeps its copy-snapshot content: nobody refreshes it, on either side, after the copy.
AGGREGATE = "SELECT coalesce(json_agg(t ORDER BY t::text), '[]') FROM (SELECT occurred_on AS day, severity, count(*) AS n FROM widening.events GROUP BY 1, 2) t;"


def relations(source_major):
    return ["widening.users", "widening.readings", "widening.events", *(["widening.full_generated"] if source_major < 18 else [])]


def expected_facts(source_major):
    generated = {"widening.readings.total_price": "s"}
    generated.update({"widening.full_generated.doubled": "s"} if source_major < 18 else {"widening.readings.discounted": "v"})
    return {"domain_check": 1, "expression_index": True, "partial_index": True, "citext_unique": "citext", "generated": generated,
            "full_identity": ["widening.full_generated"] if source_major < 18 else [], "matview_populated": True}


def rows(database, name, source_major):
    return {relation: json.loads(database.sql(name, ROWS.format(relation))) for relation in [*relations(source_major), "widening.event_counts"]}


def follow_case(source, target, case, launch, driver):
    srcname, dstname, src, dst, state = case("widening")
    source.sql(srcname, FIXTURE.read_text() + (FULL_GENERATED if source.major < 18 else VIRTUAL))
    before = rows(source, srcname, source.major)
    phases = {}
    worker = launch(src, dst, state, source_name=srcname, target_name=dstname)
    phases[worker.pid] = "follow"
    driver.ready(worker)
    source.sql(srcname, "BEGIN;\n" + STREAMING + ("\n" + STREAMING_FULL if source.major < 18 else "") + "\nCOMMIT;")
    end = source.sql(srcname, "SELECT pg_current_wal_insert_lsn();").decode().strip()
    if sessions.engine(driver, state, "endpos", phases, ["stream", "sentinel", "set", "endpos", end, "--target", dst]) != (end + "\n").encode():
        raise RuntimeError("endpos command did not persist the sampled end")
    import cutover
    cutover.finish_completed(driver, worker)
    data = json.loads(sessions.engine(driver, state, "compare-data", phases, ["compare", "data", "--source", src, "--target", dst, "--json"]))
    schema = json.loads(sessions.engine(driver, state, "compare-schema", phases, ["compare", "schema", "--source", src, "--target", dst]))
    after, followed = rows(source, srcname, source.major), rows(target, dstname, source.major)
    facts = {side: json.loads(database.sql(name, FACTS)) for side, database, name in (("source", source, srcname), ("target", target, dstname))}
    compared = {relation: {"match": followed[relation] == after[relation], "written": after[relation] != before[relation]} for relation in after}
    compared["widening.event_counts"]["refreshed_at_cutover"] = followed["widening.event_counts"] == json.loads(target.sql(dstname, AGGREGATE))
    sessions.engine(driver, state, "cleanup", phases, ["stream", "cleanup", "--source", src, "--target", dst])
    return {"completed": True, "engine_data": sorted(str(item.get("schema")) + "." + str(item.get("name")) for item in data if item.get("status") == "match"),
            "engine_data_differences": sum(item.get("status") != "match" for item in data), "schema_matched": schema.get("matched") is True,
            "facts": facts, "compared": compared}


def refusal_case(source, target, case, launch, driver):
    if source.major < 18:
        return {"applies": False}
    srcname, dstname, src, dst, state = case("widening_full_refusal")
    source.sql(srcname, FIXTURE.read_text() + FULL_GENERATED)
    resources, catalog = source.sql(srcname, RESOURCES), target.catalog_signature(dstname)
    driver.finish(launch(src, dst, state, source_name=srcname, target_name=dstname), success=False, reason=REFUSAL)
    unchanged = resources == source.sql(srcname, RESOURCES) and catalog == target.catalog_signature(dstname)
    # No publication covers the table, so the source's own UPDATE and DELETE still work.
    source.sql(srcname, "UPDATE widening.full_generated SET quantity = quantity; DELETE FROM widening.full_generated WHERE id = 2;")
    return {"applies": True, "refused": True, "resources_unchanged": unchanged, "source_writable": True}


def run(source, target, case, launch, driver):
    for database in (source, target):
        database.major = int(database.sql("postgres", "SHOW server_version_num;").strip()) // 10000
    return {"majors": {"source": source.major, "target": target.major},
            "follow": follow_case(source, target, case, launch, driver),
            "full_generated_refusal": refusal_case(source, target, case, launch, driver)}


def validate_receipt(report):
    cases = report.get("cases")
    if report.get("mode") != "widening" or not isinstance(cases, dict) or sorted(cases) != ["follow", "full_generated_refusal", "majors"]:
        raise RuntimeError("widening receipt lacks its cases")
    majors = cases["majors"]
    if (not isinstance(majors, dict) or sorted(majors) != ["source", "target"] or any(type(major) is not int or major not in (16, 18) for major in majors.values())
            or report.get("pair") != f"{majors['source']}-{majors['target']}"):
        raise RuntimeError("widening receipt misstates its server majors")
    follow = cases["follow"]
    if follow.get("completed") is not True or follow.get("schema_matched") is not True or follow.get("engine_data_differences") != 0:
        raise RuntimeError("widening follow did not complete with matching engine comparisons")
    if not set(relations(majors["source"])) <= set(follow.get("engine_data", [])):
        raise RuntimeError("engine data comparison omits a widening table")
    facts = follow.get("facts", {})
    for side in ("source", "target"):
        observed = dict(facts.get(side) or {})
        version = observed.pop("citext_version", None)
        if observed != expected_facts(majors["source"]) or not isinstance(version, str) or not version:
            raise RuntimeError(f"{side} widening objects differ from the fixture")
    # On 16 to 18 the target's citext is newer, so the UNIQUE opclass is admitted only through its update path.
    if (facts["source"]["citext_version"] == facts["target"]["citext_version"]) != (majors["source"] == majors["target"]):
        raise RuntimeError("widening citext versions do not exercise the expected admission rule")
    compared = follow.get("compared", {})
    if sorted(compared) != sorted([*relations(majors["source"]), "widening.event_counts"]) or any(item.get("match") is not True for item in compared.values()):
        raise RuntimeError("a widening object differs after cutover")
    if any(compared[relation].get("written") is not True for relation in relations(majors["source"])):
        raise RuntimeError("a widening table was not written during streaming")
    view = compared["widening.event_counts"]
    if view.get("written") is not False or view.get("refreshed_at_cutover") is not False:
        raise RuntimeError("the materialized view does not keep its copy-snapshot content")
    refusal = cases["full_generated_refusal"]
    expected = ({"applies": True, "refused": True, "resources_unchanged": True, "source_writable": True} if majors["source"] == 18
                else {"applies": False})
    if refusal != expected:
        raise RuntimeError("the 18 source FULL table with a generated column was not refused before any resource")
