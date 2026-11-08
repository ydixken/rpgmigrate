"""Check the actual executable against independently owned PostgreSQL fixtures."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from db_harness import (FIXTURES, assert_equal, assert_identity, clean_environment,
                        conninfo, databases, fingerprint, inventory, load_manifest, psql,
                        reference_copy, validate_report, validate_success_output)
from protocol_fault import ProtocolFault

TABLE = '"E1 space"."row ""payload"'
EMPTY = '"E1 space"."empty table"'


class Cases:
    def __init__(self, binary, context, manifest):
        self.binary = str(Path(binary).resolve(strict=True))
        self.context = context
        self.manifest = manifest
        self.environment = clean_environment(os.environ)
        self.environment["HOME"] = str(Path(context["root"]) / "home")
        self.environment["TMPDIR"] = context["root"]
        self.result = {}
        assert_identity(context)

    def command(self, source, target, *, source_socket=None, target_socket=None, ping=False):
        source_info = conninfo(self.context, source, "e1_source", source_socket)
        target_info = conninfo(self.context, target, "e1_target", target_socket)
        args = [self.binary] + (["ping"] if ping else ["copy", "table-data"])
        args += ["--source", source_info, "--target", target_info]
        if not ping:
            args += ["--dir", str(self.state)]
        return args

    def invoke(self, source, target, **options):
        result = subprocess.run(self.command(source, target, **options), env=self.environment, capture_output=True,
                                text=True, encoding="utf-8", timeout=45)
        self.result["returncode"] = result.returncode
        # Synthetic content canaries must not appear in runtime diagnostics.
        for value in ("Grüße 日本語", "e1-wide-seed"):
            assert value not in result.stdout + result.stderr, "row content leaked in diagnostics"
        return result

    def no_completion(self):
        path = self.state / "manifest.json"
        if not path.exists():
            return
        value = json.loads(path.read_text())
        assert value["magic"] == "RPGMIGRATE" and value["format_version"] == 4
        assert value["target_provenance"] == [], "COPY receipt contains target authority provenance"
        assert value["source_slot"] is None, "COPY receipt contains source slot provenance"
        assert value["copy_snapshot"] is None, "COPY receipt contains a snapshot anchor intent"
        assert value["copy_complete"] is None, "completion published after failure"

    def completed(self, tables, rows, output, min_bytes=1):
        value = json.loads((self.state / "manifest.json").read_text())
        assert value["magic"] == "RPGMIGRATE" and value["format_version"] == 4
        assert value["target_provenance"] == [], "COPY receipt contains target authority provenance"
        assert value["source_slot"] is None, "COPY receipt contains source slot provenance"
        assert value["copy_snapshot"] is None, "COPY receipt contains a snapshot anchor intent"
        assert len(value["run"]) == 16
        assert Path(value["canonical_path"]) == self.state.resolve()
        receipt = value["copy_complete"]
        assert receipt["tables"] == tables and receipt["rows"] == rows, "incorrect completion totals"
        assert receipt["bytes"] >= min_bytes if rows else receipt["bytes"] == 0
        validate_success_output(output, value)

    def fail(self, result):
        assert result.returncode != 0, "required failure returned zero"
        self.no_completion()

    def empty_target(self, database):
        count = int(psql(self.context, database, f"SELECT count(*) FROM {TABLE};"))
        assert count == 0, "failed table left committed target rows"

    def run(self, case):
        case_id = case["id"]
        self.result = {"id": case_id, "status": "failed"}
        self.state = Path(self.context["root"]) / ("state-" + case_id)
        if case_id in ("copy_deferrable_fk", "preflight_nondeferrable_fk", "preflight_fk_external_parent",
                       "preflight_fk_mixed_types", "preflight_fk_disabled_triggers",
                       "preflight_fk_replica_triggers", "fault_deferrable_fk"):
            self.foreign_keys(case_id)
        elif case_id == "empty_catalog":
            source, target = databases(self.context, case_id, empty=True)
            before = inventory(self.context, target)
            assert before == {"schemas": ["public"], "relations": []}
            result = self.invoke(source, target)
            assert result.returncode == 0, "verified empty catalog failed"
            assert inventory(self.context, target) == before
            self.completed(0, 0, result.stdout)
        else:
            encodings = {"preflight_source_non_utf8": ("SQL_ASCII", "UTF8"),
                         "preflight_target_non_utf8": ("UTF8", "SQL_ASCII")}
            source, target = databases(self.context, case_id, encoding=encodings.get(case_id, "UTF8"))
            if case_id == "ping":
                assert self.invoke(source, target, ping=True).returncode == 0, "ping failed"
            elif case_id == "ping_unreachable":
                missing = Path(self.context["socket_dir"]) / "absent"
                self.fail(self.invoke(source, target, target_socket=str(missing), ping=True))
            elif case_id == "copy_contents":
                fingerprint(self.context, target, self.manifest, empty=True)
                source_settings = psql(self.context, source, "SHOW DateStyle; SHOW IntervalStyle;", role="e1_source", canonical=False)
                target_settings = psql(self.context, target, "SHOW DateStyle; SHOW IntervalStyle;", role="e1_target", canonical=False)
                assert source_settings != target_settings, "opposing session defaults absent"
                largest = int(psql(self.context, source, f"SELECT max(octet_length(wide)) FROM {TABLE};"))
                assert largest > 8 * 1024 * 1024, "fixture lacks oversize row"
                result = self.invoke(source, target)
                assert result.returncode == 0, "COPY failed"
                self.result["fingerprints"] = assert_equal(self.context, source, target, self.manifest)
                self.completed(2, 10003, result.stdout, min_bytes=8 * 1024 * 1024)
            elif case_id.startswith("oracle_"):
                self.oracle_control(case_id, source, target)
            elif case_id.startswith("preflight_"):
                self.preflight(case_id, source, target)
            elif case_id == "fault_target_constraint":
                self.unique_fault(source, target)
            elif case_id in ("fault_source_terminal", "fault_commit_unknown"):
                self.protocol_fault(case_id, source, target)
            elif case_id in ("signal_sigint", "signal_sigterm"):
                self.signal_fault(case_id, source, target)
            else:
                raise AssertionError("unimplemented manifest case")
        self.result["status"] = "passed"
        return self.result

    def oracle_control(self, case_id, source, target):
        reference_copy(self.context, source, target, self.manifest)
        assert_equal(self.context, source, target, self.manifest)
        sql = {
            "oracle_missing_table": f"DROP TABLE {EMPTY};",
            "oracle_changed_schema": f"ALTER TABLE {TABLE} ALTER COLUMN bucket TYPE bigint;",
            "oracle_changed_value": f"UPDATE {TABLE} SET \"text value\" = 'changed' WHERE ctid = (SELECT ctid FROM {TABLE} LIMIT 1);",
        }[case_id]
        psql(self.context, target, sql)
        try:
            assert_equal(self.context, source, target, self.manifest)
        except AssertionError:
            self.result["oracle_rejected"] = True
            return
        raise AssertionError("oracle accepted deliberately corrupted target")

    def preflight(self, case_id, source, target):
        database = source if "source" in case_id or case_id == "preflight_generated_column" else target
        if case_id.endswith("_rls"):
            sql = f"ALTER TABLE {TABLE} ENABLE ROW LEVEL SECURITY;"
        elif case_id.endswith("_trigger"):
            sql = f"CREATE FUNCTION \"E1 space\".noop() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$; CREATE TRIGGER user_hook BEFORE INSERT ON {TABLE} FOR EACH ROW EXECUTE FUNCTION \"E1 space\".noop();"
        elif case_id == "preflight_target_check_delete":
            for name, role in ((source, "e1_source"), (target, "e1_target")):
                psql(self.context, name, 'CREATE TABLE "E1 space"."zz check sink" (n integer);', role=role)
            psql(self.context, source, 'INSERT INTO "E1 space"."zz check sink" VALUES (1);')
            sql = f"CREATE FUNCTION \"E1 space\".delete_copied_rows(integer) RETURNS boolean LANGUAGE plpgsql AS $$BEGIN DELETE FROM {TABLE}; RETURN true; END$$; ALTER TABLE \"E1 space\".\"zz check sink\" ADD CHECK (\"E1 space\".delete_copied_rows(n));"
        elif case_id == "preflight_target_expression_index":
            sql = f"CREATE FUNCTION \"E1 space\".mutating_index(integer) RETURNS integer LANGUAGE plpgsql IMMUTABLE AS $$BEGIN PERFORM set_config('client_encoding','LATIN1',false); RETURN $1; END$$; CREATE INDEX e1_expression ON {TABLE} ((\"E1 space\".mutating_index(bucket)));"
        elif case_id == "preflight_target_custom_opclass":
            sql = f"CREATE OPERATOR CLASS \"E1 space\".e1_int4_ops FOR TYPE integer USING btree AS OPERATOR 1 < (integer,integer), OPERATOR 2 <= (integer,integer), OPERATOR 3 = (integer,integer), OPERATOR 4 >= (integer,integer), OPERATOR 5 > (integer,integer), FUNCTION 1 pg_catalog.btint4cmp(integer,integer); CREATE INDEX e1_custom_ops ON {TABLE} (bucket \"E1 space\".e1_int4_ops);"
        elif case_id == "preflight_target_custom_am":
            sql = f"CREATE ACCESS METHOD e1_brin TYPE INDEX HANDLER pg_catalog.brinhandler; CREATE OPERATOR CLASS \"E1 space\".e1_am_ops DEFAULT FOR TYPE integer USING e1_brin AS OPERATOR 1 < (integer,integer), OPERATOR 2 <= (integer,integer), OPERATOR 3 = (integer,integer), OPERATOR 4 >= (integer,integer), OPERATOR 5 > (integer,integer), FUNCTION 1 pg_catalog.brin_minmax_opcinfo(internal), FUNCTION 2 pg_catalog.brin_minmax_add_value(internal,internal,internal,internal), FUNCTION 3 pg_catalog.brin_minmax_consistent(internal,internal,internal), FUNCTION 4 pg_catalog.brin_minmax_union(internal,internal,internal); CREATE INDEX e1_custom_am ON {TABLE} USING e1_brin (bucket);"
        elif case_id == "preflight_target_replica":
            psql(self.context, "postgres", f"ALTER ROLE e1_target IN DATABASE {target} SET session_replication_role='replica';")
            assert psql(self.context, target, "SHOW session_replication_role;", role="e1_target", canonical=False) == "replica"
            sql = "SELECT 1;"
        else:
            sql = {
                "preflight_missing_table": f"DROP TABLE {TABLE};",
                "preflight_changed_schema": f"ALTER TABLE {TABLE} ALTER COLUMN bucket TYPE bigint;",
                "preflight_nonempty_target": f"INSERT INTO {TABLE}(bucket) VALUES (777);",
                "preflight_source_unlogged": f"ALTER TABLE {TABLE} SET UNLOGGED;",
                "preflight_target_unlogged": f"ALTER TABLE {TABLE} SET UNLOGGED;",
                "preflight_source_partition": 'CREATE TABLE "E1 space".partitioned(n integer) PARTITION BY RANGE(n);',
                "preflight_generated_column": f"ALTER TABLE {TABLE} ADD COLUMN generated integer GENERATED ALWAYS AS (bucket + 1) STORED;",
                "preflight_source_non_utf8": "SELECT 1;",
                "preflight_target_non_utf8": "SELECT 1;",
                "preflight_target_check_encoding": f"ALTER TABLE {TABLE} ADD CHECK (set_config('client_encoding','LATIN1',false) = 'LATIN1');",
                "preflight_target_partial_index": f"CREATE INDEX e1_partial ON {TABLE} (bucket) WHERE bucket > 0;",
                "preflight_target_exclusion": f"ALTER TABLE {TABLE} ADD CONSTRAINT e1_exclusion EXCLUDE USING btree (bucket WITH =);",
            }[case_id]
        psql(self.context, database, sql)
        before = self.target_snapshot(target)
        self.preflight_failure(source, target)
        assert self.target_snapshot(target) == before, "preflight refusal changed target data or schema"

    def preflight_failure(self, source, target):
        with ProtocolFault(self.context, "observe") as observation:
            result = self.invoke(source, target, target_socket=str(observation.directory))
            self.fail(result)
        assert result.returncode == 2, "preflight did not return BadConfig"
        assert observation.client_copy_bytes == 0, "preflight sent target COPY data before refusing"
        self.result["copy_bytes_before_refusal"] = observation.client_copy_bytes

    def foreign_keys(self, case_id):
        source, target = databases(self.context, case_id, empty=True)
        external = case_id == "preflight_fk_external_parent"
        invalid = case_id == "fault_deferrable_fk"
        trigger_state = {"preflight_fk_disabled_triggers": "D", "preflight_fk_replica_triggers": "R"}.get(case_id)
        orphan = invalid or trigger_state is not None
        parent_type = "bigint" if case_id == "preflight_fk_mixed_types" else "integer"
        timing = "NOT DEFERRABLE" if case_id == "preflight_nondeferrable_fk" else "DEFERRABLE INITIALLY IMMEDIATE"
        parent = f"CREATE TABLE public.z_parent(id {parent_type} PRIMARY KEY);"
        child = "CREATE TABLE public.a_child(id integer PRIMARY KEY, parent_id integer"
        schema = parent + child + f" REFERENCES public.z_parent(id) {timing});"
        source_schema = (("" if external else parent) + child + ");") if external or orphan else schema
        psql(self.context, source, source_schema, role="e1_source")
        psql(self.context, target, schema, role="e1_target")
        psql(self.context, target if external else source, "INSERT INTO public.z_parent VALUES (1),(2);")
        psql(self.context, source, f"INSERT INTO public.a_child VALUES (1,1),(2,{3 if orphan else 2});")
        trigger_query = "SELECT json_agg(json_build_array(tgname,tgenabled) ORDER BY tgname) FROM pg_trigger WHERE tgrelid='public.a_child'::regclass AND tgisinternal AND tgconstraint<>0;"
        if trigger_state:
            action = "DISABLE" if trigger_state == "D" else "ENABLE REPLICA"
            psql(self.context, target, f"DO $$DECLARE t record; BEGIN FOR t IN SELECT tgname FROM pg_trigger WHERE tgrelid='public.a_child'::regclass AND tgisinternal AND tgconstraint<>0 LOOP EXECUTE format('ALTER TABLE public.a_child {action} TRIGGER %I',t.tgname); END LOOP; END$$;")
            original_triggers = json.loads(psql(self.context, target, trigger_query))
            assert original_triggers is not None and len(original_triggers) == 2, "expected FK constraint triggers missing"
            assert all(state == trigger_state for _, state in original_triggers), "FK trigger fixture state differs"
            assert psql(self.context, target, "SHOW session_replication_role;", role="e1_target", canonical=False) == "origin"
        expected = {**self.manifest["foreign_key_fixture"]}
        if parent_type == "bigint":
            expected["relations"] = [expected["relations"][0], {**expected["relations"][1], "columns": [["id", "bigint"]]}]
        before_expected = {**expected, "row_counts": {"a_child": 0, "z_parent": 2 if external else 0}}
        before = fingerprint(self.context, target, before_expected)
        if case_id.startswith("preflight_"):
            self.preflight_failure(source, target)
            assert fingerprint(self.context, target, before_expected) == before
            if trigger_state:
                assert json.loads(psql(self.context, target, trigger_query)) == original_triggers, "preflight altered existing FK trigger states"
        elif invalid:
            with ProtocolFault(self.context, "observe") as observation:
                self.fail(self.invoke(source, target, target_socket=str(observation.directory)))
            assert observation.error_states.count("23503") == 1, "deferred FK validation did not fire"
            assert observation.client_copy_bytes == 12, "FK failure occurred before complete COPY input"
            self.result.update(fault_hits=1, sqlstate="23503", copy_bytes_before_fault=observation.client_copy_bytes)
            assert fingerprint(self.context, target, before_expected) == before, "failed FK left target rows"
        else:
            result = self.invoke(source, target)
            assert result.returncode == 0, "deferrable child-before-parent COPY failed"
            self.result["fingerprints"] = assert_equal(self.context, source, target, expected)
            self.completed(2, 4, result.stdout)
            assert json.loads(result.stdout)["bytes"] == 12, "FK fixture COPY byte count differs"

    def unique_fault(self, source, target):
        for database in (source, target):
            psql(self.context, database, f"ALTER TABLE {TABLE} ADD COLUMN late_unique integer;")
        psql(self.context, source, f"WITH numbered AS (SELECT ctid, row_number() OVER (ORDER BY bucket, ctid) AS n FROM {TABLE}) UPDATE {TABLE} t SET late_unique=CASE WHEN numbered.n=10003 THEN 10002 ELSE numbered.n END FROM numbered WHERE t.ctid=numbered.ctid;")
        psql(self.context, target, f"ALTER TABLE {TABLE} ADD CONSTRAINT late_unique_failure UNIQUE (late_unique);")
        with ProtocolFault(self.context, "observe_unique") as observation:
            self.fail(self.invoke(source, target, target_socket=str(observation.directory)))
        assert observation.hits == 1 and "23505" in observation.error_states, "real UNIQUE violation was not observed"
        assert observation.client_copy_bytes > 8 * 1024 * 1024, "UNIQUE failure preceded buffered data"
        self.result.update(fault_hits=observation.hits, sqlstate="23505", copy_bytes_before_fault=observation.client_copy_bytes)
        self.empty_target(target)

    def target_snapshot(self, database):
        schema = inventory(self.context, database)
        schema["row_counts"] = {}
        from db_harness import identifier
        for relation in schema["relations"]:
            table = identifier(relation["schema"]) + "." + identifier(relation["name"])
            schema["row_counts"][relation["name"]] = int(psql(self.context, database, f"SELECT count(*) FROM {table};"))
        if schema["relations"]:
            return fingerprint(self.context, database, schema), schema
        return {}, schema

    def protocol_fault(self, case_id, source, target):
        source_fault = case_id == "fault_source_terminal"
        with ProtocolFault(self.context, "source_terminal" if source_fault else "commit_unknown") as fault:
            sockets = {"source_socket" if source_fault else "target_socket": str(fault.directory)}
            result = self.invoke(source, target, **sockets)
            self.fail(result)
        assert fault.hits == 1, "protocol fault never fired"
        self.result["fault_hits"] = fault.hits
        self.result["copy_bytes_before_fault"] = fault.server_copy_bytes if source_fault else fault.client_copy_bytes
        assert self.result["copy_bytes_before_fault"] > 8 * 1024 * 1024, "fault happened before buffered data"
        if source_fault:
            assert fault.copy_frames == 10003, "source error was not after the final data frame"
            self.empty_target(target)
        else:
            assert result.returncode == 3 and "commit-outcome-unknown" in result.stderr, "unknown COMMIT not classified as uncertain state"
            self.result["fingerprints"] = assert_equal(self.context, source, target, self.manifest)

    def signal_fault(self, case_id, source, target):
        blocker = subprocess.Popen([
            str(Path(self.context["bindir"]) / "psql"), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1",
            "--dbname", conninfo(self.context, target, "e1_target", application_name="e1_signal_blocker"), "-c",
            f"BEGIN; LOCK TABLE {TABLE} IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(30); ROLLBACK;"],
            env=self.environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        child = None
        try:
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                granted = psql(self.context, target, "SELECT count(*) FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND mode='AccessExclusiveLock' AND granted;")
                if int(granted) > 0:
                    break
                assert blocker.poll() is None, "fixture lock process exited"
                time.sleep(0.02)
            else:
                raise AssertionError("fixture lock not observed")
            child = subprocess.Popen(self.command(source, target), env=self.environment,
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                waiting = psql(self.context, "postgres", f"SELECT count(*) FROM pg_stat_activity WHERE datname='{target}' AND wait_event_type='Lock';")
                if int(waiting) > 0:
                    break
                assert child.poll() is None, "binary exited before observed lock wait"
                time.sleep(0.02)
            else:
                raise AssertionError("binary never reached fixture lock wait")
            child.send_signal(signal.SIGINT if case_id == "signal_sigint" else signal.SIGTERM)
            self.result["fault_hits"] = 1
            _, stderr = child.communicate(timeout=5)
            self.result["returncode"] = child.returncode
            assert child.returncode > 0, "signal did not exit through handled failure path"
            assert "cancel" in stderr.lower() or "interrupt" in stderr.lower(), "cancellation diagnostic absent"
            self.no_completion()
        finally:
            try:
                psql(self.context, "postgres", f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{target}' AND application_name='e1_signal_blocker';")
            finally:
                for process in (child, blocker):
                    if process is not None and process.poll() is None:
                        process.kill()
                        process.wait(timeout=5)
        self.empty_target(target)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    args = parser.parse_args()
    context = json.loads(Path(os.environ["RPGMIGRATE_TEST_CONTEXT"]).read_text())
    manifest = load_manifest()
    runner = Cases(args.binary, context, manifest)
    report = {"server_version_num": context["server_version_num"], "seed": manifest["seed"], "cases": []}
    for case in manifest["cases"]:
        try:
            result = runner.run(case)
            print(f"PASS {case['id']}", flush=True)
        except (AssertionError, OSError, RuntimeError, subprocess.SubprocessError) as error:
            result = runner.result
            result["error"] = type(error).__name__ + ": " + str(error)
            print(f"FAIL {case['id']}: {result['error']}", flush=True)
        report["cases"].append(result)
    Path(os.environ["RPGMIGRATE_TEST_RECEIPT"]).write_text(json.dumps(report, indent=2) + "\n")
    validate_report(manifest, report)
    print(f"E1 executable verified {len(report['cases'])} cases on PostgreSQL {manifest['server_version']}")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, KeyError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"E1 suite failed: {error}", file=sys.stderr)
        sys.exit(1)
