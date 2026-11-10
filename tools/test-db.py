#!/usr/bin/env python3
"""Own one private PostgreSQL server, run a DB test suite, and verify its receipt."""

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests" / "support"))
from db_harness import (FIXTURES, SETTINGS, OwnedCluster, assert_equal, conninfo, databases, load_manifest,
                        fingerprint, module_cases, psql, reference_copy, run_command, validate_libtest_output, validate_report)
from protocol_fault import ProtocolFault
from authority_fixture import authority_fixture, validate_authority_faults
from replication_fixture import replication_fixture, validate_replication_faults
from money_locale_fixture import money_locales
from follow_snapshot_fixture import (follow_snapshot_application_name,
                                     validate_follow_snapshot_faults)


def validate_module_receipt(profile, expected, output, context):
    validate_libtest_output(expected, output)
    if profile in ("Catalog", "SourceCatalog", "Clone", "Schema", "FollowSource", "FollowSnapshot", "FollowAuthority", "FollowReceive", "FollowApply"):
        summaries = re.findall(r"^test result: (.*)$", output, flags=re.MULTILINE)
        assert len(summaries) == 1 and re.fullmatch(
            rf"ok\. {len(expected)} passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in \S+",
            summaries[0]), "missing or inconsistent catalog test summary"
    if profile == "FollowSnapshot":
        assert expected == module_cases({}, profile), "wrong current source snapshot inventory"
        validate_follow_snapshot_faults(context)
    if profile == "FollowAuthority":
        validate_authority_faults(context, follow=True)
    if profile in ("Authority", "Engine"):
        validate_authority_faults(context)
    if profile in ("Replication", "Engine"):
        validate_replication_faults(context["replication"])


def self_check(cluster, manifest):
    source, target = databases(cluster.context, "selfcheck")
    auth_self_check(cluster, source)
    fingerprint(cluster.context, target, manifest, empty=True)
    reference_copy(cluster.context, source, target, manifest)
    digest = assert_equal(cluster.context, source, target, manifest)
    controls = (
        'DROP TABLE "E1 space"."empty table";',
        'ALTER TABLE "E1 space"."row ""payload" ALTER COLUMN bucket TYPE bigint;',
        'UPDATE "E1 space"."row ""payload" SET "text value" = \'changed\' WHERE ctid = (SELECT ctid FROM "E1 space"."row ""payload" LIMIT 1);',
    )
    for number, sql in enumerate(controls):
        # Fresh copies keep each negative control independent of previous corruption.
        source, target = databases(cluster.context, f"control_{number}")
        reference_copy(cluster.context, source, target, manifest)
        psql(cluster.context, target, sql)
        try:
            assert_equal(cluster.context, source, target, manifest)
        except AssertionError:
            continue
        raise AssertionError(f"oracle accepted negative control {number}")
    protocol_self_check(cluster, manifest)
    print(json.dumps({"self_check": "passed", "server_version": manifest["server_version"],
                      "negative_controls": 3, "wire_faults": 2, "scram_auth": "passed",
                      "fingerprints": digest}, sort_keys=True))


def auth_self_check(cluster, database):
    base = conninfo(cluster.context, database, "e1_auth")
    command = [str(cluster.bindir / "psql"), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1",
               "--dbname", base, "-c", "SELECT current_user;"]
    for password in (None, "deliberately-wrong-fixture-password"):
        environment = dict(cluster.environment)
        if password is not None:
            environment["PGPASSWORD"] = password
        result = subprocess.run(command, env=environment, capture_output=True, text=True, timeout=10)
        assert result.returncode != 0, "SCRAM fixture accepted missing or wrong password"
    passfile = cluster.root / "auth-selfcheck.pgpass"
    values = (str(cluster.socket), str(cluster.port), database, "e1_auth", cluster.auth_password)
    valid_contents = ":".join(value.replace("\\", "\\\\").replace(":", "\\:") for value in values) + "\n"
    passfile.touch(mode=0o600)
    passfile.write_text(valid_contents)
    command[command.index("--dbname") + 1] = base + " passfile='" + str(passfile).replace("\\", "\\\\").replace("'", "\\'") + "'"
    result = subprocess.run(command, env=cluster.environment, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0 and result.stdout.strip() == "e1_auth", "valid private passfile did not authenticate fixture role"
    service = cluster.root / "auth-selfcheck.service"
    service.touch(mode=0o600)
    service.write_text(f"[e1_auth_selfcheck]\nhost={cluster.socket}\nport={cluster.port}\ndbname={database}\nuser=e1_auth\npassfile={passfile}\nsslmode=disable\nconnect_timeout=3\n")
    service_environment = {**cluster.environment, "PGSERVICEFILE": str(service)}
    service_command = list(command)
    service_command[service_command.index("--dbname") + 1] = "service=e1_auth_selfcheck"
    result = subprocess.run(service_command, env=service_environment, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0 and result.stdout.strip() == "e1_auth", "valid private service did not authenticate fixture role"
    passfile.write_text(valid_contents.replace(cluster.auth_password, "deliberately-wrong-fixture-password"))
    result = subprocess.run(service_command, env=service_environment, capture_output=True, text=True, timeout=10)
    assert result.returncode != 0, "service accepted a wrong password"
    passfile.write_text(valid_contents)
    passfile.chmod(0o644)
    result = subprocess.run(service_command, env=service_environment, capture_output=True, text=True, timeout=10)
    assert result.returncode != 0, "service accepted an insecure passfile"
    passfile.chmod(0o600)
    service.write_text(service.read_text().replace(f"passfile={passfile}", f"passfile={cluster.root}"))
    result = subprocess.run(service_command, env=service_environment, capture_output=True, text=True, timeout=10)
    assert result.returncode != 0, "service accepted a nonregular passfile"


def protocol_self_check(cluster, manifest):
    source, target = databases(cluster.context, "protocol_selfcheck")
    table = '"E1 space"."row ""payload"'
    with ProtocolFault(cluster.context, "source_terminal") as fault:
        result = subprocess.run([
            str(cluster.bindir / "psql"), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname",
            conninfo(cluster.context, source, "e1_source", str(fault.directory))],
            input=SETTINGS + f"\nCOPY {table} TO STDOUT;", env=cluster.environment,
            text=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30)
    assert result.returncode != 0 and fault.hits == 1, "terminal fault not observed by psql"
    assert fault.copy_frames == 10003 and fault.server_copy_bytes > 8 * 1024 * 1024
    data = psql(cluster.context, source, f"COPY {table} TO STDOUT;", raw=True)
    with ProtocolFault(cluster.context, "commit_unknown") as fault:
        result = subprocess.run([
            str(cluster.bindir / "psql"), "-X", "-w", "-qAt", "-v", "ON_ERROR_STOP=1", "--dbname",
            conninfo(cluster.context, target, "e1_target", str(fault.directory))],
            input=SETTINGS + f"\nBEGIN; COPY {table} FROM STDIN;\n" + data + "\\.\nCOMMIT;\n",
            env=cluster.environment, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30)
    assert result.returncode != 0 and fault.hits == 1, "COMMIT fault not observed by psql"
    assert fault.client_copy_bytes > 8 * 1024 * 1024
    assert_equal(cluster.context, source, target, manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--follow-authority", action="store_true", help="require seventeen exact native pgoutput target authority and cutover cases")
    modes.add_argument("--follow-receive", action="store_true", help="require eleven exact native pgoutput receiver and checked cutover cases")
    modes.add_argument("--follow-apply", action="store_true", help="require eleven exact native pgoutput row semantics and catalog identity cases")
    modes.add_argument("--follow-snapshot", action="store_true", help="require nine exact pgoutput snapshot cases and five independent fault receipts")
    modes.add_argument("--follow-source", action="store_true", help="require twelve exact native pgoutput source snapshot and owned cleanup cases")
    modes.add_argument("--schema", action="store_true", help="require three exact native schema utility and pre-data drift cases")
    modes.add_argument("--clone", action="store_true", help="require all ten exact native clone component cases")
    modes.add_argument("--self-check", action="store_true")
    modes.add_argument("--transport", action="store_true", help="require the exact transport unit-test inventory")
    modes.add_argument("--prepared", action="store_true", help="require the exact prepared transport live-test inventory")
    modes.add_argument("--authority", action="store_true", help="require the exact target authority live-test inventory")
    modes.add_argument("--replication", action="store_true", help="require the exact raw CopyBoth live-test inventory")
    modes.add_argument("--catalog", action="store_true", help="require thirteen exact CDC, clone, and money catalog live tests")
    modes.add_argument("--source-catalog", action="store_true", help="require the exact three source identity live tests")
    modes.add_argument("--engine", action="store_true", help="require all transport, prepared, authority, replication, and catalog/COPY live tests")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not args.self_check and not command:
        parser.error("provide --self-check or -- followed by the integration test command")
    manifest = load_manifest()
    module_suite = args.follow_snapshot or args.follow_authority or args.follow_receive or args.follow_apply or args.follow_source or args.transport or args.engine or args.prepared or args.authority or args.replication or args.catalog or args.source_catalog or args.clone or args.schema
    if module_suite:
        profile = "FollowSnapshot" if args.follow_snapshot else "FollowAuthority" if args.follow_authority else "FollowReceive" if args.follow_receive else "FollowApply" if args.follow_apply else "FollowSource" if args.follow_source else "Schema" if args.schema else "Clone" if args.clone else "SourceCatalog" if args.source_catalog else "Catalog" if args.catalog else "Replication" if args.replication else "Authority" if args.authority else "Prepared" if args.prepared else "Engine" if args.engine else "Transport"
        expected = module_cases(manifest, profile)
    with money_locales(args.catalog), (OwnedCluster(manifest, logical=True, logical_slots=32) if args.follow_source or args.follow_snapshot or args.follow_receive else OwnedCluster(manifest, tcp=True) if args.schema else OwnedCluster(manifest)) as cluster, ExitStack() as fixtures:
        if args.self_check:
            self_check(cluster, manifest)
            return 0
        source, target = databases(cluster.context, "source_catalog" if args.source_catalog else "catalog" if args.catalog else "transport",
                                   empty=args.follow_snapshot or args.follow_source or args.follow_authority or args.follow_receive or args.follow_apply or args.catalog or args.source_catalog or args.clone or args.schema)
        receipt = cluster.root / "results.json"
        environment = dict(cluster.environment)
        environment.update({
            "RPGMIGRATE_TEST_CONTEXT": str(cluster.context_path),
            "RPGMIGRATE_TEST_SOURCE": conninfo(cluster.context, source, "e1_source"),
            "RPGMIGRATE_TEST_TARGET": conninfo(cluster.context, target, "e1_target"),
            "RPGMIGRATE_TEST_PSQL": str(cluster.bindir / "psql"),
            "RPGMIGRATE_TEST_PYTHON": sys.executable,
            "RPGMIGRATE_TEST_RECEIPT": str(receipt),
            "RPGMIGRATE_TEST_AUTH_BASE": conninfo(cluster.context, source, "e1_auth", include_passfile=False),
            "RPGMIGRATE_TEST_AUTH_PASSWORD": cluster.auth_password,
            "RPGMIGRATE_TEST_EXPECTED_PG_VERSION": manifest["server_version"],
            "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM": str(manifest["server_version_num"]),
        })
        if args.follow_receive:
            environment.update(RPGMIGRATE_TEST_FOLLOW_RECEIVE_CONTEXT=str(cluster.context_path),
                               RPGMIGRATE_TEST_FOLLOW_RECEIVE_SOURCE=conninfo(cluster.context, "postgres", "e1_admin"))
        if args.follow_apply:
            environment["RPGMIGRATE_TEST_SOURCE"] = conninfo(cluster.context, source, "e1_admin")
        if args.follow_snapshot:
            helper = ROOT / "tests/support/follow_snapshot_fault.py"
            if not helper.is_file():
                raise RuntimeError("required current follow snapshot fault helper is missing")
            environment.update(RPGMIGRATE_TEST_FOLLOW_SNAPSHOT_CONTEXT=str(cluster.context_path),
                               RPGMIGRATE_TEST_FOLLOW_SNAPSHOT_HELPER=str(helper),
                               PGAPPNAME=follow_snapshot_application_name(cluster.context))
        if args.follow_source or args.follow_snapshot:
            environment["RPGMIGRATE_TEST_FOLLOW_SOURCE_ADMIN"] = conninfo(cluster.context, "postgres", "e1_admin")
        if args.schema:
            environment.update({
                "RPGMIGRATE_TEST_SCHEMA_SOURCE_ADMIN": conninfo(cluster.context, source, "e1_admin"),
                "RPGMIGRATE_TEST_SCHEMA_TARGET_ADMIN": conninfo(cluster.context, target, "e1_admin"),
            })
        if args.clone:
            environment.update({
                "RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN": conninfo(cluster.context, source, "e1_admin"),
                "RPGMIGRATE_TEST_CLONE_TARGET_ADMIN": conninfo(cluster.context, target, "e1_admin"),
            })
        if args.catalog:
            # CDC cases commit fixtures; clone admission requires an untouched database pair.
            clone_source, clone_target = databases(cluster.context, "catalog_clone", empty=True)
            environment.update({
                "RPGMIGRATE_TEST_CLONE_SOURCE_ADMIN": conninfo(cluster.context, clone_source, "e1_admin"),
                "RPGMIGRATE_TEST_CLONE_TARGET_ADMIN": conninfo(cluster.context, clone_target, "e1_admin"),
                "RPGMIGRATE_TEST_CATALOG_SOURCE": conninfo(cluster.context, source, "e1_source"),
                "RPGMIGRATE_TEST_CATALOG_ADMIN": conninfo(cluster.context, source, "e1_admin"),
                "RPGMIGRATE_TEST_CATALOG_TARGET": conninfo(cluster.context, target, "e1_target"),
                "RPGMIGRATE_TEST_CATALOG_TARGET_ADMIN": conninfo(cluster.context, target, "e1_admin"),
            })
        if args.source_catalog:
            psql(cluster.context, source,
                 "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_control_system() FROM PUBLIC, e1_auth; "
                 "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO e1_source;")
            environment.update({
                "RPGMIGRATE_TEST_SOURCE_CATALOG_SOURCE": conninfo(cluster.context, source, "e1_source"),
                "RPGMIGRATE_TEST_SOURCE_CATALOG_ADMIN": conninfo(cluster.context, source, "e1_admin"),
            })
        if args.authority or args.engine or args.follow_authority:
            environment.update(fixtures.enter_context(authority_fixture(cluster)))
        if args.replication or args.engine:
            context, source_environment = fixtures.enter_context(replication_fixture(manifest))
            cluster.context["replication"] = context
            environment.update(source_environment)
        # Cargo keeps its cache/toolchain paths while test clients get a private HOME.
        for key, fallback in (("CARGO_HOME", ".cargo"), ("RUSTUP_HOME", ".rustup")):
            if key not in environment:
                environment[key] = str(Path.home() / fallback)
        result = run_command(command, cwd=ROOT, environment=environment, timeout=600,
                             capture_output=module_suite)
        if module_suite:
            print(result.stdout, end="")
            print(result.stderr, end="", file=sys.stderr)
        if result.returncode:
            return result.returncode
        if module_suite:
            validate_module_receipt(profile, expected, result.stdout, cluster.context)
            if args.authority or args.engine:
                print("Authority faults verified 2 held COMMIT replies and observed fence releases")
            if args.replication or args.engine:
                print("Replication faults verified 4 wire cases and observed PQflush backpressure")
            if args.follow_snapshot:
                print("FollowSnapshot faults verified 5 cases, 3 CREATE tail phases, and closed/reaped owned processes")
            print(f"{profile} verified {len(expected)} exact cases; full E1 suite not selected")
            return 0
        if not receipt.is_file():
            raise AssertionError("no E1 receipt: zero tests or wrong test selection")
        report = json.loads(receipt.read_text())
        validate_report(manifest, report)
        print(f"E1 verified {len(report['cases'])} exact cases on PostgreSQL {manifest['server_version']}")
        return 0


def interrupted(_signum, _frame):
    raise KeyboardInterrupt


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except (AssertionError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print(f"test-db: {error}", file=sys.stderr)
        sys.exit(1)
