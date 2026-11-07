#!/usr/bin/env python3
"""Own one private PostgreSQL server, run a DB test suite, and verify its receipt."""

import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests" / "support"))
from db_harness import (FIXTURES, SETTINGS, OwnedCluster, assert_equal, conninfo, databases, load_manifest,
                        fingerprint, module_cases, psql, reference_copy, run_command, validate_libtest_output, validate_report)
from protocol_fault import ProtocolFault
from authority_fixture import authority_fixture, validate_authority_faults
from replication_fixture import replication_fixture, validate_replication_faults


def validate_module_receipt(profile, expected, output, context):
    validate_libtest_output(expected, output)
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
    modes.add_argument("--self-check", action="store_true")
    modes.add_argument("--transport", action="store_true", help="require the exact transport unit-test inventory")
    modes.add_argument("--prepared", action="store_true", help="require the exact prepared transport live-test inventory")
    modes.add_argument("--authority", action="store_true", help="require the exact target authority live-test inventory")
    modes.add_argument("--replication", action="store_true", help="require the exact raw CopyBoth live-test inventory")
    modes.add_argument("--engine", action="store_true", help="require all transport, prepared, authority, replication, and catalog/COPY live tests")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not args.self_check and not command:
        parser.error("provide --self-check or -- followed by the integration test command")
    manifest = load_manifest()
    module_suite = args.transport or args.engine or args.prepared or args.authority or args.replication
    if module_suite:
        profile = "Replication" if args.replication else "Authority" if args.authority else "Prepared" if args.prepared else "Engine" if args.engine else "Transport"
        expected = module_cases(manifest, profile)
    with OwnedCluster(manifest) as cluster, ExitStack() as fixtures:
        if args.self_check:
            self_check(cluster, manifest)
            return 0
        source, target = databases(cluster.context, "transport")
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
        if args.authority or args.engine:
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


def notify_failure(error, path):
    if path is None:
        return
    try:
        path = Path(path)
        parent = path.parent.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or stat.S_IMODE(parent.st_mode) != 0o700
                or parent.st_uid != os.geteuid()):
            return
        origin = error.__traceback__
        while origin.tb_next is not None:
            origin = origin.tb_next
        code = origin.tb_frame.f_code
        context = (f"test-db-context: {type(error).__name__} at {Path(code.co_filename).name}:{origin.tb_lineno} in {code.co_name}\n").encode("ascii")
        if len(context) > 256:
            return
        descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            info = os.fstat(descriptor)
            if (stat.S_ISREG(info.st_mode) and stat.S_IMODE(info.st_mode) == 0o600
                    and info.st_uid == os.geteuid() and info.st_nlink == 1):
                os.write(descriptor, context)
        finally:
            os.close(descriptor)
    except Exception:
        # Diagnostic failure must preserve the original exception and exit behavior.
        pass


if __name__ == "__main__":
    context_path = os.environ.pop("RPGMIGRATE_CI_FAILURE_CONTEXT", None)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except (AssertionError, OSError, RuntimeError, ValueError, subprocess.SubprocessError) as error:
        print(f"test-db: {error}", file=sys.stderr)
        notify_failure(error, context_path)
        sys.exit(1)
    except Exception as error:
        notify_failure(error, context_path)
        raise
