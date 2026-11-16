"""Keep hosted qualification complete and separate from publication."""

from pathlib import Path
import json
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/ci.yml"
JOBS = {"checks", "dependencies", "docs", "engine-e2e", "runner-image"}
PAIRS = ("16-16", "18-18", "16-18")
ARCHES = {"amd64": "ubuntu-24.04", "arm64": "ubuntu-24.04-arm"}
LIVE_WORKFLOW = ROOT / ".github/workflows/live-minors.yml"
LIVE_REQUIRED = (
    "\n    timeout-minutes: ", "persist-credentials: false", "python3 tools/bootstrap-task.py",
    "        major: [16, 18]\n", "fail-fast: false", 'test "$server" = "$candidate"', "*pgdg*) ;;",
    "'libpq5=18.*'", "--test copy_integration", 'test -z "$failed" || {', "        if: always()\n",
    "actions/upload-artifact@", "path: target/live-minors/", "if-no-files-found: error",
)


def jobs(source):
    body = source.split("\njobs:\n", 1)[1]
    headers = list(re.finditer(r"^  ([a-z][a-z0-9-]*):\n", body, re.MULTILINE))
    return {match[1]: body[match.end():headers[index + 1].start() if index + 1 < len(headers) else len(body)]
            for index, match in enumerate(headers)}


def validate(source):
    sections = jobs(source)
    if set(sections) != JOBS:
        raise ValueError("required qualification jobs differ")
    required = {
        "checks": ("run: task --silent --output=interleaved --color=false check lint test",),
        "dependencies": ("cargo install cargo-deny --version 0.20.2 --locked", "cargo deny --locked check licenses advisories"),
        "docs": ("DavidAnson/markdownlint-cli2-action@", '"git", "ls-files"', "*.md", "*.markdown"),
        "engine-e2e": ("CONTAINER_RUNTIME: podman", "RPGMIGRATE_E2E_CAPACITY: hosted-linux", "podman --version", 'test "$podman_major" -ge 5', '["podman", "pull", "--quiet", reference]', "task test-e2e-clone-${{ matrix.pair }}",
                       'validate_receipt(report, clone.load_manifest())', 'test -s "target/e2e/${{ matrix.pair }}.json"',
                       "task test-e2e-follow-smoke-${{ matrix.pair }}", "task test-e2e-cutover-${{ matrix.pair }}",
                       "task test-e2e-sessions-${{ matrix.pair }}", "task test-e2e-timeouts-${{ matrix.pair }}",
                       "follow.validate_receipt(smoke, follow.load_manifest())",
                       "cutover.validate_receipt(producer, cutover.load_manifest(), require_images=True)",
                       "sessions.validate_receipt(names)", "timeouts.validate_receipt(limits)",
                       'test -s "target/e2e/follow-smoke-${{ matrix.pair }}.json"', 'test -s "target/e2e/follow-cutover-${{ matrix.pair }}.json"',
                       'test -s "target/e2e/follow-sessions-${{ matrix.pair }}.json"', 'test -s "target/e2e/follow-timeouts-${{ matrix.pair }}.json"',
                       "task test-e2e-clone-${{ matrix.pair }} PROFILE=m8", "task test-e2e-follow-widening-${{ matrix.pair }}",
                       'test -s "target/e2e/follow-widening-${{ matrix.pair }}.json"', 'test -s "target/e2e/clone-m8-${{ matrix.pair }}.json"',
                       "widening.validate_receipt(widened)",
                       'copy_features.validate_receipt(copied, copy_features.load_manifest(), binding=copied.get("binding"))'),
        "runner-image": ("CONTAINER_RUNTIME: podman", "podman --version", "task image:build", "task --silent image:smoke",
                         "test -s target/image/smoke.json"),
    }
    for name, fragments in required.items():
        for fragment in fragments:
            if fragment not in sections[name]:
                raise ValueError(f"missing {name} command: {fragment}")
    if "    runs-on: ubuntu-26.04\n" not in sections["engine-e2e"]:
        raise ValueError("engine e2e must run on the Podman 5 runner")
    if re.findall(r"^        pair: \[(.*)\]$", sections["engine-e2e"], re.MULTILINE) != [", ".join(PAIRS)]:
        raise ValueError("clone matrix differs")
    entries = re.findall(r"^          - arch: ([a-z0-9]+)\n            runner: ([a-z0-9.-]+)$",
                         sections["runner-image"], re.MULTILINE)
    if entries != list(ARCHES.items()):
        raise ValueError("native image matrix differs")
    if "runs-on: ${{ matrix.runner }}" not in sections["runner-image"]:
        raise ValueError("image matrix must select native runner")
    if source.count("fail-fast: false") != 2:
        raise ValueError("qualification matrices must finish all entries")
    if "permissions:\n  contents: read\n" not in source or re.search(r"(?:write|read)-all|:\s*write\b", source):
        raise ValueError("workflow permissions must remain read-only")
    if re.search(r"continue-on-error|docker/login|ghcr\.io|docker push|podman push|packages:", source):
        raise ValueError("publication or ignored failures are forbidden")
    actions = re.findall(r"^\s+- uses: (\S+)", source, re.MULTILINE)
    if not actions or any(not re.fullmatch(r"[\w.-]+/[\w.-]+@[a-f0-9]{40}", action) for action in actions):
        raise ValueError("every action needs an immutable commit")
    for name, section in sections.items():
        if not re.search(r"^    timeout-minutes: [1-9][0-9]*$", section, re.MULTILINE):
            raise ValueError(f"missing {name} timeout")
        if section.count("actions/checkout@") != 1 or "persist-credentials: false" not in section:
            raise ValueError(f"missing {name} isolated checkout")
    for name in ("checks", "engine-e2e", "runner-image"):
        if "python3 tools/bootstrap-task.py" not in sections[name]:
            raise ValueError(f"missing {name} Task checksum bootstrap")
    for name in ("engine-e2e", "runner-image"):
        if "actions/upload-artifact@" not in sections[name] or "if-no-files-found: error" not in sections[name]:
            raise ValueError(f"missing {name} required receipt artifact")
    if "path: target/e2e/${{ matrix.pair }}.json" not in sections["engine-e2e"]:
        raise ValueError("clone artifacts must contain only the sanitized receipt")
    if ("            target/e2e/follow-smoke-${{ matrix.pair }}.json\n            target/e2e/follow-cutover-${{ matrix.pair }}.json\n"
            "            target/e2e/follow-sessions-${{ matrix.pair }}.json\n            target/e2e/follow-timeouts-${{ matrix.pair }}.json\n"
            "            target/e2e/follow-widening-${{ matrix.pair }}.json\n            target/e2e/clone-m8-${{ matrix.pair }}.json\n") not in sections["engine-e2e"]:
        raise ValueError("follow artifacts must contain only the sanitized smoke, cutover, session census, timeout, widening and M8 receipts")
    steps = re.findall(r"^      - name: [^\n]+\n(?:        id: [a-z]+\n)?        run: (task test-e2e-[a-z-]+-\$\{\{ matrix\.pair \}\}(?: PROFILE=m8)?)\n(?=      - )",
                       sections["engine-e2e"], re.MULTILINE)
    if steps != ["task test-e2e-clone-${{ matrix.pair }}", "task test-e2e-clone-${{ matrix.pair }} PROFILE=m8", "task test-e2e-follow-smoke-${{ matrix.pair }}",
                 "task test-e2e-follow-widening-${{ matrix.pair }}", "task test-e2e-cutover-${{ matrix.pair }}",
                 "task test-e2e-sessions-${{ matrix.pair }}", "task test-e2e-timeouts-${{ matrix.pair }}"]:
        raise ValueError("every pair must run clone, the M8 copy profile, follow smoke, widening, cutover, session census and session timeouts once, unconditionally, in that order")
    validate_failure_upload(sections["engine-e2e"])
    if "path: target/image/smoke.json" not in sections["runner-image"]:
        raise ValueError("image artifact must bind the smoke receipt")


def validate_failure_upload(section):
    """Decisions (cd) and (cw): scanned ledgers on any failure, scanned census logs only when the census failed, kept 7 days."""
    scan = re.search(r"^      - name: Scan the failure diagnostics for credentials\n        if: failure\(\)\n(.*?)^          PYSCAN$", section, re.MULTILINE | re.DOTALL)
    upload = re.search(r"^      - name: Retain the scanned runtime ledgers and census logs on failure\n        if: failure\(\)\n(.*?)(?=^      - |\Z)", section, re.MULTILINE | re.DOTALL)
    if not scan or not upload or scan.start() > upload.start():
        raise ValueError("failure diagnostics must be scanned before their upload, and both only on failure")
    for fragment in ("CENSUS_FAILED: ${{ steps.census.outcome == 'failure' }}", 'glob("*.runtime.json")', "census-logs",
                     'os.environ["CENSUS_FAILED"] == "true"', "credential.search(path.read_bytes())", "raise SystemExit(", 'Path("target/e2e-upload")'):
        if fragment not in scan[1]:
            raise ValueError(f"failure diagnostics scan lacks: {fragment}")
    for fragment in ("path: target/e2e-upload/", "retention-days: 7\n", "if-no-files-found: error"):
        if fragment not in upload[1]:
            raise ValueError(f"failure diagnostics upload lacks: {fragment}")
    if "        id: census\n        run: task test-e2e-sessions-${{ matrix.pair }}\n" not in section:
        raise ValueError("the census step needs the id the census-log rule reads")


KEY_BUILDS = (ROOT / "Dockerfile", ROOT / "tools/e2e/Containerfile")


def validate_key_fetch(source):
    # Hosted image builds lost jobs to curl exit 56 on this fetch; plain --retry skips that error.
    fetches = [line for line in source.splitlines() if "ACCC4CF8.asc" in line and "curl " in line]
    if len(fetches) != 1:
        raise ValueError("expected exactly one PGDG key fetch")
    flags = fetches[0].split()
    if "--fail" not in flags or "--retry-all-errors" not in flags or not re.search(r"--retry [1-9]", fetches[0]):
        raise ValueError("PGDG key fetch must fail on HTTP errors and retry transient network errors")


def live_targets():
    """Every tools/test-db.py mode as the Taskfile target that runs it."""
    modes = re.findall(r'modes\.add_argument\("--([a-z-]+)"', (ROOT / "tools/test-db.py").read_text())
    if not modes:
        raise ValueError("no tools/test-db.py profiles found")
    return {"test-db-self-check" if mode == "self-check" else "test-" + mode for mode in modes}


def validate_live(source):
    triggers = source.split("\non:\n", 1)[1].split("\n\n", 1)[0] if "\non:\n" in source else ""
    if re.findall(r"^  ([a-z_]+):", triggers, re.MULTILINE) != ["schedule", "workflow_dispatch"]:
        raise ValueError("live minors must run only on schedule and manual dispatch")
    if "permissions:\n  contents: read\n" not in source or re.search(r"(?:write|read)-all|:\s*write\b|secrets\.", source):
        raise ValueError("live minors permissions must remain read-only")
    if re.search(r"continue-on-error|packages:|docker push|podman push", source):
        raise ValueError("publication or ignored failures are forbidden")
    actions = re.findall(r"^\s+(?:- )?uses: (\S+)", source, re.MULTILINE)
    if not actions or any(not re.fullmatch(r"[\w.-]+/[\w.-]+@[a-f0-9]{40}", action) for action in actions):
        raise ValueError("every action needs an immutable commit")
    for fragment in LIVE_REQUIRED:
        if fragment not in source:
            raise ValueError(f"missing live minors control: {fragment.strip()}")
    loop = re.search(r"for target in (.*?); do", source, re.DOTALL)
    targets = set(loop[1].replace("\\\n", " ").split()) if loop else set()
    if targets != live_targets():
        raise ValueError("live minors must run exactly every tools/test-db.py profile")


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.source = WORKFLOW.read_text()

    def test_hosted_contract(self):
        validate(self.source)

    def test_missing_job_pair_and_architecture_refuse(self):
        sections = jobs(self.source)
        for name, section in sections.items():
            with self.subTest(job=name), self.assertRaises(ValueError):
                validate(self.source.replace(f"  {name}:\n" + section, "", 1))
        for pair in PAIRS:
            with self.subTest(pair=pair), self.assertRaises(ValueError):
                validate(self.source.replace(pair, "", 1))
        for arch in ARCHES:
            with self.subTest(architecture=arch), self.assertRaises(ValueError):
                validate(self.source.replace(f"arch: {arch}", "arch: missing", 1))

    def test_noop_required_command_refuses(self):
        for command in ("task --silent --output=interleaved --color=false check lint test",
                        "cargo deny --locked check licenses advisories", "task test-e2e-clone-${{ matrix.pair }}",
                        "task image:build", "task --silent image:smoke", "validate_receipt(report, clone.load_manifest())",
                        "test -s target/image/smoke.json", 'test "$podman_major" -ge 5', "runs-on: ubuntu-26.04",
                        "task test-e2e-follow-smoke-${{ matrix.pair }}", "task test-e2e-cutover-${{ matrix.pair }}",
                        "task test-e2e-sessions-${{ matrix.pair }}", "sessions.validate_receipt(names)",
                        "task test-e2e-timeouts-${{ matrix.pair }}", "timeouts.validate_receipt(limits)",
                        "follow.validate_receipt(smoke, follow.load_manifest())", "task test-e2e-clone-${{ matrix.pair }} PROFILE=m8",
                        "task test-e2e-follow-widening-${{ matrix.pair }}", "widening.validate_receipt(widened)",
                        "credential.search(path.read_bytes())", "retention-days: 7", "if: failure()\n        env:"):
            for replacement in ("", "true"):
                with self.subTest(command=command, replacement=replacement), self.assertRaises(ValueError):
                    validate(self.source.replace(command, replacement, 1))

    def test_private_license_exemption_refuses_any_foreign_unpublished_package(self):
        body = re.search(r"python3 - <<'PYDEPENDENCIES'\n(.*?)^          PYDEPENDENCIES$",
                         self.source, re.MULTILINE | re.DOTALL)
        self.assertIsNotNone(body, "root-only private license exemption needs an executable guard")
        script = "\n".join(line[10:] for line in body[1].splitlines())
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "target"
            target.mkdir()
            for packages, root, success in (([{"id": "root", "publish": []}, {"id": "dependency", "publish": None}], "root", True),
                                             ([{"id": "root", "publish": []}, {"id": "dependency", "publish": []}], "root", False),
                                             ([{"id": "root", "publish": None}], "root", False),
                                             ([], "root", False),
                                             ([{"id": "root", "publish": []}], None, False)):
                (target / "dependency-metadata.json").write_text(json.dumps({"packages": packages, "resolve": {"root": root}}))
                result = subprocess.run([sys.executable, "-c", script], cwd=temporary,
                                        capture_output=True, text=True, timeout=10)
                with self.subTest(packages=packages, root=root):
                    self.assertEqual(result.returncode == 0, success, result.stderr)

    def test_failure_diagnostics_scan_stages_only_clean_files_and_census_logs_only_after_a_census_failure(self):
        body = re.search(r"python3 - <<'PYSCAN'\n(.*?)^          PYSCAN$", self.source, re.MULTILINE | re.DOTALL)
        self.assertIsNotNone(body, "failure diagnostics need an executable scan")
        script = "\n".join(line[10:] for line in body[1].splitlines())
        clean, census = '{"podman_query_stalls": ["0123"], "containers": [{"cgroup_exec_stalls": []}]}', '{"message": "connection authorized: user=postgres"}'
        for ledger, log, failed, staged in ((clean, census, "false", ["ledger.runtime.json"]),
                                            (clean, census, "true", ["failures/run/census-logs/source/log.json", "ledger.runtime.json"]),
                                            ("PGPASSWORD=0123", census, "false", None),
                                            (clean, "password=0123", "true", None),
                                            (clean, "postgresql://postgres:0123@source/db", "true", None),
                                            (clean, "fixture-sensitive-row-1", "true", None),
                                            (clean, "password=0123", "false", ["ledger.runtime.json"])):
            with tempfile.TemporaryDirectory() as temporary, self.subTest(ledger=ledger, log=log, failed=failed):
                root = Path(temporary)
                (root / "target/e2e/failures/run/census-logs/source").mkdir(parents=True)
                (root / "target/e2e/ledger.runtime.json").write_text(ledger)
                (root / "target/e2e/failures/run/census-logs/source/log.json").write_text(log)
                result = subprocess.run([sys.executable, "-c", script], cwd=temporary, env={"CENSUS_FAILED": failed},
                                        capture_output=True, text=True, timeout=10)
                upload = root / "target/e2e-upload"
                found = sorted(str(path.relative_to(upload)) for path in upload.rglob("*") if path.is_file()) if upload.exists() else []
                self.assertEqual(result.returncode == 0, staged is not None, result.stderr)
                self.assertEqual(found, staged or [])

    def test_dependency_policy_covers_both_targets_and_all_dependency_kinds(self):
        policy = tomllib.loads((ROOT / "deny.toml").read_text())
        self.assertEqual(policy["graph"], {
            "targets": ["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"], "all-features": True})
        self.assertEqual(policy["advisories"], {"yanked": "deny", "unmaintained": "all"})
        self.assertEqual(policy["licenses"], {
            "include-dev": True, "include-build": True, "confidence-threshold": 0.95,
            "allow": ["Apache-2.0", "MIT", "Unicode-3.0"], "private": {"ignore": True}})
        manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
        self.assertIs(manifest["package"]["publish"], False)
        self.assertEqual(manifest["package"]["license"], "Apache-2.0")

    def test_markdown_policy_preserves_semantic_lines_without_excluding_documents(self):
        policy = json.loads((ROOT / ".markdownlint-cli2.jsonc").read_text())
        self.assertEqual(policy, {"config": {
            "default": True, "MD013": False, "MD024": {"siblings_only": True}}})

    def test_mutable_actions_or_write_permissions_refuse(self):
        mutations = (re.sub(r"actions/checkout@[a-f0-9]{40}", "actions/checkout@main", self.source, count=1),
                     self.source.replace("contents: read", "contents: write", 1),
                     self.source.replace("persist-credentials: false", "persist-credentials: true", 1),
                     self.source + "\n# podman push image\n")
        for source in mutations:
            with self.subTest(source=source[-100:]), self.assertRaises(ValueError):
                validate(source)

    def test_pgdg_key_fetch_retries_transient_errors(self):
        for path in KEY_BUILDS:
            source = path.read_text()
            validate_key_fetch(source)
            for mutated in (source.replace(" --retry-all-errors", "", 1), re.sub(r" --retry [0-9]+", "", source, count=1),
                            source.replace("curl --fail ", "curl ", 1), source.replace("ACCC4CF8.asc", "missing.asc")):
                with self.subTest(path=path.name, source=mutated[:400]), self.assertRaises(ValueError):
                    validate_key_fetch(mutated)

    def test_live_minors_contract(self):
        validate_live(LIVE_WORKFLOW.read_text())

    def test_live_minors_refuses_push_missing_profile_or_ignored_failure(self):
        source = LIVE_WORKFLOW.read_text()
        mutations = [source.replace("  workflow_dispatch:\n", "  workflow_dispatch:\n  push:\n", 1),
                     source.replace("  schedule:\n    - cron: '23 5 * * *'\n", "", 1),
                     source.replace("contents: read", "contents: write", 1),
                     re.sub(r"actions/checkout@[a-f0-9]{40}", "actions/checkout@main", source, count=1),
                     source.replace("    steps:\n", "    continue-on-error: true\n    steps:\n", 1),
                     *(source.replace(fragment, "", 1) for fragment in LIVE_REQUIRED),
                     *(re.sub(rf"(?<=[ ]){re.escape(target)}(?=[ ;\n])", "", source, count=1) for target in live_targets()),
                     source.replace("test-authority;", "test-authority test-unknown;", 1)]
        for mutated in mutations:
            with self.subTest(source=mutated), self.assertRaises(ValueError):
                validate_live(mutated)


if __name__ == "__main__":
    unittest.main()
