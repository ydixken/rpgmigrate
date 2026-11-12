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
        "engine-e2e": ("CONTAINER_RUNTIME: podman", "RPGMIGRATE_E2E_CAPACITY: hosted-linux", "podman --version", '["podman", "pull", "--quiet", reference]', "task test-e2e-clone-${{ matrix.pair }}",
                       'validate_receipt(report, clone.load_manifest())', 'test -s "target/e2e/${{ matrix.pair }}.json"'),
        "runner-image": ("CONTAINER_RUNTIME: podman", "podman --version", "task image:build", "task --silent image:smoke",
                         "test -s target/image/smoke.json"),
    }
    for name, fragments in required.items():
        for fragment in fragments:
            if fragment not in sections[name]:
                raise ValueError(f"missing {name} command: {fragment}")
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
    if "path: target/image/smoke.json" not in sections["runner-image"]:
        raise ValueError("image artifact must bind the smoke receipt")


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
                        "test -s target/image/smoke.json"):
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


if __name__ == "__main__":
    unittest.main()
