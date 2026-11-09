"""Verify the local native fixture inventory without another repository."""

import copy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent / "fixtures/native/v1"
KINDS = {"version", "report", "transport", "compare_array", "compare_progress", "argv"}
QUALIFIED_CAPABILITIES = [
    "clone.single_db", "cdc.pgoutput", "progress.v1", "cutover.v1", "verify.v1", "cleanup.v1",
]


LINUX_QUALIFIED_CAPABILITIES = QUALIFIED_CAPABILITIES + ["verify.sealed_files.v1"]
RESERVED_CAPABILITIES = [
    "clone.single_db", "copy.large_objects", "copy.binary_fallback", "copy.split", "cdc.pgoutput",
    "progress.v1", "cutover.v1", "verify.v1", "cleanup.v1", "verify.sealed_files.v1",
]


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate fixture key")
        result[key] = value
    return result


def read_json(path):
    return json.loads(path.read_bytes(), object_pairs_hook=unique_object)


def verify_inventory(root):
    manifest = read_json(root / "manifest.json")
    if set(manifest) != {"protocol_version", "files"} or manifest["protocol_version"] != 1:
        raise ValueError("invalid fixture manifest")
    entries = manifest["files"]
    if not isinstance(entries, list) or not entries:
        raise ValueError("empty fixture inventory")
    names = []
    for entry in entries:
        if set(entry) != {"path", "sha256"}:
            raise ValueError("invalid inventory entry")
        name = entry["path"]
        if not isinstance(name, str) or Path(name).as_posix() != name or Path(name).is_absolute() or ".." in Path(name).parts:
            raise ValueError("invalid inventory path")
        names.append(name)
        path = root / name
        if path.is_symlink() or not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != entry["sha256"]:
            raise ValueError("missing or changed fixture")
    actual = sorted(path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file() and path.name != "manifest.json")
    if names != sorted(set(names)) or names != actual:
        raise ValueError("fixture inventory differs")
    cases = read_json(root / "cases.json")
    if cases.get("protocol_version") != 1 or not isinstance(cases.get("cases"), list) or not cases["cases"]:
        raise ValueError("empty or invalid cases")
    seen, payloads = set(), set()
    for case in cases["cases"]:
        if not case.get("id") or case["id"] in seen or case.get("kind") not in KINDS or type(case.get("accept")) is not bool:
            raise ValueError("invalid fixture case")
        seen.add(case["id"])
        if case["kind"] == "argv":
            if not case.get("argv") or not isinstance(case.get("env"), dict) or not isinstance(case.get("expect"), dict):
                raise ValueError("invalid argv case")
        elif case.get("payload") not in names:
            raise ValueError("missing case payload")
        else:
            payloads.add(case["payload"])
    if payloads | set(cases.get("support_files", [])) != set(names) - {"contract.json", "cases.json"}:
        raise ValueError("unreferenced fixture payload")
    contract = read_json(root / "contract.json")
    if contract.get("protocol_version") != 1 or contract.get("m1_capabilities") != []:
        raise ValueError("invalid native contract")
    if contract.get("capabilities") != RESERVED_CAPABILITIES:
        raise ValueError("invalid reserved capabilities")
    for case_id, metadata, capabilities in [
        ("version-qualified-profile", "qualified_capabilities", QUALIFIED_CAPABILITIES),
        ("version-qualified-linux-profile", "qualified_linux_capabilities", LINUX_QUALIFIED_CAPABILITIES),
    ]:
        if contract.get(metadata) != capabilities:
            raise ValueError("invalid qualified capabilities")
        qualified = [case for case in cases["cases"] if case["id"] == case_id]
        if qualified != [{
            "id": case_id, "kind": "version", "payload": "payloads/" + case_id + ".json", "accept": True,
        }]:
            raise ValueError("invalid qualified version case")
        version = read_json(root / qualified[0]["payload"])
        if version != {
            "protocol_version": 1, "engine_version": "0.1.0", "capabilities": capabilities,
        } or type(version["protocol_version"]) is not int:
            raise ValueError("qualified version differs from contract")
    options = contract["options"]
    if len(options) != 43 or len({option["name"] for option in options}) != 43:
        raise ValueError("operator option inventory differs")
    argv_ids = {case["id"] for case in cases["cases"] if case["kind"] == "argv"}
    if any("argv-option-" + option["name"][2:] not in argv_ids for option in options):
        raise ValueError("operator option lacks a case")
    if set(case["kind"] for case in cases["cases"]) != KINDS:
        raise ValueError("missing fixture case kind")
    return cases["cases"]


class NativeFixtureTests(unittest.TestCase):
    def test_linux_metadata_preserves_portable_profile_and_extends_discovery(self):
        contract = read_json(ROOT / "contract.json")
        self.assertEqual(contract.get("qualified_capabilities"), QUALIFIED_CAPABILITIES)
        self.assertEqual(contract.get("qualified_linux_capabilities"), QUALIFIED_CAPABILITIES + ["verify.sealed_files.v1"])
        cases = verify_inventory(ROOT)
        qualified = [case for case in cases if case["id"] == "version-qualified-linux-profile"]
        self.assertEqual(len(qualified), 1)
        self.assertTrue(qualified[0]["accept"])
        self.assertEqual(read_json(ROOT / qualified[0]["payload"]), {
            "protocol_version": 1, "engine_version": "0.1.0",
            "capabilities": QUALIFIED_CAPABILITIES + ["verify.sealed_files.v1"],
        })

    def test_local_inventory_is_complete(self):
        self.assertGreater(len(verify_inventory(ROOT)), 0)

    def mutate(self, action, refresh=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "v1"
            shutil.copytree(ROOT, root)
            action(root)
            if refresh:
                manifest = read_json(root / "manifest.json")
                for entry in manifest["files"]:
                    entry["sha256"] = hashlib.sha256((root / entry["path"]).read_bytes()).hexdigest()
                (root / "manifest.json").write_text(json.dumps(manifest))
            with self.assertRaises((ValueError, OSError)):
                verify_inventory(root)

    def change_json(self, root, name, change):
        data = read_json(root / name)
        change(data)
        (root / name).write_text(json.dumps(data))

    def test_qualified_metadata_matches_producer_golden(self):
        self.assertEqual(read_json(ROOT / "contract.json").get("qualified_capabilities"), QUALIFIED_CAPABILITIES)
        cases = verify_inventory(ROOT)
        qualified = [case for case in cases if case["id"] == "version-qualified-profile"]
        self.assertEqual(qualified, [{
            "id": "version-qualified-profile", "kind": "version",
            "payload": "payloads/version-qualified-profile.json", "accept": True,
        }])
        self.assertEqual(read_json(ROOT / qualified[0]["payload"]), {
            "protocol_version": 1, "engine_version": "0.1.0", "capabilities": QUALIFIED_CAPABILITIES,
        })
        empty = next(case for case in cases if case["id"] == "version-empty-capabilities")
        self.assertTrue(empty["accept"])
        self.assertEqual(read_json(ROOT / empty["payload"])["capabilities"], [])

    def test_invalid_qualified_metadata_is_refused_even_with_updated_digest(self):
        invalid = [None, "clone.single_db", [], QUALIFIED_CAPABILITIES + ["unknown"],
                   QUALIFIED_CAPABILITIES + ["clone.single_db"], list(reversed(QUALIFIED_CAPABILITIES))]
        invalid += [QUALIFIED_CAPABILITIES[:i] + QUALIFIED_CAPABILITIES[i + 1:] for i in range(len(QUALIFIED_CAPABILITIES))]
        invalid += [QUALIFIED_CAPABILITIES + [capability] for capability in ["copy.large_objects", "copy.binary_fallback", "copy.split"]]
        for capabilities in invalid:
            with self.subTest(capabilities=capabilities):
                self.mutate(lambda root: self.change_json(root, "contract.json", lambda data: data.update(qualified_capabilities=capabilities)), refresh=True)
        self.mutate(lambda root: self.change_json(root, "contract.json", lambda data: data.pop("qualified_capabilities", None)), refresh=True)

    def test_qualified_golden_drift_is_refused_even_with_updated_digest(self):
        for change in [
            lambda data: data.update(capabilities=[]),
            lambda data: data.update(capabilities=list(reversed(QUALIFIED_CAPABILITIES))),
            lambda data: data.update(engine_version="0.2.0"),
            lambda data: data.update(protocol_version=2),
            lambda data: data.update(protocol_version=True),
            lambda data: data.update(protocol_version=1.0),
            lambda data: data.update(qualified_capabilities=QUALIFIED_CAPABILITIES),
            lambda data: data.pop("capabilities"),
        ]:
            with self.subTest(change=change):
                self.mutate(lambda root: self.change_json(root, "payloads/version-qualified-profile.json", change), refresh=True)

    def test_qualified_case_drift_is_refused_even_with_updated_digest(self):
        for change in [
            lambda case: case.update(accept=False),
            lambda case: case.update(kind="report"),
            lambda case: case.update(id="renamed-qualified-version"),
            lambda case: case.update(payload="payloads/version-empty-capabilities.json"),
        ]:
            with self.subTest(change=change):
                def mutate(root):
                    self.change_cases(root, lambda data: change(next(case for case in data["cases"] if case["id"] == "version-qualified-profile")))
                self.mutate(mutate, refresh=True)

    def test_linux_metadata_and_golden_drift_refused_with_updated_digests(self):
        invalid = [None, [], QUALIFIED_CAPABILITIES, LINUX_QUALIFIED_CAPABILITIES + ["verify.sealed_files.v1"],
                   LINUX_QUALIFIED_CAPABILITIES + ["unknown"], list(reversed(LINUX_QUALIFIED_CAPABILITIES))]
        invalid += [LINUX_QUALIFIED_CAPABILITIES[:i] + LINUX_QUALIFIED_CAPABILITIES[i + 1:]
                    for i in range(len(LINUX_QUALIFIED_CAPABILITIES))]
        for capabilities in invalid:
            with self.subTest(capabilities=capabilities):
                self.mutate(lambda root: self.change_json(root, "contract.json", lambda data: data.update(qualified_linux_capabilities=capabilities)), refresh=True)
                self.mutate(lambda root: self.change_json(root, "payloads/version-qualified-linux-profile.json", lambda data: data.update(capabilities=capabilities)), refresh=True)
        self.mutate(lambda root: self.change_json(root, "contract.json", lambda data: data.pop("qualified_linux_capabilities")), refresh=True)
        self.mutate(lambda root: self.change_json(root, "contract.json", lambda data: data.update(capabilities=RESERVED_CAPABILITIES[:-1])), refresh=True)
        for change in [lambda case: case.update(accept=False), lambda case: case.update(kind="report"),
                       lambda case: case.update(id="renamed-linux-version"),
                       lambda case: case.update(payload="payloads/version-qualified-profile.json")]:
            with self.subTest(change=change):
                self.mutate(lambda root: self.change_cases(root, lambda data: change(next(case for case in data["cases"] if case["id"] == "version-qualified-linux-profile"))), refresh=True)

    def test_missing_payload_is_refused(self):
        self.mutate(lambda root: (root / "payloads/progress-copying.json").unlink())

    def test_changed_payload_is_refused(self):
        self.mutate(lambda root: (root / "payloads/progress-copying.json").write_text("{}"))

    def test_extra_file_is_refused(self):
        self.mutate(lambda root: (root / "extra.json").write_text("{}"))

    def change_manifest(self, root, change):
        data = read_json(root / "manifest.json")
        change(data)
        (root / "manifest.json").write_text(json.dumps(data))

    def test_duplicate_manifest_path_is_refused(self):
        self.mutate(lambda root: self.change_manifest(root, lambda data: data["files"].append(copy.deepcopy(data["files"][0]))))

    def test_missing_manifest_entry_is_refused(self):
        self.mutate(lambda root: self.change_manifest(root, lambda data: data["files"].pop()))

    def test_unsorted_manifest_is_refused(self):
        self.mutate(lambda root: self.change_manifest(root, lambda data: data["files"].reverse()))

    def change_cases(self, root, change):
        data = read_json(root / "cases.json")
        change(data)
        (root / "cases.json").write_text(json.dumps(data))

    def test_empty_cases_are_refused_even_with_updated_digest(self):
        self.mutate(lambda root: self.change_cases(root, lambda data: data.update(cases=[])), refresh=True)

    def test_duplicate_case_is_refused_even_with_updated_digest(self):
        self.mutate(lambda root: self.change_cases(root, lambda data: data["cases"].append(copy.deepcopy(data["cases"][0]))), refresh=True)

    def test_missing_case_payload_is_refused_even_with_updated_digest(self):
        self.mutate(lambda root: self.change_cases(root, lambda data: data["cases"][0].update(payload="missing.json")), refresh=True)

    def test_duplicate_json_member_is_refused(self):
        with self.assertRaises(ValueError):
            json.loads('{"protocol_version":1,"protocol_version":1}', object_pairs_hook=unique_object)

    def test_later_begin_context_preserves_feedback_limit(self):
        case = next(case for case in verify_inventory(ROOT) if case["id"] == "drain-later_begin")
        self.assertEqual(case["producer_context"], {
            "applied_lsn": "0/10", "certified_lsn": "0/10", "copy_anchor_lsn": "0/8",
            "end_lsn": "0/20", "begin_final_lsn": "0/30", "durable_replay_lsn": "0/10",
        })
        payload = read_json(ROOT / case["payload"])
        self.assertEqual(payload["replication"]["durable_replay_lsn"], "0/10")
        self.assertEqual(payload["replication"]["drain_proof"]["replay_lsn"], "0/30")


if __name__ == "__main__":
    unittest.main()
