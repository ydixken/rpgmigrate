"""Inspect the portable Python suite inventory before importing test modules."""

import ast
from pathlib import Path


HARNESS_SUITES = (
    "tests/support/test_harness.py", "tests/support/test_catalog_harness.py",
    "tests/support/test_source_catalog_harness.py", "tests/support/test_source_snapshot_harness.py",
    "tests/support/test_copy_snapshot_harness.py",
)
CI_SUITES = ("tools/ci/test_verify.py", "tools/ci/test_workflow.py", "tests/test_taskfile.py", "tests/test_native_fixtures.py", "tools/e2e/test_clone.py", "tools/image/test_smoke.py", *HARNESS_SUITES)


class VerificationError(RuntimeError):
    pass


def _unit_inventory(root, suites):
    root = Path(root) if root is not None else Path(__file__).resolve().parents[2]
    inventory = {}
    for name in suites:
        try:
            tree = ast.parse((root / name).read_text())
        except (OSError, SyntaxError):
            raise VerificationError("CI unit source is missing or invalid") from None
        count = sum(isinstance(method, ast.FunctionDef) and method.name.startswith("test_")
                    for node in tree.body if isinstance(node, ast.ClassDef) for method in node.body)
        if count == 0:
            raise VerificationError("CI unit source inventory is empty")
        inventory[name] = count
    return inventory


def ci_unit_inventory(root=None):
    return _unit_inventory(root, CI_SUITES)


def harness_unit_inventory(root=None):
    return _unit_inventory(root, HARNESS_SUITES)
