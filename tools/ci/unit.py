#!/usr/bin/env python3
"""Run the exact offline tooling inventory with one unittest result footer."""

import importlib.util
from pathlib import Path
import sys
import unittest

from verify import ci_unit_inventory, harness_unit_inventory


def main():
    if sys.flags.optimize:
        raise RuntimeError("optimized Python cannot run the CI unit gate")
    arguments = sys.argv[1:]
    if arguments not in ([], ["--harness"]):
        raise RuntimeError("unknown unit test group")
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / "tests/support"))
    inventory = harness_unit_inventory(root) if arguments else ci_unit_inventory(root)
    suite = unittest.TestSuite()
    for number, (name, expected) in enumerate(inventory.items()):
        spec = importlib.util.spec_from_file_location(f"ci_suite_{number}", root / name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        selected = unittest.defaultTestLoader.loadTestsFromModule(module)
        if expected <= 0 or selected.countTestCases() != expected:
            raise RuntimeError("CI unit inventory differs from the reviewed count")
        suite.addTests(selected)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() and not result.skipped else 1


if __name__ == "__main__":
    sys.exit(main())
