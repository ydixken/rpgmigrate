"""Exercise the host launcher against a recording engine without containers."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("linux.sh")


class LauncherTests(unittest.TestCase):
    def launch(self, image_id):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "tools/ci").mkdir(parents=True)
            shutil.copyfile(SCRIPT, root / "tools/ci/linux.sh")
            (root / "tools/ci/Containerfile").write_text("fixture Containerfile")
            (root / "tools/bootstrap-task.py").write_text("shared bootstrap bytes")
            binary = root / "podman"
            binary.write_text("#!" + sys.executable + "\n" + '''import json, os, pathlib, sys
args = sys.argv[1:]
root = pathlib.Path(os.environ['FIXTURE_ROOT'])
with (root / 'calls').open('a') as log:
    log.write(json.dumps(args) + '\\n')
if args[0] == 'build':
    context = pathlib.Path(args[-1])
    assert context != root
    assert (context / 'bootstrap-task.py').read_bytes() == (root / 'tools/bootstrap-task.py').read_bytes()
    pathlib.Path(args[args.index('--iidfile') + 1]).write_text('sha256:' + 'a' * 64)
elif args[:2] == ['image', 'inspect']:
    print(os.environ['FIXTURE_ID'])
elif args[0] == 'cp':
    pathlib.Path(args[-1]).write_text('{"status":"passed"}\\n')
''')
            binary.chmod(0o755)
            output = root / "outputs"
            result = subprocess.run(["bash", str(root / "tools/ci/linux.sh")], capture_output=True, text=True,
                                    env={**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                                         "FIXTURE_ROOT": str(root), "FIXTURE_ID": image_id, "GITHUB_OUTPUT": str(output)})
            calls = [json.loads(line) for line in (root / "calls").read_text().splitlines()]
            return result, output.read_text() if output.exists() else "", calls

    def test_podman_raw_image_id_is_normalized_and_bootstrap_context_is_minimal(self):
        result, output, calls = self.launch("a" * 64)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("fixture_image=sha256:" + "a" * 64 + "\n", output)
        create = next(call for call in calls if call[0] == "create")
        self.assertEqual(create[-1], "sha256:" + "a" * 64)
        self.assertTrue(any(call[0] == "rm" for call in calls))

    def test_mutable_or_malformed_image_identity_refuses_before_create(self):
        for identity in ("latest", "sha256:bad", "a" * 64 + "\nprivate-canary"):
            result, output, calls = self.launch(identity)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(output, "")
            self.assertFalse(any(call[0] == "create" for call in calls))
            self.assertNotIn("private-canary", result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
