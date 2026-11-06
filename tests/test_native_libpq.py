"""Run with python3 -m unittest discover -s tests -p test_native_libpq.py -v."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools/build-libpq.py"
builder = None
if SCRIPT.is_file():
    spec = importlib.util.spec_from_file_location("build_libpq", SCRIPT)
    builder = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(builder)


class NativeLibpqTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(builder, "the controlled libpq builder is missing")
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_accepts_matching_archive_checksum(self):
        archive = self.root / "source.tar.bz2"
        archive.write_bytes(b"abc")
        builder.verify_sha256(
            archive,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        )

    def test_bad_archive_checksum_fails_before_extraction(self):
        archive = self.root / "source.tar.bz2"
        archive.write_bytes(b"not the official archive")
        destination = self.root / "extracted"
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            builder.extract_archive(archive, "0" * 64, destination)
        self.assertFalse(destination.exists())

    def patch_fixture(self):
        source = self.root / "source"
        source.mkdir()
        (source / "sample.c").write_bytes(b"before\n")
        patch = self.root / "minimal.patch"
        patch.write_bytes(
            b"--- a/sample.c\n+++ b/sample.c\n@@ -1 +1 @@\n-before\n+after\n"
        )
        metadata = {
            "patches": [{
                "file": patch.name,
                "sha256": hashlib.sha256(patch.read_bytes()).hexdigest(),
                "before": {"sample.c": hashlib.sha256(b"before\n").hexdigest()},
                "after": {"sample.c": hashlib.sha256(b"after\n").hexdigest()},
            }]
        }
        return source, patch, metadata

    def test_exact_patch_changes_expected_bytes(self):
        source, _, metadata = self.patch_fixture()
        builder.apply_patches(source, metadata, self.root)
        self.assertEqual((source / "sample.c").read_bytes(), b"after\n")

    def test_patch_drift_is_rejected_without_changing_source(self):
        source, _, metadata = self.patch_fixture()
        (source / "sample.c").write_bytes(b"unreviewed\nbefore\n")
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            builder.apply_patches(source, metadata, self.root)
        self.assertEqual((source / "sample.c").read_bytes(), b"unreviewed\nbefore\n")

    def test_modified_patch_is_rejected(self):
        source, patch, metadata = self.patch_fixture()
        patch.write_bytes(patch.read_bytes().replace(b"after", b"other"))
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            builder.apply_patches(source, metadata, self.root)
        self.assertEqual((source / "sample.c").read_bytes(), b"before\n")

    def test_unexpected_patch_result_is_rejected(self):
        source, _, metadata = self.patch_fixture()
        metadata["patches"][0]["after"]["sample.c"] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            builder.apply_patches(source, metadata, self.root)

    def compile_library(self, marker):
        source = self.root / "fixture.c"
        source.write_text("int PQlibVersion(void) { return 180006; }\n" + marker)
        library = self.root / ("fixture.dylib" if sys.platform == "darwin" else "fixture.so")
        command = shlex.split(os.environ.get("CC", "clang"))
        command += ["-dynamiclib"] if sys.platform == "darwin" else ["-shared", "-fPIC"]
        subprocess.run(command + [str(source), "-o", str(library)], check=True, capture_output=True)
        return library

    def test_stock_version_without_marker_is_rejected(self):
        library = self.compile_library("")
        with self.assertRaisesRegex(RuntimeError, "capability"):
            builder.verify_library(library)

    def test_marker_with_wrong_capabilities_is_rejected(self):
        library = self.compile_library("unsigned int PQrpgmigrateCapabilities(void) { return 0U; }\n")
        with self.assertRaisesRegex(RuntimeError, "capability"):
            builder.verify_library(library)

    def test_foreign_library_directory_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "PQ_LIB_DIR"):
            builder.check_library_directory(self.root / "owned", str(self.root / "system"))

    def test_missing_receipt_cannot_pass_verification(self):
        with self.assertRaisesRegex(RuntimeError, "receipt"):
            builder.verify_install(self.root, "recipe")

    def test_empty_artifact_manifest_cannot_pass_verification(self):
        (self.root / "artifact.json").write_text(json.dumps({
            "recipe_sha256": "recipe", "files": {}, "artifact_sha256": builder.identity({}),
        }))
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            builder.verify_install(self.root, "recipe")

    def test_receipt_rejects_changed_artifact_bytes(self):
        library = self.root / "library"
        library.write_bytes(b"reviewed")
        files = {"library": hashlib.sha256(b"reviewed").hexdigest()}
        (self.root / "artifact.json").write_text(json.dumps({
            "recipe_sha256": "recipe", "files": files, "artifact_sha256": builder.identity(files),
        }))
        library.write_bytes(b"replaced")
        with self.assertRaisesRegex(RuntimeError, "SHA-256"):
            builder.verify_install(self.root, "recipe")

    def test_artifact_symlink_cannot_escape_owned_prefix(self):
        prefix = self.root / "prefix"
        prefix.mkdir()
        (self.root / "external").write_bytes(b"unowned")
        (prefix / "library").symlink_to(self.root / "external")
        with self.assertRaisesRegex(RuntimeError, "symlink"):
            builder.artifact_files(prefix)

    def test_nested_receipt_named_file_is_still_hashed(self):
        (self.root / "share").mkdir()
        (self.root / "share/artifact.json").write_bytes(b"installed file")
        self.assertIn("share/artifact.json", builder.artifact_files(self.root))

    def test_rebuild_refuses_external_install_symlinks_before_building(self):
        project = self.root / "project"
        (project / "tools").mkdir(parents=True)
        shutil.copyfile(SCRIPT, project / "tools/build-libpq.py")
        prefix = project / "target/native/libpq"
        prefix.mkdir(parents=True)
        external = self.root / "unowned"
        external.mkdir()
        (prefix / "lib").symlink_to(external, target_is_directory=True)
        env = os.environ.copy()
        env.pop("PQ_LIB_DIR", None)
        result = subprocess.run([sys.executable, str(project / "tools/build-libpq.py"), "--rebuild"], env=env, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("external symlink", result.stderr)
        self.assertEqual(list(external.iterdir()), [])

    def test_parent_make_level_does_not_skip_generated_headers(self):
        with mock.patch.dict(os.environ, {"MAKELEVEL": "9"}):
            env, _ = builder.build_environment()
        self.assertFalse("MAKELEVEL" in env, "parent make level leaked into the native build")

    def test_rebuilt_archives_ignore_input_file_timestamps(self):
        env, _ = builder.build_environment()
        source = self.root / "object.c"
        source.write_text("int fixture(void) { return 1; }\n")
        object_file = self.root / "object.o"
        subprocess.run(shlex.split(env["CC"]) + ["-c", str(source), "-o", str(object_file)], check=True, capture_output=True)
        archives = []
        for name, timestamp in (("first.a", 1600000000), ("second.a", 1700000000)):
            os.utime(object_file, (timestamp, timestamp))
            archive = self.root / name
            subprocess.run(["ar", "crs", str(archive), str(object_file)], env=env, check=True, capture_output=True)
            archives.append(archive.read_bytes())
        self.assertEqual(archives[0], archives[1])


class PinnedSourceTests(unittest.TestCase):
    def test_official_source_patch_changes_only_the_two_diagnostic_writes(self):
        metadata_path = ROOT / "native/libpq/source.json"
        self.assertTrue(metadata_path.is_file(), "official source pin and reviewed patch are missing")
        metadata = json.loads(metadata_path.read_text())
        archive = ROOT / "target/native/downloads" / metadata["archive"]
        self.assertTrue(archive.is_file(), "run the controlled build to populate its download cache")
        builder.verify_sha256(archive, metadata["sha256"])
        with tempfile.TemporaryDirectory() as temporary, tarfile.open(archive) as source:
            root = Path(temporary)
            for patch in metadata["patches"]:
                for name in patch["before"]:
                    path = root / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(source.extractfile(metadata["source_directory"] + "/" + name).read())
            connection = root / "src/interfaces/libpq/fe-connect.c"
            expected = connection.read_bytes()
            for message in (
                b'WARNING: password file \\"%s\\" is not a plain file\\n',
                b'WARNING: password file \\"%s\\" has group or world access; permissions should be u=rw (0600) or less\\n',
            ):
                diagnostic = b'\t\tfprintf(stderr,\n\t\t\t\tlibpq_gettext("' + message + b'"),\n\t\t\t\tpgpassfile);\n'
                self.assertEqual(expected.count(diagnostic), 1, "upstream diagnostic locations changed")
                expected = expected.replace(diagnostic, b"")
            expected = expected.replace(b"If password file is insecure, alert the user and ignore it.", b"If password file is insecure, ignore it.")
            builder.apply_patches(root, metadata)
            self.assertEqual(connection.read_bytes(), expected)


class InstalledArtifactTests(unittest.TestCase):
    def test_real_built_library_and_hash_receipt_verify(self):
        result = subprocess.run([sys.executable, str(SCRIPT), "--verify"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(result.stdout)
        self.assertEqual(report["capabilities"], 1)
        library = builder.library_path(ROOT / "target/native/libpq")
        self.assertEqual(report["library_sha256"], hashlib.sha256(library.read_bytes()).hexdigest())

    def test_installed_library_replacement_fails_hash_verification(self):
        prefix = ROOT / "target/native/libpq"
        self.assertTrue((prefix / "artifact.json").is_file(), "build the controlled artifact first")
        with tempfile.TemporaryDirectory() as temporary:
            replacement = Path(temporary) / "libpq"
            shutil.copytree(prefix, replacement, symlinks=True)
            builder.library_path(replacement).write_bytes(b"unpatched replacement")
            receipt = json.loads((replacement / "artifact.json").read_text())
            with self.assertRaisesRegex(RuntimeError, "SHA-256"):
                builder.verify_install(replacement, receipt["recipe_sha256"])


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    if suite.countTestCases() == 0:
        sys.exit("error: no native libpq tests discovered")
    sys.exit(not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful())
