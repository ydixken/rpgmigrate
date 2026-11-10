#!/usr/bin/env python3
"""Run pinned native Linux ASAN checks without installing tools or printing child output."""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import time
import tomllib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests" / "support"))
from db_harness import module_cases, validate_libtest_output

TARGETS = {"x86_64-unknown-linux-gnu": ("x86_64", 62, "amd64"),
           "aarch64-unknown-linux-gnu": ("aarch64", 183, "arm64")}
ASAN_OPTIONS = "detect_leaks=1:abort_on_error=1:detect_stack_use_after_return=1"
DIAGNOSTIC = re.compile(r"AddressSanitizer|LeakSanitizer|\bASan\b|\bLSan\b|Sanitizer CHECK failed", re.IGNORECASE)
CATEGORIES = ("heap-use-after-free", "heap-buffer-overflow", "stack-buffer-overflow", "stack-use-after-return",
              "stack-use-after-scope", "global-buffer-overflow", "double-free", "alloc-dealloc-mismatch", "detected memory leaks")
CONTROL = '''fn main() {
    unsafe {
        let layout = std::alloc::Layout::new::<u64>();
        let ptr = std::alloc::alloc(layout) as *mut u64;
        if ptr.is_null() { std::alloc::handle_alloc_error(layout); }
        std::ptr::write_volatile(ptr, 1);
        std::alloc::dealloc(ptr as *mut u8, layout);
        std::hint::black_box(std::ptr::read_volatile(ptr));
    }
}
'''


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(path):
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError:
        raise RuntimeError("required artifact is missing or unreadable") from None


def load_pin(path):
    try:
        pin = tomllib.loads(Path(path).read_text())
        toolchain = pin["toolchain"]
        require(re.fullmatch(r"nightly-\d{4}-\d{2}-\d{2}", toolchain), "exact dated nightly is required")
        date = toolchain.removeprefix("nightly-")
        datetime.date.fromisoformat(date)
        require(pin["schema_version"] == 1, "unknown toolchain manifest schema")
        base = f"https://static.rust-lang.org/dist/{date}/"
        require(pin["manifest_url"] == base + "channel-rust-nightly.toml", "nonofficial nightly manifest")
        for key, length in (("manifest_sha256", 64), ("compiler_commit", 40)):
            require(re.fullmatch(rf"[0-9a-f]{{{length}}}", pin[key]), "invalid nightly identity")
        release = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)-nightly \([0-9a-f]{9} \d{4}-\d{2}-\d{2}\)", pin["rustc_version"])
        require(release and tuple(map(int, release.groups())) >= (1, 99, 1), "nightly is older than required MSRV")
        require(pin["rustc_version"].split("(")[1].startswith(pin["compiler_commit"][:9]), "compiler commit mismatch")
        require(re.fullmatch(r"0\.\d+\.\d+-nightly \([0-9a-f]+ \d{4}-\d{2}-\d{2}\)", pin["cargo_distribution_version"]), "invalid Cargo distribution version")
        require(pin["compatibility"]["root_rust_version"] == "1.99.1" and pin["compatibility"]["edition"] == "2024", "incompatible root contract")
        require(re.fullmatch(r"[0-9a-f]{64}", pin["compatibility"]["lockfile_sha256"]), "missing locked graph identity")
        for name in ("rustc", "cargo", "rust-src", "rust-std", "llvm-tools-preview"):
            for target in (("*",) if name == "rust-src" else TARGETS):
                component = pin["components"][name][target]
                archive = "llvm-tools" if name == "llvm-tools-preview" else name
                suffix = "" if target == "*" else "-" + target
                require(component["url"] == f"{base}{archive}-nightly{suffix}.tar.xz", "nonofficial component URL")
                require(re.fullmatch(r"[0-9a-f]{64}", component["sha256"]), "missing component checksum")
        return pin
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        raise RuntimeError("missing or malformed pinned toolchain manifest") from None


def native_target(target):
    require(target in TARGETS and platform.system() == "Linux" and
            platform.machine() == TARGETS[target][0], "native Linux target/architecture mismatch")


def qualified_symbolizer(pin, target):
    try:
        external = pin["external_symbolizer"]
        require(external["status"] == "qualified", "external symbolizer requires qualification")
        entry = external["targets"][target]
        require(set(entry) == {"path", "sha256", "version", "package", "package_version", "architecture"}, "incomplete symbolizer qualification")
        require(re.fullmatch(r"/[A-Za-z0-9_./+-]+", entry["path"]) and ".." not in Path(entry["path"]).parts, "invalid symbolizer path")
        require(re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]), "invalid symbolizer checksum")
        require(re.fullmatch(r"(?:[A-Za-z][A-Za-z0-9.+_-]* )?LLVM version [0-9]+\.[0-9]+\.[0-9]+", entry["version"]), "invalid symbolizer version")
        for key in ("package", "package_version"):
            require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.+:~_-]{0,255}", entry[key]), "invalid symbolizer package")
        require(entry["architecture"] == TARGETS[target][2], "symbolizer architecture mismatch")
        return entry
    except (KeyError, TypeError, AttributeError):
        raise RuntimeError("incomplete symbolizer qualification") from None


def child_environment(parent, target_dir, symbolizer):
    require(not any(key in parent for key in ("CFLAGS", "HOST_CFLAGS", "TARGET_CFLAGS")), "bundled native compiler overrides are forbidden")
    require(not any(key in parent for key in ("RUSTC", "CARGO_BUILD_RUSTC", "LD_PRELOAD")), "compiler/loader override is forbidden")
    environment = dict(parent)
    for key in ("RUSTC_BOOTSTRAP", "CARGO_ENCODED_RUSTFLAGS", "LSAN_OPTIONS"):
        environment.pop(key, None)
    environment.update(RUSTFLAGS="-Zsanitizer=address", ASAN_OPTIONS=ASAN_OPTIONS,
                       ASAN_SYMBOLIZER_PATH=str(symbolizer), CARGO_TARGET_DIR=str(target_dir),
                       RUSTC_WRAPPER="", RUSTC_WORKSPACE_WRAPPER="")
    return environment


def check_binary(path, target_dir, target):
    path, target_dir = Path(path), Path(target_dir)
    require(path.is_file() and not path.is_symlink() and os.access(path, os.X_OK) and
            path.resolve().is_relative_to(target_dir.resolve()), "wrong or missing emitted binary")
    header = path.read_bytes()[:20]
    require(len(header) == 20 and header[:6] == b"\x7fELF\x02\x01" and
            int.from_bytes(header[18:20], "little") == TARGETS[target][1], "wrong native ELF binary")
    return digest(path)


def group_exists(pid):
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False


def reap(process):
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            continue
        deadline = time.monotonic() + 5
        while group_exists(process.pid) and time.monotonic() < deadline:
            time.sleep(0.01)
        if not group_exists(process.pid):
            return
    raise RuntimeError("owned child group could not be reaped")


def run(argv, environment, cwd, timeout, *, control=False):
    require(isinstance(argv, list) and argv and all(isinstance(x, str) for x in argv), "child argv must be a nonempty list")
    require(0 < timeout <= 1800, "finite child timeout is required")
    process = subprocess.Popen(argv, cwd=cwd, env=environment, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        reap(process)
        raise RuntimeError("ASAN child timed out") from None
    except BaseException:
        reap(process)
        raise
    if group_exists(process.pid):
        reap(process)
        raise RuntimeError("ASAN child left an owned process running")
    result = subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    if not control:
        if DIAGNOSTIC.search(stdout + stderr):
            category = next((name for name in CATEGORIES if name in stdout + stderr), "unsupported")
            evidence = sanitized_evidence("child", status="sanitizer-failed", returncode=result.returncode,
                                          sanitizer_category=category)
            result.stdout = result.stderr = stdout = stderr = ""
            raise RuntimeError(f"sanitizer diagnostic: {category}; sanitized_evidence_sha256={evidence['sanitized_evidence_sha256']}")
        require(result.returncode == 0, "ASAN child failed")
    return result


def sanitized_evidence(stage, *, status, returncode, timed_out=False, sanitizer_category="none", test_count=0):
    require(stage in ("child", "FFI", "Transport", "Prepared", "Replication"), "unknown evidence stage")
    require(status in ("passed", "failed", "timed-out", "sanitizer-failed"), "unknown evidence status")
    require(sanitizer_category in (*CATEGORIES, "none", "unsupported"), "unknown sanitizer category")
    require((type(returncode) is int and -255 <= returncode <= 255) or returncode is None, "invalid evidence return code")
    require(type(timed_out) is bool and timed_out == (status == "timed-out"), "invalid evidence timeout")
    require(type(test_count) is int and 0 <= test_count <= 4096, "invalid evidence test count")
    require((status == "timed-out") == (returncode is None), "inconsistent evidence return code")
    require((status == "sanitizer-failed") == (sanitizer_category != "none"), "inconsistent sanitizer evidence")
    require(status != "passed" or returncode == 0, "inconsistent passing evidence")
    projection = {"stage": stage, "status": status, "returncode": returncode, "timed_out": timed_out,
                  "sanitizer_category": sanitizer_category, "test_count": test_count}
    encoded = json.dumps(projection, sort_keys=True, separators=(",", ":")).encode()
    return {"sanitized_evidence": projection, "sanitized_evidence_sha256": hashlib.sha256(encoded).hexdigest()}


def validate_tests(expected, output):
    try:
        validate_libtest_output(expected, output)
    except AssertionError:
        raise RuntimeError("empty, ignored, failed or mismatched exact test selection") from None
    summaries = re.findall(r"^test result: (.*)$", output, re.MULTILINE)
    require(len(summaries) == 1 and re.fullmatch(
        rf"ok\. {len(expected)} passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in \S+", summaries[0]),
        "missing or inconsistent libtest receipt")


def validate_profile(profile, expected, output):
    validate_tests(expected, output)
    receipt = f"{profile} verified {len(expected)} exact cases; full E1 suite not selected"
    require(output.splitlines().count(receipt) == 1, "missing exact live profile receipt")
    if profile == "Replication":
        require(output.splitlines().count("Replication faults verified 4 wire cases and observed PQflush backpressure") == 1,
                "missing replication negative-control receipt")


def profile_evidence(profile, expected, result):
    require(result.returncode == 0 and not DIAGNOSTIC.search(result.stdout + result.stderr), "profile child did not pass cleanly")
    if profile == "FFI":
        validate_tests(expected, result.stdout)
    else:
        validate_profile(profile, expected, result.stdout)
    return sanitized_evidence(profile, status="passed", returncode=0, test_count=len(expected))


def validate_control(result):
    require(result.returncode != 0 and re.search(r"ERROR: AddressSanitizer: heap-use-after-free\b", result.stdout + result.stderr),
            "sanitizer control did not detect intentional heap-use-after-free")


def linked_libpq(output):
    values = re.findall(r"^\s*libpq\.so\.5 => (/[^\s]+) \([^\n]+\)$", output, re.MULTILINE)
    require(len(values) == 1, "missing or ambiguous linked libpq")
    return Path(values[0]).resolve()


def publish_receipt(path, receipt):
    require(isinstance(receipt, dict) and receipt.get("profiles") and receipt.get("control") == "heap-use-after-free" and
            all(item.get("cases") for item in receipt["profiles"]), "empty or incomplete ASAN receipt")
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "w") as handle:
            json.dump(receipt, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError:
        raise RuntimeError("private receipt already exists or cannot be written") from None


def artifact(output, *, test):
    artifacts = []
    try:
        for line in output.splitlines():
            message = json.loads(line)
            if message.get("reason") == "compiler-artifact" and message.get("target", {}).get("name") == "rpgmigrate" and message.get("profile", {}).get("test") is test and message.get("executable"):
                artifacts.append(Path(message["executable"]))
    except (ValueError, TypeError):
        raise RuntimeError("invalid Cargo artifact receipt") from None
    require(len(artifacts) == 1, "missing or ambiguous Cargo executable receipt")
    return artifacts[0]


def package(path, execute, architecture):
    owner = execute(["dpkg-query", "-S", str(path)]).stdout.strip()
    match = re.fullmatch(r"([a-z0-9][a-z0-9+.-]*)(?::(amd64|arm64))?: " + re.escape(str(path)), owner)
    require(match, "artifact lacks exact installed package ownership")
    identifier = match[1] + (":" + match[2] if match[2] else "")
    value = execute(["dpkg-query", "-W", "-f=${Package}\t${Version}\t${Architecture}\t${Status}", identifier]).stdout.strip().split("\t")
    require(len(value) == 4 and value[0] == match[1] and value[2] == architecture and value[3] == "install ok installed" and
            re.fullmatch(r"[A-Za-z0-9.+:~_-]{1,256}", value[1]), "installed package identity mismatch")
    return {"package": value[0], "package_version": value[1], "architecture": value[2]}


def execute(args):
    pin = load_pin(args.manifest)
    symbolizer = qualified_symbolizer(pin, args.target)
    native_target(args.target)
    require(pin["compatibility"]["license_security_review"] == "qualified", "toolchain license/security review is required")
    require(digest(ROOT / "Cargo.lock") == pin["compatibility"]["lockfile_sha256"], "locked graph changed since pin review")
    require(not Path(args.receipt).exists(), "receipt must be fresh")
    target_dir = Path(args.target_dir).absolute()
    require(not target_dir.exists() and not target_dir.resolve().is_relative_to(ROOT / "target"), "ASAN target directory must be fresh and disjoint")
    environment = child_environment(os.environ, target_dir, symbolizer["path"])
    invoke = lambda argv, **kwargs: run(argv, environment, ROOT, args.timeout, **kwargs)
    require(digest(symbolizer["path"]) == symbolizer["sha256"], "symbolizer checksum mismatch")
    version = invoke([symbolizer["path"], "--version"]).stdout
    require(symbolizer["version"] in version.splitlines(), "symbolizer version mismatch")
    require(package(Path(symbolizer["path"]).resolve(), invoke, TARGETS[args.target][2]) ==
            {key: symbolizer[key] for key in ("package", "package_version", "architecture")}, "symbolizer package mismatch")
    tool = ["rustup", "run", pin["toolchain"]]
    rustc = invoke(tool + ["rustc", "--version", "--verbose"]).stdout
    require(f"commit-hash: {pin['compiler_commit']}" in rustc.splitlines() and
            f"host: {args.target}" in rustc.splitlines() and rustc.splitlines()[0] == "rustc " + pin["rustc_version"], "installed compiler differs from pin")
    cargo = invoke(tool + ["cargo", "--version"]).stdout.strip()
    require(cargo == "cargo 1." + pin["cargo_distribution_version"].removeprefix("0."), "installed Cargo differs from pin")
    components = invoke(["rustup", "component", "list", "--installed", "--toolchain", pin["toolchain"]]).stdout.splitlines()
    require("rust-src" in components and f"llvm-tools-{args.target}" in components, "required installed Rust components missing")
    target_dir.mkdir(mode=0o700, parents=True)
    cargo_argv = tool + ["cargo"]
    flags = ["--locked", "-Zbuild-std", "--target", args.target, "--message-format=json"]
    binary = artifact(invoke(cargo_argv + ["build", "--bin", "rpgmigrate"] + flags).stdout, test=False)
    tests = artifact(invoke(cargo_argv + ["test", "--no-run", "--lib"] + flags).stdout, test=True)
    require(binary == target_dir / args.target / "debug" / "rpgmigrate", "wrong emitted CLI binary")
    hashes = {"binary_sha256": check_binary(binary, target_dir, args.target), "test_binary_sha256": check_binary(tests, target_dir, args.target)}
    libpq = linked_libpq(invoke(["ldd", str(tests)]).stdout)
    require(linked_libpq(invoke(["ldd", str(binary)]).stdout) == libpq, "CLI and test executable link different libpq")
    libpq_package = package(libpq, invoke, TARGETS[args.target][2])
    require(libpq_package["package"] == "libpq5", "linked libpq is not the stock installed package")
    libpq_version = invoke([sys.executable, "-I", "-c", "import ctypes,sys; p=ctypes.CDLL(sys.argv[1]); p.PQlibVersion.restype=ctypes.c_int; print(p.PQlibVersion())", str(libpq)]).stdout.strip()
    require(re.fullmatch(r"18[0-9]{4}", libpq_version), "unsupported stock libpq version")
    source = target_dir / "asan-control.rs"
    source.write_text(CONTROL)
    control = target_dir / "asan-control"
    invoke(tool + ["rustc", "--edition=2024", "--target", args.target, "-Zsanitizer=address", "-Copt-level=0", str(source), "-o", str(control)])
    control_hash = check_binary(control, target_dir, args.target)
    validate_control(invoke([str(control)], control=True))
    listing = invoke([str(tests), "--list", "--format=terse"]).stdout
    inventory = re.findall(r"^(\S+): test$", listing, re.MULTILINE)
    require(inventory and len(inventory) == len(set(inventory)), "missing or duplicate libtest inventory")
    ffi = [name for name in inventory if name.startswith("pg::ffi::tests::")]
    require(ffi, "empty FFI unit selection")
    result = invoke([str(tests), *ffi, "--exact", "--test-threads=1"])
    profiles = [{"profile": "FFI", "cases": ffi, "test_count": len(ffi), **profile_evidence("FFI", ffi, result)}]
    del result
    manifest = json.loads((ROOT / "tests/fixtures/e1/manifest.json").read_text())
    for major, bindir in ((16, args.pg16_bindir), (18, args.pg18_bindir)):
        postgres_version = invoke([str(Path(bindir) / "postgres"), "--version"]).stdout
        match = re.fullmatch(r"postgres \(PostgreSQL\) (" + str(major) + r")\.([0-9]+)(?: [^\n]+)?\n?", postgres_version)
        require(match and int(match[2]) < 10000, "wrong PostgreSQL server bundle")
        pg_version = f"{major}.{int(match[2])}"
        live_environment = {**environment, "PG_BINDIR": str(Path(bindir).resolve()), "RPGMIGRATE_BIN": str(binary),
                            "RPGMIGRATE_TEST_EXPECTED_PG_VERSION": pg_version,
                            "RPGMIGRATE_TEST_EXPECTED_PG_VERSION_NUM": str(major * 10000 + int(match[2]))}
        for profile in ("Transport", "Prepared", "Replication"):
            expected = module_cases(manifest, profile)
            require(expected and set(expected) <= set(inventory), "empty or undiscovered live profile")
            argv = [sys.executable, str(ROOT / "tools/test-db.py"), "--" + profile.lower(), "--", str(tests),
                    *expected, "--exact", "--ignored", "--test-threads=1"]
            result = run(argv, live_environment, ROOT, args.timeout)
            profiles.append({"profile": profile, "postgres_version": pg_version, "cases": expected,
                             "test_count": len(expected), **profile_evidence(profile, expected, result)})
            del result
    receipt = {"schema_version": 1, "target": args.target, "machine": platform.machine(), "toolchain": pin["toolchain"],
               "compiler_commit": pin["compiler_commit"], "rustc_version": pin["rustc_version"],
               "cargo": cargo, "manifest_sha256": pin["manifest_sha256"],
               "lockfile_sha256": pin["compatibility"]["lockfile_sha256"], "rustflags": environment["RUSTFLAGS"],
               "build_std": True, "asan_options": ASAN_OPTIONS, "symbolizer": symbolizer,
               "libpq": {"path": str(libpq), "sha256": digest(libpq), "version": int(libpq_version), **libpq_package},
               "profiles": profiles, "control": "heap-use-after-free", "control_binary_sha256": control_hash,
               "owned_children_reaped": True,
               "coverage": pin["external_symbolizer"]["coverage"], **hashes}
    publish_receipt(args.receipt, receipt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path(__file__).with_name("ffi_asan_toolchain.toml"))
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--pg16-bindir", type=Path, required=True)
    parser.add_argument("--pg18-bindir", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=900)
    args = parser.parse_args()
    try:
        execute(args)
    except RuntimeError as error:
        print(f"ffi-asan: {error}; no success receipt", file=sys.stderr)
        return 1
    except (OSError, ValueError, KeyError, TypeError, AssertionError):
        print("ffi-asan: qualification or execution failed; no success receipt", file=sys.stderr)
        return 1
    print("ffi-asan: exact native ASAN profiles verified; private receipt written")
    return 0


if __name__ == "__main__":
    sys.exit(main())
