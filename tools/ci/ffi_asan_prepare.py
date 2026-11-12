#!/usr/bin/env python3
"""Collect native tool provenance and controls without qualifying project ASAN execution."""

import argparse
import hashlib
import importlib.util
import json
import lzma
import os
from pathlib import Path, PurePosixPath
import re
import resource
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import time
import tomllib


ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("ffi_asan", Path(__file__).with_name("ffi_asan.py"))
asan = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(asan)
COMPONENTS = ("rustc", "rust-std", "cargo", "rust-src", "llvm-tools-preview")
STD_LOCK = "e3007071f73ee4fddcf817e4a19c1ecea766b590c9a5469980341aa79473914e"
DEB_PINS = {"amd64": "139cb82e16e75fcdd4a56562804ff9bfb482b65d0929580d621d28088075a27e",
            "arm64": "1d9c87ff9fe17d0ee00703ec0f339ab687e2807b3508b51cf517e05281de104d"}
SYMBOLIZER = Path("/usr/lib/llvm-18/bin/llvm-symbolizer")
STAGES = ("admission", "toolchain", "graphs", "symbolizer", "control", "retention", "bindings")
ADMISSION_SUBSTAGES = ("uid", "native-architecture", "dpkg-architecture", "os-release", "binfmt", "project-lock",
                      "public-pin", "runner-image", "scratch-disk", "available-memory", "core-limit", "complete")
BINFMT_DETAILS = ("binfmt-lookup-failed", "binfmt-enumeration-failed", "binfmt-entry-read-failed", "binfmt-entry-invalid",
                  "binfmt-budget-exceeded", "binfmt-possible-elf-handler")
NATIVE_ELF_TOOLS = {"self": "/proc/self/exe", "dpkg": "/usr/bin/dpkg", "git": "/usr/bin/git", "curl": "/usr/bin/curl",
                    "bash": "/bin/bash", "sh": "/bin/sh", "apt-get": "/usr/bin/apt-get", "dpkg-query": "/usr/bin/dpkg-query",
                    "dpkg-deb": "/usr/bin/dpkg-deb", "readelf": "/usr/bin/readelf", "gpgv": "/usr/bin/gpgv",
                    "ldconfig-real": "/sbin/ldconfig.real"}
# Ubuntu ships both as scripts; we execute ldconfig.real directly and ldd through the checked bash.
NATIVE_SCRIPTS = {"ldconfig": ("/sbin/ldconfig", b"#!/bin/sh\n"), "ldd": ("/usr/bin/ldd", b"#!/bin/bash\n")}
LDCONFIG_EXEC = b'exec /sbin/ldconfig.real "$@"'
NATIVE_TOOL_REASONS = ("unreadable", "not-regular", "not-native-elf", "interpreter-mismatch", "wrapper-target-mismatch")
NATIVE_TOOL_DETAILS = tuple(tool + "-" + reason for tool in (*NATIVE_ELF_TOOLS, *NATIVE_SCRIPTS) for reason in NATIVE_TOOL_REASONS)
BINFMT_ROOT = Path("/proc/sys/fs/binfmt_misc")
BINFMT_HANDLER_LIMIT = 64
BINFMT_RECORD_LIMIT = 4096
BINFMT_TOTAL_LIMIT = 256 * 1024
TOOLCHAIN_NOTICE_FILE_LIMIT = 16 * 1024 ** 2
TOOLCHAIN_NOTICE_TOTAL_LIMIT = 32 * 1024 ** 2
GIB = 1024 ** 3
HEX = re.compile(r"[0-9a-f]{64}")
PACKAGE = re.compile(r"[a-z0-9][a-z0-9+.-]*(?::(?:amd64|arm64))?")
VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9.+:~_-]{0,255}")
CODES = {"admission", "toolchain", "graphs", "symbolizer", "control", "bindings", "inventory-incomplete",
         "deadline", "child-timeout", "capture-limit", "scratch-limit", "child-failed", "owned-group",
         "input-invalid", "package-authentication", "package-payload", "archive-invalid", "configuration", "retention"}


def require(condition, code="input-invalid"):
    if not condition:
        raise RuntimeError(code)


def sha256(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while data := handle.read(1024 * 1024):
            value.update(data)
    return value.hexdigest()


def write_json(path, value):
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    except OSError:
        raise RuntimeError("input-invalid") from None
    with os.fdopen(descriptor, "w") as handle:
        json.dump(value, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def create_private_run(path, runner_temp):
    path, runner_temp = Path(path), Path(runner_temp)
    require(path.is_absolute() and re.fullmatch(r"[A-Za-z0-9_./+-]+", path.as_posix()) and
            ".." not in path.parts and path.parent.resolve() == runner_temp.resolve() and
            runner_temp.stat().st_uid == os.getuid())
    require(not path.exists() and not path.is_symlink())
    path.mkdir(mode=0o700)
    for name in ("home", "cargo", "rustup", "tmp", "captures", "downloads", "components", "prefix", "apt"):
        (path / name).mkdir(mode=0o700)


def reject_overrides(parent, root):
    exact = {"RUSTC", "RUSTDOC", "RUSTFLAGS", "RUSTC_BOOTSTRAP", "RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER",
             "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_RUSTC", "CARGO_BUILD_RUSTFLAGS", "CARGO_BUILD_TARGET",
             "CARGO_TARGET_DIR", "CARGO_HOME", "RUSTUP_HOME", "LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT",
             "LIBRARY_PATH", "COMPILER_PATH", "GCC_EXEC_PREFIX", "CC", "CXX", "AR", "RANLIB", "CFLAGS", "CXXFLAGS",
             "CPPFLAGS", "LDFLAGS", "HOST_CFLAGS", "TARGET_CFLAGS", "ASAN_OPTIONS", "LSAN_OPTIONS", "ASAN_SYMBOLIZER_PATH",
             "RUSTUP_DIST_SERVER", "RUSTUP_UPDATE_ROOT", "APT_CONFIG"}
    prefixes = ("CARGO_", "RUST_", "RUSTC_", "RUSTDOC_", "RUSTUP_", "SQLITE_", "SQLITE3_", "LIBSQLITE3_",
                "CC_", "CXX_", "CFLAGS_", "AR_", "HOST_", "TARGET_", "LD_", "PKG_CONFIG", "BINDGEN_", "CRATE_CC_")
    require(not any(key in exact or key.startswith(prefixes) for key in parent), "configuration")
    for directory in (Path(root), *Path(root).parents):
        require(not any((directory / ".cargo" / name).exists() or (directory / ".cargo" / name).is_symlink()
                        for name in ("config", "config.toml")), "configuration")
    home = parent.get("HOME")
    if home:
        require(not any((Path(home) / ".cargo" / name).exists() for name in ("config", "config.toml")), "configuration")


def private_environment(run):
    return {"PATH": "/usr/bin:/bin", "HOME": str(run / "home"), "CARGO_HOME": str(run / "cargo"),
            "RUSTUP_HOME": str(run / "rustup"), "TMPDIR": str(run / "tmp"), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
            "DEBUGINFOD_URLS": "", "RUSTFLAGS": "-Zsanitizer=address", "CARGO_BUILD_JOBS": "2",
            "CARGO_INCREMENTAL": "0", "PYTHONDONTWRITEBYTECODE": "1", "CARGO_TERM_COLOR": "never"}


def cgroup_limits(membership, root=Path("/sys/fs/cgroup")):
    match = re.fullmatch(r"0::(/[^\n]*)\n?", membership)
    require(match and ".." not in PurePosixPath(match[1]).parts, "admission")
    current = root / match[1].lstrip("/")
    require(current.is_dir() and current.resolve().is_relative_to(root.resolve()), "admission")
    cpu, memory, pids = [], [], []
    for directory in (current, *current.parents):
        if not directory.is_relative_to(root):
            break
        for name, values in (("memory.max", memory), ("pids.max", pids)):
            path = directory / name
            if path.exists():
                value = path.read_text().strip()
                require(value == "max" or re.fullmatch(r"[1-9][0-9]*", value), "admission")
                if value != "max":
                    values.append(int(value))
        path = directory / "cpu.max"
        if path.exists():
            fields = path.read_text().split()
            require(len(fields) == 2 and fields[1].isdigit() and int(fields[1]) > 0 and
                    (fields[0] == "max" or fields[0].isdigit()), "admission")
            if fields[0] != "max":
                cpu.append(int(fields[0]) / int(fields[1]))
    require(cpu and memory and pids and 0 < min(cpu) <= 2 and min(memory) <= 6 * GIB and min(pids) <= 512, "admission")
    return {"cpu_quota_cores": min(cpu), "memory_max_bytes": min(memory), "pids_max": min(pids),
            "ancestor_limits_checked": True}


class Supervisor:
    def __init__(self, run, environment, *, deadline_seconds=2400, capture_limit=64 * 1024 ** 2,
                 total_capture_limit=512 * 1024 ** 2, monitor_scratch=True):
        self.run_dir, self.environment = Path(run), environment
        self.deadline = time.monotonic() + deadline_seconds
        self.cleanup_reserve = min(15, max(0, deadline_seconds / 20))
        self.capture_limit, self.total_capture_limit = capture_limit, total_capture_limit
        self.captured = 0
        self.monitor_scratch = monitor_scratch
        self.owned_children_reaped = True
        self.poisoned = False
        self.sequence = 0
        self.next_disk_check = 0
        (self.run_dir / "captures").mkdir(mode=0o700, exist_ok=True)

    def remaining(self):
        remaining = self.deadline - time.monotonic() - self.cleanup_reserve
        require(remaining > 0, "deadline")
        return remaining

    def check_scratch(self):
        if self.monitor_scratch and time.monotonic() >= self.next_disk_check:
            self.next_disk_check = time.monotonic() + 0.25
            size = 0
            for directory, _, files in os.walk(self.run_dir, followlinks=False):
                for name in files:
                    path = Path(directory) / name
                    if not path.is_symlink():
                        size += path.stat().st_size
            require(size <= 12 * GIB and shutil.disk_usage(self.run_dir).free >= 2 * GIB, "scratch-limit")

    def reap(self, process):
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=max(0.001, min(2, self.deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                pass
            deadline = min(self.deadline, time.monotonic() + 2)
            while time.monotonic() < deadline:
                process.poll()
                if not asan.group_exists(process.pid):
                    process.wait()
                    return
                time.sleep(0.01)
        process.poll()
        require(not asan.group_exists(process.pid), "owned-group")
        process.wait()

    def run(self, argv, *, timeout=60, environment=None, cwd=ROOT, allow_failure=False):
        require(not self.poisoned, "owned-group")
        require(isinstance(argv, list) and argv and all(isinstance(value, str) for value in argv))
        end = time.monotonic() + min(timeout, self.remaining())
        self.check_scratch()
        self.sequence += 1
        paths = [self.run_dir / "captures" / f"{self.sequence}-{name}" for name in ("stdout", "stderr")]
        buffers = [bytearray(), bytearray()]
        self.owned_children_reaped = False
        try:
            process = subprocess.Popen(argv, cwd=cwd, env=environment or self.environment,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        except OSError:
            self.owned_children_reaped = True
            raise RuntimeError("child-failed") from None
        try:
            with selectors.DefaultSelector() as selector, \
                    os.fdopen(os.open(paths[0], os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "wb") as stdout, \
                    os.fdopen(os.open(paths[1], os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "wb") as stderr:
                for index, pipe in enumerate((process.stdout, process.stderr)):
                    os.set_blocking(pipe.fileno(), False)
                    selector.register(pipe, selectors.EVENT_READ, index)
                while selector.get_map():
                    require(time.monotonic() < end, "child-timeout")
                    self.remaining()
                    self.check_scratch()
                    for key, _ in selector.select(min(0.05, max(0, end - time.monotonic()))):
                        data = os.read(key.fileobj.fileno(), 65536)
                        if not data:
                            selector.unregister(key.fileobj)
                            continue
                        index = key.data
                        self.captured += len(data)
                        require(sum(map(len, buffers)) + len(data) <= self.capture_limit and
                                self.captured <= self.total_capture_limit, "capture-limit")
                        (stdout, stderr)[index].write(data)
                        buffers[index].extend(data)
                try:
                    process.wait(timeout=max(0.001, end - time.monotonic()))
                except subprocess.TimeoutExpired:
                    raise RuntimeError("child-timeout") from None
            require(not asan.group_exists(process.pid), "owned-group")
            self.owned_children_reaped = True
        except BaseException:
            try:
                self.reap(process)
            except BaseException:
                self.poisoned = True
                raise
            self.owned_children_reaped = True
            raise
        finally:
            process.stdout.close()
            process.stderr.close()
        result = subprocess.CompletedProcess(argv, process.returncode, *(data.decode("utf-8", errors="replace") for data in buffers))
        require(allow_failure or result.returncode == 0, "child-failed")
        return result


def download(supervisor, url, destination, checksum=None, *, metadata=False):
    require(url.startswith(("https://static.rust-lang.org/dist/2026-11-09/", "https://archive.ubuntu.com/ubuntu/",
                            "https://ports.ubuntu.com/ubuntu-ports/")))
    require(not destination.exists())
    supervisor.run(["/usr/bin/curl", "--disable", "--fail", "--silent", "--show-error", "--location", "--max-redirs", "3",
                    "--proto", "=https", "--proto-redir", "=https", "--connect-timeout", "30", "--max-time",
                    "60" if metadata else "300", "--output", str(destination), url], timeout=60 if metadata else 300)
    size = destination.stat().st_size
    require(0 < size <= (2 * 1024 ** 2 if metadata else 1024 ** 3))
    value = sha256(destination)
    require(checksum is None or value == checksum, "toolchain")
    return {"sha256": value, "bytes": size}


def validate_component_manifest(manifest, pin, target):
    require(target in asan.TARGETS, "toolchain")
    try:
        for name in COMPONENTS:
            architecture = "*" if name == "rust-src" else target
            actual = manifest["pkg"][name]["target"][architecture]
            expected = pin["components"][name][architecture]
            require(actual["available"] is True and actual["xz_url"] == expected["url"] and
                    actual["xz_hash"] == expected["sha256"], "toolchain")
    except (KeyError, TypeError):
        raise RuntimeError("toolchain") from None


def extract_component(archive, destination, supervisor=None):
    require(not destination.exists(), "archive-invalid")
    with tarfile.open(archive, "r:*") as bundle:
        members = bundle.getmembers()
        seen, total = set(), 0
        require(0 < len(members) <= 200000, "archive-invalid")
        for item in members:
            path = PurePosixPath(item.name)
            require(path.parts and not path.is_absolute() and ".." not in path.parts and
                    path.as_posix() == item.name.rstrip("/") and path.as_posix() not in seen and
                    not item.mode & 0o6000 and (item.isfile() or item.isdir() or item.issym()), "archive-invalid")
            seen.add(path.as_posix())
            total += item.size
            require(total <= 4 * GIB, "archive-invalid")
            if item.issym():
                link = PurePosixPath(item.linkname)
                require(not link.is_absolute() and ".." not in link.parts and str(path.parent / link) in {x.name for x in members},
                        "archive-invalid")
        destination.mkdir(mode=0o700)
        for item in [entry for entry in members if not entry.issym()] + [entry for entry in members if entry.issym()]:
            if supervisor:
                supervisor.remaining()
                supervisor.check_scratch()
            target = destination / item.name
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            if item.isdir():
                target.mkdir(mode=0o700, exist_ok=True)
            elif item.issym():
                require((target.parent / item.linkname).is_file(), "archive-invalid")
                target.symlink_to(item.linkname)
            else:
                with bundle.extractfile(item) as source, os.fdopen(os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                                                                          0o700 if item.mode & 0o111 else 0o600), "wb") as output:
                    while chunk := source.read(1024 * 1024):
                        if supervisor:
                            supervisor.remaining()
                            supervisor.check_scratch()
                        output.write(chunk)
    return destination


def prepare_toolchain(supervisor, pin, target, report=None, notices=None):
    report = {} if report is None else report
    notices = [] if notices is None else notices
    run = supervisor.run_dir
    manifest = run / "downloads/channel-rust-nightly.toml"
    manifest_identity = download(supervisor, pin["manifest_url"], manifest, pin["manifest_sha256"], metadata=True)
    validate_component_manifest(tomllib.loads(manifest.read_text()), pin, target)
    report["manifest"] = manifest_identity
    components = []
    for name in COMPONENTS:
        entry = pin["components"][name]["*" if name == "rust-src" else target]
        archive = run / "downloads" / (name + ".tar.xz")
        identity = download(supervisor, entry["url"], archive, entry["sha256"])
        components.append({"component": name, "archive": archive, "identity": identity})
    # All archives must match the dated manifest before any bundled installer runs.
    report["components"] = [{"component": item["component"], **item["identity"]} for item in components]
    installers, scripts, notice_bytes = [], [], 0
    for entry in components:
        directory = extract_component(entry["archive"], run / "components" / entry["component"], supervisor)
        roots = list(directory.iterdir())
        require(len(roots) == 1 and roots[0].is_dir(), "toolchain")
        install = roots[0] / "install.sh"
        require(install.is_file() and not install.is_symlink(), "toolchain")
        installers.append(install)
        notice_start = len(notices)
        for path in sorted(roots[0].rglob("*")):
            supervisor.remaining()
            supervisor.check_scratch()
            if path.is_file() and not path.is_symlink():
                relative = path.relative_to(roots[0]).as_posix()
                require(re.fullmatch(r"[A-Za-z0-9_./+@-]+", relative), "toolchain")
                if path.suffix == ".sh" or path.name in ("components", "manifest.in", "rust-installer-version"):
                    scripts.append({"component": entry["component"], "source": relative, "sha256": sha256(path)})
                if re.search(r"(?:^|/)(?:LICENSE|COPYRIGHT|COPYING|NOTICE)(?:[.-]|$)", relative, re.IGNORECASE):
                    size = path.stat().st_size
                    require(size <= TOOLCHAIN_NOTICE_FILE_LIMIT and notice_bytes + size <= TOOLCHAIN_NOTICE_TOTAL_LIMIT, "toolchain")
                    notice_bytes += size
                    with path.open("rb") as original:
                        data = original.read(TOOLCHAIN_NOTICE_FILE_LIMIT + 1)
                    require(len(data) == size, "toolchain")
                    notices.append({"component": entry["component"], "source": relative,
                                    "sha256": hashlib.sha256(data).hexdigest(), "text": data.decode("utf-8")})
        require(len(notices) > notice_start, "toolchain")
    require(scripts and notices, "toolchain")
    report.update(installation="verified-official-component-install.sh", installer_sources=scripts)
    prefix = run / "prefix"
    require(not any(prefix.iterdir()), "toolchain")
    for install in installers:
        directories = {"prefix": prefix, "sysconfdir": prefix / "etc", "bindir": prefix / "bin", "libdir": prefix / "lib",
                       "datadir": prefix / "share", "mandir": prefix / "share/man"}
        supervisor.run(["/bin/bash", str(install), *["--" + key + "=" + str(value) for key, value in directories.items()],
                        "--disable-ldconfig"], timeout=300)
    rustc, cargo = prefix / "bin/rustc", prefix / "bin/cargo"
    binaries = {"rustc_sha256": asan.check_binary(rustc, prefix, target), "cargo_sha256": asan.check_binary(cargo, prefix, target)}
    supervisor.environment.update(PATH=str(prefix / "bin") + ":/usr/bin:/bin", RUSTC=str(rustc), RUSTDOC=str(prefix / "bin/rustdoc"))
    version = supervisor.run([str(rustc), "--version", "--verbose"]).stdout.splitlines()
    require(version and version[0] == "rustc " + pin["rustc_version"] and
            "commit-hash: " + pin["compiler_commit"] in version and "host: " + target in version and
            "LLVM version: 23.1.5" in version, "toolchain")
    cargo_version = supervisor.run([str(cargo), "--version"]).stdout.strip()
    require(cargo_version == "cargo 1." + pin["cargo_distribution_version"].removeprefix("0."), "toolchain")
    std_lock = prefix / "lib/rustlib/src/rust/library/Cargo.lock"
    require(sha256(std_lock) == STD_LOCK, "toolchain")
    tools = prefix / "lib/rustlib" / target / "bin"
    require(tools.is_dir(), "toolchain")
    tool_inventory = []
    for path in sorted(tools.iterdir()):
        require(path.is_file() and not path.is_symlink() and re.fullmatch(r"[a-z0-9._-]+", path.name), "toolchain")
        tool_inventory.append({"name": path.name, "sha256": sha256(path)})
    report.update({
            "rustc_version": pin["rustc_version"], "compiler_commit": pin["compiler_commit"], "llvm_version": "23.1.5",
            "cargo_version": cargo_version, "stdlib_lock_sha256": STD_LOCK, "llvm_tools": tool_inventory, **binaries})
    return report


def graph_shape(graph):
    require(type(graph) is dict and set(graph) == {"version", "units", "roots"} and type(graph["version"]) is int and
            graph["version"] == 1 and type(graph["units"]) is list and type(graph["roots"]) is list, "graphs")
    allowed = {"pkg_id", "target", "profile", "platform", "mode", "features", "dependencies", "is_std", "unit_for"}
    nested = {"target": {"kind", "crate_types", "name", "src_path", "edition", "doc", "doctest", "test", "required-features"},
              "profile": {"name", "opt_level", "lto", "codegen_backend", "codegen_units", "debuginfo", "split_debuginfo",
                          "debug_assertions", "overflow_checks", "rpath", "incremental", "panic", "strip"},
              "dependencies": {"index", "extern_crate_name", "public", "noprelude"}}
    types = {dict: "object", list: "array", str: "string", int: "integer", bool: "boolean", type(None): "null"}
    fields, children = {}, {}
    package_syntax = {"registry_prefix": False, "path_prefix": False, "legacy_registry_suffix": False,
                      "fragment_separator": False, "version_separator": False, "other_prefix": False}
    for unit in graph["units"]:
        require(type(unit) is dict and set(unit) <= allowed, "graphs")
        package_id = unit.get("pkg_id")
        if type(package_id) is str:
            package_syntax["registry_prefix"] |= package_id.startswith("registry+")
            package_syntax["path_prefix"] |= package_id.startswith("path+")
            package_syntax["legacy_registry_suffix"] |= " (registry+" in package_id
            package_syntax["fragment_separator"] |= "#" in package_id
            package_syntax["version_separator"] |= "@" in package_id
            package_syntax["other_prefix"] |= not package_id.startswith(("registry+", "path+"))
        for key, value in unit.items():
            require(type(value) in types, "graphs")
            fields.setdefault(key, set()).add(types[type(value)])
            if key in nested:
                values = value if key == "dependencies" else [value]
                require(type(values) is list, "graphs")
                for child in values:
                    require(type(child) is dict and set(child) <= nested[key], "graphs")
                    for field, entry in child.items():
                        require(type(entry) in types, "graphs")
                        children.setdefault(key, {}).setdefault(field, set()).add(types[type(entry)])
    require(all(type(index) is int and 0 <= index < len(graph["units"]) for index in graph["roots"]), "graphs")
    return {"version": 1, "units": len(graph["units"]), "roots": len(graph["roots"]),
            "package_id_syntax": package_syntax,
            "unit_fields": {key: sorted(value) for key, value in sorted(fields.items())},
            "nested_fields": {key: {field: sorted(value) for field, value in sorted(fields.items())} for key, fields in sorted(children.items())}}


def collect_graphs(supervisor, cargo, target):
    graphs = {}
    for name, command in (("cli", ["build", "--bin", "rpgmigrate"]), ("libtest", ["test", "--no-run", "--lib"])):
        try:
            output = supervisor.run([str(cargo), *command, "--locked", "-Zbuild-std", "--target", target,
                                     "-Zunstable-options", "--unit-graph"], timeout=300).stdout
        except RuntimeError as error:
            graphs[name] = {"status": "failed", "reason": str(error) if str(error) in CODES else "graphs"}
            continue
        try:
            graphs[name] = graph_shape(json.loads(output))
        except (ValueError, TypeError, RuntimeError):
            graphs[name] = {"status": "shape-unrecognized"}
    return {"graphs": graphs, "selected_closure": "not-extracted"}


def require_selected_closure(shape):
    require(type(shape) is dict and type(shape.get("graphs")) is dict and set(shape["graphs"]) == {"cli", "libtest"}, "graphs")
    require(not any(value.get("status") == "failed" for value in shape["graphs"].values()), "graphs")
    # The extractor is deliberately gated on the first real pinned Cargo output.
    raise RuntimeError("inventory-incomplete")


def validate_package_record(record, package, version, architecture):
    require(type(record) is dict and record.get("Package") == package and record.get("Version") == version and
            record.get("Architecture") == architecture and PACKAGE.fullmatch(package) and VERSION.fullmatch(version), "package-authentication")
    filename = record.get("Filename", "")
    require(re.fullmatch(r"pool/[A-Za-z0-9_./+~%-]+\.deb", filename) and ".." not in PurePosixPath(filename).parts and
            HEX.fullmatch(record.get("SHA256", "")) and re.fullmatch(r"[1-9][0-9]*", record.get("Size", "")), "package-authentication")
    if package == "llvm-18":
        require(version == "1:18.1.3-1ubuntu1" and record["SHA256"] == DEB_PINS.get(architecture), "package-authentication")


def deb_file_payload(record, package, version, architecture, authenticated):
    require(authenticated, "package-authentication")
    validate_package_record(record, package, version, architecture)
    return record["SHA256"]


PUBLIC_ROLES = {"archive-keyring": "gpg", "signed-release": "inrelease", "package-index": "packages.xz", "package-archive": "deb"}
PUBLIC_LIMIT = 512 * 1024 ** 2


def validate_public_entry(entry, architecture):
    require(type(entry) is dict and set(entry) == {"role", "filename", "sha256", "bytes", "public_origin"}, "retention")
    role = entry["role"]
    require(role in PUBLIC_ROLES and re.fullmatch(re.escape(role) + r"-[0-9]{4}\." + re.escape(PUBLIC_ROLES[role]), entry["filename"])
            and HEX.fullmatch(entry["sha256"]) and type(entry["bytes"]) is int and 0 < entry["bytes"] <= PUBLIC_LIMIT, "retention")
    base = "https://archive.ubuntu.com/ubuntu/" if architecture == "amd64" else "https://ports.ubuntu.com/ubuntu-ports/"
    origin = entry["public_origin"]
    if role == "archive-keyring":
        require(origin == "ubuntu-installed-archive-keyring", "retention")
    elif role == "signed-release":
        require(origin in {base + "dists/" + suite + "/InRelease" for suite in ("noble", "noble-updates", "noble-security")}, "retention")
    elif role == "package-index":
        require(origin in {base + "dists/" + suite + "/" + component + "/binary-" + architecture + "/Packages.xz"
                           for suite in ("noble", "noble-updates", "noble-security") for component in ("main", "universe")}, "retention")
    else:
        require(origin.startswith(base) and re.fullmatch(r"pool/[A-Za-z0-9_./+~%-]+\.deb", origin.removeprefix(base)) and
                ".." not in PurePosixPath(origin.removeprefix(base)).parts, "retention")


class PublicProvenance:
    def __init__(self, supervisor, architecture):
        self.supervisor, self.architecture = supervisor, architecture
        self.directory = supervisor.run_dir / "public-inputs"
        self.directory.mkdir(mode=0o700)
        self.entries = []
        self.total = 0

    def add(self, role, source, origin, expected_sha256):
        source = Path(source)
        require(role in PUBLIC_ROLES and source.is_file() and not source.is_symlink(), "retention")
        allowed = {"archive-keyring": Path("/usr/share/keyrings/ubuntu-archive-keyring.gpg"),
                   "signed-release": self.supervisor.run_dir / "apt/state/lists",
                   "package-index": self.supervisor.run_dir / "apt/public-indexes",
                   "package-archive": self.supervisor.run_dir / "apt/verified-packages"}[role]
        require(source == allowed if role == "archive-keyring" else source.parent.resolve() == allowed.resolve(), "retention")
        size = source.stat().st_size
        require(HEX.fullmatch(expected_sha256) and sha256(source) == expected_sha256 and self.total + size <= PUBLIC_LIMIT - 1024 ** 2,
                "retention")
        require(len(self.entries) < 512, "retention")
        name = role + f"-{len(self.entries):04d}." + PUBLIC_ROLES[role]
        entry = {"role": role, "filename": name, "sha256": expected_sha256, "bytes": size, "public_origin": origin}
        validate_public_entry(entry, self.architecture)
        destination = self.directory / name
        with source.open("rb") as original, os.fdopen(os.open(destination, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "wb") as output:
            while data := original.read(1024 * 1024):
                self.supervisor.remaining()
                self.supervisor.check_scratch()
                output.write(data)
        require(sha256(destination) == expected_sha256, "retention")
        self.entries.append(entry)
        self.total += size
        return name

    def publish(self, directory):
        require(self.entries, "retention")
        archive = directory / "public-provenance.tar"
        with os.fdopen(os.open(archive, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), "wb") as output:
            with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as bundle:
                for entry in self.entries:
                    self.supervisor.remaining()
                    self.supervisor.check_scratch()
                    info = tarfile.TarInfo(entry["filename"])
                    info.mode, info.size, info.mtime = 0o600, entry["bytes"], 0
                    with (self.directory / entry["filename"]).open("rb") as original:
                        bundle.addfile(info, original)
        require(archive.stat().st_size <= PUBLIC_LIMIT, "retention")
        manifest = {"schema_version": 1, "kind": "rpgmigrate-public-package-provenance", "architecture": self.architecture,
                    "files": self.entries, "bundle_sha256": sha256(archive)}
        validate_public_bundle(manifest, archive)
        write_json(directory / "public-provenance.json", manifest)


def validate_public_bundle(manifest, archive):
    require(type(manifest) is dict and set(manifest) == {"schema_version", "kind", "architecture", "files", "bundle_sha256"} and
            type(manifest["schema_version"]) is int and manifest["schema_version"] == 1 and
            manifest["kind"] == "rpgmigrate-public-package-provenance" and manifest["architecture"] in ("amd64", "arm64") and
            type(manifest["files"]) is list and 0 < len(manifest["files"]) <= 512, "retention")
    require(Path(archive).is_file() and not Path(archive).is_symlink() and Path(archive).stat().st_size <= PUBLIC_LIMIT and
            HEX.fullmatch(manifest["bundle_sha256"]) and sha256(archive) == manifest["bundle_sha256"], "retention")
    expected = {}
    for entry in manifest["files"]:
        validate_public_entry(entry, manifest["architecture"])
        require(entry["filename"] not in expected, "retention")
        expected[entry["filename"]] = entry
    require(sum(entry["bytes"] for entry in expected.values()) <= PUBLIC_LIMIT, "retention")
    with tarfile.open(archive, "r:") as bundle:
        members = bundle.getmembers()
        require(len(members) == len(expected) and len({item.name for item in members}) == len(expected), "retention")
        offset = 0
        for item in members:
            require(item.name in expected and item.isreg() and item.mode == 0o600 and item.uid == item.gid == item.mtime == 0 and
                    item.uname == item.gname == item.linkname == "" and not item.pax_headers and item.offset == offset and
                    item.size == expected[item.name]["bytes"], "retention")
            canonical = tarfile.TarInfo(item.name)
            canonical.mode, canonical.size, canonical.mtime = 0o600, item.size, 0
            with Path(archive).open("rb") as raw:
                raw.seek(item.offset)
                require(raw.read(512) == canonical.tobuf(format=tarfile.USTAR_FORMAT), "retention")
                raw.seek(item.offset_data + item.size)
                padding = (-item.size) % 512
                require(raw.read(padding) == bytes(padding), "retention")
            offset = item.offset_data + item.size + padding
            value = hashlib.sha256()
            with bundle.extractfile(item) as original:
                while data := original.read(1024 * 1024):
                    value.update(data)
            require(value.hexdigest() == expected[item.name]["sha256"], "retention")
        size = ((offset + 1024 + 10239) // 10240) * 10240
        require(Path(archive).stat().st_size == size, "retention")
        with Path(archive).open("rb") as raw:
            raw.seek(offset)
            require(raw.read() == bytes(size - offset), "retention")


def release_checksums(path):
    checksums, active = {}, False
    for line in path.read_text().splitlines():
        if line == "SHA256:":
            require(not active and not checksums, "package-authentication")
            active = True
        elif active and line.startswith(" "):
            match = re.fullmatch(r" ([0-9a-f]{64}) +([1-9][0-9]*) (\S+)", line)
            require(match and match[3] not in checksums, "package-authentication")
            checksums[match[3]] = (match[1], int(match[2]))
        elif active:
            active = False
    require(checksums, "package-authentication")
    return checksums


def package_index_records(path, expected_sha256, expected_size, supervisor):
    fields, records, digest, size = {}, [], hashlib.sha256(), 0
    wanted = {"Package", "Version", "Architecture", "Filename", "SHA256", "Size"}
    with lzma.open(path, "rb") as original:
        for number, line in enumerate(original):
            digest.update(line)
            size += len(line)
            require(size <= expected_size <= 1024 ** 3, "package-authentication")
            if number % 1024 == 0:
                supervisor.remaining()
            if line == b"\n":
                require(set(fields) == wanted, "package-authentication")
                records.append(fields)
                fields = {}
            elif line and not line[:1].isspace() and b": " in line:
                key, value = line.rstrip(b"\n").split(b": ", 1)
                key = key.decode("ascii")
                if key in wanted:
                    require(key not in fields, "package-authentication")
                    fields[key] = value.decode("ascii")
    if fields:
        require(set(fields) == wanted, "package-authentication")
        records.append(fields)
    require(records and size == expected_size and digest.hexdigest() == expected_sha256, "package-authentication")
    return records


class UbuntuPackages:
    def __init__(self, supervisor, architecture):
        self.supervisor, self.architecture = supervisor, architecture
        self.root = supervisor.run_dir / "apt"
        self.environment = dict(supervisor.environment)
        self.base = "https://archive.ubuntu.com/ubuntu/" if architecture == "amd64" else "https://ports.ubuntu.com/ubuntu-ports/"
        self.packages, self.notices, self.records = {}, [], {}
        self.public = PublicProvenance(supervisor, architecture)
        supervisor.public_provenance = self.public
        for relative in ("state/lists/partial", "cache/archives/partial", "payloads", "public-indexes", "verified-packages"):
            (self.root / relative).mkdir(mode=0o700, parents=True)
        keyring = Path("/usr/share/keyrings/ubuntu-archive-keyring.gpg")
        require(keyring.is_file() and not keyring.is_symlink(), "package-authentication")
        self.keyring_sha256 = sha256(keyring)
        sources = self.root / "sources.list"
        sources.write_text("".join(f"deb [arch={architecture} signed-by={keyring}] {self.base} {suite} main universe\n"
                                   for suite in ("noble", "noble-updates", "noble-security")))
        configuration = self.root / "apt.conf"
        configuration.write_text(f'''Dir::Etc::main "-";
Dir::Etc::parts "-";
Dir::Etc::sourcelist "{sources}";
Dir::Etc::sourceparts "-";
Dir::State "{self.root / 'state'}";
Dir::State::status "/var/lib/dpkg/status";
Dir::Cache "{self.root / 'cache'}";
Dir::Log "{self.root}";
APT::Architecture "{architecture}";
APT::Architectures {{ "{architecture}"; }};
APT::Sandbox::User "";
Acquire::Languages "none";
Acquire::AllowInsecureRepositories "false";
Acquire::AllowDowngradeToInsecureRepositories "false";
Acquire::Check-Valid-Until "true";
APT::Get::AllowUnauthenticated "false";
Acquire::https::Timeout "60";
Acquire::Retries "0";
APT::Update::Error-Mode "any";
''')
        self.environment["APT_CONFIG"] = str(configuration)
        supervisor.run(["/usr/bin/apt-get", "update"], environment=self.environment, timeout=300)
        indexes = []
        for path in sorted((self.root / "state/lists").iterdir()):
            if path.is_file() and path.name != "lock":
                require(re.fullmatch(r"[A-Za-z0-9_.%-]+", path.name), "package-authentication")
                indexes.append({"index": path.name, "sha256": sha256(path)})
        require(any(item["index"].endswith("_InRelease") for item in indexes) and
                any("_Packages" in item["index"] for item in indexes), "package-authentication")
        self.indexes = indexes
        self.public.add("archive-keyring", keyring, "ubuntu-installed-archive-keyring", self.keyring_sha256)
        for suite in ("noble", "noble-updates", "noble-security"):
            releases = [path for path in (self.root / "state/lists").iterdir() if path.name.endswith("_dists_" + suite + "_InRelease")]
            require(len(releases) == 1 and not releases[0].is_symlink(), "package-authentication")
            supervisor.run(["/usr/bin/gpgv", "--keyring", str(keyring), str(releases[0])])
            self.public.add("signed-release", releases[0], self.base + "dists/" + suite + "/InRelease", sha256(releases[0]))
            checksums = release_checksums(releases[0])
            for component in ("main", "universe"):
                meta = component + "/binary-" + architecture + "/Packages"
                require(meta in checksums and meta + ".xz" in checksums, "package-authentication")
                index = self.root / "public-indexes" / (suite + "-" + component + ".packages.xz")
                index_hash, index_size = checksums[meta + ".xz"]
                require(self.public.total + index_size <= PUBLIC_LIMIT - 1024 ** 2, "retention")
                download(supervisor, self.base + "dists/" + suite + "/" + meta + ".xz", index, index_hash)
                require(index.stat().st_size == index_size, "package-authentication")
                actual = package_index_records(index, *checksums[meta], supervisor)
                self.public.add("package-index", index, self.base + "dists/" + suite + "/" + meta + ".xz", index_hash)
                for record in actual:
                    identity = (record["Package"], record["Version"], record["Architecture"])
                    require(identity not in self.records or self.records[identity] == record, "package-authentication")
                    self.records[identity] = record

    def installed(self, path):
        result = self.supervisor.run(["/usr/bin/dpkg-query", "-S", str(path)]).stdout.strip()
        match = re.fullmatch(r"([a-z0-9][a-z0-9+.-]*(?::(?:amd64|arm64))?): " + re.escape(str(path)), result)
        require(match, "package-authentication")
        value = self.supervisor.run(["/usr/bin/dpkg-query", "-W", "-f=${Package}\t${Version}\t${Architecture}\t${Status}", match[1]]).stdout.split("\t")
        require(len(value) == 4 and PACKAGE.fullmatch(value[0]) and VERSION.fullmatch(value[1]) and
                value[2] in (self.architecture, "all") and value[3] == "install ok installed", "package-authentication")
        return value[:3]

    def archive(self, package, version, architecture):
        identity = (package, version, architecture)
        if identity in self.packages:
            return self.packages[identity]
        require(identity in self.records, "package-authentication")
        record = self.records[identity]
        validate_package_record(record, package, version, architecture)
        require(self.public.total + int(record["Size"]) <= PUBLIC_LIMIT - 1024 ** 2, "retention")
        checksum = deb_file_payload(record, package, version, architecture, True)
        archive = self.root / "verified-packages" / (str(len(self.packages)) + ".deb")
        archive_identity = download(self.supervisor, self.base + record["Filename"], archive, checksum)
        require(archive_identity["bytes"] == int(record["Size"]), "package-authentication")
        self.public.add("package-archive", archive, self.base + record["Filename"], checksum)
        payload = self.root / "payloads" / str(len(self.packages))
        payload.mkdir(mode=0o700)
        self.supervisor.run(["/usr/bin/dpkg-deb", "--extract", str(archive), str(payload)], timeout=180)
        self.packages[identity] = {"payload": payload, "record": record, "archive": archive_identity}
        return self.packages[identity]

    def compare(self, path, *, native_target=None):
        path = Path(path).resolve(strict=True)
        require(path.is_file() and not path.is_symlink() and path.is_relative_to("/usr"), "package-payload")
        package, version, architecture = self.installed(path)
        entry = self.archive(package, version, architecture)
        payload = entry["payload"] / path.relative_to("/")
        require(payload.is_file() and not payload.is_symlink() and payload.resolve().is_relative_to(entry["payload"].resolve()) and
                sha256(payload) == sha256(path), "package-payload")
        if native_target:
            native_elf(path, native_target)
        return {"path": path.as_posix(), "sha256": sha256(path), "package": package, "version": version,
                "architecture": architecture, "authenticated_payload_equal": True}

    def evidence(self):
        return {"archive_keyring_sha256": self.keyring_sha256, "authentication": "isolated-apt-signed-inrelease-packages",
                "indexes": self.indexes, "packages": [{"record": value["record"], "archive": value["archive"]}
                                                       for _, value in sorted(self.packages.items())]}

    def licenses(self):
        identities = list(self.packages)
        require(identities, "package-authentication")
        notices = []
        for package, _, _ in identities:
            copyright_path = Path("/usr/share/doc") / package / "copyright"
            require(copyright_path.exists(), "package-payload")
            real = copyright_path.resolve(strict=True)
            require(real.is_relative_to("/usr/share/doc") and real.stat().st_size <= 2 * 1024 ** 2, "package-payload")
            identity = self.compare(real)
            notices.append({"component": package, "source": "copyright", "sha256": identity["sha256"], "text": real.read_text()})
        return notices


def native_elf(path, target):
    require(target in asan.TARGETS and Path(path).is_file(), "admission")
    with Path(path).open("rb") as handle:
        header = handle.read(20)
    require(len(header) == 20 and header[:6] == b"\x7fELF\x02\x01" and
            int.from_bytes(header[18:20], "little") == asan.TARGETS[target][1], "admission")


def authenticate_symbolizer(supervisor, target):
    architecture = asan.TARGETS[target][2]
    require(SYMBOLIZER.is_file() and not SYMBOLIZER.is_symlink(), "symbolizer")
    native_elf(SYMBOLIZER, target)
    require(asan.package(SYMBOLIZER, supervisor.run, architecture) ==
            {"package": "llvm-18", "package_version": "1:18.1.3-1ubuntu1", "architecture": architecture}, "symbolizer")
    packages = UbuntuPackages(supervisor, architecture)
    owner = packages.installed(SYMBOLIZER)
    require(owner == ["llvm-18", "1:18.1.3-1ubuntu1", architecture], "symbolizer")
    tools = [packages.compare(Path(path), native_target=target if native else None)
             for path, native in (("/sbin/ldconfig", False), ("/sbin/ldconfig.real", True), ("/usr/bin/ldd", False), ("/bin/bash", True))]
    candidates = {}
    cache = supervisor.run(["/sbin/ldconfig.real", "-p"]).stdout
    for name, path in re.findall(r"^\s*(\S+) \([^\n]*\) => (/[^\s]+)$", cache, re.MULTILINE):
        candidate = Path(path).resolve()
        try:
            native_elf(candidate, target)
        except (OSError, RuntimeError):
            continue
        candidates.setdefault(name, set()).add(candidate)
    pending, seen, bindings = [SYMBOLIZER.resolve()], set(), []
    while pending:
        path = pending.pop()
        if path in seen:
            continue
        seen.add(path)
        identity = packages.compare(path, native_target=target)
        bindings.append(identity)
        dynamic = supervisor.run(["/usr/bin/readelf", "--dynamic", str(path)]).stdout
        needed = re.findall(r"\(NEEDED\).*\[([A-Za-z0-9_.+-]+)\]", dynamic)
        runpaths = re.findall(r"\((?:RUNPATH|RPATH)\).*\[([^\]]*)\]", dynamic)
        require(len(runpaths) <= 1, "symbolizer")
        directories = []
        if runpaths:
            for directory in runpaths[0].split(":"):
                require(directory in ("$ORIGIN/../lib", "$ORIGIN", "/usr/lib/llvm-18/lib"), "symbolizer")
                directories.append(Path(directory.replace("$ORIGIN", str(path.parent))).resolve())
        for name in needed:
            options = [directory / name for directory in directories if (directory / name).is_file()]
            options = {value.resolve() for value in options} or candidates.get(name, set())
            require(len(options) == 1, "symbolizer")
            pending.extend(options)
        headers = supervisor.run(["/usr/bin/readelf", "--program-headers", str(path)]).stdout
        interpreters = re.findall(r"Requesting program interpreter: (/[^\]]+)\]", headers)
        require(len(interpreters) <= 1, "symbolizer")
        pending.extend(Path(value).resolve() for value in interpreters)
    require(len(bindings) > 1, "symbolizer")
    listed = supervisor.run(["/bin/bash", "/usr/bin/ldd", str(SYMBOLIZER)]).stdout
    require("not found" not in listed, "symbolizer")
    loaded = {Path(path).resolve().as_posix() for path in re.findall(r"(?:=>\s+|^\s*)(/[^\s]+)\s+\(", listed, re.MULTILINE)}
    expected = {entry["path"] for entry in bindings if entry["path"] != SYMBOLIZER.as_posix()}
    require(loaded == expected, "symbolizer")
    version = supervisor.run([str(SYMBOLIZER), "--version"]).stdout
    require(re.search(r"^(?:Ubuntu )?LLVM version 18\.1\.3(?:\s|$)", version, re.MULTILINE), "symbolizer")
    notices = packages.licenses()
    return {"candidate": "ubuntu-noble-llvm-18", "path": SYMBOLIZER.as_posix(), "version": "LLVM version 18.1.3",
            "files": bindings, "tools": tools, "provenance": packages.evidence()}, notices


def validate_controls(positive, negative, source):
    require(positive.returncode == negative.returncode == -signal.SIGABRT, "control")
    evidence = asan.validate_control(positive, source)
    text = negative.stdout + "\n" + negative.stderr
    require(type(negative.returncode) is int and
            re.search(r"^(?:==[0-9]+==)?ERROR: AddressSanitizer: heap-use-after-free\b", text, re.MULTILINE) and
            re.search(r"^READ of size [1-9][0-9]* at ", text, re.MULTILINE), "control")
    require(len(re.findall(r"^(?:==[0-9]+==)?ERROR:", text, re.MULTILINE)) == 1 and
            not re.search(r"WARNING:|DEADLYSIGNAL|LeakSanitizer|Sanitizer CHECK failed|Shadow memory|failed|unable|cannot", text, re.IGNORECASE), "control")
    try:
        asan.validate_control(negative, source)
    except RuntimeError as error:
        require(str(error) == "sanitizer control lacks its symbolized access source line", "control")
    else:
        raise RuntimeError("control")
    return {"positive": evidence, "negative": {"case": "native-symbolize-disabled", "expected_rejection": "missing-symbolized-access-line"}}


def run_controls(supervisor, target):
    source, binary = supervisor.run_dir / "asan-control.rs", supervisor.run_dir / "asan-control"
    source.write_text(asan.CONTROL)
    compiler = supervisor.run_dir / "prefix/bin/rustc"
    supervisor.run([str(compiler), "--edition=2024", "--target", target, "-Zsanitizer=address", "-Copt-level=0", "-Cdebuginfo=2",
                    str(source), "-o", str(binary)], timeout=180)
    binary_hash = asan.check_binary(binary, supervisor.run_dir, target)
    environment = {**supervisor.environment, "ASAN_SYMBOLIZER_PATH": str(SYMBOLIZER), "ASAN_OPTIONS": asan.ASAN_OPTIONS}
    positive = supervisor.run([str(binary)], environment=environment, timeout=30, allow_failure=True)
    negative = supervisor.run([str(binary)], environment={**environment, "ASAN_OPTIONS": asan.ASAN_OPTIONS + ":symbolize=0"},
                              timeout=30, allow_failure=True)
    return {**validate_controls(positive, negative, source), "source_sha256": sha256(source), "binary_sha256": binary_hash}


def new_attempt(target):
    require(target in asan.TARGETS)
    return {"schema_version": 4, "kind": "rpgmigrate-asan-preparation", "scope": "native-inventory-and-symbolization-control",
            "qualification": "not-assessed", "target": target, "status": "failed",
            "admission_substage": None, "admission_detail": None,
            "stages": {name: {"status": "not-run", "reason": "none"} for name in STAGES},
            "owned_children_reaped": False, "public_pin_unchanged": False, "artifacts": {},
            "first_failure": {"stage": "none", "reason": "none"}}


def validate_attempt(attempt):
    require(type(attempt) is dict and set(attempt) == set(new_attempt(next(iter(asan.TARGETS)))))
    require(type(attempt["schema_version"]) is int and attempt["schema_version"] == 4 and
            attempt["kind"] == "rpgmigrate-asan-preparation" and attempt["scope"] == "native-inventory-and-symbolization-control" and
            attempt["qualification"] == "not-assessed" and attempt["target"] in asan.TARGETS and attempt["status"] == "failed")
    require(type(attempt["owned_children_reaped"]) is bool and type(attempt["public_pin_unchanged"]) is bool)
    substage = attempt["admission_substage"]
    require(substage is None or (type(substage) is str and substage in ADMISSION_SUBSTAGES))
    detail = attempt["admission_detail"]
    require(detail is None or (type(detail) is str and detail in BINFMT_DETAILS + NATIVE_TOOL_DETAILS))
    require(type(attempt["first_failure"]) is dict and set(attempt["first_failure"]) == {"stage", "reason"} and
            attempt["first_failure"]["stage"] in (*STAGES, "none") and attempt["first_failure"]["reason"] in CODES | {"none"})
    require(type(attempt["stages"]) is dict and set(attempt["stages"]) == set(STAGES))
    for name, value in attempt["stages"].items():
        require(type(value) is dict and set(value) == {"status", "reason"} and
                value["status"] in ("passed", "failed", "incomplete", "not-run") and value["reason"] in CODES | {"none"})
        require((value["status"] in ("passed", "not-run")) == (value["reason"] == "none"))
        require(value["status"] != "incomplete" or (name == "graphs" and value["reason"] == "inventory-incomplete"))
    require(attempt["stages"]["admission"]["status"] != "passed" or substage == "complete")
    require(attempt["stages"]["admission"]["status"] != "not-run" or substage is None)
    failures = [name for name in STAGES if attempt["stages"][name]["status"] in ("failed", "incomplete")]
    expected_failure = {"stage": failures[0], "reason": attempt["stages"][failures[0]]["reason"]} if failures else {"stage": "none", "reason": "none"}
    require(attempt["first_failure"] == expected_failure)
    require(detail is None or (substage == ("binfmt" if detail in BINFMT_DETAILS else "native-architecture") and
                               attempt["stages"]["admission"]["status"] == "failed" and
                               attempt["first_failure"] == {"stage": "admission", "reason": "admission"}))
    require(not attempt["public_pin_unchanged"] or attempt["stages"]["bindings"]["status"] == "passed")
    require(type(attempt["artifacts"]) is dict and set(attempt["artifacts"]) <= {"graphs.json", "provenance.json", "third-party-licenses.json", "public-provenance.json", "public-provenance.tar"})
    require(all(type(value) is str and HEX.fullmatch(value) for value in attempt["artifacts"].values()))
    bundled = {"public-provenance.json", "public-provenance.tar"} & set(attempt["artifacts"])
    require(not bundled or (len(bundled) == 2 and attempt["stages"]["retention"]["status"] == "passed"))
    require(attempt["stages"]["retention"]["status"] != "passed" or len(bundled) == 2)


def source_binding(supervisor):
    commit = supervisor.run(["/usr/bin/git", "rev-parse", "HEAD"]).stdout.strip()
    tree = supervisor.run(["/usr/bin/git", "rev-parse", "HEAD^{tree}"]).stdout.strip()
    require(re.fullmatch(r"[0-9a-f]{40}", commit) and re.fullmatch(r"[0-9a-f]{40}", tree), "bindings")
    require(not supervisor.run(["/usr/bin/git", "status", "--porcelain", "--untracked-files=normal"]).stdout, "bindings")
    names = supervisor.run(["/usr/bin/git", "ls-files", "-z"]).stdout.split("\0")
    files = []
    for name in filter(None, names):
        require(re.fullmatch(r"[A-Za-z0-9_./+-]+", name) and ".." not in Path(name).parts and not Path(name).is_absolute(), "bindings")
        path = ROOT / name
        require(path.is_file() and not path.is_symlink(), "bindings")
        files.append({"source": name, "sha256": sha256(path)})
    require(files and len(files) == len({entry["source"] for entry in files}), "bindings")
    return {"commit": commit, "tree": tree, "files": files}


def parse_binfmt_record(record):
    require(type(record) is bytes and len(record) <= BINFMT_RECORD_LIMIT, "binfmt-budget-exceeded")
    require(record.endswith(b"\n"), "binfmt-entry-invalid")
    lines = record.split(b"\n")[:-1]
    require(len(lines) in (5, 6) and lines[0] in (b"enabled", b"disabled") and
            re.fullmatch(rb"interpreter /[^\x00-\x20\x7f]+", lines[1]) and
            re.fullmatch(rb"flags: P?O?C?F?", lines[2]) and lines[3] == b"offset 0" and
            re.fullmatch(rb"magic (?:[0-9a-f]{2}){1,256}", lines[4]), "binfmt-entry-invalid")
    magic = bytes.fromhex(lines[4][6:].decode("ascii"))
    mask = bytes([255]) * len(magic)
    if len(lines) == 6:
        require(re.fullmatch(rb"mask (?:[0-9a-f]{2}){1,256}", lines[5]), "binfmt-entry-invalid")
        mask = bytes.fromhex(lines[5][5:].decode("ascii"))
        require(len(mask) == len(magic), "binfmt-entry-invalid")
    # Every ELF shares these bytes, so later offsets and handler names grant no exception.
    require(any((elf ^ value) & bits for elf, value, bits in zip(b"\x7fELF", magic, mask)), "binfmt-possible-elf-handler")
    return "enabled" if lines[0] == b"enabled" else "disabled"


def read_binfmt_record(path, failure):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as handle:
            require(stat.S_ISREG(os.fstat(handle.fileno()).st_mode), "binfmt-entry-invalid")
            data = handle.read(BINFMT_RECORD_LIMIT + 1)
    except OSError:
        raise RuntimeError(failure) from None
    require(len(data) <= BINFMT_RECORD_LIMIT, "binfmt-budget-exceeded")
    return data


def inspect_binfmt(directory=None):
    directory = BINFMT_ROOT if directory is None else Path(directory)
    try:
        require(directory.is_dir() and not directory.is_symlink(), "binfmt-lookup-failed")
    except OSError:
        raise RuntimeError("binfmt-lookup-failed") from None
    entries = []
    try:
        for path in directory.iterdir():
            entries.append(path)
            require(len(entries) <= BINFMT_HANDLER_LIMIT + 2, "binfmt-budget-exceeded")
    except OSError:
        raise RuntimeError("binfmt-enumeration-failed") from None
    names = {path.name for path in entries}
    require(len(names) == len(entries), "binfmt-enumeration-failed")
    require({"register", "status"} <= names, "binfmt-lookup-failed")
    status = read_binfmt_record(directory / "status", "binfmt-lookup-failed")
    require(status in (b"enabled\n", b"disabled\n"), "binfmt-entry-invalid")
    total, enabled, disabled = len(status), 0, 0
    require(total <= BINFMT_TOTAL_LIMIT, "binfmt-budget-exceeded")
    for path in entries:
        if path.name in ("register", "status"):
            continue
        record = read_binfmt_record(path, "binfmt-entry-read-failed")
        total += len(record)
        require(total <= BINFMT_TOTAL_LIMIT, "binfmt-budget-exceeded")
        state = parse_binfmt_record(record)
        enabled += state == "enabled"
        disabled += state == "disabled"
    return {"inspection": "complete-snapshot", "handlers_examined": enabled + disabled,
            "elf_disjoint_magic_handlers": enabled + disabled, "enabled_handlers": enabled, "disabled_handlers": disabled,
            "registry_state": "enabled" if status == b"enabled\n" else "disabled"}


def check_native_tools(target):
    """Raise a closed "<tool>-<reason>" detail for the first refused tool."""
    for tool, path in NATIVE_ELF_TOOLS.items():
        try:
            native_elf(path, target)
        except OSError:
            raise RuntimeError(tool + "-unreadable") from None
        except RuntimeError:
            raise RuntimeError(tool + "-not-native-elf") from None
    for tool, (path, interpreter) in NATIVE_SCRIPTS.items():
        try:
            script = Path(path)
            require(script.is_file() and not script.is_symlink(), tool + "-not-regular")
            with script.open("rb") as handle:
                data = handle.read(BINFMT_RECORD_LIMIT + 1)
        except OSError:
            raise RuntimeError(tool + "-unreadable") from None
        require(data.startswith(interpreter), tool + "-interpreter-mismatch")
        if tool == "ldconfig":
            require(len(data) <= BINFMT_RECORD_LIMIT and data.rstrip(b"\n").rsplit(b"\n", 1)[-1] == LDCONFIG_EXEC and
                    data.count(b"exec ") == 1, "ldconfig-wrapper-target-mismatch")


def admit(supervisor, target, parent, pin, attempt):
    attempt["admission_detail"] = None
    attempt["admission_substage"] = "uid"
    require(os.getuid() != 0, "admission")
    attempt["admission_substage"] = "native-architecture"
    asan.native_target(target)
    try:
        check_native_tools(target)
    except RuntimeError as error:
        attempt["admission_detail"] = str(error) if str(error) in NATIVE_TOOL_DETAILS else None
        raise RuntimeError("admission") from None
    attempt["admission_substage"] = "binfmt"
    try:
        binfmt = inspect_binfmt()
    except RuntimeError as error:
        attempt["admission_detail"] = str(error) if str(error) in BINFMT_DETAILS else "binfmt-entry-invalid"
        raise RuntimeError("admission") from None
    attempt["admission_substage"] = "dpkg-architecture"
    architecture = supervisor.run(["/usr/bin/dpkg", "--print-architecture"]).stdout.strip()
    require(architecture == asan.TARGETS[target][2], "admission")
    attempt["admission_substage"] = "os-release"
    release = Path("/etc/os-release").read_text()
    require(re.search(r"^ID=ubuntu$", release, re.MULTILINE) and re.search(r'^VERSION_ID="24\.04"$', release, re.MULTILINE), "admission")
    attempt["admission_substage"] = "project-lock"
    require(sha256(ROOT / "Cargo.lock") == pin["compatibility"]["lockfile_sha256"], "admission")
    attempt["admission_substage"] = "public-pin"
    require(pin["external_symbolizer"]["status"] == "qualification-required", "admission")
    attempt["admission_substage"] = "runner-image"
    image = {key: parent.get(key, "") for key in ("ImageOS", "ImageVersion")}
    require(image["ImageOS"] == "ubuntu24" and re.fullmatch(r"[0-9]{8}\.[0-9]+\.[0-9]+", image["ImageVersion"]), "admission")
    attempt["admission_substage"] = "scratch-disk"
    free = shutil.disk_usage(supervisor.run_dir).free
    require(free >= 12 * GIB, "admission")
    attempt["admission_substage"] = "available-memory"
    memory = re.search(r"^MemAvailable: +([0-9]+) kB$", Path("/proc/meminfo").read_text(), re.MULTILINE)
    require(memory and int(memory[1]) * 1024 >= 6 * GIB, "admission")
    attempt["admission_substage"] = "core-limit"
    require(resource.getrlimit(resource.RLIMIT_CORE)[0] == 0, "admission")
    attempt["admission_substage"] = "complete"
    return {"machine": asan.TARGETS[target][0], "dpkg_architecture": architecture,
            "runner_image": image, "binfmt": binfmt, "core_limit_bytes": 0,
            "host_available_memory_bytes": int(memory[1]) * 1024,
            "scratch": {"free_bytes_at_admission": free, "minimum_free_bytes": 12 * GIB,
                        "growth_stop_bytes": 12 * GIB, "free_floor_bytes": 2 * GIB,
                        "enforcement": "bounded-monitor-not-hard-quota", "monitor_interval_seconds": 0.25}}


def execute(args):
    parent = dict(os.environ)
    require("RUNNER_TEMP" in parent)
    runner_temp = Path(parent["RUNNER_TEMP"]).resolve()
    receipt = args.receipt.absolute()
    require(receipt.name == "attempt.json" and re.fullmatch(r"[A-Za-z0-9_./+-]+", receipt.as_posix()) and
            receipt.parent.is_dir() and receipt.parent.stat().st_uid == os.getuid() and
            receipt.parent.stat().st_mode & 0o777 == 0o700 and not receipt.exists() and not receipt.is_symlink() and
            receipt.parent.resolve().is_relative_to(runner_temp) and not receipt.parent.resolve().is_relative_to(args.run_dir.resolve()))
    require(not any(receipt.parent.iterdir()))
    os.umask(0o077)
    create_private_run(args.run_dir.absolute(), runner_temp)
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    supervisor = Supervisor(args.run_dir.absolute(), private_environment(args.run_dir.absolute()))
    attempt = new_attempt(args.target)
    provenance = {"schema_version": 1, "kind": "rpgmigrate-asan-preparation-provenance", "qualification": "not-assessed",
                  "budget": {"original_limit_seconds": 2400, "cleanup_reserve_seconds": 15,
                             "started_unix_seconds": int(time.time()), "requested_cpu_quota_cores": 2,
                             "requested_memory_max_bytes": 6 * GIB, "requested_pids_max": 512,
                             "child_capture_max_bytes": 64 * 1024 ** 2, "total_capture_max_bytes": 512 * 1024 ** 2,
                             "child_timeout_seconds": {"metadata": 60, "download": 300, "graph": 300, "compile": 180, "control": 30}}}
    notices, graphs, pin_hash, source = [], None, None, None
    pin_path = Path(__file__).with_name("ffi_asan_toolchain.toml")

    def stage(name, operation):
        try:
            value = operation()
            attempt["stages"][name] = {"status": "passed", "reason": "none"}
            return value
        except (Exception, KeyboardInterrupt) as error:
            code = str(error) if isinstance(error, RuntimeError) and str(error) in CODES else name
            attempt["stages"][name] = {"status": "incomplete" if code == "inventory-incomplete" else "failed", "reason": code}
            if attempt["first_failure"]["stage"] == "none":
                attempt["first_failure"] = {"stage": name, "reason": code}
            return None

    def admission():
        nonlocal pin_hash, source
        reject_overrides(parent, ROOT)
        pin = asan.load_pin(pin_path)
        pin_hash = sha256(pin_path)
        provenance["effective_cgroup"] = cgroup_limits(Path("/proc/self/cgroup").read_text())
        provenance["native"] = admit(supervisor, args.target, parent, pin, attempt)
        source = source_binding(supervisor)
        provenance["source"] = source
        provenance["public_pin_sha256"] = pin_hash
        return pin

    pin = stage("admission", admission)
    if pin:
        provenance["toolchain"] = {}
        prepared = stage("toolchain", lambda: prepare_toolchain(supervisor, pin, args.target, provenance["toolchain"], notices))
        if prepared:

            def graph_collection():
                nonlocal graphs
                graphs = collect_graphs(supervisor, supervisor.run_dir / "prefix/bin/cargo", args.target)
                require_selected_closure(graphs)

            stage("graphs", graph_collection)
            symbolizer = stage("symbolizer", lambda: authenticate_symbolizer(supervisor, args.target))
            if symbolizer:
                provenance["symbolizer"], package_notices = symbolizer
                notices.extend(package_notices)
                control = stage("control", lambda: run_controls(supervisor, args.target))
                if control:
                    provenance["control"] = control

    def bindings():
        require(pin_hash and sha256(pin_path) == pin_hash and source and source_binding(supervisor) == source, "bindings")
        if attempt["stages"]["toolchain"]["status"] == "passed":
            for name in ("rustc", "cargo"):
                require(sha256(supervisor.run_dir / "prefix/bin" / name) == provenance["toolchain"][name + "_sha256"], "bindings")
        if "symbolizer" in provenance:
            require(all(sha256(entry["path"]) == entry["sha256"] for entry in provenance["symbolizer"]["files"]), "bindings")
        attempt["public_pin_unchanged"] = True

    if getattr(supervisor, "public_provenance", None):
        stage("retention", lambda: supervisor.public_provenance.publish(receipt.parent))
    if pin:
        stage("bindings", bindings)
    attempt["owned_children_reaped"] = supervisor.owned_children_reaped
    for name, value in (("graphs.json", graphs), ("provenance.json", provenance),
                        ("third-party-licenses.json", {"documents": notices})):
        if value is not None:
            output = receipt.parent / name
            write_json(output, value)
            attempt["artifacts"][name] = sha256(output)
    for name in ("public-provenance.json", "public-provenance.tar"):
        if attempt["stages"]["retention"]["status"] == "passed":
            attempt["artifacts"][name] = sha256(receipt.parent / name)
    require(sum(path.stat().st_size for path in receipt.parent.iterdir() if path.is_file()) <= PUBLIC_LIMIT, "retention")
    validate_attempt(attempt)
    write_json(receipt, attempt)
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=asan.TARGETS, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    try:
        execute(args)
    except (Exception, KeyboardInterrupt):
        print("ffi-asan-prepare: preparation failed before sanitized attempt publication", file=sys.stderr)
        return 1
    print("ffi-asan-prepare: preparation incomplete; sanitized attempt published; qualification not assessed", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
