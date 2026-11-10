#!/usr/bin/env python3
"""Check maintained guides against explicit Task output and safe executable help."""
import argparse
import html
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
from urllib.parse import unquote, urlsplit

GUIDES = (
    "README.md", "docs/getting-started.md", "docs/operator-guide.md",
    "docs/troubleshooting.md", "docs/durability.md", "docs/testing.md",
    "docs/runner-image.md", "docs/operator-compatibility.md",
    "docs/native-protocol.md", "docs/native-cutover-handoff.md",
)
# Help argv stays independent of Markdown, including unsupported command examples.
HELP_PATHS = {()} | {(name,) for name in ("clone", "fork", "follow", "snapshot", "ping", "help", "version")}
for _group, _leaves in {
    "compare": "schema data",
    "copy": "db roles extensions schema data table-data blobs sequences indexes constraints",
    "dump": "schema roles",
    "restore": "schema pre-data post-data roles parse-list",
    "list": "databases extensions collations tables table-parts sequences indexes depends schema progress",
    "stream": "setup cleanup prune prefetch catchup replay receive apply",
}.items():
    HELP_PATHS.add((_group,))
    HELP_PATHS.update((_group, leaf) for leaf in _leaves.split())
HELP_PATHS.update({("stream", "sentinel"), ("stream", "sentinel", "setup"),
                   ("stream", "sentinel", "get"), ("stream", "sentinel", "set")})
HELP_PATHS.update(("stream", "sentinel", "set", leaf) for leaf in ("startpos", "endpos", "apply", "prefetch"))
MAX_BYTES = 2 * 1024 * 1024
TIMEOUT = 10
NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_:.-]*\Z")
FLAG = re.compile(r"(?<![\w-])(?:--[a-zA-Z][a-zA-Z0-9-]*|-[a-zA-Z])(?![\w-])")
FENCE = re.compile(r"^\s*(`{3,}|~{3,})(.*)$")


class CheckError(Exception):
    pass


def read(path, category):
    try:
        if path.stat().st_size > MAX_BYTES:
            raise CheckError(f"{category}: oversized input")
        return path.read_text(encoding="utf-8")
    except UnicodeError as error:
        raise CheckError(f"{category}: invalid UTF-8") from error
    except OSError as error:
        raise CheckError(f"{category}: unreadable file") from error


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def decode(text, category):
    try:
        return json.loads(text, object_pairs_hook=unique_object)
    except (ValueError, RecursionError) as error:
        raise CheckError(f"{category}: malformed JSON") from error


def task_names(path):
    inventory = decode(read(path, "Task inventory"), "Task inventory")
    rows = inventory.get("tasks") if isinstance(inventory, dict) else None
    if not isinstance(rows, list) or not rows:
        raise CheckError("Task inventory: missing or empty tasks")
    names = set()
    for row in rows:
        if (not isinstance(row, dict) or not isinstance(row.get("name"), str)
                or not NAME.fullmatch(row["name"]) or row.get("task") != row["name"]
                or not isinstance(row.get("aliases"), list)):
            raise CheckError("Task inventory: invalid task entry")
        for name in [row["name"], *row["aliases"]]:
            if not isinstance(name, str) or not NAME.fullmatch(name) or name in names:
                raise CheckError("Task inventory: invalid or duplicate task name")
            names.add(name)
    return names


def run(argv, root, category):
    try:
        result = subprocess.run(argv, cwd=root, stdin=subprocess.DEVNULL,
                                capture_output=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired as error:
        raise CheckError(f"{category}: timeout") from error
    except OSError as error:
        raise CheckError(f"{category}: cannot execute") from error
    if len(result.stdout) + len(result.stderr) > MAX_BYTES:
        raise CheckError(f"{category}: oversized output")
    try:
        return result.returncode, result.stdout.decode("utf-8"), result.stderr.decode("utf-8")
    except UnicodeError as error:
        raise CheckError(f"{category}: invalid UTF-8 output") from error


def help_flags(binary, path, root):
    code, out, err = run([str(binary), *path, "--help"], root, "CLI help")
    text = out + "\n" + err
    usage = "Usage: rpgmigrate" + (" " + " ".join(path) if path else "")
    if code or not re.search(re.escape(usage) + r"(?:\s|$)", text):
        raise CheckError("CLI help: failed or invalid help output")
    options = text.split("\nOptions:\n", 1)
    if len(options) != 2:
        raise CheckError("CLI help: missing option inventory")
    lines = []
    for line in options[1].splitlines():
        if line and not line[0].isspace():
            break
        lines.append(line)
    flags = set(FLAG.findall("\n".join(lines)))
    if "--help" not in flags:
        raise CheckError("CLI help: empty option inventory")
    return flags


def markdown_parts(text):
    prose, commands, block = [], [], []
    fence = None
    shell = False
    for line in text.splitlines():
        match = FENCE.match(line)
        if match and fence is None:
            fence = match[1]
            shell = match[2].strip().lower() in ("", "sh", "bash", "shell", "console", "zsh")
            block = []
        elif fence and match and match[1][0] == fence[0] and len(match[1]) >= len(fence) and not match[2].strip():
            if shell:
                commands.append("\n".join(block).replace("\\\n", " "))
            fence = None
        elif fence:
            block.append(line)
        else:
            prose.append(line)
            commands.extend(match[2] for match in re.finditer(r"(`+)(.+?)\1", line))
    if fence:
        raise CheckError("unterminated Markdown fence")
    return "\n".join(prose), commands


def anchors(text):
    prose, _ = markdown_parts(text)
    result = set(re.findall(r'<(?:a|[a-z][a-z0-9]*)\b[^>]*\b(?:id|name)=[\"\']([^\"\']+)[\"\']', prose, re.I))
    previous = ""
    for line in prose.splitlines():
        heading = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*#*\s*$", line)
        title = heading[1] if heading else previous if re.fullmatch(r"\s{0,3}(?:=+|-+)\s*", line) else None
        if title:
            title = html.unescape(re.sub(r"<[^>]*>", "", title)).lower()
            title = re.sub(r"\[([^]]+)\]\([^)]*\)", r"\1", title)
            slug = "".join(char for char in title if char.isalnum() or char in "_ -").replace(" ", "-")
            candidate, suffix = slug, 0
            while candidate in result:
                suffix += 1
                candidate = f"{slug}-{suffix}"
            result.add(candidate)
        previous = line.strip()
    return result


def destinations(prose):
    # Code spans are examples, so they cannot supply live link destinations.
    prose = re.sub(r"(`+).*?\1", "", prose)
    definitions = {}
    lines = []
    for line in prose.splitlines():
        match = re.match(r"^\s{0,3}\[([^]]+)\]:\s*(<[^>]+>|\S+)", line)
        if match:
            definitions[" ".join(match[1].lower().split())] = match[2].strip("<>")
        else:
            lines.append(line)
    body = "\n".join(lines)
    targets = list(definitions.values())
    for match in re.finditer(r"!?\[([^]\n]+)\](\(|\[([^]\n]*)\])", body):
        if match[2] != "(":
            label = " ".join((match[3] or match[1]).lower().split())
            if label not in definitions:
                raise CheckError("undefined link reference")
            targets.append(definitions[label])
            continue
        start = match.end()
        if body[start:start + 1] == "<":
            end = body.find(">", start + 1)
            if end < 0:
                raise CheckError("malformed local link")
            targets.append(body[start + 1:end])
            continue
        depth, end = 0, start
        while end < len(body):
            char = body[end]
            if char == "\\":
                end += 2
                continue
            if char == "(":
                depth += 1
            elif char == ")":
                if depth == 0:
                    break
                depth -= 1
            elif char.isspace() and depth == 0:
                break
            end += 1
        if end == len(body):
            raise CheckError("malformed local link")
        targets.append(body[start:end])
    return targets


def check_links(root, file, prose):
    for target in destinations(prose):
        parsed = urlsplit(html.unescape(target))
        if parsed.scheme or parsed.netloc:
            continue
        if parsed.path.startswith("/"):
            path = root / unquote(parsed.path).lstrip("/")
        elif parsed.path:
            path = file.parent / unquote(parsed.path)
        else:
            path = file
        path = path.resolve()
        if not path.is_relative_to(root):
            raise CheckError(f"{file.relative_to(root)}: local link outside repository")
        if not path.exists():
            raise CheckError(f"{file.relative_to(root)}: broken link")
        if parsed.fragment:
            if not path.is_file() or unquote(parsed.fragment) not in anchors(read(path, "link anchor")):
                raise CheckError(f"{file.relative_to(root)}: broken anchor")


def check_commands(commands, names, binary, root, helps):
    for snippet in commands:
        try:
            lexer = shlex.shlex(snippet, posix=True, punctuation_chars=";&|<>\n")
            lexer.whitespace = " \t\r"
            lexer.whitespace_split = True
            tokens = list(lexer)
        except ValueError as error:
            raise CheckError("malformed command example") from error
        for index, token in enumerate(tokens):
            executable = token.rsplit("/", 1)[-1]
            if executable not in ("task", "rpgmigrate", "$RPGMIGRATE_BIN", "${RPGMIGRATE_BIN}"):
                continue
            args = []
            for arg in tokens[index + 1:]:
                if arg in ("\n", ";", "&&", "||", "|", ">", ">>", "<"):
                    break
                args.append(arg)
            if executable == "task":
                for arg in args:
                    if not arg.startswith("-") and "=" not in arg and arg not in names:
                        raise CheckError("unknown task in maintained guide")
                continue
            path = []
            for arg in args:
                if arg.startswith("-"):
                    break
                path.append(arg)
            prefix = tuple(path)
            routes = [route for route in HELP_PATHS if prefix[:len(route)] == route]
            route = max(routes, key=len)
            has_children = any(candidate[:len(route)] == route and len(candidate) > len(route) for candidate in HELP_PATHS)
            if len(prefix) > len(route) and has_children:
                raise CheckError("CLI command is not in the reviewed help routes")
            if route == ("help",):
                route = ()
            if route not in helps:
                helps[route] = help_flags(binary, route, root)
            for arg in args:
                if arg.startswith("--"):
                    flags = [arg.split("=", 1)[0]]
                elif arg.startswith("-") and arg != "-":
                    flags = ["-" + char for char in arg[1:]]
                else:
                    continue
                if any(flag not in helps[route] for flag in flags):
                    raise CheckError("CLI flag absent from actual command help")


def check_ste(root, files):
    script = root / "tools/docs/ste-lint.py"
    if not script.is_file():
        raise CheckError("STE: missing vendored linter")
    code, out, err = run([sys.executable, str(script), "--json", "--baseline", "0", *map(str, files)], root, "STE")
    data = decode(out, "STE")
    if not isinstance(data, dict) or not isinstance(data.get("violations"), list):
        raise CheckError("STE: invalid findings")
    findings = data["violations"]
    counts = ("count", "hard_count", "baseline", "words")
    if any(type(data.get(key)) is not int or data[key] < 0 for key in counts):
        raise CheckError("STE: invalid counts")
    for finding in findings:
        if (not isinstance(finding, dict) or finding.get("level") not in ("advisory", "advisory-free")
                or not all(isinstance(finding.get(key), str) and finding[key] for key in ("file", "rule", "message"))
                or any(type(finding.get(key)) is not int or finding[key] < 1 for key in ("line", "col"))
                or finding["file"] not in set(map(str, files))):
            raise CheckError("STE: malformed finding")
    hard = sum(finding["level"] == "advisory-free" for finding in findings)
    if data["count"] != len(findings) or data["hard_count"] != hard or data["baseline"] != 0 or not data["words"]:
        raise CheckError("STE: inconsistent or empty findings inventory")
    if hard:
        raise CheckError(f"STE hard violations: {hard}")
    if code or err:
        raise CheckError("STE: linter execution failed")


def check(root, binary, task_json):
    root = Path(root).resolve()
    binary = Path(binary)
    if not root.is_dir():
        raise CheckError("missing repository root")
    if not GUIDES:
        raise CheckError("empty guide inventory")
    files = []
    contents = []
    for name in GUIDES:
        file = root / name
        if not file.is_file():
            raise CheckError(f"missing guide: {name}")
        if not file.resolve().is_relative_to(root):
            raise CheckError("guide outside repository")
        text = read(file, name)
        if not text.strip():
            raise CheckError(f"empty guide: {name}")
        if "\u2014" in text:
            raise CheckError(f"{name}: em dash")
        files.append(file)
        contents.append(markdown_parts(text))
    names = task_names(Path(task_json))
    if not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise CheckError("missing absolute executable binary")
    helps = {(): help_flags(binary, (), root)}
    for file, (prose, commands) in zip(files, contents):
        check_links(root, file, prose)
        try:
            check_commands(commands, names, binary, root, helps)
        except CheckError as error:
            raise CheckError(f"{file.relative_to(root)}: {error}") from error
    check_ste(root, files)
    return len(files)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--task-json", type=Path, required=True,
                        help="nonempty output of pinned Task 3.54.0 --list-all --json; caller verifies the producer pin")
    args = parser.parse_args(argv)
    try:
        count = check(args.root, args.binary, args.task_json)
    except (CheckError, ValueError) as error:
        print(f"docs check failed: {error}", file=sys.stderr)
        return 1
    print(f"docs check passed: {count} guides")
    return 0


if __name__ == "__main__":
    sys.exit(main())
