#!/usr/bin/env python3
"""Extract the pinned C CLI contract without building or executing pgcopydb."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys


PIN = "86bf908adca02d87063849dae775a02c1659c829"
BASE = "src/bin/pgcopydb/"
FIXTURES = Path(__file__).resolve().parents[1] / "tests/fixtures/cli"
STRING = r'"(?:\\.|[^"\\])*"'
EXPECTED = {
    "nodes": 59, "leaves": 50, "groups_excluding_root": 8,
    "parsers": 11, "long_option_rows": 220, "long_option_names": 77,
    "parser_actions": 199, "short_bindings": 188, "help_documents": 57,
}


def normalize(source):
    # Preserve physical offsets so references survive comments and line splices.
    tokens = STRING + r"|'(?:\\.|[^'\\])*'|/\*[\s\S]*?\*/|//[^\n]*"
    source = re.sub(tokens, lambda m: re.sub(r"[^\n]", " ", m[0])
                    if m[0].startswith(("/*", "//")) else m[0], source)
    return re.sub(r"\\\r?\n", lambda m: " " * (len(m[0]) - 1) + "\n", source)


def at(path, source, start, end):
    return {"path": path, "start": source.count("\n", 0, start) + 1,
            "end": source.count("\n", 0, end - 1) + 1}


def required(pattern, source, label, flags=0):
    match = re.search(pattern, source, flags)
    if match is None:
        raise ValueError(f"Unresolved {label}")
    return match


def arguments(body):
    values = []
    while body.strip():
        match = re.match(r"\s*((?:" + STRING + r"\s*)+|[A-Za-z_]\w*)\s*(,|$)", body)
        if match is None:
            raise ValueError("Unsupported registration expression")
        values.append(match[1].strip())
        body = body[match.end():]
    if len(values) != 6:
        raise ValueError(f"Expected six registration arguments, found {len(values)}")
    return values


def strings(expression, macros):
    if expression == "NULL":
        return None
    if expression in macros:
        expression = macros[expression]
    if not re.fullmatch(r"(?:" + STRING + r"\s*)+", expression):
        raise ValueError(f"Unresolved string: {expression}")
    return "".join(json.loads(m[0]) for m in re.finditer(STRING, expression))


def extract_commands(sources):
    definitions, arrays = {}, {}
    for path, source in sorted(sources.items()):
        clean = normalize(source)
        macros = {m[1]: m[2].strip() for m in re.finditer(
            r"^#define\s+(\w+)\s+((?:" + STRING + r"\s*)+)", clean, re.M)}
        pattern = r"(?m)^(?:static\s+)?CommandLine\s+(\w+)\s*=\s*make_command(_set)?\(([\s\S]*?)\);"
        matches = list(re.finditer(pattern, clean))
        if len(matches) != len(re.findall(r"^(?:static\s+)?CommandLine\s+\w+\s*=", clean, re.M)):
            raise ValueError(f"Unresolved command declaration in {path}")
        for match in matches:
            name, description, usage, help_text, parser, handler = arguments(match[3])
            if match[1] in definitions:
                raise ValueError(f"Duplicate command symbol: {match[1]}")
            help_text = strings(help_text, macros)
            definitions[match[1]] = {
                "symbol": match[1], "name": strings(name, macros),
                "kind": "group" if match[2] else "leaf",
                "description": strings(description, macros),
                "usage": strings(usage, macros), "help": help_text,
                "advertised_options": re.findall(r"^\s*--([a-z][a-z0-9-]*)", help_text or "", re.M),
                "parser": None if parser == "NULL" else parser,
                "handler": None if match[2] else handler,
                "children_symbol": handler if match[2] else None,
                "at": at(path, source, match.start(), match.end()),
            }
        for match in re.finditer(r"CommandLine\s+\*(\w+)\[\]\s*=\s*\{([\s\S]*?)\};", clean):
            children = re.findall(r"&(\w+)\s*,", match[2])
            residue = re.sub(r"&\w+\s*,|NULL|\s", "", match[2])
            if not children or residue or match[1] in arrays:
                raise ValueError(f"Unresolved command array: {match[1]}")
            arrays[match[1]] = (children, at(path, source, match.start(), match.end()))
    if not definitions:
        raise ValueError("No command registrations")
    result, visited = [], set()

    def visit(symbol, parent):
        if symbol not in definitions:
            raise ValueError(f"Unresolved command: {symbol}")
        if symbol in visited:
            raise ValueError(f"Repeated command or cycle: {symbol}")
        visited.add(symbol)
        row = dict(definitions[symbol])
        row["path"] = parent + ([] if symbol == "root" else [row["name"]])
        children_symbol = row.pop("children_symbol")
        result.append(row)
        if row["kind"] == "group":
            if children_symbol not in arrays:
                raise ValueError(f"Unresolved command array: {children_symbol}")
            children, row["children_at"] = arrays[children_symbol]
            row["children"] = [definitions[c]["name"] for c in children if c in definitions]
            for child in children:
                visit(child, row["path"])

    visit("root", [])
    if visited != definitions.keys():
        raise ValueError(f"Unreachable command declarations: {sorted(definitions.keys() - visited)}")
    paths = [tuple(row["path"]) for row in result]
    if len(set(paths)) != len(paths):
        raise ValueError("Duplicate command paths")
    return result


def functions(source):
    return re.finditer(r"^(\w+)\([^;]*?\)\n\{[\s\S]*?^\}", normalize(source), re.M)


def extract_parsers(sources):
    result = {}
    for path, source in sorted(sources.items()):
        tables = 0
        for function in functions(source):
            body = function[0]
            if "static struct option long_options[]" not in body:
                continue
            table = required(r"static struct option long_options\[\]\s*=\s*\{([\s\S]*?)\};", body, "option table")
            tables += 1
            row_pattern = r"\{\s*\"([^\"]+)\",\s*(no_argument|required_argument|optional_argument),\s*NULL,\s*('[^']'|\d+)\s*\}"
            options = []
            for match in re.finditer(row_pattern, table[1]):
                offset = function.start() + table.start(1) + match.start()
                options.append({
                    "name": match[1],
                    "arity": {"no_argument": 0, "required_argument": 1, "optional_argument": 2}[match[2]],
                    "action": match[3].strip("'"),
                    "at": at(path, source, offset, offset + len(match[0])),
                })
            residue = re.sub(row_pattern, "", table[1])
            residue = re.sub(r"\{\s*NULL,\s*0,\s*NULL,\s*0\s*\}|[,\s]", "", residue)
            if not options or residue:
                raise ValueError(f"Unresolved option rows: {function[1]}")
            call = required(r"getopt_long\(argc,\s*argv,\s*(" + STRING + r"|\w+)\s*,\s*long_options,\s*&option_index\)", body, "getopt call")
            literal = call[1]
            position = function.start() + call.start(1)
            if not literal.startswith('"'):
                binding = required(r"\b" + re.escape(literal) + r"\s*=\s*(" + STRING + r")\s*;", body, "optstring")
                literal = binding[1]
                position = function.start() + binding.start(1)
            optstring = strings(literal, {})
            shorts, duplicates = {}, []
            if not re.fullmatch(r"(?:[A-Za-z0-9]:{0,2})+", optstring):
                raise ValueError(f"Unsupported optstring: {optstring}")
            for match in re.finditer(r"([A-Za-z0-9])(:{0,2})", optstring):
                if match[1] in shorts:
                    duplicates.append(match[1])
                shorts[match[1]] = len(match[2])
            by_action = {}
            for row in options:
                by_action.setdefault(row["action"], []).append(row["name"])
            env = []
            for helper, group in [("cli_copydb_getenv", "common"), ("cli_list_getenv", "list"), ("cli_read_coordinator_env", "coordinator")]:
                if re.search(r"\b" + helper + r"\(", body):
                    env.append(group)
            if function[1] in result:
                raise ValueError(f"Duplicate parser: {function[1]}")
            result[function[1]] = {
                "at": at(path, source, function.start(), function.end()),
                "optstring": optstring, "optstring_at": at(path, source, position, position + len(literal)),
                "short_options": shorts, "duplicate_short_options": duplicates,
                "long_options": options, "aliases": [v for v in by_action.values() if len(v) > 1],
                "ordering": "permute" if 'unsetenv("POSIXLY_CORRECT")' in body else "stop_at_operand",
                "environment": env,
            }
        if tables != normalize(source).count("static struct option long_options[]"):
            raise ValueError(f"Unresolved parser scope in {path}")
    if not result:
        raise ValueError("No option parsers")
    return result


def extract_settings(sources):
    constants = {}
    path = BASE + "defaults.h"
    source = sources[path]
    for match in re.finditer(r'^#define\s+((?:DEFAULT_|REPLICATION_|EXIT_CODE_)\w+)\s+("[^"\n]*"|\d+)', normalize(source), re.M):
        constants[match[1]] = {"value": json.loads(match[2]), "at": at(path, source, match.start(), match.end())}
    groups = {name: {"environment": [], "defaults": []} for name in ("common", "list", "coordinator", "startup")}
    for file, group in [("cli_common.c", "common"), ("cli_list.c", "list")]:
        path = BASE + file
        source = sources[path]
        pattern = r"\{\s*(PGCOPYDB_\w+),\s*ENV_TYPE_(INT|BOOL|STRING),\s*&\(options->([\w.]+)\)([\s\S]*?)\}"
        for match in re.finditer(pattern, normalize(source)):
            fields = [s.strip() for s in match[4].strip().strip(",").split(",") if s.strip()]
            row = {"name": match[1], "field": match[3], "type": match[2].lower(), "at": at(path, source, match.start(), match.end())}
            if fields:
                row["size"] = fields[0]
            if len(fields) >= 3 and fields[1] == "true":
                row["minimum"] = int(fields[2])
            if len(fields) >= 5 and fields[3] == "true":
                row["maximum"] = int(fields[4])
            groups[group]["environment"].append(row)
        for match in re.finditer(r"options->([\w.]+)\s*=\s*(DEFAULT_\w+);", normalize(source)):
            if match[2] not in constants:
                raise ValueError(f"Unresolved default: {match[2]}")
            groups[group]["defaults"].append({"field": match[1], "constant": match[2], "value": constants[match[2]]["value"], "at": at(path, source, match.start(), match.end())})
    bindings = [
        ("common", "cli_common.c", "SOURCE_PGURI", "connStrings.source_pguri", "conninfo"),
        ("common", "cli_common.c", "TARGET_PGURI", "connStrings.target_pguri", "conninfo"),
        ("common", "cli_common.c", "SPLIT_TABLES_LARGER_THAN", "splitTablesLargerThan", "bytes"),
        ("common", "cli_common.c", "OUTPUT_PLUGIN", "slot.plugin", "plugin"),
        ("list", "cli_common.c", "SOURCE_PGURI", "connStrings.source_pguri", "conninfo"),
        ("list", "cli_common.c", "SPLIT_TABLES_LARGER_THAN", "splitTablesLargerThan", "bytes"),
        ("coordinator", "cli_common.c", "HOST", "host", "string"),
        ("coordinator", "cli_common.c", "PORT", "port", "int"),
        ("startup", "main.c", "LOG_JSON", "logJSON", "bool"),
        ("startup", "main.c", "LOG_JSON_FILE", "logJSONFile", "bool"),
        ("startup", "main.c", "LOG_TIME_FORMAT", "log_time_format", "string"),
        ("startup", "main.c", "LOG_FILENAME", "log_filename", "path"),
        ("startup", "main.c", "DEBUG_BIN_PATH", "pgcopydb_program", "path"),
    ]
    for group, file, suffix, field, kind in bindings:
        path = BASE + file
        source = sources[path]
        name = "PGCOPYDB_" + suffix
        match = required(r"(?:env_exists|get_env_copy_with_fallback)\(\"?" + name + r'\b', normalize(source), name)
        groups[group]["environment"].append({"name": name, "field": field, "type": kind, "at": at(path, source, match.start(), match.end())})
    if [len(groups[g]["environment"]) for g in groups] != [21, 4, 2, 5]:
        raise ValueError("Environment binding inventory changed")
    if len(groups["common"]["defaults"]) != 5:
        raise ValueError("Default binding inventory changed")
    return constants, groups


def git_show(repo, path):
    result = subprocess.run(["git", "-C", str(repo), "show", f"{PIN}:{path}"], capture_output=True, check=False)
    if result.returncode:
        raise ValueError(f"git show failed for {path}: {result.stderr.decode('utf-8', errors='replace').strip()}")
    if not result.stdout:
        raise ValueError(f"Empty baseline source: {path}")
    return result.stdout


def check_inventory(commands, parsers, helps):
    inventory = {
        "nodes": len(commands), "leaves": sum(r["kind"] == "leaf" for r in commands),
        "groups_excluding_root": sum(r["kind"] == "group" for r in commands) - 1,
        "parsers": len(parsers),
        "long_option_rows": sum(len(p["long_options"]) for p in parsers.values()),
        "long_option_names": len({o["name"] for p in parsers.values() for o in p["long_options"]}),
        "parser_actions": sum(len({o["action"] for o in p["long_options"]}) for p in parsers.values()),
        "short_bindings": sum(len(p["short_options"]) for p in parsers.values()),
        "help_documents": len(helps),
    }
    if inventory != EXPECTED:
        raise ValueError(f"Baseline inventory changed: {inventory}")
    return inventory


def generate(repo):
    tree = git_show(repo, BASE).decode()
    cli_files = sorted(re.findall(r"^cli_\w+\.c$", tree, re.M))
    if len(cli_files) != 12:
        raise ValueError(f"Expected 12 CLI files, found {len(cli_files)}")
    paths = [BASE + name for name in cli_files]
    paths += [BASE + name for name in ("main.c", "defaults.h", "cli_common.h", "env_utils.c", "env_utils.h", "parsing_utils.c", "string_utils.c", "pgsql.c", "ld_service.c")]
    paths += ["src/bin/lib/subcommands.c/commandline.c", "src/bin/lib/subcommands.c/commandline.h", "src/bin/lib/log/src/log.h", "docs/update-help-messages.sh", "LICENSE"]
    paths += ["docs/include/" + name for name in re.findall(r"^[\w-]+\.rst$", git_show(repo, "docs/include/").decode(), re.M)]
    raw = {path: git_show(repo, path) for path in sorted(paths)}
    sources = {path: data.decode("utf-8") for path, data in raw.items()}
    cli = {path: sources[path] for path in paths if path.startswith(BASE + "cli_") and path.endswith(".c")}
    commands, parsers = extract_commands(cli), extract_parsers(cli)
    for row in commands:
        if row["parser"] is not None and row["parser"] not in parsers:
            raise ValueError(f"Unresolved parser: {row['parser']}")
    if {r["parser"] for r in commands if r["parser"]} != parsers.keys():
        raise ValueError("Unreferenced parser")
    constants, settings = extract_settings(sources)
    helps = []
    for row in commands:
        if row["path"] in [["fork"], ["version"]]:
            continue
        path = "docs/include/" + ("-".join(row["path"]) or "pgcopydb") + ".rst"
        if path not in sources:
            raise ValueError(f"Missing help fixture: {path}")
        advertised = re.findall(r"^\s*--([a-z][a-z0-9-]*)", sources[path], re.M)
        if advertised != row["advertised_options"]:
            raise ValueError(f"Help/source drift: {path}")
        helps.append({"command": row["path"], "source": path, "text": sources[path]})
    inventory = check_inventory(commands, parsers, helps)
    manifest = {
        "$schema": "schema.json", "schema_version": 1,
        "generated_by": "tools/extract-cli-contract.py",
        "baseline": {"repository": "ydixken/pgcopydb", "ref": "origin/v0.18-fixes", "commit": PIN, "executable": "pgcopydb", "evidence": "source", "runtime_verified": False},
        "sources": {path: {"sha256": hashlib.sha256(data).hexdigest(), "git_blob": hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest(), "bytes": len(data), "lines": len(data.splitlines())} for path, data in raw.items()},
        "inventory": inventory, "commands": commands, "parsers": parsers,
        "constants": constants, "settings": settings, "help_documents": helps,
        "license": {"source": "LICENSE", "text": sources["LICENSE"]},
    }
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True, help="Local repository containing the pinned baseline commit")
    parser.add_argument("--output", type=Path, default=FIXTURES / "baseline.json")
    parser.add_argument("--check", action="store_true", help="Reject a missing or changed manifest without writing files")
    args = parser.parse_args(argv)
    try:
        manifest = generate(args.repo)
        data = (json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True) + "\n").encode()
        if args.check:
            if not args.output.is_file() or args.output.read_bytes() != data:
                raise ValueError(f"Frozen manifest differs: {args.output}")
            print(f"Verified {args.output}: {json.dumps(manifest['inventory'], sort_keys=True)}")
        else:
            args.output.write_bytes(data)
            print(f"Wrote {args.output}: {json.dumps(manifest['inventory'], sort_keys=True)}")
    except (OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
