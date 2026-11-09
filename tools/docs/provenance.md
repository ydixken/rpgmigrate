# STE checker provenance

We copied the structural STE checker from the installed `asd-ste100` skill, version `0.4.0`.
The script and MIT license retain their original bytes, including the existing header and copyright notice.
The checker uses only the Python standard library.
It does not reproduce the ASD dictionary or establish complete ASD-STE100 compliance.

- Original script: `~/.agents/skills/asd-ste100/scripts/ste-lint.py`.
- Script SHA256: `155bf7bb73caa70735719ef0390cc39568fcd007509b28000a208839df29f3a3`.
- Original license: `~/.agents/skills/asd-ste100/LICENSE`.
- License SHA256: `d3c674e8592076c9021a59b55177adbcba6fd7e1710082becb922522b22ec672`.
- Local license: [LICENSE-ste](LICENSE-ste), copyright 2026 Dustin Yuchen Teng.

The CLI accepts explicit Markdown paths, `--json`, `--baseline 0`, and `--selftest`.
Exit 0 means hard findings do not exceed the baseline.
Exit 1 means hard findings exceed the baseline.
Malformed UTF-8 also exits 1 with an uncaught decoding error and traceback.
Callers MUST distinguish valid JSON findings from execution errors.
Exit 2 reports a usage error or a file-read operating-system error.
Advisory findings do not fail the check.
Normal operation reads the selected UTF-8 files or standard input and prints findings.
The self-test creates and removes private temporary fixtures.

> [!important]
> Without explicit paths, the tool reads standard input and accepts empty input.
> This tool alone is not a repository gate.
> The later repository checker MUST enforce a nonempty inventory of expected Markdown files.
