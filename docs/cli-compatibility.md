# CLI compatibility evidence

We freeze the CLI source contract from `ydixken/pgcopydb` at `86bf908adca02d87063849dae775a02c1659c829`.
The reference branch is `origin/v0.18-fixes`.
The extractor reads that commit with `git show`, regardless of the reference checkout's active branch or local edits.
The target executable is `rpgmigrate`.

This evidence describes registrations, parser descriptors, and selected control flow.
It does not prove that the target implements the corresponding migration operations.

## Artifacts

- [baseline.json](../tests/fixtures/cli/baseline.json) is the generated source manifest.
- [schema.json](../tests/fixtures/cli/schema.json) defines manifest schema version 1 with JSON Schema draft 2020-12.
- [cases.json](../tests/fixtures/cli/cases.json) contains reviewed source observations for argument, environment, default, and positional cases.
- [anomalies.json](../tests/fixtures/cli/anomalies.json) records baseline defects and help discrepancies separately.
- [target-deltas.json](../tests/fixtures/cli/target-deltas.json) records reviewed changes to target behavior and their review provenance.
- [test_extraction.py](../tests/fixtures/cli/test_extraction.py) tests the extractor and fixture integrity without executing either product.

The extractor generates only `baseline.json`.
The other JSON files contain the schema or reviewed evidence.
Regenerate the manifest with [extract-cli-contract.py](../tools/extract-cli-contract.py).
Do not edit its expected values to match the target implementation.

## Frozen inventory

| Item | Count |
|---|---:|
| Registered nodes, including root | 59 |
| Leaf commands | 50 |
| Command groups, excluding root | 8 |
| Parser functions | 11 |
| Long-option registrations | 220 |
| Distinct long-option names | 77 |
| Parser actions | 199 |
| Short-option bindings | 188 |
| Committed help documents | 57 |

The 57 help documents exclude `fork` and `version`, as the baseline documentation script specifies.
The command inventory includes both commands.
An action count groups long options by their parser return value.
The short-option count excludes duplicate letters in the same parser's raw option string.

## Manifest fields

`baseline` identifies the commit, source repository, executable, and evidence type.
`runtime_verified` is false because extraction does not run the baseline.
`sources` maps each of 83 source paths to its SHA-256, Git blob ID, byte count, and physical line count.
The manifest includes the pinned repository's license text.
It contains no timestamps or local absolute paths.

Each `commands` entry contains its path, registration symbol, kind, parser, handler, description, usage, and help text.
The root path is an empty array.
Command paths exclude the executable name.
Group entries include their child names.
The `at` and `children_at` fields identify inclusive, one-based source ranges.

`parsers` maps C function names to their descriptors.
Each descriptor preserves the raw `optstring`, independent short and long argument requirements, aliases, ordering, and environment groups.
Argument requirements use 0 for no argument, 1 for a required argument, and 2 for an optional argument.
An action is a string containing either a character or a decimal return code.
Aliases share an action within one parser.
Do not combine short flags across parsers: root `-J` means JSON, while common `-J` requires a job count.

`settings` contains 21 common environment bindings, four list bindings, two coordinator bindings, and five startup bindings.
These are binding counts, not a claim to cover every runtime environment variable.
Each binding retains its source field, type, declared bounds, and location.
The five common default assignments reference `constants`.
Conditional defaults and parsed environment values appear in `cases.json`.

`help_documents` retains the baseline's committed help text.
`advertised_options` contains only option names at the start of help rows.
References inside descriptions, such as `vacuumdb --analyze-only`, do not create options.
The extractor compares these names with the corresponding documentation rows.
The help list and parser option list remain separate because baseline discrepancies are evidence.

## Extraction limits and checks

We use narrow regular expressions for the pinned C declarations, option tables, string literals, and environment bindings.
The extractor does not interpret C or implement a CLI parser.
It preserves source offsets while masking comments and resolving line continuations.
Unsupported expressions, unresolved symbols, empty inputs, unreachable commands, incomplete parser scopes, and changed inventory cause failure.

The extractor tests cover those boundaries with synthetic source fragments.
They also check complete leaf paths, short-flag collisions, provenance ranges, and references to reviewed deltas.
Changed command, parser, option, or help counts must fail.
Serialization must remain identical across output paths and source-map order.
Check mode must reject missing or changed output without writing it.
The tests use Python's standard library and require no database or baseline executable.

## Check or regenerate

Run these commands from the repository root.
The reference repository must contain the pinned commit.

1. Check the frozen manifest against that commit.

   ```sh
   python3 -B tools/extract-cli-contract.py --repo ../pgcopydb --check
   ```

2. Run the extractor and evidence tests.

   ```sh
   python3 -B -m unittest discover -s tests/fixtures/cli -p 'test_*.py'
   ```

3. Regenerate the manifest after a reviewed extractor change.

   ```sh
   python3 -B tools/extract-cli-contract.py --repo ../pgcopydb
   ```

Generation uses sorted JSON keys, two-space indentation, ASCII escapes, and a final newline.
Check mode compares the complete bytes with fresh extraction.

## Cases and reviewed differences

Each case records its exact baseline `argv`, environment overrides, conditions, scope, source ranges, and `baseline_observation`.
Parsed fields use C field names so target tests can map them explicitly.
An `accept` observation means parser acceptance under the stated conditions.
It does not authorize handler execution or predict successful database work.
An exit status belongs to a source-derived process path, not an observed container run.
Exact platform diagnostics require separate runtime evidence.

`target_expectation` is a separate reviewed assertion.
A null value does not establish target behavior.
A non-null `delta` links the assertion to its review record.
Unsafe anomalies have no baseline-derived target requirement.

> [!warning]
> Do not execute cases marked `do-not-run` against the baseline.
> They describe unsafe argument access or unchecked arithmetic.
> Their reviewed target deltas require safe handling without reproducing the defect.

The first cases cover help and version exits, removed commands, global option placement, short-flag arity, job bounds, environment precedence, and positional forms.
Defaults include four workers for table, index, and large-object jobs, with restore jobs inheriting index jobs when unset.
Explicit split-part counts must be positive, although the unset value is zero.
Invalid numeric environment values fail before CLI overrides.
Valid CLI connection strings can replace malformed connection strings from the environment.
The typed boolean environment bug has an explicit hardening delta.

The reviewed target changes require `rpgmigrate` identity and native state.
They prohibit adoption, conversion, cleanup, or mutation of legacy pgcopydb state.
They add durability profiles, finite loss limits for relaxed mode, and `--apply-jobs` with a default of one.
The [approved design](superpowers/specs/2026-11-06-rpgmigrate-design.md) defines those additions.

Other reviewed changes reject overflow, malformed byte quantities, unknown explicit plugins, conflicting restart/resume flags, and ignored operational operands.
Dump `-F` and `-N` require values in the target.
List `-k` retains its required, ignored operand.
Invalid options must fail even where the baseline returns successful help or version output.
Unreviewed discrepancies remain observations rather than implicit target requirements.

## Production test ownership

The production CLI worker owns parser and executable tests.
The `task test-cli` suite should compare descriptors from the production parser with this frozen manifest.
Expected descriptors must come from this baseline, with reviewed deltas applied explicitly.
Tests must inspect the descriptors that parsing actually uses, rather than a second target option table.
The suite should also compare advertised help with accepted parser options and report reviewed discrepancies by ID.
Command recognition, parser acceptance, and successful execution need separate assertions.

Recognized commands or features without runtime support must return an explicit failure before side effects.
Passing inventory tests cannot justify a full compatibility claim.
The evidence worker owns only extraction, fixtures, and this guide.

A later worker can add isolated runtime evidence after checking the reference image's version and digest.
That worker must preserve the static observations and record runtime results separately.
Runtime evidence is not a prerequisite for connection or COPY implementation.
