# rpgmigrate project rules

Follow the inherited [agent instructions](/Users/ydixken/AGENTS.md).
The [approved design](docs/superpowers/specs/2026-11-06-rpgmigrate-design.md) defines product behavior.
The [roadmap](docs/roadmap.md) records the assessment, decisions and milestones; its dated amendments override the design where they differ.
Use [tasks/lessons.md](tasks/lessons.md) for user corrections.

## Project constraints

- Build the `rpgmigrate` executable in one Cargo package.
- Preserve the pinned fixes-branch compatibility baseline and explicit design exceptions.
- Use feature branches; git worktrees are allowed for parallel build work.
- Never adopt, mutate, resume, restart, convert, or clean pgcopydb state or resources.
- Keep all target bookkeeping exclusions tied to exact owned object identities.
- Preserve durable completion and explicit failure for unsupported work or recovery gaps.
- Keep credentials and row contents out of diagnostics and version control.
- Read current operator instructions in its own repository before operator edits.
- Preserve existing work in other repositories and keep operator changes separate.

## Worker boundaries

The leader assigns one owner per file or subtree.
Coordinate shared signature changes before editing them.
Do not let a planning worker edit source or install dependencies without a new assignment.
Tests and documentation accompany behavior changes.
Parallelize the build phase as far as file ownership allows.
Run independent reviews and the Go test suites once per milestone, at its close, not after each task.
Keep runtime support claims tied to passing checks.
Commit only after required checks pass and the leader requests it.

## Subagent effort

Follow the canonical [global subagent-effort policy](/Users/ydixken/.codex/AGENTS.md#subagent-effort).
