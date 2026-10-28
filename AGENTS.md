# `dbt-virtual` — AI Agent Instructions (`AGENTS.md`)

All coding agents (Claude Code, Antigravity, Cursor, Copilot, Codex) working in this repository must follow the architecture, documentation, evidence, tech-debt, and bean-tracking rules defined in:

1. **[`CLAUDE.md`](CLAUDE.md)** — Repository structure, commands, architectural invariants, and workflows.
2. **[`docs/conventions/docs_and_specs.md`](docs/conventions/docs_and_specs.md)** — Layout and frontmatter rules for `docs/specs/`, `docs/decisions/`, `docs/tech_debt/`, and `.beans/`, enforced by `scripts/check_docs_layout.py`.
3. **[`.claude/rules/evidence.md`](.claude/rules/evidence.md)** — Every claim carries its evidence (`file:line`, test, query, commit) or is phrased as a question.
4. **[`.claude/rules/a2v-track.md`](.claude/rules/a2v-track.md)** — Every bean in `.beans/` carries an `## A2V track` (`Asked → Built → Proven → Reviewed → Shipped → Verified`).
5. **[`.claude/rules/beans-tracking.md`](.claude/rules/beans-tracking.md)** — Bean lifecycle and PR state alignment.
6. **[`.claude/rules/docs-and-specs.md`](.claude/rules/docs-and-specs.md)** — Quick summary of spec, decision (`DECISION(<slug>)`), and tech debt (`TECH_DEBT(<slug>)`) markers.
7. **[`docs/guides/agent_skills.md`](docs/guides/agent_skills.md)** — Available DRY skills in `.claude/skills/` (`/dbt-virtual-setup`, `/dbt-virtual-ops`, `/record-decision`, `/record-tech-debt`, `/a2v-track`).

## Required Checks Before Committing or Opening a PR

Always run and pass these checks locally before finishing a task:

```bash
uv run python scripts/check_docs_layout.py --write-index
uv run ruff check src tests scripts
uv run pytest -v
```
