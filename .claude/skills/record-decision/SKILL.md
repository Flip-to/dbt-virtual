---
name: record-decision
description: Record a decision in docs/decisions/ with Context, Decision, Why, Alternatives, Impact and Who.
user-invocable: true
allowed-tools: Bash, Read, Grep, Glob, Edit, Write
---

# Record a decision

Standard: `docs/conventions/docs_and_specs.md` (section Decisions). CI gate: `scripts/check_docs_layout.py`.

1. Copy `docs/templates/decision.md` to `docs/decisions/YYYYMMDD_<slug>.md`, dated the day the decision was made, slug in snake_case.
2. Fill frontmatter (`status`, `kind`, `who`, `spec`, `prs`) and all six sections (`Context`, `Decision`, `Why`, `Alternatives`, `Impact`, `Who`).
3. Optionally add `DECISION(<slug>)` in a comment where the code enforces it.
4. Run `uv run python scripts/check_docs_layout.py --write-index`.
