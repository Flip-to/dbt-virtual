---
name: record-tech-debt
description: Record a known shortcut or limitation as docs/tech_debt/<slug>.md with a TECH_DEBT(<slug>) marker in code, or mark one paid.
user-invocable: true
allowed-tools: Bash, Read, Grep, Glob, Edit, Write
---

# Record tech debt

Standard: `docs/conventions/docs_and_specs.md` (section Tech debt). CI gate: `scripts/check_docs_layout.py`.

## New item

1. Check it is not already recorded in `docs/tech_debt/`.
2. Verify it against the current code before writing; cite the file and line.
3. Copy `docs/templates/tech_debt.md` to `docs/tech_debt/<slug>.md` (snake_case slug). Fill `status`, `area`, `impact`, `effort`, `bean`, and sections `What`, `Why it exists`, `Consequence`, `Fix`, `Workaround meanwhile`.
4. Add `TECH_DEBT(<slug>): <short reason>` in a comment at the code site.
5. Run `uv run python scripts/check_docs_layout.py --write-index`.

## Paying it off

In the fixing PR: remove every `TECH_DEBT(<slug>)` marker, set `status: paid` and `paid_by: Flip-to/dbt-virtual#<n>`, keep the file as history, complete the bean, and run `uv run python scripts/check_docs_layout.py --write-index`.
