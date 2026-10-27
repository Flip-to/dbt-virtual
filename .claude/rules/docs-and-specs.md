# Docs and Specs

Applies to: any new or moved file under `docs/`, and any known shortcut left in code.

Full standard: `docs/conventions/docs_and_specs.md`. CI gate: `scripts/check_docs_layout.py`.

- **snake_case names, no `-`**; dates `YYYYMMDD`.
- **Every spec has frontmatter `status`** and the five sections (`Problem`, `Decision`, `Alternatives`, `Rollout`, `Verification`).
- **A decision that outlives its PR gets a file** in `docs/decisions/YYYYMMDD_<slug>.md` with the six sections `Context`, `Decision`, `Why`, `Alternatives`, `Impact`, `Who`, and frontmatter `status` + `kind` + `who`. Optionally mark code with `DECISION(<slug>)`.
- **Known debt gets a file** in `docs/tech_debt/<slug>.md` (frontmatter: `status`, `area`, `impact`, `effort`; a `bean` when open) and a `TECH_DEBT(<slug>)` comment at the code site. Remove the comment and set `paid` + `paid_by` in the PR that fixes it.
- **Regenerate indexes** after adding or editing any spec, decision, or tech debt file: `uv run python scripts/check_docs_layout.py --write-index`.
