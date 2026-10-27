# Docs and specs organization

Where written documentation lives, how it is named, and what metadata it carries. Enforced in CI by
`scripts/check_docs_layout.py` (the `Guard docs layout` step of the `ci.yml` workflow).

## Layout

```
docs/                         the only docs tree
  specs/        YYYYMMDD_<slug>.md   design written before or while building
  decisions/    YYYYMMDD_<slug>.md   one choice that outlives its PR, with its reasoning
  findings/     YYYYMMDD_<slug>.md   investigation results and measurements
  runbooks/     <slug>.md            operational how-to
  conventions/  <slug>.md            standards (this file)
  tech_debt/    <slug>.md            one file per debt item, plus the generated README.md index
  templates/                         copy these: spec, decision, finding, tech_debt, runbook
```

The date in a spec, decision, or finding name is the day it was first written, not the day it was last edited.

Every claim in a doc carries its evidence (query, file:line, test, commit, date) or is written as a
question; see `.claude/rules/evidence.md`. Start every new file from `docs/templates/`. The project
skills `/record-decision` and `/record-tech-debt` walk through the two most common ones.
`docs/specs/README.md`, `docs/decisions/README.md` and `docs/tech_debt/README.md` are generated
indexes: after adding or editing a file there, run `uv run python scripts/check_docs_layout.py --write-index`.
Relative links in `docs/` must resolve.

## Names

- Lowercase snake_case for every file and folder under `docs/`. No `-`.
- Dates are `YYYYMMDD`.
- Exempt from lowercase only: `README.md`.

## Specs

Every spec starts with frontmatter:

```yaml
---
status: draft | accepted | implemented | superseded | abandoned
ticket: None                             # optional
bean: dbt-virtual-xxxx                   # optional
prs: []                                  # optional
superseded_by: docs/specs/<file>.md      # required when superseded
---
```

- `draft`: being written. `accepted`: agreed and being built (partly built counts). `implemented`:
  shipped. `superseded`: replaced by another spec. `abandoned`: will not be built.
- Status lives in the frontmatter only. CI rejects a `Status:` line or `## Status` heading in the body.
- Required sections: `## Problem`, `## Decision`, `## Alternatives`, `## Rollout`, `## Verification`.

## Decisions

One file per decision, `docs/decisions/YYYYMMDD_<slug>.md`, dated the day it was made:

```yaml
---
status: accepted | superseded | reverted
kind: product | developer
who: "Developer: <person>" | "Agent: Claude (session <id>), approved by <person>"
superseded_by: docs/decisions/<file>.md    # required when superseded
ticket: None                               # optional
spec: docs/specs/<file>.md                 # optional
prs: []                                    # optional
---
```

Six required `##` sections: `Context`, `Decision`, `Why`, `Alternatives`, `Impact`, `Who`.
Optionally mark the code it governs with `DECISION(<slug>)`, where the slug is the filename without
the date. CI fails if no decision file matches.

## A2V track

Every bean records how its work moved from Asked to Verified (`Asked`, `Built`, `Proven`, `Reviewed`,
`Shipped`, `Verified`), with Who, Why and Evidence per step and a proof level L0-L4. Rule and format:
`.claude/rules/a2v-track.md`; skill: `/a2v-track`.

## Tech debt

One file per item, `docs/tech_debt/<slug>.md`:

```yaml
---
status: open | accepted | paid
area: fingerprinting | routing | routines | lifecycle | adapters | ci
impact: high | medium | low
effort: s | m | l
bean: dbt-virtual-xxxx           # required when open
paid_by: Flip-to/dbt-virtual#N   # required when paid
---
```

- Sections: `What`, `Why it exists`, `Consequence`, `Fix`, `Workaround meanwhile`.
- Mark the code where the debt lives with `TECH_DEBT(<slug>)` in a comment. CI fails if the slug has
  no file, or if the item is `paid` and a marker remains.
- `docs/tech_debt/README.md` is generated with `uv run python scripts/check_docs_layout.py --write-index`.
