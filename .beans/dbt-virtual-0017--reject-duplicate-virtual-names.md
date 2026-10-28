---
# dbt-virtual-0017
title: 'Reject duplicate virtual model names'
status: completed
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T12:00:00Z
---

dbt permits same-named models from different packages, but dbt-virtual keys environment pointers, fingerprints passed to macros, and physical identifiers by model name. Duplicate virtual names can overwrite fingerprints and state. Detect them before planning and report both manifest node IDs.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue code review and convert bugs into regression tests | Current review task | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Reject duplicate virtual names before fingerprint generation and test two package-scoped nodes with the same name | `src/dbt_virtual/fingerprint.py`; `tests/test_fingerprint.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L3: duplicate virtual names raise a clear error; full suite passes | `uv run pytest -q` (28 passed); `uv run ruff check src tests scripts`; `uv run python scripts/check_docs_layout.py --write-index` | 2026-10-28 |
| Reviewed | Agent: Claude (session 6f3c6805-18ef-442a-8407-0380c1bfc2e1), approved by Yuri Olive | Diff reviewed against main; no defects found | PR #15 review, 2026-10-28; merge approved by Yuri Olive in chat | 2026-10-28 |
| Shipped | Developer: Yuri Olive | Squash-merged to main | PR #15 merged | 2026-10-28 |
| Verified | CI | Post-merge main CI run | pending: main CI run after merge | pending |
