---
# dbt-virtual-0011
title: 'Fail early on virtual table alias overrides'
status: completed
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T12:00:00Z
---

Virtual table aliases were written to the physical schema, but environment views used the model name. With a dbt alias override, downstream `ref()` calls resolve to the alias and point to a missing environment view. Until aliases are supported throughout the lifecycle, the planner and routing macro now reject this combination early.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue reviewing dbt naming and ref routing behavior | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Detect and reject aliases that make environment ref resolution inconsistent | `src/dbt_virtual/fingerprint.py`; `macros/routing/virtual_routing.sql` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L2: Python planner and macro routing tests verify explicit errors for alias overrides | `test_virtual_table_alias_override_fails_clearly`; `test_virtual_schema_and_alias_routing` | 2026-10-28 |
| Reviewed | Agent: Claude (session 6f3c6805-18ef-442a-8407-0380c1bfc2e1), approved by Yuri Olive | Diff reviewed against main; no defects found | PR #8 review, 2026-10-28; merge approved by Yuri Olive in chat | 2026-10-28 |
| Shipped | Developer: Yuri Olive | Squash-merged to main | PR #8 merged | 2026-10-28 |
| Verified | CI | Post-merge main CI run | pending: main CI run after merge | pending |
