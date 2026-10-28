---
# dbt-virtual-0007
title: 'Implement the documented status JSON output option'
status: in-progress
type: bug
created_at: 2026-10-28T00:00:00Z
updated_at: 2026-10-28T00:00:00Z
---

The operations guide documents `dbt-virtual status --output <path>` and describes its output as input to `plan --state-file`, but the CLI did not accept `--output` or save the state emitted by `virtual_export_state`.

## A2V track

| Step | Who | Why | Evidence | When |
|---|---|---|---|---|
| Asked | Developer: Yuri Olive | Continue reviewing CLI behavior not covered in earlier passes | Chat request, 2026-10-28 | 2026-10-28 |
| Built | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | Parse the state marker and write JSON to the requested path | `src/dbt_virtual/cli.py` | 2026-10-28 |
| Proven | Agent: Codex (session 01a18479-0b39-7373-93c3-6b159946c051), approved by Yuri Olive | L2: Regression tests cover valid export, missing marker, and dbt failure; full suite passes | `test_cli_status_writes_exported_state_json`; `uv run --extra dev pytest -v` — 28 passed; lint and docs layout passed, 2026-10-28 | 2026-10-28 |
| Reviewed | Reviewer: Yuri Olive | pending | pending | pending |
| Shipped | Developer: Yuri Olive | pending | pending | pending |
| Verified | Developer: Yuri Olive | pending | pending | pending |
