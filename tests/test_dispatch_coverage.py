"""Every macro dbt-virtual dispatches must have a `default__` implementation.

dbt resolves `adapter.dispatch('name', 'dbt_virtual')` while parsing the project, so a missing
`default__name` breaks every dbt command on adapters without their own implementation
(BigQuery and Snowflake had no `orphan_table_age_predicate_sql`, measured 2026-11-05 on BigQuery).
"""

from __future__ import annotations

import re
from pathlib import Path

MACROS = Path(__file__).resolve().parent.parent / "macros"


def test_every_dispatched_macro_has_a_default_implementation() -> None:
    source = "\n".join(p.read_text(encoding="utf-8") for p in MACROS.rglob("*.sql"))
    dispatched = set(re.findall(r"adapter\.dispatch\('([a-z_]+)', 'dbt_virtual'\)", source))
    assert dispatched, "no dispatched macros found"
    missing = sorted(name for name in dispatched if f"macro default__{name}(" not in source)
    assert missing == [], f"missing default__ implementations: {missing}"


def test_is_incremental_override_uses_dbt_namespace() -> None:
    hooks = (MACROS / "lifecycle" / "hooks.sql").read_text(encoding="utf-8")
    assert "builtins.is_incremental" not in hooks
