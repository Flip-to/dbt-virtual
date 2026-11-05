"""Findings of the second real-BigQuery pilot: macro harness (BigQuery dialect) and CLI tests.

The same bugs are reproduced through real dbt-core + dbt-duckdb in
`tests/test_dbt_duckdb_pilot_round2.py`; the emulator half is in `scripts/run_emulator_e2e.py`.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dbt_virtual.cli import main
from tests.test_cli_and_macros import _sample_manifest
from tests.test_macros_execution import FakeRelation, FakeResult, MacroTestHarness

BASE_VARS = {
    "dbt_virtual": {
        "physical_schema": "analytics_physical",
        "base_environment": "prod",
        "env_schemas": {"staging": "analytics_staging", "prod": "analytics"},
    }
}


def _index(queries: list[str], needle: str) -> int:
    return next(i for i, q in enumerate(queries) if needle in q)


# --- 1. rollback / promote over a legacy TABLE at the environment path -----------------------------


def _rollback_harness(*, missing_physical: str | None = None) -> MacroTestHarness:
    harness = MacroTestHarness(vars_dict=BASE_VARS)
    harness.query_responses.append(
        FakeResult([("orders", "model", None), ("customers", "model", None)])  # current pointers
    )
    harness.query_responses.append(
        FakeResult(
            [
                ("orders", "model", "fp_o0", "analytics_physical.orders__fp_o0", None, None),
                ("customers", "model", "fp_c0", "analytics_physical.customers__fp_c0", None, None),
            ]
        )
    )
    for table in ("orders__fp_o0", "customers__fp_c0"):
        if table != missing_physical:
            harness.existing_relations.add(("test_proj", "analytics_physical", table))
    return harness


def test_rollback_drops_legacy_table_at_env_path_before_creating_the_view() -> None:
    harness = _rollback_harness()
    # `adopt --keep-existing-table` left both legacy tables at the environment path.
    harness.existing_relations.add(("test_proj", "analytics", "orders"))
    harness.existing_relations.add(("test_proj", "analytics", "customers"))

    harness.call_macro("virtual_rollback", env="prod", to_release="v_adopt")

    queries = harness.executed_queries
    for name in ("orders", "customers"):
        drop = _index(queries, f"DROP TABLE IF EXISTS `test_proj`.`analytics`.`{name}`")
        create = _index(queries, f"CREATE OR REPLACE VIEW `test_proj`.`analytics`.`{name}`")
        # BigQuery: CREATE OR REPLACE VIEW on a TABLE fails ("is not allowed for this operation").
        assert drop < create


def test_rollback_changes_nothing_when_a_physical_table_is_gone() -> None:
    harness = _rollback_harness(missing_physical="customers__fp_c0")
    harness.existing_relations.add(("test_proj", "analytics", "orders"))
    with pytest.raises(RuntimeError, match="pre-flight failed"):
        harness.call_macro("virtual_rollback", env="prod", to_release="v_adopt")
    changing = ("DROP ", "CREATE ", "UPDATE ", "MERGE ", "INSERT ", "DELETE ")
    assert not [q for q in harness.executed_queries if q.startswith(changing)]


def test_rollback_does_not_drop_a_view_at_the_env_path() -> None:
    harness = _rollback_harness()
    harness.relation_lookup = lambda db, schema, ident: FakeRelation(db, schema, ident, "view")
    harness.call_macro("virtual_rollback", env="prod", to_release="v_adopt")
    assert not any(q.startswith("DROP TABLE") for q in harness.executed_queries)


def test_promote_replaces_a_legacy_table_even_when_the_model_is_unchanged() -> None:
    harness = MacroTestHarness(vars_dict=BASE_VARS)
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders__fp1"))
    harness.existing_relations.add(("test_proj", "analytics", "orders"))  # legacy TABLE kept by adopt
    harness.query_responses.append(
        FakeResult([("orders", "model", "fp1", "analytics_physical.orders__fp1", None, 2)])
    )
    harness.query_responses.append(FakeResult([("orders", "model", "fp1", "analytics_physical.orders__fp1")]))

    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v1")

    queries = harness.executed_queries
    drop = _index(queries, "DROP TABLE IF EXISTS `test_proj`.`analytics`.`orders`")
    assert drop < _index(queries, "CREATE OR REPLACE VIEW `test_proj`.`analytics`.`orders`")


def test_promote_still_skips_an_unchanged_model_whose_env_path_is_a_view() -> None:
    harness = MacroTestHarness(vars_dict=BASE_VARS)
    harness.relation_lookup = lambda db, schema, ident: FakeRelation(
        db, schema, ident, "view" if schema == "analytics" else "table"
    )
    harness.query_responses.append(
        FakeResult([("orders", "model", "fp1", "analytics_physical.orders__fp1", None, 2)])
    )
    harness.query_responses.append(FakeResult([("orders", "model", "fp1", "analytics_physical.orders__fp1")]))
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v1")
    assert not any("CREATE OR REPLACE VIEW" in q for q in harness.executed_queries)


# --- 2. a failed promote / rollback releases its lease lock -----------------------------------------


def _answer_lock_reads(harness: MacroTestHarness) -> None:
    """The lock read-back sees this runner's own lease; other queries use the queued responses."""
    queued = harness.query_responses
    harness.query_responses = []

    def executor(sql: str):
        if sql.lstrip().startswith("SELECT owner, expires_at"):
            return [("me", "later")], []
        is_select = sql.lstrip().upper().startswith("SELECT")
        res = queued.pop(0) if (queued and is_select) else FakeResult([])
        return res.rows, res.column_names

    harness.query_executor = executor


def _released_after_acquire(queries: list[str]) -> bool:
    acquired = _index(queries, "SELECT owner, expires_at")
    released = [i for i, q in enumerate(queries) if q.startswith("DELETE FROM") and "owner = 'me'" in q]
    return bool(released) and released[-1] > acquired


def test_promote_preflight_failure_releases_the_lock() -> None:
    harness = MacroTestHarness(vars_dict=BASE_VARS)  # the physical table does not exist
    harness.query_responses.append(
        FakeResult([("orders", "model", "fp1", "analytics_physical.orders__fp1", None, 2)])
    )
    _answer_lock_reads(harness)
    with pytest.raises(RuntimeError, match="pre-flight failed"):
        harness.call_macro(
            "virtual_promote", from_env="staging", to_env="prod", release_id="v1", lock=True, lock_owner="me"
        )
    assert _released_after_acquire(harness.executed_queries)


def test_rollback_preflight_failure_releases_the_lock() -> None:
    harness = _rollback_harness(missing_physical="customers__fp_c0")
    _answer_lock_reads(harness)
    with pytest.raises(RuntimeError, match="pre-flight failed"):
        harness.call_macro("virtual_rollback", env="prod", to_release="v_adopt", lock=True, lock_owner="me")
    assert _released_after_acquire(harness.executed_queries)


def _project(tmp_path: Path) -> None:
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(_sample_manifest()), encoding="utf-8")


@pytest.mark.parametrize("command", ["promote", "rollback"])
def test_cli_releases_the_lock_when_the_operation_fails(tmp_path: Path, command: str) -> None:
    _project(tmp_path)
    calls: list[list[str]] = []

    def fake_run_dbt(cmd: list[str], project_dir: Path) -> int:
        calls.append(cmd)
        return 1 if cmd[0] == "run-operation" and cmd[1] != "virtual_release_lock" else 0

    argv = ["--project-dir", str(tmp_path), command, "--lock-owner", "me"]
    if command == "rollback":
        argv += ["--env", "prod"]
    else:
        argv += ["--from", "staging", "--to", "prod", "--release", "v1", "--skip-fingerprint-check"]
    with patch("dbt_virtual.cli._run_dbt", side_effect=fake_run_dbt):
        assert main(argv) == 1  # the original failure is still reported

    release = [c for c in calls if c[:2] == ["run-operation", "virtual_release_lock"]]
    assert len(release) == 1
    assert json.loads(release[0][release[0].index("--args") + 1]) == {"env": "prod", "owner": "me"}


def test_cli_does_not_release_a_lock_it_never_took(tmp_path: Path) -> None:
    _project(tmp_path)
    calls: list[list[str]] = []
    with patch("dbt_virtual.cli._run_dbt", side_effect=lambda cmd, d: calls.append(cmd) or 1):
        assert main(["--project-dir", str(tmp_path), "rollback", "--env", "prod", "--no-lock"]) == 1
    assert not [c for c in calls if "virtual_release_lock" in c]


def test_cli_does_not_release_after_success(tmp_path: Path) -> None:
    _project(tmp_path)
    calls: list[list[str]] = []
    with patch("dbt_virtual.cli._run_dbt", side_effect=lambda cmd, d: calls.append(cmd) or 0):
        assert main(["--project-dir", str(tmp_path), "rollback", "--env", "prod"]) == 0
    assert not [c for c in calls if "virtual_release_lock" in c]


# --- 3. a clone made earlier in the same invocation is not re-seeded ---------------------------------


def _seed_harness(seeded_var: list[str] | None) -> MacroTestHarness:
    vars_dict = json.loads(json.dumps(BASE_VARS))
    if seeded_var is not None:
        vars_dict["_dbt_virtual_seeded"] = seeded_var
    harness = MacroTestHarness(vars_dict=vars_dict)
    harness.graph = {
        "nodes": {
            "model.pkg.events": {
                "unique_id": "model.pkg.events",
                "name": "events",
                "resource_type": "model",
                "database": "test_proj",
                "schema": "analytics_physical",
                "alias": "events__newfp",
                "config": {"materialized": "incremental", "virtual_env": True},
            }
        }
    }
    harness.existing_relations.add(("test_proj", "analytics_physical", "events__prodfp"))
    return harness


def _base_registry_row() -> FakeResult:
    return FakeResult([("events", "analytics_physical.events__prodfp")])


def test_a_clone_made_earlier_in_the_invocation_is_not_dropped_and_recloned() -> None:
    harness = _seed_harness([])
    harness.query_responses.append(_base_registry_row())
    harness.call_macro("seed_incremental_clones", model_names=["events"])  # the CLI's seeding
    assert sum("CLONE" in q for q in harness.executed_queries) == 1

    # The clone now exists, unregistered: dbt has not built the model yet.
    harness.existing_relations.add(("test_proj", "analytics_physical", "events__newfp"))
    harness.query_responses.append(_base_registry_row())
    harness.call_macro("seed_incremental_clones")  # the on-run-start hook
    assert sum("CLONE" in q for q in harness.executed_queries) == 1
    assert not any(q.startswith("DROP TABLE") for q in harness.executed_queries)
    assert not any("never registered" in m for m in harness.logs)


def test_a_clone_seeded_by_the_cli_process_is_recognized_by_the_build_process() -> None:
    harness = _seed_harness(["events"])  # `dbt-virtual run`: seeded by a run-operation before
    harness.existing_relations.add(("test_proj", "analytics_physical", "events__newfp"))
    harness.query_responses.append(_base_registry_row())
    harness.call_macro("seed_incremental_clones")
    assert not any(q.startswith("DROP TABLE") for q in harness.executed_queries)
    assert not any("never registered" in m for m in harness.logs)


def test_a_genuine_orphan_is_still_reseeded() -> None:
    harness = _seed_harness([])
    harness.existing_relations.add(("test_proj", "analytics_physical", "events__newfp"))  # failed earlier run
    harness.query_responses.append(_base_registry_row())
    harness.call_macro("seed_incremental_clones", model_names=["events"])
    assert any("never registered" in m for m in harness.logs)
    assert any(q.startswith("DROP TABLE IF EXISTS") and "events__newfp" in q for q in harness.executed_queries)


def _cli_vars(call: list[str]) -> dict:
    return json.loads(call[call.index("--vars") + 1])


def test_build_passes_an_empty_seeded_list_to_the_build(tmp_path: Path) -> None:
    _project(tmp_path)
    calls: list[list[str]] = []
    action = type("A", (), {"name": "orders_metrics", "action": "SEED_INCREMENTAL"})()
    with (
        patch("dbt_virtual.cli._run_dbt", side_effect=lambda cmd, d: calls.append(cmd) or 0),
        patch("dbt_virtual.cli.classify_plan_actions", return_value={"orders_metrics": action}),
        patch("dbt_virtual.cli.compute_dbt_select_for_build", return_value=(["orders_metrics"], {})),
    ):
        assert main(["--project-dir", str(tmp_path), "build", "--env", "staging", "--skip-compile"]) == 0
    variables = _cli_vars(next(c for c in calls if c[0] == "build"))
    assert variables["_dbt_virtual_seed"] == ["orders_metrics"]
    assert variables["_dbt_virtual_seeded"] == []  # the seeding hook appends what it cloned


# --- 4. BigQuery clone chains are limited to 3: fall back to a copy ---------------------------------


def _clone_sql(harness: MacroTestHarness) -> str:
    quote = "`" if harness.adapter_type == "bigquery" else ""
    return harness.call_macro(
        "clone_table_sql",
        FakeRelation("p", "s", "src", quote_char=quote),
        FakeRelation("p", "s", "dst", quote_char=quote),
    )


def test_bigquery_clone_falls_back_to_a_copy_on_the_chain_limit() -> None:
    sql = _clone_sql(MacroTestHarness(vars_dict=BASE_VARS))
    assert "CLONE `p`.`s`.`src`" in sql
    assert "EXCEPTION WHEN ERROR THEN" in sql
    assert "chained clones" in sql
    assert "CREATE TABLE IF NOT EXISTS `p`.`s`.`dst` COPY `p`.`s`.`src`" in sql
    assert "RAISE USING MESSAGE = @@error.message" in sql  # any other error is not swallowed


def test_clone_fallback_can_be_turned_off_and_ctas_is_unchanged() -> None:
    off = MacroTestHarness(vars_dict={"dbt_virtual": {**BASE_VARS["dbt_virtual"], "clone_chain_fallback": False}})
    assert "EXCEPTION" not in _clone_sql(off)
    ctas = MacroTestHarness(vars_dict={"dbt_virtual": {**BASE_VARS["dbt_virtual"], "use_ctas_for_clone": True}})
    assert "EXCEPTION" not in _clone_sql(ctas)


def test_other_adapters_clone_sql_is_unchanged() -> None:
    for adapter in ("snowflake", "duckdb", "postgres"):
        assert "EXCEPTION" not in _clone_sql(MacroTestHarness(vars_dict=BASE_VARS, adapter_type=adapter))


def _chain_harness(depth: int | None, *, extra_vars: dict | None = None) -> MacroTestHarness:
    """A warehouse where `src` is `depth` clones deep (c1 clone of c2 ... of a regular table)."""
    harness = MacroTestHarness(vars_dict={"dbt_virtual": {**BASE_VARS["dbt_virtual"], **(extra_vars or {})}})
    names = ["src"] + [f"base{i}" for i in range(1, (depth or 0) + 1)]  # src -> base1 -> ... -> regular

    def executor(sql: str):
        if "INFORMATION_SCHEMA.TABLES" not in sql:
            return [], []
        if depth is None:
            return [], []  # metadata not readable
        ident = sql.split("table_name = '")[1].split("'")[0]
        i = names.index(ident)
        if i < depth:
            return [("CLONE", "p", "s", names[i + 1])], []
        return [("BASE TABLE", None, None, None)], []

    harness.query_executor = executor
    return harness


def _clone_or_copy(harness: MacroTestHarness) -> tuple[str, list[str]]:
    outcome = harness.call_macro(
        "clone_or_copy_table",
        FakeRelation("p", "s", "src", quote_char="`"),
        FakeRelation("p", "s", "dst", quote_char="`"),
    )
    writes = [q for q in harness.executed_queries if "INFORMATION_SCHEMA" not in q]
    return outcome, writes


@pytest.mark.parametrize("depth", [0, 1, 2])
def test_clone_when_the_chain_stays_within_the_bigquery_limit(depth: int) -> None:
    outcome, writes = _clone_or_copy(_chain_harness(depth))
    assert outcome == "clone"
    # One statement: the CLONE (with the error backstop), not a standalone COPY.
    assert len(writes) == 1 and writes[0].startswith("BEGIN") and "CLONE `p`.`s`.`src`" in writes[0]


def test_copy_when_the_new_clone_would_be_the_fourth_in_the_chain() -> None:
    harness = _chain_harness(3)
    outcome, writes = _clone_or_copy(harness)
    assert outcome == "copy"
    assert writes == ["CREATE TABLE IF NOT EXISTS `p`.`s`.`dst` COPY `p`.`s`.`src`"]
    assert any("3 clone(s) deep" in m for m in harness.logs)


def test_clone_with_the_error_backstop_when_the_depth_is_unknown() -> None:
    harness = _chain_harness(None)
    outcome, writes = _clone_or_copy(harness)
    assert outcome == "clone"
    assert "CLONE `p`.`s`.`src`" in writes[0] and "EXCEPTION WHEN ERROR" in writes[0]
    assert any("depth" in m and "unknown" in m for m in harness.logs)


def test_depth_check_is_skipped_when_the_fallback_is_off() -> None:
    harness = _chain_harness(3, extra_vars={"clone_chain_fallback": False})
    outcome, writes = _clone_or_copy(harness)
    assert outcome == "clone" and writes[0].startswith("CREATE TABLE") and "COPY" not in writes[0]
    assert not any("INFORMATION_SCHEMA" in q for q in harness.executed_queries)


def test_other_adapters_never_check_the_chain() -> None:
    harness = MacroTestHarness(vars_dict=BASE_VARS, adapter_type="snowflake")
    harness.call_macro(
        "clone_or_copy_table",
        FakeRelation("p", "s", "src", quote_char=""),
        FakeRelation("p", "s", "dst", quote_char=""),
    )
    assert not any("INFORMATION_SCHEMA" in q for q in harness.executed_queries)


# --- 5. diff-data with a composite key -----------------------------------------------------------------


def _diff_queries(key_columns: list[tuple[str, bool]], keys: list[str]) -> list[str]:
    """Run `virtual_data_diff` (BigQuery harness) for a model with `keys`; returns the SQL it ran."""
    from types import SimpleNamespace

    from tests.test_macros_execution import _bq_harness

    harness = _bq_harness()
    harness.graph["nodes"]["model.p.events"] = {
        "name": "events",
        "resource_type": "model",
        "config": {"diff_key": keys},
    }
    cols = [SimpleNamespace(name=n, is_number=(lambda numeric=numeric: numeric)) for n, numeric in key_columns]
    cols.append(SimpleNamespace(name="v", is_number=lambda: False))
    harness.adapter_extras["get_columns_in_relation"] = lambda rel: cols
    harness.adapter_extras["quote"] = lambda name: f"`{name}`"

    def executor(sql: str):
        if "_environments" in sql or "_releases" in sql:
            table = "events__fp2" if "environment = 'staging'" in sql else "events__fp1"
            return [("events", f"analytics_physical.{table}")], []
        if "GROUP BY" in sql:  # one segment of 100 rows on both sides; the hashes differ
            side_hash = 111 if "events__fp2" in sql else 222
            return [(0, 100, side_hash, 1, 100)], []
        if "FULL OUTER JOIN" in sql and "dv_changed" in sql:
            return [(0, 0, 1, 1)], []
        return [], []

    harness.query_executor = executor
    harness.call_macro("virtual_data_diff", from_env="staging", to_env="prod", threshold=10)
    return harness.executed_queries


def test_diff_bisects_on_the_first_numeric_entry_of_a_composite_key() -> None:
    queries = _diff_queries([("event_date", False), ("id", True)], ["event_date", "id"])
    bucket = [q for q in queries if "LEAST(" in q]
    assert bucket, queries
    assert "`id`" in bucket[0].split("LEAST(")[1] and "event_date" not in bucket[0].split("LEAST(")[1].split("AS dv_seg")[0]
    for q in queries:
        if "FULL OUTER JOIN" in q:
            assert "dv_c.`event_date` = dv_t.`event_date` AND dv_c.`id` = dv_t.`id`" in q


def test_diff_with_no_numeric_key_entry_joins_whole_segments_on_every_key() -> None:
    queries = _diff_queries([("event_date", False), ("name", False)], ["event_date", "name"])
    assert not any("LEAST(" in q for q in queries)  # no range bisection
    joins = [q for q in queries if "FULL OUTER JOIN" in q]
    assert joins and all("dv_c.`event_date` = dv_t.`event_date` AND dv_c.`name` = dv_t.`name`" in q for q in joins)
