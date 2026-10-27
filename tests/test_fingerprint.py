from __future__ import annotations

from pathlib import Path

from dbt_virtual.fingerprint import (
    canonicalize_sql_ast,
    compute_manifest_fingerprints,
    compute_node_fingerprint,
    extract_sql_output_columns,
    is_virtual_node,
    normalize_sql,
    write_fingerprints_file,
)


def _make_node(
    name: str = "orders_metrics",
    *,
    compiled_code: str = "SELECT customer_id, COUNT(*) AS c FROM `proj.analytics_staging.events` GROUP BY 1",
    raw_code: str = "SELECT customer_id, COUNT(*) AS c FROM {{ ref('events') }} GROUP BY 1",
    schema: str = "analytics_staging",
    materialized: str = "incremental",
    virtual_env: bool = True,
    depends_on: list[str] | None = None,
    epoch: int = 1,
) -> dict:
    return {
        "unique_id": f"model.test.{name}",
        "name": name,
        "alias": name,
        "resource_type": "model",
        "schema": schema,
        "compiled_code": compiled_code,
        "raw_code": raw_code,
        "config": {
            "materialized": materialized,
            "incremental_strategy": "insert_overwrite",
            "partition_by": {"field": "order_date", "data_type": "date"},
            "cluster_by": ["customer_id"],
            "virtual_env": virtual_env,
            "virtual_env_epoch": epoch,
        },
        "depends_on": {"nodes": depends_on or []},
    }


def test_is_virtual_node() -> None:
    assert is_virtual_node(_make_node(virtual_env=True)) is True
    assert is_virtual_node(_make_node(virtual_env=False)) is False
    assert is_virtual_node({"resource_type": "test", "config": {"virtual_env": True}}) is False


def test_target_schema_invariance() -> None:
    """Compiling the same model under staging vs prod must yield the exact same fingerprint."""
    node_staging = _make_node(
        compiled_code=(
            "/* dbt comment */\n"
            "SELECT * FROM `my_proj`.`analytics_staging`.events "
            "WHERE dt > (SELECT MAX(dt) FROM `my_proj`.`analytics_physical`.orders_metrics__abcdef123456)"
        ),
        schema="analytics_staging",
    )
    node_prod = _make_node(
        compiled_code=(
            "SELECT * FROM `my_proj`.`analytics`.events\n"
            "WHERE dt > (SELECT MAX(dt) FROM `my_proj`.`analytics_physical`.orders_metrics__999999999999)"
        ),
        schema="analytics",
    )
    known = ["analytics_staging", "analytics", "analytics_physical"]
    fp_stg = compute_node_fingerprint(node_staging, known_schemas=known)
    fp_prod = compute_node_fingerprint(node_prod, known_schemas=known)

    assert fp_stg.fingerprint == fp_prod.fingerprint
    assert fp_stg.physical_identifier == f"orders_metrics__{fp_stg.fingerprint}"


def test_sql_or_config_change_invalidates_fingerprint() -> None:
    base = compute_node_fingerprint(_make_node())
    changed_sql = compute_node_fingerprint(
        _make_node(compiled_code="SELECT customer_id, SUM(x) FROM `proj.analytics_staging.events` GROUP BY 1")
    )
    changed_epoch = compute_node_fingerprint(_make_node(epoch=2))

    assert base.fingerprint != changed_sql.fingerprint
    assert base.fingerprint != changed_epoch.fingerprint


def test_merkle_dag_upstream_propagation(tmp_path: Path) -> None:
    """Changing an upstream virtualized model must propagate a new fingerprint to downstream nodes."""
    parent_v1 = _make_node(
        name="orders_metrics",
        compiled_code="SELECT 1 AS a",
        raw_code="SELECT 1 AS a",
    )
    child_tvf = _make_node(
        name="tvf_orders_report",
        materialized="table_function",
        compiled_code="SELECT a FROM `proj.analytics_staging.orders_metrics`",
        raw_code="SELECT a FROM {{ ref('orders_metrics') }}",
        depends_on=["model.test.orders_metrics"],
    )

    manifest_v1 = {
        "nodes": {
            parent_v1["unique_id"]: parent_v1,
            child_tvf["unique_id"]: child_tvf,
        }
    }
    fps_v1 = compute_manifest_fingerprints(manifest_v1, known_schemas=["analytics_staging"])

    parent_v2 = _make_node(
        name="orders_metrics",
        compiled_code="SELECT 2 AS a",
        raw_code="SELECT 2 AS a",
    )
    manifest_v2 = {
        "nodes": {
            parent_v2["unique_id"]: parent_v2,
            child_tvf["unique_id"]: child_tvf,
        }
    }
    fps_v2 = compute_manifest_fingerprints(manifest_v2, known_schemas=["analytics_staging"])

    assert fps_v1["model.test.orders_metrics"].fingerprint != fps_v2["model.test.orders_metrics"].fingerprint
    assert fps_v1["model.test.tvf_orders_report"].fingerprint != fps_v2["model.test.tvf_orders_report"].fingerprint
    # TVF physical identifier stays logical name (deployed per env schema)
    assert fps_v1["model.test.tvf_orders_report"].physical_identifier == "tvf_orders_report"

    out_path = tmp_path / ".dbt_virtual" / "fingerprints.json"
    write_fingerprints_file(fps_v2, out_path)
    assert out_path.exists()


def test_normalize_sql_strips_comments_and_fp_suffixes() -> None:
    raw = "/* query_tag: abc */ SELECT * FROM `p.analytics_physical.my_table__0123456789ab` WHERE x = 1"
    cleaned = normalize_sql(
        raw,
        self_identifier="my_table__0123456789ab",
        known_schemas=["analytics_physical"],
    )
    assert "0123456789ab" not in cleaned
    assert "__DBT_VIRTUAL_THIS__" in cleaned
    assert "__DBT_VIRTUAL_SCHEMA__." in cleaned


def test_sqlglot_ast_canonicalization_and_column_extraction() -> None:
    """Cosmetic SQL differences (case, line comments, whitespace) must produce identical AST SQL."""
    sql_a = """
    -- leading line comment
    select
        customer_id,
        cast(sum(orders) as int64) as total_orders
    from `proj.analytics.events`
    group by 1
    """
    sql_b = "SELECT customer_id, CAST(SUM(orders) AS INT64) AS total_orders FROM `proj.analytics.events` GROUP BY 1"

    assert normalize_sql(sql_a) == normalize_sql(sql_b)
    assert canonicalize_sql_ast(sql_a) == canonicalize_sql_ast(sql_b)

    cols = extract_sql_output_columns(sql_b)
    assert cols == {"customer_id": "", "total_orders": "INT64"}

