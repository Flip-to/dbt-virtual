from __future__ import annotations

from pathlib import Path

from dbt_virtual.fingerprint import (
    canonicalize_sql_ast,
    compute_manifest_fingerprints,
    compute_node_fingerprint,
    extract_relevant_config,
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


def test_nonvirtual_model_dependency_change_invalidates_virtual_fingerprint() -> None:
    upstream_v1 = _make_node(
        name="shared_orders",
        compiled_code="SELECT 1 AS a",
        raw_code="SELECT 1 AS a",
        virtual_env=False,
        materialized="table",
    )
    downstream = _make_node(
        name="virtual_report",
        compiled_code="SELECT a FROM `proj.analytics_staging.shared_orders`",
        raw_code="SELECT a FROM {{ ref('shared_orders') }}",
        depends_on=[upstream_v1["unique_id"]],
    )
    manifest_v1 = {"nodes": {upstream_v1["unique_id"]: upstream_v1, downstream["unique_id"]: downstream}}

    upstream_v2 = _make_node(
        name="shared_orders",
        compiled_code="SELECT 2 AS a",
        raw_code="SELECT 2 AS a",
        virtual_env=False,
        materialized="table",
    )
    manifest_v2 = {"nodes": {upstream_v2["unique_id"]: upstream_v2, downstream["unique_id"]: downstream}}

    fp_v1 = compute_manifest_fingerprints(manifest_v1)[downstream["unique_id"]]
    fp_v2 = compute_manifest_fingerprints(manifest_v2)[downstream["unique_id"]]

    assert fp_v1.fingerprint != fp_v2.fingerprint


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


def test_physical_version_pinning_and_forward_only_config() -> None:
    """Verify SQLMesh-style physical_version pinning and forward_only config extraction."""
    node_default = _make_node(
        compiled_code="SELECT 1 AS id",
    )
    node_pinned = _make_node(
        compiled_code="SELECT 2 AS id_changed",
    )
    node_pinned["config"]["physical_version"] = "pinned_fp_v1"
    node_pinned["config"]["forward_only"] = True
    fp_default = compute_node_fingerprint(node_default)
    fp_pinned = compute_node_fingerprint(node_pinned)
    assert fp_default.fingerprint != "pinned_fp_v1"
    assert fp_pinned.fingerprint == "pinned_fp_v1"
    assert fp_pinned.physical_identifier == "orders_metrics__pinned_fp_v1"
    cfg_payload = extract_relevant_config(node_pinned)
    assert cfg_payload.get("forward_only") is True
    assert cfg_payload.get("physical_version") == "pinned_fp_v1"


def test_property_based_sql_normalization_and_dag_determinism() -> None:
    """Property-based fuzzing for SQL normalization idempotence and Merkle-DAG determinism."""
    from hypothesis import given, settings
    from hypothesis import strategies as st

    ident_strategy = st.from_regex(r"[a-z][a-z0-9_]{1,12}", fullmatch=True)
    ws_strategy = st.sampled_from([" ", "  ", "\n", "\t", " /* comment */ "])

    @settings(max_examples=40)
    @given(
        col1=ident_strategy,
        col2=ident_strategy,
        ws1=ws_strategy,
        ws2=ws_strategy,
        fp_hex=st.from_regex(r"[0-9a-f]{12}", fullmatch=True),
    )
    def _check_invariants(col1: str, col2: str, ws1: str, ws2: str, fp_hex: str) -> None:
        sql_raw = (
            f"SELECT{ws1}{col1},{ws2}COUNT(*) AS {col2} "
            f"FROM `proj.analytics_staging.tbl__{fp_hex}` GROUP BY 1"
        )
        norm1 = normalize_sql(
            sql_raw,
            self_identifier=f"tbl__{fp_hex}",
            known_schemas=["analytics_staging", "analytics"],
        )
        # Idempotence: normalizing an already-normalized query produces the exact same string
        norm2 = normalize_sql(
            norm1,
            self_identifier=f"tbl__{fp_hex}",
            known_schemas=["analytics_staging", "analytics"],
        )
        assert norm1 == norm2
        assert fp_hex not in norm1

        # Target-schema invariance: compiling against `analytics` vs `analytics_staging` yields identical fingerprint
        sql_prod = (
            f"SELECT {col1}, COUNT(*) AS {col2} "
            f"FROM `proj.analytics.tbl__abcdef012345` GROUP BY 1"
        )
        node_stg = _make_node(compiled_code=sql_raw, schema="analytics_staging")
        node_prd = _make_node(compiled_code=sql_prod, schema="analytics")
        fp_s = compute_node_fingerprint(
            node_stg, known_schemas=["analytics_staging", "analytics"]
        )
        fp_p = compute_node_fingerprint(
            node_prd, known_schemas=["analytics_staging", "analytics"]
        )
        assert fp_s.fingerprint == fp_p.fingerprint

    _check_invariants()


