"""Behavioral unit tests for dbt-virtual Jinja macros using a mock dbt runtime context."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jinja2

REPO = Path(__file__).resolve().parent.parent


@dataclass
class FakeRelation:
    database: str
    schema: str
    identifier: str
    type: str = "table"
    quote_char: str = "`"

    def __str__(self) -> str:
        if not self.quote_char:
            if self.database:
                return f"{self.database}.{self.schema}.{self.identifier}"
            return f"{self.schema}.{self.identifier}"
        q = self.quote_char
        if self.database:
            return f"{q}{self.database}{q}.{q}{self.schema}{q}.{q}{self.identifier}{q}"
        return f"{q}{self.schema}{q}.{q}{self.identifier}{q}"


class FakeResult:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows


class MacroTestHarness:
    """Loads all dbt-virtual SQL macros into a Jinja2 environment with mocked dbt builtins."""

    def __init__(
        self,
        *,
        vars_dict: dict[str, Any] | None = None,
        target_name: str = "staging",
        target_schema: str = "analytics_staging",
        target_database: str = "test_proj",
        adapter_type: str = "bigquery",
        execute: bool = True,
    ) -> None:
        self.vars_dict = vars_dict or {}
        self.target = {
            "name": target_name,
            "schema": target_schema,
            "database": target_database,
            "type": adapter_type,
        }
        self.adapter_type = adapter_type
        self.execute = execute
        self.existing_relations: set[tuple[str, str, str]] = set()
        self.cache_added_relations: list[FakeRelation] = []
        self.executed_queries: list[str] = []
        self.query_responses: list[FakeResult] = []
        self.logs: list[str] = []
        self.graph: dict[str, Any] = {"nodes": {}}
        self.flags = type("Flags", (), {"FULL_REFRESH": False})()
        self.model: dict[str, Any] = {}
        self.this: FakeRelation | None = None

        self.env = jinja2.Environment(
            extensions=["jinja2.ext.do"],
            trim_blocks=True,
            lstrip_blocks=True,
        )
        self.macros: dict[str, Any] = {}
        self._load_macros()

    def _return(self, val: Any) -> Any:
        raise _MacroReturn(val)

    def _load_macros(self) -> None:
        macro_files = [
            REPO / "macros" / "fingerprint" / "get_fingerprint.sql",
            REPO / "macros" / "routing" / "virtual_routing.sql",
            REPO / "macros" / "adapters" / "dispatch.sql",
            REPO / "macros" / "lifecycle" / "hooks.sql",
            REPO / "macros" / "lifecycle" / "operations.sql",
            REPO / "macros" / "testing" / "tvf_test_ref.sql",
        ]
        combined_source = "\n\n".join(f.read_text(encoding="utf-8") for f in macro_files)
        # Replace `{{ return(expr) }}` with a sentinel marker we can intercept, or render via Jinja
        self.template_source = combined_source

    def call_macro(self, macro_name: str, *args: Any, **kwargs: Any) -> str:
        """Render a macro call with the current harness context."""
        # In dbt, `{{ return(x) }}` short-circuits the macro and returns `x`.
        # We emulate `return` by recording the first `return(...)` value if called, else returning rendered text.
        returned: list[Any] = []

        def _ret(val: Any) -> str:
            if not returned:
                returned.append(val)
            return ""

        def _var(key: str, default: Any = None) -> Any:
            return self.vars_dict.get(key, default)

        def _local_md5(s: str) -> str:
            return hashlib.md5(s.encode("utf-8")).hexdigest()

        def _get_relation(database: str, schema: str, identifier: str) -> FakeRelation | None:
            if (database, schema, identifier) in self.existing_relations:
                return FakeRelation(database, schema, identifier)
            return None

        def _cache_added(rel: FakeRelation) -> None:
            self.cache_added_relations.append(rel)

        def _dispatch(macro_name: str, package: str | None = None) -> Any:
            prefix = f"{self.adapter_type}__{macro_name}"
            if prefix in ns_dict:
                return ns_dict[prefix]
            return ns_dict[f"default__{macro_name}"]

        def _run_query(sql: str) -> FakeResult:
            self.executed_queries.append(sql.strip())
            if self.query_responses:
                return self.query_responses.pop(0)
            return FakeResult([])

        def _raise_compiler_error(msg: str) -> None:
            raise RuntimeError(msg)

        # Transform `{% call statement(...) %}` into a regular block for unit testing
        import re

        src = re.sub(
            r"\{%-?\s*call\s+statement\([^)]*\)\s*-?%\}",
            "{% if true %}",
            self.template_source,
        )
        src = re.sub(r"\{%-?\s*endcall\s*-?%\}", "{% endif %}", src)

        ns_dict: dict[str, Any] = {}
        mod_ref: list[Any] = []

        class NamespaceProxy:
            def __getattr__(self, item: str) -> Any:
                fn = getattr(mod_ref[0], item)

                def _wrapped(*a: Any, **kw: Any) -> Any:
                    prev_len = len(returned)
                    rendered = fn(*a, **kw)
                    if len(returned) > prev_len:
                        val = returned.pop()
                        return val
                    return rendered

                return _wrapped

        ctx: dict[str, Any] = {
            "return": _ret,
            "var": _var,
            "local_md5": _local_md5,
            "fromjson": json.loads,
            "tojson": json.dumps,
            "log": lambda msg, info=False: self.logs.append(str(msg)) or "",
            "execute": self.execute,
            "target": self.target,
            "flags": self.flags,
            "graph": self.graph,
            "model": self.model,
            "this": self.this,
            "run_query": _run_query,
            "ref": lambda name: FakeRelation(
                self.target["database"], "analytics_staging", name, "view"
            ),
            "dbt": type("DbtObj", (), {"string_literal": staticmethod(lambda s: repr(str(s)))})(),
            "exceptions": type("ExcObj", (), {"raise_compiler_error": staticmethod(_raise_compiler_error)})(),
            "api": type(
                "ApiObj",
                (),
                {
                    "Relation": type(
                        "RelCls",
                        (),
                        {
                            "create": staticmethod(
                                lambda database, schema, identifier, type="table": FakeRelation(
                                    database,
                                    schema,
                                    identifier,
                                    type,
                                    "`" if self.adapter_type == "bigquery" else "",
                                )
                            )
                        },
                    )
                },
            )(),
            "adapter": type(
                "AdapterObj",
                (),
                {
                    "get_relation": staticmethod(_get_relation),
                    "cache_added": staticmethod(_cache_added),
                    "dispatch": staticmethod(_dispatch),
                },
            )(),
            "builtins": type(
                "BuiltinsObj",
                (),
                {
                    "ref": staticmethod(
                        lambda name: FakeRelation(
                            self.target["database"],
                            "analytics_physical",
                            f"{name}__abcdef123456",
                            "table",
                        )
                    ),
                    "is_incremental": staticmethod(lambda: False),
                },
            )(),
        }

        ctx["dbt_virtual"] = NamespaceProxy()
        tmpl = self.env.from_string(src, globals=ctx)
        mod = tmpl.make_module(ctx)
        mod_ref.append(mod)
        for attr in dir(mod):
            if not attr.startswith("_") or attr == "_node_self_payload":
                ns_dict[attr] = getattr(ctx["dbt_virtual"], attr)

        fn = getattr(ctx["dbt_virtual"], macro_name)
        return fn(*args, **kwargs)


class _MacroReturn(Exception):
    def __init__(self, value: Any) -> None:
        self.value = value


def test_virtual_schema_and_alias_routing() -> None:
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "env_schemas": {"staging": "analytics_staging", "prod": "analytics"},
            },
            "_dbt_virtual_fps": {"orders_metrics": "825faf9d7511"},
        }
    )

    table_node = {
        "name": "orders_metrics",
        "config": {"materialized": "incremental", "virtual_env": True},
    }
    tvf_node = {
        "name": "tvf_orders_report",
        "config": {"materialized": "table_function", "virtual_env": True},
    }
    regular_node = {
        "name": "raw_stg",
        "config": {"materialized": "view", "virtual_env": False},
    }

    # Table/incremental virtual model routes to physical_schema with __<fp> suffix
    assert harness.call_macro("virtual_schema_name", None, table_node) == "analytics_physical"
    assert harness.call_macro("virtual_alias_name", None, table_node) == "orders_metrics__825faf9d7511"

    # Routine virtual model routes to env_schema (`analytics_staging`) with logical name
    assert harness.call_macro("virtual_schema_name", None, tvf_node) == "analytics_staging"
    assert harness.call_macro("virtual_alias_name", None, tvf_node) == "tvf_orders_report"

    # Non-virtual node uses standard target.schema and logical name
    assert harness.call_macro("virtual_schema_name", "custom", regular_node) == "analytics_staging_custom"
    assert harness.call_macro("virtual_alias_name", None, regular_node) == "raw_stg"


def test_virtual_ref_modes_env_view_physical_and_hybrid() -> None:
    # 1. Default Option A (`env_view`) redirects physical table ref to `<env_schema>.<model_name>` view
    harness_a = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "ref_resolution": "env_view",
                "env_schemas": {"staging": "analytics_staging"},
            }
        }
    )
    rel_a = harness_a.call_macro("virtual_ref", "orders_metrics")
    assert isinstance(rel_a, FakeRelation)
    assert rel_a.schema == "analytics_staging"
    assert rel_a.identifier == "orders_metrics"
    assert rel_a.type == "view"

    # 2. Option B (`physical`) keeps direct `<physical_schema>.<model>__<fp>` table relation
    harness_b = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "ref_resolution": "physical",
                "env_schemas": {"staging": "analytics_staging"},
            }
        }
    )
    rel_b = harness_b.call_macro("virtual_ref", "orders_metrics")
    assert rel_b.schema == "analytics_physical"
    assert rel_b.identifier == "orders_metrics__abcdef123456"

    # 3. Option C (`hybrid`) routes TVF callers to `env_view` and incremental table callers to `physical`
    harness_c = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "ref_resolution": "hybrid",
                "env_schemas": {"staging": "analytics_staging"},
            }
        }
    )
    harness_c.model = {"config": {"materialized": "table_function"}}
    rel_c_tvf = harness_c.call_macro("virtual_ref", "orders_metrics")
    assert rel_c_tvf.schema == "analytics_staging"
    assert rel_c_tvf.identifier == "orders_metrics"

    harness_c.model = {"config": {"materialized": "incremental"}}
    rel_c_tbl = harness_c.call_macro("virtual_ref", "orders_metrics")
    assert rel_c_tbl.schema == "analytics_physical"
    assert rel_c_tbl.identifier == "orders_metrics__abcdef123456"


def test_jinja_merkle_dag_fallback_walks_parent_nodes() -> None:
    parent_v1 = {
        "unique_id": "model.pkg.parent",
        "name": "parent",
        "raw_code": "SELECT 1 AS id",
        "config": {"materialized": "table", "virtual_env": True},
        "depends_on": {"nodes": []},
    }
    child = {
        "unique_id": "model.pkg.child",
        "name": "child",
        "raw_code": "SELECT id FROM {{ ref('parent') }}",
        "config": {"materialized": "table", "virtual_env": True},
        "depends_on": {"nodes": ["model.pkg.parent"]},
    }

    h1 = MacroTestHarness()
    h1.graph = {"nodes": {"model.pkg.parent": parent_v1, "model.pkg.child": child}}
    fp_child_v1 = h1.call_macro("get_fingerprint", child)

    parent_v2 = {
        **parent_v1,
        "raw_code": "SELECT 2 AS id",
    }
    h2 = MacroTestHarness()
    h2.graph = {"nodes": {"model.pkg.parent": parent_v2, "model.pkg.child": child}}
    fp_child_v2 = h2.call_macro("get_fingerprint", child)

    assert len(fp_child_v1) == 12
    assert fp_child_v1 != fp_child_v2


def test_clone_if_new_fp_and_is_incremental_behavior() -> None:
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
            }
        }
    )
    harness.model = {
        "name": "orders_metrics",
        "config": {
            "materialized": "incremental",
            "virtual_env": True,
            "virtual_add_columns": ["discount_amount NUMERIC", "coupon_code STRING"],
        },
    }
    harness.this = FakeRelation("test_proj", "analytics_physical", "orders_metrics__newfp123456")
    # Base table exists in analytics_physical
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders_metrics__prodfp000000"))
    harness.query_responses.append(FakeResult([("analytics_physical.orders_metrics__prodfp000000",)]))

    sql = harness.call_macro("clone_if_new_fp")
    assert "CLONE `test_proj`.`analytics_physical`.`orders_metrics__prodfp000000`" in sql
    assert "ADD COLUMN IF NOT EXISTS discount_amount NUMERIC" in sql
    assert "ADD COLUMN IF NOT EXISTS coupon_code STRING" in sql
    # Verify adapter.cache_added(this) was called so dbt knows the table now exists
    assert harness.cache_added_relations == [harness.this]


def test_multi_adapter_dispatch_postgres_duckdb_fallback() -> None:
    harness = MacroTestHarness(adapter_type="postgres")
    src = FakeRelation("db", "analytics_physical", "orders__v1")
    dst = FakeRelation("db", "analytics_physical", "orders__v2")

    clone_sql = harness.call_macro("clone_table_sql", src, dst)
    assert "CREATE TABLE IF NOT EXISTS" in clone_sql
    assert "SELECT * FROM" in clone_sql
    assert "CLONE" not in clone_sql

    upsert_sql = harness.call_macro(
        "upsert_environment_pointer_sql",
        "analytics_physical",
        "staging",
        "orders",
        "model",
        "fp123",
        "analytics_physical.orders__fp123",
        "rel_1",
    )
    assert "DELETE FROM analytics_physical._environments" in upsert_sql
    assert "INSERT INTO analytics_physical._environments" in upsert_sql

    # Snowflake dispatch: CLONE, UDF/TVF/Procedure DDL, and orphan table query
    sf_harness = MacroTestHarness(
        adapter_type="snowflake",
        target_name="prod",
        target_database="ANALYTICS_DB",
    )
    sf_clone = sf_harness.call_macro("clone_table_sql", src, dst)
    assert "CLONE" in sf_clone
    sf_tvf = sf_harness.call_macro(
        "create_routine_ddl_sql",
        "table_function",
        "analytics.tvf_orders",
        "min_o NUMBER",
        "TABLE(customer_id VARCHAR, orders NUMBER)",
        "SELECT customer_id, orders FROM analytics.orders WHERE orders >= min_o",
    )
    assert "RETURNS TABLE(customer_id VARCHAR, orders NUMBER)" in sf_tvf
    assert "AS $$" in sf_tvf
    sf_orphan = sf_harness.call_macro("find_orphan_tables_sql", "analytics_physical", 5, 7)
    assert "REGEXP_LIKE" in sf_orphan

    for adapter_type, matcher in (
        ("duckdb", "regexp_matches(LOWER(t.table_name), '__[0-9a-f]{8,64}$')"),
        ("postgres", "LOWER(t.table_name) ~ '__[0-9a-f]{8,64}$'"),
        ("databricks", "LOWER(t.table_name) RLIKE '__[0-9a-f]{8,64}$'"),
    ):
        adapter_harness = MacroTestHarness(adapter_type=adapter_type, target_name="prod")
        orphan_sql = adapter_harness.call_macro(
            "find_orphan_tables_sql", "analytics_physical", 5, 7
        )
        assert matcher in orphan_sql
        assert "table_name LIKE '%\\_\\_%'" not in orphan_sql

    unsupported_harness = MacroTestHarness(adapter_type="unsupported", target_name="prod")
    try:
        unsupported_harness.call_macro(
            "find_orphan_tables_sql", "analytics_physical", 5, 7
        )
        raise AssertionError("Expected an error for adapters without a safe GC matcher")
    except RuntimeError as exc:
        assert "no safe fingerprint table matcher is configured" in str(exc)

    duckdb_orphan = harness.call_macro("find_orphan_tables_sql", "analytics_physical", 5, 7)
    assert "MAX(created_at) AS release_created_at" in duckdb_orphan
    assert "GROUP BY release_id" in duckdb_orphan
    assert "DENSE_RANK() OVER (ORDER BY release_created_at DESC)" in duckdb_orphan
    assert "MIN(age_r.created_at) < CURRENT_TIMESTAMP - INTERVAL '7 days'" in duckdb_orphan

    # Databricks dispatch: SHALLOW CLONE
    dbx_harness = MacroTestHarness(adapter_type="databricks", target_name="prod")
    dbx_clone = dbx_harness.call_macro("clone_table_sql", src, dst)
    assert "SHALLOW CLONE" in dbx_clone
    dbx_orphan = dbx_harness.call_macro("find_orphan_tables_sql", "analytics_physical", 5, 7)
    assert "t.created < DATEADD(DAY, -7, CURRENT_TIMESTAMP())" in dbx_orphan


def test_sync_env_view_and_register_routine_deployment() -> None:
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "env_schemas": {"staging": "analytics_staging", "prod": "analytics"},
            },
            "_dbt_virtual_fps": {
                "orders_metrics": "fp1111222233",
                "tvf_orders_report": "fptvf999888",
            },
            "_dbt_virtual_release": "v2026.10.27",
        }
    )
    harness.model = {
        "name": "orders_metrics",
        "config": {"materialized": "incremental", "virtual_env": True},
    }
    harness.this = FakeRelation("test_proj", "analytics_physical", "orders_metrics__fp1111222233")

    sync_sql = harness.call_macro("sync_env_view")
    assert "CREATE OR REPLACE VIEW `test_proj`.`analytics_staging`.`orders_metrics`" in sync_sql
    assert "SELECT * FROM `test_proj`.`analytics_physical`.`orders_metrics__fp1111222233`" in sync_sql
    assert "MERGE `test_proj`.`analytics_physical`._environments" in sync_sql
    assert "INSERT INTO `test_proj`.`analytics_physical`._releases" in sync_sql

    tvf_node = {
        "name": "tvf_orders_report",
        "config": {
            "materialized": "table_function",
            "parameters": "min_o INT64",
            "virtual_env": True,
        },
    }
    tvf_rel = FakeRelation("test_proj", "analytics_staging", "tvf_orders_report", "external")
    reg_sql = harness.call_macro(
        "register_routine_deployment",
        tvf_node,
        tvf_rel,
        "table_function",
        "SELECT * FROM `test_proj`.`analytics_staging`.`orders_metrics` WHERE orders >= min_o",
    )
    assert "__DBT_VIRTUAL_ENV_SCHEMA__" in reg_sql
    assert "table_function" in reg_sql
    assert "3," in reg_sql  # dependency_tier 3 for table_function


def test_virtual_promote_rollback_init_drop_and_gc_operations() -> None:
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
                "env_schemas": {
                    "staging": "analytics_staging",
                    "prod": "analytics",
                    "pr_42": "analytics_pr_42",
                },
            }
        }
    )
    # Mark physical table as existing for promote pre-flight check
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders_metrics__fp111"))

    # 1. virtual_promote from release v1 (tier 1 UDF -> tier 2 model -> tier 3 TVF)
    harness.query_responses.append(
        FakeResult(
            [
                (
                    "udf_double",
                    "function",
                    "fpudf",
                    "analytics_staging.udf_double",
                    "CREATE OR REPLACE FUNCTION `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`udf_double`(n INT64) RETURNS INT64 AS (n * 2)",
                    1,
                ),
                (
                    "orders_metrics",
                    "model",
                    "fp111",
                    "analytics_physical.orders_metrics__fp111",
                    None,
                    2,
                ),
                (
                    "tvf_report",
                    "table_function",
                    "fptvf",
                    "analytics_staging.tvf_report",
                    "CREATE OR REPLACE TABLE FUNCTION `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf_report`() AS (SELECT * FROM `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`orders_metrics`)",
                    3,
                ),
            ]
        )
    )
    # Existing pointers in `prod` (includes an obsolete `legacy_view` that should be demoted,
    # and `udf_double` already at `fpudf` pointing to `analytics.udf_double` which should skip redundant DDL re-execution!)
    harness.query_responses.append(
        FakeResult(
            [
                ("udf_double", "function", "fpudf", "analytics.udf_double"),
                ("legacy_view", "model", "fp_old", "analytics_physical.legacy_view__fp_old"),
            ]
        )
    )
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="v1")
    joined_queries = "\n".join(harness.executed_queries)
    # udf_double was already promoted at fpudf, so its CREATE OR REPLACE FUNCTION is skipped (partial promotion)
    assert "CREATE OR REPLACE FUNCTION `test_proj`.`analytics`.`udf_double`" not in joined_queries
    assert "CREATE OR REPLACE VIEW `test_proj`.`analytics`.`orders_metrics`" in joined_queries
    assert "CREATE OR REPLACE TABLE FUNCTION `test_proj`.`analytics`.`tvf_report`" in joined_queries
    assert "DROP VIEW IF EXISTS `test_proj`.`analytics`.`legacy_view`" in joined_queries
    assert "SET status = 'promoted'" in joined_queries

    # 2. virtual_rollback to previous release
    harness.executed_queries.clear()
    harness.query_responses.append(FakeResult([("v0",)]))
    harness.query_responses.append(
        FakeResult(
            [
                (
                    "orders_metrics",
                    "model",
                    "fp000",
                    "analytics_physical.orders_metrics__fp000",
                    None,
                )
            ]
        )
    )
    harness.call_macro("virtual_rollback", env="prod", to_release="")
    rollback_queries = "\n".join(harness.executed_queries)
    assert "SELECT * FROM `test_proj`.`analytics_physical`.`orders_metrics__fp000`" in rollback_queries

    # 3. virtual_drop_env protects base_environment ('prod') and allows dropping 'pr_42'
    try:
        harness.call_macro("virtual_drop_env", env="prod")
        raise AssertionError("Expected RuntimeError when dropping base_environment")
    except RuntimeError as exc:
        assert "Refusing to drop base_environment" in str(exc)

    harness.executed_queries.clear()
    harness.call_macro("virtual_drop_env", env="pr_42")
    drop_queries = "\n".join(harness.executed_queries)
    assert "DROP SCHEMA IF EXISTS `test_proj`.`analytics_pr_42` CASCADE" in drop_queries
    assert "WHERE environment = 'pr_42'" in drop_queries

    # 4. virtual_gc dry_run vs actual drop
    harness.executed_queries.clear()
    harness.query_responses.append(FakeResult([("orders_metrics__deadbeef0000",)]))
    harness.call_macro("virtual_gc", dry_run=False, retention_releases=5, retention_days=7)
    gc_queries = "\n".join(harness.executed_queries)
    assert "DROP TABLE IF EXISTS `test_proj`.`analytics_physical`.`orders_metrics__deadbeef0000`" in gc_queries

    # 5. tvf_test_ref in fixture mode vs normal mode
    harness.vars_dict["tvf_test_fixtures"] = True
    assert harness.call_macro("tvf_test_ref", "orders_metrics") == "orders_metrics__fixture"
    harness.vars_dict["tvf_test_fixtures"] = False
    ref_rel = harness.call_macro("tvf_test_ref", "orders_metrics")
    assert isinstance(ref_rel, FakeRelation)


def test_virtual_audit_lock_intervals_and_export_state() -> None:
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
                "env_schemas": {
                    "staging": "analytics_staging",
                    "prod": "analytics",
                },
            },
            "_dbt_virtual_fps": {"orders_metrics": "fp1111222233"},
            "_dbt_virtual_release": "v1",
            "_dbt_virtual_start": "2026-10-01",
            "_dbt_virtual_end": "2026-10-27",
        }
    )

    # 1. Interval tracking in sync_env_view when _dbt_virtual_start / _dbt_virtual_end are set
    harness.model = {
        "name": "orders_metrics",
        "config": {"materialized": "incremental", "virtual_env": True},
    }
    harness.this = FakeRelation("test_proj", "analytics_physical", "orders_metrics__fp1111222233")
    sync_sql = harness.call_macro("sync_env_view")
    assert "INSERT INTO `test_proj`.`analytics_physical`._intervals" in sync_sql
    assert "'2026-10-01'" in sync_sql
    assert "'2026-10-27'" in sync_sql

    # 2. Concurrency lock acquisition & conflict detection
    harness.executed_queries.clear()
    harness.query_responses.append(FakeResult([]))  # delete expired
    harness.query_responses.append(FakeResult([]))  # check active lock -> none
    harness.query_responses.append(FakeResult([]))  # delete self
    harness.query_responses.append(FakeResult([]))  # insert lock
    harness.call_macro("virtual_acquire_lock", env="prod", owner="ci_job_1", ttl_minutes=15)
    lock_queries = "\n".join(harness.executed_queries)
    assert "INSERT INTO `test_proj`.`analytics_physical`._locks" in lock_queries
    assert "'ci_job_1'" in lock_queries

    # Active lock held by another owner raises RuntimeError
    harness.query_responses.append(FakeResult([]))  # delete expired
    harness.query_responses.append(FakeResult([("ci_job_other", "2026-10-27T23:59:59Z")]))
    try:
        harness.call_macro("virtual_acquire_lock", env="prod", owner="ci_job_1", ttl_minutes=15)
        raise AssertionError("Expected RuntimeError on lock conflict")
    except RuntimeError as exc:
        assert "dbt-virtual lock conflict" in str(exc)

    harness.executed_queries.clear()
    harness.call_macro("virtual_release_lock", env="prod", owner="ci_job_1")
    assert "DELETE FROM `test_proj`.`analytics_physical`._locks" in "\n".join(harness.executed_queries)

    # 3. virtual_audit quality gate: pass vs fail on empty candidate or excessive row drop
    harness.executed_queries.clear()
    harness.query_responses = [
        FakeResult([("orders_metrics", "analytics_physical.orders_metrics__fp222")]),  # candidate
        FakeResult([("orders_metrics", "analytics_physical.orders_metrics__fp111")]),  # base
        FakeResult([(95,)]),  # candidate count = 95
        FakeResult([(100,)]),  # base count = 100 (5% drop <= 10% max)
    ]
    harness.call_macro(
        "virtual_audit",
        from_env="staging",
        to_env="prod",
        release_id="v2",
        allow_empty=False,
        max_row_drop_pct=10.0,
    )

    # Excessive row drop (100 -> 50 = 50% > 10%) fails fast
    harness.query_responses = [
        FakeResult([("orders_metrics", "analytics_physical.orders_metrics__fp222")]),
        FakeResult([("orders_metrics", "analytics_physical.orders_metrics__fp111")]),
        FakeResult([(50,)]),
        FakeResult([(100,)]),
    ]
    try:
        harness.call_macro(
            "virtual_audit",
            from_env="staging",
            to_env="prod",
            release_id="v2",
            allow_empty=False,
            max_row_drop_pct=10.0,
        )
        raise AssertionError("Expected RuntimeError on excessive row drop")
    except RuntimeError as exc:
        assert "exceeding max_row_drop_pct" in str(exc)

    # 4. virtual_export_state logs structured JSON marker
    harness.logs.clear()
    harness.query_responses = [
        FakeResult(
            [
                ("staging", "orders_metrics", "fp222"),
                ("prod", "orders_metrics", "fp111"),
            ]
        ),
        FakeResult([("orders_metrics__fp111",), ("orders_metrics__fp222",)]),
    ]
    harness.call_macro("virtual_export_state", env="staging", base_env="prod")
    state_logs = [m for m in harness.logs if m.startswith("DBT_VIRTUAL_STATE_JSON:")]
    assert len(state_logs) == 1
    exported = json.loads(state_logs[0].split("DBT_VIRTUAL_STATE_JSON:", 1)[1])
    assert exported["env_state"] == {"orders_metrics": "fp222"}
    assert exported["base_env_state"] == {"orders_metrics": "fp111"}
    assert exported["existing_physical_tables"] == ["orders_metrics__fp111", "orders_metrics__fp222"]


def test_virtual_adopt_zero_compute_existing_project_migration() -> None:
    """Verify zero-compute migration of existing huge production tables via virtual_adopt."""
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
                "env_schemas": {
                    "staging": "analytics_staging",
                    "prod": "analytics",
                },
            },
            "_dbt_virtual_fps": {"huge_events": "fp_adopted_1"},
        }
    )
    harness.graph = {
        "nodes": {
            "model.pkg.huge_events": {
                "unique_id": "model.pkg.huge_events",
                "name": "huge_events",
                "resource_type": "model",
                "config": {"materialized": "incremental", "virtual_env": True},
            }
        }
    }
    # Existing physical table in `analytics.huge_events`
    harness.existing_relations.add(("test_proj", "analytics", "huge_events"))

    harness.call_macro(
        "virtual_adopt",
        env="prod",
        release_id="v_adopt_initial",
        drop_existing_table=True,
    )
    adopt_queries = "\n".join(harness.executed_queries)
    # 1. Zero-copy CLONE from `analytics.huge_events` -> `analytics_physical.huge_events__fp_adopted_1`
    assert (
        "CREATE TABLE IF NOT EXISTS `test_proj`.`analytics_physical`.`huge_events__fp_adopted_1`\n"
        "    CLONE `test_proj`.`analytics`.`huge_events`" in adopt_queries
    )
    # 2. Drops the legacy physical table `analytics.huge_events` and replaces it with a virtual VIEW
    assert "DROP TABLE IF EXISTS `test_proj`.`analytics`.`huge_events`" in adopt_queries
    assert (
        "CREATE OR REPLACE VIEW `test_proj`.`analytics`.`huge_events` AS\n"
        "  SELECT * FROM `test_proj`.`analytics_physical`.`huge_events__fp_adopted_1`" in adopt_queries
    )
    # 3. Registers the adopted table in `_environments` and `_releases`
    assert "MERGE `test_proj`.`analytics_physical`._environments" in adopt_queries
    assert "INSERT INTO `test_proj`.`analytics_physical`._releases" in adopt_queries



