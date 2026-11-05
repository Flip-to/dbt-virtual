"""Behavioral unit tests for dbt-virtual Jinja macros using a mock dbt runtime context."""

from __future__ import annotations

import datetime
import hashlib
import json
import os
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
    def __init__(self, rows: list[tuple[Any, ...]], column_names: list[str] | None = None) -> None:
        self.rows = rows
        self.column_names = column_names or []


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
        target_location: str | None = None,
    ) -> None:
        self.vars_dict = vars_dict or {}
        self.target = {
            "name": target_name,
            "schema": target_schema,
            "database": target_database,
            "type": adapter_type,
        }
        if target_location is not None:
            self.target["location"] = target_location
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
        self.ref_relations: dict[str, FakeRelation] = {}
        # Extra adapter methods for one test (e.g. BigQuery-only `is_replaceable`).
        self.adapter_extras: dict[str, Any] = {}
        # Optional live warehouse (e.g. the BigQuery emulator E2E): `query_executor(sql)` returns
        # `(rows, column_names)` and replaces the canned `query_responses`; `relation_lookup(db,
        # schema, identifier)` returns a FakeRelation or None and replaces `existing_relations`.
        self.query_executor: Any = None
        self.relation_lookup: Any = None
        self.grant_calls: list[tuple[Any, ...]] = []
        self.column_updates: list[tuple[str, dict[str, Any]]] = []

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
            REPO / "macros" / "routing" / "locations.sql",
            REPO / "macros" / "adapters" / "dispatch.sql",
            REPO / "macros" / "lifecycle" / "hooks.sql",
            REPO / "macros" / "lifecycle" / "registry_batch.sql",
            REPO / "macros" / "lifecycle" / "guards.sql",
            REPO / "macros" / "lifecycle" / "operations.sql",
            REPO / "macros" / "lifecycle" / "companion_state.sql",
            REPO / "macros" / "lifecycle" / "companion_state_ops.sql",
            REPO / "macros" / "lifecycle" / "data_diff.sql",
            REPO / "macros" / "testing" / "tvf_test_ref.sql",
        ]
        combined_source = "\n\n".join(f.read_text(encoding="utf-8") for f in macro_files)
        # Replace `{{ return(expr) }}` with a sentinel marker we can intercept, or render via Jinja
        self.template_source = combined_source

    def call_macro(self, macro_name: str, *args: Any, **kwargs: Any) -> str:
        """Render a macro call with the current harness context."""
        # In dbt, `{{ return(x) }}` short-circuits the macro and returns `x` (dbt raises
        # MacroReturn). Raise too: recording the value and rendering on ran the rest of the macro,
        # where nested macro calls then returned rendered text instead of their values.
        def _ret(val: Any) -> str:
            raise _MacroReturn(val)

        def _var(key: str, default: Any = None) -> Any:
            return self.vars_dict.get(key, default)

        def _local_md5(s: str) -> str:
            return hashlib.md5(s.encode("utf-8")).hexdigest()

        def _get_relation(database: str, schema: str, identifier: str) -> FakeRelation | None:
            if self.relation_lookup is not None:
                return self.relation_lookup(database, schema, identifier)
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
            if self.query_executor is not None:
                rows, column_names = self.query_executor(sql)
                return FakeResult(rows, column_names)
            if self.query_responses:
                return self.query_responses.pop(0)
            # Routine existence checks see routines whose CREATE statement already ran, like a warehouse.
            check = re.search(r"`([^`]+)`\.INFORMATION_SCHEMA\.ROUTINES\s+WHERE routine_name = '([^']+)'", sql)
            if check:
                schema_name, routine_name = check.groups()
                created = any(
                    re.search(rf"CREATE OR REPLACE (TABLE )?(FUNCTION|PROCEDURE) [^\n(]*`{re.escape(schema_name)}`\.`{re.escape(routine_name)}`", q)
                    for q in self.executed_queries
                )
                return FakeResult([(routine_name,)] if created else [])
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
                    try:
                        return fn(*a, **kw)
                    except _MacroReturn as macro_return:
                        return macro_return.value

                return _wrapped

        ctx: dict[str, Any] = {
            "return": _ret,
            "var": _var,
            "local_md5": _local_md5,
            "fromjson": json.loads,
            "env_var": lambda name, default="": os.environ.get(name, default),
            "tojson": json.dumps,
            "log": lambda msg, info=False: self.logs.append(str(msg)) or "",
            "execute": self.execute,
            "modules": type("ModulesObj", (), {"re": re, "datetime": datetime})(),
            # dbt's `render()` renders a string in the current context.
            "render": lambda text: self.env.from_string(text).render(**ctx),
            "target": self.target,
            "flags": self.flags,
            "graph": self.graph,
            "model": self.model,
            "this": self.this,
            "run_query": _run_query,
            "ref": lambda name: FakeRelation(
                self.target["database"], "analytics_staging", name, "view"
            ),
            "dbt": type(
                "DbtObj",
                (),
                {
                    "string_literal": staticmethod(lambda s: repr(str(s))),
                    # dbt's is_incremental(): the target table does not exist in these tests.
                    "is_incremental": staticmethod(lambda: False),
                },
            )(),
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
                    "check_schema_exists": staticmethod(lambda database, schema: True),
                    "cache_added": staticmethod(_cache_added),
                    "dispatch": staticmethod(_dispatch),
                    **{name: staticmethod(fn) for name, fn in self.adapter_extras.items()},
                    "grant_access_to": staticmethod(
                        lambda entity, entity_type, role, target: self.grant_calls.append(
                            (entity, entity_type, role, target)
                        )
                    ),
                    "update_columns": staticmethod(
                        lambda rel, cols: self.column_updates.append((str(rel), cols))
                    ),
                },
            )(),
            "builtins": type(
                "BuiltinsObj",
                (),
                {
                    "ref": staticmethod(
                        lambda name: self.ref_relations.get(
                            name,
                            FakeRelation(
                                self.target["database"],
                                "analytics_physical",
                                f"{name}__abcdef123456",
                                "table",
                            ),
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

    try:
        harness.call_macro("virtual_alias_name", "legacy_orders", table_node)
        raise AssertionError("Expected a clear error for a virtual table alias override")
    except RuntimeError as exc:
        assert "custom aliases are not supported for virtual table models" in str(exc)

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

    # A non-fingerprinted physical table whose name contains `__` must keep its relation.
    harness_a.ref_relations["daily__archive"] = FakeRelation(
        "test_proj", "analytics_physical", "daily__archive", "table"
    )
    rel_unrelated = harness_a.call_macro("virtual_ref", "daily__archive")
    assert rel_unrelated.schema == "analytics_physical"
    assert rel_unrelated.identifier == "daily__archive"

    harness_a.graph = {
        "nodes": {
            "model.pkg.pinned": {
                "name": "pinned_orders",
                "resource_type": "model",
                "config": {
                    "materialized": "incremental",
                    "virtual_env": True,
                    "physical_version": "pinned_fp_v1",
                },
            }
        }
    }
    harness_a.ref_relations["pinned_orders"] = FakeRelation(
        "test_proj", "analytics_physical", "pinned_orders__pinned_fp_v1", "table"
    )
    rel_pinned = harness_a.call_macro("virtual_ref", "pinned_orders")
    assert rel_pinned.schema == "analytics_staging"
    assert rel_pinned.identifier == "pinned_orders"

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


def test_jinja_merkle_fallback_tracks_nonvirtual_model_parents() -> None:
    parent_v1 = {
        "unique_id": "model.pkg.shared",
        "name": "shared",
        "resource_type": "model",
        "raw_code": "SELECT 1 AS id",
        "config": {"materialized": "table", "virtual_env": False},
        "depends_on": {"nodes": []},
    }
    child = {
        "unique_id": "model.pkg.virtual_child",
        "name": "virtual_child",
        "resource_type": "model",
        "raw_code": "SELECT id FROM {{ ref('shared') }}",
        "config": {"materialized": "table", "virtual_env": True},
        "depends_on": {"nodes": [parent_v1["unique_id"]]},
    }

    first = MacroTestHarness()
    first.graph = {"nodes": {parent_v1["unique_id"]: parent_v1, child["unique_id"]: child}}
    fp_v1 = first.call_macro("get_fingerprint", child)

    parent_v2 = {**parent_v1, "raw_code": "SELECT 2 AS id"}
    second = MacroTestHarness()
    second.graph = {"nodes": {parent_v2["unique_id"]: parent_v2, child["unique_id"]: child}}
    fp_v2 = second.call_macro("get_fingerprint", child)

    assert fp_v1 != fp_v2


def test_seed_incremental_clones_seeds_before_materialization() -> None:
    """Seeding happens outside the model pre-hook (dbt loads `existing_relation` before pre-hooks)."""
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
            }
        }
    )
    harness.graph = {
        "nodes": {
            "model.pkg.orders_metrics": {
                "unique_id": "model.pkg.orders_metrics",
                "name": "orders_metrics",
                "resource_type": "model",
                "database": "test_proj",
                "schema": "analytics_physical",
                "alias": "orders_metrics__newfp123456",
                "config": {
                    "materialized": "incremental",
                    "virtual_env": True,
                    "virtual_add_columns": ["discount_amount NUMERIC", "coupon_code STRING"],
                },
            }
        }
    }
    # Base table exists in analytics_physical
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders_metrics__prodfp000000"))
    harness.query_responses.append(
        FakeResult([("orders_metrics", "analytics_physical.orders_metrics__prodfp000000")])
    )

    harness.call_macro("seed_incremental_clones")
    executed = "\n".join(harness.executed_queries)
    assert (
        "CREATE TABLE IF NOT EXISTS `test_proj`.`analytics_physical`.`orders_metrics__newfp123456`\n"
        "    CLONE `test_proj`.`analytics_physical`.`orders_metrics__prodfp000000`" in executed
    )
    assert "ADD COLUMN IF NOT EXISTS discount_amount NUMERIC" in executed
    assert "ADD COLUMN IF NOT EXISTS coupon_code STRING" in executed
    # The clone is registered in dbt's relation cache so the materialization treats it as existing.
    assert [str(r) for r in harness.cache_added_relations] == [
        "`test_proj`.`analytics_physical`.`orders_metrics__newfp123456`"
    ]


def test_clone_if_new_fp_pre_hook_no_longer_clones() -> None:
    """A clone made in a pre-hook was replaced by dbt's `create or replace table` (BigQuery)."""
    harness = MacroTestHarness(vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical"}})
    harness.model = {"name": "orders_metrics", "config": {"materialized": "incremental", "virtual_env": True}}
    harness.this = FakeRelation("test_proj", "analytics_physical", "orders_metrics__newfp123456")
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders_metrics__prodfp000000"))
    harness.query_responses.append(FakeResult([("analytics_physical.orders_metrics__prodfp000000",)]))

    assert harness.call_macro("clone_if_new_fp") == ""
    assert not any("CLONE" in q for q in harness.executed_queries)
    assert harness.cache_added_relations == []


def test_is_incremental_does_not_claim_unseeded_table() -> None:
    """Returning True for a missing table made BigQuery keep only the incremental delta."""
    harness = MacroTestHarness(vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical"}})
    harness.model = {"name": "orders_metrics", "config": {"materialized": "incremental", "virtual_env": True}}
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders_metrics__prodfp000000"))
    harness.query_responses.append(FakeResult([("analytics_physical.orders_metrics__prodfp000000",)]))

    # `builtins.is_incremental()` is False in the harness: the target table does not exist yet.
    assert harness.call_macro("is_incremental") is False
    # The name used in docs/guides/getting_started.md exists.
    assert harness.call_macro("virtual_is_incremental") is False
    assert harness.executed_queries == []


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
    assert "WITH replaced AS" in upsert_sql and "DELETE FROM analytics_physical._environments" in upsert_sql
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
    assert "MAX(COALESCE(promoted_at, created_at)) AS release_ts" in sf_orphan
    assert "GROUP BY release_id" in sf_orphan
    # Retention is per environment, so PR candidates cannot push out prod's rollback targets.
    assert "DENSE_RANK() OVER (PARTITION BY release_env ORDER BY release_ts DESC) <= 5" in sf_orphan
    assert "t.created < DATEADD(day, -7, CURRENT_TIMESTAMP())" in sf_orphan
    assert "LEFT JOIN protected_tables" in sf_orphan

    bq_harness = MacroTestHarness(adapter_type="bigquery")
    bq_orphan = bq_harness.call_macro("find_orphan_tables_sql", "analytics_physical", 5, 7)
    assert "MAX(COALESCE(promoted_at, created_at)) AS release_ts" in bq_orphan
    assert "GROUP BY release_id" in bq_orphan
    assert "DENSE_RANK() OVER (PARTITION BY release_env ORDER BY release_ts DESC) <= 5" in bq_orphan

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
    assert "MAX(COALESCE(promoted_at, created_at)) AS release_ts" in duckdb_orphan
    assert "GROUP BY release_id" in duckdb_orphan
    assert "DENSE_RANK() OVER (PARTITION BY release_env ORDER BY release_ts DESC)" in duckdb_orphan
    assert "MAX(age_r.created_at) < CURRENT_TIMESTAMP - INTERVAL '7 days'" in duckdb_orphan

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
                (
                    "sp_update",
                    "procedure",
                    "fpsp",
                    "analytics_staging.sp_update",
                    "CREATE OR REPLACE PROCEDURE `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`sp_update`() BEGIN INSERT INTO `test_proj`.`analytics`.`audit_log` VALUES (1); UPDATE `test_proj`.`analytics`.`audit_log` SET id = id + 1; END;",
                    4,
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
    assert (
        "CREATE OR REPLACE PROCEDURE `test_proj`.`analytics`.`sp_update`() BEGIN "
        "INSERT INTO `test_proj`.`analytics`.`audit_log` VALUES (1); UPDATE "
        "`test_proj`.`analytics`.`audit_log` SET id = id + 1; END;"
    ) in harness.executed_queries
    assert "DROP VIEW IF EXISTS `test_proj`.`analytics`.`legacy_view`" in joined_queries
    assert "SET status = 'promoted'" in joined_queries

    # 2. virtual_rollback to previous release
    harness.executed_queries.clear()
    harness.query_responses.append(FakeResult([("v0",)]))
    harness.query_responses.append(
        FakeResult(
            [
                ("orders_metrics", "model"),
                ("new_model", "model"),
                ("new_fn", "function"),
            ]
        )
    )
    harness.query_responses.append(
        FakeResult(
            [
                (
                    "orders_metrics",
                    "model",
                    "fp000",
                    "analytics_physical.orders_metrics__fp000",
                    None,
                ),
                (
                    "sp_rollback",
                    "procedure",
                    "fpsp0",
                    "analytics.sp_rollback",
                    "CREATE OR REPLACE PROCEDURE `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`sp_rollback`() BEGIN INSERT INTO `test_proj`.`analytics`.`audit_log` VALUES (2); UPDATE `test_proj`.`analytics`.`audit_log` SET id = id + 1; END;",
                ),
            ]
        )
    )
    # Rollback checks that the release's physical tables still exist before re-pointing views.
    harness.existing_relations.add(("test_proj", "analytics_physical", "orders_metrics__fp000"))
    harness.call_macro("virtual_rollback", env="prod", to_release="")
    rollback_queries = "\n".join(harness.executed_queries)
    assert "SELECT * FROM `test_proj`.`analytics_physical`.`orders_metrics__fp000`" in rollback_queries
    assert (
        "CREATE OR REPLACE PROCEDURE `test_proj`.`analytics`.`sp_rollback`() BEGIN "
        "INSERT INTO `test_proj`.`analytics`.`audit_log` VALUES (2); UPDATE "
        "`test_proj`.`analytics`.`audit_log` SET id = id + 1; END;"
    ) in harness.executed_queries
    assert "DROP VIEW IF EXISTS `test_proj`.`analytics`.`new_model`" in rollback_queries
    assert "DROP FUNCTION IF EXISTS `test_proj`.`analytics`.`new_fn`" in rollback_queries
    assert "WHERE environment = 'prod' AND model_name = 'new_model'" in rollback_queries
    assert "WHERE environment = 'prod' AND model_name = 'new_fn'" in rollback_queries
    assert rollback_queries.index("DROP VIEW IF EXISTS `test_proj`.`analytics`.`new_model`") < rollback_queries.index(
        "DROP FUNCTION IF EXISTS `test_proj`.`analytics`.`new_fn`"
    )

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
    for registry in ("_environments", "_releases"):
        harness.existing_relations.add(("test_proj", "analytics_physical", registry))

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
    harness.query_responses.append(FakeResult([]))  # delete own lease
    harness.query_responses.append(FakeResult([]))  # insert own lease
    harness.query_responses.append(FakeResult([("ci_job_1", "2026-10-27T23:59:59Z")]))  # live leases
    harness.call_macro("virtual_acquire_lock", env="prod", owner="ci_job_1", ttl_minutes=15)
    lock_queries = "\n".join(harness.executed_queries)
    assert "INSERT INTO `test_proj`.`analytics_physical`._locks" in lock_queries
    assert "'ci_job_1'" in lock_queries
    assert "ORDER BY acquired_at ASC, owner ASC" in lock_queries

    # An older live lease of another owner wins: this owner removes its lease and fails.
    harness.executed_queries.clear()
    harness.query_responses.extend([FakeResult([]), FakeResult([]), FakeResult([])])
    harness.query_responses.append(
        FakeResult([("ci_job_other", "2026-10-27T23:59:59Z"), ("ci_job_1", "2026-10-27T23:59:59Z")])
    )
    try:
        harness.call_macro("virtual_acquire_lock", env="prod", owner="ci_job_1", ttl_minutes=15)
        raise AssertionError("Expected RuntimeError on lock conflict")
    except RuntimeError as exc:
        assert "dbt-virtual lock conflict" in str(exc)
        assert "'ci_job_other'" in str(exc)
    assert harness.executed_queries[-1].startswith("DELETE FROM `test_proj`.`analytics_physical`._locks")
    assert "owner = 'ci_job_1'" in harness.executed_queries[-1]

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



    # The registry DDL runs as SQL statements, not one statement per character of the SQL string.
    assert all(len(q) > 1 for q in harness.executed_queries)


def _bq_harness(**cfg: Any) -> MacroTestHarness:
    return MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
                "env_schemas": {"staging": "analytics_staging", "prod": "analytics"},
                **cfg,
            }
        }
    )


def test_bigquery_audit_counts_rows_from_table_metadata() -> None:
    """`COUNT(*)` fails on BigQuery tables with `require_partition_filter = true`.

    Measured 2026-11-04 on BigQuery: `SELECT COUNT(*)` on a table with require_partition_filter
    returned "Cannot query over table ... without a filter over column(s) ... that can be used
    for partition elimination", while `__TABLES__.row_count` returned the row count.
    """
    harness = _bq_harness()
    harness.query_responses.extend(
        [
            FakeResult([("events", "analytics_physical.events__fp2")]),
            FakeResult([("events", "analytics_physical.events__fp1")]),
            FakeResult([(90,)]),
            FakeResult([(100,)]),
        ]
    )
    harness.call_macro(
        "virtual_audit", from_env="staging", to_env="prod", max_row_drop_pct=20.0, check_coverage=False
    )
    count_queries = harness.executed_queries[2:]
    assert len(count_queries) == 2
    assert all("COUNT(*)" not in q for q in count_queries)
    assert "FROM `test_proj`.`analytics_physical`.__TABLES__" in count_queries[0]
    assert "WHERE table_id = 'events__fp2'" in count_queries[0]
    assert "WHERE table_id = 'events__fp1'" in count_queries[1]


def test_routine_ddl_template_replaces_only_qualified_env_schema() -> None:
    harness = _bq_harness()
    ddl = (
        "CREATE OR REPLACE TABLE FUNCTION `test_proj`.`analytics_staging`.`tvf`()\nAS (\n"
        "SELECT o.analytics_staging_flag, 'analytics_staging' AS label\n"
        "FROM `test_proj`.`analytics_staging`.`orders` AS o\n"
        "JOIN raw_analytics_staging.events USING (id)\n"
        "JOIN analytics_staging.dim USING (id)\n"
        "JOIN `test_proj.analytics_staging.fact` USING (id)\n)"
    )
    template = harness.call_macro("env_schema_placeholder_sql", ddl, "analytics_staging")
    assert template == (
        "CREATE OR REPLACE TABLE FUNCTION `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf`()\nAS (\n"
        "SELECT o.analytics_staging_flag, 'analytics_staging' AS label\n"
        "FROM `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`orders` AS o\n"
        "JOIN raw_analytics_staging.events USING (id)\n"
        "JOIN __DBT_VIRTUAL_ENV_SCHEMA__.dim USING (id)\n"
        "JOIN `test_proj.__DBT_VIRTUAL_ENV_SCHEMA__.fact` USING (id)\n)"
    )


def test_bigquery_routine_template_literal_escapes_quotes_backslashes_and_newlines() -> None:
    """dbt-bigquery's `string_literal` is `'''value'''`: it breaks on `r'\\d'` regexes and trailing quotes."""
    import ast

    harness = _bq_harness()
    body = "SELECT REGEXP_EXTRACT(x, r'\\d+') AS n\nFROM t\nWHERE s = 'it''s'"
    assert "\\d" in body and "\n" in body
    literal = harness.call_macro("sql_string_literal", body)
    assert literal.startswith("'") and literal.endswith("'")
    assert "\n" not in literal
    # BigQuery and Python share the escapes used here (\, \', \n), so Python can decode it.
    assert ast.literal_eval(literal) == body

    pg = MacroTestHarness(adapter_type="postgres")
    assert pg.call_macro("sql_string_literal", "it's") == "'it''s'"


def test_unmapped_env_schema_derives_from_base_env_schema() -> None:
    harness = _bq_harness()
    assert harness.call_macro("get_env_schema", "staging") == "analytics_staging"
    # Documented fallback `<base_env_schema>_<env>`; it used to be `target.schema`.
    assert harness.call_macro("get_env_schema", "pr_42") == "analytics_pr_42"
    assert harness.call_macro("get_env_schema", "pr-42") == "analytics_pr_42"


def test_get_config_renders_jinja_nested_in_the_vars_mapping() -> None:
    harness = _bq_harness(environment="{{ var('_dbt_virtual_env', target.name) }}")
    assert harness.call_macro("get_current_env") == "staging"
    harness.vars_dict["dbt_virtual"]["env_schemas"]["staging"] = "{{ 'stg_' ~ target.name }}"
    assert harness.call_macro("get_env_schema", "staging") == "stg_staging"


def test_drop_env_refuses_schemas_of_other_environments() -> None:
    harness = _bq_harness()
    harness.vars_dict["dbt_virtual"]["env_schemas"]["oops"] = "analytics_physical"
    try:
        harness.call_macro("virtual_drop_env", env="oops")
    except RuntimeError as exc:
        assert "Refusing to drop schema 'analytics_physical'" in str(exc)
    else:
        raise AssertionError("drop-env must refuse the physical schema")
    assert not any("DROP SCHEMA" in q for q in harness.executed_queries)


def test_init_env_deploys_routines_from_recorded_templates() -> None:
    harness = _bq_harness()
    template = (
        "CREATE OR REPLACE TABLE FUNCTION `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`tvf`()\n"
        "AS (SELECT * FROM `test_proj`.`__DBT_VIRTUAL_ENV_SCHEMA__`.`orders`)"
    )
    harness.query_responses.extend(
        [
            FakeResult([("tvf", "table_function", "fp1", "analytics.tvf", template, 3)]),
            FakeResult([]),
        ]
    )
    harness.call_macro("virtual_promote", from_env="prod", to_env="pr_7", release_id="")
    env_query = harness.executed_queries[0]
    assert "CAST(NULL AS STRING)" not in env_query
    assert "MAX(r.routine_ddl_template)" in env_query
    executed = "\n".join(harness.executed_queries)
    assert "CREATE OR REPLACE TABLE FUNCTION `test_proj`.`analytics_pr_7`.`tvf`()" in executed
    assert "FROM `test_proj`.`analytics_pr_7`.`orders`" in executed


def test_promote_keeps_project_models_missing_from_the_release() -> None:
    harness = _bq_harness()
    harness.graph = {
        "nodes": {
            "model.pkg.kept": {
                "unique_id": "model.pkg.kept",
                "name": "kept",
                "resource_type": "model",
                "config": {"materialized": "table", "virtual_env": True},
            }
        }
    }
    harness.existing_relations.add(("test_proj", "analytics_physical", "changed__fp2"))
    harness.query_responses.extend(
        [
            FakeResult([("changed", "model", "fp2", "analytics_physical.changed__fp2", None, 2)]),
            FakeResult(
                [
                    ("changed", "model", "fp1", "analytics_physical.changed__fp1"),
                    ("kept", "model", "fpk", "analytics_physical.kept__fpk"),
                    ("removed", "model", "fpr", "analytics_physical.removed__fpr"),
                ]
            ),
        ]
    )
    harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="r2")
    executed = "\n".join(harness.executed_queries)
    assert "DROP VIEW IF EXISTS `test_proj`.`analytics`.`kept`" not in executed
    assert "DROP VIEW IF EXISTS `test_proj`.`analytics`.`removed`" in executed
