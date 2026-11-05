"""Promotion guards: an incomplete table can never be promoted (macro harness, BigQuery adapter).

a) fingerprints must match the target environment's code;
b) windowed / `--empty` builds are registered as `build_quality = 'partial'` and refused;
c) releases are sealed by `dbt-virtual build --release` after a successful `dbt build`;
d) the audit compares BigQuery partitions of the candidate with the current target table.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dbt_virtual.cli import main
from dbt_virtual.fingerprint import compute_manifest_fingerprints
from dbt_virtual.planner import classify_plan_actions
from tests.test_macros_execution import FakeRelation, FakeResult, MacroTestHarness


def _harness(**cfg) -> MacroTestHarness:
    harness = MacroTestHarness(
        vars_dict={
            "dbt_virtual": {
                "physical_schema": "analytics_physical",
                "base_environment": "prod",
                "env_schemas": {"staging": "analytics_staging", "prod": "analytics"},
                **cfg,
            }
        }
    )
    for table in ("orders__fp1", "events__fp2"):
        harness.existing_relations.add(("test_proj", "analytics_physical", table))
    return harness


# (model, type, fingerprint, physical id, routine ddl, tier, schema_key, build_quality, sealed_at)
def _release_rows(*, quality: str | None = "full", sealed: str | None = "2026-11-04") -> list[tuple]:
    return [
        ("orders", "model", "fp1", "analytics_physical.orders__fp1", None, 2, None, quality, sealed),
        ("events", "model", "fp2", "analytics_physical.events__fp2", None, 2, None, quality, sealed),
    ]


def _promote(harness: MacroTestHarness, rows: list[tuple], **kwargs) -> None:
    harness.query_responses.extend([FakeResult(rows), FakeResult([])])
    harness.call_macro(
        "virtual_promote", from_env="staging", to_env="prod", release_id=kwargs.pop("release_id", "r1"), **kwargs
    )


def _assert_refused(harness: MacroTestHarness, match: str, **kwargs) -> None:
    with pytest.raises(RuntimeError, match=match):
        _promote(harness, **kwargs)
    ddl = [q for q in harness.executed_queries[1:] if not q.startswith("SELECT")]
    assert ddl == [], "a refused promote must not change anything"


# -- (a) fingerprint must match the target environment's code ------------------------------------


def test_promote_refuses_release_built_from_other_code() -> None:
    harness = _harness()
    expected = json.dumps({"orders": "fp1", "events": "fp_other"})
    _assert_refused(harness, r"fingerprint differs from the prod code.*events \(release fp2, prod code fp_other\)",
                    rows=_release_rows(), expected_fps_json=expected)


def test_promote_refuses_release_row_that_is_not_in_the_target_code() -> None:
    harness = _harness()
    _assert_refused(harness, r"events \(fp2, not a virtual model of this project\)",
                    rows=_release_rows(), expected_fps_json=json.dumps({"orders": "fp1"}))


def test_promote_with_matching_fingerprints_works_and_the_check_is_optional() -> None:
    harness = _harness()
    _promote(harness, _release_rows(), expected_fps_json=json.dumps({"orders": "fp1", "events": "fp2", "extra": "x"}))
    assert any("CREATE OR REPLACE VIEW `test_proj`.`analytics`.`orders`" in q for q in harness.executed_queries)
    manual = _harness()
    _promote(manual, _release_rows())  # no expected_fps_json: manual use
    assert any("`analytics`.`events`" in q for q in manual.executed_queries)


# -- (b) partial builds are marked and refused ---------------------------------------------------


def _sync_sql(harness: MacroTestHarness, **vars_) -> str:
    harness.vars_dict.update({"_dbt_virtual_fps": {"orders": "fp1"}, "_dbt_virtual_release": "r1", **vars_})
    harness.model = {"name": "orders", "config": {"materialized": "table", "virtual_env": True}}
    harness.this = FakeRelation("test_proj", "analytics_physical", "orders__fp1")
    return harness.call_macro("sync_env_view")


def test_sync_env_view_marks_full_builds_full() -> None:
    sql = _sync_sql(_harness())
    assert "SET build_quality = 'full'" in sql
    assert "WHERE environment = 'staging' AND model_name = 'orders'" in sql
    assert "WHERE release_id = 'r1' AND model_name = 'orders'" in sql


def test_sync_env_view_marks_windowed_builds_partial() -> None:
    harness = _harness(partial_build_vars=["backfill_start", "ci_core_window_start"])
    assert "build_quality = 'partial'" in _sync_sql(harness, ci_core_window_start="2026-11-01")
    # a declared var that is not set is a full build
    assert "build_quality = 'full'" in _sync_sql(_harness(partial_build_vars=["backfill_start"]))


def test_sync_env_view_marks_empty_and_explicit_partial_builds() -> None:
    harness = _harness()
    harness.flags = type("Flags", (), {"FULL_REFRESH": False, "EMPTY": True})()
    assert "build_quality = 'partial'" in _sync_sql(harness)
    assert "build_quality = 'partial'" in _sync_sql(_harness(), _dbt_virtual_partial=True)


def test_promote_refuses_partial_rows() -> None:
    harness = _harness()
    rows = _release_rows()
    rows[1] = (*rows[1][:7], "partial", rows[1][8])
    _assert_refused(harness, r"built partially.*: events", rows=rows)
    # the same applies to a promote from an environment (no release): quality is the 8th column
    env_harness = _harness()
    env_harness.query_responses.append(FakeResult([("events", "model", "fp2", "analytics_physical.events__fp2", None, 2, None, "partial")]))
    with pytest.raises(RuntimeError, match="built partially"):
        env_harness.call_macro("virtual_promote", from_env="staging", to_env="prod", release_id="")


def test_promote_resets_a_stale_partial_flag_of_the_target_pointer() -> None:
    harness = _harness()
    _promote(harness, _release_rows())
    reset = [q for q in harness.executed_queries if "SET build_quality = 'full'" in q]
    assert len(reset) == 1 and "model_name IN ('events', 'orders')" in reset[0]


def test_export_state_lists_partial_physical_tables_by_latest_registration() -> None:
    harness = _harness()
    for registry in ("_environments", "_releases"):
        harness.existing_relations.add((harness.target["database"], "analytics_physical", registry))
    harness.query_responses.extend(
        [
            FakeResult([]),  # env pointers
            FakeResult([]),  # existing tables
            FakeResult([]),  # registered tables
            FakeResult(
                [
                    ("analytics_physical.orders__fp1", "partial", "t1"),
                    ("analytics_physical.events__fp2", "partial", "t1"),
                    ("analytics_physical.events__fp2", "full", "t2"),  # rebuilt in full later
                ]
            ),
        ]
    )
    harness.call_macro("virtual_export_state", env="staging", base_env="prod")
    state = json.loads(next(m for m in harness.logs if "DBT_VIRTUAL_STATE_JSON:" in m).split(":", 1)[1])
    assert state["partial_physical_tables"] == ["orders__fp1"]


def _node(name: str, sql: str) -> dict:
    return {
        "unique_id": f"model.p.{name}",
        "name": name,
        "resource_type": "model",
        "compiled_code": sql,
        "config": {"materialized": "table", "virtual_env": True},
        "depends_on": {"nodes": []},
        "package_name": "p",
    }


def test_planner_never_reuses_a_partial_table() -> None:
    fps = compute_manifest_fingerprints({"nodes": {"model.p.events": _node("events", "select 1 as id")}})
    phys = fps["model.p.events"].physical_identifier
    kwargs = {"existing_physical_tables": {phys}, "registered_physical_tables": {phys}}
    assert classify_plan_actions(fps, **kwargs)["model.p.events"].action == "REUSE"
    assert classify_plan_actions(fps, **kwargs, partial_physical_tables={phys})["model.p.events"].action == "FULL_BUILD"
    # state pointers alone (no catalog snapshot) count as evidence the table exists: still not reusable
    pointers = classify_plan_actions(
        fps, env_state={"events": fps["model.p.events"].fingerprint}, partial_physical_tables={phys}
    )
    assert pointers["model.p.events"].action == "FULL_BUILD"


def test_planner_does_not_seed_an_incremental_from_a_partial_base_table() -> None:
    node = _node("events", "select 1 as id")
    node["config"] = {"materialized": "incremental", "virtual_env": True}
    fps = compute_manifest_fingerprints({"nodes": {"model.p.events": node}})
    base = {"events": "basefp"}
    tables = {"events__basefp"}
    seeded = classify_plan_actions(fps, base_env_state=base, existing_physical_tables=tables)
    assert seeded["model.p.events"].action == "SEED_INCREMENTAL"
    partial = classify_plan_actions(
        fps, base_env_state=base, existing_physical_tables=tables, partial_physical_tables=tables
    )
    assert partial["model.p.events"].action == "FULL_BUILD"


# -- (c) sealed releases -------------------------------------------------------------------------


def test_snapshot_release_seals_the_release() -> None:
    harness = _harness()
    harness.query_responses.extend(
        [
            FakeResult([]),  # models already in the release
            FakeResult([("orders", "model", "fp1", "analytics_physical.orders__fp1", None, None)]),
        ]
    )
    harness.call_macro(
        "virtual_snapshot_release",
        env="staging",
        release_id="r1",
        models_json=json.dumps({"orders": {"materialized": "table", "fingerprint": "fp1"}}),
    )
    insert = next(q for q in harness.executed_queries if q.startswith("INSERT INTO"))
    assert "build_quality" in insert and "NULL, 'full')" in insert
    assert harness.executed_queries[-1].startswith("UPDATE `test_proj`.`analytics_physical`._releases")
    assert "SET sealed_at =" in harness.executed_queries[-1]
    assert "release_id = 'r1' AND sealed_at IS NULL" in harness.executed_queries[-1]


def test_promote_refuses_an_unsealed_release_unless_allowed() -> None:
    unsealed = _release_rows(sealed=None)
    _assert_refused(_harness(), r"release r1 is not sealed", rows=unsealed)
    allowed = _harness()
    _promote(allowed, unsealed, allow_unsealed=True)
    assert any("`analytics`.`orders`" in q for q in allowed.executed_queries)
    sealed = _harness()
    _promote(sealed, _release_rows())
    assert any("`analytics`.`orders`" in q for q in sealed.executed_queries)


def test_legacy_releases_are_refused_only_when_require_sealed() -> None:
    legacy = _release_rows(quality=None, sealed=None)
    _assert_refused(_harness(), r"has no seal.*virtual_seal_release", rows=legacy, require_sealed=True)
    relaxed = _harness()
    _promote(relaxed, legacy, require_sealed=False)
    assert any("`analytics`.`orders`" in q for q in relaxed.executed_queries)
    # a legacy-shaped row (6 columns, as older callers and registries return) behaves the same
    short = _harness()
    _promote(short, [row[:6] for row in _release_rows()])
    assert any("`analytics`.`orders`" in q for q in short.executed_queries)


def test_refused_promote_releases_the_lease_lock() -> None:
    harness = _harness()
    harness.query_responses.extend(
        [FakeResult([]), FakeResult([("cli@host", "2999-01-01T00:00:00Z")])]
    )
    harness.query_responses.append(FakeResult(_release_rows(sealed=None)))
    with pytest.raises(RuntimeError, match="not sealed"):
        harness.call_macro(
            "virtual_promote", from_env="staging", to_env="prod", release_id="r1", lock=True, lock_owner="cli@host"
        )
    assert harness.executed_queries[-1].startswith("DELETE FROM `test_proj`.`analytics_physical`._locks")


def test_seal_release_operation() -> None:
    harness = _harness()
    harness.call_macro("virtual_seal_release", release_id="v_old")
    assert "SET sealed_at =" in harness.executed_queries[-1]
    assert "release_id = 'v_old'" in harness.executed_queries[-1]


# -- (d) partition coverage audit ----------------------------------------------------------------


def _audit(harness: MacroTestHarness, coverage_ids: list[str], **kwargs) -> None:
    harness.query_responses.extend(
        [
            FakeResult([("events", "analytics_physical.events__fp2")]),  # candidates
            FakeResult([("events", "analytics_physical.events__fp1")]),  # current target
            FakeResult([(100,)]),  # candidate rows
            FakeResult([(100,)]),  # target rows
            FakeResult([(pid,) for pid in coverage_ids]),  # partitions the candidate lacks
        ]
    )
    harness.call_macro("virtual_audit", from_env="staging", to_env="prod", **kwargs)


def test_audit_fails_when_candidate_lacks_target_partitions() -> None:
    harness = _harness()
    with pytest.raises(RuntimeError) as exc:
        _audit(harness, ["20261101", "20261102", "20261103", "20261105"])
    message = str(exc.value)
    assert "lacks 4 partition(s) that prod has: 20261101..20261103 (3), 20261105" in message
    sql = harness.executed_queries[-1]
    assert "`test_proj`.`analytics_physical`.INFORMATION_SCHEMA.PARTITIONS" in sql
    assert "table_name = 'events__fp1'" in sql and "table_name = 'events__fp2'" in sql
    assert "'__NULL__', '__UNPARTITIONED__'" in sql


def test_audit_ignores_partitions_before_coverage_ignore_before() -> None:
    _audit(_harness(), ["20200101", "20200102", "202010"], coverage_ignore_before="2026-01-01")
    configured = _harness(coverage_ignore_before="2026-11-02")
    with pytest.raises(RuntimeError, match=r"lacks 1 partition\(s\).*20261103.*partitions before 2026-11-02 ignored"):
        _audit(configured, ["20261101", "20261103"])


def test_audit_coverage_is_skipped_outside_bigquery_and_when_disabled() -> None:
    pg = MacroTestHarness(
        adapter_type="postgres",
        vars_dict={"dbt_virtual": {"physical_schema": "analytics_physical"}},
    )
    pg.query_responses.extend(
        [
            FakeResult([("events", "analytics_physical.events__fp2")]),
            FakeResult([("events", "analytics_physical.events__fp1")]),
            FakeResult([(100,)]),
            FakeResult([(100,)]),
        ]
    )
    pg.call_macro("virtual_audit", from_env="staging", to_env="prod")
    assert len(pg.executed_queries) == 4
    assert any("partition coverage check skipped" in m for m in pg.logs)

    bq = _harness()
    _audit(bq, ["20261101"], check_coverage=False)
    assert not any("INFORMATION_SCHEMA.PARTITIONS" in q for q in bq.executed_queries)


def test_promote_audit_runs_the_coverage_check() -> None:
    harness = _harness()
    harness.query_responses.extend(
        [
            FakeResult([("events", "analytics_physical.events__fp2")]),
            FakeResult([("events", "analytics_physical.events__fp1")]),
            FakeResult([(100,)]),
            FakeResult([(100,)]),
            FakeResult([("20261101",)]),
        ]
    )
    with pytest.raises(RuntimeError, match="lacks 1 partition"):
        harness.call_macro(
            "virtual_promote", from_env="staging", to_env="prod", release_id="r1", audit=True,
            coverage_ignore_before="2026-01-01",
        )


# -- CLI -----------------------------------------------------------------------------------------


def _manifest(tmp_path: Path) -> None:
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text(
        json.dumps({"nodes": {"model.p.events": _node("events", "select 1 as id")}}), encoding="utf-8"
    )


def _promote_cli(tmp_path: Path, *args: str) -> list[list[str]]:
    calls: list[list[str]] = []

    def fake_run_dbt(cmd: list[str], project_dir: Path) -> int:
        calls.append(cmd)
        return 0

    with patch("dbt_virtual.cli._run_dbt", side_effect=fake_run_dbt):
        assert main(["--project-dir", str(tmp_path), "promote", "--release", "r1", *args]) == 0
    return calls


def _operation_args(calls: list[list[str]]) -> dict:
    op = next(c for c in calls if c[:2] == ["run-operation", "virtual_promote"])
    return json.loads(op[op.index("--args") + 1])


def test_cli_promote_passes_the_target_code_fingerprints_by_default(tmp_path: Path) -> None:
    _manifest(tmp_path)
    calls = _promote_cli(tmp_path)
    assert calls[0] == ["compile", "--target", "prod"]
    args = _operation_args(calls)
    expected = json.loads(args["expected_fps_json"])
    fps = compute_manifest_fingerprints(json.loads((tmp_path / "target" / "manifest.json").read_text()))
    assert expected == {"events": fps["model.p.events"].fingerprint}
    assert args["audit"] is True  # --to is the base environment
    assert args["require_sealed"] is True and args["allow_unsealed"] is False
    assert args["check_coverage"] is True


def test_cli_promote_options(tmp_path: Path) -> None:
    _manifest(tmp_path)
    skipped = _promote_cli(tmp_path, "--skip-fingerprint-check", "--to", "prod")
    assert [c[0] for c in skipped] == ["run-operation"]
    assert "expected_fps_json" not in _operation_args(skipped)

    staging = _promote_cli(tmp_path, "--to", "pr_9", "--compile-target", "staging", "--no-require-sealed")
    assert staging[0] == ["compile", "--target", "staging"]
    args = _operation_args(staging)
    assert args["audit"] is False and args["require_sealed"] is False

    forced = _promote_cli(
        tmp_path, "--skip-compile", "--no-audit", "--allow-unsealed", "--coverage-ignore-before", "2026-01-01"
    )
    args = _operation_args(forced)
    assert args["audit"] is False and args["allow_unsealed"] is True
    assert args["coverage_ignore_before"] == "2026-01-01"


def test_cli_promote_audits_by_default_only_for_the_configured_base_environment(tmp_path: Path) -> None:
    _manifest(tmp_path)
    (tmp_path / "dbt_project.yml").write_text("name: p\nvars:\n  dbt_virtual:\n    base_environment: main\n", encoding="utf-8")
    assert _operation_args(_promote_cli(tmp_path, "--skip-compile", "--to", "main"))["audit"] is True
    assert _operation_args(_promote_cli(tmp_path, "--skip-compile", "--to", "prod"))["audit"] is False
