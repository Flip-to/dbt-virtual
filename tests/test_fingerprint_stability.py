"""Same code, two environments, one fingerprint (flipto-dbt pilot: 4 of 4 objects differed)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dbt_virtual.cli import main
from dbt_virtual.fingerprint import (
    MissingCompiledCodeError,
    compute_manifest_fingerprints,
    explain_fingerprint,
)

CHILD = "model.p.child"
PARENT = "model.p.parent"
SOURCE = "source.p.raw.events"


def _manifest(
    *,
    child_sql: str = "select * from `proj`.`rt_prod`.`parent`",
    child_raw: str = "select * from {{ ref('parent') }}",
    child_compiled: str | None = None,
    parent_compiled: str = "select 1 as id",
    parent_raw: str = "{% if is_incremental() %}select 2 as id{% else %}select {{ helper() }} as id{% endif %}",
    parent_macro_sql: str = "{% macro helper() %}1{% endmacro %}",
    parent_virtual: bool = False,
    child_this_sql: str | None = None,
    source: dict | None = None,
    child_materialized: str = "table",
) -> dict:
    child_node = {
        "unique_id": CHILD,
        "name": "child",
        "alias": "child",
        "resource_type": "model",
        "schema": "rt_prod",
        "compiled_code": child_compiled if child_compiled is not None else (child_this_sql or child_sql),
        "raw_code": child_raw,
        "config": {"materialized": child_materialized, "virtual_env": True},
        "depends_on": {"nodes": [PARENT] + ([SOURCE] if source else []), "macros": []},
    }
    parent_node = {
        "unique_id": PARENT,
        "name": "parent",
        "alias": "parent",
        "resource_type": "model",
        "schema": "rt_prod",
        "compiled_code": parent_compiled,
        "raw_code": parent_raw,
        "config": {"materialized": "incremental", "virtual_env": parent_virtual},
        "depends_on": {"nodes": [], "macros": ["macro.p.helper"]},
    }
    return {
        "metadata": {"adapter_type": "bigquery"},
        "nodes": {CHILD: child_node, PARENT: parent_node},
        "sources": {SOURCE: source} if source else {},
        "macros": {
            "macro.p.helper": {"macro_sql": parent_macro_sql, "depends_on": {"macros": []}},
            "macro.dbt.is_incremental": {"macro_sql": "dbt internals", "depends_on": {"macros": []}},
        },
    }


def _fp(manifest: dict, **kwargs) -> str:
    return compute_manifest_fingerprints(manifest, **kwargs)[CHILD].fingerprint


def test_self_reference_location_does_not_change_fingerprint() -> None:
    """`{{ this }}` is db.schema.alias: prod dataset in prod, the physical dataset in a sandbox."""
    prod = _manifest(child_compiled="select max(ts) from `proj`.`rt_prod`.`child__aaaaaaaaaaaa`")
    sandbox = _manifest(child_compiled="select max(ts) from `other-proj`.`sandbox_virtual`.`child__bbbbbbbbbbbb`")
    assert _fp(prod) == _fp(sandbox)


def test_deferred_parent_location_does_not_change_fingerprint() -> None:
    """Deferral points an unselected parent at prod (other project and dataset)."""
    deferred = _manifest(child_compiled="select * from `proj`.`rt_prod`.`parent`")
    local = _manifest(child_compiled="select * from `sandbox-proj`.`sandbox_rt`.`parent__0123456789ab`")
    assert _fp(deferred) == _fp(local)


def test_source_location_does_not_change_fingerprint() -> None:
    src = {"unique_id": SOURCE, "name": "events", "identifier": "events", "database": "a", "schema": "raw"}
    in_prod = _manifest(
        source=src, child_compiled="select * from `proj`.`rt_prod`.`parent`, `proj`.`raw`.`events`"
    )
    in_sandbox = _manifest(
        source=src, child_compiled="select * from `sbx`.`sbx_rt`.`parent`, `sbx`.`sbx_raw`.`events`"
    )
    assert _fp(in_prod) == _fp(in_sandbox)


def test_hard_coded_table_difference_still_changes_fingerprint() -> None:
    a = _manifest(child_compiled="select * from `proj`.`rt_prod`.`parent` join `proj`.`rt_prod`.`other`")
    b = _manifest(child_compiled="select * from `proj`.`rt_prod`.`parent` join `proj`.`rt_prod`.`another`")
    assert _fp(a) != _fp(b)


def test_target_branch_in_virtual_node_still_changes_fingerprint() -> None:
    """`{% if target.name == 'prod' %}` compiles to different SQL: a real difference, not hidden."""
    prod = _manifest(child_compiled="select * from `proj`.`rt_prod`.`parent` where true")
    dev = _manifest(child_compiled="select * from `proj`.`rt_prod`.`parent` where ts > '2024-01-01'")
    assert _fp(prod) != _fp(dev)


def test_ancestor_compiled_sql_that_depends_on_warehouse_state_is_not_hashed() -> None:
    """is_incremental() / run_started_at make an ancestor compile differently per environment."""
    prod = _manifest(parent_compiled="select 2 as id where ts > '2026-11-05 00:18:20+00:00'")
    sandbox = _manifest(parent_compiled="select 1 as id")
    assert _fp(prod) == _fp(sandbox)
    # The old rule is still available.
    assert _fp(prod, hash_compiled_ancestors=True) != _fp(sandbox, hash_compiled_ancestors=True)


def test_ancestor_raw_sql_and_macro_changes_still_propagate() -> None:
    base = _fp(_manifest())
    assert _fp(_manifest(parent_raw="select 3 as id")) != base
    assert _fp(_manifest(parent_macro_sql="{% macro helper() %}2{% endmacro %}")) != base


def test_ancestor_macro_closure_ignores_dbt_internals() -> None:
    manifest = _manifest()
    manifest["nodes"][PARENT]["depends_on"]["macros"].append("macro.dbt.is_incremental")
    base = _fp(manifest)
    manifest["macros"]["macro.dbt.is_incremental"]["macro_sql"] = "changed by a dbt upgrade"
    assert _fp(manifest) == base


def test_virtual_node_without_compiled_code_is_refused() -> None:
    manifest = _manifest()
    manifest["nodes"][CHILD].pop("compiled_code")
    with pytest.raises(MissingCompiledCodeError, match="child"):
        compute_manifest_fingerprints(manifest)
    # Plain SQL needs no compile: raw is the compiled form.
    manifest["nodes"][CHILD]["raw_code"] = "select 1"
    compute_manifest_fingerprints(manifest)


def test_plan_skip_compile_refuses_a_parsed_manifest(tmp_path: Path) -> None:
    manifest = _manifest()
    manifest["nodes"][CHILD].pop("compiled_code")
    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SystemExit) as excinfo:
        main(["--project-dir", str(tmp_path), "plan", "--skip-compile"])
    assert "no compiled SQL" in str(excinfo.value)


def test_explain_fingerprint_shows_normalized_sql_and_env_dependent_tokens(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest = _manifest(
        child_compiled=(
            "select * from `proj`.`rt_prod`.`parent` join `proj`.`other_ds`.`hardcoded` "
            "where ts > current_timestamp()"
        )
    )
    text = explain_fingerprint(manifest, "child", include_ancestors=True)
    assert "__DBT_VIRTUAL_REF__.parent" in text
    assert "hardcoded" in text and "not a dependency" in text
    assert "clock function" in text
    assert "raw_sql_and_macros" in text  # the non-virtual ancestor

    (tmp_path / "target").mkdir()
    (tmp_path / "target" / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert main(["--project-dir", str(tmp_path), "plan", "--skip-compile", "--explain-fingerprint", "child"]) == 0
    assert "normalized compiled SQL" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        main(["--project-dir", str(tmp_path), "plan", "--skip-compile", "--explain-fingerprint", "nope"])
