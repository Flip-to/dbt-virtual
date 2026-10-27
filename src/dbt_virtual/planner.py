"""Plan classification, warehouse state diffing, and schema compatibility checks for dbt-virtual.

Replicates SQLMesh's core plan/diff engine on top of dbt manifests and warehouse state:
1. Classifies every virtualized node into:
   - `REUSE`            — identical fingerprint already active or present in `<physical_schema>` (0 compute)
   - `SEED_INCREMENTAL` — new fingerprint for an incremental model with a `prod` base table to `CLONE`
   - `FULL_BUILD`       — new table model or incremental model with no base table
   - `ROUTINE_DEPLOY`   — new/modified UDF, TVF, or stored procedure
2. Performs column-level schema compatibility analysis (`NON_BREAKING` vs `BREAKING`) between
   environments or manifest versions to warn before promotion.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dbt_virtual.fingerprint import NodeFingerprint, extract_sql_output_columns

ROUTINE_TYPES = {"table_function", "function", "procedure"}


@dataclass(frozen=True)
class NodePlanAction:
    """Planned execution action for a single virtualized node."""

    unique_id: str
    name: str
    resource_type: str
    materialized: str
    fingerprint: str
    physical_identifier: str
    action: str  # REUSE | SEED_INCREMENTAL | FULL_BUILD | ROUTINE_DEPLOY
    reason: str
    base_fingerprint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "unique_id": self.unique_id,
            "name": self.name,
            "resource_type": self.resource_type,
            "materialized": self.materialized,
            "fingerprint": self.fingerprint,
            "physical_identifier": self.physical_identifier,
            "action": self.action,
            "reason": self.reason,
            "base_fingerprint": self.base_fingerprint,
        }


@dataclass(frozen=True)
class ColumnChange:
    """A single column addition, removal, or type change on a model."""

    column: str
    change_type: str  # ADDED | DROPPED | TYPE_CHANGED
    old_type: str | None = None
    new_type: str | None = None

    @property
    def is_breaking(self) -> bool:
        return self.change_type in {"DROPPED", "TYPE_CHANGED"}

    def to_dict(self) -> dict[str, Any]:
        return {
            "column": self.column,
            "change_type": self.change_type,
            "old_type": self.old_type,
            "new_type": self.new_type,
            "is_breaking": self.is_breaking,
        }


@dataclass(frozen=True)
class ModelSchemaDiff:
    """Schema compatibility report for a single model between two environments/states."""

    model_name: str
    changes: list[ColumnChange] = field(default_factory=list)

    @property
    def classification(self) -> str:
        if not self.changes:
            return "IDENTICAL"
        if any(c.is_breaking for c in self.changes):
            return "BREAKING"
        return "NON_BREAKING"

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "classification": self.classification,
            "changes": [c.to_dict() for c in self.changes],
        }


def classify_plan_actions(
    fingerprints: dict[str, NodeFingerprint],
    *,
    env_state: dict[str, str] | None = None,
    base_env_state: dict[str, str] | None = None,
    existing_physical_tables: set[str] | None = None,
    full_refresh: bool = False,
) -> dict[str, NodePlanAction]:
    """Classify each virtualized node into REUSE, SEED_INCREMENTAL, FULL_BUILD, or ROUTINE_DEPLOY.

    Parameters
    ----------
    fingerprints:
        Mapping of `unique_id -> NodeFingerprint` from `compute_manifest_fingerprints`.
    env_state:
        Optional mapping of `model_name -> active_fingerprint` in the target environment (`_environments`).
    base_env_state:
        Optional mapping of `model_name -> active_fingerprint` in `base_environment` (`prod`).
    existing_physical_tables:
        Optional set of physical table names (`<model>__<fp>`) already materialized in `<physical_schema>`.
    full_refresh:
        When True, forces `FULL_BUILD` on all table/incremental models.
    """
    env_map = env_state or {}
    base_map = base_env_state or {}
    phys_tables = set(existing_physical_tables or set())

    # Any fingerprint active in env_state or base_env_state implies its physical table exists
    for model_name, fp in (*env_map.items(), *base_map.items()):
        phys_tables.add(f"{model_name}__{fp}")

    actions: dict[str, NodePlanAction] = {}
    for uid, fp_obj in sorted(fingerprints.items()):
        base_fp = base_map.get(fp_obj.name) or env_map.get(fp_obj.name)
        mat = fp_obj.materialized

        if mat in ROUTINE_TYPES:
            if not full_refresh and env_map.get(fp_obj.name) == fp_obj.fingerprint:
                action = "REUSE"
                reason = "Routine fingerprint already active in target environment"
            else:
                action = "ROUTINE_DEPLOY"
                reason = (
                    "Routine SQL or upstream signature changed"
                    if base_fp
                    else "New virtualized routine"
                )
        elif not full_refresh and fp_obj.physical_identifier in phys_tables:
            action = "REUSE"
            reason = f"Physical table {fp_obj.physical_identifier} already exists in warehouse (0 compute)"
        elif not full_refresh and mat == "incremental" and fp_obj.name in base_map:
            action = "SEED_INCREMENTAL"
            reason = (
                f"Zero-copy CLONE from base fingerprint {base_map[fp_obj.name]} + incremental delta"
            )
        else:
            action = "FULL_BUILD"
            reason = (
                "Full refresh requested"
                if full_refresh
                else f"Build physical table {fp_obj.physical_identifier}"
            )

        actions[uid] = NodePlanAction(
            unique_id=uid,
            name=fp_obj.name,
            resource_type=fp_obj.resource_type,
            materialized=mat,
            fingerprint=fp_obj.fingerprint,
            physical_identifier=fp_obj.physical_identifier,
            action=action,
            reason=reason,
            base_fingerprint=base_fp,
        )

    return actions


def compute_dbt_select_for_build(actions: dict[str, NodePlanAction]) -> tuple[list[str], dict[str, str]]:
    """Split planned actions into:
    1. `models_to_build`: model names requiring `dbt build` (`SEED_INCREMENTAL`, `FULL_BUILD`, `ROUTINE_DEPLOY`)
    2. `reused_models`: mapping of `model_name -> fingerprint` for `REUSE` table/incremental models
       that only need a zero-compute view pointer sync.
    """
    models_to_build: list[str] = []
    reused_models: dict[str, str] = {}

    for act in actions.values():
        if act.action == "REUSE":
            if act.materialized in {"table", "incremental"}:
                reused_models[act.name] = act.fingerprint
        else:
            models_to_build.append(act.name)

    return sorted(models_to_build), reused_models


def diff_model_columns(
    model_name: str,
    old_columns: dict[str, str],
    new_columns: dict[str, str],
) -> ModelSchemaDiff:
    """Compare column `name -> data_type` mappings and classify BREAKING vs NON_BREAKING changes."""
    norm_old = {k.lower(): (k, (v or "").upper().strip()) for k, v in old_columns.items()}
    norm_new = {k.lower(): (k, (v or "").upper().strip()) for k, v in new_columns.items()}

    changes: list[ColumnChange] = []

    for col_key, (orig_name, old_type) in sorted(norm_old.items()):
        if col_key not in norm_new:
            changes.append(ColumnChange(column=orig_name, change_type="DROPPED", old_type=old_type))
        else:
            _, new_type = norm_new[col_key]
            if old_type and new_type and old_type != new_type:
                changes.append(
                    ColumnChange(
                        column=orig_name,
                        change_type="TYPE_CHANGED",
                        old_type=old_type,
                        new_type=new_type,
                    )
                )

    for col_key, (orig_name, new_type) in sorted(norm_new.items()):
        if col_key not in norm_old:
            changes.append(ColumnChange(column=orig_name, change_type="ADDED", new_type=new_type))

    return ModelSchemaDiff(model_name=model_name, changes=changes)


def diff_manifests(
    old_manifest: dict[str, Any],
    new_manifest: dict[str, Any],
    *,
    dialect: str = "bigquery",
) -> list[ModelSchemaDiff]:
    """Compare declared or SQL-AST-projected column schemas across two dbt `manifest.json` payloads."""
    old_nodes = old_manifest.get("nodes") or {}
    new_nodes = new_manifest.get("nodes") or {}

    def _extract_cols(node: dict[str, Any]) -> dict[str, str]:
        declared = {
            col_name: (col_meta.get("data_type") or "")
            for col_name, col_meta in (node.get("columns") or {}).items()
        }
        if declared:
            return declared
        compiled = node.get("compiled_code") or node.get("compiled_sql") or ""
        return extract_sql_output_columns(compiled, dialect=dialect)

    diffs: list[ModelSchemaDiff] = []
    for uid, new_node in sorted(new_nodes.items()):
        if uid not in old_nodes:
            continue
        old_node = old_nodes[uid]
        old_cols = _extract_cols(old_node)
        new_cols = _extract_cols(new_node)
        if old_cols or new_cols:
            diff = diff_model_columns(new_node.get("name", uid), old_cols, new_cols)
            if diff.changes:
                diffs.append(diff)
    return diffs
