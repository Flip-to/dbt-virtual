"""Merkle-DAG fingerprinting and canonical SQL normalization (with sqlglot AST) for dbt-virtual."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

FINGERPRINT_SCHEMA_VERSION = 1
DEFAULT_FINGERPRINT_LENGTH = 12

_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_WHITESPACE_RUN_RE = re.compile(r"[ \t]+")
_FINGERPRINT_SUFFIX_RE = re.compile(r"__[0-9a-f]{8,64}\b")


@dataclass(frozen=True)
class NodeFingerprint:
    """Computed fingerprint metadata for a single virtualized dbt node."""

    unique_id: str
    name: str
    resource_type: str
    materialized: str
    fingerprint: str
    physical_identifier: str
    upstream_fingerprints: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "unique_id": self.unique_id,
            "name": self.name,
            "resource_type": self.resource_type,
            "materialized": self.materialized,
            "fingerprint": self.fingerprint,
            "physical_identifier": self.physical_identifier,
            "upstream_fingerprints": dict(sorted(self.upstream_fingerprints.items())),
        }


def is_virtual_node(node: dict[str, Any]) -> bool:
    """Return True if a manifest node opts into dbt-virtual (`virtual_env: true`)."""
    if node.get("resource_type") not in {"model", "function", "operation"}:
        return False
    config = node.get("config") or {}
    if config.get("virtual_env") is True:
        return True
    extra = config.get("extra") or {}
    if extra.get("virtual_env") is True:
        return True
    meta = config.get("meta") or {}
    return meta.get("virtual_env") is True


def canonicalize_sql_ast(sql: str, dialect: str = "bigquery") -> str:
    """Canonicalize SQL via sqlglot AST (strips comments, normalizes keyword case, quoting, and whitespace).

    Falls back to the input string if the snippet contains raw Jinja (`{{` or `{%`) or procedural
    statements not parseable as a single expression.
    """
    if not sql or "{{" in sql or "{%" in sql:
        return sql
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
        return parsed.sql(dialect=dialect, comments=False, normalize_functions="upper")
    except (SqlglotError, ValueError, TypeError, KeyError, IndexError):
        return sql


def extract_sql_output_columns(sql: str, dialect: str = "bigquery") -> dict[str, str]:
    """Extract top-level projected column names (and inferred/cast types when explicit) from compiled SQL."""
    if not sql or "{{" in sql or "{%" in sql:
        return {}
    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
        select_expr = parsed.find(exp.Select)
        if select_expr is None:
            return {}
        cols: dict[str, str] = {}
        for proj in select_expr.expressions:
            if isinstance(proj, exp.Star):
                continue
            col_name = proj.alias_or_name
            if not col_name:
                continue
            col_type = ""
            unwrapped = proj.this if isinstance(proj, exp.Alias) else proj
            if isinstance(unwrapped, exp.Cast) and unwrapped.to:
                col_type = unwrapped.to.sql(dialect=dialect)
            cols[col_name] = col_type
        return cols
    except (SqlglotError, ValueError, TypeError, KeyError, IndexError):
        return {}


def normalize_sql(
    sql: str,
    *,
    self_identifier: str | None = None,
    known_schemas: list[str] | None = None,
    use_ast: bool = True,
    dialect: str = "bigquery",
) -> str:
    """Normalize compiled SQL so target-specific schemas, comments, and formatting do not alter the hash."""
    if not sql:
        return ""

    normalized = sql.replace("\r\n", "\n").replace("\r", "\n")
    normalized = _BLOCK_COMMENT_RE.sub("", normalized)
    normalized = _LINE_COMMENT_RE.sub("", normalized)

    # Strip any existing __<fp> suffixes from physical table names
    normalized = _FINGERPRINT_SUFFIX_RE.sub("", normalized)

    # Replace known environment / physical schemas with a canonical schema token
    if known_schemas:
        for schema in sorted({s for s in known_schemas if s}, key=len, reverse=True):
            pattern = re.compile(rf"(`?)\b{re.escape(schema)}\b\1\.")
            normalized = pattern.sub("__DBT_VIRTUAL_SCHEMA__.", normalized)

    # Replace self-identifier with canonical token
    if self_identifier:
        base_ident = _FINGERPRINT_SUFFIX_RE.sub("", self_identifier)
        ident_pattern = re.compile(rf"\b{re.escape(base_ident)}\b")
        normalized = ident_pattern.sub("__DBT_VIRTUAL_THIS__", normalized)

    if use_ast:
        normalized = canonicalize_sql_ast(normalized, dialect=dialect)

    lines = [_WHITESPACE_RUN_RE.sub(" ", line).strip() for line in normalized.split("\n")]
    return " ".join(line for line in lines if line)


def extract_relevant_config(node: dict[str, Any]) -> dict[str, Any]:
    """Extract the subset of node configuration that affects physical table or routine shape."""
    config = node.get("config") or {}
    extra = config.get("extra") or {}

    def _get(key: str, default: Any = None) -> Any:
        if key in config and config[key] is not None:
            return config[key]
        return extra.get(key, default)

    cluster_by = _get("cluster_by")
    if isinstance(cluster_by, list):
        cluster_by = list(cluster_by)

    return {
        "materialized": _get("materialized", "view"),
        "incremental_strategy": _get("incremental_strategy"),
        "unique_key": _get("unique_key"),
        "partition_by": _get("partition_by"),
        "cluster_by": cluster_by,
        "on_schema_change": _get("on_schema_change"),
        "parameters": _get("parameters"),
        "returns": _get("returns"),
        "virtual_env_epoch": _get("virtual_env_epoch", 1),
        "forward_only": bool(_get("forward_only", False)),
        "physical_version": _get("physical_version"),
    }


def compute_node_fingerprint(
    node: dict[str, Any],
    *,
    upstream_fingerprints: dict[str, str] | None = None,
    known_schemas: list[str] | None = None,
    fp_length: int = DEFAULT_FINGERPRINT_LENGTH,
) -> NodeFingerprint:
    """Compute the deterministic Merkle-DAG fingerprint for a single dbt manifest node."""
    unique_id = node["unique_id"]
    name = node["name"]
    resource_type = node.get("resource_type", "model")
    config_subset = extract_relevant_config(node)
    materialized = str(config_subset["materialized"])
    alias = node.get("alias") or name
    generated_alias = re.fullmatch(rf"{re.escape(name)}__[0-9a-f]{{8,64}}", alias)
    pinned_alias = (
        f"{name}__{config_subset['physical_version']}"
        if config_subset.get("physical_version") is not None
        else None
    )
    if (
        is_virtual_node(node)
        and materialized in {"table", "incremental"}
        and alias != name
        and generated_alias is None
        and alias != pinned_alias
    ):
        raise ValueError(
            f"Virtual table model '{name}' uses alias '{alias}'. "
            "dbt-virtual currently requires alias to match the model name."
        )

    compiled_sql = node.get("compiled_code") or node.get("compiled_sql") or ""
    raw_sql = node.get("raw_code") or node.get("raw_sql") or ""

    schemas_to_strip = list(known_schemas or [])
    if node.get("schema"):
        schemas_to_strip.append(str(node["schema"]))

    norm_compiled = normalize_sql(
        compiled_sql,
        self_identifier=node.get("alias") or name,
        known_schemas=schemas_to_strip,
    )
    norm_raw = normalize_sql(
        raw_sql,
        self_identifier=name,
        known_schemas=schemas_to_strip,
        use_ast=False,
    )

    sorted_upstreams = dict(sorted((upstream_fingerprints or {}).items()))

    payload = {
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "resource_type": resource_type,
        "name": name,
        "config": config_subset,
        "normalized_compiled_sql": norm_compiled,
        "normalized_raw_sql": norm_raw,
        "upstream_fingerprints": sorted_upstreams,
    }

    canonical_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(canonical_bytes).hexdigest()[:fp_length]
    if config_subset.get("physical_version"):
        digest = str(config_subset["physical_version"])

    is_routine = materialized in {"table_function", "function", "procedure"}
    physical_identifier = name if is_routine else f"{name}__{digest}"

    return NodeFingerprint(
        unique_id=unique_id,
        name=name,
        resource_type=resource_type,
        materialized=materialized,
        fingerprint=digest,
        physical_identifier=physical_identifier,
        upstream_fingerprints=sorted_upstreams,
    )


def compute_manifest_fingerprints(
    manifest: dict[str, Any],
    *,
    known_schemas: list[str] | None = None,
    fp_length: int = DEFAULT_FINGERPRINT_LENGTH,
) -> dict[str, NodeFingerprint]:
    """Compute Merkle-DAG fingerprints for all virtual_env nodes in a dbt manifest."""
    nodes: dict[str, dict[str, Any]] = manifest.get("nodes") or {}
    virtual_nodes = {uid: n for uid, n in nodes.items() if is_virtual_node(n)}
    virtual_name_uids: dict[str, str] = {}
    for uid, node in virtual_nodes.items():
        name = str(node.get("name", ""))
        previous_uid = virtual_name_uids.get(name)
        if previous_uid is not None:
            raise ValueError(
                f"Duplicate virtual model name '{name}' in manifest nodes "
                f"'{previous_uid}' and '{uid}'. dbt-virtual requires unique names "
                "because environment state and physical identifiers are keyed by model name."
            )
        virtual_name_uids[name] = uid

    computed: dict[str, NodeFingerprint] = {}
    visiting: set[str] = set()

    def _resolve(uid: str) -> NodeFingerprint:
        if uid in computed:
            return computed[uid]
        if uid in visiting:
            raise ValueError(f"Cycle detected in dbt-virtual dependency graph at {uid}")
        visiting.add(uid)

        node = virtual_nodes[uid]
        depends_on = (node.get("depends_on") or {}).get("nodes") or []
        upstream_fps: dict[str, str] = {}
        for parent_uid in depends_on:
            if parent_uid in virtual_nodes:
                parent_fp = _resolve(parent_uid)
                upstream_fps[parent_uid] = parent_fp.fingerprint

        fp_obj = compute_node_fingerprint(
            node,
            upstream_fingerprints=upstream_fps,
            known_schemas=known_schemas,
            fp_length=fp_length,
        )
        visiting.remove(uid)
        computed[uid] = fp_obj
        return fp_obj

    for uid in sorted(virtual_nodes):
        _resolve(uid)

    return computed


def write_fingerprints_file(
    fingerprints: dict[str, NodeFingerprint],
    output_path: Path,
) -> None:
    """Write computed fingerprints to `.dbt_virtual/fingerprints.json`."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nodes": {uid: fp.to_dict() for uid, fp in sorted(fingerprints.items())},
        "by_name": {fp.name: fp.fingerprint for fp in sorted(fingerprints.values(), key=lambda x: x.name)},
    }
    output_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
