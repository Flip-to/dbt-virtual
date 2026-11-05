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
METADATA_ONLY_CONFIG = frozenset({"forward_only", "on_schema_change"})
# `virtual_function` is the scalar UDF materialization on dbt-core 1.11+ (core owns `function`).
ROUTINE_MATERIALIZATIONS = frozenset({"table_function", "function", "virtual_function", "procedure"})

_WHITESPACE_RUN_RE = re.compile(r"[ \t]+")
_FINGERPRINT_SUFFIX_RE = re.compile(r"__[0-9a-f]{8,64}\b")


def _strip_sql_comments_and_fp_suffixes(sql: str, dialect: str = "bigquery") -> str:
    """Strip comments and relation fingerprints in one quote-aware SQL scan."""
    out: list[str] = []
    i = 0
    quote: str | None = None
    backslash_escapes = False
    dialect_name = dialect.lower()
    backslash_escape_dialects = {"bigquery", "duckdb", "hive", "mysql", "snowflake", "spark"}
    # In these dialects `"..."` is a string literal, not a quoted identifier.
    double_quote_string_dialects = {"bigquery", "databricks", "hive", "mysql", "spark"}
    string_quotes = {"'", '"'} if dialect_name in double_quote_string_dialects else {"'"}
    while i < len(sql):
        char = sql[i]
        if quote is not None:
            if quote not in string_quotes:
                fingerprint_match = _FINGERPRINT_SUFFIX_RE.match(sql, i)
                if fingerprint_match:
                    i = fingerprint_match.end()
                    continue
            out.append(char)
            if char == quote:
                # SQL escapes quote characters by doubling them.
                if i + 1 < len(sql) and sql[i + 1] == quote:
                    out.append(sql[i + 1])
                    i += 2
                    continue
                quote = None
                backslash_escapes = False
            elif backslash_escapes and char == "\\" and i + 1 < len(sql):
                # Only dialects with backslash string escapes treat the next character specially.
                out.append(sql[i + 1])
                i += 2
                continue
            i += 1
            continue

        if char in string_quotes:
            quote = char
            previous_is_escape_prefix = (
                i > 0
                and sql[i - 1] in {"e", "E"}
                and (i < 2 or not (sql[i - 2].isalnum() or sql[i - 2] == "_"))
            )
            backslash_escapes = dialect_name in backslash_escape_dialects or previous_is_escape_prefix
            out.append(char)
            i += 1
        elif char in {'"', "`"}:
            quote = char
            out.append(char)
            i += 1
        elif sql.startswith("--", i):
            end = sql.find("\n", i)
            if end < 0:
                break
            out.append("\n")
            i = end + 1
        elif sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            if end < 0:
                # Keep malformed SQL intact; parsing/execution should report the syntax error.
                out.append(sql[i:])
                break
            out.append(" ")
            i = end + 2
        else:
            fingerprint_match = _FINGERPRINT_SUFFIX_RE.match(sql, i)
            if fingerprint_match:
                i = fingerprint_match.end()
                continue
            out.append(char)
            i += 1
    return "".join(out)


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
    # `operation` nodes are on-run-start/on-run-end hooks; they inherit `+virtual_env` from the
    # project's `models:` config but are not deployable objects.
    if node.get("resource_type") not in {"model", "function"}:
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
            if isinstance(proj, exp.Star) or (
                isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star)
            ):
                # A wildcard makes the full output schema unknown; returning a partial
                # projection set could falsely report documented columns as removed.
                return {}
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


def _strip_dependency_qualifiers(
    sql: str,
    identifiers: set[str],
    *,
    replacement: str = "__DBT_VIRTUAL_REF__.{ident}",
) -> str:
    """Replace `database.schema.<dependency>` references with `__DBT_VIRTUAL_REF__.<dependency>`.

    dbt renders every `ref()` as a three-part name whose database and schema depend on the target
    (a dev or PR dataset vs prod datasets). The dependency's own fingerprint is already part of the
    Merkle payload, so its location only made fingerprints differ between environments. Only
    three-part names are rewritten: two-part names may be `alias.column`.
    """
    for ident in sorted(identifiers, key=len, reverse=True):
        e = re.escape(ident)
        patterns = (
            rf"`[^`\n]+`\.`[^`\n]+`\.`{e}`",  # `project`.`dataset`.`ident`
            rf"`[^`.\n]+\.[^`.\n]+\.{e}`",  # `project.dataset.ident`
            rf'"[^"\n]+"\."[^"\n]+"\."{e}"',  # "database"."schema"."ident"
            rf"(?<![\w.$`\"])[A-Za-z_][\w$-]*\.[A-Za-z_][\w$-]*\.{e}(?![\w$])",  # db.schema.ident
        )
        for pattern in patterns:
            sql = re.sub(pattern, replacement.format(ident=ident), sql, flags=re.IGNORECASE)
    return sql


def normalize_sql(
    sql: str,
    *,
    self_identifier: str | None = None,
    known_schemas: list[str] | None = None,
    dependency_identifiers: set[str] | None = None,
    use_ast: bool = True,
    dialect: str = "bigquery",
) -> str:
    """Normalize compiled SQL so target-specific schemas, comments, and formatting do not alter the hash."""
    if not sql:
        return ""

    normalized = _strip_sql_comments_and_fp_suffixes(
        sql.replace("\r\n", "\n").replace("\r", "\n"),
        dialect=dialect,
    )

    if dependency_identifiers:
        normalized = _strip_dependency_qualifiers(normalized, dependency_identifiers)

    # The node's own `{{ this }}` is a three-part name too, and its database and schema differ per
    # target (prod dataset vs the environment's physical dataset), so drop the qualifier before the
    # identifier itself is tokenized below.
    if self_identifier:
        normalized = _strip_dependency_qualifiers(
            normalized,
            {_FINGERPRINT_SUFFIX_RE.sub("", self_identifier)},
            replacement="__DBT_VIRTUAL_THIS__",
        )

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


_OPTIONAL_SHAPE_KEYS = (
    "require_partition_filter",
    "partition_expiration_days",
    "incremental_predicates",
    "predicates",
    "merge_update_columns",
    "merge_exclude_columns",
    "sql_header",
)


def _user_hook_sql(hooks: Any) -> list[str]:
    """Return the SQL of user hooks, skipping dbt-virtual's own lifecycle hooks."""
    if not hooks:
        return []
    if not isinstance(hooks, list):
        hooks = [hooks]
    sqls: list[str] = []
    for hook in hooks:
        sql = hook.get("sql", "") if isinstance(hook, dict) else str(hook)
        if sql and "dbt_virtual." not in sql:
            sqls.append(sql.strip())
    return sqls


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

    # Settings that change table contents or options. Added only when set, so fingerprints of
    # nodes that do not use them stay the same as before these keys existed.
    optional: dict[str, Any] = {}
    for key in _OPTIONAL_SHAPE_KEYS:
        value = _get(key)
        if value not in (None, "", [], {}):
            optional[key] = value
    for hook_key in ("pre-hook", "post-hook"):
        hooks = _user_hook_sql(config.get(hook_key) or config.get(hook_key.replace("-", "_")))
        if hooks:
            optional[hook_key] = hooks
    contract = _get("contract") or {}
    if isinstance(contract, dict) and contract.get("enforced"):
        # An enforced contract creates the table from the declared column types.
        optional["contract_columns"] = {
            name: (meta or {}).get("data_type")
            for name, meta in sorted((node.get("columns") or {}).items())
        }

    return {
        **optional,
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


class MissingCompiledCodeError(ValueError):
    """A virtual node's raw SQL has Jinja but the manifest holds no compiled SQL for it."""


# Macros shipped with dbt itself, the adapters and dbt-virtual change with a dbt upgrade or a
# dbt-virtual release, not with the project's SQL: they would rebuild every table.
_ENGINE_MACRO_PACKAGES = frozenset(
    {
        "dbt",
        "dbt_virtual",
        "dbt_bigquery",
        "dbt_snowflake",
        "dbt_postgres",
        "dbt_duckdb",
        "dbt_databricks",
        "dbt_spark",
        "dbt_redshift",
    }
)
_JINJA_MARKERS = ("{{", "{%")


def macro_closure_hash(node: dict[str, Any], macros: Any) -> tuple[list[str], str]:
    """Hash the macros a node calls (transitively), minus dbt, adapter and dbt-virtual macros.

    Returns `(macro unique_ids, sha256 hex)`. It stands in for the compiled SQL of ancestors that
    dbt-virtual does not build: their compiled SQL reads the warehouse (`is_incremental()`,
    `load_relation`, `run_query`) and the clock, so it differs between a fresh environment and
    prod for the same code. The raw SQL plus the code of the macros it calls does not.
    """
    index = macros if isinstance(macros, MacroIndex) else MacroIndex(macros or {})
    seen: set[str] = set()
    stack = list(index.referenced_ids(str(node.get("raw_code") or node.get("raw_sql") or "")))
    while stack:
        macro_id = stack.pop()
        if macro_id in seen:
            continue
        seen.add(macro_id)
        stack.extend(index.referenced_ids(index.body(macro_id)))
    hashed = {m: hashlib.sha256(index.body(m).encode("utf-8")).hexdigest() for m in sorted(seen)}
    digest = hashlib.sha256(json.dumps(hashed, sort_keys=True).encode("utf-8")).hexdigest()
    return list(hashed), digest


_CALLED_NAME_RE = re.compile(r"\b([A-Za-z_]\w*)\s*\(")
_DISPATCHED_NAME_RE = re.compile(r"dispatch\(\s*['\"]([A-Za-z_]\w*)['\"]")


class MacroIndex:
    """Finds the project and package macros a piece of Jinja calls, by scanning its text.

    The manifest's own `depends_on.macros` is no use here: dbt records only the macros a node's
    *rendered* branches called, so it changes with `is_incremental()`, the adapter and which
    packages are enabled (measured on the flipto-dbt pilot: 7 macros differed between prod and a
    sandbox for the same model). Scanning the text over-approximates, and is the same everywhere.
    """

    def __init__(self, macros: dict[str, Any]) -> None:
        self._bodies: dict[str, str] = {}
        self._by_name: dict[str, list[str]] = {}
        for macro_id, macro in macros.items():
            package = macro_id.split(".")[1] if macro_id.count(".") >= 2 else ""
            if package in _ENGINE_MACRO_PACKAGES:
                continue
            self._bodies[macro_id] = str(macro.get("macro_sql") or "").replace("\r\n", "\n")
            name = str(macro.get("name") or macro_id.rsplit(".", 1)[-1])
            self._by_name.setdefault(name, []).append(macro_id)
            if "__" in name:  # `bigquery__x` / `default__x` implement `adapter.dispatch('x')`
                self._by_name.setdefault(name.split("__", 1)[1], []).append(macro_id)

    def body(self, macro_id: str) -> str:
        return self._bodies.get(macro_id, "")

    def referenced_ids(self, text: str) -> set[str]:
        names = set(_CALLED_NAME_RE.findall(text)) | set(_DISPATCHED_NAME_RE.findall(text))
        return {macro_id for name in names for macro_id in self._by_name.get(name, ())}


def compute_node_fingerprint(
    node: dict[str, Any],
    *,
    upstream_fingerprints: dict[str, str] | None = None,
    known_schemas: list[str] | None = None,
    dependency_identifiers: set[str] | None = None,
    fp_length: int = DEFAULT_FINGERPRINT_LENGTH,
    dialect: str = "bigquery",
    macros: dict[str, Any] | MacroIndex | None = None,
    hash_compiled: bool = True,
    details: dict[str, Any] | None = None,
) -> NodeFingerprint:
    """Compute the deterministic Merkle-DAG fingerprint for a single dbt manifest node.

    `hash_compiled=False` replaces the compiled SQL by a hash of the raw SQL's macro closure
    (see `macro_closure_hash`); `compute_manifest_fingerprints` uses it for ancestors that
    dbt-virtual does not build. `details`, when given, receives what went into the hash.
    """
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

    norm_compiled = (
        normalize_sql(
            compiled_sql,
            self_identifier=node.get("alias") or name,
            known_schemas=schemas_to_strip,
            dependency_identifiers=dependency_identifiers,
            dialect=dialect,
        )
        if hash_compiled
        else ""
    )
    norm_raw = normalize_sql(
        raw_sql,
        self_identifier=name,
        # Raw SQL holds no rendered schema: the node's own schema (which differs per target) would
        # only strip a hard-coded literal of that name in one environment and not the other.
        known_schemas=list(known_schemas or []),
        use_ast=False,
        dialect=dialect,
    )

    sorted_upstreams = dict(sorted((upstream_fingerprints or {}).items()))

    payload = {
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "resource_type": resource_type,
        "name": name,
        # How later runs treat the table, not what it contains (SQLMesh also keeps these out of
        # the data hash), so changing them must not rebuild the physical table.
        "config": {k: v for k, v in config_subset.items() if k not in METADATA_ONLY_CONFIG},
        "normalized_raw_sql": norm_raw,
        "upstream_fingerprints": sorted_upstreams,
    }
    if hash_compiled:
        payload["normalized_compiled_sql"] = norm_compiled
        macro_ids: list[str] = []
    else:
        macro_ids, payload["macro_closure_hash"] = macro_closure_hash(node, macros)
    if details is not None:
        details.update(
            {
                "mode": "compiled_sql" if hash_compiled else "raw_sql_and_macros",
                "normalized_compiled_sql": norm_compiled,
                "normalized_raw_sql": norm_raw,
                "macro_closure_hash": payload.get("macro_closure_hash"),
                "macros": macro_ids,
                "config": payload["config"],
                "upstream_fingerprints": sorted_upstreams,
            }
        )
    if resource_type == "seed":
        # A seed's data lives in its CSV, which only the manifest checksum reflects.
        payload["seed_checksum"] = (node.get("checksum") or {}).get("checksum")

    canonical_bytes = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(canonical_bytes).hexdigest()[:fp_length]
    if config_subset.get("physical_version"):
        digest = str(config_subset["physical_version"])

    # Routines and views have no `<name>__<fp>` physical table; they are deployed by DDL.
    has_physical_table = materialized not in ROUTINE_MATERIALIZATIONS and materialized != "view"
    physical_identifier = f"{name}__{digest}" if has_physical_table else name

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
    dialect: str | None = None,
    hash_compiled_ancestors: bool = False,
    details: dict[str, dict[str, Any]] | None = None,
) -> dict[str, NodeFingerprint]:
    """Compute Merkle-DAG fingerprints for all virtual_env nodes in a dbt manifest.

    Virtual nodes hash their compiled SQL. Ancestors that dbt-virtual does not build (non-virtual
    models and snapshots) hash their raw SQL and macro closure instead, because their compiled SQL
    depends on the warehouse and the clock; `hash_compiled_ancestors=True` restores the old rule.
    `details`, when given, is filled with `unique_id -> what went into each hash` (every node
    reached, not only virtual ones).

    Raises `MissingCompiledCodeError` when a virtual node has Jinja in its raw SQL but the
    manifest was parsed, not compiled (`dbt parse`, `run-operation`): hashing only the raw SQL
    would give a different fingerprint than `plan` after a compile.
    """
    nodes: dict[str, dict[str, Any]] = manifest.get("nodes") or {}
    macros = MacroIndex(manifest.get("macros") or {})
    sources: dict[str, Any] = manifest.get("sources") or {}
    adapter_type = (manifest.get("metadata") or {}).get("adapter_type")
    sql_dialect = dialect or (str(adapter_type).removeprefix("dbt-") if adapter_type else "bigquery")
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
    uncompiled = sorted(
        str(n.get("name"))
        for n in virtual_nodes.values()
        if not (n.get("compiled_code") or n.get("compiled_sql"))
        and any(m in (n.get("raw_code") or n.get("raw_sql") or "") for m in _JINJA_MARKERS)
    )
    if uncompiled:
        raise MissingCompiledCodeError(
            f"The manifest has no compiled SQL for virtual node(s) {', '.join(uncompiled)}: it comes "
            "from `dbt parse` or a `run-operation`, not from `dbt compile`. Fingerprints would hash "
            "the raw SQL only and differ from `dbt-virtual plan`. Compile first (omit --skip-compile)."
        )
    # Every ref()-able node contributes to its children's fingerprints: models (virtual or not),
    # seeds (CSV checksum) and snapshots.
    dependency_nodes = {
        uid: n
        for uid, n in nodes.items()
        if is_virtual_node(n) or n.get("resource_type") in {"model", "seed", "snapshot"}
    }

    computed: dict[str, NodeFingerprint] = {}
    visiting: set[str] = set()

    def _resolve(uid: str) -> NodeFingerprint:
        if uid in computed:
            return computed[uid]
        if uid in visiting:
            raise ValueError(f"Cycle detected in dbt-virtual dependency graph at {uid}")
        visiting.add(uid)

        node = dependency_nodes[uid]
        depends_on = (node.get("depends_on") or {}).get("nodes") or []
        upstream_fps: dict[str, str] = {}
        parent_identifiers: set[str] = set()
        for parent_uid in depends_on:
            if parent_uid in dependency_nodes:
                parent_fp = _resolve(parent_uid)
                upstream_fps[parent_uid] = parent_fp.fingerprint
                parent = dependency_nodes[parent_uid]
                parent_identifiers.update(
                    str(v) for v in (parent.get("name"), parent.get("alias")) if v
                )
            elif parent_uid in sources:
                # A source's location often differs per target; its schema is not a logic change.
                parent_identifiers.add(str(sources[parent_uid].get("identifier") or sources[parent_uid].get("name")))

        node_details: dict[str, Any] | None = {} if details is not None else None
        fp_obj = compute_node_fingerprint(
            node,
            upstream_fingerprints=upstream_fps,
            known_schemas=known_schemas,
            dependency_identifiers=parent_identifiers,
            fp_length=fp_length,
            dialect=sql_dialect,
            macros=macros,
            hash_compiled=hash_compiled_ancestors
            or is_virtual_node(node)
            or node.get("resource_type") == "seed",
            details=node_details,
        )
        if details is not None and node_details is not None:
            details[uid] = {**node_details, "fingerprint": fp_obj.fingerprint}
        visiting.remove(uid)
        computed[uid] = fp_obj
        return fp_obj

    for uid in sorted(virtual_nodes):
        _resolve(uid)

    return {uid: computed[uid] for uid in sorted(virtual_nodes)}


_ENV_DEPENDENT_PATTERNS: tuple[tuple[str, str], ...] = (
    (
        "qualified relation that is not a dependency (hard-coded or per-target location)",
        r"`[^`\n]+`\.`[^`\n]+`\.`[^`\n]+`|`[^`.\n]+\.[^`.\n]+\.[^`.\n]+`",
    ),
    ("clock function (value moves between runs)", r"\b(?:CURRENT_(?:DATE|TIMESTAMP|DATETIME|TIME)|NOW|GETDATE|SYSDATE)\b"),
    (
        "timestamp literal (often run_started_at or a watermark read from the warehouse)",
        r"'\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[^']*'",
    ),
)


def find_environment_dependent_tokens(normalized_sql: str) -> list[tuple[str, str]]:
    """List `(reason, text)` for tokens left in normalized SQL that may differ between environments.

    These are not removed: a hard-coded prod table or a branch on `target.name` is a real logic
    difference. They are what to look at when the same code gets two fingerprints.
    """
    found: list[tuple[str, str]] = []
    for reason, pattern in _ENV_DEPENDENT_PATTERNS:
        for match in sorted({m.group(0) for m in re.finditer(pattern, normalized_sql, re.IGNORECASE)}):
            found.append((reason, match))
    return found


def explain_fingerprint(
    manifest: dict[str, Any],
    selector: str,
    *,
    known_schemas: list[str] | None = None,
    hash_compiled_ancestors: bool = False,
    include_ancestors: bool = False,
) -> str:
    """Render what went into the fingerprint of a model (by name or unique_id) as text."""
    details: dict[str, dict[str, Any]] = {}
    compute_manifest_fingerprints(
        manifest,
        known_schemas=known_schemas,
        hash_compiled_ancestors=hash_compiled_ancestors,
        details=details,
    )
    nodes = manifest.get("nodes") or {}
    matches = [uid for uid in details if uid == selector or nodes[uid].get("name") == selector]
    if not matches:
        raise ValueError(f"No virtual model or ancestor named '{selector}' in the manifest.")
    lines: list[str] = []

    def _render(uid: str) -> None:
        info = details[uid]
        lines.append(f"fingerprint {info['fingerprint']}  {uid}  (hashed from: {info['mode']})")
        lines.append(f"  config: {json.dumps(info['config'], sort_keys=True, default=str)}")
        for parent, parent_fp in info["upstream_fingerprints"].items():
            lines.append(f"  upstream {parent}: {parent_fp}")
        if info["mode"] == "compiled_sql":
            lines.append(f"  normalized compiled SQL: {info['normalized_compiled_sql']}")
            for reason, text in find_environment_dependent_tokens(info["normalized_compiled_sql"]):
                lines.append(f"  env-dependent? {reason}: {text}")
        else:
            lines.append(f"  macros hashed ({len(info['macros'])}): {', '.join(info['macros'])}")
            lines.append("  compiled SQL is not hashed for this ancestor (it depends on warehouse state and time)")
        lines.append(f"  normalized raw SQL: {info['normalized_raw_sql']}")
        lines.append("")

    for uid in matches:
        _render(uid)
        if include_ancestors:
            stack = list(details[uid]["upstream_fingerprints"])
            done: set[str] = set()
            while stack:
                parent = stack.pop()
                if parent in done or parent not in details:
                    continue
                done.add(parent)
                _render(parent)
                stack.extend(details[parent]["upstream_fingerprints"])
    return "\n".join(lines)


def write_fingerprints_file(
    fingerprints: dict[str, NodeFingerprint],
    output_path: Path,
    cache: dict | None = None,
) -> None:
    """Write computed fingerprints to `.dbt_virtual/fingerprints.json`.

    `cache` (inputs hash + manifest checksum) lets `dbt-virtual dbt` skip `dbt compile` next time.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "schema_version": FINGERPRINT_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nodes": {uid: fp.to_dict() for uid, fp in sorted(fingerprints.items())},
        "by_name": {fp.name: fp.fingerprint for fp in sorted(fingerprints.values(), key=lambda x: x.name)},
    }
    if cache is not None:
        data["cache"] = cache
    output_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
