# SPEC 01: Merkle-DAG Fingerprinting & Canonical SQL Normalization

- **Status**: Draft v1.0
- **Component**: `src/dbt_virtual/fingerprint.py` & `macros/fingerprint/get_fingerprint.sql`

---

## 1. Objective

Provide a deterministic, collision-resistant content fingerprint `<fp>` (12-character lowercase hexadecimal prefix of SHA-256) for every opted-in dbt node (`virtual_env: true`) such that:

1. **Zero False Positives (Target Invariance)**: Compiling the exact same code under `--target staging`, `--target prod`, or `--target dev` produces the **exact same `<fp>`**.
2. **Zero False Negatives (Complete Input Coverage)**: Any change to a model's SQL, an inline or shared Jinja macro it calls, a `var()` it references in `dbt_project.yml`, its physical materialization config (`partition_by`, `cluster_by`, `unique_key`, `incremental_strategy`), or an upstream virtualized dependency's `<fp>` produces a **new `<fp>`**.

---

## 2. Why Raw SQL Hashing Is Insufficient

As identified in `Flip-to/flipto-dbt#509`:
- Hashing `node.raw_code` (uncompiled Jinja) misses changes to shared macros (e.g., `macros/transaction_metrics_root_query.sql`) and project variables (e.g., `var('flipto_spacetime').grouping_dimension_maps`).
- Hashing `node.compiled_code` verbatim includes target-specific schema names (`spacetime_staging` vs. `spacetime` vs. `rt_pipeline_dev_user`) and self-references (`{{ this }}`), which would force a new fingerprint on every environment promotion and defeat zero-rebuild promotion.

Therefore, `dbt-virtual` hashes **Canonical Normalized Compiled SQL** combined with a **Merkle DAG** of upstream virtualized node fingerprints.

---

## 3. Canonical Fingerprint Payload Schema

For any node $u \in V_{\text{virtual}}$, the fingerprint input is a canonical JSON object (keys sorted lexicographically, UTF-8 encoded, no whitespace separators `(",", ":")`):

```json
{
  "config": {
    "cluster_by": ["entity_uuid"],
    "incremental_strategy": "insert_overwrite",
    "materialized": "incremental",
    "on_schema_change": "ignore",
    "parameters": null,
    "partition_by": {
      "data_type": "date",
      "field": "granular_date",
      "granularity": "day"
    },
    "returns": null,
    "unique_key": null,
    "virtual_env_epoch": 1
  },
  "normalized_sql": "SELECT ...",
  "resource_type": "model",
  "schema_version": 1,
  "upstream_fingerprints": {
    "model.my_project.sessions_root": "a1b2c3d4e5f6"
  }
}
```

The resulting fingerprint is:
$$\text{fp}(u) = \text{ hex}(\text{SHA-256}(\text{canonical\_json}(u)))[0:12]$$

---

## 4. SQL Normalization Rules

Given `node.compiled_code` from `target/manifest.json`:

1. **Self-Relation Replacement (`{{ this }}`)**:
   - Replace all occurrences of the node's own database/schema/alias identifier (quoted or unquoted, including any `__<fp>` suffix) with the token `__DBT_VIRTUAL_THIS__`.
2. **Environment Schema Normalization**:
   - Replace all configured `env_schemas` values (`spacetime_staging`, `spacetime`, `rt_pipeline_prod1`, `rt_pipeline_dev_*`) and `physical_schema` references on upstream `ref()` relations with canonical logical relation tokens `__DBT_VIRTUAL_REF__<model_name>__`.
3. **Comment & Whitespace Canonicalization**:
   - Strip SQL block comments (`/* ... */`) injected by dbt or query tags while preserving string literals.
   - Normalize line endings (`\r\n` → `\n`) and collapse runs of whitespace outside string literals.
4. **`is_incremental()` Branch Determinism**:
   - When `dbt compile` runs against a target where the physical table does not yet exist, `is_incremental()` may evaluate to `false` unless overridden, whereas on an existing table it evaluates to `true`.
   - To guarantee branch invariance for `incremental` models, `dbt-virtual` includes a normalized hash of `node.raw_code` alongside `normalized_sql` (so changes inside either the `{% if is_incremental() %}` branch or `{% else %}` full-refresh branch always invalidate the fingerprint).

---

## 5. Topological Merkle-DAG Traversal

1. Load `nodes` from `target/manifest.json`.
2. Filter to nodes where `node.config.extra.get("virtual_env")` or `node.config.meta.get("virtual_env")` is truthy.
3. Perform a topological sort over `node.depends_on.nodes`.
4. Compute $\text{fp}(u)$ in topological order so every parent's $\text{fp}(p)$ is already resolved before computing any child $u$.
5. Output `.dbt_virtual/fingerprints.json`:
   ```json
   {
     "generated_at": "2026-10-27T20:00:00Z",
     "schema_version": 1,
     "nodes": {
       "model.flipto.traffic_metrics_root": {
         "name": "traffic_metrics_root",
         "fingerprint": "8f3a9c1b2d4e",
         "physical_identifier": "traffic_metrics_root__8f3a9c1b2d4e",
         "materialized": "incremental",
         "upstream_fingerprints": {}
       }
     }
   }
   ```

---

## 6. Pure Jinja Fallback Mode

When a user invokes `dbt run` directly without `dbt-virtual build`:
1. `dbt_virtual.get_fingerprint(node)` checks `var('_dbt_virtual_fps', {})`.
2. If the node's key is present in `_dbt_virtual_fps`, it returns that exact precomputed fingerprint.
3. Otherwise, it computes an in-Jinja MD5/SHA hash (`local_md5`) over:
   - `node.raw_code`
   - `node.config.materialized`, `node.config.incremental_strategy`, `node.config.partition_by`, `node.config.cluster_by`
   - `tojson(var('dbt_virtual_fingerprint_vars', []))`
   and slices the first 12 hex characters.
