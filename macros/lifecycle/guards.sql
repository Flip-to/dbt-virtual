{#-
  Promotion guards: an incomplete table must never reach a target environment.

  - `build_quality()`        'partial' for a windowed / `--empty` build, else 'full'.
  - `set_build_quality_sql`  marks the pointer and release rows a build just wrote.
  - `seal_release_sql`       marks a release sealed once `dbt build` finished for all of it.
  - `promotion_guard_problems` fingerprint / partial / sealed checks used by `virtual_promote`.
  - `partition_coverage_sql` BigQuery partition comparison used by `virtual_audit`.
-#}

{% macro build_partial_reasons() -%}
  {#- Why this dbt invocation builds only part of the data. Empty list = a full build. -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set reasons = [] -%}
  {%- if flags is defined and flags.EMPTY -%}
    {%- do reasons.append('dbt --empty') -%}
  {%- endif -%}
  {%- for var_name in (cfg.get('partial_build_vars') or []) -%}
    {%- set var_value = var(var_name, none) -%}
    {%- if var_value is not none and var_value != '' -%}
      {%- do reasons.append("var '" ~ var_name ~ "' is set") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if var('_dbt_virtual_partial', false) in [true, 'true', 'True', 'TRUE', 1] -%}
    {%- do reasons.append('_dbt_virtual_partial') -%}
  {%- endif -%}
  {{ return(reasons) }}
{%- endmacro %}


{% macro build_quality() -%}
  {#- TECH_DEBT(reuse_pointer_build_quality): per invocation; REUSE pointers should keep the reused release's quality. docs/tech_debt/reuse_pointer_build_quality.md -#}
  {{ return('partial' if (dbt_virtual.build_partial_reasons() | length) > 0 else 'full') }}
{%- endmacro %}


{% macro set_build_quality_sql(physical_schema, env_name, model_name, release_id, quality) -%}
  {#- Statements stamping `build_quality` on the rows the surrounding post-hook just wrote.
      Always written, so a full rebuild clears an earlier 'partial' on the same pointer row. -#}
  UPDATE {{ dbt_virtual.registry_relation(physical_schema, '_environments') }}
  SET build_quality = '{{ quality }}'
  WHERE environment = '{{ env_name }}' AND model_name = '{{ model_name }}';
  {%- if release_id != '' %}
  UPDATE {{ dbt_virtual.registry_relation(physical_schema, '_releases') }}
  SET build_quality = '{{ quality }}'
  WHERE release_id = '{{ release_id }}' AND model_name = '{{ model_name }}';
  {%- endif %}
{%- endmacro %}


{% macro seal_release_sql(physical_schema, release_id) -%}
  UPDATE {{ dbt_virtual.registry_relation(physical_schema, '_releases') }}
  SET sealed_at = {{ dbt_virtual.current_timestamp_sql() }}
  WHERE release_id = '{{ release_id }}' AND sealed_at IS NULL
{%- endmacro %}


{% macro virtual_seal_release(release_id='') -%}
  {#- Manually seals a release, e.g. one created before sealing existed after you verified its tables:
      `dbt run-operation virtual_seal_release --args '{release_id: v1}'`. -#}
  {%- if release_id == '' -%}
    {{ exceptions.raise_compiler_error("dbt-virtual seal-release: release_id is required.") }}
  {%- endif -%}
  {#- A release spans the registries of every location (one per BigQuery region). -#}
  {%- for location in dbt_virtual.physical_locations() -%}
    {%- do dbt_virtual.ensure_registry(location) -%}
    {%- do dbt_virtual.exec_ddl_dml(dbt_virtual.seal_release_sql(dbt_virtual.physical_schema_for(location), release_id)) -%}
  {%- endfor -%}
  {{ log("dbt-virtual seal-release: sealed release " ~ release_id ~ ".", info=True) }}
{%- endmacro %}


{% macro promotion_guard_problems(rows, to_env, release_id, expected_fps, allow_unsealed, require_sealed, build_quality_index, sealed_at_index) -%}
  {#- Returns a list of reasons the registry rows of one release (or environment) must not be
      promoted. Rows are `model_name, resource_type, fingerprint, ...`; the guard columns are read
      with `row_value`, so a row without them (older callers) counts as full and legacy. -#}
  {%- set problems = [] -%}

  {%- if expected_fps is string -%}
    {%- set expected_fps = fromjson(expected_fps) if expected_fps != '' else none -%}
  {%- endif -%}
  {%- if expected_fps is mapping -%}
    {%- set stale = [] -%}
    {%- for row in rows -%}
      {%- if row[0] not in expected_fps -%}
        {%- do stale.append(row[0] ~ ' (' ~ row[2] ~ ', not a virtual model of this project)') -%}
      {%- elif expected_fps[row[0]] != row[2] -%}
        {%- do stale.append(row[0] ~ ' (release ' ~ row[2] ~ ', ' ~ to_env ~ ' code ' ~ expected_fps[row[0]] ~ ')') -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if stale | length > 0 -%}
      {%- do problems.append(
        "fingerprint differs from the " ~ to_env ~ " code for " ~ (stale | length) ~ " object(s): "
        ~ (stale | join(', ')) ~ ". They were built from different code or variables (e.g. a windowed build)."
      ) -%}
    {%- endif -%}
  {%- endif -%}

  {%- set partial = [] -%}
  {%- set tracked = namespace(value=false, sealed=false) -%}
  {%- for row in rows -%}
    {%- set quality = dbt_virtual.row_value(row, build_quality_index) -%}
    {%- if quality == 'partial' -%}
      {%- do partial.append(row[0]) -%}
    {%- endif -%}
    {%- if quality is not none -%}
      {%- set tracked.value = true -%}
    {%- endif -%}
    {%- if sealed_at_index is not none and dbt_virtual.row_value(row, sealed_at_index) is not none -%}
      {%- set tracked.sealed = true -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if partial | length > 0 -%}
    {%- do problems.append(
      "built partially (windowed or `--empty` build, build_quality='partial'): " ~ (partial | sort | join(', '))
      ~ ". Rebuild without the window / `--empty`."
    ) -%}
  {%- endif -%}

  {%- if release_id != '' and sealed_at_index is not none and not allow_unsealed and not tracked.sealed -%}
    {%- if tracked.value -%}
      {%- do problems.append(
        "release " ~ release_id ~ " is not sealed: `dbt-virtual build --release` did not finish successfully for it."
        ~ " Re-run the build, or pass allow_unsealed=true (--allow-unsealed) after checking the tables yourself."
      ) -%}
    {%- elif require_sealed -%}
      {%- do problems.append(
        "release " ~ release_id ~ " has no seal (created before sealing existed). Verify its tables, then run"
        ~ " `dbt run-operation virtual_seal_release --args '{release_id: " ~ release_id ~ "}'`, or pass"
        ~ " require_sealed=false (--no-require-sealed)."
      ) -%}
    {%- endif -%}
  {%- endif -%}
  {{ return(problems) }}
{%- endmacro %}


{% macro partition_coverage_sql(cand_rel, base_rel) -%}
  {#- SQL returning the partition ids that `base_rel` has and `cand_rel` lacks; '' when unsupported. -#}
  {{ return(adapter.dispatch('partition_coverage_sql', 'dbt_virtual')(cand_rel, base_rel)) }}
{%- endmacro %}

{% macro bigquery__partition_coverage_sql(cand_rel, base_rel) -%}
  {#- `INFORMATION_SCHEMA.PARTITIONS` is dataset-qualified; both tables live in `physical_schema`. -#}
  SELECT b.partition_id
  FROM `{{ base_rel.database }}`.`{{ base_rel.schema }}`.INFORMATION_SCHEMA.PARTITIONS AS b
  WHERE b.table_name = '{{ base_rel.identifier }}'
    AND b.partition_id NOT IN ('__NULL__', '__UNPARTITIONED__')
    AND b.total_rows > 0
    AND NOT EXISTS (
      SELECT 1
      FROM `{{ cand_rel.database }}`.`{{ cand_rel.schema }}`.INFORMATION_SCHEMA.PARTITIONS AS c
      WHERE c.table_name = '{{ cand_rel.identifier }}'
        AND c.partition_id = b.partition_id
        AND c.total_rows > 0
    )
  ORDER BY b.partition_id
{%- endmacro %}

{% macro default__partition_coverage_sql(cand_rel, base_rel) -%}
  {{ return('') }}
{%- endmacro %}


{% macro partition_ranges(partition_ids) -%}
  {#- Collapses sorted partition ids into ranges: daily ids `YYYYMMDD` that follow each other become
      `YYYYMMDD..YYYYMMDD (n)`; other ids are listed as they are. -#}
  {%- set ranges = [] -%}
  {%- set current = namespace(start=none, end=none, count=0, last_date=none) -%}
  {%- for pid in partition_ids -%}
    {%- set pid = pid | string -%}
    {%- set date_value = none -%}
    {%- if pid | length == 8 and pid.isdigit() and modules is defined and modules.datetime is defined -%}
      {%- set date_value = modules.datetime.datetime.strptime(pid, '%Y%m%d') -%}
    {%- endif -%}
    {%- if current.start is not none and date_value is not none and current.last_date is not none
          and (date_value - current.last_date).days == 1 -%}
      {%- set current.end = pid -%}
      {%- set current.count = current.count + 1 -%}
      {%- set current.last_date = date_value -%}
    {%- else -%}
      {%- if current.start is not none -%}
        {%- do ranges.append(current.start if current.count == 1 else current.start ~ '..' ~ current.end ~ ' (' ~ current.count ~ ')') -%}
      {%- endif -%}
      {%- set current.start = pid -%}
      {%- set current.end = pid -%}
      {%- set current.count = 1 -%}
      {%- set current.last_date = date_value -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if current.start is not none -%}
    {%- do ranges.append(current.start if current.count == 1 else current.start ~ '..' ~ current.end ~ ' (' ~ current.count ~ ')') -%}
  {%- endif -%}
  {{ return(ranges) }}
{%- endmacro %}


{% macro partition_is_before(partition_id, ignore_before) -%}
  {#- True when the partition id (YYYY, YYYYMM, YYYYMMDD, YYYYMMDDHH) is older than `ignore_before`
      (a date such as '2026-10-01'). Ids that are not calendar ids (integer ranges) are never ignored. -#}
  {%- set digits = (ignore_before | string) | replace('-', '') | replace(' ', '') | replace(':', '') -%}
  {%- set pid = partition_id | string -%}
  {%- if digits == '' or not pid.isdigit() or (pid | length) not in [4, 6, 8, 10] -%}
    {{ return(false) }}
  {%- endif -%}
  {{ return(pid < digits[:(pid | length)]) }}
{%- endmacro %}
