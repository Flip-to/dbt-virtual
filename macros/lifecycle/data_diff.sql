{#-
  Row-level data diff by checksum bisection (the Datafold data-diff / reladiff approach).

  For each model, the candidate table (`from_env` or `release_id`) is compared with the table
  `to_env` points at:
  1. Segment checksums: one grouped query per side returns `COUNT(*)` and an order-independent
     row-hash aggregate per segment. A segment is a partition (BigQuery `partition_by` in the node
     config), or the whole table when it is not partitioned.
  2. Bisection: a segment whose (count, hash) differ and that holds more than `threshold` rows is
     split into `branching` key ranges (`unique_key` / `diff_key`), one grouped query per side per
     segment, and only the ranges that differ are split again, until a range holds at most
     `threshold` rows or `max_depth` is reached.
  3. Leaf join: only the remaining small ranges are joined on the key to count rows only in the
     candidate, only in the target, and changed rows (with per-column mismatch counts and a sample).
  Identical tables never reach step 3: they cost one checksum query per side.

  Logs one `DBT_VIRTUAL_DIFF_JSON:{...}` line.
-#}

{% macro diff_int_type() -%}
  {{ return(adapter.dispatch('diff_int_type', 'dbt_virtual')()) }}
{%- endmacro %}

{% macro bigquery__diff_int_type() -%}
  INT64
{%- endmacro %}

{% macro default__diff_int_type() -%}
  BIGINT
{%- endmacro %}


{% macro diff_row_hash_agg_sql(columns) -%}
  {#- Order-independent aggregate of one 64-bit hash per row; `columns` are already quoted. -#}
  {{ return(adapter.dispatch('diff_row_hash_agg_sql', 'dbt_virtual')(columns)) }}
{%- endmacro %}

{% macro bigquery__diff_row_hash_agg_sql(columns) -%}
  BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({{ columns | join(', ') }}))))
{%- endmacro %}

{% macro snowflake__diff_row_hash_agg_sql(columns) -%}
  BITXOR_AGG(HASH({{ columns | join(', ') }}))
{%- endmacro %}

{% macro databricks__diff_row_hash_agg_sql(columns) -%}
  bit_xor(xxhash64({{ columns | join(', ') }}))
{%- endmacro %}

{% macro postgres__diff_row_hash_agg_sql(columns) -%}
  bit_xor(hashtextextended(ROW({{ columns | join(', ') }})::text, 0))
{%- endmacro %}

{% macro duckdb__diff_row_hash_agg_sql(columns) -%}
  bit_xor(hash({{ columns | join(', ') }}))
{%- endmacro %}

{% macro default__diff_row_hash_agg_sql(columns) -%}
  bit_xor(hash({{ columns | join(', ') }}))
{%- endmacro %}


{% macro diff_distinct_sql(left_expr, right_expr) -%}
  {#- Null-safe "values differ". BigQuery cannot compare ARRAY / STRUCT / JSON, so it compares their JSON text. -#}
  {{ return(adapter.dispatch('diff_distinct_sql', 'dbt_virtual')(left_expr, right_expr)) }}
{%- endmacro %}

{% macro bigquery__diff_distinct_sql(left_expr, right_expr) -%}
  TO_JSON_STRING({{ left_expr }}) IS DISTINCT FROM TO_JSON_STRING({{ right_expr }})
{%- endmacro %}

{% macro default__diff_distinct_sql(left_expr, right_expr) -%}
  {{ left_expr }} IS DISTINCT FROM {{ right_expr }}
{%- endmacro %}


{% macro diff_to_date_sql(expr) -%}
  {{ return(adapter.dispatch('diff_to_date_sql', 'dbt_virtual')(expr)) }}
{%- endmacro %}

{% macro bigquery__diff_to_date_sql(expr) -%}
  DATE({{ expr }})
{%- endmacro %}

{% macro default__diff_to_date_sql(expr) -%}
  CAST({{ expr }} AS DATE)
{%- endmacro %}


{% macro diff_window_predicate_sql(date_expr, days) -%}
  {{ return(adapter.dispatch('diff_window_predicate_sql', 'dbt_virtual')(date_expr, days)) }}
{%- endmacro %}

{% macro bigquery__diff_window_predicate_sql(date_expr, days) -%}
  {{ date_expr }} >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ days | int }} DAY)
{%- endmacro %}

{% macro snowflake__diff_window_predicate_sql(date_expr, days) -%}
  {{ date_expr }} >= DATEADD(day, -{{ days | int }}, CURRENT_DATE)
{%- endmacro %}

{% macro databricks__diff_window_predicate_sql(date_expr, days) -%}
  {{ date_expr }} >= date_sub(current_date(), {{ days | int }})
{%- endmacro %}

{% macro default__diff_window_predicate_sql(date_expr, days) -%}
  {{ date_expr }} >= CURRENT_DATE - INTERVAL '{{ days | int }} day'
{%- endmacro %}


{% macro diff_as_list(value) -%}
  {#- A config value that may be a list, a comma separated string, or empty -> list of strings. -#}
  {%- if value is none or value == '' -%}
    {{ return([]) }}
  {%- elif value is string -%}
    {%- set out = [] -%}
    {%- for part in value.split(',') -%}
      {%- if (part | trim) != '' -%}{%- do out.append(part | trim) -%}{%- endif -%}
    {%- endfor -%}
    {{ return(out) }}
  {%- else -%}
    {{ return(value | list) }}
  {%- endif -%}
{%- endmacro %}


{% macro diff_node_config(model_name, key) -%}
  {#- `key` from the model's node config (or its `meta`), looked up by model name in the graph. -#}
  {%- set nodes_dict = graph.nodes if (graph is defined and graph.nodes is defined) else {} -%}
  {%- for node in nodes_dict.values() -%}
    {%- if dbt_virtual.node_attr(node, 'resource_type', '') == 'model' and dbt_virtual.node_attr(node, 'name', '') == model_name -%}
      {%- set node_cfg = dbt_virtual.node_attr(node, 'config', {}) -%}
      {%- set val = dbt_virtual.node_attr(node_cfg, key, none) -%}
      {%- if val is none -%}
        {%- set val = dbt_virtual.node_attr(dbt_virtual.node_attr(node_cfg, 'meta', {}), key, none) -%}
      {%- endif -%}
      {{ return(val) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return(none) }}
{%- endmacro %}


{% macro diff_fetch_stats(relation, hash_agg, group_expr, key_q, where_sql, counters) -%}
  {#- One grouped checksum query: {group value as string: {seg, cnt, h, kmin, kmax}}. -#}
  {%- set sql -%}
    SELECT {{ group_expr }} AS dv_seg, COUNT(*) AS dv_cnt, {{ hash_agg }} AS dv_hash
      {%- if key_q %}, MIN({{ key_q }}) AS dv_kmin, MAX({{ key_q }}) AS dv_kmax{% endif %}
    FROM {{ relation }}
    WHERE {{ where_sql }}
    GROUP BY 1
  {%- endset -%}
  {%- do counters.update({'checksum_queries': counters['checksum_queries'] + 1}) -%}
  {%- set res = run_query(sql) -%}
  {%- set out = {} -%}
  {%- if res -%}
    {%- for r in res.rows -%}
      {%- do out.update({
        (r[0] | string): {
          'seg': r[0], 'cnt': r[1] | int, 'h': r[2] | string,
          'kmin': (r[3] if (key_q and (r | length) > 3) else none),
          'kmax': (r[4] if (key_q and (r | length) > 4) else none)
        }
      }) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(out) }}
{%- endmacro %}


{% macro diff_merge_stats(cand_stats, base_stats) -%}
  {#- Segments (group values) of both sides, with `differs` set when (count, hash) are not equal. -#}
  {%- set seen = [] -%}
  {%- set segs = [] -%}
  {%- for gv in (cand_stats.keys() | list) + (base_stats.keys() | list) -%}
    {%- if gv not in seen -%}
      {%- do seen.append(gv) -%}
      {%- set c = cand_stats.get(gv, {'cnt': 0, 'h': none, 'kmin': none, 'kmax': none, 'seg': none}) -%}
      {%- set b = base_stats.get(gv, {'cnt': 0, 'h': none, 'kmin': none, 'kmax': none, 'seg': none}) -%}
      {%- set los = [c.kmin, b.kmin] | reject('none') | list -%}
      {%- set his = [c.kmax, b.kmax] | reject('none') | list -%}
      {%- do segs.append({
        'gv': gv,
        'seg': (c.seg if cand_stats.get(gv) else b.seg),
        'cc': c.cnt, 'ct': b.cnt,
        'differs': (c.cnt != b.cnt or (c.h | string) != (b.h | string)),
        'lo': (los | min if los | length > 0 else none),
        'hi': (his | max if his | length > 0 else none)
      }) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(segs) }}
{%- endmacro %}


{% macro diff_leaf_aggregate_sql(cand_rel, base_rel, columns, keys, where_sql) -%}
  {%- set non_keys = columns | reject('in', keys) | list -%}
  {%- set differs = [] -%}
  {%- for col in non_keys -%}
    {%- do differs.append(dbt_virtual.diff_distinct_sql('dv_c.' ~ adapter.quote(col), 'dv_t.' ~ adapter.quote(col))) -%}
  {%- endfor -%}
  {%- set k0 = adapter.quote(keys[0]) -%}
  {%- set both = 'dv_c.' ~ k0 ~ ' IS NOT NULL AND dv_t.' ~ k0 ~ ' IS NOT NULL' -%}
  {%- set join_on = [] -%}
  {%- for k in keys -%}
    {%- do join_on.append('dv_c.' ~ adapter.quote(k) ~ ' = dv_t.' ~ adapter.quote(k)) -%}
  {%- endfor -%}
  {%- set quoted = [] -%}
  {%- for col in columns -%}{%- do quoted.append(adapter.quote(col)) -%}{%- endfor -%}
  SELECT
    COUNT(CASE WHEN dv_t.{{ k0 }} IS NULL THEN 1 END) AS dv_only_candidate,
    COUNT(CASE WHEN dv_c.{{ k0 }} IS NULL THEN 1 END) AS dv_only_target,
    COUNT(CASE WHEN {{ both }}{% if differs %} AND ({{ differs | join(' OR ') }}){% else %} AND FALSE{% endif %} THEN 1 END) AS dv_changed
    {%- for col in non_keys %},
    COUNT(CASE WHEN {{ both }} AND {{ differs[loop.index0] }} THEN 1 END) AS dv_m_{{ loop.index0 }}
    {%- endfor %}
  FROM (SELECT {{ quoted | join(', ') }} FROM {{ cand_rel }} WHERE {{ where_sql }}) AS dv_c
  FULL OUTER JOIN (SELECT {{ quoted | join(', ') }} FROM {{ base_rel }} WHERE {{ where_sql }}) AS dv_t
    ON {{ join_on | join(' AND ') }}
{%- endmacro %}


{% macro diff_leaf_sample_sql(cand_rel, base_rel, columns, keys, where_sql, limit) -%}
  {%- set non_keys = columns | reject('in', keys) | list -%}
  {%- set differs = [] -%}
  {%- for col in non_keys -%}
    {%- do differs.append(dbt_virtual.diff_distinct_sql('dv_c.' ~ adapter.quote(col), 'dv_t.' ~ adapter.quote(col))) -%}
  {%- endfor -%}
  {%- set k0 = adapter.quote(keys[0]) -%}
  {%- set both = 'dv_c.' ~ k0 ~ ' IS NOT NULL AND dv_t.' ~ k0 ~ ' IS NOT NULL' -%}
  {%- set join_on = [] -%}
  {%- for k in keys -%}
    {%- do join_on.append('dv_c.' ~ adapter.quote(k) ~ ' = dv_t.' ~ adapter.quote(k)) -%}
  {%- endfor -%}
  {%- set quoted = [] -%}
  {%- for col in columns -%}{%- do quoted.append(adapter.quote(col)) -%}{%- endfor -%}
  SELECT
    CASE WHEN dv_c.{{ k0 }} IS NULL THEN 'only_target' WHEN dv_t.{{ k0 }} IS NULL THEN 'only_candidate' ELSE 'changed' END AS dv_kind
    {%- for k in keys %},
    COALESCE(dv_c.{{ adapter.quote(k) }}, dv_t.{{ adapter.quote(k) }}) AS dv_k_{{ loop.index0 }}
    {%- endfor %}
    {%- for col in non_keys %},
    CASE WHEN {{ both }} AND {{ differs[loop.index0] }} THEN 1 ELSE 0 END AS dv_d_{{ loop.index0 }}
    {%- endfor %}
  FROM (SELECT {{ quoted | join(', ') }} FROM {{ cand_rel }} WHERE {{ where_sql }}) AS dv_c
  FULL OUTER JOIN (SELECT {{ quoted | join(', ') }} FROM {{ base_rel }} WHERE {{ where_sql }}) AS dv_t
    ON {{ join_on | join(' AND ') }}
  WHERE dv_t.{{ k0 }} IS NULL OR dv_c.{{ k0 }} IS NULL{% if differs %} OR ({{ both }} AND ({{ differs | join(' OR ') }})){% endif %}

  ORDER BY dv_kind, dv_k_0
  LIMIT {{ limit | int }}
{%- endmacro %}


{% macro diff_model(model_name, cand_rel, base_rel, opts) -%}
  {#- Diff one model. `opts`: threshold, branching, max_depth, sample_limit, exclude_columns, where, window_days. -#}
  {%- set counters = {'checksum_queries': 0, 'join_queries': 0, 'sample_queries': 0} -%}

  {#- Configuration: the model's node config, then the call's arguments. -#}
  {%- set unique_key = dbt_virtual.diff_node_config(model_name, 'unique_key') -%}
  {%- set keys = dbt_virtual.diff_as_list(dbt_virtual.diff_node_config(model_name, 'diff_key') or unique_key) -%}
  {%- set excludes = (dbt_virtual.diff_as_list(dbt_virtual.diff_node_config(model_name, 'diff_exclude_columns')) + dbt_virtual.diff_as_list(opts.get('exclude_columns'))) | map('lower') | list -%}
  {%- set node_where = dbt_virtual.diff_node_config(model_name, 'diff_where') -%}
  {%- set window_days = opts.get('window_days') or dbt_virtual.diff_node_config(model_name, 'diff_window_days') -%}
  {#- `diff_partition_by` (config or meta) overrides `partition_by`, which adapters other than BigQuery reject or read differently. -#}
  {%- set partition_by = dbt_virtual.diff_node_config(model_name, 'diff_partition_by') or dbt_virtual.diff_node_config(model_name, 'partition_by') -%}

  {#- Columns present on both sides, minus exclusions. -#}
  {%- set cand_col_objects = adapter.get_columns_in_relation(cand_rel) -%}
  {%- set cand_cols = cand_col_objects | map(attribute='name') | list -%}
  {%- set base_cols = adapter.get_columns_in_relation(base_rel) | map(attribute='name') | list -%}
  {%- set base_lower = base_cols | map('lower') | list -%}
  {%- set cand_lower = cand_cols | map('lower') | list -%}
  {%- set columns = [] -%}
  {%- for col in cand_cols -%}
    {%- if (col | lower) in base_lower and (col | lower) not in excludes -%}{%- do columns.append(col) -%}{%- endif -%}
  {%- endfor -%}
  {%- set only_candidate_columns = cand_cols | reject('in', base_cols) | list -%}
  {%- set only_target_columns = base_cols | reject('in', cand_cols) | list -%}
  {%- set quoted = [] -%}
  {%- for col in columns -%}{%- do quoted.append(adapter.quote(col)) -%}{%- endfor -%}
  {%- set key_ok = (keys | length) > 0 and (keys | select('in', columns) | list | length) == (keys | length) -%}
  {#- All `keys` are joined on at the leaves. Range bisection needs ONE numeric key: the first
      numeric entry of `keys` (a composite key such as (date, id) bisects on `id`), none when no entry
      is numeric: the differing segments are then joined whole. Column objects without a type
      (no `is_number`) are taken as numeric, the previous behaviour. -#}
  {%- set bisect_pick = [] -%}
  {%- if key_ok -%}
    {%- for k in keys -%}
      {%- if bisect_pick | length == 0 -%}
        {%- for col_obj in cand_col_objects if col_obj.name == k -%}
          {%- if col_obj.is_number is not defined or col_obj.is_number() -%}{%- do bisect_pick.append(k) -%}{%- endif -%}
        {%- endfor -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set bisect_key = bisect_pick[0] if bisect_pick else none -%}
  {%- set bisect_ok = bisect_key is not none -%}
  {%- set key_q = adapter.quote(bisect_key) if bisect_ok else '' -%}

  {#- Segment expression: a partition (date / timestamp / int) or the whole table. -#}
  {%- set seg_expr = '0' -%}
  {%- set seg_kind = 'table' -%}
  {%- set part_date_expr = none -%}
  {%- if partition_by is mapping and partition_by.get('field') -%}
    {%- set pfield = adapter.quote(partition_by.get('field')) -%}
    {%- set ptype = (partition_by.get('data_type') or 'date') | lower -%}
    {%- if ptype == 'date' -%}
      {%- set seg_expr = pfield -%}{%- set seg_kind = 'date' -%}{%- set part_date_expr = pfield -%}
    {%- elif ptype in ['timestamp', 'datetime', 'timestamp_ntz'] -%}
      {%- set seg_expr = dbt_virtual.diff_to_date_sql(pfield) -%}{%- set seg_kind = 'date' -%}{%- set part_date_expr = seg_expr -%}
    {%- elif ptype in ['int64', 'int', 'integer', 'bigint'] -%}
      {%- set seg_expr = pfield -%}{%- set seg_kind = 'int' -%}
    {%- endif -%}
  {%- endif -%}

  {%- set where_parts = [] -%}
  {%- if opts.get('where') -%}{%- do where_parts.append('(' ~ opts.get('where') ~ ')') -%}{%- endif -%}
  {%- if node_where -%}{%- do where_parts.append('(' ~ node_where ~ ')') -%}{%- endif -%}
  {%- if window_days and part_date_expr is not none -%}
    {%- do where_parts.append(dbt_virtual.diff_window_predicate_sql(part_date_expr, window_days)) -%}
  {%- endif -%}
  {%- set base_where = where_parts | join(' AND ') if where_parts else 'TRUE' -%}

  {%- set hash_agg = dbt_virtual.diff_row_hash_agg_sql(quoted) -%}

  {#- Level 0: one grouped query per side. -#}
  {%- set cand_stats = dbt_virtual.diff_fetch_stats(cand_rel, hash_agg, seg_expr, key_q, base_where, counters) -%}
  {%- set base_stats = dbt_virtual.diff_fetch_stats(base_rel, hash_agg, seg_expr, key_q, base_where, counters) -%}
  {%- set segments = dbt_virtual.diff_merge_stats(cand_stats, base_stats) -%}

  {%- set total_candidate = [0] -%}
  {%- set total_target = [0] -%}
  {%- set identical_segments = [0] -%}
  {%- set pending = [] -%}
  {%- for seg in segments -%}
    {%- do total_candidate.append(total_candidate.pop() + seg.cc) -%}
    {%- do total_target.append(total_target.pop() + seg.ct) -%}
    {%- if seg.differs -%}
      {%- if seg_kind == 'table' -%}
        {%- set seg_filter = 'TRUE' -%}{%- set label = 'table' -%}
      {%- elif seg.seg is none -%}
        {%- set seg_filter = seg_expr ~ ' IS NULL' -%}{%- set label = 'partition IS NULL' -%}
      {%- elif seg_kind == 'date' -%}
        {%- set seg_filter = seg_expr ~ " = DATE '" ~ ((seg.seg | string)[:10]) ~ "'" -%}{%- set label = 'partition=' ~ ((seg.seg | string)[:10]) -%}
      {%- else -%}
        {%- set seg_filter = seg_expr ~ ' = ' ~ (seg.seg | string) -%}{%- set label = 'partition=' ~ (seg.seg | string) -%}
      {%- endif -%}
      {%- do pending.append({'label': label, 'filter': seg_filter, 'cc': seg.cc, 'ct': seg.ct, 'lo': seg.lo, 'hi': seg.hi, 'depth': 0, 'forced_leaf': false}) -%}
    {%- else -%}
      {%- do identical_segments.append(identical_segments.pop() + 1) -%}
    {%- endif -%}
  {%- endfor -%}

  {#- Bisection: split the differing segments that are still too big, level by level. -#}
  {%- set threshold = opts.get('threshold') | int -%}
  {%- set branching = opts.get('branching') | int -%}
  {%- set max_depth = opts.get('max_depth') | int -%}
  {%- set state = {'pending': pending} -%}
  {%- set leaves = [] -%}
  {%- set segments_split = [0] -%}
  {%- for level in range(max_depth + 1) -%}
    {%- set nxt = [] -%}
    {%- for seg in state.pending -%}
      {%- set biggest = [seg.cc, seg.ct] | max -%}
      {%- if biggest <= threshold or not bisect_ok or seg.forced_leaf or level >= max_depth or seg.lo is none or seg.hi is none or seg.hi <= seg.lo -%}
        {%- do leaves.append(seg) -%}
      {%- else -%}
        {%- set step = ((seg.hi - seg.lo) | float) / branching -%}
        {%- if step <= 0 -%}
          {%- do leaves.append(seg) -%}
        {%- else -%}
          {%- set bucket = 'LEAST(' ~ (branching - 1) ~ ', CAST(FLOOR((' ~ key_q ~ ' - ' ~ seg.lo ~ ') / ' ~ ('%.12g' | format(step)) ~ ') AS ' ~ dbt_virtual.diff_int_type() ~ '))' -%}
          {%- set seg_where = '(' ~ base_where ~ ') AND (' ~ seg.filter ~ ')' -%}
          {%- do segments_split.append(segments_split.pop() + 1) -%}
          {%- set c_b = dbt_virtual.diff_fetch_stats(cand_rel, hash_agg, bucket, key_q, seg_where, counters) -%}
          {%- set t_b = dbt_virtual.diff_fetch_stats(base_rel, hash_agg, bucket, key_q, seg_where, counters) -%}
          {%- for sub in dbt_virtual.diff_merge_stats(c_b, t_b) -%}
            {%- if sub.differs -%}
              {%- if sub.lo is none or sub.hi is none -%}
                {#- Rows whose key is NULL cannot be ranged: join them directly. -#}
                {%- do nxt.append({'label': seg.label ~ ' key IS NULL', 'filter': seg.filter ~ ' AND ' ~ key_q ~ ' IS NULL', 'cc': sub.cc, 'ct': sub.ct, 'lo': none, 'hi': none, 'depth': level + 1, 'forced_leaf': true}) -%}
              {%- else -%}
                {%- do nxt.append({'label': (seg.label if seg.label != 'table' else '') ~ ' ' ~ bisect_key ~ ' in [' ~ sub.lo ~ ', ' ~ sub.hi ~ ']', 'filter': seg.filter ~ ' AND ' ~ key_q ~ ' >= ' ~ sub.lo ~ ' AND ' ~ key_q ~ ' <= ' ~ sub.hi, 'cc': sub.cc, 'ct': sub.ct, 'lo': sub.lo, 'hi': sub.hi, 'depth': level + 1, 'forced_leaf': false}) -%}
              {%- endif -%}
            {%- endif -%}
          {%- endfor -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
    {%- do state.update({'pending': nxt}) -%}
  {%- endfor -%}

  {#- Leaf join: only the small differing ranges. -#}
  {%- set only_candidate = [0] -%}
  {%- set only_target = [0] -%}
  {%- set changed = [0] -%}
  {%- set non_keys = columns | reject('in', keys) | list if key_ok else [] -%}
  {%- set col_mismatch = {} -%}
  {%- for col in non_keys -%}{%- do col_mismatch.update({col: 0}) -%}{%- endfor -%}
  {%- set leaf_report = [] -%}
  {%- set samples = [] -%}
  {%- if key_ok -%}
    {%- for leaf in leaves -%}
      {%- set leaf_where = '(' ~ base_where ~ ') AND (' ~ leaf.filter ~ ')' -%}
      {%- do counters.update({'join_queries': counters['join_queries'] + 1}) -%}
      {%- set agg = run_query(dbt_virtual.diff_leaf_aggregate_sql(cand_rel, base_rel, columns, keys, leaf_where)) -%}
      {%- set r = agg.rows[0] -%}
      {%- do only_candidate.append(only_candidate.pop() + (r[0] | int)) -%}
      {%- do only_target.append(only_target.pop() + (r[1] | int)) -%}
      {%- do changed.append(changed.pop() + (r[2] | int)) -%}
      {%- for col in non_keys -%}
        {%- do col_mismatch.update({col: col_mismatch[col] + (r[3 + loop.index0] | int)}) -%}
      {%- endfor -%}
      {%- do leaf_report.append({'segment': (leaf.label | trim), 'depth': leaf.depth, 'candidate_rows': leaf.cc, 'target_rows': leaf.ct, 'only_candidate': r[0] | int, 'only_target': r[1] | int, 'changed': r[2] | int}) -%}
      {%- set remaining = (opts.get('sample_limit') | int) - (samples | length) -%}
      {%- if remaining > 0 and ((r[0] | int) + (r[1] | int) + (r[2] | int)) > 0 -%}
        {%- do counters.update({'sample_queries': counters['sample_queries'] + 1}) -%}
        {%- set sres = run_query(dbt_virtual.diff_leaf_sample_sql(cand_rel, base_rel, columns, keys, leaf_where, remaining)) -%}
        {%- for sr in sres.rows -%}
          {%- set key_vals = {} -%}
          {%- for k in keys -%}{%- do key_vals.update({k: sr[1 + loop.index0] | string}) -%}{%- endfor -%}
          {%- set cols_changed = [] -%}
          {%- for col in non_keys -%}
            {%- if (sr[1 + (keys | length) + loop.index0] | int) == 1 -%}{%- do cols_changed.append(col) -%}{%- endif -%}
          {%- endfor -%}
          {%- do samples.append({'kind': sr[0], 'key': key_vals, 'changed_columns': cols_changed}) -%}
        {%- endfor -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set total_rows = [total_candidate[-1], total_target[-1]] | max -%}
  {%- set differing_rows = only_candidate[-1] + only_target[-1] + changed[-1] -%}
  {%- set any_difference = (segments | selectattr('differs') | list | length) > 0 -%}
  {%- if any_difference and not key_ok -%}
    {#- No key to bisect on: the segment counts are only an upper bound. -#}
    {%- set bound_rows = [0] -%}
    {%- for leaf in leaves -%}{%- do bound_rows.append(bound_rows.pop() + ([leaf.cc, leaf.ct] | max)) -%}{%- endfor -%}
    {%- set mismatch_pct = (bound_rows[-1] * 100.0 / total_rows) if total_rows > 0 else 100.0 -%}
  {%- else -%}
    {%- set mismatch_pct = (differing_rows * 100.0 / total_rows) if total_rows > 0 else (100.0 if differing_rows > 0 else 0.0) -%}
  {%- endif -%}

  {{ return({
    'model': model_name,
    'candidate': cand_rel | string,
    'target': base_rel | string,
    'status': ('identical' if not any_difference else 'differs'),
    'drilled': key_ok,
    'key': keys,
    'bisect_key': bisect_key,
    'segment_kind': seg_kind,
    'candidate_rows': total_candidate[-1],
    'target_rows': total_target[-1],
    'segments_total': segments | length,
    'segments_identical': identical_segments[-1],
    'segments_split': segments_split[-1],
    'only_candidate': only_candidate[-1],
    'only_target': only_target[-1],
    'changed': changed[-1],
    'mismatch_pct': (mismatch_pct | round(4)),
    'mismatch_pct_is_upper_bound': (any_difference and not key_ok),
    'column_mismatches': col_mismatch,
    'excluded_columns': excludes,
    'columns_only_in_candidate': only_candidate_columns,
    'columns_only_in_target': only_target_columns,
    'leaf_segments': leaf_report,
    'differing_segments_undrilled': ((leaves | map(attribute='label') | map('trim') | list) if (any_difference and not key_ok) else []),
    'sample': samples,
    'queries': counters
  }) }}
{%- endmacro %}


{% macro virtual_data_diff(from_env='staging', to_env='prod', release_id='', models=none, threshold=10000, branching=16, max_depth=8, sample_limit=10, exclude_columns=none, where='', window_days=none, max_mismatch_pct=none, source_database='', source_schema='') -%}
  {#-
    Row-level diff of the candidate (`release_id`, else `from_env`) against `to_env`, per model.
    Fails (after logging the JSON summary) when a model's mismatch percentage exceeds
    `max_mismatch_pct`. Models only in the candidate are reported as `new`; they have nothing to diff.
    With `physical_locations` each side is read from its own location: the candidate from
    `from_env`'s (or `source_database` / `source_schema`, as `promote` passes for a release that
    is not cloned yet) and `to_env` from its own. DECISION(cross_project_promote_by_clone).
  -#}
  {%- set cfg = dbt_virtual.get_config() -%}
  {%- set phys_schema = cfg.get('physical_schema') -%}
  {%- set cand_loc = dbt_virtual.registry_location_for(from_env, phys_schema, source_database, source_schema) -%}
  {%- set base_loc = dbt_virtual.registry_location_for(to_env, phys_schema) -%}
  {%- set wanted = dbt_virtual.diff_as_list(models) -%}

  {%- if release_id != '' -%}
    {%- set cand_query -%}
      SELECT model_name, physical_identifier
      FROM {{ dbt_virtual.registry_relation(cand_loc['schema'], '_releases', cand_loc['database']) }}
      WHERE release_id = '{{ release_id }}' AND resource_type = 'model'
      ORDER BY model_name ASC
    {%- endset -%}
  {%- else -%}
    {%- set cand_query -%}
      SELECT model_name, physical_identifier
      FROM {{ dbt_virtual.registry_relation(cand_loc['schema'], '_environments', cand_loc['database']) }}
      WHERE environment = '{{ from_env }}' AND resource_type = 'model'
      ORDER BY model_name ASC
    {%- endset -%}
  {%- endif -%}
  {%- set cand_rows = run_query(cand_query) -%}

  {%- set base_query -%}
    SELECT model_name, physical_identifier
    FROM {{ dbt_virtual.registry_relation(base_loc['schema'], '_environments', base_loc['database']) }}
    WHERE environment = '{{ to_env }}' AND resource_type = 'model'
  {%- endset -%}
  {%- set base_rows = run_query(base_query) -%}
  {%- set base_map = {} -%}
  {%- if base_rows -%}
    {%- for brow in base_rows.rows -%}{%- do base_map.update({brow[0]: brow[1]}) -%}{%- endfor -%}
  {%- endif -%}

  {%- set opts = {
    'threshold': threshold, 'branching': branching, 'max_depth': max_depth,
    'sample_limit': sample_limit, 'exclude_columns': exclude_columns, 'where': where,
    'window_days': window_days
  } -%}
  {%- set results = [] -%}
  {%- if not cand_rows or (cand_rows.rows | length) == 0 -%}
    {{ log("dbt-virtual data diff: no candidate models found in " ~ cand_loc['schema'] ~ " (nothing was compared).", info=True) }}
  {%- endif -%}
  {%- if cand_rows -%}
    {%- for row in cand_rows.rows -%}
      {%- set model_name = row[0] -%}
      {%- if (wanted | length) == 0 or model_name in wanted -%}
        {%- set base_ident = base_map.get(model_name, '') -%}
        {%- if base_ident == '' -%}
          {%- do results.append({'model': model_name, 'status': 'new', 'mismatch_pct': 0.0, 'only_candidate': 0, 'only_target': 0, 'changed': 0}) -%}
        {%- elif base_ident == row[1] -%}
          {#- Same physical table on both sides: nothing to compare. -#}
          {%- do results.append({'model': model_name, 'status': 'identical', 'same_physical_table': true, 'mismatch_pct': 0.0, 'only_candidate': 0, 'only_target': 0, 'changed': 0, 'queries': {'checksum_queries': 0, 'join_queries': 0, 'sample_queries': 0}}) -%}
        {%- else -%}
          {%- set cand_rel = api.Relation.create(database=cand_loc['database'], schema=cand_loc['schema'], identifier=row[1].split('.')[-1], type='table') -%}
          {%- set base_rel = api.Relation.create(database=base_loc['database'], schema=base_loc['schema'], identifier=base_ident.split('.')[-1], type='table') -%}
          {%- do results.append(dbt_virtual.diff_model(model_name, cand_rel, base_rel, opts)) -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {%- set failed = [] -%}
  {%- if max_mismatch_pct is not none -%}
    {%- for res in results -%}
      {%- if (res.mismatch_pct | float) > (max_mismatch_pct | float) -%}{%- do failed.append(res.model) -%}{%- endif -%}
    {%- endfor -%}
  {%- endif -%}
  {%- set summary = {
    'from_env': from_env, 'to_env': to_env, 'release_id': release_id,
    'max_mismatch_pct': max_mismatch_pct, 'passed': (failed | length) == 0,
    'failed_models': failed, 'models': results
  } -%}
  {{ log("DBT_VIRTUAL_DIFF_JSON:" ~ tojson(summary), info=True) }}
  {%- if failed | length > 0 -%}
    {{ exceptions.raise_compiler_error("dbt-virtual data diff failed: mismatch exceeds max_mismatch_pct=" ~ max_mismatch_pct ~ "% for " ~ (failed | join(', ')) ~ ".") }}
  {%- endif -%}
  {{ return(summary) }}
{%- endmacro %}


{% macro registry_location_for(env_name, phys_schema, database='', schema='') -%}
  {#- Where `env_name`'s registry and tables live, for a gate that reads another environment than
      the current one (`virtual_audit`, `virtual_data_diff`). Without `physical_locations` and
      without an override it is this run's `{target database, phys_schema}`, as before. -#}
  {%- set here = {'database': dbt_virtual.active_database(), 'schema': phys_schema} -%}
  {%- if not (dbt_virtual.get_config().get('physical_locations') or {}) and database == '' and schema == '' -%}
    {{ return(here) }}
  {%- endif -%}
  {%- set loc = dbt_virtual.physical_location(env_name) if (dbt_virtual.get_config().get('physical_locations') or {}) else here -%}
  {{ return({'database': database or loc['database'], 'schema': schema or loc['schema']}) }}
{%- endmacro %}
