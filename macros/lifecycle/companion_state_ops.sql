{% macro companion_pointer(env, model_name) -%}
  {#- {'fingerprint', 'updated_at'} of the model's pointer in `env`, or none. -#}
  {%- set phys_schema = dbt_virtual.get_config().get('physical_schema') -%}
  {%- set res = run_query(
    "SELECT fingerprint, updated_at FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments')
    ~ " WHERE environment = " ~ dbt_virtual.sql_string_literal(env)
    ~ " AND model_name = " ~ dbt_virtual.sql_string_literal(model_name)
    ~ " AND resource_type = 'model'"
  ) -%}
  {%- if not res or (res.rows | length) == 0 -%}
    {{ return(none) }}
  {%- endif -%}
  {{ return({'fingerprint': res.rows[0][0], 'updated_at': res.rows[0][1]}) }}
{%- endmacro %}


{% macro companion_envs_at(model_name, fp) -%}
  {#- Environments whose pointer for the model is `fp`. -#}
  {%- set phys_schema = dbt_virtual.get_config().get('physical_schema') -%}
  {%- set res = run_query(
    "SELECT environment FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments')
    ~ " WHERE model_name = " ~ dbt_virtual.sql_string_literal(model_name)
    ~ " AND fingerprint = " ~ dbt_virtual.sql_string_literal(fp)
    ~ " AND resource_type = 'model' ORDER BY environment"
  ) -%}
  {%- set envs = [] -%}
  {%- if res -%}
    {%- for r in res.rows -%}
      {%- do envs.append(r[0]) -%}
    {%- endfor -%}
  {%- endif -%}
  {{ return(envs) }}
{%- endmacro %}


{% macro companion_depart(node, env, new_fp) -%}
  {#-
    Call BEFORE `env`'s pointer for the model moves to `new_fp` and before anything overwrites the
    env's state for it. While an environment points at a fingerprint, its own live state rows ARE
    that table's state; this saves them against the old fingerprint so a later promote or rollback
    back to it can restore them. Returns true when the pointer is moving (none, or another
    fingerprint). It does not save twice for one departure: a snapshot newer than the pointer is
    kept, because the live rows may already have been overwritten by a seed.
  -#}
  {%- set model_name = dbt_virtual.node_attr(node, 'name', '') -%}
  {%- set entries = [] -%}
  {%- for entry in dbt_virtual.companion_entries() -%}
    {%- if dbt_virtual.companion_applies(entry, node) -%}
      {%- do entries.append(entry) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if (entries | length) == 0 -%}
    {{ return(false) }}
  {%- endif -%}
  {%- do dbt_virtual.ensure_companion_registry() -%}
  {%- set pointer = dbt_virtual.companion_pointer(env, model_name) -%}
  {%- if pointer is none -%}
    {{ return(true) }}
  {%- endif -%}
  {%- if pointer['fingerprint'] == new_fp -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set phys_schema = dbt_virtual.get_config().get('physical_schema') -%}
  {%- for entry in entries -%}
    {%- set newer = run_query(
      "SELECT 1 FROM " ~ dbt_virtual.registry_relation(phys_schema, '_companion_state')
      ~ " WHERE companion = " ~ dbt_virtual.sql_string_literal(entry['table'])
      ~ " AND model_name = " ~ dbt_virtual.sql_string_literal(model_name)
      ~ " AND fingerprint = " ~ dbt_virtual.sql_string_literal(pointer['fingerprint'])
      ~ " AND recorded_at >= (SELECT MAX(updated_at) FROM " ~ dbt_virtual.registry_relation(phys_schema, '_environments')
      ~ " WHERE environment = " ~ dbt_virtual.sql_string_literal(env)
      ~ " AND model_name = " ~ dbt_virtual.sql_string_literal(model_name) ~ ")"
    ) -%}
    {%- if not newer or (newer.rows | length) == 0 -%}
      {%- set rows = dbt_virtual.companion_read(entry, env, model_name) -%}
      {{ log("dbt-virtual companion [" ~ env ~ "]: saving " ~ (rows | length) ~ " row(s) of " ~ entry['table'] ~ " for " ~ model_name ~ "__" ~ pointer['fingerprint'] ~ " before env moves to " ~ new_fp, info=True) }}
      {%- do dbt_virtual.companion_snapshot_save(entry, model_name, pointer['fingerprint'], env, '', rows) -%}
    {%- endif -%}
  {%- endfor -%}
  {{ return(true) }}
{%- endmacro %}


{% macro restore_companion_state(model_name, fp, env, prefer_env=none) -%}
  {#-
    Makes `env`'s state for the model describe `<model>__<fp>`. Sources, first non-empty wins:
    the live rows of an environment that points at `fp` (`prefer_env` first), then the rows saved
    when an environment last left `fp`. A holder whose table is the same as `env`'s needs nothing.
    All sources empty: the table has no state, so `env`'s rows are deleted. No source at all:
    warns and leaves `env`'s rows alone.
  -#}
  {%- set node = dbt_virtual.virtual_node_by_name(model_name) -%}
  {%- for entry in dbt_virtual.companion_entries() -%}
    {%- if dbt_virtual.companion_applies(entry, node) -%}
      {%- do dbt_virtual.ensure_companion_registry() -%}
      {%- set own = dbt_virtual.companion_relation(entry, env) | string -%}
      {%- set at_fp = dbt_virtual.companion_envs_at(model_name, fp) -%}
      {%- set holders = [] -%}
      {%- if prefer_env is not none and prefer_env in at_fp -%}
        {%- do holders.append(prefer_env) -%}
      {%- endif -%}
      {%- for holder in at_fp -%}
        {%- if holder not in holders -%}
          {%- do holders.append(holder) -%}
        {%- endif -%}
      {%- endfor -%}
      {%- set picked = namespace(rows=none, source='', shared=false, known=false, like_env=none) -%}
      {%- for holder in holders -%}
        {%- if holder != env and picked.rows is none -%}
          {%- if (dbt_virtual.companion_relation(entry, holder) | string) == own -%}
            {%- set picked.shared = true -%}
          {%- else -%}
            {%- set held = dbt_virtual.companion_read(entry, holder, model_name) -%}
            {%- set picked.known = true -%}
            {%- set picked.like_env = holder -%}
            {%- if (held | length) > 0 -%}
              {%- set picked.rows = held -%}
              {%- set picked.source = 'live rows of env ' ~ holder -%}
            {%- endif -%}
          {%- endif -%}
        {%- endif -%}
      {%- endfor -%}
      {%- if picked.rows is none and not picked.shared -%}
        {%- set saved = dbt_virtual.companion_snapshot_load(entry, model_name, fp) -%}
        {%- if saved is not none and ((saved | length) > 0 or not picked.known) -%}
          {%- set picked.rows = saved -%}
          {%- set picked.source = 'rows saved when an environment left it' -%}
        {%- endif -%}
      {%- endif -%}
      {%- if picked.rows is none and picked.known -%}
        {%- set picked.rows = [] -%}
        {%- set picked.source = 'no state (empty)' -%}
      {%- endif -%}
      {%- if picked.rows is not none -%}
        {{ log("dbt-virtual companion [" ~ env ~ "]: " ~ model_name ~ " in " ~ entry['table'] ~ " <- " ~ picked.source ~ " of " ~ model_name ~ "__" ~ fp ~ " (" ~ (picked.rows | length) ~ " row(s))", info=True) }}
        {%- do dbt_virtual.companion_write(entry, env, model_name, picked.rows, like_env=picked.like_env) -%}
      {%- elif not picked.shared -%}
        {{ log("dbt-virtual WARNING: no state is known for " ~ model_name ~ "__" ~ fp ~ " in " ~ entry['table'] ~ " (no environment points at it and nothing was saved), so env " ~ env ~ " keeps whatever it holds for " ~ model_name ~ ".", info=True) }}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}
{%- endmacro %}


{% macro companion_full_build(node, env) -%}
  {#-
    Called when a tracked model's table is about to be built from scratch (no clone source, or
    `--full-refresh`). If `env` already holds state for the model, that state describes a table
    that is not the one being built: the build would see only the window after it and silently
    drop everything before. Returns true when it did (or would have) conflict.
    `companion_on_full_build: reset` deletes the rows, so the package backfills like it does for a
    new model; the default `error` is raised by the caller for all models at once.
  -#}
  {%- set model_name = dbt_virtual.node_attr(node, 'name', '') -%}
  {%- set conflicts = [] -%}
  {%- for entry in dbt_virtual.companion_entries() -%}
    {%- if dbt_virtual.companion_applies(entry, node) and (dbt_virtual.companion_read(entry, env, model_name) | length) > 0 -%}
      {%- do conflicts.append(entry) -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if (conflicts | length) > 0 and dbt_virtual.get_config().get('companion_on_full_build', 'error') == 'reset' -%}
    {%- for entry in conflicts -%}
      {{ log("dbt-virtual companion [" ~ env ~ "]: " ~ model_name ~ " is built in full, deleting its rows from " ~ entry['table'] ~ " so it backfills.", info=True) }}
      {%- do dbt_virtual.companion_delete(entry, env, model_name) -%}
    {%- endfor -%}
    {{ return(false) }}
  {%- endif -%}
  {{ return((conflicts | length) > 0) }}
{%- endmacro %}


{% macro raise_companion_full_build(model_names, env) -%}
  {%- set names = model_names | sort -%}
  {{ exceptions.raise_compiler_error(
    "dbt-virtual: " ~ (names | length) ~ " tracked incremental model(s) (" ~ (names[:8] | join(', ')) ~ (', ...' if (names | length) > 8 else '')
    ~ ") would be built from scratch in env '" ~ env ~ "' while its companion state (companion_state in vars.dbt_virtual) "
    ~ "says an earlier version already consumed the input. A full build would then read only the window after "
    ~ "that state and silently lose the history. Set vars.dbt_virtual.companion_on_full_build to 'reset' to "
    ~ "delete that state so the package backfills the table, or seed the table by clone from the base environment."
  ) }}
{%- endmacro %}
