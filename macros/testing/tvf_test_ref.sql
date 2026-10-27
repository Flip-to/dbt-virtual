{% macro tvf_test_ref(model_name) -%}
  {#-
    Fixture injection helper for BigQuery Table Functions (TVFs).
    In normal/prod mode (`var('tvf_test_fixtures', false)` is false): delegates to `ref(model_name)`.
    In test compile mode (`--vars '{tvf_test_fixtures: true}'`): emits `<model_name>__fixture` so the
    compiled query can be wrapped in `CREATE OR REPLACE TABLE FUNCTION _test_<tvf>(<model_name>__fixture ANY TABLE, ...)`.
  -#}
  {%- if var('tvf_test_fixtures', false) -%}
    {{ return(model_name ~ '__fixture') }}
  {%- else -%}
    {{ return(ref(model_name)) }}
  {%- endif -%}
{%- endmacro %}
