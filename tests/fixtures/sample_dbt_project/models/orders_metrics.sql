{{
  config(
    materialized='table',
    virtual_env=true
  )
}}

SELECT
  'store_1' AS customer_id,
  10 AS orders
UNION ALL
SELECT
  'store_2' AS customer_id,
  25 AS orders
