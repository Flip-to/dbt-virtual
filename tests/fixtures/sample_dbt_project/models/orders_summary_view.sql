{{
  config(
    materialized='view',
    virtual_env=false
  )
}}

SELECT
  customer_id,
  orders,
  orders * 2 AS doubled_orders
FROM {{ ref('orders_metrics') }}
WHERE orders >= 15
