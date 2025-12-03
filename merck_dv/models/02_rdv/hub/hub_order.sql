{{ config(
    materialized='incremental', 
    unique_key='order_hk',
    schema='RDV'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
deduped_source AS (
    SELECT 
        order_hk,
        o_orderkey as order_nk,
        load_ts,
        rsrc,
        ROW_NUMBER() OVER (PARTITION BY order_hk ORDER BY load_ts, rsrc) as rn
    FROM source 
    WHERE order_hk IS NOT NULL 
      AND o_orderkey IS NOT NULL
),
new_orders AS (
    SELECT * FROM deduped_source 
    WHERE rn = 1
    {% if is_incremental() %}
    -- Checks existing Hub for new keys only
    AND order_hk NOT IN (SELECT order_hk FROM {{ this }})
    {% endif %}
)
SELECT 
    order_hk,
    order_nk,
    load_ts,
    rsrc, 
    CURRENT_TIMESTAMP() as load_date
FROM new_orders