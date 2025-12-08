{{ config(
    materialized='incremental', 
    unique_key='link_customer_order_hk',
    schema='RDV'
) }}

WITH source AS (
    SELECT 
        link_customer_order_hk,
        customer_hk,
        order_hk,
        load_ts,
        rsrc
    FROM {{ ref('stg_orders') }}
    WHERE o_orderkey IS NOT NULL    -- 1. Must ensure the Customer Key is valid.
      AND o_custkey IS NOT NULL    -- 2. Must ensure the Order Key is valid.
),
deduped_source AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY link_customer_order_hk ORDER BY load_ts) as rn
    FROM source
    WHERE link_customer_order_hk IS NOT NULL
),
new_links AS (
    SELECT * FROM deduped_source 
    WHERE rn = 1
    {% if is_incremental() %}
    -- Checks existing Link for new link keys only
    AND link_customer_order_hk NOT IN (SELECT link_customer_order_hk FROM {{ this }})
    {% endif %}
)
SELECT 
    link_customer_order_hk,
    customer_hk,
    order_hk,
    load_ts,
    rsrc,
    CURRENT_TIMESTAMP() as first_seen_ts
FROM new_links