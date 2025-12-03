{{ config(
    materialized='incremental', 
    unique_key='link_order_lineitem_hk',
    schema='RDV'
) }}

WITH source AS (
    SELECT 
        link_order_lineitem_hk,
        order_hk,
        lineitem_hk,
        load_ts,
        rsrc
    FROM {{ ref('stg_lineitem') }}
    WHERE l_orderkey IS NOT NULL    -- 1. Ensure the Order Key is valid.
      AND l_linenumber IS NOT NULL -- 2. Ensure the Lineitem Key is valid.
),
deduped_source AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY link_order_lineitem_hk ORDER BY load_ts) as rn
    FROM source
    WHERE link_order_lineitem_hk IS NOT NULL
),
new_links AS (
    SELECT * FROM deduped_source 
    WHERE rn = 1
    {% if is_incremental() %}
    -- Checks existing Link for new link keys only
    AND link_order_lineitem_hk NOT IN (SELECT link_order_lineitem_hk FROM {{ this }})
    {% endif %}
)
SELECT 
    link_order_lineitem_hk,
    order_hk,
    lineitem_hk,
    load_ts,
    rsrc,
    CURRENT_TIMESTAMP() as load_date
FROM new_links