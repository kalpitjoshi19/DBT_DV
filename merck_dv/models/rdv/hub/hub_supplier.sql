{{ config(
    materialized='incremental', 
    unique_key='supplier_hk',
    schema='RDV'
) }}

WITH source AS (
    -- Reference the staging model
    SELECT * FROM {{ ref('stg_supplier') }}
),
deduped_source AS (
    SELECT 
        supplier_hk,
        s_suppkey, -- Natural Key
        load_ts,
        ROW_NUMBER() OVER (PARTITION BY supplier_hk ORDER BY load_ts) as rn
    FROM source 
    -- Applying Null Key Management (NKM) filters
    WHERE supplier_hk IS NOT NULL 
      AND s_suppkey IS NOT NULL  -- Reject if the NK is SQL NULL
),
new_suppliers AS (
    SELECT * FROM deduped_source 
    WHERE rn = 1
    {% if is_incremental() %}
    AND supplier_hk NOT IN (SELECT supplier_hk FROM {{ this }})
    {% endif %}
)
SELECT 
    supplier_hk,
    s_suppkey as supplier_nk,
    load_ts,
    CURRENT_TIMESTAMP() as first_seen_ts
FROM new_suppliers