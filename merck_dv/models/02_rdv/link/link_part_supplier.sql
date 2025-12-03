{{ config(
    materialized='incremental', 
    unique_key='link_part_supplier_hk',
    schema='RDV'
) }}

WITH source AS (
    SELECT 
        link_part_supplier_hk,
        part_hk,        -- Foreign Key 1
        supplier_hk,    -- Foreign Key 2
        ps_partkey,     -- Natural Key 1 (for filtering)
        ps_suppkey,     -- Natural Key 2 (for filtering)
        load_ts,
        rsrc
    FROM {{ ref('stg_partsupp') }}
),
deduped_source AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY link_part_supplier_hk ORDER BY load_ts) as rn
    FROM source
    -- CRITICAL INTEGRITY FILTERING (NKM on Link)
    WHERE part_hk IS NOT NULL     -- Reject if Part Hash Key is null hash
      AND supplier_hk IS NOT NULL -- Reject if Supplier Hash Key is null hash
      AND ps_partkey IS NOT NULL  -- Reject if Part NK is SQL NULL
      AND ps_suppkey IS NOT NULL  -- Reject if Supplier NK is SQL NULL
      AND link_part_supplier_hk IS NOT NULL -- Safety check on the Link Hash Key
),
new_links AS (
    SELECT * FROM deduped_source 
    WHERE rn = 1
    {% if is_incremental() %}
    -- Checks existing Link for new link keys only
    AND link_part_supplier_hk NOT IN (SELECT link_part_supplier_hk FROM {{ this }})
    {% endif %}
)
SELECT 
    link_part_supplier_hk,
    part_hk,
    supplier_hk,
    load_ts,
    rsrc,
    CURRENT_TIMESTAMP() as load_date
FROM new_links