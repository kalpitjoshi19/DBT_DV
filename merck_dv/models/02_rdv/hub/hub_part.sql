{{ config(
    materialized='incremental', 
    unique_key='part_hk',
    schema='RDV'
) }}

WITH source AS (
    -- Reference the staging model
    SELECT * FROM {{ ref('stg_part') }} 
),
deduped_source AS (
    SELECT 
        part_hk,
        p_partkey, -- Natural Key
        load_ts,
        ROW_NUMBER() OVER (PARTITION BY part_hk ORDER BY load_ts) as rn
    FROM source 
    -- Applying Null Key Management (NKM) filters
    WHERE part_hk IS NOT NULL 
      AND p_partkey IS NOT NULL  -- Reject if the NK is SQL NULL
),
new_parts AS (
    SELECT * FROM deduped_source 
    WHERE rn = 1
    {% if is_incremental() %}
    AND part_hk NOT IN (SELECT part_hk FROM {{ this }})
    {% endif %}
)
SELECT 
    part_hk,
    p_partkey as part_nk,
    load_ts,
    CURRENT_TIMESTAMP() as load_date
FROM new_parts