{{ config(
    materialized='incremental',
    unique_key='region_hk',
    schema='RDV'
) }}

WITH source AS (
    -- Reference the staging model
    SELECT * FROM {{ ref('stg_region') }}
),
deduped_source AS (
    SELECT
        region_hk,
        r_regionkey, -- Natural Key
        load_ts,
        ROW_NUMBER() OVER (PARTITION BY region_hk ORDER BY load_ts) as rn
    FROM source
    -- Applying Null Key Management (NKM) filters
    WHERE region_hk IS NOT NULL
      AND r_regionkey IS NOT NULL  -- Reject if the NK is SQL NULL
),
new_regions AS (
    SELECT * FROM deduped_source
    WHERE rn = 1
    {% if is_incremental() %}
    AND region_hk NOT IN (SELECT region_hk FROM {{ this }})
    {% endif %}
)
SELECT
    region_hk,
    r_regionkey as region_nk,
    load_ts,
    CURRENT_TIMESTAMP() as load_date
FROM new_regions    