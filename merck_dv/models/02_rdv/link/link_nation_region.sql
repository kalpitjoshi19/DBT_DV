{{ config(
    materialized='incremental',
    unique_key='link_nation_region_hk',
    schema='RDV'
) }}

WITH source AS (
    SELECT
        nation_hk,
        region_hk,
        n_nationkey,    -- Nation NK (for filtering)
        n_regionkey,    -- Region FK (for filtering)
        load_ts,
        rsrc
    FROM {{ ref('stg_nation') }}
),
-- Second CTE to calculate the Link Hash Key cleanly
link_key_source AS (
    SELECT
        -- Calculate the composite Link Hash Key using the two parent keys
        {{ datavault4dbt.hash_columns({
            'link_nation_region_hk': ['nation_hk', 'region_hk']
        }) }},
        source.*
    FROM source
),
deduped_source AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY link_nation_region_hk ORDER BY load_ts) as rn
    FROM link_key_source
    -- CRITICAL INTEGRITY FILTERING (NKM on Link)
    WHERE nation_hk IS NOT NULL
      AND region_hk IS NOT NULL
      AND n_nationkey IS NOT NULL
      AND n_regionkey IS NOT NULL
      AND link_nation_region_hk IS NOT NULL
),
new_links AS (
    SELECT * FROM deduped_source
    WHERE rn = 1
    {% if is_incremental() %}
    AND link_nation_region_hk NOT IN (SELECT link_nation_region_hk FROM {{ this }})
    {% endif %}
)
SELECT
    link_nation_region_hk,
    nation_hk,
    region_hk,
    load_ts,
    rsrc,
    CURRENT_TIMESTAMP() as load_date
FROM new_links