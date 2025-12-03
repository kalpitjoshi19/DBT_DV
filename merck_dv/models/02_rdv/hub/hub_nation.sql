{{ config(
    materialized='incremental',
    unique_key='nation_hk',
    schema='RDV'
) }}

WITH source AS (
    -- Reference the staging model
    SELECT * FROM {{ ref('stg_nation') }}
),
deduped_source AS (
    SELECT
        nation_hk,
        n_nationkey, -- Natural Key
        load_ts,
        ROW_NUMBER() OVER (PARTITION BY nation_hk ORDER BY load_ts) as rn
    FROM source
    -- Applying Null Key Management (NKM) filters
    WHERE nation_hk IS NOT NULL
      AND n_nationkey IS NOT NULL  -- Reject if the NK is SQL NULL
),
new_nations AS (
    SELECT * FROM deduped_source
    WHERE rn = 1
    {% if is_incremental() %}
    AND nation_hk NOT IN (SELECT nation_hk FROM {{ this }})
    {% endif %}
)
SELECT
    nation_hk,
    n_nationkey as nation_nk,
    load_ts,
    CURRENT_TIMESTAMP() as load_date
FROM new_nations