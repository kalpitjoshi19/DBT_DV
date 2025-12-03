{{ config(materialized='table', schema='STG') }}

WITH source AS (
    SELECT * FROM {{ source('tpch_sf1', 'region') }}
),

staged AS (
    SELECT
        r_regionkey,
        r_name,
        r_comment,
        CURRENT_TIMESTAMP() as load_ts,
        'REGION_SOURCE' AS rsrc
    FROM source
)

SELECT
    staged.*,

    -- Hub Hash Key for the Region (NK: r_regionkey)
    {{ datavault4dbt.hash_columns({
        'region_hk': ['r_regionkey']
    }) }},

    -- Satellite Hash Diff (HD)
    {{ datavault4dbt.hash_columns({
        'region_hashdiff': {
            'columns': [
                'r_name',
                'r_comment'
            ],
            'is_hashdiff': true
        }
    }) }}
FROM staged