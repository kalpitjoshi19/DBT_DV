{{ config(materialized='table', schema='STG') }}

WITH source AS (
    SELECT * FROM {{ source('tpch_sf1', 'part') }}
),

staged AS (
    SELECT
        p_partkey,
        p_name,
        p_mfgr,
        p_brand,
        p_type,
        p_size,
        p_container,
        p_retailprice,
        p_comment,
        CURRENT_TIMESTAMP() as load_ts,
        'PART_SOURCE' AS rsrc
    FROM source
)

SELECT
    staged.*,

    -- Hub Hash Key for the Part (NK: p_partkey) - Standard Macro Call
    {{ datavault4dbt.hash_columns({
        'part_hk': ['p_partkey']
    }) }},

    -- Satellite Hash Diff (HD)
    {{ datavault4dbt.hash_columns({
        'part_hashdiff': {
            'columns': [
                'p_name',
                'p_mfgr',
                'p_brand',
                'p_type',
                'p_size',
                'p_container',
                'p_retailprice',
                'p_comment'
            ],
            'is_hashdiff': true
        }
    }) }}
FROM staged