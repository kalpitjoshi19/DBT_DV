{{ config(materialized='table', schema='STG') }}

WITH source AS (
    SELECT * FROM {{ source('tpch_sf1', 'nation') }}
),

staged AS (
    SELECT
        n_nationkey,
        n_name,
        n_regionkey, -- Foreign Key
        n_comment,
        CURRENT_TIMESTAMP() as load_ts,
        'NATION_SOURCE' AS rsrc
    FROM source
)

SELECT
    staged.*,

    -- Hub Hash Key for the Nation (NK: n_nationkey)
    {{ datavault4dbt.hash_columns({
        'nation_hk': ['n_nationkey']
    }) }},

    -- Foreign Hub Hash Key: Region (FK: n_regionkey)
    -- This is crucial for linking Nation back to its Region Hub.
    {{ datavault4dbt.hash_columns({
        'region_hk': ['n_regionkey']
    }) }},

    -- Satellite Hash Diff (HD)
    {{ datavault4dbt.hash_columns({
        'nation_hashdiff': {
            'columns': [
                'n_name',
                'n_comment'
            ],
            'is_hashdiff': true
        }
    }) }}
FROM staged