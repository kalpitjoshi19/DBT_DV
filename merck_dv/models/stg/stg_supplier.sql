{{ config(materialized='table', schema='STG') }}

WITH source AS (
    SELECT * FROM {{ source('tpch_sf1', 'supplier') }}
),

staged AS (
    SELECT
        s_suppkey,
        s_name,
        s_address,
        s_nationkey,
        s_phone,
        s_acctbal,
        s_comment,
        CURRENT_TIMESTAMP() as load_ts,
        'SUPPLIER_SOURCE' AS rsrc
    FROM source
)

SELECT
    staged.*,

    -- Hub Hash Key for the Supplier (NK: s_suppkey) - Standard Macro Call
    {{ datavault4dbt.hash_columns({
        'supplier_hk': ['s_suppkey']
    }) }},

    -- Satellite Hash Diff (HD)
    {{ datavault4dbt.hash_columns({
        'supplier_hashdiff': {
            'columns': [
                's_name',
                's_address',
                's_nationkey',
                's_phone',
                's_acctbal',
                's_comment'
            ],
            'is_hashdiff': true
        }
    }) }}
FROM staged