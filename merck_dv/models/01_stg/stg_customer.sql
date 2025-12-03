{{ config(materialized='table', schema='STG') }}

WITH source AS (
    SELECT * FROM {{ source('tpch_sf1', 'customer') }}
),

staged AS (
    SELECT
        c_custkey,
        c_name,
        c_address,
        c_nationkey,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment,
        CURRENT_TIMESTAMP() as load_ts, -- Load Timestamp
        'CUSTOMER_SOURCE' AS rsrc       -- Record Source
    FROM source
),

hashed AS (
    SELECT 
        staged.*,
        -- Calculate the Hub Hash Key (HK) using the Natural Key (NK)
        {{ datavault4dbt.hash_columns({
            'customer_hk': ['c_custkey']
        }) }},
        -- Calculate the Satellite Hash Diff (HD) using all descriptive columns (Payload)
        {{ datavault4dbt.hash_columns({
            'customer_hashdiff': {
                'columns': ['c_name', 'c_address', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment', 'c_nationkey'], 
                'is_hashdiff': true
            }
        }) }}
    FROM staged
)

SELECT * FROM hashed