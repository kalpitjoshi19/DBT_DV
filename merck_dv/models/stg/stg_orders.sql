{{ config(materialized='table', schema='STG') }}

WITH source AS (
    SELECT * FROM {{ source('tpch_sf1', 'orders') }}
),

staged AS (
    SELECT
        o_orderkey,
        o_custkey,
        o_orderstatus,
        o_totalprice,
        o_orderdate,
        o_orderpriority,
        o_clerk,
        o_shippriority,
        o_comment,
        CURRENT_TIMESTAMP() as load_ts,
        'ORDER_SOURCE' AS rsrc 
    FROM source
),

hashed AS (
    SELECT 
        staged.*,
        -- Hub Hash Key for the Order (NK: o_orderkey)
        {{ datavault4dbt.hash_columns({
            'order_hk': ['o_orderkey']
        }) }},
        -- Link Hash Key for the Customer (NK: o_custkey) - NEEDED FOR LINK MODEL
        {{ datavault4dbt.hash_columns({
            'customer_hk': ['o_custkey']
        }) }},
        -- ADDED: Link Hash Key (Hash of all Foreign Keys)
        {{ datavault4dbt.hash_columns({
            'link_customer_order_hk': {
                'columns': ['customer_hk', 'order_hk']
            }
        }) }},
        -- Satellite Hash Diff (HD) for the Order's descriptive attributes
        {{ datavault4dbt.hash_columns({
            'order_hashdiff': {
                'columns': [
                    'o_custkey', 
                    'o_orderstatus', 
                    'o_totalprice', 
                    'o_orderdate', 
                    'o_orderpriority', 
                    'o_clerk', 
                    'o_shippriority', 
                    'o_comment'
                ], 
                'is_hashdiff': true
            }
        }) }}
    FROM staged
)

SELECT * FROM hashed