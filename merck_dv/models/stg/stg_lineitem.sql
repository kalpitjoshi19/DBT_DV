{{ config(materialized='table', schema='STG') }}

WITH source AS (
    SELECT * FROM {{ source('tpch_sf1', 'lineitem') }}
),

staged AS (
    SELECT
        l_orderkey,
        l_partkey,
        l_suppkey,
        l_linenumber,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_returnflag,
        l_linestatus,
        l_shipdate,
        l_commitdate,
        l_receiptdate,
        l_shipinstruct,
        l_shipmode,
        l_comment,
        CURRENT_TIMESTAMP() as load_ts,
        'LINEITEM_SOURCE' AS rsrc
    FROM source
),

hashed AS (
    SELECT 
        staged.*,
        
        -- Hub Hash Key for the Lineitem (Composite NK: l_orderkey, l_linenumber) - DICTIONARY SYNTAX
        {{ datavault4dbt.hash_columns({
            'lineitem_hk': {
                'columns': ['l_orderkey', 'l_linenumber'], 
                'is_hashdiff': false
            }
        }) }},
        
        -- Parent Hub Hash Key (Order) - DICTIONARY SYNTAX
        {{ datavault4dbt.hash_columns({
            'order_hk': {
                'columns': ['l_orderkey'], 
                'is_hashdiff': false
            }
        }) }},

        -- Link Hash Key (Hash of Order HK and Lineitem HK) - DICTIONARY SYNTAX
        {{ datavault4dbt.hash_columns({
            'link_order_lineitem_hk': {
                'columns': ['order_hk', 'lineitem_hk'], 
                'is_hashdiff': false
            }
        }) }},

        -- Satellite Hash Diff (HD) - DICTIONARY SYNTAX
        {{ datavault4dbt.hash_columns({
            'lineitem_hashdiff': {
                'columns': [
                    'l_partkey', 'l_suppkey', 'l_quantity', 'l_extendedprice', 
                    'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 
                    'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 
                    'l_shipmode', 'l_comment'
                ], 
                'is_hashdiff': true
            }
        }) }}
    FROM staged
)

SELECT * FROM hashed