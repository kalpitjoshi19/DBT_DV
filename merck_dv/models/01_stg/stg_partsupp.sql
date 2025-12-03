{{ config(materialized='table', schema='STG') }}

WITH source AS (
    SELECT * FROM {{ source('tpch_sf1', 'partsupp') }}
),

staged AS (
    SELECT
        ps_partkey,
        ps_suppkey,
        ps_availqty,
        ps_supplycost,
        ps_comment,
        CURRENT_TIMESTAMP() as load_ts,
        'PARTSUPP_SOURCE' AS rsrc
    FROM source
)

SELECT
    staged.*,

    -- Foreign Hub Hash Key: Part (FK: ps_partkey)
    {{ datavault4dbt.hash_columns({
        'part_hk': ['ps_partkey']
    }) }},

    -- Foreign Hub Hash Key: Supplier (FK: ps_suppkey)
    {{ datavault4dbt.hash_columns({
        'supplier_hk': ['ps_suppkey']
    }) }},

    -- Link Hash Key (Composite Hash: part_hk and supplier_hk)
    {{ datavault4dbt.hash_columns({
        'link_part_supplier_hk': ['part_hk', 'supplier_hk']
    }) }},

    -- Satellite Hash Diff (HD)
    {{ datavault4dbt.hash_columns({
        'partsupp_hashdiff': {
            'columns': [
                'ps_availqty',
                'ps_supplycost',
                'ps_comment'
            ],
            'is_hashdiff': true
        }
    }) }}
FROM staged