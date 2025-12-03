{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey = 'lineitem_hk',
    src_hashdiff = 'lineitem_hashdiff',
    src_payload = [
        'l_partkey', 
        'l_suppkey', 
        'l_quantity', 
        'l_extendedprice', 
        'l_discount', 
        'l_tax', 
        'l_returnflag', 
        'l_linestatus', 
        'l_shipdate', 
        'l_commitdate', 
        'l_receiptdate', 
        'l_shipinstruct', 
        'l_shipmode', 
        'l_comment'
    ],
    src_ldts = 'load_ts',
    src_rsrc = 'rsrc',
    source_model = 'stg_lineitem_valid_keys'
) }}