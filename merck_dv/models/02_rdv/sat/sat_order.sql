{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey = 'order_hk',
    src_hashdiff = 'order_hashdiff',
    src_payload = [
        'o_custkey',
        'o_orderstatus',
        'o_totalprice',
        'o_orderdate',
        'o_orderpriority',
        'o_clerk',
        'o_shippriority',
        'o_comment'
    ],
    src_ldts = 'load_ts',
    src_rsrc = 'rsrc',
    source_model = 'stg_orders_valid_keys'
) }}