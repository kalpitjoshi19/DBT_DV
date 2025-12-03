{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey = 'customer_hk',
    src_hashdiff = 'customer_hashdiff',
    src_payload = [
        'c_name',
        'c_address',
        'c_phone',
        'c_acctbal',
        'c_mktsegment',
        'c_comment',
        'c_nationkey'
    ],
    src_ldts = 'load_ts',
    src_rsrc = 'rsrc',
    source_model = 'stg_customer_valid_keys'
) }}