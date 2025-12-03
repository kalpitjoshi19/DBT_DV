{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey = 'supplier_hk',
    src_hashdiff = 'supplier_hashdiff',
    src_payload = [
        's_name',
        's_address',
        's_nationkey',
        's_phone',
        's_acctbal',
        's_comment'
    ],
    src_ldts = 'load_ts',
    src_rsrc = 'rsrc',
    source_model = 'stg_supplier_valid_keys'
) }}