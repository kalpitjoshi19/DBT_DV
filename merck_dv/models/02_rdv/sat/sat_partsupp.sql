{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey = 'link_part_supplier_hk',
    src_hashdiff = 'partsupp_hashdiff',
    src_payload = [
        'ps_availqty',
        'ps_supplycost',
        'ps_comment'
    ],
    src_ldts = 'load_ts',
    src_rsrc = 'rsrc',
    source_model = 'stg_partsupp_valid_keys'
) }}