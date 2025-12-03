{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey = 'part_hk',
    src_hashdiff = 'part_hashdiff',
    src_payload = [
        'p_name',
        'p_mfgr',
        'p_brand',
        'p_type',
        'p_size',
        'p_container',
        'p_retailprice',
        'p_comment'
    ],
    src_ldts = 'load_ts',
    src_rsrc = 'rsrc',
    source_model = 'stg_part_valid_keys' 
) }}