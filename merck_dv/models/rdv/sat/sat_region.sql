{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey = 'region_hk',
    src_hashdiff = 'region_hashdiff',
    src_payload = [
        'r_name',
        'r_comment'
    ],
    src_ldts = 'load_ts',
    src_rsrc = 'rsrc',
    source_model = 'stg_region_valid_keys'
) }}