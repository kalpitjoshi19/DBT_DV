{{ config(
    materialized = 'incremental',
    schema = 'RDV'
) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey = 'nation_hk',
    src_hashdiff = 'nation_hashdiff',
    src_payload = [
        'n_name',
        'n_comment'
    ],
    src_ldts = 'load_ts',
    src_rsrc = 'rsrc',
    source_model = 'stg_nation_valid_keys'
) }}