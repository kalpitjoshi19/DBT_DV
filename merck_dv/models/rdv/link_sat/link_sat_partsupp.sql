{{
    config(
        materialized='incremental',
        schema='DBT_RDV',
        unique_key=['LINK_PART_SUPPLIER_HK', 'LOAD_TS'],
        is_sat=true
    )
}}

{%- set source_model = 'stg_partsupp' -%}
{%- set src_pk = 'LINK_PART_SUPPLIER_HK' -%}
{%- set src_hashdiff = 'PARTSUPP_HASHDIFF' -%}
{%- set src_payload = ['PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'] -%}
{%- set src_ldts = 'LOAD_TS' -%}
{%- set src_rsrc = 'RSRC' -%} 

{{
    datavault4dbt.sat_v0( 
        parent_hashkey=src_pk, 
        src_hashdiff=src_hashdiff,
        src_payload=src_payload,
        src_ldts=src_ldts,
        src_rsrc=src_rsrc,
        source_model=source_model
    )
}}