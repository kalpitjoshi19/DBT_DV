{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

{{ dv_staging_filter('stg_lineitem', 'L_ORDERKEY') }}