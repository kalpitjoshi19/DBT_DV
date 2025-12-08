{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

{{ dv_staging_filter('stg_supplier', 'S_SUPPKEY') }}