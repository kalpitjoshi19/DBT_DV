{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

{{ dv_staging_filter('stg_partsupp', 'PS_PARTKEY') }}