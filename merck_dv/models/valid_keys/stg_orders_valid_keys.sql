{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

{{ dv_staging_filter('stg_orders', 'O_ORDERKEY') }}