{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

{{ dv_staging_filter('stg_customer', 'C_CUSTKEY') }}