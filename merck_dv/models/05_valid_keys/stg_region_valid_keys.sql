{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

SELECT *
FROM {{ ref('stg_region') }}
WHERE R_REGIONKEY IS NOT NULL -- Exclude records where the Natural Key is NULL