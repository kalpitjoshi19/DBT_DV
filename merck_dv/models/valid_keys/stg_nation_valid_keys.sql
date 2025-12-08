{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

SELECT *
FROM {{ ref('stg_nation') }}
WHERE N_NATIONKEY IS NOT NULL -- Exclude records where the Natural Key is NULL