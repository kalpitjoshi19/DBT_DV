{{ config(
    materialized='incremental', 
    unique_key='customer_hk',
    schema='RDV'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_customer') }}
),
deduped_source AS (
    SELECT 
        customer_hk,
        c_custkey,
        load_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_hk ORDER BY load_ts) as rn
    FROM source 
    WHERE customer_hk IS NOT NULL 
      AND c_custkey IS NOT NULL
),
new_customers AS (
    SELECT * FROM deduped_source 
    WHERE rn = 1
    {% if is_incremental() %}
    AND customer_hk NOT IN (SELECT customer_hk FROM {{ this }})
    {% endif %}
)
SELECT 
    customer_hk,
    c_custkey as customer_nk,
    load_ts,
    CURRENT_TIMESTAMP() as first_seen_ts
FROM new_customers
