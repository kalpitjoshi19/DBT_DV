{{ config(
    materialized='view',
    schema='BDV'
) }}

WITH latest_satellite AS (
    -- Find the latest (max) load date for each customer_hk
    SELECT
        customer_hk,
        MAX(load_ts) AS max_load_ts
    FROM {{ ref('sat_customer') }}
    GROUP BY 1
),
current_satellite AS (
    -- Select the full attribute row matching that latest load date
    SELECT
        s.customer_hk,
        s.c_name,
        s.c_address,
        s.c_nationkey,
        s.c_phone,
        s.c_acctbal,
        s.c_mktsegment,
        s.c_comment,
        s.load_ts
    FROM {{ ref('sat_customer') }} AS s
    INNER JOIN latest_satellite AS l
        ON s.customer_hk = l.customer_hk
        AND s.load_ts = l.max_load_ts
)
SELECT
    h.customer_hk,
    h.customer_nk,
    s.c_name,
    s.c_address,
    s.c_nationkey,
    s.c_phone,
    s.c_acctbal,
    s.c_mktsegment,
    s.c_comment,
    s.load_ts AS satellite_load_ts,
    h.load_ts AS hub_load_ts -- Retain Hub Load Date for completeness
FROM {{ ref('hub_customer') }} AS h
LEFT JOIN current_satellite AS s
    ON h.customer_hk = s.customer_hk