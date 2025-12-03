{{ config(
    materialized='view',
    schema='BDV'
) }}

WITH latest_satellite AS (
    SELECT
        supplier_hk,
        MAX(load_ts) AS max_load_ts
    FROM {{ ref('sat_supplier') }}
    GROUP BY 1
),
current_satellite AS (
    SELECT
        s.supplier_hk,
        s.s_name,
        s.s_address,
        s.s_nationkey,
        s.s_phone,
        s.s_acctbal,
        s.s_comment
    FROM {{ ref('sat_supplier') }} AS s
    INNER JOIN latest_satellite AS l
        ON s.supplier_hk = l.supplier_hk
        AND s.load_ts = l.max_load_ts
)
SELECT
    h.supplier_hk,
    h.supplier_nk,
    s.s_name,
    s.s_address,
    s.s_nationkey,
    s.s_phone,
    s.s_acctbal,
    s.s_comment,
    s.supplier_hk AS satellite_hk
FROM {{ ref('hub_supplier') }} AS h
LEFT JOIN current_satellite AS s
    ON h.supplier_hk = s.supplier_hk