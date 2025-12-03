{{ config(
    materialized='view',
    schema='BDV'
) }}

WITH latest_satellite AS (
    -- 1. Find the latest (max) load date for each order_hk
    SELECT
        order_hk,
        MAX(load_ts) AS max_load_ts
    FROM {{ ref('sat_order') }}
    GROUP BY 1
),
current_satellite AS (
    -- 2. Select the full attribute row matching that latest load date
    SELECT
        s.order_hk,
        -- ** ALL ATTRIBUTES FROM SAT_ORDER ARE INCLUDED HERE **
        s.o_custkey,
        s.o_orderstatus,
        s.o_totalprice,
        s.o_orderdate,
        s.o_orderpriority,
        s.o_clerk,
        s.o_shippriority,
        s.o_comment,
        s.load_ts AS satellite_load_ts
    FROM {{ ref('sat_order') }} AS s
    INNER JOIN latest_satellite AS l
        ON s.order_hk = l.order_hk
        AND s.load_ts = l.max_load_ts -- <= This is the key filtering step
)
-- 3. Final join to Hub to get the Natural Key and combine with current attributes
SELECT
    h.order_hk,
    h.order_nk,
    -- ** ALL ATTRIBUTES FROM CURRENT_SATELLITE ARE INCLUDED HERE **
    s.o_custkey,
    s.o_orderstatus,
    s.o_totalprice,
    s.o_orderdate,
    s.o_orderpriority,
    s.o_clerk,
    s.o_shippriority,
    s.o_comment,
    s.satellite_load_ts
FROM {{ ref('hub_order') }} AS h
LEFT JOIN current_satellite AS s
    ON h.order_hk = s.order_hk