{{ config(
    materialized='view',
    schema='BDV'
) }}

WITH latest_satellite AS (
    -- 1. Find the latest (max) load date for each lineitem_hk
    SELECT
        lineitem_hk,
        MAX(load_ts) AS max_load_ts
    FROM {{ ref('sat_lineitem_details') }}
    GROUP BY 1
),
current_satellite AS (
    -- 2. Select the full attribute row matching that latest load date
    SELECT
        s.lineitem_hk,
        s.l_partkey,   -- Foreign Key Natural Key (or HK) for Part
        s.l_suppkey,   -- Foreign Key Natural Key (or HK) for Supplier
        s.l_quantity,
        s.l_extendedprice,
        s.l_discount,
        s.l_tax,
        s.l_returnflag,
        s.l_linestatus,
        s.l_shipdate,
        s.l_commitdate,
        s.l_receiptdate,
        s.l_shipinstruct,
        s.l_shipmode,
        s.l_comment,
        s.load_ts AS satellite_load_ts
    FROM {{ ref('sat_lineitem_details') }} AS s
    INNER JOIN latest_satellite AS l
        ON s.lineitem_hk = l.lineitem_hk
        AND s.load_ts = l.max_load_ts
)
-- 3. Final join to Hub to expose keys and current attributes
SELECT
    h.lineitem_hk,
    h.lineitem_nk,
    
    -- ** FIX: Extract L_ORDERKEY from the composite LINEITEM_NK string **
    CAST(SPLIT_PART(h.lineitem_nk, '-', 1) AS NUMBER) AS l_orderkey,
    
    -- ** FIX: Extract L_LINENUMBER from the composite LINEITEM_NK string **
    CAST(SPLIT_PART(h.lineitem_nk, '-', 2) AS NUMBER) AS l_linenumber,
    
    -- Foreign Keys for Part and Supplier (from the Satellite payload)
    s.l_partkey,
    s.l_suppkey,
    
    -- Attributes (from the Satellite payload)
    s.l_quantity,
    s.l_extendedprice,
    s.l_discount,
    s.l_tax,
    s.l_returnflag,
    s.l_linestatus,
    s.l_shipdate,
    s.l_commitdate,
    s.l_receiptdate,
    s.l_shipinstruct,
    s.l_shipmode,
    s.l_comment,
    s.satellite_load_ts
FROM {{ ref('hub_lineitem') }} AS h
LEFT JOIN current_satellite AS s
    ON h.lineitem_hk = s.lineitem_hk