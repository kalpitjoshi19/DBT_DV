{{ config(
    materialized='view',
    schema='BDV'
) }}

WITH latest_satellite AS (
    -- 1. Find the latest (max) load date for each link hash key
    SELECT
        link_part_supplier_hk,
        MAX(load_ts) AS max_load_ts
    FROM {{ ref('link_sat_partsupp') }}
    GROUP BY 1
),
current_satellite AS (
    -- 2. Select the full attribute row matching that latest load date
    SELECT
        s.link_part_supplier_hk,
        s.ps_availqty,
        s.ps_supplycost,
        s.ps_comment,
        s.load_ts AS satellite_load_ts
    FROM {{ ref('link_sat_partsupp') }} AS s
    INNER JOIN latest_satellite AS l
        ON s.link_part_supplier_hk = l.link_part_supplier_hk
        AND s.load_ts = l.max_load_ts
)
-- 3. Join Link to Hubs and current Satellite attributes
SELECT
    l.link_part_supplier_hk,
    
    -- Part Hub Keys and NKs
    hp.part_hk,
    hp.part_nk,
    
    -- Supplier Hub Keys and NKs
    hs.supplier_hk,
    hs.supplier_nk,
    
    -- Satellite Attributes
    s.ps_availqty,
    s.ps_supplycost,
    s.ps_comment,
    s.satellite_load_ts
FROM {{ ref('link_part_supplier') }} AS l
LEFT JOIN current_satellite AS s
    ON l.link_part_supplier_hk = s.link_part_supplier_hk
INNER JOIN {{ ref('hub_part') }} AS hp
    ON l.part_hk = hp.part_hk
INNER JOIN {{ ref('hub_supplier') }} AS hs
    ON l.supplier_hk = hs.supplier_hk