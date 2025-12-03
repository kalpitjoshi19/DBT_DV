{{ config(
    materialized='view',
    schema='BDV'
) }}

WITH current_nation_sat AS (
    -- 1. Get current Nation attributes
    SELECT
        s.nation_hk,
        s.n_name,
        s.n_comment
    FROM {{ ref('sat_nation') }} AS s
    INNER JOIN (SELECT nation_hk, MAX(load_ts) AS max_load_ts FROM {{ ref('sat_nation') }} GROUP BY 1) AS l
        ON s.nation_hk = l.nation_hk AND s.load_ts = l.max_load_ts
),
current_region_sat AS (
    -- 2. Get current Region attributes
    SELECT
        s.region_hk,
        s.r_name,
        s.r_comment
    FROM {{ ref('sat_region') }} AS s
    INNER JOIN (SELECT region_hk, MAX(load_ts) AS max_load_ts FROM {{ ref('sat_region') }} GROUP BY 1) AS l
        ON s.region_hk = l.region_hk AND s.load_ts = l.max_load_ts
),
consolidated_geo AS (
    -- 3. Join Hub Nation to Link Nation/Region
    SELECT
        hn.nation_hk,
        hn.nation_nk,
        lr.region_hk
    FROM {{ ref('hub_nation') }} AS hn
    INNER JOIN {{ ref('link_nation_region') }} AS lr
        ON hn.nation_hk = lr.nation_hk
)
-- 4. Final join of all current attributes
SELECT
    cg.nation_hk,
    cg.nation_nk,
    ns.n_name AS nation_name,
    ns.n_comment AS nation_comment,
    
    cg.region_hk,
    rs.r_name AS region_name,
    rs.r_comment AS region_comment
FROM consolidated_geo AS cg
LEFT JOIN current_nation_sat AS ns
    ON cg.nation_hk = ns.nation_hk
LEFT JOIN current_region_sat AS rs
    ON cg.region_hk = rs.region_hk