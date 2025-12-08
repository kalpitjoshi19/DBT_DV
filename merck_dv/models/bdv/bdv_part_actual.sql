{{ config(
    materialized='view',
    schema='BDV'
) }}

WITH latest_satellite AS (
    SELECT
        part_hk,
        MAX(load_ts) AS max_load_ts
    FROM {{ ref('sat_part') }}
    GROUP BY 1
),
current_satellite AS (
    SELECT
        s.part_hk,
        s.p_name,
        s.p_mfgr,
        s.p_brand,
        s.p_type,
        s.p_size,
        s.p_container,
        s.p_retailprice,
        s.p_comment
    FROM {{ ref('sat_part') }} AS s
    INNER JOIN latest_satellite AS l
        ON s.part_hk = l.part_hk
        AND s.load_ts = l.max_load_ts
)
SELECT
    h.part_hk,
    h.part_nk,
    s.p_name,
    s.p_mfgr,
    s.p_brand,
    s.p_type,
    s.p_size,
    s.p_container,
    s.p_retailprice,
    s.p_comment,
    s.part_hk AS satellite_hk -- Expose the satellite key for easier debugging
FROM {{ ref('hub_part') }} AS h
LEFT JOIN current_satellite AS s
    ON h.part_hk = s.part_hk