{{ config(
    materialized='table',
    schema='CON',
    unique_key='part_hk'
) }}

WITH part_data AS (
    -- Source all attributes from the Part BDV
    SELECT
        part_hk,
        part_nk, -- Natural Key for reference
        p_name,
        p_mfgr,
        p_brand,
        p_type,
        p_size,
        p_container,
        p_retailprice,
        p_comment
    FROM {{ ref('bdv_part_actual') }}
)

SELECT
    -- 1. Dimension Key (BDV Hash Key)
    p.part_hk,
    p.part_nk,

    -- 2. Core Part Attributes
    p.p_name AS part_name,
    p.p_mfgr AS part_manufacturer,
    p.p_brand AS part_brand,
    p.p_type AS part_type,
    p.p_size AS part_size,
    p.p_container AS part_container,
    p.p_retailprice AS part_retail_price,
    p.p_comment AS part_comment

FROM part_data AS p