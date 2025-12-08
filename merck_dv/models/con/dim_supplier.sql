{{ config(
    materialized='table',
    schema='CON',
    unique_key='supplier_hk'
) }}

WITH supplier_data AS (
    -- Source all attributes from the Supplier BDV
    SELECT
        supplier_hk,
        supplier_nk, -- Natural Key for reference
        s_name,
        s_address,
        s_phone,
        s_acctbal,
        s_comment,
        s_nationkey AS nation_nk -- Foreign Key to Nation
    FROM {{ ref('bdv_supplier_actual') }}
),

nation_region AS (
    -- Source Nation and Region names from the Nation/Region BDV
    SELECT
        nation_nk,
        nation_name,
        region_name
    FROM {{ ref('bdv_nation_region_actual') }}
)

SELECT
    -- 1. Dimension Key (BDV Hash Key)
    s.supplier_hk,
    s.supplier_nk,

    -- 2. Core Supplier Attributes
    s.s_name AS supplier_name,
    s.s_address AS supplier_address,
    s.s_phone AS supplier_phone,
    s.s_acctbal AS supplier_account_balance,
    s.s_comment AS supplier_comment,

    -- 3. Geographic Attributes (De-normalized from Nation/Region BDV)
    n.nation_name,
    n.region_name

FROM supplier_data AS s
LEFT JOIN nation_region AS n
    ON s.nation_nk = n.nation_nk