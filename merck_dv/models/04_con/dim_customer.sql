{{ config(
    materialized='table',
    schema='CON',
    unique_key='customer_hk'
) }}

WITH customer_data AS (
    -- Source all attributes from the Customer BDV
    SELECT
        customer_hk,
        customer_nk, -- Natural Key for reference
        c_name,
        c_address,
        c_phone,
        c_acctbal,
        c_mktsegment,
        c_comment,
        c_nationkey AS nation_nk 
    FROM {{ ref('bdv_customer_actual') }}
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
    c.customer_hk,
    c.customer_nk,

    -- 2. Core Customer Attributes
    c.c_name AS customer_name,
    c.c_address AS customer_address,
    c.c_phone AS customer_phone,
    c.c_acctbal AS customer_account_balance,
    c.c_mktsegment AS customer_market_segment,
    c.c_comment AS customer_comment,

    -- 3. Geographic Attributes (De-normalized from Nation/Region BDV)
    n.nation_name,
    n.region_name

FROM customer_data AS c
LEFT JOIN nation_region AS n
    ON c.nation_nk = n.nation_nk