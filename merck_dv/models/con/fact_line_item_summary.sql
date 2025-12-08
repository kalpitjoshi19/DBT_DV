{{ config(
    materialized='table',
    schema='CON'
) }}

WITH source_data AS (
    SELECT
        l_returnflag,
        l_linestatus,
        l_quantity,
        l_extendedprice,
        l_discount,
        l_tax,
        l_shipdate
    FROM {{ ref('bdv_lineitem_actual') }}
)

SELECT
    l_returnflag,
    l_linestatus,

    -- 1. Key Metrics Calculation
    SUM(l_quantity) AS total_quantity_shipped,
    SUM(l_extendedprice) AS total_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS total_gross_sales,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS total_net_sales,
    AVG(l_quantity) AS avg_quantity_per_line,
    AVG(l_extendedprice) AS avg_extended_price,
    AVG(l_discount) AS avg_discount_rate,
    COUNT(*) AS line_item_count

FROM source_data

GROUP BY 1, 2
ORDER BY 1, 2