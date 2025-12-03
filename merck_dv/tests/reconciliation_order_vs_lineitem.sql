-- This test checks if the aggregated 'total_net_sales' in fact_order matches
-- the sum of 'net_sales' from all corresponding line items.

WITH line_item_totals AS (
    SELECT
        -- We need the order's natural key (or HK) to join back to fact_order
        {{ dbt_utils.surrogate_key(['l.l_orderkey', 'l.l_linenumber']) }} as order_hk_from_lineitem,
        SUM(l.net_sales) AS calculated_order_net_sales
    FROM {{ ref('fact_lineitem') }} l
    GROUP BY 1
),

reconciliation_check AS (
    SELECT
        o.order_hk,
        o.total_net_sales,
        t.calculated_order_net_sales
    FROM {{ ref('fact_order') }} o
    INNER JOIN line_item_totals t
        -- Note: This join must be on the order key/HK
        ON o.order_hk = t.order_hk_from_lineitem
    WHERE
        -- Find any order where the difference is greater than a tiny epsilon
        -- Use an absolute function to catch errors on either side
        ABS(o.total_net_sales - t.calculated_order_net_sales) > 0.001
)

SELECT * FROM reconciliation_check