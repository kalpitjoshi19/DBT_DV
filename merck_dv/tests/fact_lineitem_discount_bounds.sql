-- This test checks if the line item discount percentage (l_discount) falls
-- within the acceptable business range (e.g., 0% to 10%).

SELECT
    lineitem_hk,
    l_discount
FROM {{ ref('fact_lineitem') }}
WHERE
    l_discount < 0 OR l_discount > 0.10