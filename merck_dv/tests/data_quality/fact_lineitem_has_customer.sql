-- Referential Integrity Check 
-- This test verifies that every line item successfully mapped to a customer in the dim_customer table, ensuring no missing foreign key links.

SELECT
    f.lineitem_hk
FROM {{ ref('fact_lineitem') }} f
WHERE
    f.customer_hk IS NULL
    -- We can also check that the customer_hk actually exists in the dimension
    AND NOT EXISTS (
         SELECT 1 FROM {{ ref('dim_customer') }} d WHERE d.customer_hk = f.customer_hk
     )