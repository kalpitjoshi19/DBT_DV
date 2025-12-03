{{ config(
    materialized='table',
    schema='CON',
    unique_key='lineitem_hk',
    sort='order_date_key'
) }}

WITH lineitem_source AS (
    -- 1. Source Lineitem data
    SELECT * FROM {{ ref('bdv_lineitem_actual') }}
),

order_source AS (
    -- 2. Source Order data to get Customer and Order Date
    SELECT
        order_hk,
        order_nk, -- This holds the O_ORDERKEY (Order Natural Key)
        o_custkey AS customer_nk,
        o_orderdate
    FROM {{ ref('bdv_order_actual') }}
),

lineitem_orders AS (
    -- Join Lineitem and Order BDVs together to link the transaction details to the customer
    SELECT
        li.*,
        o.customer_nk,
        o.o_orderdate
    FROM lineitem_source li
    INNER JOIN order_source o
        -- Join Lineitem's Order Natural Key (l_orderkey)
        -- to the Order BDV's Natural Key (order_nk).
        ON li.l_orderkey = o.order_nk
),

key_lookups AS (
    SELECT
        -- Dimension keys (Surrogate Key Lookups)
        o.date_key AS order_date_key,
        s.supplier_hk,
        p.part_hk,
        c.customer_hk,
        d_ship.date_key AS ship_date_key,
        d_commit.date_key AS commit_date_key,

        -- Lineitem Primary Key (Hash Key)
        li.lineitem_hk,

        -- Measures
        li.l_quantity,
        li.l_extendedprice,
        li.l_discount,
        li.l_tax,

        -- Calculated Measures (The most important metrics for business analysis)
        (li.l_extendedprice * (1 - li.l_discount)) AS net_price,
        (li.l_extendedprice * (1 - li.l_discount) * (1 + li.l_tax)) AS sales_amount,

        -- Additional Attributes (Pass-through)
        li.l_returnflag,
        li.l_linestatus,
        li.l_shipinstruct,
        li.l_shipmode

    FROM lineitem_orders li

    -- Join 1: Supplier Dimension Lookup (using Natural Key l_suppkey)
    LEFT JOIN {{ ref('dim_supplier') }} s
        ON li.l_suppkey = s.supplier_nk

    -- Join 2: Part Dimension Lookup (using Natural Key l_partkey)
    LEFT JOIN {{ ref('dim_part') }} p
        ON li.l_partkey = p.part_nk

    -- Join 3: Customer Dimension Lookup (using Natural Key customer_nk from the Order BDV)
    LEFT JOIN {{ ref('dim_customer') }} c
        ON li.customer_nk = c.customer_nk

    -- Join 4: Order Date Dimension Lookup (convert date to YYYYMMDD integer)
    LEFT JOIN {{ ref('dim_date') }} o
        ON TO_NUMBER(TO_CHAR(li.o_orderdate, 'YYYYMMDD')) = o.date_key

    -- Join 5: Ship Date Dimension Lookup
    LEFT JOIN {{ ref('dim_date') }} d_ship
        ON TO_NUMBER(TO_CHAR(li.l_shipdate, 'YYYYMMDD')) = d_ship.date_key

    -- Join 6: Commit Date Dimension Lookup
    LEFT JOIN {{ ref('dim_date') }} d_commit
        ON TO_NUMBER(TO_CHAR(li.l_commitdate, 'YYYYMMDD')) = d_commit.date_key
)

SELECT * FROM key_lookups