{{ config(
    materialized = 'incremental',
    schema = 'RDV',
    unique_key = ['CUSTOMER_HK', 'NATION_HK']
) }}

{%- set source_model = ref('stg_customer') -%}

WITH staged_customer AS (
    -- 1. STAGE CUSTOMER DATA: Select the required keys and metadata from staging
    SELECT
        customer_hk AS CUSTOMER_HK,
        c_nationkey AS NATION_NK, -- Confirmed Nation Natural Key from stg_customer
        load_ts AS LOAD_DATE,
        rsrc AS SOURCE
    FROM {{ source_model }}
    -- Ensure we only process non-null customer keys and nations
    WHERE customer_hk IS NOT NULL AND c_nationkey IS NOT NULL
),

hub_nation AS (
    -- 2. HUB NATION: Retrieve the Hash Key and Natural Key for Nations
    SELECT
        NATION_HK,
        NATION_NK
    FROM {{ ref('hub_nation') }}
),

staged AS (
    -- 3. JOIN: Join staged customer data to the Nation Hub using the Natural Key
    -- This retrieves the necessary NATION_HK to build the link.
    SELECT
        sc.CUSTOMER_HK,
        hn.NATION_HK,
        -- FIX: Replaced the datavault4dbt.hash_columns macro call with explicit SQL
        -- to resolve the 'unexpected AS' syntax error.
        -- We are creating the Link Primary Key (LPK) using both Hash Keys.
        MD5(CONCAT_WS('||', sc.CUSTOMER_HK, hn.NATION_HK)) AS CUSTOMER_NATION_PK,
        sc.LOAD_DATE,
        sc.SOURCE
    FROM staged_customer sc
    INNER JOIN hub_nation hn
        ON sc.NATION_NK = hn.NATION_NK -- Join on the Nation Natural Key
)

{% if is_incremental() %}

, existing_links AS (
    -- 4. EXISTING_LINKS: Select keys that already exist in the target table
    SELECT
        CUSTOMER_NATION_PK
    FROM {{ this }}
)

, records_to_insert AS (
    -- 5. RECORDS_TO_INSERT: Only insert new, unique links
    SELECT
        stg.CUSTOMER_NATION_PK,
        stg.CUSTOMER_HK,
        stg.NATION_HK,
        stg.LOAD_DATE,
        stg.SOURCE
    FROM staged AS stg
    LEFT JOIN existing_links AS el
        ON stg.CUSTOMER_NATION_PK = el.CUSTOMER_NATION_PK
    WHERE el.CUSTOMER_NATION_PK IS NULL
)

-- Final SELECT for Incremental Run
SELECT * FROM records_to_insert

{% else %}

-- Final SELECT for Full Refresh
SELECT DISTINCT
    CUSTOMER_NATION_PK,
    CUSTOMER_HK,
    NATION_HK,
    LOAD_DATE,
    SOURCE
FROM staged

{% endif %}